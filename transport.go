package grinta

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/go-metrics"
	"github.com/hashicorp/memberlist"
	"github.com/quic-go/quic-go"
	grintav1alpha1 "github.com/raskyld/grinta/gen/grinta/v1alpha1"
	"github.com/raskyld/grinta/pkg/flow"
)

// TransportConfig represents configuration for the GRINTA protocol.
type TransportConfig struct {
	// TlsConfig should be configured to ensure mTLS is enabled between the
	// peers.
	TlsConfig *tls.Config

	// BindAddr and BindPort are where we want the GRINTA protocol to
	// listen.
	BindAddr string
	BindPort int

	// HintMaxFlows gives an indication of how much flow you intend to allocate.
	// If this number is too low and you allocate a lot of flow, GRINTA will open
	// a lot of connection instead of multiplexing flows efficiently.
	HintMaxFlows int64

	// MetricsLabels to add to every metrics emitted by the GRINTA protocol.
	MetricLabels []metrics.Label

	// MetricSink to use for emitting metrics.
	MetricSink metrics.MetricSink

	// DialTimeout controls how much time we wait for stream establishment.
	// Default to 10 seconds.
	DialTimeout time.Duration

	// LogHandler to use for emitting structured logs.
	LogHandler slog.Handler
}

// Transport is an abstraction over the GRINTA protocol.
type Transport struct {
	cfg    *TransportConfig
	logger *slog.Logger
	msink  metrics.MetricSink

	finalAdvAddr net.IP
	finalAdvPort int
	finalAdvDone bool
	lk           sync.RWMutex

	wg sync.WaitGroup

	// GRINTA protocol
	flowCh   chan *streamFlow
	hostsCxs map[string][]hostCx

	// Memberlist Protocol
	packetCh chan *memberlist.Packet
	streamCh chan net.Conn

	// QUIC layer
	tr    *quic.Transport
	ln    *quic.Listener
	qconf *quic.Config

	// UDP layer
	udpLn *net.UDPConn
}

type hostCx quic.Connection

func NewTransport(cfg *TransportConfig) (trans *Transport, err error) {
	if cfg.TlsConfig == nil {
		return nil, ErrNoTLSConfig
	}

	t := &Transport{
		cfg:      cfg,
		flowCh:   make(chan *streamFlow, 64),
		hostsCxs: make(map[string][]hostCx),
		packetCh: make(chan *memberlist.Packet, 1024),
		streamCh: make(chan net.Conn, 64),
	}

	if cfg.LogHandler == nil {
		t.logger = slog.Default()
	} else {
		t.logger = slog.New(cfg.LogHandler)
	}

	if cfg.MetricSink == nil {
		t.msink = metrics.Default()
	} else {
		t.msink = cfg.MetricSink
	}

	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}

	defer func() {
		if err != nil {
			t.Shutdown()
		}
	}()

	port := cfg.BindPort
	if port == 0 {
		port = 6174
	}

	addr := net.ParseIP(cfg.BindAddr)
	if addr == nil {
		addr = net.IPv4zero
	}

	udpAddr := &net.UDPAddr{IP: addr, Port: port}
	udpLn, err := net.ListenUDP("udp", udpAddr)
	t.udpLn = udpLn
	if err != nil {
		return nil, fmt.Errorf("transport: failed to allocate UDP listener: %w", err)
	}

	t.tr = &quic.Transport{
		Conn:                             udpLn,
		DisableVersionNegotiationPackets: true,
	}

	hintFlow := cfg.HintMaxFlows
	if hintFlow == 0 {
		hintFlow = 10000
	}

	t.qconf = &quic.Config{
		Versions:        []quic.Version{quic.Version2, quic.Version1},
		EnableDatagrams: true,
		// TODO(raskyld): as a later optimisation, we may accept
		// 0-RTT to leverage session resumption and treat ping request
		// earlier when the client already connected with us.
		// The performance gains of this would be minimal though since
		// it only applies when there is no active flow between two peers,
		// in other case, pings are just piggy-backed on active quic.Connection
		// and sent as datagram.
		Allow0RTT:             false,
		MaxIncomingStreams:    hintFlow,
		MaxIncomingUniStreams: hintFlow,
		MaxIdleTimeout:        1 * time.Minute,
		HandshakeIdleTimeout:  5 * time.Minute,
	}

	ln, err := t.tr.Listen(t.cfg.TlsConfig, t.qconf)
	if err != nil {
		return nil, fmt.Errorf("transport: failed to allocate QUIC listener: %w", err)
	}

	t.ln = ln
	t.wg.Add(1)
	go t.acceptCx()
	return t, err
}

func (t *Transport) FinalAdvertiseAddr(addr string, port int) (advIP net.IP, advPort int, err error) {
	if t.udpLn == nil {
		return nil, 0, ErrUdpNotAvailable
	}

	defer func() {
		if err == nil {
			t.lk.Lock()
			t.finalAdvAddr = advIP
			t.finalAdvPort = advPort
			t.finalAdvDone = true
			t.lk.Unlock()
		}
	}()

	if addr != "" {
		advIP = net.ParseIP(addr)
	}

	if port != 0 {
		advPort = port
	}

	if advIP != nil && advPort != 0 {
		return
	}

	udpAddr := t.udpLn.LocalAddr().(*net.UDPAddr)
	if advIP == nil {
		advIP = udpAddr.IP
	}

	if advIP == nil {
		panic("could not retrieve IP to advertise")
	}

	if ip4 := advIP.To4(); ip4 != nil {
		advIP = ip4
	}

	if advPort == 0 {
		advPort = udpAddr.Port
	}
	return
}

func (t *Transport) GetAdvertiseAddr() (advIP net.IP, advPort int, err error) {
	t.lk.RLock()
	defer t.lk.RUnlock()

	if !t.finalAdvDone {
		err = ErrTransportNotAdvertised
	}
	advIP = t.finalAdvAddr
	advPort = t.finalAdvPort
	return
}

func (t *Transport) WriteTo(b []byte, addr string) (time.Time, error) {
	return t.WriteToAddress(b, memberlist.Address{
		Addr: addr,
	})
}

func (t *Transport) WriteToAddress(b []byte, addr memberlist.Address) (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), t.cfg.DialTimeout)
	defer cancel()
	conn, err := t.getActiveCx(ctx, addr)
	if err != nil {
		return time.Time{}, err
	}

	mLabels := append(
		t.cfg.MetricLabels,
		LabelPeerAddr.M(addr.Addr),
		LabelPerspective.M(ClientPerspective.String()),
	)

	err = conn.SendDatagram(b)
	ts := time.Now()

	if err == nil {
		t.msink.IncrCounterWithLabels(
			MetricDByte,
			float32(len(b)),
			mLabels,
		)
	} else {
		t.msink.IncrCounterWithLabels(
			MetricDErr,
			1.0,
			mLabels,
		)
	}

	return ts, err
}

func (t *Transport) PacketCh() <-chan *memberlist.Packet {
	return t.packetCh
}

func (t *Transport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return t.DialAddressTimeout(memberlist.Address{
		Addr: addr,
	}, timeout)
}

func (t *Transport) DialAddressTimeout(addr memberlist.Address, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	hcx, err := t.getActiveCx(ctx, addr)
	mLabels := append(t.cfg.MetricLabels, LabelPeerAddr.M(addr.Addr), LabelPerspective.M(ClientPerspective.String()))
	if err != nil {
		t.msink.IncrCounterWithLabels(
			MetricSErr,
			1.0,
			append(mLabels, LabelError.M("no_conn_to_host")),
		)
		return nil, err
	}

	stream, err := hcx.OpenStreamSync(ctx)
	if err != nil {
		t.msink.IncrCounterWithLabels(
			MetricSErr,
			1.0,
			append(mLabels, LabelError.M("cannot_open_stream")),
		)
		return nil, err
	}

	streamFlow := newStreamFlow(stream, stream, hcx, grintav1alpha1.StreamMode_STREAM_MODE_GOSSIP, "")
	return t.initialiseOutboundStream(ctx, streamFlow)
}

func (t *Transport) dialFlow(ctx context.Context, addr string, dest string, bidi bool) (*streamFlow, error) {
	hcx, err := t.getActiveCx(ctx, memberlist.Address{
		Addr: addr,
	})
	mLabels := append(t.cfg.MetricLabels, LabelPeerAddr.M(addr), LabelPerspective.M(ClientPerspective.String()))
	if err != nil {
		t.msink.IncrCounterWithLabels(
			MetricSErr,
			1.0,
			append(mLabels, LabelError.M("no_conn_to_host")),
		)
		return nil, err
	}

	var recv quic.ReceiveStream
	var send quic.SendStream
	if bidi {
		stream, err := hcx.OpenStreamSync(ctx)
		if err != nil {
			t.msink.IncrCounterWithLabels(
				MetricSErr,
				1.0,
				append(mLabels, LabelError.M("cannot_open_stream"), LabelStreamDirection.M("bidirectional")),
			)
			return nil, err
		}
		recv = stream
		send = stream
	} else {
		stream, err := hcx.OpenUniStreamSync(ctx)
		if err != nil {
			t.msink.IncrCounterWithLabels(
				MetricSErr,
				1.0,
				append(mLabels, LabelError.M("cannot_open_stream"), LabelStreamDirection.M("unidirectional")),
			)
			return nil, err
		}
		send = stream
	}

	stream := newStreamFlow(send, recv, hcx, grintav1alpha1.StreamMode_STREAM_MODE_FLOW, dest)

	return t.initialiseOutboundStream(ctx, stream)
}

func (t *Transport) StreamCh() <-chan net.Conn {
	return t.streamCh
}

// Shutdown initiate a graceful termination.
func (t *Transport) Shutdown() error {
	if t.ln != nil {
		t.ln.Close()
	}

	t.lk.Lock()
	for _, cxs := range t.hostsCxs {
		for _, cx := range cxs {
			QErrShutdown.Close(cx, "shutting down")
		}
	}
	t.lk.Unlock()

	if t.tr != nil {
		t.tr.Close()
	}

	if t.udpLn != nil {
		t.udpLn.Close()
	}
	return nil
}

func (t *Transport) acceptCx() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept(context.TODO())
		if err != nil {
			if errors.Is(err, quic.ErrServerClosed) {
				t.logger.Info("shutdown: stop accepting new QUIC connections")
			} else {
				t.logger.Error("transport listener failed", "error", err)
			}
			break
		}

		t.handleConn(conn, ServerPerspective)
	}
}

func (t *Transport) waitForDatagrams(hcx hostCx) {
	defer t.wg.Done()
	peer := hcx.RemoteAddr().String()

	logger := t.logger.With(
		LabelPeerAddr.L(peer),
		LabelPerspective.L(ServerPerspective.String()),
	)
	mLabels := append(t.cfg.MetricLabels,
		LabelPeerAddr.M(peer),
		LabelPerspective.M(ServerPerspective.String()),
	)

	for {
		buf, err := hcx.ReceiveDatagram(context.Background())
		ts := time.Now()

		if hcx.Context().Err() != nil {
			t.msink.IncrCounterWithLabels(
				MetricDErr,
				1.0,
				append(mLabels, LabelError.M("connection_broken")),
			)
			break
		}

		if err != nil {
			t.msink.IncrCounterWithLabels(
				MetricDErr,
				1.0,
				append(mLabels, LabelError.M("transient")),
			)
			logger.Error("error reading UDP packet", "error", err)
			continue
		}

		n := len(buf)
		if n < 1 {
			t.msink.IncrCounterWithLabels(
				MetricDErr,
				1.0,
				append(mLabels, LabelError.M("too_small")),
			)
			logger.Error("received a too short udp packet", "length", n)
			continue
		}

		t.msink.IncrCounterWithLabels(MetricDByte, float32(n), mLabels)
		t.packetCh <- &memberlist.Packet{
			Buf:       buf,
			From:      hcx.RemoteAddr(),
			Timestamp: ts,
		}
	}
}

func (t *Transport) handleStreams(hcx hostCx, bidi bool) {
	defer t.wg.Done()
	peer := hcx.RemoteAddr().String()

	logger := t.logger.With(
		LabelPeerAddr.L(peer),
		LabelPerspective.L(ServerPerspective.String()),
	)
	mLabels := append(t.cfg.MetricLabels,
		LabelPeerAddr.M(peer),
		LabelPerspective.M(ServerPerspective.String()),
	)

	for {
		var err error
		var sender quic.SendStream
		var recv quic.ReceiveStream

		if bidi {
			stream, gotErr := hcx.AcceptStream(context.Background())
			err = gotErr
			sender = stream
			recv = stream
		} else {
			recv, err = hcx.AcceptUniStream(context.Background())
		}

		if cerr := hcx.Context().Err(); cerr != nil {
			logger.Warn("connection was broken", LabelError.L(cerr))
			t.msink.IncrCounterWithLabels(
				MetricSErr,
				1.0,
				append(mLabels, LabelError.M("connection_broken")),
			)
			return
		}

		if err != nil {
			logger.Warn("transient error accepting stream", LabelError.L(err))
			t.msink.IncrCounterWithLabels(
				MetricSErr,
				1.0,
				append(mLabels, LabelError.M("transient")),
			)
			continue
		}

		initCtx, cancel := context.WithTimeout(context.Background(), t.cfg.DialTimeout)
		defer cancel()

		streamFlow := newInboundStreamFlow(sender, recv, hcx)
		swrap, err := t.initialiseInboundStream(initCtx, streamFlow)
		if err != nil {
			logger.Error("failed to establish stream", LabelError.L(err))
			return
		}

		switch swrap.mode {
		case grintav1alpha1.StreamMode_STREAM_MODE_GOSSIP:
			t.streamCh <- swrap
		case grintav1alpha1.StreamMode_STREAM_MODE_FLOW:
			t.flowCh <- swrap
		}
	}
}

func (t *Transport) getActiveCx(
	ctx context.Context,
	target memberlist.Address,
) (hostCx, error) {
	t.lk.RLock()
	if !t.finalAdvDone {
		t.lk.RUnlock()
		return nil, ErrTransportNotAdvertised
	}
	cx, hasCx := t.firstActiveCx(target.Addr)
	t.lk.RUnlock()

	if hasCx {
		return cx, nil
	}

	return t.dial(ctx, target.Addr)
}

func (t *Transport) dial(ctx context.Context, target string) (hostCx, error) {
	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidAddr, err)
	}

	cx, err := t.tr.Dial(ctx, addr, t.cfg.TlsConfig, t.qconf)
	if err != nil {
		return nil, err
	}

	return t.handleConn(cx, ClientPerspective)
}

// not thread safe!
// must be called by an holder of Write lock
func (t *Transport) garbageCollectCxs(target string) ([]hostCx, bool) {
	cxs, hasCxs := t.hostsCxs[target]
	if !hasCxs {
		return cxs, hasCxs
	}

	logger := t.logger.With(LabelPeerAddr.L(target))
	cleanedUpList := make([]hostCx, 0, len(cxs))
	for _, cx := range cxs {
		if cx.Context().Err() == nil {
			cleanedUpList = append(cleanedUpList, cx)
		}
	}

	if len(cleanedUpList) == 0 {
		delete(t.hostsCxs, target)
		logger.Debug("finished connection gc: all connections are now dead")
		return nil, false
	} else {
		t.hostsCxs[target] = cleanedUpList
	}

	if len(cleanedUpList) != len(cxs) {
		logger.Debug("finished connection gc: remaining connections", "remaining", len(cleanedUpList))
	}
	return cleanedUpList, true
}

// not thread safe!
// must be called by an holder of Read lock
func (t *Transport) firstActiveCx(target string) (hostCx, bool) {
	cxs, hasCxs := t.hostsCxs[target]
	if !hasCxs {
		return nil, false
	}

	for _, cx := range cxs {
		if cx.Context().Err() == nil {
			return cx, true
		}
	}

	return nil, false
}

func (t *Transport) handleConn(conn quic.Connection, perspective Perspective) (hostCx, error) {
	peer := conn.RemoteAddr().String()

	t.lk.Lock()
	if !t.finalAdvDone {
		t.lk.Unlock()
		return nil, ErrTransportNotAdvertised
	}
	gcHost, _ := t.garbageCollectCxs(peer)
	t.hostsCxs[peer] = append(gcHost, conn)
	t.lk.Unlock()

	t.msink.IncrCounterWithLabels(
		MetricConnCount,
		1.0,
		append(t.cfg.MetricLabels, LabelPeerAddr.M(peer), LabelPerspective.M(perspective.String())),
	)

	t.logger.
		With(LabelPeerAddr.L(peer), LabelPerspective.L(perspective.String())).
		Info("new connection established")

	t.wg.Add(3)
	go t.waitForDatagrams(conn)
	go t.handleStreams(conn, true)
	go t.handleStreams(conn, false)
	return conn, nil
}

func (t *Transport) initialiseOutboundStream(
	ctx context.Context,
	stream *streamFlow,
) (*streamFlow, error) {
	peerAddr := stream.remoteAddr.String()
	logger := t.logger.With(
		LabelStreamID.L(stream.StreamID()),
		LabelPeerAddr.L(peerAddr),
		LabelPerspective.L(ClientPerspective.String()),
	)
	mLabels := append(
		t.cfg.MetricLabels,
		LabelPeerAddr.M(peerAddr),
		LabelPerspective.M(ClientPerspective.String()),
	)

	initFrame := &grintav1alpha1.InitFrame{}
	initFrame.SetMode(stream.mode)
	initFrame.SetDestName(stream.destination)

	frame := &grintav1alpha1.Frame{}
	frame.SetInit(initFrame)

	encoder := flow.NewProtoCodec[*grintav1alpha1.Frame](false)

	ack := make(chan error, 1)
	ackWg := sync.WaitGroup{}
	ackWg.Add(1)
	go func() {
		defer ackWg.Done()
		ack <- encoder.Encode(stream.SendStream, frame)
	}()

	var err error
	select {
	case err = <-ack:
	case <-ctx.Done():
		stream.SendStream.CancelWrite(QErrStreamTimeout)
		err = ctx.Err()
	}
	ackWg.Wait()
	close(ack)

	if err != nil {
		t.msink.IncrCounterWithLabels(
			MetricSErr,
			1.0,
			append(
				mLabels,
				LabelError.M("cannot_send_init_frame"),
			),
		)
		logger.Warn("stream establishment failed", LabelError.L(err))
		return nil, err
	}

	t.msink.IncrCounterWithLabels(
		MetricSCount,
		1.0,
		mLabels,
	)
	logger.Debug("stream established")

	return stream, nil
}

func (t *Transport) initialiseInboundStream(
	ctx context.Context,
	stream *streamFlow,
) (*streamFlow, error) {
	peerAddr := stream.remoteAddr.String()
	logger := t.logger.With(
		LabelStreamID.L(stream.StreamID()),
		LabelPeerAddr.L(peerAddr),
		LabelPerspective.L(ServerPerspective.String()),
	)
	mLabels := append(
		t.cfg.MetricLabels,
		LabelPeerAddr.M(peerAddr),
		LabelPerspective.M(ServerPerspective.String()),
	)

	decoder := flow.NewProtoCodec[*grintav1alpha1.Frame](false)

	ack := make(chan struct {
		fr  *grintav1alpha1.Frame
		err error
	}, 1)
	ackWg := sync.WaitGroup{}
	ackWg.Add(1)
	go func() {
		defer ackWg.Done()
		untyFrame, err := decoder.Decode(stream.ReceiveStream)
		ack <- struct {
			fr  *grintav1alpha1.Frame
			err error
		}{untyFrame.(*grintav1alpha1.Frame), err}
	}()

	var frame *grintav1alpha1.Frame
	var err error
	select {
	case result := <-ack:
		frame = result.fr
		err = result.err
	case <-ctx.Done():
		stream.SendStream.CancelWrite(QErrStreamTimeout)
		err = ctx.Err()
	}
	ackWg.Wait()
	close(ack)

	if err != nil {
		logger.Error("could not retrieve init frame", LabelError.L(err))
		stream.Cancel(QErrStreamProtocolViolation)
		t.msink.IncrCounterWithLabels(
			MetricSErr,
			1.0,
			append(mLabels, LabelError.M("no_init_frame")),
		)
		return nil, ErrProtocolViolation
	}

	initFrame := frame.GetInit()
	if initFrame == nil {
		logger.Warn("grinta protocol violation: first frame is not init one")
		stream.Cancel(QErrStreamProtocolViolation)
		t.msink.IncrCounterWithLabels(
			MetricSErr,
			1.0,
			append(mLabels, LabelError.M("protocol_violation")),
		)
		return nil, ErrProtocolViolation
	}

	t.msink.IncrCounterWithLabels(
		MetricSCount,
		1.0,
		append(mLabels, LabelStreamMode.M(initFrame.GetMode().String())),
	)

	stream.destination = initFrame.GetDestName()
	stream.mode = initFrame.GetMode()

	return stream, nil
}

type Perspective uint8

const (
	ServerPerspective Perspective = iota
	ClientPerspective
)

func (p Perspective) String() string {
	switch p {
	case ServerPerspective:
		return "server"
	case ClientPerspective:
		return "client"
	}
	panic("unreachable")
}

type streamFlow struct {
	quic.ReceiveStream
	quic.SendStream

	mode                  grintav1alpha1.StreamMode
	localAddr, remoteAddr net.Addr
	destination           string
}

var _ net.Conn = (*streamFlow)(nil)

func (s *streamFlow) StreamID() quic.StreamID {
	if s.ReceiveStream != nil {
		return s.ReceiveStream.StreamID()
	}
	if s.SendStream != nil {
		return s.SendStream.StreamID()
	}
	panic("unable to retrieve StreamID both stream direction are nil")
}

func (s *streamFlow) LocalAddr() net.Addr {
	return s.localAddr
}

func (s *streamFlow) SetDeadline(t time.Time) error {
	return errors.Join(s.SetReadDeadline(t), s.SetWriteDeadline(t))
}

func (s *streamFlow) RemoteAddr() net.Addr {
	return s.remoteAddr
}

func (s *streamFlow) Cancel(err quic.StreamErrorCode) {
	if s.ReceiveStream != nil {
		s.ReceiveStream.CancelRead(err)
	}
	if s.SendStream != nil {
		s.SendStream.CancelWrite(err)
	}
}

func newStreamFlow(
	send quic.SendStream,
	recv quic.ReceiveStream,
	hcx hostCx,
	mode grintav1alpha1.StreamMode,
	destination string,
) *streamFlow {
	return &streamFlow{
		mode:          mode,
		localAddr:     hcx.LocalAddr(),
		remoteAddr:    hcx.RemoteAddr(),
		destination:   destination,
		ReceiveStream: recv,
		SendStream:    send,
	}
}

func newInboundStreamFlow(
	send quic.SendStream,
	recv quic.ReceiveStream,
	hcx hostCx,
) *streamFlow {
	return &streamFlow{
		localAddr:     hcx.LocalAddr(),
		remoteAddr:    hcx.RemoteAddr(),
		ReceiveStream: recv,
		SendStream:    send,
	}
}
