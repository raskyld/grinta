package goroutinettes

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unique"

	"github.com/hashicorp/go-metrics"
	"github.com/hashicorp/memberlist"
	"github.com/quic-go/quic-go"
	grintav1alpha1 "github.com/raskyld/goroutinettes/gen/grinta/v1alpha1"
	"google.golang.org/protobuf/proto"
)

const defaultUDPBufferSize int = 1 << 21

// TransportConfig represents configuration for the GRINTA protocol.
type TransportConfig struct {
	// BufferSize of the requested UDP kernel buffer.
	BufferSize int

	// EnforceBufferSize crashes if the kernel doesn't allocate what we asked.
	// If that's false, we retry and divide by 2 the requested
	// `TransportConfig.BufferSize` until it fits or fails.
	EnforceBufferSize bool

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

	// HostnameResolver to resolve hostname from peer certificates.
	HostnameResolver HostnameResolver

	// MetricsLabels to add to every metrics emitted by the GRINTA protocol.
	MetricLabels []metrics.Label

	// MetricSink to use for emitting metrics.
	MetricSink metrics.MetricSink

	// DialTimeout controls how much time we wait for stream establishment.
	// Default to 10 seconds.
	DialTimeout time.Duration

	// GracePeriod is how much time we accept to wait for stream buffers to
	// get flushed. Default to 10 seconds.
	GracePeriod time.Duration

	// LogHandler to use for emitting structured logs.
	LogHandler slog.Handler
}

// Transport is an abstraction over the GRINTA protocol.
type Transport struct {
	cfg    *TransportConfig
	logger *slog.Logger
	msink  metrics.MetricSink

	// graceful termination asked, do not spam of connection error in logs
	gracefulTerm atomic.Bool

	// GRINTA protocol
	flowCh     chan *streamWrapper
	AddrToHost map[string]unique.Handle[Hostname]
	hostsInfo  map[unique.Handle[Hostname]]Host
	hostsCxs   map[unique.Handle[Hostname]][]hostCx
	hostsLock  sync.RWMutex

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

type hostCx struct {
	// closeCh is closed to wake-up stream garbage collectors.
	closeCh chan struct{}
	quic.Connection
}

func NewTransport(cfg *TransportConfig) (trans *Transport, err error) {
	if cfg.TlsConfig == nil {
		return nil, ErrNoTLSConfig
	}

	t := &Transport{
		cfg:        cfg,
		flowCh:     make(chan *streamWrapper),
		AddrToHost: make(map[string]unique.Handle[Hostname]),
		hostsInfo:  make(map[unique.Handle[Hostname]]Host),
		hostsCxs:   make(map[unique.Handle[Hostname]][]hostCx),
		packetCh:   make(chan *memberlist.Packet),
		streamCh:   make(chan net.Conn),
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

	if cfg.GracePeriod == 0 {
		cfg.GracePeriod = 10 * time.Second
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
	if err != nil {
		return nil, fmt.Errorf("transport: failed to allocate UDP listener: %w", err)
	}
	t.udpLn = udpLn

	requested := cfg.BufferSize
	if requested == 0 {
		requested = defaultUDPBufferSize
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
	go t.acceptCx()
	return t, err
}

func (t *Transport) FinalAdvertiseAddr(_ string, _ int) (net.IP, int, error) {
	if t.udpLn == nil {
		return nil, 0, ErrUdpNotAvailable
	}

	ipPort := strings.Split(t.udpLn.LocalAddr().String(), ":")
	if len(ipPort) != 2 {
		panic(fmt.Sprintf("go runtime produced invalid udp addr: %s", t.udpLn.LocalAddr().String()))
	}

	ip := ipPort[0]
	parsedPort, err := strconv.Atoi(ipPort[1])
	if err != nil {
		panic(fmt.Sprintf("go runtime produced invalid udp port %s", ipPort[1]))
	}

	advertiseAddr := net.ParseIP(ip)
	if advertiseAddr == nil {
		panic(fmt.Sprintf("go runtime produced invalid udp IP %s", ip))
	}

	if ip4 := advertiseAddr.To4(); ip4 != nil {
		advertiseAddr = ip4
	}

	return advertiseAddr, parsedPort, nil
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

	mLabels := append(t.cfg.MetricLabels, LabelPeerAddr.M(addr.Addr), LabelPeerName.M(addr.Name))
	err = conn.SendDatagram(b)
	ts := time.Now()
	if err == nil {
		t.msink.IncrCounterWithLabels(
			MetricGrintaDatagramOutBytes,
			float32(len(b)),
			mLabels,
		)
	} else {
		t.msink.IncrCounterWithLabels(
			MetricGrintaDatagramOutErrorCount,
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
	mLabels := append(t.cfg.MetricLabels, LabelPeerAddr.M(addr.Addr), LabelPeerName.M(addr.Name))
	if err != nil {
		t.msink.IncrCounterWithLabels(
			MetricGrintaStreamEstOutErrorCount,
			1.0,
			append(mLabels, LabelError.M("no_conn_to_host")),
		)
		return nil, err
	}

	stream, err := hcx.OpenStreamSync(ctx)
	if err != nil {
		t.msink.IncrCounterWithLabels(
			MetricGrintaStreamEstOutErrorCount,
			1.0,
			append(mLabels, LabelError.M("cannot_open_stream")),
		)
		return nil, err
	}

	return t.initialiseOutboundStream(ctx, stream, hcx, &grintav1alpha1.InitFrame{
		Mode: grintav1alpha1.StreamMode_STREAM_MODE_GOSSIP,
	})
}

func (t *Transport) StreamCh() <-chan net.Conn {
	return t.streamCh
}

func (t *Transport) Shutdown() error {
	if !t.gracefulTerm.CompareAndSwap(false, true) {
		// no-op because it was already shutdown
		return nil
	}

	t.hostsLock.Lock()
	for _, cxs := range t.hostsCxs {
		for _, cx := range cxs {
			close(cx.closeCh)
		}
	}
	t.hostsLock.Unlock()

	// dumb SO_LINGER like behaviour until it is implemented
	// in go-quic
	time.Sleep(t.cfg.GracePeriod)

	t.hostsLock.Lock()
	for _, cxs := range t.hostsCxs {
		for _, cx := range cxs {
			QErrShutdown.Close(cx.Connection, "we are shutting down! bye!")
		}
	}
	t.hostsLock.Unlock()

	if t.tr != nil {
		t.tr.Close()
	}

	if t.udpLn != nil {
		t.udpLn.Close()
	}
	return nil
}

func (t *Transport) acceptCx() {
	for {
		conn, err := t.ln.Accept(context.TODO())
		if err != nil {
			if !t.gracefulTerm.Load() {
				// NB(raskyld): atm, the implementation only return errors if
				// Close() has been called, that's why we make assumptions but
				// that's not a good design. As the quic implementation evolve,
				// we may implement some retry mechanisms etc.
				t.logger.Warn("unexpected QUIC listener closure", "error", err)
				return
			}
		}

		t.handleConn(conn, true)
	}
}

func (t *Transport) waitForDatagrams(hcx hostCx) {
	remoteAddr := hcx.RemoteAddr()
	ctx := hcx.Context()
	logger := t.logger.With("remote", remoteAddr)
	mLabels := append(t.cfg.MetricLabels, LabelPeerAddr.M(remoteAddr.String()))
	for {
		buf, err := hcx.ReceiveDatagram(ctx)
		ts := time.Now()
		if t.gracefulTerm.Load() {
			break
		}

		if err != nil {
			if ctx.Err() != nil {
				t.msink.IncrCounterWithLabels(
					MetricGrintaDatagramInErrorCount,
					1.0,
					append(mLabels, LabelError.M("connection_broken")),
				)
				break
			}
			t.msink.IncrCounterWithLabels(
				MetricGrintaDatagramInErrorCount,
				1.0,
				append(mLabels, LabelError.M("transient")),
			)
			logger.Error("error reading UDP packet", "error", err)
			continue
		}

		n := len(buf)
		if n < 1 {
			t.msink.IncrCounterWithLabels(
				MetricGrintaDatagramInErrorCount,
				1.0,
				append(mLabels, LabelError.M("too_small")),
			)
			logger.Error("received a too short udp packet", "length", n)
			continue
		}

		t.msink.IncrCounterWithLabels(MetricGrintaDatagramInBytes, float32(n), mLabels)
		t.packetCh <- &memberlist.Packet{
			Buf:       buf,
			From:      remoteAddr,
			Timestamp: ts,
		}
	}
}

func (t *Transport) handleStreams(hcx hostCx) {
	remoteAddr := hcx.RemoteAddr()
	ctx := hcx.Context()
	logger := t.logger.With("remote", remoteAddr)
	mLabels := append(t.cfg.MetricLabels, LabelPeerAddr.M(remoteAddr.String()))

	for {
		stream, err := hcx.AcceptStream(ctx)
		if t.gracefulTerm.Load() {
			break
		}

		if err != nil {
			if ctx.Err() != nil {
				logger.Warn("connection was broken", LabelError.L(ctx.Err()))
				t.msink.IncrCounterWithLabels(
					MetricGrintaStreamEstInErrorCount,
					1.0,
					append(mLabels, LabelError.M("connection_broken")),
				)
				break
			}

			logger.Warn("transient error accepting stream", LabelError.L(err))
			t.msink.IncrCounterWithLabels(
				MetricGrintaStreamEstInErrorCount,
				1.0,
				append(mLabels, LabelError.M("transient")),
			)
			continue
		}

		// NB: we offload the stream establishment in another goroutine to avoid
		// a misbehaving peer from blocking us.
		go func() {
			initCtx, cancel := context.WithTimeout(ctx, t.cfg.DialTimeout)
			defer cancel()
			swrap, err := t.initialiseInboundStream(initCtx, stream, hcx)
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
		}()
	}
}

func (t *Transport) getActiveCx(
	ctx context.Context,
	target memberlist.Address,
) (hostCx, error) {
	t.hostsLock.RLock()
	var dest unique.Handle[Hostname]
	if target.Name != "" {
		dest = unique.Make(Hostname(target.Name))
	} else {
		resolved, ok := t.AddrToHost[target.Addr]
		if !ok {
			t.hostsLock.RUnlock()
			return t.dial(ctx, target.Addr)
		}
		dest = resolved
	}

	cx, hasCx := t.firstActiveCx(dest)
	if hasCx {
		t.hostsLock.RUnlock()
		return cx, nil
	}

	t.hostsLock.RUnlock()
	return t.dial(ctx, target.Addr)
}

func (t *Transport) dial(ctx context.Context, target string) (hostCx, error) {
	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return hostCx{}, fmt.Errorf("%w: %w", ErrInvalidAddr, err)
	}

	cx, err := t.tr.Dial(ctx, addr, t.cfg.TlsConfig, t.qconf)
	if t.gracefulTerm.Load() {
		return hostCx{}, ErrShutdown
	}
	if err != nil {
		t.logger.Warn("failed to dial", LabelPeerAddr.L(target))
		return hostCx{}, err
	}

	return t.handleConn(cx, false)
}

// not thread safe!
// must be called by an holder of Write lock
func (t *Transport) garbageCollectCxs(dest unique.Handle[Hostname]) ([]hostCx, bool) {
	logger := t.logger.With(LabelPeerName.L(dest.Value()))
	cxs, hasCxs := t.hostsCxs[dest]
	if !hasCxs {
		return cxs, hasCxs
	}

	cleanedUpList := make([]hostCx, 0, len(cxs))
	for _, cx := range cxs {
		if cx.Context().Err() == nil {
			cleanedUpList = append(cleanedUpList, cx)
		}
	}

	if len(cleanedUpList) == 0 {
		delete(t.hostsCxs, dest)
		logger.Debug("finished connection gc: all connections are now dead")
		return nil, false
	} else {
		t.hostsCxs[dest] = cleanedUpList
	}

	if len(cleanedUpList) != len(cxs) {
		logger.Debug("finished connection gc: remaining connections", "remaining", len(cleanedUpList))
	}
	return cleanedUpList, true
}

// not thread safe!
// must be called by an holder of Read lock
func (t *Transport) firstActiveCx(dest unique.Handle[Hostname]) (hostCx, bool) {
	cxs, hasCxs := t.hostsCxs[dest]
	if !hasCxs {
		return hostCx{}, false
	}

	for _, cx := range cxs {
		if cx.Context().Err() == nil {
			return cx, true
		}
	}

	return hostCx{}, false
}

func (t *Transport) handleConn(conn quic.Connection, inbound bool) (hostCx, error) {
	peer := conn.RemoteAddr().String()
	peerAddrPort := strings.Split(peer, ":")
	if len(peerAddrPort) != 2 {
		panic("unreachable: unexpected address format")
	}
	peerAddr := peerAddrPort[0]
	peerPort, err := strconv.Atoi(peerAddrPort[1])
	if err != nil {
		panic(err)
	}

	logger := t.logger.With(LabelPeerAddr.L(peer))
	mLabels := append(t.cfg.MetricLabels, LabelPeerAddr.M(peer))
	resolver := t.cfg.HostnameResolver
	if resolver == nil {
		resolver = CommonNameResolver
	}

	rsvHostname, err, uerr := resolver(conn.ConnectionState().TLS.PeerCertificates)
	if err != nil {
		logger.Error("failed to resolve hostname", "error", err)
		t.msink.IncrCounterWithLabels(
			MetricGrintaConnErrorCount,
			1.0,
			append(mLabels, LabelError.M("name_resolution")),
		)
		if uerr == "" {
			QErrInternal.Close(
				conn,
				"unexpected error during hostname resolution",
			)
		} else {
			QErrInternal.Close(
				conn,
				fmt.Sprintf("error during resolution: %s", uerr),
			)
		}
		return hostCx{}, ErrHostnameResolve
	}

	logger = logger.With(LabelPeerName.L(rsvHostname))
	mLabels = append(mLabels, LabelPeerName.M(string(rsvHostname)))

	rsvHostnameHandle := unique.Make(Hostname(rsvHostname))
	t.hostsLock.Lock()
	// First, we check if we need to update our Addr to Hostname
	// mapping.
	currentHostname, ok := t.AddrToHost[peer]
	if ok {
		if currentHostname != rsvHostnameHandle {
			logger := logger.With(
				"old", currentHostname.Value(),
				"new", rsvHostname,
			)

			logger.Warn("a peer changed its name, updating")
			t.AddrToHost[peer] = rsvHostnameHandle

			// We need to migrate the connections as well.
			cxs, hasConnections := t.hostsCxs[currentHostname]
			if hasConnections {
				logger.Debug("migrating connections")
				delete(t.hostsCxs, currentHostname)
				t.hostsCxs[rsvHostnameHandle] = cxs
			}
			t.msink.IncrCounterWithLabels(
				MetricGrintaHostNameChanges,
				1.0,
				append(t.cfg.MetricLabels, LabelPeerAddr.M(peer)),
			)
		}
	} else {
		t.AddrToHost[peer] = rsvHostnameHandle
		logger.Info("new peer discovered")
	}

	// We also check if we have node name conflict
	hostInfo, ok := t.hostsInfo[rsvHostnameHandle]
	if ok {
		if hostInfo.Addr != peerAddr || hostInfo.Port != peerPort {
			logger := logger.With(
				"oldAddr", hostInfo.Addr,
				"oldPort", hostInfo.Port,
				"newAddr", peerAddr,
				"newPort", peerPort,
			)
			logger.Warn("a node has been migrated or there is a name conflict in the cluster")
			t.msink.IncrCounterWithLabels(
				MetricGrintaHostNameChanges,
				1.0,
				append(t.cfg.MetricLabels, LabelPeerName.M(string(rsvHostname))),
			)

			gcHost, stillHasConnection := t.garbageCollectCxs(rsvHostnameHandle)
			if stillHasConnection {
				msg := "we detected a node name conflict in the cluster! " +
					"this may be because you have rescheduled a node on another machine, " +
					"if you haven't, then it could mean one of your certificate has leaked! " +
					"if that's the case, you must revoke the certificate or add the hostname to " +
					"the BanList config."

				logger.Error(msg)
				t.msink.IncrCounterWithLabels(
					MetricGrintaHostConflictsCount,
					1.0,
					append(t.cfg.MetricLabels, LabelPeerAddr.M(peer)),
				)

				for _, cx := range gcHost {
					// TODO(raskyld): implement a ban list.
					// TODO(raskyld): implement connection drain out of Shutdown()
					QErrNameConflict.Close(cx, msg)
				}
				delete(t.hostsCxs, rsvHostnameHandle)
			}

			t.hostsInfo[rsvHostnameHandle] = Host{
				Name: rsvHostnameHandle,
				Addr: peerAddr,
				Port: peerPort,
			}
		}
	} else {
		t.hostsInfo[rsvHostnameHandle] = Host{
			Name: rsvHostnameHandle,
			Addr: peerAddr,
			Port: peerPort,
		}
	}

	// Then, we actually perform the connection update
	// after a pass of garbage collection.
	hcx := hostCx{
		closeCh:    make(chan struct{}, 1),
		Connection: conn,
	}
	gcHost, _ := t.garbageCollectCxs(rsvHostnameHandle)
	t.hostsCxs[rsvHostnameHandle] = append(gcHost, hcx)
	t.hostsLock.Unlock()

	t.msink.IncrCounterWithLabels(
		MetricGrintaConnEstCount,
		1.0,
		mLabels,
	)

	logger.Info("new connection established", "inbound", inbound)

	// NB: it's ok to pass by value, the struct is just two cheap pointers.
	go t.waitForDatagrams(hcx)
	go t.handleStreams(hcx)
	return hcx, nil
}

func (t *Transport) initialiseOutboundStream(
	ctx context.Context,
	stream quic.Stream,
	hcx hostCx,
	initFrame *grintav1alpha1.InitFrame,
) (*streamWrapper, error) {
	peerAddr := hcx.Connection.RemoteAddr().String()
	logger := t.logger.With(
		LabelStreamID.L(stream.StreamID()),
		LabelPeerAddr.L(peerAddr),
	)
	mLabels := append(
		t.cfg.MetricLabels,
		LabelPeerAddr.M(peerAddr),
	)

	logger.Debug("initiating outbound stream")
	swrap := &streamWrapper{
		localAddr:  hcx.LocalAddr(),
		remoteAddr: hcx.RemoteAddr(),
		Stream:     stream,
	}

	go swrap.garbageCollector(hcx.closeCh)
	frame := &grintav1alpha1.Frame{
		Type: &grintav1alpha1.Frame_Init{
			Init: initFrame,
		},
	}

	buf, err := proto.Marshal(frame)
	if err != nil {
		logger.Error("unexpected failure to marhsal a protobuf message", LabelError.L(err))
		return nil, err
	}

	dl, ok := ctx.Deadline()
	if ok {
		stream.SetWriteDeadline(dl)
	}

	n, err := stream.Write(buf)
	if err != nil {
		mLabels := append(
			mLabels,
			LabelError.M("cannot_send_init_frame"),
		)
		t.msink.IncrCounterWithLabels(
			MetricGrintaStreamEstOutErrorCount,
			1.0,
			mLabels,
		)
		return nil, fmt.Errorf("%w: %w", ErrStreamWrite, err)
	}

	if n != len(buf) {
		logger.Error(
			"unexpected mismatch between frame size and bytes written",
			"expected", len(buf),
			"actual", n,
			LabelError.L(ErrTooLargeFrame),
		)

		mLabels := append(
			mLabels,
			LabelError.M("init_frame_too_large"),
		)
		t.msink.IncrCounterWithLabels(
			MetricGrintaStreamEstOutErrorCount,
			1.0,
			mLabels,
		)

		return nil, ErrTooLargeFrame
	}

	t.msink.IncrCounterWithLabels(
		MetricGrintaStreamEstOutCount,
		1.0,
		append(
			mLabels,
			LabelError.M("cannot_send_init_frame"),
		),
	)
	return swrap, nil
}

func (t *Transport) initialiseInboundStream(
	ctx context.Context,
	stream quic.Stream,
	hcx hostCx,
) (*streamWrapper, error) {
	peerAddr := hcx.Connection.RemoteAddr().String()
	logger := t.logger.With(
		LabelStreamID.L(stream.StreamID()),
		LabelPeerAddr.L(peerAddr),
	)
	mLabels := append(
		t.cfg.MetricLabels,
		LabelPeerAddr.M(peerAddr),
	)

	logger.Debug("initiating outbound stream")
	swrap := &streamWrapper{
		localAddr:  hcx.LocalAddr(),
		remoteAddr: hcx.RemoteAddr(),
		Stream:     stream,
	}

	go swrap.garbageCollector(hcx.closeCh)

	dl, ok := ctx.Deadline()
	if ok {
		stream.SetReadDeadline(dl)
	}

	buf := make([]byte, 1200)
	var n int
	for {
		m, err := stream.Read(buf)
		if t.gracefulTerm.Load() {
			return nil, ErrShutdown
		}

		if err != nil {
			if serr := stream.Context().Err(); serr != nil {
				logger.Warn("stream was broken", LabelError.L(serr))
				t.msink.IncrCounterWithLabels(
					MetricGrintaStreamEstInErrorCount,
					1.0,
					append(mLabels, LabelError.M("stream_broken")),
				)
				return nil, serr
			}

			if ctx.Err() != nil {
				t.msink.IncrCounterWithLabels(
					MetricGrintaStreamEstInErrorCount,
					1.0,
					append(mLabels, LabelError.M("stream_read_timeout")),
				)
				return nil, ctx.Err()
			}

			logger.Warn("transient error reading stream", LabelError.L(err))
			continue
		}

		if m > 0 {
			n = m
			break
		}

		logger.Debug("received an empty frame from stream")
	}

	var frame grintav1alpha1.Frame
	err := proto.Unmarshal(buf[:n], &frame)
	if err != nil {
		logger.Warn("grinta protocol violation: malformed frame", "error", err)
		stream.CancelRead(QErrStreamProtocolViolation)
		stream.CancelWrite(QErrStreamProtocolViolation)
		t.msink.IncrCounterWithLabels(
			MetricGrintaStreamEstInErrorCount,
			1.0,
			append(mLabels, LabelError.M("protocol_violation")),
		)
		return nil, ErrProtocolViolation
	}

	initFrame, ok := frame.Type.(*grintav1alpha1.Frame_Init)
	if !ok || initFrame.Init == nil {
		logger.Warn("grinta protocol violation: first frame is not init one")
		stream.CancelRead(QErrStreamProtocolViolation)
		stream.CancelWrite(QErrStreamProtocolViolation)
		t.msink.IncrCounterWithLabels(
			MetricGrintaStreamEstInErrorCount,
			1.0,
			append(mLabels, LabelError.M("protocol_violation")),
		)
		return nil, ErrProtocolViolation
	}

	swrap.mode = initFrame.Init.Mode
	swrap.destination = initFrame.Init.GetDestName()
	swrap.source = initFrame.Init.GetSrcName()
	t.msink.IncrCounterWithLabels(
		MetricGrintaStreamEstInCount,
		1.0,
		append(mLabels, LabelStreamMode.M(swrap.mode.String())),
	)
	return swrap, nil
}
