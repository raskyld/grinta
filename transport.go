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
	DialTimeout time.Duration

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
	tr *quic.Transport
	ln *quic.Listener

	// UDP layer
	udpLn *net.UDPConn
}

type hostCx struct {
	// closeCh is closed to wake-up stream garbage collectors.
	closeCh chan struct{}
	quic.Connection
}

func NewTransport(cfg *TransportConfig) (t *Transport, err error) {
	if cfg.TlsConfig == nil {
		return nil, ErrNoTLSConfig
	}

	t = &Transport{
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

	if err := t.negociateBufferSize(requested); err != nil {
		return nil, err
	}

	t.tr = &quic.Transport{
		Conn: udpLn,
	}

	hintFlow := cfg.HintMaxFlows
	if hintFlow == 0 {
		hintFlow = 10000
	}

	ln, err := t.tr.Listen(t.cfg.TlsConfig, &quic.Config{
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
	})
	if err != nil {
		return nil, fmt.Errorf("transport: failed to allocate QUIC listener: %w", err)
	}

	t.ln = ln
	return
}

func (t *Transport) FinalAdvertiseAddr(_ string, _ int) (net.IP, int, error) {
	if t.udpLn == nil {
		return nil, 0, ErrUdpNotAvailable
	}

	ipPort := strings.Split(t.udpLn.LocalAddr().String(), ":")
	if len(ipPort) != 2 {
		panic(fmt.Sprintf("go runtime produced invalid udp addr", t.udpLn.LocalAddr().String()))
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
	ctx, _ := context.WithTimeout(context.Background(), t.cfg.DialTimeout)
	conn, err := t.getActiveCx(ctx, addr)
	if err != nil {
		return time.Time{}, err
	}

	ts := time.Now()
	err = conn.SendDatagram(b)
	if err == nil {
		t.msink.IncrCounterWithLabels(
			MetricGrintaDatagramOutBytes,
			float32(len(b)),
			append(t.cfg.MetricLabels, LabelsForAddr(addr)...),
		)
	} else {
		t.msink.IncrCounterWithLabels(
			MetricGrintaDatagramOutErrorCount,
			1.0,
			append(t.cfg.MetricLabels, LabelsForAddr(addr)...),
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
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	hcx, err := t.getActiveCx(ctx, addr)
	if err != nil {
		mLabels := append(t.cfg.MetricLabels, metrics.Label{Name: MLabelError, Value: "no_conn_to_host"})
		mLabels = append(mLabels, LabelsForAddr(addr)...)
		t.msink.IncrCounterWithLabels(
			MetricGrintaStreamEstOutErrorCount,
			1.0,
			mLabels,
		)
		return nil, err
	}

	stream, err := hcx.OpenStreamSync(ctx)
	if err != nil {
		mLabels := append(t.cfg.MetricLabels, metrics.Label{Name: MLabelError, Value: "cannot_open_stream"})
		mLabels = append(mLabels, LabelsForAddr(addr)...)
		t.msink.IncrCounterWithLabels(
			MetricGrintaStreamEstOutErrorCount,
			1.0,
			mLabels,
		)
		return nil, err
	}

	swrap := &streamWrapper{
		localAddr:  hcx.LocalAddr(),
		remoteAddr: hcx.RemoteAddr(),
		Stream:     stream,
	}

	go swrap.garbageCollector(hcx.closeCh)
	initFrame := &grintav1alpha1.Frame{
		Type: &grintav1alpha1.Frame_Init{
			Init: &grintav1alpha1.InitFrame{
				Mode: grintav1alpha1.StreamMode_STREAM_MODE_GOSSIP,
			},
		},
	}

	buf, err := proto.Marshal(initFrame)
	if err != nil {
		// this is a software critical error, it should never happen
		panic(err)
	}

	_, err = stream.Write(buf)
	if err != nil {
		mLabels := append(t.cfg.MetricLabels, metrics.Label{Name: MLabelError, Value: "cannot_send_init_frame"})
		mLabels = append(mLabels, LabelsForAddr(addr)...)
		t.msink.IncrCounterWithLabels(
			MetricGrintaStreamEstOutErrorCount,
			1.0,
			mLabels,
		)
		return nil, fmt.Errorf("%w: %w", ErrStreamWrite, err)
	}

	mLabels := append(t.cfg.MetricLabels, LabelsForAddr(addr)...)
	t.msink.IncrCounterWithLabels(
		MetricGrintaStreamEstOutCount,
		1.0,
		mLabels,
	)
	return swrap, nil
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
	time.Sleep(10 * time.Second)

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

func (t *Transport) negociateBufferSize(requested int) error {
	size := requested
	for size > 0 {
		if err := t.udpLn.SetReadBuffer(size); err != nil {
			if t.cfg.EnforceBufferSize {
				return ErrBufferSize
			}
			size = size >> 1
			continue
		}
		if size != requested {
			t.logger.Warn("using smaller than expected UDP buffer", "bytes", size)
		}
		t.msink.SetGaugeWithLabels(
			MetricGrintaUDPBufferSizeBytes,
			float32(size),
			t.cfg.MetricLabels,
		)
		return nil
	}
	return ErrBufferSize
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

		t.handleConn(conn)
	}
}

func (t *Transport) waitForDatagrams(hcx hostCx) {
	remoteAddr := hcx.RemoteAddr()
	ctx := hcx.Context()
	logger := t.logger.With("remote", remoteAddr)
	mLabels := append(t.cfg.MetricLabels, metrics.Label{Name: MLabelPeerAddr, Value: remoteAddr.String()})

	for {
		buf, err := hcx.ReceiveDatagram(ctx)
		ts := time.Now()
		if t.gracefulTerm.Load() {
			logger.Debug("datagram listener gracefully shutting down")
			break
		}

		if err != nil {
			if ctx.Err() != nil {
				break
			}
			t.msink.IncrCounterWithLabels(
				MetricGrintaDatagramInErrorCount,
				1.0,
				append(mLabels, metrics.Label{Name: MLabelError, Value: "unknown"}),
			)
			logger.Error("error reading UDP packet", "error", err)
			continue
		}

		n := len(buf)
		if n < 1 {
			t.msink.IncrCounterWithLabels(
				MetricGrintaDatagramInErrorCount,
				1.0,
				append(mLabels, metrics.Label{Name: MLabelError, Value: "too_small"}),
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
	mLabels := append(t.cfg.MetricLabels, metrics.Label{Name: MLabelPeerAddr, Value: remoteAddr.String()})

	for {
		stream, err := hcx.AcceptStream(ctx)
		if t.gracefulTerm.Load() {
			logger.Debug("stream listener gracefully shutting down")
			break
		}

		if err != nil {
			logger.Warn("error accepting stream", "error", err)
			if ctx.Err() != nil {
				logger.Error("connection was broken", "error", ctx.Err())
				break
			}
			t.msink.IncrCounterWithLabels(
				MetricGrintaStreamEstInErrorCount,
				1.0,
				append(mLabels, metrics.Label{Name: MLabelError, Value: "unknown"}),
			)
			continue
		}

		logger = logger.With("stream_id", stream.StreamID())
		logger.Debug("received a stream request")

		swrap := &streamWrapper{
			localAddr:  hcx.LocalAddr(),
			remoteAddr: hcx.RemoteAddr(),
			Stream:     stream,
		}

		// When a connection should be closed, it will first
		// close its `closeCh` channel and wait for its streams
		// to finish draining their buffers.
		// Sadly, atm, go-quic has no public API to see if the drain
		// is over, so we will implement a dumb "wait X seconds" before
		// closing the connection itself.
		go swrap.garbageCollector(hcx.closeCh)

		buf := make([]byte, 0, 64)
		_, err = stream.Read(buf)
		if t.gracefulTerm.Load() {
			logger.Debug("stream listener gracefully shutting down")
			break
		}

		if err != nil {
			if serr := swrap.Context().Err(); serr != nil {
				logger.Warn("stream was broken", "error", serr)
				t.msink.IncrCounterWithLabels(
					MetricGrintaStreamEstInErrorCount,
					1.0,
					append(mLabels, metrics.Label{Name: MLabelError, Value: "stream_broken"}),
				)
			}
			t.msink.IncrCounterWithLabels(
				MetricGrintaStreamEstInErrorCount,
				1.0,
				append(mLabels, metrics.Label{Name: MLabelError, Value: "no_init_frame"}),
			)
			logger.Error("error waiting for stream init frame", "error", err)
			continue
		}

		var frame grintav1alpha1.Frame
		err = proto.Unmarshal(buf, &frame)
		if err != nil {
			logger.Warn("grinta protocol violation: malformed frame", "error", err)
			stream.CancelRead(QErrStreamProtocolViolation)
			stream.CancelWrite(QErrStreamProtocolViolation)
			t.msink.IncrCounterWithLabels(
				MetricGrintaStreamEstInErrorCount,
				1.0,
				append(mLabels, metrics.Label{Name: MLabelError, Value: "protocol_violation"}),
			)
			continue
		}

		initFrame, ok := frame.Type.(*grintav1alpha1.Frame_Init)
		if !ok {
			logger.Warn("grinta protocol violation: first frame is not init one")
			stream.CancelRead(QErrStreamProtocolViolation)
			stream.CancelWrite(QErrStreamProtocolViolation)
			t.msink.IncrCounterWithLabels(
				MetricGrintaStreamEstInErrorCount,
				1.0,
				append(mLabels, metrics.Label{Name: MLabelError, Value: "protocol_violation"}),
			)
			continue
		}

		switch initFrame.Init.Mode {
		case grintav1alpha1.StreamMode_STREAM_MODE_UNSPECIFIED:
			logger.Warn("grinta protocol violation: unknown mode")
			stream.CancelRead(QErrStreamProtocolViolation)
			stream.CancelWrite(QErrStreamProtocolViolation)
			t.msink.IncrCounterWithLabels(
				MetricGrintaStreamEstInErrorCount,
				1.0,
				append(mLabels, metrics.Label{Name: MLabelError, Value: "protocol_violation"}),
			)
		case grintav1alpha1.StreamMode_STREAM_MODE_GOSSIP:
			mLabels := append(mLabels, metrics.Label{Name: MLabelStreamMode, Value: "gossip"})
			t.msink.IncrCounterWithLabels(
				MetricGrintaStreamEstInCount, 1.0, mLabels)
			t.streamCh <- swrap
		case grintav1alpha1.StreamMode_STREAM_MODE_FLOW:
			mLabels := append(mLabels, metrics.Label{Name: MLabelStreamMode, Value: "flow"})
			t.msink.IncrCounterWithLabels(
				MetricGrintaStreamEstInCount, 1.0, mLabels)
			swrap.source = initFrame.Init.GetSrcName()
			swrap.destination = initFrame.Init.GetDestName()
			t.flowCh <- swrap
		}
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

	cx, err := t.tr.Dial(ctx, addr, t.cfg.TlsConfig, nil)
	if t.gracefulTerm.Load() {
		return hostCx{}, ErrShutdown
	}
	if err != nil {
		return hostCx{}, err
	}

	return t.handleConn(cx)
}

// not thread safe!
// must be called by an holder of Write lock
func (t *Transport) garbageCollectCxs(dest unique.Handle[Hostname]) ([]hostCx, bool) {
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
		return nil, false
	} else {
		t.hostsCxs[dest] = cleanedUpList
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

func (t *Transport) handleConn(conn quic.Connection) (hostCx, error) {
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

	logger := t.logger.With("addr", peerAddr, "port", peerPort)
	resolver := t.cfg.HostnameResolver
	if resolver == nil {
		resolver = CommonNameResolver
	}

	mLabels := append(t.cfg.MetricLabels, metrics.Label{Name: MLabelPeerAddr, Value: peer})

	rsvHostname, err, uerr := resolver(conn.ConnectionState().TLS.PeerCertificates)
	if err != nil {
		logger.Error("failed to resolve hostname", "error", err)
		t.msink.IncrCounterWithLabels(
			MetricGrintaConnErrorCount,
			1.0,
			append(mLabels, metrics.Label{Name: MLabelError, Value: "name_resolution"}),
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

	mLabels = append(mLabels, metrics.Label{Name: MLabelPeerName, Value: string(rsvHostname)})

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
				append(t.cfg.MetricLabels, metrics.Label{Name: MLabelPeerAddr, Value: peer}),
			)
		}
	} else {
		t.AddrToHost[peer] = rsvHostnameHandle
		logger.Info("new peer discovered", "hostname", rsvHostname)
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
			logger.Warn(
				"a node has been migrated or there is a name conflict in the cluster")
			t.msink.IncrCounterWithLabels(
				MetricGrintaHostNameChanges,
				1.0,
				append(t.cfg.MetricLabels, metrics.Label{Name: MLabelPeerName, Value: string(rsvHostname)}),
			)
			gcHost, stillHasConnection := t.garbageCollectCxs(rsvHostnameHandle)
			if stillHasConnection {
				logger.Error("connection is still active after node migration, that's a symptom of name conflict!")
				t.msink.IncrCounterWithLabels(
					MetricGrintaHostConflictsCount,
					1.0,
					append(t.cfg.MetricLabels, metrics.Label{Name: MLabelPeerAddr, Value: peer}),
				)
				for _, cx := range gcHost {
					// TODO(raskyld): implement a ban list.
					// TODO(raskyld): implement connection drain out of Shutdown()
					QErrNameConflict.Close(
						cx, "we detected a node name conflict in the cluster! "+
							"this may be because you have rescheduled a node on another machine, "+
							"if you haven't, then it could mean one of your certificate has leaked! "+
							"if that's the case, you must revoke the certificate or add the hostname to "+
							"the BanList config.",
					)
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

	// NB: it's ok to pass by value, the struct is just two cheap pointers.
	go t.waitForDatagrams(hcx)
	go t.handleStreams(hcx)
	return hcx, nil
}
