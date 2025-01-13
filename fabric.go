package grinta

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"sync"
	"time"

	"github.com/hashicorp/go-metrics"
	"github.com/hashicorp/serf/serf"
	grintav1alpha1 "github.com/raskyld/grinta/gen/grinta/v1alpha1"
	"github.com/raskyld/grinta/pkg/flow"
	"google.golang.org/protobuf/proto"
)

const MaxEndpointLength = 128
const MaxReasonBytes = 255

var InvalidEndpointName = regexp.MustCompile(`[^A-Za-z0-9\-\.]+`)

type Fabric struct {
	config config
	logger *slog.Logger

	// gossip
	dir     *nameDirectory
	serf    *serf.Serf
	eventCh chan serf.Event

	// transport
	tr            *Transport
	localIP       net.IP
	localPort     int
	localAddr     string
	localNodeName string

	// endpoints management
	localEPs   map[string]*endpoint
	epGC       chan *endpoint
	epGCWriter sync.WaitGroup

	// synchronisation
	lk sync.Mutex

	// 2-phase close:
	// phase 1: shutdown notification, graceful termination.
	// phase 2: drop, all resources are freed.
	shutdown   bool
	shutdownCh chan struct{}
	dropCh     chan struct{}
	wg         sync.WaitGroup
}

func Create(opts ...Option) (*Fabric, error) {
	fb := &Fabric{
		eventCh:  make(chan serf.Event, 512),
		localEPs: make(map[string]*endpoint),
		epGC:     make(chan *endpoint, 64),

		shutdownCh: make(chan struct{}),
		dropCh:     make(chan struct{}),
	}

	// Fine-tune Serf config.
	fb.config.serfCfg = serf.DefaultConfig()
	// We will wait for QUIC buffers to flush anyway.
	fb.config.serfCfg.LeavePropagateDelay = 4 * time.Second
	fb.config.serfCfg.LogOutput = nil
	fb.config.serfCfg.MemberlistConfig.ProbeTimeout = 2 * time.Second
	// TODO(raskyld): handle back-pressure by slowing down events.
	fb.config.serfCfg.QueueDepthWarning = 512
	// We don't do any smart routing decision, we don't need coordinates.
	fb.config.serfCfg.DisableCoordinates = true
	// We will restrict node and goroutine name to alphanum, dash and dot,
	// so all chars are encoded in a single byte.
	fb.config.serfCfg.ValidateNodeNames = true
	// When we need real-time, we do queries, in other case,
	// we should optimise bandwidth by coalescing events.
	fb.config.serfCfg.CoalescePeriod = 5 * time.Second
	fb.config.serfCfg.UserCoalescePeriod = 10 * time.Second
	fb.config.serfCfg.QuiescentPeriod = 1 * time.Second
	fb.config.serfCfg.UserQuiescentPeriod = 2 * time.Second
	fb.config.serfCfg.EventCh = fb.eventCh

	// Run options now that we have a non-nil Serf config.
	for _, opt := range opts {
		err := opt(&fb.config)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidCfg, err)
		}
	}

	fb.config.trCfg.BindAddr = fb.config.serfCfg.MemberlistConfig.BindAddr
	fb.config.trCfg.BindPort = fb.config.serfCfg.MemberlistConfig.BindPort

	// Logging implementations.
	if fb.config.logHandler != nil {
		fb.logger = slog.New(fb.config.logHandler)
		fb.config.serfCfg.Logger = slog.NewLogLogger(fb.config.logHandler, slog.LevelDebug)
	} else {
		fb.logger = slog.Default()
		fb.config.serfCfg.Logger = slog.NewLogLogger(slog.Default().Handler(), slog.LevelDebug)
	}
	fb.config.serfCfg.MemberlistConfig.Logger = fb.config.serfCfg.Logger

	// Metrics implementations.
	if fb.config.msink == nil {
		fb.config.msink = metrics.Default()
	}

	// Initiate GRINTA transport layer.
	tr, err := NewTransport(&fb.config.trCfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCfg, err)
	}
	fb.tr = tr

	// Make memberlist use GRINTA transport.
	fb.config.serfCfg.MemberlistConfig.Transport = tr

	// Initiate the Serf layer.
	serf, err := serf.Create(fb.config.serfCfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCfg, err)
	}
	fb.serf = serf

	// Fetch our final advertised interfaces.
	ip, port, err := tr.GetAdvertiseAddr()
	if err != nil {
		return nil, err
	}
	fb.localIP = ip
	fb.localPort = port
	fb.localAddr = fmt.Sprintf("%s:%d", ip, port)
	fb.localNodeName = fb.serf.LocalMember().Name

	// Handle cluster events and inbound flow.
	fb.wg.Add(3)
	go fb.handleEvents()
	go fb.handleEndpointGC()
	go fb.handleFlow()

	// Create our name dir.
	fb.dir = newNameDir(fb.logger, fb, fb.localNodeName)

	return fb, nil
}

func (fb *Fabric) JoinCluster() error {
	fb.lk.Lock()
	defer fb.lk.Unlock()
	if fb.shutdown {
		return ErrFabricClosed
	}
	if len(fb.config.neighbours) > 0 {
		joined, err := fb.serf.Join(fb.config.neighbours, true)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrJoinCluster, err)
		}
		fb.logger.Info("cluster joined")
		if len(fb.config.neighbours) != joined {
			fb.logger.Warn(
				"not all neighbours are reachable",
				"joined", joined,
				"expected", len(fb.config.neighbours),
			)
		}
	}
	return nil
}

func (fb *Fabric) Topology() []serf.Member {
	return fb.serf.Members()
}

func (fb *Fabric) Shutdown() error {
	// Phase 1: Shutdown notify.
	fb.lk.Lock()
	if fb.shutdown {
		fb.lk.Unlock()
		return nil
	}
	fb.shutdown = true
	close(fb.shutdownCh)
	fb.lk.Unlock()

	start := time.Now()
	fb.logger.Info("shutting down...")

	fb.logger.Info("shutdown: endpoint garbage collector")
	fb.epGCWriter.Wait()

	fb.logger.Info("shutdown: leave cluster")
	fb.serf.Leave()

	// Phase 2: Drop all resources.
	close(fb.dropCh)
	fb.logger.Info("shutdown: release gossip resources")
	fb.serf.Shutdown()
	fb.dir.close()

	fb.logger.Info("shutdown: wait for sub-tasks to finish")
	fb.wg.Wait()
	<-fb.serf.ShutdownCh()

	fb.logger.Info("shutdown: completed", LabelDuration.L(time.Since(start)))
	return nil
}

func (fb *Fabric) handleEvents() {
	defer fb.wg.Done()
	for {
		var event serf.Event
		select {
		case event = <-fb.eventCh:
		case <-fb.dropCh:
			return
		}

		switch event := event.(type) {
		case serf.MemberEvent:

		case serf.UserEvent:
			switch event.Name {
			case "name_record":
				record := &grintav1alpha1.NameClaim{}
				err := proto.Unmarshal(event.Payload, record)
				if err != nil {
					fb.logger.Error("failed to unmarshal an event", LabelError.L(err))
				} else {
					fb.dir.record(record, false)
				}
			default:
				fb.logger.Error("received unexpected event", "event_name", event.Name)
			}
		case *serf.Query:
			switch event.Name {
			case "resolve_endpoint":
				query := &grintav1alpha1.NameOwnershipQuery{}
				err := proto.Unmarshal(event.Payload, query)
				if err != nil {
					fb.logger.Error("failed to unmarshal a query", LabelError.L(err))
				} else if !query.HasEndpointName() {
					fb.logger.Warn("received an empty query", LabelPeerName.L(event.SourceNode()))
				} else {
					response := &grintav1alpha1.NameOwnershipResponse{}
					_, claim, _ := fb.dir.resolve(query.GetEndpointName())
					if claim != nil {
						response.SetClaim(claim)
					}
					payload, err := proto.Marshal(response)
					if err != nil {
						fb.logger.Error("failed to marshal a response", LabelError.L(err))
					} else {
						err = event.Respond(payload)
						if err != nil {
							fb.logger.Error("failed to answer to a query", LabelError.L(err))
						}
					}
				}
			default:
				fb.logger.Error("received unexpected query", "query_name", event.Name)
			}
		}
	}
}

func (fb *Fabric) handleFlow() {
	defer fb.wg.Done()
	for {
		var fl *streamFlow
		select {
		case fl = <-fb.tr.flowCh:
		case <-fb.shutdownCh:
			fb.logger.Info("shutdown: stop accepting inbound flow establishments")
			return
		}
		fb.lk.Lock()
		ep, exists := fb.localEPs[fl.destination]
		fb.lk.Unlock()

		if !exists {
			fl.Cancel(QErrStreamEndpointDoesNotExists)
			continue
		}

		ep.lk.Lock()
		if !ep.closed {
			ep.writers.Add(1)
			ep.lk.Unlock()

			ctx, cancel := context.WithTimeout(context.Background(), fb.tr.cfg.DialTimeout)

			select {
			case ep.flowCh <- flow.Raw{
				RawReceiver: flow.RemoteReceiver{ReceiveStream: fl.ReceiveStream},
				RawSender:   flow.RemoteSender{SendStream: fl.SendStream},
			}:
			case <-ep.closeCh:
				fl.Cancel(QErrStreamShutdown)
			case <-ctx.Done():
				fl.Cancel(QErrStreamBufferFull)
				fb.config.msink.IncrCounterWithLabels(
					MetricFlowErr,
					1.0,
					[]metrics.Label{
						LabelEndpointName.M(fl.destination),
						LabelError.M("buffer_full"),
						LabelPerspective.M(ServerPerspective.String()),
					},
				)
				fb.logger.Error(
					"failed to deliver flow establishment: buffer is full",
					LabelEndpointName.L(fl.destination),
				)
			}
			ep.writers.Done()
			cancel()
		} else {
			ep.lk.Unlock()
			fl.Cancel(QErrStreamEndpointDoesNotExists)
		}
	}
}

func (fb *Fabric) handleEndpointGC() {
	defer fb.wg.Done()
	for {
		var ep *endpoint
		select {
		case ep = <-fb.epGC:
		case <-fb.shutdownCh:
			fb.lk.Lock()
			for _, ep := range fb.localEPs {
				ep.close(true)
				fb.gcEndpoint(ep)
			}
			close(fb.epGC)
			fb.lk.Unlock()
			return
		}

		fb.lk.Lock()
		fb.gcEndpoint(ep)
		fb.lk.Unlock()
	}
}

func (fb *Fabric) gcEndpoint(ep *endpoint) error {
	fbEp, has := fb.localEPs[ep.name]
	if has {
		if fbEp == ep {
			delete(fb.localEPs, ep.name)
		} else {
			// we already reclaimed the name with another endpoint.
			return nil
		}
	}

	record := &grintav1alpha1.NameClaim{}
	record.SetNodeName(fb.localNodeName)
	record.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_UNCLAIM)
	record.SetEndpointName(ep.name)

	if err := fb.dir.record(record, true); err != nil {
		return err
	}

	payload, err := proto.Marshal(record)
	if err != nil {
		panic(fmt.Sprintf("unexpected fail to marshal: %s", err))
	}

	if err := fb.serf.UserEvent("name_record", payload, true); err != nil {
		return err
	}

	fb.logger.Debug("released endpoint", LabelEndpointName.L(ep.name))
	return nil
}

func (fb *Fabric) CreateEndpoint(name string) (Endpoint, error) {
	if !ValidateEndpointName(name) {
		return nil, ErrNameInvalid
	}

	fb.lk.Lock()
	defer fb.lk.Unlock()
	if fb.shutdown {
		return nil, ErrFabricClosed
	}
	_, has := fb.localEPs[name]
	if has {
		return nil, ErrNameConflict
	}
	record := &grintav1alpha1.NameClaim{}
	record.SetNodeName(fb.localNodeName)
	record.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)
	record.SetEndpointName(name)

	err := fb.dir.record(record, true)
	if err != nil {
		return nil, err
	}

	payload, err := proto.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFabricInvalidFrame, err)
	}

	err = fb.serf.UserEvent("name_record", payload, true)
	if err != nil {
		return nil, err
	}

	ep := newEndpoint(name, func(e *endpoint) {
		fb.lk.Lock()
		if fb.shutdown {
			fb.lk.Unlock()
			return
		}
		fb.epGCWriter.Add(1)
		fb.lk.Unlock()

		select {
		case <-fb.shutdownCh:
		case fb.epGC <- e:
		}

		fb.epGCWriter.Done()
	})
	fb.localEPs[name] = ep
	return ep, nil
}

func (fb *Fabric) ScanEndpoint(prefix string) ([]string, error) {
	return fb.dir.scan(prefix)
}

func (fb *Fabric) DialEndpoint(ctx context.Context, name string) (flow.RawSender, error) {
	raw, err := fb.dialEndpoint(ctx, name, false)
	if err != nil {
		return nil, err
	}

	return raw.RawSender, nil
}

func (fb *Fabric) DialEndpointBidi(ctx context.Context, name string) (raw flow.Raw, err error) {
	return fb.dialEndpoint(ctx, name, true)
}

func (fb *Fabric) dialEndpoint(ctx context.Context, name string, bidirectional bool) (raw flow.Raw, err error) {
	if !ValidateEndpointName(name) {
		return raw, ErrNameInvalid
	}

	fb.lk.Lock()
	shutdown := fb.shutdown
	fb.lk.Unlock()
	if shutdown {
		return raw, ErrFabricClosed
	}

	owner, _, err := fb.dir.resolveWithCluster(ctx, name)
	if err != nil {
		return raw, err
	}

	if owner == fb.localNodeName {
		fb.lk.Lock()
		ep, has := fb.localEPs[name]
		fb.lk.Unlock()
		if !has {
			return raw, ErrNameResolution
		}

		var inbound *flow.LocalFlow
		outbound := flow.NewLocalFlow(1024)
		raw.RawSender = outbound

		if bidirectional {
			inbound = flow.NewLocalFlow(1024)
			raw.RawReceiver = inbound
		}

		ep.lk.Lock()
		defer ep.lk.Unlock()
		if ep.closed {
			return raw, ErrNameResolution
		}
		ep.flowCh <- flow.Raw{
			RawReceiver: outbound,
			RawSender:   inbound,
		}

		return raw, nil
	}

	nodes := fb.serf.Members()
	var nodeAddr string
	for _, node := range nodes {
		if node.Name == owner {
			nodeAddr = fmt.Sprintf("%s:%d", node.Addr, node.Port)
			break
		}
	}

	if nodeAddr == "" {
		return raw, fmt.Errorf("%w: %s", ErrHostNotFound, owner)
	}

	streamFlow, err := fb.tr.dialFlow(ctx, nodeAddr, name, bidirectional)
	if err != nil {
		return raw, fmt.Errorf("%w: %w", ErrDialFailed, err)
	}

	raw.RawSender = flow.RemoteSender{SendStream: streamFlow.SendStream}
	if bidirectional {
		if streamFlow.ReceiveStream == nil {
			panic("asked bidirectional but returned a nil receive stream")
		}
		raw.RawReceiver = flow.RemoteReceiver{ReceiveStream: streamFlow.ReceiveStream}
	}

	return raw, nil
}

func (fb *Fabric) ResolveEndpoint(ctx context.Context, req ResolveEndpointRequest) (*ResolveEndpointQuery, error) {
	if !ValidateEndpointName(req.EndpointName) {
		return nil, ErrNameInvalid
	}

	fb.lk.Lock()
	shutdown := fb.shutdown
	fb.lk.Unlock()
	if shutdown {
		return nil, ErrFabricClosed
	}

	query := &grintav1alpha1.NameOwnershipQuery{}
	query.SetEndpointName(req.EndpointName)

	payload, err := proto.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrQueryInvalid, err)
	}

	// best-effort aligning the timeout of the query
	timeout := 30 * time.Second
	dl, hasDl := ctx.Deadline()
	if hasDl {
		timeout = time.Until(dl)
	}

	res, err := fb.serf.Query("resolve_endpoint", payload, &serf.QueryParam{
		FilterNodes: req.NodeNames,
		Timeout:     timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrQueryInvalid, err)
	}

	expected := len(req.NodeNames)
	if expected == 0 {
		expected = fb.serf.Memberlist().NumMembers()
	}

	rq := &ResolveEndpointQuery{
		q:          res,
		votes:      make(map[string]*grintav1alpha1.NameOwnershipResponse),
		expected:   expected,
		responseCh: make(chan *ResolveEndpointResponse, 1),
	}

	go func() {
		for {
			nodeResp, ok := <-res.ResponseCh()
			if !ok {
				break
			}
			voter := nodeResp.From
			vote := &grintav1alpha1.NameOwnershipResponse{}
			err := proto.Unmarshal(nodeResp.Payload, vote)
			if err != nil {
				fb.logger.Warn("invalid message during query", LabelError.L(err))
				continue
			}

			if req.NoConsensus && vote.HasClaim() {
				claim := vote.GetClaim()
				if claim.GetMode() == grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM {
					rq.lk.Lock()
					if rq.closed {
						rq.lk.Unlock()
						return
					}
					rq.responseCh <- &ResolveEndpointResponse{
						claim:         claim,
						Error:         nil,
						Host:          claim.GetNodeName(),
						ExpectedVotes: expected,
						Participation: float64(len(rq.votes)) / float64(rq.expected),
					}
					rq.closed = true
					close(rq.responseCh)
					res.Close()
					rq.lk.Unlock()
				}
			}

			rq.votes[voter] = vote
			if len(rq.votes) >= expected {
				res.Close()
				break
			}
		}

		participation := float64(len(rq.votes)) / float64(rq.expected)
		response := &ResolveEndpointResponse{
			ExpectedVotes: rq.expected,
			Participation: participation,
		}

		if req.NoConsensus {
			response.Error = ErrNameResolution
			rq.lk.Lock()
			if rq.closed {
				rq.lk.Unlock()
				return
			}
			rq.responseCh <- response
			rq.closed = true
			close(rq.responseCh)
			rq.lk.Unlock()
			return
		}

		requiredParticipation := req.RequiredParticipation
		if requiredParticipation == 0.0 {
			requiredParticipation = 0.67
		}

		if participation < requiredParticipation {
			response.Error = ErrNotEnoughParticipation
			rq.lk.Lock()
			if rq.closed {
				rq.lk.Unlock()
				return
			}
			rq.responseCh <- response
			rq.closed = true
			close(rq.responseCh)
			rq.lk.Unlock()
			return
		}

		var highest *voteVal
		votes := make(map[string]*voteVal)
		noOwnerVote := 0
		for _, vote := range rq.votes {
			if !vote.HasClaim() {
				noOwnerVote++
				continue
			}

			claim := vote.GetClaim()
			claimant := claim.GetNodeName()

			curr, ok := votes[claimant]
			if !ok {
				curr = &voteVal{
					count:      1,
					mostRecent: claim,
				}
				votes[claimant] = curr
			} else {
				curr.count = curr.count + 1
				if claim.GetRev() > curr.mostRecent.GetRev() {
					curr.mostRecent = claim
				}
			}

			if highest == nil || curr.count > highest.count {
				highest = curr
			}
		}

		if highest == nil {
			response.Error = ErrNameResolution
			rq.lk.Lock()
			if rq.closed {
				rq.lk.Unlock()
				return
			}
			rq.responseCh <- response
			rq.closed = true
			close(rq.responseCh)
			rq.lk.Unlock()
			return
		}

		if highest.count+noOwnerVote < len(rq.votes)/2+1 {
			fb.logger.Debug("majority not reach, tie breaking")
			highest = nil
			lowestClaimentName := ""
			for k, vote := range votes {
				if lowestClaimentName == "" || k < lowestClaimentName {
					highest = vote
					lowestClaimentName = k
				}
			}
		}

		response.claim = highest.mostRecent
		response.Host = response.claim.GetNodeName()
		rq.lk.Lock()
		if rq.closed {
			rq.lk.Unlock()
			return
		}
		rq.responseCh <- response
		rq.closed = true
		close(rq.responseCh)
		rq.lk.Unlock()
	}()

	return rq, nil
}

type FabricControlPlane interface {
	// ResolveEndpoint will asynchronously start a consensus-based
	// resolution of an `Endpoint`'s host.
	ResolveEndpoint(ctx context.Context, req ResolveEndpointRequest) (*ResolveEndpointQuery, error)
}

type ResolveEndpointRequest struct {
	// EndpointName is the name of the endpoint to resolve.
	EndpointName string

	// NodeNames allows you, when set, to limit the query to specific nodes.
	NodeNames []string

	// NoConsensus if we want the first claimant to win.
	NoConsensus bool

	// RequiredParticipation invalidate the query if not enough nodes answer.
	// Default to 0.67.
	RequiredParticipation float64
}

type ResolveEndpointQuery struct {
	q          *serf.QueryResponse
	votes      map[string]*grintav1alpha1.NameOwnershipResponse
	expected   int
	responseCh chan *ResolveEndpointResponse
	closed     bool
	lk         sync.Mutex
}

type ResolveEndpointResponse struct {
	// Error will be set if any error happened during the query.
	Error error

	// Host is where the `Endpoint` lives. It is empty if not found.
	Host string

	// ExpectedVotes is the expected participation.
	ExpectedVotes int

	// Participation is a rate of how many votes were received vs. how many
	// were expected.
	Participation float64

	claim *grintav1alpha1.NameClaim
}

type voteVal struct {
	count      int
	mostRecent *grintav1alpha1.NameClaim
}

func (rep *ResolveEndpointQuery) ResponseCh() <-chan *ResolveEndpointResponse {
	return rep.responseCh
}

func (rep *ResolveEndpointQuery) Close() {
	rep.lk.Lock()
	defer rep.lk.Unlock()
	if rep.closed {
		return
	}
	rep.closed = true
	close(rep.responseCh)
	rep.q.Close()
}

func (rep *ResolveEndpointQuery) Deadline() time.Time {
	return rep.q.Deadline()
}

func (rep *ResolveEndpointQuery) Finished() bool {
	rep.lk.Lock()
	defer rep.lk.Unlock()
	return rep.closed
}

func ValidateEndpointName(name string) bool {
	return !InvalidEndpointName.MatchString(name) && len(name) <= MaxEndpointLength
}

func ValidateCloseReason(reason string) bool {
	return len([]byte(reason)) <= MaxReasonBytes
}
