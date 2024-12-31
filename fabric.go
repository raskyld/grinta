package grinta

import (
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/hashicorp/serf/serf"
	grintav1alpha1 "github.com/raskyld/grinta/gen/grinta/v1alpha1"
	"google.golang.org/protobuf/proto"
)

const MaxEndpointLength = 128
const MaxReasonBytes = 255

var InvalidEndpointName = regexp.MustCompile(`[^A-Za-z0-9\-\.]+`)

type Fabric struct {
	config config
	serf   *serf.Serf
	tr     *Transport
	logger *slog.Logger
	dir    *nameDirectory

	eventCh    chan serf.Event
	shutdownCh chan struct{}
}

func Create(opts ...Option) (*Fabric, error) {
	fb := &Fabric{
		eventCh:    make(chan serf.Event, 512),
		shutdownCh: make(chan struct{}, 1),
	}

	// Fine-tune Serf config.
	fb.config.serfCfg = serf.DefaultConfig()
	// We will wait for QUIC buffers to flush anyway.
	fb.config.serfCfg.LeavePropagateDelay = 1 * time.Second
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

	// Logging implementations.
	if fb.config.logHandler != nil {
		fb.logger = slog.New(fb.config.logHandler)
		fb.config.serfCfg.Logger = slog.NewLogLogger(fb.config.logHandler, slog.LevelDebug)
	} else {
		fb.logger = slog.Default()
		fb.config.serfCfg.Logger = slog.NewLogLogger(slog.Default().Handler(), slog.LevelDebug)
	}
	fb.config.serfCfg.MemberlistConfig.Logger = fb.config.serfCfg.Logger

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

	// Handle cluster events.
	go fb.handleEvents()

	// Create our name dir.
	fb.dir = newNameDir(fb.logger, fb, fb.config.serfCfg.NodeName)

	return fb, nil
}

func (fb *Fabric) JoinCluster() error {
	if len(fb.config.neighbours) > 0 {
		joined, err := fb.serf.Join(fb.config.neighbours, true)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrJoinCluster, err)
		}
		fb.logger.Info("joined cluster")
		if len(fb.config.neighbours) != joined {
			fb.logger.Warn(
				"have not succeeded joining all neighbours",
				"joined", joined,
				"expected", len(fb.config.neighbours),
			)
		}
	} else {
		fb.logger.Warn("no neighbours specified, cannot join cluster")
	}
	return nil
}

func (fb *Fabric) Topology() []serf.Member {
	return fb.serf.Members()
}

func (fb *Fabric) Shutdown() error {
	fb.serf.Leave()
	fb.serf.Shutdown()
	<-fb.serf.ShutdownCh()
	fb.dir.close()
	close(fb.shutdownCh)
	return nil
}

func (fb *Fabric) handleEvents() {
	for {
		var event serf.Event
		select {
		case event = <-fb.eventCh:
		case <-fb.shutdownCh:
			return
		}

		fb.logger.Debug("event received", "event_type", event.EventType().String(), "event", event.String())
		switch event := event.(type) {
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
		}
	}
}

func (fb *Fabric) CreateEndpoint(name string) (Endpoint, error) {
	if !ValidateEndpointName(name) {
		return nil, ErrNameInvalid
	}

	record := &grintav1alpha1.NameClaim{}
	record.SetNodeName(fb.config.serfCfg.NodeName)
	record.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)
	record.SetEndpointName(name)

	payload, err := proto.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFabricInvalidFrame, err)
	}

	err = fb.dir.record(record, true)
	if err != nil {
		return nil, err
	}

	err = fb.serf.UserEvent("name_record", payload, true)
	if err != nil {
		return nil, err
	}

	return &endpoint{
		fb:      fb,
		name:    name,
		closeCh: make(chan struct{}, 1),
	}, nil
}

func (fb *Fabric) ResolveEndpoint(req ResolveEndpointRequest) (*ResolveEndpointQuery, error) {
	if !ValidateEndpointName(req.EndpointName) {
		return nil, ErrNameInvalid
	}

	query := &grintav1alpha1.NameOwnershipQuery{}
	query.SetEndpointName(req.EndpointName)

	payload, err := proto.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrQueryInvalid, err)
	}

	res, err := fb.serf.Query("resolve_endpoint", payload, &serf.QueryParam{
		FilterNodes: req.NodeNames,
		Timeout:     req.Timeout,
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
			nodeResp, closed := <-res.ResponseCh()
			if closed {
				break
			}
			voter := nodeResp.From
			vote := &grintav1alpha1.NameOwnershipResponse{}
			err := proto.Unmarshal(nodeResp.Payload, vote)
			if err != nil {
				fb.logger.Warn("invalid message during query", LabelError.L(err))
				continue
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
	ResolveEndpoint(req ResolveEndpointRequest) (*ResolveEndpointQuery, error)
}

type ResolveEndpointRequest struct {
	// EndpointName is the name of the endpoint to resolve.
	EndpointName string

	// NodeNames allows you, when set, to limit the query to specific nodes.
	NodeNames []string

	// Timeout for the request to finish
	Timeout time.Duration

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
