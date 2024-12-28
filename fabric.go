package grinta

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/hashicorp/serf/serf"
)

type Fabric struct {
	config     config
	serf       *serf.Serf
	tr         *Transport
	logger     *slog.Logger
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
	}
}
