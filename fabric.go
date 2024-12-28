package grinta

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/hashicorp/memberlist"
)

type Fabric struct {
	config config
	ml     *memberlist.Memberlist
	tr     *Transport
	logger *slog.Logger
}

func Create(opts ...Option) (*Fabric, error) {
	fb := &Fabric{}

	mlCfg := memberlist.DefaultWANConfig()
	mlCfg.GossipNodes = 3
	mlCfg.GossipInterval = 350 * time.Millisecond
	fb.config.mlCfg = mlCfg

	for _, opt := range opts {
		err := opt(&fb.config)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidCfg, err)
		}
	}

	if fb.config.logHandler != nil {
		fb.logger = slog.New(fb.config.logHandler)
		fb.config.mlCfg.Logger = slog.NewLogLogger(fb.config.logHandler, slog.LevelDebug)
	} else {
		fb.logger = slog.Default()
		fb.config.mlCfg.Logger = slog.NewLogLogger(slog.Default().Handler(), slog.LevelDebug)
	}

	mlCfg.Events = &gossip{
		logger: fb.logger,
	}

	tr, err := NewTransport(&fb.config.trCfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCfg, err)
	}
	fb.tr = tr

	mlCfg.Transport = tr
	ml, err := memberlist.Create(fb.config.mlCfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCfg, err)
	}
	fb.ml = ml
	return fb, nil
}

func (fb *Fabric) JoinCluster() error {
	if len(fb.config.neighbours) > 0 {
		joined, err := fb.ml.Join(fb.config.neighbours)
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

func (fb *Fabric) Topology() []*memberlist.Node {
	return fb.ml.Members()
}
