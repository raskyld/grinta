package grinta

import (
	"fmt"
	"log/slog"

	leg_metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/go-metrics"
	"github.com/hashicorp/memberlist"
)

type config struct {
	mlCfg        *memberlist.Config
	logHandler   slog.Handler
	logLevel     slog.Level
	metricLabels []metrics.Label
}

// Option to pass to `Create`
type Option func(*config) error

// WithSecretKey adds a secret key to the initial keyring used to communicate in
// the `Fabric`.
func WithSecretKey(key []byte) Option {
	return func(c *config) error {
		err := c.mlCfg.Keyring.AddKey(key)
		if err != nil {
			return fmt.Errorf("fabric: provided secret key is invalid: %w", err)
		}

		return nil
	}
}

// WithListenOn specifies the interface and port to use for the GRINTA
// protocol.
//
// (N.B.: Both UDP and TCP ports must be available.)
func WithListenOn(addr string, port int) Option {
	return func(c *config) error {
		c.mlCfg.BindAddr = addr
		c.mlCfg.BindPort = port
		return nil
	}
}

// WithLog specifies which `slog.Handler` to use for logging.
func WithLog(handler slog.Handler) Option {
	return func(c *config) error {
		c.logHandler = handler
		return nil
	}
}

// WithHostname specifies which `slog.Handler` to use for logging.
func WithHostname(hostname string) Option {
	return func(c *config) error {
		if hostname != "" {
			c.mlCfg.Name = hostname
		}
		return nil
	}
}

// WithLogLevel specifies the `slog.Level` to use.
func WithLogLevel(level slog.Level) Option {
	return func(c *config) error {
		c.logLevel = level
		return nil
	}
}

// WithMetricLabels adds static labels to all metrics
// produced by the Fabric.
func WithMetricLabels(labels []metrics.Label) Option {
	return func(c *config) error {
		c.metricLabels = labels
		// TODO(raskyld): Wait for the buildflag to always use the
		// hashicorp version so we don't need to do the translation.
		c.mlCfg.MetricLabels = make([]leg_metrics.Label, len(labels))
		for i, label := range labels {
			c.mlCfg.MetricLabels[i] = leg_metrics.Label{
				Name:  label.Name,
				Value: label.Value,
			}
		}
		return nil
	}
}
