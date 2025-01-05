package grinta

import (
	"crypto/tls"
	"log/slog"
	"time"

	leg_metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/go-metrics"
	"github.com/hashicorp/serf/serf"
)

type config struct {
	serfCfg      *serf.Config
	trCfg        TransportConfig
	logHandler   slog.Handler
	metricLabels []metrics.Label
	neighbours   []string
}

// Option to pass to `Create`
type Option func(*config) error

// WithListenOn specifies which UDP interface must be used by the GRINTA
// protocol.
func WithListenOn(addr string, port int) Option {
	return func(c *config) error {
		c.serfCfg.MemberlistConfig.BindAddr = addr
		c.serfCfg.MemberlistConfig.BindPort = port
		c.serfCfg.MemberlistConfig.AdvertiseAddr = addr
		c.serfCfg.MemberlistConfig.AdvertisePort = port
		return nil
	}
}

// WithAdvertise specifies which UDP interface we must advertise to
// other nodes. It defaults to Bind* values.
func WithAdvertise(addr string, port int) Option {
	return func(c *config) error {
		c.serfCfg.MemberlistConfig.AdvertiseAddr = addr
		c.serfCfg.MemberlistConfig.AdvertisePort = port
		return nil
	}
}

// WithLog specifies which `slog.Handler` to use.
func WithLog(handler slog.Handler) Option {
	return func(c *config) error {
		c.logHandler = handler
		c.trCfg.LogHandler = handler
		return nil
	}
}

// WithNodeName specifies which node name should be exposed to other
// peers when joining the cluster. For a well-behaving cluster, the name
// MUST be unique.
func WithNodeName(hostname string) Option {
	return func(c *config) error {
		if hostname != "" {
			c.serfCfg.NodeName = hostname
		}
		return nil
	}
}

// WithNodeLabels adds labels to tag your node.
func WithNodeLabels(labels map[string]string) Option {
	return func(c *config) error {
		c.serfCfg.Tags = labels
		return nil
	}
}

// WithMetricLabels adds static labels to all metrics produced by the Fabric.
func WithMetricLabels(labels []metrics.Label) Option {
	return func(c *config) error {
		c.metricLabels = labels
		c.trCfg.MetricLabels = labels

		// TODO(raskyld): Wait for the buildflag to always use the
		// hashicorp version so we don't need to do the translation.
		c.serfCfg.MetricLabels = make([]leg_metrics.Label, len(labels))
		for i, label := range labels {
			c.serfCfg.MetricLabels[i] = leg_metrics.Label{
				Name:  label.Name,
				Value: label.Value,
			}
		}
		return nil
	}
}

// WithTlsConfig set the `tls.Config` which should be used by the
// GRINTA protocol. It is REALLY important that you use mTLS in production
// since that's the only way to secure your `Fabric` at this time.
func WithTlsConfig(tlsConf *tls.Config) Option {
	return func(c *config) error {
		if tlsConf == nil {
			return ErrNoTLSConfig
		}
		c.trCfg.TlsConfig = tlsConf.Clone()
		return nil
	}
}

// WithHintMaxFlows gives an indication of the maximum number of `Flow` you
// intend to open concurrently with any peer.
//
// It is important that you stay under this number since the GRINTA protocol
// would fail to open new `Flow`, which would likely disrupt your application.
func WithHintMaxFlows(hint int64) Option {
	return func(c *config) error {
		if hint == 0 {
			hint = 10000
		}
		c.trCfg.HintMaxFlows = hint
		return nil
	}
}

// WithMetricSink allows you to chose how to collect the metrics emitted by
// your `Fabric`.
func WithMetricSink(ms metrics.MetricSink) Option {
	return func(c *config) error {
		if ms == nil {
			ms = &metrics.BlackholeSink{}
		}
		c.trCfg.MetricSink = ms
		return nil
	}
}

// WithDialTimeout controls how much time we are willing to wait for a
// remote node to answer.
func WithDialTimeout(timeout time.Duration) Option {
	return func(c *config) error {
		if timeout == 0 {
			timeout = 30 * time.Second
		}
		c.trCfg.DialTimeout = timeout
		return nil
	}
}

// WithGracePeriod controls how much time we wait on Shutdown for UDP
// buffers to flush.
func WithGracePeriod(period time.Duration) Option {
	return func(c *config) error {
		if period == 0 {
			period = 4 * time.Second
		}
		c.serfCfg.LeavePropagateDelay = period
		return nil
	}
}

// WithNeighbours controls which peers are tried initially to Join the
// cluster.
func WithNeighbours(neighbours []string) Option {
	return func(c *config) error {
		c.neighbours = neighbours
		return nil
	}
}
