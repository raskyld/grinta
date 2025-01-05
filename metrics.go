package grinta

import (
	"log/slog"

	"github.com/hashicorp/go-metrics"
)

var (
	// MetricDByte is the amount of bytes I/O made as
	// QUIC datagrams.
	MetricDByte = []string{"grinta", "datagram", "bytes"}

	// MetricDErr is the amount of errors emitted while doing I/O as
	// QUIC datagrams.
	MetricDErr = []string{"grinta", "datagram", "errors"}

	// MetricSCount is the amount of QUIC streams established.
	MetricSCount = []string{"grinta", "stream", "established", "count"}

	// MetricSErr is the amount of errors emitted while
	// establishing QUIC streams.
	MetricSErr = []string{"grinta", "stream", "errors"}

	// MetricConnCount is the amount of QUIC connections established.
	MetricConnCount = []string{"grinta", "connection", "established", "count"}

	// MetricConnErr is the amount of errors emitted while
	// establishing QUIC connections.
	MetricConnErr = []string{"grinta", "connection", "errors"}
)

type TelemetryLabel string

const (
	LabelError        TelemetryLabel = "error"
	LabelPeerAddr     TelemetryLabel = "peer_addr"
	LabelPeerName     TelemetryLabel = "peer_name"
	LabelStreamMode   TelemetryLabel = "stream_mode"
	LabelStreamID     TelemetryLabel = "stream_id"
	LabelPerspective  TelemetryLabel = "perspective"
	LabelEndpointName TelemetryLabel = "endpoint_name"
	LabelDuration     TelemetryLabel = "duration"
)

func (lab TelemetryLabel) M(val string) metrics.Label {
	return metrics.Label{Name: string(lab), Value: val}
}

func (lab TelemetryLabel) L(val any) slog.Attr {
	return slog.Attr{
		Key:   string(lab),
		Value: slog.AnyValue(val),
	}
}
