package grinta

import (
	"log/slog"

	"github.com/hashicorp/go-metrics"
)

var (
	MetricDByte = []string{"grinta", "datagram", "bytes"}
	MetricDErr  = []string{"grinta", "datagram", "errors"}

	MetricSCount = []string{"grinta", "stream", "count"}
	MetricSErr   = []string{"grinta", "stream", "errors"}

	MetricConnCount = []string{"grinta", "connection", "count"}
	MetricConnErr   = []string{"grinta", "connection", "errors"}
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
