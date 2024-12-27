package goroutinettes

import (
	"log/slog"

	"github.com/hashicorp/go-metrics"
)

var (
	// MetricGrintaUdpRx represents how much bytes have been received
	// as QUIC datagrams.
	MetricGrintaDatagramInBytes        = []string{"grinta", "datagram", "in", "bytes"}
	MetricGrintaDatagramInErrorCount   = []string{"grinta", "datagram", "in", "error", "count"}
	MetricGrintaDatagramOutBytes       = []string{"grinta", "datagram", "out", "bytes"}
	MetricGrintaDatagramOutErrorCount  = []string{"grinta", "datagram", "out", "error", "count"}
	MetricGrintaStreamEstInCount       = []string{"grinta", "stream", "establishment", "in", "count"}
	MetricGrintaStreamEstInErrorCount  = []string{"grinta", "stream", "establishment", "in", "error", "count"}
	MetricGrintaStreamEstOutCount      = []string{"grinta", "stream", "establishment", "out", "count"}
	MetricGrintaStreamEstOutErrorCount = []string{"grinta", "stream", "establishment", "out", "error", "count"}
	MetricGrintaUDPBufferSizeBytes     = []string{"grinta", "udp", "buffer", "size", "bytes"}
	MetricGrintaConnErrorCount         = []string{"grinta", "connection", "error", "count"}
	MetricGrintaConnEstCount           = []string{"grinta", "connection", "established", "count"}
	MetricGrintaHostNameChanges        = []string{"grinta", "host", "name", "changes"}
	MetricGrintaHostAddrChanges        = []string{"grinta", "host", "addr", "changes"}
	MetricGrintaHostConflictsCount     = []string{"grinta", "host", "name", "conflicts", "count"}
)

type TelemetryLabel string

var (
	LabelError      TelemetryLabel = "error"
	LabelPeerAddr   TelemetryLabel = "peer_addr"
	LabelPeerName   TelemetryLabel = "peer_name"
	LabelStreamMode TelemetryLabel = "stream_mode"
	LabelStreamID   TelemetryLabel = "stream_id"
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
