package goroutinettes

import (
	"github.com/hashicorp/go-metrics"
	"github.com/hashicorp/memberlist"
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

var (
	MLabelError      = "error"
	MLabelPeerAddr   = "peer_addr"
	MLabelPeerName   = "peer_name"
	MLabelStreamMode = "stream_mode"
)

func LabelsForAddr(addr memberlist.Address) []metrics.Label {
	return []metrics.Label{
		metrics.Label{Name: MLabelPeerAddr, Value: addr.Addr},
		metrics.Label{Name: MLabelPeerName, Value: addr.Name},
	}
}
