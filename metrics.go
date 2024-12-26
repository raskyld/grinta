package goroutinettes

var (
	// MetricGrintaUdpRx represents how much bytes have been received
	// as QUIC datagrams.
	MetricGrintaDatagramRx          = []string{"grinta", "gossip", "datagram", "in", "bytes"}
	MetricGrintaGossipStreamInCount = []string{"grinta", "gossip", "stream", "in", "count"}
	MetricGrintaFlowStreamInCount   = []string{"grinta", "flow", "stream", "in", "count"}
)
