package goroutinettes

import (
	"net"

	"github.com/quic-go/quic-go"
	grintav1alpha1 "github.com/raskyld/goroutinettes/gen/grinta/v1alpha1"
)

type streamWrapper struct {
	mode        grintav1alpha1.StreamMode
	localAddr   net.Addr
	remoteAddr  net.Addr
	source      string
	destination string

	// NB(raskyld): It is not clear from the go-quic docs and interface comments
	// whether the stream is thread-safe, it states that Close MUST NOT
	// be called concurrently with write, but looking at the implementation,
	// it does use a mutex to sync Write/Close/Read operations, so I don't
	// think we need to make it thread-safe ourselves.
	// Furthermore, even though there is few (to none) reasons to call
	// Write() and Read() concurrently, they have a 1-len channel to protect
	// against concurrent uses.
	quic.Stream
}

func (gs *streamWrapper) LocalAddr() net.Addr {
	return gs.localAddr
}

func (gs *streamWrapper) RemoteAddr() net.Addr {
	return gs.remoteAddr
}

func (gs *streamWrapper) garbageCollector(closer <-chan struct{}) {
	select {
	case <-gs.Context().Done():
		// already closed, can't clean-up.
	case <-closer:
		// graceful termination requested.
		// TODO(raskyld): contribute to go-quic to handle gracefully draining
		// the whole structured concurrency tree until a deadline or all connections
		// and streams have transmitted their frames.
		gs.Close()
	}
}
