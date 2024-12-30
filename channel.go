package grinta

import (
	"context"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
	grintav1alpha1 "github.com/raskyld/grinta/gen/grinta/v1alpha1"
)

// Endpoint is resolvable on a `Fabric` and allows the owner to
// `Accept` inbound `Channel`.
type Endpoint interface {
	Name() string
	Accept(context.Context) (Chan, error)
	Close(cause ClosedError) error
}

// Chan is the equivalent of Go built-in chan, with the following
// additions:
//
// * They are bidirectional.
// * Their two end can be in different process,
//   - If both end are in the same process, it falls back to native channel;
//   - If they are in different processes, the messages are `Marshal()`-ed
//     over the `Fabric`.
type Chan interface{}

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

type endpoint struct {
	fb          *Fabric
	name        string
	closed      bool
	closeReason ClosedError
	closeCh     chan struct{}
	lock        sync.Mutex
}

func (ep *endpoint) Name() string {
	return ep.name
}

func (ep *endpoint) Accept(context.Context) (Chan, error) {
	return nil, nil
}

func (ep *endpoint) Close(cause ClosedError) error {
	ep.lock.Lock()
	if ep.closed {
		// no-op
		ep.lock.Unlock()
		return nil
	}

	ep.closed = true
	ep.closeReason = cause
	ep.lock.Unlock()
	close(ep.closeCh)
	return nil
}
