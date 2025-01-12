package grinta

import (
	"context"
	"io"
	"sync"

	"github.com/raskyld/grinta/pkg/flow"
)

// Endpoint is resolvable on a `Fabric` and allows the owner to
// `Accept` inbound `Flow`.
type Endpoint interface {
	Name() string
	// Accept will block until the context is canceled or a flow establishment
	// request is received, it always returns a `RawReceiver` and
	// MAY return a non-nil `RawSender` in case the flow is bidirectional.
	Accept(context.Context) (flow.RawReceiver, flow.RawSender, error)
	io.Closer
}

var _ Endpoint = (*endpoint)(nil)

type endpoint struct {
	name string

	closed  bool
	closeCh chan struct{}
	writers sync.WaitGroup
	lk      sync.Mutex

	epGC   func(*endpoint)
	flowCh chan flow.Raw
}

func newEndpoint(name string, garbageCollectorCallback func(*endpoint)) *endpoint {
	return &endpoint{
		epGC:    garbageCollectorCallback,
		name:    name,
		flowCh:  make(chan flow.Raw, 64),
		closeCh: make(chan struct{}, 1),
	}
}

func (ep *endpoint) Name() string {
	return ep.name
}

func (ep *endpoint) Accept(ctx context.Context) (flow.RawReceiver, flow.RawSender, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case inboundChan := <-ep.flowCh:
		if inboundChan.RawReceiver == nil {
			return nil, nil, ErrEndpointClosed
		}
		return inboundChan.RawReceiver, inboundChan.RawSender, nil
	}
}

func (ep *endpoint) Close() error {
	return ep.close(false)
}

// since close may be initiated by the Endpoint Garbage Collector,
// we need a way to know if calling the callback is needed.
func (ep *endpoint) close(plannedForGc bool) error {
	ep.lk.Lock()
	defer ep.lk.Unlock()
	if ep.closed {
		return nil
	}

	ep.closed = true
	close(ep.closeCh)
	ep.writers.Wait()
	close(ep.flowCh)

	if !plannedForGc {
		ep.epGC(ep)
	}
	return nil
}
