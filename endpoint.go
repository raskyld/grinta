package grinta

import (
	"context"
	"io"
	"sync"
)

// Endpoint is resolvable on a `Fabric` and allows the owner to
// `Accept` inbound `Flow`.
type Endpoint interface {
	Name() string
	Accept(context.Context) (Flow, error)
	io.Closer
}

var _ Endpoint = (*endpoint)(nil)

type endpoint struct {
	name string

	closed  bool
	closeCh chan struct{}
	lk      sync.Mutex

	epGC   chan<- *endpoint
	flowCh chan Flow
}

func newEndpoint(name string, gc chan<- *endpoint) *endpoint {
	return &endpoint{
		epGC:    gc,
		name:    name,
		flowCh:  make(chan Flow),
		closeCh: make(chan struct{}, 1),
	}
}

func (ep *endpoint) Name() string {
	return ep.name
}

func (ep *endpoint) Accept(ctx context.Context) (Flow, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-ep.closeCh:
		return nil, ErrEndpointClosed
	case inboundChan := <-ep.flowCh:
		if inboundChan == nil {
			return nil, ErrEndpointClosed
		}
		return inboundChan, nil
	}
}

func (ep *endpoint) Close() error {
	ep.lk.Lock()
	defer ep.lk.Unlock()
	if ep.closed {
		return nil
	}

	ep.closed = true
	ep.epGC <- ep
	close(ep.flowCh)
	return nil
}

func (ep *endpoint) close() error {
	ep.lk.Lock()
	defer ep.lk.Unlock()
	if ep.closed {
		return nil
	}

	ep.closed = true
	close(ep.flowCh)
	return nil
}
