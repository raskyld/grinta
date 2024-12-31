package grinta

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
	grintav1alpha1 "github.com/raskyld/grinta/gen/grinta/v1alpha1"
	"google.golang.org/protobuf/encoding/protowire"
)

// Endpoint is resolvable on a `Fabric` and allows the owner to
// `Accept` inbound `Channel`.
type Endpoint interface {
	Name() string
	Accept(context.Context) (Chan, error)
	Close(cause ClosedError) error
}

// Chan is the equivalent of Go built-in chan, with the following
// distinctions:
//
// * `Recv` MUST NOT be called concurrently.
// * `Send` MUST NOT be called concurrently.
// * ... but you can call `Recv` and `Send` at the same time.
// * They are bidirectional.
// * Their two end can be in different process,
//   - If both end are in the same process, it falls back to native channel;
//   - If they are in different processes, the messages are `Marshal()`-ed
//     over the `Fabric`.
type Chan interface {
	EndpointName() string
	Recv(ctx context.Context) (interface{}, error)
	Send(ctx context.Context, item Marshaler) error
	Context() context.Context

	io.Closer
}

type Marshaler interface {
	Marshal() ([]byte, error)
}

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

func (stream *streamWrapper) LocalAddr() net.Addr {
	return stream.localAddr
}

func (stream *streamWrapper) RemoteAddr() net.Addr {
	return stream.remoteAddr
}

func (stream *streamWrapper) garbageCollector(closer <-chan struct{}) {
	select {
	case <-stream.Context().Done():
		// already closed, can't clean-up.
	case <-closer:
		// graceful termination requested.
		// TODO(raskyld): contribute to go-quic to handle gracefully draining
		// the whole structured concurrency tree until a deadline or all connections
		// and streams have transmitted their frames.
		stream.Close()
	}
}

type endpoint struct {
	name        string
	closed      bool
	closeReason ClosedError
	lock        sync.Mutex

	epGC chan<- *endpoint
	inCh chan Chan
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
	ep.epGC <- ep
	ep.lock.Unlock()
	return nil
}

type LocalChan struct {
	ep       string
	producer chan<- Marshaler
	consumer <-chan interface{}
	ctx      context.Context
	cancelFn context.CancelFunc
}

func (lc *LocalChan) EndpointName() string {
	return lc.ep
}

func (lc *LocalChan) Recv(ctx context.Context) (interface{}, error) {
	select {
	case <-lc.ctx.Done():
		return nil, lc.ctx.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-lc.consumer:
		return result, nil
	}
}

func (lc *LocalChan) Send(ctx context.Context, item Marshaler) error {
	select {
	case <-lc.ctx.Done():
		return lc.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	case lc.producer <- item:
		return nil
	}
}

func (lc *LocalChan) Context() context.Context {
	return lc.ctx
}

func (lc *LocalChan) Close() error {
	lc.cancelFn()
	close(lc.producer)
	return nil
}

type RemoteChan struct {
	stream *streamWrapper
	buf    *bufio.Reader
}

func (rc *RemoteChan) EndpointName() string {
	return rc.stream.destination
}

func (rc *RemoteChan) Recv(ctx context.Context) (interface{}, error) {
	dl, hasDl := ctx.Deadline()
	if hasDl {
		rc.stream.SetReadDeadline(dl)
	}

	var nextSize uint64
	var byteRead int
	for byteRead == 0 {
		buf, _ := rc.buf.Peek(binary.MaxVarintLen64)
		if serr := rc.stream.Context().Err(); serr != nil {
			return nil, fmt.Errorf("%w: %w", ErrChanClosed, serr)
		}
		if serr := ctx.Err(); serr != nil {
			return nil, serr
		}
		for i, b := range buf {
			if b < 0x80 {
				nextSize, byteRead = protowire.ConsumeVarint(buf[:i+1])
			}
		}
	}

	var buf []byte
	for buf == nil {
		peeked, err := rc.buf.Peek(byteRead + int(nextSize))
		if serr := rc.stream.Context().Err(); serr != nil {
			return nil, fmt.Errorf("%w: %w", ErrChanClosed, serr)
		}
		if serr := ctx.Err(); serr != nil {
			return nil, serr
		}
		if err == nil {
			buf = make([]byte, nextSize)
			copy(buf, peeked[byteRead:])
		}
	}

	rc.buf.Discard(byteRead + int(nextSize))
	return buf, nil
}

func (rc *RemoteChan) Send(ctx context.Context, item Marshaler) error {
	itemBuf, err := item.Marshal()
	if err != nil {
		return err
	}
	varintBuf := protowire.AppendVarint(nil, uint64(len(itemBuf)))
	buf := make([]byte, len(varintBuf)+len(itemBuf))
	copy(buf, varintBuf)
	copy(buf[len(varintBuf):], itemBuf)
	dl, ok := ctx.Deadline()
	if ok {
		rc.stream.SetWriteDeadline(dl)
	}
	_, err = rc.stream.Write(buf)
	return err
}

func (rc *RemoteChan) Context() context.Context {
	return rc.stream.Context()
}

func (rc *RemoteChan) Close() error {
	rc.stream.CancelRead(QErrStreamClosed)
	return rc.stream.Close()
}
