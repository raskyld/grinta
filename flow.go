package grinta

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// Flow is the equivalent of Go built-in chan, with the following
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
type Flow interface {
	FlowReader
	FlowWriter
	io.Closer
}

var _ Flow = (*LocalFlow)(nil)
var _ Flow = (*RemoteFlow)(nil)

type FlowReader interface {
	Read(ctx context.Context) (interface{}, error)
	CloseRead()
}

type FlowWriter interface {
	Write(ctx context.Context, item Marshaler) error
	CloseWrite()
}

type Marshaler interface {
	Marshal() ([]byte, error)
}

type LocalFlow struct {
	w          chan<- interface{}
	r          <-chan interface{}
	wCloseCh   chan struct{}
	wCloseOnce sync.Once
}

func newLocalChan() (client *LocalFlow, server *LocalFlow) {
	outbound := make(chan interface{})
	inbound := make(chan interface{})
	client = &LocalFlow{
		w: outbound,
		r: inbound,
	}
	server = &LocalFlow{
		w: inbound,
		r: outbound,
	}
	return
}

func (lc *LocalFlow) Read(ctx context.Context) (interface{}, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result, ok := <-lc.r:
		if !ok {
			return nil, ErrFlowClosed
		}
		return result, nil
	}
}

func (lc *LocalFlow) CloseRead() {}

func (lc *LocalFlow) Write(ctx context.Context, item Marshaler) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-lc.wCloseCh:
		return ErrFlowClosed
	case lc.w <- item:
		return nil
	}
}

func (lc *LocalFlow) CloseWrite() {
	lc.wCloseOnce.Do(func() {
		close(lc.wCloseCh)
	})
}

func (lc *LocalFlow) Close() error {
	lc.CloseWrite()
	return nil
}

type RemoteFlow struct {
	stream *streamWrapper
	buf    *bufio.Reader
}

func newRemoteChan(stream *streamWrapper) *RemoteFlow {
	return &RemoteFlow{
		stream: stream,
		buf:    bufio.NewReader(stream),
	}
}

func (rc *RemoteFlow) Read(ctx context.Context) (interface{}, error) {
	dl, hasDl := ctx.Deadline()
	if hasDl {
		rc.stream.SetReadDeadline(dl)
		defer func() { rc.stream.SetReadDeadline(time.Time{}) }()
	}

	var nextSize uint64
	var byteRead int
	for byteRead == 0 {
		buf, _ := rc.buf.Peek(binary.MaxVarintLen64)
		if serr := rc.stream.Context().Err(); serr != nil {
			return nil, serr
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
			return nil, serr
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

func (rc *RemoteFlow) CloseRead() {
	rc.stream.CancelRead(QErrStreamClosed)
}

func (rc *RemoteFlow) Write(ctx context.Context, item Marshaler) error {
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
		defer func() { rc.stream.SetReadDeadline(time.Time{}) }()
	}
	_, err = rc.stream.Write(buf)
	return err
}

func (rc *RemoteFlow) CloseWrite() {
	rc.stream.Close()
}

func (rc *RemoteFlow) Close() error {
	rc.CloseWrite()
	rc.CloseRead()
	return nil
}
