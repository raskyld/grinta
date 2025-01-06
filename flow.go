package grinta

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	grintav1alpha1 "github.com/raskyld/grinta/gen/grinta/v1alpha1"
	"google.golang.org/protobuf/encoding/protowire"
)

// RawFlow is the equivalent of Go built-in chan, with the following
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
type RawFlow interface {
	RawFlowReader
	RawFlowWriter
	io.Closer
}

var _ RawFlow = (*localFlow)(nil)
var _ RawFlow = (*remoteFlow)(nil)

type RawFlowReader interface {
	ReadRaw(ctx context.Context) (interface{}, error)
	CloseRead()
}

type RawFlowWriter interface {
	WriteRaw(ctx context.Context, item Marshaler) error
	CloseWrite()
}

type Marshaler interface {
	Marshal() ([]byte, error)
}

type Unmarshaler interface {
	Unmarshal(buf []byte) error
}

type Clonable interface {
	Clone() interface{}
}

type Flow[W Marshaler, R Unmarshaler] struct {
	inner RawFlow
	FlowWriter[W]
	FlowReader[R]
}

func NewFlow[W Marshaler, R Unmarshaler](raw RawFlow) Flow[W, R] {
	return Flow[W, R]{
		inner:      raw,
		FlowWriter: FlowWriter[W]{RawFlowWriter: raw},
		FlowReader: FlowReader[R]{RawFlowReader: raw},
	}
}

func (fl Flow[W, R]) Close() error {
	return fl.inner.Close()
}

type FlowWriter[W Marshaler] struct {
	RawFlowWriter
}

func (fl FlowWriter[W]) Write(ctx context.Context, item W) error {
	return fl.RawFlowWriter.WriteRaw(ctx, item)
}

func (fl FlowWriter[W]) WriteRaw(ctx context.Context, item Marshaler) error {
	toWrite, ok := item.(W)
	if !ok {
		return ErrFlowTypeMismatch
	}
	return fl.RawFlowWriter.WriteRaw(ctx, toWrite)
}

type FlowReader[R Unmarshaler] struct {
	RawFlowReader
}

func (fl FlowReader[R]) Read(ctx context.Context) (result R, resultErr error) {
	read, err := fl.RawFlowReader.ReadRaw(ctx)
	if err != nil {
		resultErr = err
		return
	}
	var ok bool
	result, ok = read.(R)
	if ok {
		return
	}
	var buf []byte
	buf, ok = read.([]byte)
	if !ok {
		resultErr = fmt.Errorf(
			"%w: read value is neither %s nor []byte",
			ErrFlowTypeMismatch,
			reflect.TypeFor[R]().Name(),
		)
		return
	}
	resultErr = result.Unmarshal(buf)
	return
}

type localFlow struct {
	w          chan<- interface{}
	r          <-chan interface{}
	wCloseCh   chan struct{}
	wCloseOnce sync.Once
}

func newLocalChan() (client *localFlow, server *localFlow) {
	outbound := make(chan interface{})
	inbound := make(chan interface{})
	client = &localFlow{
		w: outbound,
		r: inbound,
	}
	server = &localFlow{
		w: inbound,
		r: outbound,
	}
	return
}

func (lc *localFlow) ReadRaw(ctx context.Context) (interface{}, error) {
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

func (lc *localFlow) CloseRead() {}

func (lc *localFlow) WriteRaw(ctx context.Context, item Marshaler) error {
	var toWrite interface{}
	clonable, ok := item.(Clonable)
	if ok {
		toWrite = clonable
	} else {
		buf, err := item.Marshal()
		if err != nil {
			return err
		}
		toWrite = buf
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-lc.wCloseCh:
		return ErrFlowClosed
	case lc.w <- toWrite:
		return nil
	}
}

func (lc *localFlow) CloseWrite() {
	lc.wCloseOnce.Do(func() {
		close(lc.wCloseCh)
	})
}

func (lc *localFlow) Close() error {
	lc.CloseWrite()
	return nil
}

type remoteFlow struct {
	mode        grintav1alpha1.StreamMode
	localAddr   net.Addr
	remoteAddr  net.Addr
	destination string
	buf         *bufio.Reader

	quic.Stream
}

func newRemoteFlow(
	stream quic.Stream,
	localAddr, remoteAddr net.Addr,
	mode grintav1alpha1.StreamMode,
	dest string,
) *remoteFlow {
	return &remoteFlow{
		mode:        mode,
		localAddr:   localAddr,
		remoteAddr:  remoteAddr,
		destination: dest,
		Stream:      stream,
		buf:         bufio.NewReader(stream),
	}
}

func (rc *remoteFlow) LocalAddr() net.Addr {
	return rc.localAddr
}

func (rc *remoteFlow) RemoteAddr() net.Addr {
	return rc.remoteAddr
}

func (rc *remoteFlow) ReadRaw(ctx context.Context) (interface{}, error) {
	dl, hasDl := ctx.Deadline()
	if hasDl {
		rc.SetReadDeadline(dl)
		defer func() { rc.SetReadDeadline(time.Time{}) }()
	}

	var nextSize uint64
	var byteRead int
	for byteRead == 0 {
		buf, err := rc.buf.Peek(binary.MaxVarintLen64)
		if serr := rc.Context().Err(); serr != nil {
			return nil, serr
		}
		if serr := ctx.Err(); serr != nil {
			return nil, serr
		}
		if err != nil {
			var streamErr *quic.StreamError
			if errors.As(err, &streamErr) {
				return nil, err
			}
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
		if serr := rc.Context().Err(); serr != nil {
			return nil, serr
		}
		if serr := ctx.Err(); serr != nil {
			return nil, serr
		}
		if err != nil {
			var streamErr *quic.StreamError
			if errors.As(err, &streamErr) {
				return nil, err
			}
		} else {
			buf = make([]byte, nextSize)
			copy(buf, peeked[byteRead:])
		}
	}

	rc.buf.Discard(byteRead + int(nextSize))
	return buf, nil
}

func (rc *remoteFlow) CloseRead() {
	rc.CancelRead(QErrStreamClosed)
}

func (rc *remoteFlow) WriteRaw(ctx context.Context, item Marshaler) error {
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
		rc.SetWriteDeadline(dl)
		defer func() { rc.SetReadDeadline(time.Time{}) }()
	}
	_, err = rc.Write(buf)
	return err
}

func (rc *remoteFlow) CloseWrite() {
	rc.Stream.Close()
}

func (rc *remoteFlow) Close() error {
	rc.CloseWrite()
	rc.CloseRead()
	return nil
}
