package flow

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/quic-go/quic-go"
)

// RawWriter is a non-thread safe and blocking flow which should
// only be used by power users.
//
// Methods MUST NOT be called concurrently.
type RawReceiver interface {
	Recv(Decoder) (interface{}, error)
	Close() error
}

// Decodable can decode messages from a `quic.Stream`.
// It is supposed to return an error only when a final error is
// encountered.
type Decoder interface {
	Decode(quic.ReceiveStream) (interface{}, error)
}

// Receiver is a thread-safe and typed flow reader.
type Receiver[T any] struct {
	raw RawReceiver
	dec Decoder

	readCh     chan T
	closeCh    chan struct{}
	mainLoopWg sync.WaitGroup

	// handle Close sync.
	err error
	lk  sync.Mutex
}

func NewReceiver[T any](raw RawReceiver, dec Decoder, bufferSize uint) *Receiver[T] {
	r := &Receiver[T]{
		raw: raw,
		dec: dec,

		readCh:  make(chan T, bufferSize),
		closeCh: make(chan struct{}),
	}

	r.mainLoopWg.Add(1)
	go r.run()

	return r
}

func (r *Receiver[T]) Recv(ctx context.Context) (result T, err error) {
	r.lk.Lock()
	if r.err != nil {
		r.lk.Unlock()
		return result, r.err
	}
	r.lk.Unlock()

	select {
	case <-ctx.Done():
		return result, ctx.Err()
	case elem, ok := <-r.readCh:
		if !ok {
			return result, r.err
		}
		return elem, nil
	}
}

func (r *Receiver[T]) Close() error {
	return r.closeWith(ErrFlowClosed, true)
}

func (r *Receiver[T]) closeWith(cause error, mustWait bool) error {
	r.lk.Lock()
	if r.err != nil {
		r.lk.Unlock()
		return nil
	}
	r.err = cause
	close(r.closeCh)
	err := r.raw.Close()
	r.lk.Unlock()
	if mustWait {
		r.mainLoopWg.Wait()
	}
	close(r.readCh)
	return err
}

func (r *Receiver[T]) run() {
	defer r.mainLoopWg.Done()
	for {
		elem, err := r.raw.Recv(r.dec)
		if err != nil {
			_ = r.closeWith(err, false)
			return
		}

		msg, ok := elem.(T)
		if !ok {
			panic(
				fmt.Sprintf(
					"decoder returned no error, but returned wrong type %s instead of %s",
					reflect.TypeOf(elem).String(),
					reflect.TypeFor[T]().String(),
				),
			)
		}

		select {
		case <-r.closeCh:
			return
		case r.readCh <- msg:
		}
	}
}
