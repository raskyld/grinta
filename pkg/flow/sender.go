package flow

import (
	"context"
	"sync"

	"github.com/quic-go/quic-go"
)

// RawSender is a non-thread safe and blocking flow which should
// only be used by power users.
//
// Methods MUST NOT be called concurrently.
//
// Wrap it in a [Sender] using [NewSender] for a better DX.
type RawSender interface {
	Send(Encoder, interface{}) error
	Close() error
}

// Encoder can encode messages on a `quic.Stream`.
// It is supposed to return an error only when a final error is
// encountered.
type Encoder interface {
	Encode(quic.SendStream, interface{}) error
	ProcessLocal(interface{}) (interface{}, error)
}

type Clonable interface {
	Clone() interface{}
}

// Sender is a thread-safe and typed flow writer.
type Sender[T any] struct {
	raw RawSender
	enc Encoder

	writeCh    chan T
	closeCh    chan struct{}
	mainLoopWg sync.WaitGroup

	// handle Close sync.
	writer sync.WaitGroup
	err    error
	lk     sync.Mutex
}

func NewSender[T any](raw RawSender, enc Encoder, bufferSize uint) *Sender[T] {
	w := &Sender[T]{
		raw: raw,
		enc: enc,

		writeCh: make(chan T, bufferSize),
		closeCh: make(chan struct{}),
	}

	w.mainLoopWg.Add(1)
	go w.run()

	return w
}

func (w *Sender[T]) Send(ctx context.Context, msg T) error {
	w.lk.Lock()
	if w.err != nil {
		w.lk.Unlock()
		return w.err
	}
	w.writer.Add(1)
	defer w.writer.Done()
	w.lk.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.closeCh:
		return w.err
	case w.writeCh <- msg:
	}

	return nil
}

func (w *Sender[T]) Close() error {
	err := w.closeWith(ErrFlowClosed)
	w.mainLoopWg.Wait()
	return err
}

func (w *Sender[T]) closeWith(cause error) error {
	w.lk.Lock()
	if w.err != nil {
		w.lk.Unlock()
		return nil
	}
	w.err = cause
	close(w.closeCh)
	w.lk.Unlock()
	w.writer.Wait()
	close(w.writeCh)
	return w.raw.Close()
}

func (w *Sender[T]) run() {
	defer w.mainLoopWg.Done()
	for {
		msg, ok := <-w.writeCh
		if !ok {
			return
		}

		err := w.raw.Send(w.enc, msg)
		if err != nil {
			_ = w.closeWith(err)
			return
		}
	}
}
