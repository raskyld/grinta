package flow

import "sync"

type LocalFlow struct {
	data    chan interface{}
	lk      sync.Mutex
	closed  bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

func NewLocalFlow(bufferSize uint) *LocalFlow {
	return &LocalFlow{
		data:    make(chan interface{}, bufferSize),
		closeCh: make(chan struct{}),
	}
}

func (fl *LocalFlow) Recv(_ Decoder) (interface{}, error) {
	elem, ok := <-fl.data
	if !ok {
		return nil, ErrFlowClosed
	}
	return elem, nil
}

func (fl *LocalFlow) Send(encoder Encoder, msg interface{}) error {
	fl.lk.Lock()
	if fl.closed {
		fl.lk.Unlock()
		return ErrFlowClosed
	}
	fl.wg.Add(1)
	defer fl.wg.Done()
	fl.lk.Unlock()

	toSend, err := encoder.ProcessLocal(msg)
	if err != nil {
		return err
	}

	select {
	case fl.data <- toSend:
		return nil
	case <-fl.closeCh:
		return ErrFlowClosed
	}
}

func (fl *LocalFlow) Close() error {
	fl.lk.Lock()
	defer fl.lk.Unlock()
	if fl.closed {
		return nil
	}
	fl.closed = true
	close(fl.closeCh)
	fl.wg.Wait()
	close(fl.data)
	return nil
}
