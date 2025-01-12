package flow

import "errors"

var (
	ErrFlowClosed = errors.New("flow closed")
)

type Raw struct {
	RawReceiver
	RawSender
}

func (r Raw) Close() error {
	return errors.Join(r.RawReceiver.Close(), r.RawSender.Close())
}
