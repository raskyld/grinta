package flow

import "errors"

var (
	ErrFlowClosed = errors.New("flow closed")
)

// Raw is a bidirectional raw flow.
//
// Most users should not use it directly but wrap it
// in a [Sender] and [Receiver] for a better DX.
type Raw struct {
	RawReceiver
	RawSender
}

func (r Raw) Close() error {
	return errors.Join(r.RawReceiver.Close(), r.RawSender.Close())
}
