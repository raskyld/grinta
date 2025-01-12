package flow

import (
	"github.com/quic-go/quic-go"
)

type RemoteSender struct {
	quic.SendStream
}

var _ RawSender = RemoteSender{}

func (s RemoteSender) Send(enc Encoder, msg interface{}) error {
	return enc.Encode(s.SendStream, msg)
}

type RemoteReceiver struct {
	quic.ReceiveStream
}

var _ RawReceiver = RemoteReceiver{}

func (r RemoteReceiver) Recv(dec Decoder) (interface{}, error) {
	return dec.Decode(r.ReceiveStream)
}

func (r RemoteReceiver) Close() error {
	r.CancelRead(quic.StreamErrorCode(0xC))
	return nil
}
