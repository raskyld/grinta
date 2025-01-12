package flow

import (
	"fmt"
	"reflect"

	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"
)

type ProtoCodec[Msg proto.Message] struct {
	inner BytesCodec
}

func NewProtoCodec[Msg proto.Message](localCopy bool) ProtoCodec[Msg] {
	return ProtoCodec[Msg]{
		inner: BytesCodec{
			copyBuffers: localCopy,
		},
	}
}

func (enc ProtoCodec[Msg]) Encode(stream quic.SendStream, msg interface{}) error {
	message, ok := msg.(Msg)
	if !ok {
		panic(
			fmt.Sprintf(
				"decoder returned no error, but returned wrong type %s instead of %s",
				reflect.TypeOf(msg).String(),
				reflect.TypeFor[Msg]().String(),
			),
		)
	}

	buf, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	return enc.inner.Encode(stream, buf)
}

func (enc ProtoCodec[Msg]) ProcessLocal(msg interface{}) (interface{}, error) {
	if !enc.inner.copyBuffers {
		return msg, nil
	}

	message, ok := msg.(Msg)
	if !ok {
		panic(
			fmt.Sprintf(
				"decoder returned no error, but returned wrong type %s instead of %s",
				reflect.TypeOf(msg).String(),
				reflect.TypeFor[Msg]().String(),
			),
		)
	}

	return proto.Clone(message), nil
}

func (enc ProtoCodec[Msg]) Decode(stream quic.ReceiveStream) (interface{}, error) {
	buf, err := enc.inner.Decode(stream)
	if err != nil {
		return nil, err
	}

	var allocated Msg
	allocated = allocated.ProtoReflect().New().Interface().(Msg)
	err = proto.Unmarshal(buf.([]byte), allocated)
	return allocated, err
}
