package flow

import (
	"encoding/json"
	"reflect"

	"github.com/quic-go/quic-go"
)

type JsonEncoder struct {
	inner BytesCodec
}

func NewJsonEncoder(localCopy bool) JsonEncoder {
	return JsonEncoder{
		inner: BytesCodec{
			copyBuffers: localCopy,
		},
	}
}

func (enc JsonEncoder) Encode(stream quic.SendStream, msg interface{}) error {
	buf, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return enc.inner.Encode(stream, buf)
}

func (enc JsonEncoder) ProcessLocal(msg interface{}) (interface{}, error) {
	if !enc.inner.copyBuffers {
		return msg, nil
	}

	clonable, ok := msg.(Clonable)
	if ok {
		return clonable.Clone(), nil
	}

	buf, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	cloned := make([]byte, len(buf))
	copy(cloned, buf)
	return cloned, nil
}

type JsonDecoder[Msg any] struct {
	inner     BytesCodec
	allocator func() Msg
}

func NewJsonDecoder[Msg any]() JsonDecoder[Msg] {
	t := reflect.TypeFor[Msg]()
	if t.Kind() != reflect.Ptr {
		panic("it makes no sense to try to unmarshal into a non-pointer")
	}

	return JsonDecoder[Msg]{
		inner: BytesCodec{},
		allocator: func() Msg {
			return reflect.New(t.Elem()).Interface().(Msg)
		},
	}
}

func (dec JsonDecoder[Msg]) Decode(stream quic.ReceiveStream) (interface{}, error) {
	buf, err := dec.inner.Decode(stream)
	if err != nil {
		return nil, err
	}

	result := dec.allocator()
	err = json.Unmarshal(buf.([]byte), result)
	return result, err
}
