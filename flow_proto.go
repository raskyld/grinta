package grinta

import (
	"context"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
)

type ProtoFlow[W proto.Message, R proto.Message] struct {
	inner RawFlow
	ProtoFlowReader[R]
	ProtoFlowWriter[W]
}

func NewProtoFlow[W proto.Message, R proto.Message](raw RawFlow) ProtoFlow[W, R] {
	return ProtoFlow[W, R]{
		inner:           raw,
		ProtoFlowWriter: ProtoFlowWriter[W]{RawFlowWriter: raw},
		ProtoFlowReader: ProtoFlowReader[R]{RawFlowReader: raw},
	}
}

func (fl ProtoFlow[W, R]) Close() error {
	return fl.inner.Close()
}

type ProtoFlowWriter[W proto.Message] struct {
	RawFlowWriter
}

func (fl ProtoFlowWriter[W]) Write(ctx context.Context, item W) error {
	return fl.RawFlowWriter.WriteRaw(ctx, protoMessageMarshaler{inner: item})
}

func (fl ProtoFlowWriter[W]) WriteRaw(ctx context.Context, item Marshaler) error {
	toWrite, ok := item.(W)
	if !ok {
		return ErrFlowTypeMismatch
	}
	return fl.Write(ctx, toWrite)
}

type ProtoFlowReader[R proto.Message] struct {
	RawFlowReader
}

func (fl ProtoFlowReader[R]) Read(ctx context.Context) (result R, resultErr error) {
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
	resultErr = proto.Unmarshal(buf, result)
	return
}

type protoMessageMarshaler struct {
	inner proto.Message
}

func (p protoMessageMarshaler) Marshal() ([]byte, error) {
	return proto.Marshal(p.inner)
}
