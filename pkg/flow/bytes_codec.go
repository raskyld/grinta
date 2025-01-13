package flow

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/quic-go/quic-go"
	"google.golang.org/protobuf/encoding/protowire"
)

// BytesCodec is a simple framing codec using length-prefixed frames
// to exchange []byte over a flow.
type BytesCodec struct {
	copyBuffers bool
}

func NewBytesCodec(localCopy bool) BytesCodec {
	return BytesCodec{
		copyBuffers: localCopy,
	}
}

func (enc BytesCodec) Encode(stream quic.SendStream, msg interface{}) error {
	buf, ok := msg.([]byte)
	if !ok {
		panic(
			fmt.Sprintf(
				"decoder returned no error, but returned wrong type %s instead of []byte",
				reflect.TypeOf(msg).String(),
			),
		)
	}

	varintBuf := protowire.AppendVarint(nil, uint64(len(buf)))
	prefixedBuf := make([]byte, len(varintBuf)+len(buf))
	copy(prefixedBuf, varintBuf)
	copy(prefixedBuf[len(varintBuf):], buf)
	// TODO(raskyld): make it more failure tolerent
	_, err := stream.Write(prefixedBuf)
	return err
}

func (enc BytesCodec) ProcessLocal(msg interface{}) (interface{}, error) {
	if !enc.copyBuffers {
		return msg, nil
	}

	buf, ok := msg.([]byte)
	if !ok {
		panic(
			fmt.Sprintf(
				"decoder returned no error, but returned wrong type %s instead of []byte",
				reflect.TypeOf(msg).String(),
			),
		)
	}

	cloned := make([]byte, len(buf))
	copy(cloned, buf)
	return cloned, nil
}

func (enc BytesCodec) Decode(stream quic.ReceiveStream) (interface{}, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := 0
	for n < len(buf) {
		m, err := stream.Read(buf[n : n+1])
		if err != nil {
			return nil, err
		}
		if m != 0 {
			byteRead := buf[n]
			n = m + n
			if byteRead < 0x80 {
				break
			}
		}
	}

	// TODO(raskyld): limit message size
	prefix, prefixSize := protowire.ConsumeVarint(buf[:n])
	if err := protowire.ParseError(prefixSize); err != nil {
		return nil, err
	}

	buf = make([]byte, prefix)
	n = 0
	for n < len(buf) {
		m, err := stream.Read(buf[n:])
		if err != nil {
			return nil, err
		}
		n = n + m
	}

	return buf, nil
}
