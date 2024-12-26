package goroutinettes

import (
	"errors"
	"fmt"

	"github.com/quic-go/quic-go"
)

var (
	ErrHostnameInvalid = errors.New("fabric: node name is not valid")
	ErrNameInvalid     = errors.New("goroutinette: name is not valid")
	ErrBufferSize      = errors.New("transport: could not allocate udp buffer")
	ErrHostnameResolve = errors.New("transport: could not resolve hostname from certificate")
	ErrInvalidAddr     = errors.New("transport: the IP you provided is invalid")
	ErrUdpNotAvailable = errors.New("transport: UDP listener not available")
	ErrShutdown        = errors.New("transport: shutting down")
	ErrStreamWrite     = errors.New("transport: error writing to a stream")
)

var (
	QErrStreamProtocolViolation = quic.StreamErrorCode(0xFF)
)

var (
	QErrInternal = QuicApplicationError{
		Code:   0x1,
		Prefix: "internal",
	}
	QErrHostname = QuicApplicationError{
		Code:   0x2,
		Prefix: "hostname",
	}
	QErrShutdown = QuicApplicationError{
		Code:   0x3,
		Prefix: "shutdown",
	}
	QErrNameConflict = QuicApplicationError{
		Code:   0x4,
		Prefix: "name conflict",
	}
)

type QuicApplicationError struct {
	Code   uint64
	Prefix string
}

func (qerr *QuicApplicationError) Close(conn quic.Connection, msg string) error {
	if conn != nil {
		return conn.CloseWithError(
			quic.ApplicationErrorCode(qerr.Code),
			fmt.Sprintf("%s: %s", qerr.Prefix, msg),
		)
	}
	return nil
}
