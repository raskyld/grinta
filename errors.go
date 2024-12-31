package grinta

import (
	"errors"
	"fmt"

	"github.com/quic-go/quic-go"
)

var (
	ErrNameInvalid = errors.New("fabric: names must only contains alphanum, dashes, dots and be less than 128 chars")

	ErrInvalidCfg             = errors.New("fabric: invalid options")
	ErrQueryInvalid           = errors.New("fabric: query is invalid")
	ErrQueryClosed            = errors.New("fabric: query was closed by user")
	ErrJoinCluster            = errors.New("fabric: could not join cluster")
	ErrFabricInvalidFrame     = errors.New("fabric: invalid gossip frame")
	ErrNameConflict           = errors.New("fabric: endpoint name conflict")
	ErrNameResolution         = errors.New("fabric: endpoint does not exist")
	ErrNotEnoughParticipation = errors.New("fabric: not enough cluster participation")

	ErrBufferSize        = errors.New("transport: could not allocate udp buffer")
	ErrHostnameResolve   = errors.New("transport: could not resolve hostname from certificate")
	ErrInvalidAddr       = errors.New("transport: the IP you provided is invalid")
	ErrUdpNotAvailable   = errors.New("transport: UDP listener not available")
	ErrShutdown          = errors.New("transport: shutting down")
	ErrStreamWrite       = errors.New("transport: error writing to a stream")
	ErrProtocolViolation = errors.New("transport: protocol violation")
	ErrNoTLSConfig       = errors.New("transport: TlsConfig is required")
	ErrTooLargeFrame     = errors.New("transport: frame was too large could not send")
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

const (
	ClosedByUnknown ClosedBy = iota
	ClosedByEPRenamed
	ClosedByUser
	ClosedByRemote
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

type ClosedBy uint8

func (cause ClosedBy) String() string {
	switch cause {
	case ClosedByEPRenamed:
		return "endpoint being overriden by another"
	case ClosedByUser:
		return "explicit user close"
	case ClosedByRemote:
		return "remote"
	default:
		return "unknown"
	}
}

type ClosedError struct {
	cause ClosedBy
	msg   string
}

func (endErr *ClosedError) Error() string {
	return fmt.Sprintf("chan closed by %s: %s", endErr.cause, endErr.msg)
}

// CloseEndpointBecause returns an error to pass to
// `Endpoint.Close`
func CloseEndpointBecause(msg string) ClosedError {
	if msg == "" {
		msg = "no reason provided"
	}

	return ClosedError{
		cause: ClosedByUser,
		msg:   msg,
	}
}
