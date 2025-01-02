package grinta

import (
	"errors"
	"fmt"

	"github.com/quic-go/quic-go"
)

var (
	ErrFlowClosed     = errors.New("flow closed")
	ErrEndpointClosed = errors.New("endpoint closed")
	ErrFabricClosed   = errors.New("fabric closed")

	ErrNameInvalid            = errors.New("fabric: names must only contains alphanum, dashes, dots and be less than 128 chars")
	ErrInvalidCfg             = errors.New("fabric: invalid options")
	ErrQueryInvalid           = errors.New("fabric: query is invalid")
	ErrQueryClosed            = errors.New("fabric: query was closed by user")
	ErrJoinCluster            = errors.New("fabric: could not join cluster")
	ErrFabricInvalidFrame     = errors.New("fabric: invalid gossip frame")
	ErrNameConflict           = errors.New("fabric: endpoint name conflict")
	ErrNameResolution         = errors.New("fabric: endpoint does not exist")
	ErrNotEnoughParticipation = errors.New("fabric: not enough cluster participation")
	ErrHostNotFound           = errors.New("fabric: host not found")
	ErrDialFailed             = errors.New("fabric: failed to dial remote endpoint")

	ErrBufferSize        = errors.New("transport: could not allocate udp buffer")
	ErrInvalidAddr       = errors.New("transport: the IP you provided is invalid")
	ErrUdpNotAvailable   = errors.New("transport: UDP listener not available")
	ErrTransportShutdown = errors.New("transport: transport was closed")
	ErrStreamWrite       = errors.New("transport: error writing to a stream")
	ErrProtocolViolation = errors.New("transport: protocol violation")
	ErrNoTLSConfig       = errors.New("transport: TlsConfig is required")
	ErrTooLargeFrame     = errors.New("transport: frame was too large could not send")
)

const (
	QErrStreamProtocolViolation     = quic.StreamErrorCode(0xF)
	QErrStreamEndpointDoesNotExists = quic.StreamErrorCode(0x4)
	QErrStreamClosed                = quic.StreamErrorCode(0xC)
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
