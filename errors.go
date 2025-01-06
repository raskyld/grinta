package grinta

import (
	"errors"
	"fmt"

	"github.com/quic-go/quic-go"
)

var (
	ErrFlowTypeMismatch = errors.New("flow: type mismatch")
	ErrFlowClosed       = errors.New("flow closed")
	ErrEndpointClosed   = errors.New("endpoint closed")
	ErrFabricClosed     = errors.New("fabric closed")

	ErrNameInvalid            = errors.New("fabric: names must only contains alphanum, dashes, dots and be less than 128 chars")
	ErrInvalidCfg             = errors.New("fabric: invalid options")
	ErrQueryInvalid           = errors.New("fabric: query is invalid")
	ErrJoinCluster            = errors.New("fabric: could not join cluster")
	ErrFabricInvalidFrame     = errors.New("fabric: invalid gossip frame")
	ErrNameConflict           = errors.New("fabric: endpoint name conflict")
	ErrNameResolution         = errors.New("fabric: endpoint does not exist")
	ErrNotEnoughParticipation = errors.New("fabric: not enough cluster participation")
	ErrHostNotFound           = errors.New("fabric: host not found")
	ErrDialFailed             = errors.New("fabric: failed to dial remote endpoint")

	ErrInvalidAddr            = errors.New("transport: the IP you provided is invalid")
	ErrUdpNotAvailable        = errors.New("transport: UDP listener not available")
	ErrTransportNotAdvertised = errors.New("transport: transport was not advertised yet")
	ErrStreamWrite            = errors.New("transport: error writing to a stream")
	ErrProtocolViolation      = errors.New("transport: protocol violation")
	ErrNoTLSConfig            = errors.New("transport: TlsConfig is required")
	ErrTooLargeFrame          = errors.New("transport: frame was too large could not send")
)

const (
	// QErrStreamProtocolViolation is sent when the protocol
	// initialisation encounters unexpected packets.
	QErrStreamProtocolViolation = quic.StreamErrorCode(0xF)

	// QErrStreamEndpointDoesNotExists is sent by nodes when an
	// established flow request an endpoint which is no longer
	// available.
	QErrStreamEndpointDoesNotExists = quic.StreamErrorCode(0x4)

	// QErrStreamClosed is sent when a stream is shutting down.
	QErrStreamClosed = quic.StreamErrorCode(0xC)
)

var (
	QErrShutdown = QuicApplicationError{
		Code:   0x3,
		Prefix: "shutdown",
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
