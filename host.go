package goroutinettes

import (
	"crypto/x509"
	"log/slog"
	"unique"
)

type Hostname string

type Host struct {
	Name unique.Handle[Hostname]
	Addr string
	Port int
}

// HostnameResolver can resolve an hostname from a list of
// `x509.Certificate`, those certificates are the one received from a
// remote peer.
//
// The contract of this function is:
//
// *Implementations* MUST NOT be blocking, since they are invoked on
// the connection establishment critical path.
//
// If the resolution is successful, *Implementations* MUST return an hostname
// and a nil error.
//
// Otherwise, *Implementations* MUST return a human-friendly error string
// as a third argument, which will be sent to the remote peer, so they can
// debug the error.
//
// If they return a non-nil error but an empty third string,
// a `QErrInternal` is returned to the user instead.
type HostnameResolver func(certs []*x509.Certificate) (Hostname, error, string)

// CommonNameResolver is the default resolver used to resolve the hostname
// from the x509 Subject Common Name of the peer certificate.
func CommonNameResolver(certs []*x509.Certificate) (Hostname, error, string) {
	if len(certs) == 0 {
		return "", ErrHostnameResolve, "it seems like you haven't provided client certificate"
	}

	return Hostname(certs[0].Subject.CommonName), nil, ""
}

func (host *Host) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("name", string(host.Name.Value())),
		slog.String("addr", host.Addr),
		slog.Int("port", host.Port),
	)
}
