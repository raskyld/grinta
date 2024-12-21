package gorounette

import "time"

// Fabric is the top-level abstraction of gorounette.
type Fabric interface {
	// Join a fabric using the user-provided neighbours.
	//
	// Prefer providing several neighbours, living in different failure
	// domains to improve the reliability of your fabric.
	//
	// Returns:
	// * The number of neighbours successfuly contacted.
	// * Any errors occuring during the Join process.
	Join(neighbours []string) (int, error)

	// Leave a fabric gracefully, announcing the event to peers.
	Leave(gracePeriod time.Duration) error

	// Topology of the fabric.
	Topology() ([]Host, error)

	// Spawn a new goroutine on the Fabric.
	Spawn(body func(GContext) error, opts ...GOptions) error
}

type Config struct {
	// Name which should be advertised when joining the Fabric.
	Name string

	// Keyring of AES keys to use to be able to join a Fabric.
	//
	// All inter-host communication is encrypted using the keys.
	//
	// Encryption always use the first one. Decryption tries keys in the
	// provided order.
	//
	// When doing key rotation, first, make sure to
	// add the new key as a "secondary" one. Once all hosts of the Fabric
	// have this new key as their secondary, start swapping the keyring.
	//
	// In this way, in the transient period, a mix of both key will be used through
	// the Fabric until it finally converges to solely use the new one.
	// After a while, it is safe to remove the old key.
	Keyring [][]byte
}

type HostState uint8

const (
	HostStateHealthy HostState = iota
	HostStateWarning
	HostStateLost
	HostStateLeft
)

type Host struct {
	Name       string
	Addr       string
	State      HostState
	Goroutines []string
}
