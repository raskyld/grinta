package gorounette

type GContext interface {
	// Name of the goroutine in the fabric.
	Name() string
}
