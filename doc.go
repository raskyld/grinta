// *Goroutinettes* are *named* goroutines holding `Flow` handles, allowing them
// to communicate regardless of where they live in the `Fabric`.
//
// The `Fabric` is a mesh of Golang processes which host *Goroutinettes* capable
// of exposing themselves (under a certain *name*) so they can accept `Flow`
// establishment requests from other *Goroutinettes*.
//
// ## How it works
//
// The first thing to do is to initiate a `Fabric`, and makes it `Fabric.Join`
// an existing mesh. Under the hood, it will use a UDP gossip protocol to discover
// members of the Fabric and exchange *Goroutinettes*' name. Our host should
// converge quickly and discover which *Goroutinettes* (and `Host`s)
// are available in the `Fabric`.
//
// Then, for the actual data-plane, `Host`s are *lazily* peered together using
// [*Connect RPC*][better-grpc]'s HTTP/2 bi-di multiplexed streams.
//
// *Goroutinettes* can then be exposed on the `Fabric` and start accepting
// `Flow`s. Those are allocated on demand by *clients* and are implemented on-top
// of:
//
// * Go channels if the destination *Goroutinette* is in the same `Host`. (Value is still *copied*.)
// * Inter-`Host` streams, encapsulated in a custom protobuf-encoded structure.
//
// And *voilà*, your goroutines can communicate even if they are not on the
// same machine (or in the same process!).
//
// ## Design Principles
//
// > `goroutinettes` is **anti-fragile** and **scalable**, and **minimalist**.
//
// ### Anti-Fragile
//
// I avoided using a strongly consistent protocol since the `Fabric`
// should be **anti-fragile**, and capable of running on top of a sh.. low-quality
// network. APIs MUST NOT model an *infallible* `Fabric`:
// this doesn't exist. Hence, users MUST be ready to handle
// network errors, so they can build an anti-fragile, fault-tolerant distributed
// system on top of `goroutinettes`.
//
// ### Scalable
//
// Furthermore, not having a heavy consensus protocol allows us to
// **scale** horizontally aggressively. It SHOULD be pretty easy to build
// a *gateway* to inter-connect two independant `Fabric`s. At scale, this
// will likely be needed to avoid having full-meshing. This SHOULD allows really
// flexible topologies.
//
// ### Minimalist
//
// Finally, I don't want `goroutinettes` to become bloated.
// It should be a fundational network library **focused** on the Golang runtime
// capabilities. Namely, goroutines are really central to the library: the runtime
// is multiplexing I/O nicely and provides a sufficiently competent `net` package.
//
// Dependencies SHOULD be *kept* minimal, actually, I can enumerate them:
//
// * [`hashicorp/memberlist`][dep-mbl], for the UDP Gossip protocol of the `Fabric`s.
// * [`go-logr/logr`][dep-gll], to let you chose how to treat *structured logs*.
// * [`bufbuild/connect-go`][dep-con], used for `Fabric`'s `Host`s peering.
//
// The choice of *Connect* over *gRPC* is a direct consequence of this principle.
// gRPC has a maximalist design which is not compatible with my vision. If you want
// to understand it better, I invite you to read [this article][better-grpc] which
// will do a better job than me explaining why. Anyway, this choice mades me
// divide by two our number of dependencies.
//
// [dep-mbl]: https://pkg.go.dev/github.com/hashicorp/memberlist
// [dep-gll]: https://pkg.go.dev/github.com/go-logr/logr
// [dep-con]: https://pkg.go.dev/github.com/bufbuild/connect-go
// [better-grpc]: https://buf.build/blog/connect-a-better-grpc
package goroutinettes
