<div align="center">
  <h1><code>grinta</code></h1>

  <p>
    <strong>A minimalist network fabric for your goroutines
    :link: :sparkles:</strong>
  </p>
</div>

* *Goroutines* hold an *Endpoint* :round_pushpin: on which they can listen
  for inbound *Flow* establishment requests :ocean:
* *Flows* :ocean: are bidirectional communication chanel between
  a *client* and an *Endpoint* :round_pushpin:
* *Endpoints* :round_pushpin: are *named* listeners exposed on a *fabric* :link: 
* *Fabrics* :link: are meshes of Golang processes &mdash; potentially
  distributed over multiple machines &mdash; capable of establishing
  *flow* :ocean:

## Why? :face_with_raised_eyebrow:

Golang Runtime's concurrency model has two major actors:
* Computational units: *Goroutines*.
* Communication primitives: *Channels*. 

They have one obvious limitation: they can only be used **inside of the same
process**.

Your software may scale to the point it cannot run in a single
process anymore, you now have *Goroutines* spread in different
processes (and likely, machines).

`grinta` aims to generalise those actors, so they can be ported to a
*distributed* environment with **zero rewriting**.

## How? :star_struck:

See the `examples/` folder.

## Features

* **Simple API**: `Fabric` -> `Endpoint` -> `Flow` describes an intuitive
  hierarchy and their APIs are kept minimal.
* **Golang Focused**: By focusing on a single language, we remove a lot of
  complexity and can optimise for just our specific use-case.
* **No Central Authority**: A `grinta` cluster has no central authority, no
  strongly consistent consensus protocol, nodes collaborate together to
  converge as fast as possible.
* **GRINTA Protocol**: A custom protocol made on top of:
  * a first *QUIC Transport Layer* supporting multiplexed inter-node
    bidirectional **streams** and lightweight **datagrams**, 
  * an implementation of the ["SWIM"][swim] gossip protocol:
    [`hashicorp/memberlist`][dep-mbl],
  * an adapter to run `memberlist` clusters on top of our
    *QUIC Transport Layer*,
  * a set of versioned [Protobuf Messages](./proto/grinta) which describes
    our control plane communication format,
  * an event and query bus to propagate *Endpoint* :round_pushpin: information:
    [`hashicorp/serf`][dep-serf];

[swim]: (http://ieeexplore.ieee.org/document/1028914/)
[dep-mbl]: https://pkg.go.dev/github.com/hashicorp/memberlist
[dep-serf]: https://pkg.go.dev/github.com/hashicorp/serf
