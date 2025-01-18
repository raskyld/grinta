<div align="center">
  <h1><code>grinta</code></h1>

  <p>
    <strong>A minimalist network fabric for your goroutines
    :link: :sparkles:</strong>
  </p>
</div>

> **N.B.** This is a *Proof Of Concept* library I built as a side-project
> over the last month to fulfill the needs I have for another project, it is
> **NOT production ready**. :pray:

* *Goroutines* hold an *Endpoint* :round_pushpin: on which they can listen
  for inbound *Flow* :ocean: establishment requests :ocean:
* *Flows* :ocean: are bidirectional communication chanel between
  a *client* and an *Endpoint* :round_pushpin:
* *Endpoints* :round_pushpin: are *named* listeners exposed on a *fabric* :link: 
* *Fabrics* :link: are meshes of Golang processes &mdash; potentially
  distributed over multiple machines &mdash; capable of establishing
  *flow* :ocean:

## Demo

This video shows the [`examples/hello-world`](examples/hello-world/) example.

First, both goroutines are on the same node and can communicate using a
*Flow* :ocean: anyway, behind the scene `grinta` use native Go `chan`.

Then, we run **the same binary** but this time, we put each goroutine in a
different process. From a code point of view, nothing change for the goroutines,
they still use a *Flow* :ocean: but this time, `grinta` uses a QUIC inter-node
multiplexed stream fabric.

[hello world example](https://github.com/user-attachments/assets/24064e25-3eda-4153-adfc-de16f57423cb)

## Features

* :relieved: **Simple API**: `Fabric` -> `Endpoint` -> `Flow` describes an intuitive
  hierarchy and their APIs are kept minimal.
* :writing_hand: **Built-in Codec**: We already support:
  * Protobuf,
  * JSON,
  * Raw Bytes,
* :pinching_hand: **Golang Focused**: By focusing on a single language, we remove a lot of
  complexity and can optimise for just our specific use-case.
* :family: **No Central Authority**: A `grinta` cluster has no central authority, no
  strongly consistent consensus protocol, nodes collaborate together to
  converge as fast as possible.
* :sunglasses: **GRINTA Protocol**: A custom protocol made on top of:
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
