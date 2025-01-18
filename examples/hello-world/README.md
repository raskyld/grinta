# Example: `hello-world`

This is the most trivial example, we have a single
*Endpoint* :round_pushpin: named `server` on the `Fabric` :link:
and we start a goroutine which will `Dial` it to establish a
*Flow* :ocean:.

Once the `server` receive `"hello, world!"`, it will shutdown.

We can run it in single or dual-node, but do note that this uses
**the same binary**! This is the main mission of `grinta`: allowing
you to build distributed software which can adapt to different topology
without refactoring your codebase.

For a more powerful example, see
[`../flexible-topology`](../flexible-topology/).

## Single Node

```shell
make run-single
```

## Dual Node

```shell
make run-server
```

Then, in another terminal run

```shell
make run-client
```
