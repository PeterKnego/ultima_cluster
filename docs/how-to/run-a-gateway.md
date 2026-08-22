# How to run a gateway

A gateway (`uc2-gateway`) is a TCP front door for a UC cluster: it terminates
the [remote protocol](../reference/remote-protocol.md) from ordinary TCP
clients and relays their commands over shared memory into the co-located
node, so a client that cannot attach to shmem directly (a different host, a
non-Rust process) still gets the same replicated, linearizable-read-capable
service a local `uc2_client::Engine` would.

## Topology: one edge per node host

Run exactly one `uc2-gateway` per `uc2-node` host, attached to that node's
`instance_dir`. A gateway holds no durable state of its own — it is pure
relay — so there's no reason to run more than one per node, and running one
per node (rather than a separate fleet of stateless proxies) is what lets
`REDIRECT`/`LEADER_CHANGED` name a *node's* gateway address directly.

Each host needs two config files that must agree:

- `/etc/uc2/node.toml` — the node, as usual.
- `/etc/uc2/gateway.toml` — the gateway, `[local] instance_dir` pointing at
  the same directory, `[local] app_id` matching the node's `app_id` exactly.

**The `[[members]]` map must be byte-identical across every host.** It's the
node-id → gateway-address table that answers `REDIRECT` and `LEADER_CHANGED`
— if host A's map disagrees with host B's, a client redirected from A to a
node B doesn't recognize gets stuck. See
[the config reference](../reference/gateway-config.md) for every key.

## Start it

```bash
uc2-gateway --config /etc/uc2/gateway.toml
```

```text
uc2-gateway: listening on 0.0.0.0:9200
```

It attaches to the node's instance directory the way `uc2_client::Engine`
would — start the node first. Under systemd,
`packaging/systemd/uc2-gateway.service` already encodes this ordering
(`After=uc2-node.service`, `BindsTo=uc2-node.service` — the gateway stops
when its node does) plus the restart policy below:

```bash
sudo cp packaging/systemd/uc2-gateway.service /etc/systemd/system/
sudo cp packaging/gateway.example.toml /etc/uc2/gateway.toml   # edit it first
sudo systemctl daemon-reload
sudo systemctl enable --now uc2-gateway
```

## Stop it

`SIGTERM`/`SIGINT` (`systemctl stop uc2-gateway`, or plain `Ctrl-C`) closes
every connection and exits `0`. There's no drain step to wait for — a
gateway carries no state a stop could lose; in-flight requests fail over to
another edge the same way a mid-request node crash would.

## What a client sees on failover

A conforming client (`uc2_remote::RemoteClient`, or a port that implements
[the protocol reference](../reference/remote-protocol.md)) never surfaces
`REDIRECT`, `LEADER_CHANGED`, `RETRY`, or a dropped connection to its
caller — it absorbs all four and resolves every request to exactly one of a
response, or a definite `Expired`/`Unknown`/`PayloadTooLarge`/`TimedOut`/
`Closed` error:

- **A write lands on a follower's edge.** That edge's `Edge` answers
  `REDIRECT` to the member map's entry for the current leader (or
  `RETRY{not_serving}` if it doesn't have a leader hint yet). The client
  reconnects there, re-`HELLO`s, and re-sends every unanswered request in
  order.
- **The leader changes while a client is idle or backed off.** Every edge
  runs a leader watch that pushes `LEADER_CHANGED` to every ready connection
  the moment it observes the cluster's `(can_serve, leader_hint)` change — so
  a client with nothing in flight still learns about the move rather than
  sitting on a stale connection until it happens to try again.
- **The node's shmem instance restarts underneath a running gateway** (a
  crash-and-restart, an upgrade). The edge notices, tells every connected
  client `LEADER_CHANGED{unknown}` (forcing a reconnect to *some* other
  member) and closes them, then refuses every new `HELLO` with
  `HELLO_REFUSED_FAULTED` from then on. The `uc2-gateway` process itself
  exits `1` so systemd's `Restart=on-failure` brings up a fresh gateway
  against the new node instance — a faulted edge never idles forever
  pretending to be usable.
- **A connection is refused a write once.** That specific TCP connection is
  refused every later `SUBMIT` too, even if this node starts serving again a
  moment later (the "not-serving latch" — see
  [the flow-control note](../notes/uc2-gateway-shapes-and-flow-control.md)
  for why). The client doesn't retry on that connection; it reconnects.

None of this requires the client to poll cluster state itself — it's driven
entirely by what the edge tells it.

## The single-driver head-of-line caveat

One edge process runs exactly one driver thread that writes every response
for every one of its connections, in the order completions arrive. If one
client's socket send buffer fills (a slow or wedged peer), that write can
stall the driver for up to its write timeout (1 s) — and for that whole
time, the driver isn't draining the shared completion ring for *any* other
client either. A response computed for another client during that stall can
be overwritten in the ring before the driver gets back to it, and that
client sees `UNKNOWN` instead of its real answer. This is a real, accepted
cost of the current single-driver design (documented in
`uc2_gateway/src/edge.rs`'s module doc), not a bug: it resolves the same way
any `UNKNOWN` does — the client resends, and with the session envelope on
that resend comes back `replayed` rather than double-applying anything. If
one misbehaving client stalling every other client on the same edge is a
problem for your workload, put fewer clients behind one edge, or watch for
it: it is exactly what running with the envelope off would make unsafe
instead of merely momentarily confusing.

## When to use `envelope = false`

Leave `session_envelope = true` (the default) unless you have a specific
reason not to. Turning it off buys you:

- **Zero prepended bytes** on every `SUBMIT` — the client's payload reaches
  `apply` completely unmodified, useful if you're porting an existing
  Aeron-shaped client that already speaks a wire format the gateway
  shouldn't touch.
- **No `Sessioned<S>` wrapping requirement** on the state machine.

In exchange, a re-sent write (after any reconnect — a failover, a
`REDIRECT`, a `PING` timeout) may apply twice, and it is entirely the
application's job to make that safe (its own idempotency key, or a command
that is naturally idempotent). If you're not sure which you want, keep the
envelope on: it costs 16 bytes per `SUBMIT` and one byte per `RESPONSE`, and
turns every retry-driven duplicate into a definite `replayed`/`fresh`
instead of a silent maybe.

## Stats line

`uc2-gateway` prints one stats line to stderr every 10 s (100 ticks of the
main loop's 100 ms polling interval): connections accepted, submits,
queries, responses, redirects, retries, unknowns, backpressure events,
leader changes (observed transitions) and leader-changed frames (actually
written), and standalone status frames. Use it as a coarse eyes-on-the-box
signal; for anything durable, scrape metrics off the co-located node
(`../how-to/monitor-a-cluster.md`) — the gateway itself exposes no
`/metrics` endpoint.

## Related

- [`gateway.toml` reference](../reference/gateway-config.md) — every key,
  default, and refusal.
- [The remote protocol](../reference/remote-protocol.md) — the wire format a
  client implements against.
- [Flow control and edge shapes](../notes/uc2-gateway-shapes-and-flow-control.md)
  — why redirect instead of forward, why credits instead of relying on TCP
  alone, and the two lessons the failover work found.
- [Run a cluster on real hosts](run-a-cluster.md) — the node side of this
  same host.
