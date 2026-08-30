# How to run a gateway

A gateway (`uc2-gateway`) is a TCP front door for a UC cluster: it terminates
the [remote protocol](../reference/remote-protocol.md) from ordinary TCP
clients and relays their commands over shared memory into the co-located
node, so a client that cannot attach to shmem directly (a different host, a
non-Rust process) still gets the same replicated, linearizable-read-capable
service a local `uc_client::Engine` would.

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

## Operating envelope (2.7.0)

**The rule is connections versus cores.** Everything else the edge used to
need sizing against is now arithmetic it does itself.

`2.7.0` gives the edge a **global grant budget**: it holds one `Engine`
inflight window, keeps an eighth of it back as headroom, and divides the
rest equally across its live connections —
`grant = clamp(budget / connections, 1, per_conn_inflight)`. A connection is
told its grant in `HELLO_OK`; when a new connection shrinks everyone's share
the smaller number is pushed as a `STATUS` **before** the affected clients
can send into it, and when a connection leaves the survivors grow back into
its share. So the sum of what this edge has promised never exceeds what its
node can accept, and the reactive halve-on-backpressure ladder — which in
`2.6.0` was the only cross-connection arbiter there was — is now the
exception path. See [the grant budget](../reference/gateway-config.md#the-grant-budget-270)
for the numbers and the two startup checks that go with it
(`per_conn_inflight` above the budget is a refusal; `max_connections` above
it is a warning).

**The 2.6.0 collapse is gone.** That release's envelope warned that N
connections past the node's admission window did not degrade but collapsed —
~30× down, second-scale p95, lost responses, the edge burning seven of eight
cores. The [hop bench](../benchmarks/uc2-m13-hop-bench-2026-08-24.md) located
the cause, and it was not the credit design: it was a convoy in the
shared-memory MPSC ingress ring, where producers published in claim order and
spun on their predecessor, so one preempted producer stalled all of them.
`2.7.0` replaces that with per-record commit (no producer ever waits for
another), which is why the rule below is about **cores**, not about inflight
arithmetic. The [correction note](../notes/uc2-m12a-edge-flow-control-gap.md)
records what the `2.6.0` diagnosis got right and what it got wrong.

**What still needs your attention:**

1. **Count threads against cores.** One edge costs one acceptor, one driver,
   and **one reader thread per connection**; the co-located node runs four
   busy-spin agents and the service one more. On an 8-vCPU host, an edge with
   more than a handful of *busy* connections is oversubscribed, and past that
   point more connections buy latency, not throughput. The measured shape:
   one connection through the edge relays at the cluster's own commit
   ceiling, the aggregate knee is around two connections (the single driver
   thread saturates), and past the knee the curve is flat, then slowly down —
   degradation, not a cliff. `max_connections` (default `1024`) bounds
   threads and sockets; it is not a capacity number and never was.
2. **Bound the gateway's CPU only if you have a reason to.** With the convoy
   gone, `CPUQuota=` is a policy choice about who owns the host's cores, not
   a protection against collapse — and it was never protection: throttling
   the edge made the convoy *worse*, because it starved the preempted
   producer harder.
3. **Size `per_conn_inflight` as a ceiling, not an allocation.** It caps what
   one connection may hold while few are attached; the budget takes over as
   more arrive. A value above the budget is refused at startup by name.

The signals worth watching are the stats line's `backpressure` (should be
near zero — grants that fit the window do not hit it), `grant_changes` (moves
when connections come and go, quiet otherwise), and the node's
`Uc2AdmissionSaturated` alert ([Monitor a cluster](monitor-a-cluster.md)).

[gate]: ../benchmarks/uc2-m13-gate-2026-08-24.md

## Start it

The release tarball ships `uc2-gateway` alongside `uc2-node` and `uc2ctl`, plus
`counter-remote` — a worked remote client to point at it — so installing the
binaries is the same step as
[for a node](run-a-cluster.md#install-the-binaries-on-each-host).

```bash
uc2-gateway --config /etc/uc2/gateway.toml
```

```text
uc2-gateway: listening on 0.0.0.0:9200
```

It attaches to the node's instance directory the way `uc_client::Engine`
would — start the node first. Under systemd,
`packaging/systemd/uc2-gateway.service` already encodes this ordering
(`After=uc2-node.service`, `BindsTo=uc2-node.service` — the gateway stops
when its node does, which is the liveness mechanism described under
[When the node underneath dies](#when-the-node-underneath-dies)) plus the
restart policy below:

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

A conforming client (`uc_remote`'s `RemoteEngine` halves or the `RemoteClient`
convenience over them, or a port that implements
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

## When the node underneath dies

A gateway is a relay with no state of its own, so the interesting failure is
not the gateway crashing — it's the **node underneath it dying while the
gateway keeps running**.

The reason this needs saying: a dead node does not clear its control page. If
the `uc2-node` process is SIGKILLed or wedges, its `cnc2.dat` page is left
frozen exactly as it was — including `CAN_SERVE`. A co-located edge reads
that page to decide whether to take writes, so it keeps saying yes: it
accepts `SUBMIT`s into an ingress ring nobody is draining and can only answer
them `UNKNOWN` once the request's own deadline expires.

**What prevents that in the packaged deployment is
`BindsTo=uc2-node.service`** in `packaging/systemd/uc2-gateway.service` —
and it is worth being precise that this is the *liveness mechanism*, not just
a startup ordering hint. `After=` only orders the two at boot; `BindsTo=`
means the gateway unit is **stopped whenever the node unit stops**, however
it stopped. So the moment systemd reaps a dead node, the gateway goes with
it, its listener closes, and every client fails over to another member's
gateway on its own member list. There is no window in which a live gateway
fronts a reaped node.

The second layer catches the node coming *back*: a restarted node gets a new
shmem instance id, the edge's attached `Engine` reports `InstanceRestart`,
the edge tells every connected client `LEADER_CHANGED{unknown}`, closes them,
refuses all further handshakes with `HELLO_REFUSED_FAULTED`, and the
`uc2-gateway` process exits `1` so `Restart=on-failure` brings up a fresh one
against the new instance.

### The residual window, and how to size it

Between the two layers there is a gap: a node that is dead or wedged but
**not yet noticed by systemd** (it is still in `D` state, or its unit has a
`TimeoutStopSec` still running out). During that gap the edge is live, the
page still says `CAN_SERVE`, and a client's write is accepted and never
answered — until `request_timeout` expires and the edge answers `UNKNOWN`.

`request_timeout_ms` is therefore *the client's exposure window* to a dead
node, not merely an engine deadline. **Set it lower on a gateway than the
10 s default — `2000` is a reasonable starting point:**

```toml
[limits]
request_timeout_ms = 2000
```

The tradeoff is symmetric and shallow. Shorter means a client pinned to a
dead node hears `UNKNOWN` sooner and re-sends somewhere useful sooner;
`UNKNOWN` is not a lost write — with the session envelope on, the re-send
comes back `replayed` or applies exactly once. Longer means a genuinely slow
but *live* request is less likely to be called `UNKNOWN` prematurely. Since
the resend is safe and the alternative is a stalled client, err short. (The
same reasoning is why `examples/uc_crashtest`'s gateway binary runs with a
2 s deadline against a test that kills a node every few seconds.)

A client sees this as: the request resolves `UNKNOWN` after
`request_timeout`, and — because the connection itself is still open and the
edge is still answering `STATUS` — it re-sends **on the same connection**
rather than failing over. It only moves when systemd stops the gateway (the
socket closes) or the node restarts (the faulted path above). That is why
`request_timeout` is the number that bounds the stall, not `dead_after`.

### The stronger fix, not implemented

The edge could probe the node's liveness directly rather than trusting the
frozen page: `uc_service` already takes a **shared flock on the instance
directory** as a liveness probe against the node's exclusive lock, and an
edge doing the same could refuse writes the instant the node's lock became
acquirable — no supervisor in the loop, and no residual window at all. That
is a follow-up, deliberately not in M12a: it adds a per-instance-dir probe to
the edge's periodic work and needs its own test for the case where the lock
is momentarily free during a clean restart.

## When an edge is full

Each connection costs the edge one reader thread and one socket, so an edge
accepts at most `[limits] max_connections` of them (default `1024`). Over
that, the acceptor answers `HELLO_REFUSED{BUSY}` and closes, without spawning
a reader — and a conforming client treats `BUSY` the same way it treats
`FAULTED`: this member is out, try the next one. `EdgeStats::refused_busy`
counts them, so a rising number is the signal to raise the ceiling or spread
clients across members.

**`max_connections` is not a capacity number.** It bounds threads and
sockets, not throughput: one reader thread per connection is a real cost on a
host that is also running a node and a service. Size it against the host's
cores — see [Operating envelope (2.7.0)](#operating-envelope-270) — and note
that a value above the edge's grant budget is a startup warning, because
every connection past the budget is granted the floor of one credit.

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
`uc_gateway/src/edge.rs`'s module doc), not a bug: it resolves the same way
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
main loop's 100 ms polling interval), exactly these fields in order:
`conns` (connections accepted), `submits`, `queries`, `responses`,
`redirects`, `retries`, `unknown`, `backpressure` (squeeze events),
`grant_changes` (per-connection grant redivisions — moves as connections come
and go, quiet otherwise), `leader_changes` (observed leader-watch
transitions), `status`
(standalone `STATUS` frames written), and `refused_busy` (dials turned away
at the `max_connections` ceiling). `EdgeStats` also tracks
`leader_changed_frames` (`LEADER_CHANGED` frames actually written, which can
differ from `leader_changes` — a transition to an unresolvable leader hint
is observed but not announced) but the reference binary does not print it;
read it via `Edge::stats()` if you embed the library yourself. Use the
stats line as a coarse eyes-on-the-box signal; for anything durable, scrape
metrics off the co-located node (`../how-to/monitor-a-cluster.md`) — the
gateway itself exposes no `/metrics` endpoint.

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
- [M12 gate record](../benchmarks/uc2-m12-gate-2026-08-22.md) — the measured
  per-connection cost, the N-connection aggregate ladder, and the
  cross-connection flow-control defect behind the operating envelope above.
