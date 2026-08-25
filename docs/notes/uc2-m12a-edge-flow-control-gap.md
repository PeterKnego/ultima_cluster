# The M12a edge flow-control gap: what the spec promised, what shipped, and how it collapses

*Written 2026-08-24, after the M12 edge-saturation fleet ladders.*

> **CORRECTION (2026-08-24, superseding this note's own diagnosis).** The gap
> described below is **real and is now fixed** in `2.7.0`: the edge has a
> global outstanding-grant budget (spec
> `2026-08-24-uc2-m13-remote-path-design.md` §5), so the sum of credits it
> promises fits the `Engine` window and a reduction reaches a client before
> it can send into it. **But this note's "How it collapses" section is
> wrong about the cause of the fleet collapse.** The per-hop bench
> (`docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`) reproduced the identical
> 30× collapse with the edge's window at 65536 *and* at 4096, with a raw
> client *and* with `RemoteClient`, against a dummy sink with no admission
> window at all, and — decisively — at 8 connections × 256 inflight, i.e.
> 2,048 outstanding, comfortably *inside* the envelope this note prescribes.
> The trigger was the **number of connections**, and the mechanism was a
> convoy in `uc_protocol::ring::mpsc`: producers published in claim order and
> spun on their predecessor, so one preempted producer stalled every producer
> behind it, on the very cores it needed to make progress. Read the chain in
> "How it collapses" as *a plausible story that the measurement refuted*, and
> `docs/notes/uc2-m13-mpsc-publish-convoy-explained.md` for the one that
> survived it. The operating-envelope rule this note recommended
> ("sum of client inflight < the admission window") did not protect against
> the real fault, and the CPU-containment advice made it worse.

Status: the gap is **closed in `2.7.0`**; the collapse it was blamed for had
a different cause, also fixed in `2.7.0`. Nothing here changes consensus, the
node↔node wire, or the cnc page.

## In one paragraph

The M12a spec says the gateway edge sizes each connection's credits from the
shared `Engine` inflight window it has left. The shipped edge does not: every
connection is granted a flat `per_conn_inflight` at `HELLO_OK`, and nothing in
the edge counts outstanding submits across connections. The only cross-
connection arbiter is the `Engine`'s slot table, which every connection's
reader thread hits independently — so when the sum of client inflight exceeds
that window, each reader spins and parks on `Backpressure` in its own loop,
re-colliding, and the edge burns most of its host's cores on the churn
instead of converging to a bounded queue. On a real fleet that turned a
near-linear 451k resp/s at four connections into a 30× collapse at eight.

## What the spec promised

`docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md` §4.2:

> The client may have at most `credits` unanswered seqs beyond `acked_seq`.
> **The edge sizes `credits` from the `Engine` inflight window it has left,
> shared across its connections**, and shrinks them when `Engine` reports
> `Backpressure` — pressure is signalled before frames leave the client.

The design note (`docs/notes/uc2-gateway-shapes-and-flow-control.md:111-118`)
names the exact hazard the credits exist to prevent: one edge has **one**
`Engine` window shared by every connection, and TCP cannot express "the thing
behind me, not this socket, is the scarce resource."

## What shipped

- **Grant**: `uc2_gateway/src/edge.rs:848` —
  `HelloOk { credits: shared.cfg.per_conn_inflight, … }`. A config constant
  (default 256, `config.rs:123`), not a value computed from the window.
- **Per-connection state only**: `uc2_gateway/src/conn.rs:51-95` holds
  `credits`, `inflight`, `squeezed`, and a gate condvar — all per-`Conn`.
  `Shared` (`edge.rs:299-328`) has the connection table and stats and **no
  aggregate counter** of submits outstanding across connections.
- **The reactive ladder**: on `SubmitError::Backpressure` the reader halves
  that connection's credits once (`Conn::squeeze`, `conn.rs:324-329`, floor 1)
  and doubles them back on every completion (`Conn::relax`, `conn.rs:337-360`,
  called at `edge.rs:1328`). Multiplicative both ways, per connection,
  uncoordinated.
- **The shared arbiter**: `uc2_client::Engine`'s `SlotTable`
  (`engine.rs:273`, capacity `max_inflight`, default 4096). `SendHalf::send`
  (`engine.rs:290-319`) maps a full table **and** a full ingress ring to the
  same `Backpressure` variant (`engine.rs:313-316`, `:416`).
- **Config validation** checks only `per_conn_inflight ≤ max_inflight`
  (`config.rs:162-167`). Nothing relates `per_conn_inflight × max_connections`
  to `max_inflight`: at defaults, 1024 × 256 = 262,144 grantable credits
  against a 4,096-slot window.
- **A signal that exists and is unused**: `SendHalf::inflight()`
  (`engine.rs:368-370`) exposes live window occupancy. The edge never reads
  it for credit sizing.

## How it collapses (the mechanism, from the fleet evidence)

Fleet: 4× `c6id.2xlarge`, 3 node hosts + 1 client host, envelope off, each
client at inflight 1024 (`docs/benchmarks/uc2-m12-gate-2026-08-22.md`,
"Edge saturation ladder" + "Clean-discipline re-run").

| N conns | aggregate resp/s | edge process CPU | client host CPU | lost |
|---:|---:|---:|---:|---:|
| 1 | ~141–146k | 1.6 cores | 26% | 0 |
| 2 | ~217–225k | 2.5 cores | 57% | 0 |
| 4 | **408–451k** | 3.3 cores | 85% | 0 |
| 8 | **11–13k** | 6.7 cores | **3%** | 0 |
| 16 | 2–4k | 7.9 cores | 2% | 8–9k |

The chain:

1. Total demand crosses the window. 4 × 1024 fits the ~4k-slot `Engine`
   window; 8 × 1024 does not. The cliff sits exactly there.
2. Every reader that hits `Backpressure` runs the dispatch ladder
   (`edge.rs:1037-1127`): 64 spin-yields, then a 10 µs → 1 ms doubling park,
   re-trying until `request_timeout`, then `RETRY{SERVICE_UNAVAILABLE}`. Eight
   to sixteen reader threads doing this concurrently is the 6.7–7.9 cores.
3. Each connection's credits halve to the floor and double back on any
   completion, independently — nothing lowers the *sum* to fit the window,
   so the readers keep re-colliding. The client host goes idle (3%): every
   client is blocked on credits or a `RETRY` backoff, starved.
4. The edge's CPU burn lands on the same host as the leader's node and
   service (busy-spin agents that need their cores). They starve; commit
   throughput falls; completions slow; the ladder parks longer; more
   readers pile in. Congestive collapse, not saturation.

It reproduced identically with the edge's `max_inflight` at 65536 and at
4096, which is what falsified the "misconfigured cap" reading: with each
client asking 1024, no per-connection cap between those values was ever the
binding constraint — the missing *global* one was.

## Why the earlier gates did not see it

- The row-2 gate measured **one** connection against one shmem client; the
  single connection is capped by its own syscall-bound relay (~100–145k/s)
  long before the window matters.
- The local smoke ran everything on one oversubscribed 4-vCPU box, where
  the edge, node, service and clients contend for cores at *every* N, so
  a CPU-driven collapse is indistinguishable from the box being small.
- The remote lincheck capstone runs concurrency 4 — under the window.

## The operating envelope in `v2.6.0` (documented, not fixed)

Sum of client inflight across all connections to one edge must stay under
the `Engine` window (`max_inflight`, default 4096); within it the edge
aggregates near-linearly. For co-located deployments, bound the edge's CPU
(`CPUQuota=` in the unit) so a churning edge cannot starve its own node.
See `docs/how-to/run-a-gateway.md` "Operating envelope".

## Fix direction — built in 2.7.0

Implement §4.2 as written: a global outstanding-grant budget on `Shared`,
sized from the `Engine` window (`SendHalf::inflight()` is the live read),
distributed across connections and re-sized as they come and go; reserve
in `dispatch` beside `Conn::reserve`, release in `handle_completion` beside
`conn.relax` (`edge.rs:1328`). Then `Backpressure` becomes rare (credits
never over-promise the window), the per-connection ladder is the exception
path, not the mechanism, and the churn has nothing to churn on. Wire
protocol v1 already carries everything needed (`HELLO_OK{credits}`,
`STATUS`, credits on `RESPONSE`) — the change is edge-internal.

**As built** (`uc2_gateway/src/edge.rs`, `conn.rs`): `Shared` carries
`budget = max_inflight − max_inflight/8` and a `live` count;
`grant_for(live, budget, per_conn)` is the share; `Conn::ceiling` is dynamic
and `relax` climbs to it; under a `grant_lock`, a handshake counts itself in and
recomputes every already-attached connection's smaller share, sets its own
ceiling, and marks itself ready — all **before** it releases the lock and
`HELLO_OK` names a grant, which is what makes "the sum never exceeds the
budget" true at every instant and not merely eventually (an earlier
settle-wait design admitted against a stale `live` under concurrent connects;
see the spec §5 erratum); a reduction is
pushed as `STATUS`, including on `Conn::squeeze`, which had no call site at
all in `2.6.0`. `SendHalf::inflight()` is still not read — the budget is
sized from the window, not sampled from it, which needs no per-request
atomic load and cannot oscillate.
