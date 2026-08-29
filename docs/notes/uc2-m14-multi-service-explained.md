# One log, N state machines — how M14 multi-service works

*Written 2026-08-29 for the 2.8.0 release. Coverage: see the gate doc's
coverage statement; the two-FSM capstones are M14c2.*

## Why N FSMs behind one log

Two applications want the same consensus plane. Before M14 the only way to
give it to them was two clusters: two logs, two journals, two leader
elections, two sets of hosts to keep alive — and no ordering between them, so
nothing either one applies can be reasoned about relative to the other. M14
gives them one log instead. Every declared FSM applies **every** committed
frame; the log carries no service id and does no routing (design spec
[§1](../superpowers/specs/2026-08-21-uc2-multi-service-design.md), "Command
delivery: broadcast"). Two FSMs behind one log see the same commands in the
same order at (nearly) the same position, and that ordering is free — it is
the log's, not something the applications have to arrange.

What stays exactly as it was: one leader, one commit position, one journal,
one archive, one election. `uc2_consensus`, the log frame, the UDP datagram
header, the crypto plane and the ingress ring are untouched. An FSM is not a
consensus participant; it is a reader of the committed byte stream, and M14
adds readers.

What is new is per-FSM *plumbing*, all of it on the node's shared-memory
surface. `cnc2.dat` grew a second 4 KiB page holding `ServiceSlot[8]`, one
512-byte slot per id, one writer per cache line
(`uc_protocol/src/v2/cnc.rs:266-268` — `CNC_OFF_SERVICE_SLOTS = 4096`,
`CNC_SERVICE_SLOT_STRIDE = 512`, `CNC_MAX_SERVICES = 8`). Page 1's last free
line carries the declared set and the lag policy as two plain words
(`cnc.rs:246-251`: `services_declared` at 4032, `fsm_lag_bytes` at 4040 —
with a const-assert that page 1 is now exactly full). The node creates
`svc_query.<id>.ring`, `egress_service.<id>.broadcast` and `snapshots/<id>/`
for every declared id; each FSM takes `service.<id>.lock` at attach and is
refused with `ServiceNotDeclared` if its bit is clear
(`uc2_service/src/attach.rs:78,95-103`).

The declared set is `[services] ids` in `node.toml`, static, and must be
identical on every node — it is not a live-reconfiguration surface
([Configuration § `[services]`](../reference/configuration.md#services)). Ids
run `0..8`, and **id 0 must be declared**: it is the default responder and the
only FSM a remote client can reach, because the remote protocol stays v1 and
its `SUBMIT`/`QUERY` frames carry no service selector
(`uc2_remote/src/frame.rs:19`, spec §6.4). Both facts are pinned as hard
limits ([Limits § Hard limits](../reference/limits.md#hard-limits)).

## The lag barrier

If two FSMs read the same log at their own pace, one of them will be slower.
The question M14 had to answer is what happens then, and the answer that looks
kindest — let it drift, it will catch up — is the one that kills the node.
`uc2_node/src/services.rs:12-14`:

> The FSM pacing policy (spec §1, "FSM pacing"). There is deliberately no
> unbounded variant: an FSM slower than the log's sustained rate can never
> catch up from journal replay, so "unbounded" is a silent death spiral.

The reason is mechanical. An FSM that drifts far enough falls off the live
log buffer (`Overrun`) and must rebuild from the journal — and journal replay
is strictly *slower* than live apply, because it reads from disk what the
live path reads from a warm mmap. So the moment it starts catching up it is
losing ground faster than before, and the gap never closes again. There is no
degradation curve here, only a cliff, and it is quiet: nothing errors, the
FSM just recedes.

So the lag is always bounded, and the operator picks the bound.
`FsmLag::Bounded(bytes)` means `applied_a − applied_b ≤ bytes` for any two
declared FSMs; `FsmLag::Lockstep` means no FSM starts frame *k+1* until every
FSM finished frame *k* (`services.rs:16-21`). The default is
`buffer_bytes / 4` (`services.rs:105-107`), and a bound at or above
`buffer_bytes / 2` is a named startup refusal, because the other half of the
ring is the appender's overrun margin plus the leader's admission window — a
"bounded" policy that cannot provably keep every FSM on the ring is not a
bound at all (`services.rs:117-134`).

The implementation is a **target cap**, not a lock. Before each batch the
apply loop takes `floor = min(slot.applied)` over the declared ids — N acquire
loads, no stores (`uc2_service/src/lag.rs:35-43`) — and turns the policy into
a position: `target = min(head, floor + lag)` for bounded, or one frame,
proceeding while `cursor <= floor` and waiting once `cursor > floor`, for
lockstep (`lag.rs:45-68`).
`LogFollower::next_batch(target)` then yields only frames whose *end* is at or
below the target (`uc2_service/src/apply.rs:311-348`). A capped batch simply
reads as if the log were idle; there is no barrier object, no shared lock, and
the whole thing is shmem-local — it runs identically on the leader and on
every follower, and never involves the node.

What an operator sees when a bound is doing work:
`uc2_service_lag_bytes{service}` (= `commit − applied`) sitting at
`uc2_fsm_lag_bytes`, `uc2_service_lag_waits_total{service}` climbing on its
siblings, and `Uc2ServicePinnedAtLagBound` firing after 30 s
(`uc2_node/src/obs/metrics.rs:341-356`,
`packaging/prometheus/uc2-alerts.yml:93-110`). `lag_waits` counts wait
*episodes* — the `false → true` edge — not cycles, so it reads as "how often
did this FSM have to stop", not "how long did it spin"
(`apply.rs:236-238,329-333`).

One as-built caveat the spec records: the pairwise bound is a cap on *live*
apply and does not cover journal replay, so on a follower whose FSM *k* is
stuck, a sibling rejoining via `replay_into` can transiently exceed it. That
is safe (applying more of an already-committed log is always sound, and only
the leader publishes responses) and it cannot happen on the leader, whose
admission door enforces the bound before a frame is appended (spec §4.2,
as-built errata).

## Lockstep and what it costs

Lockstep is the degenerate bound: one frame. It buys a strong statement —
when `min(applied)` passes a frame's end, *every* FSM's response for that
command exists — and it charges for it per frame, because every frame now
needs each FSM to observe every other FSM's `applied` store. That is an N-way
cross-core handshake, and on the dev box it measured about 1.6 µs per frame
(smoke, not a bar:
[M14a apply-hop](../benchmarks/uc2-m14a-apply-hop-2026-08-27.md)). Bounded
mode, by contrast, is essentially free of N on the same harness — the floor
loads over up to eight slots cost about 1 % from N=1 to N=8.

The interesting part is how lockstep was nearly shipped 34× slower than that.
The apply agent's idle strategy is a 50 µs sleep. A lockstep FSM that finds a
sibling not yet at its frame took `Wait`, broke out of the loop, and slept —
and *that sleep stalled every sibling's next frame*, so their waits exhausted
too, and the whole set fell into sleeping in lockstep at ~18 k frames/s. The
fix (`uc2_service/src/apply.rs:634-653`, `lockstep_wait`) is a ladder: 256
spins, then 2 048 yields with a heartbeat refresh every 256, and only then the
agent's sleep. Measured 631 k frames/s at N=2 on the same box.

Two lessons generalise past lockstep, and both are in CLAUDE.md now:

- **A barrier wait must never sleep on a live peer.** The yield budget has to
  exceed *any* plausible handshake, not the common one — because the failure
  is not "this wait was slow", it is a cascade: one sleeper makes sleepers of
  everyone waiting on it.
- **Code in a hot loop's body costs on paths that never run.** The same ladder
  written inline cost 9 % at N=1 — a path N=1 never executes — purely through
  codegen. Hence `#[inline(never)]` on `lockstep_wait` and the comment above
  it. Symmetrically, a bounded `Wait` deliberately does *not* spin: it is
  `fsm_lag` bytes ahead of the slowest FSM, and spinning on that FSM's
  `applied` line only slows the FSM everyone is waiting for (−6 % at N=8 when
  tried).

Cost of a *dead* sibling under lockstep, accepted by contract: each survivor
burns roughly a core yielding, while the cluster is stalled by design and the
alert fires.

## The quorum-gated report ceiling

The barrier bounds FSMs against each other *on one host*. It does nothing
about the cluster: a follower whose FSM is wedged would happily keep reporting
a durable position it can never apply, the leader would keep committing, and
the "bound" would be local decoration. M14 closes that by capping what a node
*reports*.

`report_ceiling(validated_up_to, min_applied, fsm_lag_eff)` returns
`min(validated_up_to, min_applied + lag)` (`uc2_node/src/services.rs:225-230`),
and that is the value the node publishes for the receiver to clamp every
outgoing `AppendPosition` to
(`uc2_node/src/node.rs:5095-5110`, `publish_validated_frontier`). Two
properties make this safe rather than clever: the ceiling is never above the
validated frontier, so a node never attests content it has not validated (the
wire-0.5.0 content-attestation argument is unchanged); and reporting *less*
than you hold is always sound in Raft — it only delays commit. Elections are
untouched, because `RequestVote` carries the node's own
`(last_term, last_durable)` from `ElectionSm`, never a report (spec §5.3).
The sim carries it as invariant **inv10**: a node's outgoing durable report
never exceeds its own apply ceiling (`uc2_sim/src/invariants.rs:24,207-237`).

The consequence is the liveness coupling M14 deliberately bought. If a
**quorum's** FSMs lag past the bound, the leader's `CommitTracker` cannot
advance, `append − commit` grows, and the leader's admission door closes —
cluster-wide back-pressure, produced by the same predicate that already
guards the window (`node.rs:3535-3540`, a second call of
`admission_open(append, min_applied, lag)` at the existing site). A lagging
**minority** does not stall anything: it looks like a slow follower, gets
paced out of the quorum window, and recovers by journal replay.

The price is the mirror image, and it is real: **one stalled FSM on a quorum
of hosts is a cluster-scope stall.** A declared FSM that never started is the
same thing — it holds `min(applied)` at 0 and caps the report at the bound
from boot (`Uc2ServiceAbsent`, `uc2-alerts.yml:83-92`). This is the intended
outcome, not a bug: the alternative is silently letting the log outrun a
replica's state machine. It also means an FSM is code with cluster-wide
liveness authority — the same class of trust as `apply` itself, which is
already out of model ([threat model §5](../security/threat-model.md#5-out-of-model)).

## Routing and fan-in

Every FSM applies every command; only the *response* is selective, and only on
the leader (`uc2_service/src/apply.rs:380` — followers apply and publish
nothing).

- `try_submit(user_data, cmd)` is `try_submit_to(.., 0, ..)`: FSM 0 answers
  (`uc2_client/src/engine.rs:480-482`).
- `try_submit_to(user_data, id, cmd)` awaits FSM `id`'s response
  (`engine.rs:486`).
- `try_submit_all(user_data, cmd)` awaits the whole declared set and completes
  once, with every answer ordered by id (`engine.rs:508`).
- `query_snapshot_on(id, q)` / `query_linearizable_on(id, q)` are the read
  forms (`uc2_client/src/client.rs:147-164`).

The selector rides in one place only: the `query.ring` payload became
`service_id: u8 ++ query` (`uc_protocol/src/v2/ipc.rs:89-95`). The submit
path needs no selector at all — the log is broadcast, so the id lives entirely
in the client's expectations. `drain_query_ring` splits the prefix, and an id
this node has no ring for is answered `MSG_V2_BAD_SERVICE` on the node egress,
pre-forward and side-effect-free, so the client may simply re-issue
(`uc2_node/src/node.rs:3779-3796,3607-3614`). An undeclared id given to the
SDK fails locally, before any ring is touched (`engine.rs:457-465`,
`SubmitError::ServiceNotDeclared`; wrapped as `ClientError::ServiceNotDeclared`
at `pipelined.rs:378-380`).

So what happens to a command when only one FSM answers? Every declared FSM
applies it, and on the leader every declared FSM publishes a response onto its
own egress broadcast. The client's slot carries an `expected` bitmask; the
poll half reads all N rings, and a response arriving on a ring whose bit is
clear resolves as `Resolve::WrongRing` — dropped and counted, never delivered
(`engine.rs:792-796`). The siblings' answers are produced and discarded at the
client, not suppressed at the source; that is what keeps the FSMs independent
of who is listening. For a fan-in, pieces arrive as `Resolve::Partial` and are
buffered in a `PollHalf`-owned `FanIn` (one entry per slot index, at most 8
pieces, cleared on the generation's first piece and on every terminal), then
handed to the callback whole, sorted by id, when the last one lands
(`engine.rs:244-252,769-786`). The buffer lives on the poll half, not in the
slot, precisely so the slot stays all-atomic and fixed-size.

## Snapshots on wire 0.6.0

A snapshot session used to carry one artifact. With N FSMs it must carry N —
each FSM has its own state and its own `snapshots/<id>/` — and shipping them
as N sessions would let a joiner adopt a floor with only half its FSMs served.
So a session became a **stream of artifacts**: one `SNAP_BEGIN` per declared
id in ascending order, each followed by that artifact's chunks, with
**stream-global chunk offsets**, which is what makes `SNAP_NAK` repair
byte-identical to before.

`SNAP_BEGIN`'s body carries the new fields: `layout` (the body discriminator,
`SNAP_BEGIN_LAYOUT_V2 = 1`, `uc_protocol/src/v2/datagram.rs:174`),
`service_id`, and `services_declared`. It reuses 0.5.0's four-byte pad and
inserts one word, so `SNAP_BEGIN_FIXED_LEN` goes 26 → 34
(`datagram.rs:165-170`). That single payload change is the whole
`0.5.0 → 0.6.0` wire bump; `DATA`, `NAK`, `APPEND_POSITION`, `TERM_MAP`, the
16-byte header and every admin datagram are byte-identical.

The receiver writes each artifact to its own pre-sized `.part`, fsyncs and
renames it as the contiguous frontier passes its end, and **adopts the floor
only once every declared id has landed** — `received == services_declared` —
so no FSM is ever stranded below an adopted floor
(`uc2_net/src/receiver.rs:528-545`). Each FSM then installs its own artifact
and tail-replays: the existing per-id path, untouched.

Two named, counted refusals drop a session outright
(`uc2_net/src/receiver.rs:1643-1657`, counters at `:467-481`, exposed as
`Node::snapshot_session_refusals()`, `uc2_node/src/node.rs:1547`):

| refusal | trigger |
|---|---|
| `peer wire 0.5.0` | a `layout` byte we do not speak, or a body too short to be 0.6.0 at all (`receiver.rs:1590-1601`) |
| `declared-set mismatch` | the sender's `services_declared` differs from ours, or names an id outside it |

Both drop the session and let the joiner keep NAKing. That is the deliberate
choice: **a mixed or mis-declared cluster stalls a joiner rather than
installing half a set.** A half-installed set is a node whose FSM *k* sits
below an adopted floor with no way back; a stalled joiner is a counter and an
alert. The sending side has the quieter counterpart — a leader that cannot
assemble a set covering its declared mask declines to open the session and
says why once, `floor 0` / `missing artifact` / `set does not cover declared`
(`uc2_net/src/sender.rs:1016-1046`, `uc2_node/src/node.rs:957-1004`).

The flag-day terms are the ones every prior wire bump used, with one twist
worth knowing: **nothing on the receive path enforces the version.** The
16-byte header has no version field and `uc_protocol::version::CURRENT` is
documentary. A mixed 0.5.0/0.6.0 cluster therefore replicates and elects
normally — which is exactly what makes it dangerous, because the damage is
confined to snapshot sessions and surfaces later, when a learner joins or a
node falls below the purge floor. Stop every node, swap, start every node:
[Upgrade a cluster § Wire change in 2.8.0](../how-to/upgrade-a-cluster.md#wire-change-in-280-snap_begin-carries-every-fsms-snapshot-060).

## Observing it

Every M10 family that described "the service" now has a labelled twin per
declared id, rendered through `push_service_labeled`
(`uc2_node/src/obs/metrics.rs:273`), a thin wrapper over the same
`push_labeled` the peer-slot band already used (`metrics.rs:156`). The
unlabelled names keep their names and now mean **the slowest FSM** — the
node's consensus agent computes `min(applied)`,
`min(snapshot_pos)`, `min(output_completed)` and the oldest heartbeat over the
declared ids each cycle and publishes them to page 1
(`uc2_node/src/services.rs:150-167`, `uc2_node/src/node.rs:2830`), so the
purge floor, the readiness heartbeat, `uc2ctl status` and the dashboard all
keep reading one number whose meaning widened.

The new families are `uc2_service_attached{service}`,
`uc2_service_lag_bytes{service}`, `uc2_service_lag_waits_total{service}`,
`uc2_services_declared` (the bitmask) and `uc2_fsm_lag_bytes` (0 = lockstep),
all in `CONTRACT_SERIES` (`uc2_node/src/obs/metrics.rs:61-65,333-371`), and
each is rendered for every *declared* id — an FSM that never started is a `0`
sample rather than a missing series, which is what makes it alertable at all
([Monitor a cluster § the per-FSM families](../how-to/monitor-a-cluster.md#the-per-fsm-families-m14)).

The two alerts (`packaging/prometheus/uc2-alerts.yml:83-110`):
`Uc2ServiceAbsent` (`uc2_service_attached == 0 for 30s`, critical) and
`Uc2ServicePinnedAtLagBound` (`lag_bytes >= fsm_lag_bytes for 30s`, bounded
mode only, and gated on the FSM being attached so an absent FSM does not page
twice with the wrong one on top).

`uc2ctl status` prints the declared set, the effective policy, and one row per
declared id — attached, epoch, incarnation, applied, lag, `snapshot_pos`,
heartbeat age — straight off page 2 (`uc2ctl/src/main.rs:522-558`). Including
ids nothing has attached to, because that is exactly the row that explains a
stalled cluster. And the `[log]` transition stream gained
`service_attached` / `service_detached` records, emitted from the node's
per-cycle aggregate pass on an epoch bump and on a heartbeat aging past the
wedged threshold — or on the ATTACHED bit clearing, so an orderly stop is
reported at once instead of after the stale window
(`uc2_node/src/node.rs:2889-2910`).

## What is not there yet

**Remote-path FSM selection.** The remote protocol stays v1 and its request
frames carry no selector (`uc2_remote/src/frame.rs:19`), so the gateway relays
every `SUBMIT`/`QUERY` through the local `Engine`'s default calls: a remote
client always gets FSM 0's answer. `submit_to`, `submit_all` and `query_*_on`
are shmem-only. A selector is a protocol-v2 item (spec §6.4, §11).

**A datagram header version field.** There is none, and 0.6.0 did not add one
— the 16-byte header is full, and growing it would be its own flag day. The
`layout` byte gives the 0.6.0 side a check on `SNAP_BEGIN` specifically; that
is the whole of it (spec §14.3).

**The two-FSM capstones.** Quoting spec §15.1:

> **M14c2 moves *after* the release** (ruling 2026-08-29, reversing §14.1's
> order): the §12 capstones (`lin_v2 two_fsm`, `lin_partition_v2` with two
> FSMs, the two hard-crash scenarios, the elle two-FSM tier) and the M14c
> plan's "Deferred to M14c2" list land as a proof-only `2.8.1`. `2.8.0`
> therefore ships multi-service with the coverage VERIFICATION §11 states
> today — unit, in-process integration on one node and a 3-node cluster, the
> M14b sim scenario, the fuzz seeds — **and says so** in the gate doc, in
> VERIFICATION §11 and in the release notes. This is a disclosed gap, not a
> claim.

The fleet rows that *are* adjudicated for this release, and their
pre-committed bars, are in
[the M14 gate doc](../benchmarks/uc2-m14-gate-2026-08-29.md); it carries the
same coverage statement.

**The deferred minors.** M14c's triaged list — sender refusal-path unit
tests, a receiver-side intake timeout, the bounded-mode `lag_waits`
undercount, an alert tolerance for non-dividing frame sizes and the rest — is
in `docs/superpowers/plans/2026-08-28-uc2-m14c-perf-wire-observability.md`
§ "Deferred to M14c2". M14b's are in
`docs/superpowers/plans/2026-08-27-uc2-m14b-query-routing-and-fan-in.md`
§ "Deferred to M14c"; the client-hop perf item there was closed by refutation
rather than by a fix
([M14c client hop](../benchmarks/uc2-m14c-client-hop-2026-08-28.md)).
