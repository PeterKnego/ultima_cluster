# uc-side Service-State Reconstruction — Design

**Date:** 2026-06-06
**Status:** Design — approved in brainstorming, pending spec review
**Author:** Peter Knego (with Claude Code)
**Related:** task12 (linearizability harness — surfaced the contract gap), task10 (eventual log durability), task27 (`ultima_db` snapshot_stream).

## 1. Goal & framing

Make the node **reconstruct a reconnecting or cold-starting service's state machine
from the Raft log and/or a snapshot**, so that purely in-memory state machines are
**first-class** — they survive a service-only restart (node stays up) and a full
cold start without any per-SM persistence.

Today the node does *not* rebuild a reconnecting service: the service's SM is
handed straight to the apply loop, and the node never queries the service's
`last_applied` nor replays anything. An in-memory SM therefore loses all state on a
service-only restart; only self-persisting SMs (e.g. `StoreStateMachine` →
`ultima_db`) survive. This was caught by task12, which had to make the test
`RegisterSm` self-persisting to keep the service-crash failover test green.

### Why this is the right boundary

`apply` is deterministic and keyed by `log_index`. The Raft log is the source of
truth. So a service at `service_last_applied` can be brought to the node's current
apply frontier by **replaying the committed entries strictly above it** — and, when
those entries have been purged, by **installing a snapshot then replaying the
tail**. The service reports exactly what it has; the node replays strictly above;
no entry is applied twice or skipped. Self-persisting and in-memory SMs go through
the *identical* path — the only difference is the size of the gap. Self-persistence
becomes a performance optimization (smaller replay), not a correctness requirement.

### Scope (YAGNI)

**In scope:** the full reconstruction path (log-replay + snapshot fallback +
cold-start parity), phased. **Out of scope:** network partition / quorum-loss
testing; deterministic simulation; the `kill -9` mid-apply hard-crash fault
(separate task12 follow-up); rich service→node control messaging beyond the single
`last_applied` scalar (see §4 channel decision).

## 2. Reconstruction flow (the core mechanism)

**Principle:** reconstruction is **node-driven and out-of-band from openraft**.
openraft only ever drives the *live* apply frontier forward; rebuilding a lagging
service from history is the node's job, sourced from the journal/snapshot, gated so
live applies never overtake catch-up.

> **What openraft already handles (do not rebuild) — corrected 2026-06-06.**
> The durable `last_applied` `StableValue` only advances at **snapshot cadence**,
> not per-apply (`state_machine.rs` / `recovery.rs`). On a **full cold restart with
> no snapshot yet**, openraft recovers `committed` from the journal, sees it exceeds
> the durable `applied`, and **re-applies `(applied, committed]` through the normal
> apply path** — which reconstructs a fresh in-memory service for free. So cold-start
> reconstruction is **already correct when no snapshot has been taken**; the only
> cold-start gap is when a snapshot exists and the prefix is purged (→ Phase 2
> snapshot-install). The node-driven catch-up below is therefore meaningful **only
> when `node_frontier` (the node's *live in-memory* applied index) is ahead of the
> service AND openraft will not re-drive that range** — i.e. the **mid-life reattach**
> case (service crashes and reconnects while the node keeps running and stays at its
> in-memory frontier). That is the real task12 gap and the primary target of Phase 1.

Sequence on (re)attach (the mid-life reattach case):

1. **Service attaches** (existing `attach.rs` flow). Before flipping `READY`, it
   reads its SM's `last_applied()` (in-memory → `None`/`0`; persistent → its durable
   value) and writes it into `ServiceStatus.last_applied` (Release), then sets
   `state → READY` (Release).
2. **Node reads the frontier.** On observing `READY` (Acquire) in
   `wait_for_service_ready`, the node reads `service_last_applied` and compares to its
   **current in-memory apply frontier** `node_frontier` (the accurate runtime value,
   not the snapshot-cadence durable `StableValue`).
3. **Decide the catch-up source:**
   - `service_last_applied == node_frontier` → nothing to do; open the gate (today's
     behavior).
   - `service_last_applied ≥ last_purged` → **log-replay** `(service_last_applied,
     node_frontier]`.
   - `service_last_applied < last_purged` → **snapshot-install** the latest snapshot,
     then **log-replay** the tail `(snapshot_index, node_frontier]`.
4. **Catch-up apply.** The node reads committed entries from the journal
   (`try_get_log_entries`) and publishes `ApplyFrame`s to `service/apply.ring` in
   index order, draining `apply_resp` acks with the existing `await_apply_resp`
   helper. Because these entries are already committed from openraft's view, the node
   feeds them **directly, bypassing `openraft.client_write`** — so there is no
   submit→broadcast path, hence no stale client `response.broadcast` and no
   `output_progress` advance to suppress; it falls out naturally.
5. **Gate.** `apply.ring` is **SPSC (single producer = the node)**, so catch-up and
   live applies must serialize. Catch-up holds the `ShmemInner` apply-producer lock
   (the same lock the live apply path takes) and runs to `node_frontier`; openraft's
   live `apply()` blocks on that lock until catch-up completes, then resumes at
   exactly `node_frontier + 1`. New commits arriving during catch-up simply wait for
   the lock and apply live afterward, in order.

### Correctness

- No double-apply / no gap: service reports its true frontier; node replays strictly
  above it; `apply` is deterministic + `log_index`-keyed.
- Ordering: a single ordered SPSC producer (the node), historical-then-live, gated by
  one lock.
- `service_last_applied` is sourced cheaply from a cnc atomic (see §4); the catch-up
  target is the node's accurate in-memory frontier.

## 3. Phasing

All phases are in scope. **Each phase is its own implementation plan and its own
PR.** Sequenced; later phases build on earlier ones.

> **Decomposition corrected 2026-06-06** (after an aborted "Phase 1a cold-start
> log-replay" attempt — see `…/plans/2026-06-06-uc-service-state-reconstruction-phase1a.md`,
> now SUPERSEDED). That attempt revealed cold-start is already handled by openraft
> (§2 callout), so a standalone cold-start log-replay phase was a mirage. The real
> task12 gap is **mid-life reattach**, which is now Phase 1.

1. **Phase 1 — mid-life reattach reconstruction.** The task12 gap: a service
   crashes and reconnects while the node keeps running. **Not additive** — today the
   node's `apply()` parks indefinitely on the apply rings and a reattached service
   resumes the persistent SPSC cursor mid-stream (exactly what loses in-memory
   state). Reuses the **channel-A handshake** (service publishes `last_applied`) and
   the **node-driven catch-up driver** (`decide_catchup_source` + replay over the
   apply ring) — both prototyped in the aborted attempt and to be re-derived here in
   the reattach context. Adds the reconnect-path redesign: **reattach/epoch
   detection**, **apply-ring cursor reset** to feed from `service_last_applied+1`,
   abandoning the parked apply, and the **gate** holding live applies until catch-up
   completes. `node_frontier` is the node's *live in-memory* applied index (the value
   openraft will not re-drive). **Needs a focused design pass on the reconnect
   mechanics (cursor reset is the crux) before planning.** No wire-format change.
2. **Phase 2 — real snapshot path** via `snapshot.region` (§5). Both directions:
   BUILD (service→node) and INSTALL (node→service). Closes the cold-start+snapshot
   gap, the below-purge reattach case, and the latent safe-purge hole (see §5). The
   bulk of the effort. **Split (decided 2026-06-06) into correctness-first / opt-second:**
   - **Phase 2a — functional snapshot path.** Real bidirectional BUILD+INSTALL over
     `snapshot.region`, using the **existing blocking** `build_snapshot(&self,dst)` /
     `install_snapshot(&mut self,src)` — **NO trait change**. Wires the reattach
     `NeedsSnapshot` path to install-then-tail-replay, and backs log-purge with real
     snapshots. Closes all the correctness gaps. `StoreStateMachine` already
     implements these methods over `ultima_db::snapshot_stream`.
   - **Phase 2b — async build.** The `freeze`/`stream_snapshot`/`SnapshotHandle`
     trait change (§6) so snapshot BUILD doesn't stall applies. Pure optimization;
     maps cleanly onto `ultima_db` (`freeze` = `store.snapshot_stream(version)`'s
     reader, which already pins an MVCC version under a brief lock). Separate PR.

   **Transport decision (2026-06-06): `snapshot.region` is a SEPARATE
   dynamically-sized mmap file** under `instance_dir/service/` (like the ring files —
   NOT a fixed-size cnc sub-buffer). Investigation note: there's no existing
   dynamic-mmap precedent, so the writer builds bytes to a buffer, `ftruncate`s the
   region to length, then writes; the reader mmaps that length and validates a header
   (magic, len, snapshot `last_log_id`, crc).
3. **Phase 3 — contract flip + parity cleanups + proof** (§6, §8).

**Not a phase (already works):** cold-start reconstruction when no snapshot has been
taken — openraft re-applies `(applied, committed]` itself (§2 callout). A loud error
on the unsupported cold-start+snapshot+in-memory case lands naturally with Phase 2.

## 3a. Phase 1 reattach mechanics (design pass, 2026-06-06)

Grounded in the SPSC ring + apply-pipeline internals:

- **Ring cursors are in the shared header** (`publish_position`, `consumer_position`,
  both `AtomicU64`), so they survive a service restart — a fresh `SpscConsumer`
  resumes from the persisted `consumer_position` (the bug). A "reset" = advance
  `consumer_position` to `publish_position` (discard stale unconsumed frames).
- **`apply()` is strictly serial: at most ONE entry in flight.** It publishes N, then
  `await_apply_resp(N)`, then loops. So on a mid-apply crash there is exactly one
  unacked frame and `node_frontier = N` (apply sets `last_applied = Some(N)` *before*
  publishing). This single-in-flight invariant is what makes the design tractable.
- **The parked `apply()` holds the `inner` lock** while awaiting resp N, so a
  separate task cannot drive catch-up concurrently — the parked apply must handle it.
- **`await_apply_resp` treats any log_index mismatch as fatal**, and openraft treats
  an `apply()` `Err` as fatal — so we must NOT abandon the parked apply with an error,
  and replayed resps must not trip the ordering check.

### Decisions

1. **Detection — explicit `service_epoch` (default; overridable).** Add
   `service_epoch: AtomicU64` to `ServiceStatus`. Each new service incarnation bumps
   it at attach **before** flipping `READY` (Release, ordered with the state flag like
   `last_applied` in channel A). The node tracks the last-seen epoch; a change = a
   reattach. Monotonic and unambiguous (vs. `service_pid` reuse).
2. **Reattach-aware, self-driving apply.** `await_apply_resp` wakes not only on the
   resp-ring signal but on an epoch change. When the parked `apply(N)` observes a
   reattach, *that same call*: (a) resets the apply + resp ring cursors (discard the
   stale frame N and any stale resp from the dead incarnation), (b) replays
   `(service_last_applied, N-1]` from the journal — publishing each and consuming its
   resp **itself** (it owns the ring exclusively, so the strict ordering check holds
   within the replay and there is no contention), (c) re-publishes N, awaits resp N,
   returns it. openraft never sees an error; the single-in-flight invariant guarantees
   one coordinator. For the **idle** case (crash with no apply parked), the next
   `apply()` checks the epoch at the top and runs the same catch-up before publishing.
3. **Proactive + lazy trigger.** A reattach watcher (extend `service_watcher`)
   detects the epoch change and signals "reconstruction needed". If an `apply()` is
   parked (holds the lock), it self-drives (decision 2). If the node is idle (no lock
   held), the watcher-triggered coordinator takes the `inner` lock and drives catch-up
   `(service_last_applied, node_frontier]` proactively — so queries to the
   reattached service aren't answered from stale/empty state while waiting for the
   next commit. Both paths take the same lock, so they serialize; exactly one runs.

### Open implementation risks (resolve in the plan / a spike)

- **Waking `await_apply_resp` on epoch change — RESOLVED by spike (2026-06-06).**
  No new wakeup plumbing or `select!` cancel-safety is needed. `NotifyBridge`'s
  parker fires `notify_one` at least every `PARK_CEIL` (2 ms) as a backstop
  (`ring_bridge.rs` module doc: "correctness never depends on the wake"), so the
  await loop just adds an `if epoch_changed() { return Reattach }` check at the top
  and rides the existing backstop. A throwaway spike (real `SpscRing` +
  `NotifyBridge`, epoch bumped with zero ring traffic) detected the change in
  **~8 ms** (bump @5 ms + ~one backstop) — fine for a recovery path. The epoch
  atomic is the source of truth; the Notify is only for liveness, so a lost wakeup
  is non-fatal (the next backstop re-checks). An optional reattach `Notify` could
  cut the latency but is not required for correctness.
- **Idempotent re-apply of N for self-persisting SMs.** The dead incarnation may have
  applied N before crashing. Re-applying N is safe for `StoreStateMachine` (version
  pinned to `log_index`) and for in-memory SMs (deterministic); document the
  requirement. `log_index` remains the idempotency key.
- **Resp-ring reset vs. a late resp from the dead incarnation.** Resetting the resp
  `consumer_position` to its `publish_position` discards any straggler resp; confirm
  no torn read across the reset.
- **`node_frontier` source.** Must be the **in-memory** `last_applied` (current), not
  the durable StableValue — correct here because the node stayed up.

## 4. Channel: how the node learns `service_last_applied` (Decision: A)

The service publishes its recovered `last_applied` into the **existing**
`ServiceStatus.last_applied: AtomicU64` field in `cnc.dat` (cnc.rs:91), before
flipping `state → READY`. The node reads it (Acquire) when it observes `READY`
(Acquire) — the existing state-flag Release/Acquire edge guarantees the
`last_applied` write is visible and untorn.

- **Why A over the `ServiceReady` MPSC frame (B):** the frame path requires first
  building the deferred cnc-sub-mmap MPSC attach machinery on the node side, just to
  deliver a single `u64`. The atomic reuses a field that already exists and a
  handshake the node already watches, with zero new transport. If a richer
  service→node control channel is ever genuinely needed, build the MPSC attach then,
  as its own concern.
- **Mandatory rule:** the `ServiceStatus.last_applied` atomic lives in the node's
  mmap and **persists across service restarts**. A restarted service MUST overwrite
  it with its own recovered value (`0` for fresh in-memory) **before** flipping
  `READY`, or the node reads a stale-high value from the prior incarnation and
  under-replays. This is a hard requirement in the service attach path.

## 5. Snapshot path (Phase 2)

### Key insight: real snapshots are also a safe-purge prerequisite

The node holds **no application state** — the SM lives in the service, the node-side
snapshot SM is degenerate (opaque bytes + meta only). Today the log is purged at
snapshot cadence, but snapshots are degenerate (empty). For a self-persisting SM
that's fine; for an **in-memory** SM it is a latent **data-loss hole** — purging
committed entries whose only record of the state was the log, with no real snapshot
capturing it. So "real snapshot BUILD" is not merely the install-fallback enabler;
it is a prerequisite for **safe log purge** once in-memory SMs are first-class.
Hence Phase 2 wires *both* directions.

### `snapshot.region`

A **separate** mmap'd file `service/snapshot.region` under the instance dir (not a
`cnc.dat` sub-buffer — snapshots can be large and would bloat the control file).
Dynamically sized: the writer `ftruncate`s to the snapshot length, the reader mmaps
that length. Region header: magic, byte length, snapshot `last_log_id`, crc; then the
bytes. Payload is the **`ultima_db::snapshot_stream`** format end-to-end. Guarded by
`ServiceStatus.state = Snapshotting (3)`; one snapshot operation at a time.

### BUILD (service → node) — async (Decision)

Triggered by openraft's snapshot policy. **Must not block the apply pipeline**
(a blocking build serializes under the SM read lock → the service stops draining
`apply.ring` → the node's `apply()` blocks on `await_apply_resp` → openraft's
applied-index stalls → a periodic latency cliff every snapshot interval).

The default `StoreStateMachine` is `ultima_db` MVCC copy-on-write: a snapshot at
version `V` (= `last_applied`) is stable under CoW while new versions `V+1…` are
written. So BUILD is async:

1. At the snapshot index the apply thread takes the SM write lock **briefly** to
   capture a cheap consistent handle, `freeze() -> SnapshotHandle` (O(1) for
   ultima_db: pin the MVCC version; the handle carries its `last_applied`).
2. Release the lock — **apply (and query) keep flowing.**
3. A **background thread** runs `stream_snapshot(&handle, region_writer)` with no SM
   lock held, into `snapshot.region`.
4. Service replies `SNAPSHOT_BUILT{last_log_id}` on `control_to_node`; the node copies
   the region to its on-disk snapshot store + records `snapshot_meta`/`last_log_id`.
   Now purge is backed by a real snapshot, and the same bytes satisfy openraft's
   node→node InstallSnapshot RPC (currently fed degenerate bytes via the M2 cursor
   path).

For a non-MVCC in-memory SM there is no free lunch: `freeze` **clones** the state
under the brief write lock (cheap for small SMs like `RegisterSm`), then the clone is
streamed off-thread.

### INSTALL (node → service) — the reconstruction fallback (§2 step 3)

The service is gated (not yet caught up) so this is exclusive — no concurrent applies.

1. Node writes the persisted snapshot bytes into `snapshot.region`, sets
   `Snapshotting`, signals install on `control_to_service`.
2. Service `install_snapshot(Cursor over region)`, acks on `control_to_node`.
3. Node then log-replays the tail `(snapshot_index, node_frontier]` per §2.

## 5a. Phase 2a concrete design (functional path, no trait change — 2026-06-06)

Implementation-shape decisions for 2a, grounded in the snapshot-machinery
investigation. (2b later swaps the blocking build for the §6 freeze/stream trait.)

**Command channel — a dedicated SPSC control-ring file PAIR** (not the cnc
`control_to_service`/`control_to_node` rings, which need the deferred sub-mmap MPSC
attach API). `service/snapshot.ring` (node→service) + `service/snapshot_resp.ring`
(service→node), created in `service_link.rs` + opened in `attach.rs` exactly like the
apply/query/output ring pairs. The service spawns a **snapshot-control loop** thread
that consumes `snapshot.ring`. This is the same lean choice as Phase 1's channel A:
reuse the established per-stream-ring-file pattern, avoid the deferred cnc-ring infra.

**Frames** (`uc_protocol/src/frames/snapshot.rs`): reuse `MSG_TYPE_BUILD_SNAPSHOT`
(100) + `MSG_TYPE_SNAPSHOT_BUILT` (101); ADD `MSG_TYPE_INSTALL_SNAPSHOT` +
`MSG_TYPE_SNAPSHOT_INSTALLED` (next free constants). `header_extra` carries the
relevant `last_log_id` index (built index for BUILT; install target for INSTALL).

**`snapshot.region`** — separate file `service/snapshot.region`. A small
`SnapshotRegion` helper: write = build bytes to a `Vec`, truncate the file to
`HEADER_LEN + len`, write `{magic, format_ver, byte_len, snapshot_index, crc32}` +
bytes; read = open, validate header+crc, return `(snapshot_index, bytes)`.
(Implemented with the snapshot's `last_log_id` **index** only — the term is recovered
node-side from `snapshot_meta`, so it need not be in the region header.)
**Cross-process ordering comes from the control-ring ack** (writer fills region →
sends BUILT/INSTALL → reader reads region only after receiving the frame), so no
atomic fencing on the region itself. One op at a time (request/ack is serial).

**Service-side snapshot-control loop** (new thread, shares the `Arc<RwLock<S>>`):
- on `BUILD_SNAPSHOT`: `sm.read()`, **blocking** `build_snapshot(&mut Vec)` (2a
  accepts the apply stall here; 2b removes it), write the region, reply
  `SNAPSHOT_BUILT{built_index}`.
- on `INSTALL_SNAPSHOT`: `sm.write()`, `install_snapshot(Cursor over region)`, reply
  `SNAPSHOT_INSTALLED{new_last_applied}`. The `RwLock<S>` serializes build/install
  against `apply()`.

**Node-side BUILD** (`ShmemSnapshotBuilder::build_snapshot`, currently degenerate):
send `BUILD_SNAPSHOT` on `snapshot.ring`, await `SNAPSHOT_BUILT`, read
`snapshot.region`, return the real bytes as the openraft `Snapshot`
(`Cursor<Vec<u8>>`) + persist via the existing `snapshot_meta`/on-disk store. Now
openraft snapshots reflect real service state ⇒ **log purge is safe**, and the same
bytes feed the node→node InstallSnapshot RPC. Runs on openraft's snapshot-builder
task (separate from the apply worker; uses the snapshot ring, not the apply ring).

**Node-side INSTALL** (in `drive_catchup`, replacing the `NeedsSnapshot` error): write
the node's persisted snapshot bytes into `snapshot.region`, send `INSTALL_SNAPSHOT`,
await `SNAPSHOT_INSTALLED`, then set the effective `service_last = snapshot_index`
(the snapshot's `last_log_id.index`, from `snapshot_meta`) and **replay the tail
`(snapshot_index, up_to]`** via the existing apply-ring replay loop. Ordering: the
install ack precedes any tail apply publish, so the service installs before it
consumes tail entries.

**`plan_replay` extension:** `NeedsSnapshot` now carries the `snapshot_index` to
replay from after install (or `drive_catchup` reads it from `snapshot_meta`). Keep
the variant; just wire its consumer to install-then-replay instead of erroring.

**Concurrency note:** node-side, BUILD (snapshot-builder task, snapshot ring) and
apply (SM worker, apply ring) use disjoint rings → no node-side contention. Service
-side, the `RwLock<S>` is the single serialization point across apply/build/install.

**Open items for the plan / a spike:** (a) is the node's `build_snapshot` call path
`async` enough to do ring send + await without blocking openraft's snapshot worker
loop (it returns `Snapshot` from an async fn — confirm); (b) the snapshot-control
loop's interaction with `apply_loop` shutdown ordering (join on `Service::shutdown`);
(c) sizing/cleanup of `snapshot.region` across repeated ops (truncate-on-write).

## 6. `StateMachine` trait change

Breaking change (all impls updated): replace `build_snapshot(&self, dst)` with a
freeze/stream split, and make `last_applied()` load-bearing.

```rust
trait StateMachine {
    type Command; type Response; type Query; type QueryResponse;
    type SnapshotHandle: Send;                          // NEW

    fn apply(&mut self, log_index: u64, cmd: Self::Command) -> Self::Response;
    fn query(&self, q: Self::Query) -> Self::QueryResponse;

    /// MUST report the SM's true applied frontier. Published to
    /// ServiceStatus.last_applied at attach; now a correctness requirement.
    fn last_applied(&self) -> Option<u64>;

    /// Capture a cheap, consistent point-in-time view at the current last_applied.
    /// O(1) for MVCC stores; a clone for trivial in-memory SMs. Called under the
    /// brief apply-write-lock; the returned handle is streamed lock-free.
    fn freeze(&self) -> Self::SnapshotHandle;

    /// Stream a frozen handle to dst (background thread, no SM lock). Returns the
    /// log_index represented (resolves the last_applied-vs-snapshot-call race).
    fn stream_snapshot(handle: &Self::SnapshotHandle, dst: &mut dyn Write)
        -> Result<u64, SnapshotError>;

    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<u64, SnapshotError>;
}
```

Impls to update: `StoreStateMachine` (ultima_db, MVCC freeze), lincheck `RegisterSm`
(clone), `KvSm` (autobench, clone), `kv_service` / `counter_loop` examples.

## 7. Error handling

Principle: **reconstruction is best-effort-with-retry; failure never corrupts
node/raft state** — the service is simply not marked usable until it catches up.
Everything logs loudly.

> **Phase 1 implementation reality (2026-06-06):** because the reattach catch-up
> runs *inside* `apply()` (the self-driving design, §3a), an unrecoverable
> reconstruction error there returns `Err` from `apply()`, which **openraft treats
> as fatal → the node shuts down** — it does NOT degrade to "service not marked
> usable" as the principle above envisions. The two error paths that hit this in
> Phase 1: (a) the **`NeedsSnapshot`** case (service below the purge boundary —
> only reachable on a long-running cluster after a snapshot+purge, since
> snapshot-install is Phase 2), and (b) a **journal gap** (the `saw_up_to` guard).
> Both are loud, fail-stop, and non-corrupting, but node-killing rather than
> service-scoped. Phase 2 (snapshot-install) removes (a); revisiting the
> in-apply error surface to degrade gracefully is follow-up work.

- Snapshot-install / log-replay failure (crc mismatch, `install_snapshot` error,
  journal IO): abort reconstruction, don't mark the service ready; service retries
  attach; node keeps replicating (and transfers leadership if leader, per existing
  service-down behavior).
- Mid-replay service crash: detected via heartbeat/pid; abort, await re-attach,
  restart reconstruction from the new `service_last_applied` (idempotent / restartable
  from anywhere).
- `service_last_applied > node_frontier` (impossible — a service cannot apply past
  what the node delivered): treat as divergence/corruption → refuse, loud error.
- Defensive purge race: if a log-replay read comes up short despite
  `service_last_applied ≥ last_purged` (a purge landed mid-decision), fall back to the
  snapshot path.
- New commits during catch-up: serialized by the apply-producer lock (§2).

## 8. Contract change & cold-start parity (Phase 3)

- **`StateMachine::last_applied()` is load-bearing** — document the requirement in the
  trait and CLAUDE.md.
- **Re-enable the node-side user/framework `last_applied` cross-check**
  (`state_machine_shmem.rs:165-171`, currently skipped) now that `service_last_applied`
  is available; refuse on divergence.
- **Revert `RegisterSm` to plain in-memory** in lincheck (drop the task12
  self-persistence workaround from `162b7ad`); a non-persisting SM surviving
  service-crash is the proof the feature works.
- **CLAUDE.md:** update the "service crash → resumes apply when service reconnects"
  and "in-memory SMs lose state" statements to describe reconstruction; note that
  safe purge now depends on real snapshots.

## 9. Components & interfaces (touch map)

**`uc_protocol`:** `ServiceStatus.last_applied` contract (no layout change);
`snapshot.region` file format (header + bytes); reuse `state = Snapshotting (3)` and
the `BUILD_SNAPSHOT`/`SNAPSHOT_BUILT` control frames (+ an install-direction frame if
needed). Phase 1 adds no frames.

**`uc_node`:** `ipc/handshake.rs::wait_for_service_ready` reads
`ServiceStatus.last_applied`; **new** `runtime/reconstruct.rs` driver (source
decision + snapshot-install + journal log-replay under the apply-producer lock);
`state_machine_shmem.rs` exposes catch-up publish/await on the shared lock and the
`node_frontier`; node-side snapshot store gains real BUILD ingest + INSTALL serve.

**`uc_service`:** attach path writes `last_applied()` to `ServiceStatus.last_applied`
before `READY`; snapshot build runs `freeze` + background `stream_snapshot`; apply
loop handles the `Snapshotting` install signal (`install_snapshot` from
`snapshot.region`).

## 10. Testing

- **Unit:** `reconstruct` source-decision (`none` / `log-replay` / `snapshot+tail`)
  over `(service_last_applied, node_frontier, last_purged)`.
- **Integration (`uc_node` tests):** (a) service-only restart, in-memory SM,
  log-replay → state reconstructed, no double-apply; (b) force a purge below
  `service_last_applied` → snapshot-install + tail replay; (c) node+service
  cold-start, in-memory SM → reconstructed; (d) async snapshot build↔install
  round-trip, with applies/queries continuing during build.
- **Lincheck capstone (end-to-end proof):** non-persisting in-memory `RegisterSm`,
  both fault types (node-kill + service-crash) enabled, assert linearizable across
  seeds.
- **Regression:** existing `eventual_durability` + m3 suites stay green.

## 11. Success criteria

- An in-memory SM survives a service-only restart and a full cold start, with state
  reconstructed to the node's apply frontier, verified by the lincheck capstone with
  a non-persisting `RegisterSm`.
- Log purge is backed by real snapshots; a service below the purge boundary is
  reconstructed via snapshot-install + tail replay.
- Snapshot BUILD does not stall the apply/query pipeline (async freeze + background
  stream), verified by an integration test.
- The `last_applied` cross-check is re-enabled; the `RegisterSm` self-persistence
  workaround is removed.
