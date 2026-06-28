# SyncCore 3d redesign (pass 1) — completion-as-notification on the hot path — Design

**Date:** 2026-06-28
**Status:** Proposed — awaiting review
**Repo of work:** openraft fork `PeterKnego/openraft` branch `sync-core` (continues the minimal-3d spike, commit `324d5bd5`)
**Predecessors:** `2026-06-28-synccore-phase3-decisions.md` (sequencing + the deferred 3b.2 Tasks 2–3), `2026-06-28-synccore-disruptor-pipeline-design.md` (architecture), `ultima_cluster/docs/benchmarks/synccore-3d-commit-latency-2026-06-28.md` (the measurement that motivates this).

## Why

The minimal-3d spike made the consensus loop synchronous on a dedicated thread, but it
drives every off-thread I/O op with a never-park `block_on` — a **busy-wait** on the
durability consumer. The `commit_latency` microbench quantified the cost:

- single-node inflight=1 p50: RaftCore ~27µs, 3b.2 async-loop ~53µs, **3d sync-loop ~35µs**
  (the sync loop is −34% vs the async loop with the same off-thread arch — busy-spin beats
  futex park — but still +30% vs RaftCore);
- throughput **collapses** under concurrency (conc=64: RaftCore ~1.05M op/s vs 3d
  ~330–540k) because the `block_on` busy-wait **serializes** the consensus thread: it cannot
  drain the client backlog while spinning on one op's completion.

The fix is the decisions-doc's "completion-as-notification": the consensus loop must not
busy-wait on I/O — it submits and continues, and completions come back through the loop's
normal inputs (the engine already models this). This pass does the **hot path only**
(append + per-commit save_committed), then re-measures.

## Key prior facts (established by source reading)

1. **The engine already does completion-as-notification.** `Command::Respond` /
   `UpdateIOProgress` carry an `IOFlushed` condition and are *postponed*
   (`postpone_command`) until `io_state.log_progress.flushed` advances. Flush completion
   flows natively: `IOFlushed` callback (fired by `log_store.append`) → `tx_io_completed`
   watch → `io_completion_forwarder` task → `Notification::LocalIO` → `rx_notification` →
   `handle_notification` → engine advances `io_state`. **We build no new completion
   plumbing for flush.**
2. **The spike's append `block_on` waits only for *submission* (readability), not flush.**
   Its sole purpose is to keep the "submitted ⇒ readable before a later `Replicate` reads"
   invariant across the consensus→consumer thread boundary. Single-node has no replication,
   so the wait is pure overhead there.
3. **`save_committed` is optional/advisory.** The `RaftLogStorage::save_committed` trait
   method defaults to a no-op; openraft explicitly tolerates a lagging/absent committed
   marker (recovery re-applies up to it, or `wait_for_recovery` blocks). It is a recovery
   optimization, **not** a safety invariant, so it need not be durable before apply.
4. **Apply is already fire-and-forget.** `apply_to_state_machine` drains the client
   responders, calls `on_commit`, and hands `sm::Command::apply` (with the responders) to
   the `sm::Worker` via an mpsc send — the worker applies and responds to clients. The loop
   does not wait for apply to finish; apply completion advances `io_state.apply_progress`
   via the worker's notification (the `Condition::Applied` path).

## Scope (this pass)

In: **`AppendEntries` fire-and-forget + readability gate**, and **`SaveCommittedAndApply`
fire-and-forget save_committed**. Out: `SaveVote` / `PurgeLog` / `TruncateLog` (rare —
election/compaction; stay on `block_on`), pulling apply inline / reactor-free SM (Phase
3e), core pinning, UDP (3e), the 3c A/B (replication off RaftCore).

## Change 1 — `AppendEntries` fire-and-forget

In `SyncCore::run_command`'s `AppendEntries` arm: keep the before-work (set
`io_accepted`/submit markers, stats) and publish the op to the durability consumer, but
**remove** the `done` oneshot and the `Self::await_completion(rx).await?`. Drop the `done`
field from `DurabilityOp::Append`. Flush completion is unchanged — the `IOFlushed` callback
(built from `io_id` + `tx_io_completed`) still drives the `LocalIO` notification path that
advances `io_state.log_progress.flushed`. **Submission-error propagation:** previously a
submission error (`append()` returning `Err`) was surfaced through `done` and `?`-ed to
Fatal on the loop — matching RaftCore, where `append().await?` makes a submission error
fatal. With `done` removed, the `IOFlushed` callback never fires on a submission failure,
so the consumer must explicitly send `Err(storage_error)` into `tx_io_completed` (the same
watch the callback uses); the forwarder turns it into a `LocalIO` notification carrying the
storage error, which the engine handles as a fatal storage error — same outcome, via the
notification path. (In-memory `append` does not fail, so this is correctness-completeness,
not a hot path.)

## Change 2 — readability gate (gated reader + consumer watermark)

Makes Change 1 safe multi-node. Fully contained to `sync_durability`:

- **Watermark.** The consumer owns a `watch` whose value is the highest `LogId` whose
  `append()` has returned (the contract's *readable* point). In `run_op`'s `Append` arm,
  after `block_on(log_store.append(...))` returns, update the watch to that op's
  `last_log_id` (carried in `io_id`). Monotonic by FIFO consumer ordering; assert it.
- **Gated reader.** The reader-vending path (already rerouted through the consumer in
  3b.2 via the reader-request channel) wraps each vended `LS::LogReader` in a
  `GatedLogReader<C, R>` holding a clone of the watermark `watch` receiver. It implements
  `RaftLogReader<C>`: `try_get_log_entries(range)` first awaits `watermark >= range.end-1`,
  then delegates to the inner reader; other methods delegate unchanged.
- **Why correct & cheap.** It gates at the *actual read*, so correctness is independent of
  `Replicate`'s emission timing and the eager submit marker. All external log reads
  (replication, snapshot-building) go through the single reader-vending path, so gating
  every vended reader covers them uniformly. The await runs in the *replication task's*
  tokio context — it overlaps, never blocking the consensus loop. Single-node vends no
  readers and does no log reads → zero cost.

## Change 3 — `SaveCommittedAndApply` fire-and-forget save_committed

In the `SaveCommittedAndApply` arm: keep the before-work (`apply_progress.submit`, stats),
**publish `SaveCommitted` without the `done` wait** (drop `done` from
`DurabilityOp::SaveCommitted`), then call `apply_to_state_machine(first, upto)` as today.
Safe per fact (3): the committed marker is advisory and FIFO ordering keeps it monotonic;
apply does not depend on it being durable. Apply remains fire-and-forget per fact (4).

## What stays on `block_on` (unchanged this pass)

`SaveVote`, `PurgeLog`, `TruncateLog` keep their `done` oneshot + `block_on` wait. They are
off the steady-state hot path (election / log compaction) and their after-work touches
consensus state on the loop; deferring them is a later pass if the re-measurement shows
they matter.

## Testing & verification

**Correctness (green at every step; the suite is the oracle):**
- `cargo test -p tests --features sync-core` — full 180 integration suite. The gate is
  exercised by multi-node replication/membership/snapshot tests (they read the log while
  writes are in flight); a wrong gate fails them deterministically (a too-low watermark
  hangs a read → test timeout, not silent corruption).
- `cargo test -p openraft --features sync-core --lib` — 495 lib tests; extend the
  `sync_durability` consumer test to cover the watermark update + a gated read that blocks
  until the watermark advances.
- Default RaftCore path unaffected: re-confirm `-p openraft --lib` (494) + clippy in both
  feature states.

**Performance (the point):** re-run `commit_latency` A/B (RaftCore vs redesigned-3d) on the
same harness — single-node inflight=1 latency and the 16/64/256 concurrency sweep.
**Success criterion:** redesigned-3d **≥ RaftCore at inflight=1 AND under concurrency**.
Expected: the inflight=1 gap closes (no append / save_committed round-trip) and the
throughput collapse reverses (loop no longer serialized per-write). If it does not fully
flip, the remaining suspects are the durability *thread hop* itself and 4-core
oversubscription (→ core pinning) — both visible in the numbers and addressed in later
passes.

**Order of work (bisectable):** Change 1+2 (append fire-and-forget + gate) → run suite →
Change 3 (save_committed fire-and-forget) → run suite → measure.

## Risks

- **Gate hangs instead of races.** A watermark that never reaches a requested index hangs
  the read. Mitigated by: monotonic FIFO updates, the watermark keyed on the same `io_id`
  the loop submits, and the suite's timeouts surfacing any stall.
- **Generic `GatedLogReader` over `LS::LogReader`.** Must implement the full
  `RaftLogReader<C>` surface (delegate all but `try_get_log_entries`). Low risk; the suite
  covers every reader method.
- **Apply backpressure under load.** `apply_to_state_machine`'s `sm_handle.send().await`
  can block under `block_on` if the sm worker's mpsc fills. Out of scope this pass; watch
  for it in the concurrency re-measurement and address with the apply-hop work (3e) if it
  appears.
- **Fork divergence.** Continues to grow the `sync-core` diff vs upstream; the 180-suite
  keeps it honest.

## Next

On approval → writing-plans for a bite-sized, suite-guarded implementation plan, executed
the same way 3a/3b ran. After this pass measures, return to the decisions-doc sequencing:
the remaining ops, then the 3c A/B (replication off RaftCore) with data in hand.
