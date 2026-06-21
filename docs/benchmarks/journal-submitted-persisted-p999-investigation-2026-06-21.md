# Investigation: the ~213 ms `submitted→persisted` P99.9 cluster stall

**Date:** 2026-06-21
**Hardware:** AWS `c6id.4xlarge`, instance-store NVMe (`/opt/bench`, ext4 `data=ordered,commit=5,noatime`).
**Origin:** the e2e fill A/B (`docs/benchmarks/journal-prealloc-fill-e2e-ab-2026-06-21.md`) surfaced an
intermittent ~213 ms P99.9 in the leader's `submitted→persisted` Raft log stage, strategy-independent.
**Method:** systematic-debugging Phase 1–3 (evidence → hypothesis → one decisive test).

## Result so far

- **Device/fs hypothesis REFUTED.** An isolated 90 s `fdatasync` storm (`fio`, 4 KiB writes +
  `fdatasync=1`, metadata-free overwrite of a pre-laid file, spanning ~18 jbd2 5 s cycles) on the same
  instance-store NVMe gave: P50 **0.034 ms**, P99 **0.161 ms**, **P99.9 0.88 ms**, max 0.99 ms. The bare
  device/fs `fdatasync` tail is **sub-millisecond** — it does **not** explain the 213 ms cluster stall.
  (The earlier jbd2-periodic-commit / NVMe-GC hypothesis is wrong.)
- **Therefore the 213 ms is UC-path contention, not the device.**

## What Phase 1 established (evidence)

From the 6-pass e2e A/B `runtime-stats`:
- The 213 ms appears **only** in stage 3 `submitted→persisted` (the journal append→fsync); stages 1/2/4/5
  stay ≤ ~384 µs in every pass. → not a node-wide freeze (GC/scheduler/runtime) — those hit downstream
  stages too.
- **Strategy-independent** (full, paced, AND fallocate all hit it) → not the prealloc fill.
- payload=64, ~61 k records ⇒ **one 64 MiB segment** all run ⇒ segment rotation never happens and
  `purge_before` (segment-granular) is a **no-op**; `PurgeLog: 92` / `BuildSnapshot: 92` do no segment
  I/O. The snapshot/purge cadence is deterministic but incidental.
- `append()` (`log_storage.rs:403`) chains openraft's flush callback onto the journal's per-commit
  `Notifier`, which resolves after the bg writer's `sync_data`. So the stalled quantity is precisely the
  journal group-commit fsync completion, on the journal's own bg writer thread.

## Leading hypothesis (NOT yet proven) — journal `WriterState` mutex contention

The journal serializes reads with appends under a single `Mutex<WriterState>`. The journal's own code
documents the risk (`ultima_journal/src/journal/mod.rs:320-327`): *"reads are serialized with appends …
a slow read can block a concurrent append."* The bg writer holds this lock during `write_batch`
(the buffered `write_all`); `read`/`read_range`/`iter_range` also take it.

On the leader, openraft reads the log for **replication** (`try_get_log_entries` → journal `read_range`,
`replicate_batch` showed 244 k samples) and around snapshot/compaction. A rare long read (e.g. a follower
catch-up range, or a read coinciding with snapshot/purge bookkeeping) holding the `WriterState` lock would
block the writer's next `write_batch`, delaying the batch's fsync completion — inflating
`submitted→persisted` without touching the device. This is the only mechanism found that is
(a) isolated to the journal fsync stage, (b) strategy-independent, and (c) intermittent. **It is a
hypothesis from code reading, not yet measured.**

Other candidates not yet excluded: a competing burst of StableValue fsyncs (`save_committed` is fsynced
per commit-advance, `log_storage.rs:379`; plus `snapshot_meta` 92×) momentarily saturating the device
queue; or the journal writer thread being descheduled. The `fio` result makes a pure device-queue
explanation unlikely (bare load was clean), pointing back at the lock-hold or a UC-specific I/O pileup.

## Decision rule applied

> sub-ms isolated `fdatasync` ⇒ not the device ⇒ UC-path contention ⇒ escalate.

Confirmed UC-path. Pinning the exact mechanism (lock-hold vs fsync pileup) is the **escalation**, which
needs a full cluster run with targeted instrumentation — not done here:
- Off-CPU / wall-clock trace of the **journal bg writer thread** during a stall (is it blocked on
  `WriterState.lock()`?).
- `WriterState` lock-wait instrumentation (max hold time, holder identity: writer vs reader vs truncate).
- Correlate stall timestamps with replication-read ranges and snapshot/purge events.

## Significance / priority

This is a **rare tail (P99.9) at the journal-fsync stage that is masked end-to-end** — the e2e commit
path is `api_batch_linger` (~6.5 ms) + replication dominated, so `submit→response` p99 ≈ 7 ms and never
sees the 213 ms. Worth fixing only if cluster tail latency becomes a target. If pursued, the lock-hold
hypothesis suggests the long-deferred journal change (lock-free reads via
`Arc<RwLock<Vec<Arc<SegmentRef>>>>`, scoped out in task15, see `mod.rs:325`) as the likely fix —
decoupling log reads from the append/fsync path.

## Artifacts

`fio` JSON was on node0 (`/opt/bench/fio.json`, ephemeral — fleet destroyed). Numbers transcribed above.
