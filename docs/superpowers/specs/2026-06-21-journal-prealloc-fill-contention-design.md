# Design: fix background-prealloc fsync contention (journal depth-1 p99 tail)

**Date:** 2026-06-21
**Status:** Design (approved for planning)
**Origin:** Follow-up fix for the verdict in
`docs/benchmarks/journal-p99-tail-investigation-2026-06-20.md`.

## Problem

The journal's depth-1 `append_consistent_prealloc_p99` shows a ~2.9–5 ms tail on c6id NVMe. The
investigation proved (perf sched: 40 µs max runqueue; perf trace: every tail sample a long `fdatasync`;
store-WAL matched microbench p99 72 µs vs journal 2.9 ms) that the tail is the writer's per-commit
`fdatasync` **stalled by device contention from the `SegmentPipeline`'s background segment
preallocation**, not the `Notifier` machinery (so the proposed `SeqWatermark` transplant was rejected).

Refined mechanism (from code reading): `SegmentPipeline::spawn` pre-warms temp #0 during `Journal::open`
(before commits). The *first* append takes temp #0 and immediately signals the preallocator to fill
temp #1. `SegmentFile::create_prealloc_temp` (`segment.rs:232`) then writes the full
`segment_size_bytes` (default 64 MiB) of real zeros in 1 MiB chunks, `sync_all`s, and `sync_all`s the
parent dir — all on the background thread, **concurrent with the early commits of the fresh segment**.
That 64 MiB write + two full barriers queues ahead of the latency-sensitive foreground `fdatasync` on
the shared NVMe → the observed contiguous ~2.85 ms commit burst. The store WAL avoids this by filling
its prealloc chunk inline, once, up front (one one-time setup fsync, then no concurrent filler).

The real zero-fill is itself load-bearing: `segment.rs:421-427`/`:466-477` deliberately reject
`set_len`/plain `fallocate(mode=0)` because on ext4 those leave **unwritten** extents that re-journal a
metadata commit on first overwrite — exactly what preallocation exists to avoid.

## Goal & non-goals

**Goal.** Collapse `append_consistent_prealloc_p99` from ~2.9 ms toward the ~57 µs p50 floor (store WAL
is 72 µs) by stopping the background fill from monopolizing the device during fresh-segment commits,
**while preserving** the preallocation throughput win (`group_commit_throughput_prealloc`), the
`Durability::Consistent` power-loss guarantee, and tail-tolerant recovery.

**Non-goals.** No change to the apply/commit pipeline, the `Notifier`, or `activate_prealloc_temp`. No
`SeqWatermark` transplant (rejected by the investigation). No default-strategy flip in this change (the
default flips in a follow-up commit once the A/B picks the winner).

## Approach: a selectable preallocation fill strategy

Add a `PreallocFill` strategy enum to `JournalConfig`, dispatched by the background fill path. Three
strategies; the A/B run picks the winner.

### `PreallocFill::ZeroWriteFull` — baseline (current behavior, unchanged)

64 MiB real-zero write in 1 MiB chunks + `file.sync_all()` + parent-dir `sync_all()`. Retained as the
A/B baseline and as the **default** until the A/B decides. Behavior identical to today.

### `PreallocFill::ZeroWritePaced` — reliable fix (B), pure `std`, no new deps

Same real-written extents as `ZeroWriteFull`, but the device flush is broken up and spaced so a
foreground `fdatasync` can interleave:

- Write the segment in chunks; issue `file.sync_data()` every `prealloc_fill_chunk_bytes` (default
  **4 MiB**) rather than one terminal `sync_all`; `std::thread::yield_now()` between chunks.
- **Drop the per-temp parent-dir `sync_all`.** A prealloc temp is recreated on crash (the preallocator
  rebuilds its slot at open), so its directory entry need not be crash-durable. (The *activation* path
  `activate_prealloc_temp` keeps its rename + dir `sync_all` — that durability is unchanged.)
- A final `sync_data` covers any partial last chunk.

Net: identical written-extent guarantee, many small spaced barriers instead of one large one. The device
still moves 64 MiB total, but never as a single queue-monopolizing flush. No `fallocate`, no new
dependency, no filesystem-behavior gamble.

### `PreallocFill::FallocateZeroRange` — bigger-win fix (A), validated empirically

`rustix::fs::fallocate(fd, FallocateFlags::ZERO_RANGE, 0, segment_size_bytes)` followed by one
`file.sync_data()` (to commit the extent allocation), and drop the parent-dir `sync_all` as in B. If the
kernel returns **initialized** extents, the background "fill" becomes one cheap syscall + a small
metadata barrier — contention essentially eliminated, and `open()` is faster too.

- **Linux-gated** (`#[cfg(target_os = "linux")]`); on non-Linux, or on `ENOTSUP`/`EOPNOTSUPP` from
  `fallocate`, **fall back to `ZeroWritePaced`** so the journal always works.
- Adds a `rustix` dependency (feature `fs`) to `ultima_journal` (currently has no libc/nix/rustix dep).
  `rustix` is chosen over raw `libc` for the safe wrapper and small footprint.

**Risk, made explicit:** on ext4, `ZERO_RANGE` may produce *zeroed-but-unwritten* extents, in which
case the first overwrite re-journals metadata and the per-commit fsync tail returns — defeating
preallocation (the code's stated worry about `fallocate`). This is why A is **validated, not trusted**.

## Selection, configuration & the A/B

- `JournalConfig.prealloc_fill: PreallocFill` (default `ZeroWriteFull`) and
  `JournalConfig.prealloc_fill_chunk_bytes: u64` (default 4 MiB, used by `ZeroWritePaced` and the paced
  fallback).
- Env override resolved where the cluster/bench builds the config:
  `UC_JOURNAL_PREALLOC_FILL = full | paced | fallocate` (unset → config default). The journal microbench
  opens journals directly, so the bench arm reads the same env to select the strategy under test.
- One fleet A/B session compares all three on `append_consistent_prealloc_{p50,p99}` and
  `group_commit_throughput_prealloc`, with a `perf trace --duration` fdatasync check per strategy
  (reusing the investigation's instrumentation).

**Pre-registered accept/reject rule for A:** `FallocateZeroRange` is metadata-free ⟺ its
`append_consistent_prealloc_p99` collapses like `ZeroWritePaced`'s **and**
`group_commit_throughput_prealloc` ≥ `ZeroWriteFull` baseline. If A's fdatasync tail returns or its
prealloc throughput drops below baseline → `ZERO_RANGE` left unwritten extents → **reject A, ship B.**
The default flips to the winner (`paced` or `fallocate`) in a follow-up commit after the A/B; this
change leaves the default at `ZeroWriteFull` (no behavior change until validated).

## Durability & recovery (unchanged)

Both B and A still produce a full-size segment file that reads back as zeros, so:
- Tail-tolerant recovery (`scan` treating a zero region as end-of-log) is unaffected.
- The torn-record CRC logic is unaffected.
- `Durability::Consistent`'s per-commit `sync_data` guarantee on *committed records* is unchanged — the
  fill strategy only governs how the *empty* preallocated tail is laid down before any record lands in
  it.

The only durability reduction is intentional and safe: dropping the parent-dir `sync_all` for the
*temp* file (not the activated segment), justified because the temp is rebuilt on crash.

## Components & boundaries

- `JournalConfig` (`journal/mod.rs`): add `PreallocFill` enum, `prealloc_fill`,
  `prealloc_fill_chunk_bytes`; thread into `SegmentPipeline::spawn`.
- `segment.rs`: `create_prealloc_temp` takes the strategy + chunk size and dispatches to one of three
  fill implementations (`fill_zero_write_full`, `fill_zero_write_paced`, `fill_fallocate_zero_range`).
  Keep `preallocate_to`/bench `preallocate_zerofill_for_bench` consistent (paced variant available for
  the bench so the isolated `fsync_prealloc` arm can mirror the chosen strategy if desired).
- `segment_pipeline.rs`: `Shared` carries the strategy + chunk size; `preallocator_loop` passes them to
  `create_prealloc_temp`. No structural change to the one-slot pipeline.
- Env wiring: wherever the cluster builds `JournalConfig` from `UC_JOURNAL_*` (and the journal microbench
  in `uc_autobench`), resolve `UC_JOURNAL_PREALLOC_FILL`.
- `rustix` dep added to `ultima_journal/Cargo.toml` (feature `fs`).

## Testing

- Unit (per strategy): a freshly created prealloc temp is exactly `segment_size_bytes` long and reads
  back as all zeros; `ZeroWritePaced` with a non-divisible size still fills the whole file; the
  `FallocateZeroRange` test is `#[cfg(target_os = "linux")]` and tolerates the documented fallback.
- Strategy round-trip: open a journal under each strategy, append + `wait_durable` + reopen + replay;
  assert records recovered (exercises that overwriting the preallocated tail works for each fill).
- Existing journal durability/recovery/torture suites must pass under each strategy (run the suite once
  per `UC_JOURNAL_PREALLOC_FILL` value, or parameterize).
- Metadata-free behavior of A is **not** assertable in a unit test (it's a perf/fs property) — it is
  gated by the fleet A/B's accept/reject rule above, not by CI.

## Open risks

- `ZERO_RANGE` semantics on the target ext4 (the whole reason A is validated, not trusted).
- Paced fill total duration: many small `sync_data`s lengthen the fill; must still complete before the
  active segment rotates. For 64 MiB segments at realistic write rates this is comfortable, but the A/B
  should confirm `group_commit_throughput_prealloc` (which exercises rotation under burst) does not
  regress versus baseline for `ZeroWritePaced`.
- `rustix` is a new dependency for the lean `ultima_journal` crate; justified only by A. If A is
  rejected, consider dropping the dep when the default flips to `paced`.
