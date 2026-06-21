# Journal prealloc fill strategy — A/B results

**Date:** 2026-06-21
**Hardware:** AWS `c6id.4xlarge`, us-east-1, local NVMe `/opt/bench` (`/dev/nvme1n1`, ext4), 16 vCPU.
**Spec / plan:** `docs/superpowers/specs/2026-06-21-journal-prealloc-fill-contention-design.md`,
`docs/superpowers/plans/2026-06-21-journal-prealloc-fill-contention.md`.
**Branch under test:** `fix/journal-prealloc-fill-contention` (`PreallocFill` strategy selector).

## TL;DR — verdict

`FallocateZeroRange` (A) is the winner and is **accepted**: it collapses the depth-1
`append_consistent_prealloc_p99` from **2.90 ms → 0.175 ms (~16×)** with **zero** ms-scale samples,
keeps p50 at the floor, and *raises* preallocated group-commit throughput **+43%**. `ZeroWritePaced`
(B) is a solid reliable fallback (~3× p99) and is what A falls back to on non-Linux / unsupported
filesystems. **Recommendation: flip the default to `FallocateZeroRange` (follow-up commit) and keep
`rustix`.**

## Results (journal microbench, 400 serial depth-1 samples, `Durability::Consistent`)

| strategy | prealloc p50 | prealloc **p99** | group_commit_throughput_prealloc | dump samples >1 ms |
|---|---|---|---|---|
| `full` (ZeroWriteFull, baseline) | 56.1 µs | **2.90 ms** | 293.6 K/s | 8 / 400 |
| `paced` (ZeroWritePaced, B) | 59.3 µs | **0.98 ms** | 281.6 K/s | 2 / 400 |
| `fallocate` (FallocateZeroRange, A) | 54.0 µs | **0.175 ms** | 420.3 K/s | 0 / 400 |

Top dump samples (ns): full `[3.90 ms, 3.00 ms, 2.90 ms, …]`; paced `[1.20 ms, 1.00 ms, 0.998 ms]`;
fallocate `[368 µs, 178 µs, 176 µs]`.

## Pre-registered accept/reject rule for A

> A is metadata-free ⟺ its `append_consistent_prealloc_p99` collapses like paced's **and**
> `group_commit_throughput_prealloc` ≥ `ZeroWriteFull` baseline. Else reject A → ship B.

- A's p99 = **0.175 ms** — collapses (better than paced's 0.98 ms). ✅
- A's throughput = **420.3 K/s** ≥ baseline **293.6 K/s** (+43%). ✅
- **→ A ACCEPTED.**

**Why this proves `ZERO_RANGE` yields metadata-free overwrites on this ext4** (the design's open risk):
if `ZERO_RANGE` left *zeroed-but-unwritten* extents, every per-commit overwrite would convert an extent
and re-journal jbd2 metadata — driving p50 up toward the non-prealloc ~180 µs and re-introducing the
fsync tail. Instead p50 *dropped* to 54 µs (below baseline) and the tail vanished, and throughput rose.
That is only consistent with **initialized** extents (metadata-free overwrites) plus a near-instant fill
(no 64 MiB background write to contend with foreground commits).

## Mechanism confirmation (`perf trace --duration 1.0`, whole bench)

- Total syscalls > 1 ms across the entire microbench: **paced = 82, fallocate = 4** — fallocate removes
  the background-fill I/O bursts everywhere, not just the depth-1 arm. (The residual 10–24 ms entries on
  fds 3/4/5 are the high-volume eventual-fill / throughput arms, which always use `ZeroWriteFull`-shaped
  fills and are not the depth-1 prealloc arm under test.)
- The `fallocate` syscall **never appears** in the > 1 ms trace — it is sub-millisecond, confirming the
  fill itself is cheap (vs the baseline's 64 MiB write + `sync_all`).
- The authoritative per-arm signal is the per-strategy raw dump of the depth-1 prealloc arm:
  full 8 / paced 2 / fallocate 0 samples > 1 ms.

This confirms the investigation's root cause (`docs/benchmarks/journal-p99-tail-investigation-2026-06-20.md`):
the tail was the background `SegmentPipeline` 64 MiB zero-fill + `sync_all` contending with the
foreground commit `fdatasync`. Replacing the fill with a near-instant `fallocate(ZERO_RANGE)` removes the
contention at the source.

## Recommendation (follow-up commit, not in this branch)

1. **Flip the default:** `JournalConfig::new` → `prealloc_fill: PreallocFill::FallocateZeroRange`. The
   built-in fallback to `ZeroWritePaced` (non-Linux / `OPNOTSUPP` / `NOSYS`) means this is safe
   everywhere; `paced` remains a ~3× improvement where `fallocate` is unavailable.
2. **Keep the `rustix` dependency** — now justified (A won).
3. Keep `UC_JOURNAL_PREALLOC_FILL` as the override/rollback (`paced` or `full`).
4. Optional: end-to-end cluster A/B (`submitted→persisted` p99) — note the cluster commit path is
   dominated by IPC + replication, so this journal p99 win may be partly masked there (consistent with
   prior preallocation/fdatasync findings).

## Artifacts (on node0, ephemeral with the fleet)

`/opt/bench/src/ab-{full,paced,fallocate}.json` (metrics), `/opt/bench/src/dump-{full,paced,fallocate}.txt`
(raw per-sample latencies), `/opt/bench/src/trace-{paced,fallocate}.txt` (per-syscall > 1 ms traces).
