# The ~15k throughput ceiling — root-caused and fixed (2026-07-01)

**Result:** the long-standing ~15k msg/s throughput ceiling was a single journal bug —
`Journal::purge_before` full-scanning a 64 MiB segment on every log purge. Fixed in one place
(`ultima_journal`, commit `aa031e8`); the leader's decode volume dropped **13×** and the
inflight-128 knee rose **10k → 15k (+50%)** — the first fix in the whole investigation to move
the ceiling.

## Root cause

`Journal::purge_before` (`ultima_journal/src/journal/mod.rs`) decided which sealed segments to
drop by calling `seg.scan()` on each — **decoding and CRC-verifying every record of a 64 MiB
segment** — just to read its *last record's seq*. openraft purges the log after every snapshot,
so under load this ran constantly:

- Measured on the leader at 15k msg/s (3-node consistent): **~90M record decodes / ~8 GB of
  CRC per 40 s.** That is exactly the invariant **21% `crc32` + 20% `read()`** that saturated
  one core (while 7 sat idle) in every profile.
- It was invisible to five prior fix attempts (entry cache, NoopOutput, output_dispatcher
  fsync-coalescing, `limited_get_log_entries` cap, denser sparse index) — **all null on the
  knee** — because none of them touched the purge path. The 3-process shmem bench was a red
  herring; the culprit is pure openraft-storage log-purge.

## The fix

A non-active segment's last seq is `segments[i+1].base_seq() - 1` (segments are contiguous —
segment `i+1` begins exactly one seq after segment `i` ends), so it is O(1), no scan. The active
(last) segment is never dropped, so it never needs its last seq here.

```rust
let n = st.segments.len();
let mut keep_idx = 0;
for i in 0..n {
    if i + 1 >= n { break; }                                  // active — never dropped
    let seg_last = st.segments[i + 1].base_seq().saturating_sub(1);
    if seg_last <= seq { keep_idx = i + 1; } else { break; }
}
```

## Measured (fleet A/B, c6id.2xlarge ×3, us-east-1, linger=2, 3-node consistent, 15k)

| metric | before | after |
|---|---|---|
| journal decodes / 40 s | 90M | **7M (13×)** |
| CRC volume | 8.3 GB | **614 MB** |
| `crc32` CPU (busiest thread) | 21% | **14%** |
| `read()` CPU | 20% | **~0 (fell out of top)** |
| `scan_calls` / 40 s | 252 | **32** |
| **inflight-128 knee** | **10k** | **15k (+50%)** |

Correctness: journal purge tests (`purge_before_drops_full_segments`,
`purge_below_threshold_protects_active_segment`, `end_to_end_..._truncate_purge_reopen`) +
`lin_register` 3/3 (incl. `linearizable_under_failover`) + `lin_partition` 4/4 — the scan-free
purge is exact under load, failover, and network faults.

**inflight ≥ 256 unmeasured:** the `commit-path-load` client cannot sustain inflight ≥ 256 through
the overload rung (its 10 s per-request timeout trips when p99 spikes at saturation), so it times
out before reporting. The true post-fix ceiling is > 15k but this client can't cleanly probe the
overload region — a client-robustness follow-up, not a cluster limit.

## How it was found (method that worked)

After five null fixes, stopped guessing and instrumented:
1. **Per-read-path decode counters at INFO** (`read_profile.rs`, temporary) — `debug!` is filtered
   on the fleet, so INFO was required to be visible. Showed the decode volume (90M) vastly
   exceeded every instrumented read path (~13M), so an *uninstrumented* decoder dominated.
2. **`std::backtrace::Backtrace::force_capture()` `eprintln!` in the journal read/scan paths**,
   rate-limited — named the caller in one fleet: `purge_before → scan()`.

The diagnostic instrumentation was reverted after the fix; only the one-block `purge_before`
change remains.

## Remaining (separate, not the ceiling)

`output_chan full` warns (~570k/40s) persist — the `output_dispatcher` advances its durable
`output_progress` marker with a per-entry fsync (now batch-coalesced in `ada14e1`, but the
per-entry shmem round-trip remains). This is **off the client-response critical path** (apply
`try_send`s and drops on full), so it does not gate throughput; it is an efficiency follow-up.

## Post-fix ceiling curve (multi-threaded load client, 3-node consistent, linger=2)

With the load client fixed (`commit-path-load` now multi-threaded + spawn-per-request, so it can
drive inflight ≥ 512 — see below), the full concurrency sweep (max achieved per inflight, offered
ladder 20/30/45/60k, two runs):

| inflight | max achieved | note |
|---|---|---|
| 128 | 18.7–20.3k | |
| **256** | **23.7–26.6k** | **peak (the ceiling)** |
| 512 | 19.1k | declining; 60k rung collapsed to 1.5k |
| 1024 | 11.9k | congestion collapse |
| 2048 | 14.2k | congestion collapse |

**The cluster ceiling is ~24–27k msg/s, at an optimal concurrency of ~256 in-flight.** Beyond
that the system does **not** scale — it suffers **congestion collapse** (goodput *falls* as
concurrency/offered-load rise: e.g. 60k-offered at inflight=512 collapsed to 1.5k). So the answer
to "how high does it go" is a peak, not a plateau: push past ~256 concurrency and throughput
degrades. p99 at the top rungs is multi-second (coordinated-omission latency under overload).

Net vs the start of the investigation: the *measured* ceiling went from a **hidden ~15k** (the
old client's 10s timeout aborted the overload rungs, and it couldn't drive past inflight=256) to
a **real ~24–27k peak** after the `purge_before` fix — with the operating sweet spot at ~256
in-flight.

### The load-client stall at inflight ≥ 512 (fixed)

`commit-path-load` ran on a single-threaded tokio runtime (`current_thread`) and polled all N
in-flight futures in one `FuturesUnordered` on one task; the client's response-reader is a
`tokio::spawn` task, so it shared that one core, and each in-flight future also woke a 100ms
stall-check timer + grabbed a shared mutex. At inflight ≥ 512 the core saturated, the reader
starved, and the rung never finished (900s cell timeout) — a *load-generator* limit, not the
cluster. Fixed by switching to a multi-threaded runtime and spawning each request as its own task
under a `Semaphore(inflight)` cap (commits `36c0a5a`, `1bb9108`). The cluster itself handled
256-concurrency fine; it's the *excess* concurrency that triggers congestion collapse.

## SyncCore vs RaftCore — 5-repeat A/B at the 30k rung (2026-07-01)

A first single-run A/B (both arms on the purge fix) suggested SyncCore resisted a 30k congestion
collapse that flattened RaftCore, and had lower p99. **Five repeats per arm (30k offered,
inflight=256, independent fresh-cluster iterate runs) show both signals were noise:**

| arm | achieved (5 runs, k msg/s) | mean | collapsed <10k | p99 mean |
|---|---|---|---|---|
| SyncCore | 24.7 25.2 26.2 22.6 22.9 | 24.3k | 0/5 | 2294 ms |
| RaftCore | 25.9 25.0 25.8 26.2 24.5 | **25.5k** | 0/5 | **1829 ms** |

- **No collapse in either arm** — the earlier RaftCore 30k→2.6k was a one-off transient, not a
  systematic weakness. SyncCore has **no collapse-resistance advantage**.
- **p99 reversed** — RaftCore is marginally *lower* here; the single-run SyncCore p99 edge was noise.
- **At the operating ceiling (inflight=256/30k), SyncCore and RaftCore are statistically equal,
  RaftCore marginally ahead on both throughput and p99.**

SyncCore's one measured real edge is **low-concurrency latency** (task19: ~+35-45% at inflight=1),
a different regime than this ceiling. SyncCore was made the default consensus model (commit
addb8ee, merged to main) on the cleaner-synchronous-foundation + maintainer-direction basis —
**not** on a proven throughput or robustness advantage at load.

> **⚠️ CORRECTION 2026-07-01: the 5-repeat A/B above is INVALIDATED.** It ran *after* SyncCore was made the default Cargo feature (addb8ee), and due to Cargo feature unification (`uc_autobench` depended on `uc_node` without `default-features = false`), `-e uc_sync_core=false --no-default-features` did NOT disable uc_node's sync-core default — so BOTH arms were actually **SyncCore**. That is why they looked equal and neither collapsed. It did NOT test RaftCore and did NOT debunk the collapse-resistance. The only valid RaftCore-vs-SyncCore comparison remains the *first* single-run A/B (RaftCore 24.2k, collapsed at 30k; SyncCore 24.9k, held) — a single point, still unsettled. Build fixed (uc_autobench uc_node dep `default-features = false`; verified RaftCore build = 0 sync_core symbols, SyncCore = 266); a corrected A/B + a clean RaftCore ceiling profile are pending.

---

## Correction (2026-07-02): the crc32 was NOT the throughput ceiling — the ceiling is the latency floor

The framing above ("the ~15k ceiling was journal read+CRC", "crc32 is the ceiling") is **only
half right, and misleading for throughput.** A second round of diagnosis + a proper sweep
corrected it.

**Residual crc32 found + fixed.** After the first purge fix, a fresh RaftCore profile still showed
crc32 ~18% at the ceiling. Decode-partition counters (a per-primitive `scan`/`read_record`/
`read_window` decode count, plus per-caller entry counts) pinned it definitively:
`scan()` = **93-99% of all journal decodes** (~42M/40s on the leader), and the only steady-load
`scan()` caller is **`purge_before`'s `first_seq` recompute** (`ultima_journal` mod.rs ~549) — the
**residual half of the purge full-scan bug**: the first fix (`aa031e8`) removed the *last-seq*
segment-drop scan but missed the *first_seq* recompute scan right below it, which still scanned a
whole 64 MiB segment on every purge (~5/s, post-snapshot). Fixed in `28dcd6c`
(`first_seq = first-segment.base_seq()`, filtered by `last_seq`; O(1), no scan; journal purge tests
+ lin_register 3/3 green). `log_id_at` (58k decodes) and `read_record` (0) were ruled out by
direct measurement — two wrong hypotheses (log_id_at, and earlier a build variant) were caught by
measuring instead of inferring.

**But it's throughput-neutral — which is the real lesson.** The fix eliminated 93% of the journal
decodes: crc32 vanished from the profile (top symbol became `finish_task_switch`), and the box went
from ~50% to **~85-96% idle**. Yet a proper post-fix sweep (RaftCore, inflight 128/256/512, 2 reps)
gave **20-30k, centered ~25k — statistically identical to pre-fix.**

| inflight | rep 1 | rep 2 |
|---|---|---|
| 128 | 22,813 | 20,494 |
| 256 | 19,991 | 29,988 |
| 512 | 24,989 | 19,993 |

So the crc32/scan was real **wasted CPU**, but on an otherwise-idle box (5-7 of 8 cores idle) it was
**never the throughput-binding constraint.** The **~25k ceiling is the latency floor**: fsync
durability + 3-process shmem IPC round-trips + openraft's async replication choreography (the
~1-2 ms commit floor). Throughput = concurrency ÷ latency; that floor caps it regardless of spare
CPU. This is the same structural floor the floor-decomposition found (~73% software/IPC/async,
~27% fsync/wire), and it explains the whole investigation:

- **SyncCore throughput-null** — it trims per-op latency but the floor is fsync+IPC, not consensus CPU.
- **cache / output-handler / limited_get all null** — none touched the latency floor.
- **The *first* purge fix (`aa031e8`) *did* move 15k→25k** — that scan (90M decodes) was heavy
  enough to actually saturate the leader thread; once removed, the ceiling became latency-bound.
  This second scan (42M) was already below the floor, so removing it is throughput-neutral.

**Disposition of `28dcd6c`:** keep it — it's correct hygiene (a full-segment scan per purge
eliminated: less CPU, less I/O, less read-amplification, tail-latency headroom) — but it is **not**
a throughput win.

**To actually move past ~25k you must attack the latency floor** — co-locate to remove IPC hops,
cut async replication choreography, or batch fsync harder — **not** CPU/crc32/journal work.

**Method lessons:** (1) the top CPU symbol is not the throughput bottleneck when cores are idle;
(2) measure the ceiling with a sweep + repeats, not a single profile cell (one anomalous cell read
11k; six reps showed 20-30k); (3) attribute by direct measurement (decode-delta / per-primitive
counters), not by inference from a partial profile.
