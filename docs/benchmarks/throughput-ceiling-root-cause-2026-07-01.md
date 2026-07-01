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
