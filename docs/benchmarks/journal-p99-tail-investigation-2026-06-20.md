# Journal depth-1 p99 tail — root-cause investigation results

**Date:** 2026-06-20 (fleet run 2026-06-21)
**Hardware:** AWS `c6id.4xlarge`, us-east-1, local NVMe `/opt/bench` (`/dev/nvme1n1`, ext4), 16 vCPU.
**Spec / plan:** `docs/superpowers/specs/2026-06-20-journal-p99-tail-investigation-design.md`, `docs/superpowers/plans/2026-06-20-journal-p99-tail-investigation.md`.

## TL;DR — verdict

The ~3–5 ms depth-1 `append_consistent_prealloc_p99` tail is the **`fdatasync` syscall on the
journal writer thread**, stalled by **device contention from the `SegmentPipeline`'s background
segment preallocation** (a 64 MiB zero-fill + two `sync_all` barriers, fired at journal open). It is
**not** the per-append `Notifier` allocation/park, **not** scheduler latency, **not** a device floor,
and **not** a sampling artifact.

- **`SeqWatermark` transplant (the handoff's proposed fix): NO-GO.** Both `Notifier::wait()` and
  `SeqWatermark::wait()` simply block until the writer finishes the *same* fdatasync. Swapping the
  wait primitive cannot shorten a contended syscall. The transplant's only structural wins (one fewer
  per-append alloc, no inline callback fan-out) are real but touch none of the tail.
- **The real fix is elsewhere** (background-prealloc/foreground-commit device contention) and is left
  as a scoped follow-up below — not implemented here.

## Method & decision rules (as pre-registered)

Tiered, stop at the first decisive tier. Verdict table from the spec:
`device` / `scheduler-cstate` / `sampling-artifact` → no-go; `alloc-fan-out` → go.

## Tier 0 — isolation metrics (journal microbench, 400 samples)

| metric | value |
|---|---|
| `write_only_p99` | 4.1 µs |
| `fsync_prealloc_p50` | 34.6 µs |
| `fsync_prealloc_p99` | **45.5 µs** |
| `append_consistent_prealloc_p50` | 56.7 µs |
| `append_consistent_prealloc_p99` | **2.80 ms** |

The *isolated* preallocated fsync barrier p99 is 45.5 µs — ~61× below the full-append p99. By the Tier-0
rule this says "not a simple device floor." **But this was based on an unrepresentative measurement:**
the isolated `fsync_prealloc` arm fires `sync_data` back-to-back on the main thread with no concurrent
I/O, which understates the real per-commit fsync tail (see Tier 1). Tier 0 was *not* terminal; we
proceeded.

(The append tail was 2.80 ms here vs 5.2 ms in the originating handoff — same order, different
magnitude, consistent with a 4th-worst-of-400 rare-event tail.)

## Tier 1 — localization (`perf sched` + `perf trace`)

**Scheduler ruled out.** `perf sched latency`: the bench process (21 threads, 40,848 context switches)
had a **max runqueue delay of 0.040 ms (40 µs)**. Once runnable, a thread always gets the CPU within
40 µs — so neither runqueue latency nor (given the 40 µs max wakeup) C-state-exit can account for a
~3 ms stall.

**Tail localized to `fdatasync`.** `perf trace --duration 1.0` (every syscall > 1 ms): in the
preallocated append arm, *every* long syscall is `fdatasync`. The writer thread for that arm shows a
contiguous burst:

```
446.984 ( 4.058 ms): fdatasync(fd 5)
451.086 ( 2.854 ms): fdatasync(fd 5)
453.983 ( 2.860 ms): fdatasync(fd 5)
456.890 ( 2.854 ms): fdatasync(fd 5)
459.801 ( 2.843 ms): fdatasync(fd 5)
462.689 ( 2.853 ms): fdatasync(fd 5)
465.588 ( 2.857 ms): fdatasync(fd 5)
468.493 ( 1.649 ms): fdatasync(fd 5)
```

matching the raw-dump tail exactly (peak 4.11 ms, cluster ~2.90 ms; 8 of 400 samples > 1 ms). The 172
`futex` waits > 1 ms are the **main thread parked in `Notifier::wait` for precisely the fdatasync
duration** — a *consequence* of the slow fsync, not a cause (the `futex` max in the per-thread summary
is 0.03–0.06 ms when not shadowing a slow fsync).

**Tier-1 verdict: the tail is the `fdatasync` syscall, not the notifier machinery.** This already kills
the transplant.

## Tier 2 — skipped (hypothesis pre-falsified)

The pin + C-state knob was designed to confirm/deny a scheduler/C-state cause. Tier 1 gave *direct*
evidence that the time is inside the `fdatasync` syscall (40 µs max runqueue; syscall-duration trace),
so the knob test would be redundant. Skipped deliberately.

## Task 8 — store-WAL matched comparison (decision C)

Matched depth-1 single-commit microbench, **same 400 samples, same disk, same `CoalescedPrealloc`**:

| | journal `append_consistent_prealloc` | store WAL `wal_depth1_prealloc` |
|---|---|---|
| p50 | 56.7 µs | 55.4 µs (run2 60.9 µs) |
| **p99** | **2.90 ms** | **72.1 µs (run2 81.9 µs)** |

Identical floor; the store WAL has **no tail**. `perf trace` on the WAL: **zero** per-commit
fdatasyncs > 1 ms — only a single one-time 6.46 ms `fsync` at setup (its up-front chunk preallocation).

This **rules out `sampling-artifact`** (the WAL's 400-sample p99 is tight) and **rules out a device
floor** (the device demonstrably does 72 µs p99). The tail is **journal-specific**.

YCSB-A cross-check (store, prealloc ON), corroborative secondary signal: criterion mean **3.34 ms/iter**
(CI [2.90, 3.89], 18% outliers, `ycsb_a_update_heavy/burst`). Caveat: criterion reports mean-with-CI,
not p99, and the per-iteration op count differs from the originating handoff's setup, so this is a
coarse aggregate check, not a clean per-commit p99 — the matched microbench above is the authoritative
comparison. The tight CI (upper ≈ 1.16× lower, no order-of-magnitude blowup) corroborates that the
store WAL path carries no pathological per-commit tail.

## Root cause

`SegmentPipeline` (active under `preallocate_segments: true`) runs a background thread that keeps one
preallocated temp segment ready. `segment.rs:create_prealloc_temp` writes the **entire 64 MiB segment**
in 1 MiB zero chunks, then issues `file.sync_all()` **and** a parent-directory `sync_all()`. This fires
at journal open, concurrent with the early commits of the measured loop.

That background 64 MiB write + two full barriers contends with the foreground per-commit `fdatasync` on
the same NVMe queue → the observed **contiguous burst** of ~2.85 ms commits (perf window 447–468 ms),
then commits return to ~57 µs once the fill completes. The tight ~2.85 ms clustering is each foreground
fsync queuing behind a roughly fixed slab of background dirty data + barrier.

Corroborating: the journal's **non**-preallocated append arms (no pipeline thread running) had
`fdatasync` max ≤ 0.35 ms in the same perf session — i.e. the tail appears *with* background
preallocation, not without it. The store WAL avoids it entirely by filling its prealloc chunk inline,
once, before the commit loop.

**Strength of the attribution (stated plainly).** The transplant NO-GO is *proven* at Tier 1: the time
is provably inside `fdatasync` (40 µs max runqueue; per-syscall trace), so swapping the wait primitive
cannot help — this stands regardless of *why* the fdatasync is slow. The further attribution of the
slowness to the `SegmentPipeline` specifically is **inferential**, resting on three consistent facts
rather than a single direct observation: (a) `create_prealloc_temp` is the only heavy background I/O on
the code path and it fires at open, when the burst occurs; (b) the slow fdatasyncs form one *contiguous*
~21 ms burst that then stops (a fixed-bandwidth competition signature, not scattered jitter), with the
last entry shorter (1.65 ms) as if the competitor finished mid-flush; (c) the non-prealloc arm (no
pipeline) is clean and the store WAL (no concurrent filler) is clean. What is *not* in hand is a trace
slice showing the pipeline thread's own `write`/`sync_all` overlapping the burst — and the fleet has
been torn down, so this last confirmation is left to the follow-up task (re-run with the pipeline thread
explicitly traced, e.g. `perf trace` filtered to both tids, or an `ext4:`/`block:` tracepoint capture).

Bridging Tier 0 → Tier 1: the isolated `fsync_prealloc_p99` of 45.5 µs and the 2.85 ms append-path tail
are not in tension — the device queue serialises the entire background 64 MiB fill (+ barriers) ahead of
a single foreground `fdatasync`, so the foreground call inherits the whole competing flush's latency
rather than a small fraction of it.

## Decision

| verdict candidate | evidence | result |
|---|---|---|
| `device` (floor) | store WAL does 72 µs p99 on same device | rejected |
| `scheduler-cstate` | 40 µs max runqueue; time in syscall | rejected |
| `sampling-artifact` | WAL 400-sample p99 tight | rejected |
| `alloc-fan-out` (→ transplant go) | time is in fdatasync, not alloc/park | **rejected → transplant NO-GO** |
| **fsync vs background-prealloc contention** (new) | perf trace burst + pipeline code | **accepted** |

**`SeqWatermark` transplant: do not build it for this tail.** (It may still be justified independently
as a minor cleanup — one fewer per-append `Arc`, no inline callbacks on the writer hot path — but that
is a code-quality call, not a p99 fix, and the handoff's empirical premise for it does not hold.)

## Follow-up lead (not implemented — scoped for a future task)

Eliminate the background-prealloc/foreground-commit device contention in `ultima_journal`. Candidate
directions, roughly in order of attractiveness:

1. **Lighten the background fill barrier:** `sync_data` instead of `sync_all`, and drop the per-temp
   parent-dir `sync_all` (do the directory barrier once, or at activation), reducing the competing
   barrier load.
2. **Pace the fill:** write the 64 MiB in smaller slices with yields / bounded in-flight, so it never
   monopolizes the device queue against a latency-sensitive commit.
3. **Lower-priority background I/O** (e.g. `ionice`/`RWF` hints) for the preallocator thread.
4. **Fill the first temp before accepting commits** (move the unavoidable cost into open, like the
   store WAL's one-time setup fsync), so steady-state commits never race a fill.
5. **Smaller `segment_size`** shrinks each fill window (trades rotation frequency).

Any of these should be A/B'd with `append_consistent_prealloc_p99` (this same microbench) before/after,
and cross-checked end-to-end — note the cluster commit path is dominated by IPC + replication, so this
p99 may or may not surface there.

## Artifacts (on node0, ephemeral with the fleet)

`/opt/bench/src/{jdump.txt,jdump2.txt}` (journal raw samples), `/opt/bench/src/perf.sched`,
`/opt/bench/src/{dur.txt,wal-dur.txt}` (per-syscall > 1 ms traces), `/opt/bench/src/wal-d1.json`.
