# Design: journal depth-1 p99 tail — confirm root cause before any fix

**Date:** 2026-06-20
**Status:** Design (approved for planning)
**Origin:** Open lead §4 of `docs/handoff-2026-06-20-wal-prealloc-and-journal-wakeup.md`

## Problem

The journal microbench `append_consistent_prealloc_p99` (a depth-1 serial
`Journal::append().wait()` on a preallocated journal) showed a **~5.2 ms p99
tail** on the AWS c6id.4xlarge NVMe, while the ultima_db store WAL shows no
comparable tail under the same disk and the same depth-1 serial discipline.

The handoff proposed a fix: route the blocking `append().wait()` path through the
existing `SeqWatermark` (the journal's reusable single-condvar, seq-keyed
durability watermark) instead of the freshly-allocated per-append `Notifier`,
eliminating per-append allocation (D1) and inline completion fan-out (D2). The
handoff also flagged that **static analysis cannot prove the 5.2 ms
attribution** — it needs an off-CPU / `perf sched` profile.

This investigation determines where the tail actually comes from and decides
go/no-go on the transplant **before** any implementation effort.

## Why the transplant is in doubt (load-bearing context)

Three findings from code + autoresearch-history review reframe the lead:

1. **The bench is 400 serial samples**, so p99 ≈ the 4th-worst sample — a handful
   of rare events, not a steady distribution. Any profile must capture
   **per-sample latency and zoom into the few slow samples**; a flat flamegraph
   (which averages over all samples) is useless for a 1%-tail of 400.

2. **Prior autoresearch already profiled this exact path** and concluded it is
   **VM-scheduling / park-bound**, not allocation- or lock-bound. From
   `uc_autobench/tasks/journal-commit/results.tsv` discard rows:
   - *"spin-before-park in `Notifier::wait`… profiler said path is 79.7%
     futex/park-bound, ~28µs park vs ~free fsync"* — washed.
   - *"3rd consecutive contention/sync lever to wash → group_commit primary is
     NOT state-lock-bound on this VM; profiler's VM-scheduling-bound verdict
     confirmed."*
   This undercuts the handoff's D1 (per-append alloc) lead suspect and rules out
   D3 (shared state lock).

3. **Both `Notifier::wait()` and `SeqWatermark::wait()` park on a condvar**
   (`notifier.rs:84`, `writer.rs:175`). If the tail is park-to-wakeup scheduling
   latency, the transplant changes *which* condvar is parked on but not *that* it
   parks, nor the wakeup scheduling cost. It removes only the per-append `Arc`
   alloc (D1) and the empty inline-callback fan-out (D2) — **not the park**. So
   if the prior profiler's verdict holds, the transplant likely will not move
   p99 at all. **This is the hypothesis the investigation must falsify.**

The valuable outcome of this investigation may well be *not* shipping the
transplant — saving effort on a change that cannot touch the tail it targets.

## Goal & deliverable

**Goal.** Attribute the 5.2 ms tail to one of: device flush, scheduler/C-state
wakeup, per-append alloc/fan-out, or sampling artifact — and emit a go/no-go on
the `SeqWatermark` transplant.

**Deliverable.** A report under `docs/benchmarks/` recording the c6id Tier-0
numbers, the slow-sample classification, the confirmation-knob result, the
journal-vs-store-WAL matched comparison, and a one-line verdict mapping to a
transplant decision. No production code change ships in this investigation.

## Method: tiered, pre-registered decision rule

Stop at the first tier that is decisive. The decision rules are pre-registered
here to avoid post-hoc rationalization once data is in hand.

### Tier 0 — free (read already-emitted isolation metrics)

On fleet restart, run the journal bench once on the c6id local NVMe
(`/opt/bench`, `/dev/nvme1n1`, ext4) and read the **already-emitted**
`fsync_prealloc_p99` and `write_only_p99`. The bench
(`uc_autobench/src/journal_bench.rs`) already computes these; the handoff only
transcribed the c6id `fsync_prealloc_p50` (34.6 µs), never its p99.

- **Rule:** if `fsync_prealloc_p99 ≈ 5 ms` (same order as the append tail) →
  the tail is the **device/virt fdatasync flush**, not journal machinery →
  **STOP. Transplant irrelevant.** Record verdict = `device`.
- Sandbox reference (noisy, not c6id): `fsync_prealloc_p99` 0.56 ms vs full
  `append_consistent_p99` 1.82 ms — there the device is *not* the whole tail,
  but the c6id number is the one that decides.

### Tier 1 — localize within the machinery

If Tier 0 shows a machinery tail (device flush p99 << 5 ms), profile to localize:

- `perf sched record` → `perf sched timehist` / `perf sched latency` for
  wakeup-to-run (run-queue) delay per thread.
- `perf record -e sched:sched_switch,sched:sched_wakeup -g --call-graph dwarf`
  (or BCC `offcputime` if available) for off-CPU attribution.
- **Per-sample latency capture in the bench**: emit each sample's latency +
  a timestamp so the ~4 slow samples can be located in the trace by time. The
  bench currently keeps only percentiles; this tier needs the raw slow-sample
  windows. (A debug/instrumented bench build or a temporary dump arm —
  investigation-only, not shipped.)
- Classify each slow sample as one of: **malloc/alloc** (on-CPU in
  `__libc_malloc`/mmap/page-fault inside `append()`), **fdatasync syscall**
  (in `sync_data` on the writer thread), **off-CPU run-queue** (waiter runnable
  but not scheduled), or **idle-C-state-exit** (core idle between serial
  samples, wake latency on the virtualized core).

### Tier 2 — confirm by knob

Re-run the journal bench with:
- both bench + writer threads pinned (`taskset`), and
- deep C-states disabled (`cpupower idle-set -D 1`, or boot
  `intel_idle.max_cstate=1`).

- **Rule:** if the tail collapses → it is **scheduler/C-state wakeup** → the fix
  is pinning / C-state policy, **not** the transplant → record verdict =
  `scheduler-cstate`. Transplant = no-go.

### Tier 3 — only if Tier 1–2 implicate per-append alloc/fan-out

Only reached if the slow samples are on-CPU in allocation or in completion
fan-out (the D1/D2 mechanisms the transplant actually removes). Prototype the
`SeqWatermark` transplant on a branch in `ultima_cluster/ultima_journal` and A/B
`append_consistent_prealloc_p50/p99` before/after on the c6id NVMe.

- **Rule:** transplant ships only if the p99 tail materially collapses in the
  A/B. Record verdict = `alloc-fan-out`.

## Comparative store-WAL leg (decision C: matched microbench + YCSB)

The store WAL's "no tail" was inferred from YCSB-A over ~25,000 commits, where
1%@5 ms would inflate the median visibly. The journal bench is 400 samples
(p99 = 4th-worst) — far more sensitive to a few rare scheduling/device events.
These are not comparable measurements, so the comparison is made on two signals:

- **Matched microbench (primary).** Add a depth-1 serial single-commit p99 arm
  to the store WAL bench using the **same 400 sample count** and the **same
  preallocated path** (`WalWrite::CoalescedPrealloc`) as the journal bench.
  - **Rule:** if the WAL's 400-sample depth-1 p99 is **also ~5 ms**, there is no
    journal-specific tail — it is a sampling/scheduling artifact common to both
    engines → record verdict = `sampling-artifact`. Transplant = no-go.
  - If the WAL's matched p99 stays sub-ms while the journal's is ~5 ms, the tail
    is genuinely journal-specific and Tier 1–3 attribution stands.
- **YCSB cross-check (secondary).** Reuse the existing YCSB-A aggregate as the
  high-sample-count sanity signal corroborating the matched-microbench result.

This leg runs in the same fleet session as the journal tiers (the store WAL A/B
scratch tree lives at `/opt/bench/wal-ab/ultima_db` per the handoff; it dies with
the instance).

## Verdict → decision mapping

| Verdict | Evidence | Transplant |
|---|---|---|
| `device` | Tier 0: c6id `fsync_prealloc_p99 ≈ 5 ms` | no-go |
| `scheduler-cstate` | Tier 2: tail collapses under pin + C-state disable | no-go (fix = pin/C-state) |
| `sampling-artifact` | Store-WAL matched 400-sample p99 also ~5 ms | no-go |
| `alloc-fan-out` | Tier 1 slow samples on-CPU in alloc/fan-out **and** Tier 3 A/B collapses p99 | go |

## Environment / logistics

- AWS fleet (down; restart for this work): 3× `c6id.4xlarge`, us-east-1, via
  `bench-infra`. Inventory `bench-infra/inventory/hosts.yml`. SSH
  `ssh -i /home/claude/.ssh/id_ed25519 ubuntu@<ip>`, passwordless sudo.
- Local NVMe `/opt/bench` (`/dev/nvme1n1`, ext4) — use for `ULTIMA_BENCH_DIR`.
- Build as root with the root-owned toolchain:
  `sudo env PATH=/opt/bench/.cargo/bin:/usr/bin:/bin CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup CARGO_TARGET_DIR=... cargo ...`.
- The rig's `make bench` is the **cluster parity sweep** (Aeron-vs-UC), a
  different benchmark — it does not exercise the store WAL or this microbench.
- **Tear down when done:** `make -C bench-infra destroy`.

## Non-goals

- No `SeqWatermark` transplant ships in this investigation (Tier 3 is a gated
  prototype + A/B, not a merge).
- No default-flip of WAL preallocation (`CoalescedPrealloc` stays opt-in).
- No shared journal/store-WAL preallocation code (the journal left the
  ultima_db workspace; a shared crate would re-couple them).

## Open risks

- `perf`/BCC availability and permissions on the EC2 hosts (may need
  `kernel.perf_event_paranoid` lowering via the passwordless sudo).
- C-state control under EC2 virtualization may be limited; if
  `cpupower idle-set` is a no-op on the instance, Tier 2 falls back to thread
  pinning + a busy-poll variant as the scheduler-isolation signal.
- Per-sample latency capture must not itself perturb the tail (keep the dump
  off the measured path — buffer raw timings, write after the loop).
