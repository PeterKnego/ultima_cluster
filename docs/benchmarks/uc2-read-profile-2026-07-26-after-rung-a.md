# UC v2 — Linearizable-read profile: post-Rung-A fleet re-measurement

**Date:** 2026-07-26 (same day as the baseline)
**Baseline:** `docs/benchmarks/uc2-read-profile-2026-07-26.md` (main @ `913e38d`)
**This run:** main @ `16eff8f` (Rung A batch-probe coalescing merged) — identical
fleet class (3 × `c6id.2xlarge`, us-east-1, placement group), identical sweep
(`--rp-readers 1,4,16,64,256,1024 --rp-secs 20 --rp-write-rate 20000`, fresh
cluster per rung), identical pre-committed rule via `read_profile decide`.
**Raw data:** `uc2-read-profile-2026-07-26-after-rung-a-rungs.jsonl` (24 rungs,
24/24 valid, 0 client failures). Fleet destroyed after the run; state verified
empty.

---

## 1. Headline

> **The ReadIndex barrier's capacity cost went from ~58% to ~0.** The
> pre-committed rule now returns **"Rung A NOT JUSTIFIED — the barrier costs at
> most 0.0% (read-only) / 3.3% (mixed) of read capacity"** — which is the rule
> working as designed: the optimization it justified has landed, and nothing
> further is justified.

| | baseline lin plateau | post-Rung-A lin plateau | baseline ratio | post ratio |
| --- | ---: | ---: | ---: | ---: |
| Read-only | 244,052 r/s | **542,065 r/s** (2.2×) | 41.7% | **100.2%** (clamped: cost ≤ 0.0%) |
| Mixed (20k writes/s) | 206,520 r/s | **953,030 r/s** (4.6×) | 21.0% | **96.7%** (cost ≤ 3.3%) |

The linearizable and snapshot arms are now statistically indistinguishable at
plateau. The mixed arm sustains **~953k linearizable reads/s across 3 hosts at
p50 1.08 ms** — 4.6× the baseline — with zero retries, zero redirects, zero
regressions, zero unresolved reads across all 24 rungs, and client in-flight
depth at 99.9%+ of target throughout.

Latency converged as predicted, not just throughput: lin p50 at 256 readers
went 1.763 ms → **0.472 ms** (read-only), landing within 2% of snapshot's
0.464 ms. Rung A amortizes probe *coordination*; the remaining lin-vs-snap p50
gap at low concurrency (0.163 vs 0.082 ms at 1 reader) is the one probe RTT a
lone read still pays, exactly as the spec's lifecycle predicts (a lone read
gets its own immediate round).

## 2. The ladder (post-Rung-A)

### Read-only arm (`write_rate=0`)

| readers | lin r/s | snap r/s | lin/snap | lin p50 (ms) | baseline lin r/s |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 7,862 | 8,801 | 89% | 0.163 | 6,985 |
| 4 | 27,982 | 34,015 | 82% | 0.163 | 24,577 |
| 16 | 95,796 | 133,198 | 72% | 0.167 | 83,862 |
| 64 | 378,223 | 524,367 | 72% | 0.178 | 128,145 |
| 256 | 524,399 | 525,793 | 100% | 0.472 | 145,302 |
| 1024 | **542,065** | 540,965 | 100% | 1.942 | 244,052 |

### Mixed arm (`write_rate=20000`)

| readers | lin r/s | snap r/s | lin/snap | lin p50 (ms) | baseline lin r/s |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 5,838 | 8,498 | 69% | 0.168 | 5,637 |
| 4 | 23,066 | 33,025 | 70% | 0.168 | 21,412 |
| 16 | 84,263 | 140,799 | 60% | 0.175 | 75,789 |
| 64 | 329,564 | 548,592 | 60% | 0.185 | 161,061 |
| 256 | 932,922 | 985,799 | 95% | 0.281 | 162,568 |
| 1024 | **953,030** | 977,184 | 98% | 1.079 | 206,520 |

Health: 0.0% degraded on every rung; `inflight_at_end = 0` everywhere; depth
mean ≥ 99.9% of target at every plateau rung (e.g. 1023.2/1024, min 931).

## 3. The control held

The snapshot arm — untouched by Rung A, verified byte-identical in review — is
the comparison's integrity check:

- **Mixed-arm control: 985,799 vs baseline 982,997 r/s — within 0.3%.** As
  clean a cross-fleet reproduction as UDP benchmarking gets.
- **Read-only control:** plateau 540,965 (@1024) vs baseline 585,414 (@256) —
  −7.6%, and the shape shifted (baseline peaked at 256 then dipped; this run is
  flat 524–541k from 64 up). Two different EC2 placements on two boots; the
  mixed-arm agreement at 0.3% says the harness and control path are stable, and
  the baseline's 585k@256 spike looks like that run's noise, not this one's.

## 4. The instrument's own caveats, addressed

Both post-fix guard NOTEs fired in the read-only verdict, as they should have:

1. **"Arms plateau within 2% — suspect the drain cap or the load generator."**
   The load generator is exonerated by the mixed arm: the same client machinery
   sustained ~980k reads/s there, so the ~540k read-only plateau is not the
   client's ceiling. What binds the read-only plateau is a **shared node-side
   ceiling common to both arms** — the equal plateau is the finding, and it is
   exactly the situation where "the barrier costs ~0" is the correct reading:
   whatever now limits read-only throughput, it is not the barrier, because
   removing per-read probes moved lin from 145k to 524k at 256 readers while
   snapshot stayed put.
2. **"Lin OUT-RAN snap (ratio > 100%) — re-examine."** 542,065 vs 540,965 is
   +0.2% — run-to-run noise on equal plateaus, precisely the benign case the
   note exists to distinguish from a stalled snapshot arm (which would show
   `inflight_at_end != 0`; all rungs show 0).

One pre-existing oddity, noted for honesty and out of scope: both arms run
**faster** under 20k writes/s than read-only (986k vs 526k snap; present in the
baseline too: 983k vs 585k). It is arm-symmetric in both runs, so it cannot
bias the lin/snap comparison; understanding it (duty-cycle occupancy under
write traffic is the obvious suspect) is a separate investigation if ever
wanted.

## 5. Disposition

- **The leader-lease brief's LAN-throughput motivation is discharged.** Rung A
  captured essentially the entire gap the 2026-07-26 baseline measured. Per the
  brief's own sequencing ("measure after A before committing to B"), **Rung B
  (time-based leases) has no remaining LAN-throughput justification** — the
  residual 0–3.3% is inside run noise. Rung B would now be motivated only by
  the WAN/cross-region story the brief explicitly scoped out.
- The read-only shared ceiling (~540k r/s on both arms) is the next lever if
  read-only throughput ever matters: spec §6 threat 2's candidates
  (`QUERY_DRAIN_PER_CYCLE`, egress broadcast) now apply to both arms equally.
  Not scheduled.
- Deferred Rung A minors (majority-formula unification, retransmit-while-idle
  tightening) remain ledgered in the merge history; nothing here changes their
  priority.

## 6. Cost and hygiene

One provision + one 24-rung sweep + destroy, ~50 minutes of 3 × `c6id.2xlarge`.
Teardown verified by `terraform -chdir=terraform state list` returning empty
(the destroy-output-is-not-proof rule).
