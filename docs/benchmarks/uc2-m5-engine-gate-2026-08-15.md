# uc2 M5 gate RE-RUN — the public Engine as the measured path (pipelined-client acceptance)

**Date:** 2026-08-15
**Status:** PRE-COMMITTED — this header and the decide rule below are written
and committed BEFORE the fleet run; results land in the "Fleet result"
section at the bottom afterward. (Project discipline: the rule may not be
touched after data exists.)

## Why a re-run

The pipelined-client arc (spec
`docs/superpowers/specs/2026-08-13-uc2-pipelined-client-design.md`, merged
main `c3011e2`) rewrote `m5_gate`'s client role onto the PUBLIC
`uc2_client::Engine` (`SendHalf`/`PollHalf`) — the raw ring pump the original
gate used is gone. The plan's acceptance criterion: **the measured path is
now the shipped path, and the M5 fleet gate must reproduce its numbers
through the public API.** Sandbox smoke showed parity (~407k median vs 435k
baseline on the core-starved box) after the poll-thread idle ruling
(20 µs sleep, old-matcher parity); this run is the real proof.

One measurement-semantics change vs the original harness, deliberate and
review-mandated: the client now also counts engine-swept losses
(`Outcome::TimedOut`/`InstanceRestart` → `lost`) and the PASS bar gained
`lost == 0` — the engine's 30 s deadline sweep would otherwise convert a
lost response into a resolved slot and erase the old harness's
in-flight-at-end evidence. The bar is strictly TIGHTER than the original.

## Pre-committed decide rule

Setup must match the original gate (`uc2-m5-gate-2026-07-12.md`) exactly:
3 × c6id.2xlarge, us-east-1, single AZ, cluster placement group, private-IP
binding, one `node` + one `service` per host under `systemd-run`, instance
dirs on NVMe (`/opt/bench/m5/inst`, fresh per point), client on the leader's
host, 64 B payloads, 15 s per point, the same 7-point sweep
(admission {64,128,256} KiB × inflight {4096,1024} + 64/512).

1. **GATE (spec §9 bar):** PASS iff at least one sweep point clears the
   client binary's own bar — `responses/s ≥ 400_000 && p50 ≤ 1.0 ms &&
   in-flight at end == 0 && lost == 0` — with `RESULT: PASS` printed by the
   binary itself (never inferred).
2. **ENGINE PARITY (the extraction question):** at the historical best point
   (**256 KiB admission, inflight 1024**; original: 1,639,187 resp/s @ p50
   0.600 ms), the re-run's responses/s must be **≥ 90% of 1,639,187
   (= 1,475,268)** with p50 ≤ 1.0 ms. The 10% envelope is the project's
   standing dip rule (M6/M7 precedent). Verdict vocabulary:
   - both (1) and (2) hold → **PASS, parity — extraction accepted**;
   - (1) holds, (2) misses → **PASS with regression** — recorded honestly,
     investigated off-fleet; no on-fleet tuning beyond the pre-committed
     sweep;
   - (1) misses → **FAIL (honest)**, investigate.
3. **Invalidation rules (unchanged from the original doc):** mid-run
   leadership loss (`not_leader > 0`) or a stalled service
   (`in-flight at end` large / `lost > 0` / `broadcast overwritten > 0`)
   invalidates the POINT — diagnose and re-run that point (once); it never
   counts as a sweep result.

Source tree: local main @ `c3011e2` (pushed), rsync-shipped by ansible,
built `--release` on-host.

## Fleet result — RULE 1: **GATE PASSED** · RULE 2: **REGRESSION** (verdict: *PASS with regression*)

Run performed 2026-08-15, one sweep, no point re-run (no invalidation rule
fired — every point had `sends == responses` exactly, `not_leader 0`,
`retries 0`, `dups 0`, `overwritten 0`, `lost 0`, `in-flight at end 0`).
Fleet: 3 × c6id.2xlarge, us-east-1, single AZ, cluster placement group,
private IPs 10.10.1.10/11/12:19100, node+service per host under
`systemd-run`, instance dirs fresh per point on NVMe, client on host0
(node0 won every election under the 0.7 s start bias). Source: main
`c3011e2`, rsync-shipped, built `--release` on-host; driver
`bench-infra/scripts/m5_fleet_gate.py` (`c138941`); raw consoles + JSONL in
`bench-out/m5-engine-2026-08-15/` (local, not committed).

| admission | inflight W | responses/s | p50 | p90 | p99 | GATE | vs 2026-07-12 |
|---|---|---|---|---|---|---|---|
| 64 KiB | 4096 | 1,027,256 | 3.809 ms | 4.106 ms | 5.095 ms | FAIL (latency) | −4.7% |
| 128 KiB | 4096 | 1,540,457 | 2.617 ms | 2.748 ms | 3.213 ms | FAIL (latency) | −16.8% |
| 256 KiB | 4096 | 1,811,972 | 1.992 ms | 2.175 ms | 2.658 ms | FAIL (latency) | −15.5% |
| 64 KiB | 1024 | 1,032,248 | 0.931 ms | 1.066 ms | 1.251 ms | **PASS** | −6.0% |
| **128 KiB** | **1024** | **1,470,985** | **0.655 ms** | 0.725 ms | 0.798 ms | **PASS** | −9.3% |
| 256 KiB | 1024 | 1,416,659 | 0.671 ms | 0.776 ms | 0.919 ms | **PASS** | −13.6% |
| 64 KiB | 512 | 891,307 | 0.546 ms | 0.629 ms | 0.755 ms | **PASS** | −7.3% |

**Rule 1 (spec §9 bar): PASS.** The same four points as the original clear
the bar — now the strictly TIGHTER bar including `lost == 0` — with
`RESULT: PASS` printed by the client binary. Best PASS point this run:
**1,470,985 responses/s @ p50 0.655 ms (128 KiB / W=1024), 3.7× the 400k
bar**, through the public `uc2_client::Engine`. The stretch goal (800k)
remains exceeded, 1.8×.

**Rule 2 (parity @ 256 KiB/1024): REGRESSION by the pre-committed floor.**
1,416,659 vs floor 1,475,268 (90% of 1,639,187) — −13.6% vs baseline,
missing the floor by 4.0%. Per the rule this is recorded as **PASS with
regression**; the point was NOT re-rolled (no invalidation fired, and the
rule forbids on-fleet tuning).

Fastest-PASS-point client console, verbatim:

```
================ uc2 M5 gate: client -> apply -> response ================
sends                 : 22065602
responses             : 22065602
not_leader redirects  : 0
retries               : 0
dup responses dropped : 0
broadcast overwritten : 0
in-flight at end      : 0
lost (timeout/restart): 0
elapsed (drain-incl.) : 15.001 s
responses/s           : 1470985
p50                   : 0.655 ms
p90                   : 0.725 ms
p99                   : 0.798 ms
max                   : 38.339 ms
bar                   : responses/s >= 400000 && p50 <= 1.0 ms && in-flight at end == 0 && lost == 0
============================================================================
RESULT: PASS
```

### Honest analysis of the regression signal (analysis, not verdict-bending)

- **The deltas are not uniform, and the pattern does not cleanly implicate
  the engine.** The −4.7% / −16.8% / −15.5% spread at W=4096 sits in the
  pipeline-saturated band where the commit pipeline (untouched by the client
  extraction) dominates; the W=1024 PASS band spreads −6.0% to −13.6%. A
  client-side cost would be expected to show most uniformly at the
  client-paced points, not to put its largest deltas at 128/4096 and
  256/4096.
- **The 128↔256 saturation flip:** in the original, 256 KiB ≥ 128 KiB at
  W=1024 (+1.0%); this run 128 KiB > 256 KiB (+3.8%). The admission-window
  saturation point moved — consistent with fleet-to-fleet variance (these
  are different physical hosts/placement than 2026-07-12), not with a fixed
  per-request client cost.
- **Latency at the operating points is at parity:** 0.931 ms is IDENTICAL
  to the original 64/1024 point; the p50 penalty at the other PASS points is
  45-70 µs — consistent with the engine's extra per-request atomics but far
  too small to explain a 13% throughput delta by itself.
- **Same-hardware evidence points the other way:** the same-box sandbox A/B
  (old pump vs engine, same day, `m5-smoke-before/after2`) measured ~-7%,
  within that box's run-to-run noise band.
- **Conclusion:** the pre-committed floor was missed, so REGRESSION is the
  recorded verdict at the named point; the data is consistent with a
  mixture of fleet-to-fleet variance and a real-but-small engine cost, and
  THIS run cannot separate the two.

### Off-fleet follow-up (per rule 2's "investigated off-fleet")

The clean discriminating experiment, if/when the ~10-14% matters: ONE fleet,
BOTH client binaries (pre-extraction `m5_gate` from `35daaae^` and the
engine build), interleaved A/B/A/B points at 256 KiB/1024 on the same
hosts/placement — removes fleet variance entirely. Secondary: perf-profile
the engine submit path (the claim path is ~6 atomic RMWs vs the old pump's
2 stores; `inflight` is a shared cross-thread cacheline). Neither is fleet-
gated work; both are cheap to run the next time a fleet is up for any
reason.

**AWS: fleet destroyed immediately after the run; `terraform state list`
verified EMPTY (the real state in `bench-infra/terraform/`).**

---

## A/B follow-up — PRE-REGISTERED (written & committed before the A/B data)

**Question:** on identical hardware, is the engine client slower than the
raw-pump client at the regression point, or was the −13.6% fleet-to-fleet
variance?

**Design** (one fresh fleet, same 3 × c6id.2xlarge protocol):
- Server side IDENTICAL for both arms and all runs: node + service processes
  from main `c3011e2`'s `m5_gate` on every host. (Verified: `git diff
  35daaae c3011e2` touches NO server-side crate — `uc2_node/src`,
  `uc2_service`, `uc2_log`, `uc2_net`, `uc2_consensus`, `uc_protocol` are
  byte-identical; the two trees differ only in `uc2_client`, the m5_gate
  CLIENT role, and docs.)
- Arm A: the PRE-extraction client (raw ring pump) — `m5_gate` built from
  `35daaae` (the last commit before the Task-8 client rewrite), on host0 at
  `/opt/bench/uc-old`.
- Arm B: the engine client — main's `m5_gate`.
- **Primary: 6 interleaved pairs (A then B), 256 KiB admission / W=1024,
  15 s, 64 B payloads, fresh instance dirs per RUN.** n = 6 pairs is FIXED
  in advance (house rule: fix n before coding); no extension, no
  re-rolls except the standing per-run invalidation rule (redirects > 0 /
  lost > 0 / in-flight-at-end > 0 → re-run that single run once).
- Secondary, context only (no verdict weight): 3 pairs at 128 KiB / W=1024.

**Pre-committed decide rule:** pairwise ratio `r_i = B_i/A_i` on
responses/s; **median(r) < 0.95 → ENGINE COST CONFIRMED** (the extraction
costs ≥5% at this point and the gate doc's regression is at least partly
real engine cost); **median(r) ≥ 0.95 → PARITY** (the −13.6% is attributed
to fleet-to-fleet variance; note the old client's own numbers on THIS fleet
land in the same section either way, so the variance claim is directly
checkable against 2026-07-12's 1,639,187). Report per-arm medians, min/max
ratios, and p50s alongside; the old binary prints no `lost` line (the field
was added with the engine client) — parse accordingly.

### A/B result — **PARITY** (primary median ratio 0.9616 ≥ 0.95): the gate-run regression is attributed to fleet-to-fleet variance

Run 2026-08-15, fresh 3 × c6id.2xlarge fleet, arm A binary built on host0
from `35daaae` (`/opt/bench/uc-old`), arm B + all node/service processes
from main `c3011e2`. All 18 runs clean (no invalidation fired anywhere:
zero redirects, zero lost, zero in-flight residue). Driver
`bench-infra/scripts/m5_ab_gate.py` (`2f5c2f0`); raw consoles + JSONL in
`bench-out/m5-ab-2026-08-15/` (local). Fleet destroyed immediately after;
`terraform state list` verified EMPTY.

**PRIMARY (256 KiB / W=1024, 6 interleaved pairs — the pre-registered
verdict block):**

| pair | A (old pump) rps @ p50 ms | B (engine) rps @ p50 ms | B/A |
|---|---|---|---|
| 1 | 1,487,685 @ 0.591 | 1,459,770 @ 0.680 | 0.9812 |
| 2 | 1,505,603 @ 0.643 | 1,402,386 @ 0.671 | 0.9314 |
| 3 | 1,465,124 @ 0.671 | 1,433,742 @ 0.680 | 0.9786 |
| 4 | 1,449,303 @ 0.673 | 1,335,344 @ 0.771 | 0.9214 |
| 5 | 1,480,219 @ 0.632 | 1,398,352 @ 0.709 | 0.9447 |
| 6 | 1,464,261 @ 0.569 | 1,522,244 @ 0.612 | 1.0396 |

Medians: **A = 1,472,672 · B = 1,418,064 · pairwise-median ratio = 0.9616**
(min 0.9214, max 1.0396). **≥ 0.95 → PARITY** per the pre-committed rule.

**SECONDARY (128 KiB / W=1024, 3 pairs, context only, no verdict weight):**
ratios 0.8507 / 1.0009 / 0.8135 — median 0.8507. Noisy and small-n by
design; two of three B runs at this point coincided with visibly degraded
runs (p50 0.769/0.822 ms vs A's ~0.65 ms), the same run-to-run mode swings
the primary block shows in both arms.

**Reading, in order of what the data actually establishes:**

1. **The decisive datum: arm A — the UNCHANGED July client — medians
   1,472,672 on this fleet, itself −10.2% below its own 2026-07-12 number
   (1,639,187).** The gate-run's "regression" band (−13.6%) is almost
   entirely reproduced by the OLD binary on new hardware: fleet-to-fleet
   variance (different physical hosts/placement) is real and ≈10% at this
   operating point.
2. The engine client sits ~4% below the old pump in pairwise median
   (0.9616), with per-pair spread 0.92–1.04 — the engine WON one pair
   outright. A small real cost of roughly this size is consistent with the
   engine's extra per-request atomics, but it is inside the pre-committed
   parity envelope and much smaller than run-to-run variance on a single
   pair.
3. Combined with the gate re-run: RULE 2's REGRESSION verdict stands as
   recorded (the floor was computed against a different fleet's baseline),
   but the A/B resolves its open question — **the extraction did not cost
   the ~10-14% the raw comparison suggested; the public-Engine client is at
   parity within ~4% on identical hardware, and both gate verdicts
   (spec-§9 bar PASS at 3.7×, engine parity) now hold together.**

No further follow-up is fleet-gated. If the residual ~4% ever matters, the
profiling avenue stands (claim-path RMW count; the shared `inflight`
cacheline), and the deferred raw-passthrough / SendHalf-per-thread paths
(spec §10) are the structural wins available above the ticket layer.
