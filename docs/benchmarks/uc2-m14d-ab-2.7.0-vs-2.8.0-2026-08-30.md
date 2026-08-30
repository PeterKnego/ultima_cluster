# A/B on the fleet: 2.7.0 vs 2.8.0 on the direct arm — no regression detectable; the rig is multi-modal

**Date:** 2026-08-30 · **Fleet:** 4 × `c6id.2xlarge`, `us-east-1a`, one
placement group (100.27.192.52 / 54.205.57.205 / 34.234.70.122 voters, one
host idle) · **Driver:** `bench-infra/scripts/m14_ab_27_vs_28.py` ·
**Logs:** `~/.cache/uc2-ab-27-vs-28-2026-08-30.log` (round 1),
`…-round2-2026-08-30.log` (round 2).

**This is a measurement, not a gate.** No bar was committed. It answers one
question the M14 gate could not: *did 2.8.0 regress the single-client direct
arm relative to 2.7.0?* — asked because the M14 gate's `n1` rate
(1,362,555 ops/s on 2026-08-29) sat 3.5 % under the M12 row-1 number for the
same arm (1,424,941 on 2026-08-22), and 22 % under a *differently configured*
M13 arm (1,751,213: raw state machine, envelope off, `hop_bench` client).

## Method

- **A** = `v2.7.0` (`4fcad3c`), **B** = `main` (`e0a5a47`, the released
  2.8.0 tree). Both trees rsynced to every voter and built there with the
  same pinned toolchain (1.96.0); binaries
  `/opt/bench/uc27/target/release/examples/m12_gate` (sha256
  `bf98193…c689f5`) and `/opt/bench/uc/target/release/examples/m12_gate`
  (`6f8df27…811785`).
- **The arm** is M12 row 1 exactly: `m12_gate client-direct`, session
  envelope on (`Sessioned<CountSm>`, typed tier), `--inflight 4096`,
  `--payload 64`, `--secs 12`, one shmem client on the leader host. Rate =
  the RESULT line's whole-run `responses_per_sec` — the one number both
  harness versions print (2.7.0 has no steady-window flags).
- **Every arm is a fresh cluster generation** on one version: instance dirs
  wiped, three voters booted on that version's binary, its own `m6_gate`
  probe for the leader wait (2.7.0 writes a 4 KiB cnc 2.x page the 2.8.0
  probe would refuse). No mixed-wire cluster ever exists.
- **Interleaved:** round 1 A B A B A B; round 2 B A B A B A B A (order
  swapped to expose any ordering effect). 14 arms total, 7 per version.

## The 14 arms

| # | arm | resp/s | p50 ms | p99 ms | lost |
|---|---|---:|---:|---:|---:|
| 1 | A-2.7.0 r1-1 | 1,202,352 | 3.41 | 5.21 | 0 |
| 2 | B-2.8.0 r1-1 | 1,277,668 | 3.29 | 3.47 | 0 |
| 3 | A-2.7.0 r1-2 | 1,487,959 | 2.78 | 3.19 | 0 |
| 4 | B-2.8.0 r1-2 | 1,329,718 | 3.18 | 3.73 | 0 |
| 5 | A-2.7.0 r1-3 | 1,493,518 | 2.85 | 3.27 | 0 |
| 6 | B-2.8.0 r1-3 | 1,347,353 | 3.21 | 3.37 | 0 |
| 7 | B-2.8.0 r2-1 | 989,516 | **0.47** | 1.99 | 0 |
| 8 | A-2.7.0 r2-1 | 1,198,666 | 3.49 | 3.77 | 0 |
| 9 | B-2.8.0 r2-2 | 1,260,018 | 3.36 | 3.70 | 0 |
| 10 | A-2.7.0 r2-2 | 1,065,713 | 4.08 | 4.45 | 0 |
| 11 | B-2.8.0 r2-3 | 1,887,310 | 2.13 | 2.46 | 0 |
| 12 | A-2.7.0 r2-3 | 1,187,367 | 3.54 | 3.80 | 0 |
| 13 | B-2.8.0 r2-4 | 1,666,594 | 2.73 | 3.08 | 0 |
| 14 | A-2.7.0 r2-4 | 1,407,793 | 1.67 | 2.14 | 0 |

| | n | mean | median | min | max | spread |
|---|---:|---:|---:|---:|---:|---:|
| A-2.7.0 | 7 | 1,291,910 | 1,202,352 | 1,065,713 | 1,493,518 | 33 % |
| B-2.8.0 | 7 | 1,394,025 | 1,329,718 | 989,516 | 1,887,310 | 64 % |

**B/A:** 1.079 by mean, 1.106 by median; 1.131 excluding arm 7 (see below).
Round 1 alone read 0.945, round 2 alone 1.194. The two versions' ranges
overlap in both rounds and pooled.

## What it says

1. **No regression from 2.7.0 to 2.8.0 is detectable on this arm.** Every
   point estimate favours 2.8.0 (+8 % to +13 %), but the honest statement is
   the resolution: with per-arm spreads of 33 % and 64 %, this rig cannot
   resolve a version delta smaller than roughly ±25 % in 7 reps. Pooled
   with the dev-box hop A/Bs (apply hop −1.2 % at N=8, client hop ±0.3 %
   at a 1 % build-to-build resolution — `uc2-m14a-apply-hop-2026-08-27.md`,
   `uc2-m14c-client-hop-2026-08-28.md`), the supportable claim is: **2.8.0's
   direct arm is within the noise of 2.7.0's, and the M14 gate's 3.5 % dip
   against the 2026-08-22 M12 number is a day-to-day / fleet-to-fleet
   effect, not a code effect.** The M13 1.75 M figure is a different arm and
   was never comparable.

2. **The rig is multi-modal per cluster generation — this is the finding.**
   The same binary, same hosts, same arm produced 1.07 / 1.19 / 1.20 / 1.20 /
   1.41 / 1.49 / 1.49 M for 2.7.0 and 1.26 / 1.28 / 1.33 / 1.35 / 1.67 /
   1.89 M for 2.8.0. In every arm except #7 and #14 the client's window was
   saturated (p50 ≈ 4096 ÷ rate: 3.4 ms at 1.2 M, 2.8 ms at 1.49 M, 2.1 ms at
   1.89 M), so the mode is the **cluster's service rate for that
   generation**, not client noise. HYPOTHESIS (untested): thread placement —
   the node's four busy-spin agents, the service's apply thread and the
   client's two threads land on hyperthread siblings differently each
   generation on an 8-vCPU host; a fast mode is a placement where the hot
   pair does not share a core. The test is to pin the agents (`taskset` in
   `systemd-run`'s `-p CPUAffinity=`) and see the spread collapse. Until a
   harness does that, **no single-generation fleet number from this rig is
   worth more than ±25 %**, and every prior gate's rate bars (M12 row 1,
   M13 rows a–d, M14 rows a–b) inherited this variance. The M14 bars survived
   it because they are *ratios within one generation*.

3. **The first arm of each round reads low regardless of version** (#1 at
   1.20 M with p99 5.2 ms; #7 at 0.99 M with p50 0.47 ms — the only
   unsaturated arm, i.e. the client itself was slow to start). A cold-start
   effect on freshly built trees / wiped dirs; the next A/B should run one
   discarded warm-up arm per version.

4. **Latency:** at a saturated window p50 is queueing (4096 ÷ rate), so the
   p50/p99 columns say nothing about per-op cost beyond confirming
   saturation. 2.8.0's p99 is tighter (mean 3.1 ms vs 3.5 ms) but that is
   within the same variance.

## What this does not say

Nothing about the multi-FSM paths (rows a/b/e of the M14 gate cover those),
the remote path (M13's rows), or lockstep (row e's 60× is a separate
finding). It does not rank 2.8.0 above 2.7.0 either — the +8 % point estimate
is inside the resolution.

## Links

- The M14 gate: `uc2-m14-gate-2026-08-29.md` (row a's `n1` is the number this
  A/B was asked about).
- The M12 row-1 reference: `uc2-m12-gate-2026-08-22.md` (1,424,941 on
  2026-08-22, same arm).
- Driver: `bench-infra/scripts/m14_ab_27_vs_28.py` (`--order AB|BA`,
  `--reps`, `--no-sync` to reuse built trees).
