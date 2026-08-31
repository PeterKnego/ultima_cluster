# The "two regimes" were one distribution — a per-second timeline probe

*Ran 2026-08-31 on `main` `04ee1d8`. 3 × `c8id.4xlarge`, us-east-1. 16 arms
(8 × `w6` pinned, 8 × unpinned), 12 s each, `--timeline` on, zero lost
responses. Fleet destroyed immediately after.*

**Result: there is no bistability.** The two "operating regimes" reported in
[`uc2-node-core-count-sweep-2026-08-31`](uc2-node-core-count-sweep-2026-08-31.md)
do not survive a larger sample. They were the two ends of **one broad
distribution with a long low tail**, mistaken for discrete states because 21
arms left a gap in the middle and the classification threshold was chosen
after seeing the data.

What IS real, and is the useful finding: **pinning does not remove a
bistability — it removes the tail**, tightening p50 spread by ~31×.

## Why this run happened

The sweep found 7/21 arms at p50 0.32-0.37 ms and 14/21 at 1.23-1.75 ms, with
a 0.872 ms gap and nothing inside it. Two readings were possible and they
imply different mechanisms: a regime fixed at cluster startup, or one that
flips mid-run. Aggregate p50/p99 cannot separate them. `--timeline` (already
in `m12_gate`, one `TL {"sec":..,"unix_ms":..,"responses":..}` line per
elapsed second) can.

## Harness defect found first

`--timeline` was not plumbed through `m12_fleet_gate.run_direct_arm`, and once
added, the first probe produced **16 arms of unusable data that looked
perfectly normal**: every `POINT` was plausible (3.10 M/s, p50 1.243 ms), but
every `TL` row was seconds 32-51 with `responses: 0`.

Cause: `echo(prefix, out, lines=40)` prints the last 40 lines, and
`--timeline` emits one row per bucket of a FIXED-SIZE array **including its
unused tail**. A 12 s run's 12 data rows fell off the front; only zero-filled
padding survived. Fixed by widening the echo when timeline is on. The lesson
is the one this session kept re-learning: **a plausible aggregate is not
evidence that the underlying data exists.**

## Result

| | rate | p50 | mean climb (first 3 s → last 3 s) |
|---|---|---|---|
| `w6` pinned (n=8) | 3.04 M [2.95 .. 3.12] | 1.264 ms [1.251 .. 1.281] | +3.2 % |
| unpinned (n=8) | 2.68 M [2.45 .. 2.99] | 0.917 ms [0.361 .. 1.290] | +5.1 % |

All 16 p50s, sorted:

```
0.361 0.467 0.592 1.053 1.062 1.241 1.251 1.252
1.256 1.262 1.266 1.268 1.270 1.276 1.281 1.290
```

**The gap closed.** Largest gap is now 0.461 ms (0.592 → 1.053), down from the
sweep's 0.872 ms, and 0.592 / 1.053 / 1.062 sit exactly where the sweep had
nothing. Unpinned alone produced both 2.45 M and 2.99 M arms — spanning what
had been called two separate regimes.

## What the timelines show

**Every arm's level is set in second 0 and held.** No arm steps between
levels. On top of that both groups drift upward 3-5 % over the run — an
ordinary warmup, present in pinned and unpinned alike.

```
w6 rep1 (3.01 M)      2.80 3.14 3.14 3.03 3.16 3.06 2.97 2.97 3.13 3.00 2.63 3.09
unpinned rep1 (2.49M) 2.58 2.50 2.44 2.30 2.34 2.23 2.30 2.41 2.46 2.86 2.69 2.81
```

The apparent mid-run "transition" in `unpinned rep3` was a **+11.4 % warmup
climb** — statistically indistinguishable from `unpinned rep1`'s +11.2 %. The
only difference is that rep3's p50 landed at 1.062 and crossed the 0.8 ms
classification line while rep1's landed at 0.467 and did not. That is a defect
in the classification, not a property of the system.

## The real finding: pinning removes the tail

| | p50 span over 8 arms | rate span |
|---|---|---|
| pinned `w6` | **0.030 ms** | 5.6 % |
| unpinned | **0.929 ms** | 22 % |

**31× tighter p50 under pinning.** Pinning's value is variance reduction, not
the elimination of a second state. And it still costs throughput — consistent
with [`uc2-m14c2-fleet-pinning`](uc2-m14c2-fleet-pinning-2026-08-30.md)'s
measured −9.4 % mean.

## Corrections this forces

1. **The sweep doc's "two stable regimes" section is wrong** and is marked
   superseded at its head. Its core-count answer (4 cores) is unaffected — it
   was computed from arms that were all in the high band.
2. **The pinning doc's 2026-08-31 follow-up note is wrong** where it cites a
   bistability as the better candidate for the residual spread. The better
   candidate is a long low tail in the unpinned distribution.
3. **The pinning run's adoption rule tested the wrong statistic at the wrong
   n.** A spread over 4 reps cannot distinguish a distribution's width from a
   tail; this run needed 8 per arm before the gap filled. Any future spread
   bar should fix its rep count from observed arm-to-arm variance first.

## Limits

- 16 arms, one host type, one session, direct shmem path only (no gateway).
- Only `w6` and unpinned were probed — the configs the sweep showed producing
  both bands. Intermediate widths were not re-measured.
- "No bistability" is a claim about THIS operating point. It does not rule out
  genuine bimodality at other payloads, inflight windows or host types.
- The 3-5 % warmup climb means 12 s arms include warmup. `m12_gate` has
  `--warmup-secs`/`--measure-secs` (a steady window, `window_rps`), and the
  driver used here — `m14_core_sweep.py` — does **not** pass them:
  `window_rps` was 0 in all 37 arms measured today, so every rate in this doc
  and in the core-count sweep includes the climb. (An earlier revision of this
  bullet said *no* fleet driver used them. That is wrong:
  `m14_fleet_gate.py` sets `WARMUP_SECS, MEASURE_SECS = 2, 8` and only rows d
  and f opt out, so the M14 gate's rows a/b/e ARE steady-window numbers. The
  consequence is that rates from this doc are not directly comparable with a
  gate row.) Passing them here would remove the drift from every future
  number.

## Reproducing

```bash
cd bench-infra && make up-uc     # 3x c8id.4xlarge
python3 scripts/m14_core_sweep.py --fleet --no-sync --timeline \
    --widths 6 --reps 8 --secs 12 --hosts <pub/priv x3>
cd bench-infra && make destroy && terraform -chdir=terraform state list
```

Destroyed: 11 resources, `state list` empty, 0 EC2 instances tagged
`uc-bench` by direct API query.
