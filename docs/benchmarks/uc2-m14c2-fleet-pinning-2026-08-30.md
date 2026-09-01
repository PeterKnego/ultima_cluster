# Fleet CPU pinning (`--pin`) — RAN 2026-08-31 — NOT ADOPTED

**Status:** Task 9 Step 2 executed on a real fleet, 2026-08-31, on `main`
`6b431ee`. The adoption rule was fixed before the run and is applied below
unchanged. **Verdict: pinning is NOT adopted** — it does not clear the bar.
`--pin` stays opt-in and off by default.

**2026-09-01:** the not-adopted verdict got independent cross-architecture
support: on a 16-core no-SMT host (`c9gd.4xlarge`, Graviton) pinning costs
**20 %** of throughput while unpinned wins outright — the variance benefit
(p50 span ~37× tighter) is the same, the throughput price is steeper. See
[`uc2-arch-sweep-c8id-vs-c9gd-2026-08-31`](uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md).

This run also did something the stub could not: it **verified the
hyperthread-sibling assumption** `PIN_MAP_C6ID_2XL` was built on, which until
now was a documented guess.

## Why this was run

[`uc2-m14d-ab-2.7.0-vs-2.8.0-2026-08-30.md`](uc2-m14d-ab-2.7.0-vs-2.8.0-2026-08-30.md)
found the rig **multi-modal per cluster generation**: the same binary, same
hosts, same arm produced whole-run rates ~25 % apart across otherwise
identical generations. Its untested hypothesis was thread placement — the
node's four busy-spin agents, the per-FSM service threads and the client land
on hyperthread siblings differently each generation on an 8-vCPU host, and a
fast mode is a placement where no hot pair shares a physical core.

`--pin` pins every role to disjoint physical cores via
`systemd-run -p CPUAffinity=`. This document is whether that fixes it.

## The assumption, now verified

`PIN_MAP_C6ID_2XL` assumes logical CPU `i` and `i+4` are the two SMT threads
of one physical core. **Confirmed on all three voters** (`lscpu -e=CPU,CORE`,
Intel Xeon Platinum 8375C @ 2.90 GHz, 4 physical cores × 2 threads):

```
CPU:  0 1 2 3 4 5 6 7
CORE: 0 1 2 3 0 1 2 3      → sibling pairs (0,4) (1,5) (2,6) (3,7)
```

That is exactly `EXPECTED_SIBLING_PAIRS`, so `require_pin_layout` accepted and
the map pins what it claims to pin. Confirmed live in the unit commands:
node `CPUAffinity=0,1,4,5`, service0 `=2`, service1 `=6`, client/edge `=3,7`.

## Method as run

4 × `c6id.2xlarge`, us-east-1, one placement group; 3 voters + 1 unused
(the driver parses 4 host entries but starts units on `hosts[:3]`).

```
python3 bench-infra/scripts/m14_ab_27_vs_28.py --fleet --reps 4 --pin \
    --tree27 /home/claude/ultima/ultima_cluster --hosts <4 pub/priv>
python3 bench-infra/scripts/m14_ab_27_vs_28.py --fleet --reps 4 --no-sync \
    --tree27 /home/claude/ultima/ultima_cluster --hosts <4 pub/priv>
```

`--tree27` points at `main`, so **A = B = main**: the 8 arms per run are one
source against itself and no 2.7.0/2.8.0 delta can contaminate the spread.
The second run uses `--no-sync`, so it reuses the **identical binaries** —
pinning is the only variable between the two runs.

The two arms are still two distinct *builds* of that one source
(`d6fcdeb…` at `/opt/bench/uc`, `95e476c…` at `/opt/bench/uc27` — Rust
embeds build paths, so same source ≠ same bytes). That does not contaminate
the result: `spread_pct` is computed per version, so each figure is the
spread across 4 reps of ONE binary, and build-to-build variation sits between
A and B rather than inside either spread.

## Result

Whole-run `responses_per_sec`, `m12_gate` client-direct, envelope on,
inflight 4096, payload 64 B, 12 s per arm. **Zero lost responses in all 16
arms.**

| | pinned | unpinned |
|---|---|---|
| A mean | 1 576 963 | 1 898 841 |
| A range | [1 465 851 .. 1 694 866] | [1 841 550 .. 1 968 240] |
| **A spread** | **14.5 %** | **6.7 %** |
| B mean | 1 627 123 | 1 639 369 |
| B range | [1 605 661 .. 1 656 645] | [1 123 630 .. 1 900 439] |
| **B spread** | **3.1 %** | **47.4 %** |
| pooled mean (8 arms) | 1 602 043 | 1 769 105 |
| pooled range | [1 465 851 .. 1 694 866] | [1 123 630 .. 1 968 240] |
| **pooled spread** | **14.3 %** | **47.7 %** |

Per-arm points, in run order:

```
pinned     A1 1694866  B1 1656645  A2 1587779  B2 1605661
           A3 1559355  B3 1628618  A4 1465851  B4 1617567
unpinned   A1 1853847  B1 1866239  A2 1968240  B2 1900439
           A3 1841550  B3 1667167  A4 1931727  B4 1123630
```

## Adjudication against the pre-committed rule

The rule (spec §16.5), fixed before any measurement:

> Adopt pinning (flip `--pin` to default **on**) **iff the pinned spread is
> < 5 %**.

**Pinned pooled spread is 14.3 %; the per-version pinned spreads are 14.5 %
and 3.1 %.** One of the two clears 5 %, the pooled figure and the other do
not. The bar is not met. **NOT ADOPTED.**

## What the numbers do say

**1. The multi-modality is real and was reproduced.** Unpinned B rep4 came in
at 1 123 630 against 1 900 439 for its own rep2 — a 41 % collapse of one
generation, p50 3.586 ms vs 2.064 ms, with zero lost responses. That is the
effect `uc2-m14d` described, seen again on fresh hardware.

> **Follow-up, 2026-08-31 (corrected same day):** a first look suggested the
> residual variance was **two stable regimes**; a 16-arm probe with per-second
> timelines refuted that — the gap fills in with more samples and no arm ever
> transitions between levels
> ([`uc2-regime-probe-2026-08-31.md`](uc2-regime-probe-2026-08-31.md)).
>
> What the residual spread actually is: **a long low tail in the UNPINNED
> distribution.** Measured over 8 arms each, pinning tightens p50 spread
> **31x** (0.030 ms vs 0.929 ms) and rate spread from 22 % to 5.6 %. So
> pinning's value is variance reduction — which is what this document was
> trying to measure, with a rule (spread < 5 %) applied at **n=4, too few to
> tell a distribution's width from its tail**. The −9.4 % throughput cost
> below stands. A re-run of this adoption decision should fix its rep count
> from observed arm-to-arm variance before adjudicating.

**2. Placement is *a* cause, not *the* cause.** Pooled spread falls
47.7 % → 14.3 % with pinning, a 3.3× reduction, and the catastrophic
low-mode arm never appears in the pinned run (pinned minimum 1 465 851, vs
1 123 630 unpinned). So the hypothesis is partly right: pinning removes the
worst mode. But 14.3 % residual is nowhere near 5 %, so a second source of
variance remains unidentified.

**3. NEW, and not anticipated by the hypothesis: pinning costs throughput.**
Pooled mean drops 1 769 105 → 1 602 043, **−9.4 %**, and the pinned *maximum*
(1 694 866) is below the unpinned *mean*. Pinning trims the top of the
distribution as well as the bottom. A rig change that costs ~9 % of the
number being measured needs a much better reason than it currently has.

**4. Four reps is too few to estimate a spread.** The per-version figures
disagree in direction: pinning took A from 6.7 % → 14.5 % (worse) and B from
47.4 % → 3.1 % (much better), on identical source and identical pinning.
Both unpinned spreads are dominated by whether an outlier happened to land in
that particular sample of four — B's 47.4 % is one arm. **The spread statistic
is itself noisy at n=4**, which is a limitation of the method in the spec, not
of this execution. Any future attempt should fix the rep count from the
observed arm-to-arm variance rather than inherit `--reps 4`.

## What this does not settle

- **Row e has still not been re-measured.** The M14 gate's 60× lockstep
  finding is untouched by this run; confirming that pinning recovers it was
  the original motivation and remains open.
- **Why the residual 14.3 % exists.** Not diagnosed. Candidates not examined
  here: EBS/NVMe interference, the placement group's actual topology, C-state
  or turbo behaviour under a busy-spin load, and the interaction between
  pinning and the 4-vCPU-per-role allocation itself.
- **One session, one fleet.** 16 arms total. The `−9.4 %` throughput cost is
  the firmest number here (pinned max < unpinned mean), but it is still a
  single session.

## Reproducing

```bash
cd bench-infra && make up-uc                       # 4x c6id.2xlarge, ~$1.61/hr
python3 scripts/m14_ab_27_vs_28.py --fleet --reps 4 --pin \
    --tree27 <checkout of main> --hosts <pub/priv x4>
python3 scripts/m14_ab_27_vs_28.py --fleet --reps 4 --no-sync \
    --tree27 <checkout of main> --hosts <pub/priv x4>
cd bench-infra && make destroy && terraform -chdir=terraform state list  # must be empty
```

`ttl_hours` is an advisory tag; nothing reaps the hosts. This run's fleet was
destroyed immediately after the second arm — 12 resources destroyed,
`terraform state list` empty, 0 resources in state.
