# Fleet CPU pinning (`--pin`) — PENDING — validation run not yet executed

**Status:** Task 9 Step 1 (the driver change) is done and merged; Task 9
Step 2 (the fleet validation run, ~$3, needs the user's go) has **not run
yet**. Nothing in this document below the method/plan sections is a
measurement — do not cite a spread or an adoption decision from this file
until Step 2 has actually run and this stub has been replaced with the real
numbers.

## Why

`docs/benchmarks/uc2-m14d-ab-2.7.0-vs-2.8.0-2026-08-30.md` found the rig
**multi-modal per cluster generation**: the same binary, same hosts, same
arm produced whole-run rates 25 % apart across otherwise-identical
generations (e.g. 2.8.0: 1.26 / 1.28 / 1.33 / 1.35 / 1.67 / 1.89 M resp/s
over 7 arms). That doc's untested hypothesis: thread placement — the node's
four busy-spin agents, the per-FSM service threads, and the client land on
hyperthread siblings differently each generation on an 8-vCPU host, and a
fast mode is a placement where no hot pair shares a physical core. The
fix under test: pin every role to disjoint physical cores via
`systemd-run -p CPUAffinity=` (`taskset -c` for the one client path that
isn't a systemd unit) and see whether the spread collapses.

## The pin map (`PIN_MAP_C6ID_2XL`, `bench-infra/scripts/m12_fleet_gate.py`)

```python
PIN_MAP_C6ID_2XL = {
    "node": "0,1,4,5",
    "service0": "2",
    "service1": "6",
    "client": "3,7",
    "edge": "3,7",
}
```

Built for an 8-vCPU `c6id.2xlarge` (4 physical cores × 2 SMT threads). The
node's four busy-spin polling agents (consensus/sender/receiver/archive) get
two whole physical cores, both threads each (`0,1,4,5`) — no agent shares a
physical core with another agent or with anything else. Each service gets
one thread of a third core (`service0` on cpu 2, `service1` on cpu 6, its
sibling — left idle on purpose so the two FSMs never share a physical core);
an FSM id ≥ 2 (M14 allows up to 8) has no dedicated pin and shares
`service1`'s thread (`m14_fleet_gate.service_cpu`). `client`/`edge` share the
fourth core's two threads (`3,7`) — they are never both live at once on any
arm these drivers run, so sharing costs nothing.

## The assumption that must be verified before trusting the map

`PIN_MAP_C6ID_2XL` **assumes** `c6id.2xlarge`'s hyperthread sibling pairs are
`(0,4) (1,5) (2,6) (3,7)` — logical CPU `i` and `i+4` are the two SMT threads
of one physical core. **This has not been verified on a real host yet.**
Step 2's validation run must, before trusting any pinned number:

1. Run `lscpu -e=CPU,CORE` (or `-p=CPU,CORE`) on a live `c6id.2xlarge` host
   and record the raw output in this document, replacing this stub.
2. Confirm `m12_fleet_gate.verify_pin_layout` (called automatically by
   `--pin` on every voter before the first arm, via `require_pin_layout`)
   accepted it. If the real layout doesn't match
   `EXPECTED_SIBLING_PAIRS = {(0,4),(1,5),(2,6),(3,7)}`, `--pin` refuses to
   run (`SystemExit`, printing the actual layout) rather than pinning onto
   siblings silently — that refusal, if it fires, is itself the Step 2
   finding and this map needs redrawing before any run proceeds.

## Method (Step 2, not yet run)

```
python3 bench-infra/scripts/m14_ab_27_vs_28.py --fleet --reps 4 --pin \
    --tree27 <local 2.7.0 worktree, pointed at main too — A = B = main> \
    --hosts <pub/priv,...>
```

then the same `--reps 4` **without** `--pin`. `--tree27` pointed at `main`
makes A = B = main, so the 8 arms this produces are a pinned-vs-unpinned
comparison of one binary against itself — isolating the pinning effect from
any 2.7.0/2.8.0 delta. Record both runs' per-version spread
(`spread_pct` in the driver's `SUMMARY-JSON`) here.

## Adoption rule (spec §16.5)

Adopt pinning (flip `--pin` to default **on** in `m14_fleet_gate.py`, and
say so in the gate doc's "Reading the rules") **iff the pinned spread is
< 5 %**. If the pinned run's spread is not clearly better than the unpinned
run's ~25–64 % baseline (per-version spread in the A/B doc above), pinning
is not adopted and this document records why, with both spreads, instead.

## Result

**PENDING.** Nothing below this line exists yet — Step 2 has not run.
