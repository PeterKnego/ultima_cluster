# UC v2 — Answered queries count as apply progress: fleet A/B

**Date:** 2026-10-10
**Status:** questions and bars pre-committed before any fleet run (this section
is fixed once data exists; results are appended below it, never edited into
it).
**Kind:** A/B for one fix on branch `fix/query-drain-progress`. Rate bars are
fleet-only; the dev-box readings in §4 are smoke and decide nothing.

## 1. The change

`apply_cycle` (`uc_service/src/apply.rs`) returns whether the cycle made
progress, and that return alone drives the apply agent's idle ladder
(`APPLY_IDLE`: 2 000 spins, 1 024 yields, then 50 µs sleeps). Before this fix
`drain_queries`, which answers up to `QUERY_DRAIN_PER_CYCLE` (64) reads per
cycle, returned nothing, so a service that answered reads but applied no
frames reported "no progress" every cycle and climbed to the sleep rung. That
capped read-only reads at about 550–650k/s per node in two fleet runs
(`uc2-read-spread-2026-10-07.md` §5.4,
`uc2-read-types-under-writes-2026-10-10.md` §4.3), against 2–3M/s with any
write load.

- `f06c9710`: `drain_queries` returns whether it consumed a record, and
  `apply_cycle` returns `drain_queries(st) || progressed`.
- `c1410182`: `drain_queries` is `#[inline(never)]`. Inlined, the extra flag
  changed `apply_cycle`'s layout (501 bytes smaller, identical callee list) and
  the dev-box apply-hop A/B read −1.23 % on the write path, outside a 0.014 %
  resolution (§4).

The arm under test is `c1410182`. The base is `93a46cf3` (main at the time).

## 2. Rows and bars

**Row a — write-path cost (apply hop).** `scripts/apply_ab.sh 93a46cf3
c1410182 --pairs 8` on one idle fleet host, at `--fsms 1` and then
`--fsms 2`, bounded lag. Bar: the script's verdict is not `outside
resolution` with a negative head-vs-base delta. `within resolution` or a
positive delta passes; `inconclusive` is reported as inconclusive, never as a
pass; `inconclusive (driver-bound)` voids the row.

**Row b — the read-only gain.** The read-types matrix
(`m6_fleet_gate.py --read-profile --rp-matrix`, 5 arms: lin/leader,
snap/leader, ryw/leader, snap/spread, ryw/spread; 1 024 reads in flight per
reading host, 20 s per rung, fresh cluster per rung, 2 reps) at **0
writes/s**, run once on the base tree and once on the fix tree. Base tree =
`bench/read-spread` at `b4e7cc4f` (main's library plus the read harness);
fix tree = the same plus the two fix commits cherry-picked (branch
`bench/query-progress-fleet`), so the arms differ only in
`uc_service/src/apply.rs`. Bar: fix ÷ base reads/s (mean of reps) **≥ 2.0 on
every arm**.

**Row c — no harm under writes.** The same two runs at **20 000 writes/s**.
Reported: fix ÷ base reads/s per arm, and writes reached. Bar (spread arms
only, whose rep-to-rep spread was about ±4 % on 2026-10-10): fix ÷ base
**≥ 0.90** for snap/spread and ryw/spread. The leader arms' rep-to-rep spread
was up to ±18 %, too wide for a 2-rep bar, so they are descriptive.

**Validity.** As in `uc2-read-types-under-writes-2026-10-10.md` §2: a rung is
invalid only when a client crashes or two hosts serve; an invalid rung is
reported and never substituted; a missing rep makes that cell INCONCLUSIVE.

## 3. Fleet shape

3 × `c6id.2xlarge`, us-east-1, one placement group (the 2026-10-07 and
2026-10-10 shape). Row a runs on one host with no cluster running on any
host. Rows b and c follow, base tree first, then fix tree.

## 4. Dev-box smoke (before the fleet, decides nothing)

`scripts/apply_ab.sh --pairs 8`, `--fsms 1`, `min_rate` applied frames/s:

| run | A base | B head | B′ head again | head vs base | resolution | verdict |
|---|---:|---:|---:|---:|---:|---|
| `93a46cf3` vs `f06c9710` (inlined) | 22,474,525 | 22,199,018 | 22,202,086 | −1.226 % | 0.014 % | outside resolution |
| `93a46cf3` vs `c1410182` (out of line) | 22,354,180 | 22,483,858 | 22,461,006 | +0.580 % | 0.102 % | inconclusive (noisy run: worst sem 0.442 %, another session's cargo tests running) |

`apply_cycle` sizes (`nm -S`): base 0x5cc9, inlined fix 0x5ad4, out-of-line
fix 0x5797 plus `drain_queries` 0x460 per monomorph. Resolving every
`call *slot(%rip)` in `apply_cycle` through `readelf -rW` gave identical
callee lists for base and inlined fix except one extra `free`, so the inlined
fix's cost was layout, not a callee falling out of line.

## 5. Results

Run 2026-10-10, 22:01–23:50 UTC, 3 × `c6id.2xlarge`, us-east-1, placement
group. Fleet destroyed after the run; teardown verified by `terraform state
list` returning 0 entries and an EC2 `describe_instances` showing no
non-terminated instances.

| row | measured | bar | verdict |
|---|---|---|---|
| a, N=1 | +0.117 % (worst sem 0.186 %, resolution 0.016 %) | not `outside resolution` negative | **INCONCLUSIVE** (noisy run) |
| a, N=2 | +2.953 % (base arm sem 3.209 %: one 10.9M outlier run) | same | **INCONCLUSIVE** (noisy run) |
| b | fix ÷ base 3.81–5.62 on every arm | ≥ 2.0 every arm | **PASS** |
| c | spread arms 1.02 · 1.11 | ≥ 0.90 spread arms | **PASS** |

### 5.1 Row a — write-path cost

`scripts/apply_ab.sh 93a46cf3 c1410182 --pairs 8` on node2, run from a git
bundle of this branch, before any cluster ran on the fleet; each arm built in
its own git worktree and target dir. `min_rate` applied frames/s:

| N | A base | B head | B′ head again | head vs base | resolution | worst sem |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 14,765,618 | 14,782,918 | 14,780,594 | +0.117 % | 0.016 % | 0.186 % |
| 2 | 14,087,043 | 14,503,079 | 14,501,165 | +2.953 % | 0.013 % | 3.209 % |

`pace_stalls` was non-zero on every arm, so neither run is driver-bound.
Both verdicts are `inconclusive (noisy run)`, which §2 says is reported as
inconclusive, not as a pass. Description, not verdict: neither N shows a
negative delta, and the −1.23 % the inlined variant read on the dev box (§4)
did not appear for the out-of-line variant. More pairs would not settle it: a
`within resolution` verdict needs arm sem below a ~0.015 % rebuild
resolution, hundreds of pairs on this rig (the standing bar question in
CLAUDE.md "Next up").

### 5.2 Rows b and c — reads/s, mean of 2 reps

| writes/s | arm | base | fix | fix ÷ base |
|---:|---|---:|---:|---:|
| 0 | lin/leader | 568,292 | 2,164,410 | **3.81** |
| 0 | snap/leader | 574,814 | 3,126,619 | **5.44** |
| 0 | ryw/leader | 607,077 | 3,157,766 | **5.20** |
| 0 | snap/spread | 1,749,369 | 9,776,610 | **5.59** |
| 0 | ryw/spread | 1,699,800 | 9,547,957 | **5.62** |
| 20k | lin/leader | 2,109,217 | 1,872,292 | 0.89 |
| 20k | snap/leader | 3,164,324 | 2,547,617 | 0.81 |
| 20k | ryw/leader | 2,518,283 | 2,384,354 | 0.95 |
| 20k | snap/spread | 8,937,256 | 9,093,314 | **1.02** |
| 20k | ryw/spread | 8,591,082 | 9,515,825 | **1.11** |

All 40 rungs valid (20 per tree), 0 retries on every host, every write target
reached (19,999–20,000 writes/s). At 0 writes worst-host p99 fell from
1.94–1.99 ms to 0.41–0.61 ms on every arm.

- **Row b passes on every arm.** A read-only node now serves about 3.1M
  snapshot or read-your-writes reads/s, where it served about 0.6M; the cap
  recorded in `uc2-read-spread-2026-10-07.md` §5.4 is gone.
- **Row c passes on both spread arms.** The leader arms are descriptive (§2):
  0.81–0.95, inside their rep-to-rep spread (the fix arm's lin/leader reps
  were 1.42M and 2.33M; snap/leader 2.23M and 2.86M). Whether the leader
  arms lose a little under writes is not resolved by 2 reps.
- With the fix, reads no longer run faster under writes than without: at the
  leader, 0 writes now reads at or above 20k writes.

### 5.3 What the numbers measure

`read_profile`'s state machine is a counter: `apply` increments it, `query`
returns it (`uc_node/examples/read_profile.rs`, `ProfileSm`). Writes are real
64-byte commands (appended, replicated, made durable, committed, applied on
every node) and reads are real queries through the rings and the service,
each answer decoded and checked by the client's monotonic guard. So these
are the read pipeline's ceiling, not an application's: a `query` that does
real work adds its own cost per read and shrinks the share of time the old
sleeps cost. The 3.8–5.6× is an upper bound for a cheap query.

### 5.4 One invalid run, not used

The first fix-tree matrix (run directory `fix/`, not committed) ran the
**base** binary. The fix tree was rsync'd onto the hosts with `rsync -a`,
which keeps the source's mtimes: `apply.rs` arrived dated 21:59, older than
the 22:15 base build, so cargo's freshness check rebuilt nothing, and the
driver's `prepare` step reported success. `nm` on the hosts' `read_profile`
found no `uc_service::apply::drain_queries` symbol, which the
`#[inline(never)]` fix creates. After `touch`ing `apply.rs` and rebuilding,
every host's binary carried the symbol (checked before and after the valid
run). That run's numbers matched the base (about 0.6M/s per node read-only)
and are reported only as this cautionary note. Swapping trees on a host that
has already built must change mtimes (`--no-times` or `--checksum`) or touch
the changed files, and should be checked at the binary.
