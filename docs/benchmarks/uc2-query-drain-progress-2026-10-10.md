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

_Not yet run._
