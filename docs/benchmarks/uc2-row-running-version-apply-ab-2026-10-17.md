# UC v2 — The row running version (#33): fleet apply-hop A/B

**Date:** 2026-10-17
**Status:** question and bar pre-committed before any fleet run (this section
is fixed once data exists; results are appended below it, never edited into
it).
**Kind:** A/B for one merged change, owed by PR #68 before the `2.14.0` cut
(`docs/releases.md` § Release evidence). Rate bars are fleet-only; the
dev-box reading in §3 is smoke and decides nothing.

## 1. The change

#33 (PR #68) gives every row a committed running version. In the apply hot
loop (`uc_service/src/apply.rs`) it adds one arm: a `FRAME_TYPE_CLUSTER`
frame goes to `version_gate::on_cluster_frame`, which passes it, stops the
service at a record that supersedes its version, or leaves it pending and
rewinds the cursor to that record. A `MESSAGE` frame takes the existing path,
so the added cost on the write path is the extra type test and whatever the
new arm does to the loop's code layout (the M14a lesson in CLAUDE.md:
code in a hot loop's body can cost on paths that never run).

- Base `a409b3ae`: `main` immediately before #68 merged.
- Head `a70539a1`: the #68 merge.
- `uc_node/examples/apply_bench.rs` is byte-identical between the two
  commits, so both arms build the same harness and differ only in the
  library under it. No `--harness` overlay is needed.

## 2. Row and bar

**Row a — write-path cost (apply hop).** `scripts/apply_ab.sh a409b3ae
a70539a1 --pairs 12` on one idle fleet host, at `--fsms 1` and then
`--fsms 2`, bounded lag, default `--secs 6` / `--warmup-secs 1`. Bar: the
script's verdict is not `outside resolution` with a negative head-vs-base
delta. `within resolution` or a positive delta passes; `inconclusive (noisy
run)` is reported as inconclusive, never as a pass; `inconclusive
(driver-bound)` voids the row. Each N is judged on its own.

`--pairs 12` rather than the 8 of `uc2-query-drain-progress-2026-10-10.md`
row a: that run's N=2 read `inconclusive (noisy run)` with a worst sem of
3.2 % against a 0.013 % resolution, and the arm sem shrinks as 1/√K.

**Rig.** One `c6id.2xlarge`, us-east-1, the shape of every earlier fleet-host
`apply_ab.sh` run (`uc2-time-and-timers-gate-2026-09-03.md` rows d and f,
`uc2-query-drain-progress-2026-10-10.md` row a). Nothing else runs on it.
The tree reaches the host as a git bundle of this branch; each arm builds in
its own worktree and private target dir (`apply_ab.sh` does this).

## 3. Prior reading (smoke)

Dev box, 2026-09-28 (PR #68): −2.8 % head vs base against a 4.2 %
rebuild-control resolution. Decides nothing.
