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

## 4. Results

**Run 2026-10-17, 10:05–10:18 UTC, one `c6id.2xlarge`** (Intel Xeon Platinum
8375C, 8 vCPU, kernel 7.0.0-1014-aws), us-east-1, provisioned for this run
alone and destroyed after it: terraform state empty and an EC2
`describe_instances` showing no live instance before the apply and after the
destroy. The tree arrived as a git bundle of this branch at `a10334ae` (the
bar commit). `--selftest` passed first. Both N ran with `--reuse-targets`, so
N=2 reused N=1's three arm builds; that changes nothing measured.
`min_rate` applied frames/s, 12 runs per arm:

| N | A `a409b3ae` | B `a70539a1` | B′ head again | head vs base | resolution | worst sem | signal bar | verdict |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 15,201,992 | 14,512,841 | 14,536,213 | **−4.533 %** | 0.161 % | 0.167 % | 0.333 % | `outside resolution` |
| 2 | 15,123,163 | 14,053,522 | 14,057,853 | **−7.073 %** | 0.031 % | 0.573 % | 1.147 % | `outside resolution` |

`pace_stalls` summed to 3.6–3.8 × 10⁹ on every arm, so neither run is
driver-bound and both verdicts stand. Load average was 2.2 during both runs:
that is `apply_bench`'s own busy threads, and no other cargo or rustc process
ran.

### Verdict

| row | measured | bar | verdict |
|---|---|---|---|
| a, N=1 | −4.53 %, outside a 0.333 % signal bar | not `outside resolution` with a negative delta | **FAIL** |
| a, N=2 | −7.07 %, outside a 1.147 % signal bar | same | **FAIL** |

The bar is missed at both N and is kept. #33 costs the apply hop 4.5 % at one
FSM and 7.1 % at two on this rig. The dev-box smoke's −2.8 % (§3) was the
same sign, inside a 4.2 % resolution that could not see it.

### First look at the cause (not a diagnosis)

`apply_cycle` is slightly smaller at head (0x537a bytes against 0x5408 at
base, in the binaries `apply_ab.sh` kept). `version_gate::on_cluster_frame`
is a separate out-of-line function, and `stop_at_record` is a new call site
in the loop. A normalised diff of the loop's direct call targets shows no new
per-frame callee, but the count of GOT-indirect calls moves from 101 to 103,
and those have not yet been resolved with `readelf -rW`. Where the cost
comes from is open. The method is CLAUDE.md's 2.11.0 apply-hop lesson: compare
the per-frame path's call list and layout arm against arm, and A/B a fix
candidate on the same rig.

Raw logs, `runs.tsv` and the three binaries for each N are kept off-tree at
`$HOME/scratch/ab33-fleet/` on the maintainer's box.

## 5. The fix, and its re-run

**Cause (dev box, idle, `apply_bench` N=1, six interleaved reps per commit
over nine commits of #68).** Against `a409b3ae`: every commit up to
`a8de0e25` reads +0.8 to +1.0 %; `54caa65b`, the commit that adds the
`CLUSTER` arm to the per-frame loop, reads −2.0 %, and the later #33 commits
stay between −2.7 % and −3.7 %. `apply_bench` appends only `MESSAGE` frames,
so the arm never runs: the MESSAGE path pays for the arm's inline code (the
verdict match, the guard-consuming `stop_at_record` call and the `Pending`
break), the M14a codegen effect.

**Fix (`ddf5b470` on `main`; the same change on `a70539a1` is `ecd560a0`).**
The arm is the type test and one `#[cold] #[inline(never)]` call,
`version_gate::ends_batch`, which leaves the record's start and verdict in a
local and returns whether the batch ends; the loop's exit acts on it.
Behaviour is unchanged. Dev-box smoke, N=1, eight interleaved reps against
`a409b3ae`: `main` −1.44 %, the fix on `main` +0.63 %.

**Rows, pre-committed before the re-run** (same rig shape, same script, same
verdict rule as §2):

- **Row b — #33 as fixed.** `scripts/apply_ab.sh a409b3ae ecd560a0 --pairs 12`
  at `--fsms 1` and `--fsms 2`. Bar: §2's — not `outside resolution` with a
  negative delta. This is the row #68 owed, re-asked of the fixed change.
- **Row c — what ships.** `scripts/apply_ab.sh 0a9bed82 ddf5b470 --pairs 12`
  (`main` against `main` plus the fix) at `--fsms 1` and `--fsms 2`. Bar: the
  fix is not `outside resolution` with a negative delta (it must not make
  `main` slower). Its size is reported.

Row a's FAIL stands as recorded; rows b and c do not replace it.
