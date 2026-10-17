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

### Re-run results (rows b and c)

**Run 2026-10-17, 10:59–11:28 UTC, a fresh `c6id.2xlarge`** (same shape,
same procedure as §4, destroyed afterwards with state empty and no live
instance or `uc-bench` VPC). Tree: a bundle of `d82fa6df` plus
`bench/33-fix-on-a70539a1`. `--selftest` passed.

| row | N | A | B | head vs base | resolution | worst sem | signal bar | verdict |
|---|---|---:|---:|---:|---:|---:|---:|---|
| b | 1 | `a409b3ae` 15,142,330 | `ecd560a0` 14,821,026 | **−2.122 %** | 0.443 % | 0.649 % | 1.298 % | `outside resolution` |
| b | 2 | 14,768,737 | 14,378,754 | **−2.641 %** | 0.286 % | 0.221 % | 0.442 % | `outside resolution` |
| c | 1 | `0a9bed82` 14,486,119 | `ddf5b470` 14,373,209 | **−0.779 %** | 0.091 % | 0.127 % | 0.253 % | `outside resolution` |
| c | 2 | 14,526,323 | 14,335,019 | **−1.317 %** | 0.149 % | 0.170 % | 0.341 % | `outside resolution` |

`pace_stalls` was 3.5–3.8 × 10⁹ on every arm; no run is driver-bound.
**Row b FAILS at both N** (half row a's loss). **Row c FAILS at both N**: on
this CPU the fix makes `main` slower, where the dev box read it +2.1 %
faster.

### What the instrument is measuring (investigation, not a verdict)

Two more runs on the same host, with `apply_bench` binaries built on the
maintainer's dev box (same `rustc` 1.96, a newer system linker), six or eight
interleaved reps per binary at N=1, deltas against `a409b3ae`:

| commit | dev-box build, default layout | dev-box build, every function and loop aligned to 64 B |
|---|---:|---:|
| `69fb38ab` (cnc words only) | −0.04 % | — |
| `855a0aa6` (attach only) | −2.33 % | — |
| `a8de0e25` (attach only) | −3.13 % | **+0.50 %** |
| `54caa65b` (adds the arm) | −5.70 % | — |
| `a70539a1` (#68 merged) | −3.58 % | **−2.63 %** |
| `0a9bed82` (`main`) | −9.60 % | **−8.52 %** |
| `main` + fix | −2.76 % | **−1.65 %** |

Three readings follow, and the third decides what rows a–c can claim:

1. **Commits that never touch the apply loop move it by 2–3 %** under the
   default layout (`855a0aa6`, `a8de0e25` change attach, which runs once),
   and the move disappears under forced alignment. Part of every delta in
   this doc is where the hot loop happens to land.
2. **Some of #33's cost survives forced alignment:** `a70539a1` at −2.6 %.
3. **The same source reads differently from different builders.** Host-built
   `main` ran 14.49 M frames/s in row c; the dev-box build of the same
   commit ran 13.46 M on the same host type, 7 % slower. Within the dev-box
   set the fix is worth +7.5 % over `main`; within the host-built set it
   costs 0.8 %. Forced 64-byte alignment does not remove this, so what
   matters on this CPU is not (only) a loop's alignment but the relative
   placement of the hot code, which the linker decides.

`apply_ab.sh`'s B′ control cannot see this: two builds of one source by one
builder get one layout, so the "resolution" it reports is build
non-determinism, not layout sensitivity. On this rig a few-percent apply-hop
delta between two sources is not attributable to the source change until
layout is varied. The method that answers it is to measure each source under
several randomised layouts (for example the linker's section shuffle with
different seeds) and compare the distributions.

Logs: `$HOME/scratch/ab33-fleet2/` on the maintainer's box.

## 6. Layout-randomised re-measurement (method fixed before the run)

Written before any number exists. Rows a–c keep their bars and their FAIL
verdicts; this section measures what those rows could not separate, the
source's cost from the layout's luck.

**Arms.** Four sources: `a409b3ae` (base), `a70539a1` (#68 merged),
`0a9bed82` (`main`), and `main` + fix (`ddf5b470`, the `bench/33-apply-ab`
head). Each is linked under **ten layouts**: the same object code, with
`rust-lld`'s `--shuffle-sections='.text*=<seed>'` for seeds 1–10 (a linker
wrapper adds the flag, so one source's ten binaries differ only in where
the linker placed each function). All forty binaries are built on one
machine with the pinned `rustc` 1.96.

**Run.** One c6id.2xlarge, the interleaved runner (every rep runs all forty
binaries once, in a rotated and alternately reversed order), `--mode bounded
--secs 6 --warmup-secs 1`, three reps per binary, at N=1 and at N=2. The
unit is one binary's mean over its three reps; each arm is ten such units.

**Statistic.** For a pair of arms, the delta is the difference of the two
arms' means of seed-means, as a percentage of the first. Its 95 % interval
is Welch's, from the between-seed spread (which contains both layout luck
and run noise, so it is the honest error for a claim about source).

**Readings, decided now:**

1. **#33's cost** is `a70539a1` vs `a409b3ae`. An interval wholly below zero
   says #33 costs the apply hop; one that spans zero says this rig cannot
   distinguish its cost from layout, at this sample size.
2. **What `main` adds** is `main` vs `a70539a1`, read the same way.
3. **The fix** is `main` + fix vs `main`. It ships only if the interval lies
   wholly above zero at N=1 and does not lie wholly below zero at N=2.
   Otherwise it is dropped from 2.14.0 and stays on its branch.
4. **The layout spread** (each arm's between-seed max − min, and its
   standard deviation) is reported as the rig's layout sensitivity, the
   number a future `apply_ab.sh` layout arm would be sized from.

### 6.1 Bisect of what `main` adds (method fixed before the run)

The §6 run found −4.2 % between `a70539a1` and `main` at both N. The
first-parent range holds 90 merges; ten touch the apply path. The bisect
measures the tree at each of those ten merges plus the two endpoints, so the
twelve points partition the whole range: a cost anywhere in it, including in
merges that touch no apply file, lands in exactly one step.

Points, in order: `a70539a1`, `baae5ae0` (#70), `e4bb9f65` (#71), `d7ab8d1a`
(#75, changes `apply_bench` itself), `19b5788d` (#80), `0d773273` (#81),
`b7ebcc55` (#83), `5d8cdbc1` (#87), `dffe5f43` (#88), `9044e522` (#90),
`c240a896` (#97), `0a9bed82` (`main`). Five shuffle seeds each (1–5; the two
endpoints reuse their §6 binaries), N=1 only, three interleaved reps, same
host shape and runner. A step is **attributed** when its adjacent-point
delta's Welch 95 % interval lies wholly below zero; a step whose interval
spans zero is not attributed, however large its point estimate.

### 6.4 Counter readout across #81's step (method fixed before the run)

#81's step (`19b5788d` → `0d773273`, −3.4 %) is the largest and changes
`apply_cycle` by 19 bytes, so its cost is in how the same work executes, or
in work the size does not show. This session reads hardware counters on the
`uc2-apply` thread alone (`perf stat -t`, a 4 s window inside the measured
window, after warmup), for the five shuffle seeds of each side, two runs per
binary, interleaved. Every count is divided by the frames the run applied
in that window (its reported rate × the window), giving per-frame:
instructions, cycles, branch misses, and whichever front-end, back-end and
memory-ordering counters the instance's PMU exposes (the list is recorded
with the results). The difference of the two sides' means gets a Welch
95 % interval over the ten runs each.

Readings, decided now:

1. **Instructions per frame differ outside the interval:** the step is
   more work per frame, and `perf annotate` of one seed per side names
   the instructions.
2. **Instructions per frame do not differ, cycles do:** the step is the
   same work executed worse. The counter whose per-frame difference covers
   the largest share of the cycle difference is named the leading
   candidate. It is called the cause only if the annotation shows the hot
   cycles moving to instructions of that kind.
3. **Neither reading holds:** the step is recorded as unexplained by the
   counters this PMU exposes.

**Amendment (before any §6.4 number exists).** The c6id.2xlarge exposes no
hardware PMU (`dmesg`: "unsupported CPU family 6 model 106 no PMU driver,
software events only"); AWS passes the counters through only on
full-socket sizes, which for this CPU exceed the bench account's vCPU
quota. So §6.4 cannot run as written, and two substitutes replace it:

- **Reading 1 (more work?) on the dev box.** Instructions and branches
  retired per applied frame, from the AMD PMU on the `uc2-apply` thread,
  same ten binaries, same window, same statistic. Instruction count is a
  property of the binary and the path taken, not of the core, so it
  transfers to the Intel host (glibc's CPU-dispatched routines aside, which
  the apply loop does not call per frame). Cycles there do not transfer and
  are not read.
- **Where the time goes, on the Intel host.** A `cpu-clock` timer-sampled
  profile (a software event) of the `uc2-apply` thread, three seeds per
  side, 4 s each, attributed by instruction inside `apply_cycle`. A step is
  explained only if the samples move to a specific instruction group (a
  load, a store forward, a branch, a spill reload) on the slow side. Timer
  samples skid, so a group is read as a cluster of adjacent instructions,
  never a single one.
