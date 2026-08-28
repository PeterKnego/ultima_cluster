# uc2 M14c — the client hop in isolation (`hop_bench engine-load`, dev-box smoke)

**Date:** 2026-08-28. **Trees:** `main` 3a7f9a5 (the M14a tip, extracted read-only
with `git archive` into `/home/claude/m14c-a0-tree`) · `main` 4347bc2 (the M14b
tip — docs-only over `ce85fea`, `git diff --name-only ce85fea 4347bc2` lists
zero `.rs` files) · `worktree-uc2-multi-service` f47fe5f (M14c Task 1, the
single-ring `fetch_or` fast path). **Box:** the 32-core dev box; a private
`CARGO_TARGET_DIR` per tree. **Runner:** `scripts/hop1_ab.sh` (new, committed
with this document).

Binaries, all copied out of their target dirs before any run:

| name | tree | sha256 (12) |
|---|---|---|
| `hb-m14a` | `main` 3a7f9a5 | `5a7d48e2de1e` |
| `hb-main` | `main` 4347bc2 | `90e77c5982cd` |
| `hb-t1` | branch f47fe5f | `bef27ebc0571` |
| `hop_bench.main` | prior session's build of 3a7f9a5 | `022e7c3fbdb1` |
| `hop_bench.branch` | prior session's build of ce85fea | `dfb8e7529b8d` |

> **Smoke, not a gate.** Dev-box numbers are never compared to a bar
> (`docs/notes/dev-box-not-a-bench.md`; CLAUDE.md "Benchmarking discipline").
> What this document records is *ratios* from one hop measured alone — the
> divide-and-conquer method of CLAUDE.md "Finding a performance bottleneck".
> **This document sets no bar and moves none.** M14d's fleet gate is the
> adjudicator for the client hop.

## What the harness isolates

`hop_bench engine-load` → `hop_bench dummy-node` over a real instance dir: the
cnc page and the rings, no log buffer, no consensus, no service. The measured
chain is client `Engine` → ingress MPSC ring → node stand-in → egress
broadcast → `Engine`. One engine (one sender thread, one poll thread),
`--wait yield`, inflight 4096, payload 64 B, 6 s per run, a **fresh sink
process per run**.

Only the **driver** differs between the two sides of an A/B: `--sink` is one
fixed binary used for every single run, so a sink-side codegen difference can
never leak into the delta. Reps alternate order (odd reps A→B, even reps
B→A) so warm-up or thermal drift cannot favour a side.

`uc2_gateway/examples/hop_bench/engine_load.rs` differs by exactly four net
lines between 3a7f9a5 and 4347bc2 (`git diff 3a7f9a5 HEAD -- …/engine_load.rs`):
the mandatory `Outcome::Responses(_)` and `Outcome::BadService { .. }` arms
added to an exhaustive match on a path a bench never takes. The harness is
otherwise byte-identical, so `hb-m14a` vs `hb-main` is a `uc2_client`
comparison.

## Results

Every row is `--reps 6` (12 runs) through `scripts/hop1_ab.sh` unless the
"runner" column says otherwise; `mean [min–max]` in resp/s; "ranges" is the
runner's own disjointness verdict on the two min–max bands.

| id | A | B | sink | runner | A mean [min–max] | B mean [min–max] | Δ | ranges | p90 A / B |
|---|---|---|---|---|---|---|---|---|---|
| R0 | `hb-m14a` | `hb-main` | `hb-main` | settled | 5 751 317 [5 565 812–5 857 821] | 5 733 784 [5 643 433–5 824 722] | **−0.30 %** | OVERLAP | 2 µs / 3 µs |
| R0b | `hb-m14a` | `hb-main` | `hb-main` | unsettled | 5 736 493 [5 532 881–5 817 626] | 5 754 230 [5 649 579–5 793 577] | **+0.31 %** | OVERLAP | 2 µs / 3 µs |
| R0f | `hb-m14a` | `hb-main` | `hb-m14a` | unsettled | 5 742 282 [5 559 180–5 807 332] | 5 739 130 [5 711 077–5 785 766] | **−0.05 %** | OVERLAP | 2 µs / 3 µs |
| R1 | `hb-main` | `hb-t1` | `hb-main` | settled | 5 709 264 [5 596 020–5 786 592] | 5 706 366 [5 566 875–5 808 009] | **−0.05 %** | OVERLAP | 3 µs / **2 µs** |
| R6 | `hb-m14a` | `hop_bench.main` | `hb-main` | settled | 5 715 441 [5 599 706–5 791 121] | 5 773 587 [5 696 404–5 821 174] | **+1.02 %** | OVERLAP | 2 µs / 2 µs |
| R0c | `hop_bench.main` | `hop_bench.branch` | `hb-main` | unsettled | 5 770 720 [5 545 271–5 828 976] | 5 735 472 [5 646 363–5 803 125] | −0.61 % | OVERLAP | 2 µs / 3 µs |
| R0g | `hop_bench.main` | `hop_bench.branch` | `hop_bench.main` | unsettled | 5 713 976 [5 607 470–5 803 770] | 5 674 506 [5 613 707–5 744 003] | −0.69 % | OVERLAP | 2 µs / 3 µs |
| R0h | `hop_bench.main` | `hop_bench.branch` | `hop_bench.main` | settled | 5 750 506 [5 654 633–5 823 464] | 5 676 945 [5 622 806–5 759 988] | −1.28 % | OVERLAP | 2 µs / 3 µs |
| R0d | `hop_bench.main` | `hop_bench.branch` | `hop_bench.main` | **prior session's script, verbatim** (5 reps, fixed order branch→main) | 5 816 770 [5 591 259–5 961 067] | 5 655 937 [5 575 656–5 719 748] | **−2.76 %** | OVERLAP | 2 µs / 3 µs |
| R0e | `hop_bench.main` | `hop_bench.branch` | `hop_bench.main` | prior script, **order swapped** (main→branch) | 5 893 403 [5 791 977–5 991 680] | 5 722 307 [5 663 219–5 755 313] | **−2.90 %** | disjoint | 2 µs / 3 µs |

Logs: `/home/claude/m14c-ab/r{0,0b,0c,0d,0e,0f,0g,0h,1,6}-*.log`. R0 was also
run once in the unsettled configuration before R0b (**+0.03 %**, OVERLAP);
its log was overwritten by the settled re-run of the same name, so only R0b's
unsettled numbers are tabulated above.

## Findings

1. **The M14b hop-1 rate loss does not reproduce, and the −4.2 % it was
   quoted at is not a property of the M14b client code.** Built fresh for this
   session from the two commits, the M14a tip and the M14b tip measure
   **−0.30 %, +0.31 %, −0.05 %** (R0, R0b, R0f) across two different fixed
   sinks and both runner configurations — three series whose signs disagree
   and whose min–max bands overlap in every case. On this box, with these
   binaries, the difference is not measurable.

2. **Two independent builds of the *same commit* differ by more than the
   effect being hunted.** R6 A/B'd `hb-m14a` against `hop_bench.main` — both
   built from 3a7f9a5, in different directories, with the pinned toolchain —
   and measured **+1.02 %**. That is the instrument's build-to-build
   resolution, and it is larger in magnitude than every matched-provenance
   M14a-vs-M14b delta in this document. It generalises M14a's codegen lesson
   (`docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`: an inline wait ladder
   cost 9 % at N=1 on a path N=1 never executes): at this hop's scale, *where
   the code lands* is worth ~1 %, so a ~1 % A/B between two commits measures
   layout luck, not the source change.

3. **The prior binaries do carry a small, sign-consistent delta — and it is
   the older build pair, not the source.** Rows R0c/R0g/R0h (−0.61 %, −0.69 %,
   −1.28 %, all overlapping) and R0d/R0e (−2.76 %, −2.90 %) all use the prior
   session's two binaries. Their sign never flips, so something in *those two
   builds* is real; but freshly building the identical two commits erases it
   (finding 1), and building the same commit twice manufactures a delta of
   comparable size (finding 2). The residue is the build, not `uc2_client`.

4. **Order was refuted as the confound; the harness's pacing is a second
   one.** The prior session's script ran a fixed `branch → main` order every
   rep. Swapping it (R0e) *kept* the −2.9 %, so a within-pair position effect
   is not the explanation. But the same two binaries and the same sink read
   −2.76 %/−2.90 % under that script and −0.69 %/−1.28 % under
   `scripts/hop1_ab.sh` (R0g/R0h) — a factor of two to four from the runner
   alone. See "The runner's own confound" below.

5. **Task 1's fast path did what it was built to do, and only that.** R1:
   `p90` median **3 µs → 2 µs** — the pre-M14b tail, restored — with the rate
   at −0.05 % and ranges overlapping. The `received.fetch_or` RMW was the
   *tail*; it was never the rate. This is the committed confirmation of the
   M14b-era scratch-build result.

6. **The three hot-body suspects were not measured, deliberately.** The plan's
   Steps 4–6 (v1: `handle_record`'s fan-in arms out of line; v2: `send`'s
   query prefix out of line; v3: `poll`'s single-ring slice pattern) are
   gated on R0 reproducing a negative delta, and it does not. Their decision
   rule requires a **≥ +1 %** disjoint delta over 6 reps; finding 2 shows this
   harness cannot resolve 1 % on this box, so every one of the three would
   have returned OVERLAP by construction. Recording them as "refuted" would
   have been a claim the instrument cannot support. They remain untried
   suspects, and `uc2_client/src/engine.rs` is unchanged by this work.

## The runner's own confound

Two pauses in `scripts/hop1_ab.sh` are measured, not cosmetic: `sleep 0.5`
after the sink prints `READY` (before the driver attaches) and `sleep 1` after
the sink is killed (before the next run's sink starts). Without them, the
driver races the sink's own start-up and every side reads ≈ 5.74 M resp/s
regardless of which binary drives — a ceiling that masks a real driver-side
difference. The same binaries and sink read **−0.69 %** unsettled (R0g) and
**−1.28 %** settled (R0h). The pauses are in the committed script with this
rationale beside them.

This is the M13 lesson in a new place: the first three explanations tried here
— cross-session drift, the fixed run order, the choice of sink binary — were
each "consistent with the symptom" and each refuted by a controlled cell
(R0c/R0g, R0e, R0f respectively). Only the two-builds-of-one-commit control
(R6) explained it.

## What this changes

- **Nothing in `uc2_client`.** No variant was applied; `engine.rs` is exactly
  the Task-1 commit `f47fe5f`. The claim that M14b cost hop 1 ≈ −4.2 % should
  not be carried forward from the M14b addendum without a fleet measurement —
  it is within this box's build-to-build noise.
- **`scripts/hop1_ab.sh` is committed** so the next attempt starts from a
  runner with the alternation, the fixed sink, the checksum echo, the pacing
  pauses and the disjointness verdict already in it, rather than an ad-hoc
  script per session.
- **A ~1 % client-hop question belongs on the fleet, not here.** M13 measured
  the fleet chain as cluster-bound (1.75 M/s into a 3-node cluster vs 2.44 M/s
  against a dummy node), so a few percent of client hop may be masked end to
  end — real per core either way. M14d's fleet gate is where it is adjudicated.
- **If the bisection is resumed**, the prerequisite is an instrument that
  resolves better than 1 %: more reps with a paired (per-rep) statistic rather
  than min–max disjointness, a longer `--secs`, pinned cores, and ideally the
  two sides built into one binary so layout is shared.

## Reproduce

```bash
mkdir -p /home/claude/m14c-ab/bin

# (1) main 4347bc2 — the M14b tip. Supplies BOTH the fixed sink and driver A.
cd /home/claude/ultima/ultima_cluster
CARGO_TARGET_DIR=/home/claude/cargo-target-m14c-main \
  cargo build --release -p uc2_gateway --example hop_bench
cp /home/claude/cargo-target-m14c-main/release/examples/hop_bench /home/claude/m14c-ab/bin/hb-main

# (2) main 3a7f9a5 — M14a's tip, extracted read-only (no worktree, no HEAD move).
mkdir -p /home/claude/m14c-a0-tree
git -C /home/claude/ultima/ultima_cluster archive 3a7f9a5 | tar -x -C /home/claude/m14c-a0-tree
cd /home/claude/m14c-a0-tree
CARGO_TARGET_DIR=/home/claude/cargo-target-m14c-a0 \
  cargo build --release -p uc2_gateway --example hop_bench
cp /home/claude/cargo-target-m14c-a0/release/examples/hop_bench /home/claude/m14c-ab/bin/hb-m14a

# (3) the branch after Task 1.
cd /home/claude/ultima/ultima_cluster/.claude/worktrees/uc2-multi-service
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a \
  cargo build --release -p uc2_gateway --example hop_bench
cp /home/claude/cargo-target-uc2-m14a/release/examples/hop_bench /home/claude/m14c-ab/bin/hb-t1

sha256sum /home/claude/m14c-ab/bin/*

# R0 — the premise check.
scripts/hop1_ab.sh --sink /home/claude/m14c-ab/bin/hb-main \
  --a /home/claude/m14c-ab/bin/hb-m14a --b /home/claude/m14c-ab/bin/hb-main \
  --reps 6 --root /home/claude/m14c-ab
```

Box hygiene for every run above: nothing else on the box — no `cargo build` in
another checkout, no test suite, no second agent session doing work — and the
same conditions for every rep. `--root` must be on real disk; the script
refuses `/tmp` (RAM-backed, no swap).
