# UC v2 Lean proofs gate — Phase 1 (kernels)

**Date:** 2026-07-16
**Branch:** `uc2/lean-proofs` (base `main` @ `e23b1e2`)
**Head commit at gate time:** `0cc3899` (Task 13, conformance rig) — this doc
lands as Task 14's commit on top.
**Toolchain:** `leanprover/lean4:v4.32.0` (`proofs/lean-toolchain`)
**Mathlib pin:** `v4.32.0` (rev `81a5d25`, `proofs/lakefile.toml`)
**Build:** `lake build` — 3021 jobs, zero warnings, zero `sorry`.

This is the permanent record for Phase 1 of the Lean proofs arc: a
mathlib-free `Uc2Model/` mirroring `uc2_consensus`'s three pure-sync safety
kernels (`CommitTracker`, `reconcile`, vote-freshness/`log_ok`), 14 sorry-free
theorems over that model in `Uc2Proofs/`, and an executable conformance rig
(`Conform/`) that replays real Rust output through the model and diffs it bit
for bit. Plan: `docs/superpowers/plans/2026-07-16-uc2-lean-proofs.md`. Spec:
`docs/superpowers/specs/2026-07-16-uc2-lean-proofs-design.md`. Task-by-task
detail: `.superpowers/sdd/progress.md` + `.superpowers/sdd/task-{1..13}-report.md`.

## Theorems proved (all sorry-free)

All 14 theorems pass the axiom check (`#print axioms`) with only the standard
Lean/mathlib trio (`propext`, `Classical.choice`/`Quot.sound` where `Finset`
cardinality reasoning pulls them in) — no `sorry`, no `native_decide`, no
project-local axiom escape hatches.

| # | Theorem | File | Sim-invariant mapping |
|---|---|---|---|
| C1 | `commit_mono_step` / `commit_mono_run` | `Uc2Proofs/Commit.lean` | commit is non-decreasing across any single event or run — a structural precondition every other commit invariant leans on |
| C2 | `advance_le_own` | `Uc2Proofs/Commit.lean` | commit never certifies past the caller's own reported durable position |
| C3 | `advance_certified` (step) / `commit_certified_run` (run, **strengthened** — see Findings) | `Uc2Proofs/Commit.lean` | **↔ inv7's quorum half**: every commit value was backed by quorum-many reported positions at or above it at the step it was set |
| C4 | `reset_then_advance_none` (immediate) / `reports_only_from_onDurable` (provenance, **restated** — see Findings) | `Uc2Proofs/Commit.lean` | a reset genuinely clears certifying power; every post-reset reported value is traceable to a real `on_durable` call |
| C5 | `quorum_intersect` | `Uc2Proofs/Quorum.lean` | any two majority (`n/2+1`) subsets of a fixed cluster share a member — the Raft quorum-overlap axiom, as a Lean `Finset` fact over `Fin n` |
| R1 | `reconcile_validUpTo_le` | `Uc2Proofs/Reconcile.lean` | reconcile's output `validUpTo` never exceeds the caller's durable frontier `d` |
| R2 | `reconcile_preserves_shared_prefix` | `Uc2Proofs/Reconcile.lean` | **↔ inv4's local form**: bytes both sides already agree on below `d` stay valid after reconcile |
| R3a | `reconcile_cuts_own_conflict` (fixed with `hne : leader ≠ []` — see Findings #1) | `Uc2Proofs/Reconcile.lean` | our own first beyond-common-prefix entry is never certified valid |
| R3b | `reconcile_cuts_leader_uncovered` | `Uc2Proofs/Reconcile.lean` | a leader term certified below `d` that our map doesn't cover is proven divergence, always cut |
| R4 | `reconcile_validUpTo_eq_firstDivergence` | `Uc2Proofs/Reconcile.lean` | **↔ inv4's local form (exactness half)**: under a strengthened data-stamped contract (see Findings #2), `validUpTo` is exactly the first byte-content divergence, not merely a sound lower/upper bound |
| R5 | `reconcile_idempotent` | `Uc2Proofs/Reconcile.lean` | re-reconciling reconcile's own output against the same leader map is a fixed point |
| R6 | `noCommonPrefix_iff` | `Uc2Proofs/Reconcile.lean` | exact characterization of when reconcile reports `NoCommonPrefix` (wipe-and-rejoin) vs `Ok` |
| V1 | `vote_unique_per_term` | `Uc2Proofs/Vote.lean` | **↔ inv5's seed**: a voter cannot grant two different candidates in the same term (proved via the stronger `c1 = c2 ∨ term-advanced`, per the brief's granted latitude) |
| V2 | `grant_implies_logOk` + `logOk_iff` | `Uc2Proofs/Vote.lean` | a granted vote implies the candidate's log passed the freshness check; `logOk_iff` gives the exact `(term, durable)` lexicographic characterization |
| V3 | runtime-assumption docstring (not a theorem) | `Uc2Proofs/Vote.lean` | records the external assumption `log_ok`'s freshness check relies on (data-stamped log positions), transcribed verbatim from the brief |

C5's `quorum_intersect` is also the **Tier B foundation**: any inductive
whole-cluster-run safety proof (Phase 1.5+) needs quorum overlap as its base
lemma, which is now a standalone, dependency-light Lean fact rather than an
assumption.

## Conformance

`uc2_consensus/examples/conform_gen.rs` (zero-dep — the crate's
`[dependencies]` stayed empty) drives real `CommitTracker::advance`/
`on_durable`/`reset_reports`, `reconcile::reconcile`, and
`election::log_ok_order` under a seeded splitmix64 PRNG, emitting
`{fn, ..., expect}` JSONL vectors where `expect` is the implementation's own
output. `proofs/Conform/Main.lean` (`lake exe conform`) re-derives each
vector's outcome from `Uc2Model` and diffs it against `expect`, exiting 0 (all
agree), 1 (divergence, offending line printed), or 2 (malformed/usage) — all
three branches live-verified (task-13-report.md).

- **Implementer run:** 100,000 vectors, seed `20260716` (the ISO date, per
  the brief's daily-rotation convention) — **zero divergence**, `real 0m1.376s`
  replay.
- **Independent reviewer re-run:** 20,000 vectors, seed `424242` — **zero
  divergence**.
- **Vector distribution** (from the 100k run, confirmed diverse rather than
  degenerate): ~8% `NoCommonPrefix` outcomes, ~4,000 vectors exercising a real
  truncation (`validUpTo` strictly below at least one side's max base), ~2,400
  full-tie `log_ok` calls (`our_term == cand_term`, decided by durable
  comparison), ~20% tracker-reset events.
- **No model corrections were needed during T13** — the conformance rig
  exercises three kernels the theorem layer (T7-T12) had already independently
  proved sound against the model; the zero-divergence result is a second,
  executable line of evidence, not a bug hunt that found anything.

Nightly `lean-proofs` job (`.github/workflows/nightly.yml`) reruns this daily
with `--seed "$(date +%Y%m%d)"`, rotating coverage while staying reproducible
from the run log; local reruns use `$HOME/.cache/uc2-conform/` (never `/tmp` —
see the box rule in `CLAUDE.md`).

## Findings

The value ledger. Two forced strengthenings of the informal contract, one
statement that was outright false and got a principled fix, and one candidate
real gap in the Rust that the proof work surfaced but did not itself resolve.

1. **R3a as originally spec'd was FALSE.** `reconcile_cuts_own_conflict`
   without qualification claims our first beyond-common-prefix entry is never
   certified valid — but the empty-leader early return (`reconcile.rs`'s
   "a leader with no map tells us nothing — clean as-is" branch) never clamps
   at all. Countermodel (task-10-report.md): `own = [(1,0),(2,4096)]`,
   `d = 8000`, `leader = []` — `validUpTo = d = 8000 > 4096`, violating the
   unqualified claim; reviewer-verified by `#eval`. **Fix:** added
   `hne : leader ≠ []` (the same exclusion `R4` already needed, so no new
   precedent), landed as a follow-up commit (`232666b`) after controller
   sanction — the statement is unweakened for the only regime `reconcile`
   actually clamps in.

2. **The informal `DataStamped` contract was INSUFFICIENT for R4.** Proving
   `reconcile_validUpTo_eq_firstDivergence` (task-11-report.md) required three
   additional fields on top of the brief's five, each pinned as strictly
   necessary by a hand-verified countermodel (`#guard`, violating only the
   named field with all others held true):
   - `own_stamped` / `leader_stamped` — no retained *shadowed phantom* below
     the durable frontier (an entry whose base is below `d` must genuinely
     stamp that byte, not merely be consistent with `termAt`). Without this,
     `encodes` can't see a shadowed entry but `reconcile`'s entry-wise
     comparison can, and the theorem is false (countermodels: own
     `[(1,0),(2,500),(5,500)]` @ `d=1000` vs leader `[(1,0),(5,500)]` — cuts
     at 500 but both histories genuinely agree to 1000; and the
     `leader_stamped` case below).
   - `leader_pos` — leader map terms are `≥ 1` (protocol-true: `election.rs`'s
     construction sites never push term 0).
   - `shared_base` (the brief's original fifth field) turned out **unused** by
     the R4 proof itself — retained in the structure for shape parity with the
     brief, but the theorem's exactness rests entirely on the no-shadowed-
     phantom fields above, not on `shared_base`.

3. **CANDIDATE REAL RUST GAP (open — user decision pending, this is the STOP
   point for Phase 1.5).** The `leader_stamped` countermodel above is not just
   a formal artifact — it is protocol-shaped (task-11-report.md, §"LOUD
   FINDING"): a crashed ex-leader that re-wins an election ships its own
   shadowed phantom pair `(t, D), (t+1, D)` in its term map. Nothing prunes it:
   `become_leader` pushes the new term entry unconditionally
   (`uc2_consensus/src/election.rs:1014`), leaders never reconcile their own map
   (reconciliation is a follower-side operation), and recovery's
   **`rederive_term_map`** (`uc2_node/src/node.rs:3020-3045`) is append-only —
   it does not detect or drop an equal-base shadowed entry either (the
   reviewer independently re-verified this during task-11's review, closing a
   hole the implementer's own writeup had left open). Consequence: followers
   durably past `D` hit a truncate/refill loop on every subsequent gossip from
   that leader — content-identical refill (the bytes don't actually change),
   so this is an **exactness/liveness hazard, not a divergence** as things
   stand; if the cut ever removed already-committed bytes it would trip the
   sim's `inv4`. Two disposition options identified, neither implemented here:
   (a) a Tier B (Phase 1.5+) unreachability proof that this shadowed-phantom
   state is never actually constructible end-to-end under the full protocol,
   or (b) a same-base prune at `become_leader` and/or in `rederive_term_map`
   directly. **This finding is why Phase 1.5 is user-gated rather than
   auto-started from this gate.**

### Restatements (recorded for completeness, not spec gaps)

- `commit_certified_run` (C3, run form) was **strengthened**, not weakened: an
  extra at-step certification conjunct was added so "quorum-many members sat
  at or above the final commit value at the step it was set" is part of the
  theorem itself rather than left to composition (task-8-report.md).
- `reports_only_from_onDurable` (C4, provenance) was **fixed** from the
  brief's malformed disjunction (`… ≤ d ∨ ∃ d, report i d ∈ evs`, whose second
  disjunct made the strong half trivially escapable) to the properly strong
  conjunctive form; the brief's `hi : i < t.reported.length` hypothesis was
  also dropped as unnecessary (task-8-report.md).
- V1's brief-literal dot-notation signature
  (`(handleRequestVote s c1 t1 d1).1.handleRequestVote c2 t2 d2`) does not
  elaborate in this codebase (`handleRequestVote` lives in namespace `Uc2`,
  not `Uc2.VoterState`) — rewritten as the definitionally identical plain
  application. Syntactic only, no change in meaning (task-12-report.md).

## Phase 1.5 status

**Gated, not started.** Requires explicit user go-ahead per the plan and per
Finding #3 above. Scope (per spec `docs/superpowers/specs/2026-07-16-uc2-lean-proofs-design.md`
§7 when it lands): Aeneas translation of the Rust kernels into Lean and an
equivalence proof (`proofs/Aeneas/Equiv.lean` + `proofs/Aeneas/SOURCES.sha256`)
against `Uc2Model`, closing the gap between "the model is proved safe" and
"the model matches the Rust" beyond conformance testing's sampling guarantee.

## Phase 2 spike

Not started. Pointer: spec `docs/superpowers/specs/2026-07-16-uc2-lean-proofs-design.md`
§7.

---

Phase 1 is complete as of this doc. **Per the Task 14 brief: STOP for user
review before Phase 1.5** — it is gated on the disposition of Finding #3.
