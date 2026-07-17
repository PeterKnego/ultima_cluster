# UC v2 Lean proofs — Phase 2 spike gate + Tier B go/no-go memo

**Date:** 2026-07-17
**Branch:** `uc2/lean-phase2-spike` (base `main` @ `acef50a`)
**Head commit at gate time:** `66fd70e` (Task S2, election safety) — this doc
lands as Task S3's commit on top.
**Toolchain:** `leanprover/lean4:v4.32.0` / mathlib `v4.32.0` (rev `81a5d25`),
unchanged from Phase 1.
**Gate:** `cd proofs && lake build && ! grep -rn --include='*.lean' -w 'sorry'
Uc2Model Uc2Proofs Conform` — re-run for this doc, `3023 jobs`, zero
`sorry`, `GATE_PASS`.

This is the permanent record for the Phase 2 spike (spec
`docs/superpowers/specs/2026-07-16-uc2-lean-proofs-design.md` §7): a fixed,
small N-node election-safety proof, run specifically to *price* the rest of
Tier B (spec §7's remainder, and the pre-existing "1–3 months" placeholder in
§9's effort table) from real measured costs instead of guesswork. Plan:
`docs/superpowers/plans/2026-07-17-uc2-lean-phase2-spike.md`. Task detail:
`.superpowers/sdd/task-{S1,S2,S3}-report.md`, ledger:
`.superpowers/sdd/progress.md`.

## 1. Result

**Election safety is proved**, sorry-free, over a nondeterministic N-node
model of `uc2_consensus/src/election.rs`'s vote path:

```lean
theorem election_safety {n : Nat} (w : World n) (hw : Reachable w)
    (i j : Fin n)
    (hi : (w.nodes i).role = .leader) (hj : (w.nodes j).role = .leader)
    (ht : (w.nodes i).currentTerm = (w.nodes j).currentTerm) : i = j
```

— `Uc2Proofs/ElectionSafety.lean` (496 lines). The statement is the one FIXED
by the plan (hypotheses exactly `Reachable` + two `.leader` roles + equal
`currentTerm`; the internal 5-clause invariant was discovery work, not
contract). Axiom check: `[propext, Classical.choice, Quot.sound]` — the
standard trio, no project-local escape hatches, no `native_decide`. Both S1
and S2 were **Approved on first review pass** (fable impl + fable review).

**Model shape** (`Uc2Proofs/Protocol.lean`, 273 lines): per-node state
`(currentTerm, votedFor, role, votesReceived, lastTerm, durable)`; an
**8-constructor** `Step` relation — `startElection`, `deliverRequestVote`,
`rejectStaleRequestVote`, `deliverVote`, `deliverVoteHigherTerm`,
`becomeLeader`, `crashRestart`, `havocData` — each cross-referenced by
docstring to its `election.rs` line range; `Reachable` = `ReflTransGen Step`
from `World.init`. Three fault-model encodings were the deliberate cost/power
trade at the center of the spike:

- **Sent-set network**: `sent : List Msg` is append-only and a step may
  process any message in it at any time, never removing it — loss,
  duplication, and reordering fall out for free, no explicit fault
  constructors needed.
- **Havoc data plane**: `lastTerm`/`durable` (the `log_ok` inputs) evolve by
  an unconstrained `havocData` step instead of real log content. This makes
  the *theorem* strictly stronger (safety under arbitrary data evolution ⊇
  safety under UC's actual, constrained data plane) and the *model* much
  smaller — election safety genuinely does not depend on log content, so
  nothing is lost. This is also exactly the simplification that does **not**
  carry to the rest of Tier B (§3 below).
- **Crash-restart** preserves `currentTerm`/`votedFor` (the `StableValue`-
  persisted vote record) and resets `role`/`votesReceived` — the model's
  encoding of the V3 persist-before-send assumption.

**Non-vacuity** (a hard deliverable per the brief, not a nicety): named
theorem `Uc2.nonvacuity_leader_trace : ∃ w : World 3, Reachable w ∧
(w.nodes 0).role = .leader`, an explicit 5-step trace
(`startElection → deliverRequestVote ×2 → deliverVote → becomeLeader`)
built as nested `ReflTransGen.tail` and discharged entirely by `decide`
(kernel reduction — no `native_decide`, axioms `[propext, Quot.sound]`). It
compiled on the first attempt with no enabling-condition weakening. Without
this, "at most one leader per term" would hold **vacuously** over a model
that never reaches `.leader` at all — the classic failure mode for
distributed-safety Lean specs, explicitly guarded against here.

## 2. Measured costs

| Task | Wall-clock | Output | Debug iterations | Review |
|---|---|---|---|---|
| S1 (model + non-vacuity) | **~15 min** (~8 read, ~5 write, ~2 build/gate) | `Protocol.lean`, 273 lines, 8-constructor `Step` | **0** — first `lake build` succeeded, including the trace | Approved, first pass |
| S2 (invariant + `election_safety`) | **~30 min** (~10 read, ~15 write, ~5 build/gate) | `ElectionSafety.lean`, 496 lines, 5-clause `Inv` | 2, both **Lean-mechanics-only** (see below), zero invariant-clause churn | Approved, first pass |
| **S1+S2 combined** | **~45 min** | 769 lines total | — | — |

The 5 invariant clauses (`grant_state`, `grant_uniq`, `self_vote`,
`votes_sound`, `leader_quorum`) were designed **on paper before any Lean was
written**; the two reshapes that made the invariant inductive (a strengthened
`currentTerm = t` conjunct on `grant_state`; a live-tally `leader_quorum`
instead of an existential quorum-certificate clause) were both made at that
design stage, not discovered by a failing proof. Zero clause was added or
reshaped after the first `lake build`.

**What the two debug iterations actually cost was toolchain mechanics, not
proof content** — worth carrying forward as a known-cost line item for
anything that reuses this model: (1) `cases` on `Step` doesn't bind the
constructor's index-unified `World n` field, shifting every case's
hypothesis names by one; (2) `rw [Function.update_self]` doesn't see through
an unreduced `World`-literal projection where `simp only` does; (3)
`induction` over the `Reachable`/`ReflTransGen` definition needs a `have`
recast + `clear` to avoid a polluted induction hypothesis. None of these
touched what was being proved.

**Honest caveat — why this number is not representative of Tier B at
large.** Two structural reasons the spike came in this cheap, both explicit
modeling decisions (§1) rather than proof skill:

1. **Election safety is the *smallest* Tier B theorem.** It is the one
   property in spec §7's list that provably does not depend on log content
   at all — that's *why* `havocData` is a sound stand-in and not a cheat.
   Every other Tier B theorem (§3) needs the data plane to be real.
2. **The model was purpose-built for exactly this theorem.** Frozen leader
   state (a leader's `(role, currentTerm)` never changes while it stays
   leader — verified, not assumed, in S2) and atomic grant+send
   (`deliverRequestVote` updates `votedFor` and emits the `Msg.vote` in one
   step) are both modeling choices that make *this* invariant's preservation
   a frame check in 7 of 8 constructor cases. A model built to also carry
   term-map/byte content has no equivalent "mostly frame checks" luck to draw
   on — see §3.

S2's report also flags that the R4-class risk (Phase 1's actual long pole:
task 11, "the arc's long pole" per the ledger, an informal contract that
turned out **insufficient** and needed three additional fields discovered
under proof pressure — Finding #2 of the Phase 1 gate doc) simply **did not
materialize** here. That is itself evidence about *this* theorem's shape, not
a general property of Tier B proofs — the remainder is explicitly the set of
theorems where that luck is least likely to repeat, because they're the ones
that need real log content.

**Estimate reliability, stated plainly.** Spec §9's own effort table priced
*this spike* — "N-node model + election safety" — at "~1–2 weeks of
sessions." Measured actual: ~45 minutes. That is a **~100×+ overestimate**
against the single most directly comparable prior available (a same-author,
same-domain estimate for this exact pair of theorems, made before either
line of Lean existed). Read plainly, this cuts both ways and neither should
be discounted: estimates in this domain are demonstrably unreliable, and on
the one data point measured so far the unreliability ran entirely in the
cheap direction — but only because the model was purpose-built for the
theorem being priced (the same caveat as above). §3's estimates for (a)–(c)
should be read with that ~100× miss in mind as a live possibility in *either*
direction, not just as a floor.

## 3. Pricing the remainder (spec §7)

Spec §7 lists three remaining Tier B theorems, each strictly building on the
last. None of them can reuse the havoc-data-plane trick — that was §7's own
framing ("Leader completeness... will need the real data plane") and S1's
decision-2 rationale makes the *reason* explicit: the havoc plane is sound
**only because** election safety doesn't depend on log content; leader
completeness and state-machine safety are defined entirely in terms of log
content, so the plane must become real before either can even be *stated*
over this model, let alone proved.

### (a) Log-matching analog — term `t`'s bytes are determined by `t`'s unique leader

**What the model must grow.** This is the single biggest structural change,
and it happens once, then is inherited by (b) and (c):

- Real per-node `term_map : TermMap` / byte-history state, replacing the
  `lastTerm : Nat` / `durable : Nat` havoc pair. `Uc2Model/TermMap.lean` (38
  lines) and `Uc2Model/ByteHistory.lean` (34 lines) already exist as pure
  data types with proved lemmas from Phase 1 — but embedding them *per node,
  under a nondeterministic N-node step relation with a real network* is a
  different exercise from proving properties of one call to a pure function.
- Two new message shapes replacing `havocData`: a data-append message
  (content actually gets written under the current leader's term) and a
  term-map-gossip message (the payload `reconcile` — `Uc2Model/Reconcile.lean`,
  87 lines of pure function — actually consumes).
- An invariant tying "which leader owned term `t`" (already available from
  election safety's machinery) to "what bytes exist at term `t`" — the
  actual log-matching content-identity claim, which the current model has no
  vocabulary to even state.

**Basis for the estimate.** Phase 1's `Reconcile.lean` proof file (802
lines) — proving properties of the **single, pure, already-fully-specified**
`reconcile` function — was Phase 1's long pole: task 11 alone (R4,
`reconcile_validUpTo_eq_firstDivergence`) consumed the largest single-task
budget in the whole 14-task arc, and it *still* needed a genuine contract
strengthening (Finding #2 — three fields the brief's informal `DataStamped`
sketch omitted, each pinned necessary by a hand-verified countermodel) before
it went through. Log-matching has to redo the *reasoning content* of that
proof — data-stamped, no-shadowed-phantom content identity — but now
**inside** the distributed step relation, where gossip interacts with
concurrent local `startElection`/`crashRestart`/`becomeLeader` transitions
that S1/S2 built havoc precisely to avoid reasoning about.

**Estimate: 3–7 S2-equivalents, 3–5 wall-clock sessions** (a "session" here
tracks Phase 1's own task cadence — read/design/write/build/review/fix, the
same shape as one `T7`–`T12` task, not the spike's uninterrupted 30
minutes). Reasoning: this is structurally a second `Reconcile`-class proof
(R4-class invariant-discovery risk *does* apply here — it's the same content
identity claim) plus the N-node/network wrapping S1 did once already. No
`havocData`-style shortcut exists for this component; it is the one the spec
explicitly called out as needing pricing.

### (b) Leader completeness — elected leader's durable ≥ global commit

**What the model must grow, beyond (a).** Commit tracking is currently
**absent** from `Protocol.lean` entirely (havoc replaced it along with the
rest of the data plane) — `CommitTracker`/`C3` (`advance_certified`,
quorum-many reported positions at or above the commit value) has to be wired
into the step relation as a real, quorum-driven counter, not a modeled-away
concept. More consequentially: `logOk` currently is **applied but
irrelevant** — S1's decision 4 states this explicitly ("any grant predicate
preserves election safety — that's *why* the havoc data plane is sound").
Leader completeness is exactly the theorem where that stops being true: the
vote-freshness rule (`V2`/`logOk_iff` — the exact lexicographic `(term,
durable)` characterization already proved in Phase 1) becomes **load-
bearing**, and several of S2's clauses (`grant_state`, `grant_uniq`) need to
be re-derived with real `term_map`/`durable` content correlated against
quorum commit state, not just vote *counts*.

**Basis for the estimate.** This removes the exact simplification that made
S1+S2 cheap (§2's caveat #1) — the theorem is defined on the content `havoc`
was free to ignore. It also needs C3 (`Uc2Proofs/Commit.lean`, 345 lines,
already proved as a pure-fold property in Phase 1) re-derived as a property
of the step relation's global commit counter, composed with the now-real
`logOk` gate.

**Estimate: 4–9 S2-equivalents, 4–7 wall-clock sessions**, strictly on top of
(a) — cannot start until (a)'s real data plane exists to constrain `logOk`
against.

### (c) State-machine safety — sim's inv4 (no truncation of committed bytes) + inv5 (election above commit) composed

**What the model must grow, beyond (a)+(b).** The reconcile-on-gossip steps
themselves (not just term-map identity — the actual clamp/truncate
transitions: `R2`/`R3a`/`R3b`/`R4` slot in as the semantics of a gossip-
delivery constructor, replacing the current model's total absence of any
truncation step) plus discharging, as formal proof obligations rather than
informally-argued safety notes, the two residuals the Phase 1 gate doc left
open under Finding #3's disposition:
- `start_election`'s vote credentials reading the **pre-prune** term map
  (currently justified only by an informal "safe by quorum intersection"
  argument in `docs/benchmarks/uc2-lean-gate-2026-07-16.md`'s Finding #3
  disposition, not a theorem);
- the `awaiting_reconcile` gate's role in blocking the second phantom-
  creation path (currently a structural argument about the node's intake
  gate, not something this Lean model has any way to represent yet, since
  gating/intake ordering isn't modeled at all in `Protocol.lean`).

**Basis for the estimate.** This is a genuine composition of two large
invariants (per-node non-truncation + election-above-commit) across N nodes
under a real network. Invariant count tends to grow quadratically-ish with
state coupling, and that pattern applies most directly here: each new piece
of correlated state (term map × commit × votedFor × the gate ordering)
multiplies the case analysis in every preservation lemma, the opposite of
S2's "7 of 8 cases are frame checks" luck.

**Estimate: 5–12 S2-equivalents, 4–8 wall-clock sessions**, strictly on top
of (a)+(b).

### Rolled up

| Component | S2-equivalents | Wall-clock sessions | Depends on |
|---|---|---|---|
| (a) log-matching analog | 3–7 | 3–5 | — (first) |
| (b) leader completeness | 4–9 | 4–7 | (a) |
| (c) state-machine safety | 5–12 | 4–8 | (a)+(b) |
| **Total** | **12–28** | **11–20** | |

At Phase 1's own observed cadence (9–12 proof tasks ≈ "1–2 weeks of
sessions", per spec §9), 11–20 sessions of this kind is **~2–4 weeks of
session time even on a clean run**, and any single component hitting an
R4-class stuck proof (a genuine possibility — the spec's own risk register
item #2 calls a stuck proof "a finding... not a failure," meaning schedule
risk is *intrinsic* to this exercise, not a discipline gap) pushes the whole
tail toward spec §9's pre-existing "1–3 months" placeholder for
"leader completeness + state-machine safety," which — note — **didn't even
itemize (a) as an explicit prerequisite**. If anything this memo's estimate,
now that (a) is priced separately, suggests that placeholder was optimistic
rather than pessimistic.

As a sanity band: the controller's dispatch prompt for this memo suggested
treating the remainder as plausibly **10–30× the spike**, as a way of
flagging "days-to-weeks, not another 30 minutes." Our independently-derived
12–28 S2-equivalents lands inside that band — but read it as a multiple of
*task count and invariant-discovery exposure*, not of the spike's 45 minutes
of wall-clock (a literal 10–30× of 45 minutes is only 7.5–22.5 hours, which
this memo's own session-cadence math above already exceeds). S1/S2's
near-zero cost came from a model purpose-built to make the smallest Tier B
theorem's preservation lemmas mostly frame checks (§2's caveat). None of
(a)/(b)/(c) gets that luck — each is the same *kind* of proof as Phase 1's
actual long pole (`Reconcile`/R4), not the spike's. Read this as
**days-to-weeks of session time, not "another 30 minutes,"** with real
variance driven by whether any component's informal contract turns out
insufficient the way `DataStamped` did in Phase 1 — and by the ~100×
estimate miss noted in §2, which could run in either direction here too.

## 4. Recommendation: GO (phased), re-gate before (b) and (c)

**GO for (a) log-matching only, as a single time-boxed sub-spike (recommend
capping at 5 sessions — the top of its own estimated range) with a mandatory
re-gate before committing to (b) or (c).** This is functionally GO-LATER for
the expensive back half of Tier B, structured the same way Phase 1.5's
Aeneas attempt was time-boxed with a defined exit clause rather than open-
ended.

**Phased theorem order if GO: (a) → (b) → (c)**, forced by the dependency
chain in §3, not a preference — (a) is the vocabulary (`term_map`/byte
content/gossip messages) that (b) and (c) both consume; the gate doc's
C5/`quorum_intersect` precedent (Phase 1's own note: "Tier B foundation...
needed as its base lemma") is the same shape of investment, already
amortized. There is no version of (b) or (c) that skips (a).

**Why phased rather than a single GO or NO-GO on the whole tail:**

- **The track record for real-bug yield is strong and should not be
  ignored**: Phase 1 surfaced three real findings under proof pressure
  (Finding #1 — R3a false as originally spec'd; Finding #2 — the
  `DataStamped` contract insufficient; Finding #3 — a real, fixed Rust bug,
  the crash-rewin shadowed-phantom hazard) purely from the discipline of
  making informal contracts precise enough to push through a kernel checker.
  Tier B's marginal value over the sim/elle/lincheck stack that already
  covers these properties empirically is exactly this: **the mechanized
  guarantee, plus the Finding-#3-class spec gaps that proof pressure keeps
  surfacing and that fuzz/simulation testing did not catch on its own.**
  That argues against NO-GO outright.
- **But the cost is real and front-loaded onto exactly the components with
  the least favorable cost profile measured so far** (§2's caveat,
  §3's per-component reasoning) — an unconditioned GO on the full 11–20-
  session tail commits weeks before any checkpoint confirms the model
  scaffolding actually transfers past the smallest theorem. That argues
  against a blanket GO.

**Trigger conditions:**

- **Continue to (b)** if (a) lands within its time-box with the invariant
  clauses designed on paper (S2's pattern) and little-to-no clause churn
  after first compile — that would be direct evidence the model-growth cost
  was priced correctly and the R4-class risk stayed dormant a second time.
- **Stop and re-price before (b)** if (a) either blows through its 5-session
  cap materially, or surfaces an R4-class stuck proof / insufficient
  informal contract (itself a valuable Finding, per the spec's own risk
  register — record it either way, but do not treat the schedule slip as
  "almost done, push through"; re-estimate (b)/(c) upward from the actual
  cost, the same way this memo re-priced §9's placeholder from S1/S2's
  numbers).
- **NO-GO is not recommended outright** given the 3-for-3 real-finding yield
  in Phase 1, but is defensible on pure opportunity-cost grounds if the 2–4+
  weeks of session time is needed elsewhere (memory records "leader leases,
  wire-crypto" as the standing next-priority alternatives) — that is a
  resourcing call for the user, not a technical one this memo can settle.
- What the user should weigh either way: the sim (`uc2_sim` invariants,
  including inv4/inv5 directly), the elle consistency harness, and the WGL
  lincheck capstones **already cover** log-matching/leader-completeness/
  state-machine-safety *empirically*, under real fault injection, today.
  Tier B does not add coverage of a previously-untested property; it adds a
  mechanized, exhaustive (not sampled) guarantee over the model, plus the
  proof-pressure side effect of surfacing spec gaps like Finding #3 — a real
  benefit, but one that has to be weighed against several weeks of session
  time against a codebase where the properties in question are already
  under active empirical test.
