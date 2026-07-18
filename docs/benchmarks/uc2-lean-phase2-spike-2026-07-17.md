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
  encoding of the V3 persist-before-send assumption. Conformance caveat
  (S1 review): "preserves `currentTerm` exactly" is not literal Rust —
  `ElectionSm::new` recovers `max(vote.term, term-map last)`, which can
  *regress* below a term the node had merely gossip-adopted pre-crash.
  Harmless for this theorem: the recovered term never drops below the
  node's last *vote* term, so the `(term, id)`-tagged `votedFor` still
  forbids a conflicting re-grant at any granted term, and every
  regressed-node global state is bisimulated by a model trace in which the
  node simply never processed the higher-term traffic (message loss).

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

## Tier B(a) actuals + re-gate

**Date:** 2026-07-17. **Branch:** `uc2/lean-log-matching` (base `main` @
`a551dfc`, this spike merged). Plan:
`docs/superpowers/plans/2026-07-17-uc2-lean-log-matching.md` (LA1 data-plane
model / LA2 invariant + `log_matching` / LA3, this section). Task detail:
`.superpowers/sdd/task-{LA1,LA2,LA3}-report.md`, ledger:
`.superpowers/sdd/progress.md`. The original memo's §4 ("Recommendation: GO
(phased)," above) authorized this sub-spike as a single time-boxed unit
(capped at 5 sessions); it landed **within the box**, and this section
reports the actuals and re-prices the remainder per that recommendation's
mandatory re-gate. (This section has its own §1–§6 below — references to the
memo's original numbered sections above are spelled out as "the original
memo's §N" to avoid collision.)

### 1. Result: log-matching analog proved

**`Uc2.Data.log_matching` is proved**, sorry-free, over the real data plane
(payload history + data-stamped term map), verbatim to the statement §7/the
LA-series brief fixed at plan time:

```lean
theorem log_matching {n : Nat} (w : World n) (hw : Reachable w)
    (i j : Fin n) (p : Nat) (t vi vj : Nat)
    (hi : (w.nodes i).hist p = some (t, vi))
    (hj : (w.nodes j).hist p = some (t, vj)) : vi = vj
```

— `Uc2Proofs/LogMatching.lean`, 1046 lines. Axiom check:
`[propext, Classical.choice, Quot.sound]` — the standard trio, unchanged from
Phase 1/Phase 2. Both LA1 (the data-plane model) and LA2 (the invariant +
theorem) were **Approved on first review pass** (fable impl + fable review,
same reviewer discipline as S1/S2).

**Model growth over the Phase-2 election model** (`Uc2Proofs/ProtocolData.lean`,
511 lines as of this task's gate run — 508 as LA1 left it, +3 from this
task's `deliverReplicate` docstring fix, §3 below; `wc -l`-verified),
layered — `Uc2Model/`, `Protocol.lean`, and `ElectionSafety.lean` all
untouched except one sanctioned election-model extension, below): the
spike's `havocData` stand-in is replaced by a real per-node payload history
(`hist : Nat → Option (Nat × Nat)`, position ↦ `(term-stamp, payload)`) and
data-stamped term map, driven by an **11-constructor** `Step` relation — the
spike's 7 non-`havocData` election mirrors plus `leaderAppend`,
`deliverReplicate`, `shipTermMap`, `deliverTermMap`. The election-safety
guarantee is carried forward by **projection lift**, not re-proof: every data
step simulates as 0–2 election steps (`step_project`), so
`Uc2.Data.election_safety` is a one-line application of the lifted theorem —
Route 1 of the two the LA-series plan sanctioned for keeping `election_safety`
green under model extension. The one election-model extension needed,
`adoptHigherTerm` (message-free higher-term adoption on `Protocol.lean`), was
required because gossip-triggered adoption (`TermMapReceived{term >
current_term}` → `adopt_term`, `election.rs` 656–663) has no election-wire
message to witness once the gossip payload moves to the data wire the
projection erases; `election_safety` was **re-proved green** in the same task
against the extended model (the new `inv_step` case is the
`deliverVoteHigherTerm` argument verbatim), satisfying the plan's
never-weaken-under-extension constraint by the re-prove route as a belt on
top of the projection-lift route.

**The truncation trace** (opening deliverable of LA2, closing an LA1-carried
review item that truncation had never been exercised at `Step` level):
`nonvacuity_truncation_trace`, a 15-step, fully `decide`-discharged 3-node
trace — a leader wins term 1 and appends two positions but only replicates
one; a challenger wins term 2 on honestly-compared credentials and appends a
divergent tail at the same position under a different term; the challenger's
gossip reconciles the first leader down to `validUpTo = 1`, erasing its stale
tail (`hist 1 = none`, durable 2→1); a follow-on replication re-converges it
onto the challenger's byte. Every side condition and every pre/post `hist`
fact is pinned by kernel `decide`; it built on the first attempt.

**`election_safety` is green at both levels** after this sub-spike: the
original `Uc2.election_safety` (over the pure election model, now carrying
the `adoptHigherTerm` extension) and the lifted `Uc2.Data.election_safety`
(over the full data-plane model) both re-verified by the gate run above.

### 2. Findings this sub-spike

The value ledger continues — two more entries, both about the precise shape
of the `stamp ≤ currentTerm` guard LA1 introduced to keep `log_matching`
true, and both sharpen (not weaken) the spike's own scope-collapse notes
rather than undermining the theorem.

**(i) Finding #4 — the settled design alone made LM-core false.** Before LA1
added any guard, the data plane as sketched in the plan's "settled design"
section admits a genuine countermodel: a deposed-but-uninformed leader
accepts a higher-term byte at its own frontier while its `role` is still
`.leader` at its stale term, gets truncated by its *own* stale gossip, and
re-appends a different payload under a `(position, term)` pair already
shipped — `log_matching`'s conclusion (`vi = vj`) would be false at that
position/term. The fix — `stamp ≤ currentTerm` on `deliverReplicate` — is a
faithful, conservative model of two jointly-operating Rust mechanisms:
`uc2_net/src/receiver.rs`'s intake drops a DATA datagram whose header term
doesn't match the node's currently-adopted term (adoption itself happens only
via consensus datagrams — vote grants, term-map gossip — never via DATA), and
the `awaiting_reconcile` intake gate additionally forces reconcile-before-data
on new-term adoption. The model keeps only the weaker `≤` **consequence** of
those two guards (accept if not-behind, rather than exact-match-or-drop),
which is why the model is strictly *more* permissive than Rust here — a
conservative over-approximation, never a shortcut that could hide a real
divergence LM-core would otherwise catch.

**(ii) The prefix-form is FALSE in the over-approximated model — and this
sub-spike measured exactly where that over-approximation stops being free.**
The brief's stretch goal ("same `(p, t)` on both nodes ⇒ histories agree at
every `q ≤ p` within term `t`'s span") is refuted by a paper countermodel
with four **named roles**, verified by LA2's review at **`n = 5`** (the
LA2 review asserts reachability at `n ≥ 4` in general; the exact minimum
node count has not been computed — no `n = 4` trace was exhibited, so it
should be read as "at least 4," not "exactly 4"):

- **node 0** — the term-1 leader: appends `(0,1,a)` at position 0 and
  `(1,1,b)` at position 1, and replicates both to node 1.
- **node 1** — the term-2 leader: having received `(0,1,a),(1,1,b)` from
  node 0, node 1 wins term 2 and, as term-2 leader, **authors and ships**
  the floating replicate frame `(2,2,c)` (a `leaderAppend` at node 1's own
  frontier — the frame that later goes stale once node 1 is itself
  deposed).
- **node 2** — the node that later wins term 5 and accepts the stale
  frame: holds only `(0,1,a)` (never received node 0's second byte), wins
  term 5 on its lagging-but-sufficient credentials (durable 1), appends its
  own `(1,5,y)` at position 1, then — now at `currentTerm = 5`, frontier
  2 — accepts node 1's still-floating `(2,2,c)` frame via the `≤` guard
  (stamp 2 ≤ 5, position = frontier).
- **nodes 3 and 4** — voters: both sit at the boot credential `(lastTerm
  0, durable 0)`, which loses to every other node's credentials in this
  trace, so they can only ever grant (never plausibly win); they supply
  the quorum node 2 needs to win term 5 without a grant from node 0 or
  node 1 (both of whose credentials by then exceed node 2's, so neither
  would grant it).

Node 1 and node 2 now **agree at the shared pivot position 2** — both hold
`(2,c)` there, the `(p, t)` pair the prefix-form's hypothesis is about —
but **disagree at position 1**, strictly *below* that pivot: node 1's entry
there is `(1,b)` (relayed from node 0's term-1 tenure), node 2's is `(5,y)`
(node 2's own term-5 append), two different, non-`t`-matching stamps.

This forces a precise disambiguation of "within term `t`'s span" that the
brief's stretch statement left informal, which this memo states explicitly
because the countermodel actually distinguishes the two readings:

- **Entry-stamp reading** ("both histories are stamped `t` at every
  `q ≤ p`, and those entries agree"): this is **not a new claim** — it is a
  pointwise corollary of `log_matching` itself (apply the proved theorem at
  each such `q`), and it is **true**, trivially. Position 1 in the
  countermodel is *not* a counterexample to this reading at all: neither
  node's entry at position 1 is stamped term 2, so the reading never asserts
  anything about it.
- **Lineage/map reading** ("every `q ≤ p` that the term map's *span* for `t`
  covers, regardless of that position's own literal stamp"): this is the
  reading the countermodel refutes — position 1 falls inside what a
  map-derived "span of term 2, below position 2" would cover under either
  node's map, yet the two nodes hold genuinely different, differently-stamped
  content there. This is the reading that would require exact-match-or-drop
  replication (Rust's real behavior) to hold, and it is **false** under the
  model's conservative `≤` guard.

The false reading is the boundary of the "accept-before-reconcile
over-approximation" scope collapse (item 4 below) made precise and
measured: tightening `deliverReplicate` toward Rust's exact-match-or-drop
semantics (a real model change, out of this sub-spike's scope) is exactly
what the lineage/map reading would need. `log_matching` itself is unaffected
— indeed the same over-approximation that defeats the lineage reading is
what makes LM-core's proof *easier* (a strictly more permissive replication
rule can only produce more occurrences to reason about, never fewer, and the
`DInv.cert` machinery closes over all of them regardless).

### 3. Scope-collapse enumeration (routed from LA1's review)

All four documented model/reality gaps from the LA-series, restated together
per LA3's charter — none of them weakens what `log_matching` proves; each is
either provably harmless or is the explicit, now-measured boundary of the
theorem's reach:

1. **Fsync-lag collapse** (`ProtocolData.lean` module doc, item 4). A leader
   append is modeled immediately durable and `crashRestart` preserves the
   whole data plane; the real lost-unfsynced-suffix crash (the journal/
   `StableValue` recovery boundary) is not modeled. Safety argument: crash
   demotes (`currentTerm` is strictly monotone across restart-then-reelect —
   Phase 2's single-tenure fact), so any re-append after a lost suffix
   happens under a strictly higher term than what was lost, and can never
   manufacture two payloads under one already-used `(position, term)` pair —
   the one fact `log_matching` is about. Crash-plane fidelity is out of
   scope, not swept under the theorem.
2. **Atomic truncation** (module doc, item 5). `node.rs`'s real pipeline —
   persist-map → `Action::Truncate` → archive truncate → epoch'd `Truncated`
   ack → pending-map adoption, with a data-plane latch holding the window
   shut throughout — collapses into one atomic `deliverTermMap` step.
   Reorder-equivalence argument: the real latch admits no data event into
   the window, and a crash inside the window self-heals to the same
   post-state (persist-before-truncate + `rederive_term_map`), so the
   atomic model transition already *is* the committed real-world outcome;
   nothing the latch would have serialized differently is reachable.
3. **Full-map gossip** (module doc, item 7). `shipTermMap` ships the entire
   term map every time, rather than Rust's bounded `term_map_wire_tail()`
   window. The window is precisely what makes `NoCommonPrefix` reachable in
   production (a follower whose shared prefix has been purged past the
   window); under full-map gossip the wipe arm (`Node.applyGossip`'s
   `.noCommonPrefix` case, `wipe_on_no_common_prefix`) stays **modeled but
   unreachable in this model's worlds** — an honest simplification carried
   unchanged from LA1's review, not newly discovered here.
4. **Accept-before-reconcile over-approximation** (module doc, item 6 —
   the `stamp ≤ currentTerm` guard vs. the real `awaiting_reconcile` gate).
   This is Finding #4's fix (§2i) and its now-measured boundary (§2ii): the
   model keeps only the `≤` consequence of Rust's exact-match-or-drop +
   reconcile-before-data pair, which is conservative for `log_matching`
   (strengthens it, if anything) but is exactly what blocks the
   lineage/map reading of the prefix-form stretch goal from holding.

**Docstring fix (in scope per the LA3 brief, applied this task):**
`ProtocolData.lean`'s `deliverReplicate` docstring previously glossed the
guard as "the header-term-adopt + reconcile-before-data intake gate," which
reads as if a DATA datagram's header term itself triggers adoption. It does
not — `uc2_net`'s receiver *drops* a DATA datagram whose header term doesn't
match the adopted term; adoption comes only from consensus datagrams
(vote grants, term-map gossip). The docstring now states the guard as the
model's `≤` consequence of that drop-not-adopt behavior plus the
`awaiting_reconcile` gate, matching item 6 above and Finding #4's write-up
precisely.

### 4. Measured costs vs. estimate

| Task | Wall-clock | Output | Debug iterations | Review |
|---|---|---|---|---|
| LA1 (data-plane model + Finding #4 guard) | **~50 min** (~15 read, ~10 design, ~20 write, ~5 build/gate) | `ProtocolData.lean`, 508 lines as committed by LA1, 11-constructor `Step`, projection lift + `adoptHigherTerm` | 2, both **Lean-mechanics-only** (a structure-literal field-alignment parse quirk; a `simpa`-vs-`simp;exact` transparency mismatch) | Approved, first pass |
| LA2 (`DInv` + `log_matching` + truncation trace) | **~2h15m** (~25 read, ~35 design, ~10 trace, ~40 write, ~15 build/gate, ~10 evidence/commit) | `LogMatching.lean`, 1046 lines, 5-clause `DInv` + `Cert` | 2, both **mechanics-only** (the same structure-literal quirk recurring; one `simp`/`.symm`/unification fix each) | Approved, first pass |
| **LA1+LA2 combined** | **~3h05m (185 min)** | 1554 lines as LA2 left them (508 + 1046; now 1557 — `ProtocolData.lean` gained +3 from this task's own docstring fix, §3 above) | — | — |

In S2-equivalents (S2 = the Phase-2 spike's `ElectionSafety.lean` task,
~30 min — this memo's own reference unit): **185 min ÷ 30 min ≈ 6.2
S2-equivalents.**

Memo §3(a) priced this component at **3–7 S2-equivalents, 3–5 wall-clock
sessions**. The actual, ~6.2 S2-equivalents, lands **inside the band, near
its top** — a real contrast with the Phase 2 spike's own ~100×
overestimate (the original memo's §2): this time the effort estimate was, if anything,
close to accurate rather than wildly high. The pattern that *did* recur is
narrower than "estimates run high" — it's specifically **session-count**
that overshot: 3–5 sessions were priced (at Phase 1's task cadence of
discrete ~30–45 min sittings), but the actual work landed in **2** genuinely
continuous sittings (one per task), with LA2 alone running long enough
(~2h15m, ~4.5 S2-equivalents) to internally cover what the cadence model
would have called 3–4 separate sessions. So: the *total-effort* estimate was
good this time; the *session-count* framing (assuming Phase-1-sized discrete
sittings) was not — a different-shaped miss than S1/S2's, worth carrying
forward as its own known-cost note rather than folding into the same bucket.
Zero invariant-content churn in either task (both sets of debug iterations
were pure Lean-mechanics, the same class of cost LA1 first hit and flagged
forward to LA2, where it recurred exactly as predicted) — the R4-class
stuck-proof risk did not materialize a third time running (after S1/S2 and
now LA1/LA2), which is itself informative for §5's re-pricing below.

### 5. Re-pricing (b) leader completeness and (c) state-machine safety

**What (b) gets for free from (a), beyond just "the data plane now exists."**
The original §3(b) pricing assumed leader completeness would have to build
its own cross-time reasoning from scratch once `logOk` became load-bearing.
Two concrete pieces of that work are now already sitting in
`LogMatching.lean`, proved and exported:

- **`DInv.cert`** — the cross-time writer certificate (`quorum`: a
  grants-or-self quorum for `(term, leader)` in the append-only `sent` set;
  `pinned`: the StableValue-persisted vote record, riding
  `currentTerm`/`votedFor`/`role`; `noForeign`: that leader never granted the
  term to anyone else) is *exactly* the shape of statement a
  leader-completeness proof needs to say "the node that is the elected
  leader for term `t` really did win `t`, historically, not just in the
  current instant" — `election_safety` alone is simultaneous-only and can't
  express this (LA2's report notes this explicitly: `Cert` was the
  discovered inductive form of the temporal claim `election_safety` doesn't
  cover). `Cert` and `reachable_dinv` are public, non-`private` — directly
  reusable as (b)'s leader-election certificate rather than a rediscovery.
- **`DInv.frontier`** — a leader's `durable` strictly bounds every
  occurrence stamped with its own term — is half of what "elected leader's
  durable ≥ global commit" needs: the other half is C3
  (`advance_certified`/`Uc2Proofs/Commit.lean`, already proved in Phase 1 as
  a pure fold over reported positions) wired into the *step relation* as a
  real quorum-driven global counter, which is genuinely new — `Protocol.lean`
  /`ProtocolData.lean` have no commit-counter transition at all yet.

**What's still net-new for (b).** `logOk` becoming load-bearing (§3(b)'s
original framing, unchanged) is real: `V2`/`logOk_iff` (Phase 1,
`Uc2Proofs/Vote.lean`) gives the exact `(term, durable)` lexicographic
characterization already proved, but it has never been correlated against a
*live* commit counter inside a step relation before — that correlation, and
wiring C3 into `Step` as an actual constructor rather than a property of one
pure-fold call, is the part of §3(b)'s original estimate that survives
untouched.

**Re-estimate for (b): 3–6 S2-equivalents / 2–4 sessions** (down from the
original 4–9 / 4–7), on top of (a). Reasoning: the hardest single piece
of the original estimate — cross-time writer uniqueness under a real
network — is discharged and reusable (`Cert`); the `frontier` clause covers
half of the commit-bound argument; what remains (wiring C3 as a step-level
transition, correlating it against `logOk_iff`) is real but bounded work,
not a fresh invariant-discovery exercise. The range stays wide rather than
collapsing to a point because the *pattern* observed twice now (S1/S2 then
LA1/LA2: zero invariant-content churn, all debug cost mechanical) is
encouraging but is not yet three-for-three at this specific proof's shape —
commit-tracking-as-a-step-relation-transition is genuinely unprecedented in
this codebase's Lean work.

**What (c) gets for free from (a).** The original §3(c) pricing assumed the
reconcile-on-gossip transitions (`R2`/`R3a`/`R3b`/`R4`) would need to be
newly wired in as step-level constructors — that wiring is **done**:
`deliverTermMap`/`Node.applyGossip` already consume the PROVED
`Uc2Model.reconcile` kernel as an atomic step transition (scope collapse #2
above), and `LogMatching.lean`'s `applyGossip_hist` lemma (truncation/wipe
only erases, never invents content) is exactly the non-truncation-adjacent
fact (c) needs as a building block for sim's inv4 analog. The 15-step
truncation trace (this section's §1 above) is also a ready-made non-vacuity witness for whatever
(c)'s truncation-composition theorem needs to exhibit.

**What's still net-new for (c).** The two Finding-#3 residuals the Phase 1
gate doc left as informal arguments rather than theorems: `start_election`
reading the pre-prune term map (currently justified only by an informal
"safe by quorum intersection" note in
`docs/benchmarks/uc2-lean-gate-2026-07-16.md`'s Finding #3 disposition), and
the `awaiting_reconcile` gate's role blocking the second phantom-creation
path — neither is representable in `ProtocolData.lean` today, since intake
ordering / gating state isn't modeled at all (this sub-spike's `≤` guard,
§2i/§4-item-4, is the closest thing, and it is explicitly a weaker
stand-in). Formalizing either requires new step-relation machinery, not
reuse of (a)'s artifacts. Composing inv4 (non-truncation of committed bytes)
with inv5 (election above commit) also strictly needs (b)'s commit counter
first.

**Re-estimate for (c): 4–9 S2-equivalents / 3–6 sessions** (down from the
original 5–12 / 4–8), strictly on top of (a)+(b). Reasoning: the
reconcile-step machinery and its erasure-only lemma are reused wholesale,
which was the single biggest line item in the original estimate's "R2/R3/R4
slot in as step semantics" framing; the Finding-#3 residuals are real,
un-reused new work (intake-gate modeling has no precedent in this codebase's
Lean model at all) and keep the range from collapsing further.

### 6. Recommendation for (b): GO, phased, same cap discipline

**GO for (b) leader completeness, as a single time-boxed sub-spike capped at
6 sessions (the top of the re-priced range), with a mandatory re-gate before
(c)** — the same phased structure the original memo's §4 recommended, now
re-run one link down the chain. The evidence for continuing rather than
stopping is stronger than it was at the original gate, not weaker:

- **The R4-class stuck-proof risk has now stayed dormant twice in a row**
  (S1/S2, then LA1/LA2) — both sub-spikes reported zero invariant-content
  churn, with every debug iteration being pure Lean mechanics identified and
  named in advance. That is genuine, if still limited, evidence the model
  layering discipline (havoc → real data plane → the next real component)
  generalizes rather than being a one-off.
- **(a)'s artifacts are directly reusable for (b)**, not just conceptually
  adjacent (§5) — `Cert`, `frontier`, and the truncation-trace pattern are
  concrete exported Lean objects (b) consumes, which is the mechanism, not
  just the hope, behind the downward re-price.
- **The effort estimate itself was accurate this time** (this section's §4
  above) — a second data point (after S1/S2's ~100× miss) suggesting the
  estimation is converging as more of this specific proof's shape gets
  measured, which should make the re-priced (b)/(c) ranges more trustworthy
  than the original memo's §3 ranges were.

**Trigger conditions**, unchanged in kind from the original memo's §4
framing:

- **Continue to (c)** if (b) lands within its 6-session cap with clauses
  designed on paper and little-to-no post-first-build churn (the pattern
  both prior sub-spikes showed).
- **Stop and re-price before (c)** if (b) blows through its cap or surfaces
  an R4-class stuck proof / insufficient informal contract — a real finding
  either way, not a discipline failure, per the spec's own risk register.
- **NO-GO on (b) is still not recommended** given the by-now 2-for-2 clean
  run and the reusable artifacts sitting ready, but remains a legitimate
  opportunity-cost call for the user (leader leases / wire-crypto stand as
  the recorded alternatives) — a resourcing decision, not a technical one.
- What the user should weigh, unchanged from the original memo's §4: the
  sim/elle/lincheck stack
  already covers leader completeness and state-machine safety empirically
  under fault injection; Tier B's marginal value stays "mechanized exhaustive
  guarantee + proof-pressure findings," now with a second and third real
  finding (#4 and the prefix-form boundary) added to Phase 1's three since
  the original gate — the finding-yield trend, if anything, continues to
  favor continuing.

## Tier B(b) actuals + re-gate

**Date:** 2026-07-18. **Branch:** `uc2/lean-leader-completeness` (base
`main` @ `9ee8e00`, the Tier B(a) merge). Plan:
`docs/superpowers/plans/2026-07-17-uc2-lean-leader-completeness.md` (LB1
commit machinery / LB2 `leader_completeness` / LB3, this section). Task
detail: `.superpowers/sdd/task-LB1-report.md`,
`.superpowers/sdd/finding5-fix-report.md`,
`.superpowers/sdd/task-LB2-report.md`,
`.superpowers/sdd/finding6-fix-report.md`,
`.superpowers/sdd/task-LB2-rerun-report.md`,
`.superpowers/sdd/task-LB2b-report.md`; ledger: `.superpowers/sdd/progress.md`
("TIER B(b) LEADER COMPLETENESS" onward). Gate re-run for this doc:
`cd proofs && lake build && ! grep -rn --include='*.lean' -w 'sorry'
Uc2Model Uc2Proofs Conform` — `3027 jobs`, zero `sorry`, `GATE_PASS`.
`proofs/Uc2Proofs/ProtocolCommit.lean` is 748 lines,
`proofs/Uc2Proofs/LeaderCompleteness.lean` is 638 lines, as of the head
commit (`1936141`) at gate time.

The original memo's §6 (above) authorized (b) leader completeness as a
single time-boxed sub-spike capped at 6 sessions, on the strength of two
prior sub-spikes (S1/S2, LA1/LA2) that both landed with zero
invariant-content churn. This section is that sub-spike's actuals. Unlike
Tier B(a) — which landed inside its estimated band (the original memo's own
§4 above) — this sub-spike **did not land the theorem and blew its cost
estimate by roughly an order of magnitude**, for reasons that are, on
inspection, the opposite of a discipline failure: the sub-spike found two
real, shipped safety bugs and one genuine model-fidelity gap, each of which
forced a stop-the-line adjudication before proof work could continue.

### 1. Result: leader completeness lands CONDITIONALLY, not unconditionally

**`leader_completeness` is NOT proved.** Stated plainly, because the
headline could easily read as a green checkmark and it is not one: the
theorem itself does not exist anywhere in `proofs/Uc2Proofs/`. What exists,
machine-checked and gate-green, is a **partial, conditional** landing —
`task-LB2b-report.md`'s Option-2 deliverable:

- `FramesCurrentAuthored` — a hypothesis, designed and hand-verified
  (sufficient / faithful to `receiver.rs:636-639` / non-circular) to carry
  away exactly the model over-approximation Finding #7 (§2 below) exploits,
  no stronger.
- `hist_frame_provenance` and `committed_frame_provenance` — two
  UNCONDITIONAL supporting lemmas (no `FramesCurrentAuthored` hypothesis)
  that every stamped history entry, and every committed ghost entry, traces
  back to an actual frame on the wire.
- `nonvacuity_leader_completeness_trace` — the required non-vacuity
  witness: a reachable world satisfying `FramesCurrentAuthored` and
  non-trivial `leader_completeness` premises.
- **NOT landed: the `leader_completeness` theorem itself.** The
  `becomeLeader` induction case needs supporting invariant machinery this
  codebase does not yet have (§4 below); `task-LB2b-report.md` estimated
  building it at "at least 3–5 more S2/LA2-scale inductions" and stopped
  per the sub-spike's own stuck-protocol rather than sorry it, weaken it, or
  force it through with an over-strengthened hypothesis.

So: election safety and log-matching are unconditionally proved (Phase 2,
Tier B(a)). Leader completeness is conditionally landed — sufficient
machinery exists to state and non-vacuously instantiate a
`FramesCurrentAuthored`-conditioned version, but the theorem closing the
induction is open. State-machine safety (c) was never started. This is a
materially different place than the original memo's phased plan (§4/§6
above) assumed (b) would land.

### 2. THE FINDINGS — the value ledger, this sub-spike's headline

Four findings surfaced during this sub-spike's adversarial invariant-design
passes, each escalated and adjudicated before any further proof attempt, per
the plan's stuck-protocol. Two are real, shipped, now-fixed bugs; one is a
statement-design gap; one is a model-fidelity gap. This is the actual
deliverable of the sub-spike, independent of whether the theorem itself
lands.

**Finding #5 (FIXED — real shipped bug, safety-class, commit path).**
`task-LB1-report.md`'s commit-certification layer surfaced this before any
preservation proof was attempted: `uc2_node` booted the receiver intake gate
OPEN (`node.rs`, `AtomicBool::new(true)`, was line 516) with
`awaiting_reconcile: false` (`node.rs`, was line 801), while
`ElectionSm::new` recovers `current_term = vote_term.max(map_term)`
(`election.rs:400-402`) and votes persist before send. A voter that GRANTED
term T, held a divergent tail, and crashed before reconciling rebooted AT
term T with the gate open; the receiver's 20ms `AppendPosition` floor
re-send (`receiver.rs:1052-1078`) could beat the leader's 100ms idle
term-map re-ship, and the same-term report fed the T-leader's
`CommitTracker` — a phantom commit backed by content the reporter did not
actually hold. Machine-checked first as a 27-step kernel-decided
countermodel (`finding_boot_gate_stale_report_lc_violation`, now deleted
post-fix); `finding5-fix-report.md` records the RED-first directed
`uc2_sim` scenario
(`rebooted_unreconciled_voter_must_not_certify_phantom_commit`) that
reproduced the same shape and turned GREEN post-fix. **Fix:** the boot gate
now closes iff `vote_term > map_term` (`node.rs`, both the gate-init and
`awaiting_reconcile`-init sites); reopening rides the existing
clean-reconcile / truncate-ack / `BecomeLeader` arms — no new machinery.

**Finding #6b (FIXED — real shipped data-loss bug, Raft §5.4.2 / Figure-8
class).** `task-LB2-report.md`'s first `leader_completeness` attempt
surfaced this: `uc2_consensus/src/election.rs::rank_leader` pushed
`Action::AdvanceCommit` off the positions-only `CommitTracker`
UNCONDITIONALLY; `new_term_pos` gated reads/ingress/M7-propose via
`serving`/`can_serve` but never the commit store itself. At any failover
inheriting an uncommitted tail, followers reconcile clean and their
gate-open 20ms `AppendPosition` floor reports the election base BEFORE the
NewTerm frame is quorum-durable, so the leader could commit an
**old-term-only range** — acks/apply/leader-only outputs firing below the
§5.4.2 barrier. The loss continuation (machine-checked as the 46-step, n=5
countermodel `finding_fig8_old_term_commit_data_loss`, now deleted
post-fix): a divergent higher-`lastTerm` rival wins the next term with a
commit-quorum member's honest grant and truncates the committed byte
cluster-wide — the committed entry ends with zero copies anywhere in the
cluster. `finding6-fix-report.md`'s RED-first `uc2_sim` scenario
(`old_term_range_must_not_commit_before_new_term_quorum`, 5 nodes) caught
the same shape via the existing inv2 oracle (term-map prefix consistency —
the review's inv4/inv5 prediction was structurally preempted by inv2 firing
first, an honest deviation the fix report documents rather than papers
over) and turned GREEN post-fix. **Fix:** `rank_leader`'s advance/store/
gossip block is clamped to `ranked ≥ new_term_pos`
(`election.rs:1451-1465`) — `None` means no advance, a suppressed advance
does not touch `commit_seen`, no new state.

**Finding #6a (statement re-key, not a Rust bug).** Independent of #6b and
surviving its fix: the ghost ledger (LB1 decision 1) recorded
`(position, stamp, payload)` while Raft's Leader Completeness (§5.4.3)
quantifies over the COMMIT term, not the stamp — a stamp-`t` entry can be
committed at `T > t` (a re-elected leader certifying its own inherited
prefix), and an honest intermediate-term leader owes it nothing. Machine-
checked as a 23-step countermodel
(`finding_stamp_keyed_lc_stale_leader`/`lc_core_stamp_keyed_is_false`, both
now deleted). No Rust change: UC never exposes a stamp-keyed commitment.
**Fix:** the ghost now carries the commit term —
`committed : List (Nat × Nat × Nat × Nat)` as `(p, stamp, T, v)` — and
`leader_completeness`'s hypothesis re-keyed to `T ≤ currentTerm i` (the
form still open, §1 above).

**Finding #7 (MODEL-FIDELITY gap, NOT a Rust bug).** `task-LB2-rerun-report.md`,
surfacing after both #6-class fixes landed: the LA1 `≤`-guard
over-approximation on `deliverReplicate` (`hstamp : t ≤ currentTerm`,
already flagged in its own docstring as "the model's ≤ CONSEQUENCE of two
Rust guards," per the original memo's §3(a)/Tier B(a) §3 above) is sound for
log-matching but unsound for leader completeness: it lets a follower accept
a dead leader's in-flight stale frame interleaved with the live leader's
stream — a "Frankenstein log" real UC structurally forbids — and then
honestly report a durable frontier covering the leader's commit range with
divergent content underneath. Machine-checked as a 33-step countermodel
(`finding_stale_replicate_replay_lc_violation`/
`lc_core_commit_term_keyed_is_false`, both KEPT — they refute the
unconditional statement, which stays refuted regardless of the conditional
route). Rust evidence: `receiver.rs:636-639`
(`if h.leadership_term_id != term { dropped_stale_term; return; }`) —
exact header-match, not `≤`; real replication re-serves old-stamped bytes
during catch-up/NAK-repair strictly inside the CURRENT leader's stream, a
distinction the model's record-stamp-only `Frame.replicate` cannot express.
This is what `FramesCurrentAuthored` (§1 above) assumes away, and what the
scheduled Option 1 model refinement (§4 below) is meant to discharge by
construction rather than by hypothesis.

**The meta-point, stated as this memo's own reading of the four reports
together:** Findings #5 and #6b were invisible to the entire empirical
stack (`uc2_sim`'s seeded fuzz, the elle consistency harness, the WGL
lincheck capstones) not because those oracles were poorly designed, but
because of an **interleaving coverage gap** — both fix reports say this
explicitly (`finding5-fix-report.md`'s "why nothing caught it before,"
`finding6-fix-report.md`'s oracle-determination section): the existing sim
oracles (inv7 for #5, inv2 for #6b) fire correctly and immediately the
moment a directed scenario reaches the violating interleaving — `uc2_sim`
just never reached it. `task-LB2-report.md` names the precise reason for
#6b: same-disk kill-restart crashtests and 3-node elle scenarios cannot
produce the divergent-rival, two-term-choreography shape the bug needs;
`finding5-fix-report.md` names the analogous reason for #5 (rebooted voter
+ persisted grant + report-beats-gossip race). The prover did not invent a
new class of defect the empirical stack was blind to in principle — it
walked a path through the state space the empirical stack's own
fuzz/scenario generators had structurally never walked.

### 3. Measured costs

**Wall-clock, by task, from the reports' own proof-cost accounting**
(`task-LB1-report.md`, `task-LB2-report.md`, `task-LB2-rerun-report.md`,
`task-LB2b-report.md`; the two fix reports do not carry an equivalent
wall-clock section — see the note below):

| Task | Wall-clock | Outcome |
|---|---|---|
| LB1 (commit machinery + non-vacuity) | ~95 min | complete, Finding #5 escalated |
| LB2, first attempt | ~210 min (3.5h) | BLOCKED — Finding #6a + #6b |
| LB2, re-run | ~150 min (2.5h) | BLOCKED — Finding #7 |
| LB2b (Option 2, hypothesis + lemmas + non-vacuity) | ~630 min (10.5h, summed from the report's own stage breakdown: ~2.5h reading + ~1.5h hypothesis design + ~2.5h mechanizing the two lemmas + ~1.5h the non-vacuity trace + ~2h proof-strategy analysis + ~0.5h docstrings/gate) | PARTIAL — theorem itself BLOCKED |
| **Measured Lean-task total** | **~1085 min ≈ 18h05m** | |

In S2-equivalents (S2 = the Phase-2 spike's `ElectionSafety.lean` task,
~30 min — the original memo's own reference unit, §2 above): **1085 min ÷
30 min ≈ 36.2 S2-equivalents**, against the original memo's §6 estimate of
**3–6 S2-equivalents for (b)**. That is a **~6–12× overrun on the Lean-task
time alone** — before folding in the two Rust fix cycles at all.

**Honest gap in the record:** `finding5-fix-report.md` and
`finding6-fix-report.md` do not report a wall-clock total the way the four
Lean-task reports do (grepped for "wall-clock"/"hour"/"minutes" — none
present). Both cycles were real, substantial engineering effort by their
own deliverable lists — RED-first directed `uc2_sim` scenario construction
(one new sim fidelity mechanism for #5's 20ms report-floor mirror; a 5-node
scripted-partition scenario for #6b), a Rust source fix, a `Uc2Proofs/
ProtocolCommit.lean` model amendment plus finding-theorem deletion, a storm
crash-rate re-tune with a multi-point ppm probe sweep for each, and a full
cross-crate gate re-run (`uc2_sim`, `uc2_consensus` both feature configs,
`uc2_node --lib`, workspace clippy, `lin_v2`, and for #6b also
`lin_partition_v2`) — but this memo will not fabricate a number where the
source record has none. The true total is **the measured ~18h05m of
Lean-task time PLUS an unrecorded, additive amount for the two fix
cycles** — meaning 36.2 S2-equivalents is a floor on the sub-spike's actual
cost, not the whole of it.

**This is the opposite pattern from Tier B(a).** The original memo's own
Tier B(a) section (above) measured LA1+LA2 at ~6.2 S2-equivalents against a
3–7 estimate — inside the band, near its top, and explicitly called out
there as "a real contrast with the Phase 2 spike's own ~100× overestimate:
this time the effort estimate was, if anything, close to accurate." Tier
B(b) breaks that emerging pattern in the other direction: not because the
proof mechanics were hard (LB1, LB2, and the LB2 re-run each report **zero
failed build iterations** — every countermodel type-checked on the first
`lake build`), but because the adversarial invariant-design phase kept
surfacing genuine defects — two real bugs and one model-fidelity gap —
each of which stopped the clock for an out-of-band Rust-fix-and-regate
cycle before Lean work could resume. The R4-class "informal contract turns
out insufficient" risk that stayed dormant through S1/S2 and LA1/LA2 (the
original memo flagged this dormancy explicitly at each prior gate) did not
recur here in that exact form — instead, the sub-spike's proof pressure hit
something arguably more valuable and more expensive: real protocol bugs,
not just proof-statement gaps.

### 4. The hybrid plan + follow-ups

The user's directive after Finding #7 (`progress.md`: "USER DECISION:
HYBRID (Option 2 first — conditional LC now; Option 1 model-refinement to
discharge later, scheduled follow-up. Fable out of credits → sonnet.")
structures what remains:

- **Option 2 (this sub-spike, `task-LB2b-report.md`): PARTIAL.**
  `FramesCurrentAuthored` + `hist_frame_provenance` +
  `committed_frame_provenance` + `nonvacuity_leader_completeness_trace` are
  landed and machine-checked (§1 above). The `leader_completeness` theorem
  itself is OPEN. `task-LB2b-report.md`'s own "Recommendation for the next
  attempt" lists the remaining work in dependency order:
  1. Prove `Uc2.TermMap.Ascending` is a maintained world invariant — it
     exists as a pure predicate in `Uc2Model/TermMap.lean` but, per the
     report's own `grep -rn Ascending Uc2Proofs/*.lean` check, nothing
     outside `Reconcile.lean` establishes it holds across reachable worlds.
  2. Prove a message-indexed report-provenance clause over `CMsg.report`,
     mirroring `ElectionSafety.lean`'s `Inv.grant_state` pattern (an S2-era
     artifact) applied to the commit-plane report messages instead of vote
     messages.
  3. Prove `committed_term_at_leaders` (the report's working name for the
     invariant clause the `becomeLeader` induction needs), consuming (1)
     and (2) plus the already-public `quorum_intersect` (C5) and
     `logOk_iff` (V2).
  4. Assemble `leader_completeness` from (3) plus the already-landed
     `hist_frame_provenance`/`committed_frame_provenance` and the
     unconditionally-proved `Uc2.Data.log_matching`.
  `task-LB2b-report.md` estimates this at "at least 3–5 more S2/LA2-scale
  inductions."
- **Option 1 (scheduled, NOT started): frame header/stamp split +
  `serveTail`.** `task-LB2-rerun-report.md`'s recommended fix: give
  `Frame.replicate` both a header term (provenance — must equal the
  receiver's currently-adopted term, mirroring `receiver.rs:636-639`
  exactly rather than approximating it) and a record stamp, plus a new
  `serveTail` leader step modeling the real NAK-repair/journal-replay path
  by which old-stamped bytes legitimately reach a reconciled follower under
  the CURRENT leader's header. This discharges `FramesCurrentAuthored`
  entirely — it becomes a provable fact rather than a standing hypothesis —
  at the cost of touching `Uc2Proofs/ProtocolData.lean`'s `Frame` shape,
  which per the LA1 layering rules requires `Uc2.Data.log_matching`
  (`LogMatching.lean`, 1046 lines) to re-green in the same task.
  `progress.md`'s own gloss on this fix's scope: "~2–3 S2-eq model work
  before the LC proof itself" — i.e. before whatever remains of Option 2's
  four-step list above, since the `becomeLeader` invariant machinery is
  needed either way.
- **Tooling.** `progress.md` records that fable (the model used for LB1
  and both LB2 BLOCKED-with-countermodel attempts) hit its credit limit
  mid-task during the LB2 re-run's adjudication, forcing implementers and
  reviewers onto sonnet from LB2b onward. Sonnet landed a genuinely useful
  partial result (§1 above) but self-assessed the remaining
  `becomeLeader`-endgame work as exceeding its own stuck-protocol ceiling.
  Read against the arc's track record — fable closed S2's invariant
  discovery, LA1/LA2's data-plane layering and log-matching proof, and both
  LB2 countermodel discoveries, each within a single measured session — the
  remaining LC-closing work (an `Ascending` world invariant plus a
  message-indexed provenance clause plus a `becomeLeader` quorum-intersect
  endgame) looks, on this memo's own reading of the pattern, like the kind
  of invariant-discovery task this arc has consistently routed to fable
  rather than sonnet. Whether that observation should gate the next attempt
  on fable-credit availability is a scheduling call, not a technical one
  this memo settles (§6 below).

### 5. Re-pricing (c) state-machine safety

The original memo's §5/§6 above (the Tier B(a) re-gate) priced (c) at
**4–9 S2-equivalents / 3–6 sessions, strictly on top of (a)+(b)** — a
pricing that implicitly assumed (b) would be a COMPLETE, unconditional
`leader_completeness` by the time (c)'s design work started, the same way
(a) was complete and unconditional when (b) was priced.

That assumption no longer holds. (c) composes an inv4-analog
(non-truncation of committed bytes) with an inv5-analog (election above
commit) — both explicitly named as needing (b)'s commit-counter machinery
as a dependency in the original memo's own §5 above ("Composing inv4 …
with inv5 … also strictly needs (b)'s commit counter first"). A conditional
`leader_completeness` is not the same dependency as a complete one: (c)
would either have to (i) wait for `leader_completeness` to close
unconditionally, or (ii) inherit `FramesCurrentAuthored` as its own standing
hypothesis, compounding rather than resolving the conditionality one layer
further into the arc's most safety-critical theorem. This memo does not
pick between those two options — that is a design call for whoever starts
(c), informed by whichever of §6's recommendations the user picks first.

**Re-estimate: (c) is now gated on finishing (b), and "finishing (b)"
itself costs more than the original 3–6 S2-equivalents already spent.**
Stacking the two scheduled pieces of remaining (b) work: Option 1's
"~2–3 S2-eq model work" (`progress.md`'s own gloss, §4 above) plus Option
2's remaining "at least 3–5 more S2/LA2-scale inductions"
(`task-LB2b-report.md`, §4 above) — noting these are not strictly additive
if Option 1 lands first and simplifies what Option 2's remaining steps need
to prove, but no report has re-derived that simplification, so treating
them as additive is the honest default — puts finishing (b) at roughly
**5–8 more S2-equivalents** on top of the ~36.2 already measured, before
(c) can even begin its own estimated 4–9 S2-equivalents. Total to reach a
complete (c): **roughly 9–17 S2-equivalents of NEW work from here**, on top
of the ~36.2 (plus the two fix cycles' unrecorded time) already spent on
(b).

**The explicit honest caveat this memo owes, given §3 above:** (b)'s own
3–6 S2-equivalent estimate missed the actual by 6–12×, and the miss was not
a calibration error in the usual sense — it came from real bugs the
adversarial proof process found, which by nature cannot be foreseen by an
estimate made before the invariant-design pass happens. There is no
principled reason to expect the 9–17 S2-equivalent number above is any
better calibrated than the number it revises. Read it as a floor, stated
with the same honesty the original memo applied to its own ~100×
Phase-2-spike miss (§2 above): informative for ordering-of-magnitude
planning, not a number to schedule against precisely.

### 6. Recommendation

Given three real findings (two shipped bugs fixed, one model-fidelity gap
scheduled but not yet discharged) and a partial-but-genuinely-useful
mechanized landing, this memo presents the options rather than picking one
— consistent with how the arc has handled every prior stop-the-line
decision (Finding #3's disposition options in the Phase 1 gate doc,
Findings #5/#6/#7's escalations in this sub-spike, and the original
hybrid-vs-full-refinement decision after Finding #7 itself):

1. **Continue Option 2 now, more sonnet sessions.** Directly consumes
   `task-LB2b-report.md`'s landed machinery
   (`FramesCurrentAuthored`/`hist_frame_provenance`/
   `committed_frame_provenance`/the non-vacuity trace) via its own
   dependency-ordered four-step list (§4 above). Risk: sonnet already hit
   its stuck-protocol ceiling once on exactly this remaining work; even a
   clean landing leaves `leader_completeness` **permanently conditional**
   on `FramesCurrentAuthored` unless Option 1 is done afterward anyway.
2. **Wait for fable credits, then continue Option 2 with fable.** This
   arc's track record (§4 above) suggests fable is the better-matched tool
   for the remaining invariant-discovery work specifically. Risk: unknown
   timeline for credit availability; still leaves the theorem conditional
   even on success, same as option 1 above.
3. **Do Option 1 first** (frame header/stamp split + `serveTail`,
   `LogMatching.lean` re-green), converting the eventual
   `leader_completeness` into a strictly stronger, UNCONDITIONAL theorem
   rather than one permanently carrying `FramesCurrentAuthored`. This is
   the follow-up the user already scheduled at the Finding #7 adjudication.
   Likely the most expensive of the three paths in isolated cost (touches
   the data-plane model + a 1046-line re-green), but the only one that
   retires the model-fidelity debt Finding #7 identified rather than
   working around it.
4. **Treat the arc's bug-finding as the deliverable and pause further
   proving on leader completeness.** Two real, shipped safety bugs
   (Findings #5 and #6b) were found and fixed as a direct, measurable
   result of this sub-spike's proof pressure — permanent value, already
   banked, independent of whether the theorem itself is ever finished. Per
   §2 above, neither bug was an oracle-design gap the empirical stack
   (`uc2_sim`/elle/lincheck) could have been tuned to catch without the
   adversarial invariant-design process that found them; pausing does not
   retroactively lose that value. Cost: (c) stays permanently blocked and
   the arc's own `(a) → (b) → (c)` phased plan (original memo's §4 above)
   stops one theorem short of state-machine safety.

This memo does not recommend among these four — §5's honest re-price and
§3's honest cost accounting are offered as the inputs to that call, not a
conclusion in place of it.
