# UC v2 Lean Phase 2 Spike — Election Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An N-node protocol model of UC's election machinery in Lean and a sorry-free proof of **election safety** (at most one leader per term), plus the go/no-go memo pricing the rest of Tier B.

**Architecture:** A relational small-step model in `Uc2Proofs/Protocol.lean` (mathlib allowed — this is NOT a conformance-mirrored executable kernel, so it lives outside the `Uc2Model` executable layer), reusing Phase 1's `Uc2.quorum_intersect` (C5) and mirroring `Uc2Model.Vote`'s discipline. Spec: `docs/superpowers/specs/2026-07-16-uc2-lean-proofs-design.md` §7.

**Tech Stack:** Lean v4.32.0 + mathlib v4.32.0 (pinned, warm cache), existing `proofs/` lake package.

## Global Constraints

- Theorem statements are the contract; the election-safety statement below is FIXED (weakening = controller escalation). The INVARIANT is discovery work — reformulate freely.
- Sorry-free, standard axioms only (`propext`, `Classical.choice`, `Quot.sound`), verified via `#print axioms`.
- `Uc2Model` stays untouched and mathlib-free; new files go under `Uc2Proofs/` only.
- The model must be demonstrably NON-VACUOUS: an `example` trace must reach a `.leader` state (else election safety is vacuously true and the spike proves nothing).
- Gate before every commit: `cd proofs && lake build && ! grep -rn --include='*.lean' -w 'sorry' Uc2Model Uc2Proofs Conform`.
- No heavy artifacts to `/tmp`; stage only your own files.
- **Proof-cost accounting**: each task's report must record wall-clock effort and which lemmas were hard — this feeds the go/no-go memo, and is as much a deliverable as the proofs.

## Modeling decisions (settled at plan time — deviations need controller sign-off)

1. **Sent-set network semantics.** `sent : List Msg` is append-only; a step may *process* any message in `sent` at any time, never removing it. This gives loss (never processed), duplication (processed twice), and reordering (any order) for free — the standard safety-proof encoding of the sim's fault model.
2. **Havoc data plane.** Election safety does not depend on log content, so `durable`/`lastTerm` (the `log_ok` inputs) evolve by an unconstrained havoc step instead of modeled data-append/gossip messages. This makes the theorem STRONGER (safety under arbitrary data evolution ⊇ safety under UC's actual data plane) and the model smaller. Leader completeness (post-gate) will need the real data plane; the memo must price that.
3. **Crash-restart** preserves `currentTerm` and `votedFor` (the `StableValue`-persisted vote record — V3's persist-before-send makes grant-then-crash safe), resets `role := .follower` and `votesReceived := ∅`.
4. **Vote discipline mirrors `Uc2Model.Vote`**: `votedFor : Option (Nat × Fin n)`; grants only at `currentTerm`; idempotent re-grant to the same candidate; older-term `votedFor` falls through to a fresh grant. Freshness (`logOk`) is applied but is irrelevant to this theorem (any grant predicate preserves election safety — that's *why* the havoc data plane is sound).
5. **Quorum** = `Finset (Fin n)` with `n / 2 + 1 ≤ card`, consuming `Uc2.quorum_intersect` directly.

---

### Task S1: Protocol model + non-vacuity

**Files:**
- Create: `proofs/Uc2Proofs/Protocol.lean`
- Modify: `proofs/Uc2Proofs.lean` (add import)

**Interfaces (produces, consumed by S2/S3):**
- `structure PNode (n : Nat)` — `currentTerm : Nat`, `votedFor : Option (Nat × Fin n)`, `role : Role` (`inductive Role | follower | candidate | leader`), `votesReceived : Finset (Fin n)`, `lastTerm durable : Nat` (havoc payload)
- `inductive Msg (n : Nat)` — `| requestVote (from : Fin n) (newTerm lastTerm durable : Nat)` `| vote (from to : Fin n) (term : Nat) (granted : Bool)`
- `structure World (n : Nat)` — `nodes : Fin n → PNode n`, `sent : List (Msg n)`
- `def World.init (n : Nat) : World n` — all followers, term 0, nothing sent
- `inductive Step : World n → World n → Prop` — constructors for: `startElection` (bump term, become candidate, vote self, send requestVote), `deliverRequestVote` (term adoption + `Uc2Model.Vote`-style grant/reject, sends vote msg; grant records votedFor BEFORE the send lands in `sent` — same step, but the state update and message append are atomic here, which is exactly V3's persist-before-send assumption), `deliverVote` (candidate collects a grant for its current term into `votesReceived`), `becomeLeader` (candidate with `n/2+1 ≤ votesReceived.card ∪ self` → leader), `crashRestart` (per decision 3), `havocData` (arbitrary new `lastTerm`/`durable`)
- `def Reachable (w : World n) : Prop` — reflexive-transitive closure of `Step` from `World.init n`

- [ ] **Step 1: Write the model.** Follow the interface above; every constructor's docstring names the Rust/`Uc2Model` behavior it mirrors (`election.rs::start_election`, `handle_request_vote`, `grant_vote`, the `votes_received` counting at `election.rs:617-634`, `become_leader`). Read `uc_consensus/src/election.rs`'s vote path first and mirror its EXACT discipline (self-vote seeds `votesReceived`; grants counted only while `.candidate` and only for the candidate's own `currentTerm`; term adoption on higher-term messages sets `.follower`).
- [ ] **Step 2: Non-vacuity examples.** In the same file: `example : ∃ w : World 3, Reachable w ∧ (w.nodes 0).role = .leader` proved by exhibiting the explicit 3-node trace (node 0 starts election → nodes 1,2 grant → node 0 collects → becomes leader). Also a smaller sanity `example` that a single `startElection` step is `Reachable`-composable. These are the model's "tests" — if the trace can't be driven through the constructors, the model is broken, not the example.
- [ ] **Step 3: Gate + commit** (`proof(proofs): phase-2 spike — N-node election protocol model + non-vacuity traces (lean S1)`).

### Task S2: Inductive invariant + election safety

**Files:**
- Modify: `proofs/Uc2Proofs/Protocol.lean` (append; or split a `ProtocolInv.lean` if it grows past ~600 lines)

**The FIXED theorem statement:**

```lean
/-- **ELECTION SAFETY** (spec §7): at most one leader per term, under message
loss/duplication/reordering, crash-restart, and an arbitrary (havoc) data
plane. -/
theorem election_safety {n : Nat} (w : World n) (hw : Reachable w)
    (i j : Fin n)
    (hi : (w.nodes i).role = .leader) (hj : (w.nodes j).role = .leader)
    (ht : (w.nodes i).currentTerm = (w.nodes j).currentTerm) : i = j
```

- [ ] **Step 1: State the invariant bundle** (a `structure Inv (w : World n) : Prop`), expected clauses (discovery may reshape them; the invariant is NOT contract):
  - term monotonicity is implicit (no clause needed — nothing reads history);
  - **vote-message discipline**: every granted `vote v c t` in `sent` has... the voter `v`'s state satisfies `t < (w.nodes v).currentTerm ∨ (w.nodes v).votedFor = some (t, c)` — "a grant in flight is still recorded, unless the voter has moved past that term, in which case it was recorded when granted"; PLUS the per-term uniqueness carrier: any two granted `vote v _ t` messages in `sent` from the same voter for the same term name the SAME candidate;
  - **votesReceived soundness**: a candidate `c` at term `t` has, for every `v ∈ votesReceived`, a granted `vote v c t ∈ sent` (or `v = c`, the self-vote);
  - **leader certification**: a `.leader` at term `t` has a `Finset` quorum of voters with granted `vote · leader t ∈ sent` (∪ self) — carried from the `becomeLeader` step;
  - **candidate self-consistency**: `votedFor = some (currentTerm, self)` while candidate/leader at that term (start_election self-votes).
- [ ] **Step 2: Prove `Inv` holds at `init` and is preserved by every `Step` constructor** (the bulk — six preservation cases; `deliverRequestVote` with term adoption is the delicate one: adopting a higher term must not break the vote-message discipline for OLD terms — the `t < currentTerm` disjunct absorbs it).
- [ ] **Step 3: Prove `election_safety`**: `Reachable → Inv` (closure induction), then two leaders at term `t` each carry a quorum; `Uc2.quorum_intersect` yields a shared voter `v`; `v ∈` both quorums gives two granted `vote v · t` messages naming each leader; the per-term uniqueness clause forces the candidates equal. If the statement resists after honest effort exceeding roughly the R4 budget: STOP, return BLOCKED with the failing case analysis — do not weaken.
- [ ] **Step 4: Axiom check** (`#print axioms Uc2.election_safety` scratch, delete after), gate, commit (`proof(proofs): phase-2 spike — election safety over the N-node model (lean S2)`).

### Task S3: Go/no-go memo

**Files:**
- Create: `docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md`
- Modify: `docs/benchmarks/uc2-lean-gate-2026-07-16.md` (Phase 2 section: point at the memo)

- [ ] **Step 1: Write the memo** from S1/S2's reports: model size, invariant clause count, proof effort per stage (wall-clock + hard lemmas), what broke and how it was fixed. Then the honest **pricing of the remaining Tier B theorems** — log-matching analog, leader completeness, state-machine safety (sim inv4+inv5) — each with: what the model must GROW (real data plane replacing havoc, term-map/byte-history state per node, reconcile-on-gossip steps, commit certification), which Phase 1 theorems slot in (R2/R4, C3, `logOk_iff`), and an estimate range with the spike's measured costs as the basis. End with a recommendation: GO (with phased theorem order) / NO-GO / GO-LATER, and the trigger conditions.
- [ ] **Step 2: Gate doc cross-link, full `lake build` + sorry gate, commit** (`docs(benchmarks): phase-2 spike memo — election safety proved, Tier B pricing (lean S3)`).

## Self-review notes
- Spec §7 coverage: model shape (per-node state, message multiset, fault model incl. crash-restart preserving durable state) → S1 (sent-set + havoc encodings documented as decisions 1-2); election safety → S2; go/no-go memo with proof-cost data → S3.
- The V3 persist-before-send assumption is discharged structurally (grant records state and appends the message in one atomic step) — noted in S1's `deliverRequestVote` docstring requirement.
- Non-vacuity is a hard deliverable (S1 Step 2), preventing the classic vacuous-safety-theorem failure.
