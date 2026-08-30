# UC v2 Lean Tier B(b) — Leader Completeness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Model UC's commit certification (the `CommitTracker` quorum rank, durable reports, commit events) over the Tier B(a) data plane and prove **leader completeness** — an elected leader holds every committed entry — the inv5-analog and the deepest theorem of the Raft-safety family.

**Architecture:** Extend `Uc2Proofs/ProtocolData.lean`'s world (layered, as LA1 did) with per-leader tracker state driven by the PROVED `Uc2Model.CommitTracker` kernel (C1–C5 available), durable-report messages, and an append-only ghost `committed` ledger written at tracker-advance steps. The proof composes election safety + log-matching + `Cert` (all proved) with the vote-freshness rule (`logOk` finally load-bearing) via the standard minimal-term-counterexample induction, mutually inductive with committed-never-truncated (the inv4-analog, via R2). Re-gate memo: `docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md` (Tier B(a) actuals section priced this at 3–6 S2-equivalents).

**Tech Stack:** Lean v4.32.0 + mathlib (pinned, warm), existing `proofs/` package.

## Global Constraints

- The LC-core statement (below) is the FIXED minimum contract (`t < currentTerm` form); proving the stronger `t ≤ currentTerm` form is welcome. Weakening = controller escalation. The mutual invariant (committed-never-truncated analog) is discovery — reshape freely, but if it must be SURFACED as a standalone theorem to make the induction go, name it `committed_never_truncated` and treat its statement as reviewable output.
- Sorry-free, standard axioms, `#print axioms` verified for every public theorem, and `election_safety` + `Data.election_safety` + `log_matching` must remain green.
- `Uc2Model/` untouched (its `CommitTracker` is CONSUMED — like `reconcile` in LA1). `Protocol.lean`/`ElectionSafety.lean`/`ProtocolData.lean`/`LogMatching.lean` may be extended only under the LA1 rules (election model changes require the S2 proof re-green in the same task; layering preferred).
- Every new step constructor's docstring names its Rust source (`commit.rs`, `election.rs` report/advance arms, node.rs commit-gossip).
- Non-vacuity: a named theorem must drive a trace to a genuine commit event (quorum of reports, tracker advance) followed by a NEW leader election whose winner provably holds the committed entry — the LC hypotheses must be reachable, including the term change.
- Gate before every commit: `cd proofs && lake build && ! grep -rn --include='*.lean' -w 'sorry' Uc2Model Uc2Proofs Conform`.
- Proof-cost accounting in every report (feeds the re-gate).
- No heavy artifacts to `/tmp`; stage only your own files.

## Settled design decisions

1. **Commit is an event, recorded in an append-only ghost.** `DWorld` gains `committed : List (Nat × Nat × Nat)` (position, term-stamp, payload). A `leaderAdvanceCommit` step fires when the leader's tracker advance (the REAL `Uc2Model.CommitTracker.advance`, fed by report messages, clamped by own durable — C2's `min`) crosses a position; it appends every newly-committed `(p, t, v)` from the leader's own hist. Ghost-append-only makes committed-stability trivial; the CONTENT claims (quorum holds it; never truncated; later leaders have it) are the theorems.
2. **Reports are messages.** Followers emit `report (from) (durable)` messages (the `AppendPosition` analog) reflecting their durable at send time; the leader folds them into its tracker via `deliverReport` (per-slot monotone — `onDurable`). Tracker state lives in `DNode` (only meaningful while leader); `resetReports` on term transitions per C4 (find the Rust site: `become_leader`'s reset + rebuild; mirror it).
3. **A report never overstates**: the enabling condition ties the report message's durable to the sender's ACTUAL durable at emission. (In UC the receiver's AppendPosition is its fsynced frontier — the report is truthful by construction.) In-flight staleness (reports arriving after the sender truncated) is the adversarial case the proof must survive — do NOT guard it away; UC survives it via term-scoped reports (C4 reset) and the commit-quorum argument. If the proof CANNOT survive some stale-report interleaving, that is a FINDING (candidate real gap) — stuck-protocol, escalate with the trace.
4. **Commit-gossip is optional scope**: followers learning the commit index is NOT needed for leader completeness (the theorem is about the leader's hist). Include a `commitGossip` message + follower commit field ONLY if the invariant needs it; prefer omitting (YAGNI — it's (c)'s vocabulary).
5. **LC-core (FIXED)**:

```lean
theorem leader_completeness {n : Nat} (w : World n) (hw : Reachable w)
    (p t v : Nat) (hc : (p, t, v) ∈ w.committed)
    (i : Fin n) (hi : (w.nodes i).pn.role = .leader)
    (ht : t < (w.nodes i).pn.currentTerm) :
    (w.nodes i).hist p = some (t, v)
```

(Adjust field paths to the actual DNode shape; `≤` form welcome as a strengthening.)

6. **Expected proof skeleton** (guidance, not contract): commit event ⇒ at that step, a quorum Q each reported durable > p under the commit term (tracker fed post-reset reports only — C4 semantics) AND (via log_matching + the leader's own hist) each member of Q durably held `(p, t, v)`. For a later leader L at term t' > t: its `Cert` vote quorum intersects Q (`quorum_intersect`); the shared voter v granted with `logOk (lastTerm, durable) ≤ L's` — the minimal-term-counterexample induction (strong induction on t': every leader with term in (t, t') is complete at p, so no reconcile-gossip between t and t' ever truncated v's copy — R2 shared-prefix preservation with the gossiping leader's own completeness) keeps v's copy intact at grant time, and `logOk` forces L's frontier/term high enough that L itself holds the entry (L's hist at p: either it held it as a Q-member/replica, or its election was impossible — the case analysis where `logOk`'s lexicographic order does the work). Expect this to be the hardest invariant-engineering of the arc; the mutual induction may need the invariant to carry "every PAST commit event's quorum still holds the entry" as a clause.

---

### Task LB1: Commit machinery model + non-vacuity

**Files:**
- Create: `proofs/Uc2Proofs/ProtocolCommit.lean` (layered over ProtocolData; or extend ProtocolData.lean under the LA1 rules — document the call)
- Modify: `proofs/Uc2Proofs.lean` (import)

- [ ] **Step 1**: Read the Rust ground truth: `uc_consensus/src/commit.rs` (whole file — the kernel you consume), `election.rs`'s report arm + tracker wiring (`on_durable` feed sites, `become_leader`'s `reset_reports` + `last_reports.clear`), node.rs's AppendPosition flow (module-doc level). Re-read `proofs/Uc2Model/Commit.lean` (the kernel's Lean port + its C-theorems in `Uc2Proofs/Commit.lean`).
- [ ] **Step 2**: Design + write the extension per decisions 1–4. Projection to the Tier B(a) world must be proved FIRST (LA1's pattern): commit steps are stutters on the data+election planes ⇒ `election_safety` and `log_matching` lift for free.
- [ ] **Step 3**: Non-vacuity theorem per Global Constraints (commit event + later-term election + winner holds the entry), `decide`-discharged where possible.
- [ ] **Step 4**: Gate + commit (`proof(proofs): tier-B(b) — commit certification machinery + non-vacuity (lean LB1)`).

### Task LB2: The invariant + leader_completeness

**Files:**
- Create: `proofs/Uc2Proofs/LeaderCompleteness.lean` (+ import)

- [ ] **Step 1**: State LC-core verbatim (decision 5) with `sorry`; build.
- [ ] **Step 2**: Discover + prove the invariant (decision 6 is the sketch; the mutual committed-never-truncated clause is expected). Consume: `quorum_intersect`, `log_matching`, `Cert`/`cert_blocks_candidate`, the C-theorems (`advance_certified` for the commit-event quorum), R2 (`reconcile_preserves_shared_prefix`) for the never-truncated half, `logOk_iff` (V2) for the freshness endgame.
- [ ] **Step 3**: STUCK-PROTOCOL: honest sustained effort up to ~6 S2-equivalents (the re-gate memo's upper bound); if LC-core resists: STOP, BLOCKED with the failing case — especially if a stale-report or truncation interleaving looks like a REAL protocol gap (decision 3): that would be Finding #5 territory and goes to the controller with the trace, not into a weakened theorem.
- [ ] **Step 4**: Axiom check (LC + all prior theorems re-verified), gate, commit (`proof(proofs): tier-B(b) — leader completeness proved (lean LB2)`).

### Task LB3: Re-gate memo update

**Files:**
- Modify: `docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md` (append "Tier B(b) actuals + re-gate")
- Modify: `docs/benchmarks/uc2-lean-gate-2026-07-16.md` (Phase 2 pointer freshened)

- [ ] **Step 1**: Record LB1/LB2 measured costs vs the 3–6 S2-equivalent estimate; findings (any stale-report/truncation discoveries, invariant shape, whether committed_never_truncated surfaced as a standalone theorem); re-price (c) state-machine safety (now mostly composition: inv2-analog prefix consistency from log_matching + LC + the commit ghost); recommendation for (c) with triggers. Attribution discipline (own the analysis; cite only named artifacts).
- [ ] **Step 2**: Gate, commit (`docs(benchmarks): tier-B(b) actuals + re-gate (lean LB3)`).

## Self-review notes
- Memo §re-price(b) coverage: commit certification (decisions 1–3) → LB1; logOk load-bearing + Cert reuse → LB2 skeleton; the (c) re-price → LB3.
- Decision 3 deliberately REFUSES to guard away stale reports — the LA1 lesson (a guard is only admissible when it mirrors a real Rust gate; stale reports have no such gate, C4's reset is the real mechanism and must carry the proof).
- Commit-gossip explicitly deferred to (c) (decision 4, YAGNI).
