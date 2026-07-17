# UC v2 Lean Tier B(a) — Log-Matching Sub-Spike Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the spike's havoc data plane with a real (payload-carrying) data plane and prove the **log-matching analog** — same position + same term-stamp ⇒ same content — the vocabulary theorem the rest of Tier B (leader completeness, state-machine safety) consumes. Then re-gate with measured costs, per the Phase 2 memo's phased-GO.

**Architecture:** Extend `Uc2Proofs/Protocol.lean`'s world (or a new `ProtocolData.lean` layered on it) with per-node payload histories, leader-append and contiguous-replication steps, term-map growth (`DataTermObserved` analog) and reconcile-on-gossip truncation — consuming Phase 1's `TermMap`/`reconcile` model and the proved `election_safety`. Memo: `docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md` §3(a); spec §7.

**Tech Stack:** Lean v4.32.0 + mathlib (pinned, warm), existing `proofs/` package.

## Global Constraints

- The LM-core theorem statement (below) is the FIXED minimum contract; the prefix-form is a declared stretch goal, allowed to be dropped with an honest memo note. Weakening LM-core = controller escalation.
- Sorry-free, standard axioms, `#print axioms` verified.
- `Uc2Model/` untouched (its kernels are consumed, not modified). The spike's `Protocol.lean`/`ElectionSafety.lean` may be EXTENDED (new fields/constructors) only if `election_safety` is re-proved green under the extension in the same task — never weakened; layering a new file over the old model is the lower-risk default.
- Every new step constructor's docstring names the Rust behavior it mirrors (node.rs/election.rs/reconcile.rs).
- Non-vacuity: a named theorem must drive a trace in which a leader appends, a follower replicates the bytes, and both hold the same stamped payload — the LM hypotheses must be genuinely reachable.
- Gate before every commit: `cd proofs && lake build && ! grep -rn --include='*.lean' -w 'sorry' Uc2Model Uc2Proofs Conform`.
- Proof-cost accounting in every report (feeds the re-gate).
- No heavy artifacts to `/tmp`; stage only your own files.

## Settled design decisions

1. **Payloads are real.** The spike's stamps-only abstraction is too abstract for log-matching (stamp-equality ⇒ content-equality would be vacuous). Per-node history becomes `hist : Nat → Option (Nat × Nat)` — position ↦ `(term-stamp, payload)`; `durable` bounds the defined prefix (contiguity is an invariant, not a type constraint).
2. **Single-tenure is load-bearing and free.** A node's `currentTerm` is strictly monotone across `startElection` and adoption, so a term-`t` leadership can never resume after being lost — the t-writer's append sequence is a single tenure. The LM proof leans on `election_safety` + this.
3. **Leader append**: a `.leader` at term `t` appends `(t, v)` at its own append frontier for arbitrary `v : Nat`, advancing the frontier. Model leader-durable vs in-flight minimally: appends go to the leader's own hist immediately (the log buffer IS the leader's history; archive-fsync lag is modeled by `durable` trailing the append frontier if the designer finds it necessary for fidelity — designer's call, documented).
4. **Replication is contiguous at the receiver**: a follower accepts `(pos, t, v)` only at exactly its own frontier (UC streams contiguously from the follower's durable; NAK repair preserves this). Sent-set semantics as before: append/replicate messages live in `sent`, delivered any number of times, in any order — the contiguity guard makes reordered/duplicated deliveries no-ops rather than corruption, mirroring the real receiver.
5. **Truncation enters as reconcile-on-gossip**: the leader gossips its term map; a follower applies `Uc2Model.reconcile` (the PROVED kernel — R1/R2/R3/R5 available) and truncates hist/durable to `validUpTo`. This is where divergent tails die, exactly as in UC.
6. **Term-map growth mirrors the Rust**: leader-side entry on `becomeLeader` (with the phantom prune! — model the pruned push, matching post-Finding-#3 `election.rs`), follower-side `DataTermObserved` analog when replication first delivers a byte of a new term.
7. **LM-core (FIXED)** over the extended `Reachable`:

```lean
theorem log_matching {n : Nat} (w : World n) (hw : Reachable w)
    (i j : Fin n) (p : Nat) (t vi vj : Nat)
    (hi : (w.nodes i).hist p = some (t, vi))
    (hj : (w.nodes j).hist p = some (t, vj)) : vi = vj
```

   **Prefix-form (STRETCH)**: same `(p, t)` on both nodes ⇒ their histories agree at every `q ≤ p` within term `t`'s span (the contiguity + single-tenure consequence; drop with a memo note if it blows the budget).

---

### Task LA1: Data-plane model + non-vacuity

**Files:**
- Create: `proofs/Uc2Proofs/ProtocolData.lean` (layered extension; or modify `Protocol.lean` per Global Constraints if layering proves worse — document the call)
- Modify: `proofs/Uc2Proofs.lean` (import)

- [ ] **Step 1**: Read the Rust ground truth: `uc2_log`'s appender/receiver contiguity (module docs suffice), `uc2_consensus/src/election.rs` `become_leader` (post-prune form) + `DataTermObserved` arm, `uc2_consensus/src/reconcile.rs` (the kernel you re-use via `Uc2Model.reconcile`), and node.rs's reconcile-then-truncate action flow (grep `Action::Truncate`). Also re-read the spike model + S1 report.
- [ ] **Step 2**: Design + write the extended model per decisions 1–6. Prefer layering: a `DNode` wrapping `PNode` + `hist`, a `DWorld`, a `DStep` that embeds the election `Step` on the `PNode` projections (so `election_safety` lifts by projection rather than re-proof) — if embedding is awkward, extending `Protocol.lean` in place + re-running the S2 proof is acceptable per Global Constraints. Document the choice and why.
- [ ] **Step 3**: Non-vacuity theorem: leader elected (reuse the S1 trace shape) → appends `(t, 42)` at 0 → follower replicates → both `hist 0 = some (t, 42)`. Named theorem, `decide`-discharged where possible.
- [ ] **Step 4**: Gate + commit (`proof(proofs): tier-B(a) — payload data plane over the election model + non-vacuity (lean LA1)`).

### Task LA2: Invariant + log_matching

**Files:**
- Modify/Create: `proofs/Uc2Proofs/ProtocolData.lean` or a `LogMatching.lean` split

- [ ] **Step 1**: State LM-core verbatim (decision 7) with `sorry`; build.
- [ ] **Step 2**: Discover + prove the invariant. Expected shape (discovery may reshape): a per-term ghost coherence clause — for each term `t`, all `(p, t, ·)` entries anywhere (node hists AND in-flight replicate messages in `sent`) agree with the term-`t` leader's write sequence; carried via election safety (unique `t`-leader) + single-tenure monotonicity + append-frontier discipline + contiguity + reconcile-only-truncates (truncation removes entries, never rewrites — R-series lemmas apply). Preservation per constructor; the reconcile-on-gossip case consumes Phase 1's `reconcile` theorems.
- [ ] **Step 3**: Attempt the prefix-form stretch ONLY if LM-core landed within ~2 S2-equivalents of effort; otherwise record the honest drop.
- [ ] **Step 4**: Axiom check (LM + re-check `election_safety` still green under any model change), gate, commit (`proof(proofs): tier-B(a) — log-matching analog proved (lean LA2)`).

### Task LA3: Re-gate memo update

**Files:**
- Modify: `docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md` (append a "Tier B(a) actuals + re-gate" section)
- Modify: `docs/benchmarks/uc2-lean-gate-2026-07-16.md` (Phase 2 pointer freshened)

- [ ] **Step 1**: Record LA1/LA2 measured costs vs the memo's (a) estimate (3–7 S2-equivalents / 3–5 sessions); re-price (b) leader completeness and (c) state-machine safety with the new data point; restate the GO/no-go recommendation for (b) with trigger conditions.
- [ ] **Step 2**: Gate, commit (`docs(benchmarks): tier-B(a) actuals + re-gate (lean LA3)`).

## Self-review notes
- Memo §3(a) coverage: model growth (payloads, append, replication, term-map growth, reconcile-on-gossip) → LA1 decisions 1–6; the theorem → LA2 (LM-core fixed, prefix stretch); re-gate → LA3.
- `election_safety` preservation under model extension is an explicit Global Constraint with two sanctioned routes (projection-lift or re-prove).
- Phase 1 reuse is structural: `Uc2Model.TermMap`/`reconcile` as the truncation kernel (decisions 5–6), R-series theorems available to LA2.
