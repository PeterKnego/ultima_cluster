# UC v2 — Veil follow-up brief: the Reconfig commit/log plane, option (a)

**Date:** 2026-07-26
**Status:** DECIDED + dispatch brief, not started. User decision (Peter,
2026-07-26): **option (a)** — abstract-quorum reformulation + inductive proof —
resolving the "USER DECISION PENDING" item 4 of
`docs/benchmarks/uc2-veil-spike-2026-07-24.md` §6.
**Parent:** the Veil spike (gate doc above; brief
`docs/superpowers/specs/2026-07-19-uc2-veil-spike-brief.md`, Amendment-3
discipline applies). Finding under discharge: **F-M7-2** — the V-M7
`Reconfig.lean` model's `adopt` grants a config without requiring the committed
prefix, so quorum-overlap/leader-completeness properties false-positive on
data loss real UC cannot exhibit.
**Why (a) over (b):** the SAFE direction of the commit-plane state space is
exponential, not compute-bound — a bigger box (option b) buys only deeper
counterexample search, never the assurance result. The goal here IS the
assurance result, which needs induction. Explicit-state runs remain useful only
for CE calibration at small n.

## Guardrails (unchanged from the spike — non-negotiable)

- All work in `proofs-veil/` + the separate `veil-2.0-preview` checkout
  (Lean 4.28, cvc5/z3 FFI). **Never `proofs/`** — it remains the sole trusted
  base; nothing here is "the record".
- Box limits: `Fin 4` term bounds at n=3 are NOT viable on this box (12.1 GB
  RSS, no swap — kills sessions). Stay within known-affordable explicit-state
  configs (n=3 / `Fin 3` stayed ≥12.6 GB free); the inductive route is the
  point of (a) precisely because it does not pay this cost.
- The spike's standing lesson: **the checker finds the model's bugs long before
  UC's — a CE is a question, not an answer.** Every CE is adjudicated against
  the Rust before being believed; every fidelity gap goes in the ledger
  (`proofs-veil/spike-ledger.md`), F-M7-1 discipline.

## The new asset: a verified Rust ground-truth map

The §5 directed Rust checks were **discharged 2026-07-26** (gate doc §5,
CONFIRMED-SAFE ×2, main @ `e87b108`). Q2's trace is exactly the mechanism chain
the commit plane must mirror — the model refinement no longer starts from
guesses. The chain, each link with file:line in the gate doc:

1. **Contiguity**: the receiver publishes `append` only at the contiguous
   frontier (NAK-repaired, no holes); the archive records + fsyncs only that
   prefix; config frames are detected ONLY in the recorded-block walk; the
   consensus drain belt-checks `position <= durable`.
2. **Reports**: a node's quorum contribution is its durable counter = that
   contiguous frontier. Counting toward ANY config's quorum at X ⟹ holding
   every entry ≤ X.
3. **Commit**: bounded by leader-own-durable (`CommitTracker::advance
   .min(own_durable)`).
4. **Below-floor**: snapshot install substitutes committed state + the
   snapshot-carried authoritative config; `adopt_floor` is legal only on an
   empty journal; post-install reports restart at the floor.
5. **Elections**: vote solicitation/granting membership-gated on the ADOPTED
   config; `log_ok` is `(last_term, last_durable)` lexicographic with the
   Figure-8 `new_term_pos` clamp; `ClusterConfig::apply` is the unique shared
   transition (±1 voter), one change in flight (`ChangePending`).

**The modeling move whose ABSENCE caused F-M7-2 is link 1+2: adoption and
quorum-counting must be conditioned on holding the prefix.** In the abstraction
this collapses to: a node's `adopt(cfg@P)` and its report `durable = X ≥ P`
both imply possession of the committed prefix ≤ X — which the model can carry
as a per-node `log`/`durable` abstraction rather than byte streams (the LC
arc's data-plane refinement is the precedent for how much structure is enough).

## Scope

Extend `proofs-veil/models/Reconfig.lean` (or a successor file) with a
commit/log plane and prove, **inductively via `#check_invariants` (SMT, all-n)
with abstract quorums** — the Bar-1 idiom, not `#model_check`:

- **P2 (the target): leader completeness across reconfiguration** — an elected
  leader of any config holds every committed entry, where "committed" is
  quorum-durable under the config in force at commit time.
- **P1 (repair of the false positive):** the old `quorum_overlap` property is
  *expected* to stay false as stated (single-server change deliberately
  permits non-adjacent-config quorum disjointness); the repaired statement is
  P2, not a patched P1. Do not chase P1 green.
- **Election safety re-verified** in the extended model (it held even ablated;
  it must not regress under the added plane).

Abstract quorums: the `member` + intersection-assumption idiom (Lean C5) over
the CURRENT config, plus a proved lemma that ±1 single-server change gives
consecutive-config quorum intersection (from `apply`'s shape + one-in-flight) —
adjacency as a theorem, not an axiom.

Also carry (cheap, from the spike's open items): the **run-2 narrowing lift**
(per-term stream identity — a second tracked entry or an `entryTerm`) IF the
commit plane naturally provides it; do not force it.

## Bars (Amendment-3 style — checkpoint, then commit)

1. **Calibration first:** the extended model WITHOUT the adoption-prefix
   coupling must exhibit the F-M7-2-shaped loss as a CE (`#model_check`,
   n=3 / affordable Fin bounds) — proving the plane is expressive enough to
   see the class it guards against.
2. **Re-gate checkpoint** after (1): model built + CE reproduced + fidelity
   ledger entries written. Stop and re-decide scope with the user before the
   proof push (the spike's session-1 re-gate discipline; the LC arc's cost
   history — 13.3 S2-eq vs 5-8 estimated — is the cautionary anchor).
3. **The proof:** with the coupling, `Inv` clauses + P2 inductive, all-n, via
   cvc5. Any CTI adjudicated against the Rust chain above before the model is
   "fixed" to make it pass.
4. Fidelity ledger updated per F-M7-1; gate doc §6 item 4 closed with the
   outcome either way.

**Cost anchor:** ~one LC-arc task S2-equivalent (NOT the whole arc). If the
checkpoint suggests otherwise, that is what the checkpoint is for.

## What this is NOT

- Not a change to `proofs/` or any shipped claim.
- Not a Rust change: Q1/Q2 are already CONFIRMED-SAFE; if a genuine CTI
  survives Rust adjudication, that is a **finding**, handled by the spike's
  "any hit → Rust" rule, not silently modeled away.
- Not the nightly Veil CI job (separate follow-up, still optional).
