# proofs-veil/ — Veil bug-hunting scratch (NOT the record)

Archived artifacts from the Veil spike (brief:
`docs/superpowers/specs/2026-07-19-uc2-veil-spike-brief.md`). Veil
(`verse-lab/veil`, CAV 2025) is used here as a **bounded explicit-state model
checker for bug-finding and design assurance** — never as a proof of record.

## Hard guardrails (from the brief — preserve the repo's trust story)

1. **Veil is never the record.** Permanent proofs live in `proofs/` (Lean
   v4.32.0, standard axiom trio, no SMT in the trusted base). Veil's only
   deliverables are **countermodel traces** (→ directed `uc2_sim` regressions +
   Rust fixes) and, secondarily, candidate invariant text. A Veil model has **no
   conformance rig**; it is scratchpad-only. A bug it finds is reconfirmed
   independently in Rust before any fix.
2. **Toolchain isolation.** These models target Veil's `veil-2.0-preview`
   branch on **Lean v4.28.0** — incompatible with `proofs/`'s v4.32.0. They are
   **never** on any `proofs/` `lake build` path, CI gate, or "proved" claim.
   They do not build in this repo; they are run inside a separate
   `veil-2.0-preview` checkout (where cvc5/z3 FFI links). These files are kept
   here purely as an auditable archive of what was checked.
3. **No migration of anything proved.** election_safety, log_matching, the LC
   support stack are done in `proofs/`; Veil touches none of it.

## Contents

- `models/Election.lean` — UC S2 election plane, abstract quorum + intersection
  assumption (Lean C5). `#check_invariants` (SMT) certifies the 5-clause `Inv` +
  `election_safety` inductive, all-n. (Bar-1, session 1.)
- `models/ElectionMC.lean` — same, concrete "excludes-one" majority (n≥3).
  `#model_check {Fin 3, Fin 3}` → ✅ no violation, 60761 states. (Explicit-state
  engine confirmed on the UC model, session 2.)
- `models/Reconfig.lean` — **V-M7**: UC single-server reconfiguration. Config as
  an evolving per-node voter `nodeSet` (the `TLA/Raft.lean` `isQuorum`
  cardinality idiom applied to a *changing* set); single-server change via
  `insert`/`remove`; one-in-flight; term-coupled adoption; `adjacencyGuard`
  toggle. Safety: `election_safety` + `quorum_overlap`.
- `logs/` — the three decisive `#model_check` runs (see the gate doc).
- `spike-ledger.md` — the running SDD ledger across all sessions.

## Results (see `docs/benchmarks/uc2-veil-spike-2026-07-24.md`)

- **V0/V1 + Bar-1**: PASS (sessions 1–2). Both Veil engines confirmed on the UC
  model: SMT-inductive (all-n) and explicit-state-safe (60761 states, n=3).
- **V-M7** (session 3): `election_safety` robustly SAFE across reconfig (even
  ablated, 187907 states); the checker rediscovers the textbook disjoint-quorum
  data-loss shape (calibration ✓); Finding **F-M7-2** identifies that a faithful
  leader-completeness check needs a commit/log plane coupling config adoption to
  the committed prefix — the M7 analog of the LC arc's data-plane refinement.
