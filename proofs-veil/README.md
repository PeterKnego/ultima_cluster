# proofs-veil/ — Veil bug-hunting scratch (NOT the record)

Archived artifacts from the Veil spike (brief:
`docs/superpowers/specs/2026-07-19-uc2-veil-spike-brief.md`). Veil
(`verse-lab/veil`, CAV 2025) is used here as a **bounded explicit-state model
checker for bug-finding and design assurance** — never as a proof of record.

## Hard guardrails (from the brief — preserve the repo's trust story)

1. **Veil is never the record.** Permanent proofs live in `proofs/` (Lean
   v4.32.0, standard axiom trio, no SMT in the trusted base). Veil's only
   deliverables are **countermodel traces** (→ directed `uc_sim` regressions +
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
- `models/ReconfigCommit.lean` — **the commit/log plane over the reconfig model**
  (option-(a) arc, session 1 — brief
  `docs/superpowers/specs/2026-07-26-uc2-veil-reconfig-commit-plane-brief.md`):
  entry-level `holdsE`/`committed` plane mirroring the discharged Q2 Rust chain,
  `prefixCoupling` knob (the F-M7-2 mechanism). Calibration: coupling OFF →
  ❌ `leader_completeness` at depth 13 (the F-M7-2 shape); coupling ON → ✅ clean
  through the same horizon (4,211,943 states — bounded, not a proof) + canary
  witnessed + election-safety regression clean (9,160,143 states, uncoupled,
  d14). Checkpoint memo:
  `docs/benchmarks/uc2-veil-commit-plane-checkpoint-2026-07-26.md`.
- `models/ReconfigCommitSMT.lean` — the abstract-quorum (C5) sketch for the
  inductive route: definitions + P2 + seed invariant clauses; the ±1 adjacency
  lemma stated as a marked TO-BE-PROVED obligation. Elaborates green;
  `#check_invariants` deliberately not yet run (next session).
- `logs/` — the decisive `#model_check` runs (see the gate doc + checkpoint memo).
- `spike-ledger.md` — the running SDD ledger across all sessions.

## Reproducing a run (there is deliberately NO CI job — see below)

Guardrail 2 means nothing here builds in this repo or on any CI gate, so
re-running a model is a manual, on-demand act. The recipe (extracted from
`spike-ledger.md`'s V0 section, which is the primary record):

1. Clone `verse-lab/veil` at branch **`veil-2.0-preview`** — the ONLY branch with
   the explicit-state `#model_check` reachability engine. `main` pins Lean 4.24
   and has SMT (`#check_invariants`) only. The spike used
   `/home/claude/veil-spike/veil-preview`, which is what
   `scripts/runmod.sh` hardcodes.
2. `lake exe cache get` (mathlib, ~8010 files), then `lake build` bounded to
   `-j3` — the full build is ~1418 jobs and this box has no swap.
3. Expect ONE failure: the npm infoview widget (browser trace visualization, not
   core). Work around it offline by stubbing
   `.lake/build/js/{RefreshComponent,traceDisplay,verificationResults}.js` and
   commenting `needs := #[widgetJsAll]` in the lakefile. Core + ModelChecker are
   unaffected; the text trace — the artifact that matters — prints fine.
4. Copy the model under `Examples/UC/` in that checkout and run it with
   **`lake build Examples.UC.<Module>`** (`scripts/runmod.sh` does exactly this).
   NOT `lake env lean`: `#model_check` runs at elaboration and needs the cvc5/z3
   `.so` FFI under `.lake/packages/`, which the interpreter misses
   (`cvc5.TermManager.new` symbol error).
5. Watch memory. Peak `lean` RSS on decisive explicit-state runs was ~5.7 GB and
   this box has no swap (see `CLAUDE.md`); the spike ran an active memory-watch
   that killed on <2.5 GB free. Logs belong on real disk, never `/tmp`.

**Why no nightly job** (adjudicated 2026-07-28, closing the gate doc's §6 item 5):
a Veil model has no conformance rig by construction (guardrail 1), so it cannot
detect Rust drift — the thing a nightly gate exists to catch. What it WOULD catch
is rot in an external preview-branch toolchain, at the price of building veil +
mathlib + FFI on every run and standing an isolation guardrail down. The
`lean-proofs` nightly job already covers model-vs-Rust drift for the trusted base,
because `proofs/` DOES have a conformance rig. On-demand reproduction, per the
recipe above, is the right cadence for a scratch instrument.

## Results (see `docs/benchmarks/uc2-veil-spike-2026-07-24.md`)

- **V0/V1 + Bar-1**: PASS (sessions 1–2). Both Veil engines confirmed on the UC
  model: SMT-inductive (all-n) and explicit-state-safe (60761 states, n=3).
- **V-M7** (session 3): `election_safety` robustly SAFE across reconfig (even
  ablated, 187907 states); the checker rediscovers the textbook disjoint-quorum
  data-loss shape (calibration ✓); Finding **F-M7-2** identifies that a faithful
  leader-completeness check needs a commit/log plane coupling config adoption to
  the committed prefix — the M7 analog of the LC arc's data-plane refinement.
