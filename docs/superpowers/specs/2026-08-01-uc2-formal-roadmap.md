# UC v2 — formal verification roadmap (post-Veil-spike, post-durable-split)

**Date:** 2026-08-01
**Status:** RECOMMENDATION — not started (except the noted in-flight brief).
Dispatch brief for future sessions; each numbered task is self-contained
enough to pick up cold, with pointers to the artifacts it builds on.
**Origin:** a cross-repo assessment comparing this repo's two formal arcs
(the Lean track in `proofs/`, the Veil spike in `proofs-veil/`) and ranking
what remains. Headline conclusions the ranking rests on:

- The **Lean track** has driven four real shipped-bug fixes (#5 phantom
  commit, #6b Figure-8 acked-write loss, #9 intake-gate reopen, #12
  durable-read collapse — the last on 2026-07-30) and produced ~17k lines of
  sorry-free theorems. Its bug yield comes from **code-tight models +
  adversarial invariant articulation**; when a proof refuses to close, the
  reason has repeatedly been a real Rust bug. It remains the high-ROI track.
- The **Veil spike** (2026-07-19 → 07-28, BANKED at gate 2) found zero UC
  bugs but delivered: calibration on all four known bugs, an exhaustive
  11.7M-state no-fifth-bug result at n=3/Fin 3, a capacity envelope for the
  box, and a residue (`P2` cross-config holder supply) that is
  **structurally unprovable in Veil's SMT fragment** (no well-founded
  induction on the config chain — ledger item 73). Its verdicts are bounded
  and are NOT the record; `proofs/` is the sole trusted base. **Do not
  reopen the Veil arc**; its residue closes in Lean (Task F-UC-2).
- Rule of thumb carried forward: **model checking is a scout, theorem
  proving is the siege engine.** Both arcs' failure modes were asking one to
  do the other's job.

**⚠ IN FLIGHT (do not collide):** the term-map-to-SM brief
(`docs/superpowers/specs/2026-07-31-uc2-lean-term-map-to-sm-brief.md`,
issue #7 role (d)) — step-0 refactors landed 07-31/08-01
(`d4f04ff`, `64d519e`); the main model change is scoped, not started. Any
session touching `Uc2Proofs/ProtocolData.lean` / `ProtocolCommit.lean` must
read that brief first.

---

## Ground state (verified 2026-08-01)

- `proofs/` (`Uc2Model/`, `Uc2Proofs/`, `Conform/`): sorry-free, standard
  axiom trio, ~17k lines. Proved: 14 Phase-1 kernel theorems, N-node
  `election_safety` (both model levels), `Uc2.Data.log_matching`,
  `frames_current_authored` (discharged unconditionally in the B(b) closure
  arc), the `PNode.durable` split + durable-skew lifted to WORLDS (issue #7,
  07-30). Conformance rig replays 170k real-Rust vectors bit-for-bit.
- **The one named open theorem:** `leader_completeness` is reduced to the
  `canon` invariant (see Task F-UC-1).
- Permanent records: `docs/benchmarks/uc2-lean-gate-2026-07-16.md` (Phase 1
  + findings ledger #1–#12), `docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md`
  (Phase 2 + Tier B(a)/(b) actuals, closure arc, **joint-induction
  blueprint**, (c) re-price), `proofs-veil/spike-ledger.md` (the full Veil
  arc, items 1–74), `docs/benchmarks/uc2-veil-commit-plane-checkpoint-2026-07-26.md`
  (gate-2 dossier incl. the citable claim).

---

## Task F-UC-1 — close `leader_completeness`: the `canon` obligation

**Priority: HIGHEST. Estimated ≈7–12 S2-equivalents / 3–4 tasks** (the
closure arc's own re-price, on record). This is the sole named open theorem
in the corpus; closing it completes the Raft safety triad and retires the
last conditionality.

**Ground state.** The B(b) closure arc (LC1–LC4h, banked 2026-07-19, merged)
landed: the assembly, the crux, the canon statement + consumer interface,
antitonicity, and a machine-checked `k>0` satisfiability witness — files
`Uc2Proofs/LeaderCompleteness.lean`, `Uc2Proofs/LcClosure.lean`,
`Uc2Proofs/CanonWitness.lean`. What remains is proving `canon` itself
inductive.

**The known blocker, precisely (Finding #11, gate doc):** the corpus's
standard single-`ReflTransGen`-shell induction **provably cannot** reach
canon's monotone-forward antecedent's newly-born instances — canon needs
**joint / well-founded induction**. Finding F-A confirmed this is a scope
limit of the proof shape, not falsity of the statement (no countermodel
exists). The **joint-induction blueprint** is written out in the phase-2
memo's "Tier B(b) CLOSURE ARC" section — start there, not from scratch; it
also records the kernel-cost traps hit en route.

**Approach notes.** Expect the durable-split (issue #7) and, once landed,
the term-map-to-SM change to have shifted some hypotheses — rebase the
blueprint against current `ProtocolCommit.lean` before writing tactics. The
Veil coda's case analysis (ledger items 72–73) is an independent map of the
same difficulty (its proved cases (a)/(b)/(c)-adjacent mirror the easy
branches; its open gap-case is the canon shape) — useful as a cross-check
that the case split is exhaustive.

**Exit criteria.** `leader_completeness` unconditional, sorry-free,
`#print axioms` = trio; non-vacuity trace retained; gate-doc section
appended to the phase-2 memo with actuals vs the 7–12 S2-eq estimate.

---

## Task F-UC-2 — M7 reconfiguration safety in Lean proper

**Priority: HIGH — the youngest consensus code with no unbounded theorem.**
Membership change is historically where Raft implementations break (the
single-server-change bug class is in the Raft literature itself). M7's only
formal evidence today is bounded: Veil verdicts at n=3 plus the banked
55-invariant SMT bundle. This task converts the Veil arc's sunk cost into a
theorem — the bundle is effectively a **proof sketch awaiting a prover with
induction**.

**What to port (all under `proofs-veil/`, read-only inputs — never build on
them, rebuild in `Uc2Proofs/`):**

- `models/ReconfigCommitSMT.lean` — the abstract-quorum commit-plane bundle:
  55 invariant clauses + 2 safeties, 35 `require`s / 11 `assumption`s.
  Gate 2 (ledger, "GATE 2" section) ratified its methodology; its 11
  `assumption`s are exactly the proof obligations Lean must discharge —
  first among them `adjacent_cfg_quorum_intersection` (stated as an
  assumption there; must become a theorem here; the arc's route r1 =
  instantiate over concrete majorities).
- The **residue** — `P2` (leader completeness under reconfig) open at
  `becomeLeader` and `commitEntry`, unified by the coda (item 74) into ONE
  gap: **cross-config holder supply** — a holder of the committed entry
  inside the election quorum when `elecCfg L` is ≥2 `succCfg` steps above
  the committing config. Item 73 shows why Veil can't close it: adjacency
  bridges exactly one step, and chain induction needs well-foundedness the
  fragment can't express. **In Lean, `succCfg` chains are inductively
  defined — the induction is free.** That is the whole point of the port.
- The Rust-fidelity ground truth: the discharged §5 Q2 mechanism chain in
  the gate-2 dossier (adopt-requires-committed-prefix; commit counts only
  C_new-majority `hasAdopted` witnesses; vote granting membership-gated on
  the voter's adopted config — ledger items 12–13 record the two fidelity
  gaps and their Rust anchors). Model these as steps, not assumptions.

**Scope.** Extend the existing protocol model (`ProtocolCommit.lean` layer)
with config-as-log-entries + single-server `succCfg`, or build a dedicated
reconfig layer that imports the election/commit machinery — decide after
F-UC-1 lands (canon's final shape affects which is cheaper). Prove:
`election_safety` under reconfig, and `leader_completeness`'s reconfig
analog (the P2 statement). Anti-vacuity: a named trace exercising a
config change followed by a commit and an election in the new config.

**Cost anchor.** The Veil arc priced the abstract-quorum inductive route at
"~LC-arc S2-equivalent effort" (ledger, session-3 close). Expect Tier-B(b)-
like overrun risk; timebox and re-gate per the house pattern.

**Exit criteria.** Sorry-free reconfig safety theorems; every one of the 11
ported `assumption`s either proved or explicitly re-recorded as a modeling
axiom with a Rust anchor; conformance extended if `uc2_consensus` grows a
pure reconfig kernel worth vectoring.

---

## Task F-UC-3 — Tier B(c): state-machine safety

**Priority: MEDIUM-HIGH, gated on F-UC-1.** The guarantee UC actually sells:
every node applies the same command at the same byte position. The honest
re-price and its gating on finishing (b) are in the phase-2 memo's "Tier
B(b) actuals + re-gate" section. Builds directly on the commit plane +
`leader_completeness`; with canon closed this is mostly assembling
log-matching + completeness into the apply-level statement, plus modeling
the service's `min(commit, durable)` apply rule and the position-keyed
idempotency contract (`apply` keyed on position — see CLAUDE.md code
conventions). Do not start before F-UC-1; the closure arc already measured
that ordering as the cheap direction.

---

## Task F-UC-4 — `loom` on the ring buffers + cnc counter protocol

**Priority: MEDIUM value, LOW cost — CI tooling, not a proof campaign. Not
Lean.** ~6,700 lines of lock-free code (`uc_protocol/src/ring/{spsc,mpsc,
broadcast,futex,common}.rs`) plus the counter-coordinated four-agent
protocol, and **the workspace has no `loom` at all today** (verified
2026-08-01). Every theorem above assumes these primitives are correct;
relaxed-atomics bugs are invisible to ordinary tests on x86 (TSO hides
reorderings that ARM will exhibit).

**Scope.** Add `loom` as a dev-dependency behind `--cfg loom`; write
exhaustive-interleaving tests for: SPSC produce/consume with wraparound;
MPSC multi-producer claim; Broadcast reader-overrun detection; the
atomic-after-write length-prefix torn-record protection (reader sees
length=0 ⇒ spin — this is the repo's standing framing convention and the
single most load-bearing ordering claim); and the log-buffer
appender-vs-recorded overrun gate if extractable. Model `memmap2`-backed
atomics as plain `loom` atomics (the mapping is what loom can't see —
document that as the abstraction obligation). Optionally `kani` for index
arithmetic (wraparound, power-of-two masks).

**Exit criteria.** `loom` suite green in CI (a scheduled job is fine — loom
runs are slow); each test documents the ordering claim it checks and the
`Ordering`s it would catch being weakened (mutation-check at least one:
demote an `Acquire` to `Relaxed`, confirm loom fails — the elle-mutation
"teeth" pattern, task47 in ultima_db).

---

## Deprioritized — recorded so the reasoning isn't re-litigated

- **Reopening Veil / the P2 residue in the SMT fragment.** Item 73 is a
  proof that it cannot close there. Closes via F-UC-2. The Veil toolchain
  stays useful as a *scout* for genuinely NEW protocol surfaces (run it at
  design time, before fixes exist — the one configuration where it beats
  proving; the 2026-07-19 brief's Amendment 3 said this and was right). Box
  envelope if ever revived: n=3/Fin 3 exhaustive ≈ affordable (~7 GB);
  n=3/Fin 4 and n=4 are NOT viable on a 15 GB box (ledger, capacity
  envelope). Every constrained run needs a vacuity canary run FIRST.
- **Linearizable-read barrier TOCTOU model.** Subtle and provable, but
  `lin_v2.rs` / `lin_partition_v2.rs` hammer it empirically with the WGL
  checker; marginal value low until the barrier changes.
- **M8 wire crypto.** Do NOT re-prove Noise IK — published Tamarin/ProVerif
  analyses cover it upstream (`snow` implements it). The only UC-composed
  parts are group-key rotation epochs + RFC-6479 window + header-as-AAD
  composition; a small model is defensible if M8's residual threat model
  (documented: symmetric group key ⇒ any holder forges fan-out) is ever
  revisited, otherwise skip.
- **Weak-memory formalization in Lean.** Research-project territory; loom
  (F-UC-4) is the right altitude.

---

## Standing constraints (apply to every task above)

- **`proofs/` is the sole trusted record.** Nothing under `proofs-veil/` is
  ever imported, built on, or cited as proof — read-only prior art.
- No `sorry` committed; `#print axioms` = `propext, Classical.choice,
  Quot.sound` on every top-level theorem; conformance rig stays green.
- Model-fidelity discipline (the arcs' single most-repeated lesson):
  **every counterexample and every refused proof is adjudicated against the
  Rust before the model is changed** — cite exact `node.rs`/`election.rs`
  lines in the ledger/doc. A CE is a question, not an answer; patching a
  model until it goes green is how an arc talks itself into a false result.
- One-Rust-read-one-model-value: never collapse two independently-read Rust
  values into one model field (Finding #12's class).
- Heavy artifacts (histories, logs, model-checker output) go to real disk,
  never `/tmp` (RAM tmpfs, no swap — the box OOM-kills the harness).
- Timebox + gate: estimate in S2-equivalents against the phase-2 memo's
  measured actuals, re-gate at checkpoints, and bank honest "not closed"
  verdicts rather than pushing past a structural wall (gate 2 is the model
  precedent).
