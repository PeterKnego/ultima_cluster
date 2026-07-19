# UC v2 — Veil spike brief: bounded model-checking as a bug-hunting oracle for M7 reconfig + the election-time coherence window (NOT for closing the LC theorem)

**Date:** 2026-07-19 (amended twice same day — first after the Tier B(b)
*actuals* landed, then again after the **Tier B(b) closure arc** (LC1–LC4h,
merge `aceb01e`) landed and materially retargeted this brief. The original
draft pitched Veil as an invariant-discovery oracle for a *blocked
`leader_completeness` theorem*; the closure arc changed what "blocked"
means, and that framing is now wrong. See §2.)
**Status:** RECOMMENDATION — not started. Dispatch brief for a future
session; self-contained but assumes a full checkout (do NOT trust public
GitHub HTML/web-fetch — a prior research session read a months-stale cached
root listing and derived an entirely wrong assessment; use the checkout or
the GitHub API).
**Decision being recommended:** a **1–2 session, time-boxed spike** using
Veil (`verse-lab/veil`) as a **bounded explicit-state model checker for
bug-finding and design assurance**, aimed at (i) M7 reconfiguration
(primary — no existing Lean model to duplicate) and (ii) the election-time
term/gate/commit coherence window that has now produced *four* real shipped
consensus bugs the empirical stack structurally missed. **Explicitly NOT
recommended:** using Veil to help close `leader_completeness` — the sole
remaining obligation (canon) has no countermodel and needs a proof
technique (joint induction), not a CTI. See §2.

---

## 1. Verified ground state (2026-07-19, post-closure-arc)

Verified directly against the checkout at merge `aceb01e`, not recalled:

- **Everything is on `main`.** M7 (`eb8d543`), Lean Phase 1/1.5/2, Tier
  B(a) (`9ee8e00`), Tier B(b) actuals (`de2271c`), and the **B(b) closure
  arc** (`aceb01e`, merging `uc2/lean-lc-closure`) are all merged. No live
  `uc2/*` feature branches remain; other remote branches are stale v1-era.
- `proofs/` — Lean 4 Lake package, `leanprover/lean4:v4.32.0`, Mathlib
  `v4.32.0`. Targets `Uc2Model` (executable, mathlib-free), `Uc2Proofs`,
  `conform`. Zero `sorry`/`admit`, zero project-local axioms, no
  `native_decide`. Model↔Rust: `conform_gen.rs` → 100k seeded vectors →
  `Conform/Main.lean` bit-for-bit.
- **Unconditionally proved:** Phase 1 kernels (14), `election_safety`,
  Tier B(a) `log_matching`.
- **Leader completeness — now UNCONDITIONAL down to a single obligation,
  and the arc is BANKED (not finished).** The closure arc chose Option 1
  (retire the Finding #7 debt, prove LC with no `FramesCurrentAuthored`
  hypothesis). What LANDED (all sorry-free, standard axioms):
  - **The Finding #7 model refinement** (LC1/LC1b): replicate frames now
    carry a wire **header term** (provenance) distinct from the record
    stamp; delivery is exact-match on a **lagging `dataTerm` handle**
    mirroring `uc2_net/src/receiver.rs`; a `serveTail` re-serve step models
    NAK-repair/journal-replay. `log_matching`/`election_safety` re-green.
    → **Option 1 is DONE.** (The prior draft's "prototype Option 1 in
    Veil" task is obsolete.)
  - **`frames_current_authored` DISCHARGED hypothesis-free** (LC3), inside
    a 21-clause message-indexed `ProvInv` bundle. → **`FramesCurrentAuthored`
    is no longer a standing hypothesis.** (The prior draft's guardrail
    about "Veil can't discharge FCA in the record" is moot.)
  - Full supporting stack: `MapsWF` (LC2), Stage-A take-discipline (LC4b),
    Stage-B credential layer (LC4c), the `leader_completeness` ASSEMBLY
    (`lc_of_ctl`, LC4e — composes first-try in `T ≤` form once its one
    input lands), the `becomeLeader` crux (`crux_become_leader` /
    `CruxInputs`), and canon's statement + consumer pin
    (`canon_reconcile_clean`) + antitonicity (`repquorum_anti`) + a
    machine-checked **satisfiability witness** that canon holds nontrivially
    at `k > 0` (LC4h/F-A).
- **The one remaining obligation is `canon`** (entry-level canonical-prefix
  agreement): canon ⇒ `CruxInputs` ⇒ `committed_term_at_leaders` ⇒ (via
  landed `lc_of_ctl`) `leader_completeness`. **Finding #11** proved canon
  **cannot** be closed by the single-`ReflTransGen`-shell induction every
  other invariant here uses — its `RepQuorum` antecedent is monotone-forward
  (a conjunction of existentials over append-only wires, provably never
  antitone), so newly-born instances get nothing from the induction
  hypothesis; it needs **joint / well-founded induction**, machinery this
  corpus has never used. **F-A's witness confirms canon is satisfiable —
  this is SCOPE, not falsity: no countermodel exists, nothing implicates
  `reconcile`-vs-Rust.** Remaining to a complete LC: **≈7–12 S2-equivalents /
  3–4 tasks, joint induction unavoidable.** Blueprint recorded in the
  LC4g/LC4h reports for clean resume.
- **FOUR real shipped consensus bugs have now come out of this proof
  effort, all in the same neighborhood — the election-time term-handle /
  intake-gate / commit coherence window** — all acked-write-loss class, all
  invisible to sim/elle/lincheck for an *interleaving-coverage* reason (the
  directed scenario just never walked the path), NOT an oracle-design gap:
  - Finding #5 — boot intake gate open over unreconciled vote (`6ca2c95`);
  - Finding #6b — commit advance not clamped to current-term base, a Raft
    §5.4.2 / Figure-8 data-loss bug (`52c11d5`);
  - Finding #8 — model-fidelity gap (delivery keyed to `currentTerm`
    admitted a cross-stream frame a lagging handle drops in Rust; fixed by
    the `dataTerm` model, no Rust bug) — it *forced the faithful model that
    exposed*:
  - **Finding #9 — CONFIRMED REAL Rust bug** (`4ce6eb3`, fix at
    `uc2_node/src/node.rs:~2421` keying BOTH intake-gate reopen arms to
    `current_term == adopted_term`): a candidate whose term handle lags
    could reopen DATA intake for the stale handle-term stream against a map
    never reconciled with it — handle-stamped reports then certify a
    phantom commit past the Finding-#6b clamp. Confirmed reachable
    link-by-link in Rust; RED/GREEN sim regression pinned.
- **Tier B(c)** (state-machine safety) remains fully open, gated on
  finishing (b); re-priced ≈11–21 S2-equivalents of new work from the
  banked state.
- **Veil**: Lean-embedded Ivy-style framework (CAV 2025). `main` pins
  `leanprover/lean4:v4.24.0` (verified via GitHub API 2026-07-19 against
  `verse-lab/veil`). Incompatible with `proofs/`'s v4.32.0 → any Veil work
  lives in a **separate sibling Lake package** (`proofs-veil/`, own
  `lean-toolchain`, never imported by `proofs/`). `veil-2.0-preview` adds a
  TLC-style explicit-state model checker + CTI-guided invariant inference —
  check its state/pin at spike time.

## 2. Honest re-assessment: what Veil can and cannot do here

The closure arc is a natural experiment in what a countermodel/model-checking
tool is worth on this codebase, and it cuts both ways.

**Where Veil does NOT help (retracted from the prior draft): closing
`leader_completeness`.** The sole remaining obligation, canon, has a landed
satisfiability witness and **no countermodel** — Finding #11 established the
blocker is that canon's monotone-forward antecedent defeats the corpus's
single-shell induction and needs joint/well-founded induction. A bounded
model checker finds counterexamples; there are none to find. CTI-guided
invariant inference proposes *strengthenings to restore inductiveness* — but
the issue is not a missing conjunct, it's the induction principle. **Veil
cannot supply joint induction, and pointing it at canon would burn the box
confirming "no CTI exists," which F-A already tells us.** Do not aim the
spike there. (This is the single biggest correction over the two prior
drafts, which were written when LC was "blocked at the `becomeLeader` case
needing undiscovered invariant machinery" — that machinery is now landed;
what's left is a proof-power gap, not a discovery gap.)

**Where Veil DOES help, and the evidence is now strong:**

1. **Bug-hunting the election-time coherence window (bounded model
   checking).** Four real shipped bugs (#5/#6b/#8/#9) have come out of one
   narrow window — term-handle vs. intake-gate vs. commit clamp during
   elections — every one invisible to the empirical stack purely for
   interleaving coverage, i.e. the state-space paths its fuzz/scenario
   generators structurally never walked. **A bounded explicit-state model
   checker is exactly an interleaving-coverage machine.** The B(b) findings
   are the calibration ground truth: a checker that can rediscover the #6b
   Figure-8 trace or the #9 cross-stream accept from the *pre-fix* model is
   a checker worth running forward on the *current* model to hunt for a
   fifth. This is a **bug-finding** pitch (find new countermodels), not a
   proof-assist pitch — and it targets the corner this codebase has
   demonstrably the most residual risk in.
2. **M7 reconfiguration design assurance.** No Lean model of reconfig
   exists to duplicate; config-change × election × commit is the
   historically bug-richest Raft corner *and* sits squarely in the same
   coherence window the four bugs came from. Primary target — see §4.
3. **(Weak, tertiary) structural invariants for Tier B(c).** B(c)'s
   election-facing / non-truncation *structural* invariants may accept CTI
   help, but its hard parts (byte-content identity, `commonPrefixLen`
   sequence reasoning) fall outside the decidable fragment (CAV paper
   concedes this class), and it is gated on finishing canon (which Veil
   can't help with) anyway. Do not lead with this.

**Hard guardrails (non-negotiable — preserve the repo's trust story):**

1. **Veil is never the record.** Permanent proofs stay in `proofs/`
   (v4.32.0, standard axiom trio, no SMT in the trusted base). Veil's only
   deliverables are **countermodel traces** (→ directed `uc2_sim`
   regressions + Rust fixes, the #6b/#9 pattern) and, secondarily,
   candidate invariant *text*. A Veil model has **no conformance rig**;
   it is scratchpad-only. Under this rule Veil soundness is not
   load-bearing: a spurious CTI wastes minutes; a bug it finds gets
   independently reconfirmed in Rust before any fix (as #9 was).
2. **No migration of anything proved.** Election safety, log-matching,
   the whole LC support stack — done. Veil touches none of it.
3. **Toolchain isolation.** `proofs-veil/` has its own `lean-toolchain`.
   Never on any `lake build` path, CI gate, or "proved" claim unless a
   deliberate later decision says so (see §4 CI note).

## 3. The spike (1–2 sessions, time-boxed)

- **V1 — package + port + CALIBRATE AGAINST KNOWN BUGS.** Create
  `proofs-veil/` on Veil's toolchain. Port the current world — election
  plane + B(b) commit plane + the LC1/LC1b frame plane (header term +
  `dataTerm` lagging handle + `serveTail`), from `Protocol.lean`,
  `ProtocolCommit.lean`, `ProtocolData.lean` (frame content relationally
  abstracted). Then the sharp calibration, which the B(b) arc uniquely
  makes possible:
  - **Bar 1:** `#check_invariants` certifies the already-proved invariants
    (election `Inv`, B(a) `DInv`) inductive.
  - **Bar 2 (the decisive one):** point the explicit-state checker at the
    **pre-fix** model (revert the #6b commit clamp, or the #9
    `current_term`-keyed reopen) and confirm it **rediscovers the known
    countermodel** (Figure-8 old-term commit loss / cross-stream phantom
    accept) within a small node/step bound.
  Either bar failing → exit, cheap. A checker that can't reproduce a bug
  we already have in hand will not find the next one.
- **V2 — hunt forward on the current model.** With calibration passed, run
  the checker on the *fixed* current model across 3–5 nodes, biased toward
  the election-time coherence window (concurrent `startElection` /
  `crashRestart` / gate-reopen / commit interleavings). Goal: a fifth
  countermodel, or bounded evidence of its absence at depth. Any hit →
  reconfirm in Rust, file a directed sim regression (the #5/#6b/#8/#9
  workflow). This is the spike's headline deliverable.
- **V3 — report + go/no-go.** Gate-doc writeup
  (`docs/benchmarks/uc2-veil-spike-<date>.md`): port fidelity, both
  calibration bars, any new countermodel, bound/depth reached, wall-clock
  in S2-equivalents.

**Exit criteria (decide honestly — the repo rewards findings over outcomes;
the closure arc banked rather than force a proof, same discipline):**

- **KEEP** (stand up a nightly Veil model-check job, extend to M7 §4) iff
  BOTH calibration bars passed AND V2 either found a real interleaving or
  gave credible bounded coverage of the window at a useful depth.
- **DROP** iff the frame/commit-plane abstraction contorted against the
  decidable fragment, or the checker couldn't rediscover a known bug (Bar
  2). Dropping does NOT drop M7 (§4) if only the Tier-B/bug-hunt fit
  failed and the reconfig fit is untried — but if Bar 1/2 failed outright,
  the tool is wrong for this codebase and M7 likely fails too; note that.
- **Do NOT** spend any of the box trying to help canon — see §2.

## 4. M7 reconfiguration — the primary standalone target

Independently GO-able and arguably the best fit, precisely because it lives
in the same election-time coherence window the four bugs came from and has
**no existing model to duplicate**:

- M7 merged (`eb8d543`; spec
  `docs/superpowers/specs/2026-07-13-uc2-reconfig-design.md`, gate
  `docs/benchmarks/uc2-m7-gate-2026-07-13.md`): single-server
  promote/demote/add/remove, no joint consensus, under load. The original
  single-server-change algorithm shipped a real safety bug found only after
  publication; quorums over *changing* configs is classic Ivy/Veil
  territory.
- Model: config as a relation, one-at-a-time change steps
  (`FRAME_TYPE_CONFIG` propagation, promote/demote/remove) composed with
  the §3 election/commit actions. Explicit-state check election safety +
  commit safety across a config change on 3–5 node instances (shallow bugs
  in seconds), then `#check_invariants` for inductiveness. Given the fleet
  gate is the only reconfig assurance beyond the sim today, this is genuine
  new coverage.
- If it survives, a nightly job next to the `elle` tier is cheap — but per
  guardrail 3, adding it to CI is a deliberate follow-up, not part of the
  spike.
- Timing: any point after V1's port exists; independent of everything else.

## 5. What NOT to do

- Do **not** aim the spike at closing `leader_completeness` / canon — §2.
- Do **not** research from public web fetches of the repo — header warning.
- Do **not** attempt Aeneas extraction — Phase 1.5 already EXITED on a
  version wall (`acef50a`); hand-model + conformance was chosen deliberately.
- Do **not** put Veil obligations / SMT results / the `proofs-veil/` build
  into any "proved" claim — proved means `proofs/` + `lake build` + the
  axiom check.
- Do **not** re-open the B(b) findings: #5/#6b/#8/#9 are fixed on `main`
  with directed sim pins; #7 is retired by the LC1 model refinement; the
  canon/joint-induction remainder is banked with a blueprint, not stuck.
- Do **not** run open-ended: box is 1–2 sessions; a stuck port is a finding
  — write it up and exit.

## 6. Pointers

- **B(b) closure arc actuals + banked disposition** (READ FIRST — this
  amendment is based on it): "Tier B(b) CLOSURE ARC (LC1–LC4h)" section of
  `docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md`; plan
  `docs/superpowers/plans/2026-07-18-uc2-lean-lc-closure.md`; task detail
  `.superpowers/sdd/task-LC{1..4h}-report.md`, ledger `progress.md`.
- B(b) actuals (pre-closure) + B(a) actuals: earlier sections of the same
  doc. Phase 1 record: `docs/benchmarks/uc2-lean-gate-2026-07-16.md`.
- LC proof state (canon, the crux, the landed stack, kept countermodels):
  `proofs/Uc2Proofs/LeaderCompleteness.lean`, `ProtocolCommit.lean`,
  `ProtocolData.lean` (the LC1 frame plane — header term + `dataTerm` +
  `serveTail`); election/log-matching: `Protocol.lean`,
  `ElectionSafety.lean`, `LogMatching.lean`.
- Rust anchors for the coherence window: `uc2_net/src/receiver.rs`
  (exact header-term match, lagging handle), `uc2_node/src/node.rs:~2421`
  (Finding #9 gate-reopen fix), `52c11d5` (commit clamp), `6ca2c95` (boot
  intake gate).
- Conformance rig (the pattern Veil must NOT be mistaken for):
  `proofs/Conform/Main.lean` + `uc2_consensus/examples/conform_gen.rs`.
- Veil: `github.com/verse-lab/veil` (CAV 2025; re-check `lean-toolchain`
  and `veil-2.0-preview` at spike time — v4.24.0 pin verified 2026-07-19).
- M7: `docs/superpowers/specs/2026-07-13-uc2-reconfig-design.md`,
  `docs/benchmarks/uc2-m7-gate-2026-07-13.md`, merge `eb8d543`.
