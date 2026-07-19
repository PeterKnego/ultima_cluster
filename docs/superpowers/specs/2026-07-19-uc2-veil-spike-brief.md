# UC v2 — Veil spike brief: bounded model-checking as a bug-hunting oracle for M7 reconfig + the election-time coherence window (NOT for closing the LC theorem)

**Date:** 2026-07-19 (amended three times same day — first after the Tier
B(b) *actuals* landed; again after the **Tier B(b) closure arc** (LC1–LC4h,
merge `aceb01e`) landed and materially retargeted this brief; and a third
time (**Amendment 3**, controller review) to reorder the spike and harden
its calibration — see the Amendment-3 block below and §3. The original draft
pitched Veil as an invariant-discovery oracle for a *blocked
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

**Amendment 3 (controller review, 2026-07-19) — folded in below:** the §2
retraction and the two hard guardrails are endorsed unchanged (they are the
document's load-bearing calls). Four scope corrections were folded into §3–§4
and the exit criteria, because as originally sequenced the spike led with its
riskiest, deepest, most-likely-to-abstract-wrong half:
  1. **Lead with M7, not the coherence window** — M7 is higher expected
     value (no model to duplicate, shallow bugs in seconds) and lower
     abstraction risk; the coherence-window hunt is the riskier *second*
     half. §3 resequenced; §4's "any point after the port" downgraded.
  2. **Must-pass calibration moves to the *shallowest* known bug (Finding
     #5**, 1-node few-step boot-gate phantom). #6b/#9 rediscovery is a
     *depth-probe / stretch*, NOT a gate: a bounded checker missing a deep
     3-term Figure-8 interleaving is a statement about the bug's depth, not
     the tool's fitness — so a #6b miss must NOT be read as "tool wrong,
     M7 fails too" (the original exit logic's overreach, corrected below).
  3. **Frame-provenance abstraction is promoted into an explicit Bar-2
     obligation** — #9 is a which-bytes-land-where bug, so the relational
     abstraction of frame content must be shown to *preserve the
     stale-stream-vs-current-stream distinction* before the forward hunt,
     or V2 is blind to the class it was calibrated on.
  4. **Hard session-1 re-gate at "port + Bar-1"** — the "1–2 session"
     anchor is optimistic for an unfamiliar toolchain + a three-plane
     relational port; checkpoint and re-decide there rather than commit the
     whole box to a hopeful V2. Plus: front-load the `veil-2.0-preview`
     maturity check *before* the port, not mid-spike.

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

## 3. The spike (time-boxed; session-1 re-gate is mandatory)

Resequenced per Amendment 3: M7 (the higher-value, lower-abstraction-risk
target) comes BEFORE the coherence-window hunt (deeper bugs, riskier content
abstraction). Session 1 ends at a hard re-gate.

- **V0 — pre-flight maturity check (before any port).** Confirm the
  explicit-state checker + CTI inference actually exist and run in a usable
  state at the pinned Veil revision (`main` @ v4.24.0, or `veil-2.0-preview`
  — re-check per §6). If the checker lives only in a preview branch that
  doesn't build, that is the finding: write it up and exit before spending
  a session on a port. Front-loaded deliberately — do not discover this
  mid-spike.
- **V1 — package + port + Bar 1.** Create `proofs-veil/` on Veil's
  toolchain. Port the current world — election plane + B(b) commit plane +
  the LC1/LC1b frame plane (header term + `dataTerm` lagging handle +
  `serveTail`), from `Protocol.lean`, `ProtocolCommit.lean`,
  `ProtocolData.lean` (frame content relationally abstracted — see V2's
  abstraction obligation, which this port must be built to satisfy).
  - **Bar 1:** `#check_invariants` certifies the already-proved invariants
    (election `Inv`, B(a) `DInv`) inductive.
  - **Bar 2 (must-pass, retargeted): rediscover the SHALLOWEST known bug.**
    Point the checker at the **pre-fix** model with the Finding-#5 boot gate
    reverted (boot intake gate open over an unreconciled vote) and confirm
    it rediscovers that phantom-commit at a **1-node, few-step** bound. This
    is the true tool-fitness gate — a shallow, cheap trace the checker MUST
    reach.
  - **Bar 2b (abstraction obligation, also must-pass): the frame
    abstraction preserves the #9 distinction.** #9 is a
    which-bytes-land-where bug, so demonstrate — as a tiny directed check —
    that the relational abstraction of frame content still distinguishes a
    stale-handle-term stream byte from a current-term stream byte at the
    same position. If the abstraction erases this, the coherence-window hunt
    (V2) is blind to the class it targets; fix the abstraction or record it
    as the reason the window hunt is out of scope.
  **>>> SESSION-1 RE-GATE (mandatory).** Stop here. V0 + V1 + both must-pass
  bars is a full session's honest budget for an unfamiliar toolchain and a
  three-plane relational port. Re-decide before spending session 2: if V0
  or either must-pass bar failed, that IS the spike's finding — write it up
  and exit. Only a clean re-gate authorizes V-M7 / V2.
- **V-M7 — the primary hunt (do this FIRST after the re-gate; see §4).**
  Add the config-relation + one-at-a-time change steps and explicit-state
  check election + commit safety across a config change on 3–5 nodes. Best
  fit, no model to duplicate, shallow bugs surface in seconds. This is the
  spike's primary deliverable.
- **V2 — coherence-window forward hunt (second, riskier).** Only if Bar 2b
  showed the abstraction preserves the #9 distinction: run the checker on
  the *fixed* current model across 3–5 nodes, biased toward the
  election-time window (concurrent `startElection` / `crashRestart` /
  gate-reopen / commit interleavings). **Depth-probe (stretch, NOT a
  gate):** how deep a bound is needed before the checker rediscovers #6b's
  3-term Figure-8 / #9's cross-stream accept — this calibrates the forward
  hunt's confidence and tells you whether "absence at depth N" means
  anything. Goal: a fifth countermodel, or honest bounded-coverage
  evidence. Any hit → reconfirm in Rust, file a directed sim regression (the
  #5/#6b/#8/#9 workflow).
- **V3 — report + go/no-go.** Gate-doc writeup
  (`docs/benchmarks/uc2-veil-spike-<date>.md`): port fidelity, both
  must-pass bars, the #6b/#9 depth-probe result, M7 findings, any new
  window countermodel, bound/depth reached, wall-clock in S2-equivalents.

**Exit criteria (decide honestly — the repo rewards findings over outcomes;
the closure arc banked rather than force a proof, same discipline):**

- **KEEP** (stand up a nightly Veil model-check job, extend to M7 §4) iff
  Bar 1 + both must-pass bars (2, 2b) passed AND at least one of {V-M7
  surfaced/cleared a config-change scenario, V2 found a real interleaving or
  gave credible bounded coverage} landed.
- **DROP** iff the frame/commit-plane abstraction contorted against the
  decidable fragment (Bar 2b unfixable), or the checker couldn't rediscover
  the **shallow** Finding-#5 bug (Bar 2). Only a Bar-1/Bar-2 failure
  licenses "the tool is wrong for this codebase, M7 likely fails too" — a
  *deep-bug* miss (the #6b/#9 depth-probe coming up empty at a tractable
  bound) does NOT: it is expected state-explosion behaviour and says nothing
  about M7's shallow-bug fit, which is judged on its own at V-M7.
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
- Timing (Amendment 3): M7 is the PRIMARY hunt and runs FIRST after the
  session-1 re-gate (V-M7 in §3), before the riskier coherence-window V2 —
  it needs only V1's port + Bar-1, not Bar-2b's frame-abstraction result
  (config/quorum reasoning is relational, not byte-content), so it is the
  cleanest place to spend the tool's first real forward run.

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
