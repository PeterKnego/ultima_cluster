# UC v2 Lean Tier B(b) closure — Option-1 model refinement + unconditional leader completeness

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Retire the Finding-#7 model-fidelity debt (frame header/stamp split + `serveTail`) and land `leader_completeness` as an UNCONDITIONAL theorem — no `FramesCurrentAuthored` hypothesis — completing Tier B(b).

**Architecture:** Amend the `Uc2.Cert`/`Uc2.Data` protocol model so replicate frames carry BOTH a wire header term (provenance, exact-match at delivery — mirroring `uc2_net/src/receiver.rs:636-639`) and a record stamp, add the `serveTail` leader re-serve step (the NAK-repair/journal-replay path), re-green `log_matching` under the amended model, then build the three missing invariant layers from `task-LB2b-report.md`'s dependency-ordered list (map well-formedness → message-indexed report provenance → `committed_term_at_leaders`) and assemble `leader_completeness`.

**Tech Stack:** Lean 4.32 + mathlib (pinned, `proofs/`), existing `Uc2Model` kernels + `Uc2Proofs` corpus (election_safety, log_matching, C/R/V theorem inventory, ProtocolCommit ghost ledger).

**This plan continues the banked Tier B(b) arc.** Prior context: `docs/superpowers/plans/2026-07-17-uc2-lean-leader-completeness.md` (the LB plan), `.superpowers/sdd/task-LB2-rerun-report.md` (Finding #7 + the recommended model fix — the amendment this plan implements), `.superpowers/sdd/task-LB2b-report.md` (landed `FramesCurrentAuthored`/provenance lemmas + the four-step endgame list this plan mechanizes). Branch: `uc2/lean-lc-closure` off `main`.

## Global Constraints

- Gate before EVERY commit: `cd proofs && lake build && ! grep -rn --include='*.lean' -w 'sorry' Uc2Model Uc2Proofs Conform` — then `#print axioms` on every NEW/touched public theorem (standard axioms only: `propext`, `Classical.choice`, `Quot.sound`; no `native_decide`, no `sorryAx`).
- **Proof statements are the CONTRACT.** Reformulating induction structure is fine; WEAKENING a FIXED statement requires controller escalation. The FIXED LC-core target (re-keyed #6a form, now hypothesis-free):
  ```lean
  theorem leader_completeness {n : Nat} (w : World n) (hw : Reachable w)
      (p t T v : Nat) (hc : (p, t, T, v) ∈ w.committed)
      (i : Fin n) (hi : (w.nodes i).pn.role = .leader)
      (ht : T < (w.nodes i).pn.currentTerm) :
      (w.nodes i).hist p = some (t, v)
  ```
  (field paths adjusted to the actual node shape as in the LB2b file; the `T ≤` form is a welcome strengthening, expected to fall out per the LB2b report).
- STUCK-PROTOCOL per task: honest sustained effort up to the task's stated ceiling; if a statement resists because it looks FALSE, machine-check the countermodel, STOP, BLOCKED, escalate with the trace — never weaken, never sorry. If it resists on proof-engineering grounds, STOP at the ceiling with a design record (the LB2b precedent).
- All prior public theorems must replay unchanged at every commit (`Data.election_safety`, `Data.log_matching`, `Cert.election_safety`, `Cert.log_matching`, C1–C5, R1–R6, V1–V2, the non-vacuity traces, `hist_frame_provenance`, `committed_frame_provenance`).
- Staging hygiene: implementers stage ONLY their own files; never `git add -A`.
- No heavy artifacts to `/tmp` (RAM tmpfs, no swap — box rule). Lean caches under `$HOME`.
- Proof-cost accounting in every task report (feeds the re-gate memo).
- Attribution discipline: own the analysis; cite only named artifacts (the S3 lesson — no phantom-brief attributions).
- Models: LC1–LC4 fable (implementer AND reviewer — invariant-discovery work, per the re-gate memo's routing analysis); LC5 sonnet. If fable is unavailable mid-arc, STOP and record, do not silently downgrade.

## Settled design decisions

1. **Frame shape (Finding-#7 fix, from `task-LB2-rerun-report.md` §recommended-fix — adjudicated there, not revisited):**
   `Frame.replicate (pos hdr stamp payload : Nat)` — `hdr` is the wire `leadership_term_id` (provenance), `stamp` the record's term stamp. Delivery enabling: `hhdr : hdr = (nodes j).currentTerm` (EXACT match — `receiver.rs:636-639` `dropped_stale_term`) plus the existing intake-gate/reconciled condition. The old `hstamp : stamp ≤ currentTerm` delivery guard is DELETED (its job moves to emission-side truthfulness, decision 2). `observeTerm` stays keyed on the record STAMP (unchanged semantics).
2. **Truth at emission, not delivery** (house rule, LB decision-3 lineage): `leaderAppend` emits `hdr = stamp = currentTerm i`. New step `serveTail`: a leader `i` re-ships any existing `hist i p = some (t, v)` with `p < durable i` as `Frame.replicate p (currentTerm i) t v` — old stamp, CURRENT header. This is the NAK-repair/deep-NAK/journal-replay path by which old-stamped bytes legitimately reach a reconciled follower; it is what keeps inherited-prefix catch-up (the #6a/Fig-8 non-vacuity case) alive after the exact-match guard lands. No other emission site for replicate frames.
3. **`FramesCurrentAuthored` is DISCHARGED, not deleted**: keep the definition in `LeaderCompleteness.lean`, prove `frames_current_authored : Reachable w → FramesCurrentAuthored w` as a world invariant of the amended model. The eventual `leader_completeness` consumes it as a lemma; its hypothesis slot disappears. The two Finding-#7 countermodel theorems (`finding_stale_replicate_replay_lc_violation`, `lc_core_commit_term_keyed_is_false`) are DELETED — their pivotal step no longer type-checks under the amended `Frame` shape (precedent: the #5/#6b finding-theorem deletions; the history lives in the gate doc + reports).
4. **De-privatize the LA2 Cert helpers while touching the file**: `cert_of_leader`, `cert_blocks_candidate`, `Cert.transport` go `public` in `LogMatching.lean` during LC1's re-green (recorded LA3 debt; LC4 consumes them — no more per-file local re-proving).
5. **Map well-formedness is ONE bundled invariant** (LC2): `MapsWF w := ∀ j, Ascending (termMap j) ∧ mapFloor (nodes j)` where `mapFloor` is the LB2-rerun report's flagged clause (the committed position must be covered by SOME entry with base ≤ p — concretely: first entry base = 0 whenever the map is nonempty, or the equivalent formulation the induction supports; designed IN, not discovered mid-preservation).
6. **The endgame follows `task-LB2b-report.md` §recommendation VERBATIM in dependency order** — (1) `MapsWF` (LC2), (2) message-indexed report provenance mirroring `Inv.grant_state` over `CMsg.report` (LC3), (3) `committed_term_at_leaders` + (4) assembly (LC4). The LB2-rerun report's establishment-order analysis (credential-chain closure for pre-existing higher-term leaders, §"What the NEXT re-run needs") is the paper proof the LC3/LC4 invariant clauses mechanize.
7. **Estimates** (from the re-gate memo, floors not promises): LC1 ~2–3 S2-eq; LC2 ~1 S2-eq; LC3 ~1–2 S2-eq; LC4 ~1–2 S2-eq; total ~5–8 S2-eq. Per-task stuck ceilings stated in each task.

---

### Task LC1: Model amendment — frame header/stamp split + `serveTail` + LM re-green + `FramesCurrentAuthored` discharged

**Files:**
- Modify: `proofs/Uc2Proofs/ProtocolData.lean` (Frame shape, `deliverReplicate`, new `serveTail` constructor, `leaderAppend` emission)
- Modify: `proofs/Uc2Proofs/LogMatching.lean` (re-green under amended model; de-privatize `cert_of_leader`/`cert_blocks_candidate`/`Cert.transport` — decision 4)
- Modify: `proofs/Uc2Proofs/ProtocolCommit.lean` (mirrors + existing traces gain the header argument)
- Modify: `proofs/Uc2Proofs/LeaderCompleteness.lean` (delete the two countermodel theorems per decision 3; prove `frames_current_authored`; re-green `hist_frame_provenance`/`committed_frame_provenance`/non-vacuity under the new shape)

**Interfaces:**
- Consumes: current `DStep`/`Frame` model (LA1 as amended by #5/#6), `Uc2.TermMap.termAt`, `observeTerm`.
- Produces (for LC2–LC4): `Frame.replicate (pos hdr stamp payload : Nat)`; `DStep.serveTail` (leader-only re-serve, emits `hdr = currentTerm`); PUBLIC `Uc2.Data.cert_of_leader`, `Uc2.Data.cert_blocks_candidate`, `Uc2.Data.Cert.transport`; `Uc2.Cert.frames_current_authored : Reachable w → FramesCurrentAuthored w`; all prior public theorem names UNCHANGED.

- [ ] **Step 1**: Read the ground truth end-to-end: `task-LB2-rerun-report.md` (whole file — the fix spec), `task-LB2b-report.md` (whole file), current `ProtocolData.lean`, `LogMatching.lean`, `ProtocolCommit.lean`, `LeaderCompleteness.lean`; Rust: `uc2_net/src/receiver.rs` DATA-path guard (~:630-660, exact-match + intake gate) — the faithfulness anchor.
- [ ] **Step 2**: Amend `Frame`/`deliverReplicate`/`leaderAppend` + add `serveTail` per decisions 1–2. Build; walk EVERY breakage outward (`ProtocolCommit` mirrors, traces gain the header argument mechanically).
- [ ] **Step 3**: Re-green `LogMatching.lean`: `Occ` quantifies over frames — the `serveTail` case copies an existing occurrence (expected routine per the rerun report); de-privatize the three Cert helpers (decision 4). `Data.election_safety` must lift untouched (projection: both new/changed steps are stutters on the election plane).
- [ ] **Step 4**: `LeaderCompleteness.lean`: delete the two countermodel theorems (decision 3, docstring history preserved in module doc); re-green `hist_frame_provenance` (statement now `Frame.replicate p hdr t v ∈ dsent` — existential over `hdr` or the sharpened per-case form, implementer's call, but it must still feed `committed_frame_provenance` and the non-vacuity route); prove **`frames_current_authored`** by induction (the `deliverReplicate` case is where the exact-match guard + `observeTerm` interplay closes it — the LB2b report's §"the shape that works" is the paper argument).
- [ ] **Step 5**: Non-vacuity (REQUIRED, two traces): (a) re-green `nonvacuity_leader_completeness_trace` under the amended shape; (b) NEW `serveTail` trace — a follower truncates a divergent tail (reconcile), then RE-ACQUIRES an old-stamped byte via `serveTail` under the current leader's header, ending durably past it (proves the amendment did not gut the #6a inherited-prefix catch-up case — the rejected-alternative hazard from the rerun report).
- [ ] **Step 6**: Gate (Global Constraints) + axiom check on every touched public theorem. Commit: `proof(proofs): LC closure — frame provenance split + serveTail, LM re-green, FramesCurrentAuthored discharged (lean LC1)`.

STUCK ceiling: ~3 S2-equivalents. The known risk is LM re-green scope (1046 lines); the rerun report estimates "LA1-amendment scale, not a full re-derivation" — if it cascades beyond the ceiling, STOP with the breakage inventory.

### Task LC2: `MapsWF` — term-map well-formedness world invariant

**Files:**
- Create: `proofs/Uc2Proofs/MapWF.lean` (+ import in `proofs/Uc2Proofs.lean`)

**Interfaces:**
- Consumes: LC1's amended model; `Uc2.TermMap.Ascending` (pure predicate, `Uc2Model/TermMap.lean`); `prunePush`/`observeTerm`/reconcile output shapes; `Ascending` toolkit in `Reconcile.lean` (`Ascending.termAt_take`, `reconcile_ok_newMap_take`).
- Produces (for LC3/LC4): `Uc2.Cert.MapsWF (w : World n) : Prop` (decision 5: per-node `Ascending` ∧ `mapFloor`) and `Uc2.Cert.reachable_mapsWF : Reachable w → MapsWF w`.

- [ ] **Step 1**: State `MapsWF` + `reachable_mapsWF` with `sorry`; build (sorry-gate red confirms wiring).
- [ ] **Step 2**: Prove by induction. Non-trivial cases (from the LB2b report): `becomeLeader` (`prunePush` — pruned-phantom push preserves ordering), `deliverReplicate` (`observeTerm` conditional append), `deliverTermMap` (reconcile `take k ++ filter` output). All other constructors are map-stutters. `mapFloor` preservation rides the same cases.
- [ ] **Step 3**: Gate + axiom check; priors replay. Commit: `proof(proofs): LC closure — MapsWF term-map well-formedness invariant (lean LC2)`.

STUCK ceiling: ~1.5 S2-equivalents (LB2b estimated "roughly LA2/S2 scale on its own").

### Task LC3: Message-indexed report-provenance invariant

**Files:**
- Create: `proofs/Uc2Proofs/ReportProvenance.lean` (+ import), or extend `MapWF.lean` if the invariants must be mutually inductive (document the call in the report)

**Interfaces:**
- Consumes: `reachable_mapsWF` (LC2), `frames_current_authored` (LC1), `reconcile_preserves_shared_prefix` (R2), `Ascending.termAt_take`, `ElectionSafety.lean`'s `Inv.grant_state` as the PATTERN (vote-message analog), `CMsg.report` machinery in `ProtocolCommit.lean`.
- Produces (for LC4): the report-provenance clause — working statement: for every `CMsg.report u T d ∈ w.csent`, the sender's term-map facts at send time (`termAt p` for `p < d` under term-`T` gate-open provenance) transport forward to `u`'s current state unless `u` has moved past them in a way the credential chain records (the LB2-rerun report's §"establishment-order" chain, clause 2, restricted to `termAt`-only content per LB2b). Exact clause shape is DISCOVERY — the statement above is the contract of what it must deliver to LC4, not its literal form.

- [ ] **Step 1**: Re-read `Inv.grant_state` (the S2 pattern) + the LB2-rerun §"What the NEXT re-run needs" items 1–2 (the paper argument being mechanized).
- [ ] **Step 2**: Design + prove the clause(s). Expected: a send-time `termAt` snapshot claim + forward transport through same-or-higher-term reconciles (R2 + `Ascending.termAt_take` + `MapsWF`) and through `serveTail`/append deliveries (`frames_current_authored`). If transport fails on a genuine interleaving: machine-check, STOP, escalate (Finding-#8 territory — candidate real gap, per the arc's standing rule).
- [ ] **Step 3**: Gate + axiom check; priors replay. Commit: `proof(proofs): LC closure — message-indexed report provenance (lean LC3)`.

STUCK ceiling: ~2 S2-equivalents.

### Task LC4: `committed_term_at_leaders` + `leader_completeness` assembly

**Files:**
- Modify: `proofs/Uc2Proofs/LeaderCompleteness.lean` (the invariant + the theorem; module doc updated to reflect UNCONDITIONAL status)

**Interfaces:**
- Consumes: everything — LC1 (`frames_current_authored`, provenance lemmas, public Cert helpers), LC2 (`reachable_mapsWF`), LC3 (report provenance), `quorum_intersect` (C5), `logOk_iff` (V2), `Uc2.Data.log_matching`, `advance_certified` (C3).
- Produces: `Uc2.Cert.committed_term_at_leaders` (LB2b step-2 statement: `(p, stamp, T, v) ∈ committed → role i = leader → T ≤ currentTerm i → p < durable i ∧ termAt (termMap i) p = stamp`) and **`Uc2.Cert.leader_completeness`** (the FIXED contract statement, Global Constraints — `T <` form minimum, `T ≤` strengthening expected).

- [ ] **Step 1**: State both with `sorry`; build.
- [ ] **Step 2**: Prove `committed_term_at_leaders` by induction. The `becomeLeader` case is THE crux (classical Raft-completeness): vote quorum ∩ report quorum via `quorum_intersect`; the shared voter's report transports `termAt`/`durable` to the candidate's credentials via LC3 + `logOk_iff`; the pre-existing-higher-term-leader sub-case closes via the credential chain (LB2-rerun §establishment-order item 2 — mechanize as stated there, minimal-term induction if needed).
- [ ] **Step 3**: Assemble `leader_completeness` per LB2b's four-step sketch (steps 1/3/4 are "essentially free": `committed_frame_provenance` → occurrence; the invariant → `p < durable` + `termAt = stamp`; `frames_current_authored` → stamp pin; `log_matching` → payload). Attempt the `T ≤` strengthening.
- [ ] **Step 4**: Full gate: `lake build` + sorry grep + `#print axioms` on ALL public theorems (new + priors). Commit: `proof(proofs): LC closure — leader_completeness UNCONDITIONAL (lean LC4)`.

STUCK ceiling: ~2.5 S2-equivalents. This is the arc's long pole; the two prior BLOCKED attempts were statement-falseness (fixed) and missing infrastructure (LC1–LC3 build exactly that infrastructure) — a THIRD distinct blocker would be new information: machine-check and escalate, never force.

### Task LC5: Re-gate memo + gate doc + arc close

**Files:**
- Modify: `docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md` (append "Tier B(b) closure actuals")
- Modify: `docs/benchmarks/uc2-lean-gate-2026-07-16.md` (LC status: conditional/open → UNCONDITIONAL; Finding #7 disposition: discharged by model refinement)

- [ ] **Step 1**: Record LC1–LC4 measured costs vs the ~5–8 S2-eq estimate; whether the `T ≤` strengthening landed; the final (c) state-machine-safety re-price now that (b) is COMPLETE (the memo's §5 said (c)'s conditionality dilemma dissolves if (b) closes unconditionally — say so plainly and re-price against the 4–9 S2-eq prior). Findings section: anything LC2–LC4 surfaced. Attribution discipline.
- [ ] **Step 2**: Gate (docs-only build sanity), commit: `docs(benchmarks): tier-B(b) closure actuals + (c) re-price (lean LC5)`.

Then STOP: final whole-branch review (fable) + user disposition (merge/push + GO/no-go on (c)) — per the arc's standing pattern.

## Self-review notes

- Spec coverage: rerun-report §recommended-fix → LC1 (decisions 1–2 verbatim, rejected alternatives NOT revisited); rerun-report §establishment-order → LC3/LC4 (its clause-2 credential chain is LC3's contract); LB2b §recommendation items 1–4 → LC2/LC3/LC4 steps 2–3; LA3 de-privatization debt → decision 4; map_floor flag → decision 5. The LB2b "frame_uniq/Tcert may fold in near-free via gossip_pinned" note is left to LC3's discovery (not contracted).
- Type consistency: `Frame.replicate (pos hdr stamp payload)` is the ONLY frame shape named anywhere; `MapsWF`/`reachable_mapsWF`/`frames_current_authored`/`committed_term_at_leaders` names are used identically across tasks.
- The LC-core statement in Global Constraints is the #6a re-keyed FIXED form with the hypothesis slot REMOVED — that removal is the entire point of the arc and is contract, not latitude.
- Non-vacuity latitude: LC1's serveTail trace is REQUIRED (it guards the rejected-alternative hazard); LC4 reuses the landed trace.
