import Mathlib.Tactic.FinCases
import Uc2Proofs.ProtocolCommit

/-! LC1/LC1b (Option-1 frame-provenance refinement) — the header/stamp
split + the `dataTerm` lagging-handle repair; Findings #7 AND #8 keyed out.

## What LC1 + LC1b landed

The LB2-rerun report's "recommended fix (faithful)" (arc decisions 1–2)
plus the adjudicated Finding-#8 repair, applied in `ProtocolData.lean` /
`ProtocolCommit.lean`:

- `Frame.replicate (pos hdr stamp payload)` — the wire header
  (`leadership_term_id`) and the record stamp are separate fields;
- delivery requires an EXACT header match against the node's DATA-PLANE
  TERM HANDLE (`hhdr : hdr = dataTerm j`, `receiver.rs:636-639`
  `dropped_stale_term` comparing `self.term` — the handle) plus the
  existing intake gate; the old `hstamp : stamp ≤ currentTerm` delivery
  guard is gone — its content moved to emission-side truthfulness
  (`LogMatching.lean`'s `StampInv`, whose `data_le` clause carries the
  handle lag `dataTerm ≤ currentTerm`);
- `dataTerm` (LC1b) advances exactly where Rust stores `term_handle`:
  `becomeLeader` (`node.rs:2462`), every strict adoption
  (`Action::BecomeFollower`, `node.rs:2490` — the vote-request arm, the
  higher-term-vote arm, and `deliverTermMap`'s gossip-adoption arm), and
  boot/`crashRestart` (recovered term, `node.rs:513`); `startElection`
  does NOT touch it (`node.rs` 2440–2450) — a candidate's data plane
  keeps running at its pre-bump term;
- `leaderAppend` emits `hdr = stamp = currentTerm`; the leader-only
  `serveTail` step re-ships a held byte under the CURRENT header with its
  ORIGINAL stamp (the NAK-repair/journal-replay catch-up path);
- `observeTerm` stays keyed on the record STAMP (unchanged semantics).

`nonvacuity_serve_tail_catchup_trace` (below) proves the amendment did NOT
gut the legitimate inherited-prefix catch-up that the rejected
equality-on-one-term alternative would have killed.

## History (the finding-theorem deletions, decision-3 precedent)

- **Finding #6a (statement gap)**: ghost re-keyed to
  `(position, stamp, commitTerm, payload)` (`ProtocolCommit.lean`, item 1).
- **Finding #6b (PROTOCOL gap, Raft §5.4.2/Fig-8 — a real v2.x
  acked-write-loss bug)**: fixed in Rust (`rank_leader` commit clamp),
  mirrored as `leaderAdvanceCommit`'s `hbase`.
- **Finding #7 (model-fidelity)**: single-term frames + the `≤` stamp
  guard admitted a cross-stream stale-replicate replay — sound for log
  matching, unsound for leader completeness. Fixed by the LC1 header/stamp
  split: the dead leader's in-flight frame carries its own tenure's
  header, which the exact match refuses at any later-term receiver. The
  33-step countermodel `finding_stale_replicate_replay_lc_violation` + its
  corollary `lc_core_commit_term_keyed_is_false` no longer type-check and
  were DELETED (full traces in the LB2-rerun report + the lean gate doc).
- **Finding #8 (model-fidelity, LC1)**: keying the exact match to
  `currentTerm` left a CANDIDATE hole Rust does not have — a node that
  crash-opened its gate at its own led term and then `startElection`ed
  kept the gate open while `currentTerm` ran ahead of its data-plane
  regime, accepting a current-header `serveTail` byte its map never
  reconciled against (Rust drops it: header ≠ handle). Refuted
  `Reachable → FramesCurrentAuthored` via a 24-step kernel trace
  (`finding_candidate_stale_gate_fca_violation` +
  `fca_unconditional_is_false`, commit `ce17fb7`). Fixed by LC1b's
  `dataTerm` handle (adjudicated repair option 1): the pivotal accept now
  requires `hdr = dataTerm`, and the poisoned candidate's handle still
  names its pre-bump term — the trace's final step no longer type-checks
  and both finding-theorems were DELETED (full trace in the LC1 task
  report §Finding #8).

## Status of the discharge

`frames_current_authored : Reachable w → FramesCurrentAuthored w` is
**PROVEN** (task LC3, `Uc2Proofs/ReportProvenance.lean`): the predicate is
the `fca` clause of the message-indexed provenance bundle
`Uc2.Cert.ProvInv`, whose frame/gossip-indexed clauses carry the D-tenure
attribution through the append-only wire — closing the serveTail residual
(a header-`D` frame accepted AFTER the `D`-leader stepped down) that no
state-only invariant could see. The definition stays HERE, as the predicate
the endgame consumes; the discharge and its supporting bundle live in
`ReportProvenance.lean` (which imports this file). -/

namespace Uc2.Cert

/-- Trace-discharge helper for `leaderAdvanceCommit`'s enabling (the LB1
pattern, re-proved locally — `ProtocolCommit.lean`'s copy is private): the
kernel cannot `decide`-reduce `advance` (`List.mergeSort` is well-founded
recursion), so the trace world's tracker/durable are pinned by kernel
`decide` and the concrete advance is discharged by `simp` once, here. -/
private theorem advance_fires {t : CommitTracker} {own : Nat}
    (t' : CommitTracker) (own' k : Nat)
    (ht : t = t') (hown : own = own')
    (hk : (t'.advance own').2 = some k) :
    (t.advance own).2 = some k := by
  subst ht
  subst hown
  exact hk

/-! ## `FramesCurrentAuthored` — the Option-2 provenance predicate (LB2b)

### Design (LB2b, unchanged by LC1)

The Rust invariant the single-term frame model lost was: **the term MAP is
exactly the receiver's own record of which header-authenticated segment
produced which byte range** — `observeTerm` grows the map only when a
just-accepted stamp exceeds the map's frontier, and Rust's exact
header-match means every BYTE accepted while the map's frontier sits at
term `u` was authenticated by the term-`u` stream. Consequently, in real
UC, a node's held content at a position is ALWAYS the term its OWN term
map attributes to that position — `hist j p = some (t, v) →
termMap_j.termAt p = t`. `FramesCurrentAuthored` asserts exactly this,
for every node, at the given world.

It is UNAFFECTED by legitimate inherited-prefix catch-up: `observeTerm`
only ever GROWS the map, so a genuine catch-up delivery (old stamp,
landing at a position the map's existing entries already attribute to that
stamp) leaves `termAt` unchanged and consistent — witnessed reachably by
`nonvacuity_serve_tail_catchup_trace` below. It excluded Finding #7's
replay (stamp 2 at a position the reconciled map attributed to term 3),
and it was violated by Finding #8's candidate-window accept (stamp 1 at a
position the carried-over map attributes to term 2) until LC1b keyed the
guard to the `dataTerm` handle.

### Faithfulness

The predicate speaks ONLY about a node's own two local fields (`hist`,
`termMap`) agreeing with each other — the direct, node-local consequence
of "every accepted byte was authenticated by the stream the map's frontier
currently names," which is what the exact header-match guard
(`receiver.rs:636-639`) plus the reconcile-before-data intake gate
(`receiver.rs:657`) jointly establish in Rust — where the compared term is
the node-level `term_handle`, mirrored since LC1b as the model's
`dataTerm` (module-doc History, Finding #8).

### Non-circularity

The predicate never mentions `committed`, never mentions "every leader has
the entry," and never mentions cross-node agreement; it is meaningful (and
checkable) per node from its own two fields alone, in worlds with an empty
`committed` ledger included. Any bridge to leader completeness is the
consuming theorem's own work (combining it with `Uc2.Data.log_matching`
and a committed-entry invariant — the LB2b report's proof strategy; the
arc's LC2–LC3 built the attribution-level (`termAt`) machinery, LC4 the
commit-event plumbing (`LcClosure.lean`), and the remaining ENTRY-level
canonical-prefix closure is the LC4b/LC4c arc — see
`.superpowers/sdd/task-LC4-report.md` for the design record and the
F-LC4-1 guard `bare_report_durable_stability_is_false`). -/

/-- **The Option-2 provenance predicate.** A node's held content always
agrees with what its OWN term map attributes to that position. See the
module doc immediately above for the design / faithfulness /
non-circularity arguments; in one line, this is the model-level trace of
Rust's exact DATA-header match (`receiver.rs:636-639`, against the
`dataTerm` handle since LC1b) plus the reconcile-before-data intake gate
(`receiver.rs:657`) — the latter now HANDLE-KEYED on both reopen arms
(Finding #9 fix, task LC2-fix). The unconditional discharge
(`Reachable w → FramesCurrentAuthored w`) is PROVEN:
`Uc2.Cert.frames_current_authored` in `Uc2Proofs/ReportProvenance.lean`
(task LC3), as the `fca` clause of the `ProvInv` bundle. -/
def FramesCurrentAuthored {n : Nat} (w : World n) : Prop :=
  ∀ j : Fin n, ∀ p t v : Nat, (w.nodes j).hist p = some (t, v) →
    Uc2.TermMap.termAt (w.nodes j).dn.termMap p = t

/-! ## Frame provenance (UNCONDITIONAL — no `hprov` needed)

Bookkeeping the endgame needs: every stamped history entry any node
currently holds traces back to an actual `replicate` frame that was put on
the wire — `hist` entries are never minted out of thin air. Only
`leaderAppend` (co-emits the matching frame in the same step) and
`deliverReplicate` (copies FROM an already-wired frame, `hmsg`) ever write a
`some` into `hist`; `serveTail` re-emits an existing entry's frame (a
fortiori covered); `deliverTermMap`'s truncation only ERASES entries. Pure
structural fact, independent of `hprov`. -/

/-- The frame-provenance invariant, carried through the induction. Post-LC1
(header/stamp split) the wire header is EXISTENTIAL: a held `(t, v)` at `p`
traces to a frame `replicate p hdr t v` for SOME header `hdr` (fresh appends
have `hdr = stamp`; `serveTail` re-serves keep the old stamp under a newer
header — the consumer cares about the record stamp, and a per-case-sharpened
`hdr` form would buy nothing downstream while complicating every erasure
case). -/
private def HistFrameProvenance {n : Nat} (w : World n) : Prop :=
  ∀ j : Fin n, ∀ p t v : Nat, (w.nodes j).hist p = some (t, v) →
    ∃ hdr, Uc2.Data.Frame.replicate p hdr t v ∈ w.dsent

private theorem hfp_init (n : Nat) : HistFrameProvenance (World.init n) := by
  intro j p t v h
  simp [World.init, Node.hist] at h

private theorem hfp_step {n : Nat} {w w' : World n} (h : HistFrameProvenance w)
    (hs : Step w w') : HistFrameProvenance w' := by
  cases hs with
  | startElection i _ =>
    intro k p t v hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t v hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t v hh
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    intro k p t v hh
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t v hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t v hh
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    intro k p t v hh
    exact h k p t v hh
  | deliverVote i v t hmsg hrole hterm =>
    intro k p t' v' hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t' v' hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t' v' hh
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    intro k p t' v' hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t' v' hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t' v' hh
  | becomeLeader i hrole hquorum =>
    intro k p t v hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t v hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t v hh
  | absorbDurable i =>
    intro k p t v hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t v hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t v hh
  | crashRestart i =>
    intro k p t v hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t v hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t v hh
  | leaderAppend i v hrole =>
    intro j p t v' hh
    rcases eq_or_ne i j with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh
      by_cases hp : p = (w.nodes i).pn.durable
      · subst hp
        rw [Function.update_self, Option.some.injEq, Prod.mk.injEq] at hh
        refine ⟨(w.nodes i).pn.currentTerm, ?_⟩
        rw [← hh.1, ← hh.2]
        exact List.mem_append_right _ (by simp)
      · rw [Function.update_of_ne hp] at hh
        obtain ⟨hdr, hf⟩ := h i p t v' hh
        exact ⟨hdr, List.mem_append_left _ hf⟩
    · simp only [Node.hist, Function.update_of_ne (Ne.symm hne)] at hh
      obtain ⟨hdr, hf⟩ := h j p t v' hh
      exact ⟨hdr, List.mem_append_left _ hf⟩
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate =>
    intro k p t' v' hh
    rcases eq_or_ne j k with rfl | hne
    · simp only [Node.hist, Function.update_self, Uc2.Data.Node.recvReplicate] at hh
      by_cases hp : p = pos
      · subst hp
        rw [Function.update_self, Option.some.injEq, Prod.mk.injEq] at hh
        refine ⟨hdr, ?_⟩
        rw [← hh.1, ← hh.2]
        exact hmsg
      · rw [Function.update_of_ne hp] at hh
        exact h j p t' v' hh
    · simp only [Node.hist, Function.update_of_ne (Ne.symm hne)] at hh
      exact h k p t' v' hh
  | serveTail i p t v hrole hhist hp =>
    intro k p' t' v' hh
    obtain ⟨hdr, hf⟩ := h k p' t' v' hh
    exact ⟨hdr, List.mem_append_left _ hf⟩
  | shipTermMap i hrole =>
    intro k p t v hh
    obtain ⟨hdr, hf⟩ := h k p t v hh
    exact ⟨hdr, List.mem_append_left _ hf⟩
  | deliverTermMap j t entries hmsg hterm =>
    intro k p t' v' hh
    rcases eq_or_ne j k with rfl | hne
    · simp only [Node.hist, Function.update_self, Uc2.Data.Node.applyGossip] at hh
      cases hrec : Uc2.reconcile (w.nodes j).dn.termMap (w.nodes j).dn.pn.durable
          entries with
      | ok o =>
        rw [hrec] at hh
        dsimp only at hh
        split at hh
        · exact h j p t' v' hh
        · cases hh
      | noCommonPrefix =>
        rw [hrec] at hh
        dsimp only at hh
        cases hh
    · simp only [Node.hist, Function.update_of_ne (Ne.symm hne)] at hh
      exact h k p t' v' hh
  | sendReport j hrole hgate =>
    intro k p t v hh
    exact h k p t v hh
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    intro k p t' v' hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t' v' hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t' v' hh
  | leaderAdvanceCommit i k hrole hbase hadv =>
    intro j p t v' hh
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h j p t v' hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h j p t v' hh

/-- **Frame provenance.** Every currently-held stamped history entry traces
back to an actual `replicate` frame on the wire. Unconditional (no `hprov`):
pure structural bookkeeping, established by induction over `Reachable`. -/
theorem hist_frame_provenance {n : Nat} {w : World n} (hw : Reachable w) :
    HistFrameProvenance w := by
  induction hw with
  | refl => exact hfp_init n
  | tail _ hstep ih => exact hfp_step ih hstep

/-- A committed entry's `(position, stamp, payload)` traces back to an
actual `replicate` frame on the wire — the corollary of frame provenance at
the `leaderAdvanceCommit` event (`ghostEntries` reads straight off the
committing leader's `hist`). Unconditional (no `hprov`). -/
private def CommittedFrameProvenance {n : Nat} (w : World n) : Prop :=
  ∀ p stamp T v : Nat, (p, stamp, T, v) ∈ w.committed →
    ∃ hdr, Uc2.Data.Frame.replicate p hdr stamp v ∈ w.dsent

private theorem cfp_init (n : Nat) : CommittedFrameProvenance (World.init n) := by
  intro p stamp T v h
  simp [World.init] at h

private theorem cfp_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : CommittedFrameProvenance w) (hs : Step w w') :
    CommittedFrameProvenance w' := by
  cases hs with
  | startElection _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | deliverRequestVote _ _ _ _ _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | rejectStaleRequestVote _ _ _ _ _ _ _ =>
    intro p stamp T v hh; exact h p stamp T v hh
  | deliverVote _ _ _ _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | deliverVoteHigherTerm _ _ _ _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | becomeLeader _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | absorbDurable _ => intro p stamp T v hh; exact h p stamp T v hh
  | crashRestart _ => intro p stamp T v hh; exact h p stamp T v hh
  | leaderAppend i v hrole =>
    intro p stamp T v' hh
    obtain ⟨hdr, hf⟩ := h p stamp T v' hh
    exact ⟨hdr, List.mem_append_left _ hf⟩
  | deliverReplicate _ _ _ _ _ _ _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | serveTail i p t v hrole hhist hp =>
    intro p' stamp T v' hh
    obtain ⟨hdr, hf⟩ := h p' stamp T v' hh
    exact ⟨hdr, List.mem_append_left _ hf⟩
  | shipTermMap i hrole =>
    intro p stamp T v hh
    obtain ⟨hdr, hf⟩ := h p stamp T v hh
    exact ⟨hdr, List.mem_append_left _ hf⟩
  | deliverTermMap _ _ _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | sendReport _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | deliverReport _ _ _ _ _ _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | leaderAdvanceCommit i k hrole hbase hadv =>
    intro p stamp T v hh
    rcases List.mem_append.mp hh with hold | hnew
    · exact h p stamp T v hold
    · obtain ⟨p', hp', heq⟩ := List.mem_filterMap.mp hnew
      cases hcase : (w.nodes i).dn.hist p' with
      | none => rw [hcase] at heq; simp at heq
      | some tv =>
        rw [hcase] at heq
        simp only [Option.map_some, Option.some.injEq, Prod.mk.injEq] at heq
        obtain ⟨rfl, rfl, rfl, rfl⟩ := heq
        exact hist_frame_provenance hw i p' tv.1 tv.2 hcase

/-- **Committed-entry frame provenance.** Unconditional (no `hprov`):
established by induction over `Reachable`. -/
theorem committed_frame_provenance {n : Nat} {w : World n} (hw : Reachable w) :
    CommittedFrameProvenance w := by
  induction hw with
  | refl => exact cfp_init n
  | tail hprev hstep ih => exact cfp_step hprev ih hstep

#print axioms hist_frame_provenance
#print axioms committed_frame_provenance

/-! ## Non-vacuity of `FramesCurrentAuthored` + LC-core premises

Required deliverable (LB2b, re-greened by LC1 under the split frame
shape): a reachable world satisfying BOTH `FramesCurrentAuthored` and
non-trivial LC-core premises — with the unconditional discharge refuted
(Finding #8), this stays the witness that the predicate is satisfiable on
honest commit traces, not vacuous.
Reuses LB1's `nonvacuity_commit_completeness_trace`
shape (`ProtocolCommit.lean`) — the SAME 14-step trace, rebuilt here so its
concrete `hist`/`termMap` values are available to check `FramesCurrentAuthored`
directly: node 0 leads term 1, appends `42` at position 0, gossips, node 1
reconciles CLEANLY (`[(1,0)]`, no truncation — the trace never diverges) and
replicates the SAME byte at its OWN current term, so every held entry is
either fresh (`t = currentTerm`) or matches the frozen `[(1,0)]` map — never a
cross-stream replay. This confirms `hprov` is satisfied by ordinary,
current-leader-authored replication, exactly the brief's claim. -/
/-- The concrete trace, bundled with every DECIDABLE fact about it the
`FramesCurrentAuthored` proof below needs (`w`'s `Reachable` witness is
established here, ALONGSIDE the facts, while `w` is still the concrete,
kernel-transparent term the `refine` unifies — `decide` cannot see through it
once `obtain` turns `w` into an opaque local, so every decidable fact is
harvested in this one shot). -/
private theorem nonvacuity_lc_trace :
    ∃ w : World 3, Reachable w ∧
      (0, 1, 1, 42) ∈ w.committed ∧
      (w.nodes 1).pn.role = .leader ∧
      1 < (w.nodes 1).pn.currentTerm ∧
      w.dsent = [.replicate 0 1 1 42, .gossip 1 [(1, 0)]] ∧
      (w.nodes 0).dn.termMap.termAt 0 = 1 ∧
      (w.nodes 1).dn.termMap.termAt 0 = 1 ∧
      (w.nodes 2).hist 0 = none := by
  refine ⟨_,
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail
      (.single (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 0 1 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 42 (by decide)))
      (.shipTermMap _ 0 (by decide)))
      (.deliverTermMap _ 1 1 [(1, 0)] (by decide) (by decide)))
      (.deliverReplicate _ 1 0 1 1 42 (by decide) (by decide) (by decide)
        (by decide)))
      (.sendReport _ 1 (by decide) (by decide)))
      (.deliverReport _ 0 1 1 1 (by decide) (by decide) (by decide)
        (by decide)))
      (.leaderAdvanceCommit _ 0 1 (by decide) ⟨(1, 0), by decide⟩
        (advance_fires ⟨[1, 0], 2, 0⟩ 1 1 (by decide) (by decide)
          (by simp [CommitTracker.advance, CommitTracker.ranking,
                List.mergeSort]))))
      (.startElection _ 1 (by decide)))
      (.deliverRequestVote _ 2 1 2 1 1 (by decide) (by decide)))
      (.deliverVote _ 1 2 2 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 1 (by decide) (by decide)),
    by decide, by decide, by decide, by decide, by decide, by decide, by decide⟩

theorem nonvacuity_leader_completeness_trace :
    ∃ w : World 3, Reachable w ∧ FramesCurrentAuthored w ∧
      (0, 1, 1, 42) ∈ w.committed ∧
      (w.nodes 1).pn.role = .leader ∧
      1 < (w.nodes 1).pn.currentTerm := by
  obtain ⟨w, hw, hc, hrole, hterm, hdsent, ht0, ht1, hh2⟩ := nonvacuity_lc_trace
  refine ⟨w, hw, ?_, hc, hrole, hterm⟩
  intro j p t v hh
  -- Route through the UNCONDITIONAL frame-provenance lemma: `dsent` is flat
  -- (not per-node `Function.update`-nested), so pinning `(p, t, v)` this way
  -- avoids ever unfolding the 14-step `hist` chain symbolically.
  obtain ⟨hdr, hf⟩ := hist_frame_provenance hw j p t v hh
  rw [hdsent] at hf
  simp only [List.mem_cons, List.not_mem_nil, or_false,
    Uc2.Data.Frame.replicate.injEq] at hf
  rcases hf with ⟨rfl, rfl, rfl, rfl⟩ | hf
  · have hj : j = 0 ∨ j = 1 ∨ j = 2 := by omega
    rcases hj with rfl | rfl | rfl
    · exact ht0
    · exact ht1
    · rw [hh2] at hh; cases hh
  · exact absurd hf (by simp)

#print axioms nonvacuity_leader_completeness_trace

/-! ## Non-vacuity of `serveTail` catch-up (the amendment did not gut #6a)

The rerun report's rejected-alternative hazard: an exact-match delivery
guard on the SINGLE-term frame would have made old-stamped bytes
unre-acquirable after truncation, gutting the inherited-prefix catch-up
that the #6a/Fig-8 re-key exists for. The header/stamp split + `serveTail`
must keep it alive: this trace shows a follower whose divergent tail dies
in a reconcile and which then RE-ACQUIRES an old-stamped inherited byte
from the new leader's `serveTail` re-serve — old stamp, CURRENT header —
ending durably past it on the live stream. -/

/-- **Non-vacuity (`serveTail` catch-up).** Node 0 leads term 1 with two
bytes; node 1 replicates only the first. Node 1 wins term 2 on `(1, 1)`
and appends a divergent `(pos 1, stamp 2, 20)`. Node 0 re-wins term 3
(inheriting its term-1 bytes below the new base 2) and appends
`(pos 2, stamp 3, 31)`. Node 1 reconciles against the term-3 map
`[(1,0),(3,2)]`: its divergent tail dies at `validUpTo = 1` (gate reopens).
Node 0 `serveTail`s its inherited `(pos 1, stamp 1, 11)` byte under
header 3; node 1 re-acquires it at its truncated frontier — the exact
catch-up path Rust's NAK-repair/journal-replay serves inside the current
leader's stream — then takes the live `(2, 3, 3, 31)` frame, ending at
durable 3. -/
theorem nonvacuity_serve_tail_catchup_trace :
    ∃ w w' w'' w''' w'''' : World 3,
      Reachable w ∧ Step w w' ∧ Step w' w'' ∧ Step w'' w''' ∧
      Step w''' w'''' ∧
      -- divergent tail before the reconcile:
      (w.nodes 1).hist 1 = some (2, 20) ∧
      -- the term-3 gossip truncates it and reopens the gate:
      (w'.nodes 1).hist 1 = none ∧ (w'.nodes 1).pn.durable = 1 ∧
      (w'.nodes 1).reconciled = true ∧
      -- the follower RE-ACQUIRES the old-stamped byte via serveTail:
      (w'''.nodes 1).hist 1 = some (1, 11) ∧
      -- and ends durably past it on the live stream:
      (w''''.nodes 1).hist 2 = some (3, 31) ∧
      (w''''.nodes 1).pn.durable = 3 := by
  refine ⟨_, _, _, _, _,
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail
      -- t1: node 0 leads {0,2}, appends 10@0 and 11@1; node 1 reconciles
      -- (adopt + gate open) and replicates only byte 0.
      (.single (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 2 0 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 0 2 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 10 (by decide)))
      (.leaderAppend _ 0 11 (by decide)))
      (.shipTermMap _ 0 (by decide)))
      (.deliverTermMap _ 1 1 [(1, 0)] (by decide) (by decide)))
      (.deliverReplicate _ 1 0 1 1 10 (by decide) (by decide) (by decide)
        (by decide)))
      -- t2: node 1 wins term 2 on (lastTerm 1, durable 1) and appends the
      -- divergent (1, 2, 20) — map [(1,0),(2,1)], durable 2.
      (.startElection _ 1 (by decide)))
      (.deliverRequestVote _ 2 1 2 1 1 (by decide) (by decide)))
      (.deliverVote _ 1 2 2 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 1 (by decide) (by decide)))
      (.leaderAppend _ 1 20 (by decide)))
      -- t3: node 0 crash-opens and re-wins term 3 on (lastTerm 1,
      -- durable 2) — map [(1,0),(3,2)], its term-1 bytes inherited —
      -- appends 31@2 and gossips.
      (.crashRestart _ 0))
      (.startElection _ 0 (by decide)))
      (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 2 0 3 1 2 (by decide) (by decide)))
      (.deliverVote _ 0 2 3 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 31 (by decide)))
      (.shipTermMap _ 0 (by decide)),
    .deliverTermMap _ 1 3 [(1, 0), (3, 2)] (by decide) (by decide),
    .serveTail _ 0 1 1 11 (by decide) (by decide) (by decide),
    .deliverReplicate _ 1 1 3 1 11 (by decide) (by decide) (by decide)
      (by decide),
    .deliverReplicate _ 1 2 3 3 31 (by decide) (by decide) (by decide)
      (by decide),
    by decide, by decide, by decide, by decide, by decide, by decide,
    by decide⟩

#print axioms nonvacuity_serve_tail_catchup_trace

/-! ## Finding #9 — the candidate-window GATE REOPEN (found here, FIXED)

**History (decision-3 precedent: the finding theorems are DELETED once the
fix lands, trace preserved here).** LC2's first pass REFUTED
`frames_current_authored` under the LC1b `dataTerm` DELIVERY guard: the
intake-gate REOPEN was still keyed to `currentTerm`, not the handle. A
CANDIDATE (handle T, `currentTerm` T+1, gate latched closed by its
pre-candidacy adoption of T) that cleanly reconciled a (T+1)-leader's map
reopened intake for its stale handle-T stream, then accepted a term-T
`serveTail` byte its map never attributed — hist/map disagreement, and
(role-blind reports) an acked-write-loss phantom of the §5.4.2 / #6b
family. The n=5, 56-step kernel countermodel
`finding_candidate_gate_reopen_fca_violation` +
`fca_gate_reopen_unconditional_is_false` lived here.

Adjudicated a REAL reachable gap (independent adversarial review verified
the loss chain link-by-link in Rust: role-blind handle-stamped reports
`receiver.rs:1049-1078`, report retarget 648-650, same-term fold
`election.rs:545-573`, position-only #6b clamp 1421-1456) and FIXED
(task LC2-fix): BOTH Rust reopen arms — the clean-reconcile arm
(`node.rs` ~2403-2413) and the truncation-ack arm (`on_truncated`
~2731-2743) — are keyed to `current_term == adopted_term` (== the
`term_handle` the receiver filters DATA at); a candidate's handle lags, so
it never reopens. Mirrored in `ProtocolCommit.lean`'s `deliverTermMap`
(`reconciled := true` only when the post-step regime is handle-aligned).
Under the fix the countermodel's penultimate step (the candidate's clean
reconcile setting `reconciled := true`) no longer holds — the subsequent
`deliverReplicate`'s `hgate` is unprovable — so both finding theorems were
DELETED (this section) in the same commit as the fix. Sim regression pin:
`uc2_sim` `finding9_lagged_handle_candidate_reopen_needs_handle_keyed`
(RED under the `handle_keyed:false` counterfactual → GREEN under the fix).

`frames_current_authored` itself is PROVEN on the repaired (handle-keyed
reopen) semantics — task LC3, `Uc2Proofs/ReportProvenance.lean`. -/

end Uc2.Cert
