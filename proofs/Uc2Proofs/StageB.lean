import Uc2Proofs.Vote
import Uc2Proofs.TakeDiscipline

/-! LC4c Stage B — the credential-recording layer over the take discipline.

Consumes Stage A (`TakeDiscipline.lean`) and the LC4 plumbing
(`LcClosure.lean`); extends via its own bundles per the standing L3 rule
(ProvInv untouched). Landed here:

- support inductions: `reachable_lastTerm_sync` (the `logOk` input IS the
  map frontier), `reachable_gossip_le` (gossip entries never out-term
  their gossip), `reachable_orig` (every held/shipped stamp traces to its
  ORIGIN frame `replicate p t t v` — the tenure-t append of position `p`);
- `RepQuorum` — the commit-side quorum witness (certificate + base-frame +
  a `Finset` of `k`-floored reports), produced by every commit
  (`committed_repquorum`);
- `report_era_floor` (B1) — the era-conditioned reporter facts: while NO
  gossip above `T` exists on the wire (the only damage vehicle — review
  M-1), a `T`-reporter keeps its reported durable, and its map is either
  strictly past `T` or `T`-frontier take-disciplined with frame-pinned
  attribution;
- `grant_report` (B2/M1, the F-LC4-1-corrected, M-1-corrected form) — a
  grant above a reported term records the voter's `logOk`-transported
  credentials against the candidate's requestVote, with the damage escape
  GOSSIP-witnessed and the good arms conditioned on a below-`d` tenure-`T`
  append frame (supplied by `RepQuorum`'s base witness at consumption).

The `no_branch`/`no_branch_frame` pair is NOT here: its preservation
(bounding a reconcile CUT from an entry bound alone) turned out to require
sub-`k` canonical-prefix agreement — Stage C's canon — see the LC4c report
section (the review's verdict-3a(ii) glossed this dependence). -/

namespace Uc2.Cert

open Uc2.Data (Frame)

/-! ## `lastTerm` is the map frontier (the `logOk` input, made readable) -/

/-- `pn.lastTerm = lastTermOf termMap` — the derived-credential discipline
of `ProtocolData.lean` (module doc, item 3), as an invariant. -/
def LastTermSync {n : Nat} (w : World n) : Prop :=
  ∀ j : Fin n, (w.nodes j).pn.lastTerm = Data.lastTermOf (w.nodes j).dn.termMap

private theorem recv_lastTerm {n : Nat} (s : PNode n) (c : Fin n)
    (nt lt d : Nat) :
    ((s.recvRequestVote c nt lt d).1).lastTerm = s.lastTerm := by
  by_cases hadopt : s.currentTerm < nt
  · simp only [PNode.recvRequestVote, if_pos hadopt, PNode.adoptTerm,
      PNode.recvRequestVote.grantIfFresh]
    split_ifs <;> rfl
  · rcases hvf : s.votedFor with _ | ⟨vt, vid⟩ <;>
      simp only [PNode.recvRequestVote, if_neg hadopt, hvf,
        PNode.recvRequestVote.grantIfFresh] <;>
      split_ifs <;> rfl

private theorem lts_step {n : Nat} {w w' : World n} (h : LastTermSync w)
    (hs : Step w w') : LastTermSync w' := by
  cases hs with
  | startElection i _ =>
    intro k
    rcases eq_or_ne k i with rfl | hne
    · simpa [Node.pn, Function.update_self] using h k
    · simpa [Node.pn, Function.update_of_ne hne] using h k
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    intro k
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.pn, Function.update_self]
      rw [recv_lastTerm]
      exact h k
    · simpa [Node.pn, Function.update_of_ne hne] using h k
  | rejectStaleRequestVote j c nt clt cd hmsg hstale => exact h
  | deliverVote i v t hmsg hrole hterm =>
    intro k
    rcases eq_or_ne k i with rfl | hne
    · simpa [Node.pn, Function.update_self] using h k
    · simpa [Node.pn, Function.update_of_ne hne] using h k
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    intro k
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self, PNode.adoptTerm]
      exact h k
    · simpa [Node.pn, Function.update_of_ne hne] using h k
  | becomeLeader i hrole hquorum =>
    intro k
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self]
      rw [Data.lastTermOf_prunePush]
    · simpa [Node.pn, Function.update_of_ne hne] using h k
  | absorbDurable i hrole =>
    intro k
    rcases eq_or_ne k i with rfl | hne
    · simpa [Node.pn, Function.update_self] using h k
    · simpa [Node.pn, Function.update_of_ne hne] using h k
  | crashRestart i =>
    intro k
    rcases eq_or_ne k i with rfl | hne
    · simpa [Node.pn, Function.update_self] using h k
    · simpa [Node.pn, Function.update_of_ne hne] using h k
  | leaderAppend i v hrole =>
    intro k
    rcases eq_or_ne k i with rfl | hne
    · simpa [Node.pn, Function.update_self] using h k
    · simpa [Node.pn, Function.update_of_ne hne] using h k
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate =>
    intro k
    rcases eq_or_ne k j with rfl | hne
    · simp [Node.pn, Function.update_self, Uc2.Data.Node.recvReplicate]
    · simpa [Node.pn, Function.update_of_ne hne] using h k
  | serveTail i p t v hrole hhist hp => exact h
  | shipTermMap i hrole => exact h
  | deliverTermMap j t entries hmsg hterm =>
    intro k
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.pn, Function.update_self]
      cases hrec : Uc2.reconcile (w.nodes k).dn.termMap
          (w.nodes k).dn.pn.durable entries with
      | ok o =>
        obtain ⟨hmapE, -, -, -, -, -⟩ :=
          Data.applyGossip_ok (w.nodes k).dn t hrec
        have hlt : ((w.nodes k).dn.applyGossip t entries).pn.lastTerm
            = Data.lastTermOf o.newMap := by
          simp [Uc2.Data.Node.applyGossip, hrec]
        rw [hlt, hmapE]
      | noCommonPrefix =>
        obtain ⟨hmapE, -, -, -, -, -⟩ :=
          Data.applyGossip_ncp (w.nodes k).dn t hrec
        have hlt : ((w.nodes k).dn.applyGossip t entries).pn.lastTerm = 0 := by
          simp [Uc2.Data.Node.applyGossip, hrec]
        rw [hlt, hmapE]
        rfl
    · simpa [Node.pn, Function.update_of_ne hne] using h k
  | sendReport j hrole hgate => exact h
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    intro k
    rcases eq_or_ne k i with rfl | hne
    · simpa [Node.pn, Function.update_self] using h k
    · simpa [Node.pn, Function.update_of_ne hne] using h k
  | leaderAdvanceCommit i kk hrole hbase hadv =>
    intro k
    rcases eq_or_ne k i with rfl | hne
    · simpa [Node.pn, Function.update_self] using h k
    · simpa [Node.pn, Function.update_of_ne hne] using h k

/-- **`lastTerm` sync** in every reachable world. -/
theorem reachable_lastTerm_sync {n : Nat} {w : World n} (hw : Reachable w) :
    LastTermSync w := by
  induction hw with
  | refl => intro j; rfl
  | tail _ hstep ih => exact lts_step ih hstep

#print axioms reachable_lastTerm_sync

/-! ## Gossip entries never out-term their gossip -/

/-- Shipped maps are leader maps, and a leader's map terms are bounded by
its term (`NodeWF.map_le` at every ship). -/
def GossipLe {n : Nat} (w : World n) : Prop :=
  ∀ t es, Frame.gossip t es ∈ w.dsent → ∀ e ∈ es, e.1 ≤ t

private theorem gle_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : GossipLe w) (hs : Step w w') : GossipLe w' := by
  cases hs with
  | leaderAppend i v hrole =>
    intro t es hg
    rcases List.mem_append.mp hg with hg | hg
    · exact h t es hg
    · simp at hg
  | serveTail i p t v hrole hhist hp =>
    intro t' es hg
    rcases List.mem_append.mp hg with hg | hg
    · exact h t' es hg
    · simp at hg
  | shipTermMap i hrole =>
    intro t es hg
    rcases List.mem_append.mp hg with hg | hg
    · exact h t es hg
    · simp only [List.mem_singleton, Frame.gossip.injEq] at hg
      obtain ⟨rfl, rfl⟩ := hg
      exact fun e he =>
        ((Data.reachable_mapInv (reachable_project hw)).node i).map_le e he
  | startElection i hrole => exact h
  | deliverRequestVote j c nt clt cd hmsg hterm => exact h
  | rejectStaleRequestVote j c nt clt cd hmsg hstale => exact h
  | deliverVote i v t hmsg hrole hterm => exact h
  | deliverVoteHigherTerm i v t g hmsg hterm => exact h
  | becomeLeader i hrole hquorum => exact h
  | absorbDurable i hrole => exact h
  | crashRestart i => exact h
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate => exact h
  | deliverTermMap j t entries hmsg hterm => exact h
  | sendReport j hrole hgate => exact h
  | deliverReport i src t d hmsg hrole hterm hsrc => exact h
  | leaderAdvanceCommit i k hrole hbase hadv => exact h

/-- **Gossip term bound** in every reachable world. -/
theorem reachable_gossip_le {n : Nat} {w : World n} (hw : Reachable w) :
    GossipLe w := by
  induction hw with
  | refl => intro t es hg; simp [World.init] at hg
  | tail hprev hstep ih => exact gle_step hprev ih hstep

#print axioms reachable_gossip_le

/-! ## Origin frames: every stamp traces to its tenure's own append -/

/-- Every held byte and every shipped frame traces to the ORIGIN frame
`replicate p t t v` — position `p` as appended by the stamp-`t` tenure
itself (`leaderAppend` emits `hdr = stamp`; `serveTail` re-serves a held
stamp whose origin is already on the wire; delivery copies a wired
frame). -/
def OrigInv {n : Nat} (w : World n) : Prop :=
  (∀ j : Fin n, ∀ p t v, (w.nodes j).hist p = some (t, v) →
    Frame.replicate p t t v ∈ w.dsent) ∧
  (∀ p hdr t v, Frame.replicate p hdr t v ∈ w.dsent →
    Frame.replicate p t t v ∈ w.dsent)

private theorem orig_step {n : Nat} {w w' : World n} (h : OrigInv w)
    (hs : Step w w') : OrigInv w' := by
  obtain ⟨hh, hf⟩ := h
  cases hs with
  | leaderAppend i v hrole =>
    constructor
    · intro k p t v' hhk
      rcases eq_or_ne k i with rfl | hne
      · simp only [Node.hist, Function.update_self] at hhk
        by_cases hpd : p = (w.nodes k).pn.durable
        · subst hpd
          rw [Function.update_self, Option.some.injEq, Prod.mk.injEq] at hhk
          obtain ⟨rfl, rfl⟩ := hhk
          exact List.mem_append_right _ (by simp)
        · rw [Function.update_of_ne hpd] at hhk
          exact List.mem_append_left _ (hh k p t v' hhk)
      · simp only [Node.hist, Function.update_of_ne hne] at hhk
        exact List.mem_append_left _ (hh k p t v' hhk)
    · intro p hdr t v' hfk
      rcases List.mem_append.mp hfk with hfk | hfk
      · exact List.mem_append_left _ (hf p hdr t v' hfk)
      · simp only [List.mem_singleton, Frame.replicate.injEq] at hfk
        obtain ⟨rfl, rfl, rfl, rfl⟩ := hfk
        exact List.mem_append_right _ (by simp)
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate =>
    constructor
    · intro k p t' v' hhk
      rcases eq_or_ne k j with rfl | hne
      · simp only [Node.hist, Function.update_self,
          Uc2.Data.Node.recvReplicate] at hhk
        by_cases hpd : p = pos
        · subst hpd
          rw [Function.update_self, Option.some.injEq, Prod.mk.injEq] at hhk
          obtain ⟨rfl, rfl⟩ := hhk
          exact hf p hdr t v hmsg
        · rw [Function.update_of_ne hpd] at hhk
          exact hh k p t' v' hhk
      · simp only [Node.hist, Function.update_of_ne hne] at hhk
        exact hh k p t' v' hhk
    · exact hf
  | serveTail i p t v hrole hhist hp =>
    constructor
    · intro k p' t' v' hhk
      exact List.mem_append_left _ (hh k p' t' v' hhk)
    · intro p' hdr t' v' hfk
      rcases List.mem_append.mp hfk with hfk | hfk
      · exact List.mem_append_left _ (hf p' hdr t' v' hfk)
      · simp only [List.mem_singleton, Frame.replicate.injEq] at hfk
        obtain ⟨rfl, rfl, rfl, rfl⟩ := hfk
        exact List.mem_append_left _ (hh i p' t' v' hhist)
  | deliverTermMap j t entries hmsg hterm =>
    constructor
    · intro k p t' v' hhk
      rcases eq_or_ne k j with rfl | hne
      · simp only [Node.hist, Function.update_self] at hhk
        exact hh k p t' v'
          (Data.applyGossip_hist (w.nodes k).dn t entries p hhk)
      · simp only [Node.hist, Function.update_of_ne hne] at hhk
        exact hh k p t' v' hhk
    · exact hf
  | shipTermMap i hrole =>
    constructor
    · intro k p t v hhk
      exact List.mem_append_left _ (hh k p t v hhk)
    · intro p hdr t v hfk
      rcases List.mem_append.mp hfk with hfk | hfk
      · exact List.mem_append_left _ (hf p hdr t v hfk)
      · simp at hfk
  | startElection i hrole =>
    refine ⟨fun k p t v hhk => ?_, hf⟩
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hhk
      exact hh k p t v hhk
    · simp only [Node.hist, Function.update_of_ne hne] at hhk
      exact hh k p t v hhk
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    refine ⟨fun k p t v hhk => ?_, hf⟩
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.hist, Function.update_self] at hhk
      exact hh k p t v hhk
    · simp only [Node.hist, Function.update_of_ne hne] at hhk
      exact hh k p t v hhk
  | rejectStaleRequestVote j c nt clt cd hmsg hstale => exact ⟨hh, hf⟩
  | deliverVote i v t hmsg hrole hterm =>
    refine ⟨fun k p t' v' hhk => ?_, hf⟩
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hhk
      exact hh k p t' v' hhk
    · simp only [Node.hist, Function.update_of_ne hne] at hhk
      exact hh k p t' v' hhk
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    refine ⟨fun k p t' v' hhk => ?_, hf⟩
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hhk
      exact hh k p t' v' hhk
    · simp only [Node.hist, Function.update_of_ne hne] at hhk
      exact hh k p t' v' hhk
  | becomeLeader i hrole hquorum =>
    refine ⟨fun k p t v hhk => ?_, hf⟩
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hhk
      exact hh k p t v hhk
    · simp only [Node.hist, Function.update_of_ne hne] at hhk
      exact hh k p t v hhk
  | absorbDurable i hrole =>
    refine ⟨fun k p t v hhk => ?_, hf⟩
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hhk
      exact hh k p t v hhk
    · simp only [Node.hist, Function.update_of_ne hne] at hhk
      exact hh k p t v hhk
  | crashRestart i =>
    refine ⟨fun k p t v hhk => ?_, hf⟩
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hhk
      exact hh k p t v hhk
    · simp only [Node.hist, Function.update_of_ne hne] at hhk
      exact hh k p t v hhk
  | sendReport j hrole hgate => exact ⟨hh, hf⟩
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    refine ⟨fun k p t' v' hhk => ?_, hf⟩
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hhk
      exact hh k p t' v' hhk
    · simp only [Node.hist, Function.update_of_ne hne] at hhk
      exact hh k p t' v' hhk
  | leaderAdvanceCommit i k hrole hbase hadv =>
    refine ⟨fun k' p t v hhk => ?_, hf⟩
    rcases eq_or_ne k' i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hhk
      exact hh k' p t v hhk
    · simp only [Node.hist, Function.update_of_ne hne] at hhk
      exact hh k' p t v hhk

/-- **Origin frames** in every reachable world. -/
theorem reachable_orig {n : Nat} {w : World n} (hw : Reachable w) :
    OrigInv w := by
  induction hw with
  | refl =>
    constructor
    · intro j p t v hh
      simp [World.init, Node.hist] at hh
    · intro p hdr t v hf
      simp [World.init] at hf
  | tail _ hstep ih => exact orig_step ih hstep

#print axioms reachable_orig

/-! ## `RepQuorum` — the commit-side quorum witness -/

/-- The message-backed record of a term-`T` commit through `k`: the
tenure's writer certificate, its own base-append frame strictly below `k`
(the #6b `hbase` clamp, made wire-visible via `reachable_orig`), and a
member quorum each of whom is the writer or has a `k`-floored `T`-report
in flight. Everything here is monotone (certificates transport; the wires
are append-only; the `Finset` is data). -/
def RepQuorum {n : Nat} (w : World n) (T k : Nat) : Prop :=
  1 ≤ T ∧ ∃ (ℓ : Fin n) (Q : Finset (Fin n)) (bT v0 : Nat),
    Data.Cert w.project T ℓ ∧ bT < k ∧
    Frame.replicate bT T T v0 ∈ w.dsent ∧
    n / 2 + 1 ≤ Q.card ∧
    ∀ u ∈ Q, u = ℓ ∨ ∃ d, k ≤ d ∧ CMsg.report u T d ∈ w.csent

/-- Monotone transport of `RepQuorum` across any step. -/
theorem RepQuorum.step {n : Nat} {w w' : World n} (hs : Step w w')
    {T k : Nat} (h : RepQuorum w T k)
    (hds : ∀ f, f ∈ w.dsent → f ∈ w'.dsent)
    (hcs : ∀ m, m ∈ w.csent → m ∈ w'.csent) : RepQuorum w' T k := by
  obtain ⟨hT, ℓ, Q, bT, v0, hc, hb, hf, hcard, hQ⟩ := h
  refine ⟨hT, ℓ, Q, bT, v0, Data.cert_drtg (step_project hs) hc, hb,
    hds _ hf, hcard, ?_⟩
  intro u hu
  rcases hQ u hu with rfl | ⟨d, hkd, hm⟩
  · exact .inl rfl
  · exact .inr ⟨d, hkd, hcs _ hm⟩

/-- Every committed entry is covered by a `RepQuorum` reaching past it. -/
def CommittedRepQuorum {n : Nat} (w : World n) : Prop :=
  ∀ p stamp T v : Nat, (p, stamp, T, v) ∈ w.committed →
    ∃ k, p < k ∧ RepQuorum w T k

private theorem crq_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : CommittedRepQuorum w) (hs : Step w w') : CommittedRepQuorum w' := by
  have hstep := hs
  have hmono : ∀ (hds : ∀ f, f ∈ w.dsent → f ∈ w'.dsent)
      (hcs : ∀ m, m ∈ w.csent → m ∈ w'.csent)
      (hcm : ∀ e, e ∈ w'.committed → e ∈ w.committed),
      CommittedRepQuorum w' := by
    intro hds hcs hcm p stamp T v hc
    obtain ⟨k, hpk, hrq⟩ := h p stamp T v (hcm _ hc)
    exact ⟨k, hpk, hrq.step hstep hds hcs⟩
  cases hs
  case leaderAdvanceCommit i k hrole hbase hadv =>
    intro p stamp T v hc
    rcases List.mem_append.mp hc with hold | hnew
    · obtain ⟨k', hpk, hrq⟩ := h p stamp T v hold
      exact ⟨k', hpk, hrq.step hstep (fun f hf => hf) (fun m hm => hm)⟩
    · -- a fresh ghost entry: build the quorum witness at the advance
      obtain ⟨p', hp', heq⟩ := List.mem_filterMap.mp hnew
      have hrange := List.mem_range'.mp hp'
      cases hcase : (w.nodes i).dn.hist p' with
      | none => rw [hcase] at heq; simp at heq
      | some tv =>
        rw [hcase] at heq
        simp only [Option.map_some, Option.some.injEq, Prod.mk.injEq] at heq
        obtain ⟨rfl, rfl, rfl, rfl⟩ := heq
        have hadv' : (w.nodes i).tracker.advance (w.nodes i).pn.durable
            = (((w.nodes i).tracker.advance (w.nodes i).pn.durable).1,
                some k) := by
          rw [← hadv]
        have hkdur : k ≤ (w.nodes i).pn.durable :=
          CommitTracker.advance_le_own _ _ _ _ hadv'
        obtain ⟨Q, hcard, hiQ, hQ⟩ := advance_report_quorum hw i k hrole hadv
        obtain ⟨e, hlaste, hek⟩ := hbase
        -- the base entry is the tenure's own: term = currentTerm
        have hasc : TermMap.Ascending (w.nodes i).dn.termMap :=
          ((Data.reachable_mapsWF (reachable_project hw)) i).1
        have hmp : Data.lastTermOf (w.nodes i).dn.termMap
            = (w.nodes i).pn.currentTerm :=
          (Data.reachable_dinv (reachable_project hw)).map_pinned i hrole
        have hetf : e.1 = (w.nodes i).pn.currentTerm := by
          have := Data.lastTermOf_getLast hlaste
          omega
        -- the base position is durably held and stamped with the tenure term
        have hbdur : e.2 < (w.nodes i).pn.durable := by omega
        obtain ⟨tv0, hh0⟩ := reachable_hist_defined hw i e.2 hbdur
        have hattr : TermMap.termAt (w.nodes i).dn.termMap e.2 = tv0.1 := by
          have := (reachable_provInv hw).fca i e.2 tv0.1 tv0.2
            (by rw [hh0])
          exact this
        have hattr2 : TermMap.termAt (w.nodes i).dn.termMap e.2 = e.1 :=
          TermMap.termAt_of_last_base_le hasc hlaste (Nat.le_refl _)
        have hstamp0 : tv0.1 = (w.nodes i).pn.currentTerm := by omega
        have horig : Frame.replicate e.2 ((w.nodes i).pn.currentTerm)
            ((w.nodes i).pn.currentTerm) tv0.2 ∈ w.dsent := by
          have := (reachable_orig hw).1 i e.2 tv0.1 tv0.2 (by rw [hh0])
          rwa [hstamp0] at this
        refine ⟨k, by omega, ?_⟩
        refine ⟨((Data.reachable_mapInv
            (reachable_project hw)).node i).role_term_pos
          (by rw [show (w.project.nodes i).pn.role = Role.leader from hrole]
              decide),
          i, Q, e.2, tv0.2, ?_, by omega, horig, hcard, hQ⟩
        exact Data.cert_drtg (step_project hstep)
          (Data.cert_of_leader
            (Uc2.reachable_inv (Data.reachable_project (reachable_project hw)))
            hrole)
  all_goals
    first
    | exact hmono (fun f hf => hf) (fun m hm => hm) (fun e he => he)
    | exact hmono (fun f hf => List.mem_append_left _ hf) (fun m hm => hm)
        (fun e he => he)
    | exact hmono (fun f hf => hf) (fun m hm => List.mem_append_left _ hm)
        (fun e he => he)

/-- **Committed entries carry their quorum witness** in every reachable
world (the `RepQuorum` production site: the `leaderAdvanceCommit` event,
where the leader is live, `hbase` pins the tenure base below the advance,
and `advance_report_quorum` extracts the member `Finset`). -/
theorem reachable_committed_repquorum {n : Nat} {w : World n}
    (hw : Reachable w) : CommittedRepQuorum w := by
  induction hw with
  | refl => intro p stamp T v hc; simp [World.init] at hc
  | tail hprev hstep ih => exact crq_step hprev ih hstep

#print axioms reachable_committed_repquorum

/-- No gossip above `T` anywhere on the data wire. -/
def Era {n : Nat} (w : World n) (T : Nat) : Prop :=
  ∀ u es, Frame.gossip u es ∈ w.dsent → u ≤ T

/-! ## In-era regime pinning for open gates -/

/-- In an era with no gossip above `T`, an open gate at a regime above `T`
forces the map frontier up to the regime: the gate opened either against a
regime gossip (impossible above `T` in-era), at `becomeLeader` (frontier =
regime by the push), or at boot (frontier = regime by the boot predicate) —
and in-era no delivery can ever cut the frontier back (any gossip term is
`≤ T <` the node's current term). Sidesteps certificate-without-win
entirely (review M-1's mechanism). -/
def OpenFrontierEra {n : Nat} (w : World n) : Prop :=
  ∀ T : Nat, Era w T → ∀ j : Fin n, (w.nodes j).reconciled = true →
    T < (w.nodes j).dataTerm →
    (w.nodes j).dataTerm ≤ Data.lastTermOf (w.nodes j).dn.termMap

private theorem ofe_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : OpenFrontierEra w) (hs : Step w w') : OpenFrontierEra w' := by
  have hstep := hs
  cases hs with
  | startElection i hrole =>
    intro T hera k hr hdt
    have hera' : Era w T := hera
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.dataTerm, Function.update_self] at hr hdt ⊢
      exact h T hera' k (by simpa [Function.update_self] using hr) hdt
    · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hdt ⊢
      exact h T hera' k (by simpa [Function.update_of_ne hne] using hr) hdt
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    intro T hera k hr hdt
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.dataTerm, Function.update_self] at hr hdt ⊢
      by_cases hadopt : (w.nodes k).pn.currentTerm < nt
      · rw [if_pos hadopt] at hr
        cases hr
      · rw [if_neg hadopt] at hr hdt ⊢
        exact h T hera k hr hdt
    · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hdt ⊢
      exact h T hera k (by simpa [Function.update_of_ne hne] using hr) hdt
  | rejectStaleRequestVote j c nt clt cd hmsg hstale => exact h
  | deliverVote i v t hmsg hrole hterm =>
    intro T hera k hr hdt
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.dataTerm, Function.update_self] at hr hdt ⊢
      exact h T hera k (by simpa [Function.update_self] using hr) hdt
    · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hdt ⊢
      exact h T hera k (by simpa [Function.update_of_ne hne] using hr) hdt
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    intro T hera k hr hdt
    rcases eq_or_ne k i with rfl | hne
    · simp only [Function.update_self] at hr
      cases hr
    · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hdt ⊢
      exact h T hera k (by simpa [Function.update_of_ne hne] using hr) hdt
  | becomeLeader i hrole hquorum =>
    intro T hera k hr hdt
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.dataTerm, Function.update_self] at hdt ⊢
      rw [Data.lastTermOf_prunePush]
    · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hdt ⊢
      exact h T hera k (by simpa [Function.update_of_ne hne] using hr) hdt
  | absorbDurable i hrole =>
    -- issue #7: `dataTerm` is UNCHANGED, so the hypothesis transfers (the
    -- crashRestart case re-derives it because the reboot re-keys `dataTerm`).
    intro T hera k hr hdt
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.dataTerm, Function.update_self] at hr hdt ⊢
      exact h T hera k hr hdt
    · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hdt ⊢
      exact h T hera k (by simpa [Function.update_of_ne hne] using hr) hdt
  | crashRestart i =>
    intro T hera k hr hdt
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.dataTerm, Function.update_self] at hr hdt ⊢
      exact of_decide_eq_true hr
    · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hdt ⊢
      exact h T hera k (by simpa [Function.update_of_ne hne] using hr) hdt
  | leaderAppend i v hrole =>
    intro T hera k hr hdt
    have hera' : Era w T := by
      intro u es hg
      exact hera u es (List.mem_append_left _ hg)
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.dataTerm, Function.update_self] at hr hdt ⊢
      exact h T hera' k (by simpa [Function.update_self] using hr) hdt
    · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hdt ⊢
      exact h T hera' k (by simpa [Function.update_of_ne hne] using hr) hdt
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate =>
    intro T hera k hr hdt
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.dataTerm, Function.update_self,
        Uc2.Data.Node.recvReplicate] at hr hdt ⊢
      have hpre := h T hera k (by simpa [Function.update_self] using hr) hdt
      have hle : Data.lastTermOf (w.nodes k).dn.termMap
          ≤ Data.lastTermOf
            (Data.observeTerm (w.nodes k).dn.termMap t pos) := by
        by_cases hgrow : Data.lastTermOf (w.nodes k).dn.termMap < t
        · rw [show Data.observeTerm (w.nodes k).dn.termMap t pos
              = (w.nodes k).dn.termMap ++ [(t, pos)] by
            simp [Data.observeTerm, hgrow]]
          rw [Data.lastTermOf_getLast (Data.getLast?_append_singleton _ _)]
          omega
        · rw [Data.observeTerm_of_le (Nat.not_lt.mp hgrow) pos]
      have hpre' : (w.nodes k).dn.dataTerm
          ≤ Data.lastTermOf (w.nodes k).dn.termMap := hpre
      omega
    · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hdt ⊢
      exact h T hera k (by simpa [Function.update_of_ne hne] using hr) hdt
  | serveTail i p t v hrole hhist hp =>
    intro T hera k hr hdt
    have hera' : Era w T := by
      intro u es hg
      exact hera u es (List.mem_append_left _ hg)
    exact h T hera' k hr hdt
  | shipTermMap i hrole =>
    intro T hera k hr hdt
    have hera' : Era w T := by
      intro u es hg
      exact hera u es (List.mem_append_left _ hg)
    exact h T hera' k hr hdt
  | deliverTermMap j t entries hmsg hterm =>
    intro T hera k hr hdt
    have htT : t ≤ T := hera t entries hmsg
    rcases eq_or_ne k j with rfl | hne
    · exfalso
      -- in-era, a node with regime above T cannot be delivered to at all
      simp only [Node.dataTerm, Function.update_self] at hr hdt
      rw [Data.applyGossip_dataTerm] at hdt
      have hct : (w.nodes k).dn.pn.currentTerm ≤ t := hterm
      by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
      · rw [if_pos hadopt] at hdt
        omega
      · rw [if_neg hadopt] at hdt
        have hdle : (w.nodes k).dn.dataTerm
            ≤ (w.nodes k).dn.pn.currentTerm :=
          (Data.reachable_stamp (reachable_project hw)).data_le k
        omega
    · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hdt ⊢
      exact h T hera k (by simpa [Function.update_of_ne hne] using hr) hdt
  | sendReport j hrole hgate => exact h
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    intro T hera k hr hdt
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.dataTerm, Function.update_self] at hr hdt ⊢
      exact h T hera k (by simpa [Function.update_self] using hr) hdt
    · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hdt ⊢
      exact h T hera k (by simpa [Function.update_of_ne hne] using hr) hdt
  | leaderAdvanceCommit i kk hrole hbase hadv =>
    intro T hera k hr hdt
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.dataTerm, Function.update_self] at hr hdt ⊢
      exact h T hera k (by simpa [Function.update_self] using hr) hdt
    · simp only [Node.dataTerm, Function.update_of_ne hne] at hr hdt ⊢
      exact h T hera k (by simpa [Function.update_of_ne hne] using hr) hdt

/-- **In-era open-gate frontier pinning** in every reachable world. -/
theorem reachable_open_frontier_era {n : Nat} {w : World n}
    (hw : Reachable w) : OpenFrontierEra w := by
  induction hw with
  | refl => intro T hera j hr hdt; simp [World.init, Node.dataTerm] at hdt
  | tail hprev hstep ih => exact ofe_step hprev ih hstep

#print axioms reachable_open_frontier_era

/-! ## B1 — the era-conditioned reporter facts

While NO gossip above `T` is on the wire (`Era` — gossip delivery being the
only truncation vehicle, review M-1), a `T`-reporter keeps its reported
durable, and it sits either strictly past `T` (it won a higher term) or in
a `T`-regime state whose map is take-disciplined against the `T`-stream.
Crucially the clauses carry NO `dataTerm`/gate conditions: vote-path
adoptions move the handle without touching the data plane, and in-`Era`
nothing else can touch it either, so the facts survive arbitrary
candidacies and crashes — exactly what the grant event needs. -/

/-- The in-regime half: `T`-frontier-bounded, take-disciplined against
`T`-gossips and the live `T`-leader, frame-pinned attribution, and the
live-leader durable floor. -/
structure InRegime {n : Nat} (w : World n) (y : Fin n) (T : Nat) : Prop where
  frontier_le : Data.lastTermOf (w.nodes y).dn.termMap ≤ T
  gtake : ∀ es, Frame.gossip T es ∈ w.dsent →
      (w.nodes y).dn.termMap = es.take (w.nodes y).dn.termMap.length ∧
      ∀ f ∈ es[(w.nodes y).dn.termMap.length]?, (w.nodes y).pn.durable ≤ f.2
  fpin : ∀ b v0, Frame.replicate b T T v0 ∈ w.dsent →
      b < (w.nodes y).pn.durable →
      TermMap.termAt (w.nodes y).dn.termMap b = T
  ltake : ∀ ℓ : Fin n, (w.nodes ℓ).pn.role = .leader →
      (w.nodes ℓ).pn.currentTerm = T →
      (w.nodes y).dn.termMap
        = (w.nodes ℓ).dn.termMap.take (w.nodes y).dn.termMap.length ∧
      ∀ f ∈ (w.nodes ℓ).dn.termMap[(w.nodes y).dn.termMap.length]?,
        (w.nodes y).pn.durable ≤ f.2
  lfloor : ∀ ℓ : Fin n, (w.nodes ℓ).pn.role = .leader →
      (w.nodes ℓ).pn.currentTerm = T →
      (w.nodes y).pn.durable ≤ (w.nodes ℓ).pn.durable

/-- B1: every reporter, in-`Era`, keeps its floor and its regime shape. -/
def ReportEraFloor {n : Nat} (w : World n) : Prop :=
  ∀ (y : Fin n) (T d : Nat), CMsg.report y T d ∈ w.csent → 1 ≤ T →
    Era w T →
    d ≤ (w.nodes y).pn.durable ∧
    (T < Data.lastTermOf (w.nodes y).dn.termMap ∨ InRegime w y T)

private theorem ref_transport {n : Nat} {w w' : World n}
    (h : ReportEraFloor w)
    (hmap : ∀ k, (w'.nodes k).dn.termMap = (w.nodes k).dn.termMap)
    (hdur : ∀ k, (w'.nodes k).pn.durable = (w.nodes k).pn.durable)
    (hldr : ∀ k, (w'.nodes k).pn.role = .leader →
      (w.nodes k).pn.role = .leader ∧
      (w'.nodes k).pn.currentTerm = (w.nodes k).pn.currentTerm)
    (hds : w'.dsent = w.dsent)
    (hcs : ∀ (u : Fin n) (T d : Nat), CMsg.report u T d ∈ w'.csent →
      CMsg.report u T d ∈ w.csent) :
    ReportEraFloor w' := by
  intro y T d hm h1T hera
  have hera' : Era w T := by
    intro u es hg
    exact hera u es (hds ▸ hg)
  obtain ⟨hfl, hst⟩ := h y T d (hcs y T d hm) h1T hera'
  refine ⟨by rw [hdur]; exact hfl, ?_⟩
  rcases hst with h1 | h2
  · left
    rw [hmap]
    exact h1
  · right
    refine ⟨by rw [hmap]; exact h2.frontier_le, ?_, ?_, ?_, ?_⟩
    · intro es hg
      rw [hds] at hg
      rw [hmap, hdur]
      exact h2.gtake es hg
    · intro b v0 hf hb
      rw [hds] at hf
      rw [hdur] at hb
      rw [hmap]
      exact h2.fpin b v0 hf hb
    · intro ℓ hrl hct
      obtain ⟨hpre, hcteq⟩ := hldr ℓ hrl
      rw [hcteq] at hct
      rw [hmap y, hmap ℓ, hdur]
      exact h2.ltake ℓ hpre hct
    · intro ℓ hrl hct
      obtain ⟨hpre, hcteq⟩ := hldr ℓ hrl
      rw [hcteq] at hct
      rw [hdur y, hdur ℓ]
      exact h2.lfloor ℓ hpre hct

private theorem ref_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (hw' : Reachable w') (h : ReportEraFloor w) (hs : Step w w') :
    ReportEraFloor w' := by
  have hstep := hs
  cases hs with
  | startElection i hrole =>
    refine ref_transport h (fun k => ?_) (fun k => ?_) (fun k hrl => ?_)
      rfl (fun u T d hm => hm)
    · rcases eq_or_ne k i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp [Node.pn, Function.update_self]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp only [Node.pn, Function.update_self] at hrl
        exact absurd hrl (by decide)
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    refine ref_transport h (fun k => ?_) (fun k => ?_) (fun k hrl => ?_)
      rfl (fun u T d hm => hm)
    · rcases eq_or_ne k j with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k j with rfl | hne
      · simp only [Node.pn, Function.update_self]
        exact Data.recv_durable _ _ _ _ _
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k j with rfl | hne
      · simp only [Node.pn, Function.update_self] at hrl ⊢
        by_cases hadopt : (w.nodes k).dn.pn.currentTerm < nt
        · rw [Data.recv_adopt_role _ _ _ _ _ hadopt] at hrl
          exact absurd hrl (by decide)
        · rw [(Data.recv_frame _ _ _ _ _ hadopt).1] at hrl
          exact ⟨hrl, (Data.recv_frame _ _ _ _ _ hadopt).2⟩
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    exact ref_transport h (fun k => rfl) (fun k => rfl)
      (fun k hrl => ⟨hrl, rfl⟩) rfl (fun u T d hm => hm)
  | deliverVote i v t hmsg hrole hterm =>
    refine ref_transport h (fun k => ?_) (fun k => ?_) (fun k hrl => ?_)
      rfl (fun u T d hm => hm)
    · rcases eq_or_ne k i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp [Node.pn, Function.update_self]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · refine ⟨by simpa [Node.pn, Function.update_self] using hrl, ?_⟩
        simp [Node.pn, Function.update_self]
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    refine ref_transport h (fun k => ?_) (fun k => ?_) (fun k hrl => ?_)
      rfl (fun u T d hm => hm)
    · rcases eq_or_ne k i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp [Node.pn, Function.update_self, PNode.adoptTerm]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp only [Node.pn, Function.update_self, PNode.adoptTerm] at hrl
        exact absurd hrl (by decide)
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
  | becomeLeader i hrole hquorum =>
    intro y T d hm h1T hera
    have hInv : Uc2.Inv w.project.project :=
      Uc2.reachable_inv (Data.reachable_project (reachable_project hw))
    have hera' : Era w T := hera
    obtain ⟨hfl, hst⟩ := h y T d hm h1T hera'
    rcases eq_or_ne y i with rfl | hne
    · -- the reporter itself takes a HIGHER crown: flips to the past-T arm
      have hTlt : T < (w.nodes y).pn.currentTerm := by
        have h1 : T ≤ (w.nodes y).dn.dataTerm :=
          (reachable_provInv hw).report_dt y T d hm
        have h2 : (w.nodes y).dn.dataTerm ≤ (w.nodes y).dn.pn.currentTerm :=
          (Data.reachable_stamp (reachable_project hw)).data_le y
        rcases Nat.lt_or_ge T ((w.nodes y).dn.pn.currentTerm) with hlt | hge
        · exact hlt
        · exfalso
          have hTeq : T = (w.nodes y).pn.currentTerm := by
            show T = (w.nodes y).dn.pn.currentTerm
            omega
          exact reachable_no_self_report hw y (by rw [hrole]; decide) d
            (by rw [← hTeq]; exact hm)
      refine ⟨by simpa [Node.pn, Function.update_self] using hfl, .inl ?_⟩
      simp only [Node.pn, Function.update_self]
      rw [Data.lastTermOf_prunePush]
      exact hTlt
    · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hfl, ?_⟩
      rcases hst with h1 | h2
      · left
        simpa [Node.pn, Function.update_of_ne hne] using h1
      · right
        have hblockT : (w.nodes i).pn.currentTerm = T → False := by
          intro hTeq
          obtain ⟨ℓ', hc⟩ := (reachable_provInv hw).report_cert y T d hm h1T
          rw [← hTeq] at hc
          exact Data.cert_blocks_candidate hInv hrole rfl hquorum hc
        refine ⟨?_, ?_, ?_, ?_, ?_⟩
        · simpa [Node.pn, Function.update_of_ne hne] using h2.frontier_le
        · intro es hg
          simp only [Node.pn, Function.update_of_ne hne]
          exact h2.gtake es hg
        · intro b v0 hf hb
          simp only [Node.pn, Function.update_of_ne hne] at hb ⊢
          exact h2.fpin b v0 hf hb
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ i with rfl | hneℓ
          · exfalso
            simp only [Node.pn, Function.update_self] at hct
            exact hblockT hct
          · simp only [Node.pn, Function.update_of_ne hne,
              Function.update_of_ne hneℓ] at hrl hct ⊢
            exact h2.ltake ℓ hrl hct
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ i with rfl | hneℓ
          · exfalso
            simp only [Node.pn, Function.update_self] at hct
            exact hblockT hct
          · simp only [Node.pn, Function.update_of_ne hne,
              Function.update_of_ne hneℓ] at hrl hct ⊢
            exact h2.lfloor ℓ hrl hct
  | absorbDurable i hrole =>
    refine ref_transport h (fun k => ?_) (fun k => ?_) (fun k hrl => ?_)
      rfl (fun u T d hm => hm)
    · rcases eq_or_ne k i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp [Node.pn, Function.update_self]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · -- issue #7: role UNCHANGED, so this is absurd by the step's own
        -- non-leader guard rather than by crashRestart's drop to follower.
        simp only [Node.pn, Function.update_self] at hrl
        exact absurd hrl hrole
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
  | crashRestart i =>
    refine ref_transport h (fun k => ?_) (fun k => ?_) (fun k hrl => ?_)
      rfl (fun u T d hm => hm)
    · rcases eq_or_ne k i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp [Node.pn, Function.update_self]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp only [Node.pn, Function.update_self] at hrl
        exact absurd hrl (by decide)
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
  | leaderAppend i v hrole =>
    intro y T d hm h1T hera
    have hera' : Era w T := by
      intro u es hg
      exact hera u es (List.mem_append_left _ hg)
    obtain ⟨hfl, hst⟩ := h y T d hm h1T hera'
    have hmp : Data.lastTermOf (w.nodes i).dn.termMap
        = (w.nodes i).pn.currentTerm :=
      (Data.reachable_dinv (reachable_project hw)).map_pinned i hrole
    rcases eq_or_ne y i with rfl | hne
    · -- the leader's own report: necessarily below its term; past-T arm
      have hTlt : T < (w.nodes y).pn.currentTerm := by
        have h1 : T ≤ (w.nodes y).dn.dataTerm :=
          (reachable_provInv hw).report_dt y T d hm
        have h2 : (w.nodes y).dn.dataTerm ≤ (w.nodes y).dn.pn.currentTerm :=
          (Data.reachable_stamp (reachable_project hw)).data_le y
        rcases Nat.lt_or_ge T ((w.nodes y).dn.pn.currentTerm) with hlt | hge
        · exact hlt
        · exfalso
          have hTeq : T = (w.nodes y).pn.currentTerm := by
            show T = (w.nodes y).dn.pn.currentTerm
            omega
          exact reachable_no_self_report hw y (by rw [hrole]; decide) d
            (by rw [← hTeq]; exact hm)
      refine ⟨?_, .inl ?_⟩
      · simp only [Node.pn, Function.update_self]
        have hfl' : d ≤ (w.nodes y).dn.pn.durable := hfl
        omega
      · simp only [Node.pn, Function.update_self]
        have hmp' : Data.lastTermOf (w.nodes y).dn.termMap
            = (w.nodes y).dn.pn.currentTerm := hmp
        have hTlt' : T < (w.nodes y).dn.pn.currentTerm := hTlt
        omega
    · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hfl, ?_⟩
      rcases hst with h1 | h2
      · left
        simpa [Node.pn, Function.update_of_ne hne] using h1
      · right
        refine ⟨?_, ?_, ?_, ?_, ?_⟩
        · simpa [Node.pn, Function.update_of_ne hne] using h2.frontier_le
        · intro es hg
          rcases List.mem_append.mp hg with hg | hg
          · simp only [Node.pn, Function.update_of_ne hne]
            exact h2.gtake es hg
          · simp at hg
        · intro b v0 hf hb
          simp only [Node.pn, Function.update_of_ne hne] at hb ⊢
          rcases List.mem_append.mp hf with hf | hf
          · exact h2.fpin b v0 hf hb
          · simp only [List.mem_singleton, Frame.replicate.injEq] at hf
            obtain ⟨hbd, hcti, hts, hvs⟩ := hf
            exfalso
            have hlf := h2.lfloor i hrole hcti.symm
            have hlf' : (w.nodes y).pn.durable ≤ (w.nodes i).dn.pn.durable :=
              hlf
            have hb' : b < (w.nodes y).pn.durable := hb
            omega
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ i with rfl | hneℓ
          · simp only [Node.pn, Function.update_self,
              Function.update_of_ne hne] at hrl hct ⊢
            exact h2.ltake ℓ hrole hct
          · simp only [Node.pn, Function.update_of_ne hne,
              Function.update_of_ne hneℓ] at hrl hct ⊢
            exact h2.ltake ℓ hrl hct
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ i with rfl | hneℓ
          · simp only [Node.pn, Function.update_self,
              Function.update_of_ne hne] at hrl hct ⊢
            have hlf := h2.lfloor ℓ hrole hct
            have hlf' : (w.nodes y).pn.durable ≤ (w.nodes ℓ).dn.pn.durable :=
              hlf
            show (w.nodes y).pn.durable ≤ (w.nodes ℓ).dn.pn.durable + 1
            omega
          · simp only [Node.pn, Function.update_of_ne hne,
              Function.update_of_ne hneℓ] at hrl hct ⊢
            exact h2.lfloor ℓ hrl hct
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate =>
    intro y T d hm h1T hera
    have hera' : Era w T := hera
    obtain ⟨hfl, hst⟩ := h y T d hm h1T hera'
    have hstamp : t ≤ hdr :=
      (Data.reachable_stamp (reachable_project hw)).frame_le pos hdr t v hmsg
    have hposd : pos = (w.nodes j).dn.pn.durable := hpos
    rcases eq_or_ne y j with rfl | hne
    · -- the reporter accepts a byte
      rcases hst with h1 | h2
      · -- already past T: growth only raises the frontier
        refine ⟨?_, .inl ?_⟩
        · simp only [Node.pn, Function.update_self,
            Uc2.Data.Node.recvReplicate]
          have hfl' : d ≤ (w.nodes y).dn.pn.durable := hfl
          omega
        · simp only [Node.pn, Function.update_self,
            Uc2.Data.Node.recvReplicate]
          by_cases hgrow : Data.lastTermOf (w.nodes y).dn.termMap < t
          · rw [show Data.observeTerm (w.nodes y).dn.termMap t pos
                = (w.nodes y).dn.termMap ++ [(t, pos)] by
              simp [Data.observeTerm, hgrow]]
            rw [Data.lastTermOf_getLast (Data.getLast?_append_singleton _ _)]
            omega
          · rw [Data.observeTerm_of_le (Nat.not_lt.mp hgrow) pos]
            exact h1
      · -- in-regime: the accept is necessarily a regime-T accept
        have hdtT : (w.nodes y).dn.dataTerm = T := by
          have hTle : T ≤ (w.nodes y).dn.dataTerm :=
            (reachable_provInv hw).report_dt y T d hm
          rcases Nat.lt_or_ge T ((w.nodes y).dn.dataTerm) with hlt | hge
          · exfalso
            have hofe : (w.nodes y).dn.dataTerm
                ≤ Data.lastTermOf (w.nodes y).dn.termMap :=
              reachable_open_frontier_era hw T hera y hgate hlt
            have hfr : Data.lastTermOf (w.nodes y).dn.termMap ≤ T :=
              h2.frontier_le
            omega
          · omega
        -- the post-state is gate-open at the same regime: pull everything
        -- from the POST-state invariants
        refine ⟨?_, .inr ?_⟩
        · simp only [Node.pn, Function.update_self,
            Uc2.Data.Node.recvReplicate]
          have hfl' : d ≤ (w.nodes y).dn.pn.durable := hfl
          omega
        · refine ⟨?_, ?_, ?_, ?_, ?_⟩
          · have hml : Data.lastTermOf
                (Data.observeTerm (w.nodes y).dn.termMap t pos)
                ≤ (w.nodes y).dn.dataTerm := by
              have h0 := Data.reachable_map_le_dataTerm
                (reachable_project hw') y
              simpa [World.project, Function.update_self,
                Uc2.Data.Node.recvReplicate] using h0
            simp only [Function.update_self, Uc2.Data.Node.recvReplicate]
            omega
          · intro es hg
            refine (reachable_tkInv hw').gate_take y ?_ es ?_
            · simpa [Function.update_self] using hgate
            · simp only [Node.dataTerm, Function.update_self,
                Uc2.Data.Node.recvReplicate]
              rw [hdtT]
              exact hg
          · intro b v0 hf hb
            have hb2 : b < pos + 1 := by
              simpa [Node.pn, Function.update_self,
                Uc2.Data.Node.recvReplicate] using hb
            refine (reachable_provInv hw').gate_frames_eq y ?_ b T v0 ?_ ?_
            · simpa [Function.update_self] using hgate
            · simp only [Node.dataTerm, Function.update_self,
                Uc2.Data.Node.recvReplicate]
              rw [hdtT]
              exact hf
            · simp only [Node.pn, Function.update_self,
                Uc2.Data.Node.recvReplicate]
              omega
          · intro ℓ hrl hct
            refine (reachable_tkInv hw').open_leader y ℓ ?_ hrl ?_
            · simpa [Function.update_self] using hgate
            · exact hct.trans (by
                simp only [Node.dataTerm, Function.update_self,
                  Uc2.Data.Node.recvReplicate]
                exact hdtT.symm)
          · intro ℓ hrl hct
            refine (reachable_provInv hw').gate_durable y ℓ ?_ hrl ?_
            · simpa [Function.update_self] using hgate
            · exact hct.trans (by
                simp only [Node.dataTerm, Function.update_self,
                  Uc2.Data.Node.recvReplicate]
                exact hdtT.symm)
    · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hfl, ?_⟩
      rcases hst with h1 | h2
      · left
        simpa [Node.pn, Function.update_of_ne hne] using h1
      · right
        have hnotldr : (w.nodes j).dn.pn.role = Role.leader → False := by
          intro hrl
          have hdtj : (w.nodes j).dn.dataTerm
              = (w.nodes j).dn.pn.currentTerm :=
            Data.reachable_leader_dataTerm (reachable_project hw) j hrl
          have hf' : Frame.replicate pos ((w.nodes j).pn.currentTerm) t v
              ∈ w.dsent := by
            have hh : hdr = (w.nodes j).dn.pn.currentTerm := by
              have hh2 : hdr = (w.nodes j).dn.dataTerm := hhdr
              omega
            rwa [hh] at hmsg
          have hlt : pos < (w.nodes j).dn.pn.durable :=
            (reachable_provInv hw).leader_frontier j hrl pos t v hf'
          omega
        refine ⟨?_, ?_, ?_, ?_, ?_⟩
        · simpa [Node.pn, Function.update_of_ne hne] using h2.frontier_le
        · intro es hg
          simp only [Node.pn, Function.update_of_ne hne]
          exact h2.gtake es hg
        · intro b v0 hf hb
          simp only [Node.pn, Function.update_of_ne hne] at hb ⊢
          exact h2.fpin b v0 hf hb
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ j with rfl | hneℓ
          · exfalso
            simp only [Node.pn, Function.update_self,
              Uc2.Data.Node.recvReplicate] at hrl
            exact hnotldr hrl
          · simp only [Node.pn, Function.update_of_ne hne,
              Function.update_of_ne hneℓ] at hrl hct ⊢
            exact h2.ltake ℓ hrl hct
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ j with rfl | hneℓ
          · exfalso
            simp only [Node.pn, Function.update_self,
              Uc2.Data.Node.recvReplicate] at hrl
            exact hnotldr hrl
          · simp only [Node.pn, Function.update_of_ne hne,
              Function.update_of_ne hneℓ] at hrl hct ⊢
            exact h2.lfloor ℓ hrl hct
  | serveTail i p t v hrole hhist hp =>
    intro y T d hm h1T hera
    have hera' : Era w T := by
      intro u es hg
      exact hera u es (List.mem_append_left _ hg)
    obtain ⟨hfl, hst⟩ := h y T d hm h1T hera'
    refine ⟨hfl, ?_⟩
    rcases hst with h1 | h2
    · exact .inl h1
    · refine .inr ⟨h2.frontier_le, ?_, ?_, h2.ltake, h2.lfloor⟩
      · intro es hg
        rcases List.mem_append.mp hg with hg | hg
        · exact h2.gtake es hg
        · simp at hg
      · intro b v0 hf hb
        rcases List.mem_append.mp hf with hf | hf
        · exact h2.fpin b v0 hf hb
        · simp only [List.mem_singleton, Frame.replicate.injEq] at hf
          obtain ⟨hpb, hcti, hts, hvs⟩ := hf
          -- the fresh serveTail frame: pin through the live leader's map
          obtain ⟨htake, hbound⟩ := h2.ltake i hrole hcti.symm
          have hasci : TermMap.Ascending (w.nodes i).dn.termMap :=
            ((Data.reachable_mapsWF (reachable_project hw)) i).1
          have hattr : TermMap.termAt (w.nodes i).dn.termMap p = t :=
            (reachable_provInv hw).fca i p t v hhist
          have hb' : b < (w.nodes y).pn.durable := hb
          have hagree : TermMap.termAt (w.nodes y).dn.termMap b
              = TermMap.termAt (w.nodes i).dn.termMap b := by
            rw [htake]
            refine (hasci.termAt_take ?_).symm
            intro e he
            have hbe := hbound e he
            have hbe' : (w.nodes y).pn.durable ≤ e.2 := hbe
            omega
          show TermMap.termAt (w.nodes y).dn.termMap b = T
          rw [hagree, hpb, hattr]
          exact hts.symm
  | shipTermMap i hrole =>
    intro y T d hm h1T hera
    have hera' : Era w T := by
      intro u es hg
      exact hera u es (List.mem_append_left _ hg)
    obtain ⟨hfl, hst⟩ := h y T d hm h1T hera'
    refine ⟨hfl, ?_⟩
    rcases hst with h1 | h2
    · exact .inl h1
    · refine .inr ⟨h2.frontier_le, ?_, ?_, h2.ltake, h2.lfloor⟩
      · intro es hg
        rcases List.mem_append.mp hg with hg | hg
        · exact h2.gtake es hg
        · simp only [List.mem_singleton, Frame.gossip.injEq] at hg
          obtain ⟨hct, rfl⟩ := hg
          exact h2.ltake i hrole hct.symm
      · intro b v0 hf hb
        rcases List.mem_append.mp hf with hf | hf
        · exact h2.fpin b v0 hf hb
        · simp at hf
  | deliverTermMap j t entries hmsg hterm =>
    intro y T d hm h1T hera
    have hera' : Era w T := hera
    obtain ⟨hfl, hst⟩ := h y T d hm h1T hera'
    have htT : t ≤ T := hera t entries hmsg
    rcases eq_or_ne y j with rfl | hne
    · -- the receiver is the reporter: in-Era the delivery is same-regime
      -- and provably clean
      have hTle : T ≤ (w.nodes y).dn.dataTerm :=
        (reachable_provInv hw).report_dt y T d hm
      have hdle : (w.nodes y).dn.dataTerm ≤ (w.nodes y).dn.pn.currentTerm :=
        (Data.reachable_stamp (reachable_project hw)).data_le y
      have hct' : (w.nodes y).dn.pn.currentTerm ≤ t := hterm
      have hteq : t = T := by omega
      have hcteq : (w.nodes y).dn.pn.currentTerm = T := by omega
      rcases hst with h1 | h2
      · exfalso
        have hml : Data.lastTermOf (w.nodes y).dn.termMap
            ≤ (w.nodes y).dn.dataTerm :=
          Data.reachable_map_le_dataTerm (reachable_project hw) y
        omega
      · obtain ⟨htake, hbound⟩ := h2.gtake entries (by rw [← hteq]; exact hmsg)
        have hrec : Uc2.reconcile (w.nodes y).dn.termMap
            (w.nodes y).dn.pn.durable entries
            = .ok ⟨(w.nodes y).dn.pn.durable, (w.nodes y).dn.termMap⟩ :=
          take_reconcile_clean htake hbound
        obtain ⟨hmapE, hdurE, -, -, -, -⟩ :=
          Data.applyGossip_ok (w.nodes y).dn t hrec
        refine ⟨?_, .inr ⟨?_, ?_, ?_, ?_, ?_⟩⟩
        · simp only [Node.pn, Function.update_self]
          rw [hdurE]
          exact hfl
        · simp only [Function.update_self]
          rw [hmapE]
          exact h2.frontier_le
        · intro es hg
          simp only [Node.pn, Function.update_self]
          rw [hmapE, hdurE]
          exact h2.gtake es hg
        · intro b v0 hf hb
          simp only [Node.pn, Function.update_self] at hb ⊢
          rw [hmapE]
          rw [hdurE] at hb
          exact h2.fpin b v0 hf hb
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ y with rfl | hneℓ
          · exfalso
            simp only [Node.pn, Function.update_self] at hrl
            rw [(Data.applyGossip_ok (w.nodes ℓ).dn t hrec).2.2.2.1] at hrl
            rw [if_neg (by omega)] at hrl
            exact reachable_no_self_report hw ℓ
              (by rw [show (w.nodes ℓ).pn.role = Role.leader from hrl]
                  decide) d
              (by rw [show (w.nodes ℓ).pn.currentTerm = T from hcteq]
                  exact hm)
          · simp only [Node.pn, Function.update_self,
              Function.update_of_ne hneℓ] at hrl hct ⊢
            rw [hmapE, hdurE]
            exact h2.ltake ℓ hrl hct
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ y with rfl | hneℓ
          · exfalso
            simp only [Node.pn, Function.update_self] at hrl
            rw [(Data.applyGossip_ok (w.nodes ℓ).dn t hrec).2.2.2.1] at hrl
            rw [if_neg (by omega)] at hrl
            exact reachable_no_self_report hw ℓ
              (by rw [show (w.nodes ℓ).pn.role = Role.leader from hrl]
                  decide) d
              (by rw [show (w.nodes ℓ).pn.currentTerm = T from hcteq]
                  exact hm)
          · simp only [Node.pn, Function.update_self,
              Function.update_of_ne hneℓ] at hrl hct ⊢
            rw [hdurE]
            exact h2.lfloor ℓ hrl hct
    · -- another receiver; the reporter's state is untouched, and any live
      -- T-leader receiving in-Era reconciles its own frozen map (identity)
      refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hfl, ?_⟩
      rcases hst with h1 | h2
      · left
        simpa [Node.pn, Function.update_of_ne hne] using h1
      · right
        have hlj : ∀ (hrl : (w.nodes j).dn.pn.role = Role.leader)
            (hct : (w.nodes j).dn.pn.currentTerm = T),
            ((w.nodes j).dn.applyGossip t entries).termMap
              = (w.nodes j).dn.termMap ∧
            ((w.nodes j).dn.applyGossip t entries).pn.durable
              = (w.nodes j).dn.pn.durable := by
          intro hrl hct
          have hterm2 : (w.nodes j).dn.pn.currentTerm ≤ t := hterm
          have hcteq3 : (w.nodes j).dn.pn.currentTerm = t := by omega
          have hpin : entries = (w.nodes j).dn.termMap :=
            (Data.reachable_dinv (reachable_project hw)).gossip_pinned j hrl
              entries
              (show Frame.gossip ((w.nodes j).dn.pn.currentTerm) entries
                  ∈ w.dsent by rw [hcteq3]; exact hmsg)
          have hrec : Uc2.reconcile (w.nodes j).dn.termMap
              (w.nodes j).dn.pn.durable entries
              = .ok ⟨(w.nodes j).dn.pn.durable, (w.nodes j).dn.termMap⟩ := by
            rw [hpin]
            exact Data.reconcile_self _ _
          obtain ⟨hmapE, hdurE, -, -, -, -⟩ :=
            Data.applyGossip_ok (w.nodes j).dn t hrec
          exact ⟨hmapE, hdurE⟩
        refine ⟨?_, ?_, ?_, ?_, ?_⟩
        · simpa [Node.pn, Function.update_of_ne hne] using h2.frontier_le
        · intro es hg
          simp only [Node.pn, Function.update_of_ne hne]
          exact h2.gtake es hg
        · intro b v0 hf hb
          simp only [Node.pn, Function.update_of_ne hne] at hb ⊢
          exact h2.fpin b v0 hf hb
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ j with rfl | hneℓ
          · simp only [Node.pn, Function.update_self,
              Function.update_of_ne hne] at hrl hct ⊢
            have hnad : ¬ (w.nodes ℓ).dn.pn.currentTerm < t := by
              by_contra hc
              rw [((Data.applyGossip_adopt (w.nodes ℓ).dn entries hc)).1]
                at hrl
              exact absurd hrl (by decide)
            have hrl' : (w.nodes ℓ).dn.pn.role = Role.leader := by
              rw [(Data.applyGossip_no_adopt (w.nodes ℓ).dn entries hnad).1]
                at hrl
              exact hrl
            have hct2 : (w.nodes ℓ).dn.pn.currentTerm = T := by
              rw [(Data.applyGossip_no_adopt
                (w.nodes ℓ).dn entries hnad).2.1] at hct
              exact hct
            obtain ⟨hmapE, hdurE⟩ := hlj hrl' hct2
            rw [hmapE]
            exact h2.ltake ℓ hrl' hct2
          · simp only [Node.pn, Function.update_of_ne hne,
              Function.update_of_ne hneℓ] at hrl hct ⊢
            exact h2.ltake ℓ hrl hct
        · intro ℓ hrl hct
          rcases eq_or_ne ℓ j with rfl | hneℓ
          · simp only [Node.pn, Function.update_self,
              Function.update_of_ne hne] at hrl hct ⊢
            have hnad : ¬ (w.nodes ℓ).dn.pn.currentTerm < t := by
              by_contra hc
              rw [((Data.applyGossip_adopt (w.nodes ℓ).dn entries hc)).1]
                at hrl
              exact absurd hrl (by decide)
            have hrl' : (w.nodes ℓ).dn.pn.role = Role.leader := by
              rw [(Data.applyGossip_no_adopt (w.nodes ℓ).dn entries hnad).1]
                at hrl
              exact hrl
            have hct2 : (w.nodes ℓ).dn.pn.currentTerm = T := by
              rw [(Data.applyGossip_no_adopt
                (w.nodes ℓ).dn entries hnad).2.1] at hct
              exact hct
            obtain ⟨hmapE, hdurE⟩ := hlj hrl' hct2
            rw [hdurE]
            exact h2.lfloor ℓ hrl' hct2
          · simp only [Node.pn, Function.update_of_ne hne,
              Function.update_of_ne hneℓ] at hrl hct ⊢
            exact h2.lfloor ℓ hrl hct
  | sendReport j hrole hgate =>
    intro y T d hm h1T hera
    rcases List.mem_append.mp hm with hold | hnew
    · obtain ⟨hfl, hst⟩ := h y T d hold h1T hera
      refine ⟨hfl, ?_⟩
      rcases hst with h1 | h2
      · exact .inl h1
      · exact .inr ⟨h2.frontier_le, h2.gtake, h2.fpin, h2.ltake, h2.lfloor⟩
    · simp only [List.mem_singleton, CMsg.report.injEq] at hnew
      obtain ⟨rfl, rfl, rfl⟩ := hnew
      have hdtj : (w.nodes y).dataTerm = (w.nodes y).pn.currentTerm :=
        (reachable_provInv hw).role_dt y (by rw [hrole]; decide)
      refine ⟨Nat.le_refl _, .inr ?_⟩
      have hml : Data.lastTermOf (w.nodes y).dn.termMap
          ≤ (w.nodes y).dn.dataTerm :=
        Data.reachable_map_le_dataTerm (reachable_project hw) y
      refine ⟨?_, ?_, ?_, ?_, ?_⟩
      · have hd : (w.nodes y).dn.dataTerm = (w.nodes y).dn.pn.currentTerm :=
          hdtj
        show Data.lastTermOf (w.nodes y).dn.termMap
          ≤ (w.nodes y).dn.pn.currentTerm
        omega
      · intro es hg
        refine (reachable_tkInv hw).gate_take y hgate es ?_
        rw [hdtj]
        exact hg
      · intro b v0 hf hb
        refine (reachable_provInv hw).gate_frames_eq y hgate b
          ((w.nodes y).pn.currentTerm) v0 ?_ hb
        rw [hdtj]
        exact hf
      · intro ℓ hrl hct
        refine (reachable_tkInv hw).open_leader y ℓ hgate hrl ?_
        rw [hdtj]
        exact hct
      · intro ℓ hrl hct
        refine (reachable_provInv hw).gate_durable y ℓ hgate hrl ?_
        rw [hdtj]
        exact hct
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    refine ref_transport h (fun k => ?_) (fun k => ?_) (fun k hrl => ?_)
      rfl (fun u T d' hm => hm)
    · rcases eq_or_ne k i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · simp [Node.pn, Function.update_self]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k i with rfl | hne
      · refine ⟨by simpa [Node.pn, Function.update_self] using hrl, ?_⟩
        simp [Node.pn, Function.update_self]
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]
  | leaderAdvanceCommit i k hrole hbase hadv =>
    refine ref_transport h (fun k' => ?_) (fun k' => ?_) (fun k' hrl => ?_)
      rfl (fun u T d hm => hm)
    · rcases eq_or_ne k' i with rfl | hne
      · simp [Function.update_self]
      · simp [Function.update_of_ne hne]
    · rcases eq_or_ne k' i with rfl | hne
      · simp [Node.pn, Function.update_self]
      · simp [Node.pn, Function.update_of_ne hne]
    · rcases eq_or_ne k' i with rfl | hne
      · refine ⟨by simpa [Node.pn, Function.update_self] using hrl, ?_⟩
        simp [Node.pn, Function.update_self]
      · refine ⟨by simpa [Node.pn, Function.update_of_ne hne] using hrl, ?_⟩
        simp [Node.pn, Function.update_of_ne hne]

/-- **B1** in every reachable world. -/
theorem reachable_report_era_floor {n : Nat} {w : World n}
    (hw : Reachable w) : ReportEraFloor w := by
  induction hw with
  | refl => intro y T d hm; simp [World.init] at hm
  | tail hprev hstep ih => exact ref_step hprev (hprev.tail hstep) ih hstep

#print axioms reachable_report_era_floor

/-! ## Reports never outrun a live leader of their term -/

/-- A `T`-report's durable is below every LIVE `T`-leader's frontier —
unconditioned on eras or regimes: it held at the send (`gate_durable`), a
live leader's durable never shrinks (own-term reconciles are the frozen-map
identity), and a NEW `T`-leader can never arise once a `T`-report exists
(`report_cert` + `cert_blocks_candidate`). -/
def ReportLeaderFloor {n : Nat} (w : World n) : Prop :=
  ∀ (y : Fin n) (T d : Nat), CMsg.report y T d ∈ w.csent → 1 ≤ T →
    ∀ ℓ : Fin n, (w.nodes ℓ).pn.role = .leader →
      (w.nodes ℓ).pn.currentTerm = T → d ≤ (w.nodes ℓ).pn.durable

private theorem rlf_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : ReportLeaderFloor w) (hs : Step w w') : ReportLeaderFloor w' := by
  have hInv : Uc2.Inv w.project.project :=
    Uc2.reachable_inv (Data.reachable_project (reachable_project hw))
  cases hs with
  | startElection i hrole =>
    intro y T d hm h1T ℓ hrl hct
    rcases eq_or_ne ℓ i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hrl
      exact absurd hrl (by decide)
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      exact h y T d hm h1T ℓ hrl hct
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    intro y T d hm h1T ℓ hrl hct
    rcases eq_or_ne ℓ j with rfl | hne
    · simp only [Node.pn, Function.update_self] at hrl hct ⊢
      by_cases hadopt : (w.nodes ℓ).dn.pn.currentTerm < nt
      · rw [Data.recv_adopt_role _ _ _ _ _ hadopt] at hrl
        exact absurd hrl (by decide)
      · rw [(Data.recv_frame _ _ _ _ _ hadopt).1] at hrl
        rw [(Data.recv_frame _ _ _ _ _ hadopt).2] at hct
        rw [Data.recv_durable]
        exact h y T d hm h1T ℓ hrl hct
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      exact h y T d hm h1T ℓ hrl hct
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    exact fun y T d hm h1T ℓ hrl hct => h y T d hm h1T ℓ hrl hct
  | deliverVote i v t hmsg hrole hterm =>
    intro y T d hm h1T ℓ hrl hct
    rcases eq_or_ne ℓ i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hrl hct ⊢
      exact h y T d hm h1T ℓ hrl hct
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      exact h y T d hm h1T ℓ hrl hct
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    intro y T d hm h1T ℓ hrl hct
    rcases eq_or_ne ℓ i with rfl | hne
    · simp only [Node.pn, Function.update_self, PNode.adoptTerm] at hrl
      exact absurd hrl (by decide)
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      exact h y T d hm h1T ℓ hrl hct
  | becomeLeader i hrole hquorum =>
    intro y T d hm h1T ℓ hrl hct
    rcases eq_or_ne ℓ i with rfl | hne
    · exfalso
      simp only [Node.pn, Function.update_self] at hct
      obtain ⟨ℓ', hc⟩ := (reachable_provInv hw).report_cert y T d hm h1T
      rw [← hct] at hc
      exact Data.cert_blocks_candidate hInv hrole rfl hquorum hc
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      exact h y T d hm h1T ℓ hrl hct
  | absorbDurable i hrole =>
    -- issue #7: absurd by the step's non-leader guard (role is UNCHANGED here).
    intro y T d hm h1T ℓ hrl hct
    rcases eq_or_ne ℓ i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hrl
      exact absurd hrl hrole
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      exact h y T d hm h1T ℓ hrl hct
  | crashRestart i =>
    intro y T d hm h1T ℓ hrl hct
    rcases eq_or_ne ℓ i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hrl
      exact absurd hrl (by decide)
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      exact h y T d hm h1T ℓ hrl hct
  | leaderAppend i v hrole =>
    intro y T d hm h1T ℓ hrl hct
    rcases eq_or_ne ℓ i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hrl hct ⊢
      have hd := h y T d hm h1T ℓ hrole hct
      have hd' : d ≤ (w.nodes ℓ).dn.pn.durable := hd
      omega
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      exact h y T d hm h1T ℓ hrl hct
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate =>
    intro y T d hm h1T ℓ hrl hct
    rcases eq_or_ne ℓ j with rfl | hne
    · exfalso
      simp only [Node.pn, Function.update_self,
        Uc2.Data.Node.recvReplicate] at hrl
      have hdtj : (w.nodes ℓ).dn.dataTerm = (w.nodes ℓ).dn.pn.currentTerm :=
        Data.reachable_leader_dataTerm (reachable_project hw) ℓ hrl
      have hf' : Frame.replicate pos ((w.nodes ℓ).pn.currentTerm) t v
          ∈ w.dsent := by
        have hh : hdr = (w.nodes ℓ).dn.pn.currentTerm := by
          have hh2 : hdr = (w.nodes ℓ).dn.dataTerm := hhdr
          omega
        rwa [hh] at hmsg
      have hlt : pos < (w.nodes ℓ).dn.pn.durable :=
        (reachable_provInv hw).leader_frontier ℓ hrl pos t v hf'
      have hposd : pos = (w.nodes ℓ).dn.pn.durable := hpos
      omega
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      exact h y T d hm h1T ℓ hrl hct
  | serveTail i p t v hrole hhist hp =>
    exact fun y T d hm h1T ℓ hrl hct => h y T d hm h1T ℓ hrl hct
  | shipTermMap i hrole =>
    exact fun y T d hm h1T ℓ hrl hct => h y T d hm h1T ℓ hrl hct
  | deliverTermMap j t entries hmsg hterm =>
    intro y T d hm h1T ℓ hrl hct
    rcases eq_or_ne ℓ j with rfl | hne
    · simp only [Node.pn, Function.update_self] at hrl hct ⊢
      have hnad : ¬ (w.nodes ℓ).dn.pn.currentTerm < t := by
        by_contra hc
        rw [(Data.applyGossip_adopt (w.nodes ℓ).dn entries hc).1] at hrl
        exact absurd hrl (by decide)
      have hrl' : (w.nodes ℓ).dn.pn.role = Role.leader := by
        rw [(Data.applyGossip_no_adopt (w.nodes ℓ).dn entries hnad).1] at hrl
        exact hrl
      have hct2 : (w.nodes ℓ).dn.pn.currentTerm = T := by
        rw [(Data.applyGossip_no_adopt (w.nodes ℓ).dn entries hnad).2.1]
          at hct
        exact hct
      have hteq : t = (w.nodes ℓ).dn.pn.currentTerm := by
        have hterm2 : (w.nodes ℓ).dn.pn.currentTerm ≤ t := hterm
        omega
      have hpin : entries = (w.nodes ℓ).dn.termMap :=
        (Data.reachable_dinv (reachable_project hw)).gossip_pinned ℓ hrl'
          entries
          (show Frame.gossip ((w.nodes ℓ).dn.pn.currentTerm) entries
              ∈ w.dsent by rw [← hteq]; exact hmsg)
      have hrec : Uc2.reconcile (w.nodes ℓ).dn.termMap
          (w.nodes ℓ).dn.pn.durable entries
          = .ok ⟨(w.nodes ℓ).dn.pn.durable, (w.nodes ℓ).dn.termMap⟩ := by
        rw [hpin]
        exact Data.reconcile_self _ _
      obtain ⟨-, hdurE, -, -, -, -⟩ :=
        Data.applyGossip_ok (w.nodes ℓ).dn t hrec
      rw [hdurE]
      exact h y T d hm h1T ℓ hrl' hct2
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      exact h y T d hm h1T ℓ hrl hct
  | sendReport j hrole hgate =>
    intro y T d hm h1T ℓ hrl hct
    rcases List.mem_append.mp hm with hold | hnew
    · exact h y T d hold h1T ℓ hrl hct
    · simp only [List.mem_singleton, CMsg.report.injEq] at hnew
      obtain ⟨rfl, rfl, rfl⟩ := hnew
      have hdtj : (w.nodes y).dataTerm = (w.nodes y).pn.currentTerm :=
        (reachable_provInv hw).role_dt y (by rw [hrole]; decide)
      refine (reachable_provInv hw).gate_durable y ℓ hgate hrl ?_
      rw [hdtj]
      exact hct
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    intro y T d' hm h1T ℓ hrl hct
    rcases eq_or_ne ℓ i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hrl hct ⊢
      exact h y T d' hm h1T ℓ hrl hct
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      exact h y T d' hm h1T ℓ hrl hct
  | leaderAdvanceCommit i k hrole hbase hadv =>
    intro y T d hm h1T ℓ hrl hct
    rcases eq_or_ne ℓ i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hrl hct ⊢
      exact h y T d hm h1T ℓ hrl hct
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      exact h y T d hm h1T ℓ hrl hct

/-- **Report/leader floor** in every reachable world. -/
theorem reachable_report_leader_floor {n : Nat} {w : World n}
    (hw : Reachable w) : ReportLeaderFloor w := by
  induction hw with
  | refl => intro y T d hm; simp [World.init] at hm
  | tail hprev hstep ih => exact rlf_step hprev ih hstep

#print axioms reachable_report_leader_floor

/-! ## Recorded votes trace to their grant messages -/

/-- A recorded current-term vote for ANOTHER node was emitted as a grant
message (fresh grants emit atomically; self-votes are the excluded
`startElection` record; adoption clears the record). -/
def VotedMsg {n : Nat} (w : World n) : Prop :=
  ∀ (j c : Fin n) (t : Nat), (w.nodes j).pn.votedFor = some (t, c) →
    (w.nodes j).pn.currentTerm = t → c ≠ j → Msg.vote j c t true ∈ w.sent

/-- Grant-flag analysis for `recvRequestVote`: a `true` reply is a fresh
`logOk`-checked grant or an idempotent re-grant of the recorded
current-term vote. -/
private theorem recv_grant_cases {n : Nat} (s : PNode n) (c : Fin n)
    (nt clt cd : Nat) (hle : s.currentTerm ≤ nt)
    (hflag : (s.recvRequestVote c nt clt cd).2 = true) :
    logOk s.lastTerm s.durable clt cd = true ∨
    (s.votedFor = some (nt, c) ∧ s.currentTerm = nt) := by
  by_cases hadopt : s.currentTerm < nt
  · left
    simp only [PNode.recvRequestVote, if_pos hadopt, PNode.adoptTerm,
      PNode.recvRequestVote.grantIfFresh] at hflag
    split_ifs at hflag with hlog
    · exact hlog
  · have hct : s.currentTerm = nt := by omega
    rcases hvf : s.votedFor with _ | ⟨vt, vid⟩
    · left
      simp only [PNode.recvRequestVote, if_neg hadopt, hvf,
        PNode.recvRequestVote.grantIfFresh] at hflag
      split_ifs at hflag with hlog
      · exact hlog
    · by_cases hvt : vt = s.currentTerm
      · by_cases hvid : vid = c
        · right
          refine ⟨?_, hct⟩
          rw [hvt, hvid, hct]
        · exfalso
          simp only [PNode.recvRequestVote, if_neg hadopt, hvf, if_pos hvt,
            if_neg hvid] at hflag
          cases hflag
      · left
        simp only [PNode.recvRequestVote, if_neg hadopt, hvf, if_neg hvt,
          PNode.recvRequestVote.grantIfFresh] at hflag
        split_ifs at hflag with hlog
        · exact hlog

private theorem vm_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : VotedMsg w) (hs : Step w w') : VotedMsg w' := by
  cases hs with
  | startElection i hrole =>
    intro j c t hvf hct hcj
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.pn, Function.update_self, Option.some.injEq,
        Prod.mk.injEq] at hvf hct
      obtain ⟨-, rfl⟩ := hvf
      exact absurd rfl hcj
    · simp only [Node.pn, Function.update_of_ne hne] at hvf hct
      exact List.mem_append_left _ (h j c t hvf hct hcj)
  | deliverRequestVote j' c' nt clt cd hmsg hterm =>
    intro j c t hvf hct hcj
    rcases eq_or_ne j j' with rfl | hne
    · simp only [Node.pn, Function.update_self] at hvf hct
      -- either the recorded vote predates the step, or it is THIS grant
      by_cases hadopt : (w.nodes j).dn.pn.currentTerm < nt
      · -- adopted: votedFor is none-or-the-fresh-grant at nt
        have hctnt : ((w.nodes j).dn.pn.recvRequestVote c' nt clt cd).1.currentTerm
            = nt := Data.recv_term _ _ _ _ _ (by omega)
        have htnt : t = nt := by omega
        subst htnt
        simp only [PNode.recvRequestVote, if_pos hadopt, PNode.adoptTerm,
          PNode.recvRequestVote.grantIfFresh] at hvf
        split_ifs at hvf with hlog
        · simp only [Option.some.injEq, Prod.mk.injEq] at hvf
          obtain ⟨-, rfl⟩ := hvf
          have hflag : ((w.nodes j).pn.recvRequestVote c' t clt cd).2
              = true := by
            have hadopt2 : (w.nodes j).pn.currentTerm < t := hadopt
            have hlog2 : logOk (w.nodes j).pn.lastTerm
                (w.nodes j).pn.durable clt cd = true := hlog
            simp only [PNode.recvRequestVote, if_pos hadopt2, PNode.adoptTerm,
              PNode.recvRequestVote.grantIfFresh]
            rw [if_pos hlog2]
          refine List.mem_append_right _ ?_
          rw [hflag]
          exact List.mem_singleton.mpr rfl
      · have hctnt : (w.nodes j).dn.pn.currentTerm = nt := by
          have h2 : (w.nodes j).dn.pn.currentTerm ≤ nt := hterm
          omega
        rcases hvfpre : (w.nodes j).dn.pn.votedFor with _ | ⟨vt, vid⟩
        · -- fresh grant from a clean record
          simp only [PNode.recvRequestVote, if_neg hadopt, hvfpre,
            PNode.recvRequestVote.grantIfFresh] at hvf hct
          split_ifs at hvf with hlog
          · simp only [Option.some.injEq, Prod.mk.injEq] at hvf
            obtain ⟨hteq, rfl⟩ := hvf
            have hflag : ((w.nodes j).pn.recvRequestVote c' nt clt cd).2
                = true := by
              have hadopt2 : ¬ (w.nodes j).pn.currentTerm < nt := hadopt
              have hvfpre2 : (w.nodes j).pn.votedFor = none := hvfpre
              have hlog2 : logOk (w.nodes j).pn.lastTerm
                  (w.nodes j).pn.durable clt cd = true := hlog
              simp only [PNode.recvRequestVote, if_neg hadopt2, hvfpre2,
                PNode.recvRequestVote.grantIfFresh]
              rw [if_pos hlog2]
            have htnt : t = nt := by omega
            subst htnt
            refine List.mem_append_right _ ?_
            rw [hflag]
            exact List.mem_singleton.mpr rfl
          · dsimp only at hvf
            rw [hvfpre] at hvf
            cases hvf
        · by_cases hvt : vt = (w.nodes j).dn.pn.currentTerm
          · -- pinned current-term record: state unchanged
            have hstate := (Data.recv_voted_current (w.nodes j).dn.pn c' vid
              nt clt cd hctnt (by rw [hvfpre, hvt])).1
            rw [hstate] at hvf hct
            exact List.mem_append_left _ (h j c t hvf hct hcj)
          · -- stale record falls through to a fresh grant
            simp only [PNode.recvRequestVote, if_neg hadopt, hvfpre,
              if_neg hvt, PNode.recvRequestVote.grantIfFresh] at hvf hct
            split_ifs at hvf hct with hlog
            · simp only [Option.some.injEq, Prod.mk.injEq] at hvf
              obtain ⟨hteq, rfl⟩ := hvf
              have hflag : ((w.nodes j).pn.recvRequestVote c' nt clt cd).2
                  = true := by
                have hadopt2 : ¬ (w.nodes j).pn.currentTerm < nt := hadopt
                have hvfpre2 : (w.nodes j).pn.votedFor = some (vt, vid) :=
                  hvfpre
                have hvt2 : ¬ vt = (w.nodes j).pn.currentTerm := hvt
                have hlog2 : logOk (w.nodes j).pn.lastTerm
                    (w.nodes j).pn.durable clt cd = true := hlog
                simp only [PNode.recvRequestVote, if_neg hadopt2, hvfpre2,
                  if_neg hvt2, PNode.recvRequestVote.grantIfFresh]
                rw [if_pos hlog2]
              have htnt : t = nt := by omega
              subst htnt
              refine List.mem_append_right _ ?_
              rw [hflag]
              exact List.mem_singleton.mpr rfl
            · dsimp only at hvf hct
              rw [hvfpre] at hvf
              simp only [Option.some.injEq, Prod.mk.injEq] at hvf
              exact absurd (hvf.1.trans hct.symm) hvt
    · simp only [Node.pn, Function.update_of_ne hne] at hvf hct
      exact List.mem_append_left _ (h j c t hvf hct hcj)
  | rejectStaleRequestVote j' c' nt clt cd hmsg hstale =>
    intro j c t hvf hct hcj
    exact List.mem_append_left _ (h j c t hvf hct hcj)
  | deliverVote i v t' hmsg hrole hterm =>
    intro j c t hvf hct hcj
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hvf hct
      exact h j c t hvf hct hcj
    · simp only [Node.pn, Function.update_of_ne hne] at hvf hct
      exact h j c t hvf hct hcj
  | deliverVoteHigherTerm i v t' g hmsg hterm =>
    intro j c t hvf hct hcj
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.pn, Function.update_self, PNode.adoptTerm] at hvf
      cases hvf
    · simp only [Node.pn, Function.update_of_ne hne] at hvf hct
      exact h j c t hvf hct hcj
  | becomeLeader i hrole hquorum =>
    intro j c t hvf hct hcj
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hvf hct
      exact h j c t hvf hct hcj
    · simp only [Node.pn, Function.update_of_ne hne] at hvf hct
      exact h j c t hvf hct hcj
  | absorbDurable i hrole =>
    intro j c t hvf hct hcj
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hvf hct
      exact h j c t hvf hct hcj
    · simp only [Node.pn, Function.update_of_ne hne] at hvf hct
      exact h j c t hvf hct hcj
  | crashRestart i =>
    intro j c t hvf hct hcj
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hvf hct
      exact h j c t hvf hct hcj
    · simp only [Node.pn, Function.update_of_ne hne] at hvf hct
      exact h j c t hvf hct hcj
  | leaderAppend i v hrole =>
    intro j c t hvf hct hcj
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hvf hct
      exact h j c t hvf hct hcj
    · simp only [Node.pn, Function.update_of_ne hne] at hvf hct
      exact h j c t hvf hct hcj
  | deliverReplicate j' pos hdr t' v hmsg hpos hhdr hgate =>
    intro j c t hvf hct hcj
    rcases eq_or_ne j j' with rfl | hne
    · simp only [Node.pn, Function.update_self,
        Uc2.Data.Node.recvReplicate] at hvf hct
      exact h j c t hvf hct hcj
    · simp only [Node.pn, Function.update_of_ne hne] at hvf hct
      exact h j c t hvf hct hcj
  | serveTail i p t' v hrole hhist hp => exact h
  | shipTermMap i hrole => exact h
  | deliverTermMap j' t' entries hmsg hterm =>
    intro j c t hvf hct hcj
    rcases eq_or_ne j j' with rfl | hne
    · simp only [Node.pn, Function.update_self] at hvf hct
      by_cases hadopt : (w.nodes j).dn.pn.currentTerm < t'
      · rw [(Data.applyGossip_adopt (w.nodes j).dn entries hadopt).2] at hct
        have hvf2 : ((w.nodes j).dn.applyGossip t' entries).pn.votedFor
            = none := by
          cases hrec : Uc2.reconcile (w.nodes j).dn.termMap
              (w.nodes j).dn.pn.durable entries <;>
            simp [Uc2.Data.Node.applyGossip, hrec, if_pos hadopt,
              PNode.adoptTerm]
        rw [hvf2] at hvf
        cases hvf
      · rw [(Data.applyGossip_no_adopt (w.nodes j).dn entries hadopt).2.2]
          at hvf
        rw [(Data.applyGossip_no_adopt (w.nodes j).dn entries hadopt).2.1]
          at hct
        exact h j c t hvf hct hcj
    · simp only [Node.pn, Function.update_of_ne hne] at hvf hct
      exact h j c t hvf hct hcj
  | sendReport j' hrole hgate => exact h
  | deliverReport i src t' d hmsg hrole hterm hsrc =>
    intro j c t hvf hct hcj
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hvf hct
      exact h j c t hvf hct hcj
    · simp only [Node.pn, Function.update_of_ne hne] at hvf hct
      exact h j c t hvf hct hcj
  | leaderAdvanceCommit i k hrole hbase hadv =>
    intro j c t hvf hct hcj
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hvf hct
      exact h j c t hvf hct hcj
    · simp only [Node.pn, Function.update_of_ne hne] at hvf hct
      exact h j c t hvf hct hcj

/-- **Recorded non-self votes are on the wire** in every reachable world. -/
theorem reachable_voted_msg {n : Nat} {w : World n} (hw : Reachable w) :
    VotedMsg w := by
  induction hw with
  | refl => intro j c t hvf; simp [World.init, Node.pn] at hvf
  | tail hprev hstep ih => exact vm_step hprev ih hstep

#print axioms reachable_voted_msg

/-! ## B2 — `grant_report`: the corrected M1

A grant above a reported term records the voter's credentials against the
candidate's requestVote. Arms (all message-indexed, hence step-stable):
the damage escape is GOSSIP-witnessed (review M-1 — gossip delivery is the
only truncation vehicle, so a certificate alone, which can exist without a
realized win, is not damage-capable evidence); the good arm is conditioned
on a tenure-`T` append frame strictly below the reported durable (supplied
at consumption by `RepQuorum`'s base witness `bT < k ≤ d`), which forces
the voter's frontier to exactly `T` at grant time (`report_era_floor`) and
lets `logOk` transport the floor onto `(clt, cd)`. Self-votes (`y = c`)
are excluded: the consumer handles the reporter-is-the-candidate slot
directly through B1. -/
def GrantReport {n : Nat} (w : World n) : Prop :=
  ∀ (y c : Fin n) (u T d : Nat), y ≠ c →
    Msg.vote y c u true ∈ w.sent → CMsg.report y T d ∈ w.csent →
    T < u → 1 ≤ T →
    ∃ clt cd, Msg.requestVote c u clt cd ∈ w.sent ∧
      ((∃ u'' es, T < u'' ∧ Frame.gossip u'' es ∈ w.dsent) ∨
        ∀ b v0, Frame.replicate b T T v0 ∈ w.dsent → b < d →
          (T < clt ∨ (clt = T ∧ d ≤ cd)))

private theorem gr_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : GrantReport w) (hs : Step w w') : GrantReport w' := by
  cases hs with
  | deliverVote i v t hmsg hrole hterm => exact h
  | deliverVoteHigherTerm i v t g hmsg hterm => exact h
  | becomeLeader i hrole hquorum => exact h
  | absorbDurable i hrole => exact h
  | crashRestart i => exact h
  | deliverReport i src t d hmsg hrole hterm hsrc => exact h
  | leaderAdvanceCommit i k hrole hbase hadv => exact h
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate => exact h
  | deliverTermMap j t entries hmsg hterm => exact h
  | startElection i hrole =>
    intro y c u T d hyc hv hrp hTu h1T
    rcases List.mem_append.mp hv with hv | hv
    · obtain ⟨clt, cd, hrv, harm⟩ := h y c u T d hyc hv hrp hTu h1T
      exact ⟨clt, cd, List.mem_append_left _ hrv, harm⟩
    · simp at hv
  | rejectStaleRequestVote j c' nt clt cd hmsg hstale =>
    intro y c u T d hyc hv hrp hTu h1T
    rcases List.mem_append.mp hv with hv | hv
    · obtain ⟨clt', cd', hrv, harm⟩ := h y c u T d hyc hv hrp hTu h1T
      exact ⟨clt', cd', List.mem_append_left _ hrv, harm⟩
    · simp only [List.mem_singleton, Msg.vote.injEq] at hv
      exact absurd hv.2.2.2 (by decide)
  | sendReport j hrole hgate =>
    intro y c u T d hyc hv hrp hTu h1T
    rcases List.mem_append.mp hrp with hrp | hrp
    · exact h y c u T d hyc hv hrp hTu h1T
    · exfalso
      simp only [List.mem_singleton, CMsg.report.injEq] at hrp
      obtain ⟨rfl, rfl, rfl⟩ := hrp
      have hgs := (Uc2.reachable_inv
        (Data.reachable_project (reachable_project hw))).grant_state y c u hv
      have hgs' : u < (w.nodes y).dn.pn.currentTerm ∨
          ((w.nodes y).dn.pn.currentTerm = u ∧
            (w.nodes y).dn.pn.votedFor = some (u, c)) := hgs
      have hTu' : (w.nodes y).dn.pn.currentTerm < u := hTu
      omega
  | leaderAppend i v hrole =>
    intro y c u T d hyc hv hrp hTu h1T
    obtain ⟨clt, cd, hrv, harm⟩ := h y c u T d hyc hv hrp hTu h1T
    refine ⟨clt, cd, hrv, ?_⟩
    rcases harm with ⟨u'', es, hu, hg⟩ | hgood
    · exact .inl ⟨u'', es, hu, List.mem_append_left _ hg⟩
    · right
      intro b v0 hf hbd
      rcases List.mem_append.mp hf with hf | hf
      · exact hgood b v0 hf hbd
      · exfalso
        simp only [List.mem_singleton, Frame.replicate.injEq] at hf
        obtain ⟨hb, hctT, -, -⟩ := hf
        have hrlf := reachable_report_leader_floor hw y T d hrp h1T i hrole
          hctT.symm
        have hb' : b = (w.nodes i).dn.pn.durable := hb
        have hrlf' : d ≤ (w.nodes i).dn.pn.durable := hrlf
        omega
  | serveTail i p t v hrole hhist hp =>
    intro y c u T d hyc hv hrp hTu h1T
    obtain ⟨clt, cd, hrv, harm⟩ := h y c u T d hyc hv hrp hTu h1T
    refine ⟨clt, cd, hrv, ?_⟩
    rcases harm with ⟨u'', es, hu, hg⟩ | hgood
    · exact .inl ⟨u'', es, hu, List.mem_append_left _ hg⟩
    · right
      intro b v0 hf hbd
      rcases List.mem_append.mp hf with hf | hf
      · exact hgood b v0 hf hbd
      · simp only [List.mem_singleton, Frame.replicate.injEq] at hf
        obtain ⟨hbp, hctT, htT, hv0⟩ := hf
        have horig := (reachable_orig hw).1 i p t v hhist
        refine hgood b v0 ?_ hbd
        rw [hbp, htT, hv0]
        exact horig
  | shipTermMap i hrole =>
    intro y c u T d hyc hv hrp hTu h1T
    obtain ⟨clt, cd, hrv, harm⟩ := h y c u T d hyc hv hrp hTu h1T
    refine ⟨clt, cd, hrv, ?_⟩
    rcases harm with ⟨u'', es, hu, hg⟩ | hgood
    · exact .inl ⟨u'', es, hu, List.mem_append_left _ hg⟩
    · right
      intro b v0 hf hbd
      rcases List.mem_append.mp hf with hf | hf
      · exact hgood b v0 hf hbd
      · simp at hf
  | deliverRequestVote j c' nt clt cd hmsg hterm =>
    intro y c u T d hyc hv hrp hTu h1T
    rcases List.mem_append.mp hv with hv | hv
    · obtain ⟨clt', cd', hrv, harm⟩ := h y c u T d hyc hv hrp hTu h1T
      exact ⟨clt', cd', List.mem_append_left _ hrv, harm⟩
    · simp only [List.mem_singleton, Msg.vote.injEq] at hv
      obtain ⟨rfl, rfl, rfl, hflag⟩ := hv
      rcases recv_grant_cases (w.nodes y).dn.pn c u clt cd hterm hflag.symm
        with hlog | ⟨hvfeq, hcteq⟩
      · -- a fresh grant: the era-split
        refine ⟨clt, cd, List.mem_append_left _ hmsg, ?_⟩
        by_cases hesc : ∃ u'' es, T < u'' ∧ Frame.gossip u'' es ∈ w.dsent
        · exact .inl hesc
        · right
          have hera : Era w T := by
            intro u'' es hg
            by_contra hgt
            exact hesc ⟨u'', es, by omega, hg⟩
          intro b v0 hf hbd
          obtain ⟨hfl, hst⟩ := reachable_report_era_floor hw y T d hrp h1T
            hera
          have hlts : (w.nodes y).dn.pn.lastTerm
              = Data.lastTermOf (w.nodes y).dn.termMap :=
            reachable_lastTerm_sync hw y
          have hlogI := (Uc2.logOk_iff _ _ _ _).mp hlog
          rcases hst with h1 | h2
          · left
            omega
          · have hfl' : d ≤ (w.nodes y).dn.pn.durable := hfl
            have hpin := h2.fpin b v0 hf (by omega)
            have hasc : TermMap.Ascending (w.nodes y).dn.termMap :=
              ((Data.reachable_mapsWF (reachable_project hw)) y).1
            have hle2 := TermMap.termAt_le_lastTermOf hasc b
            have hfle : Data.lastTermOf (w.nodes y).dn.termMap ≤ T :=
              h2.frontier_le
            rcases hlogI with hlt | ⟨heq, hdur⟩
            · left
              omega
            · right
              constructor
              · omega
              · omega
      · -- idempotent re-grant: the original grant message carries the pair
        have hprev := reachable_voted_msg hw y c u hvfeq hcteq
          (fun hc => hyc hc.symm)
        obtain ⟨clt', cd', hrv, harm⟩ := h y c u T d hyc hprev hrp hTu h1T
        exact ⟨clt', cd', List.mem_append_left _ hrv, harm⟩

/-- **B2/M1** in every reachable world. -/
theorem reachable_grant_report {n : Nat} {w : World n} (hw : Reachable w) :
    GrantReport w := by
  induction hw with
  | refl => intro y c u T d hyc hv; simp [World.init] at hv
  | tail hprev hstep ih => exact gr_step hprev ih hstep

#print axioms reachable_grant_report

end Uc2.Cert
