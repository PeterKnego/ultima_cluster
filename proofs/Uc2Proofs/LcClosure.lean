import Uc2Proofs.Commit
import Uc2Proofs.ReportProvenance

/-! LC4 — LC-closure plumbing: the commit-event quorum extraction and the
regime/committed witnesses that any `committed_term_at_leaders` induction
consumes, each landed fully proven and standalone.

**Status honesty (task LC4).** The arc target — `committed_term_at_leaders`
and the unconditional `leader_completeness` — is NOT in this file. The
becomeLeader-crux design (worked out in full in the LC4 task report) needs
an ENTRY-level canonical-prefix bundle (`reconcile`'s `commonPrefixLen` is
entry equality, while the LC2/LC3 stack pins ATTRIBUTION (`termAt`) only),
and the re-brief's amendment-2 lemma (within-regime durable stability)
turned out NOT to be extractable standalone: its `deliverTermMap` case at a
carried-open LAGGED CANDIDATE (the B1 over-approx state) truncating against
a higher-certified-term gossip is exactly as strong as canonical-prefix
agreement below the reported frontier — i.e. it is mutually inductive with
the very canon machinery it was meant to precede. Per the stuck protocol
(proof-engineering resistance at the ceiling → STOP with a design record,
never weaken, nothing admitted), this file lands the standalone-provable pieces:

- `reachable_hist_defined` — a node's history is DEFINED on `[0, durable)`
  (the contiguity converse of `ProvInv.hist_bound`; the endgame's step from
  `p < durable i` to `hist i p = some _`).
- `reachable_gossip_uniq` — one term, one gossip map (tenure maps are
  frozen; every ship re-ships the same map).
- `reachable_no_self_report` — a candidate/leader has no self-report at its
  own current term in flight (kills the `report_take` self-instances at
  `becomeLeader`/`leaderAppend`).
- `reachable_regime_witness` — an open gate at a live regime is WITNESSED:
  a gossip at the regime term is on the wire, or the node itself is the
  regime's certified writer (the seam that splits every regime-history
  argument into gossip-witnessed vs winner-self).
- `reachable_trackerInv` + `advance_report_quorum` — tracker slots trace to
  `csent` reports at the leader's term, and a firing kernel `advance`
  yields a `Finset` quorum of report-witnessed members (the C1–C5 → C5
  `quorum_intersect` bridge, LB2-rerun §3's "slot-count → Finset quorum
  conversion").
- `reachable_committed_cert` — every committed entry's commit term is
  certified and bounds its stamp (`1 ≤ stamp ≤ T`).

The full invariant design for the remaining gap (the take-discipline bundle
`{strict bases, gate_take, open_leader}` + the RepQuorum-keyed no-branch
clause + the grant/report credential bridge + `committed_term_at_leaders`)
is recorded in `.superpowers/sdd/task-LC4-report.md`. -/

namespace Uc2.Cert

open Uc2.Data (Frame)

/-- Certificate carry across one commit-layer step (public-API re-derivation
of `ReportProvenance.lean`'s private `cert_carry`). -/
private theorem ccarry {n : Nat} {w w' : World n} (hs : Step w w')
    {t : Nat} {ℓ : Fin n} (hc : Data.Cert w.project t ℓ) :
    Data.Cert w'.project t ℓ :=
  Data.cert_drtg (step_project hs) hc

/-! ## Contiguity: `hist` is defined below the durable frontier -/

/-- A node's stamped history is defined at every position below its durable
frontier — the converse of `ProvInv.hist_bound`. This is what turns the
endgame's `p < durable i` into an actual `hist i p = some _`. -/
def HistDefined {n : Nat} (w : World n) : Prop :=
  ∀ j : Fin n, ∀ p : Nat, p < (w.nodes j).pn.durable →
    ∃ tv, (w.nodes j).hist p = some tv

private theorem hd_init (n : Nat) : HistDefined (World.init n) := by
  intro j p hp
  simp [World.init, Node.pn] at hp

private theorem hd_step {n : Nat} {w w' : World n} (h : HistDefined w)
    (hs : Step w w') : HistDefined w' := by
  cases hs with
  | startElection i _ =>
    intro k p hp
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Node.pn, Function.update_self] at hp ⊢
      exact h k p hp
    · simp only [Node.hist, Node.pn, Function.update_of_ne hne] at hp ⊢
      exact h k p hp
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    intro k p hp
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.hist, Node.pn, Function.update_self] at hp ⊢
      rw [Data.recv_durable] at hp
      exact h k p hp
    · simp only [Node.hist, Node.pn, Function.update_of_ne hne] at hp ⊢
      exact h k p hp
  | rejectStaleRequestVote j c nt clt cd hmsg hstale => exact h
  | deliverVote i v t hmsg hrole hterm =>
    intro k p hp
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Node.pn, Function.update_self] at hp ⊢
      exact h k p hp
    · simp only [Node.hist, Node.pn, Function.update_of_ne hne] at hp ⊢
      exact h k p hp
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    intro k p hp
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Node.pn, Function.update_self] at hp ⊢
      simp only [PNode.adoptTerm] at hp
      exact h k p hp
    · simp only [Node.hist, Node.pn, Function.update_of_ne hne] at hp ⊢
      exact h k p hp
  | becomeLeader i hrole hquorum =>
    intro k p hp
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Node.pn, Function.update_self] at hp ⊢
      exact h k p hp
    · simp only [Node.hist, Node.pn, Function.update_of_ne hne] at hp ⊢
      exact h k p hp
  | crashRestart i =>
    intro k p hp
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Node.pn, Function.update_self] at hp ⊢
      exact h k p hp
    · simp only [Node.hist, Node.pn, Function.update_of_ne hne] at hp ⊢
      exact h k p hp
  | leaderAppend i v hrole =>
    intro k p hp
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Node.pn, Function.update_self] at hp ⊢
      by_cases hpd : p = (w.nodes k).dn.pn.durable
      · subst hpd
        exact ⟨_, Function.update_self _ _ _⟩
      · rw [Function.update_of_ne hpd]
        have hp' : p < (w.nodes k).dn.pn.durable := by omega
        exact h k p hp'
    · simp only [Node.hist, Node.pn, Function.update_of_ne hne] at hp ⊢
      exact h k p hp
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate =>
    intro k p hp
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.hist, Node.pn, Function.update_self,
        Uc2.Data.Node.recvReplicate] at hp ⊢
      by_cases hpd : p = pos
      · subst hpd
        exact ⟨_, Function.update_self _ _ _⟩
      · rw [Function.update_of_ne hpd]
        simp only [Node.pn] at hpos
        have hp' : p < (w.nodes k).dn.pn.durable := by omega
        exact h k p hp'
    · simp only [Node.hist, Node.pn, Function.update_of_ne hne] at hp ⊢
      exact h k p hp
  | serveTail i p t v hrole hhist hp => exact h
  | shipTermMap i hrole => exact h
  | deliverTermMap j t entries hmsg hterm =>
    intro k p hp
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.hist, Node.pn, Function.update_self,
        Uc2.Data.Node.applyGossip] at hp ⊢
      cases hrec : Uc2.reconcile (w.nodes k).dn.termMap (w.nodes k).dn.pn.durable
          entries with
      | ok o =>
        rw [hrec] at hp
        dsimp only at hp ⊢
        rw [if_pos hp]
        have hle : o.validUpTo ≤ (w.nodes k).dn.pn.durable :=
          Uc2.reconcile_validUpTo_le _ _ _ _ hrec
        have hp' : p < (w.nodes k).dn.pn.durable := by omega
        exact h k p hp'
      | noCommonPrefix =>
        rw [hrec] at hp
        dsimp only at hp
        omega
    · simp only [Node.hist, Node.pn, Function.update_of_ne hne] at hp ⊢
      exact h k p hp
  | sendReport j hrole hgate => exact h
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    intro k p hp
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Node.pn, Function.update_self] at hp ⊢
      exact h k p hp
    · simp only [Node.hist, Node.pn, Function.update_of_ne hne] at hp ⊢
      exact h k p hp
  | leaderAdvanceCommit i k hrole hbase hadv =>
    intro j p hp
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.hist, Node.pn, Function.update_self] at hp ⊢
      exact h j p hp
    · simp only [Node.hist, Node.pn, Function.update_of_ne hne] at hp ⊢
      exact h j p hp

/-- **Contiguity.** In every reachable world, `hist j` is defined on
`[0, durable j)`. -/
theorem reachable_hist_defined {n : Nat} {w : World n} (hw : Reachable w) :
    HistDefined w := by
  induction hw with
  | refl => exact hd_init n
  | tail _ hstep ih => exact hd_step ih hstep

#print axioms reachable_hist_defined

/-! ## Gossip uniqueness: one term, one shipped map -/

/-- Same-term gossips carry the same map — a tenure's map is frozen and
every ship re-ships it (`DInv.gossip_pinned` at each emission). -/
def GossipUniq {n : Nat} (w : World n) : Prop :=
  ∀ t es es', Frame.gossip t es ∈ w.dsent → Frame.gossip t es' ∈ w.dsent →
    es = es'

private theorem gu_init (n : Nat) : GossipUniq (World.init n) := by
  intro t es es' h1
  simp [World.init] at h1

/-- Frame-append steps: a `replicate` frame changes no gossip membership. -/
private theorem gu_frame_append {n : Nat} {w : World n} (h : GossipUniq w)
    {ds : List Frame} {p hdr t v : Nat}
    (hds : ds = w.dsent ++ [.replicate p hdr t v]) :
    ∀ tg es es', Frame.gossip tg es ∈ ds → Frame.gossip tg es' ∈ ds →
      es = es' := by
  intro tg es es' h1 h2
  subst hds
  rcases List.mem_append.mp h1 with h1 | h1
  · rcases List.mem_append.mp h2 with h2 | h2
    · exact h tg es es' h1 h2
    · simp at h2
  · simp at h1

private theorem gu_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : GossipUniq w) (hs : Step w w') : GossipUniq w' := by
  cases hs with
  | startElection i _ => exact h
  | deliverRequestVote j c nt clt cd hmsg hterm => exact h
  | rejectStaleRequestVote j c nt clt cd hmsg hstale => exact h
  | deliverVote i v t hmsg hrole hterm => exact h
  | deliverVoteHigherTerm i v t g hmsg hterm => exact h
  | becomeLeader i hrole hquorum => exact h
  | crashRestart i => exact h
  | leaderAppend i v hrole => exact gu_frame_append h rfl
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate => exact h
  | serveTail i p t v hrole hhist hp => exact gu_frame_append h rfl
  | shipTermMap i hrole =>
    intro tg es es' h1 h2
    have hpin := (Data.reachable_dinv (reachable_project hw)).gossip_pinned i
      hrole
    rcases List.mem_append.mp h1 with h1 | h1 <;>
      rcases List.mem_append.mp h2 with h2 | h2
    · exact h tg es es' h1 h2
    · simp only [List.mem_singleton, Frame.gossip.injEq] at h2
      obtain ⟨rfl, rfl⟩ := h2
      exact hpin es h1
    · simp only [List.mem_singleton, Frame.gossip.injEq] at h1
      obtain ⟨rfl, rfl⟩ := h1
      exact (hpin es' h2).symm
    · simp only [List.mem_singleton, Frame.gossip.injEq] at h1 h2
      rw [h1.2, h2.2]
  | deliverTermMap j t entries hmsg hterm => exact h
  | sendReport j hrole hgate => exact h
  | deliverReport i src t d hmsg hrole hterm hsrc => exact h
  | leaderAdvanceCommit i k hrole hbase hadv => exact h

/-- **Gossip uniqueness** in every reachable world. -/
theorem reachable_gossip_uniq {n : Nat} {w : World n} (hw : Reachable w) :
    GossipUniq w := by
  induction hw with
  | refl => exact gu_init n
  | tail hprev hstep ih => exact gu_step hprev ih hstep

#print axioms reachable_gossip_uniq

/-! ## No self-report at the own term for candidates/leaders -/

/-- A non-follower has no self-report at its own current term in flight:
reports are follower-only, and reaching candidacy at a term strictly bumps
past every term the node ever reported at. Kills the reporter-is-the-leader
instances at `becomeLeader`/`leaderAppend` in the endgame bundle. -/
def NoSelfReport {n : Nat} (w : World n) : Prop :=
  ∀ j : Fin n, (w.nodes j).pn.role ≠ .follower →
    ∀ d, CMsg.report j ((w.nodes j).pn.currentTerm) d ∉ w.csent

private theorem nsr_init (n : Nat) : NoSelfReport (World.init n) := by
  intro j hrole
  simp [World.init, Node.pn] at hrole

private theorem nsr_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : NoSelfReport w) (hs : Step w w') : NoSelfReport w' := by
  cases hs with
  | startElection i _ =>
    intro k hrole d hmem
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hmem
      have h1 : (w.nodes k).dn.pn.currentTerm + 1 ≤ (w.nodes k).dn.dataTerm :=
        (reachable_provInv hw).report_dt k _ d hmem
      have h2 : (w.nodes k).dn.dataTerm ≤ (w.nodes k).dn.pn.currentTerm :=
        (Data.reachable_stamp (reachable_project hw)).data_le k
      omega
    · simp only [Node.pn, Function.update_of_ne hne] at hrole hmem
      exact h k hrole d hmem
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    intro k hrole d hmem
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.pn, Function.update_self] at hrole hmem
      by_cases hadopt : (w.nodes k).dn.pn.currentTerm < nt
      · rw [Data.recv_adopt_role _ _ _ _ _ hadopt] at hrole
        exact hrole rfl
      · rw [(Data.recv_frame _ _ _ _ _ hadopt).1] at hrole
        rw [(Data.recv_frame _ _ _ _ _ hadopt).2] at hmem
        exact h k hrole d hmem
    · simp only [Node.pn, Function.update_of_ne hne] at hrole hmem
      exact h k hrole d hmem
  | rejectStaleRequestVote j c nt clt cd hmsg hstale => exact h
  | deliverVote i v t hmsg hrole hterm =>
    intro k hnf d hmem
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hnf hmem
      exact h k hnf d hmem
    · simp only [Node.pn, Function.update_of_ne hne] at hnf hmem
      exact h k hnf d hmem
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    intro k hnf d hmem
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self, PNode.adoptTerm] at hnf
      exact hnf rfl
    · simp only [Node.pn, Function.update_of_ne hne] at hnf hmem
      exact h k hnf d hmem
  | becomeLeader i hrole hquorum =>
    intro k hnf d hmem
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hmem
      refine h k ?_ d hmem
      rw [hrole]
      decide
    · simp only [Node.pn, Function.update_of_ne hne] at hnf hmem
      exact h k hnf d hmem
  | crashRestart i =>
    intro k hnf d hmem
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hnf
      exact hnf rfl
    · simp only [Node.pn, Function.update_of_ne hne] at hnf hmem
      exact h k hnf d hmem
  | leaderAppend i v hrole =>
    intro k hnf d hmem
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hnf hmem
      exact h k hnf d hmem
    · simp only [Node.pn, Function.update_of_ne hne] at hnf hmem
      exact h k hnf d hmem
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate =>
    intro k hnf d hmem
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.pn, Function.update_self,
        Uc2.Data.Node.recvReplicate] at hnf hmem
      exact h k hnf d hmem
    · simp only [Node.pn, Function.update_of_ne hne] at hnf hmem
      exact h k hnf d hmem
  | serveTail i p t v hrole hhist hp => exact h
  | shipTermMap i hrole => exact h
  | deliverTermMap j t entries hmsg hterm =>
    intro k hnf d hmem
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.pn, Function.update_self] at hnf hmem
      by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
      · rw [(Data.applyGossip_adopt _ _ hadopt).1] at hnf
        exact hnf rfl
      · rw [(Data.applyGossip_no_adopt _ _ hadopt).1] at hnf
        rw [(Data.applyGossip_no_adopt _ _ hadopt).2.1] at hmem
        exact h k hnf d hmem
    · simp only [Node.pn, Function.update_of_ne hne] at hnf hmem
      exact h k hnf d hmem
  | sendReport j hrole hgate =>
    intro k hnf d hmem
    rcases List.mem_append.mp hmem with hold | hnew
    · exact h k hnf d hold
    · simp only [List.mem_singleton, CMsg.report.injEq] at hnew
      obtain ⟨rfl, -, -⟩ := hnew
      rw [hrole] at hnf
      exact hnf rfl
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    intro k hnf d' hmem
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hnf hmem
      exact h k hnf d' hmem
    · simp only [Node.pn, Function.update_of_ne hne] at hnf hmem
      exact h k hnf d' hmem
  | leaderAdvanceCommit i k hrole hbase hadv =>
    intro j hnf d hmem
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hnf hmem
      exact h j hnf d hmem
    · simp only [Node.pn, Function.update_of_ne hne] at hnf hmem
      exact h j hnf d hmem

/-- **No self-report**: a candidate/leader never has a report of its own at
its current term in flight. -/
theorem reachable_no_self_report {n : Nat} {w : World n} (hw : Reachable w) :
    NoSelfReport w := by
  induction hw with
  | refl => exact nsr_init n
  | tail hprev hstep ih => exact nsr_step hprev ih hstep

#print axioms reachable_no_self_report

/-! ## Regime witness: an open gate names its evidence -/

/-- An open gate at a live (≥ 1) regime is witnessed on the wire or by the
node's own writer certificate: either a gossip at the regime term was
shipped (the reconcile that opened — or could have opened — the gate), or
the node itself won the regime term (`becomeLeader` opens gate-and-handle
together). The seam that splits every regime-history argument into
gossip-witnessed vs winner-self. -/
def RegimeWitness {n : Nat} (w : World n) : Prop :=
  ∀ j : Fin n, (w.nodes j).reconciled = true → 1 ≤ (w.nodes j).dataTerm →
    (∃ es, Frame.gossip ((w.nodes j).dataTerm) es ∈ w.dsent) ∨
      Data.Cert w.project ((w.nodes j).dataTerm) j

private theorem rgw_init (n : Nat) : RegimeWitness (World.init n) := by
  intro j _ hpos
  simp [World.init, Node.dataTerm] at hpos

/-- Generalized step: per open node, either gate and handle CARRY from the
pre-state, or a fresh witness (a gossip on the post wire / a post-state
certificate) is produced directly. All per-node normalization happens
inside the `hopen` obligation — its statement keeps the composite
`(w'.nodes k).dataTerm` pattern, which never occurs inside the `Cert`
world literal, so rewriting stays targeted. -/
private theorem rgw_step_gen {n : Nat} {w w' : World n} (hs : Step w w')
    (hw' : Reachable w') (h : RegimeWitness w)
    (hds : ∀ f, f ∈ w.dsent → f ∈ w'.dsent)
    (hopen : ∀ k, (w'.nodes k).reconciled = true → 1 ≤ (w'.nodes k).dataTerm →
      ((w'.nodes k).dataTerm = (w.nodes k).dataTerm ∧
        (w.nodes k).reconciled = true) ∨
      (∃ es, Frame.gossip ((w'.nodes k).dataTerm) es ∈ w'.dsent) ∨
      ((w'.nodes k).pn.role = .leader ∧
        (w'.nodes k).dataTerm = (w'.nodes k).pn.currentTerm)) :
    RegimeWitness w' := by
  intro k hr hpos
  rcases hopen k hr hpos with ⟨hdt, hrec⟩ | hg | ⟨hrl, hdt⟩
  · rw [hdt] at hpos ⊢
    rcases h k hrec hpos with ⟨es, hes⟩ | hc
    · exact .inl ⟨es, hds _ hes⟩
    · exact .inr (ccarry hs hc)
  · exact .inl hg
  · rw [hdt]
    exact .inr (Data.cert_of_leader
      (Uc2.reachable_inv (Data.reachable_project (reachable_project hw')))
      (i := k) hrl)

private theorem rgw_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (hw' : Reachable w') (h : RegimeWitness w) (hs : Step w w') :
    RegimeWitness w' := by
  refine rgw_step_gen hs hw' h (fun f hf => ?_) ?_
  · cases hs <;> first
      | exact hf
      | exact List.mem_append_left _ hf
  · intro k hr hpos
    cases hs with
    | startElection i _ =>
      refine .inl ⟨?_, ?_⟩ <;> rcases eq_or_ne k i with rfl | hne
      · simp [Node.dataTerm, Function.update_self]
      · simp [Node.dataTerm, Function.update_of_ne hne]
      · simpa [Function.update_self] using hr
      · simpa [Function.update_of_ne hne] using hr
    | deliverRequestVote j c nt clt cd hmsg hterm =>
      rcases eq_or_ne k j with rfl | hne
      · by_cases hadopt : (w.nodes k).pn.currentTerm < nt
        · simp only [Function.update_self] at hr
          rw [if_pos hadopt] at hr
          cases hr
        · refine .inl ⟨?_, ?_⟩
          · simp only [Node.dataTerm, Function.update_self]
            rw [if_neg hadopt]
          · simp only [Function.update_self] at hr
            rwa [if_neg hadopt] at hr
      · exact .inl ⟨by simp [Node.dataTerm, Function.update_of_ne hne],
          by simpa [Function.update_of_ne hne] using hr⟩
    | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
      exact .inl ⟨rfl, hr⟩
    | deliverVote i v t hmsg hrole hterm =>
      refine .inl ⟨?_, ?_⟩ <;> rcases eq_or_ne k i with rfl | hne
      · simp [Node.dataTerm, Function.update_self]
      · simp [Node.dataTerm, Function.update_of_ne hne]
      · simpa [Function.update_self] using hr
      · simpa [Function.update_of_ne hne] using hr
    | deliverVoteHigherTerm i v t g hmsg hterm =>
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hr
        cases hr
      · exact .inl ⟨by simp [Node.dataTerm, Function.update_of_ne hne],
          by simpa [Function.update_of_ne hne] using hr⟩
    | becomeLeader i hrole hquorum =>
      rcases eq_or_ne k i with rfl | hne
      · refine .inr (.inr ⟨?_, ?_⟩) <;>
          simp [Node.dataTerm, Node.pn, Function.update_self]
      · exact .inl ⟨by simp [Node.dataTerm, Function.update_of_ne hne],
          by simpa [Function.update_of_ne hne] using hr⟩
    | crashRestart i =>
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hr
        have hle : (w.nodes k).dn.pn.currentTerm ≤
            Data.lastTermOf (w.nodes k).dn.termMap := of_decide_eq_true hr
        have h2 : Data.lastTermOf (w.nodes k).dn.termMap ≤
            (w.nodes k).dn.dataTerm :=
          Data.reachable_map_le_dataTerm (reachable_project hw) k
        have h3 : (w.nodes k).dn.dataTerm ≤ (w.nodes k).dn.pn.currentTerm :=
          (Data.reachable_stamp (reachable_project hw)).data_le k
        refine .inl ⟨?_, ?_⟩
        · simp only [Node.dataTerm, Node.pn, Function.update_self]
          omega
        · by_contra hcl
          have h4 : Data.lastTermOf (w.nodes k).dn.termMap <
              (w.nodes k).dn.dataTerm :=
            (reachable_provInv hw).closed_lag k (Bool.eq_false_iff.mpr hcl)
          omega
      · exact .inl ⟨by simp [Node.dataTerm, Function.update_of_ne hne],
          by simpa [Function.update_of_ne hne] using hr⟩
    | leaderAppend i v hrole =>
      refine .inl ⟨?_, ?_⟩ <;> rcases eq_or_ne k i with rfl | hne
      · simp [Node.dataTerm, Function.update_self]
      · simp [Node.dataTerm, Function.update_of_ne hne]
      · simpa [Function.update_self] using hr
      · simpa [Function.update_of_ne hne] using hr
    | deliverReplicate j pos hdr t v hmsg hpos2 hhdr hgate =>
      refine .inl ⟨?_, ?_⟩ <;> rcases eq_or_ne k j with rfl | hne
      · simp [Node.dataTerm, Function.update_self, Uc2.Data.Node.recvReplicate]
      · simp [Node.dataTerm, Function.update_of_ne hne]
      · simpa [Function.update_self] using hr
      · simpa [Function.update_of_ne hne] using hr
    | serveTail i p t v hrole hhist hp => exact .inl ⟨rfl, hr⟩
    | shipTermMap i hrole => exact .inl ⟨rfl, hr⟩
    | deliverTermMap j t entries hmsg hterm =>
      rcases eq_or_ne k j with rfl | hne
      · simp only [Node.dataTerm, Function.update_self] at hr hpos ⊢
        rw [Data.applyGossip_dataTerm] at hpos ⊢
        by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
        · rw [if_pos hadopt]
          exact .inr (.inl ⟨entries, hmsg⟩)
        · rw [if_neg hadopt]
          have hadopt' : ¬ (w.nodes k).pn.currentTerm < t := hadopt
          rw [if_neg hadopt'] at hr
          simp only [Bool.or_eq_true] at hr
          rcases hr with hr | hdec
          · exact .inl ⟨rfl, hr⟩
          · have hdteq : (w.nodes k).dn.dataTerm =
                (w.nodes k).pn.currentTerm := of_decide_eq_true hdec
            have hteq : t = (w.nodes k).pn.currentTerm :=
              Nat.le_antisymm (Nat.not_lt.mp hadopt') hterm
            rw [hdteq, ← hteq]
            exact .inr (.inl ⟨entries, hmsg⟩)
      · exact .inl ⟨by simp [Node.dataTerm, Function.update_of_ne hne],
          by simpa [Function.update_of_ne hne] using hr⟩
    | sendReport j hrole hgate => exact .inl ⟨rfl, hr⟩
    | deliverReport i src t d hmsg hrole hterm hsrc =>
      refine .inl ⟨?_, ?_⟩ <;> rcases eq_or_ne k i with rfl | hne
      · simp [Node.dataTerm, Function.update_self]
      · simp [Node.dataTerm, Function.update_of_ne hne]
      · simpa [Function.update_self] using hr
      · simpa [Function.update_of_ne hne] using hr
    | leaderAdvanceCommit i kk hrole hbase hadv =>
      refine .inl ⟨?_, ?_⟩ <;> rcases eq_or_ne k i with rfl | hne
      · simp [Node.dataTerm, Function.update_self]
      · simp [Node.dataTerm, Function.update_of_ne hne]
      · simpa [Function.update_self] using hr
      · simpa [Function.update_of_ne hne] using hr

/-- **Regime witness** in every reachable world. -/
theorem reachable_regime_witness {n : Nat} {w : World n} (hw : Reachable w) :
    RegimeWitness w := by
  induction hw with
  | refl => exact rgw_init n
  | tail hprev hstep ih => exact rgw_step hprev (hprev.tail hstep) ih hstep

#print axioms reachable_regime_witness

/-! ## Tracker provenance: slots trace to reports on the commit wire -/

/-- Local copies of `Uc2Proofs/Commit.lean`'s private tracker bookkeeping. -/
private theorem getD_replicate_zero' (m i : Nat) :
    (List.replicate m (0 : Nat)).getD i 0 = 0 := by
  rcases Nat.lt_or_ge i m with hlt | hge
  · rw [List.getD_eq_getElem?_getD, List.getElem?_eq_getElem (by simpa using hlt)]
    simp
  · rw [List.getD_eq_getElem?_getD, List.getElem?_eq_none (by simpa using hge)]
    rfl

private theorem advance_fst_reported' (t : CommitTracker) (own : Nat) :
    ((t.advance own).1).reported = t.reported := by
  simp only [CommitTracker.advance]
  split <;> rfl

private theorem advance_fst_quorum' (t : CommitTracker) (own : Nat) :
    ((t.advance own).1).quorum = t.quorum := by
  simp only [CommitTracker.advance]
  split <;> rfl

/-- Tracker well-formedness + slot provenance: every tracker keeps the boot
quorum/slot-count shape, and at a LEADER every nonzero slot value is
covered by an actual `CMsg.report` at the leader's current term from the
slot's member (per-slot `max`-fold ⇒ the report's durable dominates the
slot value). -/
def TrackerInv {n : Nat} (w : World n) : Prop :=
  ∀ i : Fin n, (w.nodes i).tracker.quorum = n / 2 + 1 ∧
    (w.nodes i).tracker.reported.length = n - 1 ∧
    ((w.nodes i).pn.role = .leader →
      ∀ s : Nat, 0 < (w.nodes i).tracker.reported.getD s 0 →
        ∃ (src : Fin n) (d : Nat), src ≠ i ∧ slotOf i src = s ∧
          (w.nodes i).tracker.reported.getD s 0 ≤ d ∧
          CMsg.report src ((w.nodes i).pn.currentTerm) d ∈ w.csent)

private theorem ti_init (n : Nat) : TrackerInv (World.init n) := by
  intro i
  refine ⟨rfl, by simp [World.init, CommitTracker.new], ?_⟩
  intro _ s hpos
  have := getD_replicate_zero' (n - 1) s
  simp only [World.init] at hpos
  rw [show (CommitTracker.new (n - 1) n).reported
    = List.replicate (n - 1) 0 from rfl] at hpos
  omega

/-- Carry: tracker untouched per node, leadership only ever narrows, term
stable at surviving leaders, commit wire monotone. -/
private theorem ti_carry {n : Nat} {w w' : World n} (h : TrackerInv w)
    (hnode : ∀ k, (w'.nodes k).tracker = (w.nodes k).tracker ∧
      ((w'.nodes k).pn.role = .leader → (w.nodes k).pn.role = .leader ∧
        (w'.nodes k).pn.currentTerm = (w.nodes k).pn.currentTerm))
    (hcs : ∀ m, m ∈ w.csent → m ∈ w'.csent) : TrackerInv w' := by
  intro i
  obtain ⟨htr, hrl⟩ := hnode i
  obtain ⟨h1, h2, h3⟩ := h i
  refine ⟨by rw [htr]; exact h1, by rw [htr]; exact h2, ?_⟩
  intro hldr s hpos
  obtain ⟨hpre, hct⟩ := hrl hldr
  rw [htr] at hpos ⊢
  obtain ⟨src, d, hsrc, hslot, hle, hmem⟩ := h3 hpre s hpos
  exact ⟨src, d, hsrc, hslot, hle, by rw [hct]; exact hcs _ hmem⟩

private theorem ti_step {n : Nat} {w w' : World n} (h : TrackerInv w)
    (hs : Step w w') : TrackerInv w' := by
  cases hs with
  | startElection i _ =>
    refine ti_carry h (fun k => ?_) fun m hm => hm
    rcases eq_or_ne k i with rfl | hne
    · exact ⟨by simp [Function.update_self], fun hrl => absurd hrl
        (by simp [Node.pn, Function.update_self])⟩
    · simp [Node.pn, Function.update_of_ne hne]
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    refine ti_carry h (fun k => ?_) fun m hm => hm
    rcases eq_or_ne k j with rfl | hne
    · refine ⟨by simp [Function.update_self], ?_⟩
      intro hrl
      simp only [Node.pn, Function.update_self] at hrl ⊢
      by_cases hadopt : (w.nodes k).dn.pn.currentTerm < nt
      · rw [Data.recv_adopt_role _ _ _ _ _ hadopt] at hrl
        exact absurd hrl (by decide)
      · rw [(Data.recv_frame _ _ _ _ _ hadopt).1] at hrl
        exact ⟨hrl, (Data.recv_frame _ _ _ _ _ hadopt).2⟩
    · simp [Node.pn, Function.update_of_ne hne]
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    exact ti_carry h (fun k => ⟨rfl, fun hrl => ⟨hrl, rfl⟩⟩) fun m hm => hm
  | deliverVote i v t hmsg hrole hterm =>
    refine ti_carry h (fun k => ?_) fun m hm => hm
    rcases eq_or_ne k i with rfl | hne
    · exact ⟨by simp [Function.update_self],
        fun hrl => ⟨by simpa [Node.pn, Function.update_self] using hrl,
          by simp [Node.pn, Function.update_self]⟩⟩
    · simp [Node.pn, Function.update_of_ne hne]
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    refine ti_carry h (fun k => ?_) fun m hm => hm
    rcases eq_or_ne k i with rfl | hne
    · exact ⟨by simp [Function.update_self], fun hrl => absurd hrl
        (by simp [Node.pn, Function.update_self, PNode.adoptTerm])⟩
    · simp [Node.pn, Function.update_of_ne hne]
  | becomeLeader i hrole hquorum =>
    intro k
    rcases eq_or_ne k i with rfl | hne
    · obtain ⟨h1, h2, -⟩ := h k
      simp only [Node.pn, Function.update_self]
      refine ⟨h1, by simpa [CommitTracker.resetReports] using h2, ?_⟩
      intro _ s hpos
      rw [show ((w.nodes k).tracker.resetReports).reported
        = List.replicate (w.nodes k).tracker.reported.length 0 from rfl,
        getD_replicate_zero'] at hpos
      omega
    · obtain ⟨h1, h2, h3⟩ := h k
      simp only [Node.pn, Function.update_of_ne hne]
      exact ⟨h1, h2, h3⟩
  | crashRestart i =>
    intro k
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self]
      refine ⟨rfl, by simp [CommitTracker.new], ?_⟩
      intro hldr
      exact absurd hldr (by decide)
    · obtain ⟨h1, h2, h3⟩ := h k
      simp only [Node.pn, Function.update_of_ne hne]
      exact ⟨h1, h2, h3⟩
  | leaderAppend i v hrole =>
    refine ti_carry h (fun k => ?_) fun m hm => hm
    rcases eq_or_ne k i with rfl | hne
    · exact ⟨by simp [Function.update_self],
        fun hrl => ⟨by simpa [Node.pn, Function.update_self] using hrl,
          by simp [Node.pn, Function.update_self]⟩⟩
    · simp [Node.pn, Function.update_of_ne hne]
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate =>
    refine ti_carry h (fun k => ?_) fun m hm => hm
    rcases eq_or_ne k j with rfl | hne
    · refine ⟨by simp [Function.update_self], ?_⟩
      intro hrl
      simp only [Node.pn, Function.update_self] at hrl ⊢
      rw [(Data.recvReplicate_pn _ _ _ _).1] at hrl
      exact ⟨hrl, (Data.recvReplicate_pn _ _ _ _).2.1⟩
    · simp [Node.pn, Function.update_of_ne hne]
  | serveTail i p t v hrole hhist hp =>
    exact ti_carry h (fun k => ⟨rfl, fun hrl => ⟨hrl, rfl⟩⟩) fun m hm => hm
  | shipTermMap i hrole =>
    exact ti_carry h (fun k => ⟨rfl, fun hrl => ⟨hrl, rfl⟩⟩) fun m hm => hm
  | deliverTermMap j t entries hmsg hterm =>
    refine ti_carry h (fun k => ?_) fun m hm => hm
    rcases eq_or_ne k j with rfl | hne
    · refine ⟨by simp [Function.update_self], ?_⟩
      intro hrl
      simp only [Node.pn, Function.update_self] at hrl ⊢
      by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
      · rw [(Data.applyGossip_adopt _ _ hadopt).1] at hrl
        exact absurd hrl (by decide)
      · rw [(Data.applyGossip_no_adopt _ _ hadopt).1] at hrl
        exact ⟨hrl, (Data.applyGossip_no_adopt _ _ hadopt).2.1⟩
    · simp [Node.pn, Function.update_of_ne hne]
  | sendReport j hrole hgate =>
    exact ti_carry h (fun k => ⟨rfl, fun hrl => ⟨hrl, rfl⟩⟩)
      fun m hm => List.mem_append_left _ hm
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    intro k
    rcases eq_or_ne k i with rfl | hne
    · obtain ⟨h1, h2, h3⟩ := h k
      simp only [Node.pn, Function.update_self, CommitTracker.onDurable]
      refine ⟨h1, by simpa using h2, ?_⟩
      intro _ s hpos
      by_cases hseq : s = slotOf k src
      · subst hseq
        by_cases hlen : slotOf k src < (w.nodes k).tracker.reported.length
        · have hval : (((w.nodes k).tracker.reported).set (slotOf k src)
              (max ((w.nodes k).tracker.reported.getD (slotOf k src) 0) d)).getD
                (slotOf k src) 0
              = max ((w.nodes k).tracker.reported.getD (slotOf k src) 0) d := by
            rw [List.getD_eq_getElem?_getD, List.getElem?_set_self hlen]
            rfl
          rw [hval] at hpos ⊢
          rcases Nat.le_total ((w.nodes k).tracker.reported.getD
              (slotOf k src) 0) d with hod | hdo
          · exact ⟨src, d, hsrc, rfl, by omega, hterm ▸ hmsg⟩
          · rw [Nat.max_eq_left hdo] at hpos ⊢
            exact h3 hrole (slotOf k src) hpos
        · rw [List.set_eq_of_length_le (Nat.le_of_not_lt hlen)] at hpos ⊢
          exact h3 hrole (slotOf k src) hpos
      · have hval : (((w.nodes k).tracker.reported).set (slotOf k src)
            (max ((w.nodes k).tracker.reported.getD (slotOf k src) 0) d)).getD
              s 0
            = (w.nodes k).tracker.reported.getD s 0 := by
          rw [List.getD_eq_getElem?_getD,
            List.getElem?_set_ne (fun hc => hseq hc.symm)]
          rfl
        rw [hval] at hpos ⊢
        exact h3 hrole s hpos
    · obtain ⟨h1, h2, h3⟩ := h k
      simp only [Node.pn, Function.update_of_ne hne]
      exact ⟨h1, h2, h3⟩
  | leaderAdvanceCommit i kk hrole hbase hadv =>
    intro k
    rcases eq_or_ne k i with rfl | hne
    · obtain ⟨h1, h2, h3⟩ := h k
      simp only [Node.pn, Function.update_self]
      refine ⟨by rw [advance_fst_quorum']; exact h1,
        by rw [advance_fst_reported']; exact h2, ?_⟩
      intro _ s hpos
      rw [advance_fst_reported'] at hpos ⊢
      exact h3 hrole s hpos
    · obtain ⟨h1, h2, h3⟩ := h k
      simp only [Node.pn, Function.update_of_ne hne]
      exact ⟨h1, h2, h3⟩

/-- **Tracker provenance** in every reachable world. -/
theorem reachable_trackerInv {n : Nat} {w : World n} (hw : Reachable w) :
    TrackerInv w := by
  induction hw with
  | refl => exact ti_init n
  | tail _ hstep ih => exact ti_step ih hstep

#print axioms reachable_trackerInv

/-! ## Committed entries: certified commit terms, bounded stamps -/

/-- Every committed entry's commit term is certified (the committing leader
won it) and bounds its record stamp: `1 ≤ stamp ≤ T`. -/
def CommittedCert {n : Nat} (w : World n) : Prop :=
  ∀ p stamp T v : Nat, (p, stamp, T, v) ∈ w.committed →
    1 ≤ stamp ∧ stamp ≤ T ∧ ∃ ℓ : Fin n, Data.Cert w.project T ℓ

private theorem cc_init (n : Nat) : CommittedCert (World.init n) := by
  intro p stamp T v hc
  simp [World.init] at hc

private theorem cc_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : CommittedCert w) (hs : Step w w') : CommittedCert w' := by
  have hstep := hs
  cases hs
  case leaderAdvanceCommit i k hrole hbase hadv =>
    intro p stamp T v hc
    rcases List.mem_append.mp hc with hold | hnew
    · obtain ⟨h1, h2, ℓ, hcert⟩ := h _ _ _ _ hold
      exact ⟨h1, h2, ℓ, ccarry hstep hcert⟩
    · obtain ⟨p', hp', heq⟩ := List.mem_filterMap.mp hnew
      cases hcase : (w.nodes i).dn.hist p' with
      | none => rw [hcase] at heq; simp at heq
      | some tv =>
        rw [hcase] at heq
        simp only [Option.map_some, Option.some.injEq, Prod.mk.injEq] at heq
        obtain ⟨rfl, rfl, rfl, rfl⟩ := heq
        refine ⟨((Data.reachable_mapInv (reachable_project hw)).node i).hist_pos
            p' tv.1 tv.2 hcase,
          (Data.reachable_stamp (reachable_project hw)).hist_le i p' tv.1 tv.2
            hcase,
          i, ccarry hstep (Data.cert_of_leader
            (Uc2.reachable_inv (Data.reachable_project (reachable_project hw)))
            hrole)⟩
  all_goals
    intro p stamp T v hc
    obtain ⟨h1, h2, ℓ, hcert⟩ := h _ _ _ _ hc
    exact ⟨h1, h2, ℓ, ccarry hstep hcert⟩

/-- **Committed certification** in every reachable world. -/
theorem reachable_committed_cert {n : Nat} {w : World n} (hw : Reachable w) :
    CommittedCert w := by
  induction hw with
  | refl => exact cc_init n
  | tail hprev hstep ih => exact cc_step hprev ih hstep

#print axioms reachable_committed_cert

/-! ## The commit-event quorum extraction (slot-count → `Finset` quorum) -/

/-- A firing advance strictly raises `commit` (local copy of
`Uc2Proofs/Commit.lean`'s private `advance_some_lt`). -/
private theorem advance_commit_lt' (t : CommitTracker) (own k : Nat)
    (h : (t.advance own).2 = some k) : t.commit < k := by
  simp only [CommitTracker.advance] at h
  split at h
  · rename_i hlt
    simp only at h
    cases h
    exact hlt
  · simp at h

/-- Slot-index counting → member `Finset`: qualifying slots (value ≥ `k`)
convert to distinct non-leader members via `slotOf` injectivity. -/
private theorem slots_to_finset {n : Nat} (i : Fin n) (P : Fin n → Prop)
    (k : Nat) :
    ∀ rep : List Nat,
      (∀ s, s < rep.length → k ≤ rep.getD s 0 →
        ∃ src : Fin n, src ≠ i ∧ slotOf i src = s ∧ P src) →
      ∃ Q : Finset (Fin n),
        (rep.filter (fun v => k ≤ v)).length ≤ Q.card ∧ i ∉ Q ∧
        ∀ u ∈ Q, P u ∧ slotOf i u < rep.length := by
  intro rep
  induction rep using List.reverseRecOn with
  | nil => exact fun _ => ⟨∅, by simp, by simp, by simp⟩
  | append_singleton l a ih =>
    intro W
    obtain ⟨Q, hQlen, hiQ, hQP⟩ := ih fun s hs hks => by
      refine W s (by simp; omega) ?_
      rwa [List.getD_eq_getElem?_getD, List.getElem?_append_left hs,
        ← List.getD_eq_getElem?_getD]
    have hfl : ((l ++ [a]).filter (fun v => k ≤ v)).length
        = (l.filter (fun v => k ≤ v)).length
          + ([a].filter (fun v => k ≤ v)).length := by
      rw [List.filter_append, List.length_append]
    by_cases hka : k ≤ a
    · obtain ⟨src, hsrc, hslot, hP⟩ := W l.length (by simp) (by
        rw [List.getD_eq_getElem?_getD, List.getElem?_append_right
          (Nat.le_refl _)]
        simpa using hka)
      have hsQ : src ∉ Q := by
        intro hin
        have := (hQP src hin).2
        omega
      refine ⟨insert src Q, ?_, ?_, ?_⟩
      · rw [Finset.card_insert_of_notMem hsQ, hfl]
        have : ([a].filter (fun v => k ≤ v)).length ≤ 1 :=
          List.length_filter_le _ _
        omega
      · intro hin
        rcases Finset.mem_insert.mp hin with rfl | hin
        · exact hsrc rfl
        · exact hiQ hin
      · intro u hu
        rcases Finset.mem_insert.mp hu with rfl | hu
        · refine ⟨hP, ?_⟩
          rw [hslot]
          simp
        · refine ⟨(hQP u hu).1, ?_⟩
          have := (hQP u hu).2
          simp
          omega
    · refine ⟨Q, ?_, hiQ, ?_⟩
      · rw [hfl]
        have h0 : ([a].filter (fun v => k ≤ v)).length = 0 := by
          simp [hka]
        omega
      · intro u hu
        refine ⟨(hQP u hu).1, ?_⟩
        have := (hQP u hu).2
        simp
        omega

/-- **Commit-event quorum extraction.** A firing kernel `advance` at a
leader yields an explicit member quorum: the leader plus, per member, a
`CMsg.report` at the leader's current term whose durable reaches the
advanced position — the bridge from C1–C5's slot counting to C5's
`quorum_intersect` (LB2-rerun §3). -/
theorem advance_report_quorum {n : Nat} {w : World n} (hw : Reachable w)
    (i : Fin n) (k : Nat) (hrole : (w.nodes i).pn.role = .leader)
    (hadv : ((w.nodes i).tracker.advance (w.nodes i).pn.durable).2 = some k) :
    ∃ Q : Finset (Fin n), n / 2 + 1 ≤ Q.card ∧ i ∈ Q ∧
      ∀ u ∈ Q, u = i ∨ ∃ d, k ≤ d ∧
        CMsg.report u ((w.nodes i).pn.currentTerm) d ∈ w.csent := by
  obtain ⟨hq, hlen, hslots⟩ := reachable_trackerInv hw i
  have hadv' : (w.nodes i).tracker.advance (w.nodes i).pn.durable
      = (((w.nodes i).tracker.advance (w.nodes i).pn.durable).1, some k) := by
    rw [← hadv]
  have hcert := CommitTracker.advance_certified _ _ _ _ hadv'
  have hown : k ≤ (w.nodes i).pn.durable :=
    CommitTracker.advance_le_own _ _ _ _ hadv'
  have hkpos : 0 < k := by
    have := advance_commit_lt' _ _ _ hadv
    omega
  rw [hq, List.filter_cons, if_pos (by simpa using hown)] at hcert
  obtain ⟨Q, hQlen, hiQ, hQP⟩ := slots_to_finset i
    (fun src => ∃ d, k ≤ d ∧
      CMsg.report src ((w.nodes i).pn.currentTerm) d ∈ w.csent) k
    (w.nodes i).tracker.reported
    (fun s _ hks => by
      obtain ⟨src, d, hsrc, hslot, hle, hmem⟩ := hslots hrole s (by omega)
      exact ⟨src, hsrc, hslot, d, by omega, hmem⟩)
  refine ⟨insert i Q, ?_, Finset.mem_insert_self i Q, ?_⟩
  · rw [Finset.card_insert_of_notMem hiQ]
    simp only [List.length_cons] at hcert
    omega
  · intro u hu
    rcases Finset.mem_insert.mp hu with rfl | hu
    · exact .inl rfl
    · exact .inr (hQP u hu).1

#print axioms advance_report_quorum

/-! ## F-LC4-1 guard: BARE within-regime durable stability is FALSE

The LC4 review's countermodel, machine-checked (review-LC2-verdict.md,
F-LC4-1): `report u T d ∈ csent → dataTerm u = T → d ≤ durable u` — the
LC3-review L1 statement that became amendment 2 — is refuted by a
data-less later leader's map zero-cutting a lagged candidate. Node 4 leads
term 1 and replicates 3 bytes to node 0, which reports `(0, 1, 3)` and
then campaigns (ct 2, handle still 1, gate carried open). Node 2 — with
NO data — wins term 2 on the two other data-less voters and ships
`prunePush [] 2 0 = [(2,0)]`. Node 0 delivers it non-adopt:
`commonPrefixLen [(1,0)] [(2,0)] = 0`, the NoCommonPrefix gate misses
(`0 < 0` is false), and `reconcileClamped` zero-cuts via the pinned
`same_base_different_term_truncates_to_zero` arm — durable 0, map wiped,
`dataTerm` still 1 = T. Any true durable-floor must therefore be
commit/RepQuorum-conditioned (the quorum blocks the data-less winner via
`quorum_intersect` + `logOk`): see the LC4b design record. -/
theorem bare_report_durable_stability_is_false :
    ∃ w : World 5, Reachable w ∧
      CMsg.report 0 1 3 ∈ w.csent ∧
      (w.nodes 0).dataTerm = 1 ∧
      (w.nodes 0).pn.durable = 0 := by
  refine ⟨_,
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail
      (.single (.startElection _ 4 (by decide)))
      (.deliverRequestVote _ 0 4 1 0 0 (by decide) (by decide)))
      (.deliverRequestVote _ 1 4 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 4 0 1 (by decide) (by decide) (by decide)))
      (.deliverVote _ 4 1 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 4 (by decide) (by decide)))
      (.leaderAppend _ 4 10 (by decide)))
      (.leaderAppend _ 4 11 (by decide)))
      (.leaderAppend _ 4 12 (by decide)))
      (.shipTermMap _ 4 (by decide)))
      (.deliverTermMap _ 0 1 [(1, 0)] (by decide) (by decide)))
      (.deliverReplicate _ 0 0 1 1 10 (by decide) (by decide) (by decide)
        (by decide)))
      (.deliverReplicate _ 0 1 1 1 11 (by decide) (by decide) (by decide)
        (by decide)))
      (.deliverReplicate _ 0 2 1 1 12 (by decide) (by decide) (by decide)
        (by decide)))
      (.sendReport _ 0 (by decide) (by decide)))
      (.startElection _ 0 (by decide)))
      (.startElection _ 2 (by decide)))
      (.startElection _ 2 (by decide)))
      (.deliverRequestVote _ 1 2 2 0 0 (by decide) (by decide)))
      (.deliverRequestVote _ 3 2 2 0 0 (by decide) (by decide)))
      (.deliverVote _ 2 1 2 (by decide) (by decide) (by decide)))
      (.deliverVote _ 2 3 2 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 2 (by decide) (by decide)))
      (.shipTermMap _ 2 (by decide)))
      (.deliverTermMap _ 0 2 [(2, 0)] (by decide) (by decide)),
    by decide, by decide, by decide⟩

#print axioms bare_report_durable_stability_is_false

end Uc2.Cert
