import Uc2Proofs.StageB

/-! LC4d — Stage C: canon + `committed_term_at_leaders` + the assembly of
`leader_completeness`.

**Status honesty (task LC4d).** This file lands the two standalone,
consumer-side conveniences the LC4d brief scopes as item 2 — `cert_uniq`
and `cand_cred` (mechanized as the reachable-inductive `CandCredRaw` +
its `hquorum`-discharged corollary `cand_cred`) — plus the standalone
supports `CampaignTermBound`/`reachable_campaign_term_bound` those needed.
All four are standalone (no canon dependence, verified case-by-case).

The `no_branch`/`canon` mutual bundle (brief items 1, 3, 4) is NOT landed —
STOPPED at the ceiling per the stuck protocol, exactly the difficulty class
the brief flagged in advance. `committed_term_at_leaders` and
`leader_completeness` remain absent (never sorried, never weakened). The
full design record — including a refinement of the LC4c handoff's own
analysis (the `clt = T` "good arm" of the becomeLeader crux closes
CLEANLY with the now-landed stack; the `clt > T`/escape "reversed-wins"
arm is where canon is unavoidable, and WHY, precisely) — is in
`.superpowers/sdd/task-LC4d-report.md`.

## `cert_uniq`

At most one writer certifies a given term: two `Cert w t ℓ₁`/`Cert w t ℓ₂`
facts force `ℓ₁ = ℓ₂`. Mechanical mirror of `cert_blocks_candidate`'s own
proof shape (`quorum_intersect` finds a shared voter; `grant_uniq`/
`noForeign` chase the four `u = ℓᵢ ∨ grant` cases to `ℓ₁ = ℓ₂`).

## `cand_cred`

The becomeLeader crux needs the WINNING candidate's post-win
`(lastTerm, durable)` to match what it advertised in its own `requestVote`
message at campaign time (`startElection`'s `(lastTerm, durable)` at that
instant) — NOT because the values are pinned syntactically (a reconcile
CAN shrink a live candidate's map while its gate stays open — this is
exactly the B1 "carried-open lagged candidate" state, and F-LC4-1's
countermodel realizes precisely this shrink), but because at the ONE
moment `cand_cred` is consumed (`becomeLeader`, where `hquorum` — an
`n/2+1`-sized `votesReceived` — ALREADY holds), the standard
`cert_blocks_candidate` vote-counting argument makes any DAMAGING same-term
delivery vacuous: a gossip at the candidate's own campaign term `u`
requires a pre-existing certified writer at `u` (`DInv.cert`), and — by
`cert_blocks_candidate` applied AT THAT DELIVERY, using the ALREADY-HELD
`hquorum` — a certified writer at `u` and a QUORATE candidate at `u`
cannot coexist (`quorum_intersect` + `grant_uniq`/`vote_unique_per_term`:
two `n/2+1` grant-sets over `Fin n` cannot be disjoint). Any HIGHER-term
delivery adopts (loses candidacy, vacuous for a `role = candidate`
conclusion). So `cand_cred`, STATED WITH `hquorum` baked into its own
hypothesis (the exact shape the crux consumes it at), is a standalone
reachable-inductive invariant with no canon dependence — the "quorate
candidate can't be truncated" fact is a genuinely different (and cheaper)
mechanism than the sub-`k` canonical-prefix agreement `no_branch`/`canon`
need for the NON-quorate, cross-node cases. -/

namespace Uc2.Data

/-- **`cert_uniq`.** At most one writer certifies a term. -/
theorem cert_uniq {n : Nat} {w : World n} (hpInv : Uc2.Inv w.project)
    {t : Nat} {ℓ1 ℓ2 : Fin n} (hc1 : Cert w t ℓ1) (hc2 : Cert w t ℓ2) :
    ℓ1 = ℓ2 := by
  obtain ⟨Q1, hQ1c, hQ1⟩ := hc1.quorum
  obtain ⟨Q2, hQ2c, hQ2⟩ := hc2.quorum
  obtain ⟨u, hu⟩ := quorum_intersect n Q1 Q2 hQ1c hQ2c
  rw [Finset.mem_inter] at hu
  rcases hQ1 u hu.1 with rfl | hg1 <;> rcases hQ2 u hu.2 with rfl | hg2
  · rfl
  · exact (hc1.noForeign ℓ2 hg2).symm
  · exact hc2.noForeign ℓ1 hg1
  · exact hpInv.grant_uniq u ℓ1 ℓ2 t hg1 hg2

#print axioms cert_uniq

end Uc2.Data

namespace Uc2.Cert

open Uc2.Data (Frame)

/-! ## `cand_cred` toolkit: a `requestVote` message never outruns its
sender's (monotone) current term -/

/-- A recorded `requestVote i u _ _` message's term never exceeds `i`'s
CURRENT `currentTerm` — `currentTerm` only increases, and the message was
minted exactly when `currentTerm` first reached `u` (`startElection`, the
only emission site). Building block for `cand_cred`'s `startElection` case
(a node cannot campaign twice for the term it is ABOUT to bump to). -/
def CampaignTermBound {n : Nat} (w : World n) : Prop :=
  ∀ (i : Fin n) (u clt cd : Nat), Msg.requestVote i u clt cd ∈ w.sent →
    u ≤ (w.nodes i).pn.currentTerm

private theorem ctb_init (n : Nat) : CampaignTermBound (World.init n) := by
  intro i u clt cd hm
  simp [World.init] at hm

private theorem ctb_step {n : Nat} {w w' : World n} (h : CampaignTermBound w)
    (hs : Step w w') : CampaignTermBound w' := by
  cases hs with
  | startElection i hrole =>
    intro k u clt cd hm
    simp only [List.mem_append, List.mem_singleton] at hm
    rcases hm with hm | hm
    · rcases eq_or_ne k i with rfl | hne
      · simpa [Node.pn, Function.update_self] using
          Nat.le_succ_of_le (h k u clt cd hm)
      · simpa [Node.pn, Function.update_of_ne hne] using h k u clt cd hm
    · simp only [Msg.requestVote.injEq] at hm
      obtain ⟨rfl, rfl, -, -⟩ := hm
      simp [Node.pn, Function.update_self]
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    intro k u clt' cd' hm
    simp only [List.mem_append, List.mem_singleton] at hm
    rcases hm with hm | hm
    · rcases eq_or_ne k j with rfl | hne
      · simp only [Node.pn, Function.update_self]
        have h1 := h k u clt' cd' hm
        have h2 := Data.recv_term (w.nodes k).dn.pn c nt clt cd hterm
        rw [h2]
        omega
      · simpa [Node.pn, Function.update_of_ne hne] using h k u clt' cd' hm
    · simp at hm
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    intro k u clt' cd' hm
    simp only [List.mem_append, List.mem_singleton] at hm
    rcases hm with hm | hm
    · exact h k u clt' cd' hm
    · simp at hm
  | deliverVote i v t hmsg hrole hterm =>
    intro k u clt cd hm
    rcases eq_or_ne k i with rfl | hne
    · simpa [Node.pn, Function.update_self] using h k u clt cd hm
    · simpa [Node.pn, Function.update_of_ne hne] using h k u clt cd hm
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    intro k u clt cd hm
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self, PNode.adoptTerm]
      exact Nat.le_of_lt (Nat.lt_of_le_of_lt (h k u clt cd hm) hterm)
    · simpa [Node.pn, Function.update_of_ne hne] using h k u clt cd hm
  | becomeLeader i hrole hquorum =>
    intro k u clt cd hm
    rcases eq_or_ne k i with rfl | hne
    · simpa [Node.pn, Function.update_self] using h k u clt cd hm
    · simpa [Node.pn, Function.update_of_ne hne] using h k u clt cd hm
  | crashRestart i =>
    intro k u clt cd hm
    rcases eq_or_ne k i with rfl | hne
    · simpa [Node.pn, Function.update_self] using h k u clt cd hm
    · simpa [Node.pn, Function.update_of_ne hne] using h k u clt cd hm
  | leaderAppend i v hrole =>
    intro k u clt cd hm
    rcases eq_or_ne k i with rfl | hne
    · simpa [Node.pn, Function.update_self] using h k u clt cd hm
    · simpa [Node.pn, Function.update_of_ne hne] using h k u clt cd hm
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate =>
    intro k u clt cd hm
    rcases eq_or_ne k j with rfl | hne
    · simpa [Node.pn, Function.update_self, Uc2.Data.Node.recvReplicate] using
        h k u clt cd hm
    · simpa [Node.pn, Function.update_of_ne hne] using h k u clt cd hm
  | serveTail i p t v hrole hhist hp => exact h
  | shipTermMap i hrole => exact h
  | deliverTermMap j t entries hmsg hterm =>
    intro k u clt cd hm
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.pn, Function.update_self]
      have h1 := h k u clt cd hm
      by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
      · rw [(Data.applyGossip_adopt _ entries hadopt).2]; omega
      · rw [(Data.applyGossip_no_adopt _ entries hadopt).2.1]; omega
    · simpa [Node.pn, Function.update_of_ne hne] using h k u clt cd hm
  | sendReport j hrole hgate => exact h
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    intro k u clt cd hm
    rcases eq_or_ne k i with rfl | hne
    · simpa [Node.pn, Function.update_self] using h k u clt cd hm
    · simpa [Node.pn, Function.update_of_ne hne] using h k u clt cd hm
  | leaderAdvanceCommit i kk hrole hbase hadv =>
    intro k u clt cd hm
    rcases eq_or_ne k i with rfl | hne
    · simpa [Node.pn, Function.update_self] using h k u clt cd hm
    · simpa [Node.pn, Function.update_of_ne hne] using h k u clt cd hm

/-- **`CampaignTermBound`** in every reachable world. -/
theorem reachable_campaign_term_bound {n : Nat} {w : World n}
    (hw : Reachable w) : CampaignTermBound w := by
  induction hw with
  | refl => exact ctb_init n
  | tail _ hstep ih => exact ctb_step ih hstep

#print axioms reachable_campaign_term_bound

/-! ## `cand_cred`, raw (unconditioned) form

Either a live candidate's `(lastTerm, durable)` still dominate what it
advertised at campaign time (growth-only: `deliverReplicate` can only grow
`durable`, and grows `lastTerm` only via a no-op-safe `observeTerm` at or
below the still-lagging `dataTerm`), OR its own campaign term is ALREADY
certified by a writer elsewhere — the escape the `deliverTermMap` case
takes instead of tracking the reconcile's exact outcome (a same-term gossip
forces `∃ℓ, Cert w u ℓ` via `DInv.cert` REGARDLESS of what the reconcile
does to `lastTerm`/`durable`). The disjunction is discharged at
consumption (`cand_cred` below) using `cert_blocks_candidate` against the
`hquorum` the crux always has in hand. -/
def CandCredRaw {n : Nat} (w : World n) : Prop :=
  ∀ (i : Fin n) (u clt cd : Nat), Msg.requestVote i u clt cd ∈ w.sent →
    (w.nodes i).pn.role = .candidate → (w.nodes i).pn.currentTerm = u →
    (clt ≤ (w.nodes i).pn.lastTerm ∧ cd ≤ (w.nodes i).pn.durable) ∨
      ∃ ℓ : Fin n, Data.Cert w.project u ℓ

private theorem ccr_init (n : Nat) : CandCredRaw (World.init n) := by
  intro i u clt cd hm
  simp [World.init] at hm

/-- `recvRequestVote` never touches `lastTerm` (local copy of `StageB.lean`'s
private `recv_lastTerm`, unconditional — both the adopt and grant-record
paths only ever override `currentTerm`/`role`/`votedFor`/`votesReceived`
via `with`-updates). -/
private theorem ccr_recv_lastTerm {n : Nat} (s : PNode n) (c : Fin n)
    (nt lt d : Nat) : ((s.recvRequestVote c nt lt d).1).lastTerm = s.lastTerm := by
  by_cases hadopt : s.currentTerm < nt
  · simp only [PNode.recvRequestVote, if_pos hadopt, PNode.adoptTerm,
      PNode.recvRequestVote.grantIfFresh]
    split_ifs <;> rfl
  · rcases hvf : s.votedFor with _ | ⟨vt, vid⟩ <;>
      simp only [PNode.recvRequestVote, if_neg hadopt, hvf,
        PNode.recvRequestVote.grantIfFresh] <;>
      split_ifs <;> rfl

private theorem ccr_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : CandCredRaw w) (hs : Step w w') : CandCredRaw w' := by
  have hcert : ∀ {t : Nat} {ℓ : Fin n}, Data.Cert w.project t ℓ →
      Data.Cert w'.project t ℓ := fun hc => Data.cert_drtg (step_project hs) hc
  cases hs with
  | startElection i hrole =>
    intro k u clt cd hm hrl hct
    simp only [List.mem_append, List.mem_singleton] at hm
    rcases hm with hm | hm
    · rcases eq_or_ne k i with rfl | hne
      · exfalso
        have hct' : (w.nodes k).pn.currentTerm + 1 = u := by
          simpa [Node.pn, Function.update_self] using hct
        have hb := reachable_campaign_term_bound hw k u clt cd hm
        omega
      · simp only [Node.pn, Function.update_of_ne hne] at hrl hct
        rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
        · left
          simpa [Node.pn, Function.update_of_ne hne] using hleft
        · exact .inr ⟨ℓ, hcert hcL⟩
    · simp only [Msg.requestVote.injEq] at hm
      obtain ⟨rfl, rfl, rfl, rfl⟩ := hm
      left
      simp [Node.pn, Function.update_self]
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    intro k u clt' cd' hm hrl hct
    simp only [List.mem_append, List.mem_singleton] at hm
    rcases hm with hm | hm
    · rcases eq_or_ne k j with rfl | hne
      · by_cases hadopt : (w.nodes k).dn.pn.currentTerm < nt
        · exfalso
          simp only [Node.pn, Function.update_self] at hrl
          rw [Data.recv_adopt_role _ _ _ _ _ hadopt] at hrl
          exact absurd hrl (by decide)
        · simp only [Node.pn, Function.update_self] at hrl hct ⊢
          rw [(Data.recv_frame _ _ _ _ _ hadopt).1] at hrl
          rw [(Data.recv_frame _ _ _ _ _ hadopt).2] at hct
          rcases h k u clt' cd' hm hrl hct with hleft | ⟨ℓ, hcL⟩
          · left
            rw [Data.recv_durable,
              ccr_recv_lastTerm (w.nodes k).dn.pn c nt clt cd]
            exact hleft
          · exact .inr ⟨ℓ, hcert hcL⟩
      · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
        rcases h k u clt' cd' hm hrl hct with hleft | ⟨ℓ, hcL⟩
        · exact .inl hleft
        · exact .inr ⟨ℓ, hcert hcL⟩
    · simp at hm
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    intro k u clt' cd' hm hrl hct
    simp only [List.mem_append, List.mem_singleton] at hm
    rcases hm with hm | hm
    · rcases h k u clt' cd' hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · exact .inl hleft
      · exact .inr ⟨ℓ, hcert hcL⟩
    · simp at hm
  | deliverVote i v t hmsg hrole hterm =>
    intro k u clt cd hm hrl hct
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hrl hct ⊢
      rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · exact .inl hleft
      · exact .inr ⟨ℓ, hcert hcL⟩
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · exact .inl hleft
      · exact .inr ⟨ℓ, hcert hcL⟩
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    intro k u clt cd hm hrl hct
    rcases eq_or_ne k i with rfl | hne
    · exfalso
      simp only [Node.pn, Function.update_self, PNode.adoptTerm] at hrl
      exact absurd hrl (by decide)
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · exact .inl hleft
      · exact .inr ⟨ℓ, hcert hcL⟩
  | becomeLeader i hrole hquorum =>
    intro k u clt cd hm hrl hct
    rcases eq_or_ne k i with rfl | hne
    · exfalso
      simp only [Node.pn, Function.update_self] at hrl
      exact absurd hrl (by decide)
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · exact .inl hleft
      · exact .inr ⟨ℓ, hcert hcL⟩
  | crashRestart i =>
    intro k u clt cd hm hrl hct
    rcases eq_or_ne k i with rfl | hne
    · exfalso
      simp only [Node.pn, Function.update_self] at hrl
      exact absurd hrl (by decide)
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · exact .inl hleft
      · exact .inr ⟨ℓ, hcert hcL⟩
  | leaderAppend i v hrole =>
    intro k u clt cd hm hrl hct
    rcases eq_or_ne k i with rfl | hne
    · exfalso
      simp only [Node.pn, Function.update_self] at hrl
      exact absurd (hrole.symm.trans hrl) (by decide)
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · exact .inl hleft
      · exact .inr ⟨ℓ, hcert hcL⟩
  | deliverReplicate j pos hdr t v hmsg hpos hhdr hgate =>
    intro k u clt cd hm hrl hct
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.pn, Function.update_self,
        Uc2.Data.Node.recvReplicate] at hrl hct ⊢
      rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · left
        obtain ⟨hgL, hgD⟩ := hleft
        have hgrow : (w.nodes k).dn.pn.lastTerm ≤
            Uc2.Data.lastTermOf (Uc2.Data.observeTerm (w.nodes k).dn.termMap t pos) := by
          have hsync : (w.nodes k).dn.pn.lastTerm =
              Uc2.Data.lastTermOf (w.nodes k).dn.termMap :=
            reachable_lastTerm_sync hw k
          by_cases hg2 : Uc2.Data.lastTermOf (w.nodes k).dn.termMap < t
          · rw [show Uc2.Data.observeTerm (w.nodes k).dn.termMap t pos
                = (w.nodes k).dn.termMap ++ [(t, pos)] by
              simp [Uc2.Data.observeTerm, hg2]]
            rw [Uc2.Data.lastTermOf_getLast
              (Uc2.Data.getLast?_append_singleton _ _)]
            omega
          · rw [Uc2.Data.observeTerm_of_le (Nat.not_lt.mp hg2) pos, ← hsync]
        exact ⟨le_trans hgL hgrow, by omega⟩
      · exact .inr ⟨ℓ, hcert hcL⟩
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · exact .inl hleft
      · exact .inr ⟨ℓ, hcert hcL⟩
  | serveTail i p t v hrole hhist hp =>
    intro k u clt cd hm hrl hct
    rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
    · exact .inl hleft
    · exact .inr ⟨ℓ, hcert hcL⟩
  | shipTermMap i hrole =>
    intro k u clt cd hm hrl hct
    rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
    · exact .inl hleft
    · exact .inr ⟨ℓ, hcert hcL⟩
  | sendReport j hrole hgate =>
    intro k u clt cd hm hrl hct
    rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
    · exact .inl hleft
    · exact .inr ⟨ℓ, hcert hcL⟩
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    intro k u clt cd hm hrl hct
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hrl hct ⊢
      rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · exact .inl hleft
      · exact .inr ⟨ℓ, hcert hcL⟩
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · exact .inl hleft
      · exact .inr ⟨ℓ, hcert hcL⟩
  | leaderAdvanceCommit i kk hrole hbase hadv =>
    intro k u clt cd hm hrl hct
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.pn, Function.update_self] at hrl hct ⊢
      rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · exact .inl hleft
      · exact .inr ⟨ℓ, hcert hcL⟩
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · exact .inl hleft
      · exact .inr ⟨ℓ, hcert hcL⟩
  | deliverTermMap j t entries hmsg hterm =>
    intro k u clt cd hm hrl hct
    rcases eq_or_ne k j with rfl | hne
    · by_cases hadopt : (w.nodes k).dn.pn.currentTerm < t
      · exfalso
        simp only [Node.pn, Function.update_self] at hrl
        rw [(Data.applyGossip_adopt _ entries hadopt).1] at hrl
        exact absurd hrl (by decide)
      · right
        have hterm' : (w.nodes k).dn.pn.currentTerm ≤ t := hterm
        have hct' : (w.nodes k).dn.pn.currentTerm = u := by
          simpa [Node.pn, Function.update_self,
            (Data.applyGossip_no_adopt (w.nodes k).dn entries hadopt).2.1]
            using hct
        have htu : t = u := by omega
        have hoc : (∃ p v, Data.Occ w.project p t v) ∨
            (∃ es, Frame.gossip t es ∈ w.dsent) := .inr ⟨entries, hmsg⟩
        obtain ⟨ℓ, hcL⟩ := (Data.reachable_dinv (reachable_project hw)).cert t hoc
        rw [htu] at hcL
        exact ⟨ℓ, hcert hcL⟩
    · simp only [Node.pn, Function.update_of_ne hne] at hrl hct ⊢
      rcases h k u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
      · exact .inl hleft
      · exact .inr ⟨ℓ, hcert hcL⟩

/-- **`cand_cred`, raw form**, in every reachable world. -/
theorem reachable_cand_cred_raw {n : Nat} {w : World n} (hw : Reachable w) :
    CandCredRaw w := by
  induction hw with
  | refl => exact ccr_init n
  | tail hprev hstep ih => exact ccr_step hprev ih hstep

#print axioms reachable_cand_cred_raw

/-- **`cand_cred`.** The becomeLeader-crux shape: a QUORATE candidate's
`(lastTerm, durable)` dominate what it advertised in its own `requestVote`
at campaign time. The escape disjunct of `CandCredRaw` is killed by
`cert_blocks_candidate` against the CURRENT `hquorum` — a certified writer
at the candidate's own term and a quorate candidate at that same term
cannot coexist. -/
theorem cand_cred {n : Nat} {w : World n} (hw : Reachable w)
    (i : Fin n) (u clt cd : Nat) (hm : Msg.requestVote i u clt cd ∈ w.sent)
    (hrl : (w.nodes i).pn.role = .candidate)
    (hct : (w.nodes i).pn.currentTerm = u)
    (hquorum : n / 2 + 1 ≤ (w.nodes i).pn.votesReceived.card) :
    clt ≤ (w.nodes i).pn.lastTerm ∧ cd ≤ (w.nodes i).pn.durable := by
  rcases reachable_cand_cred_raw hw i u clt cd hm hrl hct with hleft | ⟨ℓ, hcL⟩
  · exact hleft
  · exact (Data.cert_blocks_candidate
      (Uc2.reachable_inv (Data.reachable_project (reachable_project hw)))
      hrl hct hquorum hcL).elim

#print axioms cand_cred

end Uc2.Cert
