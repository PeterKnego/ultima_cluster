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
  | absorbDurable i hrole =>
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
      -- Issue #7: `startElection` now advertises the ABSORBED copy, so the
      -- credential is bounded by the counter via `SmLeDurable` rather than by
      -- `rfl`. That inequality is the whole content of the split on this side.
      simp only [Node.pn, Function.update_self]
      exact ⟨Nat.le_refl _, Data.reachable_smLeDurable (reachable_project hw) k⟩
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
  | absorbDurable i hrole =>
    -- issue #7: role is UNCHANGED, so unlike crashRestart this node may still be
    -- a candidate — the hypothesis transfers instead of being refuted.
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

/-! ## `committed_term_at_leaders` — the statement, and the assembly it feeds

The LC endgame's shape (LB2b's four steps): a committed entry's position is
inside every later-or-equal-term leader's durable frontier, and that
leader's OWN term map attributes the position to the committed stamp. With
that, `frames_current_authored` pins the held stamp and `Uc2.Data`'s
`coherent` (log matching, at the frame occurrence `committed_frame_provenance`
produces) pins the payload. -/

/-- **`committed_term_at_leaders`**, predicate form (`T ≤` — the
strengthened relation the brief asks for; the `becomeLeader` case at
`u = T` is vacuous by `cert_blocks_candidate` against `RepQuorum`'s own
certificate, so `≤` costs nothing over `<`). -/
def CommittedTermAtLeaders {n : Nat} (w : World n) : Prop :=
  ∀ (p stamp T v : Nat), (p, stamp, T, v) ∈ w.committed →
    ∀ i : Fin n, (w.nodes i).pn.role = .leader →
      T ≤ (w.nodes i).pn.currentTerm →
      p < (w.nodes i).pn.durable ∧
        Uc2.TermMap.termAt (w.nodes i).dn.termMap p = stamp

/-- **The assembly.** Leader completeness follows from
`CommittedTermAtLeaders` and the landed stack alone:

1. `reachable_hist_defined` — `p < durable i` means `i` holds SOMETHING at `p`;
2. `frames_current_authored` — what it holds is stamped by its own map's
   attribution at `p`, which CTL pins to `stamp`;
3. `committed_frame_provenance` — the committed `(stamp, v)` is a real
   `replicate` frame on the data wire, i.e. an `Occ` at `(p, stamp)`;
4. `Data.DInv.coherent` (log matching) — two occurrences at the same
   `(position, stamp)` carry the same payload. -/
private theorem lc_of_ctl {n : Nat} {w : World n} (hw : Reachable w)
    (hctl : CommittedTermAtLeaders w)
    (p t T v : Nat) (hc : (p, t, T, v) ∈ w.committed)
    (i : Fin n) (hi : (w.nodes i).pn.role = .leader)
    (ht : T ≤ (w.nodes i).pn.currentTerm) :
    (w.nodes i).hist p = some (t, v) := by
  obtain ⟨hpd, hattr⟩ := hctl p t T v hc i hi ht
  obtain ⟨⟨t', v'⟩, hh⟩ := reachable_hist_defined hw i p hpd
  have hst : t' = t := ((frames_current_authored hw i p t' v' hh).symm.trans
    hattr)
  subst hst
  obtain ⟨hdr, hfr⟩ := committed_frame_provenance hw p t' T v hc
  have hv : v = v' :=
    (Data.reachable_dinv (reachable_project hw)).coherent p t' v v'
      (.inl ⟨hdr, hfr⟩) (.inr ⟨i, hh⟩)
  rw [hh, hv]

/-! ## Canon consumption: a shared below-`k` prefix forbids a cut below `k`

The single reconcile-side fact the (unmechanized) canon layer exists to
feed. Adjudicated fact: `reconcile` cuts at the first ENTRY mismatch
(`commonPrefixLen` compares whole entries), so `termAt`-level agreement
cannot control the cut position — but LITERAL agreement of the entries
whose bases sit below `k` does, and bounds the cut at or above `k`. This is
the consumption side of canon: it is stated and proved here independently
of how the shared prefix `P` is established (that is canon's own,
unmechanized induction). -/

/-- `commonPrefixLen` splits over a shared literal prefix. -/
private theorem cpl_append : ∀ (P a b : TermMap),
    Uc2.commonPrefixLen (P ++ a) (P ++ b) = P.length + Uc2.commonPrefixLen a b
  | [], a, b => by simp
  | e :: P, a, b => by
    have ih := cpl_append P a b
    simp [Uc2.commonPrefixLen]
    omega

/-- An index at or past a prefix's length reads out of the suffix. -/
private theorem mem_suffix_of_getElem {P a : TermMap} {i : Nat}
    {e : Nat × Nat} (hi : P.length ≤ i) (h : (P ++ a)[i]? = some e) :
    e ∈ a := by
  rw [List.getElem?_append_right hi] at h
  exact List.mem_of_getElem? h

/-- Lower bound for the clamped rebuild: `validUpTo` is a `min` of the
durable with the two slot-`cp` bases, so any common floor on all three is a
floor on the outcome. -/
private theorem clamped_ge {own leader : TermMap} {d k cp : Nat}
    {o : Uc2.Outcome}
    (h : Uc2.reconcile.reconcileClamped own d leader cp = .ok o)
    (hd : k ≤ d)
    (ho : ∀ e, own[cp]? = some e → k ≤ e.2)
    (hl : ∀ e, leader[cp]? = some e → k ≤ e.2) :
    k ≤ o.validUpTo := by
  obtain ⟨v, m⟩ := o
  dsimp only [Uc2.Outcome.validUpTo]
  rcases hoe : own[cp]? with _ | e <;> rcases hle : leader[cp]? with _ | f <;>
    simp only [Uc2.reconcile.reconcileClamped, hoe, hle,
      Uc2.ReconcileResult.ok.injEq, Uc2.Outcome.mk.injEq] at h <;>
    obtain ⟨hv, -⟩ := h
  · omega
  · have h2 := hl f hle
    simp only [Nat.min_def] at hv
    split_ifs at hv <;> omega
  · have h1 := ho e hoe
    simp only [Nat.min_def] at hv
    split_ifs at hv <;> omega
  · have h1 := ho e hoe
    have h2 := hl f hle
    simp only [Nat.min_def] at hv
    split_ifs at hv <;> omega

/-- **Canon consumption.** Two maps that share a literal (nonempty) prefix
`P`, with every entry BEYOND `P` on either side opening at or above `k`,
reconcile cleanly at any durable at-or-above `k` without cutting below `k`.

`hP` (nonempty shared prefix) is exactly what rules out the
`NoCommonPrefix` wipe arm; in the intended consumer `P` is the below-`k`
canonical prefix of the `T`-tenure, nonempty because `MapFloor` pins a
map's head base to `0 < k`. -/
theorem reconcile_ge_of_canon {P a b : TermMap} {d k : Nat}
    (hP : P ≠ [])
    (ha : ∀ e ∈ a, k ≤ e.2) (hb : ∀ e ∈ b, k ≤ e.2) (hd : k ≤ d) :
    ∃ o : Uc2.Outcome,
      Uc2.reconcile (P ++ a) d (P ++ b) = .ok o ∧ k ≤ o.validUpTo := by
  obtain ⟨l0, ls, hPeq⟩ : ∃ l0 ls, P = l0 :: ls := by
    cases P with
    | nil => exact absurd rfl hP
    | cons l0 ls => exact ⟨l0, ls, rfl⟩
  have hlen1 : 1 ≤ P.length := by rw [hPeq]; simp
  have hlen : P.length ≤ Uc2.commonPrefixLen (P ++ a) (P ++ b) := by
    rw [cpl_append]; omega
  have hcons : P ++ b = l0 :: (ls ++ b) := by rw [hPeq]; rfl
  have hclamped :
      Uc2.reconcile (P ++ a) d (P ++ b)
        = Uc2.reconcile.reconcileClamped (P ++ a) d (P ++ b)
            (Uc2.commonPrefixLen (P ++ a) (P ++ b)) := by
    conv_lhs => rw [hcons]
    rw [Uc2.reconcile_eq_clamped (P ++ a) d l0 (ls ++ b)
      (Or.inr (by rw [← hcons]; omega)), ← hcons]
  have hoa : ∀ e, (P ++ a)[Uc2.commonPrefixLen (P ++ a) (P ++ b)]? = some e →
      k ≤ e.2 := fun e he => ha e (mem_suffix_of_getElem hlen he)
  have hob : ∀ e, (P ++ b)[Uc2.commonPrefixLen (P ++ a) (P ++ b)]? = some e →
      k ≤ e.2 := fun e he => hb e (mem_suffix_of_getElem hlen he)
  rcases hres : Uc2.reconcile (P ++ a) d (P ++ b) with o | _
  · exact ⟨o, rfl, clamped_ge (hclamped.symm.trans hres) hd hoa hob⟩
  · exfalso
    rw [hclamped] at hres
    simp [Uc2.reconcile.reconcileClamped] at hres

#print axioms reconcile_ge_of_canon

/-! ## F-3 — the `preK` → `++` bridge

Canon's clauses are phrased with `preK m k` (the below-`k` initial segment);
`reconcile_ge_of_canon` consumes a LITERAL `P ++ a` decomposition with every
beyond-`P` entry at-or-above `k`. `takeWhile_append_dropWhile` supplies the
split definitionally, but the "beyond" half needs ascending bases — without
it `dropWhile` can retain a below-`k` entry. `MapsWF` supplies `Ascending`;
this packages the two so canon never re-derives the conversion. -/

/-- The below-`k` initial segment of a map. An initial segment (not a
filter) precisely because bases ascend. -/
def preK (m : TermMap) (k : Nat) : TermMap := m.takeWhile (fun e => e.2 < k)

private theorem asc_head_le {m : TermMap} {a : Nat × Nat}
    (h : TermMap.Ascending (a :: m)) : ∀ f ∈ m, a.2 ≤ f.2 := by
  intro f hf
  obtain ⟨j, hj⟩ := List.getElem?_of_mem hf
  exact TermMap.Ascending.head_base_le h hj

/-- **The bridge.** An ascending map splits as `preK m k ++ a` with every
entry of the tail `a` opening at or above `k` — exactly the `ha`/`hb` shape
`reconcile_ge_of_canon` demands. -/
theorem preK_split : ∀ {m : TermMap}, TermMap.Ascending m → ∀ k : Nat,
    ∃ a : TermMap, m = preK m k ++ a ∧ ∀ e ∈ a, k ≤ e.2 := by
  intro m
  induction m with
  | nil => intro _ k; exact ⟨[], rfl, by simp⟩
  | cons a t ih =>
    intro hwf k
    by_cases hak : a.2 < k
    · obtain ⟨s, hs, hge⟩ := ih hwf.tail k
      refine ⟨s, ?_, hge⟩
      show a :: t = List.takeWhile _ (a :: t) ++ s
      rw [List.takeWhile_cons_of_pos (by simpa using hak)]
      exact congrArg (a :: ·) hs
    · refine ⟨a :: t, ?_, ?_⟩
      · show a :: t = List.takeWhile _ (a :: t) ++ (a :: t)
        rw [List.takeWhile_cons_of_neg (by simpa using hak)]
        simp
      · intro e he
        rcases List.mem_cons.mp he with rfl | het
        · omega
        · exact Nat.le_trans (by omega) (asc_head_le hwf e het)

#print axioms preK_split

/-! ## F-2 — birth-site confinement for canon's antecedent

The LC4e review's F-2: canon keyed on `RepQuorum w T k` is monotone-FORWARD
(`RepQuorum.step` only transports `w → w'`), so at every step where a NEW
`(T, k)` pair enters the antecedent the induction hypothesis supplies
nothing and the whole bundle must be established ex nihilo. The review
located those births at "a report delivery, a vote delivery, or an append".

That over-counts. `RepQuorum`'s certificate conjunct is *recoverable* from
its base-frame conjunct: a `replicate bT T T v0` frame is an `Occ` at term
`T`, so `DInv.cert` mints a term-`T` certificate in ANY world holding that
frame, and `cert_uniq` identifies it with the one the post-state carries.
So the certificate can never be the last conjunct to complete, and the
election plane — where certificates are actually born (`deliverRequestVote`
completing a grant quorum, `becomeLeader` flipping `role` out of
`.candidate` so `Cert.pinned`'s right disjunct closes) — cannot birth
`RepQuorum` at all.

What remains: `RepQuorum` is antitone across any step that adds no
`replicate` frame and no `report` message. Reading that off the `Step`
constructors, canon's births are confined to exactly THREE sites —
`leaderAppend` and `serveTail` (the `replicate` conjunct) and `sendReport`
(the `k`-floored report quorum). The three hard map-surgery cases
(`deliverTermMap`, `deliverReplicate`, `becomeLeader`), plus
`leaderAdvanceCommit`, `shipTermMap` and the whole election plane, are
**birth-free**: their IH always reaches. -/

/-- **Birth-site confinement.** `RepQuorum` is antitone across any step that
adds no `replicate` frame and no `report` message. -/
theorem repquorum_anti {n : Nat} {w w' : World n} (hw : Reachable w)
    (hstep : Step w w')
    (hd : ∀ p hdr t v, Frame.replicate p hdr t v ∈ w'.dsent →
      Frame.replicate p hdr t v ∈ w.dsent)
    (hcs : ∀ (u : Fin n) (t d : Nat), CMsg.report u t d ∈ w'.csent →
      CMsg.report u t d ∈ w.csent)
    {T k : Nat} (h : RepQuorum w' T k) : RepQuorum w T k := by
  obtain ⟨hT, ℓ, Q, bT, v0, hcert, hbTk, horig, hQcard, hQ⟩ := h
  have horig' := hd _ _ _ _ horig
  obtain ⟨ℓ', hc'⟩ := (Data.reachable_dinv (reachable_project hw)).cert T
    (.inl ⟨bT, v0, .inl ⟨T, horig'⟩⟩)
  have heq : ℓ = ℓ' :=
    Data.cert_uniq (Uc2.reachable_inv (Data.reachable_project
      (reachable_project (hw.tail hstep)))) hcert
      (Data.cert_drtg (step_project hstep) hc')
  subst heq
  refine ⟨hT, ℓ, Q, bT, v0, hc', hbTk, horig', hQcard, ?_⟩
  intro u hu
  rcases hQ u hu with rfl | ⟨d, hkd, hm⟩
  · exact .inl rfl
  · exact .inr ⟨d, hkd, hcs _ _ _ hm⟩

/-- The form the birth-free constructors are instantiated at: both data
wires unchanged. Covers `becomeLeader`, `deliverTermMap`,
`deliverReplicate`, `leaderAdvanceCommit`, `deliverReport`, `crashRestart`
and the entire election plane definitionally. -/
theorem repquorum_anti_of_wires {n : Nat} {w w' : World n} (hw : Reachable w)
    (hstep : Step w w') (hd : w'.dsent = w.dsent) (hc : w'.csent = w.csent)
    {T k : Nat} (h : RepQuorum w' T k) : RepQuorum w T k :=
  repquorum_anti hw hstep (fun _ _ _ _ hf => hd ▸ hf)
    (fun _ _ _ hm => hc ▸ hm) h

#print axioms repquorum_anti

/-! ## The `becomeLeader` crux, mechanized against an explicit hypothesis
bundle

`committed_term_at_leaders` is NOT landed (see the task report). What IS
landed here is its crux — the `becomeLeader` case — proved in full against
`CruxInputs`, the three facts the still-unmechanized canon layer has to
supply. Everything else in the crux (the quorum routing, the four-way case
split, the F3 reporter-is-candidate slot, and the `T < clt` reduction) is
discharged from the landed stack.

**Item 1 of the LC4e brief, answered and machine-checked: the `EntryIdentity`
shortcut is NOT needed.** LC4d's design record routed the `clt > T` arm
through an entry-identity step plus a term-descent recursion. Neither is
required: `cand_frontier` (below) shows a quorate candidate's CURRENT map
frontier already dominates the `lastTerm` it advertised at campaign time, so
`T < clt` puts the candidate itself strictly past `T` in the SAME pre-state,
where canon's past-`T` floor applies directly. -/

/-- A quorate candidate's CURRENT map frontier and durable dominate the
credentials it advertised at campaign time — `cand_cred` restated on the map
frontier via the derived-credential sync. -/
theorem cand_frontier {n : Nat} {w : World n} (hw : Reachable w)
    (i : Fin n) (u clt cd : Nat) (hm : Msg.requestVote i u clt cd ∈ w.sent)
    (hrl : (w.nodes i).pn.role = .candidate)
    (hct : (w.nodes i).pn.currentTerm = u)
    (hquorum : n / 2 + 1 ≤ (w.nodes i).pn.votesReceived.card) :
    clt ≤ Data.lastTermOf (w.nodes i).dn.termMap ∧
      cd ≤ (w.nodes i).pn.durable := by
  obtain ⟨h1, h2⟩ := cand_cred hw i u clt cd hm hrl hct hquorum
  exact ⟨(reachable_lastTerm_sync hw i) ▸ h1, h2⟩

#print axioms cand_frontier

/-- The facts the `becomeLeader` crux consumes that the canon layer still
owes; none is landed.

**F-1 — READ THE CONDITIONING BEFORE TARGETING THESE.** The three floor
clauses are **FALSE for arbitrary `(T, k)`**, and the target
`∀ w T k, Reachable w → CruxInputs w T k` is therefore unprovable:

- `past_floor` fails for any `k` above some node's durable, and for a node
  whose frontier is past `T` but which has not yet replicated through `k`;
- `writer_floor` fails for a sharper, already-adjudicated reason —
  **certificates exist without wins**, so `Cert w T ℓ` alone implies nothing
  about `ℓ` ever having led at `T`, appended, or reached `durable ≥ k`.

The conditioning that makes them provable is `RepQuorum w T k`, which is
therefore carried as a FIELD (`rq`) rather than left to the consumption
site: `k` is meaningful only as a position a term-`T` quorum actually
reached, above the tenure's own base frame. The discharge obligation is
consequently `∀ w T k, Reachable w → RepQuorum w T k → <the three floors>`,
and `crux_become_leader` reads `RepQuorum` back out of the bundle.

`Era w T` is deliberately NOT a field: it stays an explicit hypothesis of
`crux_become_leader`, because canon's era-free reporter clause ((P6) in the
LC4e design record) is expected to retire it, and baking it in here would
entrench a conditioning the eventual `committed_term_at_leaders` cannot
supply. -/
structure CruxInputs {n : Nat} (w : World n) (T k : Nat) : Prop where
  /-- The conditioning. Every clause below is false without it. -/
  rq : RepQuorum w T k
  /-- Canon's past-`T` floor: a map frontier strictly past `T` implies the
  node is durable through `k` (no above-`T` byte lives below `k`). -/
  past_floor : ∀ j : Fin n, T < Data.lastTermOf (w.nodes j).dn.termMap →
      k ≤ (w.nodes j).pn.durable
  /-- The `T`-tenure writer's own floor (it committed through `k`, and canon
  forbids any later reconcile from cutting it back below `k`). -/
  writer_floor : ∀ ℓ : Fin n, Data.Cert w.project T ℓ →
      k ≤ (w.nodes ℓ).pn.durable
  /-- B2′: the writer-grant analog of `reachable_grant_report`. The writer
  never reports (`sendReport` is follower-only), so its quorum slot needs its
  own credential-comparison invariant — same shape as B2's good arm, with the
  writer's own `k`-floor in place of a report's. -/
  writer_grant : ∀ (ℓ c : Fin n) (u : Nat), Data.Cert w.project T ℓ →
      ℓ ≠ c → T < u → Msg.vote ℓ c u true ∈ w.sent →
      ∃ clt cd, Msg.requestVote c u clt cd ∈ w.sent ∧
        (T < clt ∨ (clt = T ∧ k ≤ cd))

/-- **The `becomeLeader` crux.** A quorate candidate at a term above `T`
is already durable through `k`: its tally meets the commit quorum in some
member `y`, and every way `y` can sit in that intersection forces the
candidate's own frontier past `k`.

- `y = c` and `y` is the writer — `writer_floor` directly;
- `y = c` and `y` reported — **F3**: `reachable_report_era_floor` (B1)
  directly, no grant event needed (a self-vote emits no message);
- `y ≠ c` and `y` reported — `reachable_grant_report` (B2), whose escape arm
  `Era` kills, instantiated at `RepQuorum`'s own `bT < k ≤ d` base witness;
- `y ≠ c` and `y` is the writer — `writer_grant` (B2′).

The last two both land on the same dichotomy, closed by `cand_frontier`:
`clt = T` gives `k ≤ cd ≤ durable c` outright, and `T < clt` puts `c` itself
past `T`, where `past_floor` applies.

Scope honesty: this is the DURABLE-FLOOR half of the crux.
`CommittedTermAtLeaders` additionally carries the attribution half
(`termAt (termMap c) p = stamp`), which reads off canon's prefix agreement
directly and is not part of this statement. -/
theorem crux_become_leader {n : Nat} {w : World n} (hw : Reachable w)
    {T k : Nat} (hera : Era w T)
    (hin : CruxInputs w T k)
    (c : Fin n) (hrole : (w.nodes c).pn.role = .candidate)
    (hquorum : n / 2 + 1 ≤ (w.nodes c).pn.votesReceived.card)
    (hT : T < (w.nodes c).pn.currentTerm) :
    k ≤ (w.nodes c).pn.durable := by
  obtain ⟨h1T, ℓ, Q, bT, v0, hcert, hbTk, horig, hQcard, hQ⟩ := hin.rq
  obtain ⟨y, hy⟩ := Uc2.quorum_intersect n (w.nodes c).pn.votesReceived Q
    hquorum hQcard
  rw [Finset.mem_inter] at hy
  -- the shared dichotomy both grant routes land on
  have hclose : ∀ clt cd : Nat,
      Msg.requestVote c ((w.nodes c).pn.currentTerm) clt cd ∈ w.sent →
      (T < clt ∨ (clt = T ∧ k ≤ cd)) → k ≤ (w.nodes c).pn.durable := by
    intro clt cd hrv harm
    obtain ⟨hfr, hdur⟩ := cand_frontier hw c ((w.nodes c).pn.currentTerm)
      clt cd hrv hrole rfl hquorum
    rcases harm with hlt | ⟨rfl, hkcd⟩
    · exact hin.past_floor c (Nat.lt_of_lt_of_le hlt hfr)
    · omega
  rcases eq_or_ne y c with rfl | hyc
  · -- the self-vote slot: no grant message exists, so route on `Q` alone
    rcases hQ y hy.2 with rfl | ⟨d, hkd, hrp⟩
    · exact hin.writer_floor y hcert
    · exact le_trans hkd (reachable_report_era_floor hw y T d hrp h1T hera).1
  · -- a real grant from `y`
    have hvs := (Uc2.reachable_inv
      (Data.reachable_project (reachable_project hw))).votes_sound c
      (by rw [show (w.project.project.nodes c).role = Role.candidate from hrole]
          decide) y hy.1
    have hvote : Msg.vote y c ((w.nodes c).pn.currentTerm) true ∈ w.sent := by
      rcases hvs with rfl | hv
      · exact absurd rfl hyc
      · exact hv
    rcases hQ y hy.2 with rfl | ⟨d, hkd, hrp⟩
    · obtain ⟨clt, cd, hrv, harm⟩ :=
        hin.writer_grant y c ((w.nodes c).pn.currentTerm) hcert hyc hT hvote
      exact hclose clt cd hrv harm
    · obtain ⟨clt, cd, hrv, harm⟩ := reachable_grant_report hw y c
        ((w.nodes c).pn.currentTerm) T d hyc hvote hrp hT h1T
      rcases harm with ⟨u'', es, hu, hg⟩ | hgood
      · exact absurd (hera u'' es hg) (by omega)
      · exact hclose clt cd hrv
          (by rcases hgood bT v0 horig (by omega) with h | ⟨h1, h2⟩
              · exact .inl h
              · exact .inr ⟨h1, by omega⟩)

#print axioms crux_become_leader

/-! ## F-6 — reports are stamped with `currentTerm`, not `dataTerm`

`sendReport` (`ProtocolCommit.lean` L516–525) emits
`report j (w.nodes j).pn.currentTerm (w.nodes j).pn.durable` — the datagram's
`leadership_term_id`, i.e. the node-level term handle, NOT the data plane's
`dataTerm`. Combined with `currentTerm` monotonicity this **totally orders**
every `T`-report before every above-`T` grant by the same node: the report
was emitted while `y.currentTerm = T`, and `y` can only grant at `u > T`
after adopting `u`, which is strictly later.

This is the pivot of canon's three birth base cases, so it lands as its own
machine-checked lemma rather than a docstring (the LC4f report's own
recommendation, and this arc's rule that prose is not evidence). -/

/-- `recvRequestVote` never lowers the term: it either adopts strictly
upward or leaves `currentTerm` alone, and every grant/reject arm returns a
node differing only in `votedFor`. -/
private theorem recvRequestVote_currentTerm_le {n : Nat} (s : PNode n)
    (c : Fin n) (nt clt cd : Nat) :
    s.currentTerm ≤ (s.recvRequestVote c nt clt cd).1.currentTerm := by
  unfold PNode.recvRequestVote
  by_cases h : s.currentTerm < nt
  · -- adopted strictly upward: `votedFor` is cleared, so the fresh-grant arm
    simp only [h, if_true, PNode.adoptTerm]
    unfold PNode.recvRequestVote.grantIfFresh
    split <;> simp <;> omega
  · -- no adoption: every arm returns `s` up to `votedFor`
    simp only [h, if_false]
    split
    · split
      · split <;> simp
      · unfold PNode.recvRequestVote.grantIfFresh
        split <;> simp
    · unfold PNode.recvRequestVote.grantIfFresh
      split <;> simp

/-- `applyGossip` adopts a strictly-higher gossip term and otherwise leaves
`currentTerm` alone — on both the `.ok` and the `.noCommonPrefix` arm. -/
private theorem applyGossip_currentTerm_le {n : Nat} (d : Data.Node n)
    (t : Nat) (entries : TermMap) :
    d.pn.currentTerm ≤ (d.applyGossip t entries).pn.currentTerm := by
  unfold Data.Node.applyGossip
  split <;>
  · dsimp only
    split <;> simp [PNode.adoptTerm] <;> omega

/-- **`currentTerm` is monotone across every step.** The term handle rises at
`startElection` (self-bump), at `deliverRequestVote`/`deliverTermMap` (adopt
upward) and at `deliverVoteHigherTerm` (strict adoption); the remaining
eleven constructors leave it fixed. -/
theorem step_currentTerm_mono {n : Nat} {w w' : World n} (hs : Step w w')
    (j : Fin n) :
    (w.nodes j).pn.currentTerm ≤ (w'.nodes j).pn.currentTerm := by
  cases hs with
  | rejectStaleRequestVote i c nt clt cd hmsg hstale => exact le_refl _
  | serveTail i p t v hrole hhist hp => exact le_refl _
  | shipTermMap i hrole => exact le_refl _
  | sendReport i hrole hgate => exact le_refl _
  | startElection i hrole =>
    rcases eq_or_ne j i with rfl | hne
    · simp [Node.pn, Function.update_self]
    · simp [Node.pn, Function.update_of_ne hne]
  | deliverRequestVote i c nt clt cd hmsg hterm =>
    rcases eq_or_ne j i with rfl | hne
    · simpa [Node.pn, Function.update_self] using
        recvRequestVote_currentTerm_le (w.nodes j).pn c nt clt cd
    · simp [Node.pn, Function.update_of_ne hne]
  | deliverVote i v t hmsg hrole hterm =>
    rcases eq_or_ne j i with rfl | hne
    · simp [Node.pn, Function.update_self]
    · simp [Node.pn, Function.update_of_ne hne]
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.pn] at hterm ⊢
      simp [Function.update_self, PNode.adoptTerm]
      omega
    · simp [Node.pn, Function.update_of_ne hne]
  | becomeLeader i hrole hquorum =>
    rcases eq_or_ne j i with rfl | hne
    · simp [Node.pn, Function.update_self]
    · simp [Node.pn, Function.update_of_ne hne]
  | absorbDurable i hrole =>
    rcases eq_or_ne j i with rfl | hne
    · simp [Node.pn, Function.update_self]
    · simp [Node.pn, Function.update_of_ne hne]
  | crashRestart i =>
    rcases eq_or_ne j i with rfl | hne
    · simp [Node.pn, Function.update_self]
    · simp [Node.pn, Function.update_of_ne hne]
  | leaderAppend i v hrole =>
    rcases eq_or_ne j i with rfl | hne
    · simp [Node.pn, Function.update_self]
    · simp [Node.pn, Function.update_of_ne hne]
  | deliverReplicate i pos hdr t v hmsg hpos hhdr hgate =>
    rcases eq_or_ne j i with rfl | hne
    · simp [Node.pn, Function.update_self, Data.Node.recvReplicate]
    · simp [Node.pn, Function.update_of_ne hne]
  | deliverTermMap i t entries hmsg hterm =>
    rcases eq_or_ne j i with rfl | hne
    · simpa [Node.pn, Function.update_self] using
        applyGossip_currentTerm_le (w.nodes j).dn t entries
    · simp [Node.pn, Function.update_of_ne hne]
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    rcases eq_or_ne j i with rfl | hne
    · simp [Node.pn, Function.update_self]
    · simp [Node.pn, Function.update_of_ne hne]
  | leaderAdvanceCommit i k hrole hbase hadv =>
    rcases eq_or_ne j i with rfl | hne
    · simp [Node.pn, Function.update_self]
    · simp [Node.pn, Function.update_of_ne hne]

#print axioms step_currentTerm_mono

/-- **F-6, the stamping fact.** `sendReport` is the ONLY constructor that
appends to `csent`, and it stamps the datagram with the emitter's
`pn.currentTerm` and `pn.durable` — not with `dataTerm`. So a report that is
new at a step pins its emitter's term handle, durable, role and gate in the
PRE-state of that step. -/
theorem step_report_new {n : Nat} {w w' : World n} (hs : Step w w')
    {y : Fin n} {T d : Nat}
    (hnew : CMsg.report y T d ∈ w'.csent)
    (hold : CMsg.report y T d ∉ w.csent) :
    T = (w.nodes y).pn.currentTerm ∧ d = (w.nodes y).pn.durable ∧
      (w.nodes y).pn.role = .follower ∧ (w.nodes y).reconciled = true := by
  cases hs with
  | sendReport i hrole hgate =>
    rcases List.mem_append.mp hnew with h | h
    · exact absurd h hold
    · rw [List.mem_singleton] at h
      injection h with h1 h2 h3
      subst h1; subst h2; subst h3
      exact ⟨rfl, rfl, hrole, hgate⟩
  | _ => exact absurd hnew hold

#print axioms step_report_new

/-- A report never outruns its emitter's term handle. -/
def ReportStamp {n : Nat} (w : World n) : Prop :=
  ∀ (y : Fin n) (T d : Nat), CMsg.report y T d ∈ w.csent →
    T ≤ (w.nodes y).pn.currentTerm

private theorem rs_step {n : Nat} {w w' : World n} (h : ReportStamp w)
    (hs : Step w w') : ReportStamp w' := by
  intro y T d hm
  by_cases hold : CMsg.report y T d ∈ w.csent
  · exact le_trans (h y T d hold) (step_currentTerm_mono hs y)
  · rw [(step_report_new hs hm hold).1]
    exact step_currentTerm_mono hs y

/-- **F-6.** Every `T`-report on the commit wire was stamped at its emitter's
own `currentTerm`, which since then can only have risen. -/
theorem reachable_report_stamp {n : Nat} {w : World n} (hw : Reachable w) :
    ReportStamp w := by
  induction hw with
  | refl => intro y T d hm; simp [World.init] at hm
  | tail _ hstep ih => exact rs_step ih hstep

#print axioms reachable_report_stamp

/-- **F-6's ordering corollary — the birth-case pivot.** At the step that
emits a `T`-report, the emitter's term handle is exactly `T`, so it sits
strictly below every `u > T`. Since `currentTerm` is monotone and a node can
only grant a vote at `u` once its handle has reached `u`, **every `T`-report
by `y` strictly precedes every above-`T` grant by `y`** — the interleaving
(report-after-grant) that would have been an acked-write-loss in the #6b
family is foreclosed structurally, by the stamping discipline alone. -/
theorem report_before_grant {n : Nat} {w w' : World n} (hs : Step w w')
    {y : Fin n} {T d u : Nat}
    (hnew : CMsg.report y T d ∈ w'.csent)
    (hold : CMsg.report y T d ∉ w.csent) (hu : T < u) :
    (w.nodes y).pn.currentTerm < u := by
  rw [← (step_report_new hs hnew hold).1]; exact hu

#print axioms report_before_grant

/-! ## The `past_floor` / (P0) layering probe — DECIDED, with a witness

The LC4f report left one structural question open: `CruxInputs.past_floor`
"may not belong to canon at all — it is close to CTL's own conclusion and
may be better carried as CTL's induction hypothesis". Settled here by
binder analysis plus a machine-checked witness, not by prose.

**`past_floor` STAYS IN CANON.** The two statements are keyed on different
binders, and no implication bridges them:

- `CommittedTermAtLeaders` concludes about node `i` only under
  `(w.nodes i).pn.role = .leader`;
- `past_floor` quantifies over EVERY node `j` with
  `T < lastTermOf (termMap j)`, most of which are followers.

For CTL to supply `past_floor` one would need
`T < lastTermOf (termMap j) → (w.nodes j).pn.role = .leader`. That is false,
and `crashRestart` is the witness: it sets `role := .follower` while leaving
`dn.termMap` **untouched**, so any node whose frontier is past `T` can be a
follower in the very next world. `past_floor` is therefore exactly the
durable half of canon's (P1), and CTL's leader-keyed conclusion cannot reach
its `j`.

**(P0) is OUT of canon** — for the dual reason, and it was never in: (P0)
(`termAt C p' = stamp'` for term-`T` committed entries below `k`) is
literally the attribution conjunct of `CommittedTermAtLeaders`' own
conclusion, restricted to the canonical prefix. It also mentions
`w.committed`, and keeping the commit plane out of canon's statement is
precisely why LC4f rejected re-keying canon on `w.committed`. (P0) is CTL's
obligation and stays with LC4i, as the LC4e report already scoped it. -/

/-- **The `past_floor` probe witness.** `crashRestart` demotes a node to
follower while preserving its term map verbatim — so a map frontier past `T`
carries NO role information, and a leader-keyed invariant (CTL) can never
discharge an all-nodes frontier-keyed floor (`past_floor`). -/
theorem crashRestart_demotes_keeping_map {n : Nat} (w : World n) (i : Fin n) :
    ∃ w' : World n, Step w w' ∧
      (w'.nodes i).dn.termMap = (w.nodes i).dn.termMap ∧
      (w'.nodes i).pn.role = .follower :=
  ⟨_, Step.crashRestart w i, by simp [Function.update_self],
    by simp [Node.pn, Function.update_self]⟩

#print axioms crashRestart_demotes_keeping_map

/-! ## Canon — the canonical below-`k` prefix bundle

Pairwise form: the LC4f design pass deleted the existential canonical map
`C` in favour of "any two `k`-covering maps agree below `k`", which is what
every consumer (`reconcile_ge_of_canon`, the crux) actually needs and which
removes a witness that would otherwise be threaded through all 15
constructors.

**READ THE CONDITIONING (F-1 applies here exactly as it does to
`CruxInputs`).** `Canon w T k` is **FALSE for arbitrary `(T, k)`** — take `k`
above every node's durable and `past_floor`/`rep_floor` fail immediately. The
discharge obligation is
`∀ w T k, Reachable w → RepQuorum w T k → Canon w T k`, never
`∀ w T k, Reachable w → Canon w T k`. `RepQuorum` is what makes `k`
meaningful: a position a term-`T` quorum actually reached, above the tenure's
own base frame. Canon deliberately keeps that antecedent and is NOT
era-conditioned (an `Era` hypothesis would be unavailable to CTL) and NOT
joint with a wins statement (`repquorum_anti` confines births to three
constructors, which retires the joint-induction contingency).

Statement audit against this arc's adjudicated facts:

- `agree` is **literal** `preK` equality, not `termAt` agreement — the shape
  `reconcile_ge_of_canon` consumes, and the shape the "reconcile cuts at the
  first ENTRY mismatch" fact demands.
- `past_floor`/`rep_floor` are **not** bare durable stability
  (`bare_report_durable_stability_is_false`): both are `k`-conditioned, and
  `rep_floor` is B1 with `Era` traded for the `k ≤ d` conditioning — the
  direction the guard theorem permits.
- `past_floor` is canon's, not CTL's: see the layering probe above. -/

/-- The maps canon forces into agreement below `k`: a past-`T` node's map, a
`T`-regime node's map once it is durable through `k`, a `k`-floored `T`
reporter's map, and any above-`T` gossip's entries. -/
def Canonical {n : Nat} (w : World n) (T k : Nat) (m : TermMap) : Prop :=
  (∃ j : Fin n, m = (w.nodes j).dn.termMap ∧
      (T < Data.lastTermOf m ∨
        (Data.lastTermOf m = T ∧ k ≤ (w.nodes j).pn.durable) ∨
        (∃ d, k ≤ d ∧ CMsg.report j T d ∈ w.csent)))
  ∨ (∃ u : Nat, T < u ∧ Frame.gossip u m ∈ w.dsent)

/-- **Canon.** Under `RepQuorum w T k` (see the conditioning note above). -/
structure Canon {n : Nat} (w : World n) (T k : Nat) : Prop where
  /-- (P1)/(P3)/(P5)/(P6), prefix half, PAIRWISE — no existential witness. -/
  agree : ∀ m₁ m₂ : TermMap, Canonical w T k m₁ → Canonical w T k m₂ →
      preK m₁ k = preK m₂ k
  /-- (P1) durable half — this is `CruxInputs.past_floor`. -/
  past_floor : ∀ j : Fin n, T < Data.lastTermOf (w.nodes j).dn.termMap →
      k ≤ (w.nodes j).pn.durable
  /-- (P2) a past-`T` node's above-`T` entries all open at or above `k`. -/
  above : ∀ j : Fin n, T < Data.lastTermOf (w.nodes j).dn.termMap →
      ∀ e ∈ (w.nodes j).dn.termMap, T < e.1 → k ≤ e.2
  /-- (P3) above-half: an above-`T` gossip's above-`T` entries open at or
  above `k`. -/
  gossip_above : ∀ (u : Nat) (es : TermMap), Frame.gossip u es ∈ w.dsent →
      T < u → ∀ e ∈ es, T < e.1 → k ≤ e.2
  /-- (P4) the data wire: no above-`T` byte lives below `k`. -/
  wire : ∀ pos hdr t v : Nat, Frame.replicate pos hdr t v ∈ w.dsent →
      T < t → k ≤ pos
  /-- (P6) durable half — B1 without its `Era` condition. -/
  rep_floor : ∀ (y : Fin n) (d : Nat), CMsg.report y T d ∈ w.csent →
      k ≤ d → k ≤ (w.nodes y).pn.durable

/-- **Canon's statement matches its consumer, machine-checked.** Canon's
`agree` clause plus `preK_split` is exactly what `reconcile_ge_of_canon`
consumes: two `k`-covering maps reconcile CLEANLY (never `.noCommonPrefix`)
and never cut `validUpTo` below `k`. This is the interface obligation the
LC4f brief pins — "canon's statement must be exactly what this consumes" —
discharged as a theorem rather than asserted in prose.

`Ascending` comes from `MapsWF` at every call site; `preK m₁ k ≠ []` is the
`NoCommonPrefix`-wipe exclusion (`MapFloor` pins a nonempty map's head base
below `k` at the consumer). -/
theorem canon_reconcile_clean {n : Nat} {w : World n} {T k : Nat}
    (hc : Canon w T k) {m₁ m₂ : TermMap}
    (h1 : Canonical w T k m₁) (h2 : Canonical w T k m₂)
    (hw1 : TermMap.Ascending m₁) (hw2 : TermMap.Ascending m₂)
    (hne : preK m₁ k ≠ []) {d : Nat} (hd : k ≤ d) :
    ∃ o, Uc2.reconcile m₁ d m₂ = .ok o ∧ k ≤ o.validUpTo := by
  obtain ⟨a, ha, hae⟩ := preK_split hw1 k
  obtain ⟨b, hb, hbe⟩ := preK_split hw2 k
  have hP : preK m₁ k = preK m₂ k := hc.agree m₁ m₂ h1 h2
  have hne2 : preK m₂ k ≠ [] := hP ▸ hne
  have h1' : m₁ = preK m₂ k ++ a := by rw [← hP]; exact ha
  have hrw : Uc2.reconcile m₁ d m₂
      = Uc2.reconcile (preK m₂ k ++ a) d (preK m₂ k ++ b) := by
    rw [← h1', ← hb]
  rw [hrw]
  exact reconcile_ge_of_canon hne2 hae hbe hd

#print axioms canon_reconcile_clean

/-- **The conditioning is load-bearing, machine-checked.** `Canon` is FALSE
in a reachable world at `(T, k) = (1, 3)`: F-LC4-1's landed countermodel has
`report 0 1 3` on the commit wire and `(w.nodes 0).pn.durable = 0`, which
refutes `rep_floor` outright. So the F-1 note above is not a stylistic
caution — `∀ w T k, Reachable w → Canon w T k` is refutable, and every
discharge of canon must carry `RepQuorum w T k`.

In that world `RepQuorum w 1 3` indeed fails (node 0 is the only reporter, so
any `Q` is contained in `{ℓ, 0}` and cannot reach `5/2+1 = 3`) — that is the
quorum fact which blocks the data-less term-2 winner, exactly as the
countermodel's own docstring prescribes. That containment is read off the
trace by hand and is NOT machine-checked here; the refutation of bare `Canon`
below is. -/
theorem bare_canon_is_false :
    ∃ w : World 5, Reachable w ∧ ¬ Canon w 1 3 := by
  obtain ⟨w, hw, hm, _, hdur⟩ := bare_report_durable_stability_is_false
  refine ⟨w, hw, fun hc => ?_⟩
  have h := hc.rep_floor 0 3 hm (le_refl 3)
  omega

#print axioms bare_canon_is_false

/-- `Canonical` is read off term maps, durables and the two wires only. -/
private theorem canonical_transport {n : Nat} {w w' : World n} {T k : Nat}
    (hmap : ∀ j : Fin n, (w'.nodes j).dn.termMap = (w.nodes j).dn.termMap)
    (hdur : ∀ j : Fin n, (w'.nodes j).pn.durable = (w.nodes j).pn.durable)
    (hds : w'.dsent = w.dsent) (hcs : w'.csent = w.csent)
    {m : TermMap} (h : Canonical w' T k m) : Canonical w T k m := by
  rcases h with ⟨j, hm, hcase⟩ | ⟨u, hu, hg⟩
  · refine .inl ⟨j, hm.trans (hmap j), ?_⟩
    rcases hcase with h1 | ⟨h1, h2⟩ | ⟨d, hd, hr⟩
    · exact .inl h1
    · exact .inr (.inl ⟨h1, by rw [← hdur j]; exact h2⟩)
    · exact .inr (.inr ⟨d, hd, by rw [← hcs]; exact hr⟩)
  · exact .inr ⟨u, hu, by rw [← hds]; exact hg⟩

/-- **Canon transports across every step that touches no term map, no
durable and neither data wire.** Instantiated definitionally by the eight
birth-free, canon-inert constructors: `startElection`,
`deliverRequestVote`, `rejectStaleRequestVote`, `deliverVote`,
`deliverVoteHigherTerm`, `crashRestart`, `deliverReport` and
`leaderAdvanceCommit` (all of which move only election state, the tracker or
the commit watermark). Paired with `repquorum_anti_of_wires` — which pulls
the antecedent back so the IH applies — these eight constructors close. -/
theorem canon_transport {n : Nat} {w w' : World n} {T k : Nat}
    (h : Canon w T k)
    (hmap : ∀ j : Fin n, (w'.nodes j).dn.termMap = (w.nodes j).dn.termMap)
    (hdur : ∀ j : Fin n, (w'.nodes j).pn.durable = (w.nodes j).pn.durable)
    (hds : w'.dsent = w.dsent) (hcs : w'.csent = w.csent) :
    Canon w' T k where
  agree m₁ m₂ h1 h2 :=
    h.agree m₁ m₂ (canonical_transport hmap hdur hds hcs h1)
      (canonical_transport hmap hdur hds hcs h2)
  past_floor j hj := by
    rw [hdur j]; exact h.past_floor j (by rwa [hmap j] at hj)
  above j hj e he ht := by
    rw [hmap j] at hj he; exact h.above j hj e he ht
  gossip_above u es hg hu e he ht :=
    h.gossip_above u es (by rwa [hds] at hg) hu e he ht
  wire pos hdr t v hf ht := h.wire pos hdr t v (by rwa [hds] at hf) ht
  rep_floor y d hr hkd := by
    rw [hdur y]; exact h.rep_floor y d (by rwa [hcs] at hr) hkd

#print axioms canon_transport

/-! ## Finding #11 — canon's birth base cases are NOT payable from F-6

**Canon's 15-constructor induction is NOT landed** (task LC4g stopped at its
ceiling per the stuck protocol; never sorried, never weakened). What blocks
it is a gap in the LC4f design record's base-case argument, found by the
mandatory bare-vs-conditioned audit. Canon is **not** suspected false — this
is a scope/design finding, not a Finding #10 (nothing here implicates
`reconcile`'s entry-equality semantics against the Rust).

LC4f argued the three birth base cases are payable because F-6's stamping
discipline means "every member of the `k`-floored quorum `Q` was already
durable through `k` **at the moment it granted** to any later winner". F-6 as
mechanized above (`report_before_grant`) delivers the **ordering** half of
that sentence — a `T`-report by `y` strictly precedes every above-`T` grant
by `y` — and that half is real and useful: it rules out the newly-reporting
node `j` itself sitting in a later winner's quorum, since `j.currentTerm = T`
at the emission step.

It does **not** deliver the **stability** half. "Was durable through `k` at
report time" does not give "is durable through `k` at grant time", and that
implication is exactly what this arc has already adjudicated as FALSE:
`bare_report_durable_stability_is_false` (`LcClosure.lean`) is a landed
25-step countermodel in which node 0 reports durable 3 and is later
zero-cut to durable 0 by a data-less winner's gossip. The base-case argument
silently assumes the refuted statement.

Concretely, at a `sendReport` birth of `(T, k)` with an above-`T` gossip
already on the wire (a reachable configuration — a live-but-stale `T` writer
is not forbidden), the obligation is canon's own (P3) for that gossip. The
available route is B2 (`reachable_grant_report`) on the quorum-intersection
member, but B2's **escape arm is precisely "an above-`T` gossip exists"**,
which is the case hypothesis — so B2 returns nothing. `crux_become_leader`
escapes this only because it assumes `Era w T`, and canon deliberately drops
`Era` (CTL cannot supply it). The base case therefore carries the genuine
cross-term Raft content: it needs induction on the winner's term, or canon
made joint with a wins statement — i.e. the LC4e review's F-2 concern and its
+2–3 joint-induction contingency, which `repquorum_anti` confined but did not
retire. Birth confinement is correct and load-bearing; it is just not
sufficient. -/

end Uc2.Cert
