import Uc2Proofs.Protocol

/-! S2 — the inductive invariant and ELECTION SAFETY.

Split out of `Protocol.lean` (per the S2 brief's size guidance): `Protocol.lean`
stays the pristine S1 model, this file owns the proof. The invariant `Inv` is
a five-clause bundle (see the structure docstrings); the load-bearing pair is

- `grant_state` — a granted vote in flight is still recorded in the voter's
  `votedFor`, unless the voter has strictly moved past that term (in which
  case it WAS recorded when granted, and the term tag keeps it honest); and
- `grant_uniq` — the per-term uniqueness carrier: two granted votes from the
  same voter at the same term name the same candidate. `grant_uniq` is itself
  inductive *because* `grant_state` bridges it across a new grant: a fresh
  grant at term `t` forces the voter's `currentTerm = t` (the
  `deliverRequestVote` enabling condition rules out `t < currentTerm`), and
  then the vote discipline's idempotency forces the same candidate as any
  prior term-`t` grant.

The endgame (`election_safety`): two leaders at the same term each carry a
quorum of grants (`leader_quorum` + `votes_sound`), `quorum_intersect` yields
a shared voter, and the shared voter's grants collapse the leaders — either
directly (`grant_uniq`), or because one leader granted the other at its own
leadership term, contradicting its self-vote (`self_vote` + `grant_state`). -/

namespace Uc2

/-! ## `recvRequestVote` characterization

Five small lemmas that let every `deliverRequestVote` preservation case avoid
re-unfolding the vote discipline. All are stated under the constructor's
enabling condition `currentTerm ≤ newTerm` (or its adopt/no-adopt refinement). -/

/-- After receiving a request-vote at `newTerm ≥ currentTerm`, the receiver's
term is exactly `newTerm` (adoption raises it; otherwise they were equal). -/
private theorem recv_term {n : Nat} (s : PNode n) (c : Fin n) (nt lt d : Nat)
    (hle : s.currentTerm ≤ nt) :
    ((s.recvRequestVote c nt lt d).1).currentTerm = nt := by
  by_cases hadopt : s.currentTerm < nt
  · simp only [PNode.recvRequestVote, if_pos hadopt, PNode.adoptTerm,
      PNode.recvRequestVote.grantIfFresh]
    split_ifs <;> rfl
  · have heq : s.currentTerm = nt := by omega
    rcases hvf : s.votedFor with _ | ⟨vt, vid⟩ <;>
      simp only [PNode.recvRequestVote, if_neg hadopt, hvf,
        PNode.recvRequestVote.grantIfFresh] <;>
      split_ifs <;> simp [heq]

/-- A granted receive records the vote: `votedFor = some (newTerm, c)`. -/
private theorem recv_granted {n : Nat} (s : PNode n) (c : Fin n) (nt lt d : Nat)
    (hle : s.currentTerm ≤ nt)
    (hg : (s.recvRequestVote c nt lt d).2 = true) :
    ((s.recvRequestVote c nt lt d).1).votedFor = some (nt, c) := by
  by_cases hadopt : s.currentTerm < nt
  · simp only [PNode.recvRequestVote, if_pos hadopt, PNode.adoptTerm,
      PNode.recvRequestVote.grantIfFresh] at hg ⊢
    split_ifs at hg ⊢
    simp_all
  · have heq : s.currentTerm = nt := by omega
    rcases hvf : s.votedFor with _ | ⟨vt, vid⟩ <;>
      simp only [PNode.recvRequestVote, if_neg hadopt, hvf,
        PNode.recvRequestVote.grantIfFresh] at hg ⊢ <;>
      split_ifs at hg ⊢ <;> simp_all

/-- The already-voted-this-term receive: if the voter's recorded vote is AT
its current term (which equals `newTerm` — no adoption), the state is
untouched, and a grant can only be the idempotent re-grant to the same
candidate. This is the bridge that makes `grant_uniq` inductive. -/
private theorem recv_voted_current {n : Nat} (s : PNode n) (c x : Fin n)
    (nt lt d : Nat) (heq : s.currentTerm = nt)
    (hvf : s.votedFor = some (s.currentTerm, x)) :
    (s.recvRequestVote c nt lt d).1 = s ∧
      ((s.recvRequestVote c nt lt d).2 = true → x = c) := by
  have hnadopt : ¬ s.currentTerm < nt := by omega
  by_cases hx : x = c <;>
    simp [PNode.recvRequestVote, if_neg hnadopt, hvf, hx]

/-- Without adoption, a receive frames `role`, `currentTerm`, and
`votesReceived` (only `votedFor` may change, by a fresh grant). -/
private theorem recv_frame {n : Nat} (s : PNode n) (c : Fin n) (nt lt d : Nat)
    (hnadopt : ¬ s.currentTerm < nt) :
    ((s.recvRequestVote c nt lt d).1).role = s.role ∧
    ((s.recvRequestVote c nt lt d).1).currentTerm = s.currentTerm ∧
    ((s.recvRequestVote c nt lt d).1).votesReceived = s.votesReceived := by
  rcases hvf : s.votedFor with _ | ⟨vt, vid⟩ <;>
    simp only [PNode.recvRequestVote, if_neg hnadopt, hvf,
      PNode.recvRequestVote.grantIfFresh] <;>
    split_ifs <;> simp

/-- Adoption (a strictly higher `newTerm`) drops the receiver to follower. -/
private theorem recv_adopt_role {n : Nat} (s : PNode n) (c : Fin n)
    (nt lt d : Nat) (hadopt : s.currentTerm < nt) :
    ((s.recvRequestVote c nt lt d).1).role = .follower := by
  simp only [PNode.recvRequestVote, if_pos hadopt, PNode.adoptTerm,
    PNode.recvRequestVote.grantIfFresh]
  split_ifs <;> rfl

/-! ## The inductive invariant -/

/-- The election-safety invariant. Five clauses; `grant_state`/`grant_uniq`
are message-indexed and term-tagged (S1's note: grants must be tied to the
term IN the message, never to current state, so crash-restart + re-election
can't shake them off). -/
structure Inv {n : Nat} (w : World n) : Prop where
  /-- A granted `vote v c t` in flight is still recorded (`votedFor =
  some (t, c)` at `currentTerm = t`), unless the voter has strictly moved
  past `t`. The `currentTerm = t` conjunct is load-bearing: it rules out a
  recorded vote AHEAD of the voter's term, which is what lets a fresh grant
  at `newTerm` know that any prior `newTerm`-grant pinned `currentTerm`. -/
  grant_state : ∀ (v c : Fin n) (t : Nat), Msg.vote v c t true ∈ w.sent →
      t < (w.nodes v).currentTerm ∨
        ((w.nodes v).currentTerm = t ∧ (w.nodes v).votedFor = some (t, c))
  /-- Per-term uniqueness: one voter, one term, one candidate — across the
  whole (append-only, hence duplication/reorder-closed) sent set. -/
  grant_uniq : ∀ (v c₁ c₂ : Fin n) (t : Nat),
      Msg.vote v c₁ t true ∈ w.sent → Msg.vote v c₂ t true ∈ w.sent → c₁ = c₂
  /-- A candidate or leader has its own self-vote recorded at its current
  term (`start_election` records it; nothing erases it while the role
  survives, because every term change demotes to follower). -/
  self_vote : ∀ i : Fin n, (w.nodes i).role ≠ .follower →
      (w.nodes i).votedFor = some ((w.nodes i).currentTerm, i)
  /-- Every counted vote of a candidate/leader is witnessed by a granted
  vote message at its current term (or is the self-vote). -/
  votes_sound : ∀ i : Fin n, (w.nodes i).role ≠ .follower →
      ∀ v ∈ (w.nodes i).votesReceived,
        v = i ∨ Msg.vote v i ((w.nodes i).currentTerm) true ∈ w.sent
  /-- A leader's tally is a quorum (checked at `becomeLeader`, and the tally
  never shrinks while leadership survives). -/
  leader_quorum : ∀ i : Fin n, (w.nodes i).role = .leader →
      n / 2 + 1 ≤ (w.nodes i).votesReceived.card

/-- The invariant holds at boot: nothing sent, everyone a follower. -/
theorem inv_init (n : Nat) : Inv (World.init n) where
  grant_state := by intro v c t hmem; simp [World.init] at hmem
  grant_uniq := by intro v c₁ c₂ t h1 _; simp [World.init] at h1
  self_vote := by intro i hrole; simp [World.init] at hrole
  votes_sound := by intro i hrole; simp [World.init] at hrole
  leader_quorum := by intro i hrole; simp [World.init] at hrole

/-- **Inductive step**: every `Step` constructor preserves `Inv`. -/
theorem inv_step {n : Nat} {w w' : World n} (h : Inv w) (hs : Step w w') :
    Inv w' := by
  cases hs with
  | startElection i hrole =>
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · -- grant_state: no new grant (a requestVote was appended); the
      -- candidate's term bump absorbs its old grants into `t < currentTerm`.
      intro v c t hmem
      simp only [List.mem_append, List.mem_singleton] at hmem
      rcases hmem with hmem | hmem
      · rcases eq_or_ne v i with rfl | hv
        · simp only [Function.update_self]
          left
          show t < (w.nodes v).currentTerm + 1
          rcases h.grant_state v c t hmem with hlt | ⟨heq, _⟩ <;> omega
        · simp only [Function.update_of_ne hv]
          exact h.grant_state v c t hmem
      · exact absurd hmem (by simp)
    · intro v c₁ c₂ t h1 h2
      simp only [List.mem_append, List.mem_singleton] at h1 h2
      rcases h1 with h1 | h1
      · rcases h2 with h2 | h2
        · exact h.grant_uniq v c₁ c₂ t h1 h2
        · exact absurd h2 (by simp)
      · exact absurd h1 (by simp)
    · -- self_vote: the new candidate records its own vote at the new term.
      intro v hrole'
      rcases eq_or_ne v i with rfl | hv
      · simp only [Function.update_self]
      · simp only [Function.update_of_ne hv] at hrole' ⊢
        exact h.self_vote v hrole'
    · -- votes_sound: the fresh tally is exactly the self-vote.
      intro v hrole' u hu
      rcases eq_or_ne v i with rfl | hv
      · simp only [Function.update_self] at hu
        have hu' : u ∈ ({v} : Finset (Fin n)) := hu
        exact Or.inl (Finset.mem_singleton.mp hu')
      · simp only [Function.update_of_ne hv] at hrole' hu ⊢
        exact (h.votes_sound v hrole' u hu).imp id (List.mem_append_left _)
    · intro v hL
      rcases eq_or_ne v i with rfl | hv
      · simp only [Function.update_self] at hL
        exact absurd hL (by simp)
      · simp only [Function.update_of_ne hv] at hL ⊢
        exact h.leader_quorum v hL
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · -- grant_state: the delicate case. New grant → recorded at `nt`
      -- (recv_granted). Old grants of the receiver: adoption pushes
      -- `currentTerm` strictly past them (left disjunct absorbs it);
      -- without adoption a recorded current-term vote freezes the state
      -- (recv_voted_current).
      intro v c' t hmem
      simp only [List.mem_append, List.mem_singleton] at hmem
      rcases hmem with hmem | hmem
      · rcases eq_or_ne v j with rfl | hv
        · simp only [Function.update_self]
          rcases h.grant_state v c' t hmem with hlt | ⟨heq, hvf⟩
          · left
            rw [recv_term _ _ _ _ _ hterm]
            omega
          · by_cases hadopt : (w.nodes v).currentTerm < nt
            · left
              rw [recv_term _ _ _ _ _ hterm]
              omega
            · have heqn : (w.nodes v).currentTerm = nt := by omega
              have hst := (recv_voted_current (w.nodes v) c c' nt clt cd heqn
                (by rw [heq]; exact hvf)).1
              rw [hst]
              exact Or.inr ⟨heq, hvf⟩
        · simp only [Function.update_of_ne hv]
          exact h.grant_state v c' t hmem
      · -- the appended message, when granted
        rw [Msg.vote.injEq] at hmem
        obtain ⟨rfl, rfl, rfl, hg⟩ := hmem
        simp only [Function.update_self]
        exact Or.inr ⟨recv_term _ _ _ _ _ hterm,
          recv_granted _ _ _ _ _ hterm hg.symm⟩
    · -- grant_uniq: old×old by induction; old×new via grant_state — the
      -- enabling `currentTerm ≤ newTerm` forces the old grant's right
      -- disjunct, and recv_voted_current's idempotency pins the candidate.
      intro v c₁ c₂ t h1 h2
      simp only [List.mem_append, List.mem_singleton] at h1 h2
      rcases h1 with h1 | h1
      · rcases h2 with h2 | h2
        · exact h.grant_uniq v c₁ c₂ t h1 h2
        · rw [Msg.vote.injEq] at h2
          obtain ⟨rfl, rfl, rfl, hg⟩ := h2
          rcases h.grant_state v c₁ _ h1 with hlt | ⟨heq, hvf⟩
          · exact absurd hlt (by omega)
          · exact (recv_voted_current _ _ _ _ _ _ heq
              (by rw [heq]; exact hvf)).2 hg.symm
      · rw [Msg.vote.injEq] at h1
        obtain ⟨rfl, rfl, rfl, hg⟩ := h1
        rcases h2 with h2 | h2
        · rcases h.grant_state v c₂ _ h2 with hlt | ⟨heq, hvf⟩
          · exact absurd hlt (by omega)
          · exact ((recv_voted_current _ _ _ _ _ _ heq
              (by rw [heq]; exact hvf)).2 hg.symm).symm
        · rw [Msg.vote.injEq] at h2
          obtain ⟨-, rfl, -, -⟩ := h2
          rfl
    · -- self_vote: surviving candidacy/leadership means no adoption, and a
      -- recorded current-term (self-)vote freezes the whole state.
      intro v hrole'
      rcases eq_or_ne v j with rfl | hv
      · simp only [Function.update_self] at hrole' ⊢
        by_cases hadopt : (w.nodes v).currentTerm < nt
        · exact absurd (recv_adopt_role _ _ _ _ _ hadopt) hrole'
        · have hfr := recv_frame (w.nodes v) c nt clt cd hadopt
          have hold := h.self_vote v (hfr.1 ▸ hrole')
          have heqn : (w.nodes v).currentTerm = nt := by omega
          rw [(recv_voted_current (w.nodes v) c v nt clt cd heqn hold).1]
          exact hold
      · simp only [Function.update_of_ne hv] at hrole' ⊢
        exact h.self_vote v hrole'
    · intro v hrole' u hu
      rcases eq_or_ne v j with rfl | hv
      · simp only [Function.update_self] at hrole' hu ⊢
        by_cases hadopt : (w.nodes v).currentTerm < nt
        · exact absurd (recv_adopt_role _ _ _ _ _ hadopt) hrole'
        · have hfr := recv_frame (w.nodes v) c nt clt cd hadopt
          rw [hfr.2.2] at hu
          rw [hfr.2.1]
          exact (h.votes_sound v (hfr.1 ▸ hrole') u hu).imp id
            (List.mem_append_left _)
      · simp only [Function.update_of_ne hv] at hrole' hu ⊢
        exact (h.votes_sound v hrole' u hu).imp id (List.mem_append_left _)
    · intro v hL
      rcases eq_or_ne v j with rfl | hv
      · simp only [Function.update_self] at hL ⊢
        by_cases hadopt : (w.nodes v).currentTerm < nt
        · rw [recv_adopt_role _ _ _ _ _ hadopt] at hL
          simp at hL
        · have hfr := recv_frame (w.nodes v) c nt clt cd hadopt
          rw [hfr.2.2]
          exact h.leader_quorum v (hfr.1 ▸ hL)
      · simp only [Function.update_of_ne hv] at hL ⊢
        exact h.leader_quorum v hL
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro v c' t hmem
      simp only [List.mem_append, List.mem_singleton] at hmem
      rcases hmem with hmem | hmem
      · exact h.grant_state v c' t hmem
      · exact absurd hmem (by simp)
    · intro v c₁ c₂ t h1 h2
      simp only [List.mem_append, List.mem_singleton] at h1 h2
      rcases h1 with h1 | h1
      · rcases h2 with h2 | h2
        · exact h.grant_uniq v c₁ c₂ t h1 h2
        · exact absurd h2 (by simp)
      · exact absurd h1 (by simp)
    · exact h.self_vote
    · intro v hrole' u hu
      exact (h.votes_sound v hrole' u hu).imp id (List.mem_append_left _)
    · exact h.leader_quorum
  | deliverVote i v t hmsg hrole hterm =>
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro a c' t' hmem
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self]
        exact h.grant_state a c' t' hmem
      · simp only [Function.update_of_ne ha]
        exact h.grant_state a c' t' hmem
    · exact h.grant_uniq
    · intro a hrole'
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self]
        exact h.self_vote a (by rw [hrole]; decide)
      · simp only [Function.update_of_ne ha] at hrole' ⊢
        exact h.self_vote a hrole'
    · -- votes_sound: the inserted voter is witnessed by exactly `hmsg`.
      intro a hrole' u hu
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hu ⊢
        have hu' : u ∈ insert v (w.nodes a).votesReceived := hu
        rcases Finset.mem_insert.mp hu' with rfl | hu''
        · right
          show Msg.vote u a ((w.nodes a).currentTerm) true ∈ w.sent
          rw [hterm]
          exact hmsg
        · exact h.votes_sound a (by rw [hrole]; decide) u hu''
      · simp only [Function.update_of_ne ha] at hrole' hu ⊢
        exact h.votes_sound a hrole' u hu
    · intro a hL
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hL
        have hL' : (w.nodes a).role = .leader := hL
        rw [hrole] at hL'
        simp at hL'
      · simp only [Function.update_of_ne ha] at hL ⊢
        exact h.leader_quorum a hL
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · -- grant_state: adoption jumps strictly past every old grant.
      intro a c' t' hmem
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self]
        left
        show t' < t
        rcases h.grant_state a c' t' hmem with hlt | ⟨heq, _⟩ <;> omega
      · simp only [Function.update_of_ne ha]
        exact h.grant_state a c' t' hmem
    · exact h.grant_uniq
    · intro a hrole'
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hrole'
        exact absurd rfl hrole'
      · simp only [Function.update_of_ne ha] at hrole' ⊢
        exact h.self_vote a hrole'
    · intro a hrole' u hu
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hrole'
        exact absurd rfl hrole'
      · simp only [Function.update_of_ne ha] at hrole' hu ⊢
        exact h.votes_sound a hrole' u hu
    · intro a hL
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hL
        have hL' : Role.follower = Role.leader := hL
        simp at hL'
      · simp only [Function.update_of_ne ha] at hL ⊢
        exact h.leader_quorum a hL
  | becomeLeader i hrole hquorum =>
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro a c' t' hmem
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self]
        exact h.grant_state a c' t' hmem
      · simp only [Function.update_of_ne ha]
        exact h.grant_state a c' t' hmem
    · exact h.grant_uniq
    · intro a hrole'
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self]
        exact h.self_vote a (by rw [hrole]; decide)
      · simp only [Function.update_of_ne ha] at hrole' ⊢
        exact h.self_vote a hrole'
    · intro a hrole' u hu
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hu ⊢
        exact h.votes_sound a (by rw [hrole]; decide) u hu
      · simp only [Function.update_of_ne ha] at hrole' hu ⊢
        exact h.votes_sound a hrole' u hu
    · -- leader_quorum: exactly the `becomeLeader` enabling condition.
      intro a hL
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self]
        exact hquorum
      · simp only [Function.update_of_ne ha] at hL ⊢
        exact h.leader_quorum a hL
  | crashRestart i =>
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · -- grant_state survives a crash because `currentTerm`/`votedFor` are
      -- the StableValue-persisted pair (V3, decision 3).
      intro a c' t' hmem
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self]
        exact h.grant_state a c' t' hmem
      · simp only [Function.update_of_ne ha]
        exact h.grant_state a c' t' hmem
    · exact h.grant_uniq
    · intro a hrole'
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hrole'
        exact absurd rfl hrole'
      · simp only [Function.update_of_ne ha] at hrole' ⊢
        exact h.self_vote a hrole'
    · intro a hrole' u hu
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hrole'
        exact absurd rfl hrole'
      · simp only [Function.update_of_ne ha] at hrole' hu ⊢
        exact h.votes_sound a hrole' u hu
    · intro a hL
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hL
        have hL' : Role.follower = Role.leader := hL
        simp at hL'
      · simp only [Function.update_of_ne ha] at hL ⊢
        exact h.leader_quorum a hL
  | adoptHigherTerm i t hterm =>
    -- LA1: message-free higher-term adoption — the `deliverVoteHigherTerm`
    -- argument verbatim (that case never consumed its message witness).
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · -- grant_state: adoption jumps strictly past every old grant.
      intro a c' t' hmem
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self]
        left
        show t' < t
        rcases h.grant_state a c' t' hmem with hlt | ⟨heq, _⟩ <;> omega
      · simp only [Function.update_of_ne ha]
        exact h.grant_state a c' t' hmem
    · exact h.grant_uniq
    · intro a hrole'
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hrole'
        exact absurd rfl hrole'
      · simp only [Function.update_of_ne ha] at hrole' ⊢
        exact h.self_vote a hrole'
    · intro a hrole' u hu
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hrole'
        exact absurd rfl hrole'
      · simp only [Function.update_of_ne ha] at hrole' hu ⊢
        exact h.votes_sound a hrole' u hu
    · intro a hL
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hL
        have hL' : Role.follower = Role.leader := hL
        simp at hL'
      · simp only [Function.update_of_ne ha] at hL ⊢
        exact h.leader_quorum a hL
  | absorbDurable i =>
    -- issue #7: the consensus agent absorbing the durable counter into
    -- `smDurable` touches nothing the invariant mentions (no term, no vote,
    -- no role, no tally, no message) — same shape as `havocData` below.
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro a c' t' hmem
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self]
        exact h.grant_state a c' t' hmem
      · simp only [Function.update_of_ne ha]
        exact h.grant_state a c' t' hmem
    · exact h.grant_uniq
    · intro a hrole'
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hrole' ⊢
        exact h.self_vote a hrole'
      · simp only [Function.update_of_ne ha] at hrole' ⊢
        exact h.self_vote a hrole'
    · intro a hrole' u hu
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hrole' hu ⊢
        exact h.votes_sound a hrole' u hu
      · simp only [Function.update_of_ne ha] at hrole' hu ⊢
        exact h.votes_sound a hrole' u hu
    · intro a hL
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hL ⊢
        exact h.leader_quorum a hL
      · simp only [Function.update_of_ne ha] at hL ⊢
        exact h.leader_quorum a hL
  | havocData i nlt nd nsm =>
    -- the havoc data plane touches nothing the invariant mentions
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro a c' t' hmem
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self]
        exact h.grant_state a c' t' hmem
      · simp only [Function.update_of_ne ha]
        exact h.grant_state a c' t' hmem
    · exact h.grant_uniq
    · intro a hrole'
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hrole' ⊢
        exact h.self_vote a hrole'
      · simp only [Function.update_of_ne ha] at hrole' ⊢
        exact h.self_vote a hrole'
    · intro a hrole' u hu
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hrole' hu ⊢
        exact h.votes_sound a hrole' u hu
      · simp only [Function.update_of_ne ha] at hrole' hu ⊢
        exact h.votes_sound a hrole' u hu
    · intro a hL
      rcases eq_or_ne a i with rfl | ha
      · simp only [Function.update_self] at hL ⊢
        exact h.leader_quorum a hL
      · simp only [Function.update_of_ne ha] at hL ⊢
        exact h.leader_quorum a hL

/-- Closure induction: the invariant holds in every reachable world. -/
theorem reachable_inv {n : Nat} {w : World n} (hw : Reachable w) : Inv w := by
  have h : Relation.ReflTransGen Step (World.init n) w := hw
  clear hw
  induction h with
  | refl => exact inv_init n
  | tail _ hstep ih => exact inv_step ih hstep

/-- **ELECTION SAFETY** (spec §7): at most one leader per term, under message
loss/duplication/reordering, crash-restart, and an arbitrary (havoc) data
plane. -/
theorem election_safety {n : Nat} (w : World n) (hw : Reachable w)
    (i j : Fin n)
    (hi : (w.nodes i).role = .leader) (hj : (w.nodes j).role = .leader)
    (ht : (w.nodes i).currentTerm = (w.nodes j).currentTerm) : i = j := by
  have hInv := reachable_inv hw
  obtain ⟨v, hv⟩ := quorum_intersect n _ _
    (hInv.leader_quorum i hi) (hInv.leader_quorum j hj)
  rw [Finset.mem_inter] at hv
  have hine : (w.nodes i).role ≠ .follower := by rw [hi]; decide
  have hjne : (w.nodes j).role ≠ .follower := by rw [hj]; decide
  rcases hInv.votes_sound i hine v hv.1 with hvi | hgi
  · rcases hInv.votes_sound j hjne v hv.2 with hvj | hgj
    · exact hvi.symm.trans hvj
    · -- the shared voter IS leader i, and it granted j at j's term = i's
      -- term: that contradicts i's recorded self-vote unless i = j.
      rw [hvi] at hgj
      rcases hInv.grant_state i j _ hgj with hlt | ⟨_, hvf⟩
      · exact absurd hlt (by omega)
      · have hcomb := hvf.symm.trans (hInv.self_vote i hine)
        simp only [Option.some.injEq, Prod.mk.injEq] at hcomb
        exact hcomb.2.symm
  · rcases hInv.votes_sound j hjne v hv.2 with hvj | hgj
    · -- symmetric: the shared voter IS leader j, and it granted i.
      rw [hvj] at hgi
      rcases hInv.grant_state j i _ hgi with hlt | ⟨_, hvf⟩
      · exact absurd hlt (by omega)
      · have hcomb := hvf.symm.trans (hInv.self_vote j hjne)
        simp only [Option.some.injEq, Prod.mk.injEq] at hcomb
        exact hcomb.2
    · -- the shared voter granted BOTH leaders at the same term.
      rw [← ht] at hgj
      exact hInv.grant_uniq v i j _ hgi hgj

end Uc2
