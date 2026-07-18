import Uc2Proofs.ProtocolData

/-! LA2 — the coherence invariant and LOG-MATCHING.

Over LA1's data-plane model (`Uc2Proofs/ProtocolData.lean`): if two nodes hold
a stamped record at the same byte position under the same term, the payloads
are equal (`log_matching`, the Raft log-matching analog over byte positions —
spec §6's "within a term, bytes are cluster-identical").

Proof architecture (the invariant `DInv`, five clauses):

- `coherent` — the theorem's carrier: any two *occurrences* of `(pos, term, ·)`
  anywhere in the world (a node's `hist` OR an in-flight `replicate` frame in
  `dsent`) agree on the payload. Every constructor either copies an existing
  occurrence (`deliverReplicate`), erases occurrences (`deliverTermMap`
  truncation/wipe — erasure preserves agreement), or creates one fresh
  (`leaderAppend`, protected by `frontier`).
- `frontier` — a leader's `durable` strictly bounds every occurrence stamped
  with its own term, so `leaderAppend` can never collide with a shipped
  `(pos, term)` pair.
- `gossip_pinned` — a gossip frame at the current leader's term carries
  exactly that leader's term map, so same-term self-gossip reconciles as the
  identity (`reconcile_self`) and a leader never truncates its own tenure.
- `map_pinned` — a leader's map frontier term IS its `currentTerm`; with the
  `stamp ≤ currentTerm` replication guard (LA1 module doc, item 6) this
  freezes the leader's term map for the whole tenure.
- `cert` — the cross-time writer certificate: any term that has ever produced
  content (an occurrence or a gossip frame) carries a quorum of granted votes
  in the append-only `sent` set, a self-pin on the writer's persistent vote
  record, and a no-foreign-grant guarantee. Together with the lifted S2
  invariant (`votes_sound`/`grant_state`/`grant_uniq`/`self_vote`) this blocks
  a SECOND `becomeLeader` at the same term forever — the across-time
  uniqueness that the (simultaneous-only) `election_safety` cannot provide.

The election-side facts come through LA1's projection
(`Uc2.reachable_inv (reachable_project hw)`); the truncation case needs only
that erasure is monotone, so the R-series lemmas are not consumed here (they
carry the *exactness* of truncation, not its safety). -/

namespace Uc2.Data

/-! ## Non-vacuity: the truncation arm fires

LA1's review deliverable: a trace in which `deliverTermMap`'s reconcile
genuinely truncates (`validUpTo < durable`) and a stale tail dies. Node 0
leads term 1 and appends two records, but only the first replicates to
node 1 before node 1 wins term 2 with the SHORTER durable (its credentials
`(lastTerm 1, durable 1)` beat node 2's `(0, 0)`, so the election restriction
is satisfied honestly). Node 1 appends `(pos 1, term 2, 99)` — the world now
holds divergent tails at position 1 (`(1, 43)` at node 0 vs `(2, 99)` at
node 1; different terms, so LM is not violated). Node 1's gossip
`[(1, 0), (2, 1)]` reconciles at node 0 to `validUpTo = 1 < durable = 2`:
node 0's stale tail dies. The follow-on `deliverReplicate` re-converges
node 0 onto the new leader's byte. -/

/-- **Non-vacuity (truncation).** Reconcile-on-gossip genuinely truncates a
reachable divergent tail, and the truncated follower re-converges. -/
theorem nonvacuity_truncation_trace :
    ∃ w w' w'' : World 3,
      Reachable w ∧ Step w w' ∧ Step w' w'' ∧
      -- divergent tails at position 1 (stale term-1 byte vs term-2 byte):
      (w.nodes 0).hist 1 = some (1, 43) ∧ (w.nodes 0).pn.durable = 2 ∧
      (w.nodes 1).hist 1 = some (2, 99) ∧
      -- the gossip kills node 0's tail (validUpTo = 1 < durable = 2):
      (w'.nodes 0).hist 1 = none ∧ (w'.nodes 0).pn.durable = 1 ∧
      (w'.nodes 0).hist 0 = some (1, 42) ∧
      -- and node 0 re-converges onto the term-2 leader's byte:
      (w''.nodes 0).hist 1 = some (2, 99) := by
  refine ⟨_, _, _,
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail
      (.single (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 0 1 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 42 (by decide)))
      (.leaderAppend _ 0 43 (by decide)))
      (.deliverReplicate _ 1 0 1 1 42 (by decide) (by decide) (by decide)))
      (.startElection _ 1 (by decide)))
      (.deliverRequestVote _ 2 1 2 1 1 (by decide) (by decide)))
      (.deliverVote _ 1 2 2 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 1 (by decide) (by decide)))
      (.leaderAppend _ 1 99 (by decide)))
      (.shipTermMap _ 1 (by decide)),
    .deliverTermMap _ 0 2 [(1, 0), (2, 1)] (by decide) (by decide),
    .deliverReplicate _ 0 1 2 2 99 (by decide) (by decide) (by decide),
    by decide, by decide, by decide, by decide, by decide, by decide,
    by decide⟩

#print axioms nonvacuity_truncation_trace

/-! ## Occurrences -/

/-- A stamped payload *occurrence* anywhere in the world: an in-flight
`replicate` frame (under ANY wire header — occurrences are keyed on the
record STAMP `t`, which is what LM is about; the LC1 header/stamp split
leaves this quantification existential), or a node's history entry.
Coherence is stated over occurrences so that delivery — a copy from `dsent`
into a `hist` — can never mint a new payload, and so that `leaderAppend`'s
freshness obligation covers the frames the leader already shipped. -/
def Occ {n : Nat} (w : World n) (p t v : Nat) : Prop :=
  (∃ hdr, Frame.replicate p hdr t v ∈ w.dsent) ∨
    ∃ i : Fin n, (w.nodes i).hist p = some (t, v)

/-- The cross-time writer certificate for term `t`: evidence, stable under
every step, that `ℓ` won term `t` and nobody else ever can. `quorum` lives in
the append-only `sent` set; `pinned` rides the `StableValue`-persisted vote
record (`currentTerm` is monotone, a current-term vote is frozen, and a node
can never RE-become candidate at a term it already holds — role changes to
candidate only via `startElection`, which strictly bumps the term); dually,
`noForeign` says `ℓ` never granted term `t` away (a grant at `t` can only be
created while `currentTerm ≤ t`, where the vote discipline's idempotency pins
it to the recorded self-vote). -/
structure Cert {n : Nat} (w : World n) (t : Nat) (ℓ : Fin n) : Prop where
  /-- A quorum of grants-or-self for `ℓ` at `t` in the append-only wire. -/
  quorum : ∃ Q : Finset (Fin n), n / 2 + 1 ≤ Q.card ∧
      ∀ u ∈ Q, u = ℓ ∨ Uc2.Msg.vote u ℓ t true ∈ w.sent
  /-- The writer's own vote record still pins term `t`, unless it has moved
  strictly past it. The `role ≠ .candidate` conjunct is what makes the
  certificate block a RE-election at `t`. -/
  pinned : t < (w.nodes ℓ).pn.currentTerm ∨
      ((w.nodes ℓ).pn.currentTerm = t ∧
       (w.nodes ℓ).pn.votedFor = some (t, ℓ) ∧
       (w.nodes ℓ).pn.role ≠ .candidate)
  /-- `ℓ` never granted term `t` to anyone else. -/
  noForeign : ∀ c : Fin n, Uc2.Msg.vote ℓ c t true ∈ w.sent → c = ℓ

/-- The five-clause data-plane invariant (module doc). -/
structure DInv {n : Nat} (w : World n) : Prop where
  /-- LM's carrier: occurrences at the same `(pos, term)` agree. -/
  coherent : ∀ p t v v', Occ w p t v → Occ w p t v' → v = v'
  /-- A leader's `durable` strictly bounds every occurrence at its term. -/
  frontier : ∀ i : Fin n, (w.nodes i).pn.role = .leader →
      ∀ p v, Occ w p ((w.nodes i).pn.currentTerm) v →
        p < (w.nodes i).pn.durable
  /-- A gossip frame at the current leader's term is that leader's map. -/
  gossip_pinned : ∀ i : Fin n, (w.nodes i).pn.role = .leader →
      ∀ es, Frame.gossip ((w.nodes i).pn.currentTerm) es ∈ w.dsent →
        es = (w.nodes i).termMap
  /-- A leader's map frontier term is its `currentTerm`. -/
  map_pinned : ∀ i : Fin n, (w.nodes i).pn.role = .leader →
      lastTermOf (w.nodes i).termMap = (w.nodes i).pn.currentTerm
  /-- Every term with content carries a writer certificate. -/
  cert : ∀ t : Nat,
      ((∃ p v, Occ w p t v) ∨ (∃ es, Frame.gossip t es ∈ w.dsent)) →
      ∃ ℓ : Fin n, Cert w t ℓ

/-! ## Model-function toolkit -/

private theorem commonPrefixLen_self : ∀ m : TermMap,
    commonPrefixLen m m = m.length
  | [] => rfl
  | _ :: es => by
    simp [commonPrefixLen, commonPrefixLen_self es]

/-- Reconciling against one's own map is the identity — the leader-side
no-truncation fact `gossip_pinned` cashes in. -/
private theorem reconcile_self (m : TermMap) (d : Nat) :
    reconcile m d m = .ok ⟨d, m⟩ := by
  cases m with
  | nil => rfl
  | cons e es =>
    have hnone : (e :: es)[(e :: es).length]? = none :=
      List.getElem?_eq_none (Nat.le_refl _)
    simp only [reconcile, commonPrefixLen_self]
    rw [if_neg (by simp)]
    simp only [reconcile.reconcileClamped, hnone,
      List.take_of_length_le (Nat.le_refl _),
      List.drop_of_length_le (Nat.le_refl _), List.filter_nil,
      List.append_nil]

/-- `become_leader`'s pruned push always lands `(t, d)` last. -/
private theorem lastTermOf_prunePush (m : TermMap) (t d : Nat) :
    lastTermOf (prunePush m t d) = t := by
  simp [prunePush, lastTermOf]

/-- `DataTermObserved` is a no-op at or below the map's frontier term — with
the `stamp ≤ currentTerm` guard and `map_pinned`, a leader's map is frozen
for its whole tenure. -/
private theorem observeTerm_of_le {m : TermMap} {t : Nat}
    (h : t ≤ lastTermOf m) (pos : Nat) : observeTerm m t pos = m := by
  simp [observeTerm, Nat.not_lt.mpr h]

/-! ## `applyGossip` / `recvReplicate` / `recvRequestVote` toolkit -/

/-- Truncation/wipe only ERASES history — never rewrites. -/
private theorem applyGossip_hist {n : Nat} (d : Node n) (t : Nat)
    (entries : TermMap) (p : Nat) {tv : Nat × Nat}
    (h : (d.applyGossip t entries).hist p = some tv) :
    d.hist p = some tv := by
  cases hrec : reconcile d.termMap d.pn.durable entries <;>
    simp only [Node.applyGossip, hrec] at h
  · split at h
    · exact h
    · cases h
  · cases h

private theorem applyGossip_adopt {n : Nat} (d : Node n) {t : Nat}
    (entries : TermMap) (h : d.pn.currentTerm < t) :
    (d.applyGossip t entries).pn.role = .follower ∧
    (d.applyGossip t entries).pn.currentTerm = t := by
  cases hrec : reconcile d.termMap d.pn.durable entries <;>
    simp [Node.applyGossip, hrec, h, PNode.adoptTerm]

private theorem applyGossip_no_adopt {n : Nat} (d : Node n) {t : Nat}
    (entries : TermMap) (h : ¬ d.pn.currentTerm < t) :
    (d.applyGossip t entries).pn.role = d.pn.role ∧
    (d.applyGossip t entries).pn.currentTerm = d.pn.currentTerm ∧
    (d.applyGossip t entries).pn.votedFor = d.pn.votedFor := by
  cases hrec : reconcile d.termMap d.pn.durable entries <;>
    simp [Node.applyGossip, hrec, h]

/-- Same-map gossip is the identity on the data plane (`reconcile_self`). -/
private theorem applyGossip_self {n : Nat} (d : Node n) (t : Nat) :
    (d.applyGossip t d.termMap).pn.durable = d.pn.durable ∧
    (d.applyGossip t d.termMap).termMap = d.termMap := by
  simp [Node.applyGossip, reconcile_self]

private theorem recvReplicate_pn {n : Nat} (d : Node n) (pos t v : Nat) :
    (d.recvReplicate pos t v).pn.role = d.pn.role ∧
    (d.recvReplicate pos t v).pn.currentTerm = d.pn.currentTerm ∧
    (d.recvReplicate pos t v).pn.votedFor = d.pn.votedFor ∧
    (d.recvReplicate pos t v).pn.durable = pos + 1 := by
  simp [Node.recvReplicate]

/- The next four restate `ElectionSafety.lean`'s private `recvRequestVote`
characterization lemmas (private there, so re-proved here verbatim). -/

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

private theorem recv_voted_current {n : Nat} (s : PNode n) (c x : Fin n)
    (nt lt d : Nat) (heq : s.currentTerm = nt)
    (hvf : s.votedFor = some (s.currentTerm, x)) :
    (s.recvRequestVote c nt lt d).1 = s ∧
      ((s.recvRequestVote c nt lt d).2 = true → x = c) := by
  have hnadopt : ¬ s.currentTerm < nt := by omega
  by_cases hx : x = c <;>
    simp [PNode.recvRequestVote, if_neg hnadopt, hvf, hx]

private theorem recv_frame {n : Nat} (s : PNode n) (c : Fin n) (nt lt d : Nat)
    (hnadopt : ¬ s.currentTerm < nt) :
    ((s.recvRequestVote c nt lt d).1).role = s.role ∧
    ((s.recvRequestVote c nt lt d).1).currentTerm = s.currentTerm := by
  rcases hvf : s.votedFor with _ | ⟨vt, vid⟩ <;>
    simp only [PNode.recvRequestVote, if_neg hnadopt, hvf,
      PNode.recvRequestVote.grantIfFresh] <;>
    split_ifs <;> simp

private theorem recv_adopt_role {n : Nat} (s : PNode n) (c : Fin n)
    (nt lt d : Nat) (hadopt : s.currentTerm < nt) :
    ((s.recvRequestVote c nt lt d).1).role = .follower := by
  simp only [PNode.recvRequestVote, if_pos hadopt, PNode.adoptTerm,
    PNode.recvRequestVote.grantIfFresh]
  split_ifs <;> rfl

/-- `recvRequestVote` never touches the data plane (`durable`). -/
private theorem recv_durable {n : Nat} (s : PNode n) (c : Fin n)
    (nt lt d : Nat) :
    ((s.recvRequestVote c nt lt d).1).durable = s.durable := by
  by_cases hadopt : s.currentTerm < nt
  · simp only [PNode.recvRequestVote, if_pos hadopt, PNode.adoptTerm,
      PNode.recvRequestVote.grantIfFresh]
    split_ifs <;> rfl
  · rcases hvf : s.votedFor with _ | ⟨vt, vid⟩ <;>
      simp only [PNode.recvRequestVote, if_neg hadopt, hvf,
        PNode.recvRequestVote.grantIfFresh] <;>
      split_ifs <;> rfl

/-! ## Emission-side stamp truthfulness (LC1)

The header/stamp frame split deleted the delivery-side `stamp ≤ currentTerm`
guard; its job moved to the emission sites. `StampInv` is the invariant that
carries it: every replicate frame's record stamp is bounded by its wire
header (`leaderAppend` emits `stamp = hdr`; `serveTail` re-serves a held
stamp, bounded by the leader's term via `hist_le`), and every held stamp is
bounded by the holder's monotone `currentTerm` (delivery accepts only under
an exact header match, so a fresh entry's stamp is `≤ hdr = currentTerm`).
Jointly inductive; `dinv_step`'s `deliverReplicate` case re-derives the old
`≤`-guard from `frame_le + hhdr`, which is what freezes a leader's map for
its whole tenure exactly as before. -/

/-- Frame stamps never exceed their wire header; held stamps never exceed
the holder's current term. -/
structure StampInv {n : Nat} (w : World n) : Prop where
  /-- `stamp ≤ hdr` for every replicate frame ever shipped. -/
  frame_le : ∀ p hdr t v, Frame.replicate p hdr t v ∈ w.dsent → t ≤ hdr
  /-- A node's held stamps are bounded by its (monotone) `currentTerm`. -/
  hist_le : ∀ j : Fin n, ∀ p t v, (w.nodes j).hist p = some (t, v) →
      t ≤ (w.nodes j).pn.currentTerm

private theorem stamp_init (n : Nat) : StampInv (World.init n) := by
  constructor
  · intro p hdr t v h
    simp [World.init] at h
  · intro j p t v h
    simp [World.init] at h

private theorem stamp_step {n : Nat} {w w' : World n} (h : StampInv w)
    (hs : Step w w') : StampInv w' := by
  cases hs with
  | startElection i hrole =>
    refine ⟨h.frame_le, ?_⟩
    intro k p t v hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Function.update_self] at hh ⊢
      exact Nat.le_succ_of_le (h.hist_le k p t v hh)
    · simp only [Function.update_of_ne hne] at hh ⊢
      exact h.hist_le k p t v hh
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    refine ⟨h.frame_le, ?_⟩
    intro k p t v hh
    rcases eq_or_ne k j with rfl | hne
    · simp only [Function.update_self] at hh ⊢
      rw [recv_term _ _ _ _ _ hterm]
      exact Nat.le_trans (h.hist_le k p t v hh) hterm
    · simp only [Function.update_of_ne hne] at hh ⊢
      exact h.hist_le k p t v hh
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    exact ⟨h.frame_le, h.hist_le⟩
  | deliverVote i v t hmsg hrole hterm =>
    refine ⟨h.frame_le, ?_⟩
    intro k p t' v' hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Function.update_self] at hh ⊢
      exact h.hist_le k p t' v' hh
    · simp only [Function.update_of_ne hne] at hh ⊢
      exact h.hist_le k p t' v' hh
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    refine ⟨h.frame_le, ?_⟩
    intro k p t' v' hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Function.update_self] at hh ⊢
      exact Nat.le_trans (h.hist_le k p t' v' hh)
        (Nat.le_of_lt (show (w.nodes k).pn.currentTerm <
          ((w.nodes k).pn.adoptTerm t).currentTerm from hterm))
    · simp only [Function.update_of_ne hne] at hh ⊢
      exact h.hist_le k p t' v' hh
  | becomeLeader i hrole hquorum =>
    refine ⟨h.frame_le, ?_⟩
    intro k p t v hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Function.update_self] at hh ⊢
      exact h.hist_le k p t v hh
    · simp only [Function.update_of_ne hne] at hh ⊢
      exact h.hist_le k p t v hh
  | crashRestart i =>
    refine ⟨h.frame_le, ?_⟩
    intro k p t v hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Function.update_self] at hh ⊢
      exact h.hist_le k p t v hh
    · simp only [Function.update_of_ne hne] at hh ⊢
      exact h.hist_le k p t v hh
  | leaderAppend i v hrole =>
    constructor
    · intro p hdr t v' hf
      rcases List.mem_append.mp hf with hf | hf
      · exact h.frame_le p hdr t v' hf
      · rw [List.mem_singleton, Frame.replicate.injEq] at hf
        omega
    · intro k p t v' hh
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hh ⊢
        by_cases hp : p = (w.nodes k).pn.durable
        · subst hp
          rw [Function.update_self, Option.some.injEq, Prod.mk.injEq] at hh
          omega
        · rw [Function.update_of_ne hp] at hh
          exact h.hist_le k p t v' hh
      · simp only [Function.update_of_ne hne] at hh ⊢
        exact h.hist_le k p t v' hh
  | deliverReplicate j pos hdr t v hmsg hpos hhdr =>
    refine ⟨h.frame_le, ?_⟩
    intro k p t' v' hh
    rcases eq_or_ne k j with rfl | hne
    · simp only [Function.update_self, Node.recvReplicate] at hh ⊢
      by_cases hp : p = pos
      · subst hp
        rw [Function.update_self, Option.some.injEq, Prod.mk.injEq] at hh
        have := h.frame_le _ _ _ _ hmsg
        omega
      · rw [Function.update_of_ne hp] at hh
        exact h.hist_le k p t' v' hh
    · simp only [Function.update_of_ne hne] at hh ⊢
      exact h.hist_le k p t' v' hh
  | serveTail i p t v hrole hhist hp =>
    constructor
    · intro p' hdr t' v' hf
      rcases List.mem_append.mp hf with hf | hf
      · exact h.frame_le p' hdr t' v' hf
      · rw [List.mem_singleton, Frame.replicate.injEq] at hf
        obtain ⟨rfl, rfl, rfl, rfl⟩ := hf
        exact h.hist_le i _ _ _ hhist
    · exact h.hist_le
  | shipTermMap i hrole =>
    constructor
    · intro p hdr t v hf
      rcases List.mem_append.mp hf with hf | hf
      · exact h.frame_le p hdr t v hf
      · simp at hf
    · exact h.hist_le
  | deliverTermMap j t entries hmsg hterm =>
    refine ⟨h.frame_le, ?_⟩
    intro k p t' v' hh
    rcases eq_or_ne k j with rfl | hne
    · simp only [Function.update_self] at hh ⊢
      have hold := h.hist_le k p t' v' (applyGossip_hist _ _ _ _ hh)
      by_cases hadopt : (w.nodes k).pn.currentTerm < t
      · rw [(applyGossip_adopt _ entries hadopt).2]
        omega
      · rw [(applyGossip_no_adopt _ entries hadopt).2.1]
        exact hold
    · simp only [Function.update_of_ne hne] at hh ⊢
      exact h.hist_le k p t' v' hh

/-- **Stamp truthfulness holds in every reachable world** (public: the LC
layer consumes `frame_le` alongside `hist_frame_provenance`). -/
theorem reachable_stamp {n : Nat} {w : World n} (hw : Reachable w) :
    StampInv w := by
  have h : Relation.ReflTransGen Step (World.init n) w := hw
  clear hw
  induction h with
  | refl => exact stamp_init n
  | tail _ hstep ih => exact stamp_step ih hstep

/-! ## Occurrence decomposition -/

/-- Occurrences of the standard one-node-update successor world. -/
private theorem occ_mk {n : Nat} (w : World n) (i : Fin n) (d : Node n)
    (s : List (Uc2.Msg n)) (ds : List Frame) (p t v : Nat) :
    Occ { nodes := Function.update w.nodes i d, sent := s, dsent := ds }
        p t v ↔
      ((∃ hdr, Frame.replicate p hdr t v ∈ ds) ∨ d.hist p = some (t, v)) ∨
        ∃ k, k ≠ i ∧ (w.nodes k).hist p = some (t, v) := by
  simp only [Occ]
  constructor
  · rintro (hf | ⟨k, hk⟩)
    · exact .inl (.inl hf)
    · rcases eq_or_ne k i with rfl | hne
      · rw [Function.update_self] at hk
        exact .inl (.inr hk)
      · rw [Function.update_of_ne hne] at hk
        exact .inr ⟨k, hne, hk⟩
  · rintro ((hf | hd) | ⟨k, hne, hk⟩)
    · exact .inl hf
    · exact .inr ⟨i, by rw [Function.update_self]; exact hd⟩
    · exact .inr ⟨k, by rw [Function.update_of_ne hne]; exact hk⟩

/-- A pure-`pn` update (history untouched, data wire untouched) can only
shrink the occurrence set. Stated one-directionally with the update hypothesis
LAST so the node `d` is inferred from the occurrence argument. -/
private theorem occ_pn_sub {n : Nat} {w : World n} {i : Fin n} {d : Node n}
    {s : List (Uc2.Msg n)} {p t v : Nat}
    (hOcc : Occ { nodes := Function.update w.nodes i d
                  sent := s
                  dsent := w.dsent } p t v)
    (hd : d.hist = (w.nodes i).hist) : Occ w p t v := by
  rw [occ_mk, hd] at hOcc
  rcases hOcc with (hf | hd') | ⟨k, -, hk⟩
  · exact .inl hf
  · exact .inr ⟨i, hd'⟩
  · exact .inr ⟨k, hk⟩

/-- Coherence transports along occurrence inclusion (erasure/copy steps). -/
private theorem coherent_of_sub {n : Nat} {w w' : World n}
    (hsub : ∀ p t v, Occ w' p t v → Occ w p t v) (h : DInv w) :
    ∀ p t v v', Occ w' p t v → Occ w' p t v' → v = v' :=
  fun p t v v' h1 h2 => h.coherent p t v v' (hsub p t v h1) (hsub p t v' h2)

/-! ## Certificate transport and the two election-side lemmas -/

/-- Certificate transport: the quorum rides the growing `sent` set; the pin
is re-supplied by the caller (frozen fields, or a strict term bump); the
no-foreign-grant obligation only needs the NEW messages checked. -/
theorem Cert.transport {n : Nat} {w w' : World n} {t : Nat}
    {ℓ : Fin n} (hc : Cert w t ℓ)
    (hsent : ∀ m : Uc2.Msg n, m ∈ w.sent → m ∈ w'.sent)
    (hnew : ∀ c : Fin n, Uc2.Msg.vote ℓ c t true ∈ w'.sent →
      Uc2.Msg.vote ℓ c t true ∈ w.sent ∨ c = ℓ)
    (hpin : t < (w'.nodes ℓ).pn.currentTerm ∨
      ((w'.nodes ℓ).pn.currentTerm = t ∧
       (w'.nodes ℓ).pn.votedFor = some (t, ℓ) ∧
       (w'.nodes ℓ).pn.role ≠ .candidate)) :
    Cert w' t ℓ := by
  obtain ⟨Q, hQc, hQ⟩ := hc.quorum
  exact ⟨⟨Q, hQc, fun u hu => (hQ u hu).imp id (hsent _)⟩, hpin,
    fun c h => (hnew c h).elim (hc.noForeign c) id⟩

/-- A strict term bump at the writer collapses the pin to its left arm. -/
private theorem pinned_bump {n : Nat} {t : Nat} {ℓ : Fin n} {p q : PNode n}
    (hlt : p.currentTerm < q.currentTerm)
    (h : t < p.currentTerm ∨
      (p.currentTerm = t ∧ p.votedFor = some (t, ℓ) ∧ p.role ≠ .candidate)) :
    t < q.currentTerm ∨
      (q.currentTerm = t ∧ q.votedFor = some (t, ℓ) ∧
       q.role ≠ .candidate) := by
  left
  rcases h with h | ⟨h, -, -⟩ <;> omega

/-- A live leader certifies its own term: the tally is the quorum
(`leader_quorum`/`votes_sound`), the self-vote is the pin (`self_vote`), and
`grant_state` + `self_vote` rule out any foreign grant at its term. -/
theorem cert_of_leader {n : Nat} {w : World n}
    (hpInv : Uc2.Inv w.project) {i : Fin n}
    (hrole : (w.nodes i).pn.role = .leader) :
    Cert w ((w.nodes i).pn.currentTerm) i := by
  have hne : (w.project.nodes i).role ≠ .follower := by
    show (w.nodes i).pn.role ≠ .follower
    rw [hrole]
    decide
  refine ⟨⟨(w.nodes i).pn.votesReceived, hpInv.leader_quorum i hrole,
    fun u hu => hpInv.votes_sound i hne u hu⟩,
    .inr ⟨rfl, hpInv.self_vote i hne, by rw [hrole]; decide⟩, ?_⟩
  intro c hg
  rcases hpInv.grant_state i c _ hg with hlt | ⟨-, hvf⟩
  · exact absurd hlt (lt_irrefl _)
  · have hsv := hpInv.self_vote i hne
    rw [hvf] at hsv
    simp only [Option.some.injEq, Prod.mk.injEq] at hsv
    exact hsv.2

/-- **The cross-time blocker**: a certified term can never be won again. A
candidate's tally-quorum and the certificate's grant-quorum intersect; every
arm of the case split names the candidate as the certified writer, whose pin
then contradicts candidacy (either the term moved strictly past `t`, or
`role ≠ .candidate`). -/
theorem cert_blocks_candidate {n : Nat} {w : World n}
    (hpInv : Uc2.Inv w.project) {j : Fin n} {t : Nat}
    (hrole : (w.nodes j).pn.role = .candidate)
    (hterm : (w.nodes j).pn.currentTerm = t)
    (hquorum : n / 2 + 1 ≤ (w.nodes j).pn.votesReceived.card)
    {ℓ : Fin n} (hc : Cert w t ℓ) : False := by
  have hne : (w.project.nodes j).role ≠ .follower := by
    show (w.nodes j).pn.role ≠ .follower
    rw [hrole]
    decide
  have hterm' : (w.project.nodes j).currentTerm = t := hterm
  obtain ⟨Q, hQc, hQ⟩ := hc.quorum
  obtain ⟨u, hu⟩ :=
    quorum_intersect n (w.nodes j).pn.votesReceived Q hquorum hQc
  rw [Finset.mem_inter] at hu
  have hvs := hpInv.votes_sound j hne u hu.1
  rw [hterm'] at hvs
  have hQu := hQ u hu.2
  have hlj : ℓ = j := by
    rcases hvs with rfl | hgj
    · -- the shared voter IS the candidate
      rcases hQu with rfl | hgl
      · rfl
      · -- the candidate granted ℓ at t; its recorded self-vote pins ℓ
        rcases hpInv.grant_state u ℓ t hgl with hlt | ⟨-, hvf⟩
        · have hlt' : t < (w.nodes u).pn.currentTerm := hlt
          have : (w.nodes u).pn.currentTerm = t := hterm
          omega
        · have hsv := hpInv.self_vote u hne
          rw [hvf] at hsv
          simp only [Option.some.injEq, Prod.mk.injEq] at hsv
          exact hsv.2
    · rcases hQu with rfl | hgl
      · -- the shared voter IS the writer, and it granted the candidate
        exact (hc.noForeign j hgj).symm
      · -- the shared voter granted both at t
        exact (hpInv.grant_uniq u j ℓ t hgj hgl).symm
  subst hlj
  rcases hc.pinned with hlt | ⟨-, -, hnc⟩
  · have : (w.nodes ℓ).pn.currentTerm = t := hterm
    omega
  · exact hnc hrole

/-! ## The invariant holds at boot and is preserved -/

theorem dinv_init (n : Nat) : DInv (World.init n) where
  coherent := by
    intro p t v v' h1 _
    simp only [Occ] at h1
    rcases h1 with h1 | ⟨i, h1⟩ <;> simp [World.init] at h1
  frontier := by
    intro i h
    simp [World.init] at h
  gossip_pinned := by
    intro i h
    simp [World.init] at h
  map_pinned := by
    intro i h
    simp [World.init] at h
  cert := by
    intro t h
    rcases h with ⟨p, v, h⟩ | ⟨es, h⟩
    · simp only [Occ] at h
      rcases h with h | ⟨i, h⟩ <;> simp [World.init] at h
    · simp [World.init] at h

/-- **Preservation**: every `Step` preserves `DInv`. The election-side facts
(`votes_sound`, `grant_state`, `grant_uniq`, `self_vote`, `leader_quorum`,
`election_safety`) come through LA1's projection, which is why the pre-state
`Reachable` hypothesis is carried. -/
theorem dinv_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : DInv w) (hs : Step w w') : DInv w' := by
  have hpInv : Uc2.Inv w.project := Uc2.reachable_inv (reachable_project hw)
  cases hs with
  | startElection i hrole =>
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro p t v v' h1 h2
      exact h.coherent p t v v' (occ_pn_sub h1 rfl) (occ_pn_sub h2 rfl)
    · intro k hk p v hOcc
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk
        cases hk
      · simp only [Function.update_of_ne hne] at hk hOcc ⊢
        exact h.frontier k hk p v (occ_pn_sub hOcc rfl)
    · intro k hk es hg
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk
        cases hk
      · simp only [Function.update_of_ne hne] at hk hg ⊢
        exact h.gossip_pinned k hk es hg
    · intro k hk
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk
        cases hk
      · simp only [Function.update_of_ne hne] at hk ⊢
        exact h.map_pinned k hk
    · intro t hcg
      have hcg' : (∃ p v, Occ w p t v) ∨
          (∃ es, Frame.gossip t es ∈ w.dsent) := by
        rcases hcg with ⟨p, v, hOcc⟩ | hg
        · exact .inl ⟨p, v, occ_pn_sub hOcc rfl⟩
        · exact .inr hg
      obtain ⟨ℓ, hc⟩ := h.cert t hcg'
      refine ⟨ℓ, hc.transport (fun m hm => List.mem_append_left _ hm) ?_ ?_⟩
      · intro c hcm
        rcases List.mem_append.mp hcm with hcm | hcm
        · exact .inl hcm
        · simp at hcm
      · rcases eq_or_ne ℓ i with rfl | hne
        · simp only [Function.update_self]
          left
          rcases hc.pinned with hlt | ⟨heqt, -, -⟩ <;> omega
        · simp only [Function.update_of_ne hne]
          exact hc.pinned
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro p t v v' h1 h2
      exact h.coherent p t v v' (occ_pn_sub h1 rfl) (occ_pn_sub h2 rfl)
    · intro k hk p v hOcc
      rcases eq_or_ne k j with rfl | hne
      · simp only [Function.update_self] at hk hOcc ⊢
        by_cases hadopt : (w.nodes k).pn.currentTerm < nt
        · rw [recv_adopt_role _ _ _ _ _ hadopt] at hk
          cases hk
        · have hfr := recv_frame (w.nodes k).pn c nt clt cd hadopt
          rw [recv_durable]
          rw [hfr.2] at hOcc
          exact h.frontier k (hfr.1 ▸ hk) p v (occ_pn_sub hOcc rfl)
      · simp only [Function.update_of_ne hne] at hk hOcc ⊢
        exact h.frontier k hk p v (occ_pn_sub hOcc rfl)
    · intro k hk es hg
      rcases eq_or_ne k j with rfl | hne
      · simp only [Function.update_self] at hk hg ⊢
        by_cases hadopt : (w.nodes k).pn.currentTerm < nt
        · rw [recv_adopt_role _ _ _ _ _ hadopt] at hk
          cases hk
        · have hfr := recv_frame (w.nodes k).pn c nt clt cd hadopt
          rw [hfr.2] at hg
          exact h.gossip_pinned k (hfr.1 ▸ hk) es hg
      · simp only [Function.update_of_ne hne] at hk hg ⊢
        exact h.gossip_pinned k hk es hg
    · intro k hk
      rcases eq_or_ne k j with rfl | hne
      · simp only [Function.update_self] at hk ⊢
        by_cases hadopt : (w.nodes k).pn.currentTerm < nt
        · rw [recv_adopt_role _ _ _ _ _ hadopt] at hk
          cases hk
        · have hfr := recv_frame (w.nodes k).pn c nt clt cd hadopt
          rw [hfr.2]
          exact h.map_pinned k (hfr.1 ▸ hk)
      · simp only [Function.update_of_ne hne] at hk ⊢
        exact h.map_pinned k hk
    · intro t hcg
      have hcg' : (∃ p v, Occ w p t v) ∨
          (∃ es, Frame.gossip t es ∈ w.dsent) := by
        rcases hcg with ⟨p, v, hOcc⟩ | hg
        · exact .inl ⟨p, v, occ_pn_sub hOcc rfl⟩
        · exact .inr hg
      obtain ⟨ℓ, hc⟩ := h.cert t hcg'
      refine ⟨ℓ, hc.transport (fun m hm => List.mem_append_left _ hm) ?_ ?_⟩
      · -- a new grant matching (ℓ, t) can only be the idempotent self
        -- re-grant: the enabling `currentTerm ≤ nt` kills the pin's left
        -- arm, and the right arm freezes the vote via `recv_voted_current`
        intro c' hcm
        rcases List.mem_append.mp hcm with hcm | hcm
        · exact .inl hcm
        · rw [List.mem_singleton, Uc2.Msg.vote.injEq] at hcm
          obtain ⟨rfl, rfl, rfl, hg⟩ := hcm
          rcases hc.pinned with hlt | ⟨heqt, hvf, -⟩
          · exact absurd hlt (Nat.not_lt.mpr hterm)
          · right
            exact ((recv_voted_current (w.nodes ℓ).pn c' ℓ t clt cd heqt
              (by rw [heqt]; exact hvf)).2 hg.symm).symm
      · rcases eq_or_ne ℓ j with rfl | hne
        · simp only [Function.update_self]
          by_cases hadopt : (w.nodes ℓ).pn.currentTerm < nt
          · refine pinned_bump ?_ hc.pinned
            rw [recv_term _ _ _ _ _ hterm]
            omega
          · rcases hc.pinned with hlt | ⟨heqt, hvf, hnc⟩
            · have hfr := recv_frame (w.nodes ℓ).pn c nt clt cd hadopt
              rw [hfr.2]
              exact .inl hlt
            · rw [(recv_voted_current (w.nodes ℓ).pn c ℓ nt clt cd
                (by omega) (by rw [heqt]; exact hvf)).1]
              exact .inr ⟨heqt, hvf, hnc⟩
        · simp only [Function.update_of_ne hne]
          exact hc.pinned
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    refine ⟨h.coherent, h.frontier, h.gossip_pinned, h.map_pinned, ?_⟩
    intro t hcg
    obtain ⟨ℓ, hc⟩ := h.cert t hcg
    refine ⟨ℓ, hc.transport (fun m hm => List.mem_append_left _ hm) ?_
      hc.pinned⟩
    intro c' hcm
    rcases List.mem_append.mp hcm with hcm | hcm
    · exact .inl hcm
    · simp at hcm
  | deliverVote i v t hmsg hrole hterm =>
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro p t' v1 v2 h1 h2
      exact h.coherent p t' v1 v2 (occ_pn_sub h1 rfl) (occ_pn_sub h2 rfl)
    · intro k hk p v' hOcc
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk hOcc ⊢
        exact h.frontier k hk p v' (occ_pn_sub hOcc rfl)
      · simp only [Function.update_of_ne hne] at hk hOcc ⊢
        exact h.frontier k hk p v' (occ_pn_sub hOcc rfl)
    · intro k hk es hg
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk hg ⊢
        exact h.gossip_pinned k hk es hg
      · simp only [Function.update_of_ne hne] at hk hg ⊢
        exact h.gossip_pinned k hk es hg
    · intro k hk
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk ⊢
        exact h.map_pinned k hk
      · simp only [Function.update_of_ne hne] at hk ⊢
        exact h.map_pinned k hk
    · intro t' hcg
      have hcg' : (∃ p v', Occ w p t' v') ∨
          (∃ es, Frame.gossip t' es ∈ w.dsent) := by
        rcases hcg with ⟨p, v', hOcc⟩ | hg
        · exact .inl ⟨p, v', occ_pn_sub hOcc rfl⟩
        · exact .inr hg
      obtain ⟨ℓ, hc⟩ := h.cert t' hcg'
      refine ⟨ℓ, hc.transport (fun m hm => hm) (fun c' hcm => .inl hcm) ?_⟩
      rcases eq_or_ne ℓ i with rfl | hne
      · simp only [Function.update_self]
        exact hc.pinned
      · simp only [Function.update_of_ne hne]
        exact hc.pinned
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro p t' v1 v2 h1 h2
      exact h.coherent p t' v1 v2 (occ_pn_sub h1 rfl) (occ_pn_sub h2 rfl)
    · intro k hk p v' hOcc
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk
        cases hk
      · simp only [Function.update_of_ne hne] at hk hOcc ⊢
        exact h.frontier k hk p v' (occ_pn_sub hOcc rfl)
    · intro k hk es hg
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk
        cases hk
      · simp only [Function.update_of_ne hne] at hk hg ⊢
        exact h.gossip_pinned k hk es hg
    · intro k hk
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk
        cases hk
      · simp only [Function.update_of_ne hne] at hk ⊢
        exact h.map_pinned k hk
    · intro t' hcg
      have hcg' : (∃ p v', Occ w p t' v') ∨
          (∃ es, Frame.gossip t' es ∈ w.dsent) := by
        rcases hcg with ⟨p, v', hOcc⟩ | hg
        · exact .inl ⟨p, v', occ_pn_sub hOcc rfl⟩
        · exact .inr hg
      obtain ⟨ℓ, hc⟩ := h.cert t' hcg'
      refine ⟨ℓ, hc.transport (fun m hm => hm) (fun c' hcm => .inl hcm) ?_⟩
      rcases eq_or_ne ℓ i with rfl | hne
      · simp only [Function.update_self]
        exact pinned_bump (show (w.nodes ℓ).pn.currentTerm <
          ((w.nodes ℓ).pn.adoptTerm t).currentTerm from hterm) hc.pinned
      · simp only [Function.update_of_ne hne]
        exact hc.pinned
  | becomeLeader i hrole hquorum =>
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro p t v v' h1 h2
      exact h.coherent p t v v' (occ_pn_sub h1 rfl) (occ_pn_sub h2 rfl)
    · -- fresh leadership: the certificate blocks any pre-existing content
      -- at this term, so the frontier obligation is vacuous
      intro k hk p v hOcc
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hOcc ⊢
        have hOcc' : Occ w p ((w.nodes k).pn.currentTerm) v :=
          occ_pn_sub hOcc rfl
        obtain ⟨ℓ, hc⟩ := h.cert _ (.inl ⟨p, v, hOcc'⟩)
        exact (cert_blocks_candidate hpInv hrole rfl hquorum hc).elim
      · simp only [Function.update_of_ne hne] at hk hOcc ⊢
        exact h.frontier k hk p v (occ_pn_sub hOcc rfl)
    · intro k hk es hg
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hg ⊢
        have hg' : Frame.gossip ((w.nodes k).pn.currentTerm) es ∈ w.dsent :=
          hg
        obtain ⟨ℓ, hc⟩ := h.cert _ (.inr ⟨es, hg'⟩)
        exact (cert_blocks_candidate hpInv hrole rfl hquorum hc).elim
      · simp only [Function.update_of_ne hne] at hk hg ⊢
        exact h.gossip_pinned k hk es hg
    · intro k hk
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self]
        exact lastTermOf_prunePush _ _ _
      · simp only [Function.update_of_ne hne] at hk ⊢
        exact h.map_pinned k hk
    · intro t hcg
      have hcg' : (∃ p v, Occ w p t v) ∨
          (∃ es, Frame.gossip t es ∈ w.dsent) := by
        rcases hcg with ⟨p, v, hOcc⟩ | hg
        · exact .inl ⟨p, v, occ_pn_sub hOcc rfl⟩
        · exact .inr hg
      obtain ⟨ℓ, hc⟩ := h.cert t hcg'
      refine ⟨ℓ, hc.transport (fun m hm => hm) (fun c' hcm => .inl hcm) ?_⟩
      rcases eq_or_ne ℓ i with rfl | hne
      · -- the enabling `role = .candidate` forces the pre-pin's left arm
        simp only [Function.update_self]
        rcases hc.pinned with hlt | ⟨-, -, hnc⟩
        · exact .inl hlt
        · exact absurd hrole hnc
      · simp only [Function.update_of_ne hne]
        exact hc.pinned
  | crashRestart i =>
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro p t v v' h1 h2
      exact h.coherent p t v v' (occ_pn_sub h1 rfl) (occ_pn_sub h2 rfl)
    · intro k hk p v hOcc
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk
        cases hk
      · simp only [Function.update_of_ne hne] at hk hOcc ⊢
        exact h.frontier k hk p v (occ_pn_sub hOcc rfl)
    · intro k hk es hg
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk
        cases hk
      · simp only [Function.update_of_ne hne] at hk hg ⊢
        exact h.gossip_pinned k hk es hg
    · intro k hk
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk
        cases hk
      · simp only [Function.update_of_ne hne] at hk ⊢
        exact h.map_pinned k hk
    · intro t hcg
      have hcg' : (∃ p v, Occ w p t v) ∨
          (∃ es, Frame.gossip t es ∈ w.dsent) := by
        rcases hcg with ⟨p, v, hOcc⟩ | hg
        · exact .inl ⟨p, v, occ_pn_sub hOcc rfl⟩
        · exact .inr hg
      obtain ⟨ℓ, hc⟩ := h.cert t hcg'
      refine ⟨ℓ, hc.transport (fun m hm => hm) (fun c' hcm => .inl hcm) ?_⟩
      rcases eq_or_ne ℓ i with rfl | hne
      · simp only [Function.update_self]
        rcases hc.pinned with hlt | ⟨heqt, hvf, -⟩
        · exact .inl hlt
        · exact .inr ⟨heqt, hvf, by decide⟩
      · simp only [Function.update_of_ne hne]
        exact hc.pinned
  | leaderAppend i v hrole =>
    -- the ONE creation site: freshness comes from `frontier`
    have hsub : ∀ p t v₀,
        Occ { nodes := Function.update w.nodes i
                { pn := { (w.nodes i).pn with
                    durable := (w.nodes i).pn.durable + 1 }
                  termMap := (w.nodes i).termMap
                  hist := Function.update (w.nodes i).hist
                    (w.nodes i).pn.durable
                    (some ((w.nodes i).pn.currentTerm, v)) }
              sent := w.sent
              dsent := w.dsent ++
                [.replicate (w.nodes i).pn.durable
                  (w.nodes i).pn.currentTerm (w.nodes i).pn.currentTerm v] }
          p t v₀ →
        Occ w p t v₀ ∨
          (p = (w.nodes i).pn.durable ∧
           t = (w.nodes i).pn.currentTerm ∧ v₀ = v) := by
      intro p t v₀ hOcc
      rw [occ_mk] at hOcc
      rcases hOcc with (⟨hdr, hf⟩ | hd) | ⟨k, hne, hk⟩
      · rcases List.mem_append.mp hf with hf | hf
        · exact .inl (.inl ⟨hdr, hf⟩)
        · rw [List.mem_singleton, Frame.replicate.injEq] at hf
          exact .inr ⟨hf.1, hf.2.2.1, hf.2.2.2⟩
      · by_cases hp : p = (w.nodes i).pn.durable
        · subst hp
          have hd' : Function.update (w.nodes i).hist
              ((w.nodes i).pn.durable)
              (some ((w.nodes i).pn.currentTerm, v))
              ((w.nodes i).pn.durable) = some (t, v₀) := hd
          rw [Function.update_self, Option.some.injEq,
            Prod.mk.injEq] at hd'
          exact .inr ⟨rfl, hd'.1.symm, hd'.2.symm⟩
        · have hd' : Function.update (w.nodes i).hist
              ((w.nodes i).pn.durable)
              (some ((w.nodes i).pn.currentTerm, v)) p = some (t, v₀) := hd
          rw [Function.update_of_ne hp] at hd'
          exact .inl (.inr ⟨i, hd'⟩)
      · exact .inl (.inr ⟨k, hk⟩)
    have hfresh : ∀ v₀, ¬ Occ w ((w.nodes i).pn.durable)
        ((w.nodes i).pn.currentTerm) v₀ := by
      intro v₀ hOcc
      exact absurd (h.frontier i hrole _ v₀ hOcc) (lt_irrefl _)
    refine ⟨?_, ?_, ?_, ?_, ?_⟩
    · intro p t v1 v2 h1 h2
      rcases hsub p t v1 h1 with ho1 | ⟨rfl, rfl, rfl⟩
      · rcases hsub p t v2 h2 with ho2 | ⟨rfl, rfl, rfl⟩
        · exact h.coherent p t v1 v2 ho1 ho2
        · exact absurd ho1 (hfresh v1)
      · rcases hsub _ _ v2 h2 with ho2 | ⟨-, -, rfl⟩
        · exact absurd ho2 (hfresh v2)
        · rfl
    · intro k hk p v₀ hOcc
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk hOcc ⊢
        rcases hsub p _ v₀ hOcc with ho | ⟨rfl, -, -⟩
        · exact Nat.lt_succ_of_lt (h.frontier k hrole p v₀ ho)
        · exact Nat.lt_succ_self _
      · simp only [Function.update_of_ne hne] at hk hOcc ⊢
        rcases hsub p _ v₀ hOcc with ho | ⟨rfl, ht, rfl⟩
        · exact h.frontier k hk p v₀ ho
        · exact absurd (election_safety w hw k i hk hrole ht) hne
    · intro k hk es hg
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self] at hk hg ⊢
        rcases List.mem_append.mp hg with hg' | hg'
        · exact h.gossip_pinned k hrole es hg'
        · simp at hg'
      · simp only [Function.update_of_ne hne] at hk hg ⊢
        rcases List.mem_append.mp hg with hg' | hg'
        · exact h.gossip_pinned k hk es hg'
        · simp at hg'
    · intro k hk
      rcases eq_or_ne k i with rfl | hne
      · simp only [Function.update_self]
        exact h.map_pinned k hrole
      · simp only [Function.update_of_ne hne] at hk ⊢
        exact h.map_pinned k hk
    · intro t hcg
      have hkey : ∃ ℓ, Cert w t ℓ := by
        rcases hcg with ⟨p, v₀, hOcc⟩ | ⟨es, hg⟩
        · rcases hsub p t v₀ hOcc with ho | ⟨-, rfl, -⟩
          · exact h.cert t (.inl ⟨p, v₀, ho⟩)
          · exact ⟨i, cert_of_leader hpInv hrole⟩
        · have hg' : Frame.gossip t es ∈ w.dsent := by
            rcases List.mem_append.mp hg with hg | hg
            · exact hg
            · simp at hg
          exact h.cert t (.inr ⟨es, hg'⟩)
      obtain ⟨ℓ, hc⟩ := hkey
      refine ⟨ℓ, hc.transport (fun m hm => hm) (fun c' hcm => .inl hcm) ?_⟩
      rcases eq_or_ne ℓ i with rfl | hne
      · simp only [Function.update_self]
        exact hc.pinned
      · simp only [Function.update_of_ne hne]
        exact hc.pinned
  | deliverReplicate j pos hdr t v hmsg hpos hhdr =>
    -- the old `≤`-guard, re-derived from emission-side truthfulness + the
    -- exact header match (LC1: `stamp ≤ hdr = currentTerm`)
    have hstamp : t ≤ (w.nodes j).pn.currentTerm :=
      hhdr ▸ (reachable_stamp hw).frame_le pos hdr t v hmsg
    have hsub : ∀ p t₀ v₀,
        Occ { nodes := Function.update w.nodes j
                ((w.nodes j).recvReplicate pos t v)
              sent := w.sent
              dsent := w.dsent } p t₀ v₀ → Occ w p t₀ v₀ := by
      intro p t₀ v₀ hOcc
      rw [occ_mk] at hOcc
      rcases hOcc with (hf | hd) | ⟨k, hne, hk⟩
      · exact .inl hf
      · by_cases hp : p = pos
        · subst hp
          have hd' : Function.update (w.nodes j).hist p
              (some (t, v)) p = some (t₀, v₀) := hd
          rw [Function.update_self, Option.some.injEq,
            Prod.mk.injEq] at hd'
          obtain ⟨rfl, rfl⟩ := hd'
          exact .inl ⟨hdr, hmsg⟩
        · have hd' : Function.update (w.nodes j).hist pos
              (some (t, v)) p = some (t₀, v₀) := hd
          rw [Function.update_of_ne hp] at hd'
          exact .inr ⟨j, hd'⟩
      · exact .inr ⟨k, hk⟩
    have hpn := recvReplicate_pn (w.nodes j) pos t v
    refine ⟨coherent_of_sub hsub h, ?_, ?_, ?_, ?_⟩
    · intro k hk p v₀ hOcc
      rcases eq_or_ne k j with rfl | hne
      · simp only [Function.update_self] at hk hOcc ⊢
        rw [hpn.1] at hk
        rw [hpn.2.1] at hOcc
        rw [hpn.2.2.2]
        have hlt := h.frontier k hk p v₀ (hsub _ _ _ hOcc)
        omega
      · simp only [Function.update_of_ne hne] at hk hOcc ⊢
        exact h.frontier k hk p v₀ (hsub _ _ _ hOcc)
    · intro k hk es hg
      rcases eq_or_ne k j with rfl | hne
      · simp only [Function.update_self] at hk hg ⊢
        rw [hpn.1] at hk
        rw [hpn.2.1] at hg
        have hmap : observeTerm (w.nodes k).termMap t pos =
            (w.nodes k).termMap :=
          observeTerm_of_le (by rw [h.map_pinned k hk]; omega) pos
        show es = observeTerm (w.nodes k).termMap t pos
        rw [hmap]
        exact h.gossip_pinned k hk es hg
      · simp only [Function.update_of_ne hne] at hk hg ⊢
        exact h.gossip_pinned k hk es hg
    · intro k hk
      rcases eq_or_ne k j with rfl | hne
      · simp only [Function.update_self] at hk ⊢
        rw [hpn.1] at hk
        rw [hpn.2.1]
        have hmap : observeTerm (w.nodes k).termMap t pos =
            (w.nodes k).termMap :=
          observeTerm_of_le (by rw [h.map_pinned k hk]; omega) pos
        show lastTermOf (observeTerm (w.nodes k).termMap t pos) = _
        rw [hmap]
        exact h.map_pinned k hk
      · simp only [Function.update_of_ne hne] at hk ⊢
        exact h.map_pinned k hk
    · intro t₀ hcg
      have hcg' : (∃ p v₀, Occ w p t₀ v₀) ∨
          (∃ es, Frame.gossip t₀ es ∈ w.dsent) := by
        rcases hcg with ⟨p, v₀, hOcc⟩ | hg
        · exact .inl ⟨p, v₀, hsub _ _ _ hOcc⟩
        · exact .inr hg
      obtain ⟨ℓ, hc⟩ := h.cert t₀ hcg'
      refine ⟨ℓ, hc.transport (fun m hm => hm) (fun c' hcm => .inl hcm) ?_⟩
      rcases eq_or_ne ℓ j with rfl | hne
      · simp only [Function.update_self]
        rw [hpn.2.1, hpn.2.2.1, hpn.1]
        exact hc.pinned
      · simp only [Function.update_of_ne hne]
        exact hc.pinned
  | serveTail i p t v hrole hhist hp =>
    -- pure re-emission: the served frame copies an EXISTING hist occurrence
    -- (`hhist`), so the occurrence set is unchanged and every clause
    -- transports along `hsub` (the routine copy-step the rerun report
    -- predicted)
    have hsub : ∀ p₀ t₀ v₀,
        Occ { nodes := w.nodes, sent := w.sent,
              dsent := w.dsent ++
                [.replicate p (w.nodes i).pn.currentTerm t v] }
          p₀ t₀ v₀ → Occ w p₀ t₀ v₀ := by
      intro p₀ t₀ v₀ hOcc
      rcases hOcc with ⟨hdr, hf⟩ | hk
      · rcases List.mem_append.mp hf with hf | hf
        · exact .inl ⟨hdr, hf⟩
        · rw [List.mem_singleton, Frame.replicate.injEq] at hf
          obtain ⟨rfl, -, rfl, rfl⟩ := hf
          exact .inr ⟨i, hhist⟩
      · exact .inr hk
    refine ⟨coherent_of_sub hsub h, ?_, ?_, h.map_pinned, ?_⟩
    · intro k hk p₀ v₀ hOcc
      exact h.frontier k hk p₀ v₀ (hsub _ _ _ hOcc)
    · intro k hk es hg
      rcases List.mem_append.mp hg with hg | hg
      · exact h.gossip_pinned k hk es hg
      · simp at hg
    · intro t₀ hcg
      have hkey : ∃ ℓ, Cert w t₀ ℓ := by
        rcases hcg with ⟨p₀, v₀, hOcc⟩ | ⟨es, hg⟩
        · exact h.cert t₀ (.inl ⟨p₀, v₀, hsub _ _ _ hOcc⟩)
        · rcases List.mem_append.mp hg with hg | hg
          · exact h.cert t₀ (.inr ⟨es, hg⟩)
          · simp at hg
      obtain ⟨ℓ, hc⟩ := hkey
      exact ⟨ℓ, hc.transport (fun m hm => hm) (fun c' hcm => .inl hcm)
        hc.pinned⟩
  | shipTermMap i hrole =>
    have hsub : ∀ p t v,
        Occ { nodes := w.nodes, sent := w.sent,
              dsent := w.dsent ++
                [.gossip (w.nodes i).pn.currentTerm (w.nodes i).termMap] }
          p t v → Occ w p t v := by
      intro p t v hOcc
      rcases hOcc with ⟨hdr, hf⟩ | hk
      · rcases List.mem_append.mp hf with hf | hf
        · exact .inl ⟨hdr, hf⟩
        · simp at hf
      · exact .inr hk
    refine ⟨coherent_of_sub hsub h, ?_, ?_, h.map_pinned, ?_⟩
    · intro k hk p v hOcc
      exact h.frontier k hk p v (hsub _ _ _ hOcc)
    · intro k hk es hg
      rcases List.mem_append.mp hg with hg | hg
      · exact h.gossip_pinned k hk es hg
      · rw [List.mem_singleton, Frame.gossip.injEq] at hg
        have hki : k = i := election_safety w hw k i hk hrole hg.1
        subst hki
        exact hg.2
    · intro t hcg
      have hkey : ∃ ℓ, Cert w t ℓ := by
        rcases hcg with ⟨p, v, hOcc⟩ | ⟨es, hg⟩
        · exact h.cert t (.inl ⟨p, v, hsub _ _ _ hOcc⟩)
        · rcases List.mem_append.mp hg with hg | hg
          · exact h.cert t (.inr ⟨es, hg⟩)
          · rw [List.mem_singleton, Frame.gossip.injEq] at hg
            exact ⟨i, hg.1 ▸ cert_of_leader hpInv hrole⟩
      obtain ⟨ℓ, hc⟩ := hkey
      exact ⟨ℓ, hc.transport (fun m hm => hm) (fun c' hcm => .inl hcm)
        hc.pinned⟩
  | deliverTermMap j t entries hmsg hterm =>
    have hsub : ∀ p t₀ v₀,
        Occ { nodes := Function.update w.nodes j
                ((w.nodes j).applyGossip t entries)
              sent := w.sent
              dsent := w.dsent } p t₀ v₀ → Occ w p t₀ v₀ := by
      intro p t₀ v₀ hOcc
      rw [occ_mk] at hOcc
      rcases hOcc with (hf | hd) | ⟨k, hne, hk⟩
      · exact .inl hf
      · exact .inr ⟨j, applyGossip_hist _ _ _ _ hd⟩
      · exact .inr ⟨k, hk⟩
    refine ⟨coherent_of_sub hsub h, ?_, ?_, ?_, ?_⟩
    · intro k hk p v₀ hOcc
      rcases eq_or_ne k j with rfl | hne
      · simp only [Function.update_self] at hk hOcc ⊢
        by_cases hadopt : (w.nodes k).pn.currentTerm < t
        · rw [(applyGossip_adopt _ entries hadopt).1] at hk
          cases hk
        · have hna := applyGossip_no_adopt (w.nodes k) entries hadopt
          rw [hna.1] at hk
          have hteq : t = (w.nodes k).pn.currentTerm := by omega
          subst hteq
          have hes : entries = (w.nodes k).termMap :=
            h.gossip_pinned k hk entries hmsg
          subst hes
          rw [hna.2.1] at hOcc
          rw [(applyGossip_self (w.nodes k) _).1]
          exact h.frontier k hk p v₀ (hsub _ _ _ hOcc)
      · simp only [Function.update_of_ne hne] at hk hOcc ⊢
        exact h.frontier k hk p v₀ (hsub _ _ _ hOcc)
    · intro k hk es hg
      rcases eq_or_ne k j with rfl | hne
      · simp only [Function.update_self] at hk hg ⊢
        by_cases hadopt : (w.nodes k).pn.currentTerm < t
        · rw [(applyGossip_adopt _ entries hadopt).1] at hk
          cases hk
        · have hna := applyGossip_no_adopt (w.nodes k) entries hadopt
          rw [hna.1] at hk
          have hteq : t = (w.nodes k).pn.currentTerm := by omega
          subst hteq
          have hes : entries = (w.nodes k).termMap :=
            h.gossip_pinned k hk entries hmsg
          subst hes
          rw [hna.2.1] at hg
          rw [(applyGossip_self (w.nodes k) _).2]
          exact h.gossip_pinned k hk es hg
      · simp only [Function.update_of_ne hne] at hk hg ⊢
        exact h.gossip_pinned k hk es hg
    · intro k hk
      rcases eq_or_ne k j with rfl | hne
      · simp only [Function.update_self] at hk ⊢
        by_cases hadopt : (w.nodes k).pn.currentTerm < t
        · rw [(applyGossip_adopt _ entries hadopt).1] at hk
          cases hk
        · have hna := applyGossip_no_adopt (w.nodes k) entries hadopt
          rw [hna.1] at hk
          have hteq : t = (w.nodes k).pn.currentTerm := by omega
          subst hteq
          have hes : entries = (w.nodes k).termMap :=
            h.gossip_pinned k hk entries hmsg
          subst hes
          rw [(applyGossip_self (w.nodes k) _).2, hna.2.1]
          exact h.map_pinned k hk
      · simp only [Function.update_of_ne hne] at hk ⊢
        exact h.map_pinned k hk
    · intro t₀ hcg
      have hcg' : (∃ p v₀, Occ w p t₀ v₀) ∨
          (∃ es, Frame.gossip t₀ es ∈ w.dsent) := by
        rcases hcg with ⟨p, v₀, hOcc⟩ | hg
        · exact .inl ⟨p, v₀, hsub _ _ _ hOcc⟩
        · exact .inr hg
      obtain ⟨ℓ, hc⟩ := h.cert t₀ hcg'
      refine ⟨ℓ, hc.transport (fun m hm => hm) (fun c' hcm => .inl hcm) ?_⟩
      rcases eq_or_ne ℓ j with rfl | hne
      · simp only [Function.update_self]
        by_cases hadopt : (w.nodes ℓ).pn.currentTerm < t
        · refine pinned_bump ?_ hc.pinned
          rw [(applyGossip_adopt _ entries hadopt).2]
          omega
        · have hna := applyGossip_no_adopt (w.nodes ℓ) entries hadopt
          rw [hna.1, hna.2.1, hna.2.2]
          exact hc.pinned
      · simp only [Function.update_of_ne hne]
        exact hc.pinned

/-- The invariant holds in every reachable world. -/
theorem reachable_dinv {n : Nat} {w : World n} (hw : Reachable w) :
    DInv w := by
  have h : Relation.ReflTransGen Step (World.init n) w := hw
  clear hw
  induction h with
  | refl => exact dinv_init n
  | tail hsteps hstep ih => exact dinv_step hsteps ih hstep

/-! ## LOG MATCHING -/

/-- **LOG MATCHING** (LM-core, statement fixed by the sub-spike brief): in
any reachable world, two nodes holding a stamped record at the same byte
position under the same term hold the same payload — the Raft log-matching
property over byte positions (spec §6: within a term, bytes are
cluster-identical). -/
theorem log_matching {n : Nat} (w : World n) (hw : Reachable w)
    (i j : Fin n) (p : Nat) (t vi vj : Nat)
    (hi : (w.nodes i).hist p = some (t, vi))
    (hj : (w.nodes j).hist p = some (t, vj)) : vi = vj :=
  (reachable_dinv hw).coherent p t vi vj (.inr ⟨i, hi⟩) (.inr ⟨j, hj⟩)

#print axioms log_matching
#print axioms reachable_stamp
#print axioms Cert.transport
#print axioms cert_of_leader
#print axioms cert_blocks_candidate

end Uc2.Data
