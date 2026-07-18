import Mathlib.Data.List.TakeWhile
import Uc2Proofs.Reconcile
import Uc2Proofs.ProtocolCommit

/-! LC2 — `MapsWF`: term-map well-formedness as a world invariant.

The LB2b report flagged that `Uc2.TermMap.Ascending` had NO reachability
invariant anywhere in the corpus — `Ascending.termAt_take` /
`reconcile_ok_newMap_take` (the tools the LC endgame's `becomeLeader` case
needs) demand it as a hypothesis. This module proves it, per the arc's
decision 5: one bundled invariant, per-node `Ascending` ∧ `MapFloor`
(head base 0 + nonempty-under-data), established by induction over the
DATA-plane model and lifted to the commit layer through the projection
(the commit steps are data-plane stutters; the gate plays no role in map
well-formedness).

The bundle (`MapInv` / per-node `NodeWF`) carries more than the two
decision-5 clauses because the induction forces it:

- `last_base` (last entry base ≤ durable) — `deliverReplicate`'s
  `observeTerm` growth appends `(t, pos)` at `pos = durable`, and
  `Ascending` of the append needs the old last base ≤ `pos`.
- `map_le` / `cand_map_lt` / `cand_dt_lt` — `becomeLeader`'s `prunePush`
  appends `(currentTerm, durable)`, and `Ascending` needs the surviving
  last term STRICTLY below the new term: a candidate's map terms sit
  strictly below its (bumped) `currentTerm` because its data plane still
  runs at the lagging `dataTerm` handle (LC1b), which bounds every
  candidate-era `observeTerm` stamp.
- `leader_map` / `nonempty` / `stamp_pos` / `hist_pos` /
  `role_term_pos` — `MapFloor`'s nonemptiness rides on "no stamp-0
  frames" (terms with content are ≥ 1), which in turn rides on leader
  terms being ≥ 1.
- `gossip_wf` (shipped maps are `Ascending`) — `deliverTermMap`'s
  surviving prefix `own.take k` keeps its last base below the NEW
  (smaller) durable `validUpTo` only because the common prefix is also a
  prefix of the (ascending) shipped map, pinning the prefix bases below
  the leader-side clamp boundary.

`frames_current_authored` — the folded-in LC1 discharge this task was
also to prove — is REFUTED (Finding #9, machine-checked in
`LeaderCompleteness.lean`: the gate-reopen keying admits a candidate-
window cross-stream accept). It is therefore ABSENT here, per the
stuck-protocol; see the LC2 task report. -/

namespace Uc2

namespace TermMap

/-- In an ascending map the head's base bounds every later base (the base
analog of `Ascending.head_term_lt`). -/
theorem Ascending.head_base_le :
    ∀ {l : TermMap} {a : Nat × Nat}, Ascending (a :: l) →
      ∀ {j : Nat} {e : Nat × Nat}, l[j]? = some e → a.2 ≤ e.2 := by
  intro l
  induction l with
  | nil => intro a _ j e hj; simp at hj
  | cons b t ih =>
    intro a hwf j e hj
    cases j with
    | zero =>
      rw [List.getElem?_cons_zero] at hj
      injection hj with hbe
      subst hbe
      exact hwf.2.1
    | succ j' =>
      rw [List.getElem?_cons_succ] at hj
      exact Nat.le_trans hwf.2.1 (ih hwf.2.2 hj)

/-- Every entry in the `k`-prefix has base ≤ the `k`-th entry's base (the
base analog of `Ascending.take_term_lt`). -/
theorem Ascending.take_base_le :
    ∀ {m : TermMap}, Ascending m → ∀ {k : Nat} {e : Nat × Nat},
      m[k]? = some e → ∀ x ∈ m.take k, x.2 ≤ e.2 := by
  intro m
  induction m with
  | nil => intro _ k e hk; simp at hk
  | cons a t ih =>
    intro hwf k e hk x hx
    cases k with
    | zero => simp at hx
    | succ j =>
      rw [List.getElem?_cons_succ] at hk
      rw [List.take_succ_cons] at hx
      rcases List.mem_cons.mp hx with rfl | hxt
      · exact hwf.head_base_le hk
      · exact ih hwf.tail hk x hxt

/-- Every base is bounded by the last entry's base. -/
theorem Ascending.base_le_getLast :
    ∀ {m : TermMap}, Ascending m → ∀ {l : Nat × Nat}, m.getLast? = some l →
      ∀ e ∈ m, e.2 ≤ l.2 := by
  intro m
  induction m with
  | nil => intro _ l hl; simp at hl
  | cons a t ih =>
    intro hwf l hl e he
    cases t with
    | nil =>
      rw [List.getLast?_singleton] at hl
      injection hl with hal
      rcases List.mem_cons.mp he with rfl | het
      · rw [← hal]
      · cases het
    | cons b t' =>
      rw [List.getLast?_cons_cons] at hl
      rcases List.mem_cons.mp he with rfl | het
      · exact Nat.le_trans hwf.2.1 (ih hwf.2.2 hl b List.mem_cons_self)
      · exact ih hwf.2.2 hl e het

/-- Cons-introduction: prepending below the head preserves `Ascending`. -/
theorem ascending_cons_of {a : Nat × Nat} {l : TermMap}
    (hl : Ascending l) (h : ∀ b, l.head? = some b → a.1 < b.1 ∧ a.2 ≤ b.2) :
    Ascending (a :: l) := by
  cases l with
  | nil => trivial
  | cons b t => exact ⟨(h b rfl).1, (h b rfl).2, hl⟩

/-- A prefix of an ascending map is ascending. -/
theorem Ascending.take : ∀ {m : TermMap}, Ascending m →
    ∀ (k : Nat), Ascending (m.take k) := by
  intro m
  induction m with
  | nil => intro _ k; rw [List.take_nil]; trivial
  | cons a t ih =>
    intro hwf k
    cases k with
    | zero => trivial
    | succ k' =>
      rw [List.take_succ_cons]
      refine ascending_cons_of (ih hwf.tail k') ?_
      intro b hb
      cases t with
      | nil => simp at hb
      | cons c t' =>
        cases k' with
        | zero => simp at hb
        | succ k'' =>
          rw [List.take_succ_cons, List.head?_cons] at hb
          injection hb with hcb
          subst hcb
          exact ⟨hwf.1, hwf.2.1⟩

/-- Snoc-introduction: appending strictly above the last term with a base
at least the last base preserves `Ascending`. -/
theorem Ascending.snoc : ∀ {m : TermMap} {t b : Nat}, Ascending m →
    (∀ l, m.getLast? = some l → l.1 < t ∧ l.2 ≤ b) →
    Ascending (m ++ [(t, b)])
  | [], _, _, _, _ => trivial
  | [a], t, b, _, h => by
    have ha := h a List.getLast?_singleton
    exact ⟨ha.1, ha.2, trivial⟩
  | a :: c :: m', t, b, hwf, h =>
    ⟨hwf.1, hwf.2.1,
      Ascending.snoc hwf.2.2 fun l hl =>
        h l (by rw [List.getLast?_cons_cons]; exact hl)⟩

end TermMap

namespace Data

/-- Decision 5's `mapFloor`: the map's coverage floor. Head base 0 whenever
the map is nonempty, and the map IS nonempty whenever any byte is durable —
jointly: every position `p < durable` is covered by some entry with
base ≤ `p` (see `Uc2.Cert.MapsWF.covered`). -/
def MapFloor {n : Nat} (nd : Node n) : Prop :=
  (∀ e, nd.termMap.head? = some e → e.2 = 0) ∧
  (0 < nd.pn.durable → nd.termMap ≠ [])

/-- The per-node slice of the LC2 well-formedness bundle. The first four
fields are the decision-5 payload (`asc` + `MapFloor` + the frontier bound);
the rest are the support clauses the induction forces (module doc). -/
structure NodeWF {n : Nat} (nd : Node n) : Prop where
  /-- The Rust construction-site invariant: terms strictly ascend, bases
  non-strictly. -/
  asc : TermMap.Ascending nd.termMap
  /-- Head base 0 on a nonempty map. -/
  floor0 : ∀ e, nd.termMap.head? = some e → e.2 = 0
  /-- Durable bytes imply a nonempty map. -/
  nonempty : 0 < nd.pn.durable → nd.termMap ≠ []
  /-- The map's frontier entry never sits beyond the durable frontier. -/
  last_base : ∀ e, nd.termMap.getLast? = some e → e.2 ≤ nd.pn.durable
  /-- A leader's map is never empty (`becomeLeader` pushed an entry). -/
  leader_map : nd.pn.role = .leader → nd.termMap ≠ []
  /-- Candidates and leaders live at term ≥ 1 (`startElection` bumps). -/
  role_term_pos : nd.pn.role ≠ .follower → 1 ≤ nd.pn.currentTerm
  /-- Map terms never exceed the node's current term. -/
  map_le : ∀ e ∈ nd.termMap, e.1 ≤ nd.pn.currentTerm
  /-- A candidate's data-plane handle strictly lags its bumped term
  (LC1b: `startElection` stores nothing). -/
  cand_dt_lt : nd.pn.role = .candidate → nd.dataTerm < nd.pn.currentTerm
  /-- A candidate's map terms sit strictly below its bumped term (its
  candidate-era intake is scoped to the lagging handle). -/
  cand_map_lt : nd.pn.role = .candidate →
      ∀ e ∈ nd.termMap, e.1 < nd.pn.currentTerm
  /-- Held stamps are ≥ 1 (term 0 is the genesis sentinel). -/
  hist_pos : ∀ p t v, nd.hist p = some (t, v) → 1 ≤ t

/-- The LC2 world bundle: per-node `NodeWF` plus the two wire clauses.
LC3 extends this bundle (arc decision 6). -/
structure MapInv {n : Nat} (w : World n) : Prop where
  node : ∀ j, NodeWF (w.nodes j)
  /-- Replicate-frame stamps are ≥ 1 (no term-0 content on the wire). -/
  stamp_pos : ∀ p hdr t v, Frame.replicate p hdr t v ∈ w.dsent → 1 ≤ t
  /-- Shipped term maps are ascending (they are leader maps). -/
  gossip_wf : ∀ t es, Frame.gossip t es ∈ w.dsent → TermMap.Ascending es

/-! ### Local toolkit (kernel/model bridges)

`commonPrefixLen_self`/`reconcile_self` and the four `recvRequestVote`
characterizations are re-proved private copies of `LogMatching.lean`'s
(private there — the LB2b `advance_fires` precedent). -/

private theorem commonPrefixLen_self : ∀ m : TermMap,
    commonPrefixLen m m = m.length
  | [] => rfl
  | _ :: es => by
    simp [commonPrefixLen, commonPrefixLen_self es]

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

private theorem lastTermOf_getLast {m : TermMap} {l : Nat × Nat}
    (h : m.getLast? = some l) : lastTermOf m = l.1 := by
  simp [lastTermOf, h]

private theorem getLast?_append_singleton {α : Type _} :
    ∀ (l : List α) (a : α), (l ++ [a]).getLast? = some a
  | [], _ => rfl
  | x :: xs, a => by
    cases xs with
    | nil => rfl
    | cons y ys =>
      rw [List.cons_append, List.cons_append, List.getLast?_cons_cons]
      exact getLast?_append_singleton (y :: ys) a

/-- The pruned slice of `prunePush` is a PREFIX of the original map
(reverse-dropWhile-reverse drops a suffix). -/
private theorem prunePush_prefix (m : TermMap) (d : Nat) :
    (m.reverse.dropWhile (fun e => e.2 == d)).reverse <+: m := by
  rw [← List.reverse_suffix, List.reverse_reverse]
  exact List.dropWhile_suffix _

/-- `become_leader`'s pruned push preserves the whole per-node
well-formedness payload: given candidate-grade inputs (ascending, floored,
frontier-bounded, terms strictly below the new term `c`), the pushed map is
ascending, floored, nonempty, ends exactly at `(c, d)`, and keeps terms
≤ `c`. -/
private theorem prunePush_wf {m : TermMap} {c d : Nat}
    (hasc : TermMap.Ascending m)
    (hflr : ∀ e, m.head? = some e → e.2 = 0)
    (hne : 0 < d → m ≠ [])
    (hlast : ∀ e, m.getLast? = some e → e.2 ≤ d)
    (hlt : ∀ e ∈ m, e.1 < c) :
    TermMap.Ascending (prunePush m c d) ∧
    (∀ e, (prunePush m c d).head? = some e → e.2 = 0) ∧
    prunePush m c d ≠ [] ∧
    (prunePush m c d).getLast? = some (c, d) ∧
    (∀ e ∈ prunePush m c d, e.1 ≤ c) := by
  have hpp : prunePush m c d
      = (m.reverse.dropWhile (fun e => e.2 == d)).reverse ++ [(c, d)] := rfl
  have hpre : (m.reverse.dropWhile (fun e => e.2 == d)).reverse <+: m :=
    prunePush_prefix m d
  have hsub := hpre.subset
  have hasc_p : TermMap.Ascending
      (m.reverse.dropWhile (fun e => e.2 == d)).reverse := by
    have h1 := hasc.take
      (m.reverse.dropWhile (fun e => e.2 == d)).reverse.length
    rwa [← List.prefix_iff_eq_take.mp hpre] at h1
  have hbase : ∀ x ∈ (m.reverse.dropWhile (fun e => e.2 == d)).reverse,
      x.2 ≤ d := by
    intro x hx
    cases hlo : m.getLast? with
    | none =>
      rw [List.getLast?_eq_none_iff] at hlo
      subst hlo
      exact absurd (hsub hx) List.not_mem_nil
    | some l =>
      exact Nat.le_trans (hasc.base_le_getLast hlo x (hsub hx)) (hlast l hlo)
  refine ⟨?_, ?_, ?_, ?_, ?_⟩
  · rw [hpp]
    exact hasc_p.snoc fun l hl =>
      ⟨hlt l (hsub (List.mem_of_getLast? hl)),
       hbase l (List.mem_of_getLast? hl)⟩
  · intro e he
    rw [hpp] at he
    cases hp : (m.reverse.dropWhile (fun e => e.2 == d)).reverse with
    | nil =>
      rw [hp, List.nil_append, List.head?_cons] at he
      injection he with he'
      subst he'
      -- the WHOLE map was same-base phantoms at `d` — its floor pins d = 0
      rw [List.reverse_eq_nil_iff, List.dropWhile_eq_nil_iff] at hp
      cases hm : m with
      | nil =>
        have hd0 : ¬ 0 < d := fun hd => hne hd hm
        omega
      | cons a as =>
        have ha : (a.2 == d) = true :=
          hp a (by rw [hm]; exact List.mem_reverse.mpr List.mem_cons_self)
        have ha0 : a.2 = 0 := hflr a (by rw [hm]; rfl)
        have had : a.2 = d := by simpa using ha
        omega
    | cons y ys =>
      rw [hp, List.cons_append, List.head?_cons] at he
      injection he with he'
      subst he'
      have hpre' : (y :: ys) <+: m := hp ▸ hpre
      cases hm : m with
      | nil =>
        rw [hm] at hpre'
        exact absurd (List.prefix_nil.mp hpre') (by simp)
      | cons z zs =>
        rw [hm] at hpre'
        obtain ⟨rfl, -⟩ := List.cons_prefix_cons.mp hpre'
        exact hflr y (by rw [hm]; rfl)
  · rw [hpp]
    simp
  · rw [hpp]
    exact getLast?_append_singleton _ _
  · intro e he
    rw [hpp] at he
    rcases List.mem_append.mp he with he | he
    · exact Nat.le_of_lt (hlt e (hsub he))
    · rw [List.mem_singleton] at he
      subst he
      exact Nat.le_refl c

/-- `reconcileClamped`'s `validUpTo` is bounded BELOW by anything below the
durable and both boundary bases (the `≤` twin of R2's strict form). -/
private theorem reconcileClamped_ge {own leader : TermMap} {d k : Nat}
    {o : Outcome} (p : Nat)
    (h : reconcile.reconcileClamped own d leader k = .ok o)
    (hp : p ≤ d)
    (hown : ∀ e, own[k]? = some e → p ≤ e.2)
    (hldr : ∀ e, leader[k]? = some e → p ≤ e.2) :
    p ≤ o.validUpTo := by
  obtain ⟨v, mm⟩ := o
  dsimp only [Outcome.validUpTo]
  rcases ho : own[k]? with _ | e <;>
    rcases hl : leader[k]? with _ | f <;>
    simp only [reconcile.reconcileClamped, ho, hl, ReconcileResult.ok.injEq,
      Outcome.mk.injEq] at h <;>
    obtain ⟨hv, hm⟩ := h <;>
    subst hv
  · exact hp
  · have hf := hldr f hl
    simp only [Nat.min_def]
    split_ifs <;> omega
  · have he := hown e ho
    simp only [Nat.min_def]
    split_ifs <;> omega
  · have he := hown e ho
    have hf := hldr f hl
    simp only [Nat.min_def]
    split_ifs <;> omega

/-- Every surviving entry of a clean reconcile (against an ASCENDING
shipped map) keeps its base at or below the new frontier `validUpTo` — the
`last_base` transport across `deliverTermMap`. -/
private theorem newMap_base_le {own : TermMap} {d : Nat} {l0 : Nat × Nat}
    {ls : TermMap} {o : Outcome}
    (hwf : TermMap.Ascending own) (hlwf : TermMap.Ascending (l0 :: ls))
    (hlast : ∀ l, own.getLast? = some l → l.2 ≤ d)
    (h : reconcile own d (l0 :: ls) = .ok o) :
    ∀ x ∈ o.newMap, x.2 ≤ o.validUpTo := by
  intro x hx
  have hmap : o.newMap = own.take (commonPrefixLen own (l0 :: ls)) :=
    reconcile_ok_newMap_take hwf h
  rw [hmap] at hx
  have hxm : x ∈ own := List.mem_of_mem_take hx
  refine reconcileClamped_ge x.2 (reconcile_ok_clamped h) ?_ ?_ ?_
  · cases hlo : own.getLast? with
    | none =>
      rw [List.getLast?_eq_none_iff] at hlo
      subst hlo
      exact absurd hxm List.not_mem_nil
    | some l =>
      exact Nat.le_trans (hwf.base_le_getLast hlo x hxm) (hlast l hlo)
  · intro e he
    exact hwf.take_base_le he x hx
  · intro f hf
    have hx' : x ∈ (l0 :: ls).take (commonPrefixLen own (l0 :: ls)) := by
      rw [← take_commonPrefixLen_eq own (l0 :: ls)]
      exact hx
    exact hlwf.take_base_le hf x hx'

/-- `applyGossip`'s field values on a clean reconcile. -/
private theorem applyGossip_ok {n : Nat} (nd : Node n) (t : Nat)
    {entries : TermMap} {o : Outcome}
    (hrec : reconcile nd.termMap nd.pn.durable entries = .ok o) :
    (nd.applyGossip t entries).termMap = o.newMap ∧
    (nd.applyGossip t entries).pn.durable = o.validUpTo ∧
    ((nd.applyGossip t entries).hist
      = fun p => if p < o.validUpTo then nd.hist p else none) ∧
    (nd.applyGossip t entries).pn.role
      = (if nd.pn.currentTerm < t then Role.follower else nd.pn.role) ∧
    (nd.applyGossip t entries).pn.currentTerm
      = (if nd.pn.currentTerm < t then t else nd.pn.currentTerm) ∧
    (nd.applyGossip t entries).dataTerm
      = (if nd.pn.currentTerm < t then t else nd.dataTerm) := by
  by_cases hadopt : nd.pn.currentTerm < t <;>
    simp [Node.applyGossip, hrec, hadopt, PNode.adoptTerm]

/-- `applyGossip`'s field values on the wipe arm. -/
private theorem applyGossip_ncp {n : Nat} (nd : Node n) (t : Nat)
    {entries : TermMap}
    (hrec : reconcile nd.termMap nd.pn.durable entries = .noCommonPrefix) :
    (nd.applyGossip t entries).termMap = [] ∧
    (nd.applyGossip t entries).pn.durable = 0 ∧
    ((nd.applyGossip t entries).hist = fun _ => none) ∧
    (nd.applyGossip t entries).pn.role
      = (if nd.pn.currentTerm < t then Role.follower else nd.pn.role) ∧
    (nd.applyGossip t entries).pn.currentTerm
      = (if nd.pn.currentTerm < t then t else nd.pn.currentTerm) ∧
    (nd.applyGossip t entries).dataTerm
      = (if nd.pn.currentTerm < t then t else nd.dataTerm) := by
  by_cases hadopt : nd.pn.currentTerm < t <;>
    simp [Node.applyGossip, hrec, hadopt, PNode.adoptTerm]

private theorem recvReplicate_fields {n : Nat} (nd : Node n) (pos t v : Nat) :
    (nd.recvReplicate pos t v).termMap = observeTerm nd.termMap t pos ∧
    (nd.recvReplicate pos t v).pn.durable = pos + 1 ∧
    (nd.recvReplicate pos t v).hist
      = Function.update nd.hist pos (some (t, v)) ∧
    (nd.recvReplicate pos t v).pn.role = nd.pn.role ∧
    (nd.recvReplicate pos t v).pn.currentTerm = nd.pn.currentTerm ∧
    (nd.recvReplicate pos t v).dataTerm = nd.dataTerm := by
  simp [Node.recvReplicate]

/-- Rebuild `NodeWF` across an update that leaves the data plane
(termMap, hist, durable) untouched, given role/term transport. -/
private theorem NodeWF.pn_step {n : Nat} {nd nd' : Node n} (h : NodeWF nd)
    (hmap : nd'.termMap = nd.termMap)
    (hhist : nd'.hist = nd.hist)
    (hdur : nd'.pn.durable = nd.pn.durable)
    (hleader : nd'.pn.role = .leader → nd.termMap ≠ [])
    (hrtp : nd'.pn.role ≠ .follower → 1 ≤ nd'.pn.currentTerm)
    (hmle : ∀ e ∈ nd.termMap, e.1 ≤ nd'.pn.currentTerm)
    (hcdt : nd'.pn.role = .candidate → nd'.dataTerm < nd'.pn.currentTerm)
    (hcml : nd'.pn.role = .candidate →
      ∀ e ∈ nd.termMap, e.1 < nd'.pn.currentTerm) :
    NodeWF nd' := by
  refine ⟨?_, ?_, ?_, ?_, ?_, hrtp, ?_, hcdt, ?_, ?_⟩
  · rw [hmap]; exact h.asc
  · intro e he; rw [hmap] at he; exact h.floor0 e he
  · rw [hdur, hmap]; exact h.nonempty
  · intro e he; rw [hmap] at he; rw [hdur]; exact h.last_base e he
  · intro hr; rw [hmap]; exact hleader hr
  · intro e he; rw [hmap] at he; exact hmle e he
  · intro hr e he; rw [hmap] at he; exact hcml hr e he
  · intro p t v hh; rw [hhist] at hh; exact h.hist_pos p t v hh

private theorem minv_init (n : Nat) : MapInv (World.init n) where
  node j :=
    { asc := trivial
      floor0 := by intro e he; cases he
      nonempty := fun hd => (Nat.lt_irrefl 0 hd).elim
      last_base := by intro e he; cases he
      leader_map := fun hr => nomatch hr
      role_term_pos := fun hr => absurd rfl hr
      map_le := by intro e he; cases he
      cand_dt_lt := fun hr => nomatch hr
      cand_map_lt := by intro hr e he; cases he
      hist_pos := by intro p t v hh; cases hh }
  stamp_pos := by intro p hdr t v hf; cases hf
  gossip_wf := by intro t es hf; cases hf

private theorem minv_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : MapInv w) (hs : Step w w') : MapInv w' := by
  have hstamp := reachable_stamp hw
  cases hs with
  | startElection i hrole =>
    refine ⟨?_, h.stamp_pos, h.gossip_wf⟩
    intro k
    show NodeWF (Function.update w.nodes i _ k)
    rcases eq_or_ne k i with rfl | hne
    · rw [Function.update_self]
      have hn := h.node k
      exact hn.pn_step rfl rfl rfl (fun hr => nomatch hr)
        (fun _ => Nat.le_add_left 1 _)
        (fun e he => Nat.le_succ_of_le (hn.map_le e he))
        (fun _ => Nat.lt_succ_of_le (hstamp.data_le k))
        (fun _ e he => Nat.lt_succ_of_le (hn.map_le e he))
    · rw [Function.update_of_ne hne]
      exact h.node k
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    refine ⟨?_, h.stamp_pos, h.gossip_wf⟩
    intro k
    show NodeWF (Function.update w.nodes j _ k)
    rcases eq_or_ne k j with rfl | hne
    · rw [Function.update_self]
      have hn := h.node k
      have hdur := recv_durable (w.nodes k).pn c nt clt cd
      by_cases hadopt : (w.nodes k).pn.currentTerm < nt
      · have hro := recv_adopt_role (w.nodes k).pn c nt clt cd hadopt
        have hct := recv_term (w.nodes k).pn c nt clt cd hterm
        refine hn.pn_step rfl rfl hdur ?_ ?_ ?_ ?_ ?_
        · show ((w.nodes k).pn.recvRequestVote c nt clt cd).1.role = .leader →
            (w.nodes k).termMap ≠ []
          rw [hro]
          exact fun hr => nomatch hr
        · show ((w.nodes k).pn.recvRequestVote c nt clt cd).1.role
              ≠ .follower →
            1 ≤ ((w.nodes k).pn.recvRequestVote c nt clt cd).1.currentTerm
          rw [hro]
          exact fun hr => absurd rfl hr
        · show ∀ e ∈ (w.nodes k).termMap,
            e.1 ≤ ((w.nodes k).pn.recvRequestVote c nt clt cd).1.currentTerm
          rw [hct]
          exact fun e he => Nat.le_trans (hn.map_le e he) hterm
        · show ((w.nodes k).pn.recvRequestVote c nt clt cd).1.role
              = .candidate →
            (if (w.nodes k).pn.currentTerm < nt then nt
              else (w.nodes k).dataTerm)
              < ((w.nodes k).pn.recvRequestVote c nt clt cd).1.currentTerm
          rw [hro]
          exact fun hr => nomatch hr
        · show ((w.nodes k).pn.recvRequestVote c nt clt cd).1.role
              = .candidate →
            ∀ e ∈ (w.nodes k).termMap,
              e.1 < ((w.nodes k).pn.recvRequestVote c nt clt cd).1.currentTerm
          rw [hro]
          exact fun hr => nomatch hr
      · have hfr := recv_frame (w.nodes k).pn c nt clt cd hadopt
        refine hn.pn_step rfl rfl hdur ?_ ?_ ?_ ?_ ?_
        · show ((w.nodes k).pn.recvRequestVote c nt clt cd).1.role = .leader →
            (w.nodes k).termMap ≠ []
          rw [hfr.1]
          exact hn.leader_map
        · show ((w.nodes k).pn.recvRequestVote c nt clt cd).1.role
              ≠ .follower →
            1 ≤ ((w.nodes k).pn.recvRequestVote c nt clt cd).1.currentTerm
          rw [hfr.1, hfr.2]
          exact hn.role_term_pos
        · show ∀ e ∈ (w.nodes k).termMap,
            e.1 ≤ ((w.nodes k).pn.recvRequestVote c nt clt cd).1.currentTerm
          rw [hfr.2]
          exact hn.map_le
        · show ((w.nodes k).pn.recvRequestVote c nt clt cd).1.role
              = .candidate →
            (if (w.nodes k).pn.currentTerm < nt then nt
              else (w.nodes k).dataTerm)
              < ((w.nodes k).pn.recvRequestVote c nt clt cd).1.currentTerm
          rw [hfr.1, hfr.2, if_neg hadopt]
          exact hn.cand_dt_lt
        · show ((w.nodes k).pn.recvRequestVote c nt clt cd).1.role
              = .candidate →
            ∀ e ∈ (w.nodes k).termMap,
              e.1 < ((w.nodes k).pn.recvRequestVote c nt clt cd).1.currentTerm
          rw [hfr.1, hfr.2]
          exact hn.cand_map_lt
    · rw [Function.update_of_ne hne]
      exact h.node k
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    exact ⟨h.node, h.stamp_pos, h.gossip_wf⟩
  | deliverVote i v t hmsg hrole hterm =>
    refine ⟨?_, h.stamp_pos, h.gossip_wf⟩
    intro k
    show NodeWF (Function.update w.nodes i _ k)
    rcases eq_or_ne k i with rfl | hne
    · rw [Function.update_self]
      have hn := h.node k
      exact hn.pn_step rfl rfl rfl hn.leader_map hn.role_term_pos hn.map_le
        hn.cand_dt_lt hn.cand_map_lt
    · rw [Function.update_of_ne hne]
      exact h.node k
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    refine ⟨?_, h.stamp_pos, h.gossip_wf⟩
    intro k
    show NodeWF (Function.update w.nodes i _ k)
    rcases eq_or_ne k i with rfl | hne
    · rw [Function.update_self]
      have hn := h.node k
      exact hn.pn_step rfl rfl rfl (fun hr => nomatch hr)
        (fun hr => absurd rfl hr)
        (fun e he => Nat.le_trans (hn.map_le e he) (Nat.le_of_lt hterm))
        (fun hr => nomatch hr) (fun hr => nomatch hr)
    · rw [Function.update_of_ne hne]
      exact h.node k
  | becomeLeader i hrole hquorum =>
    refine ⟨?_, h.stamp_pos, h.gossip_wf⟩
    intro k
    show NodeWF (Function.update w.nodes i _ k)
    rcases eq_or_ne k i with rfl | hne
    · rw [Function.update_self]
      have hn := h.node k
      obtain ⟨pasc, pflr, pne, plast, ple⟩ :=
        prunePush_wf hn.asc hn.floor0 hn.nonempty hn.last_base
          (hn.cand_map_lt hrole)
      refine ⟨pasc, pflr, fun _ => pne, ?_, fun _ => pne, ?_, ple,
        (fun hr => nomatch hr), (fun hr => nomatch hr), hn.hist_pos⟩
      · intro e he
        replace he : (prunePush (w.nodes k).termMap
            (w.nodes k).pn.currentTerm (w.nodes k).pn.durable).getLast?
            = some e := he
        rw [plast] at he
        cases he
        exact Nat.le_refl _
      · exact fun _ =>
          hn.role_term_pos fun hh => nomatch (hrole.symm.trans hh)
    · rw [Function.update_of_ne hne]
      exact h.node k
  | crashRestart i =>
    refine ⟨?_, h.stamp_pos, h.gossip_wf⟩
    intro k
    show NodeWF (Function.update w.nodes i _ k)
    rcases eq_or_ne k i with rfl | hne
    · rw [Function.update_self]
      have hn := h.node k
      exact hn.pn_step rfl rfl rfl (fun hr => nomatch hr)
        (fun hr => absurd rfl hr) hn.map_le (fun hr => nomatch hr)
        (fun hr => nomatch hr)
    · rw [Function.update_of_ne hne]
      exact h.node k
  | leaderAppend i v hrole =>
    have hc1 : 1 ≤ (w.nodes i).pn.currentTerm :=
      (h.node i).role_term_pos fun hh => nomatch (hrole.symm.trans hh)
    refine ⟨?_, ?_, ?_⟩
    · intro k
      show NodeWF (Function.update w.nodes i _ k)
      rcases eq_or_ne k i with rfl | hne
      · rw [Function.update_self]
        have hn := h.node k
        refine ⟨hn.asc, hn.floor0, fun _ => hn.leader_map hrole,
          fun e he => Nat.le_succ_of_le (hn.last_base e he),
          fun _ => hn.leader_map hrole, hn.role_term_pos, hn.map_le,
          (fun hr => nomatch (hrole.symm.trans hr)),
          (fun hr => nomatch (hrole.symm.trans hr)), ?_⟩
        intro p t' v' hh
        replace hh : Function.update (w.nodes k).hist (w.nodes k).pn.durable
            (some ((w.nodes k).pn.currentTerm, v)) p = some (t', v') := hh
        by_cases hp : p = (w.nodes k).pn.durable
        · subst hp
          rw [Function.update_self, Option.some.injEq, Prod.mk.injEq] at hh
          omega
        · rw [Function.update_of_ne hp] at hh
          exact hn.hist_pos p t' v' hh
      · rw [Function.update_of_ne hne]
        exact h.node k
    · intro p hdr t' v' hf
      rcases List.mem_append.mp hf with hf | hf
      · exact h.stamp_pos p hdr t' v' hf
      · rw [List.mem_singleton, Frame.replicate.injEq] at hf
        omega
    · intro t' es hf
      rcases List.mem_append.mp hf with hf | hf
      · exact h.gossip_wf t' es hf
      · simp at hf
  | deliverReplicate j pos hdr t v hmsg hpos hhdr =>
    have ht1 : 1 ≤ t := h.stamp_pos pos hdr t v hmsg
    have hthdr : t ≤ hdr := hstamp.frame_le pos hdr t v hmsg
    refine ⟨?_, h.stamp_pos, h.gossip_wf⟩
    intro k
    show NodeWF (Function.update w.nodes j _ k)
    rcases eq_or_ne k j with rfl | hne
    · rw [Function.update_self]
      have hn := h.node k
      have hdl := hstamp.data_le k
      obtain ⟨hm, hd, hhst, hro, hct, hdt⟩ :=
        recvReplicate_fields (w.nodes k) pos t v
      by_cases hg : lastTermOf (w.nodes k).termMap < t
      · -- growth arm: the map opens the new term at the frontier
        have hm' : observeTerm (w.nodes k).termMap t pos
            = (w.nodes k).termMap ++ [(t, pos)] := by
          simp [observeTerm, hg]
        refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
        · rw [hm, hm']
          exact hn.asc.snoc fun l hl =>
            ⟨lastTermOf_getLast hl ▸ hg,
             by rw [hpos]; exact hn.last_base l hl⟩
        · intro e he
          rw [hm, hm'] at he
          cases hmm : (w.nodes k).termMap with
          | nil =>
            rw [hmm, List.nil_append, List.head?_cons] at he
            injection he with he'
            subst he'
            have hd0 : ¬ 0 < (w.nodes k).pn.durable :=
              fun hlt => hn.nonempty hlt hmm
            omega
          | cons a as =>
            rw [hmm, List.cons_append, List.head?_cons] at he
            injection he with he'
            subst he'
            exact hn.floor0 a (by rw [hmm]; rfl)
        · rw [hd, hm, hm']
          intro _
          simp
        · intro e he
          rw [hm, hm', getLast?_append_singleton] at he
          cases he
          rw [hd]
          exact Nat.le_succ pos
        · intro hr
          rw [hm, hm']
          simp
        · rw [hro, hct]
          exact hn.role_term_pos
        · intro e he
          rw [hct]
          rw [hm, hm'] at he
          rcases List.mem_append.mp he with he | he
          · exact hn.map_le e he
          · rw [List.mem_singleton] at he
            subst he
            omega
        · rw [hro, hct, hdt]
          exact hn.cand_dt_lt
        · intro hr e he
          rw [hro] at hr
          rw [hct]
          rw [hm, hm'] at he
          rcases List.mem_append.mp he with he | he
          · exact hn.cand_map_lt hr e he
          · rw [List.mem_singleton] at he
            subst he
            have := hn.cand_dt_lt hr
            omega
        · intro p t' v' hh
          rw [hhst] at hh
          by_cases hp : p = pos
          · subst hp
            rw [Function.update_self, Option.some.injEq, Prod.mk.injEq] at hh
            omega
          · rw [Function.update_of_ne hp] at hh
            exact hn.hist_pos p t' v' hh
      · -- idempotent arm: the stamp is inside the mapped region
        have hm'' : observeTerm (w.nodes k).termMap t pos
            = (w.nodes k).termMap := by
          simp [observeTerm, hg]
        refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
        · rw [hm, hm'']
          exact hn.asc
        · intro e he
          rw [hm, hm''] at he
          exact hn.floor0 e he
        · rw [hd, hm, hm'']
          intro _ hnil
          rw [hnil] at hg
          simp [lastTermOf] at hg
          omega
        · intro e he
          rw [hm, hm''] at he
          rw [hd]
          have := hn.last_base e he
          omega
        · intro hr
          rw [hro] at hr
          rw [hm, hm'']
          exact hn.leader_map hr
        · rw [hro, hct]
          exact hn.role_term_pos
        · intro e he
          rw [hct]
          rw [hm, hm''] at he
          exact hn.map_le e he
        · rw [hro, hct, hdt]
          exact hn.cand_dt_lt
        · intro hr e he
          rw [hro] at hr
          rw [hct]
          rw [hm, hm''] at he
          exact hn.cand_map_lt hr e he
        · intro p t' v' hh
          rw [hhst] at hh
          by_cases hp : p = pos
          · subst hp
            rw [Function.update_self, Option.some.injEq, Prod.mk.injEq] at hh
            omega
          · rw [Function.update_of_ne hp] at hh
            exact hn.hist_pos p t' v' hh
    · rw [Function.update_of_ne hne]
      exact h.node k
  | serveTail i p t v hrole hhist hp =>
    refine ⟨h.node, ?_, ?_⟩
    · intro p' hdr t' v' hf
      rcases List.mem_append.mp hf with hf | hf
      · exact h.stamp_pos p' hdr t' v' hf
      · rw [List.mem_singleton, Frame.replicate.injEq] at hf
        rw [hf.2.2.1]
        exact (h.node i).hist_pos p t v hhist
    · intro t' es hf
      rcases List.mem_append.mp hf with hf | hf
      · exact h.gossip_wf t' es hf
      · simp at hf
  | shipTermMap i hrole =>
    refine ⟨h.node, ?_, ?_⟩
    · intro p hdr t v hf
      rcases List.mem_append.mp hf with hf | hf
      · exact h.stamp_pos p hdr t v hf
      · simp at hf
    · intro t es hf
      rcases List.mem_append.mp hf with hf | hf
      · exact h.gossip_wf t es hf
      · rw [List.mem_singleton, Frame.gossip.injEq] at hf
        obtain ⟨-, rfl⟩ := hf
        exact (h.node i).asc
  | deliverTermMap j t entries hmsg hterm =>
    refine ⟨?_, h.stamp_pos, h.gossip_wf⟩
    intro k
    show NodeWF (Function.update w.nodes j _ k)
    rcases eq_or_ne k j with rfl | hne
    · rw [Function.update_self]
      have hn := h.node k
      cases hrec : reconcile (w.nodes k).termMap (w.nodes k).pn.durable
          entries with
      | noCommonPrefix =>
        obtain ⟨hm, hd, hhst, hro, hct, hdt⟩ :=
          applyGossip_ncp (w.nodes k) t hrec
        refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
        · rw [hm]; trivial
        · intro e he; rw [hm] at he; cases he
        · rw [hd]; intro h0; exact (Nat.lt_irrefl 0 h0).elim
        · intro e he; rw [hm] at he; cases he
        · intro hr
          rw [hro] at hr
          by_cases hadopt : (w.nodes k).pn.currentTerm < t
          · rw [if_pos hadopt] at hr; cases hr
          · rw [if_neg hadopt] at hr
            have hteq : t = (w.nodes k).pn.currentTerm := by omega
            have hgp := (reachable_dinv hw).gossip_pinned k hr entries
              (hteq ▸ hmsg)
            rw [hgp, reconcile_self] at hrec
            cases hrec
        · intro hr
          rw [hro] at hr
          rw [hct]
          by_cases hadopt : (w.nodes k).pn.currentTerm < t
          · rw [if_pos hadopt] at hr; exact absurd rfl hr
          · rw [if_neg hadopt] at hr
            rw [if_neg hadopt]
            exact hn.role_term_pos hr
        · intro e he; rw [hm] at he; cases he
        · intro hr
          rw [hro] at hr
          rw [hct, hdt]
          by_cases hadopt : (w.nodes k).pn.currentTerm < t
          · rw [if_pos hadopt] at hr; cases hr
          · rw [if_neg hadopt] at hr
            rw [if_neg hadopt, if_neg hadopt]
            exact hn.cand_dt_lt hr
        · intro hr e he; rw [hm] at he; cases he
        · intro p t' v' hh
          rw [hhst] at hh
          cases hh
      | ok o =>
        obtain ⟨hm, hd, hhst, hro, hct, hdt⟩ :=
          applyGossip_ok (w.nodes k) t hrec
        cases entries with
        | nil =>
          rw [show reconcile (w.nodes k).termMap (w.nodes k).pn.durable []
              = ReconcileResult.ok
                  ⟨(w.nodes k).pn.durable, (w.nodes k).termMap⟩ from rfl]
            at hrec
          injection hrec with hrec'
          subst hrec'
          refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
          · rw [hm]; exact hn.asc
          · intro e he; rw [hm] at he; exact hn.floor0 e he
          · rw [hd, hm]; exact hn.nonempty
          · intro e he; rw [hm] at he; rw [hd]; exact hn.last_base e he
          · intro hr
            rw [hro] at hr
            rw [hm]
            by_cases hadopt : (w.nodes k).pn.currentTerm < t
            · rw [if_pos hadopt] at hr; cases hr
            · rw [if_neg hadopt] at hr
              exact hn.leader_map hr
          · intro hr
            rw [hro] at hr
            rw [hct]
            by_cases hadopt : (w.nodes k).pn.currentTerm < t
            · rw [if_pos hadopt] at hr; exact absurd rfl hr
            · rw [if_neg hadopt] at hr
              rw [if_neg hadopt]
              exact hn.role_term_pos hr
          · intro e he
            rw [hm] at he
            rw [hct]
            by_cases hadopt : (w.nodes k).pn.currentTerm < t
            · rw [if_pos hadopt]
              exact Nat.le_trans (hn.map_le e he) hterm
            · rw [if_neg hadopt]
              exact hn.map_le e he
          · intro hr
            rw [hro] at hr
            rw [hct, hdt]
            by_cases hadopt : (w.nodes k).pn.currentTerm < t
            · rw [if_pos hadopt] at hr; cases hr
            · rw [if_neg hadopt] at hr
              rw [if_neg hadopt, if_neg hadopt]
              exact hn.cand_dt_lt hr
          · intro hr e he
            rw [hro] at hr
            rw [hm] at he
            rw [hct]
            by_cases hadopt : (w.nodes k).pn.currentTerm < t
            · rw [if_pos hadopt] at hr; cases hr
            · rw [if_neg hadopt] at hr
              rw [if_neg hadopt]
              exact hn.cand_map_lt hr e he
          · intro p t' v' hh
            rw [hhst] at hh
            dsimp only at hh
            split at hh
            · exact hn.hist_pos p t' v' hh
            · cases hh
        | cons l0 ls =>
          have hlwf := h.gossip_wf t (l0 :: ls) hmsg
          have hmap : o.newMap = (w.nodes k).termMap.take
              (commonPrefixLen (w.nodes k).termMap (l0 :: ls)) :=
            reconcile_ok_newMap_take hn.asc hrec
          have hvle : o.validUpTo ≤ (w.nodes k).pn.durable :=
            reconcile_validUpTo_le _ _ _ _ hrec
          have hbnd : ∀ x ∈ o.newMap, x.2 ≤ o.validUpTo :=
            newMap_base_le hn.asc hlwf hn.last_base hrec
          refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
          · rw [hm, hmap]
            exact hn.asc.take _
          · intro e he
            rw [hm, hmap] at he
            cases hk : commonPrefixLen (w.nodes k).termMap (l0 :: ls) with
            | zero =>
              rw [hk, List.take_zero] at he
              cases he
            | succ k' =>
              rw [hk] at he
              cases hmm : (w.nodes k).termMap with
              | nil => rw [hmm] at he; cases he
              | cons a as =>
                rw [hmm, List.take_succ_cons, List.head?_cons] at he
                injection he with he'
                subst he'
                exact hn.floor0 a (by rw [hmm]; rfl)
          · rw [hd, hm, hmap]
            intro hpos hnil
            rw [List.take_eq_nil_iff] at hnil
            rcases hnil with hk0 | hmnil
            · cases hmm : (w.nodes k).termMap with
              | nil =>
                have hd0 : ¬ 0 < (w.nodes k).pn.durable :=
                  fun hlt => hn.nonempty hlt hmm
                omega
              | cons a as =>
                have h3a := reconcile_cuts_own_conflict
                  (w.nodes k).termMap (w.nodes k).pn.durable (l0 :: ls) o
                  (by simp) hrec
                have ha : (w.nodes k).termMap[0]? = some a := by
                  rw [hmm]; rfl
                have hva := h3a a (by rw [hk0]; exact ha)
                have ha0 : a.2 = 0 := hn.floor0 a (by rw [hmm]; rfl)
                omega
            · have hd0 : ¬ 0 < (w.nodes k).pn.durable :=
                fun hlt => hn.nonempty hlt hmnil
              omega
          · intro e he
            rw [hm] at he
            rw [hd]
            exact hbnd e (List.mem_of_getLast? he)
          · intro hr
            rw [hro] at hr
            by_cases hadopt : (w.nodes k).pn.currentTerm < t
            · rw [if_pos hadopt] at hr; cases hr
            · rw [if_neg hadopt] at hr
              have hteq : t = (w.nodes k).pn.currentTerm := by omega
              have hgp := (reachable_dinv hw).gossip_pinned k hr (l0 :: ls)
                (hteq ▸ hmsg)
              rw [hgp, reconcile_self] at hrec
              injection hrec with hrec'
              rw [hm, ← hrec']
              exact hn.leader_map hr
          · intro hr
            rw [hro] at hr
            rw [hct]
            by_cases hadopt : (w.nodes k).pn.currentTerm < t
            · rw [if_pos hadopt] at hr; exact absurd rfl hr
            · rw [if_neg hadopt] at hr
              rw [if_neg hadopt]
              exact hn.role_term_pos hr
          · intro e he
            rw [hm, hmap] at he
            have hem := List.mem_of_mem_take he
            rw [hct]
            by_cases hadopt : (w.nodes k).pn.currentTerm < t
            · rw [if_pos hadopt]
              exact Nat.le_trans (hn.map_le e hem) hterm
            · rw [if_neg hadopt]
              exact hn.map_le e hem
          · intro hr
            rw [hro] at hr
            rw [hct, hdt]
            by_cases hadopt : (w.nodes k).pn.currentTerm < t
            · rw [if_pos hadopt] at hr; cases hr
            · rw [if_neg hadopt] at hr
              rw [if_neg hadopt, if_neg hadopt]
              exact hn.cand_dt_lt hr
          · intro hr e he
            rw [hro] at hr
            rw [hm, hmap] at he
            have hem := List.mem_of_mem_take he
            rw [hct]
            by_cases hadopt : (w.nodes k).pn.currentTerm < t
            · rw [if_pos hadopt] at hr; cases hr
            · rw [if_neg hadopt] at hr
              rw [if_neg hadopt]
              exact hn.cand_map_lt hr e hem
          · intro p t' v' hh
            rw [hhst] at hh
            dsimp only at hh
            split at hh
            · exact hn.hist_pos p t' v' hh
            · cases hh
    · rw [Function.update_of_ne hne]
      exact h.node k

/-- The bundle holds in every reachable world. -/
theorem reachable_mapInv {n : Nat} {w : World n} (hw : Reachable w) :
    MapInv w := by
  have h : Relation.ReflTransGen Step (World.init n) w := hw
  clear hw
  induction h with
  | refl => exact minv_init n
  | tail hsteps hstep ih => exact minv_step hsteps ih hstep

/-- **Decision-5 `MapsWF`** over the data-plane world: per-node
`Ascending` ∧ `MapFloor`. -/
def MapsWF {n : Nat} (w : World n) : Prop :=
  ∀ j : Fin n, TermMap.Ascending (w.nodes j).termMap ∧ MapFloor (w.nodes j)

theorem reachable_mapsWF {n : Nat} {w : World n} (hw : Reachable w) :
    MapsWF w := fun j =>
  let hn := (reachable_mapInv hw).node j
  ⟨hn.asc, hn.floor0, hn.nonempty⟩

#print axioms reachable_mapInv
#print axioms reachable_mapsWF

end Data

namespace Cert

/-- **Decision-5 `MapsWF`** over the commit-layer world (the LC3/LC4
consumer surface): per-node `Ascending` ∧ `MapFloor` on the `dn` slice. -/
def MapsWF {n : Nat} (w : World n) : Prop :=
  ∀ j : Fin n, Uc2.TermMap.Ascending (w.nodes j).dn.termMap ∧
    Uc2.Data.MapFloor (w.nodes j).dn

/-- `MapsWF` holds in every reachable commit-layer world — the data-level
induction lifted through the projection (commit steps are data stutters). -/
theorem reachable_mapsWF {n : Nat} {w : World n} (hw : Reachable w) :
    MapsWF w := fun j =>
  Uc2.Data.reachable_mapsWF (reachable_project hw) j

/-- The decision-5 downstream intent, cashed out: every durable position is
covered by a map entry at or below it. -/
theorem MapsWF.covered {n : Nat} {w : World n} (h : MapsWF w) (j : Fin n)
    {p : Nat} (hp : p < (w.nodes j).pn.durable) :
    ∃ e ∈ (w.nodes j).dn.termMap, e.2 ≤ p := by
  obtain ⟨-, hfl, hne⟩ := h j
  cases hm : (w.nodes j).dn.termMap with
  | nil => exact absurd hm (hne (Nat.lt_of_le_of_lt (Nat.zero_le p) hp))
  | cons e es =>
    refine ⟨e, List.mem_cons_self, ?_⟩
    have := hfl e (by rw [hm]; rfl)
    omega

#print axioms reachable_mapsWF
#print axioms MapsWF.covered

end Cert

#print axioms TermMap.Ascending.head_base_le
#print axioms TermMap.Ascending.take_base_le
#print axioms TermMap.Ascending.base_le_getLast
#print axioms TermMap.ascending_cons_of
#print axioms TermMap.Ascending.take
#print axioms TermMap.Ascending.snoc

end Uc2
