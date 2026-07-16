import Mathlib.Tactic
import Uc2Model.Reconcile
import Uc2Model.ByteHistory

/-! R1/R5/R6 — reconcile safety shape (spec §5):
R1 bound (`valid_up_to ≤ own_durable`), R5 idempotence (phantom hygiene:
re-reconciling the outcome against the same leader is a fixed point), R6
NoCommonPrefix characterization. The `commonPrefixLen` toolkit and the
`reconcileClamped` elimination/fixpoint lemmas are public — T10/T11 reuse
them. -/

namespace Uc2

/-! ### `commonPrefixLen` toolkit (shared with T10/T11) -/

theorem commonPrefixLen_le_left : ∀ (a b : TermMap), commonPrefixLen a b ≤ a.length
  | [], _ => Nat.zero_le _
  | _ :: _, [] => Nat.zero_le _
  | a :: as, b :: bs => by
    simp only [commonPrefixLen, List.length_cons]
    split
    · exact Nat.succ_le_succ (commonPrefixLen_le_left as bs)
    · exact Nat.zero_le _

theorem commonPrefixLen_le_right : ∀ (a b : TermMap), commonPrefixLen a b ≤ b.length
  | [], _ => Nat.zero_le _
  | _ :: _, [] => Nat.zero_le _
  | a :: as, b :: bs => by
    simp only [commonPrefixLen, List.length_cons]
    split
    · exact Nat.succ_le_succ (commonPrefixLen_le_right as bs)
    · exact Nat.zero_le _

/-- The two maps agree entry-for-entry on the whole common prefix. -/
theorem take_commonPrefixLen_eq : ∀ (a b : TermMap),
    a.take (commonPrefixLen a b) = b.take (commonPrefixLen a b)
  | [], _ => rfl
  | _ :: _, [] => rfl
  | a :: as, b :: bs => by
    by_cases hab : a = b
    · subst hab
      simp only [commonPrefixLen, if_true, List.take_succ_cons]
      rw [take_commonPrefixLen_eq as bs]
    · simp only [commonPrefixLen, if_neg hab, List.take_zero]

/-- Truncating to the common prefix recomputes the same common prefix:
`commonPrefixLen (a.take k) b = k` for `k = commonPrefixLen a b`. -/
theorem commonPrefixLen_take : ∀ (a b : TermMap),
    commonPrefixLen (a.take (commonPrefixLen a b)) b = commonPrefixLen a b
  | [], _ => rfl
  | _ :: _, [] => rfl
  | a :: as, b :: bs => by
    by_cases hab : a = b
    · subst hab
      simp only [commonPrefixLen, if_true, List.take_succ_cons]
      rw [commonPrefixLen_take as bs]
    · simp only [commonPrefixLen, if_neg hab, List.take_zero]

/-! ### `Ascending` toolkit -/

namespace TermMap

theorem Ascending.tail {a : Nat × Nat} {l : TermMap} (h : Ascending (a :: l)) :
    Ascending l := by
  cases l with
  | nil => trivial
  | cons b t => exact h.2.2

theorem Ascending.drop {l : TermMap} (h : Ascending l) (k : Nat) :
    Ascending (l.drop k) := by
  induction k generalizing l with
  | zero => exact h
  | succ n ih =>
    cases l with
    | nil => trivial
    | cons a t => exact ih h.tail

/-- In an ascending map the head's base is a lower bound for every base. -/
theorem Ascending.head_le {e : Nat × Nat} {l : TermMap} (h : Ascending (e :: l)) :
    ∀ x ∈ l, e.2 ≤ x.2 := by
  induction l generalizing e with
  | nil => intro x hx; cases hx
  | cons b t ih =>
    intro x hx
    obtain ⟨-, hbase, hrest⟩ := h
    rcases List.mem_cons.mp hx with rfl | hxt
    · exact hbase
    · exact le_trans hbase (ih hrest x hxt)

/-- Ascending phantom hygiene: if the `k`-th entry's base already reaches `v`,
NO beyond-`k` entry survives a `base < v` filter (bases are non-decreasing, so
the survivors of the suffix would have to sit before index `k`). -/
theorem Ascending.filter_drop_eq_nil {own : TermMap} (hwf : Ascending own)
    {k v : Nat} (hbound : ∀ e, own[k]? = some e → v ≤ e.2) :
    (own.drop k).filter (fun e => e.2 < v) = [] := by
  cases ho : own[k]? with
  | none =>
    rw [List.drop_of_length_le (List.getElem?_eq_none_iff.mp ho)]
    rfl
  | some e =>
    obtain ⟨hk, hke⟩ := List.getElem?_eq_some_iff.mp ho
    have hdrop : own.drop k = e :: own.drop (k + 1) := by
      rw [List.drop_eq_getElem_cons hk, hke]
    have hasc : Ascending (e :: own.drop (k + 1)) := hdrop ▸ hwf.drop k
    have hv := hbound e ho
    rw [hdrop, List.filter_eq_nil_iff]
    intro x hx
    simp only [decide_eq_true_eq]
    rcases List.mem_cons.mp hx with rfl | hxt
    · omega
    · have := hasc.head_le x hxt
      omega

end TermMap

/-! ### `reconcileClamped` elimination + fixpoint -/

/-- Elimination for the clamped rebuild: the bound (`≤ d`), both frontier
bounds (`validUpTo` never reaches `own[k]` nor `leader[k]`), and the exact
shape of the surviving map. -/
theorem reconcileClamped_ok {own leader : TermMap} {d k : Nat} {o : Outcome}
    (h : reconcile.reconcileClamped own d leader k = .ok o) :
    o.validUpTo ≤ d ∧
    (∀ e, own[k]? = some e → o.validUpTo ≤ e.2) ∧
    (∀ e, leader[k]? = some e → o.validUpTo ≤ e.2) ∧
    o.newMap = own.take k ++ (own.drop k).filter (fun e => e.2 < o.validUpTo) := by
  obtain ⟨v, m⟩ := o
  dsimp only [Outcome.validUpTo, Outcome.newMap]
  rcases ho : own[k]? with _ | e <;> rcases hl : leader[k]? with _ | f <;>
    simp only [reconcile.reconcileClamped, ho, hl, ReconcileResult.ok.injEq,
      Outcome.mk.injEq] at h <;>
    obtain ⟨hv, hm⟩ := h <;>
    rw [hv] at hm
  -- own[k]? = none, leader[k]? = none
  · subst hv
    exact ⟨Nat.le_refl d, by simp, by simp, hm.symm⟩
  -- own[k]? = none, leader[k]? = some f
  · have hb : v ≤ d ∧ v ≤ f.2 := by
      simp only [Nat.min_def] at hv
      split_ifs at hv <;> omega
    refine ⟨hb.1, by simp, ?_, hm.symm⟩
    intro f' hf'
    cases hf'
    exact hb.2
  -- own[k]? = some e, leader[k]? = none
  · have hb : v ≤ d ∧ v ≤ e.2 := by
      simp only [Nat.min_def] at hv
      split_ifs at hv <;> omega
    refine ⟨hb.1, ?_, by simp, hm.symm⟩
    intro e' he'
    cases he'
    exact hb.2
  -- own[k]? = some e, leader[k]? = some f
  · have hb : v ≤ d ∧ v ≤ e.2 ∧ v ≤ f.2 := by
      simp only [Nat.min_def] at hv
      split_ifs at hv <;> omega
    refine ⟨hb.1, ?_, ?_, hm.symm⟩
    · intro e' he'
      cases he'
      exact hb.2.1
    · intro f' hf'
      cases hf'
      exact hb.2.2

/-- Fixpoint introduction for the clamped rebuild: a map that ends before
slot `k`, reconciled at a durable the leader's `k`-th base already covers,
passes through unchanged. -/
theorem reconcileClamped_fixpoint {own leader : TermMap} {v k : Nat}
    (hlen : own.length ≤ k)
    (hlead : ∀ e, leader[k]? = some e → v ≤ e.2) :
    reconcile.reconcileClamped own v leader k = .ok ⟨v, own⟩ := by
  have ho : own[k]? = none := List.getElem?_eq_none hlen
  rcases hl : leader[k]? with _ | f
  · simp only [reconcile.reconcileClamped, ho, hl, List.take_of_length_le hlen,
      List.drop_of_length_le hlen, List.filter_nil, List.append_nil]
  · have hnf : ¬f.2 < v := Nat.not_lt.mpr (hlead f hl)
    simp only [reconcile.reconcileClamped, ho, hl, if_neg hnf,
      List.take_of_length_le hlen, List.drop_of_length_le hlen,
      List.filter_nil, List.append_nil]

/-! ### `reconcile` ↔ `reconcileClamped` bridges -/

/-- Elimination: a successful reconcile against a nonempty leader map is
exactly the clamped rebuild at `k = commonPrefixLen own leader`. -/
theorem reconcile_ok_clamped {own : TermMap} {d : Nat} {l0 : Nat × Nat}
    {ls : TermMap} {o : Outcome} (h : reconcile own d (l0 :: ls) = .ok o) :
    reconcile.reconcileClamped own d (l0 :: ls)
      (commonPrefixLen own (l0 :: ls)) = .ok o := by
  cases own with
  | nil => simpa only [reconcile] using h
  | cons o0 os =>
    simp only [reconcile] at h
    split at h
    · cases h
    · exact h

/-- Introduction: when the NoCommonPrefix gate cannot fire (`own` empty, or a
nonzero common prefix), reconcile IS the clamped rebuild. -/
theorem reconcile_eq_clamped (own : TermMap) (d : Nat) (l0 : Nat × Nat)
    (ls : TermMap)
    (hgate : own = [] ∨ commonPrefixLen own (l0 :: ls) ≠ 0) :
    reconcile own d (l0 :: ls)
      = reconcile.reconcileClamped own d (l0 :: ls)
          (commonPrefixLen own (l0 :: ls)) := by
  cases own with
  | nil => simp only [reconcile]
  | cons o0 os =>
    have hk : commonPrefixLen (o0 :: os) (l0 :: ls) ≠ 0 := by
      rcases hgate with h | h
      · cases h
      · exact h
    simp only [reconcile]
    exact if_neg fun hc => hk hc.1

/-- With an ascending own map, a clean reconcile's surviving map is exactly
the common prefix — the whole beyond-prefix suffix is dropped (public: the
T10/T11 shape lemma). -/
theorem reconcile_ok_newMap_take {own : TermMap} {d : Nat} {l0 : Nat × Nat}
    {ls : TermMap} {o : Outcome} (hwf : TermMap.Ascending own)
    (h : reconcile own d (l0 :: ls) = .ok o) :
    o.newMap = own.take (commonPrefixLen own (l0 :: ls)) := by
  obtain ⟨-, hown, -, hmap⟩ := reconcileClamped_ok (reconcile_ok_clamped h)
  rw [hmap, hwf.filter_drop_eq_nil hown, List.append_nil]

/-! ### R1 / R5 / R6 -/

/-- **R1.** `valid_up_to ≤ own_durable` — reconcile never validates beyond
what we durably hold. -/
theorem reconcile_validUpTo_le (own : TermMap) (d : Nat) (leader : TermMap)
    (o : Outcome) (h : reconcile own d leader = .ok o) :
    o.validUpTo ≤ d := by
  cases leader with
  | nil =>
    simp only [reconcile] at h
    cases h
    exact Nat.le_refl d
  | cons l0 ls => exact (reconcileClamped_ok (reconcile_ok_clamped h)).1

/-- **R5.** Reconcile against the same leader map is a fixed point: applying
the outcome (truncated log, surviving map) reconciles clean and unchanged —
no phantom left behind can cause a later spurious truncation. -/
theorem reconcile_idempotent (own : TermMap) (d : Nat) (leader : TermMap)
    (o : Outcome) (hwf : TermMap.Ascending own)
    (h : reconcile own d leader = .ok o) :
    reconcile o.newMap o.validUpTo leader = .ok ⟨o.validUpTo, o.newMap⟩ := by
  cases leader with
  | nil =>
    simp only [reconcile] at h
    cases h
    rfl
  | cons l0 ls =>
    obtain ⟨-, -, hlead, -⟩ := reconcileClamped_ok (reconcile_ok_clamped h)
    have hmap : o.newMap = own.take (commonPrefixLen own (l0 :: ls)) :=
      reconcile_ok_newMap_take hwf h
    have hklen : commonPrefixLen own (l0 :: ls) ≤ own.length :=
      commonPrefixLen_le_left own (l0 :: ls)
    have hlen : o.newMap.length ≤ commonPrefixLen own (l0 :: ls) := by
      rw [hmap, List.length_take]; omega
    have hk' : commonPrefixLen o.newMap (l0 :: ls)
        = commonPrefixLen own (l0 :: ls) := by
      rw [hmap]; exact commonPrefixLen_take own (l0 :: ls)
    have hgate : o.newMap = [] ∨ commonPrefixLen o.newMap (l0 :: ls) ≠ 0 := by
      rcases Nat.eq_zero_or_pos (commonPrefixLen own (l0 :: ls)) with h0 | hpos
      · left; rw [hmap, h0, List.take_zero]
      · right; rw [hk']; omega
    rw [reconcile_eq_clamped o.newMap o.validUpTo l0 ls hgate, hk']
    exact reconcileClamped_fixpoint hlen hlead

/-- **R6.** NoCommonPrefix is surfaced exactly when the leader's shipped
window truly slid past our history: no shared entry AND the leader's first
base begins strictly beyond ours. -/
theorem noCommonPrefix_iff (own : TermMap) (d : Nat) (leader : TermMap) :
    reconcile own d leader = .noCommonPrefix ↔
      commonPrefixLen own leader = 0 ∧
      (∃ o0 os, own = o0 :: os) ∧
      (∃ l0 ls, leader = l0 :: ls ∧ ∀ o0 os, own = o0 :: os → o0.2 < l0.2) := by
  constructor
  · intro h
    cases leader with
    | nil => simp only [reconcile] at h; cases h
    | cons l0 ls =>
      cases own with
      | nil =>
        simp only [reconcile, reconcile.reconcileClamped] at h
        cases h
      | cons o0 os =>
        simp only [reconcile] at h
        split at h
        · rename_i hcond
          exact ⟨hcond.1, ⟨o0, os, rfl⟩, ⟨l0, ls, rfl, by
            intro a b hab
            injection hab with h1 _
            exact h1 ▸ hcond.2⟩⟩
        · simp only [reconcile.reconcileClamped] at h
          cases h
  · rintro ⟨hk, ⟨o0, os, rfl⟩, ⟨l0, ls, rfl, hlt⟩⟩
    simp only [reconcile]
    exact if_pos ⟨hk, hlt o0 os rfl⟩

/-! ### R2 / R3 -/

/-- **R2.** Positions under the common certified prefix always survive: any
`p < d` below BOTH maps' first beyond-prefix boundary is below
`validUpTo`. "Committed bytes at a healed follower survive reconcile",
local form. -/
theorem reconcile_preserves_shared_prefix (own : TermMap) (d : Nat)
    (leader : TermMap) (o : Outcome) (p : Nat)
    (h : reconcile own d leader = .ok o)
    (hp : p < d)
    (hown : ∀ e ∈ own[commonPrefixLen own leader]?, p < e.2)
    (hldr : ∀ e ∈ leader[commonPrefixLen own leader]?, p < e.2) :
    p < o.validUpTo := by
  cases leader with
  | nil =>
    simp only [reconcile] at h
    cases h
    exact hp
  | cons l0 ls =>
    have hc := reconcile_ok_clamped h
    obtain ⟨v, m⟩ := o
    dsimp only [Outcome.validUpTo]
    rcases ho : own[commonPrefixLen own (l0 :: ls)]? with _ | e <;>
      rcases hl : (l0 :: ls)[commonPrefixLen own (l0 :: ls)]? with _ | f <;>
      simp only [reconcile.reconcileClamped, ho, hl, ReconcileResult.ok.injEq,
        Outcome.mk.injEq] at hc <;>
      obtain ⟨hv, hm⟩ := hc <;>
      subst hv
    -- own[k]? = none, leader[k]? = none
    · exact hp
    -- own[k]? = none, leader[k]? = some f
    · have hf := hldr f hl
      simp only [Nat.min_def]
      split_ifs <;> omega
    -- own[k]? = some e, leader[k]? = none
    · have he := hown e ho
      simp only [Nat.min_def]
      split_ifs <;> omega
    -- own[k]? = some e, leader[k]? = some f
    · have he := hown e ho
      have hf := hldr f hl
      simp only [Nat.min_def]
      split_ifs <;> omega

/-- **R3b.** A leader term certified below our durable that our data-stamped
map lacks is proven divergence — always cut. -/
theorem reconcile_cuts_leader_uncovered (own : TermMap) (d : Nat)
    (leader : TermMap) (o : Outcome)
    (h : reconcile own d leader = .ok o) :
    ∀ e ∈ leader[commonPrefixLen own leader]?, e.2 < d → o.validUpTo ≤ e.2 := by
  cases leader with
  | nil =>
    simp only [reconcile] at h
    cases h
    simp
  | cons l0 ls =>
    intro e he _
    exact (reconcileClamped_ok (reconcile_ok_clamped h)).2.2.1 e he

end Uc2
