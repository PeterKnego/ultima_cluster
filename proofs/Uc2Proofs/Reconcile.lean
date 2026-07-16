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

/-- **R3a.** Our first beyond-prefix entry (conflict or overhang) is always
cut: `validUpTo` never exceeds its base. The `leader ≠ []` hypothesis
excludes the empty-leader early return (Rust: "a leader with no map tells us
nothing — clean as-is"), where no clamp applies by design; discovered via
countermodel during proving (see gate doc). -/
theorem reconcile_cuts_own_conflict (own : TermMap) (d : Nat)
    (leader : TermMap) (o : Outcome)
    (hne : leader ≠ [])
    (h : reconcile own d leader = .ok o) :
    ∀ e ∈ own[commonPrefixLen own leader]?, o.validUpTo ≤ e.2 := by
  cases leader with
  | nil => exact absurd rfl hne
  | cons l0 ls =>
    intro e he
    exact (reconcileClamped_ok (reconcile_ok_clamped h)).2.1 e he

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

/-! ### T11 — R4 divergence soundness

The `termAt` structure toolkit, the `firstDivergence` characterization, the
`DataStamped` contract, and the capstone theorem
`reconcile_validUpTo_eq_firstDivergence`. -/

namespace TermMap

/-- Folding `termAt`'s step over entries whose bases all sit strictly above
`p` never moves the accumulator. -/
theorem foldl_termAt_high {p : Nat} :
    ∀ (l : TermMap) (init : Nat), (∀ e ∈ l, p < e.2) →
      l.foldl (fun acc e => if e.2 ≤ p then e.1 else acc) init = init
  | [], _, _ => rfl
  | a :: t, init, h => by
    rw [List.foldl_cons, if_neg (Nat.not_le.mpr (h a List.mem_cons_self))]
    exact foldl_termAt_high t init fun e he => h e (List.mem_cons_of_mem a he)

/-- Appending entries that all begin strictly above `p` does not change
`termAt` at `p`. -/
theorem termAt_append_high {a b : TermMap} {p : Nat}
    (hb : ∀ e ∈ b, p < e.2) : (a ++ b).termAt p = a.termAt p := by
  simp only [termAt, List.foldl_append]
  exact foldl_termAt_high b _ hb

/-- Below every entry from slot `k` on, `termAt` only sees the `k`-prefix
(the beyond-`k` bases are all strictly above `p` by ascent). -/
theorem Ascending.termAt_take {m : TermMap} (hwf : Ascending m) {k p : Nat}
    (hb : ∀ e, m[k]? = some e → p < e.2) :
    m.termAt p = TermMap.termAt (m.take k) p := by
  cases ho : m[k]? with
  | none =>
    rw [List.take_of_length_le (List.getElem?_eq_none_iff.mp ho)]
  | some e =>
    obtain ⟨hk, hke⟩ := List.getElem?_eq_some_iff.mp ho
    have hdrop : m.drop k = e :: m.drop (k + 1) := by
      rw [List.drop_eq_getElem_cons hk, hke]
    have hasc : Ascending (e :: m.drop (k + 1)) := hdrop ▸ hwf.drop k
    have hhigh : ∀ x ∈ m.drop k, p < x.2 := by
      intro x hx
      rw [hdrop] at hx
      rcases List.mem_cons.mp hx with rfl | hxt
      · exact hb x ho
      · exact Nat.lt_of_lt_of_le (hb e ho) (hasc.head_le x hxt)
    conv_lhs => rw [← List.take_append_drop k m]
    exact termAt_append_high hhigh

/-- Folding `termAt`'s step over entries whose terms are all strictly below
`t` keeps the accumulator strictly below `t`. -/
theorem foldl_termAt_lt {t p : Nat} :
    ∀ (l : TermMap) (init : Nat), init < t → (∀ e ∈ l, e.1 < t) →
      l.foldl (fun acc e => if e.2 ≤ p then e.1 else acc) init < t
  | [], _, hinit, _ => hinit
  | a :: s, init, hinit, hl => by
    rw [List.foldl_cons]
    refine foldl_termAt_lt s _ ?_ fun e he => hl e (List.mem_cons_of_mem a he)
    split
    · exact hl a List.mem_cons_self
    · exact hinit

/-- `termAt` never escapes a strict (positive) upper bound on the terms —
positivity covers the 0 accumulator of the empty/uncovered case. -/
theorem termAt_lt_of_forall_lt {l : TermMap} {t p : Nat} (hpos : 0 < t)
    (hlt : ∀ e ∈ l, e.1 < t) : l.termAt p < t := by
  simp only [termAt]
  exact foldl_termAt_lt l 0 hpos hlt

/-- Terms strictly ascend across the whole map: the head's term is strictly
below every later entry's term. -/
theorem Ascending.head_term_lt :
    ∀ {l : TermMap} {a : Nat × Nat}, Ascending (a :: l) →
      ∀ {j : Nat} {e : Nat × Nat}, l[j]? = some e → a.1 < e.1 := by
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
      exact hwf.1
    | succ j' =>
      rw [List.getElem?_cons_succ] at hj
      exact Nat.lt_trans hwf.1 (ih hwf.2.2 hj)

/-- Every entry in the `k`-prefix has term strictly below the `k`-th term. -/
theorem Ascending.take_term_lt :
    ∀ {m : TermMap}, Ascending m → ∀ {k : Nat} {e : Nat × Nat},
      m[k]? = some e → ∀ x ∈ m.take k, x.1 < e.1 := by
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
      · exact hwf.head_term_lt hk
      · exact ih hwf.tail hk x hxt

/-- The `termAt` of a strict (`k ≥ 1`) prefix stays strictly below the
`k`-th term — the head entry's term dominates the 0 accumulator. -/
theorem Ascending.termAt_take_lt {m : TermMap} (hwf : Ascending m)
    {k : Nat} {e : Nat × Nat} (hk : m[k]? = some e) (hk1 : 0 < k) (p : Nat) :
    TermMap.termAt (m.take k) p < e.1 := by
  have hpos : 0 < e.1 := by
    cases m with
    | nil => simp at hk
    | cons m0 mt =>
      cases k with
      | zero => omega
      | succ j =>
        have hmem : m0 ∈ (m0 :: mt).take (j + 1) := by
          rw [List.take_succ_cons]; exact List.mem_cons_self
        exact Nat.lt_of_le_of_lt (Nat.zero_le _) (hwf.take_term_lt hk m0 hmem)
  exact termAt_lt_of_forall_lt hpos (hwf.take_term_lt hk)

end TermMap

/-- `commonPrefixLen` is maximal: when both maps still have a `k`-th entry
at `k = commonPrefixLen`, those entries differ. -/
theorem commonPrefixLen_getElem_ne :
    ∀ (a b : TermMap) (e f : Nat × Nat),
      a[commonPrefixLen a b]? = some e → b[commonPrefixLen a b]? = some f →
      e ≠ f
  | [], _, _, _, he, _ => by simp at he
  | _ :: _, [], _, _, _, hf => by simp at hf
  | a :: as, b :: bs, e, f, he, hf => by
    by_cases hab : a = b
    · subst hab
      simp only [commonPrefixLen, if_true, List.getElem?_cons_succ] at he hf
      exact commonPrefixLen_getElem_ne as bs e f he hf
    · simp only [commonPrefixLen, if_neg hab, List.getElem?_cons_zero,
        Option.some.injEq] at he hf
      subst he; subst hf
      exact hab

/-- The NoCommonPrefix gate, read back from a successful reconcile: with no
shared entry and a nonempty own map, the leader's window did NOT slide past
our first byte (`l0.base ≤ own-head base`). -/
theorem reconcile_ok_gate {own : TermMap} {d : Nat} {l0 : Nat × Nat}
    {ls : TermMap} {o : Outcome} (h : reconcile own d (l0 :: ls) = .ok o)
    (hk : commonPrefixLen own (l0 :: ls) = 0)
    {e : Nat × Nat} (he : own[0]? = some e) : l0.2 ≤ e.2 := by
  cases own with
  | nil => simp at he
  | cons o0 os =>
    rw [List.getElem?_cons_zero] at he
    injection he with he'
    subst he'
    simp only [reconcile] at h
    split at h
    · cases h
    · rename_i hcond
      exact Nat.le_of_not_lt fun hlt => hcond ⟨hk, hlt⟩

/-- A truncating clamp (`validUpTo < d`) is always NAMED by a boundary: it
equals the own-side or the leader-side first-beyond-prefix base. -/
theorem reconcileClamped_lt_cases {own leader : TermMap} {d k : Nat}
    {o : Outcome} (h : reconcile.reconcileClamped own d leader k = .ok o)
    (hlt : o.validUpTo < d) :
    (∃ e, own[k]? = some e ∧ o.validUpTo = e.2) ∨
    (∃ f, leader[k]? = some f ∧ o.validUpTo = f.2) := by
  obtain ⟨v, m⟩ := o
  dsimp only [Outcome.validUpTo] at hlt ⊢
  rcases ho : own[k]? with _ | e <;> rcases hl : leader[k]? with _ | f <;>
    simp only [reconcile.reconcileClamped, ho, hl, ReconcileResult.ok.injEq,
      Outcome.mk.injEq] at h <;>
    obtain ⟨hv, -⟩ := h
  -- own[k]? = none, leader[k]? = none: v = d contradicts v < d
  · exact absurd hlt (by omega)
  -- own[k]? = none, leader[k]? = some f
  · right
    refine ⟨f, rfl, ?_⟩
    simp only [Nat.min_def] at hv
    split_ifs at hv <;> omega
  -- own[k]? = some e, leader[k]? = none
  · left
    refine ⟨e, rfl, ?_⟩
    simp only [Nat.min_def] at hv
    split_ifs at hv <;> omega
  -- own[k]? = some e, leader[k]? = some f
  · by_cases hve : v = e.2
    · exact .inl ⟨e, rfl, hve⟩
    · right
      refine ⟨f, rfl, ?_⟩
      simp only [Nat.min_def] at hv
      split_ifs at hv <;> omega

/-- No content divergence below `bound` ⇒ `firstDivergence` saturates at
`bound`. -/
theorem firstDivergence_eq_bound {a b : ByteHistory} {bound : Nat}
    (h : ∀ p, p < bound → a p = b p) : firstDivergence a b bound = bound := by
  have hnone : (List.range bound).find? (fun p => a p ≠ b p) = none := by
    rw [List.find?_eq_none]
    intro x hx
    simp [h x (List.mem_range.mp hx)]
  unfold firstDivergence
  rw [hnone]

private theorem find?_range'_eq_some {p : Nat → Bool} :
    ∀ (n s x : Nat), s ≤ x → x < s + n → p x = true →
      (∀ q, s ≤ q → q < x → p q = false) →
      (List.range' s n).find? p = some x
  | 0, _, _, h1, h2, _, _ => absurd h2 (by omega)
  | n + 1, s, x, h1, h2, hp, hmin => by
    rw [List.range'_succ]
    rcases Nat.eq_or_lt_of_le h1 with rfl | hlt
    · exact List.find?_cons_of_pos hp
    · rw [List.find?_cons_of_neg (by simp [hmin s (Nat.le_refl s) hlt])]
      exact find?_range'_eq_some n (s + 1) x hlt (by omega) hp
        fun q hq1 hq2 => hmin q (by omega) hq2

/-- `firstDivergence` characterization, constructor form: agreement strictly
below a divergent `p₀ < bound` pins `firstDivergence` to exactly `p₀`. -/
theorem firstDivergence_eq_of {a b : ByteHistory} {bound p₀ : Nat}
    (hp : p₀ < bound) (hne : a p₀ ≠ b p₀) (hmin : ∀ q, q < p₀ → a q = b q) :
    firstDivergence a b bound = p₀ := by
  have hsome : (List.range bound).find? (fun p => a p ≠ b p) = some p₀ := by
    rw [List.range_eq_range']
    exact find?_range'_eq_some bound 0 p₀ (Nat.zero_le _) (by omega)
      (by simpa using hne) fun q _ hq => by simpa using hmin q hq
  unfold firstDivergence
  rw [hsome]

/-- The data-stamped-map contract (`reconcile.rs` module docs): our map
records a term only when we actually held that term's data — never by
gossip — and the leader's shipped map is its certified lineage. This is the
reachability envelope the election SM maintains; Tier B proves it inductive,
Phase 1 takes it as hypothesis.

The first five fields are the contract as originally speced (T11 brief). The
last three were **discovered necessary during the R4 proof** — without any
one of them the theorem is FALSE (executable countermodels are pinned as
`#guard`s below the structure). Each is protocol-true:

- `own_stamped` / `leader_stamped`: an entry recorded with base strictly
  below the follower's durable frontier genuinely stamps its base byte —
  equivalently, **no retained shadowed phantom below durable**. This is
  exactly the invariant `reconcile.rs`'s phantom-dropping filter exists to
  maintain (its comment: "once genuine data streams past its base, the next
  reconcile's own-side clamp would spuriously truncate genuine (possibly
  committed) bytes at the phantom's base"). A shadowed phantom below the
  frontier (e.g. `[(1,0),(2,500),(5,500)]` at durable 1000) makes reconcile
  cut at a position where the histories genuinely AGREE.
- `leader_pos`: term 0 is the genesis sentinel (`termAt` yields 0 below the
  first entry / on the empty map); real terms are `≥ 1` (`start_election`
  bumps the term before `become_leader` opens it; `DataTermObserved` accepts
  only `term > last_term ≥ 0`). A term-0 leader entry would be a clamp with
  no content behind it.

**LOUD FINDING (candidate spec gap, for the gate doc):** the
`leader_stamped` countermodel is protocol-shaped. A leader that crashed
after persisting its term-`t` map entry but before the NewTerm frame
fsynced, restarted, and won term `t+1` itself, ships
`[..., (t, D), (t+1, D)]` — a shadowed phantom in its OWN map that no
reconcile ever prunes (leaders don't reconcile; recovery adopts the
persisted map verbatim). Against a follower that data-observed only
`(t+1, D)` and streamed past `D`, reconcile then cuts genuinely shared
bytes at `D` — and re-cuts on every subsequent map gossip. Tier B must
either prove this unreachable end-to-end or the Rust needs a
same-base-phantom prune (e.g. at `become_leader`/recovery).

Note `shared_base` turned out NOT to be needed by R4 (the stamped fields
subsume the same-term-confusion argument it was expected to settle); it is
retained as part of the documented contract. -/
structure DataStamped (own : TermMap) (d : Nat) (hOwn : ByteHistory)
    (leader : TermMap) (hLeader : ByteHistory) : Prop where
  own_wf : TermMap.Ascending own
  leader_wf : TermMap.Ascending leader
  own_encodes : TermMap.encodes own hOwn d
  leader_encodes : ∀ p, hLeader p = leader.termAt p
  shared_base : ∀ t b b', (t, b) ∈ own → (t, b') ∈ leader → b = b'
  own_stamped : ∀ e ∈ own, e.2 < d → hOwn e.2 = e.1
  leader_stamped : ∀ e ∈ leader, e.2 < d → hLeader e.2 = e.1
  leader_pos : ∀ e ∈ leader, 0 < e.1

/-- **R4.** Under the data-stamped contract, truncation is EXACTLY the first
content divergence against the leader's certified lineage: nothing genuinely
shared is cut (with R2), and everything from the first divergent byte on is
cut. `validUpTo = firstDivergence hOwn hLeader d` (i.e. `d` itself when no
divergence exists below `d`). -/
theorem reconcile_validUpTo_eq_firstDivergence (own : TermMap) (d : Nat)
    (leader : TermMap) (hOwn hLeader : ByteHistory) (o : Outcome)
    (hds : DataStamped own d hOwn leader hLeader)
    (hne : leader ≠ [])
    (h : reconcile own d leader = .ok o) :
    o.validUpTo = firstDivergence hOwn hLeader d := by
  cases leader with
  | nil => exact absurd rfl hne
  | cons l0 ls =>
    have hc := reconcile_ok_clamped h
    obtain ⟨hvd, hownb, hldrb, -⟩ := reconcileClamped_ok hc
    have htake : own.take (commonPrefixLen own (l0 :: ls))
        = (l0 :: ls).take (commonPrefixLen own (l0 :: ls)) :=
      take_commonPrefixLen_eq own (l0 :: ls)
    -- (≥) Common-prefix agreement: every position below the clamp agrees.
    have hagree : ∀ q, q < o.validUpTo → hOwn q = hLeader q := by
      intro q hq
      have hq_d : q < d := Nat.lt_of_lt_of_le hq hvd
      rw [hds.own_encodes q hq_d, hds.leader_encodes q,
        hds.own_wf.termAt_take (fun e he => Nat.lt_of_lt_of_le hq (hownb e he)),
        hds.leader_wf.termAt_take (fun f hf => Nat.lt_of_lt_of_le hq (hldrb f hf)),
        htake]
    rcases Nat.lt_or_ge o.validUpTo d with hlt | hge
    · -- (≤) The byte AT the clamp diverges.
      have hA_stamp : ∀ e, own[commonPrefixLen own (l0 :: ls)]? = some e →
          e.2 = o.validUpTo → own.termAt o.validUpTo = e.1 := by
        intro e he hep
        have hmem : e ∈ own := List.mem_of_getElem? he
        have hed : e.2 < d := by omega
        rw [← hep, ← hds.own_encodes e.2 hed, hds.own_stamped e hmem hed]
      have hB_stamp : ∀ f, (l0 :: ls)[commonPrefixLen own (l0 :: ls)]? = some f →
          f.2 = o.validUpTo → TermMap.termAt (l0 :: ls) o.validUpTo = f.1 := by
        intro f hf hfp
        have hmem : f ∈ l0 :: ls := List.mem_of_getElem? hf
        have hfd : f.2 < d := by omega
        rw [← hfp, ← hds.leader_encodes f.2, hds.leader_stamped f hmem hfd]
      have hdiv : own.termAt o.validUpTo ≠ TermMap.termAt (l0 :: ls) o.validUpTo := by
        by_cases hOB : ∃ e, own[commonPrefixLen own (l0 :: ls)]? = some e ∧
            e.2 = o.validUpTo
        · obtain ⟨e, hoe, hep⟩ := hOB
          have hA : own.termAt o.validUpTo = e.1 := hA_stamp e hoe hep
          by_cases hLB : ∃ f, (l0 :: ls)[commonPrefixLen own (l0 :: ls)]? = some f ∧
              f.2 = o.validUpTo
          · -- CLASH: both boundary entries sit exactly at the clamp. They
            -- differ (k-maximality) yet share the base, so terms differ.
            obtain ⟨f, hlf, hfp⟩ := hLB
            have hB : TermMap.termAt (l0 :: ls) o.validUpTo = f.1 := hB_stamp f hlf hfp
            have hef : e ≠ f := commonPrefixLen_getElem_ne own (l0 :: ls) e f hoe hlf
            rw [hA, hB]
            intro h1
            exact hef (Prod.ext h1 (hep.trans hfp.symm))
          · -- Own-side boundary only: the leader reduces to the shared prefix.
            have hlead_high : ∀ f, (l0 :: ls)[commonPrefixLen own (l0 :: ls)]? = some f →
                o.validUpTo < f.2 := by
              intro f hf
              rcases Nat.eq_or_lt_of_le (hldrb f hf) with heq | hlt2
              · exact absurd ⟨f, hf, heq.symm⟩ hLB
              · exact hlt2
            have hB : TermMap.termAt (l0 :: ls) o.validUpTo
                = TermMap.termAt (own.take (commonPrefixLen own (l0 :: ls))) o.validUpTo := by
              rw [hds.leader_wf.termAt_take hlead_high, ← htake]
            rcases Nat.eq_zero_or_pos (commonPrefixLen own (l0 :: ls)) with hk0 | hkpos
            · -- k = 0: the NoCommonPrefix gate pins l0.base ≤ own-head base
              -- = the clamp, contradicting the leader boundary sitting above it.
              exfalso
              rw [hk0] at hoe
              have hgate := reconcile_ok_gate h hk0 hoe
              have hl0 : o.validUpTo < l0.2 :=
                hlead_high l0 (by rw [hk0]; simp)
              omega
            · -- k ≥ 1: the prefix's termAt is strictly below e's term.
              have hBlt : TermMap.termAt (own.take (commonPrefixLen own (l0 :: ls))) o.validUpTo
                  < e.1 := hds.own_wf.termAt_take_lt hoe hkpos o.validUpTo
              rw [hA, hB]
              exact (Nat.ne_of_lt hBlt).symm
        · -- Leader-side boundary (the clamp must be named by SOME boundary).
          have hLB : ∃ f, (l0 :: ls)[commonPrefixLen own (l0 :: ls)]? = some f ∧
              f.2 = o.validUpTo := by
            rcases reconcileClamped_lt_cases hc hlt with ⟨e, he, hpe⟩ | ⟨f, hf, hpf⟩
            · exact absurd ⟨e, he, hpe.symm⟩ hOB
            · exact ⟨f, hf, hpf.symm⟩
          obtain ⟨f, hlf, hfp⟩ := hLB
          have hB : TermMap.termAt (l0 :: ls) o.validUpTo = f.1 := hB_stamp f hlf hfp
          have hown_high : ∀ e, own[commonPrefixLen own (l0 :: ls)]? = some e →
              o.validUpTo < e.2 := by
            intro e he
            rcases Nat.eq_or_lt_of_le (hownb e he) with heq | hlt2
            · exact absurd ⟨e, he, heq.symm⟩ hOB
            · exact hlt2
          have hA : own.termAt o.validUpTo
              = TermMap.termAt ((l0 :: ls).take (commonPrefixLen own (l0 :: ls))) o.validUpTo := by
            rw [hds.own_wf.termAt_take hown_high, htake]
          have hfpos : 0 < f.1 := hds.leader_pos f (List.mem_of_getElem? hlf)
          rcases Nat.eq_zero_or_pos (commonPrefixLen own (l0 :: ls)) with hk0 | hkpos
          · -- k = 0: own reduces to the empty prefix (termAt 0 < leader term).
            rw [hA, hB, hk0]
            simp only [List.take_zero, TermMap.termAt, List.foldl_nil]
            exact Nat.ne_of_lt hfpos
          · -- k ≥ 1: the prefix's termAt is strictly below f's term.
            have hAlt : TermMap.termAt ((l0 :: ls).take (commonPrefixLen own (l0 :: ls))) o.validUpTo
                < f.1 := hds.leader_wf.termAt_take_lt hlf hkpos o.validUpTo
            rw [hA, hB]
            exact Nat.ne_of_lt hAlt
      have hdiv' : hOwn o.validUpTo ≠ hLeader o.validUpTo := by
        rw [hds.own_encodes o.validUpTo hlt, hds.leader_encodes o.validUpTo]
        exact hdiv
      exact (firstDivergence_eq_of hlt hdiv' hagree).symm
    · -- validUpTo = d: no divergence below d — firstDivergence saturates.
      have hpd : o.validUpTo = d := Nat.le_antisymm hvd hge
      rw [hpd] at hagree ⊢
      exact (firstDivergence_eq_bound hagree).symm

end Uc2

-- Countermodel pins: each of the three T11-added `DataStamped` fields is
-- NECESSARY — drop it and `validUpTo = firstDivergence` is false. Every
-- countermodel below satisfies ALL the remaining fields (with
-- `hOwn := termAt own`, `hLeader := termAt leader`).
open Uc2 in
section
-- `own_stamped` necessity: own carries a shadowed phantom (2,500) below its
-- durable (violating ONLY own_stamped). Histories agree everywhere below
-- d = 1000 (both stamp 1 then 5), yet reconcile cuts at 500 ≠ 1000.
#guard reconcile [(1, 0), (2, 500), (5, 500)] 1000 [(1, 0), (5, 500)]
  == .ok ⟨500, [(1, 0)]⟩
#guard firstDivergence (TermMap.termAt [(1, 0), (2, 500), (5, 500)])
  (TermMap.termAt [(1, 0), (5, 500)]) 1000 == 1000
-- `leader_stamped` necessity (the protocol-shaped one — see the DataStamped
-- docstring's LOUD FINDING): the leader ships a shadowed phantom (7,2000)
-- below the follower's durable. Histories agree everywhere below d = 3000
-- (both stamp 1 then 8), yet reconcile cuts at 2000 ≠ 3000.
#guard reconcile [(1, 0), (8, 2000)] 3000 [(1, 0), (7, 2000), (8, 2000)]
  == .ok ⟨2000, [(1, 0)]⟩
#guard firstDivergence (TermMap.termAt [(1, 0), (8, 2000)])
  (TermMap.termAt [(1, 0), (7, 2000), (8, 2000)]) 3000 == 3000
-- `leader_pos` necessity: a term-0 leader entry clamps at 500 although both
-- histories are identically the genesis sentinel (agree everywhere).
#guard reconcile [] 1000 [(0, 500)] == .ok ⟨500, []⟩
#guard firstDivergence (TermMap.termAt [])
  (TermMap.termAt [(0, 500)]) 1000 == 1000
end
