import Mathlib.Tactic
import Uc2Model.Commit

/-! C1/C2 — CommitTracker safety shape (spec §4):
monotonicity and bounded-by-own.
C3/C4 — no-phantom-commit and reset soundness (spec §4). -/

namespace Uc2
namespace CommitTracker

/-- **C1 (step).** No single event ever lowers `commit`. -/
theorem commit_mono_step (t : CommitTracker) (e : Ev) :
    t.commit ≤ (t.step e).commit := by
  cases e with
  | report idx d => simp [step, onDurable]
  | reset => simp [step, resetReports]
  | advance own =>
    simp only [step, advance]
    split
    · rename_i hlt
      exact le_of_lt hlt
    · exact le_refl _

/-- **C1 (run).** `commit` never decreases across any event sequence. -/
theorem commit_mono_run (t : CommitTracker) (evs : List Ev) :
    t.commit ≤ (t.run evs).commit := by
  unfold run
  induction evs generalizing t with
  | nil => exact le_refl _
  | cons e es ih =>
    simp only [List.foldl_cons]
    exact le_trans (commit_mono_step t e) (ih (t.step e))

/-- **C2.** An advance that fires never certifies beyond the leader's own
durable: `advance t own = (t', some k)` ⟹ `k ≤ own`. -/
theorem advance_le_own (t : CommitTracker) (own k : Nat) (t' : CommitTracker)
    (h : t.advance own = (t', some k)) : k ≤ own := by
  simp only [advance] at h
  split at h
  · rw [Prod.mk.injEq] at h
    obtain ⟨_, hk⟩ := h
    have hk' : min ((t.ranking own).getD (t.quorum - 1) 0) own = k := Option.some_inj.mp hk
    rw [← hk']
    exact min_le_right _ _
  · exact absurd h (by simp)

/-! ### Helpers for C3/C4 -/

/-- The ranking is descending: `mergeSort` with `fun a b => decide (b ≤ a)`
yields a `Pairwise (· ≥ ·)` list (bridged from the Bool predicate). -/
private theorem ranking_pairwise (t : CommitTracker) (own : Nat) :
    (t.ranking own).Pairwise (fun a b => b ≤ a) := by
  have h := List.pairwise_mergeSort (le := fun a b : Nat => decide (b ≤ a))
    (by intro a b c hab hbc; simp only [decide_eq_true_eq] at *; omega)
    (by intro a b; simp only [Bool.or_eq_true, decide_eq_true_eq]; omega)
    (own :: t.reported)
  exact h.imp (by intro a b hab; simpa using hab)

/-- A firing advance strictly raises `commit` (the `<` guard). -/
private theorem advance_some_lt (t : CommitTracker) (own k : Nat)
    (t' : CommitTracker) (h : t.advance own = (t', some k)) : t.commit < k := by
  simp only [advance] at h
  split at h
  · rename_i hlt
    rw [Prod.mk.injEq] at h
    obtain ⟨-, hk⟩ := h
    have := Option.some_inj.mp hk
    omega
  · exact absurd h (by simp)

/-- A firing advance sets `commit` to exactly the certified value. -/
private theorem advance_some_commit (t : CommitTracker) (own k : Nat)
    (t' : CommitTracker) (h : t.advance own = (t', some k)) : t'.commit = k := by
  simp only [advance] at h
  split at h
  · rw [Prod.mk.injEq] at h
    obtain ⟨ht, hk⟩ := h
    rw [← ht]
    exact Option.some_inj.mp hk
  · exact absurd h (by simp)

/-- A non-firing advance leaves the tracker untouched. -/
private theorem advance_none_fst (t : CommitTracker) (own : Nat)
    (t' : CommitTracker) (h : t.advance own = (t', none)) : t' = t := by
  simp only [advance] at h
  split at h
  · exact absurd h (by simp)
  · rw [Prod.mk.injEq] at h
    exact h.1.symm

/-- `advance` never touches the report slots. -/
private theorem advance_fst_reported (t : CommitTracker) (own : Nat) :
    ((t.advance own).1).reported = t.reported := by
  simp only [advance]
  split <;> rfl

private theorem run_cons (t : CommitTracker) (e : Ev) (es : List Ev) :
    t.run (e :: es) = (t.step e).run es := rfl

private theorem run_append (t : CommitTracker) (l₁ l₂ : List Ev) :
    t.run (l₁ ++ l₂) = (t.run l₁).run l₂ := by
  simp [run]

private theorem getD_replicate_zero (n i : Nat) :
    (List.replicate n (0 : Nat)).getD i 0 = 0 := by
  rcases Nat.lt_or_ge i n with h | h
  · rw [List.getD_eq_getElem?_getD, List.getElem?_eq_getElem (by simpa using h)]
    simp
  · rw [List.getD_eq_getElem?_getD, List.getElem?_eq_none (by simpa using h)]
    rfl

/-! ### C3 — no phantom commit -/

/-- **C3 (step).** A firing advance is quorum-certified: at least `quorum`
members of {own} ∪ reported sit at or above the new commit. -/
theorem advance_certified (t : CommitTracker) (own k : Nat)
    (t' : CommitTracker) (h : t.advance own = (t', some k)) :
    t.quorum ≤ ((own :: t.reported).filter (fun v => k ≤ v)).length := by
  simp only [advance] at h
  split at h
  · rename_i hlt
    rw [Prod.mk.injEq] at h
    obtain ⟨-, hk⟩ := h
    have hk' : min ((t.ranking own).getD (t.quorum - 1) 0) own = k :=
      Option.some_inj.mp hk
    have hkpos : 0 < k := by omega
    have hkle : k ≤ (t.ranking own).getD (t.quorum - 1) 0 := by omega
    set r : List Nat := t.ranking own with hr
    set q : Nat := t.quorum with hq
    rcases Nat.eq_zero_or_pos q with hq0 | hqpos
    · omega
    -- the quorum-th slot is a real element (else `getD` = 0 < k)
    have hlen : q - 1 < r.length := by
      by_contra hout
      rw [List.getD_eq_getElem?_getD, List.getElem?_eq_none (by omega)] at hkle
      simp at hkle
      omega
    have hget : r.getD (q - 1) 0 = r[q - 1]'hlen := by
      rw [List.getD_eq_getElem?_getD, List.getElem?_eq_getElem hlen]
      rfl
    -- descending sortedness: every one of the first q entries is ≥ r[q-1] ≥ k
    have hpair := ranking_pairwise t own
    rw [← hr] at hpair
    have hsorted := List.pairwise_iff_getElem.mp hpair
    have hall : ∀ x ∈ r.take q, decide (k ≤ x) = true := by
      intro x hx
      obtain ⟨j, hj, rfl⟩ := List.getElem_of_mem hx
      have hjq : j < q := by
        rw [List.length_take] at hj
        omega
      have hjr : j < r.length := by omega
      rw [List.getElem_take]
      simp only [decide_eq_true_eq]
      rcases Nat.lt_or_ge j (q - 1) with hlt' | hge
      · have hle' := hsorted j (q - 1) hjr hlen hlt'
        omega
      · have hjeq : j = q - 1 := by omega
        subst hjeq
        omega
    -- count: the filter keeps all q of them, and filtering commutes with the
    -- mergeSort permutation
    have htake : ((r.take q).filter (fun v => k ≤ v)).length = q := by
      rw [List.filter_eq_self.mpr hall, List.length_take]
      omega
    have hmono : ((r.take q).filter (fun v => k ≤ v)).length
        ≤ (r.filter (fun v => k ≤ v)).length :=
      ((List.take_sublist q r).filter _).length_le
    have hperm : (r.filter (fun v => k ≤ v)).length
        = ((own :: t.reported).filter (fun v => k ≤ v)).length := by
      have hp : List.Perm r (own :: t.reported) := by
        rw [hr]
        exact List.mergeSort_perm _ _
      exact (hp.filter _).length_eq
    omega
  · exact absurd h (by simp)

/-- Any run that changes `commit` contains an advance step that produced the
final value (induction backbone for `commit_certified_run`). -/
private theorem commit_change_certified (t : CommitTracker) (evs : List Ev) :
    (t.run evs).commit ≠ t.commit →
    ∃ pre own post t', evs = pre ++ Ev.advance own :: post ∧
      (t.run pre).advance own = (t', some ((t.run evs).commit)) := by
  induction evs using List.reverseRecOn with
  | nil => exact fun hne => absurd rfl hne
  | append_singleton l e ih =>
    intro hne
    have hstep : t.run (l ++ [e]) = (t.run l).step e := by
      rw [run_append]
      rfl
    -- shared tail: if the last event left commit unchanged, lift the IH
    have lift : ((t.run l).step e).commit = (t.run l).commit →
        ∃ pre own post t', l ++ [e] = pre ++ Ev.advance own :: post ∧
          (t.run pre).advance own = (t', some ((t.run (l ++ [e])).commit)) := by
      intro hc
      rw [hstep, hc] at hne
      obtain ⟨pre, own, post, t', hl, hadv⟩ := ih hne
      refine ⟨pre, own, post ++ [e], t', ?_, ?_⟩
      · rw [hl, List.append_assoc, List.cons_append]
      · rw [hstep, hc]
        exact hadv
    cases e with
    | report idx d => exact lift rfl
    | reset => exact lift rfl
    | advance own =>
      rcases hadv : (t.run l).advance own with ⟨t₂, r⟩
      cases r with
      | none =>
        apply lift
        show ((t.run l).advance own).1.commit = (t.run l).commit
        rw [hadv, advance_none_fst _ _ _ hadv]
      | some k =>
        have hcommit : (t.run (l ++ [Ev.advance own])).commit = k := by
          rw [hstep]
          show ((t.run l).advance own).1.commit = k
          rw [hadv]
          exact advance_some_commit _ own k t₂ hadv
        refine ⟨l, own, [], t₂, rfl, ?_⟩
        rw [hcommit]
        exact hadv

/-- **C3 (run).** Every positive commit value the fold attains was produced by
some advance step along the way, and that step was quorum-certified at the
tracker state it fired from (`new` starts at commit 0).

Restated from the task brief (see task-8 report): the split + firing-advance
equation is the brief's claim with the product `k` unpacked, and the third
conjunct states the certification explicitly (no weakening — it is exactly
`advance_certified` at the firing step). -/
theorem commit_certified_run (nF cS : Nat) (evs : List Ev)
    (hpos : 0 < ((new nF cS).run evs).commit) :
    ∃ pre own post t',
      evs = pre ++ Ev.advance own :: post ∧
      ((new nF cS).run pre).advance own
        = (t', some (((new nF cS).run evs).commit)) ∧
      ((new nF cS).run pre).quorum ≤
        ((own :: ((new nF cS).run pre).reported).filter
          (fun v => ((new nF cS).run evs).commit ≤ v)).length := by
  have hne : ((new nF cS).run evs).commit ≠ (new nF cS).commit := by
    show _ ≠ 0
    omega
  obtain ⟨pre, own, post, t', hl, hadv⟩ := commit_change_certified _ evs hne
  exact ⟨pre, own, post, t', hl, hadv, advance_certified _ own _ t' hadv⟩

/-! ### C4 — reset soundness -/

/-- **C4 (immediate form).** Right after a reset, an advance can never fire
when a real quorum (≥ 2) is required: stale-term reports are gone and own
alone is not a quorum. -/
theorem reset_then_advance_none (t : CommitTracker) (own : Nat)
    (hq : 2 ≤ t.quorum) : (t.resetReports.advance own).2 = none := by
  cases hres : t.resetReports.advance own with
  | mk t' r =>
    cases r with
    | none => rfl
    | some k =>
      exfalso
      have hcert := advance_certified _ own k _ hres
      have hlt := advance_some_lt _ own k _ hres
      have hkpos : 0 < k := by omega
      -- post-reset, {own} ∪ reported holds at most one value ≥ k > 0
      have hrep : t.resetReports.reported = List.replicate t.reported.length 0 := rfl
      have hquo : t.resetReports.quorum = t.quorum := rfl
      rw [hquo, hrep, List.filter_cons,
        List.filter_replicate_of_neg (by simp; omega)] at hcert
      split at hcert <;> simp at hcert <;> omega

/-- Slot values only rise via matching reports: after any run, slot `i` is
either still bounded by its starting value or was set by a `report i d` in the
event sequence with `d` at least the final value. -/
private theorem run_slot_provenance (s : CommitTracker) (evs : List Ev) (i : Nat) :
    (s.run evs).reported.getD i 0 ≤ s.reported.getD i 0 ∨
      ∃ d, Ev.report i d ∈ evs ∧ (s.run evs).reported.getD i 0 ≤ d := by
  induction evs generalizing s with
  | nil => exact Or.inl (Nat.le_refl _)
  | cons e es ih =>
    rw [run_cons]
    cases e with
    | report j dd =>
      simp only [step]
      rcases ih (s.onDurable j dd) with hle | ⟨d, hmem, hled⟩
      · by_cases hij : j = i
        · subst hij
          by_cases hlen : j < s.reported.length
          · -- Post-2026-08-16 the slot IS the report (no high-water max), so
            -- the run's value is bounded by `dd` outright.
            have hset : (s.onDurable j dd).reported.getD j 0 = dd := by
              simp [onDurable, List.getD_eq_getElem?_getD,
                List.getElem?_set_self (by simpa using hlen)]
            rw [hset] at hle
            exact Or.inr ⟨dd, by simp, hle⟩
          · have hge : s.reported.length ≤ j := Nat.le_of_not_lt hlen
            have hset : (s.onDurable j dd).reported = s.reported := by
              simp [onDurable, List.set_eq_of_length_le hge]
            rw [hset] at hle
            exact Or.inl hle
        · have hset : (s.onDurable j dd).reported.getD i 0
              = s.reported.getD i 0 := by
            simp [onDurable, List.getD_eq_getElem?_getD, List.getElem?_set_ne hij]
          rw [hset] at hle
          exact Or.inl hle
      · exact Or.inr ⟨d, List.mem_cons_of_mem _ hmem, hled⟩
    | reset =>
      simp only [step]
      rcases ih s.resetReports with hle | ⟨d, hmem, hled⟩
      · left
        have h0 : (s.resetReports).reported.getD i 0 = 0 := by
          show (List.replicate s.reported.length 0).getD i 0 = 0
          exact getD_replicate_zero _ _
        omega
      · exact Or.inr ⟨d, List.mem_cons_of_mem _ hmem, hled⟩
    | advance own =>
      simp only [step]
      rcases ih ((s.advance own).1) with hle | ⟨d, hmem, hled⟩
      · left
        rw [advance_fst_reported] at hle
        exact hle
      · exact Or.inr ⟨d, List.mem_cons_of_mem _ hmem, hled⟩

/-- **C4 (provenance).** After `run` from a just-reset state, every nonzero
slot value comes from a `report` on that slot in the suffix, with the report's
durable at least the final slot value: reset zeroes; nothing else writes.

Restated from the task brief (see task-8 report): the brief's disjunction made
the strong half trivially escapable; this is the properly strong claim. The
brief's `hi : i < t.reported.length` hypothesis is dropped — the theorem holds
for all `i` (an out-of-range slot reads 0, contradicting `hnz`). -/
theorem reports_only_from_onDurable (t : CommitTracker) (evs : List Ev) (i : Nat)
    (hnz : 0 < ((t.resetReports).run evs).reported.getD i 0) :
    ∃ d, Ev.report i d ∈ evs ∧
      ((t.resetReports).run evs).reported.getD i 0 ≤ d := by
  rcases run_slot_provenance t.resetReports evs i with hle | h
  · exfalso
    have h0 : (t.resetReports).reported.getD i 0 = 0 := by
      show (List.replicate t.reported.length 0).getD i 0 = 0
      exact getD_replicate_zero _ _
    omega
  · exact h

end CommitTracker
end Uc2
