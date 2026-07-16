import Mathlib.Tactic
import Uc2Model.Commit

/-! C1/C2 — CommitTracker safety shape (spec §4):
monotonicity and bounded-by-own. -/

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

end CommitTracker
end Uc2
