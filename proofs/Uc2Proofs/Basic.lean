import Mathlib.Data.Finset.Card
import Uc2Model.Basic

/-! # Uc2Proofs — theorems about Uc2Model (mathlib allowed here). -/

/-- Hello-theorem: mathlib is wired up. -/
theorem uc2_hello : (2 : Nat) ∣ 4 := ⟨2, rfl⟩
