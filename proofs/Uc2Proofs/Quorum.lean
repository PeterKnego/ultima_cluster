import Mathlib.Data.Finset.Card
import Mathlib.Data.Fintype.Card
import Uc2Model.Commit

/-! C5 — quorum intersection (spec §4, table `Commit`): any two
`⌊n/2⌋+1`-sized member subsets intersect. Foundational; reused by Tier B
(election safety, leader completeness). -/

namespace Uc2

/-- **C5.** Two quorums over `Fin n` always share a member. -/
theorem quorum_intersect (n : Nat) (S T : Finset (Fin n))
    (hS : n / 2 + 1 ≤ S.card) (hT : n / 2 + 1 ≤ T.card) :
    (S ∩ T).Nonempty := by
  have hcard : (S ∪ T).card + (S ∩ T).card = S.card + T.card :=
    Finset.card_union_add_card_inter S T
  have hunion : (S ∪ T).card ≤ n := by
    simpa [Fintype.card_fin] using (S ∪ T).card_le_univ
  have : 0 < (S ∩ T).card := by omega
  exact Finset.card_pos.mp this

end Uc2
