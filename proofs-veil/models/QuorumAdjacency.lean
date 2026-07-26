import Mathlib.Data.Finset.Card

/- UC v2.1 (M7) reconfig commit plane — the ADJACENCY OBLIGATION, DISCHARGED.
   Route **r1** (concrete-majority instantiation), per the session-1 checkpoint's
   two routes. Scratch — NEVER the record.

   WHAT THIS FILE IS FOR. `ReconfigCommitSMT.lean` states its quorum theory as
   Veil `assumption`s over ABSTRACT types (`cfgid`, `quorum`, `cmember`,
   `qmember`, `quorumOf`, `succCfg`) — the Lean-C5/Ivy idiom that makes
   `#check_invariants` an all-n result. Assumptions are free: an INCONSISTENT
   bundle makes every `#check_invariants` verdict vacuously green. This file is
   the antidote and the discharge in one:

     * it exhibits the INTENDED INTERPRETATION (configs = `Finset node`,
       quorums = strict-majority subsets, `succCfg` = ±1 voter), and
     * it PROVES every assumption of that bundle holds there — in particular
       `adjacent_cfg_quorum_intersection`, which the checkpoint required be a
       theorem, not an axiom.

   Consequently the abstract bundle is (a) satisfiable — so no `#check_invariants`
   verdict over it is vacuous — and (b) sound for the deployment UC actually has
   (majority quorums over a voter set changed one server at a time).

   The arithmetic, for the record: a C-quorum has ≥ ⌊|C|/2⌋+1 members and a
   (C ∖ {x})-quorum ≥ ⌊(|C|−1)/2⌋+1, and ⌊n/2⌋ + ⌊(n−1)/2⌋ + 1 = n > n−1 forces
   an intersection inside C even after deleting x; the add case is symmetric
   against |C ∪ {x}| ≤ |C|+1. Both are discharged below by `omega` from the
   two cardinality facts plus the union/intersection identity. -/

namespace UcQuorumAdjacency

variable {V : Type _} [DecidableEq V]

/-- A quorum of config `c`: a subset of `c` that is a strict majority of it.
    (The concrete reading of `quorumOf q c` + `quorum_member_sound`.) -/
def IsQuorum (q c : Finset V) : Prop := q ⊆ c ∧ c.card < 2 * q.card

/-- `ClusterConfig::apply`'s shape: `d` is `c` plus-or-minus exactly one voter.
    Stated exactly as the model's `succ_shape` assumption. -/
def SuccCfg (c d : Finset V) : Prop :=
  ∃ x : V, (∀ n, n ∈ d ↔ (n ∈ c ∨ n = x)) ∨ (∀ n, n ∈ d ↔ (n ∈ c ∧ n ≠ x))

/-- Counting core: two subsets of a common carrier whose sizes together exceed
    the carrier must meet. -/
theorem inter_of_card {q1 q2 s : Finset V}
    (h1 : q1 ⊆ s) (h2 : q2 ⊆ s) (hc : s.card < q1.card + q2.card) :
    ∃ n, n ∈ q1 ∧ n ∈ q2 := by
  have hu : (q1 ∪ q2).card ≤ s.card := Finset.card_le_card (Finset.union_subset h1 h2)
  have hkey : (q1 ∪ q2).card + (q1 ∩ q2).card = q1.card + q2.card :=
    Finset.card_union_add_card_inter q1 q2
  have hpos : 0 < (q1 ∩ q2).card := by omega
  obtain ⟨n, hn⟩ := Finset.card_pos.mp hpos
  exact ⟨n, (Finset.mem_inter.mp hn).1, (Finset.mem_inter.mp hn).2⟩

/-- `quorum_member_sound`, concretely: a quorum of `c` consists of `c`'s voters. -/
theorem quorum_member_sound_concrete {c q : Finset V} (h : IsQuorum q c) :
    ∀ n, n ∈ q → n ∈ c := fun _ hn => h.1 hn

/-- `same_cfg_quorum_intersection` (the C5 axiom), concretely. -/
theorem same_cfg_intersection {c q1 q2 : Finset V}
    (h1 : IsQuorum q1 c) (h2 : IsQuorum q2 c) : ∃ n, n ∈ q1 ∧ n ∈ q2 := by
  obtain ⟨hs1, hc1⟩ := h1
  obtain ⟨hs2, hc2⟩ := h2
  exact inter_of_card hs1 hs2 (by omega)

/-- **THE ADJACENCY LEMMA — the obligation, discharged.**
    Quorums of consecutive configs (±1 voter) always intersect. -/
theorem adjacent_cfg_intersection {c d q1 q2 : Finset V}
    (hs : SuccCfg c d) (h1 : IsQuorum q1 c) (h2 : IsQuorum q2 d) :
    ∃ n, n ∈ q1 ∧ n ∈ q2 := by
  obtain ⟨hs1, hc1⟩ := h1
  obtain ⟨hs2, hc2⟩ := h2
  obtain ⟨x, hadd | hrem⟩ := hs
  · -- ADD: d = insert x c, so c ⊆ d and |d| ≤ |c| + 1. Carrier = d.
    have hd : d = insert x c := by
      apply Finset.ext; intro n
      rw [Finset.mem_insert, hadd n]; tauto
    have hcd : c ⊆ d := fun n hn => (hadd n).mpr (Or.inl hn)
    have hcard : d.card ≤ c.card + 1 := by
      rw [hd]; exact Finset.card_insert_le x c
    exact inter_of_card (hs1.trans hcd) hs2 (by omega)
  · -- REMOVE: d = c.erase x, so d ⊆ c and |c| ≤ |d| + 1. Carrier = c.
    have hd : d = c.erase x := by
      apply Finset.ext; intro n
      rw [Finset.mem_erase, hrem n]; tauto
    have hdc : d ⊆ c := fun n hn => ((hrem n).mp hn).1
    have hsub : c ⊆ insert x d := by
      intro n hn
      by_cases hnx : n = x
      · exact Finset.mem_insert.mpr (Or.inl hnx)
      · exact Finset.mem_insert.mpr (Or.inr ((hrem n).mpr ⟨hn, hnx⟩))
    have hcard : c.card ≤ d.card + 1 :=
      le_trans (Finset.card_le_card hsub) (Finset.card_insert_le x d)
    exact inter_of_card hs1 (hs2.trans hdc) (by omega)

/-- `succ_shape` holds by construction in the concrete interpretation (it IS the
    definition) — recorded so the whole assumption bundle is covered. -/
theorem succ_shape_concrete {c d : Finset V} (h : SuccCfg c d) :
    ∃ x : V, (∀ n, n ∈ d ↔ (n ∈ c ∨ n = x)) ∨ (∀ n, n ∈ d ↔ (n ∈ c ∧ n ≠ x)) := h

/-- Non-vacuity of the bundle: majority quorums of a config really exist
    (the config itself is one), so the assumptions are not satisfied merely by
    having no quorums at all. -/
theorem self_is_quorum {c : Finset V} (h : 0 < c.card) : IsQuorum c c :=
  ⟨Finset.Subset.refl c, by omega⟩

end UcQuorumAdjacency

-- Axiom audit: the discharge must rest on nothing but Lean's own axioms
-- (no `sorry`, no model assumption).
#print axioms UcQuorumAdjacency.adjacent_cfg_intersection
#print axioms UcQuorumAdjacency.same_cfg_intersection
