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

/- ===================================================================
   GATE-1 CRITICAL FINDING, DISCHARGED: the bundle needs CHAIN-INDEXED configs.

   Fable gate 1 (2026-07-26) showed the naive interpretation `cfgid ↦ Finset node`
   makes the `cfgLt` axioms of MODEL-EDIT-2 UNSATISFIABLE: add-x-then-remove-x gives
   `succCfg c d ∧ succCfg d c`, so any transitive superset of `succCfg` yields
   `cfgLt c c`, contradicting irreflexivity — and by this file's own doctrine an
   unsatisfiable bundle makes every `#check_invariants` green VACUOUS.

   The fix is also the more faithful reading of UC: **a config is a log ENTRY at a
   position, not a bare voter set.** Two witnesses follow.

   W1 (`ICfg = ℕ × Finset V`) — the primary one. It permits BRANCHING config history
   (two different successors of one config), so it proves the model does not secretly
   assume linearity.

   W2 (`LCfg = ℕ`, a fixed ±1 chain) — **now the operative witness.** It additionally
   satisfies `succCfg` FUNCTIONALITY, `cfgLt` TOTALITY, SUCCESSOR IMMEDIACY and
   CONNECTEDNESS — the assumptions the branch-shaped CTIs called for (MODEL-EDIT-2b,
   ledger 19: real UC linearizes config history through the log).
   ** BOOKKEEPING NOTE (anti-vacuity): once `cfglt_total` is assumed, W1 NO LONGER
   witnesses the bundle — two W1 configs sharing an index are incomparable. W1 is kept
   because it still witnesses the pre-2b bundle and therefore documents exactly which
   assumption buys linearity; the SATISFIABILITY of the current model rests on W2. **
   =================================================================== -/

section ChainIndexed

/-- A config is a log entry: a chain index paired with the voter set it installs. -/
abbrev ICfg (V : Type _) := ℕ × Finset V

def ICMember (n : V) (c : ICfg V) : Prop := n ∈ c.2
def IIsQuorum (q : Finset V) (c : ICfg V) : Prop := IsQuorum q c.2
def ISuccCfg (c d : ICfg V) : Prop := d.1 = c.1 + 1 ∧ SuccCfg c.2 d.2
def ICfgLt (c d : ICfg V) : Prop := c.1 < d.1

theorem i_quorum_member_sound {c : ICfg V} {q : Finset V} (h : IIsQuorum q c) :
    ∀ n, n ∈ q → ICMember n c := fun _ hn => h.1 hn

theorem i_same_cfg_intersection {c : ICfg V} {q1 q2 : Finset V}
    (h1 : IIsQuorum q1 c) (h2 : IIsQuorum q2 c) : ∃ n, n ∈ q1 ∧ n ∈ q2 :=
  same_cfg_intersection h1 h2

theorem i_succ_shape {c d : ICfg V} (h : ISuccCfg c d) :
    ∃ x : V, (∀ n, ICMember n d ↔ (ICMember n c ∨ n = x)) ∨
             (∀ n, ICMember n d ↔ (ICMember n c ∧ n ≠ x)) := h.2

theorem i_adjacent_cfg_intersection {c d : ICfg V} {q1 q2 : Finset V}
    (hs : ISuccCfg c d) (h1 : IIsQuorum q1 c) (h2 : IIsQuorum q2 d) :
    ∃ n, n ∈ q1 ∧ n ∈ q2 :=
  adjacent_cfg_intersection hs.2 h1 h2

theorem i_cfglt_irrefl (c : ICfg V) : ¬ ICfgLt c c := by
  simp [ICfgLt]

theorem i_cfglt_trans {c d e : ICfg V} (h1 : ICfgLt c d) (h2 : ICfgLt d e) :
    ICfgLt c e := lt_trans h1 h2

theorem i_succ_cfglt {c d : ICfg V} (h : ISuccCfg c d) : ICfgLt c d := by
  simp [ICfgLt, h.1]

/-- Non-vacuity of W1: `ISuccCfg` really relates something (an empty `succCfg` would
    satisfy the adjacency assumption trivially). -/
theorem i_succ_inhabited (s : Finset V) (x : V) :
    ISuccCfg (0, s) (1, insert x s) := by
  refine ⟨rfl, ⟨x, Or.inl ?_⟩⟩
  intro n; rw [Finset.mem_insert]; tauto

end ChainIndexed

section LinearChain

variable (chain : ℕ → Finset V)

def LCMember (n : V) (k : ℕ) : Prop := n ∈ chain k
def LIsQuorum (q : Finset V) (k : ℕ) : Prop := IsQuorum q (chain k)
def LSuccCfg (k k' : ℕ) : Prop := k' = k + 1
def LCfgLt (k k' : ℕ) : Prop := k < k'

theorem l_quorum_member_sound {k : ℕ} {q : Finset V} (h : LIsQuorum chain q k) :
    ∀ n, n ∈ q → LCMember chain n k := fun _ hn => h.1 hn

theorem l_same_cfg_intersection {k : ℕ} {q1 q2 : Finset V}
    (h1 : LIsQuorum chain q1 k) (h2 : LIsQuorum chain q2 k) : ∃ n, n ∈ q1 ∧ n ∈ q2 :=
  same_cfg_intersection h1 h2

theorem l_succ_shape (hchain : ∀ k, SuccCfg (chain k) (chain (k + 1))) {k k' : ℕ}
    (h : LSuccCfg k k') :
    ∃ x : V, (∀ n, LCMember chain n k' ↔ (LCMember chain n k ∨ n = x)) ∨
             (∀ n, LCMember chain n k' ↔ (LCMember chain n k ∧ n ≠ x)) := by
  subst h; exact hchain k

theorem l_adjacent_cfg_intersection (hchain : ∀ k, SuccCfg (chain k) (chain (k + 1)))
    {k k' : ℕ} {q1 q2 : Finset V}
    (hs : LSuccCfg k k') (h1 : LIsQuorum chain q1 k) (h2 : LIsQuorum chain q2 k') :
    ∃ n, n ∈ q1 ∧ n ∈ q2 := by
  subst hs; exact adjacent_cfg_intersection (hchain k) h1 h2

theorem l_cfglt_irrefl (k : ℕ) : ¬ LCfgLt k k := by simp [LCfgLt]

theorem l_cfglt_trans {k1 k2 k3 : ℕ} (h1 : LCfgLt k1 k2) (h2 : LCfgLt k2 k3) :
    LCfgLt k1 k3 := lt_trans h1 h2

theorem l_succ_cfglt {k k' : ℕ} (h : LSuccCfg k k') : LCfgLt k k' := by
  subst h; exact Nat.lt_succ_self k

/-- IN RESERVE (branch-shaped CTIs): the config chain has ONE successor per config. -/
theorem l_succ_functional {k k1 k2 : ℕ} (h1 : LSuccCfg k k1) (h2 : LSuccCfg k k2) :
    k1 = k2 := by simp [LSuccCfg] at h1 h2; omega

/-- IN RESERVE (branch-shaped CTIs): config history is TOTALLY ordered. -/
theorem l_cfglt_total (k1 k2 : ℕ) : k1 = k2 ∨ LCfgLt k1 k2 ∨ LCfgLt k2 k1 := by
  simp [LCfgLt]; omega

/-- Genesis (index 0) is the least config — nothing precedes it. -/
theorem l_genesis_least (k : ℕ) : ¬ LCfgLt k 0 := by simp [LCfgLt]

/-- The order has no room strictly between a config and its successor. -/
theorem l_succ_immediate {k k' e : ℕ} (h : LSuccCfg k k')
    (h1 : LCfgLt k e) (h2 : LCfgLt e k') : False := by
  simp [LSuccCfg] at h; simp [LCfgLt] at h1 h2; omega

/-- The order is GENERATED by the successor relation: from any `c < d` there is a
    succ-step out of `c` that does not overshoot `d`. (This is what lets a chain
    induction walk from a stale config up to a committed one.) -/
theorem l_cfglt_connected {k k' : ℕ} (h : LCfgLt k k') :
    ∃ e, LSuccCfg k e ∧ (e = k' ∨ LCfgLt e k') := by
  refine ⟨k + 1, rfl, ?_⟩
  simp [LCfgLt] at h ⊢; omega

end LinearChain

end UcQuorumAdjacency

-- Axiom audit: the discharge must rest on nothing but Lean's own axioms
-- (no `sorry`, no model assumption).
#print axioms UcQuorumAdjacency.adjacent_cfg_intersection
#print axioms UcQuorumAdjacency.same_cfg_intersection
#print axioms UcQuorumAdjacency.i_adjacent_cfg_intersection
#print axioms UcQuorumAdjacency.i_cfglt_irrefl
#print axioms UcQuorumAdjacency.i_cfglt_trans
#print axioms UcQuorumAdjacency.i_succ_cfglt
#print axioms UcQuorumAdjacency.i_succ_shape
#print axioms UcQuorumAdjacency.i_same_cfg_intersection
#print axioms UcQuorumAdjacency.i_quorum_member_sound
#print axioms UcQuorumAdjacency.i_succ_inhabited
#print axioms UcQuorumAdjacency.l_cfglt_total
#print axioms UcQuorumAdjacency.l_succ_functional
#print axioms UcQuorumAdjacency.l_adjacent_cfg_intersection
#print axioms UcQuorumAdjacency.l_succ_shape
#print axioms UcQuorumAdjacency.l_genesis_least
#print axioms UcQuorumAdjacency.l_succ_immediate
#print axioms UcQuorumAdjacency.l_cfglt_connected
