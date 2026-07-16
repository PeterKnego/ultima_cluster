import Uc2Model.TermMap

/-! `uc2_consensus/src/reconcile.rs::reconcile` — the pure core of log
truncation. 1:1 port; the Rust module docs are the specification and the
Rust unit tests are re-pinned below as #guards. -/

namespace Uc2

-- `reconcile.rs::Outcome`.
structure Outcome where
  validUpTo : Nat
  newMap    : TermMap
deriving Repr, DecidableEq

-- `reconcile.rs::Reconcile`.
inductive ReconcileResult where
  | ok (o : Outcome)
  | noCommonPrefix
deriving Repr, DecidableEq

-- Length of the longest common prefix (entries equal in term AND base).
def commonPrefixLen : TermMap → TermMap → Nat
  | a :: as, b :: bs => if a = b then commonPrefixLen as bs + 1 else 0
  | _, _ => 0

-- `reconcile.rs::reconcile`.
def reconcile (own : TermMap) (ownDurable : Nat) (leader : TermMap) :
    ReconcileResult :=
  match leader with
  | [] => .ok ⟨ownDurable, own⟩   -- empty leader map tells us nothing
  | l0 :: _ =>
    let k := commonPrefixLen own leader
    match own with
    | o0 :: _ =>
      if k = 0 ∧ o0.2 < l0.2 then .noCommonPrefix
      else reconcileClamped own ownDurable leader k
    | [] => reconcileClamped own ownDurable leader k
where
  -- The two clamps + the phantom-dropping map rebuild (`reconcile.rs` body
  -- after the NoCommonPrefix gate).
  reconcileClamped (own : TermMap) (ownDurable : Nat) (leader : TermMap)
      (k : Nat) : ReconcileResult :=
    let v1 := match own[k]? with
      | some e => min ownDurable e.2
      | none => ownDurable
    let validUpTo := match leader[k]? with
      | some e => if e.2 < ownDurable then min v1 e.2 else v1
      | none => v1
    let newMap := own.take k ++ (own.drop k).filter (fun e => e.2 < validUpTo)
    .ok ⟨validUpTo, newMap⟩

end Uc2

-- Ports of the reconcile.rs unit tests (binding contract).
open Uc2 in
section
-- clean_outcome_drops_beyond_prefix_phantom_frontier_entry
#guard reconcile [(1, 0), (2, 5000)] 5000 [(1, 0), (3, 5000)]
  == .ok ⟨5000, [(1, 0)]⟩
#guard reconcile [(1, 0), (2, 5000)] 5000 [(1, 0), (2, 5000)]
  == .ok ⟨5000, [(1, 0), (2, 5000)]⟩
-- identical_histories_are_clean
#guard reconcile [(1, 0), (3, 4096)] 8000 [(1, 0), (3, 4096)]
  == .ok ⟨8000, [(1, 0), (3, 4096)]⟩
-- divergent_own_tail_truncates_at_own_divergent_base
#guard reconcile [(1, 0), (2, 4096)] 6000 [(1, 0), (3, 4096)]
  == .ok ⟨4096, [(1, 0)]⟩
-- own_overhang_beyond_leader_truncates_at_own_next_base
#guard reconcile [(1, 0), (2, 5000)] 6000 [(1, 0)]
  == .ok ⟨5000, [(1, 0)]⟩
-- behind_follower_with_stamped_term_is_clean
#guard reconcile [(1, 0), (2, 2000)] 3000 [(1, 0), (2, 2000)]
  == .ok ⟨3000, [(1, 0), (2, 2000)]⟩
-- ex_leader_divergent_truncates_at_leaders_uncovered_base (F4 scenario A)
#guard reconcile [(1, 0)] 3000 [(1, 0), (2, 2000)]
  == .ok ⟨2000, [(1, 0)]⟩
-- entry_at_the_bound_is_not_a_divergence
#guard reconcile [(1, 0)] 3000 [(1, 0), (2, 3000)]
  == .ok ⟨3000, [(1, 0)]⟩
-- same_base_different_term_truncates_to_zero
#guard reconcile [(5, 0)] 4096 [(6, 0)] == .ok ⟨0, []⟩
-- no_common_prefix_is_surfaced
#guard reconcile [(1, 0)] 5000 [(40, 1048576), (41, 2097152)]
  == .noCommonPrefix
-- empty_own_map_reconciles_clean_at_durable_zero
#guard reconcile [] 0 [(1, 0), (2, 5000)] == .ok ⟨0, []⟩
end
