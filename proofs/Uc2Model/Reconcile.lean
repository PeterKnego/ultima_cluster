import Uc2Model.TermMap

/-! `uc_consensus/src/reconcile.rs::reconcile` — the pure core of log
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

/-- `reconcile.rs::reconcile` POST the 2026-08-16 window-alignment fix. The
Rust function now ALIGNS the leader's shipped window (`term_map_wire_tail`,
last 64 entries) inside the follower's full map before prefix-matching —
the old index-aligned match declared `NoCommonPrefix` against every healthy
follower once a cluster's lifetime leadership count exceeded the window,
wiping followers in a loop and ultimately truncating committed bytes
cluster-wide (the Aug 2026 nightly acked-write-loss).

Structurally the fix is a thin wrapper over the UNCHANGED core above: find
the alignment index `j` of `leader[0]` in `own`, run the core on the aligned
suffix `own.drop j` (its head equals `leader[0]`, so the core's
NoCommonPrefix gate never fires and its clamps operate at the aligned
offsets), and re-prepend the below-window prefix `own.take j` (our honest
observations of history the window simply did not ship). When `leader[0]`
is not in `own` at all: an empty `own` reconciles clean; a window starting
strictly beyond our bytes is the genuine purged-prefix `NoCommonPrefix`; a
window starting INSIDE our bytes at a term our data-stamped map lacks is
proven divergence — cut there (clamped further by our first entry claiming
a term ≥ the window's first, the same-term/different-base conflict).

The theorems in `Uc2Proofs/Reconcile.lean` are stated over the CORE and are
untouched; wrapper-level ports are queued follow-up work (see the 2026-08-16
flake-hunt brief). Decision 7 in `ProtocolData.lean` (full-map gossip) means
the protocol model's reachable states have `j = 0`, where the wrapper and
the core coincide definitionally. -/
def reconcileAligned (own : TermMap) (ownDurable : Nat) (leader : TermMap) :
    ReconcileResult :=
  match leader with
  | [] => .ok ⟨ownDurable, own⟩
  | l0 :: _ =>
    match own.findIdx? (fun e => e == l0) with
    | some j =>
      match reconcile (own.drop j) ownDurable leader with
      | .ok o => .ok ⟨o.validUpTo, own.take j ++ o.newMap⟩
      | .noCommonPrefix => .noCommonPrefix  -- unreachable: (own.drop j).head = l0
    | none =>
      if own.isEmpty then .ok ⟨ownDurable, []⟩
      else if ownDurable < l0.2 then .noCommonPrefix
      else
        let cut0 := min ownDurable l0.2
        let cut := match own.find? (fun e => l0.1 ≤ e.1) with
          | some e => min cut0 e.2
          | none => cut0
        .ok ⟨cut, own.filter (fun e => e.2 < cut)⟩

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

-- 2026-08-16 window-alignment fix: wrapper guards (Rust regression tests).
-- windowed_leader_map_aligns_against_full_own_map (shape reduced: own of 6
-- entries, window = last 3; healthy follower reconciles CLEAN, full map kept)
#guard reconcileAligned
    [(1, 0), (2, 1000), (3, 2000), (4, 3000), (5, 4000), (6, 5000)] 6000
    [(4, 3000), (5, 4000), (6, 5000)]
  == .ok ⟨6000, [(1, 0), (2, 1000), (3, 2000), (4, 3000), (5, 4000), (6, 5000)]⟩
-- windowed alignment still cuts at real divergence (leader term below durable
-- that our aligned run lacks)
#guard reconcileAligned
    [(1, 0), (2, 1000), (3, 2000), (5, 4000)] 5000
    [(3, 2000), (6, 4500)]
  == .ok ⟨4000, [(1, 0), (2, 1000), (3, 2000)]⟩
-- window_start_inside_our_bytes_but_unknown_term_cuts_there
#guard reconcileAligned [(1, 0), (2, 2000)] 5000 [(40, 4000), (41, 9000)]
  == .ok ⟨4000, [(1, 0), (2, 2000)]⟩
-- genuine purged-prefix window (strictly beyond our bytes) still surfaces
#guard reconcileAligned [(1, 0)] 5000 [(40, 1048576), (41, 2097152)]
  == .noCommonPrefix
-- aligned-at-0 wrapper coincides with the core on the old contract cases
#guard reconcileAligned [(1, 0), (2, 5000)] 5000 [(1, 0), (3, 5000)]
  == .ok ⟨5000, [(1, 0)]⟩
#guard reconcileAligned [] 0 [(1, 0), (2, 5000)] == .ok ⟨0, []⟩
end
