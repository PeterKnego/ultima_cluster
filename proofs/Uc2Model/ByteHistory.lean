import Uc2Model.TermMap

/-! Logs as term-stamped byte sequences — the semantic ground the deep
Reconcile theorems (R2–R4) stand on. Mirrors the sim's content-identity
oracle (`uc_sim/src/invariants.rs`: `term_at` equality ⇔ content identity,
`first_content_divergence`). -/

namespace Uc2

/-- A log's content identity: the term stamped on each byte position. -/
abbrev ByteHistory := Nat → Nat

namespace TermMap

/-- `m` encodes history `h` up to `durable`: the map is the run-length
encoding of the stamps actually held. -/
def encodes (m : TermMap) (h : ByteHistory) (durable : Nat) : Prop :=
  ∀ p, p < durable → h p = m.termAt p

end TermMap

/-- `uc_sim/src/invariants.rs::first_content_divergence`: the least position
`< bound` where the two histories' stamps differ, or `bound` if none. -/
def firstDivergence (a b : ByteHistory) (bound : Nat) : Nat :=
  match (List.range bound).find? (fun p => a p ≠ b p) with
  | some p => p
  | none => bound

end Uc2

#guard Uc2.firstDivergence (fun _ => 1) (fun _ => 1) 100 == 100
#guard Uc2.firstDivergence
  (Uc2.TermMap.termAt [(1, 0)])
  (Uc2.TermMap.termAt [(1, 0), (2, 2000)]) 3000 == 2000
