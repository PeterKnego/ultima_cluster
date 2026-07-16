/-! Ascending `(term, base)` maps — the wire/consensus term maps of
`uc2_consensus/src/reconcile.rs` (module docs) and the `term_at` oracle of
`uc2_sim/src/invariants.rs::term_at`. -/

namespace Uc2

/-- `uc2_consensus`: an ascending `(term, base)` map — term `t`'s bytes begin
at byte position `base`. `Nat` abstracts `u32`/`u64` faithfully: the kernels
only compare (no wrapping arithmetic). -/
abbrev TermMap := List (Nat × Nat)

namespace TermMap

/-- The well-formedness the Rust maintains at every construction site:
terms strictly ascending, bases non-strictly ascending (a zero-byte frontier
entry may share its base with its successor). -/
def Ascending : TermMap → Prop
  | [] => True
  | [_] => True
  | a :: b :: rest => a.1 < b.1 ∧ a.2 ≤ b.2 ∧ Ascending (b :: rest)

/-- `uc2_sim/src/invariants.rs::term_at`: the term covering byte `pos` —
the term of the greatest entry whose base is `<= pos`, or 0 below the first
entry. Within a term, bytes are cluster-identical (spec §6), so the term at a
position IS its content identity. -/
def termAt (m : TermMap) (pos : Nat) : Nat :=
  m.foldl (fun acc e => if e.2 ≤ pos then e.1 else acc) 0

end TermMap
end Uc2

-- Executable pins (Rust semantics, small worked examples):
#guard Uc2.TermMap.termAt [] 0 == 0
#guard Uc2.TermMap.termAt [(1, 0), (3, 4096)] 0 == 1
#guard Uc2.TermMap.termAt [(1, 0), (3, 4096)] 4095 == 1
#guard Uc2.TermMap.termAt [(1, 0), (3, 4096)] 4096 == 3
#guard Uc2.TermMap.termAt [(2, 100)] 99 == 0       -- below the first entry
#guard Uc2.TermMap.termAt [(1, 0), (2, 5000), (3, 5000)] 5000 == 3  -- phantom shadowed
