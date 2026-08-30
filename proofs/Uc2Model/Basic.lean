import Init.Guard

/-! # Uc2Model
Executable Lean 4 model of the `uc_consensus` pure kernels.
NO mathlib imports anywhere in this library — it must build in seconds
for the conformance loop. -/

-- Sanity anchor: the library builds and `#guard` works.
#guard 1 + 1 == 2
