# Aeneas retry: the real Rust in Lean, 2026-11-10

Part of the Lean proofs record. The first attempt is the Phase 1.5 section of [`uc2-lean-gate-2026-07-16.md`](uc2-lean-gate-2026-07-16.md). The page that explains the proofs is [`docs/verification/lean-proofs.md`](../verification/lean-proofs.md).

## The short version

Aeneas is a toolchain that turns Rust into Lean definitions mechanically. In July it could not build against this repo's Lean version. On 2026-11-10 both sides were on Lean v4.35.0, so the attempt was run again. It worked, with one condition.

- Two of the three consensus kernels translate exactly as they ship: the commit tracker and the vote rule.
- One theorem is proved about real translated code. `log_ok_order_eq_model` says the translation of the shipped `election.rs::log_ok_order` returns exactly the Lean model's `Uc2.logOk`, for every input. It rests only on the standard three axioms.
- The third kernel, `reconcile`, does not translate as it ships. Three spots added by the 2026-08-16 alignment fix use constructs Aeneas cannot handle. A scratch copy with those three spots rewritten as plain loops translates completely, with no axioms and no `sorry`. It still passes all 92 of `uc_consensus`'s library tests, and the conformance rig cannot tell it apart from the shipped code.

Nothing from this run is committed to the code. The rewrite of `reconcile` changes a shipped consensus kernel, so it is a decision for its own change, not a side effect of a spike.

## Terms used on this page

- **Charon** is Aeneas's front end. It reads a Rust crate through the compiler and writes its code as LLBC, a simplified form of the compiler's internal representation.
- **Aeneas** reads LLBC and writes Lean. Each Rust function becomes a Lean function that returns a `Result`, so a panic or an overflow is an explicit error value rather than a crash.
- **An external definition** is a standard-library function Aeneas has no Lean model for. Aeneas emits it as an `axiom`: a name with a type and no meaning. A theorem that reaches one says nothing about real behaviour until someone supplies a definition.
- **The model** is `Uc2Model`, the hand-written Lean description of the kernels that the proofs in `proofs/` are about.

## Tools and versions

The tools are prebuilt nightly releases, so neither OCaml nor the Aeneas Lean library had to be compiled. Every version below was read from the tools or from the upstream files, not from earlier notes.

| piece | version | how it was checked |
|---|---|---|
| Aeneas | `nightly-2026.11.09-5a392f4` | `aeneas -version` |
| Aeneas's Lean library | Lean `v4.35.0`, mathlib `v4.35.0` at rev `56a00585` | `backends/lean/lean-toolchain` and `lake-manifest.json` in the release |
| Charon | `0.1.315` at `454586e9` | `charon version`. The binary ships inside the Aeneas release and is exactly the commit in Aeneas's `charon-pin` file. |
| Charon's Rust | `nightly-2026-09-17` | Charon's `rust-toolchain` file |
| this repo's proofs | Lean `v4.35.0`, mathlib `v4.35.0` at rev `56a00585` | `proofs/lean-toolchain` and `proofs/lake-manifest.json`, after PR #201 |

`nightly-2026.11.09` is the newest Aeneas nightly still on Lean v4.35.0. Aeneas's `main` moved to `v4.36.0-rc2` later the same day. The first v4.35.0 commit was `1238879f`, on 2026-10-15.

Both mathlib and batteries resolve to the same revision in `proofs/` and in Aeneas. So the version-diamond problem the July attempt expected does not arise. A Lake package can require both `proofs/` and Aeneas with no ordering fix.

## What was run

The tools were unpacked under `~/.cache/aeneas-toolchain/` and run from a worktree of this repo.

```bash
# 1. Rust to LLBC, starting from the five functions the conformance rig drives
charon cargo --preset aeneas --dest OUT \
  --start-from crate::reconcile::reconcile \
  --start-from crate::election::log_ok_order \
  --start-from crate::commit::CommitTracker::advance \
  --start-from crate::commit::CommitTracker::on_durable \
  --start-from crate::commit::CommitTracker::reset_reports

# 2. LLBC to Lean, split so the external definitions land in their own file
aeneas -backend lean -dest split -split-files -use-lean-modules false uc_consensus.llbc

# 3. a Lake package that requires both Aeneas's backends/lean and proofs/, then
lake build
```

`-use-lean-modules false` matches `proofs/`, whose files do not use Lean's module system.

## Results, kernel by kernel

### The commit tracker translates, over three external definitions

`CommitTracker::advance`, `on_durable` and `reset_reports` translate with full bodies. The translation of `advance` follows the Rust step for step: clear the scratch vector, push the leader's own durable position, append the reports, sort descending, take the quorum-th entry, clamp it to the leader's own position, and compare it with the current commit.

It rests on three external definitions:

- `core::slice::sort_unstable_by`, used by `advance`
- `alloc::vec::Vec::clear`, used by `advance`
- `IntoIterator for &mut Vec`, used by the `reset_reports` loop

No theorem about the commit tracker was attempted. One would need a Lean definition for each of the three. The sort is the substantial one: a statement that the result is a descending permutation of the input.

### The vote rule translates, and one theorem is proved

`log_ok_order` translates to one call: Rust's tuple `>=` on `(term, durable)` pairs. Aeneas has no model of tuple ordering, so that call arrived as an external definition. It was given this definition, which follows `lexical_ord!` in Rust's `core/src/tuple.rs`:

```lean
def Pair.Insts.CoreCmpPartialOrdPair.ge ... : (U × T) → (U × T) → Result Bool :=
  fun a b => do
  let c ← cmpPartialOrdInst.partial_cmp a.1 b.1
  match c with
  | some Ordering.eq => cmpPartialOrdInst1.ge a.2 b.2
  | some Ordering.gt => ok true
  | some Ordering.lt => ok false
  | none => ok false
```

With that, the theorem is:

```lean
theorem log_ok_order_eq_model (ot : U32) (od : U64) (ct : U32) (cd : U64) :
    uc_consensus.election.log_ok_order ot od ct cd
      = ok (Uc2.logOk ot.val od.val ct.val cd.val)
```

It is proved with `#print axioms` reporting only `propext`, `Classical.choice` and `Quot.sound`. The remaining external definitions do not appear in that list, and neither does the `sorry` that stands in for `reconcile`.

The theorem rests on one piece of trust beyond the Lean kernel: the hand-written definition of tuple `ge` above. It is nine lines and matches the Rust source it names. Every other definition in the chain is Aeneas's translation or Aeneas's library.

### Reconcile does not translate as shipped

Aeneas stopped on `reconcile`'s body with two errors. Both are in the window-alignment code added by the 2026-08-16 acked-write-loss fix. The July attempt only reached Charon, and only on the code from before that fix.

- **`own.iter().find(...)` at `reconcile.rs:138`.** Aeneas cannot yet translate the signature of `Iterator::find` on a slice iterator. Its error is "a lifetime constraint relating a higher-ranked lifetime to a free lifetime".
- **The `while` loop at `reconcile.rs:160`.** Aeneas could not reconcile the state of the program at the loop's join point. Its error is "Could not match the contexts". The loop's condition compares two tuples with `==`.

The same function also calls `own.iter().position(...)`, which Charon emitted as an external definition.

### A scratch rewrite of reconcile does translate

To measure what it would take, a scratch copy of the crate was changed in exactly those three places:

- The `position` call became an index loop that finds the first entry equal to `leader[0]`.
- The `find` call became an index loop that clamps `cut` at the first entry whose term is at least `leader[0]`'s.
- The `while` condition compares the two fields of each tuple separately instead of the tuples.

Three checks say the rewrite behaves exactly like the shipped function:

- All 92 of `uc_consensus`'s library tests pass on it. That includes the 15 tests in `reconcile.rs`: 13 unit tests and 2 lineage-property tests.
- At seed 1, `conform_gen` writes the same 100,000 vectors, byte for byte, from the rewrite as from the shipped code. The rig records each kernel's output on each input, so this means the two agree on all of those inputs.
- The rig's replay against the Lean model passes on 100,000 vectors at seed 1 and 100,000 at seed 20261110.

Aeneas translated the rewrite with no errors, no external definitions and no `sorry`, and the result built against Aeneas's library on Lean v4.35.0. So `reconcile` is the cheapest kernel to reason about once translated, because it needs nothing taken on trust.

The rewrite, against the shipped file:

```diff
-    let j = match own.iter().position(|&e| e == leader[0]) {
+    let mut pos: Option<usize> = None;
+    let mut i = 0;
+    while i < own.len() {
+        if own[i].0 == leader[0].0 && own[i].1 == leader[0].1 {
+            pos = Some(i);
+            break;
+        }
+        i += 1;
+    }
+    let j = match pos {
         Some(j) => j,
 ...
-            if let Some(&(_, base)) = own.iter().find(|&&(t, _)| t >= leader[0].0) {
-                cut = cut.min(base);
+            let mut i = 0;
+            while i < own.len() {
+                if own[i].0 >= leader[0].0 {
+                    cut = cut.min(own[i].1);
+                    break;
+                }
+                i += 1;
             }
 ...
-    while j + k < own.len() && k < leader.len() && own[j + k] == leader[k] {
+    while j + k < own.len()
+        && k < leader.len()
+        && own[j + k].0 == leader[k].0
+        && own[j + k].1 == leader[k].1
+    {
```

## What this changes

The standing link between the model and the Rust is still the conformance rig plus the `#guard` pins. That has not changed. What has changed is that the mechanical route is now shown to work here, not only shown to be blocked.

These are the next steps, in order of cost:

1. **Adopt the loop form of `reconcile` in the shipped code.** It is a small change to a proved kernel. It needs the procedure in `docs/ops/uc2-runbook.md` §9 for changing a proved kernel, including a conformance run. Its precedent is the comment already in `reconcile`, which chose a loop over an iterator chain for this very toolchain.
2. **Prove that translated `reconcile` equals the model's `reconcileAligned`.** It is the shipped function, it needs no external definitions, and it is the kernel whose aligned branch the theorems R1 to R6 do not yet cover.
3. **Supply definitions for the commit tracker's three external functions** and prove the translation equals the model's tracker.
4. **Run the extraction in CI**, so that a change to a kernel that breaks the translation is caught where it is made.
