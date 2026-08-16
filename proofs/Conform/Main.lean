import Uc2Model
import Lean.Data.Json

/-! Conformance replay: reads JSONL vectors emitted by
`uc2_consensus/examples/conform_gen.rs`, replays each through `Uc2Model`,
exits 1 on the first divergence (printing the offending line). Uses core
`Lean.Data.Json` only — no mathlib. -/

open Lean Uc2

def getNat! (j : Json) (k : String) : Except String Nat := do
  let v ← j.getObjVal? k
  v.getNat?

def getMap! (j : Json) (k : String) : Except String TermMap := do
  let v ← j.getObjVal? k
  let arr ← v.getArr?
  arr.foldrM (init := []) fun e acc => do
    let pair ← e.getArr?
    let t ← pair[0]!.getNat?
    let b ← pair[1]!.getNat?
    pure ((t, b) :: acc)

def checkReconcile (j : Json) : Except String Bool := do
  let own ← getMap! j "own"
  let d ← getNat! j "own_durable"
  let leader ← getMap! j "leader"
  let expect ← j.getObjVal? "expect"
  let kind ← (← expect.getObjVal? "kind").getStr?
  -- 2026-08-16: Rust's `reconcile` is the window-ALIGNED form; the model's
  -- `reconcile` remains the unchanged core the theorems are stated over.
  match reconcileAligned own d leader, kind with
  | .noCommonPrefix, "no_common_prefix" => pure true
  | .ok o, "ok" => do
    let v ← getNat! expect "valid_up_to"
    let m ← getMap! expect "new_map"
    pure (o.validUpTo == v && o.newMap == m)
  | _, _ => pure false

def checkAdvanceFold (j : Json) : Except String Bool := do
  let nF ← getNat! j "n_followers"
  let cS ← getNat! j "cluster_size"
  let evsJ ← (← j.getObjVal? "events").getArr?
  let evs ← evsJ.toList.mapM fun e => do
    let a ← e.getArr?
    let tag ← a[0]!.getStr?
    match tag with
    | "reset" => pure CommitTracker.Ev.reset
    | "advance" => pure (CommitTracker.Ev.advance (← a[1]!.getNat?))
    | "report" =>
      pure (CommitTracker.Ev.report (← a[1]!.getNat?) (← a[2]!.getNat?))
    | other => throw s!"bad event tag {other}"
  let expect ← getNat! (← j.getObjVal? "expect") "commit"
  pure (((CommitTracker.new nF cS).run evs).commit == expect)

def checkLogOk (j : Json) : Except String Bool := do
  let ot ← getNat! j "our_term"
  let od ← getNat! j "our_durable"
  let ct ← getNat! j "cand_term"
  let cd ← getNat! j "cand_durable"
  let expect ← (← j.getObjVal? "expect").getBool?
  pure (logOk ot od ct cd == expect)

def checkLine (line : String) : Except String Bool := do
  let j ← Json.parse line
  let fn ← (← j.getObjVal? "fn").getStr?
  match fn with
  | "reconcile" => checkReconcile j
  | "advance_fold" => checkAdvanceFold j
  | "log_ok" => checkLogOk j
  | other => throw s!"unknown fn {other}"

def main (args : List String) : IO UInt32 := do
  let some (path : String) := args[0]? | do
    IO.eprintln "usage: conform <vectors.jsonl>"
    return 2
  let lines ← IO.FS.lines path
  let mut n := 0
  for line in lines do
    if line.isEmpty then continue
    match checkLine line with
    | .ok true => n := n + 1
    | .ok false =>
      IO.eprintln s!"CONFORMANCE DIVERGENCE at vector {n}:\n{line}"
      return 1
    | .error e =>
      IO.eprintln s!"vector {n} malformed ({e}):\n{line}"
      return 2
  IO.println s!"conform: {n} vectors OK"
  return 0
