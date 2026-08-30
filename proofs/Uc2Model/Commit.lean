import Uc2Model.TermMap

/-! `uc_consensus/src/commit.rs::CommitTracker` — quorum commit ranking
(spec §6): commit = the quorum-th highest of {own durable} ∪ {reports},
bounded by own, monotonic. -/

namespace Uc2

/-- `commit.rs::CommitTracker`. Constructor preconditions
(`cluster_size > n_followers`, `n_followers + 1 > cluster_size / 2`) are
Rust `assert!`s; the model takes them as hypotheses where needed and the
conformance generator only emits valid configurations. -/
structure CommitTracker where
  reported : List Nat
  quorum   : Nat
  commit   : Nat
deriving Repr, DecidableEq

namespace CommitTracker

/-- `commit.rs::new`. -/
def new (nFollowers clusterSize : Nat) : CommitTracker :=
  { reported := List.replicate nFollowers 0
    quorum := clusterSize / 2 + 1
    commit := 0 }

/-- `commit.rs::on_durable`. Out-of-range `idx` is a no-op (Rust would panic;
the conformance generator never emits it). Takes the report AS GIVEN — NOT a high-water
mark. It was `max` until 2026-08-16; a follower's durable genuinely regresses
when it truncates (reconcile cut, wipe, restart onto a shorter journal), and
keeping the pre-truncation mark let the leader rank a quorum that no longer
existed. Over-counting is a safety bug (a phantom commit); under-counting is
only a liveness delay, and `advance` keeps `commit` monotone regardless. -/
def onDurable (t : CommitTracker) (idx durable : Nat) : CommitTracker :=
  { t with reported := t.reported.set idx durable }

/-- `commit.rs::reset_reports` — term transition: stale-term reports must not
certify bytes in the new term. Commit itself stays monotonic. -/
def resetReports (t : CommitTracker) : CommitTracker :=
  { t with reported := List.replicate t.reported.length 0 }

/-- The descending ranking of {own} ∪ reported. -/
def ranking (t : CommitTracker) (own : Nat) : List Nat :=
  (own :: t.reported).mergeSort (fun a b => decide (b ≤ a))

/-- `commit.rs::advance` — rank the quorum; `some new_commit` iff advanced.
`ranked = scratch[quorum-1].min(own)`. -/
def advance (t : CommitTracker) (own : Nat) : CommitTracker × Option Nat :=
  let ranked := min ((t.ranking own).getD (t.quorum - 1) 0) own
  if t.commit < ranked then ({ t with commit := ranked }, some ranked)
  else (t, none)

/-- Event alphabet for the fold (the agent's driving calls). -/
inductive Ev where
  | report (idx durable : Nat)
  | reset
  | advance (own : Nat)
deriving Repr, DecidableEq

def step (t : CommitTracker) : Ev → CommitTracker
  | .report idx d => t.onDurable idx d
  | .reset => t.resetReports
  | .advance own => (t.advance own).1

def run (t : CommitTracker) (evs : List Ev) : CommitTracker :=
  evs.foldl step t

end CommitTracker
end Uc2

-- Ports of the commit.rs unit tests (binding contract), as executable pins.
open Uc2 CommitTracker in
section
-- three_node_commit_is_second_highest_bounded_by_own
#guard ((new 2 3).advance 1000).2 == none
#guard (((new 2 3).onDurable 0 400).advance 1000).2 == some 400
#guard ((((new 2 3).onDurable 0 400).onDurable 1 700).advance 1000).2 == some 700
#guard (let t := (((new 2 3).onDurable 0 400).onDurable 1 700).advance 1000 |>.1
        let t := (t.onDurable 0 5000).onDurable 1 5000
        (t.advance 1000).2) == some 1000
#guard (let t := (((new 2 3).onDurable 0 400).onDurable 1 700).advance 1000 |>.1
        let t := ((t.onDurable 0 5000).onDurable 1 5000).advance 1000 |>.1
        (t.advance 1000).2) == none -- no re-advance without movement
#guard (let t := (((new 2 3).onDurable 0 400).onDurable 1 700).advance 1000 |>.1
        let t := ((t.onDurable 0 5000).onDurable 1 5000).advance 1000 |>.1
        ((t.advance 1000).1.advance 4000).2) == some 4000 -- own durable catches up
-- reports_are_monotonic_per_follower_and_commit_never_regresses
#guard (let t := ((new 2 3).onDurable 0 800).onDurable 1 900
        let (t, r1) := t.advance 1000
        let t := t.onDurable 1 100
        let (t, r2) := t.advance 1000
        (r1, r2, t.commit)) == (some 900, none, 900)
-- four_node_even_cluster_commit_is_third_highest
#guard ((((new 3 4).onDurable 0 90).onDurable 1 80).advance 100).2 == some 80
#guard (((new 3 4).onDurable 0 90).advance 100).2 == none
-- five_node_commit_is_third_highest
#guard (((((new 4 5).onDurable 0 90).onDurable 1 80).onDurable 2 70).advance 100).2
  == some 80
-- quorum_loss_never_commits_on_own_durable_alone
#guard ((new 2 3).advance 999999999).2 == none
-- untracked_member_counts_as_permanent_zero
#guard (((new 1 3).onDurable 0 700).advance 1000).2 == some 700
#guard (let t := ((new 1 3).onDurable 0 700).advance 1000 |>.1
        ((t.onDurable 0 2000).advance 1000).2) == some 1000
-- reset_reports_clears_slots_but_keeps_commit
#guard (let t := ((new 2 3).onDurable 0 5000).onDurable 1 5000
        let (t, _) := t.advance 1000
        let t := t.resetReports
        let (t, r) := t.advance 6000
        (r, t.commit)) == (none, 1000)
end
