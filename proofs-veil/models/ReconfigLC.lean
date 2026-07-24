import Veil

/- UC v2.1 (M7) reconfiguration — LEADER-COMPLETENESS extension (spike V-M7 commit
   plane, scratch — NEVER the record). Builds on Reconfig.lean by adding a commit/log
   plane so the reconfig DATA-loss hazard (not just election safety) is checkable.

   Closes Finding F-M7-2: a faithful leader_completeness check needs the config-entry-
   in-log-PREFIX coupling. Config changes are LOG ENTRIES that sit AFTER committed data
   entries, so a node adopting a config entry must already hold the committed prefix.

   Single tracked committed entry E (bounded): `committed` + `committedTerm` +
   `entryHolders` (the nodeSet with E). The UN-abstracted election restriction (a voter
   holding E won't grant to a candidate lacking E — the general Raft leader-completeness
   mechanism) replaces Reconfig.lean's nondeterministic grant guard.

   TWO knobs:
     * adjacencyGuard — single-server-adjacent adoption (Reconfig.lean's knob).
     * prefixCoupling  — adopting a config entry requires holding the committed prefix
                         (the F-M7-2 mechanism: config-as-log-entry).
   Hypothesis: prefixCoupling is the load-bearing guard for DATA safety; the knob matrix
   below adjudicates it. -/

veil module UcReconfigLC

type node
type term
type nodeSet

instantiate tot : TotalOrderWithZero term
instantiate nset : TSet node nodeSet

immutable individual genesis : nodeSet
immutable individual adjacencyGuard : Bool
immutable individual prefixCoupling : Bool

-- election plane
relation candidate (N : node)
relation leader    (N : node)
function curTerm   (N : node) : term
relation hasVoted  (N : node)
function voteTerm  (N : node) : term
function voteCand  (N : node) : node
function votes     (N : node) : nodeSet
relation reqVote   (C : node) (T : term)
relation voteMsg   (V : node) (C : node) (T : term)

-- config plane
function cfg        (N : node) : nodeSet
relation pending    (I : node)
relation hasProposal(I : node)
function proposed   (I : node) : nodeSet

-- commit/log plane: a single tracked committed entry E.
individual committed     : Bool        -- E has been committed
individual committedTerm : term        -- the term E was committed in
individual entryHolders  : nodeSet      -- the nodes whose log contains E

#gen_state

theory ghost relation tlt (x y : term) := tot.le x y ∧ x ≠ y
theory ghost relation majorityOf (q c : nodeSet) := nset.count q * 2 > nset.count c
theory ghost relation adjacent (a b : nodeSet) :=
  nset.count (nset.diff a b) + nset.count (nset.diff b a) ≤ 1

after_init {
  candidate N := false
  leader N := false
  curTerm N := tot.zero
  hasVoted N := false
  voteTerm N := tot.zero
  voteCand N := N
  votes N := nset.empty
  reqVote C T := false
  voteMsg V C T := false
  cfg N := genesis
  pending N := false
  hasProposal N := false
  proposed N := genesis
  committed := false
  committedTerm := tot.zero
  entryHolders := nset.empty
}

-- ---------- election plane (with the UN-abstracted up-to-date restriction) ----------

action startElection (i : node) (t : term) {
  require ¬ leader i
  require tlt (curTerm i) t
  curTerm i := t
  candidate i := true
  leader i := false
  hasVoted i := true
  voteTerm i := t
  voteCand i := i
  votes i := nset.insert i nset.empty
  reqVote i t := true
}

-- grant arm: voter j grants to candidate c UNLESS j holds E in its LOG and c does not.
-- The Raft up-to-date restriction keys on log contents, NOT commit status: a voter
-- holding E must refuse a candidate lacking it even before E commits (otherwise a vote
-- cast pre-commit can strand a soon-to-commit entry). Sound for the single-entry model
-- (no divergent logs): "has E" = "at least as up-to-date w.r.t. E".
action deliverRequestVoteGrant (j : node) (c : node) (t : term) {
  require reqVote c t
  require tot.le (curTerm j) t
  require ¬ (hasVoted j ∧ voteTerm j = t ∧ voteCand j ≠ c)
  require ¬ (nset.contains j entryHolders ∧ ¬ nset.contains c entryHolders)
  if tlt (curTerm j) t then candidate j := false
  if tlt (curTerm j) t then leader j := false
  curTerm j := t
  hasVoted j := true
  voteTerm j := t
  voteCand j := c
  voteMsg j c t := true
}

action deliverVote (i : node) (v : node) {
  require candidate i ∨ leader i
  require voteMsg v i (curTerm i)
  votes i := nset.insert v (votes i)
}

action becomeLeader (i : node) {
  require candidate i
  require majorityOf (nset.intersection (votes i) (cfg i)) (cfg i)
  leader i := true
  candidate i := false
}

action crashRestart (i : node) {
  candidate i := false
  leader i := false
  votes i := nset.empty
}

-- ---------- commit/log plane ----------

-- leader i appends the entry E to its own log (E is authored in i's term).
action appendEntry (i : node) {
  require leader i
  require ¬ committed
  require nset.contains i (cfg i)                  -- author is a current voter
  entryHolders := nset.insert i entryHolders
}

-- leader i replicates E to node j (term-stamped frame → j hears from leader i).
action replicate (i : node) (j : node) {
  require leader i
  require nset.contains i entryHolders
  require tot.le (curTerm j) (curTerm i)
  candidate j := false
  leader j := false
  curTerm j := curTerm i
  entryHolders := nset.insert j entryHolders
}

-- E commits once a majority of the leader's config holds it (new-config quorum).
action commitEntry (i : node) {
  require leader i
  require ¬ committed
  require nset.contains i entryHolders
  require majorityOf (nset.intersection entryHolders (cfg i)) (cfg i)
  committed := true
  committedTerm := curTerm i
}

-- ---------- config plane ----------

-- config entries sit AFTER E in the log, so a leader proposing one must hold E.
action proposeRemove (i : node) (x : node) {
  require leader i
  require ¬ pending i
  require nset.contains x (cfg i)
  require nset.count (cfg i) > 1
  require (¬ committed) ∨ nset.contains i entryHolders
  cfg i := nset.remove x (cfg i)
  proposed i := nset.remove x (cfg i)
  hasProposal i := true
  pending i := true
}

action proposeAdd (i : node) (x : node) {
  require leader i
  require ¬ pending i
  require ¬ nset.contains x (cfg i)
  require (¬ committed) ∨ nset.contains i entryHolders
  cfg i := nset.insert x (cfg i)
  proposed i := nset.insert x (cfg i)
  hasProposal i := true
  pending i := true
}

-- follower j adopts leader i's config entry. Term-coupled (heard from leader) AND,
-- under prefixCoupling, requires j already hold the committed prefix E — since the
-- config entry sits after E in the log, j cannot have it without E.
action adopt (j : node) (i : node) {
  require hasProposal i
  require tot.le (curTerm j) (curTerm i)
  require (¬ adjacencyGuard) ∨ adjacent (proposed i) (cfg j)
  require (¬ prefixCoupling) ∨ (¬ committed) ∨ nset.contains j entryHolders
  candidate j := false
  leader j := false
  curTerm j := curTerm i
  cfg j := proposed i
}

action commitCfg (i : node) {
  require pending i
  pending i := false
  hasProposal i := false
}

-- ---------- safety ----------

safety [election_safety]
  (leader N1 ∧ leader N2 ∧ curTerm N1 = curTerm N2) → N1 = N2

-- LEADER COMPLETENESS: any leader of term ≥ committedTerm holds the committed entry E.
-- This is the property the reconfig disjoint-quorum hazard attacks.
safety [leader_completeness]
  (committed ∧ leader L ∧ tot.le committedTerm (curTerm L)) → nset.contains L entryHolders

#gen_spec

open Std

-- TRACTABILITY BOUNDARY (findings, see gate doc §V-M7 commit plane):
--  * #check_invariants (SMT, all-n) does NOT work on this model: the concrete
--    TSet ops (count/intersection/subsets are List-backed) break VC generation
--    with "incorrect number of universe levels List" — the concrete-set model
--    cannot take the SMT inductiveness path. (An abstract-quorum reformulation
--    — member/quorumin relations + adjacency assumptions, VerticalPaxos-style —
--    would, but needs the LC-arc-style inductive supporting invariants.)
--  * #model_check (explicit-state) handles the concrete cardinality, but the
--    config+commit state space EXPLODES at n=3: BFS to the deep (~13-step)
--    data-loss CE did not complete in 700s. Bounding term:=Fin 2 hides the CE
--    (term-coupled adopt needs a fresh term). A larger box gives more bounded
--    coverage but the safe direction is exponential, not compute-bound.
-- Runnable form kept for reference (knobs (F,F) = the data-loss CE hunt):
#model_check { node := Fin 3, term := Fin 3, nodeSet := ExtTreeSet (Fin 3) compare }
  { genesis := Std.ExtTreeSet.empty.insertMany (List.finRange 3),
    adjacencyGuard := false,
    prefixCoupling := false }
  (maxDepth := 13)
