import Veil

/- UC v2.1 (M7) single-server reconfiguration, ported to Veil (spike V-M7, scratch —
   NEVER the record; the real assurance is proofs/ + the fleet gate).

   Faithful to docs/superpowers/specs/2026-07-13-uc2-reconfig-design.md:
   config-as-log-entry, leader-adopts-at-append / follower-adopts-when-durable,
   ONE change in flight, single-server (add/remove one) so ADJACENT configs
   differ by exactly one member — the overlap that makes joint consensus
   unnecessary. Election plane reused verbatim from Examples/UC/ElectionMC.lean.

   Config is a concrete per-node voter SET (nodeSet), so majority = count*2 > |cfg|
   is explicit-state enumerable (the TLA/Raft.lean isQuorum idiom applied to a
   *changing* set). The `member`/`diff`/`count` TSet primitives give single-server
   adjacency and cross-config overlap directly.

   TWO checkable regimes (toggle `adjacencyGuard`):
     * GUARDED   — adoption is single-server-adjacent + one-in-flight.
                   #model_check should report NO election_safety violation
                   across a config change = M7 design assurance.
     * ABLATED   — a follower may adopt an ARBITRARY proposed config (the guard
                   removed). The checker should FIND a disjoint-quorum double
                   leader in the same term = evidence the model preserves the
                   reconfig-safety bug class (the V-M7 calibration, cf. Bar-2b). -/

veil module UcReconfig

type node
type term
type nodeSet

instantiate tot : TotalOrderWithZero term
instantiate nset : TSet node nodeSet

-- Instantiation-time genesis voter set (e.g. {0,1,2}) + the guard toggle.
immutable individual genesis : nodeSet
immutable individual adjacencyGuard : Bool

-- Per-node election state (PNode). role: follower = ¬candidate ∧ ¬leader.
relation candidate (N : node)
relation leader    (N : node)
function curTerm   (N : node) : term
relation hasVoted  (N : node)
function voteTerm  (N : node) : term
function voteCand  (N : node) : node
-- votesReceived tally, as a concrete set (TLA/Raft votesGranted idiom).
function votes     (N : node) : nodeSet
relation reqVote   (C : node) (T : term)
relation voteMsg   (V : node) (C : node) (T : term)
-- the quorum a node actually used to win (votes ∩ cfg at becomeLeader) — history
-- variable for the reconfig-safety property (cross-leader quorum overlap).
function elecQuorum (N : node) : nodeSet

-- Config plane.
function cfg        (N : node) : nodeSet   -- N's adopted voter set
relation pending    (I : node)             -- I has an uncommitted change in flight
relation hasProposal(I : node)             -- I published a config entry to adopt
function proposed   (I : node) : nodeSet   -- the config I is proposing

#gen_state

-- strict term order
theory ghost relation tlt (x y : term) := tot.le x y ∧ x ≠ y

-- majority of a concrete config: |q| * 2 > |cfg|
theory ghost relation majorityOf (q c : nodeSet) := nset.count q * 2 > nset.count c

-- single-server adjacency: symmetric difference of a and b is at most one node.
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
  elecQuorum N := nset.empty
  reqVote C T := false
  voteMsg V C T := false
  cfg N := genesis
  pending N := false
  hasProposal N := false
  proposed N := genesis
}

-- ---------- election plane (verbatim shape from ElectionMC) ----------

action startElection (i : node) (t : term) {
  require ¬ leader i
  require tlt (curTerm i) t
  curTerm i := t
  candidate i := true
  leader i := false
  hasVoted i := true
  voteTerm i := t
  voteCand i := i
  votes i := nset.insert i nset.empty     -- tally = {i}
  reqVote i t := true
}

action deliverRequestVoteGrant (j : node) (c : node) (t : term) {
  require reqVote c t
  require tot.le (curTerm j) t
  require ¬ (hasVoted j ∧ voteTerm j = t ∧ voteCand j ≠ c)
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

-- becomeLeader: candidate i whose tally, RESTRICTED TO ITS OWN CONFIG'S VOTERS,
-- is a majority of that config. Votes from non-members do not count toward the
-- config quorum (the crux — a self-vote from a node not in its config is not a
-- quorum). This is the faithful analog of counting a majority of `cfg i`.
action becomeLeader (i : node) {
  require candidate i
  require majorityOf (nset.intersection (votes i) (cfg i)) (cfg i)
  leader i := true
  candidate i := false
  elecQuorum i := nset.intersection (votes i) (cfg i)   -- record the winning quorum
}

action crashRestart (i : node) {
  candidate i := false
  leader i := false
  votes i := nset.empty
}

-- ---------- config plane (single-server change, one in flight) ----------

-- leader i proposes removing voter x (one-server change). Leader adopts at append.
action proposeRemove (i : node) (x : node) {
  require leader i
  require ¬ pending i                    -- ONE change in flight
  require nset.contains x (cfg i)
  require nset.count (cfg i) > 1         -- never empty the voter set
  cfg i := nset.remove x (cfg i)
  proposed i := nset.remove x (cfg i)
  hasProposal i := true
  pending i := true
}

-- leader i proposes adding voter x (one-server change). Leader adopts at append.
action proposeAdd (i : node) (x : node) {
  require leader i
  require ¬ pending i
  require ¬ nset.contains x (cfg i)
  cfg i := nset.insert x (cfg i)
  proposed i := nset.insert x (cfg i)
  hasProposal i := true
  pending i := true
}

-- follower j adopts leader i's published config (follower-adopts-when-durable).
-- FIDELITY: config entries ride a term-stamped replication frame authored by
-- leader i at curTerm i. Receiving one means j has heard from leader i at that
-- term, so (Raft §5.2) a candidate/leader j of term ≤ curTerm i REVERTS to
-- follower and adopts i's term. j cannot ingest i's config entry AND remain an
-- independent same-term candidate — that incoherence was a model artifact, not
-- a UC-reachable state.
-- GUARDED: only if the target is single-server-adjacent to j's current config.
-- ABLATED (adjacencyGuard=false): j may jump to an arbitrary published config.
action adopt (j : node) (i : node) {
  require hasProposal i
  require tot.le (curTerm j) (curTerm i)              -- can't accept a stale leader's frame
  require (¬ adjacencyGuard) ∨ adjacent (proposed i) (cfg j)
  candidate j := false                                -- revert: heard from the leader
  leader j := false
  curTerm j := curTerm i
  cfg j := proposed i
}

-- the in-flight change commits (quorum of the new config); clears one-in-flight.
action commitCfg (i : node) {
  require pending i
  pending i := false
  hasProposal i := false
}

-- ---------- safety ----------

-- election safety: two leaders at the same term are the same node.
safety [election_safety]
  (leader N1 ∧ leader N2 ∧ curTerm N1 = curTerm N2) → N1 = N2

-- RECONFIG SAFETY (the property that exposes the single-server disjoint-quorum
-- hazard): any two current leaders' electing quorums must share a node. In static
-- Raft this is free (same config). Under single-server reconfig it holds because
-- ADJACENT configs overlap; if a node could adopt a NON-adjacent config (ablation),
-- two leaders can be backed by disjoint quorums — the classic data-loss shape.
safety [quorum_overlap]
  (leader N1 ∧ leader N2) →
    ∃ r, nset.contains r (elecQuorum N1) ∧ nset.contains r (elecQuorum N2)

#gen_spec

open Std

-- V-M7 CALIBRATION (ablated): drop the single-server adjacency guard so a follower
-- may adopt an ARBITRARY published config. The checker must FIND a disjoint-quorum
-- double-leader = evidence the model preserves the reconfig-safety bug class.
-- term := Fin 3: term-coupled adopt consumes a term, so exposing the disjoint
-- quorum needs a fresh higher term to re-elect under the adopted config.
-- GUARDED (adjacencyGuard := true): single-server-adjacent adoption keeps
-- consecutive configs overlapping — expect quorum_overlap SAFE = M7 assurance
-- that UC's single-server-change rule closes exactly the ablated hazard above.
#model_check { node := Fin 3, term := Fin 3, nodeSet := ExtTreeSet (Fin 3) compare }
  { genesis := Std.ExtTreeSet.empty.insertMany (List.finRange 3),
    adjacencyGuard := true }

