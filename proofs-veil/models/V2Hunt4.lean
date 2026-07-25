import Veil

/- UC v2 — V2 FORWARD HUNT AT n=4, WITH STATE CONSTRAINTS (scratch — NEVER the record).

   Follow-on to `V2Hunt.lean`, which was EXHAUSTIVE and clean at n=3/Fin 3 over
   11,697,699 states. This asks the one question n=3 cannot: **does a fourth node enable
   a coherence-window bug that three cannot?** That is not idle — at n=4 the majority is
   3-of-4, so two successive quorums can overlap in exactly TWO nodes, a structure with
   no n=3 analogue, and #6b's full loss needed n=5 in Lean.

   SEPARATE FILE ON PURPOSE: `state_constraint` is MODULE-level and applies to every
   `#model_check` in the file. Adding constraints to `V2Hunt.lean` would retroactively
   narrow its exhaustive n=3 result. That result stays constraint-free.

   ---- THE NARROWING, STATED UP FRONT ----
   n=4 is far beyond an unconstrained search: the reachable space grows super-
   exponentially (`voteMsg` alone goes from 2^27 to 2^48 assignments), and n=3
   unconstrained already took 93 minutes for 11.7M states. So this run is narrowed
   THREE ways, and every one can hide a real bug:

     C1  at most ONE node awaiting reconciliation
     C2  at most ONE node carrying a divergent tail
     C3  at most TWO simultaneous candidates

   Justification: all four known coherence-window bugs (#5, #6b, #8, #9) involve
   exactly ONE misbehaving node — one unreconciled voter, one lagging-handle candidate.
   These constraints preserve those shapes while cutting the combinatorics of multiple
   SIMULTANEOUS anomalies, which is where the n=4 blowup lives. What they buy is the
   4-node QUORUM STRUCTURE, which is the actual reason to go to n=4.

   CONSEQUENCE: **a clean result here is strictly WEAKER than the n=3 exhaustive one.**
   It reads "no bug of the known shapes at n=4, within the depth bound, under C1-C3" —
   NOT "n=4 is safe". A VIOLATION, by contrast, would be fully meaningful.

   A `maxDepth` is also set, deliberately: a killed Veil run yields ZERO output (Lean
   buffers verdicts until elaboration ends), and session 4c lost 15 minutes that way.
   A bounded run that reports beats an unbounded one that dies. -/

veil module UcV2Hunt4

-- MUST come AFTER `veil module`: opening a module resets `maxHeartbeats` to 500000
-- (Veil/Base.lean:52, veilDefaultOptions), clobbering any earlier setting.
-- `crashRestart` carries six conditional assignments (boot recovery writes curTerm AND
-- the handle on each arm of max(vote, map), plus the gate/latch pair) and exceeds the
-- default budget during action elaboration.
set_option maxHeartbeats 2000000

-- NON-VACUITY canary: true asks the checker to witness that a commit is reachable AT
-- ALL under the constraints. A violation is the GOOD outcome. Without this, a clean
-- constrained run cannot be distinguished from "the constraints pruned the mainline".
immutable individual vacuityCanary : Bool

type node
type term

instantiate tot : TotalOrderWithZero term

-- No knobs: every shipped fix is ON. This is the fixed model.

relation candidate (N : node)
relation leader    (N : node)
function curTerm   (N : node) : term
relation hasVoted  (N : node)
function voteTerm  (N : node) : term
function voteCand  (N : node) : node
relation reqVote   (C : node) (T : term)
relation voteMsg   (V : node) (C : node) (T : term)

function handleTerm (N : node) : term   -- receiver's data-plane term handle
function mapTerm    (N : node) : term   -- durable term map

relation durableTo      (N : node)
relation holdsEntry     (N : node)
relation tailAttributed (N : node)

relation gateOpen          (N : node)
relation awaitingReconcile (N : node)

relation report (V : node) (T : term)
individual committed : Bool

#gen_state

theory ghost relation tlt (x y : term) := tot.le x y ∧ x ≠ y
theory ghost relation member (n q : node) := n ≠ q

after_init {
  candidate N := false
  leader N := false
  curTerm N := tot.zero
  hasVoted N := false
  voteTerm N := tot.zero
  voteCand N := N
  reqVote C T := false
  voteMsg V C T := false
  handleTerm N := tot.zero
  mapTerm N := tot.zero
  durableTo N := false
  holdsEntry N := false
  tailAttributed N := true
  gateOpen N := true
  awaitingReconcile N := false
  report V T := false
  committed := false
}

-- ---------- election plane ----------

action startElection (i : node) (t : term) {
  require ¬ leader i
  require tlt (curTerm i) t
  curTerm i := t
  candidate i := true
  leader i := false
  hasVoted i := true
  voteTerm i := t
  voteCand i := i
  reqVote i t := true
}

action deliverRequestVoteGrant (j : node) (c : node) (t : term) {
  require reqVote c t
  require tot.le (curTerm j) t
  require ¬ (hasVoted j ∧ voteTerm j = t ∧ voteCand j ≠ c)
  if tlt (curTerm j) t then candidate j := false
  if tlt (curTerm j) t then leader j := false
  if tlt (curTerm j) t then handleTerm j := t
  if tlt (curTerm j) t then gateOpen j := false
  if tlt (curTerm j) t then awaitingReconcile j := true
  curTerm j := t
  hasVoted j := true
  voteTerm j := t
  voteCand j := c
  voteMsg j c t := true
}

action becomeLeader (i : node) (q : node) {
  require candidate i
  require member i q
  require ∀ V, member V q → (V = i ∨ voteMsg V i (curTerm i))
  leader i := true
  candidate i := false
  handleTerm i := curTerm i
  if (durableTo i ∧ ¬ holdsEntry i) then durableTo i := false   -- prime(base)
}

action revertToFollower (j : node) (i : node) {
  require leader i
  require candidate j
  require curTerm j = curTerm i
  candidate j := false
  if tlt (handleTerm j) (curTerm i) then gateOpen j := false
  if tlt (handleTerm j) (curTerm i) then awaitingReconcile j := true
  handleTerm j := curTerm i
}

-- ---------- reconcile (both arms, FIXED) ----------

action reconcileAdopt (j : node) (i : node) {
  require leader i
  require tlt (curTerm j) (curTerm i)
  if (durableTo j ∧ ¬ holdsEntry j ∧ tailAttributed j) then durableTo j := false
  candidate j := false
  leader j := false
  curTerm j := curTerm i
  handleTerm j := curTerm i
  mapTerm j := curTerm i
  gateOpen j := true
  awaitingReconcile j := false
}

-- FIXED: reopen only when the SM's active term equals the data-plane handle
-- (node.rs:2423, Finding #9's shipped guard).
action reconcileNonAdopt (j : node) (i : node) {
  require leader i
  require awaitingReconcile j
  require tot.le (curTerm i) (curTerm j)
  require tlt (handleTerm j) (curTerm i)
  if (durableTo j ∧ ¬ holdsEntry j ∧ tailAttributed j) then durableTo j := false
  mapTerm j := curTerm i
  if (curTerm j = handleTerm j) then gateOpen j := true
  if (curTerm j = handleTerm j) then awaitingReconcile j := false
}

-- ---------- data plane ----------

action appendEntry (i : node) {
  require leader i
  require ¬ committed
  holdsEntry i := true
  durableTo i := true
  tailAttributed i := true
}

action replicate (i : node) (j : node) {
  require leader i
  require holdsEntry i
  require gateOpen j
  require curTerm j = curTerm i
  require ¬ (durableTo j ∧ ¬ holdsEntry j)
  candidate j := false
  leader j := false
  holdsEntry j := true
  durableTo j := true
  tailAttributed j := true
}

action staleStreamAppend (j : node) {
  require gateOpen j
  require ¬ holdsEntry j
  require ∀ L, leader L → handleTerm j ≠ curTerm L
  durableTo j := true
  tailAttributed j := true
}

action crossStreamAccept (j : node) {
  require gateOpen j
  require ¬ holdsEntry j
  require tlt (handleTerm j) (mapTerm j)
  durableTo j := true
  tailAttributed j := false
}

action sendReport (j : node) {
  require gateOpen j
  require durableTo j
  report j (handleTerm j) := true
}

-- FIXED boot gate (node.rs:533-534): closed iff vote_term > map_term.
-- Split into the two arms of `max(vote_term, map_term)` rather than written with
-- conditional assignments: the `require`s partition the state space so the semantics
-- are identical, but a single action carrying six `if`s SEGFAULTS Veil's action
-- elaborator (exit 139) and blows the heartbeat budget before that. Zero conditionals
-- here, and the two arms of the shipped fix read explicitly.

-- Arm 1: granted a vote this node never reconciled against — gate boots CLOSED.
action crashRestartUnreconciled (j : node) {
  require tlt (mapTerm j) (voteTerm j)
  candidate j := false
  leader j := false
  curTerm j := voteTerm j
  handleTerm j := voteTerm j
  gateOpen j := false
  awaitingReconcile j := true
}

-- Arm 2: the map has caught up to the vote — gate boots OPEN, as pre-fix.
action crashRestartReconciled (j : node) {
  require tot.le (voteTerm j) (mapTerm j)
  candidate j := false
  leader j := false
  curTerm j := mapTerm j
  handleTerm j := mapTerm j
  gateOpen j := true
  awaitingReconcile j := false
}

action commitEntry (i : node) (q : node) {
  require leader i
  require ¬ committed
  require member i q
  require holdsEntry i
  require ∀ V, member V q → (V = i ∨ report V (curTerm i))
  committed := true
}

-- ---------- STATE CONSTRAINTS (the n=4 narrowing — see header) ----------
-- A state is explored ONLY if all of these hold; states violating them are silently
-- SKIPPED, not reported. That is exactly why they are dangerous for a forward hunt and
-- why the n=3 run was deliberately run without them.

-- C1 RETIRED — IT MADE THE MODEL VACUOUS. "At most one node awaiting reconciliation"
-- sounded like an anomaly bound, but `deliverRequestVoteGrant` sets
-- `awaitingReconcile := true` on EVERY node that adopts a new term, and at n=4 a
-- candidate needs THREE granters. So C1 pruned the mainline election path: no leader
-- could assemble a 3-of-4 quorum, nothing ever committed, and the whole invariant
-- battery went trivially true. The vacuity canary caught it (no violation = no commit
-- is reachable at all). Kept here as the record of a constraint that silently
-- destroyed the run it was meant to make possible.

state_constraint [c2_one_divergent_tail]
  ((durableTo N1 ∧ ¬ holdsEntry N1) ∧ (durableTo N2 ∧ ¬ holdsEntry N2)) → N1 = N2

state_constraint [c3_at_most_two_candidates]
  (candidate N1 ∧ candidate N2 ∧ candidate N3) → (N1 = N2 ∨ N1 = N3 ∨ N2 = N3)

-- ---------- THE HUNT: a battery of proxy invariants ----------
-- Each is a condition a shipped guard is supposed to establish. On the FIXED model
-- every one should hold; a violation is a candidate NEW finding (to be adjudicated
-- against the Rust before it is called anything more).

-- #9's guard: intake is never open at a lagging handle.
safety [no_cross_stream_reopen]
  ¬ (gateOpen N ∧ tlt (handleTerm N) (mapTerm N))

-- #5/#6b class: a commit is backed by a genuine quorum that holds the entry.
safety [no_phantom_commit]
  committed → ∃ (q : node), ∀ V, member V q → holdsEntry V

-- The damage #9 would cause even if the reopen were reachable: a node must never have
-- a live report at its handle term over content its map never attributed.
safety [no_unattributed_report]
  (durableTo N ∧ ¬ tailAttributed N) → ¬ report N (handleTerm N)

-- The gate/latch coupling: awaiting reconciliation implies intake shut.
safety [gate_shut_while_unreconciled]
  awaitingReconcile N → ¬ gateOpen N

-- The data-plane handle never leads the consensus term (StartElection may make it LAG,
-- never lead; only BecomeLeader/BecomeFollower write it).
safety [handle_never_leads_current]
  tot.le (handleTerm N) (curTerm N)

-- BecomeLeader stores the handle, so a sitting leader is never at a lagging handle.
safety [leader_handle_is_current]
  leader N → handleTerm N = curTerm N

-- Baseline sanity (already proved inductive at Bar-1; here as a tripwire).
safety [commit_reachable_canary]
  vacuityCanary → ¬ committed

safety [election_safety]
  (leader N1 ∧ leader N2 ∧ curTerm N1 = curTerm N2) → N1 = N2

#gen_spec

-- RUN A — VACUITY CHECK FIRST, always, on any constrained model. A REPORTED VIOLATION
-- is the good news: it witnesses that a commit is reachable under the constraints.
#model_check { node := Fin 4, term := Fin 3 }
  { vacuityCanary := true } (maxDepth := 10)

-- RUN B — the actual n=4 hunt, only meaningful if RUN A reported a violation.
#model_check { node := Fin 4, term := Fin 3 }
  { vacuityCanary := false } (maxDepth := 10)
