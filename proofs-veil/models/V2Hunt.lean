import Veil

/- UC v2 — V2 COHERENCE-WINDOW FORWARD HUNT (scratch — NEVER the record).

   This is the ONLY part of the spike that hunts an UNKNOWN bug. Everything before it
   was backward-looking calibration: revert a known fix, confirm the checker finds the
   known bug, restore the fix, confirm it disappears. Here EVERY SHIPPED FIX IS ON, so
   any counterexample is a bug nobody knows about (or — as eleven times already this
   session — a gap in my model, to be adjudicated against the Rust before it is called
   anything else).

   Gated on Bar-2b (passed): the frame abstraction demonstrably still distinguishes a
   stale-handle-term stream byte from a current-term one at the same position, so a
   null result here is informative rather than vacuous.

   AIMED PER THE DEPTH-PROBE CALIBRATION (gate doc §3b/§3c): hunt PROXY INVARIANTS —
   the conditions the shipped guards establish — not end-to-end loss properties. The
   known bugs' proxies sat at depth 5 and 7 while their losses were beyond depth 13
   (and 46 steps at n=5 in Lean). A forward hunt for a loss property would search where
   the checker provably cannot reach; a hunt for guard-shaped invariants searches where
   it demonstrably can.

   Base: `Finding9.lean` (gate + vote + lagging `handleTerm` + `tailAttributed` +
   commit plane), with `crashRestart` RESTORED — the brief asks for concurrent
   `startElection` / `crashRestart` / gate-reopen / commit interleavings, and crash is
   the one window ingredient `Finding9.lean` had dropped.

   ---- ON state_constraint (deliberately NOT used in run 1) ----
   `state_constraint` PRUNES states: anything filtered is never explored. For a hunt
   whose entire value is finding something unknown, aggressive narrowing is
   self-defeating — you cannot find what you pruned. So run 1 is UNCONSTRAINED and
   UNBOUNDED (no maxDepth) at n=3, which prior runs suggest is affordable; that yields
   a genuine EXHAUSTIVE coverage claim. Constraints are held in reserve for n=4, where
   they buy tractability at a cost that must then be stated explicitly. -/

veil module UcV2Hunt

-- MUST come AFTER `veil module`: opening a module resets `maxHeartbeats` to 500000
-- (Veil/Base.lean:52, veilDefaultOptions), clobbering any earlier setting.
-- `crashRestart` carries six conditional assignments (boot recovery writes curTerm AND
-- the handle on each arm of max(vote, map), plus the gate/latch pair) and exceeds the
-- default budget during action elaboration.
set_option maxHeartbeats 2000000

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
safety [election_safety]
  (leader N1 ∧ leader N2 ∧ curTerm N1 = curTerm N2) → N1 = N2

#gen_spec

-- RUN 1 — UNCONSTRAINED, UNBOUNDED, n=3. No maxDepth, so `✅ No violation` here would
-- mean genuinely EXHAUSTIVE coverage of the reachable space, not a depth-limited miss.
#model_check { node := Fin 3, term := Fin 3 } { }
