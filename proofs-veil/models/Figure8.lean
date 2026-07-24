import Veil

/- UC v2 — FINDING #6b FIGURE-8 DEPTH PROBE (spike stretch, scratch — NEVER the record).

   The second calibration point for the depth probe, and a DIRECT TEST of the guidance
   that fell out of the #9 probe: hunt the PROXY INVARIANT the shipped guard
   establishes, not the end-to-end loss. #6b is the sharpest possible test of that
   claim, because its full loss was machine-checked in Lean as a **46-step, n=5**
   countermodel (`finding_fig8_old_term_commit_data_loss`, deleted post-fix) — far
   beyond any explicit-state bound reachable here — while its proxy should be trivial.

   TARGET (election.rs:1421-1465). `rank_leader` pushed `Action::AdvanceCommit` off the
   positions-only `CommitTracker` UNCONDITIONALLY. Right after an election, followers
   reconcile clean and their gate-open AppendPosition floor reports the ELECTION BASE
   before this term's NewTerm frame is quorum-durable, so the leader commits an
   OLD-TERM-ONLY range — acks/apply/outputs firing below the Raft §5.4.2 barrier.
   LOSS CONTINUATION: a divergent higher-`lastTerm` rival still wins the next term with
   a commit-quorum member's HONEST grant (the granters' data-stamped `last_term` has
   not reached this term yet) and truncates the committed bytes cluster-wide — the
   committed entry ends with ZERO copies anywhere. Classical Figure-8 acked-write loss.

   FIX: advance/store/gossip only once the ranked position covers `new_term_pos`;
   `None` (the window between `become_leader` and `Event::NewTermAppended`) means NO
   advance. Knob `clampFix`.

   TWO PROPERTIES, deliberately at different depths (the #9 probe's lesson):
   * `no_old_term_only_commit` (PROXY) — if the old range is committed, a quorum holds
     this term's NewTerm frame. Exactly the §5.4.2 barrier the clamp enforces.
   * `no_acked_write_loss` (FULL) — a committed range keeps at least one copy anywhere.
   `proxyOn` gates the former because BFS halts at the first violation. -/

veil module UcFigure8

type node
type term

instantiate tot : TotalOrderWithZero term

immutable individual clampFix : Bool
immutable individual proxyOn  : Bool

-- election plane
relation candidate (N : node)
relation leader    (N : node)
function curTerm   (N : node) : term
relation hasVoted  (N : node)
function voteTerm  (N : node) : term
function voteCand  (N : node) : node
relation reqVote   (C : node) (T : term)
relation voteMsg   (V : node) (C : node) (T : term)

-- data-stamped last term of a node's log: the `last_term` half of UC's lexicographic
-- (last_term, last_durable) vote comparison. The whole point of #6b's loss
-- continuation is that a commit-quorum member's `last_term` has NOT yet reached the
-- committing leader's term, so its grant to a divergent rival is HONEST.
function lastTerm (N : node) : term

-- data plane. Two tracked ranges: the inherited OLD-term range (the election base)
-- and this term's NewTerm frame.
relation oldRangeHeld  (N : node)   -- N's log actually contains the old-term range
-- The TERM of the newest NewTerm frame N holds. Deliberately NOT a bare
-- `newTermDurable` relation wiped on each election: that global wipe let a STALE
-- candidate winning an OLD term retroactively falsify a properly-barriered earlier
-- commit. Term-keyed and monotone, so the barrier evidence cannot be destroyed later.
function newTermTerm (N : node) : term
relation newTermAppended (N : node) -- leader appended it: `new_term_pos = Some`
relation reportedOld   (N : node)   -- N's AppendPosition floor reported the base

individual committedOld : Bool
individual commitTerm   : term      -- the term the old range was committed IN

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
  lastTerm N := tot.zero
  newTermTerm N := tot.zero
  -- everyone starts holding the inherited old-term range (they reconciled clean);
  -- that is precisely why committing it looks safe to a positions-only tracker.
  oldRangeHeld N := true
  newTermAppended N := false
  reportedOld N := false
  committedOld := false
  commitTerm := tot.zero
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

-- UC's lexicographic vote: grant only to a candidate at least as up-to-date by
-- data-stamped last term. This is what makes the rival's win HONEST in #6b.
action deliverRequestVoteGrant (j : node) (c : node) (t : term) {
  require reqVote c t
  require tot.le (curTerm j) t
  require ¬ (hasVoted j ∧ voteTerm j = t ∧ voteCand j ≠ c)
  require tot.le (lastTerm j) (lastTerm c)
  if tlt (curTerm j) t then candidate j := false
  if tlt (curTerm j) t then leader j := false
  curTerm j := t
  hasVoted j := true
  voteTerm j := t
  voteCand j := c
  voteMsg j c t := true
}

-- A new leader starts with `new_term_pos = None` — the window the clamp closes.
action becomeLeader (i : node) (q : node) {
  require candidate i
  require member i q
  require ∀ V, member V q → (V = i ∨ voteMsg V i (curTerm i))
  leader i := true
  candidate i := false
  newTermAppended i := false
}

-- ---------- the NewTerm barrier ----------

action appendNewTerm (i : node) {
  require leader i
  newTermAppended i := true
  newTermTerm i := curTerm i
  lastTerm i := curTerm i          -- authoring stamps the log with this term
}

action replicateNewTerm (i : node) (j : node) {
  require leader i
  require newTermAppended i
  require tot.le (curTerm j) (curTerm i)
  curTerm j := curTerm i
  candidate j := false
  leader j := false
  newTermTerm j := curTerm i
  lastTerm j := curTerm i          -- now this voter's last_term HAS reached the term
}

-- ---------- the #6b site ----------

-- Post-reconcile AppendPosition floor: an honest follower reports its durable, which
-- covers the inherited election base. Says nothing about the NewTerm frame.
action reportOldRange (j : node) {
  require oldRangeHeld j
  reportedOld j := true
}

-- `rank_leader`. PRE-FIX: advances on a bare quorum of position reports.
-- POST-FIX: clamped to `ranked >= new_term_pos` — which, since the ranked position is
-- the quorum-th durable, means a QUORUM must hold this term's NewTerm frame, and
-- `new_term_pos = None` (not yet appended) means no advance at all.
action commitOldRange (i : node) (q : node) {
  require leader i
  require ¬ committedOld
  require member i q
  require ∀ V, member V q → (V = i ∨ reportedOld V)
  -- The leader must HOLD the range it commits: `rank_leader` ranks the quorum-th
  -- durable, which includes the leader's own. Omitting this let a leader that had
  -- already discarded the old range commit it anyway — the same gap as BootGate's #4.
  require oldRangeHeld i
  require (¬ clampFix) ∨ newTermAppended i
  require (¬ clampFix) ∨ (∃ (q2 : node), ∀ V, member V q2 → newTermTerm V = curTerm i)
  committedOld := true
  commitTerm := curTerm i
}

-- ---------- the loss continuation ----------

-- A rival authors its own divergent tail at its term WITHOUT the old range, lifting
-- its data-stamped last_term above the commit-quorum members' — which is what lets it
-- win the next election honestly.
-- A node that simply never received the inherited old-term tail. Only before any
-- commit — this models absence, NOT a leader discarding its own committed prefix.
action divergeFromOldRange (j : node) {
  require ¬ committedOld
  require ¬ leader j
  oldRangeHeld j := false
}

-- A leader that LACKS the old range authors its own content, lifting its data-stamped
-- last_term above the commit-quorum members' — what lets it later win honestly.
action authorDivergentTail (i : node) {
  require leader i
  require ¬ oldRangeHeld i
  lastTerm i := curTerm i
}

-- The rival, once leader, overwrites the divergent range on a follower: the committed
-- bytes are truncated cluster-wide. If this reaches every node while `committedOld`
-- holds, that is the acked-write loss.
action truncateToRival (j : node) (i : node) {
  require leader i
  require ¬ oldRangeHeld i
  require tot.le (curTerm j) (curTerm i)
  curTerm j := curTerm i
  candidate j := false
  leader j := false
  oldRangeHeld j := false
  lastTerm j := curTerm i
}

-- ---------- safety ----------

-- PROXY: the §5.4.2 barrier itself. If the inherited old-term range is committed, a
-- quorum holds THIS term's NewTerm frame.
safety [no_old_term_only_commit]
  proxyOn → (committedOld → ∃ (q : node), ∀ V, member V q → tot.le commitTerm (newTermTerm V))

-- FULL: the acked-write loss. A committed range keeps at least one copy somewhere.
safety [no_acked_write_loss]
  committedOld → ∃ (V : node), oldRangeHeld V

#gen_spec

-- PROBE D — pre-fix, PROXY, term=Fin 3 (the proxy needs only one election term).
#model_check { node := Fin 3, term := Fin 3 }
  { clampFix := false, proxyOn := true } (maxDepth := 12)
