import Veil

/- UC v2 election plane ported to Veil (spike V1, scratch — NEVER the record).
   Faithful to proofs/Uc2Proofs/Protocol.lean (S2 election model) + the 5-clause
   Inv of ElectionSafety.lean. Data plane (logOk/lastTerm/durable) abstracted to a
   nondeterministic grant guard — sound for ELECTION safety. Quorums abstract
   (Paxos idiom): `member` + `quorum_intersection` assumption = Lean C5. Bug-finding
   only; the real proof is in proofs/. -/

veil module UcElection

type node
type term
type quorum

instantiate tot : TotalOrderWithZero term

immutable relation member (N : node) (Q : quorum)

-- Per-node state (PNode). role: follower = ¬candidate ∧ ¬leader.
relation candidate (N : node)
relation leader    (N : node)
function curTerm   (N : node) : term
-- votedFor : node → Option (term × node) ≈ hasVoted + voteTerm + voteCand.
relation hasVoted  (N : node)
function voteTerm  (N : node) : term
function voteCand  (N : node) : node
-- votesReceived tally.
relation counted   (I : node) (V : node)
-- sent set: requestVote(cand,term) and granted vote(voter,cand,term).
relation reqVote   (C : node) (T : term)
relation voteMsg   (V : node) (C : node) (T : term)

#gen_state

-- Lean C5 quorum_intersect, as a model axiom.
assumption [quorum_intersection]
  ∀ (q1 q2 : quorum), ∃ (r : node), member r q1 ∧ member r q2

-- strict term order
theory ghost relation tlt (x y : term) := tot.le x y ∧ x ≠ y

after_init {
  candidate N := false
  leader N := false
  curTerm N := tot.zero
  hasVoted N := false
  voteTerm N := tot.zero
  voteCand N := N
  counted I V := false
  reqVote C T := false
  voteMsg V C T := false
}

-- startElection: i (non-leader) bumps to a strictly higher term t, self-votes,
-- becomes candidate with tally {i}, emits a requestVote at t.
action startElection (i : node) (t : term) {
  require ¬ leader i
  require tlt (curTerm i) t
  curTerm i := t
  candidate i := true
  leader i := false
  hasVoted i := true
  voteTerm i := t
  voteCand i := i
  counted i V := false            -- clear i's tally row...
  counted i i := true             -- ...then self-vote → {i}; other rows unchanged
  reqVote i t := true
}

-- deliverRequestVote (grant arm): voter j adopts t if higher, then grants iff not
-- already committed at t to a different candidate. logOk abstracted (nondeterministic).
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

-- deliverVote: candidate/leader i counts a granted vote from v at its current term.
action deliverVote (i : node) (v : node) {
  require candidate i ∨ leader i
  require voteMsg v i (curTerm i)
  counted i v := true
}

-- becomeLeader: candidate i whose tally covers a quorum wins.
action becomeLeader (i : node) (q : quorum) {
  require candidate i
  require ∀ V, member V q → counted i V
  leader i := true
  candidate i := false
}

-- crashRestart: i loses volatile role/tally; persisted vote + curTerm survive.
action crashRestart (i : node) {
  candidate i := false
  leader i := false
  counted i V := false            -- i's volatile tally cleared; other rows unchanged
}

-- election safety: two leaders at the same term are the same node.
safety [election_safety]
  (leader N1 ∧ leader N2 ∧ curTerm N1 = curTerm N2) → N1 = N2

invariant [grant_state]
  voteMsg V C T → (tlt T (curTerm V) ∨ (curTerm V = T ∧ hasVoted V ∧ voteTerm V = T ∧ voteCand V = C))
invariant [grant_uniq]
  voteMsg V C1 T ∧ voteMsg V C2 T → C1 = C2
invariant [self_vote]
  (candidate I ∨ leader I) → (hasVoted I ∧ voteTerm I = curTerm I ∧ voteCand I = I)
invariant [votes_sound]
  ((candidate I ∨ leader I) ∧ counted I V) → (V = I ∨ voteMsg V I (curTerm I))
invariant [leader_quorum]
  leader I → ∃ (q : quorum), ∀ V, member V q → counted I V

#gen_spec


-- reachability: a leader is electable within 3 actions (sanity that the model is live).
sat trace {
  any 3 actions
  assert (∃ l, leader l)
}

#check_invariants
