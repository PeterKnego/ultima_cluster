/-! `uc2_consensus/src/election.rs`: the vote-safety kernel —
`log_ok_order` (lexicographic freshness) + the `voted_for`
single-vote-per-term discipline of `handle_request_vote`. Modeled at the
post-term-adoption point (the Rust comment: "At this point current_term ==
new_term"). Persist-before-send (`PersistAndSendVote`) is modeled as an
atomic step — recorded as a runtime assumption (V3), discharged by
inspection of uc2_node, not by proof. -/

namespace Uc2

/-- `election.rs::log_ok_order`: grant iff
`(candTerm, candDurable) >= (ourTerm, ourDurable)` lexicographically. -/
def logOk (ourTerm ourDurable candTerm candDurable : Nat) : Bool :=
  if ourTerm < candTerm then true
  else if ourTerm = candTerm then ourDurable ≤ candDurable
  else false

/-- The vote-relevant slice of `ElectionSm`. `votedFor = some (term, id)`
mirrors the Rust `voted_for: Option<(u32, NodeId)>`. -/
structure VoterState where
  currentTerm : Nat
  votedFor    : Option (Nat × Nat)
  lastTerm    : Nat   -- term_map.last term (0 if empty)
  durable     : Nat
deriving Repr, DecidableEq

/-- `election.rs::handle_request_vote`'s reply outcome. -/
inductive VoteReply where
  | granted
  | rejected
deriving Repr, DecidableEq

/-- `election.rs::handle_request_vote` (post-term-adoption): already voted
this term ⇒ idempotent re-grant to the same candidate only; else grant iff
`logOk`. Granting records `votedFor`. -/
def handleRequestVote (s : VoterState) (cand : Nat)
    (candLastTerm candLastDurable : Nat) : VoterState × VoteReply :=
  match s.votedFor with
  | some (vt, vid) =>
    if vt = s.currentTerm then
      if vid = cand then (s, .granted) else (s, .rejected)
    else grantIfFresh s cand candLastTerm candLastDurable
  | none => grantIfFresh s cand candLastTerm candLastDurable
where
  grantIfFresh (s : VoterState) (cand candLastTerm candLastDurable : Nat) :
      VoterState × VoteReply :=
    if logOk s.lastTerm s.durable candLastTerm candLastDurable then
      ({ s with votedFor := some (s.currentTerm, cand) }, .granted)
    else (s, .rejected)

end Uc2

open Uc2 in
section
-- higher term wins regardless of durable
#guard logOk 3 100 4 0 == true
-- equal is fresh enough
#guard logOk 3 100 3 100 == true
-- same term, shorter log
#guard logOk 3 100 3 99 == false
-- lower term never fresh
#guard logOk 3 100 2 999999 == false
-- single-vote-per-term: second candidate same term is rejected
#guard (let s : VoterState := ⟨5, none, 3, 100⟩
        let (s, r1) := handleRequestVote s 1 4 0
        let (_, r2) := handleRequestVote s 2 4 0
        (r1, r2)) == (VoteReply.granted, VoteReply.rejected)
-- idempotent re-grant to the same candidate (lost datagram)
#guard (let s : VoterState := ⟨5, none, 3, 100⟩
        let (s, _) := handleRequestVote s 1 4 0
        (handleRequestVote s 1 4 0).2) == VoteReply.granted
end
