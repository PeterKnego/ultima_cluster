import Mathlib.Data.Finset.Card
import Mathlib.Logic.Relation
import Mathlib.Logic.Function.Basic
import Uc2Model.Vote
import Uc2Proofs.Quorum

/-! S1 — the N-node election protocol model.

A whole-cluster transition system over `uc_consensus/src/election.rs`'s vote
path, built for the Tier-B election-safety proof (S2). Modeling decisions
(settled at plan time):

1. **Sent-set network.** `sent : List (Msg n)` is append-only; any step may
   process any message in `sent`, never removing it. Loss (never processed),
   duplication (processed twice), and reordering (any order) come for free —
   the standard safety-proof encoding of the sim's fault model.
2. **Havoc data plane.** Election safety does not depend on log content, so
   `lastTerm`/`durable` (the `log_ok` inputs) evolve by an unconstrained
   `havocData` step. Safety under arbitrary data evolution is a superset of
   safety under UC's actual data plane.
3. **Crash-restart** preserves `currentTerm` and `votedFor` (the
   `StableValue`-persisted vote record; V3's persist-before-send makes
   grant-then-crash safe), resets `role := .follower`, `votesReceived := ∅`.
4. **Vote discipline mirrors `Uc2Model.Vote.handleRequestVote`** clause for
   clause (single vote per term, idempotent re-grant, older-term `votedFor`
   falls through to a fresh grant, `logOk` freshness), restated over
   `Fin n` candidate ids. Only `logOk` itself is reused from `Uc2Model`.
5. **Quorum** = `Finset (Fin n)` with `n / 2 + 1 ≤ card`, the exact shape
   `Uc2.quorum_intersect` consumes.

Fixed membership `Fin n`, all voters: `election.rs`'s non-member guards
(lines 606, 643) and the learner `can_vote` gate (609) are vacuous here.
Timers, commit gossip, term-map reconciliation, and truncation are not
modeled — steps fire nondeterministically whenever enabled.

LA1 extension: one constructor, `adoptHigherTerm` (a message-free higher-term
adoption — the `TermMapReceived` trigger of `adopt_term` the spike had no use
for), added so the data-plane layer (`Uc2Proofs/ProtocolData.lean`) projects
onto this model; `election_safety` is re-proved under it in
`ElectionSafety.lean` (one near-copy of the `deliverVoteHigherTerm` case). -/

namespace Uc2

/-- `election.rs::Role` — the vote-relevant role of a node. -/
inductive Role where
  | follower
  | candidate
  | leader
deriving Repr, DecidableEq

/-- The vote-relevant slice of one node's `ElectionSm`
(`uc_consensus/src/election.rs`). `votedFor = some (term, id)` mirrors the
Rust `voted_for: Option<(u32, NodeId)>`; `votesReceived` mirrors
`votes_received` (a dedup'd `Vec<NodeId>` in Rust, a `Finset` here);
`lastTerm`/`durable` are the `log_ok` inputs (`term_map.last`, `durable`) —
havoc-evolved per decision 2. -/
structure PNode (n : Nat) where
  currentTerm   : Nat
  votedFor      : Option (Nat × Fin n)
  role          : Role
  votesReceived : Finset (Fin n)
  lastTerm      : Nat
  /-- THE COUNTER (`cnc` `LogCounters::durable`). Two things read it, on two
  different threads in the real node: the receiver agent, which REPORTS it to
  the leader for commit ranking, and — since `main` 26d4827 — `log_ok`, which
  re-reads it immediately before answering a `RequestVote`. Raft's vote rule is
  sound only if a voter judges a candidate against everything it has DURABLY
  STORED, and this is that.

  Issue #7: this field used to serve FOUR roles at once, and the collapse made
  `ReportEraFloor`'s `sendReport` case provable by `Nat.le_refl` — a proof that
  the reported position is at most the vote-visible one BECAUSE THEY WERE THE
  SAME TERM. That identity is false in the real system, and the acked-write loss
  it hides is real. See `smDurable`. -/
  durable       : Nat
  /-- The consensus agent's ABSORBED COPY of `durable` (`ElectionSm::durable`,
  fed by `Event::DurableAdvanced` from `Consensus::do_work`). Always `≤ durable`
  (`SmLeDurable`), and strictly below it whenever the archive has fsynced since
  the agent last polled.

  Used for exactly the two roles that legitimately lag in the real node: the
  `last_durable` credential a candidate ADVERTISES (`start_election`) and the
  base a new leader COLLAPSES to (`become_leader`). Staleness is CONSERVATIVE
  for both — a candidate that under-advertises merely loses elections it could
  have won. It is unsafe only on the grant side, which is why `logOk` reads
  `durable` and not this. -/
  smDurable     : Nat
deriving DecidableEq

/-- The two election wire messages. `src`/`dst` stand in for the Rust
`from`/`to` (`from` is a Lean keyword).

- `requestVote` mirrors `Action::StartElection`'s broadcast payload
  `{new_term, last_term, last_durable}` plus the sending candidate's id.
- `vote` mirrors `Action::PersistAndSendVote` (granted) and
  `Action::SendVoteRejection` (not granted), i.e. `Event::Vote {from, term,
  granted}` on the receiving side, with the addressee made explicit. -/
inductive Msg (n : Nat) where
  | requestVote (src : Fin n) (newTerm lastTerm durable : Nat)
  | vote (src dst : Fin n) (term : Nat) (granted : Bool)
deriving DecidableEq

/-- `election.rs::adopt_term` (lines 1059–1074), vote-relevant slice: step to
the given term as a follower — `voted_for = None` ("new term: no vote cast
yet"), `votes_received.clear()`. -/
def PNode.adoptTerm {n : Nat} (s : PNode n) (t : Nat) : PNode n :=
  { s with currentTerm := t, role := .follower, votedFor := none,
           votesReceived := ∅ }

/-- The receiver side of a non-stale `Event::RequestVote` (`election.rs`
lines 623–626 then `handle_request_vote`, lines 1202–1227): first adopt a
strictly higher term (dropping to follower), then — "At this point
current_term == new_term" — run the `Uc2Model.Vote.handleRequestVote`
discipline at the adopted term:

- already voted this term for this candidate ⇒ idempotent re-grant;
- already voted this term for someone else ⇒ reject;
- otherwise (no vote, or an older-term `votedFor`) ⇒ grant iff `logOk`,
  recording `votedFor` (`grant_vote`, lines 1229–1237).

Returns the successor node state and the `granted` flag of the vote message
sent back. State update and message emission are one atomic step — exactly
V3's persist-before-send assumption. -/
def PNode.recvRequestVote {n : Nat} (s : PNode n) (c : Fin n)
    (newTerm cLastTerm cDurable : Nat) : PNode n × Bool :=
  let s := if s.currentTerm < newTerm then s.adoptTerm newTerm else s
  match s.votedFor with
  | some (vt, vid) =>
    if vt = s.currentTerm then
      if vid = c then (s, true) else (s, false)
    else grantIfFresh s c cLastTerm cDurable
  | none => grantIfFresh s c cLastTerm cDurable
where
  /-- `election.rs::handle_request_vote`'s fresh-grant arm: grant iff
  `log_ok` (lines 1222–1226), recording `votedFor` at the current term. -/
  grantIfFresh (s : PNode n) (c : Fin n) (cLastTerm cDurable : Nat) :
      PNode n × Bool :=
    if logOk s.lastTerm s.durable cLastTerm cDurable then
      ({ s with votedFor := some (s.currentTerm, c) }, true)
    else (s, false)

/-- Issue #7: the vote path is durable-agnostic — neither `durable` (the
counter) nor `smDurable` (the absorbed copy) is touched by adopting a term or by
answering a `RequestVote`. Needed to carry `SmLeDurable` across those steps. -/
@[simp] theorem adoptTerm_durable {n : Nat} (s : PNode n) (t : Nat) :
    (s.adoptTerm t).durable = s.durable := rfl

@[simp] theorem adoptTerm_smDurable {n : Nat} (s : PNode n) (t : Nat) :
    (s.adoptTerm t).smDurable = s.smDurable := rfl

@[simp] theorem recvRequestVote_durable {n : Nat} (s : PNode n) (c : Fin n)
    (nt clt cd : Nat) : (s.recvRequestVote c nt clt cd).1.durable = s.durable := by
  simp only [PNode.recvRequestVote, PNode.recvRequestVote.grantIfFresh]
  split <;> split_ifs <;> rfl

@[simp] theorem recvRequestVote_smDurable {n : Nat} (s : PNode n) (c : Fin n)
    (nt clt cd : Nat) :
    (s.recvRequestVote c nt clt cd).1.smDurable = s.smDurable := by
  simp only [PNode.recvRequestVote, PNode.recvRequestVote.grantIfFresh]
  split <;> split_ifs <;> rfl

/-- The whole cluster: per-node state plus the append-only sent set
(decision 1). -/
structure World (n : Nat) where
  nodes : Fin n → PNode n
  sent  : List (Msg n)

/-- Boot state: everyone a follower at term 0 with nothing voted, nothing
received, empty data plane, nothing sent. -/
def World.init (n : Nat) : World n :=
  { nodes := fun _ =>
      { currentTerm := 0, votedFor := none, role := .follower,
        votesReceived := ∅, lastTerm := 0, durable := 0, smDurable := 0 },
    sent := [] }

/-- One cluster transition. Each constructor carries the minimal enabling
hypotheses and defines the successor world explicitly (`Function.update` on
`nodes`, append on `sent`). -/
inductive Step {n : Nat} : World n → World n → Prop
  /-- `election.rs::start_election` (lines 974–1001): term bump, become
  candidate, self-vote recorded (`voted_for = Some((term, self))`, persisted
  before soliciting peers), `votes_received` cleared and seeded with self,
  `Action::StartElection` broadcast carrying `(new_term, last_term,
  last_durable)`. Enabled for any non-leader (the Rust election timer only
  fires in `Role::Follower | Role::Candidate`, lines 956–970); firing time
  is nondeterministic (timers not modeled). The Rust single-node immediate
  `become_leader` (line 998) is reached here via a separate `becomeLeader`
  step. -/
  | startElection (w : World n) (i : Fin n)
      (hrole : (w.nodes i).role ≠ .leader) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with
              currentTerm := (w.nodes i).currentTerm + 1
              votedFor := some ((w.nodes i).currentTerm + 1, i)
              role := .candidate
              votesReceived := {i} }
          sent := w.sent ++
            [.requestVote i ((w.nodes i).currentTerm + 1)
              (w.nodes i).lastTerm (w.nodes i).smDurable] }
  /-- `Event::RequestVote`, non-stale arm (`election.rs` lines 600–627):
  receiver `j` adopts a strictly-higher term first (lines 623–625), then runs
  `handle_request_vote` at the adopted term and answers with a `vote`
  message at `newTerm` — see `PNode.recvRequestVote`. Self-delivery
  (`j = c`) is allowed and harmless: the candidate re-grants itself
  idempotently. -/
  | deliverRequestVote (w : World n) (j c : Fin n)
      (newTerm cLastTerm cDurable : Nat)
      (hmsg : Msg.requestVote c newTerm cLastTerm cDurable ∈ w.sent)
      (hterm : (w.nodes j).currentTerm ≤ newTerm) :
      Step w
        { nodes := Function.update w.nodes j
            ((w.nodes j).recvRequestVote c newTerm cLastTerm cDurable).1
          sent := w.sent ++
            [.vote j c newTerm
              ((w.nodes j).recvRequestVote c newTerm cLastTerm cDurable).2] }
  /-- `Event::RequestVote`, stale arm (`election.rs` lines 618–622): a
  candidate below the receiver's term gets `Action::SendVoteRejection`
  carrying the RECEIVER's term ("so it learns"); no adoption, no state
  change. Kept (rather than elided as droppable) so the model can also
  express the stale candidate later adopting the higher term via
  `deliverVoteHigherTerm`. -/
  | rejectStaleRequestVote (w : World n) (j c : Fin n)
      (newTerm cLastTerm cDurable : Nat)
      (hmsg : Msg.requestVote c newTerm cLastTerm cDurable ∈ w.sent)
      (hstale : newTerm < (w.nodes j).currentTerm) :
      Step w
        { nodes := w.nodes
          sent := w.sent ++ [.vote j c (w.nodes j).currentTerm false] }
  /-- `Event::Vote`, counting arm (`election.rs` lines 646–653): only a
  candidate collects, only grants, only at exactly its `currentTerm`
  (lower-term votes are dropped at line 630, higher-term ones adopt via
  `deliverVoteHigherTerm`). `insert` mirrors the Rust dedup
  (`!votes_received.contains(&from)` before push). The Rust majority check
  inlined at lines 650–652 is the separate `becomeLeader` step. -/
  | deliverVote (w : World n) (i v : Fin n) (t : Nat)
      (hmsg : Msg.vote v i t true ∈ w.sent)
      (hrole : (w.nodes i).role = .candidate)
      (hterm : (w.nodes i).currentTerm = t) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with
              votesReceived := insert v (w.nodes i).votesReceived }
          sent := w.sent }
  /-- `Event::Vote`, higher-term arm (`election.rs` lines 633–636): a vote
  message (granted or rejection) above our term forces `adopt_term` — drop
  to follower at the message's term and stop counting. Applies to any role. -/
  | deliverVoteHigherTerm (w : World n) (i v : Fin n) (t : Nat) (g : Bool)
      (hmsg : Msg.vote v i t g ∈ w.sent)
      (hterm : (w.nodes i).currentTerm < t) :
      Step w
        { nodes := Function.update w.nodes i ((w.nodes i).adoptTerm t)
          sent := w.sent }
  /-- `election.rs::become_leader` (lines 1003–1057), gated by the count
  check at 650–652 / 998: a candidate whose `votes_received` (seeded with
  self at `start_election`) reaches `majority() = members.len() / 2 + 1`
  (lines 923–925) takes leadership. Only the role flip is vote-relevant; the
  term-map open/prune is data-plane (havoc'd, decision 2). -/
  | becomeLeader (w : World n) (i : Fin n)
      (hrole : (w.nodes i).role = .candidate)
      (hquorum : n / 2 + 1 ≤ (w.nodes i).votesReceived.card) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with role := .leader }
          sent := w.sent }
  /-- Crash-restart (decision 3): `currentTerm` and `votedFor` survive (the
  `StableValue`-persisted vote record; V3 persist-before-send makes
  grant-then-crash safe), volatile role and tally reset. `lastTerm`/`durable`
  are left to `havocData` (a crash may in reality truncate `durable`; havoc
  covers that and more). -/
  | crashRestart (w : World n) (i : Fin n) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with role := .follower, votesReceived := ∅ }
          sent := w.sent }
  /-- Havoc data plane (decision 2): `lastTerm`/`durable` — the `log_ok`
  inputs — jump to arbitrary values, over-approximating UC's real
  data-append/gossip/truncation evolution.

  Issue #7: `smDurable` is havoc'd alongside it, and deliberately UNCONSTRAINED
  here — the same over-approximation `durable` itself gets under decision 2.
  This layer proves `election_safety`, which mentions neither field; the layers
  that reason about durable positions (`ProtocolData`, `ProtocolCommit`) build
  their own steps and never take this one, so the laxity is not observable where
  it would matter. Keeping it unclamped is what lets every data-layer step
  project onto this one DEFINITIONALLY, rather than threading a
  `smDurable ≤ durable` hypothesis through `step_project`. -/
  | havocData (w : World n) (i : Fin n) (newLastTerm newDurable newSm : Nat) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with lastTerm := newLastTerm,
                             durable := newDurable,
                             smDurable := newSm }
          sent := w.sent }
  /-- Issue #7: the consensus agent's duty cycle absorbing the counter
  (`Consensus::do_work` step 2 / `refresh_durable`). The ONLY way `smDurable`
  catches up, and the reason it can lag at all — in the real node this is a
  poll on a different thread from the one that advances `durable` and from the
  one that reports it. -/
  | absorbDurable (w : World n) (i : Fin n) :
      Step w
        { nodes := Function.update w.nodes i
            { w.nodes i with smDurable := (w.nodes i).durable }
          sent := w.sent }
  /-- `election.rs::adopt_term` fired by a data-plane trigger the spike left
  unmodeled: `Event::TermMapReceived { term }` with `term > current_term`
  adopts the higher term before reconciling (`election.rs` lines 656–663).
  Message-free (the term-map gossip lives on the data wire, which this
  election-slice model does not carry), so it over-approximates every
  higher-term adoption UC performs off the vote path — term-map gossip,
  higher-term data headers, snapshot lineage adoption. Added in LA1 as the
  projection target for the data layer's `deliverTermMap`
  (`Uc2Proofs/ProtocolData.lean`); safety-wise it is `deliverVoteHigherTerm`
  without the message witness, which the invariant proof never used. -/
  | adoptHigherTerm (w : World n) (i : Fin n) (t : Nat)
      (hterm : (w.nodes i).currentTerm < t) :
      Step w
        { nodes := Function.update w.nodes i ((w.nodes i).adoptTerm t)
          sent := w.sent }

/-- Reachability from boot: the reflexive-transitive closure of `Step`. -/
def Reachable {n : Nat} (w : World n) : Prop :=
  Relation.ReflTransGen Step (World.init n) w

theorem reachable_init (n : Nat) : Reachable (World.init n) :=
  Relation.ReflTransGen.refl

/-! ## Non-vacuity

If no reachable world had a leader, election safety would be vacuously true.
The traces below are the model's tests: if they fail to typecheck, the MODEL
is broken (enabling conditions too strong), not the trace. -/

/-- Sanity: a single `startElection` composes with `Reachable` — node 0 of a
3-node cluster becomes a candidate at term 1. -/
example : ∃ w : World 3, Reachable w ∧ (w.nodes 0).role = .candidate :=
  ⟨_, .single (.startElection _ 0 (by decide)), by decide⟩

/-- **Non-vacuity.** A 3-node cluster elects a leader: node 0 starts an
election at term 1, nodes 1 and 2 grant (fresh `logOk` grants at the adopted
term), node 0 collects node 1's grant — `{0, 1}` is already a
`3 / 2 + 1 = 2` quorum — and becomes leader. -/
theorem nonvacuity_leader_trace :
    ∃ w : World 3, Reachable w ∧ (w.nodes 0).role = .leader :=
  ⟨_,
    .tail
      (.tail
        (.tail
          (.tail
            (.single (.startElection _ 0 (by decide)))
            (.deliverRequestVote _ 1 0 1 0 0 (by decide) (by decide)))
          (.deliverRequestVote _ 2 0 1 0 0 (by decide) (by decide)))
        (.deliverVote _ 0 1 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)),
    by decide⟩

#print axioms nonvacuity_leader_trace

end Uc2
