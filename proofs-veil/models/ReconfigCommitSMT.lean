import Veil

/- UC v2.1 (M7) reconfig commit plane — ABSTRACT-QUORUM SKETCH for the SMT/inductive
   route (option (a), session-1 deliverable: definitions + property statements ONLY.
   `#check_invariants` is deliberately NOT run here — the invariant hunt is the NEXT
   session's work. Scratch — NEVER the record).

   WHY THIS FILE EXISTS: the concrete-TSet model (ReconfigCommit.lean) cannot take
   the SMT inductiveness path — TSet's List-backed count/intersection ops break VC
   generation ("incorrect number of universe levels List", ReconfigLC session-3
   finding). The inductive proof needs the Lean-C5 idiom instead: quorums as an
   abstract type + `member`-style relations + intersection assumptions, with config
   evolution as an abstract successor relation whose ±1 shape is axiomatized from
   `ClusterConfig::apply` (the unique shared transition, one change in flight).

   THE ADJACENCY OBLIGATION — a theorem, not an axiom (brief requirement):
   `adjacent_cfg_quorum_intersection` below is stated as an `assumption` in this
   sketch, but next session it MUST be discharged, not assumed. It is true for
   majority quorums under ±1 change (arithmetic: for |C| = n, a C-quorum has
   ≥ ⌊n/2⌋+1 members and a (C∖{x})-quorum ≥ ⌊(n-1)/2⌋+1, and
   ⌊n/2⌋ + ⌊(n-1)/2⌋ + 1 = n > n-1 forces intersection even after deleting x;
   the add case is symmetric), but that arithmetic is NOT derivable in the
   relational fragment. Two candidate discharge routes, next session's choice:
     (r1) prove it as a plain Lean theorem over a concrete majority interpretation
          of `quorumOf` (count-based over Finset), and cite it as the instantiation
          obligation of the assumption — the assumption then has a proved witness;
     (r2) enrich the module with enough counting theory to derive it in-module
          (risk: reopens the List-universe wall).

   FIDELITY NOTES CARRIED FROM ReconfigCommit.lean (same plane, abstract dress):
     * report plane collapsed: commit-quorum membership requires `holdsE` by
       construction (Q2 links 1+2, CONFIRMED-SAFE in Rust).
     * `prefixCoupling` is NOT a knob here — the proof model is the coupled one
       (the knob existed only for the explicit-state calibration).
     * NO per-adoption adjacency require: safety across adoption comes from the
       PREFIX (F-M7-2's whole lesson), while the succCfg relation constrains the
       chain of PROPOSALS (apply's ±1 shape + one-in-flight). A lagging node
       adopting a distant config models the snapshot-carried-config / deep-replay
       path, which is legal in UC exactly because it implies holding the prefix.
     * crashRestart included un-knobbed (free under induction). -/

veil module UcReconfigCommitSMT

type node
type term
type cfgid
type quorum

instantiate tot : TotalOrderWithZero term

-- static universes: which nodes are voters of which config; which quorums belong
-- to which config. Configs are ABSTRACT ids — evolution is movement along succCfg.
immutable relation cmember (N : node) (C : cfgid)
immutable relation qmember (N : node) (Q : quorum)
immutable relation quorumOf (Q : quorum) (C : cfgid)
-- single-server successor: D = C plus-or-minus exactly one voter (apply's shape).
immutable relation succCfg (C : cfgid) (D : cfgid)
immutable individual genesisC : cfgid

-- election plane (tally collapsed, as the concrete model)
relation candidate (N : node)
relation leader    (N : node)
function curTerm   (N : node) : term
relation hasVoted  (N : node)
function voteTerm  (N : node) : term
function voteCand  (N : node) : node
relation reqVote   (C : node) (T : term)
relation voteMsg   (V : node) (C : node) (T : term)

-- config plane
function cfgOf     (N : node) : cfgid
relation pending    (I : node)
relation hasProposal(I : node)
function proposedC (I : node) : cfgid
relation propAfterE (I : node)
-- J is durable past I's current config entry (the adoption evidence a config
-- commit counts) — see ReconfigCommit.lean's session gap 1.
relation hasAdopted (J : node) (I : node)

-- commit/log plane (one tracked entry E) + history variables for the induction
relation holdsE (N : node)
individual committed     : Bool
individual committedTerm : term
individual commitCfgid   : cfgid    -- the config in force at commit time
individual commitQuorum  : quorum   -- the certifying quorum (all E-holders)

#gen_state

-- ---------- quorum theory (Lean C5 idiom) ----------

-- same-config quorums intersect (the C5 quorum_intersect axiom, per config).
assumption [same_cfg_quorum_intersection]
  ∀ (c : cfgid) (q1 q2 : quorum),
    quorumOf q1 c → quorumOf q2 c → ∃ n, qmember n q1 ∧ qmember n q2

-- a quorum of C consists of C's voters.
assumption [quorum_member_sound]
  ∀ (c : cfgid) (q : quorum) (n : node), quorumOf q c → qmember n q → cmember n c

-- apply's ±1 shape: a successor differs by exactly one added or removed voter.
assumption [succ_shape]
  ∀ (c d : cfgid), succCfg c d →
    ∃ x, (∀ n, cmember n d ↔ (cmember n c ∨ n = x)) ∨
         (∀ n, cmember n d ↔ (cmember n c ∧ n ≠ x))

-- !!! OBLIGATION — TO BE PROVED NEXT SESSION, NOT ASSUMED (see header) !!!
-- consecutive-config quorum intersection, from the ±1 shape. Stated here so P2's
-- invariant hunt can proceed; its discharge (route r1 or r2) is a hard exit
-- criterion for the proof push.
assumption [adjacent_cfg_quorum_intersection]
  ∀ (c d : cfgid) (q1 q2 : quorum),
    succCfg c d → quorumOf q1 c → quorumOf q2 d →
    ∃ n, qmember n q1 ∧ qmember n q2

theory ghost relation tlt (x y : term) := tot.le x y ∧ x ≠ y

after_init {
  candidate N := false
  leader N := false
  curTerm N := tot.zero
  hasVoted N := false
  voteTerm N := tot.zero
  voteCand N := N
  reqVote C T := false
  voteMsg V C T := false
  cfgOf N := genesisC
  pending N := false
  hasProposal N := false
  proposedC N := genesisC
  propAfterE N := false
  hasAdopted J I := false
  holdsE N := false
  committed := false
  committedTerm := tot.zero
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
  require ¬ (holdsE j ∧ ¬ holdsE c)          -- up-to-date restriction (log-keyed)
  require cmember c (cfgOf j)                -- granting membership-gated on the
                                             -- voter's ADOPTED config (link 5;
                                             -- session gap 2 — load-bearing for P2)
  if tlt (curTerm j) t then candidate j := false
  if tlt (curTerm j) t then leader j := false
  curTerm j := t
  hasVoted j := true
  voteTerm j := t
  voteCand j := c
  voteMsg j c t := true
}

action becomeLeader (i : node) (q : quorum) {
  require candidate i
  require quorumOf q (cfgOf i)               -- a quorum of i's OWN adopted config
  require ∀ V, qmember V q → (V = i ∨ voteMsg V i (curTerm i))
  leader i := true
  candidate i := false
}

action crashRestart (i : node) {
  candidate i := false
  leader i := false
}

-- ---------- commit/log plane ----------

action appendEntry (i : node) {
  require leader i
  require ¬ committed
  holdsE i := true
}

action replicate (i : node) (j : node) {
  require leader i
  require holdsE i
  require tot.le (curTerm j) (curTerm i)
  candidate j := false
  leader j := false
  curTerm j := curTerm i
  holdsE j := true
}

action commitEntry (i : node) (q : quorum) {
  require leader i
  require ¬ committed
  require holdsE i                           -- link 3: leader holds what it commits
  require quorumOf q (cfgOf i)               -- quorum-durable under config in force
  require ∀ V, qmember V q → holdsE V        -- links 1+2: counting ⟹ holding
  committed := true
  committedTerm := curTerm i
  commitCfgid := cfgOf i
  commitQuorum := q
}

-- ---------- config plane ----------

-- propose = leader-adopts-at-append, one in flight, one ±1 step along succCfg.
action propose (i : node) (d : cfgid) {
  require leader i
  require ¬ pending i
  require succCfg (cfgOf i) d
  cfgOf i := d
  proposedC i := d
  hasProposal i := true
  pending i := true
  if holdsE i then propAfterE i := true
  if ¬ holdsE i then propAfterE i := false
  hasAdopted J i := false
  hasAdopted i i := true
}

-- adopt — UNCONDITIONALLY prefix-coupled (the proof model): a config entry that
-- sits after E in the proposer's stream can only be adopted by an E-holder.
action adopt (j : node) (i : node) {
  require j ≠ i
  require hasProposal i
  require tot.le (curTerm j) (curTerm i)
  require (¬ propAfterE i) ∨ holdsE j
  candidate j := false
  leader j := false
  curTerm j := curTerm i
  cfgOf j := proposedC i
  hasAdopted j i := true
}

-- config entry commits like any entry: a C_new quorum of adopters (session gap 1).
action commitCfg (i : node) (q : quorum) {
  require pending i
  require quorumOf q (cfgOf i)
  require ∀ V, qmember V q → hasAdopted V i
  pending i := false
  hasProposal i := false
}

-- ---------- properties ----------

safety [election_safety]
  (leader N1 ∧ leader N2 ∧ curTerm N1 = curTerm N2) → N1 = N2

-- P2 — LEADER COMPLETENESS ACROSS RECONFIGURATION (the target of the proof push).
safety [leader_completeness]
  (committed ∧ leader L ∧ tot.le committedTerm (curTerm L)) → holdsE L

-- ---------- candidate invariant clauses (STARTING POINTS for the CTI loop — ----------
-- ---------- unverified; expect this set to change under #check_invariants) ----------

-- election clauses, ported in shape from Election.lean's proved 5-clause Inv
-- (deliverVote/counted collapsed into becomeLeader's ∀-require):
invariant [grant_state]
  voteMsg V C T → (tlt T (curTerm V) ∨ (curTerm V = T ∧ hasVoted V ∧ voteTerm V = T ∧ voteCand V = C))
invariant [grant_uniq]
  voteMsg V C1 T ∧ voteMsg V C2 T → C1 = C2
invariant [self_vote]
  (candidate I ∨ leader I) → (hasVoted I ∧ voteTerm I = curTerm I ∧ voteCand I = I)
invariant [leader_quorum]
  leader I → ∃ (q : quorum), quorumOf q (cfgOf I) ∧
    (∀ V, qmember V q → (V = I ∨ voteMsg V I (curTerm I)))

-- commit-plane clauses:
invariant [commit_backed]        -- the certifying quorum really holds E (holdsE is monotone)
  committed → (∀ V, qmember V commitQuorum → holdsE V)
invariant [commit_quorum_sound]  -- ...and is a quorum of the config in force at commit
  committed → quorumOf commitQuorum commitCfgid
invariant [commit_term_bound]    -- E was committed at a term some leader actually reached
  committed → ∃ N, tot.le committedTerm (curTerm N)

-- THE LOAD-BEARING CANDIDATE (early signal, unproved): after commit, EVERY config
-- any node has adopted only has quorums that contain an E-holder — the invariant
-- that blocks a non-holder's election. Expect the CTI loop to refine this (e.g.
-- restricting to configs reachable along succCfg from commitCfgid, or threading
-- propAfterE through the proposal chain); it is where the adjacency lemma and the
-- prefix coupling must meet.
invariant [electable_cfgs_contain_holder]
  committed → (∀ N (q : quorum), quorumOf q (cfgOf N) → ∃ V, qmember V q ∧ holdsE V)

#gen_spec

-- NEXT SESSION: #check_invariants (cvc5, all-n) — deliberately not run in this
-- sketch; the clause set above is a seed, not a claim.
-- #check_invariants
