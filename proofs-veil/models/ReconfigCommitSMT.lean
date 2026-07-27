import Veil

/- UC v2.1 (M7) reconfig commit plane — ABSTRACT-QUORUM model for the SMT/inductive
   route (option (a)). Scratch — NEVER the record.

   ===== FABLE-GATE-1 AMENDMENTS (2026-07-26) — READ FIRST =====
   * MODEL-EDIT-1 and MODEL-EDIT-2 are now APPLIED (gate-1 approved; EDIT-2 with
     required revisions). See spike-ledger.md items 14/15 + the gate-1 entry.
   * `gotEAt` is NO LONGER GHOST — MODEL-EDIT-1 reads it in a `require`, so the
     run-1 behaviour-equivalence claim below applies only to the OTHER three
     history variables (`elecCfg`, `isCommitLeader`, `commitElecCfg`), which still
     appear in no guard.
   * `cfgid` is CHAIN-INDEXED in the intended interpretation (a config is a log
     ENTRY at a position, not a bare voter set). This is forced: with
     `cfgid ↦ Finset node`, add-x-then-remove-x makes `succCfg` symmetric, so the
     `cfgLt` axioms below are UNSATISFIABLE and every verdict over them vacuous.
     Witnesses W1/W2 in QuorumAdjacency.lean discharge all SEVEN assumptions.
   ** DOCUMENTED NARROWINGS (gate-1 ruling, binding) **
     (n1) The config-currency grant guard — like the pre-existing E-guard — is
          STRONGER than real `log_ok`, which would grant to a candidate that lacks
          the voter's entries but carries a higher `last_term` on a divergent
          branch. Deliberate: faithful `log_ok` in a one-tracked-entry plane makes
          the Figure-8 grant model-legal with nothing here to stop it; fidelity
          would require merging in the Figure8/Finding9 plane, banked elsewhere.
     (n2) `cfgOf` conflates HOLDING a config entry with having ADOPTED it. In Rust
          a candidate can be durable past a newer config frame (so `log_ok`
          GRANTS) while its adopted config lags until the archive re-scan
          (election.rs:889-899). This guard refuses that grant; real UC performs
          it. P2-benign under the same boundary assumption (durable past the frame
          ⇒ holds the prefix ⇒ holds E, by contiguity), but a distinct exclusion.
     (n3) TRUNCATION-REVERT / CONFIG-BRANCH EXCLUSION. MODEL-EDIT-2c makes a node's
          adopted config move FORWARD ONLY. Real UC moves it BACKWARD in exactly one
          place: `election.rs:703-748`, the M7 truncation revert — when a truncation
          removes the frame backing the adopted config (`to < config_position`) the
          node reverts one history level (or, on a wipe, keeps the config by fiat).
          Those are precisely the CONFIG-BRANCH states, and MODEL-EDIT-2b's
          linearity assumption excludes them too: UC's config history is linear only
          for the CANONICAL history — across branches two configs can share a
          `version`, which is why the forward gate at `:751-756` is a version
          comparison and not a global order. Both exclusions rest on the same
          declared conditionality below.
   ** CONDITIONALITY (binding on any claim made from this model) **
     Any SAFE verdict here is CONDITIONAL — on the canonical-prefix/contiguity
     discipline (Q2 chain, CONFIRMED-SAFE in Rust) and on the data-plane
     freshness / Finding-#6b `new_term_pos` clamp (proved at the Lean tier). It is
     NEVER an unconditional claim. (Precedent: the LC arc's FramesCurrentAuthored.)
   =============================================================

   SESSION-2 (bar 3, part 1) STATE — see proofs-veil/spike-ledger.md §SESSION 7:
     * `#check_invariants` IS run (bottom of file). Run 3: **170 ✅ / 6 ❌** —
       12 of 16 clauses CERTIFIED INDUCTIVE, all-n, via cvc5, in ~93 s.
     * STILL OPEN: `election_safety` (1 CTI), `leader_completeness` = P2 (2 CTIs),
       `electable_cfgs_contain_holder` (3 CTIs). All four are blocked on the SAME
       two adjudicated model-fidelity gaps (ledger items 14/15 = MODEL-EDIT-1/2),
       which are DELIBERATELY NOT APPLIED pending the mid-arc Fable gate:
         - MODEL-EDIT-1: `commitEntry` must count only reports term-stamped with
           the committing leader's own term (election.rs:545-552/566-570). Without
           it P2 is REACHABLY FALSE here (Figure-8 shape at n=5 — ledger 14).
         - MODEL-EDIT-2: vote granting must respect CONFIG-entry currency, not just
           E (log_ok over `durable`, election.rs:342-350/1240-1247, and config
           frames live in `durable` — gate doc §5 Q2 link 1).
     * HISTORY STATE: `elecCfg` / `isCommitLeader` / `commitElecCfg` appear in NO
       `require` (behaviour-preserving). `gotEAt` DID — until MODEL-EDIT-1 promoted
       it to load-bearing; it is now part of the model, not scaffolding.

   WHY THIS FILE EXISTS: the concrete-TSet model (ReconfigCommit.lean) cannot take
   the SMT inductiveness path — TSet's List-backed count/intersection ops break VC
   generation ("incorrect number of universe levels List", ReconfigLC session-3
   finding). The inductive proof needs the Lean-C5 idiom instead: quorums as an
   abstract type + `member`-style relations + intersection assumptions, with config
   evolution as an abstract successor relation whose ±1 shape is axiomatized from
   `ClusterConfig::apply` (the unique shared transition, one change in flight).

   THE ADJACENCY OBLIGATION — **DISCHARGED (route r1), see QuorumAdjacency.lean**:
   `adjacent_cfg_quorum_intersection` below is an `assumption` here, but it is now a
   PROVED THEOREM of the intended interpretation (cfgid = Finset node, quorum =
   strict-majority subset, succCfg = ±1 voter) — as are the other three assumptions
   of the bundle, so no verdict over it is vacuous. Original note follows. It is true for
   majority quorums under ±1 change (arithmetic: for |C| = n, a C-quorum has
   ≥ ⌊n/2⌋+1 members and a (C∖{x})-quorum ≥ ⌊(n-1)/2⌋+1, and
   ⌊n/2⌋ + ⌊(n-1)/2⌋ + 1 = n > n-1 forces intersection even after deleting x;
   the add case is symmetric), but that arithmetic is NOT derivable in the
   relational fragment. Two candidate discharge routes, next session's choice:
     (r1) [CHOSEN + DONE] prove it as a plain Lean theorem over a concrete majority
          interpretation of `quorumOf` (count-based over Finset), and cite it as the
          instantiation obligation of the assumption — the assumption then has a
          proved witness. Also proves the BUNDLE satisfiable (anti-vacuity).
     (r2) [not taken] enrich the module with enough counting theory to derive it
          in-module (risk: reopens the List-universe wall).

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
-- MODEL-EDIT-2: the config CHAIN ORDER (`C` strictly precedes `D` in the log). In the
-- intended interpretation this is the config entry's POSITION order — see the gate-1
-- header note on why a bare voter-set reading is unsatisfiable.
immutable relation cfgLt (C : cfgid) (D : cfgid)
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
-- MODEL-EDIT-3 (ledger 21): which configs have COMMITTED (a C_new quorum adopted).
relation cfgCommitted (C : cfgid)

-- commit/log plane (one tracked entry E) + history variables for the induction
relation holdsE (N : node)
individual committed     : Bool
individual committedTerm : term
individual commitCfgid   : cfgid    -- the config in force at commit time
individual commitQuorum  : quorum   -- the certifying quorum (all E-holders)

-- GHOST / HISTORY STATE (session 2). None of these appear in any `require`, so
-- the reachable behaviour set is UNCHANGED — they exist only so the induction
-- can name facts the plain state forgets. (Distinct from a MODEL-EDIT, which
-- changes what the model can do; see spike-ledger.md session 7.)
--   elecCfg N       : the config N's CURRENT leadership was certified against
--                     (`cfgOf N` at becomeLeader — a later `propose` moves
--                     `cfgOf` but must not retroactively re-certify the win).
--   gotEAt N        : the term at which N ACQUIRED E (curTerm at append/replicate).
--   isCommitLeader  : the node that performed the commit (its election evidence
--                     must outlive its leadership for the same-term case of P2).
--   commitElecCfg   : that node's elecCfg at commit time.
function elecCfg (N : node) : cfgid
--   cfgAt N        : the term at which N last CHANGED its adopted config. Mirrors
--                    `gotEAt`: `adopt` requires `curTerm j <= curTerm i`, so a grant
--                    at a term strictly above `cfgAt V` POSTDATES V's adoption and the
--                    config-currency guard therefore did apply at that grant.
--   cfgCommitTerm C: the term in which config C committed.
function cfgAt (N : node) : term
function cfgCommitTerm (C : cfgid) : term
function gotEAt  (N : node) : term
relation isCommitLeader (N : node)
individual commitElecCfg : cfgid

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

-- MODEL-EDIT-2 chain-order axioms. All three are PROVED of witnesses W1 and W2 in
-- QuorumAdjacency.lean (`i_cfglt_irrefl` / `i_cfglt_trans` / `i_succ_cfglt`).
assumption [cfglt_irrefl]
  ∀ (c : cfgid), ¬ cfgLt c c
assumption [cfglt_trans]
  ∀ (c d e : cfgid), cfgLt c d → cfgLt d e → cfgLt c e
assumption [succ_cfglt]
  ∀ (c d : cfgid), succCfg c d → cfgLt c d
-- MODEL-EDIT-2b (ledger 19): config history is ONE CHAIN, not a tree — real UC
-- linearizes it through the log. A NARROWING, inside the (n1) canonical-prefix
-- boundary already declared. Witness: QuorumAdjacency.lean `l_cfglt_total` (W2).
assumption [cfglt_total]
  ∀ (c d : cfgid), c = d ∨ cfgLt c d ∨ cfgLt d c
-- the order is GENERATED by succCfg: no room strictly between a config and its
-- successor, and every strict step upward starts with a succ-step that does not
-- overshoot. Witnesses: `l_succ_immediate` / `l_cfglt_connected` (W2).
-- genesis is the least config (witness: `l_genesis_least`, W2 with genesisC := 0).
assumption [genesis_least]
  ∀ (c : cfgid), ¬ cfgLt c genesisC
assumption [succ_immediate]
  ∀ (c d e : cfgid), succCfg c d → cfgLt c e → cfgLt e d → False
assumption [cfglt_connected]
  ∀ (c d : cfgid), cfgLt c d → ∃ e, succCfg c e ∧ (e = d ∨ cfgLt e d)

-- !!! OBLIGATION — DISCHARGED, route r1 (QuorumAdjacency.lean) !!!
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
  elecCfg N := genesisC
  gotEAt N := tot.zero
  cfgAt N := tot.zero
  cfgCommitTerm C := tot.zero
  isCommitLeader N := false
  commitElecCfg := genesisC
  pending N := false
  hasProposal N := false
  proposedC N := genesisC
  propAfterE N := false
  hasAdopted J I := false
  cfgCommitted C := false
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
  elecCfg i := cfgOf i                       -- frozen from CANDIDACY (ghost)
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
  -- MODEL-EDIT-2 (gate-1 approved w/ revisions): `log_ok` covers CONFIG entries too —
  -- a voter whose adopted config is strictly AHEAD of the candidate's refuses. Rust:
  -- election.rs:342-350 / :1240-1247, over `durable`, which contains config frames
  -- (gate doc §5 Q2 link 1). Narrowings (n1)/(n2) in the header.
  require ¬ cfgLt (cfgOf c) (cfgOf j)
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
  elecCfg i := cfgOf i
}

action crashRestart (i : node) {
  candidate i := false
  leader i := false
}

-- ---------- commit/log plane ----------

action appendEntry (i : node) {
  require leader i
  require ¬ committed
  if ¬ holdsE i then gotEAt i := curTerm i
  holdsE i := true
}

action replicate (i : node) (j : node) {
  require leader i
  require holdsE i
  require tot.le (curTerm j) (curTerm i)
  if ¬ holdsE j then gotEAt j := curTerm i
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
  -- MODEL-EDIT-1 (gate-1 approved as specified): a leader counts only reports
  -- TERM-STAMPED WITH ITS OWN TERM. Rust (primary anchor): election.rs:545-552 drops
  -- `term < current_term` ("stale report: dropped") and turns `term > current_term`
  -- into adopt_term+return, so only own-term reports reach `tracker.on_durable`
  -- (:566-570). Companion only: the Figure-8 `new_term_pos` clamp (:1451-1456).
  -- Stated over the acquisition term, i.e. STRICTLY WEAKER than the Rust gate (which
  -- demands report term = leader term) — an over-approximation, the sound direction.
  -- Without it P2 is REACHABLY FALSE here (n=5 Figure-8 trace, ledger item 14).
  require ∀ V, qmember V q → tot.le (gotEAt V) (curTerm i)
  committed := true
  committedTerm := curTerm i
  commitCfgid := cfgOf i
  commitQuorum := q
  isCommitLeader N := false
  isCommitLeader i := true
  commitElecCfg := elecCfg i
}

-- ---------- config plane ----------

-- propose = leader-adopts-at-append, one in flight, one ±1 step along succCfg.
action propose (i : node) (d : cfgid) {
  require leader i
  require ¬ pending i
  -- MODEL-EDIT-3 (ledger 21): one change in flight CLUSTER-WIDE, not per node.
  -- PRIMARY anchor — this require is the literal abstraction of `config_pending()`:
  -- `config_position > commit_seen` (election.rs:854-858), enforced at :879-881
  -- (`ChangePending`); it blocks the SAME-leader C1→C2 path. COMPLEMENTARY: the
  -- serving latch (propose_config → NotServing, election.rs:876-878, "the
  -- single-server-change precondition") blocks the NEW-leader path, since commit is
  -- prefix-closed so an own-term commit also commits the adopted config entry.
  -- Without this, election_safety is REACHABLY FALSE here (n=5 trace, ledger 21).
  require cfgOf i = genesisC ∨ cfgCommitted (cfgOf i)
  require succCfg (cfgOf i) d
  cfgOf i := d
  cfgAt i := curTerm i
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
  -- MODEL-EDIT-2c (ledger 20): adoption is FORWARD-ONLY. Primary anchor: the
  -- version gate — `ConfigObserved` returns early on `config.version <=
  -- self.config.version` (election.rs:751-756), and `ClusterConfig::apply` bumps the
  -- version by exactly one (config.rs:133). Supporting: the archive's recorded-block
  -- walk is position-ordered (gate doc §5 Q2 link 1), and snapshot fiat adoption is
  -- forward by its `durable < floor` gate. Excluded: the truncation revert — see (n3).
  require ¬ cfgLt (proposedC i) (cfgOf j)
  candidate j := false
  leader j := false
  curTerm j := curTerm i
  cfgOf j := proposedC i
  cfgAt j := curTerm i
  hasAdopted j i := true
}

-- config entry commits like any entry: a C_new quorum of adopters (session gap 1).
action commitCfg (i : node) (q : quorum) {
  require pending i
  -- bookkeeping correction (session 2): the entry that commits is the PROPOSED
  -- config, and its C_new quorum is a quorum OF THAT config. Session 1 wrote
  -- `cfgOf i`, relying on the (reachable-state-only) identity proposedC i = cfgOf i.
  require quorumOf q (proposedC i)
  require ∀ V, qmember V q → hasAdopted V i
  cfgCommitted (proposedC i) := true
  cfgCommitTerm (proposedC i) := curTerm i
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
-- CTI-1 fix (run 1, `propose`): a leader's win is certified against the config
-- in force AT ELECTION TIME; `propose` moves `cfgOf` without re-certifying, so
-- the clause must name `elecCfg`, not `cfgOf`.
invariant [leader_quorum]
  leader I → ∃ (q : quorum), quorumOf q (elecCfg I) ∧
    (∀ V, qmember V q → (V = I ∨ voteMsg V I (curTerm I)))

-- commit-plane clauses:
invariant [commit_backed]        -- the certifying quorum really holds E (holdsE is monotone)
  committed → (∀ V, qmember V commitQuorum → holdsE V)
invariant [commit_quorum_sound]  -- ...and is a quorum of the config in force at commit
  committed → quorumOf commitQuorum commitCfgid
invariant [commit_term_bound]    -- E was committed at a term some leader actually reached
  committed → ∃ N, tot.le committedTerm (curTerm N)

-- CTI-3 fix (run 1, `becomeLeader`): the up-to-date grant guard says a HOLDER
-- never grants to a NON-holder — but only at grant time. `gotEAt` supplies the
-- ordering: a grant at a term strictly ABOVE the voter's acquisition term must
-- postdate that acquisition (grants raise curTerm; replicate/append require
-- curTerm <= source), so the guard applied and the candidate holds E.
invariant [holder_grants_are_covered]
  (voteMsg V C T ∧ holdsE V ∧ tlt (gotEAt V) T) → holdsE C

-- the commit leader's election evidence must outlive its leadership: P2's
-- same-term case (curTerm L = committedTerm) closes through grant_uniq against
-- THIS quorum, not through the transient `leader` flag.
invariant [commit_leader_evidence]
  committed → ∃ (i : node), isCommitLeader i ∧ holdsE i ∧
    (∃ (q : quorum), quorumOf q commitElecCfg ∧
      (∀ V, qmember V q → (V = i ∨ voteMsg V i committedTerm)))

-- ghost-state soundness (keeps the commit-leader evidence usable: without these
-- the solver invents pre-states with two commit leaders, or one before any commit)
invariant [commit_leader_unique]
  (isCommitLeader I ∧ isCommitLeader J) → I = J
invariant [commit_leader_only_after_commit]
  isCommitLeader I → committed
invariant [gotE_bounded]
  holdsE V → tot.le (gotEAt V) (curTerm V)

-- ---- config-chain bookkeeping (consumes MODEL-EDIT-2/2b/2c/3) ----
invariant [cfg_from_genesis]
  cfgOf N = genesisC ∨ cfgLt genesisC (cfgOf N)
invariant [proposal_from_genesis]
  hasProposal I → (proposedC I = genesisC ∨ cfgLt genesisC (proposedC I))
invariant [proposal_is_own_cfg]
  hasProposal I → (proposedC I = cfgOf I ∨ cfgLt (proposedC I) (cfgOf I))
invariant [eleccfg_not_ahead]
  elecCfg N = cfgOf N ∨ cfgLt (elecCfg N) (cfgOf N)
invariant [adopters_not_behind]
  hasAdopted V I → ¬ cfgLt (cfgOf V) (proposedC I)
-- MODEL-EDIT-3's payload: a committed config was adopted by a quorum of ITSELF, and
-- those adopters are at-or-past it forever after (adopters_not_behind + no regression).
-- a candidate's config cannot move (adopt clears candidacy; propose requires leader),
-- so `elecCfg` really is the config it stood for election under.
-- the two role/term hygiene clauses the run-9 CTIs demanded (both (a)-class)
invariant [role_exclusive]
  ¬ (candidate I ∧ leader I)
invariant [reqvote_term_reached]
  reqVote C T → tot.le T (curTerm C)
invariant [vote_term_reached]
  voteMsg V C T → tot.le T (curTerm C)
invariant [cand_cfg_frozen]
  candidate I → elecCfg I = cfgOf I
-- THE cfgAt CLAUSE (gate ruling 3): the granter-advanced-after-granting shape. A grant
-- at a term strictly above the voter's last config change POSTDATES that change, so the
-- config-currency guard applied with exactly the voter's CURRENT config — and the
-- candidate's config at that moment was its frozen `elecCfg`.
invariant [grant_cfg_covered]
  (voteMsg V C T ∧ curTerm C = T ∧ (candidate C ∨ leader C) ∧ tlt (cfgAt V) T)
    → ¬ cfgLt (elecCfg C) (cfgOf V)
-- NOTE (run-9 finding): the `tot.le (cfgAt V) (cfgCommitTerm D)` conjunct that the
-- `no_stale_election` argument wants CANNOT be carried here — an adopter's `cfgAt`
-- RISES when it later moves further along the chain, so the bound is not preserved
-- (it broke a previously-inductive clause at propose/adopt/commitCfg). The ordering
-- fact needed is per-(node, config) — "the term at which V FIRST reached D or later" —
-- which needs its own ghost. Next session's first move; see the ledger.
invariant [committed_cfg_quorum]
  cfgCommitted D → ∃ (q : quorum), quorumOf q D ∧ (∀ V, qmember V q → ¬ cfgLt (cfgOf V) D)
invariant [pending_iff_proposal]
  pending I ↔ hasProposal I
-- EDIT-1's payload, carried to the induction: every commit-quorum member acquired E
-- no later than the commit term (so any grant it makes at a HIGHER term postdates the
-- acquisition, and `holder_grants_are_covered` fires).
invariant [commitq_gotE]
  committed → (∀ V, qmember V commitQuorum → tot.le (gotEAt V) committedTerm)
-- EDIT-3's payload: every config strictly below one that some node has adopted has
-- already COMMITTED (one change in flight cluster-wide).
invariant [chain_committed_below]
  cfgLt D (cfgOf N) → (D = genesisC ∨ cfgCommitted D)
-- ...hence no leader was ever elected under a config strictly below a committed one:
-- a committed D has a quorum of at-or-past-D adopters, which (adjacency) meets every
-- quorum of D's predecessor, and those adopters refuse a candidate that is behind.
-- REPLACES `eleccfg_not_stale`, WHICH WAS FALSE AS STATED (session-2 correction): a
-- STALE LEADER IS LEGAL — UC has no check-quorum step-down, so a leader elected under
-- an old config keeps its flag while the cluster commits later configs. The property
-- is about TERMS, not about the leader flag: an election at a term ABOVE the one a
-- config committed in cannot have run under a config below it.
invariant [no_stale_election]
  (leader I ∧ cfgCommitted D ∧ tlt (cfgCommitTerm D) (curTerm I)) → ¬ cfgLt (elecCfg I) D
invariant [commit_cfg_backed]
  cfgCommitted D → (D = genesisC ∨ cfgLt genesisC D)

-- THE LOAD-BEARING CANDIDATE (early signal, unproved): after commit, EVERY config
-- any node has adopted only has quorums that contain an E-holder — the invariant
-- that blocks a non-holder's election. Expect the CTI loop to refine this (e.g.
-- restricting to configs reachable along succCfg from commitCfgid, or threading
-- propAfterE through the proposal chain); it is where the adjacency lemma and the
-- prefix coupling must meet.
invariant [electable_cfgs_contain_holder]
  committed → (∀ N (q : quorum), quorumOf q (cfgOf N) → ∃ V, qmember V q ∧ holdsE V)

#gen_spec

-- RUN 10 (post-gate-1b, final): cfgAt template + the (a)-class clause repairs run 9
-- demanded. Ghost state + clauses ONLY — no new `require`, no new assumption.
#check_invariants
