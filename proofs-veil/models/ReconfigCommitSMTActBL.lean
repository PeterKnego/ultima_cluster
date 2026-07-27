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

   ===== GATE-1c AMENDMENTS (2026-07-27) — MODEL-EDIT-4 APPROVED + APPLIED =====
   * MODEL-EDIT-4 is APPLIED in `propose` (see the three gate amendments (a)/(b)/(c)
     recorded inline there). **NEW BASELINE: 35 `require`s / 11 `assumption`s.**
     `cfgCommitTerm` is thereby LOAD-BEARING, no longer ghost.
   * GATE AMENDMENT (d) — CORNER, belongs in the final claim verbatim: `commit_seen` is
     NOT reset at `become_leader` (election.rs:1040-1056), so a fresh leader carries
     commit state inherited from its follower period. That inherited value cannot satisfy
     the serving latch (:522-527 needs `commit_seen ≥` the fresh NewTerm position)
     without a pre-existing completeness violation — the SAME conditionality bucket as
     narrowing (n1) below.
   * GATE AMENDMENT (e): run 12's `propose` CTI is an UNREACHABLE pre-state (leader at
     `curTerm` zero). The reachability evidence is the gate's hand trace: proposer p
     proposes cfg2 at t_p; node 0 adopts and wins t1 > t_p; a second cfg2-adopter wins
     t0 > t1, and p's grant to it lifts `curTerm p` to t0; `commitCfg` stamps
     `cfgCommitTerm cfg2 = t0`; leader 0 at t1 < t0 legally proposes past cfg2.
   * THE OPEN-CLAUSE TRUTH RULE (gate 1c, binding): a clause with open CTIs is a LIVE
     HYPOTHESIS for every verdict in its bundle. No run's greens may be quoted as
     progress unless every clause still open in that run carries either a WRITTEN truth
     argument (it holds in all reachable states of the current model) or an explicit
     CONDITIONAL label naming what its truth awaits. A clause later found false VOIDS the
     quoted greens of every run that carried it.
   =============================================================

   SESSION-5 (bar 3, part 5) STATE — see proofs-veil/spike-ledger.md §SESSION 10 (32-40):
     * **⏱️ PROTOCOL (BINDING).** Veil prints THREE verdict markers per VC, and ⏱️ (one per
       TIMED-OUT VC) is an **OPEN** verdict — never a green, never a red. Grep every log
       (`grep -c "⏱️"`) before banking it and quote **"N ✅ / M ❌ / K ⏱️"**. Whole-arc audit:
       exactly three ⏱️ ever — runs 12, 13, 14. **Run 14's `election_safety` was a ⏱️, never
       a ✅**: the session-4 retraction STANDS with a corrected cause, and NO tool distrust is
       warranted (the "unsound ✅" gate-2 item is dissolved). Run 16 = 470 ✅ / 3 ❌ / 0 ⏱️,
       timeout-clean, still the baseline.
     * **ADDED: `leader_reach_strict` (T12)** — CERTIFIED INDUCTIVE, all-n, cvc5, in an
       80-SECOND nine-clause SLICE (run 20, `ReconfigCommitSMTSlice.lean`). A slice ✅
       TRANSFERS to the full bundle (`Inv_full → Inv_slice` only weakens each VC's
       antecedent); a slice ❌/⏱️ transfers neither way. It answers BOTH open tasks: the
       same-term grant corner (the SESSION-4 note below predicting a GRANT-TIME ghost is
       SUPERSEDED — the voter's side of that CTI is legal, the leader's side is not), and,
       at `C := genesisC` with the theory's `zero_le`, the withdrawn `role_positive_term`.
     * `election_safety`@`becomeLeader`: **OPEN (⏱️), NO LONGER REFUTED.** Run 16's CTI
       violates T12 (hand check: `reachAt 1 (elecCfg 1) = curTerm 1`). Runs 21/23 (a
       17-clause election slice) leave that VC ⏱️ even at 900 s per VC. Carried under the
       truth rule with the WRITTEN argument T8 + T12. The obstacle is now solver search,
       not a missing invariant.
     * P2 residue UNCHANGED and mapped (ledger 36): same-term half = the `commitElecQuorum`
       ghost + T13/T14 (written, count-exempt, UNMEASURED); strict half = the cross-config
       HOLDER supply, with **MODEL-EDIT-5 PREPARED BUT NOT REQUESTED**.
     * **TOOLCHAIN: `set_option veil.smt.timeout N in <cmd>` DOES NOT PROPAGATE** — it must
       be FILE SCOPE (as at the top of this file). Runs 17/18/19 died at ~30/30/96 min with
       no verdict; run 19 was therefore never the 12 s run it was launched as.
     * COUNT UNCHANGED: **35 `require`s / 11 `assumption`s**, verified before every launch.
       Everything added this session is clause-only; `QuorumAdjacency.lean` untouched.

   SESSION-4 (bar 3, part 4) STATE — see proofs-veil/spike-ledger.md §SESSION 9:
     * **RUN 16: 470 ✅ / 3 ❌ — 40 clauses + `doesNotThrow` INDUCTIVE, all-n, cvc5**
       (run 12: 32; run 14: 38). 35 requires / 11 assumptions, verified before every run.
     * ADDED: MODEL-EDIT-4 (above); the `elecQuorum` certifying-quorum ghost and
       `elecq_witness` / `elecq_grant_covers_reach`; `cand_reach_strict`;
       `voteterm_bounded` / `commit_leader_self_vote` / `commit_leader_no_foreign_grant`
       (the persistent carrier); `commit_leader_at_commit_cfg`.
       CLOSED: `reach_quorum_below`, `no_stale_election` (both sites).
     * RETIRED with written rulings (below, at their sites): `grant_cfg_covered` and
       `electable_cfgs_contain_holder`.
     * WITHDRAWN FOR COST, NOT TRUTH: `role_positive_term` — run 15 carried it for
       2h28m wall / 3h10m CPU with NO verdict. `veil.smt.timeout` is 60 s PER VC, so at
       this bundle size a single expensive clause converts a 10-min run into a ~7-h one
       with no partial output.
     * !! **`election_safety`'s RUN-14 GREEN IS RETRACTED** (ledger 29): run 16's CTI was
       hand-checked to satisfy run 14's bundle as well, so the earlier ✅ was not sound.
       A ✅ here means "not refuted by this bundle at this solver configuration".
     * STILL OPEN: `election_safety` (1, `becomeLeader`) — the SAME-TERM GRANT WRINKLE:
       `grant_reach_covered`'s bound is STRICT, so a voter that reached its config at the
       very term it granted at is uncovered; needs a GRANT-TIME config ghost (the `gotEAt`
       pattern applied to the grant), not a model edit. `leader_completeness` (2):
       `becomeLeader` needs the cross-config HOLDER supply (`commit_leader_at_commit_cfg`
       recovers the QUORUM supply only); `commitEntry` awaits a cheaper
       `role_positive_term`.

   SESSION-3 (bar 3, part 3) STATE — see proofs-veil/spike-ledger.md §SESSION 8:
     * `#check_invariants` IS run (bottom of file). **RUN 12: 409 OK / 8 CTI —
       32 clauses + `doesNotThrow` CERTIFIED INDUCTIVE, all-n, via cvc5.**
     * NEW THIS SESSION (clause/ghost ONLY — 34 `require`s / 11 assumptions,
       unchanged from run 10): the per-(node, config) first-reach ghost `reachAt`
       and its clauses (`reach_bound`, `reach_mono`, `grant_reach_covered`,
       `eleccfg_covers_early_reach`, `adopted_reach_bound`, the strengthened
       `committed_cfg_quorum`, and `reach_quorum_below`).
     * CORRECTION: `electable_cfgs_contain_holder` as stated through run 11 was
       FALSE in reachable states (ledger 25) — restricted here to configs
       at-or-above `commitCfgid`.
     * STILL OPEN: `reach_quorum_below` (1 CTI, `propose`),
       `electable_cfgs_contain_holder` (3), `grant_cfg_covered` (1 — RETIRE in
       favour of `grant_reach_covered`, ledger 27), `election_safety` (1),
       `leader_completeness` = P2 (1), `no_stale_election` (1).
     * !! THE WHOLE RESIDUE IS BLOCKED ON ONE UNAPPROVED EDIT !!
       **MODEL-EDIT-4 (ledger 26, REQUESTED AT GATE 1c, NOT APPLIED):** `propose`
       must also require `tot.le (cfgCommitTerm (cfgOf i)) (curTerm i)`. The model's
       `cfgCommitted` is a GLOBAL flag; real UC's gate is the LEADER'S OWN
       own-term-certified commit view (`config_pending()` = `config_position >
       commit_seen`, election.rs:854-858 enforced :879-880; a leader's `commit_seen`
       has one writer, `rank_leader` :1421-1457, clamped to `new_term_pos`; the
       gossip intake :594-595 is explicitly non-leader). Until it is approved,
       **`reach_quorum_below` is expected FALSE here, and the two run-12 greens it
       supports (`no_stale_election`@becomeLeader, `leader_completeness`@commitEntry)
       are CONDITIONAL on that approval.**
     * HISTORY STATE: `elecCfg` / `isCommitLeader` / `commitElecCfg` / `cfgAt` /
       `reachAt` / `elecQuorum` appear in NO `require`. `gotEAt` and `cfgCommitTerm`
       do NOT belong to that list: MODEL-EDIT-1 promoted `gotEAt` to load-bearing and
       MODEL-EDIT-4 has now promoted `cfgCommitTerm` the same way.

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

-- RUN 24 (session 5): FILE-SCOPE per-VC budget. TOOLCHAIN FINDING: `set_option
-- veil.smt.timeout N in #check_invariants` does NOT propagate to the solver calls (run 19
-- at "12 s" and run 22 at "900 s" both behaved exactly like the 60 s default); the option
-- must be set at FILE SCOPE, as here (proved by run 23, where the same slice went from
-- 3 min to 30 min). At 5 s the whole 41-clause bundle is bounded by ~490 x 5 s ~ 41 min,
-- which is what makes a full-bundle measurement possible at all this session. Greens at
-- 5 s are real greens; ⏱️ are OPEN verdicts (ledger 32).
set_option veil.smt.timeout 60

veil module UcReconfigCommitSMTActBL

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
--   reachAt N C   : SESSION-3 GHOST — the term at which N FIRST reached C-or-later
--                   ("reached C" := ¬ cfgLt (cfgOf N) C, a MONOTONE predicate because
--                   MODEL-EDIT-2c makes adoption forward-only). This is the
--                   per-(node, config) ordering fact the run-9 finding (ledger 24)
--                   showed `cfgAt` cannot carry: `cfgAt V` RISES when V moves further
--                   along the chain, whereas `reachAt V C` FREEZES the moment V
--                   reaches C. Written at the two `cfgOf` writers (propose/adopt) for
--                   EVERY config the move newly covers; read in NO `require`.
function reachAt (N : node) (C : cfgid) : term
--   elecQuorum N  : SESSION-4 GHOST (gate-1c corrected residue map) — the quorum N's
--                   CURRENT leadership was certified against, frozen at `becomeLeader`.
--                   `leader_quorum`'s ∃q is NOT a substitute: its witness can be
--                   supplied by LATE grants made against a MOVED config, and
--                   `cand_cfg_frozen` (the `becomeLeader`-time bridge) is unavailable
--                   for a LONGSTANDING leader — which is exactly why
--                   `no_stale_election` fails at `commitCfg`. Read in NO `require`.
function elecQuorum (N : node) : quorum
function gotEAt  (N : node) : term
relation isCommitLeader (N : node)
individual commitElecCfg : cfgid
--   commitElecQuorum : SESSION-6 GHOST (ledger item 36 case (A), truth argument T18) —
--                   the CERTIFYING QUORUM of the commit leader's election, frozen at
--                   `commitEntry`. Same lesson as `elecQuorum`: `commit_leader_evidence`'s
--                   exists-q can be witnessed by LATE grants against a MOVED config; a frozen
--                   ghost cannot. Read in NO `require` — count stays 35/11, no gate.
individual commitElecQuorum : quorum

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
  reachAt N C := tot.zero
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
  elecQuorum i := q                          -- GHOST: freeze the CERTIFYING quorum
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
  commitElecQuorum := elecQuorum i         -- GHOST (T18)
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
  -- MODEL-EDIT-4 (ledger 26; GATE 1c APPROVED with binding amendments (a)-(e)).
  -- The clause above uses `cfgCommitted`, a GLOBAL flag set by any node at any term,
  -- so the model let a LOW-term leader propose past a config that committed at a HIGH
  -- term. Real UC's gate is the LEADER'S OWN, own-term-certified commit view:
  -- `config_pending()` = `config_position > commit_seen` (election.rs:856-858) enforced
  -- at :879-880, and a LEADER's `commit_seen` has exactly TWO writers — the gossip
  -- intake at :594-595, which is literally leader-excluded, and `rank_leader` at :1457,
  -- behind the Finding-#6b clamp at :1451-1456 (init :431). So a leader may propose past
  -- C only after IT ITSELF ranked a commit covering C's frame, at its own term.
  -- Written as a SECOND require rather than a conjunct inside the first: the two are
  -- equivalent by distribution (`g ∨ (a ∧ b)` = `(g ∨ a) ∧ (g ∨ b)`) and this makes the
  -- mechanical count honest at 35.
  -- ** GATE AMENDMENT (a): `cfgCommitTerm` is the PROPOSER-STAMPED term at commitCfg-fire
  --    time (:439 below), NOT the real certification term — the proposer's term drifts
  --    upward, so the stamp can EXCEED the term a quorum actually certified at.
  -- ** GATE AMENDMENT (b) — the over-approximation argument, without which this edit is
  --    unproven: every real UC behaviour maps into a model behaviour satisfying this
  --    require, because (i) `commitCfg` has no message plane and may fire at the EARLIEST
  --    enabling point, before causally-independent term raises, so the real certification
  --    term is always an available stamp; and (ii) the own-term report gate
  --    (election.rs:545-552) makes a certifying quorum's adoption evidence causally
  --    independent of any term above the certifying leader's — any member touched by a
  --    higher term would depose that leader before ranking it.
  -- ** GATE AMENDMENT (c) — PROHIBITION: never strengthen this `≤` to `=`. Ledger 26's
  --    "(in fact =)" holds only for the AUTHORIZING advance; `=` would UNDER-approximate,
  --    since a leader's SECOND proposal compares against a config committed earlier.
  require cfgOf i = genesisC ∨ tot.le (cfgCommitTerm (cfgOf i)) (curTerm i)
  require succCfg (cfgOf i) d
  -- GHOST: freeze the term at which i first reaches each config this move covers.
  reachAt i Z := if (cfgLt (cfgOf i) Z ∧ ¬ cfgLt d Z) then curTerm i else reachAt i Z
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
  -- GHOST: same freeze, at the adopter (the term is the proposer's, which is also
  -- j's new term — `adopt` requires curTerm j <= curTerm i).
  reachAt j Z := if (cfgLt (cfgOf j) Z ∧ ¬ cfgLt (proposedC i) Z) then curTerm i else reachAt j Z
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
-- **RETIRED, SESSION 4 (gate-1c map; retirement pre-sanctioned).** WRITTEN RULING: this
-- is the session-2 `elecCfg`-flavoured ANCESTOR of `grant_reach_covered`. It has never
-- been inductive (1 CTI at the grant arm in every run since 9) because its conclusion
-- names `elecCfg C`, which a candidate that has WON and then PROPOSED no longer matches —
-- a late same-term grant compares against the MOVED `cfgOf C`. `grant_reach_covered`
-- states the preservable form and IS inductive; every consumer in the residue map (the
-- `elecq_*` route below, `no_stale_election`, `election_safety`) goes through
-- `cfgOf`/the frozen quorum. NOTHING consumes this clause. Under the gate's open-clause
-- truth rule its retirement is MANDATORY, not optional: a clause with open CTIs and no
-- truth argument is a live FALSE hypothesis for every green around it.
-- invariant [grant_cfg_covered]
--   (voteMsg V C T ∧ curTerm C = T ∧ (candidate C ∨ leader C) ∧ tlt (cfgAt V) T)
--     → ¬ cfgLt (elecCfg C) (cfgOf V)
-- NOTE (run-9 finding): the `tot.le (cfgAt V) (cfgCommitTerm D)` conjunct that the
-- `no_stale_election` argument wants CANNOT be carried here — an adopter's `cfgAt`
-- RISES when it later moves further along the chain, so the bound is not preserved
-- (it broke a previously-inductive clause at propose/adopt/commitCfg). The ordering
-- fact needed is per-(node, config) — "the term at which V FIRST reached D or later" —
-- which needs its own ghost. Next session's first move; see the ledger.
-- ---- SESSION 3: THE PER-(node, config) FIRST-REACH GHOST (ledger 24's alternative) ----
-- `reachAt N C` freezes when N first reaches C-or-later; these five clauses are its
-- soundness + the two grant-ordering facts the stale-config argument needs.
invariant [reach_bound]                 -- a reached config was reached by now
  ¬ cfgLt (cfgOf N) C → tot.le (reachAt N C) (curTerm N)
invariant [reach_mono]                  -- earlier configs are reached no later
  (cfgLt C D ∧ ¬ cfgLt (cfgOf N) D) → tot.le (reachAt N C) (reachAt N D)
-- SESSION 3, run 12: the clause the stale-config argument actually consumes. For any
-- config C some node N has reached and any E strictly below it, E has a quorum that
-- reached E no later than N reached C. Instantiated at N := a member of a committed D's
-- certifying quorum, it delivers "a quorum of succ(stale cfg) reached it strictly before
-- the stale leader's term", which is exactly what adjacency + `grant_reach_covered` need.
-- EXPECTED TO FAIL at `propose`/`adopt` in run 12: its preservation needs the proposer's
-- own-term bound on the PREDECESSOR's commit term, which the model's GLOBAL
-- `cfgCommitted` flag does not carry. See ledger item 25 (MODEL-EDIT-4 request).
invariant [reach_quorum_below]
  (cfgLt E C ∧ ¬ cfgLt (cfgOf N) C) →
    (E = genesisC ∨ ∃ (q : quorum), quorumOf q E ∧
      (∀ V, qmember V q → ¬ cfgLt (cfgOf V) E ∧ tot.le (reachAt V E) (reachAt N C)))
-- GRANT-POSTDATES-ADOPTION, per-(node, config). A voter that had ALREADY reached D
-- when it granted at T forces the candidate to be at-or-past D — that is exactly the
-- MODEL-EDIT-2 guard `¬ cfgLt (cfgOf c) (cfgOf j)`, frozen by the strict term bound.
-- NOTE the conclusion is over `cfgOf C`, NOT `elecCfg C`: a candidate that has WON and
-- then PROPOSED can still receive late grants at the same term, at which point the
-- guard compares against its MOVED config. `cfgOf` is forward-only, so this form is
-- preserved; `cand_cfg_frozen` bridges it back to `elecCfg` at `becomeLeader`, which
-- is the only action that can newly create a stale-config leader.
invariant [grant_reach_covered]
  (voteMsg V C T ∧ ¬ cfgLt (cfgOf V) D ∧ tlt (reachAt V D) T) → ¬ cfgLt (cfgOf C) D
-- ...and the V = I disjunct of the same intersection: a leader/candidate that itself
-- reached D strictly before its own term stood for election at-or-past D.
invariant [eleccfg_covers_early_reach]
  ((candidate I ∨ leader I) ∧ ¬ cfgLt (cfgOf I) D ∧ tlt (reachAt I D) (curTerm I))
    → ¬ cfgLt (elecCfg I) D
-- the bound that lets `commitCfg` establish the reach conjunct of committed_cfg_quorum
invariant [adopted_reach_bound]
  hasAdopted V I → tot.le (reachAt V (proposedC I)) (curTerm I)
invariant [committed_cfg_quorum]
  cfgCommitted D → ∃ (q : quorum), quorumOf q D ∧
    (∀ V, qmember V q → ¬ cfgLt (cfgOf V) D ∧ tot.le (reachAt V D) (cfgCommitTerm D))
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
-- SESSION-3 CORRECTION — THE CLAUSE AS FIRST STATED IS **FALSE IN REACHABLE STATES**,
-- and by the run-8 lesson (ledger session 8, task 1) that made it an unsound hypothesis
-- for every green around it. Countermodel, every step legal in this model (n=5, W2
-- interpretation): genesis C0={0..4}; leader 0 commits C1=C0∖{4} (adopters 0,1,2 — a
-- 3-of-4 quorum) and then C2=C1∖{3} (adopters 0,1 — a 2-of-3 quorum); node 4 never
-- adopts and stays at C0. Now `appendEntry(0)` + `replicate(0→1)` + `commitEntry(0,{0,1})`
-- commits E under `commitCfgid = C2`. `cfgOf 4 = C0`, and the C0-quorum {2,3,4} holds
-- no E. Real UC is safe there NOT because such a quorum contains a holder — it does not —
-- but because the config-currency guard makes nodes 0,1,2 (at C2) and 3 (at C1) REFUSE a
-- C0-stale candidate. So the clause must be restricted to configs at-or-above the commit
-- config; the stale side is the `no_stale_election` argument's job, not this one's.
-- **RETIRED, SESSION 4 (gate-1c map-or-retire, RETIRE taken).** WRITTEN RULING: item 25
-- already established the UNRESTRICTED form is FALSE in reachable states. The RESTRICTED
-- form below (configs at-or-above `commitCfgid`) still has 3 open CTIs and **no truth
-- argument can be written for it**: the natural chain induction upward from `commitCfgid`
-- FAILS at the successor step — `adjacent_cfg_quorum_intersection` says a quorum of
-- `succ C` MEETS every quorum of C, but the meeting member need not be the holder the
-- inductive hypothesis supplies. Carrying it would repeat exactly the defect of run 8 and
-- of item 25 (an unproved clause propping up its neighbours' greens), so under the truth
-- rule it leaves the bundle. THIS RETIREMENT IS A MEASUREMENT: if P2 and election_safety
-- close without it, "P2's argument nowhere consumes it" is EARNED; if they do not, the
-- residue is honestly re-described as needing a cross-config holder-supply argument.
-- invariant [electable_cfgs_contain_holder]
--   (committed ∧ ¬ cfgLt (cfgOf N) commitCfgid) →
--     (∀ (q : quorum), quorumOf q (cfgOf N) → ∃ V, qmember V q ∧ holdsE V)

-- ---- SESSION 4: the CERTIFYING-QUORUM route + the persistent no-foreign-grant carrier ----
-- (gate-1c corrected residue map; truth arguments T2/T3/T4/T5/T6/T7 in the ledger.)

-- T2 — `leader_quorum` with its ∃q replaced by the FROZEN ghost witness. Sound because
-- `becomeLeader`'s own require establishes it at the only site that creates leadership,
-- and every action that changes `curTerm I` also CLEARS `leader I`.
invariant [elecq_witness]
  leader I → (quorumOf (elecQuorum I) (elecCfg I) ∧
    (∀ V, qmember V (elecQuorum I) → (V = I ∨ voteMsg V I (curTerm I))))
-- T3 — THE CLAUSE THE GATE'S MAP REQUIRES. A member of the FROZEN certifying quorum that
-- had already reached D when it granted forces the leader's ELECTION config at-or-past D.
-- Grants in the frozen quorum all PREDATE `becomeLeader`, i.e. were made while I was a
-- CANDIDATE, and a candidate's config cannot move (`adopt` clears candidacy, `propose`
-- requires `leader`), so `cand_cfg_frozen` gives `elecCfg I = cfgOf I` across that window
-- and the MODEL-EDIT-2 currency guard applied with `cfgOf V >= D`. The antecedent cannot
-- be created AFTER the win: `grant_state` gives `curTerm I <= curTerm V`, and a later
-- `propose`/`adopt` stamps `reachAt V D` with the MOVER's term, which is >= `curTerm V`.
invariant [elecq_grant_covers_reach]
  (leader I ∧ qmember V (elecQuorum I) ∧ ¬ cfgLt (cfgOf V) D ∧ tlt (reachAt V D) (curTerm I))
    → ¬ cfgLt (elecCfg I) D
-- T4 — a candidate reached every config it holds STRICTLY BEFORE its own term:
-- `startElection` bumps strictly past a term that already bounds `reachAt` (`reach_bound`),
-- and nothing re-stamps while candidacy persists. Excludes run 12's `election_safety` CTI
-- (a candidate at `tot.zero` whose `reachAt` equals its term).
invariant [cand_reach_strict]
  (candidate I ∧ ¬ cfgLt (cfgOf I) C) → tlt (reachAt I C) (curTerm I)
-- T12 (SESSION 5) — THE SAME-TERM GRANT CORNER, closed from the OTHER side. The run-16
-- `election_safety` CTI (ledger 30) has a voter that reached its config AT the very term it
-- granted at, which `grant_reach_covered`'s STRICT bound cannot cover. A grant-time config
-- ghost does NOT close it (the model genuinely permits granting at cfg C and adopting a
-- higher config at the same term — `adopt` only requires `curTerm j <= curTerm i`). What IS
-- excluded is the OTHER side of that CTI: the incumbent LEADER reached its own ELECTION
-- config at its own term. A leader's `elecCfg` was frozen while it was a CANDIDATE, and
-- `startElection` bumps STRICTLY past a term that already bounds every reach stamp
-- (`reach_bound`) — so every config at-or-below `elecCfg I` was reached strictly before
-- `curTerm I`. This generalises `cand_reach_strict` from `cfgOf`/candidates to
-- `elecCfg`/both roles; for a candidate the two coincide (`cand_cfg_frozen`).
-- PRESERVATION: `propose` stamps only configs STRICTLY ABOVE `cfgOf i >= elecCfg i`
-- (`eleccfg_not_ahead` + `cfglt_trans`), i.e. outside the antecedent; `adopt`/`replicate`/
-- `crashRestart` clear the role; a strict-raise grant clears the role and an equal-term
-- grant changes nothing; `becomeLeader` re-freezes `elecCfg i := cfgOf i` = the candidate's
-- already-covered config. SECOND CONSUMER (task 2): at `C := genesisC` (antecedent free by
-- `genesis_least`) this yields `reachAt I genesisC < curTerm I`, and with `zero_le` that IS
-- `role_positive_term` — WITHOUT putting `tot.zero` into every VC's hypothesis set, which
-- is what made run 15 intractable (ledger 28).
-- !! RUN 17 (the ∀C form, `((candidate I ∨ leader I) ∧ ¬ cfgLt (elecCfg I) C) →
--    tlt (reachAt I C) (curTerm I)`) WAS KILLED AT 29m42s under the 3x-wall rule (item 28),
--    NO VERDICT. Bisected to the GROUND form below: the same clause at the only two
--    instantiations its consumers use — `C := elecCfg I` (antecedent by `cfglt_irrefl`) and
--    `C := genesisC` (antecedent by `genesis_least`). The quantified `C` ranges over `cfgid`
--    in the hypothesis set of every VC, which is the cost; the ground form both removes that
--    search AND hands the solver the two instances directly. T12's truth argument is
--    unchanged — these are two of its instances.
invariant [leader_reach_strict]
  (candidate I ∨ leader I) →
    (tlt (reachAt I (elecCfg I)) (curTerm I) ∧ tlt (reachAt I genesisC) (curTerm I))
-- T5/T6/T7 — the same-term commit-leader-self-vote hole (P2's last CTI). `grant_state`'s
-- FIRST disjunct absorbs a stray `voteMsg` without constraining `voteCand`, so the absence
-- of a foreign grant at `committedTerm` must be carried PERSISTENTLY; the two supports
-- below are what make the carrier preserved at the grant arm.
invariant [voteterm_bounded]
  tot.le (voteTerm V) (curTerm V)
-- RUN-14 CORRECTION: `hasVoted I` is part of the fact, not a detail. Without it the
-- grant guard `¬(hasVoted j ∧ voteTerm j = t ∧ voteCand j ≠ c)` passes VACUOUSLY for a
-- commit leader the solver gives `hasVoted = []`, and both this clause and the carrier
-- fail at the grant arm. `self_vote` supplies it at commit time and NO action ever
-- clears `hasVoted`.
invariant [commit_leader_self_vote]
  isCommitLeader I → (hasVoted I ∧ tot.le committedTerm (voteTerm I) ∧
    (voteTerm I = committedTerm → voteCand I = I))
invariant [commit_leader_no_foreign_grant]
  (isCommitLeader V ∧ C ≠ V) → ¬ voteMsg V C committedTerm

-- ---- RUN-15 additions: the two clause-only facts P2's remaining CTIs need ----
-- T10 — NO ROLE AT THE ZERO TERM. `candidate` is created only by `startElection`, which
-- requires `tlt (curTerm i) t` and sets `curTerm i := t`; with `zero_le`, t is strictly
-- above `tot.zero`, and `curTerm` only rises. `becomeLeader` inherits it from candidacy.
-- Excludes the run-14 `commitEntry` P2 CTI, whose committing leader sits AT `tot.zero`.
-- !! WITHDRAWN IN RUN 16 — TRACTABILITY, NOT TRUTH (the clause is true; see T10). !!
-- Run 15 carried it and ran **2h28m wall / 3h10m CPU at 139%** with NO verdict before
-- being killed under the box rule (Lean buffers until elaboration ends, so the run
-- yielded nothing). It is the FIRST clause in this bundle to put `tot.zero` into the
-- hypothesis set of every VC, which forces `zero_le` instantiation across the whole
-- term theory alongside the existing `tlt`/`tot.le` clauses — the prime suspect for the
-- blow-up. Withdrawn to isolate it; the `commitEntry` P2 CTI it excludes therefore
-- REMAINS OPEN in run 16, and finding a cheaper encoding of "no role at the zero term"
-- is named work for the next session.
-- invariant [role_positive_term]
--   (candidate I ∨ leader I) → tlt tot.zero (curTerm I)
-- T11 — THE COMMIT LEADER IS AT-OR-PAST THE CONFIG IT COMMITTED UNDER. `commitEntry` sets
-- `commitCfgid := cfgOf i` at the same instant it sets `isCommitLeader i`, and `cfgOf` is
-- FORWARD-ONLY (propose moves along `succCfg`; adopt requires `¬cfgLt (proposedC i) (cfgOf j)`),
-- so it can never fall back below. Load-bearing: with `chain_committed_below` it forces
-- every config strictly below `commitCfgid` to be COMMITTED, which (via
-- `committed_cfg_quorum`) SUPPLIES THE QUORUMS the adjacency chain needs. The run-14
-- `becomeLeader` P2 CTI exploits exactly their absence — it interprets the intermediate
-- config as having NO quorum at all, so `adjacent_cfg_quorum_intersection` is vacuous and
-- the chain from the commit config down to the stale candidate's config breaks.
invariant [commit_leader_at_commit_cfg]
  isCommitLeader I → ¬ cfgLt (cfgOf I) commitCfgid

-- ---- SESSION 6: the two LEMMA CLAUSES that cut `election_safety`'s chain (T15/T17) ----
-- Both CERTIFIED INDUCTIVE in their own slices before landing here (runs 26/27, 0 timeouts):
-- `ReconfigCommitSMTT15Slice.lean` (130 OK / 2 CTI / 0 TMO, 83 s) and
-- `ReconfigCommitSMTT17Slice.lean` (141 OK / 2 CTI / 0 TMO, 91 s); in both, the two CTIs are
-- `reach_quorum_below` at propose/adopt, the KNOWN slice artifact of omitting
-- `chain_committed_below` / `committed_cfg_quorum` / `adopted_reach_bound` (run 21 precedent),
-- and that clause is inductive in the FULL bundle from run 13 on.
-- T15 — pre-composes `reach_quorum_below` with `leader_reach_strict` (T12): every config
-- strictly below a role's ELECTION config is genesis or has a quorum, all of whose members
-- are at-or-past it AND reached it STRICTLY BEFORE that role's term. Stated over
-- `candidate or leader` (not `leader`) precisely so that at `becomeLeader` the pre-state
-- instance carries verbatim — `cand_cfg_frozen` says the action's `elecCfg i := cfgOf i`
-- changes nothing.
invariant [role_below_quorum_strict]
  ((candidate I ∨ leader I) ∧ cfgLt C (elecCfg I)) →
    (C = genesisC ∨ ∃ (q : quorum), quorumOf q C ∧
      (∀ V, qmember V q → ¬ cfgLt (cfgOf V) C ∧ tlt (reachAt V C) (curTerm I)))
-- T17 — pre-composes T15 with `adjacent_cfg_quorum_intersection`, so no single VC has to
-- find the `cfglt_connected` -> `reach_quorum_below` -> adjacency -> grant-clause sequence.
invariant [role_below_meets_quorum]
  ((candidate I ∨ leader I) ∧ succCfg F E ∧ cfgLt E (elecCfg I) ∧ quorumOf Q F) →
    (∃ V, qmember V Q ∧ ¬ cfgLt (cfgOf V) E ∧ tlt (reachAt V E) (curTerm I))

-- ---- SESSION 6: the FROZEN COMMIT-LEADERSHIP EVIDENCE (T18/T13/T14; ledger 36 case (A)) ----
-- P2's `becomeLeader` CTI is, in its SAME-TERM half, an election-safety argument against a
-- leadership that no longer carries the `leader` flag (the commit leader may have
-- crash-restarted: `crashRestart` clears `leader` without touching `curTerm`). So the
-- `elecQuorum` route (T2/T3) is replayed over the commit leader's evidence, frozen at
-- `commitEntry`. Run 25's checker-produced CTI is excluded by T18 + T13 — see ledger 42.
invariant [commitq_witness]
  (committed ∧ isCommitLeader I) →
    (quorumOf commitElecQuorum commitElecCfg ∧
     (∀ V, qmember V commitElecQuorum → (V = I ∨ voteMsg V I committedTerm)))
invariant [commitq_grant_covers_reach]
  (committed ∧ qmember V commitElecQuorum ∧ ¬ cfgLt (cfgOf V) D ∧
    tlt (reachAt V D) committedTerm) → ¬ cfgLt commitElecCfg D
invariant [commit_leader_frozen_reach]
  isCommitLeader I →
    (¬ cfgLt (cfgOf I) commitElecCfg ∧ ¬ cfgLt commitCfgid commitElecCfg ∧
     tlt (reachAt I commitElecCfg) committedTerm ∧
     tot.le (reachAt I commitCfgid) committedTerm)

#gen_spec

-- RUN 12 (session 3): + `reach_quorum_below` (expected ❌ at propose/adopt — the
-- MODEL-EDIT-4 evidence) and the CORRECTED `electable_cfgs_contain_holder`. Clause-only:
-- verified 34 requires / 11 assumptions, identical to the run-10/run-11 baseline.
-- RUN 19 (session 5): `veil.smt.timeout` LOWERED from its 60 s default to 12 s. Runs 17/18
-- (the ∀C form and the GROUND form of `leader_reach_strict`) were both KILLED at ~30 min
-- under the 3x-wall rule with NO verdict, because Lean buffers everything until elaboration
-- ends. A lower per-VC bound makes the run FINISH: every VC that closes still closes (a
-- proof found at 12 s is a proof), and every VC that does not is reported as ⏱️ = OPEN, per
-- the session-5 ⏱️ protocol (ledger 32). Greens from this configuration are quotable; ⏱️
-- are not, and the clause carrying one is OPEN regardless of the tally.
#check_action becomeLeader
