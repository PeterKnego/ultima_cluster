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
set_option veil.smt.timeout 20

veil module PropG3

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
--   SESSION-8 GHOSTS (T20, the holder supply indexed by COMMITTED CONFIG). All four
--   are written at `propose`/`commitCfg` and read in NO `require` — the count stays
--   35 requires / 11 assumptions and the reachable behaviour set is unchanged.
--   cfgSeen C  : C has been PROPOSED at least once.
--   cfgPred C  : the config the proposer sat at when it proposed C — the
--                config→proposer link session 12 (T20) identified as missing. It is
--                what lets a VC NAME the predecessor of a config without a new
--                connectivity assumption (the chain axioms only connect UPWARD).
--   cfgQ C     : the ADOPTER quorum that certified C's config commit, frozen at
--                `commitCfg` (same lesson as `elecQuorum`/`commitElecQuorum`: the
--                `hasAdopted` evidence is CLEARED by the next `propose`, so the
--                quorum must be frozen to stay usable).
--   cfgBacked C: C's proposal was authored by an E-HOLDER (`propAfterE` at the
--                proposer, read at `commitCfg`) — the hypothesis under which the
--                coupling `require` makes every adopter of C a holder.
relation cfgSeen  (C : cfgid)
function cfgPred  (C : cfgid) : cfgid
function cfgQ     (C : cfgid) : quorum
relation cfgBacked (C : cfgid)

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
  cfgSeen C := false
  cfgBacked C := false
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
  -- GHOST (session 8): the config→proposer link. Both writes read `cfgOf i` and so
  -- must precede the move below.
  cfgSeen d := true
  cfgPred d := cfgOf i
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
  -- GHOST (session 8): freeze the certifying ADOPTER quorum and whether the proposal
  -- was authored by an E-holder. `propAfterE i` cannot have changed since the proposal:
  -- it is written only by `propose i`, which requires `¬ pending i`, and `pending i` has
  -- been true since that proposal.
  cfgQ (proposedC i) := q
  if propAfterE i then cfgBacked (proposedC i) := true
  if ¬ propAfterE i then cfgBacked (proposedC i) := false
  pending i := false
  hasProposal i := false
}

-- ---------- properties ----------

invariant [self_vote]
  (candidate I ∨ leader I) → (hasVoted I ∧ voteTerm I = curTerm I ∧ voteCand I = I)
invariant [eleccfg_not_ahead]
  elecCfg N = cfgOf N ∨ cfgLt (elecCfg N) (cfgOf N)
invariant [role_exclusive]
  ¬ (candidate I ∧ leader I)
invariant [cand_cfg_frozen]
  candidate I → elecCfg I = cfgOf I
invariant [reach_bound]                 -- a reached config was reached by now
  ¬ cfgLt (cfgOf N) C → tot.le (reachAt N C) (curTerm N)
invariant [reach_mono]                  -- earlier configs are reached no later
  (cfgLt C D ∧ ¬ cfgLt (cfgOf N) D) → tot.le (reachAt N C) (reachAt N D)
invariant [grant_reach_covered]
  (voteMsg V C T ∧ ¬ cfgLt (cfgOf V) D ∧ tlt (reachAt V D) T) → ¬ cfgLt (cfgOf C) D
invariant [eleccfg_covers_early_reach]
  ((candidate I ∨ leader I) ∧ ¬ cfgLt (cfgOf I) D ∧ tlt (reachAt I D) (curTerm I))
    → ¬ cfgLt (elecCfg I) D
invariant [cand_reach_strict]
  (candidate I ∧ ¬ cfgLt (cfgOf I) C) → tlt (reachAt I C) (curTerm I)
invariant [leader_reach_strict]
  (candidate I ∨ leader I) →
    (tlt (reachAt I (elecCfg I)) (curTerm I) ∧ tlt (reachAt I genesisC) (curTerm I))
#gen_spec

#check_action propose
