import Veil

/- UC v2.1 (M7) reconfiguration — COMMIT/LOG PLANE, session 1 of the option-(a) arc
   (scratch — NEVER the record; the record is `proofs/`).

   ===== SESSION-2 / FABLE-GATE-1 AMENDMENTS (2026-07-26) — READ FIRST =====
   (1) MODEL-EDIT-2 APPLIED (grant arm): a voter refuses a candidate whose adopted
       config is STRICTLY AHEAD of its own — `log_ok` extended from E to the CONFIG
       entries that share the log (election.rs:342-350 / :1240-1247 over `durable`;
       config frames live in `durable`, gate doc §5 Q2 link 1).
       ** DOCUMENTED NARROWING (gate-1 ruling, binding): this guard — like the
       pre-existing E-guard — is STRONGER than real `log_ok`, which WOULD grant to a
       candidate that lacks the voter's entries but carries a higher `last_term` on a
       divergent branch. Excluding those behaviours is deliberate: importing faithful
       `log_ok` into a one-tracked-entry plane makes the Figure-8 grant model-legal
       with nothing here to stop it, and restoring fidelity would mean merging in the
       whole Figure8/Finding9 plane — already banked separately. A second exclusion:
       `cfg` conflates HOLDING a config entry with having ADOPTED it (in Rust a
       candidate can be durable past a newer config frame — so `log_ok` grants —
       while its adopted config lags until the archive re-scan, election.rs:889-899).
       A third, (n3): the SMT twin's forward-only adoption + linear config history
       exclude the TRUNCATION-REVERT branch states (election.rs:703-748 moves the
       adopted config BACKWARD when truncation removes the frame behind it); UC's
       config history is linear only for the CANONICAL history — across branches two
       configs can share a `version`, which is why the forward gate at :751-756 is a
       version comparison, not a global order.
       ** CONSEQUENCE: any SAFE verdict from this plane is CONDITIONAL — on the
       canonical-prefix/contiguity discipline (Q2, CONFIRMED-SAFE in Rust) and on the
       data-plane freshness / Finding-#6b `new_term_pos` clamp (proved at the Lean
       tier). It is never an unconditional claim. **
   (2) TWIN <-> SMT DIVERGENCE LIST (complete, as of session 2 close). FOUR mechanisms
       live in the SMT proof model ONLY; this model has none of them, so it strictly
       OVER-approximates the proof model — more behaviours, the sound direction for a
       CE calibrator, and the reason a clean verdict here is weaker than one there:
         (d1) MODEL-EDIT-1, own-term-stamped commit reports. Omitted deliberately: a
              per-node acquisition-term function is a ~x27 state multiplier at Fin 3,
              past this box's explicit-state envelope.
         (d2) MODEL-EDIT-2b, linear config history (`cfglt_total`). Here the config
              order is the concrete strict-subset relation, which is only partial.
         (d3) MODEL-EDIT-2c, forward-only adoption (the version gate).
         (d4) MODEL-EDIT-3, cluster-wide one-in-flight (`config_pending`): this model
              keeps session 1's PER-NODE `pending i` only.
         (d5) MODEL-EDIT-4 (gate 1c, 2026-07-27), the proposer's OWN-TERM bound on the
              predecessor config's commit term (`cfgCommitTerm (cfgOf i) <= curTerm i`,
              abstracting `config_pending()` = `config_position > commit_seen`,
              election.rs:856-858/:879-880, where a LEADER's `commit_seen` has exactly
              two writers — the leader-excluded gossip intake :594-595 and `rank_leader`
              :1457 behind the :1451-1456 clamp). DELIBERATELY NOT MIRRORED here: this
              model has no `cfgCommitTerm` at all (no per-config commit-term function),
              and adding one is a state multiplier past this box's explicit-state
              envelope — the same reason (d1) was skipped. Consequence: the twin
              over-approximates the SMT model on this axis TOO, which is the sound
              direction for a CE calibrator. RE-VERIFIED after the edit
              (twin-runA2C2-gate1c.log): RUN A2 ❌ leader_completeness at DEPTH 13 and
              RUN C2 ❌ p2_antecedent_canary at DEPTH 10, both trace-for-trace as before.
   (3) `proposeAdd` is knob-gated OFF (addEnabled) for the re-runs: strict-subset is a
       valid config order only on a remove-only chain. Both calibration traces are
       remove-only, so this cannot destroy them.
   =========================================================================

   Brief: docs/superpowers/specs/2026-07-26-uc2-veil-reconfig-commit-plane-brief.md.
   Closes the modeling gap of Finding F-M7-2 (gate doc §4): `Reconfig.lean`'s `adopt`
   granted a config without requiring the committed prefix, so quorum-overlap /
   leader-completeness properties false-positived on data loss real UC cannot exhibit.
   The verified Rust ground truth is the discharged §5 Q2 mechanism chain (gate doc,
   CONFIRMED-SAFE, main @ e87b108); this model mirrors it link by link:

     link 1+2 (contiguity + reports → holding): the report plane is COLLAPSED —
              counting a node toward E's commit quorum requires `holdsE` BY
              CONSTRUCTION (`commitEntry`'s ∀-require). Sound to assume here because
              Q2 links 1-2 are CONFIRMED-SAFE shipped mechanisms, and the
              stale-report/divergent-tail class is banked in BootGate.lean /
              Finding9.lean. The knob below does NOT weaken this: weakening
              commit-counting would admit a cheap phantom commit (depth ~7) that BFS
              would return INSTEAD of the F-M7-2 config-walk CE — the session-4
              "Bar-2 passes on the wrong bug" trap.
     link 3   (commit bounded by leader-own-durable): `commitEntry` requires
              `holdsE i` — the leader holds what it commits (the spike's thrice-
              recurring trap, BootGate #4 / Figure8 #10).
     link 5   (elections membership-gated on the ADOPTED config; log_ok): quorums are
              majorities of the CANDIDATE'S OWN adopted `cfg`; the up-to-date grant
              restriction is UN-abstracted and keyed on LOG CONTENT (`holdsE`), not
              commit status (ReconfigLC's pre-commit-vote lesson).
     link 4   (below-floor / snapshot-carried config): NOT modeled — scope trim,
              recorded in the ledger. No purge/snapshot in this plane.

   THE KNOB (`prefixCoupling`) — the F-M7-2 mechanism itself, at the ADOPT site:
   config entries are LOG ENTRIES, so a node adopting a config entry that sits after
   E in the proposer's stream must already HOLD E (in Rust: config frames are detected
   only in the archive's recorded-block walk over the contiguous fsynced prefix — the
   bytes of E landed before the config frame was walked). `propAfterE` records at
   PROPOSE time whether E precedes the config entry in the proposer's log; checking
   `holdsE i` at adopt time instead would over-require when the leader appends E
   between propose and adopt — a narrowing in the dangerous (safety-claim) direction.

   CALIBRATION DESIGN (bar 1 of the brief):
     * coupling OFF, adjacency ON  → `#model_check` must exhibit the F-M7-2-shaped
       loss: a leader elected under a later config missing a committed entry, through
       a VALID adjacent chain (that validity is what made F-M7-2 a false positive of
       the old property — the plane must SEE the class it guards against).
       Hand-derived 13-step CE (re-derived against this model text before running;
       attempt 1's depth-11 CE rode a then-ungated commitCfg and was adjudicated a
       MODEL ARTIFACT — see fidelity notes at commitCfg/deliverRequestVoteGrant):
         1  startElection(0, t1)          8  adopt(j=2, i=0)          → cfg2={0,2}
         2  grant(j=1, c=0, t1)           9  commitCfg(0, q={0,2})
         3  becomeLeader(0, q={0,1})     10  proposeRemove(0, x=0)    → cfg0={2}
         4  appendEntry(0)               11  adopt(j=2, i=0)          → cfg2={2}
         5  replicate(0, 1)              12  startElection(2, t2)
         6  commitEntry(0, q={0,1})      13  becomeLeader(2, q={2})   → P2 VIOLATED
         7  proposeRemove(0, x=1)  → cfg0={0,2}, propAfterE 0
       (Note the two-hop chain is FORCED by the up-to-date restriction: under {0,2}
       node 0 holds E and refuses node 2, so the config must shrink past every
       E-holder — exactly the class single-server adjacency alone does not prevent.)
     * coupling ON → the same run must come back clean. BOUNDED ≠ PROOF: a clean
       bounded verdict here is calibration of the knob, not an assurance result —
       the inductive proof (`#check_invariants`, all-n) is the NEXT session's work.

   STATE-SPACE DIET vs ReconfigLC.lean (which stalled >700s at maxDepth 13):
     * vote tally COLLAPSED (BootGate obligation 2, adapted to concrete configs via a
       quorum-witness parameter `q : nodeSet` in becomeLeader/commitEntry): drops
       `votes : node → nodeSet` (8^3 = 512× state) and the deliverVote steps.
     * `elecQuorum` history variable dropped (it served quorum_overlap = P1, which
       the brief says NOT to chase; P2 needs no history variable).
     * `crashRestart` behind `crashEnabled` (OFF in bounded runs — it only clears
       volatile role state; `holdsE` is durable. The inductive path can turn it on
       symbolically at no explicit-state cost).

   P2 (the target property): LEADER COMPLETENESS ACROSS RECONFIG — an elected leader
   of any config, at any term ≥ the commit term, holds the committed entry. This is
   the repaired statement; the old `quorum_overlap` (P1) stays deliberately absent
   (expected false as stated — single-server change permits non-adjacent-config
   quorum disjointness; the brief forbids chasing it green). -/

veil module UcReconfigCommit

type node
type term
type nodeSet

instantiate tot : TotalOrderWithZero term
instantiate nset : TSet node nodeSet

immutable individual genesis : nodeSet
immutable individual adjacencyGuard : Bool
immutable individual prefixCoupling : Bool   -- THE knob (F-M7-2 mechanism)
immutable individual crashEnabled : Bool
immutable individual vacuityCanary : Bool
immutable individual p2On : Bool
-- GATE-1 twin restriction: proposeAdd is knob-gated OFF for the EDIT-2 re-runs so the
-- reachable config space is REMOVE-ONLY from a full genesis — the precondition that
-- makes strict-subset a valid "further along the config chain" order here. Both the
-- run-A CE and the run-C canary witness are remove-only traces, so the restriction
-- cannot destroy either. (The SMT twin uses an abstract chain order instead.)
immutable individual addEnabled : Bool

-- election plane (tally collapsed — no votes set, no deliverVote)
relation candidate (N : node)
relation leader    (N : node)
function curTerm   (N : node) : term
relation hasVoted  (N : node)
function voteTerm  (N : node) : term
function voteCand  (N : node) : node
relation reqVote   (C : node) (T : term)
relation voteMsg   (V : node) (C : node) (T : term)

-- config plane
function cfg        (N : node) : nodeSet   -- N's ADOPTED voter set
relation pending    (I : node)             -- I has an uncommitted change in flight
relation hasProposal(I : node)             -- I published a config entry to adopt
function proposed   (I : node) : nodeSet   -- the config I is proposing
-- Recorded at PROPOSE time: does the config entry sit AFTER E in I's log?
-- (i.e. did I hold E when it appended the config entry). The coupling keys on this,
-- not on holdsE-at-adopt-time — see header.
relation propAfterE (I : node)
-- J is durable past I's CURRENT config entry (received + walked the frame): the
-- adoption evidence a config commit counts. Cleared per-proposer at each propose
-- (a fresh entry needs fresh acks); the proposer self-adopts at append.
relation hasAdopted (J : node) (I : node)

-- commit/log plane: one tracked entry E, entry-level (LC-arc style, NOT bytes)
relation holdsE (N : node)                 -- N's log contains E
individual committed     : Bool            -- E is committed
individual committedTerm : term            -- the term E was committed in

#gen_state

theory ghost relation tlt (x y : term) := tot.le x y ∧ x ≠ y
theory ghost relation majorityOf (q c : nodeSet) := nset.count q * 2 > nset.count c
theory ghost relation adjacent (a b : nodeSet) :=
  nset.count (nset.diff a b) + nset.count (nset.diff b a) ≤ 1
-- MODEL-EDIT-2 (gate-1 approved, twin form): `a` is STRICTLY AHEAD of `b` in the
-- config chain. Remove-only chain (addEnabled := false) ⇒ later = strictly fewer
-- voters ⇒ strict subset. Abstract-model counterpart: `cfgLt (cfgOf c) (cfgOf j)`.
theory ghost relation strictlyAhead (a b : nodeSet) :=
  nset.count (nset.diff a b) = 0 ∧ nset.count (nset.diff b a) > 0

after_init {
  candidate N := false
  leader N := false
  curTerm N := tot.zero
  hasVoted N := false
  voteTerm N := tot.zero
  voteCand N := N
  reqVote C T := false
  voteMsg V C T := false
  cfg N := genesis
  pending N := false
  hasProposal N := false
  proposed N := genesis
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

-- grant arm, with the UN-abstracted up-to-date restriction: a voter whose LOG
-- contains E refuses a candidate lacking E. Keyed on log content (`holdsE`), NOT on
-- `committed` — a vote cast pre-commit must not strand a soon-to-commit entry
-- (ReconfigLC's model-bug lesson, session 3).
action deliverRequestVoteGrant (j : node) (c : node) (t : term) {
  require reqVote c t
  require tot.le (curTerm j) t
  require ¬ (hasVoted j ∧ voteTerm j = t ∧ voteCand j ≠ c)
  require ¬ (holdsE j ∧ ¬ holdsE c)
  -- MODEL-EDIT-2 (gate-1 approved): log_ok covers CONFIG entries too — a voter whose
  -- adopted config is strictly ahead of the candidate's refuses the grant. Same
  -- abstraction of `log_ok` the line above already makes for E, extended to the other
  -- log content this plane tracks. Rust: election.rs:342-350 / :1240-1247 over
  -- `durable`, which contains config frames (gate doc §5 Q2 link 1).
  require ¬ strictlyAhead (cfg j) (cfg c)
  -- FIDELITY (Q2 link 5, this session's gap 2): vote GRANTING is membership-gated
  -- on the voter's ADOPTED config — j refuses a candidate outside cfg j. Without
  -- this, even the COUPLED model loses E: a pre-E config walk shrinks the voter
  -- set, E commits under the shrunken config, and a stale-config candidate then
  -- assembles an old-config quorum from voters who have long since moved on —
  -- a grant real UC refuses (membership-gated solicitation/granting, M7 design).
  require nset.contains c (cfg j)
  if tlt (curTerm j) t then candidate j := false
  if tlt (curTerm j) t then leader j := false
  curTerm j := t
  hasVoted j := true
  voteTerm j := t
  voteCand j := c
  voteMsg j c t := true
}

-- becomeLeader, tally collapsed: candidate i wins with a quorum-witness q whose
-- members all granted at i's current term (or are i itself), where q restricted to
-- i's OWN ADOPTED config is a majority of that config. Votes from non-members of
-- cfg i contribute nothing (F-M7-1 fix (a) preserved).
action becomeLeader (i : node) (q : nodeSet) {
  require candidate i
  require majorityOf (nset.intersection q (cfg i)) (cfg i)
  require ∀ V, nset.contains V q → (V = i ∨ voteMsg V i (curTerm i))
  leader i := true
  candidate i := false
}

-- volatile role lost; vote, term, LOG (holdsE) and adopted config are durable.
action crashRestart (i : node) {
  require crashEnabled
  candidate i := false
  leader i := false
}

-- ---------- commit/log plane ----------

-- Leader authors E in its own term. No `i ∈ cfg i` require: a self-removed leader
-- keeps serving (and appending) until the removing config commits — Q1's confirmed
-- deliberate window (election.rs:1307-1310).
action appendEntry (i : node) {
  require leader i
  require ¬ committed
  holdsE i := true
}

-- Term-stamped replication frame: j hears from leader i (revert + term adoption),
-- and E lands in j's log.
action replicate (i : node) (j : node) {
  require leader i
  require holdsE i
  require tot.le (curTerm j) (curTerm i)
  candidate j := false
  leader j := false
  curTerm j := curTerm i
  holdsE j := true
}

-- E commits: a quorum-witness q of E-HOLDERS that is a majority of the leader's
-- CURRENT config — "quorum-durable under the config in force at commit time"
-- (adoption-at-append means cfg i IS the config in force; Q1 confirmed the round
-- quorum is re-derived over C_new). `holdsE i`: the leader holds what it commits
-- (link 3; the spike's recurring trap). `∀ V ∈ q → holdsE V`: counting toward the
-- quorum implies holding the prefix (links 1+2, by construction — see header).
action commitEntry (i : node) (q : nodeSet) {
  require leader i
  require ¬ committed
  require holdsE i
  require majorityOf (nset.intersection q (cfg i)) (cfg i)
  require ∀ V, nset.contains V q → holdsE V
  committed := true
  committedTerm := curTerm i
}

-- ---------- config plane (single-server change, one in flight) ----------

-- Leader proposes removing voter x; leader adopts at append. `propAfterE` records
-- whether the config entry sits after E in i's log.
action proposeRemove (i : node) (x : node) {
  require leader i
  require ¬ pending i
  require nset.contains x (cfg i)
  require nset.count (cfg i) > 1
  cfg i := nset.remove x (cfg i)
  proposed i := nset.remove x (cfg i)
  hasProposal i := true
  pending i := true
  if holdsE i then propAfterE i := true
  if ¬ holdsE i then propAfterE i := false
  hasAdopted J i := false          -- fresh entry, fresh acks (clear i's column)...
  hasAdopted i i := true           -- ...then the proposer self-adopts at append
}

action proposeAdd (i : node) (x : node) {
  require addEnabled
  require leader i
  require ¬ pending i
  require ¬ nset.contains x (cfg i)
  cfg i := nset.insert x (cfg i)
  proposed i := nset.insert x (cfg i)
  hasProposal i := true
  pending i := true
  if holdsE i then propAfterE i := true
  if ¬ holdsE i then propAfterE i := false
  hasAdopted J i := false
  hasAdopted i i := true
}

-- Follower j adopts leader i's published config entry.
--   * term-coupled (F-M7-1 fix (b)): the entry rides a term-stamped frame, so j
--     reverts and adopts i's term; a stale-leader frame is refused.
--   * adjacencyGuard: single-server-adjacent adoption (the M7 rule).
--   * prefixCoupling — THE F-M7-2 MECHANISM: if the config entry sits after E in
--     the proposer's stream (`propAfterE i`), j must already HOLD E to have walked
--     to the config frame (Q2 links 1+2). OFF = the old Reconfig.lean adopt, which
--     is exactly what false-positived.
action adopt (j : node) (i : node) {
  require j ≠ i                              -- leader adopted at append already
  require hasProposal i
  require tot.le (curTerm j) (curTerm i)
  require (¬ adjacencyGuard) ∨ adjacent (proposed i) (cfg j)
  require (¬ prefixCoupling) ∨ (¬ propAfterE i) ∨ holdsE j
  candidate j := false
  leader j := false
  curTerm j := curTerm i
  cfg j := proposed i
  hasAdopted j i := true
}

-- The in-flight change commits: a quorum-witness q of ADOPTERS (durable past the
-- config entry) that is a majority of C_new — the config entry commits like any
-- entry, under the config in force (adoption-at-append means cfg i = C_new; Q1:
-- a removed leader's self-ack is a NON-VOTER seed, which `∩ cfg i` discards).
-- FIDELITY (this session's gap 1, found by the checker): attempt 1 left this
-- UNGUARDED as a "sound superset" — wrong for the calibration architecture. The
-- checker promptly rode it to a depth-11 P2 CE (config chain to {0} with ZERO
-- adopters, then a solo commit invisible to a legitimately-elected t2 leader) —
-- unreachable in Rust, where ChangePending clears only when the entry commits
-- under a C_new quorum, and present in BOTH knob positions, so it would have
-- masked the class the knob exists to isolate.
action commitCfg (i : node) (q : nodeSet) {
  require pending i
  require majorityOf (nset.intersection q (cfg i)) (cfg i)
  require ∀ V, nset.contains V q → hasAdopted V i
  pending i := false
  hasProposal i := false
}

-- ---------- safety ----------

safety [election_safety]
  (leader N1 ∧ leader N2 ∧ curTerm N1 = curTerm N2) → N1 = N2

-- P2 — LEADER COMPLETENESS ACROSS RECONFIGURATION (the target): any current leader
-- at a term ≥ E's commit term holds E. Knob-gated so the election-safety regression
-- run can be read in the uncoupled model without BFS stopping at the expected CE.
safety [leader_completeness]
  p2On → ((committed ∧ leader L ∧ tot.le committedTerm (curTerm L)) → holdsE L)

-- NON-VACUITY canary for the coupling-ON clean run (the session-5b rule: a clean
-- verdict over a model whose interesting antecedent is unreachable is worthless).
-- A REPORTED VIOLATION is the good outcome: its trace witnesses P2's antecedent
-- realized non-trivially WITH the coupling on — a commit, then a LATER-term leader
-- under a CHANGED config (count < |genesis|: at n=3 any change from full is a
-- shrink through the interesting direction).
safety [p2_antecedent_canary]
  vacuityCanary →
    ¬ (committed ∧ leader L ∧ tlt committedTerm (curTerm L) ∧
       nset.count (cfg L) < nset.count genesis)

#gen_spec

open Std

-- Reading verdicts (session-4 lessons): `✅ No violation` renders identically for
-- exhaustion and a depth bound — a SAFE verdict is exhaustive ONLY if no maxDepth
-- was passed. And an elaboration error silently VOIDS a run into a vacuous pass:
-- always confirm zero `error: Examples/...` lines before believing any verdict.

-- RUN A — BAR-1 CALIBRATION: PASSED (attempt 2, 2026-07-26, ~26 min) — ❌
-- leader_completeness at DEPTH 13, isomorphic to the hand-derived trace (the
-- checker's relabeling: E held by {0,2}, walk {0,1,2}→{0,1}→{1} adopted by
-- non-holder 1, self-election at t2). Steps 8/11 are the knobbed move; all others
-- real-UC-legal. Log: proofs-veil/logs/reconfigcommit-runA-calibrationCE-depth13.log.
-- RE-RUN A2 (gate-1 cross-check, EDIT-2 applied, EDIT-1 deliberately NOT — see the
-- header divergence note): the calibration CE MUST survive both guards, else the
-- plane's eyesight is broken and EDIT-2 must be reverted.
#model_check { node := Fin 3, term := Fin 3, nodeSet := ExtTreeSet (Fin 3) compare }
  { genesis := Std.ExtTreeSet.empty.insertMany (List.finRange 3),
    adjacencyGuard := true, prefixCoupling := false, addEnabled := false,
    crashEnabled := false, vacuityCanary := false, p2On := true }
  (maxDepth := 14)

-- RUN B — coupling ON, same regime, maxDepth 13 = THE CALIBRATION HORIZON (the
-- depth at which the uncoupled model exhibits the loss). A maxDepth-15 attempt was
-- ABANDONED at ~50 min: lean at 6.5 GB RSS with 3 GB available and falling on the
-- no-swap box — killed before OOM, yielding NO verdict (Lean buffers until
-- elaboration ends; log reconfigcommit-runB-d15-abandoned.log). Expected clean.
-- BOUNDED ≠ PROOF: this is CALIBRATION of the knob, not an assurance result —
-- induction is next session's work.
-- RESULT (2026-07-26): ✅ No violation, 4,211,943 states (maxDepth 13 — bounded).
-- (B+C+D ran in ONE ~80-min detached build after two ~60-min attempts were killed
-- by the harness background-task ceiling — see the ledger's toolchain note.)
/- RUN-B-BANKED (session 6: ✅ 4,211,943 states, maxDepth 13)
#model_check { node := Fin 3, term := Fin 3, nodeSet := ExtTreeSet (Fin 3) compare }
  { genesis := Std.ExtTreeSet.empty.insertMany (List.finRange 3),
    adjacencyGuard := true, prefixCoupling := true, addEnabled := true,
    crashEnabled := false, vacuityCanary := false, p2On := true }
  (maxDepth := 13)
RUN-B-BANKED-END -/

-- RUN C — non-vacuity for RUN B: a reported canary violation is the GOOD outcome
-- (witness that P2's antecedent is realizable with the coupling on).
-- RESULT (2026-07-26): ❌ p2_antecedent_canary at depth 10 — witnessed. NOTE the
-- witness shape: a STALE t1 leader commits E with a holder whose `holdsE` came from
-- its own t2 append — single-tracked-entry stream conflation (no per-term entry
-- identity; real UC's handle-term-stamped reports reject this). An OVER-approximation
-- (sound direction for the safety verdicts), ledgered; the SMT session may need an
-- `entryTerm` if this class produces nuisance CTIs.
-- RE-RUN C2 (gate-1 cross-check): the canary MUST still witness non-vacuity with
-- EDIT-2 applied. If it stops witnessing altogether that is a non-vacuity FAILURE.
#model_check { node := Fin 3, term := Fin 3, nodeSet := ExtTreeSet (Fin 3) compare }
  { genesis := Std.ExtTreeSet.empty.insertMany (List.finRange 3),
    adjacencyGuard := true, prefixCoupling := true, addEnabled := false,
    crashEnabled := false, vacuityCanary := true, p2On := true }

-- RUN D — election-safety regression in the UNCOUPLED model (p2 gated off so BFS
-- does not stop at RUN A's expected CE). Bounded to RUN A's horizon.
-- RESULT (2026-07-26): ✅ No violation, 9,160,143 states (maxDepth 14 — bounded).
/- RUN-D-BANKED (session 6: ✅ 9,160,143 states, maxDepth 14)
#model_check { node := Fin 3, term := Fin 3, nodeSet := ExtTreeSet (Fin 3) compare }
  { genesis := Std.ExtTreeSet.empty.insertMany (List.finRange 3),
    adjacencyGuard := true, prefixCoupling := false, addEnabled := true,
    crashEnabled := false, vacuityCanary := false, p2On := false }
  (maxDepth := 14)
RUN-D-BANKED-END -/

