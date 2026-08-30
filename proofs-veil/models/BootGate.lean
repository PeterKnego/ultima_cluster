import Veil

/- UC v2 — BOOT-GATE / COMMIT-CERTIFICATION plane (spike Bar-2 + Bar-2b, scratch —
   NEVER the record; the record is `proofs/`).

   TARGET: Finding #5, the SHALLOWEST of the four known election-time coherence-window
   bugs, and therefore the brief's true tool-fitness gate (Bar 2). The shipped bug:

     `uc_node` booted the receiver intake gate OPEN (`AtomicBool::new(true)`) with
     `awaiting_reconcile: false`, while `ElectionSm::new` recovers
     `current_term = vote_term.max(map_term)` (election.rs:400-402) and votes persist
     before send. A voter that GRANTED term T, held a divergent tail, and crashed
     before reconciling rebooted AT term T with the gate open; its 20ms AppendPosition
     floor re-send (receiver.rs:1052-1078) fed the T-leader's positions-only
     `CommitTracker` a same-term durable report — certifying a commit backed by content
     the reporter did not actually hold. FIX (node.rs:533-534): the boot gate closes
     iff `vote_term > map_term`.

   KNOB `bootGateFix` — false = the pre-fix model (Bar 2: the checker MUST rediscover
   the phantom commit), true = the shipped fix (calibration: expect SAFE).

   ---- Bar-2b, the abstraction obligation, is discharged BY CONSTRUCTION here ----
   The frame abstraction keeps two INDEPENDENT relations at the one tracked position P:
     * `durableTo N`  — N's durable byte position covers P.  BYTES LANDED.
     * `holdsEntry N` — the bytes N holds at P are the CURRENT leader's entry E.  WHOSE
                        STREAM those bytes came from.
   A divergent tail replicated from an EARLIER leader's stream sets `durableTo` without
   `holdsEntry` — a stale-handle-term stream byte and a current-term stream byte at the
   SAME position, distinguished. This is exactly the #9 distinction the brief requires
   the abstraction to preserve; `staleStreamAppend` vs `replicate` are the two arms.
   AppendPosition reports `durableTo` and NEVER attests `holdsEntry` — which is the
   whole bug class, and is why erasing the distinction would blind the V2 hunt.

   ---- Abstraction obligations taken (recorded, per the brief's discipline) ----
   1. Grant guard (logOk / lastTerm / lastDurable) left NONDETERMINISTIC, as in
      ElectionMC.lean's election port: SOUND for a bug hunt (admits ≥ the real
      behaviours). Finding #5 is a commit-path bug; the real voter did grant.
   2. Vote tally collapsed — `becomeLeader` reads `voteMsg` directly instead of a
      separate `counted` relation + `deliverVote` step. Sound (the tally is pure
      message counting) and a large state-space saving.
   3. One tracked entry E at one tracked position P (bounded, as ReconfigLC.lean).
   4. Quorums concrete via the excluded-node encoding (`member n q := n ≠ q` =
      the (n-1)-of-n majority excluding q; at n=3 exactly 2-of-3). Deliberately NOT
      the TSet encoding — that is what hit the SMT `List` universe wall in
      ReconfigLC.lean. -/

veil module UcBootGate

type node
type term

instantiate tot : TotalOrderWithZero term

immutable individual bootGateFix : Bool
-- NON-VACUITY canary (see the safety section): false in a real verdict run, true to
-- ask the checker for a witness that a GENUINE commit is reachable at all.
immutable individual vacuityCanary : Bool
-- Bar-2b canary: true asks the checker to witness the #9 distinction being live.
immutable individual bar2bCanary : Bool

-- ---------- election plane (ElectionMC.lean, tally collapsed) ----------
relation candidate (N : node)
relation leader    (N : node)
function curTerm   (N : node) : term
relation hasVoted  (N : node)
function voteTerm  (N : node) : term
function voteCand  (N : node) : node
relation reqVote   (C : node) (T : term)
relation voteMsg   (V : node) (C : node) (T : term)

-- ---------- receiver term handle (the LAGGING handle — cf. the #9 class) ----------
-- The term the RECEIVER stamps on NAK/STATUS/AppendPosition (`term_handle`). It is
-- NOT the consensus current term: `term_handle.store` has exactly two call sites,
-- `Action::BecomeLeader` (node.rs:2478) and `Action::BecomeFollower` (node.rs:2506).
-- Starting one's OWN election has no such action, so a candidate keeps stamping the
-- OLD term and a same-term leader rejects its reports as stale. Modeling reports with
-- `curTerm` instead admits a bogus phantom commit via an independent candidate that
-- never heard from the leader. At boot the handle is seeded with `boot_term`
-- (node.rs:513) = the recovered max(vote_term, map_term) — which is precisely what
-- lets a rebooted unreconciled voter stamp a SAME-TERM report. Finding #5's mechanism.
function handleTerm (N : node) : term

-- ---------- durable term map ----------
-- The leader ships its term map; a node's recovered term on boot is
-- max(voteTerm, mapTerm) (ElectionSm::new, election.rs:400-402). vote_term > map_term
-- is precisely "granted a vote I never reconciled against" — the fix's predicate.
function mapTerm (N : node) : term

-- ---------- data plane at the tracked position P (see Bar-2b note above) ----------
relation durableTo  (N : node)   -- BYTES at P landed (what AppendPosition reports)
relation holdsEntry (N : node)   -- those bytes are the CURRENT leader's entry E

-- ---------- receiver intake gate ----------
-- Gate closed => DATA dropped AND AppendPosition emission suppressed entirely
-- (uc_net/src/receiver.rs::set_intake_gate).
-- `awaiting_reconcile` is NOT modeled as separate state: in UC it only gates the
-- reopen path (node.rs ~2766), and `gateOpen` already carries every behaviour that
-- path produces here. Dropping it is a pure state-space saving, not a fidelity loss.
relation gateOpen (N : node)

-- ---------- reports + commit ----------
relation report    (V : node) (T : term)  -- AppendPosition: durable-to-P stamped at T
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
  mapTerm N := tot.zero
  handleTerm N := tot.zero
  durableTo N := false
  holdsEntry N := false
  -- fresh boot at term 0: voteTerm = mapTerm = 0, so there is no unreconciled vote
  -- and the gate is open under BOTH policies.
  gateOpen N := true
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

-- grant arm: voter j adopts t if higher, grants unless already committed at t to a
-- different candidate. logOk abstracted (obligation 1).
action deliverRequestVoteGrant (j : node) (c : node) (t : term) {
  require reqVote c t
  require tot.le (curTerm j) t
  require ¬ (hasVoted j ∧ voteTerm j = t ∧ voteCand j ≠ c)
  if tlt (curTerm j) t then candidate j := false
  if tlt (curTerm j) t then leader j := false
  -- Adopting a strictly NEW term CLOSES the intake gate and arms reconciliation
  -- (`Action::BecomeFollower`, node.rs:2511-2513; test node.rs:3456 drives exactly
  -- this via a higher-term RequestVote). Without this the model admits a shallower
  -- phantom commit that is NOT Finding #5 — an unreconciled divergent follower
  -- reporting at the new term without ever crashing — and the post-fix calibration
  -- run would be spuriously UNSAFE. This guard is what makes the boot gate the
  -- ONLY remaining way to report an unreconciled divergent tail.
  if tlt (curTerm j) t then gateOpen j := false
  -- BecomeFollower also advances the receiver's stamping handle (node.rs:2506).
  if tlt (curTerm j) t then handleTerm j := t
  curTerm j := t
  hasVoted j := true
  voteTerm j := t
  voteCand j := c
  voteMsg j c t := true
}

-- becomeLeader: candidate i wins if every member of quorum q self-is-i or granted at
-- i's current term (tally collapsed, obligation 2).
action becomeLeader (i : node) (q : node) {
  require candidate i
  require member i q
  require ∀ V, member V q → (V = i ∨ voteMsg V i (curTerm i))
  leader i := true
  candidate i := false
  handleTerm i := curTerm i        -- BecomeLeader stores the handle (node.rs:2478)
}

-- ---------- data plane ----------

-- leader i authors E at P in its own term.
action appendEntry (i : node) {
  require leader i
  require ¬ committed
  holdsEntry i := true
  durableTo i := true
}

-- CURRENT-stream replication: term-stamped frame from leader i lands E at P on j.
-- Cannot overwrite a divergent tail — j must reconcile (truncate) first.
action replicate (i : node) (j : node) {
  require leader i
  require holdsEntry i
  require gateOpen j
  -- DATA lands only from the current-term leader onto a node that has already
  -- adopted AND reconciled that term. A node still behind the leader's term has
  -- not adopted it, so its gate is shut and the frame is dropped — adoption goes
  -- through `reconcile`, never through the data path.
  require curTerm j = curTerm i
  require ¬ (durableTo j ∧ ¬ holdsEntry j)
  candidate j := false
  leader j := false
  holdsEntry j := true
  durableTo j := true
}

-- STALE-stream append (the Bar-2b other arm): bytes from an EARLIER leader's stream
-- cover P on j. Position covered, content is NOT the current leader's entry. Only
-- possible while j's intake gate is open.
action staleStreamAppend (j : node) {
  require gateOpen j
  require ¬ holdsEntry j
  -- j can only take bytes that are NOT the current leader's while it is BEHIND that
  -- leader's term. DATA is filtered at `adopted_term == term_handle`
  -- (receiver.rs:635 `dropped_stale_term`; node.rs:2404-2418), so a node synced to
  -- the sitting leader receives that leader's stream and nothing else — foreign bytes
  -- at P cannot materialise on it. Without this the checker (correctly) exhibits a
  -- node reconciling at term 1 and THEN sprouting non-leader bytes at P.
  --
  -- ABSTRACTION OBLIGATION (narrowing — recorded honestly): this also rules out the
  -- ordering where a node takes a divergent tail FROM the sitting older-term leader
  -- (L0's uncommitted tail, later overwritten by a term-1 leader). With one tracked
  -- entry and one tracked position that class is still represented — by taking the
  -- stale bytes while no leader sits, which is what the Finding-#5 CE does — but the
  -- post-fix SAFE verdict is therefore "safe within this restriction", NOT unqualified.
  require ∀ L, leader L → tlt (handleTerm j) (curTerm L)
  durableTo j := true
}

-- reconcile: j receives leader i's term map, adopts the term, TRUNCATES a divergent
-- tail, and the gate reopens (node.rs ~2427/2495/2615, 2773). A clean tail survives.
--
-- There is deliberately NO standalone "ship the term map" action that advances
-- `mapTerm` on its own. Receiving the term map IS the reconciliation trigger, so a
-- node cannot end up with an advanced `mapTerm` while still holding an unreconciled
-- divergent tail. Modeling it separately would let `mapTerm` catch up to `voteTerm`
-- with the tail intact, which silently BYPASSES the fix's `vote_term > map_term`
-- predicate and would make the post-fix run spuriously UNSAFE.
action reconcile (j : node) (i : node) {
  require leader i
  require tot.le (curTerm j) (curTerm i)
  if (durableTo j ∧ ¬ holdsEntry j) then durableTo j := false
  curTerm j := curTerm i
  mapTerm j := curTerm i
  handleTerm j := curTerm i
  gateOpen j := true
}

-- AppendPosition: report the durable POSITION, stamped with the RECEIVER'S HANDLE
-- term (not the consensus term — see `handleTerm`). Reports BYTES; attests NOTHING
-- about content. Suppressed entirely while the intake gate is closed.
action sendReport (j : node) {
  require gateOpen j
  require durableTo j
  report j (handleTerm j) := true
}

-- ---------- crash / boot: where the knob lives ----------

-- Volatile role is lost. The VOTE, the TERM MAP and the ON-DISK DIVERGENT TAIL are
-- durable and survive — that is the whole hazard. Recovered term = max(vote, map).
action crashRestart (j : node) {
  candidate j := false
  leader j := false
  -- ElectionSm::new recovers current_term = max(vote_term, map_term)
  -- (election.rs:400-402); the receiver handle is seeded with the same boot_term
  -- (node.rs:513), which is what makes the rebooted voter's report SAME-TERM.
  if tlt (mapTerm j) (voteTerm j) then curTerm j := voteTerm j
  if tlt (mapTerm j) (voteTerm j) then handleTerm j := voteTerm j
  if tot.le (voteTerm j) (mapTerm j) then curTerm j := mapTerm j
  if tot.le (voteTerm j) (mapTerm j) then handleTerm j := mapTerm j
  -- PRE-FIX: gate boots OPEN unconditionally (AtomicBool::new(true)).
  -- FIXED  : closed iff vote_term > map_term (node.rs:533-534) — i.e. exactly when
  -- this node granted a vote it never reconciled against.
  gateOpen j := true
  if (bootGateFix ∧ tlt (mapTerm j) (voteTerm j)) then gateOpen j := false
}

-- ---------- commit ----------

-- The leader's CommitTracker is POSITIONS-ONLY: it advances on a quorum of same-term
-- durable reports and trusts the term stamp, never the content behind it.
action commitEntry (i : node) (q : node) {
  require leader i
  require ¬ committed
  require member i q
  -- The leader must HOLD the entry it commits, not merely have bytes at P. Requiring
  -- only `durableTo i` let a leader commit an entry it never authored, off nothing
  -- but its own stale tail — `holdsEntry` is empty in that trace. (ReconfigLC.lean
  -- had this right as `nset.contains i entryHolders`; weakening it was my error.)
  require holdsEntry i
  require ∀ V, member V q → (V = i ∨ report V (curTerm i))
  committed := true
}

-- ---------- safety ----------

-- NO PHANTOM COMMIT: once E is committed, some genuine quorum really holds it. This
-- is the gate doc's own phrasing of Finding #5 — "phantom commit: no genuine quorum
-- backing". Stated over `holdsEntry` alone rather than over a recorded certifying
-- set, which needs no extra state: relation assignments in Veil take Bool, so a
-- pointwise `certified V := V ≠ q` does not elaborate. `holdsEntry` is monotone (only
-- `reconcile` clears bytes, and it clears `durableTo`, never `holdsEntry`), so the
-- property is stable after the commit moment rather than merely true at it.
safety [no_phantom_commit]
  committed → ∃ (q : node), ∀ V, member V q → holdsEntry V

-- NON-VACUITY canary. A SAFE verdict is worthless if the model can never commit at
-- all, so we need a witness that a GENUINE commit (every certifier really holds E)
-- is reachable. Encoded as a knob-gated safety rather than a `sat trace`: the
-- explicit-state engine already enumerates the space, whereas `sat trace` at the 7
-- actions this needs blows up `simp` (maximum steps exceeded). With
-- `vacuityCanary := false` this is vacuously true and costs nothing; with `true`,
-- a REPORTED VIOLATION is the good news — its trace is the genuine-commit witness.
safety [genuine_commit_canary]
  vacuityCanary → ¬ (committed ∧ ∃ (q : node), ∀ V, member V q → holdsEntry V)

-- BAR-2b — the abstraction obligation, as a directed reachability question.
-- The brief requires showing the frame abstraction still distinguishes a
-- stale-handle-term stream byte from a current-term stream byte AT THE SAME
-- POSITION. Here that is literally: can two nodes both have bytes at P, one from
-- an earlier leader's stream (¬holdsEntry) and one from the current leader's
-- (holdsEntry)? A REPORTED VIOLATION means yes — the distinction is live and the
-- V2 window hunt is not blind to its target class. No violation would mean the
-- abstraction collapsed the two, and Bar-2b FAILS.
safety [bar2b_stream_distinction]
  bar2bCanary → ¬ (durableTo V1 ∧ ¬ holdsEntry V1 ∧ durableTo V2 ∧ holdsEntry V2)

#gen_spec

-- NOTE on reading every verdict below: `✅ No violation` does NOT distinguish full
-- exhaustion from a depth bound — Veil/Core/UI/Trace/TraceDisplay.lean:104 renders
-- `exploredAllReachableStates` and `reachedDepthBound` identically. A SAFE verdict is
-- therefore only exhaustive when no `maxDepth` was passed, as in all four runs here.

-- RUN 1 — BAR 2 (must-pass). Pre-fix: the checker MUST rediscover the phantom commit.
#model_check { node := Fin 3, term := Fin 2 }
  { bootGateFix := false, vacuityCanary := false, bar2bCanary := false }

-- RUN 2 — Bar-2 calibration. Post-fix: the CE must be GONE. Discriminates the fix.
#model_check { node := Fin 3, term := Fin 2 }
  { bootGateFix := true, vacuityCanary := false, bar2bCanary := false }

-- RUN 3 — non-vacuity for RUN 2. A reported violation is the GOOD outcome: it is the
-- witness that a genuine, correctly-backed commit is still reachable post-fix, so
-- RUN 2's SAFE is not "nothing ever commits".
#model_check { node := Fin 3, term := Fin 2 }
  { bootGateFix := true, vacuityCanary := true, bar2bCanary := false }

-- RUN 4 — BAR 2b (must-pass). A reported violation is the GOOD outcome (see above).
#model_check { node := Fin 3, term := Fin 2 }
  { bootGateFix := true, vacuityCanary := false, bar2bCanary := true }
