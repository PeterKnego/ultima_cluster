import Veil

/- UC v2 — FINDING #9 DEPTH PROBE (spike V2 stretch item, scratch — NEVER the record).

   NOT A GATE. The brief lists this as an explicit stretch: "how deep a bound is
   needed before the checker rediscovers #6b's 3-term Figure-8 / #9's cross-stream
   accept — this calibrates the forward hunt's confidence and tells you whether
   'absence at depth N' means anything". Coming up empty here does NOT license a DROP.

   TARGET (node.rs:2404-2423, the shipped guard `sm.current_term() == adopted_term`):
     `Action::StartElection` bumps `current_term` but stores NO term handle
     (node.rs:2440-2450), so a CANDIDATE runs its data plane at a LAGGING handle.
     Pre-fix, a candidate that cleanly reconciles a HIGHER-term leader's map
     (non-adopt: `t` not > current_term) REOPENS intake for its stale handle-term
     stream, and then accepts a cross-stream `serveTail` byte ITS MAP NEVER
     ATTRIBUTED — acked-write-loss, Raft §5.4.2 / the #6b family.

   KNOB `finding9Fix`: false = pre-fix (reopen unconditional), true = shipped guard.

   ---- THE KEY NEW STATE: `tailAttributed` ----
   #9's damage is invisible to reconciliation. A byte accepted cross-stream is one
   "its map never attributed", so a LATER clean reconcile cannot detect it and does
   NOT truncate it — the node then reports a durable position over content it does
   not hold. Modeled as `tailAttributed`: `staleStreamAppend` (an ordinary divergent
   tail, taken under a term whose map DID attribute it) sets it TRUE and is therefore
   truncated on reconcile; `crossStreamAccept` sets it FALSE and SURVIVES reconcile.
   Without this distinction the model cannot express #9's loss at all.

   ---- TWO PROPERTIES, deliberately at different depths ----
   * `no_cross_stream_reopen` — PROXY, shallow. The invariant the shipped guard
     actually establishes: intake is never open while the handle lags the map.
   * `no_phantom_commit` — the FULL acked-write-loss, and much deeper (it needs the
     candidate to revert, re-reconcile clean, and have its bad byte certify a commit).
   Reporting both is the point: if only the proxy is reachable inside the budget,
   that IS the calibration answer, honestly stated.

   ---- Scope trims (recorded) ----
   * `crashRestart` is DROPPED — that is Finding #5's mechanism, already banked in
     BootGate.lean. Its absence keeps any CE found here in the #9 class and shrinks
     a state space that needs term = Fin 3 (three distinct terms are structural here:
     adopt T1, candidate at T2, leader at T2).
   * The `staleStreamAppend` guard is RELAXED vs BootGate.lean to
     `∀ L, leader L → handleTerm j ≠ curTerm L` ("a node synced to a sitting leader
     receives that leader's stream and nothing else"). Strictly more permissive than
     BootGate's version, so better for a bug hunt, and it still blocks the artifact
     BootGate needed it for. -/

veil module UcFinding9

type node
type term

instantiate tot : TotalOrderWithZero term

immutable individual finding9Fix : Bool
-- The proxy property is knob-gated so it can be switched OFF: BFS stops at the FIRST
-- violation, and the proxy sits at depth 7, so leaving it on would mask the deeper
-- `no_phantom_commit` hunt entirely.
immutable individual proxyOn : Bool

-- election plane
relation candidate (N : node)
relation leader    (N : node)
function curTerm   (N : node) : term
relation hasVoted  (N : node)
function voteTerm  (N : node) : term
function voteCand  (N : node) : node
relation reqVote   (C : node) (T : term)
relation voteMsg   (V : node) (C : node) (T : term)

-- data-plane term handle (what the receiver filters DATA at and stamps reports with).
-- Written ONLY by BecomeLeader (node.rs:2478) and BecomeFollower (node.rs:2506) —
-- never by StartElection. That omission is the whole of #9.
function handleTerm (N : node) : term
function mapTerm    (N : node) : term

-- data plane at the single tracked position P
relation durableTo      (N : node)  -- bytes at P landed (what AppendPosition reports)
relation holdsEntry     (N : node)  -- those bytes are the current leader's entry E
relation tailAttributed (N : node)  -- j's MAP knows what sits at P (see header)

-- receiver intake gate + reconcile latch
relation gateOpen          (N : node)
relation awaitingReconcile (N : node)

relation report (V : node) (T : term)
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
  handleTerm N := tot.zero
  mapTerm N := tot.zero
  durableTo N := false
  holdsEntry N := false
  tailAttributed N := true
  gateOpen N := true
  awaitingReconcile N := false
  report V T := false
  committed := false
}

-- ---------- election plane ----------

-- StartElection bumps curTerm and stores NO handle — the lagging handle is created
-- HERE. It also leaves the gate/latch untouched.
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

-- grant arm; adopting a strictly new term is a BecomeFollower: re-key the handle,
-- close the gate, arm reconciliation.
action deliverRequestVoteGrant (j : node) (c : node) (t : term) {
  require reqVote c t
  require tot.le (curTerm j) t
  require ¬ (hasVoted j ∧ voteTerm j = t ∧ voteCand j ≠ c)
  if tlt (curTerm j) t then candidate j := false
  if tlt (curTerm j) t then leader j := false
  if tlt (curTerm j) t then handleTerm j := t
  if tlt (curTerm j) t then gateOpen j := false
  if tlt (curTerm j) t then awaitingReconcile j := true
  curTerm j := t
  hasVoted j := true
  voteTerm j := t
  voteCand j := c
  voteMsg j c t := true
}

action becomeLeader (i : node) (q : node) {
  require candidate i
  require member i q
  require ∀ V, member V q → (V = i ∨ voteMsg V i (curTerm i))
  leader i := true
  candidate i := false
  handleTerm i := curTerm i
  -- BecomeLeader PRIMES the counters (node.rs:2472-2483): "collapse volatile via
  -- prime(base) — old bytes above base must never be streamable". A new leader's
  -- divergent tail is dropped, so it cannot go on to report those bytes as durable.
  if (durableTo i ∧ ¬ holdsEntry i) then durableTo i := false
}

-- Raft §5.2: a candidate that hears from a leader of its OWN term reverts to
-- follower. This is a BecomeFollower, so it re-keys the handle to that term and (the
-- handle strictly advancing) closes the gate and re-arms reconciliation. This is the
-- step that lets #9's bad byte later be reported at the LEADER's term.
action revertToFollower (j : node) (i : node) {
  require leader i
  require candidate j
  require curTerm j = curTerm i
  candidate j := false
  if tlt (handleTerm j) (curTerm i) then gateOpen j := false
  if tlt (handleTerm j) (curTerm i) then awaitingReconcile j := true
  handleTerm j := curTerm i
}

-- ---------- reconcile ----------

-- ADOPTING reconcile: leader i's map term is strictly above j's current term, so
-- BecomeFollower re-keys the handle FIRST and `current_term == adopted_term` holds —
-- the reopen fires under both policies. Truncates a divergent tail the map can see.
action reconcileAdopt (j : node) (i : node) {
  require leader i
  require tlt (curTerm j) (curTerm i)
  if (durableTo j ∧ ¬ holdsEntry j ∧ tailAttributed j) then durableTo j := false
  -- Adopting a strictly higher term IS a BecomeFollower: j STEPS DOWN. Without this
  -- a sitting leader could adopt a higher term, stay flagged leader, and go on to
  -- commit at the new term — which is how the artifact CE reached a phantom commit.
  candidate j := false
  leader j := false
  curTerm j := curTerm i
  handleTerm j := curTerm i
  mapTerm j := curTerm i
  gateOpen j := true
  awaitingReconcile j := false
}

-- NON-ADOPTING reconcile — THE #9 SITE. The map term does not exceed j's current
-- term, so there is no BecomeFollower and the handle is NOT re-keyed. Pre-fix the
-- clean reconcile reopens intake anyway — for j's STALE handle-term stream.
-- The shipped guard reopens only when `sm.current_term() == adopted_term`.
action reconcileNonAdopt (j : node) (i : node) {
  require leader i
  require awaitingReconcile j                 -- the reopen lives in this branch
  require tot.le (curTerm i) (curTerm j)      -- non-adopt
  require tlt (handleTerm j) (curTerm i)      -- a HIGHER-term leader's map (t > handle)
  if (durableTo j ∧ ¬ holdsEntry j ∧ tailAttributed j) then durableTo j := false
  mapTerm j := curTerm i
  if (¬ finding9Fix ∨ curTerm j = handleTerm j) then gateOpen j := true
  if (¬ finding9Fix ∨ curTerm j = handleTerm j) then awaitingReconcile j := false
}

-- ---------- data plane ----------

action appendEntry (i : node) {
  require leader i
  require ¬ committed
  holdsEntry i := true
  durableTo i := true
  tailAttributed i := true
}

action replicate (i : node) (j : node) {
  require leader i
  require holdsEntry i
  require gateOpen j
  require curTerm j = curTerm i
  require ¬ (durableTo j ∧ ¬ holdsEntry j)
  candidate j := false
  leader j := false
  holdsEntry j := true
  durableTo j := true
  tailAttributed j := true
}

-- An ORDINARY divergent tail: taken under a term whose map DID attribute it, so a
-- later reconcile detects and truncates it.
action staleStreamAppend (j : node) {
  require gateOpen j
  require ¬ holdsEntry j
  require ∀ L, leader L → handleTerm j ≠ curTerm L
  durableTo j := true
  tailAttributed j := true
}

-- THE #9 CROSS-STREAM ACCEPT. j takes a byte at P from its stale handle-term stream
-- while its map has already moved on (handle lags map). Because the map never
-- attributed it, `tailAttributed` stays FALSE and no later reconcile truncates it.
-- Reachable only if intake is open at a lagging handle — which is exactly what the
-- shipped guard prevents, so the checker still has to FIND the bad reopen.
action crossStreamAccept (j : node) {
  require gateOpen j
  require ¬ holdsEntry j
  require tlt (handleTerm j) (mapTerm j)      -- handle lags the map: cross-stream
  durableTo j := true
  tailAttributed j := false
}

action sendReport (j : node) {
  require gateOpen j
  require durableTo j
  report j (handleTerm j) := true
}

action commitEntry (i : node) (q : node) {
  require leader i
  require ¬ committed
  require member i q
  require holdsEntry i
  require ∀ V, member V q → (V = i ∨ report V (curTerm i))
  committed := true
}

-- ---------- safety ----------

-- PROXY (shallow): the invariant the shipped guard establishes — intake is never
-- open while the data-plane handle lags the map. Violating this IS the cross-stream
-- accept becoming possible.
safety [no_cross_stream_reopen]
  proxyOn → ¬ (gateOpen N ∧ tlt (handleTerm N) (mapTerm N))

-- FULL (deep): the acked-write-loss itself.
safety [no_phantom_commit]
  committed → ∃ (q : node), ∀ V, member V q → holdsEntry V

#gen_spec

-- BFS reports the SHALLOWEST violation, so a returned trace's length IS the minimum
-- depth — "not found at any smaller bound" follows for free, no ladder of runs needed.

-- PROBE A — pre-fix, proxy ON. RESULT (recorded): CE at depth 7 in 1m44s.
#model_check { node := Fin 3, term := Fin 3 }
  { finding9Fix := false, proxyOn := true } (maxDepth := 12)

-- PROBE B — post-fix, proxy ON. Calibration: the shipped guard must close it.
#model_check { node := Fin 3, term := Fin 3 }
  { finding9Fix := true, proxyOn := true } (maxDepth := 12)

-- PROBE C — pre-fix, proxy OFF: the REAL depth question. How deep before the checker
-- reaches the full acked-write-loss (`no_phantom_commit`), which needs the candidate
-- to revert, re-reconcile clean over an unattributed byte, and certify a commit?
#model_check { node := Fin 3, term := Fin 3 }
  { finding9Fix := false, proxyOn := false } (maxDepth := 13)
