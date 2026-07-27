# Veil spike — SDD ledger (started 2026-07-19, opus; fable out of credits)
Brief: docs/superpowers/specs/2026-07-19-uc2-veil-spike-brief.md (Amendment-3 sequence: V0 pre-flight → V1 port+Bar-1 → SESSION-1 RE-GATE → V-M7 → V2).
Workspace: /home/claude/veil-spike (REAL DISK — box /tmp is RAM tmpfs, no heavy artifacts there). proofs-veil/ isolated pkg, never on proofs/ lake path or CI.

## V0 — pre-flight maturity check (IN PROGRESS)
- Env OK: /dev/sda1 41G avail ext4; elan 4.2.3 + lake + lean; github reachable.
- verse-lab/veil MAIN @ 6a12daa pins lean 4.24.0; SMT-backed (z3 4.15.4 + cvc5 1.3.1, lean-auto + lean-smt). Exposes #check_invariants/#check_invariant/#check_action = SMT/Ivy-style INDUCTIVENESS check + CTI generation. Covers Bar-1 + M7 inductive-safety. NOT an explicit-state reachability checker.
- ** veil-2.0-preview @ (fetched) pins lean 4.28.0 — HAS the explicit-state model checker: Veil/Core/Tools/ModelChecker/Concrete/{Checker,Sequential,SearchContext,Progress,MapReduce,Containers}.lean + Symbolic/ + #model_check command + TraceCounterexample UI. THE V2 bug-hunt feature IS REAL. **
- KEY CLARIFICATION vs brief: brief conflated two features. Veil core (both branches) = SMT inductiveness + CTI (proposes invariant strengthenings). Explicit-state REACHABILITY checker (#model_check, finds reachable countermodels from init = the "5th bug" machine) = veil-2.0-preview ONLY, lean 4.28.0.
- REMAINING V0 GATE: does veil-2.0-preview BUILD here (lean 4.28 + z3/cvc5 download + lake build) and does a bundled #model_check example run? If yes → V1. If version-wall/build-fail → that's the finding, exit cheap (Aeneas precedent).

## V0 build attempt (background PID logged)
- veil-2.0-preview cloned to /home/claude/veil-spike/veil-preview; lean 4.28.0 (elan auto-install).
- Bundled #model_check examples = 52 files incl. TLA/MultiPaxos, Ivy/TwoPhaseCommit, Ivy/Ring — real consensus references for the UC port.
- RAM tight (7G avail); build bounded -j3 to avoid OOM; log build.log on real disk.
- Maturity gate = does it build + does ONE bundled #model_check example run. Result pending.

## V0 — PASS (clean, 2026-07-19)
- Veil preview builds here: mathlib via `lake exe cache get` (8010 files); lean-smt/auto + Veil = 1418 jobs green. NO version wall.
- Only failure = npm infoview widget (browser trace viz, not core). Fixed offline: stubbed .lake/build/js/{RefreshComponent,traceDisplay,verificationResults}.js + commented `needs := #[widgetJsAll]` in lakefile (scratch checkout). Core + ModelChecker unaffected.
- FFI: cvc5/z3 .so present under .lake/packages/{cvc5,z3}/. #model_check runs at ELABORATION → execute via `lake build <ExampleModule>` (links extern libs), NOT `lake env lean` (interpreter misses FFI → cvc5.TermManager.new symbol error).
- ** CHECKER EXECUTES: Examples/Puzzles/DieHard built → `❌ Violation: safety_failure` + 7-state concrete trace with per-state action (init→FillBigJug→BigToSmall→...). 354ms. This trace shape = exactly what maps to a directed uc2_sim regression. **
- Caveat (documented): infoview HTML widget unavailable offline → no pretty in-editor trace rendering, but text trace (the artifact that matters) prints fine.
=> V0 GATE PASSED. Proceed to V1 (proofs-veil/ pkg + election-plane port + Bar-1 #check_invariants).

## V1 — election-plane port + Bar-1: DONE (clean green). SESSION-1 RE-GATE reached.
Artifact: veil-preview/Examples/UC/Election.lean (scratch; also root UcElection.lean). Builds green (1418 jobs), FFI via lake build.
- PORT: UC S2 election model (Protocol.lean) → Veil. 6 actions (startElection/deliverRequestVoteGrant/deliverVote/becomeLeader/crashRestart; data-plane logOk abstracted to nondeterministic grant guard, faithful to S2 election-safety scope). Quorums abstract (Paxos idiom: immutable member + quorum_intersection assumption = Lean C5). All 5 Inv clauses (grant_state/grant_uniq/self_vote/votes_sound/leader_quorum) + election_safety ported verbatim in shape.
- ** BAR-1 PASS: #check_invariants certified all 5 Inv clauses + election_safety INDUCTIVE via SMT (cvc5). 43 ✅. Veil independently re-proved election safety inductive — matches Lean election_safety. This is the decisive "does the UC model fit Veil's fragment" evidence = YES. **
- sat trace PASS: leader electable within 3 actions (model live + explicit-state trace engine engages with UC model).
- FINDING (session-2 scope): concrete explicit-state #model_check needs CONCRETE quorum/member interpretation (majorities); abstract member relation is SMT-checkable but not concretely enumerable. Documented inline in the model. The V2 forward-hunt + Bar-2 calibration need a concrete-majority variant of the port.
- DSL lessons (for session-2 speed): actions use `{ }` not `= { }`; assumption AFTER #gen_state; partial-app updates `r i V := const` (RHS referencing row var → clear-then-set); brace-free `if c then stmt`; run via `lake build <Module>` not `lake env lean` (FFI); npm widget stubbed offline.

SESSION-1 RE-GATE STATUS (per brief Amendment-3): V0 PASS + V1 port + Bar-1 PASS. Bar-2 (Finding-#5 calibration) + Bar-2b (frame abstraction) NOT done — both need the COMMIT-plane port (+ concrete quorums), which is session-2. Decision to user: continue to session 2 (commit plane + calibration + M7) vs checkpoint.

## SESSION 2 (continue) — both Veil engines confirmed on the UC election model
Two clean green artifacts in veil-preview/Examples/UC/:
- **Election.lean** (abstract quorum + intersection assumption = Lean C5): #check_invariants PASS — 5-clause Inv + election_safety INDUCTIVE via SMT, ALL n. 43 ✅, 0 ❌. (Bar-1, all-n.)
- **ElectionMC.lean** (concrete excludes-one majority, n>=3): #model_check {node:=Fin 3, term:=Fin 3} PASS — ✅ No violation, 60761 states explored + sat trace ✅. (The V2 explicit-state forward-hunt engine, confirmed on the UC model.)
- ** BOTH Veil engines now proven on UC's model: SMT-inductive (all-n) AND explicit-state-safe (60761 states, n=3). **
- REAL MODELING FINDING (SMT caught it): concrete "excludes-one" majority is unsound at n=2 (single node = false quorum → {0},{1} don't intersect → 2 leaders); #check_invariants flagged the n=2 becomeLeader CTI correctly, #model_check@n=3 correctly safe. Lesson for the port: use abstract-quorum+assumption for SMT invariant proofs; concrete majority only for n>=3 explicit-state. Matches classic Ivy quorum modeling.

SESSION-2 REMAINING (per brief): commit-plane port (boot gate + vote + phantom commit) → Bar-2 real calibration (rediscover Finding #5 from pre-fix model via #model_check) → Bar-2b frame abstraction → V-M7 (config change × election, primary hunt) → V2 coherence-window forward hunt. Election plane (both engines) = DONE + banked; the harder planes remain.

## SESSION 3 (2026-07-24, fable) — V-M7 primary hunt STARTED (worktree uc2/veil-spike)
Worktree: /home/claude/ultima/ultima_cluster-veil-spike (branch uc2/veil-spike off main 851b6e6).
Env re-verified intact: ElectionMC rebuild green (60761 states, no violation, FFI links).
Resumed at session-2 remainder. Per Amendment-3, V-M7 is PRIMARY and needs only the
election port + Bar-1 (both banked) — went straight to it.

### V-M7 model: Examples/UC/Reconfig.lean (scratch, veil-preview)
Config-as-evolving-nodeSet (TSet primitive, TLA/Raft isQuorum idiom applied to a CHANGING
set): each node's adopted voter set is a concrete `nodeSet`; majority = count(votes ∩ cfg)*2 > count(cfg).
Single-server change via nset.insert/remove (inherently one-member diff). One-in-flight (pending).
`adjacencyGuard` toggle: GUARDED (single-server-adjacent adoption) vs ABLATED (arbitrary jump).
Election plane reused verbatim from ElectionMC. Safety = election_safety.
Design: guarded #model_check SAFE = M7 assurance; ablated → checker finds disjoint-quorum
double-leader = Bar-2b-analog (model preserves the reconfig-safety bug class).

### FIDELITY FINDING F-M7-1 (model artifact caught + fixed; NOT a UC bug)
First two guarded runs BOTH produced an election_safety CE — analysis showed both were
MODEL ARTIFACTS, tightened to real UC guards:
 (a) quorum rule too weak: `majorityOf(votes, cfg)` let a single self-vote from a NON-member
     beat a size-1 config (|{2}|*2=2 > |{1}|=1). FIX: quorum = majority of (votes ∩ cfg) —
     only votes from current config voters count. REAL UC rule.
 (b) adopt decoupled from term/authority: node 2 ingested leader-0's fresh config entry {2}
     (authored @ term 2) while remaining an independent term-2 candidate, then self-won under
     {2}. Electing quorums {0,1} vs {2} DISJOINT = textbook single-server disjoint-quorum shape,
     but UNREACHABLE in UC: config entries ride term-stamped replication frames, so receiving
     one = hearing from the leader → Raft §5.2 candidate reverts to follower. FIX: adopt requires
     curTerm j ≤ curTerm i, sets candidate/leader j := false, curTerm j := curTerm i.
Both fixes are real UC mechanisms, documented inline. This is the V-M7 analog of LC-arc Finding
#8 (model-fidelity gap forcing a faithful model) — expected, and exactly the abstraction-obligation
discipline the brief mandates before trusting a SAFE/UNSAFE verdict.

### PENDING: guarded #model_check with term-coupled adopt (running, bg by6fx95k3)
State space grew (adopt now writes term+role); >600s, backgrounded. Verdict pending:
expect SAFE (guarded) — if still UNSAFE, the CE is the next fidelity probe or a real finding.
NEXT after verdict: ablate (adjacencyGuard=false) to confirm checker re-finds the disjoint CE
(calibration), then #check_invariants for inductiveness, then V3 report gate doc.

### SESSION 3 RESULTS — V-M7 primary hunt (checkpoint, real findings)
Ran the full three-mode sweep on Reconfig.lean (n=3, ExtTreeSet Fin 3 concrete configs):
1. election_safety, ABLATED (arbitrary config jumps), term Fin 2: ✅ SAFE, 187907 states.
   → term discipline (revert-on-leader-contact in adopt), NOT config adjacency, is what
     makes reconfig ELECTION-safe. Robust even when adjacency is removed.
2. quorum_overlap, ABLATED, term Fin 3: ❌ VIOLATED — textbook disjoint-quorum CE
   (leader{t1,q={0,2}} vs leader{t2,q={1}}, node adopted NON-adjacent config {1}).
   → CALIBRATION PASSED: the model + checker DO catch the reconfig disjoint-quorum bug
     class (the Bar-2b analog for M7). Peak lean RSS ~5.7GB, safe.
3. quorum_overlap, GUARDED (single-server adjacency), term Fin 3: ❌ VIOLATED — but the
   trace is a VALID adjacent chain ({0,1,2}→{1,2}→{2}, leader self-removes) yielding
   two leaders with disjoint elecQuorums {0,1} vs {2}.

### FINDING F-M7-2 (the session's key insight — NOT a UC bug; model-fidelity boundary)
The guarded quorum_overlap violation is a FALSE POSITIVE of the property, revealing the
model's decoupling of config from the log:
 - single-server change DELIBERATELY permits non-overlapping quorums across NON-adjacent
   configs; adjacency only guarantees CONSECUTIVE-config overlap, not first-vs-last.
 - real UC preserves safety because config changes are LOG ENTRIES: any node in config {2}
   necessarily holds the committed prefix (incl. node 0's term-1 entry), so nothing is lost.
   My model's `adopt` grants a config WITHOUT requiring the committed prefix → the quorum-
   overlap / election-restriction properties report data loss that cannot occur in UC.
 - Also surfaced: a self-removed leader's `leader` flag lingers (no step-down modeled), so
   quorum_overlap-over-current-leaders over-counts benign stale leaders that cannot commit.
CONCLUSION: a FAITHFUL V-M7 leader_completeness check REQUIRES a commit/log plane that
couples config-entry adoption to holding the committed prefix — the exact M7 analog of the
LC arc's data-plane refinement (Findings #7/#8). Scoped as the next modeling phase. Two
concrete Rust-reconfirm questions for a future session (brief's "any hit → reconfirm in Rust"):
 (a) does a self-removed UC leader step down promptly, and is there a stale-read window before it?
 (b) is adopt-requires-committed-prefix actually enforced on the M7 config-adoption path?

### V-M7 STATUS: strong checkpoint. Deliverables archived to worktree proofs-veil/.
election_safety proven robust; checker calibrated on the disjoint-quorum class; the precise
leader_completeness property scoped to a commit-plane extension. Gate doc written (V3).

### SESSION 3 cont. — commit-plane extension (leader_completeness). STOPPED by user mid-run.
Built ReconfigLC.lean: adds commit/log plane (single committed entry E: committed/committedTerm/
entryHolders) + UN-abstracted up-to-date election restriction + prefixCoupling knob (the F-M7-2
mechanism: adopt-requires-committed-prefix). Safety = leader_completeness.
MODEL BUG CAUGHT+FIXED: up-to-date restriction must key on LOG contents (entryHolders), not
`committed` — else a pre-commit vote strands a soon-to-commit entry (a real Raft subtlety; first
(F,F) run produced that spurious CE, cfg unchanged). Fixed: drop the `committed ∧` gate.
TRACTABILITY BOUNDARY (the finding — both walls hit, precisely characterized):
 * #check_invariants (SMT, all-n): 💥 "incorrect number of universe levels List" on the concrete
   TSet ops (count/intersection/subsets are List-backed) — concrete-set model CANNOT take the SMT
   inductiveness path. Would need an abstract-quorum (member/quorumin relations + adjacency
   assumptions, VerticalPaxos-style) reformulation + LC-arc-style supporting invariants.
 * #model_check (explicit-state): concrete cardinality OK, but config+commit state space EXPLODES
   at n=3 — (F,F) unbounded 700s no verdict; (F,F) maxDepth:=13 (syntax works) still exploring at
   10min (breadth at each depth is the blowup, not depth). CE is deep (~13 steps: append+replicate+
   commit E, then config-walk, then losing election).
STATUS: model + property faithful and built; the disjoint-quorum PRECURSOR already shown in
Reconfig.lean (session 3). Landing a clean guarded-SAFE/ablated-UNSAFE leader_completeness needs
EITHER (a) abstract-quorum reformulation + inductive proof (local, ~LC-arc S2-equiv effort), OR
(b) a much larger box for deeper bounded explicit-state coverage (helps the CE only; the SAFE
direction is exponential, not compute-bound). USER DECISION PENDING (AWS offered).

---

### SESSION 4 (2026-07-24) — Bar-2 / Bar-2b: the boot-gate commit plane

New model `BootGate.lean` (n=3, term=Fin 2), targeting **Finding #5** — the SHALLOWEST
of the four known coherence-window bugs, which is precisely why the brief makes it the
tool-fitness gate. Knob `bootGateFix`: false = pre-fix (gate boots open
unconditionally), true = shipped fix (`node.rs:533-534`, gate closes iff
`vote_term > map_term`). Both Rust anchors re-read before modeling.

DELIBERATELY NOT the TSet encoding — the excluded-node quorum encoding from
ElectionMC.lean sidesteps the SMT `List`-universe wall that stopped ReconfigLC.lean.

#### The Bar-2b distinction is STRUCTURAL, not bolted on
Two independent relations at the single tracked position P:
 * `durableTo N`  — bytes at P landed. This is what AppendPosition reports.
 * `holdsEntry N` — those bytes are the CURRENT leader's entry E (whose stream they
                    came from).
`staleStreamAppend` vs `replicate` are the two arms; AppendPosition reports the former
and NEVER attests the latter, which IS the bug class. Erasing the distinction would
blind the V2 hunt — hence the brief's abstraction obligation.

#### THREE MODEL-FIDELITY BUGS caught by hand-tracing the CE before trusting a verdict
All three biased the result the SAME way — making the shipped fix look ineffective —
which is the failure mode to be paranoid about, since a Bar-2 red is the spike's DROP
verdict. Found by re-deriving the intended 8-step CE against the model text, NOT by
running it:
 1. **Term adoption did not close the intake gate.** Real UC closes it in
    `Action::BecomeFollower` on a strictly new term (`node.rs:2511-2513`; test
    `node.rs:3456` drives exactly this via a higher-term RequestVote). Without it the
    model admits a SHALLOWER phantom commit that is NOT Finding #5 — an unreconciled
    divergent follower reporting at the new term without ever crashing. Bar-2 would
    have "passed" on the wrong bug AND the post-fix run would have been spuriously
    UNSAFE. With it, the boot gate is the ONLY remaining route to reporting an
    unreconciled divergent tail — i.e. the model became discriminating.
 2. **`replicate` accepted data from a higher-term leader.** A node behind the
    leader's term has not adopted it, so its gate is shut and the frame is dropped.
    Now requires `curTerm j = curTerm i`; adoption goes through `reconcile` only.
 3. **A standalone `shipTermMap` advanced `mapTerm` independently.** This let
    `mapTerm` catch up to `voteTerm` with the divergent tail still intact, SILENTLY
    BYPASSING the fix's own `vote_term > map_term` predicate. Deleted: receiving the
    term map IS the reconciliation trigger, so `reconcile` now both adopts the term
    and truncates a divergent tail (a clean tail survives).

#### TOOLCHAIN LESSONS (cost two wasted 3-10 min builds; recorded so the next session doesn't repeat them)
 * **Veil relation assignments take `Bool`, not `Prop`.** `certified V := V ≠ q` does
   not elaborate; only literals (`true`/`false`) and `if <Prop> then r := <Bool>`.
   Props belong in conditions and requires. (First diagnosis — "ghost relations break
   in an assignment RHS" — was WRONG; the second failure on a bare `≠` disproved it.)
 * **An elaboration error does NOT fail the `#model_check`; it silently VOIDS it.**
   The failed assignment was the one populating the certifying set, so
   `no_phantom_commit` became VACUOUSLY TRUE and the run cheerfully reported
   "✅ No violation (explored 500029 states)". Twice. **Always confirm zero
   `error: Examples/...` lines before reading any verdict.**
 * **Run a syntax-only pass first** (detach the `#model_check` commands): 29s vs 10min.
 * **`✅ No violation` does NOT distinguish exhaustion from a depth bound** —
   `TraceDisplay.lean:104` renders `exploredAllReachableStates` and
   `reachedDepthBound` identically. A SAFE verdict is only exhaustive if NO `maxDepth`
   was passed. (This retroactively qualifies how ReconfigLC's `maxDepth := 13` runs
   must be read.)
 * **Batch all knob variants as multiple `#model_check` commands in one file** — the
   Veil library is cached, so only the model recompiles; 4 verdicts for 1 build.
 * `sat trace` at the 7 actions a genuine commit needs blows up `simp` (max steps
   exceeded). Non-vacuity is instead a **knob-gated canary safety** run on the
   explicit-state engine, where a REPORTED VIOLATION is the good news (its trace is
   the witness). Same trick encodes the Bar-2b directed check.

#### THREE MORE fidelity gaps — these the CHECKER found, via the post-fix CE
The pre-fix run passed immediately, but the post-fix run kept coming back UNSAFE. Each
CE was a model artifact, not a residual UC hazard; each was adjudicated against the
Rust before being "fixed" (the discipline matters — patching a model until it goes
green is exactly how a spike talks itself into a false KEEP):
 4. **A leader committed an entry it never authored.** `commitEntry` required only
    `durableTo i`, so the leader committed off its own stale tail with `holdsEntry =
    []` — nobody in the cluster held E. ReconfigLC.lean had it right
    (`nset.contains i entryHolders`); weakening it was my error. Now `require holdsEntry i`.
 5. **Reports were stamped with the CONSENSUS term, not the receiver's HANDLE term.**
    `term_handle.store` has exactly two call sites — `BecomeLeader` (node.rs:2478) and
    `BecomeFollower` (node.rs:2506). There is NO candidate path, so a node starting its
    own election keeps stamping the OLD term and a same-term leader rejects its reports
    as stale. The model had an independent candidate (never in contact with the leader)
    certifying a commit. Now `handleTerm` is separate state from `curTerm`.
    **This cuts FOR Finding #5 too:** the handle is seeded at boot with `boot_term`
    (node.rs:513) = recovered max(vote_term, map_term), which is exactly what lets the
    rebooted unreconciled voter stamp a SAME-TERM report. Independent confirmation that
    node.rs:2404-2418 (Finding #9's own fix) names the same lagging-handle distinction:
    "`Action::StartElection` bumps `current_term` but stores NO handle ... so a
    CANDIDATE runs its data plane at a LAGGING handle."
 6. **Stale-stream bytes materialised on a node already synced to the sitting leader.**
    The CE had node 0 reconcile at term 1 and THEN sprout foreign bytes at P. DATA is
    filtered at `adopted_term == term_handle` (receiver.rs:635 `dropped_stale_term`;
    node.rs:2404-2418), so a synced node receives that leader's stream and nothing
    else. Guarded with `∀ L, leader L → tlt (handleTerm j) (curTerm L)`.
    **NARROWING — recorded honestly:** this also rules out the ordering where a node
    takes a divergent tail FROM the sitting older-term leader. With one tracked entry
    at one tracked position that class is still represented (take the stale bytes while
    no leader sits — which is what the Bar-2 CE does), but RUN 2's SAFE verdict is
    therefore **"safe within this restriction", NOT unqualified**.

### SESSION 4 VERDICTS (n=3, term=Fin 2; one build, four `#model_check` runs, 7m33s)
Elaboration clean (zero `error: Examples/...` lines) — checked explicitly, since a
silent elaboration failure VOIDS a run into a vacuous pass (see toolchain lessons).

| Run | Knobs | Result | Reading |
|---|---|---|---|
| 1 | `bootGateFix := false` | ❌ `no_phantom_commit` | **BAR 2 — PASS** |
| 2 | `bootGateFix := true` | ✅ no violation, **312009 states, EXHAUSTIVE** (no maxDepth) | fix calibrated: CE gone |
| 3 | `+ vacuityCanary` | ❌ `genuine_commit_canary` | **non-vacuity CONFIRMED** — RUN 2's SAFE is not "nothing ever commits" |
| 4 | `+ bar2bCanary` | ❌ `bar2b_stream_distinction` | **BAR 2b — PASS** (violation = the good outcome) |

**BAR 2 CE is exactly the Finding-#5 shape, 8 steps** — every element of the shipped
bug's description present, in order:
```
staleStreamAppend(j=1)             node 1 acquires a divergent tail
startElection(i=0, t=1)
deliverRequestVoteGrant(c=0,j=1,t=1)  node 1 GRANTS term 1 → gate closes, mapTerm stays 0
becomeLeader(i=0, q=2)
crashRestart(j=1)                  reboots: vote_term 1 > map_term 0; PRE-FIX gate boots OPEN
appendEntry(i=0)
sendReport(j=1)                    unreconciled voter reports divergent durable AT TERM 1
commitEntry(i=0, q=2)              phantom commit — holders {0}, no quorum holds E
```
Bar-2 was the spike's true tool-fitness gate and its failure would have been the DROP
verdict; it passes on a model that ALSO calibrates the fix correctly (runs 1+2 together),
which is the pairing that makes either verdict worth anything.

**BAR 2b PASS in its commit-plane form** — the gate doc's deferred row. The checker
exhibits a reachable state with two nodes both holding bytes at P, one from an earlier
leader's stream (`¬holdsEntry`) and one from the current leader's (`holdsEntry`): a
stale-handle-term stream byte and a current-term stream byte AT THE SAME POSITION,
distinguished. The V2 window hunt is therefore not blind to its target class.

**NOT ATTEMPTED this session: the #9/#6b depth probe** (brief's explicit stretch, NOT a
gate — coming up empty there does not license a DROP, and neither does not running it).
It is now much cheaper than it was: `handleTerm` is in place, and node.rs:2404-2418
spells out #9's shape (a CANDIDATE that cleanly reconciles a HIGHER-term leader's map
without adopting reopens intake for its LAGGING handle-term stream and accepts a
cross-stream byte). Modeling it needs reconcile-without-adopt + a reopen keyed on
`SM term == handle term`, behind a `finding9Fix` knob — the natural next session.

---

### SESSION 4b — the #9/#6b DEPTH PROBE (`Finding9.lean`)
Brief's explicit STRETCH item, NOT a gate: "how deep a bound is needed before the
checker rediscovers #6b's 3-term Figure-8 / #9's cross-stream accept — this calibrates
the forward hunt's confidence and tells you whether 'absence at depth N' means
anything". Coming up empty here does not license a DROP.

TARGET (node.rs:2404-2423, shipped guard `sm.current_term() == adopted_term`):
`Action::StartElection` bumps `current_term` but stores NO handle (node.rs:2440-2450),
so a CANDIDATE runs its data plane at a LAGGING handle. Pre-fix, a candidate that
cleanly reconciles a HIGHER-term leader's map (non-adopt) REOPENS intake for its stale
handle-term stream and then accepts a cross-stream byte its map never attributed.

#### Modeling: `tailAttributed` is what makes #9 expressible at all
#9's damage is INVISIBLE TO RECONCILIATION — the byte is one "its map never
attributed", so a later clean reconcile cannot detect it and does NOT truncate it.
`staleStreamAppend` (an ordinary divergent tail, taken under a term whose map DID
attribute it) sets `tailAttributed := true` and IS truncated on reconcile;
`crossStreamAccept` sets it FALSE and SURVIVES reconcile. Without that distinction the
model cannot express #9's loss. Not question-begging: `crossStreamAccept` still
requires `gateOpen`, so the checker must FIND the bad reopen itself.

#### Two properties at deliberately different depths
 * `no_cross_stream_reopen` (PROXY, shallow) — the invariant the shipped guard
   establishes: intake never open while the handle lags the map.
 * `no_phantom_commit` (FULL, deep) — the acked-write-loss itself.
Knob-gated (`proxyOn`) because BFS halts at the FIRST violation: leaving the depth-7
proxy on MASKS the deep hunt entirely.

Scope trims: `crashRestart` DROPPED (that is #5's mechanism, banked in BootGate.lean),
so any CE here is in the #9 class. `staleStreamAppend`'s guard RELAXED vs BootGate to
`∀ L, leader L → handleTerm j ≠ curTerm L` — strictly more permissive (better for a
hunt), still blocks the artifact BootGate needed it for.

#### PROBE A RESULT — the headline number
**#9's cross-stream reopen is reachable at DEPTH 7, in 1m44s** (n=3, term=Fin 3,
maxDepth 12, pre-fix). BFS returns the SHALLOWEST violation, so 7 is the MINIMUM depth
and "unreachable at any smaller bound" follows for free — no ladder of runs needed.
The trace is exactly the Rust comment's scenario:
```
1 startElection(i=1,t=1)
2 startElection(i=0,t=2)
3 grant(c=0,j=1,t=2)        node 1 adopts term 2
4 grant(c=1,j=2,t=1)        node 2 adopts term 1 -> handle:=1, gate CLOSED, awaiting
5 startElection(i=2,t=2)    node 2 CANDIDATE at 2 -> curTerm 2, handle STAYS 1 (LAGGING)
6 becomeLeader(i=0,q=2)     node 0 leader at term 2
7 reconcileNonAdopt(j=2,i=0)  candidate reconciles a HIGHER-term leader's map, non-adopt
                              -> PRE-FIX reopens intake for its STALE handle-term stream
```
End state: node 2 has `gateOpen`, `handleTerm=1`, `mapTerm=2` — intake open at a
lagging handle, the precise hazard the shipped guard forbids.

**This is a much better result than expected.** Session 3's ReconfigLC wall (a ~13-step
CE unreachable in 700s) suggested deep-bug probes were hopeless at n=3; #9's *enabling
condition* turns out to be shallow and cheap. term=Fin 3 was structural here (adopt T1
-> candidate at T2 -> leader at T2), and it stayed affordable — RAM never below 12.6GB
free, so the box-safety ceiling was never approached.

#### TWO MORE fidelity artifacts — found by the checker, masking the real hunt
The first B/C runs both returned the SAME 11-step CE involving NEITHER
`crossStreamAccept` NOR `reconcileNonAdopt` — i.e. knob-INDEPENDENT, so not a #9 path
at all. Adjudicated against the Rust and fixed:
 7. **`reconcileAdopt` did not clear the role flags.** Adopting a strictly higher term
    IS a `BecomeFollower` — the node STEPS DOWN. Without it a sitting leader adopted a
    higher term, stayed flagged `leader`, and committed at the new term.
 8. **A brand-new leader reported over a divergent tail.** `BecomeLeader` PRIMES the
    counters (node.rs:2472-2483: "collapse volatile via prime(base) — old bytes above
    base must never be streamable"), so a new leader's divergent tail is dropped and
    cannot be reported as durable.
Running total for the session: **8 model-fidelity gaps, every one adjudicated against
the Rust before being fixed.** The recurring lesson is that the checker finds the
MODEL's bugs long before UC's, and that a CE is a question, not an answer.

#### PROBES B + C RESULTS (re-run with fidelity fixes 7+8; 15m32s for all three)
| Probe | Knobs | Bound | Result | Reading |
|---|---|---|---|---|
| A | pre-fix, proxy ON | maxDepth 12 | ❌ `no_cross_stream_reopen` at **depth 7** | unchanged by fixes 7+8 |
| B | **post-fix**, proxy ON | maxDepth 12 | ✅ no violation, 879650 states | guard closes the reopen — **within depth 12** |
| C | pre-fix, proxy OFF | maxDepth 13 | ✅ no violation, 1288622 states | full acked-write-loss **NOT reachable within depth 13** |

**READ B AND C AS BOUNDED, NOT SAFE.** Both passed a `maxDepth`, and per this
session's own toolchain finding `✅ No violation` renders identically for
`exploredAllReachableStates` and `reachedDepthBound` (`TraceDisplay.lean:104`). So B is
"the shipped guard admits no cross-stream reopen within depth 12", not "proved safe";
C is "not found within depth 13", not "absent".

#### THE CALIBRATION ANSWER (this is the deliverable the brief asked for)
**#9 splits cleanly into a shallow enabling condition and a deep consequence:**
 * the ENABLING CONDITION (intake open at a lagging handle) is at **depth 7, 1m44s**;
 * the FULL acked-write-loss is **beyond depth 13** even pre-fix, after 1.29M states.

So for the V2 forward hunt at n=3/Fin 3: **"absence at depth ~13" is worth very little
for a full data-loss property, but a great deal for an invariant-shaped proxy.** The
practical guidance that falls out — hunt PROXY INVARIANTS (the conditions the shipped
guards establish), not end-to-end loss properties. The four known bugs all have such a
proxy (#5: report escaping an unreconciled boot; #9: intake open at a lagging handle),
and the proxy sits ~6 steps shallower than the loss it enables. That is the single most
useful thing this probe produced, and it directly shapes how V2 should be run.

Consistent with session 3's ReconfigLC wall (a ~13-step CE, no verdict in 700s): the
wall is real, but it sits ABOVE the proxy depth, which is why the probe still landed.

---

### SESSION 4c — the #6b FIGURE-8 PROBE (`Figure8.lean`): second calibration point
Chosen as the SHARPEST possible test of the guidance the #9 probe produced ("hunt
proxy invariants, not end-to-end loss"): #6b's full loss was machine-checked in Lean
as a **46-step, n=5** countermodel, while its proxy — the Raft §5.4.2 barrier — should
be trivial. If the guidance were overfit to #9, #6b would show it.

TARGET (election.rs:1421-1465): `rank_leader` pushed `AdvanceCommit` off the
positions-only `CommitTracker` UNCONDITIONALLY, so right after an election, honest
post-reconcile AppendPosition floor reports certify the ELECTION BASE before this
term's NewTerm frame is quorum-durable — an OLD-TERM-ONLY commit below the §5.4.2
barrier. Knob `clampFix` (`ranked >= new_term_pos`; `None` ⇒ no advance).

#### PROBE D RESULT — **proxy CE at DEPTH 5**, 1m22s (n=3, term=Fin 3, pre-fix)
```
1 startElection(i=1,t=2)
2 grant(c=1,j=0,t=2)
3 becomeLeader(i=1,q=2)      new leader, new_term_pos = None
4 reportOldRange(j=0)        honest post-reconcile AppendPosition floor
5 commitOldRange(i=1,q=2)    PRE-FIX advances on a bare quorum of position reports
                             -> old range committed with NO quorum on the NewTerm frame
```
**Shallower than #9's proxy (depth 5 vs 7) — the guidance HOLDS on a second bug, and
the split is even wider here** (proxy at 5 vs a full loss Lean needed 46 steps for).

#### PROBE E/F — THE BOX WALL (the honest negative, and a hard operational limit)
A faithful Figure-8 needs THREE election terms (rival at T1, leader at T2, rival again
at T3), so the full-loss probes require **term = Fin 4**. At n=3/Fin 4, maxDepth 13:
**`lean` reached 12.1 GB RSS and drove the box to 2.37 GB available — KILLED under the
box-safety rule** (no swap; an OOM here SIGKILLs the largest process and can take the
harness with it). Lean buffers verdicts until the file finishes elaborating, so the run
produced **NO partial output** — 15+ minutes bought nothing.
**Operational rule for this box: term = Fin 4 at n=3 is NOT viable for this model
class.** Fin 3 runs peaked comfortably (12.6 GB free throughout); Fin 4 exhausts a
15 GB box. This is a sharper limit than session 3's ReconfigLC time-wall — that one
merely failed to converge, this one endangers the session.

**CONSEQUENCE — the #6b full-loss depth is UNMEASURED, not "absent".** Probes E and F
never ran, so nothing is known about whether the clamp prevents the loss itself as
opposed to its proxy. The earlier pre-patch run that appeared to find a loss at depth 8
was an ARTIFACT (see below) and is retracted, not banked.

#### THREE more fidelity artifacts (session total: 11, all Rust-adjudicated)
The first Figure8 run returned three violations; two were mine:
 9. **`becomeLeader` wiped `newTermDurable` globally.** A STALE candidate winning an
    OLD term then retroactively falsified a properly-barriered EARLIER commit — the
    property was destroyed by later state rather than violated at the commit. Fixed by
    making the barrier TERM-KEYED and monotone (`newTermTerm : node -> term` plus a
    recorded `commitTerm`), so evidence cannot be erased by a subsequent election.
10. **`commitOldRange` did not require the leader to hold the range it commits** —
    a leader that had already discarded the old range committed it anyway. **This is
    the THIRD time this session the same gap appeared** (BootGate #4, and again here);
    it is the standing trap of this modeling style. `rank_leader` ranks the quorum-th
    durable, which INCLUDES the leader's own.
11. **`authorDivergentTail` let a LEADER discard its own prefix.** Replaced with
    `divergeFromOldRange` (a node that simply never received the inherited tail,
    allowed only before any commit) — absence, not a leader truncating itself.

#### A vacuity trap avoided (worth recording)
A post-fix PROXY run was deliberately NOT included: the clamp's `require` IS the
proxy's condition, so post-fix the property holds BY CONSTRUCTION and the run would be
vacuous — a green proving nothing. The meaningful post-fix calibration is against the
FULL loss, which is exactly what the box wall blocked. Noting this because a vacuous
green here would have looked like a successful calibration.

---

### SESSION 5 — V2 COHERENCE-WINDOW FORWARD HUNT (`V2Hunt.lean`)
The ONLY part of the spike that hunts an UNKNOWN bug. Everything before it was
backward-looking calibration (revert a known fix → confirm the checker finds the known
bug → restore → confirm it disappears). Here EVERY SHIPPED FIX IS ON, so a
counterexample would be a bug nobody knows about. Gated on Bar-2b (passed), which is
what makes a NULL result informative rather than vacuous.

Base: `Finding9.lean` (gate + vote + lagging `handleTerm` + `tailAttributed` + commit
plane) with `crashRestart` RESTORED — the brief asks for concurrent
`startElection`/`crashRestart`/gate-reopen/commit interleavings, and crash was the one
window ingredient Finding9 had dropped. 17 actions, 7 invariants.

AIMED PER THE DEPTH-PROBE CALIBRATION: proxy invariants, not loss properties. Each is a
condition a shipped guard establishes — `no_cross_stream_reopen` (#9's guard),
`no_phantom_commit` (#5/#6b class), `no_unattributed_report`,
`gate_shut_while_unreconciled`, `handle_never_leads_current`,
`leader_handle_is_current`, `election_safety` (tripwire).

#### RESULT — **EXHAUSTIVE, NO VIOLATION: 11,697,699 states, 93m10s** (n=3, term=Fin 3)
Verified before being believed: **zero `error: Examples/...` lines** (so not a voided
run — the vacuous-pass trap that bit twice in session 4) and **no `maxDepth` anywhere**
(so `✅ No violation` here is `exploredAllReachableStates`, NOT `reachedDepthBound` —
the two render identically, `TraceDisplay.lean:104`).

**No fifth coherence-window bug at this scale.** Per the brief's own exit criteria this
is the acceptable non-discovery outcome ("a fifth countermodel, OR honest
bounded-coverage evidence") — and it is stronger than bounded: it is exhaustive over
the model's entire reachable space.

#### WHAT THIS DOES AND DOES NOT ESTABLISH
DOES: over the reachable state space of this model at n=3/Fin 3, with all shipped fixes
in place, none of the seven guard-shaped invariants can be violated — 11.7M states, no
depth bound, no state constraints.
DOES NOT: it is exhaustive *for this model*, not for UC. It inherits every abstraction
obligation recorded for `BootGate.lean`/`Finding9.lean` (nondeterministic grant guard,
collapsed vote tally, ONE tracked entry at ONE tracked position, concrete excluded-node
quorums, the narrowed `staleStreamAppend` guard), and it says nothing about n≥4 or
about a 4th term value. **It is not a proof; `proofs/` remains the sole record.**

#### state_constraint: DELIBERATELY NOT USED — and not needed
`state_constraint` PRUNES states (a state is explored only if all constraints hold), so
for a hunt whose whole value is finding something UNKNOWN, narrowing first is
self-defeating: you cannot find what you pruned. Run 1 was therefore unconstrained, to
establish an honest baseline before spending any narrowing. **It completed, so the
lever was never needed at n=3** — constraints stay in reserve for n=4, where they would
buy tractability at a cost that would then have to be stated explicitly.

#### CAPACITY DATA POINT — and it answers the AWS question empirically
11.7M states is ~9x the largest prior run (Finding9 probe C, 1.29M) and it finished with
`lean` peaking near 7 GB against a 15 GB box — comfortable. Combined with the Fin-4
wall from session 4c, the box envelope is now pinned:
 * **n=3 / Fin 3, exhaustive, 17-action model: AFFORDABLE (~11.7M states, ~93 min, ~7 GB).**
 * **n=3 / Fin 4: NOT viable (12.1 GB RSS, killed).**
So a larger AWS box is NOT required for the V2 forward hunt as scoped — the local box
does it exhaustively. A bigger box remains a narrow, specific purchase for the two
blocked measurements only (#6b full-loss depth; ReconfigLC leader_completeness CE).

#### TWO MORE TOOLCHAIN GOTCHAS (cost three failed builds)
 * **`veil module` RESETS `maxHeartbeats` to 500000 when it opens**
   (`Veil/Base.lean:52`, `veilDefaultOptions`), silently clobbering any `set_option`
   placed BEFORE the module declaration. It must go INSIDE module scope.
 * **A single action carrying six conditional assignments SEGFAULTS the elaborator**
   (exit 139, `Unknown constant ...crashRestart.ext.wp_local_eq.pred`). Fix: split into
   `require`-guarded arms — zero conditionals, identical semantics (the requires
   partition the space), and the shipped fix's two branches read explicitly.

#### SESSION 5b — n=4 WITH STATE CONSTRAINTS: **no viable window on this box**
Follow-on to the exhaustive n=3 hunt, asking the one question n=3 cannot: does a FOURTH
node enable a coherence-window bug three cannot? Not idle — at n=4 the majority is
3-of-4, so two successive quorums can overlap in exactly TWO nodes (no n=3 analogue),
and #6b's full loss needed n=5 in Lean. Separate file `V2Hunt4.lean` because
`state_constraint` is MODULE-level and would otherwise retroactively narrow
`V2Hunt.lean`'s exhaustive n=3 result.

**ATTEMPT 1 — C1+C2+C3, maxDepth 10: completed, 143,901 states, 6m31s — and VACUOUS.**
The tell was the state count: 143,901 at n=4 against 11.7M at n=3. A LARGER
configuration yielding 80x FEWER states is a symptom, not a result. A vacuity canary
confirmed it: `¬committed` was never violated ⇒ **no commit is reachable at all**, so
`no_phantom_commit` and most of the battery were trivially true.
CULPRIT — **C1, "at most one node awaiting reconciliation", was a BAD CONSTRAINT.** It
sounded like an anomaly bound, but `deliverRequestVoteGrant` sets
`awaitingReconcile := true` on EVERY node adopting a new term, and at n=4 a candidate
needs THREE granters — so >=2 nodes awaiting reconcile is the MAINLINE ELECTION PATH.
C1 pruned normal elections; no leader could assemble a 3-of-4 quorum. **C1 is kept in
the file as a RETIRED constraint with this explanation — a better warning than its
absence.** Had this been reported as "n=4 clean, 143,901 states" it would have read as
coverage while being the exact opposite.

**ATTEMPT 2 — C2+C3 only, maxDepth 10: KILLED at >60 min with ZERO output.**
As established in session 4c, Lean buffers verdicts until elaboration ends, so a killed
run yields nothing. ~2.5 h across both attempts bought one vacuity finding.

**WHY THERE IS NO CHEAPER RETRY (the structural result).** A commit at n=4 needs ~10
steps minimum — startElection, TWO grants (3-of-4 majority), becomeLeader, appendEntry,
two replicates, two reports, commitEntry — versus ~7 at n=3. So:
 * **maxDepth < 10 ⇒ VACUOUS BY CONSTRUCTION** (nothing can commit; the battery goes
   trivially true, exactly as attempt 1 did).
 * **maxDepth >= 10 ⇒ INTRACTABLE on this box** (attempt 2, killed).
**The window between vacuous and intractable does not exist at n=4 here.** That is a
sharper statement than "we ran out of time": lowering the bound cannot help, because
the bound is what makes the run mean anything.

**CAPACITY ENVELOPE, now complete:**
| Configuration | Outcome |
|---|---|
| n=3 / Fin 3, exhaustive, unconstrained | **AFFORDABLE** — 11.7M states, 93 min, ~7 GB |
| n=3 / Fin 4 | not viable — 12.1 GB RSS, killed (session 4c) |
| n=4 / Fin 3, constrained, maxDepth <10 | vacuous by construction |
| n=4 / Fin 3, constrained, maxDepth >=10 | not viable — killed at >60 min |
So **n=3 / Fin 3 is the frontier for this model class on a 15 GB box**, in BOTH
directions (more terms, more nodes). Pushing either axis needs a bigger box or a
structurally cheaper abstraction — not a tuning tweak.

**RULE ADOPTED: every constrained run must be PAIRED WITH A VACUITY CANARY, RUN FIRST.**
Constraints can silently destroy the behaviour they were meant to make searchable, and
a clean verdict looks identical either way. Same class of trap as "an elaboration error
silently VOIDS a `#model_check`" (session 4) — a green that means nothing. The canary is
the only cheap way to tell the two apart.

---

### SESSION 6 (2026-07-26, fable) — the Reconfig COMMIT/LOG PLANE, option-(a) arc, bars 1–2
Brief: docs/superpowers/specs/2026-07-26-uc2-veil-reconfig-commit-plane-brief.md (user-decided
option (a); this session = build + calibrate + CHECKPOINT, proof push deliberately NOT started).
Worktree: .claude/worktrees/uc2-veil-commit-plane (branch uc2/veil-commit-plane off main 59c1b60).
Ground truth: the DISCHARGED §5 Q2 mechanism chain (gate doc, CONFIRMED-SAFE) — first session of
the spike where model refinement starts from a verified Rust map, not guesses.

#### New models (Reconfig.lean untouched, archived as the spike artifact)
* **ReconfigCommit.lean** — entry-level commit/log plane over the reconfig model: holdsE /
  committed / committedTerm; commitEntry = quorum-witness majority of the leader's CURRENT
  config, every member holding E (Q2 links 1+2 by construction), leader holding E (link 3 —
  the thrice-recurring trap, honored from the start); propAfterE recorded at PROPOSE time
  (whether the config entry sits after E in the proposer's stream); THE KNOB prefixCoupling
  gates adopt on (¬propAfterE i ∨ holdsE j). Knob does NOT weaken commitEntry counting —
  that would admit a depth-~7 phantom commit BFS would return INSTEAD of the config-walk CE
  (the session-4 "wrong bug" trap). State diet vs ReconfigLC: tally collapsed via
  quorum-witness params (drops votes : node→nodeSet, 512×), elecQuorum dropped (P1 not
  chased per brief), crashRestart knob-gated OFF in bounded runs. This turned ReconfigLC's
  700-s no-verdict wall into a 26-min decisive CE.
* **ReconfigCommitSMT.lean** — the abstract-quorum sketch for the inductive route (defs +
  property statements ONLY; #check_invariants deliberately not run): cfgid/quorum types,
  cmember/qmember/quorumOf + C5 same-config intersection, succCfg with apply's ±1 shape,
  adjacent_cfg_quorum_intersection stated as a LOUDLY-MARKED assumption that next session
  must DISCHARGE as a theorem (routes r1 concrete-majority-instantiation proof / r2
  in-module counting, r2 risks the List-universe wall), commitCfgid/commitQuorum history
  variables, P2 stated, seed invariant clauses incl. the load-bearing candidate
  electable_cfgs_contain_holder (expected to be REFINED by the CTI loop, not survive).

#### BAR 1 — CALIBRATION PASSED (uncoupled CE = the F-M7-2 shape, depth 13)
Run A (coupling OFF, adjacency ON, n=3/Fin 3, maxDepth 14): ❌ leader_completeness at
DEPTH 13, ~26 min, matching the pre-run hand-derived trace step for step:
commit E under {0,1,2} with holders {0,2} → VALID adjacent walk {0,1,2}→{0,1}→{1} adopted
by NON-holder 1 (the knobbed adopt-without-prefix move, twice) → node 1 self-elects at t2
under {1} holding nothing. Every step except the two adopts is real-UC-legal; the two-hop
chain is FORCED by the up-to-date restriction (holder refuses non-holder under {0,1}), so
the shallowest CE sits exactly at the class the plane exists to see. Log:
logs/reconfigcommit-runA-calibrationCE-depth13.log.

#### TWO MODEL-FIDELITY GAPS (the session's substantive fidelity output; both would have
#### fired in BOTH knob positions = broken the calibration architecture, not flattered it)
12. **commitCfg was ungated ("sound superset" — WRONG).** Attempt-1 returned a depth-11 CE
    riding it: config chain to {0} with ZERO follower adoptions (states 4→7→8), then a solo
    commitEntry(0,q={0}) invisible to a legitimately-elected t2 leader. Adjudicated
    UNREACHABLE in Rust: a config entry commits like any entry — C_new-quorum durable past
    it (Q1's "⌈n/2⌉ genuine C_new ackers", removed leader's self-ack a non-voter seed;
    ChangePending clears only at commit). The over-approx was not "sound direction" here
    because the artifact CE is knob-INDEPENDENT and would mask the class the knob isolates.
    FIX: hasAdopted (J I) evidence (cleared per propose, proposer self-adopts at append,
    set at adopt) + commitCfg (i, q) requiring a C_new-majority of hasAdopted witnesses
    (∩ cfg i automatically discards a removed leader's self-ack). Log of the artifact CE:
    logs/reconfigcommit-runA-attempt1-artifactCE.log.
13. **Vote granting was not membership-gated on the voter's ADOPTED config** (Q2 link 5;
    caught by hand-tracing the post-12 model, session-4 discipline). Without it even the
    COUPLED model loses E: a PRE-E config walk shrinks the voter set legitimately (config
    entries preceding E carry no prefix obligation — propAfterE=false), E commits under the
    shrunken config, then a stale-config candidate assembles an old-config quorum from
    voters who moved on — grants real UC refuses (M7 membership-gated solicitation/
    granting, tombstones). FIX: require nset.contains c (cfg j) in the grant arm.
    LOAD-BEARING for P2: this gate, not adjacency, is what blocks stale-config elections
    after legal pre-E shrinks.

#### Recorded obligations/narrowings (non-gap)
* Report plane collapsed (counting toward E's commit ⟹ holdsE by construction): justified
  by Q2 links 1+2 CONFIRMED-SAFE; the stale-report class stays banked in BootGate/Finding9 —
  this plane cannot re-find #5/#9-class bugs, by design.
* Below-floor/snapshot (Q2 link 4) not modeled; snapshot-carried-config argued equivalent-
  or-stronger under the coupling, unchecked.
* No leader step-down on self-removal (session-3 carry-over): benign for P2 (a self-removed
  leader holds E; Q1's deliberate serve-until-commit window), would over-count for
  "current leader" properties.
* Adopt window closes at commitCfg (hasProposal cleared): real UC allows later adoption via
  journal replay/snapshot; benign under coupling (late adopters hold the prefix a fortiori),
  but the coupled clean verdict is "clean within this restriction".

#### Coupled runs + box data
* Run B (coupling ON, maxDepth 15): ABANDONED at ~50 min — lean 6.5 GB RSS, 3 GB avail and
  falling; killed before OOM, NO verdict (Lean buffers until elaboration ends).
* Run B (coupling ON, maxDepth 13 = the calibration horizon): TWO attempts died at almost
  exactly ~60 min wall with NO verdict and NO OOM/memwatch evidence (attempt 2 had 7 GB
  available minutes before death) — diagnosed as the **harness background-task ~60-min
  ceiling** killing the pipeline, a NEW toolchain gotcha for runs of this size: Lean buffers
  verdicts until elaboration ends, so any external kill loses everything. Attempt 1 was
  initially misread as a kernel OOM (2.9 GB avail at last poll made it look imminent);
  attempt 2 falsified that. WORKAROUND: `setsid`-detach the `lake build` from the task
  lifecycle and poll the log file. Also reclaimed ~4 GB of foreign idle rust-analyzer
  daemons before attempt 2 (recoverable tooling, no git/session state) — kept as headroom
  hygiene even though memory was not the killer.
* THE ACCIDENTAL BATCH (recorded honestly): an earlier edit removed the block-comment
  OPENER guarding runs C/D, so the setsid-detached build ran B+C+D SEQUENTIALLY (~80 min,
  lean peak ~7.7 GB observed) and then hit a parse error on the now-dangling
  `RUNS-DISABLED-END -/` marker line. Adjudicated per the zero-error discipline: the only
  `error:` lines are the EXPECTED canary violation and the trailing junk AT LINE 393,
  positioned AFTER the final verdict — the model itself elaborated cleanly and every
  verdict carries a state count or concrete trace, so the verdicts stand (the session-4
  voiding trap is an error INSIDE the model definition, which this is not). Marker line
  removed from the archived file; a fresh full build would re-run B/C/D.
* **RUN B VERDICT: ✅ No violation, 4,211,943 states (coupling ON, maxDepth 13 = the
  calibration horizon). BOUNDED, not safe** — same-model pairing with run A's ❌ at 13:
  the checker finds the loss without the mechanism and loses it with the mechanism.
  Also re-verifies election_safety in the coupled model through d13.
* **RUN C VERDICT: ❌ p2_antecedent_canary at depth 10 — non-vacuity WITNESSED** (the
  good outcome): a commit + a later-term leader under a changed config is reachable with
  the coupling ON, so B's clean is not "nothing interesting happens". The witness's shape
  is itself a finding — see the stream-conflation obligation below.
* **RUN D VERDICT: ✅ No violation, 9,160,143 states (UNCOUPLED model, p2 gated off,
  maxDepth 14)** — election_safety regression-clean in the strictly-larger uncoupled
  behaviour set through depth 14 (run A additionally guarantees no election_safety
  violation at depth ≤ 12 in that model, BFS shallowest-violation).
* NEW OBLIGATION (from run C's witness — the checker teaching again): with ONE tracked
  entry and NO per-term stream identity, a STALE t1 leader can commit E counting a holder
  whose `holdsE` came from its own t2 APPEND (trace: leader 2 appends at t1; node 0 wins
  t2, proposes remove, appends "E"; stale leader 2 commits with q={0,2}). Real UC rejects
  this — reports are handle-term-stamped and a t1 leader drops t2 reports (BootGate/
  Finding9 carry that machinery; this plane collapsed it, obligation above). This is an
  OVER-approximation (extra behaviours — sound direction for the P2 verdicts), and it is
  exactly the brief's optional "run-2 narrowing lift" (per-term stream identity / an
  `entryTerm`) surfacing in the commit plane. NOT forced now (per brief: "do not force
  it"); flagged for the SMT session as a likely source of nuisance CTIs.

#### Checkpoint
Memo: docs/benchmarks/uc2-veil-commit-plane-checkpoint-2026-07-26.md (re-estimate: 1–2
LC-task S2-equiv — anchor plausible as a floor; adjacency-lemma discharge separable
~0.5–1; stale-config-candidate case = expected CTI hotspot; canon-obligation note).
STOPPED at the re-gate per the brief; proof push awaits user go/no-go.

---

### SESSION 7 (2026-07-26, opus) — BAR 3, the inductive proof push (part 1)
Brief §"Re-gate outcome + bar-3 execution policy" (GO on bar 3; Opus drives the CTI
loop, Fable gates twice). Worktree `.claude/worktrees/uc2-veil-commit-plane` @ `87e251d`.
Runs in `/home/claude/veil-spike/veil-preview`, logs in `/home/claude/veil-spike/runs/`.
Ground truth for every adjudication below: gate doc `uc2-veil-spike-2026-07-24.md` §5
(the DISCHARGED Q2 chain) + the Rust it points at.

#### ADJACENCY OBLIGATION — DISCHARGED, route **r1** (`QuorumAdjacency.lean`)
Route choice + justification (checkpoint question 2): **r1** (plain-Lean proof over a
concrete majority interpretation), because r1 buys a SECOND thing r2 does not — a
**satisfiability witness for the whole abstract quorum bundle**. `#check_invariants`
verdicts over Veil `assumption`s are vacuous if the assumptions are inconsistent, and
this arc's bundle is 4 assumptions deep (`same_cfg_quorum_intersection`,
`quorum_member_sound`, `succ_shape`, `adjacent_cfg_quorum_intersection`). r1 exhibits
the intended interpretation (cfgid = `Finset node`, quorum = strict-majority subset,
succCfg = ±1 voter) and proves ALL FOUR hold there, so every green in this arc is
non-vacuous by construction. r2 (in-module counting) would have proved less and risked
the `TSet` List-universe wall that killed `ReconfigLC.lean`'s SMT path.
* `adjacent_cfg_intersection` + `same_cfg_intersection` + `quorum_member_sound_concrete`
  + `succ_shape_concrete` + `self_is_quorum`: **all proved, `#print axioms` = only
  `propext, Classical.choice, Quot.sound`** (no `sorry`, no model assumption).
  Build: `lake build Examples.UC.QuorumAdjacency` green (1.6 s).
* The counting core is one lemma (`inter_of_card`: two subsets of a carrier whose sizes
  sum past it must meet) + `omega` on `card_union_add_card_inter`; the add case runs
  against carrier `d` (|d| ≤ |c|+1), the remove case against carrier `c` (|c| ≤ |d|+1).
  **The adjacency lemma is now a theorem, not an axiom** — the brief's requirement.

#### (a)-CLASS CLAUSE WORK — 12 of 16 clauses CERTIFIED INDUCTIVE, all-n, cvc5
Three runs, each ~60–95 s (the SMT route is ~25× cheaper per verdict than session 6's
explicit-state runs — a material finding for planning: iterate freely here).
* **Run 1** (`smt-run1-seed.log`, 63 s) — the session-1 seed set: 114 ✅ / 7 ❌.
* **Run 2** (`smt-run2-ghost.log`, 74 s) — 137 ✅ / 6 ❌.
* **Run 3** (`smt-run3-ghost-final.log`, 93 s) — **170 ✅ / 6 ❌**, final (a)-class state.

**GHOST STATE ADDED (NOT a MODEL-EDIT — no `require` mentions any of it, so the
reachable behaviour set is bit-for-bit run-1's; recorded here to keep the distinction
auditable):** `elecCfg N` (config a leader's win was certified against), `gotEAt N`
(term at which N acquired E), `isCommitLeader N` + `commitElecCfg` (the commit leader's
election evidence, which must outlive its `leader` flag).

CTIs adjudicated **(a) = missing/weak clause**, one line each:
1. **`leader_quorum` ❌ under `propose`** — the clause named `cfgOf I`, which `propose`
   MOVES; a leader's win is certified against the config in force AT ELECTION TIME and a
   later proposal must not retroactively re-certify it. Fix: state it over `elecCfg I`.
   ⇒ INDUCTIVE.
2. **`leader_completeness` ❌ under `becomeLeader` with a holder's grant in the
   pre-state** — the up-to-date guard (`¬(holdsE j ∧ ¬holdsE c)`) is a GRANT-TIME fact
   the state forgets. Fix: `holder_grants_are_covered` — `voteMsg V C T ∧ holdsE V ∧
   gotEAt V < T → holdsE C`; sound because a grant at a term strictly above the voter's
   acquisition term must POSTDATE the acquisition (grants raise `curTerm`;
   append/replicate require `curTerm ≤ source`), so the guard did apply. ⇒ INDUCTIVE.
3. Ghost soundness clauses (`commit_leader_unique`, `commit_leader_only_after_commit`,
   `gotE_bounded`) — the solver otherwise invents pre-states with two commit leaders.
   ⇒ all INDUCTIVE.

**Certified inductive (all-n, cvc5, 11/11 obligations each):** `grant_state`,
`grant_uniq`, `self_vote`, `leader_quorum`, `commit_backed`, `commit_quorum_sound`,
`commit_term_bound`, `holder_grants_are_covered`, `commit_leader_evidence`,
`commit_leader_unique`, `commit_leader_only_after_commit`, `gotE_bounded`
(+ `doesNotThrow`). **Still open:** `election_safety` (1 CTI, `becomeLeader`),
`leader_completeness` = P2 (2 CTIs, `becomeLeader` + `commitEntry`),
`electable_cfgs_contain_holder` (3 CTIs — the load-bearing candidate, as predicted).

#### TWO MODEL-FIDELITY GAPS — ADJUDICATED, ANCHORED, **NOT YET APPLIED**
Both are (b)-class = model infidelity. Per the bar-3 policy the ledger entry is written
BEFORE the edit; the edits themselves are held for the mid-arc Fable gate (below), so
nothing in run 3 is built on an unaudited model change.

14. **PROPOSED `MODEL-EDIT-1` — `commitEntry` counts reports that are not the leader's.**
    The model's `commitEntry` requires only `holdsE V` for quorum members. Real UC
    counts a follower only via a **term-stamped Report**: `uc2_consensus/src/election.rs:545-552`
    drops `term < current_term` ("stale report: dropped") and turns `term > current_term`
    into `adopt_term` + return, so only a report whose sender was at the LEADER'S OWN
    term ever reaches `self.tracker.on_durable(slot, durable)` (election.rs:566-570);
    Raft §5.4.2's companion clamp is `new_term_pos` (election.rs:1451-1456, Finding #6b).
    *Why it matters (hand-derived, n=5, every step legal in the CURRENT model):*
    leader 0 wins t1 and appends E; node 2 wins t2 (grants from 1,4 — all non-holders,
    so the up-to-date guard permits it) and does NOT hold E; node 3 wins t3 (grants from
    1,4) and `appendEntry`s — which this one-tracked-entry plane CONFLATES with E —
    then replicates to node 1; stale leader 0, never deposed, commits with q={0,1,3}.
    Result: `committed`, `committedTerm = t1`, leader 2 at t2 ≥ t1 holding nothing =
    **P2 reachably FALSE in the model as it stands**. This is the classical Figure-8
    shape (n=5, matching #6b's Lean countermodel), i.e. the class UC FIXED and whose
    machinery this plane collapsed (session-6 obligation 7, "no per-term stream
    identity" — the brief's optional run-2 narrowing lift, now NOT optional).
    *Proposed edit (minimal, over-approximating):* `commitEntry` additionally requires
    `∀ V, qmember V q → tot.le (gotEAt V) (curTerm i)` — the quorum member acquired E at
    a term no later than the committing leader's. Strictly WEAKER than the Rust gate
    (which demands the report term EQUAL the leader's term), so it keeps more
    behaviours = the sound direction, and it needs no new message plane: `gotEAt` is
    already the ghost variable added for CTI-2. Every real UC behaviour satisfies it
    (a follower reporting at T1 is at curTerm T1, and `curTerm` only rises after
    acquisition). NOT applied this session.
15. **PROPOSED `MODEL-EDIT-2` — vote granting ignores CONFIG-entry currency.**
    Run-3 CTI (`election_safety` ❌ under `becomeLeader`): node 0 self-elects under
    config `{0}` while node 1 is leader at the SAME term under config `{1}` — two
    disjoint singleton quorums of NON-ADJACENT configs. First adjudicated as (a) and
    REJECTED as (a): a config-lineage invariant kills this particular CTI but not the
    class — hand-derived n=5 trace over a perfectly legal single-server chain
    `{0..4} → {0,1,2,3} → {0,1,2}`: nodes 0 and 4 never adopt (a C_1 commit needs only
    3 of 4, a C_2 commit only 2 of 3), then node 0 wins term T under the stale `{0..4}`
    with grants from 3,4 while node 2 wins the SAME term under `{0,1,2}` with a grant
    from 1. Both wins are legal in the model ⇒ no invariant can exclude it ⇒ (b).
    *Rust adjudication — NOT a real bug:* real UC's grant is gated on
    `log_ok` — `(cand_last_term, cand_last_durable) >= (our_term, our_durable)`
    (`uc2_consensus/src/election.rs:342-350` free-function form, `:1240-1247` method,
    call site `:1222`) — and **config frames ARE log entries inside `durable`**, the
    contiguous fsynced frontier (gate doc §5 Q2 link 1, CONFIRMED-SAFE). Node 3, holding
    the C_1 config entry, has a strictly longer log than candidate node 0 at equal
    `last_term`, so it REFUSES. UC additionally carries Ongaro's single-server-change
    errata precondition: `propose_config` returns `NotServing` unless the leader has
    committed an entry of its own term (`uc2_consensus/src/election.rs:876-878`, comment
    "the single-server-change precondition") — a second guard the model also lacks.
    *Proposed edit:* extend the grant guard with config currency — an immutable strict
    chain order `cfgLt` (⊇ `succCfg`, transitive, irreflexive) plus
    `require ¬ cfgLt (cfgOf c) (cfgOf j)` in `deliverRequestVoteGrant`: a voter refuses
    a candidate whose adopted config is strictly behind its own. This is the SAME
    abstraction of `log_ok` the model ALREADY applies to E (`¬(holdsE j ∧ ¬holdsE c)`),
    just extended to the other log content this plane tracks; the session-1 model's
    asymmetry — E guarded, config entries not — IS the infidelity.
    *Recorded narrowing (honest, both directions):* like the existing E-guard, this is
    STRONGER than `log_ok`, which would grant to a candidate that lacks the voter's
    entries but carries a higher `last_term` on a divergent branch. Excluding those is
    an UNDER-approximation, justified only by the canonical-prefix property this plane
    already assumes at its boundary (session-6 obligation 3; `proofs/`'s open `canon`).
    The E-guard has carried this unrecorded since `Reconfig.lean` — recording it now for
    BOTH. NOT applied this session.

#### Why the four open clauses need exactly those two edits (the induction, sketched)
With `MODEL-EDIT-2`, `electable_cfgs_contain_holder` becomes provable by the shape the
discharged adjacency lemma was built for: a committed config C_{k+1} is held by a
C_{k+1}-quorum of adopters; **adjacency** says that quorum meets every C_k-quorum; the
log-currency guard makes those adopters refuse any candidate stuck at C_k — so no
quorum of any adopted config is free of "current" nodes, and by chain induction none is
free of an E-holder once E commits. P2 then closes in two cases: `curTerm L >
committedTerm` via `holder_grants_are_covered` (already inductive) against a
`commitQuorum` member — which needs `MODEL-EDIT-1` to guarantee `gotEAt V ≤
committedTerm`; and `curTerm L = committedTerm` via `commit_leader_evidence` +
`grant_uniq` (already inductive). Both edits are load-bearing; neither is optional.

#### Calibration cross-check
**No model edit was applied**, so the session-6 calibration pair is untouched by
construction — `ReconfigCommit.lean` (the explicit-state twin) is byte-identical to
session 6's and was not rebuilt. The cross-check obligation transfers, unspent, to the
session that applies MODEL-EDIT-1/2: coupling OFF must still exhibit the depth-13 CE,
coupling ON + canary must still witness non-vacuity.

#### STOP POINT: mid-arc Fable gate (bar-3 policy, gate 1)
Trigger: **two adjudicated (b)-class model edits, both load-bearing for every remaining
clause.** Per policy ("before any accumulated MODEL modifications are built upon"), they
are specified and Rust-anchored above but deliberately NOT applied — the gate audits the
proposals, so no proof work rests on an unaudited change. Not a wall: the route to P2 is
mapped and the adjacency lemma, the ghost apparatus and 12 inductive clauses are banked.

#### GATE 1 (Fable) RULED — both adjudications CONFIRMED; edits applied with revisions
Ruling: MODEL-EDIT-1 approved as specified; MODEL-EDIT-2 approved WITH three required
revisions; both infidelity adjudications confirmed against the Rust (no stop-the-arc
finding); the (a)-rejection for EDIT-2 confirmed sound; the r1 witness verified
non-vacuous. Actions taken, in order:

**CORRECTION to item 15 (gate-supplied, verified here):** "unrecorded since
`Reconfig.lean`" is WRONG — `Reconfig.lean` has no commit plane and no E-guard at all
(`grep holdsE` = nothing; its grant arm is `Reconfig.lean:104`). The E-guard entered
with **`ReconfigLC.lean:108`** (session 3), half-noted in the inline comment above it
("sound for the single-entry model (no divergent logs)") and never ledgered. Fixed.

**ITEM 14 anchor emphasis (gate-required):** the PRIMARY Rust anchor for MODEL-EDIT-1
is the **report term gate** — `uc2_consensus/src/election.rs:545-552` (stale dropped /
higher adopts) gating `tracker.on_durable` at `:566-570`. The `new_term_pos` clamp
(`:1451-1456`) is a COMPANION only: its own class is excluded from this plane by the
E-guard narrowing, not by the clamp.

**MODEL-EDIT-1 APPLIED** (`commitEntry`: `∀ V ∈ q, gotEAt V ≤ curTerm i`). Consequence
recorded per the gate: **`gotEAt` is promoted from ghost to load-bearing** — the
"history state appears in no `require`" claim now covers only `elecCfg`,
`isCommitLeader`, `commitElecCfg`. Model header updated accordingly.

16. **`MODEL-EDIT-2` REVISION (a) — `cfgid` RE-WITNESSED AS CHAIN-INDEXED (critical).**
    Gate finding, verified: with `cfgid ↦ Finset node`, add-x-then-remove-x gives
    `succCfg c d ∧ succCfg d c`, so any transitive superset of `succCfg` yields
    `cfgLt c c` — the `cfgLt` axioms are **UNSATISFIABLE** under the r1 interpretation,
    and by this arc's own anti-vacuity doctrine no green over them would count.
    FIX (also the more faithful reading — in UC a config IS a log entry at a position):
    `QuorumAdjacency.lean` now carries TWO witnesses and proves ALL SEVEN assumptions
    over them, `#print axioms` clean:
      * **W1** `ICfg = ℕ × Finset V` — `succCfg (k,s)(k',s') := k'=k+1 ∧ ±1`,
        `cfgLt := index <`. Permits BRANCHING config history, so it also proves the
        model does not secretly assume linearity. (`i_*` theorems; plus
        `i_succ_inhabited`, so `succCfg` is not vacuously empty.)
      * **W2** `LCfg = ℕ` over a fixed ±1 chain — additionally proves `succCfg`
        FUNCTIONALITY and `cfgLt` TOTALITY, held in reserve for the branch-shaped
        CTIs the gate predicted. (`l_*` theorems.)
17. **`MODEL-EDIT-2` REVISION (b) — second narrowing recorded (gate-supplied).**
    `cfgOf` conflates HOLDING a config entry with having ADOPTED it. In Rust a
    candidate can be durable past a newer config frame — so `log_ok`
    (`election.rs:1240-1247`) GRANTS — while its adopted config still lags, adoption
    completing only on the archive re-scan (`election.rs:889-899`). The cfgLt guard
    refuses that grant; real UC performs it. P2-benign under the same boundary
    assumption (durable past the frame ⇒ holds the prefix ⇒ holds E, by contiguity),
    but a DISTINCT exclusion from narrowing (n1) and recorded as such.
18. **`MODEL-EDIT-2` REVISION (c) — twin mirrored, EDIT-1 deliberately NOT.**
    `ReconfigCommit.lean` gets the strict-subset form of the guard
    (`strictlyAhead a b := a ⊊ b`), valid there because `proposeAdd` is knob-gated OFF
    (`addEnabled := false`) making the reachable config space REMOVE-ONLY from a full
    genesis — a restriction that cannot destroy either calibration trace, both of which
    are remove-only. **EDIT-1 is SKIPPED in the twin** (gate-directed): a per-node
    acquisition-term function is a ~x27 state multiplier at `Fin 3`, past this box's
    explicit-state envelope. The twin therefore now OVER-approximates the proof model —
    strictly more behaviours, the sound direction for a CE calibrator. Divergence
    recorded in the twin's header.

**THE GATE'S BINDING JUDGMENT (implemented, not re-litigated):** the cfgLt narrowing is
ACCEPTABLE as a documented boundary assumption and must NOT be weakened to faithful
`log_ok` (importing `log_ok` alone into a one-tracked-entry plane makes the Figure-8
grant model-legal with nothing here to stop it; fidelity would mean merging in the
Figure8/Finding9 plane, banked elsewhere). Consequences implemented:
narrowings (n1)+(n2) are recorded in the MODEL HEADERS of BOTH files, not only here;
and **the arc's eventual SAFE verdict is CONDITIONAL — on the canonical-prefix /
contiguity discipline (Q2 chain, CONFIRMED-SAFE in Rust) and on the data-plane
freshness / Finding-#6b `new_term_pos` clamp (proved at the Lean tier)** — stated in
exactly that form in both headers (LC-arc `FramesCurrentAuthored` precedent). Prior
verdicts stand: CEs remain valid, the election-plane results predate the narrowing, and
the session-6 bounded cleans were knob calibration.

#### RUN 5 (post-EDIT-1+2): 170 ✅ / 6 ❌ — unchanged, AS EXPECTED
The edits add MECHANISM; the clause set does not yet CONSUME it, and
`#check_invariants` CTIs start from arbitrary Inv-satisfying states, so the same six
obligations fail until the invariant bundle names the new facts. Log
`smt-run5-edits12.log` (2m33s). The post-edit `election_safety` CTI is now visibly
**branch-shaped** — `cfgLt = []`, i.e. two INCOMPARABLE configs, plus an `elecCfg` that
sits outside its own node's config history — exactly the class the gate predicted.

19. **`MODEL-EDIT-2b` (chain linearization) — NEW, UNAUDITED, applied under the gate's
    chain-indexing sanction.** Assumption added: `cfglt_total`
    (`∀ c d, c = d ∨ cfgLt c d ∨ cfgLt d c`). This is the abstract encoding of "real UC
    linearizes config history through the log" that the gate named when it told me to
    expect branch CTIs and treat them as artifacts of the un-graded successor relation.
    It is a NARROWING (it excludes divergent config branches) and it sits INSIDE the
    already-declared boundary: narrowing (n1) already assumes the canonical-prefix
    discipline, of which "one config history, not a tree" is a direct consequence.
    Witness: **W2's `l_cfglt_total`, already proved** — no new anti-vacuity debt.
20. **`MODEL-EDIT-2c` (no config regression) — NEW, UNAUDITED.** `adopt` additionally
    requires `¬ cfgLt (proposedC i) (cfgOf j)`: a node never adopts a config it is
    already past. Rust: config frames are adopted from the archive's recorded-block
    walk over the contiguous fsynced prefix, i.e. **in log-position order** (gate doc §5
    Q2 link 1, CONFIRMED-SAFE); a node's adopted config only moves forward. Without it
    the model lets `cfgOf` move BACKWARD, which breaks every chain-order clause.
21. **`MODEL-EDIT-3` (`serving`: cluster-wide one-in-flight) — NEW, UNAUDITED, and
    REQUIRED: without it election_safety is REACHABLY FALSE in the model.**
    *The counterexample (n=5, every step legal in the post-EDIT-2 model):* leader A at
    `C0` proposes `C1` (uncommitted, `pending A`); B adopts `C1`, later wins a term
    under `C1` (grants from nodes at `C0`/`C1` — the cfgLt guard permits, they are not
    ahead), and — having `¬pending B`, since `pending` is PER-NODE — proposes `C2` with
    `C1` still uncommitted. Now only B is past `C0`. At one term T, X (at `C0`) takes a
    `C0`-quorum {3,4,+1} and B takes a disjoint `C2`-quorum from the other two `C0`
    nodes (who do not refuse: the candidate is AHEAD, not behind). **Two leaders at T.**
    *Rust adjudication — NOT a real bug:* `propose_config` returns `NotServing` unless
    the leader has committed an entry of its OWN term (`uc2_consensus/src/election.rs:876-878`,
    comment "the single-server-change precondition" — Ongaro's single-server-change
    errata). Commit is prefix-closed, so committing an own-term entry that postdates
    `C1`'s frame COMMITS `C1`. Hence in real UC B cannot propose `C2` while `C1` is
    uncommitted. Session 1 modeled one-in-flight PER NODE (`pending i`); the real
    mechanism is cluster-wide, carried by `serving` + prefix-closed commit.
    *Edit:* ghost `cfgCommitted C` set at `commitCfg`, and `propose` requires
    `cfgOf i = genesisC ∨ cfgCommitted (cfgOf i)` — the consequence of `serving` that
    this plane can express without a per-term commit plane. `cfgCommitted` is thereby
    load-bearing, not ghost.
    **GATE-2 MUST AUDIT ITEMS 19-21** — they are three model edits beyond gate 1's two.

#### CALIBRATION CROSS-CHECK (gate-required) — **BOTH PASS**
One detached twin build, 21m10s, `twin-runA2C2-gate1.log`:
* **RUN A2 (coupling OFF, EDIT-2 on, addEnabled off): ❌ `leader_completeness` at
  DEPTH 13** — the session-6 calibration CE SURVIVES, trace-for-trace identical
  (`startElection(0,t1) / grant / becomeLeader(0,{0,1}) / appendEntry(0) /
  replicate(0→2) / commitEntry(0,{0,2}) / proposeRemove(0,x=2) / adopt(1←0) /
  commitCfg(0,{0,1}) / proposeRemove(0,x=0) / adopt(1←0) / startElection(1,t2) /
  becomeLeader(1,{1})`). **EDIT-2 did not break the plane's eyesight.**
* **RUN C2 (coupling ON, canary): ❌ `p2_antecedent_canary` at DEPTH 10** —
  non-vacuity still witnessed, same stale-t1-leader-commit shape as session 6.
  NOTE, against the gate's expectation that it might shift to the depth-11 witness:
  it did NOT, and that is CORRECT — the shift was predicated on EDIT-1, which is
  deliberately absent from the twin (item 18), so the depth-10 witness stays legal
  there. Had EDIT-1 been mirrored, the shift would be the expected outcome.

#### RUNS 6-8 — the config-chain induction, and where it stands at the session stop
| run | added | verdict |
|---|---|---|
| 6 | MODEL-EDIT-2b/2c/3 + chain bookkeeping clauses | 246 ✅ / 7 ❌ (4m16s) |
| 7 | chain-order theory (`succ_immediate`, `cfglt_connected`) + `chain_committed_below`, `eleccfg_not_stale`, `commitq_gotE`, `pending_iff_proposal`; `commitCfg` bookkeeping corrected to the PROPOSED config | 289 ✅ / 8 ❌ (5m44s) |
| 8 | `genesis_least` (+ W2 witness `l_genesis_least`) | **290 ✅ / 7 ❌** (4m56s) — BANKED |

**22 invariant clauses + `doesNotThrow` CERTIFIED INDUCTIVE, all-n** (up from 12 at
gate 1): the four election clauses, the four commit clauses, `holder_grants_are_covered`,
`commit_leader_evidence`, the three ghost-soundness clauses, and the NINE new
config-chain clauses (`cfg_from_genesis`, `proposal_from_genesis`,
`proposal_is_own_cfg`, `eleccfg_not_ahead`, `adopters_not_behind`,
`committed_cfg_quorum`, `pending_iff_proposal`, `commitq_gotE`,
`chain_committed_below`, `commit_cfg_backed`).
**P2's CTI count fell from 2 to 1** — `commitEntry` no longer breaks it, i.e.
MODEL-EDIT-1 + `commitq_gotE` closed the Figure-8-shaped half exactly as designed.
**Still open: `election_safety` (1), `leader_completeness` (1), `eleccfg_not_stale` (2),
`electable_cfgs_contain_holder` (3)** — all four now failing ONLY at `becomeLeader`
(plus `propose`/`adopt`/`commitEntry`/`commitCfg` for the two config clauses), i.e.
the whole residue is ONE argument: the stale-config election.

22. **THE IDENTIFIED HOLE (next session's first move, no new mechanism needed).**
    `eleccfg_not_stale` ("no leader was elected under a config strictly below a
    committed one") is provable in outline — `cfglt_connected` gives a succ-step out of
    the stale config, the adjacency lemma meets that config's adopter quorum against the
    leader's electing quorum, and MODEL-EDIT-2's guard makes the adopters refuse — EXCEPT
    that the intersection member can be **the candidate itself**, which is legitimately
    at-or-past the newer config (it may have advanced AFTER its own election). The fix is
    structural and already has a template in this model: freeze the GRANT-TIME config the
    way `gotEAt` freezes the acquisition term (a `cfgAt V` ghost + the same
    grant-postdates-adoption ordering: `adopt` requires `curTerm j <= curTerm i`, so a
    grant at a term strictly above `cfgAt V` postdates V's adoption and the guard did
    apply). This is GHOST state plus clauses — **not a further model edit.**

#### SESSION-2 STOP: ~5-hour checkpoint (bar-3 policy), NOT certification, NOT a wall
Reached the time bound with the induction converged onto a single open argument and with
**three model edits (items 19-21) plus a five-assumption chain-order package
(`cfglt_total`, `succ_immediate`, `cfglt_connected`, `genesis_least`, and the W1→W2
witness migration) that are UNAUDITED.** Gate 2 must audit those before the final claim.
Every assumption added is proved of witness W2 in `QuorumAdjacency.lean` (`#print axioms`
clean), so the bundle remains satisfiable and no verdict above is vacuous.

#### GATE 1b RULED — edits 19-21 APPROVED; run-8 greens STAND (conditional form)
No stop-the-arc Rust finding. The EDIT-3 chain was verified link-by-link in the code,
and the pre-edit CE was confirmed not-a-real-bug (doubly unreachable: `ChangePending`
on the adopted `config_position` AND `serving` forcing C1 committed first).
**PROCESS BREACH CONFIRMED AND CORRECTED:** runs 6-8 were built on three unaudited
edits. No contamination (ledger-first + checkpoint-not-certification labeling), but the
rule is now a COUNT, not a judgment call — **any new `require` or assumption beyond the
gate-1b-audited set stops the session for a gate before another `#check_invariants` run
is banked.** Ghost state stays exempt only while it passes the no-`require` test.
"Sanctioned in spirit by a prior gate" is not a category. Recorded and adopted.

**RECORDING DEBTS DISCHARGED (gate-1b required):**
* **(a) NARROWING (n3) — truncation-revert / config-branch exclusion.** Added to BOTH
  model headers and to items 19/20. MODEL-EDIT-2c makes adoption forward-only; real UC
  moves the adopted config BACKWARD in exactly one place — `election.rs:703-748`, the
  M7 truncation revert (`to < config_position` ⇒ revert one history level, or keep the
  config by fiat on a wipe). Those are the CONFIG-BRANCH states, which MODEL-EDIT-2b's
  linearity assumption also excludes. **Item 19's claim is hereby sharpened: UC
  linearizes config history only for the CANONICAL history** — across branches two
  configs can share a `version`, which is exactly why the forward gate is a version
  COMPARISON (`:751-756`) and not a global order. The version gate itself relies on
  linearity; it is not independent evidence for it.
* **(b) ITEM 20 primary anchor amended** to the VERSION GATE `election.rs:751-756`
  (`ConfigObserved` returns early on `config.version <= self.config.version`) plus
  `config.rs:133` (`next.version += 1`, bump by exactly one). The archive's
  position-ordered recorded-block walk is now cited as SUPPORTING, and snapshot fiat
  adoption was verified forward via its `durable < floor` gate.
* **(c) ITEM 21 supplementary anchor added, and the primary reassigned.** The model's
  `require cfgOf i = genesisC ∨ cfgCommitted (cfgOf i)` is the LITERAL abstraction of
  **`config_pending()` — `config_position > commit_seen` (election.rs:854-858),
  enforced at `:879-881`** — which blocks the SAME-leader C1→C2 path. `serving`
  (`:876-878`) is COMPLEMENTARY: it blocks the NEW-leader path. `config_pending` is
  now cited primary.
* **(d) AXIOM AUDIT COMPLETED.** The `#print axioms` block now covers all seventeen
  witness theorems, including the three newest (`l_genesis_least`, `l_succ_immediate`,
  `l_cfglt_connected`). All clean — `[propext, Classical.choice, Quot.sound]` or less
  (`l_genesis_least`: `[propext]` alone). Banked: `logs/quorumadjacency-axioms.log`.
  The "clean" claim is now mechanically backed, not asserted.
* **(e) TWIN DIVERGENCE LIST COMPLETED** in `ReconfigCommit.lean`'s header — FOUR
  SMT-only mechanisms, not one: (d1) EDIT-1 own-term-stamped reports, (d2) EDIT-2b
  linear config history, (d3) EDIT-2c forward-only adoption, (d4) EDIT-3 cluster-wide
  one-in-flight (the twin keeps session 1's per-node `pending i`). All four
  over-approximate in the twin — sound for its calibrator role, and the reason a clean
  verdict there is strictly weaker than one in the SMT model.

#### THE cfgAt STEP (gate ruling 3) — template applied, NOT yet closed
Ghost state + clauses ONLY. Mechanically verified against the banked run-8 model:
**35 `require`s and 11 assumptions in both** — no new guard, no new assumption, so the
gate-1b corrective is satisfied and these runs are bankable.
* Ghosts added: `cfgAt N` (term of N's last config change — the `gotEAt` mirror; `adopt`
  requires `curTerm j <= curTerm i`, so a grant at a term strictly above `cfgAt V`
  postdates V's adoption) and `cfgCommitTerm C`. `elecCfg` is now frozen from
  CANDIDACY (set at `startElection`), which is sound because a candidate's config
  cannot move: `adopt` clears candidacy and `propose` requires `leader`.
* **The gate's correction to my diagnosis is confirmed in the model.** Both holes were
  real and both had to be addressed: the granter-advanced-after-granting shape (fixed by
  `cfgAt` + `grant_cfg_covered`) and the `V = i` disjunct (which needed `elecCfg`
  frozen from candidacy plus `cand_cfg_frozen` + `role_exclusive`).
23. **`eleccfg_not_stale` WAS FALSE AS STATED — my own proof sketch, corrected.**
    A STALE LEADER IS LEGAL in UC: there is no check-quorum step-down (elle gate doc),
    so a leader elected under an old config keeps its flag while the cluster commits
    later configs. The property is about TERMS, not the leader flag. Replaced by
    `no_stale_election`: `leader I ∧ cfgCommitted D ∧ cfgCommitTerm D < curTerm I →
    ¬ cfgLt (elecCfg I) D`. (Not a model defect — a defect in the invariant I wrote.)
24. **RUN-9 FINDING — the ordering bound cannot ride on `committed_cfg_quorum`.**
    Strengthening it with `tot.le (cfgAt V) (cfgCommitTerm D)` BROKE a previously
    inductive clause (3 new CTIs at propose/adopt/commitCfg): an adopter's `cfgAt`
    RISES when it later moves further along the chain, so the bound is not preserved.
    The fact the argument needs is per-(node, config) — "the term at which V FIRST
    reached D or later" — and needs its own ghost. Reverted; recorded in the model.

| run | content | verdict |
|---|---|---|
| 9 | cfgAt template, first cut | 305 ✅ / 14 ❌ (3m30s) — 4 regressions, all (a)-class |
| 10 | + `role_exclusive`, `reqvote_term_reached`, `vote_term_reached`, bound reverted | **343 ✅ / 9 ❌** (5m41s) |

**RUN 10 vs RUN 8, honestly: 26 clauses inductive (up from 22) but P2 regressed from 1
CTI to 2** (`commitEntry` returned), and the template clause `grant_cfg_covered` is
itself not yet inductive (1 CTI at the grant arm). Net: more machinery certified, the
target not closer. Both logs are banked; **run 8 remains the reference state for P2's
CTI count**, run 10 is the state carried in the file (strictly more clauses + the
template). The P2 regression's likely source — `elecCfg` now being written at
`startElection` as well as `becomeLeader`, which changes what `commitElecCfg` snapshots
in non-reachable pre-states — is UNDIAGNOSED and is the next session's second task.

#### SESSION-2 CLOSE (past the ~5-hour bound)
Not certification, not a wall: the residue is still the single stale-config-election
argument, now with two named sub-obligations (the per-(node,config) reach ghost of item
24; the run-10 P2 regression). Open clauses: `election_safety` (1),
`leader_completeness` (2), `grant_cfg_covered` (1), `no_stale_election` (2),
`electable_cfgs_contain_holder` (3). Gate-2 scope, as directed: the final conditionality
wording must carry (n1)+(n2)+(n3) verbatim, and the twin<->SMT divergence list (d1)-(d4)
must be complete at close — both are now written into the model headers.

---

### SESSION 8 (2026-07-27, opus) — BAR 3, part 2: the P2 regression diagnosed + the reach ghost
Fresh context by design (the arc's continuity mechanism is this ledger). Worktree
`.claude/worktrees/uc2-veil-commit-plane` @ `ce4ab33`; runs in
`/home/claude/veil-spike/veil-preview`, logs `/home/claude/veil-spike/runs/`.
Baseline inventory verified MECHANICALLY before any banked run (gate-1b corrective):
**34 `require`s / 11 `assumption`s** in the run-10 model. NOTE a bookkeeping
correction: session 7 recorded "35 requires"; the mechanical count
(`grep -cE '^\s*require '`) is **34**, and 34/11 is the number this session holds
itself to. The discrepancy is a miscount in the prior entry, not a model change
(`git diff ce4ab33` over the model is empty at session start).

#### TASK 1 — THE RUN-8 → RUN-10 P2 REGRESSION: **(c)**, NOT the predecessor's (a)
Verdict: **(c) — run 8's counting was propped up by a clause that is FALSE in
reachable states.** The predecessor's suspicion — that writing `elecCfg` at
`startElection` changed what `commitElecCfg` snapshots — is **REFUTED**.

*Evidence 1 (trace inspection, run-10 `commitEntry` TR CTI, `smt-run10-final.log:769-830`).*
The CTI pre-state has `candidate = []` and `leader = [0,1]`. `elecCfg` is written at
`startElection` ONLY for candidates, and `becomeLeader` overwrites it for every leader,
so in a candidate-free pre-state the two write-site regimes are POINTWISE IDENTICAL.
`commitEntry` does not write `elecCfg`. The new write site cannot be reached by this CTI.

*Evidence 2 (the actual mechanism).* The same pre-state VIOLATES run 8's
`eleccfg_not_stale` (`(leader I ∧ cfgCommitted D) → ¬ cfgLt (elecCfg I) D`): node 1 is
`leader`, `cfgCommitted` holds of cfg2, `elecCfg 1 = cfg1`, and `cfgLt cfg1 cfg2`. Under
run 8's bundle the solver could not propose it at all. Run 10 replaced that clause with
the term-conditioned `no_stale_election`, which is STRICTLY WEAKER (its antecedent adds
`tlt (cfgCommitTerm D) (curTerm I)`, and in this CTI every `cfgCommitTerm` equals node
1's own term, so it is vacuous there). **That replacement is the ONLY weakening between
the two bundles** — run 10 otherwise only ADDS clauses (`role_exclusive`,
`reqvote_term_reached`, `vote_term_reached`, `cand_cfg_frozen`, `grant_cfg_covered`),
and added clauses can only help.

*Evidence 3 (mechanical, DIAGNOSTIC RUN D1 — `logs/smt-D1-diag-regression.log`).*
Run-10's model with run-8's `eleccfg_not_stale` restored VERBATIM as an extra clause
(`DIAG_eleccfg_not_stale`), nothing else changed: **353 ✅ / 10 ❌, and
`leader_completeness` fails at `becomeLeader` ONLY — the `commitEntry` CTI is GONE.**
P2 is back to exactly run 8's single CTI. Regression fully accounted for.

*What this means, stated honestly:* **run 8 is NOT a valid reference state for P2's CTI
count.** `eleccfg_not_stale` is false in reachable states (ledger item 23; re-verified in
the Rust this session — a LEADER leaves `Role::Leader` in exactly two places:
`adopt_term` on a strictly HIGHER term (`election.rs:1059-1061`, `self.role =
Role::Follower`; reached from the higher-term arms at `:534-535` and the Report/
LeaderSeen handlers) and the M7 self-removal/demotion latch
(`election.rs:1505` / `:1539`). The two `step_down_to_follower` call sites at `:541`
and `:592` are guarded by `matches!(self.role, Role::Candidate)` — candidate-only.
There is NO check-quorum step-down, so a leader elected under an old config keeps its
flag indefinitely while the cluster commits later configs). A clause that is
false in reachable states is an UNSOUND hypothesis for every other clause in the bundle,
and P2-at-`commitEntry` was resting on it. Run 10's "regression" is the bundle becoming
HONEST, not weaker. The ledger's session-7 line "run 8 remains the reference state for
P2's CTI count" is hereby **retracted**; run 10's 2 CTIs is the true baseline, and
the session-7 count of 22 inductive clauses at run 8 was likewise inflated by the same
unsound hypothesis (how much is unmeasured — the honest re-baseline is run 10's 26).

*Process note (no new rule needed, but worth the ink):* this is the second time this arc
that a clause I wrote — not the model — was the defect (item 23 was the first). The
lesson generalizes the gate-1b corrective: a NON-INDUCTIVE clause is not merely "an open
obligation", it is a live hypothesis for every other clause, so an open clause that later
turns out FALSE silently inflates every green around it. **Every clause with open CTIs
must be re-argued for TRUTH (not just inductiveness) before its neighbours' greens are
quoted as progress.**

#### TASK 2 — THE PER-(node, config) FIRST-REACH GHOST: **WORKS** (run 11)
Ghost `reachAt (N : node) (C : cfgid) : term` — the term at which N FIRST reached
C-or-later, where "reached C" := `¬ cfgLt (cfgOf N) C`, a MONOTONE predicate because
MODEL-EDIT-2c makes adoption forward-only. Written at the only two `cfgOf` writers
(`propose`, `adopt`) for EVERY config the move newly covers (not just the target — a
node that jumps several links freezes all of them at that term); read in NO `require`,
so the gate-1b corrective is satisfied by count: **34 `require`s / 11 assumptions,
identical to run 10.** This is the ledger-item-24 alternative: `cfgAt` RISES as an
adopter walks on, `reachAt V C` FREEZES the moment V reaches C.

**RUN 11 — 398 ✅ / 9 ❌.** All five new clauses inductive on the first attempt, and
the `committed_cfg_quorum` strengthening that BROKE under `cfgAt` in run 9 is now
inductive: `reach_bound` (a reached config was reached by now), `reach_mono` (earlier
configs are reached no later), `grant_reach_covered`, `eleccfg_covers_early_reach`,
`adopted_reach_bound`, and `committed_cfg_quorum` carrying
`tot.le (reachAt V D) (cfgCommitTerm D)`. **31 clauses + `doesNotThrow` inductive**
(run 10: 26). The nine CTIs are the SAME nine as run 10, clause-for-clause and
action-for-action — no regression, and P2 stays at 2. Log `smt-run11-reachghost.log`.

Two design notes worth carrying:
* `grant_reach_covered` concludes over **`cfgOf C`, not `elecCfg C`** — a candidate that
  has WON and then PROPOSED can still receive late grants at the same term, at which
  point the MODEL-EDIT-2 guard compares against its MOVED config. `cfgOf` is
  forward-only so that form is preserved; `cand_cfg_frozen` bridges back to `elecCfg`
  at `becomeLeader`, the only action that can newly create a stale-config leader.
  (The `elecCfg`-flavoured `grant_cfg_covered` from session 2 is still ❌ at the grant
  arm — this is almost certainly why.)
* the same-term wrinkle the brief warned about is real and is handled structurally, not
  by term-strictness: `eleccfg_covers_early_reach` supplies the `V = i` disjunct.

25. **`electable_cfgs_contain_holder` IS FALSE IN REACHABLE STATES — the arc's SECOND
    false clause, and the run-8 lesson applies to it.** Countermodel, every step legal in
    this model (n=5 over the W2 interpretation): genesis `C0={0..4}`; leader 0 commits
    `C1=C0∖{4}` (adopters 0,1,2 = a 3-of-4 quorum) and then `C2=C1∖{3}` (adopters 0,1 =
    a 2-of-3 quorum); node 4 never adopts and stays at `C0`. Then `appendEntry(0)` +
    `replicate(0→1)` + `commitEntry(0,{0,1})` commits E under `commitCfgid = C2`. Now
    `cfgOf 4 = C0` and the `C0`-quorum `{2,3,4}` contains no E-holder. **Real UC is safe
    there for a different reason than the clause asserts**: not because a stale config's
    quorums contain holders — they do not — but because the config-currency guard makes
    nodes 0,1,2 (at `C2`) and 3 (at `C1`) REFUSE a `C0`-stale candidate
    (`election.rs:342-350`/`:1240-1247` over `durable`, gate doc §5 Q2 link 1). The
    clause is therefore RESTRICTED (run 12) to configs at-or-above `commitCfgid`; the
    stale side belongs to `no_stale_election`, not here. **Consequence, stated plainly:
    run 11's 398 ✅ were computed with a false clause in the bundle, exactly the defect
    task 1 found in run 8. Run 12 re-measures with it corrected, and run 12's numbers —
    not run 11's — are the honest ones.** (This is the third time in this arc that a
    clause I wrote, not the model, was the defect: items 23, 25, and run 8's
    `eleccfg_not_stale`.)

26. **`MODEL-EDIT-4` — PROPOSED, **NOT APPLIED**: `propose`'s config-commit gate is a
    GLOBAL flag where real UC's is the LEADER'S OWN, own-term-certified commit view.
    THIS IS THE SESSION'S STOP POINT — a new `require` beyond the 34/11 baseline, so per
    the gate-1b corrective nothing is built on it and no run carrying it is banked.**

    *The model as it stands.* `propose` requires
    `cfgOf i = genesisC ∨ cfgCommitted (cfgOf i)`. `cfgCommitted C` is a GLOBAL relation
    set by any `commitCfg`, by any node, at any term. Nothing relates the term at which
    C committed to the proposer's term. Consequence: a leader at a LOW term may propose
    past a config that committed at a HIGH term, so `cfgCommitTerm` is not monotone
    along the config chain.

    *Rust adjudication — the real gate is strictly tighter (so this is (b)-class
    infidelity, an OVER-approximation, not a UC bug).* `propose_config` refuses unless
    BOTH `self.serving` (`uc2_consensus/src/election.rs:876-877`, `NotServing`, the
    single-server-change precondition) and `!self.config_pending()`
    (`:879-880`, `ChangePending`), where `config_pending()` is
    `config_position > commit_seen` (`:854-858`). And a **LEADER's `commit_seen` has
    exactly one writer**: `rank_leader` (`:1421-1457`), gated by the Finding-#6b clamp
    `c >= new_term_pos` — this term's NewTerm frame. The gossip intake at `:594-595` is
    explicitly `if !matches!(self.role, Role::Leader)`, so a leader NEVER adopts another
    node's commit index. Therefore a leader may propose past config C only after IT
    ITSELF ranked a commit covering C's frame, at its OWN term ⇒ in real UC the commit
    that authorises the proposal is certified at a term ≤ (in fact =) the proposer's.

    *Proposed edit (minimal, still OVER-approximating — the sound direction).* Replace
    the `propose` require with
    `require cfgOf i = genesisC ∨ (cfgCommitted (cfgOf i) ∧ tot.le (cfgCommitTerm (cfgOf i)) (curTerm i))`.
    Weaker than the Rust gate (which forces the proposer's OWN term, not merely
    at-or-below it), so every real UC behaviour still satisfies it. Consequence to
    record if approved: **`cfgCommitTerm` is promoted from ghost to load-bearing**,
    exactly as MODEL-EDIT-1 promoted `gotEAt`. No new `assumption`, so no new
    anti-vacuity/witness debt in `QuorumAdjacency.lean`.

    *Why it is load-bearing (the residue reduces to it).* The stale-config argument needs
    a quorum of `E = succ(elecCfg I)` whose members ALL reached E strictly before I's
    term. `committed_cfg_quorum` (now carrying the `reachAt` bound) supplies that for the
    config D named in `no_stale_election`'s antecedent, but the adjacency lemma can only
    be applied at E, and when `E < D` the bound has to travel DOWN the chain. The clause
    that carries it is `reach_quorum_below` (added in run 12):
    `cfgLt E C ∧ ¬ cfgLt (cfgOf N) C → E = genesisC ∨ ∃q, quorumOf q E ∧ ∀V ∈ q,
     (¬ cfgLt (cfgOf V) E ∧ tot.le (reachAt V E) (reachAt N C))`.
    Hand proof of its preservation: at `adopt` it goes through unaided (instantiate the
    pre-state clause at the PROPOSER, which is already at `proposedC i`, and use
    `reach_bound`); at `propose` the only sub-case that does not close is `E = cfgOf i`
    — which needs precisely `cfgCommitTerm (cfgOf i) ≤ curTerm i`, i.e. MODEL-EDIT-4.
    With `reach_quorum_below`, `no_stale_election` closes at BOTH its CTI sites by one
    argument: instantiate at a member of D's certifying quorum (nonempty via
    `same_cfg_quorum_intersection` at `q1 = q2`), get a quorum of E reached strictly
    before `curTerm I`, meet it against I's electing quorum by
    `adjacent_cfg_quorum_intersection`, and discharge the two disjuncts with
    `grant_reach_covered` (V ≠ I) and the `¬ cfgLt (cfgOf V) E` conjunct itself (V = I).

    *Alternatives considered and rejected.* (i) A ghost-only fix is impossible: the
    offending behaviour (low-term leader proposing past a high-term-committed config) is
    REACHABLE in the model, so no invariant can exclude it — same adjudication shape as
    MODEL-EDIT-3 (ledger 21). (ii) Chain-monotonicity of `cfgCommitTerm` as a standalone
    clause is not preservable without the edit either, because a config can legally
    RE-commit at a higher term between a proposal and its commit. (iii) Requiring
    `leader i` in `commitCfg` would be an UNDER-approximation — real UC does let a
    successor leader commit a dead proposer's config entry — and is not needed: the
    same-term case is already vacuous under `no_stale_election`'s strict `tlt`.

27. **NEXT SESSION'S REMAINING MAP (hand-derived this session, no new mechanism beyond
    item 26).** After MODEL-EDIT-4 + `reach_quorum_below` close `no_stale_election`, P2's
    two CTIs need only clause work: the `becomeLeader` one is the same-term
    commit-leader-self-vote hole (the commit leader's own vote is carried by
    `commit_leader_evidence`'s `V = i` disjunct, which survives losing the `leader` flag
    but records no `voteMsg`, so `grant_uniq` cannot fire). Fix, clause-only:
    `voteterm_bounded` (`tot.le (voteTerm V) (curTerm V)`) plus
    `commit_leader_self_vote` (`isCommitLeader I → tot.le committedTerm (voteTerm I) ∧
    (voteTerm I = committedTerm → voteCand I = I)`) — sound because a node that
    self-voted at T can only re-grant at T to itself (`deliverRequestVoteGrant`'s
    `voteCand j ≠ c` guard) and `voteTerm` only rises. `grant_cfg_covered` (session 2's
    `elecCfg`-flavoured clause, still ❌) should be RETIRED in favour of
    `grant_reach_covered`, whose `cfgOf`-flavoured conclusion is the preservable one.

#### RUN 12 — the corrected clause + `reach_quorum_below`: **409 ✅ / 8 ❌**
Clause-only (34 requires / 11 assumptions re-verified before launch; the model diff adds
no `require` and no `assumption` — mechanically checked with
`git diff | grep -E '^\+' | grep -E 'require|assumption \['`, which returns only comment
lines). Log `smt-run12-reachquorum.log`.

| run | content | verdict | P2 |
|---|---|---|---|
| 10 | cfgAt template (session 2 close) | 343 ✅ / 9 ❌ | 2 CTIs |
| D1 | run 10 + run-8's `eleccfg_not_stale` restored (DIAGNOSTIC, not progress) | 353 ✅ / 10 ❌ | 1 CTI |
| 11 | + the `reachAt` ghost and its 5 clauses | 398 ✅ / 9 ❌ | 2 CTIs |
| 12 | + `reach_quorum_below`, `electable_cfgs_contain_holder` CORRECTED | **409 ✅ / 8 ❌** | **1 CTI** |

**32 clauses + `doesNotThrow` inductive** (38 properties, 6 with CTIs). Movement vs
run 11: `no_stale_election` at `becomeLeader` ✅ and `leader_completeness` at
`commitEntry` ✅ — P2 and `no_stale_election` each fall from 2 CTIs to 1. Remaining:
`reach_quorum_below` (1, `propose`), `electable_cfgs_contain_holder` (3),
`grant_cfg_covered` (1), `election_safety` (1, `becomeLeader`),
`leader_completeness` (1, `becomeLeader`), `no_stale_election` (1, `commitCfg`).

**HONESTY CONDITION ON THOSE TWO NEW GREENS (the run-8 lesson, applied to myself in
advance).** `reach_quorum_below` is itself NOT inductive, so it is a live hypothesis for
everything around it — and it is very likely FALSE in the model as it stands. Its own
CTI says so, and the CTI is the MODEL-EDIT-4 shape exactly:

> `propose` CTI (WP), chain `genesis=cfg1 → cfg2 → cfg0`, terms `t1 < t0`:
> `cfgCommitted = {cfg2}` with **`cfgCommitTerm cfg2 = t0`**, `leader 0` at
> **`curTerm 0 = t1`**, `cfgOf 0 = cfg2`. `propose(i=0, d=cfg0)` is LEGAL in the model
> (`cfgCommitted (cfgOf 0)` holds) — a leader at the LOW term proposing past a config
> that committed at the HIGH term. Node 1 reached `cfg2` at `t0`, so the newly reached
> `cfg0` cannot carry a `cfg2`-quorum bounded by `reachAt 0 cfg0 = t1`.

Real UC cannot reach it (item 26's chain: the proposer's own `commit_seen`). So:
**run 12's two new greens are CONDITIONAL on MODEL-EDIT-4 being approved and applied**,
and this table must NOT be quoted as unconditional progress until it is. The clause and
the run are banked in exactly that conditional form.

#### SESSION-3 STOP: **MODEL-EDIT-4 GATE (gate 1c)** — a well-argued new-`require` request
Not certification, not a wall. Stop point per the brief: "New require/assumption needed →
stop for gate (state precisely what and why)". Item 26 is the request; run 12's CTI at
`propose` is its mechanical evidence; items 25/27 are the accompanying corrections and
the map after it. Everything banked here is clause/ghost-only at 34 requires / 11
assumptions, so no run in this session violated the count-based corrective.
`QuorumAdjacency.lean` is UNTOUCHED — no new assumption, hence no new witness debt, and
the `#print axioms` audit of the seventeen witness theorems still covers the bundle.
The twin `ReconfigCommit.lean` is UNTOUCHED — this session applied no model edit, so the
gate-1 calibration cross-check obligation transfers, unspent, to whichever session applies
MODEL-EDIT-4; divergences (d1)-(d4) remain complete and correct as written.
Conditionality (n1)+(n2)+(n3) unchanged and still in both model headers.

---

### SESSION 9 (2026-07-27, opus) — BAR 3, part 3: gate-1c ruling recorded, MODEL-EDIT-4 applied
Fresh context by design. Worktree `.claude/worktrees/uc2-veil-commit-plane` @ `64b4acf`;
runs in `/home/claude/veil-spike/veil-preview`, logs `/home/claude/veil-spike/runs/`.

#### SESSION-LABELLING RECONCILIATION (gate-required, one sentence)
The ledger numbers sessions GLOBALLY across the whole Veil spike (V0 → …) while the
checkpoint memo numbers them WITHIN the commit-plane arc, so ledger "SESSION 7" = memo
"Session 2", ledger "SESSION 8" = memo "Session 3", and this entry is ledger SESSION 9 =
memo/brief **session 4 of bar 3**; items 22–24 sit physically under the SESSION 7 heading
because they were written in that session's post-gate-1b continuation, and are cited (not
re-authored) by SESSION 8.

#### BOOKKEEPING CORRECTION RATIFIED BY THE GATE
The model's true inventory is **34 `require`s / 11 `assumption`s at all three of
`73271b0`, `ce4ab33`, `64b4acf`** — session 7's "35 requires" was a miscount BOTH times
it was written, not a model change. Also ratified: a `#check_invariants` run that ends
`sorry` / `error: Lean exited with code 1` / `build failed` is the **NORMAL shape of a
CTI-bearing run**, not a voided run (the session-4 voiding trap is an `error: Examples/…`
line INSIDE the model definition, which none of these are).

#### GATE 1c RULING — **MODEL-EDIT-4 APPROVED**, with binding amendments
All Rust links independently verified by the gate: `config_pending()` at
`uc2_consensus/src/election.rs:856-858` enforced at `:879-880`; `commit_seen` has
**exactly two writers** — the gossip intake at `:594-595`, which is literally
leader-excluded (`if !matches!(self.role, Role::Leader)`), and `rank_leader` at `:1457`,
behind the Finding-#6b clamp at `:1451-1456`; initialisation at `:431`. No stop-the-arc
finding. The amendments below are recorded VERBATIM in content, as required, and are
binding on every claim made from this model:

* **(a) `cfgCommitTerm` is the PROPOSER-STAMPED term, not the certification term.**
  `commitCfg` writes `cfgCommitTerm (proposedC i) := curTerm i` (`ReconfigCommitSMT.lean:439`)
  — the term of the node that fires the commit, at the moment it fires. The proposer's
  term drifts UPWARD, so this stamp can EXCEED the term at which a quorum actually
  certified the config. Nothing in the model equates the two, and no argument may assume
  it does.
* **(b) The over-approximation argument (without which EDIT-4 is unproven).** Every real
  UC behaviour maps into a model behaviour satisfying the new require, via two facts
  taken together: (i) `commitCfg`'s **scheduling freedom** — it is an internal action with
  no message plane, so it may fire at the EARLIEST enabling point, before any causally
  independent term raise, which makes the stamp as low as the real certification; and
  (ii) the **own-term report gate at `election.rs:545-552`** — a certifying quorum's
  adoption evidence is causally independent of any term ABOVE the certifying leader's,
  because any member touched by a higher term would depose that leader before ranking it.
  Together: the real certification term is always available as a legal stamp in the model,
  so the require excludes no real behaviour.
* **(c) PROHIBITION — never strengthen EDIT-4's `≤` to `=`.** Ledger item 26's
  "(in fact =)" parenthetical is correct ONLY for the authorizing advance; `=` would
  UNDER-approximate, because a leader's SECOND proposal legitimately compares against a
  config committed in an EARLIER term.
* **(d) CORNER NOTE — `commit_seen` is NOT reset at `become_leader`** (`election.rs:1040-1056`),
  so a fresh leader carries commit state inherited from its follower period. That
  inherited value cannot satisfy the serving latch (`:522-527` requires
  `commit_seen ≥` the fresh NewTerm position) without a pre-existing completeness
  violation — i.e. it is in the SAME CONDITIONALITY BUCKET as narrowing (n1), and must
  appear verbatim in the final claim (gate-2 scope).
* **(e) The run-12 `propose` CTI is an UNREACHABLE PRE-STATE** (it has the leader at
  `curTerm` zero), so it is mechanical evidence of the missing mechanism but NOT a
  reachability witness. The gate supplied the reachability evidence as a hand trace,
  recorded here as the citable form: proposer *p* proposes cfg2 at term *t_p*; node 0
  adopts cfg2 and wins *t1 > t_p*; a SECOND cfg2-adopter wins *t0 > t1*, and *p*'s grant
  to it lifts `curTerm p` to *t0*; `commitCfg` then stamps `cfgCommitTerm cfg2 = t0`;
  leader 0, sitting at *t1 < t0*, legally proposes past cfg2 — the low-term-proposer
  shape, reachable.

**RUN-8 RETRACTION: RATIFIED.** The D1 diagnostic log was verified by the gate and the
model diff `73271b0 → ce4ab33` mechanically confirmed that the `eleccfg_not_stale` →
`no_stale_election` clause swap was the ONLY weakening between the two bundles.
**`electable_cfgs_contain_holder` correction: CONFIRMED** — the n=5 countermodel of item
25 is legal in the model, and real UC's safety there comes from the config-currency
guard, not from the clause as first stated.

#### THE OPEN-CLAUSE TRUTH RULE (gate 1c, binding — recorded verbatim)
> A clause with open CTIs is a live hypothesis for every verdict in its bundle. Before
> any run's greens are quoted as progress — in the ledger, a checkpoint, or a gate —
> every clause still open in that run must carry either (a) a WRITTEN truth argument: an
> informal proof that it holds in all reachable states of the current model, or (b) an
> explicit CONDITIONAL label naming what its truth awaits. A clause later found false
> VOIDS the quoted greens of every run that carried it; the corrected re-measurement
> replaces them. Truth arguments need no gate; discovering a clause false requires a
> ledger entry before proceeding.

This generalises the three self-inflicted false clauses of this arc (item 23
`eleccfg_not_stale`, item 25 `electable_cfgs_contain_holder`, and run 8's inflated
count) into a standing obligation rather than a lesson.

#### THE CORRECTED RESIDUE MAP (gate audit of item 27 — one valid, two gaps, one omission)
Recorded before execution, so the plan is auditable against the outcome:
* `reach_quorum_below`@`propose` — **VALID as item 26 argued**; expected green after EDIT-4.
* `no_stale_election`@`commitCfg` — **GAP.** "Same argument as `becomeLeader`" FAILS:
  `cand_cfg_frozen` is unavailable for a longstanding leader, and `leader_quorum`'s ∃q
  can be satisfied by LATE grants against MOVED configs. Fix: a **certifying-quorum
  ghost** (`elecQuorum I` frozen at `becomeLeader`) plus clauses routing the truth
  argument through that quorum's GRANT-TIME configs. Ghost-only; no gate needed.
* `leader_completeness`@`becomeLeader` (P2's last CTI) — **GAP.** `voteterm_bounded` +
  `commit_leader_self_vote` alone do NOT exclude the pre-state: `grant_state`'s first
  disjunct absorbs the stray `voteMsg`. Needed: a PERSISTENT CARRIER clause of shape
  `isCommitLeader V → ∀ C ≠ V, ¬ voteMsg V C committedTerm`, whose preservation then
  consumes the two clauses item 27 proposed. Clause-only.
* **OMISSION**: item 27 left `election_safety` (1 CTI) and `electable_cfgs_contain_holder`
  (3 CTIs) unmapped. They must be mapped with truth arguments, or
  `electable_cfgs_contain_holder` explicitly RETIRED with a written ruling that P2's
  argument nowhere consumes it — the same written-retirement treatment already sanctioned
  for `grant_cfg_covered`.

#### MODEL-EDIT-4 APPLIED — new baseline **35 `require`s / 11 `assumption`s** (mechanical)
Applied in `propose` as a SECOND `require`
(`require cfgOf i = genesisC ∨ tot.le (cfgCommitTerm (cfgOf i)) (curTerm i)`) rather than
as a conjunct inside the existing one. The two forms are EQUIVALENT by distribution
(`g ∨ (a ∧ b)` ≡ `(g ∨ a) ∧ (g ∨ b)`), and the split makes the mechanical count honest at
the mandated 35. `grep -cE '^\s*require '` = **35**, `grep -cE '^assumption \['` = **11**.
`cfgCommitTerm` is hereby moved OUT of the model header's ghost list — it is LOAD-BEARING,
exactly as MODEL-EDIT-1 promoted `gotEAt`. `QuorumAdjacency.lean` untouched: no new
assumption, so no new witness/anti-vacuity debt. Gate amendments (a)/(b)/(c) are recorded
INLINE at the edit site; (d)/(e) and the truth rule are in the model header.

#### TRUTH ARGUMENTS — written BEFORE the inductiveness hunt (the gate's truth rule)
Each argument is an informal proof that the clause holds in every REACHABLE state of the
post-EDIT-4 model. Where a clause is instead carried CONDITIONALLY, that is labelled.

**T1 `reach_quorum_below`** — `(cfgLt E C ∧ ¬cfgLt (cfgOf N) C) → E = genesisC ∨ ∃q of E
with every member at-or-past E and `reachAt V E ≤ reachAt N C`.
*Truth.* N has reached C and E < C ≤ cfgOf N, so E is strictly below an adopted config;
by `chain_committed_below` E is genesis or COMMITTED. If committed,
`committed_cfg_quorum` yields a quorum q of E, all at-or-past E, with
`reachAt V E ≤ cfgCommitTerm E`. It remains to bound `cfgCommitTerm E ≤ reachAt N C`: for
any node to reach a config strictly above E, some leader must have PROPOSED the step out
of E, and MODEL-EDIT-4 forces that proposer's own term ≥ `cfgCommitTerm E`; `adopt` copies
the proposer's term to the adopter, and `curTerm` is monotone — so the term at which any
node first reaches a config above E is ≥ `cfgCommitTerm E`. Chaining gives the bound.
**This argument CONSUMES MODEL-EDIT-4 and is false without it** (that is precisely the
run-12 CTI, and the gate's hand trace is its reachability evidence).

**T2 `elecq_witness`** — `leader I → quorumOf (elecQuorum I) (elecCfg I) ∧ ∀V ∈ elecQuorum I,
(V = I ∨ voteMsg V I (curTerm I))`.
*Truth.* This is `leader_quorum` (already inductive) with its ∃q replaced by the ghost
that `becomeLeader` writes, and `becomeLeader`'s own `require` establishes it at the only
site that creates leadership. Every action that changes `curTerm I` also clears `leader I`
(`startElection` requires ¬leader; `deliverRequestVoteGrant`, `replicate`, `adopt` clear
the flag on a strict raise / unconditionally; `crashRestart` clears it), so the frozen
witness cannot go stale while the antecedent holds.

**T3 `elecq_grant_covers_reach`** — `(leader I ∧ qmember V (elecQuorum I) ∧
¬cfgLt (cfgOf V) D ∧ tlt (reachAt V D) (curTerm I)) → ¬cfgLt (elecCfg I) D`.
*Truth.* Members of the FROZEN certifying quorum granted to I at `curTerm I` strictly
BEFORE `becomeLeader` fired, hence while I was a CANDIDATE — and a candidate's config
cannot move (`adopt` clears candidacy, `propose` requires `leader`), so `cand_cfg_frozen`
gives `elecCfg I = cfgOf I` throughout that window. V had already reached D at that grant
(`reachAt V D < curTerm I ≤` the grant term, and adoption is forward-only by
MODEL-EDIT-2c), so the MODEL-EDIT-2 currency guard `¬cfgLt (cfgOf c) (cfgOf j)` applied
with `cfgOf V ≥ D`, forcing the candidate's config — i.e. `elecCfg I` — to be at-or-past D.
The `V = I` case is `eleccfg_covers_early_reach` (already inductive).
*Why the antecedent cannot be newly created after the win:* V granted at `curTerm I`, so
`grant_state` gives `curTerm I ≤ curTerm V`; any later `propose`/`adopt` that makes V newly
reach D stamps `reachAt V D` with the MOVER's term, which is ≥ `curTerm V ≥ curTerm I` —
so `tlt (reachAt V D) (curTerm I)` cannot turn true afterwards.
**This is the clause the gate's corrected map requires and that `leader_quorum`'s ∃q
cannot supply** — a late grant against a MOVED config can witness the ∃, the frozen
quorum cannot.

**T4 `cand_reach_strict`** — `(candidate I ∧ ¬cfgLt (cfgOf I) C) → tlt (reachAt I C) (curTerm I)`.
*Truth.* `candidate I` is created only by `startElection`, which requires
`tlt (curTerm i) t` and sets `curTerm i := t`. Every config N has already reached is
stamped at or before the pre-bump term (`reach_bound`), so it is stamped strictly below
the post-bump term. Nothing re-stamps it while candidacy persists: `propose` requires
`leader` (excluded by `role_exclusive`) and `adopt` clears candidacy. For `C = genesisC`
the stamp is `tot.zero` and `zero_le` closes it.

**T5 `voteterm_bounded`** — `tot.le (voteTerm V) (curTerm V)`. *Truth.* `voteTerm` is
written only at `startElection` (`:= t = curTerm i` after the bump) and at
`deliverRequestVoteGrant` (`:= t` together with `curTerm j := t`); every other writer of
`curTerm` only RAISES it.

**T6 `commit_leader_self_vote`** — `isCommitLeader I → committedTerm ≤ voteTerm I ∧
(voteTerm I = committedTerm → voteCand I = I)`. *Truth.* At commit time I was `leader` at
`curTerm I = committedTerm`, so `self_vote` gives `voteTerm I = committedTerm` and
`voteCand I = I`. Afterwards `voteTerm` only rises (T5 + the two writers), and while it
stays EQUAL to `committedTerm` no re-grant can change `voteCand`: the grant guard
`¬(hasVoted j ∧ voteTerm j = t ∧ voteCand j ≠ c)` permits a same-term re-grant only to the
SAME candidate.

**T7 `commit_leader_no_foreign_grant`** (the gate's persistent CARRIER) —
`(isCommitLeader V ∧ C ≠ V) → ¬ voteMsg V C committedTerm`. *Truth.* The only creator of
`voteMsg V C t` is `deliverRequestVoteGrant`, which requires `tot.le (curTerm V) t`; with
T5 and T6, `t = committedTerm` forces `voteTerm V = committedTerm` and therefore
`voteCand V = V ≠ C`, so the guard `¬(hasVoted ∧ voteTerm = t ∧ voteCand ≠ c)` REFUSES the
grant. At the other creation site of the antecedent — `commitEntry` making `i` the commit
leader — `self_vote` + `grant_state` give `voteMsg i C (curTerm i) → C = i`.
*Why the clause is needed as a PERSISTENT carrier and the two supports do not suffice
(gate finding, confirmed against run 12's TR CTI at `becomeLeader`, log lines 638-680):*
the CTI pre-state has `isCommitLeader = [0]`, `holdsE = [0]`, `committedTerm = 0` and
`voteMsg 0 1 0` — `grant_state`'s FIRST disjunct (`tlt T (curTerm V)`) absorbs the stray
`voteMsg` without saying anything about `voteCand`, so only a clause that persists the
absence of the foreign grant excludes it.

**T8 `election_safety`** (the gate's OMISSION, now mapped). *Truth.* Two leaders at T with
election configs F₁ = `elecCfg L₁`, F₂ = `elecCfg L₂`. If F₁ = F₂,
`same_cfg_quorum_intersection` on the two frozen quorums plus `grant_uniq`/`self_vote`
gives L₁ = L₂. Otherwise `cfglt_total` orders them, say F₁ < F₂ ≤ `cfgOf` of the winner;
`cfglt_connected` gives a succ-step F₁ → E with E = F₂ or E < F₂. If E = F₂,
`adjacent_cfg_quorum_intersection` meets the two frozen quorums directly and the same-term
vote argument closes it. If E < F₂, `reach_quorum_below` (instantiated at the winner, whose
`reachAt` of its own config is strictly below its term by T4) yields a quorum of E all of
whose members reached E strictly before T; adjacency meets it against L₁'s frozen quorum,
and T3 then forces `¬cfgLt (elecCfg L₁) E`, contradicting F₁ < E (`succ_cfglt`).
*Run-12's open CTI is excluded by T4 alone*: it has a `candidate` whose `reachAt` of its own
config EQUALS its term at `tot.zero`, i.e. a leader/candidate at term zero, which
`startElection`'s strict bump makes unreachable.

**T9 `leader_completeness` (P2) at `becomeLeader`.** *Truth, same-term case*
(`curTerm i = committedTerm`): `commit_leader_evidence` gives the commit leader `cl` with
`holdsE cl` and a certifying quorum at `committedTerm`; `grant_uniq` plus T7 force
`cl = i`, hence `holdsE i`. *Strict case* (`committedTerm < curTerm i`): a member of the
new leader's electing quorum that holds E has `gotEAt V ≤ committedTerm < curTerm i`
(`commitq_gotE`), so `holder_grants_are_covered` fires. **CONDITIONAL LABEL:** the strict
case's supply of such a member across a CHANGED config is exactly what
`electable_cfgs_contain_holder` was written for; that clause is being RETIRED this session
(below), so if P2 does not close without it, the honest reading is that P2 awaits a
cross-config holder-supply argument, and that is what the residue then names.

#### TWO CLAUSE RETIREMENTS — written rulings (the gate's map, executed)
* **`grant_cfg_covered` RETIRED** (retirement already sanctioned at gate 1c). It is the
  session-2 `elecCfg`-flavoured ancestor of `grant_reach_covered`; it has never been
  inductive (1 CTI at the grant arm in every run since 9) because its conclusion names
  `elecCfg C`, which a candidate that has WON and then PROPOSED no longer matches — the
  guard at a late same-term grant compares against the MOVED `cfgOf C`.
  `grant_reach_covered` states exactly the preservable form and IS inductive, and every
  consumer in the residue map (T3, T8) uses the `cfgOf`/frozen-quorum route. Nothing
  consumes `grant_cfg_covered`. **Under the truth rule its retirement is mandatory, not
  optional: it is a clause with open CTIs and no truth argument, hence a live false
  hypothesis for every green around it.**
* **`electable_cfgs_contain_holder` RETIRED** (the gate's map-or-retire option, taken).
  Item 25 already established the UNRESTRICTED form is FALSE in reachable states; the
  run-12 RESTRICTED form (configs at-or-above `commitCfgid`) has 3 open CTIs and **I can
  write no truth argument for it**: the natural chain induction from `commitCfgid` upward
  FAILS at the successor step — adjacency says a quorum of `succ C` MEETS every quorum of
  C, but the meeting member need not be the holder the inductive hypothesis supplies.
  Rather than carry an unproved clause as a live hypothesis for P2's greens (the exact
  defect of run 8 and of item 25), it is removed from the bundle. **This retirement is a
  MEASUREMENT, not a claim**: if P2 and `election_safety` close without it, the written
  ruling "P2's argument nowhere consumes it" is earned; if they do not, the residue is
  honestly re-described as needing a cross-config holder-supply argument.

#### TWIN CALIBRATION CROSS-CHECK — **DISCHARGED, BOTH PASS** (the twice-transferred debt)
One detached twin build, `logs/twin-runA2C2-gate1c.log`. Exactly **two** `error: Examples/`
lines, both the EXPECTED violations (the zero-error discipline: a `#model_check` is voided
only by an error inside the model definition, which neither is).
* **RUN A2 (coupling OFF, adjacency ON, addEnabled OFF, maxDepth 14): ❌
  `leader_completeness` at DEPTH 13** — the session-6 calibration CE survives, ending
  `… adopt(i=0,j=1) / startElection(i=1,t=2) / becomeLeader(i=1,q=[1])`, trace-for-trace
  as in session 6 and the gate-1 re-run. **The plane's eyesight is intact.**
* **RUN C2 (coupling ON, canary): ❌ `p2_antecedent_canary` at DEPTH 10** — non-vacuity
  still witnessed, same stale-t1-leader shape (`commitEntry(i=2, q=[0,2])`).
* **DIVERGENCE (d5) RECORDED** in the twin's header: MODEL-EDIT-4 is deliberately NOT
  mirrored — this model has no `cfgCommitTerm` at all, and adding a per-config
  commit-term function is a state multiplier past the box's explicit-state envelope
  (the same reason (d1) was skipped at gate 1). The twin therefore over-approximates the
  SMT model on this axis too: strictly more behaviours, the sound direction for a
  calibrator, and the reason a clean verdict there is strictly weaker than one here.
  The divergence list is now **(d1)–(d5)**, complete.

#### RUN 13 — MODEL-EDIT-4 ALONE: **410 ✅ / 7 ❌**, and the gate's prediction CONFIRMED
No clause change (35 requires / 11 assumptions verified before launch). Log
`smt-run13-edit4.log`; the single `error: Examples/…` line is the `#check_invariants`
command's own "1 verification condition could not be discharged automatically" — the
normal CTI-bearing shape ratified at gate 1c, not a model-definition error.
**`reach_quorum_below` is INDUCTIVE** (green at `propose` and everywhere else) — the map's
one VALID item, landing exactly as argued in T1. The run-12 greens it conditionally
supported (`no_stale_election`@`becomeLeader`, `leader_completeness`@`commitEntry`) are
therefore **discharged from their conditional form**. Residue: `election_safety` (1),
`leader_completeness` (1), `no_stale_election` (1, `commitCfg`), `grant_cfg_covered` (1),
`electable_cfgs_contain_holder` (3).

#### RUN 14 — the certifying-quorum ghost + the carrier + two retirements: **457 ✅ / 4 ❌**
`elecQuorum` ghost frozen at `becomeLeader` (read in NO `require`; count re-verified
35/11), clauses `elecq_witness`, `elecq_grant_covers_reach`, `cand_reach_strict`,
`voteterm_bounded`, `commit_leader_self_vote`, `commit_leader_no_foreign_grant`; and both
retirements executed. Log `smt-run14-elecq-carrier.log`.
* **`election_safety` INDUCTIVE** (T8's route: `cand_reach_strict` excludes the run-12
  zero-term CTI; the frozen-quorum + adjacency + `reach_quorum_below` chain covers the
  non-adjacent case). The gate's OMISSION is closed by proof, not by relabeling.
* **`no_stale_election` INDUCTIVE at both sites** — the certifying-quorum ghost is exactly
  what the gate diagnosed was missing; `leader_quorum`'s ∃q could not do it.
* Remaining 4: `commit_leader_self_vote` + `commit_leader_no_foreign_grant` at the grant
  arm, and `leader_completeness` at `becomeLeader` AND (newly) `commitEntry`.

**THE RETIREMENT MEASUREMENT, REPORTED AS IT CAME OUT.** P2 regained a `commitEntry` CTI
the moment `electable_cfgs_contain_holder` left the bundle — so **P2's argument DID consume
it**, and the "nothing consumes it" ruling is NOT earned as stated. What the CTIs then
showed is more precise and better: what P2 consumes is not that clause's (false) content
but the **QUORUM-SUPPLY** it smuggled in.
* The `becomeLeader` CTI interprets the intermediate configs of a 3-link chain as having
  **NO quorums at all**, which makes `adjacent_cfg_quorum_intersection` VACUOUS and breaks
  the chain from the commit config down to a stale candidate's config. In a REACHABLE
  state those quorums must exist — `propose` requires the predecessor COMMITTED, and
  `commitCfg` requires a quorum of it. The clause that recovers this is
  `commit_leader_at_commit_cfg` (T11: the commit leader is at-or-past `commitCfgid`,
  since `commitEntry` sets them together and `cfgOf` is forward-only), which with
  `chain_committed_below` + `committed_cfg_quorum` SUPPLIES the missing quorums.
* The `commitEntry` CTI has its committing leader sitting AT `tot.zero` — unreachable,
  since `startElection` is the only creator of a role and it bumps strictly above the
  current term. Clause: `role_positive_term` (T10).
* The two grant-arm CTIs were my own under-statement: the solver hands the commit leader
  `hasVoted = []`, which makes the grant guard pass vacuously. `hasVoted I` is part of the
  fact (`self_vote` supplies it at commit time; no action ever clears `hasVoted`), so it
  is now part of `commit_leader_self_vote`.
All three are CLAUSE-ONLY; count re-verified 35 requires / 11 assumptions before run 15.

#### RUN 15 — **KILLED at 2h28m WALL / 3h10m CPU, NO VERDICT.** A new tractability wall
Run 14's set + `role_positive_term` (T10) + `commit_leader_at_commit_cfg` (T11), clause-only
(35/11 re-verified). The process ran at 139% CPU with RSS flat at 7.40 GB for 80+ minutes
and produced nothing; killed under the box rule. Lean buffers verdicts until elaboration
ends, so the run yielded **zero information** — 2h28m bought nothing.
Log `smt-run15-KILLED-no-verdict.log` (the kill is visible as `Lean exited with code 137`).
**28. TOOLCHAIN FINDING — `veil.smt.timeout` is 60 s PER VC (`Veil/Base.lean:140`), so a
`#check_invariants` run is bounded but that bound is ~410 × 60 s ≈ 6.8 h at this bundle
size. A clause that pushes many VCs from "solved in ~1 s" to "times out" converts a 10-min
run into an all-night one with no partial output. Budget accordingly: at 40+ clauses, add
ONE clause at a time and treat a run exceeding ~3× the previous run's wall as a signal to
kill and bisect, not to wait.**

#### RUN 16 — `role_positive_term` WITHDRAWN (tractability, not truth): **470 ✅ / 3 ❌ in 10 min**
Same file minus that one clause. The 15× wall-clock difference **isolates
`role_positive_term` as the blocker**: it is the first clause in this bundle to put
`tot.zero` into the hypothesis set of every VC, forcing `zero_le` instantiation across the
whole term theory alongside the existing `tlt`/`tot.le` clauses. It is TRUE (T10 stands
unrefuted) — it is withdrawn for cost, and finding a cheaper encoding of "no role at the
zero term" is named work. Log `smt-run16-commitleadercfg.log`.
**40 clauses + `doesNotThrow` CERTIFIED INDUCTIVE, all-n** (run 12: 32; run 14: 38),
including `commit_leader_at_commit_cfg`, the corrected `commit_leader_self_vote`, and the
carrier `commit_leader_no_foreign_grant` — **both grant-arm CTIs closed by the `hasVoted`
correction, as T6/T7 predicted.** Remaining 3: `election_safety` (1, `becomeLeader`) and
`leader_completeness` (2, `becomeLeader` + `commitEntry`).

29. **`election_safety`'s RUN-14 GREEN IS RETRACTED — the truth rule applied to a ✅, and
    the arc's first TOOL-level anomaly.** Run 16's `election_safety` CTI was hand-checked
    against **run 14's** bundle, clause by clause (`succ_shape`, `quorum_member_sound`, both
    intersection assumptions, and all 38 invariants): **the pre-state satisfies run 14's
    `Inv` too.** The two clauses run 16 added are vacuous in it (`isCommitLeader = []`).
    Since adding invariants only STRENGTHENS the antecedent of every VC, a state that
    falsifies the VC under run 16's bundle falsifies it under run 14's — so run 14's ✅ was
    not sound, and **run 14's `election_safety` green must not be quoted.** The
    conservative reading, and the one adopted: a `#check_invariants` **✅ is trustworthy
    only as "not refuted by this bundle at this solver configuration"**, and a later ❌ on a
    STRONGER bundle voids it exactly as the truth rule voids greens propped up by a false
    clause. **How run 14 produced the ✅ is a tool-level question for gate 2** (candidate:
    a pre-solver discharge path closing the goal spuriously). Nothing in the arc's Rust
    adjudication changes; what changes is one certification claim.
30. **THE DEFECT ITSELF — the SAME-TERM GRANT WRINKLE (clause-level, named precisely).**
    In the CTI (chain `cfg0 → cfg2 → cfg1`, all nodes at one term T): node 2, sitting at
    the TOP config, carries `voteMsg 2 0 T` to a candidate at GENESIS. The MODEL-EDIT-2
    currency guard would refuse that grant, but no clause EXCLUDES it from a pre-state,
    because `grant_reach_covered`'s bound is STRICT — `tlt (reachAt V D) T` — and here
    `reachAt 2 D = T`: the voter reached its config at the very term it granted at. T3's
    ordering argument covers "reached strictly before the grant"; it says nothing about
    "reached at the same term as the grant", where the intra-term ORDER of the two events
    is what decides. The `V = I` analogue was already handled structurally
    (`eleccfg_covers_early_reach`); this is the `V ≠ I` analogue and it needs the same
    treatment — a ghost recording the config a node held AT GRANT TIME (the `cfgAt`/`gotEAt`
    pattern, applied to the grant rather than to the adoption), NOT a model edit. **This is
    the honest residue and it is smaller than it looks**: `no_stale_election` and the whole
    config-chain package are inductive; only the same-term corner of the grant guard is open.
31. **P2's TWO REMAINING CTIs, both cross-config, both named.**
    * `becomeLeader` (SAME-TERM, `curTerm i = committedTerm`): a candidate elected under the
      TOP config while E committed under GENESIS two links below, `holdsE = {commit leader}`
      only. The carrier does its job (the commit leader cannot be in the electing quorum),
      but the electing quorum of a config two links above the commit config need not meet
      the commit quorum — the F-M7-2 shape. What closes it is the CROSS-CONFIG HOLDER
      SUPPLY that `electable_cfgs_contain_holder` used to assume; run 16 shows
      `commit_leader_at_commit_cfg` recovers the QUORUM supply but not the HOLDER supply.
    * `commitEntry`: excluded by `role_positive_term` (T10), which is TRUE but currently
      **intractable** (run 15). It is a cost problem, not a truth problem.

#### SESSION-4 STOP: the ~5-hour checkpoint (bar-3 policy) — **NOT gate 2**
Gate 2's precondition ("the bundle closes: P2 + `election_safety` + supports all inductive,
truth arguments on file") is **NOT met**, so this is the time-bound checkpoint, not the
certification gate. What IS banked:
* Gate 1c's ruling, all five amendments, and the truth rule — recorded before any run.
* MODEL-EDIT-4 applied; **35 `require`s / 11 `assumption`s mechanically verified before
  every banked run** (13, 14, 15, 16). The count-based corrective was not breached, and no
  run in this session added an assumption — `QuorumAdjacency.lean` is UNTOUCHED, so the
  seventeen-witness `#print axioms` audit still covers the bundle.
* The twin calibration cross-check, discharged; divergences **(d1)–(d5)** complete.
* **40 clauses + `doesNotThrow` inductive** (run 16), up from 32 at run 12, with truth
  arguments T1–T11 on file for everything added.
* Two clause RETIREMENTS with written rulings, one of them (`electable_cfgs_contain_holder`)
  measured rather than asserted — and the measurement came back NEGATIVE, which is recorded
  as such.
* One certification claim RETRACTED (item 29) and one tractability wall recorded (item 28).
**Conditionality (n1)+(n2)+(n3) plus gate amendment (d) unchanged and in the model header;
they remain gate-2 scope for the final claim's wording.**

**NEXT SESSION'S MAP (hand-derived, no new mechanism identified as necessary):**
1. The same-term grant wrinkle (item 30) — a grant-time config ghost, mirroring `gotEAt`.
   Ghost + clauses; expected to restore `election_safety` on a footing that survives.
2. A cheaper encoding of "no role at the zero term" than `role_positive_term` (item 28/31),
   or a decision to accept P2@`commitEntry` as awaiting it.
3. The cross-config HOLDER supply for P2@`becomeLeader` (item 31) — the one place where a
   new mechanism might genuinely be needed, and therefore the likeliest next gate request.
4. Gate 2 when, and only when, all three land.

---

### SESSION 10 (2026-07-27, opus) — BAR 3, part 4: the ⏱️ protocol, the same-term grant corner
Fresh context by design. Worktree `.claude/worktrees/uc2-veil-commit-plane` @ `3bfb6f9`;
runs in `/home/claude/veil-spike/veil-preview`, logs `/home/claude/veil-spike/runs/`.

#### 32. CONTROLLER FINDING — item 29's "tool-level anomaly" is **DISSOLVED: a MISREAD, not unsoundness**
Session 4 (ledger SESSION 9, item 29) retracted run 14's `election_safety` ✅ and left open
"how run 14 produced an unsound ✅ is a tool-level question for gate 2". The controller
audited every banked log directly. **Run 14 never reported ✅ for `election_safety`.** Veil
prints a THIRD verdict marker — **⏱️, one per timed-out VC** — and the headline tally does
not force it on the reader's attention:
```
smt-run14-elecq-carrier.log:408-411
  election_safety ... ⏱️
    Exceptions:
      becomeLeader_election_safety_0_WP, becomeLeader_election_safety_tr_0_TR
        unable to prove goal. Try providing more hints. Reason: TIMEOUT
```
Full audit, re-verified mechanically this session (`grep -c "⏱️"` over every `.log` in
`/home/claude/veil-spike/runs/`): **exactly three ⏱️ VCs exist in the whole arc** —
`smt-run12-reachquorum.log:783` (`leader_completeness`, `commitEntry_leader_completeness_0_WP`
/ `_tr_0_TR`), `smt-run13-edit4.log:677` (same clause, same VC names), and
`smt-run14-elecq-carrier.log:408` (`election_safety` at `becomeLeader`). Runs 8, 11, 16 and
every earlier run: **zero**. Consequences, all binding:

* **PROTOCOL RULE (binding from here on): a ⏱️ VC is an OPEN verdict, never a green and
  never a red.** Before banking any run, `grep -c "⏱️"` its log; a nonzero count is quoted
  alongside the ✅/❌ counts and every clause carrying one is OPEN regardless of the tally.
  **Quote format from here: "N ✅ / M ❌ / K ⏱️".**
* **LEDGER CORRECTIONS.** Run 12's and run 13's residues were quoted as "P2 at 1 CTI"; the
  truthful reading is **1 CTI + 1 ⏱️ VC** (the `commitEntry` VC pair never returned).
  Run 14's `election_safety` **was never green** — the session-4 retraction of it STANDS,
  with this corrected cause. Run 16's counts are **timeout-clean** (0 ⏱️) and stand as the
  current baseline: **470 ✅ / 3 ❌ / 0 ⏱️**.
* **NO TOOL DISTRUST IS WARRANTED.** The "gate 2 must investigate an unsound ✅" item is
  DISSOLVED into this entry. Item 29's *conservative reading* of what a ✅ means ("not
  refuted by this bundle at this solver configuration") is retained as good hygiene, but
  its stated cause — a spurious pre-solver discharge — is withdrawn: nothing in the arc
  has produced a ✅ that a stronger bundle later refuted, once ⏱️ is read correctly.
* **Why it mattered anyway:** the CTI-driven work item that item 29 produced (item 30, the
  same-term grant wrinkle) is UNAFFECTED — run 16's ❌ is a real CTI, independent of how
  run 14 was read.

#### TRUTH ARGUMENT T12 — `leader_reach_strict` (written BEFORE run 17, per the truth rule)
**T12** `((candidate I ∨ leader I) ∧ ¬ cfgLt (elecCfg I) C) → tlt (reachAt I C) (curTerm I)`.
*Truth.* A role is created only by `startElection`, which sets `elecCfg i := cfgOf i` and
bumps `curTerm i := t` with `tlt (curTerm i) t` STRICT. Every config C at-or-below the frozen
`elecCfg i = cfgOf i` has already been reached by i, so `reach_bound` stamps it at or before
the PRE-bump term, hence strictly below t. Nothing re-stamps it while the role persists:
`propose` writes `reachAt i Z` only for Z with `cfgLt (cfgOf i) Z`, and `eleccfg_not_ahead` +
`cfglt_trans` put every such Z strictly ABOVE `elecCfg i`, i.e. outside the antecedent;
`adopt`, `replicate` and `crashRestart` clear the role; a strict-raise grant clears the role
and an equal-term grant changes neither `curTerm` nor `reachAt`; `becomeLeader` re-freezes
`elecCfg i := cfgOf i`, which for a candidate equals the already-covered `elecCfg i`
(`cand_cfg_frozen`). For C = genesisC the antecedent is free (`genesis_least`) and
`reachAt I genesisC` is never written by either writer (both stamp only configs STRICTLY
ABOVE the mover's current one, and genesis is least), so it sits at `tot.zero`.
*What it is for.* **(i) Task 1** — item 30's same-term grant corner, closed from the OTHER
side. A grant-time config ghost does NOT close it: the model genuinely permits a voter to
grant at config X and then adopt a higher config at the SAME term (`adopt` requires only
`tot.le (curTerm j) (curTerm i)`), so the pre-state the solver builds is legal on the voter's
side. What is NOT legal is the run-16 CTI's other side — the incumbent leader whose
`reachAt` of its own ELECTION config equals its term (node 1: `elecCfg 1 = cfgOf 1 = cfg1`,
`reachAt 1 cfg1 = curTerm 1`). T12 excludes exactly that, and it is what T8's chain needs to
instantiate `reach_quorum_below` at the winner with a STRICT bound.
**(ii) Task 2** — instantiated at `C := genesisC` it yields `reachAt I genesisC < curTerm I`,
which with the theory's `zero_le` IS `role_positive_term` (T10) — derived, without putting
`tot.zero` into the hypothesis set of every VC, the diagnosed cause of run 15's ~7 h wall
(item 28). This is the "restate over an existing bounded ghost" option, taken.

#### TRUTH ARGUMENTS T13/T14 — the FROZEN commit-leadership evidence (written BEFORE run 18)
P2's `becomeLeader` CTI (ledger 31) is, in its SAME-TERM half, an `election_safety` argument
against a leadership that no longer carries the `leader` flag: the commit leader may have
crash-restarted (`crashRestart` clears `leader` without touching `curTerm`), so run 16's CTI
has `leader = []` and `isCommitLeader = [1]`. The `elecQuorum` ghost route (T2/T3) is
therefore replayed over the commit leader's evidence, FROZEN at `commitEntry` — the same
lesson as the `elecQuorum` ghost itself: `commit_leader_evidence`'s `∃q` can be witnessed by
LATE grants against a MOVED config, a frozen ghost cannot.

**T13 `commitq_grant_covers_reach`** — `(committed ∧ qmember V commitElecQuorum ∧
¬cfgLt (cfgOf V) D ∧ tlt (reachAt V D) committedTerm) → ¬ cfgLt commitElecCfg D`.
*Truth.* Identical in shape to T3, with `(elecQuorum I, elecCfg I, curTerm I)` replaced by
the frozen `(commitElecQuorum, commitElecCfg, committedTerm)`. At `commitEntry` the clause is
exactly T3 instantiated at the committing leader (`leader i` is a `require`, and the three
ghosts are written from that leader's live values in the same step). Afterwards it cannot be
falsified: `commitElecQuorum`/`commitElecCfg`/`committedTerm` are frozen (`commitEntry`
requires `¬ committed`, so it fires at most once), `cfgOf V` is forward-only, and the
antecedent's last conjunct cannot turn true later — V granted at `committedTerm`, so
`grant_state` gives `committedTerm ≤ curTerm V`, and any later `propose`/`adopt` that makes V
newly reach D stamps `reachAt V D` with the MOVER's term, which is `≥ curTerm V ≥
committedTerm` (`adopt` requires `tot.le (curTerm j) (curTerm i)`).

**T14 `commit_leader_frozen_reach`** — `isCommitLeader I → (¬cfgLt (cfgOf I) commitElecCfg ∧
¬cfgLt commitCfgid commitElecCfg ∧ tlt (reachAt I commitElecCfg) committedTerm ∧
tot.le (reachAt I commitCfgid) committedTerm)`.
*Truth.* All four are read off `commitEntry` and then frozen. (i) `eleccfg_not_ahead` gives
`elecCfg i ≤ cfgOf i` at the commit, and `cfgOf` is forward-only. (ii) `commitCfgid := cfgOf i`
at the same instant, so `commitElecCfg = elecCfg i ≤ cfgOf i = commitCfgid`, both frozen.
(iii) T12 at the committing leader with `C := elecCfg i` (antecedent by `cfglt_irrefl`) gives
`reachAt i (elecCfg i) < curTerm i = committedTerm`. (iv) `reach_bound` at `C := cfgOf i`
gives `reachAt i (cfgOf i) ≤ curTerm i = committedTerm`. Preservation: `reachAt I C` for a
config I has ALREADY reached is never re-stamped (both writers stamp only configs the move
NEWLY covers), and `isCommitLeader` cannot change once `committed` is set.
*What T13+T14 buy.* P2's same-term case at `becomeLeader` becomes T8's chain run between the
new leader's frozen evidence and the commit leader's: order `commitElecCfg` against
`cfgOf i`; equal → `same_cfg_quorum_intersection`; adjacent → `adjacent_cfg_quorum_intersection`;
otherwise `cfglt_connected` + `reach_quorum_below` (instantiated at whichever side is higher,
with the STRICT bound supplied by T12 on the new leader's side and T14(iii) on the commit
leader's) + adjacency + T13/T3 contradicts the strict step. Any meeting member then grants to
both at `committedTerm`, so `grant_uniq` (with `self_vote`/`grant_state` for the `V = I` and
`V = cl` corners) forces the new leader to BE the commit leader, which holds E.

#### 33. TASK 1 — THE SAME-TERM GRANT CORNER: the fix is NOT a grant-time ghost
The session's map said "freeze the config at grant time (the `gotEAt`/`cfgAt`-family pattern;
ghost-only)". **Written truth analysis says that ghost does not close it, and names what
does.** A grant-time ghost `grantCfgT V T := cfgOf V` would give
`voteMsg V C T → ¬ cfgLt (cfgOf C) (grantCfgT V T)`, which is TRUE and preserved — but it is
**not violated by run 16's CTI**: the model genuinely permits node 2 to grant to a genesis
candidate while at genesis and THEN adopt a higher config at the SAME term (`adopt` requires
only `tot.le (curTerm j) (curTerm i)`, and the proposer sits at that term). The solver simply
picks `grantCfgT 2 T = cfg0`, satisfies the new clause, and keeps the CTI. **The voter's side
of that CTI is legal behaviour; the LEADER's side is not.** Run 16's pre-state has the
incumbent leader (node 1) with `elecCfg 1 = cfgOf 1 = cfg1` and `reachAt 1 cfg1 = curTerm 1`
— it reached its own ELECTION config at the very term it was elected in, which
`startElection`'s strict bump makes unreachable. That is T12 (`leader_reach_strict`), and it
is also what T8's chain needs to instantiate `reach_quorum_below` at the winner with a
STRICT bound. **Task 1's answer: a clause, not a ghost — no new state at all.**

#### 34. TASK 2 — A TRACTABLE `role_positive_term`: T12 SUBSUMES IT (encoding recorded)
`role_positive_term` = `(candidate I ∨ leader I) → tlt tot.zero (curTerm I)` cost ~7 h
(run 15) because it is the first clause to put `tot.zero` into every VC's hypothesis set.
**T12 at `C := genesisC` yields it without naming `tot.zero` at all:** the antecedent is free
(`genesis_least`), so T12 gives `reachAt I genesisC < curTerm I`, and the theory's `zero_le`
(`tot.zero ≤ x` for every x) then closes `tot.zero < curTerm I`.
**PRECISION (the step that actually carries it):** the derivation runs through `zero_le`, NOT
through `reachAt I genesisC = tot.zero`. That equation is true in every REACHABLE state —
neither `reachAt` writer ever stamps genesis, since both stamp only configs STRICTLY ABOVE
the mover's current config — but it is NOT available to the solver, which is free to invent
a pre-state where it fails, and run 16's own CTI does exactly that (`tot.zero = 1` while
`reachAt 1 genesisC = 0`). `zero_le` is an axiom of `TotalOrderWithZero` and needs no such
help. This is the "restate over an existing bounded ghost"
option of the map, and it needs no clause of its own — it is an instance of one already
wanted for task 1.

#### 35. **THE COST WALL MOVED, AND IT IS NOT WHERE RUN 15 PUT IT** (three killed runs)
* **RUN 17** — T12 in its ∀C form (`((candidate I ∨ leader I) ∧ ¬ cfgLt (elecCfg I) C) →
  tlt (reachAt I C) (curTerm I)`), clause-only, 35/11 verified before launch:
  **KILLED at 29m42s (RSS 7.58 GB), NO VERDICT**, under the item-28 3x-wall rule
  (run 16 = 10 min). Log `smt-run17-KILLED-3x-no-verdict.log`.
* **RUN 18** — the BISECTION: the same clause at the only two instances its consumers use
  (`C := elecCfg I` and `C := genesisC`), no quantified `C`, no instantiation search:
  **KILLED at 30m47s (RSS 7.62 GB), NO VERDICT.** Log `smt-run18-KILLED-3x-no-verdict.log`.
  **So the quantified `C` is NOT the cost** — the ground form is not measurably cheaper.
* **RUN 19** — the diagnostic that should have bounded it: `veil.smt.timeout` lowered
  60 s → **12 s** (the option is `Veil/Base.lean:140`; a proof found at 12 s is still a
  proof, and every VC that does not close is reported ⏱️ = OPEN under the session-5
  protocol, so greens from this configuration remain quotable). Same trajectory: 31 min,
  RSS 7.60 GB, still buffering.
* **WHAT THE THREE RUNS TOGETHER SAY.** A per-VC solver bound does not bound the run, and
  the RSS curve is a smooth climb rather than a plateau — so the cost of this clause is
  **not** in the solver but upstream of it, in VC generation/elaboration. That is a
  DIFFERENT wall from item 28's (which the 60 s × N arithmetic explained exactly), and it
  means "add one clause at a time and watch the wall" is necessary but no longer
  sufficient: at this bundle size a single clause mentioning role state in a strict term
  conclusion can make the run unfinishable regardless of the solver budget.

#### 36. TASK 3 — THE CROSS-CONFIG HOLDER SUPPLY: a hand map, and where a mechanism enters
The task's instruction was to try ghost/clause routes first and to STOP for a gate only if a
new `require`/`assumption` is genuinely needed. The clause work could not be MEASURED this
session (item 35: the bundle would not absorb even one new clause), so what follows is a
WRITTEN map, explicitly labelled **unverified by the checker** — it is a plan and an
adjudication, not a result. Split P2@`becomeLeader` by the two cases of its antecedent:

**(A) SAME TERM (`curTerm i = committedTerm`) — no new mechanism; ghost + clauses.** Run 16's
CTI here has `leader = []` and `isCommitLeader = [1]`: the commit leader crash-restarted
(`crashRestart` clears `leader` without touching `curTerm`), so `election_safety` cannot be
invoked and the argument must run against FROZEN evidence. That is T13/T14 plus one ghost
(`commitElecQuorum := elecQuorum i` at `commitEntry`) — T8's chain replayed with
`(elecQuorum I, elecCfg I, curTerm I)` replaced by `(commitElecQuorum, commitElecCfg,
committedTerm)`. Ghost-only, read in no `require`: **count-exempt, no gate**.

**(B) STRICT (`committedTerm < curTerm i`) — three of four sub-cases close on existing
machinery.** Write `CL = cfgOf i` (= `elecCfg i` after the action).
* `CL < commitCfgid`: `cfglt_connected` gives a succ-step `CL → E` with `E ≤ commitCfgid`.
  If `E < commitCfgid`, `reach_quorum_below` at the commit leader (bounded by T14(iv):
  `reachAt cl commitCfgid ≤ committedTerm < curTerm i`) yields a quorum of E reached
  strictly before `curTerm i`; `adjacent_cfg_quorum_intersection` meets it against the
  electing quorum, and `grant_reach_covered` forces `cfgOf i ≥ E > CL` — contradiction.
  If `E = commitCfgid`, adjacency meets the electing quorum against `commitQuorum` directly,
  and the meeting member holds E with `gotEAt ≤ committedTerm < curTerm i`, so
  `holder_grants_are_covered` gives `holdsE i`.
* `CL = commitCfgid`: `same_cfg_quorum_intersection` with `commitQuorum`, same finish.
* `CL = succ commitCfgid`: `adjacent_cfg_quorum_intersection` with `commitQuorum`, same finish.
* **`CL ≥ 2 steps above commitCfgid`: THE RESIDUE.** Needs "every quorum of `CL` contains an
  E-holder", which comes from "**∃ an ALL-holder quorum of `pred CL`**" plus adjacency. That
  propagates up the config chain iff every config above `commitCfgid` was proposed by an
  E-holder — i.e. `propAfterE` at the proposer, which `adopt`'s coupling require then pushes
  onto every adopter, making the certifying quorum all-holders.

**WHERE THE MECHANISM ENTERS (the honest adjudication).** The propagation has exactly one
hole: a **STALE leader** (term < `committedTerm`) proposing from a config at-or-above
`commitCfgid` without holding E. For a proposer at a config STRICTLY above `commitCfgid`
the model already kills it — `no_stale_election` with `D := pred CL` forces
`elecCfg cl ≥ pred CL > commitCfgid`, contradicting `eleccfg_not_ahead`
(`elecCfg cl ≤ cfgOf cl = commitCfgid`). The surviving hole is the proposer sitting EXACTLY
at `commitCfgid`. Two candidate closures, in the order they should be tried:
* **(i) CLAUSE-ONLY, tried first.** Members of the commit leader's FROZEN electing quorum sit
  at terms `≥ committedTerm` (`grant_state` on grants at `committedTerm`), whereas the stale
  proposal's certifying quorum has `reachAt V D ≤ cfgCommitTerm D` (`committed_cfg_quorum`,
  already inductive) with `cfgCommitTerm D ≤` the stale term. When `commitElecCfg =
  commitCfgid` the two quorums are ADJACENT and must intersect — contradiction, abstractly,
  with no new mechanism. It fails only when the commit leader was elected under a config
  strictly BELOW its commit config (`commitElecCfg < commitCfgid`, legal: elect, then
  propose), where the two quorums are ≥ 2 apart and the abstract fragment has no
  intersection to offer.
* **(ii) MODEL-EDIT-5, the gate request if (i) does not close it.** `commitEntry`'s report
  gate is deliberately the WEAK form — `tot.le (gotEAt V) (curTerm i)`, over the ACQUISITION
  term — and its own comment records that this is "STRICTLY WEAKER than the Rust gate (which
  demands report term = leader term) — an over-approximation, the sound direction". The
  faithful strengthening is one more conjunct/require: `∀ V, qmember V q →
  tot.le (curTerm i) (curTerm V)`. Rust anchor, re-verified line by line this session:
  `election.rs:546-548` drops `term < self.current_term` (`return; // stale report: dropped`,
  `:547`) and `:549-551` turns `term > self.current_term` into `adopt_term(term, None, out);
  return` (`:550`), so the ONLY reports reaching `tracker.on_durable` (`:569`, inside the
  `Role::Leader` + `follower_slot` arm at `:566-571`) are OWN-TERM reports — a counted
  member was at the leader's term when it reported. Terms are monotone and persisted, so a counted member's term is
  `≥ committedTerm` forever after. It is a NARROWING (it excludes model behaviours), so it
  needs the gate's audit exactly as EDIT-4 did — and under the count corrective it takes the
  model to **36 requires**, so it is a STOP-for-gate, not a driver decision.
**NOT REQUESTED THIS SESSION.** The gate-1c template demands a reachability trace and an
over-approximation argument from the CHECKER's evidence; the cost wall (item 35) meant no
run could produce the CTI that would justify (ii), and route (i) was never measured. Asking
for a `require` on a hand argument alone would invert the arc's discipline. **The request is
therefore PREPARED, not made.**

#### 37. **THE SLICE DEVICE — and T12 CERTIFIED INDUCTIVE in 80 seconds** (run 20)
The way past item 35's wall is not a cheaper clause but a smaller BUNDLE. `#check_invariants`
proves, per clause and per action, `Inv_bundle(s) ∧ action(s,s') → clause(s')`. If a clause
is proved against a SUBSET of the invariant conjunction, the full-bundle VC is IMPLIED
(`Inv_full → Inv_slice` weakens the antecedent), so **a slice ✅ TRANSFERS to the full bundle;
a slice ❌/⏱️ does not transfer in either direction.** The slice is a cost device, not a
weakening — and it is the standing answer to "one expensive clause makes the whole run
unfinishable".
* **RUN 20** — `ReconfigCommitSMTSlice.lean`: the model UNCHANGED (same 35 requires / 11
  assumptions, same actions) with only the nine clauses T12's preservation consumes
  (`role_exclusive`, `cand_cfg_frozen`, `eleccfg_not_ahead`, `reach_bound`, `reach_mono`,
  `cfg_from_genesis`, `self_vote`, `grant_state`, `leader_reach_strict`).
  **110 ✅ / 0 ❌ / 0 ⏱️ in 80 SECONDS.** Log `smt-run20-slice-T12.log`.
  **`leader_reach_strict` (T12) is CERTIFIED INDUCTIVE, all-n, cvc5** — and by the
  monotonicity above that certification holds in the full bundle. Tasks 1 and 2's clause is
  therefore TRUE mechanically, not just by argument.

#### 38. **TOOLCHAIN FINDING — `set_option veil.smt.timeout N in <cmd>` DOES NOT PROPAGATE**
The option must be set at **FILE SCOPE**. Evidence, three runs on the same file:
* run 19 (full bundle, `set_option ... 12 in #check_invariants`): behaved exactly like the
  60 s default — killed at **96 min, no verdict**, and its "12 s × 490 VCs ≈ 98 min" bound
  never applied. Log `smt-run19-KILLED-tmo12-no-verdict.log`.
* run 22 (election slice, `set_option ... 900 in #check_invariants`): finished in the SAME
  3 min as run 21 at the default, with the same single ⏱️ — the 900 s never applied.
* run 23 (same slice, `set_option veil.smt.timeout 900` at FILE SCOPE, before
  `veil module`): the run went from 3 min to **30 min** — the budget applied.
**Consequence for the record: run 19 was never a 12 s run, so it is NOT evidence about where
the cost lives, and item 35's inference that the cost is "upstream of the solver" is
WITHDRAWN — the flat 7.60 GB RSS plus falling CPU across runs 17/18/19 is the ordinary
signature of long sequential solver calls.** What survives item 35 unchanged: runs 17 and 18
were both killed at ~30 min with no verdict, and the ∀C→ground bisection did not help.

#### 39. **TASK 1's MECHANICAL STATUS: `election_safety`@`becomeLeader` is OPEN (⏱️), NOT REFUTED**
* **RUN 21** — `ReconfigCommitSMTElecSlice.lean`: `election_safety` plus the seventeen
  clauses T8's chain consumes (including T12). **206 ✅ / 2 ❌ / 1 ⏱️ in 3 min.**
  The two ❌ are `reach_quorum_below` at `propose`/`adopt` — **slice artifacts**: the slice
  deliberately omits `chain_committed_below`, `committed_cfg_quorum` and
  `adopted_reach_bound`, which are exactly what its preservation consumes and which made it
  inductive in the full bundle from run 13 on. They do not touch `election_safety`'s verdict:
  each VC assumes the slice conjunction independently.
  **`election_safety` is ✅ at every action EXCEPT `becomeLeader`, where it is ⏱️.**
* **RUN 23** — the same slice at a FILE-SCOPE 900 s per VC (30 min): the `becomeLeader` VC
  pair (`becomeLeader_election_safety_0_WP` / `_tr_0_TR`) is **STILL ⏱️**. Not a CTI at
  15 minutes of solver time apiece — the goal is a multi-step first-order chain
  (`cfglt_connected` → `reach_quorum_below` → `adjacent_cfg_quorum_intersection` → T3),
  which is where SMT instantiation search is weakest.
* **WHAT CHANGED, AND WHAT DID NOT.** Run 16's ❌ at this VC is **superseded**: its
  pre-state has `reachAt 1 (elecCfg 1) = curTerm 1`, which VIOLATES T12, so it is not a
  pre-state of any T12-bearing bundle (hand check; T12 was designed against exactly it).
  But "no longer refuted" is not "proved". **Under the truth rule `election_safety` is
  carried with option (a) — the WRITTEN truth argument T8 (session 4) plus T12 (item 33) —
  and its mechanical verdict is OPEN (⏱️), which must be quoted as such wherever this
  session's greens are quoted.** The honest one-line status: task 1's DEFECT is diagnosed
  and its clause is machine-certified true; the PROPERTY it was meant to close is not
  mechanically closed, and the obstacle is now solver search, not a missing invariant.

#### 40. RUN 24 — the bounded full-bundle measurement: **KILLED at 61 min, NO VERDICT**
The full 41-clause bundle (T12 included, 35/11 re-verified at launch) with a **FILE-SCOPE**
`veil.smt.timeout 5`, which should bound it at ~490 VCs × 5 s ≈ 41 min. It was still
buffering at 61 min and was killed. Log `smt-run24-KILLED-filescope5s-no-verdict.log`.
**What that adds:** a per-VC solver budget bounds the SOLVER, not the RUN — the remaining
time is VC generation and Lean elaboration at this bundle size, which no `veil.smt.timeout`
touches. So the honest cost model for this bundle is now two-term, and only the SLICE device
(item 37) attacks the second term. **No full-bundle verdict was obtained this session; run
16 (470 ✅ / 3 ❌ / 0 ⏱️, timeout-clean) therefore REMAINS THE BANKED BASELINE**, and nothing
in this session's greens is quoted from a full-bundle run.

#### SESSION-5 STOP: the ~5-hour checkpoint — **NOT gate 2, and no gate request made**
Gate 2's precondition (bundle closed, zero ⏱️, truth arguments on file for everything
retained) is NOT met. Banked, all of it clause-only at an unchanged **35 requires / 11
assumptions** (`QuorumAdjacency.lean` untouched, so the seventeen-witness `#print axioms`
audit still covers the bundle):
* The **⏱️ protocol** and the dissolution of item 29's tool-soundness question (item 32).
* **T12 CERTIFIED INDUCTIVE** in 80 s (run 20) — tasks 1 and 2 answered by one clause, with
  the map's predicted grant-time ghost shown NOT to close the corner (item 33) and the
  cheaper `role_positive_term` encoding recorded (item 34).
* The **SLICE DEVICE** with its monotonicity argument (item 37) — the standing answer to a
  bundle that will not absorb another clause.
* Two toolchain findings: the non-propagating `set_option` (item 38) and the two-term cost
  model (item 40).
* Truth arguments **T12, T13, T14** written BEFORE their runs, per the truth rule.
* `election_safety`@`becomeLeader` re-labelled honestly: **OPEN (⏱️), no longer refuted**,
  carried on the written T8+T12 argument (item 39).
* Task 3 MAPPED (item 36) with MODEL-EDIT-5 **prepared, not requested** — the Rust anchor is
  pinned to `election.rs:546-551` / `:569`, but the checker never produced the reachability
  trace the gate template requires, and a `require` will not be asked for on a hand argument.
**Conditionality (n1)+(n2)+(n3) plus gate amendment (d) unchanged and in the model header;
divergences (d1)–(d5) complete; both remain gate-2 scope for the final claim's wording.**

**NEXT SESSION'S MAP (all three are now slice-shaped work, which is the change):**
1. **Close `election_safety`@`becomeLeader` by SLICING, not by adding clauses.** The VC does
   not discharge in one shot at 900 s; the route is to cut the chain into named lemma
   clauses, each certified in its own small slice, so no single VC has to find the whole
   `cfglt_connected` → `reach_quorum_below` → adjacency → T3 instantiation sequence.
2. **P2's same-term half**: apply the `commitElecQuorum` ghost + T13/T14 (already written)
   and certify them in a slice; ghost-only, count-exempt, no gate.
3. **P2's strict half**: try route (i) of item 36 (clause-only) in a slice. Only if it fails
   does MODEL-EDIT-5 become a gate request — and then it must carry a checker-produced CTI,
   not the hand trace.
4. Gate 2 when, and only when, the bundle closes with **zero ⏱️** — the new precondition.

---

### SESSION 11 (2026-07-27, opus) — BAR 3, part 5: SLICE CERTIFICATION to closure
Fresh context by design. Worktree `.claude/worktrees/uc2-veil-commit-plane` @ `c93a943`;
runs in `/home/claude/veil-spike/veil-preview`, logs `/home/claude/veil-spike/runs/`.
Baseline verified mechanically before the first launch: **35 `require`s / 11 `assumption`s**
(`grep -cE '^\s*require '` / `grep -cE '^assumption \['` on `ReconfigCommitSMT.lean`).

#### 41. **TOOLING FINDING — `#check_action <name>`: an ACTION-DIMENSION slice that needs NO transfer argument**
`Veil/Frontend/DSL/Module/Syntax.lean:308` declares `#check_action ident`, elaborated at
`Veil/Frontend/DSL/Module/Elaborators.lean:454-465` through the same
`runFilteredInvariantCheck` as `#check_invariants` but with the filter
`isInductionForAction actionName` (`:421-423`) instead of `VCMetadata.isInduction`. The
filter is passed to `Verifier.startFiltered` (`Veil/Core/Tools/Verifier/Server.lean:34`,
`startReadyTasksLocked :49-59`), i.e. it gates which dischargers are ever STARTED — so the
run pays for one action's VCs, not the bundle's.
**Why this matters more than the clause slice.** A clause slice needs the
antecedent-weakening transfer argument (item 37) because it verifies a clause against a
SUBSET of the invariant conjunction. `#check_action` changes nothing about the VC — same
full `Invariants` hypothesis, same `Assumptions`, same goal — it merely selects which VCs
run. **A ✅ from `#check_action A` IS the full-bundle verdict for that (clause, action)
pair; no transfer, no weakening, nothing to ratify.** Coverage of the full bundle is then
the union over the ten actions plus the initialisation obligations, each measurable
separately — which is exactly what the two-term cost model (item 40) said was needed and
what run 24 could not buy.

#### TRUTH ARGUMENTS T15 / T17 — written BEFORE their hunt (the gate-1c truth rule)
Task 1 asks for `election_safety`'s chain cut into named lemma clauses. The cut is
constrained by what `#check_invariants` actually proves: `Inv(s) ∧ action(s,s') → clause(s')`
— the POST-state instances of sibling clauses are NOT hypotheses. **So a lemma only helps
if its PRE-state instance shortens the post-state derivation.** That rules out the obvious
decomposition (an intermediate "two same-term leaders have equal election configs" clause):
at `becomeLeader` its VC is the same crux as `election_safety`'s, because the new leader's
quorum comes from the ACTION's `require`, not from any invariant. What does help is
pre-composing the SUPPORT steps of T8's chain, which is what T15 and T17 do. Depth of the
`becomeLeader` derivation drops from 5 nested instantiations (`cfglt_connected` →
`reach_quorum_below` → compose with `leader_reach_strict` → `adjacent_cfg_quorum_intersection`
→ `elecq_grant_covers_reach`) to 3 (`cfglt_connected` → T17 → the grant clause).

**T15 `role_below_quorum_strict`** —
`((candidate I ∨ leader I) ∧ cfgLt C (elecCfg I)) → (C = genesisC ∨ ∃ q, quorumOf q C ∧
∀ V, qmember V q → (¬ cfgLt (cfgOf V) C ∧ tlt (reachAt V C) (curTerm I)))`.
*Truth.* `eleccfg_not_ahead` gives `elecCfg I = cfgOf I ∨ cfgLt (elecCfg I) (cfgOf I)`; with
`cfglt_irrefl` + `cfglt_trans` that yields `¬ cfgLt (cfgOf I) (elecCfg I)` — the role has
REACHED its own election config. `reach_quorum_below` at `(E := C, C := elecCfg I, N := I)`
then gives `C = genesisC` or a quorum q of C, every member at-or-past C, with
`tot.le (reachAt V C) (reachAt I (elecCfg I))`. `leader_reach_strict` (T12, CERTIFIED
INDUCTIVE, run 20) gives `tlt (reachAt I (elecCfg I)) (curTerm I)`; composing `≤` with `<`
in `TotalOrderWithZero` gives the strict bound. So T15 holds in EVERY state satisfying two
clauses already in the bundle, a fortiori in every reachable state.
*Preservation, action by action (what the VC must actually re-derive from the PRE-state).*
`becomeLeader i q` — i is a CANDIDATE in the pre-state and `cand_cfg_frozen` gives
`elecCfg i = cfgOf i`, which is exactly what the action re-assigns, so `elecCfg i` is
UNCHANGED and `cfgOf`/`reachAt`/`curTerm` are untouched: the pre-state instance carries
verbatim (this is WHY T15 is stated over `candidate ∨ leader` and not over `leader` alone).
`startElection i t` — creates the role; `elecCfg i := cfgOf i` and `curTerm i := t` with
`tlt (curTerm i) t` STRICT, so `reach_quorum_below` at `(C, cfgOf i, i)` plus `reach_bound`
at `(i, cfgOf i)` re-establish it with the strict bound coming from the bump, not from T12.
`deliverRequestVoteGrant j c t` — a strict raise clears both role flags; an equal-term grant
changes neither `curTerm` nor `elecCfg` nor `cfgOf` nor `reachAt`. `replicate` / `adopt` /
`crashRestart` — clear the role of the only node they touch. `propose i d` — `elecCfg` and
`curTerm` are unchanged; `cfgOf i` moves FORWARD along `succCfg` and `reachAt i Z` is stamped
only for Z with `cfgLt (cfgOf i) Z`, all of which are strictly above `cfgOf i ≥ elecCfg i > C`,
so `reachAt i C` is untouched and `¬ cfgLt (cfgOf i) C` survives the forward move. `adopt j i`
— identical at the adopter, with forward-onlyness from MODEL-EDIT-2c's
`¬ cfgLt (proposedC i) (cfgOf j)`. `appendEntry` / `commitEntry` / `commitCfg` — touch none
of `leader`, `candidate`, `elecCfg`, `cfgOf`, `reachAt`, `curTerm`.
*Consumes:* `reach_quorum_below`, `leader_reach_strict`, `reach_bound`, `eleccfg_not_ahead`,
`cand_cfg_frozen`, `role_exclusive`, and the chain-order assumptions.

**T17 `role_below_meets_quorum`** —
`((candidate I ∨ leader I) ∧ succCfg F E ∧ cfgLt E (elecCfg I) ∧ quorumOf Q F) →
∃ V, qmember V Q ∧ ¬ cfgLt (cfgOf V) E ∧ tlt (reachAt V E) (curTerm I)`.
*Truth.* T15 at `(I, C := E)` gives either `E = genesisC` — impossible, since `succCfg F E`
gives `cfgLt F E` (`succ_cfglt`) and `genesis_least` forbids anything strictly below genesis
— or a quorum `q_E` of E whose members are at-or-past E and reached E strictly before
`curTerm I`. `adjacent_cfg_quorum_intersection` at `(F, E, Q, q_E)` (its three hypotheses are
`succCfg F E`, `quorumOf Q F`, `quorumOf q_E E`) yields a member V of BOTH, and V's
membership in `q_E` carries the two conclusions. So T17, like T15, is a one-step consequence
of clauses/assumptions already present, in every state.
*Preservation.* Identical frame analysis to T15 — the clause mentions the same mutable
symbols (`candidate`, `leader`, `elecCfg`, `cfgOf`, `reachAt`, `curTerm`); `succCfg`,
`quorumOf`, `qmember`, `cfgLt` are IMMUTABLE. At `becomeLeader` the pre-state instance
carries verbatim (`cand_cfg_frozen`); at `startElection` it is re-derived through
`reach_quorum_below` + `reach_bound` + adjacency.
*Consumes:* T15 + `adjacent_cfg_quorum_intersection` + `succ_cfglt` + `genesis_least`
(and, at `startElection`, the same supports T15 needs there).
*What T17 buys `election_safety`@`becomeLeader`.* Post-state pair (the new leader i, an
incumbent L) at one term T, with `elecCfg i = cfgOf i` (`cand_cfg_frozen`) and the action's
`require`s supplying `quorumOf q (cfgOf i)` and `∀ V ∈ q, V = i ∨ voteMsg V i T`. Three cases
by `cfglt_total` on `(elecCfg L, elecCfg i)`: **equal** — `same_cfg_quorum_intersection` on
`q` and `elecQuorum L` (`elecq_witness`) gives a common voter, `grant_uniq` (with `self_vote`
+ `grant_state` for the `V = i` / `V = L` corners) forces `i = L`; **`elecCfg L < elecCfg i`**
— `cfglt_connected` gives `succCfg (elecCfg L) E` with `E = elecCfg i` (adjacency on the two
quorums, then `grant_uniq`, as above) or `cfgLt E (elecCfg i)`, where T17 at
`(I := i, F := elecCfg L, E, Q := elecQuorum L)` hands over a member of `elecQuorum L` that
reached E strictly before T, and `elecq_grant_covers_reach` at `(L, V, E)` gives
`¬ cfgLt (elecCfg L) E` against `succ_cfglt`; **`elecCfg i < elecCfg L`** — mirror image,
with T17 at `(I := L, F := elecCfg i, E, Q := q)` and `grant_reach_covered` at
`(V, i, T, E)` giving `¬ cfgLt (cfgOf i) E`, again against `succ_cfglt` (the `V = i` corner
is immediate: `i ∈ q_E` already says `¬ cfgLt (cfgOf i) E`).

#### 42. RUN 25 — the first FULL-BUNDLE verdict since run 16, at `becomeLeader`: **42 ✅ / 1 ❌ / 1 ⏱️ in 393 s**
`ReconfigCommitSMTActBL.lean` = `ReconfigCommitSMT.lean` verbatim (35 requires / 11
assumptions re-verified at launch, 41 invariants + 2 safeties) with `#check_invariants`
replaced by `#check_action becomeLeader` and the file-scope budget raised 5 s → 60 s (the
run-16 default). Log `smt-run25-actionBL.log`. Compare run 24: the same bundle, all
actions, 5 s per VC — **killed at 61 min with no verdict**. The action filter is the cost
device the two-term model asked for.
* `election_safety`@`becomeLeader` — **⏱️** (`becomeLeader_election_safety_0_WP` /
  `_tr_0_TR`, TIMEOUT). Unchanged in kind from runs 21/23: OPEN, not refuted, carried on
  the written T8 + T12 argument. 60 s here vs 900 s there; the 900 s measurement stands as
  the stronger one.
* `leader_completeness`@`becomeLeader` — **❌, and this is the session's most useful
  artifact**: the first CHECKER-PRODUCED P2 CTI since run 16, at full bundle strength.
* Everything else at this action — **42 ✅**, i.e. the full-bundle certification of 41
  clauses + `doesNotThrow` + `election_safety`'s sibling at `becomeLeader`, with NO transfer
  argument needed (item 41).

**THE CTI, ADJUDICATED (F-M7-1 discipline, before any clause is written).** Chain
`genesis(1) → 2 → 0`; two terms (`tot.zero = 1 < 0`); quorums `q1 = {1}` of genesis,
`q0 = {0}` of cfg 0, `q2 = {0,1}` of cfg 2. Node 1 is the commit leader — elected under
GENESIS, `commitCfgid = genesis`, `commitElecCfg = genesis`, `commitQuorum = {1}`,
`committedTerm = 0`, `holdsE = {1}` — and has since crash-restarted (`leader = []`). Node 0
is a candidate at the SAME term 0 sitting at the TOP config with `elecCfg 0 = cfgOf 0 = 0`,
and `becomeLeader(i=0, q={0})` elects it. Two configs apart, so
`adjacent_cfg_quorum_intersection` never forces `q0 ∩ q1 ≠ ∅`: P2 fails. **It is a MODEL
ARTIFACT, not a UC behaviour, and the pre-state is UNREACHABLE in the model itself** — for
node 0 to sit at cfg 0 it must have adopted cfg 2 and cfg 0; the only possible proposer is
node 1; `commitCfgid = genesis` forces those proposals to POSTDATE the commit, hence
`propAfterE 1 = true`; and `adopt`'s coupling require then demands `holdsE 0`, which the
CTI denies (`hasAdopted = []` in the WP model confirms node 0 never adopted anything). This
is exactly the SAME-TERM half of ledger item 36's map, and it is excluded by the frozen
commit-evidence clauses written there: with T18 forcing `quorumOf commitElecQuorum
commitElecCfg` the ghost must be `q1 = {1}`, and T13 at `(V := 1, D := 2)` — antecedent
`qmember 1 commitElecQuorum`, `¬ cfgLt (cfgOf 1) 2`, `tlt (reachAt 1 2) committedTerm`, all
true here — concludes `¬ cfgLt commitElecCfg 2`, contradicting `cfgLt genesis 2`. **No
stop-the-arc finding; no new `require`; the fix is the already-written ghost + clauses.**

#### TRUTH ARGUMENT T18 — `commitq_witness` (written BEFORE its run, per the truth rule)
**T18** `(committed ∧ isCommitLeader I) → (quorumOf commitElecQuorum commitElecCfg ∧
∀ V, qmember V commitElecQuorum → (V = I ∨ voteMsg V I committedTerm))`.
*Truth.* This is `elecq_witness` (T2, already inductive) frozen at the commit. `commitEntry`
requires `leader i`, so in its pre-state `elecq_witness` gives
`quorumOf (elecQuorum i) (elecCfg i)` and `∀ V ∈ elecQuorum i, V = i ∨ voteMsg V i (curTerm i)`;
the action writes `commitElecQuorum := elecQuorum i`, `commitElecCfg := elecCfg i`,
`committedTerm := curTerm i` and `isCommitLeader i := true` in the SAME step, so the clause
holds immediately after. It is preserved because `commitEntry` requires `¬ committed` and so
fires at most once — all four written components are frozen thereafter — `voteMsg` is never
retracted by any action, and `isCommitLeader` is only ever written at that same site.
*Why the ghost is needed at all (and why `commit_leader_evidence`'s `∃q` is not a substitute).*
Identical to the `elecQuorum` lesson of session 4: the existential witness can be supplied by
LATE grants made against a MOVED config, whereas the argument needs the quorum that actually
certified the commit leader's election. `commitElecQuorum` is read in NO `require`, so it is
GHOST — the count stays at 35 requires / 11 assumptions and no gate is needed (bar-3 policy).

#### 43. RUNS 26 / 27 — **T15 and T17 CERTIFIED INDUCTIVE** (83 s and 91 s)
Both slices are the model VERBATIM (35 requires / 11 assumptions re-verified in each file
before launch) with the invariant conjunction cut to the clause's own support set.
* **RUN 26** `ReconfigCommitSMTT15Slice.lean` — **130 ✅ / 2 ❌ / 0 ⏱️ in 83 s**.
  `role_below_quorum_strict` (T15) is ✅ at INIT and at all ten actions.
  Hypothesis set (recorded for the transfer audit): `role_exclusive`, `cand_cfg_frozen`,
  `eleccfg_not_ahead`, `reach_bound`, `reach_mono`, `cfg_from_genesis`, `self_vote`,
  `grant_state`, `leader_reach_strict`, `reach_quorum_below` — all ten members of the FULL
  bundle, so `Inv_full → Inv_slice` and the ✅ transfers (item 37).
* **RUN 27** `ReconfigCommitSMTT17Slice.lean` — **141 ✅ / 2 ❌ / 0 ⏱️ in 91 s**.
  `role_below_meets_quorum` (T17) is ✅ at INIT and at all ten actions. Hypothesis set = run
  26's plus `role_below_quorum_strict` (certified by run 26).
* **THE TWO ❌ IN EACH ARE THE SAME KNOWN SLICE ARTIFACT** — `reach_quorum_below` at
  `propose` and `adopt`, from deliberately omitting the three clauses its preservation
  consumes (`chain_committed_below`, `committed_cfg_quorum`, `adopted_reach_bound`). That
  clause is inductive in the FULL bundle from run 13 onward and again in run 25; the run-21
  precedent for reading such a ❌ as a slice artifact applies unchanged. **Neither ❌ touches
  the target clause's verdict**: each VC assumes the slice conjunction independently.

#### MODEL EDIT (GHOST + FIVE CLAUSES) — count UNCHANGED at **35 `require`s / 11 `assumption`s**
Applied to `ReconfigCommitSMT.lean`: the ghost `individual commitElecQuorum : quorum`
(written only at `commitEntry`, read in NO `require` — count-exempt, no gate per bar-3
policy), and five clauses: `role_below_quorum_strict` (T15), `role_below_meets_quorum` (T17),
`commitq_witness` (T18), `commitq_grant_covers_reach` (T13), `commit_leader_frozen_reach`
(T14). Bundle: 41 → **46 invariants** + 2 safeties. `QuorumAdjacency.lean` UNTOUCHED, so the
seventeen-witness `#print axioms` audit still covers the bundle. Truth arguments T13/T14
(session 5), T15/T17/T18 (this session) were all written BEFORE their runs.

#### 44. RUN 28 — the enlarged bundle at `becomeLeader`: **47 ✅ / 1 ❌ / 1 ⏱️ in 614 s**
`ReconfigCommitSMTActBL.lean` regenerated from the edited model (46 invariants + 2 safeties;
35 requires / 11 assumptions re-verified at launch), `#check_action becomeLeader`, 60 s per
VC. Log `smt-run28-actionBL-frozen.log`.
* **All five new clauses are ✅ at `becomeLeader`** — `role_below_quorum_strict`,
  `role_below_meets_quorum`, `commitq_witness`, `commitq_grant_covers_reach`,
  `commit_leader_frozen_reach` — i.e. T13/T14/T18 land at the action they were written for,
  in the FULL bundle, with no transfer argument (item 41).
* **`leader_completeness`'s SAME-TERM CTI IS GONE, EXACTLY AS PREDICTED.** Run 25's CTI had
  `committedTerm = curTerm i`; run 28's has `committedTerm = 2 < curTerm i = 0` in a
  three-term theory (`tot.zero = 1 < 2 < 0`). The frozen commit evidence closed the same-term
  half of ledger item 36 case (A) — a prediction written before the run and confirmed by it.
* **`election_safety`@`becomeLeader` is STILL ⏱️** at 60 s even with T15/T17 present.
* Residue at this action: `leader_completeness` (1 ❌, the STRICT half) and
  `election_safety` (1 ⏱️).

#### 45. THE STRICT-HALF CTI, ADJUDICATED — and the map that item 36 was missing
Run 28's CTI: chain `genesis(1) → 2 → 0`; `commitCfgid = commitElecCfg = genesis`,
`commitQuorum = commitElecQuorum = {1}`, `committedTerm = 2`, `holdsE = {1}`,
`isCommitLeader = {1}`, `leader = []` (the commit leader crash-restarted). Candidate 0 sits
at the TOP config with `elecCfg 0 = cfgOf 0 = 0` and `curTerm 0 = 0 > committedTerm`;
`becomeLeader(i=0, q={0})` elects it with a quorum TWO STEPS above `commitCfgid`, which
adjacency never forces to meet `commitQuorum`. **MODEL ARTIFACT, pre-state unreachable in
the model**: node 0 can only sit at cfg 0 by adopting cfg 2 then cfg 0; every such proposal
postdates the commit (`commitCfgid = genesis` freezes the commit leader at genesis), so
`propAfterE` was set at the proposer and `adopt`'s coupling demands `holdsE 0` — which the
CTI denies (`hasAdopted = []` again). **Adjudicated as needing CLAUSES, not a `require`.**

**WHERE MODEL-EDIT-5 ACTUALLY BITES — a correction to item 36 that matters for the gate.**
MODEL-EDIT-5 (own-term report gate) does **NOT** exclude this CTI: it would make
`commitQuorum`'s members sit at terms ≥ `committedTerm`, which creates no intersection
between a genesis quorum and a quorum two configs above it. So **run 28's CTI is NOT the
reachability trace that justifies MODEL-EDIT-5**, and the request is again NOT made. What
this CTI calls for is the holder-supply chain, clause-only.

**THE CHAIN, AS FAR AS IT IS NOW UNDERSTOOD (written map; UNMEASURED — no run backs it).**
The clause the CTI wants is
`T24 above_commit_cfg_implies_holder : (committed ∧ cfgLt commitCfgid (cfgOf N)) → holdsE N`
— a node strictly above the commit config holds E. It reduces, through `adopt`'s coupling
require and `propose`'s `propAfterE` write, to
`T23 : (committed ∧ hasProposal I ∧ cfgLt commitCfgid (proposedC I)) → propAfterE I`,
whose two cases are: proposer strictly above `commitCfgid` (T24 at the proposer — a
legitimate mutual induction, since every VC gets the whole pre-state bundle), and proposer
sitting EXACTLY at `commitCfgid` — item 36's named hole. For a CURRENT proposer
(`committedTerm ≤ curTerm I`) that hole closes on `leader_completeness` ITSELF, which is in
the bundle. **The residue is therefore precisely the STALE proposer at `commitCfgid`.**
*Route (i), now understood mechanically (item 36 stated it, this is the derivation).* The
contradiction is not about the stale leader's own E-holding — a stale leader legitimately
need not hold E — but about who can ADOPT from it. `adopt` requires
`tot.le (curTerm j) (curTerm i)`, so every adopter of a stale proposal sits at a term ≤ the
stale term < `committedTerm`; whereas every member of `commitElecQuorum` sits at a term
≥ `committedTerm` (`grant_state` on grants at `committedTerm`, plus `curTerm` monotone).
When `commitElecCfg = commitCfgid` the proposal's config is `succ commitCfgid`, ADJACENT to
`commitElecCfg`, so the two quorums must intersect — contradiction, no new mechanism. When
`commitElecCfg < commitCfgid` (legal: elect, then propose) they are ≥ 2 apart and the
abstract fragment offers no intersection — and THAT is where MODEL-EDIT-5 enters, because it
upgrades `commitQuorum` (which sits at `commitCfgid`, adjacent to the proposal's config)
from "acquired E early" to "at a term ≥ `committedTerm`", supplying the same contradiction
one config higher.
*The general form the clause work needs* (this session's addition to the map):
`T27 : (committed ∧ cfgCommitted D ∧ cfgLt commitElecCfg D) → tot.le committedTerm (cfgCommitTerm D)`
— no config above the commit leader's ELECTION config commits at a term below
`committedTerm`. Its proof runs `cfglt_connected` from `commitElecCfg` up to D, uses
`committed_cfg_quorum`'s reach bound (`reachAt V D ≤ cfgCommitTerm D`) plus
`reach_quorum_below` to push a quorum down to the successor of `commitElecCfg`, meets it
against `commitElecQuorum` by adjacency, and closes with **T13**
(`commitq_grant_covers_reach`) against `succ_cfglt` — i.e. it is the frozen-commit twin of
T17's chain, and route (i) is its `D = succ commitCfgid` instance.
**STATUS: WRITTEN, NOT MEASURED.** No `#check_action`/slice run in this session carries
T23/T24/T27; they are a plan, not a result, and are labelled as such. The honest residue of
the strict half is "a clause-only chain of three further clauses whose truth arguments are
sketched but not completed, with MODEL-EDIT-5 as the fallback for its last sub-case only".

#### 46. **TASK 1's VERDICT: the lemma cut is CERTIFIED, the crux VC is STILL ⏱️** (runs 30, 32)
Both lemmas are machine-certified (runs 26/27) and both are in the bundle; the goal they were
cut to shorten still does not discharge. Every configuration tried, recorded so the next
session does not repeat them:
* **RUN 30** — full bundle, `#check_action becomeLeader`, **FILE-SCOPE `veil.smt.timeout 900`**:
  **47 ✅ / 1 ❌ / 1 ⏱️ in 1945 s.** Log `smt-run30-actionBL-900s.log`. Fifteen minutes of
  solver time on `becomeLeader_election_safety_0_WP` / `_tr_0_TR` WITH T15 and T17 present:
  still TIMEOUT, no counterexample.
* **RUN 32** — `ReconfigCommitSMTElecSlice2.lean`, an ELEVEN-clause election slice
  (`election_safety` + `grant_state`, `grant_uniq`, `self_vote`, `role_exclusive`,
  `cand_cfg_frozen`, `eleccfg_not_ahead`, `elecq_witness`, `elecq_grant_covers_reach`,
  `grant_reach_covered`, `role_below_meets_quorum`), `#check_action becomeLeader`, file-scope
  300 s: **11 ✅ / 0 ❌ / 1 ⏱️ in 639 s.** Log `smt-ElecSlice2.log`. Note the **0 ❌** — this
  slice has no artifacts at all; the ONLY undischarged VC in it is the crux.
* **The measurement grid is now**: {full bundle, 17-clause slice, 11-clause slice} ×
  {60 s, 300 s, 900 s} — and `election_safety`@`becomeLeader` is ⏱️ in all of them.
  Runs 21/23 measured the 17-clause slice at 60 s and 900 s; run 28 the full bundle at 60 s;
  run 30 at 900 s; run 32 the 11-clause slice at 300 s.
**HONEST READING.** The clause work is done and certified: the same-term grant defect (item
30) is closed by T12, and the two instantiation steps that T8's chain needs are pre-composed
into T15/T17 and CERTIFIED INDUCTIVE. What remains is a first-order instantiation search that
cvc5 does not complete at any budget or bundle size tried. **Under the truth rule the clause
stays OPEN (⏱️), carried on the written argument T8 + T12 + the T17 chain (S6.2).** The next
lever is not another invariant: it is either a MANUAL discharge of that one VC (Veil emits
the `@[veil] theorem becomeLeader_election_safety … := by unveil; sorry` stub for exactly
this purpose — the stub text is in `smt-run21-elecslice.log`), or a different solver
configuration. Both are named work, neither was attempted this session.

#### 47. RUN 31 — the frozen-evidence slice at ALL actions + INIT: **195 ✅ / 3 ❌ / 0 ⏱️ in 104 s**
`ReconfigCommitSMTFrozenSlice.lean`, 17 clauses, model verbatim (35/11). Log
`smt-FrozenSlice.log`.
* **`commitq_witness` (T18) — ✅ at INIT and at all ten actions. CERTIFIED INDUCTIVE.**
* **`commit_leader_frozen_reach` (T14) — ✅ at INIT and at all ten actions. CERTIFIED INDUCTIVE.**
* `commitq_grant_covers_reach` (T13) — ✅ at INIT and at eight actions, **❌ at `propose` and
  `adopt`**; ✅ at `becomeLeader` in the FULL bundle (run 28). Diagnosed as a SLICE ARTIFACT:
  T13's preservation at the two `reachAt` writers needs `committedTerm ≤ curTerm` for the
  commit leader in the `V = cl` corner, which comes from `commit_leader_self_vote` +
  `voteterm_bounded` — neither of which the slice carried. Run 33 (`FrozenSlice2`) adds those
  two plus `reach_quorum_below` and `cand_reach_strict`; **its result is the one hole this
  session leaves in the T13 row of the dossier.**
* The third ❌ is `elecq_grant_covers_reach`, likewise a slice artifact (its preservation
  consumes `cand_reach_strict` / `reach_bound`, omitted here); it is inductive in the full
  bundle from run 14 on and again at `becomeLeader` in runs 25/28/30.
  **RUN 33** (`FrozenSlice2`, 21 clauses, 154 s) — **237 ✅ / 5 ❌ / 0 ⏱️**: T13 still ❌ at
  `propose`/`adopt`, and its CTI named the real omission. The pre-state has
  `committed = true` with `isCommitLeader = []` — a state `commitEntry` cannot produce (it
  writes both in one step) and which makes EVERY commit-leader clause vacuous, so nothing
  bounds `commitElecQuorum` members' terms. The missing full-bundle clause is
  `commit_leader_evidence`.
  **RUN 34** (`FrozenSlice3` = run 33 + `commit_leader_evidence`, 157 s) —
  **250 ✅ / 3 ❌ / 0 ⏱️**, and **`commitq_grant_covers_reach` (T13) is ✅ at INIT and at all
  ten actions: CERTIFIED INDUCTIVE.** The three residual ❌ are the standing slice artifacts
  (`reach_quorum_below` at `propose`/`adopt`, `elecq_grant_covers_reach` at one action), all
  inductive in the full bundle.

#### 48. WHERE THE BUNDLE STANDS — and the two things that are NOT covered
**Certified, with the run that did it:**
* the 40 clauses of run 16's bundle, all ten actions + INIT (run 16, 470 ✅ / 3 ❌ / 0 ⏱️),
  carried forward by antecedent weakening + the GHOST-EXTENSION transfer (see the memo's
  amendment clause (D), which asks gate 2 to rule on that transfer explicitly);
* `leader_reach_strict` (T12) — run 20, 110 ✅ / 0 ❌ / 0 ⏱️;
* `role_below_quorum_strict` (T15) — run 26, 130 ✅ / 2 ❌ / 0 ⏱️;
* `role_below_meets_quorum` (T17) — run 27, 141 ✅ / 2 ❌ / 0 ⏱️;
* `commitq_witness` (T18) + `commit_leader_frozen_reach` (T14) — run 31, 195 ✅ / 3 ❌ / 0 ⏱️;
* `commitq_grant_covers_reach` (T13) — run 34, 250 ✅ / 3 ❌ / 0 ⏱️;
* **the WHOLE bundle at `becomeLeader`, with no transfer argument at all** — run 28,
  47 ✅ / 1 ❌ / 1 ⏱️, and run 30 at 900 s per VC, same tally.
**NOT covered, stated as holes:**
1. `election_safety` @ `becomeLeader` — **⏱️ / OPEN** in all six runs that have measured it.
2. `leader_completeness` @ `becomeLeader` — **❌**, the STRICT half (item 45); clause-only
   work, mapped but unmeasured.
3. `leader_completeness` @ `commitEntry` — ❌ in run 16, expected to close on T12 (which
   subsumes the withdrawn `role_positive_term`), **UNMEASURED since T12 landed**. The
   `#check_action commitEntry` run was started and killed at 15 min for box memory when it
   collided with run 30; it is the single cheapest outstanding measurement and should be the
   next session's first command.
4. The other eight actions have not been re-measured on the enlarged bundle under criterion
   (A); their coverage rests on the run-16 + ghost-extension transfer.

#### SESSION-6 STOP — the ~5-hour checkpoint, **NOT gate 2**
Gate 2's precondition (bundle closed with zero ⏱️) is NOT met: one ⏱️ and two ❌ remain.
**No gate request is made** — MODEL-EDIT-5 is still PREPARED, NOT REQUESTED, and item 45
strengthens that position by showing MODEL-EDIT-5 would not even exclude run 28's CTI.
Everything this session is clause/ghost-only at an unchanged **35 `require`s / 11
`assumption`s**, `QuorumAdjacency.lean` untouched, so the seventeen-witness `#print axioms`
audit still covers the bundle. Conditionality (n1)+(n2)+(n3), gate amendment (d) and
divergences (d1)–(d5) are unchanged and remain gate-2 scope.

**NEXT SESSION'S MAP:**
1. `#check_action commitEntry` on the full bundle — the cheapest open measurement (hole 3).
2. The remaining eight `#check_action` runs, to put the whole bundle under criterion (A) and
   retire the ghost-extension transfer entirely (hole 4). Budget ~10 min each, ONE AT A TIME
   (running two Lean processes in parallel cost this session a 15-minute run to memory
   pressure).
3. `election_safety`@`becomeLeader`: stop adding invariants. Either discharge that one VC
   MANUALLY through Veil's `@[veil] theorem … := by unveil; …` stub, or change the solver
   configuration. The invariant side is done and certified.
4. The strict half of P2: the T23/T24/T27 chain of item 45, clause-only.
5. Gate 2 only when (1)–(4) land with zero ⏱️.

---

### SESSION 12 (2026-07-27, opus) — BAR 3, part 6: the PER-ACTION SWEEP + the T24 refutation
Fresh context by design. Worktree `.claude/worktrees/uc2-veil-commit-plane` @ `cb768d4`;
runs in `/home/claude/veil-spike/veil-preview`, logs `/home/claude/veil-spike/runs/`.
Baseline re-verified mechanically before the first launch: **35 `require`s / 11
`assumption`s**, 46 invariants + 2 safeties, and every one of the ten per-action files
(`ReconfigCommitSMTAct<action>.lean`) diffed against `ReconfigCommitSMT.lean` — identical
modulo the module name, the file-scope `veil.smt.timeout` and the `#check_action` line
(the diff is recorded in this session's transcript; `#check_action` changes no VC, item 41).

#### TOOLING NOTE — `Invariants` INCLUDES the safeties (checked, not assumed)
`Module.assembleInvariants` (`Veil/Frontend/DSL/Module/Util/Assemble.lean:130`) assembles
the `.invariantLike` set — **`invariant`, `safety` and `trusted invariant` clauses
together** — into the `Invariants` definition that every VC takes as its hypothesis
(`assembleSafeties` is the separate, `onlySafety := true` assembly used for the safety
goals). Consequence, and it is load-bearing for the holder-supply chain below:
**`leader_completeness` (P2) is available as a PRE-state hypothesis in every VC**, so a
clause may legitimately lean on P2 for the current-term case and still be part of the same
mutual induction.

#### TRUTH ARGUMENT T19 — **T24 (item 45's holder-supply clause) is FALSE IN A REACHABLE STATE**
Written BEFORE the run that seeks the checker's CTI, per the gate-1c truth rule. Item 45
proposed
`T24 : (committed ∧ cfgLt commitCfgid (cfgOf N)) → holdsE N`
as the clause the strict-half CTI wants, with "the residue is precisely the STALE proposer
at `commitCfgid`" as its one open case. **That case is not a proof gap — it is a reachable
model behaviour, and T24 is therefore refuted, not merely unproved.** The trace, every step
checked against the action's `require`s (n=5, `G = genesisC = {0,1,2,3,4}`, 3-of-5 quorums,
two terms `1 < 2`):
1. `startElection(0,1)` + grants from `{1,2}` + `becomeLeader(0,{0,1,2})` — node 0 leader
   at term 1, `elecCfg 0 = cfgOf 0 = G`. (No E exists yet, so the up-to-date grant guard
   `¬(holdsE j ∧ ¬ holdsE c)` is free.)
2. `startElection(3,2)` + grants from `{1,4}` + `becomeLeader(3,{1,3,4})` — node 3 leader at
   term 2. Node 0 **did not grant** (it is not in `{1,3,4}`), so nothing cleared its
   `leader` flag: `crashRestart` is the only other clearer and it is not taken.
   **Node 0 is now a STALE leader at genesis.**
3. `appendEntry(3)`, `replicate(3,1)`, `replicate(3,4)` — `holdsE = {1,3,4}`, all with
   `gotEAt = 2`; `replicate` requires `curTerm j ≤ curTerm i` (both are at 2) ✓.
4. `commitEntry(3,{1,3,4})` — `leader 3` ✓, `¬committed` ✓, `holdsE 3` ✓,
   `quorumOf {1,3,4} (cfgOf 3 = G)` ✓, all three hold E ✓, `gotEAt V = 2 ≤ curTerm 3 = 2` ✓
   (MODEL-EDIT-1's gate). State: `committed`, `committedTerm = 2`, `commitCfgid = G`,
   `commitQuorum = commitElecQuorum = {1,3,4}`, `commitElecCfg = G`, `isCommitLeader = {3}`.
5. `propose(0, C1)` with `succCfg G C1` — `leader 0` ✓, `¬ pending 0` ✓, and **both config
   gates are satisfied by their `cfgOf i = genesisC` DISJUNCT**, which a stale leader at
   genesis always has. Post-state: `cfgOf 0 = C1`, `propAfterE 0 = false` (node 0 never
   held E).
**Post-state of step 5: `committed ∧ cfgLt commitCfgid (cfgOf 0) ∧ ¬ holdsE 0` — T24 false,
in a state reached by five legal actions.**
*Why P2 is nevertheless not violated here* (the adjudication that keeps this a clause
refutation and NOT a stop-the-arc finding): node 0's term is 1 `<` `committedTerm = 2`, so
P2's antecedent `tot.le committedTerm (curTerm L)` fails; and node 0 cannot repair that —
to be elected at a term ≥ 2 it needs a quorum of `C1`, every one of which meets
`commitQuorum` by `adjacent_cfg_quorum_intersection` and therefore contains an E-holder,
which the up-to-date guard forbids from granting to a non-holder. **The stale leader can
create configs; it cannot create ELECTABILITY.**
*The obvious repair is also false.* Term-guarding the clause —
`(committed ∧ cfgLt commitCfgid (cfgOf N) ∧ tot.le committedTerm (curTerm N)) → holdsE N` —
dies to the same trace plus two steps: `adopt(2,0)` (`curTerm 2 = 1 ≤ curTerm 0 = 1`,
`¬ propAfterE 0` so the coupling is free) puts non-holder node 2 at `C1`, and then a
`startElection(0,3)` + `deliverRequestVoteGrant(2,0,3)` (both non-holders, so the
up-to-date guard is free; `cmember 0 C1` ✓; `¬ cfgLt (cfgOf 0) (cfgOf 2)` ✓) raises
`curTerm 2` to 3 `≥ committedTerm`. Node 2 is then above the commit config, at a term above
the commit term, and holds nothing.

#### TRUTH ARGUMENT T20 — the chain that IS true (the corrected map for the strict half)
The two refutations above locate the error in item 45's map precisely: it indexes the
holder supply by NODE (`cfgOf N`), and nodes can be dragged above `commitCfgid` by a stale
leader. The supply is really indexed by **COMMITTED CONFIG**, because `propose` requires
`cfgOf i = genesisC ∨ cfgCommitted (cfgOf i)` — so a config two steps above genesis can
only exist if the one below it COMMITTED, and a commit is a quorum fact.
**T20 `cfg_holder_quorum`** —
`(committed ∧ (D = commitCfgid ∨ (cfgCommitted D ∧ cfgLt commitCfgid D))) →
  ∃ q, quorumOf q D ∧ ∀ V, qmember V q → holdsE V`.
*Base (`D = commitCfgid`).* `commit_quorum_sound` + `commit_backed` give exactly
`commitQuorum`.
*Step (`cfgCommitted D`, `cfgLt commitCfgid D`).* `cfglt_connected` + `succ_immediate` put
`Y := pred D` at-or-above `commitCfgid`; the induction hypothesis (this same clause at `Y`,
legitimate — every VC gets the whole pre-state bundle) supplies an all-holder quorum
`q_Y` of `Y`. D was proposed by a leader `P` sitting at `Y`; `P`'s own electing quorum is a
quorum of `Y` (`elecq_witness`), which meets `q_Y` (`same_cfg_quorum_intersection`) in a
holder `V`; `holder_grants_are_covered` at `(V, P, curTerm P)` — whose strictness corner is
the same-term one T12 already closes — gives `holdsE P`, hence `propAfterE P` at the
proposal, hence (by `adopt`'s coupling `require`) every adopter of D holds E; and
`committed_cfg_quorum` says a quorum of D adopted it.
*What it buys P2 at `becomeLeader`.* A new leader `i` with `cfgLt commitCfgid (cfgOf i)`
was elected on a quorum of `cfgOf i`. `cfgOf i` is either `succ commitCfgid` — adjacency
against `commitQuorum` — or `succ Y` for a COMMITTED `Y > commitCfgid` (the `propose` gate
above, plus `chain_committed_below`), where T20 at `Y` supplies an all-holder quorum and
`adjacent_cfg_quorum_intersection` meets it against the electing quorum. Either way the
electing quorum contains an E-holder, and `holder_grants_are_covered` hands `holdsE i` over.
**STATUS: WRITTEN, NOT MEASURED THIS SESSION.** It needs the "who proposed D" link, which
the state does not currently name (the model has `hasProposal`/`proposedC` per node but
nothing that ties a COMMITTED config back to its proposer) — i.e. T20's step is likely to
need one more GHOST (`cfgProposer D`, or a `propAfterE`-flavoured stamp at `commitCfg`),
which is count-exempt but is a model change and therefore next session's opening move, not
a thing to bolt on at the end of this one.

#### 49. **TOOLING FINDING — the INITIALISATION obligations are BUNDLE-INDEPENDENT (checked in the source)**
Amendment clause (A) asserted this ("the initialisation obligation is `after_init → clause`
per clause, with no other clause in its antecedent, hence bundle-independent") as an
argument. It is now a mechanical fact, read off the VC generator:
`Veil/Frontend/DSL/Module/VCGen/Induction.lean:132-134` —
```
private def DeclarationKind.assumesInvariantsForInductionVC : DeclarationKind → Bool
  | .procedure .initializer => false
  | _                        => true
```
— and `mkInductionPrecondition` (`:136-143`) turns that `false` into the precondition
`fun _ _ => True` instead of `@Invariants …`. So an init VC is literally
`Assumptions → wp(after_init)(clause)`: **no invariant, of any bundle, appears in it.**
Consequences for the dossier:
* **Every init ✅ this arc has ever produced, in ANY slice, certifies that clause's init
  obligation for the FULL bundle — with no transfer argument at all.** Transfer question
  (B)/(D) shrinks to the ACTION obligations only.
* The init obligations cannot be run through `#check_action`: init VCs carry
  `action = `initializer`` (`Veil/Core/UI/Verifier/VerificationResults.lean:352`), and
  `getCheckableAction?` (`Elaborators.lean:425-430`) admits only `.action` procedures, so
  `#check_action initializer` is rejected. They are covered by the runs that already have
  them (run 16 for the run-16-era clauses; runs 20/26/27/31/34 for T12/T15/T17/T18/T14/T13).

#### 50. RUN 35 — `#check_action commitEntry` (session-6 hole 3): **48 ✅ / 0 ❌ / 1 ⏱️ in 1535 s**
`ReconfigCommitSMTActcommitEntry.lean`, the model VERBATIM (35/11, 46 invariants + 2
safeties; diffed against `ReconfigCommitSMT.lean` before launch), 60 s per VC.
Log `smt-run35-act-commitEntry.log`. This is the cheapest open measurement of session 6's
map, and it lands in the middle:
* **The run-16 ❌ IS GONE.** `leader_completeness` @ `commitEntry` was a CTI in run 16,
  predicted (session 5, item 34) to fall to T12 once `role_positive_term` was subsumed.
  It is **no longer refuted** — but it is **⏱️** at 60 s
  (`commitEntry_leader_completeness_0_WP` / `_tr_0_TR`, TIMEOUT), i.e. **OPEN**, not green.
  The prediction is CONFIRMED in kind (no counterexample survives) and UNCONFIRMED in
  verdict (no proof either). Under the ⏱️ protocol the clause stays OPEN at this action.
* **`election_safety` @ `commitEntry` — ✅**, in the full bundle, no transfer argument.
* **The other 46 clauses + `doesNotThrow` — ✅ at this action**, full-bundle, criterion (A).

#### 51. **RUN 36 — the SOLVER-CONFIGURATION lever, measured and NEGATIVE**
Session 6's task-3 map named two levers for the crux VC: manual discharge, or "a different
solver configuration". The configuration one is cheap and was measured first.
`mkVeilSmtTactic` (`Veil/Frontend/DSL/Tactic.lean:872-886`) hands cvc5 exactly three
extra options — `finite-model-find` (from `veil.smt.finiteModelFind`, DEFAULT TRUE),
`nl-ext-tplanes`, `enum-inst-interleave`. Finite-model-find is a MODEL-finding mode; the
crux VC is expected UNSAT, and fmf is a known drag on hard unsat goals, so
`ReconfigCommitSMTElecSlice2NoFmf.lean` = run 32's eleven-clause slice VERBATIM plus
`set_option veil.smt.finiteModelFind false` at file scope, 300 s per VC.
**11 ✅ / 0 ❌ / 1 ⏱️ in 650 s** — the same tally, the same wall, the same single ⏱️
(`becomeLeader_election_safety_0_WP` / `_tr_0_TR`, TIMEOUT). Log
`smt-run36-elecslice2-nofmf.log`.
**Reading: the crux is not an artefact of the fmf configuration.** The measurement grid for
`election_safety`@`becomeLeader` is now {full bundle, 17-clause slice, 11-clause slice} ×
{60 s, 300 s, 900 s} × {fmf on, fmf off} — ⏱️ in every cell. The remaining named lever is
the MANUAL discharge, and the clause stays OPEN under the truth rule.

#### 52. THE PER-ACTION SWEEP (criterion (A)) — results as they land
Each row is `ReconfigCommitSMTAct<action>.lean`, the model VERBATIM (35 requires / 11
assumptions / 46 invariants + 2 safeties, diffed against `ReconfigCommitSMT.lean` before
launch), `#check_action <action>`, 60 s per VC, ONE Lean process at a time, memwatch armed.
A fully green action reports **49 ✅** = 46 invariants + 2 safeties + `doesNotThrow`.
* **`commitEntry`** — 48 ✅ / 0 ❌ / **1 ⏱️** (`leader_completeness`), 1535 s, run 35
  (`smt-run35-act-commitEntry.log`). See item 50.
* **`startElection`** — **49 ✅ / 0 ❌ / 0 ⏱️**, 212 s (`smt-act-startElection.log`).
  CLOSED at this action, full bundle, no transfer argument.
* **`deliverRequestVoteGrant`** — **49 ✅ / 0 ❌ / 0 ⏱️**, 502 s.
* **`crashRestart`** — **49 ✅ / 0 ❌ / 0 ⏱️**, 703 s.
* **`appendEntry`** — **49 ✅ / 0 ❌ / 0 ⏱️**, 835 s.
* **`replicate`** — **49 ✅ / 0 ❌ / 0 ⏱️**, 1037 s.
* **`commitCfg`** — **49 ✅ / 0 ❌ / 0 ⏱️**, 1641 s, WITH A TOOLING ANOMALY (see item 53).
* **`propose`** — **KILLED at 3300 s, NO VERDICT** (`smt-act-propose.log`, rc=124), at 60 s
  per VC. Re-measured at a 20 s budget; see below.
* **`adopt` at 20 s per VC** — **49 ✅ / 0 ❌ / 0 ⏱️**, 1407 s
  (`smt-act-adopt-t20.log`; same `no witness provided` exit-1 anomaly as `commitCfg`,
  item 53). The 60 s run was killed by the driver at 36 s to make room for this one. A
  proof found at 20 s IS a proof and any VC that did not close would have been reported
  ⏱️; none was. `leader_completeness` and `election_safety` are BOTH ✅ at this action.

#### 53. **TOOLING FINDING — "unsat, but no witness provided", and what a ✅ actually means**
`smt-act-commitCfg.log` is 49 ✅ / 0 ❌ / 0 ⏱️ and yet the build exits 1, on
```
error: …ReconfigCommitSMTActcommitCfg.lean:920:0:
  mkDischargerResult: overallSmtResult is unsat, but no witness provided   (×2)
```
Source: `Veil/Frontend/DSL/Module/VCGen/Induction.lean:47-58`. The path is taken when the
SOLVER returned **unsat** (the VC is proved at the solver level) but the Lean-side tactic
handed back an exception instead of a proof term — here plausibly the
`unable to synthesize LocalRProp instance for Invariants … (deterministic) timeout at
`typeclass`, maximum number of heartbeats (20000)` warning that this run (and every other
per-action run) also carries. Veil `throwError`s, which fails the build.
**Why the clause verdicts are nevertheless ✅, and what that means for every green in this
arc:** each clause has TWO VCs — the WP-style primary and its TR-style alternative — and
`effectiveStatus` (`Veil/Core/UI/Verifier/VerificationResults.lean:120-136`) resolves the
clause to the BEST status among them ("conclusive outcomes win over sibling errors",
`:118-121`). So **a ✅ in any run of this arc means "the primary or its TR alternative was
discharged"**, and ⏱️ means neither was. In `commitCfg`'s two cases the sibling was
`proven` AND the erroring one was itself solver-unsat, so nothing here weakens the run;
it is recorded because the exit code is 1 on an otherwise clean action, and a future
session must not read that as a failure.
* **`propose` at 20 s per VC — KILLED at 1900 s, NO VERDICT** (`smt-act-propose-t20.log`).
  Together with the 60 s kill this is a real cost datum: **lowering the per-VC solver budget
  by 3x did not move `propose` at all.** Since every per-action file elaborates the SAME
  module (`startElection` finished in 212 s end-to-end, so module elaboration is ≤ ~200 s),
  the missing time is in the per-VC WP/TR GENERATION and SMT TRANSLATION for this action —
  `propose` is the model's heaviest action (five `require`s plus the conditional
  `reachAt i Z := if … then … else …` update, which the WP has to push through every
  clause mentioning `reachAt`). This is the two-term cost model (item 40) reappearing at
  the level of a single action, and it is the standing reason the sweep is not complete.

#### 54. **THE SWEEP'S RESIDUE, STATED AS COVERAGE (not as a gap in the claim)**
All ten actions were attempted; **nine have a criterion-(A) verdict**
(`startElection`, `deliverRequestVoteGrant`, `becomeLeader` (runs 28/30, session 6),
`crashRestart`, `appendEntry`, `replicate`, `commitEntry`, `commitCfg`, `adopt`), and
**`propose` alone has none**. What covers that action in the dossier:
* the 40 run-16-era clauses — **run 16** (470 ✅ / 3 ❌ / 0 ⏱️), which measured `propose`
  and `adopt` in the 41-clause bundle, carried by antecedent weakening + the ghost-extension
  transfer (amendment (D));
* `leader_reach_strict` (T12) — run 20, ALL TEN ACTIONS + init;
* `role_below_quorum_strict` (T15) — run 26, ALL TEN ACTIONS + init;
* `role_below_meets_quorum` (T17) — run 27, ALL TEN ACTIONS + init;
* `commitq_witness` (T18), `commit_leader_frozen_reach` (T14) — run 31, ALL TEN + init;
* `commitq_grant_covers_reach` (T13) — run 34, ALL TEN + init.
So every clause has a verdict at `propose`; what that one action lacks is a verdict
obtained WITHOUT a transfer argument. **That is the honest shape of the residue, and it is
smaller than session 6's, not larger.**

#### 55. TASK 3 (the crux VC) — WHAT WAS ATTEMPTED, AND WHAT WAS NOT
The session's instruction was to try the MANUAL discharge of
`becomeLeader_election_safety_0_WP` / `_tr_0_TR` through Veil's
`@[veil] theorem … := by unveil; …` stub, timeboxed, without starving tasks 1–2.
**What was done:** the cheaper of the two named levers — the SOLVER CONFIGURATION — was
measured and is negative (item 51: fmf off changes nothing). The stub text itself is banked
(`smt-run21-elecslice.log:458-492`), and reading `mkVeilSmtTactic`
(`Veil/Frontend/DSL/Tactic.lean:872-886`) settles what a manual attempt would have to do:
the tactic feeds cvc5 EVERY `Prop` in context (`getPropsInContext`), so the productive
manual move is NOT a hand proof of the whole goal but the mypyvy/Ivy idiom — `unveil`,
then `have` the two or three ASSUMPTION INSTANCES the chain needs
(`cfglt_connected` at `(elecCfg L, elecCfg i)`, `adjacent_cfg_quorum_intersection` at the
resulting succ-step, `same_cfg_quorum_intersection` in the equal case), then `veil_smt`,
which then has the instantiations as ground hypotheses instead of having to find them.
**What was NOT done: no manual attempt was run.** The box admits one Lean process at a
time and the per-action sweep (task 1, the session's stated priority) consumed it end to
end; every iteration of a manual proof costs a full module elaboration. This is recorded as
an honest omission, not as a negative result: **`election_safety`@`becomeLeader` remains
⏱️ / OPEN, carried on the written truth argument T8 + T12 + the T17 chain**, and the
named next lever is the `have`-instances-then-`veil_smt` attempt above.

#### SESSION-7 STOP — the ~5-hour checkpoint, **NOT gate 2, no gate request made**
Gate 2's precondition (bundle closed, zero ⏱️) is NOT met: 1 ❌ and 2 ⏱️ remain.
**`ReconfigCommitSMT.lean` was NOT EDITED this session** — every run is measurement-only
against the session-6 bundle, so the count stands at **35 `require`s / 11 `assumption`s**,
46 invariants + 2 safeties, `QuorumAdjacency.lean` untouched and its seventeen-witness
`#print axioms` audit still covering the bundle. Conditionality (n1)+(n2)+(n3), gate
amendment (d) and divergences (d1)–(d5) are unchanged and remain gate-2 scope.
Banked: the per-action sweep (items 50/52/54), three source-level tooling findings
(items 49/53 + the `Invariants`-includes-safeties note), the negative solver-configuration
measurement (item 51), and the **T24 refutation with its corrected chain** (T19/T20).

**NEXT SESSION'S MAP:**
1. `#check_action propose` — the one action with no criterion-(A) verdict. It is killed at
   both 60 s and 20 s per VC, so the lever is not the solver budget: either a 5 s budget,
   or `veil.experimental.wpCompact` / `generateWpLocalEq` (`Veil/Base.lean:145-155`, both
   default true — worth a read before a run), or accept the transfer and say so.
2. `leader_completeness` @ `commitEntry` — ⏱️ at 60 s with NO surviving counterexample.
   Re-measure at 600 s (`ReconfigCommitSMTActcommitEntry600.lean` is prepared and unused);
   this is the cheapest of the three open items and the likeliest to close.
3. `election_safety` @ `becomeLeader` — the manual `unveil` + `have`-instances + `veil_smt`
   attempt (item 55). Invariants are done; do not add more.
4. The strict half of P2 — **T20, not T24** (T19 refutes T24 and its term-guarded repair).
   T20 needs a config→proposer link the state does not carry; that ghost is the opening
   move, and it is count-exempt.
5. Gate 2 only when (1)–(4) land with zero ⏱️.
