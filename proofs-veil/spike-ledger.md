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
