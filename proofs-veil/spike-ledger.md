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
