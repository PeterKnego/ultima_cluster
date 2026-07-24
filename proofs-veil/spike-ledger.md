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
