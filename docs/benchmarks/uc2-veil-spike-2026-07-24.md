# UC v2 — Veil spike gate doc (V3): bounded model-checking as a bug-hunting oracle

**Date:** 2026-07-24
**Brief:** `docs/superpowers/specs/2026-07-19-uc2-veil-spike-brief.md`
(Amendment-3 sequence: V0 pre-flight → V1 port + Bar-1 → session-1 re-gate →
**V-M7 (primary)** → V2 coherence-window hunt).
**Branch:** `uc2/veil-spike`. Scratch models archived under `proofs-veil/`
(guardrail-isolated; never on `proofs/`'s build path or CI).
**Tool:** `verse-lab/veil` `veil-2.0-preview` @ Lean v4.28.0 (the branch that
carries the explicit-state `#model_check` reachability engine). Run inside a
separate checkout where cvc5/z3 FFI links; **never the record** — `proofs/`
(Lean v4.32.0, standard axioms) remains the sole trusted base.

---

## 1. TL;DR

The spike is a **KEEP**. Across three sessions Veil's two engines were both
confirmed on UC's consensus model, and the primary target (V-M7 reconfiguration)
produced real design-assurance findings:

- **V0 (maturity):** PASS. `veil-2.0-preview` builds here; `#model_check`
  executes (concrete traces, FFI-linked). The brief had conflated two features;
  corrected: SMT-inductiveness + CTI live in both Veil branches, the
  explicit-state **reachability** checker (the "find a new bug" machine) is
  `veil-2.0-preview` only.
- **V1 + Bar-1:** PASS. UC election plane ported; `#check_invariants` (SMT)
  independently re-proves `election_safety` + the 5-clause `Inv` inductive
  (all-n); `#model_check` explores 60761 states clean at n=3. **Both engines fit
  the UC model.**
- **V-M7 (primary):** a working Veil model of UC single-server reconfiguration;
  `election_safety` verified robustly safe across config change (even ablated);
  the checker **rediscovers the textbook disjoint-quorum data-loss shape**
  (calibration passed); and **Finding F-M7-2** pins down exactly what a faithful
  leader-completeness check for M7 requires.
- **Bar-2 + Bar-2b (session 4):** **both PASS** — the two remaining must-pass
  bars are now discharged (§3a). A pre-fix `BootGate.lean` yields the Finding-#5
  phantom commit as an 8-step CE matching the shipped bug's shape exactly, the
  post-fix model is clean over an exhaustive 312009 states, non-vacuity is
  witnessed, and the frame abstraction demonstrably still distinguishes a
  stale-handle-term stream byte from a current-term one at the same position.
  Six model-fidelity gaps were found and adjudicated against the Rust along the
  way — the substantive output of the session.
- **Depth probe (sessions 4b/4c, stretch — never a gate):** run on BOTH #9 and
  #6b, and it produced the session's most reusable result. Each bug splits into a
  **shallow proxy invariant** (the condition its shipped guard establishes) and a
  **deep end-to-end loss**: #9's proxy at **depth 7**, #6b's §5.4.2 proxy at
  **depth 5**, while #9's full loss is beyond depth 13 and #6b's needed 46 steps
  in Lean. Hence the operating rule for V2: **hunt proxy invariants, not
  end-to-end loss properties** (§3b, §3c). #6b's full-loss depth remains
  **unmeasured** — blocked by a hard `Fin 4` memory wall on this box (§3c).
- **V2 (coherence-window forward hunt):** **RUN — exhaustive, no violation over
  11,697,699 states** (n=3, term=Fin 3, all shipped fixes on, seven guard-shaped
  invariants, no depth bound, no state constraints; §3d). **No fifth
  coherence-window bug at this scale.** Per the brief's exit criteria this is the
  acceptable non-discovery outcome, and it is stronger than the "credible bounded
  coverage" it asks for — the search was exhaustive over the model's reachable
  space, not depth-limited.

No claim in `proofs/` or any "proved" status is affected. Nothing was migrated
out of `proofs/`.

## 2. Port fidelity

The models are faithful in shape to `proofs/Uc2Proofs/`:

- **Election plane** (`Election.lean` / `ElectionMC.lean`): mirrors the S2 model
  in `Protocol.lean` — `startElection` / `deliverRequestVoteGrant` /
  `deliverVote` / `becomeLeader` / `crashRestart`, with the data-plane grant
  guard (`logOk`) abstracted to a nondeterministic predicate (sound for
  election-safety scope). All 5 `Inv` clauses + `election_safety` ported
  verbatim in shape. Quorums: abstract `member` + intersection assumption (Lean
  C5) for SMT; concrete "excludes-one" / `count*2>|cfg|` majority for
  explicit-state.
- **Reconfig plane** (`Reconfig.lean`): config as an evolving per-node voter
  `nodeSet` — the `TLA/Raft.lean` `isQuorum` cardinality idiom applied to a
  *changing* set. Single-server change via `insert`/`remove` (inherently a
  one-member diff); one-change-in-flight (`pending`); leader-adopts-at-append /
  follower-adopts-when-durable; the `FRAME_TYPE_CONFIG` propagation modelled as
  a term-stamped adoption.

**Two fidelity corrections were forced during the port (Finding F-M7-1) — the
abstraction-obligation discipline the brief mandates:**

1. **Member-restricted quorum.** The first cut (`|votes|*2 > |cfg|`) let a lone
   self-vote from a *non-member* clear a size-1 config. Corrected to
   `|votes ∩ cfg| * 2 > |cfg|` — only votes from *current config voters* count.
2. **Term-coupled adoption.** The first `adopt` let a node ingest a leader's
   config entry while remaining an independent same-term candidate — physically
   impossible in UC, where config entries ride term-stamped replication frames
   (receiving one = hearing from the leader, so a candidate reverts to follower,
   Raft §5.2). Corrected: `adopt` requires `curTerm j ≤ curTerm i` and sets
   `candidate/leader j := false`, `curTerm j := curTerm i`.

Both are real UC mechanisms; documented inline. This is the V-M7 analog of the
LC arc's Finding #8 (a model-fidelity gap forcing a faithful model).

## 3. Bars

| Bar | Definition | Result |
|---|---|---|
| **Bar-1** | `#check_invariants` certifies the already-proved election `Inv` inductive | **PASS** — 43 ✅, `election_safety` + 5 clauses inductive via cvc5, all-n (session 1) |
| **Bar-2** (shallowest known bug, Finding #5) | rediscover the boot-gate phantom commit from a pre-fix commit-plane model | **PASS** (session 4) — `BootGate.lean`, pre-fix run returns the phantom commit as an **8-step CE matching Finding #5's shape exactly**; the post-fix run is clean and exhaustive (§3a) |
| **Bar-2b** (frame abstraction preserves the bug class) | show the abstraction still distinguishes the hazard it targets | **PASS** — in V-M7 form (the reconfig model reproduces the disjoint-quorum data-loss shape, §4) **and now in its commit-plane #9 form** (session 4, §3a) |

All three must-pass bars are now discharged. Per Amendment-3, V-M7 needed only
V1's port + Bar-1 (both passed) so it ran first; Bar-2/Bar-2b's commit-plane
form landed in session 4.

## 3a. Bar-2 / Bar-2b — the boot-gate commit plane (session 4)

Model `proofs-veil/models/BootGate.lean`, log
`proofs-veil/logs/bootgate-bar2-bar2b-runs.log`. One build, four `#model_check`
runs at n=3 / term=Fin 2, 7m33s. Knob `bootGateFix`: false = pre-fix (gate boots
open unconditionally), true = the shipped fix (`node.rs:533-534`, gate closes iff
`vote_term > map_term`).

| Run | Knobs | Result | Reading |
|---|---|---|---|
| 1 | `bootGateFix := false` | ❌ `no_phantom_commit` | **BAR 2 — PASS** |
| 2 | `bootGateFix := true` | ✅ no violation, **312009 states, exhaustive** | fix calibrated: the CE is gone |
| 3 | `+ vacuityCanary` | ❌ `genuine_commit_canary` | **non-vacuity confirmed** — run 2's SAFE is not "nothing ever commits" |
| 4 | `+ bar2bCanary` | ❌ `bar2b_stream_distinction` | **BAR 2b — PASS** (a violation is the good outcome for a canary) |

Runs 1 and 2 are only worth anything *as a pair*: the checker finds the bug in
the pre-fix model AND loses it in the post-fix one, on the same model. Run 3
exists because a SAFE verdict over a model that can never commit would be
worthless.

**Bar-2's CE is the Finding-#5 shape, in order:** node 1 takes a divergent tail
→ node 0 starts an election → node 1 GRANTS term 1 (gate closes, `mapTerm` stays
0) → node 0 wins → **node 1 crash-restarts and, pre-fix, boots its gate OPEN with
`vote_term 1 > map_term 0`** → node 0 appends → node 1 reports its divergent
durable AT TERM 1 → node 0 commits with holders `{0}`, no quorum holding E.

**Bar-2b** is discharged by construction and then witnessed: the model keeps
`durableTo` (bytes at P landed — what AppendPosition reports) independent of
`holdsEntry` (those bytes are the current leader's), and the checker exhibits a
reachable state where two nodes hold bytes at the SAME position from DIFFERENT
streams. The V2 window hunt is therefore not blind to its target class.

## 3b. The #9 depth probe (session 4b) — stretch item, NOT a gate

Model `proofs-veil/models/Finding9.lean`, log
`proofs-veil/logs/finding9-depth-probe.log`. The brief asks how deep a bound is
needed before the checker rediscovers #9's cross-stream accept, to calibrate
whether "absence at depth N" means anything for the forward hunt.

**Headline: #9's cross-stream reopen is reachable at DEPTH 7, in 1m44s**
(n=3, term=Fin 3, pre-fix). BFS returns the shallowest violation, so 7 is the
*minimum* depth and "unreachable at any smaller bound" follows for free — no
ladder of runs required. The trace is exactly the scenario `node.rs:2404-2423`
describes: a node adopts term 1 (handle := 1, gate closed), becomes a
**candidate at term 2** so its handle stays at 1 — `StartElection` bumps
`current_term` but stores no handle — and then cleanly reconciles a term-2
leader's map *without adopting*, which pre-fix **reopens intake for its stale
handle-term stream**. End state: `gateOpen`, `handleTerm = 1`, `mapTerm = 2`.

**This is materially better news than session 3 suggested.** The ReconfigLC wall
(a ~13-step CE unreachable in 700s) implied deep-bug probes were hopeless at
n=3; #9's *enabling condition* turns out to be shallow and cheap. `term = Fin 3`
was structural (adopt T1 → candidate at T2 → leader at T2) and stayed
affordable — RAM never dropped below 12.6 GB free.

The probe distinguishes two properties on purpose: `no_cross_stream_reopen` (the
invariant the shipped guard establishes — shallow) and `no_phantom_commit` (the
full acked-write-loss — deep). They are knob-gated because BFS halts at the
first violation, so the depth-7 proxy would otherwise mask the deep hunt.
Expressing #9's loss at all required new state, `tailAttributed`: the byte is one
"its map never attributed", so a later clean reconcile cannot detect or truncate
it.

| Probe | Knobs | Bound | Result |
|---|---|---|---|
| A | pre-fix, proxy ON | maxDepth 12 | ❌ `no_cross_stream_reopen` at **depth 7**, 1m44s |
| B | **post-fix**, proxy ON | maxDepth 12 | ✅ no violation, 879650 states — guard closes the reopen |
| C | pre-fix, proxy OFF | maxDepth 13 | ✅ no violation, 1288622 states — full loss **not reached** |

**B and C are BOUNDED results, not safety claims.** Both passed a `maxDepth`, and
`✅ No violation` renders identically for exhaustion and for a depth bound
(`TraceDisplay.lean:104`). B reads as "no cross-stream reopen within depth 12
under the shipped guard"; C as "the full acked-write-loss was not reached within
depth 13", not "it is absent".

### The calibration answer

**#9 splits into a shallow enabling condition and a deep consequence:** the
enabling condition (intake open at a lagging handle) sits at **depth 7 / 1m44s**,
while the full acked-write-loss is **beyond depth 13** even pre-fix, after 1.29M
states. For the V2 forward hunt at n=3 / Fin 3 that means: *absence at depth ~13
is worth very little for an end-to-end data-loss property, and a great deal for
an invariant-shaped proxy.*

The practical guidance — **hunt proxy invariants (the conditions the shipped
guards establish), not end-to-end loss properties.** Each of the four known bugs
has such a proxy (#5: a report escaping an unreconciled boot; #9: intake open at
a lagging handle), and the proxy sits roughly six steps shallower than the loss
it enables. This is the most useful thing the probe produced and it directly
shapes how V2 should be run. It is also consistent with session 3's ReconfigLC
wall: the wall is real, but it sits *above* the proxy depth, which is why this
probe landed where that one stalled.

## 3c. The #6b Figure-8 probe (session 4c) — second calibration point

Model `proofs-veil/models/Figure8.lean`, log
`proofs-veil/logs/figure8-probeD.log`. Chosen as the sharpest available test of
§3b's guidance: #6b's full loss was machine-checked in Lean as a **46-step, n=5**
countermodel, while its proxy — the Raft §5.4.2 barrier — should be trivial. If
the guidance were overfit to #9, #6b is where that would show.

**Probe D (pre-fix, n=3 / Fin 3): proxy CE at DEPTH 5, in 1m22s.** A new leader
(`new_term_pos = None`) takes one honest post-reconcile AppendPosition floor
report and commits the inherited old-term range with no quorum on this term's
NewTerm frame — the §5.4.2 barrier violated in five steps.

**The guidance holds on a second bug, and the split is wider here:** proxy at
depth 5 versus a full loss that needed 46 steps in Lean. §3b's rule is not
overfit to #9.

### The box wall — an honest negative, and a hard operational limit

A faithful Figure-8 needs **three election terms** (rival at T1, leader at T2,
rival again at T3), so the full-loss probes require `term := Fin 4`. At n=3 /
Fin 4 / maxDepth 13, `lean` reached **12.1 GB RSS** and drove the box to 2.37 GB
available — **killed under the box-safety rule** (no swap; an OOM SIGKILLs the
largest process and can take the session with it). Lean buffers verdicts until
the file finishes elaborating, so 15+ minutes produced **no partial output**.

**Operational limit for this box: `term := Fin 4` at n=3 is not viable for this
model class.** Fin 3 runs stayed comfortable throughout (≥12.6 GB free). This is
a sharper constraint than session 3's ReconfigLC time-wall — that one merely
failed to converge; this one endangers the session.

**Consequence, stated plainly: the #6b full-loss depth is UNMEASURED, not
absent.** Probes E and F never ran, so nothing is known about whether the clamp
prevents the loss itself as opposed to its proxy. An earlier pre-patch run that
appeared to find a loss at depth 8 was an artifact (a leader committing a range
it had already discarded) and is **retracted, not banked**.

### Fidelity work — eleven gaps, and why they matter more than the verdicts

Eleven across the session (six in `BootGate.lean`, two from the #9 probe, three
from the #6b probe). One recurred **three separate times**: letting a leader
commit a range it does not itself hold — the standing trap of this modeling
style, since `rank_leader` ranks the quorum-th durable *including the leader's
own*. Three were caught by hand-tracing the intended CE against the model text
before running it; five were caught by adjudicating a counterexample against the
Rust rather than accepting it. Every one of the first three biased the result the
*same* way — making the shipped fix look ineffective — which is the dangerous
direction, since a Bar-2 red is the spike's DROP verdict. The standing lesson:
**the checker finds the model's bugs long before UC's, so a CE is a question, not
an answer.** Full detail in `proofs-veil/spike-ledger.md`; the headline three:

- **Term adoption must close the intake gate** (`node.rs:2511-2513`). Without it
  the model admits a shallower phantom commit that is *not* Finding #5, so Bar-2
  would have "passed" on the wrong bug.
- **Reports are stamped with the receiver's HANDLE term, not the consensus term.**
  `term_handle.store` has exactly two call sites (`BecomeLeader`,
  `BecomeFollower`) — no candidate path. This independently rediscovers the
  distinction `node.rs:2404-2418` already names for Finding #9 ("a CANDIDATE runs
  its data plane at a LAGGING handle"), and it is also what makes Finding #5 work:
  the handle is seeded at boot from `boot_term = max(vote_term, map_term)`.
- **Stale-stream bytes cannot appear on a node synced to the sitting leader** —
  DATA is filtered at `adopted_term == term_handle` (`receiver.rs:635`).

**Qualification on run 2, stated plainly:** that last guard also rules out the
ordering where a node takes a divergent tail *from* the sitting older-term leader.
With one tracked entry at one tracked position the class is still represented (the
Bar-2 CE takes its stale bytes while no leader sits), but run 2's SAFE is
therefore **"safe within this restriction", not unqualified**.

## 3d. V2 — the coherence-window forward hunt (session 5)

Model `proofs-veil/models/V2Hunt.lean`, log
`proofs-veil/logs/v2-hunt-run1.log`. **The only part of the spike that hunts an
unknown bug.** Everything before it was backward-looking calibration — revert a
known fix, confirm the checker finds the known bug, restore it, confirm the CE
disappears. Here every shipped fix is ON, so a counterexample would be a bug
nobody knows about. Gated on Bar-2b, which is what makes a *null* result
informative rather than vacuous.

Base: `Finding9.lean` with `crashRestart` restored (the brief asks for concurrent
`startElection` / `crashRestart` / gate-reopen / commit interleavings, and crash
was the one window ingredient that model had dropped). 17 actions, 7 invariants,
aimed per §3b/§3c at **proxy invariants** — `no_cross_stream_reopen` (#9's guard),
`no_phantom_commit` (#5/#6b class), `no_unattributed_report`,
`gate_shut_while_unreconciled`, `handle_never_leads_current`,
`leader_handle_is_current`, and `election_safety` as a tripwire.

**Result: exhaustive, no violation — 11,697,699 states in 93m10s.** Verified
before being believed: zero `error: Examples/...` lines (not a voided run) and no
`maxDepth` anywhere, so `✅ No violation` here is `exploredAllReachableStates`
rather than `reachedDepthBound` — the two render identically.

**What it does and does not establish.** It does: over this model's entire
reachable space at n=3 / Fin 3, with all shipped fixes in place, none of the seven
guard-shaped invariants can be violated. It does **not**: generalise to UC. It
inherits every abstraction obligation recorded for `BootGate.lean` /
`Finding9.lean` (nondeterministic grant guard, collapsed tally, one tracked entry
at one tracked position, concrete excluded-node quorums, the narrowed
`staleStreamAppend` guard), and says nothing about n≥4 or a fourth term value.
**It is not a proof; `proofs/` remains the sole record.**

**`state_constraint` was deliberately not used — and turned out not to be needed.**
Constraints *prune* states, so for a hunt whose value is finding something unknown,
narrowing first is self-defeating: you cannot find what you pruned. Run 1 was
unconstrained to establish an honest baseline, and it completed, so the lever was
never spent. It stays in reserve for n=4, where it would buy tractability at a cost
that would then have to be stated.

### Capacity envelope — this answers the "do we need a bigger box" question

11.7M states is ~9× the largest prior run and finished with `lean` peaking near
7 GB against a 15 GB box.

| Configuration | Outcome |
|---|---|
| n=3 / Fin 3, exhaustive, unconstrained | **affordable** — 11.7M states, ~93 min, ~7 GB |
| n=3 / **Fin 4** | not viable — 12.1 GB RSS, killed (§3c) |
| n=4 / Fin 3, constrained, maxDepth <10 | **vacuous by construction** (§3e) |
| n=4 / Fin 3, constrained, maxDepth ≥10 | not viable — killed at >60 min (§3e) |

**n=3 / Fin 3 is the frontier for this model class on a 15 GB box, in both
directions** — more terms and more nodes each hit a wall. **A larger AWS box is not
required for the V2 forward hunt as scoped** (the local box does n=3
exhaustively); it becomes the only option for pushing *past* the frontier, alongside
the two blocked measurements: #6b's full-loss depth and ReconfigLC's
`leader_completeness` counterexample hunt.

## 3e. n=4 with state constraints (session 5b) — no viable window

Model `proofs-veil/models/V2Hunt4.lean`. Asks the one question n=3 cannot: does a
**fourth node** enable a coherence-window bug three cannot? Not idle — at n=4 the
majority is 3-of-4, so two successive quorums can overlap in exactly **two** nodes,
a structure with no n=3 analogue, and #6b's full loss needed n=5 in Lean. Kept in a
separate file because `state_constraint` is module-level and would otherwise
retroactively narrow §3d's exhaustive n=3 result.

**Attempt 1 (C1+C2+C3, maxDepth 10): completed — and vacuous.** 143,901 states in
6m31s. The tell was the count itself: 143,901 at n=4 against 11.7M at n=3. A
*larger* configuration yielding 80× *fewer* states is a symptom, not a result. A
vacuity canary confirmed it — `¬committed` was never violated, so **no commit is
reachable at all**, and most of the invariant battery was trivially true.

The culprit was **C1, "at most one node awaiting reconciliation"** — which sounded
like an anomaly bound but isn't: `deliverRequestVoteGrant` sets
`awaitingReconcile := true` on *every* node adopting a new term, and at n=4 a
candidate needs three granters, so ≥2 nodes awaiting reconcile is the **mainline
election path**. C1 pruned normal elections outright. It is kept in the file as a
*retired* constraint with the explanation — a better warning than its absence.
Reported as "n=4 clean, 143,901 states", it would have read as coverage while being
the exact opposite.

**Attempt 2 (C2+C3 only, maxDepth 10): killed at >60 min with zero output** — Lean
buffers verdicts until elaboration ends (§3c).

**Why no cheaper retry exists.** A commit at n=4 needs ~10 steps minimum
(startElection, *two* grants for a 3-of-4 majority, becomeLeader, appendEntry, two
replicates, two reports, commitEntry) versus ~7 at n=3. So `maxDepth < 10` is
**vacuous by construction**, and `maxDepth ≥ 10` is **intractable here**. The window
between vacuous and intractable does not exist at n=4 on this box — a sharper
statement than "we ran out of time", because lowering the bound cannot help when the
bound is what makes the run mean anything.

**Rule adopted: every constrained run must be paired with a vacuity canary, run
first.** Constraints can silently destroy the behaviour they were meant to make
searchable, and a clean verdict looks identical either way — the same class of trap
as an elaboration error silently voiding a `#model_check`.

## 4. V-M7 — the primary hunt (results)

Three decisive `#model_check` runs at n=3 with concrete `ExtTreeSet (Fin 3)`
configs (logs in `proofs-veil/logs/`):

| # | Property | Mode | Verdict | States |
|---|---|---|---|---|
| 1 | `election_safety` | ablated (arbitrary config jumps), term Fin 2 | ✅ **SAFE** | 187907 |
| 2 | `quorum_overlap` | ablated, term Fin 3 | ❌ **VIOLATED** (disjoint-quorum CE) | — |
| 3 | `quorum_overlap` | guarded (single-server adjacency), term Fin 3 | ❌ VIOLATED (false positive — see F-M7-2) | — |

**Run 1 — election safety is robustly safe.** Even with the adjacency guard
*removed*, no two leaders in the same term can form. The guarantor is **term
discipline**, not config adjacency: term-coupled adoption reverts a candidate to
follower on hearing from a leader, so a node that adopts a fresh config consumes
that leader's term and must seek a strictly higher term to re-elect — precluding
a same-term disjoint double-election. This is a genuine (and stronger than
expected) assurance result for M7.

**Run 2 — the checker catches the reconfig bug class (calibration).** Dropping
the adjacency guard, the checker finds the textbook single-server disjoint-quorum
shape: node 2 wins term 1 under `{0,1,2}` (quorum `{0,2}`), self-removes/removes
down to config `{1}`, a follower adopts the **non-adjacent** `{1}` in one jump
and wins term 2 (quorum `{1}`); electing quorums `{0,2}` and `{1}` are disjoint.
This is exactly the data-loss hazard single-server adjacency exists to prevent —
the checker reaches it in seconds. The model is expressive enough to catch the
class (the V-M7 Bar-2b analog).

**Run 3 + Finding F-M7-2 — a model-fidelity boundary (NOT a UC bug).** The
guarded model *also* violates `quorum_overlap`, via a **valid adjacent chain**
`{0,1,2}→{1,2}→{2}` (leader self-removes, then removes node 1): two leaders end
with disjoint electing quorums `{0,1}` and `{2}`. This is a **false positive of
the property**, and it is instructive:

- Single-server change **deliberately** permits non-overlapping quorums across
  *non-adjacent* configs — adjacency only guarantees *consecutive*-config
  overlap, never first-vs-last.
- Real UC stays safe because **config changes are log entries**: a node in config
  `{2}` necessarily holds the committed prefix (including node 0's term-1 entry),
  so nothing is lost. The model's `adopt` grants a config *without* requiring the
  committed prefix, so quorum-overlap / election-restriction properties report a
  data loss that cannot occur in UC.
- A secondary artifact compounds it: a self-removed leader's `leader` flag
  lingers (no step-down modeled), so a property quantified over *current* leaders
  over-counts benign stale leaders that cannot commit.

**Conclusion:** a faithful V-M7 **leader-completeness** check requires a
commit/log plane that couples config-entry adoption to holding the committed
prefix — the exact M7 analog of the LC arc's data-plane refinement (Findings
#7/#8). Scoped as the next modeling phase (§6).

## 5. Two questions to reconfirm in Rust (per the brief's "any hit → Rust")

F-M7-2 is a model boundary, not a bug, but it surfaced two concrete questions
worth a directed check against the real M7 code before a full leader-completeness
model:

1. **Self-removed-leader step-down window.** Does a UC leader that removes itself
   step down promptly once the removing config commits, and is there a window in
   which a self-removed leader still serves a (stale) linearizable read? (M7
   self-removal is supported — fleet gate 3.22s — and self-*demote* is refused;
   the read-barrier + service-epoch backstop are the relevant guards.)
2. **Adopt-requires-committed-prefix.** Is config-entry adoption on the M7 path
   actually gated on holding the committed log prefix (it should be, since config
   is an in-stream log entry adopted by the archive frame-scan) — i.e., can a
   node ever count toward a new config's quorum without the prior committed
   entries?

Both are expected to hold by construction; the value is a directed confirmation.

### §5 DISCHARGED 2026-07-26 — both CONFIRMED-SAFE (directed Rust trace, post-Rung-A code)

Run against `main` @ `29e324f`, i.e. AFTER the Rung A batch-probe rework of the
read barrier, so the answers hold for the current read path, not the one this
doc's line anchors point at.

**Q1 (self-removed-leader read window): CONFIRMED-SAFE.** The window is real and
deliberate — a self-removing leader keeps serving from append until the removing
config commits (Ongaro's rule: C_new must be replicated by a leader that still
exists; `adopt_config` defers the halt, election.rs:1307-1310) — but every read
served in it is certified by an ack set that **intersects every possible C_new
and C_old election quorum**: adoption-at-append re-derives the round quorum over
C_new (`rebuild_peer_maps` also voids the in-flight round), and the removed
leader needs ⌈n/2⌉ *genuine* C_new ackers on top of its (non-voter) self-seed —
a set no C_new majority can avoid, with the follower ack-iff-term-equal filter
making the intersection sound and the Rung A ordering rule guaranteeing ack
sends postdate admission. At commit, `StepDownRemoved` → `halt()` → `do_work`
short-circuits and the commit counter freezes. Notable subtlety the trace
corrected: **commit does NOT imply a quorum of followers has already adopted**
(Reports come from the receiver's durable counter; adoption waits for the
consensus agent's archive-observation drain), so follower probe-refusal must
never be cited as the guard — the quorum-intersection argument is.

**Q2 (adopt-requires-committed-prefix): CONFIRMED-SAFE.** The Veil model's
`adopt`-without-prefix move has no Rust counterpart: config frames are detected
only in the archive's recorded-block walk over the CONTIGUOUS fsynced prefix
(receiver publishes `append` only at the contiguous frontier; archive records
and fsyncs only that prefix; the consensus drain belt-checks `position <=
durable`), Reports carry exactly that contiguous durable frontier, commit is
bounded by leader-own-durable, and the below-floor path substitutes committed
snapshot state + the snapshot-carried authoritative config with
empty-journal-only floor adoption. Adjacency needs no guard beyond the single
shared `ClusterConfig::apply` (±1 voter, one change in flight).

**Adjacent observations from the trace (non-blocking, recorded here):**

1. **Liveness blemish:** reads admitted in the same duty cycle as
   `StepDownRemoved` (raw `sm.can_serve()` window, ≤64 reads) are parked
   forever — `do_work` short-circuits before their deadline can RETRY them;
   client timeout is the only recovery. Bounded, liveness-only.
2. `maybe_adopt_incoming_snapshot` fiat-adopts the snapshot's config/lineage on
   `durable < pos` and only then sends `AdoptFloor`, which the archive refuses
   for a non-empty journal — a mid-life follower completing an inbound transfer
   in an odd interleaving could have SM config/lineage overwritten while its
   physical log stays put. Normal paths wipe-and-rejoin first.
3. The self-removal commit window is deliberately more permissive than Ongaro
   §4.2.2 (documented decision, election.rs:1337-1381); Q1's read arithmetic is
   independent of it, but a future tightening should revisit both sites.
4. `on_read_probe_ack` completes a round without re-checking term/serving at
   the completion site — verified safe under both intra-drain orderings; the
   safety is carried by `advance_pending_reads`' subsequent checks.

## 6. Disposition + next steps

**KEEP — and now on the brief's own terms.** The exit criterion was: KEEP iff
Bar-1 + both must-pass bars (2, 2b) passed AND at least one of {V-M7 surfaced or
cleared a config-change scenario, V2 gave a real interleaving or credible bounded
coverage} landed. **All three bars have now passed** (Bar-1 session 1, Bar-2 and
Bar-2b session 4) and V-M7 landed. The DROP conditions are both explicitly
excluded: Bar-2b is not unfixable, and the checker *did* rediscover the shallow
Finding-#5 bug — which was the only failure that would have licensed "the tool is
wrong for this codebase".

Next session, in priority order:

1. ~~V2 forward hunt~~ — **DONE (§3d).** Exhaustive at n=3 / Fin 3 over 11.7M
   states, no violation; no fifth coherence-window bug at this scale.
   ~~n=4 follow-on~~ — **ATTEMPTED (§3e), no viable window on this box**: below
   depth 10 it is vacuous by construction, at depth ≥10 it is intractable. Reaching
   n=4 needs a bigger box or a structurally cheaper abstraction, not tuning.
2. ~~#9 / #6b depth probe~~ — **DONE (§3b, §3c).** #9's enabling condition at
   depth 7 (full loss beyond depth 13); #6b's §5.4.2 proxy at depth 5. **Still
   open: #6b's full-loss depth**, blocked by the `Fin 4` box wall (§3c) — it
   needs either a bigger box or an abstraction that expresses a 3-term Figure-8
   without a fourth term value.
3. **Lift run 2's narrowing** (§3a qualification): let a node take a divergent
   tail from the sitting older-term leader, which needs per-term stream identity
   (a second tracked entry, or an `entryTerm`) rather than one tracked entry. Would
   upgrade run 2's SAFE from "within this restriction" to unqualified.
4. **`Reconfig.lean` commit/log plane** (F-M7-2) — **DECIDED 2026-07-26:
   option (a)**, abstract-quorum reformulation + inductive proof. Dispatch
   brief:
   `docs/superpowers/specs/2026-07-26-uc2-veil-reconfig-commit-plane-brief.md`
   — which now inherits the discharged §5 Q2 trace as the verified Rust
   ground-truth map for the commit plane. Option (b) rejected: the SAFE
   direction is exponential, so a bigger box buys CE depth only, never the
   assurance result this item exists for.
5. If V2 survives: a nightly Veil model-check job next to the `elle` tier — a
   deliberate CI follow-up, not part of the spike (guardrail 3).

## 7. Cost

≈1 session (this doc's session) for V-M7 on top of the prior 2 sessions' V0/V1.
Peak `lean` RSS during explicit-state runs ~5.7 GB (bounded by an active
memory-watch that kills on <2.5 GB free — the box has no swap; see CLAUDE.md).
No OOM. Model-check wall-clock per decisive run: seconds-to-~3 min at n=3.
