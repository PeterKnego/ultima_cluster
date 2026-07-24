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
- **V2 (coherence-window forward hunt):** NOT run. Bar-2b (its precondition) now
  passes, so it is unblocked for the next session. The #9/#6b depth probe — an
  explicit stretch item, never a gate — was also not attempted (§6).

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
it. **The deep-property depth is still being measured** (probes B/C were
re-running when this was written; their first attempt returned a knob-independent
artifact CE, since fixed — see the ledger).

### Fidelity work — eight gaps, and why they matter more than the verdicts

Eight across the session (six in `BootGate.lean`, two more surfaced by the #9
probe). Three were caught by hand-tracing the intended CE against the model text
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

1. **V2 forward hunt** — now unblocked (Bar-2b was its precondition): run the
   *fixed* model biased toward the election-time window (concurrent
   `startElection` / `crashRestart` / gate-reopen / commit interleavings), hunting
   a fifth countermodel. `BootGate.lean` is the natural base — it already carries
   the gate, the vote, the lagging `handleTerm`, and the commit plane.
2. **#9 / #6b depth probe** (stretch, NOT a gate). Much cheaper now: `handleTerm`
   is in place and `node.rs:2404-2418` spells out #9's shape — a CANDIDATE that
   cleanly reconciles a HIGHER-term leader's map *without adopting* reopens intake
   for its lagging handle-term stream and accepts a cross-stream byte. Needs
   reconcile-without-adopt + a reopen keyed on `SM term == handle term`, behind a
   `finding9Fix` knob. Calibrates whether "absence at depth N" means anything.
3. **Lift run 2's narrowing** (§3a qualification): let a node take a divergent
   tail from the sitting older-term leader, which needs per-term stream identity
   (a second tracked entry, or an `entryTerm`) rather than one tracked entry. Would
   upgrade run 2's SAFE from "within this restriction" to unqualified.
4. **`Reconfig.lean` commit/log plane** (F-M7-2) — still open, still blocked on the
   tractability boundary characterised in session 3; **USER DECISION PENDING**
   between (a) abstract-quorum reformulation + inductive proof (local, ~LC-arc
   S2-equivalent) and (b) a larger box for deeper bounded coverage (CE-only; the
   SAFE direction is exponential, not compute-bound). Unchanged by this session.
5. If V2 survives: a nightly Veil model-check job next to the `elle` tier — a
   deliberate CI follow-up, not part of the spike (guardrail 3).

## 7. Cost

≈1 session (this doc's session) for V-M7 on top of the prior 2 sessions' V0/V1.
Peak `lean` RSS during explicit-state runs ~5.7 GB (bounded by an active
memory-watch that kills on <2.5 GB free — the box has no swap; see CLAUDE.md).
No OOM. Model-check wall-clock per decisive run: seconds-to-~3 min at n=3.
