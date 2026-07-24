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
- **V2 (coherence-window forward hunt):** NOT run this session — deferred with
  its commit-plane Bar-2/Bar-2b calibration (see §6). Per Amendment-3 V-M7 was
  the primary and was taken first; V2 is the next session's work.

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
| **Bar-2** (shallowest known bug, Finding #5) | rediscover the boot-gate phantom commit from a pre-fix commit-plane model | **NOT RUN** — needs the commit plane; deferred to V2 (§6) |
| **Bar-2b** (frame abstraction preserves the bug class) | show the abstraction still distinguishes the hazard it targets | **PASS in V-M7 form** — the reconfig model demonstrably reproduces the disjoint-quorum data-loss shape (§4); the commit-plane #9 variant is deferred with Bar-2 |

Per Amendment-3, V-M7 needs only V1's port + Bar-1 (both passed), so it ran
first; Bar-2/Bar-2b's *coherence-window* form is V2 work.

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

**KEEP.** Bar-1 passed, V-M7 landed real assurance + a sharp fidelity finding,
and both Veil engines are confirmed on the UC model. The tool fits.

Next session (V2 + the commit plane), in priority order:

1. **Commit/log plane** for `Reconfig.lean`: couple `adopt` to the committed
   prefix + a committed-entry marker, then check **leader_completeness**
   (committed-entry survival). Expect guarded-SAFE / ablated-UNSAFE on the *same*
   property — the clean V-M7 result F-M7-2 scopes.
2. **Bar-2 / Bar-2b (coherence window):** port the election-time commit plane
   (boot intake gate + vote), revert Finding #5, confirm `#model_check`
   rediscovers the shallow phantom-commit; then the #9 cross-stream depth-probe.
3. **V2 forward hunt** for a fifth countermodel in the election-time coherence
   window on the *fixed* model.
4. If it survives: a nightly Veil model-check job next to the `elle` tier — a
   deliberate CI follow-up, not part of the spike (guardrail 3).

## 7. Cost

≈1 session (this doc's session) for V-M7 on top of the prior 2 sessions' V0/V1.
Peak `lean` RSS during explicit-state runs ~5.7 GB (bounded by an active
memory-watch that kills on <2.5 GB free — the box has no swap; see CLAUDE.md).
No OOM. Model-check wall-clock per decisive run: seconds-to-~3 min at n=3.
