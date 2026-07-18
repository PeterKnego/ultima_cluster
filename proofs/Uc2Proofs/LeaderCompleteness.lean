import Mathlib.Tactic.FinCases
import Uc2Proofs.ProtocolCommit

/-! LB2b (Option-2 hybrid) — LEADER COMPLETENESS under an explicit
provenance hypothesis `FramesCurrentAuthored`.

## Status: BLOCKED on the FULL conditional theorem, past-ceiling — but the
## hypothesis design + its supporting UNCONDITIONAL lemmas + its required
## non-vacuity ARE landed and machine-checked (see the module doc further
## down, "`FramesCurrentAuthored` — the Option-2 provenance hypothesis").

Finding #7 (below) proved the FIXED, unconditional LC-core statement is
FALSE over the CURRENT model: `deliverReplicate`'s stamp guard
`hstamp : t ≤ currentTerm` (LA1's documented `≤`-over-approximation of two
Rust guards — an exact HEADER-term match plus the reconcile-before-data
intake gate, `uc2_net/src/receiver.rs` 636–639/657) lets a follower replay a
DEAD leader's stale in-flight frame across a reconcile boundary. This task's
job was to prove the CONDITIONAL form instead — LC-core plus exactly one
added hypothesis, `FramesCurrentAuthored w` (defined below: a node's held
content always agrees with what its OWN term map attributes to that
position — the model-level consequence of the same two Rust guards, a
provenance/content-canonicity fact that never mentions `committed` or
leader-hist completeness — see the predicate's own docstring for the full
faithfulness / non-circularity argument). Hand-verified against the Finding
#7 world: it is FALSE there (node 1's replayed entry is stamped 2 while its
own reconciled term map attributes position 1 to term 3), so the hypothesis
is SUFFICIENT to exclude the known countermodel, and
`nonvacuity_leader_completeness_trace` (below) proves it is not vacuous —
LB1's plain commit trace (current-leader-authored replication only)
satisfies it, machine-checked.

**What is NOT landed: the `leader_completeness` theorem itself.** Closing
the induction (the `becomeLeader` endgame in particular) needs supporting
invariant infrastructure this codebase does not yet have — a term-map
well-formedness (`Ascending`) invariant, and a message-indexed report-
provenance clause (the `grant_state`/S2 pattern, applied to `CMsg.report`)
tying a folded report's durable frontier back to the reporter's term-map
state at SEND time. Design work for both is recorded in
`.superpowers/sdd/task-LB2b-report.md`; per the task's stuck-protocol, this
exceeds the effort ceiling and is reported BLOCKED rather than sorried or
forced through with a weakened statement. `Reachable w →
FramesCurrentAuthored w` remains FALSE (Finding #7), so the hypothesis
stays an explicit, carried assumption either way; a later, separate task
(Option 1: split the frame's wire HEADER term from its record STAMP, plus a
`serveTail` re-serve constructor) is expected to DISCHARGE it unconditionally
for every reachable world — that model refinement is NOT this task.

## History

The original FIXED LC-core contract (LB2 brief, decision 5) was refuted
twice over by machine-checked countermodels that lived in this file
(commit `14cdcfc`; full traces and Rust evidence in
`.superpowers/sdd/task-LB2-report.md` and the lean gate doc):

- **Finding #6a (statement gap)**: the ghost recorded the data STAMP while
  Raft's Leader Completeness (§5.4.3) keys on the COMMIT term. FIXED by
  re-keying the ghost to `(position, stamp, commitTerm, payload)`
  (`Uc2Proofs/ProtocolCommit.lean`, module doc item 1).
- **Finding #6b (PROTOCOL gap, Raft §5.4.2 / Figure 8 — a REAL v2.x
  acked-write-loss bug)**: an old-term-only range could commit at the
  election base before the NewTerm frame was quorum-durable. FIXED in Rust
  (`election.rs::rank_leader` commit clamp) and mirrored as
  `leaderAdvanceCommit`'s `hbase` enabling (module doc item 9).

## Finding #7 — the re-keyed LC-core is STILL false in this model
(`finding_stale_replicate_replay_lc_violation`, 33-step kernel trace,
n = 3, + `lc_core_commit_term_keyed_is_false`)

**The re-keyed FIXED statement**

```
theorem leader_completeness {n : Nat} (w : World n) (hw : Reachable w)
    (p t T v : Nat) (hc : (p, t, T, v) ∈ w.committed)
    (i : Fin n) (hi : (w.nodes i).pn.role = .leader)
    (ht : T < (w.nodes i).pn.currentTerm) :
    (w.nodes i).hist p = some (t, v)
```

is refuted (in the `T < currentTerm` FIXED-contract form, hence a fortiori
in the `T ≤` strengthening) by a reachable trace whose pivot is
**cross-stream stale-frame replay**: `deliverReplicate`'s stamp guard
`hstamp : t ≤ currentTerm` (inherited verbatim from the data model, LA1
module doc item 6) lets a follower that has just reconciled CLEANLY against
the current T-leader's map re-accept a **stale lower-term replicate frame
from a dead leader's stream** at its truncated frontier, then accept a
genuine T-stamped byte on top. Its data-stamped map now ends in a `(T, b)`
entry, its durable covers the leader's commit range, and its intake gate
never closed — so its AppendPosition report at term T is truthful,
gate-open, and folded by the T-leader's tracker, certifying (through the
Finding-#6b clamp, which is satisfied: the range crosses the T base) a
commit range containing a position where the reporter's CONTENT diverges
from the leader's log. The reporter then wins term T+1 on its
`(lastTerm = T, durable ≥ k)` credentials — a leader above the commit term
that does not hold the committed entry.

**Trace shape** (n = 3; every enabling condition honest — no staleness
beyond ordinary in-flight frames, one crash used only to free node 0 from
its own stale leadership):

- t1: node 0 leads {0,1}, appends `(pos 0, stamp 1, payload 10)`; node 1
  reconciles (gate opens) and accepts it.
- t2: node 1 leads {1,2} and appends `(pos 1, stamp 2, payload 20)` — the
  future stale frame — reaching durable 2.
- t3: node 0 (crash-restarted to follower) wins {0,2} on credentials
  `(1, 1)`; `prunePush` opens its map `[(1,0),(3,1)]` (base₃ = 1); it
  appends `(pos 1, stamp 3, payload 30)` and `(pos 2, stamp 3, payload 31)`
  and gossips its map. Node 1 adopts term 3 via the RequestVote (gate
  closes), reconciles against the t3 map — its divergent t2 tail dies at
  `validUpTo = 1`, gate REOPENS — and then:
  - **re-accepts its own stale t2 frame** `(1, 2, 20)` at its truncated
    frontier 1 (`hstamp : 2 ≤ 3` — the over-approximation), then
  - accepts the leader's genuine `(2, 3, 31)` at frontier 2, so its map
    grows `[(1,0),(2,1),(3,2)]` — LAST ENTRY TERM 3 — durable 3.
- Node 1 reports `(term 3, durable 3)` (follower, gate open — truthful);
  the leader folds it; the kernel `advance` fires at k = 3; `hbase` holds
  (base₃ = 1 < 3); the ghost commits `(1, 3, 3, 30)` from the LEADER's
  hist — while node 1's hist at 1 is `(2, 20)`.
- t4: node 1 wins {1,2} on `(lastTerm 3, durable 3)`. Final world:
  `(1, 3, 3, 30) ∈ committed`, node 1 is leader, `3 < currentTerm 1 = 4`,
  `hist 1 1 = some (2, 20) ≠ some (3, 30)`.

**Classification: MODEL-FIDELITY gap, not a Rust bug.** Verified in source
this session: `uc2_net/src/receiver.rs:635-639` drops any DATA datagram
whose header `leadership_term_id` is not EXACTLY the adopted term
(`dropped_stale_term`) — record stamps ride inside the datagram BODY under
the CURRENT leader's header (catch-up/NAK repair re-serves old-stamped
records under the new leadership term), while adoption comes only from
consensus datagrams. So in Rust a follower's post-reconcile intake is
scoped to the live T-leader's stream and the Frankenstein log above is
structurally impossible. The model's `Frame.replicate pos term payload`
conflates the record STAMP with the datagram HEADER term, and the `≤`
guard (documented in LA1 as "the model's ≤ consequence of two Rust
guards") is sound for LOG MATCHING (which is per-(pos, stamp) and does not
care which stream delivered a byte) but UNSOUND for LEADER COMPLETENESS.

**Why no local repair exists inside this task's staging envelope**: the
faithful fix gives frames both a header term and a record stamp
(acceptance requires `header = currentTerm`; `observeTerm` keeps stamping
by the record stamp; a leader re-serves its hist under its own header via
a new serve-tail step) — a `Uc2Proofs/ProtocolData.lean` amendment that
re-opens the LA2 preservation proof, i.e. controller territory under the
LA1 rules, exactly like the #6a ghost re-key was. The alternatives
(equality-`hstamp`, or an enabling that names the live same-term leader's
content) under-approximate Rust — they erase the real catch-up-of-
old-stamped-bytes behavior the model exists to cover — and are recorded in
the task report as explicit controller trade-offs, not applied here.

The LC-core proof against the repaired model remains the next re-run's
deliverable; the invariant architecture (holders quorum + canonical-prefix
`SplitsAt` + Cert-at-T + the grant-time freshness chain) and the
establishment-order analysis this trace fell out of are in
`.superpowers/sdd/task-LB2-rerun-report.md`. -/

namespace Uc2.Cert

/-- Trace-discharge helper for `leaderAdvanceCommit`'s enabling (the LB1
pattern, re-proved locally — `ProtocolCommit.lean`'s copy is private): the
kernel cannot `decide`-reduce `advance` (`List.mergeSort` is well-founded
recursion), so the trace world's tracker/durable are pinned by kernel
`decide` and the concrete advance is discharged by `simp` once, here. -/
private theorem advance_fires {t : CommitTracker} {own : Nat}
    (t' : CommitTracker) (own' k : Nat)
    (ht : t = t') (hown : own = own')
    (hk : (t'.advance own').2 = some k) :
    (t.advance own).2 = some k := by
  subst ht
  subst hown
  exact hk

/-- **Finding #7 (KEPT — documents why `FramesCurrentAuthored` is needed).**
The cross-stream stale-replicate replay trace (module doc): a reachable
world satisfying every hypothesis of the re-keyed FIXED LC-core — a
genuinely committed `(1, 3, 3, 30)` (through the kernel tracker AND the #6b
`hbase` clamp) and a leader strictly above the commit term — whose leader
holds DIFFERENT content at the committed position (`(2, 20)`, its replayed
stale byte). This is exactly why the FIXED statement can only be proven
CONDITIONAL on `FramesCurrentAuthored` (BLOCKED for now — see the module
doc's Status section): this trace's final world does NOT satisfy
`FramesCurrentAuthored` (node 1's replayed entry at position 1 is stamped 2
while node 1's own term map, frozen by the clean reconcile against node 0's
`[(1,0),(3,1)]`, attributes position 1 to term 3 — `termAt ≠` the held
stamp), so `hprov` correctly excludes it. Option 1 (the header/stamp frame
split + `serveTail`, a separate follow-up task) is expected to make this
trace UNREACHABLE outright, discharging `FramesCurrentAuthored`
unconditionally; until then this countermodel remains the reachability
witness that the UNCONDITIONAL statement below it (`lc_core_...is_false`)
is genuinely false, and that `hprov` is doing real work, not padding. -/
theorem finding_stale_replicate_replay_lc_violation :
    ∃ w : World 3, Reachable w ∧
      (1, 3, 3, 30) ∈ w.committed ∧
      (w.nodes 1).pn.role = .leader ∧
      3 < (w.nodes 1).pn.currentTerm ∧
      (w.nodes 1).hist 1 = some (2, 20) ∧
      (w.nodes 1).hist 1 ≠ some (3, 30) := by
  refine ⟨_,
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail
      -- t1: node 0 leads {0,1} and replicates (0, 1, 10) to node 1.
      (.single (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 0 1 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 10 (by decide)))
      (.shipTermMap _ 0 (by decide)))
      (.deliverTermMap _ 1 1 [(1, 0)] (by decide) (by decide)))
      (.deliverReplicate _ 1 0 1 10 (by decide) (by decide) (by decide)
        (by decide)))
      -- t2: node 1 leads {1,2}; its (1, 2, 20) append is the stale frame.
      (.startElection _ 1 (by decide)))
      (.deliverRequestVote _ 2 1 2 1 1 (by decide) (by decide)))
      (.deliverVote _ 1 2 2 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 1 (by decide) (by decide)))
      (.leaderAppend _ 1 20 (by decide)))
      -- t3: node 0 (crash-restarted follower) wins {0,2} at term 3.
      (.crashRestart _ 0))
      (.startElection _ 0 (by decide)))
      (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 2 0 3 1 1 (by decide) (by decide)))
      (.deliverVote _ 0 2 3 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 30 (by decide)))
      (.leaderAppend _ 0 31 (by decide)))
      (.shipTermMap _ 0 (by decide)))
      -- node 1: adopt t3 (gate closes), reconcile clean (tail dies,
      -- gate reopens) ...
      (.deliverRequestVote _ 1 0 3 1 1 (by decide) (by decide)))
      (.deliverTermMap _ 1 3 [(1, 0), (3, 1)] (by decide) (by decide)))
      -- ... then REPLAY the stale t2 frame at the truncated frontier
      -- (hstamp: 2 ≤ 3 — the over-approximation) and a genuine t3 byte.
      (.deliverReplicate _ 1 1 2 20 (by decide) (by decide) (by decide)
        (by decide)))
      (.deliverReplicate _ 1 2 3 31 (by decide) (by decide) (by decide)
        (by decide)))
      -- the truthful gate-open report certifies the commit at k = 3
      -- (hbase: base₃ = 1 < 3 — the #6b clamp is satisfied).
      (.sendReport _ 1 (by decide) (by decide)))
      (.deliverReport _ 0 1 3 3 (by decide) (by decide) (by decide)
        (by decide)))
      (.leaderAdvanceCommit _ 0 3 (by decide) ⟨(3, 1), by decide⟩
        (advance_fires ⟨[3, 0], 2, 0⟩ 3 3 (by decide) (by decide)
          (by simp [CommitTracker.advance, CommitTracker.ranking,
                List.mergeSort]))))
      -- t4: the divergent reporter wins on (lastTerm 3, durable 3).
      (.startElection _ 1 (by decide)))
      (.deliverRequestVote _ 2 1 4 3 3 (by decide) (by decide)))
      (.deliverVote _ 1 2 4 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 1 (by decide) (by decide)),
    by decide, by decide, by decide, by decide, by decide⟩

/-- The re-keyed FIXED LC-core statement (`T < currentTerm` form, at
n = 3) is FALSE for the post-#6a/#6b model **without the provenance
hypothesis** — hence so is the `T ≤` strengthening (the countermodel
satisfies the strictly stronger hypothesis). KEPT (not deleted): it would
only become redundant once a `leader_completeness` theorem exists whose
ADDED hypothesis (`FramesCurrentAuthored w`) this countermodel's world is
shown to fail — which is exactly the shape this task designed
(`FramesCurrentAuthored` below, verified by hand against this trace) but did
NOT reach a machine-checked `leader_completeness` for (BLOCKED — see the
module doc's Status section). Until that theorem lands, this is the
machine-checked record of exactly why an unconditional statement cannot
work over the current model, motivating the conditional route. -/
theorem lc_core_commit_term_keyed_is_false :
    ¬ ∀ (w : World 3), Reachable w →
        ∀ (p t T v : Nat), (p, t, T, v) ∈ w.committed →
        ∀ i : Fin 3, (w.nodes i).pn.role = .leader →
        T < (w.nodes i).pn.currentTerm →
        (w.nodes i).hist p = some (t, v) := by
  intro h
  obtain ⟨w, hw, hc, hrole, hterm, -, hne⟩ :=
    finding_stale_replicate_replay_lc_violation
  exact hne (h w hw 1 3 3 30 hc 1 hrole hterm)

#print axioms finding_stale_replicate_replay_lc_violation
#print axioms lc_core_commit_term_keyed_is_false

/-! ## `FramesCurrentAuthored` — the Option-2 provenance hypothesis

**The hardest part of this task, per the brief: design a hypothesis that is
sufficient (closes Finding #7), faithful (exactly the strength of
`receiver.rs` 636–639 + the intake gate, no stronger), and non-circular
(never mentions `committed` or leader-hist completeness directly).**

### Design

`Frame.replicate pos term payload` (`ProtocolData.lean`) conflates two
things Rust keeps separate: the datagram's wire HEADER (`leadership_term_id`,
checked for EXACT equality against the receiver's adopted term,
`receiver.rs:636-639`) and the record's own STAMP (the term the byte was
originally written under, which can be OLDER than the header on a legitimate
catch-up/NAK-repair re-serve — the current leader re-ships an INHERITED
prefix byte under ITS OWN header). `deliverReplicate`'s `hstamp : t ≤
currentTerm` is LA1's documented `≤`-collapse of both guards into the single
stamp field. The Rust invariant this loses is: **the term MAP is exactly the
receiver's own record of which header-authenticated segment produced which
byte range** — `observeTerm` grows the map only when a just-accepted stamp
exceeds the map's frontier, and Rust's exact header-match means every BYTE
accepted while the map's frontier sits at term `u` was authenticated by the
term-`u` stream. Consequently, in real UC, a node's held content at a
position is ALWAYS the term its OWN term map attributes to that position —
`hist j p = some (t, v) → termMap_j.termAt p = t`. `FramesCurrentAuthored`
asserts exactly this, for every node, at the given world.

Why this survives the Finding-#7 replay unscathed for LEGITIMATE catch-up
but excludes the replay: `observeTerm` only ever GROWS the map (case
`t > lastTermOf m`), never shrinks or rewrites it, so a genuine inherited-
prefix delivery (stamp `t` strictly below the map's current frontier,
landing at a position the map's EXISTING entries already attribute to `t`)
leaves `termAt` unchanged and consistent — `FramesCurrentAuthored` says
nothing against it. Finding #7's replay is a `t ≤ lastTermOf m` delivery
whose stamp (2) does NOT match what the receiver's OWN (already-reconciled-
to-the-live-leader) map attributes to that position (3, per node 1's map
`[(1,0),(3,1)]` after its clean reconcile) — `FramesCurrentAuthored` is
FALSE at that world (see the amended Finding #7 docstring above), so `hprov`
rules the countermodel out.

### Faithfulness argument (for the docstring, as the brief requires)

The predicate speaks ONLY about a node's own two local fields (`hist`,
`termMap`) agreeing with each other — it is the direct, node-local
CONSEQUENCE of "every accepted byte was authenticated by the stream the
map's frontier currently names," which is exactly what the exact
header-match guard (`receiver.rs:636-639`) plus the reconcile-before-data
intake gate (`receiver.rs:657`, closing ingestion until the map itself is
resynchronized) jointly establish in Rust. It asserts NOTHING stronger:
it does not require `t = currentTerm` (that would forbid legitimate
inherited-prefix catch-up — the rejected `hstamp`-equality alternative the
LB2-rerun report already documented as an under-approximation), and it does
not reach into ANY other node's state, `committed`, or `dsent` — a node's
own consistency is checkable from its own two fields alone.

### Non-circularity argument (for the docstring, as the brief requires)

The predicate never mentions `committed`, never mentions "every leader has
the entry," and never mentions cross-node agreement. It is symmetric in
every position and every node, including nodes that are followers, nodes
with no committed content anywhere near them, and worlds with an EMPTY
`committed` ledger (vacuously about nothing). The bridge from
`FramesCurrentAuthored` to leader completeness is NOT a rewrite of the
hypothesis; it would be the theorem's OWN work, combining the hypothesis
with an already-proven, UNCONDITIONAL fact (`Uc2.Data.log_matching`) and an
INDUCTIVE, hprov-FREE invariant (NOT mechanized by this task — see the
module doc's Status section and the task report) that a committed entry's
stamp is exactly what every sufficiently-advanced later leader's term map
attributes to its position. The hand-verified argument for why that
invariant needs no `hprov` (it survives the Finding-#7 replay unscathed,
since `observeTerm` only ever GROWS a term map — a `t ≤ lastTermOf`
delivery, replay included, cannot corrupt it) is recorded in the task
report; mechanizing it needs a term-map well-formedness (`Ascending`)
invariant plus a message-indexed report-provenance clause this codebase
does not yet have, which is why this task stops short of it.

**Sufficiency, checked by hand against Finding #7**: node 1's replayed
entry is stamped 2, but node 1's own (correctly-reconciled) term map
attributes position 1 to term 3 — `FramesCurrentAuthored` is FALSE at that
world, so it excludes the known countermodel; see the amended Finding #7
docstring above. -/

/-- **The Option-2 hybrid hypothesis.** A node's held content always agrees
with what its OWN term map attributes to that position. See the module doc
immediately above for the full faithfulness / non-circularity / sufficiency
argument; in one line, this is the model-level trace of Rust's exact
DATA-header match (`receiver.rs:636-639`) plus the reconcile-before-data
intake gate (`receiver.rs:657`): an accepted byte's term is always the term
the receiver's own (header-authenticated) map segment names for that byte.
`Reachable w → FramesCurrentAuthored w` is FALSE over the CURRENT model
(Finding #7 above is the countermodel) — it is designed to be carried as an
EXPLICIT hypothesis on a `leader_completeness` theorem (NOT mechanized by
this task — BLOCKED, see the module doc's Status section), not derived,
pending the Option-1 model refinement (header/stamp split + `serveTail`)
that is expected to discharge it unconditionally. -/
def FramesCurrentAuthored {n : Nat} (w : World n) : Prop :=
  ∀ j : Fin n, ∀ p t v : Nat, (w.nodes j).hist p = some (t, v) →
    Uc2.TermMap.termAt (w.nodes j).dn.termMap p = t

/-! ## Frame provenance (UNCONDITIONAL — no `hprov` needed)

Bookkeeping the endgame needs: every stamped history entry any node
currently holds traces back to an actual `replicate` frame that was put on
the wire — `hist` entries are never minted out of thin air. Only
`leaderAppend` (co-emits the matching frame in the same step) and
`deliverReplicate` (copies FROM an already-wired frame, `hmsg`) ever write a
`some` into `hist`; `deliverTermMap`'s truncation only ERASES entries. Pure
structural fact, independent of `hprov`. -/

/-- The frame-provenance invariant, carried through the induction. -/
private def HistFrameProvenance {n : Nat} (w : World n) : Prop :=
  ∀ j : Fin n, ∀ p t v : Nat, (w.nodes j).hist p = some (t, v) →
    Uc2.Data.Frame.replicate p t v ∈ w.dsent

private theorem hfp_init (n : Nat) : HistFrameProvenance (World.init n) := by
  intro j p t v h
  simp [World.init, Node.hist] at h

private theorem hfp_step {n : Nat} {w w' : World n} (h : HistFrameProvenance w)
    (hs : Step w w') : HistFrameProvenance w' := by
  cases hs with
  | startElection i _ =>
    intro k p t v hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t v hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t v hh
  | deliverRequestVote j c nt clt cd hmsg hterm =>
    intro k p t v hh
    rcases eq_or_ne k j with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t v hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t v hh
  | rejectStaleRequestVote j c nt clt cd hmsg hstale =>
    intro k p t v hh
    exact h k p t v hh
  | deliverVote i v t hmsg hrole hterm =>
    intro k p t' v' hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t' v' hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t' v' hh
  | deliverVoteHigherTerm i v t g hmsg hterm =>
    intro k p t' v' hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t' v' hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t' v' hh
  | becomeLeader i hrole hquorum =>
    intro k p t v hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t v hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t v hh
  | crashRestart i =>
    intro k p t v hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t v hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t v hh
  | leaderAppend i v hrole =>
    intro j p t v' hh
    rcases eq_or_ne i j with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh
      by_cases hp : p = (w.nodes i).pn.durable
      · subst hp
        rw [Function.update_self, Option.some.injEq, Prod.mk.injEq] at hh
        rw [← hh.1, ← hh.2]
        exact List.mem_append_right _ (by simp)
      · rw [Function.update_of_ne hp] at hh
        exact List.mem_append_left _ (h i p t v' hh)
    · simp only [Node.hist, Function.update_of_ne (Ne.symm hne)] at hh
      exact List.mem_append_left _ (h j p t v' hh)
  | deliverReplicate j pos t v hmsg hpos hstamp hgate =>
    intro k p t' v' hh
    rcases eq_or_ne j k with rfl | hne
    · simp only [Node.hist, Function.update_self, Uc2.Data.Node.recvReplicate] at hh
      by_cases hp : p = pos
      · subst hp
        rw [Function.update_self, Option.some.injEq, Prod.mk.injEq] at hh
        rw [← hh.1, ← hh.2]
        exact hmsg
      · rw [Function.update_of_ne hp] at hh
        exact h j p t' v' hh
    · simp only [Node.hist, Function.update_of_ne (Ne.symm hne)] at hh
      exact h k p t' v' hh
  | shipTermMap i hrole =>
    intro k p t v hh
    exact List.mem_append_left _ (h k p t v hh)
  | deliverTermMap j t entries hmsg hterm =>
    intro k p t' v' hh
    rcases eq_or_ne j k with rfl | hne
    · simp only [Node.hist, Function.update_self, Uc2.Data.Node.applyGossip] at hh
      cases hrec : Uc2.reconcile (w.nodes j).dn.termMap (w.nodes j).dn.pn.durable
          entries with
      | ok o =>
        rw [hrec] at hh
        dsimp only at hh
        split at hh
        · exact h j p t' v' hh
        · cases hh
      | noCommonPrefix =>
        rw [hrec] at hh
        dsimp only at hh
        cases hh
    · simp only [Node.hist, Function.update_of_ne (Ne.symm hne)] at hh
      exact h k p t' v' hh
  | sendReport j hrole hgate =>
    intro k p t v hh
    exact h k p t v hh
  | deliverReport i src t d hmsg hrole hterm hsrc =>
    intro k p t' v' hh
    rcases eq_or_ne k i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h k p t' v' hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h k p t' v' hh
  | leaderAdvanceCommit i k hrole hbase hadv =>
    intro j p t v' hh
    rcases eq_or_ne j i with rfl | hne
    · simp only [Node.hist, Function.update_self] at hh; exact h j p t v' hh
    · simp only [Node.hist, Function.update_of_ne hne] at hh; exact h j p t v' hh

/-- **Frame provenance.** Every currently-held stamped history entry traces
back to an actual `replicate` frame on the wire. Unconditional (no `hprov`):
pure structural bookkeeping, established by induction over `Reachable`. -/
theorem hist_frame_provenance {n : Nat} {w : World n} (hw : Reachable w) :
    HistFrameProvenance w := by
  induction hw with
  | refl => exact hfp_init n
  | tail _ hstep ih => exact hfp_step ih hstep

/-- A committed entry's `(position, stamp, payload)` traces back to an
actual `replicate` frame on the wire — the corollary of frame provenance at
the `leaderAdvanceCommit` event (`ghostEntries` reads straight off the
committing leader's `hist`). Unconditional (no `hprov`). -/
private def CommittedFrameProvenance {n : Nat} (w : World n) : Prop :=
  ∀ p stamp T v : Nat, (p, stamp, T, v) ∈ w.committed →
    Uc2.Data.Frame.replicate p stamp v ∈ w.dsent

private theorem cfp_init (n : Nat) : CommittedFrameProvenance (World.init n) := by
  intro p stamp T v h
  simp [World.init] at h

private theorem cfp_step {n : Nat} {w w' : World n} (hw : Reachable w)
    (h : CommittedFrameProvenance w) (hs : Step w w') :
    CommittedFrameProvenance w' := by
  cases hs with
  | startElection _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | deliverRequestVote _ _ _ _ _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | rejectStaleRequestVote _ _ _ _ _ _ _ =>
    intro p stamp T v hh; exact h p stamp T v hh
  | deliverVote _ _ _ _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | deliverVoteHigherTerm _ _ _ _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | becomeLeader _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | crashRestart _ => intro p stamp T v hh; exact h p stamp T v hh
  | leaderAppend i v hrole =>
    intro p stamp T v' hh
    exact List.mem_append_left _ (h p stamp T v' hh)
  | deliverReplicate _ _ _ _ _ _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | shipTermMap i hrole =>
    intro p stamp T v hh
    exact List.mem_append_left _ (h p stamp T v hh)
  | deliverTermMap _ _ _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | sendReport _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | deliverReport _ _ _ _ _ _ _ _ => intro p stamp T v hh; exact h p stamp T v hh
  | leaderAdvanceCommit i k hrole hbase hadv =>
    intro p stamp T v hh
    rcases List.mem_append.mp hh with hold | hnew
    · exact h p stamp T v hold
    · obtain ⟨p', hp', heq⟩ := List.mem_filterMap.mp hnew
      cases hcase : (w.nodes i).dn.hist p' with
      | none => rw [hcase] at heq; simp at heq
      | some tv =>
        rw [hcase] at heq
        simp only [Option.map_some, Option.some.injEq, Prod.mk.injEq] at heq
        obtain ⟨rfl, rfl, rfl, rfl⟩ := heq
        exact hist_frame_provenance hw i p' tv.1 tv.2 hcase

/-- **Committed-entry frame provenance.** Unconditional (no `hprov`):
established by induction over `Reachable`. -/
theorem committed_frame_provenance {n : Nat} {w : World n} (hw : Reachable w) :
    CommittedFrameProvenance w := by
  induction hw with
  | refl => exact cfp_init n
  | tail hprev hstep ih => exact cfp_step hprev ih hstep

#print axioms hist_frame_provenance
#print axioms committed_frame_provenance

/-! ## Non-vacuity of the conditional

Required deliverable (brief): a reachable world satisfying BOTH `hprov` and
non-trivial LC-core premises, so the conditional theorem is not vacuously
true of an empty hypothesis set. Reuses LB1's `nonvacuity_commit_completeness_trace`
shape (`ProtocolCommit.lean`) — the SAME 14-step trace, rebuilt here so its
concrete `hist`/`termMap` values are available to check `FramesCurrentAuthored`
directly: node 0 leads term 1, appends `42` at position 0, gossips, node 1
reconciles CLEANLY (`[(1,0)]`, no truncation — the trace never diverges) and
replicates the SAME byte at its OWN current term, so every held entry is
either fresh (`t = currentTerm`) or matches the frozen `[(1,0)]` map — never a
cross-stream replay. This confirms `hprov` is satisfied by ordinary,
current-leader-authored replication, exactly the brief's claim. -/
/-- The concrete trace, bundled with every DECIDABLE fact about it the
`FramesCurrentAuthored` proof below needs (`w`'s `Reachable` witness is
established here, ALONGSIDE the facts, while `w` is still the concrete,
kernel-transparent term the `refine` unifies — `decide` cannot see through it
once `obtain` turns `w` into an opaque local, so every decidable fact is
harvested in this one shot). -/
private theorem nonvacuity_lc_trace :
    ∃ w : World 3, Reachable w ∧
      (0, 1, 1, 42) ∈ w.committed ∧
      (w.nodes 1).pn.role = .leader ∧
      1 < (w.nodes 1).pn.currentTerm ∧
      w.dsent = [.replicate 0 1 42, .gossip 1 [(1, 0)]] ∧
      (w.nodes 0).dn.termMap.termAt 0 = 1 ∧
      (w.nodes 1).dn.termMap.termAt 0 = 1 ∧
      (w.nodes 2).hist 0 = none := by
  refine ⟨_,
    .tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail (.tail
      (.tail (.tail (.tail (.tail
      (.single (.startElection _ 0 (by decide)))
      (.deliverRequestVote _ 1 0 1 0 0 (by decide) (by decide)))
      (.deliverVote _ 0 1 1 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 0 (by decide) (by decide)))
      (.leaderAppend _ 0 42 (by decide)))
      (.shipTermMap _ 0 (by decide)))
      (.deliverTermMap _ 1 1 [(1, 0)] (by decide) (by decide)))
      (.deliverReplicate _ 1 0 1 42 (by decide) (by decide) (by decide)
        (by decide)))
      (.sendReport _ 1 (by decide) (by decide)))
      (.deliverReport _ 0 1 1 1 (by decide) (by decide) (by decide)
        (by decide)))
      (.leaderAdvanceCommit _ 0 1 (by decide) ⟨(1, 0), by decide⟩
        (advance_fires ⟨[1, 0], 2, 0⟩ 1 1 (by decide) (by decide)
          (by simp [CommitTracker.advance, CommitTracker.ranking,
                List.mergeSort]))))
      (.startElection _ 1 (by decide)))
      (.deliverRequestVote _ 2 1 2 1 1 (by decide) (by decide)))
      (.deliverVote _ 1 2 2 (by decide) (by decide) (by decide)))
      (.becomeLeader _ 1 (by decide) (by decide)),
    by decide, by decide, by decide, by decide, by decide, by decide, by decide⟩

theorem nonvacuity_leader_completeness_trace :
    ∃ w : World 3, Reachable w ∧ FramesCurrentAuthored w ∧
      (0, 1, 1, 42) ∈ w.committed ∧
      (w.nodes 1).pn.role = .leader ∧
      1 < (w.nodes 1).pn.currentTerm := by
  obtain ⟨w, hw, hc, hrole, hterm, hdsent, ht0, ht1, hh2⟩ := nonvacuity_lc_trace
  refine ⟨w, hw, ?_, hc, hrole, hterm⟩
  intro j p t v hh
  -- Route through the UNCONDITIONAL frame-provenance lemma: `dsent` is flat
  -- (not per-node `Function.update`-nested), so pinning `(p, t, v)` this way
  -- avoids ever unfolding the 14-step `hist` chain symbolically.
  have hfp := hist_frame_provenance hw j p t v hh
  rw [hdsent] at hfp
  simp only [List.mem_cons, List.not_mem_nil, or_false,
    Uc2.Data.Frame.replicate.injEq] at hfp
  rcases hfp with ⟨rfl, rfl, rfl⟩ | hfp
  · have hj : j = 0 ∨ j = 1 ∨ j = 2 := by omega
    rcases hj with rfl | rfl | rfl
    · exact ht0
    · exact ht1
    · rw [hh2] at hh; cases hh
  · exact absurd hfp (by simp)

#print axioms nonvacuity_leader_completeness_trace

end Uc2.Cert
