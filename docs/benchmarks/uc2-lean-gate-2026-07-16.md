# UC v2 Lean proofs gate — Phase 1 (kernels)

**Date:** 2026-07-16
**Branch:** `uc2/lean-proofs` (base `main` @ `e23b1e2`)
**Head commit at gate time:** `0cc3899` (Task 13, conformance rig) — this doc
lands as Task 14's commit on top.
**Toolchain:** `leanprover/lean4:v4.32.0` (`proofs/lean-toolchain`)
**Mathlib pin:** `v4.32.0` (rev `81a5d25`, `proofs/lakefile.toml`)
**Build:** `lake build` — 3021 jobs, zero warnings, zero `sorry`.

This is the permanent record for Phase 1 of the Lean proofs arc: a
mathlib-free `Uc2Model/` mirroring `uc2_consensus`'s three pure-sync safety
kernels (`CommitTracker`, `reconcile`, vote-freshness/`log_ok`), 14 sorry-free
theorems over that model in `Uc2Proofs/`, and an executable conformance rig
(`Conform/`) that replays real Rust output through the model and diffs it bit
for bit. Plan: `docs/superpowers/plans/2026-07-16-uc2-lean-proofs.md`. Spec:
`docs/superpowers/specs/2026-07-16-uc2-lean-proofs-design.md`. Task-by-task
detail: `.superpowers/sdd/progress.md` + `.superpowers/sdd/task-{1..13}-report.md`.

## Theorems proved (all sorry-free)

All 14 theorems pass the axiom check (`#print axioms`) with only the standard
Lean/mathlib trio (`propext`, `Classical.choice`/`Quot.sound` where `Finset`
cardinality reasoning pulls them in) — no `sorry`, no `native_decide`, no
project-local axiom escape hatches.

| # | Theorem | File | Sim-invariant mapping |
|---|---|---|---|
| C1 | `commit_mono_step` / `commit_mono_run` | `Uc2Proofs/Commit.lean` | commit is non-decreasing across any single event or run — a structural precondition every other commit invariant leans on |
| C2 | `advance_le_own` | `Uc2Proofs/Commit.lean` | commit never certifies past the caller's own reported durable position |
| C3 | `advance_certified` (step) / `commit_certified_run` (run, **strengthened** — see Findings) | `Uc2Proofs/Commit.lean` | **↔ inv7's quorum half**: every commit value was backed by quorum-many reported positions at or above it at the step it was set |
| C4 | `reset_then_advance_none` (immediate) / `reports_only_from_onDurable` (provenance, **restated** — see Findings) | `Uc2Proofs/Commit.lean` | a reset genuinely clears certifying power; every post-reset reported value is traceable to a real `on_durable` call |
| C5 | `quorum_intersect` | `Uc2Proofs/Quorum.lean` | any two majority (`n/2+1`) subsets of a fixed cluster share a member — the Raft quorum-overlap axiom, as a Lean `Finset` fact over `Fin n` |
| R1 | `reconcile_validUpTo_le` | `Uc2Proofs/Reconcile.lean` | reconcile's output `validUpTo` never exceeds the caller's durable frontier `d` |
| R2 | `reconcile_preserves_shared_prefix` | `Uc2Proofs/Reconcile.lean` | **↔ inv4's local form**: bytes both sides already agree on below `d` stay valid after reconcile |
| R3a | `reconcile_cuts_own_conflict` (fixed with `hne : leader ≠ []` — see Findings #1) | `Uc2Proofs/Reconcile.lean` | our own first beyond-common-prefix entry is never certified valid |
| R3b | `reconcile_cuts_leader_uncovered` | `Uc2Proofs/Reconcile.lean` | a leader term certified below `d` that our map doesn't cover is proven divergence, always cut |
| R4 | `reconcile_validUpTo_eq_firstDivergence` | `Uc2Proofs/Reconcile.lean` | **↔ inv4's local form (exactness half)**: under a strengthened data-stamped contract (see Findings #2), `validUpTo` is exactly the first byte-content divergence, not merely a sound lower/upper bound |
| R5 | `reconcile_idempotent` | `Uc2Proofs/Reconcile.lean` | re-reconciling reconcile's own output against the same leader map is a fixed point |
| R6 | `noCommonPrefix_iff` | `Uc2Proofs/Reconcile.lean` | exact characterization of when reconcile reports `NoCommonPrefix` (wipe-and-rejoin) vs `Ok` |
| V1 | `vote_unique_per_term` | `Uc2Proofs/Vote.lean` | **↔ inv5's seed**: a voter cannot grant two different candidates in the same term (proved via the stronger `c1 = c2 ∨ term-advanced`, per the brief's granted latitude) |
| V2 | `grant_implies_logOk` + `logOk_iff` | `Uc2Proofs/Vote.lean` | a granted vote implies the candidate's log passed the freshness check; `logOk_iff` gives the exact `(term, durable)` lexicographic characterization |
| V3 | runtime-assumption docstring (not a theorem) | `Uc2Proofs/Vote.lean` | records the external assumption `log_ok`'s freshness check relies on (data-stamped log positions), transcribed verbatim from the brief |

C5's `quorum_intersect` is also the **Tier B foundation**: any inductive
whole-cluster-run safety proof (Phase 1.5+) needs quorum overlap as its base
lemma, which is now a standalone, dependency-light Lean fact rather than an
assumption.

## Conformance

`uc2_consensus/examples/conform_gen.rs` (zero-dep — the crate's
`[dependencies]` stayed empty) drives real `CommitTracker::advance`/
`on_durable`/`reset_reports`, `reconcile::reconcile`, and
`election::log_ok_order` under a seeded splitmix64 PRNG, emitting
`{fn, ..., expect}` JSONL vectors where `expect` is the implementation's own
output. `proofs/Conform/Main.lean` (`lake exe conform`) re-derives each
vector's outcome from `Uc2Model` and diffs it against `expect`, exiting 0 (all
agree), 1 (divergence, offending line printed), or 2 (malformed/usage) — all
three branches live-verified (task-13-report.md).

- **Implementer run:** 100,000 vectors, seed `20260716` (the ISO date, per
  the brief's daily-rotation convention) — **zero divergence**, `real 0m1.376s`
  replay.
- **Independent reviewer re-run:** 20,000 vectors, seed `424242` — **zero
  divergence**.
- **Vector distribution** (from the 100k run, confirmed diverse rather than
  degenerate): ~8% `NoCommonPrefix` outcomes, ~4,000 vectors exercising a real
  truncation (`validUpTo` strictly below at least one side's max base), ~2,400
  full-tie `log_ok` calls (`our_term == cand_term`, decided by durable
  comparison), ~20% tracker-reset events.
- **No model corrections were needed during T13** — the conformance rig
  exercises three kernels the theorem layer (T7-T12) had already independently
  proved sound against the model; the zero-divergence result is a second,
  executable line of evidence, not a bug hunt that found anything.

Nightly `lean-proofs` job (`.github/workflows/nightly.yml`) reruns this daily
with `--seed "$(date +%Y%m%d)"`, rotating coverage while staying reproducible
from the run log; local reruns use `$HOME/.cache/uc2-conform/` (never `/tmp` —
see the box rule in `CLAUDE.md`).

**Honest note on spec §5's "plus every hand-written edge vector from the Rust
unit tests" clause.** That coverage is not carried by the JSONL replay above —
the replay only carries the seeded-random distribution, and a hand-written
edge case has no guarantee of being sampled. It is instead discharged
separately, at build time, as `#guard` pins inside `Uc2Model`: every
`reconcile.rs` unit test (all 10) and every `commit.rs` unit test are each
ported to an executable `#guard` assertion next to the corresponding model
definition, checked on every `lake build` rather than via replay. As of this
fix, `commit.rs` is fully pinned — the two trailing assertions of
`three_node_commit_is_second_highest_bounded_by_own` (repeat `advance` at an
already-reached commit returns `none`; own durable catching up past it still
returns `some`) were previously unpinned and are now covered
(`proofs/Uc2Model/Commit.lean`). The only exclusions are the 2
`#[should_panic]` constructor tests (`leader_must_be_a_member`,
`too_few_tracked_followers_is_rejected`) — the model takes those constructor
preconditions as hypotheses rather than encoding a panic — and the
`mutation-testing`-feature test (`forced_quorum_minus_one_commits_without_any_report`),
which exercises an injected-bug knob outside the default build.

## Findings

The value ledger. Two forced strengthenings of the informal contract, one
statement that was outright false and got a principled fix, and one candidate
real gap in the Rust that the proof work surfaced but did not itself resolve.

1. **R3a as originally spec'd was FALSE.** `reconcile_cuts_own_conflict`
   without qualification claims our first beyond-common-prefix entry is never
   certified valid — but the empty-leader early return (`reconcile.rs`'s
   "a leader with no map tells us nothing — clean as-is" branch) never clamps
   at all. Countermodel (task-10-report.md): `own = [(1,0),(2,4096)]`,
   `d = 8000`, `leader = []` — `validUpTo = d = 8000 > 4096`, violating the
   unqualified claim; reviewer-verified by `#eval`. **Fix:** added
   `hne : leader ≠ []` (the same exclusion `R4` already needed, so no new
   precedent), landed as a follow-up commit (`232666b`) after controller
   sanction — the statement is unweakened for the only regime `reconcile`
   actually clamps in.

2. **The informal `DataStamped` contract was INSUFFICIENT for R4.** Proving
   `reconcile_validUpTo_eq_firstDivergence` (task-11-report.md) required three
   additional fields on top of the brief's five, each pinned as strictly
   necessary by a hand-verified countermodel (`#guard`, violating only the
   named field with all others held true):
   - `own_stamped` / `leader_stamped` — no retained *shadowed phantom* below
     the durable frontier (an entry whose base is below `d` must genuinely
     stamp that byte, not merely be consistent with `termAt`). Without this,
     `encodes` can't see a shadowed entry but `reconcile`'s entry-wise
     comparison can, and the theorem is false (countermodels: own
     `[(1,0),(2,500),(5,500)]` @ `d=1000` vs leader `[(1,0),(5,500)]` — cuts
     at 500 but both histories genuinely agree to 1000; and the
     `leader_stamped` case below).
   - `leader_pos` — leader map terms are `≥ 1` (protocol-true: `election.rs`'s
     construction sites never push term 0).
   - `shared_base` (the brief's original fifth field) turned out **unused** by
     the R4 proof itself — retained in the structure for shape parity with the
     brief, but the theorem's exactness rests entirely on the no-shadowed-
     phantom fields above, not on `shared_base`.

3. **CANDIDATE REAL RUST GAP (open — user decision pending, this is the STOP
   point for Phase 1.5).** The `leader_stamped` countermodel above is not just
   a formal artifact — it is protocol-shaped (task-11-report.md, §"LOUD
   FINDING"): a crashed ex-leader that re-wins an election ships its own
   shadowed phantom pair `(t, D), (t+1, D)` in its term map. Nothing prunes it:
   `become_leader` pushes the new term entry unconditionally
   (`uc2_consensus/src/election.rs:1014`), leaders never reconcile their own map
   (reconciliation is a follower-side operation), and recovery's
   **`rederive_term_map`** (`uc2_node/src/node.rs:3020-3045`) is append-only —
   it does not detect or drop an equal-base shadowed entry either (the
   reviewer independently re-verified this during task-11's review, closing a
   hole the implementer's own writeup had left open). Consequence: followers
   durably past `D` hit a truncate/refill loop on every subsequent gossip from
   that leader — content-identical refill (the bytes don't actually change),
   so this is an **exactness/liveness hazard, not a divergence** as things
   stand; if the cut ever removed already-committed bytes it would trip the
   sim's `inv4`. Two disposition options identified, neither implemented here:
   (a) a Tier B (Phase 1.5+) unreachability proof that this shadowed-phantom
   state is never actually constructible end-to-end under the full protocol,
   or (b) a same-base prune at `become_leader` and/or in `rederive_term_map`
   directly. **This finding is why Phase 1.5 is user-gated rather than
   auto-started from this gate.**

   **Disposition (2026-07-16): FIXED via option (b), same-base prune at
   `become_leader`** (branch `uc2/phantom-prune`). Before pushing
   `(current_term, durable)`, `become_leader` now pops every trailing map
   entry whose base equals the node's durable — exactly the zero-byte
   phantoms a crashed prior life could have persisted at that position.
   Safety argument, each half verified against the code: (1)
   **termAt-invariance** — a pruned `(t', D)` is immediately shadowed by the
   pushed `(t, D)`; `term_at` (the content-identity oracle,
   `uc2_sim/src/invariants.rs`) returns the *last* entry with base ≤ pos, so
   the prune changes `term_at` at no position; (2) **C2 bounded-by-own** —
   commit is clamped to the leader's own durable (`CommitTracker::advance`'s
   `.min(own_durable)`; the Lean `advance_le_own`), so with our durable == D
   nothing at/above D was ever committed under t', and a follower holding
   *un*committed t'-bytes above D is truncated by its own-side clamp with or
   without the prune. Pinned by new tests in `uc2_consensus/src/election.rs`:
   `crash_rewin_prunes_same_base_phantom_at_become_leader` (the Finding #3
   crash-rewin sequence), `crash_rewin_collapses_multi_phantom_chain`,
   `become_leader_keeps_predecessor_when_durable_advanced` (normal-path
   regression: real bytes under the predecessor ⇒ no prune), and
   `pruned_leader_map_reconciles_clean_on_caught_up_follower` (the reconcile
   kernel: the pre-fix map truncates at D — the loop — while the pruned map
   reconciles clean at the follower's durable). A recovery-side prune in
   `rederive_term_map` was deliberately **not** added: a frontier entry with
   base == durable is legitimate follower state (reconcile's own docs call
   out the shared zero-byte frontier entry), and only the `become_leader`
   shadowing site ever *creates* the hazardous `(t, D), (t+1, D)` pair — a
   uniqueness that leans on the node's `awaiting_reconcile` intake gate
   (reconcile-before-data on new-term adoption), which blocks the would-be
   second creation path (a follower-side `DataTermObserved` pushing onto a
   still-phantom-bearing map); weakening that gate would re-open it (noted
   at the prune site in `election.rs`). Note for Finding #2's `DataStamped`
   contract: the prune now actively maintains the
   `own_stamped`/`leader_stamped` no-shadowed-phantom property at its one
   creation site, so the R4 hypothesis is enforced by construction rather
   than merely assumed. Known residual (adversarial review, Minor):
   `start_election`'s vote credentials still read the phantom-bearing map
   pre-prune, so a phantom can inflate `last_term` in `RequestVote` — safe
   by quorum intersection (the phantom term was legitimately won, so commit
   cannot have advanced past its base under any older term), and the prune
   only ever makes credentials *more* conservative; left for Tier B to
   formalize.

4. **Finding #5 — CONFIRMED REAL RUST BUG, SAFETY-CLASS (commit path),
   FIXED (2026-07-17).** Surfaced by Tier B(b)'s LB1 commit-certification
   layer (Finding #4, the data-plane one, lives in the Phase-2 spike memo):
   the reconciliation intake gate — THE load-bearing guard tying a term-T
   AppendPosition report to a tail reconciled against the T-leader — did
   not survive a reboot. `uc2_node` booted the gate OPEN (`node.rs`
   intake-gate init: `AtomicBool::new(true)`) with `awaiting_reconcile:
   false`, while `ElectionSm::new` recovers `current_term =
   vote_term.max(map_term)` — so a voter that GRANTED term T (vote
   persisted before send), held a divergent tail, and crashed before
   reconciling rebooted AT term T with the gate open; the receiver's 20 ms
   AppendPosition floor re-send (`receiver.rs` 1052–1078) beat the
   leader's 100 ms idle map re-ship, the same-term report fed the
   T-leader's `CommitTracker`, and the tracker certified a **phantom
   commit** over content the reporter did not hold (blast radius:
   committed-acked write loss after a leader crash; SMR apply divergence
   in a sub-interleaving). Machine-checked first as LB1's 27-step
   kernel-decided countermodel `finding_boot_gate_stale_report_lc_violation`
   (the FIXED LC-core statement was provably FALSE against
   Rust-as-shipped), then adversarially line-verified against every cited
   Rust site.

   **Disposition: FIXED (TDD, RED-first).** (a) A directed `uc2_sim`
   scenario, `rebooted_unreconciled_voter_must_not_certify_phantom_commit`
   (`uc2_sim/tests/scenarios.rs`), stages the exact trace — divergent
   term-1 ex-leader grants term T, crashes at the grant, reboots, its
   report floor races the map — and the **inv7 phantom oracle flagged it
   RED pre-fix** (`quorum legality (inv7): phantom commit — no genuine
   quorum`, seed 3: term-4 leader certified 2880 against a genuine
   quorum-frontier of 2784) and **GREEN post-fix**, validating both the
   oracle and the fix; the scenario stays as the permanent regression pin
   (it also asserts the closed-gate boot's liveness: one extra reconcile
   round, then genuine commits resume). Expressing the trace required
   mirroring the receiver's 20 ms report FLOOR into the sim's `Mechanism`
   data plane (`world.rs` archive step — reports were previously
   advance-triggered only, so a rebooted node could never report at all);
   the nasty-storm crash rate was re-tuned 1000→700 ppm to stay below the
   documented strict-inv2 benign-transient onset the more-realistic floor
   lowered (probed over 200 seeds at 500..=1000 ppm; the unguarded C-1
   arm still catches its phantom). (b) The Rust fix, both halves: the
   intake gate boots CLOSED iff `vote_term > map_term` (`node.rs` gate
   init) AND `awaiting_reconcile` boots to the same predicate (`node.rs`
   Consensus init) — reopen rides only the EXISTING clean-reconcile /
   truncate-ack / `BecomeLeader` arms. Completeness of the predicate:
   `map_term >= vote_term` implies the tail's last mapped term was
   validated under that term's leader (the map grows only via
   `DataTermObserved` / `become_leader`; a gossip-only adoption reboots at
   the old term and its stale reports are dropped). The sim's
   `on_restart` mirrors the same predicate so the sim keeps modeling
   `uc2_node` boot. (c) Model amendment (`Uc2Proofs/ProtocolCommit.lean`):
   `crashRestart` now sets `reconciled := decide (currentTerm ≤ lastTermOf
   termMap)`; the finding theorem became unprovable and was deleted in the
   same commit (`lake build` green, 3026 jobs, sorry gate clean, axioms
   unchanged). Gates re-run green: uc2_sim (23 scenarios incl. the pin +
   both storm arms), uc2_consensus both configs, uc2_node lib, workspace
   clippy, `lin_v2` release capstone.

5. **Finding #6b — CONFIRMED REAL v2.x DATA-LOSS BUG (Raft §5.4.2 /
   Figure 8 class, acked-write loss), FIXED (2026-07-17)** — and **Finding
   #6a — statement gap (stamp vs commit term), model re-keyed** in the same
   commit. Both surfaced by Tier B(b)'s LB2 adversarial invariant design
   (two kernel-`decide`d countermodels, independently review-confirmed with
   Rust line evidence; full record: `.superpowers/sdd/task-LB2-report.md`).

   **#6b, the bug.** UC records `new_term_pos` (the NewTerm no-op frame's
   end, `election.rs` `Event::NewTermAppended`) but applied the Raft §5.4.2
   barrier only to linearizable reads, ingress admission, and M7
   `propose_config` (via `serving`/`can_serve`) — NEVER to the commit
   advance: `rank_leader` pushed `Action::AdvanceCommit` unconditionally
   off the positions-only `CommitTracker`. At every failover inheriting an
   uncommitted tail, followers reconcile clean (their data-stamped maps not
   yet bumped — `DataTermObserved` rides the archive scan, i.e. durable
   bytes) and their gate-open 20 ms AppendPosition floor reports the
   election base BEFORE the NewTerm frame is quorum-durable, so the leader
   routinely committed an OLD-TERM-ONLY range — acks, apply, and
   leader-only outputs firing below the §5.4.2 barrier. Loss continuation
   (the race): a divergent higher-`lastTerm` rival wins the next term with
   a commit-quorum member's grant (lexicographic `logOk`; the granter's
   stamped `last_term` still old) and truncates the committed byte
   cluster-wide — the 46-step, n = 5 Lean countermodel
   `finding_fig8_old_term_commit_data_loss` drove it end to end (deleted
   with the fix).

   **Disposition: FIXED (TDD, RED-first), no new state.** (a) Directed
   `uc2_sim` scenario `old_term_range_must_not_commit_before_new_term_quorum`
   (5 nodes, `Mechanism` data plane, seed 3): term-1 leader + one caught-up
   follower grow an uncommitted tail; a rival wins term t2 on the other
   trio and is isolated holding only its divergent `(t2, base)` NewTerm
   frame; the original leader re-wins at T > t2 and the third voter's
   post-reconcile floor reports re-replicate + certify the old-term tail.
   **RED pre-fix** at the exact violating `AdvanceCommit` — captured
   verbatim: `term-map prefix consistency (inv2)… node 1 committed-position
   boundaries [(1, 0), (3, 864)] are not a leading slice of the committed
   lineage prefix [(1, 0)] (gmc=1007)` (the commit crossed the live rival's
   divergence base with the T-term NewTerm frame not quorum-durable).
   ORACLE NOTE (honest deviation from the review's inv4/inv5 prediction):
   inv4/inv5 are structurally unreachable behind inv2 in this sim — the
   post-event inv2 sweep runs after EVERY event, so the first commit past
   the rival's divergent boundary reds the run before any rival election
   (inv5) or truncation (inv4) event can occur; inv2 is the EARLIEST
   existing oracle for the class (no oracle was added or weakened), and the
   t4 loss continuation is pinned by the Lean countermodel instead.
   (b) The Rust fix (`uc2_consensus/src/election.rs::rank_leader`): the
   advance/store/gossip block is clamped to `ranked ≥ new_term_pos`;
   `new_term_pos == None` (between `become_leader` and `NewTermAppended`)
   means NO advance; a suppressed advance does not update `commit_seen`
   (the tracker's internal watermark advancing is harmless — the next
   covering rank re-fires). No new state — `new_term_pos` already existed;
   `serving` now latches unconditionally on any emitted advance (every
   emitted advance covers the NewTerm frame by construction). M7
   self-removal/demote step-downs are unaffected (a removal entry is a
   current-term serving-leader append, so its commit crossing always
   passes the clamp; both configs' suites green unchanged). (c) Scenario
   GREEN post-fix, with the clamp asserted DIRECTLY: the commit stays
   frozen through the entire re-replication window, the healed rival
   reconciles inside it (one truncation at exactly its divergence base),
   and the commit then advances past the whole inherited tail + NewTerm
   frame in one certification — W survives. A crate-level unit pin
   (`commit_clamped_to_new_term_base_never_certifies_old_term_only_range`)
   pins all three clamp behaviors (None-window, below-base suppression
   without eating the later emission, covering-advance + serving latch)
   and was RED-verified against a temporary revert of the clamp.
   (d) Liveness: one NewTerm replication round per election before
   inherited-tail commit — the same round the read path already paid via
   `serving`; verified by the scenario's resumed-commit assert and the
   full suites (incl. both release lincheck capstones).
   (e) Sim mirror: none needed — `uc2_sim` drives the REAL
   `ElectionSm::rank_leader` (`world.rs` wires `uc2_consensus` directly),
   so the clamp is automatically reflected; the only sim-side artifact is
   the scenario itself.

   **#6a, the statement gap (independent; survives the #6b fix).** The
   ghost ledger recorded `(position, stamp, payload)` while Raft's Leader
   Completeness (§5.4.3) keys on the COMMIT term: a stamp-`t` entry can be
   committed at `T > t` (a re-elected leader certifying its inherited
   prefix), and an honest intermediate-term leader `u ∈ (t, T)` owes
   nothing about it (`finding_stamp_keyed_lc_stale_leader` +
   `lc_core_stamp_keyed_is_false`, 23-step trace — deleted with the
   re-key). No Rust change: UC never exposes a stamp-keyed commitment.
   **Model re-key**: `committed` now carries `(p, stamp, T, v)` with `T` =
   the committing leader's `currentTerm` at the advance;
   `leaderAdvanceCommit` records it and gains the #6b enabling (`hbase`:
   the advance must cross the term map's last entry's base — the
   `prunePush`-maintained `(currentTerm, base)` IS the model's
   `new_term_pos` analog, `ranked ≥ new_term_pos > base ⟺ k > base`).
   The LC-core statement is now commit-term-keyed —
   `(p, t, T, v) ∈ committed → leader i → T ≤ currentTerm i → hist i p =
   some (t, v)` — recorded in `Uc2Proofs/LeaderCompleteness.lean` as LB2's
   target; the non-vacuity trace was adapted (clamped commit
   `(0, 1, 1, 42)` + later-term winner) and the proofs gate is green
   (`lake build` 3027 jobs, sorry gate clean, axioms unchanged:
   non-vacuity `[propext, Quot.sound]`, lifted safety theorems
   `[propext, Classical.choice, Quot.sound]`).

   **Storm-pin re-tune rider**: the §5.4.2 clamp is genuine
   defense-in-depth for the M4 C-1 class too (an unguarded-reopen escaped
   divergent report can no longer certify anything below the new leader's
   NewTerm frame), which pushed the `mechanism_unguarded_reopen` storm
   catch out of its documented tuning window (700 ppm: no catch over
   200+ seeds and every probed knob; the genuine inv7 phantom first
   reappears at crash 1000 ppm, seed 21 — proving the reopen guard is
   still load-bearing for post-NewTerm-commit escapes). Disposition per
   the config's "rates, not oracles" rule: the twins now run asymmetric
   crash rates (red arm 1000 ppm, green arm at the shared 700 ppm — its
   ceiling is the documented benign both-arms strict-inv2 transient at
   800 ppm), and the red arm's catch predicate is SHARPENED to the inv7
   phantom class only, so the benign transient can never satisfy the pin.
   Full probe data in the scenario-file config comment.

6. **Finding #7 — MODEL-FIDELITY gap, NOT a Rust bug (open, discharge
   scheduled).** Surfaced by Tier B(b)'s LB2 re-run, after both #6a/#6b were
   fixed (`.superpowers/sdd/task-LB2-rerun-report.md`): the LA1 `≤`-guard
   over-approximation on `deliverReplicate` (`hstamp : t ≤ currentTerm`,
   already flagged in its own docstring as the model's conservative
   consequence of two Rust guards — see the Phase 2 spike memo's Tier B(a)
   §3 item 4) is sound for `Uc2.Data.log_matching` but unsound for leader
   completeness: it lets a follower accept a dead leader's in-flight stale
   frame interleaved with the live leader's stream — a "Frankenstein log"
   real UC structurally forbids — and then honestly report a durable
   frontier covering the leader's commit range with divergent content
   underneath. Machine-checked as a 33-step kernel-decided countermodel
   (`finding_stale_replicate_replay_lc_violation` /
   `lc_core_commit_term_keyed_is_false`, kept in
   `Uc2Proofs/LeaderCompleteness.lean` — they refute the unconditional
   `leader_completeness` statement, which stays refuted regardless of any
   conditional route). Rust evidence: `uc2_net/src/receiver.rs:636-639`
   (`if h.leadership_term_id != term { dropped_stale_term; return; }`) —
   exact header-match, not `≤`; real replication re-serves old-stamped
   bytes during catch-up/NAK-repair strictly inside the CURRENT leader's
   stream, a distinction the model's record-stamp-only `Frame.replicate`
   cannot express. **No Rust change indicated; no sim change indicated** —
   this is a model-fidelity gap, not a protocol defect.

   **Disposition: OPEN, discharge scheduled as "Option 1."** User directive
   was a hybrid: land a conditional `leader_completeness` now (carrying a
   new hypothesis, `FramesCurrentAuthored`, that assumes away exactly this
   gap — designed, hand-verified sufficient/faithful/non-circular, and
   machine-checked non-vacuous in `task-LB2b-report.md`, though the
   `leader_completeness` theorem itself remains open for unrelated reasons)
   while scheduling the real fix as a follow-up: split `Frame.replicate`'s
   record stamp from a header provenance term (mirroring
   `receiver.rs:636-639` exactly) plus a new `serveTail` leader-re-serve
   step, which discharges `FramesCurrentAuthored` by construction rather
   than by hypothesis — at the cost of `Uc2.Data.log_matching`
   (`LogMatching.lean`, 1046 lines) needing to re-green under the extended
   model per the LA1 layering rules. Not started as of this gate. Full
   record, cost accounting, and the options for what to do next:
   `docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md`'s **"Tier B(b)
   actuals + re-gate"** section.

### Finding #12 — model-fidelity: `PNode.durable` collapses TWO independently-read node values (found 2026-07-30 from the Rust side; **real Rust bug, fixed**)

**Not found by the proofs — found while investigating issue #6 and recorded here
because it invalidates a load-bearing lemma.** In the Rust node the shared
`durable` byte-position counter has two independent readers on two threads:

- the **receiver** agent reads it directly and reports it to the leader, which
  ranks those reports into `commit`
  (`uc2_net/src/receiver.rs`, the `DGRAM_KIND_APPEND_POSITION` send);
- the **consensus** agent polls it a duty cycle later into `ElectionSm::durable`
  (`node.rs`, `Consensus::do_work`), which is what `log_ok` compares, what
  `start_election` advertises, and what `become_leader` uses as `base`.

So a voter could grant while comparing against a self-view LOWER than the value
it had already reported for commit purposes — letting a candidate behind a
committed position win and collapse below it (acked-write loss). Fixed in Rust
by re-absorbing the counter immediately before the grant decision, and at the top
of the duty cycle so candidates advertise symmetrically; regression test
`a_vote_is_refused_against_a_fresh_read_of_our_own_log` (red-verified).

**Why the model could not have caught it, and why that matters here.**
`PNode.durable` (`Protocol.lean`) is ONE `Nat` serving all four roles; neither
`ProtocolData` nor `ProtocolCommit` adds a second. The derived lemma
`ReportEraFloor` (`StageB.lean`) — "a reported `d` is `≤` the reporter's
durable" — has its `sendReport` case discharged by **`Nat.le_refl`**, because in
the model `d` *is* `pn.durable`. `GrantReport` then composes that floor with
`log_ok` (the `omega` in `StageB.lean`) to conclude `d ≤ cd`. **That composition
is exactly the informal safety argument this bug refutes**, and it closes only
because the two durables are literally the same term. Any future
`leader_completeness` completed over the current model would be completed over a
model that assumes this bug away.

Secondary: under decision 4 ("fsync lag collapsed", `ProtocolData.lean`)
`durable` IS the append frontier, so `base = durable` always and there are never
bytes above `durable` to discard — the model has no collapse-discards-bytes step
either.

**This is the same shape as Finding #8**, which split `dataTerm` out of
`currentTerm` after collapsing the node-level term handle hid a real hole. The
durable counter has not been given the same treatment. Splitting it invalidates
`ReportEraFloor`'s reflexivity proof and everything composed from it, so it is
scoped as its own piece of work, not a patch. Note `bare_report_durable_stability_is_false`
(`LcClosure.lean`) is NOT this: it breaks the same shape of statement via
gossip-driven reconcile truncation, which `Era`-conditioning repairs — the Rust
hazard needs no truncation, no gossip and no term change on the voter, so
`Era`-conditioning does not exclude it.

**`uc2_sim` is blind to it too**, and for a mirror-image reason: `world.rs`
advances the node's durable and feeds `Event::DurableAdvanced` as consecutive
statements in one `ArchiveStep` handler, with the commit report derived from the
same value — and `SimEvent` has no consensus-agent step at all. The invariants
(`inv4` committed-never-truncated, `inv5` leader completeness) WOULD catch the
resulting loss; the world model cannot generate the trace. Making it expressible
needs a `SimEvent::ConsensusStep` carrying `DurableAdvanced`, or a separate
`Node.reported_durable` with a deferred feed.

### Restatements (recorded for completeness, not spec gaps)

- `commit_certified_run` (C3, run form) was **strengthened**, not weakened: an
  extra at-step certification conjunct was added so "quorum-many members sat
  at or above the final commit value at the step it was set" is part of the
  theorem itself rather than left to composition (task-8-report.md).
- `reports_only_from_onDurable` (C4, provenance) was **fixed** from the
  brief's malformed disjunction (`… ≤ d ∨ ∃ d, report i d ∈ evs`, whose second
  disjunct made the strong half trivially escapable) to the properly strong
  conjunctive form; the brief's `hi : i < t.reported.length` hypothesis was
  also dropped as unnecessary (task-8-report.md).
- V1's brief-literal dot-notation signature
  (`(handleRequestVote s c1 t1 d1).1.handleRequestVote c2 t2 d2`) does not
  elaborate in this codebase (`handleRequestVote` lives in namespace `Uc2`,
  not `Uc2.VoterState`) — rewritten as the definitionally identical plain
  application. Syntactic only, no change in meaning (task-12-report.md).

## Phase 1.5 status

**Attempted 2026-07-16 (user go-ahead), EXITED at a toolchain-version wall —
fallback to conformance-only linkage per the spec §6 exit clause.** Outcome
of the time-boxed attempt (full record: the T15 task report):

- **The hard half is proven feasible.** Charon processed `uc2_consensus`
  cleanly (`--start-from crate::reconcile::reconcile`; the 3.2k-line
  `election.rs` was no obstacle) and produced LLBC for `reconcile` — the
  translation path the spec worried about (iterator chains, crate size)
  works, validating the T2 verifiability refactors.
- **The block is version timing, not capability.** Aeneas HEAD's Lean
  support library (`AeneasMeta`, a hard dependency of `import Aeneas`) pins
  Lean v4.31.0 and fails to build under this repo's pinned v4.32.0 with two
  independent Lean-core API breaks (a missing
  `BVDecide.Frontend.Normalize.Enums` module and a `Simp` tactic-framework
  type mismatch) — confirmed by an actual `lake build`, not just
  toolchain-file diffing. No v4.32.x-compatible aeneas tag existed at
  attempt time. Downgrading the repo's Lean pin to chase a research tool was
  rejected (it would put the 14 proved theorems at risk to serve their
  linkage upgrade).
- **Retry condition:** aeneas bumps `backends/lean/lean-toolchain` to
  ≥ v4.32.0 (watch https://github.com/AeneasVerif/aeneas). The full
  toolchain (opam switch, charon binary, aeneas checkout) is left installed
  under `/home/claude/{aeneas,charon,local-deps}` for a cheap resume; the
  T15 report records exact tool commits, commands, and a solved
  `require`-ordering fix for the mathlib version diamond the retry will hit.
- **Standing linkage** until then: the conformance rig (170k vectors, three
  seeds, zero divergence) + the build-time `#guard` pins. No
  `proofs/Aeneas/` artifacts were vendored (tree kept clean on exit).

## Phase 2 spike

**Complete 2026-07-17** (branch `uc2/lean-phase2-spike`): N-node election
protocol model (`Uc2Proofs/Protocol.lean`, 8-constructor `Step`, sent-set
network + havoc data plane + crash-restart, named non-vacuity leader trace)
and **election safety proved** sorry-free over it
(`Uc2Proofs/ElectionSafety.lean`, 5-clause invariant, axioms
`[propext, Classical.choice, Quot.sound]`). Full result, measured proof-cost
data, and the go/no-go pricing of the Tier B remainder (log-matching analog,
leader completeness, state-machine safety) are in the dedicated memo:
**`docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md`**. Recommendation:
**GO (phased)** — log-matching analog first as a time-boxed sub-spike,
re-gate before leader completeness / state-machine safety.

**Tier B(a) log-matching sub-spike complete 2026-07-17** (branch
`uc2/lean-log-matching`, merged): the real data plane (payload history +
data-stamped term map) is layered over the election model
(`Uc2Proofs/ProtocolData.lean`, 11-constructor `Step`, `election_safety`
carried forward by projection lift + one sanctioned `Protocol.lean`
extension, `adoptHigherTerm`, re-proved green) and **`Uc2.Data.log_matching`
is proved** sorry-free over it (`Uc2Proofs/LogMatching.lean`, 5-clause
`DInv` + a cross-time writer `Cert`, axioms
`[propext, Classical.choice, Quot.sound]`). **Election safety and
log-matching are both proved, at every model level now in the tree.** The
stretch prefix-form is dropped with a countermodel (false in the model,
in the lineage/map reading only — see the memo). Measured cost (~6.2
S2-equivalents) and the re-priced (b)/(c) estimates, plus a GO recommendation
for (b) leader completeness, are in the memo's **"Tier B(a) actuals +
re-gate"** section (same file as above).

**Tier B(b) leader-completeness sub-spike complete 2026-07-18** (branch
`uc2/lean-leader-completeness`): **election safety + log-matching proved;
leader completeness conditional-partial (`FramesCurrentAuthored`), 2 shipped
bugs found+fixed en route.** Spelled out: `Uc2.Data.log_matching` and both
levels of `election_safety` stay green, lifted into a new commit-plane layer
(`Uc2Proofs/ProtocolCommit.lean`, kernel `CommitTracker` consumed, ghost
committed ledger); `leader_completeness` itself is NOT proved — a
conditional hypothesis (`FramesCurrentAuthored`) plus two supporting
unconditional lemmas plus a required non-vacuity trace are landed and
machine-checked in `Uc2Proofs/LeaderCompleteness.lean`, but the theorem
closing the induction is open (Finding #7 above names exactly the gap the
hypothesis carries away; discharging it is the scheduled "Option 1"
follow-up). Along the way, the sub-spike's adversarial invariant design
found and FIXED two real, shipped safety bugs — Finding #5 (a boot-time
phantom-commit hazard on the commit path) and Finding #6b (a Raft
§5.4.2/Figure-8-class acked-write-loss bug) — plus re-keyed one statement
gap (Finding #6a). All four findings and their dispositions are in this
doc's Findings section (items 4-6 above); the full actuals (measured
cost — this sub-spike ran ~6-12× over its own 3–6 S2-equivalent estimate,
the opposite of Tier B(a)'s in-range result), the hybrid-plan follow-ups, and
the honest re-price of (c) state-machine safety (now gated on finishing
(b)) are in the memo's **"Tier B(b) actuals + re-gate"** section (same file
as above). Next formal-methods decision: presented, not resolved, in that
section's recommendation — finish `leader_completeness` (Option 2 more
sessions, on sonnet or fable), do the Option 1 model refinement first, or
pause further proving on leader completeness given the two shipped-bug fixes
already banked.

---

Phase 1 is complete as of this doc. Post-gate dispositions (2026-07-16, all
user-directed): branch merged to main; Finding #3 fixed via the same-base
prune (see its Disposition paragraph); Phase 1.5 attempted and exited at the
aeneas/Lean-4.32 version wall (see Phase 1.5 status above). The Phase 2
election-safety spike (spec §7) is complete — see the Phase 2 spike section
above and its memo. Its Tier B(a) log-matching sub-spike is also complete
(see the Phase 2 spike section) — `log_matching` is proved. Its Tier B(b)
leader-completeness sub-spike is also complete, but did not finish: election
safety and log-matching stay proved, `leader_completeness` itself lands only
conditionally (open theorem, `FramesCurrentAuthored` hypothesis), and the
sub-spike found and fixed two real, shipped safety bugs (Findings #5, #6b)
plus one statement re-key (#6a) along the way (Finding #7, model-fidelity,
is the reason the conditional route exists at all) — see the Phase 2 spike
section above and the memo's "Tier B(b) actuals + re-gate" section for the
full record, cost accounting, and the presented (not resolved) options for
what to do next. Other options when desired: a Phase 1.5 retry once aeneas
supports Lean ≥ 4.32.

---

## Tier B(b) closure arc (LC1–LC4h) — banked 2026-07-19

The Option-1 follow-up (retire Finding #7's model debt, prove
`leader_completeness` UNCONDITIONALLY) ran and is **banked, not finished**.
Net change to the proof corpus's standing:

- `frames_current_authored` is now **discharged unconditionally** (was the
  hypothesis that made the prior arc's `leader_completeness` conditional).
  `election_safety` and `log_matching` stay proved under the refined model.
- `leader_completeness` is **reduced to one named obligation** — the `canon`
  invariant — with its assembly, crux, canon statement/consumer-interface,
  antitonicity, and a machine-checked `k>0` satisfiability witness all landed.
  Finding #11: canon needs joint/well-founded induction (the corpus's standard
  single-`ReflTransGen`-shell shape provably cannot reach its monotone-forward
  antecedent's newly-born instances); F-A confirms this is scope, not falsity.
  Remaining ≈7–12 S2-eq / 3–4 tasks to a complete unconditional theorem.
- **Two real shipped consensus bugs found and fixed**: Finding #9 (intake-gate
  reopen keyed to `currentTerm` not the term handle — acked-write-loss, fixed in
  Rust `node.rs:2423` + model mirror + directed sim regression pin, releases.md
  filed) and Finding #8 (the model-fidelity gap that exposed #9). Combined with
  the prior sub-spike (#5, #6b), this proof effort has now driven **four real
  shipped-bug fixes**, all in the election-time term-handle/gate/commit window.

Full actuals, findings ledger (#8–#11), the joint-induction blueprint, the
resume notes, and the (c) re-price are in the memo's **"Tier B(b) CLOSURE ARC"**
section (`uc2-lean-phase2-spike-2026-07-17.md`). Resume is clean: nothing decays;
the branch is merged and the canon blueprint + kernel-cost traps are on record.
