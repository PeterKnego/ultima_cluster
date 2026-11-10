# Machine-checked proofs in Lean 4

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

This tier holds the project's mathematical proofs. They are written in Lean 4, a proof assistant whose kernel checks every step of every proof. A Lean proof is the strongest evidence in this repo, but it is a proof about a model of the code, not about the Rust itself. The proofs live in [`proofs/`](/proofs). The record of how they were built and gated is in [`docs/benchmarks/uc2-lean-gate-2026-07-16.md`](/docs/benchmarks/uc2-lean-gate-2026-07-16.md) and [`docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md`](/docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md).

## Toolchain and build

The proofs use Lean 4 v4.32.0. The mathlib library is pinned at v4.32.0.

In Lean, `sorry` is a placeholder that tells the checker "assume this step is true". A proof that contains `sorry` anywhere is not a finished proof. `lake build` completes about 3,027 jobs with zero warnings and zero `sorry`. Zero `sorry` means every step was actually checked.

## What is proved

The proofs come in two layers. The kernel layer covers the small pure functions that make safety decisions. The protocol layer covers a whole cluster of nodes exchanging messages.

### Kernel layer

The kernel layer is Phase 1 of the work. It holds 14 theorems over `Uc2Model`. `Uc2Model` is a mirror of `uc_consensus`'s three pure-sync safety kernels, written without mathlib.

**Commit tracker.**

- Commit is monotone.
- Commit never certifies past the caller's own durable position.
- Every commit value was backed by quorum-many reports at or above it. This holds *at the step it was set*.
- A reset genuinely clears certifying power.

**Reconcile.**

- The output never exceeds the caller's durable frontier.
- Agreed bytes stay valid.
- Divergence is always cut.
- `validUpTo` is *exactly* the first byte-content divergence. It is not merely a sound bound.
- Reconcile is idempotent.

**Vote freshness.**

- A voter cannot grant two different candidates in the same term.
- A granted vote implies the candidate's log passed the freshness check. The proof gives an exact lexicographic characterization of that check.

**Quorum intersection.**

- Any two majorities of a fixed cluster share a member. This is proved as a Lean `Finset` fact. It is not taken as an assumption.

### Protocol layer

The protocol layer is Phase 2 and Tier B of the work. It models N nodes. Messages travel over a sent-set network, which means any message once sent may be delivered at any later time. The data plane is havoc, which means its contents can change arbitrarily within the model's rules. Nodes can crash and restart.

Two theorems are proved sorry-free at this layer:

- `election_safety`, at both model levels.
- `Uc2.Data.log_matching`, over the layered data plane. The layered data plane is the payload history plus the data-stamped term map.

## What is not proved: `leader_completeness`

**`leader_completeness` is open.** This is the theorem that a newly elected leader already holds every committed entry.

As of the closure arc banked 2026-07-19, it is reduced to a single named obligation. That obligation is the `canon` invariant. Everything around the `canon` invariant has landed: its assembly, its crux, its consumer interface, its antitonicity, and a machine-checked satisfiability witness.

A hypothesis used to make the theorem conditional. It was called `frames_current_authored`. It has since been **discharged unconditionally**.

Finding #11 in the phase 2 spike record explains why the remaining step is hard. It needs joint or well-founded induction rather than the standard induction shape the rest of the proof corpus uses. Finding #11 also confirms that this is a matter of scope, not of the theorem being false. The estimate is 3 to 4 further tasks.

## Finding #12: the durable-counter caveat

**This caveat matters more than the open `leader_completeness` theorem itself.**

### Two readers in the code, one number in the model

Each node has a durable counter. It records how far the node's log is safely on disk. In the real node, that counter has **two independent readers on two threads**:

- The receiver reads it and reports it to the leader for commit ranking.
- The consensus agent absorbs it a duty cycle later and uses it for vote credentials.

In the model, `PNode.durable` is a single `Nat`. The model therefore treats both readers as seeing the same value at the same moment.

### Why the collapse matters

In the real system, the two readers can see different values. The receiver may have reported a durable position to the leader before the consensus agent has absorbed it. For a short window, the node's commit reports and its vote credentials disagree about how much log it holds. The model cannot express that window at all, because it has only one number.

Because the model collapses the two readers, the derived lemma `ReportEraFloor` discharges its `sendReport` case by `Nat.le_refl`. That step says "a number is at most itself". It is trivially true in the model. It is false in the real system, where the two readers hold two different numbers.

Composing the two readers into one number this way is exactly the informal safety argument that Finding #12 refuted. Finding #12 is recorded in the Lean gate record. It found the argument was wrong by finding a shipped acked-write-loss bug. In that bug, a write the cluster had acknowledged to a client could be lost.

### What follows

**`leader_completeness`, if completed over the current model, would be completed over a model that assumes the acked-write-loss bug away.**

Splitting the counter into two in the model invalidates `ReportEraFloor`'s reflexivity proof. It also invalidates everything composed from that proof. For that reason, the split is scoped as its own piece of work rather than as a patch.

The fix has three parts, and their current state is:

- The Rust is fixed.
- The `uc_sim` half is closed. See [the simulation page](simulation.md).
- The Lean half is open.

State-machine safety is not attempted. It is gated on finishing the work above.

## The trusted base

The trusted base is the set of things you must believe for the proofs to mean anything. Lean's `#print axioms` command lists every axiom a theorem ultimately depends on. Every theorem here passes `#print axioms` with only the standard Lean and mathlib trio: `propext`, `Classical.choice`, and `Quot.sound`.

Specifically:

- There is no `sorry`, anywhere in the chain.
- There is no `native_decide`. That tactic proves a fact by compiling and running code, so using it would place the Lean compiler in the trusted base.
- There are no project-local axiom escape hatches.
- **There is no SMT.** Nothing in `proofs/` calls a solver. See [the Veil page](veil.md).

## The model-versus-code gap

The honest objection to any protocol proof is this: *your theorems are about a model, and you ship Rust.* Two mechanisms address it. One is a conformance rig that replays the real Rust against the model. The other is a set of build-time pins for hand-written edge cases.

### The conformance rig

`uc_consensus/examples/conform_gen.rs` drives the *real* Rust functions under a seeded PRNG. The functions it drives are:

- `CommitTracker::advance`, `on_durable` and `reset_reports`
- `reconcile::reconcile`
- `election::log_ok_order`

It emits JSONL vectors. Each vector's expected value is the implementation's own output. `proofs/Conform/Main.lean` then re-derives each outcome from `Uc2Model` and diffs it bit for bit. A difference means the Rust and the model disagree.

### Rig results

- Implementer run: **100,000 vectors, zero divergence.**
- Independent reviewer re-run, with a different seed: **20,000 vectors, zero divergence.**
- Nightly CI re-runs the rig with a date-rotated seed.

The vector distribution was confirmed diverse rather than degenerate:

| shape | share or count |
|---|---|
| `NoCommonPrefix` outcomes | about 8% |
| vectors exercising a real truncation | about 4,000 |
| full-tie `log_ok` calls | about 2,400 |
| tracker resets | about 20% |

### The window-aligned reconcile branch

The 2026-08-16 acked-write-loss fix changed `reconcile`. It now aligns the leader's 64-entry wire window inside the follower's full map. The alignment condition is `leader[0] == own[j]`, with `j > 0`. The window-aligned branch is the code path that handles `j > 0`.

**The generator has produced this branch by construction since 2026-08-29.**

Until 2026-08-29, the generator only ever built `own` as a prefix-extension of `leader`. So `j > 0` occurred in **zero** of its vectors. The measured count was 0 of 33,334 reconcile vectors under seed 1. The branch the fix added was replayed against `reconcileAligned` exactly never.

Now one reconcile vector in three comes from a windowed arm. That arm builds a shared history of 65 to 96 entries. Per 100k run, it yields:

| case | count per 100k run |
|---|---|
| `j > 0` vectors | about 7,400 |
| of which clean | about 800 |
| of which cut at a divergence | about 6,600 |
| `leader[0] ∉ own` → `NoCommonPrefix` | about 2,200 |
| cut at window start | about 2,200 |

About 2,350 of the vectors in this table also fire the same-term/different-base clamp.

This was verified on three seeds and 300k vectors, with zero divergence. A corrupted `j > 0` expectation is reported as a divergence. That shows the checker has teeth on the new shapes: it catches a wrong answer, not just a right one.

**The theorems lag the rig here.** The `Uc2Proofs` theorems R1 to R6 are still stated over the pre-alignment core. See the note in `Uc2Model/Reconcile.lean`. The rig now checks the shipped wrapper. The theorems do not yet.

### Build-time `#guard` pins

Random sampling gives no guarantee of hitting a hand-written edge case. So the unit tests are also ported into Lean. A `#guard` is a Lean assertion that runs during the build and fails the build if it evaluates to false.

Every `reconcile.rs` unit test, all 10 of them, is ported to an executable `#guard` assertion. So is every `commit.rs` unit test. Each `#guard` sits next to the corresponding model definition. They are checked on every `lake build`.

## The residual gap, stated plainly

The rig covers the three consensus kernels. It does not cover the whole node.

The durable-counter collapse above is a concrete instance of what that leaves uncovered. It was a hazard the model could not express at all. So no amount of vector replay against the model would have surfaced the bug.

### Extraction via Aeneas: blocked by a toolchain wall

One way to close the model-versus-code gap is to translate the real Rust into Lean mechanically and prove theorems about the translation. [Aeneas](https://github.com/AeneasVerif/aeneas) does this. The approach was used successfully in [`ultima_db`](https://github.com/PeterKnego/ultima_db), where proofs run against mechanically translated code.

Extraction via Aeneas was attempted here and **exited at a toolchain wall**. Aeneas's Lean support library pins v4.31.0. It does not build under this repo's v4.32.0.

Charon, the Aeneas front end, processed `uc_consensus` cleanly. So the approach is feasible. Only the version timing blocks it.

Downgrading the repo's Lean pin to chase a research tool was rejected. The retry condition is that Aeneas bumps its toolchain to v4.32.0 or later.
