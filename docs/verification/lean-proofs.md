# Machine-checked proofs in Lean 4

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

## The short version

UC's consensus code has three small decision functions at its heart. One decides when a position in the log is committed. One decides where a follower's log stops agreeing with the leader's and must be cut. One decides whether a node may vote for a candidate. These three are written in Lean 4, a proof assistant whose kernel checks every step of a proof by machine, and fourteen theorems about them are proved. Above them, a model of a whole cluster of nodes proves two of the three classic consensus safety properties: election safety and log matching.

That is the strongest evidence in this repo. It is also evidence about a model, not about the Rust that ships. So this page spends as much time on the gap between the two as on the theorems. There is one open theorem, leader completeness. And there is one caveat that matters more than the open theorem: the model holds a number in one place where the real node reads it from two threads, and a real bug once lived in exactly that gap.

The proofs live in [`proofs/`](/proofs). The record of how they were built and gated is [`docs/benchmarks/uc2-lean-gate-2026-07-16.md`](/docs/benchmarks/uc2-lean-gate-2026-07-16.md), with the second phase in [`docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md`](/docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md).

## Why this work was worth doing

The proof effort found and fixed four real safety bugs that had shipped. All four were in code that had already passed the simulation, the linearizability capstones and the crash-recovery tier. Two of them could lose a write the cluster had already acknowledged to a client. The findings are numbered #5, #6b, #8 and #9 in the gate record, and the [overview](../VERIFICATION.md) tells the story of the two worst ones. Each was found the same way: someone tried to prove a theorem and discovered it was false against the code as written.

## What a Lean proof is, and what it is not

Lean checks that every step of a proof follows from the step before it. A finished Lean proof cannot be wrong about the thing it describes. But the thing it describes here is a **model**: a rewrite of the Rust logic in Lean's own language. If the model and the Rust disagree, the proof is about the model only. The section on [the model-versus-code gap](#the-model-versus-code-gap) explains how UC keeps the two aligned.

Three Lean terms come up on this page:

- **`sorry`** is a placeholder that tells the checker "assume this step is true". A proof containing a `sorry` anywhere is unfinished. Every proof here is `sorry`-free.
- **`#guard`** is an assertion evaluated during the build. If it is false, the build fails.
- **`#print axioms`** lists every axiom a theorem ultimately rests on. It is how you see what you are being asked to take on faith.

## What is proved

### The three kernels

The Rust crate `uc_consensus` holds three pure functions that make the safety decisions. They are pure in the sense that they take values in and return values out, with no I/O, no clock and no threads, which is what makes them provable. The Lean model of them is called `Uc2Model`. It is written without the mathlib library, so it depends on nothing but Lean itself. This is Phase 1 of the work, and it holds 14 theorems.

**The commit tracker** decides when a log position is committed. Each follower reports how far its log is safely on disk, its durable position. The tracker ranks those reports and commits a position once a quorum of nodes has it durable. The theorems say:

- Commit only ever moves forward.
- Commit never passes the leader's own durable position.
- Every commit value really was backed by reports from a quorum at or above it, at the moment it was set.
- Resetting the tracker genuinely clears its power to certify. It cannot commit on stale reports after a reset.

**Reconcile** runs when a follower compares its log against the leader's. It finds the first byte where the two disagree and cuts everything from there. The theorems say:

- Reconcile never claims agreement past the follower's own durable frontier.
- Bytes it keeps really do agree with the leader.
- Where the logs diverge, the cut is always made.
- The agreed length it reports, `validUpTo`, is *exactly* the first point of disagreement, not just a safe underestimate.
- Running reconcile twice gives the same answer as running it once.

**Vote freshness** decides whether a node grants its vote to a candidate. The rule is lexicographic: a candidate's last term and last durable position, taken as a pair, must be at least the voter's own pair. The theorems say:

- A node cannot grant two different candidates in the same term.
- If a vote was granted, the candidate's log passed that freshness check. The proof pins down the exact comparison, not an approximation of it.

**Quorum intersection** is the fact that lets any of this work: any two majorities of a fixed cluster share at least one member. It is proved as a fact about Lean's `Finset` type, finite sets, rather than assumed.

### The whole-cluster model

Phase 2 and Tier B of the work model N nodes exchanging messages. The network is a sent-set, which means any message once sent may arrive at any later time, or never. The data plane is "havoc", which means the model lets log contents change in any way its rules allow, so a proof cannot lean on data being well-behaved. Nodes can crash and restart.

Two of the three classic consensus safety properties are proved here, both `sorry`-free:

- **Election safety**, `election_safety`: at most one leader per term. Proved at both model levels.
- **Log matching**, `Uc2.Data.log_matching`: if two logs agree at a position, they agree on everything before it. Proved over the layered data plane, which is the payload history together with the term map stamped onto the data.

## What is not proved

### Leader completeness is open

The third classic property, **leader completeness**, says a newly elected leader already holds every committed entry. It is not proved.

It is close. As of the closure arc banked on 2026-07-19, the whole theorem is reduced to one remaining obligation, an invariant called `canon`. Everything built around that invariant has landed: how it is assembled, the key lemma at its crux, the interface other proofs use to consume it, the proof that it only gets stronger over time, and a machine-checked witness that it can be satisfied at all. One hypothesis that used to make the theorem conditional, `frames_current_authored`, has since been discharged with no conditions attached.

Finding #11 in the phase 2 spike record explains why the last step is hard. It needs a joint or well-founded induction, a different shape from the standard induction the rest of the proof corpus uses. The same finding confirms this is a matter of scope, not a sign the theorem is false. The estimate is 3 to 4 further tasks.

### The caveat that matters more: one number where the node has two

This is Finding #12, and it is the most important thing on this page.

Each node tracks its durable position, how far its log is safely on disk. In the real node, that one counter is read by **two different threads** for two different purposes:

- The receiver thread reads it and reports it to the leader, which uses the report to rank commits.
- The consensus thread reads it one duty cycle later and uses it as the node's credential when voting.

Those two reads do not happen at the same instant. For a short window, the node's commit report and its vote credential can disagree about how much log it holds.

In the Lean model, `PNode.durable` is a single number. The model has no way to say "the receiver saw one value and the consensus agent saw another". So one of the model's lemmas, `ReportEraFloor`, proves its `sendReport` case with `Nat.le_refl`, the fact that a number is less than or equal to itself. In the model that is trivially true. In the real system it is false, because the two readers hold two different numbers.

That is not an abstract worry. Collapsing the two readers into one is exactly the informal safety argument the team once believed, and Finding #12 refuted it by finding a shipped bug that could lose an acknowledged write. The Rust was fixed, and the simulator, `uc_sim`, was taught to model the two readers separately. The [simulation page](simulation.md) covers that half.

The consequence for the proofs: **if `leader_completeness` were finished over today's model, it would be finished over a model that assumes that bug away.** Splitting the counter in the model breaks the `ReportEraFloor` proof and everything built on it. That is why the split is scoped as its own piece of work rather than a patch, and why it is still open.

State-machine safety, the property that replicas end up in the same state, is not attempted. It waits on the two items above.

## The model-versus-code gap

The honest objection to any protocol proof is: *your theorems are about a model, and you ship Rust.* Two mechanisms keep the model honest.

### The conformance rig: replay the real Rust against the model

`uc_consensus/examples/conform_gen.rs` runs the *real* Rust functions on inputs drawn from a seeded random generator. The functions are `CommitTracker::advance`, `on_durable` and `reset_reports`, the function `reconcile::reconcile`, and the vote rule `election::log_ok_order`. For each input it records the Rust output as a test vector, one JSON object per line. `proofs/Conform/Main.lean` then feeds each vector to the Lean model and compares the two answers bit for bit. Any difference means the model and the code have drifted.

The results so far:

- The implementer's run: **100,000 vectors, zero divergence.**
- An independent reviewer's re-run with a different seed: **20,000 vectors, zero divergence.**
- Nightly CI repeats the run with a seed that changes with the date.

A rig that only ever generates easy inputs proves little, so the distribution was checked and found varied rather than degenerate:

| shape | share or count per run |
|---|---|
| `NoCommonPrefix` outcomes, where reconcile finds no agreement at all | about 8% |
| vectors that exercise a real truncation | about 4,000 |
| vote comparisons that are a full tie | about 2,400 |
| tracker resets | about 20% |

### A branch the rig never reached, and how that was found

A rig is only as good as the inputs it generates. On 2026-08-16 a fix for an acked-write-loss bug changed `reconcile`. The leader sends only a 64-entry window of its term map, and the fix made reconcile line that window up inside the follower's full map before comparing. In code terms the window is aligned where `leader[0] == own[j]` with `j > 0`, and the new branch handles the `j > 0` case.

Until 2026-08-29, the generator only ever built the follower's map as a prefix-extension of the leader's. In that shape `j` is always zero. So the branch the fix added appeared in **zero** of the rig's vectors. The measured count was 0 of 33,334 reconcile vectors under seed 1. The fix had been replayed against the model's `reconcileAligned` exactly never.

Since 2026-08-29 the generator produces the branch by construction. One reconcile vector in three now comes from a windowed arm that builds a shared history of 65 to 96 entries. Per 100,000-vector run:

| case | count |
|---|---|
| `j > 0` vectors | about 7,400 |
| of which clean, no divergence found | about 800 |
| of which cut at a divergence | about 6,600 |
| `leader[0] ∉ own`, so `NoCommonPrefix` | about 2,200 |
| cut at the window's first entry | about 2,200 |

About 2,350 of the vectors in this table also fire the same-term, different-base clamp.

This was verified on three seeds and 300,000 vectors with zero divergence. The checker was also shown to have teeth: a deliberately corrupted `j > 0` expectation is reported as a divergence, so it catches a wrong answer and not only confirms a right one.

**The theorems lag the rig here.** The `Uc2Proofs` theorems R1 to R6 are still stated over the pre-alignment core. The note in `Uc2Model/Reconcile.lean` says so. The rig checks the shipped wrapper. The theorems do not yet.

### Build-time pins for the hand-written edge cases

Random sampling gives no guarantee of hitting an edge case a human thought of. So every unit test in `reconcile.rs`, all 10 of them, and every unit test in `commit.rs` is also ported into Lean as a `#guard` assertion. Each sits next to the model definition it exercises and is evaluated on every `lake build`. If the model ever disagrees with a unit test, the build fails.

### What the rig cannot see

The rig covers the three consensus kernels. It does not cover the whole node. The durable-counter caveat above is the concrete example: the hazard was in how two threads read one value, and the model could not express that at all. No amount of replaying vectors against such a model would have surfaced it.

The way to close that gap for good is to translate the real Rust into Lean mechanically and prove theorems about the translation. [Aeneas](https://github.com/AeneasVerif/aeneas) does this, and the approach worked in [`ultima_db`](https://github.com/PeterKnego/ultima_db), where the proofs run against mechanically translated code. It was tried here and **stopped at a toolchain wall**. At the time, Aeneas's Lean support library pinned Lean v4.31.0, and it did not build under this repo's v4.32.0. Charon, the front end that reads the Rust, processed `uc_consensus` cleanly. So the approach was feasible, and only the version timing blocked it. Downgrading the repo's Lean to chase a research tool was rejected. The retry condition was Aeneas moving to v4.32.0 or later.

**That condition has been met, but the retry has not been run.** Aeneas moved to Lean v4.35.0 on 2026-10-15, in commit `1238879f`. On 2026-11-10 this repo moved to v4.35.0 too, so the two now share a toolchain. A retry should pin that Aeneas commit rather than Aeneas's `main`, which has since moved on to a release candidate.

## The trusted base

The trusted base is everything you must take on faith for the proofs to mean anything. Here it is as small as Lean allows. Every theorem passes `#print axioms` with only the three standard axioms Lean and mathlib rest on: `propext`, `Classical.choice` and `Quot.sound`. Beyond those:

- No `sorry` anywhere in the chain.
- No `native_decide`. That tactic proves a fact by compiling and running code, which would put the Lean compiler itself in the trusted base.
- No project-local axioms or escape hatches.
- **No SMT solver.** Nothing in `proofs/` calls one. The [Veil page](veil.md) explains why that matters.

## Toolchain and build

The proofs use Lean 4 v4.35.0 with mathlib pinned at v4.35.0. `lake build` completes about 3,177 jobs with zero warnings and zero `sorry`.

The move from v4.32.0 to v4.35.0 was made on 2026-11-10. It changed no theorem statement. It made three kinds of edit to proof scripts:

- Renamed lemmas: `if_pos`, `if_neg`, `if_true` and `if_false` became `ite_eq_left`, `ite_eq_right`, `ite_true` and `ite_false`, which have identical types.
- Three `rw` steps in `TakeDiscipline.lean` stopped matching their hypothesis, and became `simp only` steps.
- New linters flagged unused `simp` arguments and redundant tactics, which were removed.

mathlib v4.35.0 is built for Lean's module system, and these files do not use it yet. `allowNonModules = true` in `lakefile.toml` declares that mix instead of carrying one warning per import.

The axiom check moved in one way. Four results now also list `Classical.choice`: `canon_sat_witness`, `Cert.transport`, `cert_of_leader` and `nonvacuity_commit_completeness_trace`. The cause is upstream. Under v4.35.0, `simp` and `omega` reach core lemmas such as `List.perm_iff_count` that already rested on `Classical.choice` in v4.32.0. `CanonWitness.lean` was not edited, and it moved too. Every result still rests only on the standard three axioms. Two conformance replays of 100,000 vectors each, seed 1 and seed 20261110, showed zero divergence after the move.
