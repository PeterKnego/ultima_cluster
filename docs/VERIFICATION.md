# Verification

What is **proved**, what is **checked**, what is only **bug-hunted** — and how to
reproduce each one yourself.

This document exists because "verified" is a word that hides an enormous range.
A machine-checked theorem and a passing fuzz run are both often called
verification, and they carry very different weight. Everything below is sorted by
the strength of the evidence, and the boundaries between tiers are stated
explicitly rather than blurred.

**Status of this document:** current as of the M8 gate (2026-07-29). Each section
cites the dated record it summarizes; where the two disagree, the dated record
wins.

---

## Summary

| Layer | Strength | What it covers |
|---|---|---|
| **Lean 4 proofs** | Machine-checked | Consensus safety kernels; election safety and log matching over an N-node protocol model |
| **Conformance rig** | Executable, exhaustive-by-sampling | That the Lean model and the real Rust agree, vector by vector |
| **Deterministic simulation** | Checked under seeded fuzz | Nine whole-cluster safety invariants under fault injection |
| **WGL lincheck capstones** | Checked on real processes | Linearizability of a register under leader kills, crashes, partitions, purge |
| **Elle** | Checked on real processes | Transactional safety (serializable and strict) — plus a mutation tier proving the harness has teeth |
| **Multi-process crashtest** | Checked on real processes | Recovery correctness under `SIGKILL` mid-load |
| **loom** | Exhaustive over interleavings | The frame-visibility memory protocol |
| **Veil** | Bug-hunting only — **never the record** | Bounded model checking of the election and reconfiguration planes |

### The headline result

The Lean effort is not decorative. **It found and fixed four real, shipped safety
bugs** in code that had already passed the simulation, linearizability, and
crash-recovery tiers (Findings #5, #6b, #8, #9 in
[`docs/benchmarks/uc2-lean-gate-2026-07-16.md`](/docs/benchmarks/uc2-lean-gate-2026-07-16.md)).
Two of them were acked-write-loss class:

- **Finding #6b** — a Raft §5.4.2 / Figure-8-class bug. The §5.4.2 barrier was
  applied to linearizable reads, ingress admission, and reconfiguration, but
  *never to the commit advance itself*, so a leader routinely committed an
  old-term-only range below the barrier. A divergent higher-term rival could then
  win the next election and truncate the committed bytes cluster-wide. Driven end
  to end by a 46-step, n = 5 machine-checked countermodel.
- **Finding #5** — the reconciliation intake gate did not survive a reboot. A
  voter that granted term T, held a divergent tail, and crashed before
  reconciling rebooted with the gate open; its stale same-term report fed the
  leader's commit tracker, certifying a **phantom commit** over content it did
  not hold.

Both were found by trying to prove a theorem and discovering it was false against
the code as shipped. Neither was reachable by the fuzz tiers that had been green
for weeks. That is the argument for doing this work, and it is the reason this
document leads with it.

---

## 1. Machine-checked proofs (Lean 4)

**Location:** [`proofs/`](/proofs) · **Record:**
[`docs/benchmarks/uc2-lean-gate-2026-07-16.md`](/docs/benchmarks/uc2-lean-gate-2026-07-16.md)
and [`docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md`](/docs/benchmarks/uc2-lean-phase2-spike-2026-07-17.md)

Lean 4 v4.32.0, mathlib pinned at v4.32.0. `lake build` completes ~3,027 jobs
with zero warnings and zero `sorry`.

### What is proved

**Kernel layer** (Phase 1) — 14 theorems over `Uc2Model`, a mathlib-free mirror of
`uc2_consensus`'s three pure-sync safety kernels:

- **Commit tracker** — commit is monotone; never certifies past the caller's own
  durable position; every commit value was backed by quorum-many reports at or
  above it *at the step it was set*; a reset genuinely clears certifying power.
- **Reconcile** — output never exceeds the caller's durable frontier; agreed
  bytes stay valid; divergence is always cut; `validUpTo` is *exactly* the first
  byte-content divergence, not merely a sound bound; idempotent.
- **Vote freshness** — a voter cannot grant two different candidates in the same
  term; a granted vote implies the candidate's log passed the freshness check,
  with an exact lexicographic characterization.
- **Quorum intersection** — any two majorities of a fixed cluster share a member,
  as a Lean `Finset` fact rather than an assumption.

**Protocol layer** (Phase 2 and Tier B) — over an N-node model with a sent-set
network, havoc data plane, and crash-restart:

- **`election_safety`** — proved sorry-free, at both model levels.
- **`Uc2.Data.log_matching`** — proved sorry-free over the layered data plane
  (payload history + data-stamped term map).

### What is *not* proved

**`leader_completeness` is open.** As of the closure arc banked 2026-07-19 it is
reduced to a single named obligation — the `canon` invariant — with its assembly,
crux, consumer interface, antitonicity, and a machine-checked satisfiability
witness all landed. The hypothesis that previously made it conditional
(`frames_current_authored`) has since been **discharged unconditionally**.
Finding #11 records why the remaining step needs joint/well-founded induction
rather than the corpus's standard shape, and confirms this is a matter of scope
rather than falsity. Estimated 3–4 further tasks.

**And a caveat that matters more than the open theorem itself.** The model's
`PNode.durable` is a single `Nat`, while the real node's durable counter has
**two independent readers on two threads** — the receiver, which reports it for
commit ranking, and the consensus agent, which absorbs it a duty cycle later for
vote credentials. Because the model collapses them, the derived lemma
`ReportEraFloor` discharges its `sendReport` case by `Nat.le_refl`: trivially
true in the model, and false in the real system. That composition is exactly the
informal safety argument that Finding #12 refuted with a shipped acked-write-loss
bug.

So: **`leader_completeness`, if completed over the current model, would be
completed over a model that assumes that bug away.** Splitting the counter
invalidates `ReportEraFloor`'s reflexivity proof and everything composed from
it, so it is scoped as its own piece of work rather than a patch. The Rust is
fixed and the `uc2_sim` half is closed (§2); the Lean half is open.

State-machine safety is not attempted and is gated on finishing the above.

### The trusted base

Every theorem passes `#print axioms` with only the standard Lean/mathlib trio —
`propext`, `Classical.choice`, `Quot.sound`. Specifically:

- No `sorry`, anywhere in the chain.
- No `native_decide` (which would place the Lean compiler in the trusted base).
- No project-local axiom escape hatches.
- **No SMT.** Nothing in `proofs/` calls a solver; see §7 on Veil.

### The model-versus-code gap, and the rig that closes it

The honest objection to any protocol proof is: *your theorems are about a model,
and you ship Rust.* Two mechanisms address it.

**The conformance rig.** `uc2_consensus/examples/conform_gen.rs` drives the
*real* `CommitTracker::advance`/`on_durable`/`reset_reports`,
`reconcile::reconcile`, and `election::log_ok_order` under a seeded PRNG, emitting
JSONL vectors whose expected values are the implementation's own output.
`proofs/Conform/Main.lean` re-derives each outcome from `Uc2Model` and diffs it
bit for bit.

- Implementer run: **100,000 vectors, zero divergence.**
- Independent reviewer re-run, different seed: **20,000 vectors, zero divergence.**
- Nightly CI re-runs with a date-rotated seed.
- Distribution confirmed diverse rather than degenerate — ~8% `NoCommonPrefix`
  outcomes, ~4,000 vectors exercising a real truncation, ~2,400 full-tie
  `log_ok` calls, ~20% tracker resets.

**Build-time `#guard` pins.** Random sampling gives no guarantee of hitting a
hand-written edge case, so every `reconcile.rs` unit test (all 10) and every
`commit.rs` unit test is additionally ported to an executable `#guard` assertion
next to the corresponding model definition, checked on every `lake build`.

**The residual gap, stated plainly.** The rig covers the three consensus kernels,
not the whole node — and the durable-counter collapse above is a concrete
instance of what that leaves uncovered: a hazard the model could not express at
all, so no amount of vector replay against it would have surfaced the bug. Extraction of the real Rust into Lean via
[Aeneas](https://github.com/AeneasVerif/aeneas) — the approach used successfully
in [`ultima_db`](https://github.com/PeterKnego/ultima_db), where proofs run
against mechanically translated code — was attempted here and **exited at a
toolchain wall**: Aeneas's Lean support library pins v4.31.0 and does not build
under this repo's v4.32.0. Charon processed `uc2_consensus` cleanly, so the
approach is feasible; only the version timing blocks it. Downgrading the repo's
Lean pin to chase a research tool was rejected. Retry condition: Aeneas bumps its
toolchain to ≥ v4.32.0.

---

## 2. Deterministic simulation

**Location:** [`uc2_sim/`](/uc2_sim)

A virtual-time cluster driving the *real* `ElectionSm` — `world.rs` wires
`uc2_consensus` directly, so a fix in the consensus crate is automatically
reflected rather than mirrored by hand. Seeded fault fuzz with nine whole-cluster
safety invariants swept after **every** event:

| | Invariant |
|---|---|
| inv1 | Election safety — no split brain |
| inv2 | Term-map prefix consistency |
| inv3 | Commit monotonicity |
| inv4 | Committed bytes are never truncated |
| inv5 | Leader completeness |
| inv6 | Config determinism |
| inv7 | Quorum legality — no phantom commit, and the commit rides a chaining config |
| inv8 | Revert correctness after truncation settles |
| inv9 | Tombstone permanence |

Directed scenarios stage specific historical bugs as permanent regression pins —
including `rebooted_unreconciled_voter_must_not_certify_phantom_commit` (Finding
#5) and `old_term_range_must_not_commit_before_new_term_quorum` (Finding #6b),
both verified RED before their fix and GREEN after.

The simulator was itself blind to the durable dual-reader hazard until recently:
`world.rs` advanced the counter and fed `DurableAdvanced` in the same handler, so
the report path and the vote-credential path could never disagree.
`SimEvent::ConsensusStep` now absorbs the counter on its own cadence — exactly as
the two threads do — pinned by
`stale_vote_credential_opens_a_term_below_a_committed_position`.

```bash
cargo test -p uc2_sim                          # standard tier
cargo test -p uc2_sim --features sim-heavy     # 1000-seed fuzz
```

---

## 3. Linearizability — WGL capstones

**Location:** [`uc-lincheck/`](/uc-lincheck) + `uc2_node/tests`

A concurrent CAS-register history checked for linearizability by a
Wing-Gong-Lowe search, while the harness kills leaders, crashes services,
partitions the network, and — in the M6 tier — runs snapshot-backed purge
underneath. Real node and service agents, real reliable-UDP over loopback, real
instance directories.

```bash
cargo test --workspace          # includes the capstones
```

---

## 4. Transactional safety — Elle

**Record:** [`docs/benchmarks/uc2-elle-gate-2026-07-16.md`](/docs/benchmarks/uc2-elle-gate-2026-07-16.md)

Where the lincheck capstones check linearizability of a *single register*, Elle
checks **transactional safety of a list-append workload** by cycle detection over
the recorded history — catching a class of anomaly the register capstone cannot
phrase.

Checker: vendored `elle-cli` 0.1.9, pinned by sha256. Histories recorded in
Jepsen EDN. Both models are run: `serializable` and **`strong-serializable`** (the
strict, real-time model). A cycle-search timeout (`unknown`) is treated as a hard
**FAIL**, never a pass.

### Clean tier — five passes, all clean under both models

| Pass | Events | serializable | strong-serializable |
|---|--:|---|---|
| quiet | 100,836 | clean | clean |
| failover | 45,702 | clean | clean |
| partition | 51,770 | clean | clean |
| purge | 54,574 | clean | clean |
| reconfig | 96,714 | clean | clean |

Every invocation first self-tests the checker against two fixtures — a known
write-skew cycle that must be rejected under `serializable`, and a real-time
violation that plain serializability accepts but the strict model must reject.
The checker's teeth are verified before any real verdict is trusted.

### Mutation tier — proving the harness can fail

A clean verdict from a harness that cannot detect anything is worthless. Three
consensus bugs are injected behind a `mutation-testing` cargo feature (off in
every default build) and the harness must catch all three:

| Injected bug | Oracle | Caught as |
|---|---|---|
| `commit-quorum-minus-one` | Elle INVALID under both models | `incompatible-order`, `strong-PL-1-cycle-exists` |
| `skip-read-barrier` | Elle INVALID under the **strict model only** | `G-single-item-realtime` |
| `skip-vote-order-check` | Driver hard-fails | truncation-below-commit panic |

`skip-read-barrier` is the tooth that proves the strict model earns its keep: its
anomaly is invisible to plain serializability, and only the real-time model
catches it.

**A finding worth recording.** The original design mapped two mutations to the
natural failover and partition passes — and empirically *neither could be
exposed there*, because UC's own layered defenses absorb them. `kill_and_restart_leader`
restarts the same node on its own disk, so a quorum-minus-one-committed tail
never actually dies; and UC has no check-quorum step-down, so skipping only the
read barrier still yields no stale read under gross isolation. Each tooth
therefore uses a dedicated adversary matched to how UC actually catches that bug.
This is a real robustness result about the system, not a harness defect — and it
is exactly the kind of thing a mutation tier exists to discover.

```bash
scripts/elle_check.sh       # clean tier
scripts/elle_mutation.sh    # mutation tier
```

Feature-off inertness is verified explicitly: with the feature off, the default
build is byte-identical and the read-path mutation is `#[cfg]`-shadowed out.

---

## 5. Multi-process hard crash

**Location:** [`examples/uc2-crashtest/`](/examples/uc2-crashtest)

Real node and service processes, `SIGKILL`ed mid-load. Recovery is required to
stay linearizable — not merely to start up.

```bash
cargo test -p uc2-crashtest --features hard-crash-tests
```

---

## 6. Memory model — loom

An exhaustive interleaving check of the frame-visibility protocol: the atomic
handshake by which a reader observes a fully-written frame and never a torn one.

```bash
RUSTFLAGS="--cfg loom" cargo test -p uc2_log --test loom_frame --release
```

Offset-pin tests additionally freeze the wire and `cnc` page layouts, so a
layout change cannot pass silently.

---

## 7. Veil — bug-hunting only, never the record

**Location:** [`proofs-veil/`](/proofs-veil) (archive) · **Record:**
[`docs/benchmarks/uc2-veil-commit-plane-checkpoint-2026-07-26.md`](/docs/benchmarks/uc2-veil-commit-plane-checkpoint-2026-07-26.md)

[Veil](https://github.com/verse-lab/veil) (CAV 2025) is used here as a **bounded
explicit-state model checker for bug-finding and design assurance**. It is
deliberately excluded from the trust story, under hard guardrails:

1. **Veil is never the record.** Permanent proofs live in `proofs/` — Lean
   v4.32.0, standard axiom trio, no SMT in the trusted base. Veil's deliverables
   are *countermodel traces* (which become directed `uc2_sim` regressions and
   Rust fixes) and, secondarily, candidate invariant text. A Veil model has no
   conformance rig; it is scratchpad-only. Any bug it finds is independently
   reconfirmed in Rust before a fix is made.
2. **Toolchain isolation.** The models target Veil's `veil-2.0-preview` on Lean
   v4.28.0 — incompatible with `proofs/`'s v4.32.0. They are never on any
   `proofs/` build path, CI gate, or "proved" claim, and they do not build in
   this repo. There is deliberately **no CI job**.
3. **No migration of anything proved.** Election safety, log matching, and the
   leader-completeness support stack are done in `proofs/`; Veil touches none of
   them.

Why say all this out loud? Because a bounded model check over 4.2 million states
is genuinely useful and is *not* a proof, and a project that blurs the two has
made its strongest claims unfalsifiable. The distinction is the point.

---

## 8. Benchmark methodology

Verification claims and performance claims fail the same way — by choosing the
bar after seeing the number. Every milestone gate in
[`docs/benchmarks/`](/docs/benchmarks) therefore commits its **pass/fail rule to the
repository before the run**, in its own commit. From the M8 gate:

> **Status: RULE PRE-COMMITTED, NOTHING MEASURED YET.** This file is committed
> *before* either arm runs. Everything below the "Pre-committed decide rule"
> heading is a promise made in ignorance of the data; the result sections are
> empty placeholders and are filled in afterwards, whatever they say.
>
> A bar chosen after seeing the number is not a bar, it is a description.

If an arm misses, the document records an honest FAIL and the real number, and
the bar does not move. Git history is the audit trail — the decide rule and the
result are separate commits, in that order.

---

## 9. Continuous integration

| Workflow | Contents |
|---|---|
| `ci.yml` | Fast gate on every PR: workspace build, tests, clippy `-D warnings` |
| `nightly.yml` | Full proof suite — lincheck capstones, `sim-heavy`, loom, crashtest, Elle clean tier, `lean-proofs` conformance replay with a date-rotated seed |
| `elle-weekly.yml` | Elle mutation tier |

---

## 10. What is *not* verified

The most important section, and the one most projects omit.

- **`leader_completeness` is not proved** (§1). Election safety and log matching
  are; the remaining theorem is reduced to one named obligation and is open.
- **The model collapses the durable counter's two independent readers into one
  value** (§1), which makes a load-bearing lemma trivially true in the model and
  false in the real system. A real acked-write-loss bug lived in exactly that gap
  (Finding #12) — found from the Rust side, not by the proofs. Rust fixed,
  `uc2_sim` closed, Lean split still open. Until it is done, proofs composed over
  that lemma are weaker than they look.
- **State-machine safety is not attempted**, and is gated on the above.
- **The proofs cover the consensus kernels and protocol model, not the whole
  node.** The reliable-UDP data plane, the archive/journal path, the IPC ring
  buffers, and the service SDK are covered by the simulation, linearizability,
  and crash tiers — not by machine-checked proof.
- **No Rust-to-Lean extraction on this repo** (§1). The conformance rig plus
  `#guard` pins are the standing linkage; Aeneas extraction is blocked on
  toolchain versions, not on feasibility.
- **Your state machine's determinism is your responsibility.** SMR replicates
  bytes and guarantees every replica applies the same commands in the same order.
  Nondeterminism in `apply` — clocks, iteration order, floats, ambient state —
  produces divergence that no layer here can catch.
- **Bounded model checks are bounded.** Veil's clean runs are exhaustive to a
  depth, not to all executions (§7).
- **The published gate numbers are fleet measurements**, on the hardware and
  configuration each record names. They are reproducible, not universal.
- **Wire crypto is opt-in and off by default.** With it disabled the posture is a
  trusted network. With it enabled, the threat model is a network-path adversary;
  a compromised host and a malicious cluster member are explicitly **out of
  model** — the group key is symmetric, so any holder can forge fan-out traffic
  as any node. See the M8 gate record and runbook §11.

---

## Reproducing everything

```bash
# Proofs — from proofs/
lake build                                                  # 3027 jobs, zero sorry
lake exe conform --seed 20260716 --count 100000             # model vs. real Rust

# Simulation
cargo test -p uc2_sim --features sim-heavy                  # 1000-seed fuzz

# Linearizability + the rest of the suite
cargo test --workspace

# Transactional safety
scripts/elle_check.sh                                       # clean tier
scripts/elle_mutation.sh                                    # the harness's teeth

# Hard crash
cargo test -p uc2-crashtest --features hard-crash-tests

# Memory model
RUSTFLAGS="--cfg loom" cargo test -p uc2_log --test loom_frame --release
```

Elle histories must not be written to `/tmp` on a RAM-backed box; both scripts
default to `$HOME/.cache`.
