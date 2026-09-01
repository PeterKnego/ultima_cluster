# Verification

What is **proved**, what is **checked**, what is only **bug-hunted** — and how to
reproduce each one yourself.

This document exists because "verified" is a word that hides an enormous range.
A machine-checked theorem and a passing fuzz run are both often called
verification, and they carry very different weight. Everything below is sorted by
the strength of the evidence, and the boundaries between tiers are stated
explicitly rather than blurred.

**Status of this document:** current as of the M14c2 proof pass (`2.8.1`,
2026-08-30), which replaced §11's M14 gap statement with the two-FSM coverage
record and added the two-FSM capstones to §3–§5; §7 as of M12d; the proof and
simulation tiers as of the M8 gate (2026-07-29). Each section cites the dated
record it summarizes; where the two disagree, the dated record wins.

---

## Summary

| Layer | Strength | What it covers |
|---|---|---|
| **Lean 4 proofs** | Machine-checked | Consensus safety kernels; election safety and log matching over an N-node protocol model |
| **Conformance rig** | Executable, exhaustive-by-sampling | That the Lean model and the real Rust agree, vector by vector |
| **Deterministic simulation** | Checked under seeded fuzz | Nine whole-cluster safety invariants under fault injection |
| **WGL lincheck capstones** | Checked on real processes | Linearizability of a register under leader kills, crashes, partitions, purge — single- and two-FSM |
| **Elle** | Checked on real processes | Transactional safety (serializable and strict), single- and two-FSM — plus a mutation tier proving the harness has teeth |
| **Multi-process crashtest** | Checked on real processes | Recovery correctness under `SIGKILL` mid-load — single- and two-FSM |
| **loom** | Exhaustive over interleavings | The frame-visibility memory protocol, the MPSC ring's per-record commit protocol, **and the Broadcast ring's seqlock read barrier** |
| **Fuzzing (libFuzzer)** | Checked under coverage-guided input search | Totality of the fifteen decoders that see bytes the process did not write |
| **Miri** | Checked under a symbolic interpreter | Undefined behaviour in the pure wire/journal decoders and `uc_remote`'s Vec-backed SPSC internals (**not** the file-backed rings) |
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
`uc_consensus`'s three pure-sync safety kernels:

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
fixed and the `uc_sim` half is closed (§2); the Lean half is open.

State-machine safety is not attempted and is gated on finishing the above.

### The trusted base

Every theorem passes `#print axioms` with only the standard Lean/mathlib trio —
`propext`, `Classical.choice`, `Quot.sound`. Specifically:

- No `sorry`, anywhere in the chain.
- No `native_decide` (which would place the Lean compiler in the trusted base).
- No project-local axiom escape hatches.
- **No SMT.** Nothing in `proofs/` calls a solver; see §8 on Veil.

### The model-versus-code gap, and the rig that closes it

The honest objection to any protocol proof is: *your theorems are about a model,
and you ship Rust.* Two mechanisms address it.

**The conformance rig.** `uc_consensus/examples/conform_gen.rs` drives the
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
- **The window-aligned reconcile branch is generated by construction
  (2026-08-29).** The 2026-08-16 acked-write-loss fix made `reconcile` align
  the leader's 64-entry wire window inside the follower's full map
  (`leader[0] == own[j]`, `j > 0`). Until 2026-08-29 the generator only ever
  built `own` as a prefix-extension of `leader`, so `j > 0` occurred in
  **zero** of its vectors (measured: 0 of 33,334 reconcile vectors under seed
  1) — the branch the fix added was replayed against `reconcileAligned`
  exactly never. One reconcile vector in three now comes from a windowed arm
  over a 65–96-entry shared history: per 100k run, ~7,400 `j > 0` vectors
  (~800 clean, ~6,600 cut at a divergence), ~2,200 `leader[0] ∉ own` →
  `NoCommonPrefix`, ~2,200 cut-at-window-start, ~2,350 of those also firing
  the same-term/different-base clamp. Verified on three seeds, 300k vectors,
  zero divergence; a corrupted `j > 0` expectation is reported as a
  divergence (the checker has teeth on the new shapes). The `Uc2Proofs`
  theorems R1–R6 are still stated over the pre-alignment core (see
  `Uc2Model/Reconcile.lean`'s note) — the rig now checks the shipped
  wrapper, the theorems do not yet.

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
under this repo's v4.32.0. Charon processed `uc_consensus` cleanly, so the
approach is feasible; only the version timing blocks it. Downgrading the repo's
Lean pin to chase a research tool was rejected. Retry condition: Aeneas bumps its
toolchain to ≥ v4.32.0.

---

## 2. Deterministic simulation

**Location:** [`uc_sim/`](/uc_sim)

A virtual-time cluster driving the *real* `ElectionSm` — `world.rs` wires
`uc_consensus` directly, so a fix in the consensus crate is automatically
reflected rather than mirrored by hand. Seeded fault fuzz with ten whole-cluster
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
| inv10 | Report ceiling — a clamped report never exceeds its unclamped value or its apply ceiling, and never decreases except across a truncation, restart, role change or ceiling change (M14b) |

Directed scenarios stage specific historical bugs as permanent regression pins —
including `rebooted_unreconciled_voter_must_not_certify_phantom_commit` (Finding
#5) and `old_term_range_must_not_commit_before_new_term_quorum` (Finding #6b),
both verified RED before their fix and GREEN after.
`capped_quorum_stalls_commit_and_releasing_one_follower_resumes_it` models
M14a's report ceiling as a per-node apply cap and pins that commit stalls iff a
commit quorum is capped.

The simulator was itself blind to the durable dual-reader hazard until recently:
`world.rs` advanced the counter and fed `DurableAdvanced` in the same handler, so
the report path and the vote-credential path could never disagree.
`SimEvent::ConsensusStep` now absorbs the counter on its own cadence — exactly as
the two threads do — pinned by
`stale_vote_credential_opens_a_term_below_a_committed_position`.

It was blind in a second place until 2026-08-29. Every `run_*` loop stopped a
run the moment any node's term map reached `MAX_TERM_MAP_WIRE_ENTRIES - 2`
entries ("a wire limitation, not a safety bug — cap the run rather than
provoke it"), so the leader's 64-entry wire window never slid inside a
follower's map — and the window-aligned `reconcile` branch added by the
2026-08-16 acked-write-loss fix was executed by the simulator, including
`sim-heavy`'s thousand seeds, exactly never. The cap is gone;
`window_slide_past_64_lifetime_terms_reconciles_healthy_followers_clean`
drives a cluster through 70+ leader changes and pins zero wipes and zero
truncations of healthy followers with every map converging (verified RED
against the cap: "round 61: the cluster did not reconverge"), and its red
twin `window_slide_with_index_aligned_reconcile_wipes_healthy_followers`
restores the pre-fix index-aligned match through a `mutation-testing` tooth
(`reconcile::reconcile_index_aligned`, the old body verbatim) and pins that
the sim then sees the wipes. Nightly runs both.

```bash
cargo test -p uc_sim                          # standard tier
cargo test -p uc_sim --features sim-heavy     # 1000-seed fuzz
cargo test -p uc_sim --features mutation-testing --test scenarios window_slide  # red twin
```

---

## 3. Linearizability: WGL capstones

**Location:** [`uc_lincheck/`](/uc_lincheck) + `uc_node/tests`

A concurrent CAS-register history checked for linearizability by a
Wing-Gong-Lowe search, while the harness kills leaders, crashes services,
partitions the network, and — in the M6 tier — runs snapshot-backed purge
underneath. Real node and service agents, real reliable-UDP over loopback, real
instance directories.

**Two-FSM capstones (`2.8.1`, M14c2).** Every node runs two attached state
machines and the same checker adjudicates **one history per FSM**, with a
second oracle on top: **replication equivalence** — every `submit_all`'s
per-FSM answers must be byte-equal, and a disagreement is counted and recorded
as `Indeterminate` in both histories rather than silently taken from FSM 0.

| test | file | what it drives |
|---|---|---|
| `two_fsm_bounded` | `uc_node/tests/lin_v2.rs` | two FSMs at `FsmLag::Bounded(64 KiB)` under failover and purge/snapshot churn |
| `two_fsm_lockstep` | `uc_node/tests/lin_v2.rs` | the same faults at `FsmLag::Lockstep` |
| `two_fsm_slow` | `uc_node/tests/lin_v2.rs` | a fast FSM beside `Slow<RegisterSm, 200>` (200 µs/apply), bounded |
| `two_fsm_slow_lockstep` | `uc_node/tests/lin_v2.rs` | the same pair in lockstep |
| `minority_partition_and_heal_two_fsm` | `uc_node/tests/lin_partition_v2.rs` | minority partition, quorum loss and heal, per-FSM WGL before and after |
| `two_fsm_service_sigkill` | `examples/uc_crashtest/tests/hard_crash.rs` | §5 — FSM 1's process `SIGKILL`ed mid-load |
| `two_fsm_node_sigkill` | `examples/uc_crashtest/tests/hard_crash.rs` | §5 — the node and both services killed together |

Two more tests keep those honest rather than adding coverage of their own:

- **The equivalence oracle is shown to bite — the same oracle the capstones
  run.** `two_fsm_oracle_bites` (`lin_v2.rs`, `#[should_panic(expected =
  "replication-equivalence violated")]`) puts FSM 1 behind `Corrupt<RegisterSm>`
  and then drives `lincheck_v2::spawn_workers2` — the capstones' own worker
  loop, whose `a != b` arm is what feeds `equiv_failures` — before making the
  identical `equiv_failures == 0` assertion `run_two_fsm` makes. It is
  deliberately not an inline hand-rolled comparison: weakening the workers'
  fan-in check has to stop this test panicking. An oracle that has never been
  made to fail is not evidence.
- **The slow-FSM oracle** is what `two_fsm_slow*` actually assert, beyond
  linearizability: at every 50 ms sample the FSMs' separation stays inside the
  policy (≤ `fsm_lag` bounded, ≤ one 288 B frame in lockstep), **and** over the
  run's second half their apply rates agree within 10 %. Measured ratio 1.000
  in both runs.

  What that does **not** show, measured 2026-08-30 (dev-box smoke, never a
  gate): both FSMs progressed at the same rate, and the separation never
  approached the bound. `two_fsm_slow` (`Bounded(64 KiB)`) sampled
  `max_lag = 192 B of 65536` — 0.3 % of the bound, ~3.7 frames at ~52 B/frame —
  at `rate0 = rate1 = 22 111 B/s`; `two_fsm_slow_lockstep` sampled
  `max_lag = 64 B of 288` (~1.2 frames) at `rate0 = rate1 = 22 227 B/s`. At
  ~52 B/frame that is ~425 records/s, an order of magnitude under
  `Slow<RegisterSm, 200>`'s own ~5 000 applies/s ceiling: the **client loop**,
  not FSM 1, is the limiter here, and the lag policy never binds. These runs
  therefore do not exercise a bound-pinned state; the slow-FSM oracle is
  evidence of **equal progress** across a heterogeneous pair, not of the
  barrier's behaviour at the bound. The barrier at the bound is covered by
  `uc_service`'s `lag` unit tests and the apply-hop bench, not here.

One more pin, from the M14d row-d lesson —
`snapshot_restart_installs_only_with_purge` (`lin_v2.rs`): a `SnapshotPolicy`
shortens a service restart **only together with purge, and only once the live
log buffer has wrapped past `start_pos`**; below the wrap a restart reads the
still-live ring and touches neither the journal nor a snapshot, whatever the
purge posture.

```bash
cargo test --workspace          # includes the capstones
```

---

## 4. Transactional safety: Elle

**Record:** [`docs/benchmarks/uc2-elle-gate-2026-07-16.md`](/docs/benchmarks/uc2-elle-gate-2026-07-16.md)

Where the lincheck capstones check linearizability of a *single register*, Elle
checks **transactional safety of a list-append workload** by cycle detection over
the recorded history — catching a class of anomaly the register capstone cannot
phrase.

Checker: vendored `elle-cli` 0.1.9, pinned by sha256. Histories recorded in
Jepsen EDN. Both models are run: `serializable` and **`strong-serializable`** (the
strict, real-time model). A cycle-search timeout (`unknown`) is treated as a hard
**FAIL**, never a pass.

### Clean tier — six passes, all clean under both models

| Pass | Events | serializable | strong-serializable |
|---|--:|---|---|
| quiet | 100,836 | clean | clean |
| failover | 45,702 | clean | clean |
| partition | 51,770 | clean | clean |
| purge | 54,574 | clean | clean |
| reconfig | 96,714 | clean | clean |
| quiet_two_fsm — FSM 0 | 16,062 | clean | clean |
| quiet_two_fsm — FSM 1 | 16,060 | clean | clean |

`quiet_two_fsm` is the M14c2 (`2.8.1`) addition: a quiet two-FSM cluster at
`FsmLag::Bounded(64 KiB)`, recording **one history per FSM**
(`$ELLE_DIR/quiet_two_fsm/fsm{0,1}/history.edn`), each adjudicated separately
under both models by `scripts/elle_check.sh`, with the same replication-
equivalence oracle §3 describes asserted at zero. **That oracle is structurally
weak in this tier and carries no equivalence evidence of its own**: the Elle
workload's `LaResp` has a single variant, so the two FSMs' answers can only
differ by being malformed — a missing id, a non-ascending pair — and the check
can therefore only fail on a malformed fan-in, never on a genuine state
divergence. The equivalence evidence is the WGL capstones' (§3, §5). The five
older rows are the
2026-07-16 gate's default sizing; the two-FSM row is the 2026-08-30 run at
`ELLE_TARGET_OPS=8000` (the sizing nightly uses), 100 % of ops `ok` on both
FSMs.

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

**Location:** [`examples/uc_crashtest/`](/examples/uc_crashtest)

Real node and service processes, `SIGKILL`ed mid-load. Recovery is required to
stay linearizable — not merely to start up.

**Two FSMs (`2.8.1`, M14c2).** Two scenarios run a node with `--services 0,1`
and check *both* FSMs' histories plus the replication-equivalence oracle across
every restart: `two_fsm_service_sigkill` kills and respawns FSM 1's process
under load, and `two_fsm_node_sigkill` kills the node and both services
together, then brings the node back and reattaches both. Six kill cycles across
the two tests, every FSM history `Linearizable`, `equiv == 0` throughout.

```bash
cargo test -p uc_crashtest --features hard-crash-tests
```

---

## 6. Memory model: loom

An exhaustive interleaving check of the frame-visibility protocol: the atomic
handshake by which a reader observes a fully-written frame and never a torn one.

```bash
RUSTFLAGS="--cfg loom" cargo test -p uc_log --test loom_frame --release
RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_mpsc --release
RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_broadcast --release
RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_broadcast --release
```

The second model is the MPSC ring's per-record commit protocol (M13a):
disjoint concurrent claims, the commit word's Release/Acquire visibility
pair, and head-of-line behaviour at a claimed slot.

The third is the **Broadcast ring's seqlock read barrier** (2026-08-31).
Broadcast is the only ring with no backpressure — the single producer may lap
a reader mid-copy — so a read's validity rests entirely on re-reading
`publish_position` after the copy. **Writing this model found a real defect,
which is the reason it exists.** The re-check needs "if lap N+1's bytes are
visible then `publish_position >= N+1` is visible", and the producer's
`Release` store did not provide it: release orders accesses *before* the
store, not after, so the next record's body writes could be observed ahead of
the publish that would have warned the reader. Loom's counterexample is a
consumer accepting a record whose first word is from lap 0 and whose second is
from lap 2 — a torn read reaching the crc that the code's own comment says it
cannot reach. The fix is one publish-before-body `Release` fence in
`BroadcastProducer::write`; it costs **no instruction on x86_64** (TSO already
forbids the reordering) and one `dmb ish` on aarch64, i.e. exactly where it is
needed. The failure is weak-memory-only, and UC builds aarch64 binaries that
CI never executes — so it was unreachable by any test the project runs, only
by a model. (2026-08-31, after the fix shipped: the full test stack ran once
on real aarch64 hardware —
[`uc2-arch-sweep-c8id-vs-c9gd-2026-08-31`](benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md)
— which weakens nothing here: it executed the *fixed* code, and a
weak-memory race is not reliably reachable by running tests on weak hardware
anyway. The model remains the coverage; the hardware run is a smoke check.) Two mutations
(`m1_without_the_producer_publish_before_body_fence`,
`m2_without_the_post_copy_revalidation`) are `#[should_panic]`, so a green run
means the model has teeth rather than that it explored nothing. Plain-language
writeup: [`docs/notes/uc2-broadcast-seqlock-explained.md`](/docs/notes/uc2-broadcast-seqlock-explained.md).

All three models are of protocols, not of mappings — loom cannot see an mmap,
so the ring's *layout* stays frozen by offset-pin tests.

Offset-pin tests additionally freeze the wire and `cnc` page layouts, so a
layout change cannot pass silently.

---

## 7. Fuzzing: decoders total on untrusted bytes

**Location:** [`fuzz/`](/fuzz) · **Runner:** [`scripts/fuzz_smoke.sh`](/scripts/fuzz_smoke.sh)
· **CI:** `nightly.yml`'s `fuzz-groups` + `fuzz` jobs

Every place ultima_cluster parses bytes it did not write has a
structure-unaware [libFuzzer](https://llvm.org/docs/LibFuzzer.html) target
behind it. The property under test is **totality**: for any input, the decoder
returns a value or an error — it does not panic, does not index out of bounds,
and does not allocate a size an attacker chose. Panicking is not a memory-safety
failure in Rust, but on a node it is a fail-stop: the datagram path runs on the
receiver agent and `apply` runs on the service's apply thread, so a panic there
takes the process down. Availability is the thing being defended here.

### The fifteen targets

| Target | Seam, and why its input is untrusted |
|---|---|
| `uc_protocol_datagram` | `uc_protocol::v2::datagram` — the 16-byte header and every body reader. The **first code an unauthenticated UDP packet reaches**; with `[crypto].enabled = false` it is reached before any authentication at all. |
| `uc_protocol_log_frame` | `uc_protocol::v2::frame::read_header`, driven behind the real caller's `len >= HEADER_LEN` guard. Deliberately caller-guarded, so the target pins the guard's contract rather than pretending it is absent. |
| `uc_protocol_cnc` | `uc_protocol::v2::cnc` — the 8 KiB control page (page-2 service-slot band and the 4032 pair since M14a) every attaching process maps and parses. A file on disk any local process with write access can corrupt. |
| `ring_mpsc_record` | `uc_protocol::ring::common`'s MPSC slot decision (`classify_commit_word`) and record decoder (`decode_record_slice`) — what the node's consensus agent meets in a shared-memory ring any local process can write. |
| `uc_remote_frame` | `uc_remote::frame` — the gateway edge's 24-byte TCP frame header and every typed body decoder. Input from any client that can open a socket to the gateway. |
| `uc_crypto_open` | `uc_crypto::seal::{open_in_place, open_detached}` — the AEAD envelope's framing arithmetic, which runs on attacker-chosen bytes *before* the tag has been verified. |
| `uc_crypto_handshake` | `uc_crypto::handshake::Peers::on_message` — the pre-auth Noise `IK` surface. With crypto enabled this is the first thing in the process to see bytes from anyone who can reach the UDP port. |
| `uc_crypto_group_key` | `uc_crypto::group::GroupPlane::on_key_message` — the two distinct message shapes that share datagram kind 20, i.e. a decoder that must disambiguate hostile input. |
| `uc_crypto_admin` | `uc_crypto::admin` — a **property** target over the M12b signed-tag layout: canonical-length agreement, sign/verify round-trip, tag bit-flips rejected, foreign key rejected. |
| `uc_journal_record` | `uc_journal`'s segment header and record decoder — what crash recovery meets in a torn or corrupt segment after a power loss or a full disk. |
| `uc_journal_stable_value` | `uc_journal::stable_value` — the durable vote / term map / snapshot floor slots. Corruption here is a consensus-safety input, not merely a data-loss one. |
| `uc_service_session` | `Sessioned<S>` — the exactly-once envelope under a fuzz-derived, deliberately tiny `SessionConfig` so client eviction, byte eviction and window trim are all reachable, plus its snapshot install path. |
| `uc_node_toml` | `uc_node::config_file::parse_str` — the `node.toml` parser behind every M9/M11/M12b named startup refusal. |
| `uc_gateway_toml` | `uc_gateway::config_file::parse_str` — the gateway's whole named-refusal path, including its own `EdgeConfig::validate`. |
| `uc_node_http` | `uc_node::obs::http::route_raw` — the **unauthenticated** `/metrics` + `/healthz` + `/readyz` request parser. |

### Method

Seeds are generated from fixed literals by the real encoders (`cargo +nightly
run --bin seed-corpus`), so the committed corpus is deterministic and a corpus
change is reviewable in a diff. Nightly CI runs every target for **600 seconds**
on that corpus across four matrix legs; a crash fails the leg and uploads the
artifact. The `fuzz-groups` job asserts the legs' union is exactly the set of
declared targets, so a new target cannot be silently left unfuzzed.

Two honest limits on what a green run means. First, this is a **regression
gate** — "no new crash from this corpus in that budget" — not a bug hunt; real
hunting is a long local `cargo fuzz run`. Second, a fuzz job can be green while
fuzzing almost nothing, so the runner asserts a floor on libFuzzer's reported
execution count (`--min-runs 10000` against 600 s). That assertion exists
because it happened: see the harness finding below.

### What it found

- **Five caller-guarded datagram readers panicked on short slices.**
  `read_datagram_header`, `read_request_vote_body`, `read_vote_body`,
  `read_nak_body` and `read_status_body` sliced fixed offsets out of their
  input, relying entirely on every caller's length pre-guard. Every caller did
  in fact guard, so this was never reachable through the receiver — but a
  totality property that holds only by the discipline of five call sites is a
  property waiting to be broken by the sixth. All five now return `Option` and
  are total; the pre-guards were kept, so behaviour on the real path is byte for
  byte unchanged.
- **`Sessioned::apply` violated the contract it was itself a caller of** —
  user-reachable, and a fail-stop. `RawStateMachine::apply` documents `out` as
  *cleared by the caller*; `Sessioned` pushed its one-byte FRESH tag into `out`
  first and then recovered the response as `out[1..]`. An inner state machine
  that starts with `out.clear()` — which the contract invites — truncated the
  tag away and the slice panicked **on the apply thread**, killing the service on
  its first command. Found by the session target's seed generator before the
  target had been fuzzed once. Fixed by giving the inner machine a genuinely
  cleared buffer; the regression test asserts the response **bytes**, and the
  fuzz target's inner machine deliberately keeps its `out.clear()` so the fix
  stays guarded.
- **`Sessioned::install_snapshot` pre-allocated up to 1 GiB from an unvalidated
  length.** It read an 8-byte length, bounds-checked it against a 1 GiB ceiling,
  and then `vec![0u8; len]` before reading a single blob byte — using the sanity
  bound as an instruction rather than a ceiling. A truncated or corrupt snapshot
  artifact therefore cost a 1 GiB zeroing and an RSS spike per attempt, on the
  apply thread. Now bounded with `take(len)` plus a named truncation error.
  Found not as a crash but as a **throughput collapse** — ten executions in
  ninety seconds where every other target did millions; 20 000 executions went
  91.8 s → 0.34 s after the fix.
- **A harness finding worth recording, because it nearly invalidated the
  tier.** Four of the fourteen targets were executing roughly *sixteen inputs
  per sixty-second run* while printing a perfectly clean line. libFuzzer
  symbolizes each newly discovered function to print a `NEW_FUNC` line, and
  `cargo fuzz` builds with its own `--config profile.release.debug=
  "line-tables-only"` (the root workspace's release profile does not reach the
  excluded `fuzz/` workspace at all), so each sanitizer binary carries ~27 MB of
  debug info and `llvm-symbolizer` needed about ninety seconds to index one for
  a single address — longer than the whole budget. `-print_funcs=0` fixed it (400 runs: 90,180 ms → 57 ms). The lesson is
  not the flag; it is that **a fuzz tier can be green and vacuous**, which is why
  the run-count floor is now asserted in the script and in CI.

### What fuzzing here does *not* cover

- **`FramedConn::read_frame`'s accumulate loop.** The targets drive the frame
  *decoder*; the socket-side loop that assembles partial reads into a frame is
  covered by the gateway's own tests, not by libFuzzer.
- **`CncPage` over a real mmap.** The cnc target parses a byte slice. The real
  page is a shared, concurrently written memory map; its layout is pinned by
  offset-assertion tests and its concurrency is not a fuzzing question.
- **The receiver's stateful dispatch.** Each target is one decoder on one input.
  Sequences of datagrams that drive the receiver's state machine across terms,
  epochs and sessions are the simulation's and the capstones' job (§2, §3).
- **`bincode` itself.** An external crate reached through these seams; it is a
  dependency-posture question (`deny.toml`, SBOM), not a target here.

### Miri — UB detection over the pure decoders

The same decoders are run under [Miri](https://github.com/rust-lang/miri) in
nightly CI (`miri` job): `uc_protocol`'s `v2::` wire/cnc/ipc layer and `version`
packing (43 tests), and `uc_journal`'s segment and `stable_value` record
decoders (19 tests), and `uc_remote`'s `outgoing`/`completion`/`slots` SPSC
structures (29 tests, added with the 2.7.0 client — Vec-backed, so Miri models
them fully, and it caught a real Stacked-Borrows aliasing bug there during
that client's development). libFuzzer finds inputs that panic; Miri finds undefined
behaviour on inputs that do not. Every selected test passes with Miri's
**isolation left on** — nothing needed excluding, and isolation is never
disabled.

**The IPC ring buffers are not Miri-checked, and cannot be.** They are
file-backed shared memory, and Miri stops on them at three separate points:
with isolation on it stops at

```text
unsupported operation: `open` not available when isolation is enabled
```

with `-Zmiri-disable-isolation` it gets one step further and stops at

```text
unsupported operation: unsupported flags for `fallocate` in `mode` argument: 16
```

(mode 16 is `FALLOC_FL_ZERO_RANGE`, the M11 block-reservation fix), and past
that it would stop regardless, on

```text
unsupported operation: Miri does not support file-backed memory mappings
```

A Vec-backed ring variant built purely so Miri could run it is
the stated fallback and **has not been built**, because it would check a
different object than the one that ships. §6's loom coverage is partial: one
model is of the **log buffer's** frame-visibility protocol, a second of the
**MPSC ring's** per-record commit protocol (M13a), a third of the **Broadcast
ring's** seqlock read barrier — but none is of `uc_protocol::ring` as a
whole. So the rings' layout is frozen by offset-pin tests, and their
interleavings and UB are covered by **nothing** — except MPSC (commit-protocol
loom model plus the `ring_mpsc_record` fuzz target since 2.7.0) and Broadcast
(the seqlock model since 2026-08-31). **SPSC, the futex layer and the mapping
itself remain uncovered** — stated again in §11.

---

## 8. Veil: bug-hunting only, never the record

**Location:** [`proofs-veil/`](/proofs-veil) (archive) · **Record:**
[`docs/benchmarks/uc2-veil-commit-plane-checkpoint-2026-07-26.md`](/docs/benchmarks/uc2-veil-commit-plane-checkpoint-2026-07-26.md)

[Veil](https://github.com/verse-lab/veil) (CAV 2025) is used here as a **bounded
explicit-state model checker for bug-finding and design assurance**. It is
deliberately excluded from the trust story, under hard guardrails:

1. **Veil is never the record.** Permanent proofs live in `proofs/` — Lean
   v4.32.0, standard axiom trio, no SMT in the trusted base. Veil's deliverables
   are *countermodel traces* (which become directed `uc_sim` regressions and
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

## 9. Benchmark methodology

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

Below the gates sit the **hop-isolation harnesses** — `uc_gateway/examples/hop_bench`
(client, edge and node hops with stand-ins at their boundaries) and
`uc_node/examples/apply_bench` (the FSM's apply loop alone, driven by a fake
node) — which produce ratios and ladders, never gated numbers: a dev-box
figure is smoke. Their worked examples are `docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`
and `docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md` (the latter found and
fixed lockstep's 50 µs-sleep throttle the day M14a merged). `scripts/hop1_ab.sh`
A/Bs exact binaries of hop 1 (the client) back to back and includes a
same-source rebuild control; its worked example,
`docs/benchmarks/uc2-m14c-client-hop-2026-08-28.md`, is the one that found
the M14b "−4.2 %" reading did not survive a fresh-build control.

**Alert rules are proven to fire, not just to parse.**
`scripts/m10_alert_fire.sh` builds or breaks a real cluster per rule
(`uc_node/examples/m10_alerts.rs`), scrapes each node's *real* `/metrics`
HTTP endpoint once a second, time-dilates the captured samples onto a
synthetic timeline sized to that rule's `for:` clause, and lets
`promtool test rules` adjudicate — one `PASS`/`FAIL` line per shipped rule,
with the dilation policy disclosed at runtime and every scenario labelled
`real` or `synthetic`. A rule that ships without an adjudication entry fails
the run before any cluster starts. All 16 rules are covered; the two M14c
per-FSM rules (`Uc2ServiceAbsent`, `Uc2ServicePinnedAtLagBound`) are backed
by `real` two-FSM scenarios — a declared FSM that never attaches, and one
whose apply loop is slow enough that the node's own report ceiling pins
`commit − applied` exactly at the lag bound.

---

## 10. Continuous integration

| Workflow | Contents |
|---|---|
| `ci.yml` | Fast gate on every PR: workspace build, tests, clippy `-D warnings` |
| `nightly.yml` | Full proof suite — lincheck capstones (single- and two-FSM), `sim-heavy`, loom, crashtest (single- and two-FSM), the Elle clean tier's **six** passes, `lean-proofs` conformance replay with a date-rotated seed, `fuzz` (four legs, 600 s per target, with an asserted run-count floor) and `miri` (pure decoders + `uc_remote` SPSC) |
| `elle-weekly.yml` | Elle mutation tier |

---

## 11. What is *not* verified

The most important section, and the one most projects omit.

- **`leader_completeness` is not proved** (§1). Election safety and log matching
  are; the remaining theorem is reduced to one named obligation and is open.
- **The model collapses the durable counter's two independent readers into one
  value** (§1), which makes a load-bearing lemma trivially true in the model and
  false in the real system. A real acked-write-loss bug lived in exactly that gap
  (Finding #12) — found from the Rust side, not by the proofs. Rust fixed,
  `uc_sim` closed, Lean split still open. Until it is done, proofs composed over
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
  depth, not to all executions (§8).
- **The IPC rings' interleavings and UB are covered by nothing — except MPSC
  and Broadcast** (§6, §7).
  `uc_protocol/src/ring/{spsc,mpsc,broadcast,common,futex}.rs` — the one place
  in the system where a Rust memory-safety bug is most plausible — has its
  **layout** frozen by offset-pin tests for all four ring kinds. On top of
  that, MPSC has a commit-protocol loom model and the `ring_mpsc_record` fuzz
  target (2.7.0), and Broadcast has a seqlock-barrier loom model
  (2026-08-31) which found and fixed a real weak-memory defect the moment it
  was written. **SPSC, the futex layer and the mapping itself remain
  uncovered.** Miri does not support file-backed memory mappings. A Vec-backed
  ring variant would let Miri run and would be checking a different object
  than the one that ships; it has not been built, and that trade-off is
  recorded rather than resolved.
- **Fuzzing is a regression gate, not a proof of totality** (§7). Green means no
  new crash from the committed corpus inside the budget. It does not mean the
  decoders are total for all inputs, and it says nothing about stateful
  sequences across the receiver's dispatch or about the external crates
  (`bincode`) reached through those seams.
- **The published gate numbers are fleet measurements**, on the hardware and
  configuration each record names. They are reproducible, not universal.
- **CI executes tests on x86_64 only.** aarch64 binaries are built (natively,
  on arm64 runners) but their tests never run in CI. The full correctness
  stack — workspace, both lincheck capstones, the SIGKILL crash tier — has
  passed on real ARM hardware exactly once (Graviton/Neoverse-V3, 2026-08-31,
  [`uc2-arch-sweep-c8id-vs-c9gd-2026-08-31`](benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md)),
  which also surfaced and fixed one x86-timing assumption in a test's
  scenario construction. A one-time pass is a data point, not a regression
  gate; ARM stays uncovered between such runs.
- **Multi-service (M14): the two-FSM proof gap `2.8.0` disclosed here is
  closed by `2.8.1` (M14c2, spec §15.1, §16).** The record, so it can be
  audited rather than taken on trust:
  - **Seven two-FSM capstones** — `two_fsm_bounded`, `two_fsm_lockstep`,
    `two_fsm_slow`, `two_fsm_slow_lockstep` (`uc_node/tests/lin_v2.rs`),
    `minority_partition_and_heal_two_fsm`
    (`uc_node/tests/lin_partition_v2.rs`), and `two_fsm_service_sigkill` /
    `two_fsm_node_sigkill` (`examples/uc_crashtest/tests/hard_crash.rs`).
    Each checks **one WGL history per FSM** with the untouched `uc_lincheck`
    checker (§3, §5).
  - **The replication-equivalence oracle** — every `submit_all`'s per-FSM
    answers must be byte-equal — is asserted at zero in all seven, and is
    **shown to bite**: `two_fsm_oracle_bites` runs FSM 1 as `Corrupt<RegisterSm>`
    through the capstones' own worker loop and dies on the same
    `equiv_failures == 0` assertion the capstones make.
  - **The slow-FSM oracle** (`two_fsm_slow`, `two_fsm_slow_lockstep`, FSM 1 =
    `Slow<RegisterSm, 200>`): the FSMs' separation stays inside the policy at
    every 50 ms sample **and** their second-half apply rates agree within
    10 % — measured ratio 1.000. Scoped by measurement: the separation never
    approached the bound (`max_lag` 192 B of 65536, and 64 B of 288), so this
    is evidence of equal progress, not of the barrier at the bound (§3).
  - **The Elle clean tier runs with two FSMs** — `elle_quiet_two_fsm`
    (`uc_node/tests/elle_v2.rs`), one history per FSM, both clean under
    `serializable` and `strong-serializable`. Its equivalence check can only
    fail on a malformed fan-in (`LaResp` has one variant); the equivalence
    evidence is the WGL capstones' (§4).
  - **The M14c deferrals are closed** in `uc_net` (snapshot-session refusal
    tests, the NAK-before-BEGIN skip, three new counters, a 60 s intake
    timeout, a paced publish re-drive) and in `uc_service`/`uc_node`/`uc2ctl`
    (`lag_waits` now counts bounded mid-frame stalls, the learner join pins the
    installed artifact positions, the pinned-at-bound alert threshold, the
    snapshot-decline latch test).
  - **The lockstep 60× the M14 gate reported (row e) was settled by
    experiment, not argument**, against a decision rule fixed before any
    measurement: it reproduces at 880× on a deliberately oversubscribed rung
    (624 k → 709 frames/s at 3 threads on 1 CPU) while bounded mode on the same
    rung is unaffected at 7.4 M frames/s; the pre-registered sleep-cascade
    mechanism was **refuted** (`lag_waits = 0` on every collapsed run); no
    candidate fix cleared the pre-committed 50 % recovery bar (yield ladder ×4
    and ×16 and an unbounded yield all 1.00×; a futex wait on the sibling's
    `applied` word 116× but still only 13 % of the unconstrained rate). The
    verdict is therefore an **operating-envelope fact stated with the number,
    not a product defect**, and no behavioural code changed →
    [`uc2-m14c2-lockstep-oversubscription-2026-08-30.md`](/docs/benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md),
    [Limits](/docs/reference/limits.md).
  - **The `--pin` rig has now run (2026-08-31) and pinning was NOT adopted.**
    16 arms on 4 × `c6id.2xlarge`, one binary against itself, pinned vs
    unpinned. The pre-committed bar (spec §16.5) was "adopt iff the pinned
    spread is < 5 %"; the pinned pooled spread is **14.3 %**, so the bar is
    not met and `--pin` stays opt-in. Pinning DOES remove the worst mode
    (pooled spread 47.7 % → 14.3 %, and the 1.12 M collapse arm never
    appears pinned), so placement is *a* cause — but it is not the only one,
    and pinning also **costs 9.4 % of mean throughput**, with the pinned
    maximum below the unpinned mean. The `c6id.2xlarge` sibling map's
    assumption is no longer an assumption: `lscpu -e=CPU,CORE` on all three
    voters gives `CORE: 0 1 2 3 0 1 2 3`, exactly
    `EXPECTED_SIBLING_PAIRS`. →
    [`uc2-m14c2-fleet-pinning-2026-08-30.md`](/docs/benchmarks/uc2-m14c2-fleet-pinning-2026-08-30.md)
  - **Still open:** the M14 fleet gate measures rates, a kill and a join — it
    is not a correctness substitute, and **row e has still not been
    re-measured**. The pinning run above did not address it, and the residual
    14.3 % spread is undiagnosed.
- **Wire crypto is opt-in and off by default.** With it disabled the posture is a
  trusted network. With it enabled, the threat model is a network-path adversary;
  a compromised host and a malicious cluster member are explicitly **out of
  model** — the group key is symmetric, so any holder can forge fan-out traffic
  as any node. See the M8 gate record and
  [`docs/security/threat-model.md`](/docs/security/threat-model.md).

---

## Reproducing everything

```bash
# Proofs — from proofs/
lake build                                                  # 3027 jobs, zero sorry
lake exe conform --seed 20260716 --count 100000             # model vs. real Rust

# Simulation
cargo test -p uc_sim --features sim-heavy                  # 1000-seed fuzz

# Linearizability + the rest of the suite
cargo test --workspace

# Transactional safety
scripts/elle_check.sh                                       # clean tier
scripts/elle_mutation.sh                                    # the harness's teeth

# Alert rules — every shipped rule fired against a real cluster (needs promtool)
scripts/m10_alert_fire.sh

# Hard crash
cargo test -p uc_crashtest --features hard-crash-tests

# Memory model
RUSTFLAGS="--cfg loom" cargo test -p uc_log --test loom_frame --release
RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_mpsc --release

# Fuzzing — needs nightly + cargo-fuzz; CI uses 600 with --min-runs 10000
scripts/fuzz_smoke.sh 30                                    # every target
scripts/fuzz_smoke.sh --min-runs 10000 600 uc_node_toml    # one, at CI budget

# Miri — the pure decoders (needs the miri component on nightly)
cargo +nightly miri test -p uc_protocol --lib -- v2:: version::
cargo +nightly miri test -p uc_journal --lib -- \
  stable_value::tests:: error::tests:: journal::segment::tests::header_ \
  journal::segment::tests::record_ journal::segment::tests::decode_
```

Elle histories must not be written to `/tmp` on a RAM-backed box; both scripts
default to `$HOME/.cache`.
