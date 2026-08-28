# Verification

What is **proved**, what is **checked**, what is only **bug-hunted** — and how to
reproduce each one yourself.

This document exists because "verified" is a word that hides an enormous range.
A machine-checked theorem and a passing fuzz run are both often called
verification, and they carry very different weight. Everything below is sorted by
the strength of the evidence, and the boundaries between tiers are stated
explicitly rather than blurred.

**Status of this document:** current as of the M12d security-posture pass
(2026-08-23), which added §7; the proof, simulation and capstone tiers are as of
the M8 gate (2026-07-29). Each section cites the dated record it summarizes;
where the two disagree, the dated record wins.

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
| **loom** | Exhaustive over interleavings | The frame-visibility memory protocol **and the MPSC ring's per-record commit protocol** |
| **Fuzzing (libFuzzer)** | Checked under coverage-guided input search | Totality of the fifteen decoders that see bytes the process did not write |
| **Miri** | Checked under a symbolic interpreter | Undefined behaviour in the pure wire/journal decoders and `uc2_remote`'s Vec-backed SPSC internals (**not** the file-backed rings) |
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
- **No SMT.** Nothing in `proofs/` calls a solver; see §8 on Veil.

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
RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_mpsc --release
```

The second model is the MPSC ring's per-record commit protocol (M13a):
disjoint concurrent claims, the commit word's Release/Acquire visibility
pair, and head-of-line behaviour at a claimed slot. Both models are of
protocols, not of mappings — loom cannot see an mmap, so the ring's
*layout* stays frozen by offset-pin tests.

Offset-pin tests additionally freeze the wire and `cnc` page layouts, so a
layout change cannot pass silently.

---

## 7. Fuzzing — decoders total on untrusted bytes

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
| `uc2_remote_frame` | `uc2_remote::frame` — the gateway edge's 24-byte TCP frame header and every typed body decoder. Input from any client that can open a socket to the gateway. |
| `uc2_crypto_open` | `uc2_crypto::seal::{open_in_place, open_detached}` — the AEAD envelope's framing arithmetic, which runs on attacker-chosen bytes *before* the tag has been verified. |
| `uc2_crypto_handshake` | `uc2_crypto::handshake::Peers::on_message` — the pre-auth Noise `IK` surface. With crypto enabled this is the first thing in the process to see bytes from anyone who can reach the UDP port. |
| `uc2_crypto_group_key` | `uc2_crypto::group::GroupPlane::on_key_message` — the two distinct message shapes that share datagram kind 20, i.e. a decoder that must disambiguate hostile input. |
| `uc2_crypto_admin` | `uc2_crypto::admin` — a **property** target over the M12b signed-tag layout: canonical-length agreement, sign/verify round-trip, tag bit-flips rejected, foreign key rejected. |
| `ultima_journal_record` | `ultima_journal`'s segment header and record decoder — what crash recovery meets in a torn or corrupt segment after a power loss or a full disk. |
| `ultima_journal_stable_value` | `ultima_journal::stable_value` — the durable vote / term map / snapshot floor slots. Corruption here is a consensus-safety input, not merely a data-loss one. |
| `uc2_service_session` | `Sessioned<S>` — the exactly-once envelope under a fuzz-derived, deliberately tiny `SessionConfig` so client eviction, byte eviction and window trim are all reachable, plus its snapshot install path. |
| `uc2_node_toml` | `uc2_node::config_file::parse_str` — the `node.toml` parser behind every M9/M11/M12b named startup refusal. |
| `uc2_gateway_toml` | `uc2_gateway::config_file::parse_str` — the gateway's whole named-refusal path, including its own `EdgeConfig::validate`. |
| `uc2_node_http` | `uc2_node::obs::http::route_raw` — the **unauthenticated** `/metrics` + `/healthz` + `/readyz` request parser. |

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
- **`bincode` itself, and the `ultima-db` `snapshot_stream` adapter.** Both are
  external crates reached through these seams; they are dependency-posture
  questions (`deny.toml`, SBOM), not targets here.

### Miri — UB detection over the pure decoders

The same decoders are run under [Miri](https://github.com/rust-lang/miri) in
nightly CI (`miri` job): `uc_protocol`'s `v2::` wire/cnc/ipc layer and `version`
packing (43 tests), and `ultima_journal`'s segment and `stable_value` record
decoders (19 tests), and `uc2_remote`'s `outgoing`/`completion`/`slots` SPSC
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
model is of the **log buffer's** frame-visibility protocol, a second is of
the **MPSC ring's** per-record commit protocol (M13a) — but neither is of
`uc_protocol::ring` as a whole. So the rings' layout is frozen by
offset-pin tests, and their interleavings and UB are covered by **nothing**
— except the MPSC ring, whose commit protocol has had a loom model and
whose slot decoder has had a fuzz target (`ring_mpsc_record`) since 2.7.0.
SPSC, Broadcast, the futex layer and the mapping itself remain uncovered —
stated again in §11.

---

## 8. Veil — bug-hunting only, never the record

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

Below the gates sit the **hop-isolation harnesses** — `uc2_gateway/examples/hop_bench`
(client, edge and node hops with stand-ins at their boundaries) and
`uc2_node/examples/apply_bench` (the FSM's apply loop alone, driven by a fake
node) — which produce ratios and ladders, never gated numbers: a dev-box
figure is smoke. Their worked examples are `docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`
and `docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md` (the latter found and
fixed lockstep's 50 µs-sleep throttle the day M14a merged).

---

## 10. Continuous integration

| Workflow | Contents |
|---|---|
| `ci.yml` | Fast gate on every PR: workspace build, tests, clippy `-D warnings` |
| `nightly.yml` | Full proof suite — lincheck capstones, `sim-heavy`, loom, crashtest, Elle clean tier, `lean-proofs` conformance replay with a date-rotated seed, `fuzz` (four legs, 600 s per target, with an asserted run-count floor) and `miri` (pure decoders + `uc2_remote` SPSC) |
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
  depth, not to all executions (§8).
- **The IPC rings' interleavings and UB are covered by nothing — except the
  MPSC ring, whose commit protocol has had a loom model and whose slot
  decoder has had a fuzz target (`ring_mpsc_record`) since 2.7.0** (§6, §7).
  `uc_protocol/src/ring/{spsc,mpsc,broadcast,common,futex}.rs` — the one place
  in the system where a Rust memory-safety bug is most plausible — has its
  **layout** frozen by offset-pin tests for all four ring kinds, plus the two
  MPSC-specific checks above. SPSC, Broadcast, the futex layer and the
  mapping itself remain uncovered. Miri does not support file-backed memory
  mappings, and §6's loom models are of the **log buffer's** frame-visibility
  protocol (a hand-written model of that atomic handshake) and, separately,
  of the **MPSC ring's** claim-then-commit sequence — the broadcast seqlock
  has never been model-checked. A Vec-backed ring variant would let Miri run
  and would be checking a different object than the one that ships; it has
  not been built, and that trade-off is recorded rather than resolved.
- **Fuzzing is a regression gate, not a proof of totality** (§7). Green means no
  new crash from the committed corpus inside the budget. It does not mean the
  decoders are total for all inputs, and it says nothing about stateful
  sequences across the receiver's dispatch or about the external crates
  (`bincode`, the `ultima-db` snapshot adapter) reached through those seams.
- **The published gate numbers are fleet measurements**, on the hardware and
  configuration each record names. They are reproducible, not universal.
- **M14a's lag barrier and quorum-gated report ceiling are unit-tested and
  integration-tested on one node and a 3-node in-process cluster.** M14b's sim
  scenario covers the ceiling's liveness property; the real node's ceiling is
  exercised by `uc2_node/tests/services.rs` on a 3-node in-process cluster.
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
RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --test loom_mpsc --release

# Fuzzing — needs nightly + cargo-fuzz; CI uses 600 with --min-runs 10000
scripts/fuzz_smoke.sh 30                                    # every target
scripts/fuzz_smoke.sh --min-runs 10000 600 uc2_node_toml    # one, at CI budget

# Miri — the pure decoders (needs the miri component on nightly)
cargo +nightly miri test -p uc_protocol --lib -- v2:: version::
cargo +nightly miri test -p ultima-journal --lib -- \
  stable_value::tests:: error::tests:: journal::segment::tests::header_ \
  journal::segment::tests::record_ journal::segment::tests::decode_
```

Elle histories must not be written to `/tmp` on a RAM-backed box; both scripts
default to `$HOME/.cache`.
