# ultima_cluster

An **Aeron-shaped, Rust-native State Machine Replication (SMR) application
server**: Raft-style consensus safety over a shared-memory log buffer, with the
user's deterministic business logic running in a separate process at
memory-channel speed.

**Status: v2.0**, milestones M1–M6 complete. Every gate below passed on a
3×`c6id.2xlarge` AWS fleet with fsync on and linearizable reads:

| Gate | Result |
|---|---|
| End-to-end SDK round trip (M5) | **1.64 M responses/s @ p50 0.600 ms** (4.1× the ≥400 k bar) |
| Commit pipeline ceiling (M3) | 2.88 M commits/s @ p50 0.946 ms |
| Leader failover (M4) | p50 202 ms, 10/10 zero committed loss |
| Learner join under load (M6, 4-host) | commit-rate dip **0.9 %** (gate < 10 %) |
| Below-floor snapshot reconstruction (M6) | worst **2.80 s** across 5 purge cycles, zero read divergence |

Every gate record commits its pass/fail rule to the repository *before* the run.

**Since v2.0:**

- **M7 — live single-server reconfiguration.** Implemented and green on the full
  local proof stack (sim invariants, WGL capstones including a reconfig-churn arm,
  crashtest, multi-process orchestrator run) —
  [gate record](/docs/benchmarks/uc2-m7-gate-2026-07-13.md). The 5-host fleet run is
  a separate step; `v2.1.0` is tagged only once it lands.
- **M8 — opt-in wire crypto.** Authenticated and encrypted node↔node UDP, off by
  default; **PASS** at 94.1% of cleartext throughput —
  [gate record](/docs/benchmarks/uc2-m8-gate-2026-07-29.md). Wire protocol 0.4.0,
  unreleased.

## Shape

```
[client process]  ──shmem──▶  [uc2_node]  ◀──reliable UDP──▶  [uc2_node on peer host]
                                  ▲
                                  │ shmem (file-backed log buffer + cnc page)
                                  ▼
                             [uc2_service]   ← your StateMachine lives here
```

Each node is **four single-writer polling agents** — consensus, sender,
receiver, archive — coordinated only through atomic counters in a 4 KiB
`cnc.dat` page and monotonic byte *positions* (the absolute-offset analog of a
Raft log index). No locks on the hot path; no async in the consensus core.

- **Replication is a byte-stream fan-out** of the log buffer itself over UC's
  own reliable UDP (NAK repair served from the buffer, quorum-paced flow
  control). Consensus is a control plane: coalesced durable-position gossip.
- **Durability** is the archive agent recording the buffer into
  [`ultima_journal`](/ultima_journal) in ≤1 MiB CRC'd blocks, fsync per block.
- **Your code** implements a sync, deterministic `StateMachine`
  (`apply(position, cmd)`, `query`) — optionally `SnapshotStateMachine`
  (enables journal purge) and an async leader-only `OutputHandler`. The apply
  agent polls committed bytes in place; responses reach clients through a
  position-keyed egress broadcast that bypasses the node.
- **Snapshots make purge safe** (off by default): a node below the purge floor
  — crashed service, fresh learner, cold start — converges by snapshot install
  + tail replay, never by reading a purged prefix.
- **Linearizable reads** run a quorum read-barrier plus a service-epoch check
  that closes the TOCTOU against a service crashing mid-query.

## Correctness story

Machine-checked proofs, checked properties, and bug-hunting — kept clearly
separated, because those words mean very different things.

- **Proved (Lean 4, sorry-free, standard axioms only):** the consensus safety
  kernels; `election_safety` and `log_matching` over an N-node protocol model. A
  conformance rig replays 100,000+ vectors of *real Rust output* through the Lean
  model and diffs it bit for bit. `leader_completeness` is reduced to one named
  obligation and remains **open**.
- **Checked:** nine whole-cluster invariants under seeded fault fuzz (`uc2_sim`);
  WGL linearizability under leader kills, partitions, and purge; Elle
  transactional safety under both the serializable and strict real-time models,
  with a mutation tier proving the harness can actually fail; multi-process
  `SIGKILL` recovery; a `loom` model of frame visibility.
- **Bug-hunted only:** Veil bounded model checking — deliberately excluded from
  the trust story and never the proof of record.

The proof effort has found and fixed **four real, shipped safety bugs** that the
fuzz and crash tiers had missed, two of them acked-write-loss class.

**→ [`docs/VERIFICATION.md`](/docs/VERIFICATION.md)** for the full picture,
including what is *not* verified and how to reproduce every layer.

## Workspace

| Crate | Role |
|---|---|
| `uc_protocol` | Wire spec: cnc page layout, datagram/frame formats, lock-free rings (SPSC/MPSC/broadcast) |
| `uc2_log` | Shared log buffer + archive agent (journal recording, snapshots, purge floor) |
| `uc2_net` | Reliable-UDP sender/receiver agents, NAK repair, flow control, snapshot sessions |
| `uc2_consensus` | Pure-sync safety core: commit tracker, elections, term maps, truncation |
| `uc2_sim` | Deterministic simulation + invariants + fuzz |
| `uc2_node` | The node: agents wired together, IPC surface, read barrier, gate harnesses |
| `uc2_service` | Service SDK: `StateMachine` traits, apply agent, reconstruction; optional [`ultima-db`](https://crates.io/crates/ultima-db) store adapter (feature `ultima_db`) |
| `uc2_client` | Sync client SDK: submit, linearizable/snapshot queries, response matcher |
| `uc-lincheck` | WGL linearizability checker + history recorder + register model |
| `ultima_journal` | Segmented append journal + atomic `StableValue`s |

Builds standalone — the only external storage dep, `ultima-db`, comes from
crates.io.

## Build & test

```bash
cargo build --workspace
cargo test --workspace                                    # includes the lincheck capstones
cargo clippy --workspace --all-targets -- -D warnings     # the lint gate
cargo test -p uc2_sim --features sim-heavy                # 1000-seed fuzz tier
cargo test -p uc2-crashtest --features hard-crash-tests   # multi-process SIGKILL
RUSTFLAGS="--cfg loom" cargo test -p uc2_log --test loom_frame --release
```

`ci.yml` runs the fast gate on every PR; `nightly.yml` runs the full proof
suite (capstones, sim-heavy, loom, crashtest).

## Documentation

Start here:

- **[`docs/ARCHITECTURE.md`](/docs/ARCHITECTURE.md)** — how it works: positions
  instead of indices, the four agents, the data and control planes, the apply
  path. Written for someone who knows Raft and has not read the specs.
- **[`docs/VERIFICATION.md`](/docs/VERIFICATION.md)** — what is proved, what is
  checked, what is only bug-hunted, and how to reproduce each.
- **[`docs/ops/uc2-runbook.md`](/docs/ops/uc2-runbook.md)** — operations:
  instance-dir layout, durability requirements, cnc decoding, purge enablement,
  live reconfiguration (`uc2ctl`), wire crypto setup.

Reference:

- **Design specs (canonical):** [`docs/superpowers/specs/`](/docs/superpowers/specs) — [the v2 core design](/docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md) (M1–M6), [reconfiguration](/docs/superpowers/specs/2026-07-13-uc2-reconfig-design.md) (M7), [wire crypto](/docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md) (M8)
- **Milestone gate records:** [`docs/benchmarks/`](/docs/benchmarks) (`uc2-m1` … `uc2-m8`)
- **v1 history:** [`docs/tasks/`](/docs/tasks) — the retired openraft-based v1 stack's consolidated record (kept as the negative-results archive that shaped v2)

Bench/fleet tooling lives under [`bench-infra/`](/bench-infra) (terraform +
ansible + the fleet-gate orchestrator; refuses to run journal-bearing gates on
RAM-backed filesystems).

## Scope (v2.0 + M7)

**Dynamic membership (M7)**: single-server reconfiguration is shipped —
promote / demote / add / remove one member at a time, live, under load, via
the `uc2ctl` admin tool. Joint consensus is not needed for the supported ops
(adjacent configs differ by one member, so majorities always intersect). Hard
cap: **8 total members** (voters + learners) in the cnc observability band —
unchanged from v2.0. One node per instance directory.

**Wire security (M8)**: authenticated + encrypted node↔node UDP is available and
**off by default** — X25519 identities on a runtime-reloadable allowlist, Noise
`IK` handshake, a rotating cluster group key for the byte-identical fan-out,
24 B/datagram overhead. A cluster runs either all-encrypted or all-cleartext;
there is no mixed mode. With crypto disabled the posture is a trusted private
network. The threat model is a network-path adversary; a compromised host and a
malicious cluster member are explicitly out of model (see
[`docs/VERIFICATION.md`](/docs/VERIFICATION.md) §10 and runbook §11).

## License

Apache-2.0
