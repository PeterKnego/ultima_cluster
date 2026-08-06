# ultima_cluster

An **Aeron-shaped, Rust-native State Machine Replication (SMR) application
server**: Raft-style consensus safety over a shared-memory log buffer, with the
user's deterministic business logic running in a separate process at
memory-channel speed.

### 1.64 M responses/s at p99 0.771 ms

End to end through the SDK — client submit, consensus, apply, response — with
**every operation quorum-fsync'd before it is acked** and reads linearizable.
p50 0.600 ms, p90 0.682 ms; the tail sits 0.17 ms above the median. Measured on
3 × `c6id.2xlarge`, 64 B payloads.
→ [M5 gate record](/docs/benchmarks/uc2-m5-gate-2026-07-12.md)

### Leader failover p50 202 ms, zero committed loss in 10 of 10 kills

p90 279 ms, worst 394 ms. Measured on a **4-vCPU sandbox over loopback, not the
fleet** — failover here is timeout-dominated and real NVMe fsync is faster than
the sandbox's ext4, so this is a conservative upper bound; the fleet
detection-timing confirmation is still outstanding.
→ [M4 gate record](/docs/benchmarks/uc2-m4-gate-2026-07-11.md)

---

**Status: v2.1.0** — milestones M1–M7 complete. M8 (opt-in wire crypto) is
merged and unreleased.

| Gate | Result | Measured on |
|---|---|---|
| End-to-end SDK round trip (M5) | **1.64 M responses/s** @ p50 0.600 / p99 0.771 ms (4.1× the ≥400 k bar) | 3-host fleet |
| Commit pipeline ceiling (M3) | 2.88 M commits/s @ p50 0.946 / p99 1.132 ms | 3-host fleet |
| Leader failover (M4) | p50 202 ms, p90 279 ms, 10/10 zero committed loss | 4-vCPU sandbox, loopback |
| Learner join under load (M6) | commit-rate dip **0.9 %** (gate < 10 %) | 4-host fleet |
| Below-floor snapshot reconstruction (M6) | worst **2.80 s** across 5 purge cycles, zero read divergence | 4-host fleet |
| Live single-server reconfiguration (M7) | per-transition dip **0.0–4.7 %**, leader self-removal handoff 3.22 s | 5-host fleet |
| Opt-in wire crypto (M8) | **94.1 %** of cleartext throughput | 4-vCPU dev box; ratio only |

Fleet runs are `c6id.2xlarge`, us-east-1, single AZ, cluster placement group,
NVMe journals, fsync on. The two non-fleet rows say so rather than borrowing the
fleet's credibility — M4's fleet confirmation and M8's fleet ratio are both open
work.

**Every gate record commits its pass/fail rule to this repository before the
run.** The decide rule and the result are separate commits, in that order; git
history is the audit trail. Records that failed their bar say so and keep the
bar.

## Try it

```bash
cargo run -p counter --bin counter-single
```

A replicated counter — node, service, and client in one process. For a real
three-node cluster, a leader kill, and a follower read that proves replication
happened, see **[`docs/QUICKSTART.md`](/docs/QUICKSTART.md)**.

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

- **[API documentation](https://peterknego.github.io/ultima_cluster/)** — rustdoc
  for every library crate, rebuilt on each push to `main`.
- **[`docs/QUICKSTART.md`](/docs/QUICKSTART.md)** — zero to a running three-node
  cluster, with the state machine you write shown in full. Every command and
  output is from a real run.
- **[`docs/ARCHITECTURE.md`](/docs/ARCHITECTURE.md)** — how it works: positions
  instead of indices, the four agents, the data and control planes, the apply
  path. Written for someone who knows Raft and has not read the specs.
- **[`docs/VERIFICATION.md`](/docs/VERIFICATION.md)** — what is proved, what is
  checked, what is only bug-hunted, and how to reproduce each.
- **[`docs/BENCHMARKS.md`](/docs/BENCHMARKS.md)** — every measured result, what
  it was measured on, and the read-barrier arc.
- **[`docs/how-to/`](/docs/how-to)** — task guides for running a cluster:
  getting nodes onto real hosts, changing membership live, encrypting node
  traffic, bounding journal growth, diagnosing a node, investigating a red
  correctness run, reproducing a published result.
- **[`docs/reference/`](/docs/reference)** — the surfaces those tasks drive:
  `uc2ctl`, the instance directory, the cnc control page, configuration and
  environment switches, the wire protocol, the linearizable read path. The
  library API is the rustdoc above, not here.
- **[`docs/ops/uc2-runbook.md`](/docs/ops/uc2-runbook.md)** — the operations
  landing page; indexes the two directories above.

Reference:

- **Design specs (canonical):** [`docs/superpowers/specs/`](/docs/superpowers/specs) — [the v2 core design](/docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md) (M1–M6), [reconfiguration](/docs/superpowers/specs/2026-07-13-uc2-reconfig-design.md) (M7), [wire crypto](/docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md) (M8)
- **Raw gate records:** [`docs/benchmarks/`](/docs/benchmarks) — the dated
  originals behind [`BENCHMARKS.md`](/docs/BENCHMARKS.md), including the
  correctness gates (elle, lean, veil)
- **v1 history:** [`docs/tasks/`](/docs/tasks) — the retired openraft-based v1 stack's consolidated record (kept as the negative-results archive that shaped v2)

Bench/fleet tooling lives under [`bench-infra/`](/bench-infra) (terraform +
ansible + the fleet-gate orchestrator; refuses to run journal-bearing gates on
RAM-backed filesystems).

## Scope (v2.1)

**Dynamic membership (M7)**: single-server reconfiguration is shipped —
promote / demote / add / remove one member at a time, live, under load, via
the `uc2ctl` admin CLI. Joint consensus is not needed for the supported ops
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
