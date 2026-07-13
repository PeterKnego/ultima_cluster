# ultima_cluster

An **Aeron-shaped, Rust-native State Machine Replication (SMR) application
server**: Raft-style consensus safety over a shared-memory log buffer, with the
user's deterministic business logic running in a separate process at
memory-channel speed.

**Status: v2.0** — milestones M1–M6 complete, every gate passed on real
hardware. Measured on a 3×`c6id.2xlarge` AWS fleet (fsync on, linearizable):

| Gate | Result |
|---|---|
| End-to-end SDK round trip (M5) | **1.64 M responses/s @ p50 0.600 ms** (4.1× the ≥400 k bar) |
| Commit pipeline ceiling (M3) | 2.88 M commits/s @ p50 0.946 ms |
| Leader failover (M4) | p50 202 ms, 10/10 zero committed loss |
| Learner join under load (M6, 4-host) | commit-rate dip **0.9 %** (gate < 10 %) |
| Below-floor snapshot reconstruction (M6) | worst **2.80 s** across 5 purge cycles, zero read divergence |

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
  [`ultima_journal`](ultima_journal/) in ≤1 MiB CRC'd blocks, fsync per block.
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

Three independent layers, all in CI:

1. **Deterministic simulation** (`uc2_sim`) — virtual-time cluster with safety
   invariants (no split-brain, no phantom commit, no divergent adoption) under
   seeded fault fuzz; the election/truncation state machine is pure-sync and
   fully sim-driven.
2. **WGL linearizability capstones** (`uc-lincheck` + `uc2_node/tests`) — a
   concurrent CAS-register history checked for linearizability while the
   harness kills leaders, crashes services, partitions the network, and (the
   M6 tier) runs snapshot-backed purge underneath.
3. **Multi-process hard-crash** (`examples/uc2-crashtest`) — real node +
   service processes SIGKILLed mid-load, recovery required to stay linearizable.

Plus a `loom` model of the frame-visibility protocol and offset-pin tests that
freeze the wire and cnc layouts.

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

- **Design spec (canonical):** [`docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md`](docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md)
- **Milestone gate records:** [`docs/benchmarks/`](docs/benchmarks/) (`uc2-m1` … `uc2-m6`)
- **Operations runbook:** [`docs/ops/uc2-runbook.md`](docs/ops/uc2-runbook.md) — instance-dir layout, durability requirements, cnc decoding, purge enablement, learner add/remove
- **v1 history:** [`docs/tasks/`](docs/tasks/) — the retired openraft-based v1 stack's consolidated record (kept as the negative-results archive that shaped v2)

Bench/fleet tooling lives under [`bench-infra/`](bench-infra/) (terraform +
ansible + the fleet-gate orchestrator; refuses to run journal-bearing gates on
RAM-backed filesystems).

## Scope (v2.0)

Static voting set (learners supported; joint-consensus reconfiguration is
v2.x). Trusted-network posture — no wire encryption (a PSK-MAC slot is
reserved in the datagram header). One node per instance directory, up to 8
peers in the cnc observability band.

## License

Apache-2.0
