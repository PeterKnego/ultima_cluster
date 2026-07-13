# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

`ultima_cluster` (UC) is an **Aeron-shaped, Rust-native State Machine Replication
application server**. This is **UC v2**: milestones M1–M6 are complete and on
`main` (log+archive, replication, static-leader commit pipeline, elections, the
end-to-end SDK, and snapshots/learners/purge). The M5 throughput gate and M6
fleet gate both passed on real AWS fleets.

**The v1 stack (an `openraft`-based design) has been retired** and its crates
deleted — v2 owns consensus, elections, and transport directly. Do not
reintroduce `openraft`, `quinn`/QUIC, or the `uc_node`/`uc_service`/`uc_client`
crate names; those are gone.

Canonical documents, in order:

1. `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md` — the
   canonical v2 design spec. Read it end-to-end before substantial work.
2. `docs/benchmarks/uc2-m{1..6}-gate-*.md` — the per-milestone gate docs (the
   permanent record for v2; there is no `docs/tasks/` consolidation for v2 — the
   `taskNN` docs under `docs/tasks/` are v1-era history).
3. `docs/ops/uc2-runbook.md` — operational runbook (instance-dir layout, cnc
   decode, purge enablement, learner add/remove).
4. Storage primitives: `../ultima_db/docs/tasks/task26_journal.md`
   (`ultima_journal` log primitives) and `task27_snapshot_stream.md` (the
   `ultima_db` snapshot wire format).

## Build & Test Commands

```bash
cargo build --workspace                          # build all workspace crates
cargo test                                       # in-process integration + sim tests (default)
cargo test -p uc2_node --test lin_v2             # WGL linearizability capstone (failover + purge/snapshot churn)
cargo test -p uc2_node --test lin_partition_v2   # network-partition / quorum-loss linearizability
cargo test -p uc2-crashtest --features hard-crash-tests   # spawn real node+service procs; SIGKILL mid-load, assert linearizable
cargo clippy --workspace -- -D warnings          # lint (must pass with zero warnings)
cargo run -p uc2_node --release --example m5_gate # throughput gate harness (see the gate doc)
cargo run -p uc2_node --release --example m6_gate -- all --secs 6 --cycles 5   # snapshots/learners/purge gate
```

Cross-host fleet gates run via `bench-infra/` (terraform + ansible provisioning,
`bench-infra/scripts/m6_fleet_gate.py` as the orchestrator).

Workspace crates:

- `uc_protocol` — wire spec; `core`-friendly data types (`version`, `magic`,
  `error_codes`) plus the lock-free ring buffers (`ring`: SPSC/MPSC/Broadcast)
  and the v2 wire spec (`v2`): the `cnc.dat` 4 KiB page layout, the self-locating
  UDP datagram header, and per-message frame layouts. Multi-language gate.
- `uc2_log` — the log buffer + archive. File-backed shared log buffer (readers
  poll positions in place, bounded by the commit counter) and the archive agent
  that records ≤1 MiB blocks into `ultima_journal` (the retransmit + recovery
  store). Owns snapshot builder + below-floor reconstruction primitives.
- `uc2_net` — own reliable-UDP transport (no QUIC): sender/receiver polling
  agents, NAK-based retransmit off the log buffer, quorum-paced flow control,
  snapshot sessions. A seeded fault layer drives the sim.
- `uc2_consensus` — pure-sync Raft-safety core over **byte positions**:
  `CommitTracker` (quorum-th highest committed position), `ElectionSm`
  (lexicographic `(last_term, last_durable)` vote, data-stamped term map,
  truncation). No async, no I/O — driven deterministically by the sim.
- `uc2_sim` — virtual-time deterministic world + safety invariants + seeded
  fuzz. The gate that proves consensus safety without hardware.
- `uc2_node` — the node binary + library. Wires the **four single-writer polling
  agents** (consensus / sender / receiver / archive), the `cnc.dat` page, the
  ingress ring, and the linearizable-read barrier. Owns elections and truncation.
- `uc2_service` — service-side SDK. User implements `StateMachine` (sync `apply`,
  sync `query`) + optionally `SnapshotStateMachine` (M6 purge) + `OutputHandler`
  (async, leader-only). The apply agent polls committed positions in the log
  buffer; reconstruction replays the journal or installs a snapshot + tail-replays.
- `uc2_client` — sync local-shmem input-client SDK. Small dep set (no transport,
  no consensus); matcher over the broadcast response ring.
- `uc-lincheck` — test/verification library: WGL linearizability `checker`, op
  `history` recorder, `model`, and the in-memory CAS-`register` SM
  (`Cmd`/`CmdResp`/`RegisterSm: uc2_service::StateMachine`). One source of truth
  shared by the in-process lincheck capstone (`uc2_node/tests/lin_v2.rs`) and the
  multi-process hard-crash test.
- `examples/uc2-crashtest` — multi-process test harness: reference bins (node +
  service halves over a shared instance_dir) + the hard-crash tests behind the
  `hard-crash-tests` feature. The real `kill -9` path for reconstruction validation.
- `ultima_journal` — segmented append journal + `StableValue`. In-tree workspace
  member (moved in from `ultima_db`; full history preserved).

## Architecture overview

UC is a State Machine Replication application server. Three process roles;
same-host inter-process traffic via shared memory, cross-host traffic via UC's
own reliable-UDP transport between nodes:

```
[client process]      ──shmem──▶  [uc2_node]  ◀──reliable-UDP──▶  [uc2_node on peer host]
                                      ▲
                                      │ shmem (file-backed log buffer + cnc page)
                                      ▼
                                 [uc2_service]
```

Each node is **four single-writer polling agents**, counter-coordinated (no
locks on the hot path): **consensus** (commit tracking + elections), **sender**
and **receiver** (reliable-UDP replication + NAK repair), and **archive** (record
the log buffer into `ultima_journal` in ≤1 MiB blocks). All coordination is
through atomic counters in the `cnc.dat` page and monotonic byte **positions**
(the absolute-offset analog of a Raft log index); `apply` is keyed on position.

- `uc2_node` owns consensus, log durability, snapshot transport, leader election.
- `uc2_service` owns the user's deterministic business logic (`apply`, `query`)
  and side-effecting `on_committed` (leader-only, at-least-once).
- Client processes translate external requests into Commands and submit via shmem.

The shmem layer is a fixed-layout `cnc.dat` 4 KiB control page (`uc_protocol::v2::cnc`,
offsets pinned in both `uc_protocol` and `uc2_log` so they never drift) plus the
file-backed log buffer and per-stream ring buffers under an instance directory.
Ring buffers are lock-free; SPSC for service↔node, MPSC for clients→node,
Broadcast for node→clients (position-keyed responses bypass the node via an
egress broadcast).

Storage primitives:
- Log buffer: `uc2_log` file-backed ring; the appender never overwrites bytes not
  yet recorded (one hard overrun gate); all other readers degrade to journal replay.
- Archive / recovery: `ultima_journal::Journal` (segmented append, group commit,
  CRC per block; block seq = block index, meta = base position).
- Durable state: `ultima_journal::StableValue<T>` (rotating two-slot atomic value)
  for vote, term map, commit, snapshot floor, output progress.
- App state + snapshots: the user's `StateMachine`; M6 snapshots use the
  `SnapshotStateMachine` capability and (for the default store) `ultima_db`'s
  `snapshot_stream` wire format.

Replication is reliable-UDP: the log buffer doubles as the retransmit buffer, a
receiver that falls behind sends NAKs repaired from the buffer (or, below the
purge floor, upgraded to a snapshot session), and flow control is a quorum
order-statistic over follower durable positions. A follower more than one buffer
behind is served from the journal (deep-NAK replay), never prefilled.

Commit / apply pipeline (steady state):
- Client writes a submit frame into the ingress MPSC ring (admission window at the door).
- The leader appends to the log buffer, replicates via the sender agent, and the
  consensus agent advances the commit counter when a quorum's durable positions cross it.
- The service's apply agent polls `min(commit, durable)` in the log buffer in
  place and calls `state_machine.apply(position, cmd)`, publishing the response
  to the egress broadcast (position-keyed) for the client's matcher.

Snapshots + purge (M6, **OFF by default** — `PurgePolicy::Disabled`): a
service-built snapshot lets a node drop the journal prefix below the snapshot
floor (`PurgePolicy::BelowSnapshot { slack_bytes }`). A node that has fallen
below the floor — a crashed-and-restarted service, a fresh **learner**
(replicated-to, never counted in quorum), a cold-started node — converges by
**installing a snapshot + tail-replaying**, never by reading the purged prefix.
`NoCommonPrefix` = wipe-and-rejoin.

Linearizable reads go through a `READ_PROBE`/`ACK` quorum barrier, wait for the
service to catch up to the read position, and use a follower header-term check +
capture-recheck + a service-epoch backstop (accept the answer only if the service
didn't restart during the query) to close the TOCTOU against a crashing service.

Correctness is proven at three levels: the deterministic sim (`uc2_sim`, safety
invariants + seeded fuzz), the WGL lincheck capstones (`uc2_node/tests/lin_v2.rs`
under failover AND purge/snapshot churn; `lin_partition_v2.rs` under
partition/quorum-loss — all driving the untouched `uc-lincheck` checker), and the
multi-process SIGKILL crashtest (`examples/uc2-crashtest`).

## Code conventions

- **`uc_protocol` core types stay `core`-friendly.** `version`/`magic`/`error_codes`
  import nothing outside `core`; the ring buffers need `std::sync::atomic` + `memmap2`.
  No `tokio` in the protocol layer.
- **Apply is sync, deterministic, no I/O.** The trait signature enforces it:
  `fn apply(&mut self, position: u64, cmd: Self::Command) -> Self::Response`. No
  `async`, no clock, no randomness. Non-negotiable for SMR correctness. `position`
  (the absolute byte offset) is the idempotency key.
- **Consensus is pure-sync.** `uc2_consensus` (CommitTracker, ElectionSm) has no
  async and no I/O — it is driven by the node's polling agents and the sim. Safety
  logic lands there so the sim can adjudicate it deterministically.
- **`output_handler` is async, leader-only, retryable.** Returns
  `Result<(), OutputError>` where `Retryable` retries while leader and `Permanent`
  advances the durable (increase-only) progress marker anyway.
- **Reads are typed `Query` / `QueryResponse`, not closures.** The IPC boundary
  doesn't carry closures; the framework routes linearizable vs. snapshot reads.
- **`AppCommand = bytes::Bytes` end-to-end.** Refcounted; flows from the log
  buffer through apply without intermediate copies.
- **Per-record framing uses an atomic-after-write length prefix.** Reader sees
  length=0 → record not yet committed → spin/yield. Standard torn-record protection.
- **cnc page offsets are pinned in BOTH `uc_protocol` and `uc2_log`** with
  offset-assertion tests, and must never drift. Add fields in the reserved band.
- **Snapshot `freeze`/`install_snapshot` are keyed on `position`.** `install_snapshot`
  takes the target position and rejects a mis-tagged artifact.
- **One node per instance directory** — an exclusive flock prevents accidental
  coexistence; service and clients take a shared lock as a liveness probe.
- **`app_id` + `instance_id` + `protocol_version` checked at every IPC entry.**
  Wrong `app_id` = wrong cluster; changed `instance_id` = node restart since last
  attach; protocol mismatch = refuse.

## Feature Development Workflow

Using superpowers (brainstorming, writing-plans, executing-plans) during feature
development is fine — the generated plans/notes under `docs/superpowers/` are
working artifacts. For v2, the per-milestone record is the **gate doc**
(`docs/benchmarks/uc2-mX-gate-*.md`) plus the runbook and the retained superpowers
plan; there is **no `docs/tasks/` consolidation** for v2 (that pattern was v1-era).

**Leave the corresponding superpowers artifacts (`docs/superpowers/plans/*.md`,
`docs/superpowers/specs/*.md`) in place.** Do NOT delete them as part of finishing
a feature — they are retained as historical scaffolding; the maintainer removes
them manually if ever.

## Pointers to dependent crates

- `ultima_journal/` — segmented append journal + `StableValue`. In-tree workspace
  member (moved in from `ultima_db`; full history preserved). Design notes:
  `../ultima_db/docs/tasks/task26_journal.md`.
- `../ultima_db/` — MVCC copy-on-write B-tree store with `snapshot_stream` wire
  format (the default app-state store + snapshot format). See `../ultima_db/CLAUDE.md`
  and `../ultima_db/docs/tasks/task27_snapshot_stream.md`.
