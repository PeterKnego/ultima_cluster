# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

`ultima_cluster` is in early design. The implementation has not started — `src/main.rs` is a placeholder. Before writing any code:

1. Read `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` end-to-end. It is the canonical design.
2. Cross-reference `../ultima_db/docs/tasks/task26_journal.md` (the `ultima_journal` log primitives) and `../ultima_db/docs/tasks/task27_snapshot_stream.md` (the `ultima_db` snapshot wire format). UC's storage adapters are built directly on top of these.
3. The implementation plan lives under `docs/superpowers/plans/` once written.

## Build & Test Commands

```bash
cargo build                                      # build all workspace crates
cargo test                                       # in-process integration tests (default)
cargo test -p uc-crashtest --features hard-crash-tests   # spawn real node+service procs; SIGKILL service mid-load, assert linearizable
cargo clippy --workspace -- -D warnings          # lint (must pass with zero warnings)
cargo bench                                      # criterion benchmarks
cargo run -p uc_node --example kv_node           # reference node binary
cargo run -p uc_service --example kv_service     # reference service binary using ultima_db adapter
cargo run -p uc_client --example kv_client       # reference local-shmem client
```

Workspace crates (per the design spec):

- `uc_protocol` — wire spec; `no_std`-friendly (`core` only). Defines `cnc.dat` layout, ring buffer types (SPSC/MPSC/Broadcast), per-message frame layouts, liveness mechanics, protocol version, error codes. Multi-language gate.
- `uc_node` — cluster engine binary + library. Owns Raft (via `openraft`), log storage (`ultima_journal` + raft `StableValue`s), QUIC inter-node network (`quinn`), discovery directory, `cnc.dat`, dispatchers.
- `uc_service` — service-side SDK. User implements `StateMachine` (sync apply, sync query, snapshot in/out) + optionally `OutputHandler` (async, leader-only). Provides `uc_service::ultima_db::StoreStateMachine` adapter (Cargo feature `ultima_db`, default-on).
- `uc_client` — local-shmem input-client SDK. Small dep set (no openraft, no quinn).
- `uc-lincheck` — test/verification library: WGL linearizability `checker`, op `history` recorder, `model`, and the in-memory CAS-`register` SM (`Cmd`/`CmdResp`/`RegisterSm`). One source of truth shared by the in-process lincheck capstone (`uc_node/tests/lin_register.rs`) and the multi-process hard-crash test.
- `examples/uc-crashtest` — multi-process test harness: `uc-crashtest-{node,service}` reference bins (each runs one half over a shared instance_dir) + the `hard_crash.rs` / `smoke.rs` tests behind the `hard-crash-tests` feature. The real `kill -9` path for service-reconstruction validation (task14).

## Architecture overview

`ultima_cluster` is a State Machine Replication application server on top of `openraft`. Three process roles, all same-host inter-process traffic via shared memory; cross-host traffic via QUIC between cluster engines:

```
[client process]      ──shmem──▶  [uc_node]  ◀──QUIC──▶  [uc_node on peer host]
                                      ▲
                                      │ shmem
                                      ▼
                                 [uc_service]
```

- `uc_node` owns Raft consensus, log durability, snapshot transport, leader election.
- `uc_service` owns the user's deterministic business logic (`apply`, `query`) and side-effecting `on_committed`.
- `uc_client` processes own input handling — translate external requests into Commands, submit via shmem.

The shmem layer is a fixed-layout `cnc.dat` control file plus per-stream ring buffer files under a discovery directory (default `/dev/shm/ultima-{user}-{instance}`). All ring buffers are lock-free; SPSC for service↔node, MPSC for clients→node, Broadcast for node→clients.

Storage primitives:
- Raft log: `ultima_journal::Journal` (segmented append, group commit, per-record term in `meta` slot).
- Raft state durables: `ultima_journal::StableValue<T>` (rotating two-slot atomic value) for vote, committed, output_progress, last_purged, membership.
- App state: `ultima_db::Store` (when using the default `StoreStateMachine` adapter). Snapshots use `ultima_db::snapshot_stream` wire format end-to-end.

Inter-node transport: QUIC via `quinn`. One connection per peer-pair, multiple bidirectional streams (heartbeat / append-entries / vote / install-snapshot) — no head-of-line blocking across RPC classes. TLS by default; `TlsConfig::SelfSigned` mode for v1 dev/test.

Apply pipeline (steady state):
- Client writes `SubmitFrame` into `clients/submit.ring` (MPSC).
- Node's client_dispatcher consumes, calls `openraft.client_write(payload)`.
- Openraft replicates, commits, calls our `RaftStateMachine::apply` with committed entries.
- We publish `ApplyFrame{log_index, payload}` into `service/apply.ring` (SPSC).
- Service's apply_loop (sync thread) consumes, calls `state_machine.apply(log_index, cmd)`, publishes `ApplyRespFrame` into `service/apply_resp.ring`.
- Node consumes the response, hands back to openraft, broadcasts `SubmitResponse` to client via `clients/response.broadcast`.

Output (leader-only, at-least-once with durable progress marker `output_progress.state` on node):
- Apply happens; node sends `OutputFrame` via `service/output.ring`.
- Service's output_loop calls `output_handler.on_committed(log_index, &cmd, &state).await`.
- On Ok: node advances `output_progress.state` (durable); on Retryable: backoff + retry while still leader; on Permanent: log warn, advance anyway.
- On leader transition: new leader scans `(last_completed, last_applied]` from journal, replays `on_committed` for each. `log_index` is the natural idempotency key — user's responsibility.

Service crash → node keeps replicating, voluntarily transfers leadership if leader, and **reconstructs** the (possibly fresh in-memory) service when it reconnects — replaying `(service_last_applied, node_frontier]` from the journal, or installing a snapshot + tail-replaying when the gap is below the purge boundary. So an in-memory SM survives a service-only restart and a node cold-start (it no longer "loses state"); log purge is safe because purge is backed by a real service-built snapshot. Reconstruction triggers on a service-epoch change OR a prefix gap (node restart whose log was purged), driven both lazily by `apply()` and proactively by a reconcile-driver task. Linearizable reads go through a `ReadIndex` barrier (`Raft::ensure_linearizable`), wait for the service to catch up to the read index, and use a seqlock check (accept the answer only if the service didn't restart during the query) to close the TOCTOU against a crashing service. The lincheck capstone runs a non-persisting in-memory SM under both faults + heavy churn and stays linearizable. See `docs/tasks/task14_service_state_reconstruction.md`.

## Code conventions

- **`uc_protocol` is `no_std`-friendly.** `core` only — no `tokio`, no `std::io`, no `serde` in the protocol layer (bincode framing happens above protocol). The `tokio`-bound code lives in `uc_node`/`uc_service`.
- **Apply is sync, deterministic, no I/O.** The trait signature enforces this: `fn apply(&mut self, log_index: u64, cmd: Self::Command) -> Self::Response`. No `async`. No clock. No randomness. This is non-negotiable for SMR correctness.
- **`output_handler` is async, leader-only, retryable.** Returns `Result<(), OutputError>` where `Retryable` retries while leader and `Permanent` advances the marker anyway.
- **Reads are typed `Query` / `QueryResponse`, not closures.** The IPC boundary doesn't carry closures. Same trait method (`fn query(&self, q: Self::Query) -> Self::QueryResponse`); the framework decides linearizable vs. snapshot routing.
- **`AppCommand = bytes::Bytes` end-to-end.** Refcounted; flows from journal read through openraft into the apply ring without intermediate copies.
- **Per-record framing uses atomic-after-write length prefix.** Reader sees length=0 → record not yet committed → spin/yield. Standard lock-free torn-record protection.
- **Inter-node QUIC zero-copy via `quinn::SendStream::write_chunks(&[Bytes])`.** AppendEntries body assembled as scatter-gather slices; no internal copy.
- **`StoreStateMachine` pins `ultima_db` version to log_index.** `store.begin_write(Some(log_index))` on every apply; `store.latest_version() == last_applied` for the SMR persistence path. This is what makes recovery deterministic.
- **`build_snapshot` and `install_snapshot` return `u64`** (the log_index represented / post-install). Resolves the race where `last_applied` may advance between framework decision and snapshot call.
- **One node per instance directory** — `instance.lock` exclusive flock prevents accidental coexistence. Service and clients try shared lock as liveness probe.
- **`app_id` + `instance_id` + `protocol_version` checked at every IPC entry.** Mismatched `app_id` = wrong cluster. Mismatched `instance_id` = node restart since last attach. Mismatched protocol version = refuse.

## Feature Development Workflow

Using superpowers (brainstorming, writing-plans, executing-plans) during feature development is fine — the generated plans/notes under `docs/superpowers/` are working artifacts. Before finishing and committing the feature:

1. Consolidate the architectural decisions and implementation details into `docs/tasks/taskXX_feature_name.md` (the canonical per-feature doc). This is the permanent record — it must stand on its own, folding in the essential design rationale so it does not depend on the superpowers artifacts.
2. **Leave the corresponding superpowers artifacts (`docs/superpowers/plans/*.md`, `docs/superpowers/specs/*.md`) in place.** Do NOT delete them as part of consolidation — they are retained as historical scaffolding and the maintainer removes them manually if ever. Committing them alongside the `taskXX_feature_name.md` is fine.

`docs/tasks/` is the canonical permanent record; the superpowers artifacts are retained design history. The current `2026-05-10-ultima-cluster-design.md` spec will be consolidated into `docs/tasks/task01_initial_implementation.md` (or similar) once v1 ships.

## Pointers to dependent crates

- `../ultima_journal/` — segmented append journal + StableValue. See `../ultima_db/docs/tasks/task26_journal.md`.
- `../ultima_db/` — MVCC copy-on-write B-tree store with snapshot_stream wire format. See `../ultima_db/CLAUDE.md` and `../ultima_db/docs/tasks/task27_snapshot_stream.md`.
- `openraft` — Raft consensus library; storage trait spec at https://deepwiki.com/databendlabs/openraft/2.3-implementing-storage-traits ; network trait spec at https://deepwiki.com/databendlabs/openraft/2.4-implementing-the-network-layer .
- `quinn` — QUIC implementation in Rust.
