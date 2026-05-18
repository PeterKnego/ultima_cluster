# ultima_cluster

SMR cluster implementation on top of openraft.

**Status:** M4 — MPSC/Broadcast wrap-fix + `uc_client` SDK end-to-end.
Builds on M3.5's openraft 0.10 + `transfer_leader` cutover. See
`docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` for the
canonical design and `docs/tasks/` for per-milestone records.

## Workspace

- `uc_protocol` — wire spec (`no_std`-friendly): ring buffer primitives
  (SPSC / MPSC / Broadcast), `cnc.dat` layout, per-RPC frame types,
  liveness + handshake helpers.
- `uc_service` — service-side SDK (`StateMachine`, `OutputHandler`
  traits) + the `uc_service::ultima_db` adapter
  (`StoreStateMachine`).
- `uc_node` — cluster engine (Raft, log storage, QUIC inter-node
  network, shmem IPC owner: `instance.lock`, `cnc.dat`, service rings,
  liveness ticker, service watcher).
- `uc_client` — local-shmem client SDK (`Client::{connect, submit,
  query_linearizable, query_snapshot, shutdown}`; session liveness; node
  + service stall detection; `NotLeader` hint surfaced to callers).

## Build & test

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

See `CLAUDE.md` for orientation.
