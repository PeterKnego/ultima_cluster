# ultima_cluster

SMR cluster implementation on top of openraft.

**Status:** M2 — multi-node + QUIC inter-node transport complete. See
`docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` for the
canonical design and `docs/tasks/` for per-milestone records.

## Workspace

- `uc_protocol` — wire spec (`no_std`-friendly).
- `uc_service` — service-side SDK (`StateMachine`, `OutputHandler` traits).
- `uc_node` — cluster engine (Raft, log storage, QUIC inter-node network).
- `uc_client` — local-shmem client SDK (M1 stub; full impl in M4).

## Build & test

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

See `CLAUDE.md` for orientation.
