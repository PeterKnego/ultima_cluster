# Task 04 — M3.5: openraft 0.9.24 → 0.10 upgrade

**Status:** Complete.
**Branch:** `main`, commits `43c5180..c3fb3a9` (8 commits + Task 9 polish).
**Workspace:** `ultima_cluster/`.

## Goal

Cut over from openraft `0.9.24` to `0.10.0-alpha.20`, preserving every M1/M2/M3 capstone test and replacing the M3 `raft.shutdown()` substitute in `ipc::service_watcher` with the real `raft.trigger().transfer_leader(target)`.

## Shipped

1. **Workspace dep bump** (`43c5180`) — `openraft 0.10.0-alpha.20` + new `openraft-legacy 0.10.0-alpha.20` for the V1 `RaftNetwork` trait (moved out of openraft proper in 0.10). Dropped the `storage-v2` feature flag (now the only storage path).

2. **`declare_raft_types!` extended** (`4e68c1b`) — added `Term = u64`, `LeaderId = LeaderId<Term, NodeId>` (adv path, preserves on-disk format), `Vote = Vote<Self::LeaderId>`, `Entry = openraft::Entry<<Self::LeaderId as RaftLeaderId>::Committed, Self::D, Self::NodeId, Self::Node>`. The `Responder<T>` line that the spec called for **cannot be set via the macro** (macro grammar limitation, documented in openraft's own `declare_raft_types_test.rs:27`); the default `ProgressResponder<Self, T>` works identically through `Raft::client_write().await`. AppCommand wrapped in a newtype with `#[serde(transparent)]` to satisfy 0.10's new `AppData: Display` bound — on-disk bytes preserved.

3. **Network adapter switch** (`df79e42`) — `QuicRaftNetwork`'s V1 `RaftNetwork` impl now imports from `openraft_legacy::network_v1::RaftNetwork` (the in-tree path is now a deprecation stub).

4. **`JournalLogStorage` refactor** (`8443763`) — `RaftLogStorage` methods return `io::Error` (vs `StorageError<NodeId>`), `truncate` renamed to `truncate_after`, `LogFlushed` → `IOFlushed`, `log_io_completed` → `io_completed`. `read_vote` moved from `RaftLogStorage` to `RaftLogReader` (new in 0.10). Errors preserved via `io::Error::new(kind, e)` source-chain pattern.

5. **`AdaptedStateMachine` refactor** (`e81f41d`) — `apply()` consumes a `Stream<EntryResponder<C>>` and delivers responses via `ApplyResponder::send` per-entry (vs returning `Vec<Bytes>`). `EntryPayload` pattern-match retained (`is_blank`/`into_app_data` don't exist; the plan's claim about them was wrong). Snapshot data drops `Box<>` wrapping per 0.10's "SnapshotData without Box" change.

6. **`ShmemAdaptedStateMachine` refactor** (`cd9c88b`) — mirror of (5) for the shmem variant. Ring publish/await semantics on apply.ring/apply_resp.ring unchanged.

7. **`Raft<TypeConfig, SM>` threading** (`9465b7a`) — 0.10's new SM type parameter required:
   - `pub(crate) enum RaftHandle<S>` in `runtime/node.rs` with `Embedded(Raft<TypeConfig, AdaptedStateMachine<S>>)` and `Shmem(Raft<TypeConfig, ShmemAdaptedStateMachine<S>>)` variants. Match-dispatches all raft methods. `NodeHandle<S>` stays a single-param public type — no breaking change to the public API.
   - `RaftNetworkFactory::Network` requires V2 in 0.10; the V1 `QuicRaftNetwork` is wrapped via `openraft_legacy::network_v1::Adapter::into_v2()`.
   - `RPCError<C, RaftError<C, E>>` (2-param) replaces the 0.9 `RPCError<NID, N, RaftError<NID, E>>` (3-param) throughout the network layer.
   - `runtime/node.rs::submit()` wraps `AppCommand::from(Bytes::from(bytes))`.

8. **`service_watcher` cutover + bootstrap race fix** (`c3fb3a9`) — the only behavioral change in M3.5:
   - `service_watcher.rs` calls `raft.trigger().transfer_leader(target)`. Strict target selection (any voter ≠ self in current membership). 15 s fallback timer fires `raft.shutdown()` if the transfer doesn't take.
   - The 15 s headroom (vs an initial 5 s draft) accounts for openraft 0.10's transfer behavior: `trigger_transfer_leader` is purely local (stops heartbeats, sets `transfer_to`); `current_leader()` only transitions after a new-term election + heartbeat arrival, which is 4-5 s on a loaded host.
   - The V1 adapter's `transfer_leader` RPC returns `Unreachable`, so the target waits its election timer; the generous fallback budget keeps tests deterministic.
   - `m3_service_crash.rs` updated to assert the new behavior: the stalled leader stays alive as a follower (`current_leader() == Some(new_leader)`, not `None`), and the cluster retains all 3 voters. Test uses an all-nodes-agreement convergence loop to avoid racey single-point assertions.
   - **Also fixes a 0.10 bootstrap race:** `Raft::initialize()` returns after the init log is flushed (not committed). A subsequent `add_learner(blocking=true)` racing with the in-flight membership change failed with `InProgress`. Builder retries `add_learner` with 5 ms backoff against a 10 s deadline. Without this, every multi-node cluster was silently single-voter since Task 7 — m2_multi_node and m3_three_node_shmem tests would have regressed.

9. **Polish** (Task 9) — consolidated type aliases (`LeaderId`, `RaftLogId`, `RaftVote`, `RaftStoredMembership`, `RaftSnapshotMeta`, `RaftSnapshot`) from three SM/log_storage files into `uc_node/src/raft/mod.rs` as `pub(crate)` aliases; this task doc; spec patches; README pointer update; plan file deleted per CLAUDE.md workflow. Stale `0.9.24` comments in `node.rs` and `builder.rs` updated.

## On-disk compatibility

`LeaderId`, `LogId`, and `Vote` serialize identically across 0.9 and 0.10 (field shapes unchanged; only the number of type parameters differs). Existing `StableValue<LogId<...>>` / `StableValue<Vote<...>>` files written by 0.9 are readable by 0.10 without conversion. Journal record format also unchanged. AppCommand's `#[serde(transparent)]` newtype preserves on-disk bytes.

## What stayed deferred (to M5+)

- **`RaftNetworkV2` migration.** V1 kept via `openraft-legacy::network_v1`. V2 sub-trait split (`NetBackoff`, `NetStreamAppend`, `NetVote`, `NetSnapshot`, `NetTransferLeader`) is M5.
- **Custom `Responder<T>`** for client_dispatcher — only useful once `clients/response.broadcast` exists (M4).
- **`SnapshotData` swap to `snapshot.region` mmap** — M5 alongside the snapshot wire-format work.
- **`Raft::data_metrics()` / `server_metrics()` migration** — current `metrics()` still works.
- **`generic-snapshot-data` feature flag** — only useful with the mmap swap; M5.
- **Smarter `transfer_leader` target selection** (peer-service-health visibility, prefer highest `last_applied`) — requires cnc-sub-mmap MPSC attach; M4 ground work.

## Verification

All commands green at M3.5 close:

```bash
cargo build --workspace          # clean
cargo test  --workspace          # all tests pass; m3_service_crash updated
cargo clippy --workspace --all-targets -- -D warnings   # zero warnings
cargo fmt --check                # clean
```

## Follow-ups (2026-06-04): shutdown deadlock + test isolation

Running the full suite under load surfaced an intermittent **hang** (not a panic)
in `m3_service_crash::service_crash_on_leader_transfers_leadership`. The test's
own NOTE anticipated *timing* flakiness (bump the fallback / smarter target
selection); the real cause was deeper — a genuine product deadlock in the
0.10 shmem shutdown path — plus two unrelated pre-existing test-isolation bugs.
All fixed and pushed.

1. **Shmem shutdown deadlock** (`4fca5fd`, real bug). `ShmemAdaptedStateMachine::apply`
   (§6) publishes a Normal entry to `apply.ring` then blocks in `await_apply_resp`
   on `apply_resp.ring` — **indefinitely by design**, so apply resumes when a
   crashed service reconnects (it also holds the SM `inner` mutex throughout).
   But `node.shutdown()` begins with `raft.shutdown().await`, and openraft 0.10's
   shutdown *drains the state-machine worker*; if that worker is parked in our
   wedged `apply()`, shutdown never returns. Triggered whenever a node whose
   service has crashed must apply a committed Normal entry — e.g. the ex-leader
   from the `service_watcher` transfer (§8), now a follower, applying the new
   leader's write. Timing-dependent ⇒ intermittent under load, clean in isolation.
   **Fix:** an `Arc<AtomicBool>` shutdown flag on `ShmemAdaptedStateMachine`,
   placed *outside* the `inner` mutex (a wedged apply holds that lock) and shared
   across `Clone` (openraft's worker copy + the `NodeHandle` copy). `node.shutdown()`
   sets it before `raft.shutdown()`; `publish_apply` / `await_apply_resp` poll it
   each iteration and return `io::ErrorKind::Interrupted`. openraft accepts the
   apply error and `raft.shutdown()` returns `Ok`. The entry is **not** durably
   applied (no `last_applied` advance), so it re-applies on restart once the
   service is back — the service store is the source of truth. Embedded mode is
   unaffected (in-process apply never blocks on an external service). Steady-state
   cost: one atomic load per ring-wait iteration. Deterministic regression test:
   `uc_node/tests/m3_shutdown_dead_service.rs` (single node: crash service, wedge
   an apply, assert `node.shutdown()` completes within 10 s — hangs pre-fix,
   ~2 s post-fix).

2. **`m2_multi_node` parallel contention** (`2f4ad6c`, test isolation). The five
   multi-node tests each stand up a 2–3 node loopback-QUIC cluster; run
   concurrently (cargo's default within a binary) they raced two ways:
   `pick_*_addrs` binds ephemeral UDP ports then releases them before the nodes
   re-bind (TOCTOU → cross-cluster QUIC collisions), and ~15 raft nodes at once
   saturate the box so fixed apply/election timeouts expire and openraft timing
   invariants trip. Serialized all five on a shared `tokio::sync::Mutex`
   (`CLUSTER_SERIAL`) held for each test's whole body (the `#[serial]` pattern;
   tokio mutex avoids `clippy::await_holding_lock`) — exactly the known-good
   `--test-threads=1` condition, now automatic under default `cargo test`.

3. **`validate_cnc` misaligned-pointer UB** (`a11a911`, latent bug). `validate_cnc`
   is a safe `fn(&[u8])` but formed `&CncHeader` (`#[repr(C, align(64))]`) from the
   buffer pointer — UB unless 64-byte aligned. Production attachers pass
   page-aligned mmaps (fine); a heap `Vec` from `fs::read` (in a test) tripped
   the debug misalignment check nondeterministically. Fix: reject non-aligned
   buffers with `RingError::Corrupt`; the zero-copy `&CncHeader` return is
   unchanged for the mmap path.

Net: plain `cargo test --workspace` (no flags) is now deterministically green
(149 tests, verified across repeated runs). Related cleanups in the same sweep:
`61d0dc8` (serialize the process-global probe-sink unit tests) and `2ca8995`
(pre-existing clippy lints under newer toolchain). The deferred "smarter
`transfer_leader` target selection" item below is unchanged — it was never the
cause of the hang.

## Pointers

- M3.5 design spec: `docs/superpowers/specs/2026-05-15-uc-m3-5-openraft-0-10-upgrade-design.md`.
- M3 record: `docs/tasks/task03_m3_shmem_service_split.md`.
- M4 spec (rebased on this baseline): `docs/superpowers/specs/2026-05-15-uc-m4-clients-and-ring-fix-design.md`.
- openraft 0.10 source (read during planning + implementation): `../openraft/` (`0.10.0-alpha.20`). Key files:
  - `openraft/src/type_config.rs` — `RaftTypeConfig` trait + `declare_raft_types!` macro.
  - `openraft/src/raft/trigger.rs:86` — `transfer_leader(to)`.
  - `openraft/src/storage/v2/raft_log_storage.rs` — 0.10 trait surface.
  - `openraft/src/storage/v2/raft_state_machine.rs` — apply-stream + EntryResponder model.
  - `openraft/legacy/src/network_v1/` — V1 adapter for legacy RaftNetwork.
