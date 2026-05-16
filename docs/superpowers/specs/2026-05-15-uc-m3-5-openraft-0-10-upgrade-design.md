# M3.5 design — openraft 0.9.24 → 0.10 upgrade

**Status:** design (brainstormed 2026-05-15, awaiting plan).
**Predecessors:** M3 (`docs/tasks/task03_m3_shmem_service_split.md`) — shmem IPC + `uc_service` process split.
**Successors:** M4 (`docs/superpowers/specs/2026-05-15-uc-m4-clients-and-ring-fix-design.md`) rebases on the upgraded baseline. The M4 spec's "Out of scope" entries for "openraft 0.10 upgrade" and "real `Raft::trigger_leader_transfer`" get re-tagged as "shipped in M3.5" when M3.5 lands (small inline edit, no behavioral impact).
**Workspace:** `ultima_cluster/`.

## Goal

Cut over from `openraft = "0.9.24"` to `openraft = "0.10"` (current alpha or first stable when we land), preserving all M1/M2/M3 capstone test coverage. Two motivating wins:

1. **Replace M3's `raft.shutdown()` substitute in `ipc::service_watcher`** with the real `Raft::trigger().transfer_leader(to)` — restoring the design spec's intended behavior on service-crash leadership transfer.
2. **Land on a stable type surface for M4's `uc_client`** — building dispatchers on 0.9 types and refactoring them in the same milestone as `client_dispatcher` would be two big interleaved changes; M3.5 separates the dep upgrade from the feature work.

This is intentionally a small, mechanical milestone — bounded blast radius, all tests stay green, no protocol changes.

## Scope

**In:**

- Workspace `Cargo.toml` bump: `openraft = "0.10"`.
- Extend the `declare_raft_types!` invocation in `uc_node/src/raft/mod.rs` with the new associated types: `Term`, `LeaderId`, `Vote`, `Responder<T>`.
- Thread the new `Raft<C, SM>` second type parameter through `NodeHandle::raft`, `QuicRaftNetworkFactory`, `network::server::spawn_server`, and any other touch points the compiler surfaces.
- Trait-impl signature audit in `state_machine.rs`, `state_machine_shmem.rs`, `log_storage.rs`, `network/*` for the types decoupled-from-`C` in 0.10 (`Entry`, `LogId`, `SnapshotMeta`, `StoredMembership`, `LeaderId`, `Vote`).
- **`ipc::service_watcher` cutover** — replace `raft.shutdown()` with `raft.trigger().transfer_leader(target)`, with `raft.shutdown()` retained as a 5 s fallback timeout if the transfer doesn't take.
- Update the M3 service-crash test (`m3_service_crash.rs`) to assert the new behavior: leader transfers (its raft stays alive, role becomes Follower) rather than terminating its raft.
- Update the M3 task doc's "Follow-ups for M4+" section to mark "openraft 0.10 upgrade" and "real `Raft::trigger_leader_transfer`" as shipped.

**Out (deferred):**

- **Custom `Responder<T>` for `client_dispatcher`** that publishes directly to `response.broadcast`. M4 work (depends on the broadcast existing). **Note (discovered during Task 2):** `declare_raft_types!` does not support a `Responder<T>` line — the macro grammar doesn't include it (documented in openraft's own `declare_raft_types_test.rs:27`). The default `ProgressResponder<Self, T>` is used and works identically through `Raft::client_write().await`.
- **`SnapshotData` swap to `snapshot.region` mmap.** M5.
- **`Raft::data_metrics()` / `server_metrics()` migration.** Current `metrics()` keeps working; deferred to M5 when observability sub-buffers in `cnc.dat` get wired.
- **`generic-snapshot-data` feature flag.** Only useful alongside the mmap swap; M5.
- **`RaftNetworkV2`.** Stay on the V1 trait surface; revisit only if a concrete win shows up later.
- **`Raft::install_full_snapshot()` / `Raft::begin_receiving_snapshot()`** new install APIs — current chunked path keeps working; revisit alongside the mmap swap.

## On-disk format: no migration needed

I verified field-by-field across 0.9.24 and the 0.10-alpha source on disk (`../openraft`, currently `0.10.0-alpha.20`):

| Type | 0.9.24 layout | 0.10 layout | Serde-bincode bytes |
|---|---|---|---|
| `LeaderId<NID>` (us: `NID = u64`) | `{ term: u64, node_id: u64 }` | `LeaderId<Term, NID>` with `Term = u64, NID = u64` — same struct, two type params instead of one | **identical** |
| `LogId<NID>` (us: `NID = u64`) | `{ leader_id: LeaderId<NID>, index: u64 }` | `LogId<CLID>` with `CLID = LeaderId<u64, u64>` | **identical** |
| `Vote<NID>` | `{ leader_id: LeaderId<NID>, committed: bool }` | `Vote<LID>` with `LID = LeaderId<u64, u64>` | **identical** |

Bincode keys on field names + concrete field types; the type-parameter shape changes don't affect emitted bytes. `StableValue<LogId<u64>>` files written by 0.9 can be loaded by 0.10 without conversion. Same for `StableValue<Vote<u64>>` and `StableValue<StoredSnapshotMeta>` (the latter is our own struct unchanged).

This makes the upgrade pure compile-time work — no migration code, no on-disk format bump, no version flag in the journal.

## Type-config changes

Current (0.9.24) — `uc_node/src/raft/mod.rs:41`:

```rust
openraft::declare_raft_types!(
    pub TypeConfig:
        D = AppCommand,
        R = AppResponse,
        NodeId = NodeId,
        Node = NodeAddr,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
```

After (0.10):

```rust
openraft::declare_raft_types!(
    pub TypeConfig:
        D = AppCommand,
        R = AppResponse,
        NodeId = NodeId,
        Node = NodeAddr,
        Term = u64,
        LeaderId = openraft::impls::leader_id_adv::LeaderId<Self::Term, Self::NodeId>,
        Vote = openraft::impls::Vote<Self::LeaderId>,
        Entry = openraft::impls::Entry<Self>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        Responder<T> = openraft::impls::OneshotResponder<Self, T>,
        AsyncRuntime = openraft::impls::TokioRuntime,
);
```

The leader-id-adv path keeps the totally-ordered `LeaderId { term, node_id }` shape — same serde format as 0.9. (The alternative is `leader_id_std::LeaderId` which uses `(term, voted_for: Option<NodeId>)`; that's a *different* on-disk layout, so we explicitly pick adv to preserve compat.)

## API touch points

The compiler will surface most of these; this section lists what we already know moves.

**`Raft<C>` → `Raft<C, SM = ()>`** (`openraft/src/raft/mod.rs:319`).
- `uc_node::runtime::node::NodeHandle::raft: Raft<TypeConfig>` becomes `Raft<TypeConfig, SmAdapter<S>>`, where `SmAdapter<S>` is whichever adapter the builder constructed.
- `QuicRaftNetworkFactory` and `network::server::spawn_server`'s signatures gain the SM parameter (it's mostly threading; openraft uses SM internally for state-machine messaging).
- `service_watcher`'s captured `Raft<TypeConfig>` clone becomes `Raft<TypeConfig, _>` — but since the watcher only calls `raft.current_leader()` and `raft.trigger()`, the SM parameter is fine to leave inferred.

**Types decoupled from `C`** — every place we wrote `LogId<NodeId>`, `StoredMembership<NodeId, NodeAddr>`, `SnapshotMeta<NodeId, NodeAddr>`, `Entry<TypeConfig>`:
- `LogId<NodeId>` → `LogId<CommittedLeaderId<u64, u64>>` (or write it via `<TypeConfig as RaftTypeConfig>::LogId`-style associated-type aliases if openraft exposes them).
- `StoredMembership<NodeId, NodeAddr>` → `StoredMembership<NodeId, NodeAddr>` (same generics, but now decoupled from C — same usage).
- `SnapshotMeta<NodeId, NodeAddr>` → `SnapshotMeta<NodeId, NodeAddr>` (similar; verify).
- `Entry<TypeConfig>` → still parameterized over `C` in trait signatures; concrete `Entry<TypeConfig>` continues to work.
- `Vote<NodeId>` → `Vote<LeaderId<u64, u64>>`.

**`RaftStateMachine` impl** (both `AdaptedStateMachine` and `ShmemAdaptedStateMachine`):
- `apply<I: IntoIterator<Item = openraft::Entry<TypeConfig>>>` — the `Entry` shape changes per the decoupling above; the trait method signature should be unchanged in form.
- `install_snapshot(&SnapshotMeta<NodeId, NodeAddr>, ...)` — same shape.
- `get_snapshot_builder` — same.

**`RaftLogStorage` impl** (`JournalLogStorage`):
- `append`, `truncate`, `purge`, `get_log_state`, `read_log_entries`, `save_vote`, `save_committed`, `read_vote`, `read_committed` — all retain their shape but may have different `LogId<...>` / `Entry<...>` / `Vote<...>` parameterizations. Verify each.

**`RaftNetwork` / `RaftNetworkFactory` impls** (`QuicRaftNetworkFactory`, `QuicConnection`):
- 0.10 keeps both V1 (`RaftNetwork`) and V2 (`RaftNetworkV2`). We stay on V1.
- `append_entries`, `install_snapshot`, `vote` — shapes should be preserved; the inner type changes are absorbed by the trait surface.

## `service_watcher` cutover (the one behavioral change)

Current (M3):

```rust
// uc_node/src/ipc/service_watcher.rs
if !alive && !stalled_for_task.load(Ordering::Relaxed) {
    stalled_for_task.store(true, Ordering::Relaxed);
    if raft.current_leader().await == Some(node_id) {
        let _ = raft.shutdown().await;     // M3 substitute
    }
}
```

After (M3.5):

```rust
if !alive && !stalled_for_task.load(Ordering::Relaxed) {
    stalled_for_task.store(true, Ordering::Relaxed);
    if raft.current_leader().await == Some(node_id) {
        if let Some(target) = pick_transfer_target(&raft, node_id).await {
            // Fire-and-forget; openraft handles target rejection internally.
            let _ = raft.trigger().transfer_leader(target).await;
            // Fallback timer: if we're still leader after 5 s, fall back to
            // raft.shutdown() so we don't pin leadership on a doomed transfer.
            let raft_for_fallback = raft.clone();
            let node_id_for_fallback = node_id;
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                if raft_for_fallback.current_leader().await == Some(node_id_for_fallback) {
                    let _ = raft_for_fallback.shutdown().await;
                }
            });
        } else {
            // No viable peer — single-node cluster or all peers unreachable.
            // Same fallback as M3.
            let _ = raft.shutdown().await;
        }
    }
}
```

**Target selection** (`pick_transfer_target`) — **strict**: pick any voter in the current membership other than self. No service-liveness probe of peers (each node only has access to its own service's heartbeat via shmem — peer service health isn't visible from here in M3.5). openraft refuses to transfer to a peer it can't reach; a doomed transfer is bounded by the 5 s fallback timer. Smarter target selection (peer health visibility, prefer highest `last_applied`) is M4+ work and would need cnc-sub-mmap MPSC attach to land first.

Implementation: read the current membership from `raft.metrics().borrow().membership_config`, iterate voters, return the first one that isn't `self`.

**Note on the orphan fallback task.** The 5 s timer in `tokio::spawn` is fire-and-forget — `NodeHandle::shutdown` doesn't track it. If a test or production shutdown happens within that window the orphan completes on its own (the `current_leader()` check fails after `raft.shutdown()` runs, and the orphan's branch becomes a no-op). Acceptable v1 trade-off; a future tightening could thread the task into `service_watcher`'s `LivenessHandle` join set.

## M3 service-crash test update

`m3_service_crash.rs` currently asserts:
- Watcher fires `service_stalled = true`.
- Surviving nodes re-elect (because the leader's raft was shut down by the watcher).

After M3.5, the assertion shifts:
- Watcher fires `service_stalled = true`.
- The stalled node's raft is **still alive** (no `shutdown()` was called).
- The stalled node's role transitions to **Follower** (after `transfer_leader` takes).
- A new leader is elected among the remaining voters (including the stalled node, which is back in the pool as a follower).
- Submit through the new leader succeeds.

This test is the canary that the upgrade preserves M3 behavior *and* improves it (the cluster keeps three voters instead of two).

## Implementation phasing

| Phase | Scope | Commits |
|---|---|---|
| 1 | Bump `openraft = "0.10"` in workspace Cargo.toml. Update the `declare_raft_types!` invocation. Fix anything else that compiles trivially. | 1 |
| 2 | Trait-impl signature fixes — `RaftStateMachine` for both adapters; `RaftLogStorage` for `JournalLogStorage`; `RaftNetwork`/`RaftNetworkFactory` for QUIC. Compile to green. | 2-3 |
| 3 | Thread `Raft<TypeConfig, SmAdapter>` second-param through `NodeHandle`, `QuicRaftNetworkFactory`, server. | 1 |
| 4 | `ipc::service_watcher`: replace `raft.shutdown()` with `raft.trigger().transfer_leader(target)` + 5 s fallback. Add `pick_transfer_target` helper. Update `m3_service_crash.rs` assertions. | 1 |
| 5 | Polish: clippy / fmt; update `docs/tasks/task03_m3_shmem_service_split.md` follow-ups; bump README pointer if needed. | 1 |

Total: **8 commits + 1 polish = 9 total**, ~600-700 line diff, no behavior change in steady-state paths (only `service_watcher` action under stall changes).

## Verification checklist

After M3.5 ships:

- `cargo build --workspace` — clean.
- `cargo test --workspace` — all 93 M3-era tests still pass, plus updated `m3_service_crash` reflecting the transfer behavior.
- `cargo clippy --workspace --all-targets -- -D warnings` — zero warnings.
- `cargo fmt --check` — clean.
- `cargo doc --workspace --no-deps` — builds.
- Manually verify `m3_service_crash`:
  - Stalled-leader node's raft is still alive after the transfer (`current_leader()` returns `Some(_)` not `None`).
  - The surviving cluster has all 3 voters in the membership (vs M3's 2).

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Hidden trait-method signature shift surfaces only late in the trait-impl fixes | Phase 2 is the longest phase exactly because of this; budget 2-3 commits. Compiler errors are the safety net. |
| `transfer_leader` to a stalled peer pins the leader role forever | 5 s fallback timer firing `raft.shutdown()` if we're still leader. |
| `LeaderId` adv-vs-std picks accidentally regress on-disk compat | Spec pins `leader_id_adv` explicitly. A `cargo test` against an existing `data_dir` from an M3 run would confirm. |
| 0.10 stable not yet released when M3.5 lands | Track the latest 0.10 alpha (`0.10.0-alpha.20` at the time of brainstorming). If a `0.10.0` release happens before M3.5 ships, use that. Pin a specific version, not `0.10`. |
| New transitive deps from 0.10 break our `cargo deny`/audit story | Run `cargo tree`/`cargo audit` as part of Phase 1; flag and decide. |

## Pointers

- Canonical design: `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` (§10 service-crash leader transfer references `Raft::trigger_leader_transfer`).
- M3 record: `docs/tasks/task03_m3_shmem_service_split.md` (the "Follow-ups for M4+" section lists the upgrade and transfer-leader as deferred items M3.5 ships).
- openraft 0.10 source: `../openraft/` (currently `0.10.0-alpha.20`). Key files referenced during brainstorming:
  - `openraft/src/type_config.rs:30-160` — the `RaftTypeConfig` trait + `declare_raft_types!` macro example.
  - `openraft/src/raft/mod.rs:319` — `pub struct Raft<C, SM = ()>`.
  - `openraft/src/raft/trigger.rs:86` — `pub async fn transfer_leader(&self, to: C::NodeId) -> Result<(), Fatal<C>>`.
  - `openraft/src/log_id/mod.rs:58` — `pub struct LogId<CLID>`.
  - `openraft/src/vote/leader_id/leader_id_adv.rs:17` — `pub struct LeaderId<Term, NID>`.
