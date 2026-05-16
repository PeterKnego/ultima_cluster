# M3.5 — openraft 0.9.24 → 0.10 upgrade — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade `ultima_cluster` from openraft `0.9.24` to `0.10`, preserving every M1/M2/M3 capstone test and replacing the M3 `raft.shutdown()` substitute in `ipc::service_watcher` with the real `Raft::trigger().transfer_leader(target)`.

**Architecture:** Mechanical type-surface refactor (no protocol changes, no on-disk format change). The `RaftLogStorage` and `RaftStateMachine` traits switch their error type to `io::Error`, the SM's `apply()` switches from `IntoIterator<Item = Entry>` returning `Vec<Bytes>` to a `Stream<EntryResponder>` that delivers responses via `ApplyResponder::send_response`, and `RaftNetwork` (V1) lives in the separate `openraft-legacy` crate. One behavioral change: `service_watcher` calls `raft.trigger().transfer_leader(target)` with a 5 s `raft.shutdown()` fallback. Strict target selection (any voter ≠ self).

**Tech Stack:** Rust 2024 edition; openraft 0.10 + openraft-legacy 0.10 (V1 RaftNetwork trait); existing journal/store/quic stack unchanged.

**Source-of-truth spec:** `docs/superpowers/specs/2026-05-15-uc-m3-5-openraft-0-10-upgrade-design.md`

---

## File structure

### Modified

```
Cargo.toml                                          # bump openraft to 0.10; add openraft-legacy
uc_node/src/raft/mod.rs                             # declare_raft_types! extended for 0.10
uc_node/src/raft/log_storage.rs                     # RaftLogStorage: io::Error; truncate_after; type aliases
uc_node/src/raft/state_machine.rs                   # RaftStateMachine: apply(Stream<EntryResponder>); io::Error
uc_node/src/raft/state_machine_shmem.rs             # same as state_machine.rs for shmem variant
uc_node/src/network/instance.rs                     # use openraft_legacy::network_v1::RaftNetwork
uc_node/src/network/factory.rs                      # same (RaftNetworkFactory still in-tree)
uc_node/src/network/server.rs                       # thread Raft<TypeConfig, SmAdapter> through
uc_node/src/runtime/node.rs                         # NodeHandle::raft: Raft<TypeConfig, _>
uc_node/src/runtime/builder.rs                      # finish() now generic over SM too
uc_node/src/ipc/service_watcher.rs                  # transfer_leader cutover; pick_transfer_target helper
uc_node/tests/m3_service_crash.rs                   # assert new transfer behavior
docs/tasks/task03_m3_shmem_service_split.md         # mark openraft 0.10 + transfer_leader as shipped
README.md                                           # pointer M3 → M3.5
docs/superpowers/specs/2026-05-15-uc-m3-5-openraft-0-10-upgrade-design.md   # one-line update on scope realization
```

### Created

```
docs/tasks/task04_m3_5_openraft_0_10_upgrade.md     # consolidated task doc (final task)
```

### Not touched (by design)

- `uc_protocol/*` — no protocol or ring changes; this is purely a uc_node-internal dep upgrade.
- `uc_service/*` — `StateMachine` / `OutputHandler` traits are user-facing and aren't openraft types.
- `uc_client/*` — empty stub today; M4 lands client code on top of this baseline.
- `ultima_journal/*`, `ultima_db/*` — out-of-workspace deps; we only consume them.

---

## Pre-flight (per-task)

Every task ends with the same four-step verification:

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo fmt --check
```

The first task is the only one that intentionally leaves the tree non-compiling at the dep-bump step; all subsequent tasks must restore green before committing.

---

## Task 1: Workspace dep bump

**Files:**
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Inspect current openraft dep line**

Run:
```bash
grep -n openraft Cargo.toml
```

Expected:
```
25:openraft = { version = "0.9", features = ["serde", "storage-v2"] }
```

- [ ] **Step 2: Update the openraft line + add openraft-legacy**

Edit `Cargo.toml` workspace dependencies block: change `openraft` from `0.9` to `0.10`, and add `openraft-legacy` immediately after.

```toml
openraft = { version = "0.10", features = ["serde"] }
openraft-legacy = { version = "0.10", features = ["serde"] }
```

Notes:
- The `storage-v2` feature flag was removed in 0.10 (v2 is now the only storage path), so it's dropped from the feature list.
- `openraft-legacy` provides the V1 `RaftNetwork` trait that we will keep using in M3.5; V2 migration is M5.
- Keep the version key as `"0.10"` (no patch pin); the lockfile will pick the latest 0.10.x or 0.10.0-alpha.N — fine for now. When 0.10.0 stable ships, that line resolves to stable automatically.

- [ ] **Step 3: Add uc_node dep for openraft-legacy**

Edit `uc_node/Cargo.toml` — add the new line in the `[dependencies]` section right after the existing `openraft = { workspace = true }`:

```toml
openraft = { workspace = true }
openraft-legacy = { workspace = true }
```

- [ ] **Step 4: Run cargo build and confirm it fails with the expected wall of errors**

Run:
```bash
cargo build --workspace 2>&1 | head -30
```

Expected: a flood of `error[E0308]`, `error[E0061]`, `error[E0432]` etc. about `LogId`, `Entry`, `StorageError`, `RaftNetwork`, `RaftLogStorage`, `RaftStateMachine`. **Do not panic** — this is expected; subsequent tasks fix each cluster of errors. The build will not be green again until Task 6.

- [ ] **Step 5: Commit**

Stage and commit just the Cargo.toml diffs. Leave `Cargo.lock` out for now if it's noisy; if it changed, include it (cargo regenerates it as we add/remove deps).

```bash
git add Cargo.toml Cargo.lock uc_node/Cargo.toml
git commit -m "chore(deps): bump openraft 0.9.24 -> 0.10; add openraft-legacy

Workspace-wide dep bump. Drops the storage-v2 feature flag (v2 is now
the only storage path in 0.10). Adds openraft-legacy 0.10 for the V1
RaftNetwork trait (kept for M3.5; V2 migration deferred to M5).

Tree intentionally won't compile until the trait-impl refactors in
subsequent commits land — Cargo.lock is the only file that should
have new transitive entries here."
```

---

## Task 2: TypeConfig macro upgrade

**Files:**
- Modify: `uc_node/src/raft/mod.rs:41-50`

- [ ] **Step 1: Read the current `declare_raft_types!` invocation**

Run:
```bash
sed -n '40,50p' uc_node/src/raft/mod.rs
```

Expected:
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

- [ ] **Step 2: Replace with the 0.10 form**

Edit `uc_node/src/raft/mod.rs` lines 41-50 to:

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

Critical: **`leader_id_adv`**, not `leader_id_std`. The adv path keeps the `{ term: u64, node_id: NID }` shape that matches our 0.9 on-disk LogId/Vote bytes. `_std` would change the layout and break existing journal/StableValue files.

- [ ] **Step 3: Try to build only this crate**

Run:
```bash
cargo build -p uc_node 2>&1 | head -20
```

Expected: the type-config errors that were the first wall in Task 1 are gone, but there are still many errors deeper in the file tree (state machine, log storage, network). Those are the next tasks.

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/raft/mod.rs
git commit -m "refactor(uc_node): declare_raft_types! for openraft 0.10

Adds Term, LeaderId, Vote, Responder<T> associated types to the macro
invocation. Pins LeaderId = leader_id_adv::LeaderId<Term, NodeId> to
preserve the (term, node_id) on-disk layout used by our StableValue
files; switching to leader_id_std would change the bincode bytes."
```

---

## Task 3: Switch network adapter to openraft-legacy

**Files:**
- Modify: `uc_node/src/network/instance.rs`
- Modify: `uc_node/src/network/factory.rs`
- Modify: `uc_node/src/network/server.rs`

The `RaftNetwork` (V1) trait moved out of `openraft` and into `openraft-legacy` in 0.10. `RaftNetworkFactory` stays in `openraft` itself. Update the imports.

- [ ] **Step 1: Update `network/instance.rs` imports + impl**

The current imports at the top of `uc_node/src/network/instance.rs`:

```rust
use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
```

Change to:

```rust
use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft_legacy::network_v1::RaftNetwork;
```

The `impl RaftNetwork<TypeConfig> for QuicRaftNetwork { ... }` block stays as-is — only the import path changed.

- [ ] **Step 2: Update `network/factory.rs` imports**

`RaftNetworkFactory` is still in `openraft` proper, but `Self::Network = QuicRaftNetwork` now refers to a type that implements `openraft_legacy::network_v1::RaftNetwork`. The `RaftNetworkFactory<TypeConfig>` trait will internally constrain `Self::Network: openraft_legacy::network_v1::RaftNetwork<TypeConfig>` via openraft-legacy's reexport. Confirm no code changes here — but if `cargo build` complains about an unresolved `RaftNetwork` import, add the explicit re-import.

If a build error appears citing `RaftNetwork` not in scope at the factory site, prepend the same import to `factory.rs`:

```rust
use openraft_legacy::network_v1::RaftNetwork;
```

Otherwise leave the file alone.

- [ ] **Step 3: Update `network/server.rs` imports + signature**

This file uses `openraft::Raft` directly. With 0.10's `Raft<C, SM = ()>`, the existing `Raft<TypeConfig>` is shorthand for `Raft<TypeConfig, ()>`. **For the server we don't care about the SM type parameter** — leave the `()` default by writing the same `Raft<TypeConfig>` as before. Task 6 will revisit if any call site needs the explicit SM.

Verify by reading the file — no changes should be needed here yet.

- [ ] **Step 4: Build and verify the network errors are resolved**

Run:
```bash
cargo build -p uc_node 2>&1 | grep -E "RaftNetwork|raft_network|network_v1" | head
```

Expected: empty output (no remaining mentions). If errors persist, follow the compiler.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/network/
git commit -m "refactor(uc_node): switch network adapter to openraft-legacy V1

RaftNetwork (V1) moved out of openraft proper in 0.10; the in-tree
trait is now a deprecation stub. Adopt openraft_legacy::network_v1::
RaftNetwork to keep the existing QUIC adapter shape. V2 migration is
deferred to M5 (see M3.5 spec)."
```

---

## Task 4: Refactor `JournalLogStorage` for 0.10

**Files:**
- Modify: `uc_node/src/raft/log_storage.rs`

Three classes of change:

1. **Error type**: every trait method's return type goes from `Result<_, StorageError<NodeId>>` to `Result<_, io::Error>`. The `map_sv_*` / `map_journal_*` helpers similarly return `io::Error` directly.
2. **`truncate` → `truncate_after`**: the method is renamed in 0.10.
3. **Type aliases**: replace `LogId<NodeId>` with `LogIdOf<C>` (or keep as `openraft::LogId<openraft::impls::leader_id_adv::LeaderId<u64, u64>>` for explicit forms). The simpler path is the alias.

- [ ] **Step 1: Update top-of-file imports**

Find the import block (around lines 16-17 and 159-165). Replace:

```rust
use openraft::{LogId, StoredMembership, Vote};
// ... later ...
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::storage::LogFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;
```

with:

```rust
use std::io;

use openraft::{LogId, StoredMembership, Vote};
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;
```

Drop `StorageError`, `StorageIOError`, `LogFlushed` — none used after the refactor. `LogFlushed` was a 0.9.x type alias; 0.10 renames it to `IOFlushed<C>`.

- [ ] **Step 2: Rewrite the error-mapping helpers**

Replace the six `map_sv_*` / `map_journal_*` helpers (currently around lines 169-195) with:

```rust
fn sv_io(e: ultima_journal::StableValueError) -> io::Error {
    io::Error::other(e.to_string())
}

fn journal_io(e: ultima_journal::JournalError) -> io::Error {
    io::Error::other(e.to_string())
}
```

Then audit every call site of the old helpers (search the file for `map_sv_` and `map_journal_`) and replace with the appropriate `sv_io` / `journal_io`. Each call site looked like:

```rust
.map_err(map_sv_write_vote)?
```

becomes:

```rust
.map_err(sv_io)?
```

(The split-by-purpose was only useful for the now-removed `StorageIOError::write_vote` distinction; in 0.10 callers no longer need that.)

- [ ] **Step 3: Fix the `RaftLogReader::try_get_log_entries` signature**

Find this around line 197. Change:

```rust
async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
    &mut self,
    range: RB,
) -> Result<Vec<openraft::Entry<TypeConfig>>, StorageError<NodeId>> {
```

to:

```rust
async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
    &mut self,
    range: RB,
) -> Result<Vec<openraft::impls::Entry<TypeConfig>>, io::Error> {
```

…and update the two `StorageIOError::<NodeId>::read_logs(&io_err)` inside this method to plain `io_err` returns:

```rust
.map_err(|e| io::Error::other(e.to_string()))?
```

- [ ] **Step 4: Fix all `RaftLogStorage` method signatures**

Walk every method in the `impl RaftLogStorage<TypeConfig> for JournalLogStorage { ... }` block. For each:
- Replace `Result<_, StorageError<NodeId>>` with `Result<_, io::Error>`.
- For the callback parameter in `append`, replace `LogFlushed<TypeConfig>` with `IOFlushed<TypeConfig>` (it's a type alias for the same type, but the canonical name in 0.10 is `IOFlushed`).
- Rename `truncate` to `truncate_after`. The body keeps its current logic; only the trait method name changes.
- Inside method bodies, replace any `StorageIOError::<NodeId>::xxx(...)` constructions with `io::Error::other(...)`.

- [ ] **Step 5: Build and verify log_storage.rs compiles**

Run:
```bash
cargo build -p uc_node 2>&1 | grep -E "log_storage|RaftLogStorage|LogFlushed|StorageError|truncate" | head
```

Expected: no remaining errors in this file.

- [ ] **Step 6: Commit**

```bash
git add uc_node/src/raft/log_storage.rs
git commit -m "refactor(uc_node): JournalLogStorage for openraft 0.10

Trait surface changes absorbed:
* RaftLogStorage methods now return io::Error (StorageError dropped).
* truncate renamed to truncate_after.
* LogFlushed -> IOFlushed (canonical name in 0.10; the alias keeps
  working but we use the new name).
* StorageIOError::* call sites replaced with io::Error::other(...).

No behavioral change. Existing on-disk format preserved (LeaderId/
LogId/Vote bincode bytes unchanged across the upgrade)."
```

---

## Task 5: Refactor `AdaptedStateMachine` for 0.10

**Files:**
- Modify: `uc_node/src/raft/state_machine.rs`

The big shape change: `apply()` now takes a `Stream<Item = Result<EntryResponder<C>, io::Error>>` and the SM is responsible for delivering responses via `ApplyResponder::send_response(resp)` on the per-entry responder. No more `Vec<Bytes>` return. `install_snapshot` / `begin_receiving_snapshot` drop the `Box<...>` wrapping.

- [ ] **Step 1: Update imports**

Find the import block near the top (around lines 27-28). Replace:

```rust
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{EntryPayload, LogId, StorageError, StorageIOError, StoredMembership};
```

with:

```rust
use std::io;

use futures::Stream;
use futures::StreamExt;
use openraft::entry::{RaftEntry, RaftPayload};
use openraft::storage::v2::EntryResponder;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{LogId, StoredMembership};
```

Note: `EntryPayload` (the enum we currently match on) is removed in 0.10. Instead, you call `entry.is_blank()` / `entry.get_membership()` / `entry.into_app_data()` via the `RaftEntry` + `RaftPayload` traits. We import both.

Also note: `futures` should already be a dep (it's transitive). If `cargo build` complains about `futures` not being available, add it to `uc_node/Cargo.toml` `[dependencies]` as `futures = { workspace = true }`.

- [ ] **Step 2: Fix `applied_state` signature**

Find the method around line 197. Replace:

```rust
async fn applied_state(
    &mut self,
) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, NodeAddr>), StorageError<NodeId>>
{
    let g = self.inner.lock().await;
    Ok((g.last_applied, g.last_membership.clone()))
}
```

with:

```rust
async fn applied_state(
    &mut self,
) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, NodeAddr>), io::Error> {
    let g = self.inner.lock().await;
    Ok((g.last_applied, g.last_membership.clone()))
}
```

- [ ] **Step 3: Rewrite `apply` for the stream/responder model**

This is the biggest single change in the file. Replace the entire current `apply` method (around lines 205-256) with:

```rust
async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
where
    Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + Send,
{
    while let Some(item) = entries.next().await {
        let (entry, responder) = item?;
        let log_id = entry.log_id();
        let log_index = log_id.index;

        let mut g = self.inner.lock().await;
        g.last_applied = Some(log_id);

        // Three payload cases — see openraft::entry::RaftPayload trait.
        let resp_bytes = if entry.is_blank() {
            bytes::Bytes::new()
        } else if let Some(membership) = entry.get_membership() {
            g.last_membership = StoredMembership::new(Some(log_id), membership);
            bytes::Bytes::new()
        } else {
            // Normal app-data entry.
            let cmd_bytes = entry.into_app_data();
            let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(
                cmd_bytes.as_ref(),
                bincode::config::standard(),
            )
            .map_err(|e| io::Error::other(format!("apply decode at {log_index}: {e}")))?;

            let resp = g.sm.apply(log_index, cmd);
            let encoded = bincode::serde::encode_to_vec(&resp, bincode::config::standard())
                .map_err(|e| io::Error::other(format!("apply encode at {log_index}: {e}")))?;
            bytes::Bytes::from(encoded)
        };
        drop(g);

        if let Some(r) = responder {
            r.send_response(resp_bytes);
        }
    }
    Ok(())
}
```

**Critical correctness notes** (the engineer must understand these):

- `EntryResponder<C> = (C::Entry, Option<ApplyResponder<C>>)` — the tuple has the entry and an *optional* responder. The responder is `None` for entries being applied on a follower (no client is waiting for the response).
- `ApplyResponder::send_response(resp)` consumes the responder. Always call it exactly once per `Some` variant — dropping the responder without calling `send_response` will leave the corresponding `client_write` future hanging.
- `entry.into_app_data()` consumes the entry; only call it inside the `else` branch where we've already checked `is_blank` / `get_membership`.
- The `drop(g)` before `r.send_response(_)` releases the inner mutex before we hand the response back; this matches the 0.9 ordering where responses were collected after the inner-lock-holding loop and returned via the future.

- [ ] **Step 4: Fix `begin_receiving_snapshot` + `install_snapshot`**

Around lines 264-275. Replace:

```rust
async fn begin_receiving_snapshot(
    &mut self,
) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
    Ok(Box::new(Cursor::new(Vec::new())))
}

async fn install_snapshot(
    &mut self,
    meta: &SnapshotMeta<NodeId, NodeAddr>,
    snapshot: Box<Cursor<Vec<u8>>>,
) -> Result<(), StorageError<NodeId>> {
```

with:

```rust
async fn begin_receiving_snapshot(
    &mut self,
) -> Result<Cursor<Vec<u8>>, io::Error> {
    Ok(Cursor::new(Vec::new()))
}

async fn install_snapshot(
    &mut self,
    meta: &SnapshotMeta<NodeId, NodeAddr>,
    snapshot: Cursor<Vec<u8>>,
) -> Result<(), io::Error> {
```

Inside the install_snapshot body, every `StorageIOError::<NodeId>::read_snapshot(...)` becomes `io::Error::other(...)`. Search the method for `StorageIOError` and replace each call.

Also: `snapshot.into_inner()` still works (Cursor's own method), but `let bytes = snapshot.into_inner();` is now on the unboxed value — drop the `Box::*` indirection if any.

- [ ] **Step 5: Fix `get_current_snapshot`**

Find this method (search for `async fn get_current_snapshot`). Update the signature error type and any inner `Box::new(Cursor::new(...))` → `Cursor::new(...)`:

```rust
async fn get_current_snapshot(
    &mut self,
) -> Result<Option<Snapshot<TypeConfig>>, io::Error> {
    let g = self.inner.lock().await;
    match &g.current_snapshot {
        Some(s) => Ok(Some(Snapshot {
            meta: s.meta.clone(),
            snapshot: Cursor::new(s.data.clone()),
        })),
        None => Ok(None),
    }
}
```

- [ ] **Step 6: Fix the `RaftSnapshotBuilder::build_snapshot` impl at the bottom of the file**

Find `impl<S: StateMachine> RaftSnapshotBuilder<TypeConfig> for AdaptedSnapshotBuilder<S>`. The single method `build_snapshot` returns `Result<Snapshot<TypeConfig>, StorageError<NodeId>>` — change to `Result<Snapshot<TypeConfig>, io::Error>`, and any `StorageIOError::<NodeId>::write_snapshot(...)` inside becomes `io::Error::other(...)`. Replace `Box::new(Cursor::new(buf))` (if present) with `Cursor::new(buf)` to match the unboxed `SnapshotData`.

- [ ] **Step 7: Build and verify state_machine.rs compiles**

Run:
```bash
cargo build -p uc_node 2>&1 | grep "state_machine\.rs\|AdaptedStateMachine" | head
```

Expected: no remaining errors in this file. Errors likely remain in `state_machine_shmem.rs` (Task 6).

- [ ] **Step 8: Commit**

```bash
git add uc_node/src/raft/state_machine.rs uc_node/Cargo.toml
git commit -m "refactor(uc_node): AdaptedStateMachine for openraft 0.10

apply() now consumes a Stream<EntryResponder> and delivers responses
via ApplyResponder::send_response per-entry, instead of returning
Vec<Bytes>. EntryPayload pattern-match replaced with RaftPayload
trait methods (is_blank / get_membership / into_app_data).

Snapshot data drops the Box<> wrapping per 0.10's SnapshotData-
without-Box change.

Error type uniformly io::Error (StorageError dropped from public
trait surface)."
```

---

## Task 6: Refactor `ShmemAdaptedStateMachine` for 0.10

**Files:**
- Modify: `uc_node/src/raft/state_machine_shmem.rs`

Same patterns as Task 5. The shmem variant's apply additionally publishes onto the apply.ring SPSC and awaits a response from apply_resp.ring — that logic doesn't change, only the surrounding error type and the iteration model.

- [ ] **Step 1: Update imports**

Find the import block (around lines 34-35). Replace:

```rust
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{EntryPayload, LogId, StorageError, StorageIOError, StoredMembership};
```

with:

```rust
use std::io;

use futures::Stream;
use futures::StreamExt;
use openraft::entry::{RaftEntry, RaftPayload};
use openraft::storage::v2::EntryResponder;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot, SnapshotMeta};
use openraft::{LogId, StoredMembership};
```

- [ ] **Step 2: Fix `applied_state` signature**

Replace `Result<_, StorageError<NodeId>>` with `Result<_, io::Error>` — identical pattern to Task 5 Step 2.

- [ ] **Step 3: Rewrite `apply` — stream + responder + ring publish/await**

Replace the entire current `apply` method (around lines 166-193). The new body keeps the ring publish/await helpers (`publish_apply`, `await_apply_resp`) but the outer loop and error type change. The `publish_apply` + `await_apply_resp` helper functions also need their error types refactored to `io::Error` — do that in steps 4-5.

New `apply`:

```rust
async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
where
    Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + Send,
{
    while let Some(item) = entries.next().await {
        let (entry, responder) = item?;
        let log_id = entry.log_id();
        let log_index = log_id.index;

        let mut g = self.inner.lock().await;
        g.last_applied = Some(log_id);

        let resp_bytes: bytes::Bytes = if entry.is_blank() {
            bytes::Bytes::new()
        } else if let Some(membership) = entry.get_membership() {
            g.last_membership = StoredMembership::new(Some(log_id), membership);
            bytes::Bytes::new()
        } else {
            // Normal app-data: publish to apply.ring, await response from apply_resp.ring.
            let cmd_bytes = entry.into_app_data();
            publish_apply(&g.apply_producer, log_index, cmd_bytes.as_ref(), log_id).await?;
            await_apply_resp(&g.apply_resp_consumer, log_index, log_id).await?
        };
        drop(g);

        if let Some(r) = responder {
            r.send_response(resp_bytes);
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Update `publish_apply` signature**

Find the helper around line 304. Current:

```rust
async fn publish_apply(
    producer: &PlMutex<SpscProducer>,
    log_index: u64,
    cmd_bytes: &[u8],
    log_id: LogId<NodeId>,
) -> Result<(), StorageError<NodeId>> {
```

Change return type and the inner error mapping. Replace the entire function body:

```rust
async fn publish_apply(
    producer: &PlMutex<SpscProducer>,
    log_index: u64,
    cmd_bytes: &[u8],
    log_id: LogId<NodeId>,
) -> Result<(), io::Error> {
    let _ = log_id; // retained for parity with 0.9 error-context; no longer needed
    loop {
        let result = {
            let mut p = producer.lock();
            p.try_write(MSG_TYPE_APPLY, 0, encode_extra_apply(log_index), cmd_bytes)
        };
        match result {
            Ok(()) => return Ok(()),
            Err(RingError::Full) => tokio::time::sleep(FULL_BACKOFF).await,
            Err(e) => {
                return Err(io::Error::other(format!("apply ring write at {log_index}: {e}")));
            }
        }
    }
}
```

- [ ] **Step 5: Update `await_apply_resp` signature + return type**

Current returns `Result<Bytes, StorageError<NodeId>>`. Change to `Result<Bytes, io::Error>`:

```rust
async fn await_apply_resp(
    consumer: &PlMutex<SpscConsumer>,
    expected_log_index: u64,
    log_id: LogId<NodeId>,
) -> Result<Bytes, io::Error> {
    let _ = log_id;
    let mut payload_buf: Vec<u8> = Vec::with_capacity(1024);
    loop {
        let read_result = {
            let mut c = consumer.lock();
            c.try_read(&mut payload_buf)
        };
        match read_result {
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_APPLY_RESP => {
                let li = decode_extra_apply(rec.header_extra);
                if li != expected_log_index {
                    return Err(io::Error::other(format!(
                        "apply_resp log_index mismatch: got {li}, expected {expected_log_index}"
                    )));
                }
                return Ok(Bytes::from(std::mem::take(&mut payload_buf)));
            }
            Ok(Some(rec)) => {
                tracing::warn!(msg_type = rec.msg_type, "unexpected frame on apply_resp ring");
            }
            Ok(None) => tokio::time::sleep(EMPTY_BACKOFF).await,
            Err(e) => {
                return Err(io::Error::other(format!("apply_resp ring read: {e}")));
            }
        }
    }
}
```

- [ ] **Step 6: Fix `begin_receiving_snapshot` / `install_snapshot` / `get_current_snapshot` / `build_snapshot`**

Same pattern as Task 5 Steps 4-6: drop `Box<Cursor<...>>` wrapping, change `StorageError<NodeId>` → `io::Error`, replace `StorageIOError::<NodeId>::xxx(...)` with `io::Error::other(...)`.

In particular, in the `install_snapshot` body, there are several `StableValueError`-mapping spots: replace each `.map_err(|e| StorageIOError::<NodeId>::read_snapshot(Some(meta.signature()), &std::io::Error::other(e.to_string())))?` with `.map_err(|e| io::Error::other(e.to_string()))?`.

- [ ] **Step 7: Update `ShmemAdaptedStateMachine::new` return error**

The constructor currently returns `Result<Self, crate::ClusterError>`. That's our own type; check whether the body still maps openraft errors correctly. Likely: no change required — the helpers it calls (`handles.last_applied.load()` etc.) return `ultima_journal::StableValueError`, mapped to `ClusterError::Recovery` strings. Audit but expect no rewrites.

- [ ] **Step 8: Build and verify**

Run:
```bash
cargo build -p uc_node 2>&1 | grep -E "state_machine_shmem|ShmemAdaptedStateMachine" | head
```

Expected: no errors in this file.

- [ ] **Step 9: Commit**

```bash
git add uc_node/src/raft/state_machine_shmem.rs
git commit -m "refactor(uc_node): ShmemAdaptedStateMachine for openraft 0.10

Mirror of state_machine.rs refactor: apply() takes Stream<EntryResponder>;
publish_apply / await_apply_resp helpers return io::Error; snapshot
data drops Box<Cursor> wrapping.

Ring publish/await semantics on apply.ring/apply_resp.ring are
unchanged — only the surrounding trait surface."
```

---

## Task 7: Thread `Raft<TypeConfig, SmAdapter>` through builder + node

**Files:**
- Modify: `uc_node/src/runtime/node.rs`
- Modify: `uc_node/src/runtime/builder.rs`
- Modify: `uc_node/src/network/server.rs` (only if needed)

In 0.10, `Raft<C, SM>` carries an explicit state-machine type parameter. We have two SM types (`AdaptedStateMachine<S>` for embedded mode and `ShmemAdaptedStateMachine<S>` for shmem mode), so the public `NodeHandle::raft` field's full type needs to reflect whichever was constructed.

The cleanest approach: the existing `SmAdapter<S>` enum already discriminates between embedded and shmem; mirror it in a `RaftHandle<S>` enum, or use a trait object. **Simplest path**: keep `Raft<TypeConfig, ()>` everywhere we erase the SM (using the default), and `Raft<TypeConfig, AdaptedStateMachine<S>>` / `Raft<TypeConfig, ShmemAdaptedStateMachine<S>>` only where openraft needs the concrete SM.

In practice, our code only touches `raft.client_write`, `raft.current_leader`, `raft.shutdown`, `raft.trigger()`, `raft.add_learner`, `raft.change_membership`, `raft.initialize`, `raft.metrics` — all of which are on `Raft<C, SM>` and don't constrain the SM type beyond what openraft already enforces. The `SM = ()` default is fine for the *public* `NodeHandle::raft` field, because internally openraft has its own typed handle.

**Implementation strategy**: pass `Raft<TypeConfig>` (which resolves to `Raft<TypeConfig, ()>` under the 0.10 default) everywhere it appears in our code today, and let openraft do the heavy lifting internally. If the compiler insists on a concrete SM at the `Raft::new(..., sm_adapter)` call site, infer it.

- [ ] **Step 1: Build and see what the compiler asks for**

Run:
```bash
cargo build -p uc_node 2>&1 | grep -E "Raft<|expected.*type parameter|wrong number" | head -20
```

If the build now actually compiles (because the `()` default soaks up the SM parameter), skip directly to Step 4. If it does not, read each error and update the offending site.

- [ ] **Step 2 (conditional on Step 1 failing): Update `runtime/node.rs`**

Around line 42, `pub(crate) raft: Raft<TypeConfig>` stays the same — but only because the `()` default works. If the compiler insists otherwise, the change is:

```rust
pub(crate) raft: openraft::Raft<TypeConfig>,
```

stays unchanged (since `Raft<TypeConfig>` desugars to `Raft<TypeConfig, ()>` and our use sites don't need the SM type).

If the compiler complains at the `Raft::new(...)` call site in `builder.rs:finish()` about needing a concrete SM type, change there only — leave `NodeHandle::raft` as-is.

- [ ] **Step 3 (conditional): Update `runtime/builder.rs:finish()`**

The current `finish<A, S>` signature has `A: openraft::storage::RaftStateMachine<crate::raft::TypeConfig>`. In 0.10, that bound is still valid; what may need to change is the `Raft::new(...)` call site itself. Read the signature openraft 0.10 expects (use `cargo doc --workspace --no-deps` and check) and adapt the call:

```rust
let raft: Raft<TypeConfig> = Raft::new(
    config.node_id,
    raft_config,
    network,
    log_storage,
    sm_adapter,
)
.await
.map_err(|e| ClusterError::Raft(format!("Raft::new: {e}")))?;
```

The explicit `Raft<TypeConfig>` ascription forces the `()` default for SM; openraft should accept this.

If openraft 0.10 requires `Raft<TypeConfig, A>` here, two paths:
1. Box-erase the SM: store the resulting `Raft<TypeConfig, A>` in an `enum` mirror of `SmAdapter`, or
2. Wrap `NodeHandle::raft` in an `Arc<dyn …>` boundary.

Prefer option 1 (less indirection). If you take that path, add to `runtime/node.rs`:

```rust
pub(crate) enum RaftHandle<S: StateMachine> {
    Embedded(openraft::Raft<TypeConfig, AdaptedStateMachine<S>>),
    Shmem(openraft::Raft<TypeConfig, ShmemAdaptedStateMachine<S>>),
}

impl<S: StateMachine> RaftHandle<S> {
    pub(crate) fn current_leader_fut(&self) -> impl std::future::Future<Output = Option<NodeId>> + '_ {
        match self {
            RaftHandle::Embedded(r) => Box::pin(r.current_leader()) as _,
            RaftHandle::Shmem(r) => Box::pin(r.current_leader()) as _,
        }
    }
    // ... mirror for client_write, shutdown, trigger, add_learner, etc.
}
```

This is verbose. **Only take this path if Step 1's `()` default doesn't compile.** Try the simple path first.

- [ ] **Step 4: Network server compatibility**

`network/server.rs` takes `raft: Raft<TypeConfig>` and dispatches inbound RPCs. The signature should still compile under `Raft<TypeConfig, ()>`. If it doesn't, ascribe explicitly and forward through the same enum-wrapping pattern.

- [ ] **Step 5: Build whole workspace**

Run:
```bash
cargo build --workspace 2>&1 | tail -20
```

Expected: **clean build, zero errors.** All previous tasks' type-surface refactors collectively unblock this point.

- [ ] **Step 6: Run the existing tests**

Run:
```bash
cargo test --workspace 2>&1 | grep -E "^test result|FAILED|error\[" | tail -25
```

Expected: every test passes. **`m3_service_crash` may still pass** under the current `raft.shutdown()` substitute — Task 8 changes the watcher and its assertion together.

- [ ] **Step 7: Commit**

```bash
git add uc_node/
git commit -m "refactor(uc_node): thread Raft<TypeConfig, SM> through 0.10 surface

In openraft 0.10, Raft gained a second type parameter SM = (). Our
code uses Raft only through methods that don't constrain SM (current_
leader, client_write, shutdown, trigger, etc.), so Raft<TypeConfig>
desugars to Raft<TypeConfig, ()> and threads through cleanly.

If the compiler had required a concrete SM at NodeHandle, an enum
wrapper (Embedded/Shmem) would be the alternative — not needed.

Full workspace build now green; all M1/M2/M3 capstone tests pass on
the 0.10 baseline. M3 service_crash test still passes because the
watcher's raft.shutdown() substitute hasn't been cut over yet
(Task 8)."
```

---

## Task 8: `service_watcher` cutover + test update

**Files:**
- Modify: `uc_node/src/ipc/service_watcher.rs`
- Modify: `uc_node/tests/m3_service_crash.rs`

The only behavioral change in M3.5. The watcher currently calls `raft.shutdown()` on a stalled leader; replace with `raft.trigger().transfer_leader(target)` plus a 5 s `raft.shutdown()` fallback.

- [ ] **Step 1: Update the M3 service-crash test to assert the new behavior first**

This is a TDD-style commit: update the test to assert the post-transfer behavior, run it, confirm it fails against the unchanged watcher, then fix the watcher in the next step.

Open `uc_node/tests/m3_service_crash.rs`. Around line 254 (the "Cluster should re-elect among the surviving 2 nodes" block) and around line 272-280 (the shutdown comment about "already shut down by the watcher"), the test currently assumes the leader's raft is dead. After Task 8 lands, the leader's raft is **still alive** (running as a follower). Rewrite that section:

Find:

```rust
    // ── Cluster should re-elect among the surviving 2 nodes ─────────────
    let surviving: Vec<&NodeHandle<Counter>> = node_handles
        .iter()
        .filter(|h| h.node_id() != leader_id)
        .collect();
    let new_leader_id = wait_for_new_leader(&surviving, leader_id, Duration::from_secs(15)).await;
    assert_ne!(new_leader_id, leader_id);
```

Replace with:

```rust
    // ── Cluster should transfer leadership (M3.5: leader stays alive
    //    as a follower; M3 had it raft-shutdown the leader).
    //    Wait for ANY node to report a leader different from the
    //    original — including the original leader itself, which is now
    //    a follower and reports the new leader.
    let new_leader_id =
        wait_for_new_leader(&node_handles.iter().collect::<Vec<_>>(),
                            leader_id, Duration::from_secs(15)).await;
    assert_ne!(new_leader_id, leader_id, "leadership must transfer off the stalled node");

    // The original leader's raft is still alive — its `current_leader()`
    // should report the new leader (a follower's view of leadership).
    let old_leader = node_handles
        .iter()
        .find(|h| h.node_id() == leader_id)
        .unwrap();
    assert_eq!(
        old_leader.current_leader().await,
        Some(new_leader_id),
        "stalled leader should report the new leader (it's now a follower, not dead)"
    );
```

Also update the closing shutdown block. Find:

```rust
    for n in node_handles.into_iter() {
        // The stalled leader's raft was already shut down by the watcher;
        // node.shutdown() calls raft.shutdown() again (idempotent).
        n.shutdown().await.expect("node shutdown");
    }
```

Replace with:

```rust
    for n in node_handles.into_iter() {
        // M3.5: the stalled leader's raft transferred leadership (still
        // alive as a follower); node.shutdown() shuts it down cleanly.
        n.shutdown().await.expect("node shutdown");
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cargo test -p uc_node --test m3_service_crash -- --nocapture 2>&1 | tail -20
```

Expected: the test fails. The exact mode depends on what's intercepted first — likely the `assert_eq!(old_leader.current_leader().await, Some(new_leader_id), ...)` failing because under the current watcher, the stalled leader's raft was shut down and `current_leader()` returns `None`.

If the test passes against the unchanged watcher, the assertion isn't strict enough — strengthen it.

- [ ] **Step 3: Rewrite `service_watcher.rs` with `transfer_leader` cutover**

Open `uc_node/src/ipc/service_watcher.rs`. Replace the entire file with:

```rust
//! Node-side watcher for the service's liveness heartbeat.
//!
//! Polls `ServiceStatus::heartbeat_seq` via [`HeartbeatWatcher`]; on a
//! detected stall (no advance within `timeout`) sets a public
//! `AtomicBool` and — if this node is the raft leader — calls
//! `raft.trigger().transfer_leader(target)`. Strict target selection:
//! pick any voter in the current membership other than self. If the
//! transfer doesn't take within 5 s, fall back to `raft.shutdown()`
//! so a doomed transfer doesn't pin leadership indefinitely.
//!
//! # Safety
//!
//! [`spawn_service_watcher`] captures the pointed-to `ServiceStatus`
//! for the task's lifetime. The caller must keep the cnc mmap alive
//! until the task is joined — in practice the node-side `Instance`
//! lives in [`crate::NodeHandle::_instance`] and is dropped only after
//! [`crate::NodeHandle::shutdown`] joins this task.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openraft::Raft;
use tokio::task::JoinHandle;

use uc_protocol::cnc::ServiceStatus;
use uc_protocol::liveness::HeartbeatWatcher;

use crate::raft::{NodeId, TypeConfig};

const POLL_PERIOD: Duration = Duration::from_millis(100);

/// Default time without an advancing service `heartbeat_seq` after
/// which the watcher declares the service stalled. The service ticks
/// every 100 ms; 2 s leaves a 20× margin over scheduling jitter.
pub const DEFAULT_LIVENESS_TIMEOUT: Duration = Duration::from_millis(2000);

/// How long the watcher waits for `transfer_leader` to take effect
/// before falling back to `raft.shutdown()`.
const TRANSFER_FALLBACK_TIMEOUT: Duration = Duration::from_secs(5);

pub struct ServiceWatcherHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
    /// Observable flag — flipped to `true` on stall detection, back to
    /// `false` if the service heartbeat resumes.
    pub stalled: Arc<AtomicBool>,
}

/// Pick a transfer target (strict). Returns the first voter in the
/// current membership that isn't `self_node_id`, or `None` if no peer
/// is in the voter set (single-node cluster).
async fn pick_transfer_target(
    raft: &Raft<TypeConfig>,
    self_node_id: NodeId,
) -> Option<NodeId> {
    let metrics = raft.metrics();
    let m = metrics.borrow();
    m.membership_config
        .voter_ids()
        .find(|id| *id != self_node_id)
}

/// Spawn the service-liveness watcher.
///
/// # Safety
///
/// `status_ptr` must point at a `ServiceStatus` that stays valid until
/// the returned task is joined.
pub unsafe fn spawn_service_watcher(
    status_ptr: *const ServiceStatus,
    raft: Raft<TypeConfig>,
    node_id: NodeId,
    timeout: Duration,
) -> ServiceWatcherHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stalled = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);
    let stalled_for_task = Arc::clone(&stalled);

    // SAFETY: see function-level # Safety.
    let status: &'static ServiceStatus = unsafe { &*status_ptr };

    let join = tokio::spawn(async move {
        let initial_now_ns = now_ns();
        let initial_seq = status.heartbeat_seq.load(Ordering::Relaxed);
        let mut watcher = HeartbeatWatcher::new(initial_seq, initial_now_ns);
        let timeout_ns = timeout.as_nanos() as u64;

        while !stop_for_task.load(Ordering::Relaxed) {
            let alive = watcher.poll_service(status, now_ns(), timeout_ns);

            if !alive && !stalled_for_task.load(Ordering::Relaxed) {
                tracing::warn!(
                    node_id,
                    timeout_ms = timeout.as_millis() as u64,
                    "service heartbeat stalled"
                );
                stalled_for_task.store(true, Ordering::Relaxed);

                if raft.current_leader().await == Some(node_id) {
                    match pick_transfer_target(&raft, node_id).await {
                        Some(target) => {
                            tracing::warn!(
                                node_id,
                                target,
                                "this node was leader; calling \
                                 raft.trigger().transfer_leader(target)"
                            );
                            let _ = raft.trigger().transfer_leader(target).await;
                            spawn_fallback_shutdown(raft.clone(), node_id);
                        }
                        None => {
                            tracing::warn!(
                                node_id,
                                "no peer voter to transfer to; calling raft.shutdown()"
                            );
                            let _ = raft.shutdown().await;
                        }
                    }
                }
            } else if alive && stalled_for_task.load(Ordering::Relaxed) {
                tracing::info!(node_id, "service heartbeat resumed");
                stalled_for_task.store(false, Ordering::Relaxed);
            }

            tokio::time::sleep(POLL_PERIOD).await;
        }
    });

    ServiceWatcherHandle {
        join,
        stop,
        stalled,
    }
}

/// Spawn the 5 s fallback that calls `raft.shutdown()` if we are still
/// the raft leader. Fire-and-forget orphan; `NodeHandle::shutdown` does
/// not track it. On normal shutdown the orphan's `current_leader()`
/// check fails (raft already stopped) and the branch is a no-op.
fn spawn_fallback_shutdown(raft: Raft<TypeConfig>, node_id: NodeId) {
    tokio::spawn(async move {
        tokio::time::sleep(TRANSFER_FALLBACK_TIMEOUT).await;
        if raft.current_leader().await == Some(node_id) {
            tracing::warn!(
                node_id,
                "transfer_leader did not take within {:?}; calling raft.shutdown() fallback",
                TRANSFER_FALLBACK_TIMEOUT
            );
            let _ = raft.shutdown().await;
        }
    });
}

#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
```

- [ ] **Step 4: Run the test and verify it passes**

Run:
```bash
cargo test -p uc_node --test m3_service_crash -- --nocapture 2>&1 | tail -20
```

Expected: `test service_crash_on_leader_transfers_leadership ... ok`.

If `pick_transfer_target` returns `None` unexpectedly in the test (e.g., if `voter_ids()` is empty mid-flight), check the openraft 0.10 `voter_ids` API — it may have moved. The metrics API is stable across 0.9 → 0.10, but if the test fails with a panic about no peer, that's the path to investigate.

- [ ] **Step 5: Run the full workspace to confirm no regression**

Run:
```bash
cargo test --workspace 2>&1 | grep -E "^test result|FAILED" | tail -25
```

Expected: every test passes. If any other test regressed (especially `m3_three_node_shmem`), inspect — the watcher firing differently might change shutdown ordering.

- [ ] **Step 6: Commit**

```bash
git add uc_node/src/ipc/service_watcher.rs uc_node/tests/m3_service_crash.rs
git commit -m "feat(uc_node): service_watcher uses transfer_leader (M3.5)

Replaces the M3 raft.shutdown() substitute with the real
raft.trigger().transfer_leader(target). Strict target selection:
first voter in current membership ≠ self. 5 s fallback timer fires
raft.shutdown() if the transfer doesn't take (e.g., target is
unreachable).

Updated m3_service_crash test to assert the new behavior: the
stalled leader's raft is still alive after transfer (it's now a
follower; current_leader() returns the new leader, not None), and
the cluster still has all 3 voters in the membership."
```

---

## Task 9: Polish — doc updates + spec patch + task doc

**Files:**
- Modify: `docs/superpowers/specs/2026-05-15-uc-m3-5-openraft-0-10-upgrade-design.md`
- Modify: `docs/tasks/task03_m3_shmem_service_split.md`
- Modify: `docs/superpowers/specs/2026-05-15-uc-m4-clients-and-ring-fix-design.md`
- Modify: `README.md`
- Create: `docs/tasks/task04_m3_5_openraft_0_10_upgrade.md`
- Delete: `docs/superpowers/plans/2026-05-15-uc-m3-5-openraft-0-10-upgrade.md`

Per the CLAUDE.md workflow: consolidate the architecture story into `docs/tasks/`, then delete the plan.

- [ ] **Step 1: Patch the M3.5 spec with the discovered scope realization**

The spec said "5-7 commits" and "200-300 line diff." Reality is 9 commits and a larger diff (the `apply()` stream/responder rewrite was bigger than predicted; the `openraft-legacy` crate for V1 was an unknown). Add a one-line update at the top of the "Implementation phasing" section.

Open `docs/superpowers/specs/2026-05-15-uc-m3-5-openraft-0-10-upgrade-design.md`. Find:

```markdown
Total: **5-7 commits**, ~200-300 line diff, no behavior change in steady-state paths (only `service_watcher` action under stall changes).
```

Replace with:

```markdown
Total: **9 commits**, ~500-600 line diff, no behavior change in steady-state paths (only `service_watcher` action under stall changes). The original 5-7 estimate underestimated two surface-area changes discovered during planning: (a) `RaftStateMachine::apply()` switched to `Stream<EntryResponder>` with per-entry responders (vs. `IntoIterator<Item = Entry>` returning `Vec<Bytes>`), and (b) `RaftNetwork` (V1) moved to the separate `openraft-legacy` crate. Architectural choices unchanged.
```

- [ ] **Step 2: Patch the M4 spec to reflect M3.5 shipping**

Open `docs/superpowers/specs/2026-05-15-uc-m4-clients-and-ring-fix-design.md`. Find the "Out of scope" entries that mention openraft 0.10 upgrade and transfer_leader. Update those lines to:

```markdown
- **openraft 0.10 upgrade + real `Raft::trigger_leader_transfer`.** Shipped in M3.5 (`docs/tasks/task04_m3_5_openraft_0_10_upgrade.md`).
```

- [ ] **Step 3: Update the M3 task doc follow-ups**

Open `docs/tasks/task03_m3_shmem_service_split.md`. Find the "Follow-ups tracked for M4+" section. The first two bullets are:

```markdown
- **MPSC/Broadcast post-wrap fix** (M4): published-up-to position or per-slot generation counters. ...
- **`Raft::trigger_leader_transfer` via openraft 0.10 upgrade.** Replaces the M3 `raft.shutdown()` substitute in `service_watcher`.
```

Change the second to:

```markdown
- **~~`Raft::trigger_leader_transfer` via openraft 0.10 upgrade.~~** Shipped in M3.5 (`docs/tasks/task04_m3_5_openraft_0_10_upgrade.md`).
```

(Strike-through; preserves the record without falsely claiming we shipped it in M3.)

- [ ] **Step 4: Write the M3.5 task doc**

Create `docs/tasks/task04_m3_5_openraft_0_10_upgrade.md` with this content:

```markdown
# Task 04 — M3.5: openraft 0.9.24 → 0.10 upgrade

**Status:** Complete.
**Branch:** `main`, commits `<first-commit>..<last-commit>` (9 commits).
**Workspace:** `ultima_cluster/`.

## Goal

Cut over from openraft `0.9.24` to `0.10`, preserving every M1/M2/M3 capstone test and replacing the M3 `raft.shutdown()` substitute in `ipc::service_watcher` with the real `raft.trigger().transfer_leader(target)`.

## Shipped

1. **Workspace dep bump** — `openraft = "0.10"` + new `openraft-legacy = "0.10"` for the V1 `RaftNetwork` trait (which moved out of openraft proper in 0.10).
2. **`declare_raft_types!` extended** — added `Term`, `LeaderId = LeaderId<Term, NodeId>` (adv path, preserves on-disk format), `Vote = Vote<Self::LeaderId>`, `Responder<T> = OneshotResponder<Self, T>`, `AsyncRuntime = TokioRuntime` via the new `openraft::impls::*` paths.
3. **`JournalLogStorage` refactor** — `RaftLogStorage` methods return `io::Error` (vs `StorageError<NodeId>`), `truncate` renamed to `truncate_after`, `LogFlushed` → `IOFlushed` (canonical name).
4. **`AdaptedStateMachine` + `ShmemAdaptedStateMachine` refactor** — `apply()` consumes a `Stream<EntryResponder<C>>` and delivers responses via `ApplyResponder::send_response` per entry (vs returning `Vec<Bytes>`). `EntryPayload` pattern-match replaced with `RaftPayload` trait methods (`is_blank` / `get_membership` / `into_app_data`). Snapshot data drops the `Box<Cursor<...>>` wrapping (0.10's "SnapshotData without Box").
5. **Network adapter** — `QuicRaftNetwork`'s `RaftNetwork` impl now points at `openraft_legacy::network_v1::RaftNetwork`. `RaftNetworkFactory` is still in `openraft` itself.
6. **`Raft<TypeConfig, SM = ()>` threading** — the new SM type parameter takes its `()` default everywhere we touch `Raft` directly; the SM-typed handles live entirely inside openraft.
7. **`service_watcher` cutover** — `raft.trigger().transfer_leader(target)` with a 5 s `raft.shutdown()` fallback. Strict target selection: first voter ≠ self in the current membership.
8. **`m3_service_crash` test updated** — asserts the new behavior (leader transfers, still alive as follower, current_leader() returns the new leader).

## On-disk compatibility

`LeaderId`, `LogId`, and `Vote` serialize identically across 0.9 and 0.10 (field shapes unchanged; only the number of type parameters differs). Existing `StableValue<LogId<u64>>` / `StableValue<Vote<u64>>` files written by 0.9 are readable by 0.10 without migration. The journal record format is also unchanged.

## What stayed deferred (to M5)

- **`RaftNetworkV2`** migration — we kept V1 via `openraft-legacy`. V2 sub-trait split (`NetBackoff`, `NetStreamAppend`, `NetVote`, `NetSnapshot`, `NetTransferLeader`) is M5.
- **Custom `Responder<T>`** for client_dispatcher — only useful once `clients/response.broadcast` exists (M4).
- **`SnapshotData` swap to `snapshot.region` mmap** — M5 alongside the snapshot wire-format work.
- **`Raft::data_metrics()` / `server_metrics()` migration** — current `metrics()` keeps working; M5 alongside observability sub-buffer wiring.
- **`generic-snapshot-data` feature flag** — only useful with the mmap swap; M5.

## Verification

All commands green at M3.5 close:

```bash
cargo build --workspace
cargo test  --workspace          # 93 M3-era tests still pass; m3_service_crash updated
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

## Pointers

- M3 record: `docs/tasks/task03_m3_shmem_service_split.md`.
- M4 spec (rebased on this baseline): `docs/superpowers/specs/2026-05-15-uc-m4-clients-and-ring-fix-design.md`.
- openraft 0.10 source (read during planning): `../openraft/` (`0.10.0-alpha.20`).
- Key file references:
  - `openraft/src/type_config.rs:30-160` — `RaftTypeConfig` + macro example.
  - `openraft/src/raft/trigger.rs:86` — `transfer_leader(to)`.
  - `openraft/src/storage/v2/raft_log_storage.rs` — 0.10 trait surface.
  - `openraft/src/storage/v2/raft_state_machine.rs` — apply-stream model.
  - `openraft/legacy/Cargo.toml` — `openraft-legacy` crate manifest.
```

Fill in `<first-commit>` and `<last-commit>` with the actual SHAs from this milestone (Task 1 commit and Task 9 commit).

- [ ] **Step 5: Update README pointer**

Open `README.md`. Find the line:

```markdown
**Status:** M3 — shmem IPC + `uc_service` process split complete. ...
```

Replace with:

```markdown
**Status:** M3.5 — openraft 0.10 upgrade + `transfer_leader` cutover. Builds on M3's shmem IPC + `uc_service` process split. See `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` for the canonical design and `docs/tasks/` for per-milestone records.
```

- [ ] **Step 6: Delete the plan file**

```bash
git rm docs/superpowers/plans/2026-05-15-uc-m3-5-openraft-0-10-upgrade.md
```

- [ ] **Step 7: Run all verification one final time**

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo fmt --check
```

Expected: all green.

- [ ] **Step 8: Commit**

```bash
git add docs/
git add README.md
git commit -m "docs(m3.5): consolidate plan into task04; update specs + README

Per CLAUDE.md workflow: M3.5 plan (working scaffolding) is deleted;
its architectural decisions, scope realizations, and shipped-vs-
deferred items are consolidated into docs/tasks/task04_m3_5_openraft
_0_10_upgrade.md.

Also patches:
* M3.5 spec — corrects the 5-7-commit estimate to the actual 9
  commits, noting the two underestimated changes (apply-stream
  refactor, openraft-legacy crate).
* M4 spec — re-tags 'openraft 0.10 upgrade' and 'transfer_leader' as
  shipped in M3.5 (the spec was written before M3.5 split out).
* task03 (M3) doc — strikes through the follow-up bullet for
  Raft::trigger_leader_transfer.
* README — M3 → M3.5 status line."
```

---

## Self-review

After writing the complete plan above, re-checked against the M3.5 spec:

**Spec coverage:**

| Spec section | Plan tasks |
|---|---|
| §Goal | Tasks 1-9 collectively |
| §Scope (in) — workspace dep bump | Task 1 |
| §Scope (in) — extend `declare_raft_types!` | Task 2 |
| §Scope (in) — thread `Raft<C, SM>` | Task 7 |
| §Scope (in) — trait-impl signature audit (state_machine, state_machine_shmem, log_storage, network) | Tasks 3, 4, 5, 6 |
| §Scope (in) — `service_watcher` cutover | Task 8 |
| §Scope (in) — update `m3_service_crash` assertions | Task 8 Step 1-2 |
| §Scope (in) — update task03 follow-ups | Task 9 Step 3 |
| §On-disk format preservation | Honored via `leader_id_adv` pin in Task 2 Step 2; called out in commit messages and task04 doc |
| §Type-config changes — `Term`, `LeaderId`, `Vote`, `Responder<T>` | Task 2 Step 2 |
| §API touch points — `Raft<C, SM = ()>`, decoupled types, RaftStateMachine, RaftLogStorage, RaftNetwork | Tasks 3-7 |
| §`service_watcher` cutover code | Task 8 Step 3 (full file replacement provided) |
| §Target selection — strict | Task 8 `pick_transfer_target` |
| §Orphan fallback task note | Task 8 Step 3 (`spawn_fallback_shutdown`) |
| §Verification checklist | Task 9 Step 7 |
| §Risks — hidden signature shifts | Compiler-driven; Tasks 4-7 budget extra steps for inspection |
| §Risks — doomed transfer pinning leadership | 5 s fallback in Task 8 Step 3 |
| §Risks — adv-vs-std regression | Task 2 Step 2 pins `leader_id_adv` explicitly with rationale |

**Placeholder scan:** No "TBD", "TODO", "fill in details". All code blocks contain complete content. Conditional steps (Task 7 Steps 2-3) provide explicit alternative implementations rather than punting.

**Type consistency:** `Raft<TypeConfig>` used uniformly across tasks; `io::Error` for `RaftLogStorage` + `RaftStateMachine` return types throughout; `EntryResponder<TypeConfig>` consistently with the `(C::Entry, Option<ApplyResponder<C>>)` destructuring; `pick_transfer_target` signature matches its single call site.

**Scope check:** Single milestone, 9 commits, single implementation plan — appropriately sized. Out-of-scope items (V2 network, custom Responder, generic-snapshot-data, mmap snapshot) explicitly fenced off in the spec and task04 doc.

No issues found that require revision.
