# Eventual Log Durability for `uc_node` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `uc_node`'s Raft log to Aeron-style eventual durability — acknowledge an append at the page-cache write (`Durability::Eventual`) with fsync off the commit critical path, durability provided by quorum replication — configurable per node, plus the recovery repair the change requires.

**Architecture:** Make `JournalLogStorage::open` default the *log* journal to `Eventual` (the six metadata `StableValue`s stay `Consistent`); the append path's existing `on_complete` ack already fires at the page-cache-write boundary in Eventual mode. Add a `NodeConfig.log_durability` knob (default Eventual) threaded through the builder. Repair the one new power-loss edge — a fsynced `committed` ahead of the eventual log's recovered tail — by clamping it at recovery. Rename a now-misleading probe and expose a durability-lag observability primitive.

**Tech Stack:** Rust 2024, `uc_node` / `uc_protocol` crates, `ultima_journal` (`Durability`, `Journal`, `StableValue`, `durable_seq` watermark), `openraft` 0.10.

**Working directory for all commands:** `/home/claude/ultima/ultima_cluster`
**Branch:** create `feat/eventual-log-durability` before Task 1 (see Task 0).
**Test/lint:** `cargo test -p uc_node` (targeted), `cargo test` (full in-process suite), `cargo clippy --workspace -- -D warnings`.

---

## File Structure

- `uc_node/src/raft/log_storage.rs` — **modify**: `open_with_durability` + `open` wrapper; `last_log_id_at` helper; `durability_lag` method.
- `uc_node/src/config.rs` — **modify**: `log_durability` field on `NodeConfig`.
- `uc_node/src/runtime/builder.rs` — **modify**: pass `config.log_durability` to open; call `reconcile`.
- `uc_node/src/runtime/recovery.rs` — **modify**: `assert_consistent` → `reconcile` + committed clamp.
- `uc_protocol/src/probes.rs` — **modify**: rename `JournalFsynced` → `JournalDurable`.
- `uc_node/tests/` — **modify/create**: update `NodeConfig` literals; new `eventual_durability.rs` (clamp + durability-lag + behavior + micro-measurement).
- `docs/tasks/task10_eventual_log_durability.md` — **create**: durability-model doc + measurement.

---

## Task 0: Branch

- [ ] **Step 1: Create the feature branch**

Run:
```bash
git checkout -b feat/eventual-log-durability
```
Expected: `Switched to a new branch 'feat/eventual-log-durability'`.

---

## Task 1: Default the log journal to Eventual (`open_with_durability` + wrapper)

The core behavioral change with minimal churn: `open(data_dir)` keeps its signature but now defaults to Eventual via a new `open_with_durability`. The full suite then runs under the new default — the safety net.

**Files:**
- Modify: `uc_node/src/raft/log_storage.rs`

- [ ] **Step 1: Split `open` into `open` (wrapper) + `open_with_durability`**

In `uc_node/src/raft/log_storage.rs`, find the current method signature:

```rust
    pub fn open(data_dir: &Path) -> Result<Self, ClusterError> {
        std::fs::create_dir_all(data_dir.join("journal"))?;

        let journal = Arc::new(Journal::open(JournalConfig {
            dir: data_dir.join("journal"),
            segment_size_bytes: SEGMENT_SIZE_BYTES,
            durability: Durability::Consistent,
        })?);
```

Replace those lines (the signature line and the `Journal::open` block's `durability:` field) so the method becomes parameterized and `open` delegates. Concretely, change the signature line `pub fn open(data_dir: &Path) -> Result<Self, ClusterError> {` to:

```rust
    /// Open with the default log durability (`Eventual` — Aeron `fileSyncLevel=0`
    /// model: ack on page-cache write, background fsync, durability via quorum
    /// replication). Use [`open_with_durability`] to choose `Consistent`.
    pub fn open(data_dir: &Path) -> Result<Self, ClusterError> {
        Self::open_with_durability(data_dir, Durability::Eventual)
    }

    /// Open the Raft log + metadata. `log_durability` controls ONLY the log
    /// journal; the metadata `StableValue`s are always `Consistent`.
    pub fn open_with_durability(
        data_dir: &Path,
        log_durability: Durability,
    ) -> Result<Self, ClusterError> {
        std::fs::create_dir_all(data_dir.join("journal"))?;

        let journal = Arc::new(Journal::open(JournalConfig {
            dir: data_dir.join("journal"),
            segment_size_bytes: SEGMENT_SIZE_BYTES,
            durability: log_durability,
        })?);
```

Leave the rest of the method body (all the `StableValue::open(... Durability::Consistent ...)` blocks and the `Ok(Self { ... })`) exactly as-is — it now closes `open_with_durability`.

- [ ] **Step 2: Build**

Run: `cargo build -p uc_node 2>&1 | tail -5`
Expected: compiles. `Durability` is already imported in this file (the `use ultima_journal::{Durability, ...}` line).

- [ ] **Step 3: Run the full in-process suite under the new Eventual default**

Run: `cargo test 2>&1 | tail -30`
Expected: ALL tests pass. Every existing test now exercises the Eventual log (process-level reopen + crash tests survive because the page cache survives a process exit). If any test fails, STOP and report which — it indicates a real fsync-durability dependency to discuss, not something to paper over.

- [ ] **Step 4: Lint**

Run: `cargo clippy --workspace -- -D warnings 2>&1 | tail -10`
Expected: zero warnings.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/raft/log_storage.rs
git commit -m "feat(uc_node): default Raft log journal to Eventual durability (Aeron fileSyncLevel=0 model)"
```

---

## Task 2: Make it configurable via `NodeConfig.log_durability`

Adds the operator knob and threads it through the builder. The field is required on a struct-literal-constructed `NodeConfig`, so every construction site must add it — the compiler enforces completeness.

**Files:**
- Modify: `uc_node/src/config.rs`
- Modify: `uc_node/src/runtime/builder.rs`
- Modify: all `NodeConfig { … }` construction sites (listed below)

- [ ] **Step 1: Add the field to `NodeConfig`**

In `uc_node/src/config.rs`, in `pub struct NodeConfig { … }` (line ~50), add this field after `service_rings`:

```rust
    /// Durability for the Raft log journal. `Eventual` (recommended/default) acks
    /// an append at the page-cache write, with fsync off the commit critical path
    /// — durability via quorum replication; survives process crash, **not**
    /// simultaneous quorum power loss (Aeron `fileSyncLevel=0`). `Consistent`
    /// fsyncs before ack (power-loss safe; Aeron `fileSyncLevel>=1`). Aeron sync
    /// levels 1 and 2 both map to `Consistent` (the journal fsyncs data+metadata).
    pub log_durability: ultima_journal::Durability,
```

Ensure `ultima_journal` is referenceable here (it is a dependency of `uc_node`; the fully-qualified `ultima_journal::Durability` needs no new `use`).

- [ ] **Step 2: Thread it through the builder**

In `uc_node/src/runtime/builder.rs`, change line 49 from:

```rust
        let log_storage = JournalLogStorage::open(&self.config.data_dir)?;
```

to:

```rust
        let log_storage = JournalLogStorage::open_with_durability(
            &self.config.data_dir,
            self.config.log_durability,
        )?;
```

- [ ] **Step 3: Build to enumerate every missing construction site**

Run: `cargo build --workspace --tests 2>&1 | grep -E "missing field|error\[" | head -40`
Expected: a list of `missing field log_durability in initializer of NodeConfig` errors, one per construction site. The sites are (add the field to each `NodeConfig { … }` literal):
- `uc_node/src/test_support.rs:75`
- `uc_node/tests/m1_single_node.rs:85`
- `uc_node/tests/m2_multi_node.rs:119`, `:294`, `:411`
- `uc_node/tests/m3_service_crash.rs:144`
- `uc_node/tests/m3_shmem_single_node.rs:96`, `:194`
- `uc_node/tests/m3_three_node_shmem.rs:149`
- `uc_node/tests/m3_ultima_db_adapter.rs:89`
- `uc_node/tests/m4_client_leader_failover.rs:158`
- `uc_node/tests/m4_client_response_overwritten.rs:108`
- `uc_node/tests/m4_client_three_node.rs:147`
- `uc_node/tests/m4_client_wrap.rs:101`
- `uc_node/tests/m5_output_leader_transition_replay.rs:166`
- `uc_node/tests/m5_output_apply_does_not_stall.rs:102`
- `uc_node/tests/m5_output_ring_backpressure_skip.rs:96`
- `uc_node/tests/m5_output_idempotent_replay.rs:115`
- `uc_node/tests/m5_output_permanent_advances_marker.rs:145`
- `uc_node/tests/m5_output_retryable_backoff.rs:128`
- `uc_node/tests/m5_output_smoke.rs:102`
- `examples/counter_loop/src/bin/counter_loop_service.rs:200`

- [ ] **Step 4: Add the field to every site**

In EACH `NodeConfig { … }` literal listed above, add this exact line (anywhere among the fields; alongside the other `*_rings` fields reads naturally):

```rust
        log_durability: ultima_journal::Durability::Eventual,
```

All current tests pass under Eventual (Task 1 proved it), so every site uses `Eventual`. If `ultima_journal` is not in scope in a given test/example file, use the fully-qualified path as written above (no `use` needed).

- [ ] **Step 5: Build until clean**

Run: `cargo build --workspace --tests 2>&1 | tail -5`
Expected: compiles with no `missing field` errors. Re-run Step 3's grep to confirm zero remaining.

- [ ] **Step 6: Full suite + lint**

Run: `cargo test 2>&1 | tail -15 && cargo clippy --workspace -- -D warnings 2>&1 | tail -10`
Expected: all tests pass; zero clippy warnings. Behavior is identical to Task 1 (every site is Eventual) — this task only adds the knob.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(uc_node): add NodeConfig.log_durability knob (default Eventual), thread to open"
```

---

## Task 3: Rename the now-misleading `JournalFsynced` probe → `JournalDurable`

In Eventual mode the append ack fires at the page-cache write, not fsync, so the probe name is wrong. Keep its numeric discriminant (3) for wire stability.

**Files:**
- Modify: `uc_protocol/src/probes.rs`
- Modify: `uc_node/src/raft/log_storage.rs`

- [ ] **Step 1: Rename in `probes.rs`**

In `uc_protocol/src/probes.rs`:
- In `pub enum Checkpoint`, change `JournalFsynced = 3,` (line ~16) to `JournalDurable = 3,`.
- Update the pipeline label table (lines ~140–141): change `("journal_fsync", JournalAppended, JournalFsynced)` to `("journal_durable", JournalAppended, JournalDurable)`, and `("commit_to_apply_enq", JournalFsynced, ApplyEnqueue)` to `("commit_to_apply_enq", JournalDurable, ApplyEnqueue)`.
- Update the test stamps (lines ~185–186): change `Checkpoint::JournalFsynced` to `Checkpoint::JournalDurable`.

- [ ] **Step 2: Rename the stamp site in `log_storage.rs`**

In `uc_node/src/raft/log_storage.rs:347`, change:
```rust
                    uc_protocol::probes::Checkpoint::JournalFsynced,
```
to:
```rust
                    uc_protocol::probes::Checkpoint::JournalDurable,
```

- [ ] **Step 3: Confirm no stragglers, build, test, lint**

Run: `grep -rn "JournalFsynced" --include="*.rs" . | grep -v target ; cargo test -p uc_protocol -p uc_node 2>&1 | tail -10 && cargo clippy --workspace -- -D warnings 2>&1 | tail -5`
Expected: the grep prints nothing; tests pass; zero clippy warnings.

- [ ] **Step 4: Commit**

```bash
git add uc_protocol/src/probes.rs uc_node/src/raft/log_storage.rs
git commit -m "refactor(probes): rename JournalFsynced -> JournalDurable (Eventual acks pre-fsync)"
```

---

## Task 4: Recovery clamp — `last_log_id_at` helper + `reconcile`

Repairs the one new power-loss edge: a fsynced `committed` ahead of the eventual log's recovered tail.

**Files:**
- Modify: `uc_node/src/raft/log_storage.rs` (add `last_log_id_at`)
- Modify: `uc_node/src/runtime/recovery.rs` (`assert_consistent` → `reconcile` + clamp)
- Modify: `uc_node/src/runtime/builder.rs` (call `reconcile`)
- Test: `uc_node/tests/eventual_durability.rs` (new)

- [ ] **Step 1: Write the failing clamp test**

Create `uc_node/tests/eventual_durability.rs` with:

```rust
//! Eventual-log durability: recovery clamp, durability-lag, mode behavior.

use openraft::storage::RaftLogStorage as _;
use openraft::storage::RaftLogStorageExt as _;
use openraft::{Entry, EntryPayload};
use tempfile::TempDir;
use uc_node::raft::log_storage::JournalLogStorage;

type LeaderId = openraft::impls::leader_id_adv::LeaderId<u64, u64>;
type RaftLogId = openraft::LogId<LeaderId>;
type RaftEntry = Entry<LeaderId, uc_node::raft::AppCommand, u64, uc_node::raft::NodeAddr>;

fn make_log_id(term: u64, node_id: u64, index: u64) -> RaftLogId {
    openraft::LogId::new(LeaderId::new(term, node_id), index)
}

async fn append_1_to(storage: &mut JournalLogStorage, n: u64) {
    let entries: Vec<RaftEntry> = (1..=n)
        .map(|i| Entry {
            log_id: make_log_id(1, 0, i),
            payload: EntryPayload::Normal(uc_node::raft::AppCommand(bytes::Bytes::from(
                format!("cmd-{i}"),
            ))),
        })
        .collect();
    storage.blocking_append(entries).await.expect("append");
}

#[tokio::test]
async fn reconcile_clamps_committed_ahead_of_log() {
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");

    // Log 1..=5; committed = index 5.
    append_1_to(&mut storage, 5).await;
    storage
        .save_committed(Some(make_log_id(1, 0, 5)))
        .await
        .expect("save_committed");

    // Simulate power-loss tail loss: drop entries 4,5 from the log while
    // `committed` (fsynced) stays at 5. truncate_after keeps index <= 3.
    storage
        .truncate_after(Some(make_log_id(1, 0, 3)))
        .await
        .expect("truncate");

    // Inversion now present: committed.index (5) > last_seq (3).
    assert_eq!(storage.read_committed().await.unwrap(), Some(make_log_id(1, 0, 5)));

    // Reconcile clamps committed down to the durable tail (index 3).
    uc_node::runtime::recovery::reconcile(&storage).expect("reconcile");
    assert_eq!(storage.read_committed().await.unwrap(), Some(make_log_id(1, 0, 3)));
}

#[tokio::test]
async fn reconcile_leaves_consistent_committed_untouched() {
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");
    append_1_to(&mut storage, 5).await;
    storage
        .save_committed(Some(make_log_id(1, 0, 3)))
        .await
        .expect("save_committed");

    // committed.index (3) <= last_seq (5): no clamp.
    uc_node::runtime::recovery::reconcile(&storage).expect("reconcile");
    assert_eq!(storage.read_committed().await.unwrap(), Some(make_log_id(1, 0, 3)));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --test eventual_durability reconcile_ 2>&1 | tail -20`
Expected: compile error — `no function or associated item named reconcile` (and/or `last_log_id_at`). This confirms the test targets the not-yet-existing API.

- [ ] **Step 3: Add `last_log_id_at` to `JournalLogStorage`**

In `uc_node/src/raft/log_storage.rs`, add this method to the `impl JournalLogStorage` block (near `open`/`handles`):

```rust
    /// The `RaftLogId` of the record at `seq` (the entry's own `log_id`), or
    /// `None` if `seq == 0` or no record exists there. Used by recovery to clamp
    /// a power-loss-inverted `committed` down to the durable log tail.
    pub(crate) fn last_log_id_at(&self, seq: u64) -> Result<Option<RaftLogId>, ClusterError> {
        if seq == 0 {
            return Ok(None);
        }
        let Some((_term, payload)) = self
            .journal
            .read(seq)
            .map_err(|e| ClusterError::Recovery(format!("read seq {seq}: {e}")))?
        else {
            return Ok(None);
        };
        let (entry, _) = bincode::serde::decode_from_slice::<
            <TypeConfig as openraft::RaftTypeConfig>::Entry,
            _,
        >(&payload, bincode::config::standard())
        .map_err(|e| ClusterError::Recovery(format!("decode seq {seq}: {e}")))?;
        Ok(Some(entry.log_id))
    }
```

`RaftLogId`, `TypeConfig`, `bincode`, and `ClusterError` are already in scope in this file.

- [ ] **Step 4: Rename `assert_consistent` → `reconcile` and add the clamp**

In `uc_node/src/runtime/recovery.rs`, rename the function and append the clamp. Change the signature line `pub fn assert_consistent(storage: &JournalLogStorage) -> Result<(), ClusterError> {` to `pub fn reconcile(storage: &JournalLogStorage) -> Result<(), ClusterError> {`, keep the existing `last_seq >= last_purged.index` and `output_progress <= last_applied` checks exactly as they are, and immediately before the final `Ok(())` insert:

```rust
    // Eventual-log / Consistent-committed inversion repair: a power loss can leave
    // a fsynced `committed` ahead of the eventual log's recovered (page-cache-lost)
    // tail. Clamp committed down to the durable log tail; the node re-learns the
    // true commit from the leader via normal Raft catch-up. Lowering committed is
    // always safe.
    let durable_last = storage.journal.last_seq().unwrap_or(0);
    if let Some(c) = storage
        .committed
        .load()
        .map_err(|e| ClusterError::Recovery(format!("read committed: {e}")))?
        && c.index > durable_last
    {
        match storage.last_log_id_at(durable_last)? {
            Some(id) => storage
                .committed
                .store(&id)
                .map_err(|e| ClusterError::Recovery(format!("clamp committed: {e}")))?
                .wait()
                .map_err(|e| ClusterError::Recovery(format!("clamp committed wait: {e}")))?,
            None => storage
                .committed
                .clear()
                .map_err(|e| ClusterError::Recovery(format!("clear committed: {e}")))?
                .wait()
                .map_err(|e| ClusterError::Recovery(format!("clear committed wait: {e}")))?,
        }
    }
    Ok(())
```

`storage.committed` and `storage.journal` are `pub(crate)` and `last_log_id_at` is `pub(crate)` — all reachable from this in-crate module.

- [ ] **Step 5: Update the builder caller**

In `uc_node/src/runtime/builder.rs:52`, change:
```rust
        crate::runtime::recovery::assert_consistent(&log_storage)?;
```
to:
```rust
        crate::runtime::recovery::reconcile(&log_storage)?;
```

- [ ] **Step 6: Run the clamp test + full suite + lint**

Run: `cargo test -p uc_node --test eventual_durability 2>&1 | tail -15 && cargo test 2>&1 | tail -10 && cargo clippy --workspace -- -D warnings 2>&1 | tail -5`
Expected: `reconcile_clamps_committed_ahead_of_log` and `reconcile_leaves_consistent_committed_untouched` pass; full suite green; zero clippy warnings.

- [ ] **Step 7: Commit**

```bash
git add uc_node/src/raft/log_storage.rs uc_node/src/runtime/recovery.rs uc_node/src/runtime/builder.rs uc_node/tests/eventual_durability.rs
git commit -m "feat(uc_node): clamp power-loss-inverted committed at recovery (assert_consistent -> reconcile)"
```

---

## Task 5: Durability-lag observability + mode-behavior tests

**Files:**
- Modify: `uc_node/src/raft/log_storage.rs` (add `durability_lag`)
- Test: `uc_node/tests/eventual_durability.rs` (append)

- [ ] **Step 1: Write the failing behavior tests**

Append to `uc_node/tests/eventual_durability.rs`:

```rust
#[tokio::test]
async fn consistent_mode_has_zero_durability_lag() {
    use ultima_journal::Durability;
    let dir = TempDir::new().unwrap();
    let mut storage =
        JournalLogStorage::open_with_durability(dir.path(), Durability::Consistent).expect("open");
    append_1_to(&mut storage, 3).await;
    // Consistent fsyncs before ack, so the durable watermark == last_seq.
    assert_eq!(storage.durability_lag(), 0);
}

#[tokio::test]
async fn eventual_mode_durability_lag_drains_to_zero() {
    use ultima_journal::Durability;
    let dir = TempDir::new().unwrap();
    let mut storage =
        JournalLogStorage::open_with_durability(dir.path(), Durability::Eventual).expect("open");
    append_1_to(&mut storage, 3).await;
    // The background idle-fsync eventually flushes; lag drains to 0. Spin with a
    // bound (mirrors ultima_journal's own task28 watermark tests).
    let mut ok = false;
    for _ in 0..200 {
        if storage.durability_lag() == 0 {
            ok = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(ok, "durability_lag never drained to 0");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --test eventual_durability durability_lag 2>&1 | tail -15`
Expected: compile error — `no method named durability_lag`.

- [ ] **Step 3: Implement `durability_lag`**

In `uc_node/src/raft/log_storage.rs`, add to `impl JournalLogStorage`:

```rust
    /// Log entries written but not yet fsync-durable — the Eventual-mode window
    /// (`last_seq - durable_seq`). Always 0 in Consistent mode. This is the health
    /// signal for Eventual durability; surface it via node telemetry.
    pub fn durability_lag(&self) -> u64 {
        let last = self.journal.last_seq().unwrap_or(0);
        last.saturating_sub(self.journal.durable_seq())
    }
```

`Journal::durable_seq()` is the task28 fsync watermark.

- [ ] **Step 4: Run behavior tests + full suite + lint**

Run: `cargo test -p uc_node --test eventual_durability 2>&1 | tail -15 && cargo clippy --workspace -- -D warnings 2>&1 | tail -5`
Expected: both new tests pass (plus the Task 4 ones); zero clippy warnings.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/raft/log_storage.rs uc_node/tests/eventual_durability.rs
git commit -m "feat(uc_node): expose JournalLogStorage::durability_lag (Eventual window) + tests"
```

---

## Task 6: Micro-measurement + task10 doc

**Files:**
- Test: `uc_node/tests/eventual_durability.rs` (append a measurement)
- Create: `docs/tasks/task10_eventual_log_durability.md`

- [ ] **Step 1: Add the append-ack micro-measurement**

Append to `uc_node/tests/eventual_durability.rs`. It times `blocking_append` of single-entry batches under each mode and prints medians; it does NOT assert tight bounds (fsync cost is storage-dependent — on tmpfs/fast SSD the gap is small; on real disk it is the prior benchmark's dominant cost).

```rust
#[tokio::test]
async fn measure_append_ack_latency_by_mode() {
    use std::time::Instant;
    use ultima_journal::Durability;

    async fn median_ack_us(durability: Durability) -> u128 {
        let dir = TempDir::new().unwrap();
        let mut storage =
            JournalLogStorage::open_with_durability(dir.path(), durability).expect("open");
        let mut samples = Vec::new();
        for i in 1..=200u64 {
            let e = vec![RaftEntry {
                log_id: make_log_id(1, 0, i),
                payload: EntryPayload::Normal(uc_node::raft::AppCommand(bytes::Bytes::from(
                    vec![0xABu8; 256],
                ))),
            }];
            let t = Instant::now();
            storage.blocking_append(e).await.expect("append");
            samples.push(t.elapsed().as_micros());
        }
        samples.sort_unstable();
        samples[samples.len() / 2]
    }

    let consistent = median_ack_us(Durability::Consistent).await;
    let eventual = median_ack_us(Durability::Eventual).await;
    println!(
        "append-ack median µs: Consistent={consistent} Eventual={eventual} \
         (storage-dependent; fsync cost dominates on real disk)"
    );
}
```

- [ ] **Step 2: Run the measurement and capture numbers**

Run: `cargo test -p uc_node --test eventual_durability measure_append_ack_latency_by_mode -- --nocapture 2>&1 | grep "append-ack median"`
Expected: a line like `append-ack median µs: Consistent=<X> Eventual=<Y> ...`. Record `<X>` and `<Y>`. Also capture the storage context: `findmnt -no FSTYPE,TARGET --target "$(git rev-parse --show-toplevel)" && uname -srm`.

- [ ] **Step 3: Write the task doc**

Create `docs/tasks/task10_eventual_log_durability.md`, replacing every `<...>` with real values from Step 2:

```markdown
# Task 10: Eventual Log Durability (Aeron-style)

## Summary

`uc_node` now stores the Raft log in an `ultima_journal::Journal` opened (by default)
with `Durability::Eventual`: an append is acknowledged to openraft at the **page-cache
write**, with fsync running asynchronously in the journal's background writer — off the
commit critical path. Cluster durability is provided by **quorum replication** (an entry
commits only when a quorum holds it). This mirrors Aeron Archive's default
`fileSyncLevel=0` (verified: Aeron's `RecordingWriter`/`Catalog` call `force()` only at
sync level ≥ 1; the consensus log commits on quorum append-position). Configurable via
`NodeConfig.log_durability`; `Consistent` (fsync-before-ack, power-loss safe) is opt-in.

## Durability model

| Mode | Ack when | On-disk fsync | Survives process crash | Survives power loss | Aeron analog |
|------|----------|---------------|------------------------|---------------------|--------------|
| Eventual (default) | page-cache write | background (journal idle-fsync) | yes | only if not a quorum simultaneously | fileSyncLevel=0 |
| Consistent | after fsync | inline, before ack | yes | yes | fileSyncLevel>=1 |

A client-acknowledged write is lost only if a quorum of nodes suffer power loss / kernel
panic within the un-fsynced window. The metadata `StableValue`s (vote, committed, …) stay
`Consistent` — they are cold-path and safety-critical (a forgotten vote risks split-brain).

## Implementation

- `JournalLogStorage::open_with_durability(dir, durability)`; `open(dir)` defaults Eventual.
- `NodeConfig.log_durability` (default Eventual) threaded via `NodeBuilder`.
- Append path unchanged: the existing `on_complete` ack fires at the page-cache write in
  Eventual, at fsync in Consistent.
- Recovery clamp (`recovery::reconcile`): a power loss can leave a fsynced `committed`
  ahead of the eventual log's recovered tail; reconcile clamps `committed` down to
  `last_log_id_at(last_seq)`. Lowering committed is safe — the node re-learns commit from
  the leader.
- Observability: `JournalLogStorage::durability_lag()` = `last_seq − durable_seq` (the
  un-fsynced window).
- Probe `JournalFsynced` renamed `JournalDurable` (Eventual acks pre-fsync).

## Measurement

Storage: `<FSTYPE>` at `<TARGET>`; `<uname -srm output>`.
Micro-measurement (`measure_append_ack_latency_by_mode`), single-entry `blocking_append`,
median latency:

| mode | append-ack median |
|------|-------------------|
| Consistent | <X> µs |
| Eventual | <Y> µs |

This isolates the **log-append** durability cost on the changed path. The gap here is
storage-dependent: on tmpfs / fast SSD the fsync cost is low so the difference is modest;
on real disk it is large — the prior `aeron-vs-uc-commit-path-benchmark` design attributed
the full single-node commit floor (p50 ≈ 36 ms) to the "journal group-commit window
(~38 ms/committed entry)".

## Scope & next levers (honest)

This removes the **log-append** fsync from the commit critical path. A commit round-trip
still incurs a `save_committed` fsync, and the output path a per-record `output_progress`
fsync — both kept `Consistent` here (out of scope). Validating the full commit-path effect
needs the `aeron-vs-uc` harness (single-node tmpfs vs real-disk, in-flight concurrency
sweep); if the ~38 ms window only partly collapses, `committed` / `output_progress` are the
next candidates.
```

- [ ] **Step 4: Verify the doc has no leftover placeholders**

Run: `grep -n "<X>\|<Y>\|<FSTYPE>\|<TARGET>\|<uname" docs/tasks/task10_eventual_log_durability.md`
Expected: no output (all replaced).

- [ ] **Step 5: Commit**

```bash
git add uc_node/tests/eventual_durability.rs docs/tasks/task10_eventual_log_durability.md
git commit -m "docs(uc_node): document eventual log durability + append-ack measurement (task10)"
```

---

## Final verification

- [ ] **Full workspace test + lint**

Run: `cargo test 2>&1 | tail -8 && cargo clippy --workspace -- -D warnings 2>&1 | tail -5`
Expected: all tests pass; zero clippy warnings.

Confirm: the default Raft-log durability is Eventual; `Consistent` is reachable via
`NodeConfig.log_durability` / `open_with_durability`; metadata `StableValue`s remain
`Consistent`; recovery clamps a power-loss-inverted `committed`.

---

## Notes for the implementer

- **Task 1 is the real behavioral change** (Eventual default); its value is the full-suite
  run proving every existing test survives the new default (page cache survives process
  crashes, so process-level reopen/crash tests pass). If one fails, that is a genuine
  fsync-dependency finding to surface, not to paper over.
- **Task 2 churn is compiler-guarded:** a missing `log_durability` is a hard error naming the
  file; the build is not clean until every `NodeConfig { … }` literal has it.
- **The clamp is required for correctness** of the log-Eventual / committed-Consistent split,
  not an optional extra — `committed > last_seq` becomes a legitimate post-power-loss state.
- **Honesty over headline:** the micro-measurement's gap is storage-dependent and isolates
  only the log-append fsync; the doc says so and points at the `aeron-vs-uc` harness for the
  full commit-path number and at `committed`/`output_progress` as the next levers.
- **Scope:** Raft log only. vote/committed/output_progress durability unchanged (separate
  follow-ons).
- **Feature-doc convention:** the canonical record is `docs/tasks/task10_*.md`; the
  `docs/superpowers/specs` + `plans` artifacts are gitignored scaffolding.
```
