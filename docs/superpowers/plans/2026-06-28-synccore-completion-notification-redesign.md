# SyncCore 3d redesign (pass 1) — completion-as-notification (hot path) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the SyncCore consensus loop from busy-waiting on per-write I/O — make `AppendEntries` and `save_committed` fire-and-forget — so the loop pipelines instead of serializing, while staying linearizable.

**Architecture:** The consensus loop publishes log-write ops to the off-thread durability consumer and continues without `block_on`-waiting. Flush completion already flows back via openraft's native `IOFlushed`→`LocalIO` notification path (no new plumbing). Because append is now fire-and-forget, a *readability gate* — a `watch` watermark the durability consumer updates after each `append`/`truncate`, plus a `GatedLogReader` wrapper that blocks reads until the watermark covers the requested range — preserves the `RaftLogStorage::append` "appended ⇒ readable" contract for the delegated replication path. `save_committed` becomes fire-and-forget because it is optional/advisory.

**Tech Stack:** Rust, openraft fork (`sync-core` feature), `disruptor` ring (durability), tokio watch channels, the `commit_latency` microbench.

## Global Constraints

- All work is on the openraft fork `PeterKnego/openraft` branch `sync-core`, behind the `sync-core` Cargo feature. The default (RaftCore) build path must remain behavior-identical.
- Correctness oracle: openraft's own suite must stay green at every task — `cargo test -p tests --features sync-core` (**180** integration) and `cargo test -p openraft --features sync-core --lib` (**495** lib). Default path: `cargo test -p openraft --lib` (**494**). `cargo clippy -p openraft --features sync-core -- -D warnings` and `cargo clippy -p openraft -- -D warnings` both clean.
- Crate layout: openraft crate is at `/home/claude/ultima/openraft/openraft/`; commands below assume cwd `/home/claude/ultima/openraft`.
- No engine changes. No changes to the async RaftCore command execution. `SaveVote`/`PurgeLog`/`TruncateLog` keep their `block_on` wait this pass (only their consumer-side watermark update is touched).
- Reactor-free invariant: the gate's await runs only in the replication task's tokio context, never on the consensus loop.

---

## File structure

- `openraft/src/core/sync_durability.rs` — add the readable-watermark `watch`, the `GatedLogReader<C, R>` wrapper + its `VendedReader<C, LS>` alias, update the watermark in `run_op` (`Append` Ok, `Truncate`), vend gated readers, drop `done` from `Append`/`SaveCommitted`, forward append submission errors via `tx_io_completed`. Extend the unit test.
- `openraft/src/core/sync_core.rs` — `AppendEntries` arm fire-and-forget; `SaveCommittedAndApply` arm fire-and-forget `save_committed`.
- `openraft/src/core/raft_core.rs` — `log_reader_request_tx` field type changes to vend `VendedReader` (gated) instead of `LS::LogReader`.
- `openraft/src/replication/mod.rs` — one cfg type alias `ReplLogReader<C, LS>` used for `ReplicationCore`'s reader field + `spawn` param (so it accepts the gated reader under `sync-core`).
- `benchmarks/minimal/src/bin/commit_latency.rs` — unchanged (re-used to measure).

---

## Task 1: Readability gate scaffolding (watermark + GatedLogReader), inert while append still waits

Lands all the type plumbing safely: append still uniform-awaits, so the consumer sets the watermark *before* the loop's await returns — every read therefore sees the watermark already satisfied and the gate never blocks yet. Suite must stay green.

**Files:**
- Modify: `openraft/src/core/sync_durability.rs`
- Modify: `openraft/src/core/raft_core.rs:207-208` (`log_reader_request_tx` type)
- Modify: `openraft/src/replication/mod.rs` (reader type alias + 2 use sites)
- Test: `openraft/src/core/sync_durability.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `sync_durability::GatedLogReader<C, R>` implementing `RaftLogReader<C>`; `sync_durability::VendedReader<C, LS> = GatedLogReader<C, <LS as RaftLogStorage<C>>::LogReader>`; `sync_durability::ReaderRequest<C, LS> = OneshotSenderOf<C, VendedReader<C, LS>>` (changed from vending `LS::LogReader`). The watermark is a `watch` of `Option<u64>` (highest readable log index; `None` = empty/none).
- Consumes (unchanged): `spawn(log_store, reader_rx)` returning `LogStoreHandle<C>`; the disruptor write ring; `DurabilityOp`.

- [ ] **Step 1: Add the unit test for the gate (failing)**

In `openraft/src/core/sync_durability.rs` `#[cfg(test)] mod tests`, add a test that drives the real `GatedLogReader` against a stub inner reader and asserts a read blocks until the watermark advances. Add near the existing test:

```rust
#[test]
fn gated_reader_blocks_until_watermark_covers_range() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    // Minimal in-thread executor: drive a future to completion with the reactor-free block_on,
    // while a side thread advances the watermark. Uses the openraft default TypeConfig in tests.
    // We assert the gated read does NOT complete until the watermark reaches the requested index.
    use crate::engine::testing::UTConfig; // openraft's unit-test TypeConfig
    let (tx, rx) = <UTConfig as crate::type_config::TypeConfigExt>::watch_channel::<Option<u64>>(None);

    // Stub inner reader: records the highest index requested; returns empty.
    #[derive(Clone)]
    struct Stub(Arc<AtomicU64>);
    impl crate::storage::RaftLogReader<UTConfig> for Stub {
        async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + crate::base::OptionalSend>(
            &mut self,
            range: RB,
        ) -> Result<Vec<crate::type_config::alias::EntryOf<UTConfig>>, std::io::Error> {
            let end = match range.end_bound() {
                std::ops::Bound::Included(&e) => e,
                std::ops::Bound::Excluded(&e) => e.saturating_sub(1),
                std::ops::Bound::Unbounded => u64::MAX,
            };
            self.0.store(end, Ordering::SeqCst);
            Ok(vec![])
        }
        async fn read_vote(&mut self) -> Result<Option<crate::type_config::alias::VoteOf<UTConfig>>, std::io::Error> {
            Ok(None)
        }
    }

    let observed = Arc::new(AtomicU64::new(0));
    let mut reader = GatedLogReader::new(Stub(observed.clone()), rx);

    // Advance the watermark from another thread after a short spin, then the read should unblock.
    let advancer = std::thread::spawn(move || {
        for _ in 0..1000 {
            std::hint::spin_loop();
        }
        tx.send(Some(5)).ok();
        // Keep tx alive until the reader has observed the value.
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(tx);
    });

    // block_on drives the gated read; it must wait for watermark>=5 (range 0..6 -> hi=5).
    let res = block_on(reader.try_get_log_entries(0u64..6u64));
    assert!(res.is_ok());
    assert_eq!(observed.load(Ordering::SeqCst), 5, "inner read happened only after the gate opened");
    advancer.join().unwrap();
}
```

- [ ] **Step 2: Run the test to verify it fails (no `GatedLogReader` yet)**

Run: `cargo test -p openraft --features sync-core --lib sync_durability 2>&1 | tail -20`
Expected: FAIL — `cannot find ... GatedLogReader` / `GatedLogReader::new`.

- [ ] **Step 3: Implement `GatedLogReader` + `VendedReader` and re-type `ReaderRequest`**

In `openraft/src/core/sync_durability.rs`, add imports and the wrapper. Add near the top imports:

```rust
use std::fmt::Debug;
use std::ops::Bound;
use std::ops::RangeBounds;

use crate::base::OptionalSend;
use crate::storage::RaftLogReader;
use crate::type_config::alias::EntryOf;
use crate::type_config::alias::VoteOf;
use crate::type_config::alias::WatchReceiverOf;
use crate::type_config::alias::WatchSenderOf;
use crate::async_runtime::watch::WatchReceiver;
use crate::async_runtime::watch::WatchSender;
```

Change the reader-request alias (it currently vends `LS::LogReader`):

```rust
/// A reader request: the consumer fulfils it by sending a fresh **gated** reader back.
pub(crate) type ReaderRequest<C, LS> = OneshotSenderOf<C, VendedReader<C, LS>>;

/// The reader type vended to the (delegated) replication path: the storage's own reader
/// wrapped in the readability gate.
pub(crate) type VendedReader<C, LS> = GatedLogReader<C, <LS as RaftLogStorage<C>>::LogReader>;
```

Add the wrapper:

```rust
/// Wraps a `RaftLogStorage::LogReader` with a readability gate. Because append is
/// fire-and-forget onto the durability consumer, an entry may be "submitted" on the
/// consensus loop before the consumer has actually `append`ed it. The
/// `RaftLogStorage::append` contract requires appended entries to be readable the moment
/// `append` returns, so a reader on another thread must not short-read an entry that is
/// in-flight. This wrapper blocks each read until the consumer's `readable` watermark
/// (highest log index currently in the store) covers the requested range, then delegates.
pub(crate) struct GatedLogReader<C, R>
where
    C: RaftTypeConfig,
    R: RaftLogReader<C>,
{
    inner: R,
    /// Highest readable log index currently in the store (`None` = nothing readable). Updated
    /// by the consumer in FIFO op order, so it tracks truncation (it can decrease).
    readable: WatchReceiverOf<C, Option<u64>>,
}

impl<C, R> GatedLogReader<C, R>
where
    C: RaftTypeConfig,
    R: RaftLogReader<C>,
{
    pub(crate) fn new(inner: R, readable: WatchReceiverOf<C, Option<u64>>) -> Self {
        Self { inner, readable }
    }

    /// Block until the watermark covers `hi` (the highest index the read needs). Best-effort:
    /// if the watch sender is gone (consumer shut down) we proceed and let the inner read
    /// reflect final state. Only the END of the range is gated — absence at the start
    /// (purged entries) is a tolerated short read per the reader contract.
    async fn await_readable(&mut self, hi: Option<u64>) {
        if let Some(hi) = hi {
            let _ = self.readable.wait_until_ge(&Some(hi)).await;
        }
    }
}

/// Compute the highest index a range needs, or `None` for an empty/unbounded-end range.
fn range_hi<RB: RangeBounds<u64>>(range: &RB) -> Option<u64> {
    match range.end_bound() {
        Bound::Included(&e) => Some(e),
        Bound::Excluded(&e) => e.checked_sub(1),
        Bound::Unbounded => None,
    }
}

impl<C, R> RaftLogReader<C> for GatedLogReader<C, R>
where
    C: RaftTypeConfig,
    R: RaftLogReader<C>,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<C>>, std::io::Error> {
        self.await_readable(range_hi(&range)).await;
        self.inner.try_get_log_entries(range).await
    }

    async fn limited_get_log_entries(&mut self, start: u64, end: u64) -> Result<Vec<EntryOf<C>>, std::io::Error> {
        self.await_readable(end.checked_sub(1)).await;
        self.inner.limited_get_log_entries(start, end).await
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, std::io::Error> {
        self.inner.read_vote().await
    }
}
```

- [ ] **Step 4: Thread the watermark through `spawn`/`consumer_loop`/`run_op` and vend gated readers**

In `openraft/src/core/sync_durability.rs`:

In `spawn`, create the watermark and pass its sender to the consumer:

```rust
pub(crate) fn spawn<C, LS>(
    log_store: LS,
    reader_rx: mpsc::Receiver<ReaderRequest<C, LS>>,
) -> LogStoreHandle<C>
where
    C: RaftTypeConfig,
    LS: RaftLogStorage<C>,
{
    let factory = || DurabilityEvent::<C> { op: Mutex::new(None) };
    let (poller, builder) = build_single_producer(1024, factory, BusySpin).new_event_poller();
    let producer = builder.build();

    // Readable watermark: highest log index currently in the store. Vended readers gate on it.
    let (readable_tx, _readable_rx0) = C::watch_channel::<Option<u64>>(None);

    let join = std::thread::spawn(move || consumer_loop::<C, LS>(log_store, poller, reader_rx, readable_tx));

    LogStoreHandle {
        producer: Some(producer),
        join: Some(join),
    }
}
```

In `consumer_loop`, accept the sender, vend gated readers, and pass the sender to `run_op`:

```rust
fn consumer_loop<C, LS>(
    mut log_store: LS,
    mut poller: EventPoller<DurabilityEvent<C>, SingleProducerBarrier>,
    reader_rx: mpsc::Receiver<ReaderRequest<C, LS>>,
    readable_tx: WatchSenderOf<C, Option<u64>>,
) where
    C: RaftTypeConfig,
    LS: RaftLogStorage<C>,
{
    loop {
        // (a) Vend gated readers.
        loop {
            match reader_rx.try_recv() {
                Ok(done) => {
                    let reader = block_on(log_store.get_log_reader());
                    done.send(GatedLogReader::new(reader, readable_tx.subscribe())).ok();
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }

        // (b) Drain the write ring.
        let mut did_work = false;
        match poller.poll() {
            Ok(mut events) => {
                for event in &mut events {
                    let taken = event.op.lock().unwrap().take();
                    if let Some(op) = taken {
                        run_op(&mut log_store, op, &readable_tx);
                        did_work = true;
                    }
                }
            }
            Err(Polling::NoEvents) => {}
            Err(Polling::Shutdown) => return,
        }

        if !did_work {
            std::thread::yield_now();
        }
    }
}
```

In `run_op`, accept the watermark sender and update it for `Append`/`Truncate` (leave the other arms' logic unchanged for now — `Append`/`SaveCommitted` still carry `done` in this task):

```rust
fn run_op<C, LS>(log_store: &mut LS, op: DurabilityOp<C>, readable_tx: &WatchSenderOf<C, Option<u64>>)
where
    C: RaftTypeConfig,
    LS: RaftLogStorage<C>,
{
    match op {
        DurabilityOp::Append { entries, io_id, tx_io_completed, done } => {
            let last_idx = io_id.last_log_id().map(|l| l.index());
            let callback = IOFlushed::new(io_id, tx_io_completed);
            let res = block_on(log_store.append(entries, callback)).sto_write_logs();
            if res.is_ok() {
                if let Some(idx) = last_idx {
                    // FIFO consumer order ⇒ this is the current store high-water for appends.
                    readable_tx.send(Some(idx)).ok();
                }
            }
            done.send(res).ok();
        }
        DurabilityOp::SaveVote { vote, done } => {
            let res = block_on(log_store.save_vote(&vote)).sto_write_vote();
            done.send(res).ok();
        }
        DurabilityOp::Purge { upto, done } => {
            let res = block_on(log_store.purge(upto)).sto_write_logs();
            done.send(res).ok();
        }
        DurabilityOp::Truncate { after, done } => {
            let res = block_on(log_store.truncate_after(after.clone())).sto_write_logs();
            if res.is_ok() {
                // Suffix removed ⇒ the high-water drops to the truncation point.
                readable_tx.send(after.map(|l| l.index())).ok();
            }
            done.send(res).ok();
        }
        DurabilityOp::SaveCommitted { committed, done } => {
            let res = block_on(log_store.save_committed(committed)).sto_write();
            done.send(res).ok();
        }
    }
}
```

- [ ] **Step 5: Re-type `log_reader_request_tx` in `raft_core.rs`**

In `openraft/src/core/raft_core.rs:207-208`, change the field type from vending `LS::LogReader` to the gated `VendedReader`:

```rust
    pub(crate) log_reader_request_tx:
        std::sync::mpsc::Sender<crate::type_config::alias::OneshotSenderOf<C, crate::core::sync_durability::VendedReader<C, LS>>>,
```

(The `spawn_replication_stream` reroute at `raft_core.rs:1084-1090` is unchanged — `rx.await` now yields a `VendedReader`, which is passed straight into `ReplicationCore::spawn`.)

- [ ] **Step 6: Add the `ReplLogReader` alias in `replication/mod.rs` and use it for the field + spawn param**

In `openraft/src/replication/mod.rs`, add near the top (after imports):

```rust
/// The log-reader type held by a replication stream. Under `sync-core` the durability
/// consumer vends a readability-gated reader; otherwise it is the storage's own reader.
#[cfg(not(feature = "sync-core"))]
pub(crate) type ReplLogReader<C, LS> = <LS as RaftLogStorage<C>>::LogReader;
#[cfg(feature = "sync-core")]
pub(crate) type ReplLogReader<C, LS> = crate::core::sync_durability::VendedReader<C, LS>;
```

Change the `ReplicationCore` field (currently `log_reader: LS::LogReader`, ~line 95) to:

```rust
    log_reader: ReplLogReader<C, LS>,
```

Change the `spawn` parameter (currently `log_reader: LS::LogReader`, ~line 123) to:

```rust
        log_reader: ReplLogReader<C, LS>,
```

- [ ] **Step 7: Run the gate unit test — verify it passes**

Run: `cargo test -p openraft --features sync-core --lib sync_durability 2>&1 | tail -20`
Expected: PASS (both `consumer_services_writes_and_reader_requests` and `gated_reader_blocks_until_watermark_covers_range`).

- [ ] **Step 8: Full suite + clippy, both feature states**

Run:
```bash
cargo test -p openraft --features sync-core --lib 2>&1 | grep "test result"
cargo test -p tests --features sync-core 2>&1 | grep "test result"
cargo test -p openraft --lib 2>&1 | grep "test result"
cargo clippy -p openraft --features sync-core -- -D warnings 2>&1 | tail -3
cargo clippy -p openraft -- -D warnings 2>&1 | tail -3
```
Expected: lib 495/0 (sync-core), integration 180/0, lib 494/0 (default), clippy clean both. (The gate is inert here — append still waits — so nothing should change behaviorally.)

- [ ] **Step 9: Commit**

```bash
cd /home/claude/ultima/openraft
git add openraft/src/core/sync_durability.rs openraft/src/core/raft_core.rs openraft/src/replication/mod.rs
git commit -m "feat(sync-core): readability gate scaffolding (watermark + GatedLogReader)

Add a readable-watermark watch the durability consumer updates after each append/
truncate, and a GatedLogReader wrapper that blocks reads until the watermark covers
the requested range. Vend gated readers to the delegated replication path (ReplLogReader
cfg alias). Inert for now (append still uniform-awaits), so behavior is unchanged;
suite 180/495 green. Prepares append fire-and-forget."
```

---

## Task 2: `AppendEntries` fire-and-forget (the gate becomes load-bearing)

Remove the per-append `block_on` wait. The gate from Task 1 now does the real work on the replication read path. This is the change that should lift throughput.

**Files:**
- Modify: `openraft/src/core/sync_durability.rs` (`DurabilityOp::Append` drops `done`; `run_op` Append forwards submission errors via `tx_io_completed`)
- Modify: `openraft/src/core/sync_core.rs:350-384` (`AppendEntries` arm)

**Interfaces:**
- Consumes: `GatedLogReader` / watermark from Task 1.
- Produces: `DurabilityOp::Append { entries, io_id, tx_io_completed }` (no `done`).

- [ ] **Step 1: Drop `done` from `DurabilityOp::Append` and rewrite its `run_op` arm**

In `openraft/src/core/sync_durability.rs`, change the `Append` variant (remove `done`):

```rust
    Append {
        entries: BatchOf<C, C::Entry>,
        io_id: IOId<C>,
        tx_io_completed: WatchSenderOf<C, Result<IOId<C>, StorageError<C>>>,
    },
```

Rewrite the `run_op` `Append` arm (fire-and-forget; on submission error, surface via the same watch the flush callback uses, since the callback never fires on a submission failure):

```rust
        DurabilityOp::Append { entries, io_id, tx_io_completed } => {
            let last_idx = io_id.last_log_id().map(|l| l.index());
            let callback = IOFlushed::new(io_id, tx_io_completed.clone());
            match block_on(log_store.append(entries, callback)).sto_write_logs() {
                Ok(()) => {
                    if let Some(idx) = last_idx {
                        readable_tx.send(Some(idx)).ok();
                    }
                }
                Err(e) => {
                    // Submission failed ⇒ the flush callback will not fire; surface the storage
                    // error through `tx_io_completed` so the forwarder turns it into a LocalIO
                    // notification the engine handles as fatal (matches RaftCore's append?-to-Fatal).
                    tx_io_completed.send(Err(e)).ok();
                }
            }
        }
```

- [ ] **Step 2: Rewrite the `AppendEntries` arm in `sync_core.rs` (fire-and-forget)**

In `openraft/src/core/sync_core.rs`, replace the body of the `Command::AppendEntries` arm (currently lines ~350-384, which build a oneshot, publish `{... done: tx}`, then `Self::await_completion(rx).await?`). Keep the before-work; drop the oneshot and the await:

```rust
            Command::AppendEntries { committed_vote: vote, entries } => {
                let last_log_id = entries.last().unwrap().log_id();
                let last_log_index = last_log_id.index();
                let entry_count = entries.len() as u64;
                self.core.runtime_stats.append_batch.record(entry_count);
                if let Some(r) = &self.core.metrics_recorder {
                    r.record_append_batch(entry_count);
                }
                let io_id = IOId::new_log_io(vote, Some(last_log_id));
                // Before-work stays on the consensus loop, ahead of publish.
                self.core.io_accepted_tx.send_if_greater(io_id.clone());
                self.core.engine.state.log_progress_mut().submit(io_id.clone());
                self.core.runtime_stats.record_log_stage_now(Stage::Submitted, last_log_index + 1);
                // Fire-and-forget: publish and return. Flush completion flows via the `IOFlushed`
                // callback → `tx_io_completed` → forwarder → `Notification::LocalIO` → engine, as
                // in RaftCore. Readability for a later (delegated) `Replicate` is preserved by the
                // durability consumer's readable-watermark + `GatedLogReader` (no consensus-loop
                // wait). Submission errors surface via `tx_io_completed` (see `run_op`).
                self.durability.publish(DurabilityOp::Append {
                    entries,
                    io_id,
                    tx_io_completed: self.core.tx_io_completed.clone(),
                });
            }
```

- [ ] **Step 3: Build (catches the dropped-field / unused-import fallout)**

Run: `cargo build -p openraft --features sync-core 2>&1 | tail -15`
Expected: compiles. If `await_completion` is now unused, leave it (still used by SaveVote/Purge/Truncate) — it is. If `IOId` import or others go unused, fix per the compiler.

- [ ] **Step 4: Full suite — the gate is now load-bearing on multi-node**

Run:
```bash
cargo test -p openraft --features sync-core --lib 2>&1 | grep "test result"
cargo test -p tests --features sync-core 2>&1 | grep -E "test result|FAILED"
```
Expected: lib 495/0, integration 180/0. (Replication/membership/snapshot tests now exercise the gate under fire-and-forget. A wrong gate would hang → test timeout, or short-read → deterministic replication failure.)

- [ ] **Step 5: Default path unaffected + clippy**

Run:
```bash
cargo test -p openraft --lib 2>&1 | grep "test result"
cargo clippy -p openraft --features sync-core -- -D warnings 2>&1 | tail -3
```
Expected: 494/0, clippy clean.

- [ ] **Step 6: Commit**

```bash
cd /home/claude/ultima/openraft
git add openraft/src/core/sync_durability.rs openraft/src/core/sync_core.rs
git commit -m "feat(sync-core): AppendEntries fire-and-forget (gate load-bearing)

Drop the per-append block_on submission wait on the consensus loop. Flush completion
still flows via the native IOFlushed->LocalIO path; readability for delegated Replicate
is preserved by the durability watermark + GatedLogReader. Submission errors surface via
tx_io_completed. Suite 180/495 green."
```

---

## Task 3: `SaveCommittedAndApply` fire-and-forget `save_committed`

Remove the per-commit `block_on` wait. `save_committed` is optional/advisory (trait default no-op; openraft tolerates a lagging committed marker), FIFO consumer order keeps it monotonic, and apply does not depend on it being durable.

**Files:**
- Modify: `openraft/src/core/sync_durability.rs` (`DurabilityOp::SaveCommitted` drops `done`; `run_op` arm drops `done.send`)
- Modify: `openraft/src/core/sync_core.rs:437-445` (`SaveCommittedAndApply` arm)

**Interfaces:**
- Produces: `DurabilityOp::SaveCommitted { committed }` (no `done`).

- [ ] **Step 1: Drop `done` from `DurabilityOp::SaveCommitted` and its `run_op` arm**

In `openraft/src/core/sync_durability.rs`, change the variant:

```rust
    SaveCommitted {
        committed: Option<LogIdOf<C>>,
    },
```

And the `run_op` arm (fire-and-forget; advisory, so a save error is logged, not propagated):

```rust
        DurabilityOp::SaveCommitted { committed } => {
            if let Err(e) = block_on(log_store.save_committed(committed)).sto_write() {
                tracing::warn!("sync-core: save_committed failed (advisory, ignored): {}", e);
            }
        }
```

- [ ] **Step 2: Rewrite the `SaveCommittedAndApply` arm in `sync_core.rs`**

In `openraft/src/core/sync_core.rs`, replace the `Command::SaveCommittedAndApply` arm body (currently lines ~437-445, which builds a oneshot, publishes `SaveCommitted{... done: tx}`, `await_completion`, then applies):

```rust
            Command::SaveCommittedAndApply { already_applied: already_committed, upto } => {
                self.core.runtime_stats.record_log_stage_now(Stage::Committed, upto.index() + 1);
                self.core.engine.state.apply_progress_mut().submit(upto.clone());
                // Fire-and-forget: `save_committed` is optional/advisory (recovery optimization)
                // and apply does not depend on it being durable; FIFO consumer order keeps the
                // persisted committed marker monotonic. Apply is already fire-and-forget (it hands
                // responders to the sm::Worker, which responds to clients).
                self.durability.publish(DurabilityOp::SaveCommitted { committed: Some(upto.clone()) });
                let first = self.core.engine.state.get_log_id(already_committed.next_index()).unwrap();
                self.core.apply_to_state_machine(first, upto).await?;
            }
```

- [ ] **Step 3: Build**

Run: `cargo build -p openraft --features sync-core 2>&1 | tail -15`
Expected: compiles.

- [ ] **Step 4: Full suite + default + clippy**

Run:
```bash
cargo test -p openraft --features sync-core --lib 2>&1 | grep "test result"
cargo test -p tests --features sync-core 2>&1 | grep -E "test result|FAILED"
cargo test -p openraft --lib 2>&1 | grep "test result"
cargo clippy -p openraft --features sync-core -- -D warnings 2>&1 | tail -3
```
Expected: 495/0, 180/0, 494/0, clippy clean.

- [ ] **Step 5: Commit**

```bash
cd /home/claude/ultima/openraft
git add openraft/src/core/sync_durability.rs openraft/src/core/sync_core.rs
git commit -m "feat(sync-core): SaveCommittedAndApply fire-and-forget save_committed

save_committed is optional/advisory (trait default no-op; openraft tolerates a lagging
committed marker) and apply does not depend on it being durable; FIFO consumer order keeps
it monotonic. Drop the per-commit block_on wait. Suite 180/495 green."
```

---

## Task 4: Re-measure (commit_latency A/B) and record

**Files:**
- Use: `benchmarks/minimal/src/bin/commit_latency.rs` (unchanged)
- Create: `ultima_cluster/docs/benchmarks/synccore-3d-redesign-commit-latency-2026-06-28.md`

- [ ] **Step 1: Build both variants, preserve to distinct paths**

```bash
cd /home/claude/ultima/openraft
SCRATCH=/tmp/claude-1000/-home-claude-ultima-ultima-cluster/847f3226-c28a-4889-b08b-20d6d4db8f4c/scratchpad
cargo build --release --manifest-path benchmarks/minimal/Cargo.toml --bin commit_latency --features sync-core 2>&1 | tail -2
cp /home/claude/.cache/cargo-target/release/commit_latency "$SCRATCH/cl_sync_redesign"
cargo build --release --manifest-path benchmarks/minimal/Cargo.toml --bin commit_latency 2>&1 | tail -2
cp /home/claude/.cache/cargo-target/release/commit_latency "$SCRATCH/cl_raft"
```
Expected: two binaries of differing size.

- [ ] **Step 2: Run the A/B — inflight=1 latency (3 reps) + concurrency sweep**

```bash
SCRATCH=/tmp/claude-1000/-home-claude-ultima-ultima-cluster/847f3226-c28a-4889-b08b-20d6d4db8f4c/scratchpad
echo "== inflight=1, n=50k, --server-workers 2, 3 reps =="
for r in 1 2 3; do "$SCRATCH/cl_raft" -m 1 -n 50000 -w 5000 --server-workers 2; "$SCRATCH/cl_sync_redesign" -m 1 -n 50000 -w 5000 --server-workers 2; done
echo "== throughput conc 16/64/256, n=100k =="
for c in 16 64 256; do "$SCRATCH/cl_raft" -m 1 -n 100000 -w 5000 -c $c | grep throughput; "$SCRATCH/cl_sync_redesign" -m 1 -n 100000 -w 5000 -c $c | grep throughput; done
```
Expected: numbers to record. **Success criterion:** redesigned-3d p50 ≤ RaftCore at inflight=1 AND redesigned-3d throughput ≥ RaftCore under concurrency. If not fully met, note the residual (durability thread hop / 4-core oversubscription) for the next pass.

- [ ] **Step 3: Write the findings doc**

Create `ultima_cluster/docs/benchmarks/synccore-3d-redesign-commit-latency-2026-06-28.md` with: the table (RaftCore vs redesigned-3d, inflight=1 p50 + throughput sweep), the delta vs the pre-redesign spike (from `synccore-3d-commit-latency-2026-06-28.md`), whether the success criterion was met, and the next suspect if not. (Fill with the actual Step 2 numbers — no placeholders.)

- [ ] **Step 4: Commit the findings (ultima_cluster repo)**

```bash
cd /home/claude/ultima/ultima_cluster
git add docs/benchmarks/synccore-3d-redesign-commit-latency-2026-06-28.md
git commit -m "docs(benchmarks): SyncCore 3d redesign re-measurement (commit_latency A/B)"
```

---

## Self-review notes

- **Spec coverage:** Change 1 (append f-a-f) → Task 2; Change 2 (gate) → Task 1; Change 3 (save_committed f-a-f) → Task 3; testing/measurement → Tasks 1 & 4. `SaveVote`/`Purge`/`Truncate` stay on `block_on` (their `done` arms untouched; only `Truncate`'s watermark update added). Submission-error propagation (spec Change 1 refinement) → Task 2 Step 1.
- **Refinement vs spec:** the gate is *not* fully contained to `sync_durability` — it changes the vended-reader type (`raft_core.rs` field + `replication/mod.rs` `ReplLogReader` alias). This is the minimal type-flow for a read-side gate, established by the finding that replication's read range is bounded by the engine's in-memory `last_log_id`, not an io_state marker we control. The watermark is consumer-FIFO-ordered (not naively monotonic) so truncation lowers it correctly.
- **Type consistency:** `GatedLogReader<C,R>` / `VendedReader<C,LS>` / `ReplLogReader<C,LS>` / `ReaderRequest<C,LS>` used consistently across `sync_durability.rs`, `raft_core.rs:207`, `replication/mod.rs`. Watermark type is `Option<u64>` everywhere; `wait_until_ge(&Some(hi))`.
- **Risk — gate hang:** a watermark that never reaches `hi` hangs a read → surfaces as a suite timeout, not corruption (Task 2 Step 4).
