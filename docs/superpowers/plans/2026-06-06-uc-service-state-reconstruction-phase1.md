# Service-State Reconstruction — Phase 1 (mid-life reattach) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a service crashes and reconnects while the node keeps running, reconstruct its (possibly in-memory) state machine by replaying committed entries `(service_last_applied, node_frontier]` from the journal — so in-memory SMs survive a service-only restart.

**Architecture:** The reattaching service publishes its recovered `last_applied` and bumps a `service_epoch` (cnc atomics) before flipping `READY`, and resets its apply-ring read cursor to discard the stale backlog. The node's parked `apply(N)` detects the epoch change (riding `NotifyBridge`'s PARK_CEIL backstop — proven feasible by spike) and *self-drives* catch-up within the same call: reset the resp-ring read cursor, replay `(service_last_applied, N]` from the journal (publishing each, consuming its resp), and return entry N's resp. openraft never sees an error. Single-in-flight `apply()` guarantees exactly one coordinator. See spec §2, §3, §3a.

**Tech Stack:** Rust, openraft 0.10, `ultima_journal`, shmem SPSC rings, tokio (`current_thread` for the shmem path).

**Spec:** `docs/superpowers/specs/2026-06-06-uc-service-state-reconstruction-design.md`.

**Out of scope (later phases):** snapshot build/install + below-purge reattach + safe-purge (Phase 2); revert `RegisterSm`, re-enable the node cross-check, lincheck capstone (Phase 3). Phase 1 returns an explicit `NeedsSnapshot` error when the gap is below the purge boundary.

---

## File structure

- **Modify** `uc_protocol/src/cnc.rs` — add `service_epoch: AtomicU64` to `ServiceStatus` (consume padding; keep the 64-byte size assertion).
- **Modify** `uc_protocol/src/ring/spsc.rs` — add `SpscConsumer::discard_backlog()`.
- **Modify** `uc_service/src/runtime/handshake.rs` — `publish_service_last_applied()` + `bump_service_epoch()` helpers.
- **Modify** `uc_service/src/runtime/service.rs` — at attach: discard apply backlog, publish `last_applied`, bump epoch, then `READY`.
- **Create** `uc_node/src/runtime/reconstruct.rs` — `CatchupSource` + `decide_catchup_source()` (pure).
- **Modify** `uc_node/src/runtime/mod.rs` — `pub(crate) mod reconstruct;`.
- **Modify** `uc_node/src/raft/state_machine_shmem.rs` — plumb the `service_epoch` pointer + last-seen epoch into the adapter; `ApplyOutcome` from `await_apply_resp`; self-driving catch-up in `apply()`.
- **Modify** `uc_node/src/runtime/builder.rs` — pass the `service_epoch` pointer into the adapter; seed last-seen epoch.
- **Create** `uc_node/tests/reconstruct_reattach.rs` — service crash + restart, in-memory SM, node up → reconstructed.

---

## Task 1: `service_epoch` field in `ServiceStatus`

**Files:** Modify `uc_protocol/src/cnc.rs` (the `ServiceStatus` struct ~line 87-101).

- [ ] **Step 1: Add the field, preserving 64-byte layout + alignment**

Current:
```rust
#[repr(C, align(64))]
pub struct ServiceStatus {
    pub state: AtomicU32,        // 0..4
    pub _pad_1: u32,             // 4..8
    pub last_applied: AtomicU64, // 8..16
    pub last_output_ack: AtomicU64, // 16..24
    pub heartbeat_seq: AtomicU64,   // 24..32
    pub heartbeat_at_ns: AtomicU64, // 32..40
    pub service_pid: AtomicU32,     // 40..44
    pub _pad_2: [u8; 20],           // 44..64
}
```
Replace the tail (`service_pid` + `_pad_2`) with an 8-byte-aligned `service_epoch`:
```rust
    pub service_pid: AtomicU32,     // 40..44
    pub _pad_2a: u32,               // 44..48  (align service_epoch to 8)
    /// Bumped by each new service incarnation at attach (before READY). The node
    /// tracks the last-seen value; a change signals a reattach (Phase 1
    /// reconstruction). Monotonic.
    pub service_epoch: AtomicU64,   // 48..56
    pub _pad_2: [u8; 8],            // 56..64
}
```

- [ ] **Step 2: Update every `ServiceStatus { .. }` literal**

The struct is constructed in test code in several files. Grep and fix each:
Run: `rg -n "ServiceStatus \{" --type rust`
For each literal, add `_pad_2a: 0,` and `service_epoch: AtomicU64::new(0),` and change `_pad_2: [0; 20]` → `_pad_2: [0; 8]`. Known sites: `uc_service/src/runtime/handshake.rs` (tests), `uc_node/src/ipc/handshake.rs` (tests), and any cnc/service_status constructors. Let the compiler list them: `cargo build --workspace` and fix each error.

- [ ] **Step 3: Verify the size assertion still holds**

The file has `const _: () = { assert!(std::mem::size_of::<ServiceStatus>() == STATUS_BLOCK_LEN); };`. Run:
Run: `cargo build -p uc_protocol`
Expected: builds — if the layout is wrong the static assert fails at compile time.

- [ ] **Step 4: Commit**

```bash
git add uc_protocol/src/cnc.rs
git commit -m "feat(uc_protocol): add ServiceStatus.service_epoch (reattach detection)"
```

---

## Task 2: `SpscConsumer::discard_backlog()`

**Files:** Modify `uc_protocol/src/ring/spsc.rs` (the `impl SpscConsumer` block, near `try_read`).

Lets a consumer drop everything currently unread by jumping its read cursor to the producer's write cursor. Used by the reattaching service (apply ring) and the node during catch-up (resp ring).

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` in `uc_protocol/src/ring/spsc.rs`:
```rust
    #[test]
    fn discard_backlog_drops_unread_records() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 8192, 1024).unwrap();
        let (mut producer, mut consumer) = ring.into_split();
        producer.try_write(1, 0, [0u8; 8], b"a").unwrap();
        producer.try_write(1, 0, [0u8; 8], b"b").unwrap();
        consumer.discard_backlog();
        let mut buf = Vec::new();
        assert!(consumer.try_read(&mut buf).unwrap().is_none(), "backlog discarded");
        // Ring still usable afterwards.
        producer.try_write(1, 0, [0u8; 8], b"c").unwrap();
        let rec = consumer.try_read(&mut buf).unwrap().expect("c readable");
        assert_eq!(rec.msg_type, 1);
        assert_eq!(&buf, b"c");
    }
```

- [ ] **Step 2: Run, confirm it fails**

Run: `cargo test -p uc_protocol ring::spsc::tests::discard_backlog_drops_unread_records`
Expected: FAIL — no method `discard_backlog`.

- [ ] **Step 3: Implement**

In `impl SpscConsumer`, add:
```rust
    /// Drop all currently-unread records by advancing the read cursor to the
    /// producer's write cursor. Only the consumer writes `consumer_position`, so
    /// this is SPSC-safe. Used at service reattach (apply ring) and during
    /// node-side catch-up (resp ring) to discard a crashed incarnation's
    /// leftovers. A no-op when already caught up (publish == consumer).
    pub fn discard_backlog(&mut self) {
        let header = self.inner.header();
        let producer_pos = header.publish_position.load(Ordering::Acquire);
        header.consumer_position.store(producer_pos, Ordering::Release);
    }
```

- [ ] **Step 4: Run, confirm it passes**

Run: `cargo test -p uc_protocol ring::spsc::tests::discard_backlog_drops_unread_records`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/ring/spsc.rs
git commit -m "feat(uc_protocol): SpscConsumer::discard_backlog (reattach cursor reset)"
```

---

## Task 3: Service publishes epoch + last_applied + discards apply backlog before READY

**Files:** Modify `uc_service/src/runtime/handshake.rs`, `uc_service/src/runtime/service.rs`.

- [ ] **Step 1: Add handshake helpers + tests**

In `uc_service/src/runtime/handshake.rs`, add after `set_service_state`:
```rust
/// Publish the service's recovered `last_applied` (channel A). Call BEFORE
/// `set_service_state(.., READY)`; the state-flag Release carries it to the node.
/// A restarted service MUST overwrite it (0 for a fresh in-memory SM).
pub fn publish_service_last_applied(status: &ServiceStatus, last_applied: u64) {
    status.last_applied.store(last_applied, Ordering::Release);
}

/// Bump `service_epoch` so the node detects this incarnation as a reattach.
/// Call BEFORE `set_service_state(.., READY)`. Read-modify-write under the
/// single-service invariant (only one live service writes it).
pub fn bump_service_epoch(status: &ServiceStatus) {
    let prev = status.service_epoch.load(Ordering::Relaxed);
    status.service_epoch.store(prev + 1, Ordering::Release);
}
```
Add tests mirroring `set_state_round_trip` (construct a `ServiceStatus`, call each helper, assert the field). Run `cargo test -p uc_service runtime::handshake` to confirm green.

- [ ] **Step 2: Wire into `service.rs run()`**

Find the READY block (~line 192-197). Replace:
```rust
        let status = unsafe { &*service_status_ptr };
        set_service_state(status, service_state::READY);
```
with:
```rust
        // Reconstruction handshake (Phase 1). Order matters — all stores are
        // Release and must precede the state→READY Release the node Acquires on:
        //   1. discard any stale apply backlog from a crashed prior incarnation
        //      (so our fresh apply_loop doesn't apply mid-stream frames),
        //   2. publish our recovered last_applied (0 for fresh in-memory),
        //   3. bump the epoch so the node detects this as a (re)attach,
        //   4. flip READY.
        // SAFETY: cnc mmap owned by `Service` for the loop lifetime.
        let status = unsafe { &*service_status_ptr };
        attached.apply_consumer_discard_backlog(); // see note below
        let recovered = sm_shared.read().await.last_applied().unwrap_or(0);
        super::handshake::publish_service_last_applied(status, recovered);
        super::handshake::bump_service_epoch(status);
        set_service_state(status, service_state::READY);
```
NOTE: the apply consumer is moved into `spawn_apply_loop` (line 155-159) BEFORE this block, so it is no longer accessible here. Reorder: call `discard_backlog()` on `attached.apply_consumer` **before** `spawn_apply_loop` consumes it. Concretely, just before the `let apply = spawn_apply_loop(...)` call (line 155), insert:
```rust
        // Discard any stale apply backlog from a crashed prior incarnation
        // before the apply_loop starts consuming (reattach: our SM is fresh, so
        // mid-stream frames would be applied on empty state). No-op on first
        // attach (publish == consumer == 0).
        attached.apply_consumer.discard_backlog();
```
Then remove the `attached.apply_consumer_discard_backlog();` placeholder from the READY block (keep only the publish/bump/READY there). `attached.apply_consumer` is the `SpscConsumer` (confirm the field name in `AttachedRings` via `attach.rs`; adjust if it's named differently, e.g. `apply_consumer`).

- [ ] **Step 3: Build**

Run: `cargo build -p uc_service`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add uc_service/src/runtime/handshake.rs uc_service/src/runtime/service.rs
git commit -m "feat(uc_service): reattach handshake — discard backlog, publish last_applied, bump epoch"
```

---

## Task 4: Catch-up source decision (pure)

**Files:** Create `uc_node/src/runtime/reconstruct.rs`; modify `uc_node/src/runtime/mod.rs`.

Identical to the (sound, reviewed) decision logic from the superseded Phase-1a plan; re-derived here.

- [ ] **Step 1: Register the module** — in `uc_node/src/runtime/mod.rs` add `pub(crate) mod reconstruct;`.

- [ ] **Step 2: Create `reconstruct.rs` with the type, stub, and tests**

```rust
//! Service-state reconstruction (Phase 1, mid-life reattach).
//!
//! When the node detects a service reattach (service_epoch change), it replays
//! committed entries `(service_last_applied, node_frontier]` from the journal to
//! the reattached (possibly fresh in-memory) service. `node_frontier` is the
//! node's LIVE in-memory applied index — the range openraft will not re-drive.

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CatchupSource {
    Nothing,
    LogReplay { from: u64, to: u64 },
    NeedsSnapshot { service_last_applied: u64, last_purged: u64 },
}

/// Pure decision. `last_purged` is the highest purged log index (0 if none).
pub(crate) fn decide_catchup_source(
    service_last_applied: u64,
    node_frontier: u64,
    last_purged: u64,
) -> CatchupSource {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nothing_when_current_or_ahead_or_empty() {
        assert_eq!(decide_catchup_source(10, 10, 0), CatchupSource::Nothing);
        assert_eq!(decide_catchup_source(12, 10, 0), CatchupSource::Nothing);
        assert_eq!(decide_catchup_source(0, 0, 0), CatchupSource::Nothing);
    }
    #[test]
    fn log_replay_above_purge_incl_boundary() {
        assert_eq!(decide_catchup_source(3, 10, 0), CatchupSource::LogReplay { from: 3, to: 10 });
        assert_eq!(decide_catchup_source(5, 10, 5), CatchupSource::LogReplay { from: 5, to: 10 });
    }
    #[test]
    fn needs_snapshot_below_purge() {
        assert_eq!(decide_catchup_source(2, 10, 5),
            CatchupSource::NeedsSnapshot { service_last_applied: 2, last_purged: 5 });
    }
}
```

- [ ] **Step 3: Run, confirm fail** — `cargo test -p uc_node --lib runtime::reconstruct::tests` → FAIL (unimplemented).

- [ ] **Step 4: Implement the body**
```rust
    if service_last_applied >= node_frontier {
        CatchupSource::Nothing
    } else if service_last_applied < last_purged {
        CatchupSource::NeedsSnapshot { service_last_applied, last_purged }
    } else {
        CatchupSource::LogReplay { from: service_last_applied, to: node_frontier }
    }
```

- [ ] **Step 5: Run, confirm pass** (3 tests). **Commit:**
```bash
git add uc_node/src/runtime/reconstruct.rs uc_node/src/runtime/mod.rs
git commit -m "feat(uc_node): catch-up source decision (reconstruct, Phase 1)"
```

---

## Task 5: Plumb the ServiceStatus pointer + journal + last_purged into the adapter

**Files:** Modify `uc_node/src/raft/state_machine_shmem.rs`, `uc_node/src/runtime/builder.rs`.

The node-side `apply()` must read `ServiceStatus.{service_epoch,last_applied}` and replay from the journal. Plumb one `Send` pointer to the cnc `ServiceStatus`, the journal, the `last_purged` handle, and a tracked last-seen epoch into `ShmemInner`.

- [ ] **Step 1: Add the `Send` pointer wrapper + module-level epoch/last-applied readers**

Near the top of `state_machine_shmem.rs` (after imports), add:
```rust
/// `Send`/`Sync` wrapper for a `*const ServiceStatus` into the cnc mmap. The
/// cnc mmap is owned by the node `Instance`/handle and outlives the adapter; all
/// `ServiceStatus` fields are atomics (so `Sync`) and the node only reads them.
#[derive(Clone, Copy)]
pub(crate) struct ServiceStatusPtr(pub(crate) *const uc_protocol::cnc::ServiceStatus);
// SAFETY: see doc comment — points into a Sync, longer-lived mmap; read-only.
unsafe impl Send for ServiceStatusPtr {}
unsafe impl Sync for ServiceStatusPtr {}

/// Current service epoch (0 if no pointer). SAFETY: live cnc mmap.
fn epoch_of(p: Option<ServiceStatusPtr>) -> u64 {
    match p {
        Some(ServiceStatusPtr(s)) => unsafe { (*s).service_epoch.load(Ordering::Acquire) },
        None => 0,
    }
}

/// The reattached service's reported last_applied (0 if no pointer). SAFETY: live cnc mmap.
fn service_last_of(p: Option<ServiceStatusPtr>) -> u64 {
    match p {
        Some(ServiceStatusPtr(s)) => unsafe { (*s).last_applied.load(Ordering::Acquire) },
        None => 0,
    }
}
```
(`Ordering` is already imported; `uc_protocol::cnc::ServiceStatus` may need a `use`.)

- [ ] **Step 2: Add fields to `ShmemInner` + constructor params**

Add to `ShmemInner`:
```rust
    /// Pointer to the cnc ServiceStatus (epoch + last_applied). None in tests.
    pub(crate) service_status_ptr: Option<ServiceStatusPtr>,
    /// Epoch last reconciled by reconstruction; a change means a service reattach.
    pub(crate) last_seen_epoch: u64,
    /// Journal handle for replaying committed entries during catch-up.
    pub(crate) journal: Arc<ultima_journal::Journal>,
    /// Purge boundary, for the below-purge → NeedsSnapshot (Phase 2) decision.
    pub(crate) last_purged: Arc<StableValue<RaftLogId>>,
```
Add constructor params to `ShmemAdaptedStateMachine::new`:
`journal: Arc<ultima_journal::Journal>`, `last_purged: Arc<StableValue<RaftLogId>>`, `service_status_ptr: Option<ServiceStatusPtr>`. Initialize the fields in the `ShmemInner { .. }` literal, seeding **`last_seen_epoch: epoch_of(service_status_ptr)`** — i.e. the CURRENT cnc epoch at construction (the service already bumped it to 1 before READY, since `new()` runs after `wait_for_service_ready`). This is important: seeding to the current epoch means a cold start's first live apply takes the normal path (openraft already reconstructs the cold-start range), and ONLY a genuine mid-life re-bump (1→2) triggers catch-up. Seeding to 0 would cause a harmless-but-redundant catch-up on the first cold-start apply. (`Arc`, `StableValue`, `RaftLogId` are already in scope; `ultima_journal::Journal` likewise — it's the journal type used by `LogStorageHandles`/`JournalLogStorage`.)

- [ ] **Step 3: Pass them from the builder**

In `builder.rs`, the shmem path already has `let journal_for_replay = log_storage.journal.clone();` and `log_storage.last_purged` is `pub(crate)`. At the `ShmemAdaptedStateMachine::new(...)` call, add the three new args:
```rust
                    journal_for_replay.clone(),
                    log_storage.last_purged.clone(),
                    Some(crate::raft::state_machine_shmem::ServiceStatusPtr(service_status.0)),
```
(`service_status.0` is the existing `*const ServiceStatus` SendPtr payload.)

- [ ] **Step 4: Fix any other `new()` caller**

Run: `rg -n "ShmemAdaptedStateMachine::new" --type rust`. For any non-builder caller (e.g. a shmem state-machine test), pass that caller's journal + last_purged handles and `None` for `service_status_ptr`.

- [ ] **Step 5: Build** — `cargo build -p uc_node` (dead-code warnings for the new fields/readers are expected until Task 6). **Commit:**
```bash
git add uc_node/src/raft/state_machine_shmem.rs uc_node/src/runtime/builder.rs
git commit -m "feat(uc_node): plumb ServiceStatus ptr + journal + last_purged into the adapter"
```

---

## Task 6: Reattach-aware `await_apply_resp` + self-driving catch-up in `apply()`

**Files:** Modify `uc_node/src/raft/state_machine_shmem.rs`.

The core. `await_apply_resp` returns `Reattach` (not an error → openraft stays happy) when the epoch changes; `apply()`'s Normal branch, on `Reattach` (or a lazy epoch check before publishing), runs `drive_catchup`, which replays `(service_last, N]` from the journal — consuming each resp itself — and returns N's resp.

- [ ] **Step 1: Add `ApplyOutcome` and make `await_apply_resp` epoch-aware**

Add near the ring helpers:
```rust
enum ApplyOutcome {
    Resp(Bytes),
    /// The service reattached (epoch changed) before this resp arrived.
    Reattach,
}
```
Change `await_apply_resp`'s signature to add two Copy params and return `ApplyOutcome`:
```rust
async fn await_apply_resp(
    consumer: &PlMutex<SpscConsumer>,
    expected_log_index: u64,
    log_id: RaftLogId,
    shutdown: &AtomicBool,
    bridge: &NotifyBridge,
    service_status_ptr: Option<ServiceStatusPtr>,
    expected_epoch: u64,
) -> Result<ApplyOutcome, io::Error> {
```
At the TOP of the loop, right after the existing `shutdown` check, add the epoch check (rides the existing `bridge.notified()` PARK_CEIL backstop — spike-proven, no new wakeup plumbing):
```rust
        if epoch_of(service_status_ptr) != expected_epoch {
            return Ok(ApplyOutcome::Reattach);
        }
```
Change the success return from `return Ok(Bytes::from(std::mem::take(&mut payload_buf)));` to:
```rust
                return Ok(ApplyOutcome::Resp(Bytes::from(std::mem::take(&mut payload_buf))));
```
Leave the mismatch and ring-error `Err(...)` paths unchanged.

- [ ] **Step 2: Add the `drive_catchup` free function**

Add after `await_apply_resp` (free function, borrows `&ShmemInner` shared so the caller can `&mut` after it returns; returns the resp for `up_to` plus the reconciled epoch):
```rust
/// Replay committed entries to a reattached service. Called from `apply()` when a
/// reattach is observed (the parked entry `up_to` is, by the single-in-flight
/// invariant, the node's live frontier). Reuses the apply ring + resp ring under
/// the already-held inner lock (no concurrent apply). Returns `(resp_for_up_to,
/// reconciled_epoch)`. An outer loop restarts if the service reattaches AGAIN
/// mid-catch-up.
async fn drive_catchup<S: StateMachine>(
    g: &ShmemInner<S>,
    shutdown: &AtomicBool,
    up_to_log_id: RaftLogId,
) -> Result<(Bytes, u64), io::Error> {
    let up_to = up_to_log_id.index;
    let ss_ptr = g.service_status_ptr;
    let last_purged = g.last_purged.load().ok().flatten().map(|l| l.index).unwrap_or(0);

    loop {
        let epoch = epoch_of(ss_ptr);
        let service_last = service_last_of(ss_ptr);

        // Below the purge boundary: needs snapshot-install (Phase 2).
        if service_last < last_purged {
            return Err(io::Error::other(format!(
                "reconstruct: service at {service_last} below purge boundary {last_purged}; \
                 snapshot-install is Phase 2"
            )));
        }

        // Drop any stale resps left by the dead incarnation (node owns the resp
        // ring's consumer_position).
        g.apply_resp_consumer.lock().discard_backlog();

        // Replay `(from, up_to]`, always including up_to (the parked entry must be
        // applied + acked). Fresh in-memory service => service_last == 0. A
        // self-persisting service already at/above up_to => replay just up_to
        // (idempotent re-apply; log_index is the idempotency key).
        let from = if service_last >= up_to { up_to - 1 } else { service_last };

        let iter = g
            .journal
            .iter_range((from + 1)..(up_to + 1))
            .map_err(|e| io::Error::other(format!("reconstruct: iter_range: {e}")))?;

        let mut last_resp = Bytes::new();
        let mut restart = false;
        for record in iter {
            let (seq, _meta, payload) = record
                .map_err(|e| io::Error::other(format!("reconstruct: journal read: {e}")))?;
            let (entry, _) = bincode::serde::decode_from_slice::<
                <TypeConfig as openraft::RaftTypeConfig>::Entry,
                _,
            >(&payload, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let log_id = entry.log_id;
            // Only Normal entries were ever delivered to the service; Blank /
            // Membership never publish to apply.ring (mirror live apply()).
            let cmd = match entry.payload {
                EntryPayload::Normal(cmd) => cmd,
                _ => continue,
            };
            publish_apply(&g.apply_producer, seq, cmd.as_ref(), log_id, shutdown).await?;
            match await_apply_resp(
                &g.apply_resp_consumer,
                seq,
                log_id,
                shutdown,
                &g.apply_resp_bridge,
                ss_ptr,
                epoch,
            )
            .await?
            {
                ApplyOutcome::Resp(b) => {
                    if seq == up_to {
                        last_resp = b;
                    }
                }
                ApplyOutcome::Reattach => {
                    // Service reattached AGAIN mid-catch-up: restart from the new
                    // epoch/service_last. Progress is guaranteed once it stays up.
                    restart = true;
                    break;
                }
            }
        }
        if restart {
            continue;
        }
        return Ok((last_resp, epoch));
    }
}
```
Notes verified against the codebase: `entry.log_id` is a field and `entry.payload` matches `EntryPayload::Normal(cmd)` exactly as in the existing `apply()`; `cmd.as_ref()` yields `&[u8]` (the `AppCommand`→`Bytes` newtype); the decode mirrors `try_get_log_entries` (`log_storage.rs:235-239`); `iter_range` yields `Result<(seq, meta, payload), _>`. `EntryPayload`, `TypeConfig`, `RaftLogId`, `bincode` are already used in this file.

- [ ] **Step 3: Restructure `apply()`'s Normal arm**

Current arm (≈ lines 240-267): publishes, awaits a `Bytes`, sends to `output_chan_tx`, returns the bytes. Replace the `EntryPayload::Normal(cmd_bytes) => { .. }` arm body with:
```rust
                EntryPayload::Normal(cmd_bytes) => {
                    // Copy the epoch context out before the field borrows below.
                    let ss_ptr = g.service_status_ptr;
                    let expected_epoch = g.last_seen_epoch;
                    let resp: Bytes = if epoch_of(ss_ptr) != expected_epoch {
                        // Service reattached since we last reconciled: catch up
                        // (replays incl. this entry) before publishing it live.
                        let (b, epoch) = drive_catchup(&g, &shutdown, log_id).await?;
                        g.last_seen_epoch = epoch;
                        b
                    } else {
                        publish_apply(&g.apply_producer, log_index, cmd_bytes.as_ref(), log_id, &shutdown).await?;
                        match await_apply_resp(
                            &g.apply_resp_consumer, log_index, log_id, &shutdown,
                            &g.apply_resp_bridge, ss_ptr, expected_epoch,
                        )
                        .await?
                        {
                            ApplyOutcome::Resp(b) => b,
                            ApplyOutcome::Reattach => {
                                let (b, epoch) = drive_catchup(&g, &shutdown, log_id).await?;
                                g.last_seen_epoch = epoch;
                                b
                            }
                        }
                    };
                    // M5: hand off to output_dispatcher exactly once for this entry
                    // (unchanged from the original arm).
                    if let Err(e) = g.output_chan_tx.try_send((log_index, cmd_bytes.clone().into())) {
                        tracing::warn!(log_index, ?e, "output_chan full; replay will catch this");
                    }
                    resp
                }
```
Borrow notes: `drive_catchup(&g, ..)` takes `&ShmemInner` (deref-coerced from `&MutexGuard`) and returns owned values, so the subsequent `g.last_seen_epoch = epoch` and `g.output_chan_tx.try_send(..)` (`&mut g`) compile cleanly — the shared borrow ends at the call's return. `ss_ptr`/`expected_epoch` are `Copy`, captured before the field borrows. Do NOT change the `g.last_applied = Some(log_id)` line above the match (it stays).

- [ ] **Step 4: Build + regression**

Run: `cargo build -p uc_node && cargo test -p uc_node --test m3_shmem_single_node`
Expected: builds; existing single-node tests pass. Because `last_seen_epoch` is seeded to the current epoch (Task 5), the first live apply after attach takes the normal path (epoch unchanged) — no spurious catch-up. A reattach (epoch re-bump) is the only trigger.

- [ ] **Step 5: Commit**
```bash
git add uc_node/src/raft/state_machine_shmem.rs
git commit -m "feat(uc_node): reattach-aware apply — self-driving catch-up (Phase 1 core)"
```

---

## Task 7: Integration test — in-memory SM reconstructed on service-only restart

**Files:** Create `uc_node/tests/reconstruct_reattach.rs`.

The task12 proof: a non-persisting in-memory SM survives a *service* restart while the *node* stays up.

- [ ] **Step 1: Write the test**

Pattern: mirror the node+service bring-up from `uc_node/tests/m3_service_crash.rs` (it already exercises service crash + the node staying up — the closest template) and `m3_shmem_single_node.rs` (SM + submit/query shape). Use a non-persisting `CounterSm` (`apply(n)` adds n; `last_applied()` returns the tracked highest index; in-memory only). Sequence:
```text
1. Bring up node + service (CounterSm). submit Inc(1), Inc(2), Inc(3) (await each).
2. Kill ONLY the service (drop/abort its task + handle) — node stays up.
3. Restart the service with a FRESH CounterSm (same instance_dir; node unchanged).
4. submit Inc(10) (forces an apply → triggers lazy catch-up before applying 10),
   OR submit_query(()) after a short settle (proactive path, if implemented).
5. Assert the counter == 1+2+3+10 == 16 (state reconstructed: the 6 from before
   the crash was replayed, not lost).
```
Provide the full `CounterSm` (as in the superseded Phase-1a plan's Task 7) and the bring-up/submit boilerplate copied from `m3_service_crash.rs`. The decisive assertion: without reconstruction the fresh SM would compute `0 + 10 == 10`; with it, `16`.

- [ ] **Step 2: Run**

Run: `cargo test -p uc_node --test reconstruct_reattach -- --test-threads=1 --nocapture`
Expected: PASS (counter == 16).

- [ ] **Step 3: Commit**
```bash
git add uc_node/tests/reconstruct_reattach.rs
git commit -m "test(uc_node): in-memory SM reconstructed on service-only restart"
```

---

## Task 8: Full verification + clippy

- [ ] **Step 1:** `cargo clippy --workspace -- -D warnings` → clean.
- [ ] **Step 2:** run the affected suites:
```bash
cargo test -p uc_protocol ring::spsc
cargo test -p uc_service runtime::handshake
cargo test -p uc_node --lib runtime::reconstruct
cargo test -p uc_node --test m3_shmem_single_node
cargo test -p uc_node --test m3_service_crash
cargo test -p uc_node --test reconstruct_reattach -- --test-threads=1
```
Expected: all PASS.
- [ ] **Step 3:** `cargo fmt && git add -A && git commit -m "style: cargo fmt (reconstruction phase 1)" || true`

---

## Deferred to a follow-up within Phase 1 (note, don't silently skip)

- **Proactive trigger (idle reattach + query freshness).** Tasks 5-6 implement the
  **lazy** path (catch-up on the next `apply()`). A service that reattaches while the
  node is idle answers *queries* from empty state until the next commit. The proactive
  path — a `service_watcher` extension that, on epoch change with no apply parked,
  takes the `inner` lock and drives catch-up to the current in-memory frontier — is a
  clean follow-on task. Add it once the lazy path is green, or split to Phase 1.5.
  Document the gap if shipping lazy-only first.

## Self-review notes (against the spec)

- **§3a detection** → Task 1 (epoch field) + Task 3 (bump) + Task 5 (node tracks).
- **§3a self-driving apply** → Task 6 (`ApplyOutcome::Reattach` + `drive_catchup`).
- **§3a cursor reset** → Task 2 (`discard_backlog`) + Task 3 (service: apply ring) + Task 6 (node: resp ring).
- **§3a wakeup** → Task 6 epoch check rides the PARK_CEIL backstop (spike-proven).
- **§7 errors** → `NeedsSnapshot` → loud error; replay/journal errors → `io::Error` out of `apply()` only after exhausting reattach handling. NOTE: an `apply()` error is fatal to openraft — ensure only genuinely-unrecoverable conditions error; `Reattach` is an outcome, not an error.
- **Risk — `apply()` returning Err is fatal:** the `NeedsSnapshot` path errors `apply()`, which shuts openraft down. For Phase 1 (no snapshot support) that is the honest behavior (can't reconstruct), but call it out in the test/docs; Phase 2 removes this path.
- **Task 6 is now fully concretized** (no `todo!()`s): `drive_catchup` and the
  `apply()` arm restructure are written out with verified APIs (`entry.log_id` field,
  `EntryPayload::Normal`, `iter_range` + bincode decode mirroring `try_get_log_entries`,
  borrow-sound `&ShmemInner` + Copy epoch context). The remaining real risk is the
  `apply()` arm restructure compiling against the borrow checker exactly as written —
  the implementer should build incrementally and, if it fights, keep the documented
  shape (Copy the epoch context first; `drive_catchup` returns owned values).
- **Single-in-flight invariant is load-bearing:** `drive_catchup` assumes `up_to` (the
  parked entry) is the node frontier. If `apply()` is ever made to publish more than one
  entry concurrently in the future, revisit `drive_catchup`'s range logic.
