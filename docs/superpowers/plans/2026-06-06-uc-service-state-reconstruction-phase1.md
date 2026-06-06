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

## Task 5: Plumb the service_epoch pointer + last-seen epoch into the adapter

**Files:** Modify `uc_node/src/raft/state_machine_shmem.rs`, `uc_node/src/runtime/builder.rs`.

The node-side `apply()` must observe `ServiceStatus.service_epoch`. Plumb a `Send`-able pointer + a tracked last-seen value into `ShmemInner`.

- [ ] **Step 1: Add fields to `ShmemInner` + constructor param**

In `state_machine_shmem.rs`, add to `ShmemInner`:
```rust
    /// Raw pointer to the cnc ServiceStatus.service_epoch atomic. The cnc mmap
    /// is owned by the node Instance/handle and outlives the adapter. Used to
    /// detect a service reattach. None in non-shmem/test paths.
    pub(crate) service_epoch_ptr: Option<EpochPtr>,
    /// The epoch value last reconciled by reconstruction. A change means reattach.
    pub(crate) last_seen_epoch: u64,
```
Add a `Send` wrapper near the top of the file (mirror the existing `SendPtr` pattern in builder.rs):
```rust
/// Send wrapper for the `*const AtomicU64` service_epoch pointer (the AtomicU64
/// is Sync; the cnc mmap outlives the adapter).
#[derive(Clone, Copy)]
pub(crate) struct EpochPtr(pub(crate) *const std::sync::atomic::AtomicU64);
// SAFETY: points into the cnc mmap, which is Sync and outlives the adapter.
unsafe impl Send for EpochPtr {}
unsafe impl Sync for EpochPtr {}
```
Add a constructor parameter `service_epoch_ptr: Option<EpochPtr>` to `ShmemAdaptedStateMachine::new` and initialize `service_epoch_ptr` + `last_seen_epoch: 0` in the `ShmemInner { .. }` literal. (Seed `last_seen_epoch` to 0; the first reattach bump makes epoch ≥ 1, and a fresh first-attach catch-up is `Nothing` anyway.)

- [ ] **Step 2: Add an epoch read helper**
```rust
impl<S: StateMachine> ShmemInner<S> {
    /// Current service epoch (0 if no pointer). SAFETY: pointer into the live
    /// cnc mmap.
    fn current_epoch(&self) -> u64 {
        match self.service_epoch_ptr {
            Some(EpochPtr(p)) => unsafe { (*p).load(std::sync::atomic::Ordering::Acquire) },
            None => 0,
        }
    }
}
```

- [ ] **Step 3: Pass the pointer from the builder**

In `builder.rs`, where `ShmemAdaptedStateMachine::new(...)` is called, compute the epoch pointer from `service_status.0` and pass `Some(EpochPtr(&(*service_status.0).service_epoch as *const _))`. Concretely, just before the `new(...)` call:
```rust
                // SAFETY: same cnc mmap as service_status; outlives the adapter.
                let service_epoch_ptr = {
                    let status = unsafe { &*service_status.0 };
                    crate::raft::state_machine_shmem::EpochPtr(
                        &status.service_epoch as *const std::sync::atomic::AtomicU64,
                    )
                };
```
and add `Some(service_epoch_ptr)` as the new final arg to `ShmemAdaptedStateMachine::new(...)`. Any other caller of `new` (tests) passes `None`.

- [ ] **Step 4: Build** — `cargo build -p uc_node` (expect dead-code warnings for `current_epoch`/fields until Task 6; that's fine). **Commit:**
```bash
git add uc_node/src/raft/state_machine_shmem.rs uc_node/src/runtime/builder.rs
git commit -m "feat(uc_node): plumb service_epoch pointer into the shmem adapter"
```

---

## Task 6: Reattach-aware `await_apply_resp` + self-driving catch-up in `apply()`

**Files:** Modify `uc_node/src/raft/state_machine_shmem.rs`.

The core. `await_apply_resp` returns a `Reattach` outcome instead of a resp when the epoch changes; `apply()`'s Normal branch, on `Reattach`, runs catch-up and uses the replay of entry N to satisfy the original wait. Needs a journal reader — pass an `Arc<Journal>` (already cloned as `journal_for_replay` in the builder) into the adapter, or read via a stored handle.

- [ ] **Step 1: Add `ApplyOutcome` and make `await_apply_resp` epoch-aware**

Define near the helpers:
```rust
enum ApplyOutcome {
    Resp(Bytes),
    /// The service reattached (epoch changed) before this resp arrived.
    Reattach,
}
```
Change `await_apply_resp` to take the current epoch context and return `Result<ApplyOutcome, io::Error>`. Add an `epoch_changed: &dyn Fn() -> bool` (or pass `service_epoch_ptr: Option<EpochPtr>` + `expected_epoch: u64`) parameter; at the TOP of the loop, before/after the shutdown check:
```rust
        if epoch_changed() {
            return Ok(ApplyOutcome::Reattach);
        }
```
Keep the existing resp path but wrap the success return as `Ok(ApplyOutcome::Resp(...))`. The epoch check rides the existing `bridge.notified()` PARK_CEIL backstop (spike-proven ~8ms), so the parked await wakes and re-checks within ~2ms even with no resp traffic — no new wakeup plumbing.

- [ ] **Step 2: Add a journal handle to the adapter**

Add `pub(crate) journal: Arc<ultima_journal::Journal>` to `ShmemInner`, a constructor param, and pass `journal_for_replay.clone()` from the builder (it is already cloned there for the output-replay watcher). (Reading entries directly via the journal avoids needing a `&mut JournalLogStorage`; decode the same way `try_get_log_entries` does — see `log_storage.rs:226-243` for the bincode decode of an `Entry`.)

- [ ] **Step 3: Catch-up driver method on the adapter**
```rust
    /// Replay committed entries to a reattached service. Called from apply() when
    /// a Reattach is observed. Holds the inner lock for the duration (gating any
    /// concurrent apply). Returns the resp bytes for `up_to` (the entry apply()
    /// was waiting on), or an error.
    async fn drive_catchup(
        &self,
        g: &mut tokio::sync::MutexGuard<'_, ShmemInner<S>>,
        up_to_log_id: RaftLogId,
    ) -> Result<Bytes, io::Error> {
        let up_to = up_to_log_id.index;
        // Reconcile epoch + read the reattached service's reported last_applied.
        let new_epoch = g.current_epoch();
        let service_last = /* read ServiceStatus.last_applied via a plumbed ptr,
            mirroring service_epoch_ptr; add a `service_last_applied_ptr` field the
            same way in Task 5 */ 0u64;
        let last_purged = /* g.journal / last_purged index — plumb the last_purged
            StableValue handle like the journal, or read from a stored LogStorageHandles */ 0u64;
        // Drop stale resps from the dead incarnation (node owns resp consumer_position).
        g.apply_resp_consumer.lock().discard_backlog();

        let node_frontier = up_to; // single-in-flight: the parked entry IS the frontier
        let mut last_resp = Bytes::new();
        match crate::runtime::reconstruct::decide_catchup_source(service_last, node_frontier, last_purged) {
            crate::runtime::reconstruct::CatchupSource::Nothing => {}
            crate::runtime::reconstruct::CatchupSource::NeedsSnapshot { .. } => {
                return Err(io::Error::other(
                    "reconstruct: service below purge boundary; snapshot-install is Phase 2",
                ));
            }
            crate::runtime::reconstruct::CatchupSource::LogReplay { from, to } => {
                for idx in (from + 1)..=to {
                    // read entry `idx` from the journal, decode, skip non-Normal
                    let cmd_bytes: Bytes = /* journal read + decode at idx; Normal only */ todo!();
                    publish_apply(&g.apply_producer, idx, cmd_bytes.as_ref(), up_to_log_id, &self.shutdown).await?;
                    match await_apply_resp(&g.apply_resp_consumer, idx, up_to_log_id,
                        &self.shutdown, &g.apply_resp_bridge, /* epoch ctx */).await? {
                        ApplyOutcome::Resp(b) => last_resp = b,
                        ApplyOutcome::Reattach => {
                            // Service crashed AGAIN mid-catch-up: restart catch-up.
                            // (bounded recursion / loop; reconcile epoch and retry)
                            todo!("re-enter catch-up for the newer epoch");
                        }
                    }
                }
            }
        }
        g.last_seen_epoch = new_epoch;
        Ok(last_resp)
    }
```
> **IMPLEMENTER NOTE — this method has two `todo!()`s that MUST be filled, they are not optional:**
> 1. **journal read+decode at `idx`** — mirror `JournalLogStorage::try_get_log_entries` (`log_storage.rs:226-243`): `g.journal.read(idx)` (or `iter_range`), bincode-decode to `Entry<TypeConfig>`, match `EntryPayload::Normal(cmd) => cmd.0`, skip `Blank`/`Membership` (advance the loop without publishing). If non-Normal, `continue` without publishing (the service's `last_applied` jumps over them, matching live apply).
> 2. **second-reattach handling** — if a `Reattach` is observed mid-catch-up, reconcile the new epoch + new `service_last`, reset the apply-resp backlog again, and restart the replay loop from the new `service_last`. Implement as an outer `loop` around the replay with a re-read of epoch/service_last at the top, rather than recursion. Bound is natural (each restart makes progress once the service stays up).
>
> These are genuinely intricate; if the journal-read API or the entry decode is unclear, STOP and report NEEDS_CONTEXT with the actual `log_storage.rs` decode code rather than guessing.

- [ ] **Step 4: Wire `apply()`'s Normal branch to handle Reattach**

In `apply()`, the `EntryPayload::Normal(cmd_bytes)` arm currently does `publish_apply` then `await_apply_resp`. Restructure:
```rust
                EntryPayload::Normal(cmd_bytes) => {
                    // Lazy reattach check: if the service reattached since we last
                    // reconciled, catch it up before applying this entry.
                    if g.current_epoch() != g.last_seen_epoch {
                        let resp = self.drive_catchup(&mut g, log_id).await?;
                        // drive_catchup replayed up to and including this entry.
                        if let Some(r) = responder { r.send(resp); }
                        continue; // or fall through appropriately
                    }
                    publish_apply(&g.apply_producer, log_index, cmd_bytes.as_ref(), log_id, &self.shutdown).await?;
                    match await_apply_resp(&g.apply_resp_consumer, log_index, log_id,
                        &self.shutdown, &g.apply_resp_bridge, /* epoch ctx */).await? {
                        ApplyOutcome::Resp(b) => b,
                        ApplyOutcome::Reattach => self.drive_catchup(&mut g, log_id).await?,
                    }
                }
```
> **IMPLEMENTER NOTE:** the control flow here (the `continue` vs producing `resp_bytes`, the output_chan_tx `try_send` still applying to the entry, the `g.last_applied = Some(log_id)` already set earlier) needs care to match the existing arm's post-processing. Keep the existing `output_chan_tx.try_send` for the entry and the `responder.send` exactly once. Prefer restructuring so `drive_catchup` returns the resp and the arm's tail (`output_chan_tx`, `responder.send`) runs once for `log_index`. If this restructure fights the borrow checker (holding `&mut g` across `drive_catchup` which also takes `&mut g`), pass the already-held guard into `drive_catchup` (as the signature shows) rather than re-locking.

- [ ] **Step 5: Build + run the existing shmem tests (regression)**

Run: `cargo build -p uc_node && cargo test -p uc_node --test m3_shmem_single_node`
Expected: builds; existing single-node tests pass (no reattach occurs, epoch stays 1 after first attach so the lazy check is false).

- [ ] **Step 6: Commit**
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
- **Tasks 6's two `todo!()`s and the apply() restructure are the risk centers** — they are explicitly flagged with IMPLEMENTER NOTEs and NEEDS_CONTEXT escalation guidance, not left as silent placeholders.
