# Service-State Reconstruction — Phase 2a (functional snapshot path) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the node a *real* bidirectional snapshot path so (a) log purge is backed by an actual service snapshot, and (b) a service that reattaches below the purge boundary is reconstructed via snapshot-install + tail-replay — instead of today's `NeedsSnapshot` hard error.

**Architecture:** A dedicated SPSC control-ring pair `service/snapshot.ring` (node→service) + `snapshot_resp.ring` (service→node) carries `BUILD_SNAPSHOT`/`SNAPSHOT_BUILT` and `INSTALL_SNAPSHOT`/`SNAPSHOT_INSTALLED`. Bytes flow through a separate `service/snapshot.region` file (header + crc; cross-process ordering provided by the control-ring ack). A new service-side snapshot loop calls the **existing blocking** `build_snapshot`/`install_snapshot` under the SM `RwLock`. Node-side `build_snapshot` drives the service BUILD (real bytes → safe purge); `drive_catchup`'s `NeedsSnapshot` drives INSTALL then tail-replays `(snapshot_index, up_to]`. **No `StateMachine` trait change** (that is Phase 2b).

**Tech Stack:** Rust, openraft 0.10, shmem SPSC rings, `ultima_db::snapshot_stream` (via `StoreStateMachine`), tokio.

**Spec:** `docs/superpowers/specs/2026-06-06-uc-service-state-reconstruction-design.md` §5, **§5a** (the concrete 2a design), §3 (phasing).

**Out of scope (Phase 2b / 3):** the `freeze`/`stream_snapshot` async trait change; reverting `RegisterSm`; lincheck capstone.

**Accepted 2a limitation (document, don't fix here):** BUILD is blocking — the node-side `build_snapshot` holds the `inner` lock across the service round-trip, and the service-side build holds the SM read lock — so a snapshot stalls applies for its duration. Phase 2b removes this.

---

## File structure

- **Modify** `uc_protocol/src/frames/snapshot.rs` — add `INSTALL_SNAPSHOT`/`SNAPSHOT_INSTALLED` + encode/decode.
- **Create** `uc_protocol/src/snapshot_region.rs` (+ `pub mod` in `lib.rs`) — `SnapshotRegion` write/read helper (header + crc).
- **Modify** `uc_node/src/ipc/service_link.rs` — `SNAPSHOT_RING_CAP/MAX_MSG` + `snapshot_producer`/`snapshot_resp_consumer` fields + ring creation.
- **Modify** `uc_service/src/runtime/attach.rs` — matching `AttachedRings` fields + ring open.
- **Modify** `uc_node/src/raft/state_machine_shmem.rs` — `ShmemInner` snapshot ring fields + `new()` params + bridge; node-side BUILD (replace degenerate `ShmemSnapshotBuilder::build_snapshot`); node-side INSTALL (replace `drive_catchup` `NeedsSnapshot` error); snapshot publish/await helpers.
- **Modify** `uc_node/src/runtime/builder.rs` — destructure + pass the snapshot halves.
- **Create** `uc_service/src/runtime/snapshot_loop.rs` (+ `mod`) — service snapshot-control loop (BUILD/INSTALL handlers).
- **Modify** `uc_service/src/runtime/service.rs` — spawn the snapshot loop; `Service` field + shutdown join.
- **Create** `uc_node/tests/reconstruct_snapshot.rs` — below-purge reattach reconstruction (covers BUILD+INSTALL end-to-end).

---

## Task 1: INSTALL/INSTALLED snapshot frames

**Files:** Modify `uc_protocol/src/frames/snapshot.rs`.

- [ ] **Step 1: Write the failing test**

Add to (or create) `#[cfg(test)] mod tests` in `uc_protocol/src/frames/snapshot.rs`:
```rust
    #[test]
    fn install_extra_round_trip() {
        assert_eq!(decode_extra_install_snapshot(encode_extra_install_snapshot(42)), 42);
        assert_eq!(decode_extra_snapshot_installed(encode_extra_snapshot_installed(99)), 99);
    }
```

- [ ] **Step 2: Run, confirm fail** — `cargo test -p uc_protocol frames::snapshot::tests::install_extra_round_trip` → FAIL (fns missing).

- [ ] **Step 3: Add the constants + helpers** (after the existing `BUILD_SNAPSHOT`/`SNAPSHOT_BUILT` block, mirroring `decode_extra_snapshot_built`):
```rust
/// node → service: "install the snapshot now in `snapshot.region`." `header_extra`
/// carries the snapshot's last_log_id index (the index the service will be at after).
pub const MSG_TYPE_INSTALL_SNAPSHOT: u16 = 102;
/// service → node: "snapshot installed." `header_extra` carries the new last_applied.
pub const MSG_TYPE_SNAPSHOT_INSTALLED: u16 = 103;

#[inline]
pub fn encode_extra_install_snapshot(snapshot_index: u64) -> [u8; 8] {
    snapshot_index.to_le_bytes()
}
#[inline]
pub fn decode_extra_install_snapshot(extra: [u8; 8]) -> u64 {
    u64::from_le_bytes(extra)
}
#[inline]
pub fn encode_extra_snapshot_installed(new_last_applied: u64) -> [u8; 8] {
    new_last_applied.to_le_bytes()
}
#[inline]
pub fn decode_extra_snapshot_installed(extra: [u8; 8]) -> u64 {
    u64::from_le_bytes(extra)
}
```
Confirm `100`/`101` are the existing BUILD/BUILT values so `102`/`103` don't collide. Update the module doc to note the frames flow over the `snapshot.ring` pair (not the cnc control rings).

- [ ] **Step 4: Run, confirm pass.** Commit:
```bash
git add uc_protocol/src/frames/snapshot.rs
git commit -m "feat(uc_protocol): INSTALL_SNAPSHOT/SNAPSHOT_INSTALLED frames (Phase 2a)"
```

---

## Task 2: `SnapshotRegion` byte-transport helper

**Files:** Create `uc_protocol/src/snapshot_region.rs`; modify `uc_protocol/src/lib.rs` (add `pub mod snapshot_region;`).

A fixed-header file holding snapshot bytes. Writer truncates+writes; reader validates + returns bytes. Cross-process ordering is provided by the control-ring ack (caller sends the frame only after `write`, reads only after receiving the frame), so no internal fencing.

- [ ] **Step 1: Write the failing test**

Create `uc_protocol/src/snapshot_region.rs`:
```rust
//! `snapshot.region`: a separate file under the instance dir carrying snapshot
//! bytes between node and service (Phase 2a). Header: magic, format_ver, byte_len,
//! snapshot last_log_id index, crc32 of the bytes. Cross-process ordering is the
//! CALLER's responsibility via the snapshot control-ring ack (write region → send
//! BUILT/INSTALL frame → peer reads region only after receiving the frame).

use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;

const MAGIC: [u8; 8] = *b"UCSNAPRG";
const FORMAT_VER: u32 = 1;
/// magic(8) + format_ver(4) + _pad(4) + byte_len(8) + snapshot_index(8) + crc32(4) + _pad(4)
const HEADER_LEN: usize = 40;

#[derive(Debug, thiserror::Error)]
pub enum SnapshotRegionError {
    #[error("snapshot region io: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot region bad magic")]
    BadMagic,
    #[error("snapshot region unsupported format {0}")]
    BadFormat(u32),
    #[error("snapshot region crc mismatch: header {header:#x} computed {computed:#x}")]
    CrcMismatch { header: u32, computed: u32 },
    #[error("snapshot region truncated: header says {expected} bytes, file has {actual}")]
    Truncated { expected: u64, actual: u64 },
}

/// Write `bytes` (a snapshot at `snapshot_index`) to `path`, replacing any prior
/// content. Truncates the file to exactly HEADER_LEN + bytes.len().
pub fn write(path: &Path, snapshot_index: u64, bytes: &[u8]) -> Result<(), SnapshotRegionError> {
    let crc = crc32fast::hash(bytes);
    let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(path)?;
    let mut header = [0u8; HEADER_LEN];
    header[0..8].copy_from_slice(&MAGIC);
    header[8..12].copy_from_slice(&FORMAT_VER.to_le_bytes());
    header[16..24].copy_from_slice(&(bytes.len() as u64).to_le_bytes());
    header[24..32].copy_from_slice(&snapshot_index.to_le_bytes());
    header[32..36].copy_from_slice(&crc.to_le_bytes());
    f.write_all(&header)?;
    f.write_all(bytes)?;
    f.flush()?;
    Ok(())
}

/// Read + validate the region. Returns `(snapshot_index, bytes)`.
pub fn read(path: &Path) -> Result<(u64, Vec<u8>), SnapshotRegionError> {
    let mut f = OpenOptions::new().read(true).open(path)?;
    let mut header = [0u8; HEADER_LEN];
    f.read_exact(&mut header)?;
    if header[0..8] != MAGIC {
        return Err(SnapshotRegionError::BadMagic);
    }
    let fmt = u32::from_le_bytes(header[8..12].try_into().unwrap());
    if fmt != FORMAT_VER {
        return Err(SnapshotRegionError::BadFormat(fmt));
    }
    let byte_len = u64::from_le_bytes(header[16..24].try_into().unwrap());
    let snapshot_index = u64::from_le_bytes(header[24..32].try_into().unwrap());
    let crc = u32::from_le_bytes(header[32..36].try_into().unwrap());
    let mut bytes = Vec::with_capacity(byte_len as usize);
    f.read_to_end(&mut bytes)?;
    if bytes.len() as u64 != byte_len {
        return Err(SnapshotRegionError::Truncated { expected: byte_len, actual: bytes.len() as u64 });
    }
    let computed = crc32fast::hash(&bytes);
    if computed != crc {
        return Err(SnapshotRegionError::CrcMismatch { header: crc, computed });
    }
    Ok((snapshot_index, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round_trip() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let data = vec![7u8; 5000];
        write(tmp.path(), 123, &data).unwrap();
        let (idx, got) = read(tmp.path()).unwrap();
        assert_eq!(idx, 123);
        assert_eq!(got, data);
    }
    #[test]
    fn crc_mismatch_detected() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        write(tmp.path(), 1, b"hello").unwrap();
        // Corrupt the last payload byte.
        let mut buf = std::fs::read(tmp.path()).unwrap();
        let n = buf.len();
        buf[n - 1] ^= 0xFF;
        std::fs::write(tmp.path(), &buf).unwrap();
        assert!(matches!(read(tmp.path()), Err(SnapshotRegionError::CrcMismatch { .. })));
    }
    #[test]
    fn rewrite_shrinks() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        write(tmp.path(), 1, &vec![1u8; 9000]).unwrap();
        write(tmp.path(), 2, b"tiny").unwrap(); // truncates
        let (idx, got) = read(tmp.path()).unwrap();
        assert_eq!(idx, 2);
        assert_eq!(got, b"tiny");
    }
}
```
Add `pub mod snapshot_region;` to `uc_protocol/src/lib.rs`. Confirm `crc32fast`, `thiserror`, and `tempfile` (dev) are deps of `uc_protocol` (the ring code uses crc32fast + thiserror; tempfile is a dev-dep used in ring tests). If `crc32fast` isn't a normal dep, add it.

- [ ] **Step 2: Run, confirm fail then implement is already inline → run pass**

Run: `cargo test -p uc_protocol snapshot_region` — Expected: 3 pass.

- [ ] **Step 3: Commit**
```bash
git add uc_protocol/src/snapshot_region.rs uc_protocol/src/lib.rs
git commit -m "feat(uc_protocol): SnapshotRegion file transport helper (Phase 2a)"
```

---

## Task 3: snapshot ring pair (node creates, service opens)

**Files:** Modify `uc_node/src/ipc/service_link.rs`, `uc_service/src/runtime/attach.rs`.

- [ ] **Step 1: service_link.rs — constants + fields + creation**

Add constants near the existing ring constants:
```rust
pub const SNAPSHOT_RING_CAP: u64 = 16 * 1024 * 1024;
pub const SNAPSHOT_RING_MAX_MSG: u32 = 4 * 1024 * 1024;
```
Add to the `ServiceLink` struct:
```rust
    pub snapshot_producer: SpscProducer,
    pub snapshot_resp_consumer: SpscConsumer,
```
In the create fn (the one creating `apply`/`query`/`output` rings via `SpscRing::create(...)`), mirror the OUTPUT block:
```rust
        let snapshot = SpscRing::create(
            &service_dir.join("snapshot.ring"),
            SNAPSHOT_RING_CAP,
            SNAPSHOT_RING_MAX_MSG,
        )?;
        let snapshot_resp = SpscRing::create(
            &service_dir.join("snapshot_resp.ring"),
            SNAPSHOT_RING_CAP,
            SNAPSHOT_RING_MAX_MSG,
        )?;
```
and in the split section + struct construction:
```rust
        let (snapshot_producer, _) = snapshot.into_split();
        let (_, snapshot_resp_consumer) = snapshot_resp.into_split();
```
add `snapshot_producer, snapshot_resp_consumer,` to the returned `ServiceLink { .. }`.

- [ ] **Step 2: attach.rs — fields + open**

Add to `AttachedRings`:
```rust
    pub snapshot_consumer: SpscConsumer,
    pub snapshot_resp_producer: SpscProducer,
```
In `attach()`, mirror the output open + split:
```rust
        let snapshot_ring = SpscRing::open(&service_dir.join("snapshot.ring"))?;
        let snapshot_resp_ring = SpscRing::open(&service_dir.join("snapshot_resp.ring"))?;
        let (_, snapshot_consumer) = snapshot_ring.into_split();
        let (snapshot_resp_producer, _) = snapshot_resp_ring.into_split();
```
add `snapshot_consumer, snapshot_resp_producer,` to the returned `AttachedRings { .. }`.

- [ ] **Step 3: Build** — `cargo build -p uc_node -p uc_service`. Expect: builds (the new ServiceLink fields are unused until Task 4/5 → dead_code/unused warnings are fine for now; if `-D warnings` in a build script bites, they're plain `cargo build` warnings, not clippy). Existing tests that construct `ServiceLink`/`AttachedRings` literals (if any) must be updated — grep `ServiceLink {`/`AttachedRings {` and fix.

- [ ] **Step 4: Commit**
```bash
git add uc_node/src/ipc/service_link.rs uc_service/src/runtime/attach.rs
git commit -m "feat(ipc): snapshot.ring/snapshot_resp.ring pair (Phase 2a transport)"
```

---

## Task 4: plumb snapshot ring halves into the node adapter

**Files:** Modify `uc_node/src/raft/state_machine_shmem.rs`, `uc_node/src/runtime/builder.rs`.

- [ ] **Step 1: ShmemInner fields**

Add to `ShmemInner` (after `apply_resp_bridge`):
```rust
    /// Phase 2a snapshot control ring (node→service BUILD/INSTALL).
    pub(crate) snapshot_producer: PlMutex<SpscProducer>,
    /// Phase 2a snapshot resp ring (service→node BUILT/INSTALLED).
    pub(crate) snapshot_resp_consumer: PlMutex<SpscConsumer>,
    /// Wakes the snapshot-resp await (mirrors apply_resp_bridge).
    pub(crate) snapshot_resp_bridge: NotifyBridge,
```

- [ ] **Step 2: `new()` params + init**

Add params to `ShmemAdaptedStateMachine::new` (after the apply ring params, before/around the others): `snapshot_producer: SpscProducer`, `snapshot_resp_consumer: SpscConsumer`. Build the bridge before the struct literal, mirroring `apply_resp_bridge` (the investigation noted it's built at ~line 217):
```rust
        let snapshot_resp_bridge =
            NotifyBridge::spawn(snapshot_resp_consumer.wait_handle(), "snapshot_resp");
```
and init the three fields in `ShmemInner { .. }`:
```rust
            snapshot_producer: PlMutex::new(snapshot_producer),
            snapshot_resp_consumer: PlMutex::new(snapshot_resp_consumer),
            snapshot_resp_bridge,
```
Add `#[allow(clippy::too_many_arguments)]` is already on `new` (Phase 1) — keep it.

- [ ] **Step 3: builder.rs — destructure + pass**

Add `snapshot_producer` and `snapshot_resp_consumer` to the `ServiceLink { .. }` destructure, and pass them into `ShmemAdaptedStateMachine::new(...)` in the matching arg positions.

- [ ] **Step 4: Fix other `new()` callers** — `rg -n "ShmemAdaptedStateMachine::new"`. The shmem test caller(s) (`uc_node/tests/shmem_state_machine.rs`) must create a snapshot ring pair (use `SpscRing::create(..).into_split()` with the SNAPSHOT consts, or a tempfile pair) and pass the producer + resp consumer. Mirror how that test already builds the apply rings.

- [ ] **Step 5: Build** — `cargo build -p uc_node` (dead_code warnings for the 3 new fields until Tasks 6/7 — expected). Commit:
```bash
git add uc_node/src/raft/state_machine_shmem.rs uc_node/src/runtime/builder.rs uc_node/tests/shmem_state_machine.rs
git commit -m "feat(uc_node): plumb snapshot ring + bridge into the shmem adapter"
```

---

## Task 5: service-side snapshot-control loop

**Files:** Create `uc_service/src/runtime/snapshot_loop.rs`; modify `uc_service/src/runtime/mod.rs` (add `mod snapshot_loop;` / re-export), `uc_service/src/runtime/service.rs`.

A tokio task (mirrors `query_loop`) consuming `snapshot.ring`. On `BUILD_SNAPSHOT`: `sm.read().await`, blocking `build_snapshot(&mut Vec)`, `snapshot_region::write`, reply `SNAPSHOT_BUILT{built_index}`. On `INSTALL_SNAPSHOT`: `snapshot_region::read`, `sm.write().await`, `install_snapshot(Cursor)`, reply `SNAPSHOT_INSTALLED{new_last_applied}`. The snapshot.region path is `instance_dir/service/snapshot.region`.

- [ ] **Step 1: Create `snapshot_loop.rs`**
```rust
//! Service-side snapshot control loop (Phase 2a). Consumes `snapshot.ring`:
//! BUILD_SNAPSHOT → build into snapshot.region; INSTALL_SNAPSHOT → install from it.
//! Blocking build/install under the SM RwLock (2a; Phase 2b makes build async).

use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use uc_protocol::frames::snapshot::{
    MSG_TYPE_BUILD_SNAPSHOT, MSG_TYPE_INSTALL_SNAPSHOT, MSG_TYPE_SNAPSHOT_BUILT,
    MSG_TYPE_SNAPSHOT_INSTALLED, decode_extra_install_snapshot, encode_extra_snapshot_built,
    encode_extra_snapshot_installed,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};
use uc_protocol::snapshot_region;

use crate::StateMachine;

const IDLE_BACKOFF: Duration = Duration::from_millis(2);

pub struct SnapshotLoopHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn_snapshot_loop<S>(
    sm: Arc<RwLock<S>>,
    consumer: SpscConsumer,
    resp_producer: SpscProducer,
    region_path: PathBuf,
) -> SnapshotLoopHandle
where
    S: StateMachine,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);
    let join = tokio::spawn(snapshot_task_body::<S>(
        sm,
        consumer,
        resp_producer,
        region_path,
        stop_for_task,
    ));
    SnapshotLoopHandle { join, stop }
}

async fn snapshot_task_body<S>(
    sm: Arc<RwLock<S>>,
    mut consumer: SpscConsumer,
    mut resp_producer: SpscProducer,
    region_path: PathBuf,
    stop: Arc<AtomicBool>,
) where
    S: StateMachine,
{
    let mut payload_buf: Vec<u8> = Vec::with_capacity(64);
    while !stop.load(Ordering::Relaxed) {
        match consumer.try_read(&mut payload_buf) {
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_BUILD_SNAPSHOT => {
                // Build at the SM's current last_applied (read lock; 2a blocking).
                let (built_index, bytes) = {
                    let guard = sm.read().await;
                    let mut buf: Vec<u8> = Vec::new();
                    match guard.build_snapshot(&mut buf) {
                        Ok(idx) => (idx, buf),
                        Err(e) => {
                            tracing::error!(error = %e, "snapshot build failed");
                            // No partial reply; node's await times out / errors.
                            continue;
                        }
                    }
                };
                if let Err(e) = snapshot_region::write(&region_path, built_index, &bytes) {
                    tracing::error!(error = %e, "snapshot.region write failed");
                    continue;
                }
                publish_resp(
                    &mut resp_producer,
                    MSG_TYPE_SNAPSHOT_BUILT,
                    encode_extra_snapshot_built(built_index),
                    &stop,
                );
            }
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_INSTALL_SNAPSHOT => {
                let target = decode_extra_install_snapshot(rec.header_extra);
                let bytes = match snapshot_region::read(&region_path) {
                    Ok((_idx, b)) => b,
                    Err(e) => {
                        tracing::error!(error = %e, "snapshot.region read failed");
                        continue;
                    }
                };
                let new_last_applied = {
                    let mut guard = sm.write().await;
                    match guard.install_snapshot(&mut Cursor::new(bytes)) {
                        Ok(li) => li,
                        Err(e) => {
                            tracing::error!(error = %e, "snapshot install failed");
                            continue;
                        }
                    }
                };
                let _ = target; // target == new_last_applied expected; reply the actual
                publish_resp(
                    &mut resp_producer,
                    MSG_TYPE_SNAPSHOT_INSTALLED,
                    encode_extra_snapshot_installed(new_last_applied),
                    &stop,
                );
            }
            Ok(Some(rec)) => {
                tracing::warn!(msg_type = rec.msg_type, "snapshot ring: unexpected frame");
            }
            Ok(None) => tokio::time::sleep(IDLE_BACKOFF).await,
            Err(e) => {
                tracing::warn!(error = %e, "snapshot ring read error");
                tokio::time::sleep(IDLE_BACKOFF).await;
            }
        }
    }
}

fn publish_resp(producer: &mut SpscProducer, msg_type: u16, extra: [u8; 8], stop: &AtomicBool) {
    loop {
        match producer.try_write(msg_type, 0, extra, &[]) {
            Ok(()) => return,
            Err(RingError::Full) => {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::yield_now();
            }
            Err(e) => panic!("snapshot_resp write: {e}"),
        }
    }
}
```
ADAPT: confirm the `RecordHeader` field is `header_extra` (per apply frames) and `try_read` returns `Ok(Some(rec))` with `rec.msg_type`/`rec.header_extra`; confirm `try_write(msg_type, flags, header_extra, payload)` arg order (mirror `publish_response` in apply_loop.rs). Confirm `StateMachine::build_snapshot(&self, &mut dyn Write)` / `install_snapshot(&mut self, &mut dyn Read)` signatures (they're unchanged in 2a). `Vec<u8>: Write` and `Cursor<Vec<u8>>: Read`.

- [ ] **Step 2: register module** — in `uc_service/src/runtime/mod.rs` add `mod snapshot_loop;` and re-export `spawn_snapshot_loop`/`SnapshotLoopHandle` as the other loops are.

- [ ] **Step 3: wire into `service.rs run()`**

After the `query`/`liveness` spawns, add:
```rust
        let snapshot_region_path = self.config.instance_dir.join("service").join("snapshot.region");
        let snapshot_control = spawn_snapshot_loop(
            Arc::clone(&sm_shared),
            attached.snapshot_consumer,
            attached.snapshot_resp_producer,
            snapshot_region_path,
        );
```
Add `snapshot_control: SnapshotLoopHandle,` to the `Service` struct and `snapshot_control,` to its constructor. In `Service::shutdown`, mirror the query loop (async join):
```rust
        self.snapshot_control.stop.store(true, Ordering::Relaxed);
        let _ = self.snapshot_control.join.await;
```
Place the stop+join consistently with the existing order (stop all, then join). Confirm `self.config.instance_dir` is accessible in `run()` (it is — used for attach).

- [ ] **Step 4: Build + commit**

`cargo build -p uc_service` (clean). Commit:
```bash
git add uc_service/src/runtime/snapshot_loop.rs uc_service/src/runtime/mod.rs uc_service/src/runtime/service.rs
git commit -m "feat(uc_service): snapshot-control loop — BUILD/INSTALL handlers (Phase 2a)"
```

---

## Task 6: node-side BUILD — drive the real service snapshot

**Files:** Modify `uc_node/src/raft/state_machine_shmem.rs`.

Replace the degenerate `ShmemSnapshotBuilder::build_snapshot` so it asks the service to build, reads the region, and returns real bytes (+ persists `snapshot_meta`/on-disk so purge is backed by it). Add snapshot publish/await helpers mirroring `publish_apply`/`await_apply_resp` (no epoch-awareness needed).

- [ ] **Step 1: add snapshot ring helpers** (free fns near `publish_apply`/`await_apply_resp`):
```rust
async fn publish_snapshot_cmd(
    producer: &PlMutex<SpscProducer>,
    msg_type: u16,
    extra: [u8; 8],
    shutdown: &AtomicBool,
) -> Result<(), io::Error> {
    loop {
        if shutdown.load(Ordering::Acquire) {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "snapshot cmd: shutting down"));
        }
        let r = { producer.lock().try_write(msg_type, 0, extra, &[]) };
        match r {
            Ok(()) => return Ok(()),
            Err(uc_protocol::ring::RingError::Full) => {
                tokio::time::sleep(FULL_BACKOFF).await;
            }
            Err(e) => return Err(io::Error::other(format!("snapshot cmd write: {e}"))),
        }
    }
}

/// Await a snapshot resp frame of `expected_msg_type`. Returns its header_extra u64.
async fn await_snapshot_resp(
    consumer: &PlMutex<SpscConsumer>,
    expected_msg_type: u16,
    shutdown: &AtomicBool,
    bridge: &NotifyBridge,
) -> Result<u64, io::Error> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        if shutdown.load(Ordering::Acquire) {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "snapshot resp: shutting down"));
        }
        let read = { consumer.lock().try_read(&mut buf) };
        match read {
            Ok(Some(rec)) if rec.msg_type == expected_msg_type => {
                return Ok(u64::from_le_bytes(rec.header_extra));
            }
            Ok(Some(rec)) => {
                tracing::warn!(msg_type = rec.msg_type, "unexpected frame on snapshot_resp ring");
            }
            Ok(None) => bridge.notified().await,
            Err(e) => return Err(io::Error::other(format!("snapshot_resp read: {e}"))),
        }
    }
}
```
(`FULL_BACKOFF` already exists in this file. `rec.header_extra` is `[u8;8]`.)

- [ ] **Step 2: replace `ShmemSnapshotBuilder::build_snapshot`**

Read the current degenerate impl (investigation showed it locks `inner`, calls degenerate `g.sm.build_snapshot`, builds meta, stores `current_snapshot`, returns `Cursor`). Replace its body with a drive-the-service version. Note `ShmemSnapshotBuilder` holds `inner: Arc<TokioMutex<ShmemInner<S>>>` and `build_snapshot` is `async`. It needs the shutdown flag + the snapshot.region path. **Add a `shutdown: Arc<AtomicBool>` and `region_path: PathBuf` field to `ShmemSnapshotBuilder`** (set in `get_snapshot_builder` from `self.shutdown.clone()` and a region path plumbed into `ShmemInner`/the adapter — store `snapshot_region_path: PathBuf` in `ShmemInner`, derived in `new()` from the instance dir; the builder copies it under the lock). New body:
```rust
    async fn build_snapshot(&mut self) -> Result<RaftSnapshot, io::Error> {
        // Snapshot the service's real state via the snapshot control ring.
        // Hold the inner lock across the round-trip (2a: blocking — stalls apply;
        // Phase 2b removes this). Single-op: the inner lock serializes vs apply()
        // and vs drive_catchup's INSTALL.
        let mut g = self.inner.lock().await;
        let region_path = g.snapshot_region_path.clone();
        publish_snapshot_cmd(&g.snapshot_producer, MSG_TYPE_BUILD_SNAPSHOT, [0u8; 8], &self.shutdown).await?;
        let built_index =
            await_snapshot_resp(&g.snapshot_resp_consumer, MSG_TYPE_SNAPSHOT_BUILT, &self.shutdown, &g.snapshot_resp_bridge).await?;
        let (_idx, bytes) = snapshot_region::read(&region_path).map_err(|e| io::Error::other(e.to_string()))?;

        // last_log_id for the snapshot meta: the built index (the service's
        // last_applied at build time). Prefer g.last_applied if it matches.
        let last_log_id = g.last_applied.filter(|l| l.index >= built_index).or(g.last_applied);
        let last_membership = g.last_membership.clone();
        let meta = RaftSnapshotMeta {
            last_log_id,
            last_membership,
            snapshot_id: format!("snap-{built_index}"),
        };
        g.current_snapshot = Some(StoredSnapshot { meta: meta.clone(), data: bytes.clone() });
        Ok(Snapshot { meta, snapshot: Cursor::new(bytes) })
    }
```
> **IMPLEMENTER NOTE:** the `last_log_id` for `meta` must be the openraft `LogId` at the built index. The service returns only the *index*; the node maps it to a `LogId` via its own state. Simplest correct choice: use `g.last_applied` (the node's frontier) as `meta.last_log_id` — openraft requires the snapshot's last_log_id to be ≤ the applied index, and the service builds at its last_applied which equals the node's frontier in steady state. If `built_index < g.last_applied.index` (service slightly behind a just-advanced frontier), still use the built index's `LogId`; read it from the journal (`journal.read(built_index)` → term) or accept `g.last_applied` if `built_index == g.last_applied.index`. Keep it simple: assert/expect `built_index == g.last_applied.map(|l| l.index).unwrap_or(0)` (true when build is triggered between applies, which is the openraft snapshot-worker timing) and use `g.last_applied`. Add a `tracing::warn!` + use the journal term lookup if they differ.

**REQUIRED — persist the self-built snapshot to disk (purge-safety).** openraft does
NOT call `install_snapshot` on the node for its OWN snapshots, and the current
`build_snapshot` only stores `g.current_snapshot` **in memory**. But openraft purges
the log (`RaftLogStorage::purge`, durable `last_purged`) right after snapshotting — so
if the snapshot isn't durable, a node restart after purge LOSES it (the log is gone
*and* the snapshot is gone = data loss). Therefore `build_snapshot` MUST persist to
disk exactly like `install_snapshot` does. Add, before returning, a persist block
mirroring `ShmemAdaptedStateMachine::install_snapshot` (investigation showed it:
writes bytes to `g.snapshot_bytes_dir.join(bytes_filename)`, `sync_all`, then
`g.snapshot_meta_sv.store(StoredSnapshotMeta { last_log_id, last_membership,
bytes_filename }).wait()`):
```rust
        let bytes_filename = format!("snapshot_{built_index}.bin");
        let bytes_path = g.snapshot_bytes_dir.join(&bytes_filename);
        std::fs::write(&bytes_path, &bytes).map_err(io::Error::other)?;
        let f = std::fs::File::open(&bytes_path).map_err(io::Error::other)?;
        f.sync_all().map_err(io::Error::other)?;
        drop(f);
        let stored_meta = StoredSnapshotMeta {
            last_log_id,
            last_membership: last_membership.clone(),
            bytes_filename,
        };
        g.snapshot_meta_sv.store(&stored_meta).map_err(io::Error::other)?.wait().map_err(io::Error::other)?;
```
This makes the snapshot recoverable after restart (adapter `new()` already loads
`snapshot_meta_sv` + the bytes file) and is what the Task 7 INSTALL fallback reads
from disk. (Do NOT advance `last_applied_sv` here — the snapshot index ≤ applied; the
existing apply path owns that marker. install_snapshot advances it because it's a
catch-up-from-behind; a self-built snapshot is at the current frontier.)

- [ ] **Step 3: `get_snapshot_builder`** — set the new builder fields:
```rust
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        ShmemSnapshotBuilder {
            inner: self.inner.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
```
(region path comes from `g.snapshot_region_path`, so the builder doesn't need its own copy — drop that field if you read it from `g`.)

- [ ] **Step 4: add `snapshot_region_path` to `ShmemInner` + `new()`** — derive in the builder from the instance dir (`instance_dir/service/snapshot.region`) and pass into `new()`, OR store the instance dir already available. Plumb like the other paths.

- [ ] **Step 5: Build + a focused build-snapshot test** — defer the full assertion to Task 8's integration test, but `cargo build -p uc_node` must be clean and the snapshot ring fields are now used (dead_code gone). Commit:
```bash
git add uc_node/src/raft/state_machine_shmem.rs uc_node/src/runtime/builder.rs
git commit -m "feat(uc_node): build_snapshot drives real service snapshot via snapshot.ring"
```

---

## Task 7: node-side INSTALL — wire `drive_catchup` NeedsSnapshot to install + tail replay

**Files:** Modify `uc_node/src/raft/state_machine_shmem.rs`.

Replace the `NeedsSnapshot` error arm in `drive_catchup` with: read the node's persisted snapshot bytes + index, `snapshot_region::write` them, send `INSTALL_SNAPSHOT`, await `SNAPSHOT_INSTALLED`, then set the replay floor to the snapshot index and replay `(snapshot_index, up_to]` via the existing loop.

- [ ] **Step 1: replace the NeedsSnapshot arm**

Current (investigation): the `match plan_replay(...)` arm `ReplayPlan::NeedsSnapshot { service_last, last_purged } => return Err(...)`. Replace with code that performs the install and computes the post-install replay floor. The node's snapshot is in `g.current_snapshot` (in memory) or on disk via `snapshot_meta_sv` + `snapshot_bytes_dir`:
```rust
            crate::runtime::reconstruct::ReplayPlan::NeedsSnapshot { service_last, last_purged } => {
                // Install the node's snapshot into the service, then replay the tail.
                let (snap_index, snap_bytes) = match &g.current_snapshot {
                    Some(s) => (s.meta.last_log_id.map(|l| l.index).unwrap_or(0), s.data.clone()),
                    None => {
                        // Fall back to the durable snapshot on disk.
                        let meta = g.snapshot_meta_sv.load().ok().flatten();
                        match meta {
                            Some(m) => {
                                let path = g.snapshot_bytes_dir.join(&m.bytes_filename);
                                let bytes = std::fs::read(&path).map_err(|e| io::Error::other(
                                    format!("reconstruct: read snapshot {path:?}: {e}")))?;
                                (m.last_log_id.map(|l| l.index).unwrap_or(0), bytes)
                            }
                            None => return Err(io::Error::other(format!(
                                "reconstruct: service at {service_last} below purge {last_purged} \
                                 but node has no snapshot to install"))),
                        }
                    }
                };
                let region_path = g.snapshot_region_path.clone();
                snapshot_region::write(&region_path, snap_index, &snap_bytes)
                    .map_err(|e| io::Error::other(e.to_string()))?;
                publish_snapshot_cmd(&g.snapshot_producer, MSG_TYPE_INSTALL_SNAPSHOT,
                    uc_protocol::frames::snapshot::encode_extra_install_snapshot(snap_index),
                    shutdown).await?;
                let _installed = await_snapshot_resp(&g.snapshot_resp_consumer,
                    MSG_TYPE_SNAPSHOT_INSTALLED, shutdown, &g.snapshot_resp_bridge).await?;
                // Service is now at snap_index; replay the tail (snap_index, up_to].
                (snap_index, up_to)
            }
```
so the `match` yields `(from, to)` in all arms (`Replay { from, to } => (from, to)` stays). The existing replay loop then runs over `(from+1)..(to+1)` = `(snap_index, up_to]`. Confirm the surrounding code binds `let (from, to) = match ... ;` and that `shutdown` is in scope in `drive_catchup` (it's the `&AtomicBool` param). Add the needed `use` for `snapshot_region` + the frame fns (or fully-qualify as shown).

> **IMPLEMENTER NOTE:** `g` is the `&ShmemInner` borrow inside `drive_catchup` (Phase 1 made `drive_catchup(g: &ShmemInner<S>, ...)`). All accesses above are shared reads of `g` fields + `std::fs::read` + ring ops via the `PlMutex` halves — no `&mut g` needed. Confirm `g.current_snapshot`/`g.snapshot_meta_sv`/`g.snapshot_bytes_dir`/`g.snapshot_region_path`/`g.snapshot_producer`/`g.snapshot_resp_consumer`/`g.snapshot_resp_bridge` are all reachable through the shared borrow. The `saw_up_to` guard + tail-replay loop after the match are unchanged.

- [ ] **Step 2: Build + clippy**

`cargo build -p uc_node` + `cargo clippy -p uc_node --all-targets -- -D warnings` (clean; the `NeedsSnapshot` Phase-2 error message in the spec §7 note is now obsolete for the reachable path — leave the spec note, it documents history). Run `cargo test -p uc_node --test m3_shmem_single_node --test reconstruct_reattach -- --test-threads=1` (Phase 1 paths still green).

- [ ] **Step 3: Commit**
```bash
git add uc_node/src/raft/state_machine_shmem.rs
git commit -m "feat(uc_node): drive_catchup installs node snapshot below purge, then tail-replays"
```

---

## Task 8: integration test — below-purge reattach reconstruction (BUILD+INSTALL end-to-end)

**Files:** Create `uc_node/tests/reconstruct_snapshot.rs`.

Proves the full Phase 2a path: with a small snapshot policy, the cluster snapshots+purges; a service that restarts fresh **below the purge boundary** is reconstructed via snapshot-install + tail-replay. This exercises BUILD (the snapshot backing the purge must be real, else install yields empty state) and INSTALL together.

- [ ] **Step 1: Write the test**

Mirror `uc_node/tests/reconstruct_reattach.rs` (Phase 1) for the bring-up/crash/restart harness + the non-persisting `CounterSm`, with two changes: (a) set a **small snapshot policy** so a snapshot+purge happens after a few commits — set `config.raft.snapshot_policy_logs_since_last` to a small value (e.g. `4`), the field used at `builder.rs:313` (see `m2_multi_node.rs:290` for the override pattern); (b) submit **enough commands to cross the snapshot+purge threshold** before crashing the service.

Sequence:
```text
1. Node + service (CounterSm), snapshot_policy_logs_since_last = 4.
2. Submit 1,2,3,4,5,6 (await each) — crosses the threshold, so the node BUILDs a
   real snapshot (driving the service) and PURGES the log below it. sum = 21.
   (Give a short settle so the snapshot+purge completes — poll/sleep.)
3. Crash ONLY the service (node stays up).
4. Restart service with a FRESH CounterSm (sum=0, last_applied=None → reports 0).
5. Submit 10. The node's apply(10) detects the reattach; plan_replay sees
   service_last=0 < last_purged → INSTALL the node's snapshot (CounterSm sum=21 at
   the snapshot index) into the fresh service, then tail-replay (snap_index, up_to].
6. Assert the submit-10 response == 31 (21 + any post-snapshot entries + 10).
```
> **IMPLEMENTER NOTE on the expected value:** the exact sum depends on which entries are in the snapshot vs the tail. The invariant to assert is **total == sum of ALL applied increments (1..6) + 10 == 31**, regardless of the snapshot/tail split — reconstruction must reproduce the full history. If the fresh service were NOT reconstructed it would return 10; if only tail-replayed without install it would be missing the pre-snapshot increments. Assert `== 31` with a message naming both failure modes. Also assert a follow-up `submit_query(())` == 31.

Provide the full `CounterSm` (copy from `reconstruct_reattach.rs`) and the bring-up (copy from `reconstruct_reattach.rs`, adding the snapshot-policy override on the `NodeConfig`). `#[tokio::test(flavor = "current_thread")]`, `--test-threads=1`.

- [ ] **Step 2: Run**

Run: `cargo test -p uc_node --test reconstruct_snapshot -- --test-threads=1 --nocapture`
Expected: PASS (== 31). If it returns 10 → reconstruction didn't run; if it returns 10+tail-only → INSTALL didn't apply the snapshot (BUILD produced empty bytes — the degenerate path wasn't replaced). These are the signal; debug accordingly (this is the proof test — a failure is important, do not paper over).

- [ ] **Step 3: Commit**
```bash
git add uc_node/tests/reconstruct_snapshot.rs
git commit -m "test(uc_node): below-purge reattach reconstructed via snapshot install + tail replay"
```

---

## Task 9: full verification

- [ ] **Step 1:** `cargo clippy --workspace --all-targets -- -D warnings` → clean.
- [ ] **Step 2:** run the affected suites:
```bash
cargo test -p uc_protocol frames::snapshot snapshot_region ring::spsc
cargo test -p uc_service
cargo test -p uc_node --lib runtime::reconstruct
cargo test -p uc_node --test m3_shmem_single_node
cargo test -p uc_node --test reconstruct_reattach -- --test-threads=1
cargo test -p uc_node --test reconstruct_snapshot -- --test-threads=1
```
Expected: all PASS. (`m3_service_crash` is the known pre-existing flake — retry if it fails; not Phase-2a-related.)
- [ ] **Step 3:** `cargo fmt` then restore any incidentally-formatted files NOT touched by Phase 2a (mirror Phase 1's fmt hygiene), commit:
```bash
git add -A && git commit -m "style: cargo fmt (reconstruction phase 2a files)" || true
```

---

## Self-review notes (against spec §5a)

- **Command channel** (dedicated `snapshot.ring` pair) → Tasks 3 (rings), 4 (node plumbing), 5 (service loop).
- **Frames** → Task 1. **`snapshot.region`** → Task 2 (+ used in 5/6/7).
- **Service BUILD/INSTALL handlers** → Task 5. **Node BUILD drives real snapshot** → Task 6 (safe purge). **Node INSTALL + tail replay** → Task 7.
- **No trait change** — all SM impls keep `build_snapshot(&self,dst)`/`install_snapshot(&mut self,src)`. Confirmed `StoreStateMachine` already implements them over `ultima_db::snapshot_stream`.
- **Purge-safety is REQUIRED, now concrete:** Task 6 persists self-built snapshots to
  disk (mirrors `install_snapshot`) so a node restart after purge can still
  reconstruct — non-negotiable, with exact code in Task 6 Step 2.
- **Risk centers (flagged with IMPLEMENTER NOTEs):** Task 6 — `meta.last_log_id`
  mapping from the built index (use `g.last_applied`; journal-term lookup if they
  differ). Task 7 — borrow soundness of the install arm through the shared
  `&ShmemInner`. Task 8 — the expected-sum invariant (full history reproduced,
  regardless of snapshot/tail split).
- **Accepted limitation:** blocking BUILD stalls applies (node holds `inner` lock + service holds SM read lock across the round-trip) — Phase 2b fixes it. Documented in the plan header + spec §5/§5a.
- **Out of scope confirmed:** trait change (2b), RegisterSm revert + lincheck (Phase 3).
