# M3: Shmem Ring Buffers + Service Process Split — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `uc_protocol`'s ring buffer primitives + `cnc.dat` shared-memory control file + per-RPC frame layouts; refactor `uc_service` and `uc_node` so the user's `StateMachine` runs in a logically separate process from the cluster engine, communicating via shmem. M1/M2's embedded mode stays as a coexisting code path; M3 adds the shmem-fronted mode.

**Architecture:** Three coupled subsystems: (1) `uc_protocol` defines the canonical shmem layouts (mmap'd ring buffers + cnc.dat + frame types), (2) `uc_service` gains a `ServiceBuilder` runtime that attaches to those rings as a consumer/producer + an `ultima_db` adapter module, (3) `uc_node` gains an `ipc` module that owns the instance directory and produces/consumes the same rings on the engine side, and `AdaptedStateMachine` is refactored so apply goes through the apply ring instead of a direct trait call. Tests run service + node as separate `tokio` tasks in the same process — the shmem protocol does the heavy lifting; multi-process subprocess tests are deferred to a follow-up. M1's embedded mode (direct trait call from `AdaptedStateMachine`) coexists, gated by `NodeConfig::ipc_mode`.

**Tech Stack:** Rust 2024, existing M1/M2 deps + `memmap2` for mmap, `parking_lot` for spinlock-fallback Mutex, `fs2` for advisory file locks (already pulls in `nix`/`winapi` transitively which we want), all on stable.

**Reference:** Design spec at `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` (Sections 3-6 + 8 + 10 are the canonical references). M1 record at `docs/tasks/task01_m1_embedded_single_node.md`. M2 record at `docs/tasks/task02_m2_multi_node_quic.md`. The spec's §4 "Discovery directory + shared-memory layout" and §8 "Pipelines" are the architectural ground truth.

**Out of scope for M3 (deferred to M4/M5):**
- `clients/*.ring` (MPSC submit, broadcast response) — M4 ships the `uc_client` real impl.
- `service/output.ring` + `output_progress.state` durable progress marker + retry-while-leader semantics — M5 polish.
- `service/snapshot.region` mmap-backed snapshot bytes (M3 keeps the M2 in-memory `Cursor<Vec<u8>>` path; M5 swaps).
- Multi-process subprocess tests (M3 runs service + node as tokio tasks in the same process; subprocess spawning gated behind `--features multi-process-tests` is a follow-up).
- Prometheus exporter / metrics inventory (M5).
- `cnc.dat` counters/error-log sub-buffers (M5 — used for observability).

---

## File structure

```
ultima_cluster/
├── Cargo.toml                          # add memmap2, fs2, parking_lot workspace deps
├── uc_protocol/
│   ├── Cargo.toml                      # pull the new deps
│   └── src/
│       ├── lib.rs                      # add re-exports for ring, cnc, frames, liveness
│       ├── error_codes.rs              # (existing) M1
│       ├── magic.rs                    # (existing) M1
│       ├── version.rs                  # (existing) M1
│       ├── ring/                       # NEW
│       │   ├── mod.rs                  # public re-exports + RingError
│       │   ├── common.rs               # RingHeader, FrameHeader, slot layout helpers
│       │   ├── spsc.rs                 # Spsc producer + consumer
│       │   ├── mpsc.rs                 # Mpsc producers + single consumer
│       │   └── broadcast.rs            # Broadcast producer + multi-consumer
│       ├── cnc.rs                      # NEW: CncHeader + sub-buffer layout
│       ├── frames/                     # NEW
│       │   ├── mod.rs                  # re-exports
│       │   ├── apply.rs                # ApplyFrame + ApplyRespFrame
│       │   ├── query.rs                # QueryFrame + QueryRespFrame
│       │   └── snapshot.rs             # SnapshotControlFrame
│       ├── liveness.rs                 # NEW: heartbeat helpers
│       └── handshake.rs                # NEW: handshake frame types
├── uc_service/
│   ├── Cargo.toml                      # add uc_protocol path-dep usage; add memmap2 + tokio
│   └── src/
│       ├── lib.rs                      # add re-exports for runtime, ultima_db (feature-gated)
│       ├── error.rs                    # (existing) + add IpcError variant
│       ├── output_handler.rs           # (existing)
│       ├── state_machine.rs            # (existing)
│       ├── ultima_db/                  # NEW (cargo feature "ultima_db", default-on)
│       │   ├── mod.rs                  # re-exports
│       │   ├── store_state_machine.rs  # StoreStateMachine<C,R,Q,QR>
│       │   └── builder.rs              # StoreStateMachineBuilder fluent API
│       └── runtime/                    # NEW
│           ├── mod.rs                  # re-exports
│           ├── service.rs              # ServiceBuilder + ServiceConfig + Service handle
│           ├── handshake.rs            # service-side handshake (writes ServiceReady to cnc)
│           ├── attach.rs               # opens cnc.dat + maps rings + validates instance_id
│           ├── apply_loop.rs           # sync apply thread (no tokio)
│           ├── query_loop.rs           # tokio task draining query.ring
│           └── liveness.rs             # service heartbeat producer (tokio task)
└── uc_node/
    ├── Cargo.toml                      # add memmap2, fs2, parking_lot
    └── src/
        ├── lib.rs                      # add `pub mod ipc;`
        ├── config.rs                   # add IpcMode { Embedded | Shmem } enum + field on NodeConfig
        ├── error.rs                    # add ClusterError::Ipc(IpcError) variant
        ├── ipc/                        # NEW
        │   ├── mod.rs                  # re-exports + IpcError
        │   ├── instance.rs             # owns instance.lock + cnc.dat; service handshake wait
        │   ├── service_link.rs         # apply/query ring producers/consumers (node side)
        │   ├── liveness.rs             # node heartbeat producer
        │   └── handshake.rs            # node-side handshake reader
        ├── raft/
        │   └── state_machine.rs        # MODIFY: split into embedded mode (existing) + shmem mode
        └── runtime/
            ├── builder.rs              # MODIFY: dispatch on IpcMode; spawn ipc layer for Shmem
            └── node.rs                 # MODIFY: support both modes; query_snapshot variant per mode
```

Two key M3 design decisions locked here:

1. **`NodeConfig::ipc_mode: IpcMode { Embedded, Shmem }`** — explicit operator choice. `Embedded` keeps M1/M2 behavior verbatim (direct trait call from `AdaptedStateMachine`). `Shmem` activates the new ipc layer.
2. **Service runs as a tokio task in the same process for M3 tests.** The shmem protocol is correct in both single-process and multi-process configurations. Subprocess spawning is a separate follow-up (M3.x) gated on a feature flag.

---

## Task 1: Workspace deps + `uc_protocol::ring::common`

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/Cargo.toml` — add `memmap2`, `fs2`, `parking_lot` workspace deps.
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_protocol/Cargo.toml` — pull workspace deps (note: this adds `std` dependency; the `no_std`-friendly posture relaxes here for the ring buffer primitives — they need atomics + memory mapping which require `std` features. Document this).
- Create: `/Users/peter/Projects/ultima/ultima_cluster/uc_protocol/src/ring/mod.rs`
- Create: `/Users/peter/Projects/ultima/ultima_cluster/uc_protocol/src/ring/common.rs`
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_protocol/src/lib.rs`

- [ ] **Step 1: Add workspace deps**

In root `Cargo.toml` `[workspace.dependencies]`:

```toml
# M3 — shmem IPC
memmap2 = "0.9"
fs2 = "0.4"
parking_lot = "0.12"
```

- [ ] **Step 2: Add to uc_protocol**

In `uc_protocol/Cargo.toml` `[dependencies]`:

```toml
memmap2 = { workspace = true }
parking_lot = { workspace = true }
```

Remove the `#![cfg_attr(not(test), no_std)]` line from `src/lib.rs` — ring buffers need `std::sync::atomic` and `memmap2` which is `std`-bound. Document at the top: "M3 relaxes the no_std posture for ring buffer primitives; pure data types in `version.rs`/`error_codes.rs`/`magic.rs` remain core-only."

- [ ] **Step 3: Create `uc_protocol/src/ring/mod.rs`**

```rust
//! Shared-memory ring buffer primitives.
//!
//! Three kinds:
//!   * `Spsc` — single-producer single-consumer (service↔node).
//!   * `Mpsc` — many-producers single-consumer (clients→node; M4 uses this).
//!   * `Broadcast` — single-producer many-consumers (node→clients; M4 uses this).

pub mod common;
pub mod spsc;
pub mod mpsc;
pub mod broadcast;

pub use common::{FrameHeader, RingError, RingHeader};
pub use spsc::{SpscProducer, SpscConsumer};
pub use mpsc::{MpscProducer, MpscConsumer};
pub use broadcast::{BroadcastProducer, BroadcastConsumer};

use thiserror::Error;
```

Wait — `thiserror` isn't in `uc_protocol`'s deps currently. Add it:

```toml
thiserror = { workspace = true }
```

- [ ] **Step 4: Create `uc_protocol/src/ring/common.rs`**

This file holds the on-disk layout (header + frame metadata) and shared atomic helpers. The header has cache-padded `producer_position` / `consumer_position` for cache-line ping-pong avoidance. Slot framing matches the spec §4 record layout: length-inclusive header + per-record CRC32.

```rust
//! Shared ring buffer types: header layout, frame layout, error types.

use std::sync::atomic::AtomicU64;
use thiserror::Error;

/// Fixed-size header at the start of every ring file. 128 bytes.
/// On-disk layout (little-endian):
/// ```text
/// magic              [u8; 8]      b"ULTRNG\0\0"
/// capacity_bytes     u64          slot region capacity, power of two
/// max_msg_size       u32
/// msg_kind_filter    u32          allow-list bitmask
/// producer_position  AtomicU64    cache-padded (64-byte alignment)
/// (padding to 64-byte boundary)
/// consumer_position  AtomicU64    cache-padded
/// (padding to 128 bytes total)
/// ```
#[repr(C, align(64))]
pub struct RingHeader {
    pub magic: [u8; 8],
    pub capacity_bytes: u64,
    pub max_msg_size: u32,
    pub msg_kind_filter: u32,
    pub _pad_1: [u8; 40],
    pub producer_position: AtomicU64,
    pub _pad_2: [u8; 56],
    pub consumer_position: AtomicU64,
    pub _pad_3: [u8; 56],
}

const _: () = {
    // Compile-time assertion that the layout is exactly 128 bytes (header) +
    // cache-line padded so the two atomics don't share a cache line.
    assert!(std::mem::size_of::<RingHeader>() == 192);
    assert!(std::mem::align_of::<RingHeader>() == 64);
};

pub const RING_HEADER_LEN: usize = std::mem::size_of::<RingHeader>();

/// Per-record framing inside a ring slot.
/// Wire layout, little-endian:
/// ```text
/// length_inclusive_header  u32      total record size (header+payload+trailer)
/// msg_type                 u16
/// flags                    u16
/// header_extra             [u8; 8]  per-msg-type metadata (e.g. log_index for ApplyFrame)
/// payload                  variable
/// crc32                    u32      CRC over (msg_type..end-of-payload)
/// ```
///
/// The length field is the **atomic commit point** — written last by the
/// producer using a `Release` store. The consumer reads it with `Acquire`
/// and treats zero as "not yet committed."
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct FrameHeader {
    pub length_inclusive_header: u32,
    pub msg_type: u16,
    pub flags: u16,
    pub header_extra: [u8; 8],
}

pub const FRAME_HEADER_LEN: usize = std::mem::size_of::<FrameHeader>();
pub const FRAME_TRAILER_LEN: usize = 4;  // CRC32

#[derive(Debug, Error)]
pub enum RingError {
    #[error("ring full")]
    Full,
    #[error("ring empty")]
    Empty,
    #[error("frame too large: {len} > {max}")]
    TooLarge { len: usize, max: usize },
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt: {0}")]
    Corrupt(String),
    #[error("magic mismatch")]
    BadMagic,
    #[error("crc mismatch")]
    BadCrc,
}

/// Initialize a freshly-mmap'd ring file's header.
/// Safe iff the caller holds exclusive access to the byte range.
pub fn init_ring_header(buf: &mut [u8], capacity_bytes: u64, max_msg_size: u32, msg_kind_filter: u32)
    -> Result<(), RingError>
{
    if buf.len() < RING_HEADER_LEN {
        return Err(RingError::Corrupt(format!(
            "buffer too small for ring header: {} < {RING_HEADER_LEN}", buf.len())));
    }
    // Use raw pointer init to avoid construction issues with the AtomicU64 fields.
    // SAFETY: buf is at least RING_HEADER_LEN bytes and properly aligned (mmap pages are
    // page-aligned which exceeds the 64-byte alignment we need).
    let header_ptr = buf.as_mut_ptr() as *mut RingHeader;
    unsafe {
        std::ptr::write(header_ptr, RingHeader {
            magic: crate::magic::RING_MAGIC,
            capacity_bytes,
            max_msg_size,
            msg_kind_filter,
            _pad_1: [0; 40],
            producer_position: AtomicU64::new(0),
            _pad_2: [0; 56],
            consumer_position: AtomicU64::new(0),
            _pad_3: [0; 56],
        });
    }
    Ok(())
}

/// Validate an existing ring file's header (e.g., on attach).
pub fn validate_ring_header(buf: &[u8]) -> Result<&RingHeader, RingError> {
    if buf.len() < RING_HEADER_LEN {
        return Err(RingError::Corrupt(format!(
            "buffer too small: {} < {RING_HEADER_LEN}", buf.len())));
    }
    let header_ptr = buf.as_ptr() as *const RingHeader;
    // SAFETY: buf is at least RING_HEADER_LEN bytes and the mmap is properly aligned.
    let header = unsafe { &*header_ptr };
    if header.magic != crate::magic::RING_MAGIC {
        return Err(RingError::BadMagic);
    }
    Ok(header)
}
```

- [ ] **Step 5: Update `uc_protocol/src/lib.rs`**

Add `pub mod ring;` alongside the existing modules. Also add `pub mod cnc;`, `pub mod frames;`, `pub mod liveness;`, `pub mod handshake;` — those files come in Tasks 5-9 but declare them now (with empty stub files for the others).

For now, create the stub files:

```rust
// uc_protocol/src/cnc.rs
//! Filled in by Task 5.

// uc_protocol/src/frames/mod.rs
//! Filled in by Task 6.
pub mod apply;
pub mod query;
pub mod snapshot;

// uc_protocol/src/frames/apply.rs        — stub for Task 6
// uc_protocol/src/frames/query.rs        — stub for Task 6
// uc_protocol/src/frames/snapshot.rs     — stub for Task 6

// uc_protocol/src/liveness.rs            — stub for Task 4
// uc_protocol/src/handshake.rs           — stub for Task 4
```

- [ ] **Step 6: Build and clippy**

```bash
cargo build -p uc_protocol
cargo clippy -p uc_protocol --all-targets -- -D warnings
```

Expected: clean. Some warnings about unused modules in the stubs are OK.

- [ ] **Step 7: Add a header-init/validate test**

In `uc_protocol/src/ring/common.rs`, append:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_then_validate_round_trip() {
        let mut buf = vec![0u8; RING_HEADER_LEN * 2];
        init_ring_header(&mut buf, 65536, 4096, 0xff).expect("init");
        let header = validate_ring_header(&buf).expect("validate");
        assert_eq!(header.magic, crate::magic::RING_MAGIC);
        assert_eq!(header.capacity_bytes, 65536);
        assert_eq!(header.max_msg_size, 4096);
        assert_eq!(header.msg_kind_filter, 0xff);
        assert_eq!(header.producer_position.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn validate_rejects_bad_magic() {
        let buf = vec![0u8; RING_HEADER_LEN];
        let result = validate_ring_header(&buf);
        assert!(matches!(result, Err(RingError::BadMagic)));
    }
}
```

Run: `cargo test -p uc_protocol ring::common::tests`
Expected: 2 passed.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml uc_protocol/
git commit -m "feat(uc_protocol): ring buffer common types (RingHeader, FrameHeader)"
```

---

## Task 2: `uc_protocol::ring::spsc`

Single-producer, single-consumer lock-free ring. Used for service↔node rings (apply, query, output). Producer claims via `fetch_add` on `producer_position`; consumer reads with relaxed loads + `Acquire` on the frame length field.

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_protocol/src/ring/spsc.rs`

- [ ] **Step 1: Implement Spsc producer/consumer**

The plan's full SPSC implementation is ~150 lines. Key invariants:
- `producer_position` advances monotonically; only the producer mutates it.
- `consumer_position` advances monotonically; only the consumer mutates it.
- Slot indexing: `position % capacity_bytes` gives the byte offset within the slot region.
- Empty: `producer_position == consumer_position`.
- Full: `producer_position - consumer_position == capacity_bytes`.
- Records can wrap the buffer; if a record doesn't fit at the tail, a "padding marker" is written (`length_inclusive_header > 0` with `msg_type = 0xffff` indicating skip-to-buffer-start) and the producer claims a fresh slot at offset 0.

The producer's `try_write(header_extra, msg_type, payload)`:
1. Compute total record size = `FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN`.
2. Reject if > `max_msg_size`.
3. Load `consumer_position` (Acquire).
4. Load `producer_position` (Relaxed — we're the only writer).
5. Check free space: `capacity - (producer - consumer) >= record_size`. If not, return `RingError::Full`.
6. If the record wraps the tail, write a padding marker and advance `producer_position` to wrap; recompute.
7. Write the slot bytes: msg_type, flags, header_extra, payload, crc32. **Write `length_inclusive_header` last** with `Release` ordering — this is the commit point.
8. Advance `producer_position` (Release).

The consumer's `try_read(out_buf)`:
1. Load `producer_position` (Acquire).
2. Load `consumer_position` (Relaxed — we're the only reader).
3. If equal → `RingError::Empty`.
4. Read the frame header at offset `consumer_position % capacity` (Acquire on the length field).
5. If length is zero, the record isn't committed yet → return `Empty`.
6. If msg_type indicates padding, advance `consumer_position` past the padding and retry.
7. Validate CRC; copy payload into `out_buf` (or pass back as a slice).
8. Advance `consumer_position` (Release).

Concrete skeleton:

```rust
//! Single-producer single-consumer ring buffer.
//!
//! Both producer and consumer hold an `Arc<Inner>` so they can be sent to
//! different threads/tasks. `Inner` owns the mmap and exposes the header
//! via stable pointers.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use crate::ring::common::{
    FrameHeader, RingError, RingHeader,
    FRAME_HEADER_LEN, FRAME_TRAILER_LEN, RING_HEADER_LEN,
    validate_ring_header,
};

/// Padding marker `msg_type` — consumer skips to start of slot region.
const PADDING_MSG_TYPE: u16 = 0xffff;

pub struct SpscInner {
    /// Raw pointer to the mmap'd ring file. Lives as long as the Arc.
    /// The owner (`SpscRing::open`) is responsible for keeping the mmap alive.
    base: *mut u8,
    /// Total file size in bytes (header + slot region).
    file_len: usize,
}

// SAFETY: base is mmap'd memory shared between producer and consumer.
// Synchronization is via the atomics in RingHeader.
unsafe impl Send for SpscInner {}
unsafe impl Sync for SpscInner {}

impl SpscInner {
    fn header(&self) -> &RingHeader {
        // SAFETY: base points to a valid RingHeader (validated at attach time).
        unsafe { &*(self.base as *const RingHeader) }
    }

    fn slot_region(&self) -> *mut u8 {
        // SAFETY: file_len > RING_HEADER_LEN (validated at attach).
        unsafe { self.base.add(RING_HEADER_LEN) }
    }

    fn capacity(&self) -> usize {
        self.header().capacity_bytes as usize
    }

    fn max_msg_size(&self) -> usize {
        self.header().max_msg_size as usize
    }
}

pub struct SpscProducer { inner: Arc<SpscInner> }
pub struct SpscConsumer { inner: Arc<SpscInner> }

impl SpscProducer {
    /// Try to write a record. Returns Ok on success, Full / TooLarge on failure.
    pub fn try_write(
        &mut self,
        msg_type: u16,
        flags: u16,
        header_extra: [u8; 8],
        payload: &[u8],
    ) -> Result<(), RingError> {
        let total_record_size = FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN;
        if total_record_size > self.inner.max_msg_size() {
            return Err(RingError::TooLarge {
                len: total_record_size,
                max: self.inner.max_msg_size(),
            });
        }

        let header = self.inner.header();
        let capacity = self.inner.capacity();
        let consumer_pos = header.consumer_position.load(Ordering::Acquire);
        let producer_pos = header.producer_position.load(Ordering::Relaxed);

        let free = capacity.saturating_sub((producer_pos - consumer_pos) as usize);
        if free < total_record_size {
            return Err(RingError::Full);
        }

        // Detect tail-wrap. If the record would straddle the end of the slot
        // region, emit a padding marker and start over at offset 0.
        let slot_offset = (producer_pos as usize) % capacity;
        let bytes_to_tail = capacity - slot_offset;
        if bytes_to_tail < total_record_size {
            // Need at minimum FRAME_HEADER_LEN bytes for the padding marker. If
            // we don't even have that, the free-space check would have failed
            // above. Otherwise write the padding marker.
            self.write_record_at(slot_offset, PADDING_MSG_TYPE, 0, [0; 8], &[])?;
            // Commit the padding record and advance.
            header.producer_position.store(producer_pos + bytes_to_tail as u64, Ordering::Release);
            return self.try_write(msg_type, flags, header_extra, payload);
        }

        self.write_record_at(slot_offset, msg_type, flags, header_extra, payload)?;
        header.producer_position.store(producer_pos + total_record_size as u64, Ordering::Release);
        Ok(())
    }

    fn write_record_at(
        &mut self,
        slot_offset: usize,
        msg_type: u16,
        flags: u16,
        header_extra: [u8; 8],
        payload: &[u8],
    ) -> Result<(), RingError> {
        // SAFETY: free-space + tail-wrap checks ensure we have enough room.
        unsafe {
            let dst = self.inner.slot_region().add(slot_offset);
            // Write FrameHeader fields EXCEPT length first.
            std::ptr::copy_nonoverlapping(&msg_type as *const _ as *const u8,
                dst.add(4), 2);
            std::ptr::copy_nonoverlapping(&flags as *const _ as *const u8,
                dst.add(6), 2);
            std::ptr::copy_nonoverlapping(header_extra.as_ptr(), dst.add(8), 8);
            std::ptr::copy_nonoverlapping(payload.as_ptr(), dst.add(FRAME_HEADER_LEN),
                payload.len());

            // Compute CRC32 over (msg_type..end-of-payload).
            let crc_input = std::slice::from_raw_parts(
                dst.add(4),
                FRAME_HEADER_LEN - 4 + payload.len());
            let crc = crc32fast::hash(crc_input);
            std::ptr::copy_nonoverlapping(&crc.to_le_bytes() as *const _ as *const u8,
                dst.add(FRAME_HEADER_LEN + payload.len()), 4);

            // Commit: write length last with Release ordering.
            let total = (FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN) as u32;
            std::sync::atomic::fence(Ordering::Release);
            std::ptr::copy_nonoverlapping(&total.to_le_bytes() as *const _ as *const u8,
                dst, 4);
        }
        Ok(())
    }
}

impl SpscConsumer {
    /// Try to read a record. Returns Ok(Some) with metadata + payload, Ok(None) if empty.
    pub fn try_read(&mut self, payload_buf: &mut Vec<u8>) -> Result<Option<RecordHeader>, RingError> {
        let header = self.inner.header();
        let capacity = self.inner.capacity();
        let producer_pos = header.producer_position.load(Ordering::Acquire);
        let consumer_pos = header.consumer_position.load(Ordering::Relaxed);
        if producer_pos == consumer_pos {
            return Ok(None);
        }

        let slot_offset = (consumer_pos as usize) % capacity;
        // SAFETY: producer_pos > consumer_pos and capacity is correct, so this offset is in-range.
        let dst = unsafe { self.inner.slot_region().add(slot_offset) };

        // Read length with Acquire — pairs with producer's Release.
        let length_bytes = unsafe { std::slice::from_raw_parts(dst, 4) };
        let length = u32::from_le_bytes(length_bytes.try_into().unwrap());
        if length == 0 {
            return Ok(None);    // not yet committed
        }
        std::sync::atomic::fence(Ordering::Acquire);

        let msg_type_bytes = unsafe { std::slice::from_raw_parts(dst.add(4), 2) };
        let msg_type = u16::from_le_bytes(msg_type_bytes.try_into().unwrap());
        let flags_bytes = unsafe { std::slice::from_raw_parts(dst.add(6), 2) };
        let flags = u16::from_le_bytes(flags_bytes.try_into().unwrap());
        let header_extra: [u8; 8] = unsafe { std::slice::from_raw_parts(dst.add(8), 8) }
            .try_into().unwrap();

        if msg_type == PADDING_MSG_TYPE {
            // Skip the padding record (length covers from tail to buffer end).
            header.consumer_position.store(consumer_pos + length as u64, Ordering::Release);
            return self.try_read(payload_buf);
        }

        let payload_len = (length as usize) - FRAME_HEADER_LEN - FRAME_TRAILER_LEN;
        let payload_src = unsafe { std::slice::from_raw_parts(
            dst.add(FRAME_HEADER_LEN), payload_len) };

        // Validate CRC.
        let crc_actual_bytes = unsafe { std::slice::from_raw_parts(
            dst.add(FRAME_HEADER_LEN + payload_len), 4) };
        let crc_actual = u32::from_le_bytes(crc_actual_bytes.try_into().unwrap());
        let crc_input = unsafe { std::slice::from_raw_parts(
            dst.add(4), FRAME_HEADER_LEN - 4 + payload_len) };
        let crc_expected = crc32fast::hash(crc_input);
        if crc_actual != crc_expected {
            return Err(RingError::BadCrc);
        }

        payload_buf.clear();
        payload_buf.extend_from_slice(payload_src);

        header.consumer_position.store(consumer_pos + length as u64, Ordering::Release);

        Ok(Some(RecordHeader { msg_type, flags, header_extra }))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RecordHeader {
    pub msg_type: u16,
    pub flags: u16,
    pub header_extra: [u8; 8],
}

/// Open or create a ring file as an SPSC pair.
/// Caller owns the mmap; the producer and consumer hold Arc clones of Inner
/// that borrow the mmap's pointer. Caller MUST keep the mmap alive for the
/// lifetime of producer/consumer.
pub struct SpscRing {
    _mmap: memmap2::MmapMut,
    inner: Arc<SpscInner>,
}

impl SpscRing {
    pub fn create(path: &std::path::Path, capacity_bytes: u64, max_msg_size: u32)
        -> Result<Self, RingError>
    {
        let file_len = RING_HEADER_LEN + capacity_bytes as usize;
        let file = std::fs::OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path)?;
        file.set_len(file_len as u64)?;
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
        crate::ring::common::init_ring_header(&mut mmap[..], capacity_bytes, max_msg_size, 0)?;

        let inner = Arc::new(SpscInner {
            base: mmap.as_mut_ptr(),
            file_len,
        });

        Ok(SpscRing { _mmap: mmap, inner })
    }

    pub fn open(path: &std::path::Path) -> Result<Self, RingError> {
        let file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
        let _header = validate_ring_header(&mmap[..])?;
        let file_len = mmap.len();
        let inner = Arc::new(SpscInner {
            base: mmap.as_mut_ptr(),
            file_len,
        });
        Ok(SpscRing { _mmap: mmap, inner })
    }

    pub fn split(self) -> (SpscProducer, SpscConsumer, memmap2::MmapMut) {
        let producer = SpscProducer { inner: self.inner.clone() };
        let consumer = SpscConsumer { inner: self.inner };
        (producer, consumer, self._mmap)
    }
}
```

(The producer and consumer hold raw pointers via `Arc<SpscInner>`. The owner of the mmap must outlive them. This is brittle by-construction; a v2 could use `Pin<Box<_>>` instead, but the current shape is the standard idiom for shared-memory rings.)

Add `crc32fast = "1"` to `uc_protocol/Cargo.toml` (currently only `uc_node` has it; share it).

- [ ] **Step 2: Build**

```bash
cargo build -p uc_protocol
cargo clippy -p uc_protocol --all-targets -- -D warnings
```

Expected: clean. The `unsafe` blocks may surface clippy lints; address by adding `// SAFETY:` comments where they don't already exist.

- [ ] **Step 3: Add SPSC tests**

In `uc_protocol/src/ring/spsc.rs`, append:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn write_then_read_single_record() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (mut producer, mut consumer, _keepalive) = ring.split();

        producer.try_write(7, 0, [1, 2, 3, 4, 0, 0, 0, 0], b"hello").expect("write");

        let mut buf = Vec::new();
        let rec = consumer.try_read(&mut buf).expect("read").expect("non-empty");
        assert_eq!(rec.msg_type, 7);
        assert_eq!(rec.header_extra, [1, 2, 3, 4, 0, 0, 0, 0]);
        assert_eq!(&buf[..], b"hello");
    }

    #[test]
    fn read_empty_returns_none() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (_producer, mut consumer, _keepalive) = ring.split();

        let mut buf = Vec::new();
        let rec = consumer.try_read(&mut buf).expect("read");
        assert!(rec.is_none());
    }

    #[test]
    fn full_ring_returns_err() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 256, 128).expect("create");
        let (mut producer, _consumer, _keepalive) = ring.split();

        // Write until full.
        let payload = vec![0u8; 64];
        let mut writes = 0;
        loop {
            match producer.try_write(1, 0, [0; 8], &payload) {
                Ok(()) => writes += 1,
                Err(RingError::Full) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
            if writes > 100 { panic!("ring never filled"); }
        }
        assert!(writes >= 1, "should have written at least once");
    }

    #[test]
    fn wrap_around_via_padding() {
        let tmp = NamedTempFile::new().unwrap();
        // Tight ring: capacity = 256, max_msg = 128.
        let ring = SpscRing::create(tmp.path(), 256, 128).expect("create");
        let (mut producer, mut consumer, _keepalive) = ring.split();
        let payload = vec![0u8; 64];

        // Write + read 5 times — should exercise wrap-around at least once.
        for i in 0..5 {
            producer.try_write(1, 0, [i as u8; 8], &payload).expect("write");
            let mut buf = Vec::new();
            let rec = consumer.try_read(&mut buf).expect("read").expect("non-empty");
            assert_eq!(rec.header_extra[0], i as u8);
        }
    }
}
```

Run: `cargo test -p uc_protocol ring::spsc::tests`
Expected: 4 passed.

- [ ] **Step 4: Commit**

```bash
git add uc_protocol/Cargo.toml uc_protocol/src/ring/spsc.rs
git commit -m "feat(uc_protocol): SPSC ring buffer (single-producer single-consumer)"
```

---

## Task 3: `uc_protocol::ring::mpsc`

Many-producers single-consumer. Producers claim slots via CAS on `producer_position`. Consumer uses relaxed loads (single reader). Used for client→node submit/query rings in M4; tested here for completeness.

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_protocol/src/ring/mpsc.rs`

The MPSC differs from SPSC in producer flow:
- Producer's `try_write` does a CAS on `producer_position` to claim `total_record_size` bytes atomically. On contention, retry.
- The CAS gives the producer exclusive ownership of `[claimed_pos, claimed_pos + size)` in the slot region.
- Tail-wrap is handled the same way — if the claimed range would straddle the buffer end, the producer instead claims `bytes_to_tail` for a padding marker and retries.

- [ ] **Step 1: Implement MPSC**

```rust
//! Many-producer single-consumer ring buffer.
//!
//! Producers claim slots via `compare_exchange_weak` on `producer_position`.
//! Consumer reads with relaxed loads (single reader; no contention on the
//! consumer side).

use std::sync::atomic::Ordering;
use std::sync::Arc;
use crate::ring::common::{
    FrameHeader, RingError, RingHeader,
    FRAME_HEADER_LEN, FRAME_TRAILER_LEN, RING_HEADER_LEN,
    validate_ring_header,
};
use crate::ring::spsc::{RecordHeader};

const PADDING_MSG_TYPE: u16 = 0xffff;

pub struct MpscInner {
    base: *mut u8,
    file_len: usize,
}

unsafe impl Send for MpscInner {}
unsafe impl Sync for MpscInner {}

impl MpscInner {
    fn header(&self) -> &RingHeader {
        unsafe { &*(self.base as *const RingHeader) }
    }
    fn slot_region(&self) -> *mut u8 {
        unsafe { self.base.add(RING_HEADER_LEN) }
    }
    fn capacity(&self) -> usize { self.header().capacity_bytes as usize }
    fn max_msg_size(&self) -> usize { self.header().max_msg_size as usize }
}

#[derive(Clone)]
pub struct MpscProducer { inner: Arc<MpscInner> }
pub struct MpscConsumer { inner: Arc<MpscInner> }

unsafe impl Send for MpscProducer {}
unsafe impl Sync for MpscProducer {}

impl MpscProducer {
    pub fn try_write(
        &self,
        msg_type: u16,
        flags: u16,
        header_extra: [u8; 8],
        payload: &[u8],
    ) -> Result<(), RingError> {
        let total = FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN;
        if total > self.inner.max_msg_size() {
            return Err(RingError::TooLarge { len: total, max: self.inner.max_msg_size() });
        }

        let header = self.inner.header();
        let capacity = self.inner.capacity();

        loop {
            let consumer_pos = header.consumer_position.load(Ordering::Acquire);
            let producer_pos = header.producer_position.load(Ordering::Acquire);

            let free = capacity.saturating_sub((producer_pos - consumer_pos) as usize);
            if free < total {
                return Err(RingError::Full);
            }

            let slot_offset = (producer_pos as usize) % capacity;
            let bytes_to_tail = capacity - slot_offset;

            // If we'd straddle the tail, claim the tail bytes for a padding marker.
            let claim_size = if bytes_to_tail < total { bytes_to_tail } else { total };

            let target_pos = producer_pos + claim_size as u64;
            if header.producer_position.compare_exchange_weak(
                producer_pos, target_pos,
                Ordering::AcqRel, Ordering::Relaxed,
            ).is_err() {
                continue;     // raced; retry
            }

            // We own [slot_offset, slot_offset + claim_size).
            if claim_size != total {
                // Wrote a padding marker only; recurse to claim the real record.
                Self::write_record(self.inner.slot_region(), slot_offset,
                    PADDING_MSG_TYPE, 0, [0; 8], &[], claim_size);
                continue;
            }

            Self::write_record(self.inner.slot_region(), slot_offset,
                msg_type, flags, header_extra, payload, total);
            return Ok(());
        }
    }

    fn write_record(
        slot_region: *mut u8,
        slot_offset: usize,
        msg_type: u16,
        flags: u16,
        header_extra: [u8; 8],
        payload: &[u8],
        total_record_size: usize,
    ) {
        unsafe {
            let dst = slot_region.add(slot_offset);
            std::ptr::copy_nonoverlapping(&msg_type as *const _ as *const u8, dst.add(4), 2);
            std::ptr::copy_nonoverlapping(&flags as *const _ as *const u8, dst.add(6), 2);
            std::ptr::copy_nonoverlapping(header_extra.as_ptr(), dst.add(8), 8);
            std::ptr::copy_nonoverlapping(payload.as_ptr(), dst.add(FRAME_HEADER_LEN),
                payload.len());

            let crc_input = std::slice::from_raw_parts(
                dst.add(4), FRAME_HEADER_LEN - 4 + payload.len());
            let crc = crc32fast::hash(crc_input);
            std::ptr::copy_nonoverlapping(&crc.to_le_bytes() as *const _ as *const u8,
                dst.add(FRAME_HEADER_LEN + payload.len()), 4);

            let total = total_record_size as u32;
            std::sync::atomic::fence(Ordering::Release);
            std::ptr::copy_nonoverlapping(&total.to_le_bytes() as *const _ as *const u8,
                dst, 4);
        }
    }
}

impl MpscConsumer {
    pub fn try_read(&mut self, payload_buf: &mut Vec<u8>) -> Result<Option<RecordHeader>, RingError> {
        // Implementation mirrors SpscConsumer::try_read — single reader, so
        // relaxed loads on consumer_position are safe.
        // (Copy the body from spsc.rs and adapt the types — pure transcription.)
        let header = self.inner.header();
        let capacity = self.inner.capacity();
        let producer_pos = header.producer_position.load(Ordering::Acquire);
        let consumer_pos = header.consumer_position.load(Ordering::Relaxed);
        if producer_pos == consumer_pos { return Ok(None); }

        let slot_offset = (consumer_pos as usize) % capacity;
        let dst = unsafe { self.inner.slot_region().add(slot_offset) };
        let length = u32::from_le_bytes(
            unsafe { std::slice::from_raw_parts(dst, 4) }.try_into().unwrap());
        if length == 0 { return Ok(None); }
        std::sync::atomic::fence(Ordering::Acquire);

        let msg_type = u16::from_le_bytes(
            unsafe { std::slice::from_raw_parts(dst.add(4), 2) }.try_into().unwrap());
        let flags = u16::from_le_bytes(
            unsafe { std::slice::from_raw_parts(dst.add(6), 2) }.try_into().unwrap());
        let header_extra: [u8; 8] = unsafe { std::slice::from_raw_parts(dst.add(8), 8) }
            .try_into().unwrap();

        if msg_type == PADDING_MSG_TYPE {
            header.consumer_position.store(consumer_pos + length as u64, Ordering::Release);
            return self.try_read(payload_buf);
        }

        let payload_len = (length as usize) - FRAME_HEADER_LEN - FRAME_TRAILER_LEN;
        let payload_src = unsafe { std::slice::from_raw_parts(
            dst.add(FRAME_HEADER_LEN), payload_len) };

        let crc_actual = u32::from_le_bytes(
            unsafe { std::slice::from_raw_parts(dst.add(FRAME_HEADER_LEN + payload_len), 4) }
                .try_into().unwrap());
        let crc_input = unsafe { std::slice::from_raw_parts(
            dst.add(4), FRAME_HEADER_LEN - 4 + payload_len) };
        if crc_actual != crc32fast::hash(crc_input) {
            return Err(RingError::BadCrc);
        }

        payload_buf.clear();
        payload_buf.extend_from_slice(payload_src);
        header.consumer_position.store(consumer_pos + length as u64, Ordering::Release);
        Ok(Some(RecordHeader { msg_type, flags, header_extra }))
    }
}

pub struct MpscRing {
    _mmap: memmap2::MmapMut,
    inner: Arc<MpscInner>,
}

impl MpscRing {
    pub fn create(path: &std::path::Path, capacity_bytes: u64, max_msg_size: u32)
        -> Result<Self, RingError>
    {
        let file_len = RING_HEADER_LEN + capacity_bytes as usize;
        let file = std::fs::OpenOptions::new().read(true).write(true)
            .create(true).truncate(true).open(path)?;
        file.set_len(file_len as u64)?;
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
        crate::ring::common::init_ring_header(&mut mmap[..], capacity_bytes, max_msg_size, 0)?;
        let inner = Arc::new(MpscInner { base: mmap.as_mut_ptr(), file_len });
        Ok(MpscRing { _mmap: mmap, inner })
    }

    pub fn open(path: &std::path::Path) -> Result<Self, RingError> {
        let file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
        validate_ring_header(&mmap[..])?;
        let file_len = mmap.len();
        let inner = Arc::new(MpscInner { base: mmap.as_mut_ptr(), file_len });
        Ok(MpscRing { _mmap: mmap, inner })
    }

    pub fn split(self) -> (MpscProducer, MpscConsumer, memmap2::MmapMut) {
        let producer = MpscProducer { inner: self.inner.clone() };
        let consumer = MpscConsumer { inner: self.inner };
        (producer, consumer, self._mmap)
    }
}
```

- [ ] **Step 2: Concurrent producer test**

Append to `mpsc.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::thread;
    use std::sync::Arc as StdArc;

    #[test]
    fn many_producers_one_consumer() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 65536, 1024).expect("create");
        let (producer, mut consumer, _keepalive) = ring.split();

        const N_THREADS: usize = 8;
        const PER_THREAD: usize = 50;

        let handles: Vec<_> = (0..N_THREADS).map(|t| {
            let p = producer.clone();
            thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let payload = format!("t{t}-i{i}").into_bytes();
                    loop {
                        match p.try_write(1, 0, [0; 8], &payload) {
                            Ok(()) => break,
                            Err(RingError::Full) => thread::yield_now(),
                            Err(e) => panic!("{e}"),
                        }
                    }
                }
            })
        }).collect();

        let mut received = 0;
        let mut payloads: std::collections::HashSet<Vec<u8>> = Default::default();
        while received < N_THREADS * PER_THREAD {
            let mut buf = Vec::new();
            if let Some(_rec) = consumer.try_read(&mut buf).expect("read") {
                payloads.insert(buf);
                received += 1;
            }
        }

        for h in handles { h.join().unwrap(); }
        assert_eq!(payloads.len(), N_THREADS * PER_THREAD);
    }
}
```

Run: `cargo test -p uc_protocol ring::mpsc::tests`
Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add uc_protocol/src/ring/mpsc.rs
git commit -m "feat(uc_protocol): MPSC ring buffer (CAS-based claim)"
```

---

## Task 4: `uc_protocol::ring::broadcast`

Single-producer many-consumers. No backpressure: producer never blocks; slow consumers may lag and report `Overwritten`. Each consumer holds its own `head` position. Tested with one producer + two consumers tracking independent positions.

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_protocol/src/ring/broadcast.rs`

- [ ] **Step 1: Implement Broadcast**

Implementation differs from SPSC/MPSC:
- The `RingHeader::consumer_position` is unused (broadcast has many consumers, each with their own position).
- Each `BroadcastConsumer` holds an in-memory `head: u64`.
- Producer's `write` (NOT `try_write` — no backpressure) advances `producer_position` unconditionally. If the buffer wraps, old records get overwritten.
- Consumer's `try_read` checks if its `head` is too far behind (>= 1 buffer-capacity behind producer) → `RingError::Overwritten`. Otherwise advance.

Skeleton (~120 lines). Add `RingError::Overwritten` variant in `ring/common.rs`:

```rust
#[error("consumer fell behind; producer overwrote unread records")]
Overwritten,
```

Then in `broadcast.rs`:

```rust
//! Single-producer many-consumer broadcast ring buffer.
//!
//! Producer never blocks; slow consumers detect overwrite via the
//! `producer_position - head >= capacity_bytes` check.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use crate::ring::common::{
    FrameHeader, RingError, RingHeader,
    FRAME_HEADER_LEN, FRAME_TRAILER_LEN, RING_HEADER_LEN,
    validate_ring_header,
};
use crate::ring::spsc::RecordHeader;

const PADDING_MSG_TYPE: u16 = 0xffff;

pub struct BroadcastInner {
    base: *mut u8,
    file_len: usize,
}

unsafe impl Send for BroadcastInner {}
unsafe impl Sync for BroadcastInner {}

impl BroadcastInner {
    fn header(&self) -> &RingHeader {
        unsafe { &*(self.base as *const RingHeader) }
    }
    fn slot_region(&self) -> *mut u8 {
        unsafe { self.base.add(RING_HEADER_LEN) }
    }
    fn capacity(&self) -> usize { self.header().capacity_bytes as usize }
    fn max_msg_size(&self) -> usize { self.header().max_msg_size as usize }
}

pub struct BroadcastProducer { inner: Arc<BroadcastInner> }

impl BroadcastProducer {
    /// Write a record. Never blocks; slow consumers may miss records.
    pub fn write(
        &mut self,
        msg_type: u16,
        flags: u16,
        header_extra: [u8; 8],
        payload: &[u8],
    ) -> Result<(), RingError> {
        let total = FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN;
        if total > self.inner.max_msg_size() {
            return Err(RingError::TooLarge { len: total, max: self.inner.max_msg_size() });
        }

        let header = self.inner.header();
        let capacity = self.inner.capacity();
        let producer_pos = header.producer_position.load(Ordering::Relaxed);
        let slot_offset = (producer_pos as usize) % capacity;
        let bytes_to_tail = capacity - slot_offset;

        if bytes_to_tail < total {
            // Padding marker.
            unsafe {
                let dst = self.inner.slot_region().add(slot_offset);
                std::ptr::copy_nonoverlapping(&PADDING_MSG_TYPE as *const _ as *const u8, dst.add(4), 2);
                std::sync::atomic::fence(Ordering::Release);
                let len = bytes_to_tail as u32;
                std::ptr::copy_nonoverlapping(&len.to_le_bytes() as *const _ as *const u8, dst, 4);
            }
            header.producer_position.store(producer_pos + bytes_to_tail as u64, Ordering::Release);
            return self.write(msg_type, flags, header_extra, payload);
        }

        unsafe {
            let dst = self.inner.slot_region().add(slot_offset);
            std::ptr::copy_nonoverlapping(&msg_type as *const _ as *const u8, dst.add(4), 2);
            std::ptr::copy_nonoverlapping(&flags as *const _ as *const u8, dst.add(6), 2);
            std::ptr::copy_nonoverlapping(header_extra.as_ptr(), dst.add(8), 8);
            std::ptr::copy_nonoverlapping(payload.as_ptr(), dst.add(FRAME_HEADER_LEN), payload.len());
            let crc_input = std::slice::from_raw_parts(dst.add(4),
                FRAME_HEADER_LEN - 4 + payload.len());
            let crc = crc32fast::hash(crc_input);
            std::ptr::copy_nonoverlapping(&crc.to_le_bytes() as *const _ as *const u8,
                dst.add(FRAME_HEADER_LEN + payload.len()), 4);
            std::sync::atomic::fence(Ordering::Release);
            let len = total as u32;
            std::ptr::copy_nonoverlapping(&len.to_le_bytes() as *const _ as *const u8, dst, 4);
        }
        header.producer_position.store(producer_pos + total as u64, Ordering::Release);
        Ok(())
    }
}

pub struct BroadcastConsumer {
    inner: Arc<BroadcastInner>,
    head: u64,
}

impl BroadcastConsumer {
    /// New consumer starts reading from the current producer position
    /// (does not see historical records — broadcast is "join-and-listen").
    pub fn current_head(&self) -> u64 { self.head }

    pub fn try_read(&mut self, payload_buf: &mut Vec<u8>) -> Result<Option<RecordHeader>, RingError> {
        let header = self.inner.header();
        let capacity = self.inner.capacity();
        let producer_pos = header.producer_position.load(Ordering::Acquire);

        if self.head == producer_pos { return Ok(None); }

        // Check if we've fallen behind by more than the buffer.
        if (producer_pos - self.head) as usize > capacity {
            // Reset head to the current producer position; next call returns Overwritten.
            self.head = producer_pos;
            return Err(RingError::Overwritten);
        }

        let slot_offset = (self.head as usize) % capacity;
        let dst = unsafe { self.inner.slot_region().add(slot_offset) };

        let length = u32::from_le_bytes(
            unsafe { std::slice::from_raw_parts(dst, 4) }.try_into().unwrap());
        if length == 0 { return Ok(None); }
        std::sync::atomic::fence(Ordering::Acquire);

        let msg_type = u16::from_le_bytes(
            unsafe { std::slice::from_raw_parts(dst.add(4), 2) }.try_into().unwrap());
        let flags = u16::from_le_bytes(
            unsafe { std::slice::from_raw_parts(dst.add(6), 2) }.try_into().unwrap());
        let header_extra: [u8; 8] = unsafe { std::slice::from_raw_parts(dst.add(8), 8) }
            .try_into().unwrap();

        if msg_type == PADDING_MSG_TYPE {
            self.head += length as u64;
            return self.try_read(payload_buf);
        }

        let payload_len = (length as usize) - FRAME_HEADER_LEN - FRAME_TRAILER_LEN;
        let payload_src = unsafe { std::slice::from_raw_parts(
            dst.add(FRAME_HEADER_LEN), payload_len) };

        let crc_actual = u32::from_le_bytes(
            unsafe { std::slice::from_raw_parts(dst.add(FRAME_HEADER_LEN + payload_len), 4) }
                .try_into().unwrap());
        let crc_input = unsafe { std::slice::from_raw_parts(
            dst.add(4), FRAME_HEADER_LEN - 4 + payload_len) };
        if crc_actual != crc32fast::hash(crc_input) {
            return Err(RingError::BadCrc);
        }

        payload_buf.clear();
        payload_buf.extend_from_slice(payload_src);
        self.head += length as u64;
        Ok(Some(RecordHeader { msg_type, flags, header_extra }))
    }
}

pub struct BroadcastRing {
    _mmap: memmap2::MmapMut,
    inner: Arc<BroadcastInner>,
}

impl BroadcastRing {
    pub fn create(path: &std::path::Path, capacity_bytes: u64, max_msg_size: u32)
        -> Result<Self, RingError>
    {
        let file_len = RING_HEADER_LEN + capacity_bytes as usize;
        let file = std::fs::OpenOptions::new().read(true).write(true)
            .create(true).truncate(true).open(path)?;
        file.set_len(file_len as u64)?;
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
        crate::ring::common::init_ring_header(&mut mmap[..], capacity_bytes, max_msg_size, 0)?;
        let inner = Arc::new(BroadcastInner { base: mmap.as_mut_ptr(), file_len });
        Ok(BroadcastRing { _mmap: mmap, inner })
    }

    pub fn open(path: &std::path::Path) -> Result<Self, RingError> {
        let file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
        validate_ring_header(&mmap[..])?;
        let file_len = mmap.len();
        let inner = Arc::new(BroadcastInner { base: mmap.as_mut_ptr(), file_len });
        Ok(BroadcastRing { _mmap: mmap, inner })
    }

    pub fn producer(&self) -> BroadcastProducer {
        BroadcastProducer { inner: self.inner.clone() }
    }

    pub fn subscribe(&self) -> BroadcastConsumer {
        let head = self.inner.header().producer_position.load(Ordering::Acquire);
        BroadcastConsumer { inner: self.inner.clone(), head }
    }
}
```

- [ ] **Step 2: Test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn one_producer_two_consumers_same_records() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 65536, 1024).expect("create");
        let mut producer = ring.producer();
        let mut sub_a = ring.subscribe();
        let mut sub_b = ring.subscribe();

        for i in 0..5u8 {
            producer.write(1, 0, [i; 8], b"hello").expect("write");
        }

        for sub in [&mut sub_a, &mut sub_b] {
            for i in 0..5u8 {
                let mut buf = Vec::new();
                let rec = sub.try_read(&mut buf).expect("read").expect("non-empty");
                assert_eq!(rec.header_extra, [i; 8]);
                assert_eq!(&buf[..], b"hello");
            }
        }
    }

    #[test]
    fn slow_consumer_gets_overwritten_error() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 256, 128).expect("create");
        let mut producer = ring.producer();
        let mut sub = ring.subscribe();

        let payload = vec![0u8; 64];
        // Write many — enough that the slow consumer falls > 1 buffer behind.
        for _ in 0..20 { producer.write(1, 0, [0; 8], &payload).expect("write"); }

        let mut buf = Vec::new();
        let result = sub.try_read(&mut buf);
        assert!(matches!(result, Err(RingError::Overwritten)),
            "slow consumer should see Overwritten, got: {result:?}");
    }
}
```

- [ ] **Step 3: Build, test, commit**

```bash
cargo test -p uc_protocol ring::broadcast::tests
git add uc_protocol/src/ring/{broadcast,common}.rs
git commit -m "feat(uc_protocol): Broadcast ring (1-producer many-consumers, no backpressure)"
```

---

## Task 5: `uc_protocol::cnc` — the cnc.dat layout

The cnc.dat file is a single mmap'd region with a fixed-offset header + per-region sub-buffers. The header points to: node_status, service_status, control_to_service (small MPSC), control_to_node (small MPSC). Counters and error_log sub-buffers are reserved for M5 and not populated.

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_protocol/src/cnc.rs`

- [ ] **Step 1: Implement CncHeader + status blocks**

Per spec §4 cnc.dat layout. Header is 256 bytes; node_status and service_status are 64 bytes each; control rings are sized at construction.

```rust
//! cnc.dat layout — the shared-memory control plane between node, service, and clients.

use std::sync::atomic::{AtomicU32, AtomicU64};
use crate::ProtocolVersion;

pub const CNC_HEADER_LEN: usize = 256;
pub const STATUS_BLOCK_LEN: usize = 64;
pub const CNC_CONTROL_RING_CAP: u64 = 16 * 1024;
pub const CNC_CONTROL_RING_MAX_MSG: u32 = 1024;

/// Fixed-offset header at the start of cnc.dat. 256 bytes.
#[repr(C, align(64))]
pub struct CncHeader {
    pub magic: [u8; 8],
    pub protocol_version: u32,                   // ProtocolVersion::0
    pub page_size: u32,
    pub cnc_size_bytes: u64,
    pub instance_id_low: u64,
    pub instance_id_high: u64,
    pub app_id: [u8; 64],                        // utf-8, null-padded
    pub node_id: u64,
    pub created_at_unix_ns: u64,
    pub sub_buffer_offsets: [u64; 8],            // offsets within cnc.dat
    pub sub_buffer_sizes: [u64; 8],
    pub header_crc32: u32,                       // over bytes [0..0xb4]
    pub _pad: [u8; 256 - 0xb4 - 4],
}

const _: () = {
    assert!(std::mem::size_of::<CncHeader>() == CNC_HEADER_LEN);
};

#[repr(C, align(64))]
pub struct NodeStatus {
    pub role: AtomicU32,                          // 0=Init, 1=Follower, 2=Candidate, 3=Leader, 4=Shutting
    pub current_term: AtomicU64,
    pub leader_node_id: AtomicU64,                // u64::MAX = unknown
    pub last_applied: AtomicU64,
    pub last_committed: AtomicU64,
    pub heartbeat_seq: AtomicU64,
    pub heartbeat_at_ns: AtomicU64,
    pub _pad: [u8; 8],
}

const _: () = {
    assert!(std::mem::size_of::<NodeStatus>() == STATUS_BLOCK_LEN);
};

#[repr(C, align(64))]
pub struct ServiceStatus {
    pub state: AtomicU32,                         // 0=Disconnected, 1=Handshaking, 2=Ready, 3=Snapshotting, 4=Stalled
    pub _pad_1: u32,
    pub last_applied: AtomicU64,
    pub last_output_ack: AtomicU64,
    pub heartbeat_seq: AtomicU64,
    pub heartbeat_at_ns: AtomicU64,
    pub service_pid: AtomicU32,
    pub _pad_2: [u8; 20],
}

const _: () = {
    assert!(std::mem::size_of::<ServiceStatus>() == STATUS_BLOCK_LEN);
};

/// Sub-buffer indices in `CncHeader::sub_buffer_{offsets,sizes}`.
pub mod sub {
    pub const NODE_STATUS: usize = 0;
    pub const SERVICE_STATUS: usize = 1;
    pub const CONTROL_TO_SERVICE: usize = 2;
    pub const CONTROL_TO_NODE: usize = 3;
    pub const CONTROL_TO_CLIENTS: usize = 4;     // (M4)
    pub const COUNTERS_METADATA: usize = 5;       // (M5)
    pub const COUNTERS_VALUES: usize = 6;         // (M5)
    pub const ERROR_LOG: usize = 7;               // (M5)
}

/// Compute total cnc.dat file size for the given control-ring layout.
pub fn cnc_file_size() -> usize {
    CNC_HEADER_LEN
        + STATUS_BLOCK_LEN     // node_status
        + STATUS_BLOCK_LEN     // service_status
        + crate::ring::common::RING_HEADER_LEN + CNC_CONTROL_RING_CAP as usize    // control_to_service
        + crate::ring::common::RING_HEADER_LEN + CNC_CONTROL_RING_CAP as usize    // control_to_node
}

/// Initialize a freshly-created cnc.dat mmap.
/// Writes header, status blocks (zeroed), and initializes the two control MPSC rings.
pub fn init_cnc(
    mmap: &mut [u8],
    app_id: &str,
    node_id: u64,
    instance_id: u128,
) -> Result<(), crate::ring::common::RingError> {
    let file_size = cnc_file_size();
    if mmap.len() < file_size {
        return Err(crate::ring::common::RingError::Corrupt(format!(
            "cnc mmap too small: {} < {file_size}", mmap.len())));
    }

    let mut app_id_bytes = [0u8; 64];
    let copy_len = app_id.len().min(64);
    app_id_bytes[..copy_len].copy_from_slice(&app_id.as_bytes()[..copy_len]);

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    let page_size = page_size::get() as u32;

    let off_node_status = CNC_HEADER_LEN as u64;
    let off_service_status = off_node_status + STATUS_BLOCK_LEN as u64;
    let off_control_to_service = off_service_status + STATUS_BLOCK_LEN as u64;
    let off_control_to_node = off_control_to_service
        + crate::ring::common::RING_HEADER_LEN as u64 + CNC_CONTROL_RING_CAP;

    let mut sub_buffer_offsets = [0u64; 8];
    let mut sub_buffer_sizes = [0u64; 8];
    sub_buffer_offsets[sub::NODE_STATUS] = off_node_status;
    sub_buffer_sizes[sub::NODE_STATUS] = STATUS_BLOCK_LEN as u64;
    sub_buffer_offsets[sub::SERVICE_STATUS] = off_service_status;
    sub_buffer_sizes[sub::SERVICE_STATUS] = STATUS_BLOCK_LEN as u64;
    sub_buffer_offsets[sub::CONTROL_TO_SERVICE] = off_control_to_service;
    sub_buffer_sizes[sub::CONTROL_TO_SERVICE] =
        crate::ring::common::RING_HEADER_LEN as u64 + CNC_CONTROL_RING_CAP;
    sub_buffer_offsets[sub::CONTROL_TO_NODE] = off_control_to_node;
    sub_buffer_sizes[sub::CONTROL_TO_NODE] =
        crate::ring::common::RING_HEADER_LEN as u64 + CNC_CONTROL_RING_CAP;

    let header = CncHeader {
        magic: crate::magic::CNC_MAGIC,
        protocol_version: crate::version::CURRENT.0,
        page_size,
        cnc_size_bytes: file_size as u64,
        instance_id_low: instance_id as u64,
        instance_id_high: (instance_id >> 64) as u64,
        app_id: app_id_bytes,
        node_id,
        created_at_unix_ns: now_ns,
        sub_buffer_offsets,
        sub_buffer_sizes,
        header_crc32: 0,
        _pad: [0; 256 - 0xb4 - 4],
    };
    // SAFETY: mmap is at least file_size bytes and aligned.
    unsafe {
        std::ptr::write(mmap.as_mut_ptr() as *mut CncHeader, header);
    }

    // Compute & write CRC over the header's leading 0xb4 bytes.
    let crc = crc32fast::hash(&mmap[..0xb4]);
    unsafe {
        std::ptr::copy_nonoverlapping(
            &crc.to_le_bytes() as *const _ as *const u8,
            mmap.as_mut_ptr().add(0xb4), 4);
    }

    // Initialize status blocks (zero out — atomics start at zero).
    mmap[off_node_status as usize..off_node_status as usize + STATUS_BLOCK_LEN].fill(0);
    mmap[off_service_status as usize..off_service_status as usize + STATUS_BLOCK_LEN].fill(0);

    // Initialize the two control rings (MPSC).
    crate::ring::common::init_ring_header(
        &mut mmap[off_control_to_service as usize..],
        CNC_CONTROL_RING_CAP, CNC_CONTROL_RING_MAX_MSG, 0)?;
    crate::ring::common::init_ring_header(
        &mut mmap[off_control_to_node as usize..],
        CNC_CONTROL_RING_CAP, CNC_CONTROL_RING_MAX_MSG, 0)?;

    Ok(())
}

/// Validate an existing cnc.dat header. Used by attaching parties (service, clients).
pub fn validate_cnc<'a>(mmap: &'a [u8])
    -> Result<&'a CncHeader, crate::ring::common::RingError>
{
    if mmap.len() < CNC_HEADER_LEN {
        return Err(crate::ring::common::RingError::Corrupt(
            "cnc mmap too small for header".into()));
    }
    let header_ptr = mmap.as_ptr() as *const CncHeader;
    let header = unsafe { &*header_ptr };
    if header.magic != crate::magic::CNC_MAGIC {
        return Err(crate::ring::common::RingError::BadMagic);
    }
    let crc_expected = crc32fast::hash(&mmap[..0xb4]);
    if header.header_crc32 != crc_expected {
        return Err(crate::ring::common::RingError::BadCrc);
    }
    Ok(header)
}

impl CncHeader {
    pub fn instance_id(&self) -> u128 {
        (self.instance_id_high as u128) << 64 | (self.instance_id_low as u128)
    }
    pub fn app_id_str(&self) -> &str {
        let null = self.app_id.iter().position(|&b| b == 0).unwrap_or(64);
        std::str::from_utf8(&self.app_id[..null]).unwrap_or("")
    }
}
```

Add `page_size = "0.6"` to workspace deps + uc_protocol deps.

- [ ] **Step 2: Test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn init_then_validate_cnc() {
        let file = NamedTempFile::new().unwrap();
        let file_size = cnc_file_size();
        std::fs::File::options().read(true).write(true).open(file.path()).unwrap()
            .set_len(file_size as u64).unwrap();
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(
            &std::fs::File::options().read(true).write(true).open(file.path()).unwrap()).unwrap() };

        init_cnc(&mut mmap[..], "test-app", 42, 0xdeadbeef).expect("init");
        let header = validate_cnc(&mmap[..]).expect("validate");
        assert_eq!(header.app_id_str(), "test-app");
        assert_eq!(header.node_id, 42);
        assert_eq!(header.instance_id(), 0xdeadbeef);
    }
}
```

- [ ] **Step 3: Commit**

```bash
git add uc_protocol/Cargo.toml uc_protocol/src/cnc.rs Cargo.toml
git commit -m "feat(uc_protocol): cnc.dat layout (header + status blocks + control rings)"
```

---

## Task 6: Frame types (apply/query/snapshot control)

Define typed wrappers around the SPSC frame slot's `header_extra` for the three message kinds M3 uses.

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_protocol/src/frames/apply.rs`
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_protocol/src/frames/query.rs`
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_protocol/src/frames/snapshot.rs`

- [ ] **Step 1: `frames/apply.rs`**

```rust
//! Apply ring frame types.
//!
//! `header_extra` layout (8 bytes):
//!   bytes 0..8 — log_index (u64, little-endian).
//!
//! `msg_type`:
//!   1 = ApplyFrame (node → service)
//!   2 = ApplyRespFrame (service → node)

pub const MSG_TYPE_APPLY: u16 = 1;
pub const MSG_TYPE_APPLY_RESP: u16 = 2;

pub fn encode_extra_apply(log_index: u64) -> [u8; 8] {
    log_index.to_le_bytes()
}

pub fn decode_extra_apply(extra: [u8; 8]) -> u64 {
    u64::from_le_bytes(extra)
}
```

- [ ] **Step 2: `frames/query.rs`**

```rust
//! Query ring frame types.
//!
//! `header_extra` layout (8 bytes):
//!   bytes 0..4 — request_id (u32, little-endian; allocated by node, scoped to lifetime of the ring)
//!   byte 4    — kind (0 = Linearizable, 1 = Snapshot)
//!   bytes 5..8 — reserved (zero)
//!
//! `msg_type`:
//!   3 = QueryFrame (node → service)
//!   4 = QueryRespFrame (service → node)

pub const MSG_TYPE_QUERY: u16 = 3;
pub const MSG_TYPE_QUERY_RESP: u16 = 4;

#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum QueryKind { Linearizable = 0, Snapshot = 1 }

pub fn encode_extra_query(request_id: u32, kind: QueryKind) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[0..4].copy_from_slice(&request_id.to_le_bytes());
    out[4] = kind as u8;
    out
}

pub fn decode_extra_query(extra: [u8; 8]) -> (u32, QueryKind) {
    let request_id = u32::from_le_bytes(extra[0..4].try_into().unwrap());
    let kind = match extra[4] { 0 => QueryKind::Linearizable, 1 => QueryKind::Snapshot, _ => QueryKind::Snapshot };
    (request_id, kind)
}
```

- [ ] **Step 3: `frames/snapshot.rs`** (cnc-control-channel frame for snapshot orchestration)

```rust
//! Snapshot control frames (sent over cnc control_to_service / control_to_node rings).
//!
//! M3 uses these for the build/install handshake; the actual snapshot bytes
//! flow via the existing M2 path (Cursor<Vec<u8>> + openraft InstallSnapshot
//! RPC over QUIC). M5 swaps to a snapshot.region mmap.

pub const MSG_TYPE_BUILD_SNAPSHOT: u16 = 100;     // node → service
pub const MSG_TYPE_SNAPSHOT_BUILT: u16 = 101;     // service → node
```

- [ ] **Step 4: Build, test, commit**

```bash
cargo build -p uc_protocol
cargo test -p uc_protocol
git add uc_protocol/src/frames/
git commit -m "feat(uc_protocol): frame types for apply / query / snapshot control"
```

---

## Task 7: Liveness + handshake helpers in `uc_protocol`

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_protocol/src/liveness.rs`
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_protocol/src/handshake.rs`

- [ ] **Step 1: `liveness.rs`**

```rust
//! Heartbeat helpers. Each writer (node, service) increments its own
//! `heartbeat_seq` in the cnc.dat status block every `tick_interval`.
//! Watchers compare seq deltas against `liveness_timeout` to detect peer death.

use std::sync::atomic::Ordering;
use crate::cnc::{NodeStatus, ServiceStatus};

pub fn tick_node(status: &NodeStatus, now_ns: u64) {
    status.heartbeat_seq.fetch_add(1, Ordering::Relaxed);
    status.heartbeat_at_ns.store(now_ns, Ordering::Relaxed);
}

pub fn tick_service(status: &ServiceStatus, now_ns: u64) {
    status.heartbeat_seq.fetch_add(1, Ordering::Relaxed);
    status.heartbeat_at_ns.store(now_ns, Ordering::Relaxed);
}

#[derive(Debug, Copy, Clone)]
pub struct HeartbeatWatcher {
    pub last_seq: u64,
    pub last_seen_ns: u64,
}

impl HeartbeatWatcher {
    pub fn new(current_seq: u64, now_ns: u64) -> Self {
        Self { last_seq: current_seq, last_seen_ns: now_ns }
    }

    /// Returns true if the watched party is still alive (seq advanced since last poll).
    pub fn poll_alive_node(&mut self, status: &NodeStatus, now_ns: u64,
        timeout_ns: u64) -> bool
    {
        let seq = status.heartbeat_seq.load(Ordering::Relaxed);
        if seq != self.last_seq {
            self.last_seq = seq;
            self.last_seen_ns = now_ns;
            true
        } else {
            now_ns.saturating_sub(self.last_seen_ns) < timeout_ns
        }
    }

    pub fn poll_alive_service(&mut self, status: &ServiceStatus, now_ns: u64,
        timeout_ns: u64) -> bool
    {
        let seq = status.heartbeat_seq.load(Ordering::Relaxed);
        if seq != self.last_seq {
            self.last_seq = seq;
            self.last_seen_ns = now_ns;
            true
        } else {
            now_ns.saturating_sub(self.last_seen_ns) < timeout_ns
        }
    }
}
```

- [ ] **Step 2: `handshake.rs`**

```rust
//! Handshake frame types sent over cnc.dat's control rings.
//!
//! Service → node: `ServiceReady { last_applied }` via control_to_node ring.
//!   msg_type = 200
//! Node → service: `RoleChanged { role }` via control_to_service ring.
//!   msg_type = 201

pub const MSG_TYPE_SERVICE_READY: u16 = 200;
pub const MSG_TYPE_ROLE_CHANGED: u16 = 201;

/// `header_extra` for ServiceReady: bytes 0..8 = service's last_applied (u64 LE).
pub fn encode_extra_service_ready(last_applied: u64) -> [u8; 8] {
    last_applied.to_le_bytes()
}

pub fn decode_extra_service_ready(extra: [u8; 8]) -> u64 {
    u64::from_le_bytes(extra)
}

/// `header_extra` for RoleChanged: byte 0 = role (0=Init, 1=Follower, 2=Candidate, 3=Leader, 4=Shutting).
pub fn encode_extra_role_changed(role: u8) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[0] = role;
    out
}
```

- [ ] **Step 3: Commit**

```bash
cargo build -p uc_protocol
cargo test -p uc_protocol
git add uc_protocol/src/{liveness,handshake}.rs
git commit -m "feat(uc_protocol): liveness heartbeat helpers + handshake frame types"
```

---

## Task 8: `uc_service::ultima_db` adapter module

The canonical default path for `ultima_db` users: zero snapshot wiring, automatic version-pinning.

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/Cargo.toml` — add `ultima_db` feature (default-on) pulling `ultima-db` workspace dep.
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/Cargo.toml` — add `ultima-db = { path = "../ultima_db" }` workspace dep.
- Create: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/ultima_db/mod.rs`
- Create: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/ultima_db/store_state_machine.rs`
- Create: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/ultima_db/builder.rs`
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/lib.rs` — add `pub mod ultima_db;` gated on feature.

- [ ] **Step 1: Workspace dep + uc_service feature**

In root `Cargo.toml`:
```toml
ultima-db = { path = "../ultima_db" }
```

In `uc_service/Cargo.toml`:
```toml
[features]
default = ["ultima_db"]
ultima_db = ["dep:ultima-db"]

[dependencies]
# ... existing ...
ultima-db = { workspace = true, optional = true }
```

- [ ] **Step 2: `uc_service/src/ultima_db/mod.rs`**

```rust
//! Canonical adapter from `uc_service::StateMachine` to `ultima_db::Store`.
//!
//! Gated on the `ultima_db` Cargo feature (default-on). Users with non-ultima_db
//! state implement `StateMachine` themselves and skip this module.

pub mod store_state_machine;
pub mod builder;

pub use store_state_machine::StoreStateMachine;
pub use builder::StoreStateMachineBuilder;
```

- [ ] **Step 3: `store_state_machine.rs`**

The full code is per spec §5 "uc_service::ultima_db". Implements `StateMachine` over `ultima_db::Store` with user-provided closures for the command-handler and query-handler. The adapter pins `store.begin_write(Some(log_index))` on every apply to keep raft log_index in lockstep with ultima_db version.

Skeleton:

```rust
use std::io::{Read, Write};
use serde::{Serialize, de::DeserializeOwned};
use ultima_db::{Store, WriteTx, ReadTx};
use crate::{StateMachine, SnapshotError};

pub struct StoreStateMachine<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    pub(crate) store: Store,
    pub(crate) apply_fn: Box<dyn Fn(&mut WriteTx<'_>, C) -> R + Send + Sync>,
    pub(crate) query_fn: Box<dyn Fn(&ReadTx<'_>, Q) -> QR + Send + Sync>,
}

impl<C, R, Q, QR> StateMachine for StoreStateMachine<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    type Command = C;
    type Response = R;
    type Query = Q;
    type QueryResponse = QR;

    fn apply(&mut self, log_index: u64, cmd: Self::Command) -> Self::Response {
        let mut tx = self.store.begin_write(Some(log_index))
            .expect("ultima_db begin_write");
        let resp = (self.apply_fn)(&mut tx, cmd);
        tx.commit().expect("ultima_db commit");
        resp
    }

    fn query(&self, q: Self::Query) -> Self::QueryResponse {
        let tx = self.store.begin_read(None).expect("ultima_db begin_read");
        (self.query_fn)(&tx, q)
    }

    fn last_applied(&self) -> Option<u64> {
        Some(self.store.latest_version())
    }

    fn build_snapshot(&self, dst: &mut dyn Write) -> Result<u64, SnapshotError> {
        let mut reader = self.store.snapshot_stream(None)
            .map_err(|e| SnapshotError::Codec(format!("{e}")))?;
        std::io::copy(&mut reader, dst)?;
        Ok(self.store.latest_version())
    }

    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<u64, SnapshotError> {
        self.store.install_snapshot_stream(src, ultima_db::InstallOptions::default())
            .map_err(|e| SnapshotError::Codec(format!("{e}")))?;
        self.store.checkpoint().map_err(|e| SnapshotError::Codec(format!("{e}")))?;
        Ok(self.store.latest_version())
    }
}
```

(Note: this uses the ultima_db APIs from task27. Exact method names — `begin_write`, `begin_read`, `latest_version`, `snapshot_stream`, `install_snapshot_stream`, `checkpoint` — should be verified against the current ultima_db source.)

- [ ] **Step 4: `builder.rs`**

```rust
use ultima_db::{Store, WriteTx, ReadTx};
use serde::{Serialize, de::DeserializeOwned};
use super::store_state_machine::StoreStateMachine;

pub struct StoreStateMachineBuilder<C, R, Q, QR> {
    store: Store,
    apply_fn: Option<Box<dyn Fn(&mut WriteTx<'_>, C) -> R + Send + Sync>>,
    query_fn: Option<Box<dyn Fn(&ReadTx<'_>, Q) -> QR + Send + Sync>>,
}

impl<C, R, Q, QR> StoreStateMachine<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    pub fn builder(store: Store) -> StoreStateMachineBuilder<C, R, Q, QR> {
        StoreStateMachineBuilder { store, apply_fn: None, query_fn: None }
    }
}

impl<C, R, Q, QR> StoreStateMachineBuilder<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    pub fn apply_fn<F>(mut self, f: F) -> Self
    where F: Fn(&mut WriteTx<'_>, C) -> R + Send + Sync + 'static
    {
        self.apply_fn = Some(Box::new(f));
        self
    }

    pub fn query_fn<F>(mut self, f: F) -> Self
    where F: Fn(&ReadTx<'_>, Q) -> QR + Send + Sync + 'static
    {
        self.query_fn = Some(Box::new(f));
        self
    }

    pub fn build(self) -> Result<StoreStateMachine<C, R, Q, QR>, BuildError> {
        Ok(StoreStateMachine {
            store: self.store,
            apply_fn: self.apply_fn.ok_or(BuildError::MissingApplyFn)?,
            query_fn: self.query_fn.ok_or(BuildError::MissingQueryFn)?,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("missing apply_fn")]
    MissingApplyFn,
    #[error("missing query_fn")]
    MissingQueryFn,
}
```

- [ ] **Step 5: Wire into lib.rs**

```rust
#[cfg(feature = "ultima_db")]
pub mod ultima_db;
```

- [ ] **Step 6: Build + commit**

```bash
cargo build -p uc_service
cargo build -p uc_service --no-default-features    # verify ultima_db is optional
git add Cargo.toml uc_service/
git commit -m "feat(uc_service): ultima_db adapter module (StoreStateMachine, default-on feature)"
```

---

## Task 9: `uc_service` runtime — module skeleton + ServiceBuilder

**Files:**
- Create: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/runtime/mod.rs`
- Create: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/runtime/service.rs`
- Create: 4 stubs (`attach.rs`, `apply_loop.rs`, `query_loop.rs`, `handshake.rs`, `liveness.rs`)
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/Cargo.toml` — add tokio + uc_protocol path deps.
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/lib.rs` — `pub mod runtime;`.

- [ ] **Step 1: Cargo.toml additions**

```toml
[dependencies]
# ... existing ...
tokio = { workspace = true }
bincode = { workspace = true }
bytes = { workspace = true }
```

- [ ] **Step 2: `runtime/mod.rs`**

```rust
//! Service-side runtime — attaches to a uc_node's cnc.dat + rings.

pub mod attach;
pub mod apply_loop;
pub mod handshake;
pub mod liveness;
pub mod query_loop;
pub mod service;

pub use service::{Service, ServiceBuilder, ServiceConfig};
```

- [ ] **Step 3: `runtime/service.rs` — ServiceConfig + ServiceBuilder skeleton**

```rust
use std::path::PathBuf;
use std::time::Duration;
use crate::{StateMachine, OutputHandler, NoopOutput};
use super::attach::AttachedRings;

pub struct ServiceConfig {
    pub instance_dir: PathBuf,
    pub app_id: String,
    pub data_dir: PathBuf,
    pub liveness_timeout: Duration,
    pub apply_ring_capacity_bytes: u64,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            instance_dir: PathBuf::from("/tmp/ultima-default"),
            app_id: String::new(),
            data_dir: PathBuf::from("./service-data"),
            liveness_timeout: Duration::from_secs(5),
            apply_ring_capacity_bytes: 64 * 1024 * 1024,
        }
    }
}

pub struct ServiceBuilder<S: StateMachine> {
    pub(crate) config: ServiceConfig,
    pub(crate) state_machine: S,
}

impl<S: StateMachine> ServiceBuilder<S> {
    pub fn new(config: ServiceConfig, state_machine: S) -> Self {
        Self { config, state_machine }
    }

    pub fn output_handler<O: OutputHandler<S>>(self, _h: O) -> Self {
        // M5 wires this in.
        self
    }

    pub async fn run(self) -> Result<(), ServiceError> {
        // Implementation lands in Task 11.
        let _ = self.config;
        let _ = self.state_machine;
        unimplemented!("Task 11")
    }
}

pub struct Service { _opaque: () }

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("ipc: {0}")]
    Ipc(String),
    #[error("snapshot: {0}")]
    Snapshot(#[from] crate::SnapshotError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("ring: {0}")]
    Ring(#[from] uc_protocol::ring::RingError),
}
```

- [ ] **Step 4: Stub the other 5 files with `//! Filled in by Task N.`** mapping to Tasks 10, 11, 12, 12 again, 12.

- [ ] **Step 5: Build + commit**

```bash
cargo build -p uc_service
git add uc_service/
git commit -m "feat(uc_service): runtime module skeleton (ServiceBuilder + ServiceConfig)"
```

---

## Task 10: Service-side attach + handshake

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/runtime/attach.rs`
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/runtime/handshake.rs`

- [ ] **Step 1: `attach.rs` — opens cnc.dat + rings**

```rust
use std::path::{Path, PathBuf};
use memmap2::MmapMut;
use uc_protocol::cnc::{CncHeader, validate_cnc, CNC_HEADER_LEN, NodeStatus, ServiceStatus, sub};
use uc_protocol::ring::common::{validate_ring_header, RING_HEADER_LEN};
use uc_protocol::ring::spsc::{SpscRing, SpscProducer, SpscConsumer};

pub struct AttachedRings {
    pub _cnc_mmap: MmapMut,                  // keep alive
    pub instance_id: u128,
    pub node_id: u64,
    pub apply_ring_consumer: SpscConsumer,
    pub apply_resp_ring_producer: SpscProducer,
    pub query_ring_consumer: SpscConsumer,
    pub query_resp_ring_producer: SpscProducer,
    // Hold onto the mmaps for ring files too.
    pub _apply_mmap: MmapMut,
    pub _apply_resp_mmap: MmapMut,
    pub _query_mmap: MmapMut,
    pub _query_resp_mmap: MmapMut,
}

pub fn attach(instance_dir: &Path, expected_app_id: &str) -> Result<AttachedRings, super::service::ServiceError> {
    let cnc_path = instance_dir.join("cnc.dat");
    let cnc_file = std::fs::OpenOptions::new().read(true).write(true).open(&cnc_path)?;
    let cnc_mmap = unsafe { MmapMut::map_mut(&cnc_file)? };
    let header = validate_cnc(&cnc_mmap[..])
        .map_err(|e| super::service::ServiceError::Ipc(format!("validate cnc: {e}")))?;

    if header.app_id_str() != expected_app_id {
        return Err(super::service::ServiceError::Ipc(format!(
            "app_id mismatch: got '{}' expected '{expected_app_id}'", header.app_id_str())));
    }
    let instance_id = header.instance_id();
    let node_id = header.node_id;

    // Open the four ring files.
    let attach_ring = |path: PathBuf| -> Result<(SpscRing, MmapMut), super::service::ServiceError> {
        let ring = SpscRing::open(&path)
            .map_err(|e| super::service::ServiceError::Ring(e))?;
        let (p, c, mm) = ring.split();
        // We need to return both producer and consumer separately; split here is awkward.
        // Refactor: have SpscRing expose owned producer/consumer with mmap held inside.
        todo!("see Task 10 step 2 — refactor SpscRing API");
    };
    todo!()
}
```

Step 1 reveals a real API issue: `SpscRing::split` returns the mmap separately, which makes attaching N rings clumsy. Refactor in **Task 10 step 2**.

- [ ] **Step 2: Refactor SpscRing API**

In `uc_protocol/src/ring/spsc.rs`, change `SpscRing::split` to keep the mmap alive inside one of the returned handles (or use `Arc<MmapMut>`):

```rust
// Replace existing SpscInner with:
pub struct SpscInner {
    _mmap: memmap2::MmapMut,    // owns the mmap — keeps the pointer valid
    base: *mut u8,
    file_len: usize,
}

// SAFETY: base points into _mmap, which is owned by this struct.
unsafe impl Send for SpscInner {}
unsafe impl Sync for SpscInner {}

// SpscRing::create / open builds Arc<SpscInner>:
impl SpscRing {
    pub fn create(...) -> Result<Self, RingError> {
        let mut mmap = ...;
        crate::ring::common::init_ring_header(&mut mmap[..], capacity_bytes, max_msg_size, 0)?;
        let base = mmap.as_mut_ptr();
        let inner = Arc::new(SpscInner { _mmap: mmap, base, file_len });
        Ok(SpscRing { inner })
    }
    // open similarly
    pub fn into_split(self) -> (SpscProducer, SpscConsumer) {
        (
            SpscProducer { inner: self.inner.clone() },
            SpscConsumer { inner: self.inner },
        )
    }
}
```

Update tests in `spsc.rs` to use `into_split` (no longer returns the mmap separately — Arc<SpscInner> holds it). Same refactor for `mpsc.rs` and `broadcast.rs`.

This makes attach much cleaner.

- [ ] **Step 3: Now finish `attach.rs`**

```rust
pub fn attach(instance_dir: &Path, expected_app_id: &str) -> Result<AttachedRings, super::service::ServiceError> {
    let cnc_path = instance_dir.join("cnc.dat");
    let cnc_file = std::fs::OpenOptions::new().read(true).write(true).open(&cnc_path)?;
    let cnc_mmap = unsafe { MmapMut::map_mut(&cnc_file)? };
    let header = validate_cnc(&cnc_mmap[..])
        .map_err(|e| super::service::ServiceError::Ipc(format!("validate cnc: {e}")))?;

    if header.app_id_str() != expected_app_id {
        return Err(super::service::ServiceError::Ipc(format!(
            "app_id mismatch: got '{}' expected '{expected_app_id}'", header.app_id_str())));
    }
    let instance_id = header.instance_id();
    let node_id = header.node_id;

    let service_dir = instance_dir.join("service");

    let apply_ring = SpscRing::open(&service_dir.join("apply.ring"))?;
    let apply_resp_ring = SpscRing::open(&service_dir.join("apply_resp.ring"))?;
    let query_ring = SpscRing::open(&service_dir.join("query.ring"))?;
    let query_resp_ring = SpscRing::open(&service_dir.join("query_resp.ring"))?;

    let (_, apply_consumer) = apply_ring.into_split();
    let (apply_resp_producer, _) = apply_resp_ring.into_split();
    let (_, query_consumer) = query_ring.into_split();
    let (query_resp_producer, _) = query_resp_ring.into_split();

    Ok(AttachedRings {
        _cnc_mmap: cnc_mmap,
        instance_id,
        node_id,
        apply_ring_consumer: apply_consumer,
        apply_resp_ring_producer: apply_resp_producer,
        query_ring_consumer: query_consumer,
        query_resp_ring_producer: query_resp_producer,
        _apply_mmap: ...,   // (we lose the unused half of each ring's Arc; alternative is to keep both consumers/producers)
        ...
    })
}
```

Practical note: `into_split` discards the half we don't need. The mmap is held inside `Arc<SpscInner>` which is shared. When we discard the unused half, the Arc count drops but the held half keeps the inner alive. So we don't need to keep the separate mmap field.

Simplify the struct:

```rust
pub struct AttachedRings {
    pub _cnc_mmap: MmapMut,
    pub instance_id: u128,
    pub node_id: u64,
    pub apply_consumer: SpscConsumer,
    pub apply_resp_producer: SpscProducer,
    pub query_consumer: SpscConsumer,
    pub query_resp_producer: SpscProducer,
}
```

- [ ] **Step 4: `handshake.rs` — service-side handshake**

```rust
//! Service-side handshake: write ServiceReady to the cnc control_to_node ring,
//! set service_status.state = Ready, begin heartbeat.

use uc_protocol::cnc::{ServiceStatus};
use uc_protocol::handshake::{MSG_TYPE_SERVICE_READY, encode_extra_service_ready};
use uc_protocol::ring::mpsc::MpscProducer;
use std::sync::atomic::Ordering;

pub const STATE_DISCONNECTED: u32 = 0;
pub const STATE_HANDSHAKING: u32 = 1;
pub const STATE_READY: u32 = 2;
pub const STATE_SNAPSHOTTING: u32 = 3;
pub const STATE_STALLED: u32 = 4;

pub fn send_service_ready(
    control_to_node: &MpscProducer,
    last_applied: u64,
) -> Result<(), uc_protocol::ring::common::RingError> {
    control_to_node.try_write(
        MSG_TYPE_SERVICE_READY,
        0,
        encode_extra_service_ready(last_applied),
        &[],
    )
}

pub fn set_service_state(status: &ServiceStatus, state: u32) {
    status.state.store(state, Ordering::Release);
}
```

- [ ] **Step 5: Commit**

```bash
cargo build -p uc_service
git add uc_protocol/src/ring/ uc_service/src/runtime/{attach,handshake}.rs
git commit -m "feat(uc_service): cnc attach + service-side handshake"
```

---

## Task 11: Service apply loop + query loop + heartbeat

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/runtime/apply_loop.rs`
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/runtime/query_loop.rs`
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/runtime/liveness.rs`
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_service/src/runtime/service.rs` — finish `run()`.

- [ ] **Step 1: `apply_loop.rs` — sync apply thread**

```rust
//! Sync apply loop running on a dedicated std::thread.

use bincode;
use uc_protocol::frames::apply::{
    MSG_TYPE_APPLY, MSG_TYPE_APPLY_RESP, decode_extra_apply, encode_extra_apply,
};
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};
use crate::StateMachine;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

pub struct ApplyLoopHandle {
    pub join: std::thread::JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn_apply_loop<S: StateMachine>(
    sm: Arc<parking_lot::Mutex<S>>,
    mut consumer: SpscConsumer,
    mut resp_producer: SpscProducer,
) -> ApplyLoopHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_thread = stop.clone();
    let join = std::thread::Builder::new()
        .name("uc-service-apply".into())
        .spawn(move || {
            let mut payload_buf = Vec::with_capacity(4096);
            while !stop_for_thread.load(Ordering::Relaxed) {
                match consumer.try_read(&mut payload_buf) {
                    Ok(Some(rec)) if rec.msg_type == MSG_TYPE_APPLY => {
                        let log_index = decode_extra_apply(rec.header_extra);
                        let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(
                            &payload_buf, bincode::config::standard())
                            .expect("apply decode");
                        let mut sm_guard = sm.lock();
                        let resp = sm_guard.apply(log_index, cmd);
                        drop(sm_guard);
                        let resp_bytes = bincode::serde::encode_to_vec(&resp,
                            bincode::config::standard()).expect("encode");
                        // Write response — busy-spin on Full.
                        loop {
                            match resp_producer.try_write(MSG_TYPE_APPLY_RESP, 0,
                                encode_extra_apply(log_index), &resp_bytes)
                            {
                                Ok(()) => break,
                                Err(uc_protocol::ring::RingError::Full) =>
                                    std::thread::yield_now(),
                                Err(e) => panic!("apply_resp write: {e}"),
                            }
                        }
                    }
                    Ok(Some(_)) | Ok(None) => {
                        std::thread::sleep(std::time::Duration::from_micros(100));
                    }
                    Err(e) => {
                        tracing::warn!(error = ?e, "apply ring read error");
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                }
            }
        })
        .expect("spawn apply thread");
    ApplyLoopHandle { join, stop }
}
```

- [ ] **Step 2: `query_loop.rs` — tokio task draining query.ring**

Similar to apply_loop but on tokio. Uses `tokio::task::spawn_blocking` for the actual sync ring read + sm.query call.

```rust
//! Async query loop.

use bincode;
use uc_protocol::frames::query::{
    MSG_TYPE_QUERY, MSG_TYPE_QUERY_RESP, decode_extra_query, encode_extra_query,
};
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};
use crate::StateMachine;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use tokio::task::JoinHandle;

pub struct QueryLoopHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn_query_loop<S: StateMachine>(
    sm: Arc<parking_lot::Mutex<S>>,
    mut consumer: SpscConsumer,
    mut resp_producer: SpscProducer,
) -> QueryLoopHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = stop.clone();
    let join = tokio::spawn(async move {
        let mut payload_buf = Vec::with_capacity(4096);
        while !stop_for_task.load(Ordering::Relaxed) {
            match consumer.try_read(&mut payload_buf) {
                Ok(Some(rec)) if rec.msg_type == MSG_TYPE_QUERY => {
                    let (request_id, _kind) = decode_extra_query(rec.header_extra);
                    let (q, _) = bincode::serde::decode_from_slice::<S::Query, _>(
                        &payload_buf, bincode::config::standard()).expect("query decode");
                    let sm_guard = sm.lock();
                    let resp = sm_guard.query(q);
                    drop(sm_guard);
                    let resp_bytes = bincode::serde::encode_to_vec(&resp,
                        bincode::config::standard()).expect("encode");
                    loop {
                        match resp_producer.try_write(MSG_TYPE_QUERY_RESP, 0,
                            uc_protocol::frames::query::encode_extra_query(request_id,
                                uc_protocol::frames::query::QueryKind::Snapshot), &resp_bytes)
                        {
                            Ok(()) => break,
                            Err(uc_protocol::ring::RingError::Full) =>
                                tokio::task::yield_now().await,
                            Err(e) => panic!("query_resp write: {e}"),
                        }
                    }
                }
                Ok(Some(_)) | Ok(None) => tokio::time::sleep(
                    std::time::Duration::from_micros(200)).await,
                Err(e) => {
                    tracing::warn!(error = ?e, "query ring read");
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }
    });
    QueryLoopHandle { join, stop }
}
```

- [ ] **Step 3: `liveness.rs` — service heartbeat producer**

```rust
//! Service-side heartbeat: increments ServiceStatus.heartbeat_seq every 100ms.

use uc_protocol::cnc::ServiceStatus;
use uc_protocol::liveness::tick_service;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::task::JoinHandle;

pub struct LivenessHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn_liveness(status_ptr: *mut ServiceStatus) -> LivenessHandle {
    // SAFETY: caller guarantees `status_ptr` is valid for the lifetime of the task.
    // We capture the raw pointer in a Send wrapper. In practice the cnc mmap is
    // held in `Service` for the runtime lifetime.
    struct SafePtr(*mut ServiceStatus);
    unsafe impl Send for SafePtr {}
    let ptr = SafePtr(status_ptr);

    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = stop.clone();
    let join = tokio::spawn(async move {
        let SafePtr(p) = ptr;
        while !stop_for_task.load(Ordering::Relaxed) {
            let status = unsafe { &*p };
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
            tick_service(status, now_ns);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });
    LivenessHandle { join, stop }
}
```

- [ ] **Step 4: Finish `service.rs::run()`**

```rust
impl<S: StateMachine> ServiceBuilder<S> {
    pub async fn run(self) -> Result<(), ServiceError> {
        let attached = super::attach::attach(&self.config.instance_dir, &self.config.app_id)?;

        // Find pointers to status blocks.
        // SAFETY: cnc_mmap is valid; sub-buffer offsets validated at attach.
        let cnc_base = attached._cnc_mmap.as_ptr();
        let header = unsafe { &*(cnc_base as *const uc_protocol::cnc::CncHeader) };
        let service_status_offset = header.sub_buffer_offsets[uc_protocol::cnc::sub::SERVICE_STATUS];
        let service_status_ptr = unsafe {
            (cnc_base as *mut u8).add(service_status_offset as usize)
                as *mut uc_protocol::cnc::ServiceStatus
        };

        // Send ServiceReady to node, wait for ack via spawn.
        // For M3 simplicity, we set ready state and spawn loops immediately;
        // node-side handshake is asynchronous (node may apply backfill, etc).
        let last_applied = self.state_machine.last_applied().unwrap_or(0);

        // Wrap the state machine for sharing between apply + query loops.
        let sm_shared = std::sync::Arc::new(parking_lot::Mutex::new(self.state_machine));

        let apply_handle = super::apply_loop::spawn_apply_loop(
            sm_shared.clone(),
            attached.apply_consumer,
            attached.apply_resp_producer,
        );
        let query_handle = super::query_loop::spawn_query_loop(
            sm_shared.clone(),
            attached.query_consumer,
            attached.query_resp_producer,
        );
        let liveness_handle = super::liveness::spawn_liveness(service_status_ptr);

        super::handshake::set_service_state(
            unsafe { &*service_status_ptr },
            super::handshake::STATE_READY,
        );
        // Service ready notification via control_to_node is M3.x; for the in-process test harness,
        // setting service_status.state is sufficient.

        // Block on a shutdown signal — for M3 in-process tests, callers spawn `run` in
        // a tokio task and use cancellation. Keep `run` blocking on a never-resolving
        // future so it doesn't return early.
        let _ = tokio::signal::ctrl_c().await;

        // Shutdown.
        apply_handle.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        query_handle.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        liveness_handle.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = apply_handle.join.join();
        let _ = query_handle.join.await;
        let _ = liveness_handle.join.await;

        Ok(())
    }
}
```

For tests we'll need a non-blocking `run_until_cancelled(cancel_token)` variant. Add that.

- [ ] **Step 5: Commit**

```bash
cargo build -p uc_service
git add uc_service/src/runtime/
git commit -m "feat(uc_service): apply loop + query loop + heartbeat + Service::run"
```

---

## Task 12: `uc_node::ipc` — module skeleton + instance directory + cnc.dat owner

**Files:**
- Create: `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/ipc/mod.rs`
- Create: `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/ipc/instance.rs`
- Create: 3 stubs (`service_link.rs`, `liveness.rs`, `handshake.rs`)
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_node/Cargo.toml`
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/lib.rs`

- [ ] **Step 1: Cargo + lib.rs**

uc_node/Cargo.toml:
```toml
memmap2 = { workspace = true }
fs2 = { workspace = true }
parking_lot = { workspace = true }
```

uc_node/src/lib.rs:
```rust
pub mod ipc;
```

- [ ] **Step 2: `ipc/mod.rs`**

```rust
//! Inter-process communication layer between uc_node and uc_service.
//! Owns the instance directory and cnc.dat; creates the ring files.

pub mod handshake;
pub mod instance;
pub mod liveness;
pub mod service_link;

pub use instance::{Instance, IpcError};

use thiserror::Error;
```

- [ ] **Step 3: `ipc/instance.rs` — creates instance.lock + cnc.dat**

```rust
use std::path::{Path, PathBuf};
use memmap2::MmapMut;
use fs2::FileExt;
use uc_protocol::cnc::{init_cnc, cnc_file_size};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("instance already running at {0}")]
    AlreadyRunning(PathBuf),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("ring: {0}")]
    Ring(#[from] uc_protocol::ring::common::RingError),
}

pub struct Instance {
    pub instance_dir: PathBuf,
    pub _lock_file: std::fs::File,
    pub cnc_mmap: MmapMut,
    pub instance_id: u128,
}

impl Instance {
    pub fn create(instance_dir: &Path, app_id: &str, node_id: u64) -> Result<Self, IpcError> {
        std::fs::create_dir_all(instance_dir)?;
        std::fs::create_dir_all(instance_dir.join("service"))?;

        // Acquire instance.lock (exclusive flock).
        let lock_path = instance_dir.join("instance.lock");
        let lock_file = std::fs::OpenOptions::new()
            .read(true).write(true).create(true).truncate(false).open(&lock_path)?;
        lock_file.try_lock_exclusive()
            .map_err(|_| IpcError::AlreadyRunning(instance_dir.to_owned()))?;

        // Create cnc.dat.
        let cnc_path = instance_dir.join("cnc.dat");
        let cnc_file = std::fs::OpenOptions::new()
            .read(true).write(true).create(true).truncate(true).open(&cnc_path)?;
        let file_size = cnc_file_size();
        cnc_file.set_len(file_size as u64)?;
        let mut cnc_mmap = unsafe { MmapMut::map_mut(&cnc_file)? };

        let instance_id: u128 = rand::random();    // simple; M5 may use a more structured ID
        init_cnc(&mut cnc_mmap[..], app_id, node_id, instance_id)?;

        Ok(Instance {
            instance_dir: instance_dir.to_owned(),
            _lock_file: lock_file,
            cnc_mmap,
            instance_id,
        })
    }
}
```

Add `rand = "0.8"` to workspace + uc_node deps.

- [ ] **Step 4: Commit**

```bash
cargo build -p uc_node
git add uc_node/Cargo.toml uc_node/src/ipc/ Cargo.toml
git commit -m "feat(uc_node): ipc module + instance directory + cnc.dat creator"
```

---

## Task 13: `ipc::service_link` — create + own apply/query rings

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/ipc/service_link.rs`

- [ ] **Step 1: Implement**

```rust
//! Node-side ownership of the apply/query rings.
//!
//! Creates the four ring files under `{instance_dir}/service/`:
//!   apply.ring        SPSC, node→service
//!   apply_resp.ring   SPSC, service→node
//!   query.ring        SPSC, node→service
//!   query_resp.ring   SPSC, service→node

use std::path::Path;
use uc_protocol::ring::spsc::{SpscRing, SpscProducer, SpscConsumer};
use super::instance::IpcError;

pub struct ServiceLink {
    pub apply_producer: SpscProducer,
    pub apply_resp_consumer: SpscConsumer,
    pub query_producer: SpscProducer,
    pub query_resp_consumer: SpscConsumer,
}

const APPLY_RING_CAP: u64 = 64 * 1024 * 1024;
const APPLY_RING_MAX_MSG: u32 = 16 * 1024 * 1024;
const QUERY_RING_CAP: u64 = 16 * 1024 * 1024;
const QUERY_RING_MAX_MSG: u32 = 4 * 1024 * 1024;

impl ServiceLink {
    pub fn create(instance_dir: &Path) -> Result<Self, IpcError> {
        let service_dir = instance_dir.join("service");

        let apply = SpscRing::create(&service_dir.join("apply.ring"),
            APPLY_RING_CAP, APPLY_RING_MAX_MSG)?;
        let apply_resp = SpscRing::create(&service_dir.join("apply_resp.ring"),
            APPLY_RING_CAP, APPLY_RING_MAX_MSG)?;
        let query = SpscRing::create(&service_dir.join("query.ring"),
            QUERY_RING_CAP, QUERY_RING_MAX_MSG)?;
        let query_resp = SpscRing::create(&service_dir.join("query_resp.ring"),
            QUERY_RING_CAP, QUERY_RING_MAX_MSG)?;

        let (apply_producer, _) = apply.into_split();
        let (_, apply_resp_consumer) = apply_resp.into_split();
        let (query_producer, _) = query.into_split();
        let (_, query_resp_consumer) = query_resp.into_split();

        Ok(ServiceLink {
            apply_producer,
            apply_resp_consumer,
            query_producer,
            query_resp_consumer,
        })
    }
}
```

- [ ] **Step 2: Commit**

```bash
cargo build -p uc_node
git add uc_node/src/ipc/service_link.rs
git commit -m "feat(uc_node): service_link — creates apply/query SPSC rings"
```

---

## Task 14: Node-side heartbeat + service-handshake watcher

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/ipc/liveness.rs`
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/ipc/handshake.rs`

- [ ] **Step 1: `liveness.rs` — same pattern as service-side, but ticks NodeStatus**

(Implementation mirrors service-side liveness; just point at NodeStatus pointer.)

- [ ] **Step 2: `handshake.rs` — wait for service ready**

Reads `service_status.state` until it equals `STATE_READY` or times out.

```rust
use uc_protocol::cnc::ServiceStatus;
use std::sync::atomic::Ordering;
use std::time::Duration;

pub async fn wait_for_service_ready(
    status_ptr: *const ServiceStatus,
    timeout: Duration,
) -> Result<(), crate::ipc::IpcError> {
    let start = std::time::Instant::now();
    loop {
        let status = unsafe { &*status_ptr };
        if status.state.load(Ordering::Acquire) == 2 {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(crate::ipc::IpcError::Io(
                std::io::Error::new(std::io::ErrorKind::TimedOut, "service handshake")));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
```

- [ ] **Step 3: Commit**

```bash
cargo build -p uc_node
git add uc_node/src/ipc/{liveness,handshake}.rs
git commit -m "feat(uc_node): node-side heartbeat + service-ready handshake watcher"
```

---

## Task 15: Shmem-mode `AdaptedStateMachine`

Replace M1/M2's direct `g.sm.apply(...)` call with "publish to apply.ring + await response from apply_resp.ring." Keep the existing embedded variant; add a parallel `ShmemAdaptedStateMachine`.

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/raft/state_machine.rs`

- [ ] **Step 1: Add `ShmemAdaptedStateMachine`**

This impl satisfies `openraft::storage::RaftStateMachine<TypeConfig>` but the body of `apply()` does:
1. bincode-encode the openraft `Entry`'s payload bytes (which is already `S::Command` encoded — but openraft hands us `Entry<TypeConfig>` with `EntryPayload::Normal(Bytes)` — so the payload is the same bytes the user submitted).
2. SpscProducer::try_write into apply.ring with header_extra = log_index and the payload bytes.
3. SpscConsumer::try_read on apply_resp.ring (busy-wait or async with tokio sleep), match on header_extra's log_index.
4. Return the response bytes as `bytes::Bytes`.

Snapshot build/install methods stay in-process (call sm.build_snapshot / install_snapshot directly). M3 keeps the M2 path; M5 swaps to the snapshot.region mmap.

`query_snapshot` (NodeHandle method) on shmem mode: write to query.ring, await query_resp.ring. The closure-shortcut version of `query_snapshot` is **only available in Embedded mode**. Shmem mode requires the user's `Query` type (which the trait already supports).

Approx 200 lines. Adapted from the existing M2 `AdaptedStateMachine` but with publishes/awaits instead of direct calls.

- [ ] **Step 2: Commit**

```bash
cargo build -p uc_node
git add uc_node/src/raft/state_machine.rs
git commit -m "feat(uc_node): ShmemAdaptedStateMachine (apply via ring publish/await)"
```

---

## Task 16: `NodeConfig::ipc_mode` + dispatch in `NodeBuilder`

**Files:**
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/config.rs`
- Modify: `/Users/peter/Projects/ultima/ultima_cluster/uc_node/src/runtime/builder.rs`

- [ ] **Step 1: Add IpcMode enum**

```rust
#[derive(Debug, Clone)]
pub enum IpcMode {
    /// M1/M2 in-process: AdaptedStateMachine calls user's sm.apply() directly.
    Embedded,
    /// M3 shmem: AdaptedStateMachine publishes to apply.ring; user's sm runs
    /// in uc_service::Service (typically a separate tokio task or process).
    Shmem { instance_dir: PathBuf },
}

impl Default for IpcMode {
    fn default() -> Self { Self::Embedded }
}
```

Add field `pub ipc_mode: IpcMode` to NodeConfig.

- [ ] **Step 2: Dispatch in builder**

```rust
match self.config.ipc_mode.clone() {
    IpcMode::Embedded => {
        // Existing M2 path: AdaptedStateMachine::new(sm, handles)
        let handles = log_storage.handles(self.config.data_dir.clone());
        let sm_adapter = AdaptedStateMachine::new(self.state_machine, handles)?;
        // ... rest of M2 builder path ...
    }
    IpcMode::Shmem { instance_dir } => {
        // Create instance directory + cnc.dat + rings.
        let instance = crate::ipc::Instance::create(&instance_dir,
            &self.config.app_id, self.config.node_id)?;
        let service_link = crate::ipc::service_link::ServiceLink::create(&instance_dir)?;

        let handles = log_storage.handles(self.config.data_dir.clone());
        let sm_adapter = ShmemAdaptedStateMachine::new(
            self.state_machine,    // for snapshot build/install only
            handles,
            service_link.apply_producer,
            service_link.apply_resp_consumer,
        )?;

        // Spawn node-side heartbeat. Wait for service handshake (with timeout).
        // (Test harness spawns Service::run in parallel before calling start().)
        crate::ipc::handshake::wait_for_service_ready(
            ...,    // service_status_ptr from instance.cnc_mmap
            std::time::Duration::from_secs(30),
        ).await?;

        // ... rest of M2 builder path with sm_adapter ...
    }
}
```

- [ ] **Step 3: Commit**

```bash
cargo build --workspace
git add uc_node/src/config.rs uc_node/src/runtime/builder.rs
git commit -m "feat(uc_node): NodeConfig::ipc_mode dispatch (Embedded vs Shmem)"
```

---

## Task 17: First in-process shmem test

The first test that wires both halves together: spawn a Service in one tokio task, then start a NodeBuilder in Shmem mode. Submit a command, verify the response round-trips through the apply ring.

**Files:**
- Create: `/Users/peter/Projects/ultima/ultima_cluster/uc_node/tests/m3_shmem_single_node.rs`

The test pattern:
1. Create a tempdir for the instance.
2. Spawn `uc_service::ServiceBuilder::new(...).run()` in a tokio::spawn — this attaches to (not-yet-existing) cnc.dat. The service polls cnc.dat with retry.

Actually, the ordering is: node creates cnc.dat first, then service attaches. So either:
- Service.run polls for cnc.dat existence with retry.
- Test harness creates the node first, then spawns service.

For simplicity: test harness spawns NodeBuilder::start in tokio::spawn (returns a future); then spawns ServiceBuilder::run in another tokio::spawn; the node's `wait_for_service_ready` will block until the service comes up.

That's the natural ordering. Implement accordingly.

- [ ] **Step 1: Add a Counter state machine (re-use from m2_multi_node where possible by extracting to a test-helpers module, OR just duplicate).**

```rust
// Counter SM, similar to m2_multi_node.rs.
```

- [ ] **Step 2: Add the test**

```rust
#[tokio::test]
async fn shmem_single_node_submit_apply() {
    let instance_tempdir = TempDir::new().unwrap();
    let node_data_tempdir = TempDir::new().unwrap();
    let service_data_tempdir = TempDir::new().unwrap();

    let instance_dir = instance_tempdir.path().to_owned();

    // Spawn the node start in the background.
    let node_cfg = NodeConfig {
        node_id: 1,
        data_dir: node_data_tempdir.path().to_owned(),
        raft_listen_addr: "127.0.0.1:0".parse().unwrap(),
        app_id: "m3-shmem".into(),
        bootstrap: BootstrapConfig::SingleNode,
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        ipc_mode: IpcMode::Shmem { instance_dir: instance_dir.clone() },
    };
    let node_task = tokio::spawn(async move {
        NodeBuilder::new(node_cfg, Counter::default())   // Counter unused in shmem mode
            .start().await
    });

    // Give the node a moment to create cnc.dat.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Spawn the service.
    let svc_cfg = uc_service::runtime::ServiceConfig {
        instance_dir: instance_dir.clone(),
        app_id: "m3-shmem".into(),
        data_dir: service_data_tempdir.path().to_owned(),
        ..Default::default()
    };
    let svc_task = tokio::spawn(async move {
        ServiceBuilder::new(svc_cfg, Counter::default())
            .run().await
    });

    // Wait for the node start to complete.
    let handle = tokio::time::timeout(Duration::from_secs(10), node_task)
        .await.expect("node start timeout").expect("node task panic").expect("node start");

    // Submit + verify.
    let resp = handle.submit(Cmd::Inc(5)).await.expect("submit");
    assert_eq!(resp.value, 5);

    // Shutdown.
    handle.shutdown().await.expect("shutdown");
    drop(svc_task);
}
```

Tricky bit: the node's `IpcMode::Shmem` path uses `self.state_machine` only for snapshot build/install (since apply goes through the ring). For M3 we still pass a Counter — it's used purely as a snapshot codec.

- [ ] **Step 3: Run and iterate**

```bash
cargo test -p uc_node --test m3_shmem_single_node shmem_single_node_submit_apply
```

Likely needs several iteration rounds to debug timing / ring blocking issues.

- [ ] **Step 4: Commit**

```bash
git add uc_node/tests/m3_shmem_single_node.rs
git commit -m "test(uc_node): shmem single-node submit_apply (first end-to-end shmem test)"
```

---

## Task 18: Query roundtrip test

Similar to Task 17 but exercises the query path. Builds confidence that the query ring + tokio query_loop work.

(Test code follows the same pattern.)

```bash
git add uc_node/tests/m3_shmem_single_node.rs
git commit -m "test(uc_node): shmem query roundtrip"
```

---

## Task 19: 3-node shmem cluster test

Three nodes, each with its own Service task in the same tokio runtime. Demonstrates that QUIC + shmem coexist correctly under multi-node load.

```rust
#[tokio::test]
async fn three_node_shmem_cluster() {
    // For each of 3 nodes: tempdir, spawn node+service pair.
    // Then run the same replication test as m2_multi_node::three_node_replication
    // but with IpcMode::Shmem.
}
```

```bash
git commit -m "test(uc_node): 3-node shmem cluster replication"
```

---

## Task 20: Service-crash + leadership-transfer test

Kill the service task on the leader. Verify:
1. Node detects via missed heartbeats.
2. If leader, calls `Raft::trigger_leader_transfer`.
3. Cluster continues operating; new leader's submit succeeds.

```bash
git commit -m "test(uc_node): service crash triggers leadership transfer"
```

---

## Task 21-22: ultima_db adapter end-to-end test + polish

- Test using StoreStateMachine instead of a hand-rolled Counter.
- clippy/fmt/README update.

```bash
git commit -m "test(uc_service): ultima_db adapter end-to-end via shmem"
git commit -m "style: cargo fmt across workspace"
git commit -m "chore(m3): clippy/fmt clean + README pointer"
```

---

## Verification checklist

After all tasks complete:

- `cargo build --workspace` — clean.
- `cargo test --workspace` — all tests pass (M1's 11 + M2's 18 + M3's 5-7 = ~35).
- `cargo clippy --workspace --all-targets -- -D warnings` — zero warnings.
- `cargo doc --workspace --no-deps` — docs build.
- M3 capstone tests pass:
  - `shmem_single_node_submit_apply`
  - `shmem_query_roundtrip`
  - `three_node_shmem_cluster`
  - `service_crash_leadership_transfer`
  - `ultima_db_adapter_end_to_end`

---

## Self-review notes

**Spec coverage:**
- Sections 3-4 (process model + cnc.dat layout): Tasks 1-7 implement the protocol primitives; Task 12 owns the instance directory + cnc.dat.
- Section 5 (public APIs): Tasks 9-11 implement uc_service::ServiceBuilder + runtime; Task 8 implements uc_service::ultima_db; Tasks 15-16 add ShmemAdaptedStateMachine + IpcMode dispatch.
- Section 8 (pipelines): apply path (Tasks 11, 15) + query path (Tasks 11, 17-18).
- Section 9 (snapshot transfer): M3 keeps the M2 path (Cursor<Vec<u8>>); the snapshot.region mmap is deferred to M5 per the design spec's explicit milestone breakdown.
- Section 10 (bootstrap + recovery): Task 14 implements the service-ready handshake; service-crash leadership transfer is Task 20.

**Known M3 simplifications:**
- snapshot.region mmap deferred to M5 (snapshot bytes still flow as `Cursor<Vec<u8>>` via openraft's existing SnapshotData path).
- clients/*.ring deferred to M4 (no uc_client real impl yet).
- output.ring + at-least-once OutputHandler dispatch deferred to M5.
- cnc.dat counters / error_log sub-buffers deferred to M5.
- Multi-process subprocess tests deferred to M3.x (in-process tokio-task tests prove the shmem protocol).
- Service runs as a tokio task in the same process for tests; the protocol works identically when run as a separate process (M3.x).

**Forward-compat concerns:**
- The `NodeHandle::query_snapshot(closure)` API only works in Embedded mode. Shmem mode requires typed `Query`/`QueryResponse`. Document this in the M3 task02 doc as a known asymmetry.
- The apply ring's busy-spin (`std::thread::yield_now()` on Full) under heavy load can starve other threads. Acceptable for M3; M5 perf may tune this.
- The shmem rings hold raw pointers via `Arc<Inner>`. The mmap is owned inside Inner; cloning is cheap; but the `unsafe` boundary is wide. M3.x could harden this with `Pin<Box<_>>` or a stricter ownership model.

**Plan size:** ~22 tasks. Roughly comparable to M2 (17 tasks) but with bigger per-task work in Tasks 1-4 (ring buffer primitives). Expect ~6000-7000 line plan.
