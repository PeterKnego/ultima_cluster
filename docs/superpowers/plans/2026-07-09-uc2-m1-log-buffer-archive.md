# UC v2 M1 — `uc_log` Log Buffer + Archive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build UC v2's core data structure — the mmap'd single-writer log buffer with position counters — plus the archive agent that records it to `uc_journal` in fsync'd blocks, hitting the M1 gate: ≥1 M msgs/s (64 B payloads) append+record+fsync on one node.

**Architecture:** Per spec `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md` §4. Frame layout lives in `uc_protocol::v2` (core-only). New crate `uc_log` provides: `Region` (heap- or mmap-backed raw memory), `LogBuffer`+`Appender` (single-writer ring addressed by absolute u64 positions, atomic-after-write length commit, padding frames at wrap, one hard overrun gate against the durable counter), `Archive` (block-records the buffer into `uc_journal`: one journal record per ≤1 MiB frame-aligned block, `seq`=block index, `meta`=base position, one fsync per block, durable counter advances after; replay-from-position via binary search), and `agent` (IdleStrategy + AgentRunner threads).

**Tech Stack:** Rust edition 2024, `uc_journal` (in-tree), `memmap2`, `thiserror`; dev: `tempfile`, `loom`.

## Global Constraints

- `cargo clippy --workspace -- -D warnings` must pass at every commit (repo rule).
- `uc_protocol::v2::frame` must stay `core`-only: no `std` imports, no atomics — the atomic commit-word store/load lives in `uc_log`.
- Frame alignment 32 B; header 32 B; `length` field = total frame length (header+payload); `length == 0` means uncommitted (spec §4).
- Positions are absolute `u64` byte offsets, monotonic forever; ring offset = `position & (capacity-1)`.
- Buffer capacity: power of two, multiple of 32, ≤ 2^31 (length field is u32), default 512 MiB.
- Overrun rule (spec §4): the appender may NEVER overwrite bytes the archive hasn't recorded — gate against the `durable` counter; all other readers degrade (validated reads may return `Overrun`).
- Archive: `Durability::Consistent`, `preallocate_segments: true`, block ≤ 1 MiB and frame-aligned, `meta` = block base position.
- No timer-based batching anywhere: the archive records whatever accumulated per poll (structural batching).
- SPDX header on every new file: `// SPDX-License-Identifier: Apache-2.0` + `// Copyright 2026 Peter Knego` (match existing files).
- Commit after every task; commit messages `feat(uc_log): ...` / `feat(uc_protocol): ...`.

---

### Task 1: `uc_protocol::v2::frame` — frame layout (core-only)

**Files:**
- Create: `uc_protocol/src/v2/mod.rs`
- Create: `uc_protocol/src/v2/frame.rs`
- Modify: `uc_protocol/src/lib.rs` (add `pub mod v2;` after the existing `pub mod` list)

**Interfaces:**
- Consumes: nothing.
- Produces (used by Tasks 3–7):
  - `const FRAME_ALIGNMENT: usize = 32`, `const HEADER_LEN: usize = 32`
  - `const OFF_LENGTH: usize = 0` (u32 LE commit word)
  - `const FRAME_TYPE_MESSAGE: u8 = 1`, `const FRAME_TYPE_PADDING: u8 = 2`
  - `struct FrameHeader { length: u32, frame_type: u8, flags: u8, leadership_term_id: u32, session_id: u64, correlation_id: u64 }`
  - `const fn align_frame_len(total: usize) -> usize`
  - `fn write_header_except_length(buf: &mut [u8], h: &FrameHeader)`
  - `fn read_header(buf: &[u8]) -> FrameHeader` (reads length non-atomically — only call after an acquire load observed it non-zero)

- [ ] **Step 1: Write the failing tests**

Create `uc_protocol/src/v2/frame.rs` with the test module only (code in Step 3 goes above it):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alignment_math() {
        assert_eq!(align_frame_len(32), 32);
        assert_eq!(align_frame_len(33), 64);
        assert_eq!(align_frame_len(96), 96);
        assert_eq!(align_frame_len(97), 128);
        // 64 B payload + 32 B header = 96 B on the wire (spec §4 / anatomy doc)
        assert_eq!(align_frame_len(HEADER_LEN + 64), 96);
    }

    #[test]
    fn header_roundtrip_except_length() {
        let h = FrameHeader {
            length: 0, // not written by write_header_except_length
            frame_type: FRAME_TYPE_MESSAGE,
            flags: 0x5a,
            leadership_term_id: 7,
            session_id: 0x1122_3344_5566_7788,
            correlation_id: 42,
        };
        let mut buf = [0u8; HEADER_LEN];
        write_header_except_length(&mut buf, &h);
        // length bytes untouched (commit word is written atomically elsewhere, last)
        assert_eq!(&buf[OFF_LENGTH..OFF_LENGTH + 4], &[0, 0, 0, 0]);
        // simulate the runtime's commit-word store
        buf[OFF_LENGTH..OFF_LENGTH + 4].copy_from_slice(&(HEADER_LEN as u32 + 64).to_le_bytes());
        let out = read_header(&buf);
        assert_eq!(out.length, HEADER_LEN as u32 + 64);
        assert_eq!(out.frame_type, FRAME_TYPE_MESSAGE);
        assert_eq!(out.flags, 0x5a);
        assert_eq!(out.leadership_term_id, 7);
        assert_eq!(out.session_id, 0x1122_3344_5566_7788);
        assert_eq!(out.correlation_id, 42);
    }

    #[test]
    fn field_offsets_do_not_overlap() {
        // layout: length(4) type(1) flags(1) rsvd(2) term(4) rsvd(4) session(8) correlation(8) = 32
        assert_eq!(OFF_LENGTH, 0);
        assert_eq!(OFF_TYPE, 4);
        assert_eq!(OFF_FLAGS, 5);
        assert_eq!(OFF_TERM_ID, 8);
        assert_eq!(OFF_SESSION_ID, 16);
        assert_eq!(OFF_CORRELATION_ID, 24);
        assert_eq!(HEADER_LEN, 32);
        assert_eq!(FRAME_ALIGNMENT, 32);
    }
}
```

Create `uc_protocol/src/v2/mod.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UC v2 protocol layouts (spec: docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md).
//! Core-only modules — the multi-language gate for protocol v2.

pub mod frame;
```

Add to `uc_protocol/src/lib.rs` after `pub mod snapshot_region;`:

```rust
pub mod v2;
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p uc_protocol v2::frame -- --nocapture`
Expected: COMPILE ERROR (`align_frame_len`, `FrameHeader`, … not defined).

- [ ] **Step 3: Write the implementation**

Top of `uc_protocol/src/v2/frame.rs` (above the test module):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Log-buffer frame layout (spec §4). Core-only: layout constants and
//! (de)serialization over byte slices. The `length` field at offset 0 is the
//! commit word: written LAST with a release store, read with an acquire load,
//! `0` = frame not yet committed. Those atomic ops live in the runtime crate
//! (`uc_log`) — this module never touches atomics so it stays `core`-only.

/// Every frame starts on a 32-byte boundary; frame slots are padded up to it.
pub const FRAME_ALIGNMENT: usize = 32;
/// Fixed header size; payload follows immediately.
pub const HEADER_LEN: usize = 32;

pub const OFF_LENGTH: usize = 0; // u32 LE — TOTAL frame length (header + payload); 0 = uncommitted
pub const OFF_TYPE: usize = 4; // u8
pub const OFF_FLAGS: usize = 5; // u8
pub const OFF_RESERVED0: usize = 6; // u16 — reserved, written as zero
pub const OFF_TERM_ID: usize = 8; // u32 LE — leadership_term_id
pub const OFF_RESERVED1: usize = 12; // u32 — reserved, written as zero
pub const OFF_SESSION_ID: usize = 16; // u64 LE
pub const OFF_CORRELATION_ID: usize = 24; // u64 LE

/// Application message; payload = user command bytes.
pub const FRAME_TYPE_MESSAGE: u8 = 1;
/// Wrap padding: `length` spans to the end of the buffer; ONLY the 32-byte
/// header is actually written — the rest of the padded region is stale bytes.
/// Readers and the archive skip it by `length`; replay drops it.
pub const FRAME_TYPE_PADDING: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub length: u32,
    pub frame_type: u8,
    pub flags: u8,
    pub leadership_term_id: u32,
    pub session_id: u64,
    pub correlation_id: u64,
}

/// Round a total frame length up to the 32-byte slot size.
#[inline]
pub const fn align_frame_len(total: usize) -> usize {
    (total + FRAME_ALIGNMENT - 1) & !(FRAME_ALIGNMENT - 1)
}

/// Write every header field EXCEPT `length` (the commit word — the runtime
/// stores it atomically, last). `buf` must be at least `HEADER_LEN` bytes.
pub fn write_header_except_length(buf: &mut [u8], h: &FrameHeader) {
    buf[OFF_TYPE] = h.frame_type;
    buf[OFF_FLAGS] = h.flags;
    buf[OFF_RESERVED0..OFF_RESERVED0 + 2].copy_from_slice(&0u16.to_le_bytes());
    buf[OFF_TERM_ID..OFF_TERM_ID + 4].copy_from_slice(&h.leadership_term_id.to_le_bytes());
    buf[OFF_RESERVED1..OFF_RESERVED1 + 4].copy_from_slice(&0u32.to_le_bytes());
    buf[OFF_SESSION_ID..OFF_SESSION_ID + 8].copy_from_slice(&h.session_id.to_le_bytes());
    buf[OFF_CORRELATION_ID..OFF_CORRELATION_ID + 8].copy_from_slice(&h.correlation_id.to_le_bytes());
}

/// Parse a header from a committed frame. The caller must already have
/// observed `length != 0` via an acquire load (or hold the buffer's
/// single-writer/contiguity guarantees); this function does plain reads.
pub fn read_header(buf: &[u8]) -> FrameHeader {
    FrameHeader {
        length: u32::from_le_bytes(buf[OFF_LENGTH..OFF_LENGTH + 4].try_into().unwrap()),
        frame_type: buf[OFF_TYPE],
        flags: buf[OFF_FLAGS],
        leadership_term_id: u32::from_le_bytes(buf[OFF_TERM_ID..OFF_TERM_ID + 4].try_into().unwrap()),
        session_id: u64::from_le_bytes(buf[OFF_SESSION_ID..OFF_SESSION_ID + 8].try_into().unwrap()),
        correlation_id: u64::from_le_bytes(
            buf[OFF_CORRELATION_ID..OFF_CORRELATION_ID + 8].try_into().unwrap(),
        ),
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p uc_protocol v2::frame`
Expected: 3 passed.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p uc_protocol -- -D warnings
git add uc_protocol/src/v2 uc_protocol/src/lib.rs
git commit -m "feat(uc_protocol): v2 frame layout — 32B header, commit-word length, padding type"
```

---

### Task 2: `uc_log` crate scaffold — counters + region

**Files:**
- Modify: `Cargo.toml` (workspace `members`: add `"uc_log"` after `"uc_protocol"`)
- Create: `uc_log/Cargo.toml`
- Create: `uc_log/src/lib.rs`
- Create: `uc_log/src/counters.rs`
- Create: `uc_log/src/region.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces (used by Tasks 3–9):
  - `counters::PaddedAtomicU64` with `new(u64)`, `load_acquire() -> u64`, `store_release(u64)`
  - `counters::LogCounters { pub append: PaddedAtomicU64, pub durable: PaddedAtomicU64 }` with `new() -> Self` (zeros) and `prime(&self, pos: u64)` (stores both)
  - `region::Region` with `heap_zeroed(len: usize) -> Region`, `len(&self) -> usize`, `is_empty(&self) -> bool`, `unsafe fn ptr_at(&self, off: usize) -> *mut u8`; `Send + Sync`

- [ ] **Step 1: Create the crate and workspace wiring**

`uc_log/Cargo.toml`:

```toml
[package]
name = "uc_log"
description = "UC v2 log buffer + archive (spec 2026-07-09, M1)"
edition.workspace = true
version.workspace = true
license.workspace = true
authors.workspace = true

[dependencies]
uc_protocol = { path = "../uc_protocol" }
uc_journal = { workspace = true }
memmap2 = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }

[target.'cfg(loom)'.dev-dependencies]
loom = "0.7"
```

In the root `Cargo.toml`, change the members line to include `"uc_log"`:

```toml
members = ["uc_protocol", "uc_log", "uc_service", "uc_node", "uc_client", "uc_autobench", "uc_lincheck", "uc-rt-busyspin", "uc_journal", "examples/counter_loop", "examples/uc-crashtest"]
```

`uc_log/src/lib.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UC v2 log buffer + archive (M1).
//! Spec: docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md §4.

pub mod counters;
pub mod region;
```

- [ ] **Step 2: Write the failing tests**

Test module at the bottom of `uc_log/src/counters.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero_and_prime() {
        let c = LogCounters::new();
        assert_eq!(c.append.load_acquire(), 0);
        assert_eq!(c.durable.load_acquire(), 0);
        c.prime(4096);
        assert_eq!(c.append.load_acquire(), 4096);
        assert_eq!(c.durable.load_acquire(), 4096);
    }

    #[test]
    fn padded_is_a_full_cache_line() {
        assert_eq!(std::mem::size_of::<PaddedAtomicU64>(), 64);
        assert_eq!(std::mem::align_of::<PaddedAtomicU64>(), 64);
    }
}
```

Test module at the bottom of `uc_log/src/region.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_region_is_zeroed_and_writable() {
        let r = Region::heap_zeroed(4096);
        assert_eq!(r.len(), 4096);
        unsafe {
            assert_eq!(*r.ptr_at(0), 0);
            assert_eq!(*r.ptr_at(4095), 0);
            *r.ptr_at(17) = 0xab;
            assert_eq!(*r.ptr_at(17), 0xab);
        }
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p uc_log`
Expected: COMPILE ERROR (types not defined).

- [ ] **Step 4: Write the implementations**

`uc_log/src/counters.rs` (above tests):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Position counters (spec §4). One writer per counter, many readers, all
//! coordination is release/acquire on these — no locks, no wakeups.
//! `repr(C)` + fixed layout: these will be placed into the mmap'd cnc v2
//! page when protocol v2 IPC lands (M5); until then they live on the heap.

use std::sync::atomic::{AtomicU64, Ordering};

/// A cache-line-isolated atomic u64 (prevents false sharing between counters).
#[repr(C, align(64))]
pub struct PaddedAtomicU64 {
    v: AtomicU64,
    _pad: [u8; 56],
}

impl PaddedAtomicU64 {
    pub fn new(v: u64) -> Self {
        Self { v: AtomicU64::new(v), _pad: [0; 56] }
    }
    #[inline]
    pub fn load_acquire(&self) -> u64 {
        self.v.load(Ordering::Acquire)
    }
    #[inline]
    pub fn store_release(&self, v: u64) {
        self.v.store(v, Ordering::Release)
    }
}

/// The M1 counter set. append: written only by the appender, after the frame
/// commit word (so any position below `append` is a committed frame).
/// durable: written only by the archive, after write+fdatasync of the block.
#[repr(C)]
pub struct LogCounters {
    pub append: PaddedAtomicU64,
    pub durable: PaddedAtomicU64,
}

impl LogCounters {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self { append: PaddedAtomicU64::new(0), durable: PaddedAtomicU64::new(0) }
    }
    /// Prime both counters after archive recovery (append resumes at durable —
    /// bytes beyond durable are discarded on restart, spec §6).
    pub fn prime(&self, pos: u64) {
        self.durable.store_release(pos);
        self.append.store_release(pos);
    }
}
```

`uc_log/src/region.rs` (above tests):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Raw memory region backing the log buffer: heap for unit tests (miri-clean)
//! and mmap'd files for real instances (Task 5 adds `from_mmap`).
//!
//! Safety model: the region hands out raw pointers only; no `&`/`&mut`
//! references to buffer bytes are ever held across threads. All cross-thread
//! ordering goes through the frame commit word and the position counters
//! (release/acquire). Concurrent writers/readers never touch the same bytes:
//! the appender's overrun gate (vs `durable`) and readers' bounds (vs
//! `append`) partition the address space by position.

use std::alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error};
use std::ptr::NonNull;

enum Backing {
    Heap(Layout),
}

pub struct Region {
    ptr: NonNull<u8>,
    len: usize,
    backing: Backing,
}

// SAFETY: raw memory; synchronization is the caller's protocol (see module doc).
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

impl Region {
    /// Heap-backed zeroed region (unit tests / miri).
    pub fn heap_zeroed(len: usize) -> Self {
        assert!(len > 0);
        let layout = Layout::from_size_align(len, 64).expect("region layout");
        // SAFETY: len > 0, valid layout.
        let raw = unsafe { alloc_zeroed(layout) };
        let Some(ptr) = NonNull::new(raw) else { handle_alloc_error(layout) };
        Self { ptr, len, backing: Backing::Heap(layout) }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// # Safety
    /// `off < self.len()`; the caller upholds the module-level aliasing
    /// protocol (position-partitioned access, atomics for cross-thread order).
    #[inline]
    pub unsafe fn ptr_at(&self, off: usize) -> *mut u8 {
        debug_assert!(off < self.len);
        // SAFETY: off < len per contract.
        unsafe { self.ptr.as_ptr().add(off) }
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        match &self.backing {
            // SAFETY: allocated with this exact layout in heap_zeroed.
            Backing::Heap(layout) => unsafe { dealloc(self.ptr.as_ptr(), *layout) },
        }
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p uc_log`
Expected: 3 passed.

- [ ] **Step 6: Lint and commit**

```bash
cargo clippy -p uc_log -- -D warnings
git add Cargo.toml uc_log
git commit -m "feat(uc_log): crate scaffold — padded position counters + raw region"
```

---

### Task 3: `LogBuffer` + `Appender` — single-writer append, padding, overrun gate, recordable slices

**Files:**
- Create: `uc_log/src/buffer.rs`
- Modify: `uc_log/src/lib.rs` (add `pub mod buffer;`)

**Interfaces:**
- Consumes: `Region`, `LogCounters` (Task 2); `uc_protocol::v2::frame` (Task 1).
- Produces (used by Tasks 4–9):
  - `LogBuffer::new(region: Region, counters: Arc<LogCounters>, max_payload: usize) -> LogBuffer`
  - `LogBuffer::capacity(&self) -> u64`, `LogBuffer::counters(&self) -> &Arc<LogCounters>`
  - `LogBuffer::recordable_slice(&self, from: u64, max_bytes: usize) -> &[u8]` — contiguous committed whole frames `[from, append)`, never wrapping, ≥1 frame if any; FOR THE ARCHIVE ONLY (the gate holder)
  - `Appender::new(buffer: Arc<LogBuffer>, leadership_term_id: u32) -> Appender`
  - `Appender::append(&mut self, session_id: u64, correlation_id: u64, payload: &[u8]) -> Result<u64, AppendError>` returning the frame's position
  - `Appender::position(&self) -> u64`
  - `enum AppendError { WouldOverrun, PayloadTooLarge }`

- [ ] **Step 1: Write the failing tests**

Test module at the bottom of `uc_log/src/buffer.rs`. Tests use a small buffer (4096 B) so wrap and gate paths are cheap to hit, and drive the durable counter by hand (no archive yet):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::counters::LogCounters;
    use crate::region::Region;
    use std::sync::Arc;
    use uc_protocol::v2::frame::{
        FRAME_TYPE_MESSAGE, FRAME_TYPE_PADDING, HEADER_LEN, read_header,
    };

    const CAP: u64 = 4096;

    fn buf() -> (Arc<LogBuffer>, Arc<LogCounters>) {
        let counters = Arc::new(LogCounters::new());
        let b = Arc::new(LogBuffer::new(
            Region::heap_zeroed(CAP as usize),
            Arc::clone(&counters),
            256, // max_payload for tests
        ));
        (b, counters)
    }

    #[test]
    fn append_then_recordable_roundtrip() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 3);
        let pos = a.append(11, 42, b"hello world!").unwrap();
        assert_eq!(pos, 0);
        // 32 header + 12 payload = 44 -> aligned 64
        assert_eq!(a.position(), 64);
        assert_eq!(c.append.load_acquire(), 64);

        let s = b.recordable_slice(0, 1 << 20);
        assert_eq!(s.len(), 64);
        let h = read_header(s);
        assert_eq!(h.length, (HEADER_LEN + 12) as u32);
        assert_eq!(h.frame_type, FRAME_TYPE_MESSAGE);
        assert_eq!(h.leadership_term_id, 3);
        assert_eq!(h.session_id, 11);
        assert_eq!(h.correlation_id, 42);
        assert_eq!(&s[HEADER_LEN..HEADER_LEN + 12], b"hello world!");
    }

    #[test]
    fn recordable_slice_is_bounded_and_frame_aligned() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..4 {
            a.append(1, i, &[0u8; 64]).unwrap(); // 96 B frames
        }
        // max_bytes cuts mid-frame at 200 -> trimmed to 2 whole frames (192)
        let s = b.recordable_slice(0, 200);
        assert_eq!(s.len(), 192);
        // always returns at least one whole frame even if max_bytes is tiny
        let s = b.recordable_slice(0, 8);
        assert_eq!(s.len(), 96);
        // empty when caught up
        assert_eq!(b.recordable_slice(4 * 96, 1 << 20).len(), 0);
    }

    #[test]
    fn wrap_emits_padding_and_slice_stops_at_wrap() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        // fill to 4032: 42 frames of 96 B
        for i in 0..42 {
            a.append(1, i, &[0u8; 64]).unwrap();
        }
        assert_eq!(a.position(), 4032);
        // next 96 B frame doesn't fit in the remaining 64 -> 64 B padding + frame at 4096
        c.durable.store_release(4032); // let the gate breathe
        let pos = a.append(1, 99, &[0u8; 64]).unwrap();
        assert_eq!(pos, 4096);
        assert_eq!(a.position(), 4192);

        // slice from 4032 stops at the wrap: just the 64 B padding frame
        c.durable.store_release(4032);
        let s = b.recordable_slice(4032, 1 << 20);
        assert_eq!(s.len(), 64);
        let h = read_header(s);
        assert_eq!(h.frame_type, FRAME_TYPE_PADDING);
        assert_eq!(h.length, 64);
        // and the next slice (post-wrap) starts with the message frame
        let s = b.recordable_slice(4096, 1 << 20);
        assert_eq!(s.len(), 96);
        assert_eq!(read_header(s).correlation_id, 99);
    }

    #[test]
    fn overrun_gate_blocks_until_durable_advances() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        // durable stays 0: we can fill exactly one capacity, no more
        for i in 0..42 {
            a.append(1, i, &[0u8; 64]).unwrap();
        }
        // 4032 used; next append needs padding(64) + frame(96) -> end 4192 > 0 + 4096
        assert_eq!(a.append(1, 500, &[0u8; 64]).unwrap_err(), AppendError::WouldOverrun);
        // archive "records" one frame -> gate opens exactly enough
        c.durable.store_release(96);
        assert_eq!(a.append(1, 500, &[0u8; 64]).unwrap(), 4096);
        // and closes again
        assert_eq!(a.append(1, 501, &[0u8; 64]).unwrap_err(), AppendError::WouldOverrun);
    }

    #[test]
    fn payload_too_large_is_rejected() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        assert_eq!(a.append(1, 1, &[0u8; 257]).unwrap_err(), AppendError::PayloadTooLarge);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p uc_log buffer`
Expected: COMPILE ERROR.

- [ ] **Step 3: Write the implementation**

`uc_log/src/buffer.rs` (above tests):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The log buffer (spec §4): one mmap'd/heap ring per node, addressed by
//! absolute u64 positions, exactly one writer (role-determined), readers
//! coordinated by position counters.
//!
//! Commit protocol: payload + header fields are plain writes; the u32 length
//! word at the frame's offset is stored LAST with `Release`. The `append`
//! counter is stored `Release` after the commit word, so any reader that
//! bounds itself by an acquire-load of `append` sees only whole committed
//! frames. Padding frames write ONLY their 32-byte header.
//!
//! Overrun rule: the appender never claims past `durable + capacity` — the
//! single hard gate (the archive is the only reader the ring can never drop).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use uc_protocol::v2::frame::{
    self, FRAME_TYPE_MESSAGE, FRAME_TYPE_PADDING, FrameHeader, HEADER_LEN, align_frame_len,
};

use crate::counters::LogCounters;
use crate::region::Region;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AppendError {
    /// Claim would overwrite bytes the archive hasn't recorded. Retry after
    /// the durable counter advances (surfaced as admission backpressure).
    #[error("append would overrun unrecorded bytes")]
    WouldOverrun,
    #[error("payload exceeds max_payload")]
    PayloadTooLarge,
}

pub struct LogBuffer {
    region: Region,
    capacity: u64,
    mask: u64,
    max_payload: usize,
    counters: Arc<LogCounters>,
}

impl LogBuffer {
    pub fn new(region: Region, counters: Arc<LogCounters>, max_payload: usize) -> Self {
        let capacity = region.len() as u64;
        assert!(capacity.is_power_of_two(), "capacity must be a power of two");
        assert!(capacity <= 1 << 31, "length commit word is u32");
        let max_claim = 2 * align_frame_len(HEADER_LEN + max_payload) as u64;
        assert!(
            capacity >= 4 * max_claim,
            "capacity too small for max_payload (need >= 4x max claim)"
        );
        Self { region, capacity, mask: capacity - 1, max_payload, counters }
    }

    #[inline]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    #[inline]
    pub fn counters(&self) -> &Arc<LogCounters> {
        &self.counters
    }

    #[inline]
    pub fn max_payload(&self) -> usize {
        self.max_payload
    }

    /// Worst-case single-append write footprint span: padding (< one aligned
    /// max frame, since padding is only emitted when the frame doesn't fit
    /// the space before the wrap) + the frame itself. Used by validated
    /// readers (Task 4) as their safety margin.
    #[inline]
    pub(crate) fn max_claim(&self) -> u64 {
        2 * align_frame_len(HEADER_LEN + self.max_payload) as u64
    }

    #[inline]
    fn offset(&self, pos: u64) -> usize {
        (pos & self.mask) as usize
    }

    /// The u32 commit word at a frame offset. Offsets are 32-aligned, which
    /// satisfies AtomicU32's alignment.
    #[inline]
    pub(crate) fn commit_word(&self, off: usize) -> &AtomicU32 {
        debug_assert_eq!(off % frame::FRAME_ALIGNMENT, 0);
        // SAFETY: off < capacity (masked), 4-byte aligned, points into the
        // region for its whole lifetime; concurrent access only via atomics.
        unsafe { AtomicU32::from_ptr(self.region.ptr_at(off).cast::<u32>()) }
    }

    /// Contiguous committed whole frames starting at `from`, bounded by the
    /// append counter, the wrap point, and (softly) `max_bytes` — the result
    /// contains at least one whole frame if any is available, and never cuts
    /// a frame in half. CONTRACT: only the archive (the durability gate
    /// holder) may call this; the returned slice is protected from overwrite
    /// by the appender's gate against `durable`.
    pub fn recordable_slice(&self, from: u64, max_bytes: usize) -> &[u8] {
        let append = self.counters.append.load_acquire();
        if append <= from {
            return &[];
        }
        let off = self.offset(from);
        let hard = (append - from).min(self.capacity - off as u64);
        // Frame-walk to trim to whole frames within max_bytes (>= 1 frame).
        // Everything in [from, append) is committed, so plain length reads
        // are safe (the acquire on `append` ordered them).
        let mut end = 0u64;
        while end < hard {
            let len = u32::from_le_bytes(
                // SAFETY: off+end within capacity (end < hard <= capacity-off).
                unsafe {
                    std::slice::from_raw_parts(self.region.ptr_at(off + end as usize), 4)
                }
                .try_into()
                .unwrap(),
            );
            let aligned = align_frame_len(len as usize) as u64;
            debug_assert!(aligned > 0 && end + aligned <= hard);
            if end > 0 && end + aligned > max_bytes as u64 {
                break;
            }
            end += aligned;
        }
        // SAFETY: [from, from+end) is committed, contiguous in the region,
        // and gate-protected from overwrite while the archive holds it.
        unsafe { std::slice::from_raw_parts(self.region.ptr_at(off), end as usize) }
    }
}

/// The single writer. On the leader this is driven by the consensus agent;
/// M1 drives it directly. NOT Sync — exactly one appender per buffer.
pub struct Appender {
    buffer: Arc<LogBuffer>,
    pos: u64,
    cached_durable: u64,
    leadership_term_id: u32,
}

impl Appender {
    pub fn new(buffer: Arc<LogBuffer>, leadership_term_id: u32) -> Self {
        let pos = buffer.counters.append.load_acquire();
        let cached_durable = buffer.counters.durable.load_acquire();
        Self { buffer, pos, cached_durable, leadership_term_id }
    }

    #[inline]
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Append one message frame; returns its position. `WouldOverrun` is
    /// retryable (backpressure), `PayloadTooLarge` is not.
    pub fn append(
        &mut self,
        session_id: u64,
        correlation_id: u64,
        payload: &[u8],
    ) -> Result<u64, AppendError> {
        if payload.len() > self.buffer.max_payload {
            return Err(AppendError::PayloadTooLarge);
        }
        let total = HEADER_LEN + payload.len();
        let aligned = align_frame_len(total) as u64;
        let b = &self.buffer;

        let off = b.offset(self.pos);
        let to_end = b.capacity - off as u64;
        let pad = if aligned > to_end { to_end } else { 0 };
        let end = self.pos + pad + aligned;

        // The one hard gate: never claim past durable + capacity.
        if end > self.cached_durable + b.capacity {
            self.cached_durable = b.counters.durable.load_acquire();
            if end > self.cached_durable + b.capacity {
                return Err(AppendError::WouldOverrun);
            }
        }

        let frame_pos = if pad > 0 {
            self.write_padding(off, pad as u32);
            self.pos + pad
        } else {
            self.pos
        };
        let foff = b.offset(frame_pos);

        // SAFETY (all raw writes): within capacity; bytes in [append,
        // durable+capacity) are writer-owned per the gate; ordering via the
        // commit word + append counter release stores below.
        unsafe {
            std::ptr::copy_nonoverlapping(
                payload.as_ptr(),
                b.region.ptr_at(foff + HEADER_LEN),
                payload.len(),
            );
            let hdr = std::slice::from_raw_parts_mut(b.region.ptr_at(foff), HEADER_LEN);
            frame::write_header_except_length(
                hdr,
                &FrameHeader {
                    length: 0,
                    frame_type: FRAME_TYPE_MESSAGE,
                    flags: 0,
                    leadership_term_id: self.leadership_term_id,
                    session_id,
                    correlation_id,
                },
            );
        }
        b.commit_word(foff).store(total as u32, Ordering::Release);

        self.pos = end;
        b.counters.append.store_release(self.pos);
        Ok(frame_pos)
    }

    /// Padding frame: header only; `length` spans to the buffer end.
    fn write_padding(&self, off: usize, pad_len: u32) {
        let b = &self.buffer;
        // SAFETY: as in append().
        unsafe {
            let hdr = std::slice::from_raw_parts_mut(b.region.ptr_at(off), HEADER_LEN);
            frame::write_header_except_length(
                hdr,
                &FrameHeader {
                    length: 0,
                    frame_type: FRAME_TYPE_PADDING,
                    flags: 0,
                    leadership_term_id: self.leadership_term_id,
                    session_id: 0,
                    correlation_id: 0,
                },
            );
        }
        b.commit_word(off).store(pad_len, Ordering::Release);
    }
}
```

Add `pub mod buffer;` to `uc_log/src/lib.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p uc_log buffer`
Expected: 5 passed. (If `wrap_emits_padding_and_slice_stops_at_wrap` fails on the gate, note the test advances `durable` before wrapping — the gate math is `end > durable + capacity`.)

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p uc_log -- -D warnings
git add uc_log/src/buffer.rs uc_log/src/lib.rs
git commit -m "feat(uc_log): log buffer — single-writer append, wrap padding, overrun gate, recordable slices"
```

---

### Task 4: Validated positional reads + loom model of the commit protocol

**Files:**
- Modify: `uc_log/src/buffer.rs` (add `FrameRead`, `LogBuffer::read_frame_validated`)
- Create: `uc_log/tests/loom_frame.rs`

**Interfaces:**
- Consumes: Task 3's `LogBuffer` internals (`commit_word`, `max_claim`, `offset`, region).
- Produces (used by M2's sender/NAK path and M5's service reader):
  - `enum FrameRead { Frame(FrameHeader), NotCommitted, Overrun }`
  - `LogBuffer::read_frame_validated(&self, pos: u64, out: &mut Vec<u8>) -> FrameRead` — copies the whole frame (header+payload) into `out` on `Frame`

- [ ] **Step 1: Write the failing tests**

Append to the test module in `uc_log/src/buffer.rs`:

```rust
    #[test]
    fn validated_read_roundtrip_and_not_committed() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 2);
        a.append(9, 77, b"abc").unwrap();
        let mut out = Vec::new();
        match b.read_frame_validated(0, &mut out) {
            FrameRead::Frame(h) => {
                assert_eq!(h.correlation_id, 77);
                assert_eq!(h.length as usize, HEADER_LEN + 3);
                assert_eq!(&out[HEADER_LEN..], b"abc");
            }
            other => panic!("expected Frame, got {other:?}"),
        }
        // beyond append -> NotCommitted
        assert!(matches!(b.read_frame_validated(64, &mut out), FrameRead::NotCommitted));
    }

    #[test]
    fn validated_read_detects_overrun_after_wrap() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        // write ~3 capacities worth, letting the gate breathe by keeping
        // durable glued to append (as a healthy archive would)
        let mut n = 0u64;
        while a.position() < 3 * CAP {
            a.append(1, n, &[0u8; 64]).unwrap();
            c.durable.store_release(a.position());
            n += 1;
        }
        // position 0 was overwritten laps ago
        let mut out = Vec::new();
        assert!(matches!(b.read_frame_validated(0, &mut out), FrameRead::Overrun));
        // a recent frame still reads fine (within capacity minus margin)
        let recent = a.position() - 96;
        assert!(matches!(b.read_frame_validated(recent, &mut out), FrameRead::Frame(_)));
    }
```

Create `uc_log/tests/loom_frame.rs` — a loom model of the exact ordering protocol (release commit word + release append counter vs acquire reader). It models the protocol on loom atomics rather than driving the mmap code (loom requires its own atomic types); the model must mirror `buffer.rs` ordering choices — if those change, change this test:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Loom model of the frame commit protocol (buffer.rs):
//!   writer: plain payload writes -> Release store of length -> Release store of append
//!   reader: Acquire load of append -> bounded read -> payload fully visible
//! Run: RUSTFLAGS="--cfg loom" cargo test -p uc_log --test loom_frame --release
#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use loom::thread;

#[test]
fn committed_frame_is_fully_visible_to_append_bounded_reader() {
    loom::model(|| {
        // 2 "frames": [len_word, payload_word] each, plus an append counter.
        let cells: Arc<Vec<AtomicU32>> = Arc::new((0..4).map(|_| AtomicU32::new(0)).collect());
        let append = Arc::new(AtomicU64::new(0));

        let w_cells = Arc::clone(&cells);
        let w_append = Arc::clone(&append);
        let writer = thread::spawn(move || {
            for f in 0..2u64 {
                let base = (f * 2) as usize;
                // payload (plain-ish: modeled as relaxed — buffer.rs uses raw
                // non-atomic writes; the Release on the length word orders them)
                w_cells[base + 1].store(0xAB00 + f as u32, Ordering::Relaxed);
                // commit word, Release (mirrors commit_word().store(Release))
                w_cells[base].store(64, Ordering::Release);
                // append counter, Release (mirrors counters.append.store_release)
                w_append.store(f + 1, Ordering::Release);
            }
        });

        // reader bounded by an acquire of append (mirrors recordable_slice /
        // read_frame_validated pre-check)
        let bound = append.load(Ordering::Acquire);
        for f in 0..bound {
            let base = (f * 2) as usize;
            let len = cells[base].load(Ordering::Acquire);
            assert_eq!(len, 64, "commit word must be visible below append");
            let payload = cells[base + 1].load(Ordering::Relaxed);
            assert_eq!(payload, 0xAB00 + f as u32, "payload must be visible after acquire");
        }

        writer.join().unwrap();
    });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p uc_log buffer`
Expected: COMPILE ERROR (`FrameRead` / `read_frame_validated` not defined).

- [ ] **Step 3: Write the implementation**

Add to `impl LogBuffer` in `uc_log/src/buffer.rs`:

```rust
    /// Read one frame at `pos` with overwrite validation, for lagging /
    /// position-addressed readers (M2 NAK retransmit, M5 service). Copies the
    /// frame into `out` then re-checks the append counter: if the appender
    /// could have advanced into (or near) this frame's bytes, returns
    /// `Overrun` (caller falls back to journal replay). The margin is
    /// `max_claim()` because an in-flight append's writes (padding header +
    /// frame) are not yet reflected in the counter.
    pub fn read_frame_validated(&self, pos: u64, out: &mut Vec<u8>) -> FrameRead {
        let append = self.counters.append.load_acquire();
        if pos >= append {
            return FrameRead::NotCommitted;
        }
        if append + self.max_claim() > pos + self.capacity {
            return FrameRead::Overrun;
        }
        let off = self.offset(pos);
        let len = self.commit_word(off).load(Ordering::Acquire) as usize;
        debug_assert!(len >= 4 && align_frame_len(len) as u64 <= self.capacity - off as u64);
        out.clear();
        // SAFETY: [off, off+len) within capacity (frames never span the wrap).
        out.extend_from_slice(unsafe { std::slice::from_raw_parts(self.region.ptr_at(off), len) });
        // Re-validate: did the appender advance into our margin during the copy?
        let append_after = self.counters.append.load_acquire();
        if append_after + self.max_claim() > pos + self.capacity {
            return FrameRead::Overrun;
        }
        FrameRead::Frame(frame::read_header(out))
    }
```

And above `LogBuffer`:

```rust
#[derive(Debug)]
pub enum FrameRead {
    /// Frame copied into `out` (header + payload, unaligned length).
    Frame(FrameHeader),
    /// `pos` is at or beyond the append counter.
    NotCommitted,
    /// The frame's bytes may have been overwritten (reader lagged more than
    /// capacity − max_claim behind). Fall back to journal replay.
    Overrun,
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p uc_log buffer`
Expected: 7 passed.

Run the loom model: `RUSTFLAGS="--cfg loom" cargo test -p uc_log --test loom_frame --release`
Expected: 1 passed (loom explores all interleavings; takes seconds).

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p uc_log -- -D warnings
git add uc_log/src/buffer.rs uc_log/tests/loom_frame.rs
git commit -m "feat(uc_log): validated positional reads with overwrite margin + loom model of the commit protocol"
```

---

### Task 5: mmap-backed `Region` + `LogBuffer` file create/open

**Files:**
- Modify: `uc_log/src/region.rs` (add `Backing::Mmap`, `Region::from_mmap`)
- Modify: `uc_log/src/buffer.rs` (add `LogBuffer::create_file` / `LogBuffer::open_file`)
- Create: `uc_log/tests/buffer_file.rs`

**Interfaces:**
- Consumes: Tasks 2–4.
- Produces (used by Tasks 6–9 and later milestones):
  - `Region::from_mmap(m: memmap2::MmapMut) -> Region`
  - `LogBuffer::create_file(path: &Path, capacity: u64, counters: Arc<LogCounters>, max_payload: usize) -> Result<LogBuffer, std::io::Error>` (creates/truncates, `set_len(capacity)`)
  - `LogBuffer::open_file(path: &Path, counters: Arc<LogCounters>, max_payload: usize) -> Result<LogBuffer, std::io::Error>` (capacity = file length; validated by `LogBuffer::new` asserts)

- [ ] **Step 1: Write the failing test**

`uc_log/tests/buffer_file.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![cfg(not(loom))]

use std::sync::Arc;
use uc_log::buffer::{Appender, FrameRead, LogBuffer};
use uc_log::counters::LogCounters;

#[test]
#[cfg_attr(miri, ignore)] // real mmap
fn file_backed_buffer_roundtrip_across_reopen_of_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.buf");
    let counters = Arc::new(LogCounters::new());

    let b = Arc::new(
        LogBuffer::create_file(&path, 1 << 16, Arc::clone(&counters), 1024).unwrap(),
    );
    let mut a = Appender::new(Arc::clone(&b), 1);
    let pos = a.append(5, 6, b"persisted?").unwrap();

    let mut out = Vec::new();
    assert!(matches!(b.read_frame_validated(pos, &mut out), FrameRead::Frame(_)));
    drop(a);
    drop(b);

    // Re-map the same file: bytes are there (same-host shared mapping is the
    // M5 IPC story; counters are NOT in the file yet — prime them by hand).
    let counters2 = Arc::new(LogCounters::new());
    counters2.prime(64); // one 42-byte frame -> aligned 64
    let b2 = LogBuffer::open_file(&path, counters2, 1024).unwrap();
    let mut out2 = Vec::new();
    match b2.read_frame_validated(0, &mut out2) {
        FrameRead::Frame(h) => {
            assert_eq!(h.session_id, 5);
            assert_eq!(&out2[32..], b"persisted?");
        }
        other => panic!("expected Frame, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p uc_log --test buffer_file`
Expected: COMPILE ERROR (`create_file` not defined).

- [ ] **Step 3: Write the implementation**

In `uc_log/src/region.rs`, extend `Backing` and add the constructor:

```rust
enum Backing {
    Heap(Layout),
    Mmap(memmap2::MmapMut),
}
```

```rust
    /// mmap-backed region (real instances; the file lives in the instance
    /// dir, e.g. /dev/shm for same-host IPC). The mapping's address is stable
    /// for the Region's lifetime (the MmapMut is owned and never remapped).
    pub fn from_mmap(m: memmap2::MmapMut) -> Self {
        let len = m.len();
        assert!(len > 0);
        let ptr = NonNull::new(m.as_ptr() as *mut u8).expect("mmap ptr");
        Self { ptr, len, backing: Backing::Mmap(m) }
    }
```

And in `Drop`, add the arm:

```rust
            Backing::Mmap(_) => {} // munmap on MmapMut drop
```

In `uc_log/src/buffer.rs`, add to `impl LogBuffer` (with `use std::path::Path;` at the top):

```rust
    /// Create (or truncate) the buffer file at `capacity` bytes and map it.
    pub fn create_file(
        path: &Path,
        capacity: u64,
        counters: Arc<LogCounters>,
        max_payload: usize,
    ) -> Result<Self, std::io::Error> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(capacity)?;
        // SAFETY: exclusive logical ownership per the instance-dir contract
        // (one node per instance dir; instance.lock arrives with uc_node).
        let m = unsafe { memmap2::MmapMut::map_mut(&file)? };
        Ok(Self::new(Region::from_mmap(m), counters, max_payload))
    }

    /// Map an existing buffer file; capacity = file length.
    pub fn open_file(
        path: &Path,
        counters: Arc<LogCounters>,
        max_payload: usize,
    ) -> Result<Self, std::io::Error> {
        let file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
        // SAFETY: see create_file.
        let m = unsafe { memmap2::MmapMut::map_mut(&file)? };
        Ok(Self::new(Region::from_mmap(m), counters, max_payload))
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p uc_log`
Expected: all pass (unit + buffer_file).

Best-effort miri over the heap-backed unit tests (mmap/journal tests are `#[cfg_attr(miri, ignore)]` or unit-level heap only):

```bash
rustup +nightly component add miri 2>/dev/null && cargo +nightly miri test -p uc_log --lib || echo "miri unavailable on this box — record as TODO in the task doc"
```
Expected if available: all lib tests pass under miri (raw-pointer buffer code is the point of this run).

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p uc_log -- -D warnings
git add uc_log/src/region.rs uc_log/src/buffer.rs uc_log/tests/buffer_file.rs
git commit -m "feat(uc_log): mmap-backed region + buffer file create/open"
```

---

### Task 6: `Archive` — block recording into uc_journal

**Files:**
- Create: `uc_log/src/archive.rs`
- Modify: `uc_log/src/lib.rs` (add `pub mod archive;`)

**Interfaces:**
- Consumes: `LogBuffer::recordable_slice` (Task 3), `LogCounters` (Task 2), `uc_journal::{Journal, JournalConfig, Durability}`.
- Produces (used by Tasks 7–9):
  - `struct ArchiveConfig { pub dir: PathBuf, pub max_block_bytes: usize, pub segment_size_bytes: u64, pub preallocate_segments: bool }` with `ArchiveConfig::new(dir) -> Self` (defaults: 1 MiB blocks, 64 MiB segments, preallocate true)
  - `Archive::open(cfg: ArchiveConfig) -> Result<Archive, ArchiveError>`
  - `Archive::recovered_position(&self) -> u64` (0 on a fresh dir)
  - `Archive::do_work(&mut self, buffer: &LogBuffer) -> Result<bool, ArchiveError>` — records ≤1 block, fsyncs, advances the durable counter; `Ok(false)` = nothing to do
  - `Archive::blocks_recorded(&self) -> u64`
  - `enum ArchiveError` (wraps `JournalError`, plus `PositionPurged` in Task 7)

- [ ] **Step 1: Write the failing tests**

Test module at the bottom of `uc_log/src/archive.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{Appender, LogBuffer};
    use crate::counters::LogCounters;
    use crate::region::Region;
    use std::sync::Arc;
    use uc_protocol::v2::frame::read_header;

    fn setup(cap: usize) -> (Arc<LogBuffer>, Arc<LogCounters>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let counters = Arc::new(LogCounters::new());
        let b = Arc::new(LogBuffer::new(
            Region::heap_zeroed(cap),
            Arc::clone(&counters),
            256,
        ));
        (b, counters, dir)
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn records_blocks_and_advances_durable() {
        let (b, c, dir) = setup(1 << 16);
        let mut arch = Archive::open(ArchiveConfig::new(dir.path())).unwrap();
        assert_eq!(arch.recovered_position(), 0);

        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..10 {
            a.append(1, i, &[7u8; 64]).unwrap();
        }
        assert!(arch.do_work(&b).unwrap()); // one block: all 10 frames (960 B < 1 MiB)
        assert!(!arch.do_work(&b).unwrap()); // caught up
        assert_eq!(c.durable.load_acquire(), 960);
        assert_eq!(arch.blocks_recorded(), 1);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn blocks_split_at_max_and_meta_is_base_position() {
        let (b, _c, dir) = setup(1 << 16);
        let cfg = ArchiveConfig { max_block_bytes: 200, ..ArchiveConfig::new(dir.path()) };
        let mut arch = Archive::open(cfg).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..4 {
            a.append(1, i, &[0u8; 64]).unwrap(); // 4 x 96 B
        }
        // 200-byte cap -> 2 frames per block (192 B), frame-aligned
        assert!(arch.do_work(&b).unwrap());
        assert!(arch.do_work(&b).unwrap());
        assert!(!arch.do_work(&b).unwrap());
        let j = arch.journal();
        let (meta0, blk0) = j.read(0).unwrap().unwrap();
        let (meta1, blk1) = j.read(1).unwrap().unwrap();
        assert_eq!((meta0, blk0.len()), (0, 192));
        assert_eq!((meta1, blk1.len()), (192, 192));
        // block content is raw frames: header parses, payload intact
        let h = read_header(&blk1);
        assert_eq!(h.correlation_id, 2);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn durable_only_advances_after_fsync_completion() {
        // Durability::Consistent -> Notifier::wait() returns post-fdatasync;
        // observable contract here: durable equals exactly what was recorded,
        // and journal.durable_seq() covers every block we advanced over.
        let (b, c, dir) = setup(1 << 16);
        let mut arch = Archive::open(ArchiveConfig::new(dir.path())).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        a.append(1, 0, &[1u8; 64]).unwrap();
        arch.do_work(&b).unwrap();
        assert_eq!(c.durable.load_acquire(), 96);
        // block 0 must already be durable (wait returns immediately)
        arch.journal().wait_durable(0).unwrap();
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p uc_log archive`
Expected: COMPILE ERROR.

- [ ] **Step 3: Write the implementation**

`uc_log/src/archive.rs` (above tests):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The archive agent (spec §4): polls the log buffer from the durable
//! position, block-writes whatever accumulated (≤ max_block_bytes,
//! frame-aligned) as ONE journal record per block — seq = block index,
//! meta = block base position — with one fdatasync per block
//! (Durability::Consistent), then advances the durable counter. The poll
//! batching IS the group commit: fsync frequency scales with block rate,
//! not message rate, and there is no linger anywhere.

use std::path::PathBuf;

use uc_journal::{Durability, Journal, JournalConfig, JournalError};

use crate::buffer::LogBuffer;

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("journal error: {0}")]
    Journal(#[from] JournalError),
    #[error("position {pos} is below the first archived block (first base {first_base})")]
    PositionPurged { pos: u64, first_base: u64 },
}

#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    pub dir: PathBuf,
    /// Soft cap per recorded block; a single frame larger than this still
    /// records as one block (blocks are frame-aligned, never split a frame).
    pub max_block_bytes: usize,
    pub segment_size_bytes: u64,
    pub preallocate_segments: bool,
}

impl ArchiveConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            max_block_bytes: 1024 * 1024,
            segment_size_bytes: 64 * 1024 * 1024,
            preallocate_segments: true,
        }
    }
}

pub struct Archive {
    journal: Journal,
    cfg: ArchiveConfig,
    durable_pos: u64,
    next_block_seq: u64,
}

impl Archive {
    /// Open the journal and recover the durable frontier: the last block's
    /// base position + length. Fresh dir -> position 0.
    pub fn open(cfg: ArchiveConfig) -> Result<Self, ArchiveError> {
        let jcfg = JournalConfig {
            segment_size_bytes: cfg.segment_size_bytes,
            durability: Durability::Consistent,
            preallocate_segments: cfg.preallocate_segments,
            ..JournalConfig::new(&cfg.dir)
        };
        let journal = Journal::open(jcfg)?;
        let (durable_pos, next_block_seq) = match journal.last_seq() {
            None => (0, 0),
            Some(last) => {
                let (meta, payload) = journal
                    .read(last)?
                    .expect("last_seq block must be readable");
                (meta + payload.len() as u64, last + 1)
            }
        };
        Ok(Self { journal, cfg, durable_pos, next_block_seq })
    }

    /// Where the log resumes after recovery (counters.prime(this)).
    #[inline]
    pub fn recovered_position(&self) -> u64 {
        self.durable_pos
    }

    #[inline]
    pub fn blocks_recorded(&self) -> u64 {
        self.next_block_seq
    }

    /// Test/replay access to the underlying journal.
    pub fn journal(&self) -> &Journal {
        &self.journal
    }

    /// One duty cycle: record at most one block. Returns Ok(true) if work was
    /// done. The durable counter is advanced ONLY after Notifier::wait()
    /// returns (Consistent durability => post-fdatasync).
    pub fn do_work(&mut self, buffer: &LogBuffer) -> Result<bool, ArchiveError> {
        let slice = buffer.recordable_slice(self.durable_pos, self.cfg.max_block_bytes);
        if slice.is_empty() {
            return Ok(false);
        }
        let notifier = self.journal.append(self.next_block_seq, self.durable_pos, slice)?;
        let len = slice.len() as u64;
        notifier.wait()?;
        self.durable_pos += len;
        self.next_block_seq += 1;
        buffer.counters().durable.store_release(self.durable_pos);
        Ok(true)
    }
}
```

Add `pub mod archive;` to `uc_log/src/lib.rs`.

Note: `tempfile` is already a dev-dependency (Task 2); journal tests write real files, so keep them out of miri (`--lib` miri runs skip integration; these are unit tests in-module — if miri is being run, they hit real syscalls: add `#[cfg_attr(miri, ignore)]` on all three tests in this module).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p uc_log archive`
Expected: 3 passed. (If `Journal::append` rejects seq 0 as the first record, adapt `next_block_seq` recovery to the journal's actual first-seq convention — the test failure will say which; update the test's `read(0)` accordingly and note it in the commit message.)

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p uc_log -- -D warnings
git add uc_log/src/archive.rs uc_log/src/lib.rs
git commit -m "feat(uc_log): archive — frame-aligned block recording, fsync per block, durable counter"
```

---

### Task 7: Archive recovery on reopen + `replay_from`

**Files:**
- Modify: `uc_log/src/archive.rs` (add `Replay`, `ReplayFrame`, `Archive::replay_from`; recovery tests)

**Interfaces:**
- Consumes: Task 6's `Archive`.
- Produces (used by M2 replay sessions, M5 service catch-up, M4 reconciliation):
  - `struct ReplayFrame { pub position: u64, pub header: FrameHeader, pub payload: Vec<u8> }`
  - `Archive::replay_from(&self, pos: u64) -> Result<Replay<'_>, ArchiveError>` — `pos` must be a frame start (or `>= durable`, yielding an empty replay); `Err(PositionPurged)` below the first block
  - `Replay::next(&mut self) -> Result<Option<ReplayFrame>, ArchiveError>` — yields message frames in position order, skipping padding

- [ ] **Step 1: Write the failing tests**

Append to the test module in `uc_log/src/archive.rs`:

```rust
    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn reopen_recovers_durable_frontier_and_appends_continue() {
        let (b, c, dir) = setup(1 << 16);
        {
            let mut arch = Archive::open(ArchiveConfig::new(dir.path())).unwrap();
            let mut a = Appender::new(Arc::clone(&b), 1);
            for i in 0..5 {
                a.append(1, i, &[0u8; 64]).unwrap();
            }
            while arch.do_work(&b).unwrap() {}
        }
        // "restart": fresh archive over the same dir, fresh buffer/counters
        let arch = Archive::open(ArchiveConfig::new(dir.path())).unwrap();
        assert_eq!(arch.recovered_position(), 480);
        let (b2, c2, _) = setup(1 << 16);
        c2.prime(arch.recovered_position());
        let mut arch = arch;
        let mut a2 = Appender::new(Arc::clone(&b2), 2);
        // Appender::new picks up position from the primed counters
        assert_eq!(a2.position(), 480);
        let pos = a2.append(1, 100, &[0u8; 64]).unwrap();
        assert_eq!(pos, 480);
        assert!(arch.do_work(&b2).unwrap());
        assert_eq!(c2.durable.load_acquire(), 576);
        let _ = (c, b);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn replay_from_yields_frames_and_skips_padding() {
        // small buffer so a wrap (padding frame) lands in the journal
        let (b, c, dir) = setup(4096);
        let mut arch = Archive::open(ArchiveConfig::new(dir.path())).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        let mut n = 0u64;
        let mut positions = Vec::new();
        while a.position() < 5000 {
            match a.append(1, n, &[n as u8; 64]) {
                Ok(p) => {
                    positions.push((p, n));
                    n += 1;
                }
                Err(crate::buffer::AppendError::WouldOverrun) => {
                    arch.do_work(&b).unwrap();
                }
                Err(e) => panic!("{e}"),
            }
            let _ = &c;
        }
        while arch.do_work(&b).unwrap() {}

        // replay from the very beginning: every message frame, in order,
        // padding silently skipped
        let mut r = arch.replay_from(0).unwrap();
        for (p, corr) in &positions {
            let f = r.next().unwrap().expect("frame");
            assert_eq!(f.position, *p);
            assert_eq!(f.header.correlation_id, *corr);
            assert_eq!(f.payload, vec![*corr as u8; 64]);
        }
        assert!(r.next().unwrap().is_none());

        // replay from a mid-stream frame start (binary search across blocks)
        let (mid_pos, mid_corr) = positions[positions.len() / 2];
        let mut r = arch.replay_from(mid_pos).unwrap();
        let f = r.next().unwrap().expect("frame");
        assert_eq!((f.position, f.header.correlation_id), (mid_pos, mid_corr));

        // at/beyond durable: empty replay, not an error
        let mut r = arch.replay_from(arch.recovered_position()).unwrap();
        assert!(r.next().unwrap().is_none());
    }
```

(`PositionPurged` gets its test in M6 when purge lands; the error arm exists now because `replay_from` must already refuse positions below block 0's base after any future purge — with no purge, base is 0 and the arm is unreachable.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p uc_log archive`
Expected: COMPILE ERROR (`replay_from` not defined).

- [ ] **Step 3: Write the implementation**

Add to `uc_log/src/archive.rs` (imports: `use uc_protocol::v2::frame::{self, FrameHeader, FRAME_TYPE_PADDING, HEADER_LEN};`):

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayFrame {
    pub position: u64,
    pub header: FrameHeader,
    pub payload: Vec<u8>,
}

/// Sequential frame reader over archived blocks. Not a std::Iterator because
/// journal reads are fallible.
pub struct Replay<'a> {
    journal: &'a Journal,
    /// next block seq to read; > last_seq means exhausted
    seq: u64,
    last_seq: Option<u64>,
    block: Vec<u8>,
    block_base: u64,
    off: usize,
    /// skip frames below this position (mid-block replay starts)
    skip_below: u64,
}

impl Replay<'_> {
    pub fn next(&mut self) -> Result<Option<ReplayFrame>, ArchiveError> {
        loop {
            if self.off >= self.block.len() {
                let Some(last) = self.last_seq else { return Ok(None) };
                if self.seq > last {
                    return Ok(None);
                }
                let (meta, payload) = self
                    .journal
                    .read(self.seq)?
                    .expect("block in [first,last] must be readable");
                debug_assert!(
                    self.block.is_empty() || meta == self.block_base + self.block.len() as u64,
                    "archived blocks must be position-contiguous"
                );
                self.block_base = meta;
                self.block = payload;
                self.off = 0;
                self.seq += 1;
            }
            let hdr = frame::read_header(&self.block[self.off..]);
            let total = hdr.length as usize;
            let aligned = frame::align_frame_len(total);
            let position = self.block_base + self.off as u64;
            let payload_range = self.off + HEADER_LEN..self.off + total;
            self.off += aligned;
            if hdr.frame_type == FRAME_TYPE_PADDING || position < self.skip_below {
                continue;
            }
            return Ok(Some(ReplayFrame {
                position,
                header: hdr,
                payload: self.block[payload_range].to_vec(),
            }));
        }
    }
}

impl Archive {
    /// Replay archived frames starting at `pos` (a frame start). Positions at
    /// or beyond the durable frontier yield an empty replay. Positions below
    /// the first archived block are gone (purged) -> error.
    pub fn replay_from(&self, pos: u64) -> Result<Replay<'_>, ArchiveError> {
        let exhausted = Replay {
            journal: &self.journal,
            seq: 1,
            last_seq: None,
            block: Vec::new(),
            block_base: 0,
            off: 0,
            skip_below: 0,
        };
        if pos >= self.durable_pos {
            return Ok(exhausted);
        }
        let (Some(first), Some(last)) = (self.journal.first_seq(), self.journal.last_seq())
        else {
            return Ok(exhausted);
        };
        let (first_meta, _) = self.journal.read(first)?.expect("first block readable");
        if pos < first_meta {
            return Err(ArchiveError::PositionPurged { pos, first_base: first_meta });
        }
        // binary search: greatest block with base <= pos
        let (mut lo, mut hi) = (first, last);
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            let (meta, _) = self.journal.read(mid)?.expect("block readable");
            if meta <= pos { lo = mid } else { hi = mid - 1 }
        }
        Ok(Replay {
            journal: &self.journal,
            seq: lo,
            last_seq: Some(last),
            block: Vec::new(),
            block_base: 0,
            off: 0,
            skip_below: pos,
        })
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p uc_log archive`
Expected: 5 passed.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p uc_log -- -D warnings
git add uc_log/src/archive.rs
git commit -m "feat(uc_log): archive recovery on reopen + replay_from with block binary search"
```

---

### Task 8: `agent` — IdleStrategy + AgentRunner

**Files:**
- Create: `uc_log/src/agent.rs`
- Modify: `uc_log/src/lib.rs` (add `pub mod agent;`)

**Interfaces:**
- Consumes: nothing crate-internal.
- Produces (used by Task 9 and, later, `uc_net`/`uc_node` — may migrate to a shared crate then):
  - `enum IdleStrategy { BusySpin, Yield, Sleep(std::time::Duration) }` with `fn idle(&self)`
  - `AgentRunner::spawn(name: &str, idle: IdleStrategy, work: impl FnMut() -> bool + Send + 'static) -> std::io::Result<AgentRunner>` — loops `work()`, idling per strategy when it returns `false`
  - `AgentRunner::stop(self)` — signals stop, joins the thread (propagates a panic from `work`)

- [ ] **Step 1: Write the failing test**

Test module at the bottom of `uc_log/src/agent.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn runner_drives_work_and_stops_cleanly() {
        let count = Arc::new(AtomicU64::new(0));
        let c = Arc::clone(&count);
        let runner = AgentRunner::spawn("test-agent", IdleStrategy::Yield, move || {
            c.fetch_add(1, Ordering::Relaxed);
            true
        })
        .unwrap();
        while count.load(Ordering::Relaxed) < 1000 {
            std::thread::yield_now();
        }
        runner.stop();
        let n = count.load(Ordering::Relaxed);
        assert!(n >= 1000);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p uc_log agent`
Expected: COMPILE ERROR.

- [ ] **Step 3: Write the implementation**

`uc_log/src/agent.rs` (above tests):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Single-writer polling agents (spec §3.1): a duty-cycle closure on a
//! dedicated thread with a configurable idle strategy. No pools, no async.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleStrategy {
    /// Never park; lowest latency, pegs the core.
    BusySpin,
    /// Yield to the OS scheduler between empty cycles.
    Yield,
    /// Sleep between empty cycles (background-grade agents).
    Sleep(Duration),
}

impl IdleStrategy {
    #[inline]
    pub fn idle(&self) {
        match self {
            IdleStrategy::BusySpin => std::hint::spin_loop(),
            IdleStrategy::Yield => std::thread::yield_now(),
            IdleStrategy::Sleep(d) => std::thread::sleep(*d),
        }
    }
}

pub struct AgentRunner {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

impl AgentRunner {
    /// Spawn a named agent thread looping `work()`; when `work` returns
    /// false (no work done), the idle strategy runs.
    pub fn spawn<F>(name: &str, idle: IdleStrategy, mut work: F) -> io::Result<AgentRunner>
    where
        F: FnMut() -> bool + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let handle = std::thread::Builder::new().name(name.to_string()).spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                if !work() {
                    idle.idle();
                }
            }
        })?;
        Ok(AgentRunner { stop, handle })
    }

    /// Signal stop and join; propagates a panic from the work closure.
    pub fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().expect("agent thread panicked");
    }
}
```

Add `pub mod agent;` to `uc_log/src/lib.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p uc_log agent`
Expected: 1 passed.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p uc_log -- -D warnings
git add uc_log/src/agent.rs uc_log/src/lib.rs
git commit -m "feat(uc_log): polling agent runner + idle strategies"
```

---

### Task 9: `m1_gate` throughput example + gate run + docs

**Files:**
- Create: `uc_log/examples/m1_gate.rs`
- Create: `docs/benchmarks/uc2-m1-gate-2026-07-09.md` (written from the run's output)

**Interfaces:**
- Consumes: everything above (`LogBuffer::create_file`, `Appender`, `Archive`, `AgentRunner`).
- Produces: the M1 gate measurement (spec §9: ≥1 M msgs/s, 64 B payloads, append+record+fsync, solo).

- [ ] **Step 1: Write the example**

`uc_log/examples/m1_gate.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M1 gate: solo append+record+fsync throughput (spec §9: >= 1M msgs/s @ 64B).
//!
//! Usage: cargo run -p uc_log --release --example m1_gate -- <journal_dir> \
//!            [secs=10] [payload=64] [buffer_mib=512] [buffer_path=/dev/shm/uc2-m1-gate.buf]
//!
//! Layout mirrors deployment: buffer file on tmpfs (/dev/shm — no writeback
//! I/O), journal on the real disk (<journal_dir> — put it on NVMe).
//! Appender runs on the main thread (stand-in for the consensus agent);
//! the archive agent busy-spins on its own thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use uc_log::agent::{AgentRunner, IdleStrategy};
use uc_log::archive::{Archive, ArchiveConfig};
use uc_log::buffer::{Appender, AppendError, LogBuffer};
use uc_log::counters::LogCounters;

fn main() {
    let mut args = std::env::args().skip(1);
    let journal_dir = args.next().expect("usage: m1_gate <journal_dir> [secs] [payload] [buffer_mib] [buffer_path]");
    let secs: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(10);
    let payload_len: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(64);
    let buffer_mib: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(512);
    let buffer_path = args
        .next()
        .unwrap_or_else(|| "/dev/shm/uc2-m1-gate.buf".to_string());

    std::fs::create_dir_all(&journal_dir).unwrap();
    let counters = Arc::new(LogCounters::new());
    let mut archive = Archive::open(ArchiveConfig::new(&journal_dir)).unwrap();
    counters.prime(archive.recovered_position());
    let buffer = Arc::new(
        LogBuffer::create_file(
            buffer_path.as_ref(),
            buffer_mib * 1024 * 1024,
            Arc::clone(&counters),
            1024 * 1024,
        )
        .unwrap(),
    );
    assert_eq!(archive.recovered_position(), 0, "use a fresh journal dir for the gate");

    let blocks = Arc::new(AtomicU64::new(0));
    let blocks_c = Arc::clone(&blocks);
    let buf_c = Arc::clone(&buffer);
    let agent = AgentRunner::spawn("uc2-archive", IdleStrategy::BusySpin, move || {
        match archive.do_work(&buf_c) {
            Ok(true) => {
                blocks_c.store(archive.blocks_recorded(), Ordering::Relaxed);
                true
            }
            Ok(false) => false,
            Err(e) => panic!("archive: {e}"),
        }
    })
    .unwrap();

    let payload = vec![0xa5u8; payload_len];
    let mut appender = Appender::new(Arc::clone(&buffer), 1);
    let start = Instant::now();
    let deadline = start + Duration::from_secs(secs);
    let mut appended = 0u64;
    let mut overruns = 0u64;
    let mut next_report = start + Duration::from_secs(1);
    while Instant::now() < deadline {
        for _ in 0..1024 {
            match appender.append(1, appended, &payload) {
                Ok(_) => appended += 1,
                Err(AppendError::WouldOverrun) => {
                    overruns += 1;
                    std::hint::spin_loop();
                }
                Err(e) => panic!("{e}"),
            }
        }
        let now = Instant::now();
        if now >= next_report {
            next_report = now + Duration::from_secs(1);
            eprintln!(
                "t={:>3}s appended={} durable_lag={}B",
                start.elapsed().as_secs(),
                appended,
                counters.append.load_acquire() - counters.durable.load_acquire(),
            );
        }
    }
    let elapsed = start.elapsed();
    // drain: wait for the archive to catch up, then stop it
    while counters.durable.load_acquire() < counters.append.load_acquire() {
        std::thread::yield_now();
    }
    agent.stop();

    let nblocks = blocks.load(Ordering::Relaxed);
    let bytes = counters.durable.load_acquire();
    let rate = appended as f64 / elapsed.as_secs_f64();
    println!("== uc2 M1 gate ==");
    println!("payload            {payload_len} B  (96 B framed at 64 B)");
    println!("duration           {:.2} s", elapsed.as_secs_f64());
    println!("appended           {appended} msgs");
    println!("rate               {:.0} msgs/s", rate);
    println!("recorded+fsynced   {bytes} B ({:.1} MB/s)", bytes as f64 / elapsed.as_secs_f64() / 1e6);
    println!("blocks (=fsyncs)   {nblocks} ({:.0}/s, avg {:.0} KiB)", nblocks as f64 / elapsed.as_secs_f64(), bytes as f64 / nblocks.max(1) as f64 / 1024.0);
    println!("overrun stalls     {overruns}");
    println!("GATE (>=1M msgs/s @64B): {}", if payload_len == 64 && rate >= 1_000_000.0 { "PASS" } else { "CHECK" });
    let _ = std::fs::remove_file(&buffer_path);
}
```

- [ ] **Step 2: Build and smoke-run it (short)**

```bash
cargo build --release -p uc_log --example m1_gate
rm -rf /tmp/uc2-m1-smoke && cargo run --release -p uc_log --example m1_gate -- /tmp/uc2-m1-smoke 3
```
Expected: runs 3 s, prints the report, no panics, `durable_lag` bounded (not growing monotonically), overrun stalls small or zero.

- [ ] **Step 3: Run the gate (10 s, journal on the fastest local disk)**

```bash
rm -rf /tmp/uc2-m1-gate && cargo run --release -p uc_log --example m1_gate -- /tmp/uc2-m1-gate 10
```
Record the full output. The official gate hardware is a c6id NVMe box (fleet validation is a follow-up with the user — bench-infra has no solo-node role yet); on the local dev box, record whatever it does and compare against the journal's known standalone rates. If the local rate is far below 1 M/s, check `avg block KiB` first: healthy structural batching should show blocks growing with load and fsyncs/s in the hundreds-to-low-thousands, not per-message.

- [ ] **Step 4: Write the benchmark doc**

Create `docs/benchmarks/uc2-m1-gate-2026-07-09.md` with: the exact command, host description (CPU, disk, filesystem), the full program output from Step 3, and 2–3 sentences of interpretation (rate vs gate, block size distribution, where the time goes if it misses). This file is the M1 gate record; the fleet (c6id) run gets appended to it when performed.

- [ ] **Step 5: Full workspace gates and final commit**

```bash
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo test -p uc_log
cargo test -p uc_protocol
git add uc_log/examples/m1_gate.rs docs/benchmarks/uc2-m1-gate-2026-07-09.md
git commit -m "feat(uc_log): m1_gate throughput example + gate measurement doc"
```

---

## Self-review notes (already applied)

- Spec §4 coverage: frame layout (T1), positions/counters (T2), single-writer ring + padding + overrun gate (T3), validated reads for lagging readers (T4), mmap instance files (T5), block archive + fsync-per-block + durable counter (T6), recovery + replay-from-position via binary search (T7), agents (T8), M1 gate (T9). Term map, truncation, and purge are M4/M6 per the spec's milestone split — deliberately absent here (`PositionPurged` exists as the replay contract's error arm).
- The validated-read margin (`max_claim`) exists because an in-flight append's writes are not yet visible in the `append` counter; the conservative bound is 2× the max aligned frame (padding is only emitted when the frame doesn't fit before the wrap, so padding < one max frame). The loom test models commit-word/counter ordering; the margin is covered by the deterministic wrap test.
- Known journal-API risk called out in Task 6 Step 4 (first-seq convention); the test fails loudly if the assumption is wrong.
