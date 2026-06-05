# Event-Driven Ring Wakeups — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace poll-sleep on the four commit-path shmem rings with futex-based wakeups so the inflight=1 latency floor (~4.5 ms, almost all of it idle-backoff wait — tokio's ~1 ms timer granularity inflates each 100 µs `sleep`) collapses toward the underlying hand-off cost.

**Architecture:** A wakeup word reusing each ring's existing `publish_position` low-32 bits, plus a `waiters: AtomicU32` reclaimed from `RingHeader` pad (no protocol bump). Producers `signal()` (cross-process `FUTEX_WAKE`) on publish, gated by `waiters > 0`. Sync consumers `read_or_park` (futex wait with arm-then-recheck — no lost wakeups). Async consumers (tokio current_thread) use a parker OS thread that blocks on the futex and fires a `tokio::sync::Notify`. A timeout backstop makes poll-sleep the worst case, never a hang. A `ParkMode` seam keeps a `Poll` fallback for tests/portability.

**Tech Stack:** Rust, `libc` (cross-process `SYS_futex`, no `FUTEX_PRIVATE_FLAG`), existing `uc_protocol` ring buffers (SPSC/MPSC/Broadcast over mmap), `tokio` (`sync::Notify`), the `attribution-bench` harness for the success measurement.

**Scope:** commit-path hops only — submit (MPSC client→node), apply (SPSC node→service), apply_resp (SPSC service→node), response (Broadcast node→clients). Pipeline de-serialization and the query/read path are out of scope (deferred). Spec: `docs/superpowers/specs/2026-06-04-event-driven-ring-wakeups-design.md`.

---

## File Structure

**Phase 0 — primitive (uc_protocol)**
- Modify `uc_protocol/Cargo.toml` — add `libc` dependency.
- Modify `uc_protocol/src/ring/common.rs` — `waiters` field; `RingHeader` wakeup methods (`wake_word`, `current_seq`, `arm`, `disarm`, `park`, `signal`); `ParkMode`; `RingWaitHandle`; `PARK_CEIL` const.
- Create `uc_protocol/src/ring/futex.rs` — `futex_wait` / `futex_wake` (Linux) + `Poll` fallback.
- Modify `uc_protocol/src/ring/mod.rs` — `mod futex;` + re-exports (`ParkMode`, `RingWaitHandle`).

**Phase 1 — producer signal + SPSC apply slice**
- Modify `uc_protocol/src/ring/{spsc,mpsc,broadcast}.rs` — `mode` field on producer/consumer; fold `signal()` into the publish path; `read_or_park` on consumers; `wait_handle()` on consumers.
- Modify `uc_service/src/runtime/apply_loop.rs` — sync consumer parks.

**Phase 2 — async bridges**
- Create `uc_node/src/ipc/ring_bridge.rs` (and a sibling in uc_client) — `NotifyBridge`: parker thread → `Notify`.
- Modify `uc_node/src/ipc/client_dispatcher.rs` — submit-ring consumer via bridge.
- Modify `uc_node/src/raft/state_machine_shmem.rs` — apply_resp consumer via bridge.
- Modify `uc_client/src/rings.rs` — broadcast consumer via bridge.

**Phase 3 — shutdown, measurement, doc**
- Modify the bridge + consumer call sites — wake-on-stop.
- Create `docs/tasks/task11_event_driven_ring_wakeups.md`; capture new `bench-out/reference/attribution.csv`.

---

## Phase 0 — Wakeup primitive (TDD in isolation)

### Task 0.1: `waiters` field in `RingHeader` (no size change)

**Files:**
- Modify: `uc_protocol/src/ring/common.rs`

- [ ] **Step 1: Add the field, shrinking pad to keep size 256**

In `uc_protocol/src/ring/common.rs`, change the tail of `RingHeader` (currently `consumer_position: AtomicU64,` then `_pad_4: [u8; 56],`):

```rust
    pub consumer_position: AtomicU64,
    /// Count of consumers currently parked on this ring's wakeup word. Written
    /// by the consumer side (same cache line as `consumer_position`), read by
    /// the producer to skip the `FUTEX_WAKE` syscall when nobody is parked.
    /// Reclaimed from `_pad_4` so `RING_HEADER_LEN` is unchanged (no protocol bump).
    pub waiters: AtomicU32,
    pub _pad_4: [u8; 52],
```

Add `AtomicU32` to the atomics import at the top of the file (find `use std::sync::atomic::{... AtomicU64 ...}` and add `AtomicU32`).

- [ ] **Step 2: Initialize it in `init_ring_header`**

In `init_ring_header`'s `RingHeader { ... }` literal (the `std::ptr::write(header_ptr, RingHeader { ... })` block), find `_pad_4: [0; 56],` and replace with:

```rust
                waiters: AtomicU32::new(0),
                _pad_4: [0; 52],
```

- [ ] **Step 3: Verify the size/align asserts still hold**

The existing `const _: () = { assert!(size_of::<RingHeader>() == 256); assert!(align_of::<RingHeader>() == 64); };` must still compile.

Run: `cargo build -p uc_protocol`
Expected: builds; the 256-byte const-assert passes (8 consumer_position + 4 waiters + 52 pad + 4 alignment-tail = 64; total 256).

- [ ] **Step 4: Commit**

```bash
git add uc_protocol/src/ring/common.rs
git commit -m "feat(protocol): RingHeader waiters field (reclaimed from pad, no size change)"
```

---

### Task 0.2: Cross-process futex wait/wake

**Files:**
- Modify: `uc_protocol/Cargo.toml`
- Create: `uc_protocol/src/ring/futex.rs`
- Modify: `uc_protocol/src/ring/mod.rs`

- [ ] **Step 1: Add `libc`**

In `uc_protocol/Cargo.toml` `[dependencies]`, add:

```toml
libc = "0.2"
```

- [ ] **Step 2: Write `futex.rs`**

Create `uc_protocol/src/ring/futex.rs`:

```rust
//! Cross-process futex wait/wake over a 32-bit word living in shared memory.
//!
//! The word is the low 32 bits of a ring's `publish_position` (see
//! `RingHeader::wake_word`). We deliberately do NOT pass `FUTEX_PRIVATE_FLAG`:
//! the client, node, and service are separate processes sharing the ring mmap,
//! so the futex must be a shared (inter-process) futex.

use std::sync::atomic::AtomicU32;
use std::time::Duration;

/// Block until `*word != expected`, a wake arrives, or `timeout` elapses.
/// Returns regardless of which (the caller re-checks state in a loop). All of
/// EAGAIN (value already changed), ETIMEDOUT, and EINTR collapse to "return".
#[cfg(target_os = "linux")]
pub fn futex_wait(word: &AtomicU32, expected: u32, timeout: Duration) {
    let ts = libc::timespec {
        tv_sec: timeout.as_secs() as libc::time_t,
        tv_nsec: timeout.subsec_nanos() as libc::c_long,
    };
    // SAFETY: `word` points into a live mmap; FUTEX_WAIT reads it atomically.
    // No FUTEX_PRIVATE_FLAG -> inter-process futex on the shared mapping.
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            word as *const AtomicU32 as *const u32,
            libc::FUTEX_WAIT,
            expected,
            &ts as *const libc::timespec,
        );
    }
}

/// Wake up to `n` consumers parked on `word` (`i32::MAX` == all).
#[cfg(target_os = "linux")]
pub fn futex_wake(word: &AtomicU32, n: i32) {
    // SAFETY: same shared mapping; FUTEX_WAKE only reads the address as a key.
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            word as *const AtomicU32 as *const u32,
            libc::FUTEX_WAKE,
            n,
        );
    }
}

/// Non-Linux fallback: no kernel wait primitive — callers rely on the timeout
/// backstop (the `Poll` `ParkMode`). These should not be reached on Linux.
#[cfg(not(target_os = "linux"))]
pub fn futex_wait(_word: &AtomicU32, _expected: u32, timeout: Duration) {
    std::thread::sleep(timeout);
}
#[cfg(not(target_os = "linux"))]
pub fn futex_wake(_word: &AtomicU32, _n: i32) {}
```

- [ ] **Step 3: Declare the module**

In `uc_protocol/src/ring/mod.rs`, add near the other `mod` lines:

```rust
mod futex;
```

- [ ] **Step 4: Unit test — one thread parks, another wakes**

Add to the bottom of `uc_protocol/src/ring/futex.rs`:

```rust
#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    #[test]
    fn wake_unblocks_a_parked_waiter() {
        let word = Arc::new(AtomicU32::new(0));
        let w2 = word.clone();
        let start = Instant::now();
        let h = std::thread::spawn(move || {
            // park expecting 0; the waker will store 1 then wake.
            futex_wait(&w2, 0, Duration::from_secs(5));
        });
        std::thread::sleep(Duration::from_millis(50));
        word.store(1, Ordering::Release);
        futex_wake(&word, 1);
        h.join().unwrap();
        assert!(start.elapsed() < Duration::from_secs(2), "wake should be prompt, not timeout");
    }

    #[test]
    fn wait_returns_immediately_when_value_already_changed() {
        let word = AtomicU32::new(7);
        let start = Instant::now();
        futex_wait(&word, 0, Duration::from_secs(5)); // expected!=actual -> EAGAIN, immediate
        assert!(start.elapsed() < Duration::from_millis(500));
    }

    #[test]
    fn wait_times_out_with_no_waker() {
        let word = AtomicU32::new(0);
        let start = Instant::now();
        futex_wait(&word, 0, Duration::from_millis(150));
        assert!(start.elapsed() >= Duration::from_millis(100));
    }
}
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p uc_protocol ring::futex`
Expected: PASS — `wake_unblocks_a_parked_waiter`, `wait_returns_immediately_when_value_already_changed`, `wait_times_out_with_no_waker`.

- [ ] **Step 6: Commit**

```bash
git add uc_protocol/Cargo.toml uc_protocol/src/ring/futex.rs uc_protocol/src/ring/mod.rs
git commit -m "feat(protocol): cross-process futex wait/wake primitive"
```

---

### Task 0.3: `RingHeader` wakeup methods + `ParkMode` + `RingWaitHandle`

**Files:**
- Modify: `uc_protocol/src/ring/common.rs`
- Modify: `uc_protocol/src/ring/mod.rs`

- [ ] **Step 1: Endianness guard + `PARK_CEIL`**

In `uc_protocol/src/ring/common.rs`, near the top (after imports), add:

```rust
// The wakeup word is the low 32 bits of `publish_position`; reinterpreting an
// `AtomicU64` as its low `AtomicU32` is only correct on little-endian targets
// (all Linux targets we run). Make it a hard compile error elsewhere.
#[cfg(not(target_endian = "little"))]
compile_error!("ring wakeup word assumes little-endian publish_position");

/// Upper bound on a single park; the timeout backstop. With wakeups working
/// this is never hit in steady state — it bounds the rare lost-wakeup race and
/// shutdown latency to the old poll-sleep behavior.
pub const PARK_CEIL: std::time::Duration = std::time::Duration::from_millis(2);

/// Spin-then-park: number of `try_read` spins before a sync consumer parks.
/// Catches an in-flight publish at ~zero latency without a syscall (Aeron-style
/// idle strategy); only after these fail do we arm + futex-wait.
pub const SPIN_TRIES: u32 = 64;
```

- [ ] **Step 2: `ParkMode` + `RingHeader` wakeup methods**

In `uc_protocol/src/ring/common.rs`, add (after the `RingHeader` struct):

```rust
/// Local (per-process) choice of wakeup mechanism. The shared-memory state
/// (`publish_position`, `waiters`) is identical either way; only how a consumer
/// blocks differs. `Futex` is the default on Linux; `Poll` is the portable
/// fallback and the test oracle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkMode {
    Futex,
    Poll,
}

impl Default for ParkMode {
    fn default() -> Self {
        if cfg!(target_os = "linux") { ParkMode::Futex } else { ParkMode::Poll }
    }
}

impl RingHeader {
    /// The 32-bit wakeup word: the low half of `publish_position`. Changes on
    /// every publish (modulo 2^32 wrap, which is benign for wait-and-recheck).
    #[inline]
    pub fn wake_word(&self) -> &std::sync::atomic::AtomicU32 {
        // SAFETY: little-endian (asserted above): the low 32 bits occupy the
        // first 4 bytes of the 8-byte, 8-aligned `publish_position`.
        unsafe {
            &*(&self.publish_position as *const AtomicU64 as *const std::sync::atomic::AtomicU32)
        }
    }

    /// Current value of the wakeup word (snapshot for a subsequent `park`).
    #[inline]
    pub fn current_seq(&self) -> u32 {
        self.publish_position.load(Ordering::Acquire) as u32
    }

    /// Register a parked consumer (before parking).
    #[inline]
    pub fn arm(&self) {
        self.waiters.fetch_add(1, Ordering::AcqRel);
    }

    /// Unregister after waking.
    #[inline]
    pub fn disarm(&self) {
        self.waiters.fetch_sub(1, Ordering::AcqRel);
    }

    /// Producer-side wake: only syscalls if a consumer is parked. `all` wakes
    /// every waiter (Broadcast); otherwise wakes one (SPSC/MPSC).
    #[inline]
    pub fn signal(&self, mode: ParkMode, all: bool) {
        if self.waiters.load(Ordering::Acquire) == 0 {
            return;
        }
        match mode {
            ParkMode::Futex => {
                super::futex::futex_wake(self.wake_word(), if all { i32::MAX } else { 1 })
            }
            ParkMode::Poll => {}
        }
    }

    /// Consumer-side block until the wakeup word leaves `expected` or `timeout`.
    #[inline]
    pub fn park(&self, mode: ParkMode, expected: u32, timeout: std::time::Duration) {
        match mode {
            ParkMode::Futex => super::futex::futex_wait(self.wake_word(), expected, timeout),
            ParkMode::Poll => std::thread::sleep(timeout.min(PARK_CEIL)),
        }
    }
}
```

(`futex` is a sibling module of `common` under `ring`, hence `super::futex`. Ensure `mod.rs` declares `futex` before/around `common` — order does not matter for `mod` items.)

- [ ] **Step 3: `RingWaitHandle` (unifies the three ring types for the bridge)**

Add to `uc_protocol/src/ring/common.rs`:

```rust
use std::sync::Arc;

/// A cloneable handle that lets a parker thread block on a ring's wakeup word
/// while the owning (async) consumer reads. Holds an `Arc` keepalive so the
/// ring mmap outlives the handle, plus a raw `RingHeader` pointer into it.
/// Constructed by each consumer's `wait_handle()`.
pub struct RingWaitHandle {
    _keepalive: Arc<dyn std::any::Any + Send + Sync>,
    header: *const RingHeader,
    mode: ParkMode,
}

// SAFETY: `header` points into the mmap owned by `_keepalive` (kept alive for
// the handle's lifetime); all access goes through the `RingHeader` atomics.
unsafe impl Send for RingWaitHandle {}
unsafe impl Sync for RingWaitHandle {}

impl RingWaitHandle {
    /// Build from any ring `Inner` (held in an `Arc`) and its header pointer.
    /// `keepalive` and `header` MUST come from the same `Inner`.
    pub fn new(
        keepalive: Arc<dyn std::any::Any + Send + Sync>,
        header: *const RingHeader,
        mode: ParkMode,
    ) -> Self {
        Self { _keepalive: keepalive, header, mode }
    }
    #[inline]
    fn header(&self) -> &RingHeader {
        // SAFETY: valid for the handle's lifetime (keepalive holds the mmap).
        unsafe { &*self.header }
    }
    #[inline]
    pub fn current_seq(&self) -> u32 {
        self.header().current_seq()
    }
    #[inline]
    pub fn arm(&self) {
        self.header().arm()
    }
    #[inline]
    pub fn disarm(&self) {
        self.header().disarm()
    }
    #[inline]
    pub fn park(&self, expected: u32, timeout: std::time::Duration) {
        self.header().park(self.mode, expected, timeout)
    }
}
```

- [ ] **Step 4: Re-export from `mod.rs`**

In `uc_protocol/src/ring/mod.rs`, add to the public re-exports:

```rust
pub use common::{ParkMode, RingWaitHandle, PARK_CEIL, SPIN_TRIES};
```

- [ ] **Step 5: Build**

Run: `cargo build -p uc_protocol`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add uc_protocol/src/ring/common.rs uc_protocol/src/ring/mod.rs
git commit -m "feat(protocol): RingHeader wakeup ops + ParkMode + RingWaitHandle"
```

---

## Phase 1 — Producer signal + SPSC apply slice

### Task 1.1: `mode` field, fold `signal()` into producers, add `read_or_park` + `wait_handle` (SPSC)

**Files:**
- Modify: `uc_protocol/src/ring/spsc.rs`

- [ ] **Step 1: Add `mode` to the producer/consumer and thread it through `into_split`**

In `uc_protocol/src/ring/spsc.rs`:

- Add `use crate::ring::common::{ParkMode, RingWaitHandle, PARK_CEIL, SPIN_TRIES};` to the imports (extend the existing `use crate::ring::common::{...}`).
- Add a field to `SpscProducer`: `mode: ParkMode,` and to `SpscConsumer`: `mode: ParkMode,`.
- In `SpscRing::into_split`, set `mode: ParkMode::default()` on both halves:

```rust
    pub fn into_split(self) -> (SpscProducer, SpscConsumer) {
        (
            SpscProducer {
                inner: self.inner.clone(),
                cached_consumer_pos: 0,
                mode: ParkMode::default(),
            },
            SpscConsumer { inner: self.inner, mode: ParkMode::default() },
        )
    }
```

- [ ] **Step 2: Fold `signal()` into the real-record publish in `try_write`**

In `SpscProducer::try_write`, the real-record publish is `header.publish_position.store(new_pos, Ordering::Release);` immediately before `Ok(())` (currently spsc.rs:187). Change those two lines to:

```rust
        let new_pos = producer_pos + advance as u64;
        header.publish_position.store(new_pos, Ordering::Release);
        header.signal(self.mode, false); // SPSC: wake the single consumer
        Ok(())
```

(Do NOT add `signal()` to the padding-marker store — padding carries no record; the consumer skips it and re-parks.)

- [ ] **Step 3: Add `wait_handle()` and `read_or_park()` to `SpscConsumer`**

Add to `impl SpscConsumer`:

```rust
    /// Handle for a parker thread to block on this ring while the owner reads.
    pub fn wait_handle(&self) -> RingWaitHandle {
        RingWaitHandle::new(self.inner.clone(), self.inner.header(), self.mode)
    }

    /// Blocking read: returns a record, or `Ok(None)` only after parking up to
    /// `PARK_CEIL` with nothing available. Arm-then-recheck closes the
    /// lost-wakeup race: we snapshot the wakeup word, register as a waiter,
    /// re-check the ring, and only then park on the snapshot — if the producer
    /// published in between, `publish_position != seq` and the futex returns
    /// immediately. For SYNC (std::thread) consumers only.
    pub fn read_or_park(
        &mut self,
        payload_buf: &mut Vec<u8>,
    ) -> Result<Option<RecordHeader>, RingError> {
        // Spin-then-park: catch an in-flight publish without a syscall first.
        for _ in 0..SPIN_TRIES {
            if let Some(rec) = self.try_read(payload_buf)? {
                return Ok(Some(rec));
            }
            std::hint::spin_loop();
        }
        let seq = self.inner.header().current_seq();
        self.inner.header().arm();
        let recheck = self.try_read(payload_buf);
        if !matches!(recheck, Ok(None)) {
            self.inner.header().disarm();
            return recheck;
        }
        self.inner.header().park(self.mode, seq, PARK_CEIL);
        self.inner.header().disarm();
        self.try_read(payload_buf)
    }
```

`self.inner.header()` is a private method on `SpscInner` already used throughout this file — it is in scope here.

- [ ] **Step 4: Lost-wakeup stress test (SPSC, both ParkModes)**

Add to `uc_protocol/src/ring/spsc.rs`'s `#[cfg(test)] mod tests`:

```rust
    fn lost_wakeup_stress(mode: ParkMode) {
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 4096, 256).expect("create");
        let (mut producer, mut consumer) = ring.into_split();
        producer.mode = mode;
        consumer.mode = mode;

        let n = 2000u32;
        let h = std::thread::spawn(move || {
            for i in 0..n {
                let payload = i.to_le_bytes();
                loop {
                    match producer.try_write(2, 0, [0; 8], &payload) {
                        Ok(()) => break,
                        Err(RingError::Full) => std::thread::yield_now(),
                        Err(e) => panic!("{e}"),
                    }
                }
                if i % 7 == 0 {
                    std::thread::sleep(std::time::Duration::from_micros(50));
                }
            }
        });

        let mut got = 0u32;
        let mut buf = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while got < n {
            assert!(std::time::Instant::now() < deadline, "stalled at {got}/{n} ({mode:?})");
            if let Some(_rec) = consumer.read_or_park(&mut buf).expect("read") {
                let v = u32::from_le_bytes(buf.as_slice().try_into().unwrap());
                assert_eq!(v, got, "ordering ({mode:?})");
                got += 1;
            }
        }
        h.join().unwrap();
    }

    #[test]
    fn lost_wakeup_stress_futex() {
        lost_wakeup_stress(ParkMode::Futex);
    }

    #[test]
    fn lost_wakeup_stress_poll() {
        lost_wakeup_stress(ParkMode::Poll);
    }

    #[test]
    fn read_or_park_times_out_when_empty() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 4096, 256).expect("create");
        let (_producer, mut consumer) = ring.into_split();
        let mut buf = Vec::new();
        let start = std::time::Instant::now();
        let r = consumer.read_or_park(&mut buf).expect("read");
        assert!(r.is_none());
        assert!(start.elapsed() >= std::time::Duration::from_millis(1)); // parked ~PARK_CEIL
    }
```

Add `use crate::ring::common::ParkMode;` to the test module if not already in scope via `use super::*;` (it is, since `ParkMode` is imported at file scope in Step 1).

- [ ] **Step 5: Run tests (both modes prove the seam)**

Run: `cargo test -p uc_protocol ring::spsc`
Expected: PASS — including `lost_wakeup_stress_futex`, `lost_wakeup_stress_poll`, `read_or_park_times_out_when_empty`, and the pre-existing SPSC tests. Run a few times to shake the race:
`for i in 1 2 3; do cargo test -p uc_protocol ring::spsc -- --test-threads=2 || break; done`

- [ ] **Step 6: Commit**

```bash
git add uc_protocol/src/ring/spsc.rs
git commit -m "feat(protocol): SPSC producer signal + read_or_park/wait_handle + lost-wakeup tests"
```

---

### Task 1.2: Service apply loop parks (sync consumer — vertical slice)

**Files:**
- Modify: `uc_service/src/runtime/apply_loop.rs`

- [ ] **Step 1: Replace the idle sleep with `read_or_park`**

In `uc_service/src/runtime/apply_loop.rs`, the consume loop currently matches `consumer.try_read(&mut payload_buf)` with `Ok(None) => std::thread::sleep(IDLE_BACKOFF)` (apply_loop.rs:112). Change the match scrutinee from `consumer.try_read(&mut payload_buf)` to `consumer.read_or_park(&mut payload_buf)` and make the `Ok(None)` arm a no-op `continue` (the park already waited):

```rust
        match consumer.read_or_park(&mut payload_buf) {
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_APPLY => {
                // ... unchanged body ...
            }
            Ok(Some(rec)) => {
                tracing::warn!(msg_type = rec.msg_type, "apply ring: unexpected frame");
            }
            Ok(None) => {} // parked up to PARK_CEIL; loop re-checks stop flag
            Err(e) => {
                tracing::warn!(error = %e, "apply ring read error");
                std::thread::sleep(ERROR_BACKOFF);
            }
        }
```

`IDLE_BACKOFF` is now unused — delete its `const IDLE_BACKOFF` line (apply_loop.rs:35) to avoid a dead-code warning.

- [ ] **Step 2: Build both probe modes (apply_loop is in the probe path)**

Run: `cargo build -p uc_service`
Run: `cargo build -p uc_service --features uc_protocol/uc-bench-probes`
Expected: both clean.

- [ ] **Step 3: Existing shmem tests still pass (apply path exercised end-to-end)**

Run: `cargo test -p uc_node --test m3_shmem_single_node`
Expected: PASS (2 tests) — the apply ring now wakes via futex; behavior identical.

Run: `cargo test -p uc_node --test m3_shutdown_dead_service`
Expected: PASS — service parked on apply ring still shuts down (the consumer loop checks its stop flag each PARK_CEIL; full wake-on-stop comes in Phase 3, the backstop covers it now).

- [ ] **Step 4: Commit**

```bash
git add uc_service/src/runtime/apply_loop.rs
git commit -m "feat(service): apply_loop parks on the apply ring instead of poll-sleep"
```

---

## Phase 2 — Async-bridged consumers

### Task 2.1: `NotifyBridge` — parker thread → tokio `Notify`

**Files:**
- Create: `uc_node/src/ipc/ring_bridge.rs`
- Modify: `uc_node/src/ipc/mod.rs`

- [ ] **Step 1: Write the bridge**

Create `uc_node/src/ipc/ring_bridge.rs`:

```rust
//! Bridges a blocking ring futex-park to an async consumer.
//!
//! A `current_thread` tokio task cannot call the blocking `FUTEX_WAIT` without
//! stalling its runtime, so one dedicated OS thread parks on the ring's wakeup
//! word and fires a `tokio::sync::Notify` whenever the word changes (or every
//! `PARK_CEIL` as a backstop). The async consumer loops:
//! `match try_read { Some => .., None => bridge.notified().await }`.
//!
//! Lost-wakeup bound: if a publish lands in the gap between the consumer's
//! `try_read == None` and the parker's snapshot, the parker waits for the NEXT
//! change and the consumer is re-notified within `PARK_CEIL` (the backstop).
//! Correctness never depends on the wake; only sub-`PARK_CEIL` latency does.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;
use uc_protocol::ring::{PARK_CEIL, RingWaitHandle};

pub struct NotifyBridge {
    notify: Arc<Notify>,
    stop: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl NotifyBridge {
    /// Spawn the parker thread for `handle`. `name` is for diagnostics.
    pub fn spawn(handle: RingWaitHandle, name: &'static str) -> Self {
        let notify = Arc::new(Notify::new());
        let stop = Arc::new(AtomicBool::new(false));
        let n = notify.clone();
        let s = stop.clone();
        let join = std::thread::Builder::new()
            .name(format!("ring-park-{name}"))
            .spawn(move || {
                handle.arm();
                while !s.load(Ordering::Acquire) {
                    let seq = handle.current_seq();
                    handle.park(seq, PARK_CEIL);
                    n.notify_one();
                }
                handle.disarm();
            })
            .expect("spawn ring parker thread");
        Self { notify, stop, join: Some(join) }
    }

    /// Await the next wakeup (or a stored permit if one is pending).
    pub async fn notified(&self) {
        self.notify.notified().await;
    }

    /// Stop the parker thread and join it. Idempotent.
    pub fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.notify.notify_one(); // unblock any awaiter
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

impl Drop for NotifyBridge {
    fn drop(&mut self) {
        self.shutdown();
    }
}
```

- [ ] **Step 2: Declare the module**

In `uc_node/src/ipc/mod.rs`, add:

```rust
pub(crate) mod ring_bridge;
```

- [ ] **Step 3: Build**

Run: `cargo build -p uc_node --features test-support`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/ipc/ring_bridge.rs uc_node/src/ipc/mod.rs
git commit -m "feat(node): NotifyBridge (ring futex-park -> tokio Notify)"
```

---

### Task 2.2: `signal()` for MPSC + Broadcast producers; `wait_handle()` for their consumers

**Files:**
- Modify: `uc_protocol/src/ring/mpsc.rs`
- Modify: `uc_protocol/src/ring/broadcast.rs`

- [ ] **Step 1: MPSC — `mode` field, signal on publish, `wait_handle()`**

In `uc_protocol/src/ring/mpsc.rs`:
- Extend the `use crate::ring::common::{...}` import with `ParkMode, RingWaitHandle`.
- Add `mode: ParkMode` to `MpscProducer` and `MpscConsumer`; set `ParkMode::default()` wherever the split/halves are constructed (mirror SPSC `into_split`).
- After the real-record publish `header.publish_position.store(target_pos, Ordering::Release);` (mpsc.rs:192), add:

```rust
            header.signal(self.mode, false); // MPSC: single consumer -> wake one
```

(Not after any padding-marker store.)
- Add to `impl MpscConsumer`:

```rust
    pub fn wait_handle(&self) -> RingWaitHandle {
        RingWaitHandle::new(self.inner.clone(), self.inner.header(), self.mode)
    }
```

- [ ] **Step 2: Broadcast — `mode` field, wake-all on publish, `wait_handle()`**

In `uc_protocol/src/ring/broadcast.rs`:
- Extend the `use crate::ring::common::{...}` import with `ParkMode, RingWaitHandle`.
- Add `mode: ParkMode` to `BroadcastProducer` and `BroadcastConsumer`; default on construction.
- After the real-record publish `header.publish_position.store(new_pos, Ordering::Release);` (broadcast.rs:119), add:

```rust
        header.signal(self.mode, true); // Broadcast: wake ALL parked consumers
```

- Add to `impl BroadcastConsumer`:

```rust
    pub fn wait_handle(&self) -> RingWaitHandle {
        RingWaitHandle::new(self.inner.clone(), self.inner.header(), self.mode)
    }
```

- [ ] **Step 3: Broadcast wake-all test**

Add to `uc_protocol/src/ring/broadcast.rs` tests (use the existing test setup pattern in that file for creating a `BroadcastRing` and consumers):

```rust
    #[test]
    fn wake_all_unblocks_two_consumers() {
        use crate::ring::common::ParkMode;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 4096, 256).expect("create");
        let mut producer = ring.producer();
        producer.mode = ParkMode::Futex;
        let mk = || {
            let mut c = ring.consumer();
            c.mode = ParkMode::Futex;
            c.wait_handle()
        };
        let (h1, h2) = (mk(), mk());
        let done = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let mut threads = Vec::new();
        for h in [h1, h2] {
            let d = done.clone();
            threads.push(std::thread::spawn(move || {
                let seq = h.current_seq();
                h.arm();
                h.park(seq, std::time::Duration::from_secs(5));
                h.disarm();
                d.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            }));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        producer.write(1, 0, [0; 8], b"x").expect("write"); // signals wake-all
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(done.load(std::sync::atomic::Ordering::Acquire), 2);
    }
```

(Adjust `ring.producer()` / `ring.consumer()` to the actual constructor names in `broadcast.rs` — use whatever the existing tests in that file call.)

- [ ] **Step 4: Run + build**

Run: `cargo test -p uc_protocol ring::broadcast ring::mpsc`
Expected: PASS including `wake_all_unblocks_two_consumers`.

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/ring/mpsc.rs uc_protocol/src/ring/broadcast.rs
git commit -m "feat(protocol): MPSC wake-one + Broadcast wake-all signals + wait_handles"
```

---

### Task 2.3: apply_resp consumer via bridge (node `await_apply_resp`)

**Files:**
- Modify: `uc_node/src/raft/state_machine_shmem.rs`

- [ ] **Step 1: Build the bridge for the apply_resp consumer**

`await_apply_resp` loops reading the `apply_resp_consumer` (an `&PlMutex<SpscConsumer>`). It currently does `Ok(None) => tokio::time::sleep(EMPTY_BACKOFF).await` (state_machine_shmem.rs:432). We give the apply path a `NotifyBridge` keyed off the consumer's `wait_handle`. The consumer lives in `ShmemInner.apply_resp_consumer`. Add a bridge alongside it.

In `ShmemInner`, add a field:

```rust
    pub(crate) apply_resp_bridge: Arc<uc_node::ipc::ring_bridge::NotifyBridge>,
```

Wait — `state_machine_shmem` is inside `uc_node`; use the crate-local path `crate::ipc::ring_bridge::NotifyBridge`. Field:

```rust
    pub(crate) apply_resp_bridge: Arc<crate::ipc::ring_bridge::NotifyBridge>,
```

In `ShmemAdaptedStateMachine::new`, after the `apply_resp_consumer` is available, build the bridge from its wait handle. `apply_resp_consumer` is moved into `ShmemInner` via `PlMutex::new(apply_resp_consumer)`; take the handle before moving:

```rust
        let apply_resp_bridge = Arc::new(crate::ipc::ring_bridge::NotifyBridge::spawn(
            apply_resp_consumer.wait_handle(),
            "apply_resp",
        ));
```

then include `apply_resp_bridge,` in the `ShmemInner { ... }` literal.

- [ ] **Step 2: Use the bridge in `await_apply_resp`**

`await_apply_resp` takes `consumer: &PlMutex<SpscConsumer>`. Add a `bridge: &NotifyBridge` parameter and replace the empty-backoff sleep. Its caller is `apply()` (`await_apply_resp(&g.apply_resp_consumer, log_index, log_id, &shutdown)`). Change the signature and the `Ok(None)` arm:

```rust
async fn await_apply_resp(
    consumer: &PlMutex<SpscConsumer>,
    expected_log_index: u64,
    log_id: RaftLogId,
    shutdown: &AtomicBool,
    bridge: &crate::ipc::ring_bridge::NotifyBridge,
) -> Result<Bytes, io::Error> {
    // ... existing shutdown-check + loop preamble unchanged ...
            Ok(None) => bridge.notified().await,
    // ...
}
```

And at the call site in `apply()`:

```rust
                    let resp = await_apply_resp(
                        &g.apply_resp_consumer,
                        log_index,
                        log_id,
                        &shutdown,
                        &g.apply_resp_bridge,
                    )
                    .await?;
```

`g` is the `ShmemInner` guard, so `g.apply_resp_bridge` is in scope.

- [ ] **Step 2b: Signal apply_resp from the service side**

The producer of `apply_resp` is the service (`uc_service`). It already uses an `SpscProducer::try_write` (Task 1.1 folded `signal()` into that), so no extra change — the service publishing a response now wakes the node's parker thread automatically.

- [ ] **Step 3: Build both modes**

Run: `cargo build -p uc_node --features test-support`
Run: `cargo build -p uc_node --features test-support,uc_protocol/uc-bench-probes`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/raft/state_machine_shmem.rs
git commit -m "feat(node): apply_resp consumer wakes via NotifyBridge"
```

---

### Task 2.4: submit consumer (node `client_dispatcher`) + broadcast consumer (uc_client)

**Files:**
- Modify: `uc_node/src/ipc/client_dispatcher.rs`
- Modify: `uc_client/src/rings.rs`
- Modify: `uc_client/Cargo.toml` (+ a client-side bridge module)
- Create: `uc_client/src/ring_bridge.rs`

- [ ] **Step 1: Submit ring — bridge in `client_dispatcher`**

`spawn_client_dispatcher` owns the `submit_consumer` (an `MpscConsumer`) inside the spawned task. Before the loop, build a bridge from its `wait_handle()`:

```rust
    let submit_bridge = crate::ipc::ring_bridge::NotifyBridge::spawn(
        submit_consumer.wait_handle(),
        "submit",
    );
```

Then change the empty arm `Ok(None) => tokio::time::sleep(POLL_BACKOFF).await,` (client_dispatcher.rs:126) to:

```rust
                Ok(None) => submit_bridge.notified().await,
```

On loop exit (after the `while !stop...` loop), drop/shutdown the bridge: it drops automatically when `submit_bridge` goes out of scope at task end (its `Drop` joins the parker thread). The producers (clients) already `signal()` via the folded MPSC write.

- [ ] **Step 2: Client-side bridge module**

`uc_client` needs its own copy of the bridge (it can't depend on `uc_node`). Create `uc_client/src/ring_bridge.rs` with the SAME content as `uc_node/src/ipc/ring_bridge.rs` from Task 2.1 Step 1 (identical code; the type is small and crate-local). Add `mod ring_bridge;` to `uc_client/src/lib.rs`. Ensure `tokio` (with `sync`) is a dependency of `uc_client` (it already uses `tokio::sync::oneshot`, so `sync` is enabled).

- [ ] **Step 3: Broadcast ring — bridge in the client reader**

In `uc_client/src/rings.rs`, the broadcast reader loop has `Ok(None) => tokio::time::sleep(Duration::from_micros(100)).await` (rings.rs:117). Before the loop, build a bridge from the broadcast consumer's `wait_handle()`:

```rust
    let bcast_bridge = crate::ring_bridge::NotifyBridge::spawn(consumer.wait_handle(), "broadcast");
```

(Use the actual local name of the broadcast `BroadcastConsumer` in that function.) Change the empty arm to:

```rust
                Ok(None) => bcast_bridge.notified().await,
```

The node already `signal()`s wake-all via the folded `BroadcastProducer::write` (Task 2.2). Leave the separate `paused_for_task` 10 ms sleep (rings.rs:87) as-is — it is a pause control, not the empty-poll.

- [ ] **Step 4: Build**

Run: `cargo build -p uc_node --features test-support`
Run: `cargo build -p uc_client`
Run: `cargo build -p uc_client --features uc_protocol/uc-bench-probes`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/ipc/client_dispatcher.rs uc_client/src/rings.rs uc_client/src/ring_bridge.rs uc_client/src/lib.rs uc_client/Cargo.toml
git commit -m "feat(node,client): submit + broadcast consumers wake via NotifyBridge"
```

---

## Phase 3 — Shutdown, measurement, doc

### Task 3.1: Shutdown wakes parked consumers promptly + full-suite green

**Files:**
- Modify: `uc_node/src/runtime/node.rs`
- Modify: `uc_service/src/runtime/apply_loop.rs`

- [ ] **Step 1: Service apply loop — bound stop latency**

The apply loop already re-checks its stop flag each `PARK_CEIL` (≤2 ms) via `read_or_park`, so it stops within ~2 ms. No code change required; confirm the loop condition checks `stop` each iteration (it does — `while !stop.load(...)`).

- [ ] **Step 2: Node — bridges drop on shutdown**

The `NotifyBridge` instances live in their owning task/struct scopes (`submit_bridge` in the dispatcher task; `apply_resp_bridge` in `ShmemInner`). Each `Drop` sets stop + joins its parker thread (bounded by `PARK_CEIL`). The `ShmemInner` (and thus `apply_resp_bridge`) is dropped in `node.shutdown()` at `drop(sm)` (node.rs:392). Confirm no change needed; add a one-line comment at `drop(sm)`:

```rust
        // Dropping the SM adapter also drops apply_resp_bridge -> its parker
        // thread is stopped and joined (bounded by PARK_CEIL).
        drop(sm);
```

- [ ] **Step 3: Full shutdown + cluster suite green (default + parallel)**

Run: `cargo test --workspace -- --test-threads=1`
Expected: all green — especially `m3_shutdown_dead_service`, `m3_service_crash`, `m3_shmem_single_node`, `m2_multi_node`, `m4_*`, `m5_*`.

Run (default parallel, m2 serialization holds): `cargo test --workspace`
Expected: green, no hangs.

- [ ] **Step 4: Clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: zero warnings (the futex `unsafe` blocks have SAFETY comments; no dead `*_BACKOFF` consts remain — delete any left unused).

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/runtime/node.rs uc_service/src/runtime/apply_loop.rs
git commit -m "feat(node): document bridge shutdown ordering; suite green under wakeups"
```

---

### Task 3.2: Measure — attribution diff + new reference

**Files:**
- Modify: `bench-out/reference/attribution.csv`

- [ ] **Step 1: Re-run attribution at inflight=1 and inflight=8 (disk + tmpfs)**

```bash
mkdir -p /tmp/wk
TMPDIR=/home/$(whoami)/uc-bench-data cargo run -p uc_autobench --features uc-bench-probes \
  --bin attribution-bench --release -- --config single_disk --inflight 1 --count 5000 --out /tmp/wk/disk_if1.csv
cargo run -p uc_autobench --features uc-bench-probes \
  --bin attribution-bench --release -- --config single_tmpfs --inflight 1 --count 5000 --out /tmp/wk/tmpfs_if1.csv
```

- [ ] **Step 2: Verify the floor collapsed**

Run: `awk -F, '$5=="total"||$5=="submit_to_node"{print FILENAME,$1,$5,$7}' /tmp/wk/*_if1.csv`
Expected: `submit_to_node` and `total` p99 at inflight=1 drop from ~2.1 ms / ~4.5 ms toward sub-millisecond (tens–hundreds of µs per hop). If they did NOT drop, STOP — a bridge isn't wired (check each consumer actually calls `notified().await`, and each producer's ring is in `Futex` mode).

- [ ] **Step 3: Refresh the committed reference (disk + tmpfs, inflight=8 to match the old shape, plus inflight=1)**

```bash
TMPDIR=/home/$(whoami)/uc-bench-data cargo run -p uc_autobench --features uc-bench-probes \
  --bin attribution-bench --release -- --config single_disk --inflight 8 --count 5000 --out /tmp/wk/disk_if8.csv
cargo run -p uc_autobench --features uc-bench-probes \
  --bin attribution-bench --release -- --config single_tmpfs --inflight 8 --count 5000 --out /tmp/wk/tmpfs_if8.csv
{ head -1 /tmp/wk/disk_if8.csv; tail -n+2 /tmp/wk/disk_if8.csv; tail -n+2 /tmp/wk/tmpfs_if8.csv; } > bench-out/reference/attribution.csv
```

- [ ] **Step 4: Commit**

```bash
git add bench-out/reference/attribution.csv
git commit -m "bench: refresh attribution reference after event-driven wakeups"
```

---

### Task 3.3: Consolidate task doc + delete superpowers artifacts

**Files:**
- Create: `docs/tasks/task11_event_driven_ring_wakeups.md`
- Delete: the spec + this plan (per CLAUDE.md feature workflow)

- [ ] **Step 1: Write `docs/tasks/task11_event_driven_ring_wakeups.md`**

Record: the wakeup mechanism (publish_position low-32 word + reclaimed `waiters`, no protocol bump); the `ParkMode` seam + `FutexParker`/`Poll`; the sync `read_or_park` arm-recheck vs the async `NotifyBridge` + timeout backstop; cross-process futex (no `FUTEX_PRIVATE_FLAG`); the per-ring wiring table; the before/after attribution numbers (inflight=1 floor: ~4.5 ms → measured); and what stayed deferred (pipeline de-serialization, query path, non-Linux parker). Note tokio's ~1 ms timer granularity as the reason the old 100 µs sleeps cost ~1 ms/hop.

- [ ] **Step 2: Delete the ephemeral artifacts**

```bash
git rm docs/superpowers/specs/2026-06-04-event-driven-ring-wakeups-design.md \
       docs/superpowers/plans/2026-06-05-event-driven-ring-wakeups.md
```

- [ ] **Step 3: Commit**

```bash
git add docs/tasks/task11_event_driven_ring_wakeups.md
git commit -m "docs(task11): consolidate event-driven ring wakeups + reference numbers"
```

---

## Final verification

- [ ] Default build clean: `cargo build --workspace` → clean.
- [ ] Probe build clean: `cargo build -p uc_node -p uc_service -p uc_client --features uc_protocol/uc-bench-probes` → clean.
- [ ] Primitive tests green (both modes): `cargo test -p uc_protocol ring::` → green; run 3× for the lost-wakeup race.
- [ ] Full suite green: `cargo test --workspace` (default) and `cargo test --workspace -- --test-threads=1` → both green, no hangs.
- [ ] Clippy: `cargo clippy --workspace --all-targets -- -D warnings` → zero warnings.
- [ ] Attribution shows the inflight=1 latency floor collapsed from ~4.5 ms toward sub-millisecond, captured in `bench-out/reference/attribution.csv`; high-inflight queueing (the deferred pipeline work) now stands isolated.
```
