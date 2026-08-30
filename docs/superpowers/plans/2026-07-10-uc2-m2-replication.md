# UC v2 M2 — `uc_net` Replication Data Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The UDP replication stream (spec §5): a leader's sender agent fans the log buffer out to followers MTU-datagram by MTU-datagram; follower receivers rebuild the stream position-addressed into their own buffers, archive it durably, recover loss via NAK, and pace the sender with quorum-order-statistic flow control — gate: ≥100 MB/s per follower with durable positions keeping pace, resilient to 0.1–1 % injected loss.

**Architecture:** New crate `uc_net` (sender/receiver agents, fault-injecting UDP socket, flow/gap/NAK state) + small additions to `uc_log` (the `sent` counter, a batch validated read for the sender, a position-addressed writer for the receiver) + new core-only wire layouts in `uc_protocol::v2::datagram`. Everything is polling agents over position counters — no channels, locks, or timers on the data path. The log buffer is the retransmit buffer: NAKs are served by re-reading it.

**Tech Stack:** Rust 2024, std `UdpSocket` (nonblocking; no libc/quinn/tokio), existing `uc_log` (M1) + `uc_protocol::v2::frame`, `tempfile` (dev).

## Global Constraints

Every task's requirements implicitly include all of these (they extend the M1 plan's constraints, which remain in force for any `uc_log`/`uc_protocol` change):

- **Wire layouts live in `uc_protocol::v2`, core-only.** No `std`, no `serde`, no atomics in `uc_protocol` (spec §3.2: the multi-language gate). Fixed-size little-endian fields, exact offsets, verbatim from this plan.
- **Frame layout is frozen (M1):** `FRAME_ALIGNMENT = 32`, `HEADER_LEN = 32`, commit word = u32 `length` at offset 0, `FRAME_TYPE_MESSAGE = 1`, `FRAME_TYPE_PADDING = 2`. Positions are absolute u64 byte offsets, monotonic forever; ring offset = `position & (capacity − 1)`.
- **Datagram MTU default 1408 bytes** (spec §5), header 16 bytes ⇒ 1392 bytes of frame payload per datagram. **A max-size frame must fit one datagram**: `align_frame_len(HEADER_LEN + max_payload) + DATAGRAM_HEADER_LEN ≤ mtu`, asserted at sender construction. Larger payloads are the spec's jumbo-frame knob (raise mtu), never fragmentation.
- **The overrun rule is unchanged and extends to followers:** the appender (leader) / positioned writer (follower) never overwrites bytes the local archive hasn't recorded. Every *other* lagging reader degrades — in M2 that surfaces as `Overrun` from validated reads and an `overruns` stat; the journal-replay session that consumes it is **deferred to M4** (see Non-goals).
- **Copy-before-send is mandatory.** There is no CRC on the wire (spec §4: CRC is per journal block) — sending directly out of the live ring could transmit bytes overwritten mid-syscall as silently corrupt frames. The sender always copies frames out via a validated read, re-validates, then sends. At the gate's 100 MB/s the extra memcpy is noise.
- **Buffer capacity is identical on every node** (static config; offsets computed from absolute positions must agree). Asserted where both ends are visible (harness/example); a runtime handshake is M3's problem.
- **No timers/linger anywhere on the data path.** Batching is structural (backlog-formed): the sender packs whatever whole frames accumulated, up to MTU. Time-based machinery is control-plane only (NAK delay, status floor, heartbeats) and runs at ≤ kHz.
- **Control routing intra-process uses `std::sync::mpsc::sync_channel`** (bounded, try_send/try_recv, messages are `Copy`). This is a documented stand-in with the same shape as the cnc SPSC ring that replaces it when protocol-v2 IPC lands (M5). Control messages are single-digit kHz (spec §6); dropping one on a full channel is safe by design (NAK re-fires after backoff, status re-sends on the floor).
- **`Durability::Consistent`** for every journal; test journals use 4 MiB segments (`test_cfg` pattern from M1 — parallel `cargo test` on this box's quota'd tmpfs) and keep total test data small (< ~30 MB per test binary).
- **Gate/example runs put journal dirs on ext4 under `/home/claude`, NEVER `/tmp`** (RAM-backed tmpfs with an ~840 MiB per-user quota on this box). Bounded runs via a `UC2_M2_MAX_BYTES` env cap, mirroring M1's `UC2_M1_MAX_MSGS`.
- SPDX headers (`Apache-2.0`, `Copyright 2026 Peter Knego`) on every new file. `cargo clippy --workspace -- -D warnings` must pass after every task. `cargo fmt --check` fails workspace-wide pre-existing (rustfmt version mismatch) — do NOT reformat out-of-scope files; match neighboring style by hand.
- Implementers stage ONLY their own task's files (never `git add -A`).

**Non-goals (M2), stated so nobody "helpfully" adds them:** replay sessions (journal-read catch-up streams — M4, the `Overrun`/`overruns` seam is their entry point); elections/votes/commit gossip (M3/M4 — type codes reserved here); `sendmmsg` batching (M3 optimization if the gate demands it); wire encryption/auth (spec §5 stated posture; the reserved header slot is the future PSK-MAC home); buffer prefill on restart (M4/M6); a `Drop`/second-`Appender` guard on `LogBuffer` (M3, per final M1 review triage).

---

### Task 1: M1 carry-fixes — restart read guard + `AgentRunner` Drop

The final M1 whole-branch review flagged these as **required first commits** for M2. Both are in `uc_log`.

**Files:**
- Modify: `uc_log/src/buffer.rs` (read guard in `read_frame_validated`, ~line 200)
- Modify: `uc_log/src/counters.rs` (`prime()` doc contract)
- Modify: `uc_log/src/agent.rs` (`Drop` impl, duty-cycle rustdoc)

**Interfaces:**
- Consumes: M1's `LogBuffer::read_frame_validated`, `LogCounters::prime`, `AgentRunner`.
- Produces: `read_frame_validated` returns `FrameRead::Overrun` (never panics/garbage) for positions primed-over on a fresh buffer; `AgentRunner` joins its thread on `Drop`; `AgentRunner::stop(self)` unchanged (still panic-propagating).

- [ ] **Step 1: Write the failing test (read guard)**

Append to the `tests` module in `uc_log/src/buffer.rs`:

```rust
    #[test]
    fn primed_fresh_buffer_reads_overrun_not_garbage() {
        // Node restart: journal recovered to 2*CAP, buffer file recreated
        // (all zeros). Positions below the primed point exist only in the
        // journal — validated reads must degrade to Overrun (replay is the
        // fallback), not parse zeroed/stale bytes.
        let (b, c) = buf();
        c.prime(2 * CAP);
        let mut out = Vec::new();
        // Both positions pass the lap-overrun margin check (>= append +
        // max_claim - capacity = 8192 + 576 - 4096 = 4672) and previously
        // fell through to the zero commit word.
        assert!(matches!(b.read_frame_validated(2 * CAP - 64, &mut out), FrameRead::Overrun));
        assert!(matches!(b.read_frame_validated(4672, &mut out), FrameRead::Overrun));
        // Post-restart appends still read fine.
        let mut a = Appender::new(Arc::clone(&b), 5);
        a.append(1, 7, b"post-restart").unwrap();
        assert!(matches!(b.read_frame_validated(2 * CAP, &mut out), FrameRead::Frame(_)));
    }
```

- [ ] **Step 2: Run it — expect failure**

Run: `cargo test -p uc_log primed_fresh_buffer -- --nocapture`
Expected: FAIL — in debug the `debug_assert!(len >= 4 …)` at `buffer.rs:201` panics on the zero commit word.

- [ ] **Step 3: Implement the guard**

In `read_frame_validated`, immediately after `let len = self.commit_word(off).load(Ordering::Acquire) as usize;` and before the `debug_assert!`:

```rust
        if len == 0 {
            // A zero commit word below `append` means these bytes were never
            // written to THIS buffer file: the counters were primed past them
            // after a restart and the frames live only in the journal
            // (LogCounters::prime contract). Same remedy as a lap overrun.
            return FrameRead::Overrun;
        }
```

- [ ] **Step 4: Document the `prime()` contract**

Replace the doc comment on `LogCounters::prime` in `uc_log/src/counters.rs` with:

```rust
    /// Prime the counters after archive recovery (append resumes at durable —
    /// bytes beyond durable are discarded on restart, spec §6).
    ///
    /// CONTRACT: after priming over a FRESH (zeroed/recreated) buffer file,
    /// positions below `pos` have no bytes in the buffer — validated reads
    /// return `Overrun` and the journal is the only source, until a prefill
    /// mechanism exists (spec §4 "node restart", sized in M4/M6).
```

- [ ] **Step 5: Write the failing test (Drop)**

Append to the `tests` module in `uc_log/src/agent.rs`:

```rust
    #[test]
    fn drop_without_stop_signals_and_joins() {
        use std::time::{Duration, Instant};
        let count = Arc::new(AtomicU64::new(0));
        let c = Arc::clone(&count);
        let runner = AgentRunner::spawn("drop-agent", IdleStrategy::Yield, move || {
            c.fetch_add(1, Ordering::Relaxed);
            true
        })
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while count.load(Ordering::Relaxed) < 100 {
            assert!(Instant::now() < deadline, "agent never ran");
            std::thread::yield_now();
        }
        drop(runner); // must signal stop AND join — the thread is gone after this
        let n = count.load(Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(count.load(Ordering::Relaxed), n, "agent thread still running after drop");
    }
```

- [ ] **Step 6: Run it — expect failure**

Run: `cargo test -p uc_log drop_without_stop -- --nocapture`
Expected: FAIL (compile error is acceptable here only if you already started Step 7; otherwise the count keeps rising after `drop` because nothing stops the thread — the final `assert_eq!` fires).

- [ ] **Step 7: Implement Drop**

Rework `AgentRunner` in `uc_log/src/agent.rs` (the `JoinHandle` must become an `Option` so both `stop(self)` and `Drop` can take it):

```rust
pub struct AgentRunner {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl AgentRunner {
    /// Spawn a named agent thread looping `work()`; when `work` returns
    /// false (no work done), the idle strategy runs.
    ///
    /// CONTRACT: `work` is a DUTY CYCLE — it must do a bounded amount of work
    /// per call and return `true` iff it made progress. It must never block
    /// or loop internally waiting for input; that starves the stop flag and
    /// turns the idle strategy into a lie.
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
        Ok(AgentRunner { stop, handle: Some(handle) })
    }

    /// Signal stop and join; propagates a panic from the work closure.
    /// Prefer this over `drop` in teardown paths that must observe failures.
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.take().unwrap().join().expect("agent thread panicked");
    }
}

/// Dropping without `stop()` still signals and joins (no leaked busy-spinning
/// thread — the v1 SyncCore teardown lesson), but swallows a work-closure
/// panic to avoid a double panic during unwind.
impl Drop for AgentRunner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}
```

- [ ] **Step 8: Run the full crate suite**

Run: `cargo test -p uc_log && cargo clippy --workspace -- -D warnings`
Expected: all green (19 lib tests now), clippy clean.

- [ ] **Step 9: Commit**

```bash
git add uc_log/src/buffer.rs uc_log/src/counters.rs uc_log/src/agent.rs
git commit -m "fix(uc_log): restart-primed read guard + AgentRunner Drop joins (M1 final-review carry)"
```

---

### Task 2: `uc_protocol::v2::datagram` — datagram header + control bodies (core-only)

**Files:**
- Create: `uc_protocol/src/v2/datagram.rs`
- Modify: `uc_protocol/src/v2/mod.rs` (add `pub mod datagram;`)

**Interfaces:**
- Consumes: nothing (layout constants only; sibling of `v2::frame`).
- Produces (used by Tasks 5–10):
  - `DATAGRAM_HEADER_LEN: usize = 16`, `MTU_DEFAULT: usize = 1408`
  - `DGRAM_KIND_DATA = 1`, `DGRAM_KIND_HEARTBEAT = 2`, `DGRAM_KIND_NAK = 3`, `DGRAM_KIND_STATUS = 4` (u8; 5–8 reserved for M3/M4)
  - `struct DatagramHeader { position: u64, leadership_term_id: u32, kind: u8, flags: u8 }`
  - `write_datagram_header(&mut [u8], &DatagramHeader)` / `read_datagram_header(&[u8]) -> DatagramHeader`
  - `NAK_BODY_LEN = 16`, `struct NakBody { position: u64, length: u32 }`, `write_nak_body` / `read_nak_body`
  - `STATUS_BODY_LEN = 16`, `struct StatusBody { contiguous_position: u64, receive_window: u32 }`, `write_status_body` / `read_status_body`

- [ ] **Step 1: Write the failing tests**

Create `uc_protocol/src/v2/datagram.rs` with the tests first (module body comes in Step 3 — write the whole file in one go if you prefer, but run the tests before AND after to see them go red→green if you split):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip_and_offsets() {
        let h = DatagramHeader {
            position: 0xDEAD_BEEF_0000_0040,
            leadership_term_id: 9,
            kind: DGRAM_KIND_DATA,
            flags: 0x5a,
        };
        let mut buf = [0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(&mut buf, &h);
        assert_eq!(read_datagram_header(&buf), h);
        // reserved slot stays zero (future per-datagram PSK-MAC home, spec §5)
        assert_eq!(&buf[OFF_DGRAM_RESERVED..OFF_DGRAM_RESERVED + 2], &[0, 0]);
        // layout: position(8) term(4) kind(1) flags(1) reserved(2) = 16
        assert_eq!(OFF_DGRAM_POSITION, 0);
        assert_eq!(OFF_DGRAM_TERM_ID, 8);
        assert_eq!(OFF_DGRAM_KIND, 12);
        assert_eq!(OFF_DGRAM_FLAGS, 13);
        assert_eq!(OFF_DGRAM_RESERVED, 14);
        assert_eq!(DATAGRAM_HEADER_LEN, 16);
    }

    #[test]
    fn control_bodies_roundtrip() {
        let n = NakBody { position: 4096, length: 65536 };
        let mut buf = [0u8; NAK_BODY_LEN];
        write_nak_body(&mut buf, &n);
        assert_eq!(read_nak_body(&buf), n);

        let s = StatusBody { contiguous_position: 1 << 33, receive_window: 1 << 28 };
        let mut buf = [0u8; STATUS_BODY_LEN];
        write_status_body(&mut buf, &s);
        assert_eq!(read_status_body(&buf), s);
    }

    #[test]
    fn kind_codes_are_stable() {
        assert_eq!(DGRAM_KIND_DATA, 1);
        assert_eq!(DGRAM_KIND_HEARTBEAT, 2);
        assert_eq!(DGRAM_KIND_NAK, 3);
        assert_eq!(DGRAM_KIND_STATUS, 4);
    }
}
```

- [ ] **Step 2: Run — expect compile failure**

Run: `cargo test -p uc_protocol datagram`
Expected: FAIL — names not defined.

- [ ] **Step 3: Implement the module**

Full file `uc_protocol/src/v2/datagram.rs` (tests from Step 1 at the bottom):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Replication datagram layout (spec §5). Core-only, like `v2::frame`.
//!
//! Every UDP datagram starts with this 16-byte header. DATA datagrams are
//! **self-locating**: `position` is the absolute stream position of the first
//! payload byte, and the payload is a run of complete, offset-contiguous
//! frames (a padding frame, if present, is last and sent header-only).
//! HEARTBEAT carries the leader's append position in `position` (liveness +
//! tail-loss detection) and has no payload. NAK and STATUS carry fixed-size
//! little-endian bodies. One UDP socket per node carries everything (control
//! rides the same socket, demuxed by `kind`).

/// Fixed datagram header size; payload (if any) follows immediately.
pub const DATAGRAM_HEADER_LEN: usize = 16;
/// Default datagram budget (spec §5); jumbo-frame deployments raise it.
pub const MTU_DEFAULT: usize = 1408;

pub const OFF_DGRAM_POSITION: usize = 0; // u64 LE — meaning depends on kind
pub const OFF_DGRAM_TERM_ID: usize = 8; // u32 LE — leadership_term_id
pub const OFF_DGRAM_KIND: usize = 12; // u8
pub const OFF_DGRAM_FLAGS: usize = 13; // u8
pub const OFF_DGRAM_RESERVED: usize = 14; // u16 — zero; future per-datagram PSK-MAC slot

/// Payload = run of complete frames starting at `position`.
pub const DGRAM_KIND_DATA: u8 = 1;
/// No payload; `position` = sender's append position.
pub const DGRAM_KIND_HEARTBEAT: u8 = 2;
/// Payload = `NakBody`.
pub const DGRAM_KIND_NAK: u8 = 3;
/// Payload = `StatusBody`.
pub const DGRAM_KIND_STATUS: u8 = 4;
// 5..=8 reserved: APPEND_POSITION, COMMIT_POSITION, REQUEST_VOTE, VOTE (M3/M4).

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatagramHeader {
    pub position: u64,
    pub leadership_term_id: u32,
    pub kind: u8,
    pub flags: u8,
}

/// `buf` must be at least `DATAGRAM_HEADER_LEN` bytes.
pub fn write_datagram_header(buf: &mut [u8], h: &DatagramHeader) {
    buf[OFF_DGRAM_POSITION..OFF_DGRAM_POSITION + 8].copy_from_slice(&h.position.to_le_bytes());
    buf[OFF_DGRAM_TERM_ID..OFF_DGRAM_TERM_ID + 4]
        .copy_from_slice(&h.leadership_term_id.to_le_bytes());
    buf[OFF_DGRAM_KIND] = h.kind;
    buf[OFF_DGRAM_FLAGS] = h.flags;
    buf[OFF_DGRAM_RESERVED..OFF_DGRAM_RESERVED + 2].copy_from_slice(&0u16.to_le_bytes());
}

/// `buf` must be at least `DATAGRAM_HEADER_LEN` bytes.
pub fn read_datagram_header(buf: &[u8]) -> DatagramHeader {
    DatagramHeader {
        position: u64::from_le_bytes(buf[OFF_DGRAM_POSITION..OFF_DGRAM_POSITION + 8].try_into().unwrap()),
        leadership_term_id: u32::from_le_bytes(
            buf[OFF_DGRAM_TERM_ID..OFF_DGRAM_TERM_ID + 4].try_into().unwrap(),
        ),
        kind: buf[OFF_DGRAM_KIND],
        flags: buf[OFF_DGRAM_FLAGS],
    }
}

/// NAK: "retransmit `length` bytes from `position`" (position is the
/// receiver's contiguous frontier — always a frame start).
pub const NAK_BODY_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NakBody {
    pub position: u64,
    pub length: u32,
}

pub fn write_nak_body(buf: &mut [u8], b: &NakBody) {
    buf[0..8].copy_from_slice(&b.position.to_le_bytes());
    buf[8..12].copy_from_slice(&b.length.to_le_bytes());
    buf[12..16].copy_from_slice(&0u32.to_le_bytes());
}

pub fn read_nak_body(buf: &[u8]) -> NakBody {
    NakBody {
        position: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        length: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
    }
}

/// Status: flow-control advert (spec §5) — contiguous-rebuilt position +
/// receive window (bytes the receiver can still accept beyond it: its own
/// archive gate, `durable + capacity − contiguous`; capacity ≤ 2^31 so it
/// fits u32).
pub const STATUS_BODY_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusBody {
    pub contiguous_position: u64,
    pub receive_window: u32,
}

pub fn write_status_body(buf: &mut [u8], b: &StatusBody) {
    buf[0..8].copy_from_slice(&b.contiguous_position.to_le_bytes());
    buf[8..12].copy_from_slice(&b.receive_window.to_le_bytes());
    buf[12..16].copy_from_slice(&0u32.to_le_bytes());
}

pub fn read_status_body(buf: &[u8]) -> StatusBody {
    StatusBody {
        contiguous_position: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        receive_window: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
    }
}
```

Add to `uc_protocol/src/v2/mod.rs`, matching its existing style:

```rust
pub mod datagram;
```

- [ ] **Step 4: Verify core-only + run**

Run: `cargo test -p uc_protocol && cargo clippy -p uc_protocol -- -D warnings`
Expected: PASS. Confirm the new module imports nothing from `std` (grep the file: no `use std::`).

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/datagram.rs uc_protocol/src/v2/mod.rs
git commit -m "feat(uc_protocol): v2 datagram header + NAK/status control bodies (core-only)"
```

---

### Task 3: `uc_log` — `sent` counter + `read_run_validated` (the sender's batch read)

The M1 final review sized this as "a small `read_slice_validated`-style addition, not a refactor": a validated **copying** read of a run of whole frames, padding-aware, for MTU packing and NAK service.

**Files:**
- Modify: `uc_log/src/counters.rs` (add `sent`)
- Modify: `uc_log/src/buffer.rs` (add `RunRead`, `SliceRead`, `read_run_validated`; tests)

**Interfaces:**
- Consumes: M1 `LogBuffer` internals (`commit_word`, `max_claim`, `region`, `offset`).
- Produces (used by Task 7):
  - `LogCounters { append, durable, sent }` — `sent` written only by the sender agent; `prime(pos)` now also sets `sent = pos`.
  - `pub struct RunRead { pub bytes: usize, pub advance: u64 }`
  - `pub enum SliceRead { Run(RunRead), NotCommitted, Overrun }`
  - `LogBuffer::read_run_validated(&self, from: u64, max_bytes: usize, out: &mut Vec<u8>) -> SliceRead` — copies contiguous committed whole frames starting at `from` (a frame start) into `out` (cleared first): stops at the wrap, at `append`, and (softly, ≥ 1 frame) at `max_bytes`. **Padding rule:** a padding frame is copied header-only (32 bytes) but advances the full aligned span, and always ends the run (padding ends at the wrap by construction). `bytes` = bytes in `out`; `advance` = stream positions consumed (`> bytes` iff the run ends in padding). Same seqlock validation as `read_frame_validated` (pre/post margin check + acquire fence).

- [ ] **Step 1: Write the failing tests**

Append to the `tests` module in `uc_log/src/buffer.rs`:

```rust
    #[test]
    fn run_read_packs_whole_frames_up_to_max_bytes() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..4 {
            a.append(1, i, &[i as u8; 64]).unwrap(); // 4 x 96 B frames
        }
        let mut out = Vec::new();
        // 200-byte budget -> 2 whole frames (192)
        match b.read_run_validated(0, 200, &mut out) {
            SliceRead::Run(r) => {
                assert_eq!((r.bytes, r.advance), (192, 192));
                assert_eq!(out.len(), 192);
                assert_eq!(read_header(&out).correlation_id, 0);
                assert_eq!(read_header(&out[96..]).correlation_id, 1);
            }
            other => panic!("expected Run, got {other:?}"),
        }
        // continuing from the advance point picks up frame 2
        match b.read_run_validated(192, 4096, &mut out) {
            SliceRead::Run(r) => {
                assert_eq!((r.bytes, r.advance), (192, 192)); // frames 2,3
                assert_eq!(read_header(&out).correlation_id, 2);
            }
            other => panic!("expected Run, got {other:?}"),
        }
        // at least one frame even under a tiny budget
        match b.read_run_validated(0, 8, &mut out) {
            SliceRead::Run(r) => assert_eq!((r.bytes, r.advance), (96, 96)),
            other => panic!("expected Run, got {other:?}"),
        }
        // caught up
        assert!(matches!(b.read_run_validated(4 * 96, 4096, &mut out), SliceRead::NotCommitted));
    }

    #[test]
    fn run_read_padding_is_header_only_with_full_advance() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..42 {
            a.append(1, i, &[0u8; 64]).unwrap(); // fill to 4032
        }
        c.durable.store_release(4032);
        a.append(1, 99, &[0u8; 64]).unwrap(); // 64 B padding at 4032, frame at 4096
        let mut out = Vec::new();
        // run starting at the padding: 32 bytes copied, 64 positions advanced,
        // run ends (padding ends at the wrap)
        match b.read_run_validated(4032, 1392, &mut out) {
            SliceRead::Run(r) => {
                assert_eq!((r.bytes, r.advance), (HEADER_LEN, 64));
                assert_eq!(read_header(&out).frame_type, FRAME_TYPE_PADDING);
                assert_eq!(read_header(&out).length, 64);
            }
            other => panic!("expected Run, got {other:?}"),
        }
        // and the post-wrap frame comes as its own run
        match b.read_run_validated(4096, 1392, &mut out) {
            SliceRead::Run(r) => {
                assert_eq!((r.bytes, r.advance), (96, 96));
                assert_eq!(read_header(&out).correlation_id, 99);
            }
            other => panic!("expected Run, got {other:?}"),
        }
    }

    #[test]
    fn run_read_detects_overrun_and_primed_fresh_buffer() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        let mut n = 0u64;
        while a.position() < 3 * CAP {
            a.append(1, n, &[0u8; 64]).unwrap();
            c.durable.store_release(a.position());
            n += 1;
        }
        let mut out = Vec::new();
        assert!(matches!(b.read_run_validated(0, 1392, &mut out), SliceRead::Overrun));
        // primed-over-fresh-buffer (Task 1 semantics, run variant)
        let (b2, c2) = buf();
        c2.prime(2 * CAP);
        assert!(matches!(b2.read_run_validated(2 * CAP - 64, 1392, &mut out), SliceRead::Overrun));
    }
```

- [ ] **Step 2: Run — expect compile failure**

Run: `cargo test -p uc_log run_read`
Expected: FAIL — `read_run_validated`, `SliceRead`, `RunRead` not defined.

- [ ] **Step 3: Add the `sent` counter**

In `uc_log/src/counters.rs`, extend `LogCounters` (repr(C) — append the field LAST, layout is future cnc):

```rust
/// The M1+M2 counter set. append: written only by the appender (leader) /
/// receiver (follower), after the frame commit word (so any position below
/// `append` is a committed frame). durable: written only by the archive,
/// after write+fdatasync of the block. sent: written only by the sender
/// agent, after the datagram send (leader only; follower leaves it 0).
#[repr(C)]
pub struct LogCounters {
    pub append: PaddedAtomicU64,
    pub durable: PaddedAtomicU64,
    pub sent: PaddedAtomicU64,
}
```

Update `new()` to initialize it and `prime()` to set it (`self.sent.store_release(pos);` — a restart resends from durable; followers drop the duplicates). Update the existing `counters_start_at_zero_and_prime` test to also assert `sent`.

- [ ] **Step 4: Implement `read_run_validated`**

In `uc_log/src/buffer.rs`, next to `FrameRead`:

```rust
/// Result payload of a successful `read_run_validated`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunRead {
    /// Bytes copied into `out`.
    pub bytes: usize,
    /// Stream positions consumed (> `bytes` iff the run ends in a padding
    /// frame, which is copied header-only).
    pub advance: u64,
}

#[derive(Debug)]
pub enum SliceRead {
    Run(RunRead),
    /// `from` is at or beyond the append counter.
    NotCommitted,
    /// The run's bytes may have been overwritten (reader lagged more than
    /// capacity − max_claim behind), or `from` predates a restart prime.
    /// Fall back to journal replay.
    Overrun,
}
```

and the method on `LogBuffer` (below `read_frame_validated`):

```rust
    /// Batch validated read for the sender (M2): copy a run of contiguous
    /// committed whole frames starting at `from` (a frame start) into `out`.
    /// The run never crosses the wrap; a padding frame is copied header-only
    /// (32 B) but advances its full aligned span and ends the run. Always
    /// returns at least one frame if one is available (a frame larger than
    /// `max_bytes` is returned alone — the sender's MTU config assert makes
    /// that impossible in practice). Seqlock discipline as in
    /// `read_frame_validated`: pre/post overwrite-margin checks around the
    /// copy with an acquire fence between.
    pub fn read_run_validated(&self, from: u64, max_bytes: usize, out: &mut Vec<u8>) -> SliceRead {
        let append = self.counters.append.load_acquire();
        if from >= append {
            return SliceRead::NotCommitted;
        }
        if append + self.max_claim() > from + self.capacity {
            return SliceRead::Overrun;
        }
        let off = self.offset(from);
        let hard = (append - from).min(self.capacity - off as u64);
        out.clear();
        let mut walked = 0u64; // stream advance
        let mut copied = 0usize; // bytes in out
        while walked < hard {
            let o = off + walked as usize;
            let len = self.commit_word(o).load(Ordering::Acquire) as usize;
            if len == 0 {
                break; // restart-primed tail: no bytes in this buffer
            }
            let aligned = align_frame_len(len) as u64;
            if aligned == 0 || walked + aligned > hard {
                break; // torn/overwritten length — post-check will decide
            }
            // SAFETY: o + 5 within capacity (aligned span checked above).
            let ftype = unsafe { *self.region.ptr_at(o + frame::OFF_TYPE) };
            let copy_len = if ftype == FRAME_TYPE_PADDING { HEADER_LEN } else { aligned as usize };
            if copied > 0 && copied + copy_len > max_bytes {
                break;
            }
            // SAFETY: [o, o+copy_len) within capacity; validated below.
            out.extend_from_slice(unsafe {
                std::slice::from_raw_parts(self.region.ptr_at(o), copy_len)
            });
            copied += copy_len;
            walked += aligned;
            if ftype == FRAME_TYPE_PADDING || copied >= max_bytes {
                break; // padding ends at the wrap
            }
        }
        // Seqlock re-check (see read_frame_validated for the fence rationale).
        std::sync::atomic::fence(Ordering::Acquire);
        let append_after = self.counters.append.load_acquire();
        if append_after + self.max_claim() > from + self.capacity {
            return SliceRead::Overrun;
        }
        if walked == 0 {
            // len == 0 at a committed position: primed-over-fresh-buffer.
            return SliceRead::Overrun;
        }
        SliceRead::Run(RunRead { bytes: copied, advance: walked })
    }
```

Note `frame::OFF_TYPE` is already exported by `uc_protocol::v2::frame`; extend the existing `use uc_protocol::v2::frame::{...}` import if needed.

- [ ] **Step 5: Run tests**

Run: `cargo test -p uc_log && cargo clippy --workspace -- -D warnings`
Expected: PASS (3 new tests + updated counters test).

- [ ] **Step 6: Commit**

```bash
git add uc_log/src/counters.rs uc_log/src/buffer.rs
git commit -m "feat(uc_log): sent counter + read_run_validated batch read (padding-aware, seqlock-validated)"
```

---

### Task 4: `uc_log::writer::PositionedWriter` — the follower's write path

Spec §4: "receiver agent (follower) writes frames at their position offset — duplicates and reordering are idempotent by construction"; spec §3.2 places the position-addressed writer in `uc_log`.

**Files:**
- Create: `uc_log/src/writer.rs`
- Modify: `uc_log/src/lib.rs` (add `pub mod writer;`)
- Modify: `uc_log/src/buffer.rs` (two `pub(crate)` accessors)

**Interfaces:**
- Consumes: `LogBuffer` internals via new `pub(crate)` accessors: `pub(crate) fn region(&self) -> &Region` and change `fn offset` to `pub(crate) fn offset`.
- Produces (used by Task 8):
  - `pub struct PositionedWriter` — `new(buffer: Arc<LogBuffer>) -> Self`
  - `write_run(&self, position: u64, bytes: &[u8]) -> bool` — blind idempotent copy of a frame-run at `position`'s ring offset. Returns `false` (caller drops the datagram) if the run is empty, would cross the wrap, or would land beyond `durable + capacity` (the follower-side overrun guard). Advances NO counter — the receiver advances `append` to the contiguous frontier after gap tracking (Task 8), which is what makes duplicates/reordering safe: bytes only become visible to readers once counted contiguous.

- [ ] **Step 1: Write the failing test**

Create `uc_log/src/writer.rs` with tests first (full file with implementation is in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{Appender, LogBuffer, SliceRead};
    use crate::counters::LogCounters;
    use crate::region::Region;
    use std::sync::Arc;
    use uc_protocol::v2::frame::read_header;

    const CAP: u64 = 4096;

    fn buf() -> (Arc<LogBuffer>, Arc<LogCounters>) {
        let counters = Arc::new(LogCounters::new());
        let b = Arc::new(LogBuffer::new(
            Region::heap_zeroed(CAP as usize),
            Arc::clone(&counters),
            256,
        ));
        (b, counters)
    }

    /// End-to-end symmetry: leader appends, sender-style run read, follower
    /// write_run, follower's archive-style read sees identical bytes.
    #[test]
    fn leader_run_rewritten_on_follower_reads_back_identically() {
        let (leader, _lc) = buf();
        let (follower, fc) = buf();
        let mut a = Appender::new(Arc::clone(&leader), 7);
        for i in 0..4 {
            a.append(2, i, &[i as u8; 64]).unwrap();
        }
        let w = PositionedWriter::new(Arc::clone(&follower));
        let mut run = Vec::new();
        let mut pos = 0u64;
        while let SliceRead::Run(r) = leader.read_run_validated(pos, 200, &mut run) {
            assert!(w.write_run(pos, &run[..r.bytes]));
            pos += r.advance;
        }
        assert_eq!(pos, 4 * 96);
        // receiver-role: advance append after (simulated) gap tracking
        fc.append.store_release(pos);
        let s = follower.recordable_slice(0, 1 << 20);
        assert_eq!(s.len(), 384);
        assert_eq!(read_header(&s[96..]).correlation_id, 1);
        assert_eq!(&s[3 * 96 + 32..3 * 96 + 96], &[3u8; 64]);
        // idempotent duplicate rewrite: same bytes, still fine
        let mut run2 = Vec::new();
        if let SliceRead::Run(r) = leader.read_run_validated(0, 200, &mut run2) {
            assert!(w.write_run(0, &run2[..r.bytes]));
        }
        assert_eq!(follower.recordable_slice(0, 1 << 20).len(), 384);
    }

    #[test]
    fn write_run_rejects_wrap_cross_empty_and_overrun() {
        let (follower, fc) = buf();
        let w = PositionedWriter::new(Arc::clone(&follower));
        assert!(!w.write_run(0, &[]));
        // would cross the wrap: offset 4064 + 64 bytes > 4096
        assert!(!w.write_run(CAP - 32, &[0u8; 64]));
        // ends exactly at the wrap: fine
        assert!(w.write_run(CAP - 32, &[0u8; 32]));
        // overrun guard: durable = 0 -> nothing beyond position capacity
        assert!(!w.write_run(CAP, &[0u8; 32])); // 4096+32 > 0+4096
        fc.durable.store_release(96);
        assert!(w.write_run(CAP, &[0u8; 32])); // 4128 <= 96+4096
    }
}
```

- [ ] **Step 2: Run — expect compile failure**

Run: `cargo test -p uc_log writer`
Expected: FAIL — `PositionedWriter` not defined (add `pub mod writer;` to `lib.rs` now so the failure is about the type, not the module).

- [ ] **Step 3: Implement**

Top of `uc_log/src/writer.rs` (above the tests):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Position-addressed writer (spec §4): the follower's single writer. The
//! receiver agent copies datagram frame-runs at their ring offset — blind,
//! idempotent plain stores. Visibility discipline is the same as the
//! leader's: readers bound themselves by an acquire-load of `append`, which
//! the RECEIVER advances (Release) only to the contiguous frontier after gap
//! tracking — so duplicated/reordered writes above the frontier are never
//! observable, and re-writes below it are rejected by the caller (Task 8
//! accept rule: run.position >= contiguous).

use std::sync::Arc;

use crate::buffer::LogBuffer;

pub struct PositionedWriter {
    buffer: Arc<LogBuffer>,
}

impl PositionedWriter {
    pub fn new(buffer: Arc<LogBuffer>) -> Self {
        Self { buffer }
    }

    /// Copy a frame-run at `position`'s ring offset. Returns false (drop the
    /// datagram) if the run is empty, would cross the wrap (the sender never
    /// packs across it — padding rule), or would land beyond
    /// `durable + capacity` (the follower-side overrun gate: never overwrite
    /// what the local archive hasn't recorded).
    pub fn write_run(&self, position: u64, bytes: &[u8]) -> bool {
        let b = &self.buffer;
        debug_assert_eq!(position % 32, 0, "runs start at frame boundaries");
        let off = b.offset(position);
        if bytes.is_empty() || bytes.len() as u64 > b.capacity() - off as u64 {
            return false;
        }
        let durable = b.counters().durable.load_acquire();
        if position + bytes.len() as u64 > durable + b.capacity() {
            return false;
        }
        // SAFETY: [off, off+len) within capacity (wrap check above); bytes in
        // [append, durable+capacity) are writer-owned (single receiver per
        // buffer, the follower analog of the appender contract); visibility
        // via the receiver's later Release store of `append`.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), b.region().ptr_at(off), bytes.len());
        }
        true
    }
}
```

In `uc_log/src/buffer.rs`: change `fn offset` to `pub(crate) fn offset`, and add next to `max_claim`:

```rust
    #[inline]
    pub(crate) fn region(&self) -> &Region {
        &self.region
    }
```

In `uc_log/src/lib.rs` add `pub mod writer;` (alphabetical order with the others).

- [ ] **Step 4: Run tests**

Run: `cargo test -p uc_log && cargo clippy --workspace -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add uc_log/src/writer.rs uc_log/src/lib.rs uc_log/src/buffer.rs
git commit -m "feat(uc_log): PositionedWriter — follower position-addressed frame-run writes"
```

---

### Task 5: `uc_net` scaffold — crate + seeded fault-injecting UDP socket

Spec §8 L2: "`uc_net` … with the fault layer built in from day one (native to own-UDP)".

**Files:**
- Create: `uc_net/Cargo.toml`
- Create: `uc_net/src/lib.rs`
- Create: `uc_net/src/fault.rs`
- Modify: `Cargo.toml` (workspace `members`: add `"uc_net"` after `"uc_log"`)

**Interfaces:**
- Consumes: nothing from other tasks (std only).
- Produces (used by Tasks 7–10):
  - `pub struct FaultConfig { pub seed: u64, pub drop_per_million: u32, pub dup_per_million: u32, pub reorder_per_million: u32 }` (Default: seed 1, all rates 0)
  - `pub struct FaultSocket` — `bind(addr: &str) -> io::Result<Self>` (nonblocking), `from_socket(UdpSocket) -> io::Result<Self>`, `set_faults(&mut self, FaultConfig)`, `local_addr() -> io::Result<SocketAddr>`, `send_to(&mut self, buf: &[u8], to: SocketAddr) -> io::Result<()>` (faults applied HERE: drop skips the syscall, dup sends twice, reorder holds one datagram back and flushes it after the next send), `recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(usize, SocketAddr)>>` (`None` on WouldBlock; `ConnectionRefused`/`ConnectionReset` — ICMP echoes of earlier sends — are swallowed to `None`), `try_clone_raw(&self) -> io::Result<UdpSocket>` (for a same-node agent that only recvs).
  - `pub(crate) struct XorShift64` — `new(seed)`, `next_u64()`, `chance(per_million: u32) -> bool` (deterministic).

- [ ] **Step 1: Create the crate**

`uc_net/Cargo.toml`:

```toml
[package]
name = "uc_net"
description = "UC v2 UDP replication data plane: sender/receiver agents, NAK, flow control (spec 2026-07-09, M2)"
edition.workspace = true
version.workspace = true
license.workspace = true
authors.workspace = true

[dependencies]
uc_protocol = { path = "../uc_protocol" }
uc_log = { path = "../uc_log" }

[dev-dependencies]
tempfile = { workspace = true }
```

`uc_net/src/lib.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UC v2 replication data plane (M2).
//! Spec: docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md §5.

pub mod fault;
```

Add `"uc_net"` to the workspace `members` array in the root `Cargo.toml` (after `"uc_log"`).

- [ ] **Step 2: Write the failing tests**

Tests module at the bottom of `uc_net/src/fault.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn recv_all(sock: &FaultSocket, expect: usize) -> Vec<Vec<u8>> {
        let mut buf = [0u8; 2048];
        let mut got = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        while got.len() < expect && Instant::now() < deadline {
            match sock.recv_from(&mut buf).unwrap() {
                Some((n, _)) => got.push(buf[..n].to_vec()),
                None => std::thread::yield_now(),
            }
        }
        got
    }

    #[test]
    fn xorshift_is_deterministic_and_chance_bounded() {
        let mut a = XorShift64::new(42);
        let mut b = XorShift64::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        let mut r = XorShift64::new(7);
        assert!((0..1000).all(|_| !r.chance(0)));
        let mut r = XorShift64::new(7);
        assert!((0..1000).all(|_| r.chance(1_000_000)));
    }

    #[test]
    fn clean_roundtrip_and_wouldblock() {
        let rx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let mut buf = [0u8; 16];
        assert!(rx.recv_from(&mut buf).unwrap().is_none()); // nonblocking
        tx.send_to(b"ping", rx.local_addr().unwrap()).unwrap();
        let got = recv_all(&rx, 1);
        assert_eq!(got, vec![b"ping".to_vec()]);
    }

    #[test]
    fn drop_is_deterministic_by_seed() {
        let rx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let to = rx.local_addr().unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        tx.set_faults(FaultConfig { seed: 99, drop_per_million: 500_000, ..Default::default() });
        for i in 0..100u8 {
            tx.send_to(&[i], to).unwrap();
        }
        // loopback is lossless: received = exactly the non-dropped set.
        // Re-derive it from the same seed.
        let mut rng = XorShift64::new(99);
        let expected: Vec<Vec<u8>> =
            (0..100u8).filter(|_| !rng.chance(500_000)).map(|i| vec![i]).collect();
        assert!(!expected.is_empty() && expected.len() < 100);
        let got = recv_all(&rx, expected.len());
        assert_eq!(got, expected);
    }

    #[test]
    fn dup_duplicates_and_reorder_swaps() {
        let rx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let to = rx.local_addr().unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        tx.set_faults(FaultConfig { seed: 1, dup_per_million: 1_000_000, ..Default::default() });
        tx.send_to(b"a", to).unwrap();
        assert_eq!(recv_all(&rx, 2), vec![b"a".to_vec(), b"a".to_vec()]);

        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        tx.set_faults(FaultConfig { seed: 1, reorder_per_million: 1_000_000, ..Default::default() });
        tx.send_to(b"first", to).unwrap(); // held back
        tx.send_to(b"second", to).unwrap(); // goes out, then flushes "first"
        assert_eq!(recv_all(&rx, 2), vec![b"second".to_vec(), b"first".to_vec()]);
    }
}
```

- [ ] **Step 3: Run — expect compile failure**

Run: `cargo test -p uc_net`
Expected: FAIL — types not defined.

- [ ] **Step 4: Implement**

`uc_net/src/fault.rs` above the tests:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Nonblocking UDP socket with a built-in, seeded fault layer (spec §8 L2:
//! native to own-UDP, day one). Faults are applied on the SEND side so a
//! seeded run is deterministic: drop skips the syscall, dup sends twice,
//! reorder holds one datagram back and flushes it after the next send (a
//! held datagram is therefore delayed by at most one send — heartbeats keep
//! sends coming, so nothing is held forever).

use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};

/// Deterministic xorshift64 — no external RNG dependency.
pub(crate) struct XorShift64(u64);

impl XorShift64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed })
    }
    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    /// True with probability `per_million / 1_000_000`.
    pub(crate) fn chance(&mut self, per_million: u32) -> bool {
        self.next_u64() % 1_000_000 < per_million as u64
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FaultConfig {
    pub seed: u64,
    pub drop_per_million: u32,
    pub dup_per_million: u32,
    pub reorder_per_million: u32,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self { seed: 1, drop_per_million: 0, dup_per_million: 0, reorder_per_million: 0 }
    }
}

pub struct FaultSocket {
    sock: UdpSocket,
    cfg: FaultConfig,
    rng: XorShift64,
    held: Option<(Vec<u8>, SocketAddr)>,
}

impl FaultSocket {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Self::from_socket(UdpSocket::bind(addr)?)
    }

    pub fn from_socket(sock: UdpSocket) -> io::Result<Self> {
        sock.set_nonblocking(true)?;
        let cfg = FaultConfig::default();
        Ok(Self { sock, rng: XorShift64::new(cfg.seed), cfg, held: None })
    }

    pub fn set_faults(&mut self, cfg: FaultConfig) {
        self.rng = XorShift64::new(cfg.seed);
        self.cfg = cfg;
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// Clone the raw socket for a same-node agent that only receives (the
    /// leader's receiver shares the sender's socket).
    pub fn try_clone_raw(&self) -> io::Result<UdpSocket> {
        self.sock.try_clone()
    }

    pub fn send_to(&mut self, buf: &[u8], to: SocketAddr) -> io::Result<()> {
        if self.cfg.drop_per_million > 0 && self.rng.chance(self.cfg.drop_per_million) {
            return Ok(()); // dropped on the wire
        }
        if self.cfg.reorder_per_million > 0
            && self.held.is_none()
            && self.rng.chance(self.cfg.reorder_per_million)
        {
            self.held = Some((buf.to_vec(), to));
            return Ok(());
        }
        self.raw_send(buf, to)?;
        if self.cfg.dup_per_million > 0 && self.rng.chance(self.cfg.dup_per_million) {
            self.raw_send(buf, to)?;
        }
        if let Some((b, a)) = self.held.take() {
            self.raw_send(&b, a)?;
        }
        Ok(())
    }

    fn raw_send(&self, buf: &[u8], to: SocketAddr) -> io::Result<()> {
        match self.sock.send_to(buf, to) {
            Ok(_) => Ok(()),
            // ICMP unreachable from an earlier send surfaces here on Linux;
            // UDP is fire-and-forget for us.
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Nonblocking receive: `None` when the socket is empty.
    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(usize, SocketAddr)>> {
        match self.sock.recv_from(buf) {
            Ok(x) => Ok(Some(x)),
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::ConnectionRefused
                    || e.kind() == io::ErrorKind::ConnectionReset =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
}
```

Note the drop-determinism test relies on `chance` being called exactly once per send when only `drop_per_million` is set — the early-return structure above guarantees it (dup/reorder chances are gated on their rates being nonzero).

- [ ] **Step 5: Run tests**

Run: `cargo test -p uc_net && cargo clippy --workspace -- -D warnings`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock uc_net/Cargo.toml uc_net/src/lib.rs uc_net/src/fault.rs
git commit -m "feat(uc_net): crate scaffold + seeded fault-injecting nonblocking UDP socket"
```

---

### Task 6: gap tracking, NAK timing, flow control — pure logic

All deterministic (injected time, seeded randomness), no I/O — the testable core of loss recovery and pacing.

**Files:**
- Create: `uc_net/src/rebuild.rs` (`Rebuilt`, `NakConfig`, `NakTimer`)
- Create: `uc_net/src/flow.rs` (`FlowControl`)
- Modify: `uc_net/src/lib.rs` (add `pub mod flow;` and `pub mod rebuild;`)

**Interfaces:**
- Consumes: `crate::fault::XorShift64` (make it `pub(crate)` — it already is).
- Produces (used by Tasks 7–8):
  - `Rebuilt::new(start: u64)`, `contiguous() -> u64`, `insert(start: u64, end: u64) -> bool` (true iff the contiguous frontier advanced; coalesces out-of-order runs), `first_gap() -> Option<(u64, u64)>` (`(contiguous, start_of_first_ooo_run)`), `highest() -> u64`.
  - `NakConfig { delay_min_ns: u64, delay_max_ns: u64, backoff_ns: u64 }` with `Default { 200_000, 1_000_000, 5_000_000 }`.
  - `NakTimer::new(cfg: NakConfig, seed: u64)`, `poll(&mut self, gap: Option<(u64, u64)>, now_ns: u64) -> Option<(u64, u64)>` — arms a randomized delay when a new gap appears (~1 RTT, spec §5), fires `(start, end)` once the deadline passes, re-fires every `backoff_ns` while the same gap start persists, disarms when the gap clears.
  - `FlowControl::new(followers: &[SocketAddr], cluster_size: usize, initial_window: u64)`, `on_status(&mut self, from: SocketAddr, contiguous: u64, window: u32)`, `limit(&self) -> u64` — the quorum-th order statistic: with `needed = cluster_size/2 + 1 − 1` followers required beyond the leader, the limit is the `needed`-th highest `contiguous + window` (3 nodes ⇒ the faster of the two followers; spec §5 — deliberately not min/lockstep). Unknown-yet followers start at `(0, initial_window)`.

- [ ] **Step 1: Write the failing tests**

`uc_net/src/rebuild.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_order_advances_and_dups_do_not() {
        let mut r = Rebuilt::new(1000);
        assert_eq!(r.contiguous(), 1000);
        assert!(r.insert(1000, 1096));
        assert!(r.insert(1096, 1192));
        assert_eq!(r.contiguous(), 1192);
        assert!(!r.insert(1000, 1096)); // stale dup
        assert_eq!(r.contiguous(), 1192);
        assert_eq!(r.first_gap(), None);
        assert_eq!(r.highest(), 1192);
    }

    #[test]
    fn gap_then_fill_merges_and_reports() {
        let mut r = Rebuilt::new(0);
        assert!(!r.insert(96, 192)); // arrives ahead: gap [0, 96)
        assert!(!r.insert(288, 384)); // second ooo run
        assert_eq!(r.contiguous(), 0);
        assert_eq!(r.first_gap(), Some((0, 96)));
        assert_eq!(r.highest(), 384);
        assert!(r.insert(0, 96)); // fills the first gap, absorbs [96,192)
        assert_eq!(r.contiguous(), 192);
        assert_eq!(r.first_gap(), Some((192, 288)));
        assert!(r.insert(192, 288)); // fills the rest
        assert_eq!(r.contiguous(), 384);
        assert_eq!(r.first_gap(), None);
    }

    #[test]
    fn overlapping_ooo_runs_coalesce() {
        let mut r = Rebuilt::new(0);
        assert!(!r.insert(96, 288));
        assert!(!r.insert(192, 384)); // overlaps previous
        assert!(!r.insert(384, 480)); // adjacent
        assert!(r.insert(0, 96));
        assert_eq!(r.contiguous(), 480);
    }

    #[test]
    fn nak_timer_arms_randomized_fires_and_backs_off() {
        let cfg = NakConfig { delay_min_ns: 200_000, delay_max_ns: 1_000_000, backoff_ns: 5_000_000 };
        let mut t = NakTimer::new(cfg, 42);
        // new gap arms; nothing fires before the deadline
        assert_eq!(t.poll(Some((0, 96)), 0), None);
        assert_eq!(t.poll(Some((0, 96)), 199_999), None);
        // by delay_max it must have fired exactly once
        let fired = t.poll(Some((0, 96)), 1_000_000);
        assert_eq!(fired, Some((0, 96)));
        // same gap: re-fires only after backoff
        assert_eq!(t.poll(Some((0, 96)), 1_000_001), None);
        assert_eq!(t.poll(Some((0, 96)), 1_000_000 + 5_000_000), Some((0, 96)));
        // gap cleared: disarm; new gap re-arms fresh
        assert_eq!(t.poll(None, 7_000_000), None);
        assert_eq!(t.poll(Some((96, 192)), 7_000_000), None); // arming, not firing
        assert!(t.poll(Some((96, 192)), 7_000_000 + 1_000_000).is_some());
    }

    #[test]
    fn nak_timer_tracks_growing_gap_end() {
        let mut t = NakTimer::new(NakConfig::default(), 7);
        assert_eq!(t.poll(Some((0, 96)), 0), None);
        // gap END grew while armed (more ooo arrived); same start = same gap
        let fired = t.poll(Some((0, 480)), 1_000_000).unwrap();
        assert_eq!(fired, (0, 480));
    }
}
```

`uc_net/src/flow.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    #[test]
    fn three_node_limit_is_the_faster_follower() {
        let (a, b) = (addr(1), addr(2));
        let mut f = FlowControl::new(&[a, b], 3, 65536);
        // bootstrap: both unknown at (0, initial) -> limit = initial
        assert_eq!(f.limit(), 65536);
        f.on_status(a, 1_000_000, 100_000);
        assert_eq!(f.limit(), 1_100_000); // max(1.1M, 64k)
        f.on_status(b, 2_000_000, 50_000);
        assert_eq!(f.limit(), 2_050_000); // the faster of the two
        // a slow follower's shrinking window never drags the limit down
        f.on_status(a, 1_000_000, 0);
        assert_eq!(f.limit(), 2_050_000);
        // statuses are latest-wins, not max: the fast one's window can shrink
        f.on_status(b, 2_000_000, 10_000);
        assert_eq!(f.limit(), 2_010_000);
    }

    #[test]
    fn five_node_limit_is_second_highest() {
        let fs: Vec<SocketAddr> = (1..=4).map(addr).collect();
        let mut f = FlowControl::new(&fs, 5, 1000);
        for (i, a) in fs.iter().enumerate() {
            f.on_status(*a, (i as u64 + 1) * 1000, 0);
        }
        // limits: 1000 2000 3000 4000; quorum 3 needs 2 followers -> 2nd highest
        assert_eq!(f.limit(), 3000);
    }

    #[test]
    fn unknown_source_is_ignored() {
        let a = addr(1);
        let mut f = FlowControl::new(&[a], 2, 500);
        f.on_status(addr(9), 1 << 40, 1 << 20); // not a configured follower
        assert_eq!(f.limit(), 500);
    }
}
```

- [ ] **Step 2: Run — expect compile failure**

Run: `cargo test -p uc_net rebuild flow` (two invocations or just `cargo test -p uc_net`)
Expected: FAIL — modules/types not defined.

- [ ] **Step 3: Implement `rebuild.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Follower-side loss recovery state (spec §5): gap tracking over absolute
//! positions (no reliance on buffer contents — stale bytes from a previous
//! lap can hold nonzero length words, so contiguity must be tracked here,
//! not scanned) + the randomized-delay NAK timer (~1 RTT; a short delay
//! absorbs benign reordering before asking for a retransmit).

use std::collections::BTreeMap;

use crate::fault::XorShift64;

/// Tracks which byte ranges of the stream have landed in the buffer and
/// where the contiguous frontier is. In-order traffic never touches the map.
pub struct Rebuilt {
    contiguous: u64,
    /// Out-of-order runs strictly above `contiguous`: start -> end.
    ooo: BTreeMap<u64, u64>,
}

impl Rebuilt {
    pub fn new(start: u64) -> Self {
        Self { contiguous: start, ooo: BTreeMap::new() }
    }

    #[inline]
    pub fn contiguous(&self) -> u64 {
        self.contiguous
    }

    /// Record [start, end). Returns true iff the contiguous frontier advanced.
    pub fn insert(&mut self, start: u64, end: u64) -> bool {
        debug_assert!(start <= end);
        if end <= self.contiguous {
            return false; // stale duplicate
        }
        if start <= self.contiguous {
            self.contiguous = end;
            // absorb ooo runs that are now contiguous
            while let Some((&s, &e)) = self.ooo.first_key_value() {
                if s > self.contiguous {
                    break;
                }
                self.contiguous = self.contiguous.max(e);
                self.ooo.remove(&s);
            }
            true
        } else {
            // coalesce with overlapping/adjacent neighbors
            let (mut s, mut e) = (start, end);
            if let Some((&ps, &pe)) = self.ooo.range(..=s).next_back() {
                if pe >= s {
                    s = ps;
                    e = e.max(pe);
                    self.ooo.remove(&ps);
                }
            }
            while let Some((&ns, &ne)) = self.ooo.range(s..).next() {
                if ns > e {
                    break;
                }
                e = e.max(ne);
                self.ooo.remove(&ns);
            }
            self.ooo.insert(s, e);
            false
        }
    }

    /// The first missing range, if any out-of-order data is waiting behind it.
    /// (Tail loss — nothing waiting — is detected against the leader's
    /// heartbeat position by the receiver, not here.)
    pub fn first_gap(&self) -> Option<(u64, u64)> {
        self.ooo.first_key_value().map(|(&s, _)| (self.contiguous, s))
    }

    /// Highest position received (contiguous or not).
    pub fn highest(&self) -> u64 {
        self.ooo.last_key_value().map(|(_, &e)| e).unwrap_or(self.contiguous)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NakConfig {
    pub delay_min_ns: u64,
    pub delay_max_ns: u64,
    /// Re-NAK interval while the same gap start persists (covers a lost NAK
    /// or a lost retransmission).
    pub backoff_ns: u64,
}

impl Default for NakConfig {
    fn default() -> Self {
        Self { delay_min_ns: 200_000, delay_max_ns: 1_000_000, backoff_ns: 5_000_000 }
    }
}

pub struct NakTimer {
    cfg: NakConfig,
    rng: XorShift64,
    armed: Option<Armed>,
}

struct Armed {
    start: u64,
    deadline_ns: u64,
}

impl NakTimer {
    pub fn new(cfg: NakConfig, seed: u64) -> Self {
        Self { cfg, rng: XorShift64::new(seed), armed: None }
    }

    fn delay(&mut self) -> u64 {
        let span = self.cfg.delay_max_ns - self.cfg.delay_min_ns;
        self.cfg.delay_min_ns + if span == 0 { 0 } else { self.rng.next_u64() % span }
    }

    /// Drive with the current first gap and the current time. Returns the
    /// `(start, end)` range to NAK when the timer fires.
    pub fn poll(&mut self, gap: Option<(u64, u64)>, now_ns: u64) -> Option<(u64, u64)> {
        let Some((start, end)) = gap else {
            self.armed = None;
            return None;
        };
        match &mut self.armed {
            Some(a) if a.start == start => {
                if now_ns >= a.deadline_ns {
                    a.deadline_ns = now_ns + self.cfg.backoff_ns;
                    Some((start, end))
                } else {
                    None
                }
            }
            _ => {
                let d = self.delay();
                self.armed = Some(Armed { start, deadline_ns: now_ns + d });
                None
            }
        }
    }
}
```

- [ ] **Step 4: Implement `flow.rs`**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Quorum-paced flow control (spec §5): each follower advertises
//! `contiguous + receive_window`; the sender's limit is the quorum-th order
//! statistic over those — a slow follower never stalls what the quorum could
//! legally commit (deliberately NOT min/lockstep). It recovers via NAK or,
//! below the buffer tail, via a replay session (M4).

use std::net::SocketAddr;

pub struct FlowControl {
    /// (follower, latest advertised limit = contiguous + window).
    followers: Vec<(SocketAddr, u64)>,
    /// Followers needed beyond the leader for a quorum.
    needed: usize,
}

impl FlowControl {
    pub fn new(followers: &[SocketAddr], cluster_size: usize, initial_window: u64) -> Self {
        assert!(cluster_size >= followers.len() + 1, "leader + followers exceed cluster");
        let needed = (cluster_size / 2 + 1).saturating_sub(1);
        assert!(needed <= followers.len(), "not enough followers for a quorum");
        Self { followers: followers.iter().map(|a| (*a, initial_window)).collect(), needed }
    }

    /// Latest-wins (windows legitimately shrink as a receiver fills).
    pub fn on_status(&mut self, from: SocketAddr, contiguous: u64, window: u32) {
        if let Some(f) = self.followers.iter_mut().find(|(a, _)| *a == from) {
            f.1 = contiguous + window as u64;
        }
    }

    /// The sender may not send at or beyond this position.
    pub fn limit(&self) -> u64 {
        if self.needed == 0 {
            return u64::MAX; // solo cluster: nothing to pace against
        }
        let mut limits: Vec<u64> = self.followers.iter().map(|(_, l)| *l).collect();
        limits.sort_unstable_by(|a, b| b.cmp(a));
        limits[self.needed - 1]
    }
}
```

Add both modules to `uc_net/src/lib.rs` (alphabetical: `fault`, `flow`, `rebuild`).

- [ ] **Step 5: Run tests**

Run: `cargo test -p uc_net && cargo clippy --workspace -- -D warnings`
Expected: PASS (8 new tests).

- [ ] **Step 6: Commit**

```bash
git add uc_net/src/rebuild.rs uc_net/src/flow.rs uc_net/src/lib.rs
git commit -m "feat(uc_net): gap tracking + randomized NAK timer + quorum-order-statistic flow control"
```

---

### Task 7: the sender agent

**Files:**
- Create: `uc_net/src/sender.rs`
- Modify: `uc_net/src/lib.rs` (add `pub mod sender;`)

**Interfaces:**
- Consumes: `uc_log::buffer::{LogBuffer, SliceRead}`, `uc_log::counters` (via buffer), `uc_protocol::v2::datagram::*`, `crate::fault::FaultSocket`, `crate::flow::FlowControl`.
- Produces (used by Tasks 8–10):
  - `pub enum CtrlMsg { Nak { from: SocketAddr, position: u64, length: u32 }, Status { from: SocketAddr, contiguous: u64, window: u32 } }` (`Copy`) — the leader-receiver→sender channel item.
  - `pub struct SenderConfig { pub mtu: usize, pub term_id: u32, pub heartbeat_ns: u64, pub initial_window: u64, pub dgrams_per_cycle: usize }` with `SenderConfig::new(term_id) -> Self` defaults `{ mtu: MTU_DEFAULT, heartbeat_ns: 100_000_000, initial_window: 65_536, dgrams_per_cycle: 8 }`.
  - `pub struct SenderStats` — all-`AtomicU64` fields `{ datagrams, bytes, naks_served, heartbeats, flow_stalls, overruns }`, `Relaxed` increments; shared via `Arc`.
  - `Sender::new(buffer: Arc<LogBuffer>, sock: FaultSocket, followers: Vec<SocketAddr>, cluster_size: usize, ctrl: mpsc::Receiver<CtrlMsg>, cfg: SenderConfig) -> Sender` — asserts a max frame fits one datagram; resumes `sent` from the counter.
  - `Sender::do_work(&mut self) -> bool` (an `AgentRunner` duty cycle), `Sender::stats(&self) -> Arc<SenderStats>`.

- [ ] **Step 1: Write the failing test**

Tests at the bottom of `uc_net/src/sender.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use uc_log::buffer::Appender;
    use uc_log::counters::LogCounters;
    use uc_log::region::Region;
    use uc_protocol::v2::frame::{read_header, HEADER_LEN};

    fn buffer() -> Arc<LogBuffer> {
        let counters = Arc::new(LogCounters::new());
        Arc::new(LogBuffer::new(Region::heap_zeroed(1 << 16), counters, 256))
    }

    struct Fake {
        sock: FaultSocket,
    }
    impl Fake {
        fn new() -> Self {
            Self { sock: FaultSocket::bind("127.0.0.1:0").unwrap() }
        }
        fn addr(&self) -> SocketAddr {
            self.sock.local_addr().unwrap()
        }
        fn recv(&self) -> Option<(DatagramHeader, Vec<u8>)> {
            let mut buf = [0u8; 2048];
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if let Some((n, _)) = self.sock.recv_from(&mut buf).unwrap() {
                    let h = read_datagram_header(&buf);
                    return Some((h, buf[DATAGRAM_HEADER_LEN..n].to_vec()));
                }
                std::thread::yield_now();
            }
            None
        }
        fn drain(&self) {
            let mut buf = [0u8; 2048];
            while self.sock.recv_from(&mut buf).unwrap().is_some() {}
        }
    }

    fn sender_to(followers: &[&Fake], b: &Arc<LogBuffer>) -> (Sender, mpsc::SyncSender<CtrlMsg>) {
        let (tx, rx) = mpsc::sync_channel(1024);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX; // no heartbeats: data-recv asserts must not race one
        let s = Sender::new(
            Arc::clone(b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            followers.iter().map(|f| f.addr()).collect(),
            3,
            rx,
            cfg,
        );
        (s, tx)
    }

    #[test]
    fn streams_frames_to_all_followers_and_advances_sent() {
        let b = buffer();
        let (f1, f2) = (Fake::new(), Fake::new());
        let (mut s, _tx) = sender_to(&[&f1, &f2], &b);
        let mut a = Appender::new(Arc::clone(&b), 9);
        for i in 0..3 {
            a.append(4, i, &[i as u8; 64]).unwrap();
        }
        assert!(s.do_work());
        for f in [&f1, &f2] {
            let (h, body) = f.recv().expect("data datagram");
            assert_eq!(h.kind, DGRAM_KIND_DATA);
            assert_eq!(h.leadership_term_id, 9);
            assert_eq!(h.position, 0);
            assert_eq!(body.len(), 3 * 96); // all three frames packed in one datagram
            assert_eq!(read_header(&body[96..]).correlation_id, 1);
            assert_eq!(&body[2 * 96 + HEADER_LEN..2 * 96 + HEADER_LEN + 64], &[2u8; 64]);
        }
        assert_eq!(b.counters().sent.load_acquire(), 3 * 96);
        assert_eq!(s.stats().datagrams.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[test]
    fn respects_flow_limit_and_resumes_on_status() {
        let b = buffer();
        let f1 = Fake::new();
        let (mut s, tx) = sender_to(&[&f1], &b);
        // shrink the follower's advertised limit to one datagram's worth
        tx.send(CtrlMsg::Status { from: f1.addr(), contiguous: 0, window: 96 }).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 9);
        for i in 0..4 {
            a.append(4, i, &[0u8; 64]).unwrap();
        }
        s.do_work();
        let (h, body) = f1.recv().expect("first frame");
        assert_eq!((h.position, body.len()), (0, 96)); // only up to the limit
        assert!(s.stats().flow_stalls.load(std::sync::atomic::Ordering::Relaxed) > 0);
        f1.drain();
        // status advances -> the rest flows
        tx.send(CtrlMsg::Status { from: f1.addr(), contiguous: 96, window: 1 << 20 }).unwrap();
        s.do_work();
        let (h, body) = f1.recv().expect("remaining frames");
        assert_eq!((h.position, body.len()), (96, 3 * 96));
    }

    #[test]
    fn serves_nak_to_requester_only() {
        let b = buffer();
        let (f1, f2) = (Fake::new(), Fake::new());
        let (mut s, tx) = sender_to(&[&f1, &f2], &b);
        let mut a = Appender::new(Arc::clone(&b), 9);
        for i in 0..4 {
            a.append(4, i, &[0u8; 64]).unwrap();
        }
        s.do_work(); // steady stream to both
        f1.drain();
        f2.drain();
        tx.send(CtrlMsg::Nak { from: f2.addr(), position: 96, length: 192 }).unwrap();
        s.do_work();
        let (h, body) = f2.recv().expect("retransmission");
        assert_eq!(h.kind, DGRAM_KIND_DATA);
        assert_eq!(h.position, 96);
        assert!(body.len() >= 192);
        assert!(f1.recv().is_none(), "NAK service must not fan out");
        assert_eq!(s.stats().naks_served.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn heartbeats_carry_append_position() {
        let b = buffer();
        let f1 = Fake::new();
        let (tx, rx) = mpsc::sync_channel(16);
        let _ = tx; // no control traffic in this test
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = 1; // fire every cycle
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f1.addr()],
            3,
            rx,
            cfg,
        );
        let mut a = Appender::new(Arc::clone(&b), 9);
        a.append(4, 0, &[0u8; 64]).unwrap();
        s.do_work();
        // first datagram is the data; a heartbeat follows within the cycle(s)
        let mut saw_heartbeat = false;
        for _ in 0..3 {
            s.do_work();
            while let Some((h, _)) = f1.recv() {
                if h.kind == DGRAM_KIND_HEARTBEAT {
                    assert_eq!(h.position, 96);
                    assert_eq!(h.leadership_term_id, 9);
                    saw_heartbeat = true;
                }
                if saw_heartbeat {
                    break;
                }
            }
            if saw_heartbeat {
                break;
            }
        }
        assert!(saw_heartbeat);
    }
}
```

Note for the implementer: `Fake::recv` returns `None` only after a 5 s deadline, so the `f1.recv().is_none()` assertion in the NAK test costs 5 s wall-clock once. Acceptable; do not extend the pattern.

- [ ] **Step 2: Run — expect compile failure**

Run: `cargo test -p uc_net sender`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement**

`uc_net/src/sender.rs` above the tests:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The sender agent (spec §3.1/§5): scans the log buffer from the `sent`
//! counter, packs complete frames MTU-full, and sends the identical datagram
//! to every follower (MDC-style: one scan, N sends). Serves NAKs by
//! re-reading the buffer (the buffer IS the retransmit buffer). Paced by the
//! quorum-th order statistic over follower status adverts. Batching is
//! structural — whatever whole frames accumulated, no linger. Frames are
//! COPIED out via a validated read before the syscall: with no CRC on the
//! wire, sending live ring memory could transmit silently corrupt bytes.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Instant;

use uc_log::buffer::{LogBuffer, SliceRead};
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT, DatagramHeader, MTU_DEFAULT,
    write_datagram_header,
};
use uc_protocol::v2::frame::{HEADER_LEN, align_frame_len};

use crate::fault::FaultSocket;
use crate::flow::FlowControl;

/// Control messages routed from the leader's receiver agent (Task 8).
/// Bounded channel; a dropped message is safe (NAK re-fires after backoff,
/// status re-sends on its floor).
#[derive(Debug, Clone, Copy)]
pub enum CtrlMsg {
    Nak { from: SocketAddr, position: u64, length: u32 },
    Status { from: SocketAddr, contiguous: u64, window: u32 },
}

#[derive(Debug, Clone, Copy)]
pub struct SenderConfig {
    pub mtu: usize,
    pub term_id: u32,
    /// Heartbeat interval (also drives follower tail-loss NAKs). 100 ms
    /// default per spec §6's floor; tests shrink it.
    pub heartbeat_ns: u64,
    /// Follower limit assumed before its first status arrives.
    pub initial_window: u64,
    /// Max steady-state datagrams per duty cycle (bounded work).
    pub dgrams_per_cycle: usize,
}

impl SenderConfig {
    pub fn new(term_id: u32) -> Self {
        Self {
            mtu: MTU_DEFAULT,
            term_id,
            heartbeat_ns: 100_000_000,
            initial_window: 65_536,
            dgrams_per_cycle: 8,
        }
    }
}

#[derive(Default)]
pub struct SenderStats {
    pub datagrams: AtomicU64,
    pub bytes: AtomicU64,
    pub naks_served: AtomicU64,
    pub heartbeats: AtomicU64,
    pub flow_stalls: AtomicU64,
    /// Validated read lost the race with the appender: that follower needs a
    /// journal replay session (M4) — in M2 this only counts.
    pub overruns: AtomicU64,
}

pub struct Sender {
    buffer: Arc<LogBuffer>,
    sock: FaultSocket,
    followers: Vec<SocketAddr>,
    flow: FlowControl,
    ctrl: mpsc::Receiver<CtrlMsg>,
    cfg: SenderConfig,
    sent: u64,
    /// Frame-run staging (read_run_validated output).
    run: Vec<u8>,
    /// Datagram assembly (header + run).
    scratch: Vec<u8>,
    naks: VecDeque<(SocketAddr, u64, u32)>,
    base: Instant,
    last_heartbeat_ns: u64,
    stats: Arc<SenderStats>,
}

impl Sender {
    pub fn new(
        buffer: Arc<LogBuffer>,
        sock: FaultSocket,
        followers: Vec<SocketAddr>,
        cluster_size: usize,
        ctrl: mpsc::Receiver<CtrlMsg>,
        cfg: SenderConfig,
    ) -> Sender {
        assert!(
            align_frame_len(HEADER_LEN + buffer.max_payload()) + DATAGRAM_HEADER_LEN <= cfg.mtu,
            "a max-size frame must fit one datagram (raise mtu — the jumbo-frame knob)"
        );
        let flow = FlowControl::new(&followers, cluster_size, cfg.initial_window);
        let sent = buffer.counters().sent.load_acquire();
        Sender {
            buffer,
            sock,
            followers,
            flow,
            ctrl,
            cfg,
            sent,
            run: Vec::with_capacity(cfg.mtu),
            scratch: Vec::with_capacity(cfg.mtu),
            naks: VecDeque::new(),
            base: Instant::now(),
            last_heartbeat_ns: 0,
            stats: Arc::new(SenderStats::default()),
        }
    }

    pub fn stats(&self) -> Arc<SenderStats> {
        Arc::clone(&self.stats)
    }

    fn now_ns(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    /// One duty cycle: drain control, serve one NAK, stream up to
    /// `dgrams_per_cycle` datagrams, heartbeat on interval.
    pub fn do_work(&mut self) -> bool {
        let mut did = false;

        while let Ok(m) = self.ctrl.try_recv() {
            match m {
                CtrlMsg::Status { from, contiguous, window } => {
                    self.flow.on_status(from, contiguous, window)
                }
                CtrlMsg::Nak { from, position, length } => {
                    self.naks.push_back((from, position, length))
                }
            }
            did = true;
        }

        if let Some((to, pos, len)) = self.naks.pop_front() {
            self.serve_nak(to, pos, len);
            did = true;
        }

        let append = self.buffer.counters().append.load_acquire();
        let limit = self.flow.limit();
        let budget = self.cfg.mtu - DATAGRAM_HEADER_LEN;
        let mut dgrams = 0;
        while dgrams < self.cfg.dgrams_per_cycle && self.sent < append && self.sent < limit {
            // don't read more than the flow limit allows in one datagram
            let flow_budget = (limit - self.sent).min(budget as u64) as usize;
            match self.buffer.read_run_validated(self.sent, flow_budget, &mut self.run) {
                SliceRead::Run(r) => {
                    if self.sent + r.advance > limit {
                        // a single frame overshoots the remaining window
                        // (read_run_validated always returns >= 1 frame):
                        // wait for the window to open
                        self.stats.flow_stalls.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    self.fan_out(self.sent, r.bytes);
                    self.sent += r.advance;
                    self.buffer.counters().sent.store_release(self.sent);
                    did = true;
                    dgrams += 1;
                }
                SliceRead::NotCommitted => break,
                SliceRead::Overrun => {
                    // can't happen while sent tracks append closely; counted
                    // for the M4 replay-session seam
                    self.stats.overruns.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
        }
        if self.sent < append && self.sent >= limit {
            self.stats.flow_stalls.fetch_add(1, Ordering::Relaxed);
        }

        let now = self.now_ns();
        if now - self.last_heartbeat_ns >= self.cfg.heartbeat_ns {
            self.last_heartbeat_ns = now;
            self.assemble(append, DGRAM_KIND_HEARTBEAT, 0);
            for &to in &self.followers {
                let _ = self.sock.send_to(&self.scratch, to);
            }
            self.stats.heartbeats.fetch_add(1, Ordering::Relaxed);
            did = true;
        }
        did
    }

    /// Header + the first `body_bytes` of `self.run` into `self.scratch`.
    fn assemble(&mut self, position: u64, kind: u8, body_bytes: usize) {
        self.scratch.clear();
        self.scratch.resize(DATAGRAM_HEADER_LEN, 0);
        write_datagram_header(
            &mut self.scratch,
            &DatagramHeader { position, leadership_term_id: self.cfg.term_id, kind, flags: 0 },
        );
        self.scratch.extend_from_slice(&self.run[..body_bytes]);
    }

    /// One scan, N sends (identical datagram to every follower).
    fn fan_out(&mut self, position: u64, body_bytes: usize) {
        self.assemble(position, DGRAM_KIND_DATA, body_bytes);
        for &to in &self.followers {
            let _ = self.sock.send_to(&self.scratch, to);
            self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
            self.stats.bytes.fetch_add(body_bytes as u64, Ordering::Relaxed);
        }
    }

    /// Retransmit [pos, pos+len) to ONE follower, MTU chunk by MTU chunk.
    /// `len` is capped by the follower (Task 8), so this is bounded work.
    fn serve_nak(&mut self, to: SocketAddr, pos: u64, len: u32) {
        let budget = self.cfg.mtu - DATAGRAM_HEADER_LEN;
        let end = pos + len as u64;
        let mut p = pos;
        while p < end {
            match self.buffer.read_run_validated(p, budget, &mut self.run) {
                SliceRead::Run(r) => {
                    self.assemble(p, DGRAM_KIND_DATA, r.bytes);
                    let _ = self.sock.send_to(&self.scratch, to);
                    self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
                    self.stats.bytes.fetch_add(r.bytes as u64, Ordering::Relaxed);
                    p += r.advance;
                }
                SliceRead::NotCommitted => break,
                SliceRead::Overrun => {
                    // requested bytes have left the buffer: this follower
                    // needs a journal replay session (M4)
                    self.stats.overruns.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
        }
        self.stats.naks_served.fetch_add(1, Ordering::Relaxed);
    }
}
```

Borrow-checker note: the loops borrow `self.followers` immutably while
mutating `self.sock` — disjoint FIELD accesses in the same function body, so
this compiles without cloning or index loops (which would trip
`clippy::needless_range_loop`). Method calls like `self.assemble(..)` happen
strictly before those loops. `read_run_validated(&self.buffer, .., &mut
self.run)` splits fields the same way.


- [ ] **Step 4: Run tests**

Run: `cargo test -p uc_net && cargo clippy --workspace -- -D warnings`
Expected: PASS (4 new tests; the NAK one takes ~5 s for its negative assertion).

- [ ] **Step 5: Commit**

```bash
git add uc_net/src/sender.rs uc_net/src/lib.rs
git commit -m "feat(uc_net): sender agent — MTU packing, MDC fan-out, NAK service, flow pacing, heartbeats"
```

---

### Task 8: the receiver agents — follower data path + leader control demux

**Files:**
- Create: `uc_net/src/receiver.rs`
- Modify: `uc_net/src/lib.rs` (add `pub mod receiver;`)

**Interfaces:**
- Consumes: `uc_log::{buffer::LogBuffer, writer::PositionedWriter}`, `uc_protocol::v2::{datagram::*, frame}`, `crate::{fault::FaultSocket, flow (none), rebuild::{Rebuilt, NakConfig, NakTimer}, sender::CtrlMsg}`.
- Produces (used by Tasks 9–10):
  - `pub struct FollowerConfig { pub term_id: u32, pub leader: SocketAddr, pub seed: u64, pub nak: NakConfig, pub nak_max_bytes: u32, pub status_floor_ns: u64, pub status_bytes: u64 }` with `FollowerConfig::new(term_id, leader) -> Self` defaults `{ seed: 1, nak: NakConfig::default(), nak_max_bytes: 65_536, status_floor_ns: 100_000_000, status_bytes: 0 }` (0 ⇒ `capacity/4` resolved at receiver construction).
  - `pub struct FollowerStats` — `AtomicU64` fields `{ datagrams, bytes, dropped_stale_term, dropped_dup, dropped_overrun, dropped_malformed, naks_sent, statuses_sent }`, Arc-shared.
  - `FollowerReceiver::new(buffer: Arc<LogBuffer>, sock: FaultSocket, cfg: FollowerConfig) -> Self` (starts `Rebuilt` at the buffer's `append` counter), `do_work(&mut self) -> bool`, `stats(&self) -> Arc<FollowerStats>`.
  - `LeaderReceiver::new(sock: std::net::UdpSocket, to_sender: mpsc::SyncSender<CtrlMsg>) -> io::Result<Self>` (sets nonblocking), `do_work(&mut self) -> bool`.
  - `pub(crate) fn walk_advance(body: &[u8]) -> Option<u64>` — walks the frames of a DATA payload; returns total stream advance (padding contributes its full aligned span but must be last and header-only), `None` if malformed.

**Accept rule (the invariant that makes duplicates/reordering safe):** a DATA run is applied only if `position ≥ contiguous`. Full duplicates (`position + advance ≤ contiguous`) and partial overlaps (`position < contiguous`) are dropped — retransmissions are re-requested from `contiguous` (always a frame start) by the next NAK, so a partial overlap only ever costs one backoff. Never write bytes at-or-below the frontier: readers may be reading them.

- [ ] **Step 1: Write the failing tests**

Tests at the bottom of `uc_net/src/receiver.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use uc_log::buffer::{Appender, LogBuffer, SliceRead};
    use uc_log::counters::LogCounters;
    use uc_log::region::Region;
    use uc_protocol::v2::datagram::{
        read_nak_body, read_status_body, write_datagram_header, write_nak_body,
        write_status_body, DatagramHeader, DATAGRAM_HEADER_LEN, DGRAM_KIND_DATA,
        DGRAM_KIND_HEARTBEAT, DGRAM_KIND_NAK, DGRAM_KIND_STATUS, NAK_BODY_LEN, STATUS_BODY_LEN,
    };

    const TERM: u32 = 9;

    fn buffer() -> Arc<LogBuffer> {
        let counters = Arc::new(LogCounters::new());
        Arc::new(LogBuffer::new(Region::heap_zeroed(1 << 16), counters, 256))
    }

    /// A fake leader endpoint: a raw socket we send DATA from and receive
    /// NAK/status on.
    struct FakeLeader {
        sock: FaultSocket,
    }
    impl FakeLeader {
        fn new() -> Self {
            Self { sock: FaultSocket::bind("127.0.0.1:0").unwrap() }
        }
        fn addr(&self) -> SocketAddr {
            self.sock.local_addr().unwrap()
        }
        fn send(&mut self, to: SocketAddr, kind: u8, position: u64, term: u32, body: &[u8]) {
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
            write_datagram_header(
                &mut d,
                &DatagramHeader { position, leadership_term_id: term, kind, flags: 0 },
            );
            d.extend_from_slice(body);
            self.sock.send_to(&d, to).unwrap();
        }
        fn recv(&self) -> Option<(DatagramHeader, Vec<u8>)> {
            let mut buf = [0u8; 2048];
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if let Some((n, _)) = self.sock.recv_from(&mut buf).unwrap() {
                    return Some((
                        read_datagram_header(&buf),
                        buf[DATAGRAM_HEADER_LEN..n].to_vec(),
                    ));
                }
                std::thread::yield_now();
            }
            None
        }
    }

    /// Frames as a leader buffer would produce them (via the real appender +
    /// run read — keeps wire bytes honest).
    fn frame_runs(payloads: &[&[u8]], chunk: usize) -> Vec<(u64, Vec<u8>, u64)> {
        let b = buffer();
        let mut a = Appender::new(Arc::clone(&b), TERM);
        for (i, p) in payloads.iter().enumerate() {
            a.append(4, i as u64, p).unwrap();
        }
        let mut runs = Vec::new();
        let mut pos = 0u64;
        let mut out = Vec::new();
        while let SliceRead::Run(r) = b.read_run_validated(pos, chunk, &mut out) {
            runs.push((pos, out[..r.bytes].to_vec(), r.advance));
            pos += r.advance;
        }
        runs
    }

    fn follower(b: &Arc<LogBuffer>, leader: SocketAddr) -> FollowerReceiver {
        let mut cfg = FollowerConfig::new(TERM, leader);
        cfg.nak = NakConfig { delay_min_ns: 1, delay_max_ns: 2, backoff_ns: 1_000_000 };
        cfg.status_floor_ns = u64::MAX; // no time-driven status in unit tests
        FollowerReceiver::new(Arc::clone(b), FaultSocket::bind("127.0.0.1:0").unwrap(), cfg)
    }

    fn drive_until<F: Fn() -> bool>(r: &mut FollowerReceiver, pred: F) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pred() {
            assert!(Instant::now() < deadline, "condition never reached");
            r.do_work();
            std::thread::yield_now();
        }
    }

    #[test]
    fn in_order_data_lands_and_advances_append() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let runs = frame_runs(&[b"aaaa", b"bb", b"cccccc"], 4096);
        assert_eq!(runs.len(), 1);
        let (pos, bytes, advance) = &runs[0];
        leader.send(to, DGRAM_KIND_DATA, *pos, TERM, bytes);
        drive_until(&mut r, || b.counters().append.load_acquire() == *advance);
        let s = b.recordable_slice(0, 1 << 20);
        assert_eq!(s.len(), *advance as usize);
        assert_eq!(&s[32..36], b"aaaa");
    }

    #[test]
    fn gap_naks_then_fill_converges() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let runs = frame_runs(&[&[1u8; 64], &[2u8; 64], &[3u8; 64]], 96); // one frame per run
        // deliver run 2, skip runs 0 and 1 -> gap [0, 192)
        leader.send(to, DGRAM_KIND_DATA, runs[2].0, TERM, &runs[2].1);
        // NAK must arrive, asking from the contiguous frontier (0)
        let mut got_nak = None;
        let deadline = Instant::now() + Duration::from_secs(5);
        while got_nak.is_none() {
            assert!(Instant::now() < deadline);
            r.do_work();
            if let Some((h, body)) = leader.recv() {
                if h.kind == DGRAM_KIND_NAK {
                    got_nak = Some(read_nak_body(&body));
                }
            }
        }
        let nak = got_nak.unwrap();
        assert_eq!(nak.position, 0);
        assert_eq!(nak.length as u64, 192);
        // serve the retransmission -> converges, ooo run absorbed
        leader.send(to, DGRAM_KIND_DATA, runs[0].0, TERM, &runs[0].1);
        leader.send(to, DGRAM_KIND_DATA, runs[1].0, TERM, &runs[1].1);
        drive_until(&mut r, || b.counters().append.load_acquire() == 3 * 96);
        assert!(r.stats().naks_sent.load(std::sync::atomic::Ordering::Relaxed) >= 1);
    }

    #[test]
    fn heartbeat_reveals_tail_loss() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        // nothing delivered; heartbeat says the leader is at 192
        leader.send(to, DGRAM_KIND_HEARTBEAT, 192, TERM, &[]);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(Instant::now() < deadline, "no tail NAK");
            r.do_work();
            if let Some((h, body)) = leader.recv() {
                if h.kind == DGRAM_KIND_NAK {
                    let nak = read_nak_body(&body);
                    assert_eq!((nak.position, nak.length), (0, 192));
                    break;
                }
            }
        }
    }

    #[test]
    fn drops_stale_term_dups_and_malformed() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let runs = frame_runs(&[&[1u8; 64], &[2u8; 64]], 4096);
        let (pos, bytes, advance) = &runs[0];
        leader.send(to, DGRAM_KIND_DATA, *pos, TERM - 1, bytes); // stale term
        leader.send(to, DGRAM_KIND_DATA, *pos, TERM, &bytes[..16]); // malformed (torn frame)
        leader.send(to, DGRAM_KIND_DATA, *pos, TERM, bytes); // good
        drive_until(&mut r, || b.counters().append.load_acquire() == *advance);
        leader.send(to, DGRAM_KIND_DATA, *pos, TERM, bytes); // full dup
        // the dup arrives asynchronously: wait for it to be counted, then
        // assert the log did not move
        let st = r.stats();
        use std::sync::atomic::Ordering::Relaxed;
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_dup.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "dup never observed");
            r.do_work();
        }
        assert_eq!(b.counters().append.load_acquire(), *advance);
        assert_eq!(st.dropped_stale_term.load(Relaxed), 1);
        assert_eq!(st.dropped_malformed.load(Relaxed), 1);
        assert_eq!(st.dropped_dup.load(Relaxed), 1);
    }

    #[test]
    fn status_advertises_contiguous_and_window() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut cfg = FollowerConfig::new(TERM, leader.addr());
        cfg.status_bytes = 96; // status on every frame's worth of progress
        cfg.status_floor_ns = u64::MAX;
        let mut r = FollowerReceiver::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            cfg,
        );
        let to = r.local_addr();
        let runs = frame_runs(&[&[7u8; 64]], 4096);
        leader.send(to, DGRAM_KIND_DATA, runs[0].0, TERM, &runs[0].1);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(Instant::now() < deadline, "no status");
            r.do_work();
            if let Some((h, body)) = leader.recv() {
                if h.kind == DGRAM_KIND_STATUS {
                    let s = read_status_body(&body);
                    assert_eq!(s.contiguous_position, 96);
                    // durable 0 + capacity 65536 - contiguous 96
                    assert_eq!(s.receive_window, 65536 - 96);
                    break;
                }
            }
        }
    }

    #[test]
    fn walk_advance_handles_messages_padding_and_garbage() {
        // real frames via the appender for honesty
        let runs = frame_runs(&[&[1u8; 64], &[2u8; 32]], 4096);
        let bytes = &runs[0].1;
        assert_eq!(walk_advance(bytes), Some(runs[0].2));
        assert_eq!(walk_advance(&bytes[..16]), None); // torn header
        assert_eq!(walk_advance(&[0u8; 32]), None); // zero length
        // padding-only run from a wrapping buffer
        let b = buffer();
        let c = Arc::clone(b.counters());
        let mut a = Appender::new(Arc::clone(&b), TERM);
        let per = 96u64;
        let cap = b.capacity();
        let fill = (cap / per) as usize; // 682 frames -> 65472, 64 short of the wrap
        for i in 0..fill {
            a.append(4, i as u64, &[0u8; 64]).unwrap();
            c.durable.store_release(a.position());
        }
        let pad_pos = a.position();
        a.append(4, 999, &[0u8; 64]).unwrap(); // forces padding
        let mut out = Vec::new();
        if let SliceRead::Run(r) = b.read_run_validated(pad_pos, 1392, &mut out) {
            assert!(r.advance > r.bytes as u64); // padding tail
            assert_eq!(walk_advance(&out[..r.bytes]), Some(r.advance));
        } else {
            panic!("expected padding run");
        }
    }

    #[test]
    fn leader_receiver_demuxes_control_to_sender_channel() {
        let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = sock.local_addr().unwrap();
        let (tx, rx) = mpsc::sync_channel(16);
        let mut lr = LeaderReceiver::new(sock, tx).unwrap();
        let mut f = FakeLeader::new(); // reuse as a fake follower endpoint
        let mut nb = [0u8; NAK_BODY_LEN];
        write_nak_body(&mut nb, &uc_protocol::v2::datagram::NakBody { position: 96, length: 192 });
        f.send(addr, DGRAM_KIND_NAK, 0, TERM, &nb);
        let mut sb = [0u8; STATUS_BODY_LEN];
        write_status_body(
            &mut sb,
            &uc_protocol::v2::datagram::StatusBody { contiguous_position: 4096, receive_window: 1 << 20 },
        );
        f.send(addr, DGRAM_KIND_STATUS, 0, TERM, &sb);
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut got = Vec::new();
        while got.len() < 2 {
            assert!(Instant::now() < deadline);
            lr.do_work();
            while let Ok(m) = rx.try_recv() {
                got.push(m);
            }
        }
        assert!(matches!(got[0], CtrlMsg::Nak { position: 96, length: 192, .. }));
        assert!(matches!(got[1], CtrlMsg::Status { contiguous: 4096, .. }));
    }
}
```

- [ ] **Step 2: Run — expect compile failure**

Run: `cargo test -p uc_net receiver`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement**

`uc_net/src/receiver.rs` above the tests:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Receiver agents (spec §3.1/§5).
//!
//! Follower: DATA datagrams land position-addressed in the local buffer;
//! contiguity is tracked over absolute positions (never scanned from buffer
//! bytes); the `append` counter advances (Release) only to the contiguous
//! frontier, so the local archive and any reader see exactly the leader's
//! committed-frame discipline. Gaps NAK after a randomized ~RTT delay;
//! heartbeats reveal tail loss; statuses advertise contiguous + window
//! (quarter-window cadence with a time floor).
//!
//! Leader: the same socket's inbound side — demuxes NAK/status to the sender
//! agent over a bounded channel (control is kHz; a full channel drops, and
//! NAK backoff / status refresh recover).

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Instant;

use uc_log::buffer::LogBuffer;
use uc_log::writer::PositionedWriter;
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT, DGRAM_KIND_NAK,
    DGRAM_KIND_STATUS, DatagramHeader, NAK_BODY_LEN, NakBody, STATUS_BODY_LEN, StatusBody,
    read_datagram_header, read_nak_body, read_status_body, write_datagram_header,
    write_nak_body, write_status_body,
};
use uc_protocol::v2::frame::{self, FRAME_TYPE_PADDING, HEADER_LEN, align_frame_len};

use crate::fault::FaultSocket;
use crate::rebuild::{NakConfig, NakTimer, Rebuilt};
use crate::sender::CtrlMsg;

/// Walk the frames of a DATA payload: total stream advance, or None if
/// malformed (torn frame, zero length, padding not last / not header-only).
pub(crate) fn walk_advance(body: &[u8]) -> Option<u64> {
    let mut o = 0usize;
    let mut adv = 0u64;
    while o < body.len() {
        if o + HEADER_LEN > body.len() {
            return None;
        }
        let h = frame::read_header(&body[o..]);
        if (h.length as usize) < HEADER_LEN {
            return None;
        }
        let aligned = align_frame_len(h.length as usize);
        adv += aligned as u64;
        if h.frame_type == FRAME_TYPE_PADDING {
            // padding is sent header-only and is always the run's last frame
            return (o + HEADER_LEN == body.len()).then_some(adv);
        }
        o += aligned;
        if o > body.len() {
            return None;
        }
    }
    Some(adv)
}

#[derive(Debug, Clone, Copy)]
pub struct FollowerConfig {
    pub term_id: u32,
    pub leader: SocketAddr,
    pub seed: u64,
    pub nak: NakConfig,
    /// Cap per NAK request; a bigger gap is re-requested as it drains.
    pub nak_max_bytes: u32,
    pub status_floor_ns: u64,
    /// Status every this many rebuilt bytes (0 = capacity/4, spec §5's
    /// quarter-window).
    pub status_bytes: u64,
}

impl FollowerConfig {
    pub fn new(term_id: u32, leader: SocketAddr) -> Self {
        Self {
            term_id,
            leader,
            seed: 1,
            nak: NakConfig::default(),
            nak_max_bytes: 65_536,
            status_floor_ns: 100_000_000,
            status_bytes: 0,
        }
    }
}

#[derive(Default)]
pub struct FollowerStats {
    pub datagrams: AtomicU64,
    pub bytes: AtomicU64,
    pub dropped_stale_term: AtomicU64,
    pub dropped_dup: AtomicU64,
    pub dropped_overrun: AtomicU64,
    pub dropped_malformed: AtomicU64,
    pub naks_sent: AtomicU64,
    pub statuses_sent: AtomicU64,
}

pub struct FollowerReceiver {
    buffer: Arc<LogBuffer>,
    writer: PositionedWriter,
    sock: FaultSocket,
    cfg: FollowerConfig,
    status_bytes: u64,
    rebuilt: Rebuilt,
    nak: NakTimer,
    leader_append: u64,
    base: Instant,
    last_status_ns: u64,
    status_at: u64,
    recv_buf: Vec<u8>,
    stats: Arc<FollowerStats>,
}

impl FollowerReceiver {
    pub fn new(buffer: Arc<LogBuffer>, sock: FaultSocket, cfg: FollowerConfig) -> Self {
        let start = buffer.counters().append.load_acquire();
        let status_bytes =
            if cfg.status_bytes == 0 { buffer.capacity() / 4 } else { cfg.status_bytes };
        let writer = PositionedWriter::new(Arc::clone(&buffer));
        Self {
            buffer,
            writer,
            sock,
            status_bytes,
            rebuilt: Rebuilt::new(start),
            nak: NakTimer::new(cfg.nak, cfg.seed),
            cfg,
            leader_append: start,
            base: Instant::now(),
            last_status_ns: 0,
            status_at: start,
            recv_buf: vec![0u8; 65_536],
            stats: Arc::new(FollowerStats::default()),
        }
    }

    pub fn stats(&self) -> Arc<FollowerStats> {
        Arc::clone(&self.stats)
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.sock.local_addr().expect("bound socket")
    }

    fn now_ns(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    /// One duty cycle: drain up to 64 datagrams, then NAK/status upkeep.
    pub fn do_work(&mut self) -> bool {
        let mut did = false;
        for _ in 0..64 {
            let mut buf = std::mem::take(&mut self.recv_buf);
            let r = self.sock.recv_from(&mut buf);
            let got = match r {
                Ok(Some((n, from))) => Some((n, from)),
                _ => None,
            };
            if let Some((n, _from)) = got {
                self.on_datagram(&buf[..n]);
                did = true;
            }
            self.recv_buf = buf;
            if got.is_none() {
                break;
            }
        }
        did |= self.upkeep();
        did
    }

    fn on_datagram(&mut self, d: &[u8]) {
        use Ordering::Relaxed;
        if d.len() < DATAGRAM_HEADER_LEN {
            self.stats.dropped_malformed.fetch_add(1, Relaxed);
            return;
        }
        let h = read_datagram_header(d);
        if h.leadership_term_id != self.cfg.term_id {
            self.stats.dropped_stale_term.fetch_add(1, Relaxed);
            return;
        }
        self.stats.datagrams.fetch_add(1, Relaxed);
        match h.kind {
            DGRAM_KIND_DATA => {
                let body = &d[DATAGRAM_HEADER_LEN..];
                let contiguous = self.rebuilt.contiguous();
                // Accept rule: never rewrite at-or-below the frontier —
                // readers may be reading those bytes. Partial overlaps are
                // re-requested from `contiguous` by the next NAK.
                if h.position < contiguous {
                    self.stats.dropped_dup.fetch_add(1, Relaxed);
                    return;
                }
                let Some(advance) = walk_advance(body) else {
                    self.stats.dropped_malformed.fetch_add(1, Relaxed);
                    return;
                };
                if !self.writer.write_run(h.position, body) {
                    // beyond durable + capacity (archive lagging) or wrap-
                    // crossing garbage: flow control should prevent the
                    // former; NAK/replay recovers either way
                    self.stats.dropped_overrun.fetch_add(1, Relaxed);
                    return;
                }
                self.stats.bytes.fetch_add(body.len() as u64, Relaxed);
                if self.rebuilt.insert(h.position, h.position + advance) {
                    self.buffer.counters().append.store_release(self.rebuilt.contiguous());
                }
            }
            DGRAM_KIND_HEARTBEAT => {
                self.leader_append = self.leader_append.max(h.position);
            }
            _ => {} // control kinds for the consensus agent: M3
        }
    }

    fn upkeep(&mut self) -> bool {
        use Ordering::Relaxed;
        let mut did = false;
        let now = self.now_ns();
        let contiguous = self.rebuilt.contiguous();

        // Gap = missing bytes before out-of-order data, else missing tail
        // revealed by the leader's heartbeat position.
        let gap = self.rebuilt.first_gap().or({
            if self.leader_append > contiguous {
                Some((contiguous, self.leader_append))
            } else {
                None
            }
        });
        if let Some((start, end)) = self.nak.poll(gap, now) {
            let len = (end - start).min(self.cfg.nak_max_bytes as u64) as u32;
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN + NAK_BODY_LEN];
            write_datagram_header(
                &mut d,
                &DatagramHeader {
                    position: 0,
                    leadership_term_id: self.cfg.term_id,
                    kind: DGRAM_KIND_NAK,
                    flags: 0,
                },
            );
            write_nak_body(&mut d[DATAGRAM_HEADER_LEN..], &NakBody { position: start, length: len });
            let _ = self.sock.send_to(&d, self.cfg.leader);
            self.stats.naks_sent.fetch_add(1, Relaxed);
            did = true;
        }

        // Status: every quarter-window of progress, or on the time floor.
        if contiguous - self.status_at >= self.status_bytes
            || now - self.last_status_ns >= self.cfg.status_floor_ns
        {
            let durable = self.buffer.counters().durable.load_acquire();
            let window = (durable + self.buffer.capacity() - contiguous) as u32;
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN + STATUS_BODY_LEN];
            write_datagram_header(
                &mut d,
                &DatagramHeader {
                    position: 0,
                    leadership_term_id: self.cfg.term_id,
                    kind: DGRAM_KIND_STATUS,
                    flags: 0,
                },
            );
            write_status_body(
                &mut d[DATAGRAM_HEADER_LEN..],
                &StatusBody { contiguous_position: contiguous, receive_window: window },
            );
            let _ = self.sock.send_to(&d, self.cfg.leader);
            self.status_at = contiguous;
            self.last_status_ns = now;
            self.stats.statuses_sent.fetch_add(1, Relaxed);
            did = true;
        }
        did
    }
}

/// The leader-side inbound demux: NAK/status → the sender's channel.
/// (Vote/append-position kinds get a consensus channel in M3.)
pub struct LeaderReceiver {
    sock: UdpSocket,
    to_sender: mpsc::SyncSender<CtrlMsg>,
    recv_buf: Vec<u8>,
    pub dropped_full: u64,
}

impl LeaderReceiver {
    pub fn new(sock: UdpSocket, to_sender: mpsc::SyncSender<CtrlMsg>) -> io::Result<Self> {
        sock.set_nonblocking(true)?;
        Ok(Self { sock, to_sender, recv_buf: vec![0u8; 2048], dropped_full: 0 })
    }

    pub fn do_work(&mut self) -> bool {
        let mut did = false;
        for _ in 0..64 {
            let (n, from) = match self.sock.recv_from(&mut self.recv_buf) {
                Ok(x) => x,
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::ConnectionRefused
                        || e.kind() == io::ErrorKind::ConnectionReset =>
                {
                    break;
                }
                Err(_) => break,
            };
            did = true;
            if n < DATAGRAM_HEADER_LEN {
                continue;
            }
            let h = read_datagram_header(&self.recv_buf);
            let body = &self.recv_buf[DATAGRAM_HEADER_LEN..n];
            let msg = match h.kind {
                DGRAM_KIND_NAK if body.len() >= NAK_BODY_LEN => {
                    let b = read_nak_body(body);
                    Some(CtrlMsg::Nak { from, position: b.position, length: b.length })
                }
                DGRAM_KIND_STATUS if body.len() >= STATUS_BODY_LEN => {
                    let b = read_status_body(body);
                    Some(CtrlMsg::Status {
                        from,
                        contiguous: b.contiguous_position,
                        window: b.receive_window,
                    })
                }
                _ => None,
            };
            if let Some(m) = msg {
                if self.to_sender.try_send(m).is_err() {
                    self.dropped_full += 1; // safe: NAK backoff / status floor recover
                }
            }
        }
        did
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p uc_net && cargo clippy --workspace -- -D warnings`
Expected: PASS (8 new tests).

- [ ] **Step 5: Commit**

```bash
git add uc_net/src/receiver.rs uc_net/src/lib.rs
git commit -m "feat(uc_net): follower receiver (rebuild/NAK/status) + leader control demux"
```

---

### Task 9: the 3-node localhost harness — end-to-end replication under faults

**Files:**
- Create: `uc_net/tests/replication.rs`

**Interfaces:**
- Consumes: everything above; `uc_log::{agent::AgentRunner, archive::{Archive, ArchiveConfig}}`.
- Produces: the L2 proof (spec §8) that the M2 pipeline converges: clean, under 1 % loss, under dup+reorder; stale terms rejected; quorum pacing (a dead follower doesn't stall the stream).

- [ ] **Step 1: Write the harness + tests**

Full file `uc_net/tests/replication.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! End-to-end replication over real loopback UDP: a leader (appender +
//! sender + control demux + archive) streams to two followers (receiver +
//! archive) under injected faults. Convergence asserts positions AND journal
//! content: REPLAYED FRAME STREAMS are compared, not raw journal bytes —
//! block boundaries legitimately differ between nodes (poll timing) and
//! padding spans carry node-local stale bytes (replay skips padding).
//!
//! Timing-sensitive by nature (real sockets, real threads): all assertions
//! are eventual-convergence with hard deadlines — a hang is a red test, not
//! a stuck CI job (M1 T8c lesson).

use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use uc_log::agent::{AgentRunner, IdleStrategy};
use uc_log::archive::{Archive, ArchiveConfig, ReplayFrame};
use uc_log::buffer::{AppendError, Appender, LogBuffer};
use uc_log::counters::{LogCounters, PaddedAtomicU64};
use uc_log::region::Region;
use uc_net::fault::{FaultConfig, FaultSocket};
use uc_net::rebuild::NakConfig;
use uc_net::receiver::{FollowerConfig, FollowerReceiver, LeaderReceiver};
use uc_net::sender::{Sender, SenderConfig};

const TERM: u32 = 3;
const CAP: u64 = 1 << 20; // 1 MiB buffers, identical on every node
const MAX_PAYLOAD: usize = 256;

/// Small segments so parallel test journals fit the quota'd tmpfs (M1 lesson).
fn test_cfg(dir: &std::path::Path) -> ArchiveConfig {
    ArchiveConfig { segment_size_bytes: 4 * 1024 * 1024, ..ArchiveConfig::new(dir) }
}

fn buffer() -> Arc<LogBuffer> {
    let counters = Arc::new(LogCounters::new());
    Arc::new(LogBuffer::new(Region::heap_zeroed(CAP as usize), counters, MAX_PAYLOAD))
}

struct Node {
    buffer: Arc<LogBuffer>,
    dir: tempfile::TempDir,
    agents: Vec<AgentRunner>,
}

impl Node {
    /// Join all agents (dropping their `Archive`s — required before the
    /// journal dir can be reopened for replay) and hand back the dir.
    fn stop(self) -> tempfile::TempDir {
        for a in self.agents {
            a.stop();
        }
        self.dir
    }
}

fn spawn_archive(name: &str, buffer: &Arc<LogBuffer>, dir: &std::path::Path) -> AgentRunner {
    let mut archive = Archive::open(test_cfg(dir)).unwrap();
    let b = Arc::clone(buffer);
    AgentRunner::spawn(name, IdleStrategy::Yield, move || {
        archive.do_work(&b).expect("archive fail-stop")
    })
    .unwrap()
}

struct Follower {
    node: Node,
    stats: Arc<uc_net::receiver::FollowerStats>,
    addr: SocketAddr,
}

fn spawn_follower(name: &str, leader: SocketAddr, faults: FaultConfig) -> Follower {
    let mut sock = FaultSocket::bind("127.0.0.1:0").unwrap();
    let addr = sock.local_addr().unwrap();
    sock.set_faults(faults);
    let buffer = buffer();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = FollowerConfig::new(TERM, leader);
    cfg.seed = faults.seed.wrapping_add(addr.port() as u64);
    cfg.status_floor_ns = 5_000_000; // 5 ms: keep flow adverts fresh under test loads
    cfg.nak = NakConfig { delay_min_ns: 100_000, delay_max_ns: 500_000, backoff_ns: 2_000_000 };
    let mut rx = FollowerReceiver::new(Arc::clone(&buffer), sock, cfg);
    let stats = rx.stats();
    let rxa = AgentRunner::spawn(&format!("{name}-rx"), IdleStrategy::Yield, move || rx.do_work())
        .unwrap();
    let ara = spawn_archive(&format!("{name}-ar"), &buffer, dir.path());
    Follower { node: Node { buffer, dir, agents: vec![rxa, ara] }, stats, addr }
}

struct Leader {
    node: Node,
    stats: Arc<uc_net::sender::SenderStats>,
}

/// The leader socket binds FIRST (followers need its address) — pass it in.
fn spawn_leader(raw: UdpSocket, followers: Vec<SocketAddr>, faults: FaultConfig) -> Leader {
    let buffer = buffer();
    let dir = tempfile::tempdir().unwrap();
    let recv = raw.try_clone().unwrap();
    let mut send = FaultSocket::from_socket(raw).unwrap();
    send.set_faults(faults);
    let (tx, rx) = mpsc::sync_channel(1024);
    let mut cfg = SenderConfig::new(TERM);
    cfg.heartbeat_ns = 2_000_000; // 2 ms: quick tail-loss detection in tests
    let mut sender = Sender::new(Arc::clone(&buffer), send, followers, 3, rx, cfg);
    let stats = sender.stats();
    let txa =
        AgentRunner::spawn("leader-tx", IdleStrategy::Yield, move || sender.do_work()).unwrap();
    let mut lr = LeaderReceiver::new(recv, tx).unwrap();
    let lra =
        AgentRunner::spawn("leader-ctrl", IdleStrategy::Yield, move || lr.do_work()).unwrap();
    let ara = spawn_archive("leader-ar", &buffer, dir.path());
    Leader { node: Node { buffer, dir, agents: vec![txa, lra, ara] }, stats }
}

/// Append `n_msgs` 64 B messages, pacing against the sender so the appender
/// never laps it (the M2 stand-in for admission control). Returns the end
/// position.
fn load(leader: &Arc<LogBuffer>, n_msgs: u64) -> u64 {
    let mut a = Appender::new(Arc::clone(leader), TERM);
    let deadline = Instant::now() + Duration::from_secs(60);
    for i in 0..n_msgs {
        loop {
            assert!(Instant::now() < deadline, "load timed out at msg {i}");
            match a.append(1, i, &[i as u8; 64]) {
                Ok(_) => break,
                Err(AppendError::WouldOverrun) => std::thread::yield_now(),
                Err(e) => panic!("{e}"),
            }
        }
        while a.position() > leader.counters().sent.load_acquire() + CAP / 2 {
            assert!(Instant::now() < deadline, "sender never caught up");
            std::thread::yield_now();
        }
    }
    a.position()
}

fn await_pos(c: &PaddedAtomicU64, target: u64, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let v = c.load_acquire();
        if v >= target {
            return;
        }
        assert!(Instant::now() < deadline, "{what} stuck at {v} < {target}");
        std::thread::yield_now();
    }
}

fn replayed(dir: &std::path::Path) -> Vec<ReplayFrame> {
    let arch = Archive::open(test_cfg(dir)).unwrap();
    let mut r = arch.replay_from(0).unwrap();
    let mut out = Vec::new();
    while let Some(f) = r.next().unwrap() {
        out.push(f);
    }
    out
}

/// Wait until every node has append+durable at `end`, stop everything, and
/// assert every follower's replayed frame stream equals the leader's.
fn converge_and_compare(leader: Leader, followers: Vec<Follower>, end: u64) {
    for f in &followers {
        await_pos(&f.node.buffer.counters().append, end, "follower append");
        await_pos(&f.node.buffer.counters().durable, end, "follower durable");
    }
    await_pos(&leader.node.buffer.counters().durable, end, "leader durable");
    let ldir = leader.node.stop();
    let golden = replayed(ldir.path());
    assert!(!golden.is_empty());
    for f in followers {
        let fdir = f.node.stop();
        assert_eq!(replayed(fdir.path()), golden, "follower journal diverged from leader");
    }
}

#[test]
fn clean_stream_converges_and_journals_match() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let clean = FaultConfig::default();
    let f1 = spawn_follower("c-f1", leader_addr, clean);
    let f2 = spawn_follower("c-f2", leader_addr, clean);
    let leader = spawn_leader(raw, vec![f1.addr, f2.addr], clean);
    let end = load(&leader.node.buffer, 5_000);
    converge_and_compare(leader, vec![f1, f2], end);
}

#[test]
fn one_percent_loss_recovers_via_nak() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1 = spawn_follower("l-f1", leader_addr, FaultConfig::default());
    let f2 = spawn_follower("l-f2", leader_addr, FaultConfig::default());
    let (s1, s2) = (Arc::clone(&f1.stats), Arc::clone(&f2.stats));
    // 1% loss on the leader's send side (data AND heartbeats drop; the NAK
    // delay + backoff recover both mid-stream gaps and tail loss)
    let faults = FaultConfig { seed: 20_260_710, drop_per_million: 10_000, ..Default::default() };
    let leader = spawn_leader(raw, vec![f1.addr, f2.addr], faults);
    let sstats = Arc::clone(&leader.stats);
    let end = load(&leader.node.buffer, 5_000);
    converge_and_compare(leader, vec![f1, f2], end);
    let naks = s1.naks_sent.load(Ordering::Relaxed) + s2.naks_sent.load(Ordering::Relaxed);
    assert!(naks > 0, "1% loss must exercise the NAK path");
    assert!(sstats.naks_served.load(Ordering::Relaxed) > 0);
    assert_eq!(sstats.overruns.load(Ordering::Relaxed), 0, "no replay-needed under 1% loss");
}

#[test]
fn dup_and_reorder_converge() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1 = spawn_follower("d-f1", leader_addr, FaultConfig::default());
    let f2 = spawn_follower("d-f2", leader_addr, FaultConfig::default());
    let faults = FaultConfig {
        seed: 7,
        dup_per_million: 20_000,
        reorder_per_million: 20_000,
        ..Default::default()
    };
    let leader = spawn_leader(raw, vec![f1.addr, f2.addr], faults);
    let end = load(&leader.node.buffer, 5_000);
    // dups are dropped by position, reordering is absorbed by Rebuilt —
    // convergence + identical replay IS the assertion
    converge_and_compare(leader, vec![f1, f2], end);
}

#[test]
fn stale_term_stream_is_ignored() {
    use uc_protocol::v2::datagram::{
        DATAGRAM_HEADER_LEN, DGRAM_KIND_DATA, DatagramHeader, write_datagram_header,
    };
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1 = spawn_follower("s-f1", leader_addr, FaultConfig::default());
    let stats = Arc::clone(&f1.stats);
    let fbuf = Arc::clone(&f1.node.buffer);
    let faddr = f1.addr;
    let leader = spawn_leader(raw, vec![f1.addr], FaultConfig::default());
    let end = load(&leader.node.buffer, 1_000);
    await_pos(&fbuf.counters().append, end, "follower append");

    // a "previous leader" blasts stale-term DATA at fresh positions
    let mut ghost = FaultSocket::bind("127.0.0.1:0").unwrap();
    for _ in 0..3 {
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN + 96];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: end,
                leadership_term_id: TERM - 1,
                kind: DGRAM_KIND_DATA,
                flags: 0,
            },
        );
        ghost.send_to(&d, faddr).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while stats.dropped_stale_term.load(Ordering::Relaxed) < 3 {
        assert!(Instant::now() < deadline, "stale datagrams never observed");
        std::thread::yield_now();
    }
    assert_eq!(fbuf.counters().append.load_acquire(), end, "stale term advanced the log");
    converge_and_compare(leader, vec![f1], end);
}

#[test]
fn dead_follower_does_not_stall_the_quorum() {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1 = spawn_follower("q-f1", leader_addr, FaultConfig::default());
    // Follower B is a bound socket with NO agents: silent — no statuses, no
    // NAKs. Its flow limit stays at initial_window (64 KiB) forever. Keep the
    // socket alive so sends don't turn into ICMP noise.
    let dead = FaultSocket::bind("127.0.0.1:0").unwrap();
    let leader =
        spawn_leader(raw, vec![f1.addr, dead.local_addr().unwrap()], FaultConfig::default());
    // ~3 MiB stream: several times BOTH the dead follower's window and CAP —
    // only quorum pacing (3 nodes -> the faster follower) lets this finish.
    let end = load(&leader.node.buffer, 32_768);
    assert!(end >= 3 * CAP);
    assert_eq!(leader.node.buffer.counters().sent.load_acquire(), end);
    converge_and_compare(leader, vec![f1], end);
    drop(dead);
}
```

Harness invariants (bake these in, they are load-bearing):
- Total journal data per test stays small (5 000 × 96 B ≈ 480 KB for the steady tests; the pacing test ~3 MiB × 2 nodes) — tempdirs live on the quota'd tmpfs.
- Every wait has a deadline + panic message (no unbounded spins — M1 T8c lesson).
- `converge_and_compare` stops all agents BEFORE reopening journals: `AgentRunner::stop` joins, dropping the closure and its `Archive`, which releases the journal dir for `Archive::open`.

- [ ] **Step 2: Run the harness**

Run: `cargo test -p uc_net --test replication`
Expected: all five tests pass. They are timing-sensitive by nature (real UDP + real threads): deadlines are generous (60 s) and assertions are eventual-convergence only. If a test flakes under default parallelism, do NOT serialize the whole binary — find the resource contention (usually tmpfs quota in the tempdirs: shrink the loads) and fix it.

- [ ] **Step 3: Run everything**

Run: `cargo test -p uc_net && cargo test -p uc_log && cargo clippy --workspace -- -D warnings`
Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add uc_net/tests/replication.rs
git commit -m "test(uc_net): 3-node loopback replication harness — loss/dup/reorder/stale-term/quorum-pacing"
```

---

### Task 10: `m2_gate` example + gate run + benchmark doc

**Files:**
- Create: `uc_net/examples/m2_gate.rs`
- Create: `docs/benchmarks/uc2-m2-gate-2026-07-10.md` (written from the run's output)

**Interfaces:**
- Consumes: everything above.
- Produces: the M2 gate measurement (spec §9: **≥ 100 MB/s per follower, durable positions keeping pace, resilient to 0.1–1 % injected loss**).

- [ ] **Step 1: Write the example**

`uc_net/examples/m2_gate.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M2 gate: replication stream throughput (spec §9: >= 100 MB/s per
//! follower, durable positions keeping pace, resilient to 0.1-1% loss).
//!
//! Local (single host, loopback, all three nodes in-process):
//!   cargo run -p uc_net --release --example m2_gate -- local <journal_root> \
//!       [secs=10] [payload=64] [loss_ppm=0] [buffer_mib=256]
//!
//! Fleet (one process per host; start followers first):
//!   m2_gate follower <bind_addr> <journal_dir> <leader_addr> [buffer_mib]
//!   m2_gate leader <bind_addr> <journal_dir> <f1_addr> <f2_addr> \
//!       [secs=10] [payload=64] [loss_ppm=0] [buffer_mib=256]
//!
//! Journal dirs MUST be on a real filesystem (on the dev sandbox:
//! /home/claude/..., NEVER /tmp — RAM-backed tmpfs). Buffers are heap.
//! UC2_M2_MAX_BYTES caps the appended stream (bounded runs on small disks).
//!
//! Headline = drain-inclusive durable rate: ONE wall clock around load +
//! drain (every byte fsync'd on every node before the clock stops) — the M1
//! gate's accounting lesson (docs/benchmarks/uc2-m1-gate-2026-07-09.md).

use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use uc_log::agent::{AgentRunner, IdleStrategy};
use uc_log::archive::{Archive, ArchiveConfig};
use uc_log::buffer::{AppendError, Appender, LogBuffer};
use uc_log::counters::LogCounters;
use uc_log::region::Region;
use uc_net::fault::{FaultConfig, FaultSocket};
use uc_net::receiver::{FollowerConfig, FollowerReceiver, FollowerStats, LeaderReceiver};
use uc_net::sender::{Sender, SenderConfig, SenderStats};

const TERM: u32 = 1;
const MAX_PAYLOAD: usize = 1024;

fn buffer(mib: usize) -> Arc<LogBuffer> {
    let counters = Arc::new(LogCounters::new());
    Arc::new(LogBuffer::new(Region::heap_zeroed(mib << 20), counters, MAX_PAYLOAD))
}

fn archive_agent(name: &str, b: &Arc<LogBuffer>, dir: &str) -> AgentRunner {
    std::fs::create_dir_all(dir).unwrap();
    let mut archive = Archive::open(ArchiveConfig::new(dir)).unwrap();
    let b = Arc::clone(b);
    AgentRunner::spawn(name, IdleStrategy::BusySpin, move || {
        archive.do_work(&b).expect("archive fail-stop")
    })
    .unwrap()
}

fn follower_node(
    name: &str,
    sock: FaultSocket,
    leader: SocketAddr,
    journal_dir: &str,
    buffer_mib: usize,
) -> (Arc<LogBuffer>, Arc<FollowerStats>, Vec<AgentRunner>) {
    let b = buffer(buffer_mib);
    let cfg = FollowerConfig::new(TERM, leader);
    let mut rx = FollowerReceiver::new(Arc::clone(&b), sock, cfg);
    let stats = rx.stats();
    let rxa =
        AgentRunner::spawn(&format!("{name}-rx"), IdleStrategy::BusySpin, move || rx.do_work())
            .unwrap();
    let ara = archive_agent(&format!("{name}-ar"), &b, journal_dir);
    (b, stats, vec![rxa, ara])
}

fn leader_node(
    raw: UdpSocket,
    followers: Vec<SocketAddr>,
    journal_dir: &str,
    loss_ppm: u32,
    buffer_mib: usize,
) -> (Arc<LogBuffer>, Arc<SenderStats>, Vec<AgentRunner>) {
    let b = buffer(buffer_mib);
    let recv = raw.try_clone().unwrap();
    let mut send = FaultSocket::from_socket(raw).unwrap();
    send.set_faults(FaultConfig {
        seed: 20_260_710,
        drop_per_million: loss_ppm,
        ..Default::default()
    });
    let (tx, rx) = mpsc::sync_channel(4096);
    let mut sender = Sender::new(Arc::clone(&b), send, followers, 3, rx, SenderConfig::new(TERM));
    let stats = sender.stats();
    let txa = AgentRunner::spawn("leader-tx", IdleStrategy::BusySpin, move || sender.do_work())
        .unwrap();
    let mut lr = LeaderReceiver::new(recv, tx).unwrap();
    let lra =
        AgentRunner::spawn("leader-ctrl", IdleStrategy::BusySpin, move || lr.do_work()).unwrap();
    let ara = archive_agent("leader-ar", &b, journal_dir);
    (b, stats, vec![txa, lra, ara])
}

fn max_bytes_cap() -> u64 {
    std::env::var("UC2_M2_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&v| v > 0)
        .unwrap_or(u64::MAX)
}

/// Append until `secs` elapse (measured on the shared clock) or the byte cap
/// is hit, pacing against the sender (never build more than half a buffer of
/// backlog — the M2 stand-in for admission control). Returns (end, msgs).
fn drive_load(lb: &Arc<LogBuffer>, secs: u64, payload: usize, clock: Instant) -> (u64, u64) {
    let cap = lb.capacity();
    let max_bytes = max_bytes_cap();
    let body = vec![0u8; payload];
    let mut a = Appender::new(Arc::clone(lb), TERM);
    let mut msgs = 0u64;
    while clock.elapsed().as_secs() < secs && a.position() < max_bytes {
        match a.append(1, msgs, &body) {
            Ok(_) => msgs += 1,
            Err(AppendError::WouldOverrun) => std::hint::spin_loop(),
            Err(e) => panic!("{e}"),
        }
        while a.position() > lb.counters().sent.load_acquire() + cap / 2 {
            std::hint::spin_loop();
        }
    }
    (a.position(), msgs)
}

fn await_durable(b: &Arc<LogBuffer>, end: u64, what: &str) {
    let t = Instant::now();
    while b.counters().durable.load_acquire() < end {
        assert!(t.elapsed() < Duration::from_secs(300), "{what} drain stuck");
        std::hint::spin_loop();
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("local") => local(&args[1..]),
        Some("leader") => leader_role(&args[1..]),
        Some("follower") => follower_role(&args[1..]),
        _ => {
            eprintln!("usage: m2_gate local|leader|follower ... (see file header)");
            std::process::exit(2);
        }
    }
}

fn local(args: &[String]) {
    let root = args.first().expect("usage: m2_gate local <journal_root> ...").clone();
    let secs: u64 = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(10);
    let payload: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(64);
    let loss_ppm: u32 = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(0);
    let buffer_mib: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(256);

    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1s = FaultSocket::bind("127.0.0.1:0").unwrap();
    let f2s = FaultSocket::bind("127.0.0.1:0").unwrap();
    let (a1, a2) = (f1s.local_addr().unwrap(), f2s.local_addr().unwrap());
    let (f1b, f1st, f1a) = follower_node("f1", f1s, leader_addr, &format!("{root}/f1"), buffer_mib);
    let (f2b, f2st, f2a) = follower_node("f2", f2s, leader_addr, &format!("{root}/f2"), buffer_mib);
    let (lb, lst, la) =
        leader_node(raw, vec![a1, a2], &format!("{root}/leader"), loss_ppm, buffer_mib);

    println!("== uc2 M2 gate (local loopback) ==");
    println!("payload {payload} B, loss {loss_ppm} ppm, buffers {buffer_mib} MiB x3, {secs} s");

    // per-second progress: instantaneous rebuilt rate per follower
    let (p1, p2) = (Arc::clone(&f1b), Arc::clone(&f2b));
    let progress_start = Instant::now();
    let printer = AgentRunner::spawn("printer", IdleStrategy::Sleep(Duration::from_secs(1)), {
        let mut last = (0u64, 0u64);
        move || {
            let now = (p1.counters().append.load_acquire(), p2.counters().append.load_acquire());
            println!(
                "t={:>3}s  f1 +{:>6.1} MB/s  f2 +{:>6.1} MB/s",
                progress_start.elapsed().as_secs(),
                (now.0 - last.0) as f64 / 1e6,
                (now.1 - last.1) as f64 / 1e6,
            );
            last = now;
            false // idle (sleep 1 s) every cycle
        }
    })
    .unwrap();

    // ONE wall clock around load + drain (drain-inclusive headline)
    let clock = Instant::now();
    let (end, msgs) = drive_load(&lb, secs, payload, clock);
    await_durable(&lb, end, "leader");
    await_durable(&f1b, end, "f1");
    await_durable(&f2b, end, "f2");
    let full = clock.elapsed().as_secs_f64();
    printer.stop();

    let rate_mbs = end as f64 / full / 1e6;
    use Ordering::Relaxed as R;
    println!("== uc2 M2 gate ==");
    println!("stream               {end} B ({msgs} msgs) in {full:.2} s (drain-inclusive)");
    println!("per-follower durable {rate_mbs:>7.1} MB/s   ({:.0} msgs/s)", msgs as f64 / full);
    println!(
        "sender               dgrams {}  naks_served {}  flow_stalls {}  overruns {}  heartbeats {}",
        lst.datagrams.load(R),
        lst.naks_served.load(R),
        lst.flow_stalls.load(R),
        lst.overruns.load(R),
        lst.heartbeats.load(R),
    );
    for (n, st) in [("f1", &f1st), ("f2", &f2st)] {
        println!(
            "  {n}: naks_sent {}  dropped dup {} overrun {} stale {} malformed {}",
            st.naks_sent.load(R),
            st.dropped_dup.load(R),
            st.dropped_overrun.load(R),
            st.dropped_stale_term.load(R),
            st.dropped_malformed.load(R),
        );
    }
    let naks = f1st.naks_sent.load(R) + f2st.naks_sent.load(R);
    let pass = rate_mbs >= 100.0
        && lst.overruns.load(R) == 0
        && (loss_ppm == 0 || (naks > 0 && lst.naks_served.load(R) > 0));
    let loss_note = if loss_ppm > 0 { ", loss recovered via NAK" } else { "" };
    println!("GATE (>=100 MB/s per follower{loss_note}): {}", if pass { "PASS" } else { "FAIL" });

    for a in f1a.into_iter().chain(f2a).chain(la) {
        a.stop();
    }
    if !pass {
        std::process::exit(1);
    }
}

/// Fleet follower: runs until killed, printing rebuilt/durable progress.
/// (Follower counters aren't visible cross-host until cnc lands in M5, so
/// the fleet verdict is read off these consoles.)
fn follower_role(args: &[String]) {
    let bind = args.first().expect("bind addr");
    let journal = args.get(1).expect("journal dir");
    let leader: SocketAddr = args.get(2).expect("leader addr").parse().unwrap();
    let buffer_mib: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(256);
    let sock = FaultSocket::bind(bind.as_str()).unwrap();
    let (b, st, _agents) = follower_node("follower", sock, leader, journal, buffer_mib);
    let mut last = 0u64;
    loop {
        std::thread::sleep(Duration::from_secs(1));
        let c = b.counters();
        let now = c.append.load_acquire();
        println!(
            "rebuilt {:>7.1} MB/s  contiguous {now}  durable_lag {}  naks_sent {}",
            (now - last) as f64 / 1e6,
            now - c.durable.load_acquire(),
            st.naks_sent.load(Ordering::Relaxed),
        );
        last = now;
    }
}

/// Fleet leader: drives the load, drains its OWN durable, prints sender
/// stats, then lingers briefly so followers can NAK the tail before exit.
fn leader_role(args: &[String]) {
    let bind = args.first().expect("bind addr");
    let journal = args.get(1).expect("journal dir");
    let f1: SocketAddr = args.get(2).expect("f1 addr").parse().unwrap();
    let f2: SocketAddr = args.get(3).expect("f2 addr").parse().unwrap();
    let secs: u64 = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(10);
    let payload: usize = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(64);
    let loss_ppm: u32 = args.get(6).map(|s| s.parse().unwrap()).unwrap_or(0);
    let buffer_mib: usize = args.get(7).map(|s| s.parse().unwrap()).unwrap_or(256);
    let raw = UdpSocket::bind(bind.as_str()).unwrap();
    let (lb, lst, agents) = leader_node(raw, vec![f1, f2], journal, loss_ppm, buffer_mib);
    let clock = Instant::now();
    let (end, msgs) = drive_load(&lb, secs, payload, clock);
    await_durable(&lb, end, "leader");
    let full = clock.elapsed().as_secs_f64();
    use Ordering::Relaxed as R;
    println!("leader: {end} B ({msgs} msgs) appended+durable in {full:.2} s");
    println!(
        "sender: dgrams {}  naks_served {}  flow_stalls {}  overruns {}",
        lst.datagrams.load(R),
        lst.naks_served.load(R),
        lst.flow_stalls.load(R),
        lst.overruns.load(R),
    );
    std::thread::sleep(Duration::from_secs(5)); // tail-NAK settle window
    for a in agents {
        a.stop();
    }
}
```

- [ ] **Step 2: Build + clean run**

Run:
```bash
cargo build -p uc_net --release --example m2_gate
df -h /home/claude   # before
UC2_M2_MAX_BYTES=2000000000 cargo run -p uc_net --release --example m2_gate -- \
    local /home/claude/uc2-m2-gate 10 64 0 256
```
Expected: per-second progress lines, final report, `GATE ... PASS` (loopback should be far above 100 MB/s per follower). Watch `df` — 3 journals × up to ~2 GB; abort and shrink `UC2_M2_MAX_BYTES` if free space drops below ~10 GB.

- [ ] **Step 3: Loss run**

Run:
```bash
UC2_M2_MAX_BYTES=500000000 cargo run -p uc_net --release --example m2_gate -- \
    local /home/claude/uc2-m2-gate-loss 10 64 5000 256
```
Expected: converges; report shows `naks_sent`/`naks_served` > 0, overruns 0, verdict PASS. (5 000 ppm = 0.5 %, mid-band of the gate's 0.1–1 %.)

- [ ] **Step 4: Clean up run artifacts**

```bash
rm -rf /home/claude/uc2-m2-gate /home/claude/uc2-m2-gate-loss
```
Verify `df -h /home/claude` is back to baseline. NEVER leave gate journals behind.

- [ ] **Step 5: Write the benchmark doc**

`docs/benchmarks/uc2-m2-gate-2026-07-10.md`, mirroring the M1 doc's structure exactly (`docs/benchmarks/uc2-m1-gate-2026-07-09.md`):
- Date 2026-07-10.
- **Prominent banner:** this is a SINGLE-HOST LOOPBACK run on the 4-vCPU sandbox — no NIC, no wire, kernel-internal UDP; the number is an upper bound on the transport path, NOT the official gate. The official M2 gate (spec §9: ≥ 100 MB/s per follower over a real LAN, 3 × c6id hosts) is a fleet follow-up, appended here when run.
- What the gate measures (spec §5/§9), host table (4 vCPU sandbox; journal on ext4 `/dev/sda1`; buffers heap), exact commands (clean + loss runs), verbatim output blocks, interpretation (rebuilt rate per follower, durable keeping pace, NAK counts under 0.5 % loss, flow stalls), and a "Fleet (3×c6id) result — not yet run" placeholder section.
- State the measurement methodology explicitly: drain-inclusive durable rate (one wall clock around load + drain), per-follower rebuilt = contiguous frontier delta. Reference the M1 accounting correction as the reason.

- [ ] **Step 6: Full workspace gates**

Run:
```bash
cargo test -p uc_net && cargo test -p uc_log && cargo test -p uc_protocol
cargo clippy --workspace -- -D warnings
cargo build -p uc_net --release --example m2_gate
```
Expected: all green. (`cargo fmt --check` fails workspace-wide pre-existing — ignore, but keep your files style-consistent with neighbors.)

- [ ] **Step 7: Commit**

```bash
git add uc_net/examples/m2_gate.rs docs/benchmarks/uc2-m2-gate-2026-07-10.md
git commit -m "feat(uc_net): m2_gate replication example + sandbox loopback smoke run"
```

---

## Self-review notes (already applied)

1. **Spec §5 coverage check:** self-locating datagrams (T2), MTU packing + MDC fan-out (T7), position-addressed receive + stale-term drop (T8/T4), contiguous-prefix durability (T8's append-to-contiguous + M1 archive), NAK on randomized delay with buffer-as-retransmit-buffer (T6/T7/T8), quorum-order-statistic flow control with quarter-window statuses (T6/T7/T8), heartbeats (T7), control on the same socket (T2/T8), security posture = reserved header slot (T2). **Replay sessions are the one §5 item deliberately deferred to M4** (stated in Non-goals with the seam: `Overrun` results + `overruns` stats); the M2 gate (0.1–1 % loss, 256 MiB+ buffers) cannot reach below the buffer tail.
2. **Padding-over-the-wire design decision** (spec is silent on the mechanics): padding is transmitted header-only, is always the last frame of its datagram, and receivers advance contiguity by the walked aligned span — so the sender never ships stale ring bytes and a datagram's payload stays one contiguous memcpy at the receiver. Consequence tested in T3 (`run_read_padding_…`), T8 (`walk_advance_…`), and implicitly by every wrap in T9's loads.
3. **Type-consistency pass:** `CtrlMsg` produced in T7, consumed in T8/T9; `SliceRead`/`RunRead` (T3) consumed in T7 and T4's tests; `FollowerConfig::new(term_id, leader)` matches all uses; `read_run_validated(from, max_bytes, out)` signature identical everywhere; counter names `append`/`durable`/`sent` consistent.
4. **Journal-comparison honesty (T9):** block boundaries differ across nodes by timing, so convergence compares REPLAYED FRAME STREAMS, not raw journal bytes. Padding frames are archived with stale span bytes on every node independently — replay skips them, so streams compare clean.
5. **M1 lessons encoded:** drain-inclusive gate accounting (T10 headline), journal dirs on ext4 under `/home/claude` (T9 keeps test data tiny for tmpfs-quota'd tempdirs; T10 mandates `/home/claude` + `UC2_M2_MAX_BYTES` + cleanup), 4 MiB test segments, deadlines on every test wait, `AgentRunner` Drop before any multi-agent consumer exists (T1).
6. **Known simplifications, stated:** intra-process control channel is `std::sync::mpsc` (M5 replaces with the cnc SPSC ring; same try-send/try-recv shape); one `send_to` syscall per follower per datagram (no `sendmmsg` — M3 if the gate demands it); `LeaderReceiver` ignores non-control kinds (M3 adds the consensus route); `dead_follower` recovery beyond the buffer tail is M4 replay.
