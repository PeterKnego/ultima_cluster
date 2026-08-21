//! Shared ring buffer types: header layout, per-record frame layout, and
//! low-level record-write/record-read helpers used by SPSC/MPSC/Broadcast.
//!
//! # Layout
//!
//! Every ring file is laid out as:
//!
//! ```text
//! offset   contents
//! ──────   ─────────────────────────────────────────────────────────────────
//!  0       RingHeader (256 bytes, cache-padded so the three atomics live on
//!          separate cache lines)
//!  256     slot region of `capacity_bytes` bytes (must be a power of two so
//!          that wrap-around indexing reduces to a mask)
//! ```
//!
//! Per-record framing inside the slot region:
//!
//! ```text
//!  length_inclusive_header  u32   total record size (header + payload + crc)
//!  msg_type                 u16
//!  flags                    u16
//!  header_extra             [u8; 8]   per-msg-type metadata
//!  payload                  variable
//!  crc32                    u32   over (msg_type..end-of-payload)
//! ```
//!
//! ## Torn-record protection
//!
//! Producers split a write into two atomic steps:
//!
//!   1. Claim — bump `claim_position` to reserve a slot range. MPSC uses
//!      `compare_exchange_weak` so producers can claim in parallel; SPSC
//!      and Broadcast use a single `store` (single producer per ring).
//!   2. Publish — write the record bytes, then `publish_position.store(…,
//!      Release)` (MPSC spins until `publish_position == my_slot_start` so
//!      the publication order matches the claim order).
//!
//! Consumers load `publish_position` with Acquire and read only records
//! whose `[slot, slot+size)` is fully below `publish_position`. This
//! eliminates the post-wrap torn-record race documented as an M3
//! limitation: a consumer can never see a slot offset whose bytes are
//! still being written.
//!
//! The length-last-Release commit inside `write_record_at` remains as
//! defense-in-depth; the primary torn-record guard is now the
//! `publish_position` Release → consumer Acquire edge.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use thiserror::Error;

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

/// Busy-spin chunk: in busy mode (`spin_budget == u32::MAX`) the consumer polls
/// this many times per `read_or_park` call before returning `Ok(None)`, bounding
/// stop-flag/shutdown latency without ever parking.
pub const BUSY_SPIN_CHUNK: u32 = 256;

/// Padding-marker `msg_type` — consumer skips to the start of the slot
/// region when it encounters this in a record header.
pub const PADDING_MSG_TYPE: u16 = 0xffff;

/// Fixed-size header at the start of every ring file. 256 bytes,
/// cache-padded so claim/publish/consumer atomics live on separate cache
/// lines.
///
/// * `claim_position` — producers atomically claim slot ranges here
///   (CAS for MPSC; single producer for SPSC/Broadcast).
/// * `publish_position` — producer advances this only after the record's
///   bytes are visible. Consumers read records up to this position.
///   Eliminates the post-wrap torn-record race that plagued M3.
/// * `consumer_position` — single reader's progress marker (unused on
///   Broadcast; each consumer keeps its own in-memory `head`).
#[repr(C, align(64))]
pub struct RingHeader {
    pub magic: [u8; 8],
    pub capacity_bytes: u64,
    pub max_msg_size: u32,
    pub msg_kind_filter: u32,
    pub _pad_1: [u8; 40],
    pub claim_position: AtomicU64,
    pub _pad_2: [u8; 56],
    pub publish_position: AtomicU64,
    pub _pad_3: [u8; 56],
    pub consumer_position: AtomicU64,
    /// Count of consumers currently parked on this ring's wakeup word. Written
    /// by the consumer side (same cache line as `consumer_position`), read by
    /// the producer to skip the `FUTEX_WAKE` syscall when nobody is parked.
    /// Reclaimed from `_pad_4` so `RING_HEADER_LEN` is unchanged (no protocol bump).
    pub waiters: AtomicU32,
    pub _pad_4: [u8; 52],
}

const _: () = {
    assert!(std::mem::size_of::<RingHeader>() == 256);
    assert!(std::mem::align_of::<RingHeader>() == 64);
};

pub const RING_HEADER_LEN: usize = std::mem::size_of::<RingHeader>();

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
        if cfg!(target_os = "linux") {
            ParkMode::Futex
        } else {
            ParkMode::Poll
        }
    }
}

impl RingHeader {
    /// The 32-bit wakeup word: the low half of `publish_position`. Changes on
    /// every publish (modulo 2^32 wrap, which is benign for wait-and-recheck).
    #[inline]
    pub fn wake_word(&self) -> &std::sync::atomic::AtomicU32 {
        // SAFETY: little-endian (asserted above): the low 32 bits occupy the
        // first 4 bytes of the 8-byte, 8-aligned `publish_position`. The returned
        // `&AtomicU32` is only used to take its address for the `SYS_futex`
        // syscall (see `futex.rs`); no Rust-level atomic load/store is ever
        // performed on it, so the mixed-size overlapping-atomic UB concern does
        // not apply.
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
            // Poll consumers have no parked syscall to wake; the `PARK_CEIL`
            // timeout backstop in `park` is their sole wakeup mechanism.
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

/// A cloneable handle that lets a parker thread block on a ring's wakeup word
/// while the owning (async) consumer reads. Holds an `Arc` keepalive so the
/// ring mmap outlives the handle, plus a raw `RingHeader` pointer into it.
/// Constructed by each consumer's `wait_handle()`.
pub struct RingWaitHandle {
    _keepalive: Arc<dyn std::any::Any + Send + Sync>,
    header: *const RingHeader,
    mode: ParkMode,
}

impl Clone for RingWaitHandle {
    fn clone(&self) -> Self {
        Self {
            _keepalive: self._keepalive.clone(),
            header: self.header,
            mode: self.mode,
        }
    }
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
        Self {
            _keepalive: keepalive,
            header,
            mode,
        }
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
    /// Force-wake anything parked on this ring's wakeup word, regardless of the
    /// `waiters` count or whether the word changed. Used to interrupt a parker
    /// thread's `park()` promptly at shutdown so its join doesn't block for the
    /// full `PARK_CEIL`. No-op in `Poll` mode (a `Poll` parker is in `sleep` and
    /// resolves within `PARK_CEIL` on its own).
    #[inline]
    pub fn wake(&self) {
        if self.mode == ParkMode::Futex {
            super::futex::futex_wake(self.header().wake_word(), i32::MAX);
        }
    }
}

/// Per-record frame header. Layout matches the wire format exactly
/// (`#[repr(C)]` keeps field order).
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct FrameHeader {
    pub length_inclusive_header: u32,
    pub msg_type: u16,
    pub flags: u16,
    pub header_extra: [u8; 8],
}

pub const FRAME_HEADER_LEN: usize = std::mem::size_of::<FrameHeader>();
pub const FRAME_TRAILER_LEN: usize = 4; // crc32

/// All record advancements (claim_position / publish_position /
/// consumer_position increments) are rounded up to this many bytes. Two
/// properties depend on it:
///
///   * `claim_position & (capacity - 1)` is always a multiple of
///     `RECORD_ALIGN`, so `bytes_to_tail = capacity - slot_offset` is also a
///     multiple of `RECORD_ALIGN` and is therefore ≥ `RECORD_ALIGN` whenever a
///     tail-wrap padding marker is needed.
///   * The padding marker writes 6 bytes (4-byte length + 2-byte msg_type),
///     which fits in the guaranteed `RECORD_ALIGN ≥ 8` tail window.
///
/// The length field in the record header stores the *unaligned* record size
/// (so the consumer can decode `payload_len` directly); position advances use
/// [`align_record_size`].
pub const RECORD_ALIGN: usize = 8;

/// Round a record size up to [`RECORD_ALIGN`].
#[inline]
pub const fn align_record_size(total: usize) -> usize {
    (total + RECORD_ALIGN - 1) & !(RECORD_ALIGN - 1)
}

/// Returned by consumer `try_read` on success. Mirrors the per-record header
/// fields that the caller cares about. The `length` is not exposed because
/// it's redundant with the payload length the caller already has.
#[derive(Debug, Clone, Copy)]
pub struct RecordHeader {
    pub msg_type: u16,
    pub flags: u16,
    pub header_extra: [u8; 8],
}

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
    #[error("consumer fell behind; producer overwrote unread records")]
    Overwritten,
}

/// Create — or RECREATE IN PLACE — the backing file for an mmap-shared IPC
/// structure, without ever shrinking it.
///
/// Never `.truncate(true)` a file that other processes may still have
/// mapped: between truncate-to-0 and the subsequent `set_len`, every page
/// is beyond EOF and any mapped read/write in an attached process is a
/// **SIGBUS** — a hard crash in THAT process. A node restart recreates the
/// cnc page and all five ring files while the previous incarnation's
/// service/clients still hold mappings, and attachers poll these pages
/// continuously (found 2026-08-16: 2-of-3 crash rate in the restart-heavy
/// elle harness once the pipelined client raised the read frequency).
///
/// Instead: open without truncate, grow to `len` if needed (never shrink —
/// a pre-existing longer file keeps its tail), and ZERO the content with
/// `fallocate(ZERO_RANGE)`. Attachers observe zeroed/torn content during the
/// window, which every reader of these structures already tolerates
/// (torn-header contracts); they never fault. Falls back to writing explicit
/// zeros if the filesystem lacks `ZERO_RANGE`.
///
/// # Why ZERO_RANGE and not PUNCH_HOLE — the SECOND SIGBUS
///
/// This used to punch holes, which zeroes just as well and keeps the file
/// SPARSE ("a 256 MiB buffer is not materialized"). That traded one SIGBUS
/// for another. A sparse mapping has pages with no block behind them, so the
/// first write to such a page must allocate a block AT PAGE-FAULT TIME — and
/// on a full filesystem that allocation fails, which the kernel reports as
/// `SIGBUS`. Not an `io::Error`: it cannot be returned, matched, or handled,
/// and it kills whichever process — node, service, or client — touched the
/// page. The daemon's whole fail-stop chain (journal halt → `ArchiveError` →
/// `agent_failstopped` → exit 1) is bypassed, because nothing in it ever
/// runs.
///
/// Measured directly (M11 gate row 3b, 2026-08-21, on a deliberately-filled
/// 56 MiB loopback fixture): `log.buf` 1,048,576 bytes apparent / 81,920
/// allocated, and the daemon exiting `code=None signal=Some(7) core=true`
/// with no `agent_failstopped` in its stderr — plus runs where the test's own
/// client process took the SIGBUS instead.
///
/// `ZERO_RANGE` reserves the blocks up front (as unwritten extents — no
/// zeroes are actually written, so this stays fast), which moves the failure
/// to where it can be handled: this function returns `ENOSPC` from `fallocate`
/// and the caller refuses to start, instead of a page fault killing a running
/// process later. The cost is real disk usage — these files are no longer
/// sparse, so a 256 MiB log buffer occupies 256 MiB — which is the honest
/// price of a mapping that cannot fault.
pub fn create_shared_backing_file(
    path: &std::path::Path,
    len: u64,
) -> std::io::Result<std::fs::File> {
    use std::os::unix::io::AsRawFd;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let cur = file.metadata()?.len();
    if cur < len {
        file.set_len(len)?;
    }
    let zero_len = cur.max(len);
    // SAFETY: plain fallocate syscall on our own fd; no memory contract.
    let rc = unsafe {
        libc::fallocate(file.as_raw_fd(), libc::FALLOC_FL_ZERO_RANGE, 0, zero_len as libc::off_t)
    };
    if rc != 0 {
        // Two different reasons to land here, and both want the same answer.
        // ENOSPC: there is no room to back this mapping, and writing zeros
        // will fail too -- but it fails as an `io::Error` this function can
        // return, which is the entire point (see the SIGBUS note above).
        // EOPNOTSUPP: an exotic filesystem without ZERO_RANGE; writing zeros
        // both clears the content and materializes the blocks.
        use std::io::{Seek, SeekFrom, Write};
        let mut f = &file;
        f.seek(SeekFrom::Start(0))?;
        let zeros = vec![0u8; 64 * 1024];
        let mut left = zero_len;
        while left > 0 {
            let n = left.min(zeros.len() as u64) as usize;
            f.write_all(&zeros[..n])?;
            left -= n as u64;
        }
        f.flush()?;
    }
    Ok(file)
}

/// Initialize the header of a freshly mmap'd ring file in place.
///
/// Safe iff the caller holds exclusive access to `buf` (typical: just-created
/// mmap'd file before any concurrent access).
pub fn init_ring_header(
    buf: &mut [u8],
    capacity_bytes: u64,
    max_msg_size: u32,
    msg_kind_filter: u32,
) -> Result<(), RingError> {
    if buf.len() < RING_HEADER_LEN {
        return Err(RingError::Corrupt(format!(
            "buffer too small for ring header: {} < {RING_HEADER_LEN}",
            buf.len()
        )));
    }
    if !capacity_bytes.is_power_of_two() {
        return Err(RingError::Corrupt(format!(
            "capacity_bytes must be power of two, got {capacity_bytes}"
        )));
    }
    if (capacity_bytes as usize) < RECORD_ALIGN {
        return Err(RingError::Corrupt(format!(
            "capacity_bytes must be >= RECORD_ALIGN ({RECORD_ALIGN}), got {capacity_bytes}"
        )));
    }
    // SAFETY: mmap'd buffers from `MmapMut::map_mut` are page-aligned, which
    // exceeds the 64-byte alignment required by `RingHeader`. The buffer is
    // at least `RING_HEADER_LEN` bytes per the check above. No other thread
    // can observe the bytes until this function returns.
    unsafe {
        let header_ptr = buf.as_mut_ptr().cast::<RingHeader>();
        std::ptr::write(
            header_ptr,
            RingHeader {
                magic: crate::magic::RING_MAGIC,
                capacity_bytes,
                max_msg_size,
                msg_kind_filter,
                _pad_1: [0; 40],
                claim_position: AtomicU64::new(0),
                _pad_2: [0; 56],
                publish_position: AtomicU64::new(0),
                _pad_3: [0; 56],
                consumer_position: AtomicU64::new(0),
                waiters: AtomicU32::new(0),
                _pad_4: [0; 52],
            },
        );
    }
    Ok(())
}

/// Validate an existing ring file's header (e.g., on attach). Returns a
/// shared reference into `buf`.
pub fn validate_ring_header(buf: &[u8]) -> Result<&RingHeader, RingError> {
    if buf.len() < RING_HEADER_LEN {
        return Err(RingError::Corrupt(format!(
            "buffer too small: {} < {RING_HEADER_LEN}",
            buf.len()
        )));
    }
    // SAFETY: buf is at least RING_HEADER_LEN bytes; mmap is properly aligned.
    let header = unsafe { &*buf.as_ptr().cast::<RingHeader>() };
    if header.magic != crate::magic::RING_MAGIC {
        return Err(RingError::BadMagic);
    }
    Ok(header)
}

/// Write a complete record (header + payload + crc32) at `slot_offset` within
/// the slot region. The length field is written **last** with a Release
/// fence/store so that consumers using Acquire on `publish_position` observe
/// the fully-written record.
///
/// # Safety
///
/// The caller must guarantee:
///   * `slot_region` points to the start of a slot region of at least
///     `slot_offset + total_record_size` bytes.
///   * The byte range `[slot_offset, slot_offset + total_record_size)` is not
///     concurrently read or written by any other thread.
///   * `total_record_size == FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN`.
pub unsafe fn write_record_at(
    slot_region: *mut u8,
    slot_offset: usize,
    msg_type: u16,
    flags: u16,
    header_extra: [u8; 8],
    payload: &[u8],
    total_record_size: usize,
) {
    let dst = unsafe { slot_region.add(slot_offset) };
    unsafe {
        // bytes 4..6 — msg_type
        std::ptr::copy_nonoverlapping((&msg_type as *const u16).cast::<u8>(), dst.add(4), 2);
        // bytes 6..8 — flags
        std::ptr::copy_nonoverlapping((&flags as *const u16).cast::<u8>(), dst.add(6), 2);
        // bytes 8..16 — header_extra
        std::ptr::copy_nonoverlapping(header_extra.as_ptr(), dst.add(8), 8);
        // payload
        std::ptr::copy_nonoverlapping(payload.as_ptr(), dst.add(FRAME_HEADER_LEN), payload.len());
        // crc32 over (msg_type..end-of-payload)
        let crc_input =
            std::slice::from_raw_parts(dst.add(4), FRAME_HEADER_LEN - 4 + payload.len());
        let crc = crc32fast::hash(crc_input);
        let crc_bytes = crc.to_le_bytes();
        std::ptr::copy_nonoverlapping(
            crc_bytes.as_ptr(),
            dst.add(FRAME_HEADER_LEN + payload.len()),
            4,
        );
        // Write length last (legacy "length != 0 means committed" guard). No
        // Release fence is needed here: the caller advances `publish_position`
        // with a Release store after this function returns, which orders ALL of
        // these slot writes before any reader can observe the slot (readers
        // only touch slots below `publish_position`, loaded with Acquire). On
        // arm64 the removed fence was a per-write `dmb ish`.
        let len_bytes = (total_record_size as u32).to_le_bytes();
        std::ptr::copy_nonoverlapping(len_bytes.as_ptr(), dst, 4);
    }
}

/// Write a tail-wrap padding marker at `slot_offset`. The padding record's
/// length field is the number of bytes from `slot_offset` to the end of the
/// slot region, so the consumer can skip past it cleanly.
///
/// # Safety
///
/// Same as [`write_record_at`].
pub unsafe fn write_padding_marker_at(
    slot_region: *mut u8,
    slot_offset: usize,
    padding_bytes: usize,
) {
    let dst = unsafe { slot_region.add(slot_offset) };
    unsafe {
        // bytes 4..6 — msg_type = PADDING_MSG_TYPE
        std::ptr::copy_nonoverlapping(
            (&PADDING_MSG_TYPE as *const u16).cast::<u8>(),
            dst.add(4),
            2,
        );
        // No Release fence (see `write_record_at`): the caller's subsequent
        // `publish_position` Release store orders this write before any reader
        // can observe the padding slot.
        let len_bytes = (padding_bytes as u32).to_le_bytes();
        std::ptr::copy_nonoverlapping(len_bytes.as_ptr(), dst, 4);
    }
}

/// Read a single record at `slot_offset`. Returns `Ok(None)` if the record is
/// not yet committed (length field reads as zero), `Ok(Some(...))` on success.
///
/// On success, `payload_buf` is cleared and filled with the record payload.
/// The returned `(RecordHeader, advance)` lets the caller advance its consumer
/// position — `advance` is the [`align_record_size`]-rounded record size, so
/// consumer positions stay aligned to [`RECORD_ALIGN`].
///
/// # Safety
///
/// `slot_region` must point to a valid slot region of at least
/// `slot_offset + max_msg_size` bytes, and the producer must have already
/// committed (i.e., the caller observed `publish_position > consumer_position`
/// with Acquire ordering).
pub unsafe fn try_read_record_at(
    slot_region: *const u8,
    slot_offset: usize,
    payload_buf: &mut Vec<u8>,
) -> Result<Option<(RecordHeader, usize)>, RingError> {
    let dst = unsafe { slot_region.add(slot_offset) };
    // SAFETY: length field is the first 4 bytes; the caller guarantees the
    // slot is in-range.
    let length_bytes: [u8; 4] = unsafe {
        let mut buf = [0u8; 4];
        std::ptr::copy_nonoverlapping(dst, buf.as_mut_ptr(), 4);
        buf
    };
    let length = u32::from_le_bytes(length_bytes);
    if length == 0 {
        return Ok(None); // not yet committed
    }
    // No Acquire fence here: every caller (SPSC/MPSC/Broadcast `try_read`)
    // performs an Acquire load of `publish_position` and only reads slots that
    // lie fully below it. That Acquire load synchronizes-with the producer's
    // `publish_position` Release store (made after the whole frame is written),
    // so all slot bytes are already visible. A separate fence here is redundant
    // — and on arm64 it would emit an extra `dmb ishld` on every read.

    let msg_type_bytes: [u8; 2] = unsafe {
        let mut buf = [0u8; 2];
        std::ptr::copy_nonoverlapping(dst.add(4), buf.as_mut_ptr(), 2);
        buf
    };
    let msg_type = u16::from_le_bytes(msg_type_bytes);

    if msg_type == PADDING_MSG_TYPE {
        // Padding length is always a multiple of RECORD_ALIGN by construction
        // (bytes_to_tail at an aligned producer position is aligned).
        return Ok(Some((
            RecordHeader {
                msg_type,
                flags: 0,
                header_extra: [0; 8],
            },
            length as usize,
        )));
    }

    let flags_bytes: [u8; 2] = unsafe {
        let mut buf = [0u8; 2];
        std::ptr::copy_nonoverlapping(dst.add(6), buf.as_mut_ptr(), 2);
        buf
    };
    let flags = u16::from_le_bytes(flags_bytes);

    let mut header_extra = [0u8; 8];
    unsafe {
        std::ptr::copy_nonoverlapping(dst.add(8), header_extra.as_mut_ptr(), 8);
    }

    let payload_len = (length as usize)
        .checked_sub(FRAME_HEADER_LEN + FRAME_TRAILER_LEN)
        .ok_or_else(|| RingError::Corrupt(format!("record length {length} too small")))?;

    payload_buf.clear();
    payload_buf.reserve(payload_len);
    unsafe {
        let src = dst.add(FRAME_HEADER_LEN);
        std::ptr::copy_nonoverlapping(src, payload_buf.as_mut_ptr(), payload_len);
        payload_buf.set_len(payload_len);
    }

    // Validate CRC over (msg_type..end-of-payload).
    let crc_actual_bytes: [u8; 4] = unsafe {
        let mut buf = [0u8; 4];
        std::ptr::copy_nonoverlapping(dst.add(FRAME_HEADER_LEN + payload_len), buf.as_mut_ptr(), 4);
        buf
    };
    let crc_actual = u32::from_le_bytes(crc_actual_bytes);
    let crc_input =
        unsafe { std::slice::from_raw_parts(dst.add(4), FRAME_HEADER_LEN - 4 + payload_len) };
    let crc_expected = crc32fast::hash(crc_input);
    if crc_actual != crc_expected {
        return Err(RingError::BadCrc);
    }

    Ok(Some((
        RecordHeader {
            msg_type,
            flags,
            header_extra,
        },
        align_record_size(length as usize),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use memmap2::MmapMut;
    use std::sync::atomic::Ordering;
    use tempfile::NamedTempFile;

    /// Allocate an mmap-backed buffer for tests. Real callers always pass
    /// mmap'd memory (page-aligned, far exceeds the 64-byte alignment we
    /// need). A plain `Vec<u8>` only guarantees byte alignment and would
    /// hit UB when reinterpreted as `RingHeader`.
    fn mmap_buf(len: usize) -> (MmapMut, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .unwrap();
        f.set_len(len as u64).unwrap();
        let mmap = unsafe { MmapMut::map_mut(&f).unwrap() };
        (mmap, tmp)
    }

    #[test]
    fn init_then_validate_round_trip() {
        let (mut mmap, _tmp) = mmap_buf(RING_HEADER_LEN * 2);
        init_ring_header(&mut mmap[..], 65536, 4096, 0xff).expect("init");
        let header = validate_ring_header(&mmap[..]).expect("validate");
        assert_eq!(header.magic, crate::magic::RING_MAGIC);
        assert_eq!(header.capacity_bytes, 65536);
        assert_eq!(header.max_msg_size, 4096);
        assert_eq!(header.msg_kind_filter, 0xff);
        assert_eq!(header.claim_position.load(Ordering::Relaxed), 0);
        assert_eq!(header.publish_position.load(Ordering::Relaxed), 0);
        assert_eq!(header.consumer_position.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn validate_rejects_bad_magic() {
        let (mmap, _tmp) = mmap_buf(RING_HEADER_LEN);
        let result = validate_ring_header(&mmap[..]);
        assert!(matches!(result, Err(RingError::BadMagic)));
    }

    #[test]
    fn init_rejects_non_power_of_two_capacity() {
        let (mut mmap, _tmp) = mmap_buf(RING_HEADER_LEN * 2);
        let r = init_ring_header(&mut mmap[..], 1000, 4096, 0);
        assert!(matches!(r, Err(RingError::Corrupt(_))));
    }

    #[test]
    fn init_rejects_undersized_buffer() {
        let (mut mmap, _tmp) = mmap_buf(4096);
        let r = init_ring_header(&mut mmap[..10], 65536, 4096, 0);
        assert!(matches!(r, Err(RingError::Corrupt(_))));
    }
}
