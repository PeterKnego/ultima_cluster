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
//!   2. Publish — write the record bytes, then make the write visible.
//!      **SPSC and Broadcast** do this with `publish_position.store(…,
//!      Release)`: consumers load `publish_position` with Acquire and read
//!      only records whose `[slot, slot+size)` is fully below it, which
//!      eliminates the post-wrap torn-record race documented as an M3
//!      limitation — a consumer can never see a slot offset whose bytes are
//!      still being written. **MPSC** (M13a, `ring::mpsc` module doc) does
//!      not use `publish_position` as a position at all: each record
//!      commits independently via a per-slot commit word
//!      (`encode_commit_word`/`classify_commit_word`), Release-stored after
//!      the body is written, and `publish_position` is reused as a
//!      **commit count** — bumped once per commit purely so the futex wake
//!      word changes. The torn-record guard on MPSC is therefore the commit
//!      word's Release → the consumer's Acquire load of that same word, not
//!      `publish_position`.
//!
//! The length-last-Release commit inside `write_record_at` remains as
//! defense-in-depth on SPSC/Broadcast; the primary torn-record guard there
//! is the `publish_position` Release → consumer Acquire edge. MPSC does not
//! call `write_record_at` (it uses the split `write_record_body_at` +
//! commit-word helpers instead — see `ring::mpsc`).

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
/// * `publish_position` — meaning is ring-kind-dependent (see the
///   "Torn-record protection" section of the module doc). On
///   **SPSC/Broadcast** it is a byte position: the producer advances it
///   only after the record's bytes are visible, and consumers read
///   records up to it — eliminates the post-wrap torn-record race that
///   plagued M3. On **MPSC** (M13a) it is not a position at all —
///   per-record commit uses a per-slot commit word instead, and
///   `publish_position` is reused as a commit count, bumped once per
///   commit to change the futex wake word.
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
    MagicMismatch,
    #[error("crc mismatch")]
    BadCrc,
    #[error("consumer fell behind; producer overwrote unread records")]
    Overwritten,
    /// The consumer is head-of-line behind a claim whose claim word never
    /// appeared (spec §4.2: a producer killed between its CAS on
    /// `claim_position` and its claim-word store — a window of nanoseconds).
    /// The hole's length is unknowable, so the ring refuses to guess: the
    /// caller fail-stops. Strictly better than the pre-M13a behaviour, where
    /// the same death wedged every producer and the consumer silently.
    #[error(
        "ingress ring wedged at position {position}: an unsized claim hole outlived the hole timeout"
    )]
    Wedged { position: u64 },
    /// Task 4's controller ruling on spec §4.2's residual, tightened by fix
    /// round 1's finding 2: a producer that merely STALLED past
    /// `hole_timeout` (rather than dying) can resume after the consumer has
    /// already skipped its hole. The consumer marks a skipped claim with a
    /// `compare_exchange` from the exact claim word it observed to a skip
    /// marker (`CLAIMED | LAP | 0`, spec §4.1 amendment) rather than trusting
    /// its own bookkeeping, and the producer's commit is itself a
    /// `compare_exchange` on the claim word (not an unconditional store), so
    /// a resurrection is refused the moment it tries to commit: `position`
    /// is the claim's start position, and the caller has nothing durable to
    /// retry — the record is simply lost. Loss is now ALWAYS signalled this
    /// way, in both directions the resurrection can be caught: immediately
    /// via the marker (no lap needed) if the CAS-skip landed before the
    /// resumed commit, or via a later claimant's re-stamped word if the
    /// resurrection raced the marker CAS itself and the ring has since
    /// lapped back to this exact slot.
    ///
    /// # What this does NOT catch, stated precisely
    ///
    /// A resurrection that resumes mid-BODY-write, after the slot has moved
    /// on to a yet-later claimant without its commit word changing again in
    /// the meantime. The commit CAS guards the slot's first word only; the
    /// body bytes are unguarded, and there are three shapes, only one of
    /// which the crc32 catches:
    ///
    /// 1. **Partial stomp (crc-caught).** The resurrected producer writes
    ///    some of its body over a later claimant's differently-sized record.
    ///    The bytes no longer agree with the later claimant's trailer, so
    ///    `decode_record_slice` returns [`Self::BadCrc`] (or
    ///    [`Self::Corrupt`]). Surfaced as a bad read — never a silent
    ///    misread — but not prevented.
    /// 2. **Complete same-length stomp (NOT crc-caught).** If the later
    ///    claimant's record is the same length, the resurrected producer
    ///    writes a fully self-consistent record — its own header_extra, its
    ///    own payload, its own crc — over it. The crc MATCHES, and the
    ///    consumer delivers the RESURRECTED producer's record at the later
    ///    claimant's position. The later claimant's submit is silently lost:
    ///    its client sees no response and retries on timeout, and the
    ///    resurrected record may be delivered twice (once at its original
    ///    position if it was ever read there, once here). Exactly-once
    ///    therefore holds only for callers that ride
    ///    `uc_service::session::Sessioned` — the `(client_id, seq)` envelope
    ///    is what turns the duplicate into a REPLAYED tag and the loss into
    ///    a client-side retry.
    /// 3. **Padding stomp (NOT crc-covered at all).** A producer preempted
    ///    between its tail-padding claim-word store and its padding BODY
    ///    write writes `PADDING_MSG_TYPE` into bytes 4..6 of a later
    ///    claimant's slot. `decode_record_slice` short-circuits on
    ///    `msg_type == PADDING_MSG_TYPE` BEFORE computing any crc, so
    ///    nothing about that record is checked. The MPSC consumer therefore
    ///    does not use that short-circuit blindly: padding is by
    ///    construction exactly the tail remnant, so
    ///    [`mpsc::MpscConsumer::try_read`] accepts `PADDING_MSG_TYPE` only
    ///    when the committed length is exactly `bytes_to_tail`, and
    ///    otherwise decodes through the normal record path (see
    ///    [`decode_record_slice_no_padding`]) so the stomp surfaces as
    ///    `BadCrc`/`Corrupt`. Residual: a real record that ends EXACTLY at
    ///    the tail is indistinguishable from padding by that test, so a
    ///    padding stomp on such a record is still swallowed silently.
    ///
    /// [`mpsc::MpscConsumer::try_read`]: crate::ring::mpsc::MpscConsumer::try_read
    #[error("producer at position {position} was skipped by the consumer before it could commit")]
    Skipped { position: u64 },
    /// A caller asked to write a record whose `msg_type` is
    /// [`PADDING_MSG_TYPE`], the reserved tail-remnant marker. Accepting it
    /// would let a caller manufacture something the consumer must decide
    /// between "record" and "ring padding" on length alone — see
    /// [`Self::Skipped`]'s padding-stomp case. A caller error, refused at
    /// the door.
    #[error("msg_type {msg_type:#06x} is reserved for ring padding")]
    ReservedMsgType { msg_type: u16 },
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
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_ZERO_RANGE,
            0,
            zero_len as libc::off_t,
        )
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
    init_ring_header_with_magic(
        buf,
        capacity_bytes,
        max_msg_size,
        msg_kind_filter,
        crate::magic::RING_MAGIC,
    )
}

/// As [`init_ring_header`], with the file magic chosen by the caller.
/// SPSC/Broadcast pass `RING_MAGIC`; MPSC passes `RING_MPSC_MAGIC` (M13a).
///
/// Safe iff the caller holds exclusive access to `buf` (typical: just-created
/// mmap'd file before any concurrent access).
pub fn init_ring_header_with_magic(
    buf: &mut [u8],
    capacity_bytes: u64,
    max_msg_size: u32,
    msg_kind_filter: u32,
    magic: [u8; 8],
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
                magic,
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
    validate_ring_header_with_magic(buf, crate::magic::RING_MAGIC)
}

/// As [`validate_ring_header`], with the expected magic chosen by the caller.
pub fn validate_ring_header_with_magic(
    buf: &[u8],
    magic: [u8; 8],
) -> Result<&RingHeader, RingError> {
    if buf.len() < RING_HEADER_LEN {
        return Err(RingError::Corrupt(format!(
            "buffer too small: {} < {RING_HEADER_LEN}",
            buf.len()
        )));
    }
    // SAFETY: buf is at least RING_HEADER_LEN bytes; mmap is properly aligned.
    let header = unsafe { &*buf.as_ptr().cast::<RingHeader>() };
    if header.magic != magic {
        return Err(RingError::MagicMismatch);
    }
    Ok(header)
}

/// Write everything in a record EXCEPT its first 4-byte word: `msg_type`,
/// `flags`, `header_extra`, the payload and the crc32 trailer. The caller
/// publishes the record by writing that word (a plain length for
/// SPSC/Broadcast, a commit word for MPSC).
///
/// # Safety
///
/// Same contract as [`write_record_at`].
pub unsafe fn write_record_body_at(
    slot_region: *mut u8,
    slot_offset: usize,
    msg_type: u16,
    flags: u16,
    header_extra: [u8; 8],
    payload: &[u8],
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
    }
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
    unsafe {
        write_record_body_at(
            slot_region,
            slot_offset,
            msg_type,
            flags,
            header_extra,
            payload,
        );
        // Write length last (legacy "length != 0 means committed" guard). No
        // Release fence is needed here: the caller advances `publish_position`
        // with a Release store after this function returns, which orders ALL of
        // these slot writes before any reader can observe the slot (readers
        // only touch slots below `publish_position`, loaded with Acquire). On
        // arm64 the removed fence was a per-write `dmb ish`.
        // (SPSC/Broadcast only — MPSC publishes with a commit word instead.)
        let len_bytes = (total_record_size as u32).to_le_bytes();
        std::ptr::copy_nonoverlapping(len_bytes.as_ptr(), slot_region.add(slot_offset), 4);
    }
}

/// Write a tail-wrap padding marker's `msg_type` only (bytes 4..6). The
/// caller writes the first word.
///
/// # Safety
///
/// Same as [`write_record_at`]; the slot must have at least 6 bytes.
pub unsafe fn write_padding_body_at(slot_region: *mut u8, slot_offset: usize) {
    let dst = unsafe { slot_region.add(slot_offset) };
    unsafe {
        std::ptr::copy_nonoverlapping(
            (&PADDING_MSG_TYPE as *const u16).cast::<u8>(),
            dst.add(4),
            2,
        );
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
    unsafe {
        write_padding_body_at(slot_region, slot_offset);
        // No Release fence (see `write_record_at`): the caller's subsequent
        // `publish_position` Release store orders this write before any reader
        // can observe the padding slot.
        let len_bytes = (padding_bytes as u32).to_le_bytes();
        std::ptr::copy_nonoverlapping(len_bytes.as_ptr(), slot_region.add(slot_offset), 4);
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

// ---- MPSC per-record commit word (M13a; spec §4.1) ------------------------
//
// The first word of every MPSC slot. It replaces the pre-M13a "length, 0 =
// uncommitted" convention and carries three fields:
//
//   bit 31      CLAIMED   set between claim and commit
//   bits 18-30  LAP       (record_start_pos / capacity) & 0x1FFF
//   bits 0-17   LENGTH    total record bytes (claim word: the claimed advance)
//
// The lap is what makes the consumer's read of a stale slot unambiguous
// WITHOUT the consumer ever writing into the ring: the bounded claim means a
// producer only overwrites a slot the consumer has already consumed, so the
// only stale value the consumer can meet is an OLDER lap's committed word,
// which fails lap equality. 13 bits is unambiguous because the consumer can
// never be 8192 laps behind a claim — the bound is one lap.

/// Bit 31: the slot is claimed by a producer that has not committed yet.
pub const COMMIT_CLAIMED: u32 = 1 << 31;
/// Bits 18-30 hold the lap.
pub const COMMIT_LAP_SHIFT: u32 = 18;
/// 13-bit lap field.
pub const COMMIT_LAP_MASK: u32 = 0x1FFF;
/// 18-bit length field.
pub const COMMIT_LEN_MASK: u32 = 0x3_FFFF;
/// Largest record an MPSC ring can carry: the length field's ceiling.
/// `MpscRing::create` refuses a `max_msg_size` whose aligned size exceeds it.
pub const MPSC_MAX_RECORD_BYTES: usize = COMMIT_LEN_MASK as usize;

/// What the consumer found in a slot's commit word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// Nothing of ours here: an untouched slot, an older lap's leftovers, or
    /// a claim whose word has not landed yet.
    Empty,
    /// A producer claimed `advance` bytes here and has not committed.
    Claimed { advance: u32 },
    /// A committed record of `length` bytes (unaligned; advance by
    /// [`align_record_size`]).
    Committed { length: u32 },
}

/// The lap a byte position belongs to.
#[inline]
pub const fn lap_of(pos: u64, capacity: usize) -> u32 {
    ((pos / capacity as u64) as u32) & COMMIT_LAP_MASK
}

/// Pack a commit word.
#[inline]
pub const fn encode_commit_word(lap: u32, len: u32, claimed: bool) -> u32 {
    let base = ((lap & COMMIT_LAP_MASK) << COMMIT_LAP_SHIFT) | (len & COMMIT_LEN_MASK);
    if claimed { base | COMMIT_CLAIMED } else { base }
}

/// Decide what a slot holds. Total on every `u32`.
#[inline]
pub const fn classify_commit_word(word: u32, expected_lap: u32) -> SlotState {
    if word == 0 {
        return SlotState::Empty;
    }
    if (word >> COMMIT_LAP_SHIFT) & COMMIT_LAP_MASK != expected_lap & COMMIT_LAP_MASK {
        return SlotState::Empty;
    }
    let len = word & COMMIT_LEN_MASK;
    if word & COMMIT_CLAIMED != 0 {
        SlotState::Claimed { advance: len }
    } else if len == 0 {
        // No producer can write this (a commit always carries a length).
        SlotState::Empty
    } else {
        SlotState::Committed { length: len }
    }
}

/// Acquire-load a slot's commit word.
///
/// # Safety
///
/// `slot_region + slot_offset` must be a mapped, 4-byte-aligned address
/// inside the slot region. Both hold by construction: the region is
/// page-aligned and every position advances in [`RECORD_ALIGN`] steps.
#[inline]
pub unsafe fn load_commit_word(slot_region: *const u8, slot_offset: usize) -> u32 {
    let p = unsafe { slot_region.add(slot_offset) }.cast::<AtomicU32>();
    unsafe { (*p).load(Ordering::Acquire) }
}

/// Store a slot's commit word. `Release` publishes the record; `Relaxed` is
/// for the claim stamp (nothing depends on it being ordered).
///
/// # Safety
///
/// Same as [`load_commit_word`], plus: the caller owns the claimed range.
#[inline]
pub unsafe fn store_commit_word(
    slot_region: *mut u8,
    slot_offset: usize,
    word: u32,
    ord: Ordering,
) {
    let p = unsafe { slot_region.add(slot_offset) }.cast::<AtomicU32>();
    unsafe { (*p).store(word, ord) }
}

/// Compare-and-swap a slot's commit word: publish `new` only if the word is
/// still `current`.
///
/// The producer's commit uses this — instead of an unconditional
/// [`store_commit_word`] — to close the residual described on
/// [`RingError::Skipped`]: if the consumer has timed out this claim's hole
/// (spec §4.2) and a later producer has already re-stamped the slot with its
/// own claim, `current` (the resurrected producer's OWN claim word) no
/// longer matches what's there, the CAS fails, and the caller learns
/// `Skipped` instead of overwriting the later claimant's in-flight record.
///
/// # Safety
///
/// Same as [`load_commit_word`]/[`store_commit_word`]: `slot_region +
/// slot_offset` must be a mapped, 4-byte-aligned address inside the slot
/// region, and the caller must (still, or formerly) own the claimed range.
#[inline]
pub unsafe fn cas_commit_word(
    slot_region: *mut u8,
    slot_offset: usize,
    current: u32,
    new: u32,
    success: Ordering,
    failure: Ordering,
) -> Result<u32, u32> {
    let p = unsafe { slot_region.add(slot_offset) }.cast::<AtomicU32>();
    unsafe { (*p).compare_exchange(current, new, success, failure) }
}

/// Decode one record from `slot` — exactly the record's own bytes, commit
/// word included at `slot[0..4]`. Total on any input: every access is
/// bounds-checked and the crc32 is verified. Returns the header and the
/// number of bytes to advance the consumer position by.
///
/// Safe by construction (a slice, not a pointer), which is what makes the
/// `ring_mpsc_record` fuzz target possible.
pub fn decode_record_slice(
    slot: &[u8],
    payload_buf: &mut Vec<u8>,
) -> Result<(RecordHeader, usize), RingError> {
    decode_record_slice_inner(slot, payload_buf, true)
}

/// [`decode_record_slice`] with the `PADDING_MSG_TYPE` short-circuit
/// DISABLED: a slot whose `msg_type` bytes read `0xffff` is decoded as an
/// ordinary record, crc32 and all.
///
/// The MPSC consumer uses this for every slot it does not independently
/// believe is padding (padding is exactly the tail remnant, so `length ==
/// bytes_to_tail` is the test). Without it, the padding short-circuit is a
/// crc-free hole: a producer preempted between its padding claim-word store
/// and its padding BODY write can resurrect a lap later and stamp
/// `PADDING_MSG_TYPE` into bytes 4..6 of a LATER claimant's slot, and that
/// claimant's perfectly valid record would be swallowed silently rather than
/// delivered. See [`RingError::Skipped`]'s padding-stomp case.
///
/// SPSC and Broadcast keep the plain [`decode_record_slice`]: they have a
/// single producer that cannot be raced into a foreign slot, so their
/// padding is never in doubt.
pub fn decode_record_slice_no_padding(
    slot: &[u8],
    payload_buf: &mut Vec<u8>,
) -> Result<(RecordHeader, usize), RingError> {
    decode_record_slice_inner(slot, payload_buf, false)
}

fn decode_record_slice_inner(
    slot: &[u8],
    payload_buf: &mut Vec<u8>,
    allow_padding: bool,
) -> Result<(RecordHeader, usize), RingError> {
    if slot.len() < 6 {
        return Err(RingError::Corrupt(format!(
            "record slice too short: {}",
            slot.len()
        )));
    }
    let msg_type = u16::from_le_bytes([slot[4], slot[5]]);
    if allow_padding && msg_type == PADDING_MSG_TYPE {
        // Padding length is a multiple of RECORD_ALIGN by construction — but
        // `slot.len()` (== the commit word's LENGTH field) is data that
        // arrived from another process, and an unaligned value here would
        // advance `consumer_position` off the RECORD_ALIGN grid, violating
        // `load_commit_word`'s documented alignment SAFETY contract on the
        // very next slot. Verified, not assumed.
        if !slot.len().is_multiple_of(RECORD_ALIGN) {
            return Err(RingError::Corrupt(format!(
                "padding length {} is not RECORD_ALIGN-aligned",
                slot.len()
            )));
        }
        return Ok((
            RecordHeader {
                msg_type,
                flags: 0,
                header_extra: [0; 8],
            },
            slot.len(),
        ));
    }
    if slot.len() < FRAME_HEADER_LEN + FRAME_TRAILER_LEN {
        return Err(RingError::Corrupt(format!(
            "record length {} too small",
            slot.len()
        )));
    }
    let flags = u16::from_le_bytes([slot[6], slot[7]]);
    let mut header_extra = [0u8; 8];
    header_extra.copy_from_slice(&slot[8..FRAME_HEADER_LEN]);
    let payload_end = slot.len() - FRAME_TRAILER_LEN;
    let crc_actual = u32::from_le_bytes(
        slot[payload_end..]
            .try_into()
            .expect("FRAME_TRAILER_LEN bytes remain"),
    );
    if crc32fast::hash(&slot[4..payload_end]) != crc_actual {
        return Err(RingError::BadCrc);
    }
    payload_buf.clear();
    payload_buf.extend_from_slice(&slot[FRAME_HEADER_LEN..payload_end]);
    Ok((
        RecordHeader {
            msg_type,
            flags,
            header_extra,
        },
        align_record_size(slot.len()),
    ))
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
        assert!(matches!(result, Err(RingError::MagicMismatch)));
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

    // ---- M13a: the MPSC commit word (spec §4.1) ---------------------------

    #[test]
    fn commit_word_round_trips_every_field() {
        // Max legal values in each field, so a shift/mask error cannot hide.
        let w = encode_commit_word(COMMIT_LAP_MASK, COMMIT_LEN_MASK, false);
        assert_eq!(
            classify_commit_word(w, COMMIT_LAP_MASK),
            SlotState::Committed {
                length: COMMIT_LEN_MASK
            }
        );
        let c = encode_commit_word(COMMIT_LAP_MASK, COMMIT_LEN_MASK, true);
        assert_eq!(
            classify_commit_word(c, COMMIT_LAP_MASK),
            SlotState::Claimed {
                advance: COMMIT_LEN_MASK
            }
        );
        // The claimed bit is bit 31 and nothing else.
        assert_eq!(c ^ w, COMMIT_CLAIMED);
        // A 64 KiB record — the real `max_msg_size` — fits the length field.
        assert_eq!(
            classify_commit_word(encode_commit_word(3, 65536, false), 3),
            SlotState::Committed { length: 65536 }
        );
    }

    #[test]
    fn a_zero_word_and_a_foreign_lap_both_read_as_empty() {
        // A freshly zeroed ring: every slot reads Empty at lap 0.
        assert_eq!(classify_commit_word(0, 0), SlotState::Empty);
        // The previous lap's COMMITTED record still sitting in the slot.
        let prev = encode_commit_word(4, 40, false);
        assert_eq!(classify_commit_word(prev, 5), SlotState::Empty);
        // The previous lap's CLAIMED word — also not ours.
        let prev_claim = encode_commit_word(4, 40, true);
        assert_eq!(classify_commit_word(prev_claim, 5), SlotState::Empty);
        // Lap matches but length is zero and nothing is claimed: impossible
        // from any producer, so the total classifier reads it as Empty
        // (the consumer then waits, and §4.2's wedge timer adjudicates).
        assert_eq!(
            classify_commit_word(encode_commit_word(5, 0, false), 5),
            SlotState::Empty
        );
    }

    #[test]
    fn lap_is_the_position_divided_by_capacity() {
        assert_eq!(lap_of(0, 4096), 0);
        assert_eq!(lap_of(4095, 4096), 0);
        assert_eq!(lap_of(4096, 4096), 1);
        assert_eq!(lap_of(4096 * 8192, 4096), 0, "13 bits wrap at 8192 laps");
        assert_eq!(lap_of(4096 * 8193, 4096), 1);
    }

    #[test]
    fn decode_record_slice_round_trips_the_real_writer() {
        let payload = b"hello ring";
        let total = FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN;
        let mut slot = vec![0u8; total];
        // SAFETY: `slot` is exactly `total` bytes, exclusively owned here.
        unsafe { write_record_body_at(slot.as_mut_ptr(), 0, 7, 3, [1; 8], payload) };
        slot[..4].copy_from_slice(&encode_commit_word(2, total as u32, false).to_le_bytes());

        let mut buf = Vec::new();
        let (rec, advance) = decode_record_slice(&slot, &mut buf).expect("decodes");
        assert_eq!(rec.msg_type, 7);
        assert_eq!(rec.flags, 3);
        assert_eq!(rec.header_extra, [1; 8]);
        assert_eq!(&buf[..], payload);
        assert_eq!(advance, align_record_size(total));
    }

    #[test]
    fn decode_record_slice_is_total_on_junk() {
        let mut buf = Vec::new();
        // Too short for even a msg_type.
        for n in 0..6usize {
            assert!(
                decode_record_slice(&vec![0xABu8; n], &mut buf).is_err(),
                "len {n}"
            );
        }
        // Long enough for a padding marker but not a record: a non-padding
        // msg_type in a 6..20-byte slice is Corrupt, never a panic.
        let mut short = vec![0u8; 8];
        short[4..6].copy_from_slice(&9u16.to_le_bytes());
        assert!(matches!(
            decode_record_slice(&short, &mut buf),
            Err(RingError::Corrupt(_))
        ));
        // A corrupt crc is BadCrc, not a panic.
        let payload = b"x";
        let total = FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN;
        let mut slot = vec![0u8; total];
        // SAFETY: `slot` is exactly `total` bytes, exclusively owned here.
        unsafe { write_record_body_at(slot.as_mut_ptr(), 0, 1, 0, [0; 8], payload) };
        slot[total - 1] ^= 0xFF;
        assert!(matches!(
            decode_record_slice(&slot, &mut buf),
            Err(RingError::BadCrc)
        ));
    }

    #[test]
    fn decode_record_slice_reads_a_padding_marker() {
        let mut slot = vec![0u8; 24];
        // SAFETY: `slot` is 24 bytes >= the 6 the padding body writes.
        unsafe { write_padding_body_at(slot.as_mut_ptr(), 0) };
        slot[..4].copy_from_slice(&encode_commit_word(0, 24, false).to_le_bytes());
        let mut buf = Vec::new();
        let (rec, advance) = decode_record_slice(&slot, &mut buf).expect("padding decodes");
        assert_eq!(rec.msg_type, PADDING_MSG_TYPE);
        assert_eq!(advance, 24, "padding advances by its whole length");
    }

    #[test]
    fn a_ring_header_written_with_one_magic_is_refused_by_the_other() {
        let (mut mmap, _tmp) = mmap_buf(RING_HEADER_LEN * 2);
        init_ring_header_with_magic(&mut mmap[..], 4096, 1024, 0, crate::magic::RING_MPSC_MAGIC)
            .expect("init");
        assert!(matches!(
            validate_ring_header_with_magic(&mmap[..], crate::magic::RING_MAGIC),
            Err(RingError::MagicMismatch)
        ));
        assert!(validate_ring_header_with_magic(&mmap[..], crate::magic::RING_MPSC_MAGIC).is_ok());
    }
}
