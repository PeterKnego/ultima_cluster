//! Many-producer single-consumer ring buffer.
//!
//! # Per-record commit (M13a, spec §4.1)
//!
//! A producer claims a byte range with `compare_exchange_weak` on
//! `claim_position` (bounded by an Acquire load of `consumer_position`, so a
//! claim only ever lands on a slot the consumer has finished with), stamps
//! the slot's first word `CLAIMED | LAP | advance`, writes the record, then
//! Release-stores `LAP | total`. **It waits for no other producer at any
//! step.** The single consumer walks records in claim order and decides each
//! slot from that one word: a foreign lap means nothing of ours is here yet,
//! `CLAIMED` means head-of-line behind exactly that producer (no spin, no
//! burn), and a committed word means read it.
//!
//! This replaces the pre-M13a protocol, where publication was serialized in
//! claim order by an unbounded spin on `publish_position`. That convoyed as
//! soon as producer threads outnumbered free cores: a producer preempted
//! between its CAS and its publish stalled every producer behind it, and the
//! spinners were what kept it off a core. Measured on the fleet: 1.9 M/s to
//! ~5 k/s at 8 gateway connections on 8 vCPU, every core busy
//! (`docs/notes/uc2-m13-mpsc-publish-convoy-explained.md`).
//!
//! `publish_position` keeps its name in [`RingHeader`] (SPSC and Broadcast
//! still use it as a byte position) but on an MPSC file it is a
//! **`commit_count`**: a monotonically increasing count of committed
//! records, bumped once per commit purely so the futex wake word changes.
//! Nothing reads it as a position. The MPSC file magic is
//! [`RING_MPSC_MAGIC`](crate::magic::RING_MPSC_MAGIC) so an old-format file
//! cannot be mapped by mistake.
//!
//! ## Producer death
//!
//! A producer that dies between claim and commit leaves a hole. It is no
//! longer fatal to everyone else: the consumer stops at that one record,
//! and after `hole_timeout` (default 1 s) it skips the claimed range,
//! counts it in [`MpscConsumer::holes_skipped`] and carries on. The one
//! unrecoverable case is a death inside the nanoseconds between the CAS and
//! the claim-word store — the hole's length is then unknowable, and
//! `try_read` returns [`RingError::Wedged`] rather than guessing (spec
//! §4.2).
//!
//! The consumer reads with Relaxed loads on its own `consumer_position`
//! (single reader) and never writes into the slot region at all.

use std::cell::Cell;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use memmap2::MmapMut;

use crate::ring::common::{
    FRAME_HEADER_LEN, FRAME_TRAILER_LEN, MPSC_MAX_RECORD_BYTES, PADDING_MSG_TYPE, ParkMode,
    RING_HEADER_LEN, RecordHeader, RingError, RingHeader, RingWaitHandle, SlotState,
    align_record_size, classify_commit_word, decode_record_slice, encode_commit_word,
    init_ring_header_with_magic, lap_of, load_commit_word, store_commit_word,
    validate_ring_header_with_magic, write_padding_body_at, write_record_body_at,
};

pub struct MpscInner {
    _mmap: MmapMut,
    base: *mut u8,
    file_len: usize,
}

// SAFETY: same rationale as SpscInner — base points into _mmap.
unsafe impl Send for MpscInner {}
unsafe impl Sync for MpscInner {}

impl MpscInner {
    fn header(&self) -> &RingHeader {
        unsafe { &*self.base.cast::<RingHeader>() }
    }

    fn slot_region_mut(&self) -> *mut u8 {
        unsafe { self.base.add(RING_HEADER_LEN) }
    }

    fn slot_region(&self) -> *const u8 {
        unsafe { self.base.add(RING_HEADER_LEN) }
    }

    fn capacity(&self) -> usize {
        self.header().capacity_bytes as usize
    }

    fn max_msg_size(&self) -> usize {
        self.header().max_msg_size as usize
    }

    pub fn file_len(&self) -> usize {
        self.file_len
    }
}

#[derive(Clone)]
pub struct MpscProducer {
    inner: Arc<MpscInner>,
    /// Per-producer cached lower bound on the shared `consumer_position`. The
    /// consumer only ever advances `consumer_position`, so a cached copy is
    /// always ≤ the true value; free space computed from it is therefore an
    /// UNDER-estimate (never an over-estimate), so a write admitted by the
    /// cached check can never claim into unread territory. We pay the
    /// cross-core `Acquire` load of the real `consumer_position` only when the
    /// cached value reports the ring full, then re-check. In steady state this
    /// removes one `ldar` per CAS attempt on the write hot path.
    ///
    /// `Cell` because `try_write` takes `&self` (the producer is `Clone` and
    /// fanned out across threads); each clone owns its own cache. This makes
    /// `MpscProducer` `!Sync` (still `Send`): the supported usage is to clone
    /// per producer thread, not to share one `&MpscProducer` across threads.
    cached_consumer_pos: Cell<u64>,
    /// Wakeup mechanism. Must match the consumer's `mode`: a producer in
    /// `Poll` won't `FUTEX_WAKE` a `Futex`-parked consumer (and vice versa) —
    /// mismatched modes silently degrade to poll-sleep latency but are not
    /// unsound. `into_split` sets both to `ParkMode::default()`.
    pub mode: ParkMode,
}

pub struct MpscConsumer {
    inner: Arc<MpscInner>,
    /// Wakeup mechanism; must match the producer's `mode` (see `MpscProducer::mode`).
    pub mode: ParkMode,
    /// The position of the hole currently being timed, and when it was first
    /// observed. `None` when the consumer is not stalled behind one.
    hole: Option<(u64, std::time::Instant)>,
    /// How long a hole must persist before the consumer skips it (or, for an
    /// unsized hole, fail-stops). Spec §4.2's `hole_timeout`.
    hole_timeout: std::time::Duration,
    /// Cumulative count of dead-producer holes skipped.
    holes_skipped: u64,
}

/// Default `hole_timeout` (spec §4.2). The slowest legitimate claim-to-commit
/// is microseconds; a second is four orders of magnitude of headroom.
pub const DEFAULT_HOLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// A claimed, written, not-yet-committed record. Produced by
/// [`MpscProducer::claim_without_commit`], consumed by
/// [`MpscProducer::commit_claim`].
///
/// **Not API** (`#[doc(hidden)]`): it exists so tests and harnesses can
/// reproduce a preempted or dead producer without killing a process — the
/// state a `SIGKILL` between claim and commit leaves behind. `try_write` is
/// exactly `claim` followed by `commit`, so the hook drives the production
/// path, not a copy of it.
#[doc(hidden)]
#[derive(Debug)]
#[must_use = "a claim that is never committed is a hole the consumer must time out"]
pub struct PendingClaim {
    pos: u64,
    total: usize,
    lap: u32,
}

impl MpscProducer {
    pub fn try_write(
        &self,
        msg_type: u16,
        flags: u16,
        header_extra: [u8; 8],
        payload: &[u8],
    ) -> Result<(), RingError> {
        let claim = self.claim(msg_type, flags, header_extra, payload)?;
        self.commit(claim);
        Ok(())
    }

    /// See [`PendingClaim`]. Test/harness hook.
    #[doc(hidden)]
    pub fn claim_without_commit(
        &self,
        msg_type: u16,
        flags: u16,
        header_extra: [u8; 8],
        payload: &[u8],
    ) -> Result<PendingClaim, RingError> {
        self.claim(msg_type, flags, header_extra, payload)
    }

    /// See [`PendingClaim`]. Test/harness hook.
    #[doc(hidden)]
    pub fn commit_claim(&self, claim: PendingClaim) {
        self.commit(claim)
    }

    /// Claim a slot and write the record into it, leaving the slot's word
    /// CLAIMED. Returns without waiting for any other producer.
    fn claim(
        &self,
        msg_type: u16,
        flags: u16,
        header_extra: [u8; 8],
        payload: &[u8],
    ) -> Result<PendingClaim, RingError> {
        let total = FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN;
        if total > self.inner.max_msg_size() {
            return Err(RingError::TooLarge { len: total, max: self.inner.max_msg_size() });
        }
        // Positions advance in RECORD_ALIGN-sized steps; the length field still
        // stores the unaligned `total` so the consumer can decode payload_len.
        let advance = align_record_size(total);

        let header = self.inner.header();
        let capacity = self.inner.capacity();

        loop {
            let claim_pos = header.claim_position.load(Ordering::Acquire);

            let slot_offset = (claim_pos as usize) & (capacity - 1);
            // bytes_to_tail is a multiple of RECORD_ALIGN (see SPSC for proof),
            // so the padding marker's 6-byte write always fits.
            let bytes_to_tail = capacity - slot_offset;
            // Total contiguous bytes this iteration must reserve: `advance`, or
            // — if the record straddles the tail — a padding marker filling
            // `bytes_to_tail` plus `advance` after the wrap.
            let needed = if bytes_to_tail < advance { bytes_to_tail + advance } else { advance };

            // Free space from the cached consumer position (a lower bound, so
            // `free` is under-estimated — safe). Only when the cache reports
            // too little room do we pay the `Acquire` load of the real
            // `consumer_position` and re-check.
            //
            // Invariant: consumer <= publish <= claim (in real time, on the
            // shared header). But the LOCAL `claim_pos`/`consumer_pos`
            // snapshots below can violate that ordering — see next paragraph.
            //
            // `claim_pos - consumer_pos` uses `saturating_sub`, not plain `-`:
            // under concurrent producers, `claim_pos` (loaded once at the top
            // of this loop iteration) can go stale relative to a freshly
            // reloaded `consumer_pos` if another producer's CAS advances the
            // real `claim_position` past our snapshot in between — unlike
            // SPSC (single producer, so its own `producer_pos` can never be
            // overtaken), MPSC has no such single-writer guarantee on
            // `claim_pos`. A stale-negative `claim_pos - consumer_pos` just
            // means our free-space ESTIMATE was pessimistic-then-corrected;
            // it is never the actual safety mechanism against an overrun —
            // the `compare_exchange_weak` on `claim_position` below re-checks
            // against the CURRENT value and simply fails+retries if our
            // snapshot was stale, so clamping to 0 here (reading as "fully
            // free" this iteration) cannot cause a claim past the real tail.
            let mut consumer_pos = self.cached_consumer_pos.get();
            let mut free = capacity.saturating_sub(claim_pos.saturating_sub(consumer_pos) as usize);
            if free < needed {
                consumer_pos = header.consumer_position.load(Ordering::Acquire);
                self.cached_consumer_pos.set(consumer_pos);
                free = capacity.saturating_sub(claim_pos.saturating_sub(consumer_pos) as usize);
                if free < needed {
                    return Err(RingError::Full);
                }
            }

            // If straddling tail, claim only the tail-bytes for a padding
            // marker this iteration, then retry the real record claim.
            let claim_size = if bytes_to_tail < advance { bytes_to_tail } else { advance };
            let target_pos = claim_pos + claim_size as u64;
            if header
                .claim_position
                .compare_exchange_weak(claim_pos, target_pos, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                continue; // raced with another producer; retry
            }

            // CAS succeeded: we own `[slot_offset, slot_offset + claim_size)`.
            let lap = lap_of(claim_pos, capacity);

            if claim_size != advance {
                // Tail straddle. The padding marker has no body to write, so
                // it is claimed, written and committed in one go; then loop
                // to claim the real record after the wrap. No commit_count
                // bump and no `signal`: padding carries nothing a parked
                // consumer needs to wake for, and the real record's commit
                // (one iteration later) does both.
                //
                // SAFETY: exclusive ownership of the claimed range;
                // claim_size == bytes_to_tail >= RECORD_ALIGN >= 6.
                unsafe {
                    store_commit_word(
                        self.inner.slot_region_mut(),
                        slot_offset,
                        encode_commit_word(lap, claim_size as u32, true),
                        Ordering::Relaxed,
                    );
                    write_padding_body_at(self.inner.slot_region_mut(), slot_offset);
                    store_commit_word(
                        self.inner.slot_region_mut(),
                        slot_offset,
                        encode_commit_word(lap, claim_size as u32, false),
                        Ordering::Release,
                    );
                }
                continue;
            }

            // Stamp the claim BEFORE writing the body: it is what lets a dead
            // producer's hole be sized (spec §4.2). Relaxed is enough — the
            // consumer that observes it only ever decides "not yet", and the
            // commit's Release is what orders the body.
            //
            // SAFETY: exclusive ownership of the claimed range.
            unsafe {
                store_commit_word(
                    self.inner.slot_region_mut(),
                    slot_offset,
                    encode_commit_word(lap, advance as u32, true),
                    Ordering::Relaxed,
                );
                write_record_body_at(
                    self.inner.slot_region_mut(),
                    slot_offset,
                    msg_type,
                    flags,
                    header_extra,
                    payload,
                );
            }
            return Ok(PendingClaim { pos: claim_pos, total, lap });
        }
    }

    /// Publish a claimed record: Release-store the commit word (which
    /// synchronizes-with the consumer's Acquire load and makes every byte of
    /// the record visible), bump the commit count (the futex wake word), and
    /// wake a parked consumer. No producer is waited on.
    fn commit(&self, claim: PendingClaim) {
        let header = self.inner.header();
        let slot_offset = (claim.pos as usize) & (self.inner.capacity() - 1);
        // SAFETY: we have owned this range since the CAS; this store hands it
        // to the consumer.
        unsafe {
            store_commit_word(
                self.inner.slot_region_mut(),
                slot_offset,
                encode_commit_word(claim.lap, claim.total as u32, false),
                Ordering::Release,
            );
        }
        // `publish_position` reinterpreted as `commit_count` (module doc):
        // the wake word must change on every commit.
        header.publish_position.fetch_add(1, Ordering::Release);
        header.signal(self.mode, false); // MPSC: single consumer -> wake one
    }
}

impl MpscConsumer {
    /// Handle for a parker thread to block on this ring while the owner reads.
    pub fn wait_handle(&self) -> RingWaitHandle {
        RingWaitHandle::new(self.inner.clone(), self.inner.header(), self.mode)
    }

    pub fn try_read(
        &mut self,
        payload_buf: &mut Vec<u8>,
    ) -> Result<Option<RecordHeader>, RingError> {
        loop {
            let header = self.inner.header();
            let capacity = self.inner.capacity();
            let consumer_pos = header.consumer_position.load(Ordering::Relaxed);
            let slot_offset = (consumer_pos as usize) & (capacity - 1);

            // SAFETY: `slot_offset < capacity` and is RECORD_ALIGN-aligned, so
            // the address is inside the mapping and 4-byte aligned.
            let word = unsafe { load_commit_word(self.inner.slot_region(), slot_offset) };

            match classify_commit_word(word, lap_of(consumer_pos, capacity)) {
                SlotState::Empty => {
                    // Nothing of ours here. Either the ring is genuinely
                    // empty, or a producer has CAS-claimed this range and its
                    // claim word has not landed yet (nanoseconds), or — the
                    // §4.2 residual — it died in exactly that window.
                    if header.claim_position.load(Ordering::Acquire) <= consumer_pos {
                        self.hole = None;
                        return Ok(None);
                    }
                    // NOTE: the clock is read only on this path and the
                    // Claimed path. An empty ring (claim == consumer) is the
                    // hot idle poll and never touches it.
                    if self.hole_elapsed(consumer_pos) {
                        return Err(RingError::Wedged { position: consumer_pos });
                    }
                    return Ok(None);
                }
                SlotState::Claimed { advance } => {
                    if !self.hole_elapsed(consumer_pos) {
                        // Head-of-line behind exactly this one producer. No
                        // spin, no burn — the caller polls or parks.
                        return Ok(None);
                    }
                    // Dead producer (spec §4.2): its claim is sized, so skip
                    // it. The client that died never gets an answer — correct,
                    // it is dead.
                    self.holes_skipped += 1;
                    self.hole = None;
                    // Re-derive the header reference rather than reusing the
                    // outer `header` binding: `hole_elapsed` above takes
                    // `&mut self`, which the outer binding (borrowed from
                    // `self.inner`) cannot stay live across.
                    self.inner
                        .header()
                        .consumer_position
                        .store(consumer_pos + advance as u64, Ordering::Release);
                    continue;
                }
                SlotState::Committed { length } => {
                    self.hole = None;
                    let len = length as usize;
                    let bytes_to_tail = capacity - slot_offset;
                    let max_record = align_record_size(self.inner.max_msg_size())
                        .min(MPSC_MAX_RECORD_BYTES);
                    if len < 6 || len > bytes_to_tail || len > max_record {
                        return Err(RingError::Corrupt(format!(
                            "commit word length {len} out of range at position {consumer_pos} \
                             (tail {bytes_to_tail}, max {max_record})"
                        )));
                    }
                    // SAFETY: `[slot_offset, slot_offset + len)` is inside the
                    // slot region (len <= bytes_to_tail) and is fully written
                    // and stable: the Acquire load of the commit word above
                    // synchronizes-with the producer's Release commit store,
                    // made after the record bytes, and the bounded claim means
                    // no producer can reclaim this range until we advance
                    // `consumer_position` below.
                    let slot = unsafe {
                        std::slice::from_raw_parts(
                            self.inner.slot_region().add(slot_offset),
                            len,
                        )
                    };
                    let (rec, advance) = decode_record_slice(slot, payload_buf)?;
                    header
                        .consumer_position
                        .store(consumer_pos + advance as u64, Ordering::Release);
                    if rec.msg_type == PADDING_MSG_TYPE {
                        continue;
                    }
                    return Ok(Some(rec));
                }
            }
        }
    }

    /// First observation of a hole at `pos` starts its timer and reports
    /// `false`; a later observation of the SAME position reports whether
    /// `hole_timeout` has elapsed. Moving on clears the timer.
    fn hole_elapsed(&mut self, pos: u64) -> bool {
        match self.hole {
            Some((p, since)) if p == pos => since.elapsed() >= self.hole_timeout,
            _ => {
                self.hole = Some((pos, std::time::Instant::now()));
                false
            }
        }
    }
}

pub struct MpscRing {
    inner: Arc<MpscInner>,
}

impl MpscRing {
    pub fn create(path: &Path, capacity_bytes: u64, max_msg_size: u32) -> Result<Self, RingError> {
        if !capacity_bytes.is_power_of_two() {
            return Err(RingError::Corrupt(format!(
                "capacity_bytes must be power of two, got {capacity_bytes}"
            )));
        }
        if align_record_size(max_msg_size as usize) > MPSC_MAX_RECORD_BYTES {
            return Err(RingError::Corrupt(format!(
                "max_msg_size {max_msg_size} exceeds the commit word's {MPSC_MAX_RECORD_BYTES}-byte length field"
            )));
        }
        let file_len = RING_HEADER_LEN + capacity_bytes as usize;
        // In-place recreate safe: never shrinks, zeros via punch-hole — see
        // `create_shared_backing_file` (the SIGBUS contract).
        let file = super::common::create_shared_backing_file(path, file_len as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        init_ring_header_with_magic(
            &mut mmap[..],
            capacity_bytes,
            max_msg_size,
            0,
            crate::magic::RING_MPSC_MAGIC,
        )?;
        let base = mmap.as_mut_ptr();
        let inner = Arc::new(MpscInner {
            _mmap: mmap,
            base,
            file_len,
        });
        Ok(MpscRing { inner })
    }

    pub fn open(path: &Path) -> Result<Self, RingError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        validate_ring_header_with_magic(&mmap[..], crate::magic::RING_MPSC_MAGIC)?;
        let file_len = mmap.len();
        let base = mmap.as_mut_ptr();
        let inner = Arc::new(MpscInner {
            _mmap: mmap,
            base,
            file_len,
        });
        Ok(MpscRing { inner })
    }

    /// Returns a clonable producer and the single consumer. Clone the
    /// producer to fan in from multiple threads.
    pub fn into_split(self) -> (MpscProducer, MpscConsumer) {
        (
            MpscProducer {
                inner: self.inner.clone(),
                cached_consumer_pos: Cell::new(0),
                mode: ParkMode::default(),
            },
            MpscConsumer {
                inner: self.inner,
                mode: ParkMode::default(),
                hole: None,
                hole_timeout: DEFAULT_HOLE_TIMEOUT,
                holes_skipped: 0,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::common::MPSC_MAX_RECORD_BYTES;
    use memmap2::MmapMut;
    use std::collections::HashSet;
    use std::thread;
    use tempfile::NamedTempFile;

    /// M13a: an MPSC ring file written by a pre-M13a binary carries
    /// `RING_MAGIC`, and its slots use the old "length, 0 = uncommitted"
    /// word with publication ordered by `publish_position`. A new binary
    /// that mapped it would misread every slot, so the attach is refused.
    /// This is the whole reason the magic was bumped — the operator-visible
    /// consequence is "restart node, service, gateway and clients on a host
    /// together" (docs/how-to/upgrade-a-cluster.md).
    #[test]
    fn an_old_format_ring_file_is_refused_on_open() {
        let tmp = NamedTempFile::new().unwrap();
        // Build a file with the OLD magic, exactly as a pre-M13a
        // `MpscRing::create` would have.
        let file = crate::ring::common::create_shared_backing_file(
            tmp.path(),
            (RING_HEADER_LEN + 4096) as u64,
        )
        .unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        crate::ring::common::init_ring_header_with_magic(
            &mut mmap[..],
            4096,
            1024,
            0,
            crate::magic::RING_MAGIC,
        )
        .unwrap();
        drop(mmap);

        assert!(matches!(MpscRing::open(tmp.path()), Err(RingError::MagicMismatch)));

        // And the reverse direction is covered too: a file this binary
        // creates carries the new magic.
        let fresh = NamedTempFile::new().unwrap();
        MpscRing::create(fresh.path(), 4096, 1024).expect("create");
        let bytes = std::fs::read(fresh.path()).unwrap();
        assert_eq!(&bytes[..8], &crate::magic::RING_MPSC_MAGIC[..]);
    }

    /// M13a: the commit word's LENGTH field is 18 bits, so a ring whose
    /// `max_msg_size` cannot be expressed is refused at creation rather than
    /// silently truncating a length at runtime.
    #[test]
    fn create_refuses_a_max_msg_size_the_commit_word_cannot_hold() {
        let tmp = NamedTempFile::new().unwrap();
        let too_big = (MPSC_MAX_RECORD_BYTES + 1) as u32;
        assert!(matches!(
            MpscRing::create(tmp.path(), 1 << 20, too_big),
            Err(RingError::Corrupt(_))
        ));
        // The real node's 64 KiB is comfortably inside the field.
        let ok = NamedTempFile::new().unwrap();
        MpscRing::create(ok.path(), 1 << 20, 64 << 10).expect("64 KiB max_msg_size is legal");
    }

    #[test]
    fn single_producer_round_trip() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();
        producer.try_write(9, 0, [9; 8], b"world").expect("write");

        let mut buf = Vec::new();
        let rec = consumer
            .try_read(&mut buf)
            .expect("read")
            .expect("non-empty");
        assert_eq!(rec.msg_type, 9);
        assert_eq!(rec.header_extra, [9; 8]);
        assert_eq!(&buf[..], b"world");
    }

    #[test]
    fn many_producers_one_consumer_no_wrap() {
        // Stays comfortably within the first generation (no wrap).
        // 8 threads × 50 msgs × ~24 bytes ≈ 10 KB, ring is 64 KB.
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 65536, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();

        const N_THREADS: usize = 8;
        const PER_THREAD: usize = 50;

        let handles: Vec<_> = (0..N_THREADS)
            .map(|t| {
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
            })
            .collect();

        let mut received: HashSet<Vec<u8>> = HashSet::new();
        while received.len() < N_THREADS * PER_THREAD {
            let mut buf = Vec::new();
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => {
                    received.insert(buf);
                }
                Ok(None) => thread::yield_now(),
                Err(e) => panic!("{e}"),
            }
        }

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(received.len(), N_THREADS * PER_THREAD);
    }

    /// 8 producers × 200 records on a tiny ring (~64 records' worth of
    /// capacity) forces many wraps. Verifies the post-wrap torn-record race
    /// is gone: every record written is read back exactly once, no panics.
    ///
    /// NOTE: this race is timing-dependent and most visible under `--release`.
    #[test]
    fn wrap_under_many_producers_no_torn_read() {
        let tmp = NamedTempFile::new().unwrap();
        // 4 KiB capacity, ~24 B/record => ~170 records/generation;
        // 8 × 200 = 1600 records forces ~9 wraps.
        let ring = MpscRing::create(tmp.path(), 4096, 128).expect("create");
        let (producer, mut consumer) = ring.into_split();

        const N_THREADS: usize = 8;
        const PER_THREAD: usize = 200;

        let handles: Vec<_> = (0..N_THREADS)
            .map(|t| {
                let p = producer.clone();
                thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        let payload = format!("t{t}-i{i}").into_bytes();
                        loop {
                            match p.try_write(1, 0, [0; 8], &payload) {
                                Ok(()) => break,
                                Err(RingError::Full) => thread::yield_now(),
                                Err(e) => panic!("write: {e}"),
                            }
                        }
                    }
                })
            })
            .collect();

        let mut received: HashSet<Vec<u8>> = HashSet::new();
        let total = N_THREADS * PER_THREAD;
        while received.len() < total {
            let mut buf = Vec::new();
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => {
                    assert!(received.insert(buf), "duplicate or torn record read");
                }
                Ok(None) => thread::yield_now(),
                Err(e) => panic!("read: {e}"),
            }
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(received.len(), total);
    }

    /// Regression test for the free-space underflow fix (commit 8c1ae01).
    ///
    /// `try_write`'s free-space check computes
    /// `claim_pos.saturating_sub(consumer_pos)`. The real header obeys
    /// `consumer <= publish <= claim` at all times, but a PRODUCER'S LOCAL
    /// SNAPSHOT of `claim_pos` (read once at the top of the loop) can go
    /// stale relative to a freshly (`Acquire`) reloaded `consumer_position`:
    /// under multi-producer contention another producer can race
    /// `claim_position`/`publish_position` forward and the consumer can
    /// drain past THIS producer's now-stale view of it, so for the LOCAL
    /// variables `claim_pos < consumer_pos` even though the shared header
    /// never actually violates the invariant. If that subtraction were
    /// plain `-` instead of `saturating_sub`, it would underflow a `u64`
    /// and panic in a debug build (`attempt to subtract with overflow`).
    ///
    /// Reproducing the exact multi-thread interleaving is timing-dependent
    /// (see `wrap_under_many_producers_no_torn_read`'s doc-note on that
    /// class of race). Instead this test drives the SAME production code
    /// path (`MpscProducer::try_write`) deterministically by writing the
    /// ring's header atomics directly — `#[cfg(test)]` code in this module
    /// has field access to `MpscInner`/`MpscProducer` internals via the
    /// crate-private fields, no accessor needed — to force exactly the
    /// `claim_pos < consumer_pos` shape the fix guards against, then
    /// asserts `try_write` does not panic and completes successfully
    /// (reading the stale-claim state as "fully free", per the fix's
    /// documented safety argument: the subsequent CAS re-checks against the
    /// real `claim_position` and simply retries if a snapshot was stale, so
    /// clamping to 0 here is never itself the overrun-safety mechanism).
    ///
    /// Verified to actually discriminate the fix: temporarily reverting
    /// both `saturating_sub` calls in this free-space computation back to
    /// plain `-` reproduces `panicked at ... attempt to subtract with
    /// overflow` on this exact test in a debug build; restoring
    /// `saturating_sub` makes it pass again. (Manually confirmed while
    /// authoring this test; not re-verified on every CI run.)
    #[test]
    fn free_space_computation_does_not_underflow_on_stale_claim_snapshot() {
        let tmp = NamedTempFile::new().unwrap();
        // Small power-of-two ring: a handful of "claimed" bytes forces the
        // tail-straddle + cache-miss-reload branches deterministically.
        let ring = MpscRing::create(tmp.path(), 128, 128).expect("create");
        let (producer, _consumer) = ring.into_split();

        let header = producer.inner.header();
        header.claim_position.store(120, Ordering::Release);
        // `publish_position` is the commit COUNT on an MPSC file (M13a); the
        // free-space arithmetic under test never reads it. Left at 0.
        // Force the shape the fix guards against: the (about to be
        // reloaded) real `consumer_position` sits AHEAD of the claim
        // snapshot this call will read. This is the deterministic proxy
        // for "another producer's claim + a fast consumer overtook this
        // producer's stale view" described above.
        header.consumer_position.store(200, Ordering::Release);
        // Force the cache-miss reload path: with the cache at 0, the first
        // (cache-based) free-space read looks like "ring nearly full"
        // against `claim_pos = 120`, so `try_write` pays the `Acquire`
        // reload of the real `consumer_position` (200) above — that
        // reload's subsequent computation is the one under test.
        producer.cached_consumer_pos.set(0);

        // Old code (`claim_pos - consumer_pos` with `claim_pos(120) <
        // consumer_pos(200)`) panics here in debug; the `saturating_sub`
        // fix must not, and the write must succeed.
        producer
            .try_write(1, 0, [0; 8], b"x")
            .expect("stale-claim snapshot must read as fully-free, not underflow/panic");
    }
}
