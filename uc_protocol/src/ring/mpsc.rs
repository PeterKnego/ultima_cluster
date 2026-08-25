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
//! A producer that only STALLED (not died) past `hole_timeout` can resume
//! after its hole was skipped. The consumer marks a skip with a
//! `compare_exchange` from the exact claim word it observed to the skip
//! marker `CLAIMED | LAP | 0` — the one write the otherwise read-only
//! consumer ever makes into the slot region — instead of trusting its own
//! bookkeeping; if the producer committed in the window between the
//! consumer's timeout check and that CAS, the CAS fails harmlessly and the
//! record is delivered normally, `holes_skipped` uncounted. If the marker
//! lands first, the resumed producer's own commit CAS (expecting its claim
//! word, not the marker) fails immediately and it gets
//! [`RingError::Skipped`] — no lap required. The marker also closes the
//! window a bare "later claimant re-stamped the slot" check would leave
//! open across a lap; loss is signalled either way a resurrection can be
//! caught. The one thing this does NOT catch: a resurrection mid-body-write,
//! after the slot has moved on to a yet-later claimant without its commit
//! word changing again in between — `decode_record_slice`'s crc32 surfaces
//! that as a bad read, not a silent corruption, but does not prevent the
//! write. See `MpscProducer::commit`'s doc for the full residual.
//!
//! The consumer reads with Relaxed loads on its own `consumer_position`
//! (single reader) and, other than the one skip-marker CAS above, never
//! writes into the slot region at all.

use std::cell::Cell;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use memmap2::MmapMut;

use crate::ring::common::{
    FRAME_HEADER_LEN, FRAME_TRAILER_LEN, MPSC_MAX_RECORD_BYTES, PADDING_MSG_TYPE, ParkMode,
    RING_HEADER_LEN, RecordHeader, RingError, RingHeader, RingWaitHandle, SlotState,
    align_record_size, cas_commit_word, classify_commit_word, decode_record_slice,
    encode_commit_word, init_ring_header_with_magic, lap_of, load_commit_word,
    store_commit_word, validate_ring_header_with_magic, write_padding_body_at,
    write_record_body_at,
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
        self.commit(claim)
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
    pub fn commit_claim(&self, claim: PendingClaim) -> Result<(), RingError> {
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
            // Invariant: consumer <= claim (in real time, on the shared
            // header) — `publish_position` is a commit count on an MPSC
            // file (module doc), not a position, so it plays no part in
            // this ordering. But the LOCAL `claim_pos`/`consumer_pos`
            // snapshots below can violate `consumer <= claim` — see next
            // paragraph.
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

    /// Publish a claimed record: `compare_exchange` the commit word from our
    /// own claim word to the committed word (Release on success, so it
    /// synchronizes-with the consumer's Acquire load and makes every byte of
    /// the record visible), bump the commit count (the futex wake word), and
    /// wake a parked consumer. No producer is waited on.
    ///
    /// The CAS — not an unconditional store — is the fix for the residual
    /// spec §4.2 states: a producer merely STALLED past `hole_timeout` (a
    /// major fault on the mmap'd ring, VM steal, `SIGSTOP`+resume, a
    /// debugger) is indistinguishable from a dead one, so the consumer may
    /// have already skipped this claim's hole, counted it, and let a LATER
    /// producer re-stamp this exact slot. If commit still stored
    /// unconditionally, a resumed stalled producer would overwrite that
    /// later claimant's slot out from under it — a real, silent data race.
    /// Expecting our own claim word means: if it's still there, we own the
    /// slot and the commit lands normally; if it's gone, someone else now
    /// owns these bytes and we back off with `RingError::Skipped` instead of
    /// touching them. "Gone" covers two cases the consumer can leave behind:
    /// the skip marker `CLAIMED | LAP | 0` (the consumer decided we timed
    /// out and marked us BEFORE any lap), or a later claimant's own claim/
    /// commit word (the consumer's marker CAS raced our resurrection and
    /// lost, and the ring has since lapped back to this exact slot) — either
    /// way the CAS fails and we learn `Skipped` rather than touching bytes
    /// we no longer own.
    ///
    /// Residual that survives even this: the CAS only guards the FIRST word
    /// of the slot. A producer that resumes mid-body-write — after losing
    /// the slot to a later claimant that has since committed and even been
    /// read — can still stomp payload bytes the consumer already delivered,
    /// or ones a still-later claimant is about to write, without the commit
    /// word itself ever changing again in the meantime. The record's crc32
    /// (`decode_record_slice`) surfaces that case as `RingError::BadCrc` or
    /// `Corrupt` rather than a silent misread, but it does not prevent the
    /// write. This is spec §4.2's documented residual, not new to this CAS.
    fn commit(&self, claim: PendingClaim) -> Result<(), RingError> {
        let header = self.inner.header();
        let slot_offset = (claim.pos as usize) & (self.inner.capacity() - 1);
        let advance = align_record_size(claim.total);
        let expected = encode_commit_word(claim.lap, advance as u32, true);
        let committed = encode_commit_word(claim.lap, claim.total as u32, false);
        // SAFETY: `slot_offset` is inside the mapped slot region (bounded by
        // `claim`'s own bookkeeping); the CAS only ever succeeds if the word
        // is still exactly our own claim word, i.e. we still own the range.
        let result = unsafe {
            cas_commit_word(
                self.inner.slot_region_mut(),
                slot_offset,
                expected,
                committed,
                Ordering::Release,
                Ordering::Acquire,
            )
        };
        if result.is_err() {
            return Err(RingError::Skipped { position: claim.pos });
        }
        // `publish_position` reinterpreted as `commit_count` (module doc):
        // the wake word must change on every commit.
        header.publish_position.fetch_add(1, Ordering::Release);
        header.signal(self.mode, false); // MPSC: single consumer -> wake one
        Ok(())
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
                    // `advance` is data written by another process (spec
                    // §4.2's whole premise), and the sibling `Committed` arm
                    // below validates its own length field for exactly the
                    // same reason: an unvalidated `advance` here could push
                    // `consumer_position` past `claim_position` (a silently
                    // dead ring reporting `Ok(None)` forever instead of
                    // `Wedged`), misalign it (violating `load_commit_word`'s
                    // documented alignment SAFETY contract on the very next
                    // slot), or stall forever at `advance == 0`. The
                    // `advance == 0` case is also the skip marker's own
                    // shape (see below) — a marker word is only ever written
                    // BEHIND `consumer_position`, so observing one AT
                    // `consumer_position` is impossible by construction; this
                    // same bounds check catches it as `Corrupt` too, in
                    // defence of that invariant rather than trusting it.
                    let bytes_to_tail = capacity - slot_offset;
                    let max_record = align_record_size(self.inner.max_msg_size())
                        .min(MPSC_MAX_RECORD_BYTES) as u32;
                    if advance == 0
                        || align_record_size(advance as usize) != advance as usize
                        || advance as usize > bytes_to_tail
                        || advance > max_record
                    {
                        return Err(RingError::Corrupt(format!(
                            "claim word advance {advance} out of range at position \
                             {consumer_pos} (tail {bytes_to_tail}, max {max_record})"
                        )));
                    }

                    // A claim not committed within `hole_timeout` is treated
                    // as abandoned — its producer is assumed dead (spec
                    // §4.2). This is an assumption, not a certainty: a
                    // producer that was merely stalled (a major fault on the
                    // mmap'd ring under memory pressure, VM steal, SIGSTOP, a
                    // debugger) can resume anywhere from right now to well
                    // after this. So skipping is not an unconditional store:
                    // it's a `compare_exchange` from the EXACT word we just
                    // observed to the skip marker `CLAIMED | LAP | 0` (spec
                    // §4.1 amendment — the one write the otherwise
                    // read-only consumer ever makes into the slot region).
                    // If the producer committed in the window between our
                    // timeout check and this CAS, the word has already
                    // changed underneath us and the CAS fails harmlessly:
                    // we do NOT skip, do NOT count it, and loop to
                    // re-classify whatever is there now (normally
                    // `Committed`, delivered like any other record). If the
                    // CAS succeeds, the marker is what the producer's own
                    // commit CAS (`MpscProducer::commit`) will find instead
                    // of its claim word if it resumes later — so a
                    // resurrection is refused (`RingError::Skipped`)
                    // immediately, with no lap required, closing the window
                    // the pre-marker design left open (only a LATER
                    // claimant re-stamping the slot, which needs a full lap,
                    // could detect it). This is deliberately NOT "CAS the
                    // commit succeeded, then compare `consumer_position >
                    // claim.pos`" — the consumer may have already delivered
                    // this exact record, and reporting `Skipped` for a
                    // DELIVERED record would let a caller retry and
                    // double-apply it (controller ruling, fix round 1).
                    let lap = lap_of(consumer_pos, capacity);
                    let marker = encode_commit_word(lap, 0, true);
                    // SAFETY: `slot_offset` is a mapped, 4-byte-aligned
                    // address inside the slot region (validated above); the
                    // CAS's `current` is the exact word we just Acquire-
                    // loaded, so a losing CAS only means the word changed
                    // concurrently, never that we mis-targeted memory.
                    let cas = unsafe {
                        cas_commit_word(
                            self.inner.slot_region_mut(),
                            slot_offset,
                            word,
                            marker,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                    };
                    self.hole = None;
                    if cas.is_err() {
                        continue;
                    }
                    self.holes_skipped += 1;
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

    /// Cumulative count of dead-producer holes this consumer has skipped
    /// (spec §4.2). Mirrored to the cnc page and `/metrics` by the node.
    pub fn holes_skipped(&self) -> u64 {
        self.holes_skipped
    }

    /// How long a claimed-but-uncommitted slot must persist before the
    /// consumer treats it as a dead producer's hole (spec §4.2). Default
    /// [`DEFAULT_HOLE_TIMEOUT`]. Lower it only in tests: the legitimate
    /// claim-to-commit window is microseconds, and shortening it trades a
    /// bounded stall for the risk of skipping a merely-descheduled
    /// producer's live record.
    pub fn set_hole_timeout(&mut self, d: std::time::Duration) {
        self.hole_timeout = d;
    }

    /// The current hole timeout.
    pub fn hole_timeout(&self) -> std::time::Duration {
        self.hole_timeout
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

    /// M13a regression for the convoy (spec §4.3's preemption test).
    ///
    /// Producer A claims a slot and STOPS there — exactly the state a
    /// scheduler preemption (or a SIGKILL) between the CAS and the commit
    /// leaves behind. Producers B..H must each complete their own
    /// `try_write` while A is stopped; the consumer must return `None`
    /// (head-of-line behind A) and burn nothing; and once A commits, the
    /// records must come out in claim order, A first.
    ///
    /// Against the pre-M13a protocol this test HANGS: B's `try_write` spins
    /// forever on `publish_position != claim_pos`. That is the discrimination
    /// — run it against `git stash`ed old ring code and it never returns
    /// (bound the run with `timeout 30 cargo test …` if you check that).
    /// Spec §4.2, first case: a producer died between claim and commit. Its
    /// claim word SIZES the hole, so after `hole_timeout` the consumer skips
    /// exactly that range, counts it, and delivers everything behind it.
    #[test]
    fn a_sized_hole_is_skipped_after_the_timeout_and_counted() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();
        consumer.set_hole_timeout(std::time::Duration::from_millis(0));
        assert_eq!(consumer.hole_timeout(), std::time::Duration::from_millis(0));

        // The dead producer: claimed, written, never committed. Dropping the
        // `PendingClaim` IS the death — nothing in the ring changes.
        let dead = producer.claim_without_commit(1, 0, [0; 8], b"lost").expect("claim");
        drop(dead);
        producer.try_write(1, 0, [0; 8], b"kept").expect("write behind the hole");

        // First poll starts the timer and reports nothing.
        let mut buf = Vec::new();
        assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        assert_eq!(consumer.holes_skipped(), 0);

        // Second poll finds the (zero) timeout elapsed: skip, count, deliver.
        let rec = consumer.try_read(&mut buf).expect("read").expect("the record behind the hole");
        assert_eq!(rec.msg_type, 1);
        assert_eq!(&buf[..], b"kept");
        assert_eq!(consumer.holes_skipped(), 1, "the hole is counted exactly once");

        // Nothing else is left, and the counter does not drift.
        assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        assert_eq!(consumer.holes_skipped(), 1);
    }

    /// A hole that resolves BEFORE the timeout is not a hole: no skip, no
    /// count, and the record is delivered normally.
    #[test]
    fn a_hole_that_commits_before_the_timeout_is_never_skipped() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();
        assert_eq!(consumer.hole_timeout(), DEFAULT_HOLE_TIMEOUT, "1 s by default");

        let slow = producer.claim_without_commit(1, 0, [0; 8], b"slow").expect("claim");
        let mut buf = Vec::new();
        for _ in 0..100 {
            assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        }
        producer.commit_claim(slow).expect("commit");
        let rec = consumer.try_read(&mut buf).expect("read").expect("record");
        assert_eq!(rec.msg_type, 1);
        assert_eq!(&buf[..], b"slow");
        assert_eq!(consumer.holes_skipped(), 0);
    }

    /// Spec §4.2, second case: the producer died in the nanoseconds between
    /// its CAS on `claim_position` and its claim-word store, so the slot's
    /// word is still the previous lap's (here: a fresh ring, so zero) while
    /// `claim_position > consumer_position`. The hole's length is unknowable
    /// — the ring refuses to guess and the caller fail-stops.
    ///
    /// Constructed by hand-writing the header atomic, the same technique
    /// `free_space_computation_does_not_underflow_on_stale_claim_snapshot`
    /// uses: `#[cfg(test)]` code in this module has field access to
    /// `MpscInner`, so no production accessor is added for a test.
    #[test]
    fn an_unsized_hole_wedges_after_the_timeout() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();
        consumer.set_hole_timeout(std::time::Duration::from_millis(0));

        // A claim that never stamped its word.
        producer.inner.header().claim_position.store(24, Ordering::Release);

        let mut buf = Vec::new();
        // First poll: the timer starts, nothing is decided yet.
        assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        // Second poll: the timeout has elapsed and the length is unknowable.
        assert!(matches!(
            consumer.try_read(&mut buf),
            Err(RingError::Wedged { position: 0 })
        ));
        assert_eq!(consumer.holes_skipped(), 0, "a wedge is not a skip");
    }

    /// An empty ring must never touch the clock or report a hole: this is the
    /// hot idle poll (the node's consensus agent runs it millions of times a
    /// second). Pinned by behaviour: an empty ring with `claim == consumer`
    /// reports `None` forever with a ZERO hole timeout, which is only
    /// possible if the hole path is never entered.
    #[test]
    fn an_empty_ring_never_reports_a_hole() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();
        consumer.set_hole_timeout(std::time::Duration::from_millis(0));
        let mut buf = Vec::new();
        for _ in 0..1000 {
            assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        }
        assert_eq!(consumer.holes_skipped(), 0);
        // And after a full round trip the ring is empty again, same story.
        producer.try_write(1, 0, [0; 8], b"x").unwrap();
        assert!(consumer.try_read(&mut buf).unwrap().is_some());
        for _ in 0..1000 {
            assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        }
        assert_eq!(consumer.holes_skipped(), 0);
    }

    /// Controller ruling on Task 4 (spec §4.2's stated residual): a producer
    /// merely STALLED past `hole_timeout` — not dead — can resume after the
    /// consumer has skipped its hole. If it resumed and blindly stored its
    /// commit word, and the ring had lapped in the meantime, it would
    /// overwrite a LATER claimant's slot out from under it. Instead the
    /// commit is a `compare_exchange` on the claim word: once the slot has
    /// been re-stamped by a later claimant, the stalled producer's commit
    /// CAS fails and it learns `RingError::Skipped` instead of corrupting
    /// the later claimant's record.
    ///
    /// Forces the lap deterministically: capacity 128, every record here is
    /// a 1-byte payload (record total 21 bytes, `align_record_size` rounds
    /// to 24), so 5 more `try_write`s after A's hole is skipped drive
    /// `claim_position` from 24 up through the tail (padding at 120..128)
    /// and back around to offset 0 — exactly A's original slot — at lap 1,
    /// stomping A's still-resident claim word before A ever tries to commit.
    #[test]
    fn a_resurrected_producer_is_told_it_was_skipped_once_the_ring_laps() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 128, 64).expect("create");
        let (producer, mut consumer) = ring.into_split();
        consumer.set_hole_timeout(std::time::Duration::from_millis(5));

        // A claims at offset 0 (lap 0) and stalls — indistinguishable, from
        // the consumer's side, from dead.
        let a = producer.claim_without_commit(1, 0, [0; 8], b"A").expect("A claims");

        // First poll starts the hole timer.
        let mut buf = Vec::new();
        assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        std::thread::sleep(std::time::Duration::from_millis(20));
        // Second poll: timeout elapsed, hole skipped and counted. Nothing is
        // committed behind it yet, so this poll itself reports None.
        assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        assert_eq!(consumer.holes_skipped(), 1);

        // Five more full round trips of 1-byte records (24 bytes claimed
        // each) walk claim_position from 24 to 152: four land at offsets
        // 24/48/72/96, the fifth straddles the tail (padding at 120..128)
        // and wraps to claim offset 0 again at lap 1 — A's exact slot.
        for i in 0u8..5 {
            producer.try_write(2, 0, [0; 8], &[b'0' + i]).expect("write");
        }

        // A resumes now and tries to commit into a slot a later claimant
        // already owns. The CAS on the claim word fails.
        assert!(matches!(
            producer.commit_claim(a),
            Err(RingError::Skipped { position: 0 })
        ));
        assert_eq!(consumer.holes_skipped(), 1, "a CAS-refused resurrection is not a new hole");

        // The ring still round-trips correctly: every one of the 5 records
        // (padding auto-skipped) comes out, none corrupted or duplicated.
        let mut seen: Vec<Vec<u8>> = Vec::new();
        while seen.len() < 5 {
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => seen.push(buf.clone()),
                Ok(None) => thread::yield_now(),
                Err(e) => panic!("read: {e}"),
            }
        }
        let mut want: Vec<Vec<u8>> = (0u8..5).map(|i| vec![b'0' + i]).collect();
        seen.sort();
        want.sort();
        assert_eq!(seen, want);

        // And the ring keeps working after all this.
        producer.try_write(3, 0, [0; 8], b"still fine").expect("write");
        let rec = consumer.try_read(&mut buf).expect("read").expect("record");
        assert_eq!(rec.msg_type, 3);
        assert_eq!(&buf[..], b"still fine");
    }

    /// Fix round 1, finding 1: `advance` in a `Claimed` word is data written
    /// by another process (spec §4.2's whole premise), and the skip path
    /// must not act on it unvalidated: an oversized value would push
    /// `consumer_position` past `claim_position` (a silently dead ring
    /// reporting `Ok(None)` forever, no `Wedged`), and either an oversized
    /// or misaligned value would misalign the NEXT `load_commit_word`,
    /// violating its documented alignment SAFETY contract. Hand-plants a
    /// claim word whose advance exceeds the ring's own capacity — the same
    /// header-poking technique
    /// `free_space_computation_does_not_underflow_on_stale_claim_snapshot`
    /// and `an_unsized_hole_wedges_after_the_timeout` use.
    #[test]
    fn an_oversized_claim_advance_is_refused_not_desynced() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 128, 64).expect("create");
        let (producer, mut consumer) = ring.into_split();
        consumer.set_hole_timeout(std::time::Duration::from_millis(0));

        // SAFETY: exclusive access to a freshly created, unshared ring.
        unsafe {
            store_commit_word(
                producer.inner.slot_region_mut(),
                0,
                encode_commit_word(0, 128 + 8, true), // advance > capacity
                Ordering::Release,
            );
        }

        let mut buf = Vec::new();
        assert!(matches!(consumer.try_read(&mut buf), Ok(None)), "first poll starts the timer");
        assert!(
            matches!(consumer.try_read(&mut buf), Err(RingError::Corrupt(_))),
            "an oversized advance is refused, not trusted"
        );
        assert_eq!(consumer.holes_skipped(), 0, "a refused corrupt word is not counted as skipped");
        assert_eq!(
            producer.inner.header().consumer_position.load(Ordering::Acquire),
            0,
            "consumer_position must not have moved past an unvalidated advance"
        );
    }

    /// Fix round 1, finding 1 (second case): a claim word whose `advance` is
    /// not a multiple of `RECORD_ALIGN` is refused for the same reason —
    /// acting on it would misalign every subsequent `load_commit_word`.
    #[test]
    fn an_unaligned_claim_advance_is_refused_not_desynced() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 128, 64).expect("create");
        let (producer, mut consumer) = ring.into_split();
        consumer.set_hole_timeout(std::time::Duration::from_millis(0));

        // SAFETY: exclusive access to a freshly created, unshared ring.
        unsafe {
            store_commit_word(
                producer.inner.slot_region_mut(),
                0,
                encode_commit_word(0, 13, true), // 13 is not a multiple of 8
                Ordering::Release,
            );
        }

        let mut buf = Vec::new();
        assert!(matches!(consumer.try_read(&mut buf), Ok(None)));
        assert!(matches!(consumer.try_read(&mut buf), Err(RingError::Corrupt(_))));
        assert_eq!(consumer.holes_skipped(), 0);
    }

    /// Fix round 1, finding 2, race (i): the pre-lap case. A claims and
    /// stalls; the consumer times out and skips the hole (marking the slot
    /// with the skip marker `CLAIMED|LAP|0` via a CAS, not by trusting its
    /// own bookkeeping) WITHOUT any lap ever happening. When A resumes and
    /// tries to commit, its CAS finds the marker instead of its own claim
    /// word and fails immediately — no lap required to detect the
    /// resurrection, unlike the original (pre-fix-round-1) design where only
    /// a later claimant re-stamping the slot could reveal it.
    #[test]
    fn a_skipped_producer_is_told_skipped_via_the_marker_before_any_lap() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();
        consumer.set_hole_timeout(std::time::Duration::from_millis(0));

        let a = producer.claim_without_commit(1, 0, [0; 8], b"A").expect("A claims");

        let mut buf = Vec::new();
        assert!(matches!(consumer.try_read(&mut buf), Ok(None)), "starts the timer");
        assert!(matches!(consumer.try_read(&mut buf), Ok(None)), "skips + marks the hole");
        assert_eq!(consumer.holes_skipped(), 1);

        // A resumes and tries to commit into a slot the consumer has already
        // marked skipped. No lap has happened — this is the marker CAS
        // catching it directly.
        assert!(matches!(
            producer.commit_claim(a),
            Err(RingError::Skipped { position: 0 })
        ));
        assert_eq!(consumer.holes_skipped(), 1, "a refused resurrection is not a new hole");

        // The ring keeps working: a fresh write lands past A's old slot and
        // round-trips normally.
        producer.try_write(2, 0, [0; 8], b"fine").expect("write");
        let rec = consumer.try_read(&mut buf).expect("read").expect("record");
        assert_eq!(rec.msg_type, 2);
        assert_eq!(&buf[..], b"fine");
    }

    /// Fix round 1, finding 2, race (ii): the controller's ruling explicitly
    /// forbids resolving the "did the consumer already pass this record"
    /// question by comparing `claim.pos` against `consumer_position` after a
    /// successful commit CAS — the consumer may have delivered A's own
    /// record already, and reporting `Skipped` for a DELIVERED record would
    /// let a caller retry and double-apply it. This test proves the actual
    /// design doesn't do that: if A commits between the consumer's polls
    /// (deterministically constructed: `commit_claim` is called after the
    /// hole timeout has elapsed but before the consumer's next `try_read`),
    /// the slot is simply `Committed` by the time the consumer looks again —
    /// the `Claimed`/skip path is never entered, A is delivered normally,
    /// and `holes_skipped` stays 0.
    #[test]
    fn a_producer_that_commits_between_polls_is_never_told_skipped() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();
        consumer.set_hole_timeout(std::time::Duration::from_millis(0));

        let a = producer.claim_without_commit(1, 0, [0; 8], b"A").expect("A claims");

        let mut buf = Vec::new();
        // First poll starts (and, with a zero timeout, immediately elapses)
        // the hole timer, but does not act yet.
        assert!(matches!(consumer.try_read(&mut buf), Ok(None)));

        // A wins the race: it commits before the consumer polls again.
        producer.commit_claim(a).expect("A's commit is not refused");

        let rec = consumer.try_read(&mut buf).expect("read").expect("A's record, delivered normally");
        assert_eq!(rec.msg_type, 1);
        assert_eq!(&buf[..], b"A");
        assert_eq!(consumer.holes_skipped(), 0, "A was not skipped — it won the race");
    }

    #[test]
    fn a_stopped_producer_blocks_nobody_and_order_is_preserved() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = MpscRing::create(tmp.path(), 65536, 1024).expect("create");
        let (producer, mut consumer) = ring.into_split();

        // A claims and stops.
        let a = producer
            .claim_without_commit(1, 0, [0; 8], b"A")
            .expect("A claims");

        // B..H each write a full record BEHIND A's hole, on their own
        // threads, and every one of them must finish. The join is the
        // assertion: with the old protocol these threads never return.
        const OTHERS: usize = 7;
        let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handles: Vec<_> = (0..OTHERS)
            .map(|t| {
                let p = producer.clone();
                let done = Arc::clone(&done);
                thread::spawn(move || {
                    let payload = [b'B' + t as u8];
                    p.try_write(1, 0, [0; 8], &payload).expect("write behind the hole");
                    done.fetch_add(1, Ordering::Relaxed);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            done.load(Ordering::Relaxed),
            OTHERS,
            "every producer behind a stopped one must complete"
        );

        // The consumer is head-of-line behind A: nothing readable yet, even
        // though seven records are committed behind it.
        let mut buf = Vec::new();
        for _ in 0..1000 {
            assert!(
                matches!(consumer.try_read(&mut buf), Ok(None)),
                "a claimed-but-uncommitted slot must read as None, never a record"
            );
        }
        assert_eq!(consumer.holes_skipped(), 0, "a 1 s hole timeout has not elapsed");

        // A commits. Now everything drains, A first.
        producer.commit_claim(a).unwrap();
        let mut seen: Vec<Vec<u8>> = Vec::new();
        while seen.len() < OTHERS + 1 {
            let mut buf = Vec::new();
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => seen.push(buf),
                Ok(None) => thread::yield_now(),
                Err(e) => panic!("read: {e}"),
            }
        }
        assert_eq!(seen[0], b"A".to_vec(), "claim order: A was claimed first");
        let mut rest: Vec<Vec<u8>> = seen[1..].to_vec();
        rest.sort();
        let mut want: Vec<Vec<u8>> =
            (0..OTHERS).map(|t| vec![b'B' + t as u8]).collect();
        want.sort();
        assert_eq!(rest, want, "every record behind the hole is delivered exactly once");
    }
}
