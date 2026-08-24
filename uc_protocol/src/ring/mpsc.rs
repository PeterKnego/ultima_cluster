//! Many-producer single-consumer ring buffer.
//!
//! Producers claim a slot range via `compare_exchange_weak` on
//! `claim_position`, write the record bytes, then publish in claim order by
//! spinning until `publish_position == my_slot_start` before advancing
//! `publish_position` (Release). Consumers load `publish_position` with
//! Acquire and read only fully-published records, so the post-wrap
//! torn-record race the M3 design documented is eliminated.
//!
//! The consumer reads with Relaxed loads on its own `consumer_position`
//! (single reader).
//!
//! ## Producer-panic invariant
//!
//! Producers must not panic between claim and publish: a panicking
//! producer leaves a claimed-but-unpublished slot, and every subsequent
//! producer will spin forever waiting for that slot to publish. In our
//! deployment model an in-process panic implies an unrecoverable node
//! state and process restart; ride along with that assumption.

use std::cell::Cell;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use memmap2::MmapMut;

use crate::ring::common::{
    FRAME_HEADER_LEN, FRAME_TRAILER_LEN, PADDING_MSG_TYPE, ParkMode, RING_HEADER_LEN, RecordHeader,
    RingError, RingHeader, RingWaitHandle, align_record_size, init_ring_header, try_read_record_at,
    validate_ring_header, write_padding_marker_at, write_record_at,
};

/// Spins a producer burns waiting for its predecessor to publish before it
/// starts yielding its core (see the comment at the wait site). ~100 spins
/// is a few hundred nanoseconds — longer than a predecessor that is merely
/// finishing its record write, far shorter than a scheduler quantum.
const PUBLISH_SPINS_BEFORE_YIELD: u32 = 128;

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
}

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
            return Err(RingError::TooLarge {
                len: total,
                max: self.inner.max_msg_size(),
            });
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
            let needed = if bytes_to_tail < advance {
                bytes_to_tail + advance
            } else {
                advance
            };

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
            let claim_size = if bytes_to_tail < advance {
                bytes_to_tail
            } else {
                advance
            };

            let target_pos = claim_pos + claim_size as u64;
            if header
                .claim_position
                .compare_exchange_weak(claim_pos, target_pos, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                continue; // raced with another producer; retry
            }

            // CAS succeeded: we own `[slot_offset, slot_offset + claim_size)`.
            if claim_size != advance {
                // SAFETY: exclusive ownership of the claimed range;
                // claim_size == bytes_to_tail >= RECORD_ALIGN >= 6.
                unsafe {
                    write_padding_marker_at(self.inner.slot_region_mut(), slot_offset, claim_size);
                }
            } else {
                // SAFETY: exclusive ownership of the claimed range.
                unsafe {
                    write_record_at(
                        self.inner.slot_region_mut(),
                        slot_offset,
                        msg_type,
                        flags,
                        header_extra,
                        payload,
                        total,
                    );
                }
            }

            // Publish in claim order: wait until our predecessor has
            // advanced `publish_position` up to our slot start, then bump
            // it to cover our claimed range. Consumers only read records
            // whose bytes are below `publish_position`.
            //
            // M13 hop-bench finding (2026-08-24): a pure spin here CONVOYS as
            // soon as producer threads outnumber free cores. A producer
            // preempted between its CAS and this store stalls every
            // producer behind it, and those spinning at 100% are what keep
            // the preempted one off a core — on the fleet, 8 gateway
            // connections on an 8-vCPU host dropped the ring from 1.9 M/s
            // to ~5 k/s with every core busy. Bounded spin, then yield: the
            // fast path (predecessor already published) is unchanged, and
            // under oversubscription a waiter gives its core to the
            // predecessor instead of fighting it. The structural fix —
            // per-record commit with no cross-producer wait — is M13 work.
            let mut spins: u32 = 0;
            while header.publish_position.load(Ordering::Acquire) != claim_pos {
                if spins < PUBLISH_SPINS_BEFORE_YIELD {
                    spins += 1;
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                }
            }
            header.publish_position.store(target_pos, Ordering::Release);

            if claim_size != advance {
                // Padding marker published; loop to claim the real record.
                continue;
            }
            header.signal(self.mode, false); // MPSC: single consumer -> wake one
            return Ok(());
        }
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
            let producer_pos = header.publish_position.load(Ordering::Acquire);
            let consumer_pos = header.consumer_position.load(Ordering::Relaxed);
            if producer_pos == consumer_pos {
                return Ok(None);
            }

            let slot_offset = (consumer_pos as usize) & (capacity - 1);
            // SAFETY: same as SPSC — slot offset within `[0, capacity)`, mmap
            // outlives the borrow. `publish_position` advances only after
            // record bytes are committed, so no torn-read on wrap.
            let read =
                unsafe { try_read_record_at(self.inner.slot_region(), slot_offset, payload_buf) }?;
            let Some((rec, total_size)) = read else {
                return Ok(None);
            };

            header
                .consumer_position
                .store(consumer_pos + total_size as u64, Ordering::Release);

            if rec.msg_type == PADDING_MSG_TYPE {
                continue;
            }
            return Ok(Some(rec));
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
        let file_len = RING_HEADER_LEN + capacity_bytes as usize;
        // In-place recreate safe: never shrinks, zeros via punch-hole — see
        // `create_shared_backing_file` (the SIGBUS contract).
        let file = super::common::create_shared_backing_file(path, file_len as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        init_ring_header(&mut mmap[..], capacity_bytes, max_msg_size, 0)?;
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
        validate_ring_header(&mmap[..])?;
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
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::thread;
    use tempfile::NamedTempFile;

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
        // "Claimed up to 120, all of it already published" — an
        // invariant-respecting state on its own (publish == claim == 120).
        header.claim_position.store(120, Ordering::Release);
        header.publish_position.store(120, Ordering::Release);
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
