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
            let mut consumer_pos = self.cached_consumer_pos.get();
            let mut free = capacity.saturating_sub((claim_pos - consumer_pos) as usize);
            if free < needed {
                consumer_pos = header.consumer_position.load(Ordering::Acquire);
                self.cached_consumer_pos.set(consumer_pos);
                free = capacity.saturating_sub((claim_pos - consumer_pos) as usize);
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
            while header.publish_position.load(Ordering::Acquire) != claim_pos {
                std::hint::spin_loop();
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
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(file_len as u64)?;
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
}
