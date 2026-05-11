//! Many-producer single-consumer ring buffer.
//!
//! Producers claim a slot range via `compare_exchange_weak` on
//! `producer_position`, then write the record. The consumer reads with
//! Relaxed loads on its own `consumer_position` (single reader).
//!
//! # Known limitation: post-wrap torn-record race
//!
//! After the producer CAS succeeds, the consumer can observe the advanced
//! `producer_position` *before* the producer has finished writing the record
//! bytes. The `length == 0 → not yet committed` check protects against this
//! on the **first generation** (mmap is zero-initialized), but **not after
//! wrap-around** — the stale length bytes from the previous generation at
//! the same offset can spoof a commit.
//!
//! Fixing this correctly requires either (a) a separate "published-up-to"
//! position that producers advance in claim order (LMAX-Disruptor-style), or
//! (b) per-slot generation counters. Both are tracked for M4 when the
//! client→node submit ring will see real wrap traffic.
//!
//! In M3, MPSC is only used for the cnc control rings (handshake +
//! occasional role-change frames — very low traffic, wrap not expected) and
//! the tests below stay within the first generation. **Do not use MPSC for
//! high-traffic rings until M4 lands the fix.**

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use memmap2::MmapMut;

use crate::ring::common::{
    FRAME_HEADER_LEN, FRAME_TRAILER_LEN, PADDING_MSG_TYPE, RING_HEADER_LEN, RecordHeader,
    RingError, RingHeader, align_record_size, init_ring_header, try_read_record_at,
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
}

pub struct MpscConsumer {
    inner: Arc<MpscInner>,
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
            let consumer_pos = header.consumer_position.load(Ordering::Acquire);
            let producer_pos = header.producer_position.load(Ordering::Acquire);

            let used = producer_pos - consumer_pos;
            let free = capacity.saturating_sub(used as usize);
            if free < advance {
                return Err(RingError::Full);
            }

            let slot_offset = (producer_pos as usize) & (capacity - 1);
            // bytes_to_tail is a multiple of RECORD_ALIGN (see SPSC for proof),
            // so the padding marker's 6-byte write always fits.
            let bytes_to_tail = capacity - slot_offset;

            // If straddling tail, first claim the tail-bytes for a padding
            // marker, then retry the real record claim.
            let claim_size = if bytes_to_tail < advance {
                if free < bytes_to_tail + advance {
                    return Err(RingError::Full);
                }
                bytes_to_tail
            } else {
                advance
            };

            let target_pos = producer_pos + claim_size as u64;
            if header
                .producer_position
                .compare_exchange_weak(
                    producer_pos,
                    target_pos,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
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
                continue; // claim real record on next iteration
            }

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
            return Ok(());
        }
    }
}

impl MpscConsumer {
    pub fn try_read(
        &mut self,
        payload_buf: &mut Vec<u8>,
    ) -> Result<Option<RecordHeader>, RingError> {
        loop {
            let header = self.inner.header();
            let capacity = self.inner.capacity();
            let producer_pos = header.producer_position.load(Ordering::Acquire);
            let consumer_pos = header.consumer_position.load(Ordering::Relaxed);
            if producer_pos == consumer_pos {
                return Ok(None);
            }

            let slot_offset = (consumer_pos as usize) & (capacity - 1);
            // SAFETY: same as SPSC — slot offset within `[0, capacity)`, mmap
            // outlives the borrow. See module docs for the wrap-race caveat.
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
            },
            MpscConsumer { inner: self.inner },
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
        // Stays comfortably within the first generation to avoid the
        // documented post-wrap torn-record race. 8 threads × 50 msgs ×
        // ~24 bytes ≈ 10 KB, ring is 64 KB.
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
}
