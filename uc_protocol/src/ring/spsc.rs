//! Single-producer single-consumer ring buffer.
//!
//! Both producer and consumer hold an `Arc<SpscInner>` so they can be sent to
//! different threads. The mmap is owned inside `SpscInner` so the producer
//! and consumer halves can each be moved independently while still keeping
//! the mapped file alive.
//!
//! # Memory ordering
//!
//! * Producer writes the record bytes first, then does a single `Release`
//!   store on `publish_position`. The Release store synchronizes with the
//!   consumer's `Acquire` load — by the time the consumer observes the new
//!   `publish_position`, the record bytes are fully written. `claim_position`
//!   is unused in SPSC (only the MPSC producer needs a separate claim phase);
//!   the producer tracks its own position by reloading `publish_position`
//!   (Relaxed — it is the sole writer), so the hot path does exactly one
//!   atomic store per record.
//! * Consumer reads with relaxed `consumer_position` (only the consumer
//!   mutates it) and Acquire on `publish_position`. After processing, it
//!   advances `consumer_position` with Release so the producer's next
//!   free-space check sees the freed slot.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use memmap2::MmapMut;

use crate::ring::common::{
    FRAME_HEADER_LEN, FRAME_TRAILER_LEN, PADDING_MSG_TYPE, RING_HEADER_LEN, RecordHeader,
    RingError, RingHeader, align_record_size, init_ring_header, try_read_record_at,
    validate_ring_header, write_padding_marker_at, write_record_at,
};

/// Shared inner state — owns the mmap and exposes the header + slot region.
pub struct SpscInner {
    _mmap: MmapMut,
    base: *mut u8,
    file_len: usize,
}

// SAFETY: `base` points into `_mmap`, which is owned by this struct.
// Synchronization between producer and consumer is via the atomics in
// `RingHeader`.
unsafe impl Send for SpscInner {}
unsafe impl Sync for SpscInner {}

impl SpscInner {
    fn header(&self) -> &RingHeader {
        // SAFETY: `base` points to a valid `RingHeader` (validated at attach
        // time) and the mmap outlives this reference (held in `_mmap`).
        unsafe { &*self.base.cast::<RingHeader>() }
    }

    fn slot_region_mut(&self) -> *mut u8 {
        // SAFETY: `file_len > RING_HEADER_LEN` is guaranteed at construction.
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

    /// Total mmap'd file length, in bytes. Diagnostic only.
    pub fn file_len(&self) -> usize {
        self.file_len
    }
}

pub struct SpscProducer {
    inner: Arc<SpscInner>,
}

pub struct SpscConsumer {
    inner: Arc<SpscInner>,
}

impl SpscProducer {
    /// Try to publish a record. Returns `Err(RingError::Full)` if there isn't
    /// enough free space (caller can retry after the consumer makes progress)
    /// or `Err(RingError::TooLarge)` if the record exceeds `max_msg_size`.
    pub fn try_write(
        &mut self,
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
        // We're the only writer of `publish_position`; Relaxed is fine for the
        // load of our own position (no other thread mutates it). `publish_position`
        // doubles as the producer's claim cursor in SPSC, so we never touch
        // `claim_position` on the hot path.
        let producer_pos = header.publish_position.load(Ordering::Relaxed);
        let consumer_pos = header.consumer_position.load(Ordering::Acquire);

        let used = producer_pos - consumer_pos;
        let free = capacity.saturating_sub(used as usize);
        if free < advance {
            return Err(RingError::Full);
        }

        let slot_offset = (producer_pos as usize) & (capacity - 1);
        // bytes_to_tail is a multiple of RECORD_ALIGN because slot_offset is
        // (claim_position is RECORD_ALIGN-aligned and capacity is a power of two
        // ≥ RECORD_ALIGN), so the padding marker's 6-byte write fits.
        let bytes_to_tail = capacity - slot_offset;
        if bytes_to_tail < advance {
            // Not enough contiguous space at the tail — emit a padding marker
            // and recurse from the freshly-wrapped position. The padding
            // marker takes the rest of the buffer's tail.
            if free < bytes_to_tail + advance {
                // Wrap would leave too little room after the padding.
                return Err(RingError::Full);
            }
            // SAFETY: we are the sole producer; the slot bytes from
            // `slot_offset` to capacity belong to us. `bytes_to_tail >=
            // RECORD_ALIGN >= 6` (padding marker size) by the alignment
            // invariant.
            unsafe {
                write_padding_marker_at(self.inner.slot_region_mut(), slot_offset, bytes_to_tail);
            }
            let padded_pos = producer_pos + bytes_to_tail as u64;
            header.publish_position.store(padded_pos, Ordering::Release);
            return self.try_write(msg_type, flags, header_extra, payload);
        }

        // SAFETY: free-space + tail-wrap checks above ensure
        // `[slot_offset, slot_offset + advance)` lies entirely within the slot
        // region and is not currently being read by the consumer.
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
        let new_pos = producer_pos + advance as u64;
        header.publish_position.store(new_pos, Ordering::Release);
        Ok(())
    }
}

impl SpscConsumer {
    /// Try to consume a record. Returns `Ok(None)` if the ring is empty
    /// (producer hasn't published anything since the last call), `Ok(Some)`
    /// on a successful read.
    ///
    /// On success, `payload_buf` is cleared and filled with the record's
    /// payload bytes.
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
            // SAFETY: producer_pos > consumer_pos and the slot offset is
            // within `[0, capacity)`. The producer has done a Release store
            // on `publish_position`, so all writes to the slot are visible.
            let read =
                unsafe { try_read_record_at(self.inner.slot_region(), slot_offset, payload_buf) }?;
            let Some((rec, total_size)) = read else {
                // Length is zero — producer claimed but hasn't committed.
                // In SPSC this only happens on first generation before the
                // producer's `Release` store on `publish_position` becomes
                // visible; the `Acquire` load above happens-after the
                // producer's earlier writes, so we should not reach here in
                // practice. Return Empty to let the caller retry.
                return Ok(None);
            };

            // Always advance past the record (including padding markers).
            header
                .consumer_position
                .store(consumer_pos + total_size as u64, Ordering::Release);

            if rec.msg_type == PADDING_MSG_TYPE {
                // Skip the padding and try the next record.
                continue;
            }
            return Ok(Some(rec));
        }
    }
}

/// Owns the mmap'd ring file and exposes `into_split` to obtain the producer
/// and consumer halves. The mmap is moved into a shared `Arc<SpscInner>`
/// which both halves clone.
pub struct SpscRing {
    inner: Arc<SpscInner>,
}

impl SpscRing {
    /// Create a fresh ring file. Truncates if the file exists.
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
        let inner = Arc::new(SpscInner {
            _mmap: mmap,
            base,
            file_len,
        });
        Ok(SpscRing { inner })
    }

    /// Attach to an existing ring file (validates header magic).
    pub fn open(path: &Path) -> Result<Self, RingError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        validate_ring_header(&mmap[..])?;
        let file_len = mmap.len();
        let base = mmap.as_mut_ptr();
        let inner = Arc::new(SpscInner {
            _mmap: mmap,
            base,
            file_len,
        });
        Ok(SpscRing { inner })
    }

    /// Split into independent producer and consumer halves. The mmap is held
    /// inside the shared `Arc<SpscInner>` and stays alive until both halves
    /// are dropped.
    pub fn into_split(self) -> (SpscProducer, SpscConsumer) {
        (
            SpscProducer {
                inner: self.inner.clone(),
            },
            SpscConsumer { inner: self.inner },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn write_then_read_single_record() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (mut producer, mut consumer) = ring.into_split();

        producer
            .try_write(7, 0, [1, 2, 3, 4, 0, 0, 0, 0], b"hello")
            .expect("write");

        let mut buf = Vec::new();
        let rec = consumer
            .try_read(&mut buf)
            .expect("read")
            .expect("non-empty");
        assert_eq!(rec.msg_type, 7);
        assert_eq!(rec.header_extra, [1, 2, 3, 4, 0, 0, 0, 0]);
        assert_eq!(&buf[..], b"hello");
    }

    #[test]
    fn read_empty_returns_none() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (_producer, mut consumer) = ring.into_split();

        let mut buf = Vec::new();
        let rec = consumer.try_read(&mut buf).expect("read");
        assert!(rec.is_none());
    }

    #[test]
    fn full_ring_returns_err() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 256, 128).expect("create");
        let (mut producer, _consumer) = ring.into_split();

        let payload = vec![0u8; 64];
        let mut writes = 0;
        loop {
            match producer.try_write(1, 0, [0; 8], &payload) {
                Ok(()) => writes += 1,
                Err(RingError::Full) => break,
                Err(e) => panic!("unexpected error: {e}"),
            }
            if writes > 100 {
                panic!("ring never filled");
            }
        }
        assert!(writes >= 1, "should have written at least once");
    }

    #[test]
    fn wrap_around_via_steady_drain() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 256, 128).expect("create");
        let (mut producer, mut consumer) = ring.into_split();
        let payload = vec![0u8; 64];

        // 5 write/read cycles forces wrap-around with this capacity.
        for i in 0..5 {
            producer
                .try_write(1, 0, [i as u8; 8], &payload)
                .expect("write");
            let mut buf = Vec::new();
            let rec = consumer
                .try_read(&mut buf)
                .expect("read")
                .expect("non-empty");
            assert_eq!(rec.header_extra[0], i as u8);
        }
    }

    /// Regression: with tiny payloads, the unaligned record size used to let
    /// `bytes_to_tail` drop below the padding-marker's 6-byte minimum, causing
    /// `write_padding_marker_at` to write past the slot region. With record
    /// advances forced to `RECORD_ALIGN`-multiples, `bytes_to_tail` is always
    /// `>= RECORD_ALIGN >= 6` whenever wrap is needed. This test drives
    /// dozens of wraps with the smallest possible payload.
    #[test]
    fn tiny_payload_tail_wrap_no_oob() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 64, 32).expect("create");
        let (mut producer, mut consumer) = ring.into_split();

        // 200 cycles of write-one, read-one. With capacity 64 and aligned
        // record size 24 (header 16 + payload 1 + trailer 4 = 21 → align 24),
        // each cycle's slot_offset rotates 24 mod 64, so tail-wrap padding
        // fires regularly with bytes_to_tail ∈ {16, 8}.
        for i in 0..200u8 {
            producer
                .try_write(7, 0, [i; 8], std::slice::from_ref(&i))
                .expect("write");
            let mut buf = Vec::new();
            let rec = consumer
                .try_read(&mut buf)
                .expect("read")
                .expect("non-empty");
            assert_eq!(rec.msg_type, 7);
            assert_eq!(rec.header_extra, [i; 8]);
            assert_eq!(buf, [i]);
        }
    }

    #[test]
    fn cross_thread_send() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = SpscRing::create(tmp.path(), 4096, 1024).expect("create");
        let (mut producer, mut consumer) = ring.into_split();

        let h = std::thread::spawn(move || {
            for i in 0..100u32 {
                let payload = i.to_le_bytes();
                loop {
                    match producer.try_write(2, 0, [0; 8], &payload) {
                        Ok(()) => break,
                        Err(RingError::Full) => std::thread::yield_now(),
                        Err(e) => panic!("{e}"),
                    }
                }
            }
        });

        let mut got = 0u32;
        while got < 100 {
            let mut buf = Vec::new();
            if let Some(_rec) = consumer.try_read(&mut buf).expect("read") {
                let n = u32::from_le_bytes(buf.as_slice().try_into().unwrap());
                assert_eq!(n, got);
                got += 1;
            } else {
                std::thread::yield_now();
            }
        }
        h.join().unwrap();
    }
}
