//! Single-producer many-consumer broadcast ring buffer (no backpressure).
//!
//! Producer never blocks; if consumers don't keep up, the producer simply
//! overwrites old records and the slow consumer detects it on its next
//! `try_read` (`RingError::Overwritten`). Each consumer holds its own in-
//! memory `head: u64`; the on-disk `consumer_position` is unused for
//! broadcast.
//!
//! # Known limitations
//!
//! * **Wrap-race torn-record on slow-consumer recovery.** Same shape as the
//!   MPSC race documented in `ring::mpsc`: after the producer wraps, a
//!   consumer that lands within capacity but on a slot that's mid-write
//!   could read stale length bytes. M3 doesn't use Broadcast — it's
//!   implemented here for M4's client response ring. Fix tracked for M4.
//! * **No producer↔consumer happens-before across overwrite.** A consumer
//!   that's recovering from `Overwritten` should reset its head to the
//!   producer's *current* position (which the consumer's logic does); it
//!   cannot rewind to consume records the producer has already overwritten.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use memmap2::MmapMut;

use crate::ring::common::{
    FRAME_HEADER_LEN, FRAME_TRAILER_LEN, PADDING_MSG_TYPE, RING_HEADER_LEN, RecordHeader,
    RingError, RingHeader, align_record_size, init_ring_header, try_read_record_at,
    validate_ring_header, write_padding_marker_at, write_record_at,
};

pub struct BroadcastInner {
    _mmap: MmapMut,
    base: *mut u8,
    file_len: usize,
}

unsafe impl Send for BroadcastInner {}
unsafe impl Sync for BroadcastInner {}

impl BroadcastInner {
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

pub struct BroadcastProducer {
    inner: Arc<BroadcastInner>,
}

impl BroadcastProducer {
    /// Publish a record. Never blocks; slow consumers may miss records.
    pub fn write(
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
        let producer_pos = header.producer_position.load(Ordering::Relaxed);
        let slot_offset = (producer_pos as usize) & (capacity - 1);
        // bytes_to_tail is a multiple of RECORD_ALIGN (see SPSC for proof),
        // so the padding marker's 6-byte write always fits.
        let bytes_to_tail = capacity - slot_offset;

        if bytes_to_tail < advance {
            // SAFETY: single producer; we own the tail of the slot region.
            // bytes_to_tail >= RECORD_ALIGN >= 6 (padding marker size).
            unsafe {
                write_padding_marker_at(self.inner.slot_region_mut(), slot_offset, bytes_to_tail);
            }
            header
                .producer_position
                .store(producer_pos + bytes_to_tail as u64, Ordering::Release);
            return self.write(msg_type, flags, header_extra, payload);
        }

        // SAFETY: single producer; the byte range is exclusively ours.
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
        header
            .producer_position
            .store(producer_pos + advance as u64, Ordering::Release);
        Ok(())
    }
}

pub struct BroadcastConsumer {
    inner: Arc<BroadcastInner>,
    head: u64,
}

impl BroadcastConsumer {
    /// Current head position. Diagnostic only.
    pub fn head(&self) -> u64 {
        self.head
    }

    pub fn try_read(
        &mut self,
        payload_buf: &mut Vec<u8>,
    ) -> Result<Option<RecordHeader>, RingError> {
        loop {
            let header = self.inner.header();
            let capacity = self.inner.capacity();
            let producer_pos = header.producer_position.load(Ordering::Acquire);

            if self.head == producer_pos {
                return Ok(None);
            }

            // Check for fall-behind: producer has lapped us by >= capacity.
            if (producer_pos - self.head) as usize > capacity {
                // Reset to current producer position so subsequent reads
                // resume from "now."
                self.head = producer_pos;
                return Err(RingError::Overwritten);
            }

            let slot_offset = (self.head as usize) & (capacity - 1);
            // SAFETY: head < producer_pos and head is within `capacity` of
            // producer_pos, so the slot is within the active region. See
            // module docs for the wrap-race caveat (M3 doesn't use this).
            let read =
                unsafe { try_read_record_at(self.inner.slot_region(), slot_offset, payload_buf) }?;
            let Some((rec, total_size)) = read else {
                return Ok(None);
            };

            self.head += total_size as u64;

            if rec.msg_type == PADDING_MSG_TYPE {
                continue;
            }
            return Ok(Some(rec));
        }
    }
}

pub struct BroadcastRing {
    inner: Arc<BroadcastInner>,
}

impl BroadcastRing {
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
        let inner = Arc::new(BroadcastInner {
            _mmap: mmap,
            base,
            file_len,
        });
        Ok(BroadcastRing { inner })
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
        let inner = Arc::new(BroadcastInner {
            _mmap: mmap,
            base,
            file_len,
        });
        Ok(BroadcastRing { inner })
    }

    pub fn producer(&self) -> BroadcastProducer {
        BroadcastProducer {
            inner: self.inner.clone(),
        }
    }

    /// Subscribe a new consumer at the current producer head. New
    /// subscribers do not see historical records (broadcast is
    /// "join-and-listen").
    pub fn subscribe(&self) -> BroadcastConsumer {
        let head = self
            .inner
            .header()
            .producer_position
            .load(Ordering::Acquire);
        BroadcastConsumer {
            inner: self.inner.clone(),
            head,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn one_producer_two_consumers_same_records() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 65536, 1024).expect("create");
        let mut producer = ring.producer();
        let mut sub_a = ring.subscribe();
        let mut sub_b = ring.subscribe();

        for i in 0..5u8 {
            producer.write(1, 0, [i; 8], b"hello").expect("write");
        }

        for sub in [&mut sub_a, &mut sub_b] {
            for i in 0..5u8 {
                let mut buf = Vec::new();
                let rec = sub.try_read(&mut buf).expect("read").expect("non-empty");
                assert_eq!(rec.header_extra, [i; 8]);
                assert_eq!(&buf[..], b"hello");
            }
            let mut buf = Vec::new();
            assert!(sub.try_read(&mut buf).expect("read").is_none());
        }
    }

    #[test]
    fn slow_consumer_gets_overwritten_error() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 256, 128).expect("create");
        let mut producer = ring.producer();
        let mut sub = ring.subscribe();

        // Write enough to lap the slow consumer multiple times.
        let payload = vec![0u8; 64];
        for _ in 0..20 {
            producer.write(1, 0, [0; 8], &payload).expect("write");
        }

        let mut buf = Vec::new();
        let result = sub.try_read(&mut buf);
        assert!(
            matches!(result, Err(RingError::Overwritten)),
            "slow consumer should see Overwritten, got: {result:?}"
        );
    }

    #[test]
    fn late_subscriber_skips_historical_records() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 65536, 1024).expect("create");
        let mut producer = ring.producer();

        producer.write(1, 0, [0; 8], b"first").expect("write");
        producer.write(1, 0, [0; 8], b"second").expect("write");

        // Subscribe AFTER two records were published.
        let mut sub = ring.subscribe();
        let mut buf = Vec::new();
        assert!(sub.try_read(&mut buf).expect("read").is_none());

        producer.write(1, 0, [0; 8], b"third").expect("write");
        let rec = sub.try_read(&mut buf).expect("read").expect("non-empty");
        let _ = rec;
        assert_eq!(&buf[..], b"third");
    }

    /// Single producer + 2 consumers; wrap several times. Both consumers must
    /// see every record that the producer has not yet lapped them on.
    ///
    /// NOTE: this race is timing-dependent and reliably reproduces only under
    /// `--release`. Run with `cargo test --release -p uc_protocol -- --ignored
    /// ring::broadcast::tests::wrap_no_torn_read` to validate.
    #[ignore = "regression for M4 wrap-fix; un-ignore in Task 1.5"]
    #[test]
    fn wrap_no_torn_read() {
        let tmp = NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 4096, 128).expect("create");
        let mut producer = ring.producer();
        let sub_a = ring.subscribe();
        let sub_b = ring.subscribe();

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Reader closure: reads until `stop` is set, then drains a bit more.
        // Asserts every observed record is a 4-byte u32 (no torn payloads).
        // `Overwritten` is acceptable (slow-consumer recovery).
        let reader = |mut sub: BroadcastConsumer,
                      stop: std::sync::Arc<std::sync::atomic::AtomicBool>| {
            std::thread::spawn(move || {
                let mut seen = 0usize;
                let mut buf = Vec::new();
                let drain_until =
                    || std::time::Instant::now() + std::time::Duration::from_millis(50);
                let mut deadline = None;
                loop {
                    match sub.try_read(&mut buf) {
                        Ok(Some(_rec)) => {
                            assert_eq!(buf.len(), 4, "torn read?");
                            seen += 1;
                        }
                        Ok(None) => {
                            if stop.load(std::sync::atomic::Ordering::Relaxed) {
                                if deadline.is_none() {
                                    deadline = Some(drain_until());
                                }
                                if std::time::Instant::now() >= deadline.unwrap() {
                                    break;
                                }
                            }
                            std::thread::yield_now();
                        }
                        Err(RingError::Overwritten) => { /* slow-consumer reset path */ }
                        Err(e) => panic!("torn record: {e}"),
                    }
                }
                seen
            })
        };

        let a_handle = reader(sub_a, stop.clone());
        let b_handle = reader(sub_b, stop.clone());

        // Writer: 1000 records, ~24 B each => ~6 wraps on a 4 KiB ring.
        let writer = std::thread::spawn(move || {
            for i in 0..1000u32 {
                let payload = i.to_le_bytes();
                producer.write(1, 0, [0; 8], &payload).expect("write");
            }
        });

        writer.join().unwrap();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let a_seen = a_handle.join().unwrap();
        let b_seen = b_handle.join().unwrap();

        // At least *some* records observed; we don't pin the exact count
        // because Overwritten resets are timing-dependent.
        assert!(a_seen > 0 && b_seen > 0, "a={a_seen} b={b_seen}");
    }
}
