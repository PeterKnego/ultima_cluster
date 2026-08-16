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
//! * **No producer↔consumer happens-before across overwrite.** A consumer
//!   that's recovering from `Overwritten` should reset its head to the
//!   producer's *current* `publish_position` (which the consumer's logic
//!   does); it cannot rewind to consume records the producer has already
//!   overwritten.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use memmap2::MmapMut;

use crate::ring::common::{
    FRAME_HEADER_LEN, FRAME_TRAILER_LEN, PADDING_MSG_TYPE, ParkMode, RING_HEADER_LEN, RecordHeader,
    RingError, RingHeader, RingWaitHandle, align_record_size, init_ring_header, try_read_record_at,
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
    /// Wakeup mechanism. Must match consumers' `mode`. `producer()` sets
    /// `ParkMode::default()`; override per-instance if needed.
    pub mode: ParkMode,
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
        // Single producer: `publish_position` is our sole cursor. `claim_position`
        // is unused in Broadcast (consumers track their own in-memory `head` and
        // only read `publish_position`), so we never touch it on the hot path.
        // Relaxed load is fine — we are the only writer of `publish_position`.
        let producer_pos = header.publish_position.load(Ordering::Relaxed);
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
            let padded_pos = producer_pos + bytes_to_tail as u64;
            header.publish_position.store(padded_pos, Ordering::Release);
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
        let new_pos = producer_pos + advance as u64;
        header.publish_position.store(new_pos, Ordering::Release);
        header.signal(self.mode, true); // Broadcast: wake ALL parked consumers
        Ok(())
    }
}

pub struct BroadcastConsumer {
    inner: Arc<BroadcastInner>,
    head: u64,
    /// Wakeup mechanism; must match the producer's `mode` (see `BroadcastProducer::mode`).
    pub mode: ParkMode,
}

impl BroadcastConsumer {
    /// Current head position. Diagnostic only.
    pub fn head(&self) -> u64 {
        self.head
    }

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

            if self.head == producer_pos {
                return Ok(None);
            }

            // Fast-path fall-behind: the producer is already a full capacity (or
            // more) ahead, so the slot at `head` is being — or has been —
            // overwritten. Reset to "now" and signal the slow consumer.
            if (producer_pos - self.head) as usize >= capacity {
                self.head = producer_pos;
                return Err(RingError::Overwritten);
            }

            let slot_offset = (self.head as usize) & (capacity - 1);
            let head_before = self.head;
            // SAFETY: head < producer_pos and head is within `capacity` of
            // producer_pos, so the slot is within the active region.
            //
            // Broadcast has NO backpressure (unlike SPSC/MPSC, whose
            // `consumer_position` stops the producer): the single producer can
            // LAP us during the copy below, overwriting the very slot we are
            // reading. The pre-read check above is only a snapshot, so the result
            // of this read is NOT trustworthy until we re-check the producer
            // position. A torn read otherwise escapes as a hard `BadCrc`.
            let read =
                unsafe { try_read_record_at(self.inner.slot_region(), slot_offset, payload_buf) };
            // Re-validate AFTER the copy (seqlock-style read barrier): the Acquire
            // fence keeps the payload copy from being reordered past this load on
            // weak memory models. If the producer advanced a full capacity beyond
            // the record we read, that record was (or was being) overwritten
            // mid-read — discard whatever we read/errored and surface the defined
            // slow-consumer signal. Only a genuine (non-overwrite) decode error
            // reaches the caller.
            std::sync::atomic::fence(Ordering::Acquire);
            let producer_pos2 = header.publish_position.load(Ordering::Acquire);
            if (producer_pos2 - head_before) as usize >= capacity {
                self.head = producer_pos2;
                return Err(RingError::Overwritten);
            }
            let Some((rec, total_size)) = read? else {
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
        // In-place recreate safe: never shrinks, zeros via punch-hole — see
        // `create_shared_backing_file` (the SIGBUS contract).
        let file = super::common::create_shared_backing_file(path, file_len as u64)?;
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
            mode: ParkMode::default(),
        }
    }

    /// Subscribe a new consumer at the current producer head. New
    /// subscribers do not see historical records (broadcast is
    /// "join-and-listen").
    pub fn subscribe(&self) -> BroadcastConsumer {
        let head = self.inner.header().publish_position.load(Ordering::Acquire);
        BroadcastConsumer {
            inner: self.inner.clone(),
            head,
            mode: ParkMode::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn wake_all_unblocks_two_consumers() {
        use crate::ring::common::ParkMode;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 4096, 256).expect("create");
        let mut producer = ring.producer();
        producer.mode = ParkMode::Futex;
        let mk = || {
            let mut c = ring.subscribe();
            c.mode = ParkMode::Futex;
            c.wait_handle()
        };
        let (h1, h2) = (mk(), mk());
        // Count only consumers woken *promptly* (by the signal, not by their own
        // park timeout). A wake-one regression (`signal(.., false)`) would wake
        // one thread immediately and leave the other parked until `park_to`,
        // so it would NOT be counted and the assert would fail.
        let park_to = std::time::Duration::from_millis(2000);
        let prompt = std::time::Duration::from_millis(500);
        let woke_promptly = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let mut threads = Vec::new();
        for h in [h1, h2] {
            let w = woke_promptly.clone();
            threads.push(std::thread::spawn(move || {
                // arm before snapshotting seq so the producer reliably observes
                // this waiter; the futex `expected` value guards the gap.
                h.arm();
                let seq = h.current_seq();
                let t0 = std::time::Instant::now();
                h.park(seq, park_to);
                let elapsed = t0.elapsed();
                h.disarm();
                if elapsed < prompt {
                    w.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                }
            }));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        producer.write(1, 0, [0; 8], b"x").expect("write"); // signals wake-all
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(
            woke_promptly.load(std::sync::atomic::Ordering::Acquire),
            2,
            "wake-all must wake BOTH parked consumers promptly (a wake-one \
             regression would leave one parked until its timeout)"
        );
    }

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
    /// NOTE: this race is timing-dependent and most visible under `--release`.
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
                      stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
                      started: std::sync::Arc<std::sync::atomic::AtomicBool>| {
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
                            started.store(true, std::sync::atomic::Ordering::Relaxed);
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

        let a_started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let b_started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let a_handle = reader(sub_a, stop.clone(), a_started.clone());
        let b_handle = reader(sub_b, stop.clone(), b_started.clone());

        // Writer: ≥1000 records, ~24 B each => ~6+ wraps on a 4 KiB ring.
        // yield_now between writes gives reader threads schedule time so they
        // can keep pace and meaningfully exercise the wrap-race code path.
        //
        // The writer must NOT stop before both readers have read something: the
        // slow-consumer `Overwritten` reset jumps to the live edge, so a reader
        // whose first try_read lands after the last write legitimately sees
        // zero records (observed as a CI flake on a contended 2-vCPU runner).
        // Keep the ring live until both readers have joined in, with a hard
        // time cap so a genuinely broken ring still terminates (and the final
        // assertion then reports the zero count).
        let writer = std::thread::spawn(move || {
            let cap = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut i = 0u64;
            loop {
                let payload = (i as u32).to_le_bytes();
                producer.write(1, 0, [0; 8], &payload).expect("write");
                i += 1;
                std::thread::yield_now();
                let both_started = a_started.load(std::sync::atomic::Ordering::Relaxed)
                    && b_started.load(std::sync::atomic::Ordering::Relaxed);
                if i >= 1000 && (both_started || std::time::Instant::now() >= cap) {
                    break;
                }
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

    /// Aggressive torn-read reproduction: a TINY ring, an unthrottled producer,
    /// and a deliberately slow reader guarantee the producer laps the consumer
    /// mid-read on nearly every wrap. A torn read must surface as `Overwritten`
    /// (the defined slow-consumer signal) — NEVER as a hard `BadCrc`/`Corrupt`.
    /// Without the post-read overwrite re-check this panics with "crc mismatch".
    #[test]
    fn overwrite_during_read_never_tears() {
        let tmp = NamedTempFile::new().unwrap();
        // 256 B ring, ~24 B records => a lap every ~10 records: constant races.
        let ring = BroadcastRing::create(tmp.path(), 256, 64).expect("create");
        let mut producer = ring.producer();
        let mut sub = ring.subscribe();

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_w = stop.clone();
        let writer = std::thread::spawn(move || {
            let mut i = 0u32;
            while !stop_w.load(std::sync::atomic::Ordering::Relaxed) {
                producer.write(1, 0, [0; 8], &i.to_le_bytes()).expect("write");
                i = i.wrapping_add(1);
            }
        });

        let mut buf = Vec::new();
        let mut reads = 0usize;
        let mut overwrites = 0usize;
        // Slow reader: many iterations, each doing a little work between reads so
        // the unthrottled producer laps it constantly.
        for _ in 0..200_000 {
            match sub.try_read(&mut buf) {
                Ok(Some(_)) => {
                    assert_eq!(buf.len(), 4, "torn payload leaked as a valid record");
                    reads += 1;
                }
                Ok(None) => std::thread::yield_now(),
                Err(RingError::Overwritten) => overwrites += 1,
                Err(e) => panic!("torn read leaked as a hard error: {e}"),
            }
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        writer.join().unwrap();
        // The point of the test is the overwrite path actually fired (races
        // happened) and NO hard error escaped.
        assert!(overwrites > 0, "test did not exercise the overwrite race");
        let _ = reads;
    }
}
