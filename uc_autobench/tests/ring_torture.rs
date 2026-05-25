//! ring_torture — behavioral conformance suite for uc_protocol shmem rings.
//!
//! FROZEN. The auto-optimization loop never proposes changes to this file.
//! Every candidate ring implementation must pass every test here before its
//! microbench number is trusted. It validates any candidate ring rewrite via
//! the PUBLIC API only.
//!
//! Tests cover:
//!   1. SPSC FIFO + no-loss + no-torn-read (1M small + 100k large)
//!   2. MPSC per-producer FIFO + total-count + no-torn-read (4 producers)
//!   3. Broadcast all-subscribers-see (1 producer, 4 consumers)
//!   4. Wrap stress (tiny ring, many wraps)
//!   5. Backpressure (full ring + paused reader)
//!   6. Shutdown race (producer drops mid-write)
//!
//! Each payload carries a CRC32 trailer so a torn read fails the CRC check.
//!
//! ## Real ring API (uc_protocol::ring)
//!
//! * Construct: `SpscRing::create(path, capacity_bytes, max_msg_size)` /
//!   `MpscRing::create(...)` / `BroadcastRing::create(...)`. `capacity_bytes`
//!   must be a power of two; `max_msg_size` is the max *total* record size
//!   (FrameHeader 16 + payload + crc 4), not the payload size.
//! * Split: `ring.into_split() -> (Producer, Consumer)` for SPSC/MPSC.
//!   `MpscProducer` is `Clone` (clone after split to fan in). Broadcast uses
//!   `ring.producer()` + `ring.subscribe()` (each subscriber starts at the
//!   current head).
//! * Send: `producer.try_write(msg_type, flags, header_extra, payload)
//!   -> Result<(), RingError>`. "Full" => `Err(RingError::Full)`. SPSC/Broadcast
//!   producers take `&mut self`; MPSC producer takes `&self`.
//! * Recv: `consumer.try_read(&mut Vec<u8>) -> Result<Option<RecordHeader>, RingError>`.
//!   Fills the supplied `Vec` with the payload. "Empty" => `Ok(None)`.
//!   Broadcast may also return `Err(RingError::Overwritten)` for a lapped reader.
//! * Producer/Consumer halves are `Send` (the inner `Arc<…Inner>` is
//!   `unsafe impl Send + Sync`), so we drive them on separate `std::thread`s.

use uc_protocol::ring::{BroadcastRing, MpscRing, RingError, SpscRing};

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tempfile::TempDir;

// ---- framing constants (mirror uc_protocol::ring::common) ----
//
// We size rings against the on-wire record size, not the raw payload size.
const FRAME_HEADER_LEN: usize = 16;
const FRAME_TRAILER_LEN: usize = 4;

/// Total on-wire record size for a payload of `payload_size` bytes, rounded up
/// to the ring's 8-byte record alignment (matches `align_record_size`).
fn aligned_record_size(payload_size: usize) -> usize {
    let total = FRAME_HEADER_LEN + payload_size + FRAME_TRAILER_LEN;
    (total + 7) & !7
}

/// A `max_msg_size` that comfortably admits `payload_size`-byte payloads.
fn max_msg_size_for(payload_size: usize) -> u32 {
    (FRAME_HEADER_LEN + payload_size + FRAME_TRAILER_LEN) as u32
}

// ---- payload helpers: CRC32 trailer catches torn reads ----

/// Build a `size`-byte payload carrying `seq` in the first 8 bytes, a
/// seq-derived body, and a CRC32 of everything-but-the-trailer in the last
/// 4 bytes. `size` must be >= 12 (8 seq + 4 crc).
fn payload_with_crc(seq: u64, size: usize) -> Vec<u8> {
    assert!(size >= 12, "payload must hold seq(8) + crc(4)");
    let mut buf = vec![0u8; size];
    buf[..8].copy_from_slice(&seq.to_le_bytes());
    // Fill body (after seq, before crc) with a seq-derived pattern.
    let body_len = size - 4;
    for (i, b) in buf.iter_mut().enumerate().take(body_len).skip(8) {
        *b = (seq as u8).wrapping_add(i as u8) ^ 0xA5;
    }
    let crc = crc32fast::hash(&buf[..body_len]);
    buf[body_len..].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Verify the CRC trailer and return the embedded `seq`. A torn read (partial
/// bytes from two records) fails the CRC assert.
fn verify_payload(buf: &[u8]) -> u64 {
    assert!(buf.len() >= 12, "short payload: {}", buf.len());
    let body_len = buf.len() - 4;
    let expect_crc = crc32fast::hash(&buf[..body_len]);
    let got_crc = u32::from_le_bytes(buf[body_len..].try_into().unwrap());
    assert_eq!(got_crc, expect_crc, "torn read or corrupted payload");
    u64::from_le_bytes(buf[..8].try_into().unwrap())
}

// ===========================================================================
// Test 1: SPSC FIFO + no-loss + no-torn-read
// ===========================================================================

fn spsc_fifo_no_loss(n: u64, payload_size: usize, capacity_bytes: u64) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("spsc.ring");
    let ring = SpscRing::create(&path, capacity_bytes, max_msg_size_for(payload_size)).unwrap();
    let (mut producer, mut consumer) = ring.into_split();

    let prod = std::thread::spawn(move || {
        for seq in 0..n {
            let payload = payload_with_crc(seq, payload_size);
            loop {
                match producer.try_write(1, 0, [0; 8], &payload) {
                    Ok(()) => break,
                    Err(RingError::Full) => std::hint::spin_loop(),
                    Err(e) => panic!("unexpected write error: {e}"),
                }
            }
        }
    });

    let cons = std::thread::spawn(move || {
        let mut buf = Vec::with_capacity(payload_size);
        let mut expected: u64 = 0;
        while expected < n {
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => {
                    let got = verify_payload(&buf);
                    assert_eq!(got, expected, "SPSC FIFO violated at {expected}");
                    expected += 1;
                }
                Ok(None) => std::hint::spin_loop(),
                Err(e) => panic!("unexpected read error: {e}"),
            }
        }
    });

    prod.join().unwrap();
    cons.join().unwrap();
}

#[test]
fn spsc_fifo_no_loss_no_torn_read_small() {
    // 64B payload, 1M messages, 1 MiB ring.
    spsc_fifo_no_loss(1_000_000, 64, 1 << 20);
}

#[test]
fn spsc_fifo_no_loss_no_torn_read_large() {
    // 4096B payload, 100k messages, 8 MiB ring (fits ~2000 records in flight).
    spsc_fifo_no_loss(100_000, 4096, 1 << 23);
}

// ===========================================================================
// Test 2: MPSC per-producer FIFO + total-count + no-torn-read
// ===========================================================================

#[test]
fn mpsc_four_producers_no_loss_per_producer_fifo() {
    const N_PROD: u64 = 4;
    const PER_PROD: u64 = 250_000; // 1M total
    let payload_size = 64usize;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mpsc.ring");
    // 1 MiB ring forces wraps under 1M records of ~88B each.
    let ring = MpscRing::create(&path, 1 << 20, max_msg_size_for(payload_size)).unwrap();
    let (producer, mut consumer) = ring.into_split();

    let mut prod_handles = Vec::new();
    for pid in 0..N_PROD {
        let producer = producer.clone();
        prod_handles.push(std::thread::spawn(move || {
            for seq in 0..PER_PROD {
                // Embed producer id + per-producer seq so the consumer can
                // verify per-producer FIFO. payload[8..16] stays seq-derived;
                // we overwrite [16..24]=pid and [24..32]=seq, then re-CRC.
                let mut payload = payload_with_crc((pid << 40) | seq, payload_size);
                payload[16..24].copy_from_slice(&pid.to_le_bytes());
                payload[24..32].copy_from_slice(&seq.to_le_bytes());
                let body_len = payload_size - 4;
                let crc = crc32fast::hash(&payload[..body_len]);
                payload[body_len..].copy_from_slice(&crc.to_le_bytes());
                loop {
                    match producer.try_write(1, 0, [0; 8], &payload) {
                        Ok(()) => break,
                        Err(RingError::Full) => std::hint::spin_loop(),
                        Err(e) => panic!("unexpected write error: {e}"),
                    }
                }
            }
        }));
    }

    let cons = std::thread::spawn(move || {
        let mut buf = Vec::with_capacity(payload_size);
        let mut expected = vec![0u64; N_PROD as usize];
        let mut total: u64 = 0;
        let target = N_PROD * PER_PROD;
        while total < target {
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => {
                    verify_payload(&buf); // torn-read guard
                    let pid = u64::from_le_bytes(buf[16..24].try_into().unwrap()) as usize;
                    let seq = u64::from_le_bytes(buf[24..32].try_into().unwrap());
                    assert!(pid < N_PROD as usize, "bogus producer id {pid}");
                    assert_eq!(
                        seq, expected[pid],
                        "producer {pid} out-of-order: got {seq} expected {}",
                        expected[pid]
                    );
                    expected[pid] += 1;
                    total += 1;
                }
                Ok(None) => std::hint::spin_loop(),
                Err(e) => panic!("unexpected read error: {e}"),
            }
        }
        // Every producer delivered exactly PER_PROD records, in order.
        for (pid, &e) in expected.iter().enumerate() {
            assert_eq!(e, PER_PROD, "producer {pid} delivered {e} != {PER_PROD}");
        }
    });

    for h in prod_handles {
        h.join().unwrap();
    }
    cons.join().unwrap();
}

// ===========================================================================
// Test 3: Broadcast — all subscribers see the full in-order sequence
// ===========================================================================

#[test]
fn broadcast_all_subscribers_see_full_sequence() {
    const N_SUBS: usize = 4;
    const N_MSGS: u64 = 200_000;
    let payload_size = 64usize;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bcast.ring");
    // 1 MiB ring. Broadcast has no backpressure: a subscriber that falls a
    // full lap behind gets `Overwritten`. To assert "every subscriber sees the
    // full in-order sequence", the producer must not outrun the readers. We
    // gate the producer on the slowest subscriber's progress via a shared
    // atomic low-water mark, keeping the producer within the ring window.
    let ring = BroadcastRing::create(&path, 1 << 20, max_msg_size_for(payload_size)).unwrap();
    let producer = ring.producer();

    // Per-subscriber progress counters the producer reads to throttle itself.
    let progress: Vec<Arc<std::sync::atomic::AtomicU64>> =
        (0..N_SUBS).map(|_| Arc::new(0u64.into())).collect();

    let cons_handles: Vec<_> = (0..N_SUBS)
        .map(|i| {
            let mut sub = ring.subscribe();
            let prog = progress[i].clone();
            std::thread::spawn(move || {
                let mut buf = Vec::with_capacity(payload_size);
                let mut expected: u64 = 0;
                while expected < N_MSGS {
                    match sub.try_read(&mut buf) {
                        Ok(Some(_)) => {
                            let got = verify_payload(&buf);
                            assert_eq!(
                                got, expected,
                                "broadcast sub {i} out-of-order: got {got} expected {expected}"
                            );
                            expected += 1;
                            prog.store(expected, Ordering::Release);
                        }
                        Ok(None) => std::hint::spin_loop(),
                        Err(RingError::Overwritten) => {
                            panic!("broadcast sub {i} lapped at {expected}: producer outran reader")
                        }
                        Err(e) => panic!("unexpected read error: {e}"),
                    }
                }
            })
        })
        .collect();

    // Throttle the producer so it never laps the slowest subscriber. With
    // ~88B records and a 1 MiB ring there are ~11900 records of headroom; we
    // keep a conservative window so no reader is ever lapped.
    let window: u64 = 4096;
    let prod_progress = progress.clone();
    let prod = std::thread::spawn(move || {
        let mut producer = producer;
        for seq in 0..N_MSGS {
            // Wait until every subscriber is within `window` of this seq.
            loop {
                let slowest = prod_progress
                    .iter()
                    .map(|p| p.load(Ordering::Acquire))
                    .min()
                    .unwrap();
                if seq < slowest + window {
                    break;
                }
                std::hint::spin_loop();
            }
            let payload = payload_with_crc(seq, payload_size);
            producer
                .write(1, 0, [0; 8], &payload)
                .expect("broadcast write");
        }
    });

    prod.join().unwrap();
    for h in cons_handles {
        h.join().unwrap();
    }
}

// ===========================================================================
// Test 4: Wrap stress — tiny ring, many wraps
// ===========================================================================

#[test]
fn spsc_wrap_stress_many_wraps() {
    // 4 KiB ring, 32B payloads (aligned record ~56B) => ~73 records/generation.
    // 1M messages => ~13700 wraps.
    let payload_size = 32usize;
    let capacity_bytes: u64 = 4096;
    let n: u64 = 1_000_000;

    // Sanity: ring must hold at least one record.
    assert!(aligned_record_size(payload_size) < capacity_bytes as usize);

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("spsc_wrap.ring");
    let ring = SpscRing::create(&path, capacity_bytes, max_msg_size_for(payload_size)).unwrap();
    let (mut producer, mut consumer) = ring.into_split();

    let prod = std::thread::spawn(move || {
        for seq in 0..n {
            let payload = payload_with_crc(seq, payload_size);
            loop {
                match producer.try_write(1, 0, [0; 8], &payload) {
                    Ok(()) => break,
                    Err(RingError::Full) => std::hint::spin_loop(),
                    Err(e) => panic!("unexpected write error: {e}"),
                }
            }
        }
    });

    let cons = std::thread::spawn(move || {
        let mut buf = Vec::with_capacity(payload_size);
        let mut expected = 0u64;
        while expected < n {
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => {
                    let got = verify_payload(&buf);
                    assert_eq!(got, expected, "wrap-stress FIFO violated at {expected}");
                    expected += 1;
                }
                Ok(None) => std::hint::spin_loop(),
                Err(e) => panic!("unexpected read error: {e}"),
            }
        }
    });

    prod.join().unwrap();
    cons.join().unwrap();
}

// ===========================================================================
// Test 5: Backpressure — full ring + paused reader
// ===========================================================================

#[test]
fn spsc_backpressure_full_ring_paused_reader() {
    let payload_size = 64usize;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("spsc_bp.ring");
    let ring = SpscRing::create(&path, 4096, max_msg_size_for(payload_size)).unwrap();
    let (mut producer, mut consumer) = ring.into_split();

    // Reader stays paused until `go` is set, then drains everything sent.
    let go = Arc::new(AtomicBool::new(false));
    let go2 = go.clone();
    let cons = std::thread::spawn(move || {
        while !go2.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(5));
        }
        let mut buf = Vec::with_capacity(payload_size);
        let mut count = 0u64;
        let mut expected = 0u64;
        // Drain until idle (reader is now ahead of a no-longer-writing producer).
        let mut idle = 0;
        loop {
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => {
                    let got = verify_payload(&buf);
                    assert_eq!(got, expected, "backpressure-drain FIFO violated");
                    expected += 1;
                    count += 1;
                    idle = 0;
                }
                Ok(None) => {
                    idle += 1;
                    if idle > 50 {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => panic!("unexpected read error: {e}"),
            }
        }
        count
    });

    // Producer must signal Full (not UB, not infinite spin) once the ring
    // fills with the reader paused. Bound the attempt with a wall-clock guard
    // so a buggy "never full" ring fails the test rather than hanging forever.
    let start = Instant::now();
    let mut sent = 0u64;
    let mut hit_full = false;
    while start.elapsed() < Duration::from_secs(5) {
        let payload = payload_with_crc(sent, payload_size);
        match producer.try_write(1, 0, [0; 8], &payload) {
            Ok(()) => sent += 1,
            Err(RingError::Full) => {
                hit_full = true;
                break;
            }
            Err(e) => panic!("unexpected write error: {e}"),
        }
    }
    assert!(sent > 0, "producer never sent anything");
    assert!(
        hit_full,
        "ring never signalled Full with reader paused (sent {sent})"
    );

    // Unblock the reader; the rest of the data must drain in order. Keep
    // producing the remaining sequence so the reader sees a contiguous run.
    go.store(true, Ordering::Release);
    let mut next = sent;
    let total = sent + 5_000;
    while next < total {
        let payload = payload_with_crc(next, payload_size);
        match producer.try_write(1, 0, [0; 8], &payload) {
            Ok(()) => next += 1,
            Err(RingError::Full) => std::hint::spin_loop(),
            Err(e) => panic!("unexpected write error: {e}"),
        }
    }
    drop(producer);

    let drained = cons.join().unwrap();
    assert_eq!(drained, total, "expected to drain every record in order");
}

// ===========================================================================
// Test 6: Shutdown race — producer drops mid-write, consumer ends cleanly
// ===========================================================================

#[test]
fn spsc_shutdown_race_clean_end_no_torn_read() {
    let payload_size = 64usize;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("spsc_shut.ring");
    let ring = SpscRing::create(&path, 1 << 20, max_msg_size_for(payload_size)).unwrap();
    let (mut producer, mut consumer) = ring.into_split();

    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    let prod = std::thread::spawn(move || {
        let mut seq: u64 = 0;
        let mut last_sent: i64 = -1;
        while !stop2.load(Ordering::Acquire) {
            let payload = payload_with_crc(seq, payload_size);
            match producer.try_write(1, 0, [0; 8], &payload) {
                Ok(()) => {
                    last_sent = seq as i64;
                    seq += 1;
                }
                Err(RingError::Full) => std::hint::spin_loop(),
                Err(e) => panic!("unexpected write error: {e}"),
            }
        }
        // Producer drops here (end of closure) — clean shutdown.
        last_sent
    });

    let stop3 = stop.clone();
    let cons = std::thread::spawn(move || {
        let mut buf = Vec::with_capacity(payload_size);
        let mut last_seq: i64 = -1;
        let mut idle_after_stop = 0;
        loop {
            match consumer.try_read(&mut buf) {
                Ok(Some(_)) => {
                    let seq = verify_payload(&buf) as i64;
                    assert!(
                        seq > last_seq,
                        "FIFO violated near shutdown: got {seq} after {last_seq}"
                    );
                    last_seq = seq;
                    idle_after_stop = 0;
                }
                Ok(None) => {
                    if stop3.load(Ordering::Acquire) {
                        // Producer signalled stop; drain a little more then end.
                        idle_after_stop += 1;
                        if idle_after_stop > 50 {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(1));
                    } else {
                        std::hint::spin_loop();
                    }
                }
                Err(e) => panic!("unexpected read error: {e}"),
            }
        }
        last_seq
    });

    std::thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Release);
    let last_sent = prod.join().unwrap();
    let last_read = cons.join().unwrap();

    // The consumer must have observed a clean monotonic end: it read at least
    // something, and never read past what the producer actually sent.
    assert!(last_read >= 0, "consumer read nothing");
    assert!(
        last_read <= last_sent,
        "consumer read seq {last_read} but producer only sent up to {last_sent}"
    );
}
