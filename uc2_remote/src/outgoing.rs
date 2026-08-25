// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `OutRing` — the single-producer / single-consumer byte ring the submitter
//! encodes frames into and the writer thread drains.
//!
//! # Why a byte ring and not a queue of `Vec`s
//!
//! The whole point of M13b is that a submit costs no syscall and no
//! allocation: `try_submit` encodes the frame straight into preallocated
//! bytes, and the writer hands **whatever is there** to one `write_all_bytes`
//! (flush-on-empty, no timer). A queue of buffers would reintroduce an
//! allocation per request and a gather per drain.
//!
//! # The safety invariant
//!
//! `ack <= send <= write`, and the producer only ever writes into
//! `[write, ack + capacity)`. So `[ack, write)` — every byte the writer may
//! still send or re-send — is untouched by the producer, which is why the two
//! threads need no lock. [`OutRing::release_to`] clamps to `send`;
//! [`OutRing::set_send_pos`] only ever moves forward. A frame MAY straddle the
//! wrap: [`OutRing::peek_upto`] then returns the contiguous head and the
//! writer comes back for the tail (two `write_all_bytes` once per lap, not per
//! frame).
//!
//! Task 5 gave this ring its real consumer: `link::Writer`, the writer
//! thread's role token, is the only holder of the CONSUMER methods. The
//! PRODUCER methods wait for task 6's `try_submit` and task 8's re-send path,
//! and carry a narrow per-item `allow` until then.

use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::frame::{encode_header_into, Header, HEADER_LEN};
use crate::park::WaitCell;

pub(crate) struct OutRing {
    /// The backing storage, owned via a raw pointer rather than `Box<[u8]>`
    /// or `UnsafeCell<Box<[u8]>>` — deliberately. Going through
    /// `Box`/`UnsafeCell::get()` and then indexing forces a reference (`&` or
    /// `&mut`) over the WHOLE allocation, which retags every byte in it
    /// (Stacked/Tree Borrows) and invalidates any narrower reference the
    /// other thread is concurrently holding into a different, disjoint part
    /// of the same buffer — e.g. the slice `peek_upto` hands the writer
    /// thread while the producer's next `push_frame` runs. Every access
    /// below instead builds its slice with `ptr::add`/`slice::from_raw_parts`
    /// over EXACTLY the bytes it touches, so no reference ever spans more
    /// than one thread's own range. `ptr` came from `Box::into_raw` in
    /// [`OutRing::new`] and is freed the same way in `Drop`.
    ptr: *mut u8,
    mask: usize,
    /// Producer-owned: the end of the encoded bytes.
    write: AtomicU64,
    /// Writer-owned: everything below this has been handed to the socket.
    send: AtomicU64,
    /// Producer-owned: reclaim frontier — bytes below it may be overwritten.
    ack: AtomicU64,
    wake: WaitCell,
}

// SAFETY: every byte of the buffer is accessed by exactly one thread at a
// time — the producer only touches `[write, ack + capacity)` and the
// consumer only reads `[send, write)`, and the invariant `ack <= send <=
// write` keeps those ranges disjoint. The cursors themselves are atomics
// with one writer each, and no reference is ever formed over more than the
// exact range a given call touches (see the `ptr` field doc above), so the
// two threads' accesses never alias.
unsafe impl Send for OutRing {}
unsafe impl Sync for OutRing {}

impl OutRing {
    pub(crate) fn new(capacity: usize) -> OutRing {
        let cap = capacity.max(4096).next_power_of_two();
        let boxed: Box<[u8]> = vec![0u8; cap].into_boxed_slice();
        // SAFETY: `Box::into_raw` hands back exactly the pointer `Drop`
        // reconstructs the box from, once, with the same length (`cap` via
        // `self.capacity()`).
        let ptr = Box::into_raw(boxed) as *mut u8;
        OutRing {
            ptr,
            mask: cap - 1,
            write: AtomicU64::new(0),
            send: AtomicU64::new(0),
            ack: AtomicU64::new(0),
            wake: WaitCell::new(),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.mask + 1
    }

    pub(crate) fn write_pos(&self) -> u64 {
        self.write.load(Ordering::Acquire)
    }

    pub(crate) fn send_pos(&self) -> u64 {
        self.send.load(Ordering::Acquire)
    }

    pub(crate) fn ack_pos(&self) -> u64 {
        self.ack.load(Ordering::Acquire)
    }

    pub(crate) fn wake(&self) -> &WaitCell {
        &self.wake
    }

    /// Bytes the producer may still write. Producer-only: `ack` has a single
    /// writer (this thread), so a `Relaxed` load of it is correct.
    pub(crate) fn free(&self) -> usize {
        let w = self.write.load(Ordering::Relaxed);
        let a = self.ack.load(Ordering::Relaxed);
        self.capacity() - (w - a) as usize
    }

    /// Encode one frame at the write cursor. `None` = no room (the caller
    /// reports `Backpressure`) or a frame larger than the whole ring (the
    /// caller reports `PayloadTooLarge`; the two are told apart by comparing
    /// the need against [`OutRing::capacity`]).
    ///
    /// PRODUCER ONLY.
    #[allow(dead_code, reason = "task 6's `try_submit` is the producer; this ring has no submitter yet")]
    pub(crate) fn push_frame(&self, h: Header, payload: &[u8]) -> Option<(u64, u32)> {
        let need = HEADER_LEN + payload.len();
        if need > self.capacity() || need > self.free() {
            return None;
        }
        let start = self.write.load(Ordering::Relaxed);
        let hdr = encode_header_into(h, payload.len());
        let mut pos = start;
        for src in [&hdr[..], payload] {
            let mut done = 0usize;
            while done < src.len() {
                let idx = (pos as usize) & self.mask;
                let n = src.len().min(self.capacity() - idx).min(src.len() - done);
                // SAFETY: the producer owns `[write, ack + capacity)`
                // (invariant at the top of this module) and `need <=
                // free()`, so this `n`-byte segment cannot touch a byte the
                // consumer may read. This forms no reference at all — a raw
                // pointer write — so it can't invalidate any reference the
                // consumer thread holds into a disjoint part of the buffer
                // (see the `ptr` field doc: never take `&`/`&mut` over the
                // whole allocation).
                unsafe {
                    ptr::copy_nonoverlapping(src[done..done + n].as_ptr(), self.ptr.add(idx), n);
                }
                done += n;
                pos += n as u64;
            }
        }
        self.write.store(start + need as u64, Ordering::Release);
        self.wake.signal();
        Some((start, need as u32))
    }

    /// Move the reclaim frontier up to `pos`, clamped to `send` (bytes that
    /// have not been written to the socket must never be overwritten) and
    /// never backwards. The clamp is intentional and load-bearing: a caller
    /// may legitimately ask to release past what has actually been sent
    /// (e.g. releasing up to `write_pos()` right after a push, before the
    /// writer has drained anything) and this must be a no-op rather than
    /// letting `ack` run ahead of `send`. PRODUCER ONLY.
    #[allow(dead_code, reason = "task 6's `try_submit` reclaims here once a seq is acked")]
    pub(crate) fn release_to(&self, pos: u64) {
        let send = self.send.load(Ordering::Acquire);
        let target = pos.min(send);
        debug_assert!(target <= send, "release_to: ack must never pass send");
        let cur = self.ack.load(Ordering::Relaxed);
        if target > cur {
            self.ack.store(target, Ordering::Release);
        }
    }

    /// The contiguous readable run starting at `send`, stopping at `limit`
    /// (the writer's flush limit) and at the ring's wrap. CONSUMER ONLY.
    pub(crate) fn peek_upto(&self, limit: u64) -> &[u8] {
        let s = self.send.load(Ordering::Relaxed);
        let end = limit.min(self.write.load(Ordering::Acquire));
        if end <= s {
            return &[];
        }
        let idx = (s as usize) & self.mask;
        let n = ((end - s) as usize).min(self.capacity() - idx);
        // SAFETY: `[send, write)` is written only by the producer, which
        // published it with a `Release` store to `write` that this
        // `Acquire` load synchronizes with; the producer will not touch it
        // again until it is both sent and released. The slice covers
        // EXACTLY `[idx, idx + n)` — never the whole allocation — so it
        // can't be invalidated by the producer's disjoint, equally narrow
        // raw-pointer writes elsewhere in the buffer (see the `ptr` field
        // doc).
        unsafe { slice::from_raw_parts(self.ptr.add(idx), n) }
    }

    /// CONSUMER ONLY: `n` bytes reached the socket.
    pub(crate) fn consume(&self, n: usize) {
        let s = self.send.load(Ordering::Relaxed);
        let new_send = s + n as u64;
        debug_assert!(
            new_send <= self.write_pos(),
            "consume: send must never pass write (consumed past what was ever written)"
        );
        self.send.store(new_send, Ordering::Release);
    }

    /// CONSUMER ONLY: used by the redial path, which re-sends the live window
    /// by hand and then jumps the cursor to the snapshot it worked against.
    /// Forward-only, so it can never expose bytes the producer has reclaimed.
    #[allow(dead_code, reason = "task 8's redial re-sends the live window by hand and then jumps the cursor")]
    pub(crate) fn set_send_pos(&self, pos: u64) {
        debug_assert!(
            pos <= self.write_pos(),
            "set_send_pos: send must never pass write"
        );
        let s = self.send.load(Ordering::Relaxed);
        if pos > s {
            self.send.store(pos, Ordering::Release);
        }
    }

    /// CONSUMER ONLY: copy one frame's bytes out (the RETRY / redial paths,
    /// which re-send a frame that is behind `send`). `out` is cleared first.
    #[allow(dead_code, reason = "task 8's RETRY / redial paths copy a frame out to re-send it")]
    pub(crate) fn copy_range(&self, off: u64, len: u32, out: &mut Vec<u8>) {
        debug_assert!(
            off >= self.ack_pos() && off + len as u64 <= self.write_pos(),
            "copy_range: [off, off+len) must lie within [ack, write) — the caller must only \
             re-send a frame whose slot is still live"
        );
        out.clear();
        out.reserve(len as usize);
        let mut done = 0usize;
        while done < len as usize {
            let idx = ((off + done as u64) as usize) & self.mask;
            let n = (len as usize - done).min(self.capacity() - idx);
            // SAFETY: the caller re-sends only frames whose slot is still
            // live (asserted above in debug builds), and a live slot's
            // bytes are at or above `ack` — never reclaimed, therefore
            // never rewritten by the producer. The slice covers EXACTLY
            // `[idx, idx + n)`, never the whole allocation (see the `ptr`
            // field doc).
            let chunk = unsafe { slice::from_raw_parts(self.ptr.add(idx), n) };
            out.extend_from_slice(chunk);
            done += n;
        }
    }
}

impl Drop for OutRing {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` was produced by `Box::into_raw` on a `[u8]` of
        // length `self.capacity()` in `new`; both threads that could have
        // held a borrow into it are gone by the time `drop` runs (this takes
        // `&mut self`), and this reconstructs the box exactly once (normal
        // `Drop` semantics — nothing else ever reads `self.ptr` after this).
        // `ptr::slice_from_raw_parts_mut` builds the fat pointer directly,
        // without going through `slice::from_raw_parts_mut` (which would
        // form a `&mut [u8]` reference over the whole allocation just to
        // hand it straight to `Box::from_raw` — exactly the whole-buffer
        // reference this module exists to avoid).
        unsafe {
            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(self.ptr, self.capacity())));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{decode_header, FrameType, PROTOCOL_VERSION};

    fn hdr(seq: u64) -> Header {
        Header {
            ty: FrameType::Submit,
            flags: 0,
            version: PROTOCOL_VERSION,
            client_id: 7,
            seq,
        }
    }

    #[test]
    fn a_pushed_frame_comes_back_out_byte_for_byte() {
        let r = OutRing::new(4096);
        let (off, len) = r.push_frame(hdr(1), b"abcd").expect("room");
        assert_eq!(off, 0);
        assert_eq!(len as usize, HEADER_LEN + 4);
        let chunk = r.peek_upto(r.write_pos());
        assert_eq!(chunk.len(), HEADER_LEN + 4);
        let (h, plen) = decode_header(chunk).expect("header");
        assert_eq!(h.seq, 1);
        assert_eq!(plen, 4);
        assert_eq!(&chunk[HEADER_LEN..], b"abcd");
    }

    #[test]
    fn peek_stops_at_the_wrap_and_the_tail_comes_next() {
        let r = OutRing::new(4096);
        let cap = r.capacity();
        // Fill to 32 bytes short of the wrap, draining as we go so `ack` keeps up.
        // The bound accounts for the frame size: without it, the loop's own
        // last iteration could push a frame that straddles the wrap itself,
        // draining only its head and leaving the tail's few bytes stuck
        // between `send` and `write` — which then gets silently swept into
        // the *next* peek as leftover, instead of the explicit push below
        // being the one that straddles.
        let payload = vec![0u8; 100 - HEADER_LEN];
        let frame_len = (HEADER_LEN + payload.len()) as u64;
        while r.write_pos() + frame_len <= (cap - 32) as u64 {
            r.push_frame(hdr(1), &payload).expect("room");
            let n = r.peek_upto(r.write_pos()).len();
            r.consume(n);
            r.release_to(r.write_pos());
        }
        let before = r.write_pos();
        let (off, len) = r.push_frame(hdr(2), &payload).expect("room");
        assert_eq!(off, before);
        // The frame straddles the wrap, so the first peek is the head only.
        let head = r.peek_upto(r.write_pos());
        assert!(head.len() < len as usize, "the peek must stop at the wrap: {}", head.len());
        assert_eq!(head.len(), cap - (off as usize & (cap - 1)));
        let n = head.len();
        r.consume(n);
        let tail = r.peek_upto(r.write_pos());
        assert_eq!(n + tail.len(), len as usize, "head + tail is the whole frame");
    }

    #[test]
    fn a_full_ring_refuses_a_push_and_recovers_after_a_release() {
        let r = OutRing::new(4096);
        let payload = vec![0xAAu8; 1000 - HEADER_LEN];
        let mut pushed = 0;
        while r.push_frame(hdr(1), &payload).is_some() {
            pushed += 1;
            assert!(pushed < 100, "the ring never filled");
        }
        assert!(pushed >= 4, "a 4 KiB ring must hold at least four 1 KiB frames");
        // Nothing sent yet: releasing is clamped to `send`, so it buys nothing.
        r.release_to(r.write_pos());
        assert!(r.push_frame(hdr(1), &payload).is_none(), "release must clamp to send_pos");
        // Send the whole ring, then release: room again.
        while r.send_pos() < r.write_pos() {
            let n = r.peek_upto(r.write_pos()).len();
            r.consume(n);
        }
        r.release_to(r.write_pos());
        assert!(r.push_frame(hdr(1), &payload).is_some(), "a released ring takes new frames");
    }

    #[test]
    fn copy_range_reassembles_a_wrapped_frame() {
        let r = OutRing::new(4096);
        let cap = r.capacity();
        let payload = vec![0xCDu8; 100 - HEADER_LEN];
        let frame_len = (HEADER_LEN + payload.len()) as u64;
        while r.write_pos() + frame_len <= (cap - 32) as u64 {
            r.push_frame(hdr(1), &payload).expect("room");
            let n = r.peek_upto(r.write_pos()).len();
            r.consume(n);
            r.release_to(r.write_pos());
        }
        let (off, len) = r.push_frame(hdr(9), &payload).expect("room");
        // This is only a real wrap test if the frame actually straddles the
        // ring's physical boundary -- assert it, so a future edit that moves
        // the boundary can't silently turn this into a same-lap copy.
        assert!(
            (off as usize & (cap - 1)) + len as usize > cap,
            "the frame must straddle the wrap for this test to mean anything"
        );
        let mut out = Vec::new();
        r.copy_range(off, len, &mut out);
        assert_eq!(out.len(), len as usize);
        let (h, plen) = decode_header(&out).expect("header");
        assert_eq!(h.seq, 9);
        assert_eq!(plen, payload.len());
        assert_eq!(&out[HEADER_LEN..], &payload[..]);
    }

    #[test]
    fn a_frame_larger_than_the_ring_is_refused_rather_than_wedging() {
        let r = OutRing::new(4096);
        let payload = vec![0u8; 8192];
        assert!(r.push_frame(hdr(1), &payload).is_none());
    }

    #[test]
    fn set_send_pos_never_moves_backwards() {
        let r = OutRing::new(4096);
        r.push_frame(hdr(1), b"xy").expect("room");
        let n = r.peek_upto(r.write_pos()).len();
        r.consume(n);
        let sent = r.send_pos();
        r.set_send_pos(0);
        assert_eq!(r.send_pos(), sent, "set_send_pos is forward-only");
    }

    /// The real two-thread exercise: a producer pushing on one thread while a
    /// consumer drains on another, concurrently, through the raw-pointer
    /// accesses in `push_frame`/`peek_upto`/`consume`. Payload sizes are
    /// irregular (a xorshift PRNG, not a fixed size) so frames straddle the
    /// ring's physical wrap over and over, at unaligned offsets — a fixed
    /// size only ever wraps at multiples of itself, which is exactly the gap
    /// in this file's earlier wrap tests. This is the test the Miri run in
    /// the fix report is over: it is what proves the raw-pointer rewrite (no
    /// reference ever spans more than one thread's own range) is actually
    /// alias-free under real concurrent access, not just single-threaded.
    ///
    /// `release_to` is PRODUCER ONLY (its own doc comment says so): the
    /// producer/main thread calls it, off its own `send_pos()` read. The
    /// first draft of this test had the consumer thread call it instead —
    /// which built a happens-before edge from the consumer's *own* prior
    /// read to `ack`'s Release store, but not from that read to the
    /// producer's *later* reuse write, since the producer's `free()` reads
    /// `ack` with `Relaxed` (sound only because `release_to` and `free()`
    /// are meant to share a thread). Miri's race detector caught the gap
    /// immediately — seeded in the fix report.
    #[test]
    fn two_threads_agree_on_every_byte_under_concurrent_push_and_drain() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;
        use std::thread;

        let ring = Arc::new(OutRing::new(4096));
        // Miri interprets every memory access, so keep this modest; it still
        // pushes ~24 KiB through a 4 KiB ring (~6 laps) with irregular sizes.
        const N: u64 = 600;

        let mut expected = Vec::new();
        let mut frames: Vec<(Header, Vec<u8>)> = Vec::with_capacity(N as usize);
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        for seq in 0..N {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let len = (state % 61) as usize; // 0..=60-byte payloads
            let payload: Vec<u8> = (0..len).map(|i| ((seq as usize + i) & 0xFF) as u8).collect();
            let h = hdr(seq);
            expected.extend_from_slice(&encode_header_into(h, payload.len()));
            expected.extend_from_slice(&payload);
            frames.push((h, payload));
        }

        let done = Arc::new(AtomicBool::new(false));
        let reader_ring = Arc::clone(&ring);
        let reader_done = Arc::clone(&done);
        let reader = thread::spawn(move || {
            let mut drained = Vec::new();
            loop {
                let w = reader_ring.write_pos();
                let chunk = reader_ring.peek_upto(w);
                if chunk.is_empty() {
                    if reader_done.load(Ordering::Acquire) && reader_ring.send_pos() == reader_ring.write_pos()
                    {
                        break;
                    }
                    thread::yield_now();
                    continue;
                }
                let n = chunk.len();
                drained.extend_from_slice(chunk);
                reader_ring.consume(n);
            }
            drained
        });

        for (h, payload) in frames {
            loop {
                // PRODUCER ONLY, called on the producer thread: reclaims
                // whatever the reader has drained so far, off this thread's
                // own `Acquire` read of `send` (paired with the reader's
                // `Release` store in `consume`).
                ring.release_to(ring.send_pos());
                if ring.push_frame(h, &payload).is_some() {
                    break;
                }
                thread::yield_now();
            }
        }
        // One more, so a producer that finished exactly as the ring filled
        // still leaves an accurate `ack` behind for the assertions below.
        ring.release_to(ring.send_pos());
        done.store(true, Ordering::Release);

        let drained = reader.join().unwrap();
        assert_eq!(drained, expected, "the reader's concatenated drain must equal the exact push order and bytes");
        assert!(ring.ack_pos() <= ring.send_pos(), "ack must never pass send");
        assert!(ring.send_pos() <= ring.write_pos(), "send must never pass write");
    }
}
