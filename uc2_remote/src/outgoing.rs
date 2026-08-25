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
//! This is task 2 of the M13b split-client build (design spec §3): the
//! writer thread and the submitter's `try_submit` that will drive this ring
//! land in later tasks, so until then the only caller is this module's own
//! test suite — same reasoning [`crate::park::WaitCell`] carried after task
//! 1, and lifted the same way once this ring gets its first real caller.

#![allow(dead_code)]

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::frame::{encode_header_into, Header, HEADER_LEN};
use crate::park::WaitCell;

pub(crate) struct OutRing {
    buf: UnsafeCell<Box<[u8]>>,
    mask: usize,
    /// Producer-owned: the end of the encoded bytes.
    write: AtomicU64,
    /// Writer-owned: everything below this has been handed to the socket.
    send: AtomicU64,
    /// Producer-owned: reclaim frontier — bytes below it may be overwritten.
    ack: AtomicU64,
    wake: WaitCell,
}

// SAFETY: every byte of `buf` is written by exactly one thread at a time —
// the producer only touches `[write, ack + capacity)` and the consumer only
// reads `[send, write)`, and the invariant `ack <= send <= write` keeps those
// ranges disjoint. The cursors themselves are atomics with one writer each.
unsafe impl Send for OutRing {}
unsafe impl Sync for OutRing {}

impl OutRing {
    pub(crate) fn new(capacity: usize) -> OutRing {
        let cap = capacity.max(4096).next_power_of_two();
        OutRing {
            buf: UnsafeCell::new(vec![0u8; cap].into_boxed_slice()),
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
    pub(crate) fn push_frame(&self, h: Header, payload: &[u8]) -> Option<(u64, u32)> {
        let need = HEADER_LEN + payload.len();
        if need > self.capacity() || need > self.free() {
            return None;
        }
        let start = self.write.load(Ordering::Relaxed);
        let hdr = encode_header_into(h, payload.len());
        // SAFETY: the producer owns `[write, ack + capacity)` (invariant at the
        // top of this module) and `need <= free()`, so these writes cannot
        // touch a byte the consumer may read.
        let buf = unsafe { &mut *self.buf.get() };
        let mut pos = start;
        for src in [&hdr[..], payload] {
            let mut done = 0usize;
            while done < src.len() {
                let idx = (pos as usize) & self.mask;
                let n = src.len().min(self.capacity() - idx).min(src.len() - done);
                buf[idx..idx + n].copy_from_slice(&src[done..done + n]);
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
    /// never backwards. PRODUCER ONLY.
    pub(crate) fn release_to(&self, pos: u64) {
        let target = pos.min(self.send.load(Ordering::Acquire));
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
        // again until it is both sent and released.
        let buf = unsafe { &*self.buf.get() };
        &buf[idx..idx + n]
    }

    /// CONSUMER ONLY: `n` bytes reached the socket.
    pub(crate) fn consume(&self, n: usize) {
        let s = self.send.load(Ordering::Relaxed);
        self.send.store(s + n as u64, Ordering::Release);
    }

    /// CONSUMER ONLY: used by the redial path, which re-sends the live window
    /// by hand and then jumps the cursor to the snapshot it worked against.
    /// Forward-only, so it can never expose bytes the producer has reclaimed.
    pub(crate) fn set_send_pos(&self, pos: u64) {
        let s = self.send.load(Ordering::Relaxed);
        if pos > s {
            self.send.store(pos, Ordering::Release);
        }
    }

    /// CONSUMER ONLY: copy one frame's bytes out (the RETRY / redial paths,
    /// which re-send a frame that is behind `send`). `out` is cleared first.
    pub(crate) fn copy_range(&self, off: u64, len: u32, out: &mut Vec<u8>) {
        out.clear();
        out.reserve(len as usize);
        // SAFETY: the caller re-sends only frames whose slot is still live,
        // and a live slot's bytes are at or above `ack` — never reclaimed,
        // therefore never rewritten by the producer.
        let buf = unsafe { &*self.buf.get() };
        let mut done = 0usize;
        while done < len as usize {
            let idx = ((off + done as u64) as usize) & self.mask;
            let n = (len as usize - done).min(self.capacity() - idx);
            out.extend_from_slice(&buf[idx..idx + n]);
            done += n;
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
}
