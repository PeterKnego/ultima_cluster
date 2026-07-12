// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! [`LogFollower`] (spec §4 / M5): ONE cursor abstraction over the log buffer,
//! reused by the service apply agent, the output agent, and the reconstruction
//! replay path. It wraps [`LogBuffer::read_run_validated`] with a byte cursor,
//! a reusable scratch buffer (the single deliberate copy at the apply
//! boundary), and the target/padding/overrun handling every follower needs.
//!
//! Contract (`next_batch`):
//! * `cursor >= target` → [`Batch::CaughtUp`].
//! * a validated bulk read that comes back
//!   [`SliceRead::NotCommitted`](crate::buffer::SliceRead::NotCommitted) →
//!   [`Batch::CaughtUp`] (nothing new committed yet).
//! * [`SliceRead::Overrun`](crate::buffer::SliceRead::Overrun) →
//!   [`Batch::Overrun`] (the caller degrades to journal replay).
//! * [`SliceRead::Run`](crate::buffer::SliceRead::Run) →
//!   [`Batch::Frames`]: an iterator over the whole `MESSAGE`/`NEW_TERM` frames
//!   in the run. `PADDING` is never yielded — it is skipped and the cursor
//!   advances over its full span. A frame whose END exceeds `target` ends the
//!   batch there (the cursor stays at that frame's start); the guard matters
//!   because `read_run_validated` bounds itself by `append`, not by `commit`,
//!   so the run can legitimately contain frames past `target`.

use std::sync::Arc;

use uc_protocol::v2::frame::{
    self, FRAME_TYPE_PADDING, FrameHeader, HEADER_LEN, align_frame_len,
};

use crate::buffer::{LogBuffer, SliceRead};

/// Largest run copied per `next_batch` call (the apply-boundary copy is
/// bounded so a duty cycle stays a duty cycle).
const MAX_RUN_BYTES: usize = 64 * 1024;

/// A single-cursor follower over the log buffer. Not `Clone`: each cursor is a
/// distinct reader with its own scratch buffer.
pub struct LogFollower {
    buffer: Arc<LogBuffer>,
    /// The next byte position to read. `pub` so an owner (the apply agent) can
    /// publish it as `service_applied` after a batch.
    pub cursor: u64,
    /// Reusable copy target — the ONE deliberate copy out of the mapped ring.
    buf: Vec<u8>,
}

/// The outcome of a `next_batch` call.
pub enum Batch<'a> {
    /// Committed frames with END `<= target` are available; iterate them.
    Frames(FrameIter<'a>),
    /// Nothing more to apply up to `target` (cursor reached it, or nothing new
    /// is committed).
    CaughtUp,
    /// The bytes at `cursor` may have been overwritten (or exist only in the
    /// journal after a restart prime). The caller falls back to journal replay.
    Overrun,
}

impl LogFollower {
    pub fn new(buffer: Arc<LogBuffer>, cursor: u64) -> Self {
        Self { buffer, cursor, buf: Vec::new() }
    }

    /// Read the next run of committed frames whose END position is `<= target`.
    /// See the module doc for the full contract.
    pub fn next_batch(&mut self, target: u64) -> Batch<'_> {
        if self.cursor >= target {
            return Batch::CaughtUp;
        }
        let remaining = target - self.cursor;
        let max_bytes = (remaining as usize).min(MAX_RUN_BYTES);
        let run = match self.buffer.read_run_validated(self.cursor, max_bytes, &mut self.buf) {
            SliceRead::Run(r) => r,
            SliceRead::NotCommitted => return Batch::CaughtUp,
            SliceRead::Overrun => return Batch::Overrun,
        };
        Batch::Frames(FrameIter {
            buf: &self.buf,
            bytes: run.bytes,
            boff: 0,
            cursor: &mut self.cursor,
            target,
            done: false,
        })
    }
}

/// Walks the whole frames in a validated run, advancing the follower's cursor
/// as it goes. Yields `MESSAGE` and `NEW_TERM` frames with their absolute
/// position; skips `PADDING` (advancing the cursor over its full span) and
/// stops at the first frame whose END would exceed `target`.
pub struct FrameIter<'a> {
    buf: &'a [u8],
    /// Committed bytes copied into `buf` (`<= buf.len()`; a trailing padding
    /// frame contributes only its 32-byte header here).
    bytes: usize,
    /// Cursor into `buf`.
    boff: usize,
    /// The follower's stream cursor (advanced in step with `boff`, except over
    /// padding whose stream span exceeds its 32-byte buffer footprint).
    cursor: &'a mut u64,
    target: u64,
    /// Latched once the target guard trips so subsequent `next` calls are `None`.
    done: bool,
}

impl<'a> Iterator for FrameIter<'a> {
    /// `(absolute position, frame header, payload bytes)`.
    type Item = (u64, FrameHeader, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        // Detach the run slice from `&mut self` so the yielded payload borrows
        // the underlying `'a` allocation, not this iterator.
        let buf: &'a [u8] = self.buf;
        loop {
            if self.done || self.boff >= self.bytes {
                return None;
            }
            let hdr = frame::read_header(&buf[self.boff..]);
            let length = hdr.length as usize;
            let aligned = align_frame_len(length) as u64;
            // Target guard: a frame whose END exceeds `target` ends the batch;
            // the cursor stays at this frame's start (do NOT consume it).
            if *self.cursor + aligned > self.target {
                self.done = true;
                return None;
            }
            if hdr.frame_type == FRAME_TYPE_PADDING {
                // Never yielded: skip it, advancing the cursor over the full
                // padding span while the buffer footprint is only the header.
                *self.cursor += aligned;
                self.boff += HEADER_LEN;
                continue;
            }
            let pos = *self.cursor;
            let payload = &buf[self.boff + HEADER_LEN..self.boff + length];
            *self.cursor += aligned;
            self.boff += aligned as usize;
            return Some((pos, hdr, payload));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{Appender, LogBuffer};
    use crate::cnc::{CncMeta, CncPage};
    use crate::region::Region;
    use uc_protocol::v2::frame::{FRAME_TYPE_MESSAGE, FRAME_TYPE_NEW_TERM};

    const CAP: u64 = 4096;

    fn buf() -> (Arc<LogBuffer>, Arc<CncPage>) {
        let cnc = CncPage::heap(&CncMeta {
            node_id: 0,
            instance_id: 0,
            app_id: "test".into(),
            buffer_bytes: CAP,
            max_payload: 256,
        });
        let b = Arc::new(LogBuffer::new(Region::heap_zeroed(CAP as usize), Arc::clone(&cnc), 256));
        (b, cnc)
    }

    #[test]
    fn target_guard_stops_at_the_committed_frontier() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        // Three 96-byte frames at 0, 96, 192 (append = 288).
        for i in 0..3u64 {
            a.append(1, i, &[i as u8; 64]).unwrap();
        }
        let mut f = LogFollower::new(Arc::clone(&b), 0);

        // target = 96 (one frame boundary): only the frame at 0 is yielded,
        // even though the run read bounds itself by `append` (288) and thus
        // physically copied more. This is the load-bearing guard.
        match f.next_batch(96) {
            Batch::Frames(it) => {
                let v: Vec<(u64, u64)> = it.map(|(pos, h, _)| (pos, h.correlation_id)).collect();
                assert_eq!(v, vec![(0, 0)], "only the frame ending <= target");
            }
            _ => panic!("expected Frames"),
        }
        assert_eq!(f.cursor, 96, "cursor stopped exactly at the guarding frame's start");

        // Raising the target to the full frontier yields the remaining two.
        match f.next_batch(288) {
            Batch::Frames(it) => {
                let v: Vec<(u64, u64)> = it.map(|(pos, h, _)| (pos, h.correlation_id)).collect();
                assert_eq!(v, vec![(96, 1), (192, 2)]);
            }
            _ => panic!("expected Frames"),
        }
        assert_eq!(f.cursor, 288);

        // Caught up: cursor == target.
        assert!(matches!(f.next_batch(288), Batch::CaughtUp));
    }

    #[test]
    fn new_term_frame_is_yielded_but_carries_no_payload() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 5);
        a.append_new_term().unwrap(); // 32 B header-only frame at 0
        a.append(9, 1, b"data").unwrap(); // MESSAGE at 32
        let mut f = LogFollower::new(Arc::clone(&b), 0);
        match f.next_batch(a.position()) {
            Batch::Frames(it) => {
                let v: Vec<(u64, u8, usize)> =
                    it.map(|(pos, h, p)| (pos, h.frame_type, p.len())).collect();
                assert_eq!(
                    v,
                    vec![(0, FRAME_TYPE_NEW_TERM, 0), (32, FRAME_TYPE_MESSAGE, 4)],
                    "NEW_TERM is delivered (apply skips it by type); payload lengths exact"
                );
            }
            _ => panic!("expected Frames"),
        }
    }

    #[test]
    fn padding_is_skipped_and_cursor_advances_over_its_full_span() {
        let (b, c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..42u64 {
            a.append(1, i, &[0u8; 64]).unwrap(); // fill to 4032
        }
        c.counters().durable.store_release(4032); // let the wrap append through
        a.append(1, 99, &[0u8; 64]).unwrap(); // 64 B padding at 4032, frame at 4096

        let mut f = LogFollower::new(Arc::clone(&b), 4032);
        // The padding run yields NOTHING but advances the cursor over the full
        // 64-byte padding span (the post-wrap frame is a separate run).
        match f.next_batch(4192) {
            Batch::Frames(it) => assert_eq!(it.count(), 0, "padding is never yielded"),
            _ => panic!("expected Frames"),
        }
        assert_eq!(f.cursor, 4096, "cursor advanced over the full padding span, not just 32 B");

        // The post-wrap frame comes as its own run.
        match f.next_batch(4192) {
            Batch::Frames(it) => {
                let v: Vec<(u64, u64)> = it.map(|(pos, h, _)| (pos, h.correlation_id)).collect();
                assert_eq!(v, vec![(4096, 99)]);
            }
            _ => panic!("expected Frames"),
        }
        assert_eq!(f.cursor, 4192);
    }

    #[test]
    fn overrun_surfaces_after_a_prime_over_fresh_region() {
        // Node restart: counters primed to 2*CAP over a zeroed buffer file.
        // Positions below the prime live only in the journal — the follower
        // must surface Overrun (Task 9 wires the replay), not parse zeros.
        let (b, c) = buf();
        c.counters().prime(2 * CAP);
        let mut f = LogFollower::new(Arc::clone(&b), 2 * CAP - 64);
        assert!(matches!(f.next_batch(2 * CAP), Batch::Overrun));
    }

    #[test]
    fn caught_up_when_cursor_at_or_past_target() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1);
        a.append(1, 0, &[0u8; 64]).unwrap();
        let mut f = LogFollower::new(Arc::clone(&b), 96);
        assert!(matches!(f.next_batch(96), Batch::CaughtUp), "cursor == target");
        assert!(matches!(f.next_batch(32), Batch::CaughtUp), "cursor > target");
    }
}
