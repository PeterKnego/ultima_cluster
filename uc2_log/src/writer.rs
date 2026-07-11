// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Position-addressed writer (spec §4): the follower's single writer. The
//! receiver agent copies datagram frame-runs at their ring offset — blind,
//! idempotent plain stores. Visibility discipline is the same as the
//! leader's: readers bound themselves by an acquire-load of `append`, which
//! the RECEIVER advances (Release) only to the contiguous frontier after gap
//! tracking — so duplicated/reordered writes above the frontier are never
//! observable, and re-writes below it are rejected by the caller (Task 8
//! accept rule: run.position >= contiguous).

use std::sync::Arc;

use crate::buffer::LogBuffer;

pub struct PositionedWriter {
    buffer: Arc<LogBuffer>,
}

impl PositionedWriter {
    pub fn new(buffer: Arc<LogBuffer>) -> Self {
        Self { buffer }
    }

    /// Copy a frame-run at `position`'s ring offset. Returns false (drop the
    /// datagram) if the run is empty, would cross the wrap (the sender never
    /// packs across it — padding rule), or would land beyond
    /// `durable + capacity` (the follower-side overrun gate: never overwrite
    /// what the local archive hasn't recorded).
    pub fn write_run(&self, position: u64, bytes: &[u8]) -> bool {
        let b = &self.buffer;
        debug_assert_eq!(
            position % uc_protocol::v2::frame::FRAME_ALIGNMENT as u64,
            0,
            "runs start at frame boundaries"
        );
        let off = b.offset(position);
        if bytes.is_empty() || bytes.len() as u64 > b.capacity() - off as u64 {
            return false;
        }
        let durable = b.counters().durable.load_acquire();
        if position + bytes.len() as u64 > durable + b.capacity() {
            return false;
        }
        // SAFETY: [off, off+len) within capacity (wrap check above); bytes in
        // [append, durable+capacity) are writer-owned (single receiver per
        // buffer, the follower analog of the appender contract); visibility
        // via the receiver's later Release store of `append`.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), b.region().ptr_at(off), bytes.len());
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{Appender, LogBuffer, SliceRead};
    use crate::counters::LogCounters;
    use crate::region::Region;
    use std::sync::Arc;
    use uc_protocol::v2::frame::read_header;

    const CAP: u64 = 4096;

    fn buf() -> (Arc<LogBuffer>, Arc<LogCounters>) {
        let counters = Arc::new(LogCounters::new());
        let b = Arc::new(LogBuffer::new(
            Region::heap_zeroed(CAP as usize),
            Arc::clone(&counters),
            256,
        ));
        (b, counters)
    }

    /// End-to-end symmetry: leader appends, sender-style run read, follower
    /// write_run, follower's archive-style read sees identical bytes.
    #[test]
    fn leader_run_rewritten_on_follower_reads_back_identically() {
        let (leader, _lc) = buf();
        let (follower, fc) = buf();
        let mut a = Appender::new(Arc::clone(&leader), 7);
        for i in 0..4 {
            a.append(2, i, &[i as u8; 64]).unwrap();
        }
        let w = PositionedWriter::new(Arc::clone(&follower));
        let mut run = Vec::new();
        let mut pos = 0u64;
        while let SliceRead::Run(r) = leader.read_run_validated(pos, 200, &mut run) {
            assert!(w.write_run(pos, &run[..r.bytes]));
            pos += r.advance;
        }
        assert_eq!(pos, 4 * 96);
        // receiver-role: advance append after (simulated) gap tracking
        fc.append.store_release(pos);
        let s = follower.recordable_slice(0, 1 << 20);
        assert_eq!(s.len(), 384);
        assert_eq!(read_header(&s[96..]).correlation_id, 1);
        assert_eq!(&s[3 * 96 + 32..3 * 96 + 96], &[3u8; 64]);
        // idempotent duplicate rewrite: same bytes, still fine
        let mut run2 = Vec::new();
        if let SliceRead::Run(r) = leader.read_run_validated(0, 200, &mut run2) {
            assert!(w.write_run(0, &run2[..r.bytes]));
        }
        assert_eq!(follower.recordable_slice(0, 1 << 20).len(), 384);
    }

    #[test]
    fn write_run_rejects_wrap_cross_empty_and_overrun() {
        let (follower, fc) = buf();
        let w = PositionedWriter::new(Arc::clone(&follower));
        assert!(!w.write_run(0, &[]));
        // would cross the wrap: offset 4064 + 64 bytes > 4096
        assert!(!w.write_run(CAP - 32, &[0u8; 64]));
        // ends exactly at the wrap: fine
        assert!(w.write_run(CAP - 32, &[0u8; 32]));
        // overrun guard: durable = 0 -> nothing beyond position capacity
        assert!(!w.write_run(CAP, &[0u8; 32])); // 4096+32 > 0+4096
        fc.durable.store_release(96);
        assert!(w.write_run(CAP, &[0u8; 32])); // 4128 <= 96+4096
    }
}
