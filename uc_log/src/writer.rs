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
        // …and the matching bound from BELOW: `[.., durable)` is in the journal
        // and the archive's own cursor walks it, so those bytes are immutable.
        // The SAFETY note below has always claimed `[append, durable+capacity)`
        // as writer-owned; only the upper half was enforced. A receiver whose
        // gap tracker lags the shared counter — after a leader stint its own
        // appends push `append`/`durable` far past its receive frontier —
        // otherwise accepts a new leader's DATA at a recorded position and
        // rewrites it under the archive (2026-08-03: `durable` found 32 B inside
        // a 64 B frame, 7 of 50 soak hits).
        if position < durable {
            return false;
        }
        // #78: publish how far this write reaches BEFORE the bytes land, so a
        // validated reader whose copy sees them fails its post-copy check.
        b.counters()
            .append
            .raise_reserve(position + bytes.len() as u64);
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
    use crate::cnc::{CncMeta, CncPage};
    use crate::region::Region;
    use std::sync::Arc;
    use uc_protocol::v2::frame::read_header;

    const CAP: u64 = 4096;

    fn buf() -> (Arc<LogBuffer>, Arc<CncPage>) {
        let cnc = CncPage::heap(&CncMeta {
            node_id: 0,
            instance_id: 0,
            app_id: "test".into(),
            buffer_bytes: CAP,
            max_payload: 256,
            services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
        });
        let b = Arc::new(LogBuffer::new(
            Region::heap_zeroed(CAP as usize),
            Arc::clone(&cnc),
            256,
        ));
        (b, cnc)
    }

    /// End-to-end symmetry: leader appends, sender-style run read, follower
    /// write_run, follower's archive-style read sees identical bytes.
    #[test]
    fn leader_run_rewritten_on_follower_reads_back_identically() {
        let (leader, _lc) = buf();
        let (follower, fc) = buf();
        let mut a = Appender::new(Arc::clone(&leader), 7, 0);
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
        fc.counters().append.store_release(pos);
        let s = follower.recordable_slice(0, 1 << 20).unwrap();
        assert_eq!(s.len(), 384);
        assert_eq!(read_header(&s[96..]).seq, 1);
        assert_eq!(&s[3 * 96 + 32..3 * 96 + 96], &[3u8; 64]);
        // idempotent duplicate rewrite: same bytes, still fine
        let mut run2 = Vec::new();
        if let SliceRead::Run(r) = leader.read_run_validated(0, 200, &mut run2) {
            assert!(w.write_run(0, &run2[..r.bytes]));
        }
        assert_eq!(follower.recordable_slice(0, 1 << 20).unwrap().len(), 384);
    }

    /// RECORDED BYTES ARE IMMUTABLE. `[.., durable)` has been written into the
    /// journal; the buffer copy must keep agreeing with it, and the archive's
    /// own cursor sits in there. The overrun gate above bounds writes from
    /// ABOVE (`durable + capacity`); nothing bounded them from below, even
    /// though the SAFETY comment on the copy already claims "bytes in
    /// `[append, durable+capacity)` are writer-owned".
    ///
    /// Field evidence (2026-08-03, 7 of 50 soak hits): `durable` found sitting
    /// exactly 32 B inside a 64 B frame, with `sent` marking that frame's true
    /// start — the archive had recorded a 32 B frame there (a NewTerm) and
    /// something replaced it with a 64 B data frame afterwards. The archive
    /// then fail-stops walking its own recorded region.
    /// #78: a follower's validated reader must not return bytes the receiver
    /// has already overwritten with the NEXT lap. The receiver writes a run
    /// BEFORE it publishes `append`, and may write anywhere in
    /// `[contiguous, durable + capacity)` — far past the `append + max_claim`
    /// margin the reader's seqlock allows for (that margin is the LEADER
    /// appender's in-flight bound). No concurrency is needed: write the run,
    /// then read; both of the reader's checks see the same `append`.
    ///
    /// Shape (CAP 4096, 96 B frames, max_claim 576): the follower holds the
    /// log up to `A` (> one lap), archived (`durable = A`). A reader at `F`
    /// lags `A - F = 3232` — inside `(CAP - run, CAP - max_claim]` — and the
    /// receiver accepts the next in-order run at `A` (len ≥ 1024), whose ring
    /// image covers `F`'s offset.
    #[test]
    fn a_follower_reader_never_returns_bytes_the_receiver_overwrote_ahead_of_append() {
        let (leader, lc) = buf();
        let (follower, fc) = buf();
        let mut a = Appender::new(Arc::clone(&leader), 7, 0);
        let w = PositionedWriter::new(Arc::clone(&follower));
        // Replicate leader → follower run by run, archiving as we go, so both
        // `durable` counters keep the appender's and the writer's gates open.
        let mut pos = 0u64;
        let mut i = 0u32;
        let ship = |a: &mut Appender, upto_frames: u32, i: &mut u32, pos: &mut u64| {
            // In chunks of 10 frames: the leader's own gate is
            // `durable + capacity`, so it must see each chunk archived.
            while *i < upto_frames {
                let chunk_end = (*i + 10).min(upto_frames);
                while *i < chunk_end {
                    a.append(2, *i, &[(*i % 251) as u8; 64]).unwrap();
                    *i += 1;
                }
                let mut run = Vec::new();
                while let SliceRead::Run(r) = leader.read_run_validated(*pos, 4096, &mut run) {
                    assert!(w.write_run(*pos, &run[..r.bytes]), "replicate at {pos}");
                    *pos += r.advance;
                    fc.counters().append.store_release(*pos);
                    fc.counters().durable.store_release(*pos);
                    lc.counters().durable.store_release(*pos);
                }
            }
        };
        // Lap 0 plus some of lap 1: 60 frames × 96 B = 5760 B (+ wrap padding).
        ship(&mut a, 60, &mut i, &mut pos);
        let big_a = pos;
        assert!(big_a > CAP, "the follower must be past one lap: {big_a}");
        // The reader: the first lap-0 frame start at least 3264 B behind
        // `append` (lap-0 frames sit on the 96 B grid; the wrap padding moves
        // lap 1 off it, so `A - k * 96` would land mid-frame).
        let f = (big_a - 3264).next_multiple_of(96);
        let lag = big_a - f;
        assert!(
            lag + 576 <= CAP,
            "the reader passes the seqlock pre-check: lag {lag}"
        );
        let mut before = Vec::new();
        let SliceRead::Run(r0) = follower.read_run_validated(f, 4096, &mut before) else {
            panic!("the reader's run at {f} must be readable before the overwrite");
        };
        let before = before[..r0.bytes].to_vec();

        // The leader appends the next frames; the receiver accepts the in-order
        // run at `A` and writes it — but has not yet published `append`.
        while i < 72 {
            a.append(2, i, &[0xEE; 64]).unwrap();
            i += 1;
        }
        let mut run = Vec::new();
        let SliceRead::Run(r) = leader.read_run_validated(big_a, 1408, &mut run) else {
            panic!("leader run at {big_a}");
        };
        assert!(
            r.bytes >= 1024,
            "a datagram-sized run, past max_claim: {}",
            r.bytes
        );
        assert!(
            w.write_run(big_a, &run[..r.bytes]),
            "the receiver accepts the run at A"
        );

        // The reader at F: Overrun is correct; a run is correct only if its
        // bytes are still the lap the reader is at.
        let mut after = Vec::new();
        match follower.read_run_validated(f, 4096, &mut after) {
            SliceRead::Overrun => {}
            SliceRead::Run(r1) => {
                let n = r1.bytes.min(before.len());
                assert_eq!(
                    &after[..n],
                    &before[..n],
                    "the reader at {f} returned the receiver's next-lap bytes as a valid run"
                );
            }
            other => panic!("unexpected {other:?}"),
        }

        // Liveness: a reader whose bytes the run does NOT reach still reads —
        // the reserve is a precise bound, not a blanket Overrun. Its limit
        // `from + CAP` must be at or above the run's end.
        let run_end = big_a + r.bytes as u64;
        let near = (run_end - CAP).next_multiple_of(96).max(f + 96);
        let near = if near >= 4032 { 4096 } else { near }; // lap-0 frames end at 4032
        let mut ok = Vec::new();
        assert!(
            matches!(
                follower.read_run_validated(near, 4096, &mut ok),
                SliceRead::Run(_)
            ),
            "a reader at {near} (limit {}) is past the run's reach ({run_end}) and must read",
            near + CAP
        );
    }

    /// #78: a prime discards the ring's content, so it resets the write
    /// reserve to the primed position — a stale reserve from before a
    /// restart or truncation would otherwise refuse reads until the stream
    /// passed it.
    #[test]
    fn prime_resets_the_write_reserve() {
        let (follower, fc) = buf();
        let w = PositionedWriter::new(Arc::clone(&follower));
        fc.counters().durable.store_release(0);
        assert!(w.write_run(2048, &[0u8; 96]));
        assert_eq!(fc.counters().append.reserve_acquire(), 2048 + 96);
        fc.counters().prime(512);
        assert_eq!(fc.counters().append.reserve_acquire(), 512);
    }

    #[test]
    fn write_run_refuses_to_rewrite_what_the_archive_already_recorded() {
        let (follower, fc) = buf();
        let w = PositionedWriter::new(Arc::clone(&follower));
        // Nothing recorded yet: the write lands.
        assert!(
            w.write_run(64, &[7u8; 64]),
            "control: writable while durable is 0"
        );
        // The archive records through 128.
        fc.counters().durable.store_release(128);
        assert!(
            !w.write_run(64, &[9u8; 64]),
            "rewrote bytes below `durable` — the archive has already journalled them"
        );
        // At/above the recorded frontier is still fine.
        assert!(
            w.write_run(128, &[9u8; 64]),
            "writes at the durable frontier must still land"
        );
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
        fc.counters().durable.store_release(96);
        assert!(w.write_run(CAP, &[0u8; 32])); // 4128 <= 96+4096
    }
}
