// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The archive agent (spec §4): polls the log buffer from the durable
//! position, block-writes whatever accumulated (≤ max_block_bytes,
//! frame-aligned) as ONE journal record per block — seq = block index,
//! meta = block base position — with one fdatasync per block
//! (Durability::Consistent), then advances the durable counter. The poll
//! batching IS the group commit: fsync frequency scales with block rate,
//! not message rate, and there is no linger anywhere.

use std::path::PathBuf;

use uc_protocol::v2::frame::{self, FrameHeader, FRAME_TYPE_PADDING, HEADER_LEN};
use ultima_journal::{Durability, Journal, JournalConfig, JournalError};

use crate::buffer::LogBuffer;

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("journal error: {0}")]
    Journal(#[from] JournalError),
    #[error("position {pos} is below the first archived block (first base {first_base})")]
    PositionPurged { pos: u64, first_base: u64 },
    #[error("cannot truncate to {pos}: would drop or shrink the first archived block")]
    UnsupportedTruncation { pos: u64 },
}

#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    pub dir: PathBuf,
    /// Soft cap per recorded block; a single frame larger than this still
    /// records as one block (blocks are frame-aligned, never split a frame).
    pub max_block_bytes: usize,
    pub segment_size_bytes: u64,
    pub preallocate_segments: bool,
}

impl ArchiveConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            max_block_bytes: 1024 * 1024,
            segment_size_bytes: 64 * 1024 * 1024,
            preallocate_segments: true,
        }
    }
}

pub struct Archive {
    journal: Journal,
    cfg: ArchiveConfig,
    durable_pos: u64,
    next_block_seq: u64,
}

impl Archive {
    /// Open the journal and recover the durable frontier: the last block's
    /// base position + length. Fresh dir -> position 0.
    pub fn open(cfg: ArchiveConfig) -> Result<Self, ArchiveError> {
        let jcfg = JournalConfig {
            segment_size_bytes: cfg.segment_size_bytes,
            durability: Durability::Consistent,
            preallocate_segments: cfg.preallocate_segments,
            ..JournalConfig::new(&cfg.dir)
        };
        let journal = Journal::open(jcfg)?;
        let (durable_pos, next_block_seq) = match journal.last_seq() {
            None => (0, 0),
            Some(last) => {
                let (meta, payload) = journal
                    .read(last)?
                    .expect("last_seq block must be readable");
                (meta + payload.len() as u64, last + 1)
            }
        };
        Ok(Self { journal, cfg, durable_pos, next_block_seq })
    }

    /// Where the log resumes after recovery (counters.prime(this)).
    #[inline]
    pub fn recovered_position(&self) -> u64 {
        self.durable_pos
    }

    #[inline]
    pub fn blocks_recorded(&self) -> u64 {
        self.next_block_seq
    }

    /// Test/replay access to the underlying journal.
    pub fn journal(&self) -> &Journal {
        &self.journal
    }

    /// One duty cycle: record at most one block. Returns Ok(true) if work was
    /// done. The durable counter is advanced ONLY after Notifier::wait()
    /// returns (Consistent durability => post-fdatasync).
    pub fn do_work(&mut self, buffer: &LogBuffer) -> Result<bool, ArchiveError> {
        let slice = buffer.recordable_slice(self.durable_pos, self.cfg.max_block_bytes);
        if slice.is_empty() {
            return Ok(false);
        }
        let notifier = self.journal.append(self.next_block_seq, self.durable_pos, slice)?;
        let len = slice.len() as u64;
        // If wait() errors here, next_block_seq is left unadvanced, so a
        // retry would hit NonMonotonicSeq — acceptable because a Consistent-
        // mode fsync failure poisons the journal fail-stop; the archive is
        // not retryable across it.
        notifier.wait()?;
        self.durable_pos += len;
        self.next_block_seq += 1;
        buffer.counters().durable.store_release(self.durable_pos);
        Ok(true)
    }

    /// Truncate the archived stream to end exactly at `pos` (spec §4, election
    /// reconciliation): drop whole blocks at/above `pos` and re-append the
    /// partial prefix of the block that contains it. `pos` must be a frame
    /// boundary within `(first archived base ..= durable frontier]`.
    ///
    /// `pos == durable frontier` is a no-op; `pos > frontier` errors (truncation
    /// never extends); `pos` below the first archived base is `PositionPurged`.
    ///
    /// This method touches only the journal and the archive's own cursors
    /// (`durable_pos` / `next_block_seq`). The CALLER (the consensus agent,
    /// Task 8) is responsible for resetting the buffer counters afterward
    /// (`counters.prime(pos)`) and re-deriving everything volatile.
    ///
    /// A truncation that would drop or shrink the FIRST archived block returns
    /// `UnsupportedTruncation`: `Journal::truncate_after(keep_seq)` keeps every
    /// record with `seq <= keep_seq` and so can never remove the earliest block.
    /// This is unreachable in M4 — reconciliation truncation points are term-map
    /// bases, and term 1's base is position 0 = block 0's start, which takes the
    /// whole-block or no-op path; a partial cut strictly inside the first block
    /// of the whole cluster history requires a divergence within a committed
    /// prefix, which the sim confirms never occurs.
    pub fn truncate_to(&mut self, pos: u64) -> Result<(), ArchiveError> {
        if pos == self.durable_pos {
            return Ok(());
        }
        if pos > self.durable_pos {
            return Err(ArchiveError::PositionPurged { pos, first_base: self.durable_pos });
        }
        // Alignment is only meaningful for a position we will actually cut at;
        // out-of-range values are rejected above and never reach here.
        debug_assert!(
            pos.is_multiple_of(frame::FRAME_ALIGNMENT as u64),
            "truncation positions are frame boundaries"
        );
        let (Some(first), Some(last)) = (self.journal.first_seq(), self.journal.last_seq()) else {
            return Err(ArchiveError::PositionPurged { pos, first_base: 0 });
        };
        let (first_base, _) = self.journal.read(first)?.expect("first block readable");
        if pos < first_base {
            return Err(ArchiveError::PositionPurged { pos, first_base });
        }
        // Binary search: greatest block with base <= pos (replay_from's shape).
        let (mut lo, mut hi) = (first, last);
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            let (meta, _) = self.journal.read(mid)?.expect("block readable");
            if meta <= pos {
                lo = mid
            } else {
                hi = mid - 1
            }
        }
        let (base, bytes) = self.journal.read(lo)?.expect("block readable");
        // Any cut that touches the first archived block (drop it whole when
        // pos == first_base, or shrink it when pos is inside it) is inexpressible
        // via `truncate_after`, which can never remove the earliest block. Guard
        // here so the `lo - 1` arithmetic below is always in range.
        if lo == first {
            return Err(ArchiveError::UnsupportedTruncation { pos });
        }
        if base == pos {
            // pos is exactly this block's start: drop it and everything after,
            // keeping blocks [first, lo).
            self.journal.truncate_after(lo - 1)?.wait()?;
            self.next_block_seq = lo;
        } else {
            // Partial block: keep [base, pos) — drop the block, re-append its
            // prefix at the same seq (a monotonic append since lo > lo - 1).
            let keep = (pos - base) as usize;
            debug_assert!(keep < bytes.len());
            self.journal.truncate_after(lo - 1)?.wait()?;
            self.journal.append(lo, base, &bytes[..keep])?.wait()?;
            self.next_block_seq = lo + 1;
        }
        self.durable_pos = pos;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayFrame {
    pub position: u64,
    pub header: FrameHeader,
    pub payload: Vec<u8>,
}

/// Sequential frame reader over archived blocks. Not a std::Iterator because
/// journal reads are fallible.
pub struct Replay<'a> {
    journal: &'a Journal,
    /// next block seq to read; > last_seq means exhausted
    seq: u64,
    last_seq: Option<u64>,
    block: Vec<u8>,
    block_base: u64,
    off: usize,
    /// skip frames below this position (mid-block replay starts)
    skip_below: u64,
}

impl Replay<'_> {
    #[allow(clippy::should_implement_trait)] // fallible journal reads: not std::Iterator
    pub fn next(&mut self) -> Result<Option<ReplayFrame>, ArchiveError> {
        loop {
            if self.off >= self.block.len() {
                let Some(last) = self.last_seq else { return Ok(None) };
                if self.seq > last {
                    return Ok(None);
                }
                let (meta, payload) = self
                    .journal
                    .read(self.seq)?
                    .expect("block in [first,last] must be readable");
                debug_assert!(
                    self.block.is_empty() || meta == self.block_base + self.block.len() as u64,
                    "archived blocks must be position-contiguous"
                );
                self.block_base = meta;
                self.block = payload;
                self.off = 0;
                self.seq += 1;
            }
            let hdr = frame::read_header(&self.block[self.off..]);
            let total = hdr.length as usize;
            let aligned = frame::align_frame_len(total);
            let position = self.block_base + self.off as u64;
            let payload_range = self.off + HEADER_LEN..self.off + total;
            self.off += aligned;
            if hdr.frame_type == FRAME_TYPE_PADDING || position < self.skip_below {
                continue;
            }
            return Ok(Some(ReplayFrame {
                position,
                header: hdr,
                payload: self.block[payload_range].to_vec(),
            }));
        }
    }
}

impl Archive {
    /// Replay archived frames starting at `pos` (a frame start). Positions at
    /// or beyond the durable frontier yield an empty replay. Positions below
    /// the first archived block are gone (purged) -> error.
    pub fn replay_from(&self, pos: u64) -> Result<Replay<'_>, ArchiveError> {
        let exhausted = Replay {
            journal: &self.journal,
            seq: 1,
            last_seq: None,
            block: Vec::new(),
            block_base: 0,
            off: 0,
            skip_below: 0,
        };
        if pos >= self.durable_pos {
            return Ok(exhausted);
        }
        let (Some(first), Some(last)) = (self.journal.first_seq(), self.journal.last_seq())
        else {
            return Ok(exhausted);
        };
        let (first_meta, _) = self.journal.read(first)?.expect("first block readable");
        if pos < first_meta {
            return Err(ArchiveError::PositionPurged { pos, first_base: first_meta });
        }
        // binary search: greatest block with base <= pos
        let (mut lo, mut hi) = (first, last);
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            let (meta, _) = self.journal.read(mid)?.expect("block readable");
            if meta <= pos { lo = mid } else { hi = mid - 1 }
        }
        Ok(Replay {
            journal: &self.journal,
            seq: lo,
            last_seq: Some(last),
            block: Vec::new(),
            block_base: 0,
            off: 0,
            skip_below: pos,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::{Appender, LogBuffer};
    use crate::counters::LogCounters;
    use crate::region::Region;
    use std::sync::Arc;
    use uc_protocol::v2::frame::read_header;

    fn setup(cap: usize) -> (Arc<LogBuffer>, Arc<LogCounters>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let counters = Arc::new(LogCounters::new());
        let b = Arc::new(LogBuffer::new(
            Region::heap_zeroed(cap),
            Arc::clone(&counters),
            256,
        ));
        (b, counters, dir)
    }

    /// Small segments so parallel test journals don't exhaust quota'd tmpfs;
    /// preallocation stays on (the production default path is covered by
    /// archive_config_defaults).
    fn test_cfg(dir: &std::path::Path) -> ArchiveConfig {
        ArchiveConfig { segment_size_bytes: 4 * 1024 * 1024, ..ArchiveConfig::new(dir) }
    }

    #[test]
    fn archive_config_defaults() {
        let cfg = ArchiveConfig::new("/nonexistent");
        assert_eq!(cfg.segment_size_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.max_block_bytes, 1024 * 1024);
        assert!(cfg.preallocate_segments);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn records_blocks_and_advances_durable() {
        let (b, c, dir) = setup(1 << 16);
        let mut arch = Archive::open(test_cfg(dir.path())).unwrap();
        assert_eq!(arch.recovered_position(), 0);

        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..10 {
            a.append(1, i, &[7u8; 64]).unwrap();
        }
        assert!(arch.do_work(&b).unwrap()); // one block: all 10 frames (960 B < 1 MiB)
        assert!(!arch.do_work(&b).unwrap()); // caught up
        assert_eq!(c.durable.load_acquire(), 960);
        assert_eq!(arch.blocks_recorded(), 1);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn blocks_split_at_max_and_meta_is_base_position() {
        let (b, _c, dir) = setup(1 << 16);
        let cfg = ArchiveConfig { max_block_bytes: 200, ..test_cfg(dir.path()) };
        let mut arch = Archive::open(cfg).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..4 {
            a.append(1, i, &[0u8; 64]).unwrap(); // 4 x 96 B
        }
        // 200-byte cap -> 2 frames per block (192 B), frame-aligned
        assert!(arch.do_work(&b).unwrap());
        assert!(arch.do_work(&b).unwrap());
        assert!(!arch.do_work(&b).unwrap());
        let j = arch.journal();
        let (meta0, blk0) = j.read(0).unwrap().unwrap();
        let (meta1, blk1) = j.read(1).unwrap().unwrap();
        assert_eq!((meta0, blk0.len()), (0, 192));
        assert_eq!((meta1, blk1.len()), (192, 192));
        // block content is raw frames: header parses, payload intact
        let h = read_header(&blk1);
        assert_eq!(h.correlation_id, 2);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn durable_only_advances_after_fsync_completion() {
        // Durability::Consistent -> Notifier::wait() returns post-fdatasync;
        // observable contract here: durable equals exactly what was recorded,
        // and journal.durable_seq() covers every block we advanced over.
        let (b, c, dir) = setup(1 << 16);
        let mut arch = Archive::open(test_cfg(dir.path())).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        a.append(1, 0, &[1u8; 64]).unwrap();
        arch.do_work(&b).unwrap();
        assert_eq!(c.durable.load_acquire(), 96);
        // block 0 must already be durable (wait returns immediately)
        arch.journal().wait_durable(0).unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn reopen_recovers_durable_frontier_and_appends_continue() {
        let (b, c, dir) = setup(1 << 16);
        {
            let mut arch = Archive::open(test_cfg(dir.path())).unwrap();
            let mut a = Appender::new(Arc::clone(&b), 1);
            for i in 0..5 {
                a.append(1, i, &[0u8; 64]).unwrap();
            }
            while arch.do_work(&b).unwrap() {}
        }
        // "restart": fresh archive over the same dir, fresh buffer/counters
        let arch = Archive::open(test_cfg(dir.path())).unwrap();
        assert_eq!(arch.recovered_position(), 480);
        let (b2, c2, _) = setup(1 << 16);
        c2.prime(arch.recovered_position());
        let mut arch = arch;
        let mut a2 = Appender::new(Arc::clone(&b2), 2);
        // Appender::new picks up position from the primed counters
        assert_eq!(a2.position(), 480);
        let pos = a2.append(1, 100, &[0u8; 64]).unwrap();
        assert_eq!(pos, 480);
        assert!(arch.do_work(&b2).unwrap());
        assert_eq!(c2.durable.load_acquire(), 576);
        let _ = (c, b);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn replay_from_yields_frames_and_skips_padding() {
        // small buffer so a wrap (padding frame) lands in the journal
        let (b, c, dir) = setup(4096);
        let mut arch = Archive::open(test_cfg(dir.path())).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        let mut n = 0u64;
        let mut positions = Vec::new();
        while a.position() < 5000 {
            match a.append(1, n, &[n as u8; 64]) {
                Ok(p) => {
                    positions.push((p, n));
                    n += 1;
                }
                Err(crate::buffer::AppendError::WouldOverrun) => {
                    arch.do_work(&b).unwrap();
                }
                Err(e) => panic!("{e}"),
            }
            let _ = &c;
        }
        while arch.do_work(&b).unwrap() {}

        // replay from the very beginning: every message frame, in order,
        // padding silently skipped
        let mut r = arch.replay_from(0).unwrap();
        for (p, corr) in &positions {
            let f = r.next().unwrap().expect("frame");
            assert_eq!(f.position, *p);
            assert_eq!(f.header.correlation_id, *corr);
            assert_eq!(f.payload, vec![*corr as u8; 64]);
        }
        assert!(r.next().unwrap().is_none());

        // replay from a mid-stream frame start (binary search across blocks)
        let (mid_pos, mid_corr) = positions[positions.len() / 2];
        let mut r = arch.replay_from(mid_pos).unwrap();
        let f = r.next().unwrap().expect("frame");
        assert_eq!((f.position, f.header.correlation_id), (mid_pos, mid_corr));

        // at/beyond durable: empty replay, not an error
        let mut r = arch.replay_from(arch.recovered_position()).unwrap();
        assert!(r.next().unwrap().is_none());
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn truncate_to_drops_tail_and_reappends_partial_block() {
        let (b, _c, dir) = setup(1 << 16);
        // small blocks so the stream spans several: 2 frames per block
        let cfg = ArchiveConfig { max_block_bytes: 200, ..test_cfg(dir.path()) };
        let mut arch = Archive::open(cfg).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..8 {
            a.append(1, i, &[i as u8; 64]).unwrap(); // 8 x 96 B = 768
        }
        while arch.do_work(&b).unwrap() {}
        assert_eq!(arch.recovered_position(), 768); // 4 blocks of 192
        // truncate mid-block-2: keep [0, 480) = blocks 0,1 whole + 96 of block 2
        arch.truncate_to(480).unwrap();
        assert_eq!(arch.recovered_position(), 480);
        // replay sees exactly frames 0..4 (positions 0..480), nothing beyond
        let mut r = arch.replay_from(0).unwrap();
        for i in 0..5u64 {
            let f = r.next().unwrap().expect("frame");
            assert_eq!(f.header.correlation_id, i);
        }
        assert!(r.next().unwrap().is_none());
        // the archive keeps working after truncation: append + record resumes
        let (b2, c2, _) = setup(1 << 16);
        c2.prime(480);
        let mut a2 = Appender::new(Arc::clone(&b2), 2);
        assert_eq!(a2.append(1, 100, &[9u8; 64]).unwrap(), 480);
        assert!(arch.do_work(&b2).unwrap());
        assert_eq!(arch.recovered_position(), 576);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn truncate_to_block_boundary_and_noop_and_errors() {
        let (b, _c, dir) = setup(1 << 16);
        let cfg = ArchiveConfig { max_block_bytes: 200, ..test_cfg(dir.path()) };
        let mut arch = Archive::open(cfg).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..4 {
            a.append(1, i, &[0u8; 64]).unwrap();
        }
        while arch.do_work(&b).unwrap() {}
        assert_eq!(arch.recovered_position(), 384);
        // exact block boundary: drop block 1 whole, no re-append needed
        arch.truncate_to(192).unwrap();
        assert_eq!(arch.recovered_position(), 192);
        // no-op at the frontier
        arch.truncate_to(192).unwrap();
        assert_eq!(arch.recovered_position(), 192);
        // beyond the frontier: error (truncation never extends)
        assert!(arch.truncate_to(500).is_err());
        // survives reopen: recovery sees the truncated frontier
        drop(arch);
        let arch = Archive::open(ArchiveConfig { max_block_bytes: 200, ..test_cfg(dir.path()) })
            .unwrap();
        assert_eq!(arch.recovered_position(), 192);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn truncate_to_partial_cut_in_first_block_is_unsupported() {
        // A cut that would shrink the FIRST archived block is inexpressible via
        // Journal::truncate_after (it can never remove the earliest block).
        // Unreachable in M4, but must fail cleanly rather than corrupt/underflow.
        let (b, _c, dir) = setup(1 << 16);
        let cfg = ArchiveConfig { max_block_bytes: 200, ..test_cfg(dir.path()) };
        let mut arch = Archive::open(cfg).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..2 {
            a.append(1, i, &[0u8; 64]).unwrap(); // 2 frames = one 192 B block
        }
        while arch.do_work(&b).unwrap() {}
        assert_eq!(arch.recovered_position(), 192);
        // pos 96 is a frame boundary strictly inside block 0 (base 0)
        assert!(matches!(
            arch.truncate_to(96),
            Err(ArchiveError::UnsupportedTruncation { pos: 96 })
        ));
        // frontier unchanged; the archive is still usable
        assert_eq!(arch.recovered_position(), 192);
    }
}
