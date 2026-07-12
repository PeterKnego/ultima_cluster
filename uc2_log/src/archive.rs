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
use std::sync::Arc;

use uc_protocol::v2::frame::{self, FrameHeader, FRAME_TYPE_PADDING, HEADER_LEN};
use ultima_journal::{Durability, Journal, JournalConfig, JournalError};

use crate::buffer::LogBuffer;

// The replay-session path (uc2_net, M4) shares the archive's journal handle
// across the sender thread, so it must be `Send + Sync`. It is: `Journal` is
// `&self` throughout with internal locking (`Arc<Mutex<..>>`). Assert it at
// compile time so a regression here is loud rather than a distant trait error.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Journal>();
};

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("journal error: {0}")]
    Journal(#[from] JournalError),
    #[error("position {pos} is below the first archived block (first base {first_base})")]
    PositionPurged { pos: u64, first_base: u64 },
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
    journal: Arc<Journal>,
    cfg: ArchiveConfig,
    durable_pos: u64,
    next_block_seq: u64,
    /// The leadership term of the last non-padding frame observed by `do_work`'s
    /// header walk (M4, spec §6). A frame whose term differs from this is a
    /// term transition — recorded into `term_observations` as
    /// `(term, that frame's base position)`. Reset to 0 by `truncate_to` (see
    /// there): re-observing an already-known term downstream is harmless (the
    /// SM's `DataTermObserved` handler is idempotent for `term <= last`).
    last_observed_term: u32,
    /// Term transitions detected post-fsync, position-ordered by construction
    /// (blocks record the contiguous prefix in order). Drained by
    /// `take_term_observations`.
    term_observations: Vec<(u32, u64)>,
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
        let journal = Arc::new(Journal::open(jcfg)?);
        let (durable_pos, next_block_seq) = match journal.last_seq() {
            None => (0, 0),
            Some(last) => {
                let (meta, payload) = journal
                    .read(last)?
                    .expect("last_seq block must be readable");
                (meta + payload.len() as u64, last + 1)
            }
        };
        Ok(Self {
            journal,
            cfg,
            durable_pos,
            next_block_seq,
            last_observed_term: 0,
            term_observations: Vec::new(),
        })
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

    /// A shared handle to the underlying journal, for a replay source that
    /// outlives this borrow (the sender's NAK replay path, M4). Cheap clone —
    /// the journal itself is `&self` with internal locking.
    pub fn journal_arc(&self) -> Arc<Journal> {
        Arc::clone(&self.journal)
    }

    /// One duty cycle: record at most one block. Returns Ok(true) if work was
    /// done. The durable counter is advanced ONLY after Notifier::wait()
    /// returns (Consistent durability => post-fdatasync).
    pub fn do_work(&mut self, buffer: &LogBuffer) -> Result<bool, ArchiveError> {
        let slice = buffer.recordable_slice(self.durable_pos, self.cfg.max_block_bytes);
        if slice.is_empty() {
            return Ok(false);
        }
        let base = self.durable_pos;
        let notifier = self.journal.append(self.next_block_seq, self.durable_pos, slice)?;
        let len = slice.len() as u64;
        // If wait() errors here, next_block_seq is left unadvanced, so a
        // retry would hit NonMonotonicSeq — acceptable because a Consistent-
        // mode fsync failure poisons the journal fail-stop; the archive is
        // not retryable across it.
        notifier.wait()?;
        // Post-fsync term observation (M4): walk the (now durable) block's frame
        // headers for leadership-term transitions. Cheap — headers only, no
        // payload touch. `slice` is the contiguous prefix in stream order, so
        // the pushed observations are position-ordered by construction.
        self.observe_terms(slice, base);
        self.durable_pos += len;
        self.next_block_seq += 1;
        buffer.counters().durable.store_release(self.durable_pos);
        Ok(true)
    }

    /// Walk `block`'s frame headers (payload of the record whose base stream
    /// position is `base`), recording each leadership-term transition into
    /// `term_observations` as `(term, base position of the transition's first
    /// frame)`. PADDING frames are skipped (a wrap padding carries a stale term
    /// stamp and is not a real term boundary). Header-only, so it never reads
    /// payload bytes.
    fn observe_terms(&mut self, block: &[u8], base: u64) {
        let mut off = 0usize;
        while off + HEADER_LEN <= block.len() {
            let h = frame::read_header(&block[off..]);
            let aligned = frame::align_frame_len(h.length as usize);
            // Defense in depth: a sub-header or zero length would stall the walk
            // (archived blocks are frame-aligned + CRC-validated, so unreachable
            // on real input). Stop rather than spin/over-index.
            if (h.length as usize) < HEADER_LEN || off + aligned > block.len() {
                break;
            }
            if h.frame_type != FRAME_TYPE_PADDING
                && h.leadership_term_id != self.last_observed_term
            {
                self.term_observations.push((h.leadership_term_id, base + off as u64));
                self.last_observed_term = h.leadership_term_id;
            }
            off += aligned;
        }
    }

    /// Drain the term transitions detected since the last call (M4). Each entry
    /// is `(term, base position of that term's first frame in the recorded
    /// stream)` — the NewTerm frame's position for an election-opened term.
    /// Position-ordered. The consensus agent (Task 8) feeds these to the SM as
    /// `DataTermObserved`, which is idempotent for already-known terms.
    pub fn take_term_observations(&mut self) -> Vec<(u32, u64)> {
        std::mem::take(&mut self.term_observations)
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
    /// # First-block truncation (M4 carry #3)
    ///
    /// A cut at or inside the FIRST archived block cannot be expressed via
    /// `Journal::truncate_after` (it keeps every record with `seq <= keep_seq`
    /// and so can never remove the earliest block). This IS reachable in an
    /// early-life contested election: a node fsyncs its term-1 `NewTerm` frame,
    /// is partitioned before any datagram lands, and a peer wins term 2 at
    /// base 0; on heal, reconciliation yields `Truncate{to: 0}` → `pos ==
    /// first_base` → `lo == first`. Pre-fix this returned an `UnsupportedTruncation`
    /// fail-stop that recurred on every restart (a permanent single-node loss).
    ///
    /// It is now supported by clearing the whole journal (`Journal::truncate_all`)
    /// and re-seeding the surviving prefix `[first_base, pos)` as a fresh block
    /// seq 0. `pos == first_base` (drop everything) leaves the journal empty.
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
        // A cut at or inside the FIRST archived block (drop it whole when
        // pos == first_base, or shrink it when pos is inside it) is inexpressible
        // via `truncate_after`, which can never remove the earliest block. Clear
        // the whole journal instead and re-seed the surviving prefix as a fresh
        // block seq 0. `base == first_base` here (lo == first).
        if lo == first {
            let keep = (pos - base) as usize;
            debug_assert!(keep < bytes.len(), "first-block cut must be strictly inside the block");
            let prefix = bytes[..keep].to_vec();
            self.journal.truncate_all()?.wait()?;
            if pos > base {
                // Re-append the surviving prefix [base, pos) as block seq 0.
                self.journal.append(0, base, &prefix)?.wait()?;
                self.next_block_seq = 1;
            } else {
                // pos == base == first_base: keep nothing.
                self.next_block_seq = 0;
            }
            self.durable_pos = pos;
            self.last_observed_term = 0;
            self.term_observations.clear();
            return Ok(());
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
        // The recorded stream changed under us: reset the term-observation
        // cursor (M4). Simplest correct choice — drop any pending observations
        // and re-stamp terms as the truncated tail is re-recorded. Re-observing
        // an already-known term is harmless (the SM's `DataTermObserved` handler
        // is idempotent for `term <= last`); walking the map back to the exact
        // covering term is the caller's job if it ever needs it.
        self.last_observed_term = 0;
        self.term_observations.clear();
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

/// Binary-search the archived blocks for the one whose payload contains `pos`:
/// the block with the greatest base `<= pos`. Returns its `(seq, base)`, or
/// `None` when there are no blocks or `pos` is below the first archived base
/// (purged). Extracted verbatim from `replay_from`'s search; shared with the
/// sender's NAK replay path (M4). Every record it reads is in `[first, last]`,
/// so the reads are infallible except for a genuine journal I/O error.
pub fn find_block(journal: &Journal, pos: u64) -> Result<Option<(u64, u64)>, ArchiveError> {
    let (Some(first), Some(last)) = (journal.first_seq(), journal.last_seq()) else {
        return Ok(None);
    };
    let (first_meta, _) = journal.read(first)?.expect("first block readable");
    if pos < first_meta {
        return Ok(None);
    }
    // greatest block with base <= pos
    let (mut lo, mut hi) = (first, last);
    while lo < hi {
        let mid = lo + (hi - lo).div_ceil(2);
        let (meta, _) = journal.read(mid)?.expect("block readable");
        if meta <= pos { lo = mid } else { hi = mid - 1 }
    }
    let (base, _) = journal.read(lo)?.expect("block readable");
    Ok(Some((lo, base)))
}

impl Archive {
    /// Replay archived frames starting at `pos` (a frame start). Positions at
    /// or beyond the durable frontier yield an empty replay. Positions below
    /// the first archived block are gone (purged) -> error.
    pub fn replay_from(&self, pos: u64) -> Result<Replay<'_>, ArchiveError> {
        let exhausted = Replay {
            journal: self.journal.as_ref(),
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
        let Some(last) = self.journal.last_seq() else {
            return Ok(exhausted);
        };
        let Some((lo, _base)) = find_block(&self.journal, pos)? else {
            // pos < durable frontier but below the first archived block: purged.
            let first = self.journal.first_seq().expect("last_seq implies first_seq");
            let (first_base, _) = self.journal.read(first)?.expect("first block readable");
            return Err(ArchiveError::PositionPurged { pos, first_base });
        };
        Ok(Replay {
            journal: self.journal.as_ref(),
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
    use crate::cnc::{CncMeta, CncPage};
    use crate::region::Region;
    use std::sync::Arc;
    use uc_protocol::v2::frame::read_header;

    fn setup(cap: usize) -> (Arc<LogBuffer>, Arc<CncPage>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let cnc = CncPage::heap(&CncMeta {
            node_id: 0,
            instance_id: 0,
            app_id: "test".into(),
            buffer_bytes: cap as u64,
            max_payload: 256,
        });
        let b = Arc::new(LogBuffer::new(Region::heap_zeroed(cap), Arc::clone(&cnc), 256));
        (b, cnc, dir)
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
        assert_eq!(c.counters().durable.load_acquire(), 960);
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
        assert_eq!(c.counters().durable.load_acquire(), 96);
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
        c2.counters().prime(arch.recovered_position());
        let mut arch = arch;
        let mut a2 = Appender::new(Arc::clone(&b2), 2);
        // Appender::new picks up position from the primed counters
        assert_eq!(a2.position(), 480);
        let pos = a2.append(1, 100, &[0u8; 64]).unwrap();
        assert_eq!(pos, 480);
        assert!(arch.do_work(&b2).unwrap());
        assert_eq!(c2.counters().durable.load_acquire(), 576);
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
        // reopen-pin the PARTIAL-block path (the whole-block path is pinned
        // in the boundary test): the re-appended prefix's meta+len must
        // recover to exactly the truncation point across a restart
        drop(arch);
        let mut arch = Archive::open(ArchiveConfig {
            max_block_bytes: 200,
            ..test_cfg(dir.path())
        })
        .unwrap();
        assert_eq!(arch.recovered_position(), 480);
        // the archive keeps working after truncation: append + record resumes
        let (b2, c2, _) = setup(1 << 16);
        c2.counters().prime(480);
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

    /// Helper: an archive holding exactly ONE block of 4 frames (positions
    /// 0, 96, 192, 288; block [0, 384)), recorded + fsynced. Returns the frame
    /// start positions. The `TempDir` is intentionally leaked so the journal
    /// dir outlives the returned `Archive` (which keeps the journal open).
    fn archive_with_one_block() -> (Archive, Arc<LogBuffer>, Vec<u64>) {
        let (b, _c, dir) = setup(1 << 16);
        // Default 1 MiB block cap: all 4 frames (384 B) land in ONE block.
        let mut arch = Archive::open(test_cfg(dir.path())).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        let mut frames = Vec::new();
        for i in 0..4 {
            frames.push(a.append(1, i, &[i as u8; 64]).unwrap());
        }
        while arch.do_work(&b).unwrap() {}
        assert_eq!(arch.recovered_position(), 384, "4 frames = one 384 B block");
        assert_eq!(arch.blocks_recorded(), 1, "helper must record exactly one block");
        assert_eq!(frames, vec![0, 96, 192, 288]);
        std::mem::forget(dir); // keep the journal dir alive for `arch`'s lifetime
        (arch, b, frames)
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn truncate_inside_first_block_clears_and_reappends_prefix() {
        // one block [0, N) with several frames; cut at a frame boundary inside it
        let (mut archive, buffer, appender_frames) = archive_with_one_block(); // helper: appends 4 frames, records+fsyncs 1 block
        let cut = appender_frames[2]; // position of the 3rd frame = keep frames 0,1
        archive.truncate_to(cut).unwrap();
        assert_eq!(archive.recovered_position(), cut);
        let mut replay = archive.replay_from(0).unwrap();
        let mut n = 0; while replay.next().unwrap().is_some() { n += 1; }
        assert_eq!(n, 2, "only the surviving prefix frames replay");
        let _ = buffer;
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn truncate_to_zero_clears_everything() {
        let (mut archive, _b, _f) = archive_with_one_block();
        archive.truncate_to(0).unwrap(); // the contested-first-election case (M4 final review I-2)
        assert_eq!(archive.recovered_position(), 0);
        assert!(archive.replay_from(0).unwrap().next().unwrap().is_none());
    }

    /// First-block truncation must survive a reopen: after clearing everything
    /// (`truncate_to(0)`) the recovered frontier is 0 and the archive keeps
    /// working; after a partial first-block cut the re-seeded prefix recovers.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn first_block_truncation_survives_reopen() {
        let (b, _c, dir) = setup(1 << 16);
        {
            let mut arch = Archive::open(test_cfg(dir.path())).unwrap();
            let mut a = Appender::new(Arc::clone(&b), 1);
            for i in 0..4 {
                a.append(1, i, &[i as u8; 64]).unwrap();
            }
            while arch.do_work(&b).unwrap() {}
            assert_eq!(arch.recovered_position(), 384);
            // partial cut inside the first (only) block: keep [0, 192)
            arch.truncate_to(192).unwrap();
            assert_eq!(arch.recovered_position(), 192);
        }
        // reopen: the re-seeded prefix recovers to exactly the truncation point
        let arch = Archive::open(test_cfg(dir.path())).unwrap();
        assert_eq!(arch.recovered_position(), 192);
        let mut r = arch.replay_from(0).unwrap();
        let mut n = 0;
        while r.next().unwrap().is_some() {
            n += 1;
        }
        assert_eq!(n, 2, "surviving prefix frames recover across reopen");
    }

    // ----------------------------------------------- M4 term observation (T7)

    use uc_protocol::v2::frame::{write_header_except_length, FrameHeader, FRAME_TYPE_MESSAGE};

    /// A recorded block spanning a term change yields the transition at the
    /// EXACT base of the new term's first frame; draining empties the buffer.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn term_observations_capture_transitions_with_exact_base() {
        let (b, _c, dir) = setup(1 << 16);
        let mut arch = Archive::open(test_cfg(dir.path())).unwrap();
        // term 1 frames at 0, 96; term 2 frames at 192, 288 (one block, 384 B)
        let mut a1 = Appender::new(Arc::clone(&b), 1);
        a1.append(1, 0, &[0u8; 64]).unwrap();
        a1.append(1, 1, &[0u8; 64]).unwrap();
        let mut a2 = Appender::new(Arc::clone(&b), 2);
        a2.append(1, 2, &[0u8; 64]).unwrap();
        a2.append(1, 3, &[0u8; 64]).unwrap();
        assert!(arch.do_work(&b).unwrap());
        assert_eq!(arch.take_term_observations(), vec![(1, 0), (2, 192)]);
        // draining consumed them
        assert!(arch.take_term_observations().is_empty());
    }

    /// Truncation resets the observation cursor; re-recording the still-in-ring
    /// tail re-yields its term (idempotent downstream).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn truncate_resets_and_reobserves_terms() {
        let (b, _c, dir) = setup(1 << 16);
        let cfg = ArchiveConfig { max_block_bytes: 200, ..test_cfg(dir.path()) };
        let mut arch = Archive::open(cfg).unwrap();
        // block 0 = term 1 (frames 0, 96); block 1 = term 2 (frames 192, 288)
        let mut a1 = Appender::new(Arc::clone(&b), 1);
        for i in 0..2 {
            a1.append(1, i, &[0u8; 64]).unwrap();
        }
        let mut a2 = Appender::new(Arc::clone(&b), 2);
        for i in 2..4 {
            a2.append(1, i, &[0u8; 64]).unwrap();
        }
        while arch.do_work(&b).unwrap() {}
        assert_eq!(arch.take_term_observations(), vec![(1, 0), (2, 192)]);
        // drop block 1 (term 2); the reset clears the cursor + pending buffer
        arch.truncate_to(192).unwrap();
        assert!(arch.take_term_observations().is_empty());
        // term 2's frames are still in the ring [192, 384): re-recording them
        // re-yields (2, 192) because last_observed_term was reset to 0
        assert!(arch.do_work(&b).unwrap());
        assert_eq!(arch.take_term_observations(), vec![(2, 192)]);
    }

    /// A padding-only span yields no observation (padding carries a stale term
    /// stamp and is not a real term boundary); a following message frame at the
    /// same term still transitions from the initial 0.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn observe_terms_skips_padding_only_spans() {
        let (_b, _c, dir) = setup(1 << 16);
        let mut arch = Archive::open(test_cfg(dir.path())).unwrap();

        // header-only padding frame (32 B), term-stamped 5
        let mut pad = vec![0u8; HEADER_LEN];
        write_header_except_length(
            &mut pad,
            &FrameHeader {
                length: 0,
                frame_type: FRAME_TYPE_PADDING,
                flags: 0,
                leadership_term_id: 5,
                session_id: 0,
                correlation_id: 0,
            },
        );
        pad[..4].copy_from_slice(&(HEADER_LEN as u32).to_le_bytes());
        arch.observe_terms(&pad, 4096);
        assert!(arch.take_term_observations().is_empty(), "padding produced a spurious term");

        // a real message frame at term 5 now transitions from 0 -> 5 at base 0
        let mut msg = vec![0u8; frame::align_frame_len(HEADER_LEN + 64)];
        write_header_except_length(
            &mut msg,
            &FrameHeader {
                length: 0,
                frame_type: FRAME_TYPE_MESSAGE,
                flags: 0,
                leadership_term_id: 5,
                session_id: 0,
                correlation_id: 0,
            },
        );
        msg[..4].copy_from_slice(&((HEADER_LEN + 64) as u32).to_le_bytes());
        arch.observe_terms(&msg, 0);
        assert_eq!(arch.take_term_observations(), vec![(5, 0)]);
    }
}
