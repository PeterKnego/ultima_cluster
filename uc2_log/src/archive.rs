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

use ultima_journal::{Durability, Journal, JournalConfig, JournalError};

use crate::buffer::LogBuffer;

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
}
