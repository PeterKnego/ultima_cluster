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

use uc_protocol::v2::frame::{self, FrameHeader, FRAME_TYPE_CONFIG, FRAME_TYPE_PADDING, HEADER_LEN};
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
    /// M6 Task 6: `adopt_floor(pos)` on a non-empty archive whose durable
    /// frontier is below `pos` — adopting would orphan the real prefix in
    /// `[durable, pos)`. Legal only for an empty archive (the learner-join case)
    /// or as a no-op when the frontier already covers `pos`.
    #[error("cannot adopt snapshot floor {pos}: archive already holds data up to {durable}")]
    AdoptFloorConflict { durable: u64, pos: u64 },
    /// Post-M7 follow-up: a recorded block whose frame headers are inconsistent
    /// with the block length (sub-header claimed length, or a frame overrunning
    /// the block). Archived blocks are journal-CRC-covered, so this is a
    /// recorder-side bug or on-disk corruption — surfaced as a diagnosable
    /// error (previously an unlabeled OOB slice panic in `Replay::next`).
    #[error("corrupt archived block seq {seq} (base {base}): frame at off {off} claims len {claimed_len}, block len {block_len}")]
    CorruptBlock { seq: u64, base: u64, off: usize, claimed_len: u32, block_len: usize },
    /// Post-M7 follow-up: `LogBuffer::recordable_slice`'s frame walk hit a
    /// length word inconsistent with the committed region — a recorder-side
    /// invariant break (see `RecordableCorrupt`'s doc comment). Fail-stop
    /// rather than record a malformed block.
    #[error("recorder-side corrupt frame walk: {0:?}")]
    RecorderCorrupt(crate::buffer::RecordableCorrupt),
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
    /// Lowest stream position still replayable from this archive: the base of
    /// the FIRST retained journal block, `== durable_pos` when the journal is
    /// empty. Tracked across `open` (recovered from `journal.first_seq()`'s
    /// block meta), `do_work` (first block seeds it), `truncate_to` (the
    /// first-block arm re-seeds it), and `purge_below` (recomputed after the
    /// journal drops leading segments). This is the real "floor" reported in
    /// every `PositionPurged`.
    first_base: u64,
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
    /// M7 (spec 2026-07-13): `(frame-END position, payload bytes)` for every
    /// `FRAME_TYPE_CONFIG` frame durably recorded since the last
    /// `take_config_observations` call — detected in the SAME header walk as
    /// `term_observations` (one scan, two outputs). Position-ordered by
    /// construction. `frame_end = base + off + align_frame_len(h.length)` is
    /// the effect point matching `ConfigRecord.position` semantics. Reset by
    /// `truncate_to` exactly like `term_observations`: any observation from a
    /// dropped tail is stale and must not be re-fed as if still durable.
    config_observations: Vec<(u64, Vec<u8>)>,
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
        // Recover the floor from the FIRST retained block's meta (a
        // purged-then-reopened journal keeps correct bounds); `durable_pos`
        // (== 0 here) when the journal is empty.
        let first_base = match journal.first_seq() {
            None => durable_pos,
            Some(first) => journal.read(first)?.expect("first block readable").0,
        };
        Ok(Self {
            journal,
            cfg,
            durable_pos,
            next_block_seq,
            first_base,
            last_observed_term: 0,
            term_observations: Vec::new(),
            config_observations: Vec::new(),
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

    /// Lowest position still replayable from this archive: base of the first
    /// retained block; `== recovered_position()` when the journal is empty.
    /// Task 4's purge driver and Task 5's gap guard both read this.
    #[inline]
    pub fn first_base(&self) -> u64 {
        self.first_base
    }

    /// Purge journal blocks strictly below the block COVERING `pos` (the
    /// covering block is retained — a replay at `pos` must succeed after).
    /// No-op if `pos <= first_base`, if no block covers `pos`, or if the
    /// covering block is already the first retained one. Returns the new
    /// `first_base`.
    ///
    /// Purge is SEGMENT-granular (`Journal::purge_before` drops whole
    /// non-active segment files), so the new floor is the first record meta of
    /// the first surviving segment — always `<= pos`. The active-segment guard
    /// in `purge_before` is extra slack: we never ask it to touch the covering
    /// block's segment because we purge strictly below `covering_seq`.
    pub fn purge_below(&mut self, pos: u64) -> Result<u64, ArchiveError> {
        if pos <= self.first_base {
            return Ok(self.first_base);
        }
        let Some((covering_seq, _base)) = find_block(&self.journal, pos)? else {
            // `pos` below the first block (can't happen given the guard above)
            // or the journal is empty: nothing to purge.
            return Ok(self.first_base);
        };
        let first_seq = self
            .journal
            .first_seq()
            .expect("find_block returned Some => non-empty journal");
        // Only purge when the covering block is strictly above the first
        // retained one. `covering_seq - 1 >= first_seq`, so `purge_before` drops
        // exactly the segments below the covering block's segment and never the
        // covering block itself (whose segment's last seq >= covering_seq).
        // (Guarding on `covering_seq > first_seq` rather than a bare
        // `saturating_sub(1)` avoids purge_before(0) dropping a lone first
        // segment whose last seq is 0.)
        if covering_seq > first_seq {
            self.journal.purge_before(covering_seq - 1)?;
            self.first_base = match self.journal.first_seq() {
                None => self.durable_pos,
                Some(first) => self.journal.read(first)?.expect("first block readable").0,
            };
        }
        Ok(self.first_base)
    }

    /// M6 Task 6: adopt `pos` as this archive's floor WITHOUT any bytes — the
    /// receiving side of a snapshot session (a fresh learner) has just installed
    /// the state below `pos` from the shipped snapshot, so the archive advances
    /// its durable frontier to `pos` and the next block records at base `pos`.
    ///
    /// Legal only for an EMPTY archive whose frontier is at/below `pos` (the
    /// learner-join case). A non-empty archive already covering `pos` treats it
    /// as a no-op (a mid-life follower ignores an AdoptFloor for state it has);
    /// a non-empty archive BELOW `pos` errors — adopting would orphan the real
    /// prefix in `[durable, pos)`. Returns the new `durable`/`first_base`.
    pub fn adopt_floor(&mut self, pos: u64) -> Result<u64, ArchiveError> {
        if self.journal.last_seq().is_some() {
            if self.durable_pos >= pos {
                return Ok(self.durable_pos); // already covered — no-op
            }
            return Err(ArchiveError::AdoptFloorConflict { durable: self.durable_pos, pos });
        }
        if pos < self.durable_pos {
            return Err(ArchiveError::AdoptFloorConflict { durable: self.durable_pos, pos });
        }
        self.durable_pos = pos;
        self.first_base = pos;
        Ok(pos)
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
        let slice = buffer.recordable_slice(self.durable_pos, self.cfg.max_block_bytes).map_err(
            |c| {
                // DIAGNOSTIC (nightly elle_partition fail-stop): dump the buffer
                // state before the caller turns this into a fail-stop panic.
                eprintln!(
                    "{}  archive: first_base={} next_block_seq={}",
                    buffer.corrupt_report(&c),
                    self.first_base,
                    self.next_block_seq,
                );
                ArchiveError::RecorderCorrupt(c)
            },
        )?;
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
        // The first block ever recorded (fresh journal, or one just cleared by a
        // first-block `truncate_to`) seeds the floor at its base.
        if self.next_block_seq == 0 {
            self.first_base = base;
        }
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
            // M7: CONFIG frames in the same header walk. `aligned` is already
            // the frame's end offset within `block` — `base + off + aligned`
            // is the frame-END stream position (the adoption effect point,
            // matching `ConfigRecord.position`).
            if h.frame_type == FRAME_TYPE_CONFIG {
                let payload_start = off + HEADER_LEN;
                let payload_end = off + h.length as usize;
                self.config_observations.push((
                    base + off as u64 + aligned as u64,
                    block[payload_start..payload_end].to_vec(),
                ));
            }
            off += aligned;
        }
    }

    /// Drain the CONFIG-frame observations detected since the last call (M7):
    /// `(frame-END position, payload bytes)`, position-ordered. Fed by the
    /// consensus agent as `Event::ConfigObserved` — the follower / boot-recovery
    /// half of config adoption (the leader's own append path feeds itself
    /// directly at append time; this is how everyone ELSE learns of it once it
    /// is durable).
    pub fn take_config_observations(&mut self) -> Vec<(u64, Vec<u8>)> {
        std::mem::take(&mut self.config_observations)
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
            return Err(ArchiveError::PositionPurged { pos, first_base: self.first_base });
        }
        // Alignment is only meaningful for a position we will actually cut at;
        // out-of-range values are rejected above and never reach here.
        debug_assert!(
            pos.is_multiple_of(frame::FRAME_ALIGNMENT as u64),
            "truncation positions are frame boundaries"
        );
        let (Some(first), Some(last)) = (self.journal.first_seq(), self.journal.last_seq()) else {
            return Err(ArchiveError::PositionPurged { pos, first_base: self.first_base });
        };
        let (first_base, _) = self.journal.read(first)?.expect("first block readable");
        if pos < first_base {
            return Err(ArchiveError::PositionPurged { pos, first_base: self.first_base });
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
            // The first block was rewritten (or dropped): the floor is now the
            // re-seeded prefix's base, which equals `base` (== old first_base);
            // when `pos == base` the journal is empty and the floor is `pos`
            // (== base). Either way the surviving floor is `base`.
            self.first_base = base;
            self.last_observed_term = 0;
            self.term_observations.clear();
            self.config_observations.clear();
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
        self.config_observations.clear();
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

impl std::fmt::Debug for Replay<'_> {
    // `Journal` is not `Debug`; surface the replay's cursor state instead (a
    // `Result<Replay, _>` is `{:?}`-printed by tests when an unexpected `Ok`
    // arrives where `PositionPurged` was asserted).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replay")
            .field("seq", &self.seq)
            .field("last_seq", &self.last_seq)
            .field("block_base", &self.block_base)
            .field("off", &self.off)
            .field("skip_below", &self.skip_below)
            .finish_non_exhaustive()
    }
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
            // Defense in depth (mirrors `observe_terms`'s walk guard): a
            // sub-header remainder or a frame overrunning the block would
            // previously OOB-panic at the header read / payload slice below.
            if self.block.len() - self.off < HEADER_LEN {
                return Err(ArchiveError::CorruptBlock {
                    seq: self.seq.saturating_sub(1),
                    base: self.block_base,
                    off: self.off,
                    claimed_len: 0,
                    block_len: self.block.len(),
                });
            }
            let hdr = frame::read_header(&self.block[self.off..]);
            let total = hdr.length as usize;
            let aligned = frame::align_frame_len(total);
            if total < HEADER_LEN || self.off + aligned > self.block.len() {
                return Err(ArchiveError::CorruptBlock {
                    seq: self.seq.saturating_sub(1),
                    base: self.block_base,
                    off: self.off,
                    claimed_len: hdr.length,
                    block_len: self.block.len(),
                });
            }
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
/// sender's NAK replay path (M4).
///
/// PURGE-RACE TOLERANCE (post-M7 t3b): this is called LOCK-FREE by the sender's
/// `serve_nak_from_journal` (it holds no purge lock), so a concurrent
/// `purge_below` can drop the segment backing `first`/`mid`/`lo` in the window
/// between the `first_seq()`/`last_seq()` snapshot and any read below — a read
/// of a since-purged seq then returns `Ok(None)`. Treat that as "the search
/// target is now below the floor" (`Ok(None)`) rather than panicking on an
/// `expect`: every lock-free caller already handles below-floor as "purged, not
/// servable" (`serve_nak_from_journal` via `.ok().flatten()` -> break;
/// `replay_from`/`replay_journal_from` -> `PositionPurged`/`Ok(None)`). A
/// genuine journal I/O error still propagates via `?`. Absent a race (the
/// boot-time single-threaded callers) no read here ever returns `None`, so the
/// behavior is identical; only the raced case now degrades gracefully instead
/// of crashing the sender agent. Exercised by `tests/archive_stress.rs` arm D.
pub fn find_block(journal: &Journal, pos: u64) -> Result<Option<(u64, u64)>, ArchiveError> {
    let (Some(first), Some(last)) = (journal.first_seq(), journal.last_seq()) else {
        return Ok(None);
    };
    let Some((first_meta, _)) = journal.read(first)? else {
        return Ok(None); // `first` purged out from under us: below the floor now
    };
    if pos < first_meta {
        return Ok(None);
    }
    // greatest block with base <= pos
    let (mut lo, mut hi) = (first, last);
    while lo < hi {
        let mid = lo + (hi - lo).div_ceil(2);
        let Some((meta, _)) = journal.read(mid)? else {
            return Ok(None); // purged mid-search
        };
        if meta <= pos { lo = mid } else { hi = mid - 1 }
    }
    let Some((base, _)) = journal.read(lo)? else {
        return Ok(None); // purged mid-search
    };
    Ok(Some((lo, base)))
}

/// Replay archived frames starting at `pos` over a bare `&Journal` — the
/// shared-handle counterpart of [`Archive::replay_from`] for callers holding a
/// [`journal_arc`](Archive::journal_arc) clone WITHOUT the `Archive` (the
/// sender-thread shape, M4: journal reads racing the archive agent's
/// appends/fsyncs, which `Journal`'s internal locking makes safe — blocks
/// appended concurrently land strictly above the `last_seq` snapshot taken
/// here and are never read).
///
/// # Return contract — the `Some(exhausted)` / `None` split (T2)
///
/// The two return shapes are the shared-handle mirror of `Archive::replay_from`'s
/// exhausted-vs-`PositionPurged` split, NOT an accidental asymmetry:
///
/// - `Ok(Some(replay))` — a drainable replay. This covers both a `pos` inside
///   the retained range (yields its frames) AND a `pos` at/beyond the durable
///   frontier or on an EMPTY journal (a constructible replay that drains to
///   nothing — the caller is CAUGHT UP, not missing data). `replay_from`
///   returns `Ok(exhausted)` for these same two cases.
/// - `Ok(None)` — `pos` is below the first archived block: the covering bytes
///   are PURGED and gone (the caller must upgrade to a snapshot session). This
///   is where `replay_from` returns `Err(PositionPurged)`; the free function has
///   no `Archive` floor bookkeeping to report, so it collapses that to `None`.
///
/// So empty-journal → `Some(exhausted)` (caught up to nothing) and below-floor →
/// `None` (purged) are deliberately different: "caught up" and "data gone" are
/// distinct outcomes a caller must handle differently, exactly as `replay_from`
/// distinguishes them.
///
/// LIFETIME/RACE CONTRACT (as `Archive::replay_from`): `pos` is a frame start,
/// and the blocks in the returned replay's `[seq, last_seq]` snapshot must not
/// be purged/truncated/rewritten while it is drained — `Replay::next` expects
/// every block in its range readable. Concurrent APPENDS are fine.
///
/// `#[doc(hidden)]`: this is test-support / a future-adoption seam (the
/// sender's deep-NAK path currently has its own `serve_nak_from_journal`
/// replay); it is exercised by `tests/archive_stress.rs` and the unit test
/// below, but is not part of the crate's stable surface.
#[doc(hidden)]
pub fn replay_journal_from(journal: &Journal, pos: u64) -> Result<Option<Replay<'_>>, ArchiveError> {
    let Some(last) = journal.last_seq() else {
        // Empty journal: an exhausted replay (mirrors replay_from's shape).
        return Ok(Some(Replay {
            journal,
            seq: 1,
            last_seq: None,
            block: Vec::new(),
            block_base: 0,
            off: 0,
            skip_below: 0,
        }));
    };
    let Some((lo, _base)) = find_block(journal, pos)? else {
        return Ok(None); // below the first archived block (purged)
    };
    Ok(Some(Replay {
        journal,
        seq: lo,
        last_seq: Some(last),
        block: Vec::new(),
        block_base: 0,
        off: 0,
        skip_below: pos,
    }))
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
            // Report the tracked floor (the first retained block's base).
            return Err(ArchiveError::PositionPurged { pos, first_base: self.first_base });
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

    /// REPRODUCTION of the nightly `elle_partition` archive fail-stop
    /// (`RecorderCorrupt { end: 0, claimed_len: <garbage> }`).
    ///
    /// `Action::BecomeLeader` (uc2_node `node.rs`) collapses the volatile tail
    /// with `LogCounters::prime(base)` on the CONSENSUS thread, where
    /// `base = ElectionSm::durable` — a value sampled in an EARLIER duty cycle
    /// (`do_work` step 2) than the vote drain that produced the action (step 1).
    /// The ARCHIVE agent runs concurrently and may have recorded another block
    /// in between, leaving its private `durable_pos` STRICTLY ABOVE `base`.
    /// Nothing resets it: unlike `ArchiveCmd::Truncate`, this prime does not go
    /// through the archive at all.
    ///
    /// The new leader then rewrites the buffer from `base` with a DIFFERENT
    /// frame layout, so the archive's cursor is left mid-frame — it reads a
    /// payload byte as a length word. This test drives exactly that sequence.
    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn become_leader_collapse_below_archive_cursor_corrupts_the_walk() {
        let (b, c, dir) = setup(1 << 16);
        let mut arch = Archive::open(test_cfg(dir.path())).unwrap();

        // Old leader's stream: 96-byte frames (32 header + 64 payload).
        let mut a = Appender::new(Arc::clone(&b), 1);
        for n in 0..5u64 {
            a.append(1, n, &[0xAB; 64]).unwrap();
        }
        assert_eq!(c.counters().append.load_acquire(), 480);

        // The archive records the whole stream: its cursor is now 480.
        while arch.do_work(&b).unwrap() {}
        assert_eq!(arch.recovered_position(), 480);

        // BecomeLeader with a STALE `base`: consensus sampled `durable` one
        // block ago (384), before the archive recorded the last frame (480).
        let base = 384;
        assert!(base < arch.recovered_position(), "the race window: base < cursor");
        c.counters().prime(base);

        // The new leader rewrites from `base` with a different layout: a
        // 32-byte NewTerm frame, then a 96-byte frame that STRADDLES 480.
        let mut a2 = Appender::new(Arc::clone(&b), 2);
        a2.append_new_term().unwrap();
        a2.append(1, 99, &[0xAB; 64]).unwrap();
        assert_eq!(c.counters().append.load_acquire(), 512);

        // The archive resumes at its own 480 — now mid-payload of [416, 512).
        let err = arch.do_work(&b).unwrap_err();
        let ArchiveError::RecorderCorrupt(c) = err else { panic!("expected RecorderCorrupt: {err:?}") };
        assert_eq!(c.from, 480);
        assert_eq!(c.end, 0, "the very first length word is garbage (CI signature)");
        assert_eq!(c.claimed_len, 0xABAB_ABAB, "a payload byte read as a length word");
    }

    /// The other half of the reproduction above: routing the SAME collapse
    /// through the archive (`truncate_to(base)` on the archive's own thread,
    /// THEN `prime`) resets `durable_pos` with it, so the new leader's frames
    /// land exactly at the cursor and the walk stays intact. This is the
    /// contract `uc2_node`'s `ArchiveCmd::Collapse` exists to hold.
    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn collapse_routed_through_the_archive_keeps_the_walk_intact() {
        let (b, c, dir) = setup(1 << 16);
        let mut arch = Archive::open(test_cfg(dir.path())).unwrap();

        let mut a = Appender::new(Arc::clone(&b), 1);
        for n in 0..5u64 {
            a.append(1, n, &[0xAB; 64]).unwrap();
        }
        while arch.do_work(&b).unwrap() {}
        assert_eq!(arch.recovered_position(), 480);

        // The collapse now goes through the archive FIRST — the cursor follows
        // the cut instead of being stranded above it.
        let base = 384;
        arch.truncate_to(base).unwrap();
        assert_eq!(arch.recovered_position(), base, "the cursor moved with the cut");
        c.counters().prime(base);

        let mut a2 = Appender::new(Arc::clone(&b), 2);
        a2.append_new_term().unwrap();
        a2.append(1, 99, &[0xAB; 64]).unwrap();

        // The archive resumes at `base`, on a frame boundary of the NEW stream.
        while arch.do_work(&b).unwrap() {}
        assert_eq!(arch.recovered_position(), c.counters().append.load_acquire());

        // And the journal no longer holds the discarded tail: replaying from
        // `base` yields the new leader's frames, not the old term's.
        let mut r = arch.replay_from(base).unwrap();
        let f = r.next().unwrap().expect("the NewTerm frame");
        assert_eq!(f.position, base);
        assert_eq!(f.header.leadership_term_id, 2);
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
    fn adopt_floor_on_empty_advances_frontier_without_bytes() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut arch = Archive::open(test_cfg(dir.path())).unwrap();
            assert_eq!(arch.recovered_position(), 0);
            assert_eq!(arch.adopt_floor(64 * 1024).unwrap(), 64 * 1024);
            assert_eq!(arch.recovered_position(), 64 * 1024, "durable advanced with no data");
            assert_eq!(arch.first_base(), 64 * 1024);
            // Idempotent no-op when already at/below the floor.
            assert_eq!(arch.adopt_floor(64 * 1024).unwrap(), 64 * 1024);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn adopt_floor_on_nonempty_below_pos_is_rejected() {
        let (b, _c, dir) = setup(1 << 16);
        let mut arch = Archive::open(test_cfg(dir.path())).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..5 {
            a.append(1, i, &[7u8; 64]).unwrap();
        }
        arch.do_work(&b).unwrap(); // durable = 480, journal non-empty
        // Adopting a floor ABOVE the real frontier would orphan the prefix.
        assert!(matches!(
            arch.adopt_floor(64 * 1024),
            Err(ArchiveError::AdoptFloorConflict { .. })
        ));
        // But adopting one we already cover is a harmless no-op.
        assert_eq!(arch.adopt_floor(0).unwrap(), arch.recovered_position());
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

    /// Helper: an archive holding `n_blocks` blocks of 4 frames each (96 B per
    /// frame → 384 B per block), one block PER SEGMENT (so a purge can actually
    /// drop whole segment files), recorded + fsynced. Returns the frame start
    /// positions across every block. The `TempDir` is intentionally leaked so
    /// the journal dir outlives the returned `Archive`.
    fn archive_with_blocks(n_blocks: usize) -> (Archive, Arc<LogBuffer>, Vec<u64>) {
        let (b, _c, dir) = setup(1 << 16);
        // 384 B block cap = exactly 4 frames/block; a 408 B journal record fills
        // a 440 B segment, so the next block rolls a new segment (1 block/seg).
        let cfg = ArchiveConfig {
            max_block_bytes: 384,
            segment_size_bytes: 440,
            preallocate_segments: false,
            ..ArchiveConfig::new(dir.path())
        };
        let mut arch = Archive::open(cfg).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        let mut frames = Vec::new();
        for i in 0..(n_blocks as u64 * 4) {
            frames.push(a.append(1, i, &[i as u8; 64]).unwrap());
        }
        while arch.do_work(&b).unwrap() {}
        assert_eq!(arch.blocks_recorded(), n_blocks as u64, "one block per 4 frames");
        std::mem::forget(dir); // keep the journal dir alive for `arch`'s lifetime
        (arch, b, frames)
    }

    /// The journal directory backing `a` (test accessor; `cfg` is same-module).
    fn archive_dir(a: &Archive) -> std::path::PathBuf {
        a.cfg.dir.clone()
    }

    /// The durable frontier implied by `frames`: end of the last frame (96 B).
    fn frames_end(frames: &[u64]) -> u64 {
        frames.last().unwrap() + 96
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn purge_below_keeps_covering_block_and_replay_at_pos_succeeds() {
        let (mut archive, _buffer, frames) = archive_with_blocks(4); // 4 blocks, 4 frames each
        let cut = frames[9]; // a frame inside block 2
        let new_first = archive.purge_below(cut).unwrap();
        assert!(new_first <= cut, "covering block retained");
        assert_eq!(archive.first_base(), new_first);
        // replay at the cut still works…
        let mut r = archive.replay_from(cut).unwrap();
        assert!(r.next().unwrap().is_some());
        // …and replay BELOW the new floor is PositionPurged with correct bounds
        match archive.replay_from(new_first.saturating_sub(1)) {
            Err(ArchiveError::PositionPurged { pos, first_base }) => {
                assert_eq!(first_base, new_first);
                assert!(pos < first_base);
            }
            other => panic!("expected PositionPurged, got {other:?}"),
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn purge_below_is_noop_at_or_below_floor_and_survives_reopen() {
        let (mut archive, _b, frames) = archive_with_blocks(4);
        let cut = frames[9];
        let first = archive.purge_below(cut).unwrap();
        assert_eq!(archive.purge_below(first).unwrap(), first, "no-op at floor");
        let dir = archive_dir(&archive);
        drop(archive);
        let re = Archive::open(ArchiveConfig::new(&dir)).unwrap();
        assert_eq!(re.first_base(), first, "floor recovered from journal.first_seq");
        assert_eq!(re.recovered_position(), frames_end(&frames), "frontier untouched by purge");
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

    // --------------------------------------------------------- M7 config scan

    /// A recorded block containing a CONFIG frame between two MESSAGE frames
    /// yields exactly `(frame-END position, payload)`; draining empties it.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn config_observations_capture_frame_at_exact_frame_end() {
        let (b, _c, dir) = setup(1 << 16);
        let mut arch = Archive::open(test_cfg(dir.path())).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        a.append(1, 0, &[0u8; 64]).unwrap(); // ordinary MESSAGE frame first (96 B)
        let end = a.append_config(1, b"cfg-v1").unwrap(); // CONFIG frame next
        a.append(1, 1, &[0u8; 64]).unwrap(); // MESSAGE frame after it
        assert!(arch.do_work(&b).unwrap());
        assert_eq!(arch.take_config_observations(), vec![(end, b"cfg-v1".to_vec())]);
        // draining consumed them
        assert!(arch.take_config_observations().is_empty());
    }

    /// A block with only MESSAGE/PADDING frames (no CONFIG) yields nothing.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn config_observations_empty_for_message_and_padding_only_block() {
        let (b, _c, dir) = setup(1 << 16);
        let mut arch = Archive::open(test_cfg(dir.path())).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        for i in 0..4 {
            a.append(1, i, &[0u8; 64]).unwrap();
        }
        assert!(arch.do_work(&b).unwrap());
        assert!(arch.take_config_observations().is_empty());
    }

    /// Truncation resets the config-observation buffer exactly like the term
    /// cursor: an observation from a dropped tail must never be re-fed as if
    /// still durable (it would be a stale-but-higher-version `ConfigObserved`
    /// racing the SM's own truncation-revert adoption).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn truncate_resets_config_observations() {
        let (b, _c, dir) = setup(1 << 16);
        let mut arch = Archive::open(test_cfg(dir.path())).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 1);
        a.append(1, 0, &[0u8; 64]).unwrap(); // [0, 96)
        a.append_config(1, b"cfg-v1").unwrap(); // [96, 160), same (only) block
        assert!(arch.do_work(&b).unwrap());
        // Deliberately NOT drained here — the observation is still pending when
        // the truncation lands, so the reset below is the thing under test
        // rather than an already-empty buffer.
        arch.truncate_to(96).unwrap(); // drops the CONFIG frame's bytes (first-block cut)
        assert!(arch.take_config_observations().is_empty(), "stale observation must not survive truncation");
    }

    /// Post-M7 Task 3 (review carry): `replay_journal_from` — the shared-
    /// handle `Replay` constructor — matches `Archive::replay_from` frame-for-
    /// frame, returns `Ok(None)` below the purge floor (where `replay_from`
    /// errors `PositionPurged`), and drains empty at/beyond the durable
    /// frontier and on an empty journal.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn replay_journal_from_matches_replay_from_and_handles_bounds() {
        let (mut archive, _b, frames) = archive_with_blocks(4); // 16 frames, 4 blocks
        let journal = archive.journal_arc();

        // Mid-stream frame start: identical stream to replay_from.
        let mid = frames[frames.len() / 2];
        let mut a = archive.replay_from(mid).unwrap();
        let mut b = replay_journal_from(&journal, mid).unwrap().expect("covered position");
        loop {
            match (a.next().unwrap(), b.next().unwrap()) {
                (Some(x), Some(y)) => assert_eq!(x, y),
                (None, None) => break,
                (x, y) => panic!("streams diverged: {x:?} vs {y:?}"),
            }
        }

        // At/beyond the durable frontier: exhausted, not an error.
        let frontier = frames_end(&frames);
        let mut r = replay_journal_from(&journal, frontier).unwrap().expect("constructible");
        assert!(r.next().unwrap().is_none());

        // Below the purge floor: Ok(None) (no floor bookkeeping to report).
        let cut = frames[9];
        let new_first = archive.purge_below(cut).unwrap();
        let journal = archive.journal_arc();
        assert!(
            replay_journal_from(&journal, new_first.saturating_sub(1)).unwrap().is_none(),
            "below-floor position must yield Ok(None)"
        );

        // Empty journal: exhausted replay.
        let dir = tempfile::tempdir().unwrap();
        let empty = Archive::open(test_cfg(dir.path())).unwrap();
        let ej = empty.journal_arc();
        let mut r = replay_journal_from(&ej, 0).unwrap().expect("constructible");
        assert!(r.next().unwrap().is_none());
    }

    // --- Post-M7 follow-up: Replay::next corrupt-block guard ---------------

    /// Encode one frame (header + payload, 32-byte aligned) appended to `buf`,
    /// matching `Appender::append`'s wire layout: payload, then header-sans-
    /// length via `write_header_except_length`, then the unaligned
    /// header+payload byte count written into the leading commit word.
    fn push_frame(buf: &mut Vec<u8>, payload: &[u8], correlation_id: u64) {
        let total = HEADER_LEN + payload.len();
        let start = buf.len();
        buf.resize(start + frame::align_frame_len(total), 0u8);
        buf[start + HEADER_LEN..start + total].copy_from_slice(payload);
        frame::write_header_except_length(
            &mut buf[start..start + HEADER_LEN],
            &FrameHeader {
                length: 0,
                frame_type: FRAME_TYPE_MESSAGE,
                flags: 0,
                leadership_term_id: 0,
                session_id: 0,
                correlation_id,
            },
        );
        buf[start..start + 4].copy_from_slice(&(total as u32).to_le_bytes());
    }

    /// Append a bare frame header (no payload bytes behind it) whose `length`
    /// field claims `claimed_len` bytes — simulates a corrupted/truncated
    /// block tail.
    fn push_corrupt_header(buf: &mut Vec<u8>, claimed_len: u32) {
        let start = buf.len();
        buf.resize(start + HEADER_LEN, 0u8);
        frame::write_header_except_length(
            &mut buf[start..start + HEADER_LEN],
            &FrameHeader {
                length: 0,
                frame_type: FRAME_TYPE_MESSAGE,
                flags: 0,
                leadership_term_id: 0,
                session_id: 0,
                correlation_id: 0,
            },
        );
        buf[start..start + 4].copy_from_slice(&claimed_len.to_le_bytes());
    }

    /// A `Replay` reading block seq 0 of `journal` from the start — same
    /// construction `Archive::replay_from` uses for its "start of a found
    /// block" case, but independent of any `Archive`'s own durable-frontier
    /// bookkeeping since these tests append directly to the journal (bypassing
    /// `Archive::do_work`) to hand-corrupt a block's tail.
    fn replay_block_zero(journal: &Journal) -> Replay<'_> {
        Replay {
            journal,
            seq: 0,
            last_seq: journal.last_seq(),
            block: Vec::new(),
            block_base: 0,
            off: 0,
            skip_below: 0,
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn replay_surfaces_corrupt_block_instead_of_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let arch = Archive::open(test_cfg(dir.path())).unwrap();
        let journal = arch.journal();

        let mut block = Vec::new();
        push_frame(&mut block, b"hello", 7); // one valid frame
        let valid_len = block.len();
        push_corrupt_header(&mut block, 4096); // claims far more than the block holds

        let notifier = journal.append(0, 0, &block).unwrap();
        notifier.wait().unwrap();

        let mut replay = replay_block_zero(journal);
        let first = replay.next().unwrap().expect("the valid frame replays fine");
        assert_eq!(first.payload, b"hello");
        assert_eq!(first.header.correlation_id, 7);

        let err = replay.next().unwrap_err();
        match err {
            ArchiveError::CorruptBlock { off, claimed_len, block_len, .. } => {
                assert_eq!(off, valid_len);
                assert_eq!(claimed_len, 4096);
                assert_eq!(block_len, block.len());
                assert!(off + claimed_len as usize > block_len);
            }
            other => panic!("expected CorruptBlock, got {other:?}"),
        }
    }

    /// Second hazard: a block whose tail has fewer than `HEADER_LEN` bytes
    /// left after the last valid frame (a sub-header remainder, e.g. a torn
    /// final block) — distinct from the overrunning-length case above.
    #[test]
    #[cfg_attr(miri, ignore)] // real journal files + fsync
    fn replay_surfaces_corrupt_block_for_subheader_remainder() {
        let dir = tempfile::tempdir().unwrap();
        let arch = Archive::open(test_cfg(dir.path())).unwrap();
        let journal = arch.journal();

        let mut block = Vec::new();
        push_frame(&mut block, b"hi", 3); // one valid frame
        let valid_len = block.len();
        block.extend_from_slice(&[0u8; 10]); // < HEADER_LEN bytes left in the block

        let notifier = journal.append(0, 0, &block).unwrap();
        notifier.wait().unwrap();

        let mut replay = replay_block_zero(journal);
        assert!(replay.next().unwrap().is_some(), "the valid frame replays fine");

        let err = replay.next().unwrap_err();
        match err {
            ArchiveError::CorruptBlock { off, claimed_len, block_len, .. } => {
                assert_eq!(off, valid_len);
                assert_eq!(claimed_len, 0);
                assert_eq!(block_len, block.len());
            }
            other => panic!("expected CorruptBlock, got {other:?}"),
        }
    }
}
