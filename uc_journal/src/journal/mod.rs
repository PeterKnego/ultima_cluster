// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

pub(crate) mod segment;
mod segment_pipeline;
mod tail_reader;
mod writer;

pub use tail_reader::TailReader;

use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex};

use crate::{JournalError, Notifier};
use writer::{AppendRequest, SeqWatermark, Writer, WriterState};

/// How a preallocated segment's empty tail is laid down. All three produce a
/// full-size file that reads back as zeros (so recovery's zero-tail = end-of-log
/// logic is identical); they differ only in I/O shape, which governs whether the
/// background fill contends with foreground per-commit `fdatasync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreallocFill {
    /// Real zero-write of the whole segment + one `sync_all` + parent-dir
    /// `sync_all`. Original behavior; the A/B baseline and current default.
    ZeroWriteFull,
    /// Real zero-write in chunks, `sync_data` every `prealloc_fill_chunk_bytes`
    /// with a `yield_now` between, no per-temp dir sync. Same written extents as
    /// `ZeroWriteFull`, but the device flush is split into small spaced barriers
    /// so a foreground commit `fdatasync` can interleave. Pure `std`.
    ZeroWritePaced,
    /// `fallocate(FALLOC_FL_ZERO_RANGE)` + one `sync_data`, no per-temp dir sync.
    /// Linux-only; falls back to `ZeroWritePaced` on non-Linux or `OPNOTSUPP`.
    /// Near-zero background I/O *iff* the kernel yields initialized extents
    /// (validated by the fleet A/B, not assumed).
    FallocateZeroRange,
}

#[derive(Debug, Clone)]
pub struct JournalConfig {
    pub dir: std::path::PathBuf,
    pub segment_size_bytes: u64,
    pub durability: crate::Durability,
    /// [`crate::Durability::Eventual`] only: how often the writer thread
    /// fsyncs the active segment (and publishes the durable watermark).
    /// Ignored under `Consistent` (every block syncs before its notifier
    /// resolves). Trade-off: a longer interval defers work but accumulates
    /// BIGGER dirty lumps whose fsync stalls the single writer thread — the
    /// 2026-08-16 eventual-arm bench measured a 6.65x p99 regression at the
    /// then-default 50 ms, and the 2026-08-17 interval retest's dose-response
    /// (6.65x @ 50 ms -> 1.51x @ 5 ms -> 1.03x @ 1 ms) pinned lump size as
    /// the mechanism. Default is 1 ms: at these lump sizes (~rate x 1 ms)
    /// the tail cost vs Consistent is zero.
    pub eventual_fsync_interval: std::time::Duration,
    /// Opt-in (default false): preallocate each segment to `segment_size_bytes`
    /// up front so the per-commit `fdatasync` skips the ext4 metadata commit a
    /// size-extending append otherwise forces. See task on segment preallocation.
    pub preallocate_segments: bool,
    /// Fill strategy for preallocated segments (only consulted when
    /// `preallocate_segments`). Default `FallocateZeroRange` (the A/B winner:
    /// cures the background-fill fsync-contention p99 tail; falls back to
    /// `ZeroWritePaced` on non-Linux / unsupported filesystems).
    pub prealloc_fill: PreallocFill,
    /// Chunk granularity for `ZeroWritePaced` (and the paced fallback): issue a
    /// `sync_data` after roughly this many bytes. Default 4 MiB.
    pub prealloc_fill_chunk_bytes: u64,
}

impl JournalConfig {
    pub fn new(dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            segment_size_bytes: 64 * 1024 * 1024,
            durability: crate::Durability::Consistent,
            eventual_fsync_interval: std::time::Duration::from_millis(1),
            preallocate_segments: false,
            prealloc_fill: PreallocFill::FallocateZeroRange,
            prealloc_fill_chunk_bytes: 4 * 1024 * 1024,
        }
    }
}

pub struct Journal {
    pub(crate) state: Arc<Mutex<WriterState>>,
    writer: Mutex<Option<Writer>>,
    /// Fsync-durable sequence watermark (task28).
    durability: Arc<SeqWatermark>,
    /// Whether `open()` found (and healed) a torn active-segment tail on ANY
    /// segment scanned — an incomplete/garbage record left by a process that
    /// died mid-write. Healing (rewind the cursor to the last durable record)
    /// always happens; this flag lets a caller (uc_node's offline backup
    /// verify, M11) REPORT that it happened without re-deriving the scan.
    healed_torn_tail: bool,
}

impl Journal {
    pub fn open(config: JournalConfig) -> Result<Self, JournalError> {
        std::fs::create_dir_all(&config.dir)?;
        // Remove orphan preallocation temps (a crash between temp-create and
        // activation rename). They hold zero committed records — never
        // referenced until their atomic rename to seg-{seq}.log.
        if config.preallocate_segments
            && let Ok(rd) = std::fs::read_dir(&config.dir)
        {
            for ent in rd.filter_map(|e| e.ok()) {
                let n = ent.file_name();
                let s = n.to_string_lossy();
                if s.starts_with("seg-prealloc.") && s.ends_with(".tmp") {
                    let _ = std::fs::remove_file(ent.path());
                }
            }
        }
        let mut entries: Vec<_> = std::fs::read_dir(&config.dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                let n = e.file_name();
                let s = n.to_string_lossy();
                s.starts_with("seg-") && s.ends_with(".log")
            })
            .collect();
        entries.sort_by_key(|e| e.file_name());

        // Phase 1: open each segment, fix any torn tail, collect (SegmentFile, ScanResult).
        let mut segments_with_scan: Vec<(segment::SegmentFile, segment::ScanResult)> = Vec::new();
        let mut healed_torn_tail = false;
        let n_entries = entries.len();
        for (i, ent) in entries.into_iter().enumerate() {
            let mut seg = segment::SegmentFile::open_for_read(&ent.path())?;
            // Only the LAST (active/highest-numbered) segment can have a
            // TORN-BY-CRASH tail — the writer only ever appends past the
            // current end of the active segment. A ROLL cannot leave a torn
            // tail on the segment it just sealed either: the M11 roll-fsync
            // fix (Task 2c, `writer.rs`'s `write_batch`) fsyncs the outgoing
            // segment k BEFORE k+1 is ever created, so any crash up through
            // that fsync still finds k as the LAST segment on disk (k+1
            // doesn't exist yet — scanned tail-tolerantly, not strictly, by
            // this very branch), and any crash AFTER it finds k fully
            // durable. There is no window where a sealed (non-last) segment
            // can be incomplete via the roll path. Scan the true last
            // segment tail-tolerantly so a crash mid-`append_records` (a
            // valid length prefix over a not-yet-visible zeroed body/CRC)
            // heals through the SAME machinery below instead of
            // hard-erroring `Journal::open` at boot. Every other (sealed)
            // segment keeps the strict `scan()`: a CRC mismatch there is not
            // tail-shaped and must stay a hard error (genuine corruption of
            // an immutable segment is a far more serious signal than a
            // live-tail race).
            let is_active = i + 1 == n_entries;
            let scan = if is_active {
                seg.scan_active_tail_tolerant()?
            } else {
                seg.scan()?
            };
            let scan = if scan.had_torn_tail {
                healed_torn_tail = true;
                if config.preallocate_segments {
                    // Preserve the physical zero tail; only rewind the logical
                    // cursor to the last durable record. The writer resumes
                    // appending into preallocated space (no truncate-then-refill
                    // churn). The first scan's records/index already exclude the
                    // torn tail, so reuse it directly.
                    seg.reset_cursor(scan.last_durable_offset);
                    if is_active {
                        // CRITICAL: `reset_cursor` is logical-only — it does
                        // NOT erase the torn record's bytes still physically
                        // on disk at [last_durable_offset, torn_end). For the
                        // pre-existing `Ok(None)` torn-tail shape (a zero or
                        // truncated length prefix) that residue was always
                        // all-zero and harmless. But
                        // `scan_active_tail_tolerant`'s CRC-mismatch
                        // classification (the M11 CRC anomaly investigation)
                        // heals a record whose REAL, non-zero partial payload
                        // bytes are still on disk (692/2904 bytes in
                        // run-00196, 15668/19216 in run-00751 — see the
                        // investigation's forensics). Left unzeroed, that
                        // residue is a record-shaped span that a LATER strict
                        // `scan()` — `apply_truncate_to_segments`, called
                        // from a live `truncate_after`/`truncate_all` OR from
                        // Phase 2b below replaying an intent file in THIS
                        // SAME open — CRC-fails on again and hard-errors.
                        // Worse: if that later failure happens from a LIVE
                        // `truncate_after` call, after `write_truncate_intent`
                        // already made the intent durable, the intent is
                        // never consumed (`?` skips `remove_truncate_intent`)
                        // — so EVERY subsequent boot heals here again, then
                        // Phase 2b replays the SAME leftover intent and hits
                        // the SAME un-erased residue: a permanently
                        // unbootable node.
                        //
                        // ROUND 2: zeroing only up to `segment_size_bytes` is
                        // NOT enough. `write_batch`'s roll decision
                        // (`writer.rs`: `need_new = projected >=
                        // st.segment_size`) is evaluated BEFORE the record
                        // that crosses the boundary is added — the record
                        // that pushes `projected` over the limit is still
                        // written into the CURRENT segment, only the NEXT
                        // record rolls. So a segment's actual physical size
                        // ending up past `segment_size_bytes` is the ORDINARY
                        // geometry of a nearly-full segment, not an edge
                        // case — and a torn record can straddle that
                        // boundary too. Zeroing only to `segment_size_bytes`
                        // would early-return as a no-op (`preallocate_to`'s
                        // own `total_len <= self.size` guard) whenever the
                        // healed cursor already exceeds it, leaving the real
                        // residue on disk. It also breaks if
                        // `segment_size_bytes` is ever reduced between boots
                        // for the same reason. Zero through the segment's
                        // TRUE physical end instead: `preallocate_to` fills
                        // `[cursor, total_len)` without extending past
                        // `total_len`, and passing `total_len ==
                        // physical_len` never grows `i_size` further — it
                        // only re-zeros what is already there.
                        let zero_to = config.segment_size_bytes.max(seg.physical_len()?);
                        // Skip the (fallocate/write + `sync_data`) barrier
                        // entirely when there is nothing to erase: `scan()`'s
                        // `had_torn_tail` is true whenever ANY bytes trail
                        // the last record, which for a preallocated segment
                        // is the ORDINARY clean-shutdown case (the untouched
                        // zero preallocation) on essentially every boot, not
                        // just a genuine crash-torn residue. Without this
                        // guard the heal branch would re-zero-and-fsync the
                        // active segment's tail on every single preallocated
                        // open, at a real cost against the boot-time bar.
                        if seg.has_nonzero_residue_from(scan.last_durable_offset)? {
                            seg.preallocate_to(
                                zero_to,
                                config.prealloc_fill,
                                config.prealloc_fill_chunk_bytes,
                            )?;
                        }
                    }
                    scan
                } else {
                    seg.truncate(scan.last_durable_offset)?;
                    seg.scan()?
                }
            } else {
                scan
            };
            segments_with_scan.push((seg, scan));
        }

        // Phase 2: sentinel completion — search backwards for a sentinel as the
        // last record of any segment.  A sentinel means a `truncate_after` started
        // (wrote the sentinel + fsync'd) but crashed before removing the sentinel
        // and/or unlinking later segments.  Complete the truncate now.
        let mut sentinel_found_at: Option<(usize, u64)> = None; // (seg_idx, sentinel_byte_offset)
        for (i, (_, scan)) in segments_with_scan.iter().enumerate().rev() {
            if let Some(last_rec) = scan.records.last()
                && segment::is_sentinel(last_rec)
            {
                // Compute the byte offset at which the sentinel record starts.
                // That is: header + sum of all record sizes before the sentinel.
                let sentinel_offset = segment::SEGMENT_HEADER_SIZE as u64
                    + scan.records[..scan.records.len() - 1]
                        .iter()
                        .map(|r| (4 + 16 + r.payload.len() + 4) as u64)
                        .sum::<u64>();
                sentinel_found_at = Some((i, sentinel_offset));
                break;
            }
        }
        if let Some((idx, sentinel_offset)) = sentinel_found_at {
            // Truncate the segment to just before the sentinel, removing it.
            segments_with_scan[idx].0.truncate(sentinel_offset)?;
            // Drop all later segments from disk.
            let later = segments_with_scan.split_off(idx + 1);
            for (seg, _) in later {
                let _ = std::fs::remove_file(seg.path());
            }
            // Re-scan the modified segment so records/last_seq are accurate.
            let new_scan = segments_with_scan[idx].0.scan()?;
            segments_with_scan[idx].1 = new_scan;
        }

        // Phase 2b: complete an interrupted `truncate_after`. A valid intent
        // file means the intent was durable but the destructive phase may not
        // have finished — re-run it idempotently. A torn/corrupt intent means
        // the crash happened while writing the intent, before anything
        // destructive — ignore it. Either way the file is consumed.
        if let Some(keep_seq) = read_truncate_intent(&config.dir) {
            let mut segs: Vec<segment::SegmentFile> =
                segments_with_scan.into_iter().map(|(s, _)| s).collect();
            apply_truncate_to_segments(&config.dir, &mut segs, keep_seq)?;
            segments_with_scan = segs
                .into_iter()
                .map(|s| {
                    let scan = s.scan()?;
                    Ok((s, scan))
                })
                .collect::<Result<Vec<_>, JournalError>>()?;
        }
        remove_truncate_intent(&config.dir)?;

        // Phase 3: compute first_seq / last_seq from final state.
        let mut first_seq: Option<u64> = None;
        let mut last_seq: Option<u64> = None;
        for (_, scan) in &segments_with_scan {
            if let Some(first) = scan.records.first()
                && first_seq.is_none()
            {
                first_seq = Some(first.seq);
            }
            if let Some(last) = scan.records.last() {
                last_seq = Some(last.seq);
            }
        }

        // Install each segment's sparse index from the scan we already performed
        // above, so point reads can use the windowed `read_record` path.
        let mut segments: Vec<segment::SegmentFile> = segments_with_scan
            .into_iter()
            .map(|(mut seg, scan)| {
                seg.set_index(scan.index);
                seg
            })
            .collect();

        // Re-preallocate the active segment so steady-state preallocation is in
        // effect immediately after restart — covers a segment written before
        // the flag was enabled, or one a prior recovery physically truncated.
        if config.preallocate_segments
            && let Some(active) = segments.last_mut()
            && active.physical_len()? < config.segment_size_bytes
        {
            active.preallocate_to(
                config.segment_size_bytes,
                config.prealloc_fill,
                config.prealloc_fill_chunk_bytes,
            )?;
        }

        let pipeline = if config.preallocate_segments {
            Some(crate::journal::segment_pipeline::SegmentPipeline::spawn(
                config.dir.clone(),
                config.segment_size_bytes,
                config.prealloc_fill,
                config.prealloc_fill_chunk_bytes,
            )?)
        } else {
            None
        };

        let state = Arc::new(Mutex::new(WriterState {
            dir: config.dir.clone(),
            pipeline,
            segment_size: config.segment_size_bytes,
            durability: config.durability,
            eventual_fsync_interval: config.eventual_fsync_interval,
            segments,
            last_seq,
            first_seq,
            // Empty on open: recovery rebuilds visibility from the scanned
            // segments; `append()` repopulates this going forward.
            pending: std::collections::BTreeMap::new(),
            truncate_gen: 0,
            persisted_hwm: 0,
            // A freshly-recovered active segment is either empty or has
            // only durably-recovered bytes (Journal::open's own healing —
            // and any Phase-1 residue re-zero — already fsyncs what it
            // touches via `truncate`/`preallocate_to`'s own sync); nothing
            // written since open() has yet to be fsynced by the writer.
            active_segment_dirty: false,
            poisoned: false,
            #[cfg(test)]
            fail_next_fsync: false,
            #[cfg(test)]
            seal_fsync_log: Vec::new(),
        }));
        let durability = SeqWatermark::new();
        let writer = Writer::spawn(Arc::clone(&state), Arc::clone(&durability));
        Ok(Self {
            state,
            writer: Mutex::new(Some(writer)),
            durability,
            healed_torn_tail,
        })
    }

    pub fn first_seq(&self) -> Option<u64> {
        self.state.lock().unwrap().first_seq
    }

    pub fn last_seq(&self) -> Option<u64> {
        self.state.lock().unwrap().last_seq
    }

    /// Whether `open()` found and healed a torn active-segment tail (see the
    /// field doc). `false` for a clean recovery.
    pub fn healed_torn_tail(&self) -> bool {
        self.healed_torn_tail
    }

    /// Highest seq known to be fsync-durable (task28).
    ///
    /// In `Durability::Eventual` this trails [`Journal::last_seq`] by up to the
    /// idle-fsync interval; in `Consistent` it trails by ~one in-flight batch.
    /// Returns 0 before anything is durable.
    pub fn durable_seq(&self) -> u64 {
        self.durability.current()
    }

    /// Block until `seq` is fsync-durable.
    ///
    /// Returns immediately for an already-durable seq. Returns `Err` if a
    /// covering fsync failed or the journal closed before reaching `seq`.
    /// Holds no journal lock while blocking.
    pub fn wait_durable(&self, seq: u64) -> Result<(), JournalError> {
        self.durability.wait(seq)
    }

    /// Register `cb` to fire once `seq` is fsync-durable.
    ///
    /// Fires inline on the calling thread if `seq` is already durable (or
    /// already failed / journal closed). Otherwise fires later on the writer
    /// thread. This is the durability hook for `Durability::Eventual`, where
    /// `append`'s own [`Notifier`] resolves at the buffered write, not fsync.
    pub fn on_durable<F>(&self, seq: u64, cb: F)
    where
        F: FnOnce(Result<(), JournalError>) + Send + 'static,
    {
        self.durability.on_complete(seq, Box::new(cb));
    }

    /// Append a record.  Monotonicity is validated here under the state lock so
    /// that concurrent callers claim seqs in a total order that matches the
    /// channel submission order.  The bg writer fsyncs and signals the Notifier.
    pub fn append(&self, seq: u64, meta: u64, payload: &[u8]) -> Result<Notifier, JournalError> {
        // Size guard (cheap, no I/O needed).
        let body_len = 16 + payload.len();
        let total = (4 + body_len + 4) as u64;

        let (signal, notifier) = Notifier::pending();

        {
            let mut st = self.state.lock().unwrap();
            if st.poisoned {
                // Fail-stop: a WAL fsync failed; the writer has halted and
                // nothing further may be acknowledged.
                return Err(JournalError::Closed);
            }
            if total > st.segment_size {
                return Err(JournalError::PayloadTooLargeForSegment {
                    segment_size: st.segment_size,
                    record_size: total,
                });
            }
            if let Some(last) = st.last_seq
                && seq <= last
            {
                return Err(JournalError::NonMonotonicSeq {
                    expected_gt: last,
                    got: seq,
                });
            }
            // Claim the seq under the lock: send to channel while still holding it,
            // so the channel ordering matches the monotonic seq order.
            let payload_vec = payload.to_vec();
            let req = AppendRequest {
                seq,
                meta,
                payload: payload_vec.clone(),
                signal,
                generation: st.truncate_gen,
            };
            let writer_guard = self.writer.lock().unwrap();
            let w = writer_guard.as_ref().ok_or(JournalError::Closed)?;
            w.tx.send(req).map_err(|_| JournalError::Closed)?;
            drop(writer_guard);

            // Publish the record for IMMEDIATE read visibility, still holding the
            // state lock, so a reader sees it the instant `append()` returns —
            // before the bg writer persists it and before the flush callback
            // (openraft's `RaftLogStorage::append()` readability contract). The
            // writer evicts this seq from `pending` once it lands in a segment;
            // both happen under this lock, so there is no window where the record
            // is invisible and none where it is double-counted. `append()` now
            // owns `last_seq`/`first_seq` advancement (the writer no longer sets
            // them — it would regress them behind these synchronous updates).
            if st.first_seq.is_none() {
                st.first_seq = Some(seq);
            }
            st.last_seq = Some(seq);
            let generation = st.truncate_gen;
            st.pending.insert(seq, (generation, meta, payload_vec));
        }

        Ok(notifier)
    }

    // NOTE: `read`, `read_range`, and `iter_range` all acquire `WriterState`'s mutex,
    // which means reads are serialized with appends (and with each other).  This is
    // correct and free of deadlocks — no caller holds the lock when calling these
    // methods — but it does mean a slow read can block a concurrent append.
    //
    // A profile-driven optimization (lock-free reads via `Arc<RwLock<Vec<Arc<SegmentRef>>>>`)
    // is deferred per the plan's open questions §12.  The refactor was scoped out of Task 15
    // ("take option (a) — add the concurrency test, keep `Mutex<WriterState>`").

    /// Read a single record by `seq`. Uses the segment's sparse index to read
    /// only a bounded window (~64 KiB) rather than scanning the whole segment;
    /// consequently a point read CRC-verifies only the records in that window,
    /// not the entire segment (recovery's `scan` does full validation).
    pub fn read(&self, seq: u64) -> Result<Option<(u64, Vec<u8>)>, JournalError> {
        let st = self.state.lock().unwrap();
        // Overlay the not-yet-persisted tail: an appended-but-unwritten record is
        // served from memory (it is not yet in any segment). Disjoint from the
        // segment path — the writer evicts a seq from `pending` exactly when it
        // becomes segment-readable, under this same lock.
        if let Some((_, meta, payload)) = st.pending.get(&seq) {
            return Ok(Some((*meta, payload.clone())));
        }
        let Some(seg) = st.segments.iter().rev().find(|s| s.base_seq() <= seq) else {
            return Ok(None);
        };
        seg.read_record(seq)
    }

    /// Read all records with `seq` in `range`, in order. Uses each segment's
    /// sparse index to read only the byte span covering the range, and prunes
    /// segments whose seq range does not overlap it. A partial range therefore
    /// CRC-verifies only the records in the spans it reads, not whole segments;
    /// a full/unbounded range reads and verifies everything, as before.
    pub fn read_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<Vec<(u64, u64, Vec<u8>)>, JournalError> {
        // Bounds + reads under one lock so first/last_seq and the segment state
        // are a consistent snapshot.
        let st = self.state.lock().unwrap();
        let (lo, hi) = bounds_to_inclusive(range, st.first_seq, st.last_seq);
        let mut out = Vec::new();
        for (i, seg) in st.segments.iter().enumerate() {
            // Prune: segments are seq-ordered; once one starts above hi, so do
            // all later ones.
            if seg.base_seq() > hi {
                break;
            }
            // Prune: skip a segment whose records all fall below lo — true when
            // the next segment's base_seq <= lo (this segment's max seq < it).
            if let Some(next) = st.segments.get(i + 1)
                && next.base_seq() <= lo
            {
                continue;
            }
            out.extend(seg.read_window(lo, hi)?);
        }
        // Overlay the not-yet-persisted tail. These seqs are not in any segment
        // (disjoint — the writer evicts on persist under this lock), so append
        // then sort by seq to restore ascending order across the merge. Guard the
        // `lo > hi` case (e.g. an empty `n..n` range) — `BTreeMap::range` panics
        // on an inverted range, unlike the segment path.
        if lo <= hi {
            for (&seq, (_, meta, payload)) in st.pending.range(lo..=hi) {
                out.push((seq, *meta, payload.clone()));
            }
            out.sort_by_key(|(seq, _, _)| *seq);
        }
        Ok(out)
    }

    /// Range read returning an iterator. Note: the v1 implementation is
    /// **eager** — internally calls `read_range()` to collect into a `Vec`,
    /// then returns its iterator. The signature is iterator-shaped to leave
    /// room for a future streaming implementation; today's memory profile is
    /// the same as `read_range()`. Callers that need streaming behavior over
    /// large ranges should not assume laziness yet.
    #[allow(clippy::type_complexity)]
    pub fn iter_range(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<impl Iterator<Item = Result<(u64, u64, Vec<u8>), JournalError>> + '_, JournalError>
    {
        let v = self.read_range(range)?;
        Ok(v.into_iter().map(Ok))
    }

    /// Truncate the journal so that only records with `seq <= keep_seq` remain.
    ///
    /// Crash-safe via a durable intent file:
    /// 1. Write `truncate.intent` (keep_seq + CRC), fsync it and the dir —
    ///    the intent is durable *before* anything destructive happens.
    /// 2. Truncate the boundary segment past the last kept record and unlink
    ///    all later segments, then fsync the dir.
    /// 3. Remove the intent file, fsync the dir.
    ///
    /// A crash before 1 completes leaves the journal untouched (a torn
    /// intent is ignored on open); a crash any time after leaves the intent,
    /// and `Journal::open` re-runs step 2 idempotently before consuming it.
    ///
    /// Queued-but-unwritten appends past `keep_seq` are fenced: the writer
    /// drops them via the truncation generation (see `AppendRequest::gen`),
    /// and the durable-seq watermark is pulled back to `keep_seq` so a
    /// subsequent re-append at a truncated seq is only reported durable by
    /// its own fsync.
    ///
    /// Returns a `Notifier::done()` — the operation is fully synchronous.
    pub fn truncate_after(&self, keep_seq: u64) -> Result<Notifier, JournalError> {
        let mut st = self.state.lock().unwrap();
        if st.poisoned {
            return Err(JournalError::Closed);
        }

        // Drop any not-yet-persisted records past the truncation point so the
        // in-memory overlay matches the truncated log (openraft truncates a
        // conflicting suffix that may still be sitting in `pending`), and
        // bump the truncation generation so the writer skips queued requests
        // for the seqs we just dropped.
        st.pending.retain(|&s, _| s <= keep_seq);
        st.truncate_gen += 1;

        // Pull the durability watermark back: durable seqs past keep_seq no
        // longer exist, and an in-flight publish for pre-truncate bytes must
        // not re-advance it (stale generation).
        st.persisted_hwm = st.persisted_hwm.min(keep_seq);
        self.durability.reset_to(keep_seq, st.truncate_gen);

        // Intent → destructive phase → consume intent.
        let dir = st.dir.clone();
        write_truncate_intent(&dir, keep_seq)?;
        apply_truncate_to_segments(&dir, &mut st.segments, keep_seq)?;
        // Every segment that survives this call was just made durable by
        // `apply_truncate_to_segments` itself — `SegmentFile::truncate`'s
        // own `sync_all()` on the boundary segment (the `Some(seg_idx)`
        // arm), or nothing at all if `keep_seq` fell below every segment's
        // `base_seq` (the `None` arm empties `st.segments` entirely). A
        // stale `true` surviving either case — with a possibly-DIFFERENT
        // (or NO) active segment than whichever one set it — is exactly
        // the shape that could reach `write_batch`'s seal block with no
        // current segment to fsync (round-3 fix: that used to panic there).
        st.active_segment_dirty = false;
        remove_truncate_intent(&dir)?;

        // Update in-memory bounds. Anything <= keep_seq survives (on disk or
        // in `pending`), so the journal is non-empty iff some record at or
        // below keep_seq ever existed — i.e. first_seq <= keep_seq.
        st.last_seq = if st.first_seq.is_some_and(|first| keep_seq >= first) {
            Some(keep_seq)
        } else {
            None
        };
        if st.last_seq.is_none() {
            st.first_seq = None;
        }

        Ok(Notifier::done())
    }

    /// Remove every record from the journal — the keep-nothing counterpart of
    /// [`Journal::truncate_after`]. After it returns, [`Journal::first_seq`] and
    /// [`Journal::last_seq`] are `None` and a subsequent `append` restarts the
    /// sequence (typically at 0).
    ///
    /// Crash-safe via the SAME durable intent-file discipline as
    /// `truncate_after`, using the reserved keep_seq sentinel `u64::MAX`
    /// ("keep none"): the intent is fsync-durable before any segment is
    /// unlinked, then all segments are dropped and a fresh empty segment is
    /// created at base_seq 0, then the intent is consumed. `Journal::open`
    /// completes a torn `truncate_all` idempotently by re-running the same
    /// destructive step from the surviving intent. (Consequently `u64::MAX` is
    /// not a valid `keep_seq` for `truncate_after`; a real Raft log index never
    /// reaches it.)
    ///
    /// Queued-but-unwritten appends are fenced exactly as in `truncate_after`
    /// (truncation-generation bump + durable-seq pullback to none), so a stale
    /// request still sitting in the writer channel cannot resurrect a removed
    /// record.
    ///
    /// Returns a `Notifier::done()` — the operation is fully synchronous.
    pub fn truncate_all(&self) -> Result<Notifier, JournalError> {
        let mut st = self.state.lock().unwrap();
        if st.poisoned {
            return Err(JournalError::Closed);
        }

        // Fence every queued-but-unwritten append: none survive a keep-none
        // truncate. Drop the whole in-memory overlay and bump the truncation
        // generation so the writer skips any request whose seq we just cleared.
        st.pending.clear();
        st.truncate_gen += 1;

        // Pull the durability watermark back to none: no durable seq survives,
        // and an in-flight publish for pre-truncate bytes must not re-advance it
        // (stale generation).
        st.persisted_hwm = 0;
        self.durability.reset_to(0, st.truncate_gen);

        // Intent → destructive phase → consume intent.
        let dir = st.dir.clone();
        write_truncate_intent(&dir, KEEP_NONE_SENTINEL)?;
        apply_truncate_to_segments(&dir, &mut st.segments, KEEP_NONE_SENTINEL)?;
        // The keep-none arm always ends with a FRESH segment (`SegmentFile::
        // create`, which syncs the header + parent dir itself) — nothing
        // application-level is unfsynced. See `truncate_after`'s identical
        // clear for the full round-3 rationale.
        st.active_segment_dirty = false;
        remove_truncate_intent(&dir)?;

        st.first_seq = None;
        st.last_seq = None;

        Ok(Notifier::done())
    }

    /// Drop full segments whose final record's seq <= `seq`.
    /// Never drops the active (last) segment even if eligible, to ensure
    /// future appends still have a place to go.
    /// Updates `first_seq` after purging.
    pub fn purge_before(&self, seq: u64) -> Result<(), JournalError> {
        let mut st = self.state.lock().unwrap();

        // Drop segments whose final record's seq <= seq. A non-active segment's
        // last seq is `segments[i+1].base_seq() - 1` — segments are contiguous, so
        // segment i+1 begins exactly one seq after segment i ends. This is O(1) and
        // avoids a full-segment `scan()`.
        //
        // The previous implementation called `seg.scan()` here — decoding and
        // CRC-verifying EVERY record of a 64 MiB segment — just to read its last
        // record's seq. openraft purges the log after every snapshot, so at load
        // that full-scan-per-purge dominated the leader's CPU: ~90M record decodes
        // / ~8 GB of CRC per 40s at 15k msg/s, which was THE throughput ceiling
        // (fixing it: decode 90M→7M, crc32 21%→14%, inflight-128 knee 10k→15k).
        //
        // The active (last) segment is never dropped (we still need a place to
        // append), so it never needs its last seq computed here.
        let n = st.segments.len();
        let mut keep_idx = 0;
        for i in 0..n {
            if i + 1 >= n {
                break; // active segment — never dropped
            }
            let seg_last = st.segments[i + 1].base_seq().saturating_sub(1);
            if seg_last <= seq {
                keep_idx = i + 1;
            } else {
                break;
            }
        }

        let removed: Vec<_> = st.segments.drain(..keep_idx).collect();
        for seg in removed {
            let _ = std::fs::remove_file(seg.path());
        }
        if let Ok(d) = std::fs::File::open(&st.dir) {
            let _ = d.sync_all();
        }

        // Recompute first_seq WITHOUT a full-segment scan: the first remaining
        // segment's first record seq IS its base_seq (segment header, contiguous
        // records). The `last_seq >= base_seq` filter yields None when the journal
        // is empty (fully purged). This was the RESIDUAL HALF of the purge
        // full-scan bug: the last-seq drop loop above was fixed earlier, but this
        // first_seq recompute still `scan()`ed a whole 64 MiB segment on every
        // purge — and openraft purges after every snapshot (~5/s at load), so it
        // was ~93% of the leader's journal decodes (the residual crc32 ceiling).
        st.first_seq = st
            .segments
            .first()
            .map(|s| s.base_seq())
            .filter(|&b| st.last_seq.is_some_and(|last| last >= b));

        Ok(())
    }

    pub fn close(self) -> Result<(), JournalError> {
        let mut g = self.writer.lock().unwrap();
        if let Some(w) = g.take() {
            drop(w.tx);
            if let Some(h) = w.handle {
                let _ = h.join();
            }
        }
        drop(g);
        // Stop the preallocator thread and remove any unconsumed temp. Done
        // after the writer is joined so no activation can race the shutdown.
        if let Some(pipe) = self.state.lock().unwrap().pipeline.take() {
            pipe.shutdown();
        }
        Ok(())
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        let mut g = self.writer.lock().unwrap();
        if let Some(w) = g.take() {
            drop(w.tx);
            if let Some(h) = w.handle {
                let _ = h.join();
            }
        }
        drop(g);
        // Stop the preallocator thread and remove any unconsumed temp. Done
        // after the writer is joined so no activation can race the shutdown.
        // Option::take() leaves None, so close() then Drop is a no-op.
        if let Some(pipe) = self.state.lock().unwrap().pipeline.take() {
            pipe.shutdown();
        }
        // The writer thread is gone and every queued batch has been published.
        // Release any waiter parked on a seq that was never written so it cannot
        // block forever (task28). Idempotent if close() already ran.
        self.durability.close();
    }
}

// ---------------------------------------------------------------------------
// Truncate intent file — crash-safe two-phase truncate_after
// ---------------------------------------------------------------------------

/// Name of the durable truncate-intent file. Written and fsynced *before*
/// any destructive step of `truncate_after`, removed only after the last
/// one; `Journal::open` completes the truncate if it finds one.
const TRUNCATE_INTENT_FILENAME: &str = "truncate.intent";
const TRUNCATE_INTENT_MAGIC: &[u8; 8] = b"ULTJINT1";

/// Reserved `keep_seq` value carried through the truncate-intent machinery to
/// mean "keep NOTHING" — the [`Journal::truncate_all`] sentinel. A real Raft log
/// index never reaches `u64::MAX`, so overloading it is safe; `truncate_after`
/// must never be called with it (it would be reinterpreted as keep-none).
const KEEP_NONE_SENTINEL: u64 = u64::MAX;

fn sync_dir(dir: &std::path::Path) -> Result<(), JournalError> {
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Write + fsync the intent file (magic, keep_seq, CRC), then fsync the dir.
fn write_truncate_intent(dir: &std::path::Path, keep_seq: u64) -> Result<(), JournalError> {
    use std::io::Write;
    let mut buf = Vec::with_capacity(20);
    buf.extend_from_slice(TRUNCATE_INTENT_MAGIC);
    buf.extend_from_slice(&keep_seq.to_le_bytes());
    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    let mut f = std::fs::File::create(dir.join(TRUNCATE_INTENT_FILENAME))?;
    f.write_all(&buf)?;
    f.sync_all()?;
    sync_dir(dir)
}

/// Read a valid intent's keep_seq. A missing, short, or CRC-failing file
/// returns `None` — a torn intent means the crash happened while writing it,
/// before any destructive step, so ignoring it is safe.
fn read_truncate_intent(dir: &std::path::Path) -> Option<u64> {
    let buf = std::fs::read(dir.join(TRUNCATE_INTENT_FILENAME)).ok()?;
    if buf.len() != 20 || &buf[0..8] != TRUNCATE_INTENT_MAGIC {
        return None;
    }
    let stored = u32::from_le_bytes(buf[16..20].try_into().ok()?);
    if crc32fast::hash(&buf[0..16]) != stored {
        return None;
    }
    Some(u64::from_le_bytes(buf[8..16].try_into().ok()?))
}

/// Remove the intent file (idempotent) and fsync the dir.
fn remove_truncate_intent(dir: &std::path::Path) -> Result<(), JournalError> {
    match std::fs::remove_file(dir.join(TRUNCATE_INTENT_FILENAME)) {
        Ok(()) => sync_dir(dir),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Destructive phase of a truncate: drop every record with seq > `keep_seq`
/// from the segment files (truncating the boundary segment, unlinking later
/// ones), then fsync the dir. Idempotent — always runs under a durable
/// intent file, so a crash at any point is completed on the next open.
///
/// Returns the highest surviving on-disk seq, if any.
fn apply_truncate_to_segments(
    dir: &std::path::Path,
    segments: &mut Vec<segment::SegmentFile>,
    keep_seq: u64,
) -> Result<Option<u64>, JournalError> {
    // Keep-none sentinel (truncate_all): unlink EVERY segment and re-seed a
    // fresh empty segment at base_seq 0 so the writer always has an append
    // target. Idempotent under the durable intent — a torn replay runs this
    // again, dropping the (possibly already-fresh) seg-0 and recreating it.
    if keep_seq == KEEP_NONE_SENTINEL {
        let removed = std::mem::take(segments);
        for seg in removed {
            let _ = std::fs::remove_file(seg.path());
        }
        let path = dir.join(format!("seg-{:020}.log", 0u64));
        // Defensive: a prior torn attempt may have left the fresh seg-0 on disk
        // outside `segments`; `create_new` would then hit EEXIST, so clear it.
        let _ = std::fs::remove_file(&path);
        let fresh = segment::SegmentFile::create(&path, 0)?;
        segments.push(fresh);
        sync_dir(dir)?;
        return Ok(None);
    }
    match segments.iter().rposition(|s| s.base_seq() <= keep_seq) {
        None => {
            // keep_seq is below every segment — drop everything.
            let removed = std::mem::take(segments);
            for seg in removed {
                let _ = std::fs::remove_file(seg.path());
            }
            sync_dir(dir)?;
            Ok(None)
        }
        Some(seg_idx) => {
            // Byte offset just past the last record with seq <= keep_seq.
            let scan = segments[seg_idx].scan()?;
            let cut = scan.records.iter().position(|r| r.seq > keep_seq);
            let new_end = cut
                .map(|pos| {
                    segment::SEGMENT_HEADER_SIZE as u64
                        + scan.records[..pos]
                            .iter()
                            .map(|r| (4 + 16 + r.payload.len() + 4) as u64)
                            .sum::<u64>()
                })
                .unwrap_or(scan.last_durable_offset);
            let survivor = match cut {
                Some(0) => None,
                Some(pos) => Some(scan.records[pos - 1].seq),
                None => scan.records.last().map(|r| r.seq),
            };
            segments[seg_idx].truncate(new_end)?;
            let to_remove: Vec<_> = segments.drain(seg_idx + 1..).collect();
            for seg in to_remove {
                let _ = std::fs::remove_file(seg.path());
            }
            sync_dir(dir)?;
            Ok(survivor)
        }
    }
}

fn bounds_to_inclusive(
    range: impl RangeBounds<u64>,
    first: Option<u64>,
    last: Option<u64>,
) -> (u64, u64) {
    let lo = match range.start_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n.saturating_add(1),
        Bound::Unbounded => first.unwrap_or(0),
    };
    let hi = match range.end_bound() {
        Bound::Included(&n) => n,
        Bound::Excluded(&n) => n.saturating_sub(1),
        Bound::Unbounded => last.unwrap_or(u64::MAX),
    };
    (lo, hi)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prealloc_fill_defaults() {
        let cfg = JournalConfig::new("/tmp/does-not-matter");
        assert_eq!(cfg.prealloc_fill, PreallocFill::FallocateZeroRange);
        assert_eq!(cfg.prealloc_fill_chunk_bytes, 4 * 1024 * 1024);
    }

    #[test]
    fn config_preallocate_defaults_off() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = JournalConfig::new(dir.path());
        assert!(
            !cfg.preallocate_segments,
            "preallocation is opt-in (default off)"
        );
    }

    #[test]
    fn open_empty_dir_returns_empty_journal() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        assert_eq!(j.first_seq(), None);
        assert_eq!(j.last_seq(), None);
    }

    #[test]
    fn open_creates_dir_if_missing() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("nested/deep");
        let j = Journal::open(JournalConfig::new(&sub)).unwrap();
        assert!(sub.exists());
        assert_eq!(j.last_seq(), None);
    }

    #[test]
    fn append_one_record_then_read_state() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        let n = j.append(1, 0, b"first").unwrap();
        n.wait().unwrap();
        assert_eq!(j.first_seq(), Some(1));
        assert_eq!(j.last_seq(), Some(1));
    }

    #[test]
    fn append_rejects_non_monotonic() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(5, 0, b"x").unwrap().wait().unwrap();
        let err = j.append(3, 0, b"y").unwrap_err();
        assert!(matches!(
            err,
            JournalError::NonMonotonicSeq {
                expected_gt: 5,
                got: 3
            }
        ));
    }

    #[test]
    fn append_rejects_record_larger_than_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 256;
        let j = Journal::open(cfg).unwrap();
        let big = vec![0u8; 1024];
        let err = j.append(1, 0, &big).unwrap_err();
        assert!(matches!(
            err,
            JournalError::PayloadTooLargeForSegment { .. }
        ));
    }

    #[test]
    fn reopen_sees_appended_records() {
        let dir = tempfile::tempdir().unwrap();
        {
            let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
            j.append(1, 11, b"a").unwrap().wait().unwrap();
            j.append(2, 22, b"bb").unwrap().wait().unwrap();
        }
        let j2 = Journal::open(JournalConfig::new(dir.path())).unwrap();
        assert_eq!(j2.first_seq(), Some(1));
        assert_eq!(j2.last_seq(), Some(2));
    }

    #[test]
    fn consistent_durability_survives_reopen() {
        // Under Consistent durability the group-commit fdatasync (sync_data)
        // must make every acked record durable, so a reopen recovers the full
        // [1, N] range. This guards that fdatasync preserves the same durable
        // prefix as a full fsync would.
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = crate::Durability::Consistent;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=64u64 {
                // append(seq, term, payload) — same call shape as
                // reopen_sees_appended_records.
                j.append(i, i, format!("rec-{i}").as_bytes()).unwrap();
            }
            // Wait the highest seq: the group-commit fsync barrier must make
            // every record at or below seq 64 durable before we drop.
            j.wait_durable(64).unwrap();
            assert_eq!(
                j.durable_seq(),
                64,
                "all 64 records durable after the commit-path fsync"
            );
        } // drop the Journal — clean process exit, no extra fsync on drop
        // Reopen: recovery must locate every fsync'd record.
        let j2 = Journal::open(cfg).unwrap();
        assert_eq!(j2.first_seq(), Some(1));
        assert_eq!(j2.last_seq(), Some(64));
    }

    #[test]
    fn read_returns_appended_record() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 100, b"alpha").unwrap().wait().unwrap();
        j.append(2, 200, b"beta").unwrap().wait().unwrap();
        j.append(3, 300, b"gamma").unwrap().wait().unwrap();

        let (m, p) = j.read(2).unwrap().unwrap();
        assert_eq!(m, 200);
        assert_eq!(p, b"beta");
    }

    #[test]
    fn read_missing_seq_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 0, b"x").unwrap().wait().unwrap();
        assert!(j.read(99).unwrap().is_none());
    }

    #[test]
    fn segment_rotates_when_size_exceeded() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 512;
        let j = Journal::open(cfg).unwrap();
        let payload = vec![0xAB; 200];
        for i in 1..=4u64 {
            j.append(i, 0, &payload).unwrap().wait().unwrap();
        }
        let n = j.state.lock().unwrap().segments.len();
        assert!(n >= 2, "expected segment rotation, got {n} segments");
    }

    #[test]
    fn read_range_returns_inclusive_records() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=5u64 {
            j.append(i, i * 10, format!("p{i}").as_bytes())
                .unwrap()
                .wait()
                .unwrap();
        }
        let v = j.read_range(2..=4).unwrap();
        assert_eq!(v.len(), 3);
        assert_eq!(v[0], (2, 20, b"p2".to_vec()));
        assert_eq!(v[2], (4, 40, b"p4".to_vec()));
    }

    #[test]
    fn iter_range_streams_records() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=5u64 {
            j.append(i, 0, format!("p{i}").as_bytes())
                .unwrap()
                .wait()
                .unwrap();
        }
        let it = j.iter_range(..).unwrap();
        let collected: Vec<_> = it.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(collected.len(), 5);
        assert_eq!(collected[4].0, 5);
    }

    #[test]
    fn read_range_spans_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 256;
        let j = Journal::open(cfg).unwrap();
        for i in 1..=10u64 {
            j.append(i, 0, &[0u8; 100]).unwrap().wait().unwrap();
        }
        let v = j.read_range(3..=8).unwrap();
        assert_eq!(v.len(), 6);
        assert_eq!(v.first().unwrap().0, 3);
        assert_eq!(v.last().unwrap().0, 8);
    }

    #[test]
    fn many_appenders_complete() {
        use std::sync::Arc;
        // Concurrent appenders each claim a monotonic seq and append.
        // The shared `seq_lock` serializes both seq assignment and channel
        // submission so the channel always receives seqs in strictly increasing
        // order, which is the contract the bg writer requires.
        let dir = tempfile::tempdir().unwrap();
        let j = Arc::new(Journal::open(JournalConfig::new(dir.path())).unwrap());
        let seq_lock = Arc::new(Mutex::new(0u64));
        let mut handles = Vec::new();
        for _ in 0..200 {
            let j2 = Arc::clone(&j);
            let sl = Arc::clone(&seq_lock);
            handles.push(std::thread::spawn(move || {
                // Claim seq + submit to channel atomically under the external lock.
                let notifier = {
                    let mut g = sl.lock().unwrap();
                    *g += 1;
                    let seq = *g;
                    j2.append(seq, 0, b"x").unwrap()
                };
                // Wait for durability outside the lock.
                notifier.wait().unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(j.last_seq(), Some(200));
    }

    #[test]
    fn callback_fires_after_durable() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        let counter = Arc::new(AtomicU64::new(0));
        for i in 1..=10u64 {
            let n = j.append(i, 0, b"x").unwrap();
            let c = Arc::clone(&counter);
            n.on_complete(move |r| {
                r.unwrap();
                c.fetch_add(1, Ordering::SeqCst);
            });
        }
        // Spin briefly until all callbacks fire.
        for _ in 0..1000 {
            if counter.load(Ordering::SeqCst) == 10 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn truncate_after_drops_higher_seqs() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=10u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        j.truncate_after(5).unwrap().wait().unwrap();
        assert_eq!(j.last_seq(), Some(5));
        assert!(j.read(7).unwrap().is_none());
        assert_eq!(j.read(5).unwrap().unwrap().1, b"x".to_vec());
        // Re-append from new tail.
        j.append(6, 0, b"new").unwrap().wait().unwrap();
        assert_eq!(j.read(6).unwrap().unwrap().1, b"new".to_vec());
    }

    #[test]
    fn truncate_all_empties_and_reopens_clean() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for s in 0..5 {
            j.append(s, s * 100, &[s as u8; 64])
                .unwrap()
                .wait()
                .unwrap();
        }
        j.truncate_all().unwrap().wait().unwrap();
        assert_eq!(j.first_seq(), None);
        assert_eq!(j.last_seq(), None);
        // append restarts at seq 0
        j.append(0, 7, b"fresh").unwrap().wait().unwrap();
        assert_eq!(j.read(0).unwrap().unwrap(), (7, b"fresh".to_vec()));
        drop(j);
        let j2 = Journal::open(JournalConfig::new(dir.path())).unwrap();
        assert_eq!(j2.last_seq(), Some(0));
    }

    /// A durable keep-none intent left behind by a crash (after the intent was
    /// fsynced but before/while the destructive phase ran) must be completed on
    /// open: every record dropped, the intent consumed, the journal usable.
    /// This pins the torn `truncate_all` idempotent-replay path.
    #[test]
    fn open_completes_truncate_all_from_intent_file() {
        let dir = tempfile::tempdir().unwrap();
        {
            let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
            for i in 0..5u64 {
                j.append(i, 0, b"x").unwrap().wait().unwrap();
            }
        }
        // Simulate a crash right after the keep-none intent became durable but
        // before any segment was unlinked.
        write_intent_bytes(dir.path(), u64::MAX);

        let j2 = Journal::open(JournalConfig::new(dir.path())).unwrap();
        assert_eq!(
            j2.first_seq(),
            None,
            "keep-none intent must empty the journal"
        );
        assert_eq!(j2.last_seq(), None);
        assert!(
            !dir.path().join("truncate.intent").exists(),
            "intent file must be consumed after completion"
        );
        // Journal is usable — re-append from seq 0.
        j2.append(0, 9, b"fresh").unwrap().wait().unwrap();
        assert_eq!(j2.read(0).unwrap().unwrap(), (9, b"fresh".to_vec()));
    }

    #[test]
    fn truncate_after_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 256;
        let j = Journal::open(cfg).unwrap();
        for i in 1..=12u64 {
            j.append(i, 0, &[0u8; 100]).unwrap().wait().unwrap();
        }
        j.truncate_after(4).unwrap().wait().unwrap();
        assert_eq!(j.last_seq(), Some(4));
        // Verify later segments unlinked.
        let segs = std::fs::read_dir(j.state.lock().unwrap().dir.clone())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("seg-")
            })
            .count();
        assert!(segs <= 2);
    }

    #[test]
    fn eventual_mode_returns_already_done_notifier() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = crate::Durability::Eventual;
        let j = Journal::open(cfg).unwrap();
        let n = j.append(1, 0, b"x").unwrap();
        // In Eventual mode, Notifier should resolve quickly without blocking
        // on fsync; for the public contract we just check wait() succeeds.
        n.wait().unwrap();
    }

    #[test]
    fn purge_before_drops_full_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 256;
        let j = Journal::open(cfg).unwrap();
        for i in 1..=12u64 {
            j.append(i, 0, &[0u8; 100]).unwrap().wait().unwrap();
        }
        let n_before = j.state.lock().unwrap().segments.len();
        j.purge_before(6).unwrap();
        let n_after = j.state.lock().unwrap().segments.len();
        assert!(n_after < n_before);
        // first_seq is now >= first kept segment's base_seq.
        assert!(j.first_seq().unwrap() <= 7);
    }

    #[test]
    fn open_recovers_from_unfinished_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        {
            let j = Journal::open(JournalConfig::new(&dir_path)).unwrap();
            for i in 1..=5u64 {
                j.append(i, 0, b"x").unwrap().wait().unwrap();
            }
            // Manually inject a sentinel as if a truncate started but didn't
            // complete the unlinks of later segments. Easiest: just simulate by
            // appending a sentinel record by writing to the file directly.
            let mut st = j.state.lock().unwrap();
            let seg = st.segments.last_mut().unwrap();
            seg.append_record(99, segment::SENTINEL_META, segment::SENTINEL_PAYLOAD)
                .unwrap();
            seg.fsync().unwrap();
        }
        // Reopen — recovery should treat sentinel as truncate marker, drop records >= sentinel seq.
        let j2 = Journal::open(JournalConfig::new(&dir_path)).unwrap();
        // Sentinel at seq=99, but legit records were 1..=5. Sentinel marks "drop > 5".
        assert_eq!(j2.last_seq(), Some(5));
    }

    #[test]
    fn open_truncates_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        {
            let j = Journal::open(JournalConfig::new(&dir_path)).unwrap();
            for i in 1..=3u64 {
                j.append(i, 0, b"x").unwrap().wait().unwrap();
            }
        }
        // Append 5 garbage bytes to the only segment file.
        let entries: Vec<_> = std::fs::read_dir(&dir_path).unwrap().collect();
        let seg_path = entries[0].as_ref().unwrap().path();
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&seg_path)
            .unwrap()
            .write_all(&[0xAB; 5])
            .unwrap();
        // Reopen — torn tail should be truncated.
        let j2 = Journal::open(JournalConfig::new(&dir_path)).unwrap();
        assert_eq!(j2.last_seq(), Some(3));
        // The torn bytes should have been truncated; subsequent append must succeed.
        j2.append(4, 0, b"new").unwrap().wait().unwrap();
    }

    /// Hand-write a record whose length prefix is real, whose first
    /// `real_bytes` of body are REAL non-zero bytes (a genuine in-flight
    /// partial write), and whose remaining body bytes + CRC trailer are still
    /// zero-fill — the exact torn-write shape from the M11 CRC anomaly
    /// investigation's forensics
    /// (`.superpowers/sdd/2026-08-20-uc2-m11-survivable-cluster/crc-anomaly-investigation.md`):
    /// run-00196 had 692 real bytes of a 2904-byte record before the zero
    /// transition; run-00751 had 15668 of 19216. A crash (or, for the
    /// preserved artifacts, a concurrent `fs::copy`) caught a record whose
    /// length prefix AND part of its body had already landed. `real_bytes =
    /// 0` (all-zero body) is deliberately NOT the default here: it is the one
    /// shape where a heal that only rewinds the logical cursor
    /// (`reset_cursor`, without physically zeroing the residue) is harmless —
    /// using it everywhere would hide exactly the residue-left-on-disk defect
    /// fix-round-1 shipped with (see task-2b-report.md's "Fix round 1"
    /// section). Constructing it by hand-writing bytes reproduces the
    /// identical on-disk shape without needing a real preallocated segment
    /// racing a real writer thread.
    ///
    /// Writes at the segment's true LOGICAL end (from a clean `scan()`, not
    /// the physical EOF) — required for a preallocated segment, where the
    /// physical file is already full-size and `OpenOptions::append` would
    /// land far past the real record boundary, inside the segment's ordinary
    /// (already-tolerated) untouched zero tail rather than replacing it.
    /// Returns `(offset written at, total byte length written)`.
    fn write_torn_record(
        seg_path: &std::path::Path,
        payload_len: usize,
        real_bytes: usize,
    ) -> (u64, usize) {
        use std::io::{Seek, SeekFrom, Write};
        let offset = segment::SegmentFile::open_for_read(seg_path)
            .unwrap()
            .scan()
            .unwrap()
            .last_durable_offset;
        let body_len = 16usize + payload_len;
        assert!(
            real_bytes <= body_len,
            "real_bytes must fit within the body"
        );
        let mut bytes = Vec::with_capacity(4 + body_len + 4);
        bytes.extend_from_slice(&(body_len as u32).to_le_bytes());
        bytes.extend(std::iter::repeat_n(0xABu8, real_bytes)); // real (non-zero) partial write
        bytes.extend(std::iter::repeat_n(0u8, body_len - real_bytes + 4)); // unwritten zero-fill + zero CRC trailer
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(seg_path)
            .unwrap();
        f.seek(SeekFrom::Start(offset)).unwrap();
        f.write_all(&bytes).unwrap();
        (offset, bytes.len())
    }

    /// `read_dir` a journal directory, keeping only `seg-*.log` entries and
    /// sorting them — zero-padded segment numbers sort lexicographically in
    /// numeric order, so `entries.last()` is always the active segment.
    fn segment_paths(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("seg-"))
            .map(|e| e.path())
            .collect();
        entries.sort();
        entries
    }

    /// Regression for the M11 CRC anomaly: a crash-torn tail on the ACTIVE
    /// segment (valid length prefix, zero-fill body/CRC) must heal on open,
    /// not hard-error. Non-preallocated config (the default) — exercises the
    /// PHYSICAL-truncate heal branch.
    #[test]
    fn open_heals_active_segment_crc_mismatch_zero_fill_tail() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        {
            let j = Journal::open(JournalConfig::new(&dir_path)).unwrap();
            for i in 1..=3u64 {
                j.append(i, 0, b"x").unwrap().wait().unwrap();
            }
        }
        let seg_path = segment_paths(&dir_path).into_iter().next_back().unwrap();
        write_torn_record(&seg_path, 8, 10);

        // BEFORE the fix this hard-errored (JournalError::Corrupted); AFTER,
        // it must heal and report so via `healed_torn_tail()`.
        let j2 = Journal::open(JournalConfig::new(&dir_path)).unwrap();
        assert!(j2.healed_torn_tail(), "must report the heal");
        assert_eq!(
            j2.last_seq(),
            Some(3),
            "recovered frontier is the last COMPLETE record, not the torn one"
        );
        // The journal is fully usable after the heal.
        j2.append(4, 0, b"new").unwrap().wait().unwrap();
        assert_eq!(j2.read(4).unwrap().unwrap().1, b"new".to_vec());
    }

    /// Same as above but with `preallocate_segments: true` (the production
    /// default at node boot, per `uc_node::node.rs`'s `ArchiveConfig::new`) —
    /// exercises the CURSOR-REWIND heal branch (`reset_cursor`, physical zero
    /// tail preserved) instead of a physical truncate.
    #[test]
    fn open_heals_active_segment_crc_mismatch_zero_fill_tail_preallocated() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let mut cfg = JournalConfig::new(&dir_path);
        cfg.preallocate_segments = true;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=3u64 {
                j.append(i, 0, b"x").unwrap().wait().unwrap();
            }
        }
        let seg_path = segment_paths(&dir_path).into_iter().next_back().unwrap();
        write_torn_record(&seg_path, 8, 10);

        let j2 = Journal::open(cfg).unwrap();
        // NOTE: `healed_torn_tail()` alone is a weak signal here — it is
        // ALSO true for the many pre-existing `Ok(None)`-shaped torn-tail
        // tests, so a regression that silently stopped tail-tolerating the
        // CRC-mismatch shape specifically would not be caught by this one
        // assertion. The load-bearing RED signal for THIS test is the
        // `.unwrap()` on `Journal::open(cfg)` two lines up: before this fix
        // that call returned `Err(Corrupted { reason: "record crc
        // mismatch", .. })` and this test panicked there, never reaching
        // this line at all.
        assert!(j2.healed_torn_tail(), "must report the heal");
        assert_eq!(j2.last_seq(), Some(3));
        j2.append(4, 0, b"new").unwrap().wait().unwrap();
        assert_eq!(j2.read(4).unwrap().unwrap().1, b"new".to_vec());
    }

    /// The strictness boundary: a CRC-mismatched record on the active segment
    /// FOLLOWED by more valid bytes (a subsequent well-formed record) is NOT
    /// tail-shaped — it is real corruption and `Journal::open` must still
    /// hard-error, exactly as it did before this fix.
    #[test]
    fn open_hard_errors_crc_mismatch_followed_by_valid_record() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        {
            let j = Journal::open(JournalConfig::new(&dir_path)).unwrap();
            j.append(1, 0, b"x").unwrap().wait().unwrap();
        }
        let seg_path = segment_paths(&dir_path).into_iter().next_back().unwrap();
        let (torn_offset, torn_len) = write_torn_record(&seg_path, 8, 10);
        // A fully valid record written right after the torn one (at the exact
        // byte offset the torn record's own length prefix declares as its
        // end) — NOT tail-shaped, since something valid follows.
        let good = segment::encode_record(2, 0, b"y");
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&seg_path)
                .unwrap();
            f.seek(SeekFrom::Start(torn_offset + torn_len as u64))
                .unwrap();
            f.write_all(&good).unwrap();
        }

        let err = match Journal::open(JournalConfig::new(&dir_path)) {
            Ok(_) => panic!("expected a hard error, but Journal::open succeeded"),
            Err(e) => e,
        };
        assert!(
            matches!(err, JournalError::Corrupted { .. }),
            "genuine corruption (CRC-bad record followed by valid data) must still hard-error, got {err:?}"
        );
    }

    /// Tolerance must never leak to a SEALED (non-last) segment: a CRC-bad
    /// record there is genuine corruption of an immutable segment, not a live
    /// tail, and must still hard-error `Journal::open`.
    #[test]
    fn open_hard_errors_on_sealed_segment_crc_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let mut cfg = JournalConfig::new(&dir_path);
        cfg.segment_size_bytes = 256;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=12u64 {
                j.append(i, 0, &[0u8; 100]).unwrap().wait().unwrap();
            }
        }
        let entries = segment_paths(&dir_path);
        assert!(
            entries.len() >= 2,
            "test setup must produce at least 2 segments, got {}",
            entries.len()
        );
        let sealed_path = entries[0].clone();
        // Flip the sealed segment's last byte — its final record's CRC trailer.
        let mut bytes = std::fs::read(&sealed_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&sealed_path, &bytes).unwrap();

        let err = match Journal::open(cfg) {
            Ok(_) => panic!("expected a hard error, but Journal::open succeeded"),
            Err(e) => e,
        };
        assert!(
            matches!(err, JournalError::Corrupted { .. }),
            "CRC corruption in a sealed segment must still hard-error, got {err:?}"
        );
    }

    /// Positive multi-segment case: a CRC-mismatched, zero-fill-tail torn
    /// record on the LAST of several segments heals normally, and every
    /// earlier (sealed) segment is left byte-for-byte untouched — pins that
    /// `is_active` in `Journal::open`'s Phase 1 loop correctly targets only
    /// the highest-numbered segment when there is more than one on disk.
    #[test]
    fn open_heals_torn_tail_on_the_last_of_several_segments() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let mut cfg = JournalConfig::new(&dir_path);
        cfg.segment_size_bytes = 256;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=12u64 {
                j.append(i, 0, &[0u8; 100]).unwrap().wait().unwrap();
            }
        }
        let entries = segment_paths(&dir_path);
        assert!(
            entries.len() >= 2,
            "test setup must produce at least 2 segments, got {}",
            entries.len()
        );
        let active_path = entries.last().unwrap().clone();
        let sealed_before: Vec<Vec<u8>> = entries[..entries.len() - 1]
            .iter()
            .map(|p| std::fs::read(p).unwrap())
            .collect();
        write_torn_record(&active_path, 8, 10);

        let j2 = Journal::open(cfg).unwrap();
        assert!(j2.healed_torn_tail(), "must report the heal");
        assert_eq!(
            j2.last_seq(),
            Some(12),
            "the torn record was never a durable seq"
        );

        for (path, before) in entries[..entries.len() - 1].iter().zip(sealed_before) {
            assert_eq!(
                &std::fs::read(path).unwrap(),
                &before,
                "sealed segment {path:?} must be byte-for-byte untouched by the heal"
            );
        }
    }

    /// CRITICAL regression (fix round 1): `reset_cursor` alone rewinds the
    /// LOGICAL cursor but leaves a torn record's real, non-zero residue
    /// physically on disk at [last_durable_offset, torn_end). A later strict
    /// `scan()` — e.g. inside a live `truncate_after` call's
    /// `apply_truncate_to_segments` — re-decodes that residue, CRC-fails
    /// again, and (since this can happen AFTER `truncate_after`'s own intent
    /// file already went durable) leaves an unconsumed intent that wedges
    /// EVERY subsequent `Journal::open` permanently. The fix re-zeros the
    /// healed span in place (`preallocate_to` from the rewound cursor)
    /// immediately in Phase 1, before any later phase can observe it. This
    /// drives the exact sequence: heal on open, then a LIVE `truncate_after`
    /// at a position inside the healed segment, then a reopen — all three
    /// must succeed, and the healed span must read back zero.
    #[test]
    fn heal_zeros_the_residue_so_a_later_truncate_after_does_not_wedge_the_node() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let mut cfg = JournalConfig::new(&dir_path);
        cfg.preallocate_segments = true;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=5u64 {
                j.append(i, 0, b"x").unwrap().wait().unwrap();
            }
        }
        let seg_path = segment_paths(&dir_path).into_iter().next_back().unwrap();
        let (torn_offset, torn_len) = write_torn_record(&seg_path, 8, 10);

        // Open — Phase 1 must heal (CRC-mismatch, real non-zero partial body).
        let j2 = Journal::open(cfg.clone()).unwrap();
        assert!(j2.healed_torn_tail(), "must report the heal");
        assert_eq!(j2.last_seq(), Some(5));

        // BELT: the healed span reads back all-zero on disk — the fix must
        // have physically erased the residue, not just logically skipped it.
        let after_heal = std::fs::read(&seg_path).unwrap();
        let healed_span = &after_heal[torn_offset as usize..torn_offset as usize + torn_len];
        assert!(
            healed_span.iter().all(|&b| b == 0),
            "the healed span must read back zero after the fix"
        );

        // WEDGE: a routine truncate_after at a position inside the healed
        // segment must succeed — this is the live path that, pre-round-1-fix,
        // hit the un-erased residue via apply_truncate_to_segments' strict
        // scan() and could leave an unconsumed intent behind.
        j2.truncate_after(3).unwrap().wait().unwrap();
        assert_eq!(j2.last_seq(), Some(3));
        drop(j2);

        // REOPEN: must also succeed — pins against the "permanently
        // unbootable" failure mode.
        let j3 = Journal::open(cfg).unwrap();
        assert_eq!(j3.last_seq(), Some(3));
    }

    /// Closes the deferred edge disclosed in task-2b-report.md: Phase 2b
    /// (`read_truncate_intent` / `apply_truncate_to_segments`) also scans the
    /// active segment strictly. If Phase 1's heal left non-zero residue
    /// behind, a SINGLE `Journal::open` call that must both heal a torn tail
    /// AND replay a pending truncate intent (e.g. a live `truncate_after`
    /// call whose intent went durable just before a crash) would hit that
    /// residue on the SAME open and hard-error — permanently, since Phase 2b
    /// runs on every subsequent boot too. With the fix, the residue is
    /// erased in Phase 1 before Phase 2b ever runs, so this now succeeds.
    #[test]
    fn open_heals_active_tail_and_replays_a_pending_intent_in_the_same_open() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let mut cfg = JournalConfig::new(&dir_path);
        cfg.preallocate_segments = true;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=5u64 {
                j.append(i, 0, b"x").unwrap().wait().unwrap();
            }
        }
        let seg_path = segment_paths(&dir_path).into_iter().next_back().unwrap();
        write_torn_record(&seg_path, 8, 10);
        // A durable intent left over from an earlier truncate_after(3) call
        // whose destructive phase (apply_truncate_to_segments) never got to
        // run before the (separate) crash that also tore the append tail.
        write_intent_bytes(&dir_path, 3);

        let j2 = Journal::open(cfg).unwrap();
        assert_eq!(
            j2.last_seq(),
            Some(3),
            "the replayed intent truncates to keep_seq=3"
        );
        assert!(
            !dir_path.join("truncate.intent").exists(),
            "the intent must be consumed, not left to wedge every subsequent boot"
        );
    }

    /// ROUND 2 regression: a torn record whose bytes STRADDLE
    /// `config.segment_size_bytes`. `write_batch`'s roll decision
    /// (`writer.rs`: `need_new = projected >= st.segment_size`) is evaluated
    /// BEFORE the record that crosses the boundary is added — the record
    /// that pushes `projected` past the limit is still written into the
    /// CURRENT segment; only the record AFTER it rolls. So a segment's true
    /// physical size ending up past `segment_size_bytes` is the ORDINARY
    /// geometry of a segment nearing capacity, not a contrived edge case.
    /// Round 1's heal capped its re-zero at `config.segment_size_bytes`,
    /// which silently no-ops (`preallocate_to`'s own `total_len <=
    /// self.size` early return) once the healed cursor already exceeds that
    /// bound — leaving the real non-zero residue on disk. The segment then
    /// rolls off (seals) with that residue sitting past the last valid
    /// record, and a LATER open's STRICT scan of the now-sealed segment
    /// hard-errors on it: the same permanently-unbootable outcome as the
    /// round-1 critical, one roll later.
    #[test]
    fn heal_zeros_through_physical_eof_when_the_torn_record_straddles_segment_size() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let mut cfg = JournalConfig::new(&dir_path);
        cfg.preallocate_segments = true;
        cfg.segment_size_bytes = 300;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            // Each append is its own waited (serial) batch, so `flush_run`
            // writes it immediately: 3 records of 124 bytes on disk (4-byte
            // length prefix + 16-byte seq/meta + 100-byte payload + 4-byte
            // CRC) push segment0's TRUE physical/logical size to
            // 32 (header) + 3*124 = 404 bytes — already past
            // `segment_size_bytes` (300). This is the ordinary straddling
            // geometry `writer.rs` produces, not something hand-engineered.
            for i in 1..=3u64 {
                j.append(i, 0, &[0u8; 100]).unwrap().wait().unwrap();
            }
        }
        let seg_path = segment_paths(&dir_path).into_iter().next_back().unwrap();
        let before_heal_len = std::fs::metadata(&seg_path).unwrap().len();
        assert!(
            before_heal_len > cfg.segment_size_bytes,
            "test setup must actually straddle: {before_heal_len} is not > {}",
            cfg.segment_size_bytes
        );
        let (torn_offset, torn_len) = write_torn_record(&seg_path, 42, 10);
        assert!(
            torn_offset > cfg.segment_size_bytes,
            "the torn record itself must start past segment_size_bytes, got offset {torn_offset}"
        );

        // Open — Phase 1 must heal, and the residue must be zeroed all the
        // way to the segment's TRUE physical EOF, not just to
        // segment_size_bytes.
        let j2 = Journal::open(cfg.clone()).unwrap();
        assert!(j2.healed_torn_tail(), "must report the heal");
        assert_eq!(j2.last_seq(), Some(3));

        let after_heal = std::fs::read(&seg_path).unwrap();
        let healed_span = &after_heal[torn_offset as usize..torn_offset as usize + torn_len];
        assert!(
            healed_span.iter().all(|&b| b == 0),
            "the healed span must read back zero through physical EOF, even \
             though it starts past config.segment_size_bytes"
        );

        // WRITER RESUMES: segment0's healed cursor (404) is already >=
        // segment_size_bytes (300), so the very next append rolls
        // immediately, sealing segment0 at exactly its pre-torn-record
        // length with no new content appended past the heal.
        j2.append(4, 0, b"y").unwrap().wait().unwrap();
        assert_eq!(j2.last_seq(), Some(4));
        drop(j2);

        // REOPEN: segment0 is now SEALED (no longer last) — Journal::open's
        // Phase 1 scans it with the STRICT scan(). Pre-fix, the un-erased
        // residue past segment_size_bytes would still be sitting there and
        // hard-error here; post-fix it reads back as clean trailing zero.
        let j3 = Journal::open(cfg).unwrap();
        assert_eq!(j3.last_seq(), Some(4));
    }

    /// With the configurable interval set absurdly high, Eventual mode's
    /// append notifier still resolves at the buffered write, but the fsync
    /// watermark must NOT advance in any reasonable window — pinning that
    /// the interval config is actually honored (under the former fixed
    /// 50 ms, this assertion window would see it advance).
    #[test]
    fn eventual_interval_is_honored_a_huge_interval_defers_the_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = crate::Durability::Eventual;
        cfg.eventual_fsync_interval = std::time::Duration::from_secs(3600);
        let j = Journal::open(cfg).unwrap();
        // Seq 1, not 0: the watermark's `hwm > 0` publish guard makes seq 0
        // unobservable, which would render this test vacuous.
        j.append(1, 0, b"payload").unwrap().wait().unwrap(); // buffered-write ack
        std::thread::sleep(std::time::Duration::from_millis(300));
        assert_eq!(
            j.durable_seq(),
            0,
            "watermark advanced despite a 1h interval — interval config not honored"
        );
    }

    /// Validates that concurrent readers and a writer thread do not deadlock or
    /// starve each other.  Reads and appends both acquire `WriterState`'s mutex,
    /// so they serialise, but neither side holds the lock across I/O for long
    /// enough to cause starvation in practice.
    #[test]
    fn eventual_periodic_fsync_runs_on_idle() {
        // Append once, then sit idle for > the bg writer's 50 ms periodic
        // fsync interval. The timer path (RecvTimeoutError::Timeout +
        // fsync_active_segment) must execute. We don't observe its effect
        // directly — the test passes if the journal still works after.
        use crate::Durability;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = Durability::Eventual;
        let j = Journal::open(cfg).unwrap();
        j.append(1, 0, b"x").unwrap().wait().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(150));
        // Append again to confirm the writer thread is still healthy.
        j.append(2, 0, b"y").unwrap().wait().unwrap();
        assert_eq!(j.last_seq(), Some(2));
    }

    #[test]
    fn close_releases_writer_thread() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 0, b"x").unwrap().wait().unwrap();
        j.close().unwrap();
    }

    #[test]
    fn truncate_after_below_first_seq_drops_everything() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 5..=8u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        // keep_seq is below all base_seqs — drops every segment.
        j.truncate_after(2).unwrap().wait().unwrap();
        assert_eq!(j.first_seq(), None);
        assert_eq!(j.last_seq(), None);
        // Re-append from scratch must work.
        j.append(10, 0, b"new").unwrap().wait().unwrap();
        assert_eq!(j.last_seq(), Some(10));
    }

    #[test]
    fn purge_below_threshold_protects_active_segment() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=3u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        // Threshold above all records — would drop everything; the active segment
        // must be retained so future appends still work.
        j.purge_before(100).unwrap();
        j.append(4, 0, b"y").unwrap().wait().unwrap();
        assert_eq!(j.last_seq(), Some(4));
    }

    #[test]
    fn bounds_to_inclusive_handles_excluded_variants() {
        // Excluded lo: 2..hi → starts at 3.
        let v: (u64, u64) = bounds_to_inclusive(
            (Bound::Excluded(2u64), Bound::Excluded(8u64)),
            Some(0),
            Some(20),
        );
        assert_eq!(v, (3, 7));
    }

    #[test]
    fn read_range_with_excluded_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=10u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        let v = j
            .read_range((Bound::Excluded(2u64), Bound::Excluded(6u64)))
            .unwrap();
        let seqs: Vec<u64> = v.into_iter().map(|(s, _, _)| s).collect();
        assert_eq!(seqs, vec![3, 4, 5]);
    }

    #[test]
    fn read_below_first_seq_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(10, 0, b"x").unwrap().wait().unwrap();
        // No segment has base_seq <= 5 — short-circuits.
        assert!(j.read(5).unwrap().is_none());
    }

    #[test]
    fn read_seq_in_gap_returns_none() {
        // Strictly-monotonic seqs allow gaps. Reading a seq between two records
        // hits the early-termination branch in `read` (`r.seq > seq`).
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 0, b"a").unwrap().wait().unwrap();
        j.append(5, 0, b"b").unwrap().wait().unwrap();
        j.append(10, 0, b"c").unwrap().wait().unwrap();
        assert!(j.read(3).unwrap().is_none()); // between 1 and 5
        assert!(j.read(7).unwrap().is_none()); // between 5 and 10
    }

    #[test]
    fn read_above_last_seq_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=3u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        // Linear scan walks past all records; r.seq > seq never triggers because
        // the target is beyond the tail. Falls through to Ok(None).
        assert!(j.read(99).unwrap().is_none());
    }

    /// openraft's `RaftLogStorage::append()` requires an appended entry to be
    /// readable the instant `append()` returns — before the flush callback and
    /// before durability. Verify every read path sees the record WITHOUT waiting
    /// on the Notifier (i.e. while the bg writer may not have persisted it yet),
    /// in both durability modes. Regression for the read-after-append visibility
    /// gap that tripped openraft's apply-drain debug-assert (openraft#1780): the
    /// reader used to lag `append()` until the writer thread drained the channel,
    /// yielding a short read that omitted the just-appended tail entry.
    #[test]
    fn append_is_immediately_readable_before_flush() {
        for durability in [crate::Durability::Eventual, crate::Durability::Consistent] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = JournalConfig::new(dir.path());
            cfg.durability = durability;
            let j = Journal::open(cfg).unwrap();

            for i in 1..=200u64 {
                // Do NOT wait on the notifier — visibility must not depend on the
                // bg writer having processed/persisted the record.
                let _notifier = j.append(i, i * 3, b"payload").unwrap();

                // Point read sees it immediately.
                let (meta, payload) = j.read(i).unwrap().unwrap_or_else(|| {
                    panic!("seq {i} not readable immediately after append ({durability:?})")
                });
                assert_eq!(meta, i * 3);
                assert_eq!(&payload, b"payload");

                // last_seq reflects it immediately.
                assert_eq!(j.last_seq(), Some(i));

                // Range read returns the whole prefix with NO short read — the
                // exact invariant openraft's apply path relies on.
                let v = j.read_range(1..=i).unwrap();
                assert_eq!(
                    v.len() as u64,
                    i,
                    "short read at seq {i} ({durability:?}): got {} entries",
                    v.len()
                );
                assert_eq!(v.last().unwrap().0, i);
            }

            // Drain to durability and confirm the overlay was fully evicted (no
            // double-counting once everything is persisted).
            j.wait_durable(200).unwrap();
            assert!(j.state.lock().unwrap().pending.is_empty());
            assert_eq!(j.read_range(1..=200).unwrap().len(), 200);
        }
    }

    #[test]
    fn concurrent_reads_during_appends() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let j = Arc::new(Journal::open(JournalConfig::new(dir.path())).unwrap());
        for i in 1..=100u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        let j2 = Arc::clone(&j);
        let reader = std::thread::spawn(move || {
            let mut count = 0u64;
            while !stop2.load(Ordering::SeqCst) {
                let v = j2.read_range(1..=50).unwrap();
                assert_eq!(v.len(), 50);
                count += 1;
            }
            count
        });
        for i in 101..=200u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        stop.store(true, Ordering::SeqCst);
        let read_count = reader.join().unwrap();
        assert!(read_count > 0);
        assert_eq!(j.last_seq(), Some(200));
    }

    // --- task28: durable_seq watermark ---------------------------------------

    /// In Consistent mode every append is fsynced, so the watermark reaches the
    /// last appended seq once its Notifier resolves.
    #[test]
    fn durable_seq_advances_in_consistent_mode() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=5u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        assert!(j.durable_seq() >= 5);
        j.wait_durable(5).unwrap();
    }

    /// In Eventual mode the append Notifier resolves before fsync, but the
    /// watermark still lets a caller observe durability — advanced by the
    /// idle-timer fsync. `wait_durable` blocks until that fsync lands.
    #[test]
    fn durable_seq_advances_in_eventual_mode() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = crate::Durability::Eventual;
        let j = Journal::open(cfg).unwrap();
        for i in 1..=3u64 {
            j.append(i, 0, b"x").unwrap();
        }
        // Deterministic: block until the idle-timer fsync publishes seq 3.
        j.wait_durable(3).unwrap();
        assert!(j.durable_seq() >= 3);
    }

    /// `wait_durable` on an already-durable seq returns immediately.
    #[test]
    fn wait_durable_already_durable_is_immediate() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 0, b"x").unwrap().wait().unwrap();
        j.wait_durable(1).unwrap();
        j.wait_durable(1).unwrap(); // second call must not block
    }

    /// `on_durable` fires exactly once with `Ok` after the seq becomes durable.
    #[test]
    fn on_durable_fires_once_after_fsync() {
        use std::sync::mpsc;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = crate::Durability::Eventual;
        let j = Journal::open(cfg).unwrap();
        let (tx, rx) = mpsc::channel();
        j.on_durable(1, move |res| tx.send(res).unwrap());
        j.append(1, 0, b"x").unwrap();
        let got = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("callback did not fire");
        assert!(got.is_ok());
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(50))
                .is_err()
        );
    }

    /// `on_durable` fires inline when the seq is already durable.
    #[test]
    fn on_durable_fires_inline_when_already_durable() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 0, b"x").unwrap().wait().unwrap();
        j.wait_durable(1).unwrap();
        let fired = Arc::new(AtomicBool::new(false));
        let f2 = Arc::clone(&fired);
        j.on_durable(1, move |res| {
            assert!(res.is_ok());
            f2.store(true, Ordering::SeqCst);
        });
        assert!(fired.load(Ordering::SeqCst));
    }

    #[test]
    fn coalesced_batch_spans_segments() {
        // Submit a burst WITHOUT waiting each append — this lets the bg writer
        // drain multiple records into one batch, exercising the coalesced write
        // path including rotation across the 256-byte segment boundary. Whatever
        // the batch grouping, the result must be correct.
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 256;
        let j = Journal::open(cfg).unwrap();
        let payload = [0xCDu8; 100];
        let mut last = None;
        for i in 1..=12u64 {
            last = Some(j.append(i, i * 7, &payload).unwrap());
        }
        last.unwrap().wait().unwrap();

        assert_eq!(j.last_seq(), Some(12));
        let v = j.read_range(1..=12).unwrap();
        assert_eq!(v.len(), 12);
        assert_eq!(v[0], (1, 7, payload.to_vec()));
        assert_eq!(v[11], (12, 84, payload.to_vec()));
        let segs = j.state.lock().unwrap().segments.len();
        assert!(segs >= 2, "expected rotation across segments, got {segs}");
    }

    /// A waiter parked on a seq that is never written is released with `Err`
    /// when the journal is dropped, rather than blocking forever.
    #[test]
    fn close_releases_parked_waiter_with_err() {
        use std::sync::mpsc;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = crate::Durability::Eventual;
        let j = Journal::open(cfg).unwrap();
        let (tx, rx) = mpsc::channel();
        j.on_durable(999, move |res| tx.send(res).unwrap());
        drop(j); // joins writer, then close() drains parked waiters.
        let got = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("callback did not fire on close");
        assert!(matches!(got, Err(JournalError::Closed)));
    }

    #[test]
    fn read_range_partial_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 2 * 1024; // small → many segments
        let j = Journal::open(cfg).unwrap();
        let payload = [0x5Au8; 200];
        for i in 1..=50u64 {
            j.append(i, i * 2, &payload).unwrap().wait().unwrap();
        }
        let nseg = j.state.lock().unwrap().segments.len();
        assert!(
            nseg >= 4,
            "want several segments to exercise pruning, got {nseg}"
        );

        // Localized partial range in the middle: leading and trailing segments prune.
        let v = j.read_range(20..=29).unwrap();
        let seqs: Vec<u64> = v.iter().map(|(s, _, _)| *s).collect();
        assert_eq!(seqs, (20..=29).collect::<Vec<_>>());
        assert_eq!(v[0], (20, 40, payload.to_vec()));
        assert_eq!(v[9], (29, 58, payload.to_vec()));

        // Full unbounded range is unchanged (no regression).
        assert_eq!(j.read_range(..).unwrap().len(), 50);
    }

    #[test]
    fn read_range_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let payload = vec![0x6Bu8; 20 * 1024]; // multi-window single segment
        {
            let j = Journal::open(JournalConfig::new(&dir_path)).unwrap();
            for i in 1..=30u64 {
                j.append(i, i * 5, &payload).unwrap().wait().unwrap();
            }
        }
        // Reopen: index installed at open → windowed range path.
        let j2 = Journal::open(JournalConfig::new(&dir_path)).unwrap();
        let v = j2.read_range(10..=15).unwrap();
        let seqs: Vec<u64> = v.iter().map(|(s, _, _)| *s).collect();
        assert_eq!(seqs, vec![10, 11, 12, 13, 14, 15]);
        assert_eq!(v[0].1, 50); // meta = 10 * 5
        assert_eq!(v[5].2, payload);
    }

    // --- truncate_after crash-safety (intent file), watermark, fencing ---

    /// Write a truncate-intent file the way `truncate_after` does, simulating
    /// a crash after the intent was made durable but before (or during) the
    /// destructive steps.
    fn write_intent_bytes(dir: &std::path::Path, keep_seq: u64) {
        let mut buf = Vec::with_capacity(20);
        buf.extend_from_slice(b"ULTJINT1");
        buf.extend_from_slice(&keep_seq.to_le_bytes());
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(dir.join("truncate.intent"), buf).unwrap();
    }

    /// A durable truncate intent left behind by a crash must be completed on
    /// open: records past keep_seq dropped, later segments unlinked, the
    /// intent consumed, and the journal usable afterwards.
    #[test]
    fn open_completes_truncate_from_intent_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.segment_size_bytes = 256;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=12u64 {
                j.append(i, 0, &[0u8; 100]).unwrap().wait().unwrap();
            }
        }
        write_intent_bytes(dir.path(), 4);

        let j2 = Journal::open(cfg).unwrap();
        assert_eq!(j2.last_seq(), Some(4), "intent truncate not completed");
        assert!(j2.read(5).unwrap().is_none());
        assert!(
            !dir.path().join("truncate.intent").exists(),
            "intent file must be consumed after completion"
        );
        j2.append(5, 0, b"new").unwrap().wait().unwrap();
        assert_eq!(j2.read(5).unwrap().unwrap().1, b"new".to_vec());
    }

    /// A torn/garbage intent file means the crash happened *while writing the
    /// intent* — before any destructive step — so it is ignored and removed.
    #[test]
    fn corrupt_truncate_intent_is_ignored_and_removed() {
        let dir = tempfile::tempdir().unwrap();
        {
            let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
            for i in 1..=3u64 {
                j.append(i, 0, b"x").unwrap().wait().unwrap();
            }
        }
        std::fs::write(dir.path().join("truncate.intent"), b"garbage").unwrap();

        let j2 = Journal::open(JournalConfig::new(dir.path())).unwrap();
        assert_eq!(j2.last_seq(), Some(3), "journal must be intact");
        assert!(
            !dir.path().join("truncate.intent").exists(),
            "corrupt intent must be removed"
        );
    }

    /// A completed truncate_after leaves no intent file behind.
    #[test]
    fn truncate_after_leaves_no_intent_file() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=10u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        j.truncate_after(5).unwrap().wait().unwrap();
        assert!(!dir.path().join("truncate.intent").exists());
    }

    /// `truncate_after` must pull the durable watermark back to keep_seq:
    /// otherwise a fresh append at a truncated seq is instantly reported
    /// durable (`wait_durable`/`on_durable`/`durable_seq`) before any fsync —
    /// in a Raft log that acks un-fsynced replacement entries.
    #[test]
    fn truncate_after_resets_durable_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for i in 1..=10u64 {
            j.append(i, 0, b"x").unwrap().wait().unwrap();
        }
        j.wait_durable(10).unwrap();

        j.truncate_after(5).unwrap().wait().unwrap();
        assert!(
            j.durable_seq() <= 5,
            "watermark {} not reset by truncate_after(5)",
            j.durable_seq()
        );

        // The replacement entry becomes durable through its own fsync.
        j.append(6, 0, b"new").unwrap().wait().unwrap();
        j.wait_durable(6).unwrap();
        assert!(j.durable_seq() >= 6);
    }

    /// ROUND 3 regression (M11 roll-fsync fix review): `active_segment_dirty`
    /// must not survive a `truncate_after` call that empties `st.segments`
    /// (`apply_truncate_to_segments`' `None` arm, reached when `keep_seq`
    /// falls below every segment's `base_seq`). Under `Durability::Eventual`
    /// with an idle-timer interval set absurdly high (the only OTHER thing
    /// that would clear the flag pre-fix, deliberately prevented from firing
    /// during this test), a stale `true` flag with an empty `segments` used
    /// to reach `write_batch`'s seal block on the VERY NEXT append (which
    /// re-creates a segment via `need_new`'s `segments.is_empty()` arm) and
    /// panic on an `.expect` there — killing the writer thread WHILE it held
    /// the state lock, poisoning the mutex, and leaving that append's
    /// `Notifier` uncompleted forever (the writer died before ever reaching
    /// the per-request `signal.complete(...)` loop). Exercises the exact
    /// repro sequence the reviewer traced: append, truncate_after(keep_seq
    /// below every base_seq), append again.
    #[test]
    fn eventual_truncate_that_empties_segments_does_not_panic_or_hang() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let mut cfg = JournalConfig::new(&dir_path);
        cfg.durability = crate::Durability::Eventual;
        // Absurdly high: this test must not let the idle-timer fsync fire
        // and accidentally mask the bug by clearing the flag itself.
        cfg.eventual_fsync_interval = std::time::Duration::from_secs(3600);

        let j = Journal::open(cfg.clone()).unwrap();
        // Buffered write under Eventual: sets active_segment_dirty, resolves
        // its Notifier at the write, never fsyncs.
        j.append(1, 0, b"first").unwrap().wait().unwrap();

        // keep_seq(0) is below the only segment's base_seq(1) — the `None`
        // arm of `apply_truncate_to_segments`, which empties `st.segments`.
        j.truncate_after(0).unwrap().wait().unwrap();
        assert_eq!(j.last_seq(), None);

        // The next append re-creates a segment via `need_new`'s
        // `segments.is_empty()` arm — pre-fix, this is exactly where the
        // writer thread used to panic on the stale flag.
        let notifier = j.append(1, 0, b"second").unwrap();
        // Bounded wait on a background thread: pre-fix, the writer thread
        // panicked WHILE holding the state lock and never completed this
        // Notifier, so a direct `notifier.wait()` would hang the whole test
        // suite rather than just failing this test. A real timeout turns a
        // hang into a clean, fast test failure instead.
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(notifier.wait());
        });
        let result = rx.recv_timeout(std::time::Duration::from_secs(10)).expect(
            "wait() must not hang — the writer thread must not have panicked \
             on the stale active_segment_dirty flag",
        );
        result.unwrap();

        assert_eq!(j.last_seq(), Some(1));
        drop(j);

        // The new record must be durable and readable after a reopen.
        let j2 = Journal::open(cfg).unwrap();
        assert_eq!(j2.last_seq(), Some(1));
        assert_eq!(j2.read(1).unwrap().unwrap().1, b"second".to_vec());
    }

    /// Appends still queued in the writer channel when `truncate_after` runs
    /// must not be written afterwards: they would resurrect truncated seqs,
    /// produce non-monotonic on-disk sequences, and shadow a re-appended
    /// entry at the same seq with the stale payload.
    #[test]
    fn truncated_queued_appends_do_not_resurrect() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();

        for round in 0..200u64 {
            let base = round * 100;
            // Burst without waiting so some requests are still queued when
            // the truncate runs.
            for s in (base + 1)..=(base + 20) {
                let _ = j.append(s, 0, format!("old{round}").as_bytes()).unwrap();
            }
            j.truncate_after(base + 10).unwrap().wait().unwrap();

            // Re-append at a truncated seq with a different payload.
            j.append(base + 11, 0, format!("new{round}").as_bytes())
                .unwrap()
                .wait()
                .unwrap();

            let (_, p) = j.read(base + 11).unwrap().unwrap();
            assert_eq!(
                p,
                format!("new{round}").as_bytes(),
                "round {round}: stale queued append shadowed the re-appended entry"
            );
            let tail = j.read_range(base + 12..).unwrap();
            assert!(
                tail.is_empty(),
                "round {round}: truncated seqs resurrected: {:?}",
                tail.iter().map(|t| t.0).collect::<Vec<_>>()
            );
        }

        // On-disk state after reopen must be strictly ascending.
        drop(j);
        let j2 = Journal::open(JournalConfig::new(dir.path())).unwrap();
        let v = j2.read_range(..).unwrap();
        for w in v.windows(2) {
            assert!(
                w[0].0 < w[1].0,
                "non-monotonic on-disk seqs after reopen: {} then {}",
                w[0].0,
                w[1].0
            );
        }
    }

    /// A failed fsync must halt the journal (fail-stop): the failed batch
    /// errors, subsequent appends are refused, and the durable watermark
    /// never advances past the failure — a later successful fsync cannot
    /// retroactively make the lost bytes durable (the classic fsync-retry
    /// hazard).
    #[test]
    fn fsync_failure_halts_writer() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(1, 0, b"x").unwrap().wait().unwrap();
        j.wait_durable(1).unwrap();

        j.state.lock().unwrap().fail_next_fsync = true;
        let n = j.append(2, 0, b"y").unwrap();
        assert!(
            n.wait().is_err(),
            "append covered by a failed fsync must report the failure"
        );

        // Fail-stop: the journal refuses further work...
        let halted = match j.append(3, 0, b"z") {
            Err(JournalError::Closed) => true,
            Err(other) => panic!("expected Closed, got {other:?}"),
            Ok(n) => n.wait().is_err(),
        };
        assert!(halted, "writer kept accepting work after an fsync failure");

        // ...and the watermark stays at the last truly durable seq.
        assert_eq!(j.durable_seq(), 1, "watermark advanced past a failed fsync");
    }

    #[test]
    fn preallocated_journal_rotates_and_recovers_full_range() {
        // With the flag on, write across several segments (small segment_size to
        // force rotations), wait durable, reopen, and confirm the full range.
        // Uses 512-byte payloads × 200 records ≈ 110 KiB > 64 KiB → forces ≥2 segments.
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.preallocate_segments = true;
        cfg.segment_size_bytes = 64 * 1024;
        let payload = vec![b'x'; 512];
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=200u64 {
                j.append(i, i, &payload).unwrap();
            }
            j.wait_durable(200).unwrap();
            assert_eq!(j.durable_seq(), 200);
        }
        let j2 = Journal::open(cfg).unwrap();
        assert_eq!(j2.first_seq(), Some(1));
        assert_eq!(j2.last_seq(), Some(200));
        // Confirm at least two segments were created (real rotation happened).
        let nseg = j2.state.lock().unwrap().segments.len();
        assert!(nseg >= 2, "expected ≥2 segments after rotation, got {nseg}");
    }

    #[test]
    fn open_removes_orphan_prealloc_temps() {
        let dir = tempfile::tempdir().unwrap();
        // Plant an orphan temp as if a crash hit between create and rename.
        std::fs::write(dir.path().join("seg-prealloc.9.tmp"), vec![0u8; 4096]).unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.preallocate_segments = true;
        cfg.segment_size_bytes = 64 * 1024;
        let _j = Journal::open(cfg).unwrap();
        assert!(
            !dir.path().join("seg-prealloc.9.tmp").exists(),
            "orphan temp cleaned at open"
        );
    }

    #[test]
    fn reopen_preallocated_active_segment_keeps_zero_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.preallocate_segments = true;
        cfg.segment_size_bytes = 64 * 1024;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=10u64 {
                j.append(i, i, b"x").unwrap();
            }
            j.wait_durable(10).unwrap();
        }
        let j2 = Journal::open(cfg).unwrap();
        assert_eq!(j2.first_seq(), Some(1));
        assert_eq!(j2.last_seq(), Some(10));
        // Active segment must be physically re-preallocated to segment_size, not
        // truncated to the logical end.
        let st = j2.state.lock().unwrap();
        let active = st.segments.last().unwrap();
        assert_eq!(
            active.physical_len().unwrap(),
            64 * 1024,
            "active segment re-preallocated after recovery"
        );
    }

    #[test]
    fn windowed_read_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        // Payloads large enough that the segment spans several 64 KiB index windows.
        let payload = vec![0x7Eu8; 20 * 1024];
        {
            let j = Journal::open(JournalConfig::new(&dir_path)).unwrap();
            for i in 1..=30u64 {
                j.append(i, i * 5, &payload).unwrap().wait().unwrap();
            }
        }
        // Reopen: the index is installed from the open-time scan, so reads take the
        // windowed path (not the empty-index fallback).
        let j2 = Journal::open(JournalConfig::new(&dir_path)).unwrap();
        // 30 × 20 KiB ≈ 600 KiB over a 64 KiB gap → ~9 index entries; >= 5 is a safe,
        // tighter lower bound that also guards against a SPARSE_INDEX_GAP regression.
        assert!(j2.state.lock().unwrap().segments[0].index_snapshot().len() >= 5);
        for i in [1u64, 7, 15, 23, 30] {
            let (meta, p) = j2.read(i).unwrap().unwrap();
            assert_eq!(meta, i * 5);
            assert_eq!(p, payload);
        }
        assert!(j2.read(1000).unwrap().is_none()); // out of range
    }

    #[test]
    fn consistent_durability_survives_reopen_preallocated() {
        // The flag-on twin of consistent_durability_survives_reopen: fdatasync into
        // a preallocated segment must still make every acked record durable.
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.durability = crate::Durability::Consistent;
        cfg.preallocate_segments = true;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=64u64 {
                j.append(i, i, format!("rec-{i}").as_bytes()).unwrap();
            }
            j.wait_durable(64).unwrap();
            assert_eq!(j.durable_seq(), 64);
        }
        let j2 = Journal::open(cfg).unwrap();
        assert_eq!(j2.first_seq(), Some(1));
        assert_eq!(j2.last_seq(), Some(64));
    }

    #[test]
    fn preallocated_resume_overwrites_zero_tail_correctly() {
        // Reopen a preallocated journal and append MORE: the writer must resume at
        // the logical cursor and overwrite the zero tail, and the combined range
        // must survive a second reopen.
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = JournalConfig::new(dir.path());
        cfg.preallocate_segments = true;
        cfg.segment_size_bytes = 64 * 1024;
        {
            let j = Journal::open(cfg.clone()).unwrap();
            for i in 1..=20u64 {
                j.append(i, i, b"a").unwrap();
            }
            j.wait_durable(20).unwrap();
        }
        {
            let j = Journal::open(cfg.clone()).unwrap();
            assert_eq!(j.last_seq(), Some(20));
            for i in 21..=40u64 {
                j.append(i, i, b"b").unwrap();
            }
            j.wait_durable(40).unwrap();
        }
        let j3 = Journal::open(cfg).unwrap();
        assert_eq!(j3.first_seq(), Some(1));
        assert_eq!(j3.last_seq(), Some(40));
        // Spot-check a record written into the overwritten zero tail.
        let (m, p) = j3.read(30).unwrap().unwrap();
        assert_eq!(m, 30);
        assert_eq!(p, b"b");
    }
}
