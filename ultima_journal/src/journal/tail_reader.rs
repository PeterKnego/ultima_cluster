// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! [`TailReader`] — a strictly READ-ONLY, concurrent-safe view over a journal
//! directory, for a follower that must reconstruct state from the archived log
//! while the owning node's writer keeps appending (UC v2 M5, task14 semantics).
//!
//! # Safety argument (why this is sound against a live writer)
//!
//! A `TailReader` never takes the journal's `WriterState` lock, never mutates a
//! segment, never completes a `truncate.intent`, and never spawns a writer. It
//! opens each `seg-*.log` file with [`SegmentFile::open_for_read`] and does a
//! non-mutating [`SegmentFile::scan_tolerant`] over it, re-listing the directory
//! on every [`TailReader::scan`] call so newly-rotated segments appear.
//!
//! Concurrency is handled by conservative truncation of the visible tail:
//!
//! * A record whose bytes are only PARTIALLY flushed by a concurrent writer
//!   presents either a zero length-prefix (the record isn't there yet →
//!   [`decode_record`] returns `Ok(None)`) or a valid length prefix over a
//!   not-yet-visible (zeroed) body/CRC (→ `Err`). `scan_tolerant` treats BOTH
//!   as end-of-scan, so the reader simply sees a slightly-stale durable
//!   frontier. It never parses garbage as a record, and the writer will finish
//!   the record for a later re-scan to pick up.
//! * A preallocated segment's zero tail is the same `Ok(None)` case.
//! * A segment UNLINKED under the reader (a concurrent purge / truncate) stays
//!   fully readable through the already-open fd; the directory re-listing on the
//!   NEXT `scan` simply won't include it. A partially-applied truncate leaves a
//!   `truncate.intent` file, which the reader deliberately IGNORES (it is a
//!   `.intent`, not a `seg-*.log`) — completing it is the writer's job, and a
//!   pre-truncation stale read is bounded below. `Journal::purge_before` (M6)
//!   makes this unlink-under-scan case REAL rather than theoretical: a segment
//!   listed by [`scan_from`](TailReader::scan_from)'s directory read can vanish
//!   before its `open_for_read` (or its first-record probe). Both the main scan
//!   loop and the skip-probe treat that `NotFound` as "not there" — the scan
//!   continues past a vanished file and the probe declines to skip — so a
//!   concurrent purge can never break a scan, only make it observe a slightly
//!   higher floor.
//! * Truncation sentinel records (a `truncate_after` in progress) are skipped
//!   (`segment::is_sentinel`).
//!
//! Why stale-above-commit reads are never wrong: the tail reader's callers bound
//! every APPLIED byte by the cluster commit counter (the M5 replay only applies
//! frames whose end `<= min(commit, durable)`), and committed bytes are never
//! truncated (spec inv4). So a frontier that momentarily includes a byte the
//! writer is about to truncate is harmless — that byte is by construction
//! uncommitted, and the caller will not apply it.

use std::path::{Path, PathBuf};

use crate::JournalError;

use super::segment::{self, SegmentFile};

/// A read-only cursor over a journal directory's archived records. Cheap to
/// hold open (it stores only the directory path); the actual segment fds are
/// opened per [`scan`](TailReader::scan) call and dropped when it returns.
pub struct TailReader {
    dir: PathBuf,
}

impl TailReader {
    /// Open a read-only view of the journal directory at `dir`. Does no I/O
    /// beyond confirming `dir` is a directory; the segment files are opened
    /// lazily on each [`scan`](TailReader::scan).
    pub fn open(dir: &Path) -> Result<TailReader, JournalError> {
        if !dir.is_dir() {
            return Err(JournalError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("journal dir not found: {}", dir.display()),
            )));
        }
        Ok(TailReader { dir: dir.to_path_buf() })
    }

    /// Visit every archived record `(seq, meta, payload)` in seq order across
    /// all segments, RE-LISTING the directory on each call so a segment rotated
    /// or recovered since the last call is included. `visit` returns `false` to
    /// stop early (the scan returns `Ok(())`). Truncation sentinel records are
    /// skipped. A partially-written / corrupt record conservatively ends the
    /// scan (see the module safety argument).
    pub fn scan(
        &self,
        visit: impl FnMut(u64, u64, &[u8]) -> bool,
    ) -> Result<(), JournalError> {
        // scan(v) is exactly scan_from(0, v): with start_meta 0 no segment can
        // be skipped (only the first block has meta 0, and a skip needs the NEXT
        // segment's first meta <= 0), so every record is visited in seq order.
        self.scan_from(0, visit)
    }

    /// Like [`scan`](TailReader::scan), but skips whole segment FILES whose
    /// records all end at or below `start_meta`, decided by each segment's
    /// first-record meta vs the NEXT segment's first-record meta — O(#segments)
    /// bounded probes, never O(bytes). `visit` still receives EVERY record of
    /// the first relevant segment onward (the covering segment is always
    /// yielded), so callers keep their own per-record skip. `scan(v) ==
    /// scan_from(0, v)`.
    ///
    /// Concurrent-safe exactly like `scan`: a segment (or its probe target)
    /// unlinked by a concurrent purge is treated as absent — the scan continues
    /// past it and the skip-probe declines to skip — so a live purge can never
    /// break the scan (see the module safety argument).
    pub fn scan_from(
        &self,
        start_meta: u64,
        mut visit: impl FnMut(u64, u64, &[u8]) -> bool,
    ) -> Result<(), JournalError> {
        let paths = self.segment_paths()?;

        // Find the first segment to scan: the COVERING segment (the last one
        // whose first record meta <= start_meta). Skip segment file `i` only
        // when file `i+1`'s first record meta <= start_meta — then every record
        // of file `i` is strictly below that, hence below start_meta, and file
        // `i` is entirely skippable. Stop at the first `i+1` whose first meta
        // exceeds start_meta OR cannot be probed (vanished / torn head): the
        // conservative choice is to NOT skip further, so the covering segment is
        // always retained.
        let mut start_idx = 0usize;
        for i in 0..paths.len() {
            if i + 1 >= paths.len() {
                break; // never skip the last (active) segment
            }
            match Self::probe_first_meta(&paths[i + 1])? {
                Some(next_first) if next_first <= start_meta => start_idx = i + 1,
                _ => break,
            }
        }

        for path in &paths[start_idx..] {
            // A segment can be unlinked between the listing and the open (a
            // concurrent purge); skip a vanished file rather than error.
            let seg = match SegmentFile::open_for_read(path) {
                Ok(seg) => seg,
                Err(JournalError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            let scan = seg.scan_tolerant()?;
            for rec in &scan.records {
                if segment::is_sentinel(rec) {
                    continue;
                }
                if !visit(rec.seq, rec.meta, &rec.payload) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// The `meta` of the first readable record across all segments (the
    /// archive's first block base) — `None` if the journal is empty or fully
    /// purged. Concurrent-safe like [`scan`](TailReader::scan): probes segments
    /// in seq order and returns the first readable first-record meta, skipping a
    /// segment whose head is vanished/torn.
    pub fn first_meta(&self) -> Result<Option<u64>, JournalError> {
        for path in self.segment_paths()? {
            if let Some(meta) = Self::probe_first_meta(&path)? {
                return Ok(Some(meta));
            }
        }
        Ok(None)
    }

    /// The sorted `seg-*.log` paths in `dir`, re-listed per call so segments
    /// rotated (or purged) since the last call are reflected. Orphan
    /// preallocation temps (`seg-prealloc.*.tmp`) and the `truncate.intent`
    /// file are excluded by the extension filter — the reader never touches
    /// them. Filenames `seg-{seq:020}.log` sort in seq order.
    fn segment_paths(&self) -> Result<Vec<PathBuf>, JournalError> {
        let mut paths: Vec<PathBuf> = std::fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .map(|n| {
                        let s = n.to_string_lossy();
                        s.starts_with("seg-") && s.ends_with(".log")
                    })
                    .unwrap_or(false)
            })
            .collect();
        paths.sort();
        Ok(paths)
    }

    /// Probe a segment file's first-record meta without scanning it. A file
    /// unlinked by a concurrent purge between listing and open reads as
    /// `Ok(None)` (absent → not skippable / skipped over), matching the scan's
    /// `NotFound`-continue discipline.
    fn probe_first_meta(path: &Path) -> Result<Option<u64>, JournalError> {
        match SegmentFile::open_for_read(path) {
            Ok(seg) => seg.first_record_meta(),
            Err(JournalError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TailReader;
    use crate::journal::{Journal, JournalConfig};

    #[test]
    fn tail_reader_sees_records_while_writer_appends() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for s in 0..10 { j.append(s, s, &[7u8; 128]).unwrap().wait().unwrap(); }
        let r = TailReader::open(dir.path()).unwrap();
        let mut seqs = Vec::new();
        r.scan(|seq, _, _| { seqs.push(seq); true }).unwrap();
        assert_eq!(seqs, (0..10).collect::<Vec<_>>());
        j.append(10, 10, &[7u8; 128]).unwrap().wait().unwrap(); // writer still live
        let mut n = 0; r.scan(|_, _, _| { n += 1; true }).unwrap();
        assert_eq!(n, 11, "re-scan sees the new record");
    }

    #[test]
    fn scan_stops_early_when_visit_returns_false() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        for s in 0..5 { j.append(s, s * 10, b"x").unwrap().wait().unwrap(); }
        let r = TailReader::open(dir.path()).unwrap();
        let mut seen = Vec::new();
        r.scan(|seq, _, _| { seen.push(seq); seq < 2 }).unwrap();
        assert_eq!(seen, vec![0, 1, 2], "stops right after the visit returns false");
    }

    #[test]
    fn open_rejects_missing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope");
        assert!(TailReader::open(&missing).is_err());
    }

    /// Tiny segments (~8 records/segment) so a scan spans several files.
    fn small_segment_config(dir: &std::path::Path) -> JournalConfig {
        JournalConfig { segment_size_bytes: 736, ..JournalConfig::new(dir) }
    }

    #[test]
    fn scan_from_skips_leading_segments_but_yields_the_covering_one() {
        let dir = tempfile::tempdir().unwrap();
        // tiny segments so multiple files exist: ~8 records/segment
        let j = Journal::open(small_segment_config(dir.path())).unwrap();
        for s in 0..40 { j.append(s, s * 100, &[7u8; 64]).unwrap().wait().unwrap(); }
        let r = TailReader::open(dir.path()).unwrap();
        let mut first_seen = None;
        r.scan_from(2_500, |seq, meta, _| { first_seen.get_or_insert((seq, meta)); true }).unwrap();
        let (seq, meta) = first_seen.unwrap();
        assert!(meta <= 2_500, "covering record yielded, not skipped");
        assert!(seq >= 8, "at least one leading segment file was skipped entirely");
        assert_eq!(r.first_meta().unwrap(), Some(0));
    }

    #[test]
    fn scan_from_zero_equals_scan() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(small_segment_config(dir.path())).unwrap();
        for s in 0..40 { j.append(s, s * 100, &[7u8; 64]).unwrap().wait().unwrap(); }
        let r = TailReader::open(dir.path()).unwrap();
        let mut via_scan = Vec::new();
        r.scan(|seq, meta, _| { via_scan.push((seq, meta)); true }).unwrap();
        let mut via_from = Vec::new();
        r.scan_from(0, |seq, meta, _| { via_from.push((seq, meta)); true }).unwrap();
        assert_eq!(via_scan, via_from);
        assert_eq!(via_scan.len(), 40);
    }

    #[test]
    fn first_meta_is_none_on_empty_journal() {
        let dir = tempfile::tempdir().unwrap();
        let _j = Journal::open(small_segment_config(dir.path())).unwrap();
        let r = TailReader::open(dir.path()).unwrap();
        assert_eq!(r.first_meta().unwrap(), None);
    }

    #[test]
    fn scan_meta_is_the_record_meta() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(JournalConfig::new(dir.path())).unwrap();
        j.append(3, 999, b"payload").unwrap().wait().unwrap();
        let r = TailReader::open(dir.path()).unwrap();
        let mut got = None;
        r.scan(|seq, meta, payload| { got = Some((seq, meta, payload.to_vec())); true }).unwrap();
        assert_eq!(got, Some((3, 999, b"payload".to_vec())));
    }
}
