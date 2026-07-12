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
//!   pre-truncation stale read is bounded below.
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
        mut visit: impl FnMut(u64, u64, &[u8]) -> bool,
    ) -> Result<(), JournalError> {
        // Re-list on every call: filenames `seg-{seq:020}.log` sort in seq
        // order (the same ordering `Journal::open` relies on). Orphan
        // preallocation temps (`seg-prealloc.*.tmp`) and the `truncate.intent`
        // file are excluded by the extension filter — the reader never touches
        // them.
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

        for path in paths {
            // A segment can be unlinked between the listing and the open (a
            // concurrent purge); skip a vanished file rather than error.
            let seg = match SegmentFile::open_for_read(&path) {
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
