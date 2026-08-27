// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Position-tagged on-disk snapshot files (M6 Task 3). [`SnapshotStore`] is the
//! ONLY piece that touches the filesystem for snapshots: files live at
//! `instance_dir/snapshots/snap-<pos>.ultsnap`, `pos` being the absolute byte
//! position (the artifact tag `S`, per [`crate::SnapshotStateMachine`]'s
//! position-as-version convention) the artifact was frozen at.
//!
//! **Atomicity.** [`SnapshotStore::publish`] writes to a temp file, `fsync`s it,
//! then atomically renames it onto the final `snap-<pos>.ultsnap` name. The temp
//! file's name never matches the `snap-<pos>.ultsnap` pattern
//! [`SnapshotStore::newest`] scans for, so a build that dies mid-write (before
//! the rename) leaves, at worst, an orphaned temp file that is never mistaken
//! for a complete snapshot — the rename is the single moment a snapshot becomes
//! "complete" and discoverable. This is also why the cnc marker (this FSM's
//! slot `snapshot_pos`, written by the builder agent, not this module) is
//! updated only AFTER `publish` returns `Ok`.
//!
//! **Retention.** After a successful publish, `publish` unlinks every snapshot
//! file except the newest 2 (by position) — keep-newest-2, not keep-last-N-by-
//! mtime, so retention is correct even if publishes ever raced (they don't:
//! the builder thread is single-threaded and one-in-flight, but the retention
//! rule itself doesn't depend on that for correctness).

use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::config::SnapshotError;

const DIR_NAME: &str = "snapshots";
const PREFIX: &str = "snap-";
const SUFFIX: &str = ".ultsnap";

/// Owns the `instance_dir/snapshots` directory: position-tagged file naming,
/// atomic publish, and keep-newest-2 retention. Cheap to construct — no open
/// file handles are held between calls.
///
/// `Clone` is cheap (a `PathBuf`) and lets the builder thread and the apply
/// thread's reconstruction path (M6 Task 5) each hold one over the same dir.
#[derive(Clone)]
pub struct SnapshotStore {
    dir: PathBuf,
}

impl SnapshotStore {
    /// Open (creating if absent) the snapshot directory under `instance_dir`.
    pub fn open(instance_dir: &Path) -> io::Result<SnapshotStore> {
        let dir = instance_dir.join(DIR_NAME);
        std::fs::create_dir_all(&dir)?;
        Ok(SnapshotStore { dir })
    }

    /// The path a complete snapshot at `pos` lives (or would live) at.
    pub fn path_for(&self, pos: u64) -> PathBuf {
        self.dir.join(format!("{PREFIX}{pos}{SUFFIX}"))
    }

    fn tmp_path_for(&self, pos: u64) -> PathBuf {
        // Deliberately does NOT match `parse_snap_pos`'s pattern (extra
        // `.tmp` suffix past `.ultsnap`), so a leftover temp file from a
        // crashed build is invisible to `newest`'s directory scan without
        // needing any special-casing there.
        self.dir.join(format!("{PREFIX}{pos}{SUFFIX}.tmp"))
    }

    /// The newest COMPLETE snapshot with position `<= at_most` (pass
    /// `u64::MAX` for "any"), or `None` if no complete snapshot qualifies.
    /// Ignores anything in the directory that doesn't match the
    /// `snap-<pos>.ultsnap` pattern (in particular, in-progress `.tmp` files).
    pub fn newest(&self, at_most: u64) -> io::Result<Option<(u64, PathBuf)>> {
        let mut best: Option<(u64, PathBuf)> = None;
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(pos) = name.to_str().and_then(parse_snap_pos) else { continue };
            if pos > at_most {
                continue;
            }
            if best.as_ref().is_none_or(|(best_pos, _)| pos > *best_pos) {
                best = Some((pos, entry.path()));
            }
        }
        Ok(best)
    }

    /// Write a new snapshot tagged at `pos`: `write` streams into a temp file,
    /// which is `fsync`'d then atomically renamed onto `snap-<pos>.ultsnap`
    /// (module doc). Then retention drops every snapshot file except the
    /// newest 2 (by position). Returns the final path on success.
    ///
    /// On a `write` failure (or an I/O error at any step before the rename),
    /// the temp file is best-effort unlinked and the error is returned — the
    /// final `snap-<pos>.ultsnap` name is never created, so a partial/torn
    /// attempt is never visible to `newest`. The caller (the builder agent)
    /// logs and drops; the marker is not advanced, and the next policy
    /// interval retries with a fresh attempt.
    pub fn publish(
        &self,
        pos: u64,
        write: impl FnOnce(&mut dyn Write) -> Result<(), SnapshotError>,
    ) -> Result<PathBuf, SnapshotError> {
        let tmp_path = self.tmp_path_for(pos);
        let result = (|| -> Result<(), SnapshotError> {
            let mut f = File::create(&tmp_path)?;
            write(&mut f)?;
            f.sync_all()?;
            Ok(())
        })();
        if let Err(e) = result {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }
        let final_path = self.path_for(pos);
        std::fs::rename(&tmp_path, &final_path)?;
        self.retain_newest(2)?;
        Ok(final_path)
    }

    /// Unlink every complete snapshot file except the `keep` newest (by
    /// position). Best-effort per file: a removal race (the file already gone)
    /// is not an error here — nothing else in this single-writer module
    /// deletes snapshot files, but tolerating a `NotFound` keeps this robust
    /// against, say, an operator manually clearing the directory.
    fn retain_newest(&self, keep: usize) -> io::Result<()> {
        let mut all: Vec<(u64, PathBuf)> = std::fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name();
                let pos = name.to_str().and_then(parse_snap_pos)?;
                Some((pos, e.path()))
            })
            .collect();
        all.sort_by_key(|(pos, _)| std::cmp::Reverse(*pos));
        for (_, path) in all.into_iter().skip(keep) {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// Parse `snap-<pos>.ultsnap` -> `pos`. Anything else (a `.tmp` in-progress
/// file, a foreign file an operator dropped in, a malformed number) is `None`
/// and silently ignored by both `newest` and `retain_newest`.
fn parse_snap_pos(file_name: &str) -> Option<u64> {
    file_name.strip_prefix(PREFIX)?.strip_suffix(SUFFIX)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_write(bytes: &'static [u8]) -> impl FnOnce(&mut dyn Write) -> Result<(), SnapshotError> {
        move |w| {
            w.write_all(bytes)?;
            Ok(())
        }
    }

    #[test]
    fn publish_creates_the_pinned_file_name_and_newest_finds_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        let path = store.publish(4096, ok_write(b"hello")).unwrap();
        assert_eq!(path, store.path_for(4096));
        assert!(path.ends_with("snap-4096.ultsnap"));
        assert_eq!(std::fs::read(&path).unwrap(), b"hello");

        let (pos, found) = store.newest(u64::MAX).unwrap().expect("published file exists");
        assert_eq!(pos, 4096);
        assert_eq!(found, path);
    }

    /// The atomicity pin: a `write` closure that fails partway through must
    /// never leave anything `newest` can see — the temp file is unlinked and
    /// the final name was never created.
    #[test]
    fn a_failed_write_never_becomes_newest_and_leaves_no_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        let result = store.publish(100, |w| {
            w.write_all(b"partial").ok();
            Err(SnapshotError::Codec("boom".into()))
        });
        assert!(result.is_err());
        assert!(store.newest(u64::MAX).unwrap().is_none(), "a torn build is never `newest`");
        assert!(!store.path_for(100).exists(), "final name was never created");
        // No leftover temp file either (best-effort cleanup on failure).
        let entries: Vec<_> = std::fs::read_dir(dir.path().join("snapshots"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(entries.is_empty(), "no stray temp file after a failed publish: {entries:?}");
    }

    /// A `.tmp` file placed directly in the directory (simulating a process
    /// that died between `File::create` and the rename, in an older or
    /// different-cleanup implementation) is never mistaken for a complete
    /// snapshot.
    #[test]
    fn a_bare_temp_file_on_disk_is_never_newest() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        std::fs::write(dir.path().join("snapshots").join("snap-500.ultsnap.tmp"), b"torn").unwrap();
        assert!(store.newest(u64::MAX).unwrap().is_none());
        assert!(store.newest(500).unwrap().is_none());
    }

    #[test]
    fn retention_keeps_only_the_newest_two() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        for pos in [100u64, 200, 300, 400] {
            store.publish(pos, ok_write(b"x")).unwrap();
        }
        let mut remaining: Vec<u64> = std::fs::read_dir(dir.path().join("snapshots"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| parse_snap_pos(&e.file_name().to_string_lossy()))
            .collect();
        remaining.sort_unstable();
        assert_eq!(remaining, vec![300, 400], "keep-newest-2, oldest two unlinked");
    }

    /// `newest(at_most)` picks the right one among a sparse set, per the
    /// brief's exact example: {snap-100, snap-900}.
    #[test]
    fn newest_at_most_picks_correctly_among_a_sparse_set() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path()).unwrap();
        store.publish(100, ok_write(b"a")).unwrap();
        store.publish(900, ok_write(b"b")).unwrap();

        assert_eq!(store.newest(u64::MAX).unwrap().map(|(p, _)| p), Some(900));
        assert_eq!(store.newest(500).unwrap().map(|(p, _)| p), Some(100));
        assert_eq!(store.newest(900).unwrap().map(|(p, _)| p), Some(900));
        assert_eq!(store.newest(99).unwrap(), None, "nothing qualifies below the oldest");
    }

    #[test]
    fn open_creates_the_directory_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!dir.path().join("snapshots").exists());
        let _store = SnapshotStore::open(dir.path()).unwrap();
        assert!(dir.path().join("snapshots").is_dir());
        // Reopening (e.g. a fresh service incarnation attaching again) must
        // not fail on an already-existing directory.
        let _store2 = SnapshotStore::open(dir.path()).unwrap();
    }
}
