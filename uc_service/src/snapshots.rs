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
//! **The envelope** (coordinated-snapshot ruling P6). Every file this module
//! writes starts with 16 framework-owned bytes — [`SNAPSHOT_ENVELOPE_MAGIC`]
//! then the position `P` it was built at, LE — ahead of the state machine's
//! own bytes. UC prescribes no payload encoding, but it does own the header,
//! and that is what makes a MIS-TAGGED artifact detectable: the file name is
//! just a name (a `uc2ctl restore` of a mis-copied backup, or any rename, can
//! make an artifact built at `P0` claim `P`), and installing an older image
//! under a newer tag leaves a silent state gap — the exact bug class the
//! reconstruction gap guard exists to prevent. The SM's own payload-position
//! check cannot catch it, because the tag is an EXCLUSIVE frontier and the
//! payload's cursor legitimately sits below it (see
//! [`crate::SnapshotStateMachine::install_snapshot`]). So the framework checks
//! the envelope on every install path, and the SM's check stays as
//! belt-and-suspenders.
//!
//! **Retention.** `publish` does NOT prune (coordinated-snapshot spec §5.3,
//! ruling P1): only the NODE can see which artifacts form a complete SET at an
//! instant, so it owns retention — keeping the newest complete set plus
//! anything newer. A per-writer keep-newest-N here would happily delete the
//! artifact at the floor once two later instants were abandoned, and the ship
//! gate would then decline `MISSING` forever. [`SnapshotStore::retain_newest`]
//! remains for the node-side pruner.

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::config::SnapshotError;

const DIR_NAME: &str = "snapshots";
const PREFIX: &str = "snap-";
const SUFFIX: &str = ".ultsnap";

/// The framework-owned artifact header: 8 magic bytes then `P` as `u64` LE.
/// Written by [`SnapshotStore::publish`], stripped and verified by every
/// install path (module doc, ruling P6).
pub const SNAPSHOT_ENVELOPE_LEN: usize = 16;

/// The envelope's magic. `1` is the envelope's own layout version — the
/// artifact's PAYLOAD is versioned by the state machine, never by UC.
pub const SNAPSHOT_ENVELOPE_MAGIC: &[u8; 8] = b"ULTSNAP1";

/// Why an artifact's 16-byte envelope did not verify. All three are refusals,
/// never silent: an artifact that fails any of them is not installed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
    /// Fewer than [`SNAPSHOT_ENVELOPE_LEN`] bytes — a truncated or empty file
    /// (or one written by something that is not UC).
    #[error(
        "truncated artifact: {0} bytes, need {len} for the envelope",
        len = SNAPSHOT_ENVELOPE_LEN
    )]
    Short(usize),
    /// The first 8 bytes are not [`SNAPSHOT_ENVELOPE_MAGIC`].
    #[error("not a UC snapshot artifact: magic {0:02x?}, expected {SNAPSHOT_ENVELOPE_MAGIC:?}")]
    BadMagic([u8; 8]),
    /// The envelope verified but names a DIFFERENT instant than the caller
    /// asked to land at — a renamed, mis-copied or stale artifact. Installing
    /// it would leave every frame in `(built, presented)` unapplied.
    #[error("artifact was built at position {built} but is presented as {presented}")]
    Mistagged { built: u64, presented: u64 },
}

/// Decode the 16-byte envelope at the head of an artifact, returning the
/// position it was built at. A **pure** decoder — no I/O, no allocation, total
/// on any slice (fuzz target `uc_service_snapshot_envelope`).
pub fn decode_snapshot_envelope(bytes: &[u8]) -> Result<u64, EnvelopeError> {
    let Some(head) = bytes.get(..SNAPSHOT_ENVELOPE_LEN) else {
        return Err(EnvelopeError::Short(bytes.len()));
    };
    let magic: [u8; 8] = head[..8].try_into().expect("8 bytes");
    if &magic != SNAPSHOT_ENVELOPE_MAGIC {
        return Err(EnvelopeError::BadMagic(magic));
    }
    Ok(u64::from_le_bytes(head[8..16].try_into().expect("8 bytes")))
}

/// Write the envelope for an artifact built at `pos`.
pub fn write_snapshot_envelope(dst: &mut dyn Write, pos: u64) -> io::Result<()> {
    dst.write_all(SNAPSHOT_ENVELOPE_MAGIC)?;
    dst.write_all(&pos.to_le_bytes())
}

/// Read the envelope off the front of `src` and check it names `expected`,
/// leaving the reader positioned at the state machine's first payload byte —
/// the one call every install path makes before handing the stream to
/// [`crate::SnapshotStateMachine::install_snapshot`].
///
/// Reads with a short-read loop rather than `read_exact` so a truncated file
/// is [`EnvelopeError::Short`] (a named refusal) instead of an opaque
/// `UnexpectedEof`.
pub fn verify_snapshot_envelope(src: &mut dyn Read, expected: u64) -> Result<(), EnvelopeError> {
    let mut buf = [0u8; SNAPSHOT_ENVELOPE_LEN];
    let mut n = 0usize;
    while n < SNAPSHOT_ENVELOPE_LEN {
        match src.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            // An I/O error mid-header is indistinguishable from a short file
            // for this decision, and both are refusals.
            Err(_) => break,
        }
    }
    let built = decode_snapshot_envelope(&buf[..n])?;
    if built != expected {
        return Err(EnvelopeError::Mistagged {
            built,
            presented: expected,
        });
    }
    Ok(())
}

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
    /// Open (creating if absent) `snapshots/<service_id>/` under `instance_dir`.
    pub fn open(instance_dir: &Path, service_id: u8) -> io::Result<SnapshotStore> {
        let dir = instance_dir.join(DIR_NAME).join(service_id.to_string());
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
            let Some(pos) = name.to_str().and_then(parse_snap_pos) else {
                continue;
            };
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
            // Ruling P6: the framework's 16 bytes go first, ahead of the state
            // machine's own. This is the ONE write site for a
            // `snap-<pos>.ultsnap`, so "every artifact on disk carries an
            // envelope naming its instant" is an invariant of this module
            // rather than a convention its callers have to remember.
            write_snapshot_envelope(&mut f, pos)?;
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
        // Coordinated-snapshot spec §5.3 (plan-2 ruling P1): retention is
        // NODE-owned. The node keeps the newest COMPLETE set plus anything
        // newer and deletes the rest; a per-writer "newest 2" pruner here
        // cannot see sets, so two abandoned instants after a complete set at
        // P would delete P — and the ship gate ("the complete set at my
        // floor") would then decline MISSING forever. `retain_newest` stays
        // for the node-side pruner (Task 5) to reuse.
        Ok(final_path)
    }

    /// Unlink every complete snapshot file except the `keep` newest (by
    /// position). Best-effort per file: a removal race (the file already gone)
    /// is not an error here — nothing else in this single-writer module
    /// deletes snapshot files, but tolerating a `NotFound` keeps this robust
    /// against, say, an operator manually clearing the directory.
    #[cfg_attr(not(test), allow(dead_code))]
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
    file_name
        .strip_prefix(PREFIX)?
        .strip_suffix(SUFFIX)?
        .parse()
        .ok()
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

    /// Ruling P6: the pure decoder is total, and every refusal is named.
    #[test]
    fn the_envelope_decodes_round_trip_and_refuses_short_bad_magic_and_a_mis_tag() {
        let mut buf = Vec::new();
        write_snapshot_envelope(&mut buf, 4096).unwrap();
        assert_eq!(buf.len(), SNAPSHOT_ENVELOPE_LEN);
        assert_eq!(decode_snapshot_envelope(&buf), Ok(4096));

        assert_eq!(decode_snapshot_envelope(&[]), Err(EnvelopeError::Short(0)));
        assert_eq!(
            decode_snapshot_envelope(&buf[..15]),
            Err(EnvelopeError::Short(15))
        );
        let mut bad = buf.clone();
        bad[0] ^= 0xFF;
        assert!(matches!(
            decode_snapshot_envelope(&bad),
            Err(EnvelopeError::BadMagic(_))
        ));

        // The case the envelope exists for: an artifact built at 4096 renamed
        // to claim a later instant.
        let mut src = buf.as_slice();
        assert_eq!(
            verify_snapshot_envelope(&mut src, 8192),
            Err(EnvelopeError::Mistagged {
                built: 4096,
                presented: 8192
            })
        );
        // And a trailing payload is left for the state machine, untouched.
        let mut with_payload = buf.clone();
        with_payload.extend_from_slice(b"sm bytes");
        let mut src = with_payload.as_slice();
        verify_snapshot_envelope(&mut src, 4096).unwrap();
        assert_eq!(src, b"sm bytes");
    }

    #[test]
    fn publish_creates_the_pinned_file_name_and_newest_finds_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        let path = store.publish(4096, ok_write(b"hello")).unwrap();
        assert_eq!(path, store.path_for(4096));
        assert!(path.ends_with("snap-4096.ultsnap"));
        // Ruling P6: the file is UC's 16-byte envelope, then the SM's bytes.
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(&raw[SNAPSHOT_ENVELOPE_LEN..], b"hello");
        assert_eq!(decode_snapshot_envelope(&raw), Ok(4096));
        let mut src = raw.as_slice();
        verify_snapshot_envelope(&mut src, 4096).expect("verifies at its own P");
        assert_eq!(src, b"hello", "the reader is left at the payload");

        let (pos, found) = store
            .newest(u64::MAX)
            .unwrap()
            .expect("published file exists");
        assert_eq!(pos, 4096);
        assert_eq!(found, path);
    }

    /// The atomicity pin: a `write` closure that fails partway through must
    /// never leave anything `newest` can see — the temp file is unlinked and
    /// the final name was never created.
    #[test]
    fn a_failed_write_never_becomes_newest_and_leaves_no_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        let result = store.publish(100, |w| {
            w.write_all(b"partial").ok();
            Err(SnapshotError::Codec("boom".into()))
        });
        assert!(result.is_err());
        assert!(
            store.newest(u64::MAX).unwrap().is_none(),
            "a torn build is never `newest`"
        );
        assert!(
            !store.path_for(100).exists(),
            "final name was never created"
        );
        // No leftover temp file either (best-effort cleanup on failure).
        let entries: Vec<_> = std::fs::read_dir(dir.path().join("snapshots").join("0"))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            entries.is_empty(),
            "no stray temp file after a failed publish: {entries:?}"
        );
    }

    /// A `.tmp` file placed directly in the directory (simulating a process
    /// that died between `File::create` and the rename, in an older or
    /// different-cleanup implementation) is never mistaken for a complete
    /// snapshot.
    #[test]
    fn a_bare_temp_file_on_disk_is_never_newest() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        std::fs::write(
            dir.path()
                .join("snapshots")
                .join("0")
                .join("snap-500.ultsnap.tmp"),
            b"torn",
        )
        .unwrap();
        assert!(store.newest(u64::MAX).unwrap().is_none());
        assert!(store.newest(500).unwrap().is_none());
    }

    /// Coordinated-snapshot ruling P1: `publish` no longer prunes — retention
    /// is node-owned, because only the node can see which artifacts form a
    /// COMPLETE set. `retain_newest` itself is unchanged and still keeps the
    /// newest `keep`; the node-side pruner (Task 5) is its next caller.
    #[test]
    fn publish_never_prunes_and_retain_newest_still_keeps_the_newest_two() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        for pos in [100u64, 200, 300, 400] {
            store.publish(pos, ok_write(b"x")).unwrap();
        }
        let on_disk = |()| -> Vec<u64> {
            let mut v: Vec<u64> = std::fs::read_dir(dir.path().join("snapshots").join("0"))
                .unwrap()
                .filter_map(|e| e.ok())
                .filter_map(|e| parse_snap_pos(&e.file_name().to_string_lossy()))
                .collect();
            v.sort_unstable();
            v
        };
        assert_eq!(
            on_disk(()),
            vec![100, 200, 300, 400],
            "publish keeps every artifact: the node decides what is garbage"
        );
        store.retain_newest(2).unwrap();
        assert_eq!(
            on_disk(()),
            vec![300, 400],
            "keep-newest-2, oldest two unlinked"
        );
    }

    /// `newest(at_most)` picks the right one among a sparse set, per the
    /// brief's exact example: {snap-100, snap-900}.
    #[test]
    fn newest_at_most_picks_correctly_among_a_sparse_set() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        store.publish(100, ok_write(b"a")).unwrap();
        store.publish(900, ok_write(b"b")).unwrap();

        assert_eq!(store.newest(u64::MAX).unwrap().map(|(p, _)| p), Some(900));
        assert_eq!(store.newest(500).unwrap().map(|(p, _)| p), Some(100));
        assert_eq!(store.newest(900).unwrap().map(|(p, _)| p), Some(900));
        assert_eq!(
            store.newest(99).unwrap(),
            None,
            "nothing qualifies below the oldest"
        );
    }

    #[test]
    fn open_creates_the_directory_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!dir.path().join("snapshots").exists());
        let _store = SnapshotStore::open(dir.path(), 3).unwrap();
        assert!(dir.path().join("snapshots").join("3").is_dir());
        // Reopening (e.g. a fresh service incarnation attaching again) must
        // not fail on an already-existing directory.
        let _store2 = SnapshotStore::open(dir.path(), 3).unwrap();
    }
}
