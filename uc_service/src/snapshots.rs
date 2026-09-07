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
//! **Retention.** This module does NOT prune at all (coordinated-snapshot
//! spec §5.3, ruling P1): only the NODE can see which artifacts form a
//! complete SET at an instant, so it owns retention — keeping the set at its
//! floor plus anything newer, and unlinking everything below. A per-writer
//! keep-newest-N here would happily delete the artifact at the floor once two
//! later instants were abandoned, and the ship gate ("the complete set at my
//! floor") would then decline `MISSING` forever. The pruner is
//! `uc_node`'s `prune_snapshots_below`, which matches `snap-<pos>.ultsnap`
//! EXACTLY — never a `.tmp` this module may still be writing.

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
            // Final wave M9: `Interrupted` is not an error, it is a signal
            // arriving mid-`read`. Treating it as one turned a benign EINTR
            // into `Short`, then into a `MistaggedSnapshot` fail-stop on the
            // reconstruction path — a node refusing to start over a signal.
            // Retry it, as `read_exact` itself does.
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            // Any OTHER I/O error mid-header is indistinguishable from a short
            // file for this decision, and both are refusals.
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

/// Owns the `instance_dir/snapshots` directory: position-tagged file naming
/// and atomic publish. It does **not** retain or prune — the node owns set
/// retention (module doc, ruling P1). Cheap to construct — no open file
/// handles are held between calls.
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
    /// which is `fsync`'d, atomically renamed onto `snap-<pos>.ultsnap`
    /// (module doc), and then the DIRECTORY is `fsync`'d so the rename itself
    /// is durable. Nothing is pruned — the node's set retention decides what
    /// is garbage. Returns the final path on success.
    ///
    /// A directory-fsync failure is returned as a named `Io` error even though
    /// the rename has already happened: the artifact exists but is not
    /// provably durable, and the builder agent's "did not happen" handling
    /// (below) is the conservative answer.
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
        // I2 (final wave): fsync the artifact's DIRECTORY, so the rename that
        // published it is itself durable. The file's own bytes are fsync'd
        // above; without this a crash between the rename and the next
        // unrelated directory sync can leave a node whose DURABLE snapshot
        // floor names P (`maybe_persist_snapshot_floor` stores the floor, then
        // prunes below it, and under `PurgePolicy::BelowSnapshot` purges the
        // journal below P − slack) and whose `snap-<P>.ultsnap` never reached
        // the disk. `replay_into`'s gap guard then fail-stops
        // `SnapshotRequired` and the recovery is wipe-and-rejoin.
        //
        // This is the THIRD writer of these files and the last one to close
        // the gap; the other two are `uc_node::cluster_agent::take_snapshot`
        // (cluster artifact) and `uc_net::receiver`'s snapshot intake, both of
        // which fsync the directory best-effort and COUNT a failure because
        // they must not stall an agent loop. Here the caller is the builder
        // agent, which already treats a publish error as "this instant did not
        // happen" (logs, does not advance the marker, retries next interval),
        // so a failure is returned NAMED rather than swallowed: reporting an
        // artifact whose directory entry is not durable is exactly what the
        // fsync exists to prevent.
        if let Err(e) = File::open(&self.dir).and_then(|d| d.sync_all()) {
            return Err(SnapshotError::Io(io::Error::new(
                e.kind(),
                format!(
                    "snapshot directory fsync failed after publishing {}: {e}",
                    final_path.display()
                ),
            )));
        }
        // Coordinated-snapshot spec §5.3 (plan-2 ruling P1): retention is
        // NODE-owned. The node keeps the set at its floor plus anything newer
        // and deletes the rest; a per-writer "newest 2" pruner here cannot see
        // sets, so two abandoned instants after a complete set at P would
        // delete P — and the ship gate ("the complete set at my floor") would
        // then decline MISSING forever.
        Ok(final_path)
    }
}

/// Parse `snap-<pos>.ultsnap` -> `pos`. Anything else (a `.tmp` in-progress
/// file, a foreign file an operator dropped in, a malformed number) is `None`
/// and silently ignored by `newest` — and by the node's own pruner, which
/// applies the identical rule to the same names.
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

    /// Final wave M9: an `Interrupted` mid-header is a signal arriving during
    /// a `read`, not a truncated file. Before the fix it became
    /// `EnvelopeError::Short`, which the reconstruction path turns into a
    /// `MistaggedSnapshot` fail-stop — a node refusing to start over an EINTR.
    #[test]
    fn an_interrupted_read_mid_envelope_is_retried_not_a_refusal() {
        struct EintrOnce<'a> {
            bytes: &'a [u8],
            fired: bool,
        }
        impl Read for EintrOnce<'_> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if !self.fired {
                    self.fired = true;
                    return Err(io::Error::from(io::ErrorKind::Interrupted));
                }
                // One byte at a time, so the loop is genuinely short-reading.
                if buf.is_empty() || self.bytes.is_empty() {
                    return Ok(0);
                }
                buf[0] = self.bytes[0];
                self.bytes = &self.bytes[1..];
                Ok(1)
            }
        }

        let mut raw = Vec::new();
        write_snapshot_envelope(&mut raw, 4096).unwrap();
        raw.extend_from_slice(b"payload");
        let mut src = EintrOnce {
            bytes: &raw,
            fired: false,
        };
        verify_snapshot_envelope(&mut src, 4096).expect("EINTR is retried, not a refusal");
        assert!(src.fired);
    }

    /// I2 (final wave): the rename that publishes an artifact must itself be
    /// durable, so a power loss between the build and the next unrelated
    /// directory sync cannot leave a node whose persisted snapshot FLOOR names
    /// P with no `snap-P.ultsnap` on disk (and, under
    /// `PurgePolicy::BelowSnapshot`, the journal prefix below P already gone).
    ///
    /// Watching it fail is the point of the mode: `0o300` (write + execute, no
    /// read) is exactly enough to create and rename inside the directory and
    /// NOT enough to open it `O_RDONLY` for the fsync, so a store that skips
    /// the directory fsync returns `Ok` here and one that performs it returns
    /// the named error.
    #[test]
    #[cfg(unix)]
    fn publish_fsyncs_the_artifact_directory_and_names_the_failure() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        // Happy path first: with a readable directory the fsync succeeds and
        // publish is unchanged.
        store.publish(4096, ok_write(b"hello")).unwrap();

        let perms_before = std::fs::metadata(&store.dir).unwrap().permissions();
        std::fs::set_permissions(&store.dir, std::fs::Permissions::from_mode(0o300)).unwrap();
        let result = store.publish(8192, ok_write(b"world"));
        // Restore before asserting so a failure doesn't leave an
        // undeletable temp dir behind.
        std::fs::set_permissions(&store.dir, perms_before).unwrap();

        let err = result.expect_err("a directory fsync that cannot happen is a named failure");
        let msg = err.to_string();
        assert!(
            msg.contains("snapshot directory fsync"),
            "the failure names the step, not just `io`: {msg}"
        );
        assert!(
            msg.contains("snap-8192.ultsnap") || msg.contains(store.dir.to_str().unwrap()),
            "the failure names the artifact or its directory: {msg}"
        );
        // The rename DID happen — the error reports "published but not
        // provably durable", and the caller's retry rebuilds the same name.
        assert!(store.path_for(8192).exists());
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

    /// Coordinated-snapshot ruling P1: `publish` never prunes — retention is
    /// node-owned, because only the node can see which artifacts form a
    /// COMPLETE set. This module keeps every artifact it writes; `uc_node`'s
    /// `prune_snapshots_below` is what deletes them.
    #[test]
    fn publish_never_prunes() {
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
