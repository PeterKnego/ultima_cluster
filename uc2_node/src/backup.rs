// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M11 Task 1: an offline, ordered-copy backup artifact + a read-only verify
//! over it.
//!
//! A backup is a plain filesystem copy of an instance directory's DURABLE
//! subdirectories (`journal/`, `state/`, `snapshots/` — see
//! `docs/reference/instance-directory.md`'s durable/volatile split; the
//! volatile files — `cnc2.dat`, `log.buf`, the ring files, `instance.lock` —
//! are never copied, boot recreates them unconditionally). The node MAY be
//! running throughout: a copy of the active journal segment mid-append is
//! crash-equivalent (a torn tail, healed the same way a restart heals it —
//! see [`verify_artifact`]), and the `state/*.state` files are two-slot
//! `StableValue`s, so an arbitrary-instant copy is always readable (worst
//! case one generation stale, per `ultima_journal::stable_value`'s
//! `pick_slot`).
//!
//! # The ordering rule
//!
//! [`backup_instance`] copies, in this exact order, one directory fully
//! before starting the next: `journal/` → `state/` → `snapshots/`. This is
//! not incidental — it is the whole correctness argument for a backup taken
//! while purge is running concurrently:
//!
//! > first_base only advances (purge), the newest snapshot position only
//! > advances (publish is atomic, retention keeps the newest 2, and purge
//! > only runs below a durably persisted floor that some retained snapshot
//! > covers) — so a snapshot copied AFTER the journal always covers any purge
//! > that happened BEFORE the journal copy. The reverse order can capture a
//! > snapshot set from before a purge that the journal copy then reflects: a
//! > hole.
//!
//! [`verify_artifact`] asserts this invariant on every artifact (not just
//! ones this module produced) rather than merely documenting it — see
//! `a_wrong_order_copy_across_a_purge_is_detected_as_a_hole` in
//! `uc2_node/tests/backup.rs`, the anti-vacuity test for this whole module.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use uc2_log::archive::{Archive, ArchiveConfig};
use uc2_log::state::{ConfigRecord, TermMap, VoteRecord};
use ultima_journal::{StableValue, StableValueConfig};

const STATE_FILES: [&str; 5] =
    ["vote.state", "term_map.state", "output_progress.state", "snapshot.state", "config.state"];

const SNAP_PREFIX: &str = "snap-";
const SNAP_SUFFIX: &str = ".ultsnap";

const MANIFEST_FORMAT: &str = "uc2-backup-v1";

/// The result of a backup or a verify: the artifact's recovered positions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackupReport {
    /// Lowest position still covered by the artifact's journal; `0` means the
    /// journal is unpurged (or empty) — the whole log from genesis is there.
    pub journal_first_base: u64,
    /// The recovered durable frontier of the COPY (i.e. of the artifact, not
    /// of the live instance — a backup taken under load may be short of the
    /// source's current frontier by design).
    pub journal_last_pos: u64,
    /// The highest position of any complete (`snap-<pos>.ultsnap`) snapshot
    /// file found in the artifact's `snapshots/`, or `None` if there are
    /// none.
    pub newest_snapshot: Option<u64>,
    /// The durably-persisted snapshot floor from `state/snapshot.state`
    /// (`0` if never set).
    pub snapshot_floor: u64,
    /// Whether opening the artifact's journal found (and healed) a torn
    /// active-segment tail — expected and harmless for a backup taken while
    /// the node was running; reported, not hidden.
    pub healed_torn_tail: bool,
    /// Total number of files copied (or, for a standalone [`verify_artifact`]
    /// call, found) across `journal/` + `state/` + `snapshots/`.
    pub files: usize,
}

/// Why a backup or verify failed.
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    /// `backup_instance`'s source directory does not look like a node
    /// instance directory (missing `journal/`, `state/`, or one of the five
    /// `state/*.state` files boot always creates).
    #[error(
        "not an instance directory: missing journal/, state/, or one of the state/*.state files"
    )]
    NotAnInstanceDir,
    /// `backup_instance`'s `out` directory already exists and is non-empty.
    #[error("artifact output directory already exists and is not empty")]
    ArtifactExists,
    /// The coverage invariant failed: the artifact's journal starts above
    /// `first_base`, but no retained snapshot covers that position. A backup
    /// built by this module's own ordering rule can never produce this; it
    /// is the anti-vacuity signal that a hand-built (or wrong-order) artifact
    /// is unsafe to restore from.
    #[error(
        "hole: journal first_base={first_base} is not covered by any retained snapshot \
         (newest_snapshot={newest_snapshot:?})"
    )]
    Hole { first_base: u64, newest_snapshot: Option<u64> },
    /// A `MANIFEST` is present but disagrees with the artifact's own
    /// recovered state — tampering or bitrot at the metadata level.
    #[error("manifest mismatch: {0}")]
    ManifestMismatch(String),
    /// `verify_artifact`'s target does not look like a backup artifact
    /// (missing `journal/`, `state/`, one of the five `state/*.state` files,
    /// or a `state/*.state` file that fails to decode on BOTH slots).
    #[error("not a backup artifact: missing or corrupt journal/state layout")]
    NotAnArtifact,
}

fn journal_dir(root: &Path) -> PathBuf {
    root.join("journal")
}

fn state_dir(root: &Path) -> PathBuf {
    root.join("state")
}

fn snapshots_dir(root: &Path) -> PathBuf {
    root.join("snapshots")
}

/// `journal/` and `state/` exist, and every one of the five `state/*.state`
/// files boot always creates is present. `snapshots/` is NOT required — a
/// fresh node (or one that never published a snapshot) has none, and that is
/// a valid (if `first_base == 0`-constrained) instance dir / artifact.
fn looks_like_instance_layout(root: &Path) -> bool {
    if !journal_dir(root).is_dir() || !state_dir(root).is_dir() {
        return false;
    }
    STATE_FILES.iter().all(|f| state_dir(root).join(f).is_file())
}

fn parse_snap_pos(name: &str) -> Option<u64> {
    name.strip_prefix(SNAP_PREFIX)?.strip_suffix(SNAP_SUFFIX)?.parse().ok()
}

/// Copy every regular file directly under `src` matching `keep` into `dst`
/// (created if absent), in filename-sorted order. Sorting is why the active
/// journal segment (the highest `seg-{:020}.log` name) copies last within
/// `journal/` — no special-casing needed, the fixed-width zero-padded name
/// already sorts it there.
fn copy_dir_sorted(src: &Path, dst: &Path, keep: impl Fn(&str) -> bool) -> io::Result<usize> {
    fs::create_dir_all(dst)?;
    let mut names: Vec<String> = fs::read_dir(src)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| keep(n))
        .collect();
    names.sort();
    for name in &names {
        fs::copy(src.join(name), dst.join(name))?;
    }
    Ok(names.len())
}

/// Ordered-copy `instance_dir` into a fresh `out` directory: `journal/` fully,
/// then `state/` fully, then `snapshots/` (see the module doc for why this
/// order is load-bearing). Refuses an `out` that already exists and is
/// non-empty. The node may be running throughout.
///
/// On success, runs [`verify_artifact`] on the copy (so a backup that would
/// verify as a hole is still RETURNED — callers that want backup to itself
/// refuse a hole call `verify_artifact`'s result explicitly) and writes a
/// hand-formatted `MANIFEST` (`key=value` lines — no serde_json in this
/// workspace) recording the recovered positions.
pub fn backup_instance(instance_dir: &Path, out: &Path) -> Result<BackupReport, BackupError> {
    if !looks_like_instance_layout(instance_dir) {
        return Err(BackupError::NotAnInstanceDir);
    }

    if out.exists() {
        if !out.is_dir() || fs::read_dir(out)?.next().is_some() {
            return Err(BackupError::ArtifactExists);
        }
    } else {
        fs::create_dir_all(out)?;
    }

    // The ordering rule: journal/ fully, THEN state/, THEN snapshots/.
    copy_dir_sorted(&journal_dir(instance_dir), &journal_dir(out), |_| true)?;
    copy_dir_sorted(&state_dir(instance_dir), &state_dir(out), |_| true)?;
    let src_snapshots = snapshots_dir(instance_dir);
    if src_snapshots.is_dir() {
        copy_dir_sorted(&src_snapshots, &snapshots_dir(out), |n| parse_snap_pos(n).is_some())?;
    }

    let report = verify_artifact(out)?;
    write_manifest(out, &report)?;
    Ok(report)
}

/// Read-only verification of a backup artifact (or, incidentally, a stopped
/// instance directory — the layout is the same minus `cnc2.dat`/rings, which
/// this never looks at). The ONE permitted mutation: opening the artifact's
/// journal may heal a torn active-segment tail exactly as a real boot would
/// (`Journal::open`) — reported via [`BackupReport::healed_torn_tail`], never
/// hidden. Everything else is read-only.
///
/// Steps (brief order, matches the semantics this module is built against):
/// 1. Open the journal (`uc2_log::Archive::open`, which wraps
///    `ultima_journal::Journal::open` exactly as node boot does) — recovers
///    `journal_first_base` / `journal_last_pos`, possibly healing a torn tail.
/// 2. Open all five `state/*.state` files read-only. A decode failure on
///    BOTH slots of any one of them means the artifact is corrupt.
/// 3. List `snapshots/snap-*.ultsnap`, parse positions (`*.tmp` and anything
///    else is ignored), take the newest.
/// 4. The coverage invariant: if `journal_first_base > 0`, some retained
///    snapshot must cover it (`newest_snapshot >= journal_first_base`), else
///    [`BackupError::Hole`].
/// 5. If a `MANIFEST` is present, cross-check its recorded values against
///    what was just recovered — a re-verify of a shipped artifact must catch
///    tampering/bitrot at the metadata level.
pub fn verify_artifact(artifact: &Path) -> Result<BackupReport, BackupError> {
    if !looks_like_instance_layout(artifact) {
        return Err(BackupError::NotAnArtifact);
    }

    // 1. Journal: recover positions, healing a torn tail if present.
    let archive_cfg = ArchiveConfig::new(journal_dir(artifact));
    let journal_files = count_files(&journal_dir(artifact))?;
    let archive =
        Archive::open(archive_cfg).map_err(|e| BackupError::Io(io::Error::other(e)))?;
    let journal_first_base = archive.first_base();
    let journal_last_pos = archive.recovered_position();
    let healed_torn_tail = archive.healed_torn_tail();

    // 2. State: all five files must be present (checked by
    // `looks_like_instance_layout` above) and decode. `snapshot.state`'s
    // value is also the durable snapshot floor we report.
    open_state_readonly::<VoteRecord>(&state_dir(artifact).join("vote.state"))?;
    open_state_readonly::<TermMap>(&state_dir(artifact).join("term_map.state"))?;
    open_state_readonly::<u64>(&state_dir(artifact).join("output_progress.state"))?;
    let snapshot_floor =
        open_state_readonly::<u64>(&state_dir(artifact).join("snapshot.state"))?.unwrap_or(0);
    open_state_readonly::<ConfigRecord>(&state_dir(artifact).join("config.state"))?;

    // 3. Snapshots: newest complete `snap-<pos>.ultsnap`.
    let (newest_snapshot, snapshot_files) = scan_snapshots(&snapshots_dir(artifact))?;

    // 4. Coverage invariant — the whole point of this module.
    let covered = match newest_snapshot {
        Some(pos) => pos >= journal_first_base,
        None => journal_first_base == 0,
    };
    if journal_first_base > 0 && !covered {
        return Err(BackupError::Hole { first_base: journal_first_base, newest_snapshot });
    }

    let report = BackupReport {
        journal_first_base,
        journal_last_pos,
        newest_snapshot,
        snapshot_floor,
        healed_torn_tail,
        files: journal_files + STATE_FILES.len() + snapshot_files,
    };

    // 5. Cross-check a shipped MANIFEST, if present.
    let manifest_path = artifact.join("MANIFEST");
    if manifest_path.is_file() {
        check_manifest(&manifest_path, &report)?;
    }

    Ok(report)
}

fn count_files(dir: &Path) -> io::Result<usize> {
    if !dir.is_dir() {
        return Ok(0);
    }
    Ok(fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .count())
}

/// Open a `state/*.state` `StableValue<T>` read-only. The file must already
/// exist — `ultima_journal::StableValue::open` on an absent path CREATES it
/// (header + two zeroed slots), which would be a silent mutation of a
/// supposed artifact rather than the loud [`BackupError::NotAnArtifact`] a
/// missing state file should be. Any open/decode failure (including the
/// both-slots-corrupt case) is [`BackupError::NotAnArtifact`]: this artifact
/// is not a valid one.
fn open_state_readonly<T>(path: &Path) -> Result<Option<T>, BackupError>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
{
    if !path.is_file() {
        return Err(BackupError::NotAnArtifact);
    }
    let sv = StableValue::<T>::open(StableValueConfig::new(path.to_path_buf()))
        .map_err(|_| BackupError::NotAnArtifact)?;
    sv.load().map_err(|_| BackupError::NotAnArtifact)
}

/// `(newest position, count of complete snapshot files)`. `None`/`0` if the
/// directory is absent (no snapshots ever published) or empty. Anything not
/// matching `snap-<pos>.ultsnap` (in particular `.tmp` in-progress writes) is
/// ignored, same convention as `uc2_service::snapshots::SnapshotStore`.
fn scan_snapshots(dir: &Path) -> io::Result<(Option<u64>, usize)> {
    if !dir.is_dir() {
        return Ok((None, 0));
    }
    let mut newest: Option<u64> = None;
    let mut count = 0usize;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(pos) = parse_snap_pos(name) else { continue };
        count += 1;
        newest = Some(newest.map_or(pos, |n| n.max(pos)));
    }
    Ok((newest, count))
}

fn manifest_value(newest_snapshot: Option<u64>) -> String {
    match newest_snapshot {
        Some(pos) => pos.to_string(),
        None => "none".to_string(),
    }
}

fn write_manifest(dir: &Path, report: &BackupReport) -> Result<(), BackupError> {
    let created_unix_ns =
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let contents = format!(
        "format={}\njournal_first_base={}\njournal_last_pos={}\nnewest_snapshot={}\n\
         snapshot_floor={}\nhealed_torn_tail={}\ncreated_unix_ns={}\n",
        MANIFEST_FORMAT,
        report.journal_first_base,
        report.journal_last_pos,
        manifest_value(report.newest_snapshot),
        report.snapshot_floor,
        report.healed_torn_tail,
        created_unix_ns,
    );
    fs::write(dir.join("MANIFEST"), contents)?;
    Ok(())
}

fn parse_manifest(text: &str) -> HashMap<String, String> {
    text.lines()
        .filter_map(|line| line.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn check_manifest(path: &Path, report: &BackupReport) -> Result<(), BackupError> {
    let text = fs::read_to_string(path)?;
    let map = parse_manifest(&text);

    let get = |key: &str| -> Result<&String, BackupError> {
        map.get(key).ok_or_else(|| BackupError::ManifestMismatch(format!("missing key {key}")))
    };
    let parse_u64 = |key: &str, raw: &str| -> Result<u64, BackupError> {
        raw.parse().map_err(|_| {
            BackupError::ManifestMismatch(format!("{key}: not a u64 in manifest ({raw:?})"))
        })
    };
    let mismatch = |key: &str, manifest: String, actual: String| -> BackupError {
        BackupError::ManifestMismatch(format!(
            "{key}: manifest says {manifest}, artifact recovers {actual}"
        ))
    };

    let format = get("format")?;
    if format != MANIFEST_FORMAT {
        return Err(BackupError::ManifestMismatch(format!("unknown format {format:?}")));
    }

    let jfb = parse_u64("journal_first_base", get("journal_first_base")?)?;
    if jfb != report.journal_first_base {
        return Err(mismatch(
            "journal_first_base",
            jfb.to_string(),
            report.journal_first_base.to_string(),
        ));
    }

    let jlp = parse_u64("journal_last_pos", get("journal_last_pos")?)?;
    if jlp != report.journal_last_pos {
        return Err(mismatch(
            "journal_last_pos",
            jlp.to_string(),
            report.journal_last_pos.to_string(),
        ));
    }

    let ns_raw = get("newest_snapshot")?;
    let ns: Option<u64> = if ns_raw == "none" {
        None
    } else {
        Some(parse_u64("newest_snapshot", ns_raw)?)
    };
    if ns != report.newest_snapshot {
        return Err(mismatch(
            "newest_snapshot",
            manifest_value(ns),
            manifest_value(report.newest_snapshot),
        ));
    }

    let sf = parse_u64("snapshot_floor", get("snapshot_floor")?)?;
    if sf != report.snapshot_floor {
        return Err(mismatch("snapshot_floor", sf.to_string(), report.snapshot_floor.to_string()));
    }

    let htt_raw = get("healed_torn_tail")?;
    let htt: bool = htt_raw.parse().map_err(|_| {
        BackupError::ManifestMismatch(format!("healed_torn_tail: not a bool ({htt_raw:?})"))
    })?;
    if htt != report.healed_torn_tail {
        return Err(mismatch(
            "healed_torn_tail",
            htt.to_string(),
            report.healed_torn_tail.to_string(),
        ));
    }

    let _created = get("created_unix_ns")?; // presence-only; not cross-checked against anything.

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_snap_pos_matches_the_store_convention() {
        assert_eq!(parse_snap_pos("snap-1234.ultsnap"), Some(1234));
        assert_eq!(parse_snap_pos("snap-0.ultsnap"), Some(0));
        assert_eq!(parse_snap_pos("snap-1234.ultsnap.tmp"), None);
        assert_eq!(parse_snap_pos("garbage"), None);
    }

    #[test]
    fn manifest_roundtrips() {
        let report = BackupReport {
            journal_first_base: 100,
            journal_last_pos: 5000,
            newest_snapshot: Some(200),
            snapshot_floor: 200,
            healed_torn_tail: true,
            files: 7,
        };
        let dir = tempfile::tempdir().unwrap();
        write_manifest(dir.path(), &report).unwrap();
        // check_manifest must accept exactly what write_manifest wrote.
        check_manifest(&dir.path().join("MANIFEST"), &report).unwrap();
    }

    #[test]
    fn manifest_roundtrips_with_no_snapshot() {
        let report = BackupReport {
            journal_first_base: 0,
            journal_last_pos: 5000,
            newest_snapshot: None,
            snapshot_floor: 0,
            healed_torn_tail: false,
            files: 3,
        };
        let dir = tempfile::tempdir().unwrap();
        write_manifest(dir.path(), &report).unwrap();
        check_manifest(&dir.path().join("MANIFEST"), &report).unwrap();
    }

    #[test]
    fn check_manifest_catches_a_tampered_field() {
        let report = BackupReport {
            journal_first_base: 100,
            journal_last_pos: 5000,
            newest_snapshot: Some(200),
            snapshot_floor: 200,
            healed_torn_tail: false,
            files: 7,
        };
        let dir = tempfile::tempdir().unwrap();
        write_manifest(dir.path(), &report).unwrap();
        let mut wrong = report;
        wrong.journal_first_base += 1;
        let err = check_manifest(&dir.path().join("MANIFEST"), &wrong).unwrap_err();
        assert!(matches!(err, BackupError::ManifestMismatch(_)));
    }
}
