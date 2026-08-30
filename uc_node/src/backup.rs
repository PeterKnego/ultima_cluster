// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M11 Task 1: an offline, ordered-copy backup artifact + a read-only verify
//! over it. M11 Task 2 adds [`restore_artifact`]: copy a verified artifact's
//! three durable directories into a fresh instance directory, refusing a
//! target whose durable subdirectories are already non-empty. All three
//! entry points ([`backup_instance`], [`verify_artifact`],
//! [`restore_artifact`]) are OFFLINE — filesystem-only, no cnc admin-band
//! interaction, unlike every other `uc2ctl` admin verb.
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
//! case one generation stale, per `uc_journal::stable_value`'s
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
//! `uc_node/tests/backup.rs`, the anti-vacuity test for this whole module.
//!
//! `a_wrong_order_copy_across_a_purge_is_detected_as_a_hole` hand-builds a
//! broken artifact to prove the coverage invariant is a real check, and
//! `ordered_backup_survives_a_purge_racing_the_copy` races a live purge
//! against an in-flight `backup_instance` copy directly (background submit +
//! snapshot-publish + purge, repeated `backup_instance` calls with no
//! quiescence wait) — every artifact must still verify. That test's first
//! version (before [`copy_dir_sorted`]'s whole-directory retry existed)
//! failed reproducibly ~1-in-3 runs, but never with `Hole` — with
//! `Io(NotFound)`: a concurrent purge or snapshot-retention unlink racing an
//! already-listed file's `fs::copy`. See [`copy_dir_sorted`]'s doc for the
//! fix (retry the whole directory, bounded) and why a per-file skip is
//! unsafe (a mid-journal gap the coverage check cannot see).
//!
//! # Read-only beyond the one permitted heal
//!
//! [`verify_artifact`] opens the artifact's journal with
//! `preallocate_segments: false` specifically so that healing a torn
//! active-segment tail is, at most, a physical `truncate` (shrink only,
//! never grow) — see the comment at its `ArchiveConfig` construction for why
//! the default (`true`, matching boot) is wrong for a read-mostly verify
//! path: it silently re-preallocates (GROWS) the active segment to
//! `segment_size_bytes` on every call, an artifact mutation the "verify may
//! heal, never hide" constraint does not permit.
//!
//! That truncate is still a real write to the active segment, and a real
//! backup artifact never races anyone for it — but a path an operator hands
//! to `verify_artifact`/`restore_artifact` by mistake (a typo, or swapped
//! restore-vs-target arguments) COULD be a running node's live instance
//! directory, in which case the truncate races the node's own writer for the
//! same file: a narrow, real acked-write-loss hazard. [`verify_artifact`]
//! (and, through it, `restore_artifact`'s `artifact` argument, plus
//! `restore_artifact`'s `instance_dir` target directly) guards against this:
//! a backup artifact NEVER contains `instance.lock` (`backup_instance` only
//! ever copies `journal/`/`state/`/`snapshots/`), so if `<path>/instance.lock`
//! exists AND is currently held by a running node, both functions refuse
//! with [`BackupError::LooksLikeLiveInstanceDir`] rather than touch anything.
//! A lock file that is present but NOT held (a stopped node's own leftover)
//! does not trip this — verifying (or restoring into) a stopped node's own
//! instance dir in place is the legitimate case the check must not break.
//! See [`refuse_if_live_instance_dir`].

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use uc_log::archive::{Archive, ArchiveConfig};
use uc_log::state::{ConfigRecord, TermMap, VoteRecord};
use uc_protocol::v2::cnc::CNC_MAX_SERVICES;
use uc_journal::{StableValue, StableValueConfig};

const STATE_FILES: [&str; 5] =
    ["vote.state", "term_map.state", "output_progress.state", "snapshot.state", "config.state"];

const SNAP_PREFIX: &str = "snap-";
const SNAP_SUFFIX: &str = ".ultsnap";

const MANIFEST_FORMAT: &str = "uc2-backup-v2";

/// Bounded retry count for [`copy_dir_sorted`]'s whole-directory retry on a
/// vanished source file. See that function's doc for why bounded and why a
/// FULL-directory retry rather than a per-file skip.
const MAX_COPY_RETRIES: usize = 5;

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
    /// Per service id present in `snapshots/<id>/` (M14a): the highest
    /// position of any complete (`snap-<pos>.ultsnap`) snapshot file found
    /// in that id's subdirectory, or `None` if there is no directory (or it
    /// is empty) for that id. Offline and config-blind: whatever
    /// `snapshots/<id>/` directories exist on disk are the ids present.
    pub newest_snapshots: [Option<u64>; CNC_MAX_SERVICES],
    /// The durably-persisted snapshot floor from `state/snapshot.state`
    /// (`0` if never set).
    pub snapshot_floor: u64,
    /// Whether opening the artifact's journal found (and healed) a torn
    /// active-segment tail — expected and harmless for a backup taken while
    /// the node was running; reported, not hidden.
    pub healed_torn_tail: bool,
    /// Total number of files copied (or, for a standalone [`verify_artifact`]
    /// call, found) across `journal/` + `state/` + `snapshots/*/`.
    pub files: usize,
}

impl BackupReport {
    /// The cluster-wide coverage point: the LOWEST newest-snapshot over the
    /// ids present (a restore is only as fresh as its slowest FSM). `None`
    /// if no id has any snapshot at all.
    pub fn newest_snapshot(&self) -> Option<u64> {
        self.newest_snapshots.iter().flatten().copied().min()
    }
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
    /// The coverage invariant failed for one service id: the artifact's
    /// journal starts above `first_base`, but no retained snapshot for
    /// `service` covers that position (M14a: coverage is checked per FSM id
    /// present in `snapshots/<id>/` — a `service: 0` hole with
    /// `newest_snapshot: None` also covers the "no snapshot directory at
    /// all" case, since FSM 0 is the one id every node declares). A backup
    /// built by this module's own ordering rule can never produce this; it
    /// is the anti-vacuity signal that a hand-built (or wrong-order, or
    /// tampered) artifact is unsafe to restore from.
    #[error(
        "hole: service {service}: journal first_base={first_base} is not covered by any \
         retained snapshot (newest_snapshot={newest_snapshot:?})"
    )]
    Hole { service: u8, first_base: u64, newest_snapshot: Option<u64> },
    /// A `MANIFEST` is present but disagrees with the artifact's own
    /// recovered state — tampering or bitrot at the metadata level.
    #[error("manifest mismatch: {0}")]
    ManifestMismatch(String),
    /// `verify_artifact`'s target does not look like a backup artifact
    /// (missing `journal/`, `state/`, one of the five `state/*.state` files,
    /// or a `state/*.state` file that fails to decode on BOTH slots).
    #[error("not a backup artifact: missing or corrupt journal/state layout")]
    NotAnArtifact,
    /// `restore_artifact`'s target `instance_dir` already has a non-empty
    /// `journal/`, `state/`, or `snapshots/` — refused rather than merged or
    /// overwritten. Volatile leftovers (`cnc2.dat`, `log.buf`, `*.ring`,
    /// `*.broadcast`, `instance.lock`) do NOT trigger this: boot recreates
    /// them unconditionally, so they are harmless to leave behind.
    #[error(
        "restore target already has a non-empty journal/, state/, or snapshots/ directory \
         (refusing to merge or overwrite; use a fresh instance directory)"
    )]
    TargetNotEmpty,
    /// `verify_artifact` (and, via it, `restore_artifact`'s `artifact`
    /// argument), plus `restore_artifact`'s `instance_dir` target, refuse a
    /// path whose `instance.lock` is currently HELD — see
    /// [`refuse_if_live_instance_dir`]. Backup artifacts never contain this
    /// file (`backup_instance` only ever copies `journal/`/`state/`/
    /// `snapshots/`), so an artifact can never trip this; a held lock means
    /// a node is running there right now, and `verify_artifact`'s one
    /// permitted heal (a physical truncate of the active journal segment)
    /// against a live writer's segment is a narrow, real acked-write-loss
    /// race — the classic operator typo / swapped restore-vs-target
    /// argument. A lock file that is PRESENT but not held (a stopped node's
    /// leftover) does NOT trip this — verifying a stopped node's own
    /// instance dir in place is the legitimate case this must not break.
    #[error(
        "{0}: this looks like a live instance directory, not an artifact — a node holds its lock"
    )]
    LooksLikeLiveInstanceDir(PathBuf),
}

/// Probe `<path>/instance.lock` for a currently-running node, without ever
/// holding the lock beyond the probe itself. Backup artifacts never contain
/// this file (see [`BackupError::LooksLikeLiveInstanceDir`]'s doc), so its
/// mere presence already means `path` is (or once was) a real instance
/// directory rather than a shipped artifact; a non-blocking exclusive
/// try-lock (`fs2`, same primitive `InstanceDir::acquire` uses to enforce
/// one-node-per-dir) then distinguishes the two cases that matter
/// operationally:
///
/// - held by someone -> a node owns this dir right now -> refuse
///   ([`BackupError::LooksLikeLiveInstanceDir`]).
/// - present but immediately acquirable -> nothing is currently writing (a
///   stopped node's leftover lock file — shutdown never deletes it, only
///   releases the flock) -> release it again right away (this function only
///   ever probes, it is not a caller of `InstanceDir::acquire` and must not
///   hold the dir) and let the caller proceed.
///
/// No `instance.lock` at all is the ordinary shipped-artifact case and is
/// not probed further.
fn refuse_if_live_instance_dir(path: &Path) -> Result<(), BackupError> {
    let lock_path = path.join("instance.lock");
    if !lock_path.is_file() {
        return Ok(());
    }
    let f = fs::OpenOptions::new().read(true).write(true).open(&lock_path)?;
    if f.try_lock_exclusive().is_err() {
        return Err(BackupError::LooksLikeLiveInstanceDir(path.to_path_buf()));
    }
    let _ = f.unlock();
    Ok(())
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
///
/// # Retrying the WHOLE directory on a vanished source file
///
/// `src` may be live under our feet: `journal/`'s purge and `snapshots/`'s
/// keep-newest-2 retention both unlink files while the node keeps running,
/// and a backup taken under load races both by design (this module's whole
/// premise). If a file we already listed vanishes before its `fs::copy` runs
/// (`io::ErrorKind::NotFound`), this does NOT skip just that name and
/// continue — it discards the whole partial copy of `dst`
/// (`fs::remove_dir_all`) and re-lists + re-copies `src` from scratch, up to
/// [`MAX_COPY_RETRIES`] times.
///
/// A per-file skip is unsafe: purge unlinks a CONTIGUOUS run of segments in
/// one call, but this loop and purge's removal loop are two independent,
/// unsynchronized passes over the same directory, so a file mid-run can
/// vanish while an EARLIER name (already copied) and a LATER name (not yet
/// reached) both survive — a copy with segment N present, N+1..N+5 missing,
/// N+6 present: a GAP in the middle of the journal. `Journal::open` cannot
/// detect this (it derives `first_seq`/`last_seq` from whatever segment
/// files exist, without checking they're contiguous), and neither can
/// `verify_artifact`'s coverage-invariant hole check (it only looks at the
/// FIRST retained segment's base against the newest snapshot) — so a
/// mid-journal gap would silently verify clean and only surface as a missing
/// range on replay, arbitrarily later. Restarting the WHOLE directory copy
/// avoids this by construction: whatever set of files we successfully copy
/// on a given attempt is exactly what `src` looked like at some single
/// instant during that attempt (modulo the same live-tail-of-the-active-
/// segment behavior any single copy already tolerates), never a splice of
/// two different instants.
///
/// A retried copy is equivalent to having simply started that part of the
/// backup a little LATER — which the ordering rule (module doc) already
/// covers: `first_base` only ever advances (purge) and the newest retained
/// snapshot position only ever advances (atomic publish, keep-newest-2), so
/// a later start can only make the coverage invariant easier to satisfy, not
/// harder. Retries are bounded (not unbounded) because purge/retention
/// cadence tracks SNAPSHOT cadence — orders of magnitude slower than copying
/// a handful of files — so a real race resolves within a retry or two;
/// exhausting [`MAX_COPY_RETRIES`] points at something else being wrong (a
/// permanently missing/renamed source, e.g.), which is surfaced as a loud
/// error naming the directory and the attempt count rather than retried
/// forever.
///
/// Applies uniformly to all three directories, including `state/` — nothing
/// ever unlinks a `state/*.state` file, so the retry path is simply dead
/// code there (a real `NotFound` under `state/` — e.g. the file never
/// existed — surfaces immediately as an `Io` error on the FIRST attempt's
/// very first `fs::copy`, before any retry logic even runs, since
/// `looks_like_instance_layout` already validated the source has all five
/// files before any copying starts).
fn copy_dir_sorted(
    src: &Path,
    dst: &Path,
    keep: impl Fn(&str) -> bool,
) -> Result<usize, BackupError> {
    let mut last_err: Option<io::Error> = None;
    for _attempt in 1..=MAX_COPY_RETRIES {
        match copy_dir_sorted_once(src, dst, &keep) {
            Ok(n) => return Ok(n),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let _ = fs::remove_dir_all(dst);
                last_err = Some(e);
            }
            Err(e) => return Err(BackupError::Io(e)),
        }
    }
    Err(BackupError::Io(io::Error::other(format!(
        "{}: a source file kept vanishing (likely a live purge/retention unlink racing the \
         copy) across {MAX_COPY_RETRIES} attempts; last error: {}",
        src.display(),
        last_err.map(|e| e.to_string()).unwrap_or_default(),
    ))))
}

/// One attempt at [`copy_dir_sorted`]'s job — no retry, no partial-copy
/// cleanup on failure (the caller owns both).
fn copy_dir_sorted_once(
    src: &Path,
    dst: &Path,
    keep: &impl Fn(&str) -> bool,
) -> io::Result<usize> {
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

/// Per-id snapshot subdirectories present on disk, ascending. Offline and
/// config-blind: whatever `snapshots/<id>/` directories exist under `root`
/// are the set — this module never reads `[services]`/`node.toml`.
fn snapshot_ids_present(root: &Path) -> io::Result<Vec<u8>> {
    let dir = snapshots_dir(root);
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if let Some(id) = entry.file_name().to_str().and_then(|n| n.parse::<u8>().ok())
            && (id as usize) < CNC_MAX_SERVICES
        {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// `snapshots/<id>/` for every id present, journal-then-state-then-snapshots
/// ordering preserved by the caller; the per-directory filter is the same
/// `snap-<pos>.ultsnap` rule [`copy_dir_sorted`] has always used.
fn copy_snapshot_tree(src_root: &Path, dst_root: &Path) -> Result<(), BackupError> {
    for id in snapshot_ids_present(src_root)? {
        copy_dir_sorted(
            &snapshots_dir(src_root).join(id.to_string()),
            &snapshots_dir(dst_root).join(id.to_string()),
            |n| parse_snap_pos(n).is_some(),
        )?;
    }
    Ok(())
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
///
/// OFFLINE in the `uc2ctl` sense: this is a filesystem-only operation, with no
/// interaction with a running node's cnc admin band whatsoever (unlike every
/// other `uc2ctl` admin verb, which talks to a node purely through that
/// channel). The node MAY be running throughout a call, but this function
/// never reads or writes its cnc page.
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

    // The ordering rule: journal/ fully, THEN state/, THEN snapshots/*/.
    copy_dir_sorted(&journal_dir(instance_dir), &journal_dir(out), |_| true)?;
    copy_dir_sorted(&state_dir(instance_dir), &state_dir(out), |_| true)?;
    copy_snapshot_tree(instance_dir, out)?;

    let report = verify_artifact(out)?;
    write_manifest(out, &report)?;
    Ok(report)
}

/// Read-only verification of a backup artifact (or, incidentally, a stopped
/// instance directory — the layout is the same minus `cnc2.dat`/rings, which
/// this never looks at). The ONE permitted mutation: opening the artifact's
/// journal may heal a torn active-segment tail — a physical shrink-only
/// `truncate` of the active segment (never a grow; see the module doc's
/// "Read-only beyond the one permitted heal" section and the `ArchiveConfig`
/// construction below) — reported via [`BackupReport::healed_torn_tail`],
/// never hidden. Everything else is read-only, including a re-verify: two
/// consecutive `verify_artifact` calls on the same artifact never change any
/// file's size a second time.
///
/// Refuses BEFORE step 1, without opening anything, if `artifact` looks like
/// a currently-RUNNING node's instance directory — see
/// [`BackupError::LooksLikeLiveInstanceDir`] and the module doc's "Read-only
/// beyond the one permitted heal" section. A stopped node's own instance dir
/// verifies fine in place; only a live one is refused.
///
/// Steps (brief order, matches the semantics this module is built against):
/// 1. Open the journal (`uc_log::Archive::open`, which wraps
///    `uc_journal::Journal::open` exactly as node boot does) — recovers
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
///
/// OFFLINE in the `uc2ctl` sense: filesystem-only, no cnc admin-band
/// interaction (see [`backup_instance`]'s doc for the same point).
pub fn verify_artifact(artifact: &Path) -> Result<BackupReport, BackupError> {
    // Checked FIRST, ahead of the layout check below: a live instance dir
    // (in particular one mid-boot, before all five `state/*.state` files
    // exist yet) must get the precise `LooksLikeLiveInstanceDir` refusal,
    // not a confusing `NotAnArtifact` — see that error's doc and
    // `refuse_if_live_instance_dir`'s.
    refuse_if_live_instance_dir(artifact)?;

    if !looks_like_instance_layout(artifact) {
        return Err(BackupError::NotAnArtifact);
    }

    // 1. Journal: recover positions, healing a torn tail if present.
    //
    // `preallocate_segments: false` is deliberate and load-bearing here, NOT
    // just "use the default": `Journal::open`'s post-recovery step
    // unconditionally re-preallocates the active segment up to
    // `segment_size_bytes` when `preallocate_segments` is true AND the
    // segment is physically shorter than that — which an artifact's active
    // segment always is unless it happens to match verify's own
    // `ArchiveConfig::new` default of 64 MiB exactly. With the default
    // `preallocate_segments: true`, verify would silently GROW the artifact's
    // active segment file on every call (observed 64 KiB -> 64 MiB under a
    // small-segment test config) — a mutation far outside "heal a torn tail",
    // and one that persists into any later restore. Verify never appends, so
    // it has no use for the preallocation write-path optimization anyway.
    // With `false`, a torn tail is healed by a physical `truncate` (shrink
    // only, never grow) instead of an in-memory cursor reset — still exactly
    // the one permitted mutation the module doc promises, just visible on
    // disk instead of invisible.
    let archive_cfg =
        ArchiveConfig { preallocate_segments: false, ..ArchiveConfig::new(journal_dir(artifact)) };
    let journal_files = count_files(&journal_dir(artifact), is_journal_segment_name)?;
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

    // 3. Snapshots: per id present, the newest complete `snap-<pos>.ultsnap`.
    let (newest_snapshots, snapshot_files) = scan_snapshot_tree(artifact)?;

    // 4. Coverage invariant, PER ID (M14a): every FSM whose directory exists
    // must be rebuildable from its own newest snapshot + the journal tail. A
    // purged journal with no snapshot directory at all is FSM 0's hole (the
    // one id every node declares) — the whole point of this module.
    if journal_first_base > 0 {
        let ids: Vec<u8> = snapshot_ids_present(artifact)?;
        if ids.is_empty() {
            return Err(BackupError::Hole {
                service: 0,
                first_base: journal_first_base,
                newest_snapshot: None,
            });
        }
        for id in ids {
            let n = newest_snapshots[id as usize];
            if n.is_none_or(|pos| pos < journal_first_base) {
                return Err(BackupError::Hole {
                    service: id,
                    first_base: journal_first_base,
                    newest_snapshot: n,
                });
            }
        }
    }

    let report = BackupReport {
        journal_first_base,
        journal_last_pos,
        newest_snapshots,
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

/// M11 Task 2: restore a backup artifact into an `instance_dir` a node has
/// never booted in (or one whose durable subdirectories were cleared).
///
/// Runs [`verify_artifact`] on `artifact` FIRST — the one permitted mutation
/// (healing a torn active-segment tail, a physical shrink-only `truncate`)
/// applies to the ARTIFACT, not the target; that is expected and reported via
/// [`BackupReport::healed_torn_tail`], never hidden, exactly as it is for a
/// standalone verify. A verify failure ([`BackupError::Hole`],
/// [`BackupError::NotAnArtifact`], [`BackupError::ManifestMismatch`]) aborts
/// before anything is copied.
///
/// Then refuses if `instance_dir`'s `journal/`, `state/`, or `snapshots/`
/// exist and are non-empty ([`BackupError::TargetNotEmpty`]) — restore never
/// merges or overwrites. Volatile leftovers (`cnc2.dat`, `log.buf`,
/// `*.ring`/`*.broadcast`, `instance.lock`) are allowed and left untouched;
/// a node's first boot recreates them unconditionally regardless of what
/// restore did.
///
/// On success, copies the three durable directories into `instance_dir`
/// (same [`copy_dir_sorted`] helper `backup_instance` uses — ordering does
/// not matter here the way it does for a live backup, since `artifact` is a
/// static, already-verified snapshot of a moment, not a live, concurrently-
/// mutating instance dir; reused anyway rather than duplicated). `MANIFEST`
/// is deliberately NOT copied into `instance_dir` — it is metadata about the
/// artifact, not part of a node's instance-directory layout, and boot never
/// looks for it.
///
/// The restored node's first boot does everything else: a fresh `cnc2.dat`
/// page and `instance_id`, `ConfigRecord`/vote/term-map recovery from the
/// copied `state/`, and (if the restored id is a minority of a still-healthy
/// quorum) rejoin/repair via the normal replication path. Restoring a
/// MINORITY of voters against a live majority is safe by construction (the
/// healthy quorum repairs or neutralizes any rolled-back log/vote); restoring
/// a MAJORITY is the quorum-loss procedure's domain, not this function's —
/// it carries its own data-loss statement and is not silently equivalent to
/// this.
///
/// Returns the artifact's [`BackupReport`] (i.e. what was restored, not a
/// property of the resulting `instance_dir` — the two agree immediately
/// after a successful call, but `instance_dir` is a live node's territory
/// from the moment it next boots).
///
/// OFFLINE in the `uc2ctl` sense: filesystem-only, no cnc admin-band
/// interaction — the target `instance_dir` need not (and normally must not)
/// have a node running in it at all (see [`backup_instance`]'s doc for the
/// same point about the source side). This is enforced, not just assumed:
/// both `artifact` (via `verify_artifact`) and `instance_dir` itself are
/// probed for a currently-held `instance.lock` and refused with
/// [`BackupError::LooksLikeLiveInstanceDir`] if one is found — see the
/// module doc's "Read-only beyond the one permitted heal" section. A stale,
/// unheld `instance.lock` left over in an otherwise-empty target (a stopped
/// node's leftover) is unaffected — only a HELD lock refuses.
///
/// No check guards `artifact == instance_dir`, or `instance_dir` nested
/// inside `artifact` (or vice versa): none is needed. Self-restore is simply
/// the ordinary refusal path — the target's `journal/`/`state/` ARE the
/// (non-empty) artifact's own, so [`BackupError::TargetNotEmpty`] fires
/// before any copy starts, same as any other already-populated target.
pub fn restore_artifact(artifact: &Path, instance_dir: &Path) -> Result<BackupReport, BackupError> {
    // `verify_artifact` already probes `artifact` for a live instance.lock
    // (see `refuse_if_live_instance_dir`'s doc) — nothing more needed on
    // that side.
    let report = verify_artifact(artifact)?;

    // The TARGET needs the same probe separately: `TargetNotEmpty` below
    // only looks at whether journal/state/snapshots hold files, but a node
    // that has just booted (or is between `InstanceDir::acquire` and its
    // first journal write) can hold the flock while those directories are
    // still empty — an operator pointing restore at a live node's own,
    // freshly-booted instance dir would sail past the emptiness check and
    // start copying underneath it. Cheap, so it's not worth reasoning our
    // way out of.
    refuse_if_live_instance_dir(instance_dir)?;

    for dir in [journal_dir(instance_dir), state_dir(instance_dir), snapshots_dir(instance_dir)] {
        if dir.is_dir() && fs::read_dir(&dir)?.next().is_some() {
            return Err(BackupError::TargetNotEmpty);
        }
    }

    copy_dir_sorted(&journal_dir(artifact), &journal_dir(instance_dir), |_| true)?;
    copy_dir_sorted(&state_dir(artifact), &state_dir(instance_dir), |_| true)?;
    copy_snapshot_tree(artifact, instance_dir)?;

    Ok(report)
}

fn is_journal_segment_name(name: &str) -> bool {
    name.starts_with("seg-") && name.ends_with(".log")
}

/// Count regular files directly under `dir` matching `keep`; `0` if `dir`
/// doesn't exist. `journal_files` uses `is_journal_segment_name` (symmetric
/// with `scan_snapshots`'s `snap-*.ultsnap` filter) so `BackupReport::files`
/// counts the same "meaningful artifact contents" in both directories rather
/// than incidental control files (a `truncate.intent`, say) that `copy_dir_sorted`
/// still copies verbatim but that aren't part of the artifact's semantic size.
fn count_files(dir: &Path, keep: impl Fn(&str) -> bool) -> io::Result<usize> {
    if !dir.is_dir() {
        return Ok(0);
    }
    Ok(fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| keep(n))
        .count())
}

/// Open a `state/*.state` `StableValue<T>` read-only. The file must already
/// exist — `uc_journal::StableValue::open` on an absent path CREATES it
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
/// ignored, same convention as `uc_service::snapshots::SnapshotStore`.
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

/// Per id present under `root`'s `snapshots/`: the newest complete artifact
/// (via [`scan_snapshots`] on that id's subdirectory); plus the total file
/// count across all ids.
fn scan_snapshot_tree(root: &Path) -> io::Result<([Option<u64>; CNC_MAX_SERVICES], usize)> {
    let mut newest = [None; CNC_MAX_SERVICES];
    let mut count = 0;
    for id in snapshot_ids_present(root)? {
        let (n, c) = scan_snapshots(&snapshots_dir(root).join(id.to_string()))?;
        newest[id as usize] = n;
        count += c;
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
    let mut contents = format!(
        "format={}\njournal_first_base={}\njournal_last_pos={}\n",
        MANIFEST_FORMAT, report.journal_first_base, report.journal_last_pos,
    );
    // One `newest_snapshot.<id>=<pos|none>` line for EVERY id 0..CNC_MAX_SERVICES
    // (deterministic, easy to parse) — not just the ids present, so a
    // shrinking declared set is visible in the manifest too.
    for id in 0..CNC_MAX_SERVICES {
        contents.push_str(&format!(
            "newest_snapshot.{id}={}\n",
            manifest_value(report.newest_snapshots[id])
        ));
    }
    contents.push_str(&format!(
        "snapshot_floor={}\nhealed_torn_tail={}\ncreated_unix_ns={}\n",
        report.snapshot_floor, report.healed_torn_tail, created_unix_ns,
    ));
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

    for id in 0..CNC_MAX_SERVICES {
        let key = format!("newest_snapshot.{id}");
        let raw = get(&key)?;
        let ns: Option<u64> =
            if raw == "none" { None } else { Some(parse_u64(&key, raw)?) };
        if ns != report.newest_snapshots[id] {
            return Err(mismatch(&key, manifest_value(ns), manifest_value(report.newest_snapshots[id])));
        }
    }

    let sf = parse_u64("snapshot_floor", get("snapshot_floor")?)?;
    if sf != report.snapshot_floor {
        return Err(mismatch("snapshot_floor", sf.to_string(), report.snapshot_floor.to_string()));
    }

    // `healed_torn_tail` is NOT a stable, re-derivable property of the
    // artifact the way the four position fields above are — healing (with
    // `preallocate_segments: false`, see `verify_artifact`'s `ArchiveConfig`
    // comment) is a physical, monotonic `truncate`: the FIRST open that finds
    // a torn tail fixes it on disk, so EVERY later open of that same,
    // untouched artifact correctly reports `false` (there is nothing left to
    // heal). A manifest written at backup time (which just ran that first
    // heal) legitimately says `true` forever after, even though no later
    // verify will ever reproduce `true` again. So only the SUSPICIOUS
    // direction is a mismatch: the manifest recorded no heal but THIS verify
    // found (and fixed) one anyway — a torn tail appearing in an artifact
    // that was supposedly already clean, i.e. new corruption after the fact.
    let htt_raw = get("healed_torn_tail")?;
    let htt: bool = htt_raw.parse().map_err(|_| {
        BackupError::ManifestMismatch(format!("healed_torn_tail: not a bool ({htt_raw:?})"))
    })?;
    if report.healed_torn_tail && !htt {
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
        let mut newest_snapshots = [None; CNC_MAX_SERVICES];
        newest_snapshots[0] = Some(200);
        let report = BackupReport {
            journal_first_base: 100,
            journal_last_pos: 5000,
            newest_snapshots,
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
            newest_snapshots: [None; CNC_MAX_SERVICES],
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
        let mut newest_snapshots = [None; CNC_MAX_SERVICES];
        newest_snapshots[0] = Some(200);
        let report = BackupReport {
            journal_first_base: 100,
            journal_last_pos: 5000,
            newest_snapshots,
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

    /// Fix round 1, CRITICAL fallout: `verify_artifact` now heals a torn tail
    /// via a physical `truncate` (see the `ArchiveConfig` comment at its
    /// journal open), which is monotonic — the SECOND open of the same,
    /// untouched artifact correctly finds nothing left to heal. A manifest
    /// written right after the first heal (`healed_torn_tail=true`) must not
    /// be flagged as mismatched by that expected, later `false`.
    #[test]
    fn check_manifest_allows_the_expected_true_to_false_transition_after_a_real_heal() {
        let healed_at_backup_time = BackupReport {
            journal_first_base: 0,
            journal_last_pos: 5000,
            newest_snapshots: [None; CNC_MAX_SERVICES],
            snapshot_floor: 0,
            healed_torn_tail: true,
            files: 3,
        };
        let dir = tempfile::tempdir().unwrap();
        write_manifest(dir.path(), &healed_at_backup_time).unwrap();

        // Everything else identical; only healed_torn_tail differs (a later
        // reopen of the now-clean artifact correctly finds no torn tail).
        let mut reverified = healed_at_backup_time;
        reverified.healed_torn_tail = false;
        check_manifest(&dir.path().join("MANIFEST"), &reverified)
            .expect("manifest=true, actual=false must be accepted (expected post-heal steady state)");
    }

    /// The other direction stays a real mismatch: a manifest that recorded NO
    /// heal, but a later verify finds (and fixes) a torn tail anyway, means
    /// something changed the artifact after backup — exactly the
    /// tampering/bitrot case the cross-check exists to catch.
    #[test]
    fn check_manifest_catches_an_unexpected_new_heal() {
        let clean_at_backup_time = BackupReport {
            journal_first_base: 0,
            journal_last_pos: 5000,
            newest_snapshots: [None; CNC_MAX_SERVICES],
            snapshot_floor: 0,
            healed_torn_tail: false,
            files: 3,
        };
        let dir = tempfile::tempdir().unwrap();
        write_manifest(dir.path(), &clean_at_backup_time).unwrap();

        let mut reverified = clean_at_backup_time;
        reverified.healed_torn_tail = true;
        let err = check_manifest(&dir.path().join("MANIFEST"), &reverified).unwrap_err();
        assert!(matches!(err, BackupError::ManifestMismatch(_)));
    }
}
