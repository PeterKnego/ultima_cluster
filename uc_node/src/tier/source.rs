//! Backup-tier 3b spec §6.1: the read-only handle a node holds on a tier it
//! may fetch snapshot sets from (`[tier_source]`, or its own `[backup]`
//! target). It knows the tier is this cluster's and which committed `through`
//! the cluster has registered for the tier's id; it never writes.

use super::TierError;
use super::layout::{SetLabel, cluster_object, row_object};
use super::materialize::copy_artifact;
use super::target::{BackupTarget, FsTarget, read_tier};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use uc_net::receiver::{SnapIntakeSignals, write_foreign_marker};
use uc_protocol::v2::backup::BackupTierEntry;
use uc_protocol::v2::catalog::{RowEntry, RowVerdict, SetEntry, SetState};
use uc_protocol::v2::datagram::{TierInstallBody, TierInstallReason};

/// TEST SEAM: the fetch delay every [`TierSource`] opened from now on
/// starts with ([`set_boot_fetch_delay_for_test`]); 0 by default.
static BOOT_FETCH_DELAY_NS: AtomicU64 = AtomicU64::new(0);

/// TEST SEAM, not API: every tier source OPENED after this call starts with
/// `per_artifact` as its fetch delay — for a test whose node asks for a copy
/// before `Node::start_with` returns (a below-floor learner is asked within
/// its first round trip, so `Node::set_tier_fetch_delay_for_test` comes too
/// late). Process-global: a test using it must serialize and reset it.
#[doc(hidden)]
pub fn set_boot_fetch_delay_for_test(per_artifact: Duration) {
    BOOT_FETCH_DELAY_NS.store(per_artifact.as_nanos() as u64, Ordering::Relaxed);
}

pub struct TierSource {
    target: Arc<FsTarget>,
    app_id: String,
    /// TEST SEAM: nanoseconds a `uc2-tier-fetch` thread sleeps before each
    /// artifact (default 0) — `Node::set_tier_fetch_delay_for_test`.
    fetch_delay_ns: AtomicU64,
    /// How many `uc2-tier-fetch` threads [`spawn_fetch`] has started from
    /// this source — `Node::tier_fetches_spawned`.
    fetches_spawned: AtomicU64,
}

impl TierSource {
    /// Opens `TIER`: a present `TIER` of another `app_id` is
    /// `Err(SourceForeign)` (the startup refusal `tier_source_foreign`);
    /// absent or unreadable is `Ok` — the tier id is learned lazily.
    pub fn open(path: &Path, app_id: &str) -> Result<TierSource, TierError> {
        let target = Arc::new(FsTarget::new(path));
        if let Ok(Some(info)) = read_tier(target.as_ref())
            && info.app_id != app_id
        {
            return Err(TierError::SourceForeign {
                path: path.display().to_string(),
                tier_app_id: info.app_id,
                app_id: app_id.to_string(),
            });
        }
        Ok(TierSource {
            target,
            app_id: app_id.to_string(),
            fetch_delay_ns: AtomicU64::new(BOOT_FETCH_DELAY_NS.load(Ordering::Relaxed)),
            fetches_spawned: AtomicU64::new(0),
        })
    }

    /// Why this source cannot serve right now (no `TIER` yet, or unreadable);
    /// `None` when `TIER` reads and is this cluster's.
    pub fn unavailable(&self) -> Option<String> {
        match read_tier(self.target.as_ref()) {
            Ok(Some(i)) if i.app_id == self.app_id => None,
            Ok(Some(_)) => Some("TIER names another app_id".into()),
            Ok(None) => Some("no TIER on the target".into()),
            Err(e) => Some(e.to_string()),
        }
    }

    /// The committed `through` for this tier, or `None` when `TIER` is
    /// unreadable, foreign, or its id is not registered in `backups`.
    /// Re-reads `TIER` on each call, so a tier mounted later works.
    pub fn registered_through(&self, backups: &[BackupTierEntry]) -> Option<u64> {
        let info = read_tier(self.target.as_ref()).ok().flatten()?;
        if info.app_id != self.app_id {
            return None;
        }
        backups
            .iter()
            .find(|e| e.tier_id == info.tier_id)
            .map(|e| e.through)
    }

    pub fn target(&self) -> &dyn BackupTarget {
        self.target.as_ref()
    }

    /// TEST SEAM: make every later fetch thread sleep `per_artifact` before
    /// copying each artifact.
    #[doc(hidden)]
    pub fn set_fetch_delay_for_test(&self, per_artifact: Duration) {
        self.fetch_delay_ns
            .store(per_artifact.as_nanos() as u64, Ordering::Relaxed);
    }

    /// How many fetch threads this source has started.
    pub fn fetches_spawned(&self) -> u64 {
        self.fetches_spawned.load(Ordering::Relaxed)
    }
}

/// `uc2_snapshot_fetch_total{source}` (3b spec §6.6): store-only fetches
/// that LANDED, by where the set came from. Bumped by the consensus agent,
/// read at scrape.
#[derive(Debug, Default)]
pub struct FetchSourceStats {
    pub tier: AtomicU64,
    pub peer: AtomicU64,
}

/// What a completed tier copy MEANS on this node — the receiver's
/// `IntakeMode`, for a set that came from a tier instead of a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchMode {
    /// 3b spec §6.2: the set is held (`publish_stored`) and nothing else.
    StoreOnly,
    /// 3b spec §6.3 (Task 13): the set is installed as a session's
    /// completion installs one (`publish_installed`).
    Install,
}

/// What a fetch thread reports: `Ok(lineage)` (the `SET`'s, which an
/// install adopts) or `Err(reason)` — `no_set`, `verify` or `io`.
pub type FetchResult = Result<Vec<(u32, u64)>, String>;

/// One running `uc2-tier-fetch` thread.
pub struct TierFetch {
    pub position: u64,
    /// Bytes copied so far (raw artifact bytes) — Task 13's `installing`
    /// progress figure.
    pub copied: Arc<AtomicU64>,
    /// `Some` once the thread has finished — its last act.
    pub done: Arc<Mutex<Option<FetchResult>>>,
    /// Review C1: set by [`Self::cancel`] (the node gave up on this copy and
    /// fell back to a peer, whose session writes the SAME final names). The
    /// thread checks it before every marker write and rename and before it
    /// publishes; once set it renames nothing, publishes nothing, and its
    /// cleanup removes only its own `.tier.part` files.
    cancelled: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for TierFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TierFetch")
            .field("position", &self.position)
            .field("copied", &self.copied.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl TierFetch {
    /// A fetch that never finishes (no thread) — for the node's timeout
    /// tests.
    #[cfg(test)]
    pub(crate) fn never_finishing_for_test(position: u64) -> TierFetch {
        TierFetch {
            position,
            copied: Arc::new(AtomicU64::new(0)),
            done: Arc::new(Mutex::new(None)),
            cancelled: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    /// Review C1: cancel the copy. Non-blocking; the caller keeps the
    /// `TierFetch` (draining) until [`Self::try_finish`] returns `Some`.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// The thread's result once it has finished (joining it), else `None`.
    /// Non-blocking until `done` is set; the join after that is the
    /// thread's last few instructions.
    pub fn try_finish(&mut self) -> Option<FetchResult> {
        let r = self.done.lock().unwrap_or_else(|e| e.into_inner()).take()?;
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        Some(r)
    }
}

/// The receiver's file names under `<root>/<row>/` (`<root>/cluster/` for
/// the cluster artifact), with the tier copy's OWN temp name (ruling RA3:
/// never the receiver's `incoming-<P>.part`, so a peer session for the same
/// P never shares a file with this copy).
fn artifact_paths(root: &Path, row: Option<u8>, p: u64) -> (PathBuf, PathBuf) {
    let (dir, fin) = match row {
        Some(r) => (root.join(r.to_string()), format!("snap-{p}.ultsnap")),
        None => (root.join("cluster"), format!("snap-{p}.ultcluster")),
    };
    (dir.join(format!("incoming-{p}.tier.part")), dir.join(fin))
}

/// Ruling D8: the `SET` must name exactly the catalog's agreed rows, with
/// the catalog's hashes AND versions (and sizes, where the catalog knows
/// one); the cluster artifact likewise. The copy then verifies each object
/// against the CATALOG's hash, never the `SET`'s own.
fn label_matches_catalog(label: &SetLabel, e: &SetEntry) -> bool {
    if label.position != e.position || !e.is_agreed() {
        return false;
    }
    let want = SetLabel::from_catalog(e, Vec::new());
    let size_ok = |catalog: u64, set: u64| catalog == 0 || catalog == set;
    let mut have = label.rows.clone();
    have.sort_by_key(|r| r.row);
    have.len() == want.rows.len()
        && have.iter().zip(&want.rows).all(|(h, w)| {
            h.row == w.row && h.hash == w.hash && h.version == w.version && size_ok(w.size, h.size)
        })
        && label.cluster_hash == e.cluster.hash
        && size_ok(e.cluster.size, label.cluster_size)
        && e.cluster.verdict == RowVerdict::Agreed
}

/// What one fetch has created, so a failure removes exactly that and leaves
/// the snapshot root as it was.
#[derive(Default)]
struct Written {
    /// `.tier.part` temp files not yet renamed.
    parts: Vec<PathBuf>,
    /// Final names THIS fetch created (a final already present before the
    /// rename — a peer session's or a local build's — is never removed).
    finals: Vec<PathBuf>,
    /// The `snap-<P>.foreign` marker, if THIS fetch created it.
    marker: Option<PathBuf>,
    /// Directories THIS fetch created (removed only if left empty).
    dirs: Vec<PathBuf>,
}

impl Written {
    /// Review C1: a CANCELLED copy's finals and marker may by now belong to
    /// the peer session that replaced it (same names, same verified bytes):
    /// only its own `.tier.part` temp files are its to remove.
    fn undo_parts_only(&mut self) {
        for p in self.parts.drain(..) {
            let _ = std::fs::remove_file(&p);
        }
        self.finals.clear();
        self.marker = None;
        self.dirs.clear();
    }

    fn undo(&mut self) {
        let files = self.parts.drain(..).chain(self.finals.drain(..));
        for p in files.chain(self.marker.take()) {
            let _ = std::fs::remove_file(&p);
        }
        // `remove_dir` refuses a non-empty directory, so a directory
        // something else wrote into meanwhile stays.
        for d in self.dirs.drain(..).rev() {
            let _ = std::fs::remove_dir(&d);
        }
    }
}

/// The marker [`write_foreign_marker`] makes for the set at `p` under
/// `root` (`<root>/cluster/snap-<P>.foreign`).
fn marker_path(root: &Path, p: u64) -> PathBuf {
    root.join("cluster").join(format!(
        "snap-{p}{}",
        uc_protocol::v2::catalog::FOREIGN_SET_SUFFIX
    ))
}

/// The thread body: copy, verify, rename every artifact; `Err(reason)` on
/// the first failure (the caller undoes what `w` names).
#[allow(clippy::too_many_arguments)]
fn copy_set(
    src: &TierSource,
    root: &Path,
    label: &SetLabel,
    expect: &SetEntry,
    copied: &AtomicU64,
    cancelled: &AtomicBool,
    w: &mut Written,
) -> Result<PathBuf, String> {
    let gone = || cancelled.load(Ordering::Acquire);
    let p = label.position;
    if !label_matches_catalog(label, expect) {
        return Err("verify".into());
    }
    let t = src.target();
    let catalog_rows = SetLabel::from_catalog(expect, Vec::new()).rows;
    let jobs = label
        .rows
        .iter()
        .map(|r| {
            let hash = catalog_rows
                .iter()
                .find(|c| c.row == r.row)
                .map_or(r.hash, |c| c.hash);
            (Some(r.row), row_object(p, r.row), r.size, hash, true)
        })
        .chain(std::iter::once((
            None,
            cluster_object(p),
            label.cluster_size,
            expect.cluster.hash,
            false,
        )));
    let mut cluster_final = PathBuf::new();
    for (row, object, size, hash, enveloped) in jobs {
        let delay = src.fetch_delay_ns.load(Ordering::Relaxed);
        if delay > 0 {
            std::thread::sleep(Duration::from_nanos(delay));
        }
        let (part, fin) = artifact_paths(root, row, p);
        let dir = part.parent().expect("artifact paths have a directory");
        // The marker lives in `<root>/cluster/`; note both dirs before
        // either can be created.
        for d in [dir.to_path_buf(), root.join("cluster")] {
            if !d.exists() && !w.dirs.contains(&d) {
                w.dirs.push(d);
            }
        }
        std::fs::create_dir_all(dir).map_err(|_| "io".to_string())?;
        w.parts.push(part.clone());
        if let Err(e) = copy_artifact(t, p, &object, &part, size, hash, enveloped) {
            return Err(match e {
                // The set went away under the copy (retention, a restore).
                _ if matches!(t.open(&object), Ok(None)) => "no_set",
                TierError::Io(_) => "io",
                _ => "verify",
            }
            .into());
        }
        copied.fetch_add(size, Ordering::Relaxed);
        // Ruling R42 (review m-A): the set is marked "not built here",
        // durably, before ANY of its artifacts becomes visible — exactly
        // what a peer session does.
        if gone() {
            return Err("cancelled".into());
        }
        let marker = marker_path(root, p);
        let had_marker = marker.exists();
        write_foreign_marker(&fin, p).map_err(|_| "io".to_string())?;
        if !had_marker && w.marker.is_none() {
            w.marker = Some(marker);
        }
        let existed = fin.exists();
        if gone() {
            return Err("cancelled".into());
        }
        std::fs::rename(&part, &fin).map_err(|_| "io".to_string())?;
        w.parts.retain(|x| x != &part);
        if !existed {
            w.finals.push(fin.clone());
        }
        // The receiver's directory fsync: the rename itself is durable
        // before the set is signalled. Best-effort, as there.
        let _ = std::fs::File::open(dir).and_then(|d| d.sync_all());
        if row.is_none() {
            cluster_final = fin;
        }
    }
    Ok(cluster_final)
}

/// Backup-tier 3b spec §6.2/§6.3, ruling PF4: spawn `uc2-tier-fetch`,
/// which copies the set `label` names from `src` into `root` (the node's
/// `snapshots/`): each row `sets/<P>/row-<id>.ultsnap` →
/// `<root>/<row>/incoming-<P>.tier.part` (RA3), verified against the
/// CATALOG entry `expect` (D8: raw length, envelope position, hash; and the
/// `SET`'s versions equal the catalog's), then foreign-marked and renamed to
/// `snap-<P>.ultsnap`; the cluster object likewise into
/// `<root>/cluster/snap-<P>.ultcluster`. On success it raises exactly the
/// signals a peer session's completion raises, per `mode`; on any failure it
/// removes every `.tier.part` and every final file it renamed into place,
/// and reports `Err(reason)`. It never panics the node: a panic inside the
/// copy is caught, undone and reported as `io`.
pub fn spawn_fetch(
    src: Arc<TierSource>,
    root: PathBuf,
    label: SetLabel,
    expect: SetEntry,
    mode: FetchMode,
    signals: SnapIntakeSignals,
) -> TierFetch {
    let position = label.position;
    let copied = Arc::new(AtomicU64::new(0));
    let done: Arc<Mutex<Option<FetchResult>>> = Arc::new(Mutex::new(None));
    src.fetches_spawned.fetch_add(1, Ordering::Relaxed);
    let cancelled = Arc::new(AtomicBool::new(false));
    let (c2, d2, x2) = (
        Arc::clone(&copied),
        Arc::clone(&done),
        Arc::clone(&cancelled),
    );
    let body = move || {
        let mut w = Written::default();
        let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            copy_set(&src, &root, &label, &expect, &c2, &x2, &mut w)
        }));
        let result = match run {
            // Review C1: cancelled (now or at any point) — touch nothing
            // but this copy's own temp files, signal nothing.
            _ if x2.load(Ordering::Acquire) => {
                w.undo_parts_only();
                Err("cancelled".to_string())
            }
            Ok(Ok(cluster_final)) => {
                match mode {
                    FetchMode::StoreOnly => signals.publish_stored(position),
                    FetchMode::Install => {
                        signals.publish_installed(position, position, cluster_final)
                    }
                }
                Ok(label.lineage.clone())
            }
            Ok(Err(reason)) => {
                w.undo();
                Err(reason)
            }
            Err(_) => {
                w.undo();
                Err("io".to_string())
            }
        };
        *d2.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
    };
    let handle = std::thread::Builder::new()
        .name("uc2-tier-fetch".into())
        .spawn(body);
    let handle = match handle {
        Ok(h) => Some(h),
        Err(_) => {
            *done.lock().unwrap_or_else(|e| e.into_inner()) = Some(Err("io".to_string()));
            None
        }
    };
    TierFetch {
        position,
        copied,
        done,
        cancelled,
        handle,
    }
}

/// `uc2_tier_install_total{outcome}` (3b spec §6.6), the FOLLOWER's half:
/// `installed` (a `TIER_INSTALL` this node served from its tier) and
/// `declined` (one it answered `declined`, at the door or after a failed
/// copy). The leader's `timeout` is the sender's own stat.
#[derive(Debug, Default)]
pub struct TierInstallStats {
    pub installed: AtomicU64,
    pub declined: AtomicU64,
}

/// The catalog entry a `TIER_INSTALL` body stands for: the LEADER's verdict
/// on the set at `body.s` (controller ruling on Task 13 concern 5). Every
/// row with a nonzero hash is `Agreed` with the carried hash, version and
/// size; the cluster artifact likewise; state `Complete`. `kind`/`time_ns`
/// are the `SET`'s (nothing verifies them). This is what ruling D8 checks
/// the tier's copy against on a node with no catalog of its own.
pub fn install_expect(body: &TierInstallBody, label: &SetLabel) -> SetEntry {
    let mut e = SetEntry::commanded(body.s, label.kind, label.time_ns);
    e.state = SetState::Complete;
    for (i, r) in e.rows.iter_mut().enumerate() {
        if body.row_hash[i] != 0 {
            *r = RowEntry {
                version: body.row_version[i],
                hash: body.row_hash[i],
                verdict: RowVerdict::Agreed,
                size: body.row_size[i],
            };
        }
    }
    e.cluster = RowEntry {
        version: 0,
        hash: body.cluster_hash,
        verdict: RowVerdict::Agreed,
        size: body.cluster_size,
    };
    e
}

/// Backup-tier 3b spec §6.3 step 1 (ruling PF6, as amended): may this node
/// install the set the leader NAMED (`body.s`) from its tier? The leader
/// chose S (the newest agreed set in `[floor, commit]`, Task 14) and
/// carries its catalog verdict; the follower checks only what the leader
/// cannot know:
/// - `through` None (no `TIER`, a foreign one, or an id the cluster has not
///   registered) → `NotRegistered`;
/// - `body.s` outside `[floor, min(commit, through)]` → `NoSet` (the reader
///   already refuses `s` outside `[floor, commit]`; re-checked here);
/// - no `SET` at exactly `body.s` among `sets` → `NoSet`;
/// - a `SET` whose rows / versions / hashes / sizes disagree with the
///   carried verdict → `Verify`, before any copy (ruling D8).
///
/// `Ok` = the tier's `SET` and the catalog entry built FROM THE BODY that
/// the copy is verified against. Pure.
pub fn check_install_set(
    body: &TierInstallBody,
    through: Option<u64>,
    sets: &[SetLabel],
) -> Result<(SetLabel, SetEntry), TierInstallReason> {
    let Some(through) = through else {
        return Err(TierInstallReason::NotRegistered);
    };
    if body.s < body.floor || body.s > body.commit.min(through) {
        return Err(TierInstallReason::NoSet);
    }
    let Some(label) = sets.iter().find(|l| l.position == body.s) else {
        return Err(TierInstallReason::NoSet);
    };
    let expect = install_expect(body, label);
    if !label_matches_catalog(label, &expect) {
        return Err(TierInstallReason::Verify);
    }
    Ok((label.clone(), expect))
}

/// Backup-tier 3b spec §6.3 (Task 14 ruling), the LEADER: the
/// `TIER_INSTALL` body naming the set a below-floor follower should install
/// from its tier, carrying this node's committed verdict for it. S is the
/// newest AGREED set with `floor <= S <= commit` that is also at most the
/// highest committed `through` over the registered tiers (the leader cannot
/// tell which tier the follower reads, so the best-covered one bounds it);
/// only if none qualifies, the newest agreed set in `[floor, commit]` (the
/// follower's tier may still hold it). `None` = no agreed set in range at
/// all: skip the exchange and open the session. Rows: each `Agreed` row's
/// hash/version/size in its slot, the rest zero. Pure.
pub fn leader_install_body(
    floor: u64,
    commit: u64,
    catalog: &[SetEntry],
    backups: &[BackupTierEntry],
) -> Option<TierInstallBody> {
    let through = backups.iter().map(|b| b.through).max();
    let in_range = |e: &&SetEntry| e.is_agreed() && floor <= e.position && e.position <= commit;
    let newest = |bound: u64| {
        catalog
            .iter()
            .filter(in_range)
            .filter(|e| e.position <= bound)
            .max_by_key(|e| e.position)
    };
    let e = through.and_then(newest).or_else(|| newest(u64::MAX))?;
    let mut b = TierInstallBody {
        floor,
        commit,
        s: e.position,
        row_hash: [0; 8],
        row_version: [0; 8],
        row_size: [0; 8],
        cluster_hash: e.cluster.hash,
        cluster_size: e.cluster.size,
    };
    for (i, r) in e.rows.iter().enumerate() {
        if r.verdict == RowVerdict::Agreed {
            b.row_hash[i] = r.hash;
            b.row_version[i] = r.version;
            b.row_size[i] = r.size;
        }
    }
    Some(b)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::tier::layout::{TIER_FILE, TierInfo};

    fn tmp() -> tempfile::TempDir {
        let root = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_node_tests")
            });
        std::fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("uc2-tier-")
            .tempdir_in(&root)
            .unwrap()
    }

    pub(crate) fn put_tier(dir: &Path, app: &str, id: u64) {
        let t = TierInfo {
            app_id: app.into(),
            tier_id: id,
            created_ns: 1,
        };
        std::fs::write(dir.join(TIER_FILE), t.render()).unwrap();
    }

    fn entry(tier_id: u64, through: u64) -> BackupTierEntry {
        BackupTierEntry {
            tier_id,
            oldest_set: 0,
            newest_set: 0,
            through,
            reporter: 1,
            updated_pos: 0,
        }
    }

    #[test]
    fn open_refuses_a_foreign_tier_and_tolerates_an_absent_one() {
        let d = tmp();
        let s = TierSource::open(d.path(), "mine").expect("absent TIER is tolerated");
        assert!(s.unavailable().is_some());
        assert_eq!(s.registered_through(&[entry(7, 100)]), None);
        // A tier mounted later works without reopening.
        put_tier(d.path(), "mine", 7);
        assert_eq!(s.unavailable(), None);
        assert_eq!(s.registered_through(&[entry(7, 100)]), Some(100));
        let f = tmp();
        put_tier(f.path(), "other", 7);
        let e = TierSource::open(f.path(), "mine").err().expect("foreign");
        assert!(e.to_string().contains("tier_source_foreign"), "{e}");
        assert!(!e.to_string().contains("backup_target_foreign"), "{e}");
        // A nonexistent path is also tolerated.
        TierSource::open(&d.path().join("nope"), "mine").unwrap();
    }

    // ------------------------------------------------ the fetch thread

    use crate::tier::layout::{RowLabel, set_label};
    use crate::tier::object::{Codec, MAGIC_ARTIFACT, write_object};
    use crate::tier::target::Put;
    use std::io::{Cursor, Write};
    use uc_protocol::v2::catalog::{RowEntry, SetKind, SetState};
    use uc_service::snapshots::{artifact_hash_of, write_snapshot_envelope};

    pub(crate) const P: u64 = 4096;
    pub(crate) const VERSION: u32 = 0x0001_0000;

    fn put(t: &dyn BackupTarget, path: &str, raw: &[u8]) {
        let n = raw.len() as u64;
        let r = t
            .put_if_absent(path, &mut |f| {
                write_object(
                    f,
                    MAGIC_ARTIFACT,
                    Codec::Zstd,
                    3,
                    P,
                    P,
                    &mut Cursor::new(raw),
                    n,
                )
            })
            .unwrap();
        assert!(matches!(r, Put::Written(_)));
    }

    pub(crate) struct SetFx {
        pub(crate) _d: tempfile::TempDir,
        pub(crate) src: Arc<TierSource>,
        pub(crate) root: PathBuf,
        pub(crate) label: SetLabel,
        pub(crate) entry: SetEntry,
        pub(crate) row: Vec<u8>,
        pub(crate) cluster: Vec<u8>,
    }

    /// A two-row set at `P` in a tier, its `SET`, and the catalog entry
    /// that agrees it. `bad_cluster`: the tier's cluster object holds other
    /// bytes than the catalog agreed (a mismatching artifact).
    pub(crate) fn set_fx(bad_cluster: bool) -> SetFx {
        let d = tmp();
        let tier = d.path().join("tier");
        let root = d.path().join("snapshots");
        std::fs::create_dir_all(&root).unwrap();
        let src = Arc::new(TierSource::open(&tier, "mine").unwrap());
        let t = src.target();
        let mut row = Vec::new();
        write_snapshot_envelope(&mut row, P, VERSION).unwrap();
        row.extend_from_slice(b"row payload at 4096");
        let payload_hash = artifact_hash_of(&row[24..]);
        let cluster = b"the cluster image at 4096".to_vec();
        let mut entry = SetEntry::commanded(P, SetKind::Full, 77);
        entry.state = SetState::Complete;
        let agreed = |hash, size| RowEntry {
            version: VERSION,
            hash,
            verdict: RowVerdict::Agreed,
            size,
        };
        entry.rows[0] = agreed(payload_hash, row.len() as u64);
        entry.rows[2] = agreed(payload_hash, row.len() as u64);
        entry.cluster = RowEntry {
            version: 0,
            ..agreed(artifact_hash_of(&cluster), cluster.len() as u64)
        };
        put(t, &row_object(P, 0), &row);
        put(t, &row_object(P, 2), &row);
        let mut placed = cluster.clone();
        if bad_cluster {
            placed[0] ^= 0xFF;
        }
        put(t, &cluster_object(P), &placed);
        let rl = |r| RowLabel {
            row: r,
            hash: payload_hash,
            version: VERSION,
            size: row.len() as u64,
        };
        let label = SetLabel {
            position: P,
            kind: SetKind::Full,
            time_ns: 77,
            rows: vec![rl(0), rl(2)],
            cluster_hash: artifact_hash_of(&cluster),
            cluster_size: cluster.len() as u64,
            lineage: vec![(1, 0), (3, 2048)],
        };
        let text = label.render();
        t.put_if_absent(&set_label(P), &mut |f| {
            f.write_all(text.as_bytes())?;
            Ok(text.len() as u64)
        })
        .unwrap();
        SetFx {
            _d: d,
            src,
            root,
            label,
            entry,
            row,
            cluster,
        }
    }

    fn run(fx: &SetFx) -> (FetchResult, Arc<AtomicU64>) {
        let stored = Arc::new(AtomicU64::new(0));
        let signals = SnapIntakeSignals::new(Arc::clone(&stored), None);
        let mut f = spawn_fetch(
            Arc::clone(&fx.src),
            fx.root.clone(),
            fx.label.clone(),
            fx.entry.clone(),
            FetchMode::StoreOnly,
            signals,
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(r) = f.try_finish() {
                return (r, stored);
            }
            assert!(std::time::Instant::now() < deadline, "fetch never finished");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// Every file under `root`, relative, sorted.
    fn files(root: &Path) -> Vec<String> {
        let mut out = Vec::new();
        for d in std::fs::read_dir(root).unwrap() {
            let d = d.unwrap().path();
            for f in std::fs::read_dir(&d).unwrap() {
                let f = f.unwrap().path();
                out.push(f.strip_prefix(root).unwrap().display().to_string());
            }
        }
        out.sort();
        out
    }

    #[test]
    fn a_store_only_fetch_from_the_tier_lands_every_artifact_with_the_foreign_marker() {
        let fx = set_fx(false);
        let (r, stored) = run(&fx);
        assert_eq!(
            r,
            Ok(fx.label.lineage.clone()),
            "Ok carries the SET's lineage"
        );
        assert_eq!(stored.load(Ordering::Acquire), P, "publish_stored(P)");
        assert_eq!(
            files(&fx.root),
            vec![
                "0/snap-4096.ultsnap".to_string(),
                "2/snap-4096.ultsnap".to_string(),
                "cluster/snap-4096.foreign".to_string(),
                "cluster/snap-4096.ultcluster".to_string(),
            ],
            "every artifact under its final name, the marker, no .part"
        );
        for row in ["0", "2"] {
            let got = std::fs::read(fx.root.join(row).join("snap-4096.ultsnap")).unwrap();
            assert_eq!(got, fx.row);
        }
        let got = std::fs::read(fx.root.join("cluster/snap-4096.ultcluster")).unwrap();
        assert_eq!(got, fx.cluster);
        assert_eq!(fx.src.fetches_spawned(), 1);
    }

    #[test]
    fn a_mismatching_artifact_removes_what_it_wrote_and_reports_verify() {
        let fx = set_fx(true);
        let (r, stored) = run(&fx);
        assert_eq!(r, Err("verify".to_string()));
        assert_eq!(stored.load(Ordering::Acquire), 0, "nothing is signalled");
        // The rows landed before the cluster object failed; they, the
        // marker and the directories the copy made are all gone.
        assert!(
            std::fs::read_dir(&fx.root).unwrap().next().is_none(),
            "the snapshot root is as it was: {:?}",
            files(&fx.root)
        );
    }

    #[test]
    fn a_set_that_disagrees_with_the_catalog_version_is_refused_before_any_copy() {
        let mut fx = set_fx(false);
        fx.entry.rows[2].version = VERSION + 1;
        let (r, _) = run(&fx);
        assert_eq!(r, Err("verify".to_string()));
        assert!(std::fs::read_dir(&fx.root).unwrap().next().is_none());
    }

    /// Review C1: a copy cancelled mid-flight (the node fell back to a peer,
    /// whose session writes the same final names) renames nothing more,
    /// signals nothing, and removes only its own `.tier.part` files — never
    /// a final or the marker, which may now be the peer's.
    #[test]
    fn a_cancelled_copy_leaves_the_peers_finals_and_marker_alone() {
        let fx = set_fx(false);
        fx.src.set_fetch_delay_for_test(Duration::from_millis(400));
        let stored = Arc::new(AtomicU64::new(0));
        let mut f = spawn_fetch(
            Arc::clone(&fx.src),
            fx.root.clone(),
            fx.label.clone(),
            fx.entry.clone(),
            FetchMode::StoreOnly,
            SnapIntakeSignals::new(Arc::clone(&stored), None),
        );
        let first = fx.root.join("0/snap-4096.ultsnap");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !first.exists() {
            assert!(std::time::Instant::now() < deadline, "row 0 never landed");
            std::thread::sleep(Duration::from_millis(2));
        }
        f.cancel();
        assert!(f.is_cancelled());
        // The peer session's copy of row 2 lands under the final name, and
        // the tier loses its cluster object (a late failure, were it to run).
        let peer_final = fx.root.join("2/snap-4096.ultsnap");
        std::fs::create_dir_all(peer_final.parent().unwrap()).unwrap();
        std::fs::write(&peer_final, b"the peer's row 2").unwrap();
        fx.src.target().delete(&cluster_object(P)).unwrap();
        let r = loop {
            if let Some(r) = f.try_finish() {
                break r;
            }
            assert!(std::time::Instant::now() < deadline, "never finished");
            std::thread::sleep(Duration::from_millis(2));
        };
        assert_eq!(r, Err("cancelled".to_string()));
        assert_eq!(stored.load(Ordering::Acquire), 0, "nothing signalled");
        assert_eq!(std::fs::read(&peer_final).unwrap(), b"the peer's row 2");
        assert_eq!(std::fs::read(&first).unwrap(), fx.row, "a final is kept");
        assert!(
            fx.root.join("cluster/snap-4096.foreign").exists(),
            "marker kept"
        );
        assert!(
            !files(&fx.root).iter().any(|f| f.ends_with(".tier.part")),
            "{:?}",
            files(&fx.root)
        );
    }

    #[test]
    fn the_tier_copy_never_uses_the_receivers_temp_names() {
        let fx = set_fx(false);
        // A peer session mid-copy of the same set: its `.part`.
        let dir = fx.root.join("0");
        std::fs::create_dir_all(&dir).unwrap();
        let theirs = dir.join("incoming-4096.part");
        std::fs::write(&theirs, b"half a peer session").unwrap();
        let (r, _) = run(&fx);
        assert!(r.is_ok(), "{r:?}");
        assert_eq!(std::fs::read(&theirs).unwrap(), b"half a peer session");
        let got = std::fs::read(dir.join("snap-4096.ultsnap")).unwrap();
        assert_eq!(artifact_hash_of(&got[24..]), fx.entry.rows[0].hash);
        assert!(!dir.join("incoming-4096.tier.part").exists());
    }

    #[test]
    fn registered_through_is_none_for_an_unregistered_tier() {
        let d = tmp();
        put_tier(d.path(), "mine", 7);
        let s = TierSource::open(d.path(), "mine").unwrap();
        assert_eq!(s.registered_through(&[]), None);
        assert_eq!(s.registered_through(&[entry(9, 5)]), None);
        assert_eq!(s.registered_through(&[entry(9, 5), entry(7, 42)]), Some(42));
    }

    // ------------------------------------------------ check_install_set

    fn label_at(p: u64) -> SetLabel {
        SetLabel {
            position: p,
            kind: SetKind::Full,
            time_ns: 0,
            rows: Vec::new(),
            cluster_hash: 0,
            cluster_size: 0,
            lineage: Vec::new(),
        }
    }

    pub(crate) fn body_at(floor: u64, commit: u64, s: u64) -> TierInstallBody {
        TierInstallBody {
            floor,
            commit,
            s,
            row_hash: [0; 8],
            row_version: [0; 8],
            row_size: [0; 8],
            cluster_hash: 0,
            cluster_size: 0,
        }
    }

    /// The body the leader would send for `fx`'s set: its catalog verdict.
    pub(crate) fn body_for(fx: &SetFx, floor: u64, commit: u64) -> TierInstallBody {
        let e = &fx.entry;
        let mut b = body_at(floor, commit, e.position);
        for (i, r) in e.rows.iter().enumerate() {
            if r.verdict == RowVerdict::Agreed {
                b.row_hash[i] = r.hash;
                b.row_version[i] = r.version;
                b.row_size[i] = r.size;
            }
        }
        b.cluster_hash = e.cluster.hash;
        b.cluster_size = e.cluster.size;
        b
    }

    /// Ruling PF6 as amended, exhaustively over a small space: a body that
    /// survives the reader is accepted iff its `s` is at most `through` and
    /// the tier holds a `SET` at exactly `s` — never below the floor, never
    /// above commit or `through`; `NotRegistered` exactly when `through` is
    /// None; `NoSet` otherwise. Which S to name is the leader's (Task 14).
    #[test]
    fn check_install_set_never_lands_below_the_floor_or_above_commit() {
        use uc_protocol::v2::datagram::{
            TIER_INSTALL_BODY_LEN, read_tier_install_body, write_tier_install_body,
        };
        const AT: [u64; 4] = [2, 5, 8, 11];
        let sets: Vec<SetLabel> = AT.iter().map(|&p| label_at(p)).collect();
        let (mut cases, mut oks, mut refused_by_reader) = (0u64, 0u64, 0u64);
        for floor in 0..=12u64 {
            for commit in 0..=12u64 {
                for s in 0..=12u64 {
                    let mut wire = [0u8; TIER_INSTALL_BODY_LEN];
                    write_tier_install_body(&mut wire, &body_at(floor, commit, s));
                    let Some(body) = read_tier_install_body(&wire) else {
                        refused_by_reader += 1;
                        assert!(floor > commit || s < floor || s > commit);
                        continue;
                    };
                    for through in std::iter::once(None).chain((0..=12u64).map(Some)) {
                        cases += 1;
                        let want = through.is_some_and(|t| s <= t) && AT.contains(&s);
                        match check_install_set(&body, through, &sets) {
                            Ok((l, e)) => {
                                oks += 1;
                                assert!(want, "{floor} {commit} {s} {through:?}");
                                let t = through.unwrap();
                                assert_eq!(l.position, s);
                                assert!(floor <= s && s <= commit.min(t));
                                assert_eq!(e.position, s);
                                assert!(e.is_agreed(), "the body's verdict is agreed");
                            }
                            Err(TierInstallReason::NotRegistered) => assert!(through.is_none()),
                            Err(TierInstallReason::NoSet) => {
                                assert!(
                                    through.is_some() && !want,
                                    "{floor} {commit} {s} {through:?}"
                                );
                            }
                            Err(r) => panic!("unexpected reason {r:?}"),
                        }
                    }
                }
            }
        }
        assert!(oks > 0 && refused_by_reader > 0 && cases > 0, "non-vacuous");
    }

    #[test]
    fn check_install_set_refuses_an_unregistered_tier() {
        let sets = [label_at(5)];
        let body = body_at(0, 10, 5);
        assert_eq!(
            check_install_set(&body, None, &sets).err(),
            Some(TierInstallReason::NotRegistered),
            "the named set in the tier does not make an unregistered tier servable"
        );
        assert_eq!(
            check_install_set(&body, Some(10), &sets)
                .map(|(l, _)| l.position)
                .ok(),
            Some(5)
        );
        assert_eq!(
            check_install_set(&body, Some(4), &sets).err(),
            Some(TierInstallReason::NoSet),
            "never past the registered through"
        );
    }

    /// Ruling D8 with the leader as the authority: a body built from the
    /// catalog verdict accepts the matching `SET`; one whose carried version
    /// (or hash) disagrees with the tier's `SET` is `Verify` before any copy.
    #[test]
    fn check_install_set_verifies_the_set_against_the_carried_verdict() {
        let fx = set_fx(false);
        let body = body_for(&fx, 0, 8000);
        let (l, e) =
            check_install_set(&body, Some(8000), std::slice::from_ref(&fx.label)).expect("matches");
        assert_eq!(l, fx.label);
        assert!(label_matches_catalog(&l, &e));
        let mut v = body;
        v.row_version[2] += 1;
        assert_eq!(
            check_install_set(&v, Some(8000), std::slice::from_ref(&fx.label)).err(),
            Some(TierInstallReason::Verify)
        );
        let mut h = body;
        h.cluster_hash ^= 1;
        assert_eq!(
            check_install_set(&h, Some(8000), std::slice::from_ref(&fx.label)).err(),
            Some(TierInstallReason::Verify)
        );
    }

    /// Task 14 ruling: the newest agreed set in `[floor, commit]` bounded by
    /// the best-covered tier's `through`; else the newest agreed in range;
    /// none in range → `None`. Never a non-agreed set, never outside range.
    #[test]
    fn leader_install_body_names_the_newest_agreed_set_the_tiers_cover() {
        use uc_protocol::v2::catalog::SetKind;
        let set = |p: u64, agreed: bool| {
            let mut e = SetEntry::commanded(p, SetKind::Full, p);
            e.rows[0] = RowEntry {
                version: 3,
                hash: p ^ 0xAB,
                verdict: if agreed {
                    RowVerdict::Agreed
                } else {
                    RowVerdict::Diverged
                },
                size: p + 1,
            };
            e.cluster = RowEntry {
                version: 0,
                hash: p ^ 0xCD,
                verdict: RowVerdict::Agreed,
                size: 5,
            };
            e.state = SetState::Complete;
            e
        };
        let tier = |through: u64| BackupTierEntry {
            tier_id: through + 1,
            oldest_set: 0,
            newest_set: through,
            through,
            reporter: 1,
            updated_pos: 1,
        };
        let cat = [
            set(1000, true),
            set(2000, true),
            set(3000, false),
            set(4000, true),
        ];
        let s_of = |floor, commit, b: &[BackupTierEntry]| {
            leader_install_body(floor, commit, &cat, b).map(|b| b.s)
        };
        // Bounded by the highest through (two tiers: 1500 and 2500 → 2500).
        assert_eq!(s_of(0, 5000, &[tier(1500), tier(2500)]), Some(2000));
        // Never the diverged 3000, even when through covers it.
        assert_eq!(s_of(2500, 3500, &[tier(3500)]), None);
        // No tier covers anything in range → the newest agreed in range.
        assert_eq!(s_of(1500, 5000, &[tier(1200)]), Some(4000));
        assert_eq!(s_of(0, 5000, &[]), Some(4000));
        // Commit bounds it; the floor bounds it.
        assert_eq!(s_of(0, 3999, &[]), Some(2000));
        assert_eq!(s_of(4001, 9000, &[]), None);
        // The body carries the set's verdict.
        let b = leader_install_body(0, 5000, &cat, &[tier(1500)]).unwrap();
        assert_eq!((b.floor, b.commit, b.s), (0, 5000, 1000));
        assert_eq!(
            (b.row_hash[0], b.row_version[0], b.row_size[0]),
            (1000 ^ 0xAB, 3, 1001)
        );
        assert_eq!(&b.row_hash[1..], &[0; 7]);
        assert_eq!((b.cluster_hash, b.cluster_size), (1000 ^ 0xCD, 5));
    }
}
