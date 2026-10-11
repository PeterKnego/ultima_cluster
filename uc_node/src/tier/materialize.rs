//! Backup-tier spec §8.1: the offline restore builder.
//!
//! [`materialize`] turns a tier into an instance directory a node boots from
//! unchanged: the start set's artifacts, a fresh journal holding the chain's
//! bytes `[S, Q)`, and a `state/` the builder WRITES (never copies) — the
//! snapshot floor S, a term map derived from the restored frame headers, the
//! cluster artifact's membership as the config record at S, no vote, and the
//! output-progress marker per mode. Everything is assembled under
//! `<out_dir>/.restore-tmp/`, checked by [`crate::backup::verify_artifact`],
//! and only then renamed into place; a refusal leaves `out_dir` as it was.
//!
//! Tier chunks are byte-exact copies of the committed log except padding
//! bodies and alignment slack, which the writer zeroes (ruling R8a); the
//! builder writes them as they are — nothing reads a padding body.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::Path;

use sha2::Digest;
use uc_journal::{Journal, JournalConfig};
use uc_log::state::{ConfigRecord, NodeState, TermMapEntry};
use uc_protocol::v2::frame::{FRAME_TYPE_PADDING, HEADER_LEN, align_frame_len, read_header};
use uc_service::snapshots::{SNAPSHOT_ENVELOPE_LEN, SnapshotStore, decode_snapshot_envelope};

use super::TierError;
use super::chain::{TierListing, list_tier, reach_from, read_range};
use super::layout::{SetLabel, cluster_object, row_object};
use super::object::{MAGIC_ARTIFACT, ObjectReader};
use super::target::{BackupTarget, read_tier};
use crate::cluster_agent;
use crate::cluster_fsm::ClusterState;

/// What the restored directory is for (spec §8.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Whole-cluster restore: every node rebuilt from the same tier and Q.
    Recovery,
    /// One replacement node, seeded from the tier under a NEW id.
    NewNode { node_id: u32 },
    /// A separate cluster as of Q (`uc2ctl tier fork`).
    Fork,
}

impl Mode {
    fn label(&self) -> String {
        match self {
            Mode::Recovery => "recovery".to_string(),
            Mode::NewNode { node_id } => format!("new-node:{node_id}"),
            Mode::Fork => "fork".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct Materialized {
    pub start_set: u64,
    pub q: u64,
    pub mode: Mode,
}

/// The assembly directory under `out_dir`.
pub const RESTORE_TMP: &str = ".restore-tmp";
/// The audit-trail file the builder leaves; nothing reads it.
pub const RESTORED_FILE: &str = "RESTORED";
/// One journal block's cap — the archive's own (`ArchiveConfig::new`).
const MAX_BLOCK_BYTES: usize = 1 << 20;

fn refused(msg: impl Into<String>) -> TierError {
    TierError::Refused(msg.into())
}

fn corrupt(what: impl Into<String>, detail: impl Into<String>) -> TierError {
    TierError::Corrupt {
        what: what.into(),
        detail: detail.into(),
    }
}

/// Every `(set, reach)` a restore can start from — a complete set with at
/// least one byte of chain after it — merged into disjoint `[a, b]` ranges.
/// A restore point Q is a frame end with `a < Q <= b`.
pub fn restorable_ranges(l: &TierListing) -> Vec<(u64, u64)> {
    let mut spans: Vec<(u64, u64)> = l
        .sets
        .iter()
        .map(|s| (s.position, reach_from(&l.chunks, s.position)))
        .filter(|&(p, r)| r > p)
        .collect();
    spans.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::new();
    for (a, b) in spans {
        match out.last_mut() {
            Some(last) if a <= last.1 => last.1 = last.1.max(b),
            _ => out.push((a, b)),
        }
    }
    out
}

fn unreachable(l: &TierListing, why: &str) -> TierError {
    let ranges = restorable_ranges(l);
    let list = if ranges.is_empty() {
        "none (no complete set with journal after it)".to_string()
    } else {
        ranges
            .iter()
            .map(|(a, b)| format!("[{a}, {b}]"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    refused(format!(
        "restore_point_unreachable: restorable {list} ({why})"
    ))
}

/// `(S, Q)` for a restore at `q` (default: the furthest reach). S is the
/// newest complete set strictly below Q whose chain reaches Q; Q == S is
/// refused because an empty journal cannot carry the floor across a boot.
/// Whether Q is a frame end is only known while streaming — [`materialize`]
/// checks it there.
pub fn restore_point(l: &TierListing, q: Option<u64>) -> Result<(u64, u64), TierError> {
    let spans: Vec<(u64, u64)> = l
        .sets
        .iter()
        .map(|s| (s.position, reach_from(&l.chunks, s.position)))
        .collect();
    let q = match q {
        Some(q) => q,
        None => spans
            .iter()
            .map(|&(_, r)| r)
            .max()
            .ok_or_else(|| unreachable(l, "the tier has no complete set"))?,
    };
    spans
        .iter()
        .filter(|&&(p, r)| p < q && r >= q)
        .map(|&(p, _)| p)
        .max()
        .map(|s| (s, q))
        .ok_or_else(|| unreachable(l, &format!("asked Q={q}")))
}

/// Build an instance directory at `out_dir` from the tier at `t`, as of `q`
/// (spec §8.1). Offline: no node may hold `out_dir`, and its `journal/`,
/// `state/` and `snapshots/` must be absent or empty. On any refusal
/// `out_dir` is left as it was (and removed again if this call created it).
pub fn materialize(
    t: &dyn BackupTarget,
    q: Option<u64>,
    out_dir: &Path,
    mode: Mode,
    now_ns: u64,
) -> Result<Materialized, TierError> {
    refuse_target(out_dir)?;
    let info = read_tier(t)?.ok_or_else(|| refused("no tier at the target (no TIER file)"))?;
    let listing = list_tier(t)?;
    let (s, q) = restore_point(&listing, q)?;
    let label = listing
        .sets
        .iter()
        .find(|l| l.position == s)
        .expect("restore_point picks a listed set")
        .clone();

    let created = !out_dir.exists();
    fs::create_dir_all(out_dir)?;
    let tmp = out_dir.join(RESTORE_TMP);
    let built = (|| {
        if tmp.exists() {
            // An interrupted build's residue: ours, never a node's.
            fs::remove_dir_all(&tmp)?;
        }
        fs::create_dir(&tmp)?;
        build(t, &listing, &label, q, &tmp, mode)?;
        write_restored(&tmp, info.tier_id, s, q, mode, now_ns)?;
        crate::backup::verify_artifact(&tmp)
            .map_err(|e| refused(format!("restore verify of the assembled directory: {e}")))
            .and_then(|r| {
                if (r.journal_first_base, r.journal_last_pos, r.snapshot_floor) == (s, q, s) {
                    Ok(())
                } else {
                    Err(refused(format!(
                        "restore verify: assembled journal [{}, {}) floor {} != [{s}, {q}) floor {s}",
                        r.journal_first_base, r.journal_last_pos, r.snapshot_floor
                    )))
                }
            })?;
        place(&tmp, out_dir)
    })();
    match built {
        Ok(()) => Ok(Materialized {
            start_set: s,
            q,
            mode,
        }),
        Err(e) => {
            let _ = fs::remove_dir_all(&tmp);
            if created {
                let _ = fs::remove_dir(out_dir);
            }
            Err(e)
        }
    }
}

const DURABLE_DIRS: [&str; 3] = ["journal", "state", "snapshots"];

/// `restore_artifact`'s target rules: a held `instance.lock` (a running
/// node) or a non-empty durable directory is refused; volatile leftovers
/// (`cnc2.dat`, rings, an unheld lock) are fine.
fn refuse_target(out_dir: &Path) -> Result<(), TierError> {
    if crate::ipc::probe_instance_lock(out_dir)? == crate::ipc::InstanceLock::Held {
        return Err(refused(format!(
            "{}: a running node holds this instance directory's instance.lock — stop it first",
            out_dir.display()
        )));
    }
    for d in DURABLE_DIRS {
        let p = out_dir.join(d);
        if p.is_dir() && fs::read_dir(&p)?.next().is_some() {
            return Err(refused(format!(
                "TargetNotEmpty: {} is not empty (refusing to merge or overwrite; use a fresh \
                 instance directory)",
                p.display()
            )));
        }
    }
    Ok(())
}

/// Steps 3–6 of spec §8.1 into `tmp`.
fn build(
    t: &dyn BackupTarget,
    l: &TierListing,
    label: &SetLabel,
    q: u64,
    tmp: &Path,
    mode: Mode,
) -> Result<(), TierError> {
    let s = label.position;
    // Artifacts first: a set that vanishes is refused before any journal work.
    for r in &label.rows {
        let dst = SnapshotStore::open(tmp, r.row)?.path_for(s);
        copy_artifact(t, s, &row_object(s, r.row), &dst, r.size, r.hash, true)?;
    }
    let cdir = cluster_agent::snapshot_dir_of(tmp);
    fs::create_dir_all(&cdir)?;
    copy_artifact(
        t,
        s,
        &cluster_object(s),
        &cluster_agent::artifact_path(&cdir, s),
        label.cluster_size,
        label.cluster_hash,
        false,
    )?;
    sync_dir(&cdir)?;

    let (fsm, at) = cluster_agent::recover(&cdir, ClusterState::genesis_empty(), Vec::new())?;
    if at != s {
        return Err(corrupt(
            cluster_object(s),
            format!("installed at {at}, set is {s}"),
        ));
    }
    let membership = fsm.state().membership.clone();
    if let Mode::NewNode { node_id } = mode {
        let m = &membership;
        if m.voters.iter().any(|(id, _)| *id == node_id)
            || m.learners.iter().any(|(id, _)| *id == node_id)
            || m.tombstones.contains(&node_id)
        {
            return Err(refused(format!(
                "restore_id_reused: node id {node_id} is a voter, learner or tombstone of the \
                 membership at set {s} — a replacement node takes a NEW id, then `uc2ctl add`"
            )));
        }
    }

    let terms = write_journal(t, l, s, q, &tmp.join("journal"))?;

    fs::create_dir_all(tmp.join("state"))?;
    let state = NodeState::open(&tmp.join("state")).map_err(io::Error::other)?;
    let stored = crate::node::cluster_to_stored(&membership);
    state.store_snapshot_floor(s).map_err(io::Error::other)?;
    state.store_term_map(&terms).map_err(io::Error::other)?;
    state
        .store_config_record(&ConfigRecord {
            position: s,
            config: stored.clone(),
            prev_position: s,
            prev: stored,
        })
        .map_err(io::Error::other)?;
    state
        .store_output_progress(match mode {
            Mode::Fork => q,
            Mode::Recovery | Mode::NewNode { .. } => s,
        })
        .map_err(io::Error::other)?;
    Ok(())
}

/// Stream one artifact object to `dst`, checking it against its `SET` row:
/// the raw length, the hash (over the payload after the 24-byte envelope for
/// a row, over the whole image for the cluster artifact), and a row's
/// envelope position.
fn copy_artifact(
    t: &dyn BackupTarget,
    s: u64,
    path: &str,
    dst: &Path,
    size: u64,
    hash: u64,
    enveloped: bool,
) -> Result<(), TierError> {
    let f = t
        .open(path)?
        .ok_or_else(|| refused(format!("restore set {s} incomplete: {path} missing")))?;
    let mut r = ObjectReader::open(f)?;
    if r.header().magic != MAGIC_ARTIFACT {
        return Err(corrupt(path, "not an artifact object"));
    }
    if r.header().raw_len != size {
        return Err(refused(format!(
            "restore set {s}: {path} raw length {} != SET size {size} (tier_set_mismatch)",
            r.header().raw_len
        )));
    }
    let mut out = File::create(dst)?;
    let mut h = sha2::Sha256::new();
    let mut head: Vec<u8> = Vec::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut n_total = 0u64;
    loop {
        let n = r.read(&mut buf)?;
        if n == 0 {
            break;
        }
        out.write_all(&buf[..n])?;
        n_total += n as u64;
        let mut piece = &buf[..n];
        if enveloped && head.len() < SNAPSHOT_ENVELOPE_LEN {
            let take = (SNAPSHOT_ENVELOPE_LEN - head.len()).min(piece.len());
            head.extend_from_slice(&piece[..take]);
            piece = &piece[take..];
        }
        h.update(piece);
    }
    if n_total != size {
        return Err(refused(format!(
            "restore set {s}: {path} streamed {n_total} bytes, SET says {size} (tier_set_mismatch)"
        )));
    }
    if enveloped {
        let env = decode_snapshot_envelope(&head)
            .map_err(|e| refused(format!("restore set {s}: {path} envelope: {e}")))?;
        if env.position != s {
            return Err(refused(format!(
                "restore set {s}: {path} envelope position {} != set (tier_set_mismatch)",
                env.position
            )));
        }
    }
    let d = h.finalize();
    let got = u64::from_le_bytes(d[..8].try_into().expect("8 bytes"));
    if got != hash {
        return Err(refused(format!(
            "restore set {s}: {path} hash {got:#018x} != SET {hash:#018x} (tier_set_mismatch)"
        )));
    }
    out.sync_all()?;
    Ok(())
}

/// Journal blocks under construction from a frame-aligned byte stream.
struct BlockWriter {
    journal: Journal,
    seq: u64,
    /// Bytes not yet a whole frame, starting at `pending_base`.
    pending: Vec<u8>,
    pending_base: u64,
    block: Vec<u8>,
    block_base: u64,
    terms: Vec<TermMapEntry>,
    last_term: u32,
}

impl BlockWriter {
    fn take(&mut self, pos: u64, bytes: &[u8]) -> Result<(), TierError> {
        debug_assert_eq!(pos, self.pending_base + self.pending.len() as u64);
        self.pending.extend_from_slice(bytes);
        let mut off = 0usize;
        while self.pending.len() - off >= HEADER_LEN {
            let h = read_header(&self.pending[off..]);
            let len = h.length as usize;
            let at = self.pending_base + off as u64;
            if len < HEADER_LEN {
                return Err(corrupt(
                    "tier chain",
                    format!("frame at {at} has length {len}"),
                ));
            }
            let stride = align_frame_len(len);
            if off + stride > self.pending.len() {
                break;
            }
            if h.frame_type != FRAME_TYPE_PADDING && h.leadership_term_id > self.last_term {
                self.terms.push(TermMapEntry {
                    term: h.leadership_term_id,
                    base: at,
                });
                self.last_term = h.leadership_term_id;
            }
            if !self.block.is_empty() && self.block.len() + stride > MAX_BLOCK_BYTES {
                self.flush()?;
            }
            if self.block.is_empty() {
                self.block_base = at;
            }
            self.block
                .extend_from_slice(&self.pending[off..off + stride]);
            off += stride;
        }
        self.pending.drain(..off);
        self.pending_base += off as u64;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), TierError> {
        if self.block.is_empty() {
            return Ok(());
        }
        let jerr = |e: uc_journal::JournalError| TierError::Journal(e.to_string());
        self.journal
            .append(self.seq, self.block_base, &self.block)
            .map_err(jerr)?
            .wait()
            .map_err(jerr)?;
        self.seq += 1;
        self.block.clear();
        Ok(())
    }
}

/// Step 4: the chain's bytes `[s, q)` into a fresh journal at `dir`, in
/// frame-aligned blocks of at most 1 MiB; returns the term map of the span.
/// Q must be a frame end — a stream that stops inside a frame is refused.
fn write_journal(
    t: &dyn BackupTarget,
    l: &TierListing,
    s: u64,
    q: u64,
    dir: &Path,
) -> Result<Vec<TermMapEntry>, TierError> {
    let journal = Journal::open(JournalConfig {
        preallocate_segments: false,
        ..JournalConfig::new(dir)
    })
    .map_err(|e| TierError::Journal(e.to_string()))?;
    let mut w = BlockWriter {
        journal,
        seq: 0,
        pending: Vec::new(),
        pending_base: s,
        block: Vec::new(),
        block_base: s,
        terms: Vec::new(),
        last_term: 0,
    };
    read_range(t, &l.chunks, s, q, &mut |pos, bytes| w.take(pos, bytes))?;
    if !w.pending.is_empty() || w.pending_base != q {
        return Err(unreachable(
            l,
            &format!("asked Q={q}, which is not a frame end"),
        ));
    }
    w.flush()?;
    Ok(w.terms)
}

fn write_restored(
    tmp: &Path,
    tier_id: u64,
    s: u64,
    q: u64,
    mode: Mode,
    now_ns: u64,
) -> io::Result<()> {
    let text = format!(
        "tier_id={tier_id:#018x}\nstart_set={s}\nq={q}\nmode={}\ncreated_ns={now_ns}\n",
        mode.label()
    );
    let mut f = File::create(tmp.join(RESTORED_FILE))?;
    f.write_all(text.as_bytes())?;
    f.sync_all()
}

fn sync_dir(p: &Path) -> io::Result<()> {
    File::open(p)?.sync_all()
}

/// Rename the assembled durable directories (then `RESTORED`) into
/// `out_dir`, replacing the empty ones `refuse_target` allowed, and drop the
/// assembly directory.
fn place(tmp: &Path, out_dir: &Path) -> Result<(), TierError> {
    for d in ["journal", "state"] {
        sync_dir(&tmp.join(d))?;
    }
    for d in DURABLE_DIRS {
        let dst = out_dir.join(d);
        if dst.is_dir() {
            fs::remove_dir(&dst)?; // empty, checked by `refuse_target`
        }
        fs::rename(tmp.join(d), &dst)?;
    }
    fs::rename(tmp.join(RESTORED_FILE), out_dir.join(RESTORED_FILE))?;
    fs::remove_dir(tmp)?;
    sync_dir(out_dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tier::layout::{RowLabel, SET_FILE, set_dir, set_label};
    use crate::tier::object::{Codec, MAGIC_CHUNK, write_object};
    use crate::tier::target::{FsTarget, Put, open_or_create_tier};
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use uc_consensus::config::ClusterConfig;
    use uc_log::archive::{Archive, ArchiveConfig};
    use uc_protocol::v2::catalog::SetKind;
    use uc_protocol::v2::frame::FRAME_TYPE_MESSAGE;
    use uc_service::SnapshotStateMachine;
    use uc_service::snapshots::{artifact_hash_of, write_snapshot_envelope};

    const APP: &str = "restore-app";
    const FRAME: u64 = 128;

    fn tmp() -> tempfile::TempDir {
        let root = std::env::var("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_node_tests")
            });
        fs::create_dir_all(&root).expect("scratch root");
        tempfile::Builder::new()
            .prefix("uc2-tier-")
            .tempdir_in(&root)
            .expect("tempdir")
    }

    fn membership() -> ClusterConfig {
        ClusterConfig {
            version: 4,
            voters: vec![
                (0, (0x7f00_0001, 7000)),
                (1, (0x7f00_0001, 7001)),
                (2, (0x7f00_0001, 7002)),
            ],
            learners: vec![(5, (0x7f00_0001, 7005))],
            tombstones: vec![9],
        }
    }

    /// `[from, to)` of 128-byte MESSAGE frames; the term of the frame at
    /// position `p` is `term_at(p)`.
    fn frames(from: u64, to: u64, term_at: impl Fn(u64) -> u32) -> Vec<u8> {
        let mut raw = Vec::new();
        let mut p = from;
        while p < to {
            let mut f = vec![0u8; FRAME as usize];
            f[0..4].copy_from_slice(&(FRAME as u32).to_le_bytes());
            f[4] = FRAME_TYPE_MESSAGE;
            f[8..12].copy_from_slice(&term_at(p).to_le_bytes());
            f[24..32].copy_from_slice(&(1_000 + p).to_le_bytes()); // time_ns
            f[HEADER_LEN..HEADER_LEN + 8].copy_from_slice(&p.to_le_bytes());
            raw.extend_from_slice(&f);
            p += FRAME;
        }
        raw
    }

    fn term_at(p: u64) -> u32 {
        match p {
            0..1536 => 1,
            1536..2048 => 2,
            _ => 3,
        }
    }

    fn put(t: &dyn BackupTarget, path: &str, magic: [u8; 8], from: u64, to: u64, raw: &[u8]) {
        let n = raw.len() as u64;
        let r = t
            .put_if_absent(path, &mut |f| {
                write_object(f, magic, Codec::Zstd, 3, from, to, &mut Cursor::new(raw), n)
            })
            .unwrap();
        assert!(matches!(r, Put::Written(_)));
    }

    fn row_bytes(p: u64) -> (Vec<u8>, u64) {
        let mut b = Vec::new();
        write_snapshot_envelope(&mut b, p, 0x0001_0000).unwrap();
        let payload = format!("row payload at {p}").into_bytes();
        b.extend_from_slice(&payload);
        (b, artifact_hash_of(&payload))
    }

    fn cluster_bytes(p: u64) -> Vec<u8> {
        let mut st = ClusterState::genesis(
            membership(),
            uc_protocol::v2::settings::Settings::genesis_default(),
        );
        st.applied = p;
        let fsm = crate::cluster_fsm::ClusterFsm::new(st, Vec::new());
        let (img, at) = fsm.freeze().unwrap();
        assert_eq!(at, p);
        img
    }

    /// One complete set at `p`: row 0 and the cluster artifact, then `SET`.
    fn put_set(t: &dyn BackupTarget, p: u64) {
        let (row, hash) = row_bytes(p);
        put(t, &row_object(p, 0), MAGIC_ARTIFACT, p, p, &row);
        let cl = cluster_bytes(p);
        put(t, &cluster_object(p), MAGIC_ARTIFACT, p, p, &cl);
        let label = SetLabel {
            position: p,
            kind: SetKind::Full,
            time_ns: 77,
            rows: vec![RowLabel {
                row: 0,
                hash,
                version: 0x0001_0000,
                size: row.len() as u64,
            }],
            cluster_hash: artifact_hash_of(&cl),
            cluster_size: cl.len() as u64,
        };
        let text = label.render();
        t.put_if_absent(&set_label(p), &mut |f| {
            f.write_all(text.as_bytes())?;
            Ok(text.len() as u64)
        })
        .unwrap();
    }

    struct Fx {
        _d: tempfile::TempDir,
        tier: PathBuf,
        root: PathBuf,
    }

    /// Chunks `[0, 2048)` (terms 1 then 2 at 1536) and `[2048, 4096)` (term
    /// 3); complete sets at 1024 and 3072.
    fn fixture() -> Fx {
        let d = tmp();
        let tier = d.path().join("tier");
        let t = FsTarget::new(&tier);
        open_or_create_tier(&t, APP, 1).unwrap();
        put(
            &t,
            &crate::tier::layout::chunk_object(0, 2048),
            MAGIC_CHUNK,
            0,
            2048,
            &frames(0, 2048, term_at),
        );
        put(
            &t,
            &crate::tier::layout::chunk_object(2048, 4096),
            MAGIC_CHUNK,
            2048,
            4096,
            &frames(2048, 4096, term_at),
        );
        put_set(&t, 1024);
        put_set(&t, 3072);
        Fx {
            root: d.path().to_path_buf(),
            _d: d,
            tier,
        }
    }

    fn out(fx: &Fx, name: &str) -> PathBuf {
        fx.root.join(name)
    }

    fn refusal(r: Result<Materialized, TierError>) -> String {
        match r {
            Err(TierError::Refused(m)) => m,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[test]
    fn materialize_builds_an_instance_dir_that_verify_accepts() {
        let fx = fixture();
        let t = FsTarget::new(&fx.tier);
        let info = read_tier(&t).unwrap().unwrap();

        // Default Q: the furthest reach; S the newest set below it.
        let d = out(&fx, "n0");
        let m = materialize(&t, None, &d, Mode::Recovery, 42).unwrap();
        assert_eq!((m.start_set, m.q, m.mode), (3072, 4096, Mode::Recovery));
        assert!(!d.join(RESTORE_TMP).exists(), "the assembly dir is gone");
        let rep = crate::backup::verify_artifact(&d).unwrap();
        assert_eq!((rep.journal_first_base, rep.journal_last_pos), (3072, 4096));
        assert_eq!(rep.snapshot_floor, 3072);
        assert!(SnapshotStore::open(&d, 0).unwrap().path_for(3072).is_file());
        assert!(cluster_agent::artifact_path(&cluster_agent::snapshot_dir_of(&d), 3072).is_file());
        let restored = fs::read_to_string(d.join(RESTORED_FILE)).unwrap();
        assert_eq!(
            restored,
            format!(
                "tier_id={:#018x}\nstart_set=3072\nq=4096\nmode=recovery\ncreated_ns=42\n",
                info.tier_id
            )
        );

        // An explicit Q inside the older set's chain: S = 1024, the term map
        // carries every term change in [S, Q), the config record is the
        // cluster artifact's membership at S, and no vote exists.
        let d = out(&fx, "n1");
        let m = materialize(&t, Some(2560), &d, Mode::Recovery, 1).unwrap();
        assert_eq!((m.start_set, m.q), (1024, 2560));
        let st = NodeState::open(&d.join("state")).unwrap();
        assert_eq!(st.snapshot_floor(), 1024);
        assert_eq!(st.output_progress(), 1024);
        assert_eq!(st.vote(), None);
        assert_eq!(
            st.term_map(),
            vec![
                TermMapEntry {
                    term: 1,
                    base: 1024
                },
                TermMapEntry {
                    term: 2,
                    base: 1536
                },
                TermMapEntry {
                    term: 3,
                    base: 2048
                },
            ]
        );
        let rec = st.config_record().expect("config record");
        let want = crate::node::cluster_to_stored(&membership());
        assert_eq!((rec.position, rec.prev_position), (1024, 1024));
        assert_eq!((&rec.config, &rec.prev), (&want, &want));
        let a = Archive::open(ArchiveConfig {
            preallocate_segments: false,
            ..ArchiveConfig::new(d.join("journal"))
        })
        .unwrap();
        assert_eq!((a.first_base(), a.recovered_position()), (1024, 2560));
        let mut replay = a.replay_from(1024).unwrap();
        let mut n = 0u64;
        while let Some(f) = replay.next().unwrap() {
            assert_eq!(f.position, 1024 + n * FRAME);
            assert_eq!(&f.payload[..8], &f.position.to_le_bytes());
            n += 1;
        }
        assert_eq!(n, (2560 - 1024) / FRAME);

        // A fork keeps Q as its output-progress marker.
        let d = out(&fx, "n2");
        materialize(&t, Some(2560), &d, Mode::Fork, 1).unwrap();
        let st = NodeState::open(&d.join("state")).unwrap();
        assert_eq!(st.output_progress(), 2560);
        assert!(
            fs::read_to_string(d.join(RESTORED_FILE))
                .unwrap()
                .contains("mode=fork\n")
        );
    }

    #[test]
    fn a_q_outside_the_chain_is_refused_with_the_restorable_range() {
        let fx = fixture();
        let t = FsTarget::new(&fx.tier);
        assert_eq!(
            restorable_ranges(&list_tier(&t).unwrap()),
            vec![(1024, 4096)]
        );
        // Beyond the chain, below every set, AT a set (an empty journal),
        // and inside a frame.
        for q in [5000, 512, 1024, 2048 + 52] {
            let d = out(&fx, &format!("q{q}"));
            let m = refusal(materialize(&t, Some(q), &d, Mode::Recovery, 1));
            assert!(m.contains("restore_point_unreachable"), "{q}: {m}");
            assert!(m.contains("restorable [1024, 4096]"), "{q}: {m}");
            assert!(!d.exists(), "{q}: a refusal leaves no directory");
        }
    }

    /// Deletes the set's OTHER artifact object right after the first one is
    /// opened — the set vanishing (retention, an operator) mid-build.
    struct Vanishing {
        inner: FsTarget,
        set: u64,
        armed: AtomicBool,
    }

    impl BackupTarget for Vanishing {
        fn put_if_absent(
            &self,
            path: &str,
            write: &mut dyn FnMut(&mut File) -> Result<u64, TierError>,
        ) -> Result<Put, TierError> {
            self.inner.put_if_absent(path, write)
        }
        fn list(&self, dir: &str) -> Result<Vec<String>, TierError> {
            self.inner.list(dir)
        }
        fn open(&self, path: &str) -> Result<Option<File>, TierError> {
            let f = self.inner.open(path)?;
            if path.starts_with(&set_dir(self.set))
                && !path.ends_with(SET_FILE)
                && self.armed.swap(false, Ordering::SeqCst)
            {
                let other = if path == row_object(self.set, 0) {
                    cluster_object(self.set)
                } else {
                    row_object(self.set, 0)
                };
                self.inner.delete(&other)?;
            }
            Ok(f)
        }
        fn delete(&self, path: &str) -> Result<(), TierError> {
            self.inner.delete(path)
        }
        fn delete_dir_if_empty(&self, dir: &str) -> Result<(), TierError> {
            self.inner.delete_dir_if_empty(dir)
        }
    }

    #[test]
    fn a_restore_whose_set_vanishes_mid_build_refuses_and_leaves_no_instance_dir() {
        let fx = fixture();
        let t = Vanishing {
            inner: FsTarget::new(&fx.tier),
            set: 3072,
            armed: AtomicBool::new(true),
        };
        let d = out(&fx, "vanish");
        let m = refusal(materialize(&t, None, &d, Mode::Recovery, 1));
        assert!(
            m.contains("restore set 3072 incomplete") && m.contains("missing"),
            "{m}"
        );
        assert!(!t.armed.load(Ordering::SeqCst), "the wrapper fired");
        assert!(!d.exists(), "no instance dir left behind");

        // An existing (empty) out_dir stays, holding nothing new.
        let t = Vanishing {
            inner: FsTarget::new(&fx.tier),
            set: 1024,
            armed: AtomicBool::new(true),
        };
        fs::create_dir_all(&d).unwrap();
        refusal(materialize(&t, Some(2048), &d, Mode::Recovery, 1));
        assert_eq!(fs::read_dir(&d).unwrap().count(), 0, "out_dir as it was");
    }

    #[test]
    fn a_tampered_artifact_is_refused_by_name() {
        let fx = fixture();
        let t = FsTarget::new(&fx.tier);
        // Replace set 3072's row artifact with one of the same length but
        // other bytes: the SET hash no longer matches.
        t.delete(&row_object(3072, 0)).unwrap();
        let (mut row, _) = row_bytes(3072);
        let last = row.len() - 1;
        row[last] ^= 0xff;
        put(&t, &row_object(3072, 0), MAGIC_ARTIFACT, 3072, 3072, &row);
        let d = out(&fx, "tampered");
        let m = refusal(materialize(&t, None, &d, Mode::Recovery, 1));
        assert!(m.contains("tier_set_mismatch"), "{m}");
        assert!(!d.exists());
    }

    #[test]
    fn a_running_nodes_dir_is_refused() {
        use fs2::FileExt;
        let fx = fixture();
        let t = FsTarget::new(&fx.tier);
        let d = out(&fx, "live");
        fs::create_dir_all(&d).unwrap();
        let lock = File::create(d.join("instance.lock")).unwrap();
        lock.try_lock_exclusive().unwrap();
        let m = refusal(materialize(&t, None, &d, Mode::Recovery, 1));
        assert!(m.contains("running node"), "{m}");
        assert_eq!(fs::read_dir(&d).unwrap().count(), 1, "untouched");
        FileExt::unlock(&lock).unwrap();
        drop(lock);

        // A stopped node's leftover lock is fine; a non-empty journal is not.
        fs::create_dir_all(d.join("journal")).unwrap();
        fs::write(d.join("journal/seg-0.log"), b"x").unwrap();
        let m = refusal(materialize(&t, None, &d, Mode::Recovery, 1));
        assert!(m.contains("TargetNotEmpty"), "{m}");
        assert!(!d.join(RESTORE_TMP).exists());
        fs::remove_dir_all(d.join("journal")).unwrap();
        fs::create_dir_all(d.join("state")).unwrap(); // empty: allowed
        materialize(&t, None, &d, Mode::Recovery, 1).unwrap();
        assert!(d.join("instance.lock").is_file(), "volatile leftovers stay");
    }

    #[test]
    fn new_node_refuses_a_reused_id() {
        let fx = fixture();
        let t = FsTarget::new(&fx.tier);
        // A voter, a learner and a tombstone of the membership at the set.
        for id in [0, 5, 9] {
            let d = out(&fx, &format!("reuse{id}"));
            let m = refusal(materialize(&t, None, &d, Mode::NewNode { node_id: id }, 1));
            assert!(m.contains("restore_id_reused"), "{id}: {m}");
            assert!(!d.exists());
        }
        let d = out(&fx, "fresh");
        let m = materialize(&t, None, &d, Mode::NewNode { node_id: 7 }, 1).unwrap();
        assert_eq!(m.mode, Mode::NewNode { node_id: 7 });
        let st = NodeState::open(&d.join("state")).unwrap();
        assert_eq!(st.output_progress(), m.start_set);
        assert!(
            fs::read_to_string(d.join(RESTORED_FILE))
                .unwrap()
                .contains("mode=new-node:7\n")
        );
    }
}
