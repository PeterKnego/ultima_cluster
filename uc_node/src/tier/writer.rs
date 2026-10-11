//! The `uc2-backup` writer (spec §5): copies the cluster's agreed snapshot
//! sets and its committed journal into a backup tier, and reports the chain
//! it extends to the leader as the tier's watermark.
//!
//! It runs on its own sleeping thread (`uc2-backup`, appended last to the
//! node's agents), never on a polling agent: every byte it reads comes from
//! files the node has already made durable (the snapshot artifacts and the
//! journal's archived blocks), and the only thing it hands back to the node
//! is a claim in a mutex slot the consensus agent polls every 10 ms (Task 7).
//!
//! **Journal bytes (ruling PF2).** A chunk is the log's own frames, read as
//! RAW archived blocks (`find_block` + `Journal::read`), never re-serialised
//! from a `Replay` (which skips padding frames). The frame walk only finds
//! the last frame end at or below the cut. Two spans are canonicalised to
//! zero: a PADDING frame's body and every frame's alignment slack
//! (`[length, align_frame_len(length))`). Neither is replicated content — a
//! padding frame travels header-only (`LogBuffer::read_run_validated`), so a
//! follower's padding body is whatever its own ring held from the previous
//! lap, and no reader ever interprets either span. Zeroing them makes every
//! writer's copy of a committed range byte-identical (spec §4.5.3), which
//! the raw node bytes are not.
//!
//! **Only committed bytes.** The cut's ceiling is `C = min(commit,
//! durable)`, read before the walk; committed bytes are never truncated, so
//! nothing the writer reads below `C` can be replaced by a failover.
use super::TierError;
use super::chain::{TierListing, list_fences, list_tier, reach_from, watermark_of};
use super::layout::{SetLabel, TierInfo, chunk_object, cluster_object, row_object, set_label};
use super::object::{Codec, MAGIC_ARTIFACT, MAGIC_CHUNK, ObjectReader, write_object};
use super::retention::{self, RetentionPolicy};
use super::target::{BackupTarget, Put, open_or_create_tier};
use crate::cluster_fsm::ClusterView;
use sha2::Digest;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uc_journal::Journal;
use uc_log::cnc::CncPage;
use uc_protocol::v2::backup::{BackupWatermark, WatermarkOp};
use uc_protocol::v2::frame::{
    ClusterKind, FRAME_TYPE_CLUSTER, FRAME_TYPE_PADDING, HEADER_LEN, align_frame_len,
    read_cluster_prefix, read_header,
};
use uc_service::snapshots::{SNAPSHOT_ENVELOPE_LEN, decode_snapshot_envelope};

/// `[backup] chunk_bytes` default: 64 MiB.
pub const DEFAULT_CHUNK_BYTES: u64 = 64 << 20;
/// The smallest `chunk_bytes` the loader accepts: 1 MiB.
pub const MIN_CHUNK_BYTES: u64 = 1 << 20;
/// `[backup] quiet_interval` default.
pub const DEFAULT_QUIET_INTERVAL: Duration = Duration::from_secs(60);
/// `[backup] compression_level` default (zstd).
pub const DEFAULT_COMPRESSION_LEVEL: i32 = 3;
/// How often a writer runs tier retention (spec §7) — at most once per
/// this interval, after its copy work.
pub const DEFAULT_RETENTION_INTERVAL: Duration = Duration::from_secs(60);
/// `.tmp/` entries older than this are crash residue (spec §4.4).
const TMP_MAX_AGE: Duration = Duration::from_secs(3600);
/// The cached tier listing is refreshed at least this often (another
/// writer may have extended the tier).
const LIST_REFRESH_NS: u64 = 10_000_000_000;
/// An unchanged claim is offered again this often: a leader change drops
/// the leader's pending reports (Task 7), and an idle tier would otherwise
/// strand its watermark until the next advance.
const REOFFER_NS: u64 = 10_000_000_000;
/// The unavailable-target retry ladder (spec §10): 1 s doubling to 60 s.
const RETRY_MIN_NS: u64 = 1_000_000_000;
const RETRY_MAX_NS: u64 = 60_000_000_000;

/// `[backup]` (spec §5.1). Node-local, never replicated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupConfig {
    pub target: PathBuf,
    pub chunk_bytes: u64,
    pub quiet_interval: Duration,
    pub compression: Codec,
    pub compression_level: i32,
    /// The retention pass's interval ([`DEFAULT_RETENTION_INTERVAL`]). NOT
    /// a `[backup]` key — the loader always takes the default; tests
    /// shorten it.
    pub retention_interval: Duration,
}

impl Default for BackupConfig {
    fn default() -> Self {
        BackupConfig {
            target: PathBuf::new(),
            chunk_bytes: DEFAULT_CHUNK_BYTES,
            quiet_interval: DEFAULT_QUIET_INTERVAL,
            compression: Codec::Zstd,
            compression_level: DEFAULT_COMPRESSION_LEVEL,
            retention_interval: DEFAULT_RETENTION_INTERVAL,
        }
    }
}

/// Why the writer counted an error (`stats.errors{reason}`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriterError {
    /// `TIER` names another `app_id`; the writer stopped.
    Foreign = 0,
    /// A target or journal I/O failure; retried on the ladder.
    Unavailable = 1,
    /// An `Exists` comparison or overlap disagreed; the writer stopped.
    OverlapMismatch = 2,
    /// A local set's files do not hash to the catalog's agreed hashes; the
    /// set is skipped (counted once per set).
    HashMismatch = 3,
    /// This node's journal could not supply a range (read error, purged
    /// under the cut); retried on the ladder. Not the target's fault.
    JournalUnavailable = 4,
    /// Ruling RF1: the target carries a `fence/<Q>` (a restore rolled its
    /// history back); the writer stopped.
    Fenced = 5,
    /// C5: the target's filesystem cannot hard-link; the writer stopped.
    NoHardlinks = 6,
}

impl WriterError {
    pub const ALL: [WriterError; 7] = [
        WriterError::Foreign,
        WriterError::Unavailable,
        WriterError::OverlapMismatch,
        WriterError::HashMismatch,
        WriterError::JournalUnavailable,
        WriterError::Fenced,
        WriterError::NoHardlinks,
    ];

    pub fn label(self) -> &'static str {
        match self {
            WriterError::Foreign => "foreign",
            WriterError::Unavailable => "unavailable",
            WriterError::OverlapMismatch => "overlap_mismatch",
            WriterError::HashMismatch => "hash_mismatch",
            WriterError::JournalUnavailable => "journal_unavailable",
            WriterError::Fenced => "fenced",
            WriterError::NoHardlinks => "no_hardlinks",
        }
    }
}

#[derive(Debug, Default)]
pub struct WriterStats {
    /// Stored bytes of set objects (artifacts + `SET`) this writer placed.
    pub bytes_set: AtomicU64,
    /// Stored bytes of journal chunks this writer placed.
    pub bytes_journal: AtomicU64,
    /// Chain gaps this writer saw (`backup_chain_gap`, spec §5.3).
    pub gaps: AtomicU64,
    /// Sets this writer's retention deleted from the tier (spec §7).
    pub sets_deleted: AtomicU64,
    /// Journal chunks this writer's retention deleted from the tier.
    pub chunks_deleted: AtomicU64,
    errors: [AtomicU64; WriterError::ALL.len()],
}

impl WriterStats {
    pub fn errors(&self, r: WriterError) -> u64 {
        self.errors[r as usize].load(Ordering::Relaxed)
    }

    /// Count one error. Public so the alert harness (`m10_alerts`) can drive
    /// the exporter without a real target failing.
    pub fn count(&self, r: WriterError) {
        self.errors[r as usize].fetch_add(1, Ordering::Relaxed);
    }
}

/// The writer's state the node reads (Task 9's holds, Task 13's metrics).
#[derive(Debug, Default)]
pub struct WriterShared {
    /// The end of the chain this writer last confirmed in the tier (0 =
    /// none): the purge hold.
    pub progress: AtomicU64,
    /// The set this node holds that the writer will copy next — eligible
    /// now, or deferred until the chain reaches it ([`keep_hold`]); 0 =
    /// none: the keep hold.
    pub set_hold: AtomicU64,
    /// Spec §5.4: the holds are released for lack of space. Set and cleared
    /// by the node's consensus agent (`Consensus::backup_purge_cap`), never
    /// by the writer.
    pub hold_released: AtomicBool,
    /// The writer refused its target for good (foreign, overlap mismatch,
    /// fenced, no hard links); exported as `uc2_backup_writer_stopped`
    /// (`Uc2BackupWriterStopped`). A stopped writer holds nothing (ruling
    /// R9a): its alert covers it.
    pub stopped: AtomicBool,
    /// Test seam (F2): while set, the writer's duty cycle does nothing — a
    /// writer lagging behind its peers (a slow target) without a target
    /// error. Never set outside tests.
    #[doc(hidden)]
    pub paused_for_test: AtomicBool,
    pub stats: WriterStats,
}

impl WriterShared {
    /// Spec §5.4: the highest position this node may purge below — the end
    /// of the chain the writer last confirmed in the tier (`0` before it
    /// has one: hold every purge) — or `None` when nothing is held (the
    /// hold released for space, or the writer stopped for good). Pure
    /// reads; the release decision is the consensus agent's.
    pub fn purge_cap(&self) -> Option<u64> {
        if self.stopped.load(Ordering::Acquire) || self.hold_released.load(Ordering::Acquire) {
            return None;
        }
        Some(self.progress.load(Ordering::Acquire))
    }

    /// The set the node's retention must keep for the writer, under the
    /// same release rule as [`Self::purge_cap`].
    pub fn keep(&self) -> Option<u64> {
        if self.stopped.load(Ordering::Acquire) || self.hold_released.load(Ordering::Acquire) {
            return None;
        }
        let p = self.set_hold.load(Ordering::Acquire);
        (p != 0).then_some(p)
    }
}

/// One local artifact of a set, opened once: hashing and copying read the
/// same descriptor, so a retention delete between the two cannot swap it.
struct LocalFile {
    tier_name: String,
    file: File,
    len: u64,
    /// `Some(P)`: a row artifact whose envelope must say `P`; its hash is
    /// over the payload after the envelope. `None`: the cluster artifact,
    /// hashed whole.
    envelope_at: Option<u64>,
    hash: u64,
    /// The label's size for it (`0` = unknown): the raw length the placed
    /// object must have.
    size: u64,
}

pub struct Writer {
    cfg: BackupConfig,
    app_id: String,
    node_id: u32,
    instance_dir: PathBuf,
    journal: Arc<Journal>,
    cnc: Arc<CncPage>,
    first_base: Arc<AtomicU64>,
    view: Arc<ClusterView>,
    claim: Arc<Mutex<Option<BackupWatermark>>>,
    shared: Arc<WriterShared>,
    target: Box<dyn BackupTarget>,

    tier: Option<TierInfo>,
    stopped: bool,
    unavailable: bool,
    /// A `backup_journal_unavailable` episode is open (logged once).
    journal_unavailable: bool,
    backoff_ns: u64,
    retry_at_ns: u64,
    listing: Option<TierListing>,
    /// `watermark_of(listing)`, cached with it.
    wm: Option<(u64, u64, u64)>,
    listed_at_ns: u64,
    /// Rescan the catalog when the view moved or the listing refreshed.
    scan_due: bool,
    /// `(view position, first_base)` at the last catalog scan.
    scanned_key: (u64, u64),
    last_cut_ns: Option<u64>,
    mismatched: BTreeSet<u64>,
    last_offer: Option<(BackupWatermark, u64)>,
    gap_logged: Option<u64>,
    /// `(E, C)` of the last quiet cut declined because `[E, C)` held only
    /// tier bookkeeping: not re-walked until either moves.
    quiet_declined: Option<(u64, u64)>,
    /// Ruling R9b: the agreed, locally held set `pick_set` is deferring
    /// (above the chain end, journal still reaching it). The next cut ends
    /// exactly at it, so it is copied before the catalog retires it.
    deferred: Option<u64>,
    /// When the last retention pass ran (spec §7); `None` = never.
    last_retention_ns: Option<u64>,
}

fn journal_err(e: impl std::fmt::Display) -> TierError {
    TierError::Journal(e.to_string())
}

fn wall_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// `artifact_hash_of`'s digest (the first 8 bytes of SHA-256, LE), streamed.
fn hash_stream(r: &mut dyn Read) -> io::Result<u64> {
    let mut h = sha2::Sha256::new();
    let mut buf = vec![0u8; 1 << 16];
    loop {
        match r.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => h.update(&buf[..n]),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    let d = h.finalize();
    Ok(u64::from_le_bytes(d[..8].try_into().expect("8 bytes")))
}

/// `(envelope position, payload hash)` of a row artifact stream, or `None`
/// when the envelope does not decode.
fn row_identity(r: &mut dyn Read) -> io::Result<Option<(u64, u64)>> {
    let mut env = [0u8; SNAPSHOT_ENVELOPE_LEN];
    if let Err(e) = r.read_exact(&mut env) {
        return if e.kind() == io::ErrorKind::UnexpectedEof {
            Ok(None)
        } else {
            Err(e)
        };
    }
    let Ok(e) = decode_snapshot_envelope(&env) else {
        return Ok(None);
    };
    Ok(Some((e.position, hash_stream(r)?)))
}

/// True for a frame the backup tier itself causes: a `CLUSTER`
/// `BackupWatermark` (every report commits one) or padding. A quiet-interval
/// cut over nothing else would back up its own report, whose watermark
/// commits another such frame — a self-sustaining chunk per interval on an
/// idle cluster — so a quiet cut waits for anything else (spec §5.3).
fn is_tier_bookkeeping(frame: &[u8]) -> bool {
    let h = read_header(frame);
    match h.frame_type {
        FRAME_TYPE_PADDING => true,
        FRAME_TYPE_CLUSTER => matches!(
            read_cluster_prefix(&frame[HEADER_LEN..h.length as usize]),
            Some((ClusterKind::BackupWatermark, _))
        ),
        _ => false,
    }
}

/// Which held set (positions newest first) the writer may copy now
/// (ruling R8d). A set extends the history the tier already has, so it is
/// copied only when it cannot start a second chain beside a live one:
/// (a) the tier has no watermark yet — the newest held set starts it;
/// (b) `P <= through` — the set lies inside the chain (newest such first);
/// (c) the gap case, `first_base > through` — this node's journal no longer
///     reaches the chain end, so the newest held set at or above
///     `first_base` starts the next chain (spec §5.3).
///
/// Otherwise every set is DEFERRED until the chain passes it: copying a set
/// above the chain end at once would make `watermark_of` report that set's
/// own (P, P, P), leave `[through, P)` uncopied, and jump the cluster's
/// forward-only `oldest_set` on a healthy cluster.
fn pick_set(held_newest_first: &[u64], through: Option<u64>, first_base: u64) -> Option<u64> {
    let Some(t) = through else {
        return held_newest_first.first().copied();
    };
    if let Some(&p) = held_newest_first.iter().find(|&&p| p <= t) {
        return Some(p);
    }
    if first_base > t {
        return held_newest_first.iter().copied().find(|&p| p >= first_base);
    }
    None
}

/// Ruling RF2: a gap in the tier BELOW its `through` that this node's
/// journal can fill — the lowest chain end `e` with `first_base <= e <
/// through` — and the set that starts the next chain above it. Another
/// writer whose journal no longer reached the chain end (a learner that
/// joined below the floor, a hold released for space) started a newer
/// chain at its own set; this node still holds `[e, next)`, so it fills it
/// first, cutting toward `next` (R9b's cut-to-set) until the chains join.
/// While such a gap exists, `e` — not `through` — is this writer's
/// progress, so its node never purges the bytes the gap needs.
fn fillable_gap(l: &TierListing, through: u64, first_base: u64) -> Option<(u64, u64)> {
    let e = l
        .sets
        .iter()
        .map(|s| reach_from(&l.chunks, s.position))
        .filter(|&r| first_base <= r && r < through)
        .min()?;
    let next = l.sets.iter().map(|s| s.position).filter(|&p| p > e).min()?;
    Some((e, next))
}

/// The keep hold (Task 9): the held set the writer will copy — the one
/// [`pick_set`] picks now, or, while every set is deferred (the journal
/// still reaches the chain end), the lowest held set above `through`, which
/// the chain passes first. `None` when no held set can ever be picked (the
/// gap case with every held set below `first_base`).
fn keep_hold(held_newest_first: &[u64], through: Option<u64>, first_base: u64) -> Option<u64> {
    if let Some(p) = pick_set(held_newest_first, through, first_base) {
        return Some(p);
    }
    let t = through?;
    if first_base > t {
        return None;
    }
    held_newest_first.iter().copied().filter(|&p| p > t).min()
}

/// Ruling R9b: the cut's limit from the chain end `e` under the ceiling
/// `c`, and whether a deferred set forces it now. A deferred set `P` in
/// `(e, c]` makes the cut end exactly at `P` (a frame end: the `SNAPSHOT`
/// frame ends there) — or, when `P` lies more than one `chunk_bytes` away,
/// at the last frame end within one chunk, one step at a time. Otherwise
/// (none deferred, or `P` above `C`) the ordinary rules apply.
fn cut_limit(e: u64, c: u64, chunk_bytes: u64, deferred: Option<u64>) -> (u64, bool) {
    let chunk_end = e.saturating_add(chunk_bytes);
    match deferred {
        Some(p) if p > e && p <= c => (p.min(chunk_end), true),
        _ => (c.min(chunk_end), false),
    }
}

/// Rulings R8b + R11a: whether a cut that is neither full nor forced by a
/// deferred set is declined. A range holding only tier bookkeeping
/// (padding, `BackupWatermark`) is declined — otherwise an idle writer
/// would back up its own reports forever — EXCEPT once when the chain ends
/// exactly at its newest complete set (`through == newest_set`): a set
/// with no byte of chain after it is not restorable (a restore point lies
/// strictly above its set), and on an idle cluster nothing but bookkeeping
/// ever follows it. After that one cut `through > newest_set`, so the skip
/// holds again.
fn declines_quiet_cut(
    full: bool,
    to_set: bool,
    substantive: bool,
    through: u64,
    newest_set: u64,
) -> bool {
    !full && !to_set && !substantive && through != newest_set
}

/// Pass 1 of a cut: walk `[e, …)` to the last frame end within `limit` (at
/// least one frame when one fits under `c`). `(to, crc of the canonical
/// bytes, holds anything but tier bookkeeping)`; `None` when the journal
/// no longer holds `e`.
fn walk_cut(
    journal: &Journal,
    e: u64,
    c: u64,
    limit: u64,
) -> Result<Option<(u64, u32, bool)>, TierError> {
    let Some(mut cur) = FrameCursor::open(journal, e)? else {
        return Ok(None);
    };
    let mut crc = crc32fast::Hasher::new();
    let mut scratch = Vec::new();
    let mut substantive = false;
    loop {
        let lim = if cur.pos == e { c } else { limit };
        scratch.clear();
        match cur.next_frame(lim, &mut scratch)? {
            Walk::Frame => {
                crc.update(&scratch);
                substantive |= !is_tier_bookkeeping(&scratch);
            }
            Walk::Limit => break,
            Walk::Gone => return Ok(None),
        }
    }
    Ok(Some((cur.pos, crc.finalize(), substantive)))
}

/// What one step of a [`FrameCursor`] found.
enum Walk {
    /// A whole frame within the limit was appended; the cursor moved.
    Frame,
    /// The next frame would pass the limit; the cursor did not move.
    Limit,
    /// The journal no longer holds the next block (purged under us).
    Gone,
}

/// A raw walk over archived journal blocks from a frame boundary (ruling
/// PF2), emitting each frame in canonical form (module docs).
struct FrameCursor<'a> {
    journal: &'a Journal,
    seq: u64,
    base: u64,
    block: Vec<u8>,
    pos: u64,
}

impl<'a> FrameCursor<'a> {
    /// `None` when `pos` is below the journal's first block (purged).
    fn open(journal: &'a Journal, pos: u64) -> Result<Option<Self>, TierError> {
        let Some((seq, base)) = uc_log::archive::find_block(journal, pos).map_err(journal_err)?
        else {
            return Ok(None);
        };
        let Some((meta, block)) = journal.read(seq).map_err(journal_err)? else {
            return Ok(None);
        };
        if meta != base || pos >= meta + block.len() as u64 {
            return Err(journal_err(format!(
                "block {seq} [{meta}, +{}) does not hold position {pos}",
                block.len()
            )));
        }
        Ok(Some(FrameCursor {
            journal,
            seq,
            base,
            block,
            pos,
        }))
    }

    fn next_frame(&mut self, limit: u64, out: &mut Vec<u8>) -> Result<Walk, TierError> {
        // At the limit no frame can fit: never load a block past it (at
        // `durable` the next block does not exist yet).
        if self.pos + HEADER_LEN as u64 > limit {
            return Ok(Walk::Limit);
        }
        if self.pos >= self.base + self.block.len() as u64 {
            let next = self.seq + 1;
            let Some((meta, block)) = self.journal.read(next).map_err(journal_err)? else {
                return Ok(Walk::Gone);
            };
            if meta != self.base + self.block.len() as u64 {
                return Err(journal_err(format!(
                    "block {next} starts at {meta}, not {}",
                    self.base + self.block.len() as u64
                )));
            }
            self.seq = next;
            self.base = meta;
            self.block = block;
        }
        let off = (self.pos - self.base) as usize;
        let bad = |what: &str| journal_err(format!("{what} at position {}", self.pos));
        if off + HEADER_LEN > self.block.len() {
            return Err(bad("a sub-header remainder"));
        }
        let h = read_header(&self.block[off..]);
        let len = h.length as usize;
        if len < HEADER_LEN {
            return Err(bad("a frame shorter than its header"));
        }
        let aligned = align_frame_len(len);
        if off + aligned > self.block.len() {
            // Ruling PF2's premise: an archived block holds every frame's
            // whole aligned span, padding included.
            return Err(bad("a frame crossing its archived block"));
        }
        if self.pos + aligned as u64 > limit {
            return Ok(Walk::Limit);
        }
        let keep = if h.frame_type == FRAME_TYPE_PADDING {
            HEADER_LEN
        } else {
            len
        };
        out.extend_from_slice(&self.block[off..off + keep]);
        out.resize(out.len() + (aligned - keep), 0);
        self.pos += aligned as u64;
        Ok(Walk::Frame)
    }
}

/// The canonical bytes of `[from, to)` as a `Read`, CRC'd as they stream.
struct ChunkReader<'a> {
    cur: FrameCursor<'a>,
    to: u64,
    buf: Vec<u8>,
    at: usize,
    crc: crc32fast::Hasher,
    /// Set when the journal (not the destination) failed the read, so the
    /// caller can name the journal rather than the target.
    journal_failed: Option<String>,
}

impl ChunkReader<'_> {
    fn new(cur: FrameCursor<'_>, to: u64) -> ChunkReader<'_> {
        ChunkReader {
            cur,
            to,
            buf: Vec::new(),
            at: 0,
            crc: crc32fast::Hasher::new(),
            journal_failed: None,
        }
    }

    fn journal_error(&mut self, msg: String) -> io::Error {
        self.journal_failed = Some(msg.clone());
        io::Error::other(msg)
    }
}

impl Read for ChunkReader<'_> {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        if self.at == self.buf.len() {
            if self.cur.pos >= self.to {
                return Ok(0);
            }
            self.buf.clear();
            self.at = 0;
            match self.cur.next_frame(self.to, &mut self.buf) {
                Ok(Walk::Frame) => self.crc.update(&self.buf),
                Ok(Walk::Limit) => {
                    return Err(self.journal_error(
                        "a frame passes the chunk's end on the second walk".into(),
                    ));
                }
                Ok(Walk::Gone) => return Err(self.journal_error("purged under the cut".into())),
                Err(e) => return Err(self.journal_error(e.to_string())),
            }
        }
        let n = dst.len().min(self.buf.len() - self.at);
        dst[..n].copy_from_slice(&self.buf[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// Put one artifact (write-once); an existing one is compared.
fn put_artifact(
    t: &dyn BackupTarget,
    codec: Codec,
    level: i32,
    f: &mut LocalFile,
    stats: &WriterStats,
) -> Result<(), TierError> {
    f.file.seek(SeekFrom::Start(0))?;
    let (src, len) = (&mut f.file, f.len);
    let put = t.put_if_absent(&f.tier_name, &mut |dst| {
        src.seek(SeekFrom::Start(0))?;
        write_object(dst, MAGIC_ARTIFACT, codec, level, 0, 0, src, len)
    })?;
    match put {
        Put::Written(n) => {
            stats.bytes_set.fetch_add(n, Ordering::Relaxed);
            Ok(())
        }
        Put::Exists => compare_artifact(t, f),
    }
}

/// Spec §5.2: an existing artifact is compared by envelope position and
/// payload hash (rows) or hash (cluster) — never byte equality, since
/// two members' agreeing copies may differ in their envelopes' version.
fn compare_artifact(t: &dyn BackupTarget, f: &LocalFile) -> Result<(), TierError> {
    let mismatch = || TierError::OverlapMismatch(f.tier_name.clone());
    let Some(obj) = t.open(&f.tier_name)? else {
        return Err(mismatch());
    };
    let mut r = ObjectReader::open(obj).map_err(|_| mismatch())?;
    let same = match f.envelope_at {
        Some(p) => row_identity(&mut r).map_err(|_| mismatch())? == Some((p, f.hash)),
        None => hash_stream(&mut r).map_err(|_| mismatch())? == f.hash,
    };
    if same { Ok(()) } else { Err(mismatch()) }
}

/// Ruling R10b: the objects of `files` NOT present in the tier right now.
/// A present object whose raw length is not the one this writer would
/// place (the local file's, and the label's when known) is refused as an
/// overlap mismatch — write-once, it cannot be repaired.
fn absent_objects(t: &dyn BackupTarget, files: &[LocalFile]) -> Result<Vec<usize>, TierError> {
    let mut absent = Vec::new();
    for (i, f) in files.iter().enumerate() {
        let Some(obj) = t.open(&f.tier_name)? else {
            absent.push(i);
            continue;
        };
        let mismatch = || TierError::OverlapMismatch(f.tier_name.clone());
        let raw = ObjectReader::open(obj)
            .map_err(|_| mismatch())?
            .header()
            .raw_len;
        if raw != f.len || (f.size != 0 && raw != f.size) {
            return Err(mismatch());
        }
    }
    Ok(absent)
}

/// How many times [`place_set`] re-places objects that vanished before it
/// gives the pass up to the retry ladder.
const PLACE_ROUNDS: usize = 3;

/// Copy one set: every artifact, then — ruling R10b, spec §4.5.2 ("every
/// file `SET` names is present while `SET` is") — re-check that each one is
/// still in the tier with its raw length, re-placing any that vanished
/// (another writer's retention may have cleared the directory as aged
/// residue while an upload sat in `.tmp`), and only then put `SET`.
fn place_set(
    t: &dyn BackupTarget,
    codec: Codec,
    level: i32,
    label: &SetLabel,
    files: &mut [LocalFile],
    stats: &WriterStats,
) -> Result<(), TierError> {
    for f in files.iter_mut() {
        put_artifact(t, codec, level, f, stats)?;
    }
    let mut rounds = 0;
    loop {
        let absent = absent_objects(t, files)?;
        if absent.is_empty() {
            break;
        }
        rounds += 1;
        if rounds > PLACE_ROUNDS {
            return Err(TierError::Refused(format!(
                "set {}: {} object(s) keep vanishing before SET",
                label.position,
                absent.len()
            )));
        }
        for i in absent {
            put_artifact(t, codec, level, &mut files[i], stats)?;
        }
    }
    let text = label.render();
    let put = t.put_if_absent(&set_label(label.position), &mut |dst| {
        io::Write::write_all(dst, text.as_bytes())?;
        Ok(text.len() as u64)
    })?;
    match put {
        Put::Written(n) => {
            stats.bytes_set.fetch_add(n, Ordering::Relaxed);
        }
        Put::Exists => {
            let mut s = String::new();
            if let Some(mut f) = t.open(&set_label(label.position))? {
                f.read_to_string(&mut s)?;
            }
            if SetLabel::parse(&s).ok().as_ref() != Some(label) {
                return Err(TierError::OverlapMismatch(set_label(label.position)));
            }
        }
    }
    Ok(())
}

impl Writer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: BackupConfig,
        app_id: String,
        node_id: u32,
        instance_dir: PathBuf,
        journal: Arc<Journal>,
        cnc: Arc<CncPage>,
        first_base: Arc<AtomicU64>,
        view: Arc<ClusterView>,
        claim: Arc<Mutex<Option<BackupWatermark>>>,
        shared: Arc<WriterShared>,
        target: Box<dyn BackupTarget>,
    ) -> Writer {
        Writer {
            cfg,
            app_id,
            node_id,
            instance_dir,
            journal,
            cnc,
            first_base,
            view,
            claim,
            shared,
            target,
            tier: None,
            stopped: false,
            unavailable: false,
            journal_unavailable: false,
            backoff_ns: 0,
            retry_at_ns: 0,
            listing: None,
            wm: None,
            listed_at_ns: 0,
            scan_due: true,
            scanned_key: (u64::MAX, u64::MAX),
            last_cut_ns: None,
            mismatched: BTreeSet::new(),
            last_offer: None,
            gap_logged: None,
            quiet_declined: None,
            deferred: None,
            last_retention_ns: None,
        }
    }

    /// One bounded duty cycle — at most one set or one chunk; `true` =
    /// progress (the runner then skips its sleep).
    pub fn step(&mut self, now_ns: u64) -> bool {
        if self.stopped
            || now_ns < self.retry_at_ns
            || self.shared.paused_for_test.load(Ordering::Relaxed)
        {
            return false;
        }
        match self.work(now_ns) {
            Ok(progress) => {
                self.journal_unavailable = false;
                if self.unavailable {
                    self.unavailable = false;
                    crate::obs_event!(
                        Info,
                        "backup_target_available",
                        node = self.node_id as u64,
                        target = self.cfg.target.display().to_string().as_str()
                    );
                }
                self.backoff_ns = 0;
                progress
            }
            Err(e) => {
                self.fail(e, now_ns);
                false
            }
        }
    }

    fn stop(&mut self, why: &str) {
        self.stopped = true;
        self.shared.stopped.store(true, Ordering::Release);
        crate::obs_event!(
            Warn,
            "backup_writer_stopped",
            node = self.node_id as u64,
            target = self.cfg.target.display().to_string().as_str(),
            reason = why
        );
    }

    fn fail(&mut self, e: TierError, now_ns: u64) {
        let target = self.cfg.target.display().to_string();
        match e {
            TierError::Foreign(_, theirs, ours) => {
                self.shared.stats.count(WriterError::Foreign);
                crate::obs_event!(
                    Warn,
                    "backup_target_foreign",
                    node = self.node_id as u64,
                    target = target.as_str(),
                    app_id = theirs.as_str(),
                    expected = ours.as_str()
                );
                self.stop("backup_target_foreign");
            }
            TierError::OverlapMismatch(what) => {
                self.shared.stats.count(WriterError::OverlapMismatch);
                crate::obs_event!(
                    Warn,
                    "tier_overlap_mismatch",
                    node = self.node_id as u64,
                    target = target.as_str(),
                    object = what.as_str()
                );
                self.stop("tier_overlap_mismatch");
            }
            TierError::Fenced(_, q) => {
                self.shared.stats.count(WriterError::Fenced);
                crate::obs_event!(
                    Warn,
                    "backup_target_fenced",
                    node = self.node_id as u64,
                    target = target.as_str(),
                    q = q
                );
                self.stop("backup_target_fenced");
            }
            TierError::NoHardlinks(_, err) => {
                self.shared.stats.count(WriterError::NoHardlinks);
                crate::obs_event!(
                    Warn,
                    "target_no_hardlinks",
                    node = self.node_id as u64,
                    target = target.as_str(),
                    err = err.as_str()
                );
                self.stop("target_no_hardlinks");
            }
            TierError::Journal(msg) => {
                self.shared.stats.count(WriterError::JournalUnavailable);
                if !self.journal_unavailable {
                    self.journal_unavailable = true;
                    crate::obs_event!(
                        Warn,
                        "backup_journal_unavailable",
                        node = self.node_id as u64,
                        err = msg.as_str()
                    );
                }
                self.back_off(now_ns);
            }
            other => {
                self.shared.stats.count(WriterError::Unavailable);
                if !self.unavailable {
                    self.unavailable = true;
                    crate::obs_event!(
                        Warn,
                        "backup_target_unavailable",
                        node = self.node_id as u64,
                        target = target.as_str(),
                        err = other.to_string().as_str()
                    );
                }
                self.back_off(now_ns);
            }
        }
    }

    /// The retry ladder (spec §10): 1 s doubling to 60 s; the listing is
    /// re-read on the retry.
    fn back_off(&mut self, now_ns: u64) {
        self.backoff_ns = if self.backoff_ns == 0 {
            RETRY_MIN_NS
        } else {
            (self.backoff_ns * 2).min(RETRY_MAX_NS)
        };
        self.retry_at_ns = now_ns.saturating_add(self.backoff_ns);
        self.listing = None;
    }

    fn work(&mut self, now_ns: u64) -> Result<bool, TierError> {
        if self.tier.is_none() {
            self.target.clean_tmp(TMP_MAX_AGE)?;
            let info = open_or_create_tier(&*self.target, &self.app_id, wall_ns())?;
            // Ruling RF1: a fenced target is refused before anything is
            // copied (the refresh below re-checks on every listing).
            if let Some(&q) = list_fences(&*self.target)?.first() {
                return Err(self.fenced(q));
            }
            crate::obs_event!(
                Info,
                "backup_tier_opened",
                node = self.node_id as u64,
                target = self.cfg.target.display().to_string().as_str(),
                tier_id = info.tier_id
            );
            self.tier = Some(info);
            self.last_cut_ns.get_or_insert(now_ns);
        }
        if self.listing.is_none() || now_ns.saturating_sub(self.listed_at_ns) >= LIST_REFRESH_NS {
            self.refresh(now_ns)?;
        }
        let progress = if self.copy_one_set()? || self.cut_one_chunk(now_ns)? {
            self.refresh(now_ns)?;
            true
        } else {
            false
        };
        // Spec §7: retention after the copy work, at most once per
        // `retention_interval`. A deletion refreshes the listing, so the
        // offer below names the new `oldest_set` at once.
        let interval = self.cfg.retention_interval.as_nanos() as u64;
        if self
            .last_retention_ns
            .is_none_or(|at| now_ns.saturating_sub(at) >= interval)
        {
            self.last_retention_ns = Some(now_ns);
            if self.retain()? {
                self.refresh(now_ns)?;
            }
        }
        self.maybe_offer(now_ns);
        Ok(progress)
    }

    /// One retention pass over a fresh listing (spec §7.2): the committed
    /// policy and pin protection decide, [`retention::apply`] deletes.
    /// `true` when anything was deleted. Never touches the chain from the
    /// oldest kept set to `through` (module docs of [`retention`]), so the
    /// `progress` the next refresh stores cannot move back.
    fn retain(&mut self) -> Result<bool, TierError> {
        // `consumed` FIRST: the policy and the view read after it are at
        // least as new as the walk the R10a gate checks.
        let consumed = self.view.consumed.load(Ordering::Acquire);
        let policy = RetentionPolicy::from_view(&self.view);
        let inner = self.view.snapshot_inner();
        let mut done = retention::Deleted::default();
        // Deletions already made count even when a later one fails.
        let res = retention::run_pass(
            &*self.target,
            &policy,
            &inner,
            consumed,
            TMP_MAX_AGE,
            &mut done,
        );
        let stats = &self.shared.stats;
        stats.sets_deleted.fetch_add(done.sets, Ordering::Relaxed);
        stats
            .chunks_deleted
            .fetch_add(done.chunks, Ordering::Relaxed);
        if done.any() {
            crate::obs_event!(
                Info,
                "backup_retention_deleted",
                node = self.node_id as u64,
                sets = done.sets,
                chunks = done.chunks,
                incomplete = done.incomplete,
                oldest_deleted = done.oldest_set,
                failed = res.is_err()
            );
        }
        res?;
        Ok(done.any())
    }

    fn fenced(&self, q: u64) -> TierError {
        TierError::Fenced(self.cfg.target.display().to_string(), q)
    }

    fn refresh(&mut self, now_ns: u64) -> Result<(), TierError> {
        let l = list_tier(&*self.target)?;
        // Ruling RF1: a restore fenced the tier while this writer ran (or
        // between its open and its first listing): never extend it.
        if let Some(&q) = l.fences.first() {
            return Err(self.fenced(q));
        }
        let wm = watermark_of(&l);
        // A new chain that does not connect to the old one: the tier's
        // history now has a hole between the old reach and the new start.
        if let (Some((_, _, old_through)), Some((oldest, _, _))) = (self.wm, wm)
            && oldest > old_through
        {
            self.note_gap(old_through, oldest);
        }
        self.wm = wm;
        self.listing = Some(l);
        // Ruling RF2: the purge cap is the fillable chain end while a gap
        // below `through` exists, else `through`.
        let first_base = self.first_base.load(Ordering::Acquire);
        if let Some((end, _)) = self.chain_end(first_base) {
            self.shared.progress.store(end, Ordering::Release);
        }
        self.listed_at_ns = now_ns;
        self.scan_due = true;
        Ok(())
    }

    /// `(the chain end this writer extends, the set a gap fill cuts
    /// toward)`: the fillable chain end and the next chain's set while a
    /// gap below `through` exists ([`fillable_gap`], ruling RF2), else
    /// `(through, None)`. `None` without a watermark.
    fn chain_end(&self, first_base: u64) -> Option<(u64, Option<u64>)> {
        let (_, _, through) = self.wm?;
        let gap = self
            .listing
            .as_ref()
            .and_then(|l| fillable_gap(l, through, first_base));
        Some(match gap {
            Some((e, next)) => (e, Some(next)),
            None => (through, None),
        })
    }

    /// `backup_chain_gap{from, to}`, once per chain end `from` — seen either
    /// as this node's journal no longer reaching the chain end, or as the
    /// tier's watermark moving to a chain that starts past it.
    fn note_gap(&mut self, from: u64, to: u64) {
        if self.gap_logged == Some(from) {
            return;
        }
        self.gap_logged = Some(from);
        self.shared.stats.gaps.fetch_add(1, Ordering::Relaxed);
        crate::obs_event!(
            Warn,
            "backup_chain_gap",
            node = self.node_id as u64,
            from = from,
            to = to
        );
    }

    fn maybe_offer(&mut self, now_ns: u64) {
        let (Some(info), Some((oldest, newest, through))) = (&self.tier, self.wm) else {
            return;
        };
        let w = BackupWatermark {
            op: WatermarkOp::Report,
            tier_id: info.tier_id,
            oldest_set: oldest,
            newest_set: newest,
            through,
            reporter: self.node_id as u64,
        };
        if let Some((last, at)) = self.last_offer
            && last == w
            && now_ns.saturating_sub(at) < REOFFER_NS
        {
            return;
        }
        *self.claim.lock().unwrap_or_else(|e| e.into_inner()) = Some(w);
        self.last_offer = Some((w, now_ns));
    }

    // ------------------------------------------------------------- sets

    fn copy_one_set(&mut self) -> Result<bool, TierError> {
        let view_pos = self.view.position.load(Ordering::Acquire);
        let first_base = self.first_base.load(Ordering::Acquire);
        let key = (view_pos, first_base);
        if !self.scan_due && key == self.scanned_key {
            return Ok(false);
        }
        self.scan_due = false;
        self.scanned_key = key;
        // A set whose SET label is corrupt (F4) counts as present: copying
        // it again would only meet the unparsable label (`Exists`) and stop
        // the writer as an overlap mismatch. `tier verify` names it.
        let have: BTreeSet<u64> = self
            .listing
            .as_ref()
            .map(|l| {
                l.sets
                    .iter()
                    .map(|s| s.position)
                    .chain(l.corrupt_sets.iter().copied())
                    .collect()
            })
            .unwrap_or_default();
        // Spec §7: a catalog set retention would delete again the moment it
        // landed (the policy over the tier plus it) is never (re)copied —
        // otherwise every set retention deletes comes straight back.
        let policy = RetentionPolicy::from_view(&self.view);
        let inner = self.view.snapshot_inner();
        let tier_sets: &[SetLabel] = self.listing.as_ref().map_or(&[], |l| &l.sets);
        let mut todo: Vec<SetLabel> = inner
            .catalog
            .iter()
            .filter(|e| {
                e.is_agreed()
                    && !have.contains(&e.position)
                    && !self.mismatched.contains(&e.position)
            })
            .map(SetLabel::from_catalog)
            .filter(|l| !retention::recopy_blocked(tier_sets, l, &policy, &inner))
            .collect();
        todo.sort_by_key(|l| std::cmp::Reverse(l.position));
        // The sets this node holds whole on disk, newest first.
        let mut held: Vec<(SetLabel, Vec<LocalFile>)> = Vec::new();
        for label in todo {
            if let Some(files) = self.open_local_set(&label)? {
                held.push((label, files));
            }
        }
        // Ruling RF2: while a fillable gap lies below `through`, sets are
        // picked and deferred against its lower end — a held set above it
        // waits until the fill reaches it.
        let through = self.chain_end(first_base).map(|(e, _)| e);
        loop {
            let positions: Vec<u64> = held.iter().map(|(l, _)| l.position).collect();
            // The keep hold (Task 9): the set about to be copied, or the
            // deferred one the chain reaches next — re-derived after a
            // mismatched set drops out.
            let hold = keep_hold(&positions, through, first_base).unwrap_or(0);
            self.shared.set_hold.store(hold, Ordering::Release);
            // Ruling R9b: a held set above the chain end, waiting for it.
            self.deferred = match through {
                Some(t) if hold > t && first_base <= t => Some(hold),
                _ => None,
            };
            let Some(p) = pick_set(&positions, through, first_base) else {
                return Ok(false); // nothing eligible: deferred, or none held
            };
            let i = positions
                .iter()
                .position(|&x| x == p)
                .expect("picked from held");
            let (label, mut files) = held.remove(i);
            if let Some(why) = Self::check_local_hashes(&mut files)? {
                self.mismatched.insert(label.position);
                self.shared.stats.count(WriterError::HashMismatch);
                crate::obs_event!(
                    Warn,
                    "backup_set_hash_mismatch",
                    node = self.node_id as u64,
                    position = label.position,
                    object = why.as_str()
                );
                continue;
            }
            self.copy_set(&label, &mut files)?;
            // The listing refresh after a copy sets `scan_due`: the next pass
            // rescans and moves the hold on.
            return Ok(true);
        }
    }

    /// Every artifact of `label` on this node's disk, opened; `None` when
    /// any is missing.
    fn open_local_set(&self, label: &SetLabel) -> Result<Option<Vec<LocalFile>>, TierError> {
        let p = label.position;
        let mut out = Vec::with_capacity(label.rows.len() + 1);
        let mut want: Vec<(PathBuf, String, Option<u64>, u64, u64)> = label
            .rows
            .iter()
            .map(|r| {
                (
                    local_row_artifact(&self.instance_dir, r.row, p),
                    row_object(p, r.row),
                    Some(p),
                    r.hash,
                    r.size,
                )
            })
            .collect();
        want.push((
            crate::cluster_agent::artifact_path(
                &crate::cluster_agent::snapshot_dir_of(&self.instance_dir),
                p,
            ),
            cluster_object(p),
            None,
            label.cluster_hash,
            label.cluster_size,
        ));
        for (path, tier_name, envelope_at, hash, size) in want {
            let file = match File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            let len = file.metadata()?.len();
            out.push(LocalFile {
                tier_name,
                file,
                len,
                envelope_at,
                hash,
                size,
            });
        }
        Ok(Some(out))
    }

    /// `Some(object)` naming the first local file whose identity disagrees
    /// with the catalog's agreed hash (or envelope position).
    fn check_local_hashes(files: &mut [LocalFile]) -> Result<Option<String>, TierError> {
        for f in files.iter_mut() {
            f.file.seek(SeekFrom::Start(0))?;
            let ok = match f.envelope_at {
                Some(p) => row_identity(&mut f.file)? == Some((p, f.hash)),
                None => hash_stream(&mut f.file)? == f.hash,
            };
            if !ok {
                return Ok(Some(f.tier_name.clone()));
            }
        }
        Ok(None)
    }

    fn copy_set(&mut self, label: &SetLabel, files: &mut [LocalFile]) -> Result<(), TierError> {
        let (codec, level) = (self.cfg.compression, self.cfg.compression_level);
        place_set(
            &*self.target,
            codec,
            level,
            label,
            files,
            &self.shared.stats,
        )?;
        crate::obs_event!(
            Info,
            "backup_set_copied",
            node = self.node_id as u64,
            position = label.position,
            rows = label.rows.len() as u64
        );
        Ok(())
    }

    // ---------------------------------------------------------- journal

    fn cut_one_chunk(&mut self, now_ns: u64) -> Result<bool, TierError> {
        let first_base = self.first_base.load(Ordering::Acquire);
        let (Some((_, newest_set, _)), Some((e, fill_to))) = (self.wm, self.chain_end(first_base))
        else {
            return Ok(false); // no set in the tier yet: nothing to extend
        };
        let k = self.cnc.counters();
        let c = k.commit.load_acquire().min(k.durable.load_acquire());
        if c <= e {
            return Ok(false);
        }
        if first_base > e {
            self.note_gap(e, first_base);
            return Ok(false); // the set phase starts the next chain
        }
        // Ruling RF2: a gap fill ends at the next chain's set (or at a held
        // set below it, whichever comes first) — R9b's cut-to-set.
        let deferred = match (self.deferred.filter(|&p| p > e), fill_to) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let (limit, to_set) = cut_limit(e, c, self.cfg.chunk_bytes, deferred);
        let full = c - e >= self.cfg.chunk_bytes;
        // Ruling R9b: a deferred set's boundary is cut at once — not after
        // `chunk_bytes` or `quiet_interval` — and is substantive by
        // definition, so the quiet-cut filter never declines it.
        if !to_set {
            let last = self.last_cut_ns.unwrap_or(now_ns);
            let quiet = self.cfg.quiet_interval.as_nanos() as u64;
            if !full && now_ns.saturating_sub(last) < quiet {
                return Ok(false);
            }
            if !full && self.quiet_declined == Some((e, c)) {
                return Ok(false);
            }
        }

        // Pass 1: the last frame end within the limit (at least one frame
        // when one fits under C), and the CRC of the canonical bytes.
        let journal = &*self.journal;
        let Some((to, crc, substantive)) = walk_cut(journal, e, c, limit)? else {
            return Ok(false); // purged under us; the next pass sees the gap
        };
        if to == e || declines_quiet_cut(full, to_set, substantive, e, newest_set) {
            self.quiet_declined = Some((e, c));
            return Ok(false);
        }

        // Pass 2: stream the same range into the tier object; a second walk
        // that saw different bytes refuses the object before it is placed.
        let (codec, level) = (self.cfg.compression, self.cfg.compression_level);
        let name = chunk_object(e, to);
        let put = self.target.put_if_absent(&name, &mut |dst| {
            let Some(cur) = FrameCursor::open(journal, e)? else {
                return Err(journal_err("purged under the cut"));
            };
            let mut src = ChunkReader::new(cur, to);
            let n = match write_object(dst, MAGIC_CHUNK, codec, level, e, to, &mut src, to - e) {
                Ok(n) => n,
                Err(err) => {
                    return Err(match src.journal_failed.take() {
                        Some(msg) => TierError::Journal(msg),
                        None => err,
                    });
                }
            };
            if src.crc.clone().finalize() != crc {
                return Err(journal_err(format!("[{e}, {to}) read differently twice")));
            }
            Ok(n)
        })?;
        match put {
            Put::Written(n) => {
                self.shared
                    .stats
                    .bytes_journal
                    .fetch_add(n, Ordering::Relaxed);
            }
            Put::Exists => self.compare_chunk(&name, e, to, crc)?,
        }
        self.last_cut_ns = Some(now_ns);
        crate::obs_event!(
            Info,
            "backup_chunk_written",
            node = self.node_id as u64,
            from = e,
            to = to
        );
        Ok(true)
    }

    /// An existing chunk of the same name must hold the same raw bytes
    /// (spec §4.5.3): header range, length and CRC, then a streamed compare.
    fn compare_chunk(&self, name: &str, from: u64, to: u64, crc: u32) -> Result<(), TierError> {
        let mismatch = || TierError::OverlapMismatch(name.to_string());
        let Some(obj) = self.target.open(name)? else {
            return Err(mismatch());
        };
        let mut theirs = ObjectReader::open(obj).map_err(|_| mismatch())?;
        let h = *theirs.header();
        if (h.from, h.to, h.raw_len, h.crc) != (from, to, to - from, crc) {
            return Err(mismatch());
        }
        let Some(cur) = FrameCursor::open(&self.journal, from)? else {
            return Ok(()); // purged meanwhile; the header already agreed
        };
        let mut ours = ChunkReader::new(cur, to);
        let (mut a, mut b) = (vec![0u8; 1 << 16], vec![0u8; 1 << 16]);
        loop {
            let n = fill(&mut ours, &mut a).map_err(|e| journal_err(e.to_string()))?;
            let m = fill(&mut theirs, &mut b).map_err(|_| mismatch())?;
            if n != m || a[..n] != b[..m] {
                return Err(mismatch());
            }
            if n == 0 {
                return Ok(());
            }
        }
    }
}

fn fill(r: &mut dyn Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

/// Where a row artifact lives under an instance directory.
pub fn local_row_artifact(instance_dir: &Path, row: u8, p: u64) -> PathBuf {
    instance_dir
        .join("snapshots")
        .join(row.to_string())
        .join(format!("snap-{p}.ultsnap"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_journal::JournalConfig;
    use uc_protocol::v2::frame::{
        CLUSTER_BODY_PREFIX_LEN, FRAME_TYPE_MESSAGE, FrameHeader, write_cluster_prefix,
        write_header_except_length,
    };

    /// Ruling PF4: under `CARGO_TARGET_TMPDIR`, never `/tmp`.
    fn tempdir() -> tempfile::TempDir {
        let root = std::env::var("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_node_tests")
            });
        std::fs::create_dir_all(&root).expect("scratch root");
        tempfile::Builder::new()
            .prefix("uc2-tier-writer-")
            .tempdir_in(&root)
            .expect("tempdir")
    }

    /// One frame `length` bytes long, its aligned span filled with `fill`
    /// past the header (so slack and padding bodies are visible garbage).
    fn frame(ty: u8, length: usize, fill: u8) -> Vec<u8> {
        let mut f = vec![fill; align_frame_len(length)];
        write_header_except_length(
            &mut f,
            &FrameHeader {
                length: 0,
                frame_type: ty,
                flags: 0,
                leadership_term_id: 7,
                client_id: 0,
                seq: 0,
                time_ns: 1,
            },
        );
        f[..4].copy_from_slice(&(length as u32).to_le_bytes());
        f
    }

    fn journal_with(dir: &Path, blocks: &[(u64, Vec<u8>)]) -> Journal {
        let j = Journal::open(JournalConfig::new(dir.join("journal"))).expect("journal");
        for (seq, (meta, bytes)) in blocks.iter().enumerate() {
            j.append(seq as u64, *meta, bytes).unwrap().wait().unwrap();
        }
        j
    }

    /// Rulings R8b + R11a: a bookkeeping-only quiet cut is declined unless
    /// the chain ends exactly at its newest set; a substantive, full or
    /// set-forced cut always goes ahead.
    #[test]
    fn a_bookkeeping_only_quiet_cut_runs_once_past_a_set_that_ends_the_chain() {
        // Bookkeeping only, chain ends at the newest set: cut.
        assert!(!declines_quiet_cut(false, false, false, 4096, 4096));
        // Bookkeeping only, chain already past the newest set: skip.
        assert!(declines_quiet_cut(false, false, false, 5000, 4096));
        // Substantive: cut either way.
        assert!(!declines_quiet_cut(false, false, true, 5000, 4096));
        assert!(!declines_quiet_cut(false, false, true, 4096, 4096));
        // Full or forced by a deferred set: never declined.
        assert!(!declines_quiet_cut(true, false, false, 5000, 4096));
        assert!(!declines_quiet_cut(false, true, false, 5000, 4096));
    }

    /// Ruling PF2 + the canonical form: raw frames, a padding frame's body
    /// and every frame's alignment slack zeroed; and a walk that reaches the
    /// journal's end (`durable`) stops there instead of reading a block that
    /// does not exist yet (the bug a quiet cut at exactly `C == durable` hit).
    #[test]
    fn the_walk_is_raw_canonical_and_stops_at_the_limit() {
        let d = tempdir();
        let a = frame(FRAME_TYPE_MESSAGE, HEADER_LEN + 5, 0xAA); // 64 B, 27 B slack
        let pad = frame(FRAME_TYPE_PADDING, 96, 0xBB); // header-only on the wire
        let b = frame(FRAME_TYPE_MESSAGE, HEADER_LEN + 40, 0xCC); // 96 B
        let block0 = [a.clone(), pad.clone()].concat();
        let j = journal_with(d.path(), &[(0, block0), (160, b.clone())]);

        let mut cur = FrameCursor::open(&j, 0).unwrap().unwrap();
        let mut out = Vec::new();
        for _ in 0..3 {
            assert!(matches!(
                cur.next_frame(256, &mut out).unwrap(),
                Walk::Frame
            ));
        }
        assert_eq!(cur.pos, 256);
        assert!(matches!(
            cur.next_frame(256, &mut out).unwrap(),
            Walk::Limit
        ));
        let mut want = Vec::new();
        want.extend_from_slice(&a[..HEADER_LEN + 5]);
        want.resize(64, 0);
        want.extend_from_slice(&pad[..HEADER_LEN]);
        want.resize(160, 0);
        want.extend_from_slice(&b[..HEADER_LEN + 40]);
        want.resize(256, 0);
        assert_eq!(out, want);

        // A limit inside the last frame stops before it.
        let mut cur = FrameCursor::open(&j, 64).unwrap().unwrap();
        let mut out = Vec::new();
        assert!(matches!(
            cur.next_frame(200, &mut out).unwrap(),
            Walk::Frame
        ));
        assert!(matches!(
            cur.next_frame(200, &mut out).unwrap(),
            Walk::Limit
        ));
        assert_eq!((cur.pos, out.len()), (160, 96));

        // The second-pass reader yields the same bytes.
        let mut r = ChunkReader::new(FrameCursor::open(&j, 0).unwrap().unwrap(), 256);
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, want);
        assert_eq!(r.crc.finalize(), crc32fast::hash(&want));
    }

    #[test]
    fn a_frame_crossing_its_block_is_refused_by_name() {
        let d = tempdir();
        let mut f = frame(FRAME_TYPE_MESSAGE, 200, 0); // 224 B aligned…
        f.truncate(128); // …in a 128 B block
        let j = journal_with(d.path(), &[(0, f)]);
        let mut cur = FrameCursor::open(&j, 0).unwrap().unwrap();
        let e = cur.next_frame(1 << 20, &mut Vec::new()).err().unwrap();
        assert!(e.to_string().contains("crossing its archived block"), "{e}");
    }

    #[test]
    fn below_the_first_block_is_none() {
        let d = tempdir();
        let j = journal_with(d.path(), &[(4096, frame(FRAME_TYPE_MESSAGE, 64, 0))]);
        assert!(FrameCursor::open(&j, 0).unwrap().is_none());
    }

    #[test]
    fn tier_bookkeeping_is_padding_and_backup_watermarks_only() {
        let cluster = |kind: ClusterKind| {
            let mut f = frame(
                FRAME_TYPE_CLUSTER,
                HEADER_LEN + CLUSTER_BODY_PREFIX_LEN + 48,
                0,
            );
            write_cluster_prefix(&mut f[HEADER_LEN..], kind);
            f
        };
        assert!(is_tier_bookkeeping(&frame(FRAME_TYPE_PADDING, 96, 0)));
        assert!(is_tier_bookkeeping(&cluster(ClusterKind::BackupWatermark)));
        assert!(!is_tier_bookkeeping(&cluster(ClusterKind::Settings)));
        assert!(!is_tier_bookkeeping(&frame(FRAME_TYPE_MESSAGE, 64, 0)));
    }

    /// Ruling R8d: (a) no watermark → the newest held set; (b) a set at or
    /// below `through` → copied (newest such); (c) the gap case → the newest
    /// held set at or above `first_base`; otherwise every set waits.
    #[test]
    fn a_set_is_copied_only_inside_the_chain_or_to_start_one() {
        // (a) an empty tier starts at the newest held set.
        assert_eq!(pick_set(&[900, 500], None, 0), Some(900));
        assert_eq!(pick_set(&[], None, 0), None);
        // (b) inside the chain: newest set at or below `through`.
        assert_eq!(pick_set(&[900, 500, 300], Some(600), 0), Some(500));
        assert_eq!(pick_set(&[600], Some(600), 0), Some(600));
        // Defer: above the chain end and the journal still reaches it — the
        // chain must pass P first (no second chain on a healthy cluster).
        assert_eq!(pick_set(&[900], Some(600), 100), None);
        assert_eq!(pick_set(&[900], Some(600), 600), None);
        // (c) the gap: the journal starts past the chain end; the newest held
        // set at or above `first_base` starts the next chain.
        assert_eq!(pick_set(&[1200, 900, 700], Some(600), 800), Some(1200));
        assert_eq!(
            pick_set(&[700], Some(600), 800),
            None,
            "below first_base: unusable"
        );
        // (b) still wins over (c) for a set inside the old chain.
        assert_eq!(pick_set(&[1200, 400], Some(600), 800), Some(400));
    }

    /// Ruling RF2: the lowest chain end below `through` this node's journal
    /// still holds, and the set starting the next chain above it; none when
    /// the journal starts past every lower end (a real gap) or there is
    /// only one chain.
    #[test]
    fn a_fillable_gap_is_the_lowest_chain_end_the_journal_still_holds() {
        use uc_protocol::v2::catalog::SetKind;
        let set = |p| SetLabel {
            position: p,
            kind: SetKind::Full,
            time_ns: 1,
            rows: vec![],
            cluster_hash: 0,
            cluster_size: 0,
        };
        let l = TierListing {
            sets: vec![set(100), set(900)],
            incomplete_sets: vec![],
            chunks: vec![(100, 300), (300, 400), (900, 1200)],
            corrupt_sets: vec![],
            fences: vec![],
        };
        // A's chain ends at 400, B's starts at 900 and reaches 1200.
        assert_eq!(fillable_gap(&l, 1200, 0), Some((400, 900)));
        assert_eq!(fillable_gap(&l, 1200, 400), Some((400, 900)));
        // This node's journal starts past 400: nothing it can fill.
        assert_eq!(fillable_gap(&l, 1200, 401), None);
        // One chain only: no gap.
        let one = TierListing {
            sets: vec![set(100)],
            chunks: vec![(100, 300), (300, 400)],
            ..l.clone()
        };
        assert_eq!(fillable_gap(&one, 400, 0), None);
        // Partly filled: the end moved up, the target set did not.
        let part = TierListing {
            chunks: vec![(100, 300), (300, 400), (400, 650), (900, 1200)],
            ..l.clone()
        };
        assert_eq!(fillable_gap(&part, 1200, 0), Some((650, 900)));
        // Joined: no gap.
        let joined = TierListing {
            chunks: vec![(100, 300), (300, 400), (400, 900), (900, 1200)],
            ..l
        };
        assert_eq!(fillable_gap(&joined, 1200, 0), None);
    }

    /// Task 9 (Task 8 review carry): the keep hold names the set the writer
    /// will actually copy — `pick_set`'s answer now, or, while deferred, the
    /// set the chain reaches next (the lowest held set above `through`, the
    /// journal still reaching the chain end). A held set `pick_set` can never
    /// use (below `first_base` in the gap case) is not held.
    #[test]
    fn the_keep_hold_is_the_set_the_writer_will_copy() {
        // Eligible now: whatever `pick_set` picks.
        assert_eq!(keep_hold(&[900, 500], None, 0), Some(900));
        assert_eq!(keep_hold(&[900, 500, 300], Some(600), 0), Some(500));
        assert_eq!(keep_hold(&[1200, 900], Some(600), 800), Some(1200));
        // Deferred: the lowest set above the chain end, which the chain
        // passes first.
        assert_eq!(keep_hold(&[1200, 900], Some(600), 100), Some(900));
        assert_eq!(keep_hold(&[900], Some(600), 600), Some(900));
        // The gap case with every held set below `first_base`: nothing
        // `pick_set` can ever copy, so nothing is held (the old rule held
        // 700 here).
        assert_eq!(keep_hold(&[700], Some(600), 800), None);
        // Nothing held.
        assert_eq!(keep_hold(&[], None, 0), None);
        assert_eq!(keep_hold(&[], Some(600), 0), None);
    }

    /// Ruling R9b: a deferred set `P` ends the next cut exactly at `P`; a
    /// `P` more than one chunk away ends it at the last frame end within
    /// `through + chunk_bytes`; no deferred set (or `P` above `C`) keeps
    /// the ordinary limit and no forcing.
    #[test]
    fn a_deferred_set_ends_the_cut_at_its_position() {
        // The limit rule.
        assert_eq!(cut_limit(0, 192, 1 << 20, Some(128)), (128, true));
        assert_eq!(cut_limit(0, 192, 100, Some(128)), (100, true));
        assert_eq!(cut_limit(0, 192, 1 << 20, None), (192, false));
        assert_eq!(cut_limit(0, 192, 100, None), (100, false));
        assert_eq!(cut_limit(0, 192, 1 << 20, Some(256)), (192, false), "P > C");
        assert_eq!(
            cut_limit(128, 192, 1 << 20, Some(128)),
            (192, false),
            "P <= E"
        );
        // …and the walk lands on frame ends: three 64 B frames, P = 128.
        let d = tempdir();
        let block = [
            frame(FRAME_TYPE_MESSAGE, 40, 1),
            frame(FRAME_TYPE_MESSAGE, 40, 2),
            frame(FRAME_TYPE_MESSAGE, 40, 3),
        ]
        .concat();
        let j = journal_with(d.path(), &[(0, block)]);
        let to = |chunk, deferred| {
            let (limit, _) = cut_limit(0, 192, chunk, deferred);
            walk_cut(&j, 0, 192, limit).unwrap().unwrap().0
        };
        assert_eq!(to(1 << 20, Some(128)), 128, "exactly at P");
        assert_eq!(to(100, Some(128)), 64, "last frame end within one chunk");
        assert_eq!(to(1 << 20, None), 192, "the old rule: up to C");
    }

    /// Task 9: the purge cap a node reads — the writer's progress while the
    /// hold is armed (`0` = no chain yet holds everything), none once the
    /// hold is released for space or the writer has stopped for good.
    #[test]
    fn the_purge_cap_is_progress_unless_released_or_stopped() {
        let s = WriterShared::default();
        assert_eq!(s.purge_cap(), Some(0), "no chain yet: hold everything");
        s.progress.store(4096, Ordering::Release);
        assert_eq!(s.purge_cap(), Some(4096));
        s.hold_released.store(true, Ordering::Release);
        assert_eq!(s.purge_cap(), None, "released for space");
        s.hold_released.store(false, Ordering::Release);
        s.set_hold.store(3000, Ordering::Release);
        assert_eq!(s.keep(), Some(3000));
        s.stopped.store(true, Ordering::Release);
        assert_eq!(s.purge_cap(), None, "a stopped writer holds nothing");
        assert_eq!(s.keep(), None);
        s.stopped.store(false, Ordering::Release);
        s.hold_released.store(true, Ordering::Release);
        assert_eq!(s.keep(), None);
        s.hold_released.store(false, Ordering::Release);
        s.set_hold.store(0, Ordering::Release);
        assert_eq!(s.keep(), None, "0 = no set held");
    }

    /// A journal that cannot supply the range is named as the journal, not
    /// the target (fix round 1).
    #[test]
    fn a_journal_short_of_the_range_is_a_journal_error() {
        let d = tempdir();
        let j = journal_with(d.path(), &[(0, frame(FRAME_TYPE_MESSAGE, 64, 0))]);
        let mut r = ChunkReader::new(FrameCursor::open(&j, 0).unwrap().unwrap(), 128);
        let mut out = Vec::new();
        assert!(r.read_to_end(&mut out).is_err());
        assert_eq!(r.journal_failed.as_deref(), Some("purged under the cut"));
    }

    /// Wraps an `FsTarget`; the first time the LAST object of a set is
    /// placed it deletes the first one (another writer's retention clearing
    /// the directory as aged residue mid-copy), and it logs every put.
    struct VanishOnce {
        inner: crate::tier::target::FsTarget,
        last: String,
        victim: String,
        fired: Mutex<bool>,
        puts: Mutex<Vec<String>>,
    }

    impl BackupTarget for VanishOnce {
        fn put_if_absent(
            &self,
            path: &str,
            write: &mut dyn FnMut(&mut File) -> Result<u64, TierError>,
        ) -> Result<Put, TierError> {
            self.puts.lock().unwrap().push(path.to_string());
            let r = self.inner.put_if_absent(path, write)?;
            let mut fired = self.fired.lock().unwrap();
            if path == self.last && !*fired {
                *fired = true;
                self.inner.delete(&self.victim)?;
            }
            Ok(r)
        }
        fn list(&self, dir: &str) -> Result<Vec<String>, TierError> {
            self.inner.list(dir)
        }
        fn open(&self, path: &str) -> Result<Option<File>, TierError> {
            self.inner.open(path)
        }
        fn delete(&self, path: &str) -> Result<(), TierError> {
            self.inner.delete(path)
        }
        fn delete_dir_if_empty(&self, dir: &str) -> Result<(), TierError> {
            self.inner.delete_dir_if_empty(dir)
        }
    }

    /// Ruling R10b: an object deleted between the object puts and the
    /// `SET` put is placed again BEFORE `SET`, and the tier verifies clean.
    #[test]
    fn set_is_written_only_after_a_vanished_object_is_restored() {
        use crate::tier::layout::RowLabel;
        let d = tempdir();
        let src = d.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        let p = 4096u64;
        let payload = vec![7u8; 3000];
        let mut row = Vec::new();
        uc_service::snapshots::write_snapshot_envelope(&mut row, p, 1).unwrap();
        row.extend_from_slice(&payload);
        let cluster = vec![9u8; 500];
        std::fs::write(src.join("row"), &row).unwrap();
        std::fs::write(src.join("cluster"), &cluster).unwrap();
        let h = |b: &[u8]| hash_stream(&mut &b[..]).unwrap();
        let label = SetLabel {
            position: p,
            kind: uc_protocol::v2::catalog::SetKind::Full,
            time_ns: 1,
            rows: vec![RowLabel {
                row: 0,
                hash: h(&payload),
                version: 1,
                size: row.len() as u64,
            }],
            cluster_hash: h(&cluster),
            cluster_size: cluster.len() as u64,
        };
        let local = |name: &str, tier_name: String, env, hash, size| LocalFile {
            tier_name,
            file: File::open(src.join(name)).unwrap(),
            len: std::fs::metadata(src.join(name)).unwrap().len(),
            envelope_at: env,
            hash,
            size,
        };
        let mut files = vec![
            local(
                "row",
                row_object(p, 0),
                Some(p),
                h(&payload),
                row.len() as u64,
            ),
            local(
                "cluster",
                cluster_object(p),
                None,
                h(&cluster),
                cluster.len() as u64,
            ),
        ];
        let tier = d.path().join("tier");
        let t = VanishOnce {
            inner: crate::tier::target::FsTarget::new(&tier),
            last: cluster_object(p),
            victim: row_object(p, 0),
            fired: Mutex::new(false),
            puts: Mutex::new(Vec::new()),
        };
        let stats = WriterStats::default();
        place_set(&t, Codec::Zstd, 1, &label, &mut files, &stats).unwrap();
        let puts = t.puts.lock().unwrap().clone();
        eprintln!("puts: {puts:?}");
        assert_eq!(
            puts,
            vec![
                row_object(p, 0),
                cluster_object(p),
                row_object(p, 0),
                set_label(p)
            ],
            "the vanished row object is re-placed, then SET"
        );
        assert!(*t.fired.lock().unwrap());
        let report = crate::tier::chain::verify(&t, true).unwrap();
        assert_eq!(report.sets, 1);
        assert!(report.findings.is_empty(), "{:?}", report.findings);
    }

    /// A present object of the wrong raw length cannot be repaired
    /// (write-once): refused as an overlap mismatch, and no `SET`.
    #[test]
    fn a_short_object_already_in_the_tier_refuses_the_set() {
        let d = tempdir();
        let src = d.path().join("cluster");
        std::fs::write(&src, vec![1u8; 100]).unwrap();
        let tier = d.path().join("tier");
        let t = crate::tier::target::FsTarget::new(&tier);
        t.put_if_absent(&cluster_object(64), &mut |f| {
            write_object(
                f,
                MAGIC_ARTIFACT,
                Codec::None,
                0,
                0,
                0,
                &mut &[1u8; 40][..],
                40,
            )
        })
        .unwrap();
        let files = [LocalFile {
            tier_name: cluster_object(64),
            file: File::open(&src).unwrap(),
            len: 100,
            envelope_at: None,
            hash: 0,
            size: 100,
        }];
        let e = absent_objects(&t, &files).unwrap_err();
        assert!(matches!(e, TierError::OverlapMismatch(_)), "{e}");
        assert!(t.open(&set_label(64)).unwrap().is_none());
    }

    #[test]
    fn writer_errors_have_distinct_labels() {
        let labels: BTreeSet<&str> = WriterError::ALL.iter().map(|e| e.label()).collect();
        assert_eq!(labels.len(), WriterError::ALL.len());
        let s = WriterStats::default();
        s.count(WriterError::HashMismatch);
        assert_eq!(s.errors(WriterError::HashMismatch), 1);
        assert_eq!(s.errors(WriterError::Foreign), 0);
    }
}
