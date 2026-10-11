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
use super::chain::{TierListing, list_tier, watermark_of};
use super::layout::{SetLabel, TierInfo, chunk_object, cluster_object, row_object, set_label};
use super::object::{Codec, MAGIC_ARTIFACT, MAGIC_CHUNK, ObjectReader, write_object};
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
}

impl Default for BackupConfig {
    fn default() -> Self {
        BackupConfig {
            target: PathBuf::new(),
            chunk_bytes: DEFAULT_CHUNK_BYTES,
            quiet_interval: DEFAULT_QUIET_INTERVAL,
            compression: Codec::Zstd,
            compression_level: DEFAULT_COMPRESSION_LEVEL,
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
}

impl WriterError {
    pub const ALL: [WriterError; 5] = [
        WriterError::Foreign,
        WriterError::Unavailable,
        WriterError::OverlapMismatch,
        WriterError::HashMismatch,
        WriterError::JournalUnavailable,
    ];

    pub fn label(self) -> &'static str {
        match self {
            WriterError::Foreign => "foreign",
            WriterError::Unavailable => "unavailable",
            WriterError::OverlapMismatch => "overlap_mismatch",
            WriterError::HashMismatch => "hash_mismatch",
            WriterError::JournalUnavailable => "journal_unavailable",
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
    errors: [AtomicU64; 5],
}

impl WriterStats {
    pub fn errors(&self, r: WriterError) -> u64 {
        self.errors[r as usize].load(Ordering::Relaxed)
    }

    fn count(&self, r: WriterError) {
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
    /// The writer refused its target for good (foreign or overlap
    /// mismatch); `Uc2BackupWriterError`. A stopped writer holds nothing
    /// (ruling R9a): its alert covers it.
    pub stopped: AtomicBool,
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
        }
    }

    /// One bounded duty cycle — at most one set or one chunk; `true` =
    /// progress (the runner then skips its sleep).
    pub fn step(&mut self, now_ns: u64) -> bool {
        if self.stopped || now_ns < self.retry_at_ns {
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
        self.maybe_offer(now_ns);
        Ok(progress)
    }

    fn refresh(&mut self, now_ns: u64) -> Result<(), TierError> {
        let l = list_tier(&*self.target)?;
        let wm = watermark_of(&l);
        // A new chain that does not connect to the old one: the tier's
        // history now has a hole between the old reach and the new start.
        if let (Some((_, _, old_through)), Some((oldest, _, _))) = (self.wm, wm)
            && oldest > old_through
        {
            self.note_gap(old_through, oldest);
        }
        self.wm = wm;
        if let Some((_, _, through)) = self.wm {
            self.shared.progress.store(through, Ordering::Release);
        }
        self.listing = Some(l);
        self.listed_at_ns = now_ns;
        self.scan_due = true;
        Ok(())
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
        let have: BTreeSet<u64> = self
            .listing
            .as_ref()
            .map(|l| l.sets.iter().map(|s| s.position).collect())
            .unwrap_or_default();
        let mut todo: Vec<SetLabel> = self
            .view
            .snapshot_inner()
            .catalog
            .iter()
            .filter(|e| {
                e.is_agreed()
                    && !have.contains(&e.position)
                    && !self.mismatched.contains(&e.position)
            })
            .map(SetLabel::from_catalog)
            .collect();
        todo.sort_by_key(|l| std::cmp::Reverse(l.position));
        // The sets this node holds whole on disk, newest first.
        let mut held: Vec<(SetLabel, Vec<LocalFile>)> = Vec::new();
        for label in todo {
            if let Some(files) = self.open_local_set(&label)? {
                held.push((label, files));
            }
        }
        let through = self.wm.map(|(_, _, t)| t);
        loop {
            let positions: Vec<u64> = held.iter().map(|(l, _)| l.position).collect();
            // The keep hold (Task 9): the set about to be copied, or the
            // deferred one the chain reaches next — re-derived after a
            // mismatched set drops out.
            let hold = keep_hold(&positions, through, first_base).unwrap_or(0);
            self.shared.set_hold.store(hold, Ordering::Release);
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
        let mut want: Vec<(PathBuf, String, Option<u64>, u64)> = label
            .rows
            .iter()
            .map(|r| {
                (
                    local_row_artifact(&self.instance_dir, r.row, p),
                    row_object(p, r.row),
                    Some(p),
                    r.hash,
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
        ));
        for (path, tier_name, envelope_at, hash) in want {
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
        for f in files.iter_mut() {
            f.file.seek(SeekFrom::Start(0))?;
            let (src, len) = (&mut f.file, f.len);
            let put = self.target.put_if_absent(&f.tier_name, &mut |dst| {
                src.seek(SeekFrom::Start(0))?;
                write_object(dst, MAGIC_ARTIFACT, codec, level, 0, 0, src, len)
            })?;
            match put {
                Put::Written(n) => {
                    self.shared.stats.bytes_set.fetch_add(n, Ordering::Relaxed);
                }
                Put::Exists => self.compare_artifact(f)?,
            }
        }
        let text = label.render();
        let put = self
            .target
            .put_if_absent(&set_label(label.position), &mut |dst| {
                io::Write::write_all(dst, text.as_bytes())?;
                Ok(text.len() as u64)
            })?;
        match put {
            Put::Written(n) => {
                self.shared.stats.bytes_set.fetch_add(n, Ordering::Relaxed);
            }
            Put::Exists => {
                let mut s = String::new();
                if let Some(mut f) = self.target.open(&set_label(label.position))? {
                    f.read_to_string(&mut s)?;
                }
                if SetLabel::parse(&s).ok().as_ref() != Some(label) {
                    return Err(TierError::OverlapMismatch(set_label(label.position)));
                }
            }
        }
        crate::obs_event!(
            Info,
            "backup_set_copied",
            node = self.node_id as u64,
            position = label.position,
            rows = label.rows.len() as u64
        );
        Ok(())
    }

    /// Spec §5.2: an existing artifact is compared by envelope position and
    /// payload hash (rows) or hash (cluster) — never byte equality, since
    /// two members' agreeing copies may differ in their envelopes' version.
    fn compare_artifact(&self, f: &LocalFile) -> Result<(), TierError> {
        let mismatch = || TierError::OverlapMismatch(f.tier_name.clone());
        let Some(obj) = self.target.open(&f.tier_name)? else {
            return Err(mismatch());
        };
        let mut r = ObjectReader::open(obj).map_err(|_| mismatch())?;
        let same = match f.envelope_at {
            Some(p) => row_identity(&mut r).map_err(|_| mismatch())? == Some((p, f.hash)),
            None => hash_stream(&mut r).map_err(|_| mismatch())? == f.hash,
        };
        if same { Ok(()) } else { Err(mismatch()) }
    }

    // ---------------------------------------------------------- journal

    fn cut_one_chunk(&mut self, now_ns: u64) -> Result<bool, TierError> {
        let Some((_, _, e)) = self.wm else {
            return Ok(false); // no set in the tier yet: nothing to extend
        };
        let k = self.cnc.counters();
        let c = k.commit.load_acquire().min(k.durable.load_acquire());
        if c <= e {
            return Ok(false);
        }
        let first_base = self.first_base.load(Ordering::Acquire);
        if first_base > e {
            self.note_gap(e, first_base);
            return Ok(false); // the set phase starts the next chain
        }
        let last = self.last_cut_ns.unwrap_or(now_ns);
        let quiet = self.cfg.quiet_interval.as_nanos() as u64;
        if c - e < self.cfg.chunk_bytes && now_ns.saturating_sub(last) < quiet {
            return Ok(false);
        }
        let full = c - e >= self.cfg.chunk_bytes;
        if !full && self.quiet_declined == Some((e, c)) {
            return Ok(false);
        }
        let limit = c.min(e + self.cfg.chunk_bytes);

        // Pass 1: the last frame end within the limit (at least one frame
        // when one fits under C), and the CRC of the canonical bytes.
        let journal = &*self.journal;
        let Some(mut cur) = FrameCursor::open(journal, e)? else {
            return Ok(false); // purged under us; the next pass sees the gap
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
                Walk::Gone => return Ok(false),
            }
        }
        let to = cur.pos;
        if to == e || (!full && !substantive) {
            self.quiet_declined = Some((e, c));
            return Ok(false);
        }
        let crc = crc.finalize();

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
