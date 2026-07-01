//! Implements `openraft::storage::RaftLogStorage` over `ultima_journal`.
//!
//! Storage seam mapping (per spec §6 "RaftLogStorage over ultima_journal"):
//!   * vote / committed / last_purged → StableValue<…>
//!   * append → Journal::append (seq=index, meta=term.0, payload=bincode(entry))
//!   * truncate → Journal::truncate_after
//!   * purge → Journal::purge_before
//!   * get_log_state → first_seq/last_seq + meta lookups
//!   * try_get_log_entries → Journal::iter_range

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use ultima_journal::{Durability, Journal, JournalConfig, StableValue, StableValueConfig};

use super::{RaftLogId, RaftStoredMembership, RaftVote};
use crate::raft::entry_cache::EntryCache;
use crate::ClusterError;

const SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;

/// Parse the `UC_JOURNAL_PREALLOC` value into the journal preallocation flag.
/// Default ON (task36 promotion): only an explicit `"0"`/`"false"` disables it;
/// unset or anything else is on. Pure helper so it can be unit-tested without
/// touching the process env. Mirrors `network::parse_pipeline_depth`.
fn parse_journal_prealloc(s: Option<&str>) -> bool {
    !matches!(s, Some("0") | Some("false"))
}

/// Runtime toggle for `JournalConfig.preallocate_segments` (ultima_journal task36).
/// Reads `UC_JOURNAL_PREALLOC`; default ON post-promotion, set `=0` to roll back.
/// The A/B run-book (`uc_autobench/scripts/prealloc-commit-ab.md`) drives this.
fn journal_prealloc_from_env() -> bool {
    parse_journal_prealloc(std::env::var("UC_JOURNAL_PREALLOC").ok().as_deref())
}

/// Parse `UC_JOURNAL_PREALLOC_FILL` into the segment fill strategy. Default
/// `FallocateZeroRange` (unset/unknown) — the A/B winner that cures the
/// background-fill p99 tail and itself falls back to paced on unsupported
/// filesystems; `"full"` restores the legacy zero-write baseline and `"paced"`
/// selects the dependency-free contention fix. Orthogonal to
/// `UC_JOURNAL_PREALLOC` (on/off). Pure helper for unit testing without env.
fn parse_prealloc_fill(s: Option<&str>) -> ultima_journal::PreallocFill {
    use ultima_journal::PreallocFill;
    match s {
        Some("full") => PreallocFill::ZeroWriteFull,
        Some("paced") => PreallocFill::ZeroWritePaced,
        _ => PreallocFill::FallocateZeroRange,
    }
}

fn journal_prealloc_fill_from_env() -> ultima_journal::PreallocFill {
    parse_prealloc_fill(std::env::var("UC_JOURNAL_PREALLOC_FILL").ok().as_deref())
}

/// `UC_LOG_CACHE_BYTES` — recent-entry cache budget in bytes; 0 disables. Default 256 MiB.
const LOG_CACHE_BYTES_DEFAULT: usize = 256 * 1024 * 1024;
fn log_cache_bytes_from_env() -> usize {
    std::env::var("UC_LOG_CACHE_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(LOG_CACHE_BYTES_DEFAULT)
}

/// Persisted snapshot meta (the last installed snapshot's metadata + a
/// pointer to its bytes file under data_dir).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredSnapshotMeta {
    pub last_log_id: Option<RaftLogId>,
    pub last_membership: RaftStoredMembership,
    /// Filename (relative to data_dir) of the snapshot bytes.
    pub bytes_filename: String,
}

/// Bundle of durable handles + data_dir, passed to `AdaptedStateMachine::new`
/// so it can persist install_snapshot state and recover on startup.
pub struct LogStorageHandles {
    pub last_applied: Arc<StableValue<RaftLogId>>,
    pub snapshot_meta: Arc<StableValue<StoredSnapshotMeta>>,
    /// M5: durable marker for "last log_index whose `on_committed` completed
    /// (Ok or Permanent)." Advanced per-record by the output_dispatcher.
    /// Recovery scans `(load(), last_applied]` on leader-acquisition.
    pub output_progress: Arc<StableValue<u64>>,
    pub data_dir: PathBuf,
}

pub struct JournalLogStorage {
    pub(crate) journal: Arc<Journal>,
    pub(crate) vote: Arc<StableValue<RaftVote>>,
    pub(crate) committed: Arc<StableValue<RaftLogId>>,
    pub(crate) last_purged: Arc<StableValue<RaftLogId>>,
    pub(crate) last_applied: Arc<StableValue<RaftLogId>>,
    pub(crate) snapshot_meta: Arc<StableValue<StoredSnapshotMeta>>,
    /// M5: see `LogStorageHandles::output_progress`.
    pub(crate) output_progress: Arc<StableValue<u64>>,
    /// Serializes seq assignment per the journal's caller-coordination requirement.
    /// openraft already serializes appends, so this is a no-contention guarantee.
    pub(crate) append_lock: Arc<Mutex<()>>,
    /// In-memory cache of recent log entries; shared with all log readers via `Arc`.
    /// Populated on `append`, consulted on `try_get_log_entries`, evicted on
    /// `truncate_after`/`purge`. `budget_bytes=0` disables; see `UC_LOG_CACHE_BYTES`.
    pub(crate) cache: Arc<EntryCache>,
}

impl JournalLogStorage {
    /// Open with the default log durability (`Eventual` — Aeron `fileSyncLevel=0`
    /// model: ack on page-cache write, background fsync, durability via quorum
    /// replication). Use [`Self::open_with_durability`] to choose `Consistent`.
    pub fn open(data_dir: &Path) -> Result<Self, ClusterError> {
        Self::open_with_durability(data_dir, Durability::Eventual)
    }

    /// Open the Raft log + metadata. `log_durability` controls ONLY the log
    /// journal; the metadata `StableValue`s are always `Consistent`.
    pub fn open_with_durability(
        data_dir: &Path,
        log_durability: Durability,
    ) -> Result<Self, ClusterError> {
        std::fs::create_dir_all(data_dir.join("journal"))?;

        let journal = Arc::new(Journal::open(JournalConfig {
            dir: data_dir.join("journal"),
            segment_size_bytes: SEGMENT_SIZE_BYTES,
            durability: log_durability,
            // task36 promotion: preallocation is ON by default; set
            // UC_JOURNAL_PREALLOC=0 to roll back. Gated on the cloud A/B (see
            // uc_autobench/scripts/prealloc-commit-ab.md) before this branch merges.
            preallocate_segments: journal_prealloc_from_env(),
            prealloc_fill: journal_prealloc_fill_from_env(),
            prealloc_fill_chunk_bytes: 4 * 1024 * 1024,
        })?);

        let vote = Arc::new(StableValue::open(StableValueConfig {
            path: data_dir.join("vote.state"),
            durability: Durability::Consistent,
            max_payload_bytes: 4096 - 17,
        })?);

        let committed = Arc::new(StableValue::open(StableValueConfig {
            path: data_dir.join("committed.state"),
            durability: Durability::Consistent,
            max_payload_bytes: 4096 - 17,
        })?);

        let last_purged = Arc::new(StableValue::open(StableValueConfig {
            path: data_dir.join("last_purged.state"),
            durability: Durability::Consistent,
            max_payload_bytes: 4096 - 17,
        })?);

        let last_applied = Arc::new(StableValue::open(StableValueConfig {
            path: data_dir.join("last_applied.state"),
            durability: Durability::Consistent,
            max_payload_bytes: 4096 - 17,
        })?);

        let snapshot_meta = Arc::new(StableValue::open(StableValueConfig {
            path: data_dir.join("snapshot_meta.state"),
            durability: Durability::Consistent,
            max_payload_bytes: 4096 - 17,
        })?);

        let output_progress = Arc::new(StableValue::open(StableValueConfig {
            path: data_dir.join("output_progress.state"),
            durability: Durability::Consistent,
            max_payload_bytes: 4096 - 17,
        })?);

        Ok(Self {
            journal,
            vote,
            committed,
            last_purged,
            last_applied,
            snapshot_meta,
            output_progress,
            append_lock: Arc::new(Mutex::new(())),
            cache: Arc::new(EntryCache::new(log_cache_bytes_from_env())),
        })
    }

    /// Log entries written but not yet fsync-durable — the Eventual-mode window
    /// (`last_seq - durable_seq`). Always 0 in Consistent mode. This is the health
    /// signal for Eventual durability; surface it via node telemetry.
    pub fn durability_lag(&self) -> u64 {
        let last = self.journal.last_seq().unwrap_or(0);
        last.saturating_sub(self.journal.durable_seq())
    }

    /// The `RaftLogId` of the record at `seq` (the entry's own `log_id`), or
    /// `None` if `seq == 0` or no record exists there. Used by recovery to clamp
    /// a power-loss-inverted `committed` down to the durable log tail.
    pub(crate) fn last_log_id_at(&self, seq: u64) -> Result<Option<RaftLogId>, ClusterError> {
        if seq == 0 {
            return Ok(None);
        }
        let Some((_term, payload)) = self
            .journal
            .read(seq)
            .map_err(|e| ClusterError::Recovery(format!("read seq {seq}: {e}")))?
        else {
            return Ok(None);
        };
        let (entry, _) = bincode::serde::decode_from_slice::<
            <TypeConfig as openraft::RaftTypeConfig>::Entry,
            _,
        >(&payload, bincode::config::standard())
        .map_err(|e| ClusterError::Recovery(format!("decode seq {seq}: {e}")))?;
        Ok(Some(entry.log_id))
    }

    pub fn handles(&self, data_dir: PathBuf) -> LogStorageHandles {
        LogStorageHandles {
            last_applied: self.last_applied.clone(),
            snapshot_meta: self.snapshot_meta.clone(),
            output_progress: self.output_progress.clone(),
            data_dir,
        }
    }

    /// Replace the cache with a given budget (test/test-helpers only).
    /// Allows tests to exercise both enabled (budget>0) and disabled (budget=0)
    /// code paths without touching the process env (which is racy under parallel tests).
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn _with_cache_budget(self, budget_bytes: usize) -> Self {
        Self {
            cache: Arc::new(EntryCache::new(budget_bytes)),
            ..self
        }
    }

    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_vote(&self) -> &StableValue<RaftVote> {
        &self.vote
    }
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_committed(&self) -> &StableValue<RaftLogId> {
        &self.committed
    }
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_last_purged(&self) -> &StableValue<RaftLogId> {
        &self.last_purged
    }
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_last_applied(&self) -> &StableValue<RaftLogId> {
        &self.last_applied
    }
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_snapshot_meta(&self) -> &StableValue<StoredSnapshotMeta> {
        &self.snapshot_meta
    }
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_output_progress(&self) -> &StableValue<u64> {
        &self.output_progress
    }
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_journal(&self) -> Arc<Journal> {
        self.journal.clone()
    }
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_last_purged_arc(&self) -> Arc<StableValue<RaftLogId>> {
        self.last_purged.clone()
    }
}

// ---------------------------------------------------------------------------
// RaftLogReader / RaftLogStorage impls.
//
// All methods fully implemented for openraft 0.10's storage-v2 trait surface.
//
// openraft 0.10 vs 0.9.24 changes (already applied here):
//   * All trait methods return io::Error (StorageError / StorageIOError dropped).
//   * The append callback type is `IOFlushed<C>` (LogFlushed is a deprecated alias).
//   * `truncate` is renamed to `truncate_after` and takes `Option<LogIdOf<C>>`.
//   * `read_vote` moved from RaftLogStorage to RaftLogReader.
// ---------------------------------------------------------------------------

use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;

use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::storage::IOFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;

use super::TypeConfig;

fn sv_io(e: ultima_journal::StableValueError) -> io::Error {
    io::Error::other(e)
}

fn journal_io(e: ultima_journal::JournalError) -> io::Error {
    io::Error::other(e)
}

/// Cap for a single `limited_get_log_entries` replication read.
///
/// openraft's DEFAULT `limited_get_log_entries` returns the FULL, unbounded
/// `[start, end)` range. When a follower lags under load, that materializes the
/// entire gap in ONE call — a multi-MB journal read + CRC-verify + huge buffer
/// alloc that saturates a single core (observed as ~20% `read()` + ~21% `crc32`
/// on the leader's busiest thread at the ~15k ceiling), and it is
/// self-reinforcing: a big read makes a big AppendEntries, which the follower is
/// slow to append+fsync, which grows the gap, which makes the next read bigger.
///
/// openraft sends at most `max_payload_entries` (default 300, UC default 300)
/// entries per AppendEntries regardless, so returning more is pure waste. We cap
/// each read to this bound; openraft explicitly tolerates a short return
/// (`replication/stream_state.rs` — "limited_get_log_entries will return logs
/// smaller than the range"), so replication simply catches up in bounded chunks.
/// Kept comfortably above the 300 default so a full batch is never starved.
const LIMITED_GET_MAX_ENTRIES: u64 = 512;

impl RaftLogReader<TypeConfig> for JournalLogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<TypeConfig as openraft::RaftTypeConfig>::Entry>, io::Error> {
        use std::ops::Bound;
        // Resolve to concrete [start, end). Unbounded ends can't be cache-checked
        // (we don't know the tail) → fall through to the journal.
        let start = match range.start_bound() {
            Bound::Included(&s) => Some(s),
            Bound::Excluded(&s) => Some(s + 1),
            Bound::Unbounded => None,
        };
        let end = match range.end_bound() {
            Bound::Included(&e) => Some(e + 1),
            Bound::Excluded(&e) => Some(e),
            Bound::Unbounded => None,
        };
        if let (Some(start), Some(end)) = (start, end) && let Some(entries) = self.cache.get_range(start, end) {
            return Ok(entries);
        }
        // Cache miss or unbounded range → read from the journal.
        let iter = self.journal.iter_range(range).map_err(journal_io)?;
        let mut entries = Vec::new();
        for record in iter {
            let (_seq, _meta, payload) = record.map_err(journal_io)?;
            let (entry, _) = bincode::serde::decode_from_slice::<
                <TypeConfig as openraft::RaftTypeConfig>::Entry,
                _,
            >(&payload, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Bounded replication read. openraft's default returns the whole unbounded
    /// `[start, end)`; we cap it to `LIMITED_GET_MAX_ENTRIES` so a lagging
    /// follower cannot force an unbounded catch-up read (the ~15k-ceiling cause).
    /// A short return is contractually fine — openraft advances in chunks. The
    /// capped range is near-tail and small, so it is usually served straight from
    /// the entry cache (`try_get_log_entries`), never touching the journal.
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<<TypeConfig as openraft::RaftTypeConfig>::Entry>, io::Error> {
        let capped_end = end.min(start.saturating_add(LIMITED_GET_MAX_ENTRIES));
        self.try_get_log_entries(start..capped_end).await
    }

    async fn read_vote(&mut self) -> Result<Option<RaftVote>, io::Error> {
        self.vote.load().map_err(sv_io)
    }
}

impl RaftLogStorage<TypeConfig> for JournalLogStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let last_seq = self.journal.last_seq();
        let last_purged_log_id = self.last_purged.load().map_err(sv_io)?;

        let last_log_id = if let Some(seq) = last_seq {
            let rec = self
                .journal
                .read(seq)
                .map_err(journal_io)?
                .ok_or_else(|| io::Error::other(format!("missing last record at seq {seq}")))?;
            let (_term, payload) = rec;
            // M2 Task 1 audit (confirmed unchanged in openraft 0.10):
            //
            // openraft's default leader_id mode is `leader_id_adv` (selected by
            // `#[cfg(not(feature = "single-term-leader"))]` at
            // src/vote/leader_id/mod.rs). Our workspace does not enable
            // `single-term-leader` — the adv mode is in use.
            //
            // In adv mode (src/vote/leader_id/leader_id_adv.rs), now in
            // openraft 0.10 with `Term` and `NID` as separate type params:
            //   pub struct LeaderId<Term, NID> { pub term: Term, pub node_id: NID }
            //   pub type CommittedLeaderId<Term, NID> = LeaderId<Term, NID>;
            // For us `Term = NID = u64`, identical field layout to 0.9 → bincode
            // bytes unchanged (the M3.5 on-disk-compat guarantee).
            //
            // `node_id` IS stored and IS part of the lexicographic (term, node_id)
            // ordering. Synthesizing `CommittedLeaderId::new(term, 0)` would give
            // wrong comparison results when the real leader's node_id != 0
            // (which is the multi-node M2 case).
            //
            // Recover the real leader_id by bincode-decoding the entry payload —
            // append() stored it via `encode_to_vec(&entry, ...)`. The journal's
            // meta(=term) is now redundant for this path; we keep it as an
            // integrity check below.
            let (entry, _) = bincode::serde::decode_from_slice::<
                <TypeConfig as openraft::RaftTypeConfig>::Entry,
                _,
            >(&payload, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            // Defensive: journal meta(term) and decoded entry term must agree.
            // `seq` (journal sequence) and `entry.log_id.index` are likewise
            // append()-coupled — divergence indicates corruption.
            debug_assert_eq!(entry.log_id.leader_id.term, _term);
            debug_assert_eq!(entry.log_id.index, seq);
            Some(entry.log_id)
        } else {
            // Empty journal: last_log_id falls back to last_purged (or None).
            last_purged_log_id
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self {
            journal: self.journal.clone(),
            vote: self.vote.clone(),
            committed: self.committed.clone(),
            last_purged: self.last_purged.clone(),
            last_applied: self.last_applied.clone(),
            snapshot_meta: self.snapshot_meta.clone(),
            output_progress: self.output_progress.clone(),
            append_lock: self.append_lock.clone(),
            cache: self.cache.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &RaftVote) -> Result<(), io::Error> {
        self.vote
            .store(vote)
            .map_err(sv_io)?
            .wait()
            .map_err(journal_io)?;
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<RaftLogId>) -> Result<(), io::Error> {
        match committed {
            Some(id) => {
                self.committed
                    .store(&id)
                    .map_err(sv_io)?
                    .wait()
                    .map_err(journal_io)?;
            }
            None => {
                self.committed
                    .clear()
                    .map_err(sv_io)?
                    .wait()
                    .map_err(journal_io)?;
            }
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<RaftLogId>, io::Error> {
        self.committed.load().map_err(sv_io)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = <TypeConfig as openraft::RaftTypeConfig>::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let _guard = self.append_lock.lock().unwrap();

        let mut last_notifier: Option<ultima_journal::Notifier> = None;
        let mut probe_last_seq: Option<u64> = None;

        for entry in entries {
            let payload = bincode::serde::encode_to_vec(&entry, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let term: u64 = entry.log_id.leader_id.term;
            let seq: u64 = entry.log_id.index;

            let notifier = self
                .journal
                .append(seq, term, &payload)
                .map_err(journal_io)?;
            last_notifier = Some(notifier);
            uc_protocol::probes::stamp_log(seq, uc_protocol::probes::Checkpoint::JournalAppended);
            probe_last_seq = Some(seq);
            // Populate the cache after the journal write succeeds. `entry` is still
            // owned here (encode_to_vec borrows; term/seq are field copies). Move it in.
            self.cache.append_entry(seq, entry, payload.len());
        }

        if let (Some(notifier), Some(probe_seq)) = (last_notifier, probe_last_seq) {
            // Chain IOFlushed completion onto the final entry's Notifier. The
            // Notifier resolves at the durability boundary for the configured mode:
            // in `Consistent` after the bg writer's sync_all(); in `Eventual` after
            // the buffered page-cache write, before the background fsync (durability
            // then comes from quorum replication). Zero thread hop either way.
            // IOFlushed::io_completed takes `Result<(), io::Error>`, so we map
            // JournalError → io::Error here.
            notifier.on_complete(move |result| {
                uc_protocol::probes::stamp_log(
                    probe_seq,
                    uc_protocol::probes::Checkpoint::JournalDurable,
                );
                let io_result: Result<(), io::Error> = result.map_err(io::Error::other);
                callback.io_completed(io_result);
            });
        } else {
            // No entries → fire the callback immediately as Ok.
            callback.io_completed(Ok(()));
        }

        Ok(())
    }

    async fn truncate_after(&mut self, log_id: Option<RaftLogId>) -> Result<(), io::Error> {
        // Remove entries with index > log_id.index (i.e., keep entries up to and
        // including log_id.index). `Journal::truncate_after(keep_seq)` retains
        // records with seq <= keep_seq.
        // When log_id is None, truncate everything: pass 0 so the journal drops
        // all records (keep_seq=0 means "keep nothing with seq > 0", and since
        // seq starts at 1, that effectively clears the log).
        let keep_seq = match log_id {
            Some(id) => id.index,
            None => 0,
        };
        self.journal
            .truncate_after(keep_seq)
            .map_err(journal_io)?
            .wait()
            .map_err(journal_io)?;
        // The journal and cache are updated under separate locks (the journal's
        // internal writer lock, then the cache's RwLock), not as a single atomic
        // operation — spec §4 wording notwithstanding. This is safe because Raft
        // only ever truncates uncommitted tail entries, and those entries are never
        // concurrently read for apply or replication. Confirmed by the lincheck +
        // partition suite (Task 3, cache ON).
        self.cache.truncate_after(keep_seq);
        Ok(())
    }

    async fn purge(&mut self, log_id: RaftLogId) -> Result<(), io::Error> {
        // openraft's `purge(log_id)` contract is "remove records with index <=
        // log_id.index, retain records with index > log_id.index".
        // `Journal::purge_before(N)` retains records with seq > N. So with
        // seq == index, the boundary is `log_id.index` — NOT `log_id.index + 1`
        // (which would also discard the record at log_id.index + 1, the first
        // record raft expects to retain).
        self.journal
            .purge_before(log_id.index)
            .map_err(journal_io)?;
        self.last_purged
            .store(&log_id)
            .map_err(sv_io)?
            .wait()
            .map_err(journal_io)?;
        // Evict entries with seq <= log_id.index (mirrors journal's purge_before semantics).
        self.cache.purge_upto(log_id.index);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use openraft::storage::RaftLogStorage as _;
    use openraft::storage::RaftLogStorageExt as _;
    use openraft::vote::RaftLeaderId as _;
    use openraft::RaftLogReader as _;
    use openraft::{EntryPayload, LogId};
    use tempfile::TempDir;

    use super::{parse_journal_prealloc, parse_prealloc_fill, JournalLogStorage};
    use crate::raft::entry_cache::Entry;
    use crate::raft::AppCommand;
    use crate::raft::LeaderId;
    use crate::raft::RaftLogId;

    // -----------------------------------------------------------------------
    // Helpers for cache differential tests
    // -----------------------------------------------------------------------

    async fn new_test_storage(budget: usize) -> (JournalLogStorage, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = JournalLogStorage::open(dir.path())
            .expect("open")
            ._with_cache_budget(budget);
        (store, dir)
    }

    /// Build a distinct per-index payload for use in Normal entries.
    fn cmd_for_index(i: u64) -> AppCommand {
        AppCommand(Bytes::from(format!("cmd-{i}")))
    }

    /// Append `count` Normal entries (distinct payload per index) starting at `start`.
    /// Returns the cloned Vec so callers can compare against returned entries.
    async fn append_n(store: &mut JournalLogStorage, start: u64, count: u64) -> Vec<Entry> {
        let entries: Vec<Entry> = (start..start + count)
            .map(|i| Entry {
                log_id: LogId::new(LeaderId::new(1, 1), i),
                payload: EntryPayload::Normal(cmd_for_index(i)),
            })
            .collect();
        let result = entries.clone();
        store.blocking_append(entries).await.expect("append_n");
        result
    }

    /// Extract (log_index, payload_bytes) for full-entry comparison.
    /// A divergent cached payload is a linearizability violation — this helper
    /// catches it where comparing indexes alone would not.
    fn entry_key(e: &Entry) -> (u64, Bytes) {
        let payload_bytes = match &e.payload {
            EntryPayload::Normal(cmd) => cmd.0.clone(),
            _ => Bytes::new(),
        };
        (e.log_id.index, payload_bytes)
    }

    fn log_id_at(index: u64) -> RaftLogId {
        LogId::new(LeaderId::new(1, 1), index)
    }

    // -----------------------------------------------------------------------
    // Differential test: cache-served entries == journal-served entries
    // -----------------------------------------------------------------------

    /// For every query range and both cache budgets (0=disabled, 64MB=enabled),
    /// `try_get_log_entries` must return byte-identical entries (index AND payload).
    /// In the enabled arm, also asserts the cache actually served the in-window
    /// reads — so a silent population regression causes the test to fail rather
    /// than fall through to the journal and pass spuriously.
    #[tokio::test]
    async fn cache_reads_match_journal() {
        for budget in [0usize, 64 * 1024 * 1024] {
            let (mut store, _dir) = new_test_storage(budget).await;
            append_n(&mut store, 1, 20).await; // indexes 1..=20

            for (lo, hi) in [(1u64, 21u64), (5, 15), (18, 21), (1, 2)] {
                let got = store.try_get_log_entries(lo..hi).await.unwrap();
                let expected: Vec<_> = (lo..hi)
                    .map(|i| (i, Bytes::from(format!("cmd-{i}"))))
                    .collect();
                assert_eq!(
                    got.iter().map(entry_key).collect::<Vec<_>>(),
                    expected,
                    "budget={budget} range={lo}..{hi}"
                );
            }

            // Entries 1..=20 are fully within the 64 MB budget; every in-window
            // read above must have been served by the cache, not the journal.
            if budget > 0 {
                assert!(
                    store.cache.hits() > 0,
                    "cache never hit — population regressed (budget={budget})"
                );
            }

            store.purge(log_id_at(5)).await.unwrap(); // remove <=5
            let got = store.try_get_log_entries(6u64..21).await.unwrap();
            let expected: Vec<_> = (6u64..21)
                .map(|i| (i, Bytes::from(format!("cmd-{i}"))))
                .collect();
            assert_eq!(
                got.iter().map(entry_key).collect::<Vec<_>>(),
                expected,
                "budget={budget} after purge"
            );
        }
    }

    #[test]
    fn prealloc_unset_is_on() {
        // task36 promotion: default ON when the env var is absent.
        assert!(parse_journal_prealloc(None));
    }

    #[test]
    fn prealloc_zero_and_false_are_off() {
        // Explicit rollback values.
        assert!(!parse_journal_prealloc(Some("0")));
        assert!(!parse_journal_prealloc(Some("false")));
    }

    #[test]
    fn prealloc_one_true_and_garbage_are_on() {
        assert!(parse_journal_prealloc(Some("1")));
        assert!(parse_journal_prealloc(Some("true")));
        // Fail-open to the new default for anything that isn't an explicit disable.
        assert!(parse_journal_prealloc(Some("bad")));
    }

    #[test]
    fn parse_prealloc_fill_maps_values() {
        use ultima_journal::PreallocFill;
        assert_eq!(parse_prealloc_fill(Some("paced")), PreallocFill::ZeroWritePaced);
        assert_eq!(parse_prealloc_fill(Some("fallocate")), PreallocFill::FallocateZeroRange);
        assert_eq!(parse_prealloc_fill(Some("full")), PreallocFill::ZeroWriteFull);
        // Default (unset/unknown) is now the A/B winner FallocateZeroRange.
        assert_eq!(parse_prealloc_fill(None), PreallocFill::FallocateZeroRange);
        assert_eq!(parse_prealloc_fill(Some("garbage")), PreallocFill::FallocateZeroRange);
    }
}
