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
use crate::ClusterError;

const SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;

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

impl RaftLogReader<TypeConfig> for JournalLogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<TypeConfig as openraft::RaftTypeConfig>::Entry>, io::Error> {
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
        Ok(())
    }
}
