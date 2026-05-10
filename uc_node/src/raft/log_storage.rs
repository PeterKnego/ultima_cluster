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
use std::sync::Arc;
use std::sync::Mutex;

use openraft::{LogId, Vote};
use ultima_journal::{
    Durability, Journal, JournalConfig, StableValue, StableValueConfig,
};

use crate::ClusterError;
use super::NodeId;

const SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;

pub struct JournalLogStorage {
    pub(crate) journal: Arc<Journal>,
    pub(crate) vote: Arc<StableValue<Vote<NodeId>>>,
    pub(crate) committed: Arc<StableValue<LogId<NodeId>>>,
    pub(crate) last_purged: Arc<StableValue<LogId<NodeId>>>,
    /// Serializes seq assignment per the journal's caller-coordination requirement.
    /// openraft already serializes appends, so this is a no-contention guarantee.
    pub(crate) append_lock: Arc<Mutex<()>>,
}

impl JournalLogStorage {
    pub fn open(data_dir: &Path) -> Result<Self, ClusterError> {
        std::fs::create_dir_all(data_dir.join("journal"))?;

        let journal = Arc::new(Journal::open(JournalConfig {
            dir: data_dir.join("journal"),
            segment_size_bytes: SEGMENT_SIZE_BYTES,
            durability: Durability::Consistent,
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

        Ok(Self {
            journal,
            vote,
            committed,
            last_purged,
            append_lock: Arc::new(Mutex::new(())),
        })
    }

    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_vote(&self) -> &StableValue<Vote<NodeId>> { &self.vote }
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_committed(&self) -> &StableValue<LogId<NodeId>> { &self.committed }
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_last_purged(&self) -> &StableValue<LogId<NodeId>> { &self.last_purged }
}

// ---------------------------------------------------------------------------
// RaftLogReader / RaftLogStorage impls.
//
// Task 9 implements: save_vote / read_vote / save_committed / read_committed /
// get_log_reader. Task 10 implements: append / truncate / purge / get_log_state /
// try_get_log_entries (left as `unimplemented!("Task 10")` stubs here).
//
// openraft 0.9.24 notes (deviations from the original spec):
//   * The append callback type is `LogFlushed<C>` (re-exported from
//     `openraft::storage`), not `IOFlushed`.
//   * Error mapping uses `StorageIOError::{write_vote, read_vote, write_logs,
//     read_logs}` rather than the spec's hypothetical `StorageError::write` —
//     `StorageError<NID>` only has the `Defensive`/`IO` variants and relies on
//     `From<StorageIOError<NID>>` for construction.
//   * The trait is declared with `#[add_async_trait]`, which expands to bare
//     native async (no `#[async_trait]` attribute on the impl) when the
//     `singlethreaded` feature is off — matching openraft's own `Adaptor` impl.
//   * The `RangeBounds` bound uses `std::fmt::Debug`, not `OptionalSend`-marked
//     `Debug`.
// ---------------------------------------------------------------------------

use std::fmt::Debug;
use std::ops::RangeBounds;

use openraft::storage::LogFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::StorageIOError;

use super::TypeConfig;

fn map_sv_write_vote(e: ultima_journal::StableValueError) -> StorageError<NodeId> {
    StorageIOError::write_vote(&e).into()
}

fn map_sv_read_vote(e: ultima_journal::StableValueError) -> StorageError<NodeId> {
    StorageIOError::read_vote(&e).into()
}

fn map_sv_write_logs(e: ultima_journal::StableValueError) -> StorageError<NodeId> {
    StorageIOError::write_logs(&e).into()
}

fn map_sv_read_logs(e: ultima_journal::StableValueError) -> StorageError<NodeId> {
    StorageIOError::read_logs(&e).into()
}

fn map_journal_write_vote(e: ultima_journal::JournalError) -> StorageError<NodeId> {
    StorageIOError::write_vote(&e).into()
}

fn map_journal_write_logs(e: ultima_journal::JournalError) -> StorageError<NodeId> {
    StorageIOError::write_logs(&e).into()
}

impl RaftLogReader<TypeConfig> for JournalLogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, StorageError<NodeId>> {
        // Implemented in Task 10.
        let _ = range;
        unimplemented!("Task 10")
    }
}

impl RaftLogStorage<TypeConfig> for JournalLogStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        // Implemented in Task 10.
        unimplemented!("Task 10")
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self {
            journal: self.journal.clone(),
            vote: self.vote.clone(),
            committed: self.committed.clone(),
            last_purged: self.last_purged.clone(),
            append_lock: self.append_lock.clone(),
        }
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        self.vote
            .store(vote)
            .map_err(map_sv_write_vote)?
            .wait()
            .map_err(map_journal_write_vote)?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.vote.load().map_err(map_sv_read_vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        match committed {
            Some(id) => {
                self.committed
                    .store(&id)
                    .map_err(map_sv_write_logs)?
                    .wait()
                    .map_err(map_journal_write_logs)?;
            }
            None => {
                self.committed
                    .clear()
                    .map_err(map_sv_write_logs)?
                    .wait()
                    .map_err(map_journal_write_logs)?;
            }
        }
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        self.committed.load().map_err(map_sv_read_logs)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + OptionalSend,
    {
        // Implemented in Task 10.
        let _ = (entries, callback);
        unimplemented!("Task 10")
    }

    async fn truncate(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        // Implemented in Task 10.
        let _ = log_id;
        unimplemented!("Task 10")
    }

    async fn purge(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        // Implemented in Task 10.
        let _ = log_id;
        unimplemented!("Task 10")
    }
}
