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
