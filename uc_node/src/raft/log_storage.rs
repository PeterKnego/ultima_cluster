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
use ultima_journal::{Durability, Journal, JournalConfig, StableValue, StableValueConfig};

use super::NodeId;
use crate::ClusterError;

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
    pub fn _testonly_vote(&self) -> &StableValue<Vote<NodeId>> {
        &self.vote
    }
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_committed(&self) -> &StableValue<LogId<NodeId>> {
        &self.committed
    }
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn _testonly_last_purged(&self) -> &StableValue<LogId<NodeId>> {
        &self.last_purged
    }
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

use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::storage::LogFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;

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

fn map_journal_read_logs(e: ultima_journal::JournalError) -> StorageError<NodeId> {
    StorageIOError::read_logs(&e).into()
}

impl RaftLogReader<TypeConfig> for JournalLogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, StorageError<NodeId>> {
        let iter = self
            .journal
            .iter_range(range)
            .map_err(map_journal_read_logs)?;
        let mut entries = Vec::new();
        for record in iter {
            let (_seq, _meta, payload) = record.map_err(map_journal_read_logs)?;
            let (entry, _) = bincode::serde::decode_from_slice::<openraft::Entry<TypeConfig>, _>(
                &payload,
                bincode::config::standard(),
            )
            .map_err(|e| {
                let io_err = std::io::Error::other(e.to_string());
                StorageIOError::<NodeId>::read_logs(&io_err)
            })?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for JournalLogStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_seq = self.journal.last_seq();
        let last_purged_log_id = self.last_purged.load().map_err(map_sv_read_logs)?;

        let last_log_id = if let Some(seq) = last_seq {
            let rec = self
                .journal
                .read(seq)
                .map_err(map_journal_read_logs)?
                .ok_or_else(|| {
                    let io_err = std::io::Error::other(format!("missing last record at seq {seq}"));
                    StorageIOError::<NodeId>::read_logs(&io_err)
                })?;
            let (_term, payload) = rec;
            // M2 Task 1 audit (openraft 0.9.24):
            //
            // openraft's default leader_id mode is `leader_id_adv` (selected by
            // `#[cfg(not(feature = "single-term-leader"))]` at
            // src/vote/leader_id/mod.rs). Our workspace enables openraft features
            // = ["serde", "storage-v2"] only — `single-term-leader` is OFF — so
            // the adv mode is in use.
            //
            // In adv mode (src/vote/leader_id/leader_id_adv.rs):
            //   pub struct LeaderId<NID> { pub term: u64, pub node_id: NID }
            //   #[derive(PartialOrd, Ord)]
            //   pub type CommittedLeaderId<NID> = LeaderId<NID>;
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
            let (entry, _) = bincode::serde::decode_from_slice::<openraft::Entry<TypeConfig>, _>(
                &payload,
                bincode::config::standard(),
            )
            .map_err(|e| {
                let io_err = std::io::Error::other(e.to_string());
                StorageIOError::<NodeId>::read_logs(&io_err)
            })?;
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
            append_lock: self.append_lock.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
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

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
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
        let _guard = self.append_lock.lock().unwrap();

        let mut last_notifier: Option<ultima_journal::Notifier> = None;

        for entry in entries {
            let payload = bincode::serde::encode_to_vec(&entry, bincode::config::standard())
                .map_err(|e| {
                    let io_err = std::io::Error::other(e.to_string());
                    StorageIOError::<NodeId>::write_logs(&io_err)
                })?;

            let term: u64 = entry.log_id.leader_id.term;
            let seq: u64 = entry.log_id.index;

            let notifier = self
                .journal
                .append(seq, term, &payload)
                .map_err(map_journal_write_logs)?;
            last_notifier = Some(notifier);
        }

        if let Some(notifier) = last_notifier {
            // Chain LogFlushed completion onto the final entry's Notifier.
            // ultima_journal's bg fsync thread calls our callback after sync_all() —
            // zero thread hop. openraft 0.9.24's `LogFlushed::log_io_completed` takes
            // `Result<(), io::Error>` (NOT `Result<(), StorageIOError<NodeId>>` as the
            // original spec hinted), so we map JournalError → io::Error here.
            notifier.on_complete(move |result| {
                let io_result: Result<(), std::io::Error> =
                    result.map_err(|e| std::io::Error::other(e.to_string()));
                callback.log_io_completed(io_result);
            });
        } else {
            // No entries → fire the callback immediately as Ok.
            callback.log_io_completed(Ok(()));
        }

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Remove entries with index >= log_id.index (i.e., keep entries with
        // index < log_id.index). `truncate_after(keep_seq)` retains records with
        // seq <= keep_seq. For log_id.index == 0, keep_seq saturates at 0 which
        // (per Journal::truncate_after's docs) drops every record with seq > 0.
        let keep_seq = log_id.index.saturating_sub(1);
        self.journal
            .truncate_after(keep_seq)
            .map_err(map_journal_write_logs)?
            .wait()
            .map_err(map_journal_write_logs)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // openraft's `purge(log_id)` contract is "remove records with index <=
        // log_id.index, retain records with index > log_id.index".
        // `Journal::purge_before(N)` retains records with seq > N. So with
        // seq == index, the boundary is `log_id.index` — NOT `log_id.index + 1`
        // (which would also discard the record at log_id.index + 1, the first
        // record raft expects to retain).
        self.journal
            .purge_before(log_id.index)
            .map_err(map_journal_write_logs)?;
        self.last_purged
            .store(&log_id)
            .map_err(map_sv_write_logs)?
            .wait()
            .map_err(map_journal_write_logs)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// M1-only no-op network. Replaced by QuicRaftNetwork in M2.
//
// Lives under `test_helpers` for naming continuity with the original spec, but
// is always compiled (the runtime builder uses it as the concrete network in
// single-node mode). All RaftNetwork methods are `unreachable!()` — single-node
// raft never sends RPCs to peers.
// ---------------------------------------------------------------------------

pub mod test_helpers {
    //! M1-only no-op RaftNetwork. Replaced by QuicRaftNetwork in M2.
    use openraft::error::{InstallSnapshotError, RPCError, RaftError};
    use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    };

    use crate::raft::{NodeAddr, NodeId, TypeConfig};

    pub struct NoopNetwork;

    impl RaftNetworkFactory<TypeConfig> for NoopNetwork {
        type Network = NoopNetworkInstance;
        async fn new_client(&mut self, _target: NodeId, _node: &NodeAddr) -> Self::Network {
            NoopNetworkInstance
        }
    }

    pub struct NoopNetworkInstance;

    impl RaftNetwork<TypeConfig> for NoopNetworkInstance {
        async fn append_entries(
            &mut self,
            _rpc: AppendEntriesRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NodeAddr, RaftError<NodeId>>>
        {
            unreachable!("M1 single-node — no network calls")
        }

        async fn install_snapshot(
            &mut self,
            _rpc: InstallSnapshotRequest<TypeConfig>,
            _option: RPCOption,
        ) -> Result<
            InstallSnapshotResponse<NodeId>,
            RPCError<NodeId, NodeAddr, RaftError<NodeId, InstallSnapshotError>>,
        > {
            unreachable!("M1 single-node — no network calls")
        }

        async fn vote(
            &mut self,
            _rpc: VoteRequest<NodeId>,
            _option: RPCOption,
        ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NodeAddr, RaftError<NodeId>>> {
            unreachable!("M1 single-node — no network calls")
        }
    }
}
