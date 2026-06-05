//! Eventual-log durability: recovery clamp, durability-lag, mode behavior.

use openraft::storage::RaftLogStorage as _;
use openraft::storage::RaftLogStorageExt as _;
use openraft::vote::RaftLeaderId as _;
use openraft::{Entry, EntryPayload};
use tempfile::TempDir;
use uc_node::raft::log_storage::JournalLogStorage;

type LeaderId = openraft::impls::leader_id_adv::LeaderId<u64, u64>;
type RaftLogId = openraft::LogId<LeaderId>;
type RaftEntry = Entry<LeaderId, uc_node::raft::AppCommand, u64, uc_node::raft::NodeAddr>;

fn make_log_id(term: u64, node_id: u64, index: u64) -> RaftLogId {
    openraft::LogId::new(LeaderId::new(term, node_id), index)
}

async fn append_1_to(storage: &mut JournalLogStorage, n: u64) {
    let entries: Vec<RaftEntry> = (1..=n)
        .map(|i| Entry {
            log_id: make_log_id(1, 0, i),
            payload: EntryPayload::Normal(uc_node::raft::AppCommand(bytes::Bytes::from(
                format!("cmd-{i}"),
            ))),
        })
        .collect();
    storage.blocking_append(entries).await.expect("append");
}

#[tokio::test]
async fn reconcile_clamps_committed_ahead_of_log() {
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");

    // Log 1..=5; committed = index 5.
    append_1_to(&mut storage, 5).await;
    storage
        .save_committed(Some(make_log_id(1, 0, 5)))
        .await
        .expect("save_committed");

    // Simulate power-loss tail loss: drop entries 4,5 from the log while
    // `committed` (fsynced) stays at 5. truncate_after keeps index <= 3.
    storage
        .truncate_after(Some(make_log_id(1, 0, 3)))
        .await
        .expect("truncate");

    // Inversion now present: committed.index (5) > last_seq (3).
    assert_eq!(storage.read_committed().await.unwrap(), Some(make_log_id(1, 0, 5)));

    // Reconcile clamps committed down to the durable tail (index 3).
    uc_node::runtime::recovery::reconcile(&storage).expect("reconcile");
    assert_eq!(storage.read_committed().await.unwrap(), Some(make_log_id(1, 0, 3)));
}

#[tokio::test]
async fn reconcile_leaves_consistent_committed_untouched() {
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");
    append_1_to(&mut storage, 5).await;
    storage
        .save_committed(Some(make_log_id(1, 0, 3)))
        .await
        .expect("save_committed");

    // committed.index (3) <= last_seq (5): no clamp.
    uc_node::runtime::recovery::reconcile(&storage).expect("reconcile");
    assert_eq!(storage.read_committed().await.unwrap(), Some(make_log_id(1, 0, 3)));
}
