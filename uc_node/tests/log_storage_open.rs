//! Verifies that JournalLogStorage::open is idempotent and survives restart
//! with empty state.

use openraft::RaftLogReader as _;
use openraft::storage::RaftLogStorage as _;
use openraft::storage::RaftLogStorageExt as _;
use openraft::vote::RaftLeaderId as _;
use openraft::{Entry, EntryPayload, Vote};
use tempfile::TempDir;
use uc_node::raft::log_storage::{JournalLogStorage, StoredSnapshotMeta};

// Type aliases matching log_storage.rs internal types.
type LeaderId = openraft::impls::leader_id_adv::LeaderId<u64, u64>;
type RaftLogId = openraft::LogId<LeaderId>;
type RaftVote = Vote<LeaderId>;
type RaftEntry = Entry<LeaderId, uc_node::raft::AppCommand, u64, uc_node::raft::NodeAddr>;

fn make_log_id(term: u64, node_id: u64, index: u64) -> RaftLogId {
    openraft::LogId::new(LeaderId::new(term, node_id), index)
}

#[test]
fn reopen_observes_empty_state() {
    let dir = TempDir::new().unwrap();
    let storage = JournalLogStorage::open(dir.path()).expect("open");
    drop(storage);

    // Reopen — must succeed and observe the same empty state.
    let storage = JournalLogStorage::open(dir.path()).expect("reopen");
    assert!(
        storage
            ._testonly_vote()
            .load()
            .expect("load vote")
            .is_none()
    );
    assert!(
        storage
            ._testonly_committed()
            .load()
            .expect("load committed")
            .is_none()
    );
    assert!(
        storage
            ._testonly_last_purged()
            .load()
            .expect("load last_purged")
            .is_none()
    );
}

#[tokio::test]
async fn save_and_read_vote_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");

    let v: RaftVote = Vote::new(7, 3);
    storage.save_vote(&v).await.expect("save");

    let loaded = storage.read_vote().await.expect("read");
    assert_eq!(loaded, Some(v));

    drop(storage);
    let mut storage = JournalLogStorage::open(dir.path()).expect("reopen");
    let loaded = storage.read_vote().await.expect("read after reopen");
    assert_eq!(loaded, Some(v));
}

#[tokio::test]
async fn append_then_read_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");

    // Build 3 simple entries.
    let entries: Vec<RaftEntry> = (1..=3u64)
        .map(|i| Entry {
            log_id: make_log_id(1, 0, i),
            payload: EntryPayload::Normal(uc_node::raft::AppCommand(bytes::Bytes::from(format!(
                "cmd-{i}"
            )))),
        })
        .collect();

    // RaftLogStorageExt::blocking_append wires up an internal LogFlushed callback
    // (its constructor is `pub(crate)`, so we cannot build one ourselves) and
    // awaits its completion — this is the public test path used by openraft's own
    // `Suite` harness.
    storage.blocking_append(entries).await.expect("append");

    // Read back via the reader trait.
    let read = storage
        .try_get_log_entries(1u64..=3u64)
        .await
        .expect("read");
    assert_eq!(read.len(), 3);
    for (i, entry) in read.iter().enumerate() {
        assert_eq!(entry.log_id.index, (i as u64) + 1);
        match &entry.payload {
            EntryPayload::Normal(cmd) => {
                assert_eq!(cmd.0.as_ref(), format!("cmd-{}", i + 1).as_bytes());
            }
            _ => panic!("unexpected payload kind"),
        }
    }
}

#[tokio::test]
async fn save_and_read_committed_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");

    // Initially None.
    assert!(
        storage
            .read_committed()
            .await
            .expect("read empty")
            .is_none()
    );

    // Save a LogId, read back.
    let lid = make_log_id(5, 0, 42);
    storage.save_committed(Some(lid)).await.expect("save");
    assert_eq!(storage.read_committed().await.expect("read"), Some(lid));

    // Survives reopen.
    drop(storage);
    let mut storage = JournalLogStorage::open(dir.path()).expect("reopen");
    assert_eq!(
        storage.read_committed().await.expect("read after reopen"),
        Some(lid)
    );

    // Clear, read None, survive reopen.
    storage.save_committed(None).await.expect("clear");
    assert!(
        storage
            .read_committed()
            .await
            .expect("read after clear")
            .is_none()
    );

    drop(storage);
    let mut storage = JournalLogStorage::open(dir.path()).expect("reopen 2");
    assert!(
        storage
            .read_committed()
            .await
            .expect("read after reopen 2")
            .is_none()
    );
}

#[tokio::test]
async fn purge_retains_higher_indices() {
    // Regression for off-by-one in purge: with segment-aligned purge_before,
    // calling purge(log_id{idx=N}) MUST retain records with index > N, never
    // drop record N+1. The bug was purge_before(N+1) which would drop record
    // N+1 if it happened to be the terminal record of a segment.
    //
    // Note: M1's hardcoded 64 MiB segment size means this test cannot
    // reproduce the segment-boundary case directly; it instead verifies the
    // semantic via the public RaftLogStorage API. The bug-catching power
    // strengthens once the journal config is configurable in M2+.
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");

    // Append 5 entries.
    let entries: Vec<RaftEntry> = (1..=5u64)
        .map(|i| Entry {
            log_id: make_log_id(1, 0, i),
            payload: EntryPayload::Normal(uc_node::raft::AppCommand(bytes::Bytes::from(format!(
                "cmd-{i}"
            )))),
        })
        .collect();
    storage.blocking_append(entries).await.expect("append");

    // Purge through index 3.
    let purge_thru = make_log_id(1, 0, 3);
    storage.purge(purge_thru).await.expect("purge");

    // Records 4 and 5 must still be readable.
    let read = storage
        .try_get_log_entries(4u64..=5u64)
        .await
        .expect("read");
    assert_eq!(
        read.len(),
        2,
        "records 4 and 5 must survive purge through 3"
    );
    assert_eq!(read[0].log_id.index, 4);
    assert_eq!(read[1].log_id.index, 5);

    // get_log_state should report last_log_id at index 5.
    let state = storage.get_log_state().await.expect("get_log_state");
    assert_eq!(state.last_log_id.map(|l| l.index), Some(5));
    assert_eq!(state.last_purged_log_id.map(|l| l.index), Some(3));
}

#[tokio::test]
async fn snapshot_meta_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let storage = JournalLogStorage::open(dir.path()).expect("open");

    // Initially None.
    assert!(
        storage
            ._testonly_snapshot_meta()
            .load()
            .expect("load")
            .is_none()
    );

    // Store a snapshot meta.
    let meta = StoredSnapshotMeta {
        last_log_id: Some(make_log_id(5, 0, 100)),
        last_membership: openraft::StoredMembership::default(),
        bytes_filename: "snapshot_100.bin".into(),
    };
    storage
        ._testonly_snapshot_meta()
        .store(&meta)
        .expect("store")
        .wait()
        .expect("wait");

    // Reopen and verify.
    drop(storage);
    let storage = JournalLogStorage::open(dir.path()).expect("reopen");
    let loaded = storage
        ._testonly_snapshot_meta()
        .load()
        .expect("load after reopen");
    assert!(loaded.is_some());
    let loaded = loaded.unwrap();
    assert_eq!(loaded.bytes_filename, "snapshot_100.bin");
    assert_eq!(loaded.last_log_id.map(|l| l.index), Some(100));
}
