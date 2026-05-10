//! Verifies that JournalLogStorage::open is idempotent and survives restart
//! with empty state.

use openraft::storage::RaftLogStorage as _;
use openraft::Vote;
use tempfile::TempDir;
use uc_node::raft::log_storage::JournalLogStorage;

#[test]
fn reopen_observes_empty_state() {
    let dir = TempDir::new().unwrap();
    let storage = JournalLogStorage::open(dir.path()).expect("open");
    drop(storage);

    // Reopen — must succeed and observe the same empty state.
    let storage = JournalLogStorage::open(dir.path()).expect("reopen");
    assert!(storage._testonly_vote().load().expect("load vote").is_none());
    assert!(storage._testonly_committed().load().expect("load committed").is_none());
    assert!(storage._testonly_last_purged().load().expect("load last_purged").is_none());
}

#[tokio::test]
async fn save_and_read_vote_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");

    let v: Vote<u64> = Vote::new(7, 3);
    storage.save_vote(&v).await.expect("save");

    let loaded = storage.read_vote().await.expect("read");
    assert_eq!(loaded, Some(v));

    drop(storage);
    let mut storage = JournalLogStorage::open(dir.path()).expect("reopen");
    let loaded = storage.read_vote().await.expect("read after reopen");
    assert_eq!(loaded, Some(v));
}
