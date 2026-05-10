//! Verifies that JournalLogStorage::open is idempotent and survives restart
//! with empty state.

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
