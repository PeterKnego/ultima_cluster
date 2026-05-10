//! Verifies that JournalLogStorage::open is idempotent and survives restart
//! with empty state.

use openraft::storage::RaftLogStorage as _;
use openraft::storage::RaftLogStorageExt as _;
use openraft::RaftLogReader as _;
use openraft::{Entry, EntryPayload, Vote};
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

#[tokio::test]
async fn append_then_read_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");

    // Build 3 simple entries.
    let entries: Vec<Entry<uc_node::raft::TypeConfig>> = (1..=3u64)
        .map(|i| Entry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), i),
            payload: EntryPayload::Normal(bytes::Bytes::from(format!("cmd-{i}"))),
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
            EntryPayload::Normal(bytes) => {
                assert_eq!(bytes.as_ref(), format!("cmd-{}", i + 1).as_bytes());
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
    assert!(storage
        .read_committed()
        .await
        .expect("read empty")
        .is_none());

    // Save a LogId, read back.
    let lid = openraft::LogId::new(openraft::CommittedLeaderId::new(5, 0), 42);
    storage.save_committed(Some(lid)).await.expect("save");
    assert_eq!(
        storage.read_committed().await.expect("read"),
        Some(lid)
    );

    // Survives reopen.
    drop(storage);
    let mut storage = JournalLogStorage::open(dir.path()).expect("reopen");
    assert_eq!(
        storage.read_committed().await.expect("read after reopen"),
        Some(lid)
    );

    // Clear, read None, survive reopen.
    storage.save_committed(None).await.expect("clear");
    assert!(storage
        .read_committed()
        .await
        .expect("read after clear")
        .is_none());

    drop(storage);
    let mut storage = JournalLogStorage::open(dir.path()).expect("reopen 2");
    assert!(storage
        .read_committed()
        .await
        .expect("read after reopen 2")
        .is_none());
}
