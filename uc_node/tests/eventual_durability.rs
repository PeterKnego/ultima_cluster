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
            payload: EntryPayload::Normal(uc_node::raft::AppCommand(bytes::Bytes::from(format!(
                "cmd-{i}"
            )))),
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
    assert_eq!(
        storage.read_committed().await.unwrap(),
        Some(make_log_id(1, 0, 5))
    );

    // Reconcile clamps committed down to the durable tail (index 3).
    uc_node::runtime::recovery::reconcile(&storage).expect("reconcile");
    assert_eq!(
        storage.read_committed().await.unwrap(),
        Some(make_log_id(1, 0, 3))
    );
}

#[tokio::test]
async fn reconcile_clamps_output_progress_ahead_of_last_applied() {
    let dir = TempDir::new().unwrap();
    let storage = JournalLogStorage::open(dir.path()).expect("open");

    // Shmem-lag scenario: output_progress fsynced per-output up to 5, but the
    // durable last_applied only advances at snapshot time → still 0 here. This
    // is NOT corruption: the applied entries are in the log and openraft will
    // re-apply them on startup.
    storage
        ._testonly_output_progress()
        .store(&5)
        .expect("store output_progress")
        .wait()
        .expect("wait");
    assert_eq!(storage._testonly_output_progress().load().unwrap(), Some(5));
    assert_eq!(storage._testonly_last_applied().load().unwrap(), None); // index 0

    // Reconcile must NOT error; it clamps output_progress down to last_applied
    // (0) so outputs in the gap re-run on replay (at-least-once).
    uc_node::runtime::recovery::reconcile(&storage).expect("reconcile");
    assert_eq!(storage._testonly_output_progress().load().unwrap(), Some(0));
}

#[tokio::test]
async fn reconcile_leaves_output_progress_below_last_applied_untouched() {
    let dir = TempDir::new().unwrap();
    let storage = JournalLogStorage::open(dir.path()).expect("open");

    // Normal steady state: output_progress (3) trails the durable last_applied (5).
    storage
        ._testonly_last_applied()
        .store(&make_log_id(1, 0, 5))
        .expect("store last_applied")
        .wait()
        .expect("wait");
    storage
        ._testonly_output_progress()
        .store(&3)
        .expect("store output_progress")
        .wait()
        .expect("wait");

    uc_node::runtime::recovery::reconcile(&storage).expect("reconcile");
    assert_eq!(storage._testonly_output_progress().load().unwrap(), Some(3)); // unchanged
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
    assert_eq!(
        storage.read_committed().await.unwrap(),
        Some(make_log_id(1, 0, 3))
    );
}

#[tokio::test]
async fn reconcile_preserves_committed_at_snapshot_floor_with_empty_log() {
    // Fresh follower after install_snapshot: no log entries (last_seq None),
    // last_purged + committed both at the snapshot's last log id. reconcile must
    // NOT clear committed — it is durable via the snapshot.
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");
    let snap = make_log_id(2, 0, 100);
    storage
        .save_committed(Some(snap))
        .await
        .expect("save_committed");
    storage.purge(snap).await.expect("purge"); // last_purged = snap, log empty
    assert_eq!(storage.read_committed().await.unwrap(), Some(snap));

    uc_node::runtime::recovery::reconcile(&storage).expect("reconcile");
    assert_eq!(storage.read_committed().await.unwrap(), Some(snap)); // unchanged
}

#[tokio::test]
async fn reconcile_clamps_committed_to_purge_floor_when_log_empty() {
    // committed fsynced AHEAD of both the (empty) log and the snapshot floor →
    // clamp down to last_purged, NOT clear.
    let dir = TempDir::new().unwrap();
    let mut storage = JournalLogStorage::open(dir.path()).expect("open");
    storage.purge(make_log_id(2, 0, 100)).await.expect("purge"); // last_purged = 100, log empty
    storage
        .save_committed(Some(make_log_id(2, 0, 150)))
        .await
        .expect("save_committed");

    uc_node::runtime::recovery::reconcile(&storage).expect("reconcile");
    assert_eq!(
        storage.read_committed().await.unwrap(),
        Some(make_log_id(2, 0, 100))
    );
}

#[tokio::test]
async fn consistent_mode_has_zero_durability_lag() {
    use ultima_journal::Durability;
    let dir = TempDir::new().unwrap();
    let mut storage =
        JournalLogStorage::open_with_durability(dir.path(), Durability::Consistent).expect("open");
    append_1_to(&mut storage, 3).await;
    // Consistent fsyncs before ack, so the durable watermark == last_seq.
    assert_eq!(storage.durability_lag(), 0);
}

#[tokio::test]
async fn eventual_mode_durability_lag_drains_to_zero() {
    use ultima_journal::Durability;
    let dir = TempDir::new().unwrap();
    let mut storage =
        JournalLogStorage::open_with_durability(dir.path(), Durability::Eventual).expect("open");
    append_1_to(&mut storage, 3).await;
    // The background idle-fsync eventually flushes; lag drains to 0. Spin with a
    // bound (mirrors ultima_journal's own watermark tests).
    let mut ok = false;
    for _ in 0..200 {
        if storage.durability_lag() == 0 {
            ok = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(ok, "durability_lag never drained to 0");
}

#[tokio::test]
async fn measure_append_ack_latency_by_mode() {
    use std::time::Instant;
    use ultima_journal::Durability;

    async fn median_ack_us(durability: Durability) -> u128 {
        let dir = TempDir::new().unwrap();
        let mut storage =
            JournalLogStorage::open_with_durability(dir.path(), durability).expect("open");
        let mut samples = Vec::new();
        for i in 1..=200u64 {
            let e = vec![RaftEntry {
                log_id: make_log_id(1, 0, i),
                payload: EntryPayload::Normal(uc_node::raft::AppCommand(bytes::Bytes::from(
                    vec![0xABu8; 256],
                ))),
            }];
            let t = Instant::now();
            storage.blocking_append(e).await.expect("append");
            samples.push(t.elapsed().as_micros());
        }
        samples.sort_unstable();
        samples[samples.len() / 2]
    }

    let consistent = median_ack_us(Durability::Consistent).await;
    let eventual = median_ack_us(Durability::Eventual).await;
    println!(
        "append-ack median µs: Consistent={consistent} Eventual={eventual} \
         (storage-dependent; fsync cost dominates on real disk)"
    );
}
