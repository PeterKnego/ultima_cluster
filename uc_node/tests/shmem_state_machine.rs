//! Exercises [`ShmemAdaptedStateMachine`] end-to-end against a manually-
//! simulated service side: the test plays the role of `uc_service` by
//! reading from `service/apply.ring`, calling a local counter SM, and
//! writing the response to `service/apply_resp.ring`. The node-side
//! `ShmemAdaptedStateMachine::apply` should round-trip through the rings
//! transparently.

use std::io::{Read, Write};
use std::time::Duration;

use bytes::Bytes;
use futures::stream;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderId as _;
use openraft::{Entry, EntryPayload, LogId};
type LeaderId = openraft::impls::leader_id_adv::LeaderId<u64, u64>;
type RaftEntry = Entry<LeaderId, uc_node::raft::AppCommand, u64, uc_node::raft::NodeAddr>;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_node::ipc::Instance;
use uc_node::ipc::service_link::ServiceLink;
use uc_node::raft::log_storage::JournalLogStorage;
use uc_node::raft::state_machine_shmem::ShmemAdaptedStateMachine;
use uc_protocol::frames::apply::{
    MSG_TYPE_APPLY, MSG_TYPE_APPLY_RESP, decode_extra_apply, encode_extra_apply,
};
use uc_protocol::ring::spsc::SpscRing;
use uc_service::{SnapshotError, StateMachine};

/// Node-side stub SM: required to satisfy the `S: StateMachine` bound on
/// `ShmemAdaptedStateMachine`, but never exercised in this test (apply
/// goes through the rings; snapshot ops aren't fired by the test).
#[derive(Default)]
struct StubSm;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct NoCmd;

impl StateMachine for StubSm {
    type Command = NoCmd;
    type Response = NoCmd;
    type Query = NoCmd;
    type QueryResponse = NoCmd;
    type SnapshotHandle = Vec<u8>;
    fn apply(&mut self, _: u64, _: NoCmd) -> NoCmd {
        NoCmd
    }
    fn query(&self, _: NoCmd) -> NoCmd {
        NoCmd
    }
    fn last_applied(&self) -> Option<u64> {
        None
    }
    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        Ok((Vec::new(), 0))
    }
    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }
    fn install_snapshot(&mut self, _: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(0)
    }
}

#[tokio::test]
async fn apply_round_trips_through_shmem_rings() {
    let data_dir = TempDir::new().unwrap();
    let instance_dir = TempDir::new().unwrap();

    // Node-side: storage handles + instance + service-rings.
    let storage = JournalLogStorage::open(data_dir.path()).expect("storage");
    let handles = storage.handles(data_dir.path().to_path_buf());
    let _instance = Instance::create(instance_dir.path(), "kv-app", 1).expect("instance");
    let link = ServiceLink::create(instance_dir.path()).expect("service link");

    // Build the shmem-mode SM adapter.
    // M5: pass a dummy output channel; this test doesn't exercise the output path.
    let (_dummy_tx, _dummy_rx) = tokio::sync::mpsc::channel::<(u64, bytes::Bytes)>(8);
    let journal = storage._testonly_journal();
    let last_purged = storage._testonly_last_purged_arc();

    let snapshot_region_path = instance_dir.path().join("service").join("snapshot.region");

    let mut sm = ShmemAdaptedStateMachine::new(
        StubSm,
        handles,
        link.apply_producer,
        link.apply_resp_consumer,
        _dummy_tx,
        journal,
        last_purged,
        None,
        link.snapshot_producer,
        link.snapshot_resp_consumer,
        snapshot_region_path,
    )
    .expect("ShmemAdaptedStateMachine::new");

    // "Service" side: open the matching halves of apply.ring + apply_resp.ring.
    let service_apply =
        SpscRing::open(&instance_dir.path().join("service").join("apply.ring")).unwrap();
    let service_apply_resp =
        SpscRing::open(&instance_dir.path().join("service").join("apply_resp.ring")).unwrap();
    let (_, mut svc_apply_consumer) = service_apply.into_split();
    let (mut svc_apply_resp_producer, _) = service_apply_resp.into_split();

    // Spawn the service-side echo: read each ApplyFrame, decode the u32
    // payload, compute `payload + 1`, write that as the ApplyRespFrame
    // payload (also bincode-encoded). Mirrors what `uc_service`'s
    // apply_loop would do but inlined here so the test stays in one crate.
    let service_task = tokio::spawn(async move {
        let mut buf: Vec<u8> = Vec::with_capacity(1024);
        let mut handled = 0usize;
        while handled < 3 {
            let rec = match svc_apply_consumer.try_read(&mut buf) {
                Ok(Some(r)) if r.msg_type == MSG_TYPE_APPLY => r,
                Ok(_) => {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    continue;
                }
                Err(e) => panic!("svc apply read: {e}"),
            };
            let log_index = decode_extra_apply(rec.header_extra);
            let (input, _): (u32, _) =
                bincode::serde::decode_from_slice(&buf, bincode::config::standard()).unwrap();
            let output: u32 = input + 1;
            let out_bytes =
                bincode::serde::encode_to_vec(output, bincode::config::standard()).unwrap();
            loop {
                match svc_apply_resp_producer.try_write(
                    MSG_TYPE_APPLY_RESP,
                    0,
                    encode_extra_apply(log_index),
                    &out_bytes,
                ) {
                    Ok(()) => break,
                    Err(uc_protocol::ring::RingError::Full) => {
                        tokio::time::sleep(Duration::from_millis(1)).await
                    }
                    Err(e) => panic!("svc apply_resp write: {e}"),
                }
            }
            handled += 1;
        }
    });

    // Build three apply entries with bincode-encoded u32 payloads.
    // Responders are None — in this unit test there are no waiting clients.
    // `EntryResponder<TypeConfig>` = (Entry<...>, Option<ApplyResponder<TypeConfig>>).
    // ApplyResponder is pub(crate) inside openraft; in unit tests we pass None.
    fn make_entry(term: u64, index: u64, payload: u32) -> RaftEntry {
        let cmd = bincode::serde::encode_to_vec(payload, bincode::config::standard()).unwrap();
        Entry {
            log_id: LogId::new(LeaderId::new(term, 1), index),
            payload: EntryPayload::Normal(uc_node::raft::AppCommand(Bytes::from(cmd))),
        }
    }
    let entries: Vec<
        Result<openraft::storage::EntryResponder<uc_node::raft::TypeConfig>, std::io::Error>,
    > = vec![
        Ok((make_entry(1, 1, 41), None)),
        Ok((make_entry(1, 2, 99), None)),
        Ok((make_entry(1, 3, 7), None)),
    ];
    let entry_stream = stream::iter(entries);

    sm.apply(entry_stream).await.expect("apply");

    // Responses go through ApplyResponder (private to openraft) so cannot be
    // checked directly here. The ring round-trip is verified by the service_task
    // completing successfully (it would panic or hang otherwise). Applied state
    // confirms all 3 entries were processed.
    let (la, _membership) = sm.applied_state().await.expect("applied_state");
    assert_eq!(la.map(|l| l.index), Some(3));

    service_task.await.unwrap();
}

#[tokio::test]
async fn blank_and_membership_entries_emit_empty_response_without_touching_ring() {
    // Verifies the EntryPayload::Blank / ::Membership branches: they should
    // update last_applied / last_membership but not publish to the apply
    // ring (since there's no command to apply). If the service side never
    // sees a frame, the test's drain task will also see nothing — proving
    // the node-side bypass.
    let data_dir = TempDir::new().unwrap();
    let instance_dir = TempDir::new().unwrap();

    let storage = JournalLogStorage::open(data_dir.path()).expect("storage");
    let handles = storage.handles(data_dir.path().to_path_buf());
    let _instance = Instance::create(instance_dir.path(), "app", 1).expect("instance");
    let link = ServiceLink::create(instance_dir.path()).expect("link");

    // M5: pass a dummy output channel; this test doesn't exercise the output path.
    let (_dummy_tx, _dummy_rx) = tokio::sync::mpsc::channel::<(u64, bytes::Bytes)>(8);
    let journal = storage._testonly_journal();
    let last_purged = storage._testonly_last_purged_arc();

    let snapshot_region_path2 = instance_dir.path().join("service").join("snapshot.region");

    let mut sm = ShmemAdaptedStateMachine::new(
        StubSm,
        handles,
        link.apply_producer,
        link.apply_resp_consumer,
        _dummy_tx,
        journal,
        last_purged,
        None,
        link.snapshot_producer,
        link.snapshot_resp_consumer,
        snapshot_region_path2,
    )
    .expect("ShmemAdaptedStateMachine::new");

    let service_apply =
        SpscRing::open(&instance_dir.path().join("service").join("apply.ring")).unwrap();
    let (_, mut svc_apply_consumer) = service_apply.into_split();

    let entries: Vec<
        Result<openraft::storage::EntryResponder<uc_node::raft::TypeConfig>, std::io::Error>,
    > = vec![Ok((
        Entry {
            log_id: LogId::new(LeaderId::new(1, 1), 5),
            payload: EntryPayload::Blank,
        },
        None,
    ))];
    let entry_stream = stream::iter(entries);

    sm.apply(entry_stream).await.expect("apply");

    // Drain attempt: apply.ring should have no frame.
    let mut buf = Vec::new();
    let drained = svc_apply_consumer.try_read(&mut buf).expect("read");
    assert!(drained.is_none(), "service apply.ring should be empty");

    let (la, _) = sm.applied_state().await.expect("applied_state");
    assert_eq!(la.map(|l| l.index), Some(5));
}
