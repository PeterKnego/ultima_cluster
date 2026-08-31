// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 11 service-side capstone: the read path end to end plus the epoch-
//! mismatch pin.
//!
//! 1. `reads_return_the_applied_total`: a single node + service + client;
//!    submit `Add(1)`×10, then a linearizable AND a snapshot query each return
//!    the running total 10 — the barrier (node) + `drain_queries` (service)
//!    answer a real client read.
//! 2. `stale_epoch_svc_query_gets_retry`: after the service crashes and
//!    restarts (bumping `service_epoch`), a `MSG_V2_SVC_QUERY` stamped with the
//!    OLD epoch — written directly onto `svc_query.ring` — is answered with
//!    `MSG_V2_RETRY` on the SERVICE egress, never with a stale-state answer.
//!    This pins the task14 TOCTOU close in v2 shape: the service refuses a read
//!    routed for a superseded incarnation.

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc_client::{Client, ClientError};
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig};
use uc_protocol::ring::{BroadcastRing, SpscRing};
use uc_protocol::v2::ipc::{
    MSG_V2_RESPONSE, MSG_V2_RETRY, MSG_V2_SVC_QUERY, client_from_extra, extra_client,
};
use uc_service::{ServiceBuilder, ServiceConfig, StateMachine};

// ------------------------------------------------------------- the state machine

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cmd {
    Add(u64),
}

/// A running total. `query(())` returns the current total — so after ten
/// `Add(1)` applies a read returns exactly 10, pinning both apply order and the
/// read path.
#[derive(Default)]
struct CountSm {
    total: u64,
    last_applied: Option<u64>,
}

impl StateMachine for CountSm {
    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, position: u64, cmd: Cmd) -> u64 {
        let Cmd::Add(n) = cmd;
        self.total += n;
        self.last_applied = Some(position);
        self.total
    }

    fn query(&self, _q: ()) -> u64 {
        self.total
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

// --------------------------------------------------------------------- harness

fn node_config(dir: &Path, app_id: &str) -> NodeConfig {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app_id.into(),
        buffer_bytes: 1 << 20,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::default(),
    }
}

fn wait_until(mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "condition never held");
        std::thread::sleep(Duration::from_millis(1));
    }
}

// ------------------------------------------------------------------------ tests

/// End to end: submit ten `Add(1)`, then a linearizable read and a snapshot
/// read each return the running total 10 — the node barrier (READ_PROBE quorum,
/// trivially self-satisfied on one node) plus the service's `drain_queries`
/// answer a real client `query_*` call.
#[test]
fn reads_return_the_applied_total() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(node_config(dir.path(), "q-e2e")).unwrap();
    wait_until(|| node.can_serve());

    let svc = ServiceBuilder::new(ServiceConfig::new(dir.path(), "q-e2e"), CountSm::default())
        .start()
        .unwrap();

    let client = Client::connect(dir.path(), "q-e2e").unwrap();
    for _ in 0..10 {
        let _total: u64 = client.submit(&Cmd::Add(1)).unwrap();
    }

    // Linearizable read: routed through the node's read-index barrier, waits for
    // the service to catch up to the read's commit position, then answered by
    // the service with the current total.
    let lin: u64 = client.query_linearizable(&()).unwrap();
    assert_eq!(
        lin, 10,
        "linearizable read must see all ten committed applies"
    );

    // Snapshot read: forwarded straight to the service (epoch check skipped),
    // reads the live SM total.
    let snap: u64 = client.query_snapshot(&()).unwrap();
    assert_eq!(snap, 10, "snapshot read of the caught-up SM must see 10");

    client.shutdown();
    svc.stop();
    node.stop();
}

/// The epoch-mismatch pin (deterministic, no timing race): after the service
/// crashes and a fresh incarnation attaches (bumping `service_epoch` 1 -> 2), a
/// query routed for the OLD epoch must be REFUSED with `MSG_V2_RETRY` — the
/// node's barrier stamps each forwarded read with the epoch it observed, and the
/// service rejects any read whose stamp is not its own current incarnation.
///
/// Driven by writing the `MSG_V2_SVC_QUERY` directly onto `svc_query.ring` so
/// the stale-epoch condition is forced, not waited for. `MSG_V2_RETRY` here is
/// emitted PRE-ANSWER (side-effect-free: a query has no side effects, and the
/// SM is never touched on the mismatch path).
#[test]
fn stale_epoch_svc_query_gets_retry() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(node_config(dir.path(), "q-epoch")).unwrap();
    wait_until(|| node.can_serve());

    // First incarnation: epoch 1.
    let svc1 = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "q-epoch"),
        CountSm::default(),
    )
    .start()
    .unwrap();
    let old_epoch = svc1.epoch();
    assert_eq!(old_epoch, 1);
    svc1.crash();

    // Second incarnation: epoch 2. Subscribe to the service egress BEFORE
    // driving the query so the RETRY answer cannot be missed.
    let mut egress = BroadcastRing::open(&dir.path().join("egress_service.0.broadcast"))
        .unwrap()
        .subscribe();
    let svc2 = ServiceBuilder::new(
        ServiceConfig::new(dir.path(), "q-epoch"),
        CountSm::default(),
    )
    .start()
    .unwrap();
    assert_eq!(svc2.epoch(), 2, "the restarted incarnation bumps the epoch");
    let cnc = uc_log::cnc::CncPage::open_file(&dir.path().join("cnc2.dat"), "q-epoch").unwrap();
    wait_until(|| cnc.service_slot(0).epoch.load_acquire() == 2);

    // Write an SVC_QUERY stamped with the OLD (superseded) epoch directly onto
    // svc_query.ring. Payload = expected_epoch u64 LE ++ query bytes. No client
    // query is in flight, so the node's barrier never produces onto this ring
    // concurrently — the direct write is the sole producer for this window.
    let (mut producer, _consumer) = SpscRing::open(&dir.path().join("svc_query.0.ring"))
        .unwrap()
        .into_split();
    let query_bytes = bincode::serde::encode_to_vec((), bincode::config::standard()).unwrap();
    let mut payload = old_epoch.to_le_bytes().to_vec();
    payload.extend_from_slice(&query_bytes);
    let extra = extra_client(0x1234_5678, 0x9abc_def0);
    producer
        .try_write(MSG_V2_SVC_QUERY, 0, extra, &payload)
        .unwrap();

    // The service answers with MSG_V2_RETRY addressed to our (client_id,
    // local_seq), and NEVER a MSG_V2_RESPONSE (a stale answer would be a
    // linearizability violation).
    let mut buf = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut got_retry = false;
    while !got_retry {
        assert!(
            Instant::now() < deadline,
            "no RETRY for the stale-epoch query"
        );
        match egress.try_read(&mut buf) {
            Ok(Some(rec)) if client_from_extra(rec.header_extra) == (0x1234_5678, 0x9abc_def0) => {
                assert_ne!(
                    rec.msg_type, MSG_V2_RESPONSE,
                    "a stale-epoch query must never get a state answer"
                );
                if rec.msg_type == MSG_V2_RETRY {
                    got_retry = true;
                }
            }
            Ok(_) => {}
            Err(e) => panic!("egress read error: {e}"),
        }
    }

    svc2.stop();
    node.stop();
}

/// The client surfaces a barrier/epoch RETRY as `ClientError::Retry` (a
/// side-effect-free transient). Kept as a compile-time reference to the error
/// the read path can bubble; the deterministic behavior is pinned above.
#[allow(dead_code)]
fn retry_is_a_client_error(e: ClientError) -> bool {
    matches!(e, ClientError::Retry)
}
