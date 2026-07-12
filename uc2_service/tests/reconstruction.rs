// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 9: journal-replay reconstruction (task14 semantics). A fresh or
//! restarted service, attaching to a node whose live log buffer has long since
//! scrolled past position 0, rebuilds its in-memory state by replaying the
//! ARCHIVED log and then rejoins the live buffer — one rejoin mechanism
//! (try-live-then-replay) for both a first attach and a service restart.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_service::{Service, ServiceBuilder, ServiceConfig, StateMachine};
use uc2_log::cnc::CncPage;
use uc_protocol::ring::{MpscProducer, MpscRing, RingError};
use uc_protocol::v2::ipc::{MSG_V2_SUBMIT, extra_client};

// A tiny ring so the committed history scrolls out of the live buffer fast,
// forcing the attaching service down the journal-replay reconstruction path.
const RING_BYTES: usize = 64 * 1024;
const CLIENT_ID: u32 = 7;

// ------------------------------------------------------------- the state machine

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cmd {
    Add(u64),
}

/// A running total; `query(())` returns the current total. `last_applied` tracks
/// the byte position of the last applied frame (the idempotency key).
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

fn start_single_node_with_buffer(dir: &Path, app_id: &str, buffer_bytes: usize) -> Node {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Node::start(NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app_id.into(),
        buffer_bytes,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
    })
    .unwrap()
}

fn cfg(dir: &Path, app_id: &str) -> ServiceConfig {
    ServiceConfig::new(dir, app_id)
}

fn open_cnc(dir: &Path, app_id: &str) -> Arc<CncPage> {
    CncPage::open_file(&dir.join("cnc2.dat"), app_id).unwrap()
}

fn open_ingress(dir: &Path) -> MpscProducer {
    let ring = MpscRing::open(&dir.join("ingress.ring")).unwrap();
    let (prod, _consumer) = ring.into_split();
    prod
}

fn wait_until(mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < deadline, "condition never held");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Submit one `Cmd` through the real ingress ring, retrying while the ring is
/// momentarily full (the node drains it into the log continuously). `retries`
/// scales the attempt budget; a genuinely wedged ring fails the test loudly.
fn write_submit_retrying(prod: &MpscProducer, retries: u32, local_seq: u32, cmd: &Cmd) {
    let payload = bincode::serde::encode_to_vec(cmd, bincode::config::standard()).unwrap();
    let extra = extra_client(CLIENT_ID, local_seq);
    let cap = (retries.max(1) as u64) * 20_000;
    for attempt in 0.. {
        match prod.try_write(MSG_V2_SUBMIT, 0, extra, &payload) {
            Ok(()) => return,
            Err(RingError::Full) => {
                assert!(attempt < cap, "ingress ring never drained");
                std::thread::sleep(Duration::from_micros(50));
            }
            Err(e) => panic!("submit failed: {e}"),
        }
    }
}

/// Wait until every submitted command has been committed AND locally durable —
/// the pipeline is drained (append == commit == durable, stable). By that point
/// `append` has crossed the ring capacity many times over, so the live buffer
/// has scrolled and a fresh service must reconstruct from the journal.
fn wait_commit_covers_all(node: &Node) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = 0u64;
    let mut stable_since = Instant::now();
    loop {
        let c = node.counters();
        let append = c.append.load_acquire();
        let commit = c.commit.load_acquire();
        let durable = c.durable.load_acquire();
        if append > 4096 && append == commit && append == durable {
            if append == last {
                if stable_since.elapsed() > Duration::from_millis(300) {
                    return;
                }
            } else {
                last = append;
                stable_since = Instant::now();
            }
        } else {
            last = 0;
            stable_since = Instant::now();
        }
        assert!(
            Instant::now() < deadline,
            "commit never covered all submits (append={append} commit={commit} durable={durable})"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn query_total(svc: &Service<CountSm>) -> u64 {
    svc.query(())
}

/// The service catches up to the apply frontier = min(commit, durable).
fn wait_service_caught_up(cnc: &CncPage) {
    wait_until(|| {
        let target =
            cnc.counters().commit.load_acquire().min(cnc.counters().durable.load_acquire());
        target > 0 && cnc.service().service_applied.load_acquire() >= target
    });
}

// ------------------------------------------------------------------------ tests

#[test]
fn fresh_service_reconstructs_from_journal_after_ring_scrolled() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node_with_buffer(dir.path(), "rec", RING_BYTES);
    wait_until(|| node.can_serve());

    let prod = open_ingress(dir.path());
    for i in 1..=2_000u32 {
        write_submit_retrying(&prod, 5, i, &Cmd::Add(1)); // >> ring capacity
    }
    wait_commit_covers_all(&node);
    assert!(
        node.counters().append.load_acquire() > RING_BYTES as u64,
        "precondition: the live ring must have scrolled so replay is exercised"
    );

    // FIRST service attaches only now: the ring long since scrolled → the fresh
    // SM at cursor 0 hits Overrun immediately → journal replay reconstruction.
    let svc =
        ServiceBuilder::new(cfg(dir.path(), "rec"), CountSm::default()).start().unwrap();
    let cnc = open_cnc(dir.path(), "rec");
    wait_service_caught_up(&cnc);
    assert_eq!(query_total(&svc), 2_000, "every committed Add applied exactly once");

    svc.stop();
    node.stop();
}

#[test]
fn restarted_service_epoch_bumps_and_state_rebuilds() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node_with_buffer(dir.path(), "rst", RING_BYTES);
    wait_until(|| node.can_serve());

    let prod = open_ingress(dir.path());
    for i in 1..=2_000u32 {
        write_submit_retrying(&prod, 5, i, &Cmd::Add(1));
    }
    wait_commit_covers_all(&node);

    // First incarnation: reconstructs from the journal, converges to 2000.
    let svc1 =
        ServiceBuilder::new(cfg(dir.path(), "rst"), CountSm::default()).start().unwrap();
    let cnc = open_cnc(dir.path(), "rst");
    wait_service_caught_up(&cnc);
    assert_eq!(query_total(&svc1), 2_000);
    let old_epoch = svc1.epoch();

    // Hard-crash it (no graceful teardown), then attach a FRESH SM on the same
    // dir. The node (and its cnc page) stay up across the service restart.
    svc1.crash();

    let svc2 =
        ServiceBuilder::new(cfg(dir.path(), "rst"), CountSm::default()).start().unwrap();
    let new_epoch = svc2.epoch();
    assert_eq!(new_epoch, old_epoch + 1, "each attach bumps service_epoch exactly once");

    // The fresh in-memory SM rebuilds the SAME total purely from the journal.
    wait_until(|| query_total(&svc2) == 2_000);
    assert_eq!(query_total(&svc2), 2_000, "in-memory state fully reconstructed from the journal");

    svc2.stop();
    node.stop();
}
