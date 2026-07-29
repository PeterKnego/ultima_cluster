// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 8 capstone: a service attaches to a running single node, follows the
//! committed log, applies 100 client submits in order, and publishes a
//! response per submit onto the egress broadcast with the client's identity
//! and the position ++ bincoded response payload (the pinned egress layout).

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_service::{ServiceBuilder, ServiceConfig, StateMachine};
use uc_protocol::ring::{BroadcastConsumer, BroadcastRing, MpscProducer, MpscRing};
use uc_protocol::v2::ipc::{MSG_V2_RESPONSE, MSG_V2_SUBMIT, client_from_extra, extra_client};

// ------------------------------------------------------------- the state machine

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cmd {
    Add(u64),
}

/// A running total; the response is the total AFTER applying, so the response
/// to the Nth `Add(1)` is exactly N — which pins apply ORDER (the final
/// response must be 100).
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
        purge: uc2_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc2_node::CryptoConfig::Disabled,
    }
}

fn wait_until(mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "condition never held");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn open_ingress(dir: &Path) -> MpscProducer {
    let ring = MpscRing::open(&dir.join("ingress.ring")).unwrap();
    let (prod, _consumer) = ring.into_split();
    prod
}

fn write_submit(prod: &MpscProducer, client_id: u32, local_seq: u32, cmd: &Cmd) {
    let payload = bincode::serde::encode_to_vec(cmd, bincode::config::standard()).unwrap();
    prod.try_write(MSG_V2_SUBMIT, 0, extra_client(client_id, local_seq), &payload).unwrap();
}

/// Drain every currently-available egress record; count the ones addressed to
/// `client_id` and track the maximum decoded running total seen. Returns the
/// number of new matching responses drained in THIS call.
fn drain_responses(sub: &mut BroadcastConsumer, client_id: u32, max_total: &mut u64) -> u32 {
    let mut n = 0u32;
    let mut buf = Vec::new();
    loop {
        match sub.try_read(&mut buf) {
            Ok(Some(rec)) => {
                if rec.msg_type == MSG_V2_RESPONSE && client_from_extra(rec.header_extra).0 == client_id
                {
                    // Pinned layout: position u64 LE ++ bincode(response).
                    let pos = u64::from_le_bytes(buf[..8].try_into().unwrap());
                    assert!(pos >= 32, "responses are for data frames, past the NewTerm frame");
                    let (total, _): (u64, usize) =
                        bincode::serde::decode_from_slice(&buf[8..], bincode::config::standard())
                            .unwrap();
                    *max_total = (*max_total).max(total);
                    n += 1;
                }
            }
            Ok(None) => break,
            Err(e) => panic!("egress read error: {e}"),
        }
    }
    n
}

// ------------------------------------------------------------------------ test

#[test]
fn service_applies_committed_frames_and_publishes_responses() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(node_config(dir.path(), "svc-test")).unwrap();
    wait_until(|| node.can_serve());

    // Subscribe to the egress BEFORE submitting: a broadcast subscriber is
    // "join-and-listen" (it skips records published before it joined), so
    // joining first guarantees we observe every response.
    let mut sub = BroadcastRing::open(&dir.path().join("egress_service.broadcast")).unwrap().subscribe();

    let svc = ServiceBuilder::new(ServiceConfig::new(dir.path(), "svc-test"), CountSm::default())
        .start()
        .unwrap();

    // 100 submits through the real ingress ring, client identity (13, 1..=100).
    let prod = open_ingress(dir.path());
    for i in 1..=100u32 {
        write_submit(&prod, 13, i, &Cmd::Add(1));
    }

    // Every submit produces a matching response on the egress.
    let mut got = 0u32;
    let mut max_total = 0u64;
    wait_until(|| {
        got += drain_responses(&mut sub, 13, &mut max_total);
        got == 100
    });
    assert_eq!(max_total, 100, "apply order: the 100th Add(1) response is the running total 100");

    // service_applied catches up to the apply frontier = min(commit, durable).
    let cnc = uc2_log::cnc::CncPage::open_file(&dir.path().join("cnc2.dat"), "svc-test").unwrap();
    wait_until(|| {
        let target =
            cnc.counters().commit.load_acquire().min(cnc.counters().durable.load_acquire());
        cnc.service().service_applied.load_acquire() >= target && target > 32
    });

    // The service bumped its incarnation epoch at attach.
    assert_eq!(svc.epoch(), 1, "first incarnation bumps service_epoch 0 -> 1");
    assert_eq!(svc.instance_id(), cnc.meta().instance_id);

    svc.stop();
    node.stop();
}

/// The pinned egress frame layout is byte-exact: `header_extra` echoes the
/// client's `(client_id, local_seq)` and the payload is `position u64 LE ++
/// bincode(response)`. A drift here silently breaks the Task 10 client matcher.
#[test]
fn egress_frame_layout_is_byte_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(node_config(dir.path(), "layout")).unwrap();
    wait_until(|| node.can_serve());

    let mut sub = BroadcastRing::open(&dir.path().join("egress_service.broadcast")).unwrap().subscribe();
    let svc = ServiceBuilder::new(ServiceConfig::new(dir.path(), "layout"), CountSm::default())
        .start()
        .unwrap();

    let prod = open_ingress(dir.path());
    write_submit(&prod, 0x0102_0304, 0x0506_0708, &Cmd::Add(7));

    let mut buf = Vec::new();
    let rec = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(Some(rec)) = sub.try_read(&mut buf)
                && rec.msg_type == MSG_V2_RESPONSE
            {
                break rec;
            }
            assert!(Instant::now() < deadline, "no response observed");
            std::thread::sleep(Duration::from_millis(1));
        }
    };

    // header_extra = extra_client(session_id, correlation_id), same LE pair the
    // client stamped on submit.
    assert_eq!(rec.header_extra, [0x04, 0x03, 0x02, 0x01, 0x08, 0x07, 0x06, 0x05]);
    // payload[..8] = position (a data frame, so >= 32); payload[8..] = bincode(7u64).
    let pos = u64::from_le_bytes(buf[..8].try_into().unwrap());
    assert!(pos >= 32);
    let (total, consumed): (u64, usize) =
        bincode::serde::decode_from_slice(&buf[8..], bincode::config::standard()).unwrap();
    assert_eq!(total, 7);
    assert_eq!(8 + consumed, buf.len(), "no trailing bytes beyond position ++ bincode(resp)");

    svc.stop();
    node.stop();
}
