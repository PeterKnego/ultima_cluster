// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Credit accounting over the wire (spec §4.2, §4.3): what the edge grants,
//! what a client is allowed to have unanswered, and what happens when the
//! shared `Engine` window is the binding constraint rather than the
//! per-connection one.
//!
//! `conn.rs`'s unit tests already pin the AIMD arithmetic (halve on
//! `Backpressure`, double back to the ceiling, never below 1). What they
//! cannot show is the *end-to-end* property these tests exist for: a client
//! that only ever learns its window from `HELLO_OK`, `RESPONSE` and `STATUS`
//! frames stays inside it, and every request still resolves when the window
//! is squeezed underneath it.

use std::time::Duration;

use uc2_gateway::{Edge, EdgeConfig, Member};
use uc2_remote::{RemoteClient, RemoteConfig, RemoteStats};
use uc_lincheck::register::{Cmd, CmdResp};

mod common;

/// Writes each client pipelines.
const PER_CLIENT: u64 = 50;

fn enc(c: &Cmd) -> Vec<u8> {
    bincode::serde::encode_to_vec(c, bincode::config::standard()).unwrap()
}

fn dec(b: &[u8]) -> CmdResp {
    bincode::serde::decode_from_slice(b, bincode::config::standard()).unwrap().0
}

fn edge_config(dir: &std::path::Path, max_inflight: u32, per_conn: u32) -> EdgeConfig {
    EdgeConfig {
        instance_dir: dir.to_path_buf(),
        app_id: common::APP.into(),
        listen: "127.0.0.1:0".parse().unwrap(),
        members: vec![Member { node_id: 0, gateway: "127.0.0.1:0".into() }],
        max_inflight,
        per_conn_inflight: per_conn,
        status_interval: Duration::from_millis(50),
        ..EdgeConfig::defaults()
    }
}

/// One client's whole run: connect, pipeline `PER_CLIENT` writes, assert every
/// one of them was acknowledged, hand back its stats.
fn run_client(addr: String, base: u64) -> RemoteStats {
    let client = RemoteClient::connect(RemoteConfig {
        app_id: common::APP.into(),
        members: vec![addr],
        request_timeout: Duration::from_secs(30),
        ..Default::default()
    })
    .unwrap();
    // Issue first, wait second: the point is to have more outstanding than the
    // window allows, so the credit gate is what paces it.
    let tickets: Vec<_> =
        (0..PER_CLIENT).map(|i| client.submit(&enc(&Cmd::Write(base + i))).unwrap()).collect();
    for (i, t) in tickets.into_iter().enumerate() {
        let r = t.wait().unwrap_or_else(|e| panic!("client {base} write {i}: {e:?}"));
        assert_eq!(dec(&r.bytes), CmdResp::WriteAck);
    }
    let s = client.stats();
    client.shutdown();
    s
}

/// Two clients, four credits each, against an engine window of eight: neither
/// client may ever be told it has more than four.
#[test]
fn two_clients_stay_inside_the_credits_the_edge_grants() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = uc2_service::ServiceBuilder::new(
        uc2_service::ServiceConfig::new(&dir, common::APP),
        uc2_service::Sessioned::new(
            uc_lincheck::register::RegisterSm::default(),
            uc2_service::SessionConfig::default(),
        ),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);

    let edge = Edge::start(edge_config(&dir, 8, 4)).unwrap();
    let addr = edge.local_addr().to_string();

    let (a, b) = std::thread::scope(|s| {
        let ha = s.spawn(|| run_client(addr.clone(), 1_000));
        let hb = s.spawn(|| run_client(addr.clone(), 2_000));
        (ha.join().unwrap(), hb.join().unwrap())
    });

    for (who, s) in [("a", a), ("b", b)] {
        assert!(
            (1..=4).contains(&s.max_credits_seen),
            "client {who} was advertised {} credits, ceiling is 4",
            s.max_credits_seen
        );
        assert_eq!(s.reconnects, 0, "client {who} should not have had to fail over: {s:?}");
        assert_eq!(s.unknown, 0, "client {who}: {s:?}");
    }

    let es = edge.stats();
    assert_eq!(es.connections, 2);
    assert_eq!(es.submits, 2 * PER_CLIENT, "every write reached the ring exactly once: {es:?}");
    assert_eq!(es.responses, 2 * PER_CLIENT, "one RESPONSE each: {es:?}");
    assert_eq!((es.redirects, es.unknown), (0, 0), "a healthy single-node leader: {es:?}");
    // Not asserted as > 0: whether the shared window actually fills is a
    // scheduling race, and forcing it would be testing the test. The
    // assertion is that correctness does not depend on which way it went.
    println!("backpressure_events = {} (informational)", es.backpressure_events);

    edge.stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}

/// The same two clients against an engine window that is *smaller* than the
/// credits handed out (4 shared, 4 each), so `try_submit` really does hit
/// `Backpressure` and the AIMD squeeze runs under load. Every request must
/// still resolve, and the client must still never see more than its ceiling —
/// a squeeze only ever moves credits down.
#[test]
fn a_squeezed_window_still_resolves_every_request() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = uc2_service::ServiceBuilder::new(
        uc2_service::ServiceConfig::new(&dir, common::APP),
        uc2_service::Sessioned::new(
            uc_lincheck::register::RegisterSm::default(),
            uc2_service::SessionConfig::default(),
        ),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);

    let edge = Edge::start(edge_config(&dir, 4, 4)).unwrap();
    let addr = edge.local_addr().to_string();

    let (a, b) = std::thread::scope(|s| {
        let ha = s.spawn(|| run_client(addr.clone(), 3_000));
        let hb = s.spawn(|| run_client(addr.clone(), 4_000));
        (ha.join().unwrap(), hb.join().unwrap())
    });
    for (who, s) in [("a", a), ("b", b)] {
        assert!(s.max_credits_seen <= 4, "client {who} saw {} credits", s.max_credits_seen);
        assert_eq!(s.unknown, 0, "client {who}: {s:?}");
    }

    let es = edge.stats();
    assert_eq!(es.responses, 2 * PER_CLIENT, "every request resolved: {es:?}");
    assert_eq!(es.retries, 0, "backpressure is absorbed by credits, never bounced: {es:?}");

    edge.stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}

/// (spec §5.4, i) Whatever the edge has promised across every connection, the
/// SUM of it stays inside the budget — at every observed moment, not merely
/// once the dust settles. Sampled by a watchdog thread while connections
/// arrive, so a transient over-promise between "this connection is ready" and
/// "everyone else has been told their share shrank" is caught.
#[test]
fn the_sum_of_grants_never_exceeds_the_edges_budget() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = uc2_service::ServiceBuilder::new(
        uc2_service::ServiceConfig::new(&dir, common::APP),
        uc2_service::Sessioned::new(
            uc_lincheck::register::RegisterSm::default(),
            uc2_service::SessionConfig::default(),
        ),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);

    // budget = 64 - 64/8 = 56; per-connection cap 32. Six connections is
    // well inside `live <= budget`, so the floor-at-1 exception cannot apply.
    let edge = std::sync::Arc::new(Edge::start(edge_config(&dir, 64, 32)).unwrap());
    let budget = uc2_gateway::budget_for(64);
    assert_eq!(budget, 56);

    let worst = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (e2, w2, s2) =
        (std::sync::Arc::clone(&edge), std::sync::Arc::clone(&worst), std::sync::Arc::clone(&stop));
    let sampler = std::thread::spawn(move || {
        while !s2.load(std::sync::atomic::Ordering::Relaxed) {
            let sum: u32 = e2.grants_for_tests().iter().map(|(_, g)| *g).sum();
            w2.fetch_max(sum, std::sync::atomic::Ordering::Relaxed);
            std::thread::sleep(Duration::from_micros(200));
        }
    });

    let mut conns = Vec::new();
    for i in 0..6u64 {
        let mut c = common::dial_raw(edge.local_addr());
        common::send_hello(&mut c, 0x100 + i, common::APP);
        let ok = common::read_until(&mut c, uc2_remote::frame::FrameType::HelloOk,
                                    Duration::from_secs(5));
        assert!(ok.is_some(), "connection {i} never got HELLO_OK");
        let live = (i + 1) as u32;
        let want = uc2_gateway::grant_for(live, budget, 32);
        // Settled state: everyone holds the same share.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let g = edge.grants_for_tests();
            if g.len() == live as usize && g.iter().all(|(_, x)| *x == want) {
                break;
            }
            assert!(std::time::Instant::now() < deadline,
                    "grants never settled to {want} at live={live}: {g:?}");
            std::thread::sleep(Duration::from_millis(2));
        }
        conns.push(c);
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    sampler.join().unwrap();
    let worst = worst.load(std::sync::atomic::Ordering::Relaxed);
    assert!(worst <= budget,
            "the edge promised {worst} credits at once against a budget of {budget}");
    assert!(worst > 0, "the sampler never saw a grant at all — vacuous");

    drop(conns);
    std::sync::Arc::try_unwrap(edge).ok().unwrap().stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}
