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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use uc2_gateway::{Edge, EdgeConfig, Member};
use uc2_remote::frame::FrameType;
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

/// (spec §5.4, ii) A connect shrinks everyone's grant, and the client learns
/// the smaller number from a standalone `STATUS` — **before** any `RESPONSE`
/// would have carried it. Driven raw because the property is about which
/// frame arrives when, on a connection that is deliberately idle: a
/// `RemoteClient` would only report "my window changed", never "it changed
/// without me asking anything".
#[test]
fn a_new_connection_shrinks_the_grant_and_status_says_so_unprompted() {
    use uc2_remote::frame::{FrameType, Status};

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

    // budget 56, cap 32: alone the first connection holds 32, with a second
    // one 28 each.
    let edge = Edge::start(edge_config(&dir, 64, 32)).unwrap();

    let mut first = common::dial_raw(edge.local_addr());
    common::send_hello(&mut first, 0xAAAA, common::APP);
    let (_, hello_ok) = common::read_until_frame(&mut first, FrameType::HelloOk,
                                                 Duration::from_secs(5))
        .expect("HELLO_OK");
    let granted = uc2_remote::frame::HelloOk::decode(&hello_ok).unwrap().credits;
    assert_eq!(granted, 32, "the only connection gets the whole budget, capped at per_conn");

    // The first connection sends NOTHING from here on. Whatever it hears next
    // is unprompted.
    let mut second = common::dial_raw(edge.local_addr());
    common::send_hello(&mut second, 0xBBBB, common::APP);
    assert!(common::read_until(&mut second, FrameType::HelloOk, Duration::from_secs(5)).is_some());
    let (_, body) = common::read_until_frame(&mut first, FrameType::Status,
                                             Duration::from_secs(5))
        .expect("no STATUS reached the idle client after its share shrank");
    let st = Status::decode(&body).unwrap();
    assert_eq!(st.credits, 28, "the STATUS must carry the SMALLER absolute grant");
    assert!(edge.stats().grant_changes >= 1, "stats: {:?}", edge.stats());

    drop(first);
    drop(second);
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
    // Not asserted as > 0 — whether the shared window actually fills is a
    // scheduling race (see the sibling test). What IS asserted: if it fired,
    // the client was told, unprompted.
    if es.backpressure_events > 0 {
        assert!(
            es.status_frames >= es.backpressure_events,
            "every squeeze owes the client a STATUS: {es:?}"
        );
    }

    edge.stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}

/// (spec §5.4, i) Whatever the edge has promised across every connection, the
/// SUM of it stays inside the budget — at every observed moment, not merely
/// once the dust settles.
///
/// This dials every connection CONCURRENTLY, with no inter-thread settle
/// barrier — an earlier version of this test settled connection `i` fully
/// (polling `grants_for_tests()` until it matched) before dialing `i + 1`,
/// which serializes every join through the test's own polling loop and so
/// can never observe two connects racing each other. That version could not
/// see the m13 §5 as-built erratum: `await_settled`'s generation counter
/// cannot distinguish "a driver pass that already accounts for MY `live++`"
/// from "a pass that ran just before it", so two connections joining at once
/// could both read a stale, smaller `live` and both go ready over-granted
/// (observed concretely: 3 racing connects at budget 56 produced a sum of
/// 74). A sampler thread polls the sum continuously through the whole dial.
///
/// This also checks a stricter, adjacent property while it is already
/// dialing concurrently: the FIRST frame every connection reads back must be
/// `HELLO_OK` — `Conn::set_ready` now happens inside `grant_lock`, before
/// this connection's own `HELLO_OK` is written, so it is worth confirming
/// concurrent joins never let the driver's own `STATUS` push win the race to
/// a connection's socket ahead of its own handshake reply.
#[test]
fn the_sum_of_grants_never_exceeds_the_edges_budget_under_concurrent_connects() {
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

    // budget = 64 - 64/8 = 56; per-connection cap 32. N=8 concurrent connects
    // is well inside `live <= budget`, so the floor-at-1 exception cannot
    // apply, and gives the race more simultaneous joiners than the minimal
    // repro (3) needs.
    const N: u64 = 8;
    let edge = Arc::new(Edge::start(edge_config(&dir, 64, 32)).unwrap());
    let budget = uc2_gateway::budget_for(64);
    assert_eq!(budget, 56);

    let worst = Arc::new(AtomicU32::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (e2, w2, s2) = (Arc::clone(&edge), Arc::clone(&worst), Arc::clone(&stop));
    // Sample first, THEN check `stop` — not the reverse — so a sampler thread
    // that is scheduled out until after the main thread has already set
    // `stop` still takes at least one reading, instead of exiting having
    // seen nothing (which the vacuity check below would otherwise wrongly
    // read as "the edge never granted anything" rather than "the sampler
    // never ran in time to look").
    let sampler = std::thread::spawn(move || {
        loop {
            let sum: u32 = e2.grants_for_tests().iter().map(|(_, g)| *g).sum();
            w2.fetch_max(sum, Ordering::Relaxed);
            if s2.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    });

    let conns: Vec<_> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..N)
            .map(|i| {
                let addr = edge.local_addr();
                s.spawn(move || {
                    let mut c = common::dial_raw(addr);
                    common::send_hello(&mut c, 0x200 + i, common::APP);
                    let (h, _) = loop {
                        match c.read_frame(common::READ_STALL) {
                            Ok(Some(f)) => break f,
                            Ok(None) => continue,
                            Err(e) => panic!("connection {i}: read after HELLO: {e}"),
                        }
                    };
                    assert_eq!(
                        h.ty,
                        FrameType::HelloOk,
                        "connection {i}: first frame after HELLO must be HELLO_OK, got {:?}",
                        h.ty
                    );
                    c
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(conns.len(), N as usize);

    // Settled state: everyone ends up on the same share.
    let want = uc2_gateway::grant_for(N as u32, budget, 32);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let g = edge.grants_for_tests();
        if g.len() == N as usize && g.iter().all(|(_, x)| *x == want) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "grants never settled to {want} at live={N}: {g:?}"
        );
        std::thread::sleep(Duration::from_millis(2));
    }

    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    let worst = worst.load(Ordering::Relaxed);
    assert!(
        worst <= budget,
        "the edge promised {worst} credits at once against a budget of {budget} \
         (concurrent connects)"
    );
    assert!(worst > 0, "the sampler never saw a grant at all — vacuous");

    drop(conns);
    Arc::try_unwrap(edge).ok().unwrap().stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}

/// As above, but racing connects against disconnects: `Shared::leave` takes
/// the same `grant_lock` as `join_and_grant`, and a departure must never let
/// the sum climb past budget either — a leave only ever GROWS the survivors'
/// share (the safe direction, deferred to the driver's next `push_grants` per
/// the m13 §5 as-built ruling), so this exists to catch a regression that
/// raced the two paths incorrectly, not because growth itself is suspect.
#[test]
fn the_sum_of_grants_never_exceeds_the_edges_budget_under_a_connect_disconnect_race() {
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

    let edge = Arc::new(Edge::start(edge_config(&dir, 64, 32)).unwrap());
    let budget = uc2_gateway::budget_for(64);
    assert_eq!(budget, 56);

    // Four connections, settled: grant_for(4, 56, 32) = 14 each, sum 56 —
    // exactly the budget, the tightest starting point available.
    let want4 = uc2_gateway::grant_for(4, budget, 32);
    assert_eq!(want4, 14);
    let mut conns = Vec::new();
    for i in 0..4u64 {
        let mut c = common::dial_raw(edge.local_addr());
        common::send_hello(&mut c, 0x300 + i, common::APP);
        let ok = common::read_until(&mut c, FrameType::HelloOk, Duration::from_secs(5));
        assert!(ok.is_some(), "setup connection {i} never got HELLO_OK");
        conns.push(c);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let g = edge.grants_for_tests();
        if g.len() == 4 && g.iter().all(|(_, x)| *x == want4) {
            break;
        }
        assert!(Instant::now() < deadline, "initial 4 never settled: {:?}", edge.grants_for_tests());
        std::thread::sleep(Duration::from_millis(2));
    }

    let worst = Arc::new(AtomicU32::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (e2, w2, s2) = (Arc::clone(&edge), Arc::clone(&worst), Arc::clone(&stop));
    // Sample first, THEN check `stop` — not the reverse — so a sampler thread
    // that is scheduled out until after the main thread has already set
    // `stop` still takes at least one reading, instead of exiting having
    // seen nothing (which the vacuity check below would otherwise wrongly
    // read as "the edge never granted anything" rather than "the sampler
    // never ran in time to look").
    let sampler = std::thread::spawn(move || {
        loop {
            let sum: u32 = e2.grants_for_tests().iter().map(|(_, g)| *g).sum();
            w2.fetch_max(sum, Ordering::Relaxed);
            if s2.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    });

    // Race: drop 2 of the 4 while dialing 2 new ones, all at once, no barrier
    // between the drops and the dials.
    let dropped: Vec<_> = conns.drain(0..2).collect();
    let new_conns: Vec<_> = std::thread::scope(|s| {
        let drop_handles: Vec<_> = dropped.into_iter().map(|c| s.spawn(move || drop(c))).collect();
        let dial_handles: Vec<_> = (0..2u64)
            .map(|i| {
                let addr = edge.local_addr();
                s.spawn(move || {
                    let mut c = common::dial_raw(addr);
                    common::send_hello(&mut c, 0x400 + i, common::APP);
                    let ok = common::read_until(&mut c, FrameType::HelloOk, Duration::from_secs(5));
                    assert!(ok.is_some(), "race dial {i} never got HELLO_OK");
                    c
                })
            })
            .collect();
        for h in drop_handles {
            h.join().unwrap();
        }
        dial_handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    conns.extend(new_conns);
    assert_eq!(conns.len(), 4);

    // Settled state: back to 4 live, back to grant_for(4).
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let g = edge.grants_for_tests();
        if g.len() == 4 && g.iter().all(|(_, x)| *x == want4) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "post-race grants never resettled to {want4}: {:?}",
            edge.grants_for_tests()
        );
        std::thread::sleep(Duration::from_millis(2));
    }

    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    let worst = worst.load(Ordering::Relaxed);
    assert!(
        worst <= budget,
        "the edge promised {worst} credits at once against a budget of {budget} \
         (connect/disconnect race)"
    );
    assert!(worst > 0, "the sampler never saw a grant at all — vacuous");

    drop(conns);
    Arc::try_unwrap(edge).ok().unwrap().stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}
