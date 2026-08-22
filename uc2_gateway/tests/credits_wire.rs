// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Wire-level tests: what the edge actually puts on the socket, and when.
//!
//! These drive a raw `TcpStream` rather than a `RemoteClient`, because the
//! properties under test are about *frame ordering on the wire* — which a
//! client that refuses to proceed past a bad first frame can only report as
//! "the dial failed", not as "here is what arrived instead".
//!
//! 1. **Nothing precedes `HELLO_OK`.** The edge's `STATUS` timer runs on a
//!    200 ms default while the handshake budget is 5 s, so on a slow link a
//!    naive edge would greet a still-dialing client with `STATUS{client_id 0}`
//!    and fail its handshake outright (`RemoteClient` requires the first frame
//!    to be `HELLO_OK`/`HELLO_REFUSED`/`REDIRECT`).
//! 2. **`STATUS` does flow once the handshake is done** — it is the edge→client
//!    half of the liveness contract, and a client that hears nothing for its
//!    `dead_after` fails the connection over.
//! 3. **A faulted edge refuses new handshakes**, rather than accepting a client
//!    into a loop of `LEADER_CHANGED` → reconnect → `LEADER_CHANGED`.

use std::net::TcpStream;
use std::time::{Duration, Instant};

use uc2_gateway::{Edge, EdgeConfig, Member};
use uc2_remote::conn::FramedConn;
use uc2_remote::frame::{
    FrameType, HELLO_REFUSED_APP_ID, HELLO_REFUSED_FAULTED, Header, Hello, HelloRefused,
    PROTOCOL_VERSION,
};
use uc2_remote::{RemoteClient, RemoteConfig, RemoteError};
use uc2_service::{ServiceBuilder, ServiceConfig, SessionConfig, Sessioned};
use uc_lincheck::register::RegisterSm;

mod common;

const STATUS_INTERVAL: Duration = Duration::from_millis(50);

fn edge_config(dir: &std::path::Path) -> EdgeConfig {
    EdgeConfig {
        instance_dir: dir.to_path_buf(),
        app_id: common::APP.into(),
        listen: "127.0.0.1:0".parse().unwrap(),
        members: vec![Member { node_id: 0, gateway: "127.0.0.1:0".into() }],
        status_interval: STATUS_INTERVAL,
        ..EdgeConfig::defaults()
    }
}

/// Open a raw framed connection to the edge, with a short read timeout so a
/// silent edge shows up as `Ok(None)` rather than a hang.
fn dial_raw(edge: &Edge) -> FramedConn {
    let s = TcpStream::connect(edge.local_addr()).expect("connect");
    let c = FramedConn::new(s).unwrap();
    c.set_read_timeout(Some(Duration::from_millis(10))).unwrap();
    c.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
    c
}

fn send_hello(c: &mut FramedConn, client_id: u64, app_id: &str) {
    let mut out = Vec::new();
    Hello { app_id }.encode(&mut out);
    let h = Header {
        ty: FrameType::Hello,
        flags: 0,
        version: PROTOCOL_VERSION,
        client_id,
        seq: 0,
    };
    c.write_frame(h, &out).expect("write HELLO");
}

/// Read frames until `want` arrives or `budget` runs out.
fn read_until(c: &mut FramedConn, want: FrameType, budget: Duration) -> Option<Header> {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        match c.read_frame() {
            Ok(Some((h, _))) if h.ty == want => return Some(h),
            Ok(Some(_)) => {}
            Ok(None) => {}
            Err(_) => return None,
        }
    }
    None
}

#[test]
fn no_frame_precedes_hello_ok_and_status_follows_it() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = ServiceBuilder::new(
        ServiceConfig::new(&dir, common::APP),
        Sessioned::new(RegisterSm::default(), SessionConfig::default()),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);
    let edge = Edge::start(edge_config(&dir)).unwrap();

    let mut c = dial_raw(&edge);

    // --- 1. Stay silent for well past three status intervals. A connection
    // that has not completed its handshake must receive NOTHING.
    let quiet_until = Instant::now() + STATUS_INTERVAL * 6;
    while Instant::now() < quiet_until {
        match c.read_frame() {
            Ok(None) => {}
            Ok(Some((h, _))) => {
                panic!("the edge spoke before HELLO: {:?} (client_id {})", h.ty, h.client_id)
            }
            Err(e) => panic!("the edge dropped a still-dialing connection: {e}"),
        }
    }
    assert_eq!(
        edge.stats().status_frames,
        0,
        "no STATUS may be produced for a connection that has not handshaken"
    );

    // --- 2. Now handshake: the very first frame must be HELLO_OK.
    send_hello(&mut c, 0xABCD, common::APP);
    let first = loop {
        match c.read_frame() {
            Ok(Some(f)) => break f,
            Ok(None) => continue,
            Err(e) => panic!("read after HELLO: {e}"),
        }
    };
    assert_eq!(first.0.ty, FrameType::HelloOk, "the first frame a client sees must be HELLO_OK");

    // --- 3. Idle: the STATUS timer now fires, on the wire.
    let status = read_until(&mut c, FrameType::Status, Duration::from_secs(5));
    assert!(status.is_some(), "no STATUS arrived after HELLO_OK; edge liveness is broken");
    assert!(edge.stats().status_frames >= 1, "stats: {:?}", edge.stats());

    drop(c);
    edge.stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}

/// The same property from the client's side: a real `RemoteClient` sits idle
/// across many status intervals, stays connected on the edge's `STATUS` traffic
/// alone, and is still usable afterwards.
#[test]
fn an_idle_remote_client_is_kept_alive_by_status_frames() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = ServiceBuilder::new(
        ServiceConfig::new(&dir, common::APP),
        Sessioned::new(RegisterSm::default(), SessionConfig::default()),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);
    let edge = Edge::start(edge_config(&dir)).unwrap();

    let client = RemoteClient::connect(RemoteConfig {
        app_id: common::APP.into(),
        members: vec![edge.local_addr().to_string()],
        request_timeout: Duration::from_secs(20),
        ..Default::default()
    })
    .unwrap();
    assert!(client.stats().max_credits_seen > 0, "HELLO_OK must grant a non-zero window");

    std::thread::sleep(STATUS_INTERVAL * 8);
    assert!(edge.stats().status_frames >= 1, "the idle timer never fired: {:?}", edge.stats());
    assert!(client.is_connected(), "the client failed over despite the edge's STATUS traffic");

    // Still usable after the idle stretch.
    let cmd = bincode::serde::encode_to_vec(
        uc_lincheck::register::Cmd::Write(5),
        bincode::config::standard(),
    )
    .unwrap();
    assert!(client.submit(&cmd).unwrap().wait().is_ok());

    client.shutdown();
    edge.stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}

/// Ruling 7: an edge whose node restarted underneath it takes itself out of
/// service *visibly*. Without this a client loops forever — handshake ok,
/// first SUBMIT faults, `LEADER_CHANGED{unknown}`, reconnect, repeat.
#[test]
fn a_faulted_edge_refuses_new_handshakes_instead_of_livelocking() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = ServiceBuilder::new(
        ServiceConfig::new(&dir, common::APP),
        Sessioned::new(RegisterSm::default(), SessionConfig::default()),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);
    let edge = Edge::start(edge_config(&dir)).unwrap();
    assert!(!edge.is_faulted());

    // A client connected before the fault is closed out, not left hanging.
    let before = RemoteClient::connect(RemoteConfig {
        app_id: common::APP.into(),
        members: vec![edge.local_addr().to_string()],
        request_timeout: Duration::from_secs(2),
        ..Default::default()
    })
    .unwrap();

    edge.fault_for_tests();
    assert!(edge.is_faulted(), "the fault must latch");

    // A fresh dial is refused by name. `RemoteClient` treats a refusal as
    // terminal for the whole dial rather than moving on — correct here (one
    // member), and the point is that it is an ERROR, not a silent retry loop.
    let err = RemoteClient::connect(RemoteConfig {
        app_id: common::APP.into(),
        members: vec![edge.local_addr().to_string()],
        connect_timeout: Duration::from_secs(2),
        ..Default::default()
    })
    .expect_err("a faulted edge must not accept a new client");
    match err {
        RemoteError::HelloRefused { reason, ref detail } => {
            assert_eq!(reason, HELLO_REFUSED_FAULTED, "detail: {detail}");
            assert!(detail.contains("faulted"), "the refusal must say why: {detail}");
        }
        RemoteError::NoMembersReachable => {}
        other => panic!("unexpected dial outcome: {other:?}"),
    }

    // Whatever the pre-existing client does next, it must not hang: the socket
    // was closed under it, so its own reconnect loop hits the same refusal.
    let cmd = bincode::serde::encode_to_vec(
        uc_lincheck::register::Cmd::Write(1),
        bincode::config::standard(),
    )
    .unwrap();
    let _ = before.submit(&cmd).map(|t| t.wait());
    before.shutdown();

    edge.stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}

/// A wrong-cluster client must hear `APP_ID`, even on a faulted edge.
///
/// The two refusals mean opposite things to a client with several members in
/// its list: `APP_ID` is terminal everywhere (no member will answer
/// differently), while `FAULTED` says "this edge is out of service, try
/// another". Answering `FAULTED` first would send a misconfigured client round
/// the entire member list to be refused at each one — so the identity checks
/// come before the edge's own health.
#[test]
fn a_wrong_app_id_is_refused_as_app_id_even_when_the_edge_is_faulted() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = ServiceBuilder::new(
        ServiceConfig::new(&dir, common::APP),
        Sessioned::new(RegisterSm::default(), SessionConfig::default()),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);
    let edge = Edge::start(edge_config(&dir)).unwrap();
    edge.fault_for_tests();
    assert!(edge.is_faulted());

    let mut c = dial_raw(&edge);
    send_hello(&mut c, 1, "some-other-cluster");
    let (h, payload) = loop {
        match c.read_frame() {
            Ok(Some(f)) => break f,
            Ok(None) => continue,
            Err(e) => panic!("read after HELLO: {e}"),
        }
    };
    assert_eq!(h.ty, FrameType::HelloRefused);
    let r = HelloRefused::decode(&payload).unwrap();
    assert_eq!(
        r.reason,
        HELLO_REFUSED_APP_ID,
        "cluster identity is checked before the edge's own health (got reason {}, detail {:?})",
        r.reason,
        r.detail
    );
    assert_ne!(r.reason, HELLO_REFUSED_FAULTED);

    drop(c);
    edge.stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}
