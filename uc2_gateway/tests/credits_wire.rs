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
//! 5. **The connection ceiling is refused, not silently dropped.** An edge at
//!    `max_connections` answers the next dial `HELLO_REFUSED{BUSY}` so a
//!    client can move to the next member, rather than closing the socket and
//!    leaving it to guess.
//! 4. **Accepted SUBMITs are a prefix of what a connection sent** — the
//!    not-serving latch. A connection told "not here" once is told it forever,
//!    even after this node wins the election; only a fresh connection is
//!    served. This one has to be driven raw: a `RemoteClient` would reconnect
//!    on the first `REDIRECT` and there would be no "same socket, later
//!    SUBMIT" to observe.

use std::net::TcpStream;
use std::time::{Duration, Instant};

use uc2_gateway::{Edge, EdgeConfig, Member};
use uc2_remote::conn::FramedConn;
use uc2_remote::frame::{
    FrameType, HELLO_REFUSED_APP_ID, HELLO_REFUSED_BUSY, HELLO_REFUSED_FAULTED, Header, Hello,
    HelloRefused, PROTOCOL_VERSION, RETRY_NOT_SERVING, Retry,
};
use uc2_remote::{RemoteClient, RemoteConfig, RemoteError};
use uc2_service::{ServiceBuilder, ServiceConfig, SessionConfig, Sessioned};
use uc_lincheck::register::RegisterSm;

mod common;

const STATUS_INTERVAL: Duration = Duration::from_millis(50);
/// Mid-frame stall budget for these raw reads. Nothing here writes a partial
/// frame, so it only bounds a wedged test.
const READ_STALL: Duration = Duration::from_secs(10);

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
        match c.read_frame(READ_STALL) {
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
        match c.read_frame(READ_STALL) {
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
        match c.read_frame(READ_STALL) {
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
        match c.read_frame(READ_STALL) {
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

/// The prefix invariant (`Conn::latch_not_serving`): once a connection has
/// been told this node cannot take writes, it is told that for every later
/// SUBMIT on the same socket — even after the node becomes a serving leader.
///
/// Why it matters is a correctness argument, not a tidiness one: `Sessioned`
/// classifies a re-sent `seq <= highest_seq` with no cached response as
/// EXPIRED. If a mid-flush role change let frames K+1..N in while 1..K were
/// refused, the client's re-send of 1..K would come back "outcome unknowable"
/// for requests that were never applied at all.
///
/// The node is booted with a multi-second election timeout so "exists but
/// cannot serve" is a window the test can walk into, not a ~50 ms race.
#[test]
fn a_connection_told_not_serving_is_never_served_later_on_the_same_socket() {
    let root = common::tempdir();
    let (node, dir) =
        common::start_single_node_with_election(root.path(), 3_000_000_000, 4_000_000_000);
    let svc = ServiceBuilder::new(
        ServiceConfig::new(&dir, common::APP),
        Sessioned::new(RegisterSm::default(), SessionConfig::default()),
    )
    .start()
    .unwrap();
    assert!(!node.can_serve(), "the long election timeout has not fired yet");
    let edge = Edge::start(edge_config(&dir)).unwrap();

    let write = bincode::serde::encode_to_vec(
        uc_lincheck::register::Cmd::Write(7),
        bincode::config::standard(),
    )
    .unwrap();
    let submit = |c: &mut FramedConn, id: u64, seq: u64| {
        c.write_frame(
            Header { ty: FrameType::Submit, flags: 0, version: PROTOCOL_VERSION, client_id: id, seq },
            &write,
        )
        .expect("write SUBMIT");
    };
    let answer = |c: &mut FramedConn| -> (FrameType, Vec<u8>) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            assert!(Instant::now() < deadline, "no answer to the SUBMIT");
            match c.read_frame(READ_STALL) {
                // Unsolicited: the idle STATUS timer, and the leader watch's
                // push when the node wins its election. Neither is an answer
                // to the SUBMIT.
                Ok(Some((h, _)))
                    if h.ty == FrameType::Status || h.ty == FrameType::LeaderChanged =>
                {
                    continue;
                }
                Ok(Some((h, p))) => return (h.ty, p.to_vec()),
                Ok(None) => continue,
                Err(e) => panic!("read: {e}"),
            }
        }
    };

    // --- 1. While the node cannot serve: refused, by name.
    let mut early = dial_raw(&edge);
    send_hello(&mut early, 0x1111, common::APP);
    assert!(read_until(&mut early, FrameType::HelloOk, Duration::from_secs(5)).is_some());
    submit(&mut early, 0x1111, 1);
    let (ty, payload) = answer(&mut early);
    assert_eq!(ty, FrameType::Retry, "one member, no leader hint yet: RETRY, not REDIRECT");
    assert_eq!(Retry::decode(&payload).unwrap().reason, RETRY_NOT_SERVING);

    // --- 2. The node becomes a serving leader.
    common::await_serving(&node, 20);

    // --- 3. The SAME socket is still refused. This is the invariant.
    submit(&mut early, 0x1111, 2);
    let (ty, _) = answer(&mut early);
    assert!(
        matches!(ty, FrameType::Retry | FrameType::Redirect),
        "a latched connection must never have a later SUBMIT accepted, got {ty:?}"
    );

    // --- 4. ...and a FRESH connection is served, so the latch is per
    // connection and the edge is not simply wedged.
    let mut fresh = dial_raw(&edge);
    send_hello(&mut fresh, 0x2222, common::APP);
    assert!(read_until(&mut fresh, FrameType::HelloOk, Duration::from_secs(5)).is_some());
    submit(&mut fresh, 0x2222, 1);
    let (ty, _) = answer(&mut fresh);
    assert_eq!(ty, FrameType::Response, "a new connection is served normally");

    drop(early);
    drop(fresh);
    edge.stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}

/// The connection ceiling: an edge already serving `max_connections` refuses
/// the next dial with `HELLO_REFUSED{BUSY}` instead of accepting it and
/// spawning another reader thread.
///
/// The refusal FRAME rather than a bare close is the point: `BUSY`, like
/// `FAULTED`, tells a multi-member client "this member is out, try the next
/// one" — a closed socket is indistinguishable from a network fault, and a
/// client would keep coming back to the same address.
#[test]
fn an_edge_at_its_connection_ceiling_refuses_the_next_client_as_busy() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = ServiceBuilder::new(
        ServiceConfig::new(&dir, common::APP),
        Sessioned::new(RegisterSm::default(), SessionConfig::default()),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);
    let edge = Edge::start(EdgeConfig { max_connections: 1, ..edge_config(&dir) }).unwrap();

    // The first client is served normally. Completing its handshake is also
    // what proves it is in the edge's connection table, so the ceiling below
    // is not a race against the acceptor.
    let mut first = dial_raw(&edge);
    send_hello(&mut first, 0x1111, common::APP);
    assert!(read_until(&mut first, FrameType::HelloOk, Duration::from_secs(5)).is_some());

    // The second is refused, by name, without a HELLO even being needed.
    let mut second = dial_raw(&edge);
    send_hello(&mut second, 0x2222, common::APP);
    let (h, payload) = loop {
        match second.read_frame(READ_STALL) {
            Ok(Some(f)) => break f,
            Ok(None) => continue,
            Err(e) => panic!("the edge closed the socket instead of refusing it: {e}"),
        }
    };
    assert_eq!(h.ty, FrameType::HelloRefused, "a connection over the ceiling must be REFUSED");
    let r = HelloRefused::decode(&payload).unwrap();
    assert_eq!(r.reason, HELLO_REFUSED_BUSY, "detail: {}", r.detail);
    assert!(r.detail.contains("max_connections"), "the refusal must say why: {}", r.detail);
    assert_eq!(edge.stats().refused_busy, 1, "stats: {:?}", edge.stats());
    assert_eq!(edge.stats().connections, 1, "a refused connection is not a connection taken on");

    // The first connection is untouched by any of it.
    drop(second);
    let mut out = Vec::new();
    Hello { app_id: common::APP }.encode(&mut out);
    first
        .write_frame(
            Header {
                ty: FrameType::Ping,
                flags: 0,
                version: PROTOCOL_VERSION,
                client_id: 0x1111,
                seq: 9,
            },
            &[],
        )
        .expect("the served connection is still usable");
    assert!(read_until(&mut first, FrameType::Pong, Duration::from_secs(5)).is_some());

    // ...and once it goes away, the ceiling reopens.
    drop(first);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut third = dial_raw(&edge);
        send_hello(&mut third, 0x3333, common::APP);
        if read_until(&mut third, FrameType::HelloOk, Duration::from_millis(500)).is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "the ceiling never reopened after a connection closed");
    }

    edge.stop();
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}
