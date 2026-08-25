// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 8 capstone: one node, one service, one [`Edge`], one `RemoteClient`.
//!
//! The whole M12a stack end to end — writes, a CAS, a linearizable read and a
//! snapshot read all crossing the framed TCP protocol, the edge's
//! `Engine` hop and the node's commit pipeline — in **both** envelope modes:
//!
//! 1. `session_envelope = true` (the default) over `Sessioned<RegisterSm>`:
//!    the edge prepends the 16-byte `client_id ++ seq` header to SUBMITs and
//!    lifts the `Sessioned` tag off the response into `RESPONSE` flags, so the
//!    client sees `replayed = false` for a first-time write.
//! 2. `session_envelope = false` over a plain `RegisterSm`: raw pass-through,
//!    no header, no tag, no flags — Aeron parity.

use std::time::Duration;

use uc2_gateway::{Edge, EdgeConfig, Member};
use uc2_remote::{Consistency, RemoteClient, RemoteConfig};
use uc2_service::{ServiceBuilder, ServiceConfig, SessionConfig, Sessioned};
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};

mod common;

fn enc(c: &Cmd) -> Vec<u8> {
    bincode::serde::encode_to_vec(c, bincode::config::standard()).unwrap()
}

fn dec(b: &[u8]) -> CmdResp {
    bincode::serde::decode_from_slice(b, bincode::config::standard()).unwrap().0
}

/// `RegisterSm::Query` is the unit type (`Read` is the only question there is),
/// so the query payload is bincode of `()` — an empty byte string.
fn read_query() -> Vec<u8> {
    bincode::serde::encode_to_vec((), bincode::config::standard()).unwrap()
}

fn edge_config(dir: &std::path::Path, envelope: bool) -> EdgeConfig {
    EdgeConfig {
        instance_dir: dir.to_path_buf(),
        app_id: common::APP.into(),
        listen: "127.0.0.1:0".parse().unwrap(),
        members: vec![Member { node_id: 0, gateway: "127.0.0.1:0".into() }],
        session_envelope: envelope,
        ..EdgeConfig::defaults()
    }
}

fn remote_config(edge: &Edge) -> RemoteConfig {
    RemoteConfig {
        app_id: common::APP.into(),
        members: vec![edge.local_addr().to_string()],
        request_timeout: Duration::from_secs(20),
        ..Default::default()
    }
}

#[test]
fn write_cas_read_round_trip_through_the_edge() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = ServiceBuilder::new(
        ServiceConfig::new(&dir, common::APP),
        Sessioned::new(RegisterSm::default(), SessionConfig::default()),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);

    let edge = Edge::start(edge_config(&dir, true)).unwrap();
    let client = RemoteClient::connect(remote_config(&edge)).unwrap();

    let r = client.submit(&enc(&Cmd::Write(7))).unwrap().wait().unwrap();
    assert_eq!(dec(&r.bytes), CmdResp::WriteAck);
    assert!(!r.replayed, "a first-time seq is FRESH, never a replay");
    assert!(r.position > 0, "a committed write reports its log position");

    let r = client.submit(&enc(&Cmd::Cas { old: 7, new: 8 })).unwrap().wait().unwrap();
    assert_eq!(dec(&r.bytes), CmdResp::CasResult(true));
    assert!(!r.replayed);

    let q = read_query();
    let r = client.query(&q, Consistency::Linearizable).unwrap().wait().unwrap();
    let v: Option<u64> = bincode::serde::decode_from_slice(&r.bytes, bincode::config::standard())
        .unwrap()
        .0;
    assert_eq!(v, Some(8), "the linearizable read sees the CAS");

    let r = client.query(&q, Consistency::Snapshot).unwrap().wait().unwrap();
    let v: Option<u64> = bincode::serde::decode_from_slice(&r.bytes, bincode::config::standard())
        .unwrap()
        .0;
    assert_eq!(v, Some(8), "the snapshot read sees it too (single member)");

    let s = edge.stats();
    assert_eq!((s.submits, s.queries), (2, 2), "stats: {s:?}");
    assert_eq!(s.responses, 4, "every request ended in exactly one RESPONSE: {s:?}");
    assert_eq!(s.connections, 1);
    assert_eq!((s.redirects, s.retries, s.unknown), (0, 0, 0), "a healthy leader: {s:?}");

    client.shutdown();
    edge.stop();
    // `Edge::stop` joins every thread it started — hold it to that.
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}

#[test]
fn raw_pass_through_round_trips_with_the_envelope_off() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = ServiceBuilder::new(ServiceConfig::new(&dir, common::APP), RegisterSm::default())
        .start()
        .unwrap();
    common::await_serving(&node, 10);

    // No `Sessioned` wrapper on the service, and no 16-byte header from the
    // edge: the command bytes reach `apply` exactly as the client wrote them.
    let edge = Edge::start(edge_config(&dir, false)).unwrap();
    let client = RemoteClient::connect(remote_config(&edge)).unwrap();

    let r = client.submit(&enc(&Cmd::Write(42))).unwrap().wait().unwrap();
    assert_eq!(dec(&r.bytes), CmdResp::WriteAck);
    assert!(!r.replayed, "raw pass-through never sets FLAG_REPLAYED");

    let r = client.submit(&enc(&Cmd::Cas { old: 1, new: 2 })).unwrap().wait().unwrap();
    assert_eq!(dec(&r.bytes), CmdResp::CasResult(false), "CAS against the wrong old value fails");
    assert!(!r.replayed);

    let r = client.query(&read_query(), Consistency::Linearizable).unwrap().wait().unwrap();
    let v: Option<u64> = bincode::serde::decode_from_slice(&r.bytes, bincode::config::standard())
        .unwrap()
        .0;
    assert_eq!(v, Some(42));

    let s = edge.stats();
    assert_eq!((s.submits, s.queries, s.responses), (2, 1, 3), "stats: {s:?}");

    client.shutdown();
    edge.stop();
    // `Edge::stop` joins every thread it started — hold it to that.
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}

/// A `SUBMIT` bigger than the node's `max_payload` is refused at the edge with
/// `RETRY{PAYLOAD_TOO_LARGE}` — a terminal error for the client — and never
/// reaches the ingress ring.
#[test]
fn an_oversized_submit_is_refused_with_payload_too_large() {
    let root = common::tempdir();
    let (node, dir) = common::start_single_node(root.path());
    let svc = ServiceBuilder::new(
        ServiceConfig::new(&dir, common::APP),
        Sessioned::new(RegisterSm::default(), SessionConfig::default()),
    )
    .start()
    .unwrap();
    common::await_serving(&node, 10);

    let edge = Edge::start(edge_config(&dir, true)).unwrap();
    let client = RemoteClient::connect(remote_config(&edge)).unwrap();

    // The rig's node caps `max_payload` at 256 bytes.
    let big = vec![0u8; 4096];
    let err = client.submit(&big).unwrap().wait().unwrap_err();
    assert!(
        matches!(err, uc2_remote::RemoteError::PayloadTooLarge),
        "expected a terminal PayloadTooLarge, got {err:?}"
    );

    // The connection survives it: a normal write still round-trips.
    let r = client.submit(&enc(&Cmd::Write(3))).unwrap().wait().unwrap();
    assert_eq!(dec(&r.bytes), CmdResp::WriteAck);

    let s = edge.stats();
    assert_eq!(s.submits, 1, "the oversized frame never reached the ring: {s:?}");
    assert_eq!(s.retries, 1, "exactly one RETRY frame: {s:?}");

    client.shutdown();
    edge.stop();
    // `Edge::stop` joins every thread it started — hold it to that.
    common::assert_no_gateway_threads();
    node.stop();
    svc.stop();
}
