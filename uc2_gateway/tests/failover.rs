// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 9 capstone: a leader dies mid-pipeline and the client keeps its
//! promise (spec §4.5).
//!
//! Three real nodes, one `Sessioned<RegisterSm>` service each, one [`Edge`]
//! each, and one `RemoteClient` pipelining through the framed TCP protocol.
//! The client is deliberately pointed at a **follower's** edge first, so the
//! very first write has to be redirected before it can commit; then the leader
//! is crash-stopped under load and the surviving two (still a quorum) elect a
//! new one.
//!
//! What is actually asserted:
//!
//! - a write submitted to a non-leader edge comes back as `REDIRECT`, not as
//!   an error, and the client follows it;
//! - every one of 200 pipelined writes resolves as `Ok` or `Err(Expired)` —
//!   **never** `Unknown` or `TimedOut`. `UNKNOWN` is the edge saying "may or
//!   may not have committed", and with `resend_on_unknown` (the default) the
//!   client is obliged to turn it into a definite answer;
//! - the register's final linearizable value is the highest `i` that was
//!   acknowledged, i.e. no acknowledged write was lost across the failover;
//! - the client saw at least one `LEADER_CHANGED` — the leader *watch*, not
//!   a reactive redirect, is what tells an idle-but-connected client that the
//!   cluster moved.
//!
//! ## Why the crashed member's edge is stopped too
//!
//! The whole host is modelled as going away. That is not just realism: a node
//! crash-stop leaves the cnc page frozen with `CAN_SERVE` still set (the flags
//! are written by agents that no longer run), so an edge left alive over a
//! dead node would happily accept submits into an ingress ring nobody drains
//! and answer them `UNKNOWN` a request-timeout later. Killing the edge with
//! the node is what keeps the client's failover bounded by an election rather
//! than by a timeout ladder.

use std::net::TcpStream;
use std::time::{Duration, Instant};

use uc2_gateway::{Edge, EdgeConfig, Member};
use uc2_remote::conn::FramedConn;
use uc2_remote::{RemoteClient, RemoteConfig, RemoteError};
use uc_lincheck::register::{Cmd, CmdResp};

mod common;

/// Writes issued across the failover.
const WRITES: u64 = 200;
/// How many are issued before the leader is killed.
const KILL_AFTER: u64 = 100;

fn enc(c: &Cmd) -> Vec<u8> {
    bincode::serde::encode_to_vec(c, bincode::config::standard()).unwrap()
}

fn dec(b: &[u8]) -> CmdResp {
    bincode::serde::decode_from_slice(b, bincode::config::standard()).unwrap().0
}

fn read_query() -> Vec<u8> {
    bincode::serde::encode_to_vec((), bincode::config::standard()).unwrap()
}

/// Start one edge per member, all sharing the same static node-id → gateway
/// map. The addresses are reserved before any edge starts (see
/// `common::free_tcp_addr`), so the map is complete from the first one.
fn start_edges(slots: &[common::Slot], gw: &[std::net::SocketAddr]) -> Vec<Option<Edge>> {
    let members: Vec<Member> = gw
        .iter()
        .enumerate()
        .map(|(i, a)| Member { node_id: i as u32, gateway: a.to_string() })
        .collect();
    slots
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let cfg = EdgeConfig {
                instance_dir: s.instance_dir.clone(),
                app_id: common::APP.into(),
                listen: gw[i],
                members: members.clone(),
                session_envelope: true,
                // Short enough that a surprise (a request stuck in a ring
                // nobody drains) shows up inside the test's budget rather
                // than as a hang.
                request_timeout: Duration::from_secs(5),
                status_interval: Duration::from_millis(100),
                ..EdgeConfig::defaults()
            };
            Some(Edge::start(cfg).expect("edge start"))
        })
        .collect()
}

#[test]
fn leader_crash_redirects_and_resend_is_deduped() {
    let started = Instant::now();
    let root = common::tempdir();
    let mut slots = common::start_cluster(root.path(), 3);
    let leader = common::await_single_leader(&slots, 20);
    let leader_id = slots[leader].id;

    let gw: Vec<std::net::SocketAddr> = (0..3).map(|_| common::free_tcp_addr()).collect();
    let mut edges = start_edges(&slots, &gw);

    // Point the client at a FOLLOWER first: `members[0]` is the address its
    // very first dial uses, so the first write is guaranteed to arrive
    // somewhere that cannot serve it.
    let follower = (0..3).find(|&i| i != leader).expect("a follower");
    let mut members: Vec<String> = vec![gw[follower].to_string()];
    members.extend((0..3).filter(|&i| i != follower).map(|i| gw[i].to_string()));
    let client = RemoteClient::connect(RemoteConfig {
        app_id: common::APP.into(),
        members,
        // Generous: one failover, plus every re-send it implies, has to fit.
        request_timeout: Duration::from_secs(30),
        ..Default::default()
    })
    .unwrap();

    // --- 1. The first write is redirected off the follower, then commits.
    let r = client.submit(&enc(&Cmd::Write(0))).unwrap().wait().unwrap();
    assert_eq!(dec(&r.bytes), CmdResp::WriteAck);
    assert!(!r.replayed, "a first-time seq is FRESH");
    let s = client.stats();
    assert!(s.redirects >= 1, "a write to a follower must be REDIRECTed: {s:?}");
    assert_eq!(
        client.leader().map(|(id, _)| id),
        Some(leader_id),
        "the REDIRECT (and the follower's HELLO_OK) name the leader"
    );
    assert!(
        edges[follower].as_ref().unwrap().stats().redirects >= 1,
        "the follower's edge produced the REDIRECT: {:?}",
        edges[follower].as_ref().unwrap().stats()
    );

    // A second client that will do NOTHING for the whole failover, parked on
    // the other survivor's edge. It is the watch's reason to exist: with no
    // request in flight it can never earn a `REDIRECT`, so the only way it can
    // learn the cluster moved is the edge telling it unprompted. Unlike the
    // busy client above — which spends the election bouncing between edges as
    // its re-sends are redirected — this one is provably connected at the
    // moment its edge's `leader_hint` changes.
    let other = (0..3).find(|&i| i != leader && i != follower).expect("a second follower");
    let idle = RemoteClient::connect(RemoteConfig {
        app_id: common::APP.into(),
        members: vec![gw[other].to_string()],
        request_timeout: Duration::from_secs(30),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(idle.stats().leader_changes, 0, "nothing has changed yet");

    // A connection that has opened a socket but not yet sent its `HELLO`. It
    // is deliberately left mid-handshake across the whole failover: the edge
    // may not write ANYTHING at it, `LEADER_CHANGED` included, because its
    // peer's dial requires the first frame to be `HELLO_OK` (see
    // `Conn::ready`). The edge's handshake budget is 5 s, so it stays
    // connected for the duration of this test.
    let mut silent = FramedConn::new(TcpStream::connect(gw[follower]).unwrap()).unwrap();
    silent.set_read_timeout(Some(Duration::from_millis(20))).unwrap();

    // --- 2. Pipeline 200 writes; kill the leader half way through.
    let mut tickets = Vec::with_capacity(WRITES as usize);
    for i in 1..=WRITES {
        if i == KILL_AFTER {
            // Host death: the edge goes with the node (see the module doc),
            // and the node goes before its service.
            if let Some(e) = edges[leader].take() {
                e.stop();
            }
            slots[leader].crash();
        }
        tickets.push(client.submit(&enc(&Cmd::Write(i))).unwrap());
    }

    // --- 3. Every ticket resolves definitely.
    let mut highest_ok = 0u64;
    let mut expired: Vec<u64> = Vec::new();
    for (n, t) in tickets.into_iter().enumerate() {
        let i = n as u64 + 1;
        match t.wait() {
            Ok(r) => {
                assert_eq!(dec(&r.bytes), CmdResp::WriteAck, "write {i}");
                highest_ok = highest_ok.max(i);
            }
            // The session window is 4096 entries against 200 writes, so this
            // is not expected — but it IS a definite outcome, and the promise
            // under test is that nothing ends in an indefinite one.
            Err(RemoteError::Expired) => expired.push(i),
            Err(e) => panic!("write {i} ended indefinitely: {e:?}"),
        }
    }
    assert!(highest_ok > KILL_AFTER, "nothing committed after the crash (highest {highest_ok})");

    // --- 4. No acknowledged write was lost: the register holds the last one
    // that was acknowledged. (Writes are monotone, so "last acknowledged" and
    // "highest acknowledged" are the same value.)
    let q = read_query();
    let r = client.query(&q, true).unwrap().wait().expect("linearizable read");
    let v: Option<u64> =
        bincode::serde::decode_from_slice(&r.bytes, bincode::config::standard()).unwrap().0;
    let v = v.expect("the register was written");
    if expired.is_empty() {
        assert_eq!(v, highest_ok, "an acknowledged write was lost across the failover");
    } else {
        // An EXPIRED write may or may not have been applied, so it is the one
        // thing that can legitimately sit above the highest acknowledged one.
        assert!(
            v == highest_ok || expired.contains(&v),
            "final value {v} is neither the highest acked ({highest_ok}) nor an expired write \
             ({expired:?})"
        );
    }

    // --- 5. The watch, not just the reactive redirect, told the client.
    let s = client.stats();
    assert!(s.leader_changes >= 1, "no LEADER_CHANGED reached the client: {s:?}");
    assert!(s.resends >= 1, "the failover must have forced a re-send: {s:?}");

    // The idle client learned about the failover without ever asking.
    let idle_stats = idle.stats();
    assert!(
        idle_stats.leader_changes >= 1,
        "the idle client was never told the leader changed: {idle_stats:?}"
    );
    assert_eq!(idle_stats.redirects, 0, "it sent nothing, so nothing could be redirected");

    let survivors: Vec<_> =
        (0..3).filter(|&i| i != leader).map(|i| edges[i].as_ref().unwrap().stats()).collect();
    assert!(
        survivors.iter().any(|s| s.leader_changes >= 1),
        "no surviving edge pushed LEADER_CHANGED: {survivors:?}"
    );
    // ...and NOT a storm. The watch is edge-triggered, so one failover is a
    // handful of frames (a couple of transitions per edge — hint lost, hint
    // regained, `can_serve` flipped — times the connections live at the time),
    // not one per poll iteration. A level-triggered watch would produce
    // thousands here.
    let pushed: u64 = survivors.iter().map(|s| s.leader_changes).sum();
    assert!(pushed <= 40, "LEADER_CHANGED storm: {pushed} frames for one failover ({survivors:?})");

    // The still-dialing connection heard nothing at all.
    match silent.read_frame() {
        Ok(None) => {}
        Ok(Some((h, _))) => {
            panic!("the edge wrote {:?} at a connection that has not handshaken", h.ty)
        }
        Err(e) => panic!("the edge dropped a still-dialing connection: {e}"),
    }
    drop(silent);

    idle.shutdown();
    client.shutdown();
    for e in edges.into_iter().flatten() {
        e.stop();
    }
    common::assert_no_gateway_threads();
    for s in slots.iter_mut() {
        s.stop();
    }
    assert!(started.elapsed() < Duration::from_secs(120), "took {:?}", started.elapsed());
}
