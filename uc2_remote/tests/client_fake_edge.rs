// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `RemoteClient` against an in-process fake edge that speaks the real wire
//! protocol: pipelining under credits, redirect following with ordered
//! re-send, `RETRY`, connection loss, and the `Sessioned` outcomes.

use std::time::Duration;

use uc2_remote::frame::HELLO_REFUSED_FAULTED;
use uc2_remote::{RemoteClient, RemoteConfig, RemoteError};

mod common;
use common::fake_edge::{Behaviour, FakeEdge};

const APP: &str = "fakeapp";
const WAIT: Duration = Duration::from_secs(10);

fn cfg(members: Vec<String>) -> RemoteConfig {
    RemoteConfig { app_id: APP.into(), members, ..Default::default() }
}

#[test]
fn submit_pipelined_under_credits() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();

    let tickets: Vec<_> = (0..6u8)
        .map(|i| client.submit(&[i, i + 1, i + 2]).unwrap())
        .collect();
    for (i, t) in tickets.into_iter().enumerate() {
        let r = t.wait_timeout(WAIT).unwrap();
        let i = i as u8;
        assert_eq!(&r.bytes[..], &[i + 2, i + 1, i], "response bytes are the command reversed");
        assert_eq!(r.position, (i as u64 + 1) * 64);
        assert!(!r.replayed);
    }

    // The edge counts unanswered requests: the client must never exceed the
    // credits it was granted, and must have sent at least one.
    let peak = edge.observed.max_unanswered.load(std::sync::atomic::Ordering::SeqCst);
    assert!((1..=2).contains(&peak), "unanswered peak was {peak}, want 1..=2");
    assert_eq!(edge.observed.seq_order(), (1..=6).collect::<Vec<u64>>());

    let s = client.stats();
    assert_eq!(s.max_credits_seen, 2);
    assert_eq!((s.reconnects, s.resends, s.retries, s.redirects), (0, 0, 0, 0));
    assert_eq!(client.leader(), Some((1, edge.addr.clone())));
    client.shutdown();
}

#[test]
fn redirect_is_followed_and_pending_resent_in_order() {
    let b = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let a = FakeEdge::spawn(Behaviour {
        credits: 4,
        redirect_all_to: Some(b.addr.clone()),
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![a.addr.clone()])).unwrap();

    let tickets: Vec<_> = (0..3u8).map(|i| client.submit(&[i]).unwrap()).collect();
    for (i, t) in tickets.into_iter().enumerate() {
        let r = t.wait_timeout(WAIT).unwrap();
        assert_eq!(&r.bytes[..], &[i as u8]);
    }

    assert_eq!(b.observed.seq_order(), vec![1, 2, 3], "re-sent in seq order at the new edge");
    let s = client.stats();
    assert!(s.redirects >= 1, "redirects: {}", s.redirects);
    assert!(s.reconnects >= 1, "reconnects: {}", s.reconnects);
    assert!(s.resends >= 1, "resends: {}", s.resends);
    assert_eq!(client.leader().map(|(id, _)| id), Some(1), "leader from the new edge's HELLO_OK");
    client.shutdown();
}

#[test]
fn retry_is_honoured_with_hint() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, retry_once: true, ..Default::default() });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();

    let r = client.submit(b"abc").unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"cba");

    assert_eq!(client.stats().retries, 1);
    assert_eq!(edge.observed.seq_count(), 2, "the same seq was sent twice");
    assert_eq!(edge.observed.seq_order(), vec![1]);
    assert_eq!(client.stats().reconnects, 0, "a RETRY is not a reconnect");
    client.shutdown();
}

#[test]
fn connection_loss_resends_unanswered() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        drop_after_first_request: true,
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();

    let r = client.submit(b"xy").unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"yx");

    let s = client.stats();
    assert_eq!(s.reconnects, 1);
    assert!(s.resends >= 1, "resends: {}", s.resends);
    assert_eq!(edge.observed.conns.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(edge.observed.seq_order(), vec![1]);
    client.shutdown();
}

#[test]
fn expired_surfaces_as_error() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, expired: true, ..Default::default() });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();

    let err = client.submit(b"q").unwrap().wait_timeout(WAIT).unwrap_err();
    assert!(matches!(err, RemoteError::Expired), "got {err:?}");
    assert_eq!(client.stats().expired, 1);
    client.shutdown();
}

#[test]
fn query_round_trips_and_carries_the_linearizable_flag() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();

    let r = client.query(b"rd", true).unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"dr");
    let r = client.query(b"rd", false).unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"dr");
    client.shutdown();
}

#[test]
fn unknown_is_resolved_by_a_resend_or_surfaces_when_told_not_to() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, unknown_once: true, ..Default::default() });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let r = client.submit(b"ab").unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"ba");
    assert_eq!(client.stats().unknown, 1);
    assert!(client.stats().resends >= 1);
    client.shutdown();

    let edge = FakeEdge::spawn(Behaviour { credits: 2, unknown_once: true, ..Default::default() });
    let client = RemoteClient::connect(RemoteConfig {
        resend_on_unknown: false,
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    let err = client.submit(b"ab").unwrap().wait_timeout(WAIT).unwrap_err();
    assert!(matches!(err, RemoteError::Unknown), "got {err:?}");
    client.shutdown();
}

#[test]
fn payload_too_large_is_terminal_and_never_resent() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        payload_too_large_once: true,
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let err = client.submit(b"big").unwrap().wait_timeout(WAIT).unwrap_err();
    assert!(matches!(err, RemoteError::PayloadTooLarge), "got {err:?}");
    assert_eq!(edge.observed.seq_count(), 1, "never re-sent");
    assert_eq!(client.stats().resends, 0);
    client.shutdown();
}

#[test]
fn hello_refused_is_reported_and_does_not_connect() {
    let edge = FakeEdge::spawn(Behaviour { refuse_hello: Some(1), ..Default::default() });
    let err = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap_err();
    match err {
        RemoteError::HelloRefused { reason, .. } => assert_eq!(reason, 1),
        other => panic!("got {other:?}"),
    }
}

/// A `FAULTED` refusal costs one member, not the whole dial.
///
/// The two refusals mean opposite things: `APP_ID`/`VERSION` say "you are
/// dialling the wrong cluster" (every member would say the same), while
/// `FAULTED` says "this edge is out of service, its node's shmem instance
/// restarted under it" — which is exactly the situation a member list exists
/// for. Treating them alike would take a whole cluster away from a client
/// because one gateway process needed restarting.
#[test]
fn a_faulted_member_is_skipped_and_the_next_one_serves() {
    let faulted =
        FakeEdge::spawn(Behaviour { refuse_hello: Some(HELLO_REFUSED_FAULTED), ..Default::default() });
    let healthy = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let client =
        RemoteClient::connect(cfg(vec![faulted.addr.clone(), healthy.addr.clone()])).unwrap();

    let r = client.submit(b"ok").unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"ko", "the healthy member served it");
    let s = client.stats();
    assert!(s.refused_members >= 1, "the faulted member was not counted: {s:?}");
    assert_eq!(faulted.observed.hellos.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(healthy.observed.seq_order(), vec![1]);
    client.shutdown();
}

/// ...but a faulted member is still a *failure*: if every member is faulted,
/// the dial ends in `NoMembersReachable` rather than hanging or succeeding.
#[test]
fn a_cluster_of_faulted_edges_is_unreachable() {
    let a =
        FakeEdge::spawn(Behaviour { refuse_hello: Some(HELLO_REFUSED_FAULTED), ..Default::default() });
    let b =
        FakeEdge::spawn(Behaviour { refuse_hello: Some(HELLO_REFUSED_FAULTED), ..Default::default() });
    let err = RemoteClient::connect(cfg(vec![a.addr.clone(), b.addr.clone()])).unwrap_err();
    assert!(matches!(err, RemoteError::NoMembersReachable), "got {err:?}");
}

#[test]
fn no_reachable_member_is_reported() {
    // Port 1 is privileged and never listening in a test environment, so this
    // cannot collide with a port the OS handed to another test.
    let err = RemoteClient::connect(cfg(vec!["127.0.0.1:1".to_string()])).unwrap_err();
    assert!(matches!(err, RemoteError::NoMembersReachable), "got {err:?}");
    let err = RemoteClient::connect(cfg(vec![])).unwrap_err();
    assert!(matches!(err, RemoteError::NoMembersReachable), "got {err:?}");
}

#[test]
fn shutdown_fails_outstanding_tickets_with_closed() {
    // A slow edge: the ticket is still outstanding when the client shuts down.
    let edge = FakeEdge::spawn(Behaviour {
        credits: 4,
        delay: Duration::from_secs(30),
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let t = client.submit(b"z").unwrap();
    client.shutdown();
    let err = t.wait_timeout(WAIT).unwrap_err();
    assert!(matches!(err, RemoteError::Closed), "got {err:?}");
}

#[test]
fn a_request_that_is_never_answered_times_out() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 4,
        delay: Duration::from_secs(30),
        ..Default::default()
    });
    let client = RemoteClient::connect(RemoteConfig {
        request_timeout: Duration::from_millis(200),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    let err = client.submit(b"z").unwrap().wait_timeout(WAIT).unwrap_err();
    assert!(matches!(err, RemoteError::TimedOut), "got {err:?}");
    client.shutdown();
}

#[test]
fn client_handle_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    assert_send_sync::<RemoteClient>();
    assert_send_sync::<RemoteConfig>();
    // A Ticket is `Send` but not `Sync`: it can be handed to another thread,
    // but `wait` consumes it, so there is nothing to share by reference.
    assert_send::<uc2_remote::Ticket>();
}

#[test]
fn a_silent_edge_is_declared_dead_and_the_request_fails_over() {
    // The hung edge completes the handshake and then answers nothing at all —
    // not even PING — while holding the socket open. No read error ever occurs,
    // so only the liveness clock can catch it.
    let hung = FakeEdge::spawn(Behaviour { credits: 4, hang: true, ..Default::default() });
    let healthy = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let client = RemoteClient::connect(RemoteConfig {
        ping_interval: Duration::from_millis(50),
        dead_after: Duration::from_millis(250),
        ..cfg(vec![hung.addr.clone(), healthy.addr.clone()])
    })
    .unwrap();

    let r = client.submit(b"lm").unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"ml", "re-sent to the healthy edge and answered");

    assert!(hung.observed.seq_count() >= 1, "the hung edge did receive the submit");
    assert_eq!(healthy.observed.seq_order(), vec![1]);
    let s = client.stats();
    assert!(s.reconnects >= 1, "reconnects: {}", s.reconnects);
    assert!(s.resends >= 1, "resends: {}", s.resends);
    assert!(client.is_connected());
    client.shutdown();
}

#[test]
fn an_edge_that_redirects_to_itself_does_not_wedge_or_spin() {
    // "Elected but not serving": the edge's leader hint names its own node, so
    // every submit is redirected to the address we are already on. The client
    // must neither spin nor hang — request_timeout must still be enforced, on a
    // connection that never goes idle.
    let edge = FakeEdge::spawn(Behaviour {
        credits: 4,
        redirect_to_self: true,
        delay: Duration::ZERO,
        ..Default::default()
    });
    let client = RemoteClient::connect(RemoteConfig {
        request_timeout: Duration::from_millis(300),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    let started = std::time::Instant::now();
    let err = client.submit(b"z").unwrap().wait_timeout(WAIT).unwrap_err();
    assert!(matches!(err, RemoteError::TimedOut), "got {err:?}");
    assert!(started.elapsed() < Duration::from_secs(3), "took {:?}", started.elapsed());
    // Backed off rather than spun: a hot loop over a 300 ms budget would be
    // thousands of frames.
    let seen = edge.observed.seq_count();
    assert!((1..200).contains(&seen), "redirect loop sent {seen} frames");
    assert_eq!(client.stats().reconnects, 0, "a self-redirect must not reconnect");
    client.shutdown();
}

#[test]
fn ping_pong_keeps_an_idle_connection_alive() {
    // The mirror of the silent-edge test: an edge that answers PING must never
    // be declared dead, however long the client sits idle. Without PONG counting
    // as traffic, `dead_after` would churn a perfectly good connection.
    let edge = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let client = RemoteClient::connect(RemoteConfig {
        ping_interval: Duration::from_millis(30),
        dead_after: Duration::from_millis(150),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();

    // Several ping intervals and dead_after windows of pure idle.
    std::thread::sleep(Duration::from_millis(600));

    assert_eq!(client.stats().reconnects, 0, "an answered PING must not look dead");
    assert!(client.is_connected());
    assert_eq!(edge.observed.conns.load(std::sync::atomic::Ordering::SeqCst), 1);
    let r = client.submit(b"pq").unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"qp");
    client.shutdown();
}
