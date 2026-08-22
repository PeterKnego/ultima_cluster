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

/// `RETRY{service_unavailable}` — a transient resource condition — is honoured
/// where we are: the same connection, after the hinted backoff.
#[test]
fn retry_is_honoured_with_hint() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, retry_once: true, ..Default::default() });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();

    let r = client.submit(b"abc").unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"cba");

    assert_eq!(client.stats().retries, 1);
    assert_eq!(edge.observed.seq_count(), 2, "the same seq was sent twice");
    assert_eq!(edge.observed.seq_order(), vec![1]);
    assert_eq!(client.stats().reconnects, 0, "a transient RETRY is not a reconnect");
    assert_eq!(edge.observed.conns.load(std::sync::atomic::Ordering::SeqCst), 1);
    client.shutdown();
}

/// `RETRY{not_serving}` is the opposite: a statement about the edge's ROLE.
///
/// The edge latches a connection it has refused a write on — that is what
/// keeps the SUBMITs it accepts a prefix of what was sent — so re-sending on
/// the same connection would be refused for as long as the connection lived,
/// however quickly that member became leader. The client must move.
#[test]
fn retry_not_serving_moves_the_client_rather_than_re_sending_in_place() {
    let healthy = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let not_serving =
        FakeEdge::spawn(Behaviour { credits: 2, not_serving_once: true, ..Default::default() });
    let client =
        RemoteClient::connect(cfg(vec![not_serving.addr.clone(), healthy.addr.clone()])).unwrap();

    let r = client.submit(b"abc").unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"cba");

    let s = client.stats();
    assert_eq!(s.retries, 1);
    assert!(s.reconnects >= 1, "a not-serving RETRY must move the client: {s:?}");
    assert_eq!(healthy.observed.seq_order(), vec![1], "answered somewhere else");
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

/// A fresh connection is PROBED, not flooded: exactly one SUBMIT goes out
/// until the edge answers something, however deep the pipeline behind it.
///
/// The cost model this defends: an edge that cannot serve answers EVERY submit
/// with a `REDIRECT`, and the client uses the first and discards the rest when
/// it reconnects. Flushing a 50-request window at the wrong member is 50
/// wasted frames on the way out and 50 more on the way back — per member, per
/// attempt. One probe answers the same question.
#[test]
fn a_fresh_connection_sends_one_probe_before_flushing_its_window() {
    let good = FakeEdge::spawn(Behaviour { credits: 64, ..Default::default() });
    let wrong = FakeEdge::spawn(Behaviour {
        credits: 64,
        redirect_all_to: Some(good.addr.clone()),
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![wrong.addr.clone()])).unwrap();

    // 50 deep, all admissible under the granted credits, so nothing but the
    // probe rule can be what holds them back.
    let tickets: Vec<_> = (0..50u8).map(|i| client.submit(&[i]).unwrap()).collect();
    for (i, t) in tickets.into_iter().enumerate() {
        assert_eq!(&t.wait_timeout(WAIT).unwrap().bytes[..], &[i as u8]);
    }

    assert_eq!(
        wrong.observed.seq_count(),
        1,
        "the wrong edge must see ONE submit, not the window"
    );
    assert_eq!(good.observed.seq_order(), (1..=50).collect::<Vec<u64>>(), "in order at the leader");
    client.shutdown();
}

/// An edge whose `HELLO_OK` names a different leader is left at the handshake.
///
/// The alternative — adopt the connection and let the edge redirect — costs
/// one `REDIRECT` frame per request in the pipelined window, because the whole
/// window is flushed the moment the connection is adopted. Hopping at the
/// handshake costs one extra `HELLO`.
#[test]
fn a_hello_ok_naming_another_leader_is_followed_before_anything_is_sent() {
    let b = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let a = FakeEdge::spawn(Behaviour {
        credits: 4,
        hello_ok_leader_addr: Some(b.addr.clone()),
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![a.addr.clone(), b.addr.clone()])).unwrap();

    let tickets: Vec<_> = (0..3u8).map(|i| client.submit(&[i]).unwrap()).collect();
    for (i, t) in tickets.into_iter().enumerate() {
        assert_eq!(&t.wait_timeout(WAIT).unwrap().bytes[..], &[i as u8]);
    }

    assert_eq!(a.observed.hellos.load(std::sync::atomic::Ordering::SeqCst), 1, "A was dialled");
    assert_eq!(a.observed.seq_count(), 0, "...and never sent a single request");
    assert_eq!(b.observed.seq_order(), vec![1, 2, 3], "the leader got them, in order");
    let s = client.stats();
    assert_eq!((s.redirects, s.resends), (0, 0), "no request was ever bounced: {s:?}");
    assert_eq!(client.leader().map(|(_, a)| a), Some(b.addr.clone()));
    client.shutdown();
}

/// ...but the hop is bounded: two edges that name each other must not loop.
#[test]
fn edges_that_name_each_other_as_leader_do_not_ping_pong() {
    let a = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let b = FakeEdge::spawn(Behaviour {
        credits: 2,
        hello_ok_leader_addr: Some(a.addr.clone()),
        ..Default::default()
    });
    // A names B, B names A. Whichever the client settles on, it must settle.
    let a2 = FakeEdge::spawn(Behaviour {
        credits: 2,
        hello_ok_leader_addr: Some(b.addr.clone()),
        ..Default::default()
    });
    let started = std::time::Instant::now();
    let client = RemoteClient::connect(cfg(vec![a2.addr.clone()])).unwrap();
    let r = client.submit(b"hi").unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"ih");
    assert!(started.elapsed() < Duration::from_secs(5), "took {:?}", started.elapsed());
    let _ = a;
    client.shutdown();
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
    // An EMPTY member list is a configuration mistake, not an unreachable
    // cluster, and `RemoteConfig::validate` now says so before any socket is
    // opened.
    let err = RemoteClient::connect(cfg(vec![])).unwrap_err();
    assert!(matches!(err, RemoteError::Config(_)), "got {err:?}");
}

/// `RemoteConfig::validate` refuses, by name, the four settings that cannot
/// work — before a socket is opened, so the error says what is wrong rather
/// than "the cluster is unreachable".
#[test]
fn a_config_that_cannot_work_is_refused_by_name() {
    let base = cfg(vec!["127.0.0.1:1".to_string()]);
    assert!(base.validate().is_ok(), "the baseline test config must be valid");

    let empty_app = RemoteConfig { app_id: String::new(), ..base.clone() };
    assert!(matches!(empty_app.validate(), Err(RemoteError::Config(ref m)) if m.contains("app_id")));

    let no_members = RemoteConfig { members: Vec::new(), ..base.clone() };
    assert!(
        matches!(no_members.validate(), Err(RemoteError::Config(ref m)) if m.contains("members"))
    );

    let no_window = RemoteConfig { max_inflight: 0, ..base.clone() };
    assert!(
        matches!(no_window.validate(), Err(RemoteError::Config(ref m)) if m.contains("max_inflight"))
    );

    // The liveness pair: `dead_after` at or below `ping_interval` declares a
    // healthy connection dead before its own PING could be answered.
    for dead in [Duration::from_secs(1), Duration::from_millis(500)] {
        let bad = RemoteConfig {
            ping_interval: Duration::from_secs(1),
            dead_after: dead,
            ..base.clone()
        };
        assert!(
            matches!(bad.validate(), Err(RemoteError::Config(ref m)) if m.contains("dead_after")),
            "dead_after {dead:?} vs ping_interval 1s must be refused"
        );
        // And `connect` refuses it too, without dialling anything.
        assert!(matches!(RemoteClient::connect(bad).unwrap_err(), RemoteError::Config(_)));
    }
}

/// An edge that goes silent in the MIDDLE of a frame is the same failure as
/// one that goes silent between frames — and must reach the same verdict.
///
/// Before `FramedConn::read_frame` took a `max_stall`, it did not: the reader
/// thread sat inside a half-read frame re-issuing its socket read timeout
/// forever, so its tick never ran again. No sweep, no `dead_after`, no
/// failover — every outstanding `Ticket` blocked until the process died.
#[test]
fn an_edge_that_stalls_mid_frame_is_declared_dead_and_the_request_fails_over() {
    let stalled = FakeEdge::spawn(Behaviour {
        credits: 4,
        partial_frame_then_hang: true,
        ..Default::default()
    });
    let healthy = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let client = RemoteClient::connect(RemoteConfig {
        ping_interval: Duration::from_millis(50),
        dead_after: Duration::from_millis(250),
        ..cfg(vec![stalled.addr.clone(), healthy.addr.clone()])
    })
    .unwrap();

    let started = std::time::Instant::now();
    let r = client.submit(b"lm").unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"ml", "re-sent to the healthy edge and answered");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the mid-frame stall was not bounded: took {:?}",
        started.elapsed()
    );

    assert_eq!(healthy.observed.seq_order(), vec![1]);
    let s = client.stats();
    assert!(s.reconnects >= 1, "reconnects: {}", s.reconnects);
    assert!(client.is_connected());
    client.shutdown();
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
    // thousands of frames. Each attempt is a FRESH connection (the edge latches
    // a connection it has refused a write on, so re-sending in place could
    // never be served), but the request's backoff — not the frame rate — is
    // what paces the loop, so the connections are counted in tens, not
    // thousands.
    let seen = edge.observed.seq_count();
    assert!((1..200).contains(&seen), "redirect loop sent {seen} frames");
    let conns = edge.observed.conns.load(std::sync::atomic::Ordering::SeqCst);
    assert!(conns < 200, "self-redirect loop opened {conns} connections");
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
