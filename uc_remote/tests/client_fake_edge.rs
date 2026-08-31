// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The blocking convenience client against the scripted fake edge.
//!
//! The failover behaviours themselves are pinned in `engine_fake_edge.rs`,
//! where they now live (on the link's writer/reader threads). What is left
//! here is what the CONVENIENCE layer owns: blocking admission, tickets that
//! outlive the credit window, the outcome-to-`RemoteError` mapping, and
//! shutdown.

mod common;

use std::time::{Duration, Instant};

use common::fake_edge::{Behaviour, FakeEdge};
use uc_remote::{Consistency, RemoteClient, RemoteConfig, RemoteError};

const APP: &str = "fakeapp";
const WAIT: Duration = Duration::from_secs(10);

fn cfg(members: Vec<String>) -> RemoteConfig {
    RemoteConfig {
        app_id: APP.into(),
        members,
        ..Default::default()
    }
}

#[test]
fn submit_and_wait_round_trips() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 4,
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let r = client.submit(b"abc").unwrap().wait_timeout(WAIT).unwrap();
    assert_eq!(&r.bytes[..], b"cba");
    assert_eq!(r.position, 64);
    assert!(!r.replayed);
    assert_eq!(client.leader().map(|(id, _)| id), Some(1));
    assert!(client.is_connected());
    assert_eq!(client.connected_addr(), Some(edge.addr.clone()));
    client.shutdown();
}

#[test]
fn tickets_may_outnumber_the_credit_window() {
    // The shape `uc_gateway/tests/credits.rs` and `failover.rs` rely on:
    // issue first, wait second, deeper than the grant. `submit` BLOCKS while
    // the window is closed — that block is the pacing.
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let tickets: Vec<_> = (0..20u8).map(|i| client.submit(&[i]).unwrap()).collect();
    for (i, t) in tickets.into_iter().enumerate() {
        let r = t.wait_timeout(WAIT).unwrap();
        assert_eq!(&r.bytes[..], &[i as u8]);
    }
    assert!(
        edge.observed
            .max_unanswered
            .load(std::sync::atomic::Ordering::SeqCst)
            <= 2
    );
    client.shutdown();
}

#[test]
fn query_round_trips_with_both_consistencies() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 4,
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    for c in [Consistency::Linearizable, Consistency::Snapshot] {
        let r = client.query(b"abc", c).unwrap().wait_timeout(WAIT).unwrap();
        assert_eq!(&r.bytes[..], b"cba", "{c:?}");
    }
    client.shutdown();
}

#[test]
fn expired_surfaces_as_error() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        expired: true,
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let err = client
        .submit(b"abc")
        .unwrap()
        .wait_timeout(WAIT)
        .unwrap_err();
    assert!(matches!(err, RemoteError::Expired), "got {err:?}");
    assert_eq!(client.stats().expired, 1);
    client.shutdown();
}

#[test]
fn unknown_surfaces_when_told_not_to_resend() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        unknown_once: true,
        ..Default::default()
    });
    let client = RemoteClient::connect(RemoteConfig {
        resend_on_unknown: false,
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    let err = client
        .submit(b"abc")
        .unwrap()
        .wait_timeout(WAIT)
        .unwrap_err();
    assert!(matches!(err, RemoteError::Unknown), "got {err:?}");
    client.shutdown();
}

#[test]
fn payload_too_large_is_terminal() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        payload_too_large_once: true,
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let err = client
        .submit(b"abc")
        .unwrap()
        .wait_timeout(WAIT)
        .unwrap_err();
    assert!(matches!(err, RemoteError::PayloadTooLarge), "got {err:?}");
    assert_eq!(client.stats().resends, 0);
    client.shutdown();
}

#[test]
fn shutdown_fails_outstanding_tickets_with_closed() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        delay: Duration::from_secs(30),
        ..Default::default()
    });
    let client = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let t = client.submit(b"abc").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    client.shutdown();
    let err = t.wait_timeout(Duration::from_secs(2)).unwrap_err();
    assert!(matches!(err, RemoteError::Closed), "got {err:?}");
}

#[test]
fn a_request_that_is_never_answered_times_out() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        hang: true,
        ..Default::default()
    });
    let client = RemoteClient::connect(RemoteConfig {
        request_timeout: Duration::from_millis(200),
        ping_interval: Duration::from_millis(50),
        dead_after: Duration::from_secs(30),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    let t = Instant::now();
    let err = client
        .submit(b"abc")
        .unwrap()
        .wait_timeout(Duration::from_secs(3))
        .unwrap_err();
    assert!(matches!(err, RemoteError::TimedOut), "got {err:?}");
    assert!(t.elapsed() < Duration::from_secs(2));
    client.shutdown();
}

#[test]
fn hello_refused_is_reported_and_does_not_connect() {
    let edge = FakeEdge::spawn(Behaviour {
        refuse_hello: Some(uc_remote::frame::HELLO_REFUSED_APP_ID),
        ..Default::default()
    });
    let err = RemoteClient::connect(cfg(vec![edge.addr.clone()])).unwrap_err();
    match err {
        RemoteError::HelloRefused { reason, .. } => {
            assert_eq!(reason, uc_remote::frame::HELLO_REFUSED_APP_ID)
        }
        other => panic!("expected HelloRefused, got {other:?}"),
    }
}

#[test]
fn client_handle_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    assert_send_sync::<RemoteClient>();
    assert_send_sync::<RemoteConfig>();
    assert_send::<uc_remote::Ticket>();
}
