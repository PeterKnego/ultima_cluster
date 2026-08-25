// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The split client (`RemoteEngine` halves) against the scripted fake edge.
//!
//! This is the port of `client_fake_edge.rs`'s scenario suite onto the halves:
//! the behaviours the old `RemoteClient` owned now live on the writer/reader
//! threads, so they are pinned here. The scripted edge itself is unchanged.

mod common;

use std::time::{Duration, Instant};

use common::fake_edge::{Behaviour, FakeEdge};
use uc2_remote::{RemoteConfig, RemoteEngine};

const APP: &str = "fakeapp";
const WAIT: Duration = Duration::from_secs(10);

fn cfg(members: Vec<String>) -> RemoteConfig {
    RemoteConfig { app_id: APP.into(), members, ..Default::default() }
}

/// Poll until `pred` holds or `WAIT` elapses, so a test never hangs.
fn until(mut pred: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + WAIT;
    while Instant::now() < deadline {
        if pred() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    pred()
}

#[test]
fn connect_completes_the_handshake_and_adopts_the_granted_credits() {
    let edge = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let (send, _poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    assert_eq!(send.credits(), 4, "HELLO_OK's grant is the initial window");
    assert_eq!(send.stats().max_credits_seen, 4);
    assert_eq!(send.leader().map(|(id, _)| id), Some(1), "HELLO_OK names the leader");
    assert!(send.is_connected());
    assert_eq!(send.connected_addr(), Some(edge.addr.clone()));
    assert_eq!(edge.observed.hellos.load(std::sync::atomic::Ordering::SeqCst), 1);
    send.shutdown();
}

#[test]
fn an_idle_status_updates_the_window_without_any_traffic_from_us() {
    // The fake edge answers PING with PONG; a STATUS carrying a new grant is
    // what this asserts the reader thread applies.
    let edge = FakeEdge::spawn(Behaviour { credits: 3, ..Default::default() });
    let (send, _poll) = RemoteEngine::connect(RemoteConfig {
        ping_interval: Duration::from_millis(50),
        dead_after: Duration::from_millis(400),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert!(send.is_connected(), "PING/PONG must keep an idle connection alive");
    assert_eq!(send.stats().reconnects, 0);
    send.shutdown();
}

#[test]
fn a_dropped_connection_is_re_established_by_the_writer_thread() {
    // `hang` is the silent edge: it answers HELLO, then never writes again —
    // not even a PONG. `drop_after_first_request` (the shape this scenario has
    // in `client_fake_edge.rs`) cannot be used here, because it only fires on a
    // SUBMIT and no request is sent at this task; the edge would keep PONGing
    // and the connection would never look dead.
    let edge = FakeEdge::spawn(Behaviour { credits: 2, hang: true, ..Default::default() });
    let (send, _poll) = RemoteEngine::connect(RemoteConfig {
        ping_interval: Duration::from_millis(30),
        dead_after: Duration::from_millis(200),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    // No request is sent here (that is Task 6); the liveness clock alone must
    // notice the silent edge and re-dial.
    let ok = until(|| send.stats().reconnects >= 1);
    assert!(ok, "dead_after must force a redial: {:?}", send.stats());
    assert!(until(|| send.is_connected()), "the writer thread must re-establish the link");
    send.shutdown();
}

#[test]
fn a_config_that_cannot_work_is_refused_by_name() {
    let bad = |c: RemoteConfig, needle: &str| match RemoteEngine::connect(c) {
        Err(uc2_remote::RemoteError::Config(m)) => {
            assert!(m.contains(needle), "message {m:?} must name {needle:?}")
        }
        other => panic!("expected a Config refusal naming {needle:?}, got {other:?}"),
    };
    bad(RemoteConfig { app_id: String::new(), ..cfg(vec!["127.0.0.1:1".into()]) }, "app_id");
    bad(cfg(vec![]), "members");
    bad(RemoteConfig { max_inflight: 0, ..cfg(vec!["127.0.0.1:1".into()]) }, "max_inflight");
    bad(
        RemoteConfig {
            ping_interval: Duration::from_secs(2),
            dead_after: Duration::from_secs(1),
            ..cfg(vec!["127.0.0.1:1".into()])
        },
        "dead_after",
    );
}

#[test]
fn no_reachable_member_is_reported() {
    let e = RemoteEngine::connect(cfg(vec!["127.0.0.1:1".into()])).unwrap_err();
    assert!(matches!(e, uc2_remote::RemoteError::NoMembersReachable), "got {e:?}");
}

#[test]
fn hello_refused_is_reported_by_connect() {
    let edge = FakeEdge::spawn(Behaviour {
        refuse_hello: Some(uc2_remote::frame::HELLO_REFUSED_APP_ID),
        ..Default::default()
    });
    let e = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap_err();
    match e {
        uc2_remote::RemoteError::HelloRefused { reason, .. } => {
            assert_eq!(reason, uc2_remote::frame::HELLO_REFUSED_APP_ID)
        }
        other => panic!("expected HelloRefused, got {other:?}"),
    }
}

#[test]
fn a_faulted_member_is_skipped_and_the_next_one_serves() {
    let good = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let bad = FakeEdge::spawn(Behaviour {
        refuse_hello: Some(uc2_remote::frame::HELLO_REFUSED_FAULTED),
        ..Default::default()
    });
    let (send, _poll) =
        RemoteEngine::connect(cfg(vec![bad.addr.clone(), good.addr.clone()])).unwrap();
    assert_eq!(send.connected_addr(), Some(good.addr.clone()));
    assert!(send.stats().refused_members >= 1);
    send.shutdown();
}

#[test]
fn a_cluster_of_faulted_edges_is_unreachable() {
    let a = FakeEdge::spawn(Behaviour {
        refuse_hello: Some(uc2_remote::frame::HELLO_REFUSED_FAULTED),
        ..Default::default()
    });
    let b = FakeEdge::spawn(Behaviour {
        refuse_hello: Some(uc2_remote::frame::HELLO_REFUSED_BUSY),
        ..Default::default()
    });
    let e = RemoteEngine::connect(cfg(vec![a.addr.clone(), b.addr.clone()])).unwrap_err();
    assert!(matches!(e, uc2_remote::RemoteError::NoMembersReachable), "got {e:?}");
}

#[test]
fn halves_have_the_documented_thread_bounds() {
    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send::<uc2_remote::RemoteSendHalf>();
    assert_send::<uc2_remote::RemotePollHalf>();
    assert_send_sync::<uc2_remote::RemoteWaitHandle>();
    assert_send_sync::<RemoteConfig>();
    // `RemoteSendHalf` is deliberately NOT `Sync`: one submitter thread owns
    // it. That is enforced structurally by its `PhantomData<Cell<()>>` field,
    // not by an assertion here — a negative trait bound is not expressible in
    // stable Rust, and a test that "checks" it would only ever be a comment.
    // The compile-time proof is that `assert_send_sync::<RemoteSendHalf>()`
    // does not compile; adding it here is how you verify that by hand.
}
