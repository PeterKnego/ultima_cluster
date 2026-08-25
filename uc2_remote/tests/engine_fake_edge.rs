// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The split client (`RemoteEngine` halves) against the scripted fake edge.
//!
//! This is the port of `client_fake_edge.rs`'s scenario suite onto the halves:
//! the behaviours the old `RemoteClient` owned now live on the writer/reader
//! threads, so they are pinned here. The scripted edge itself is unchanged.

mod common;

use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::{Duration, Instant};

use common::fake_edge::{Behaviour, FakeEdge};
use uc2_remote::{Consistency, RemoteConfig, RemoteEngine, RemoteOutcome, SubmitError};

const APP: &str = "fakeapp";
const WAIT: Duration = Duration::from_secs(10);

fn cfg(members: Vec<String>) -> RemoteConfig {
    RemoteConfig { app_id: APP.into(), members, ..Default::default() }
}

/// Drive `n` requests through the halves, returning `(user_data, position,
/// body, replayed)` in completion order. Panics on any non-`Response`
/// outcome, so a test that expects responses says so once, here.
///
/// The `Backpressure` arm is the wait strategy this crate's contract asks
/// for: a refusal is not an error, it is "the window is full — come back".
/// A caller with nothing else to do yields (here) or parks on
/// [`uc2_remote::RemoteWaitHandle`]; a bare spin is never right.
fn run_submits(
    send: &uc2_remote::RemoteSendHalf,
    poll: &mut uc2_remote::RemotePollHalf,
    n: u64,
    payload: impl Fn(u64) -> Vec<u8>,
) -> Vec<(u64, u64, Vec<u8>, bool)> {
    let mut got = Vec::new();
    let mut issued = 0u64;
    let deadline = Instant::now() + WAIT;
    while (got.len() as u64) < n && Instant::now() < deadline {
        if issued < n {
            match send.try_submit(issued, &payload(issued)) {
                Ok(()) => issued += 1,
                Err(SubmitError::Backpressure) => std::thread::yield_now(),
                Err(e) => panic!("try_submit({issued}): {e}"),
            }
        }
        poll.poll(|c| match c.outcome {
            RemoteOutcome::Response { body, replayed, expired } => {
                assert!(!expired, "unexpected EXPIRED for {}", c.user_data);
                got.push((c.user_data, c.position.unwrap_or(0), body.to_vec(), replayed));
            }
            other => panic!("unexpected outcome for {}: {other:?}", c.user_data),
        });
    }
    got
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
    // Nothing is submitted here at all: the edge answers PING with PONG and,
    // on its own timer, sends a standalone STATUS carrying a NEW absolute
    // grant. What this pins is that the reader thread applies an unsolicited
    // STATUS — the window moving with no request of ours involved.
    let edge =
        FakeEdge::spawn(Behaviour { credits: 3, shrink_credits_to: Some(1), ..Default::default() });
    let (send, _poll) = RemoteEngine::connect(RemoteConfig {
        ping_interval: Duration::from_millis(50),
        dead_after: Duration::from_millis(400),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    assert_eq!(send.credits(), 3, "HELLO_OK's grant, before any STATUS");
    assert!(
        until(|| send.credits() == 1),
        "an unsolicited STATUS must move the window: credits {} {:?}",
        send.credits(),
        send.stats()
    );
    assert!(send.is_connected(), "PING/PONG must keep an idle connection alive");
    assert_eq!(send.stats().reconnects, 0);
    assert_eq!(send.stats().max_credits_seen, 3, "max_credits_seen is a high-water mark");
    send.shutdown();
}

/// **The window is a count, not a seq range.** The edge advances `acked_seq`
/// on SUBMIT only (`Conn::claim`, `uc2_gateway/src/conn.rs`, pinned by that
/// module's own "a query never advances acked_seq" test), so a client that
/// admitted while `seq <= acked_seq + credits` would wedge permanently after
/// `credits` queries: its seqs keep climbing while the left edge never moves.
/// The real rule is the edge's own — unanswered requests of BOTH kinds must be
/// under the grant — so an unbounded run of queries flows.
#[test]
fn a_run_of_queries_never_closes_the_window() {
    const N: u64 = 6;
    let edge = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let mut issued = 0u64;
    let mut done = 0u64;
    let deadline = Instant::now() + WAIT;
    while done < N && Instant::now() < deadline {
        if issued < N {
            match send.try_query(issued, Consistency::Snapshot, b"rd") {
                Ok(()) => issued += 1,
                Err(SubmitError::Backpressure) => std::thread::yield_now(),
                Err(e) => panic!("try_query({issued}): {e}"),
            }
        }
        poll.poll(|c| match c.outcome {
            RemoteOutcome::Response { body, .. } => {
                assert_eq!(body, b"dr", "the fake edge reverses the payload");
                done += 1;
            }
            other => panic!("unexpected outcome for {}: {other:?}", c.user_data),
        });
    }
    assert_eq!(done, N, "a run of queries must not close the window");
    assert_eq!(send.acked_seq(), 0, "the edge never acks a query, and it does not have to");
    let peak = edge.observed.max_unanswered.load(AtomicOrdering::SeqCst);
    assert!((1..=2).contains(&peak), "the grant must still pace the pipeline: peak {peak}");

    // ... and a SUBMIT after that run is admitted on the same count rule, and
    // is what finally moves `acked_seq`.
    let got = run_submits(&send, &mut poll, 1, |_| b"xy".to_vec());
    assert_eq!(got.len(), 1, "a submit after a run of queries must still be admitted");
    assert!(until(|| send.acked_seq() == N + 1), "a SUBMIT is what advances acked_seq");
    send.shutdown();
}

/// A `STATUS` may carry a LOWER absolute grant at any time, and it binds new
/// admissions immediately — the wire's §6 clarification.
///
/// Measured in two phases, because the edge's `max_unanswered` is a LIFETIME
/// high-water mark: once the connection has run under a grant of 8, `peak <=
/// 8` is true however the client behaves afterwards, so asserting it proves
/// nothing about the shrink. `reset_peak` on a drained pipeline is what makes
/// phase two a statement about the NEW grant — and `credits() == 1` on its own
/// would only prove the word was updated, not that admission tightened.
#[test]
fn a_status_carrying_a_lower_grant_is_honoured_for_new_seqs() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 8,
        shrink_credits_to: Some(1),
        // Long enough that the submitter is always ahead of the answers, so
        // phase one's depth is about the grant and not about scheduling.
        delay: Duration::from_millis(5),
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();

    // --- phase one: the initial grant of 8, and a pipeline that uses it.
    let got = run_submits(&send, &mut poll, 8, |i| vec![i as u8]);
    assert_eq!(got.len(), 8);
    let pre = edge.observed.max_unanswered.load(AtomicOrdering::SeqCst);
    assert!(pre > 1, "a grant of 8 must pipeline, or phase two proves nothing: peak {pre}");
    assert!(pre <= 8, "and it must never exceed the grant it was given: peak {pre}");

    // --- the shrink lands, and the pipeline drains under it.
    assert!(
        until(|| send.credits() == 1 && send.inflight() == 0),
        "the STATUS must be applied and the window drained: credits {}, inflight {}",
        send.credits(),
        send.inflight()
    );
    // Nothing is outstanding at either end, so the next peak is entirely
    // about what the reduced grant admits.
    edge.observed.reset_peak();

    // --- phase two: the SAME submitter loop, now under a grant of one.
    let got = run_submits(&send, &mut poll, 6, |i| vec![i as u8]);
    assert_eq!(got.len(), 6, "the burst must still complete, one at a time, as completions drain");
    let post = edge.observed.max_unanswered.load(AtomicOrdering::SeqCst);
    assert_eq!(post, 1, "a reduced grant must bind admission, not just the reported window");
    assert_eq!(send.credits(), 1, "the last absolute grant seen is the window");
    assert_eq!(send.stats().max_credits_seen, 8, "max_credits_seen is a high-water mark");
    send.shutdown();
}

/// A grant of one is a serial pipeline: exactly one unanswered request at a
/// time, however fast the submitter offers work.
#[test]
fn a_window_of_one_serializes_the_pipeline() {
    let edge = FakeEdge::spawn(Behaviour { credits: 1, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 5, |i| vec![i as u8]);
    assert_eq!(got.len(), 5);
    assert_eq!(
        edge.observed.max_unanswered.load(AtomicOrdering::SeqCst),
        1,
        "credits: 1 means exactly one unanswered request"
    );
    send.shutdown();
}

/// `max_inflight` is the caller's own cap, applied on top of whatever the edge
/// grants — a client that wants a shallower pipeline than the edge offers gets
/// one.
#[test]
fn the_local_inflight_cap_binds_below_the_edges_grant() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 64,
        delay: Duration::from_millis(50),
        ..Default::default()
    });
    let (send, _poll) =
        RemoteEngine::connect(RemoteConfig { max_inflight: 3, ..cfg(vec![edge.addr.clone()]) })
            .unwrap();
    for i in 0..3u64 {
        assert!(send.try_submit(i, b"x").is_ok(), "request {i} fits the local cap");
    }
    assert_eq!(send.try_submit(3, b"x"), Err(SubmitError::Backpressure));
    assert_eq!(send.inflight(), 3);
    send.shutdown();
}

/// The port of `client_fake_edge.rs`'s `submit_pipelined_under_credits`
/// (23–50) onto the halves.
#[test]
fn try_submit_pipelines_under_credits() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 6, |i| vec![i as u8]);
    assert_eq!(got.len(), 6, "every accepted request must complete exactly once");
    for (i, (ud, pos, body, replayed)) in got.iter().enumerate() {
        let i = i as u64;
        assert_eq!(*ud, i, "completions arrive in issue order under one connection");
        assert_eq!(*pos, (i + 1) * 64, "the edge's position rides the completion");
        assert_eq!(body.as_slice(), &[i as u8], "the fake edge reverses the payload");
        assert!(!replayed, "a first-time seq is FRESH");
    }
    let peak = edge.observed.max_unanswered.load(AtomicOrdering::SeqCst);
    assert!((1..=2).contains(&peak), "the credit window must pace the pipeline: peak {peak}");
    assert_eq!(edge.observed.seq_order(), vec![1, 2, 3, 4, 5, 6], "seqs start at 1, gap-free");
    assert_eq!(send.inflight(), 0, "the window is empty once everything completed");
    // The whole point of M13b: many frames per socket write once the window is
    // wide enough to hold more than one. A grant of 2 cannot prove the ratio,
    // so what is pinned here is that the numerator is now counted at all —
    // task 7's window scenarios are where the batching factor is asserted.
    // `frames_written` is stamped by the writer thread AFTER the socket write
    // and the `consume` that the response (and therefore the completion this
    // loop just observed) rides on — so the main thread can legitimately read
    // the stats while the writer is still inside that window. Wait for the
    // counter rather than racing it; the wait is what makes the equality
    // below a statement about accounting instead of about scheduling.
    assert!(
        until(|| send.stats().frames_written >= 6),
        "every frame written must be counted: {:?}",
        send.stats()
    );
    let s = send.stats();
    assert_eq!(
        s.frames_written,
        edge.observed.seq_count() as u64,
        "the frame counter must match what the edge actually received: {s:?}"
    );
    assert_eq!((s.reconnects, s.resends, s.retries, s.redirects), (0, 0, 0, 0), "{s:?}");
    send.shutdown();
}

/// The port of `query_round_trips_and_carries_the_linearizable_flag`
/// (150–160). Both consistencies ride the same frame type and differ only in
/// `FLAG_LINEARIZABLE`, which the edge echoes by answering identically — so
/// what this pins is that neither flag path drops the completion.
#[test]
fn try_query_round_trips_both_consistencies() {
    let edge = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    for (i, c) in [Consistency::Linearizable, Consistency::Snapshot].into_iter().enumerate() {
        send.try_query(i as u64, c, b"abc").unwrap();
        let mut body = None;
        let deadline = Instant::now() + WAIT;
        while body.is_none() && Instant::now() < deadline {
            poll.poll(|comp| {
                if let RemoteOutcome::Response { body: b, .. } = comp.outcome {
                    assert_eq!(comp.user_data, i as u64);
                    body = Some(b.to_vec());
                }
            });
        }
        assert_eq!(body.expect("query answered").as_slice(), b"cba", "{c:?}");
    }
    send.shutdown();
}

#[test]
fn a_payload_larger_than_the_ring_is_refused_at_the_door() {
    let edge = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let (send, _poll) = RemoteEngine::connect(RemoteConfig {
        out_ring_bytes: Some(8192),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    let too_big = vec![0u8; 16 * 1024];
    assert_eq!(send.try_submit(1, &too_big), Err(SubmitError::PayloadTooLarge));
    // ... and a merely large one still goes on the wire: the node, not the
    // client, is the authority on `max_payload` (see roundtrip.rs).
    assert!(send.try_submit(2, &vec![0u8; 4096]).is_ok());
    send.shutdown();
}

/// The reclaim path, which nothing else here reaches: a ring far too small to
/// hold the run forces it to wrap many times, so every accepted request
/// depends on the submitter having released the bytes of the completed ones.
/// A reclaim that under-releases wedges (the run never finishes inside
/// `WAIT`); one that over-releases corrupts a frame the writer has not sent
/// yet, and the edge's decoder — a real one — is what catches that.
///
/// `max_inflight: 8` also puts the run through the slot table's 64-index
/// floor several times over, so every seq here lands on an index a much
/// earlier seq used: the case where reading a resolved slot's extent would
/// hand back a *different* frame's offset.
#[test]
fn a_ring_far_smaller_than_the_run_wraps_and_reclaims() {
    const N: u64 = 200;
    let edge = FakeEdge::spawn(Behaviour {
        credits: 4,
        delay: Duration::from_micros(50),
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        out_ring_bytes: Some(4096),
        max_inflight: 8,
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    let got = run_submits(&send, &mut poll, N, |i| vec![(i % 251) as u8; 200]);
    assert_eq!(got.len() as u64, N, "the run must not wedge on reclaim");
    for (i, (ud, _, body, _)) in got.iter().enumerate() {
        let i = i as u64;
        assert_eq!(*ud, i);
        assert_eq!(body.as_slice(), &vec![(i % 251) as u8; 200], "frame {i} came back corrupt");
    }
    assert_eq!(
        edge.observed.seq_order(),
        (1..=N).collect::<Vec<u64>>(),
        "every seq must arrive exactly once, in order"
    );
    assert_eq!(send.inflight(), 0);
    send.shutdown();
}

/// A refused request must consume nothing: no seq, no slot, no ring bytes.
/// If it did, the next accepted request would land on a seq the edge never
/// sees a frame for and the window would wedge behind the gap.
#[test]
fn a_refused_request_consumes_no_seq() {
    let edge = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        out_ring_bytes: Some(8192),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    assert_eq!(send.try_submit(99, &vec![0u8; 16 * 1024]), Err(SubmitError::PayloadTooLarge));
    let got = run_submits(&send, &mut poll, 2, |i| vec![i as u8]);
    assert_eq!(got.len(), 2);
    assert_eq!(edge.observed.seq_order(), vec![1, 2], "the refusal must not have burnt seq 1");
    send.shutdown();
}

/// A submit after `shutdown` is refused by name rather than accepted into a
/// link that can never complete it.
#[test]
fn a_submit_after_shutdown_is_refused() {
    let edge = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let (send, _poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    send.shutdown();
    assert_eq!(send.try_submit(1, b"x"), Err(SubmitError::Closed));
    assert_eq!(send.try_query(2, Consistency::Snapshot, b"x"), Err(SubmitError::Closed));
}

/// What this pins is the LIVENESS clock, not a dropped socket: `hang` keeps
/// the connection open and silent, so the reader's `dead_after` is the only
/// thing that can notice it. The dropped-socket case is the test below.
#[test]
fn a_silent_edge_is_noticed_and_the_link_is_re_established() {
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

/// The real dropped-socket case, which only fires once something is actually
/// submitted: the edge reads one SUBMIT and closes the connection without
/// answering.
///
/// The promise is **exactly one** completion for the accepted request — and,
/// since the ordered re-send of the live window landed (task 8), that
/// completion is the real answer rather than a timeout: the request survives
/// the connection that swallowed it. `request_timeout` is the default here on
/// purpose, so nothing in the test depends on the sweep firing.
#[test]
fn a_dropped_connection_is_re_dialled_and_the_request_still_resolves_once() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        drop_after_first_request: true,
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    send.try_submit(42, b"xy").expect("the first submit fits any window");

    let mut got: Vec<(u64, String)> = Vec::new();
    let deadline = Instant::now() + WAIT;
    while got.is_empty() && Instant::now() < deadline {
        poll.poll(|c| got.push((c.user_data, format!("{:?}", c.outcome))));
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(got.len(), 1, "the accepted request must resolve exactly once: {got:?}");
    assert_eq!(got[0].0, 42);
    assert!(
        got[0].1.starts_with("Response { body: [121, 120]"),
        "the re-send must carry the answer, not a timeout: {:?}",
        got[0].1
    );
    assert!(send.stats().resends >= 1, "{:?}", send.stats());

    // The link itself recovered from the drop, whatever became of the request.
    assert!(until(|| send.stats().reconnects >= 1), "the drop must force a redial");
    assert!(until(|| edge.observed.conns.load(AtomicOrdering::SeqCst) >= 2), "a fresh connection");
    assert!(until(|| send.is_connected()), "the writer must re-establish the link");
    assert_eq!(send.inflight(), 0, "nothing is left outstanding");

    // And exactly once means exactly once: no second completion follows.
    let mut extra = 0usize;
    for _ in 0..50 {
        extra += poll.poll(|c| panic!("a second completion for {}", c.user_data));
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(extra, 0);
    send.shutdown();
}

/// Both link threads notice the same connection dying — the reader on its
/// `dead_after` clock, the writer on the next `PING` write that fails. Only
/// the first complaint may cost a reconnect: the second names a connection
/// that has already been replaced, and honouring it would shut the fresh
/// socket down and churn one reconnect per lap.
#[test]
fn a_stale_redial_does_not_churn_the_connection_that_replaced_it() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        hang_first: true,
        second_credits: Some(7),
        ..Default::default()
    });
    let (send, _poll) = RemoteEngine::connect(RemoteConfig {
        ping_interval: Duration::from_millis(30),
        dead_after: Duration::from_millis(200),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    assert_eq!(send.credits(), 2, "the first connection's grant");

    assert!(until(|| send.stats().reconnects >= 1), "the silent edge must force a redial");
    assert!(
        until(|| send.credits() == 7),
        "the grant in effect must be the second connection's: {:?}",
        send.stats()
    );

    // Long enough for a churn loop (one lap per `dead_after`) to show up.
    std::thread::sleep(Duration::from_millis(600));
    let s = send.stats();
    assert_eq!(s.reconnects, 1, "a stale redial churned the fresh connection: {s:?}");
    assert!(send.is_connected(), "the second connection must stay up: {s:?}");
    assert_eq!(send.credits(), 7, "the fresh grant must not be overwritten: {s:?}");
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

// ----------------------------------------------------------------- failover

#[test]
fn redirect_is_followed_and_the_window_is_resent_in_order() {
    let b = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let a = FakeEdge::spawn(Behaviour {
        credits: 4,
        redirect_all_to: Some(b.addr.clone()),
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![a.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 3, |i| vec![i as u8]);
    assert_eq!(got.len(), 3);
    for (i, (ud, _, body, _)) in got.iter().enumerate() {
        assert_eq!(*ud, i as u64);
        assert_eq!(body.as_slice(), &[i as u8]);
    }
    assert_eq!(b.observed.seq_order(), vec![1, 2, 3], "re-sent in seq order at the new edge");
    let s = send.stats();
    assert!(s.redirects >= 1, "redirects: {}", s.redirects);
    assert!(s.reconnects >= 1, "reconnects: {}", s.reconnects);
    assert!(s.resends >= 1, "resends: {}", s.resends);
    assert_eq!(send.leader().map(|(id, _)| id), Some(1), "leader from the new edge's HELLO_OK");
    send.shutdown();
}

#[test]
fn retry_is_honoured_in_place_after_its_hint() {
    let edge = FakeEdge::spawn(Behaviour { credits: 2, retry_once: true, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 1, |_| b"abc".to_vec());
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].2.as_slice(), b"cba");
    let s = send.stats();
    assert_eq!(s.retries, 1, "one RETRY honoured");
    assert_eq!(s.reconnects, 0, "a transient RETRY is re-sent in place, not failed over");
    assert_eq!(edge.observed.conns.load(AtomicOrdering::SeqCst), 1, "same connection");
    send.shutdown();
}

#[test]
fn retry_not_serving_moves_the_link_rather_than_re_sending_in_place() {
    let good = FakeEdge::spawn(Behaviour { credits: 2, ..Default::default() });
    let bad =
        FakeEdge::spawn(Behaviour { credits: 2, not_serving_once: true, ..Default::default() });
    let (send, mut poll) =
        RemoteEngine::connect(cfg(vec![bad.addr.clone(), good.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 1, |_| b"abc".to_vec());
    assert_eq!(got.len(), 1);
    let s = send.stats();
    assert_eq!(s.retries, 1);
    assert!(s.reconnects >= 1, "NOT_SERVING is a role statement: go somewhere else");
    send.shutdown();
}

#[test]
fn connection_loss_resends_the_unanswered_window() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        drop_after_first_request: true,
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 1, |_| b"abc".to_vec());
    assert_eq!(got.len(), 1, "the request survives the connection that dropped it");
    assert_eq!(got[0].2.as_slice(), b"cba");
    assert_eq!(edge.observed.conns.load(AtomicOrdering::SeqCst), 2, "exactly one reconnect");
    assert_eq!(edge.observed.seq_order(), vec![1], "one logical request, re-sent");
    let s = send.stats();
    assert!(s.reconnects >= 1 && s.resends >= 1, "{s:?}");
    send.shutdown();
}

#[test]
fn a_fresh_connection_sends_one_probe_before_flushing_its_window() {
    let leader = FakeEdge::spawn(Behaviour { credits: 64, ..Default::default() });
    let wrong = FakeEdge::spawn(Behaviour {
        credits: 64,
        redirect_all_to: Some(leader.addr.clone()),
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        max_inflight: 64,
        ..cfg(vec![wrong.addr.clone()])
    })
    .unwrap();
    let got = run_submits(&send, &mut poll, 50, |i| vec![i as u8]);
    assert_eq!(got.len(), 50);
    assert_eq!(
        wrong.observed.seq_count(),
        1,
        "an edge that cannot serve costs ONE frame, not the whole window"
    );
    assert_eq!(
        leader.observed.seq_order(),
        (1..=50).collect::<Vec<u64>>(),
        "the window lands at the leader, in order"
    );
    send.shutdown();
}

#[test]
fn a_hello_ok_naming_another_leader_is_followed_before_anything_is_sent() {
    let leader = FakeEdge::spawn(Behaviour { credits: 8, ..Default::default() });
    let follower = FakeEdge::spawn(Behaviour {
        credits: 8,
        hello_ok_leader_addr: Some(leader.addr.clone()),
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![follower.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 3, |i| vec![i as u8]);
    assert_eq!(got.len(), 3);
    assert_eq!(follower.observed.hellos.load(AtomicOrdering::SeqCst), 1, "dialled once");
    assert_eq!(follower.observed.seq_count(), 0, "and never sent a request");
    assert_eq!(leader.observed.seq_order(), vec![1, 2, 3]);
    let s = send.stats();
    assert_eq!(s.redirects, 0, "the hop happens at the handshake, not by REDIRECT");
    assert_eq!(s.resends, 0, "nothing was ever sent to the wrong edge");
    send.shutdown();
}

#[test]
fn edges_that_name_each_other_as_leader_do_not_ping_pong() {
    let a = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let b = FakeEdge::spawn(Behaviour {
        credits: 4,
        hello_ok_leader_addr: Some(a.addr.clone()),
        ..Default::default()
    });
    // `a` names `b`, `b` names `a`: the hop budget must settle it.
    let t = Instant::now();
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![b.addr.clone()])).unwrap();
    let got = run_submits(&send, &mut poll, 1, |_| b"abc".to_vec());
    assert_eq!(got.len(), 1);
    assert!(t.elapsed() < Duration::from_secs(5), "the handshake hop must be bounded");
    send.shutdown();
}

#[test]
fn payload_too_large_is_terminal_and_never_resent() {
    let edge = FakeEdge::spawn(Behaviour {
        credits: 2,
        payload_too_large_once: true,
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    send.try_submit(7, b"abc").unwrap();
    let mut outcome = None;
    let deadline = Instant::now() + WAIT;
    while outcome.is_none() && Instant::now() < deadline {
        poll.poll(|c| {
            assert_eq!(c.user_data, 7);
            outcome = Some(matches!(c.outcome, RemoteOutcome::PayloadTooLarge));
        });
    }
    assert_eq!(outcome, Some(true), "RETRY{{PAYLOAD_TOO_LARGE}} is a terminal outcome");
    assert_eq!(edge.observed.seq_count(), 1, "seen exactly once on the wire");
    assert_eq!(send.stats().resends, 0);
    send.shutdown();
}

#[test]
fn an_edge_that_redirects_to_itself_does_not_wedge_or_spin() {
    let edge =
        FakeEdge::spawn(Behaviour { credits: 4, redirect_to_self: true, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(RemoteConfig {
        request_timeout: Duration::from_millis(500),
        ..cfg(vec![edge.addr.clone()])
    })
    .unwrap();
    send.try_submit(1, b"abc").unwrap();
    let t = Instant::now();
    let mut timed_out = false;
    while !timed_out && t.elapsed() < Duration::from_secs(3) {
        poll.poll(|c| timed_out = matches!(c.outcome, RemoteOutcome::TimedOut));
    }
    assert!(timed_out, "an elected-but-not-serving self-redirect must still time out");
    let frames = edge.observed.seq_count();
    assert!((1..200).contains(&frames), "backed off, not spun: {frames} frames");
    let conns = edge.observed.conns.load(AtomicOrdering::SeqCst);
    assert!(conns < 200, "backed off, not spun: {conns} connections");
    send.shutdown();
}
