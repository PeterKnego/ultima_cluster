// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Engine attach + `SendHalf` mechanics against hand-rolled instance dirs
//! (cnc page + ring files, no real `uc_node`/`uc_service` process) — the
//! same synthetic-instance idiom as `tests/synthetic.rs`, exercising the
//! serving gate, the inflight window, ring-full backpressure (and its
//! slot-release obligation), and the fail-loud payload bound.

use std::path::Path;
use std::time::Duration;

use uc_client::{Consistency, Engine, EngineConfig, SubmitError};
use uc_log::cnc::{CncMeta, CncPage};
use uc_protocol::ring::{BroadcastRing, MpscRing};

const MIB: u64 = 1 << 20;

fn meta(app_id: &str) -> CncMeta {
    CncMeta {
        node_id: 0,
        instance_id: rand_u128(),
        app_id: app_id.into(),
        buffer_bytes: MIB,
        max_payload: 256,
    }
}

fn rand_u128() -> u128 {
    // Cheap, dependency-free "random enough for a test instance_id" source.
    let a = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    a ^ 0xA5A5_5A5A_A5A5_5A5A_u128
}

/// Build a synthetic instance dir: cnc page + all four ring files, with
/// caller-chosen ingress/egress capacities (must be powers of two).
fn make_instance(dir: &Path, app_id: &str, ingress_cap: u64, egress_cap: u64) {
    CncPage::create_file(&dir.join("cnc2.dat"), &meta(app_id)).unwrap();
    MpscRing::create(&dir.join("ingress.ring"), ingress_cap, 128).unwrap();
    MpscRing::create(&dir.join("query.ring"), MIB, 256).unwrap();
    BroadcastRing::create(&dir.join("egress_service.0.broadcast"), egress_cap, 128).unwrap();
    BroadcastRing::create(&dir.join("egress_node.broadcast"), egress_cap, 128).unwrap();
}

/// Same shape as `make_instance` — explicit ingress/egress capacities — kept
/// as a distinct name for call sites that want to read as "custom caps" (a
/// tiny, permanently-full ingress ring that nothing drains).
fn make_instance_caps(dir: &Path, app_id: &str, ingress_cap: u64, egress_cap: u64) {
    make_instance(dir, app_id, ingress_cap, egress_cap);
}

/// A synthetic dir whose page declares FSMs {0, 1} and has both egress rings.
fn make_instance_two_fsms(dir: &Path, app_id: &str) {
    let page = CncPage::create_file(&dir.join("cnc2.dat"), &meta(app_id)).unwrap();
    page.store_services_declared(0b11);
    MpscRing::create(&dir.join("ingress.ring"), MIB, 128).unwrap();
    MpscRing::create(&dir.join("query.ring"), MIB, 256).unwrap();
    BroadcastRing::create(&dir.join("egress_service.0.broadcast"), MIB, 128).unwrap();
    BroadcastRing::create(&dir.join("egress_service.1.broadcast"), MIB, 128).unwrap();
    BroadcastRing::create(&dir.join("egress_node.broadcast"), MIB, 128).unwrap();
}

/// Egress producer for FSM `id`'s ring.
fn egress_for(dir: &Path, id: u8) -> uc_protocol::ring::BroadcastProducer {
    BroadcastRing::open(&dir.join(format!("egress_service.{id}.broadcast")))
        .unwrap()
        .producer()
}

/// `MSG_V2_RESPONSE` payload: `position ++ body`.
fn response(position: u64, body: &[u8]) -> Vec<u8> {
    let mut p = position.to_le_bytes().to_vec();
    p.extend_from_slice(body);
    p
}

fn cfg() -> EngineConfig {
    EngineConfig {
        serving_gate: false,
        ..EngineConfig::default()
    }
}

/// Same shape as `meta`, but with an explicit `instance_id` (modeled on
/// `tests/timeout_and_restart.rs`'s `meta`) — used by the restart test to
/// recreate the cnc page in place with a KNOWN fresh id.
fn meta_with_instance(app_id: &str, instance_id: u128) -> CncMeta {
    CncMeta {
        node_id: 0,
        instance_id,
        app_id: app_id.into(),
        buffer_bytes: MIB,
        max_payload: 256,
    }
}

#[test]
fn attach_allocates_distinct_client_ids() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-attach", 1 << 20, 1 << 20);
    let (a, _pa) = Engine::attach(dir.path(), "eng-attach", cfg()).unwrap();
    let (b, _pb) = Engine::attach(dir.path(), "eng-attach", cfg()).unwrap();
    assert_ne!(a.client_id(), b.client_id());
}

#[test]
fn serving_gate_refuses_when_can_serve_is_clear() {
    // Synthetic cnc pages have flags == 0 (no node ever sets CAN_SERVE).
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-gate", 1 << 20, 1 << 20);
    let (gated, _p) = Engine::attach(
        dir.path(),
        "eng-gate",
        EngineConfig {
            serving_gate: true,
            ..EngineConfig::default()
        },
    )
    .unwrap();
    assert!(matches!(
        gated.try_submit(1, b"x"),
        Err(SubmitError::NotServing)
    ));

    let (open, _p) = Engine::attach(dir.path(), "eng-gate", cfg()).unwrap();
    open.try_submit(1, b"x").expect("gate off: accepted");
}

#[test]
fn window_full_is_backpressure_and_failed_ring_write_releases_the_slot() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    // Tiny ingress ring (64 B) that nothing drains; generous window.
    make_instance_caps(dir.path(), "eng-bp", 64, 1 << 20);
    let (s, _p) = Engine::attach(dir.path(), "eng-bp", cfg()).unwrap();
    // Fill the ring; every failed write must RELEASE its slot (inflight
    // returns to the pre-call value), so the window never leaks.
    let mut accepted = 0u64;
    loop {
        match s.try_submit(accepted, &[0u8; 8]) {
            Ok(()) => accepted += 1,
            Err(SubmitError::Backpressure) => break,
            Err(e) => panic!("{e:?}"),
        }
    }
    assert_eq!(
        s.inflight(),
        accepted,
        "ring-full rejections must not consume window"
    );

    // Window-full backpressure, distinct path: window 2, roomy ring.
    let dir2 = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir2.path(), "eng-win", 1 << 20, 1 << 20);
    let (s2, _p2) = Engine::attach(
        dir2.path(),
        "eng-win",
        EngineConfig {
            max_inflight: 2,
            serving_gate: false,
            ..EngineConfig::default()
        },
    )
    .unwrap();
    s2.try_submit(1, b"a").unwrap();
    s2.try_submit(2, b"b").unwrap();
    assert!(matches!(
        s2.try_submit(3, b"c"),
        Err(SubmitError::Backpressure)
    ));
}

#[test]
fn payload_too_large_fails_loud_at_the_door() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-big", 1 << 20, 1 << 20);
    let (s, _p) = Engine::attach(
        dir.path(),
        "eng-big",
        EngineConfig {
            max_payload: Some(16),
            serving_gate: false,
            ..EngineConfig::default()
        },
    )
    .unwrap();
    match s.try_submit(1, &[0u8; 17]) {
        Err(SubmitError::PayloadTooLarge { len: 17, max: 16 }) => {}
        other => panic!("{other:?}"),
    }
    assert_eq!(s.inflight(), 0, "refused submit must not hold a slot");
}

#[test]
fn max_payload_defaults_to_the_attached_nodes_cnc_bound() {
    // cfg().max_payload is None (inherit); the synthetic cnc's max_payload is
    // 256 (see `meta`) — a 300-byte submit must fail loud at attach-derived
    // bound, not be silently accepted (and later dropped by a real node).
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-inherit", 1 << 20, 1 << 20);
    let (s, _p) = Engine::attach(dir.path(), "eng-inherit", cfg()).unwrap();
    match s.try_submit(1, &[0u8; 300]) {
        Err(SubmitError::PayloadTooLarge { len: 300, max: 256 }) => {}
        other => panic!("{other:?}"),
    }
    assert_eq!(s.inflight(), 0, "refused submit must not hold a slot");
}

// Consistency is exercised indirectly here — try_query isn't hit by the
// backpressure/gate/payload scenarios above, so touch it once to keep the
// public surface honest against dead-code drift.
#[test]
fn try_query_is_gated_the_same_way_as_try_submit() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-query", 1 << 20, 1 << 20);
    let (s, _p) = Engine::attach(dir.path(), "eng-query", cfg()).unwrap();
    s.try_query(1, b"q", Consistency::Linearizable)
        .expect("gate off: accepted");
    s.try_query(2, b"q", Consistency::Snapshot)
        .expect("gate off: accepted");
}

use uc_protocol::v2::ipc::{
    FLAG_V2_IS_QUERY, FLAG_V2_LINEARIZABLE, MSG_V2_BAD_SERVICE, MSG_V2_NOT_LEADER, MSG_V2_QUERY,
    MSG_V2_RESPONSE, MSG_V2_RETRY, extra_client,
};

/// Collect completions into owned tuples (payload copied out of the borrow).
fn drain(poll: &mut uc_client::PollHalf) -> Vec<(u64, Option<u64>, String)> {
    let mut out = Vec::new();
    poll.poll(|c| {
        let tag = match &c.outcome {
            uc_client::Outcome::Response(b) => format!("resp:{}", b.len()),
            uc_client::Outcome::NotLeader { hint } => format!("notleader:{hint:?}"),
            uc_client::Outcome::Responses(parts) => format!(
                "responses:{:?}",
                parts
                    .iter()
                    .map(|(id, b)| (*id, String::from_utf8_lossy(b).into_owned()))
                    .collect::<Vec<_>>()
            ),
            uc_client::Outcome::Retry => "retry".into(),
            uc_client::Outcome::BadService { id } => format!("badservice:{id}"),
            uc_client::Outcome::TimedOut => "timeout".into(),
            uc_client::Outcome::InstanceRestart { .. } => "restart".into(),
        };
        out.push((c.user_data, c.position, tag));
    });
    out
}

/// Egress producer for injecting answers into a synthetic dir.
fn egress(dir: &std::path::Path) -> uc_protocol::ring::BroadcastProducer {
    BroadcastRing::open(&dir.join("egress_service.0.broadcast"))
        .unwrap()
        .producer()
}

/// Same as `egress`, but for the NODE broadcast (`egress_node.broadcast`) —
/// where the node itself publishes NOT_LEADER/RETRY redirects (as opposed to
/// `egress_service.broadcast`, where the service publishes RESPONSE). See
/// `not_leader_via_the_node_broadcast_is_drained`.
fn egress_node(dir: &std::path::Path) -> uc_protocol::ring::BroadcastProducer {
    BroadcastRing::open(&dir.join("egress_node.broadcast"))
        .unwrap()
        .producer()
}

#[test]
fn response_resolves_with_position_and_payload_and_duplicate_is_counted() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-resp", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(dir.path(), "eng-resp", cfg()).unwrap();
    s.try_submit(0xCAFE, b"cmd").unwrap();

    let mut payload = 4096u64.to_le_bytes().to_vec();
    payload.extend_from_slice(b"answer");
    // wire_seq 0: first request of a fresh engine (start_seq 0).
    let mut prod = egress(dir.path());
    prod.write(MSG_V2_RESPONSE, 0, extra_client(s.client_id(), 0), &payload)
        .unwrap();
    prod.write(MSG_V2_RESPONSE, 0, extra_client(s.client_id(), 0), &payload)
        .unwrap(); // dup

    let got = drain(&mut p);
    assert_eq!(got, vec![(0xCAFE, Some(4096), "resp:6".to_string())]);
    assert_eq!(
        s.stats().duplicates,
        1,
        "second delivery counted, not re-emitted"
    );
    assert_eq!(s.inflight(), 0);
}

#[test]
fn kind_mismatched_response_is_dropped_counted_and_the_real_answer_still_lands() {
    // T14 moved from matcher.rs: query-flagged delivery vs a Submit slot.
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-t14", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(dir.path(), "eng-t14", cfg()).unwrap();
    s.try_submit(7, b"cmd").unwrap();

    let mut wrong = 0u64.to_le_bytes().to_vec();
    wrong.extend_from_slice(b"x");
    let mut prod = egress(dir.path());
    prod.write(
        MSG_V2_RESPONSE,
        FLAG_V2_IS_QUERY,
        extra_client(s.client_id(), 0),
        &wrong,
    )
    .unwrap();
    assert!(drain(&mut p).is_empty(), "kind mismatch must not complete");
    assert_eq!(s.stats().kind_mismatch, 1);
    assert_eq!(s.inflight(), 1, "slot survives for the real answer");

    let mut right = 9u64.to_le_bytes().to_vec();
    right.extend_from_slice(b"ok");
    prod.write(MSG_V2_RESPONSE, 0, extra_client(s.client_id(), 0), &right)
        .unwrap();
    assert_eq!(drain(&mut p), vec![(7, Some(9), "resp:2".to_string())]);
}

#[test]
fn not_leader_and_retry_resolve_kind_agnostic_with_hint_decode() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-nl", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(dir.path(), "eng-nl", cfg()).unwrap();
    s.try_query(1, b"q", Consistency::Linearizable).unwrap(); // wire_seq 0
    s.try_submit(2, b"c").unwrap(); // wire_seq 1

    let mut prod = egress(dir.path());
    prod.write(
        MSG_V2_NOT_LEADER,
        0,
        extra_client(s.client_id(), 0),
        &2u64.to_le_bytes(),
    )
    .unwrap();
    prod.write(MSG_V2_RETRY, 0, extra_client(s.client_id(), 1), &[])
        .unwrap();

    let got = drain(&mut p);
    assert_eq!(
        got,
        vec![
            (1, None, "notleader:Some(2)".to_string()),
            (2, None, "retry".to_string()),
        ]
    );
}

#[test]
fn not_leader_via_the_node_broadcast_is_drained() {
    // Same shape as `not_leader_and_retry_resolve_kind_agnostic_with_hint_decode`,
    // but injected on `egress_node.broadcast` (where the real node publishes
    // NOT_LEADER/RETRY redirects) instead of `egress_service.broadcast`
    // (where the service publishes RESPONSE). `PollHalf::poll` is supposed to
    // drain BOTH rings every cycle — this pins that a dropped
    // `drain_ring(egress_node)` call would silently pass the rest of the
    // suite (every other test injects on the service ring only).
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-nl-node", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(dir.path(), "eng-nl-node", cfg()).unwrap();
    s.try_query(1, b"q", Consistency::Linearizable).unwrap(); // wire_seq 0
    s.try_submit(2, b"c").unwrap(); // wire_seq 1

    let mut prod = egress_node(dir.path());
    prod.write(
        MSG_V2_NOT_LEADER,
        0,
        extra_client(s.client_id(), 0),
        &2u64.to_le_bytes(),
    )
    .unwrap();
    prod.write(MSG_V2_RETRY, 0, extra_client(s.client_id(), 1), &[])
        .unwrap();

    let got = drain(&mut p);
    assert_eq!(
        got,
        vec![
            (1, None, "notleader:Some(2)".to_string()),
            (2, None, "retry".to_string()),
        ]
    );
}

#[test]
fn deadline_sweep_times_out_unanswered_requests() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-to", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(
        dir.path(),
        "eng-to",
        EngineConfig {
            request_timeout: Duration::from_millis(50),
            serving_gate: false,
            ..EngineConfig::default()
        },
    )
    .unwrap();
    s.try_submit(42, b"never answered").unwrap();
    std::thread::sleep(Duration::from_millis(80));
    // Maintenance is amortized every 64 poll cycles — loop until it fires.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let got = drain(&mut p);
        if got == vec![(42, None, "timeout".to_string())] {
            break;
        }
        assert!(got.is_empty(), "unexpected completions: {got:?}");
        assert!(std::time::Instant::now() < deadline, "sweep never fired");
    }
    assert_eq!(s.stats().timed_out, 1);
    assert_eq!(s.inflight(), 0, "nothing accepted may leak");
}

#[test]
fn instance_restart_fails_all_inflight_and_poisons_the_send_half() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-rs", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(dir.path(), "eng-rs", cfg()).unwrap();
    let attached = s.instance_id();
    s.try_submit(1, b"a").unwrap();
    s.try_submit(2, b"b").unwrap();

    // Recreate the cnc in place with a fresh instance_id (Node::start's boot
    // behavior; same file/inode, mmap observes the new bytes).
    CncPage::create_file(
        &dir.path().join("cnc2.dat"),
        &meta_with_instance("eng-rs", 0xDEAD_BEEF),
    )
    .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut got = Vec::new();
    while got.len() < 2 {
        got.extend(drain(&mut p));
        assert!(
            std::time::Instant::now() < deadline,
            "restart sweep never fired"
        );
    }
    got.sort();
    assert_eq!(got[0], (1, None, "restart".to_string()));
    assert_eq!(got[1], (2, None, "restart".to_string()));

    match s.try_submit(3, b"c") {
        Err(SubmitError::InstanceRestart {
            attached: a,
            current,
        }) => {
            assert_eq!(a, attached);
            assert_eq!(current, 0xDEAD_BEEF);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn broadcast_overwrite_is_a_stat_and_the_deadline_backstops_hung_requests() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    // Tiny egress broadcast: easy to lap.
    make_instance_caps(dir.path(), "eng-ow", 1 << 20, 256);
    let (s, mut p) = Engine::attach(
        dir.path(),
        "eng-ow",
        EngineConfig {
            request_timeout: Duration::from_millis(100),
            serving_gate: false,
            ..EngineConfig::default()
        },
    )
    .unwrap();
    s.try_submit(5, b"lost").unwrap();

    // Lap the consumer with junk addressed to nobody.
    let mut prod = egress(dir.path());
    for _ in 0..64 {
        prod.write(MSG_V2_RESPONSE, 0, extra_client(u32::MAX, 0), &[0u8; 32])
            .unwrap();
    }
    let _ = drain(&mut p); // absorbs the Overwritten signal
    assert!(s.stats().overwritten >= 1, "overwrite must be counted");

    // The affected request must NOT be eagerly failed — it resolves via the
    // deadline (spec §4 item 6).
    std::thread::sleep(Duration::from_millis(150));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let got = drain(&mut p);
        if got == vec![(5, None, "timeout".to_string())] {
            break;
        }
        assert!(got.is_empty());
        assert!(std::time::Instant::now() < deadline);
    }
}

#[test]
fn wire_seq_wrap_roundtrips_through_a_real_ring() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-wrap", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(
        dir.path(),
        "eng-wrap",
        EngineConfig {
            start_seq: u32::MAX as u64 - 4,
            serving_gate: false,
            ..EngineConfig::default()
        },
    )
    .unwrap();
    let mut prod = egress(dir.path());
    for i in 0..16u64 {
        let wire = (u32::MAX as u64 - 4 + i) as u32; // == seq as u32, across the wrap
        s.try_submit(i, b"w").unwrap();
        let mut payload = i.to_le_bytes().to_vec();
        payload.extend_from_slice(b"z");
        prod.write(
            MSG_V2_RESPONSE,
            0,
            extra_client(s.client_id(), wire),
            &payload,
        )
        .unwrap();
        assert_eq!(
            drain(&mut p),
            vec![(i, Some(i), "resp:1".to_string())],
            "iteration {i}"
        );
    }
}

// ---------------------------------------------------------------------------
// M14b: N egress rings, the query prefix, per-ring matching and the fan-in.
// ---------------------------------------------------------------------------

#[test]
fn attach_opens_every_declared_ring_and_default_submit_ignores_the_other_fsm() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "two");
    let (send, mut poll) = Engine::attach(dir.path(), "two", cfg()).unwrap();
    assert_eq!(send.declared(), 0b11);
    let cid = send.client_id();
    send.try_submit(1, b"x").unwrap(); // expects FSM 0 only
    // FSM 1 answers first (it is faster today): not ours, dropped and counted.
    egress_for(dir.path(), 1)
        .write(
            MSG_V2_RESPONSE,
            0,
            extra_client(cid, 0),
            &response(96, b"one"),
        )
        .unwrap();
    assert!(drain(&mut poll).is_empty());
    assert_eq!(poll.stats().wrong_ring, 1);
    egress_for(dir.path(), 0)
        .write(
            MSG_V2_RESPONSE,
            0,
            extra_client(cid, 0),
            &response(96, b"zero"),
        )
        .unwrap();
    assert_eq!(drain(&mut poll), vec![(1, Some(96), "resp:4".to_string())]);
    assert_eq!(poll.stats().responses, 1);
}

#[test]
fn submit_all_fans_in_every_declared_ring_in_id_order_whatever_the_arrival_order() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "all");
    let (send, mut poll) = Engine::attach(dir.path(), "all", cfg()).unwrap();
    let cid = send.client_id();
    send.try_submit_all(2, b"x").unwrap();
    egress_for(dir.path(), 1)
        .write(
            MSG_V2_RESPONSE,
            0,
            extra_client(cid, 0),
            &response(4096, b"b"),
        )
        .unwrap();
    assert!(
        drain(&mut poll).is_empty(),
        "one of two pieces: not complete"
    );
    assert_eq!(send.inflight(), 1);
    egress_for(dir.path(), 0)
        .write(
            MSG_V2_RESPONSE,
            0,
            extra_client(cid, 0),
            &response(4096, b"a"),
        )
        .unwrap();
    assert_eq!(
        drain(&mut poll),
        vec![(
            2,
            Some(4096),
            "responses:[(0, \"a\"), (1, \"b\")]".to_string()
        )],
        "ordered by id, not by arrival"
    );
    assert_eq!(send.inflight(), 0);
    // A late duplicate from ring 0 is a Miss (the slot is free), not a second completion.
    egress_for(dir.path(), 0)
        .write(
            MSG_V2_RESPONSE,
            0,
            extra_client(cid, 0),
            &response(4096, b"a"),
        )
        .unwrap();
    assert!(drain(&mut poll).is_empty());
    assert_eq!(poll.stats().duplicates, 1);
}

#[test]
fn submit_to_expects_only_the_named_ring() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "to");
    let (send, mut poll) = Engine::attach(dir.path(), "to", cfg()).unwrap();
    let cid = send.client_id();
    send.try_submit_to(3, 1, b"x").unwrap();
    egress_for(dir.path(), 0)
        .write(
            MSG_V2_RESPONSE,
            0,
            extra_client(cid, 0),
            &response(96, b"zero"),
        )
        .unwrap();
    assert!(drain(&mut poll).is_empty());
    assert_eq!(poll.stats().wrong_ring, 1);
    egress_for(dir.path(), 1)
        .write(
            MSG_V2_RESPONSE,
            0,
            extra_client(cid, 0),
            &response(96, b"one"),
        )
        .unwrap();
    assert_eq!(drain(&mut poll), vec![(3, Some(96), "resp:3".to_string())]);
}

#[test]
fn an_undeclared_or_out_of_range_id_is_refused_at_the_door() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "undecl");
    let (send, _poll) = Engine::attach(dir.path(), "undecl", cfg()).unwrap();
    assert!(matches!(
        send.try_submit_to(4, 2, b"x"),
        Err(SubmitError::ServiceNotDeclared {
            id: 2,
            declared: 0b11
        })
    ));
    assert!(matches!(
        send.try_query_on(4, 9, b"q", Consistency::Snapshot),
        Err(SubmitError::ServiceNotDeclared {
            id: 9,
            declared: 0b11
        })
    ));
    assert_eq!(send.inflight(), 0, "a door refusal never claims a slot");
    // A harness page (declared 0) folds to {0}: id 1 is refused, id 0 accepted.
    let dir0 = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir0.path(), "harness", MIB, MIB);
    let (send0, _p0) = Engine::attach(dir0.path(), "harness", cfg()).unwrap();
    assert_eq!(send0.declared(), 0b1);
    assert!(matches!(
        send0.try_submit_to(5, 1, b"x"),
        Err(SubmitError::ServiceNotDeclared {
            id: 1,
            declared: 0b1
        })
    ));
    send0.try_submit_to(5, 0, b"x").unwrap();
}

#[test]
fn try_query_on_writes_the_service_id_prefix_and_counts_it_toward_the_cap() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "prefix");
    let (send, _poll) = Engine::attach(
        dir.path(),
        "prefix",
        EngineConfig {
            max_payload: Some(4),
            ..cfg()
        },
    )
    .unwrap();
    let (_qp, mut qc) = MpscRing::open(&dir.path().join("query.ring"))
        .unwrap()
        .into_split();
    send.try_query_on(6, 1, b"zz", Consistency::Linearizable)
        .unwrap();
    let mut buf = Vec::new();
    let rec = qc.try_read(&mut buf).unwrap().expect("one query record");
    assert_eq!(rec.msg_type, MSG_V2_QUERY);
    assert_eq!(rec.flags, FLAG_V2_LINEARIZABLE);
    assert_eq!(buf, [1, b'z', b'z'], "service_id ++ query");
    // The cap counts the wire payload: 4 query bytes + 1 prefix = 5 > 4.
    assert!(matches!(
        send.try_query_on(7, 0, b"zzzz", Consistency::Snapshot),
        Err(SubmitError::PayloadTooLarge { len: 5, max: 4 })
    ));
    // A submit has no prefix: 4 bytes fit exactly.
    send.try_submit(8, b"zzzz").unwrap();
}

#[test]
fn bad_service_on_the_node_ring_resolves_kind_agnostic_with_the_id() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "bad");
    let (send, mut poll) = Engine::attach(dir.path(), "bad", cfg()).unwrap();
    let cid = send.client_id();
    send.try_query_on(9, 1, b"q", Consistency::Snapshot)
        .unwrap();
    egress_node(dir.path())
        .write(MSG_V2_BAD_SERVICE, 0, extra_client(cid, 0), &[1])
        .unwrap();
    assert_eq!(
        drain(&mut poll),
        vec![(9, None, "badservice:1".to_string())]
    );
    assert_eq!(poll.stats().bad_service, 1);
    assert_eq!(send.inflight(), 0);
}

#[test]
fn a_terminal_answer_ends_a_partial_fan_in_and_late_pieces_are_duplicates() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "term");
    let (send, mut poll) = Engine::attach(dir.path(), "term", cfg()).unwrap();
    let cid = send.client_id();
    send.try_submit_all(10, b"x").unwrap();
    egress_for(dir.path(), 1)
        .write(
            MSG_V2_RESPONSE,
            0,
            extra_client(cid, 0),
            &response(96, b"b"),
        )
        .unwrap();
    assert!(drain(&mut poll).is_empty());
    egress_node(dir.path())
        .write(MSG_V2_RETRY, 0, extra_client(cid, 0), &[])
        .unwrap();
    assert_eq!(drain(&mut poll), vec![(10, None, "retry".to_string())]);
    egress_for(dir.path(), 0)
        .write(
            MSG_V2_RESPONSE,
            0,
            extra_client(cid, 0),
            &response(96, b"a"),
        )
        .unwrap();
    assert!(drain(&mut poll).is_empty());
    assert_eq!(poll.stats().duplicates, 1);
}

/// Ruling A: `fan_in` is the CLAIM-TIME flag, not the mask width — a
/// `try_submit_all` on a node that declares only FSM 0 still completes as
/// `Outcome::Responses` with one piece, so a caller's match arm does not
/// depend on how many FSMs the node happens to run.
#[test]
fn submit_all_on_a_single_fsm_page_completes_as_responses_with_one_piece() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "one-fsm", MIB, MIB);
    let (send, mut poll) = Engine::attach(dir.path(), "one-fsm", cfg()).unwrap();
    assert_eq!(send.declared(), 0b1);
    let cid = send.client_id();
    send.try_submit_all(11, b"x").unwrap();
    egress_for(dir.path(), 0)
        .write(
            MSG_V2_RESPONSE,
            0,
            extra_client(cid, 0),
            &response(96, b"a"),
        )
        .unwrap();
    assert_eq!(
        drain(&mut poll),
        vec![(11, Some(96), "responses:[(0, \"a\")]".to_string())]
    );
    assert_eq!(send.inflight(), 0);
    assert_eq!(poll.stats().responses, 1);
}

/// Finding 2: `services_declared` is a raw `u64` on a shared-memory page, but
/// this client can only ring ids `< CNC_MAX_SERVICES` (8). A page declaring
/// ONLY out-of-range ids used to be stored unmasked: `attach` opened zero
/// response rings and returned `Ok`, and the first use panicked — `poll
/// .wait_handle()` indexing an empty `egress_services`, or `try_submit_all`
/// computing `expected = 0` and tripping `claim`'s assert. The M12d posture
/// is that a shared-memory page never panics an attacher: it must be a named
/// refusal instead.
#[test]
fn a_page_declaring_only_out_of_range_ids_is_refused_at_attach() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "oor", MIB, MIB);
    CncPage::open_file(&dir.path().join("cnc2.dat"), "oor")
        .unwrap()
        .store_services_declared(0x100);

    match Engine::attach(dir.path(), "oor", cfg()) {
        Err(uc_client::ClientError::ServiceNotDeclared {
            id: 0,
            declared: 0x100,
        }) => {}
        Err(other) => panic!("wrong refusal: {other:?}"),
        Ok((send, poll)) => {
            // Pre-fix reality, kept as the RED evidence: attach succeeds with
            // an empty ring set and the first use panics.
            let _ = poll.wait_handle();
            let _ = send.try_submit_all(1, b"x");
            panic!("attach must refuse a page whose declared set names only out-of-range ids");
        }
    }
}

/// The in-range bits of a mixed page still attach: `0b101 | (1 << 9)` keeps
/// FSMs {0, 2} and drops the id this client cannot ring, so `declared()` is
/// exactly the masked set (and never names a ring `attach` did not open).
#[test]
fn out_of_range_declared_bits_are_masked_off_when_something_in_range_remains() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    CncPage::create_file(&dir.path().join("cnc2.dat"), &meta("mixed")).unwrap();
    CncPage::open_file(&dir.path().join("cnc2.dat"), "mixed")
        .unwrap()
        .store_services_declared(0b101 | (1 << 9));
    MpscRing::create(&dir.path().join("ingress.ring"), MIB, 128).unwrap();
    MpscRing::create(&dir.path().join("query.ring"), MIB, 256).unwrap();
    BroadcastRing::create(&dir.path().join("egress_service.0.broadcast"), MIB, 128).unwrap();
    BroadcastRing::create(&dir.path().join("egress_service.2.broadcast"), MIB, 128).unwrap();
    BroadcastRing::create(&dir.path().join("egress_node.broadcast"), MIB, 128).unwrap();

    let (send, _poll) = Engine::attach(dir.path(), "mixed", cfg()).unwrap();
    assert_eq!(
        send.declared(),
        0b101,
        "id 9 is masked off; {{0, 2}} remain"
    );
    send.try_submit_all(1, b"x")
        .expect("the masked set is what the engine awaits");
}
