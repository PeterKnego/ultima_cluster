// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Engine attach + `SendHalf` mechanics against hand-rolled instance dirs
//! (cnc page + ring files, no real `uc2_node`/`uc2_service` process) — the
//! same synthetic-instance idiom as `tests/synthetic.rs`, exercising the
//! serving gate, the inflight window, ring-full backpressure (and its
//! slot-release obligation), and the fail-loud payload bound.

use std::path::Path;
use std::time::Duration;

use uc2_client::{Consistency, Engine, EngineConfig, SubmitError};
use uc2_log::cnc::{CncMeta, CncPage};
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
    let a = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    a ^ 0xA5A5_5A5A_A5A5_5A5A_u128
}

/// Build a synthetic instance dir: cnc page + all four ring files, with
/// caller-chosen ingress/egress capacities (must be powers of two).
fn make_instance(dir: &Path, app_id: &str, ingress_cap: u64, egress_cap: u64) {
    CncPage::create_file(&dir.join("cnc2.dat"), &meta(app_id)).unwrap();
    MpscRing::create(&dir.join("ingress.ring"), ingress_cap, 128).unwrap();
    MpscRing::create(&dir.join("query.ring"), MIB, 256).unwrap();
    BroadcastRing::create(&dir.join("egress_service.broadcast"), egress_cap, 128).unwrap();
    BroadcastRing::create(&dir.join("egress_node.broadcast"), egress_cap, 128).unwrap();
}

/// Same shape as `make_instance` — explicit ingress/egress capacities — kept
/// as a distinct name for call sites that want to read as "custom caps" (a
/// tiny, permanently-full ingress ring that nothing drains).
fn make_instance_caps(dir: &Path, app_id: &str, ingress_cap: u64, egress_cap: u64) {
    make_instance(dir, app_id, ingress_cap, egress_cap);
}

fn cfg() -> EngineConfig {
    EngineConfig { serving_gate: false, ..EngineConfig::default() }
}

/// Same shape as `meta`, but with an explicit `instance_id` (modeled on
/// `tests/timeout_and_restart.rs`'s `meta`) — used by the restart test to
/// recreate the cnc page in place with a KNOWN fresh id.
fn meta_with_instance(app_id: &str, instance_id: u128) -> CncMeta {
    CncMeta { node_id: 0, instance_id, app_id: app_id.into(), buffer_bytes: MIB, max_payload: 256 }
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
        EngineConfig { serving_gate: true, ..EngineConfig::default() },
    )
    .unwrap();
    assert!(matches!(gated.try_submit(1, b"x"), Err(SubmitError::NotServing)));

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
    assert_eq!(s.inflight(), accepted, "ring-full rejections must not consume window");

    // Window-full backpressure, distinct path: window 2, roomy ring.
    let dir2 = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir2.path(), "eng-win", 1 << 20, 1 << 20);
    let (s2, _p2) = Engine::attach(
        dir2.path(),
        "eng-win",
        EngineConfig { max_inflight: 2, serving_gate: false, ..EngineConfig::default() },
    )
    .unwrap();
    s2.try_submit(1, b"a").unwrap();
    s2.try_submit(2, b"b").unwrap();
    assert!(matches!(s2.try_submit(3, b"c"), Err(SubmitError::Backpressure)));
}

#[test]
fn payload_too_large_fails_loud_at_the_door() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-big", 1 << 20, 1 << 20);
    let (s, _p) = Engine::attach(
        dir.path(),
        "eng-big",
        EngineConfig { max_payload: Some(16), serving_gate: false, ..EngineConfig::default() },
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
    s.try_query(1, b"q", Consistency::Linearizable).expect("gate off: accepted");
    s.try_query(2, b"q", Consistency::Snapshot).expect("gate off: accepted");
}

use uc_protocol::v2::ipc::{
    FLAG_V2_IS_QUERY, MSG_V2_NOT_LEADER, MSG_V2_RESPONSE, MSG_V2_RETRY, extra_client,
};

/// Collect completions into owned tuples (payload copied out of the borrow).
fn drain(poll: &mut uc2_client::PollHalf) -> Vec<(u64, Option<u64>, String)> {
    let mut out = Vec::new();
    poll.poll(|c| {
        let tag = match &c.outcome {
            uc2_client::Outcome::Response(b) => format!("resp:{}", b.len()),
            uc2_client::Outcome::NotLeader { hint } => format!("notleader:{hint:?}"),
            uc2_client::Outcome::Retry => "retry".into(),
            uc2_client::Outcome::TimedOut => "timeout".into(),
            uc2_client::Outcome::InstanceRestart { .. } => "restart".into(),
        };
        out.push((c.user_data, c.position, tag));
    });
    out
}

/// Egress producer for injecting answers into a synthetic dir.
fn egress(dir: &std::path::Path) -> uc_protocol::ring::BroadcastProducer {
    BroadcastRing::open(&dir.join("egress_service.broadcast")).unwrap().producer()
}

/// Same as `egress`, but for the NODE broadcast (`egress_node.broadcast`) —
/// where the node itself publishes NOT_LEADER/RETRY redirects (as opposed to
/// `egress_service.broadcast`, where the service publishes RESPONSE). See
/// `not_leader_via_the_node_broadcast_is_drained`.
fn egress_node(dir: &std::path::Path) -> uc_protocol::ring::BroadcastProducer {
    BroadcastRing::open(&dir.join("egress_node.broadcast")).unwrap().producer()
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
    prod.write(MSG_V2_RESPONSE, 0, extra_client(s.client_id(), 0), &payload).unwrap();
    prod.write(MSG_V2_RESPONSE, 0, extra_client(s.client_id(), 0), &payload).unwrap(); // dup

    let got = drain(&mut p);
    assert_eq!(got, vec![(0xCAFE, Some(4096), "resp:6".to_string())]);
    assert_eq!(s.stats().duplicates, 1, "second delivery counted, not re-emitted");
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
    prod.write(MSG_V2_RESPONSE, FLAG_V2_IS_QUERY, extra_client(s.client_id(), 0), &wrong).unwrap();
    assert!(drain(&mut p).is_empty(), "kind mismatch must not complete");
    assert_eq!(s.stats().kind_mismatch, 1);
    assert_eq!(s.inflight(), 1, "slot survives for the real answer");

    let mut right = 9u64.to_le_bytes().to_vec();
    right.extend_from_slice(b"ok");
    prod.write(MSG_V2_RESPONSE, 0, extra_client(s.client_id(), 0), &right).unwrap();
    assert_eq!(drain(&mut p), vec![(7, Some(9), "resp:2".to_string())]);
}

#[test]
fn not_leader_and_retry_resolve_kind_agnostic_with_hint_decode() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-nl", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(dir.path(), "eng-nl", cfg()).unwrap();
    s.try_query(1, b"q", Consistency::Linearizable).unwrap(); // wire_seq 0
    s.try_submit(2, b"c").unwrap();                            // wire_seq 1

    let mut prod = egress(dir.path());
    prod.write(MSG_V2_NOT_LEADER, 0, extra_client(s.client_id(), 0), &2u64.to_le_bytes()).unwrap();
    prod.write(MSG_V2_RETRY, 0, extra_client(s.client_id(), 1), &[]).unwrap();

    let got = drain(&mut p);
    assert_eq!(got, vec![
        (1, None, "notleader:Some(2)".to_string()),
        (2, None, "retry".to_string()),
    ]);
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
    s.try_submit(2, b"c").unwrap();                            // wire_seq 1

    let mut prod = egress_node(dir.path());
    prod.write(MSG_V2_NOT_LEADER, 0, extra_client(s.client_id(), 0), &2u64.to_le_bytes()).unwrap();
    prod.write(MSG_V2_RETRY, 0, extra_client(s.client_id(), 1), &[]).unwrap();

    let got = drain(&mut p);
    assert_eq!(got, vec![
        (1, None, "notleader:Some(2)".to_string()),
        (2, None, "retry".to_string()),
    ]);
}

#[test]
fn deadline_sweep_times_out_unanswered_requests() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-to", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(
        dir.path(), "eng-to",
        EngineConfig {
            request_timeout: Duration::from_millis(50),
            serving_gate: false,
            ..EngineConfig::default()
        },
    ).unwrap();
    s.try_submit(42, b"never answered").unwrap();
    std::thread::sleep(Duration::from_millis(80));
    // Maintenance is amortized every 64 poll cycles — loop until it fires.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let got = drain(&mut p);
        if got == vec![(42, None, "timeout".to_string())] { break; }
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
    ).unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut got = Vec::new();
    while got.len() < 2 {
        got.extend(drain(&mut p));
        assert!(std::time::Instant::now() < deadline, "restart sweep never fired");
    }
    got.sort();
    assert_eq!(got[0], (1, None, "restart".to_string()));
    assert_eq!(got[1], (2, None, "restart".to_string()));

    match s.try_submit(3, b"c") {
        Err(SubmitError::InstanceRestart { attached: a, current }) => {
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
        dir.path(), "eng-ow",
        EngineConfig {
            request_timeout: Duration::from_millis(100),
            serving_gate: false,
            ..EngineConfig::default()
        },
    ).unwrap();
    s.try_submit(5, b"lost").unwrap();

    // Lap the consumer with junk addressed to nobody.
    let mut prod = egress(dir.path());
    for _ in 0..64 {
        prod.write(MSG_V2_RESPONSE, 0, extra_client(u32::MAX, 0), &[0u8; 32]).unwrap();
    }
    let _ = drain(&mut p); // absorbs the Overwritten signal
    assert!(s.stats().overwritten >= 1, "overwrite must be counted");

    // The affected request must NOT be eagerly failed — it resolves via the
    // deadline (spec §4 item 6).
    std::thread::sleep(Duration::from_millis(150));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let got = drain(&mut p);
        if got == vec![(5, None, "timeout".to_string())] { break; }
        assert!(got.is_empty());
        assert!(std::time::Instant::now() < deadline);
    }
}

#[test]
fn wire_seq_wrap_roundtrips_through_a_real_ring() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "eng-wrap", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(
        dir.path(), "eng-wrap",
        EngineConfig { start_seq: u32::MAX as u64 - 4, serving_gate: false, ..EngineConfig::default() },
    ).unwrap();
    let mut prod = egress(dir.path());
    for i in 0..16u64 {
        let wire = (u32::MAX as u64 - 4 + i) as u32; // == seq as u32, across the wrap
        s.try_submit(i, b"w").unwrap();
        let mut payload = i.to_le_bytes().to_vec();
        payload.extend_from_slice(b"z");
        prod.write(MSG_V2_RESPONSE, 0, extra_client(s.client_id(), wire), &payload).unwrap();
        assert_eq!(drain(&mut p), vec![(i, Some(i), "resp:1".to_string())], "iteration {i}");
    }
}
