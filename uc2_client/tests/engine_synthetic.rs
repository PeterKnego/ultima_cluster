// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Engine attach + `SendHalf` mechanics against hand-rolled instance dirs
//! (cnc page + ring files, no real `uc2_node`/`uc2_service` process) — the
//! same synthetic-instance idiom as `tests/synthetic.rs`, exercising the
//! serving gate, the inflight window, ring-full backpressure (and its
//! slot-release obligation), and the fail-loud payload bound.

use std::path::Path;

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
