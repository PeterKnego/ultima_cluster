// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Hand-rolled instance dirs (cnc page + ring files, no real `uc_node`/
//! `uc_service` process) exercising client-side mechanics that a real node
//! either doesn't yet emit (`MSG_V2_RETRY` isn't wired by any component as of
//! M5 Task 10 — this is the only place it's exercised at all) or that are
//! awkward to provoke deterministically against a live, actually-draining
//! node (`BackpressureFull` needs a ring that's full and STAYS full).
//!
//! These tests only need `uc_log` + `uc_protocol` (already regular deps of
//! `uc_client`) — they build the same well-known files
//! (`uc_node::InstanceDir`'s contract: `cnc2.dat`, `ingress.ring`,
//! `query.ring`, `egress_service.broadcast`, `egress_node.broadcast`)
//! directly.

use std::path::Path;
use std::time::{Duration, Instant};

use uc_client::{Client, ClientError};
use uc_log::cnc::{CncMeta, CncPage};
use uc_protocol::ring::{BroadcastRing, MpscRing};
use uc_protocol::v2::ipc::{MSG_V2_RETRY, extra_client};

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

/// A tiny, permanently-full ingress ring (nothing ever drains it — there's no
/// node here) forces `submit`'s retry-on-Full window to expire.
#[test]
fn ingress_ring_stays_full_returns_backpressure_full() {
    let dir = tempfile::tempdir().unwrap();
    make_instance(dir.path(), "bp-test", 64, 4096);

    // Fill the tiny ingress ring completely via a raw producer — nobody ever
    // reads it (no node/service running), so it stays full forever.
    let (filler, _consumer) = MpscRing::open(&dir.path().join("ingress.ring"))
        .unwrap()
        .into_split();
    loop {
        if filler.try_write(1, 0, [0; 8], &[0u8; 8]).is_err() {
            break; // Full (or TooLarge for the last partial slot) — either way, full enough.
        }
    }

    let client = Client::connect(dir.path(), "bp-test").unwrap();
    let t0 = Instant::now();
    let result: Result<u8, ClientError> = client.submit(&7u8);
    let elapsed = t0.elapsed();

    assert!(
        matches!(result, Err(ClientError::BackpressureFull)),
        "{result:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(900),
        "must honor the ~1s retry window: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "must not hang well past the retry window: {elapsed:?}"
    );
}

/// `MSG_V2_RETRY` isn't emitted by any real node/service component yet (M5
/// Task 10 scope) — this is the only place the client's handling of it is
/// exercised end to end (the matcher unit tests in `src/matcher.rs` also
/// cover the routing logic directly, but this proves the real spawned
/// matcher thread + `Client::submit` plumbing agree).
#[test]
fn injected_retry_frame_is_delivered_as_retry_error() {
    let dir = tempfile::tempdir().unwrap();
    make_instance(dir.path(), "retry-test", MIB, 4096);

    let client = Client::connect(dir.path(), "retry-test").unwrap();
    let client_id = client.client_id();

    let handle = std::thread::spawn(move || client.submit::<u8, u8>(&1));

    // Give `submit` time to register local_seq 0 (its first call) and write
    // to the ingress ring before we inject the answer.
    std::thread::sleep(Duration::from_millis(50));

    let mut producer = BroadcastRing::open(&dir.path().join("egress_service.0.broadcast"))
        .unwrap()
        .producer();
    producer
        .write(MSG_V2_RETRY, 0, extra_client(client_id, 0), &[])
        .unwrap();

    let result = handle.join().unwrap();
    assert!(matches!(result, Err(ClientError::Retry)), "{result:?}");
}
