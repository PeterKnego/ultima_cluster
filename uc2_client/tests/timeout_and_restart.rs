// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `ClientError::Timeout` and `ClientError::InstanceRestart`, against a
//! synthetic instance dir (no real node/service ever answers). Sets
//! `UC2_CLIENT_TIMEOUT_MS` process-wide via `std::env::set_var`, which is
//! read fresh by every `Client::connect` (not memoized in a global) — kept in
//! its own test binary (this file) so no other `uc2_client` test sharing the
//! same process ever observes the override.

use std::path::Path;
use std::time::Duration;

use uc2_client::{Client, ClientError};
use uc2_log::cnc::{CncMeta, CncPage};
use uc_protocol::ring::{BroadcastRing, MpscRing};

const MIB: u64 = 1 << 20;

fn meta(app_id: &str, instance_id: u128) -> CncMeta {
    CncMeta { node_id: 0, instance_id, app_id: app_id.into(), buffer_bytes: MIB, max_payload: 256 }
}

fn make_instance(dir: &Path, app_id: &str, instance_id: u128) {
    CncPage::create_file(&dir.join("cnc2.dat"), &meta(app_id, instance_id)).unwrap();
    MpscRing::create(&dir.join("ingress.ring"), MIB, 256).unwrap();
    MpscRing::create(&dir.join("query.ring"), MIB, 256).unwrap();
    BroadcastRing::create(&dir.join("egress_service.0.broadcast"), MIB, 256).unwrap();
    BroadcastRing::create(&dir.join("egress_node.broadcast"), MIB, 256).unwrap();
}

#[test]
fn timeout_then_instance_restart_after_a_fresh_cnc_page() {
    // SAFETY (env var mutation in a test): this is the only test in this
    // binary/process, so there is no other thread that could observe a
    // torn or unexpectedly-overridden value.
    unsafe {
        std::env::set_var("UC2_CLIENT_TIMEOUT_MS", "200");
    }

    let dir = tempfile::tempdir().unwrap();
    let instance_a: u128 = 0x1111_1111_1111_1111;
    make_instance(dir.path(), "restart-test", instance_a);

    let client = Client::connect(dir.path(), "restart-test").unwrap();
    assert_eq!(client.instance_id(), instance_a);

    // Nobody ever answers (no node/service): plain timeout, no restart yet.
    let result: Result<u8, ClientError> = client.submit(&1u8);
    match result {
        Err(ClientError::Timeout(d)) => assert_eq!(d, Duration::from_millis(200)),
        other => panic!("expected Timeout, got {other:?}"),
    }

    // Simulate a node restart: re-create the SAME cnc file in place (as
    // `uc2_node::Node::start` does on every boot) with a fresh instance_id.
    // The client's already-mmap'd `Arc<CncPage>` observes the new bytes (same
    // inode, truncate-and-rewrite, never unlinked).
    let instance_b: u128 = 0x2222_2222_2222_2222;
    CncPage::create_file(&dir.path().join("cnc2.dat"), &meta("restart-test", instance_b)).unwrap();

    let result: Result<u8, ClientError> = client.submit(&2u8);
    match result {
        Err(ClientError::InstanceRestart { attached, current }) => {
            assert_eq!(attached, instance_a);
            assert_eq!(current, instance_b);
        }
        other => panic!("expected InstanceRestart, got {other:?}"),
    }
}
