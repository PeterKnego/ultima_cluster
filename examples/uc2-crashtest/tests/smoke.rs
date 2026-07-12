// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Multi-process smoke test (v2): spawn the node-only and service-only
//! binaries as SEPARATE OS processes sharing one instance_dir, connect a
//! `uc2_client::Client`, write 7, read 7 back. Proves the two reference
//! binaries rendezvous over the v2 shared-memory IPC (cnc2.dat + rings).
//!
//! Gated behind `hard-crash-tests` because it spawns real processes and is
//! the foundation for the later hard-crash (SIGKILL) tests.
#![cfg(feature = "hard-crash-tests")]

use std::time::{Duration, Instant};

use uc_lincheck::register::{Cmd, CmdResp};

mod common;
use common::{connect_with_retry, spawn_node, spawn_service, submit_until_ok, wait_for_path};

#[test]
fn write_then_read_across_processes() {
    let tmp = tempfile::tempdir().unwrap();
    let inst = tmp.path().join("inst");
    std::fs::create_dir_all(&inst).unwrap();

    // ── Spawn node, wait for cnc2.dat, then spawn service. `Reap` guards
    //    tear the children down on Drop (incl. on panic), so no orphaned
    //    processes. ──────────────────────────────────────────────────────
    let _node = spawn_node(&inst);
    wait_for_path(&inst.join("cnc2.dat"), Duration::from_secs(10));
    let _svc = spawn_service(&inst);

    let client = connect_with_retry(&inst, Duration::from_secs(10));

    // Write 7, read 7. A single-node cluster elects itself near-instantly,
    // but the leader's term-open frame must still commit before the first
    // write is admitted, so submit/read retry transient `NotLeader` until a
    // deadline.
    let deadline = Instant::now() + Duration::from_secs(15);
    let r = submit_until_ok(&client, &Cmd::Write(7), deadline);
    assert!(matches!(r, CmdResp::WriteAck), "expected WriteAck, got {r:?}");

    let v = common::read_until_ok(&client, deadline);
    assert_eq!(v, Some(7), "read should observe the committed write");

    client.shutdown();
    // _node / _svc dropped here → killed + reaped.
}
