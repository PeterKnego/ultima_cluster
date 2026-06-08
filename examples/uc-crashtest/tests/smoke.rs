//! Multi-process smoke test: spawn the node-only and service-only binaries as
//! SEPARATE OS processes sharing one instance_dir, connect a client, write 7,
//! read 7 back. Proves the two reference binaries rendezvous over shmem.
//!
//! Gated behind `hard-crash-tests` because it spawns real processes and is the
//! foundation for the later hard-crash (SIGKILL) tests.
#![cfg(feature = "hard-crash-tests")]

use std::time::{Duration, Instant};

use uc_lincheck::register::{Cmd, CmdResp};

mod common;
use common::{connect_with_retry, spawn_node, spawn_service, submit_until_ok, wait_for_path};

#[tokio::test]
async fn write_then_read_across_processes() {
    let tmp = tempfile::tempdir().unwrap();
    let inst = tmp.path().join("inst");
    std::fs::create_dir_all(&inst).unwrap();

    // ── Spawn node, wait for cnc.dat, then spawn service. `Reap` guards tear the
    //    children down on Drop (incl. on panic), so no orphaned processes. ──────
    let _node = spawn_node(&inst, &tmp.path().join("nodedata"));
    wait_for_path(&inst.join("cnc.dat"), Duration::from_secs(10)).await;
    let _svc = spawn_service(&inst, &tmp.path().join("svcdata"));

    let client = connect_with_retry(&inst, Duration::from_secs(10)).await;

    // Write 7, read 7. `current_leader()` going Some is necessary but NOT
    // sufficient for write-readiness (the leader's initial blank entry must commit
    // first), so submit/read retry transient `NotLeader` until a deadline.
    let deadline = Instant::now() + Duration::from_secs(15);
    let r = submit_until_ok(&client, &Cmd::Write(7), deadline).await;
    assert!(matches!(r, CmdResp::WriteAck), "expected WriteAck, got {r:?}");

    let v = common::read_until_ok(&client, deadline).await;
    assert_eq!(v, Some(7), "read should observe the committed write");

    client.shutdown().await.ok();
    // _node / _svc dropped here → killed + reaped.
}
