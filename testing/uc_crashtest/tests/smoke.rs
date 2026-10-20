// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Multi-process smoke test (v2): spawn the node-only and service-only
//! binaries as SEPARATE OS processes sharing one instance_dir, connect a
//! `uc_client::Client`, write 7, read 7 back. Proves the two reference
//! binaries rendezvous over the v2 shared-memory IPC (cnc2.dat + rings).
//!
//! Gated behind `hard-crash-tests` because it spawns real processes and is
//! the foundation for the later hard-crash (SIGKILL) tests.
#![cfg(feature = "hard-crash-tests")]

use std::time::{Duration, Instant};

use uc_lincheck::register::{Cmd, CmdResp};

mod common;
use common::{connect_with_retry, spawn_node, spawn_service, submit_until_ok, wait_for_ready};

#[test]
fn write_then_read_across_processes() {
    let tmp = common::tempdir(); // the target volume, never tmpfs (#182)
    let inst = tmp.path().join("inst");
    std::fs::create_dir_all(&inst).unwrap();

    // ── Spawn node, wait for cnc2.dat, then spawn service. `Reap` guards
    //    tear the children down on Drop (incl. on panic), so no orphaned
    //    processes. ──────────────────────────────────────────────────────
    let _node = spawn_node(&inst);
    wait_for_ready(&inst, Duration::from_secs(10));
    let _svc = spawn_service(&inst);

    let client = connect_with_retry(&inst, Duration::from_secs(10));

    // Write 7, read 7. A single-node cluster elects itself near-instantly,
    // but the leader's term-open frame must still commit before the first
    // write is admitted, so submit/read retry transient `NotLeader` until a
    // deadline.
    let deadline = Instant::now() + Duration::from_secs(15);
    let r = submit_until_ok(&client, &Cmd::Write(7), deadline);
    assert!(
        matches!(r, CmdResp::WriteAck),
        "expected WriteAck, got {r:?}"
    );

    let v = common::read_until_ok(&client, deadline);
    assert_eq!(v, Some(7), "read should observe the committed write");

    client.shutdown();
    // _node / _svc dropped here → killed + reaped.
}

/// #182: a real `Violation` dumps its history on the target volume beside the
/// instance dirs, never on `/tmp`, and the panic names the file. One client
/// writes 1 and then reads 2 back: no linearization explains that.
#[test]
fn a_violation_dump_lands_under_the_target_tmpdir() {
    use uc_lincheck::history::{Entry, Outcome};
    use uc_lincheck::model::{Op, RegResp};
    let entries = vec![
        Entry {
            client: 0,
            op: Op::Write(1),
            invoke: 0,
            ret: 1,
            outcome: Outcome::Ok(RegResp::Ack),
        },
        Entry {
            client: 0,
            op: Op::Read,
            invoke: 2,
            ret: 3,
            outcome: Outcome::Ok(RegResp::Value(Some(2))),
        },
    ];
    let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("uc2_pin182_history_t.txt");
    let _ = std::fs::remove_file(&path);
    let r = std::panic::catch_unwind(|| common::assert_linearizable(&entries, "pin182", "t"));
    let msg = match r {
        Ok(()) => panic!("a contradictory history was judged linearizable"),
        Err(e) => e
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default(),
    };
    assert!(path.is_file(), "dump missing at {}", path.display());
    assert!(
        !path.starts_with("/tmp"),
        "dump on tmpfs: {}",
        path.display()
    );
    assert!(
        msg.contains(&path.display().to_string()),
        "panic does not name the dump: {msg}"
    );
    let _ = std::fs::remove_file(&path);
}
