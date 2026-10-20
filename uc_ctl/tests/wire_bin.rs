// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Wire-level spec §4.3/§5.2 — end-to-end bin test for `uc2ctl wire show`,
//! `uc2ctl wire raise` (admin op 12) and `status`'s `wire:` line.
//!
//! The `tier_forget_bin.rs` shape: a real one-voter node IN-PROCESS (its own
//! leader), an `ObsServer` on its `obs.sock` so `wire show` reads the LIVE
//! cluster view for the raise mode, and the actual `uc2ctl` binary for every
//! request. A solo cluster at its binary's max has nothing to raise: under
//! ruling 9 that is a no-op SUCCESS, not refusal 63.

// This test needs only the node bring-up and the runner, not the service
// half `snapshot_bin.rs` and `cluster_view_bin.rs` use.
#[allow(dead_code)]
mod common;

use std::time::{Duration, Instant};

use common::{await_leader, run_ctl, start_node};

const APP: &str = "ctlwire";

#[test]
fn wire_show_prints_the_level_and_raise_is_a_no_op_success_on_a_solo_cluster() {
    let root = tempfile::Builder::new()
        .prefix("uc2ctl-wire-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let (node, dir) = start_node(root.path(), "n0", APP);
    await_leader(&node, 20);
    let srv = uc_node::obs::http::ObsServer::serve_on(node.observability(), None, Some(&dir))
        .expect("obs socket");
    let d = dir.to_str().unwrap();

    // The genesis Settings record commits the level; the cnc word follows
    // at apply, so wait for it rather than racing the cluster agent.
    let deadline = Instant::now() + Duration::from_secs(20);
    let show = loop {
        let r = run_ctl(&["wire", "show", "--instance-dir", d, "--app-id", APP]);
        if r.stdout.contains("wire: level=12 ") || Instant::now() >= deadline {
            break r;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(show.status, 0, "{show:?}");
    assert!(
        show.stdout
            .contains("wire: level=12 binary=12..12 raise=auto"),
        "{show:?}"
    );
    // The leader prints the verdict; a solo member has no peer lines.
    assert!(!show.stdout.contains("  member id="), "{show:?}");
    assert!(show.stdout.contains("verdict: at level"), "{show:?}");

    // A solo cluster at its binary's max: at level, a no-op success (ruling 9).
    let r = run_ctl(&["wire", "raise", "--instance-dir", d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    assert!(
        r.stdout.contains("ok (cluster position ")
            && r.stdout
                .contains(") — run `uc2ctl wire show` for the committed level"),
        "{r:?}"
    );

    let r = run_ctl(&["status", "--instance-dir", d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    assert!(r.stdout.contains("wire: level=12 binary=12..12"), "{r:?}");

    // The handler writes the manual raise's one audit record.
    let audit = std::fs::read_to_string(dir.join("audit.jsonl")).expect("audit.jsonl");
    let raises: Vec<&str> = audit
        .lines()
        .filter(|l| l.contains("\"op_name\":\"wire_raise\""))
        .collect();
    assert_eq!(raises.len(), 1, "{audit}");
    assert!(raises[0].contains("level=12"), "{}", raises[0]);

    srv.stop();
    node.stop();
}
