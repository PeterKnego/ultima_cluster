// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Coordinated-snapshot plan 2 Task 7 — end-to-end bin test for `uc2ctl
//! snapshot [--standby]` / `snapshot show` (spec §8).
//!
//! Starts a real node IN-PROCESS (`uc_node::Node::start_with`, the same
//! harness shape `settings_apply_bin.rs`/`admin_auth_bin.rs` use) plus a
//! real snapshot-CAPABLE service (`uc_service::ServiceBuilder::
//! start`, the same fixture shape `uc_node/tests/learner.rs`'s
//! `SumSm` uses) and shells out to the compiled `uc2ctl` binary
//! (`env!("CARGO_BIN_EXE_uc2ctl")`) for every assertion — `uc_ctl` stays
//! bin-only (Ruling R16: its own doc says "nothing in this crate is a Rust
//! API"), so a snapshot capstone belongs here, driving the compiled binary,
//! never as a library call from another crate's tests.
//!
//! `snapshot fetch` (admin op 9) is not exercised end to end here — it needs
//! a second, learner node to pull a set FROM, which is a heavier fixture
//! than this task's brief asks for; its wire encoding is pinned by
//! `uc_ctl::snapshot`'s own unit tests instead (round-tripped against the
//! node's `fetch_position` formula).

mod common;

use std::time::{Duration, Instant};

use common::{
    await_leader, await_row0_capable, parse_instant, run_ctl, start_capable_service, start_node,
};

const APP: &str = "ctlsnapshot";

/// `uc2ctl snapshot` end to end: command an instant, prove the node actually
/// completed the SET at the printed position (not just that the admin band
/// accepted the request), then `uc2ctl snapshot show` reads that same
/// position back off disk as `set=<P>`. Finally, `--standby` on this
/// learner-less cluster is refused by name.
#[test]
fn snapshot_take_commits_a_set_and_show_reads_it_back() {
    let root = tempfile::Builder::new()
        .prefix("uc2ctl-snapshot-take-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");

    let (node, instance_dir) = start_node(root.path(), "n0", APP);
    await_leader(&node, 20);
    let svc = start_capable_service(&instance_dir, APP);
    await_row0_capable(&instance_dir, APP, 20);
    let dir_s = instance_dir.to_str().unwrap();

    let r = run_ctl(&["snapshot", "--instance-dir", dir_s, "--app-id", APP]);
    assert_eq!(r.status, 0, "snapshot must succeed: {r:?}");
    assert!(
        r.stdout.contains("instant="),
        "expected an instant= line: {}",
        r.stdout
    );
    assert!(
        r.stderr.is_empty(),
        "unexpected stderr on a successful snapshot: {}",
        r.stderr
    );
    let p = parse_instant(&r.stdout);
    assert!(p > 0, "a real committed instant is never at position 0");

    // The set completes asynchronously (the row's builder thread + the
    // `uc2-cluster` agent both have to reach P) — poll the in-process
    // `Node` handle this test already holds, the same round trip
    // `uc_node/tests/learner.rs`'s `instant_until_complete` proves.
    let deadline = Instant::now() + Duration::from_secs(30);
    while node.snapshot_set_position() != p {
        assert!(
            Instant::now() < deadline,
            "the leader never completed the set at {p} (stuck at {})",
            node.snapshot_set_position()
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let r_show = run_ctl(&["snapshot", "show", "--instance-dir", dir_s, "--app-id", APP]);
    assert_eq!(r_show.status, 0, "snapshot show must succeed: {r_show:?}");
    assert!(
        r_show.stdout.contains(&format!("set={p}")),
        "expected set={p} in show output:\n{}",
        r_show.stdout
    );
    assert!(
        r_show.stdout.contains("row=0 name=sum"),
        "expected the declared row's own line:\n{}",
        r_show.stdout
    );
    assert!(
        r_show.stdout.contains(&format!("cluster newest={p}")),
        "expected the cluster artifact's own line:\n{}",
        r_show.stdout
    );

    // `--standby` needs a learner in the committed membership (spec §5.7);
    // this cluster never declared one.
    let r_standby = run_ctl(&[
        "snapshot",
        "--standby",
        "--instance-dir",
        dir_s,
        "--app-id",
        APP,
    ]);
    assert_ne!(
        r_standby.status, 0,
        "a standby instant with no learner must be refused: {r_standby:?}"
    );
    assert!(
        r_standby.stdout.contains("snapshot_no_learner")
            || r_standby.stderr.contains("snapshot_no_learner"),
        "expected snapshot_no_learner: {r_standby:?}"
    );

    svc.stop();
    node.stop();
}

/// #33 task 10: `uc2ctl upgrade show` prints the row's running version and
/// which record set it. This fixture's leader attaches `TrivialSm` (`const
/// VERSION` defaults to 0, "unversioned") with no pin ever staged, so the
/// only record that can set row 0's running version is the automatic
/// genesis append — `by=genesis`. A coordinated instant is enough to force
/// the cluster artifact to disk (the same wait `snapshot_take_...` above
/// uses), which is what `show` reads.
#[test]
fn upgrade_show_reports_the_genesis_running_version() {
    let root = tempfile::Builder::new()
        .prefix("uc2ctl-upgrade-show-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");

    let (node, instance_dir) = start_node(root.path(), "n0", APP);
    await_leader(&node, 20);
    let svc = start_capable_service(&instance_dir, APP);
    await_row0_capable(&instance_dir, APP, 20);
    let dir_s = instance_dir.to_str().unwrap();

    let r = run_ctl(&["snapshot", "--instance-dir", dir_s, "--app-id", APP]);
    assert_eq!(r.status, 0, "snapshot must succeed: {r:?}");
    let p = parse_instant(&r.stdout);
    let deadline = Instant::now() + Duration::from_secs(30);
    while node.snapshot_set_position() != p {
        assert!(
            Instant::now() < deadline,
            "the leader never completed the set at {p} (stuck at {})",
            node.snapshot_set_position()
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let r_show = run_ctl(&["upgrade", "show", "--instance-dir", dir_s, "--app-id", APP]);
    assert_eq!(r_show.status, 0, "upgrade show must succeed: {r_show:?}");
    assert!(
        r_show.stdout.contains("row=0 running=unversioned"),
        "expected the running version on row 0's line:\n{}",
        r_show.stdout
    );
    assert!(
        r_show.stdout.contains("by=genesis"),
        "no pin was ever staged, so the record must be named genesis:\n{}",
        r_show.stdout
    );

    svc.stop();
    node.stop();
}
