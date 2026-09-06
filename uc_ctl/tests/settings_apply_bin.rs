// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Cluster-FSM plan 1 task 8 — end-to-end bin test for `uc2ctl settings
//! apply`/`settings show` (spec §6, §8).
//!
//! Starts a real node IN-PROCESS (`uc_node::Node::start_with`, the same
//! harness shape `admin_auth_bin.rs` uses) and shells out to the actual
//! `uc2ctl` binary (`env!("CARGO_BIN_EXE_uc2ctl")`) for every assertion —
//! `uc_ctl` stays bin-only (Ruling R16): its own doc says "nothing in this
//! crate is a Rust API", so a settings-apply capstone belongs here, in
//! `uc_ctl`'s own test suite, driving the compiled binary — never as a
//! library call from another crate's tests (`CARGO_BIN_EXE_uc2ctl` is only
//! set for integration tests of the package that DEFINES the binary, i.e.
//! this one).

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use uc_net::fault::FaultConfig;
use uc_node::{
    AdminPolicy, CryptoConfig, DEFAULT_JOURNAL_SEGMENT_BYTES, Node, NodeConfig, PurgePolicy,
    ServicesConfig,
};

const APP: &str = "ctlsettings";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_uc2ctl")
}

fn make_config(instance_dir: PathBuf, addr: SocketAddr) -> NodeConfig {
    NodeConfig {
        id: 0,
        members: vec![(0, addr)],
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes: 1 << 22,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 0x5150_1234_ABCD_0F0F,
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: CryptoConfig::Disabled,
        // `settings apply`'s acceptance function (`ClusterFsm::validate`'s
        // `Settings` arm) checks bounds only, never membership or a declared
        // FSM set, so there is nothing for a declared row to prove — and
        // declaring none means no `uc2-cluster` artifact is EVER written
        // (the bridging trigger fires once every declared row has
        // snapshotted), which is exactly the `show` case under test.
        services: ServicesConfig::none_for_tests(),
    }
}

/// A fresh 1-node cluster (a single voter is trivially its own leader) under
/// `root/<name>`, the same fixture `admin_auth_bin.rs`'s `start_node` uses.
fn start_node(root: &std::path::Path, name: &str) -> (Node, PathBuf) {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let addr = sock.local_addr().unwrap();
    let instance_dir = root.join(name);
    let cfg = make_config(instance_dir.clone(), addr);
    let opts = uc_node::StartOpts {
        socket: Some(sock),
        admin: AdminPolicy::Filesystem,
    };
    let node = Node::start_with(cfg, opts).expect("start");
    (node, instance_dir)
}

fn await_leader(node: &Node, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !node.can_serve() {
        assert!(
            Instant::now() < deadline,
            "node never became leader/serving"
        );
        std::thread::yield_now();
    }
}

#[derive(Debug)]
struct Run {
    status: i32,
    stdout: String,
    stderr: String,
}

fn run_ctl(args: &[&str]) -> Run {
    let out = Command::new(bin())
        .args(args)
        .output()
        .expect("spawn uc2ctl");
    Run {
        status: out.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&out.stdout).to_string(),
        stderr: String::from_utf8_lossy(&out.stderr).to_string(),
    }
}

/// `uc2ctl settings apply` end to end: parse, stage, sign (a no-op under
/// `AdminPolicy::Filesystem`), send `ADMIN_OP_SETTINGS_APPLY`, print
/// `version=<frame-end>` on success — then `uc2ctl settings show`, which
/// reads the newest CLUSTER ARTIFACT rather than the live view (there is no
/// live, in-process reading in this plan; spec §13 phase 2 is what would add
/// one). This cluster declares no FSM at all, so no artifact is ever
/// written, even though the live view (checked directly through the
/// in-process `Node` handle, the same way `uc_node/tests/admin_auth.rs`
/// checks the schedule-apply capstone's committed table) proves the settings
/// command committed — `show` must print the honest "no cluster artifact
/// yet" line rather than a value nothing committed. Driving a real artifact
/// would need a `SnapshotStateMachine` service tuned the way
/// `uc_node/tests/learner.rs`'s `start_with_snapshots` tests do; that setup
/// is not needed to prove the apply path end to end, so this test does not
/// do it.
#[test]
fn settings_apply_commits_and_show_reads_the_honest_no_artifact_line() {
    let root = tempfile::Builder::new()
        .prefix("uc2ctl-settings-apply-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");

    let (node, instance_dir) = start_node(root.path(), "n0");
    await_leader(&node, 20);
    let dir_s = instance_dir.to_str().unwrap();

    let toml_path = root.path().join("settings.toml");
    std::fs::write(&toml_path, "admission_bytes = 4096\n").expect("write settings TOML");

    let r = run_ctl(&[
        "settings",
        "apply",
        toml_path.to_str().unwrap(),
        "--instance-dir",
        dir_s,
        "--app-id",
        APP,
    ]);
    assert_eq!(r.status, 0, "settings apply must succeed: {r:?}");
    assert!(
        r.stdout.contains("version="),
        "expected a version= line: {}",
        r.stdout
    );
    assert!(
        r.stderr.is_empty(),
        "unexpected stderr on a successful apply: {}",
        r.stderr
    );

    // The command travels as a CLUSTER frame and takes effect at COMMIT —
    // `cluster_view().admission_bytes` reaching 4096 (checked through the
    // in-process `Node` handle this test already holds) proves the request
    // committed and the live view followed, the same round trip
    // `uc_node/tests/admin_auth.rs`'s schedule-apply capstone proves via
    // `table_position`.
    let deadline = Instant::now() + Duration::from_secs(30);
    while node.cluster_view().admission_bytes.load(Ordering::Acquire) != 4096 {
        assert!(
            Instant::now() < deadline,
            "the leader never committed the settings it applied"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !instance_dir.join(uc_node::SETTINGS_PENDING_FILE).exists(),
        "an accepted apply consumes the staged file"
    );

    let r_show = run_ctl(&["settings", "show", "--instance-dir", dir_s, "--app-id", APP]);
    assert_eq!(r_show.status, 0, "settings show must succeed: {r_show:?}");
    assert!(
        r_show.stdout.contains("no cluster artifact yet"),
        "expected the honest no-artifact line (no service in this cluster ever \
         snapshots, so no cluster artifact should exist yet): {}",
        r_show.stdout
    );

    node.stop();
}
