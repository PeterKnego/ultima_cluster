// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Coordinated-snapshot plan 2 Task 7 — end-to-end bin test for `uc2ctl
//! snapshot [--standby]` / `snapshot show` (spec §8).
//!
//! Starts a real node IN-PROCESS (`uc_node::Node::start_with`, the same
//! harness shape `settings_apply_bin.rs`/`admin_auth_bin.rs` use) plus a
//! real snapshot-CAPABLE service (`uc_service::ServiceBuilder::
//! start_with_snapshots`, the same fixture shape `uc_node/tests/learner.rs`'s
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

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use uc_net::fault::FaultConfig;
use uc_node::{
    AdminPolicy, CryptoConfig, DEFAULT_JOURNAL_SEGMENT_BYTES, Node, NodeConfig, PurgePolicy,
    ServicesConfig,
};
use uc_protocol::v2::cnc::CNC_SVC_STATUS_SNAPSHOT_CAPABLE;
use uc_service::{ApplyCtx, RawStateMachine, ServiceBuilder, ServiceConfig, SnapshotStateMachine};

const APP: &str = "ctlsnapshot";

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
        force_jumbo_frames: false,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 0x5150_1234_ABCD_0F0F,
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: CryptoConfig::Disabled,
        // One declared, snapshot-capable row — `uc2ctl snapshot` refuses `48
        // snapshot_unsupported` for any row that lacks the capability bit,
        // and a `--standby` instant (also exercised below) needs the
        // question "does the committed membership hold a learner?" to be
        // answerable, which needs at least the ordinary (non-standby) path
        // to work first.
        services: ServicesConfig::single("sum"),
    }
}

/// A fresh 1-node cluster (a single voter is trivially its own leader) under
/// `root/<name>` — the same fixture `settings_apply_bin.rs`'s `start_node`
/// uses.
fn start_node(root: &Path, name: &str) -> (Node, PathBuf) {
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

/// A snapshot-CAPABLE raw state machine, row 0, name `"sum"` (matching
/// `NodeConfig::services` above). Trivial by design: this test only needs
/// the row to be ABLE to freeze on command, never to hold interesting
/// content — the artifact's bytes are the service's own business and
/// `uc2ctl snapshot show` never opens them (module doc).
#[derive(Default)]
struct TrivialSm;

impl RawStateMachine for TrivialSm {
    const NAME: &'static str = "sum";

    fn apply(&mut self, _ctx: &mut ApplyCtx, _cmd: &[u8], _out: &mut Vec<u8>) {}
    fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}
    fn last_applied(&self) -> Option<u64> {
        None
    }
}

impl SnapshotStateMachine for TrivialSm {
    type SnapshotHandle = ();

    fn freeze(&self) -> Result<((), u64), uc_service::SnapshotError> {
        Ok(((), 0))
    }
    fn stream_snapshot(
        _handle: (),
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        dst.write_all(b"trivial")?;
        Ok(())
    }
    fn install_snapshot(
        &mut self,
        position: u64,
        _src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        Ok(position)
    }
}

fn start_capable_service(dir: &Path, app: &str) -> uc_service::Service<TrivialSm> {
    let cfg = ServiceConfig::new(dir, app);
    ServiceBuilder::new(cfg, TrivialSm)
        .start_with_snapshots()
        .expect("service start")
}

/// Block until row 0 carries `CNC_SVC_STATUS_SNAPSHOT_CAPABLE` — the bit the
/// service SDK writes at attach (before that window `snapshot` legitimately
/// answers `48 snapshot_unsupported`, which is not what this test is about).
fn await_row0_capable(instance_dir: &Path, app: &str, secs: u64) {
    let cnc = uc_log::cnc::CncPage::open_file(&instance_dir.join("cnc2.dat"), app)
        .expect("open cnc to poll attach");
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let status = cnc.service_slot(0).status.load_acquire();
        if status & CNC_SVC_STATUS_SNAPSHOT_CAPABLE != 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "row 0 never attached as snapshot-capable"
        );
        std::thread::sleep(Duration::from_millis(10));
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

/// Parse `instant=<P>` out of `uc2ctl snapshot`'s stdout.
fn parse_instant(stdout: &str) -> u64 {
    stdout
        .lines()
        .find_map(|l| l.strip_prefix("instant="))
        .unwrap_or_else(|| panic!("no instant= line in stdout:\n{stdout}"))
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("bad instant value in {stdout:?}: {e}"))
}

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

    let (node, instance_dir) = start_node(root.path(), "n0");
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
