// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared bring-up for the `uc2ctl` binary tests that need a real node and a
//! snapshot-capable service: an in-process 1-node cluster
//! (`uc_node::Node::start_with`), a trivial snapshot-capable row 0 named
//! `"sum"`, and a runner for the compiled `uc2ctl`
//! (`env!("CARGO_BIN_EXE_uc2ctl")`). Used by `snapshot_bin.rs` and
//! `cluster_view_bin.rs`; each passes its own `app_id`.

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

pub fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_uc2ctl")
}

pub fn make_config(instance_dir: PathBuf, addr: SocketAddr, app: &str) -> NodeConfig {
    NodeConfig {
        id: 0,
        members: vec![(0, addr)],
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: app.into(),
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
        // and a `--standby` instant (`snapshot_bin.rs`) needs the
        // question "does the committed membership hold a learner?" to be
        // answerable, which needs at least the ordinary (non-standby) path
        // to work first.
        services: ServicesConfig::single("sum"),
    }
}

/// A fresh 1-node cluster (a single voter is trivially its own leader) under
/// `root/<name>` — the same fixture `settings_apply_bin.rs`'s `start_node`
/// uses.
pub fn start_node(root: &Path, name: &str, app: &str) -> (Node, PathBuf) {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let addr = sock.local_addr().unwrap();
    let instance_dir = root.join(name);
    let cfg = make_config(instance_dir.clone(), addr, app);
    let opts = uc_node::StartOpts {
        socket: Some(sock),
        admin: AdminPolicy::Filesystem,
        backup: None,
        tier_source: None,
    };
    let node = Node::start_with(cfg, opts).expect("start");
    (node, instance_dir)
}

pub fn await_leader(node: &Node, secs: u64) {
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
pub struct TrivialSm;

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

pub fn start_capable_service(dir: &Path, app: &str) -> uc_service::Service<TrivialSm> {
    let cfg = ServiceConfig::new(dir, app);
    ServiceBuilder::new(cfg, TrivialSm)
        .start()
        .expect("service start")
}

/// Block until row 0 carries `CNC_SVC_STATUS_SNAPSHOT_CAPABLE` — the bit the
/// service SDK writes at attach (before that window `snapshot` legitimately
/// answers `48 snapshot_unsupported`, which is not what this test is about).
pub fn await_row0_capable(instance_dir: &Path, app: &str, secs: u64) {
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
pub struct Run {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
}

pub fn run_ctl(args: &[&str]) -> Run {
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
pub fn parse_instant(stdout: &str) -> u64 {
    stdout
        .lines()
        .find_map(|l| l.strip_prefix("instant="))
        .unwrap_or_else(|| panic!("no instant= line in stdout:\n{stdout}"))
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("bad instant value in {stdout:?}: {e}"))
}
