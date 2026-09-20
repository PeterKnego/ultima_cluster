#![allow(dead_code)]
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use uc_client::Client;
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig};
use uc_service::{ServiceBuilder, ServiceConfig, StateMachine};

pub fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("diffreplay-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap()
}

pub fn node_config(dir: &Path, app_id: &str, fsm: &str) -> NodeConfig {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app_id.into(),
        buffer_bytes: 1 << 20,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        force_jumbo_frames: false,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(fsm),
    }
}

pub fn start_single_node(dir: &Path, app_id: &str, fsm: &str) -> Node {
    Node::start(node_config(dir, app_id, fsm)).unwrap()
}

/// Poll `f` until it holds or `timeout` elapses. Returns whether it held —
/// the caller decides what to do with a timeout, so cleanup can run before
/// an assertion fires. [`wait_until`] is this with the assertion built in.
#[must_use]
pub fn wait_for(mut f: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !f() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

pub fn wait_until(f: impl FnMut() -> bool) {
    assert!(wait_for(f, Duration::from_secs(10)), "condition never held");
}

/// The `register-replay` fixture binary, beside this test binary in the
/// cargo target directory. It sits behind `uc_lincheck`'s `replay-bin`
/// required-feature, so `cargo test` does NOT build it — assert rather than
/// skip (a silently skipped e2e test is not a test) and name the command
/// that fixes it. CI builds it before the test job.
pub fn register_replay_bin() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_BIN_EXE_uc2-diffreplay"));
    p.set_file_name("register-replay");
    assert!(
        p.exists(),
        "build it first: cargo build -p uc_lincheck --features replay-bin --bin register-replay ({})",
        p.display()
    );
    p
}

/// `uc2ctl snapshot` in process: command an instant, return its position P.
pub fn command_instant(node: &Node) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match node.command_snapshot(false) {
            Ok(p) => return p,
            Err(uc_node::SnapshotRefusal::Retry) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => panic!("uc2ctl snapshot refused: {e}"),
        }
    }
}

pub fn register_name() -> &'static str {
    <RegisterSm as StateMachine>::NAME
}

/// Drive a single node with RegisterSm: N writes, an instant at P, M more
/// writes. Returns `(P, u64::MAX)` — `Node` has no "applied frontier"
/// accessor in this plan's scope, so the end is left to the caller (the
/// driver stops at the journal's last frame instead of naming Q precisely).
pub fn build_register_history(dir: &std::path::Path, app_id: &str, n: u64, m: u64) -> (u64, u64) {
    let node = start_single_node(dir, app_id, register_name());
    wait_until(|| node.can_serve());
    let cfg = ServiceConfig::new(dir.to_path_buf(), app_id.to_string());
    let svc = ServiceBuilder::new(cfg, RegisterSm::default())
        .start_with_snapshots()
        .unwrap();
    let client = Client::connect(dir, app_id).unwrap();
    for v in 0..n {
        let _: CmdResp = client.submit(&Cmd::Write(v)).unwrap();
    }
    let p = command_instant(&node);
    // The instant completes when the row's artifact appears.
    let art = dir
        .join("snapshots")
        .join("0")
        .join(format!("snap-{p}.ultsnap"));
    wait_until(|| art.is_file());
    for v in n..n + m {
        let _: CmdResp = client.submit(&Cmd::Write(v)).unwrap();
    }
    client.shutdown();
    svc.stop();
    node.stop();
    (p, u64::MAX)
}
