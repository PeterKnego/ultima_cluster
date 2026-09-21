#![allow(dead_code)]
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use uc_client::Client;
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};
use uc_log::cnc::{AdminReq, AdminResp, CncPage};
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig};
use uc_protocol::v2::cnc::ADMIN_OP_UPGRADE_PIN;
use uc_protocol::v2::upgrade::{UpgradePin, encode_upgrade_pin};
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

/// `uc2ctl upgrade pin --row <row> --from <from> --to <to> --origin <origin>`
/// in process (FSM upgrade lifecycle spec §2.5, plan B1): stage the 20-byte
/// `UpgradePin` record at `<instance_dir>/upgrade.pending`, then submit admin
/// op 10 with the staged file's digest in the `id`/`ip`/`port` fields —
/// `uc_ctl::upgrade::pin`'s pipeline, minus the bin and minus the signature
/// (these test nodes run the filesystem admin policy, so there is no auth
/// line, exactly as `uc_node/tests/reconfig.rs`'s `admin_request` relies on).
///
/// Two of the node's three door checks are races against this fixture rather
/// than errors in it: `pin_no_set` (54) compares `origin` against the node's
/// NEWEST complete set, which the cluster agent publishes a moment after the
/// row's own artifact appears, and status 2 is the ordinary
/// single-in-flight retry. So a non-zero answer is retried until the
/// deadline and only then asserted — a refusal that is really a refusal
/// still fails the test, with its reason code.
pub fn pin_row(dir: &Path, cnc: &CncPage, row: u8, from: u32, to: u32, origin: u64) -> AdminResp {
    let mut bytes = Vec::new();
    encode_upgrade_pin(
        &UpgradePin {
            row,
            from,
            to,
            origin,
        },
        &mut bytes,
    );
    let (id, ip, port) = uc_node::staged_digest(&bytes);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        stage_upgrade_pin(dir, &bytes);
        let resp = admin_request(cnc, ADMIN_OP_UPGRADE_PIN, id, ip, port);
        if resp.status == 0 || Instant::now() >= deadline {
            assert_eq!(
                resp.status, 0,
                "upgrade pin refused: status={} reason={}",
                resp.status, resp.reason
            );
            return resp;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// `<instance_dir>/upgrade.pending`, written the way `uc2ctl` writes it:
/// 0600, fsync'd, renamed into place, so the node reads a whole record or
/// none of one.
fn stage_upgrade_pin(dir: &Path, bytes: &[u8]) {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let pending = dir.join(uc_node::UPGRADE_PENDING_FILE);
    let tmp = dir.join(format!("{}.tmp", uc_node::UPGRADE_PENDING_FILE));
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)
            .unwrap();
        f.write_all(bytes).unwrap();
        f.sync_all().unwrap();
    }
    std::fs::rename(&tmp, &pending).unwrap();
}

/// The `uc2ctl` mutating-command flow, minus the bin —
/// `uc_node/tests/reconfig.rs`'s `admin_request` verbatim: read the admin
/// band's current seq, write a fresh request at `seq + 1`, poll the response
/// line for the echoed seq.
fn admin_request(cnc: &CncPage, op: u32, id: u32, ip: u32, port: u16) -> AdminResp {
    let seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0) + 1;
    // The nonce is the anti-replay field of the SIGNED flow; with the
    // filesystem policy nothing reads it, so a fresh wall-clock reading is
    // enough to keep two requests in one run distinct without pulling `rand`
    // into this crate's dev-dependencies.
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(seq);
    cnc.write_admin_req(&AdminReq {
        seq,
        nonce,
        op,
        id,
        ip,
        port,
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(resp) = cnc.read_admin_resp(seq) {
            return resp;
        }
        assert!(
            Instant::now() < deadline,
            "admin response timed out for seq {seq}"
        );
        std::thread::yield_now();
    }
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
