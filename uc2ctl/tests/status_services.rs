// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M14c (spec §9): `uc2ctl status` prints a per-service table off page 2.
//!
//! Starts a real node IN-PROCESS declaring `{0, 1}` and writes FSM 0's slot
//! by hand — the slot band's writer is the service process, and this test
//! deliberately does not need one: `uc2ctl` reads the page, and the page is
//! what is under test. FSM 1 is left untouched (declared, never attached),
//! which is the row an operator most needs to see.

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use uc2_log::cnc::{CncPage, pack_service_status};
use uc2_net::fault::FaultConfig;
use uc2_node::{
    CryptoConfig, DEFAULT_JOURNAL_SEGMENT_BYTES, FsmLag, Node, NodeConfig, PurgePolicy,
    ServicesConfig,
};

const APP: &str = "ctlsvc";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_uc2ctl")
}

fn make_config(instance_dir: PathBuf, addr: SocketAddr, services: ServicesConfig) -> NodeConfig {
    NodeConfig {
        id: 0,
        members: vec![(0, addr)],
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes: 1 << 22,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 0x5150_1234_ABCD_0F0F,
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: CryptoConfig::Disabled,
        services,
    }
}

#[test]
fn status_prints_one_row_per_declared_fsm_including_an_absent_one() {
    let root = tempfile::Builder::new()
        .prefix("uc2ctl-svc-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let addr = sock.local_addr().unwrap();
    let dir = root.path().join("n0");
    let services = ServicesConfig::from_ids(&[0, 1], Some(FsmLag::Bounded(8192))).unwrap();
    let node = Node::start_with_socket(make_config(dir.clone(), addr, services), sock).unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "node never became leader/serving");
        std::thread::sleep(Duration::from_millis(10));
    }

    // Stand in for FSM 0's service process: attached, incarnation 1, epoch 1,
    // applied 4096, one snapshot at 2048, a heartbeat stamped just now.
    let cnc = CncPage::open_file(&dir.join("cnc2.dat"), APP).expect("open cnc");
    let s0 = cnc.service_slot(0);
    s0.applied.store_release(4096);
    s0.snapshot_pos.store_release(2048);
    s0.epoch.store_release(1);
    s0.heartbeat_ns.store_release(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );
    s0.status.store_release(pack_service_status(0, true, 1));

    let out = Command::new(bin())
        .args(["status", "--instance-dir", dir.to_str().unwrap(), "--app-id", APP])
        .output()
        .expect("spawn uc2ctl");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert_eq!(out.status.code(), Some(0), "status must succeed: {stdout}");

    assert!(stdout.contains("services: declared=[0, 1] fsm_lag=8192 bytes"), "{stdout}");
    assert!(
        stdout.contains("id=0 attached=true epoch=1 incarnation=1 applied=4096"),
        "{stdout}"
    );
    assert!(stdout.contains("snapshot_pos=2048"), "{stdout}");
    // The declared-but-absent FSM must still get a row — it is the row that
    // explains a stalled cluster.
    assert!(
        stdout.contains("id=1 attached=false epoch=0 incarnation=0 applied=0"),
        "{stdout}"
    );
    assert!(stdout.contains("heartbeat_age=never"), "{stdout}");
    // The pre-existing sections are untouched.
    assert!(stdout.contains("config: version="), "{stdout}");
    assert!(stdout.contains("members:"), "{stdout}");

    node.stop();
}
