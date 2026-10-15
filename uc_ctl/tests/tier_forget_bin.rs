// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Backup-tier spec §6.3 — end-to-end bin test for `uc2ctl tier forget`
//! (admin op 11, refusal 62).
//!
//! The `settings_apply_bin.rs` shape: a real one-voter node IN-PROCESS (its
//! own leader, so the backup claim it is offered is reported to itself and
//! appended), and the actual `uc2ctl` binary for every admin request. The
//! tier is registered through the node's report path
//! (`Node::offer_backup_claim_for_test`, the slot Task 8's writer fills) and
//! the committed watermark is read off the in-process `ClusterView`.

use std::net::UdpSocket;
use std::process::Command;
use std::time::{Duration, Instant};

use uc_net::fault::FaultConfig;
use uc_node::{
    AdminPolicy, CryptoConfig, DEFAULT_JOURNAL_SEGMENT_BYTES, Node, NodeConfig, PurgePolicy,
    ServicesConfig,
};
use uc_protocol::v2::backup::{BackupWatermark, WatermarkOp};

const APP: &str = "ctltierforget";

#[derive(Debug)]
struct Run {
    status: i32,
    stdout: String,
    stderr: String,
}

fn run_ctl(args: &[&str]) -> Run {
    let out = Command::new(env!("CARGO_BIN_EXE_uc2ctl"))
        .args(args)
        .output()
        .expect("spawn uc2ctl");
    Run {
        status: out.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&out.stdout).to_string(),
        stderr: String::from_utf8_lossy(&out.stderr).to_string(),
    }
}

fn await_until(secs: u64, msg: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{msg}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Register a tier, forget it (`0x…` hex id, the high half non-zero so the
/// `id`/`ip` split is exercised), and see it leave the committed view; then
/// forget it again — it is unknown now, so the node refuses `62
/// tier_unknown` and `uc2ctl` exits non-zero naming it. A decimal id is
/// accepted too.
#[test]
fn tier_forget_removes_a_tier_and_refuses_62_for_an_unknown_one() {
    let root = tempfile::Builder::new()
        .prefix("uc2ctl-tier-forget-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let addr = sock.local_addr().unwrap();
    let instance_dir = root.path().join("n0");
    let cfg = NodeConfig {
        id: 0,
        members: vec![(0, addr)],
        learners: Vec::new(),
        bind: addr,
        instance_dir: instance_dir.clone(),
        app_id: APP.into(),
        buffer_bytes: 1 << 22,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        force_jumbo_frames: false,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 0x7E57_F0F0_1234_5678,
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: CryptoConfig::Disabled,
        services: ServicesConfig::none_for_tests(),
    };
    let node = Node::start_with(
        cfg,
        uc_node::StartOpts {
            socket: Some(sock),
            admin: AdminPolicy::Filesystem,
            backup: None,
            tier_source: None,
        },
    )
    .expect("start");
    await_until(20, "node never became leader/serving", || node.can_serve());
    let dir_s = instance_dir.to_str().unwrap();

    let tier: u64 = 0x0123_4567_89AB_CDEF;
    let other: u64 = 42;
    for t in [tier, other] {
        node.offer_backup_claim_for_test(BackupWatermark {
            op: WatermarkOp::Report,
            tier_id: t,
            oldest_set: 0,
            newest_set: 0,
            through: 1024,
            reporter: 0,
        });
        await_until(20, "the watermark never committed", || {
            node.cluster_view().backups().iter().any(|e| e.tier_id == t)
        });
    }

    let r = run_ctl(&[
        "tier",
        "forget",
        "--tier-id",
        "0x0123456789abcdef",
        "--instance-dir",
        dir_s,
        "--app-id",
        APP,
    ]);
    assert_eq!(r.status, 0, "tier forget must succeed: {r:?}");
    // The id prints the way `tier show` prints it and `--tier-id` reads it
    // back: `0x` + 16 hex digits.
    assert!(
        r.stdout
            .contains("forgotten: tier 0x0123456789abcdef position="),
        "{r:?}"
    );
    await_until(20, "the forget never committed", || {
        !node
            .cluster_view()
            .backups()
            .iter()
            .any(|e| e.tier_id == tier)
    });
    assert!(
        node.cluster_view()
            .backups()
            .iter()
            .any(|e| e.tier_id == other),
        "only the named tier is forgotten"
    );

    let r = run_ctl(&[
        "tier",
        "forget",
        "--tier-id",
        "0x0123456789abcdef",
        "--instance-dir",
        dir_s,
        "--app-id",
        APP,
    ]);
    assert_ne!(r.status, 0, "an unknown tier must be refused: {r:?}");
    assert!(r.stdout.contains("tier_unknown"), "{r:?}");
    assert!(r.stderr.contains("refused: tier_unknown"), "{r:?}");

    // Decimal is accepted too.
    let r = run_ctl(&[
        "tier",
        "forget",
        "--tier-id",
        "42",
        "--instance-dir",
        dir_s,
        "--app-id",
        APP,
    ]);
    assert_eq!(r.status, 0, "decimal tier id: {r:?}");
    await_until(20, "the decimal forget never committed", || {
        node.cluster_view().backups().is_empty()
    });

    let audit = std::fs::read_to_string(instance_dir.join("audit.jsonl")).expect("audit.jsonl");
    let forgets: Vec<&str> = audit
        .lines()
        .filter(|l| l.contains("\"op_name\":\"tier_forget\""))
        .collect();
    assert_eq!(forgets.len(), 3, "{audit}");
    assert!(
        forgets[0].contains("tier 0123456789abcdef"),
        "{}",
        forgets[0]
    );
    assert!(forgets[1].contains("\"reason\":62"), "{}", forgets[1]);

    node.stop();
}
