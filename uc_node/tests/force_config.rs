// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M11 Task 4: `uc_node::recovery::force_single_member` — the unit layer
//! (refusals + the version/position math). The quorum-loss e2e (3-node
//! cluster, SIGKILL two, force, restart, repair) lives in
//! `examples/uc_crashtest/tests/survival.rs`, behind `survival-tests`
//! (multi-process, out of scope for this in-process suite).

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use uc_log::state::{ConfigRecord, NodeState, StoredConfig, StoredMember};
use uc_node::recovery::{data_loss_statement, force_single_member, recovered_config};
use uc_node::{CryptoConfig, DEFAULT_JOURNAL_SEGMENT_BYTES, Node, NodeConfig, PurgePolicy};

const PAYLOAD: usize = 96;
const RING: usize = 1 << 20;

/// A tempdir on the ext4 target volume, never `/tmp` (RAM-backed tmpfs, no
/// swap — CLAUDE.md).
fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-force-config-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

fn config_for(id: u32, addr: SocketAddr, instance_dir: PathBuf) -> NodeConfig {
    NodeConfig {
        id,
        members: vec![(id, addr)],
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: "force-config".into(),
        buffer_bytes: RING,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 0x1122_3344_5566_7788,
        faults: Default::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::none_for_tests(),
    }
}

fn stored_member(id: u32, a: SocketAddr) -> StoredMember {
    match a.ip() {
        std::net::IpAddr::V4(v4) => StoredMember { id, ip: u32::from(v4), port: a.port() },
        std::net::IpAddr::V6(_) => panic!("ipv4 only"),
    }
}

/// Pre-seed `instance_dir/state/config.state` directly — the `learner.rs:412`
/// pattern — without booting a node, exactly what an offline recovery tool
/// must be able to read.
fn seed_config(instance_dir: &Path, version: u64, voters: &[(u32, SocketAddr)], tombstones: Vec<u32>) {
    std::fs::create_dir_all(instance_dir.join("state")).unwrap();
    let cfg = StoredConfig {
        version,
        voters: voters.iter().map(|(id, a)| stored_member(*id, *a)).collect(),
        learners: Vec::new(),
        tombstones,
    };
    let rec = ConfigRecord { position: 0, config: cfg.clone(), prev_position: 0, prev: cfg };
    NodeState::open(&instance_dir.join("state")).unwrap().store_config_record(&rec).unwrap();
}

fn free_addr() -> SocketAddr {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    s.local_addr().unwrap()
}

#[test]
fn force_refuses_a_running_node() {
    let dir = tempdir();
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = sock.local_addr().unwrap();
    let node = Node::start_with_socket(config_for(0, addr, dir.path().to_path_buf()), sock)
        .expect("start");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "sole voter never served");
        std::thread::yield_now();
    }

    let err =
        force_single_member(dir.path(), 0).expect_err("must refuse while a node holds the flock");
    let msg = err.to_string();
    assert!(msg.to_lowercase().contains("running"), "unexpected refusal message: {msg}");

    drop(node);
}

#[test]
fn force_refuses_a_tombstoned_id() {
    let dir = tempdir();
    let addr: SocketAddr = "127.0.0.1:59901".parse().unwrap();
    seed_config(dir.path(), 3, &[(7, addr)], vec![7]);

    let err = force_single_member(dir.path(), 7).expect_err("must refuse a tombstoned id");
    assert!(err.to_string().to_lowercase().contains("tombston"), "unexpected message: {err}");
}

#[test]
fn force_refuses_a_non_member_id() {
    let dir = tempdir();
    let addr: SocketAddr = "127.0.0.1:59902".parse().unwrap();
    seed_config(dir.path(), 2, &[(1, addr)], Vec::new());

    let err = force_single_member(dir.path(), 99)
        .expect_err("must refuse an id that isn't a voter or learner");
    assert!(err.to_string().to_lowercase().contains("member"), "unexpected message: {err}");
}

/// The version/position math (plan's anchor map row): a 3-voter config at
/// version 7, forced onto voter 1 — `recovered_config` afterward must report
/// version 8, id 1 as the sole voter (at its EXISTING address), and the
/// tombstone set unchanged (empty here).
#[test]
fn force_bumps_version_and_narrows_to_sole_voter() {
    let dir = tempdir();
    let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    let voters: Vec<(u32, SocketAddr)> = (0..3u32).map(|i| (i, addrs[i as usize])).collect();
    seed_config(dir.path(), 7, &voters, Vec::new());

    let report = force_single_member(dir.path(), 1).expect("force");
    assert_eq!(report.old_version, 7);
    assert_eq!(report.new_version, 8);
    assert_eq!(report.durable, 0);
    let mut dropped = report.dropped_peers.clone();
    dropped.sort_unstable();
    assert_eq!(dropped, vec![0, 2]);

    let recovered = recovered_config(dir.path()).expect("recovered_config");
    assert_eq!(recovered.version, 8);
    assert_eq!(recovered.voters, vec![(1, addrs[1])]);
    assert!(recovered.learners.is_empty());
    assert!(recovered.tombstones.is_empty(), "dropped peers must NOT be tombstoned");
}

/// Tombstones present before the force must survive it unchanged (Global
/// Constraints: force never adds OR removes tombstones).
#[test]
fn force_leaves_pre_existing_tombstones_untouched() {
    let dir = tempdir();
    let addrs: Vec<SocketAddr> = (0..2).map(|_| free_addr()).collect();
    let voters: Vec<(u32, SocketAddr)> = (0..2u32).map(|i| (i, addrs[i as usize])).collect();
    seed_config(dir.path(), 4, &voters, vec![99]);

    force_single_member(dir.path(), 0).expect("force");

    let recovered = recovered_config(dir.path()).expect("recovered_config");
    assert_eq!(recovered.tombstones, vec![99]);
}

/// Fix round 1, Critical 1 — the reviewer's empirical probe, made
/// permanent: BEFORE the fix, a totally fresh, never-booted instance dir hit
/// the "not a member" refusal only AFTER `recover_config_record`'s
/// genesis-seed path had already PERSISTED an empty, zero-voter version-0
/// record (`state.config_record()` was `None` going in, so the seed fired).
/// This proves `state/config.state` is byte-identical before and after a
/// refused call — not merely that the call returns `Err`.
#[test]
fn force_refuses_an_uninitialized_dir_without_persisting() {
    let dir = tempdir();
    std::fs::create_dir_all(dir.path().join("state")).unwrap();
    // Materialize state/*.state (create-if-absent StableValue files, no
    // value ever stored) exactly as the recovery path itself would, so
    // `before` is a genuine "freshly created, never written" byte image.
    drop(NodeState::open(&dir.path().join("state")).unwrap());
    let config_state = dir.path().join("state").join("config.state");
    let before = std::fs::read(&config_state).unwrap();

    let err = force_single_member(dir.path(), 5)
        .expect_err("a fresh instance dir has no config record yet");
    assert!(
        err.to_string().to_lowercase().contains("no durable config record"),
        "unexpected message: {err}"
    );

    let after = std::fs::read(&config_state).unwrap();
    assert_eq!(before, after, "force_single_member must not write anything when it refuses");
    assert!(
        NodeState::open(&dir.path().join("state")).unwrap().config_record().is_none(),
        "no record must have been persisted by the refused call"
    );
}

/// Fix round 1, Critical 1: the doubly-ahead compounding-crash window
/// (`recover_config_record`'s own doc: two config adoptions durably
/// persisted before any archive catch-up, in the same crash — nothing
/// genuine left to revert to) must refuse rather than silently falling back
/// to an empty seed that would overwrite the crashed survivor's real
/// membership. Pre-seeds a record directly (the `learner.rs:412` pattern)
/// with BOTH `position` and `prev_position` above the recovered durable
/// frontier (`0`, for a fresh empty journal).
#[test]
fn force_refuses_the_doubly_ahead_crash_window_without_persisting() {
    let dir = tempdir();
    let addr: SocketAddr = "127.0.0.1:59932".parse().unwrap();
    std::fs::create_dir_all(dir.path().join("state")).unwrap();
    let cfg = StoredConfig {
        version: 5,
        voters: vec![stored_member(1, addr)],
        learners: Vec::new(),
        tombstones: Vec::new(),
    };
    let prev_cfg = StoredConfig { version: 4, ..cfg.clone() };
    let rec = ConfigRecord { position: 200, config: cfg, prev_position: 100, prev: prev_cfg };
    NodeState::open(&dir.path().join("state")).unwrap().store_config_record(&rec).unwrap();
    let config_state = dir.path().join("state").join("config.state");
    let before = std::fs::read(&config_state).unwrap();

    let err = force_single_member(dir.path(), 1)
        .expect_err("must refuse the doubly-ahead crash window, not fall back to an empty seed");
    assert!(err.to_string().to_lowercase().contains("doubly-ahead"), "unexpected message: {err}");

    let after = std::fs::read(&config_state).unwrap();
    assert_eq!(before, after, "force_single_member must not write anything when it refuses");
}

/// Fix round 1, Important 3: `uc2ctl`'s data-loss statement is lifted into
/// `uc_node::recovery::data_loss_statement` so it cannot drift from what is
/// tested — pinned here, byte-for-byte, against the brief's exact wording.
#[test]
fn data_loss_statement_pins_the_exact_wording() {
    let msg = data_loss_statement(2, 128, &[0, 1]);
    assert_eq!(
        msg,
        "forcing node 2 to a single-member cluster at durable position 128: any write \
         acknowledged by the old quorum but not held in this node's journal is LOST; peers \
         [0, 1] are dropped from the config and must be wiped and rejoined as fresh learners."
    );
}

/// After a force, a fresh boot on the survivor's own instance dir must adopt
/// the forced sole-voter config, elect itself (quorum of 1), and commit
/// submitted writes — no restart-time special-casing, `ElectionSm` simply
/// reads the adopted config.
#[test]
fn forced_single_node_boots_elects_and_serves() {
    let dir = tempdir();
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = sock.local_addr().unwrap();
    // A 3-voter genesis seed where only id 0 (this node) is ever actually
    // bound/booted — ids 1/2 stand in for the dead old quorum.
    let other_a: SocketAddr = "127.0.0.1:59911".parse().unwrap();
    let other_b: SocketAddr = "127.0.0.1:59912".parse().unwrap();
    seed_config(dir.path(), 0, &[(0, addr), (1, other_a), (2, other_b)], Vec::new());

    let report = force_single_member(dir.path(), 0).expect("force");
    assert_eq!(report.new_version, 1);

    let node = Node::start_with_socket(config_for(0, addr, dir.path().to_path_buf()), sock)
        .expect("start forced sole voter");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "forced sole voter never served within 5s");
        std::thread::yield_now();
    }

    // submit + confirm it actually committed (quorum-of-1 in practice).
    let mut p = vec![0u8; PAYLOAD];
    p[..8].copy_from_slice(&42u64.to_le_bytes());
    let submit_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match node.submit(p.clone()) {
            Ok(()) => break,
            Err(_) => {
                assert!(
                    Instant::now() < submit_deadline,
                    "submit never succeeded on forced sole voter"
                );
                std::thread::yield_now();
            }
        }
    }
    let commit_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let c = node.counters();
        let append = c.append.load_acquire();
        if append > 0 && c.commit.load_acquire() == append && c.durable.load_acquire() == append {
            break;
        }
        assert!(Instant::now() < commit_deadline, "forced sole voter never committed the submit");
        std::thread::yield_now();
    }
}
