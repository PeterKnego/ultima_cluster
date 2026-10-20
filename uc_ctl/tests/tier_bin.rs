// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Backup-tier spec §8.2 — end-to-end bin tests for the OFFLINE `uc2ctl
//! tier restore`, `tier fork` and `tier export-corpus` verbs.
//!
//! The `tier_forget_bin.rs` shape: a real one-voter node IN-PROCESS — here
//! with a backup writer and the kv example's `Sessioned<KvSm>` attached —
//! fills a tier; then the node stops and the actual `uc2ctl` binary builds
//! from it. `export-corpus`'s corpus is replayed by the real
//! `uc2-diffreplay determinism` with the real `kv-service` as the subject,
//! exactly as `kv_store`'s `regression_corpora` does. Both binaries live
//! beside `uc2ctl` in the cargo target directory and are asserted, not
//! skipped:
//!
//!     cargo build -p uc_diffreplay -p kv_store

use std::net::UdpSocket;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use kv_store::{KV_VERSION, KvSm, wire};
use uc_client::{Engine, EngineConfig, Outcome, PollHalf, SendHalf};
use uc_net::fault::FaultConfig;
use uc_node::tier::chain::{list_tier, watermark_of};
use uc_node::tier::target::FsTarget;
use uc_node::tier::writer::BackupConfig;
use uc_node::{AdminPolicy, CryptoConfig, Node, NodeConfig, PurgePolicy, ServicesConfig};
use uc_service::{
    RawStateMachine, ServiceBuilder, ServiceConfig, SessionConfig, Sessioned, TAG_FRESH,
};

const APP: &str = "ctltier";

#[derive(Debug)]
struct Run {
    status: i32,
    stdout: String,
    stderr: String,
}

fn run(bin: &Path, args: &[&str]) -> Run {
    let out = Command::new(bin)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn {}: {e}", bin.display()));
    Run {
        status: out.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&out.stdout).to_string(),
        stderr: String::from_utf8_lossy(&out.stderr).to_string(),
    }
}

fn uc2ctl() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_uc2ctl"))
}

/// A binary of another package, beside `uc2ctl`: asserted, never skipped.
fn sibling(name: &str) -> PathBuf {
    let mut p = uc2ctl();
    p.set_file_name(name);
    assert!(
        p.exists(),
        "missing {} — run `cargo build -p uc_diffreplay -p kv_store` first",
        p.display()
    );
    p
}

fn await_until(secs: u64, msg: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "{msg}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// The bytes a live `kv` request carries: the `Sessioned` envelope
/// (`client_id ‖ seq`, both u64 LE) then the KV wire frame.
fn enveloped(seq: u64, frame: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(16 + frame.len());
    v.extend_from_slice(&1u64.to_le_bytes());
    v.extend_from_slice(&seq.to_le_bytes());
    v.extend_from_slice(frame);
    v
}

/// Submit one command over the real shmem path and block until it applied
/// fresh (`kv_store`'s `gen_corpus.rs` shape).
fn submit(send: &SendHalf, poll: &mut PollHalf, seq: u64, frame: &[u8]) {
    let payload = enveloped(seq, frame);
    let deadline = Instant::now() + Duration::from_secs(30);
    while let Err(e) = send.try_submit(seq, &payload) {
        assert!(Instant::now() < deadline, "submit {seq}: {e}");
        std::thread::sleep(Duration::from_millis(2));
    }
    let mut got: Option<Vec<u8>> = None;
    while got.is_none() {
        assert!(Instant::now() < deadline, "no completion for {seq}");
        poll.poll(|c| match c.outcome {
            Outcome::Response(b) => got = Some(b.to_vec()),
            ref other => panic!("seq {seq}: {other:?}"),
        });
    }
    assert_eq!(got.unwrap().first().copied(), Some(TAG_FRESH), "seq {seq}");
}

fn instant(node: &Node) -> u64 {
    let p = loop {
        match node.command_snapshot(false) {
            Ok(p) => break p,
            Err(uc_node::SnapshotRefusal::Retry) => std::thread::sleep(Duration::from_millis(5)),
            Err(e) => panic!("snapshot refused: {e}"),
        }
    };
    await_until(30, "the instant completed", || {
        node.snapshot_set_position() >= p
    });
    p
}

/// A stopped one-node kv cluster's tier: puts below a set S, a delete and
/// more puts above it, all reached by the tier's chain. Returns `(scratch
/// root, tier dir, S, through)`.
fn kv_tier() -> (tempfile::TempDir, PathBuf, u64, u64) {
    let root = tempfile::Builder::new()
        .prefix("uc2ctl-tier-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let tier = root.path().join("tier");
    std::fs::create_dir_all(&tier).unwrap();
    let inst = root.path().join("n0");
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let addr = sock.local_addr().unwrap();
    let cfg = NodeConfig {
        id: 0,
        members: vec![(0, addr)],
        learners: Vec::new(),
        bind: addr,
        instance_dir: inst.clone(),
        app_id: APP.into(),
        buffer_bytes: 1 << 22,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        force_jumbo_frames: false,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 0x7E57_71E2,
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: 256 * 1024,
        crypto: CryptoConfig::Disabled,
        services: ServicesConfig::single(<KvSm as RawStateMachine>::NAME),
    };
    let node = Node::start_with(
        cfg,
        uc_node::StartOpts {
            socket: Some(sock),
            admin: AdminPolicy::Filesystem,
            backup: Some(BackupConfig {
                target: tier.clone(),
                chunk_bytes: 1 << 20,
                quiet_interval: Duration::from_millis(300),
                ..BackupConfig::default()
            }),
            tier_source: None,
            wire: None,
        },
    )
    .expect("start");
    await_until(20, "node never served", || node.can_serve());
    let mut scfg = ServiceConfig::new(inst.clone(), APP.to_string());
    scfg.boot_wait = Duration::from_secs(30);
    let svc = ServiceBuilder::new(
        scfg,
        Sessioned::new(KvSm::default(), SessionConfig::default()),
    )
    .start()
    .expect("kv service");

    let (send, mut poll) = Engine::attach(&inst, APP, EngineConfig::default()).expect("engine");
    submit(&send, &mut poll, 0, &wire::encode_put(b"a", b"1"));
    submit(&send, &mut poll, 1, &wire::encode_put(b"b", b"2"));
    let s = instant(&node);
    submit(&send, &mut poll, 2, &wire::encode_delete(b"a"));
    submit(&send, &mut poll, 3, &wire::encode_put(b"c", b"3"));
    drop(send);
    let target = node.counters().commit.load_acquire();
    let t = FsTarget::new(&tier);
    await_until(60, "the tier reached the span", || {
        matches!(
            watermark_of(&list_tier(&t).unwrap()),
            Some((_, newest, through)) if newest >= s && through >= target
        )
    });
    svc.stop();
    node.stop();
    let (_, _, through) = watermark_of(&list_tier(&t).unwrap()).unwrap();
    (root, tier, s, through)
}

/// `tier restore`: a wrong `--confirm-cluster` and a reused `--new-node`
/// id are refused by name and build nothing; the right app id builds a
/// directory `verify_artifact` accepts. `tier fork` under the tier's own
/// app id is refused `fork_same_app_id`; under a new one it builds and
/// forces the single voter.
#[test]
fn tier_restore_and_fork_refuse_by_name_and_build() {
    let (root, tier, s, through) = kv_tier();
    let (ctl, tier_s) = (uc2ctl(), tier.to_str().unwrap());
    let out = root.path().join("r");
    let out_s = out.to_str().unwrap();

    let r = run(
        &ctl,
        &[
            "tier",
            "restore",
            "--target",
            tier_s,
            "--instance-dir",
            out_s,
            "--confirm-cluster",
            "nope",
        ],
    );
    assert_eq!(r.status, 1, "{r:?}");
    assert!(r.stderr.contains("--confirm-cluster"), "{r:?}");
    assert!(!out.exists());

    let r = run(
        &ctl,
        &[
            "tier",
            "restore",
            "--target",
            tier_s,
            "--instance-dir",
            out_s,
            "--new-node",
            "--node-id",
            "9",
        ],
    );
    assert_eq!(r.status, 0, "{r:?}");
    assert!(r.stdout.contains("restored:"), "{r:?}");
    assert!(r.stdout.contains("add-learner 9"), "{r:?}");
    std::fs::remove_dir_all(&out).unwrap();

    let at = through.to_string();
    let r = run(
        &ctl,
        &[
            "tier",
            "restore",
            "--target",
            tier_s,
            "--at",
            &at,
            "--instance-dir",
            out_s,
            "--confirm-cluster",
            APP,
        ],
    );
    assert_eq!(r.status, 0, "{r:?}");
    assert!(
        r.stdout
            .contains(&format!("history after {through} is discarded")),
        "{r:?}"
    );
    let rep = uc_node::backup::verify_artifact(&out).expect("verify accepts the restore");
    assert_eq!(rep.journal_last_pos, through);
    assert!(rep.journal_first_base >= s);

    let fork = root.path().join("f");
    let fork_s = fork.to_str().unwrap();
    let r = run(
        &ctl,
        &[
            "tier",
            "fork",
            "--target",
            tier_s,
            "--instance-dir",
            fork_s,
            "--app-id",
            APP,
            "--node-id",
            "0",
        ],
    );
    assert_eq!(r.status, 1, "{r:?}");
    assert!(r.stderr.contains("fork_same_app_id"), "{r:?}");
    assert!(!fork.exists());

    let r = run(
        &ctl,
        &[
            "tier",
            "fork",
            "--target",
            tier_s,
            "--instance-dir",
            fork_s,
            "--app-id",
            "ctltier-fork",
            "--node-id",
            "0",
        ],
    );
    assert_eq!(r.status, 0, "{r:?}");
    assert!(r.stdout.contains("no output at or below"), "{r:?}");
    assert!(r.stdout.contains("forked:"), "{r:?}");
    let rc = uc_node::recovery::recovered_config(&fork).expect("recovered config");
    assert_eq!(rc.voters.iter().map(|v| v.0).collect::<Vec<_>>(), vec![0]);
    let st = uc_log::state::NodeState::open(&fork.join("state")).unwrap();
    assert_eq!(st.output_progress(), through, "a fork replays no outputs");
}

/// `tier export-corpus` builds a corpus `uc2-diffreplay` reads unchanged:
/// the `CORPUS` manifest names the app, the row, origin S, end Q and the
/// row's version from the set, and a determinism run of the real
/// `kv-service` over it passes. An unknown row is refused by name.
#[test]
fn export_corpus_passes_determinism_mode() {
    let (diffreplay, kv) = (sibling("uc2-diffreplay"), sibling("kv-service"));
    let (root, tier, s, through) = kv_tier();
    let (ctl, tier_s) = (uc2ctl(), tier.to_str().unwrap());
    let out = root.path().join("corpus");
    let out_s = out.to_str().unwrap();
    let (from, to) = (s.to_string(), through.to_string());

    let r = run(
        &ctl,
        &[
            "tier",
            "export-corpus",
            "--target",
            tier_s,
            "--row",
            "3",
            "--to",
            &to,
            "--out",
            out_s,
        ],
    );
    assert_eq!(r.status, 1, "{r:?}");
    assert!(r.stderr.contains("corpus_row_unknown"), "{r:?}");
    assert!(!out.exists());

    let r = run(
        &ctl,
        &[
            "tier",
            "export-corpus",
            "--target",
            tier_s,
            "--row",
            "0",
            "--from",
            &from,
            "--to",
            &to,
            "--out",
            out_s,
        ],
    );
    assert_eq!(r.status, 0, "{r:?}");
    let m = uc_diffreplay::corpus::CorpusManifest::read(&out).expect("CORPUS");
    assert_eq!(
        m,
        uc_diffreplay::corpus::CorpusManifest {
            app_id: APP.into(),
            row: 0,
            origin: s,
            end: through,
            version: KV_VERSION,
        }
    );
    let c = uc_diffreplay::corpus::Corpus::open(&out).expect("a corpus");
    assert!(c.artifact().is_file(), "{}", c.artifact().display());

    let report = root.path().join("det.json");
    let r = run(
        &diffreplay,
        &[
            "determinism",
            "--corpus",
            out_s,
            "--bin",
            kv.to_str().unwrap(),
            "--report",
            report.to_str().unwrap(),
        ],
    );
    assert_eq!(r.status, 0, "determinism failed: {r:?}");
}
