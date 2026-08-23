// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M12b Task 6 — end-to-end bin test for `uc2ctl`'s admin-auth surface:
//! `--admin-key`/`--admin-key-name` signing, the `gen-admin-key` and
//! `audit` verbs, and reason strings 20-24.
//!
//! Starts a real node IN-PROCESS (`uc2_node::Node::start_with`, the same
//! harness shape `uc2_node/tests/admin_auth.rs` uses to drive the library
//! seam) and shells out to the actual `uc2ctl` binary
//! (`env!("CARGO_BIN_EXE_uc2ctl")`) for every assertion — this is the one
//! test in the M12b arc that proves the CLI itself, not just the library
//! functions it calls, does the right thing end to end.

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use uc2_crypto::admin::{AdminKey, generate_key_file};
use uc2_net::fault::FaultConfig;
use uc2_node::{AdminPolicy, CryptoConfig, DEFAULT_JOURNAL_SEGMENT_BYTES, Node, NodeConfig, PurgePolicy};

const APP: &str = "ctlauth";

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
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 0x5150_1234_ABCD_0F0F,
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: CryptoConfig::Disabled,
    }
}

/// Starts a fresh 1-node cluster (a single voter is trivially its own
/// leader — the same fixture `add_learner_via_leader_cnc_is_accepted_and_converges`
/// and `uc2_node/tests/admin_auth.rs` rely on) under `root/<name>`.
fn start_node(root: &Path, name: &str, policy: AdminPolicy) -> (Node, PathBuf) {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let addr = sock.local_addr().unwrap();
    let instance_dir = root.join(name);
    let cfg = make_config(instance_dir.clone(), addr);
    let opts = uc2_node::StartOpts { socket: Some(sock), admin: policy };
    let node = Node::start_with(cfg, opts).expect("start");
    (node, instance_dir)
}

fn await_leader(node: &Node, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "node never became leader/serving");
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
    let out = Command::new(bin()).args(args).output().expect("spawn uc2ctl");
    Run {
        status: out.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&out.stdout).to_string(),
        stderr: String::from_utf8_lossy(&out.stderr).to_string(),
    }
}

#[test]
fn hmac_signing_audit_and_gen_key_end_to_end() {
    let root = tempfile::Builder::new()
        .prefix("uc2ctl-admin-auth-bin-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");

    // Key material: "ops-test" is registered with the node; "ops-other" is
    // never told to the node's policy — the unknown-key case for step 3.
    // File stems match the key names so the CLI's default
    // `--admin-key-name` (the file's stem) needs no override.
    let key_dir = root.path().join("keys");
    std::fs::create_dir_all(&key_dir).unwrap();
    let ops_test_path = key_dir.join("ops-test.key");
    let ops_other_path = key_dir.join("ops-other.key");
    generate_key_file(&ops_test_path).expect("gen ops-test key");
    generate_key_file(&ops_other_path).expect("gen ops-other key");
    let ops_test_key = AdminKey::load("ops-test", &ops_test_path).expect("load ops-test");

    let policy = AdminPolicy::Hmac { keys: Arc::new(vec![ops_test_key]), ttl: Duration::from_secs(30) };
    let (node, instance_dir) = start_node(root.path(), "n0", policy);
    await_leader(&node, 20);
    let dir_s = instance_dir.to_str().unwrap();

    // Step 1: no key -> reason 20 auth_missing, exit != 0.
    let r = run_ctl(&[
        "add-learner",
        "--instance-dir",
        dir_s,
        "--app-id",
        APP,
        "--id",
        "101",
        "--addr",
        "127.0.0.1:59101",
    ]);
    assert_ne!(r.status, 0, "an unsigned add-learner must fail under Hmac: {r:?}");
    assert!(
        r.stdout.contains("auth_missing") || r.stderr.contains("auth_missing"),
        "expected auth_missing: {r:?}"
    );

    // Step 2: --admin-key ops-test.key -> accepted, exit 0.
    let r = run_ctl(&[
        "add-learner",
        "--instance-dir",
        dir_s,
        "--app-id",
        APP,
        "--id",
        "102",
        "--addr",
        "127.0.0.1:59102",
        "--admin-key",
        ops_test_path.to_str().unwrap(),
    ]);
    assert_eq!(r.status, 0, "a validly signed add-learner must succeed: {r:?}");
    assert!(r.stdout.contains("accepted"), "stdout={}", r.stdout);

    // Fix round 1, minor 1: "accepted" alone doesn't prove the change
    // actually landed — cross-check with a real `status` read that the new
    // learner (id 102) is now a member and the config version advanced to 1
    // (this is the FIRST accepted change on this cluster: step 1 was
    // refused before ever reaching propose_config).
    let r_status = run_ctl(&["status", "--instance-dir", dir_s, "--app-id", APP]);
    assert_eq!(r_status.status, 0, "status must succeed: {r_status:?}");
    assert!(
        r_status.stdout.contains("config: version=1"),
        "config version did not advance: {}",
        r_status.stdout
    );
    assert!(
        r_status.stdout.contains("id=102 role=learner"),
        "learner 102 missing from status:\n{}",
        r_status.stdout
    );

    // Step 3: --admin-key ops-other.key (a real signature under a key name
    // the node's policy never loaded) -> reason 23 auth_unknown_key.
    let r = run_ctl(&[
        "add-learner",
        "--instance-dir",
        dir_s,
        "--app-id",
        APP,
        "--id",
        "103",
        "--addr",
        "127.0.0.1:59103",
        "--admin-key",
        ops_other_path.to_str().unwrap(),
    ]);
    assert_ne!(r.status, 0, "an unregistered key name must be refused: {r:?}");
    assert!(
        r.stdout.contains("auth_unknown_key") || r.stderr.contains("auth_unknown_key"),
        "expected auth_unknown_key: {r:?}"
    );

    // Step 4: `audit` shows the three records, in order: refused, accepted,
    // refused.
    let r = run_ctl(&["audit", "--instance-dir", dir_s]);
    assert_eq!(r.status, 0, "audit must succeed: {r:?}");
    let lines: Vec<&str> = r.stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 3, "expected 3 audit records:\n{}", r.stdout);
    assert!(lines[0].contains("refused"), "line0={}", lines[0]);
    assert!(lines[1].contains("accepted"), "line1={}", lines[1]);
    assert!(lines[2].contains("refused"), "line2={}", lines[2]);

    let r_json = run_ctl(&["audit", "--instance-dir", dir_s, "--json"]);
    assert_eq!(r_json.status, 0, "audit --json must succeed: {r_json:?}");
    let json_lines: Vec<&str> = r_json.stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(json_lines.len(), 3);
    for l in &json_lines {
        let t = l.trim();
        assert!(t.starts_with('{') && t.ends_with('}'), "not a JSON object: {l}");
        assert!(t.contains("\"event\":\"admin_op\""), "{l}");
    }

    // `--tail 1` shows only the most recent record.
    let r_tail = run_ctl(&["audit", "--instance-dir", dir_s, "--tail", "1"]);
    assert_eq!(r_tail.status, 0);
    let tail_lines: Vec<&str> = r_tail.stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(tail_lines.len(), 1, "stdout={}", r_tail.stdout);
    assert!(tail_lines[0].contains("refused"));

    // Fix round 1, minor 3: a garbage (non-JSON) line appended to
    // audit.jsonl by hand must not crash the printer — `format_audit_line`
    // returns `None` on it, `run_audit` falls back to printing it verbatim
    // with a `?` marker, and the command still exits 0.
    {
        use std::io::Write;
        let audit_path = instance_dir.join("audit.jsonl");
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&audit_path)
            .expect("open audit.jsonl to append garbage");
        writeln!(f, "not even remotely json").expect("append garbage line");
    }
    let r_garbage = run_ctl(&["audit", "--instance-dir", dir_s]);
    assert_eq!(r_garbage.status, 0, "a garbage audit line must not crash uc2ctl: {r_garbage:?}");
    let garbage_lines: Vec<&str> = r_garbage.stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(
        garbage_lines.len(),
        4,
        "expected the 3 real records plus the garbage line:\n{}",
        r_garbage.stdout
    );
    assert!(
        garbage_lines[3].starts_with("? "),
        "the garbage line should carry the ? marker: {}",
        garbage_lines[3]
    );
    assert!(garbage_lines[3].contains("not even remotely json"), "{}", garbage_lines[3]);

    // Step 5: gen-admin-key -> 32 bytes, 0600; second run refuses to
    // overwrite with a named error.
    let gen_path = root.path().join("generated.key");
    let r = run_ctl(&["gen-admin-key", gen_path.to_str().unwrap()]);
    assert_eq!(r.status, 0, "gen-admin-key must succeed: {r:?}");
    let meta = std::fs::metadata(&gen_path).expect("generated key file exists");
    assert_eq!(meta.len(), 32, "generated key must be 32 bytes");
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(meta.mode() & 0o777, 0o600, "generated key must be mode 0600");
    }
    // Fix round 1, minor 2: the printed snippet must name the key file's
    // stem ("generated") and the file's own absolute path — not just SOME
    // output.
    let gen_abs = std::fs::canonicalize(&gen_path).expect("generated key file exists");
    assert!(
        r.stdout.contains("name = \"generated\""),
        "snippet missing the key name:\n{}",
        r.stdout
    );
    assert!(
        r.stdout.contains(gen_abs.to_str().expect("utf8 path")),
        "snippet missing the absolute key_path:\n{}",
        r.stdout
    );

    let r2 = run_ctl(&["gen-admin-key", gen_path.to_str().unwrap()]);
    assert_ne!(r2.status, 0, "gen-admin-key must refuse to overwrite an existing file");
    assert!(
        r2.stderr.contains(gen_path.to_str().unwrap()) || r2.stdout.contains(gen_path.to_str().unwrap()),
        "the overwrite refusal should name the path: {r2:?}"
    );

    node.stop();
}

/// Legacy posture, unchanged: under `AdminPolicy::Filesystem` (the
/// default), an unsigned add-learner is accepted — the instance
/// directory's permissions are the only boundary, exactly as before M12b.
#[test]
fn filesystem_policy_accepts_add_learner_with_no_key() {
    let root = tempfile::Builder::new()
        .prefix("uc2ctl-admin-auth-bin-fs-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");

    let (node, instance_dir) = start_node(root.path(), "n0", AdminPolicy::Filesystem);
    await_leader(&node, 20);

    let r = run_ctl(&[
        "add-learner",
        "--instance-dir",
        instance_dir.to_str().unwrap(),
        "--app-id",
        APP,
        "--id",
        "201",
        "--addr",
        "127.0.0.1:59201",
    ]);
    assert_eq!(r.status, 0, "Filesystem policy must accept an unsigned add-learner: {r:?}");
    assert!(r.stdout.contains("accepted"), "stdout={}", r.stdout);

    node.stop();
}
