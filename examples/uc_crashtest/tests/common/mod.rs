// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared harness for the v2 multi-process crash tests: spawn the
//! node-only and service-only binaries as real OS processes, poll for
//! readiness, and connect a `uc_client::Client`. `Reap` makes child
//! teardown panic-safe (kills + reaps on Drop, so a failing assert can't
//! leak node/service processes or their shared-memory files).
//!
//! Mirrors `examples/uc-crashtest/tests/common/mod.rs` (v1) closely; the
//! notable v2 difference is that everything here is SYNC — the v2 client
//! and node/service SDKs have no tokio runtime, so there is no `async`/
//! `.await` anywhere in this harness.
//!
//! Used by `smoke.rs` and `hard_crash.rs`; not every helper is used by both,
//! hence the crate-level `allow(dead_code)`.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Once;
use std::time::{Duration, Instant};

use uc_client::{Client, ClientError};
use uc_crypto::identity::Identity;
use uc_lincheck::register::{Cmd, CmdResp};

static INIT_CLIENT_TIMEOUT: Once = Once::new();

/// Shorten the client's default 10s per-request timeout (`UC2_CLIENT_TIMEOUT_MS`)
/// for the hard-crash tests. A client request in flight at the moment of a
/// real SIGKILL blocks until `send_and_await`'s `rx.recv_timeout` expires
/// before the client can classify `Timeout`/`InstanceRestart` and (in
/// `hard_crash.rs`) reconnect — at the 10s default that makes a practical
/// post-restart recovery-window assertion impossible within a fast test.
/// Idempotent and safe to call from every test/worker in a binary (they all
/// want the same value): the `Once` guarantees the `unsafe` env mutation
/// runs exactly once and every caller only proceeds (to its own
/// `Client::connect`) after it has completed.
pub fn shorten_client_timeout() {
    INIT_CLIENT_TIMEOUT.call_once(|| {
        // SAFETY: single mutation, gated by `Once`, and every caller
        // (including every worker thread) calls this before its own first
        // `Client::connect` — so no thread ever reads `UC2_CLIENT_TIMEOUT_MS`
        // concurrently with this write.
        unsafe { std::env::set_var("UC2_CLIENT_TIMEOUT_MS", "1500") };
    });
}

pub const NODE_BIN: &str = env!("CARGO_BIN_EXE_uc_crashtest-node");
pub const SERVICE_BIN: &str = env!("CARGO_BIN_EXE_uc_crashtest-service");
pub const APP_ID: &str = "uc_crashtest";

// -------------------------------------------------- M8 Task 15: crypto ON
//
// `UC2_CRYPTO=1` re-runs every hard-crash test in this crate with wire
// crypto `Enabled` on every real node PROCESS it spawns. Honored uniformly:
// every test below checks this once at the top and threads the answer
// through its own `spawn_node`/`spawn_node_multi` calls.
//
// This alone does NOT mean every test exercises a genuine multi-process
// seal/open path, though — that needs a real PEER to seal traffic with.
// `linearizable_under_service_sigkill` and `node_sigkill_recovery` boot
// SINGLE-node clusters (`node.crypto_epoch()` logs `for 0 peer(s)`): with
// no peer, no inter-node datagram is ever sealed, and client/service IPC is
// shmem, never sealed. Under crypto they prove only that enabling it
// doesn't break single-node boot/apply/recovery. `sigkill_mid_config_window`
// and `leader_node_sigkill_recovery_multi` are the real 3-PROCESS clusters
// (a genuine multi-process handshake + seal/open path, not the in-process
// fixture `uc_node/tests/crypto_cluster.rs`/`lincheck_v2` exercise) —
// `leader_node_sigkill_recovery_multi` in particular is the one that
// SIGKILLs a NODE process (not just the admin protocol) and so is the one
// that forces the restarted process to re-run the Noise handshake with its
// live peers before its consensus datagrams are accepted again.

/// `UC2_CRYPTO=1` re-runs the hard-crash capstones with wire crypto enabled.
pub fn crypto_from_env() -> bool {
    std::env::var("UC2_CRYPTO").ok().as_deref() == Some("1")
}

fn crypto_private_key(id: u32) -> [u8; 32] {
    let mut k = [0x71u8; 32];
    k[0..4].copy_from_slice(&id.to_le_bytes());
    k
}

/// Standard-alphabet base64 with padding, matching `uc_crypto::identity`'s
/// allowlist parser — same hand-rolled encoder as `uc_node/tests/
/// crypto_cluster.rs` and `lincheck_v2::mod`'s own fixtures (independent
/// test binaries, no shared crate to put one canonical copy in).
fn crypto_b64_32(bytes: &[u8; 32]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 0x3F) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 0x3F) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// One keypair per id in `ids` plus a single shared allowlist naming all of
/// them.
pub struct CrashtestCrypto {
    pub key_paths: HashMap<u32, PathBuf>,
    pub allowlist_path: PathBuf,
}

pub fn provision_crypto(dir: &Path, ids: &[u32]) -> CrashtestCrypto {
    let mut key_paths = HashMap::with_capacity(ids.len());
    let mut publics = Vec::with_capacity(ids.len());
    for &id in ids {
        let node_dir = dir.join(format!("keys{id}"));
        std::fs::create_dir_all(&node_dir).unwrap();
        let key_path = node_dir.join("node.key");
        std::fs::write(&key_path, crypto_private_key(id)).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let public = Identity::load(&key_path).unwrap().public_bytes();
        publics.push((id, public));
        key_paths.insert(id, key_path);
    }
    let mut text = String::new();
    for (id, public) in &publics {
        text.push_str(&format!("{id} {}\n", crypto_b64_32(public)));
    }
    let allowlist_path = dir.join("crypto-allowlist");
    std::fs::write(&allowlist_path, text).unwrap();
    CrashtestCrypto {
        key_paths,
        allowlist_path,
    }
}

/// Anti-vacuity (M8 Task 15): poll for `instance_dir/crypto_epoch_active`,
/// which the node bin's own background poll loop writes ONLY once
/// `Node::crypto_epoch()` reports `Some` — a real, leader-only group-key
/// mint (see `uc_crashtest-node.rs`'s main loop). This is checked from a
/// SEPARATE test process with no `Node` handle of its own, hence the
/// filesystem sentinel rather than an in-process counter read. Proof that
/// `--crypto-key`/`--crypto-allowlist` did something real, not merely that
/// they were parsed — a build where the switch silently no-opped would
/// still elect a leader and pass every other assertion in this file.
pub fn assert_crypto_epoch_active(instance_dir: &Path, timeout: Duration) {
    let path = instance_dir.join("crypto_epoch_active");
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "UC2_CRYPTO=1 but {} never appeared within {timeout:?} — this node never minted a \
             crypto group epoch as leader; wire crypto did not actually engage",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A spawned child that is killed (SIGKILL) and reaped on Drop. This is what
/// makes the tests panic-safe: an assertion failure unwinds through the
/// `Reap` and the node/service processes are torn down rather than
/// orphaned. Reassigning a `Reap` (e.g. to restart the service or node)
/// drops the old one — a hard kill + reap + respawn, which is exactly the
/// hard-crash-then-restart the fault loop wants.
pub struct Reap(pub Child);

impl Drop for Reap {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawn the node-only binary (creates instance_dir/cnc2.dat, runs the v2
/// sync single-node cluster on ephemeral `--bind`/`--id 0` defaults).
pub fn spawn_node(instance_dir: &Path) -> Reap {
    spawn_node_with(instance_dir, None)
}

/// As [`spawn_node`] but optionally passing `--crypto-key`/
/// `--crypto-allowlist` (M8 Task 15's `UC2_CRYPTO=1` path). `None` is
/// byte-for-byte the pre-M8 [`spawn_node`] behavior.
pub fn spawn_node_with(instance_dir: &Path, crypto: Option<(&Path, &Path)>) -> Reap {
    let mut cmd = Command::new(NODE_BIN);
    cmd.arg("--instance-dir")
        .arg(instance_dir)
        .arg("--app-id")
        .arg(APP_ID);
    if let Some((key_path, allowlist_path)) = crypto {
        cmd.arg("--crypto-key")
            .arg(key_path)
            .arg("--crypto-allowlist")
            .arg(allowlist_path);
    }
    let child = cmd
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {NODE_BIN}: {e}"));
    Reap(child)
}

/// Spawn the service-only binary (waits for cnc2.dat, attaches, runs the SM).
pub fn spawn_service(instance_dir: &Path) -> Reap {
    spawn_service_with(instance_dir, false)
}

/// As [`spawn_service`], but optionally passing `--sessioned` so the service
/// runs `Sessioned<RegisterSm>` (M12a Task 11). The flag must agree with the
/// edge's `session_envelope` in front of it; `false` is byte-for-byte the
/// pre-M12 [`spawn_service`] behavior.
pub fn spawn_service_with(instance_dir: &Path, sessioned: bool) -> Reap {
    let mut cmd = Command::new(SERVICE_BIN);
    cmd.arg("--instance-dir")
        .arg(instance_dir)
        .arg("--app-id")
        .arg(APP_ID);
    if sessioned {
        cmd.arg("--sessioned");
    }
    let child = cmd
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {SERVICE_BIN}: {e}"));
    Reap(child)
}

/// M14c2 T6: spawn the service-only binary declaring a specific `--service-id`
/// (unlike [`spawn_service`]/[`spawn_service_with`], which always boot the
/// implicit FSM 0 process) — one process per FSM in a multi-service cluster.
pub fn spawn_service_id(instance_dir: &Path, id: u8) -> Reap {
    let mut cmd = Command::new(SERVICE_BIN);
    cmd.arg("--instance-dir")
        .arg(instance_dir)
        .arg("--app-id")
        .arg(APP_ID)
        .arg("--service-id")
        .arg(id.to_string());
    let child = cmd
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {SERVICE_BIN}: {e}"));
    Reap(child)
}

/// M14c2 T6: spawn the node-only binary declaring `--services`/`--fsm-lag`
/// (Task 1) — a multi-FSM single-node cluster, unlike [`spawn_node`]/
/// [`spawn_node_with`] which always boot the implicit single-FSM default.
pub fn spawn_node_with_services(instance_dir: &Path, services: &str, fsm_lag: &str) -> Reap {
    let mut cmd = Command::new(NODE_BIN);
    cmd.arg("--instance-dir")
        .arg(instance_dir)
        .arg("--app-id")
        .arg(APP_ID)
        .arg("--services")
        .arg(services)
        .arg("--fsm-lag")
        .arg(fsm_lag);
    let child = cmd
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {NODE_BIN}: {e}"));
    Reap(child)
}

/// Poll for `path` to exist, up to `timeout`. Only meaningful for the FIRST
/// boot on a fresh instance dir (e.g. waiting for the very first
/// `cnc2.dat`) — after a node restart on a REUSED instance dir, the old
/// `cnc2.dat`/ring files are still sitting there until the new node process
/// gets around to unlinking + recreating them, so `path.exists()` returns
/// `true` instantly on the stale leftover and proves nothing about the NEW
/// node being up. Use [`wait_for_fresh_instance`] after a restart instead.
pub fn wait_for_path(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Full readiness barrier for a freshly-spawned node (initial boot, fresh
/// instance dir). A successful `Client::connect` opens AND validates every
/// IPC artifact in sequence — the cnc page plus all rings, each magic-checked
/// — so it is a true "node fully initialized" signal.
///
/// This is strictly stronger than `wait_for_path(cnc2.dat)`, which returns as
/// soon as the FIRST-created file exists: the node creates `cnc2.dat` before
/// its ring buffers (`node.rs` — the log buffer's creation *needs* the cnc
/// page, so the order can't be swapped), leaving a window where cnc2.dat is
/// present but a ring is still being created / magic-written. A service or
/// warm-up client attaching in that window sees `ring error: No such file` or
/// `magic mismatch` — harmless on a fast box (window ~0), but a real failure
/// on a loaded CI runner. Gate warm-up on this, not on file existence.
///
/// Fresh-boot only: on a respawn over a SAME dir use `wait_for_fresh_instance`,
/// since a stale leftover cnc2.dat can let `connect` validate the OLD page.
pub fn wait_for_ready(instance_dir: &Path, timeout: Duration) {
    drop(connect_with_retry(instance_dir, timeout));
}

/// Wait until a node respawned on the SAME instance dir has actually
/// completed its boot sequence, distinguishing it from the stale files left
/// behind by the node it replaced. `Client::connect` only succeeds once
/// EVERY one of the node's IPC files exists and validates (cnc page +
/// ingress/query/egress rings, opened in sequence inside `connect`), so a
/// successful connect is already a strong readiness signal — but a
/// left-behind cnc2.dat can validate too (same `app_id`) before the new node
/// has unlinked + recreated it, mmap'ing a stale/soon-to-be-orphaned page.
/// Requiring the reported `instance_id` to differ from `old_instance_id`
/// closes that race: only the FRESH node's boot can produce a new one
/// (`Node::start_with_socket` mints a random `instance_id` on every boot).
/// Returns the fresh `instance_id`.
pub fn wait_for_fresh_instance(
    instance_dir: &Path,
    old_instance_id: u128,
    timeout: Duration,
) -> u128 {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(c) = Client::connect(instance_dir, APP_ID) {
            let id = c.instance_id();
            c.shutdown();
            if id != old_instance_id {
                return id;
            }
        }
        assert!(
            Instant::now() < deadline,
            "node restart did not present a fresh instance_id (still {old_instance_id:#034x}) \
             within {timeout:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Connect a client, retrying until the node is ready to accept the attach
/// (the cnc2.dat file can exist a moment before the node has finished
/// creating every ring file).
pub fn connect_with_retry(instance_dir: &Path, timeout: Duration) -> Client {
    let deadline = Instant::now() + timeout;
    loop {
        match Client::connect(instance_dir, APP_ID) {
            Ok(c) => return c,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out connecting client: {e}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// True for errors that mean "not ready / retry the same op" rather than a
/// real failure: leadership not yet established for writes, or a transient
/// backpressure/retry signal. None of these can have committed a mutation
/// (the node's ingress drain proves `NotLeader`/`BackpressureFull`/`Retry`
/// fire strictly before any append), so retrying is safe here.
pub fn is_transient(e: &ClientError) -> bool {
    matches!(
        e,
        ClientError::NotLeader { .. } | ClientError::BackpressureFull | ClientError::Retry
    )
}

/// Submit a command, retrying transient errors (incl. `NotLeader` before the
/// leader's initial term-open frame commits) until `deadline`. Panics on a
/// non-transient error or deadline. For tests that require success (e.g. the
/// smoke test and the warm-up write); `hard_crash.rs` classifies outcomes
/// itself via its own `submit_cmd`/`read_leader`.
pub fn submit_until_ok(client: &Client, cmd: &Cmd, deadline: Instant) -> CmdResp {
    loop {
        match client.submit::<Cmd, CmdResp>(cmd) {
            Ok(r) => return r,
            Err(e) if is_transient(&e) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("submit {cmd:?} failed: {e}"),
        }
    }
}

/// Linearizable read, retrying transient errors until `deadline`.
pub fn read_until_ok(client: &Client, deadline: Instant) -> Option<u64> {
    loop {
        match client.query_linearizable::<(), Option<u64>>(&()) {
            Ok(v) => return v,
            Err(e) if is_transient(&e) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("read failed: {e}"),
        }
    }
}
