//! Shared harness for the multi-process crash tests: spawn the node-only and
//! service-only binaries as real OS processes, poll for readiness, and connect a
//! `uc_client`. `Reap` makes child teardown panic-safe (kills + reaps on Drop, so a
//! failing assert can't leak node/service processes or their `/dev/shm` rings).
//!
//! Used by `smoke.rs` and `hard_crash.rs`; not every helper is used by both, hence
//! the crate-level `allow(dead_code)`.
#![allow(dead_code)]

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use uc_client::{Client, ClientError};
use uc_lincheck::register::{Cmd, CmdResp};

pub const NODE_BIN: &str = env!("CARGO_BIN_EXE_uc-crashtest-node");
pub const SERVICE_BIN: &str = env!("CARGO_BIN_EXE_uc-crashtest-service");
pub const APP_ID: &str = "uc-crashtest";

/// A spawned child that is killed (SIGKILL) and reaped on Drop. This is what makes
/// the tests panic-safe: an assertion failure unwinds through the `Reap` and the
/// node/service processes are torn down rather than orphaned. Reassigning a `Reap`
/// (e.g. to restart the service) drops the old one — a hard kill + reap + respawn,
/// which is exactly the hard-crash-then-restart the fault loop wants.
pub struct Reap(pub Child);

impl Drop for Reap {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(bin: &str, instance_dir: &Path, data_dir: &Path) -> Reap {
    let child = Command::new(bin)
        .arg("--instance-dir")
        .arg(instance_dir)
        .arg("--data-dir")
        .arg(data_dir)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {bin}: {e}"));
    Reap(child)
}

/// Spawn the node-only binary (creates the instance_dir/cnc.dat, runs raft).
pub fn spawn_node(instance_dir: &Path, data_dir: &Path) -> Reap {
    spawn(NODE_BIN, instance_dir, data_dir)
}

/// Spawn the service-only binary (waits for cnc.dat, attaches, runs the SM).
pub fn spawn_service(instance_dir: &Path, data_dir: &Path) -> Reap {
    spawn(SERVICE_BIN, instance_dir, data_dir)
}

/// Poll for `path` to exist, up to `timeout`.
pub async fn wait_for_path(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Connect a client, retrying until the service is READY (connect can fail before
/// the handshake completes).
pub async fn connect_with_retry(instance_dir: &Path, timeout: Duration) -> Client {
    let deadline = Instant::now() + timeout;
    loop {
        match Client::connect(instance_dir, APP_ID).await {
            Ok(c) => return c,
            Err(e) => {
                assert!(Instant::now() < deadline, "timed out connecting client: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// True for errors that mean "not ready / retry the same op" rather than a real
/// failure: leadership not yet established for writes, or a transient stall.
pub fn is_transient(e: &ClientError) -> bool {
    matches!(
        e,
        ClientError::NotLeader { .. }
            | ClientError::BackpressureFull
            | ClientError::Timeout(_)
            | ClientError::NodeStalled
            | ClientError::ServiceStalled
            | ClientError::ResponseOverwritten
    )
}

/// Submit a command, retrying transient errors (incl. `NotLeader` before the
/// leader's initial entry commits) until `deadline`. Panics on a non-transient
/// error or deadline. For tests that require success (e.g. the smoke test);
/// `hard_crash.rs` classifies outcomes itself.
pub async fn submit_until_ok(client: &Client, cmd: &Cmd, deadline: Instant) -> CmdResp {
    loop {
        match client.submit::<Cmd, CmdResp>(cmd).await {
            Ok(r) => return r,
            Err(e) if is_transient(&e) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("submit {cmd:?} failed: {e}"),
        }
    }
}

/// Linearizable read, retrying transient errors until `deadline`.
pub async fn read_until_ok(client: &Client, deadline: Instant) -> Option<u64> {
    loop {
        match client.query_linearizable::<(), Option<u64>>(&()).await {
            Ok(v) => return v,
            Err(e) if is_transient(&e) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("read failed: {e}"),
        }
    }
}
