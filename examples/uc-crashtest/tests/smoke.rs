//! Multi-process smoke test: spawn the node-only and service-only binaries as
//! SEPARATE OS processes sharing one instance_dir, connect a client, write 7,
//! read 7 back. Proves the two reference binaries rendezvous over shmem.
//!
//! Gated behind `hard-crash-tests` because it spawns real processes and is the
//! foundation for the later hard-crash (SIGKILL) tests.
#![cfg(feature = "hard-crash-tests")]

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use uc_client::Client;
use uc_lincheck::register::{Cmd, CmdResp};

const NODE_BIN: &str = env!("CARGO_BIN_EXE_uc-crashtest-node");
const SERVICE_BIN: &str = env!("CARGO_BIN_EXE_uc-crashtest-service");
const APP_ID: &str = "uc-crashtest";

/// Spawn the node-only binary. stdout/stderr inherited for debuggability.
fn spawn_node(instance_dir: &Path, data_dir: &Path) -> Child {
    Command::new(NODE_BIN)
        .arg("--instance-dir")
        .arg(instance_dir)
        .arg("--data-dir")
        .arg(data_dir)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn node binary")
}

/// Spawn the service-only binary. stdout/stderr inherited for debuggability.
fn spawn_service(instance_dir: &Path, data_dir: &Path) -> Child {
    Command::new(SERVICE_BIN)
        .arg("--instance-dir")
        .arg(instance_dir)
        .arg("--data-dir")
        .arg(data_dir)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn service binary")
}

/// Poll for `path` to exist, up to `timeout`.
async fn wait_for_path(path: &Path, timeout: Duration) {
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
async fn connect_with_retry(instance_dir: &Path, timeout: Duration) -> Client {
    let deadline = Instant::now() + timeout;
    loop {
        match Client::connect(instance_dir, APP_ID).await {
            Ok(c) => return c,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "timed out connecting client: {e}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[tokio::test]
async fn write_then_read_across_processes() {
    let tmp = tempfile::tempdir().unwrap();
    let inst = tmp.path().join("inst");
    let nodedata = tmp.path().join("nodedata");
    let svcdata = tmp.path().join("svcdata");
    std::fs::create_dir_all(&inst).unwrap();

    // ── Spawn node, wait for cnc.dat, then spawn service ────────────────────
    let mut node = spawn_node(&inst, &nodedata);
    wait_for_path(&inst.join("cnc.dat"), Duration::from_secs(10)).await;
    let mut svc = spawn_service(&inst, &svcdata);

    // ── Connect client; wait for leadership ─────────────────────────────────
    let client = connect_with_retry(&inst, Duration::from_secs(10)).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while client.current_leader().is_none() {
        assert!(Instant::now() < deadline, "leader never converged");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ── Write 7, read 7 ─────────────────────────────────────────────────────
    let r: CmdResp = client.submit(&Cmd::Write(7)).await.unwrap();
    assert!(matches!(r, CmdResp::WriteAck), "expected WriteAck, got {r:?}");

    let v: Option<u64> = client.query_linearizable(&()).await.unwrap();
    assert_eq!(v, Some(7), "read should observe the committed write");

    // ── Teardown: client, then service, then node; reap children ────────────
    client.shutdown().await.ok();
    svc.kill().ok();
    node.kill().ok();
    svc.wait().ok();
    node.wait().ok();
}
