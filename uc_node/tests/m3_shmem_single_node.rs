//! First end-to-end shmem test: a `NodeBuilder` in `IpcMode::Shmem`
//! paired with a `uc_service::ServiceBuilder::run` running in the same
//! process. A submitted command goes:
//!
//! ```text
//! handle.submit(Inc(5))
//!   → openraft client_write
//!   → replicate (single-node = local commit)
//!   → ShmemAdaptedStateMachine::apply
//!     → publish on service/apply.ring
//!   → uc_service apply_loop consumes
//!     → user SM.apply(log_index, cmd)
//!     → publish on service/apply_resp.ring
//!   → ShmemAdaptedStateMachine awaits the response
//!   → openraft client_write returns
//! ```
//!
//! Both halves are tokio tasks in the same runtime (no subprocess yet —
//! multi-process testing is a later milestone).

use std::io::{Read, Write};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_node::{BootstrapConfig, IpcMode, NodeBuilder, NodeConfig, RaftTuning, TlsConfig};
use uc_service::runtime::ServiceConfig;
use uc_service::{ServiceBuilder, SnapshotError, StateMachine};

#[derive(Default)]
struct Counter {
    value: u64,
    last_applied: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Cmd {
    Inc(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Resp {
    value: u64,
}

impl StateMachine for Counter {
    type Command = Cmd;
    type Response = Resp;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, log_index: u64, cmd: Cmd) -> Resp {
        let Cmd::Inc(delta) = cmd;
        self.value = self.value.wrapping_add(delta);
        self.last_applied = Some(log_index);
        Resp { value: self.value }
    }
    fn query(&self, _: ()) -> u64 {
        self.value
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
    fn build_snapshot(&self, _dst: &mut dyn Write) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
    fn install_snapshot(&mut self, _src: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
}

/// Poll for the existence of `path` for up to `timeout`.
async fn wait_for_path(path: &std::path::Path, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    while !path.exists() {
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for {}", path.display());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shmem_single_node_submit_apply() {
    let instance_tempdir = TempDir::new().unwrap();
    let node_data_tempdir = TempDir::new().unwrap();
    let service_data_tempdir = TempDir::new().unwrap();
    let instance_dir = instance_tempdir.path().to_owned();

    let app_id = "m3-shmem".to_string();

    // ── Node ────────────────────────────────────────────────────────────
    let node_cfg = NodeConfig {
        node_id: 1,
        data_dir: node_data_tempdir.path().to_owned(),
        raft_listen_addr: "127.0.0.1:0".parse().unwrap(),
        app_id: app_id.clone(),
        bootstrap: BootstrapConfig::SingleNode,
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        ipc_mode: IpcMode::Shmem {
            instance_dir: instance_dir.clone(),
        },
    };
    let node_task = tokio::spawn(async move {
        // Counter passed to the node is degenerate in shmem mode (used
        // only by the snapshot trait surface) — the real SM lives in the
        // service spawned below.
        NodeBuilder::new(node_cfg, Counter::default()).start().await
    });

    // The node's `NodeBuilder::start` creates cnc.dat under instance_dir,
    // then blocks in `wait_for_service_ready`. Poll until the file
    // exists, then spawn the service so it can attach.
    wait_for_path(&instance_dir.join("cnc.dat"), Duration::from_secs(5)).await;

    // ── Service ─────────────────────────────────────────────────────────
    let svc_cfg = ServiceConfig {
        instance_dir: instance_dir.clone(),
        app_id: app_id.clone(),
        data_dir: service_data_tempdir.path().to_owned(),
        ..ServiceConfig::default()
    };
    let svc_task = tokio::spawn(async move {
        ServiceBuilder::new(svc_cfg, Counter::default())
            .run()
            .await
    });

    let node = tokio::time::timeout(Duration::from_secs(15), node_task)
        .await
        .expect("node start timed out")
        .expect("node task panic")
        .expect("node start error");

    let service = tokio::time::timeout(Duration::from_secs(15), svc_task)
        .await
        .expect("service start timed out")
        .expect("service task panic")
        .expect("service start error");

    // ── Wait for leader ────────────────────────────────────────────────
    for _ in 0..50 {
        if node.current_leader().await == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(node.current_leader().await, Some(1), "leader did not converge");

    // ── Submit + verify the increment round-tripped through the rings ──
    let resp = node.submit(Cmd::Inc(5)).await.expect("submit Inc(5)");
    assert_eq!(resp, Resp { value: 5 });

    let resp = node.submit(Cmd::Inc(3)).await.expect("submit Inc(3)");
    assert_eq!(resp, Resp { value: 8 });

    // ── Shutdown ────────────────────────────────────────────────────────
    // Service first: stop its loops + drop the cnc mmap. Then the node,
    // whose `_instance` field still holds the cnc mmap — its shutdown
    // joins the heartbeat ticker before that drop.
    service.shutdown().await.expect("service shutdown");
    node.shutdown().await.expect("node shutdown");
}
