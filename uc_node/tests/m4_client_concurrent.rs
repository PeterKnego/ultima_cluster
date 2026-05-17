//! 1 node + 1 service, 4 concurrent uc_clients (M4 Task 5.3).
//!
//! Four clients each submit 50 `Inc` commands concurrently via `tokio::join!`.
//! All commands are additive, so the final state is deterministic regardless
//! of interleaving order: 50 × (1 + 2 + 3 + 4) = 500.

use std::io::{Read, Write};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_client::Client;
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
        let Cmd::Inc(d) = cmd;
        self.value = self.value.wrapping_add(d);
        self.last_applied = Some(log_index);
        Resp { value: self.value }
    }
    fn query(&self, _: ()) -> u64 {
        self.value
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
    fn build_snapshot(&self, _: &mut dyn Write) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
    fn install_snapshot(&mut self, _: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
}

async fn wait_for_path(p: &std::path::Path, t: Duration) {
    let deadline = std::time::Instant::now() + t;
    while !p.exists() {
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for {}", p.display());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn submit_many(c: &Client, delta: u64, count: usize) {
    for _ in 0..count {
        let _: Resp = c.submit(&Cmd::Inc(delta)).await.unwrap();
    }
}

#[tokio::test]
async fn m4_client_concurrent() {
    let _ = tracing_subscriber::fmt::try_init();

    let inst = TempDir::new().unwrap();
    let node_data = TempDir::new().unwrap();
    let svc_data = TempDir::new().unwrap();
    let instance_dir = inst.path().to_owned();
    let app_id = "m4-concurrent".to_string();

    let cfg = NodeConfig {
        node_id: 1,
        data_dir: node_data.path().to_owned(),
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
        NodeBuilder::new(cfg, Counter::default()).start().await
    });
    wait_for_path(&instance_dir.join("cnc.dat"), Duration::from_secs(5)).await;

    let svc_cfg = ServiceConfig {
        instance_dir: instance_dir.clone(),
        app_id: app_id.clone(),
        data_dir: svc_data.path().to_owned(),
        ..ServiceConfig::default()
    };
    let svc_task = tokio::spawn(async move {
        ServiceBuilder::new(svc_cfg, Counter::default()).run().await
    });

    let node = tokio::time::timeout(Duration::from_secs(15), node_task)
        .await
        .expect("node timeout")
        .expect("node panic")
        .expect("node start");
    let service = tokio::time::timeout(Duration::from_secs(15), svc_task)
        .await
        .expect("svc timeout")
        .expect("svc panic")
        .expect("svc start");

    // Wait for single-node leader election.
    for _ in 0..50 {
        if node.current_leader().await == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(node.current_leader().await, Some(1));

    // ── Four concurrent clients ─────────────────────────────────────────────
    let c1 = Client::connect(&instance_dir, &app_id).await.unwrap();
    let c2 = Client::connect(&instance_dir, &app_id).await.unwrap();
    let c3 = Client::connect(&instance_dir, &app_id).await.unwrap();
    let c4 = Client::connect(&instance_dir, &app_id).await.unwrap();

    // 50 increments each: deltas 1, 2, 3, 4.
    // Total: 50 × (1 + 2 + 3 + 4) = 500.
    tokio::join!(
        submit_many(&c1, 1, 50),
        submit_many(&c2, 2, 50),
        submit_many(&c3, 3, 50),
        submit_many(&c4, 4, 50),
    );

    let v: u64 = c1.query_snapshot(&()).await.unwrap();
    assert_eq!(v, 50 * (1 + 2 + 3 + 4));

    for c in [c1, c2, c3, c4] {
        c.shutdown().await.unwrap();
    }
    service.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}
