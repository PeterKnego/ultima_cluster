//! 1 node + 1 service — dropped Client's session unlinked by GC (M4 Task 5.6).
//!
//! Connects one client, drops it without calling `shutdown()`, and waits for
//! the node-side `session_gc` task to unlink the session file. GC constants:
//!   - `STALE_AFTER` = 5 s (file is stale when heartbeat_seq stops advancing)
//!   - `GC_TICK`     = 2 s (GC runs every 2 s)
//!
//! When the `Client` is dropped, `Drop` stops the heartbeat ticker (sets the
//! stop flag) but does NOT remove the session file. The GC should unlink it
//! within `STALE_AFTER + GC_TICK` ≈ 7 s. We allow 10 s for slack.

use std::io::{Read, Write};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_client::Client;
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, RaftTuning,
    ServiceRingConfig, TlsConfig,
};
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

#[tokio::test]
async fn m4_client_session_gc() {
    let _ = tracing_subscriber::fmt::try_init();

    let inst = TempDir::new().unwrap();
    let node_data = TempDir::new().unwrap();
    let svc_data = TempDir::new().unwrap();
    let instance_dir = inst.path().to_owned();
    let app_id = "m4-session-gc".to_string();

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
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
    };
    let node_task =
        tokio::spawn(async move { NodeBuilder::new(cfg, Counter::default()).start().await });
    wait_for_path(&instance_dir.join("cnc.dat"), Duration::from_secs(5)).await;

    let svc_cfg = ServiceConfig {
        instance_dir: instance_dir.clone(),
        app_id: app_id.clone(),
        data_dir: svc_data.path().to_owned(),
        ..ServiceConfig::default()
    };
    let svc_task =
        tokio::spawn(async move { ServiceBuilder::new(svc_cfg, Counter::default()).run().await });

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

    // ── Connect and immediately drop ────────────────────────────────────────
    let client = Client::connect(&instance_dir, &app_id).await.unwrap();
    let cid = client.client_id();
    let session_path = instance_dir
        .join("clients")
        .join("sessions.dir")
        .join(format!("{cid}.session"));

    assert!(
        session_path.exists(),
        "session file should exist after connect"
    );

    // Drop without calling shutdown(). Client::Drop stops the heartbeat ticker
    // (so heartbeat_seq stops advancing) but does NOT remove the session file.
    // The node-side session_gc should unlink it once the heartbeat goes stale.
    drop(client);

    // Poll until the file disappears, with a generous deadline (STALE_AFTER=5s
    // + GC_TICK=2s + extra slack = 10s total).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while session_path.exists() {
        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(
        !session_path.exists(),
        "session_gc should have unlinked the file at {}",
        session_path.display()
    );

    service.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}
