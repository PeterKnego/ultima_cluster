//! Ring wrap-path integration test (M4 Task 5.4).
//!
//! Creates tiny 32 KiB client rings and drives 2 concurrent clients each
//! submitting 500 `Inc` commands. At ~50–100 bytes per framed message the
//! submit ring wraps ~6+ times, exercising the Phase 1 wrap-fix end-to-end.
//!
//! The increments are deterministic:
//!
//! - client 1: Inc(0), Inc(1), …, Inc(499)     → sum = 0+1+…+499 = 124_750
//! - client 2: Inc(10_000), Inc(10_001), …      → sum = 10_000+…+10_499 = 5_124_750
//!
//! Total expected = 124_750 + 5_124_750 = 5_249_500
//!
//! Final value via `query_snapshot` must equal that sum regardless of
//! interleaving order (all increments are additive and commutative).

use std::io::{Read, Write};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_client::Client;
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, RaftTuning,
    ServiceRingConfig, TlsConfig, Transport,
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
    type SnapshotHandle = Vec<u8>;

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
    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        Ok((Vec::new(), self.last_applied.unwrap_or(0)))
    }
    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
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

/// Submit `count` Inc commands starting at `base` (base, base+1, …, base+count-1).
async fn submit_many(c: &Client, base: u64, count: u64) {
    for i in 0..count {
        let _: Resp = c.submit(&Cmd::Inc(base + i)).await.expect("submit");
    }
}

#[tokio::test]
async fn m4_client_wrap() {
    let _ = tracing_subscriber::fmt::try_init();

    let inst = TempDir::new().unwrap();
    let node_data = TempDir::new().unwrap();
    let svc_data = TempDir::new().unwrap();
    let instance_dir = inst.path().to_owned();
    let app_id = "m4-wrap".to_string();

    // Use 32 KiB rings to force multiple wraps during the 1 000-submit run.
    // max_msg is capped at 4 KiB (well above a single framed Inc command).
    let cfg = NodeConfig {
        node_id: 1,
        data_dir: node_data.path().to_owned(),
        raft_listen_addr: "127.0.0.1:0".parse().unwrap(),
        app_id: app_id.clone(),
        bootstrap: BootstrapConfig::SingleNode,
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        transport: Transport::Quic,
        ipc_mode: IpcMode::Shmem {
            instance_dir: instance_dir.clone(),
        },
        client_rings: ClientRingConfig {
            cap_bytes: 32 * 1024,
            max_msg: 4 * 1024,
        },
        service_rings: ServiceRingConfig::default(),
        log_durability: ultima_journal::Durability::Eventual,
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

    // ── Two concurrent clients, 500 submits each ───────────────────────────
    let c1 = Client::connect(&instance_dir, &app_id).await.expect("c1");
    let c2 = Client::connect(&instance_dir, &app_id).await.expect("c2");

    // 2 clients × 500 submits → ~1 000 records on a 32 KiB ring forces ~6+ wraps.
    tokio::join!(submit_many(&c1, 0, 500), submit_many(&c2, 10_000, 500));

    // Deterministic sum regardless of interleaving:
    //   c1: 0+1+…+499               = 124_750
    //   c2: 10_000+10_001+…+10_499  = 5_124_750
    let expected: u64 = (0u64..500).sum::<u64>() + (0u64..500).map(|i| 10_000 + i).sum::<u64>();
    let v: u64 = c1.query_snapshot(&()).await.expect("query");
    assert_eq!(v, expected, "final state mismatch (wrap-fix check failed)");

    c1.shutdown().await.unwrap();
    c2.shutdown().await.unwrap();
    service.shutdown().await.unwrap();
    node.shutdown().await.unwrap();
}
