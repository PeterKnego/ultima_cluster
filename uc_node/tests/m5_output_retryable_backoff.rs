//! M5 Task 5.3 — verify that OutputHandler retries on Retryable errors with
//! exponential backoff.
//!
//! Wire a `FlakyOutput` that returns `Retryable` for the first 3 calls and
//! `Ok` on the 4th. Submit one command and assert the handler was invoked
//! exactly 4 times (initial + 3 retries).

use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_client::Client;
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, RaftTuning,
    ServiceRingConfig, TlsConfig,
};
use uc_service::runtime::ServiceConfig;
use uc_service::{OutputError, OutputHandler, ServiceBuilder, SnapshotError, StateMachine};

// ---------------------------------------------------------------------------
// Counter state machine
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// FlakyOutput: fails with Retryable for first 3 calls, succeeds on the 4th.
// Uses Arc<AtomicU64> for the counter so the Clone can share state.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct FlakyOutput {
    tries: Arc<AtomicU64>,
}

#[async_trait]
impl OutputHandler<Counter> for FlakyOutput {
    async fn on_committed(
        &self,
        _log_index: u64,
        _cmd: &Cmd,
        _state: &Counter,
    ) -> Result<(), OutputError> {
        let n = self.tries.fetch_add(1, Ordering::Relaxed);
        if n < 3 {
            Err(OutputError::Retryable(format!(
                "transient failure attempt {n}"
            )))
        } else {
            Ok(())
        }
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

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn m5_output_retryable_backoff() {
    let _ = tracing_subscriber::fmt::try_init();

    let inst = TempDir::new().unwrap();
    let node_data = TempDir::new().unwrap();
    let svc_data = TempDir::new().unwrap();
    let instance_dir = inst.path().to_owned();
    let app_id = "m5-retryable-backoff".to_string();

    let flaky = FlakyOutput {
        tries: Arc::new(AtomicU64::new(0)),
    };
    let tries = flaky.tries.clone();

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
    let svc_task = tokio::spawn(async move {
        ServiceBuilder::new(svc_cfg, Counter::default())
            .output_handler(flaky)
            .run()
            .await
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

    // Wait for leader election.
    for _ in 0..50 {
        if node.current_leader().await == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(node.current_leader().await, Some(1));

    let client = Client::connect(&instance_dir, &app_id)
        .await
        .expect("connect");

    // Submit one command — the flaky handler will retry 3 times then succeed.
    let _: Resp = client.submit(&Cmd::Inc(1)).await.expect("submit");

    // Wait for 4 total invocations (0, 1, 2 → Retryable; 3 → Ok).
    // Backoff: initial 10ms, doubles each try → 10ms + 20ms + 40ms ≈ 70ms
    // plus apply/IPC overhead. Give 15 s headroom.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while tries.load(Ordering::Relaxed) < 4 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        tries.load(Ordering::Relaxed),
        4,
        "expected 4 invocations (initial + 3 retries)"
    );

    client.shutdown().await.expect("client shutdown");
    service.shutdown().await.expect("svc shutdown");
    node.shutdown().await.expect("node shutdown");
}
