//! M5 Task 5.2 — verify the idempotency contract of OutputHandler under
//! replay.
//!
//! Submit 3 commands → observe 3 invocations. Reset output_progress to 0.
//! Force a leader-replay sweep. Observe 3 MORE invocations (same
//! log_indexes). Net: user sees each log_index twice, confirming the
//! at-least-once / replay semantics.

use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
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
// Counter state machine (identical to m5_output_smoke)
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

// ---------------------------------------------------------------------------
// OutputLog recorder
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct OutputLog(Arc<Mutex<Vec<(u64, Cmd)>>>);

#[async_trait]
impl OutputHandler<Counter> for OutputLog {
    async fn on_committed(
        &self,
        log_index: u64,
        cmd: &Cmd,
        _state: &Counter,
    ) -> Result<(), OutputError> {
        self.0.lock().push((log_index, cmd.clone()));
        Ok(())
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
async fn m5_output_idempotent_replay() {
    let _ = tracing_subscriber::fmt::try_init();

    let inst = TempDir::new().unwrap();
    let node_data = TempDir::new().unwrap();
    let svc_data = TempDir::new().unwrap();
    let instance_dir = inst.path().to_owned();
    let app_id = "m5-idempotent-replay".to_string();

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
        log_durability: ultima_journal::Durability::Eventual,
    };
    let node_task =
        tokio::spawn(async move { NodeBuilder::new(cfg, Counter::default()).start().await });
    wait_for_path(&instance_dir.join("cnc.dat"), Duration::from_secs(5)).await;

    let log = OutputLog::default();
    let log_for_handler = log.clone();
    let svc_cfg = ServiceConfig {
        instance_dir: instance_dir.clone(),
        app_id: app_id.clone(),
        data_dir: svc_data.path().to_owned(),
        ..ServiceConfig::default()
    };
    let svc_task = tokio::spawn(async move {
        ServiceBuilder::new(svc_cfg, Counter::default())
            .output_handler(log_for_handler)
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

    // Submit 3 commands.
    for i in 1..=3u64 {
        let _: Resp = client.submit(&Cmd::Inc(i)).await.expect("submit");
    }

    // Wait until the recorder shows 3 entries.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while log.0.lock().len() < 3 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        log.0.lock().len(),
        3,
        "first batch should be 3 invocations; got {}",
        log.0.lock().len()
    );

    let original_log_indexes: Vec<u64> = log.0.lock().iter().map(|(i, _)| *i).collect();

    // Reset output_progress to 0 to simulate a partial-output crash where
    // none of the 3 commands were durably recorded as completed.
    node._test_reset_output_progress(0);

    // Force a leader-transition replay by emitting false → true on the leader
    // watch channel. We yield between the two sends so the output_replay_watcher
    // task (running on the same single-threaded executor) gets to see the
    // false state before we send true. Without the yield, both sends happen
    // before the watcher can execute, the channel coalesces to the final value
    // (true), and the watcher never sees the false→true edge.
    assert!(
        node._test_set_leader_state(false),
        "_test_set_leader_state returned false (embedded mode?)"
    );
    tokio::task::yield_now().await; // let the watcher see false
    node._test_set_leader_state(true);
    tokio::task::yield_now().await; // let the watcher see true and spawn replay

    // Wait until the recorder shows 6 entries total (3 originals + 3 replays).
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while log.0.lock().len() < 6 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let entries = log.0.lock().clone();
    assert_eq!(
        entries.len(),
        6,
        "expected 6 invocations after replay; got {}",
        entries.len()
    );

    // The replayed log_indexes must match the original ones exactly.
    let replayed: Vec<u64> = entries[3..].iter().map(|(i, _)| *i).collect();
    assert_eq!(
        replayed, original_log_indexes,
        "replay must hit the same log_indexes as original"
    );

    client.shutdown().await.expect("client shutdown");
    service.shutdown().await.expect("svc shutdown");
    node.shutdown().await.expect("node shutdown");
}
