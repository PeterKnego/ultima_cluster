//! M5 Task 5.4 — verify that a Permanent OutputHandler error advances
//! `output_progress` past the failed log_index (i.e., the node does not
//! get stuck retrying a permanently-failed entry forever).
//!
//! Wire `PermFor2`: fails with `Permanent` for whichever log_index it sees
//! second (the first user-command index). All other log_indexes succeed.
//! Submit 3 commands and assert `output_progress` eventually reaches the
//! max log_index seen by the handler.

use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
// PermForSecond: returns Permanent for the second distinct log_index seen.
// Records all seen log_indexes so the test can assert on the max.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct PermForSecond {
    seen: Arc<Mutex<Vec<u64>>>,
    // The log_index that will get the Permanent response (set on first call).
    perm_index: Arc<AtomicU64>,
}

impl PermForSecond {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
            perm_index: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait]
impl OutputHandler<Counter> for PermForSecond {
    async fn on_committed(
        &self,
        log_index: u64,
        _cmd: &Cmd,
        _state: &Counter,
    ) -> Result<(), OutputError> {
        let mut seen = self.seen.lock();
        seen.push(log_index);
        let call_number = seen.len();
        drop(seen);

        // Fail the second user-command invocation with Permanent.
        if call_number == 2 {
            self.perm_index.store(log_index, Ordering::Relaxed);
            return Err(OutputError::Permanent(format!(
                "intentional permanent failure at log_index {log_index}"
            )));
        }
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
async fn m5_output_permanent_advances_marker() {
    let _ = tracing_subscriber::fmt::try_init();

    let inst = TempDir::new().unwrap();
    let node_data = TempDir::new().unwrap();
    let svc_data = TempDir::new().unwrap();
    let instance_dir = inst.path().to_owned();
    let app_id = "m5-perm-advances-marker".to_string();

    let handler = PermForSecond::new();
    let seen = handler.seen.clone();

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
            .output_handler(handler)
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

    // Wait until the handler has processed all 3 user commands (at least 3
    // entries in `seen`). This gives the output_dispatcher time to complete
    // processing including advancing past the Permanent failure.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen.lock().len() < 3 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        seen.lock().len() >= 3,
        "handler should have seen at least 3 invocations; got {}",
        seen.lock().len()
    );

    // Find the maximum log_index the handler was invoked for.
    let max_seen = *seen.lock().iter().max().expect("non-empty");

    // Poll output_progress until it reaches max_seen (or timeout).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut got_progress = 0u64;
    while std::time::Instant::now() < deadline {
        got_progress = node._test_output_progress();
        if got_progress >= max_seen {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        got_progress >= max_seen,
        "output_progress should advance past Permanent failure to {max_seen}; got {got_progress}"
    );

    // Also verify that output_progress is not stuck at 0: even though one
    // entry returned Permanent, the marker must have advanced.
    assert!(
        got_progress > 0,
        "output_progress should be > 0; got {got_progress}"
    );

    client.shutdown().await.expect("client shutdown");
    service.shutdown().await.expect("svc shutdown");
    node.shutdown().await.expect("node shutdown");
}
