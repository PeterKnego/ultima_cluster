//! Regression test: a node whose service has crashed must still shut down
//! cleanly, even with a committed entry wedged mid-`apply` against the dead
//! service.
//!
//! Root cause this guards (see task04 / state_machine_shmem): in shmem mode
//! `RaftStateMachine::apply` publishes to the service apply ring and then blocks
//! in `await_apply_resp` until the service responds. If the service is dead the
//! response never comes — by design the node waits indefinitely so it can resume
//! when the service reconnects. But `node.shutdown()` begins with
//! `raft.shutdown().await`, which waits for openraft's state-machine worker to
//! finish; a worker wedged in `apply` made shutdown deadlock forever. The fix
//! lets shutdown signal the apply path to abort the wait.

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
    type SnapshotHandle = Vec<u8>;

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
    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        Ok((Vec::new(), self.last_applied.unwrap_or(0)))
    }
    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }
    fn install_snapshot(&mut self, _src: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
}

async fn wait_for_path(path: &std::path::Path, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    while !path.exists() {
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for {}", path.display());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn node_shutdown_with_crashed_service_does_not_hang() {
    let instance_tempdir = TempDir::new().unwrap();
    let node_data_tempdir = TempDir::new().unwrap();
    let service_data_tempdir = TempDir::new().unwrap();
    let instance_dir = instance_tempdir.path().to_owned();
    let app_id = "m3-shutdown-dead-service".to_string();

    // ── Node + service (single-node shmem) ──────────────────────────────
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
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
        log_durability: ultima_journal::Durability::Eventual,
    };
    let node_task =
        tokio::spawn(async move { NodeBuilder::new(node_cfg, Counter::default()).start().await });

    wait_for_path(&instance_dir.join("cnc.dat"), Duration::from_secs(5)).await;

    let svc_cfg = ServiceConfig {
        instance_dir: instance_dir.clone(),
        app_id: app_id.clone(),
        data_dir: service_data_tempdir.path().to_owned(),
        ..ServiceConfig::default()
    };
    let svc_task =
        tokio::spawn(async move { ServiceBuilder::new(svc_cfg, Counter::default()).run().await });

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

    for _ in 0..50 {
        if node.current_leader().await == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        node.current_leader().await,
        Some(1),
        "leader did not converge"
    );

    // ── Baseline: a write applies cleanly while the service is alive ─────
    let client = Client::connect(&instance_dir, &app_id)
        .await
        .expect("client connect");
    let r: Resp = client.submit(&Cmd::Inc(1)).await.expect("baseline submit");
    assert_eq!(r.value, 1);

    // ── Crash the service ───────────────────────────────────────────────
    service.shutdown().await.expect("crash service");

    // ── Fire a write that commits and wedges in apply against the dead
    //    service. The submit never returns (no apply response); run it in a
    //    detached task so the test can proceed to shutdown. ──────────────
    let submit_task = tokio::spawn(async move {
        let _: Result<Resp, _> = client.submit(&Cmd::Inc(2)).await;
    });
    // Give the entry time to commit and reach the (now-blocking) apply.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // ── The actual assertion: shutdown must not deadlock on the wedged
    //    apply. Pre-fix this hangs forever in `raft.shutdown()`. ──────────
    tokio::time::timeout(Duration::from_secs(10), node.shutdown())
        .await
        .expect("node.shutdown() hung: apply wedged on dead service (deadlock)")
        .expect("node shutdown returned error");

    submit_task.abort();
}
