//! M5 Task 5.6 — verify leader-transition output replay.
//!
//! Three-node cluster, each service registers its own `OutputLog`. Submit
//! 3 commands through the initial leader's client; verify the leader's log
//! shows 3 entries. Then call `transfer_leader` to gracefully hand off to
//! a different node; wait for the new leader to elect. The new leader's
//! `output_replay_watcher` fires on its `false → true` leader transition
//! and scans `(output_progress, last_applied]`. Since follower nodes never
//! advance `output_progress`, this range covers all 3 prior commits, so the
//! new leader's `OutputLog` should record 3 replayed entries.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_client::Client;
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, NodeHandle, NodeId,
    PeerSeed, RaftTuning, ServiceRingConfig, TlsConfig, Transport,
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn pick_three_addrs() -> Vec<SocketAddr> {
    (0..3)
        .map(|_| {
            let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let a = s.local_addr().unwrap();
            drop(s);
            a
        })
        .collect()
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

async fn wait_for_leader(handles: &[NodeHandle<Counter>], timeout: Duration) -> NodeId {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for h in handles {
            if let Some(l) = h.current_leader().await {
                return l;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no leader elected within {timeout:?}");
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn m5_output_leader_transition_replay() {
    let _ = tracing_subscriber::fmt::try_init();

    let addrs = pick_three_addrs();
    let peer_seeds: Vec<PeerSeed> = (1..=3u64)
        .zip(addrs.iter())
        .map(|(id, a)| PeerSeed {
            node_id: id,
            raft_addr: *a,
        })
        .collect();

    let app_id = "m5-transition".to_string();

    // Keep all tempdirs alive for the full test.
    let mut instance_dirs: Vec<Arc<TempDir>> = Vec::new();
    let mut _node_data_dirs: Vec<Arc<TempDir>> = Vec::new();
    let mut _svc_data_dirs: Vec<Arc<TempDir>> = Vec::new();

    // ── Spawn 3 NodeBuilder::start futures in parallel ──────────────────────
    let mut node_tasks = Vec::new();
    for (i, addr) in addrs.iter().enumerate() {
        let node_id = (i as u64) + 1;
        let instance_dir = Arc::new(TempDir::new().unwrap());
        let node_data_dir = Arc::new(TempDir::new().unwrap());
        instance_dirs.push(instance_dir.clone());
        _node_data_dirs.push(node_data_dir.clone());

        let cfg = NodeConfig {
            node_id,
            data_dir: node_data_dir.path().to_owned(),
            raft_listen_addr: *addr,
            app_id: app_id.clone(),
            bootstrap: BootstrapConfig::Peers {
                peers: peer_seeds.clone(),
            },
            raft: RaftTuning::default(),
            tls: TlsConfig::default(),
            transport: Transport::Quic,
            ipc_mode: IpcMode::Shmem {
                instance_dir: instance_dir.path().to_owned(),
            },
            client_rings: ClientRingConfig::default(),
            service_rings: ServiceRingConfig::default(),
            log_durability: ultima_journal::Durability::Eventual,
        };
        node_tasks.push(tokio::spawn(async move {
            NodeBuilder::new(cfg, Counter::default()).start().await
        }));
    }

    // ── For each node, wait for cnc.dat, then spawn its service with its own
    //   OutputLog ────────────────────────────────────────────────────────────
    let logs: Vec<OutputLog> = (0..3).map(|_| OutputLog::default()).collect();
    let mut svc_tasks = Vec::new();
    for (i, instance_dir) in instance_dirs.iter().enumerate() {
        wait_for_path(
            &instance_dir.path().join("cnc.dat"),
            Duration::from_secs(10),
        )
        .await;

        let svc_data_dir = Arc::new(TempDir::new().unwrap());
        _svc_data_dirs.push(svc_data_dir.clone());

        let svc_cfg = ServiceConfig {
            instance_dir: instance_dir.path().to_owned(),
            app_id: app_id.clone(),
            data_dir: svc_data_dir.path().to_owned(),
            ..ServiceConfig::default()
        };
        let log_for_handler = logs[i].clone();
        svc_tasks.push(tokio::spawn(async move {
            ServiceBuilder::new(svc_cfg, Counter::default())
                .output_handler(log_for_handler)
                .run()
                .await
        }));
    }

    // ── Collect node + service handles ─────────────────────────────────────
    let mut node_handles: Vec<NodeHandle<Counter>> = Vec::new();
    for (i, t) in node_tasks.into_iter().enumerate() {
        let h = tokio::time::timeout(Duration::from_secs(30), t)
            .await
            .unwrap_or_else(|_| panic!("node {} start timed out", i + 1))
            .expect("node task panic")
            .unwrap_or_else(|e| panic!("node {} start: {e:?}", i + 1));
        node_handles.push(h);
    }
    let mut svc_handles = Vec::new();
    for (i, t) in svc_tasks.into_iter().enumerate() {
        let s = tokio::time::timeout(Duration::from_secs(30), t)
            .await
            .unwrap_or_else(|_| panic!("svc {} start timed out", i + 1))
            .expect("svc task panic")
            .unwrap_or_else(|e| panic!("svc {} start: {e:?}", i + 1));
        svc_handles.push(s);
    }

    // ── Wait for leader convergence ─────────────────────────────────────────
    let leader_id = wait_for_leader(&node_handles, Duration::from_secs(15)).await;
    assert!(
        (1..=3).contains(&leader_id),
        "leader_id out of range: {leader_id}"
    );
    let leader_idx = (leader_id as usize) - 1;

    tracing::info!(leader_id, "initial leader elected");

    // ── Connect a client to the leader's instance dir ──────────────────────
    let client = Client::connect(instance_dirs[leader_idx].path(), &app_id)
        .await
        .expect("client connect");

    // ── Submit 3 commands ──────────────────────────────────────────────────
    for i in 1..=3u64 {
        let _: Resp = client.submit(&Cmd::Inc(i)).await.expect("submit");
    }

    // ── Wait for the leader's OutputLog to record 3 entries ────────────────
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while logs[leader_idx].0.lock().len() < 3 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        logs[leader_idx].0.lock().len(),
        3,
        "initial leader's OutputLog should record 3 entries; got {}",
        logs[leader_idx].0.lock().len()
    );

    tracing::info!(leader_id, "leader OutputLog confirmed 3 entries");

    // ── Pick a transfer target (different from the original leader) ─────────
    let target_id: NodeId = if leader_id == 1 { 2 } else { 1 };
    let _target_idx = (target_id as usize) - 1;

    tracing::info!(leader_id, target_id, "triggering transfer_leader");

    // ── Trigger graceful leader transfer ───────────────────────────────────
    node_handles[leader_idx]
        ._test_transfer_leader(target_id)
        .await
        .expect("transfer_leader");

    // ── Wait for a new leader that is different from the original ───────────
    // transfer_leader is a best-effort hint; allow up to 30 s for the new
    // term to be established. Any node (including the target) may end up
    // winning the election.
    let mut new_leader_id: NodeId = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        for h in &node_handles {
            if let Some(id) = h.current_leader().await
                && id != leader_id
            {
                new_leader_id = id;
                break;
            }
        }
        if new_leader_id != 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        (1..=3).contains(&new_leader_id),
        "new leader did not elect within 30 s (original leader_id={leader_id})"
    );
    assert_ne!(
        new_leader_id, leader_id,
        "new leader should differ from the original"
    );
    let new_leader_idx = (new_leader_id as usize) - 1;

    tracing::info!(new_leader_id, "new leader elected; waiting for replay");

    // ── Wait for the new leader's OutputLog to record 3 replayed entries ────
    //
    // Follower nodes never advance `output_progress`, so when the new leader's
    // `output_replay_watcher` fires on its `false → true` leader transition it
    // scans `(0, last_applied]` = 3 entries and re-publishes them through the
    // new leader's OutputHandler.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while logs[new_leader_idx].0.lock().len() < 3 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let new_log = logs[new_leader_idx].0.lock().clone();
    assert_eq!(
        new_log.len(),
        3,
        "new leader (node {new_leader_id}) OutputLog should record 3 replayed entries; got {}",
        new_log.len()
    );

    // The replayed entries must cover the same 3 App commands (all Inc variants).
    let all_inc = new_log.iter().all(|(_, cmd)| matches!(cmd, Cmd::Inc(_)));
    assert!(all_inc, "all replayed entries should be Cmd::Inc");

    tracing::info!(new_leader_id, "replay confirmed; shutting down");

    // ── Shutdown: client → services → nodes ────────────────────────────────
    client.shutdown().await.expect("client shutdown");
    for s in svc_handles {
        s.shutdown().await.expect("svc shutdown");
    }
    for n in node_handles {
        n.shutdown().await.expect("node shutdown");
    }
}
