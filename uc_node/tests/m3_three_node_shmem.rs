//! 3-node shmem cluster test (M3 Task 19).
//!
//! Brings up three nodes, each in `IpcMode::Shmem` paired with its own
//! `uc_service::Service`, all running in one tokio runtime. Mirrors the
//! M2 `three_node_replication` shape (submit 5 increments via the leader,
//! verify every node converges) but with the apply + query path crossing
//! the shmem rings instead of running in-process.
//!
//! Bootstrap dance: all three `NodeBuilder::start()` futures are spawned
//! concurrently because the min-id node's `add_learner(blocking=true)`
//! needs peers 2 and 3 to have their QUIC servers up. Each node's
//! `start()` blocks in `wait_for_service_ready` until *its* service comes
//! online, so we poll for each `cnc.dat` and then spawn the matching
//! `ServiceBuilder::run` in lockstep.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use uc_node::{
    BootstrapConfig, IpcMode, NodeBuilder, NodeConfig, NodeHandle, NodeId, PeerSeed, RaftTuning,
    TlsConfig,
};
use uc_service::runtime::ServiceConfig;
use uc_service::{ServiceBuilder, SnapshotError, StateMachine};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum Cmd {
    Inc(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Resp {
    value: u64,
}

#[derive(Default)]
struct Counter {
    value: u64,
    last_applied: Option<u64>,
}

impl StateMachine for Counter {
    type Command = Cmd;
    type Response = Resp;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, idx: u64, c: Cmd) -> Resp {
        let Cmd::Inc(n) = c;
        self.value += n;
        self.last_applied = Some(idx);
        Resp { value: self.value }
    }
    fn query(&self, _: ()) -> u64 {
        self.value
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
    fn build_snapshot(&self, dst: &mut dyn Write) -> Result<u64, SnapshotError> {
        let bytes = bincode::serde::encode_to_vec(
            (self.value, self.last_applied),
            bincode::config::standard(),
        )
        .map_err(|e| SnapshotError::Codec(e.to_string()))?;
        dst.write_all(&bytes)?;
        Ok(self.last_applied.unwrap_or(0))
    }
    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<u64, SnapshotError> {
        let mut buf = Vec::new();
        src.read_to_end(&mut buf)?;
        let ((v, la), _) = bincode::serde::decode_from_slice::<(u64, Option<u64>), _>(
            &buf,
            bincode::config::standard(),
        )
        .map_err(|e| SnapshotError::Codec(e.to_string()))?;
        self.value = v;
        self.last_applied = la;
        Ok(la.unwrap_or(0))
    }
}

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
    panic!("no leader within {timeout:?}");
}

#[tokio::test]
async fn three_node_shmem_cluster() {
    let addrs = pick_three_addrs();
    let peer_seeds: Vec<PeerSeed> = (1..=3u64)
        .zip(addrs.iter())
        .map(|(id, a)| PeerSeed {
            node_id: id,
            raft_addr: *a,
        })
        .collect();

    let app_id = "m3-three-node".to_string();

    // Keep all tempdirs alive for the full test.
    let mut instance_dirs: Vec<Arc<TempDir>> = Vec::new();
    let mut _node_data_dirs: Vec<Arc<TempDir>> = Vec::new();
    let mut _svc_data_dirs: Vec<Arc<TempDir>> = Vec::new();
    let mut node_tasks = Vec::new();

    // ── Spawn 3 NodeBuilder::start futures in parallel ──────────────────
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
            ipc_mode: IpcMode::Shmem {
                instance_dir: instance_dir.path().to_owned(),
            },
        };
        node_tasks.push(tokio::spawn(async move {
            NodeBuilder::new(cfg, Counter::default()).start().await
        }));
    }

    // ── For each node, wait for cnc.dat, then spawn its service ─────────
    let mut svc_tasks = Vec::new();
    for instance_dir in instance_dirs.iter() {
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
        svc_tasks.push(tokio::spawn(async move {
            ServiceBuilder::new(svc_cfg, Counter::default()).run().await
        }));
    }

    // ── Collect node + service handles ──────────────────────────────────
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

    // ── Wait for a leader, submit 5 increments through it ───────────────
    let leader_id = wait_for_leader(&node_handles, Duration::from_secs(15)).await;
    assert!((1..=3).contains(&leader_id));
    let leader = node_handles
        .iter()
        .find(|h| h.node_id() == leader_id)
        .unwrap();

    // Cumulative sums: 1, 3, 6, 10, 15.
    for i in 1..=5u64 {
        let resp = leader.submit(Cmd::Inc(i)).await.expect("submit");
        let expected: u64 = (1..=i).sum();
        assert_eq!(resp.value, expected, "leader submit {i}");
    }

    // Give followers' apply pipelines (raft → apply.ring → service) time
    // to catch up. Poll each node's service-side value via submit_query
    // instead of sleeping a fixed amount.
    for h in &node_handles {
        let mut caught_up = false;
        for _ in 0..50 {
            let v = h.submit_query(()).await.expect("submit_query");
            if v == 15 {
                caught_up = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(caught_up, "node {} did not converge to 15", h.node_id());
    }

    // ── Shutdown: services first, then nodes (the node's `_instance`
    // field still holds the cnc mmap; node.shutdown joins the heartbeat
    // ticker before dropping that mmap).
    for s in svc_handles.into_iter() {
        s.shutdown().await.expect("svc shutdown");
    }
    for n in node_handles.into_iter() {
        n.shutdown().await.expect("node shutdown");
    }
}
