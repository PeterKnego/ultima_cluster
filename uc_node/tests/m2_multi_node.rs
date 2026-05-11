//! M2 multi-node integration tests.
//!
//! Each test brings up 3 nodes on different loopback ports. Each node has
//! its own tempdir, its own QUIC endpoint, and they discover each other
//! via BootstrapConfig::Peers. The min-id node bootstraps; others wait
//! to be added as learners.
//!
//! Key implementation detail: all three `NodeBuilder::start()` calls must
//! run concurrently (via `tokio::spawn` + `join_all`), because the
//! bootstrapper's `add_learner(blocking=true)` waits for each peer to be
//! reachable.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use uc_node::{
    BootstrapConfig, NodeBuilder, NodeConfig, NodeHandle, NodeId, PeerSeed, RaftTuning, TlsConfig,
};
use uc_service::{SnapshotError, StateMachine};

// -- Shared Counter state machine for multi-node tests --

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Cmd {
    Inc(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Resp {
    pub value: u64,
}

#[derive(Default)]
pub struct Counter {
    pub value: u64,
    pub last_applied: Option<u64>,
}

impl StateMachine for Counter {
    type Command = Cmd;
    type Response = Resp;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, idx: u64, c: Cmd) -> Resp {
        match c {
            Cmd::Inc(n) => self.value += n,
        }
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

// -- Harness --

pub struct TestNode {
    pub node_id: NodeId,
    pub handle: Option<NodeHandle<Counter>>,
    pub data_dir: Arc<TempDir>,
    pub addr: SocketAddr,
}

/// Pick three loopback addresses by binding ephemeral UDP sockets, capturing
/// the addresses, and releasing the sockets.
pub fn pick_three_addrs() -> Vec<SocketAddr> {
    (0..3)
        .map(|_| {
            let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let a = s.local_addr().unwrap();
            drop(s);
            a
        })
        .collect()
}

pub fn node_config(
    node_id: NodeId,
    data_dir: &TempDir,
    listen_addr: SocketAddr,
    peers: Vec<PeerSeed>,
) -> NodeConfig {
    NodeConfig {
        node_id,
        data_dir: data_dir.path().to_owned(),
        raft_listen_addr: listen_addr,
        app_id: "m2-test".into(),
        bootstrap: BootstrapConfig::Peers { peers },
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
    }
}

/// Brings up 3 nodes concurrently via spawn+join_all, returns them.
pub async fn spawn_3_node_cluster() -> Vec<TestNode> {
    let addrs = pick_three_addrs();
    let peer_seeds: Vec<PeerSeed> = (1..=3u64)
        .zip(addrs.iter())
        .map(|(id, a)| PeerSeed {
            node_id: id,
            raft_addr: *a,
        })
        .collect();

    // Prepare config + state machine for each node; create temp dirs.
    let mut prep: Vec<(NodeId, Arc<TempDir>, SocketAddr, NodeConfig)> = Vec::new();
    for (i, addr) in addrs.iter().enumerate() {
        let node_id = (i as u64) + 1;
        let dir = Arc::new(TempDir::new().unwrap());
        let cfg = node_config(node_id, &dir, *addr, peer_seeds.clone());
        prep.push((node_id, dir, *addr, cfg));
    }

    // Spawn each `start()` concurrently — bootstrapper waits for peers, so they
    // must all be coming up at the same time.
    let mut join_handles = Vec::new();
    for (node_id, dir, addr, cfg) in prep {
        let dir_for_task = dir.clone();
        let h = tokio::spawn(async move {
            let handle = NodeBuilder::new(cfg, Counter::default())
                .start()
                .await
                .unwrap_or_else(|e| panic!("node {node_id} start: {e:?}"));
            (node_id, handle, dir_for_task, addr)
        });
        join_handles.push(h);
    }

    let mut nodes = Vec::new();
    for h in join_handles {
        let (node_id, handle, dir, addr) = h.await.expect("node spawn task");
        nodes.push(TestNode {
            node_id,
            handle: Some(handle),
            data_dir: dir,
            addr,
        });
    }
    nodes
}

pub async fn wait_for_leader(nodes: &[TestNode], timeout: Duration) -> NodeId {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        for n in nodes {
            if let Some(h) = &n.handle
                && let Some(leader) = h.current_leader().await
            {
                return leader;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no leader within {timeout:?}");
}

pub async fn shutdown_all(mut nodes: Vec<TestNode>) {
    for n in nodes.iter_mut() {
        if let Some(handle) = n.handle.take() {
            let _ = handle.shutdown().await;
        }
    }
}

// -- Tests --

#[tokio::test]
async fn three_node_cluster_elects_leader() {
    let nodes = spawn_3_node_cluster().await;
    let leader = wait_for_leader(&nodes, Duration::from_secs(10)).await;
    assert!((1..=3).contains(&leader), "leader {leader} out of range");
    shutdown_all(nodes).await;
}
