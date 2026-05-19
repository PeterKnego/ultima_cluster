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
    BootstrapConfig, ClientRingConfig, NodeBuilder, NodeConfig, NodeHandle, NodeId, PeerSeed,
    RaftTuning, ServiceRingConfig, TlsConfig,
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
        ipc_mode: uc_node::IpcMode::default(),
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
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

async fn wait_for_leader_among(handles: &[&NodeHandle<Counter>], timeout: Duration) -> NodeId {
    let surviving_ids: Vec<NodeId> = handles.iter().map(|h| h.node_id()).collect();
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        for h in handles {
            if let Some(l) = h.current_leader().await
                && surviving_ids.contains(&l)
            {
                return l;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no leader among surviving nodes within {timeout:?}");
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

#[tokio::test]
async fn three_node_replication() {
    let nodes = spawn_3_node_cluster().await;
    let leader_id = wait_for_leader(&nodes, Duration::from_secs(10)).await;
    let leader = nodes.iter().find(|n| n.node_id == leader_id).unwrap();
    let leader_handle = leader.handle.as_ref().unwrap();

    // Submit 5 increments via the leader.
    // Cumulative responses: 1, 3, 6, 10, 15.
    for i in 1..=5u64 {
        let resp = leader_handle.submit(Cmd::Inc(i)).await.expect("submit");
        let expected: u64 = (1..=i).sum();
        assert_eq!(
            resp.value, expected,
            "leader submit {i}: expected sum {expected}"
        );
    }

    // Give followers time to apply.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Every node's query_snapshot should return 15.
    for n in &nodes {
        if let Some(h) = &n.handle {
            let v = h.query_snapshot(|c: &Counter| c.value).await;
            assert_eq!(v, 15, "node {} value", n.node_id);
        }
    }
    shutdown_all(nodes).await;
}

fn tight_raft_tuning() -> RaftTuning {
    RaftTuning {
        heartbeat_interval_ms: 100,
        election_timeout_min_ms: 500,
        election_timeout_max_ms: 1000,
        max_in_snapshot_log_to_keep: 50,
        snapshot_policy_logs_since_last: 50, // trigger snapshot every 50 applied entries
    }
}

/// Brings up 2 nodes with tight snapshot policy for testing snapshot install.
async fn spawn_2_node_cluster_tight_snapshot() -> Vec<TestNode> {
    let addrs: Vec<SocketAddr> = (0..2)
        .map(|_| {
            let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let a = s.local_addr().unwrap();
            drop(s);
            a
        })
        .collect();

    let peer_seeds: Vec<PeerSeed> = (1..=2u64)
        .zip(addrs.iter())
        .map(|(id, a)| PeerSeed {
            node_id: id,
            raft_addr: *a,
        })
        .collect();

    let mut prep: Vec<(NodeId, Arc<TempDir>, SocketAddr, NodeConfig)> = Vec::new();
    for (i, addr) in addrs.iter().enumerate() {
        let node_id = (i as u64) + 1;
        let dir = Arc::new(TempDir::new().unwrap());
        let cfg = NodeConfig {
            node_id,
            data_dir: dir.path().to_owned(),
            raft_listen_addr: *addr,
            app_id: "m2-test-snap".into(),
            bootstrap: BootstrapConfig::Peers {
                peers: peer_seeds.clone(),
            },
            raft: tight_raft_tuning(),
            tls: TlsConfig::default(),
            ipc_mode: uc_node::IpcMode::default(),
            client_rings: ClientRingConfig::default(),
            service_rings: ServiceRingConfig::default(),
        };
        prep.push((node_id, dir, *addr, cfg));
    }

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
        let (node_id, handle, dir, addr) = h.await.expect("spawn task");
        nodes.push(TestNode {
            node_id,
            handle: Some(handle),
            data_dir: dir,
            addr,
        });
    }
    nodes
}

#[tokio::test]
async fn leader_failover() {
    let mut nodes = spawn_3_node_cluster().await;
    let initial_leader_id = wait_for_leader(&nodes, Duration::from_secs(10)).await;

    // Submit one command before failover.
    {
        let leader = nodes
            .iter()
            .find(|n| n.node_id == initial_leader_id)
            .unwrap();
        leader
            .handle
            .as_ref()
            .unwrap()
            .submit(Cmd::Inc(100))
            .await
            .expect("submit pre-failover");
    }

    // Kill the leader.
    {
        let leader_idx = nodes
            .iter()
            .position(|n| n.node_id == initial_leader_id)
            .unwrap();
        let leader = nodes[leader_idx].handle.take().unwrap();
        leader.shutdown().await.expect("shutdown leader");
    }

    // Wait for a new leader among surviving nodes.
    let surviving: Vec<&NodeHandle<Counter>> =
        nodes.iter().filter_map(|n| n.handle.as_ref()).collect();
    let new_leader_id = wait_for_leader_among(&surviving, Duration::from_secs(15)).await;
    assert_ne!(new_leader_id, initial_leader_id, "must elect a new leader");

    // Submit through the new leader.
    let new_leader = nodes.iter().find(|n| n.node_id == new_leader_id).unwrap();
    let resp = new_leader
        .handle
        .as_ref()
        .unwrap()
        .submit(Cmd::Inc(50))
        .await
        .expect("submit post-failover");
    assert_eq!(resp.value, 150);

    shutdown_all(nodes).await;
}

#[tokio::test]
async fn snapshot_install_on_new_follower() {
    let nodes = spawn_2_node_cluster_tight_snapshot().await;
    let leader_id = wait_for_leader(&nodes, Duration::from_secs(10)).await;
    let leader = nodes.iter().find(|n| n.node_id == leader_id).unwrap();
    let leader_handle = leader.handle.as_ref().unwrap();

    // Submit lots of commands to force at least one snapshot.
    let total: u64 = 200;
    for _ in 0..total {
        leader_handle.submit(Cmd::Inc(1)).await.expect("submit");
    }

    // Wait for the snapshot to land.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Add a 3rd node as a learner.
    let new_addr = {
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let a = s.local_addr().unwrap();
        drop(s);
        a
    };
    let new_dir = Arc::new(TempDir::new().unwrap());
    let new_cfg = NodeConfig {
        node_id: 3,
        data_dir: new_dir.path().to_owned(),
        raft_listen_addr: new_addr,
        app_id: "m2-test-snap".into(),
        bootstrap: BootstrapConfig::Resume, // wait to be added by the leader
        raft: tight_raft_tuning(),
        tls: TlsConfig::default(),
        ipc_mode: uc_node::IpcMode::default(),
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
    };
    let new_handle = NodeBuilder::new(new_cfg, Counter::default())
        .start()
        .await
        .expect("new node start");

    // Leader adds it as a learner.
    leader_handle
        .add_learner(3, new_addr)
        .await
        .expect("add_learner");

    // Wait for the new node to catch up.
    let mut caught_up = false;
    for _ in 0..50 {
        let v = new_handle.query_snapshot(|c: &Counter| c.value).await;
        if v == total {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(caught_up, "new node never caught up");

    new_handle.shutdown().await.expect("shutdown new node");
    shutdown_all(nodes).await;
}

#[tokio::test]
async fn membership_change_remove_node() {
    let nodes = spawn_3_node_cluster().await;
    let leader_id = wait_for_leader(&nodes, Duration::from_secs(10)).await;
    let leader = nodes.iter().find(|n| n.node_id == leader_id).unwrap();
    let leader_handle = leader.handle.as_ref().unwrap();

    // Submit one command with 3 voters.
    let r1 = leader_handle
        .submit(Cmd::Inc(10))
        .await
        .expect("submit pre-removal");
    assert_eq!(r1.value, 10);

    // Pick a non-leader node to remove.
    let victim = (1..=3u64).find(|i| *i != leader_id).unwrap();
    leader_handle
        .remove_node(victim)
        .await
        .expect("remove_node");

    // Submit another command with the reduced membership (2 voters).
    let r2 = leader_handle
        .submit(Cmd::Inc(5))
        .await
        .expect("submit post-removal");
    assert_eq!(r2.value, 15);

    shutdown_all(nodes).await;
}
