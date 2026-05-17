//! 3-node shmem cluster with one uc_client per node (M4 Task 5.2).
//!
//! Brings up three nodes + three services (same boot dance as
//! `m3_three_node_shmem`), then attaches one `Client` to each instance
//! directory. Verifies:
//!   - Leader's client can submit commands and accumulate state.
//!   - Follower clients see the same value via `query_snapshot` after
//!     the apply pipeline has propagated.
//!   - Follower clients receive `ClientError::NotLeader { hint: Some(leader_id) }`
//!     when they try to submit.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_client::{Client, ClientError};
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, NodeHandle, NodeId,
    PeerSeed, RaftTuning, TlsConfig,
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
async fn m4_client_three_node() {
    let _ = tracing_subscriber::fmt::try_init();

    let addrs = pick_three_addrs();
    let peer_seeds: Vec<PeerSeed> = (1..=3u64)
        .zip(addrs.iter())
        .map(|(id, a)| PeerSeed {
            node_id: id,
            raft_addr: *a,
        })
        .collect();

    let app_id = "m4-three-node".to_string();

    // Keep all tempdirs alive for the full test.
    let mut instance_dirs: Vec<Arc<TempDir>> = Vec::new();
    let mut _node_data_dirs: Vec<Arc<TempDir>> = Vec::new();
    let mut _svc_data_dirs: Vec<Arc<TempDir>> = Vec::new();
    let mut node_tasks = Vec::new();

    // ── Spawn 3 NodeBuilder::start futures in parallel ──────────────────────
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
            client_rings: ClientRingConfig::default(),
        };
        node_tasks.push(tokio::spawn(async move {
            NodeBuilder::new(cfg, Counter::default()).start().await
        }));
    }

    // ── For each node, wait for cnc.dat, then spawn its service ────────────
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
    assert!((1..=3).contains(&leader_id));

    // node_ids are 1-based; instance_dirs is in the same order (index 0 = node 1).
    let leader_idx = (leader_id as usize) - 1;
    assert!(leader_idx < 3);

    // ── Connect one client per instance_dir ────────────────────────────────
    let mut clients: Vec<Client> = Vec::with_capacity(3);
    for d in &instance_dirs {
        clients.push(
            Client::connect(d.path(), &app_id)
                .await
                .expect("client connect"),
        );
    }

    // ── Leader submits two increments ──────────────────────────────────────
    let r: Resp = clients[leader_idx]
        .submit(&Cmd::Inc(1))
        .await
        .expect("inc 1");
    assert_eq!(r.value, 1);
    let r: Resp = clients[leader_idx]
        .submit(&Cmd::Inc(4))
        .await
        .expect("inc 4");
    assert_eq!(r.value, 5);

    // ── Followers must converge to 5 ───────────────────────────────────────
    for (i, c) in clients.iter().enumerate() {
        if i == leader_idx {
            continue;
        }
        let mut got = 0u64;
        for _ in 0..50 {
            got = c.query_snapshot(&()).await.expect("query_snapshot");
            if got == 5 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(got, 5, "follower {i} did not converge to 5");
    }

    // ── Followers return NotLeader with the correct hint ───────────────────
    for (i, c) in clients.iter().enumerate() {
        if i == leader_idx {
            continue;
        }
        let err = c
            .submit::<Cmd, Resp>(&Cmd::Inc(10))
            .await
            .unwrap_err();
        match err {
            ClientError::NotLeader { hint: Some(l) } => {
                assert_eq!(l, leader_id, "follower {i}: wrong leader hint")
            }
            e => panic!(
                "follower {i}: expected NotLeader{{hint: Some({leader_id})}}, got {e:?}"
            ),
        }
    }

    // ── Shutdown: clients → services → nodes ───────────────────────────────
    for c in clients {
        c.shutdown().await.expect("client shutdown");
    }
    for s in svc_handles.into_iter() {
        s.shutdown().await.expect("svc shutdown");
    }
    for n in node_handles.into_iter() {
        n.shutdown().await.expect("node shutdown");
    }
}
