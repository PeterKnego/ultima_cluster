//! 3-node leader-failover test (M4 Task 5.5).
//!
//! Brings up three nodes + three services + one uc_client per instance
//! directory (same boot dance as `m4_client_three_node`). Then:
//!
//! 1. Identifies the current leader via `Client::current_leader()` (which
//!    reads the `NodeStatus::leader_node_id` field now populated by the
//!    metrics-publisher task).
//! 2. Shuts down the leader node (simulating a crash).
//! 3. Asserts the dead-leader's client detects `NodeStalled` (the node's
//!    heartbeat_seq stops advancing; the client's stall watcher fires).
//! 4. Waits for the surviving two nodes to elect a new leader (visible via
//!    `current_leader()` on a surviving client).
//! 5. Submits a command through a surviving client that points at the
//!    new leader, confirming the cluster is healthy post-failover.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_client::{Client, ClientError};
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, NodeHandle, NodeId,
    PeerSeed, RaftTuning, ServiceRingConfig, TlsConfig, Transport,
};
use uc_service::runtime::ServiceConfig;
use uc_service::{ServiceBuilder, SnapshotError, StateMachine};

// ── State-machine (identical to m4_client_three_node) ─────────────────────

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
    type SnapshotHandle = Vec<u8>;

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
    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        let buf = bincode::serde::encode_to_vec(
            (self.value, self.last_applied),
            bincode::config::standard(),
        )
        .map_err(|e| SnapshotError::Codec(e.to_string()))?;
        Ok((buf, self.last_applied.unwrap_or(0)))
    }
    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
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

// ── Helpers ────────────────────────────────────────────────────────────────

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

// ── Test ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn m4_client_leader_failover() {
    let _ = tracing_subscriber::fmt::try_init();

    let addrs = pick_three_addrs();
    let peer_seeds: Vec<PeerSeed> = (1..=3u64)
        .zip(addrs.iter())
        .map(|(id, a)| PeerSeed {
            node_id: id,
            raft_addr: *a,
        })
        .collect();

    let app_id = "m4-leader-failover".to_string();

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

    // ── Wait for initial leader convergence ─────────────────────────────────
    let leader_id = wait_for_leader(&node_handles, Duration::from_secs(15)).await;
    assert!((1..=3).contains(&leader_id));
    // node_ids are 1-based; vec is 0-based (index 0 = node 1).
    let leader_idx = (leader_id as usize) - 1;

    // ── Connect one client per instance_dir ────────────────────────────────
    let mut clients: Vec<Client> = Vec::with_capacity(3);
    for d in &instance_dirs {
        clients.push(
            Client::connect(d.path(), &app_id)
                .await
                .expect("client connect"),
        );
    }

    // ── Wait for clients to see the leader in cnc NodeStatus ───────────────
    // The metrics publisher runs async; poll until leader_node_id is set.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if clients[leader_idx].current_leader() == Some(leader_id) {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "client did not see leader_id={leader_id} in NodeStatus within 10s; \
                 got {:?}",
                clients[leader_idx].current_leader()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ── Submit a baseline command to confirm cluster is healthy ────────────
    let r: Resp = clients[leader_idx]
        .submit(&Cmd::Inc(1))
        .await
        .expect("baseline submit");
    assert_eq!(r.value, 1);

    // ── Shut down the leader node (simulate crash) ─────────────────────────
    // `remove` shifts later elements; process in reverse order of position
    // to keep indices stable (or just remove by leader_idx once).
    let dead_leader_node = node_handles.remove(leader_idx);
    // Corresponding svc_handle shares the same index.
    let dead_leader_svc = svc_handles.remove(leader_idx);
    // instance_dirs keeps the same order; retain it for directory access.

    // Shut down the leader's node first (raft shuts down; heartbeat stops).
    dead_leader_node
        .shutdown()
        .await
        .expect("dead leader node shutdown");
    // Also shut down its service (it's now orphaned).
    dead_leader_svc
        .shutdown()
        .await
        .expect("dead leader svc shutdown");

    // ── Dead-leader's client should detect NodeStalled ─────────────────────
    // The heartbeat ticker on the dead node has stopped. The client's stall
    // watcher fires when heartbeat_seq doesn't advance within ~2 s.
    // We wait up to 10 s total for the stall to propagate.
    let stall_deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut got_stall = false;
    while std::time::Instant::now() < stall_deadline {
        match clients[leader_idx].submit::<Cmd, Resp>(&Cmd::Inc(99)).await {
            Err(ClientError::NodeStalled) => {
                got_stall = true;
                break;
            }
            // Any other result (timeout, shutdown echo): wait and retry.
            _ => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    assert!(
        got_stall,
        "dead-leader client should detect NodeStalled within 10s"
    );

    // ── Surviving clients should see a new leader ──────────────────────────
    // node_handles still has 2 entries; clients still has 3 entries (leader_idx
    // is the dead one). Find a new leader via clients at non-leader indices.
    let surviving_indices: Vec<usize> = (0..3).filter(|&i| i != leader_idx).collect();

    let new_leader_deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut new_leader_id: Option<u64> = None;
    'outer: while std::time::Instant::now() < new_leader_deadline {
        for &si in &surviving_indices {
            if let Some(l) = clients[si].current_leader()
                && l != leader_id
            {
                new_leader_id = Some(l);
                break 'outer;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let new_leader_id =
        new_leader_id.expect("surviving nodes should elect a new leader within 30s");
    assert_ne!(
        new_leader_id, leader_id,
        "new leader must differ from the old one"
    );

    // ── Preemptively remove the dead voter from the membership ─────────────
    // Without this, openraft keeps retrying AppendEntries to the dead
    // node and post-failover client_writes time out. The new leader has
    // quorum with itself + the other survivor, so the change_membership
    // call commits without needing the dead leader.
    //
    // Find the NodeHandle whose node_id == new_leader_id (the surviving
    // handles are in node_handles after the leader_idx removal above).
    let new_leader_handle = node_handles
        .iter()
        .find(|h| h.node_id() == new_leader_id)
        .expect("new leader handle missing from surviving set");
    new_leader_handle
        .remove_node(leader_id)
        .await
        .expect("remove dead leader from voter set");

    // ── Submit through a surviving client, retrying until the new leader
    // confirms quorum. After `remove_node` the new leader may briefly
    // return NotLeader while it stabilises post-membership-change. Allow
    // up to 30 s — change_membership can churn for a while as openraft
    // processes the joint-consensus transition. ──
    let submit_deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut post_failover_ok = false;
    let mut last_observed_leader: Vec<Option<u64>> = vec![None; 3];
    while std::time::Instant::now() < submit_deadline {
        // Try every surviving client whose cnc-visible leader is non-None
        // (it may have flipped to something other than new_leader_id post-
        // membership-change; the new leader id is whoever the surviving
        // clients agree on now).
        for &si in &surviving_indices {
            let leader = clients[si].current_leader();
            last_observed_leader[si] = leader;
            if leader.is_none() {
                continue;
            }
            match clients[si].submit::<Cmd, Resp>(&Cmd::Inc(1)).await {
                Ok(r) => {
                    assert!(
                        r.value >= 2,
                        "post-failover value should be >= 2, got {}",
                        r.value
                    );
                    post_failover_ok = true;
                    break;
                }
                // Stabilisation: NotLeader (membership change in flight),
                // ResponseOverwritten (broadcast lap during stabilisation),
                // Timeout (transient). All retryable.
                Err(ClientError::NotLeader { .. })
                | Err(ClientError::ResponseOverwritten)
                | Err(ClientError::Timeout(_)) => {}
                Err(e) => panic!("post-failover submit unexpected error: {e:?}"),
            }
        }
        if post_failover_ok {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        post_failover_ok,
        "post-failover submit did not succeed within 30s; \
         last observed clients[*].current_leader() = {last_observed_leader:?}"
    );

    // ── Shutdown ────────────────────────────────────────────────────────────
    // Clients (all three, including the dead-leader's — it's just connected
    // to a stopped node so shutdown is harmless).
    for c in clients {
        c.shutdown().await.expect("client shutdown");
    }
    // Surviving services.
    for s in svc_handles {
        s.shutdown().await.expect("svc shutdown");
    }
    // Surviving nodes (dead leader was already shut down above).
    for n in node_handles {
        n.shutdown().await.expect("node shutdown");
    }
}
