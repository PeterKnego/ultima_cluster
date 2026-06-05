//! Service-crash → leadership-transfer test (M3.5 Task 8 upgrade).
//!
//! Brings up a 3-node shmem cluster, identifies the leader, and shuts
//! down that leader's `uc_service::Service` (stopping its heartbeat
//! ticker — functionally indistinguishable from a service crash from
//! the node's perspective). The node-side
//! [`uc_node::ipc::service_watcher`] should:
//!
//! 1. Observe the missed heartbeats and flip `service_stalled()` to true.
//! 2. Call `raft.trigger().transfer_leader(target)` (real openraft 0.10 API).
//! 3. The cluster re-elects; the original leader stays alive as a follower.
//! 4. A fresh submit through the new leader succeeds.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, NodeHandle, NodeId,
    PeerSeed, RaftTuning, ServiceRingConfig, TlsConfig,
};
use uc_service::runtime::ServiceConfig;
use uc_service::{Service, ServiceBuilder, SnapshotError, StateMachine};

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
async fn service_crash_on_leader_transfers_leadership() {
    let addrs = pick_three_addrs();
    let peer_seeds: Vec<PeerSeed> = (1..=3u64)
        .zip(addrs.iter())
        .map(|(id, a)| PeerSeed {
            node_id: id,
            raft_addr: *a,
        })
        .collect();
    let app_id = "m3-service-crash".to_string();

    let mut instance_dirs: Vec<Arc<TempDir>> = Vec::new();
    let mut _node_data_dirs: Vec<Arc<TempDir>> = Vec::new();
    let mut _svc_data_dirs: Vec<Arc<TempDir>> = Vec::new();
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

    let mut svc_tasks = Vec::new();
    for dir in instance_dirs.iter() {
        wait_for_path(&dir.path().join("cnc.dat"), Duration::from_secs(10)).await;
        let svc_data_dir = Arc::new(TempDir::new().unwrap());
        _svc_data_dirs.push(svc_data_dir.clone());
        let svc_cfg = ServiceConfig {
            instance_dir: dir.path().to_owned(),
            app_id: app_id.clone(),
            data_dir: svc_data_dir.path().to_owned(),
            ..ServiceConfig::default()
        };
        svc_tasks.push(tokio::spawn(async move {
            ServiceBuilder::new(svc_cfg, Counter::default()).run().await
        }));
    }

    let mut node_handles: Vec<NodeHandle<Counter>> = Vec::new();
    for (i, t) in node_tasks.into_iter().enumerate() {
        let h = tokio::time::timeout(Duration::from_secs(30), t)
            .await
            .unwrap_or_else(|_| panic!("node {} start timed out", i + 1))
            .expect("node task panic")
            .unwrap_or_else(|e| panic!("node {} start: {e:?}", i + 1));
        node_handles.push(h);
    }
    // node_id -> Service. Indexed by (node_id - 1).
    let mut services: Vec<Option<Service>> = Vec::new();
    for (i, t) in svc_tasks.into_iter().enumerate() {
        let s = tokio::time::timeout(Duration::from_secs(30), t)
            .await
            .unwrap_or_else(|_| panic!("svc {} start timed out", i + 1))
            .expect("svc task panic")
            .unwrap_or_else(|e| panic!("svc {} start: {e:?}", i + 1));
        services.push(Some(s));
    }

    // ── Elect a leader, submit one command to establish baseline state ──
    let leader_id = wait_for_leader(&node_handles, Duration::from_secs(15)).await;
    let leader = node_handles
        .iter()
        .find(|h| h.node_id() == leader_id)
        .unwrap();
    let r = leader
        .submit(Cmd::Inc(100))
        .await
        .expect("submit pre-crash");
    assert_eq!(r.value, 100);

    // ── Crash the leader's service ──────────────────────────────────────
    // Calling shutdown() on the Service stops its heartbeat ticker, which
    // is exactly the signal the node-side watcher reacts to. The leader
    // node's apply ring will block on subsequent submits.
    let leader_idx = (leader_id - 1) as usize;
    let leader_svc = services[leader_idx].take().expect("leader service");
    leader_svc.shutdown().await.expect("crash leader service");

    // ── Watcher should observe the stall within ~2s ─────────────────────
    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    let mut stalled = false;
    while std::time::Instant::now() < deadline {
        if leader.service_stalled() {
            stalled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(stalled, "leader's service_stalled flag never flipped");

    // ── Cluster should transfer leadership ───────────────────────────────
    // M3.5: leader stays alive as a follower (M3 had it raft-shutdown).
    //
    // Two invariants:
    //   1. The original leader is still alive (raft NOT shut down): its
    //      current_leader() must be Some(x), not None.
    //   2. Leadership actually moved: current_leader() != Some(leader_id).
    //
    // Strategy: wait until ALL three nodes agree on the SAME leader that is
    // not the original leader.  Polling all three avoids the race where one
    // peer briefly reports a transient intermediate winner before the cluster
    // fully converges.  Once all three agree, the new_leader_id is stable
    // and the old leader is confirmed alive as a follower.
    //
    // NOTE on flakiness risk: the test's 15 s convergence deadline AND the
    // service_watcher's 15 s `raft.shutdown()` fallback fire at the same
    // wall-clock moment (both started from stall-detection time). On a
    // pathologically slow host the fallback could fire before convergence,
    // shutting down the old leader's raft → `current_leader() == None`
    // → assertion below fails. The risk is small under normal load (5 runs
    // in CI showed 5/5 ok, ~6 s wall each). If this test flakes in M4+,
    // either bump the fallback timeout or peer-health-aware target selection
    // (deferred per task04_m3_5_openraft_0_10_upgrade.md).
    let old_leader = node_handles
        .iter()
        .find(|h| h.node_id() == leader_id)
        .unwrap();

    let new_leader_id = {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            if std::time::Instant::now() >= deadline {
                panic!(
                    "cluster did not converge on a new leader within 15 s (old leader={leader_id})"
                );
            }
            // Collect current_leader() from all three nodes.
            let mut views: Vec<Option<NodeId>> = Vec::new();
            for h in &node_handles {
                views.push(h.current_leader().await);
            }
            // All three must agree on the same non-None, non-excluded leader.
            if let Some(first) = views[0]
                && first != leader_id
                && views.iter().all(|v| *v == Some(first))
            {
                break first;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    assert_ne!(
        new_leader_id, leader_id,
        "leadership must transfer off the stalled node"
    );

    // Since the old leader was one of the three nodes polled above and it
    // already agreed on new_leader_id, this assertion is stable.
    assert_eq!(
        old_leader.current_leader().await,
        Some(new_leader_id),
        "stalled leader should report the new leader (it's now a follower, not dead)"
    );

    let new_leader = node_handles
        .iter()
        .find(|h| h.node_id() == new_leader_id)
        .unwrap();
    let r = new_leader
        .submit(Cmd::Inc(50))
        .await
        .expect("submit post-transfer");
    assert_eq!(r.value, 150);

    // ── Shutdown remaining services, then all nodes ─────────────────────
    for s in services.into_iter().flatten() {
        s.shutdown().await.expect("svc shutdown");
    }
    for n in node_handles.into_iter() {
        // M3.5: the stalled leader's raft transferred leadership (still
        // alive as a follower); node.shutdown() shuts it down cleanly.
        n.shutdown().await.expect("node shutdown");
    }
}
