//! 3-node shmem cluster with leader-kill/restart + service-crash faults,
//! keeping a quorum at all times. Built on the m2/m3 spawn patterns.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use uc_client::Client;
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, NodeHandle, NodeId,
    PeerSeed, RaftTuning, ServiceRingConfig, TlsConfig,
};
use uc_service::runtime::ServiceConfig;
use uc_service::{Service, ServiceBuilder};

use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};

/// Serialize cluster bring-up across tests in this binary (mirrors m2).
static CLUSTER_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const APP_ID: &str = "lincheck";

/// Type alias to avoid a too-complex-type lint in the start_3 local variable.
type NodeMeta = (NodeId, SocketAddr, Arc<TempDir>, Arc<TempDir>, Arc<TempDir>);

#[allow(dead_code)] // fields kept for lifetime (TempDirs) + restart (addr, peers)
struct Node {
    id: NodeId,
    addr: SocketAddr,
    instance_dir: Arc<TempDir>,
    data_dir: Arc<TempDir>,
    svc_data_dir: Arc<TempDir>,
    peers: Vec<PeerSeed>,
    handle: Option<NodeHandle<RegisterSm>>,
    service: Option<Service>,
    /// Arc so submit/read can clone-out and release the lock before .await
    client: Option<Arc<Client>>,
}

pub struct LinCluster {
    /// All methods are &self; faults + workers share Arc<LinCluster>.
    nodes: tokio::sync::Mutex<Vec<Node>>,
    _serial: tokio::sync::MutexGuard<'static, ()>,
}

fn pick_addrs(n: usize) -> Vec<SocketAddr> {
    (0..n)
        .map(|_| {
            let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let a = s.local_addr().unwrap();
            drop(s);
            a
        })
        .collect()
}

fn node_config(
    id: NodeId,
    instance: &TempDir,
    data: &TempDir,
    addr: SocketAddr,
    peers: Vec<PeerSeed>,
) -> NodeConfig {
    NodeConfig {
        node_id: id,
        data_dir: data.path().to_owned(),
        raft_listen_addr: addr,
        app_id: APP_ID.into(),
        bootstrap: BootstrapConfig::Peers { peers },
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        ipc_mode: IpcMode::Shmem {
            instance_dir: instance.path().to_owned(),
        },
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
        log_durability: ultima_journal::Durability::Eventual,
    }
}

async fn wait_for_cnc(dir: &std::path::Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !dir.join("cnc.dat").exists() {
        assert!(
            Instant::now() < deadline,
            "cnc.dat never appeared in {dir:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn spawn_service(instance_dir: &std::path::Path, data_dir: &std::path::Path) -> Service {
    let cfg = ServiceConfig {
        instance_dir: instance_dir.to_owned(),
        app_id: APP_ID.into(),
        data_dir: data_dir.to_owned(),
        ..ServiceConfig::default()
    };
    // Service-side SM is plain in-memory (register.rs persists nothing). A
    // service-only restart therefore comes back EMPTY; the node reconstructs it
    // from the replicated log — that recovery is exactly what the capstone proves.
    ServiceBuilder::new(cfg, RegisterSm::default())
        .run()
        .await
        .expect("service start")
}

impl LinCluster {
    /// Bring up a 3-node shmem cluster + one service + one client per node.
    ///
    /// Follows the m3_service_crash/m3_three_node_shmem boot dance:
    ///   1. Spawn all 3 node start() tasks in parallel.
    ///   2. For each node, wait for cnc.dat then spawn its service task.
    ///      The node's start() blocks internally on wait_for_service_ready, so
    ///      the service must be spawned (and reach Ready) before we collect the
    ///      node handle.
    ///   3. Collect all node + service handles (with timeout).
    ///   4. Connect one client per node.
    ///   5. Wait for a stable leader.
    pub async fn start_3() -> Self {
        // `CLUSTER_SERIAL` is a `static`, so the guard is already `'static` — no
        // transmute needed to store it in the struct.
        let serial: tokio::sync::MutexGuard<'static, ()> = CLUSTER_SERIAL.lock().await;

        let addrs = pick_addrs(3);
        let peers: Vec<PeerSeed> = (1..=3u64)
            .zip(addrs.iter())
            .map(|(id, a)| PeerSeed {
                node_id: id,
                raft_addr: *a,
            })
            .collect();

        // ── Step 1: spawn all 3 node tasks in parallel ──────────────────────
        // We must keep instance_dir / data_dir / svc_data_dir alive here so
        // they persist until we can move them into the Node structs.
        let mut node_meta: Vec<NodeMeta> = Vec::new();
        let mut node_tasks = Vec::new();
        for (i, addr) in addrs.iter().enumerate() {
            let id = (i as u64) + 1;
            let instance = Arc::new(TempDir::new().unwrap());
            let data = Arc::new(TempDir::new().unwrap());
            let svc_data = Arc::new(TempDir::new().unwrap());
            let cfg = node_config(id, &instance, &data, *addr, peers.clone());
            let task =
                tokio::spawn(
                    async move { NodeBuilder::new(cfg, RegisterSm::default()).start().await },
                );
            node_tasks.push(task);
            node_meta.push((id, *addr, instance, data, svc_data));
        }

        // ── Step 2: for each node, wait for cnc.dat then spawn its service ──
        // Node start() waits internally for the service to reach Ready before
        // returning, so the service MUST be spawned before we collect the
        // node handle.
        let mut svc_tasks = Vec::new();
        for (_, _, instance, _, svc_data) in &node_meta {
            wait_for_cnc(instance.path(), Duration::from_secs(10)).await;
            let instance_path = instance.path().to_owned();
            let svc_data_path = svc_data.path().to_owned();
            svc_tasks.push(tokio::spawn(async move {
                spawn_service(&instance_path, &svc_data_path).await
            }));
        }

        // ── Step 3: collect node handles ────────────────────────────────────
        let mut node_handles: Vec<NodeHandle<RegisterSm>> = Vec::new();
        for (i, task) in node_tasks.into_iter().enumerate() {
            let id = node_meta[i].0;
            let handle = tokio::time::timeout(Duration::from_secs(30), task)
                .await
                .unwrap_or_else(|_| panic!("node {id} start timed out"))
                .expect("node task panic")
                .unwrap_or_else(|e| panic!("node {id} start: {e:?}"));
            node_handles.push(handle);
        }

        // ── Step 3b: collect service handles ────────────────────────────────
        let mut svc_handles: Vec<Service> = Vec::new();
        for (i, task) in svc_tasks.into_iter().enumerate() {
            let id = node_meta[i].0;
            let svc = tokio::time::timeout(Duration::from_secs(30), task)
                .await
                .unwrap_or_else(|_| panic!("svc {id} start timed out"))
                .expect("svc task panic");
            svc_handles.push(svc);
        }

        // ── Step 4: connect one client per node ─────────────────────────────
        let mut nodes: Vec<Node> = Vec::new();
        for (id, addr, instance, data, svc_data) in node_meta {
            let client = Arc::new(
                Client::connect(instance.path(), APP_ID)
                    .await
                    .expect("client connect"),
            );
            nodes.push(Node {
                id,
                addr,
                instance_dir: instance,
                data_dir: data,
                svc_data_dir: svc_data,
                peers: peers.clone(),
                handle: Some(node_handles.remove(0)),
                service: Some(svc_handles.remove(0)),
                client: Some(client),
            });
        }

        // ── Step 5: wait for a stable leader ────────────────────────────────
        let cluster = LinCluster {
            nodes: tokio::sync::Mutex::new(nodes),
            _serial: serial,
        };
        cluster
            .wait_for_stable_leader(Duration::from_secs(15))
            .await;
        cluster
    }

    /// Clone the live (connected) clients out under a brief lock so callers can
    /// read leader status without holding the nodes lock across anything.
    async fn live_clients(&self) -> Vec<Arc<Client>> {
        self.nodes
            .lock()
            .await
            .iter()
            .filter_map(|n| n.client.clone())
            .collect()
    }

    /// node_id of the current leader, from any live client's NodeStatus.
    /// Uses the SYNC `Client::current_leader()` on cloned `Arc<Client>`s, so the
    /// nodes lock is never held across an await — safe under concurrent faults.
    pub async fn leader_id(&self) -> Option<NodeId> {
        for c in self.live_clients().await {
            if let Some(l) = c.current_leader() {
                return Some(l);
            }
        }
        None
    }

    /// Clone out the `Arc<Client>` for `id` (caller drops the guard before await).
    async fn client_for(&self, id: NodeId) -> Option<Arc<Client>> {
        let nodes = self.nodes.lock().await;
        nodes
            .iter()
            .find(|n| n.id == id)
            .and_then(|n| n.client.clone())
    }

    pub async fn wait_for_stable_leader(&self, timeout: Duration) -> NodeId {
        let deadline = Instant::now() + timeout;
        loop {
            assert!(
                Instant::now() < deadline,
                "no stable leader within {timeout:?}"
            );
            // All live nodes must agree on the same leader id. Read each via the
            // SYNC `Client::current_leader()` on cloned `Arc<Client>`s — the nodes
            // lock is dropped by `live_clients()` before any of this runs.
            let clients = self.live_clients().await;
            let count = clients.len();
            let mut seen: Option<NodeId> = None;
            let mut agree = true;
            for c in &clients {
                match c.current_leader() {
                    Some(l) => match seen {
                        None => seen = Some(l),
                        Some(s) if s == l => {}
                        Some(_) => agree = false,
                    },
                    None => agree = false,
                }
            }
            if agree
                && count >= 2
                && let Some(l) = seen
            {
                return l;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Submit a command to the current leader, retrying on did-not-execute
    /// errors. Returns Ok(resp) | "indeterminate" | propagates fatal.
    pub async fn submit_cmd(&self, cmd: &Cmd) -> SubmitOutcome {
        use uc_client::ClientError as CE;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if Instant::now() > deadline {
                // gave up routing; treat as in-limbo
                return SubmitOutcome::Indeterminate;
            }
            let Some(lid) = self.leader_id().await else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            // Clone the Arc<Client> and DROP the lock before the network await.
            let Some(client) = self.client_for(lid).await else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            match client.submit::<Cmd, CmdResp>(cmd).await {
                Ok(r) => return SubmitOutcome::Ok(r),
                // did-not-execute → retry against the (new) leader
                Err(CE::NotLeader { .. }) | Err(CE::BackpressureFull) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                // indeterminate → may have committed; do not retry
                Err(CE::Timeout(_))
                | Err(CE::ResponseOverwritten)
                | Err(CE::NodeStalled)
                | Err(CE::ServiceStalled) => {
                    return SubmitOutcome::Indeterminate;
                }
                Err(other) => return SubmitOutcome::Fatal(format!("{other:?}")),
            }
        }
    }

    /// Linearizable read against the current leader.
    pub async fn read(&self) -> ReadOutcome {
        use uc_client::ClientError as CE;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if Instant::now() > deadline {
                return ReadOutcome::Indeterminate;
            }
            let Some(lid) = self.leader_id().await else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            let Some(client) = self.client_for(lid).await else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            match client.query_linearizable::<(), Option<u64>>(&()).await {
                Ok(v) => return ReadOutcome::Ok(v),
                Err(CE::NotLeader { .. }) | Err(CE::BackpressureFull) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(CE::Timeout(_))
                | Err(CE::ResponseOverwritten)
                | Err(CE::NodeStalled)
                | Err(CE::ServiceStalled) => {
                    return ReadOutcome::Indeterminate;
                }
                Err(other) => return ReadOutcome::Fatal(format!("{other:?}")),
            }
        }
    }

    /// Kill the current leader's node + service (graceful), then restart the
    /// node (rejoin via persisted data_dir) + a fresh service, and reconnect
    /// its client (restart → new instance_id invalidates the old client).
    /// `&self`: takes handles out under a brief lock, awaits teardown/restart
    /// UNLOCKED, then re-locks to install — so workers aren't blocked on the lock
    /// across the multi-second failover.
    pub async fn kill_and_restart_leader(&self) {
        let Some(lid) = self.leader_id().await else {
            return;
        };
        let (idx, id, addr, instance, data, svc_data, peers, client, service, handle) = {
            let mut nodes = self.nodes.lock().await;
            let Some(i) = nodes.iter().position(|n| n.id == lid) else {
                return;
            };
            let n = &mut nodes[i];
            (
                i,
                n.id,
                n.addr,
                n.instance_dir.clone(),
                n.data_dir.clone(),
                n.svc_data_dir.clone(),
                n.peers.clone(),
                n.client.take(),
                n.service.take(),
                n.handle.take(),
            )
        };
        // Teardown unlocked. Order matters under concurrent load: shut the NODE
        // down FIRST, while its service is still alive. A worker may have an
        // in-flight client_write being applied; if we killed the service first,
        // the node's apply loop would block forever awaiting an apply_resp from
        // the dead service and node.shutdown() would hang. With the service still
        // up, raft shutdown drains/cancels the in-flight apply cleanly. Then the
        // service, then the (already-taken) client.
        //
        // Reuse-the-persisted-data-dir rejoin (the design intent): on restart the
        // node re-applies the replayed log and recovery clamps the durable
        // `output_progress` (which leads `last_applied` until the first snapshot)
        // down to `last_applied`, re-running outputs at-least-once. See
        // uc_node/src/runtime/recovery.rs.
        if let Some(h) = handle {
            let _ = h.shutdown().await;
        }
        if let Some(s) = service {
            let _ = s.shutdown().await;
        }
        // The client is usually still cloned by an in-flight worker; if we happen
        // to be the sole owner, shut it down (otherwise it's retired on drop).
        if let Some(c) = client
            && let Ok(c) = Arc::try_unwrap(c)
        {
            let _ = c.shutdown().await;
        }
        drop(instance); // retire the old shmem instance_dir (fresh one below)
        // Survivors re-elect (quorum 2/3 holds).
        self.wait_for_stable_leader(Duration::from_secs(15)).await;
        // Restart the killed node against its PERSISTED raft data_dir (so it
        // rejoins the existing 3-node membership under the same node_id) but a
        // FRESH shmem instance_dir. The shmem control/ring files (cnc.dat etc.)
        // are volatile per-process IPC, not cluster state; reusing the old
        // instance_dir would leave a stale cnc.dat that makes `wait_for_cnc`
        // return before the new node has reinitialized it, racing the service
        // handshake. A clean instance_dir avoids that.
        let instance = Arc::new(TempDir::new().unwrap());
        let cfg = node_config(id, &instance, &data, addr, peers);
        // Node start() blocks internally on the service handshake (waits for the
        // service to reach Ready before returning), so we must spawn start() as a
        // task, bring the service up once cnc.dat appears, THEN collect the node
        // handle — mirroring the start_3 boot dance.
        let cnc_instance = instance.clone();
        let node_task =
            tokio::spawn(async move { NodeBuilder::new(cfg, RegisterSm::default()).start().await });
        wait_for_cnc(cnc_instance.path(), Duration::from_secs(10)).await;
        let new_service = spawn_service(instance.path(), svc_data.path()).await;
        let new_handle = tokio::time::timeout(Duration::from_secs(30), node_task)
            .await
            .unwrap_or_else(|_| panic!("node {id} restart timed out"))
            .expect("node restart task panic")
            .unwrap_or_else(|e| panic!("node {id} restart: {e:?}"));
        let new_client = Arc::new(
            Client::connect(instance.path(), APP_ID)
                .await
                .expect("client reconnect after restart"),
        );
        {
            let mut nodes = self.nodes.lock().await;
            let n = &mut nodes[idx];
            n.instance_dir = instance;
            n.handle = Some(new_handle);
            n.service = Some(new_service);
            n.client = Some(new_client);
        }
        self.wait_for_stable_leader(Duration::from_secs(15)).await;
    }

    /// Crash the current leader's SERVICE only (node stays up); the service
    /// watcher transfers leadership. Then restart a fresh service on the same
    /// instance_dir so that node is fully functional again.
    pub async fn crash_and_restart_leader_service(&self) {
        let Some(lid) = self.leader_id().await else {
            return;
        };
        let (idx, instance, svc_data, service) = {
            let mut nodes = self.nodes.lock().await;
            let Some(i) = nodes.iter().position(|n| n.id == lid) else {
                return;
            };
            let n = &mut nodes[i];
            (
                i,
                n.instance_dir.clone(),
                n.svc_data_dir.clone(),
                n.service.take(),
            )
        };
        if let Some(s) = service {
            let _ = s.shutdown().await;
        }
        // Leadership transfers away from the stalled node (m3 path).
        self.wait_for_stable_leader(Duration::from_secs(15)).await;
        // Restart the service so the node can serve again.
        let new_service = spawn_service(instance.path(), svc_data.path()).await;
        {
            let mut nodes = self.nodes.lock().await;
            nodes[idx].service = Some(new_service);
        }
    }

    pub async fn shutdown(self) {
        // Take everything out under the lock, then await teardown unlocked.
        let mut drained = std::mem::take(&mut *self.nodes.lock().await);
        for n in &mut drained {
            if let Some(c) = n.client.take() {
                // Last Arc owner here; unwrap to call the by-value shutdown.
                if let Ok(c) = Arc::try_unwrap(c) {
                    let _ = c.shutdown().await;
                }
            }
            if let Some(s) = n.service.take() {
                let _ = s.shutdown().await;
            }
            if let Some(h) = n.handle.take() {
                let _ = h.shutdown().await;
            }
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)] // Ok(CmdResp) and Fatal(String) used by callers/future tests
pub enum SubmitOutcome {
    Ok(CmdResp),
    Indeterminate,
    Fatal(String),
}

#[derive(Debug)]
#[allow(dead_code)] // Ok(Option<u64>) and Fatal(String) used by callers/future tests
pub enum ReadOutcome {
    Ok(Option<u64>),
    Indeterminate,
    Fatal(String),
}
