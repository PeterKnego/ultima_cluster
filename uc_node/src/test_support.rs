//! Reusable test fixtures for spawning an in-process node + service + clients.
//!
//! Available only with `--features test-support`. Used by:
//!   - uc_node integration tests (`uc_node/tests/m4_*.rs`)
//!   - the `uc_autobench` shmem-e2e benchmark binary
//!
//! Keep this minimal: only the setup that is genuinely shared by ≥ 2 callers
//! belongs here. The `single_node` fixture extracts the boot dance repeated
//! across the M4 single-node tests (`m4_client_single_node`,
//! `m4_client_concurrent`, `m4_client_wrap`, ...): spawn the node, wait for
//! `cnc.dat`, spawn the service, await both handles, wait for the single-node
//! leader, then attach `n_clients` clients.
//!
//! ## Runtime flavor caveat
//!
//! In-process node + service must run under a `current_thread` tokio runtime
//! (`#[tokio::test]` or `#[tokio::main(flavor = "current_thread")]`). A
//! `multi_thread` runtime intermittently times out the shmem handshake. This
//! module spawns no internal runtime — `single_node` is `async` and inherits
//! the caller's runtime.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use tempfile::TempDir;
use uc_client::Client;
use uc_service::{Service, ServiceBuilder, ServiceConfig, StateMachine};

use crate::config::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeConfig, RaftTuning, ServiceRingConfig,
    TlsConfig,
};
use crate::runtime::builder::NodeBuilder;
use crate::runtime::node::NodeHandle;

/// A spawned in-process single-node cluster: one node, one service, and
/// `n_clients` attached clients. Everything that needs ordered teardown is
/// owned here; [`Drop`] shuts the clients/service/node down (best-effort) and
/// the backing temp dirs are removed when the fixture is dropped.
pub struct ClusterFixture<S: StateMachine> {
    /// Connected clients, in attach order. Use [`ClusterFixture::client`].
    clients: Vec<Client>,
    service: Option<Service>,
    node: Option<NodeHandle<S>>,
    app_id: String,
    /// Temp dirs kept alive for the lifetime of the fixture (instance,
    /// node data, service data). Dropped last.
    _instance_dir: TempDir,
    _node_data: TempDir,
    _svc_data: TempDir,
    instance_path: PathBuf,
}

impl<S: StateMachine + Default> ClusterFixture<S> {
    /// Spawn a single-node cluster with `n_clients` clients attached.
    ///
    /// Returns once the node has been elected leader (node_id 1) and every
    /// client has completed its handshake — i.e. the cluster is ready to accept
    /// submits. `app_id` is fixed to `"test-support-single"`.
    pub async fn single_node(n_clients: usize) -> anyhow::Result<Self> {
        Self::single_node_with_app_id(n_clients, "test-support-single").await
    }

    /// Like [`single_node`](Self::single_node) but with a caller-chosen `app_id`
    /// (useful when several fixtures coexist in one test process).
    pub async fn single_node_with_app_id(n_clients: usize, app_id: &str) -> anyhow::Result<Self> {
        let instance_dir = TempDir::new().context("create instance tempdir")?;
        let node_data = TempDir::new().context("create node data tempdir")?;
        let svc_data = TempDir::new().context("create service data tempdir")?;
        let instance_path = instance_dir.path().to_owned();
        let app_id = app_id.to_string();

        // ── Spawn the node ──────────────────────────────────────────────────
        let cfg = NodeConfig {
            node_id: 1,
            data_dir: node_data.path().to_owned(),
            raft_listen_addr: "127.0.0.1:0".parse().expect("loopback addr"),
            app_id: app_id.clone(),
            bootstrap: BootstrapConfig::SingleNode,
            raft: RaftTuning::default(),
            tls: TlsConfig::default(),
            ipc_mode: IpcMode::Shmem {
                instance_dir: instance_path.clone(),
            },
            client_rings: ClientRingConfig::default(),
            service_rings: ServiceRingConfig::default(),
            log_durability: ultima_journal::Durability::Eventual,
        };
        let node_task =
            tokio::spawn(async move { NodeBuilder::new(cfg, S::default()).start().await });

        wait_for_path(&instance_path.join("cnc.dat"), Duration::from_secs(5)).await?;

        // ── Spawn the service ───────────────────────────────────────────────
        let svc_cfg = ServiceConfig {
            instance_dir: instance_path.clone(),
            app_id: app_id.clone(),
            data_dir: svc_data.path().to_owned(),
            ..ServiceConfig::default()
        };
        let svc_task =
            tokio::spawn(async move { ServiceBuilder::new(svc_cfg, S::default()).run().await });

        let node = tokio::time::timeout(Duration::from_secs(15), node_task)
            .await
            .context("node start timed out")?
            .context("node task panicked")?
            .context("node start failed")?;
        let service = tokio::time::timeout(Duration::from_secs(15), svc_task)
            .await
            .context("service start timed out")?
            .context("service task panicked")?
            .context("service start failed")?;

        // ── Wait for single-node leader election ────────────────────────────
        let mut leader = None;
        for _ in 0..50 {
            if node.current_leader().await == Some(1) {
                leader = Some(1);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if leader != Some(1) {
            anyhow::bail!("single-node leader not elected within ~5s");
        }

        // ── Attach clients ──────────────────────────────────────────────────
        let mut clients = Vec::with_capacity(n_clients);
        for i in 0..n_clients {
            let c = Client::connect(&instance_path, &app_id)
                .await
                .with_context(|| format!("client {i} connect"))?;
            clients.push(c);
        }

        Ok(Self {
            clients,
            service: Some(service),
            node: Some(node),
            app_id,
            _instance_dir: instance_dir,
            _node_data: node_data,
            _svc_data: svc_data,
            instance_path,
        })
    }

    /// The `idx`-th attached client. Use the client's real API directly, e.g.
    /// `fixture.client(0).submit::<Cmd, Resp>(&cmd).await`.
    pub fn client(&self, idx: usize) -> &Client {
        &self.clients[idx]
    }

    /// Number of attached clients.
    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    /// Attach an additional client beyond the initial `n_clients`. Returns the
    /// index of the new client.
    pub async fn attach_client(&mut self) -> anyhow::Result<usize> {
        let c = Client::connect(&self.instance_path, &self.app_id)
            .await
            .context("attach extra client")?;
        self.clients.push(c);
        Ok(self.clients.len() - 1)
    }

    /// The node handle, for tests that need to inspect Raft state directly.
    pub fn node(&self) -> &NodeHandle<S> {
        self.node.as_ref().expect("node present before drop")
    }

    /// The instance directory backing the cluster (where `cnc.dat` lives).
    pub fn instance_path(&self) -> &Path {
        &self.instance_path
    }

    /// The `app_id` the cluster was started with.
    pub fn app_id(&self) -> &str {
        &self.app_id
    }

    /// Ordered async shutdown: clients → service → node. Prefer this when you
    /// want shutdown errors surfaced; otherwise [`Drop`] does a best-effort
    /// teardown. Consumes the fixture.
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        for c in self.clients.drain(..) {
            c.shutdown().await.context("client shutdown")?;
        }
        if let Some(s) = self.service.take() {
            s.shutdown().await.context("service shutdown")?;
        }
        if let Some(n) = self.node.take() {
            n.shutdown().await.context("node shutdown")?;
        }
        Ok(())
    }
}

impl<S: StateMachine> Drop for ClusterFixture<S> {
    fn drop(&mut self) {
        // Best-effort ordered teardown. `shutdown()` is async and consumes the
        // handles; from `Drop` we cannot await, so we just drop the handles in
        // the right order (clients → service → node). Each handle's own `Drop`
        // tears down its tasks. Temp dirs drop last (struct field order).
        self.clients.clear();
        drop(self.service.take());
        drop(self.node.take());
    }
}

async fn wait_for_path(p: &Path, timeout: Duration) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    while !p.exists() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for {}", p.display());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}
