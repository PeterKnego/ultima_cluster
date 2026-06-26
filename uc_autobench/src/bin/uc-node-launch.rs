//! uc-node-launch — start one real uc_node (+ co-located uc_service) as a
//! process, for multi-process N-node benchmarks. Uses
//! `BootstrapConfig::Peers` + `IpcMode::Shmem`. N of these launched
//! concurrently form a real N-node cluster. Transport is selectable via the
//! `UC_TRANSPORT` env var (`"udp"` → UDP, anything else / unset → QUIC).
//!
//! The min node_id peer bootstraps (initialize + add_learner +
//! change_membership); every peer must be started concurrently because the
//! bootstrapper's `add_learner(blocking=true)` waits for each peer's QUIC
//! server to be reachable.
//!
//! Within ONE process the node and service must come up together: in
//! `IpcMode::Shmem` `NodeBuilder::start()` blocks in `wait_for_service_ready`
//! until the co-located `ServiceBuilder::run()` publishes `state = Ready`,
//! and the service blocks attaching to cnc.dat until the node creates it.
//! So we spawn the node task, wait for cnc.dat, spawn the service, then join
//! both. Pattern mirrors `uc_node/tests/m3_three_node_shmem.rs` and
//! `examples/counter_loop/src/bin/counter_loop_service.rs`.
//!
//! Runtime MUST be current_thread (memory feedback_m3_test_runtime_flavor):
//! a multi_thread runtime intermittently times out the shmem handshake. This
//! matches `m3_three_node_shmem` (plain `#[tokio::test]`, current_thread).

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Parser;
use serde::{Deserialize, Serialize};
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, PeerSeed, RaftTuning,
    ServiceRingConfig, TlsConfig,
};
use uc_service::runtime::ServiceConfig;
use uc_service::{ServiceBuilder, SnapshotError, StateMachine};

/// KV command: write `val` at `key`. MUST match commit-path-load.rs's wire
/// shape so the load driver's `submit::<KvCmd, u64>` calls deserialize
/// correctly against this node's service.
#[derive(Serialize, Deserialize)]
enum KvCmd {
    Put { key: u64, val: Vec<u8> },
}

/// In-memory KV state machine. Identical to commit-path-load.rs's `KvSm`.
#[derive(Default)]
struct KvSm {
    map: std::collections::HashMap<u64, Vec<u8>>,
    last_applied: Option<u64>,
}

impl StateMachine for KvSm {
    type Command = KvCmd;
    type Response = u64; // returns current map.len()
    type Query = u64; // key to read
    type QueryResponse = Option<Vec<u8>>;
    type SnapshotHandle = Vec<u8>;

    fn apply(&mut self, log_index: u64, cmd: KvCmd) -> u64 {
        match cmd {
            KvCmd::Put { key, val } => {
                self.map.insert(key, val);
            }
        }
        self.last_applied = Some(log_index);
        self.map.len() as u64
    }

    fn query(&self, key: u64) -> Option<Vec<u8>> {
        self.map.get(&key).cloned()
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

#[derive(Parser)]
#[command(about = "Launch one real uc_node + co-located uc_service for a \
                   multi-process N-node cluster (Peers + Shmem)")]
struct Args {
    /// This node's raft node_id. The min node_id across the --peer set
    /// bootstraps the cluster.
    #[arg(long)]
    node_id: u64,
    /// This node's QUIC raft listen address (must match the --peer entry for
    /// this node_id on every peer).
    #[arg(long)]
    listen: SocketAddr,
    /// Repeated peer descriptors: --peer 1@127.0.0.1:7001 --peer 2@127.0.0.1:7002 ...
    /// Include THIS node in the list. Every launched peer must pass the same set.
    #[arg(long = "peer", required = true)]
    peers: Vec<String>,
    /// Directory for this node's shmem instance (cnc.dat + rings). Per-node.
    #[arg(long)]
    instance_dir: PathBuf,
    /// Persistent data directory (raft log + state + TLS). Per-node.
    #[arg(long)]
    data_dir: PathBuf,
    /// app_id; must match across all peers and the load client.
    #[arg(long, default_value = "uc-bench-3node")]
    app_id: String,
    /// Co-locate a uc_service in-process (default true). When false the node
    /// will hang in wait_for_service_ready — only disable if a service is
    /// attached out-of-band.
    #[arg(long, default_value_t = true)]
    with_service: bool,
}

fn parse_peer(s: &str) -> anyhow::Result<PeerSeed> {
    let (id, addr) = s
        .split_once('@')
        .ok_or_else(|| anyhow::anyhow!("peer must be id@addr, got `{s}`"))?;
    Ok(PeerSeed {
        node_id: id
            .parse()
            .map_err(|e| anyhow::anyhow!("bad peer node_id in `{s}`: {e}"))?,
        raft_addr: addr
            .parse()
            .map_err(|e| anyhow::anyhow!("bad peer addr in `{s}`: {e}"))?,
    })
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    let args = Args::parse();

    let peers: Vec<PeerSeed> = args
        .peers
        .iter()
        .map(|s| parse_peer(s))
        .collect::<anyhow::Result<_>>()?;

    std::fs::create_dir_all(&args.instance_dir)?;
    std::fs::create_dir_all(&args.data_dir)?;

    // NodeConfig does NOT derive Default — build every field explicitly, the
    // same way m3_three_node_shmem.rs does.
    let node_cfg = NodeConfig {
        node_id: args.node_id,
        data_dir: args.data_dir.clone(),
        raft_listen_addr: args.listen,
        app_id: args.app_id.clone(),
        bootstrap: BootstrapConfig::Peers {
            peers: peers.clone(),
        },
        raft: RaftTuning {
            // Sweep knob for the 3-node throughput experiment; default 300.
            max_payload_entries: std::env::var("UC_MAX_PAYLOAD_ENTRIES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            ..RaftTuning::default()
        },
        tls: TlsConfig::default(),
        // Transport is selectable via UC_TRANSPORT env var; default QUIC.
        // "udp" (case-insensitive) → UDP with default tuning; anything else → QUIC.
        transport: match std::env::var("UC_TRANSPORT")
            .ok()
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("udp") => uc_node::Transport::Udp(uc_node::UdpTuning::default()),
            _ => uc_node::Transport::Quic,
        },
        ipc_mode: IpcMode::Shmem {
            instance_dir: args.instance_dir.clone(),
        },
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
        // Durability is read from UC_DURABILITY env var so the bench-infra
        // `durability` knob (group_vars/all.yml) is threaded all the way down
        // to the UC node — mirrors the UC_MAX_PAYLOAD_ENTRIES pattern above.
        // "eventual" (case-insensitive) → Eventual; anything else → Consistent.
        log_durability: match std::env::var("UC_DURABILITY")
            .ok()
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("eventual") => uc_node::Durability::Eventual,
            _ => uc_node::Durability::Consistent,
        },
    };

    // Node + service rendezvous: spawn the node, wait for cnc.dat, then spawn
    // the service, then join both. In Shmem mode start() blocks in
    // wait_for_service_ready until the service publishes Ready.
    let instance_dir = args.instance_dir.clone();
    let node_task =
        tokio::spawn(async move { NodeBuilder::new(node_cfg, KvSm::default()).start().await });

    let service = if args.with_service {
        wait_for_path(&instance_dir.join("cnc.dat"), Duration::from_secs(30)).await?;
        let svc_cfg = ServiceConfig {
            instance_dir: args.instance_dir.clone(),
            app_id: args.app_id.clone(),
            data_dir: args.data_dir.join("service"),
            ..ServiceConfig::default()
        };
        let svc_task =
            tokio::spawn(async move { ServiceBuilder::new(svc_cfg, KvSm::default()).run().await });
        let svc = svc_task
            .await
            .map_err(|e| anyhow::anyhow!("service task panic: {e}"))?
            .map_err(|e| anyhow::anyhow!("service start: {e}"))?;
        Some(svc)
    } else {
        None
    };

    let node = node_task
        .await
        .map_err(|e| anyhow::anyhow!("node task panic: {e}"))?
        .map_err(|e| anyhow::anyhow!("node start: {e}"))?;

    eprintln!(
        "uc-node-launch: node {} up, listening on {} (app_id={}, instance_dir={})",
        args.node_id,
        args.listen,
        args.app_id,
        instance_dir.display()
    );

    // Floor-decomposition probe (uc-bench-probes only): the leader records every
    // append_entries round-trip it observes (network/quic/instance.rs). Drain it
    // periodically into a cumulative accumulator and emit p50/p99/count so the
    // last line before `pkill -9` teardown carries the whole-run aggregate. Only
    // the leader produces samples; 1-node arms emit n=0 (no replication).
    #[cfg(feature = "uc-bench-probes")]
    tokio::spawn(async move {
        let mut acc: Vec<u64> = Vec::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(3));
        loop {
            ticker.tick().await;
            acc.extend(uc_protocol::probes::drain_repl_rpc());
            if acc.is_empty() {
                eprintln!("REPL_RPC_STATS n=0");
                continue;
            }
            let mut s = acc.clone();
            s.sort_unstable();
            let pct = |p: f64| s[((s.len() as f64 * p) as usize).min(s.len() - 1)];
            eprintln!(
                "REPL_RPC_STATS n={} p50_ns={} p99_ns={} min_ns={} max_ns={}",
                s.len(),
                pct(0.50),
                pct(0.99),
                s[0],
                s[s.len() - 1],
            );
        }
    });

    // Run until Ctrl-C, then shut down service first, then node (the node's
    // _instance still holds the cnc mmap; node.shutdown joins the heartbeat
    // ticker before dropping it).
    tokio::signal::ctrl_c().await?;
    eprintln!("uc-node-launch: node {} shutting down", args.node_id);
    if let Some(svc) = service {
        svc.shutdown().await.ok();
    }
    node.shutdown().await?;
    Ok(())
}
