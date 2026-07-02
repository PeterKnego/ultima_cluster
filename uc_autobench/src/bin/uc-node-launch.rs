//! uc-node-launch — start one real uc_node (+ co-located uc_service) as a
//! process, for multi-process N-node benchmarks. Uses
//! `BootstrapConfig::Peers`; IPC mode is selectable (`--ipc-mode`):
//!
//! * `shmem` (default) — `IpcMode::Shmem`: the historical bench topology.
//!   Node + co-located `uc_service` over cnc.dat/rings; the load driver is a
//!   separate `commit-path-load` process attached via `uc_client`.
//! * `embedded` — `IpcMode::Embedded`: the co-location arm. The same `KvSm`
//!   runs IN-PROCESS (`AdaptedStateMachine`), no service process, no shmem
//!   surface at all. Because embedded mode has no client rings, the load
//!   driver must live in this process too: pass `--load-rates ...` (node0
//!   only) and this binary runs the shared `loadcore` sweep through
//!   `NodeHandle::submit`, writing the same CSV `commit-path-load` would.
//!   The CSV is written to `<out>.partial` and renamed to `<out>` on
//!   completion, so the harness can wait on the final path.
//!
//! N of these launched concurrently form a real N-node cluster. Transport is
//! selectable via the `UC_TRANSPORT` env var (`"udp"` → UDP, else QUIC).
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
//! Runtimes:
//! * shmem — one current_thread runtime for everything (memory
//!   feedback_m3_test_runtime_flavor: a multi_thread runtime intermittently
//!   times out the shmem handshake).
//! * embedded — SPLIT runtimes: the NODE runs on its own current_thread
//!   runtime on a dedicated thread (matches the shmem arm's node runtime, so
//!   the embedded-vs-shmem A/B isolates topology, not tokio flavor — a
//!   multi_thread node measurably raises the commit floor because every
//!   openraft task hop becomes a cross-worker wakeup), while the in-process
//!   load driver runs on a separate multi_thread runtime (high-inflight
//!   request tasks need cores; same lesson as commit-path-load's runtime,
//!   commits 36c0a5a/1bb9108). Handles/channels cross runtimes safely; the
//!   RaftCore + storage + QUIC futures stay on the node runtime.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use uc_autobench::loadcore::{
    parse_list, request_timeout_from_env, run_sweep, KvCmd, KvSm, NodeSubmitter, Submitter,
    SweepOpts,
};
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, NodeHandle, PeerSeed,
    RaftTuning, ServiceRingConfig, TlsConfig,
};
use uc_service::runtime::ServiceConfig;
use uc_service::ServiceBuilder;

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum IpcModeArg {
    Shmem,
    Embedded,
}

#[derive(Parser)]
#[command(about = "Launch one real uc_node for a multi-process N-node cluster \
                   (Peers bootstrap; --ipc-mode shmem|embedded)")]
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
    /// Ignored in embedded mode (no shmem surface exists).
    #[arg(long)]
    instance_dir: PathBuf,
    /// Persistent data directory (raft log + state + TLS). Per-node.
    #[arg(long)]
    data_dir: PathBuf,
    /// app_id; must match across all peers and the load client.
    #[arg(long, default_value = "uc-bench-3node")]
    app_id: String,
    /// IPC mode: `shmem` = node + separate service over rings (default,
    /// historical topology); `embedded` = KvSm in-process, no service, no
    /// shmem surface (co-location arm).
    #[arg(long, value_enum, default_value = "shmem")]
    ipc_mode: IpcModeArg,
    /// Co-locate a uc_service in-process (default true; shmem mode only).
    /// When false the node will hang in wait_for_service_ready — only disable
    /// if a service is attached out-of-band. Ignored in embedded mode.
    #[arg(long, default_value_t = true)]
    with_service: bool,

    // ---- embedded in-process load driver (node0 only; requires --ipc-mode
    // embedded). Presence of --load-rates turns the driver on. ----
    /// comma-separated target rates (msgs/s). Setting this makes THIS node run
    /// the loadcore sweep in-process after the cluster elects a leader.
    #[arg(long)]
    load_rates: Option<String>,
    /// in-flight concurrency values to sweep (with --load-rates)
    #[arg(long, default_value = "128")]
    load_inflight: String,
    /// KV value size in bytes (with --load-rates)
    #[arg(long, default_value_t = 64)]
    load_payload_bytes: usize,
    /// measurement window per step, seconds (with --load-rates)
    #[arg(long, default_value_t = 10.0)]
    load_window_secs: f64,
    /// warmup window per step, seconds (with --load-rates)
    #[arg(long, default_value_t = 2.0)]
    load_warmup_secs: f64,
    /// output CSV path (with --load-rates). Written as <out>.partial, renamed
    /// on completion.
    #[arg(long)]
    load_out: Option<PathBuf>,
    /// CSV config label (with --load-rates)
    #[arg(long, default_value = "3node_consistent_embedded")]
    load_config: String,
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

/// NodeConfig does NOT derive Default — build every field explicitly, the
/// same way m3_three_node_shmem.rs does.
fn build_node_cfg(args: &Args) -> anyhow::Result<NodeConfig> {
    let peers: Vec<PeerSeed> = args
        .peers
        .iter()
        .map(|s| parse_peer(s))
        .collect::<anyhow::Result<_>>()?;

    Ok(NodeConfig {
        node_id: args.node_id,
        data_dir: args.data_dir.clone(),
        raft_listen_addr: args.listen,
        app_id: args.app_id.clone(),
        bootstrap: BootstrapConfig::Peers { peers },
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
        ipc_mode: match args.ipc_mode {
            IpcModeArg::Shmem => IpcMode::Shmem {
                instance_dir: args.instance_dir.clone(),
            },
            IpcModeArg::Embedded => IpcMode::Embedded,
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
    })
}

/// Floor-decomposition probe (uc-bench-probes only): the leader records every
/// append_entries round-trip it observes (network/quic/instance.rs). Drain it
/// periodically into a cumulative accumulator and emit p50/p99/count so the
/// last line before `pkill -9` teardown carries the whole-run aggregate. Only
/// the leader produces samples; 1-node arms emit n=0 (no replication).
#[cfg(feature = "uc-bench-probes")]
fn spawn_repl_rpc_stats() {
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
}
#[cfg(not(feature = "uc-bench-probes"))]
fn spawn_repl_rpc_stats() {}

fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    let args = Args::parse();

    if args.load_rates.is_some() && args.ipc_mode != IpcModeArg::Embedded {
        anyhow::bail!(
            "--load-rates requires --ipc-mode embedded (in shmem mode use commit-path-load)"
        );
    }
    if args.load_rates.is_some() && args.load_out.is_none() {
        anyhow::bail!("--load-rates requires --load-out");
    }
    std::fs::create_dir_all(&args.instance_dir)?;
    std::fs::create_dir_all(&args.data_dir)?;

    match args.ipc_mode {
        IpcModeArg::Shmem => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(run_shmem(args)),
        IpcModeArg::Embedded => run_embedded(args),
    }
}

/// Shmem topology (historical): node + co-located service on ONE
/// current_thread runtime; load driven externally by commit-path-load.
async fn run_shmem(args: Args) -> anyhow::Result<()> {
    let node_cfg = build_node_cfg(&args)?;

    // Node + service rendezvous: spawn the node, wait for cnc.dat, then spawn
    // the service, then join both — in Shmem mode start() blocks in
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
        // Wire NoopOutput so the service drains `service/output.ring`. Without an
        // output handler, ServiceBuilder::run() DROPS the output.ring consumer, so
        // nothing drains it: the ring fills, output_dispatcher spin-burns a core
        // (~1 item/s), output_chan stays full (per-commit "output_chan full" warns),
        // and output_progress stalls near 0 — making leadership-transition output
        // replay scan the whole log (the read()+crc32 core saturation). NoopOutput
        // drains-and-discards, keeping the output path a no-op instead of a thrash.
        let svc_task = tokio::spawn(async move {
            ServiceBuilder::new(svc_cfg, KvSm::default())
                .output_handler(uc_service::NoopOutput)
                .run()
                .await
        });
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
        "uc-node-launch: node {} up, listening on {} (app_id={}, mode=shmem)",
        args.node_id, args.listen, args.app_id,
    );
    spawn_repl_rpc_stats();

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

/// Embedded topology (co-location arm): the node — RaftCore, storage, QUIC —
/// lives on a dedicated current_thread runtime on its own thread (same tokio
/// flavor as the shmem arm's node, so the A/B isolates IPC topology), and the
/// optional in-process load driver runs on a separate multi_thread runtime.
fn run_embedded(args: Args) -> anyhow::Result<()> {
    let node_cfg = build_node_cfg(&args)?;

    let (node_tx, node_rx) = std::sync::mpsc::channel::<anyhow::Result<Arc<NodeHandle<KvSm>>>>();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let node_thread = std::thread::Builder::new()
        .name("uc-node-rt".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = node_tx.send(Err(anyhow::anyhow!("node runtime: {e}")));
                    return;
                }
            };
            rt.block_on(async move {
                match NodeBuilder::new(node_cfg, KvSm::default()).start().await {
                    Ok(node) => {
                        let node = Arc::new(node);
                        let _ = node_tx.send(Ok(Arc::clone(&node)));
                        // Keep the node runtime alive (it drives RaftCore /
                        // journal / QUIC tasks) until the driver side says stop.
                        let _ = shutdown_rx.await;
                        match Arc::try_unwrap(node) {
                            Ok(node) => {
                                if let Err(e) = node.shutdown().await {
                                    eprintln!("uc-node-launch: shutdown error: {e}");
                                }
                            }
                            Err(_) => eprintln!(
                                "uc-node-launch: node handle still shared; exiting without \
                                 graceful shutdown"
                            ),
                        }
                    }
                    Err(e) => {
                        let _ = node_tx.send(Err(anyhow::anyhow!("node start: {e}")));
                    }
                }
            });
        })?;

    let node = node_rx
        .recv()
        .map_err(|_| anyhow::anyhow!("node runtime thread died before start completed"))??;

    eprintln!(
        "uc-node-launch: node {} up, listening on {} (app_id={}, mode=embedded)",
        args.node_id, args.listen, args.app_id,
    );

    // Driver runtime: the load sweep (if any) + Ctrl-C handling.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = rt.block_on(async {
        spawn_repl_rpc_stats();

        if let Some(rates) = &args.load_rates {
            let out = args.load_out.clone().expect("checked in main");
            // Wait for the cluster to elect a leader by probing with real
            // submits (embedded has no external client to do this).
            let probe = NodeSubmitter {
                node: Arc::clone(&node),
                timeout: Duration::from_secs(2),
            };
            let deadline = std::time::Instant::now() + Duration::from_secs(120);
            loop {
                match probe.submit(KvCmd::Put { key: 0, val: vec![] }).await {
                    Ok(()) => break,
                    Err(e) => {
                        if std::time::Instant::now() >= deadline {
                            anyhow::bail!("embedded load: no leader after 120s (last: {e})");
                        }
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
            eprintln!("uc-node-launch: embedded load starting (leader ready)");
            let partial = out.with_extension("csv.partial");
            let opts = SweepOpts {
                config: args.load_config.clone(),
                rates: parse_list(rates),
                inflights: parse_list(&args.load_inflight),
                payload_bytes: args.load_payload_bytes,
                window_secs: args.load_window_secs,
                warmup_secs: args.load_warmup_secs,
                out: partial.clone(),
                hgrm_dir: None,
            };
            let sub = NodeSubmitter {
                node: Arc::clone(&node),
                timeout: request_timeout_from_env(),
            };
            run_sweep(sub, &opts).await?;
            std::fs::rename(&partial, &out)?;
            eprintln!(
                "uc-node-launch: embedded load complete, wrote {}",
                out.display()
            );
        }

        tokio::signal::ctrl_c().await?;
        Ok(())
    });

    eprintln!("uc-node-launch: node {} shutting down", args.node_id);
    drop(node); // release our Arc so the node thread's try_unwrap succeeds
    let _ = shutdown_tx.send(());
    let _ = node_thread.join();
    result
}
