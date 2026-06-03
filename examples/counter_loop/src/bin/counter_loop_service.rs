//! `counter_loop_service` — single-node uc_node + uc_service + tonic
//! `IncrementNotifier` server, all in one process.
//!
//! The service-side OutputHandler pushes an `IncrementEvent` per committed
//! `Cmd::Inc` into a bounded mpsc held by the gRPC subscriber. Returns
//! `OutputError::Retryable` when no subscriber is attached, which lets the
//! M5 output dispatcher back off and retry rather than silently drop the
//! event. That at-least-once property is what keeps the client-side loop
//! from stalling on a lost gRPC poke.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use clap::Parser;
use counter_loop::proto::increment_notifier_server::{
    IncrementNotifier, IncrementNotifierServer,
};
use counter_loop::proto::{IncrementEvent, SubscribeReq};
use counter_loop::{Cmd, CounterSm, Query, QueryResp, build_state_machine};
use futures::Stream;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, transport::Server};
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, RaftTuning,
    ServiceRingConfig, TlsConfig,
};
use uc_service::runtime::ServiceConfig;
use uc_service::{OutputError, OutputHandler, ServiceBuilder, StateMachine};

const SUBSCRIBER_BUFFER: usize = 4096;

// Diagnostic counters for the 1000-counter hang investigation.
// Hypothesis: under high concurrency the bounded mpsc(SUBSCRIBER_BUFFER) fills,
// `tx.send().await` blocks, on_committed never returns, output_loop stalls,
// node-side output.ring fills, apply stalls, raft commits halt. Confirmed by
// SLOW_SENDS climbing in lockstep with submit-side timeouts.
static OC_CALLS: AtomicU64 = AtomicU64::new(0);
static OC_NO_SUB: AtomicU64 = AtomicU64::new(0);
static OC_CLOSED: AtomicU64 = AtomicU64::new(0);
static OC_SLOW_SENDS: AtomicU64 = AtomicU64::new(0);
static OC_OK: AtomicU64 = AtomicU64::new(0);
const STATS_EVERY: u64 = 1000;
const SLOW_SEND_THRESHOLD: Duration = Duration::from_millis(100);

#[derive(Parser, Debug)]
#[command(about = "counter_loop benchmark — service side (node + service + gRPC server)")]
struct Args {
    #[arg(long, default_value = "/tmp/ultima-counter-loop")]
    instance_dir: PathBuf,

    #[arg(long, default_value = "counter-loop")]
    app_id: String,

    /// gRPC listen address for IncrementNotifier.
    #[arg(long, default_value = "127.0.0.1:50051")]
    grpc_listen: std::net::SocketAddr,

    /// Persistent data directory for the node (raft log + state). A
    /// subdir of this is used for the service-side scratch dir.
    #[arg(long, default_value = "/tmp/ultima-counter-loop-data")]
    data_dir: PathBuf,
}

type SubscriberSlot = Arc<Mutex<Option<mpsc::Sender<Result<IncrementEvent, Status>>>>>;

struct CounterOutput {
    slot: SubscriberSlot,
}

fn maybe_log_stats(n: u64) {
    if n.is_multiple_of(STATS_EVERY) {
        tracing::info!(
            calls = n,
            ok = OC_OK.load(Ordering::Relaxed),
            no_sub = OC_NO_SUB.load(Ordering::Relaxed),
            closed = OC_CLOSED.load(Ordering::Relaxed),
            slow_sends = OC_SLOW_SENDS.load(Ordering::Relaxed),
            "on_committed stats"
        );
    }
}

#[async_trait]
impl OutputHandler<CounterSm> for CounterOutput {
    async fn on_committed(
        &self,
        log_index: u64,
        cmd: &Cmd,
        state: &CounterSm,
    ) -> Result<(), OutputError> {
        let n = OC_CALLS.fetch_add(1, Ordering::Relaxed) + 1;
        let Cmd::Inc { name } = cmd;
        let QueryResp { value } = state.query(Query::Get(name.clone()));
        let value = value
            .ok_or_else(|| OutputError::Permanent(format!("counter {name} missing after apply")))?;

        let tx = match self.slot.lock().clone() {
            Some(tx) => tx,
            None => {
                OC_NO_SUB.fetch_add(1, Ordering::Relaxed);
                maybe_log_stats(n);
                return Err(OutputError::Retryable("no subscriber attached".into()));
            }
        };

        let event = IncrementEvent {
            counter_name: name.clone(),
            value,
            log_index,
        };

        let send_start = Instant::now();
        let send_result = tx.send(Ok(event)).await;
        let send_elapsed = send_start.elapsed();
        if send_elapsed >= SLOW_SEND_THRESHOLD {
            OC_SLOW_SENDS.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                ?send_elapsed,
                log_index,
                "tx.send slow — mpsc backpressure"
            );
        }
        maybe_log_stats(n);

        match send_result {
            Ok(()) => {
                OC_OK.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(_closed) => {
                OC_CLOSED.fetch_add(1, Ordering::Relaxed);
                *self.slot.lock() = None;
                Err(OutputError::Retryable("subscriber dropped".into()))
            }
        }
    }
}

struct GrpcServer {
    slot: SubscriberSlot,
}

#[async_trait]
impl IncrementNotifier for GrpcServer {
    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<IncrementEvent, Status>> + Send + 'static>>;

    async fn subscribe(
        &self,
        _req: Request<SubscribeReq>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(SUBSCRIBER_BUFFER);
        // Replace any prior subscriber; one bench client at a time.
        *self.slot.lock() = Some(tx);
        tracing::info!("gRPC subscriber attached");
        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::SubscribeStream))
    }
}

async fn wait_for_path(p: &std::path::Path, timeout: Duration) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    while !p.exists() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for {}", p.display());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

// current_thread runtime: matches the runtime flavor used by every in-process
// node+service test (`m3_*`, `m5_*`) in this repo. apply_loop lives on its
// own std::thread so it doesn't compete with the tokio scheduler.
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    std::fs::create_dir_all(&args.instance_dir)?;
    std::fs::create_dir_all(&args.data_dir)?;
    let node_data = args.data_dir.join("node");
    let service_data = args.data_dir.join("service");
    std::fs::create_dir_all(&node_data)?;
    std::fs::create_dir_all(&service_data)?;

    // ── Node ────────────────────────────────────────────────────────────
    let node_cfg = NodeConfig {
        node_id: 1,
        data_dir: node_data,
        raft_listen_addr: "127.0.0.1:0".parse().unwrap(),
        app_id: args.app_id.clone(),
        bootstrap: BootstrapConfig::SingleNode,
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        ipc_mode: IpcMode::Shmem {
            instance_dir: args.instance_dir.clone(),
        },
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
    };
    // Node + service start concurrently: in shmem mode `NodeBuilder::start`
    // blocks waiting for the service handshake, and `ServiceBuilder::run`
    // blocks waiting for the node to publish cnc.dat. Spawning both lets
    // them rendezvous. Do NOT add other long-lived async work between
    // the two spawns — the m5_output_smoke pattern is precisely this
    // (node spawn -> wait_for_path -> service spawn -> join), and
    // interleaving e.g. a gRPC `serve().await` task can starve the
    // service task on a multi-thread runtime intermittently.
    let instance_dir = args.instance_dir.clone();
    let node_task = tokio::spawn(async move {
        NodeBuilder::new(node_cfg, build_state_machine())
            .start()
            .await
    });
    wait_for_path(&instance_dir.join("cnc.dat"), Duration::from_secs(30)).await?;

    let slot: SubscriberSlot = Arc::new(Mutex::new(None));
    let svc_cfg = ServiceConfig {
        instance_dir: args.instance_dir.clone(),
        app_id: args.app_id.clone(),
        data_dir: service_data,
        ..ServiceConfig::default()
    };
    let slot_for_handler = slot.clone();
    let svc_task = tokio::spawn(async move {
        ServiceBuilder::new(svc_cfg, build_state_machine())
            .output_handler(CounterOutput {
                slot: slot_for_handler,
            })
            .run()
            .await
    });

    let (node_res, svc_res) = tokio::join!(node_task, svc_task);
    let node = node_res
        .map_err(|e| anyhow::anyhow!("node task panic: {e}"))?
        .map_err(|e| anyhow::anyhow!("node start: {e}"))?;
    let service = svc_res
        .map_err(|e| anyhow::anyhow!("service task panic: {e}"))?
        .map_err(|e| anyhow::anyhow!("service start: {e}"))?;

    // ── gRPC server (started AFTER handshake) ───────────────────────────
    let grpc = GrpcServer { slot };
    let grpc_addr = args.grpc_listen;
    let grpc_task = tokio::spawn(async move {
        tracing::info!(%grpc_addr, "starting IncrementNotifier gRPC server");
        Server::builder()
            .add_service(IncrementNotifierServer::new(grpc))
            .serve(grpc_addr)
            .await
    });

    // ── Wait for leadership ─────────────────────────────────────────────
    for _ in 0..50 {
        if node.current_leader().await == Some(1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if node.current_leader().await != Some(1) {
        anyhow::bail!("node never became leader");
    }
    tracing::info!("counter_loop_service ready: instance_dir={}", instance_dir.display());

    // ── Run until Ctrl-C ────────────────────────────────────────────────
    tokio::select! {
        _ = tokio::signal::ctrl_c() => tracing::info!("Ctrl-C received, shutting down"),
        r = grpc_task => {
            tracing::error!(?r, "gRPC server exited unexpectedly");
        }
    }

    service.shutdown().await.ok();
    node.shutdown().await.ok();
    Ok(())
}
