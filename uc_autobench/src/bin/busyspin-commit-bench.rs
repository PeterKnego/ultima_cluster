//! busyspin-commit-bench — single-node M1 commit-latency A/B at inflight=1.
//!
//! Measures the unloaded commit floor (submit -> response p50) of a single-node
//! embedded (M1) node, driven one request at a time. This isolates the **base
//! commit->apply handoff** — the `1node_eventual` arm of the floor decomposition
//! (`docs/benchmarks/floor-decomposition-2026-06-25.md`): no replication, no
//! fsync (eventual durability). It is the part the busy-spin runtime can move
//! *locally*; replication + fsync need the fleet.
//!
//! THE RUNTIME FLAVOR IS THE WHOLE POINT. The futex cost the busy-spin engine
//! targets only appears when openraft's internal tasks scatter across worker
//! threads, which a **multi_thread** runtime does and `current_thread` does not.
//! So this runs on `multi_thread` (unlike `commit-path-load`, which is
//! `current_thread` and therefore cannot show the lever at all).
//!
//! Build both ways and compare the printed `p50`:
//!   cargo run -p uc_autobench --release --bin busyspin-commit-bench
//!   cargo run -p uc_autobench --release --features busyspin --bin busyspin-commit-bench
//!
//! Env: UC_BENCH_ITERS (default 20000), UC_BENCH_WARMUP (default 3000).

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use uc_node::{
    BootstrapConfig, ClientRingConfig, NodeBuilder, NodeConfig, RaftTuning, ServiceRingConfig,
    TlsConfig, Transport,
};
use uc_service::{SnapshotError, StateMachine};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum CounterCmd {
    Increment(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CounterResponse {
    value: u64,
}

#[derive(Default)]
struct Counter {
    value: u64,
    last_applied: Option<u64>,
}

impl StateMachine for Counter {
    type Command = CounterCmd;
    type Response = CounterResponse;
    type Query = ();
    type QueryResponse = u64;
    type SnapshotHandle = Vec<u8>;

    fn apply(&mut self, log_index: u64, cmd: Self::Command) -> Self::Response {
        let CounterCmd::Increment(n) = cmd;
        self.value += n;
        self.last_applied = Some(log_index);
        CounterResponse { value: self.value }
    }

    fn query(&self, _: Self::Query) -> Self::QueryResponse {
        self.value
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }

    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        // Trivial manual encoding; snapshots are not exercised in this bench.
        let la = self.last_applied.unwrap_or(0);
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.value.to_le_bytes());
        buf.extend_from_slice(&la.to_le_bytes());
        Ok((buf, la))
    }

    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }

    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<u64, SnapshotError> {
        let mut buf = Vec::new();
        src.read_to_end(&mut buf)?;
        let value = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let la = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        self.value = value;
        self.last_applied = (la != 0).then_some(la);
        Ok(la)
    }
}

fn cfg(data_dir: PathBuf) -> NodeConfig {
    NodeConfig {
        node_id: 1,
        data_dir,
        raft_listen_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        app_id: "busyspin-commit-bench".into(),
        bootstrap: BootstrapConfig::SingleNode,
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        transport: Transport::Quic,
        ipc_mode: uc_node::IpcMode::default(),
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
        // Eventual durability => no fsync on the commit path (the `*_eventual`
        // arm of the floor decomposition).
        log_durability: ultima_journal::Durability::Eventual,
    }
}

async fn wait_for_leader(node: &uc_node::NodeHandle<Counter>) {
    for _ in 0..100 {
        if node.current_leader().await == Some(1) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("never became leader");
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let runtime_name = if cfg!(feature = "busyspin") { "busyspin" } else { "tokio" };
    let warmup = env_usize("UC_BENCH_WARMUP", 3000);
    let iters = env_usize("UC_BENCH_ITERS", 20000);

    let dir = TempDir::new()?;
    let node = NodeBuilder::new(cfg(dir.path().to_owned()), Counter::default())
        .start()
        .await?;
    wait_for_leader(&node).await;

    // Warm up (JIT page faults, journal segment prealloc, leader steady state).
    for _ in 0..warmup {
        node.submit(CounterCmd::Increment(1)).await?;
    }

    // Closed-loop inflight=1: one outstanding submit at a time = the unloaded floor.
    let mut samples: Vec<f64> = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        node.submit(CounterCmd::Increment(1)).await?;
        samples.push(t0.elapsed().as_nanos() as f64);
    }
    node.shutdown().await?;

    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |q: f64| -> f64 {
        let idx = ((q / 100.0) * (samples.len() as f64 - 1.0)).round() as usize;
        samples[idx]
    };
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let us = |ns: f64| ns / 1000.0;

    eprintln!(
        "runtime={runtime_name}  iters={iters}  inflight=1  (single-node M1, eventual durability, multi_thread)"
    );
    eprintln!(
        "  p50={:.1}us  p90={:.1}us  p99={:.1}us  min={:.1}us  mean={:.1}us",
        us(pct(50.0)),
        us(pct(90.0)),
        us(pct(99.0)),
        us(samples[0]),
        us(mean),
    );
    // Machine-readable line for capture/diffing across the two builds.
    println!(
        "CSV,{runtime_name},{},{:.0},{:.0},{:.0},{:.0},{:.0}",
        iters,
        pct(50.0),
        pct(90.0),
        pct(99.0),
        samples[0],
        mean,
    );
    Ok(())
}
