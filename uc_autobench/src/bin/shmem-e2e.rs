//! shmem-e2e — the end-to-end Goodhart gate.
//!
//! Spawns an in-process single-node cluster + 4 clients (via
//! `uc_node::test_support::ClusterFixture`), drives a fixed number of
//! submit→response round-trips across the clients (2k by default; see
//! [`DEFAULT_REQS_PER_CLIENT`]), and emits ONE JSON line on stdout with the
//! three metrics the `[e2e_gate]` in `tasks/shmem/task.toml` consumes:
//!
//!   - `submit_to_resp_p50_ns`
//!   - `submit_to_resp_p99_ns`  (the e2e primary)
//!   - `submit_to_resp_throughput`
//!
//! All diagnostics go to stderr. The runtime MUST be `current_thread`: per
//! project memory (`feedback_m3_test_runtime_flavor`), a `multi_thread` runtime
//! intermittently times out the shmem handshake during fixture bring-up.

use std::io::{Read, Write};
use std::time::Instant;

use uc_node::test_support::ClusterFixture;
use uc_service::{SnapshotError, StateMachine};

const N_CLIENTS: usize = 4;
/// Default per-client request count → 4 × 500 = 2k total round-trips.
///
/// The single-node end-to-end path is dominated by the journal group-commit
/// window (~38ms per committed entry under low concurrency), giving an
/// aggregate of ~100 round-trips/s across the 4 clients. The spec nominally
/// calls for 100k, but at this rate that would take ~15 min — far past the
/// gate's runtime budget.
///
/// This binary is the orchestrator's per-promotion Goodhart gate: it runs on
/// every variant that beats the microbench best, so it must be cheap enough to
/// run many times in a loop. Because the metric is Raft-commit-dominated it
/// won't sensitively detect shmem ring changes anyway — its real job is to
/// catch a variant that *catastrophically* breaks throughput/correctness. So a
/// small, fast sample is appropriate. 2k gives stable percentiles
/// (p50≈36ms, p99≈41ms — they reflect the per-commit latency floor, not sample
/// count) and completes in ~20-40s. Override with `SHMEM_E2E_REQS_PER_CLIENT`
/// for the full 100k (`25000`).
const DEFAULT_REQS_PER_CLIENT: u64 = 500;

fn reqs_per_client() -> u64 {
    std::env::var("SHMEM_E2E_REQS_PER_CLIENT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_REQS_PER_CLIENT)
}

/// Minimal deterministic state machine for the bench. The command is a byte
/// payload (`Vec<u8>`, which is `Serialize`/`DeserializeOwned`, so it rides the
/// generic `Client::submit` path as a real wire payload); apply folds the
/// payload bytes into a running counter and returns the current value. This is
/// the simplest SM that lets a client submit a byte payload and get a response
/// back, mirroring the shape of the M4 `Counter` test SM.
#[derive(Default)]
struct Echo {
    counter: u64,
    last_applied: Option<u64>,
}

impl StateMachine for Echo {
    type Command = Vec<u8>;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, log_index: u64, cmd: Vec<u8>) -> u64 {
        // Deterministic: fold the payload length into a counter. No clocks, no
        // I/O, no randomness.
        self.counter = self.counter.wrapping_add(cmd.len() as u64);
        self.last_applied = Some(log_index);
        self.counter
    }

    fn query(&self, _: ()) -> u64 {
        self.counter
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }

    fn build_snapshot(&self, _: &mut dyn Write) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }

    fn install_snapshot(&mut self, _: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
}

fn percentile(xs: &[u64], p: f64) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    // `xs` is assumed already sorted by the caller.
    let idx = ((xs.len() as f64) * p).clamp(0.0, (xs.len() - 1) as f64) as usize;
    xs[idx] as f64
}

/// Drive `n` round-trips on a single borrowed client, recording per-request
/// latency in nanoseconds.
async fn drive_client(
    client: &uc_client::Client,
    client_id: usize,
    n: u64,
) -> anyhow::Result<Vec<u64>> {
    let mut latencies = Vec::with_capacity(n as usize);
    for i in 0..n {
        let payload = format!("c{client_id}-r{i}").into_bytes();
        let t0 = Instant::now();
        let _resp: u64 = client.submit(&payload).await?;
        latencies.push(t0.elapsed().as_nanos() as u64);
    }
    Ok(latencies)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let per_client = reqs_per_client();
    eprintln!(
        "shmem-e2e: {} total round-trips ({N_CLIENTS} clients × {per_client} reqs); \
         e2e latency is Raft-commit-dominated (~38ms/op floor), so this metric gates \
         catastrophic regressions, not fine-grained shmem ring perf",
        N_CLIENTS as u64 * per_client
    );

    let fixture = ClusterFixture::<Echo>::single_node(N_CLIENTS).await?;

    // Drive all 4 clients concurrently. `Client::submit` takes `&self`, so we
    // borrow each client and run the four loops cooperatively under the
    // current_thread runtime via `tokio::join!`. This avoids `tokio::spawn`
    // (which would need `'static` / clonable handles — `Client` is neither) and
    // gives a realistic "4 clients hammering at once" interleaving.
    let start = Instant::now();
    let (r0, r1, r2, r3) = tokio::join!(
        drive_client(fixture.client(0), 0, per_client),
        drive_client(fixture.client(1), 1, per_client),
        drive_client(fixture.client(2), 2, per_client),
        drive_client(fixture.client(3), 3, per_client),
    );
    let elapsed = start.elapsed();

    let mut all = Vec::with_capacity((N_CLIENTS as u64 * per_client) as usize);
    all.extend(r0?);
    all.extend(r1?);
    all.extend(r2?);
    all.extend(r3?);
    all.sort_unstable();

    let total = all.len() as u64;
    let p50 = percentile(&all, 0.50);
    let p99 = percentile(&all, 0.99);
    let throughput = (total as f64) / elapsed.as_secs_f64();

    eprintln!(
        "shmem-e2e: {total} round-trips in {:.3}s (p50={p50}ns p99={p99}ns thr={throughput:.0}/s)",
        elapsed.as_secs_f64()
    );

    let out = serde_json::json!({
        "submit_to_resp_p50_ns": p50,
        "submit_to_resp_p99_ns": p99,
        "submit_to_resp_throughput": throughput,
    });
    println!("{out}");

    // Clean, ordered teardown so no `/dev/shm/ultima-*` files leak.
    fixture.shutdown().await?;
    Ok(())
}
