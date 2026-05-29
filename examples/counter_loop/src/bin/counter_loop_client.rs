//! `counter_loop_client` — benchmark driver.
//!
//! Generates N random counter names, fires one `Cmd::Inc` per counter to
//! seed the loop, then for each `IncrementEvent` received on the gRPC
//! stream submits the next `Cmd::Inc` for that counter — until every
//! counter has received `per_counter` events. The (t_send, t_recv) gap is
//! sampled into an hdrhistogram and printed at the end.
//!
//! Per-counter FIFO ordering is guaranteed by Raft (per-counter writes are
//! totally ordered via the log, `on_committed` fires in log-index order,
//! the gRPC stream is FIFO). The histogram pairing assumes that ordering.
//!
//! At-least-once delivery means a single event can in principle be
//! delivered twice (e.g., leader transition after `tx.send` succeeded but
//! before the output_progress marker advanced); the client dedups by
//! `log_index` so a duplicate doesn't double-pop the per-counter
//! `send_times` queue and corrupt the histogram.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use counter_loop::proto::SubscribeReq;
use counter_loop::proto::increment_notifier_client::IncrementNotifierClient;
use counter_loop::{Cmd, Resp};
use hdrhistogram::Histogram;
use rand::Rng;
use rand::distr::Alphanumeric;
use tokio_stream::StreamExt;
use uc_client::Client;

#[derive(Parser, Debug)]
#[command(about = "counter_loop benchmark — client / bench driver")]
struct Args {
    #[arg(long, default_value = "/tmp/ultima-counter-loop")]
    instance_dir: PathBuf,

    #[arg(long, default_value = "counter-loop")]
    app_id: String,

    #[arg(long, default_value = "http://127.0.0.1:50051")]
    grpc_target: String,

    /// Number of distinct counters to drive in parallel. Sets the
    /// in-flight pipeline depth: 1 = single-shot round-trip, 100 = loaded.
    #[arg(long, default_value_t = 100)]
    counters: u64,

    /// Target number of increments observed per counter before stopping.
    #[arg(long, default_value_t = 1000)]
    per_counter: u64,

    /// Histogram ceiling in milliseconds.
    #[arg(long, default_value_t = 60_000u64)]
    hist_max_ms: u64,
}

#[derive(Default)]
struct CounterState {
    sent: u64,
    received: u64,
    send_times: VecDeque<Instant>,
    last_seen_log_index: u64,
}

fn random_name(rng: &mut impl Rng) -> String {
    (0..12)
        .map(|_| rng.sample(Alphanumeric) as char)
        .collect()
}

/// Fire one increment for `name`: stamp `t_send`, bump `sent`, spawn the
/// (fire-and-forget) shmem submit. We don't await the submit response —
/// the round-trip we measure is submit → gRPC event, not submit → shmem
/// response. The submit's oneshot still resolves in the background; we
/// just ignore it.
fn fire_send(client: &Arc<Client>, state: &mut CounterState, name: &str) {
    state.send_times.push_back(Instant::now());
    state.sent += 1;
    let client = Arc::clone(client);
    let name = name.to_string();
    tokio::spawn(async move {
        match client.submit::<Cmd, Resp>(&Cmd::Inc { name: name.clone() }).await {
            Ok(_) => {}
            Err(e) => tracing::error!(%name, ?e, "submit failed"),
        }
    });
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // ── Generate counter names ──────────────────────────────────────────
    let mut rng = rand::rng();
    let names: Vec<String> = (0..args.counters).map(|_| random_name(&mut rng)).collect();
    let mut counters: HashMap<String, CounterState> = names
        .iter()
        .map(|n| (n.clone(), CounterState::default()))
        .collect();

    // ── Connect uc_client (shmem) ───────────────────────────────────────
    let client = Arc::new(
        Client::connect(&args.instance_dir, &args.app_id)
            .await
            .context("Client::connect")?,
    );
    tracing::info!(client_id = client.client_id(), "connected via shmem");

    // ── Connect + subscribe to gRPC BEFORE firing first send ────────────
    // If we sent first and the subscriber wasn't attached yet, the
    // service-side OutputHandler would `Retryable` for a bit but
    // eventually catch up — still correct, just wastes a few ms.
    let mut grpc =
        IncrementNotifierClient::connect(args.grpc_target.clone())
            .await
            .with_context(|| format!("gRPC connect {}", args.grpc_target))?;
    let mut stream = grpc.subscribe(SubscribeReq {}).await?.into_inner();
    tracing::info!("gRPC subscribed");

    // ── Histogram ───────────────────────────────────────────────────────
    let mut histogram: Histogram<u64> =
        Histogram::new_with_max(args.hist_max_ms * 1_000_000, 3).expect("histogram");

    // ── Fire initial sends + start the clock ────────────────────────────
    let t0 = Instant::now();
    for name in &names {
        let st = counters
            .get_mut(name)
            .expect("counter map seeded with all names");
        fire_send(&client, st, name);
    }

    // ── Event loop ──────────────────────────────────────────────────────
    let mut completed: u64 = 0;
    while completed < args.counters {
        let event = match stream.next().await {
            Some(Ok(e)) => e,
            Some(Err(s)) => return Err(anyhow::anyhow!("gRPC stream error: {s}")),
            None => return Err(anyhow::anyhow!("gRPC stream closed before completion")),
        };

        let st = match counters.get_mut(&event.counter_name) {
            Some(s) => s,
            None => {
                tracing::warn!(name = %event.counter_name, "event for unknown counter, dropping");
                continue;
            }
        };

        // Dedup by log_index (at-least-once contract).
        if event.log_index <= st.last_seen_log_index {
            tracing::debug!(
                name = %event.counter_name,
                log_index = event.log_index,
                "duplicate event, dropping"
            );
            continue;
        }
        st.last_seen_log_index = event.log_index;

        let t_send = st
            .send_times
            .pop_front()
            .expect("orphan event — fewer sends than receives for this counter");
        let lat_ns = (Instant::now() - t_send).as_nanos() as u64;
        // Saturate at histogram ceiling.
        histogram
            .record(lat_ns.min(args.hist_max_ms * 1_000_000))
            .expect("record");

        st.received += 1;

        if st.received < args.per_counter {
            // Re-borrow via a fresh lookup so we can pass &mut without
            // overlap on the existing `st` borrow lifetimes.
            let name = event.counter_name.clone();
            let st = counters.get_mut(&name).unwrap();
            fire_send(&client, st, &name);
        } else if st.received == args.per_counter {
            completed += 1;
        }
    }

    let elapsed = t0.elapsed();

    // ── Report ──────────────────────────────────────────────────────────
    let total_inc = args.counters * args.per_counter;
    let throughput = total_inc as f64 / elapsed.as_secs_f64();
    let pct = |q: f64| Duration::from_nanos(histogram.value_at_quantile(q));
    println!();
    println!("================ counter_loop bench ================");
    println!("counters            : {}", args.counters);
    println!("per_counter         : {}", args.per_counter);
    println!("total_increments    : {total_inc}");
    println!("wall_time           : {:?}", elapsed);
    println!("throughput          : {throughput:.1} inc/s");
    println!("pipeline_depth      : {}   (= --counters)", args.counters);
    println!("----- per-increment latency (submit -> gRPC event) -----");
    println!("samples             : {}", histogram.len());
    println!("mean                : {:?}", Duration::from_nanos(histogram.mean() as u64));
    println!("p50                 : {:?}", pct(0.50));
    println!("p90                 : {:?}", pct(0.90));
    println!("p99                 : {:?}", pct(0.99));
    println!("p999                : {:?}", pct(0.999));
    println!("max                 : {:?}", Duration::from_nanos(histogram.max()));
    println!("====================================================");
    println!(
        "(note: latency is measured under --counters-way concurrent load;\n \
         use --counters 1 for single-shot round-trip latency)"
    );

    // Don't explicit-shutdown the client: it's Arc-shared with the spawned
    // submit tasks. Process exit drops everything.
    drop(client);
    Ok(())
}
