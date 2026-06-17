//! internode-rpc-bench — transport-isolated QUIC-vs-UDP RPC A/B.
//!
//! Drives a uniform echo-RPC against `uc_node`'s `network::bench_support` shim
//! (UDP `UdpMux` or QUIC `quinn`), with NO consensus, NO journal, NO openraft
//! — just the pure transport round-trip. Open-loop and coordinated-omission-
//! free: the same `run_step`/`next_send` shape as `commit-path-load.rs`
//! (advance `next_send` by 1/rate regardless of actual dispatch; latency is
//! measured from each request's INTENDED send time).
//!
//! Emits one CSV row to stdout with the exact 13-column task13 schema, with
//! `system = "udp-rpc" | "quic-rpc"` and `workload = "rpc-echo"`.

use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use hdrhistogram::Histogram;
use uc_node::network::bench_support::{EchoClient, quic_echo_pair, udp_echo_pair};

const CSV_HEADER: &str = "system,config,workload,payload_bytes,inflight,target_rate,achieved_rate,\
p50_ns,p99_ns,p99_9_ns,p99_99_ns,max_ns,count";

#[derive(Parser)]
#[command(about = "Transport-isolated QUIC-vs-UDP RPC echo A/B (open-loop, CO-free)")]
struct Args {
    /// transport under test
    #[arg(long, value_parser = ["quic", "udp"])]
    transport: String,
    /// echo payload size in bytes
    #[arg(long, default_value_t = 64)]
    payload: usize,
    /// in-flight concurrency cap
    #[arg(long, default_value_t = 1)]
    inflight: usize,
    /// open-loop target rate (RPCs/s)
    #[arg(long, default_value_t = 20000.0)]
    rate: f64,
    /// measurement window (seconds)
    #[arg(long, default_value_t = 5.0)]
    duration: f64,
    /// config label written into the CSV
    #[arg(long, default_value = "loopback")]
    config: String,
}

/// Open-loop, coordinated-omission-free echo-RPC step. Mirrors
/// `commit-path-load.rs::run_step`: `next_send` advances by `period` regardless
/// of when a send actually dispatches, and latency is measured from the
/// INTENDED send time so a stalled transport can't hide tail latency. Returns
/// the histogram (ns) and the achieved rate (completed / wall-seconds).
async fn run_step(
    client: &EchoClient,
    target_rate: f64,
    inflight: usize,
    duration: Duration,
    payload: usize,
) -> anyhow::Result<(Histogram<u64>, f64)> {
    // 1ns..600s range, 3 sig figs (matches commit-path-load precision).
    let mut hist = Histogram::<u64>::new_with_bounds(1, 600_000_000_000, 3)?;
    let period = Duration::from_secs_f64(1.0 / target_rate);
    let start = Instant::now();
    let deadline = start + duration;

    let mut inflight_set = FuturesUnordered::new();
    let mut next_send = start;
    let mut completed: u64 = 0;
    let body = Bytes::from(vec![0u8; payload]);

    loop {
        let now = Instant::now();
        if now >= deadline && inflight_set.is_empty() {
            break;
        }

        // Launch all sends whose intended time has arrived, up to the cap.
        while now >= next_send && inflight_set.len() < inflight && next_send < deadline {
            let intended = next_send;
            let body = body.clone();
            next_send += period;
            inflight_set.push(async move {
                client.rpc(body).await?;
                Ok::<_, anyhow::Error>(intended.elapsed().as_nanos() as u64)
            });
        }

        tokio::select! {
            Some(res) = inflight_set.next(), if !inflight_set.is_empty() => {
                hist.record(res?.min(600_000_000_000))?;
                completed += 1;
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_send)),
                if now < deadline && inflight_set.len() < inflight => {}
            else => { break; }
        }
    }

    let achieved = completed as f64 / start.elapsed().as_secs_f64();
    Ok((hist, achieved))
}

#[allow(clippy::too_many_arguments)]
fn csv_row(
    system: &str,
    config: &str,
    payload: usize,
    inflight: usize,
    target_rate: f64,
    achieved_rate: f64,
    hist: &Histogram<u64>,
) -> String {
    format!(
        "{},{},rpc-echo,{},{},{:.0},{:.1},{},{},{},{},{},{}",
        system,
        config,
        payload,
        inflight,
        target_rate,
        achieved_rate,
        hist.value_at_quantile(0.50),
        hist.value_at_quantile(0.99),
        hist.value_at_quantile(0.999),
        hist.value_at_quantile(0.9999),
        hist.max(),
        hist.len(),
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    let args = Args::parse();

    let (system, client, server) = match args.transport.as_str() {
        "udp" => {
            let (c, s) = udp_echo_pair().await?;
            ("udp-rpc", c, s)
        }
        "quic" => {
            let (c, s) = quic_echo_pair().await?;
            ("quic-rpc", c, s)
        }
        other => anyhow::bail!("unknown transport {other}"),
    };

    eprintln!(
        "internode-rpc-bench: transport={} payload={}B inflight={} rate={} duration={}s",
        args.transport, args.payload, args.inflight, args.rate, args.duration
    );

    // Warmup (discarded): connection setup, TLS handshake, page-ins.
    let _ = run_step(
        &client,
        args.rate,
        args.inflight,
        Duration::from_secs_f64(1.0),
        args.payload,
    )
    .await?;

    // Measured window.
    let (hist, achieved) = run_step(
        &client,
        args.rate,
        args.inflight,
        Duration::from_secs_f64(args.duration),
        args.payload,
    )
    .await?;

    let row = csv_row(
        system,
        &args.config,
        args.payload,
        args.inflight,
        args.rate,
        achieved,
        &hist,
    );
    println!("{CSV_HEADER}");
    println!("{row}");

    server.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_row_has_13_columns_and_correct_prefix() {
        let mut hist = Histogram::<u64>::new(3).unwrap();
        for v in [100u64, 200, 300, 400, 500] {
            hist.record(v).unwrap();
        }
        let row = csv_row("udp-rpc", "loopback", 64, 8, 5000.0, 4987.6, &hist);
        assert_eq!(row.split(',').count(), 13);
        assert!(row.starts_with("udp-rpc,loopback,rpc-echo,64,8,5000,4987.6,"));
        assert_eq!(CSV_HEADER.split(',').count(), 13);
    }
}
