//! internode-rpc-bench — transport-isolated QUIC-vs-UDP RPC A/B.
//!
//! Drives a uniform echo-RPC against `uc_node`'s `network::bench_support` shim
//! (UDP `UdpMux` or QUIC `quinn`), with NO consensus, NO journal, NO openraft
//! — just the pure transport round-trip. Open-loop and coordinated-omission-
//! free: the same `run_step`/`next_send` shape as `commit-path-load.rs`
//! (advance `next_send` by 1/rate regardless of actual dispatch; latency is
//! measured from each request's INTENDED send time).
//!
//! Emits one CSV row to stdout with the exact 13-column task13 schema. In
//! `ladder` mode: `system = "udp-rpc" | "quic-rpc"`, `workload = "rpc-echo"`.
//! In `ping` mode (sequential single-inflight RTT probe):
//! `system = "udp-ping" | "quic-ping"`, `workload = "rpc-ping"`.
//!
//! Split-role: `--role server` is a persistent responder (runs until SIGINT,
//! prints status to STDERR, nothing to STDOUT); `--role client` drives the
//! experiment against a remote `--connect` addr and emits CSV to STDOUT;
//! `--role both` (default) runs both endpoints in-process over loopback
//! (today's behavior). STDOUT carries CSV, STDERR carries logs/status so a
//! cross-host harness can capture the client's CSV cleanly.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use hdrhistogram::Histogram;
use uc_node::network::bench_support::{
    EchoClient, quic_echo_client, quic_echo_pair, quic_echo_server, udp_echo_client, udp_echo_pair,
    udp_echo_server,
};

const CSV_HEADER: &str = "system,config,workload,payload_bytes,inflight,target_rate,achieved_rate,\
p50_ns,p99_ns,p99_9_ns,p99_99_ns,max_ns,count";

#[derive(Parser)]
#[command(about = "Transport-isolated QUIC-vs-UDP RPC echo A/B (open-loop, CO-free)")]
struct Args {
    /// transport under test
    #[arg(long, value_parser = ["quic", "udp"])]
    transport: String,
    /// process role: server (persistent responder), client (driver), or both (in-process loopback)
    #[arg(long, value_parser = ["server", "client", "both"], default_value = "both")]
    role: String,
    /// experiment mode: ladder (open-loop rate driver), ping (sequential single-inflight RTT probe),
    /// or fanout (one leader → N followers, K-of-N quorum latency; Raft-replication-style)
    #[arg(long, value_parser = ["ladder", "ping", "fanout"], default_value = "ladder")]
    mode: String,
    /// listen address (role=server)
    #[arg(long, default_value = "0.0.0.0:9000")]
    listen: SocketAddr,
    /// server address(es) to connect to (role=client; required). Comma-separated list of
    /// SocketAddr — ping/ladder use the first; fanout requires ≥2 targets (the N followers).
    #[arg(long, value_delimiter = ',')]
    connect: Vec<SocketAddr>,
    /// fanout quorum K (K-of-N acks to commit a round). 0 = AUTO: Raft majority of an
    /// (N+1)-node cluster minus the leader's own vote = floor((N+1)/2). Validated 1 ≤ K ≤ N.
    #[arg(long, default_value_t = 0)]
    quorum: usize,
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

/// Sequential single-inflight RTT probe ("ping"). Forces inflight=1: strictly
/// one request → response at a time, as fast as possible, for `duration`,
/// recording each RTT into the histogram. This is the canonical transport ping
/// — no open-loop pacing, so it measures raw serial round-trip latency.
///
/// Loss-tolerant: each RPC is wrapped in a 1s timeout. On error or timeout
/// the failure is counted and the loop continues — the run is NOT aborted.
/// The caller always gets a histogram (possibly empty) + failure tally.
///
/// Returns `(histogram_ns, achieved_rate, failed_count)`.
async fn run_ping(
    client: &EchoClient,
    duration: Duration,
    payload: usize,
) -> anyhow::Result<(Histogram<u64>, f64, u64)> {
    let mut hist = Histogram::<u64>::new_with_bounds(1, 600_000_000_000, 3)?;
    let start = Instant::now();
    let deadline = start + duration;
    let body = Bytes::from(vec![0u8; payload]);
    let mut completed: u64 = 0;
    let mut failed: u64 = 0;
    // Per-RPC timeout: keeps the sequential loop bounded under packet loss or
    // a wedged transport that never returns an error.
    let rpc_timeout = Duration::from_secs(1);

    while Instant::now() < deadline {
        let t0 = Instant::now();
        match tokio::time::timeout(rpc_timeout, client.rpc(body.clone())).await {
            Ok(Ok(_)) => {
                hist.record((t0.elapsed().as_nanos() as u64).min(600_000_000_000))?;
                completed += 1;
            }
            Ok(Err(_)) | Err(_) => {
                // RPC error or per-RPC timeout: count the failure, keep going.
                failed += 1;
            }
        }
    }

    let achieved = completed as f64 / start.elapsed().as_secs_f64();
    Ok((hist, achieved, failed))
}

/// Per-target ok/fail tally for the fanout summary line.
#[derive(Clone, Copy, Default)]
struct TargetStats {
    ok: u64,
    fail: u64,
}

/// Outcome of a fanout run: quorum-latency histogram (PRIMARY), all-acks
/// histogram (SECONDARY), and round/per-target accounting for the stderr summary.
struct FanoutResult {
    quorum_hist: Histogram<u64>,
    all_acks_hist: Histogram<u64>,
    rounds: u64,
    quorum_miss: u64,
    per_target: Vec<TargetStats>,
    achieved: f64,
}

/// Fan-out (Raft-replication-style) round loop. One "leader" client fans the
/// same payload out to N "follower" `EchoClient`s CONCURRENTLY each round and
/// measures the time to reach QUORUM (K-of-N acks) — modeling a Raft leader
/// replicating a log entry and committing on majority ack.
///
/// Rounds are SEQUENTIAL (single round in flight, Raft-faithful: the leader
/// sends one entry, waits for commit, then sends the next). Within a round:
///   - capture `t0`, fire N concurrent RPCs (each wrapped in a 1s per-RPC
///     timeout, like `run_ping`),
///   - drain completions via `FuturesUnordered`; on the K-th SUCCESS record
///     `t0.elapsed()` into the PRIMARY (quorum) histogram,
///   - keep draining; when all N have completed-or-failed record the slowest
///     SUCCESS's `t0.elapsed()` into the SECONDARY (all-acks) histogram.
///   - if fewer than K succeed (loss/timeout) → count a quorum-miss, record no
///     quorum latency, continue. Loss-tolerant: the run is never aborted.
async fn run_fanout(
    clients: &[EchoClient],
    quorum_k: usize,
    duration: Duration,
    payload: usize,
) -> anyhow::Result<FanoutResult> {
    let n = clients.len();
    let mut quorum_hist = Histogram::<u64>::new_with_bounds(1, 600_000_000_000, 3)?;
    let mut all_acks_hist = Histogram::<u64>::new_with_bounds(1, 600_000_000_000, 3)?;
    let mut per_target = vec![TargetStats::default(); n];
    let body = Bytes::from(vec![0u8; payload]);
    let rpc_timeout = Duration::from_secs(1);

    let start = Instant::now();
    let deadline = start + duration;
    let mut rounds: u64 = 0;
    let mut quorum_miss: u64 = 0;

    while Instant::now() < deadline {
        let t0 = Instant::now();
        // Fire N concurrent RPCs, one per follower, tagging each with its index
        // so per-target stats are attributable as completions stream in.
        let mut inflight = FuturesUnordered::new();
        for (idx, client) in clients.iter().enumerate() {
            let body = body.clone();
            inflight.push(async move {
                let res = tokio::time::timeout(rpc_timeout, client.rpc(body)).await;
                let ok = matches!(res, Ok(Ok(_)));
                (idx, ok, t0.elapsed())
            });
        }

        let mut successes: usize = 0;
        let mut quorum_recorded = false;
        let mut slowest_success: Option<Duration> = None;

        while let Some((idx, ok, elapsed)) = inflight.next().await {
            if ok {
                per_target[idx].ok += 1;
                successes += 1;
                // K-th success → quorum reached: record quorum latency once.
                if successes == quorum_k && !quorum_recorded {
                    quorum_hist.record((elapsed.as_nanos() as u64).min(600_000_000_000))?;
                    quorum_recorded = true;
                }
                // FuturesUnordered does not guarantee completions resume in wall-clock
                // order, so take the max rather than trusting the last write.
                slowest_success = Some(slowest_success.map_or(elapsed, |prev| prev.max(elapsed)));
            } else {
                per_target[idx].fail += 1;
            }
        }

        rounds += 1;
        if quorum_recorded {
            // All N done; if any succeeded, record the slowest-success latency
            // as the "all-acks" (responses-for-all) latency.
            if let Some(slow) = slowest_success {
                all_acks_hist.record((slow.as_nanos() as u64).min(600_000_000_000))?;
            }
        } else {
            // Fewer than K succeeded → quorum miss. No quorum latency recorded.
            quorum_miss += 1;
        }
    }

    let achieved = quorum_hist.len() as f64 / start.elapsed().as_secs_f64();
    Ok(FanoutResult {
        quorum_hist,
        all_acks_hist,
        rounds,
        quorum_miss,
        per_target,
        achieved,
    })
}

#[allow(clippy::too_many_arguments)]
fn csv_row(
    system: &str,
    config: &str,
    workload: &str,
    payload: usize,
    inflight: usize,
    target_rate: f64,
    achieved_rate: f64,
    hist: &Histogram<u64>,
) -> String {
    format!(
        "{},{},{},{},{},{:.0},{:.1},{},{},{},{},{},{}",
        system,
        config,
        workload,
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

    // --- role=server: persistent responder until SIGINT. CSV→stdout (none here),
    //     all status/logs→stderr so a harness can capture client CSV cleanly. ---
    if args.role == "server" {
        let server = match args.transport.as_str() {
            "udp" => udp_echo_server(args.listen).await?,
            "quic" => quic_echo_server(args.listen).await?,
            other => anyhow::bail!("unknown transport {other}"),
        };
        let local = server.local_addr()?;
        eprintln!(
            "internode-rpc-bench: {} echo server listening on {}",
            args.transport, local
        );
        tokio::signal::ctrl_c().await?;
        eprintln!("internode-rpc-bench: SIGINT received, shutting down server");
        server.shutdown().await;
        return Ok(());
    }

    // --- fanout mode: client-only, multi-target. Build N EchoClients (the
    //     followers), resolve the quorum K, run the round loop, emit CSV. ---
    if args.mode == "fanout" {
        if args.role != "client" {
            anyhow::bail!("--mode fanout requires --role client");
        }
        let targets = &args.connect;
        if targets.len() < 2 {
            anyhow::bail!(
                "--mode fanout requires ≥2 --connect targets (got {}); pass a comma-separated list, e.g. --connect host1:9500,host2:9501",
                targets.len()
            );
        }
        let n = targets.len();
        // AUTO quorum: Raft majority of an (N+1)-node cluster (leader + N
        // followers) minus the leader's own vote = floor((N+1)/2).
        let quorum_k = if args.quorum == 0 {
            // floor((N+1)/2) == ceil(N/2) for all N ≥ 1.
            n.div_ceil(2)
        } else {
            args.quorum
        };
        if quorum_k < 1 || quorum_k > n {
            anyhow::bail!("--quorum K={quorum_k} out of range; require 1 ≤ K ≤ N (N={n})");
        }

        // Build one EchoClient per target up front.
        let mut clients = Vec::with_capacity(n);
        for &target in targets {
            let c = match args.transport.as_str() {
                "udp" => udp_echo_client(target).await?,
                "quic" => quic_echo_client(target).await?,
                other => anyhow::bail!("unknown transport {other}"),
            };
            clients.push(c);
        }
        let fanout_system = match args.transport.as_str() {
            "udp" => "udp-fanout",
            "quic" => "quic-fanout",
            _ => unreachable!(),
        };

        eprintln!(
            "internode-rpc-bench: role=client mode=fanout transport={} N={} K={} payload={}B duration={}s targets={:?}",
            args.transport, n, quorum_k, args.payload, args.duration, targets
        );

        // Warmup (discarded): connection setup, TLS handshake, page-ins.
        let _ = run_fanout(
            &clients,
            quorum_k,
            Duration::from_secs_f64(1.0),
            args.payload,
        )
        .await;
        let r = run_fanout(
            &clients,
            quorum_k,
            Duration::from_secs_f64(args.duration),
            args.payload,
        )
        .await?;

        // STDERR summary: per-target ok/fail + the all-acks p50/p99 (recorded
        // only on quorum-reached rounds; rounds that miss quorum contribute nothing).
        let per_target_str: Vec<String> = r
            .per_target
            .iter()
            .enumerate()
            .map(|(i, s)| format!("target[{i}] ok={} fail={}", s.ok, s.fail))
            .collect();
        eprintln!(
            "fanout: N={} K={} rounds={} quorum_miss={} | {} | all-acks p50={}us p99={}us (quorum-reached only)",
            n,
            quorum_k,
            r.rounds,
            r.quorum_miss,
            per_target_str.join(" | "),
            r.all_acks_hist.value_at_quantile(0.50) / 1000,
            r.all_acks_hist.value_at_quantile(0.99) / 1000,
        );

        // PRIMARY CSV row: quorum-latency histogram; inflight column = N (the
        // fan-out width); count = rounds that reached quorum. Always emitted.
        let row = csv_row(
            fanout_system,
            &args.config,
            "rpc-fanout",
            args.payload,
            n,
            args.rate,
            r.achieved,
            &r.quorum_hist,
        );
        println!("{CSV_HEADER}");
        println!("{row}");
        return Ok(());
    }

    // --- client / both (ping/ladder): build the echo client (+ owned server
    //     for `both`). These modes use the FIRST --connect target. ---
    let first_connect = || -> anyhow::Result<SocketAddr> {
        args.connect
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("--connect is required for role=client"))
    };
    let (system, client, server): (&str, EchoClient, Option<_>) = match args.transport.as_str() {
        "udp" => match args.role.as_str() {
            "client" => ("udp-rpc", udp_echo_client(first_connect()?).await?, None),
            _ => {
                let (c, s) = udp_echo_pair().await?;
                ("udp-rpc", c, Some(s))
            }
        },
        "quic" => match args.role.as_str() {
            "client" => ("quic-rpc", quic_echo_client(first_connect()?).await?, None),
            _ => {
                let (c, s) = quic_echo_pair().await?;
                ("quic-rpc", c, Some(s))
            }
        },
        other => anyhow::bail!("unknown transport {other}"),
    };

    eprintln!(
        "internode-rpc-bench: role={} mode={} transport={} payload={}B inflight={} rate={} duration={}s",
        args.role, args.mode, args.transport, args.payload, args.inflight, args.rate, args.duration
    );

    let row = match args.mode.as_str() {
        "ping" => {
            // Sequential single-inflight RTT probe. inflight is forced to 1.
            // Warmup (discarded): connection setup, TLS handshake, page-ins.
            // Warmup failures are silently ignored — they're expected under loss.
            let _ = run_ping(&client, Duration::from_secs_f64(1.0), args.payload).await;
            let (hist, achieved, failed) = run_ping(
                &client,
                Duration::from_secs_f64(args.duration),
                args.payload,
            )
            .await?;
            // ping system label = "{transport}-ping"; workload = "rpc-ping".
            let ping_system = match args.transport.as_str() {
                "udp" => "udp-ping",
                "quic" => "quic-ping",
                _ => unreachable!(),
            };
            let ok = hist.len();
            let total = ok + failed;
            let loss_pct = if total > 0 {
                100.0 * failed as f64 / total as f64
            } else {
                0.0
            };
            eprintln!(
                "ping: {} ok, {} failed ({:.1}% loss-affected)",
                ok, failed, loss_pct
            );
            // Always emit a row — even if count=0 — so the sweep never gets a
            // missing cell. Latency columns are 0 when the histogram is empty.
            csv_row(
                ping_system,
                &args.config,
                "rpc-ping",
                args.payload,
                1,
                args.rate,
                achieved,
                &hist,
            )
        }
        _ => {
            // ladder: open-loop CO-free rate driver (today's behavior).
            // Warmup (discarded): connection setup, TLS handshake, page-ins.
            let _ = run_step(
                &client,
                args.rate,
                args.inflight,
                Duration::from_secs_f64(1.0),
                args.payload,
            )
            .await?;
            let (hist, achieved) = run_step(
                &client,
                args.rate,
                args.inflight,
                Duration::from_secs_f64(args.duration),
                args.payload,
            )
            .await?;
            csv_row(
                system,
                &args.config,
                "rpc-echo",
                args.payload,
                args.inflight,
                args.rate,
                achieved,
                &hist,
            )
        }
    };

    println!("{CSV_HEADER}");
    println!("{row}");

    if let Some(server) = server {
        server.shutdown().await;
    }
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
        let row = csv_row(
            "udp-rpc", "loopback", "rpc-echo", 64, 8, 5000.0, 4987.6, &hist,
        );
        assert_eq!(row.split(',').count(), 13);
        assert!(row.starts_with("udp-rpc,loopback,rpc-echo,64,8,5000,4987.6,"));
        assert_eq!(CSV_HEADER.split(',').count(), 13);
    }

    #[test]
    fn ping_csv_row_has_13_columns_and_ping_labels() {
        let mut hist = Histogram::<u64>::new(3).unwrap();
        for v in [100u64, 200, 300, 400, 500] {
            hist.record(v).unwrap();
        }
        // ping mode: system="{transport}-ping", workload="rpc-ping", inflight=1.
        let row = csv_row(
            "udp-ping", "host-a", "rpc-ping", 64, 1, 20000.0, 31234.5, &hist,
        );
        assert_eq!(row.split(',').count(), 13);
        assert!(row.starts_with("udp-ping,host-a,rpc-ping,64,1,20000,31234.5,"));
    }
}
