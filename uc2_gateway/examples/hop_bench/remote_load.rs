// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Hop-3 driver: N real remote clients, on the `RemoteEngine` halves.
//!
//! Same measurement shape as `engine_load.rs` (the shmem arm), so the two are
//! comparable line for line: one submitter loop calling `try_submit` with the
//! request's index as `user_data`, one poll thread owning the histogram, and
//! latency correlated through `SendClock` — no `Ticket`, no waiter pool, no
//! channel per request.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use uc2_remote::{RemoteConfig, RemoteEngine, RemoteOutcome, SubmitError};

use crate::stats::{self, StreamStats};

/// End-to-end budget per request; generous, because a bar run must never
/// report a timeout it caused itself.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(clap::Args)]
pub struct Args {
    /// Comma-separated gateway addresses; the first is dialled first.
    #[arg(long)]
    pub gateways: String,
    #[arg(long, default_value = "hop-bench")]
    pub app_id: String,
    #[arg(long, default_value_t = 10)]
    pub secs: u64,
    /// SUBMIT payload bytes.
    #[arg(long, default_value_t = 64)]
    pub payload: usize,
    /// `RemoteConfig::max_inflight` — the local cap on unanswered requests,
    /// applied on top of the edge's credits.
    #[arg(long, default_value_t = 1024)]
    pub inflight: u64,
    #[arg(long, default_value_t = 1)]
    pub conns: usize,
}

pub fn run(a: Args) -> anyhow::Result<()> {
    if a.conns == 0 {
        anyhow::bail!("remote-load: --conns must be at least 1");
    }
    let members: Vec<String> =
        a.gateways.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
    if members.is_empty() {
        anyhow::bail!("remote-load: --gateways is empty");
    }

    let payload = vec![0xABu8; a.payload];
    let t0 = Instant::now();
    let deadline = t0 + Duration::from_secs(a.secs);

    let mut handles = Vec::with_capacity(a.conns);
    for i in 0..a.conns {
        let cfg = RemoteConfig {
            app_id: a.app_id.clone(),
            members: members.clone(),
            client_id: None,
            max_inflight: a.inflight as u32,
            request_timeout: REQUEST_TIMEOUT,
            ..RemoteConfig::default()
        };
        let payload = payload.clone();
        handles.push(
            thread::Builder::new().name(format!("hb-remote-{i}")).spawn(
                move || -> anyhow::Result<StreamStats> { drive_one(i, cfg, payload, t0, deadline) },
            )?,
        );
    }

    let mut merged = StreamStats::new();
    for (i, h) in handles.into_iter().enumerate() {
        let s = h.join().map_err(|_| anyhow::anyhow!("remote conn {i} panicked"))??;
        println!(
            "   conn {i}: sends={} responses={} lost={} responses/s={:.1}",
            s.sends,
            s.responses,
            s.lost,
            s.responses_per_sec()
        );
        merged.merge(&s);
    }
    stats::report(
        "remote",
        &merged,
        a.secs,
        a.payload,
        a.inflight,
        &[("conns", a.conns.to_string())],
    );
    Ok(())
}

fn drive_one(
    idx: usize,
    cfg: RemoteConfig,
    payload: Vec<u8>,
    t0: Instant,
    deadline: Instant,
) -> anyhow::Result<StreamStats> {
    let (send, mut poll) =
        RemoteEngine::connect(cfg).map_err(|e| anyhow::anyhow!("conn {idx}: connect: {e}"))?;

    let clock = Arc::new(stats::SendClock::new(t0));
    let stop = Arc::new(AtomicBool::new(false));
    let resolved = Arc::new(AtomicU64::new(0));

    // Taken BEFORE `poll` moves into the thread, so the submitter can wake a
    // parked poller at the end of the run.
    let wake = poll.wait_handle();
    let poller = thread::Builder::new()
        .name(format!("hb-remote-poll-{idx}"))
        .spawn({
            let clock = Arc::clone(&clock);
            let stop = Arc::clone(&stop);
            let resolved = Arc::clone(&resolved);
            let wait = wake.clone();
            move || {
                let mut s = StreamStats::new();
                while !stop.load(Ordering::Relaxed) {
                    let n = poll.poll(|c| {
                        match c.outcome {
                            RemoteOutcome::Response { expired, .. } => {
                                if expired {
                                    s.lost += 1;
                                } else {
                                    let now = clock.now_ns();
                                    let _ = s.hist.record(clock.latency_ns(c.user_data, now));
                                    s.responses += 1;
                                    s.last_response_ns = s.last_response_ns.max(now);
                                }
                            }
                            RemoteOutcome::Unknown
                            | RemoteOutcome::PayloadTooLarge
                            | RemoteOutcome::TimedOut
                            | RemoteOutcome::Closed => s.lost += 1,
                        }
                        resolved.fetch_add(1, Ordering::Relaxed);
                    });
                    if n == 0 {
                        wait.park(Duration::from_micros(200));
                    }
                }
                s
            }
        })?;

    let mut sent = 0u64;
    while Instant::now() < deadline {
        clock.stamp(sent);
        match send.try_submit(sent, &payload) {
            Ok(()) => sent += 1,
            Err(SubmitError::Backpressure) => thread::yield_now(),
            Err(e) => anyhow::bail!("conn {idx}: try_submit: {e}"),
        }
    }
    let send_window_end_ns = clock.now_ns();

    let drain_deadline = Instant::now() + stats::DRAIN_GRACE;
    while resolved.load(Ordering::Relaxed) < sent && Instant::now() < drain_deadline {
        thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    wake.wake();
    let mut s = poller.join().map_err(|_| anyhow::anyhow!("conn {idx}: poll thread panicked"))?;

    let st = send.stats();
    println!(
        "   conn {idx}: retries={} redirects={} leader_changes={} reconnects={} resends={} \
         unknown={} expired={} refused_members={} max_credits_seen={} \
         socket_writes={} frames_written={} frames_per_write={:.1}",
        st.retries,
        st.redirects,
        st.leader_changes,
        st.reconnects,
        st.resends,
        st.unknown,
        st.expired,
        st.refused_members,
        st.max_credits_seen,
        st.socket_writes,
        st.frames_written,
        st.frames_written as f64 / st.socket_writes.max(1) as f64
    );
    send.shutdown();

    s.sends = sent;
    s.send_window_end_ns = send_window_end_ns;
    s.lost += sent.saturating_sub(resolved.load(Ordering::Relaxed));
    Ok(s)
}
