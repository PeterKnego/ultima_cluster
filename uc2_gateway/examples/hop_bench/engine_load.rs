// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Hop-1 driver: N **independent** local `uc2_client::Engine`s over one
//! instance dir — the model of N edge processes sharing one node.
//!
//! The measurement shape is `m12_gate::run_client_measurement`'s, reproduced
//! per engine but stripped to the bytes that matter for a hop-isolation
//! number: a raw `vec![0xAB; payload]` command (no bincode, no session
//! envelope — the SINK discards the payload anyway), `stats::SendClock` for
//! the send/response correlation, and a `stats::StreamStats` per engine that
//! the process merges into the one `RESULT` line.
//!
//! Two threads per engine: the driver thread sends, a poll thread drains
//! completions. Neither sleeps while it has work — this measures a ceiling,
//! so a 20 µs idle sleep (which `m12_gate` can afford) would BE the result.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use uc2_client::{Engine, EngineConfig, Outcome, SubmitError};

use crate::stats::{self, StreamStats};

/// How long an engine waits for the node to publish `NODE_FLAG_CAN_SERVE`.
const LEADER_WAIT: Duration = Duration::from_secs(30);

#[derive(clap::Args)]
pub struct Args {
    #[arg(long)]
    pub instance_dir: PathBuf,
    #[arg(long, default_value = "hop-bench")]
    pub app_id: String,
    #[arg(long, default_value_t = 10)]
    pub secs: u64,
    #[arg(long, default_value_t = 64)]
    pub payload: usize,
    /// Per-engine inflight window (`EngineConfig::max_inflight`).
    #[arg(long, default_value_t = 4096)]
    pub inflight: u64,
    /// Independent engines (each one attach, one sender + one poll thread).
    #[arg(long, default_value_t = 1)]
    pub engines: usize,
}

pub fn run(a: Args) -> anyhow::Result<()> {
    anyhow::ensure!(a.engines >= 1, "--engines must be >= 1");
    let t0 = Instant::now();
    let instance_dir = Arc::new(a.instance_dir.clone());
    let app_id = Arc::new(a.app_id.clone());

    let mut handles = Vec::with_capacity(a.engines);
    for i in 0..a.engines {
        let instance_dir = Arc::clone(&instance_dir);
        let app_id = Arc::clone(&app_id);
        let (secs, payload, inflight) = (a.secs, a.payload, a.inflight);
        handles.push(
            thread::Builder::new()
                .name(format!("hop-engine-{i}"))
                .spawn(move || {
                    drive_one(&instance_dir, &app_id, t0, secs, payload, inflight)
                })
                .expect("spawn engine driver thread"),
        );
    }

    let mut merged = StreamStats::new();
    for (i, h) in handles.into_iter().enumerate() {
        let s = h.join().map_err(|_| anyhow::anyhow!("engine {i} thread panicked"))??;
        println!(
            "   engine[{i}]: sends={} responses={} lost={} responses/s={:.1}",
            s.sends,
            s.responses,
            s.lost,
            s.responses_per_sec()
        );
        merged.merge(&s);
    }

    stats::report(
        "engine",
        &merged,
        a.secs,
        a.payload,
        a.inflight,
        &[("engines", a.engines.to_string())],
    );
    Ok(())
}

/// One engine's full lifetime: attach, wait for a serving node, send for
/// `secs`, drain, and hand back the tally.
fn drive_one(
    instance_dir: &std::path::Path,
    app_id: &str,
    t0: Instant,
    secs: u64,
    payload: usize,
    inflight: u64,
) -> anyhow::Result<StreamStats> {
    let (send, mut poll) = Engine::attach(
        instance_dir,
        app_id,
        EngineConfig {
            max_inflight: inflight as u32,
            request_timeout: Duration::from_secs(30),
            max_payload: Some(512),
            serving_gate: true,
            ..EngineConfig::default()
        },
    )
    .map_err(|e| anyhow::anyhow!("engine attach {instance_dir:?}: {e}"))?;

    let serve_deadline = Instant::now() + LEADER_WAIT;
    while !send.can_serve() {
        anyhow::ensure!(
            Instant::now() < serve_deadline,
            "no serving node at {instance_dir:?} within {LEADER_WAIT:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }

    let clock = Arc::new(stats::SendClock::new(t0));
    let stop = Arc::new(AtomicBool::new(false));
    let resolved = Arc::new(AtomicU64::new(0));

    let poller = thread::Builder::new()
        .name("hop-engine-poll".into())
        .spawn({
            let clock = Arc::clone(&clock);
            let stop = Arc::clone(&stop);
            let resolved = Arc::clone(&resolved);
            move || {
                let mut s = StreamStats::new();
                while !stop.load(Ordering::Relaxed) {
                    let n = poll.poll(|c| {
                        match c.outcome {
                            Outcome::Response(_) => {
                                let now = clock.now_ns();
                                let lat = clock.latency_ns(c.user_data, now);
                                let _ = s.hist.record(lat);
                                s.responses += 1;
                                s.last_response_ns = s.last_response_ns.max(now);
                            }
                            Outcome::NotLeader { .. } => s.redirected += 1,
                            Outcome::Retry => s.retried += 1,
                            Outcome::TimedOut | Outcome::InstanceRestart { .. } => s.lost += 1,
                        }
                        resolved.fetch_add(1, Ordering::Relaxed);
                    });
                    if n == 0 {
                        thread::yield_now();
                    }
                }
                s
            }
        })
        .expect("spawn poll thread");

    let cmd_bytes = vec![0xABu8; payload];
    let mut sent: u64 = 0;
    let deadline = t0 + Duration::from_secs(secs);
    while Instant::now() < deadline {
        clock.stamp(sent);
        match send.try_submit(sent, &cmd_bytes) {
            Ok(()) => sent += 1,
            Err(SubmitError::Backpressure) => thread::yield_now(),
            Err(SubmitError::NotServing) => thread::sleep(Duration::from_millis(1)),
            Err(e) => panic!("try_submit: {e}"),
        }
    }
    let send_window_end_ns = clock.now_ns();

    let drain_deadline = Instant::now() + stats::DRAIN_GRACE;
    while resolved.load(Ordering::Relaxed) < sent && Instant::now() < drain_deadline {
        thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    let mut s = poller.join().map_err(|_| anyhow::anyhow!("poll thread panicked"))?;

    s.sends = sent;
    s.send_window_end_ns = send_window_end_ns;
    // Anything the drain grace never resolved is lost, by definition of the
    // engine's central contract having had its window.
    s.lost += sent.saturating_sub(resolved.load(Ordering::Relaxed));
    Ok(s)
}
