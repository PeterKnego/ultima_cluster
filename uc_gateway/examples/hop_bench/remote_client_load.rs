// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Hop-3 driver: N real remote clients, on the **blocking `RemoteClient`**.
//!
//! The twin of `remote_load.rs` (which drives the `RemoteEngine` halves
//! directly): same sink, same wire, same `StreamStats`/`report` shape, so the
//! two are comparable line for line. This arm exists to measure what a
//! developer who reaches for the convenient client actually gets (#43 step 2):
//! the per-request lock on the send half, the `Arc<TicketCore>` allocation,
//! and the condvar wakeup that the halves avoid.
//!
//! Two modes, because developers use the blocking client two ways:
//!
//! - `per-call`: `--waiters` threads per connection, each looping
//!   `submit()` → `Ticket::wait()`. One request in flight per thread — the
//!   shape a request handler falls into. Throughput is bounded by
//!   `waiters / round-trip`, which is the point: it shows the latency-bound
//!   ceiling a caller sees without a window.
//! - `window`: one submitter per connection keeps `--inflight` tickets in a
//!   deque and `wait()`s the oldest when it is full — the "window of
//!   tickets" pattern the docs table recommends. This is the fair,
//!   depth-matched comparison against `remote-load`'s `--inflight`.
//!
//! Latency is measured in the submitting thread (stamp before `submit`,
//! elapsed when `wait` returns), so no `SendClock` slot table is needed;
//! `SendClock` is used only for the run-relative `now_ns` the report reads.

use std::collections::VecDeque;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use uc_remote::{RemoteClient, RemoteConfig, RemoteError, Ticket};

use crate::stats::{self, StreamStats};

/// End-to-end budget per request; generous, because a bar run must never
/// report a timeout it caused itself.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Mode {
    /// `--waiters` threads, each `submit()` → `wait()`, one in flight each.
    PerCall,
    /// One submitter keeping `--inflight` tickets in flight.
    Window,
}

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
    /// `RemoteConfig::max_inflight`, and in `window` mode the deque depth.
    #[arg(long, default_value_t = 1024)]
    pub inflight: u64,
    #[arg(long, default_value_t = 1)]
    pub conns: usize,
    #[arg(long, value_enum, default_value_t = Mode::Window)]
    pub mode: Mode,
    /// `per-call` mode only: waiter threads per connection.
    #[arg(long, default_value_t = 4)]
    pub waiters: usize,
}

pub fn run(a: Args) -> anyhow::Result<()> {
    if a.conns == 0 {
        anyhow::bail!("remote-client-load: --conns must be at least 1");
    }
    if a.mode == Mode::PerCall && a.waiters == 0 {
        anyhow::bail!("remote-client-load: --waiters must be at least 1 in per-call mode");
    }
    if a.mode == Mode::Window && a.inflight == 0 {
        anyhow::bail!("remote-client-load: --inflight must be at least 1 in window mode");
    }
    let members: Vec<String> = a
        .gateways
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if members.is_empty() {
        anyhow::bail!("remote-client-load: --gateways is empty");
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
        let (mode, waiters, inflight) = (a.mode, a.waiters, a.inflight as usize);
        handles.push(
            thread::Builder::new()
                .name(format!("hb-rclient-{i}"))
                .spawn(move || -> anyhow::Result<StreamStats> {
                    drive_one(i, cfg, payload, t0, deadline, mode, waiters, inflight)
                })?,
        );
    }

    let mut merged = StreamStats::new();
    for (i, h) in handles.into_iter().enumerate() {
        let s = h
            .join()
            .map_err(|_| anyhow::anyhow!("remote-client conn {i} panicked"))??;
        println!(
            "   conn {i}: sends={} responses={} lost={} responses/s={:.1}",
            s.sends,
            s.responses,
            s.lost,
            s.responses_per_sec()
        );
        merged.merge(&s);
    }
    let mode = match a.mode {
        Mode::PerCall => "per-call",
        Mode::Window => "window",
    };
    stats::report(
        "remote-client",
        &merged,
        a.secs,
        a.payload,
        a.inflight,
        &[
            ("conns", a.conns.to_string()),
            ("mode", mode.to_string()),
            ("waiters", a.waiters.to_string()),
        ],
    );
    Ok(())
}

/// Resolve one ticket into the stats: a response is a latency sample, any
/// error (`Expired`, `Unknown`, `TimedOut`, `Closed`, …) is a loss — the same
/// classification `remote_load.rs` applies to `RemoteOutcome`.
///
/// The completion clock is read **after** `wait` returns, so the sample is
/// `submit → the answer landed`, the same quantity `remote_load`'s histogram
/// records. Reading it before the wait would measure `submit` alone and make
/// this arm look an order of magnitude faster than the engine arm.
///
/// `grace` bounds the wait when draining the tail; `None` lets the client's
/// own `request_timeout` end it.
fn settle(
    s: &mut StreamStats,
    clock: &stats::SendClock,
    sent_ns: u64,
    t: Ticket,
    grace: Option<Duration>,
) {
    let r = match grace {
        Some(d) => t.wait_timeout(d),
        None => t.wait(),
    };
    match r {
        Ok(_) => {
            let now = clock.now_ns();
            let _ = s
                .hist
                .record(now.saturating_sub(sent_ns).min(stats::HIST_MAX_NS));
            s.responses += 1;
            s.last_response_ns = s.last_response_ns.max(now);
        }
        Err(_) => s.lost += 1,
    }
}

#[allow(clippy::too_many_arguments)]
fn drive_one(
    idx: usize,
    cfg: RemoteConfig,
    payload: Vec<u8>,
    t0: Instant,
    deadline: Instant,
    mode: Mode,
    waiters: usize,
    inflight: usize,
) -> anyhow::Result<StreamStats> {
    let client = Arc::new(
        RemoteClient::connect(cfg).map_err(|e| anyhow::anyhow!("conn {idx}: connect: {e}"))?,
    );
    let clock = Arc::new(stats::SendClock::new(t0));

    let mut s = match mode {
        Mode::PerCall => per_call(idx, &client, &clock, &payload, deadline, waiters)?,
        Mode::Window => window(idx, &client, &clock, &payload, deadline, inflight)?,
    };
    s.send_window_end_ns = s.send_window_end_ns.max(clock.now_ns());

    let st = client.stats();
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
    client.shutdown();
    Ok(s)
}

/// `waiters` threads sharing one client, each with one request in flight.
fn per_call(
    idx: usize,
    client: &Arc<RemoteClient>,
    clock: &Arc<stats::SendClock>,
    payload: &[u8],
    deadline: Instant,
    waiters: usize,
) -> anyhow::Result<StreamStats> {
    let mut handles = Vec::with_capacity(waiters);
    for w in 0..waiters {
        let client = Arc::clone(client);
        let clock = Arc::clone(clock);
        let payload = payload.to_vec();
        handles.push(
            thread::Builder::new()
                .name(format!("hb-rclient-{idx}-w{w}"))
                .spawn(move || -> anyhow::Result<StreamStats> {
                    let mut s = StreamStats::new();
                    while Instant::now() < deadline {
                        let sent_ns = clock.now_ns();
                        match client.submit(&payload) {
                            Ok(t) => {
                                s.sends += 1;
                                settle(&mut s, &clock, sent_ns, t, None);
                            }
                            // The credit wait timed out: counted as a loss,
                            // the run continues — the same as an engine-side
                            // `TimedOut` completion.
                            Err(RemoteError::TimedOut) => {
                                s.sends += 1;
                                s.lost += 1;
                            }
                            Err(e) => anyhow::bail!("conn {idx} waiter {w}: submit: {e}"),
                        }
                    }
                    s.send_window_end_ns = clock.now_ns();
                    Ok(s)
                })?,
        );
    }
    let mut merged = StreamStats::new();
    for (w, h) in handles.into_iter().enumerate() {
        let s = h
            .join()
            .map_err(|_| anyhow::anyhow!("conn {idx} waiter {w} panicked"))??;
        merged.merge(&s);
    }
    Ok(merged)
}

/// One submitter keeping `inflight` tickets in a deque; waits the oldest
/// when the deque is full, then drains the tail after the window closes.
///
/// Waiting the OLDEST ticket is head-of-line blocking if completions ever
/// arrive out of order. Against an in-order sink on one connection (arm C's
/// `dummy-edge`) they do not, so this is faithful. Pointed at a real edge,
/// where a RETRY or REDIRECT re-sends under the same seq, out-of-order
/// completion would penalise this arm in a way `remote_load`'s
/// arrival-order poll loop is not — keep this ladder on an in-order sink.
fn window(
    idx: usize,
    client: &Arc<RemoteClient>,
    clock: &Arc<stats::SendClock>,
    payload: &[u8],
    deadline: Instant,
    inflight: usize,
) -> anyhow::Result<StreamStats> {
    let mut s = StreamStats::new();
    let mut pending: VecDeque<(u64, Ticket)> = VecDeque::with_capacity(inflight);
    while Instant::now() < deadline {
        if pending.len() >= inflight {
            let (sent_ns, t) = pending.pop_front().expect("non-empty deque");
            settle(&mut s, clock, sent_ns, t, None);
        }
        let sent_ns = clock.now_ns();
        match client.submit(payload) {
            Ok(t) => {
                s.sends += 1;
                pending.push_back((sent_ns, t));
            }
            Err(RemoteError::TimedOut) => {
                s.sends += 1;
                s.lost += 1;
            }
            Err(e) => anyhow::bail!("conn {idx}: submit: {e}"),
        }
    }
    s.send_window_end_ns = clock.now_ns();
    // Drain the tail, bounded by the same grace the engine arm allows. The
    // per-ticket wait is bounded too, so the grace is the real bound rather
    // than grace + the client's 30 s request_timeout.
    let drain_deadline = Instant::now() + stats::DRAIN_GRACE;
    while let Some((sent_ns, t)) = pending.pop_front() {
        let now = Instant::now();
        if now >= drain_deadline {
            s.lost += 1 + pending.len() as u64;
            break;
        }
        settle(&mut s, clock, sent_ns, t, Some(drain_deadline - now));
    }
    Ok(s)
}
