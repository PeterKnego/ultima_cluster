// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Hop-3 driver: N real [`uc2_remote::RemoteClient`]s.
//!
//! Same measurement core as `m12_gate`'s `run_remote_measurement` — one sender
//! thread per client (paced by `submit`, which BLOCKS on the edge's credits)
//! handing `(seq, Ticket)` round-robin to a small pool of waiter threads so
//! pipelining is not serialized behind `Ticket::wait` — but with **raw payload
//! bytes**: no bincode, so what the arm measures is the client and the wire,
//! not a codec.
//!
//! Run it against `dummy-edge` to isolate the client half of hop 3, and
//! against the real `Edge` for the end-to-end number; `blaster` against the
//! same sink is the floor the difference is charged to `RemoteClient` against.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use uc2_remote::{RemoteClient, RemoteConfig, Ticket};

use crate::stats::{new_hist, SendClock, StreamStats};

/// Waiter threads per connection (m12_gate uses 8 for its single client; one
/// pool per connection here, so 4 keeps the thread count sane at high
/// `--conns`).
const N_WAITERS: usize = 4;
/// Bound on one ticket's wait: generous relative to a healthy run so it never
/// becomes the limiter, finite so a stuck response cannot hang the harness.
const TICKET_WAIT: Duration = Duration::from_secs(10);
/// End-to-end budget for one request, across re-sends and reconnects.
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
    /// `RemoteConfig::max_inflight` — the client's local cap on unanswered
    /// requests, applied on top of the edge's credits.
    #[arg(long, default_value_t = 1024)]
    pub inflight: u64,
    #[arg(long, default_value_t = 1)]
    pub conns: usize,
}

pub fn run(a: Args) -> anyhow::Result<()> {
    if a.conns == 0 {
        anyhow::bail!("remote-load: --conns must be at least 1");
    }
    let members: Vec<String> = a
        .gateways
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
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
            std::thread::Builder::new()
                .name(format!("hb-remote-{i}"))
                .spawn(move || -> anyhow::Result<StreamStats> {
                    let remote = RemoteClient::connect(cfg)
                        .map_err(|e| anyhow::anyhow!("conn {i}: connect: {e}"))?;
                    let s = measure(&remote, &payload, t0, deadline);
                    let st = remote.stats();
                    println!(
                        "   conn {i}: retries={} redirects={} leader_changes={} reconnects={} \
                         resends={} unknown={} expired={} refused_members={} max_credits_seen={}",
                        st.retries,
                        st.redirects,
                        st.leader_changes,
                        st.reconnects,
                        st.resends,
                        st.unknown,
                        st.expired,
                        st.refused_members,
                        st.max_credits_seen
                    );
                    remote.shutdown();
                    Ok(s)
                })?,
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
    crate::stats::report(
        "remote",
        &merged,
        a.secs,
        a.payload,
        a.inflight,
        &[("conns", a.conns.to_string())],
    );
    Ok(())
}

/// One client's send window: a sender loop paced by `submit`'s own credit
/// blocking, plus [`N_WAITERS`] ticket-wait threads that each own a histogram
/// (a shared `Mutex<Histogram>` would serialize every response and charge the
/// lock to the measured latency).
fn measure(remote: &RemoteClient, payload: &[u8], t0: Instant, deadline: Instant) -> StreamStats {
    let clock = Arc::new(SendClock::new(t0));
    let responses = Arc::new(AtomicU64::new(0));
    let lost = Arc::new(AtomicU64::new(0));
    let last_response_ns = Arc::new(AtomicU64::new(0));

    let mut senders = Vec::with_capacity(N_WAITERS);
    let mut waiters = Vec::with_capacity(N_WAITERS);
    for w in 0..N_WAITERS {
        let (tx, rx) = mpsc::channel::<(u64, Ticket)>();
        senders.push(tx);
        let clock = Arc::clone(&clock);
        let responses = Arc::clone(&responses);
        let lost = Arc::clone(&lost);
        let last_response_ns = Arc::clone(&last_response_ns);
        waiters.push(
            std::thread::Builder::new()
                .name(format!("hb-remote-wait-{w}"))
                .spawn(move || {
                    let mut hist = new_hist();
                    for (seq, ticket) in rx {
                        let outcome = ticket.wait_timeout(TICKET_WAIT);
                        let now = clock.now_ns();
                        match outcome {
                            Ok(_resp) => {
                                let _ = hist.record(clock.latency_ns(seq, now));
                                responses.fetch_add(1, Ordering::Relaxed);
                                last_response_ns.fetch_max(now, Ordering::Relaxed);
                            }
                            Err(_e) => {
                                lost.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    hist
                })
                .expect("spawn waiter thread"),
        );
    }

    let mut sends = 0u64;
    let mut submit_err: Option<String> = None;
    while Instant::now() < deadline {
        clock.stamp(sends);
        // `RemoteClient::submit` BLOCKS while credits (or `max_inflight`) are
        // exhausted — that block IS the pacing of this loop; there is no
        // transient backpressure error to yield on. An `Err` here is
        // `TimedOut` (credits never reopened) or `Closed`, both genuine
        // failures for a healthy run.
        match remote.submit(payload) {
            Ok(ticket) => {
                let w = (sends as usize) % N_WAITERS;
                if senders[w].send((sends, ticket)).is_err() {
                    submit_err = Some("waiter thread died".to_string());
                    break;
                }
                sends += 1;
            }
            Err(e) => {
                submit_err = Some(e.to_string());
                break;
            }
        }
    }
    let send_window_end_ns = clock.now_ns();
    if let Some(e) = submit_err {
        eprintln!("remote-load: submit stopped early: {e}");
    }

    // Closing the channels lets each waiter drain (and resolve, one way or
    // another) everything already queued, then exit.
    drop(senders);
    let mut s = StreamStats::new();
    for w in waiters {
        if let Ok(hist) = w.join() {
            let _ = s.hist.add(hist);
        }
    }
    s.sends = sends;
    s.responses = responses.load(Ordering::Relaxed);
    s.lost = lost.load(Ordering::Relaxed);
    s.last_response_ns = last_response_ns.load(Ordering::Relaxed);
    s.send_window_end_ns = send_window_end_ns;
    s
}
