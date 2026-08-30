// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Hop-2 driver / TCP floor: a raw remote-protocol v1 client.
//!
//! It speaks the wire and nothing else — HELLO, pre-encoded SUBMITs written in
//! batches, credit tracking off `ResponseMeta`/`STATUS`, RESPONSE parsing —
//! with **none** of [`uc_remote::RemoteClient`]'s state machine: no per-request
//! `Ticket`, no pending map, no state lock, no reconnect, no redirect chase.
//! Against the real `Edge` it therefore measures hop 2 with the client's own
//! overhead removed; against `dummy-edge` it is the raw framed-TCP floor of
//! this box.
//!
//! Per connection there are exactly two threads and no mutex on the hot path:
//! a **writer** that fills the credit window, and a **reader** that parses
//! responses and owns the connection's histogram. They share three atomics
//! (`credits`, `acked_seq`, `completed`); the only lock is a resend queue that
//! is touched solely when a RETRY arrives.

use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use uc_remote::conn::FramedConn;
use uc_remote::frame::{
    encode_frame, FrameType, Hello, HelloOk, HelloRefused, Header, ResponseMeta, Status,
    PROTOCOL_VERSION,
};

use crate::stats::{SendClock, StreamStats, DRAIN_GRACE};

/// Budget for the edge's HELLO_OK.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// Socket read timeout for a live connection (the reader's idle tick).
const READ_TICK: Duration = Duration::from_millis(50);
/// Mid-frame stall budget for a live connection.
const MAX_STALL: Duration = Duration::from_secs(30);
/// Writes are bounded so a wedged peer cannot park the writer forever.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// Yield this many times on an empty window before sleeping, so an idle
/// connection does not burn a core.
const SPINS_BEFORE_SLEEP: u32 = 256;

#[derive(clap::Args)]
pub struct Args {
    /// Gateway (or `dummy-edge`) address every connection dials.
    #[arg(long)]
    pub gateway: SocketAddr,
    #[arg(long, default_value = "hop-bench")]
    pub app_id: String,
    #[arg(long, default_value_t = 10)]
    pub secs: u64,
    /// SUBMIT payload bytes.
    #[arg(long, default_value_t = 64)]
    pub payload: usize,
    /// Local cap on unanswered seqs per connection (on top of the edge's
    /// credits).
    #[arg(long, default_value_t = 1024)]
    pub inflight: u64,
    #[arg(long, default_value_t = 1)]
    pub conns: usize,
    /// Maximum frames written per `write_all_bytes`.
    #[arg(long, default_value_t = 64)]
    pub batch: usize,
}

/// The three hot-path atomics the writer and reader share.
struct ConnState {
    credits: AtomicU32,
    acked_seq: AtomicU64,
    completed: AtomicU64,
    /// Seqs a RETRY asked us to send again. Off the hot path entirely.
    resend: Mutex<Vec<u64>>,
}

pub fn run(a: Args) -> anyhow::Result<()> {
    if a.conns == 0 {
        anyhow::bail!("blaster: --conns must be at least 1");
    }
    let batch = a.batch.max(1);
    let payload = vec![0xABu8; a.payload];
    let t0 = Instant::now();
    let deadline = t0 + Duration::from_secs(a.secs);

    let mut handles = Vec::with_capacity(a.conns);
    for i in 0..a.conns {
        let gateway = a.gateway;
        let app_id = a.app_id.clone();
        let payload = payload.clone();
        let inflight = a.inflight;
        handles.push(
            std::thread::Builder::new()
                .name(format!("hb-blast-{i}"))
                .spawn(move || -> anyhow::Result<StreamStats> {
                    run_conn(i, gateway, &app_id, payload, inflight, batch, t0, deadline)
                })?,
        );
    }

    let mut merged = StreamStats::new();
    for (i, h) in handles.into_iter().enumerate() {
        let s = h.join().map_err(|_| anyhow::anyhow!("blaster conn {i} panicked"))??;
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
        "blaster",
        &merged,
        a.secs,
        a.payload,
        a.inflight,
        &[("conns", a.conns.to_string())],
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_conn(
    idx: usize,
    gateway: SocketAddr,
    app_id: &str,
    payload: Vec<u8>,
    inflight: u64,
    batch: usize,
    t0: Instant,
    deadline: Instant,
) -> anyhow::Result<StreamStats> {
    let client_id = fresh_client_id(idx);
    let sock = TcpStream::connect_timeout(&gateway, HANDSHAKE_TIMEOUT)?;
    let mut conn = FramedConn::new(sock)?;
    conn.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
    conn.set_write_timeout(Some(WRITE_TIMEOUT))?;

    // ---- HELLO / HELLO_OK -------------------------------------------------
    let mut hello = Vec::new();
    Hello { app_id }.encode(&mut hello);
    conn.write_frame(
        Header {
            ty: FrameType::Hello,
            flags: 0,
            version: PROTOCOL_VERSION,
            client_id,
            seq: 0,
        },
        &hello,
    )
    .map_err(|e| anyhow::anyhow!("conn {idx}: HELLO write: {e}"))?;
    let (h, reply) = match conn.read_frame(HANDSHAKE_TIMEOUT) {
        Ok(Some(f)) => f,
        Ok(None) => anyhow::bail!("conn {idx}: no HELLO_OK within {HANDSHAKE_TIMEOUT:?}"),
        Err(e) => anyhow::bail!("conn {idx}: HELLO read: {e}"),
    };
    let credits = match h.ty {
        FrameType::HelloOk => {
            HelloOk::decode(&reply).map_err(|e| anyhow::anyhow!("conn {idx}: HELLO_OK: {e}"))?.credits
        }
        FrameType::HelloRefused => {
            let r = HelloRefused::decode(&reply)
                .map_err(|e| anyhow::anyhow!("conn {idx}: HELLO_REFUSED: {e}"))?;
            anyhow::bail!("conn {idx}: HELLO refused (reason {}): {}", r.reason, r.detail);
        }
        other => anyhow::bail!("conn {idx}: unexpected {other:?} in answer to HELLO"),
    };

    // ---- split: reader on a cloned half, writer keeps the original --------
    conn.set_read_timeout(Some(READ_TICK))?;
    let read_half = conn.try_clone()?;
    let state = Arc::new(ConnState {
        credits: AtomicU32::new(credits),
        acked_seq: AtomicU64::new(0),
        completed: AtomicU64::new(0),
        resend: Mutex::new(Vec::new()),
    });
    let clock = Arc::new(SendClock::new(t0));

    let reader = {
        let state = Arc::clone(&state);
        let clock = Arc::clone(&clock);
        std::thread::Builder::new()
            .name(format!("hb-blast-rx-{idx}"))
            .spawn(move || reader_loop(read_half, state, clock))?
    };

    // ---- writer -----------------------------------------------------------
    let mut sends = 0u64; // distinct seqs sent; a resend is NOT a new send
    let mut next_seq = 1u64;
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut spins = 0u32;
    let hdr = |seq: u64| Header {
        ty: FrameType::Submit,
        flags: 0,
        version: PROTOCOL_VERSION,
        client_id,
        seq,
    };
    while Instant::now() < deadline {
        buf.clear();
        let mut frames = 0usize;

        // Resends first (a RETRY told us this seq never landed), re-stamped so
        // the latency measured is the one the caller actually waits.
        let pending_resends: Vec<u64> = {
            let mut q = state.resend.lock().expect("resend queue");
            if q.is_empty() { Vec::new() } else { std::mem::take(&mut *q) }
        };
        for seq in pending_resends {
            clock.stamp(seq);
            encode_frame(&mut buf, hdr(seq), &payload);
            frames += 1;
        }

        // The credit rule the real client honours: seq `next` may go only if
        // `next <= acked_seq + credits`. With `sends == next_seq - 1` that is
        // `room_credit = acked_seq + credits - sends`.
        let completed = state.completed.load(Ordering::Relaxed);
        let window =
            state.acked_seq.load(Ordering::Relaxed) + state.credits.load(Ordering::Relaxed) as u64;
        let room_credit = window.saturating_sub(sends);
        let room_local = inflight.saturating_sub(sends.saturating_sub(completed));
        let room = room_credit.min(room_local).min(batch as u64) as usize;
        for _ in 0..room {
            clock.stamp(next_seq);
            encode_frame(&mut buf, hdr(next_seq), &payload);
            next_seq += 1;
            sends += 1;
            frames += 1;
        }

        if frames == 0 {
            spins += 1;
            if spins >= SPINS_BEFORE_SLEEP {
                std::thread::sleep(Duration::from_micros(10));
            } else {
                std::thread::yield_now();
            }
            continue;
        }
        spins = 0;
        if conn.write_all_bytes(&buf).is_err() {
            break;
        }
    }
    let send_window_end_ns = clock.now_ns();

    // ---- drain ------------------------------------------------------------
    let drain_until = Instant::now() + DRAIN_GRACE;
    while state.completed.load(Ordering::Relaxed) < sends && Instant::now() < drain_until {
        std::thread::sleep(Duration::from_micros(200));
    }
    let lost = sends.saturating_sub(state.completed.load(Ordering::Relaxed));

    // Wake the reader out of its blocking read, then take its tally.
    conn.shutdown();
    let mut s = reader.join().map_err(|_| anyhow::anyhow!("conn {idx}: reader panicked"))?;
    s.sends = sends;
    s.lost = lost;
    s.send_window_end_ns = send_window_end_ns;
    Ok(s)
}

/// Parse responses until the connection ends. Owns this connection's
/// histogram, so nothing on the response path takes a lock.
fn reader_loop(mut fc: FramedConn, state: Arc<ConnState>, clock: Arc<SendClock>) -> StreamStats {
    let mut s = StreamStats::new();
    loop {
        match fc.read_frame_buffered(MAX_STALL) {
            Ok(None) => continue,
            Ok(Some((h, payload))) => handle(&mut s, &state, &clock, h, &payload),
            Err(_) => return s,
        }
        loop {
            match fc.next_buffered() {
                Ok(Some((h, payload))) => handle(&mut s, &state, &clock, h, &payload),
                Ok(None) => break,
                Err(_) => return s,
            }
        }
    }
}

fn handle(s: &mut StreamStats, state: &ConnState, clock: &SendClock, h: Header, payload: &[u8]) {
    match h.ty {
        FrameType::Response => {
            let Ok(meta) = ResponseMeta::decode(payload) else { return };
            state.credits.store(meta.credits, Ordering::Relaxed);
            state.acked_seq.fetch_max(meta.acked_seq, Ordering::Relaxed);
            state.completed.fetch_add(1, Ordering::Relaxed);
            let now = clock.now_ns();
            let _ = s.hist.record(clock.latency_ns(h.seq, now));
            s.responses += 1;
            s.last_response_ns = s.last_response_ns.max(now);
        }
        FrameType::Status => {
            let Ok(st) = Status::decode(payload) else { return };
            state.credits.store(st.credits, Ordering::Relaxed);
            state.acked_seq.fetch_max(st.acked_seq, Ordering::Relaxed);
        }
        FrameType::Retry => {
            s.retried += 1;
            state.resend.lock().expect("resend queue").push(h.seq);
        }
        FrameType::Redirect | FrameType::LeaderChanged => {
            // This client does not chase leaders; it only counts them, so a
            // run against a follower's edge is visible rather than silent.
            s.redirected += 1;
        }
        _ => {}
    }
}

/// A per-process, per-connection identity. The edge keys its session dedup on
/// this, so two connections must never collide.
fn fresh_client_id(idx: usize) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x5EED_5EED_5EED_5EED);
    let pid = std::process::id() as u64;
    nanos.rotate_left(17)
        ^ pid.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (idx as u64 + 1).wrapping_mul(0xD6E8_FEB8_6659_FD93)
}
