// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Hop-3 sink: a TCP server that speaks remote protocol v1 and answers every
//! SUBMIT/QUERY *immediately*, out of nothing.
//!
//! It is the infinitely fast edge: no `Engine`, no shmem, no node — so a
//! `remote-load` (or `blaster`) run against it measures the client half of hop
//! 3 alone (framing, credit accounting, the client's own thread choreography)
//! with the edge and everything behind it removed. Subtracting this from the
//! same driver against the real `Edge` isolates hop 2.
//!
//! The handshake and the RESPONSE shape mirror the real edge exactly
//! (`uc_gateway::edge`'s `handshake` and its completion path), including the
//! batching discipline: one blocking read pulls whatever the kernel has,
//! every frame in that burst is answered, and the answers go out in ONE
//! `write_all_bytes`.

use std::io::Write as _;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use uc_remote::conn::FramedConn;
use uc_remote::frame::{
    encode_frame, FrameType, Hello, HelloOk, HelloRefused, Header, ResponseMeta, FLAG_IS_QUERY,
    HELLO_REFUSED_APP_ID, HELLO_REFUSED_VERSION, PROTOCOL_VERSION,
};

/// Budget for the client's HELLO, matching the real edge's handshake timeout.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// Socket read timeout for a live connection: small enough that an idle
/// connection still ticks, large enough that a busy one never pays for it.
const READ_TICK: Duration = Duration::from_millis(200);
/// Mid-frame stall budget for a live connection (the `max_stall` contract).
const MAX_STALL: Duration = Duration::from_secs(30);

#[derive(clap::Args)]
pub struct Args {
    /// Address to listen on.
    #[arg(long)]
    pub listen: SocketAddr,
    /// `app_id` a HELLO must carry, or the handshake is refused.
    #[arg(long, default_value = "hop-bench")]
    pub app_id: String,
    /// Credits advertised at HELLO_OK and re-advertised on every RESPONSE.
    #[arg(long, default_value_t = 4096)]
    pub credits: u32,
    /// Bytes of response body (after the 20-byte `ResponseMeta`).
    #[arg(long, default_value_t = 8)]
    pub response_body: usize,
    /// Node id advertised as the leader in HELLO_OK.
    #[arg(long, default_value_t = 0)]
    pub node_id: u32,
}

struct Shared {
    app_id: String,
    credits: u32,
    body: Vec<u8>,
    node_id: u32,
    listen: String,
    conns: AtomicU64,
    responses: AtomicU64,
}

pub fn run(a: Args) -> anyhow::Result<()> {
    let listener = TcpListener::bind(a.listen)?;
    let local = listener.local_addr()?;
    let shared = Arc::new(Shared {
        app_id: a.app_id,
        credits: a.credits,
        body: vec![0xCDu8; a.response_body],
        node_id: a.node_id,
        listen: local.to_string(),
        conns: AtomicU64::new(0),
        responses: AtomicU64::new(0),
    });

    println!("hop_bench dummy-edge up on {local}; parking (killed externally)");
    println!("READY");
    let _ = std::io::stdout().flush();

    {
        let shared = Arc::clone(&shared);
        std::thread::Builder::new().name("hb-edge-stats".into()).spawn(move || {
            let mut last = 0u64;
            let mut next = Instant::now() + Duration::from_secs(1);
            loop {
                let now = Instant::now();
                if now < next {
                    std::thread::sleep(next - now);
                }
                next += Duration::from_secs(1);
                let total = shared.responses.load(Ordering::Relaxed);
                let live = shared.conns.load(Ordering::Relaxed);
                println!("dummy-edge: conns={live} resp/s={}", total - last);
                let _ = std::io::stdout().flush();
                last = total;
            }
        })?;
    }

    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let shared = Arc::clone(&shared);
        std::thread::Builder::new().name("hb-edge-conn".into()).spawn(move || {
            shared.conns.fetch_add(1, Ordering::Relaxed);
            serve(&shared, stream);
            shared.conns.fetch_sub(1, Ordering::Relaxed);
        })?;
    }
    Ok(())
}

fn serve(shared: &Shared, stream: std::net::TcpStream) {
    let Ok(mut fc) = FramedConn::new(stream) else { return };
    if fc.set_read_timeout(Some(HANDSHAKE_TIMEOUT)).is_err() {
        return;
    }
    if !handshake(shared, &mut fc) {
        return;
    }
    if fc.set_read_timeout(Some(READ_TICK)).is_err() {
        return;
    }

    // The batching discipline of the real edge's driver: drain everything one
    // wake delivered, then flush all the answers in one write.
    let mut out: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut acked_seq = 0u64;
    loop {
        out.clear();
        let mut answered = 0u64;
        match fc.read_frame_buffered(MAX_STALL) {
            // Idle tick at a frame boundary.
            Ok(None) => continue,
            Ok(Some((h, _payload))) => handle(shared, &mut out, &mut acked_seq, &mut answered, h),
            Err(_) => return,
        }
        loop {
            match fc.next_buffered() {
                Ok(Some((h, _payload))) => {
                    handle(shared, &mut out, &mut acked_seq, &mut answered, h)
                }
                Ok(None) => break,
                Err(_) => return,
            }
        }
        if !out.is_empty() && fc.write_all_bytes(&out).is_err() {
            return;
        }
        shared.responses.fetch_add(answered, Ordering::Relaxed);
    }
}

/// Read the client's HELLO, check it the way the real edge does (version
/// first, then `app_id`), and answer HELLO_OK.
fn handshake(shared: &Shared, fc: &mut FramedConn) -> bool {
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    let (h, payload) = loop {
        if Instant::now() >= deadline {
            return false;
        }
        match fc.read_frame(HANDSHAKE_TIMEOUT) {
            Ok(Some(f)) => break f,
            Ok(None) => continue,
            Err(_) => return false,
        }
    };
    if h.ty != FrameType::Hello {
        return false;
    }
    if h.version != PROTOCOL_VERSION {
        refuse(fc, HELLO_REFUSED_VERSION, &format!("dummy edge speaks v{PROTOCOL_VERSION}"));
        return false;
    }
    let Ok(hello) = Hello::decode(&payload) else {
        refuse(fc, HELLO_REFUSED_APP_ID, "malformed HELLO payload");
        return false;
    };
    if hello.app_id != shared.app_id {
        refuse(fc, HELLO_REFUSED_APP_ID, &shared.app_id);
        return false;
    }
    let mut out = Vec::new();
    HelloOk {
        credits: shared.credits,
        leader: Some(shared.node_id),
        leader_addr: &shared.listen,
    }
    .encode(&mut out);
    fc.write_frame(
        Header {
            ty: FrameType::HelloOk,
            flags: 0,
            version: PROTOCOL_VERSION,
            client_id: h.client_id,
            seq: h.seq,
        },
        &out,
    )
    .is_ok()
}

fn refuse(fc: &mut FramedConn, reason: u8, detail: &str) {
    let mut out = Vec::new();
    HelloRefused { reason, detail }.encode(&mut out);
    let _ = fc.write_frame(
        Header {
            ty: FrameType::HelloRefused,
            flags: 0,
            version: PROTOCOL_VERSION,
            client_id: 0,
            seq: 0,
        },
        &out,
    );
}

/// Answer one request frame into `out` (nothing is written to the socket here
/// — the caller flushes the whole batch).
fn handle(shared: &Shared, out: &mut Vec<u8>, acked_seq: &mut u64, answered: &mut u64, h: Header) {
    match h.ty {
        FrameType::Submit | FrameType::Query => {
            *acked_seq = (*acked_seq).max(h.seq);
            let mut payload = Vec::with_capacity(ResponseMeta::LEN + shared.body.len());
            ResponseMeta { credits: shared.credits, acked_seq: *acked_seq, position: h.seq }
                .encode(&mut payload);
            payload.extend_from_slice(&shared.body);
            let flags = if h.ty == FrameType::Query { FLAG_IS_QUERY } else { 0 };
            encode_frame(
                out,
                Header {
                    ty: FrameType::Response,
                    flags,
                    version: PROTOCOL_VERSION,
                    client_id: h.client_id,
                    seq: h.seq,
                },
                &payload,
            );
            *answered += 1;
        }
        FrameType::Ping => {
            encode_frame(
                out,
                Header {
                    ty: FrameType::Pong,
                    flags: 0,
                    version: PROTOCOL_VERSION,
                    client_id: h.client_id,
                    seq: h.seq,
                },
                &[],
            );
        }
        _ => {}
    }
}
