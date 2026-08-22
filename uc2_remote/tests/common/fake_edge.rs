// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! An in-process fake edge that speaks the remote protocol v1.
//!
//! It is deliberately *not* a mock: it decodes real frames with
//! [`uc2_remote::frame`], answers with real frames, and records what it saw, so
//! the client tests assert on wire behaviour rather than on call expectations.
//! No cluster, no `uc2_gateway` — just enough edge to exercise credits,
//! redirects, retries, and connection loss.
//!
//! Each connection runs two threads: a *handler* (reads frames, records them,
//! queues an action) and a *responder* (writes the answers back after a small
//! delay). The split is what makes the credit assertion meaningful: with a
//! delay between arrival and answer, the client's pipelining is visible as a
//! genuine count of unanswered requests, and the responder decrements that
//! count *before* writing, so the count can never be inflated by a response
//! that is already on the wire.

use std::collections::VecDeque;
use std::io::ErrorKind;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use uc2_remote::conn::FramedConn;
use uc2_remote::frame::*;

/// How the fake edge should answer.
#[derive(Clone, Debug)]
pub struct Behaviour {
    /// Credits advertised in `HELLO_OK` and in every `RESPONSE`/`STATUS`.
    pub credits: u32,
    /// Refuse `HELLO` with this reason instead of accepting it.
    pub refuse_hello: Option<u8>,
    /// Answer every SUBMIT/QUERY with `REDIRECT{node 2, addr}`.
    pub redirect_all_to: Option<String>,
    /// Answer every SUBMIT/QUERY with a `REDIRECT` naming *this edge's own*
    /// address — the "elected but not serving" hint an edge can legitimately
    /// produce. The client must not chase it in a loop.
    pub redirect_to_self: bool,
    /// Answer the first request on the first connection with
    /// `RETRY{NOT_SERVING, 1000}`, then behave normally.
    pub retry_once: bool,
    /// Answer the first request on the first connection with `UNKNOWN`.
    pub unknown_once: bool,
    /// Answer the first request on the first connection with
    /// `RETRY{PAYLOAD_TOO_LARGE, 0}`.
    pub payload_too_large_once: bool,
    /// Drop the first connection after reading one request, without answering.
    pub drop_after_first_request: bool,
    /// Answer every request with `FLAG_EXPIRED | FLAG_ENVELOPED`.
    pub expired: bool,
    /// Accept `HELLO`, answer `HELLO_OK`, then go silent: read every frame and
    /// answer nothing (not even `PING`), keeping the socket open. The client
    /// must notice via `dead_after`, not via an error.
    pub hang: bool,
    /// Delay between a request arriving and its answer being written.
    pub delay: Duration,
}

impl Default for Behaviour {
    fn default() -> Self {
        Behaviour {
            credits: 2,
            refuse_hello: None,
            redirect_all_to: None,
            redirect_to_self: false,
            retry_once: false,
            unknown_once: false,
            payload_too_large_once: false,
            drop_after_first_request: false,
            expired: false,
            hang: false,
            delay: Duration::from_millis(1),
        }
    }
}

/// What the edge actually saw, for assertions.
#[derive(Default)]
pub struct Observed {
    /// Accepted connections.
    pub conns: AtomicU32,
    /// `HELLO` frames read.
    pub hellos: AtomicU32,
    /// Every SUBMIT/QUERY `seq`, in arrival order, across all connections.
    pub seqs: Mutex<Vec<u64>>,
    /// The high-water mark of unanswered requests (the credit assertion).
    pub max_unanswered: AtomicU32,
    unanswered: AtomicU32,
}

impl Observed {
    fn arrived(&self, seq: u64) {
        self.seqs.lock().unwrap().push(seq);
        let n = self.unanswered.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_unanswered.fetch_max(n, Ordering::SeqCst);
    }

    fn answering(&self) {
        self.unanswered.fetch_sub(1, Ordering::SeqCst);
    }

    /// The observed seqs with consecutive duplicates collapsed — a re-send after
    /// a redirect or a drop legitimately repeats a seq.
    pub fn seq_order(&self) -> Vec<u64> {
        let mut out: Vec<u64> = Vec::new();
        for s in self.seqs.lock().unwrap().iter() {
            if out.last() != Some(s) {
                out.push(*s);
            }
        }
        out
    }

    pub fn seq_count(&self) -> usize {
        self.seqs.lock().unwrap().len()
    }
}

/// A running fake edge. Dropping it stops the acceptor and joins every
/// connection thread.
pub struct FakeEdge {
    pub addr: String,
    pub observed: Arc<Observed>,
    stop: Arc<AtomicBool>,
    acceptor: Option<JoinHandle<()>>,
}

impl FakeEdge {
    pub fn spawn(b: Behaviour) -> FakeEdge {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().unwrap().to_string();
        listener.set_nonblocking(true).unwrap();
        let observed = Arc::new(Observed::default());
        let stop = Arc::new(AtomicBool::new(false));
        let (o, s) = (observed.clone(), stop.clone());
        let acceptor = thread::spawn(move || {
            let mut conns: Vec<JoinHandle<()>> = Vec::new();
            let first = Arc::new(AtomicBool::new(true));
            while !s.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((sock, _)) => {
                        sock.set_nonblocking(false).unwrap();
                        o.conns.fetch_add(1, Ordering::SeqCst);
                        let is_first = first.swap(false, Ordering::SeqCst);
                        let (b, o, s) = (b.clone(), o.clone(), s.clone());
                        conns.push(thread::spawn(move || serve(sock, b, o, s, is_first)));
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
            for c in conns {
                let _ = c.join();
            }
        });
        FakeEdge { addr, observed, stop, acceptor: Some(acceptor) }
    }

    fn stop_inner(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.acceptor.take() {
            let _ = h.join();
        }
    }
}

impl Drop for FakeEdge {
    fn drop(&mut self) {
        self.stop_inner();
    }
}

/// One queued answer.
enum Action {
    Respond { seq: u64, is_query: bool, payload: Vec<u8> },
    Redirect { seq: u64, addr: String },
    Retry { seq: u64, reason: u8, after_us: u32 },
    Unknown { seq: u64 },
    Pong,
    DropConn,
}

type Queue = Arc<(Mutex<VecDeque<Action>>, Condvar)>;

fn serve(sock: TcpStream, b: Behaviour, o: Arc<Observed>, stop: Arc<AtomicBool>, is_first: bool) {
    let mut rd = match FramedConn::new(sock) {
        Ok(c) => c,
        Err(_) => return,
    };
    rd.set_read_timeout(Some(Duration::from_millis(20))).unwrap();
    let mut wr = match rd.try_clone() {
        Ok(c) => c,
        Err(_) => return,
    };

    // --- HELLO / HELLO_OK, written inline so the responder thread is the only
    // other writer and never races with it.
    let (h, _payload) = loop {
        match rd.read_frame() {
            Ok(Some(f)) => break f,
            Ok(None) => {
                if stop.load(Ordering::SeqCst) {
                    return;
                }
            }
            Err(_) => return,
        }
    };
    if h.ty != FrameType::Hello {
        return;
    }
    o.hellos.fetch_add(1, Ordering::SeqCst);
    let client_id = h.client_id;
    let mut out = Vec::new();
    if let Some(reason) = b.refuse_hello {
        HelloRefused { reason, detail: "refused by the fake edge" }.encode(&mut out);
        let _ = wr.write_frame(hdr(FrameType::HelloRefused, 0, client_id, 0), &out);
        return;
    }
    let self_addr = rd.local_addr().map(|a| a.to_string()).unwrap_or_default();
    HelloOk { credits: b.credits, leader: Some(1), leader_addr: &self_addr }.encode(&mut out);
    if wr.write_frame(hdr(FrameType::HelloOk, 0, client_id, 0), &out).is_err() {
        return;
    }

    // --- responder thread
    let q: Queue = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
    let done = Arc::new(AtomicBool::new(false));
    let (qq, oo, ss, bb, dd) = (q.clone(), o.clone(), stop.clone(), b.clone(), done.clone());
    let responder = thread::spawn(move || respond(wr, qq, oo, ss, dd, bb, client_id));

    // --- request loop
    let mut used_once = false;
    loop {
        match rd.read_frame() {
            Ok(Some((h, payload))) => match h.ty {
                FrameType::Submit | FrameType::Query => {
                    o.arrived(h.seq);
                    if b.hang {
                        continue;
                    }
                    let once = is_first && !used_once;
                    let action = if b.redirect_to_self {
                        Action::Redirect { seq: h.seq, addr: self_addr.clone() }
                    } else if let Some(addr) = &b.redirect_all_to {
                        Action::Redirect { seq: h.seq, addr: addr.clone() }
                    } else if once && b.drop_after_first_request {
                        used_once = true;
                        Action::DropConn
                    } else if once && b.retry_once {
                        used_once = true;
                        Action::Retry { seq: h.seq, reason: RETRY_NOT_SERVING, after_us: 1000 }
                    } else if once && b.unknown_once {
                        used_once = true;
                        Action::Unknown { seq: h.seq }
                    } else if once && b.payload_too_large_once {
                        used_once = true;
                        Action::Retry { seq: h.seq, reason: RETRY_PAYLOAD_TOO_LARGE, after_us: 0 }
                    } else {
                        let mut bytes = payload.to_vec();
                        bytes.reverse();
                        Action::Respond {
                            seq: h.seq,
                            is_query: h.ty == FrameType::Query,
                            payload: bytes,
                        }
                    };
                    let stop_after = matches!(action, Action::DropConn);
                    q.0.lock().unwrap().push_back(action);
                    q.1.notify_all();
                    if stop_after {
                        break;
                    }
                }
                FrameType::Ping if b.hang => {}
                FrameType::Ping => {
                    q.0.lock().unwrap().push_back(Action::Pong);
                    q.1.notify_all();
                }
                _ => {}
            },
            Ok(None) => {
                if stop.load(Ordering::SeqCst) {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    done.store(true, Ordering::SeqCst);
    q.1.notify_all();
    let _ = responder.join();
    rd.shutdown();
}

#[allow(clippy::too_many_arguments)]
fn respond(
    mut wr: FramedConn,
    q: Queue,
    o: Arc<Observed>,
    stop: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    b: Behaviour,
    client_id: u64,
) {
    let mut out = Vec::new();
    loop {
        let action = {
            let mut g = q.0.lock().unwrap();
            loop {
                if let Some(a) = g.pop_front() {
                    break Some(a);
                }
                if stop.load(Ordering::SeqCst) || done.load(Ordering::SeqCst) {
                    break None;
                }
                let (ng, _) = q.1.wait_timeout(g, Duration::from_millis(5)).unwrap();
                g = ng;
            }
        };
        let Some(action) = action else { return };
        // Sleep in slices so teardown never waits out a long delay.
        let mut left = b.delay;
        while !left.is_zero() {
            if stop.load(Ordering::SeqCst) || done.load(Ordering::SeqCst) {
                return;
            }
            let slice = left.min(Duration::from_millis(5));
            thread::sleep(slice);
            left -= slice;
        }
        out.clear();
        let write = match action {
            Action::DropConn => {
                o.answering();
                wr.shutdown();
                return;
            }
            Action::Respond { seq, is_query, payload } => {
                let mut flags = 0u8;
                if is_query {
                    flags |= FLAG_IS_QUERY;
                }
                if b.expired {
                    flags |= FLAG_EXPIRED | FLAG_ENVELOPED;
                }
                ResponseMeta { credits: b.credits, acked_seq: seq, position: seq * 64 }
                    .encode(&mut out);
                out.extend_from_slice(&payload);
                o.answering();
                wr.write_frame(hdr(FrameType::Response, flags, client_id, seq), &out)
            }
            Action::Redirect { seq, addr } => {
                Leader { node_id: 2, addr: &addr }.encode(&mut out);
                o.answering();
                wr.write_frame(hdr(FrameType::Redirect, 0, client_id, seq), &out)
            }
            Action::Retry { seq, reason, after_us } => {
                Retry { reason, retry_after_us: after_us }.encode(&mut out);
                o.answering();
                wr.write_frame(hdr(FrameType::Retry, 0, client_id, seq), &out)
            }
            Action::Pong => wr.write_frame(hdr(FrameType::Pong, 0, client_id, 0), &[]),
            Action::Unknown { seq } => {
                o.answering();
                wr.write_frame(hdr(FrameType::Unknown, 0, client_id, seq), &[])
            }
        };
        if write.is_err() {
            return;
        }
    }
}

fn hdr(ty: FrameType, flags: u8, client_id: u64, seq: u64) -> Header {
    Header { ty, flags, version: PROTOCOL_VERSION, client_id, seq }
}
