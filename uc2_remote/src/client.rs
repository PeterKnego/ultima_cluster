// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! [`RemoteClient`] — the Rust remote client: pipelined, credit-gated,
//! redirect-following, with ordered re-send across failover.
//!
//! # The promise (design spec §4.5)
//!
//! Every [`RemoteClient::submit`] / [`RemoteClient::query`] ends in **exactly
//! one** resolution: `Ok(RemoteResponse)`, or `Err` of [`RemoteError::Expired`]
//! / [`RemoteError::Unknown`] / [`RemoteError::PayloadTooLarge`] /
//! [`RemoteError::TimedOut`] / [`RemoteError::Closed`]. `REDIRECT`,
//! `LEADER_CHANGED`, `RETRY` and connection loss are *not* outcomes the caller
//! sees — the client reconnects, re-`HELLO`s, and re-sends every unanswered
//! `seq` in order. With the edge's session envelope on, a re-send is safe by
//! construction (the `Sessioned` adapter answers `fresh` / `replayed` /
//! `expired`); with it off, a re-sent write may duplicate — that is the
//! documented cost of turning the envelope off, not a client bug.
//!
//! # Shape
//!
//! One background **reader thread** owns the read half of the current
//! connection and is the *only* thread that reconnects — so there is no
//! reconnect race to lose. Everything else lives under a single
//! `Mutex<State>` + `Condvar`:
//!
//! - the write half of the connection (so frame order == seq order, and a
//!   half-written frame can only ever be followed by a discarded connection),
//! - `credits` / `acked_seq` (the flow-control pair) and `pending` (the
//!   unanswered requests, keyed by `seq`, holding their bytes for re-send).
//!
//! **One lock, one order.** The writer lives *inside* `State` rather than
//! behind its own mutex: with two locks, `submit` (which must hold `State` to
//! allocate a `seq` and wait on credits) and the reader thread (which must hold
//! `State` to release credits) can only be made deadlock-free by an ordering
//! that also has to admit the reconnect path. One lock removes the question.
//! The cost — a socket write happens under the lock — is bounded by a socket
//! *write timeout*: a write that cannot complete within `request_timeout` fails,
//! the connection is discarded, and the reader re-sends on a fresh one. That is
//! what stops the one real cycle (peer stops reading → our writer blocks under
//! the lock → our reader cannot drain responses → peer never resumes).
//!
//! Credit discipline (spec §4.2): the client may have at most `credits`
//! unanswered `seq`s beyond `acked_seq`, i.e. it only sends `seq` while
//! `seq <= acked_seq + credits`. `HELLO_OK` sets the initial credits; every
//! `RESPONSE` and `STATUS` updates the pair and wakes anyone waiting.

use std::collections::BTreeMap;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::conn::FramedConn;
use crate::error::RemoteError;
use crate::frame::{
    FrameType, Header, Hello, HelloOk, HelloRefused, Leader, ResponseMeta, Retry, Status,
    FLAG_EXPIRED, FLAG_LINEARIZABLE, FLAG_REPLAYED, HEADER_LEN, MAX_FRAME_LEN, PROTOCOL_VERSION,
    RETRY_PAYLOAD_TOO_LARGE,
};

/// How often the reader wakes up when the socket is quiet, to time out
/// requests and to notice `shutdown`.
const SWEEP_INTERVAL: Duration = Duration::from_millis(25);
/// A `RETRY` hint is honoured, but never for longer than this.
const MAX_RETRY_SLEEP: Duration = Duration::from_secs(1);
/// A `RETRY{retry_after_us: 0}` still backs off this much, so a retry loop can
/// never become a spin.
const MIN_RETRY_SLEEP: Duration = Duration::from_micros(100);
/// Backoff after a full pass over `members` failed to connect.
const RECONNECT_BACKOFF_START: Duration = Duration::from_millis(5);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_millis(500);
/// `REDIRECT` hops followed during one connect scan.
const MAX_REDIRECT_HOPS: usize = 8;

// ---------------------------------------------------------------- config

/// How to reach a cluster's edges, and how hard to try.
#[derive(Clone, Debug)]
pub struct RemoteConfig {
    /// Must match the edge's `app_id`, or `HELLO` is refused.
    pub app_id: String,
    /// Gateway addresses, `"host:port"`. Tried in order at connect, and
    /// round-robin (starting after the current one) on connection loss.
    pub members: Vec<String>,
    /// The client's stable identity. `None` picks a random one; supply your own
    /// (persisted) id if you want the edge's session dedup to survive a client
    /// restart.
    pub client_id: Option<u64>,
    /// A local cap on unanswered requests, applied on top of the edge's credits.
    pub max_inflight: u32,
    /// End-to-end budget for one request, across re-sends and reconnects.
    /// Also the socket write timeout.
    pub request_timeout: Duration,
    /// Per-address TCP connect + `HELLO` budget.
    pub connect_timeout: Duration,
    /// `UNKNOWN` means "may or may not have committed". `true` (the default)
    /// re-sends — correct with the edge's session envelope on, and the only way
    /// to get a definite answer. `false` surfaces [`RemoteError::Unknown`].
    pub resend_on_unknown: bool,
}

impl Default for RemoteConfig {
    fn default() -> Self {
        RemoteConfig {
            app_id: String::new(),
            members: Vec::new(),
            client_id: None,
            max_inflight: 1024,
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(2),
            resend_on_unknown: true,
        }
    }
}

/// Counters for what the client had to do to keep its promise.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RemoteStats {
    /// `REDIRECT` frames received.
    pub redirects: u64,
    /// `LEADER_CHANGED` frames received.
    pub leader_changes: u64,
    /// Reconnect episodes: a connection loss, `REDIRECT` or `LEADER_CHANGED`
    /// that forced a fresh handshake (one per episode, however many addresses
    /// it had to try).
    pub reconnects: u64,
    /// Frames written for a request that had already been written once.
    pub resends: u64,
    /// `RETRY` frames honoured (excluding `PAYLOAD_TOO_LARGE`, which is final).
    pub retries: u64,
    /// `UNKNOWN` frames received.
    pub unknown: u64,
    /// Responses flagged `EXPIRED` by the edge's session window.
    pub expired: u64,
    /// The largest `credits` value any frame ever advertised.
    pub max_credits_seen: u32,
}

/// One completed request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteResponse {
    /// The log position the command was applied at (`0` for a query).
    pub position: u64,
    /// The state machine's response bytes.
    pub bytes: Bytes,
    /// The edge's session dedup answered from its cache: this exact `seq` had
    /// already been applied. The write happened exactly once.
    pub replayed: bool,
}

/// A handle on one outstanding request.
pub struct Ticket {
    rx: Receiver<Result<RemoteResponse, RemoteError>>,
}

impl Ticket {
    /// Block until the request resolves. Only the client's own `request_timeout`
    /// or `shutdown` can end the wait without an answer.
    pub fn wait(self) -> Result<RemoteResponse, RemoteError> {
        self.rx.recv().unwrap_or(Err(RemoteError::Closed))
    }

    /// Like [`Ticket::wait`] with a caller-side bound. Note that giving up here
    /// abandons the request: the client stops tracking it once it resolves.
    pub fn wait_timeout(self, d: Duration) -> Result<RemoteResponse, RemoteError> {
        match self.rx.recv_timeout(d) {
            Ok(r) => r,
            Err(RecvTimeoutError::Timeout) => Err(RemoteError::TimedOut),
            Err(RecvTimeoutError::Disconnected) => Err(RemoteError::Closed),
        }
    }
}

// ---------------------------------------------------------------- internals

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Submit,
    Query { linearizable: bool },
}

impl Kind {
    fn ty(self) -> FrameType {
        match self {
            Kind::Submit => FrameType::Submit,
            Kind::Query { .. } => FrameType::Query,
        }
    }

    fn flags(self) -> u8 {
        match self {
            Kind::Query { linearizable: true } => FLAG_LINEARIZABLE,
            _ => 0,
        }
    }
}

struct Pending {
    kind: Kind,
    /// Kept so the request can be re-sent byte-for-byte after a failover.
    payload: Bytes,
    tx: SyncSender<Result<RemoteResponse, RemoteError>>,
    created: Instant,
    attempts: u32,
    /// `false` = written to no live connection yet (fresh, or invalidated by a
    /// reconnect / `RETRY` / `UNKNOWN`); the pump owns getting it onto the wire.
    sent: bool,
}

#[derive(Default)]
struct Stats {
    redirects: AtomicU64,
    leader_changes: AtomicU64,
    reconnects: AtomicU64,
    resends: AtomicU64,
    retries: AtomicU64,
    unknown: AtomicU64,
    expired: AtomicU64,
    max_credits_seen: AtomicU32,
}

struct State {
    /// The write half of the current connection; `None` while disconnected.
    conn: Option<FramedConn>,
    credits: u32,
    acked_seq: u64,
    next_seq: u64,
    pending: BTreeMap<u64, Pending>,
    leader: Option<(u32, String)>,
    /// Lower bound on the lowest `seq` that still needs writing — `u64::MAX`
    /// means "everything pending is on the wire". Keeps the pump from scanning
    /// the whole in-flight window on every frame.
    resend_from: u64,
    /// Index into `cfg.members` that the round-robin resumes after.
    member_idx: usize,
    /// The address currently connected to (may be a redirect target that is not
    /// in `members`).
    current_addr: String,
    closed: bool,
}

struct Inner {
    cfg: RemoteConfig,
    client_id: u64,
    state: Mutex<State>,
    cv: Condvar,
    stats: Stats,
    /// xorshift64 state for retry jitter (`rand` is not a dependency here — this
    /// crate is meant to be trivially re-implementable in another language).
    rng: AtomicU64,
}

/// What the reader thread should do after a frame.
enum Act {
    Continue,
    /// Reconnect, preferring this address if given.
    Reconnect(Option<String>),
    Stop,
}

// ---------------------------------------------------------------- client

/// A connected remote client. `Send + Sync`; share it behind an `Arc` or a
/// reference — every method takes `&self`.
pub struct RemoteClient {
    inner: Arc<Inner>,
    reader: Mutex<Option<JoinHandle<()>>>,
}

impl RemoteClient {
    /// Connect to the first reachable member, following a `REDIRECT` at `HELLO`.
    ///
    /// Fails with [`RemoteError::HelloRefused`] if an edge answers the handshake
    /// with a refusal (a wrong `app_id` or protocol version is not something
    /// another member would answer differently), and with
    /// [`RemoteError::NoMembersReachable`] only after a full pass over `members`.
    pub fn connect(cfg: RemoteConfig) -> Result<Self, RemoteError> {
        if cfg.members.is_empty() {
            return Err(RemoteError::NoMembersReachable);
        }
        let client_id = cfg.client_id.unwrap_or_else(random_u64);
        let (conn, info, idx, addr) = dial(&cfg, client_id, None, 0)?;
        let read_half = conn.try_clone()?;
        read_half.set_read_timeout(Some(SWEEP_INTERVAL))?;
        conn.set_write_timeout(Some(cfg.request_timeout))?;

        let stats = Stats::default();
        stats.max_credits_seen.store(info.credits, Ordering::Relaxed);
        let inner = Arc::new(Inner {
            client_id,
            state: Mutex::new(State {
                conn: Some(conn),
                credits: info.credits,
                acked_seq: 0,
                next_seq: 0,
                pending: BTreeMap::new(),
                resend_from: u64::MAX,
                leader: info.leader,
                member_idx: idx,
                current_addr: addr,
                closed: false,
            }),
            cv: Condvar::new(),
            stats,
            rng: AtomicU64::new(client_id | 1),
            cfg,
        });

        let reader_inner = Arc::clone(&inner);
        let handle = thread::Builder::new()
            .name("uc2-remote-rx".into())
            .spawn(move || reader_loop(reader_inner, read_half))?;
        Ok(RemoteClient { inner, reader: Mutex::new(Some(handle)) })
    }

    /// Submit a command. Blocks while the edge's credits are exhausted (or the
    /// local `max_inflight` cap is reached), and gives up with
    /// [`RemoteError::TimedOut`] if credits never reopen within
    /// `request_timeout`.
    pub fn submit(&self, cmd: &[u8]) -> Result<Ticket, RemoteError> {
        self.enqueue(Kind::Submit, cmd)
    }

    /// Ask a question. `linearizable` goes through the node's read barrier;
    /// otherwise it is a snapshot read served by whichever replica the edge sits
    /// on. Same credit accounting as [`RemoteClient::submit`].
    pub fn query(&self, q: &[u8], linearizable: bool) -> Result<Ticket, RemoteError> {
        self.enqueue(Kind::Query { linearizable }, q)
    }

    /// The identity this client asserts in every frame — the key the edge's
    /// session dedup is per.
    pub fn client_id(&self) -> u64 {
        self.inner.client_id
    }

    /// The leader the current edge last told us about, if any.
    pub fn leader(&self) -> Option<(u32, String)> {
        self.inner.state.lock().unwrap().leader.clone()
    }

    pub fn stats(&self) -> RemoteStats {
        let s = &self.inner.stats;
        RemoteStats {
            redirects: s.redirects.load(Ordering::Relaxed),
            leader_changes: s.leader_changes.load(Ordering::Relaxed),
            reconnects: s.reconnects.load(Ordering::Relaxed),
            resends: s.resends.load(Ordering::Relaxed),
            retries: s.retries.load(Ordering::Relaxed),
            unknown: s.unknown.load(Ordering::Relaxed),
            expired: s.expired.load(Ordering::Relaxed),
            max_credits_seen: s.max_credits_seen.load(Ordering::Relaxed),
        }
    }

    /// Close the connection and fail every outstanding request with
    /// [`RemoteError::Closed`]. Dropping the client does the same.
    pub fn shutdown(self) {
        self.close();
    }

    fn close(&self) {
        {
            let mut st = self.inner.state.lock().unwrap();
            if !st.closed {
                st.closed = true;
                if let Some(c) = st.conn.take() {
                    c.shutdown();
                }
                let pending = std::mem::take(&mut st.pending);
                for (_, p) in pending {
                    let _ = p.tx.send(Err(RemoteError::Closed));
                }
                self.inner.cv.notify_all();
            }
        }
        let handle = self.reader.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }

    fn enqueue(&self, kind: Kind, payload: &[u8]) -> Result<Ticket, RemoteError> {
        if HEADER_LEN + payload.len() > MAX_FRAME_LEN as usize {
            return Err(RemoteError::PayloadTooLarge);
        }
        let inner = &self.inner;
        let deadline = Instant::now() + inner.cfg.request_timeout;
        let mut st = inner.state.lock().unwrap();
        loop {
            if st.closed {
                return Err(RemoteError::Closed);
            }
            // The credit discipline, checked before a seq is even allocated:
            // the next seq must satisfy `seq <= acked_seq + credits`.
            let next = st.next_seq + 1;
            let credit_ok = next <= st.acked_seq.saturating_add(st.credits as u64);
            let inflight_ok = st.pending.len() < inner.cfg.max_inflight as usize;
            if credit_ok && inflight_ok {
                break;
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(RemoteError::TimedOut);
            }
            let (guard, _) = inner.cv.wait_timeout(st, deadline - now).unwrap();
            st = guard;
        }
        st.next_seq += 1;
        let seq = st.next_seq;
        let (tx, rx) = sync_channel(1);
        st.pending.insert(
            seq,
            Pending {
                kind,
                payload: Bytes::copy_from_slice(payload),
                tx,
                created: Instant::now(),
                attempts: 0,
                sent: false,
            },
        );
        st.resend_from = st.resend_from.min(seq);
        inner.pump(&mut st);
        Ok(Ticket { rx })
    }
}

impl std::fmt::Debug for RemoteClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let st = self.inner.state.lock().unwrap();
        f.debug_struct("RemoteClient")
            .field("client_id", &self.inner.client_id)
            .field("addr", &st.current_addr)
            .field("connected", &st.conn.is_some())
            .field("credits", &st.credits)
            .field("acked_seq", &st.acked_seq)
            .field("pending", &st.pending.len())
            .field("closed", &st.closed)
            .finish()
    }
}

impl Drop for RemoteClient {
    fn drop(&mut self) {
        self.close();
    }
}

// ---------------------------------------------------------------- inner

impl Inner {
    fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }

    fn note_credits(&self, credits: u32) {
        self.stats.max_credits_seen.fetch_max(credits, Ordering::Relaxed);
    }

    /// Write every unsent pending that credits allow, in `seq` order.
    ///
    /// Stops at the first request the credit window does not cover — a later
    /// `seq` can never be admissible when an earlier one is not, so this both
    /// respects the window and preserves order. A write failure discards the
    /// connection (leaving the request unsent); the reader thread notices the
    /// dead socket and reconnects.
    fn pump(&self, st: &mut State) {
        if st.conn.is_none() || st.resend_from == u64::MAX {
            return;
        }
        let State { conn, pending, acked_seq, credits, resend_from, .. } = &mut *st;
        let window = acked_seq.saturating_add(*credits as u64);
        let c = conn.as_mut().expect("checked above");
        let mut broken = false;
        let mut next_from = u64::MAX;
        for (seq, p) in pending.range_mut(*resend_from..) {
            if p.sent {
                continue;
            }
            if *seq > window {
                next_from = *seq;
                break;
            }
            let h = Header {
                ty: p.kind.ty(),
                flags: p.kind.flags(),
                version: PROTOCOL_VERSION,
                client_id: self.client_id,
                seq: *seq,
            };
            match c.write_frame(h, &p.payload) {
                Ok(()) => {
                    p.sent = true;
                    p.attempts += 1;
                    if p.attempts > 1 {
                        self.stats.resends.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(_) => {
                    next_from = *seq;
                    broken = true;
                    break;
                }
            }
        }
        *resend_from = next_from;
        if broken {
            // Wake the reader thread out of its blocking read; it reconnects.
            drop_conn(st);
        }
    }

    /// Fail every request older than `request_timeout`.
    fn sweep(&self) {
        let mut st = self.state.lock().unwrap();
        let budget = self.cfg.request_timeout;
        let stale: Vec<u64> = st
            .pending
            .iter()
            .filter(|(_, p)| p.created.elapsed() >= budget)
            .map(|(seq, _)| *seq)
            .collect();
        if stale.is_empty() {
            return;
        }
        for seq in stale {
            if let Some(p) = st.pending.remove(&seq) {
                let _ = p.tx.send(Err(RemoteError::TimedOut));
            }
        }
        self.cv.notify_all();
    }

    /// Update the flow-control pair from a frame that carries it, then push
    /// whatever the new window admits.
    fn credit_update(&self, st: &mut State, credits: u32, acked_seq: u64) {
        self.note_credits(credits);
        st.credits = credits;
        st.acked_seq = st.acked_seq.max(acked_seq);
        self.pump(st);
        self.cv.notify_all();
    }

    fn resolve(&self, st: &mut State, seq: u64, res: Result<RemoteResponse, RemoteError>) {
        if let Some(p) = st.pending.remove(&seq) {
            let _ = p.tx.send(res);
            self.cv.notify_all();
        }
    }

    /// Mark a request for re-send. Returns `false` if it is no longer pending
    /// (already resolved, or swept), in which case there is nothing to do.
    fn mark_unsent(&self, st: &mut State, seq: u64) -> bool {
        match st.pending.get_mut(&seq) {
            Some(p) => {
                p.sent = false;
                st.resend_from = st.resend_from.min(seq);
                true
            }
            None => false,
        }
    }

    fn on_frame(&self, h: Header, payload: Bytes) -> Act {
        match h.ty {
            FrameType::Response => {
                let Ok(meta) = ResponseMeta::decode(&payload) else { return Act::Continue };
                let body = payload.slice(ResponseMeta::LEN..);
                let mut st = self.state.lock().unwrap();
                if h.flags & FLAG_EXPIRED != 0 {
                    self.stats.expired.fetch_add(1, Ordering::Relaxed);
                    self.resolve(&mut st, h.seq, Err(RemoteError::Expired));
                } else {
                    self.resolve(
                        &mut st,
                        h.seq,
                        Ok(RemoteResponse {
                            position: meta.position,
                            bytes: body,
                            replayed: h.flags & FLAG_REPLAYED != 0,
                        }),
                    );
                }
                self.credit_update(&mut st, meta.credits, meta.acked_seq);
                Act::Continue
            }
            FrameType::Status => {
                let Ok(s) = Status::decode(&payload) else { return Act::Continue };
                let mut st = self.state.lock().unwrap();
                self.credit_update(&mut st, s.credits, s.acked_seq);
                Act::Continue
            }
            FrameType::Retry => {
                let Ok(r) = Retry::decode(&payload) else { return Act::Continue };
                if r.reason == RETRY_PAYLOAD_TOO_LARGE {
                    // Terminal: the payload will not get smaller by being sent
                    // again.
                    let mut st = self.state.lock().unwrap();
                    self.resolve(&mut st, h.seq, Err(RemoteError::PayloadTooLarge));
                    return Act::Continue;
                }
                self.stats.retries.fetch_add(1, Ordering::Relaxed);
                {
                    let mut st = self.state.lock().unwrap();
                    if !self.mark_unsent(&mut st, h.seq) {
                        return Act::Continue;
                    }
                }
                thread::sleep(self.jittered(Duration::from_micros(r.retry_after_us as u64)));
                let mut st = self.state.lock().unwrap();
                self.pump(&mut st);
                Act::Continue
            }
            FrameType::Unknown => {
                self.stats.unknown.fetch_add(1, Ordering::Relaxed);
                let mut st = self.state.lock().unwrap();
                if self.cfg.resend_on_unknown {
                    if self.mark_unsent(&mut st, h.seq) {
                        self.pump(&mut st);
                    }
                } else {
                    self.resolve(&mut st, h.seq, Err(RemoteError::Unknown));
                }
                Act::Continue
            }
            FrameType::Redirect => {
                self.stats.redirects.fetch_add(1, Ordering::Relaxed);
                let Ok(l) = Leader::decode(&payload) else { return Act::Continue };
                let mut st = self.state.lock().unwrap();
                if !l.addr.is_empty() {
                    st.leader = Some((l.node_id, l.addr.to_string()));
                }
                // The request was refused, not answered: it must go out again.
                self.mark_unsent(&mut st, h.seq);
                if l.addr.is_empty() {
                    return Act::Reconnect(None);
                }
                if l.addr == st.current_addr {
                    // An edge redirecting to itself would spin; back off and
                    // re-send here instead. `request_timeout` bounds it.
                    drop(st);
                    thread::sleep(self.jittered(Duration::ZERO));
                    let mut st = self.state.lock().unwrap();
                    self.pump(&mut st);
                    return Act::Continue;
                }
                Act::Reconnect(Some(l.addr.to_string()))
            }
            FrameType::LeaderChanged => {
                self.stats.leader_changes.fetch_add(1, Ordering::Relaxed);
                let Ok(l) = Leader::decode(&payload) else { return Act::Continue };
                let mut st = self.state.lock().unwrap();
                if l.addr.is_empty() {
                    st.leader = None;
                    return Act::Reconnect(None);
                }
                st.leader = Some((l.node_id, l.addr.to_string()));
                if l.addr == st.current_addr {
                    // We are already on the new leader's edge: nothing to do,
                    // and reconnecting would only churn the in-flight window.
                    return Act::Continue;
                }
                Act::Reconnect(Some(l.addr.to_string()))
            }
            FrameType::HelloRefused => {
                // Mid-life refusal: not something a re-send or another member
                // fixes. Fail everything and stop.
                let (reason, detail) = match HelloRefused::decode(&payload) {
                    Ok(r) => (r.reason, r.detail.to_string()),
                    Err(_) => (0, String::new()),
                };
                self.fail_all_and_close(|| RemoteError::HelloRefused {
                    reason,
                    detail: detail.clone(),
                });
                Act::Stop
            }
            FrameType::Ping => {
                let pong = Header {
                    ty: FrameType::Pong,
                    flags: 0,
                    version: PROTOCOL_VERSION,
                    client_id: self.client_id,
                    seq: h.seq,
                };
                let mut st = self.state.lock().unwrap();
                let failed = match st.conn.as_mut() {
                    Some(c) => c.write_frame(pong, &[]).is_err(),
                    None => false,
                };
                if failed {
                    drop_conn(&mut st);
                }
                Act::Continue
            }
            // HELLO_OK arrives only during a handshake; the rest are client→edge
            // types the edge must never send. Ignore rather than tear down.
            _ => Act::Continue,
        }
    }

    fn fail_all_and_close(&self, mk: impl Fn() -> RemoteError) {
        let mut st = self.state.lock().unwrap();
        st.closed = true;
        drop_conn(&mut st);
        let pending = std::mem::take(&mut st.pending);
        for (_, p) in pending {
            let _ = p.tx.send(Err(mk()));
        }
        self.cv.notify_all();
    }

    /// Open a fresh connection (preferring `preferred`, else round-robin over
    /// `members` starting after the current one), re-`HELLO`, reset credits from
    /// `HELLO_OK`, and re-send every unanswered request in `seq` order.
    ///
    /// Returns `false` when the client was shut down and the reader must stop.
    /// Never gives up otherwise: a full failed pass over `members` backs off and
    /// tries again, while `sweep` keeps failing requests that run out of budget.
    fn reconnect(&self, preferred: Option<String>, rd: &mut FramedConn) -> bool {
        self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
        let mut preferred = preferred;
        let mut backoff = RECONNECT_BACKOFF_START;
        loop {
            if self.is_closed() {
                return false;
            }
            let start = {
                let mut st = self.state.lock().unwrap();
                drop_conn(&mut st);
                // Everything unanswered has to go out again on the new
                // connection, in order.
                for p in st.pending.values_mut() {
                    p.sent = false;
                }
                st.resend_from = st.pending.keys().next().copied().unwrap_or(u64::MAX);
                st.member_idx + 1
            };
            match dial(&self.cfg, self.client_id, preferred.as_deref(), start) {
                Ok((conn, info, idx, addr)) => {
                    let read_half = match conn
                        .try_clone()
                        .and_then(|r| r.set_read_timeout(Some(SWEEP_INTERVAL)).map(|_| r))
                    {
                        Ok(r) => r,
                        Err(_) => {
                            preferred = None;
                            continue;
                        }
                    };
                    let _ = conn.set_write_timeout(Some(self.cfg.request_timeout));
                    let mut st = self.state.lock().unwrap();
                    if st.closed {
                        conn.shutdown();
                        return false;
                    }
                    st.conn = Some(conn);
                    st.member_idx = idx;
                    st.current_addr = addr;
                    if info.leader.is_some() {
                        st.leader = info.leader;
                    }
                    self.note_credits(info.credits);
                    st.credits = info.credits;
                    self.pump(&mut st);
                    self.cv.notify_all();
                    *rd = read_half;
                    return true;
                }
                Err(RemoteError::HelloRefused { reason, detail }) => {
                    self.fail_all_and_close(|| RemoteError::HelloRefused {
                        reason,
                        detail: detail.clone(),
                    });
                    return false;
                }
                Err(_) => {
                    // Nothing reachable this pass. Keep the promise anyway:
                    // requests still time out on their own budget.
                    self.sweep();
                    preferred = None;
                    thread::sleep(backoff);
                    backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
                }
            }
        }
    }

    /// `base` plus up to 25% jitter, floored at [`MIN_RETRY_SLEEP`] and capped at
    /// [`MAX_RETRY_SLEEP`].
    fn jittered(&self, base: Duration) -> Duration {
        let base = base.clamp(MIN_RETRY_SLEEP, MAX_RETRY_SLEEP);
        let span = (base.as_micros() as u64 / 4).max(1);
        base + Duration::from_micros(self.next_rand() % span)
    }

    /// xorshift64 — enough for backoff jitter, and no dependency.
    fn next_rand(&self) -> u64 {
        let mut x = self.rng.load(Ordering::Relaxed);
        if x == 0 {
            x = 0x9E37_79B9_7F4A_7C15;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng.store(x, Ordering::Relaxed);
        x
    }
}

/// Discard the current connection, shutting the socket down so a peer thread
/// blocked in `read_frame` wakes up.
fn drop_conn(st: &mut State) {
    if let Some(c) = st.conn.take() {
        c.shutdown();
    }
}

fn reader_loop(inner: Arc<Inner>, mut rd: FramedConn) {
    loop {
        if inner.is_closed() {
            return;
        }
        match rd.read_frame() {
            Ok(Some((h, payload))) => match inner.on_frame(h, payload) {
                Act::Continue => {}
                Act::Stop => return,
                Act::Reconnect(preferred) => {
                    if !inner.reconnect(preferred, &mut rd) {
                        return;
                    }
                }
            },
            // Idle at a frame boundary: time out whatever ran out of budget.
            Ok(None) => inner.sweep(),
            Err(_) => {
                if inner.is_closed() || !inner.reconnect(None, &mut rd) {
                    return;
                }
            }
        }
    }
}

// ---------------------------------------------------------------- handshake

struct HelloInfo {
    credits: u32,
    leader: Option<(u32, String)>,
}

enum Dialed {
    Ok(FramedConn, HelloInfo),
    Redirect(String),
    Refused { reason: u8, detail: String },
    Failed,
}

/// Try `preferred` (if any) and then every member in round-robin order from
/// `start_idx`, following `REDIRECT`s at the handshake.
fn dial(
    cfg: &RemoteConfig,
    client_id: u64,
    preferred: Option<&str>,
    start_idx: usize,
) -> Result<(FramedConn, HelloInfo, usize, String), RemoteError> {
    let n = cfg.members.len();
    if n == 0 {
        return Err(RemoteError::NoMembersReachable);
    }
    let mut order: Vec<String> = Vec::with_capacity(n + 1);
    if let Some(p) = preferred {
        order.push(p.to_string());
    }
    for k in 0..n {
        order.push(cfg.members[(start_idx + k) % n].clone());
    }
    let mut hops = 0usize;
    let mut i = 0usize;
    while i < order.len() {
        let addr = order[i].clone();
        match dial_one(cfg, client_id, &addr) {
            Dialed::Ok(conn, info) => {
                let idx = cfg.members.iter().position(|m| *m == addr).unwrap_or(start_idx % n);
                return Ok((conn, info, idx, addr));
            }
            Dialed::Redirect(to) => {
                if hops < MAX_REDIRECT_HOPS && !order.contains(&to) {
                    hops += 1;
                    order.insert(i + 1, to);
                }
                i += 1;
            }
            Dialed::Refused { reason, detail } => {
                return Err(RemoteError::HelloRefused { reason, detail })
            }
            Dialed::Failed => i += 1,
        }
    }
    Err(RemoteError::NoMembersReachable)
}

fn dial_one(cfg: &RemoteConfig, client_id: u64, addr: &str) -> Dialed {
    let Some(sa) = addr.to_socket_addrs().ok().and_then(|mut it| it.next()) else {
        return Dialed::Failed;
    };
    let Ok(sock) = TcpStream::connect_timeout(&sa, cfg.connect_timeout) else {
        return Dialed::Failed;
    };
    let Ok(mut conn) = FramedConn::new(sock) else { return Dialed::Failed };
    if conn.set_read_timeout(Some(cfg.connect_timeout)).is_err()
        || conn.set_write_timeout(Some(cfg.connect_timeout)).is_err()
    {
        return Dialed::Failed;
    }
    let mut out = Vec::new();
    Hello { app_id: &cfg.app_id }.encode(&mut out);
    let hello = Header {
        ty: FrameType::Hello,
        flags: 0,
        version: PROTOCOL_VERSION,
        client_id,
        seq: 0,
    };
    if conn.write_frame(hello, &out).is_err() {
        return Dialed::Failed;
    }
    let deadline = Instant::now() + cfg.connect_timeout;
    loop {
        match conn.read_frame() {
            Ok(Some((h, payload))) => {
                return match h.ty {
                    FrameType::HelloOk => match HelloOk::decode(&payload) {
                        Ok(ok) => {
                            let leader = ok
                                .leader
                                .map(|id| (id, addr_or(ok.leader_addr, addr)));
                            Dialed::Ok(conn, HelloInfo { credits: ok.credits, leader })
                        }
                        Err(_) => Dialed::Failed,
                    },
                    FrameType::HelloRefused => match HelloRefused::decode(&payload) {
                        Ok(r) => Dialed::Refused { reason: r.reason, detail: r.detail.to_string() },
                        Err(_) => Dialed::Failed,
                    },
                    FrameType::Redirect => match Leader::decode(&payload) {
                        Ok(l) if !l.addr.is_empty() => Dialed::Redirect(l.addr.to_string()),
                        _ => Dialed::Failed,
                    },
                    _ => Dialed::Failed,
                };
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    return Dialed::Failed;
                }
            }
            Err(_) => return Dialed::Failed,
        }
    }
}

fn addr_or(advertised: &str, fallback: &str) -> String {
    if advertised.is_empty() { fallback.to_string() } else { advertised.to_string() }
}

/// A random-enough `client_id`: the process-random `RandomState` seed, mixed
/// with the wall clock and a per-process counter so two clients in one process
/// never collide. (`rand` is deliberately not a dependency of this crate.)
fn random_u64() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut h = RandomState::new().build_hasher();
    h.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    h.write_u128(
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0),
    );
    let v = h.finish();
    if v == 0 { 0x5DEE_CE66_D1CE_4B1D } else { v }
}
