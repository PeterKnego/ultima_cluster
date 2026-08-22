// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! [`Edge`] — the gateway process's whole job: terminate remote-protocol TCP
//! connections and pump them through one local [`Engine`] over the node's
//! shared memory (spec §4.3).
//!
//! ## Shape
//!
//! ```text
//!   TCP clients ──▶ acceptor thread ──▶ one reader thread per connection
//!                                             │  try_submit / try_query
//!                                             ▼
//!                                        uc2_client::Engine  (one per edge)
//!                                             │  completions
//!                                             ▼
//!                                        driver thread ──▶ writes frames back
//! ```
//!
//! A reader owns its own `SendHalf` clone (the engine's submit side is `Clone`
//! precisely so each submitting thread gets one); the single driver owns the
//! `PollHalf`. Correlation is `user_data = conn_idx << 32 | corr`, with `corr`
//! allocated from one edge-wide counter and resolved to `(seq, is_query)` in a
//! per-connection map.
//!
//! ## The invariant this file exists to hold
//!
//! **Every SUBMIT and QUERY frame ends in exactly one frame written back to
//! that client, or in the connection being dropped.** There is no third
//! outcome — no silent discard, no two answers. The reader writes the answer
//! itself when the `Engine` refuses the request at the door (oversized
//! payload, not serving, ring error); otherwise it reserves a `corr` and the
//! driver writes exactly one frame when the completion lands, because the
//! engine's own contract is exactly one completion per accepted request. The
//! hand-off is the `corr` map entry: whoever removes it owns the answer.
//!
//! ## Locks and blocking
//!
//! No lock is ever held across anything unbounded. The connection table's
//! `RwLock` is released before any socket write (`for_each` hands out a
//! snapshot); the per-connection writer `Mutex` is held for one `write_frame`
//! against a socket with a write timeout; the `corr` map is never locked
//! across a write. The one place a thread blocks indefinitely on purpose is a
//! reader parked on the credit gate — which is the TCP backstop working as
//! designed (we stop reading a client that ignores its credits) and is woken
//! by the driver, by `close`, and by a 20 ms backstop tick.
//!
//! ### Head-of-line blocking in the driver — a real, accepted cost
//!
//! The single driver writes each answer **inside** the `PollHalf::poll`
//! callback. So a client whose socket send buffer is full stalls the driver in
//! `write_frame` for up to [`WRITE_TIMEOUT`], and for that whole time the
//! driver is *not draining the engine's egress broadcast*. That broadcast is a
//! ring: records that are not read before the producer laps them are
//! overwritten, and the engine reports an overwritten completion as
//! `Outcome::TimedOut`. The consequence is therefore not merely "other clients
//! wait" — it is that **other clients' already-computed responses can be lost
//! and surface to them as `UNKNOWN`**, which they must resolve by re-sending
//! (safe, and answered `replayed`, only when the session envelope is on).
//!
//! Two things bound it today: `WRITE_TIMEOUT` is 1 s, and a stalled write is
//! fatal to that connection rather than retried. The real remedy — not
//! implemented here — is to take the write off the driver entirely: a bounded
//! outbound queue per connection with a per-connection writer thread (or one
//! writer pool), so `poll` only ever enqueues and a slow peer is dropped by
//! its own queue filling up rather than by stalling the shared drain.
//!
//! ## Not here yet
//!
//! The leader **watch** (poll `can_serve`/`leader_hint`, push `LEADER_CHANGED`
//! on a transition) is Task 9; the driver has a named hook where it goes.
//! Task 8 still redirects correctly — it just does so reactively, off
//! `!can_serve()` and `Outcome::NotLeader`, against the static member map.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use uc2_client::{
    ClientError, Completion, Consistency, Engine, EngineConfig, Outcome, PollHalf, SendHalf,
    SubmitError,
};
use uc2_log::cnc::CncPage;
use uc2_remote::conn::FramedConn;
use uc2_remote::frame::{
    FLAG_ENVELOPED, FLAG_EXPIRED, FLAG_IS_QUERY, FLAG_LINEARIZABLE, FLAG_REPLAYED, FrameType,
    HELLO_REFUSED_APP_ID, HELLO_REFUSED_FAULTED, HELLO_REFUSED_VERSION, Header, Hello, HelloOk,
    HelloRefused, Leader, PROTOCOL_VERSION, RETRY_NOT_SERVING, RETRY_PAYLOAD_TOO_LARGE,
    RETRY_SERVICE_UNAVAILABLE, ResponseMeta, Retry, Status,
};
use uc_protocol::ring::RingWaitHandle;

use crate::conn::{Conn, ConnTable};
use crate::config::{ConfigError, EdgeConfig};

// ---------------------------------------------------------------- constants

/// The node's control page under an instance directory. Same well-known name
/// `uc2_node::InstanceDir` writes and `uc2_client::Engine` opens; the edge
/// opens it a second time purely to learn the node's `max_payload`, which the
/// `Engine` inherits but does not expose.
const CNC_FILE: &str = "cnc2.dat";

/// The `Sessioned` response tag, mirrored from `uc2_service::session` — the
/// gateway does not depend on the service crate (an edge links no state
/// machine, and the whole point of the raw tier is that the relay never needs
/// one), so the four envelope constants are pinned here instead. They are part
/// of the wire contract, so `the_session_envelope_constants_match_uc2_service`
/// below asserts they still agree with `uc2_service`'s definitions — a drift
/// there would silently mislabel every response.
const TAG_FRESH: u8 = 0;
const TAG_REPLAYED: u8 = 1;
const TAG_EXPIRED: u8 = 2;
/// `client_id ++ seq`, little-endian — `uc2_service::SESSION_HEADER_LEN`.
const SESSION_HEADER_LEN: usize = 16;

/// Socket write timeout — the hard bound on how long one client can stall the
/// driver's drain of the engine broadcast (see "Head-of-line blocking" above),
/// and on how long it can hold the per-connection writer lock.
///
/// One second, deliberately short. These are small frames on a `TCP_NODELAY`
/// socket: a peer that cannot absorb one within a second is not "briefly
/// busy", it is gone (or has stopped reading, which for a protocol with
/// receiver-driven credits is the same thing). Dropping the connection is both
/// cheaper and more honest than holding the shared drain hostage for longer.
const WRITE_TIMEOUT: Duration = Duration::from_secs(1);
/// Socket read timeout — the reader's tick, at which it notices the stop flag.
const READ_TIMEOUT: Duration = Duration::from_millis(200);
/// Budget for a connection's `HELLO` to arrive.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the acceptor sleeps between polls of the non-blocking listener.
const ACCEPT_POLL: Duration = Duration::from_millis(5);
/// `RETRY{service_unavailable}` backoff hint.
const RETRY_BACKOFF_US: u32 = 1_000;
/// `RETRY{not_serving}` backoff hint: about one election timeout, since that
/// is how long the client would have to wait for a leader to exist.
const NOT_SERVING_BACKOFF_US: u32 = 300_000;
/// Backpressure ladder for a reader whose `try_submit` is being refused: this
/// many `yield_now`s before it starts parking. A yield is right while the
/// engine is merely momentarily full; parking is right once it is clear the
/// node is not draining, so a wedged node cannot burn a core per connection.
const BACKPRESSURE_YIELDS: u32 = 64;
/// First park in the backpressure ladder, doubling to [`BACKPRESSURE_PARK_MAX`].
const BACKPRESSURE_PARK_MIN: Duration = Duration::from_micros(10);
const BACKPRESSURE_PARK_MAX: Duration = Duration::from_millis(1);
/// Driver idle ladder, copied in shape from `uc2_client::pipelined`'s driver:
/// spin, then yield, then park on the egress ring's wake word.
const DRIVER_SPINS: u32 = 10;
const DRIVER_YIELDS: u32 = 20;
const DRIVER_PARK: Duration = Duration::from_millis(1);
/// How often the driver runs its periodic work (status timer, leader watch)
/// while completions are streaming in without a break.
const DRIVER_PERIODIC_EVERY: u64 = 64;

// ---------------------------------------------------------------- errors

/// Why an [`Edge`] could not start.
#[derive(Debug, thiserror::Error)]
pub enum EdgeError {
    #[error("gateway configuration: {0}")]
    Config(#[from] ConfigError),
    #[error("attaching to the node's instance directory: {0}")]
    Attach(#[from] ClientError),
    #[error("binding the gateway listener: {0}")]
    Bind(std::io::Error),
    #[error("spawning a gateway thread: {0}")]
    Spawn(std::io::Error),
}

// ---------------------------------------------------------------- stats

#[derive(Default)]
struct StatCells {
    connections: AtomicU64,
    submits: AtomicU64,
    queries: AtomicU64,
    responses: AtomicU64,
    redirects: AtomicU64,
    retries: AtomicU64,
    unknown: AtomicU64,
    backpressure_events: AtomicU64,
    leader_changes: AtomicU64,
    status_frames: AtomicU64,
}

impl StatCells {
    fn snapshot(&self) -> EdgeStats {
        EdgeStats {
            connections: self.connections.load(Ordering::Relaxed),
            submits: self.submits.load(Ordering::Relaxed),
            queries: self.queries.load(Ordering::Relaxed),
            responses: self.responses.load(Ordering::Relaxed),
            redirects: self.redirects.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            unknown: self.unknown.load(Ordering::Relaxed),
            backpressure_events: self.backpressure_events.load(Ordering::Relaxed),
            leader_changes: self.leader_changes.load(Ordering::Relaxed),
            status_frames: self.status_frames.load(Ordering::Relaxed),
        }
    }
}

/// What the edge has done since it started.
///
/// Every outbound-frame counter is bumped **before** the frame is written, so
/// a caller that has just received an answer always sees it counted. (The
/// alternative — counting after a successful write — is racy from the client's
/// point of view: the bytes are on the wire before the counter moves. The cost
/// is that a frame whose write then died on a broken socket is still counted;
/// these are "frames produced", not "frames delivered", and nothing downstream
/// can promise delivery anyway.)
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EdgeStats {
    /// Connections accepted (including ones since closed).
    pub connections: u64,
    /// SUBMIT frames the `Engine` accepted. A submit refused at the door
    /// (oversized, not serving) is counted under `retries`/`redirects`, not
    /// here.
    pub submits: u64,
    /// QUERY frames the `Engine` accepted.
    pub queries: u64,
    /// `RESPONSE` frames written back.
    pub responses: u64,
    /// `REDIRECT` frames written back.
    pub redirects: u64,
    /// `RETRY` frames written back (every reason, including the terminal
    /// `PAYLOAD_TOO_LARGE`).
    pub retries: u64,
    /// `UNKNOWN` frames written back (the engine timed a slot out).
    pub unknown: u64,
    /// Times the `Engine` reported `Backpressure` and credits were halved.
    pub backpressure_events: u64,
    /// `LEADER_CHANGED` frames written back.
    pub leader_changes: u64,
    /// Standalone `STATUS` frames written back — the idle-liveness tick and
    /// the credit-reopened announcement. Never counted before `HELLO_OK`.
    pub status_frames: u64,
}

// ---------------------------------------------------------------- shared

struct Shared {
    cfg: EdgeConfig,
    /// Static node id → gateway address (spec §4.3: the cnc page has ids, not
    /// addresses).
    members: HashMap<u32, String>,
    table: ConnTable,
    stats: StatCells,
    stop: AtomicBool,
    /// Reference instant for `Conn::last_write_ns`, so the status timer costs
    /// one `u64` per connection instead of a locked `Instant`.
    t0: Instant,
    /// The node's own payload bound, read off the cnc page. Enforced here,
    /// before `try_submit`, so an oversized frame never touches the ring.
    max_payload: usize,
    /// Correlation ids, edge-wide rather than per connection: a stale
    /// completion then cannot collide with a fresh request's `corr` even in
    /// the (already guarded) case of an index collision.
    next_corr: AtomicU32,
    readers: Mutex<Vec<JoinHandle<()>>>,
    /// The edge has taken itself out of service: the node's shmem instance
    /// restarted underneath it, so its `Engine` attach is void and every
    /// request it could accept would fail the same way.
    ///
    /// Without this the edge livelocks a client: the handshake succeeds, the
    /// first SUBMIT hits `InstanceRestart`, the client is told
    /// `LEADER_CHANGED{unknown}`, it reconnects to the same address, and round
    /// it goes. Refusing the *handshake* is what makes the failure visible and
    /// terminal, and lets a multi-member client move on.
    faulted: AtomicBool,
}

impl Shared {
    fn now_ns(&self) -> u64 {
        self.t0.elapsed().as_nanos() as u64
    }

    fn stopping(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    /// The `REDIRECT`/`LEADER_CHANGED` target for a node id, if we know its
    /// gateway address.
    fn gateway_of(&self, node_id: u32) -> Option<&str> {
        self.members.get(&node_id).map(|s| s.as_str())
    }

    /// Answer a request this node cannot serve: `REDIRECT` to the leader's
    /// gateway when the hint resolves to a member, otherwise `RETRY` — there
    /// is no address to send the client to, and inventing one is worse than
    /// telling it to wait out the election.
    fn redirect_or_retry(&self, conn: &Conn, seq: u64, hint: Option<u32>) {
        match hint.and_then(|id| self.gateway_of(id).map(|a| (id, a))) {
            Some((id, addr)) => {
                let mut out = Vec::new();
                Leader { node_id: id, addr }.encode(&mut out);
                self.stats.redirects.fetch_add(1, Ordering::Relaxed);
                conn.write(conn.hdr(FrameType::Redirect, 0, seq), &out, self.now_ns());
            }
            None => {
                let mut out = Vec::new();
                Retry { reason: RETRY_NOT_SERVING, retry_after_us: NOT_SERVING_BACKOFF_US }
                    .encode(&mut out);
                self.stats.retries.fetch_add(1, Ordering::Relaxed);
                conn.write(conn.hdr(FrameType::Retry, 0, seq), &out, self.now_ns());
            }
        }
    }

    fn write_retry(&self, conn: &Conn, seq: u64, reason: u8, after_us: u32) {
        let mut out = Vec::new();
        Retry { reason, retry_after_us: after_us }.encode(&mut out);
        self.stats.retries.fetch_add(1, Ordering::Relaxed);
        conn.write(conn.hdr(FrameType::Retry, 0, seq), &out, self.now_ns());
    }

    /// Write a standalone `STATUS`. Silently does nothing on a connection whose
    /// handshake has not completed — an unsolicited frame before `HELLO_OK`
    /// would fail the peer's dial (see `Conn::ready`).
    fn write_status(&self, conn: &Conn) {
        if !conn.is_ready() {
            return;
        }
        let mut out = Vec::new();
        Status { acked_seq: conn.acked_seq(), credits: conn.credits() }.encode(&mut out);
        self.stats.status_frames.fetch_add(1, Ordering::Relaxed);
        conn.write(conn.hdr(FrameType::Status, 0, 0), &out, self.now_ns());
    }

    /// The node's shmem identity changed underneath us: every request in
    /// flight is unanswerable and every connection's session is void. Tell
    /// each client the leader is unknown (which makes `RemoteClient`
    /// reconnect and re-`HELLO`) and drop the lot.
    fn on_instance_restart(&self) {
        // Latch first: a connection accepted between here and the last close
        // must be refused at the handshake, not served into the same fault.
        self.faulted.store(true, Ordering::SeqCst);
        let mut out = Vec::new();
        // The `node_id` is a sentinel and is ignored by the client — the EMPTY
        // ADDRESS is the signal ("leader unknown: reconnect and re-HELLO").
        Leader { node_id: u32::MAX, addr: "" }.encode(&mut out);
        for c in self.table.take_all() {
            // A connection still mid-handshake gets no frame, only the close:
            // its peer is waiting for HELLO_OK and would reject anything else.
            if c.is_ready() {
                self.stats.leader_changes.fetch_add(1, Ordering::Relaxed);
                c.write(c.hdr(FrameType::LeaderChanged, 0, 0), &out, self.now_ns());
            }
            c.close();
        }
    }

    fn is_faulted(&self) -> bool {
        self.faulted.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------- edge

/// A running gateway edge.
pub struct Edge {
    shared: Arc<Shared>,
    local_addr: SocketAddr,
    /// The acceptor and driver handles; reader handles live in `shared`.
    threads: Mutex<Vec<JoinHandle<()>>>,
    /// The egress ring's wake word — lets [`Edge::stop`] interrupt a parked
    /// driver instead of waiting out its park.
    wake: RingWaitHandle,
}

impl Edge {
    /// Attach to the local node, bind the listener, and start serving.
    ///
    /// The `Engine` is attached with `serving_gate: false` on purpose: the
    /// edge must be able to answer a client that arrives at a non-leader with
    /// `REDIRECT` (an address it can act on), and the gate would instead turn
    /// every submit into a bare `NotServing` refusal at the engine door,
    /// before the edge could look up where to send it.
    pub fn start(cfg: EdgeConfig) -> Result<Edge, EdgeError> {
        cfg.validate()?;

        // The node's own payload bound. `Engine` inherits it internally but
        // does not expose it, and the edge needs the number *before*
        // `try_submit` so an oversized frame is refused without ever reaching
        // the ingress ring (spec §4.3).
        let cnc = CncPage::open_file(&cfg.instance_dir.join(CNC_FILE), &cfg.app_id)
            .map_err(|e| EdgeError::Attach(ClientError::from(e)))?;
        let max_payload = cnc.meta().max_payload as usize;
        drop(cnc);

        let (send, poll) = Engine::attach(
            &cfg.instance_dir,
            &cfg.app_id,
            EngineConfig {
                max_inflight: cfg.max_inflight,
                request_timeout: cfg.request_timeout,
                serving_gate: false,
                ..Default::default()
            },
        )?;

        let listener = TcpListener::bind(cfg.listen).map_err(EdgeError::Bind)?;
        let local_addr = listener.local_addr().map_err(EdgeError::Bind)?;
        // Non-blocking + a poll loop, so `stop` needs no connect-to-self trick
        // to break the acceptor out of `accept`.
        listener.set_nonblocking(true).map_err(EdgeError::Bind)?;

        let members =
            cfg.members.iter().map(|m| (m.node_id, m.gateway.clone())).collect::<HashMap<_, _>>();
        let wake = poll.wait_handle();
        let shared = Arc::new(Shared {
            cfg,
            members,
            table: ConnTable::default(),
            stats: StatCells::default(),
            stop: AtomicBool::new(false),
            t0: Instant::now(),
            max_payload,
            next_corr: AtomicU32::new(0),
            readers: Mutex::new(Vec::new()),
            faulted: AtomicBool::new(false),
        });

        // The driver gets its own `SendHalf` clone: `SendHalf` is `Send` but
        // NOT `Sync` (each clone carries its own MPSC producer cache), so the
        // supported way to use it from a second thread is to clone it, never
        // to share a reference. The driver needs it for `leader_hint()` (the
        // `REDIRECT` fallback, and Task 9's leader watch).
        let drv_send = send.clone();
        let acc_shared = Arc::clone(&shared);
        let acceptor = std::thread::Builder::new()
            .name("uc2-gw-accept".into())
            .spawn(move || acceptor(acc_shared, listener, send))
            .map_err(EdgeError::Spawn)?;

        let drv_shared = Arc::clone(&shared);
        let driver = std::thread::Builder::new()
            .name("uc2-gw-driver".into())
            .spawn(move || driver(drv_shared, poll, drv_send))
            .map_err(EdgeError::Spawn)?;

        Ok(Edge {
            shared,
            local_addr,
            threads: Mutex::new(vec![acceptor, driver]),
            wake,
        })
    }

    /// The address the listener actually bound (resolves a `:0` port).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn stats(&self) -> EdgeStats {
        self.shared.stats.snapshot()
    }

    /// The edge has taken itself out of service and will refuse every new
    /// handshake with `HELLO_REFUSED{HELLO_REFUSED_FAULTED}` until the process
    /// is restarted.
    ///
    /// Today the only way in is the node's shmem instance restarting under the
    /// attached `Engine`. Re-attaching in place is deliberately not attempted:
    /// `uc2_client` documents re-attach as a v2.0 decision, and a relay that
    /// silently reconnected to a *different* node incarnation would be
    /// answering with a session table the client never established. A
    /// supervisor (systemd) restarting the gateway is the intended recovery.
    pub fn is_faulted(&self) -> bool {
        self.shared.is_faulted()
    }

    /// Force the faulted state, for tests that need to observe the refusal
    /// without racing a real node restart. Not part of the public contract.
    #[doc(hidden)]
    pub fn fault_for_tests(&self) {
        self.shared.on_instance_restart();
    }

    /// Stop serving and join every thread this edge started.
    ///
    /// Order matters: the stop flag first (so nothing new is accepted or
    /// submitted), then every socket is shut down — which is what wakes the
    /// reader threads out of `read_frame` and any reader parked on the credit
    /// gate — then the driver's park is interrupted, then the joins.
    pub fn stop(self) {
        self.shutdown();
    }

    fn shutdown(&self) {
        self.shared.stop.store(true, Ordering::Relaxed);
        for c in self.shared.table.take_all() {
            c.close();
        }
        self.wake.wake();
        for h in self.threads.lock().drain(..) {
            let _ = h.join();
        }
        for h in self.shared.readers.lock().drain(..) {
            let _ = h.join();
        }
    }
}

impl Drop for Edge {
    /// Dropping an `Edge` without calling [`Edge::stop`] must not leak
    /// threads; `shutdown` is idempotent, so the explicit call and this one
    /// compose.
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl std::fmt::Debug for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Edge")
            .field("local_addr", &self.local_addr)
            .field("app_id", &self.shared.cfg.app_id)
            .field("stats", &self.stats())
            .finish()
    }
}

// ---------------------------------------------------------------- acceptor

fn acceptor(shared: Arc<Shared>, listener: TcpListener, send: SendHalf) {
    while !shared.stopping() {
        match listener.accept() {
            Ok((stream, _peer)) => {
                // A connection we could not set up is simply dropped: the
                // client sees the socket close and reconnects.
                let _ = spawn_conn(&shared, &send, stream);
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => std::thread::sleep(ACCEPT_POLL),
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            // A hard accept error (fd exhaustion, etc.) is transient from the
            // edge's point of view; back off rather than dying silently.
            Err(_) => std::thread::sleep(ACCEPT_POLL),
        }
    }
}

fn spawn_conn(shared: &Arc<Shared>, send: &SendHalf, stream: TcpStream) -> std::io::Result<()> {
    // A listener set non-blocking does not (on Linux) hand out non-blocking
    // accepted sockets, but say so explicitly rather than relying on it: the
    // reader wants blocking reads with a timeout.
    stream.set_nonblocking(false)?;
    let read_half = FramedConn::new(stream)?;
    read_half.set_read_timeout(Some(READ_TIMEOUT))?;
    read_half.set_write_timeout(Some(WRITE_TIMEOUT))?;
    // The two halves are dups of one socket and therefore share its timeouts —
    // set above, once, for both directions.
    let write_half = read_half.try_clone()?;

    let idx = shared.table.alloc_idx();
    let conn = Arc::new(Conn::new(
        idx,
        write_half,
        shared.cfg.per_conn_inflight,
        shared.now_ns(),
    ));
    shared.table.insert(Arc::clone(&conn));
    shared.stats.connections.fetch_add(1, Ordering::Relaxed);

    let rd_shared = Arc::clone(shared);
    let rd_send = send.clone();
    let handle = std::thread::Builder::new()
        .name(format!("uc2-gw-rx-{idx}"))
        .spawn(move || reader(rd_shared, conn, rd_send, read_half))?;

    let mut readers = shared.readers.lock();
    // Reap finished readers opportunistically so a long-lived edge does not
    // accumulate one `JoinHandle` per connection it ever served.
    readers.retain(|h: &JoinHandle<()>| !h.is_finished());
    readers.push(handle);
    Ok(())
}

// ---------------------------------------------------------------- reader

fn reader(shared: Arc<Shared>, conn: Arc<Conn>, send: SendHalf, mut fc: FramedConn) {
    if handshake(&shared, &conn, &send, &mut fc) {
        while !shared.stopping() && !conn.is_closed() {
            match fc.read_frame() {
                // Read timeout at a frame boundary: just re-check the flags.
                Ok(None) => {}
                Ok(Some((h, payload))) => {
                    if !handle_frame(&shared, &conn, &send, h, &payload) {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }
    shared.table.remove(conn.idx);
}

/// Read the client's `HELLO`, check it, and answer. Returns `false` if the
/// connection is finished (refused, malformed, or timed out).
fn handshake(shared: &Arc<Shared>, conn: &Arc<Conn>, send: &SendHalf, fc: &mut FramedConn) -> bool {
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    let (h, payload) = loop {
        if shared.stopping() || conn.is_closed() || Instant::now() >= deadline {
            return false;
        }
        match fc.read_frame() {
            Ok(Some(f)) => break f,
            Ok(None) => continue,
            Err(_) => return false,
        }
    };
    if h.ty != FrameType::Hello {
        return false;
    }
    conn.set_client_id(h.client_id);
    if shared.is_faulted() {
        // Terminal for this edge, not for the cluster: a client with more than
        // one member in its list should try the next one.
        refuse_hello(
            shared,
            conn,
            HELLO_REFUSED_FAULTED,
            "edge faulted: node instance restarted; restart the gateway",
        );
        return false;
    }
    if h.version != PROTOCOL_VERSION {
        let detail = format!("edge speaks remote protocol v{PROTOCOL_VERSION}");
        refuse_hello(shared, conn, HELLO_REFUSED_VERSION, &detail);
        return false;
    }
    let Ok(hello) = Hello::decode(&payload) else {
        refuse_hello(shared, conn, HELLO_REFUSED_APP_ID, "malformed HELLO payload");
        return false;
    };
    if hello.app_id != shared.cfg.app_id {
        refuse_hello(shared, conn, HELLO_REFUSED_APP_ID, &shared.cfg.app_id);
        return false;
    }

    let leader = send.leader_hint();
    let leader_addr = leader.and_then(|id| shared.gateway_of(id)).unwrap_or("");
    let mut out = Vec::new();
    HelloOk { credits: shared.cfg.per_conn_inflight, leader, leader_addr }.encode(&mut out);
    if !conn.write(conn.hdr(FrameType::HelloOk, 0, h.seq), &out, shared.now_ns()) {
        return false;
    }
    // Only now may the edge write on its own initiative (the STATUS timer).
    conn.set_ready();
    true
}

fn refuse_hello(shared: &Arc<Shared>, conn: &Arc<Conn>, reason: u8, detail: &str) {
    let mut out = Vec::new();
    HelloRefused { reason, detail }.encode(&mut out);
    conn.write(conn.hdr(FrameType::HelloRefused, 0, 0), &out, shared.now_ns());
}

/// Handle one post-handshake frame. Returns `false` to end the connection.
fn handle_frame(
    shared: &Arc<Shared>,
    conn: &Arc<Conn>,
    send: &SendHalf,
    h: Header,
    payload: &[u8],
) -> bool {
    match h.ty {
        FrameType::Submit => dispatch(shared, conn, send, h, payload, false),
        FrameType::Query => dispatch(shared, conn, send, h, payload, true),
        FrameType::Ping => {
            // The client PINGs with `seq: 0` when idle and declares the edge
            // dead after `dead_after` of total silence, so this answer is
            // load-bearing, not decoration. Echo the seq.
            conn.write(conn.hdr(FrameType::Pong, 0, h.seq), &[], shared.now_ns())
        }
        FrameType::Pong => true,
        // A second HELLO is a protocol error: the session's identity and
        // credit window are established once, and re-establishing them
        // mid-stream would silently invalidate every in-flight `seq`.
        FrameType::Hello => false,
        other => {
            if conn.first_unexpected() {
                eprintln!(
                    "uc2-gateway: connection {} sent an unexpected frame type {other:?}; ignoring \
                     it and any further ones",
                    conn.idx
                );
            }
            true
        }
    }
}

/// The SUBMIT/QUERY path. Returns `false` to end the connection.
fn dispatch(
    shared: &Arc<Shared>,
    conn: &Arc<Conn>,
    send: &SendHalf,
    h: Header,
    payload: &[u8],
    is_query: bool,
) -> bool {
    // Credit gate FIRST: a client that ignores its window is stopped by us not
    // reading its socket, never by accepting a frame and bouncing it.
    if !conn.wait_for_credit(&shared.stop) {
        return false;
    }

    // The envelope rides inside the node's payload budget, so it counts.
    //
    // This check is redundant by design: the `Engine` inherits the same bound
    // from the cnc page and refuses an oversized payload itself
    // (`uc2_client/src/engine.rs`, `SendHalf::send`), and the arm below handles
    // that refusal identically. It is kept as belt-and-braces because the
    // spec's wording is a *guarantee about the ring* ("payload > the node's
    // max_payload → refused before touching the ring"), and a guarantee that
    // holds only because some other crate's private ordering happens to check
    // first is not one this edge can make. Both paths write the same frame.
    let envelope = shared.cfg.session_envelope && !is_query;
    let wire_len = payload.len() + if envelope { SESSION_HEADER_LEN } else { 0 };
    if wire_len > shared.max_payload {
        // Terminal for the client — `RemoteClient` maps this reason to a hard
        // error and never re-sends. The ring is never touched.
        shared.write_retry(conn, h.seq, RETRY_PAYLOAD_TOO_LARGE, 0);
        return !conn.is_closed();
    }

    // Writes are leader-only; queries are answered by whichever replica this
    // edge sits on (snapshot locally, linearizable through the local node's
    // read barrier), so the serving check applies to SUBMIT alone.
    if !is_query && !send.can_serve() {
        shared.redirect_or_retry(conn, h.seq, send.leader_hint());
        return !conn.is_closed();
    }

    // With the envelope on, the 16-byte LE `client_id ++ seq` header goes in
    // front of the opaque command bytes (spec §4.3). With it off the client's
    // bytes reach `apply` exactly as written — no copy, no interpretation.
    let enveloped;
    let body: &[u8] = if envelope {
        let mut b = Vec::with_capacity(SESSION_HEADER_LEN + payload.len());
        b.extend_from_slice(&conn.client_id().to_le_bytes());
        b.extend_from_slice(&h.seq.to_le_bytes());
        b.extend_from_slice(payload);
        enveloped = b;
        &enveloped
    } else {
        payload
    };

    let corr = shared.next_corr.fetch_add(1, Ordering::Relaxed);
    let user_data = ((conn.idx as u64) << 32) | corr as u64;
    conn.reserve(corr, h.seq, is_query);

    // Retry only against `Backpressure`, and only for as long as the request's
    // own budget: an engine that never drains must not spin a reader forever.
    //
    // The window is squeezed ONCE per request, not once per retry iteration:
    // halving on every spin would drive a connection to 1 credit in six
    // iterations of what is often a sub-microsecond hiccup, and would inflate
    // `backpressure_events` into a count of loop turns rather than of episodes.
    let deadline = Instant::now() + shared.cfg.request_timeout;
    let mut squeezed = false;
    let mut spins: u32 = 0;
    let mut park = BACKPRESSURE_PARK_MIN;
    loop {
        let res = if is_query {
            let c = if h.flags & FLAG_LINEARIZABLE != 0 {
                Consistency::Linearizable
            } else {
                Consistency::Snapshot
            };
            send.try_query(user_data, body, c)
        } else {
            send.try_submit(user_data, body)
        };
        match res {
            Ok(()) => {
                let cell = if is_query { &shared.stats.queries } else { &shared.stats.submits };
                cell.fetch_add(1, Ordering::Relaxed);
                return true;
            }
            Err(SubmitError::Backpressure) => {
                if !squeezed {
                    squeezed = true;
                    shared.stats.backpressure_events.fetch_add(1, Ordering::Relaxed);
                    conn.squeeze();
                }
                if shared.stopping() || conn.is_closed() {
                    conn.unreserve(corr);
                    return false;
                }
                if Instant::now() >= deadline {
                    if conn.unreserve(corr) {
                        shared.write_retry(
                            conn,
                            h.seq,
                            RETRY_SERVICE_UNAVAILABLE,
                            RETRY_BACKOFF_US,
                        );
                    }
                    return !conn.is_closed();
                }
                // Deliberately do NOT read the socket while waiting: the TCP
                // window closing is the backstop the credit scheme leans on.
                //
                // Yield while the engine is plausibly just momentarily full,
                // then park on a doubling ladder — a node that has stopped
                // draining altogether must not cost one spinning core per
                // connection while the request burns its timeout.
                if spins < BACKPRESSURE_YIELDS {
                    spins += 1;
                    std::thread::yield_now();
                } else {
                    std::thread::park_timeout(park);
                    park = (park * 2).min(BACKPRESSURE_PARK_MAX);
                }
            }
            Err(SubmitError::NotServing) => {
                // Unreachable with `serving_gate: false`, but the engine owns
                // that flag, not us — answer it properly rather than assume.
                if conn.unreserve(corr) {
                    shared.redirect_or_retry(conn, h.seq, send.leader_hint());
                }
                return !conn.is_closed();
            }
            Err(SubmitError::PayloadTooLarge { .. }) => {
                if conn.unreserve(corr) {
                    shared.write_retry(conn, h.seq, RETRY_PAYLOAD_TOO_LARGE, 0);
                }
                return !conn.is_closed();
            }
            Err(SubmitError::InstanceRestart { .. }) => {
                conn.unreserve(corr);
                shared.on_instance_restart();
                return false;
            }
            Err(SubmitError::Ring(_)) => {
                if conn.unreserve(corr) {
                    shared.write_retry(conn, h.seq, RETRY_SERVICE_UNAVAILABLE, RETRY_BACKOFF_US);
                }
                return !conn.is_closed();
            }
        }
    }
}

// ---------------------------------------------------------------- driver

/// Arms the ring's wake word for the duration of a park and disarms it on
/// drop — including on an unwind, so a panicking driver cannot leave a stale
/// waiter count behind that makes the producer pay a wake forever.
struct ArmGuard<'a>(&'a RingWaitHandle);

impl<'a> ArmGuard<'a> {
    fn new(wh: &'a RingWaitHandle) -> Self {
        wh.arm();
        ArmGuard(wh)
    }
}

impl Drop for ArmGuard<'_> {
    fn drop(&mut self) {
        self.0.disarm();
    }
}

fn driver(shared: Arc<Shared>, mut poll: PollHalf, send: SendHalf) {
    let wh = poll.wait_handle();
    let mut cycle: u64 = 0;
    let mut idle: u32 = 0;
    // The periodic work walks every connection, which means a table snapshot;
    // an idle driver spins its ladder thousands of times a second, so running
    // it per idle iteration would allocate a `Vec` per turn to discover there
    // is nothing to do. A quarter of `status_interval` keeps the STATUS timer
    // accurate to well within its own tolerance at a fraction of the cost.
    let periodic_every = (shared.cfg.status_interval / 4).max(Duration::from_millis(1));
    let mut last_periodic = Instant::now();
    while !shared.stopping() {
        cycle += 1;
        let n = poll.poll(|c| complete(&shared, &send, c));
        if n > 0 {
            idle = 0;
            if cycle.is_multiple_of(DRIVER_PERIODIC_EVERY) {
                maybe_periodic(&shared, &send, &mut last_periodic, periodic_every);
            }
            continue;
        }
        maybe_periodic(&shared, &send, &mut last_periodic, periodic_every);
        if shared.stopping() {
            break;
        }
        // Idle ladder, same shape as `uc2_client::pipelined`'s driver: spin,
        // yield, then park on the egress ring's wake word with a 1 ms cap so
        // the stop flag is never missed for long.
        idle += 1;
        if idle <= DRIVER_SPINS {
            std::hint::spin_loop();
        } else if idle <= DRIVER_SPINS + DRIVER_YIELDS {
            std::thread::yield_now();
        } else {
            let seq = wh.current_seq();
            let _arm = ArmGuard::new(&wh); // disarms on drop, including on unwind
            if poll.poll(|c| complete(&shared, &send, c)) == 0 && !shared.stopping() {
                wh.park(seq, DRIVER_PARK);
            }
        }
    }
    // Nothing will answer what is still in flight; release the slots so the
    // engine's own accounting closes out cleanly.
    poll.drain_abort(|_| {});
}

/// Run [`periodic`] at most once per `every`.
fn maybe_periodic(
    shared: &Arc<Shared>,
    send: &SendHalf,
    last: &mut Instant,
    every: Duration,
) {
    let now = Instant::now();
    if now.duration_since(*last) >= every {
        *last = now;
        periodic(shared, send);
    }
}

/// The driver's between-polls work.
fn periodic(shared: &Arc<Shared>, _send: &SendHalf) {
    // Task 9: leader watch here — poll `can_serve()`/`leader_hint()` and push
    // `LEADER_CHANGED` to every connection on a transition.
    let now = shared.now_ns();
    let interval = shared.cfg.status_interval.as_nanos() as u64;
    shared.table.for_each(|c| {
        // `is_ready` is checked here as well as inside `write_status` so a
        // mid-handshake connection costs nothing at all: it is the common case
        // on a slow link, and the whole point is that the STATUS timer must not
        // race the handshake (see `Conn::ready`).
        if c.is_ready() && now.saturating_sub(c.last_write_ns()) >= interval {
            // Doubles as edge→client liveness: a client that hears nothing at
            // all for its `dead_after` fails the connection over.
            shared.write_status(c);
        }
    });
}

/// Resolve one `Engine` completion into exactly one frame (or a drop).
fn complete(shared: &Arc<Shared>, send: &SendHalf, c: Completion<'_>) {
    let idx = (c.user_data >> 32) as u32;
    let corr = c.user_data as u32;
    // A completion for a connection that has since gone is dropped: nobody is
    // left to answer, and the index is never reused (see `conn.rs`).
    let Some(conn) = shared.table.get(idx) else { return };
    let Some((seq, is_query)) = conn.claim(corr) else { return };

    // Relaxing before the frame is built means a RESPONSE itself carries the
    // reopened window.
    //
    // ANY completion relaxes, not just a `Response`: what a squeeze measures is
    // the engine's inflight window being full, and every completion — a retry,
    // a redirect, a timed-out slot — is that window giving a slot back. Gating
    // the relax on `Response` would pin a connection at 1 credit for as long as
    // the node was answering `NotLeader`, and then leave it there.
    let credits_up = conn.relax(shared.cfg.per_conn_inflight);
    conn.notify_gate();

    let now = shared.now_ns();
    match c.outcome {
        Outcome::Response(bytes) => {
            let (flags, body) = response_shape(shared.cfg.session_envelope, is_query, bytes);
            let meta = ResponseMeta {
                credits: conn.credits(),
                acked_seq: conn.acked_seq(),
                position: c.position.unwrap_or(0),
            };
            let mut out = Vec::with_capacity(ResponseMeta::LEN + body.len());
            meta.encode(&mut out);
            out.extend_from_slice(body);
            shared.stats.responses.fetch_add(1, Ordering::Relaxed);
            conn.write(conn.hdr(FrameType::Response, flags, seq), &out, now);
            // The RESPONSE above already carried the new credits, so no
            // separate STATUS is owed here.
            return;
        }
        Outcome::NotLeader { hint } => {
            shared.redirect_or_retry(&conn, seq, hint.or_else(|| send.leader_hint()));
        }
        Outcome::Retry => {
            shared.write_retry(&conn, seq, RETRY_SERVICE_UNAVAILABLE, RETRY_BACKOFF_US);
        }
        Outcome::TimedOut => {
            // "May or may not have committed" — the client re-sends, and with
            // the session envelope on the re-send is answered `replayed`.
            shared.stats.unknown.fetch_add(1, Ordering::Relaxed);
            conn.write(conn.hdr(FrameType::Unknown, 0, seq), &[], now);
        }
        Outcome::InstanceRestart { .. } => {
            shared.on_instance_restart();
            return;
        }
    }
    if credits_up {
        // None of the frames above carries a credit field, so an increase has
        // to be announced on its own.
        shared.write_status(&conn);
    }
}

/// Split an engine response into `RESPONSE` flags and the body the client
/// gets. With the envelope on, the leading `Sessioned` tag becomes flags and
/// stops being payload; with it off, the bytes pass through untouched.
fn response_shape(envelope: bool, is_query: bool, bytes: &[u8]) -> (u8, &[u8]) {
    if is_query {
        // Queries never carry the envelope (a read is not a session event).
        return (FLAG_IS_QUERY, bytes);
    }
    if !envelope {
        return (0, bytes);
    }
    match bytes.split_first() {
        Some((&TAG_FRESH, rest)) => (FLAG_ENVELOPED, rest),
        Some((&TAG_REPLAYED, rest)) => (FLAG_ENVELOPED | FLAG_REPLAYED, rest),
        Some((&TAG_EXPIRED, _)) => (FLAG_ENVELOPED | FLAG_EXPIRED, &[]),
        // No tag at all, or one this protocol version does not know: the
        // outcome is not knowable from here, which is exactly what EXPIRED
        // means to the client. Never guess "fresh".
        _ => (FLAG_ENVELOPED | FLAG_EXPIRED, &[]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The envelope constants are duplicated, not imported (see their doc
    /// above). `uc2_service` is a dev-dependency, so this is the guard that
    /// keeps the copy honest.
    #[test]
    fn the_session_envelope_constants_match_uc2_service() {
        assert_eq!(SESSION_HEADER_LEN, uc2_service::SESSION_HEADER_LEN);
        assert_eq!(TAG_FRESH, uc2_service::TAG_FRESH);
        assert_eq!(TAG_REPLAYED, uc2_service::TAG_REPLAYED);
        assert_eq!(TAG_EXPIRED, uc2_service::TAG_EXPIRED);
    }

    #[test]
    fn a_query_answer_is_flagged_and_never_unwrapped() {
        assert_eq!(response_shape(true, true, &[9, 9]), (FLAG_IS_QUERY, &[9u8, 9][..]));
        assert_eq!(response_shape(false, true, &[9, 9]), (FLAG_IS_QUERY, &[9u8, 9][..]));
    }

    #[test]
    fn the_session_tag_becomes_flags() {
        let fresh = response_shape(true, false, &[TAG_FRESH, 1, 2]);
        assert_eq!(fresh, (FLAG_ENVELOPED, &[1u8, 2][..]));
        assert_eq!(
            response_shape(true, false, &[TAG_REPLAYED, 1, 2]),
            (FLAG_ENVELOPED | FLAG_REPLAYED, &[1u8, 2][..])
        );
        assert_eq!(
            response_shape(true, false, &[TAG_EXPIRED]),
            (FLAG_ENVELOPED | FLAG_EXPIRED, &[][..]),
            "an expired session entry carries no body"
        );
    }

    #[test]
    fn an_unknown_or_missing_tag_reads_as_expired_not_fresh() {
        assert_eq!(response_shape(true, false, &[]), (FLAG_ENVELOPED | FLAG_EXPIRED, &[][..]));
        assert_eq!(response_shape(true, false, &[77, 1]), (FLAG_ENVELOPED | FLAG_EXPIRED, &[][..]));
    }

    #[test]
    fn with_the_envelope_off_bytes_pass_through_untouched() {
        assert_eq!(response_shape(false, false, &[0, 1, 2]), (0, &[0u8, 1, 2][..]));
    }
}
