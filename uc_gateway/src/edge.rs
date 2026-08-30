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
//!                                        uc_client::Engine  (one per edge)
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
//! ## The other invariant: accepted SUBMITs are a PREFIX
//!
//! **The set of SUBMITs a connection gets accepted is always a prefix of what
//! it sent.** A connection that is told once — `REDIRECT` or
//! `RETRY{not_serving}` — that this node cannot take writes is told the same
//! thing for every later SUBMIT on that connection, even if this node wins the
//! election a microsecond later. [`Conn::latch_not_serving`] carries the full
//! argument; the short version is that `Sessioned`'s FRESH/REPLAYED/EXPIRED
//! classification assumes it, and without it a mid-window role change makes a
//! client's re-sends read as `EXPIRED` — "outcome unknowable" — for requests
//! that were never applied at all.
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
//! ## Telling a client the cluster moved
//!
//! Two mechanisms, and they answer different questions.
//!
//! **Reactive** — a `SUBMIT` that arrives while `!can_serve()` (or completes
//! `NotLeader`) is answered `REDIRECT` against the static member map. That
//! covers every client with a request in flight.
//!
//! **Proactive** — the [`crate::watch::LeaderWatch`], polled by the driver,
//! pushes `LEADER_CHANGED` to every ready connection when `can_serve` or
//! `leader_hint` changes *and the new hint names a member we have an address
//! for*. That covers the client which has nothing in flight, or whose requests
//! are all parked on a backoff, and which would otherwise learn nothing until
//! it happened to try again.
//!
//! Both mechanisms share one rule: **never send a client somewhere it cannot
//! go.** With no resolvable leader, the reactive path answers
//! `RETRY{not_serving}` and the proactive path says nothing at all; the
//! "leader unknown" sentinel is reserved for `on_instance_restart`, where the
//! client genuinely has to go and look elsewhere.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use uc_client::{
    ClientError, Completion, Consistency, Engine, EngineConfig, Outcome, PollHalf, SendHalf,
    SubmitError,
};
use uc_log::cnc::CncPage;
use uc_remote::conn::FramedConn;
use uc_remote::frame::{
    encode_frame, FLAG_ENVELOPED, FLAG_EXPIRED, FLAG_IS_QUERY, FLAG_LINEARIZABLE, FLAG_REPLAYED,
    FrameType, HELLO_REFUSED_APP_ID, HELLO_REFUSED_BUSY, HELLO_REFUSED_FAULTED,
    HELLO_REFUSED_VERSION, Header, Hello, HelloOk, HelloRefused, Leader, PROTOCOL_VERSION,
    RETRY_NOT_SERVING, RETRY_PAYLOAD_TOO_LARGE, RETRY_SERVICE_UNAVAILABLE, ResponseMeta, Retry,
    Status,
};
use uc_protocol::ring::RingWaitHandle;

use crate::conn::{CeilingChange, Conn, ConnTable};
use crate::config::{ConfigError, EdgeConfig};
use crate::watch::LeaderWatch;

// ---------------------------------------------------------------- constants

/// The node's control page under an instance directory. Same well-known name
/// `uc_node::InstanceDir` writes and `uc_client::Engine` opens; the edge
/// opens it a second time purely to learn the node's `max_payload`, which the
/// `Engine` inherits but does not expose.
const CNC_FILE: &str = "cnc2.dat";

/// The `Sessioned` response tag, mirrored from `uc_service::session` — the
/// gateway does not depend on the service crate (an edge links no state
/// machine, and the whole point of the raw tier is that the relay never needs
/// one), so the four envelope constants are pinned here instead. They are part
/// of the wire contract, so `the_session_envelope_constants_match_uc_service`
/// below asserts they still agree with `uc_service`'s definitions — a drift
/// there would silently mislabel every response.
const TAG_FRESH: u8 = 0;
const TAG_REPLAYED: u8 = 1;
const TAG_EXPIRED: u8 = 2;
/// `client_id ++ seq`, little-endian — `uc_service::SESSION_HEADER_LEN`.
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
/// Driver idle ladder, copied in shape from `uc_client::pipelined`'s driver:
/// spin, then yield, then park on the egress ring's wake word.
const DRIVER_SPINS: u32 = 10;
const DRIVER_YIELDS: u32 = 20;
const DRIVER_PARK: Duration = Duration::from_millis(1);
/// How often the driver runs its periodic work (status timer, leader watch)
/// while completions are streaming in without a break.
const DRIVER_PERIODIC_EVERY: u64 = 64;

/// The fraction of the `Engine` window the edge keeps out of the grant
/// budget: `1/8`.
///
/// Deliberately a constant and **not** a config key. It is not a tuning
/// dial — it is the slack that makes a *shrinking* grant safe: a client that
/// is told a smaller absolute number honours it for new seqs, but the frames
/// it already put on the wire are still owed `Engine` slots. It also covers
/// the brief lag between a connection's own `HELLO_OK` (written outside
/// `Shared::grant_lock`, so it can carry a number a concurrent join has
/// since lowered) and the `STATUS` that corrects it. An operator who wants a
/// smaller sum lowers `per_conn_inflight`; one who wants a bigger one raises
/// `max_inflight`.
pub const BUDGET_HEADROOM_DIV: u32 = 8;

/// The edge's **total** outstanding-grant budget: the `Engine` window less
/// the headroom above. The sum of what every connection has been granted
/// stays at or under this (see [`grant_for`] for the one documented
/// exception).
pub fn budget_for(max_inflight: u32) -> u32 {
    max_inflight.saturating_sub(max_inflight / BUDGET_HEADROOM_DIV).max(1)
}

/// One connection's share of the budget: an equal split, capped by the
/// operator's `per_conn_inflight` and floored at 1.
///
/// The floor is the documented exception to "the sum is at most the budget":
/// once `live > budget` every connection would be entitled to zero, and a
/// zero grant wedges a connection forever (the same reason
/// [`crate::conn::Conn::squeeze`] floors at 1). `EdgeConfig::validate` warns
/// when `max_connections > budget_for(max_inflight)`, which is exactly the
/// configuration in which that can happen.
pub fn grant_for(live: u32, budget: u32, per_conn: u32) -> u32 {
    (budget / live.max(1)).clamp(1, per_conn.max(1))
}

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
    leader_changed_frames: AtomicU64,
    status_frames: AtomicU64,
    refused_busy: AtomicU64,
    grant_changes: AtomicU64,
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
            leader_changed_frames: self.leader_changed_frames.load(Ordering::Relaxed),
            status_frames: self.status_frames.load(Ordering::Relaxed),
            refused_busy: self.refused_busy.load(Ordering::Relaxed),
            grant_changes: self.grant_changes.load(Ordering::Relaxed),
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
///
/// `leader_changed_frames` is the one exception, and deliberately: it counts
/// frames that actually went out. Every other counter answers a request, so a
/// failed write leaves a client that at least knows it asked; an unsolicited
/// push at a socket that just died leaves nobody knowing anything, and the
/// connection is dropped. Counting it would overstate how many clients were
/// told — which is the only thing the number is for.
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
    /// Leader **transitions** the watch observed: `can_serve` flipped, or
    /// `leader_hint` changed. Counted whether or not any client was told —
    /// including a transition to an unresolvable hint (mid-election), which is
    /// deliberately not announced (see `watch.rs`).
    pub leader_changes: u64,
    /// `LEADER_CHANGED` frames successfully written back — the watch's pushes
    /// (one per ready connection per announced transition) plus the
    /// instance-restart notice. See the type doc for why this one counts after
    /// the write, not before.
    pub leader_changed_frames: u64,
    /// Standalone `STATUS` frames written back — the idle-liveness tick and
    /// the credit-reopened announcement. Never counted before `HELLO_OK`.
    pub status_frames: u64,
    /// Connections refused at the door with `HELLO_REFUSED{BUSY}` because the
    /// edge was already at `max_connections`. Not counted under `connections`,
    /// which counts connections actually taken on.
    pub refused_busy: u64,
    /// Times a connection's grant was recomputed to a **different** value —
    /// the edge redividing its budget as connections come and go. Counted per
    /// connection per change, both directions.
    pub grant_changes: u64,
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
    /// The total outstanding grant this edge will hand out across every
    /// connection — [`budget_for`] of the `Engine` window. Fixed at start;
    /// what moves is how it is divided.
    budget: u32,
    /// Connections counted into the budget: handshaken, not yet departed.
    live: AtomicU32,
    /// A connect or a disconnect has changed the share; the driver's next
    /// pass republishes it. A flag rather than a queue because the work is
    /// idempotent — recompute every connection's share from `live` — so
    /// coalescing two triggers into one pass is correct, not a shortcut.
    regrant: AtomicBool,
    /// Serializes `live`, every connection's `ceiling`, and a joining
    /// connection's transition to `ready` into ONE critical section — see
    /// [`Shared::join_and_grant`] for why the ready-transition has to be
    /// inside it too. No socket I/O ever happens while this is held: the
    /// critical section is atomic swaps and a table walk, so it never blocks
    /// on a peer.
    grant_lock: Mutex<()>,
    /// Ready connections a [`Shared::push_grants`] pass owes a `STATUS`
    /// about a share that just SHRANK. Appended only under `grant_lock` (by
    /// [`Shared::recompute_locked`], which both a joining handshake and the
    /// driver call under it), drained only by the driver's `push_grants` —
    /// the only thread that may write to a connection other than its own.
    owed_status: Mutex<Vec<u32>>,
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

    /// A leader hint turned into something a client can act on: an id AND an
    /// address. `None` — no hint, or a hint naming a node absent from the
    /// static member map — is the whole reason `REDIRECT` has a `RETRY`
    /// alternative and the watch has a "say nothing" case.
    fn resolve_leader(&self, hint: Option<u32>) -> Option<(u32, &str)> {
        hint.and_then(|id| self.gateway_of(id).map(|addr| (id, addr)))
    }

    /// Answer a request this node cannot serve: `REDIRECT` to the leader's
    /// gateway when the hint resolves to a member, otherwise `RETRY` — there
    /// is no address to send the client to, and inventing one is worse than
    /// telling it to wait out the election.
    fn redirect_or_retry(&self, conn: &Conn, seq: u64, hint: Option<u32>) {
        match self.resolve_leader(hint) {
            Some((id, addr)) => {
                let out = encode_leader(id, addr);
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

    /// Push `LEADER_CHANGED` naming a **resolved** leader to every connection
    /// whose handshake has completed. Driven only by the leader watch, and
    /// only on an announceable transition — see `watch.rs` for why the trigger
    /// is edge-triggered, and why an unresolvable hint is silent rather than
    /// pushed as the unknown sentinel.
    ///
    /// The body is built **once** and the table lock is not held across any
    /// write (`for_each` hands out a snapshot); a connection whose write fails
    /// is dropped afterwards, outside that snapshot walk, because `remove`
    /// takes the table's write lock.
    fn push_leader_changed(&self, node_id: u32, addr: &str) {
        let body = encode_leader(node_id, addr);
        let now = self.now_ns();
        let mut dead: Vec<u32> = Vec::new();
        self.table.for_each(|c| {
            // Never on a still-dialing connection: its peer is waiting for
            // `HELLO_OK` and would fail the dial on anything else (see
            // `Conn::ready`). It loses nothing — its own `HELLO_OK` carries
            // the leader that is current when the handshake completes.
            if !c.is_ready() {
                return;
            }
            if c.write(c.hdr(FrameType::LeaderChanged, 0, 0), &body, now) {
                self.stats.leader_changed_frames.fetch_add(1, Ordering::Relaxed);
            } else {
                dead.push(c.idx);
            }
        });
        for idx in dead {
            self.table.remove(idx);
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
    /// would fail the peer's dial (see `Conn::ready`). Returns `false` if the
    /// connection died on the write.
    fn write_status(&self, conn: &Conn) -> bool {
        if !conn.is_ready() {
            return true;
        }
        let mut out = Vec::new();
        Status { acked_seq: conn.acked_seq(), credits: conn.credits() }.encode(&mut out);
        self.stats.status_frames.fetch_add(1, Ordering::Relaxed);
        conn.write(conn.hdr(FrameType::Status, 0, 0), &out, self.now_ns())
    }

    /// Batch variants of the three answer helpers above: they encode the frame
    /// into `buf` (bumping the same counters) instead of writing it
    /// immediately. The driver funnels a whole completion drain's answers for
    /// one connection into one `buf` and flushes it in a single `write_all`
    /// (see [`driver`]). Bytes are length-prefixed, so the concatenation parses
    /// back to whole frames on the client. `_into` never touches the socket, so
    /// it cannot fail here — a flush failure is handled once, at flush time.
    fn redirect_or_retry_into(&self, buf: &mut Vec<u8>, conn: &Conn, seq: u64, hint: Option<u32>) {
        match self.resolve_leader(hint) {
            Some((id, addr)) => {
                let body = encode_leader(id, addr);
                self.stats.redirects.fetch_add(1, Ordering::Relaxed);
                encode_frame(buf, conn.hdr(FrameType::Redirect, 0, seq), &body);
            }
            None => {
                let mut out = Vec::new();
                Retry { reason: RETRY_NOT_SERVING, retry_after_us: NOT_SERVING_BACKOFF_US }
                    .encode(&mut out);
                self.stats.retries.fetch_add(1, Ordering::Relaxed);
                encode_frame(buf, conn.hdr(FrameType::Retry, 0, seq), &out);
            }
        }
    }

    fn write_retry_into(&self, buf: &mut Vec<u8>, conn: &Conn, seq: u64, reason: u8, after_us: u32) {
        let mut out = Vec::new();
        Retry { reason, retry_after_us: after_us }.encode(&mut out);
        self.stats.retries.fetch_add(1, Ordering::Relaxed);
        encode_frame(buf, conn.hdr(FrameType::Retry, 0, seq), &out);
    }

    /// Like [`Shared::write_status`], but appends to `buf`. Still silent on a
    /// connection whose handshake has not completed.
    fn write_status_into(&self, buf: &mut Vec<u8>, conn: &Conn) {
        if !conn.is_ready() {
            return;
        }
        let mut out = Vec::new();
        Status { acked_seq: conn.acked_seq(), credits: conn.credits() }.encode(&mut out);
        self.stats.status_frames.fetch_add(1, Ordering::Relaxed);
        encode_frame(buf, conn.hdr(FrameType::Status, 0, 0), &out);
    }

    /// The node's shmem identity changed underneath us: every request in
    /// flight is unanswerable and every connection's session is void. Tell
    /// each client the leader is unknown (which makes `RemoteClient`
    /// reconnect and re-`HELLO`) and drop the lot.
    fn on_instance_restart(&self) {
        // Latch first: a connection accepted between here and the last close
        // must be refused at the handshake, not served into the same fault.
        self.faulted.store(true, Ordering::SeqCst);
        // The ONLY place the unknown-leader sentinel goes on the wire. Here it
        // is exactly right — there is nothing this edge can still answer, so
        // "reconnect and re-`HELLO` somewhere" is the true instruction. The
        // watch deliberately never sends it (see `watch.rs`): mid-election it
        // would scatter every client off a working connection.
        let out = encode_leader(u32::MAX, "");
        for c in self.table.take_all() {
            // A connection still mid-handshake gets no frame, only the close:
            // its peer is waiting for HELLO_OK and would reject anything else.
            if c.is_ready()
                && c.write(c.hdr(FrameType::LeaderChanged, 0, 0), &out, self.now_ns())
            {
                self.stats.leader_changed_frames.fetch_add(1, Ordering::Relaxed);
            }
            c.close();
        }
    }

    fn is_faulted(&self) -> bool {
        self.faulted.load(Ordering::SeqCst)
    }

    // ---------------------------------------------------------- the budget
    //
    // `grant_lock` is the whole fix (m13 §5 as-built erratum): the ORIGINAL
    // mechanism sampled a generation counter, called `join`, and polled for
    // the counter to move (`await_settled`) — which cannot tell "a push that
    // already accounts for MY `live++`" from "a push that ran just before
    // it", so two connections racing to join could both read a driver pass
    // that saw a stale, smaller `live` and both go ready over-granted. The
    // fix serializes the WHOLE sequence — count this connection in, recompute
    // every OTHER ready connection, compute this connection's own share, and
    // mark it ready — as one critical section, so "every `is_ready`
    // connection" a concurrent pass sees is always "every connection this
    // pass must account for". See `join_and_grant`.

    /// Every ready connection's current share, from the current `live`.
    fn current_grant(&self) -> u32 {
        grant_for(self.live.load(Ordering::Acquire), self.budget, self.cfg.per_conn_inflight)
    }

    /// Ask the driver to republish the share. Idempotent and free.
    fn request_regrant(&self) {
        self.regrant.store(true, Ordering::Release);
    }

    /// Recompute every **ready** connection's ceiling from the current
    /// `live`, queuing a `STATUS` for any whose share just SHRANK.
    ///
    /// Caller must hold `grant_lock`. Pure atomic work and one table walk —
    /// no socket I/O — which is what makes it safe to call from a
    /// handshaking reader thread ([`Shared::join_and_grant`]) as well as the
    /// driver ([`Shared::push_grants`]): neither may block on a peer's
    /// socket while holding the lock that every OTHER connect/disconnect is
    /// waiting on.
    fn recompute_locked(&self) {
        let grant = self.current_grant();
        let mut owed = self.owed_status.lock();
        self.table.for_each(|c| {
            if !c.is_ready() {
                return;
            }
            match c.set_ceiling(grant) {
                CeilingChange::Same => {}
                CeilingChange::Raised => {
                    self.stats.grant_changes.fetch_add(1, Ordering::Relaxed);
                }
                CeilingChange::Lowered => {
                    self.stats.grant_changes.fetch_add(1, Ordering::Relaxed);
                    owed.push(c.idx);
                }
            }
        });
    }

    /// Count a handshaken connection into the budget, recompute every OTHER
    /// ready connection's share to match, and mark THIS connection ready —
    /// all inside one `grant_lock` critical section. Returns the grant this
    /// connection's own `HELLO_OK` should carry; the caller writes that frame
    /// itself, on its own socket, AFTER this returns (never under the lock —
    /// a socket write can block for up to `WRITE_TIMEOUT`, and holding
    /// `grant_lock` across that would stall every other connect, disconnect,
    /// and the driver).
    ///
    /// ## Why `Conn::set_ready` has to happen INSIDE the lock
    ///
    /// `recompute_locked` (both here and in the driver's `push_grants`) treats
    /// "every `is_ready` connection" as "every connection this pass must
    /// account for". For the whole of this method, THIS connection is
    /// mid-handshake — the exact state the old mechanism could leave visible
    /// to nobody (counted in `live`, but not yet reflected in anyone's
    /// recompute, and not holding anything that would stop a concurrent pass
    /// from proceeding without it). Doing `join` and `set_ready` in the SAME
    /// critical section closes that: a third connection's concurrent
    /// `join_and_grant` cannot even START its own `recompute_locked` until
    /// this one releases the lock, by which point this connection is either
    /// not yet counted (not joined) or fully counted AND ready (so the third
    /// connection's recompute sees and lowers it like any other survivor).
    /// There is no window in between that a lock-holder can observe.
    ///
    /// This connection's own `HELLO_OK`, written after the lock is released,
    /// can still race a LATER join that lowers this connection's share again
    /// before the frame goes out — the grant it carries can be stale-high by
    /// the time the client reads it. That is the one place lag survives, and
    /// it is exactly what [`BUDGET_HEADROOM_DIV`]'s slack is for: the
    /// connection's ATOMIC `ceiling`/`credits` (what `wait_for_credit`
    /// actually enforces, and what [`Edge::grants_for_tests`] samples) are
    /// already correct the instant the lock is released; only the client's
    /// *belief*, from a HELLO_OK in flight, can lag — and the driver's
    /// `push_grants` corrects it with a `STATUS` shortly after.
    fn join_and_grant(&self, conn: &Conn) -> u32 {
        let _guard = self.grant_lock.lock();
        if conn.mark_counted() {
            self.live.fetch_add(1, Ordering::AcqRel);
        }
        // Other ready connections first: `conn` itself is not yet ready, so
        // this cannot touch (or double-count) it.
        self.recompute_locked();
        let grant = self.current_grant();
        conn.set_ceiling(grant);
        // MUST be inside the lock — see the doc above.
        conn.set_ready();
        drop(_guard);
        // Never from this reader thread: `push_grants` is the only thread
        // allowed to write to a connection other than its own.
        self.request_regrant();
        grant
    }

    /// Take a departed connection back out. Its share is freed, so the
    /// remaining connections are owed a bigger one — but raising them is NOT
    /// done here: an over-counted `live` only ever makes grants too SMALL,
    /// never lets the sum exceed the budget, so the safe direction is left to
    /// the driver's next `push_grants` rather than run under this reader
    /// thread's exit.
    fn leave(&self, conn: &Conn) {
        {
            let _guard = self.grant_lock.lock();
            if conn.clear_counted() {
                self.live.fetch_sub(1, Ordering::AcqRel);
            }
        }
        self.request_regrant();
    }

    /// Recompute every ready connection's share and flush every `STATUS` a
    /// shrunk share is owed. Driver thread only — it is the only thread that
    /// may write on a connection other than its own.
    ///
    /// Unconditional about what to send, not "whatever changed in THIS
    /// call": a joining connection's own `join_and_grant` may already have
    /// applied a reduction to another connection's ceiling (queuing it in
    /// `owed_status`) before this ever runs, in which case this call's own
    /// `recompute_locked` sees no further change on that connection — the
    /// queue, not the recompute's return, is what this drains and sends.
    fn push_grants(&self) {
        {
            let _guard = self.grant_lock.lock();
            self.recompute_locked();
        }
        let owed: Vec<u32> = std::mem::take(&mut *self.owed_status.lock());
        let mut dead: Vec<u32> = Vec::new();
        for idx in owed {
            if let Some(c) = self.table.get(idx)
                && !self.write_status(&c)
            {
                dead.push(idx);
            }
        }
        for idx in dead {
            self.table.remove(idx);
        }
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

        let budget = budget_for(cfg.max_inflight);
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
            budget,
            live: AtomicU32::new(0),
            regrant: AtomicBool::new(false),
            grant_lock: Mutex::new(()),
            owed_status: Mutex::new(Vec::new()),
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
    /// `uc_client` documents re-attach as a v2.0 decision, and a relay that
    /// silently reconnected to a *different* node incarnation would be
    /// answering with a session table the client never established. A
    /// supervisor (systemd) restarting the gateway is the intended recovery.
    pub fn is_faulted(&self) -> bool {
        self.shared.is_faulted()
    }

    /// Force the faulted state, for tests that need to observe the refusal
    /// without racing a real node restart.
    ///
    /// Behind the non-default `test-util` feature rather than merely
    /// `#[doc(hidden)]`: hiding a method from rustdoc does not stop anything
    /// from calling it, and this one takes a live edge permanently out of
    /// service. Nothing outside a test has any business reaching it, so the
    /// build — not the documentation — is what says so.
    #[cfg(feature = "test-util")]
    pub fn fault_for_tests(&self) {
        self.shared.on_instance_restart();
    }

    /// Every **ready** connection's `(idx, grant)` right now, sorted by index.
    ///
    /// "Grant" is the connection's live credit figure — what the client is
    /// actually allowed to have outstanding — not its ceiling, so a squeezed
    /// connection reports the smaller number. The sum of these is the quantity
    /// the budget bounds.
    ///
    /// Behind `test-util` for the same reason as [`Edge::fault_for_tests`]:
    /// hiding a method from rustdoc does not stop anything calling it, and the
    /// build, not the documentation, is what says who may.
    #[cfg(feature = "test-util")]
    pub fn grants_for_tests(&self) -> Vec<(u32, u32)> {
        let mut v = Vec::new();
        self.shared.table.for_each(|c| {
            if c.is_ready() {
                v.push((c.idx, c.credits()));
            }
        });
        v.sort_unstable();
        v
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
                // The connection ceiling, enforced HERE rather than by letting
                // the reader thread spawn and refuse: one thread and one socket
                // per connection is the resource being capped, so the cap has
                // to bite before either is committed. Only this thread inserts
                // into the table, so the check-then-insert below is a hard
                // bound even though it spans no lock (`ConnTable::len`).
                if shared.table.len() >= shared.cfg.max_connections as usize {
                    refuse_busy(&shared, stream);
                    continue;
                }
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

/// Turn a connection away at the ceiling with `HELLO_REFUSED{BUSY}`, without
/// spawning a reader for it.
///
/// The frame goes out *before* the peer's `HELLO` is read, which is exactly
/// what the client's dial expects: it writes `HELLO` and then reads whatever
/// comes back, so a refusal already in the socket buffer is read as the
/// answer. A refusal frame rather than a bare close is what lets a client tell
/// "this member is full, try the next one" from "the network ate my
/// connection" — and `BUSY`, unlike `FAULTED`, says the condition is
/// transient.
fn refuse_busy(shared: &Arc<Shared>, stream: TcpStream) {
    shared.stats.refused_busy.fetch_add(1, Ordering::Relaxed);
    let Ok(mut fc) = FramedConn::new(stream) else { return };
    // A bounded write: this runs on the acceptor thread, which must not be
    // held hostage by a peer that never reads.
    if fc.set_write_timeout(Some(WRITE_TIMEOUT)).is_err() {
        return;
    }
    let mut out = Vec::new();
    HelloRefused {
        reason: HELLO_REFUSED_BUSY,
        detail: "edge at max_connections; try another member",
    }
    .encode(&mut out);
    let h = Header {
        ty: FrameType::HelloRefused,
        flags: 0,
        version: PROTOCOL_VERSION,
        client_id: 0,
        seq: 0,
    };
    let _ = fc.write_frame(h, &out);
    fc.shutdown();
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
            // A peer that vanishes MID-FRAME must not pin this thread until
            // the edge stops: `request_timeout` is the same budget the
            // request behind that frame would have had anyway.
            match fc.read_frame_buffered(shared.cfg.request_timeout) {
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
    // Order matters: out of the table FIRST, so this connection's grant is
    // invisible before `leave` lets the survivors grow into its share — the
    // reverse order would over-promise the budget for as long as the driver's
    // republication took.
    shared.table.remove(conn.idx);
    shared.leave(&conn);
}

/// Read the client's `HELLO`, check it, and answer. Returns `false` if the
/// connection is finished (refused, malformed, or timed out).
fn handshake(shared: &Arc<Shared>, conn: &Arc<Conn>, send: &SendHalf, fc: &mut FramedConn) -> bool {
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    let (h, payload) = loop {
        if shared.stopping() || conn.is_closed() || Instant::now() >= deadline {
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
    conn.set_client_id(h.client_id);

    // Identity checks come FIRST, before the edge's own health. A client that
    // dialled the wrong cluster or speaks the wrong protocol must hear that —
    // `APP_ID`/`VERSION` are terminal everywhere, while `FAULTED` invites the
    // client to try another member, which for a wrong-cluster dial would send
    // it round the whole member list to be refused again at each one.
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

    // Hold THIS connection's writer lock from BEFORE `join_and_grant`
    // through the HELLO_OK write below — not the plain `Conn::write`, which
    // would re-lock and release it, leaving a window open.
    //
    // `join_and_grant` flips `Conn::set_ready` inside `grant_lock`, which is
    // what the budget invariant needs (see its doc), but it also makes this
    // connection a legal target for the driver's `push_grants`/leader-watch
    // pushes the instant that lock releases — *before* HELLO_OK is
    // physically on the wire. Without holding the writer across that gap,
    // a thread preempted here (unbounded under load, not just a few
    // instructions) can let a `STATUS`/`LEADER_CHANGED` win the race for
    // this socket ahead of its own handshake reply, violating "nothing
    // precedes HELLO_OK" (`credits_wire.rs::
    // no_frame_precedes_hello_ok_and_status_follows_it`). Holding the
    // writer here forces any such push to block on the SAME mutex until
    // this write is done, so it can never land first.
    //
    // Lock order is `writer(conn) -> grant_lock`, and it must never be the
    // reverse anywhere: `push_grants` releases `grant_lock` before it ever
    // touches a `writer` (see its doc), `recompute_locked` never touches a
    // `writer` at all, and `leave`/`join_and_grant` never take another
    // connection's `writer`. So there is no cycle for this order to invert
    // against.
    let mut w = conn.lock_writer();

    // Join the budget and recompute every other connection's share, THIS
    // connection's own share, and its ready-transition, all as one atomic
    // step under `grant_lock` — see `Shared::join_and_grant` for why the
    // ready-transition has to be inside that same critical section. The
    // returned grant is what HELLO_OK carries.
    let grant = shared.join_and_grant(conn);

    let leader = send.leader_hint();
    let leader_addr = leader.and_then(|id| shared.gateway_of(id)).unwrap_or("");
    let mut out = Vec::new();
    HelloOk { credits: grant, leader, leader_addr }.encode(&mut out);
    let ok = conn.write_locked(&mut w, conn.hdr(FrameType::HelloOk, 0, h.seq), &out, shared.now_ns());
    drop(w);
    ok
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

/// What the `Backpressure` ladder in [`dispatch`] should do next.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ladder {
    /// Nothing has changed: hand the request to the `Engine` again.
    Try,
    /// The edge is stopping, or the connection died under us. Nobody is left
    /// to answer.
    Gone,
    /// This connection was latched not-serving while the request was parked.
    NotServing,
    /// The request burned its whole `request_timeout` against a full engine.
    OutOfBudget,
}

/// Decide what a parked request should do, from the state of the world at the
/// top of one ladder iteration.
///
/// ## Why the not-serving latch is re-read HERE and not only at the door
///
/// The door check runs once, before the request is handed to the `Engine`. A
/// SUBMIT that then parks on `Backpressure` can sit here for its whole
/// `request_timeout` — and a connection's role is decided by the *driver*
/// thread, which is meanwhile answering a completion for an EARLIER SUBMIT on
/// this same connection. So:
///
/// 1. SUBMIT A is accepted by the engine; SUBMIT B parks here on backpressure.
/// 2. A completes `NotLeader` → the driver answers `REDIRECT` and calls
///    [`Conn::latch_not_serving`].
/// 3. This node wins the election a moment later and the engine drains.
/// 4. Without this check, B's next `try_submit` succeeds — the connection got
///    B accepted after A was refused, so the accepted set is no longer a
///    PREFIX of what was sent.
/// 5. `Sessioned`'s `highest_seq` for that client is now B. The client, which
///    acted on A's `REDIRECT` and reconnected, re-sends A — `seq <=
///    highest_seq` with no cached response, which classifies as **EXPIRED**:
///    "outcome unknowable" for a request that provably never committed.
///
/// The latch is exactly the state that says "this connection has been told
/// no", so re-reading it before every attempt is what keeps the prefix
/// invariant true for a parked request as well as a fresh one. A QUERY is
/// answerable by any replica and never latches, so it is unaffected.
fn ladder_step(
    conn: &Conn,
    is_query: bool,
    stopping: bool,
    now: Instant,
    deadline: Instant,
) -> Ladder {
    if stopping || conn.is_closed() {
        return Ladder::Gone;
    }
    if !is_query && conn.is_not_serving() {
        return Ladder::NotServing;
    }
    if now >= deadline {
        return Ladder::OutOfBudget;
    }
    Ladder::Try
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
    // (`uc_client/src/engine.rs`, `SendHalf::send`), and the arm below handles
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
    //
    // `is_not_serving()` is checked FIRST and is sticky: once this connection
    // has been told "not here" for one SUBMIT, every later SUBMIT on it gets
    // the same answer even if this node wins the election in between. That is
    // the prefix invariant — see `Conn::latch_not_serving`, which explains why
    // breaking it turns provably-uncommitted requests into `EXPIRED`.
    if !is_query && (conn.is_not_serving() || !send.can_serve()) {
        conn.latch_not_serving();
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
        // Re-checked at the TOP of every iteration, not just in the
        // `Backpressure` arm — the world can change while a SUBMIT is parked
        // here, and the next `try_submit` must not be allowed to succeed once
        // it has. See `ladder_step`.
        match ladder_step(conn, is_query, shared.stopping(), Instant::now(), deadline) {
            Ladder::Try => {}
            Ladder::Gone => {
                conn.unreserve(corr);
                return false;
            }
            Ladder::NotServing => {
                if conn.unreserve(corr) {
                    shared.redirect_or_retry(conn, h.seq, send.leader_hint());
                }
                return !conn.is_closed();
            }
            Ladder::OutOfBudget => {
                if conn.unreserve(corr) {
                    shared.write_retry(conn, h.seq, RETRY_SERVICE_UNAVAILABLE, RETRY_BACKOFF_US);
                }
                return !conn.is_closed();
            }
        }
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
                    // Tell the client its window just halved, rather than
                    // letting it find out from the next RESPONSE — by which
                    // point it has already sent into a window the edge cannot
                    // honour. This is the reader writing on its OWN
                    // connection, so it can neither stall the driver nor
                    // touch anyone else's socket. Once per request, not once
                    // per ladder turn: `squeezed` gates both.
                    if !shared.write_status(conn) {
                        return false;
                    }
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
                if !is_query {
                    conn.latch_not_serving();
                }
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
            Err(SubmitError::ServiceNotDeclared { .. }) => {
                // Unreachable: the edge never names a service id (protocol v1
                // has no selector), so every request goes to FSM 0. Handled
                // like `PayloadTooLarge` — a permanent door refusal, not a
                // transient the client should keep re-sending.
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
    // Seeded from the state as it is now, so starting on a healthy leader is
    // not itself reported as a leader change.
    let mut watch = LeaderWatch::new(&send);
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
        let n = drain_once(&shared, &send, &mut poll);
        if n > 0 {
            idle = 0;
            regrant_tick(&shared);
            // Under a stream of completions the driver may never take the idle
            // path at all, so the watch gets its own cadence here: one sample
            // (two atomic loads) per `DRIVER_PERIODIC_EVERY` completions.
            if cycle.is_multiple_of(DRIVER_PERIODIC_EVERY) {
                leader_tick(&shared, &send, &mut watch);
                maybe_periodic(&shared, &mut last_periodic, periodic_every);
            }
            continue;
        }
        // Every idle iteration, ungated: the sample is two atomic loads, and
        // the idle ladder's park is capped at `DRIVER_PARK`, so an idle edge
        // notices a leader change within about a millisecond. (The STATUS
        // timer below is gated instead — it walks the whole table.)
        leader_tick(&shared, &send, &mut watch);
        regrant_tick(&shared);
        maybe_periodic(&shared, &mut last_periodic, periodic_every);
        if shared.stopping() {
            break;
        }
        // Idle ladder, same shape as `uc_client::pipelined`'s driver: spin,
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
            if drain_once(&shared, &send, &mut poll) == 0 && !shared.stopping() {
                wh.park(seq, DRIVER_PARK);
            }
        }
    }
    // Nothing will answer what is still in flight; release the slots so the
    // engine's own accounting closes out cleanly.
    poll.drain_abort(|_| {});
}

/// Sample the leader watch and, on a transition, tell every ready client.
///
/// Cheap by construction on the overwhelmingly common no-change path: two
/// atomic loads and a comparison, no allocation and no table lock. Only a real
/// transition reaches [`Shared::push_leader_changed`].
fn leader_tick(shared: &Arc<Shared>, send: &SendHalf, watch: &mut LeaderWatch) {
    let t = watch.poll(send, |id| shared.resolve_leader(Some(id)));
    if t.changed {
        // The cluster moved. Counted even when nobody is told — an operator
        // watching an edge with no clients on it still wants to see elections.
        shared.stats.leader_changes.fetch_add(1, Ordering::Relaxed);
    }
    // What goes on the wire is the CURRENT leader, and ONLY when there is one
    // to name: the client acts on the address, so "where do I go now" is the
    // only useful thing to say, and saying "I don't know" would scatter every
    // connected client (see `watch.rs`).
    if let Some((id, addr)) = t.announce {
        shared.push_leader_changed(id, addr);
    }
}

/// Republish the grant share if a connect or a disconnect has changed it.
///
/// Cheap by construction on the no-change path — one atomic swap — which is
/// every pass but the ones right after a connection arrives or leaves. Runs
/// ungated on both the busy and the idle path so a waiting handshake is
/// released within one driver iteration (bounded by [`DRIVER_PARK`]).
fn regrant_tick(shared: &Arc<Shared>) {
    if shared.regrant.swap(false, Ordering::AcqRel) {
        shared.push_grants();
    }
}

/// Run [`periodic`] at most once per `every`.
fn maybe_periodic(shared: &Arc<Shared>, last: &mut Instant, every: Duration) {
    let now = Instant::now();
    if now.duration_since(*last) >= every {
        *last = now;
        periodic(shared);
    }
}

/// The driver's between-polls work: the standalone `STATUS` timer.
fn periodic(shared: &Arc<Shared>) {
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
            let _ = shared.write_status(c);
        }
    });
}

/// A completion drain's answers, grouped by connection so each connection's
/// frames flush in one `write_all`. Keyed by `conn.idx`; the value carries the
/// `Arc<Conn>` (so it survives the drain even if the table drops it) and the
/// concatenated, already-encoded frame bytes, appended in completion order.
type DriverBatch = HashMap<u32, (Arc<Conn>, Vec<u8>)>;

/// Append an already-formed frame to a connection's slot in the drain batch,
/// keeping the `Arc<Conn>` alive for the flush.
fn push_frame(batch: &mut DriverBatch, conn: &Arc<Conn>, h: Header, payload: &[u8]) {
    let e = batch.entry(conn.idx).or_insert_with(|| (Arc::clone(conn), Vec::new()));
    encode_frame(&mut e.1, h, payload);
}

/// Drain one wave of `Engine` completions, accumulating each connection's
/// answers, then flush every touched connection in a single `write_all`
/// (flush-on-empty: the flush is per drain, on no timer). Returns the number of
/// completions handled, so the driver's idle ladder is unchanged.
///
/// The batching is invisible to the exactly-once and prefix invariants: it
/// changes only how a connection's answer bytes reach the socket, never which
/// requests are accepted (that is the reader's job) nor which answer each
/// completion earns (decided per completion below, exactly as before). Frames
/// for one connection keep their completion order; order across connections
/// never mattered (a client resolves by `seq`).
fn drain_once(shared: &Arc<Shared>, send: &SendHalf, poll: &mut PollHalf) -> usize {
    let mut batch: DriverBatch = HashMap::new();
    let mut restart = false;
    let n = poll.poll(|c| complete(shared, send, c, &mut batch, &mut restart));
    // Flush the answers computed BEFORE any instance-restart completion — they
    // are real answers a live instance produced and are owed to their clients —
    // then take the whole edge out of service.
    let now = shared.now_ns();
    for (_, (conn, buf)) in batch.drain() {
        conn.write_batch(&buf, now);
    }
    if restart {
        shared.on_instance_restart();
    }
    n
}

/// Resolve one `Engine` completion into exactly one frame appended to `batch`
/// (or a drop). The frame is not written here — [`drain_once`] flushes the
/// whole drain per connection.
fn complete(
    shared: &Arc<Shared>,
    send: &SendHalf,
    c: Completion<'_>,
    batch: &mut DriverBatch,
    restart: &mut bool,
) {
    // Once a completion has reported the node's shmem instance restarted,
    // everything else in this drain is answering from a dead instance: stop
    // producing frames and let `drain_once` flush what came before and fault
    // the edge.
    if *restart {
        return;
    }
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
    let credits_up = conn.relax();
    conn.notify_gate();

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
            push_frame(batch, &conn, conn.hdr(FrameType::Response, flags, seq), &out);
            // The RESPONSE above already carried the new credits, so no
            // separate STATUS is owed here.
            return;
        }
        Outcome::NotLeader { hint } => {
            // Same latch as the door check: this SUBMIT was refused for the
            // role, so nothing later on this connection may be accepted (see
            // `Conn::latch_not_serving`). A QUERY is answerable by any
            // replica, so it never latches.
            if !is_query {
                conn.latch_not_serving();
            }
            let (_, buf) =
                batch.entry(conn.idx).or_insert_with(|| (Arc::clone(&conn), Vec::new()));
            shared.redirect_or_retry_into(buf, &conn, seq, hint.or_else(|| send.leader_hint()));
        }
        Outcome::Retry => {
            let (_, buf) =
                batch.entry(conn.idx).or_insert_with(|| (Arc::clone(&conn), Vec::new()));
            shared.write_retry_into(buf, &conn, seq, RETRY_SERVICE_UNAVAILABLE, RETRY_BACKOFF_US);
        }
        Outcome::Responses(_) | Outcome::BadService { .. } => {
            // Unreachable on the edge: it only ever issues FSM-0 requests
            // (protocol v1 has no service selector, spec §6.4). Answer as a
            // transient so a client that somehow sees it retries.
            let (_, buf) =
                batch.entry(conn.idx).or_insert_with(|| (Arc::clone(&conn), Vec::new()));
            shared.write_retry_into(buf, &conn, seq, RETRY_SERVICE_UNAVAILABLE, RETRY_BACKOFF_US);
        }
        Outcome::TimedOut => {
            // "May or may not have committed" — the client re-sends, and with
            // the session envelope on the re-send is answered `replayed`.
            shared.stats.unknown.fetch_add(1, Ordering::Relaxed);
            push_frame(batch, &conn, conn.hdr(FrameType::Unknown, 0, seq), &[]);
        }
        Outcome::InstanceRestart { .. } => {
            *restart = true;
            return;
        }
    }
    if credits_up {
        // None of the frames above carries a credit field, so an increase has
        // to be announced on its own.
        let (_, buf) = batch.entry(conn.idx).or_insert_with(|| (Arc::clone(&conn), Vec::new()));
        shared.write_status_into(buf, &conn);
    }
}

/// Encode a `Leader` body — the payload shared by `REDIRECT` and
/// `LEADER_CHANGED`.
fn encode_leader(node_id: u32, addr: &str) -> Vec<u8> {
    let mut out = Vec::new();
    Leader { node_id, addr }.encode(&mut out);
    out
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
    use std::net::TcpListener;

    /// A `Conn` over a real (connected, unused) socket — the accounting and
    /// latch bits are all this module's tests touch, never the wire.
    fn a_conn() -> Arc<Conn> {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let s = TcpStream::connect(l.local_addr().unwrap()).unwrap();
        let _accepted = l.accept().unwrap();
        Arc::new(Conn::new(0, FramedConn::new(s).unwrap(), 4, 0))
    }

    /// The fix for the parked-SUBMIT hole: a connection latched not-serving
    /// while a SUBMIT sat in the backpressure ladder must NOT get that SUBMIT
    /// accepted when the engine drains. See `ladder_step`'s doc for the
    /// EXPIRED-from-a-never-committed-request chain this prevents.
    ///
    /// Tested at this level rather than on the wire deliberately. Driving it
    /// end-to-end needs three things true at once — the engine full enough to
    /// answer `Backpressure`, an EARLIER submit on the SAME connection
    /// completing `NotLeader`, and this node then becoming servable — and the
    /// only lever that produces the middle one is a real role change under a
    /// full engine, which no in-process rig can schedule deterministically
    /// (making the engine full means nothing is completing, and the latch
    /// arrives on a completion). What IS deterministic is the decision itself,
    /// which is why it lives in one pure function.
    #[test]
    fn a_submit_latched_while_parked_is_refused_rather_than_accepted() {
        let conn = a_conn();
        let now = Instant::now();
        let deadline = now + Duration::from_secs(10);

        assert_eq!(
            ladder_step(&conn, false, false, now, deadline),
            Ladder::Try,
            "a fresh connection keeps trying"
        );
        conn.latch_not_serving();
        assert_eq!(
            ladder_step(&conn, false, false, now, deadline),
            Ladder::NotServing,
            "a SUBMIT must abandon the ladder the moment its connection is latched"
        );
        assert_eq!(
            ladder_step(&conn, true, false, now, deadline),
            Ladder::Try,
            "a QUERY is answerable by any replica, so the latch does not touch it"
        );
    }

    #[test]
    fn the_ladder_gives_up_when_the_edge_stops_or_the_connection_dies() {
        let conn = a_conn();
        let now = Instant::now();
        let deadline = now + Duration::from_secs(10);
        assert_eq!(ladder_step(&conn, false, true, now, deadline), Ladder::Gone, "stopping");
        // A dead connection outranks even the latch: nobody is left to answer.
        conn.latch_not_serving();
        conn.close();
        assert_eq!(ladder_step(&conn, false, false, now, deadline), Ladder::Gone);
    }

    #[test]
    fn the_ladder_gives_up_when_the_request_burns_its_budget() {
        let conn = a_conn();
        let now = Instant::now();
        assert_eq!(ladder_step(&conn, false, false, now, now), Ladder::OutOfBudget);
        assert_eq!(
            ladder_step(&conn, true, false, now, now),
            Ladder::OutOfBudget,
            "a QUERY has the same budget"
        );
    }

    /// The envelope constants are duplicated, not imported (see their doc
    /// above). `uc_service` is a dev-dependency, so this is the guard that
    /// keeps the copy honest.
    #[test]
    fn the_session_envelope_constants_match_uc_service() {
        assert_eq!(SESSION_HEADER_LEN, uc_service::SESSION_HEADER_LEN);
        assert_eq!(TAG_FRESH, uc_service::TAG_FRESH);
        assert_eq!(TAG_REPLAYED, uc_service::TAG_REPLAYED);
        assert_eq!(TAG_EXPIRED, uc_service::TAG_EXPIRED);
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

    #[test]
    fn the_budget_holds_back_an_eighth_of_the_engine_window() {
        assert_eq!(budget_for(4096), 3584, "4096 - 4096/8");
        assert_eq!(budget_for(8), 7);
        // Below the divisor the headroom rounds to nothing; the budget is then
        // the whole window, which is still a bound, just a tight one.
        assert_eq!(budget_for(4), 4);
        assert_eq!(budget_for(1), 1);
        assert_eq!(budget_for(0), 1, "a zero budget would wedge every connection");
    }

    #[test]
    fn a_grant_is_an_equal_share_capped_by_the_config_and_floored_at_one() {
        // One connection takes the whole budget, but never more than the
        // operator allowed it.
        assert_eq!(grant_for(1, 3584, 256), 256);
        assert_eq!(grant_for(1, 200, 256), 200, "the budget binds below the cap");
        // Equal shares.
        assert_eq!(grant_for(2, 56, 32), 28);
        assert_eq!(grant_for(4, 56, 32), 14);
        // The floor: past `live > budget` a share would round to zero, which
        // would wedge a connection forever. It is also the point past which
        // the sum can exceed the budget — `validate` warns about it.
        assert_eq!(grant_for(100, 56, 32), 1);
        assert_eq!(grant_for(0, 56, 32), 32, "no live connections reads as one");
    }

    #[test]
    fn grants_sum_within_the_budget_while_live_is_within_it() {
        for budget in [7u32, 56, 3584, 57344] {
            for live in 1..=budget.min(64) {
                let g = grant_for(live, budget, u32::MAX);
                assert!(
                    g * live <= budget,
                    "live={live} budget={budget} grant={g}: the sum over-promises"
                );
            }
        }
    }

    /// `grant_changes` counts redivisions, in both directions, per connection.
    /// A stat nobody can reach from `EdgeStats` is a stat nobody will read.
    #[test]
    fn the_stats_snapshot_exposes_grant_changes() {
        let s = EdgeStats::default();
        assert_eq!(s.grant_changes, 0);
        let cells = StatCells::default();
        cells.grant_changes.fetch_add(3, Ordering::Relaxed);
        assert_eq!(cells.snapshot().grant_changes, 3);
    }
}
