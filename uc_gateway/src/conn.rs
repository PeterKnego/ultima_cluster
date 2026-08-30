// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! One remote client connection, and the table of them.
//!
//! ## Who touches a [`Conn`]
//!
//! Exactly two kinds of thread:
//!
//! - its **reader** thread, which owns the read half of the socket, decodes
//!   frames, and hands SUBMIT/QUERY payloads to the `Engine`;
//! - the single **driver** thread, which drains `Engine` completions and
//!   writes the answers back.
//!
//! Both write frames, so the write half lives behind a [`Mutex`]. That lock is
//! held for exactly one `write_frame` and never across anything that can block
//! indefinitely: the socket carries a **write timeout**, so the worst case is
//! bounded, and a write that fails takes the connection down rather than
//! leaving a half-frame on the wire.
//!
//! ## Why the connection index is monotonic, not a reused slot
//!
//! `user_data = conn_idx << 32 | corr` correlates an `Engine` completion back
//! to a connection, and a completion can legitimately arrive *after* its
//! connection is gone (the client vanished mid-request; the engine still owes
//! exactly one completion). Those are dropped — which is only safe if the
//! index cannot have been handed to a *different* connection in the meantime.
//! So the table is a map keyed by an ever-increasing `u32`, not a free-list of
//! reusable slots: an ABA on `conn_idx` would deliver one client's response to
//! another client's socket, which is a correctness bug, not a leak. (The `u32`
//! does eventually wrap; that needs 2^32 accepted connections *and* a
//! still-outstanding completion from exactly one wrap ago, with the engine's
//! own `request_timeout` bounding how long any completion can be outstanding.)

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex, MutexGuard, RwLock};

use uc_remote::conn::FramedConn;
use uc_remote::frame::{FrameType, Header, PROTOCOL_VERSION};

/// How long a reader parked on the credit gate sleeps before re-checking the
/// stop flag and the connection's liveness. The gate is also notified
/// explicitly, so this is a backstop, not the mechanism.
const GATE_TICK: Duration = Duration::from_millis(20);

/// One accepted remote client connection.
pub(crate) struct Conn {
    /// Table key; the high 32 bits of every `user_data` this connection owns.
    pub idx: u32,
    /// The write half of the socket. See the module doc for the locking rule.
    writer: Mutex<FramedConn>,
    /// The `client_id` the peer asserted in `HELLO`; echoed in every frame we
    /// write and used as the session-envelope identity.
    client_id: AtomicU64,
    /// Credits currently granted to this connection: the peer may have at most
    /// this many requests — SUBMITs and queries alike — outstanding at once.
    /// A COUNT bound, not a `seq` window: a query never advances `acked_seq`, so
    /// phrasing it as "unanswered seqs beyond acked_seq" would starve a client
    /// after `credits` queries (uc_remote's window is the count-based
    /// `admissible`, not a seq comparison).
    credits: AtomicU32,
    /// The ceiling `credits` may climb back to — the connection's current
    /// share of the edge's global budget, **not** the config constant.
    /// Rewritten by the driver on every connect and disconnect
    /// ([`Conn::set_ceiling`]); `relax` aims at whatever it says now.
    ceiling: AtomicU32,
    /// This connection is counted in `Shared::live` — set once, when its
    /// handshake joins it to the budget, cleared once, when it leaves. The
    /// flag is what makes `join`/`leave` idempotent: a connection can be
    /// dropped by its own reader, by a failed unsolicited push, by
    /// `on_instance_restart` and by `stop`, and only the first of those may
    /// move the counter.
    counted: AtomicBool,
    /// Requests handed to the `Engine` and not yet completed.
    inflight: AtomicU32,
    /// Highest SUBMIT `seq` this edge has answered.
    acked_seq: AtomicU64,
    /// `corr` → `(seq, is_query)` for every request in flight.
    corr_to_seq: Mutex<HashMap<u32, (u64, bool)>>,
    /// Credits have been halved by `Backpressure` at least once and have not
    /// yet climbed back to the configured ceiling.
    squeezed: AtomicBool,
    /// Nanoseconds since the edge's `t0` at the last successful write —
    /// drives the standalone `STATUS` timer.
    last_write_ns: AtomicU64,
    /// The connection is finished: the socket has been shut down and the
    /// reader must stop.
    closed: AtomicBool,
    /// The connection counts toward the budget and the edge may write
    /// unsolicited frames on it. Flipped inside `Shared::join_and_grant`
    /// (under `grant_lock`) just BEFORE the handshake writes `HELLO_OK` — it
    /// must be set under that lock so a concurrent grant recompute counts this
    /// connection, which is what keeps the sum of granted credits within the
    /// budget at every instant.
    ///
    /// A client's dial requires the first frame it reads to be
    /// `HELLO_OK`/`HELLO_REFUSED`/`REDIRECT`, so nothing the edge sends on its
    /// own initiative — the `STATUS` timer above all — may reach the socket
    /// before `HELLO_OK`. Since `ready` is set *before* that write, the
    /// ordering is NOT enforced by this flag: it is enforced by the handshake
    /// holding this connection's `writer` across `join_and_grant` and the
    /// `HELLO_OK` write, so every unsolicited-frame path blocks behind it.
    ready: AtomicBool,
    /// Readers parked on the credit gate. Lets the driver skip the gate lock
    /// entirely on the (overwhelmingly common) uncontended path.
    gate_waiters: AtomicU32,
    gate: (Mutex<()>, Condvar),
    /// This connection has already been told, for at least one SUBMIT, that
    /// this node cannot serve writes. See [`Conn::latch_not_serving`].
    not_serving: AtomicBool,
    /// An unexpected frame type has already been logged once for this
    /// connection; the rest are counted, not printed.
    logged_unexpected: AtomicBool,
}

/// What [`Conn::set_ceiling`] did — the driver needs to know whether a client
/// is owed a frame about it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum CeilingChange {
    /// The share is unchanged; nothing to say.
    Same,
    /// The share grew. The client learns it from the next `RESPONSE` or the
    /// idle `STATUS` timer — a wider window costs nothing to learn late.
    Raised,
    /// The share shrank. The client MUST be told before it sends into the
    /// window the edge no longer has.
    Lowered,
}

impl Conn {
    pub fn new(idx: u32, writer: FramedConn, credits: u32, now_ns: u64) -> Self {
        Conn {
            idx,
            writer: Mutex::new(writer),
            client_id: AtomicU64::new(0),
            credits: AtomicU32::new(credits),
            ceiling: AtomicU32::new(credits),
            counted: AtomicBool::new(false),
            inflight: AtomicU32::new(0),
            acked_seq: AtomicU64::new(0),
            corr_to_seq: Mutex::new(HashMap::new()),
            squeezed: AtomicBool::new(false),
            last_write_ns: AtomicU64::new(now_ns),
            closed: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            gate_waiters: AtomicU32::new(0),
            gate: (Mutex::new(()), Condvar::new()),
            not_serving: AtomicBool::new(false),
            logged_unexpected: AtomicBool::new(false),
        }
    }

    pub fn set_client_id(&self, id: u64) {
        self.client_id.store(id, Ordering::Relaxed);
    }

    pub fn client_id(&self) -> u64 {
        self.client_id.load(Ordering::Relaxed)
    }

    /// `SeqCst`, not `Acquire`: this load is one arm of the credit gate's
    /// Dekker pairing (see [`Conn::wait_for_credit`]). On x86-64 a `SeqCst`
    /// load is a plain `mov`, so the pairing costs nothing on the hot path.
    pub fn credits(&self) -> u32 {
        self.credits.load(Ordering::SeqCst)
    }

    pub fn acked_seq(&self) -> u64 {
        self.acked_seq.load(Ordering::Acquire)
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Call once, inside `Shared::join_and_grant` under `grant_lock`, just
    /// before `HELLO_OK` is written — the handshake holds this connection's
    /// `writer` across both, which preserves HELLO_OK-first. See [`Conn::ready`].
    pub fn set_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    /// Whether the edge may write unsolicited frames on this connection.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn last_write_ns(&self) -> u64 {
        self.last_write_ns.load(Ordering::Relaxed)
    }

    /// A frame header for this connection: our protocol version, the peer's
    /// asserted `client_id`, and the `seq` being answered.
    pub fn hdr(&self, ty: FrameType, flags: u8, seq: u64) -> Header {
        Header { ty, flags, version: PROTOCOL_VERSION, client_id: self.client_id(), seq }
    }

    /// Write one frame. Returns `false` if the connection is now dead — the
    /// socket has been shut down (which wakes the reader out of `read_frame`)
    /// and the connection marked closed.
    ///
    /// A failure here may have left a partial frame on the wire, so there is no
    /// "retry the write" path: the peer's parser is desynchronised and the only
    /// correct move is to drop the connection and let the client reconnect.
    pub fn write(&self, h: Header, payload: &[u8], now_ns: u64) -> bool {
        let mut w = self.writer.lock();
        match w.write_frame(h, payload) {
            Ok(()) => {
                self.last_write_ns.store(now_ns, Ordering::Relaxed);
                true
            }
            Err(_) => {
                w.shutdown();
                drop(w);
                self.close();
                false
            }
        }
    }

    /// Write a buffer of one-or-more **already-encoded** frames in a single
    /// `write_all` (one syscall for the whole batch), holding the writer lock
    /// once. Used by the driver to flush all of a connection's answers for one
    /// completion drain at once instead of a syscall per completion. Same
    /// failure contract as [`Conn::write`]: a partial write desynchronises the
    /// peer's parser, so it takes the connection down.
    ///
    /// An empty buffer is a no-op that neither writes nor touches
    /// `last_write_ns` (nothing went on the wire).
    pub fn write_batch(&self, buf: &[u8], now_ns: u64) -> bool {
        if buf.is_empty() {
            return !self.is_closed();
        }
        let mut w = self.writer.lock();
        match w.write_all_bytes(buf) {
            Ok(()) => {
                self.last_write_ns.store(now_ns, Ordering::Relaxed);
                true
            }
            Err(_) => {
                w.shutdown();
                drop(w);
                self.close();
                false
            }
        }
    }

    /// Acquire this connection's writer lock for a caller that needs to hold
    /// it across OTHER work before writing exactly one frame through it —
    /// the handshake path, which must hold the lock from before
    /// `Shared::join_and_grant` (which can flip [`Conn::set_ready`], making
    /// this connection a legal target for an unsolicited `STATUS`/
    /// `LEADER_CHANGED` write from another thread) through its own
    /// `HELLO_OK` write, so that write can never lose the race for the
    /// socket to one of those. Pair with [`Conn::write_locked`].
    pub fn lock_writer(&self) -> MutexGuard<'_, FramedConn> {
        self.writer.lock()
    }

    /// Write one frame through an already-held [`Conn::lock_writer`] guard.
    /// Same contract as [`Conn::write`], with one difference forced by
    /// holding the lock across the call: on failure it inlines what
    /// [`Conn::close`] would do instead of calling it, because `close` takes
    /// `writer` itself and the caller here already holds it — taking it
    /// again would deadlock.
    pub fn write_locked(&self, w: &mut FramedConn, h: Header, payload: &[u8], now_ns: u64) -> bool {
        match w.write_frame(h, payload) {
            Ok(()) => {
                self.last_write_ns.store(now_ns, Ordering::Relaxed);
                true
            }
            Err(_) => {
                w.shutdown();
                let _ = self.closed.swap(true, Ordering::AcqRel);
                self.notify_gate();
                false
            }
        }
    }

    /// Mark closed and shut the socket down in both directions, waking the
    /// reader thread and any parked credit-gate waiter.
    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.writer.lock().shutdown();
        }
        self.notify_gate();
    }

    // ---------------------------------------------------------- credits

    /// Block until the peer is inside its credit window, so that a client which
    /// ignores credits is stopped by us ceasing to read its socket (the TCP
    /// backstop, spec §4.2) rather than by frames being accepted and bounced.
    ///
    /// Returns `false` when the edge is stopping or the connection died.
    ///
    /// ## Why the wakeup cannot be lost
    ///
    /// The driver deliberately does **not** take the gate lock on every
    /// completion — that would put a lock acquisition on the hot path for a
    /// gate that is empty in steady state. Instead the two sides run Dekker's
    /// pattern, which is only correct under `SeqCst`:
    ///
    /// - here: publish `gate_waiters += 1`, *then* re-read the window;
    /// - driver ([`Conn::claim`] → [`Conn::notify_gate`]): publish the
    ///   released slot (`inflight -= 1`), *then* read `gate_waiters`.
    ///
    /// Under `SeqCst` at least one of the two reads must observe the other's
    /// write, so either this thread sees room and never parks, or the driver
    /// sees the waiter and notifies. The `GATE_TICK` timed wait is a backstop
    /// for the stop flag, not for the wakeup.
    pub fn wait_for_credit(&self, stop: &AtomicBool) -> bool {
        loop {
            if stop.load(Ordering::Relaxed) || self.is_closed() {
                return false;
            }
            if self.inflight.load(Ordering::SeqCst) < self.credits() {
                return true;
            }
            let mut g = self.gate.0.lock();
            self.gate_waiters.fetch_add(1, Ordering::SeqCst);
            let admit = self.inflight.load(Ordering::SeqCst) < self.credits();
            if !admit {
                self.gate.1.wait_for(&mut g, GATE_TICK);
            }
            self.gate_waiters.fetch_sub(1, Ordering::SeqCst);
            drop(g);
            if admit {
                return true;
            }
        }
    }

    /// Wake any reader parked on the credit gate. Free when nobody is parked,
    /// which is the steady state — see [`Conn::wait_for_credit`] for why
    /// skipping the lock here is still wakeup-safe.
    pub fn notify_gate(&self) {
        if self.gate_waiters.load(Ordering::SeqCst) > 0 {
            let _g = self.gate.0.lock();
            self.gate.1.notify_all();
        }
    }

    /// Reserve one slot in the inflight window, before the `Engine` is even
    /// asked.
    ///
    /// Order matters: `SendHalf::try_submit` can report `Ok(())` for a request
    /// whose completion has *already* been delivered to the driver (see
    /// `finish_write`'s doc in `uc_client::engine`), so the driver's
    /// decrement can race ahead of an increment done after the call — and an
    /// `AtomicU32` that wraps below zero would wedge the credit gate forever.
    /// Reserving first makes the decrement always the second half of a pair.
    pub fn reserve(&self, corr: u32, seq: u64, is_query: bool) {
        self.inflight.fetch_add(1, Ordering::SeqCst);
        self.corr_to_seq.lock().insert(corr, (seq, is_query));
    }

    /// Undo a [`Conn::reserve`] whose request the `Engine` refused. Returns
    /// `false` if the entry was already taken by the driver, in which case the
    /// driver owns the answer and the caller must not write one.
    pub fn unreserve(&self, corr: u32) -> bool {
        if self.corr_to_seq.lock().remove(&corr).is_some() {
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    /// Claim a completion: take the `corr` entry, release its inflight slot,
    /// and advance `acked_seq` for a SUBMIT. `None` means some other path
    /// already resolved it (a refused submit, or a duplicate).
    pub fn claim(&self, corr: u32) -> Option<(u64, bool)> {
        let (seq, is_query) = self.corr_to_seq.lock().remove(&corr)?;
        self.inflight.fetch_sub(1, Ordering::SeqCst);
        if !is_query {
            self.acked_seq.fetch_max(seq, Ordering::AcqRel);
        }
        Some((seq, is_query))
    }

    /// Halve the credit grant (floor 1) after the `Engine` reported
    /// `Backpressure`. Pressure is signalled *before* frames leave the client,
    /// which is the whole point of a receiver-driven window.
    ///
    /// A CAS loop, not load-then-store: this runs on a reader thread while
    /// [`Conn::relax`] runs on the driver, so a plain read-modify-write pair
    /// can lose one side's update entirely — a squeeze silently reverted (the
    /// window stays wide while the engine is full) or a relax silently
    /// reverted (the connection stays pinned at 1 credit). Uncontended, this
    /// is one `lock cmpxchg`.
    pub fn squeeze(&self) {
        let _ = self.credits.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |c| {
            Some((c / 2).max(1))
        });
        self.squeezed.store(true, Ordering::Release);
    }

    /// Double the grant back towards the connection's current ceiling after a
    /// completion, but only if it was ever squeezed. Returns `true` if credits
    /// increased, which is what obliges the caller to tell the client promptly.
    ///
    /// The ceiling is the connection's live share of the edge's budget
    /// ([`Conn::set_ceiling`]), not the config constant: a connection that
    /// relaxes while five others are connected must not climb past the share
    /// those five leave it.
    ///
    /// Same CAS discipline as [`Conn::squeeze`], and for the same reason —
    /// these two are the pair that races.
    pub fn relax(&self) -> bool {
        if !self.squeezed.load(Ordering::Acquire) {
            return false;
        }
        let ceiling = self.ceiling();
        let bumped = self.credits.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |c| {
            let next = c.saturating_mul(2).min(ceiling);
            if next > c { Some(next) } else { None }
        });
        match bumped {
            Ok(prev) => {
                if prev.saturating_mul(2) >= ceiling {
                    // Back at the ceiling: stop paying for the check.
                    self.squeezed.store(false, Ordering::Release);
                }
                self.notify_gate();
                true
            }
            // Already at (or above) the ceiling — nothing to announce.
            Err(_) => {
                self.squeezed.store(false, Ordering::Release);
                false
            }
        }
    }

    // ---------------------------------------------------------- the budget

    pub fn ceiling(&self) -> u32 {
        self.ceiling.load(Ordering::Acquire)
    }

    /// Set this connection's share of the edge's budget.
    ///
    /// A **reduction** clamps the live grant down immediately — the whole
    /// point is that the edge stops admitting into a window it cannot honour
    /// at the same moment the client is told about it, not one round trip
    /// later. An **increase** is applied to the live grant only when the
    /// connection is not squeezed; a squeezed connection climbs back through
    /// [`Conn::relax`], which now aims at this ceiling on its own, so a
    /// backpressure episode is not erased by an unrelated disconnect.
    pub fn set_ceiling(&self, ceiling: u32) -> CeilingChange {
        let ceiling = ceiling.max(1);
        let prev = self.ceiling.swap(ceiling, Ordering::AcqRel);
        if ceiling == prev {
            return CeilingChange::Same;
        }
        if ceiling < prev {
            let _ = self.credits.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |c| {
                if c > ceiling { Some(ceiling) } else { None }
            });
            CeilingChange::Lowered
        } else {
            if !self.squeezed.load(Ordering::Acquire) {
                let _ = self.credits.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |c| {
                    if c < ceiling { Some(ceiling) } else { None }
                });
                self.notify_gate();
            }
            CeilingChange::Raised
        }
    }

    /// `true` exactly once: the first call marks this connection as counted in
    /// the edge's `live` tally.
    pub fn mark_counted(&self) -> bool {
        !self.counted.swap(true, Ordering::AcqRel)
    }

    /// `true` exactly once: the first call after [`Conn::mark_counted`] takes
    /// it back out.
    pub fn clear_counted(&self) -> bool {
        self.counted.swap(false, Ordering::AcqRel)
    }

    // ------------------------------------------------- the prefix invariant

    /// Latch this connection as "cannot serve writes", the first time a SUBMIT
    /// on it is answered `REDIRECT` or `RETRY{not_serving}`.
    ///
    /// ## The invariant this exists to hold
    ///
    /// **The set of SUBMITs a connection gets accepted is always a PREFIX of
    /// what it sent.** Without the latch it is not, and the consequence is a
    /// wrong answer, not just churn:
    ///
    /// A client flushes its pipelined window at this edge mid-election. The
    /// first K frames are refused (`can_serve()` is false) and answered
    /// `REDIRECT`. Then this node WINS the election, and frames K+1..N — the
    /// same socket, the same flush — are accepted and applied. The session
    /// table's `highest_seq` for that client jumps to N. The client, which
    /// acted on the first `REDIRECT` and reconnected, re-sends 1..K; every one
    /// of them is now `seq <= highest_seq` with no cached response, which
    /// `Sessioned` classifies as **EXPIRED** — "outcome unknowable" — for
    /// requests that provably never committed. It also masks real dedup bugs:
    /// with a 4096-entry window against ~200 in flight, an expiry should be
    /// structurally impossible.
    ///
    /// So a connection that has been told "not here" once is told it for
    /// every later SUBMIT, whatever the node's role does next. It costs the
    /// client one reconnect it was already committed to making, and it is what
    /// keeps `EXPIRED` meaning what it says.
    ///
    /// Cleared only by a new connection — there is no unlatch, deliberately:
    /// the client's next window belongs to a fresh session on a fresh socket.
    ///
    /// Not covered (and not a mode change, so not latched): a one-off
    /// `RETRY{service_unavailable}`, which a request earns after burning its
    /// whole `request_timeout` against a full engine. A re-send of that seq can
    /// still land behind a later one and read as `EXPIRED`.
    pub fn latch_not_serving(&self) {
        self.not_serving.store(true, Ordering::Release);
    }

    /// Whether [`Conn::latch_not_serving`] has fired on this connection.
    pub fn is_not_serving(&self) -> bool {
        self.not_serving.load(Ordering::Acquire)
    }

    /// `true` the first time an unexpected frame type is seen, so the log line
    /// is written once per connection rather than once per frame.
    pub fn first_unexpected(&self) -> bool {
        !self.logged_unexpected.swap(true, Ordering::Relaxed)
    }
}

/// Every live connection, keyed by its monotonic index.
#[derive(Default)]
pub(crate) struct ConnTable {
    slots: RwLock<HashMap<u32, Arc<Conn>>>,
    next_idx: AtomicU32,
}

impl ConnTable {
    /// Allocate the next connection index. Never reused — see the module doc.
    pub fn alloc_idx(&self) -> u32 {
        self.next_idx.fetch_add(1, Ordering::Relaxed)
    }

    pub fn insert(&self, conn: Arc<Conn>) {
        self.slots.write().insert(conn.idx, conn);
    }

    /// How many connections are live right now.
    ///
    /// Only the single acceptor thread ever *adds* one, so an acceptor that
    /// tests this and then inserts holds a hard ceiling without a lock spanning
    /// both steps: concurrent `remove`s can only make the count smaller, never
    /// larger, so the check can never be raced upward past the limit.
    pub fn len(&self) -> usize {
        self.slots.read().len()
    }

    pub fn get(&self, idx: u32) -> Option<Arc<Conn>> {
        self.slots.read().get(&idx).cloned()
    }

    /// Drop a connection from the table and close it. Completions that arrive
    /// afterwards find no connection and are discarded.
    pub fn remove(&self, idx: u32) {
        let conn = self.slots.write().remove(&idx);
        if let Some(c) = conn {
            c.close();
        }
    }

    /// Run `f` over a snapshot of the live connections.
    ///
    /// The snapshot is taken under a *read* lock which is then released, so `f`
    /// can write to sockets without holding the table lock — a socket write can
    /// block for the write timeout, and blocking every acceptor and completion
    /// behind one slow client would be exactly the wrong coupling.
    pub fn for_each(&self, mut f: impl FnMut(&Arc<Conn>)) {
        let snapshot: Vec<Arc<Conn>> = self.slots.read().values().cloned().collect();
        for c in &snapshot {
            f(c);
        }
    }

    /// Remove every connection from the table and hand them back **still
    /// open**.
    ///
    /// Deliberately does not close them: the instance-restart path owes each
    /// client a `LEADER_CHANGED` frame *before* its socket goes away, so
    /// closing is the caller's second step, not this one's side effect.
    pub fn take_all(&self) -> Vec<Arc<Conn>> {
        self.slots.write().drain().map(|(_, c)| c).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};

    fn a_conn(idx: u32, credits: u32) -> Arc<Conn> {
        // A real socket pair: `Conn` owns a `FramedConn`, and the tests below
        // only exercise the accounting, never the wire.
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let s = TcpStream::connect(l.local_addr().unwrap()).unwrap();
        let _accepted = l.accept().unwrap();
        Arc::new(Conn::new(idx, FramedConn::new(s).unwrap(), credits, 0))
    }

    #[test]
    fn indices_are_never_reused() {
        let t = ConnTable::default();
        let a = t.alloc_idx();
        let c = a_conn(a, 4);
        t.insert(Arc::clone(&c));
        t.remove(a);
        assert!(t.get(a).is_none(), "a removed connection is gone");
        assert_ne!(t.alloc_idx(), a, "the next connection must not inherit the index");
    }

    #[test]
    fn reserve_claim_and_unreserve_keep_inflight_balanced() {
        let c = a_conn(0, 4);
        c.reserve(7, 42, false);
        assert_eq!(c.inflight.load(Ordering::SeqCst), 1);
        assert_eq!(c.claim(7), Some((42, false)));
        assert_eq!(c.inflight.load(Ordering::SeqCst), 0);
        assert_eq!(c.acked_seq(), 42, "a submit advances acked_seq");
        assert_eq!(c.claim(7), None, "claiming twice yields nothing");

        c.reserve(8, 43, true);
        assert!(c.unreserve(8));
        assert_eq!(c.inflight.load(Ordering::SeqCst), 0);
        assert!(!c.unreserve(8), "unreserving twice is a no-op");
        assert_eq!(c.acked_seq(), 42, "a query never advances acked_seq");
    }

    #[test]
    fn the_not_serving_latch_is_sticky_and_per_connection() {
        let c = a_conn(0, 4);
        assert!(!c.is_not_serving(), "a fresh connection starts servable");
        c.latch_not_serving();
        assert!(c.is_not_serving());
        c.latch_not_serving();
        assert!(c.is_not_serving(), "there is no unlatch");
        // Per connection, not per edge: a second client is unaffected.
        assert!(!a_conn(1, 4).is_not_serving());
    }

    #[test]
    fn a_fresh_connection_is_not_ready_for_unsolicited_frames() {
        let c = a_conn(0, 4);
        assert!(!c.is_ready(), "nothing may be written before HELLO_OK");
        c.set_ready();
        assert!(c.is_ready());
    }

    #[test]
    fn credits_halve_under_pressure_and_climb_back_to_the_ceiling() {
        let c = a_conn(0, 8);
        assert!(!c.relax(), "never squeezed: nothing to relax");
        c.squeeze();
        c.squeeze();
        assert_eq!(c.credits(), 2);
        assert!(c.relax());
        assert_eq!(c.credits(), 4);
        assert!(c.relax());
        assert_eq!(c.credits(), 8);
        assert!(!c.relax(), "at the ceiling the squeeze flag clears");
    }

    #[test]
    fn a_lowered_ceiling_clamps_the_live_grant_at_once() {
        let c = a_conn(0, 32);
        assert_eq!(c.set_ceiling(32), CeilingChange::Same);
        assert_eq!(c.set_ceiling(28), CeilingChange::Lowered);
        assert_eq!(c.credits(), 28, "the edge stops admitting the moment the share shrinks");
        assert_eq!(c.set_ceiling(32), CeilingChange::Raised);
        assert_eq!(c.credits(), 32, "an unsqueezed connection takes its share back at once");
    }

    #[test]
    fn a_raised_ceiling_does_not_erase_a_squeeze() {
        let c = a_conn(0, 8);
        c.squeeze();
        assert_eq!(c.credits(), 4);
        assert_eq!(c.set_ceiling(16), CeilingChange::Raised);
        assert_eq!(c.credits(), 4, "a squeezed connection climbs back through relax, not here");
        assert!(c.relax());
        assert_eq!(c.credits(), 8, "…and relax now aims at the NEW ceiling");
    }

    #[test]
    fn a_connection_is_counted_into_the_budget_exactly_once() {
        let c = a_conn(0, 4);
        assert!(c.mark_counted());
        assert!(!c.mark_counted(), "joining twice must not double-count");
        assert!(c.clear_counted());
        assert!(!c.clear_counted(), "leaving twice must not double-discount");
    }

    #[test]
    fn credits_never_fall_below_one() {
        let c = a_conn(0, 2);
        for _ in 0..8 {
            c.squeeze();
        }
        assert_eq!(c.credits(), 1, "a zero grant would wedge the connection forever");
    }

    #[test]
    fn the_credit_gate_opens_when_the_driver_releases_a_slot() {
        let c = a_conn(0, 1);
        let stop = AtomicBool::new(false);
        c.reserve(0, 1, false);
        let c2 = Arc::clone(&c);
        let h = std::thread::spawn(move || {
            let stop = AtomicBool::new(false);
            c2.wait_for_credit(&stop)
        });
        std::thread::sleep(Duration::from_millis(20));
        c.claim(0);
        c.notify_gate();
        assert!(h.join().unwrap(), "the gate opened once the slot came back");
        assert!(c.wait_for_credit(&stop), "an empty window admits immediately");
    }

    #[test]
    fn the_credit_gate_gives_up_when_the_edge_stops() {
        let c = a_conn(0, 1);
        c.reserve(0, 1, false);
        let stop = Arc::new(AtomicBool::new(false));
        let (c2, s2) = (Arc::clone(&c), Arc::clone(&stop));
        let h = std::thread::spawn(move || c2.wait_for_credit(&s2));
        std::thread::sleep(Duration::from_millis(20));
        stop.store(true, Ordering::Relaxed);
        c.notify_gate();
        assert!(!h.join().unwrap(), "stopping must release a parked reader");
    }
}
