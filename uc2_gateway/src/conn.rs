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

use parking_lot::{Condvar, Mutex, RwLock};

use uc2_remote::conn::FramedConn;
use uc2_remote::frame::{FrameType, Header, PROTOCOL_VERSION};

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
    /// Credits currently granted to this connection (the peer may have at most
    /// this many unanswered `seq`s beyond `acked_seq`).
    credits: AtomicU32,
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
    /// The handshake completed and `HELLO_OK` is on the wire.
    ///
    /// Nothing the edge sends *on its own initiative* — the `STATUS` timer
    /// above all — may go out before this is set. A client's dial requires the
    /// first frame it reads to be `HELLO_OK`/`HELLO_REFUSED`/`REDIRECT`, so a
    /// `STATUS` that beat the handshake (a slow WAN link plus a status
    /// interval well under the handshake budget) would fail the dial outright.
    ready: AtomicBool,
    /// Readers parked on the credit gate. Lets the driver skip the gate lock
    /// entirely on the (overwhelmingly common) uncontended path.
    gate_waiters: AtomicU32,
    gate: (Mutex<()>, Condvar),
    /// An unexpected frame type has already been logged once for this
    /// connection; the rest are counted, not printed.
    logged_unexpected: AtomicBool,
}

impl Conn {
    pub fn new(idx: u32, writer: FramedConn, credits: u32, now_ns: u64) -> Self {
        Conn {
            idx,
            writer: Mutex::new(writer),
            client_id: AtomicU64::new(0),
            credits: AtomicU32::new(credits),
            inflight: AtomicU32::new(0),
            acked_seq: AtomicU64::new(0),
            corr_to_seq: Mutex::new(HashMap::new()),
            squeezed: AtomicBool::new(false),
            last_write_ns: AtomicU64::new(now_ns),
            closed: AtomicBool::new(false),
            ready: AtomicBool::new(false),
            gate_waiters: AtomicU32::new(0),
            gate: (Mutex::new(()), Condvar::new()),
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

    /// Call once, immediately after `HELLO_OK` is written. See [`Conn::ready`].
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
    /// `finish_write`'s doc in `uc2_client::engine`), so the driver's
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
    pub fn squeeze(&self) {
        let c = self.credits();
        self.credits.store((c / 2).max(1), Ordering::SeqCst);
        self.squeezed.store(true, Ordering::Release);
    }

    /// Double the grant back towards `ceiling` after a successful completion,
    /// but only if it was ever squeezed. Returns `true` if credits increased,
    /// which is what obliges the caller to tell the client promptly.
    pub fn relax(&self, ceiling: u32) -> bool {
        if !self.squeezed.load(Ordering::Acquire) {
            return false;
        }
        let c = self.credits();
        let next = c.saturating_mul(2).min(ceiling);
        if next <= c {
            // Back at the ceiling: stop paying for the check.
            self.squeezed.store(false, Ordering::Release);
            return false;
        }
        self.credits.store(next, Ordering::SeqCst);
        if next >= ceiling {
            self.squeezed.store(false, Ordering::Release);
        }
        self.notify_gate();
        true
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
    fn a_fresh_connection_is_not_ready_for_unsolicited_frames() {
        let c = a_conn(0, 4);
        assert!(!c.is_ready(), "nothing may be written before HELLO_OK");
        c.set_ready();
        assert!(c.is_ready());
    }

    #[test]
    fn credits_halve_under_pressure_and_climb_back_to_the_ceiling() {
        let c = a_conn(0, 8);
        assert!(!c.relax(8), "never squeezed: nothing to relax");
        c.squeeze();
        c.squeeze();
        assert_eq!(c.credits(), 2);
        assert!(c.relax(8));
        assert_eq!(c.credits(), 4);
        assert!(c.relax(8));
        assert_eq!(c.credits(), 8);
        assert!(!c.relax(8), "at the ceiling the squeeze flag clears");
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
