// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! One connection's shared state and its two threads.
//!
//! # Shape (design spec 2026-08-24 §3.2)
//!
//! - **Submitter** (the caller's thread, in `engine.rs`): checks the window
//!   from atomics, encodes into [`OutRing`], records the slot. No syscall, no
//!   lock.
//! - **Writer thread**: drains the ring with ONE `write_all_bytes` per drain
//!   (flush-on-empty, no timer), owns the socket for dial/redial, re-sends the
//!   live window after a redial, sends `PING`, and drains the tiny control
//!   buffer (a `PONG` the reader queued).
//! - **Reader thread**: `read_frame_buffered` + `next_buffered`, updates
//!   `credits`/`acked_seq`, resolves slots, pushes completions, and wakes the
//!   poller ONCE per read batch.
//!
//! The only lock either thread takes per frame is none. `reconnect` (the
//! redial request + read-half handoff), `control` (PONG bytes) and
//! `retransmit` (seqs a RETRY asked for again) are cold-path mutexes.
//!
//! # Thread roles, and how they are enforced
//!
//! [`OutRing`] and [`CompletionQueue`] are SPSC: their docs assign each method
//! to a role, and calling one from the wrong thread is a data race, not a
//! style problem. The primitives cannot express that in the type system (a
//! `&OutRing` is a `&OutRing`), so **`Link` makes the roles structural by
//! ownership** instead:
//!
//! - `Link::out` and `Link::completions` are **private fields of this
//!   module** — no other module can reach either one, whatever it holds.
//! - The `OutRing` **consumer** role (`peek_upto`, `consume`, `set_send_pos`,
//!   `copy_range`) is reachable only through [`Writer`], which is constructed
//!   once in [`Link::start`] and **moved into the writer thread**. It is
//!   neither `Clone` nor `Sync`, so no second thread can obtain one. The
//!   writer thread is therefore the only caller of those methods — and the
//!   only holder of the socket's write half.
//! - The `OutRing` **producer** role (`stage_frame`, `commit`, `release_to`)
//!   is reachable only through [`Link::out_producer`], whose sole caller is
//!   `RemoteSendHalf` — `Send` but not `Sync`, so exactly one submitter
//!   thread.
//! - The `SlotTable` ([`Link::slots`]) is the one structure that is genuinely
//!   **multi-role by design**: the submitter claims, the reader resolves, the
//!   writer marks sent, the sweep expires. It needs no ownership token
//!   because it is not SPSC — every transition goes through the single-CAS
//!   protocol `slots.rs` documents, which is correct from any thread. Its
//!   per-method role comments (`SUBMITTER ONLY` on `claim`/`is_free`) are
//!   about the *seq allocation* being single-threaded, not about the memory.
//! - The `CompletionQueue` **producer** role is reachable only through
//!   `Link::complete`/`Link::sweep_deadlines`, which are private to this
//!   module and called from exactly two places: [`Reader`] (moved into the
//!   reader thread, same non-`Clone`/non-`Sync` shape as `Writer`), and
//!   [`Link::close`] — which **joins both threads first**, so the closing
//!   thread is the only one alive when it pushes. This is why the writer
//!   thread does *not* sweep deadlines during a redial the way the old
//!   `RemoteClient`'s dial did: the reader keeps sweeping on its own tick
//!   while it waits for the fresh read half, which preserves the
//!   `request_timeout` invariant with one producer instead of two.
//! - The `CompletionQueue` **consumer** role is reachable only through
//!   [`drain_completions`], whose sole caller is `RemotePollHalf::poll`
//!   (`&mut self`, single owner).

use std::cell::Cell;
use std::marker::PhantomData;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::completion::{CompletionQueue, OutcomeTag, Record};
use crate::conn::FramedConn;
use crate::engine::{RemoteCompletion, RemoteConfig, RemoteStats, outcome_of};
use crate::error::RemoteError;
use crate::frame::{
    FrameType, HEADER_LEN, HELLO_REFUSED_BUSY, HELLO_REFUSED_FAULTED, Header, Hello, HelloOk,
    HelloRefused, Leader, PROTOCOL_VERSION, decode_header, encode_frame,
};
use crate::outgoing::OutRing;
use crate::slots::SlotTable;

/// The reader's tick: how often it sweeps `request_timeout`, notices
/// `shutdown` and re-checks liveness. Also the socket read timeout.
pub(crate) const SWEEP_INTERVAL: Duration = Duration::from_millis(25);
/// Socket write timeout — the writer thread owns the socket alone, so this
/// only bounds a wedged peer, it never freezes a submitter.
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
/// The writer's park bound when the ring is empty: short enough that a
/// `not_before` backoff and the `PING` clock stay accurate.
const WRITER_PARK: Duration = Duration::from_millis(5);
/// A `RETRY` hint is honoured, but never for longer than this.
const MAX_RETRY_SLEEP: Duration = Duration::from_secs(1);
/// A `RETRY{retry_after_us: 0}` still backs off this much.
const MIN_RETRY_SLEEP: Duration = Duration::from_micros(100);
/// Backoff for a request an edge redirected to itself.
const SELF_REDIRECT_BACKOFF: Duration = Duration::from_millis(10);
/// The most bytes one re-send batch puts into a single `write_all_bytes`.
const RESEND_BATCH_BYTES: usize = 64 * 1024;
const RECONNECT_BACKOFF_START: Duration = Duration::from_millis(5);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_millis(500);
/// Hops followed during one connect scan.
const MAX_REDIRECT_HOPS: usize = 8;
/// Completions handed out per `poll` call — a bounded duty cycle.
const POLL_BATCH: usize = 256;

/// **The connection generation and its credit grant, in ONE word.**
///
/// The grant is per-connection state, so applying one is really "set the
/// credits *of this connection*" — two facts that have to move together. Held
/// apart (read the generation, then store the credits) they are a TOCTOU a
/// few instructions wide: the writer thread can install a fresh connection in
/// between, and a grant read off the connection that was just replaced then
/// overwrites the new one's `HELLO_OK` window. It is an ABSOLUTE count, so
/// that is not a stale-by-a-little value — it is the old edge's window
/// asserted over an edge that never granted it, which
/// [`crate::engine::admissible`] turns into over-admission.
///
/// Packed as `generation_low_32 << 32 | credits`, updated by a CAS that fails
/// if the generation half moved. Only the LOW 32 bits of the generation
/// identify a connection: a grant would have to survive exactly 2^32 redials
/// to alias, which is not a window any buffered frame lives in.
struct FlowWord(AtomicU64);

impl FlowWord {
    fn new(generation: u64, credits: u32) -> FlowWord {
        FlowWord(AtomicU64::new(Self::pack(generation, credits)))
    }

    const fn pack(generation: u64, credits: u32) -> u64 {
        ((generation as u32 as u64) << 32) | credits as u64
    }

    fn credits(&self) -> u32 {
        self.0.load(Ordering::Acquire) as u32
    }

    /// WRITER THREAD ONLY, inside the critical section that installs a
    /// connection: the new generation and the grant it arrived with, together.
    /// Unconditional — installing a connection supersedes whatever the
    /// previous one had to say.
    fn install(&self, generation: u64, credits: u32) {
        self.0
            .store(Self::pack(generation, credits), Ordering::Release);
    }

    /// Apply a grant read off connection `generation`. `false` = that
    /// connection has been replaced and the grant was dropped.
    fn try_update(&self, generation: u64, credits: u32) -> bool {
        let want = Self::pack(generation, credits);
        let mut cur = self.0.load(Ordering::Acquire);
        loop {
            if (cur >> 32) != (want >> 32) {
                return false;
            }
            match self
                .0
                .compare_exchange_weak(cur, want, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(now) => cur = now,
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct StatCells {
    pub(crate) redirects: AtomicU64,
    pub(crate) leader_changes: AtomicU64,
    pub(crate) reconnects: AtomicU64,
    pub(crate) resends: AtomicU64,
    pub(crate) retries: AtomicU64,
    pub(crate) unknown: AtomicU64,
    pub(crate) expired: AtomicU64,
    pub(crate) max_credits_seen: AtomicU32,
    pub(crate) refused_members: AtomicU64,
    pub(crate) stale_redials: AtomicU64,
    pub(crate) socket_writes: AtomicU64,
    pub(crate) frames_written: AtomicU64,
}

impl StatCells {
    pub(crate) fn snapshot(&self) -> RemoteStats {
        RemoteStats {
            redirects: self.redirects.load(Ordering::Relaxed),
            leader_changes: self.leader_changes.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            resends: self.resends.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            unknown: self.unknown.load(Ordering::Relaxed),
            expired: self.expired.load(Ordering::Relaxed),
            max_credits_seen: self.max_credits_seen.load(Ordering::Relaxed),
            refused_members: self.refused_members.load(Ordering::Relaxed),
            stale_redials: self.stale_redials.load(Ordering::Relaxed),
            socket_writes: self.socket_writes.load(Ordering::Relaxed),
            frames_written: self.frames_written.load(Ordering::Relaxed),
        }
    }
}

/// The redial request + read-half handoff. The READER asks; the WRITER dials.
struct Reconnect {
    needed: bool,
    preferred: Option<String>,
    /// The read half of the connection the writer just dialled, tagged with
    /// its generation, waiting to be picked up by the reader.
    read_half: Option<(u64, FramedConn)>,
    /// **The connection generation, and its authority.** Bumped by exactly one
    /// writer (the writer thread), under this mutex, at the instant a new
    /// socket is installed. [`Link::generation`] mirrors it for lock-free
    /// readers; this field is what that mirror is derived from.
    epoch: u64,
}

pub(crate) struct Link {
    pub(crate) cfg: RemoteConfig,
    pub(crate) client_id: u64,
    /// The correlation table. Shared by design: the submitter claims, the
    /// reader resolves, both through the single-CAS protocol `slots.rs`
    /// documents.
    slots: SlotTable,
    /// SPSC. Producer role: the submitter, via [`Link::out_producer`].
    /// Consumer role: the writer thread, via [`Writer`]. Private so no other
    /// module can reach either.
    out: OutRing,
    /// SPSC. Producer role: the reader thread (and `close`, after it has
    /// joined the reader). Consumer role: `RemotePollHalf::poll`, via
    /// [`drain_completions`]. Private for the same reason.
    completions: CompletionQueue,
    /// The credit grant AND the generation of the connection that granted it,
    /// in one word — see [`FlowWord`].
    flow: FlowWord,
    /// The current connection's generation — the lock-free mirror of
    /// `Reconnect::epoch`, published with `Release` inside the same critical
    /// section that bumps it.
    ///
    /// It exists because both threads can notice the *same* dead connection
    /// independently, and the second one to notice would otherwise tear down
    /// the connection the first one already replaced (and, worse, overwrite
    /// its `HELLO_OK` grant with a stale absolute credit count still sitting
    /// in a `next_buffered` batch). Every complaint therefore names the
    /// connection it is about: see [`Link::request_redial`] and
    /// [`credit_update`].
    generation: AtomicU64,
    /// The highest `acked_seq` any frame advertised. Monotone.
    acked_seq: AtomicU64,
    /// The current connection has answered something only a serving edge can
    /// answer. Until then the writer sends ONE frame (probe-before-flush).
    proven: AtomicBool,
    /// The single seq written while unproven; `0` = none.
    probe_seq: AtomicU64,
    /// **The writer's re-send scan starts here**: the lowest seq whose frame
    /// bytes are still in the outgoing ring, published by the submitter's
    /// `reclaim` (`engine.rs`). Everything below it has been released, so its
    /// bytes may already have been overwritten and it can never be re-sent —
    /// which is sound because `reclaim` stops at the oldest LIVE slot, so no
    /// live request is ever below this floor. Monotone.
    pub(crate) oldest_unreclaimed: AtomicU64,
    stats: StatCells,
    t0: Instant,
    closed: AtomicBool,
    connected: AtomicBool,
    leader: Mutex<Option<(u32, String)>>,
    addr: Mutex<String>,
    member_idx: AtomicUsize,
    reconnect: Mutex<Reconnect>,
    reconnect_cv: Condvar,
    /// Frames the READER needs written (only `PONG`). Cold path.
    control: Mutex<Vec<u8>>,
    /// Seqs a `RETRY`/`UNKNOWN` asked to be written again. Cold path.
    retransmit: Mutex<Vec<u64>>,
    /// A handle on the live socket kept purely so `close` can shut it down and
    /// wake both threads out of their blocking calls.
    sock: Mutex<Option<FramedConn>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    rng: AtomicU64,
}

impl Link {
    pub(crate) fn start(cfg: RemoteConfig) -> Result<Arc<Link>, RemoteError> {
        cfg.validate()?;
        let client_id = cfg.client_id.unwrap_or_else(random_u64);
        let stats = StatCells::default();
        let (conn, info, idx, addr) = dial(&cfg, client_id, None, 0, &stats, None)?;
        let read_half = conn.try_clone()?;
        read_half.set_read_timeout(Some(SWEEP_INTERVAL))?;
        conn.set_read_timeout(Some(SWEEP_INTERVAL))?;
        conn.set_write_timeout(Some(WRITE_TIMEOUT))?;
        let watch = conn.try_clone()?;
        stats
            .max_credits_seen
            .store(info.credits, Ordering::Relaxed);

        let link = Arc::new(Link {
            slots: SlotTable::new(cfg.max_inflight),
            out: OutRing::new(cfg.out_ring_bytes_resolved()),
            completions: CompletionQueue::new(
                cfg.max_inflight as usize,
                cfg.arena_bytes_resolved(),
            ),
            flow: FlowWord::new(0, info.credits),
            generation: AtomicU64::new(0),
            acked_seq: AtomicU64::new(0),
            proven: AtomicBool::new(false),
            probe_seq: AtomicU64::new(0),
            oldest_unreclaimed: AtomicU64::new(1),
            stats,
            t0: Instant::now(),
            closed: AtomicBool::new(false),
            connected: AtomicBool::new(true),
            leader: Mutex::new(info.leader),
            addr: Mutex::new(addr),
            member_idx: AtomicUsize::new(idx),
            reconnect: Mutex::new(Reconnect {
                needed: false,
                preferred: None,
                read_half: None,
                epoch: 0,
            }),
            reconnect_cv: Condvar::new(),
            control: Mutex::new(Vec::new()),
            retransmit: Mutex::new(Vec::new()),
            sock: Mutex::new(Some(watch)),
            threads: Mutex::new(Vec::new()),
            rng: AtomicU64::new(client_id | 1),
            client_id,
            cfg,
        });

        // Each role token is built ONCE here and MOVED into its thread; see
        // the module header. Neither is `Clone` or `Sync`, so this is the only
        // way either role is ever held.
        let writer_role = Writer::new(Arc::clone(&link), conn);
        let writer = std::thread::Builder::new()
            .name("uc2-remote-tx".into())
            .spawn(move || writer_role.run())?;
        let reader_role = Reader::new(Arc::clone(&link), read_half);
        let reader = match std::thread::Builder::new()
            .name("uc2-remote-rx".into())
            .spawn(move || reader_role.run())
        {
            Ok(r) => r,
            Err(e) => {
                // The writer is already running and holds an `Arc<Link>`
                // nobody will ever close. Hand its handle over and close the
                // link, or it would spin forever on a connection with no
                // owner — and keep the `Link` alive with it.
                link.threads.lock().unwrap().push(writer);
                link.close();
                return Err(RemoteError::Io(e));
            }
        };
        link.threads.lock().unwrap().extend([writer, reader]);
        Ok(link)
    }

    /// Nanoseconds since the link was established — the clock every deadline
    /// and backoff in the table is expressed in.
    pub(crate) fn now_ns(&self) -> u64 {
        self.t0.elapsed().as_nanos() as u64
    }

    pub(crate) fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire) && !self.closed()
    }

    /// The current connection's absolute grant — the ceiling
    /// [`crate::engine::admissible`] applies to unanswered requests of both
    /// kinds.
    pub(crate) fn credits(&self) -> u32 {
        self.flow.credits()
    }

    /// The generation of the connection currently installed. A thread that
    /// holds a socket compares its own stamp against this before complaining
    /// about it.
    ///
    /// **Not** the gate on a credit grant any more: a grant is applied
    /// through [`FlowWord::try_update`], which compares and stores in one
    /// atomic step because the two facts are one fact.
    #[allow(
        dead_code,
        reason = "both threads carry their OWN stamp, taken under the `reconnect` lock at the \
                  instant they were handed their half of the connection — task 8's re-send and \
                  redial paths stamp their complaints with that, not with this. The lock-free \
                  mirror is what the tests read"
    )]
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(crate) fn inflight(&self) -> u64 {
        self.slots.inflight()
    }

    pub(crate) fn stats(&self) -> RemoteStats {
        self.stats.snapshot()
    }

    pub(crate) fn leader(&self) -> Option<(u32, String)> {
        self.leader.lock().unwrap().clone()
    }

    pub(crate) fn connected_addr(&self) -> Option<String> {
        if self.is_connected() {
            Some(self.addr.lock().unwrap().clone())
        } else {
            None
        }
    }

    /// SUBMITTER ONLY: the outgoing ring's producer role (`stage_frame`,
    /// `commit`, `release_to`). `RemoteSendHalf` is `Send` but not `Sync`, so
    /// exactly one thread can ever be here.
    pub(crate) fn out_producer(&self) -> &OutRing {
        &self.out
    }

    /// The correlation table. Unlike the two SPSC structures this is shared by
    /// design (module header): the submitter claims, the reader resolves, the
    /// writer marks sent, the sweep expires — all through the single-CAS
    /// protocol in `slots.rs`, which is sound from any thread. The submitter
    /// reaches it for `is_free`/`claim`/`publish_next_seq` and for the reclaim
    /// walk.
    pub(crate) fn slots(&self) -> &SlotTable {
        &self.slots
    }

    /// The highest sequence number the edge has acknowledged — the left edge
    /// of the credit window. Monotone, and carried across a redial (every live
    /// seq is strictly greater than it).
    pub(crate) fn acked_seq(&self) -> u64 {
        self.acked_seq.load(Ordering::Acquire)
    }

    /// POLLER ONLY: park until a completion is queued. Reads the wake seq
    /// BEFORE re-checking, so a publish landing in between is not slept
    /// through.
    pub(crate) fn park_completions(&self, timeout: Duration) {
        let observed = self.completions.ready().seq();
        if !self.completions.is_empty() || self.closed() {
            return;
        }
        self.completions.ready().park(observed, timeout);
    }

    /// Wake every parked poller. Signalling a [`crate::park::WaitCell`] is
    /// role-free (an atomic bump plus a condvar notify), so a caller's own
    /// shutdown path may do it.
    pub(crate) fn wake_completions(&self) {
        self.completions.publish();
    }

    /// COMPLETION-QUEUE PRODUCER ONLY — the reader thread, or a [`Link::close`]
    /// that has already joined it. Push one completion, parking (never
    /// dropping) while the poller is behind. `false` = the link is closed and
    /// the queue is full, so this outcome was dropped.
    ///
    /// **While the link is open the park terminates and nothing is dropped.**
    /// The poller drains the queue continuously while the link is up, so this
    /// park always ends with room to push: each drained record frees a queue
    /// entry, and the arena side is floored at `MAX_FRAME_LEN`, so draining
    /// even one record frees room for any body this wire admits. The slot and
    /// queue counts track `max_inflight` closely (a request is a live slot or a
    /// queued record, rarely both — the reader frees the slot just before it
    /// pushes the record, so `queued` can momentarily exceed it), but what
    /// guarantees the park ends is the poller's progress, not that bound.
    ///
    /// **After `close` the loop gives up instead.** The slot bound does not
    /// cover the arena: `push` also refuses a body that does not fit the free
    /// arena bytes, and after a close there is provably no consumer left to
    /// free any — the poll half is being dropped, or the caller has shut the
    /// link down and is inside `close`'s join. Parking there would wedge that
    /// join forever (`RemotePollHalf::drop -> close -> join(reader)` with the
    /// reader parked here). Exactly-once is a promise about a LIVE link; once
    /// the link is closed the caller has abandoned the outcomes, so dropping
    /// them is the only termination that exists — and it is strictly better
    /// than deadlocking the drop of the very half that would have received
    /// them. `close_common` signals BOTH cells precisely so a producer already
    /// parked here wakes up to observe `closed()`.
    fn complete(&self, r: Record, body: &[u8]) -> bool {
        loop {
            if self.completions.push(r, body) {
                return true;
            }
            // The poller is behind. Publish what is queued so it has something
            // to do, then park on its drain signal. While the link is open,
            // dropping is not an option: every accepted request owes exactly
            // one completion.
            let observed = self.completions.drained().seq();
            self.completions.publish();
            if self.completions.push(r, body) {
                return true;
            }
            if self.closed() {
                return false;
            }
            self.completions
                .drained()
                .park(observed, Duration::from_millis(1));
        }
    }

    /// COMPLETION-QUEUE PRODUCER ONLY. Fail every request past its deadline.
    /// Runs on the reader's tick — including the tick it keeps while waiting
    /// for the writer to publish a fresh read half, which is what keeps
    /// `request_timeout` honest while disconnected.
    fn sweep_deadlines(&self) -> usize {
        let now = self.now_ns();
        let mut fired = Vec::new();
        let n = self.slots.sweep(now, |ud| fired.push(ud));
        for ud in fired {
            // A `false` here means the link closed under us with a full queue
            // and no consumer; see `complete`.
            let _ = self.complete(Record::simple(ud, OutcomeTag::TimedOut), &[]);
        }
        if n > 0 {
            self.completions.publish();
        }
        n
    }

    /// COMPLETION-QUEUE PRODUCER ONLY. Fail every still-live request with
    /// `Closed`. Idempotent — the slot table's single-CAS protocol makes a
    /// second pass a no-op.
    ///
    /// A `Closed` that does not fit an already-full queue is dropped rather
    /// than parked on (see [`Link::complete`]): this runs with `closed` set,
    /// so there is no consumer that could make room, and parking would wedge
    /// the join this is called from.
    fn abort_outstanding(&self) {
        let mut aborted = Vec::new();
        self.slots.drain_abort(|ud| aborted.push(ud));
        for ud in aborted {
            let _ = self.complete(Record::simple(ud, OutcomeTag::Closed), &[]);
        }
        self.completions.publish();
    }

    /// Ask the writer thread for a fresh connection, complaining about
    /// `generation` — the connection the caller was holding. Idempotent.
    ///
    /// A request whose generation is not the current one is **ignored**, and
    /// counted as [`RemoteStats::stale_redials`]. Both threads can notice the
    /// same connection dying, and the loser's complaint arrives after the
    /// winner has already been served: without this gate it would shut down
    /// the brand-new socket, the reader would then take that socket's read
    /// half and immediately fail on it, and the link would churn one
    /// reconnect per lap instead of one per real failure.
    ///
    /// The whole check-and-shutdown runs under the `reconnect` lock, which is
    /// what makes it atomic against the writer's install (`redial` takes the
    /// same lock to bump the generation and publish the socket). Lock order is
    /// always `reconnect` -> `sock`, never the reverse.
    pub(crate) fn request_redial(&self, generation: u64, preferred: Option<String>) {
        let mut g = self.reconnect.lock().unwrap();
        if generation != g.epoch {
            self.stats.stale_redials.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // Shut the doomed socket down here, still holding the lock: it is the
        // connection named by `generation`, and no dial can install another
        // one until this critical section ends.
        if let Some(c) = self.sock.lock().unwrap().as_ref() {
            c.shutdown();
        }
        g.needed = true;
        if preferred.is_some() {
            g.preferred = preferred;
        }
        self.connected.store(false, Ordering::Release);
        drop(g);
        self.reconnect_cv.notify_all();
        self.out.wake().signal();
    }

    fn redial_needed(&self) -> bool {
        self.reconnect.lock().unwrap().needed
    }

    /// Queue a frame the reader needs written (a `PONG`).
    pub(crate) fn queue_control(&self, h: Header, payload: &[u8]) {
        let mut g = self.control.lock().unwrap();
        encode_frame(&mut g, h, payload);
        drop(g);
        self.out.wake().signal();
    }

    /// Ask the writer to write `seq`'s frame again **on this connection**, not
    /// before `delay`. The transient-`RETRY` path, and the only one that
    /// re-sends in place.
    ///
    /// Generation-gated at both stamps: a request answered (or swept) between
    /// the frame that asked for the retransmit and this call must not have its
    /// successor at the same index un-marked or held back. A refusal means
    /// there is nothing left to re-send, and nothing is queued.
    ///
    /// Un-marking `sent` is also what re-opens the probe gate, so the re-send
    /// this queues IS the next probe on an unproven connection — see
    /// [`probe_gate_open`].
    pub(crate) fn queue_retransmit(&self, seq: u64, delay: Duration) {
        if !self.slots.mark_sent_if(seq, false) {
            return;
        }
        self.slots
            .set_not_before_if(seq, self.now_ns() + delay.as_nanos() as u64);
        let mut g = self.retransmit.lock().unwrap();
        if !g.contains(&seq) {
            g.push(seq);
        }
        drop(g);
        self.out.wake().signal();
    }

    /// Hold `seq` back for `delay` **without re-opening the probe gate and
    /// without queueing anything** — the failover paths' half of
    /// [`Link::queue_retransmit`].
    ///
    /// The difference is not a detail; it is what keeps a doomed connection
    /// quiet. A `REDIRECT` or a `RETRY{NOT_SERVING}` says "this edge cannot
    /// serve you", and the reader answers it by asking for a redial. If it
    /// also cleared the `sent` stamp here, the probe gate would swing open for
    /// the few microseconds before that request lands, and the writer — which
    /// wakes on its own `WRITER_PARK` timer as well as on signals — could put
    /// ANOTHER frame on the connection that just refused one. The frame the
    /// redirect refused is un-marked by [`Writer::redial`]'s scan instead,
    /// once there is a fresh connection to put it on; only the backoff is
    /// recorded here, and it survives that scan because it lives in the slot.
    fn hold_off(&self, seq: u64, delay: Duration) {
        self.slots
            .set_not_before_if(seq, self.now_ns() + delay.as_nanos() as u64);
    }

    /// The leader the edge last named. `None` clears it (a `LEADER_CHANGED`
    /// with no address: the cluster is mid-election and nobody is claiming it).
    fn set_leader(&self, l: Option<(u32, String)>) {
        *self.leader.lock().unwrap() = l;
    }

    /// A backoff of `base` plus up to 25% jitter, floored and capped.
    pub(crate) fn jittered(&self, base: Duration) -> Duration {
        let base = base.clamp(MIN_RETRY_SLEEP, MAX_RETRY_SLEEP);
        let span = (base.as_micros() as u64 / 4).max(1);
        base + Duration::from_micros(self.next_rand() % span)
    }

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

    /// The half of shutdown that is safe from ANY thread: flag, wake, and tear
    /// the socket down. Returns whether this call was the one that closed the
    /// link.
    fn close_common(&self) -> bool {
        if self.closed.swap(true, Ordering::AcqRel) {
            return false;
        }
        self.connected.store(false, Ordering::Release);
        if let Some(c) = self.sock.lock().unwrap().take() {
            c.shutdown();
        }
        self.out.wake().signal();
        // BOTH completion cells: `ready` so a parked poller wakes and sees the
        // link is closed, `drained` so a producer parked in `complete` wakes
        // and observes `closed()` instead of waiting for a consumer that will
        // never come.
        self.completions.publish();
        self.completions.drained().signal();
        self.reconnect_cv.notify_all();
        true
    }

    /// `close`, called FROM one of the link's own threads — it must not join
    /// itself, so it only flags and wakes. The reader thread completes the
    /// outstanding requests on its way out (it is the completion producer);
    /// this call must not, or two producers would race.
    pub(crate) fn close_from_thread(&self) {
        self.close_common();
    }

    /// Close the link, join both threads, and complete every outstanding
    /// request with `Closed`. Idempotent, and safe from either half's `Drop`.
    ///
    /// The `threads` lock is held across the join AND the abort: it serialises
    /// two callers racing here (both halves dropping at once), so the thread
    /// that pushes the `Closed` records is provably the only live completion
    /// producer — the join above it ordered the reader's last push before it.
    pub(crate) fn close(&self) {
        self.close_common();
        let mut g = self.threads.lock().unwrap();
        for h in g.drain(..) {
            let _ = h.join();
        }
        self.abort_outstanding();
        drop(g);
    }
}

/// `poll`'s body, here rather than in `engine.rs` so the queue's record shape
/// — and its CONSUMER role — stays private to the link layer.
pub(crate) fn drain_completions(
    link: &Arc<Link>,
    mut cb: impl FnMut(RemoteCompletion<'_>),
) -> usize {
    link.completions.drain(POLL_BATCH, |rec, body| {
        cb(RemoteCompletion {
            user_data: rec.user_data,
            position: if rec.has_position {
                Some(rec.position)
            } else {
                None
            },
            outcome: outcome_of(rec.tag, body, rec.replayed, rec.expired),
        })
    })
}

// ------------------------------------------------------------------ writer

/// The writer thread's private state — and the crate's ONLY holder of the
/// [`OutRing`] consumer role and of the socket's write half. Built once in
/// [`Link::start`] and moved into the thread; not `Clone`, not `Sync`.
struct Writer {
    link: Arc<Link>,
    conn: FramedConn,
    /// The generation of the connection in `conn`. Every complaint this
    /// thread makes names it, so a complaint about a connection the reader
    /// already replaced is ignored rather than tearing down the fresh one.
    generation: u64,
    /// The seq of the first frame at or after `OutRing::send_pos` — the
    /// writer's own view of "what I have not put on the wire yet".
    ///
    /// It is a cursor rather than a lookup because the ring is a byte stream:
    /// one `write_all_bytes` may carry many frames, and the writer needs to
    /// know WHICH requests it just sent in order to mark their slots and count
    /// them (`frames_written`, the numerator of the batching factor). Seqs are
    /// gap-free from 1 and laid into the ring in issue order, so walking it
    /// forward past every extent that ends at or below `send_pos` is exact.
    cursor: u64,
    scratch: Vec<u8>,
    last_write: Instant,
    _not_sync: PhantomData<Cell<()>>,
}

impl Writer {
    fn new(link: Arc<Link>, conn: FramedConn) -> Writer {
        Writer {
            link,
            conn,
            generation: 0,
            cursor: 1,
            scratch: Vec::with_capacity(64 * 1024),
            last_write: Instant::now(),
            _not_sync: PhantomData,
        }
    }

    fn run(mut self) {
        loop {
            if self.link.closed() {
                return;
            }
            if self.link.redial_needed() {
                if self.redial() {
                    self.last_write = Instant::now();
                    continue;
                }
                return;
            }
            let mut did_work = false;
            // 1) control frames (a PONG the reader queued).
            {
                let mut g = self.link.control.lock().unwrap();
                if !g.is_empty() {
                    self.scratch.clear();
                    std::mem::swap(&mut self.scratch, &mut g);
                    drop(g);
                    if self.conn.write_all_bytes(&self.scratch).is_err() {
                        self.link.request_redial(self.generation, None);
                        continue;
                    }
                    self.link
                        .stats
                        .socket_writes
                        .fetch_add(1, Ordering::Relaxed);
                    did_work = true;
                    self.last_write = Instant::now();
                }
            }
            // 2) re-sends: frames that have been past `send_pos` once already
            //    — a transient `RETRY`, or the whole live window a redial
            //    rebuilt. Their bytes sit BELOW the ring's send cursor, so
            //    they go out BEFORE the drain does: re-send order is part of
            //    the contract (the edge's session dedup accepts a prefix), and
            //    draining first would let a later seq overtake an earlier one
            //    on the wire.
            let (resent, still_queued) = self.write_resends();
            if resent {
                did_work = true;
                self.last_write = Instant::now();
            }
            if self.link.redial_needed() {
                continue;
            }
            // 3) the ring drain: everything admissible in ONE write per
            //    contiguous run (two only when a frame straddles the wrap).
            //    Held back entirely while ANY re-send is still queued — even
            //    one that is only waiting out its backoff — for the ordering
            //    reason above. That is the old `RemoteClient::pump`'s rule
            //    ("stop rather than skip") expressed on the byte stream.
            if !still_queued && self.drain_ring() {
                did_work = true;
                self.last_write = Instant::now();
            }
            if did_work {
                continue;
            }
            // 3) PING when nothing has been written for `ping_interval`.
            if self.last_write.elapsed() >= self.link.cfg.ping_interval {
                let ping = Header {
                    ty: FrameType::Ping,
                    flags: 0,
                    version: PROTOCOL_VERSION,
                    client_id: self.link.client_id,
                    seq: 0,
                };
                if self.conn.write_frame(ping, &[]).is_err() {
                    self.link.request_redial(self.generation, None);
                    continue;
                }
                self.last_write = Instant::now();
                continue;
            }
            // 4) nothing to do: park on the ring's wake word. Read the seq
            //    BEFORE re-checking for work, so a signal that lands in
            //    between is not slept through.
            //
            // "Bytes are pending" is NOT on its own a reason to loop any
            // more, and getting that wrong is a busy spin rather than a
            // slowdown: while the probe gate is shut, or while a re-send is
            // waiting out its backoff, the drain is deliberately writing
            // nothing, so a loop that re-checked only `write_pos > send_pos`
            // would burn a core until the answer arrived. Both conditions are
            // resolved by another thread or by the clock, which is exactly
            // what `WRITER_PARK` is sized for.
            let observed = self.link.out.wake().seq();
            let drainable = self.link.out.write_pos() > self.link.out.send_pos()
                && !still_queued
                && probe_gate_open(&self.link);
            if drainable || self.link.redial_needed() || self.link.closed() {
                continue;
            }
            self.link.out.wake().park(observed, WRITER_PARK);
        }
    }

    /// Write the queued re-sends, in seq order. Returns
    /// `(wrote anything, anything still queued)` — the second half is what
    /// holds the byte drain back (see [`Writer::run`]).
    ///
    /// A re-send is a frame the ring has already streamed past, so it is
    /// copied back out by extent rather than drained. Three rules, all of them
    /// load-bearing:
    ///
    /// - **Only a LIVE slot is re-sent.** The extent words are advisory and a
    ///   later occupant of the same INDEX overwrites them, so the offset is
    ///   read through [`crate::slots::SlotTable::live_extent`], and liveness is
    ///   re-checked AFTER the copy. That second check is not belt-and-braces:
    ///   a seq is resolved at most once, so observing the SAME owner before
    ///   and after proves the slot was never freed in between — and therefore
    ///   that the submitter's `reclaim` (which stops at the oldest live slot)
    ///   cannot have released those bytes for re-use under the copy.
    /// - **A stamp that is refused skips the frame.** `mark_sent_if` answering
    ///   `false` means the request was answered or swept under us; writing it
    ///   anyway would be a re-send of something already resolved, and counting
    ///   it would drift `resends` from what actually went out.
    /// - **One frame while unproven.** The probe rule applies to a re-send
    ///   exactly as it does to a first transmission — after a redial, every
    ///   frame in the queue is a frame for an edge that has answered nothing
    ///   yet.
    ///
    /// CONSUMER ROLE: `copy_range` is reachable only from here.
    fn write_resends(&mut self) -> (bool, bool) {
        let link = Arc::clone(&self.link);
        let (due, mut still_queued) = take_due_resends(&link, link.now_ns());
        if due.is_empty() {
            return (false, still_queued);
        }
        let mut one = Vec::new();
        self.scratch.clear();
        let mut i = 0usize;
        while i < due.len() {
            let seq = due[i];
            if !probe_gate_open(&link) {
                // The probe is on the wire and unanswered: the rest of the
                // queue waits for it.
                break;
            }
            i += 1;
            let Some((off, len)) = link.slots.live_extent(seq) else {
                continue;
            };
            link.out.copy_range(off, len, &mut one);
            if !link.slots.is_live(seq) || !link.slots.mark_sent_if(seq, true) {
                // Resolved under us between the extent read and here: the
                // bytes just copied may already belong to a later request.
                continue;
            }
            self.scratch.extend_from_slice(&one);
            if link.slots.bump_attempts_if(seq).is_some_and(|n| n > 1) {
                link.stats.resends.fetch_add(1, Ordering::Relaxed);
            }
            link.stats.frames_written.fetch_add(1, Ordering::Relaxed);
            if !link.proven.load(Ordering::Acquire) {
                link.probe_seq.store(seq, Ordering::Release);
                break;
            }
            if self.scratch.len() >= RESEND_BATCH_BYTES {
                break;
            }
        }
        if i < due.len() {
            // Whatever was not reached goes back. Dropping it would strand a
            // live request until its deadline. The membership test is not
            // decoration: the reader may have queued one of these very seqs
            // (a RETRY answering the frame just written) while this batch was
            // being built, and a duplicate would be written — and counted —
            // twice. `take_due_resends` re-sorts, so order is restored there.
            let mut g = link.retransmit.lock().unwrap();
            for seq in &due[i..] {
                if !g.contains(seq) {
                    g.push(*seq);
                }
            }
            still_queued = true;
        }
        if self.scratch.is_empty() {
            return (false, still_queued);
        }
        if self.conn.write_all_bytes(&self.scratch).is_err() {
            // The batch never reached the peer. The frames are marked sent,
            // which would be a lie — but the redial this asks for re-scans
            // every live slot and un-marks it, so the correction is exactly
            // where the re-send order is rebuilt anyway.
            self.link.request_redial(self.generation, None);
            return (false, still_queued);
        }
        link.stats.socket_writes.fetch_add(1, Ordering::Relaxed);
        (true, still_queued)
    }

    /// Write whatever the ring holds up to [`flush_limit`] — which is
    /// everything on a proven connection and exactly ONE frame on an unproven
    /// one (probe-before-flush) — with one `write_all_bytes` per contiguous
    /// run (two only when a frame straddles the wrap). Returns whether
    /// anything went out.
    ///
    /// CONSUMER ROLE: `peek_upto`/`consume` are reachable only from here.
    fn drain_ring(&mut self) -> bool {
        let limit = flush_limit(&self.link, self.cursor);
        let mut wrote = false;
        while self.link.out.send_pos() < limit {
            let n = {
                let chunk = self.link.out.peek_upto(limit);
                if chunk.is_empty() {
                    break;
                }
                if self.conn.write_all_bytes(chunk).is_err() {
                    self.link.request_redial(self.generation, None);
                    return wrote;
                }
                chunk.len()
            };
            self.link.out.consume(n);
            self.link
                .stats
                .socket_writes
                .fetch_add(1, Ordering::Relaxed);
            wrote = true;
            self.advance_cursor();
        }
        wrote
    }

    /// Walk the frame cursor up to `send_pos`, marking every request it passes
    /// as sent and counting it.
    ///
    /// The bound is the submitter's published `next_seq`, and reading it here
    /// is safe without any further ordering: `publish_next_seq` is a `Release`
    /// store made BEFORE the `Release` store to the ring's `write` cursor, and
    /// the bytes this call is accounting for were observed through an
    /// `Acquire` load of that same `write` — so the slot for every frame in
    /// the run is already visible, metadata and all. (That is the same
    /// slot-before-bytes ordering `OutRing::stage_frame` documents, seen from
    /// the consumer's end.)
    ///
    /// A frame whose slot is no longer live — the request was answered, swept
    /// or aborted while its bytes were still in the ring — has **no readable
    /// extent** ([`crate::slots::SlotTable::live_extent`] answers `None`
    /// rather than a later occupant's offset), so it cannot be checked against
    /// `send_pos`. It is skipped rather than waited for, and still counted:
    /// the ring is a byte stream that goes out in order, so every committed
    /// frame reaches the wire exactly once whatever became of its request.
    /// Skipping it cannot run the cursor past an unsent LIVE frame — the ring
    /// is FIFO, so the next live frame's own extent check stops the walk.
    fn advance_cursor(&mut self) {
        let sent_to = self.link.out.send_pos();
        let next_seq = self.link.slots.next_seq();
        while self.cursor < next_seq {
            if let Some((off, len)) = self.link.slots.live_extent(self.cursor) {
                if off + len as u64 > sent_to {
                    break;
                }
                // Both stamps are generation-gated (and generation-tagged) —
                // the request can be answered and its index re-claimed by
                // `cursor + slot_count()` between the `live_extent` above and
                // these two calls, and stamping "sent" onto a fresh request
                // that has NOT been written is exactly what task 8's re-send
                // would then skip. A refusal here means the request resolved
                // under us, which needs no bookkeeping at all.
                self.link.slots.mark_sent_if(self.cursor, true);
                // A frame written more than once is a re-send by definition;
                // TASK 8 is what creates them, and this is where they are
                // counted so the counter cannot drift from what was written.
                if self
                    .link
                    .slots
                    .bump_attempts_if(self.cursor)
                    .is_some_and(|n| n > 1)
                {
                    self.link.stats.resends.fetch_add(1, Ordering::Relaxed);
                }
            }
            self.link
                .stats
                .frames_written
                .fetch_add(1, Ordering::Relaxed);
            self.cursor += 1;
        }
    }

    /// Dial a fresh connection, publish its read half to the reader, and reset
    /// the per-connection flow-control state. `false` = the link is closed,
    /// stop.
    fn redial(&mut self) -> bool {
        let link = Arc::clone(&self.link);
        link.stats.reconnects.fetch_add(1, Ordering::Relaxed);
        let mut backoff = RECONNECT_BACKOFF_START;
        loop {
            if link.closed() {
                return false;
            }
            let preferred = {
                let mut g = link.reconnect.lock().unwrap();
                g.needed = false;
                g.preferred.take()
            };
            let start = link.member_idx.load(Ordering::Relaxed) + 1;
            match dial(
                &link.cfg,
                link.client_id,
                preferred.as_deref(),
                start,
                &link.stats,
                Some(&link),
            ) {
                Ok((fresh, info, idx, addr)) => {
                    let Ok(read_half) = fresh.try_clone() else {
                        continue;
                    };
                    if read_half.set_read_timeout(Some(SWEEP_INTERVAL)).is_err()
                        || fresh.set_read_timeout(Some(SWEEP_INTERVAL)).is_err()
                        || fresh.set_write_timeout(Some(WRITE_TIMEOUT)).is_err()
                    {
                        continue;
                    }
                    let Ok(watch) = fresh.try_clone() else {
                        continue;
                    };
                    if link.closed() {
                        fresh.shutdown();
                        return false;
                    }
                    link.member_idx.store(idx, Ordering::Relaxed);
                    *link.addr.lock().unwrap() = addr;
                    if info.leader.is_some() {
                        *link.leader.lock().unwrap() = info.leader;
                    }
                    link.stats
                        .max_credits_seen
                        .fetch_max(info.credits, Ordering::Relaxed);
                    // ONE critical section installs the connection: bump the
                    // generation, publish the socket, reset the
                    // per-connection flow-control state, hand the read half
                    // over. Everything in here is what "the current
                    // connection" means, so a `request_redial` or a
                    // `credit_update` naming the OLD generation can neither
                    // interleave with it nor land after it.
                    //
                    // Order matters inside it too: the generation is bumped
                    // BEFORE the fresh `HELLO_OK` grant is stored, so a
                    // reader still draining the old connection's buffered
                    // frames is already out of date when it tries to apply a
                    // stale absolute credit count; and the read half is
                    // published LAST, so nobody can be at the new generation
                    // before the grant that belongs to it is visible.
                    let mut g = link.reconnect.lock().unwrap();
                    *link.sock.lock().unwrap() = Some(watch);
                    // Installing generation N+1 SATISFIES every outstanding
                    // complaint, because every one of them is about a
                    // generation <= N. Without this the second thread to
                    // notice the same dead connection leaves `needed` set
                    // behind a redial that already answered it, and the writer
                    // dials again the moment it returns — the churn the
                    // generation stamp exists to stop, arriving through the
                    // flag instead of through the shutdown. A complaint that
                    // lands AFTER this section names <= N and is refused by
                    // `request_redial`'s gate, so the two together leave no
                    // window. `preferred` goes with it: it was a hint about
                    // the connection just replaced.
                    g.needed = false;
                    g.preferred = None;
                    g.epoch += 1;
                    let generation = g.epoch;
                    link.generation.store(generation, Ordering::Release);
                    // `credits` resets from HELLO_OK, stamped with the
                    // generation it belongs to in the SAME word, so a grant
                    // still in flight from the connection just replaced cannot
                    // land on top of it (`FlowWord`); `acked_seq` is carried
                    // across — it only ever moves forward and every live seq
                    // is strictly greater than it.
                    link.flow.install(generation, info.credits);
                    link.proven.store(false, Ordering::Release);
                    link.probe_seq.store(0, Ordering::Release);
                    self.conn = fresh;
                    self.generation = generation;
                    // THE ORDERED RE-SEND OF THE LIVE WINDOW.
                    //
                    // The slot table IS the unacked window: every seq that is
                    // still live and whose bytes are still in the ring goes
                    // out again, in seq order, through the same probe-gated
                    // queue a `RETRY` uses — so a reconnect flushes ONE frame
                    // first and waits for it, exactly as the old
                    // `RemoteClient::pump` did.
                    //
                    // The scan keys on slot LIVENESS, never on `acked_seq`.
                    // `acked_seq` is a `fetch_max` of what the edge last
                    // advertised, and the edge advances it on SUBMIT only, so
                    // "everything above `acked_seq`" would silently drop an
                    // unanswered LOWER seq that an out-of-order completion had
                    // jumped over — a lost request, reported as nothing at
                    // all. A slot, by contrast, is live exactly while the
                    // request it holds is unanswered. Re-sending one the edge
                    // did answer is harmless by comparison: the edge's session
                    // dedup (`Sessioned`) answers it REPLAYED.
                    //
                    // `next_seq` is read BEFORE `write_pos` because the
                    // submitter publishes them in that order (`engine.rs`'s
                    // `send`): a seq that appears below `last` but whose bytes
                    // are not committed yet is left to the byte drain, which
                    // is what the `off + len > snapshot` break marks. `commit`
                    // publishes a whole frame at a time, so a frame is either
                    // entirely below the snapshot or entirely above it.
                    let last = link.slots.next_seq();
                    let snapshot = link.out.write_pos();
                    let mut requeue = Vec::new();
                    // The first seq whose frame the byte drain still owns.
                    let mut boundary = last;
                    for seq in link.oldest_unreclaimed.load(Ordering::Acquire).max(1)..last {
                        let Some((off, len)) = link.slots.live_extent(seq) else {
                            // Answered, swept or aborted — nothing to re-send.
                            // Its bytes (wherever they are) are the drain's
                            // problem, not ours.
                            continue;
                        };
                        if off + len as u64 > snapshot {
                            boundary = seq;
                            break;
                        }
                        link.slots.mark_sent_if(seq, false);
                        requeue.push(seq);
                    }
                    {
                        // A wholesale replacement, not an append: whatever the
                        // dead connection had queued is a subset of this scan
                        // (every entry was a live slot) and re-adding it would
                        // only duplicate. The per-slot `not_before` a RETRY or
                        // a self-REDIRECT set is untouched by this — it lives
                        // in the slot, so the backoff survives the redial that
                        // the very same frame asked for.
                        let mut g = link.retransmit.lock().unwrap();
                        g.clear();
                        g.extend_from_slice(&requeue);
                    }
                    // Everything below the snapshot is now the queue's job, so
                    // the byte drain resumes above it — and the frame cursor
                    // moves with it, to the first frame the drain still owns.
                    link.out.set_send_pos(snapshot);
                    self.cursor = self.cursor.max(boundary);
                    g.read_half = Some((generation, read_half));
                    drop(g);
                    link.connected.store(true, Ordering::Release);
                    link.reconnect_cv.notify_all();
                    return true;
                }
                Err(RemoteError::Closed) => return false,
                Err(RemoteError::HelloRefused { .. }) => {
                    // No member would answer differently: fail everything. The
                    // reader completes the outstanding requests as it exits.
                    link.close_from_thread();
                    return false;
                }
                Err(_) => {
                    sleep_watching(&link, backoff);
                    if link.closed() {
                        return false;
                    }
                    backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
                }
            }
        }
    }
}

/// **May the writer send MORE than the single probe frame?**
///
/// On a connection that has answered nothing (`!proven`: fresh, reconnected or
/// hopped) the writer puts exactly ONE request on the wire and waits. The
/// reason is cost, not politeness: an edge that cannot serve answers EVERY
/// submit with a `REDIRECT`, so flushing a window of N at the wrong member
/// costs N redirect frames the client then throws away — thousands per
/// election, measured in the M12c quickstart. A probe costs one frame and one
/// round trip on a connection that is about to be replaced anyway.
///
/// A probe counts as outstanding only while it is **on the wire**: a
/// `RETRY`/`UNKNOWN` answer marks its slot unsent again, and that re-send is
/// the next probe. Testing "is there a probe seq" alone would wedge the
/// connection there until the request timed out.
fn probe_gate_open(link: &Arc<Link>) -> bool {
    if link.proven.load(Ordering::Acquire) {
        return true;
    }
    let p = link.probe_seq.load(Ordering::Acquire);
    p == 0 || !(link.slots.is_live(p) && link.slots.is_sent(p))
}

/// How far into the ring the writer may flush, given the frame it is at.
/// Everything committed on a proven connection; exactly one frame on an
/// unproven one, and nothing at all while that one frame is unanswered (see
/// [`probe_gate_open`]).
///
/// The one frame is measured from the **ring's own length prefix**, not from
/// `cursor`'s slot extent. A request that was answered or swept while its
/// bytes were still queued has no readable extent at all
/// ([`crate::slots::SlotTable::live_extent`] answers `None` rather than a
/// later occupant's offset), and the drain must still be able to step over its
/// bytes — keying the limit on the slot would stall the writer for good on a
/// frame nobody is waiting for. The header is copied out through `copy_range`
/// so a frame straddling the wrap is read correctly.
fn flush_limit(link: &Arc<Link>, cursor: u64) -> u64 {
    let write_pos = link.out.write_pos();
    if link.proven.load(Ordering::Acquire) {
        return write_pos;
    }
    let send = link.out.send_pos();
    if !probe_gate_open(link) {
        return send;
    }
    if write_pos - send < HEADER_LEN as u64 {
        // Not even a header committed yet: nothing to measure, nothing to send.
        return send;
    }
    let mut hdr = Vec::with_capacity(HEADER_LEN);
    link.out.copy_range(send, HEADER_LEN as u32, &mut hdr);
    let Ok((_, payload_len)) = decode_header(&hdr) else {
        // Unreachable: these are the bytes this client encoded itself. If it
        // ever happens, the frame stream is corrupt and holding the drain
        // back is the safest answer.
        debug_assert!(
            false,
            "the outgoing ring holds a frame this client cannot decode"
        );
        return send;
    };
    if link.slots.is_live(cursor) {
        // The frame at `send_pos` is `cursor`'s by construction (the writer's
        // cursor is the first frame at or after it), so this names the request
        // whose answer opens the gate again.
        link.probe_seq.store(cursor, Ordering::Release);
    }
    (send + (HEADER_LEN + payload_len) as u64).min(write_pos)
}

/// Seqs due for a re-write, in seq order, plus whether anything is still
/// queued behind them.
///
/// An entry whose backoff has not expired **stops the walk** rather than being
/// skipped: re-send order is part of the contract, so a later seq must not
/// overtake an earlier one. An entry whose slot is no longer live is dropped —
/// it was answered, swept or aborted while it waited.
fn take_due_resends(link: &Arc<Link>, now_ns: u64) -> (Vec<u64>, bool) {
    let mut g = link.retransmit.lock().unwrap();
    if g.is_empty() {
        return (Vec::new(), false);
    }
    g.sort_unstable();
    let mut due = Vec::new();
    let mut taken = 0usize;
    while taken < g.len() {
        let seq = g[taken];
        if link.slots.not_before(seq) > now_ns && link.slots.is_live(seq) {
            break;
        }
        taken += 1;
        // A dead slot is dropped rather than returned: answered, swept or
        // aborted while it waited.
        if link.slots.is_live(seq) {
            due.push(seq);
        }
    }
    g.drain(..taken);
    let still_queued = !g.is_empty();
    (due, still_queued)
}

/// Sleep `total` in [`SWEEP_INTERVAL`] slices so `close` is noticed promptly.
/// It deliberately does NOT sweep deadlines — that is the reader's role (see
/// the module header), and the reader is sweeping on its own tick inside
/// `Reader::await_read_half` for the whole of this backoff.
fn sleep_watching(link: &Arc<Link>, total: Duration) {
    let end = Instant::now() + total;
    loop {
        if link.closed() {
            return;
        }
        let now = Instant::now();
        if now >= end {
            return;
        }
        std::thread::sleep((end - now).min(SWEEP_INTERVAL));
    }
}

// ------------------------------------------------------------------ reader

/// The reader thread's private state — and the crate's ONLY holder of the
/// [`CompletionQueue`] producer role while the threads run. Built once in
/// [`Link::start`] and moved into the thread; not `Clone`, not `Sync`.
struct Reader {
    link: Arc<Link>,
    rd: FramedConn,
    /// The generation of the connection in `rd`. Every complaint this thread
    /// makes names it, and every grant it reads off that connection is
    /// applied only while it is still current.
    generation: u64,
    last_recv: Instant,
    last_sweep: Instant,
    _not_sync: PhantomData<Cell<()>>,
}

impl Reader {
    fn new(link: Arc<Link>, rd: FramedConn) -> Reader {
        let now = Instant::now();
        Reader {
            link,
            rd,
            generation: 0,
            last_recv: now,
            last_sweep: now,
            _not_sync: PhantomData,
        }
    }

    fn run(mut self) {
        self.read_loop();
        // The reader is the completion producer, and it is exiting: complete
        // whatever is still outstanding here, so a link closed from a thread
        // (`close_from_thread`) still keeps its promise even if the caller
        // never calls `shutdown`. A `close` racing this joins first, so the
        // two passes are ordered, and the slot table's single-CAS protocol
        // makes the second a no-op.
        self.link.abort_outstanding();
    }

    fn read_loop(&mut self) {
        loop {
            if self.link.closed() {
                return;
            }
            // `dead_after` is the mid-frame bound as well as the silence bound.
            match self.rd.read_frame_buffered(self.link.cfg.dead_after) {
                Ok(Some((h, payload))) => {
                    self.last_recv = Instant::now();
                    let mut act = self.on_frame(h, payload);
                    while matches!(act, Act::Continue) {
                        match self.rd.next_buffered() {
                            Ok(Some((h2, p2))) => act = self.on_frame(h2, p2),
                            Ok(None) => break,
                            Err(_) => {
                                act = Act::Reconnect(None);
                                break;
                            }
                        }
                    }
                    // ONE wake for the whole read batch.
                    self.link.wake_completions();
                    match act {
                        Act::Continue => {}
                        // The reader's exit invariant is "the link is closed":
                        // `run` completes every outstanding request on the way
                        // out, and that is only sound once nothing else can
                        // still resolve them. TASK 9's mid-life HELLO_REFUSED
                        // is the first frame to take this arm.
                        Act::Stop => {
                            self.link.close_from_thread();
                            return;
                        }
                        Act::Reconnect(preferred) => {
                            self.link.request_redial(self.generation, preferred);
                            if !self.await_read_half() {
                                return;
                            }
                            continue;
                        }
                    }
                }
                Ok(None) => {}
                Err(_) => {
                    self.link.request_redial(self.generation, None);
                    if !self.await_read_half() {
                        return;
                    }
                    continue;
                }
            }
            let now = Instant::now();
            if now.duration_since(self.last_sweep) >= SWEEP_INTERVAL {
                self.last_sweep = now;
                self.link.sweep_deadlines();
            }
            if now.duration_since(self.last_recv) >= self.link.cfg.dead_after {
                self.link.request_redial(self.generation, None);
                if !self.await_read_half() {
                    return;
                }
            }
        }
    }

    /// Block until the writer thread publishes the read half of a fresh
    /// connection. `false` = the link closed while waiting.
    fn await_read_half(&mut self) -> bool {
        let link = Arc::clone(&self.link);
        let mut g = link.reconnect.lock().unwrap();
        loop {
            if link.closed() {
                return false;
            }
            if let Some((generation, fresh)) = g.read_half.take() {
                self.rd = fresh;
                self.generation = generation;
                let now = Instant::now();
                self.last_recv = now;
                self.last_sweep = now;
                return true;
            }
            let (guard, _) = link.reconnect_cv.wait_timeout(g, SWEEP_INTERVAL).unwrap();
            drop(guard);
            // The sweep has to keep running while we wait, or a disconnected
            // client stops enforcing `request_timeout` — and this thread is
            // the only one allowed to run it (module header).
            link.sweep_deadlines();
            g = link.reconnect.lock().unwrap();
        }
    }

    /// TASK 9 adds UNKNOWN and the mid-life HELLO_REFUSED. Everything else the
    /// edge can say is here: liveness (PING/PONG), flow control (STATUS), the
    /// one frame that resolves a request (RESPONSE), and the four failover
    /// answers (RETRY, REDIRECT, LEADER_CHANGED).
    fn on_frame(&mut self, h: Header, payload: bytes::Bytes) -> Act {
        match h.ty {
            // The one frame that resolves a request. Order inside this arm is
            // load-bearing: the slot is taken FIRST (the single CAS that makes
            // the completion exactly-once), and the credit grant riding the
            // same frame is applied AFTER — so a submitter woken by the wider
            // window always finds the slot it is about to reuse already free.
            FrameType::Response => {
                let Ok(meta) = crate::frame::ResponseMeta::decode(&payload) else {
                    // A malformed RESPONSE resolves nothing: the request stays
                    // live and its own deadline is what answers it. Dropping
                    // the frame is strictly safer than guessing a seq.
                    return Act::Continue;
                };
                let body = payload.slice(crate::frame::ResponseMeta::LEN..);
                // Anything answered with a RESPONSE proves this edge is
                // serving us: the window may flush now (probe-before-flush is
                // task 8, this is the flag it reads).
                self.link.proven.store(true, Ordering::Release);
                let expired = h.flags & crate::frame::FLAG_EXPIRED != 0;
                if expired {
                    self.link.stats.expired.fetch_add(1, Ordering::Relaxed);
                }
                if let crate::slots::Resolve::Won { user_data } = self.link.slots.resolve(h.seq) {
                    let rec = Record {
                        user_data,
                        position: meta.position,
                        has_position: true,
                        tag: OutcomeTag::Response,
                        replayed: h.flags & crate::frame::FLAG_REPLAYED != 0,
                        expired,
                        body_off: 0,
                        body_len: 0,
                    };
                    // `false` = the poller abandoned the queue after a close;
                    // see `Link::complete`, and the caveat on the exactly-once
                    // wording in `engine.rs`.
                    let _ = self.link.complete(rec, &body);
                }
                // A RESPONSE for a seq we no longer hold is not an error: a
                // swept, aborted or already-resolved request can legitimately
                // be answered late. The grant on it is still current, so it is
                // applied either way.
                credit_update(&self.link, self.generation, meta.credits, meta.acked_seq);
                Act::Continue
            }
            FrameType::Status => {
                let Ok(s) = crate::frame::Status::decode(&payload) else {
                    return Act::Continue;
                };
                // A STATUS that acknowledges the probe says what a RESPONSE
                // would: this edge took our write. A bare idle STATUS, whose
                // `acked_seq` is still below the probe, proves only that the
                // edge is alive — which is not the question the probe asks.
                let p = self.link.probe_seq.load(Ordering::Acquire);
                if p != 0 && s.acked_seq >= p {
                    self.link.proven.store(true, Ordering::Release);
                }
                credit_update(&self.link, self.generation, s.credits, s.acked_seq);
                Act::Continue
            }
            FrameType::Retry => {
                let Ok(r) = crate::frame::Retry::decode(&payload) else {
                    return Act::Continue;
                };
                if r.reason == crate::frame::RETRY_PAYLOAD_TOO_LARGE {
                    // Terminal: the payload will not get smaller by being sent
                    // again, and no other member would take it either.
                    if let crate::slots::Resolve::Won { user_data } = self.link.slots.resolve(h.seq)
                    {
                        let _ = self
                            .link
                            .complete(Record::simple(user_data, OutcomeTag::PayloadTooLarge), &[]);
                    }
                    return Act::Continue;
                }
                self.link.stats.retries.fetch_add(1, Ordering::Relaxed);
                let delay = self
                    .link
                    .jittered(Duration::from_micros(r.retry_after_us as u64));
                if r.reason == crate::frame::RETRY_NOT_SERVING {
                    // A statement about the edge's ROLE, not a transient
                    // shortage — and one that does not expire on this
                    // connection: the edge LATCHES a connection it has refused
                    // a write on (`uc_gateway`'s `Conn::latch_not_serving`,
                    // which is how the SUBMITs it accepts stay a prefix of
                    // what was sent), so re-sending here would be refused for
                    // as long as this socket lived, however quickly this
                    // member became the leader. Go somewhere else; the backoff
                    // still paces it, wherever we land.
                    self.link.hold_off(h.seq, delay);
                    let preferred = self
                        .link
                        .leader()
                        .map(|(_, a)| a)
                        .filter(|a| Some(a.as_str()) != self.link.connected_addr().as_deref());
                    return Act::Reconnect(preferred);
                }
                // Transient (`SERVICE_UNAVAILABLE` / `INSTANCE_RESTART`): this
                // edge is the right one, it is just not ready. Re-send in
                // place once the hint has expired.
                self.link.queue_retransmit(h.seq, delay);
                Act::Continue
            }
            FrameType::Redirect => {
                self.link.stats.redirects.fetch_add(1, Ordering::Relaxed);
                let Ok(l) = Leader::decode(&payload) else {
                    return Act::Continue;
                };
                if l.addr.is_empty() {
                    // Refused, not answered, and no hint where to go. The
                    // redial's own scan is what puts it back on the wire.
                    self.link.hold_off(h.seq, Duration::ZERO);
                    return Act::Reconnect(None);
                }
                self.link.set_leader(Some((l.node_id, l.addr.to_string())));
                if Some(l.addr) == self.link.connected_addr().as_deref() {
                    // "Elected but not serving": the edge redirects us to the
                    // address we are already on. Re-sending in place cannot
                    // work (the not-serving latch again), and a FRESH
                    // connection to the same address is the thing that changes
                    // the answer. The backoff is what stops that becoming a
                    // spin: the loop then runs at the backoff's rate, not at
                    // the frame rate.
                    self.link.hold_off(h.seq, SELF_REDIRECT_BACKOFF);
                    return Act::Reconnect(Some(l.addr.to_string()));
                }
                self.link.hold_off(h.seq, Duration::ZERO);
                Act::Reconnect(Some(l.addr.to_string()))
            }
            FrameType::LeaderChanged => {
                self.link
                    .stats
                    .leader_changes
                    .fetch_add(1, Ordering::Relaxed);
                let Ok(l) = Leader::decode(&payload) else {
                    return Act::Continue;
                };
                if l.addr.is_empty() {
                    // Mid-election: nobody is claiming it. Move, and let the
                    // dial scan find whoever answers.
                    self.link.set_leader(None);
                    return Act::Reconnect(None);
                }
                self.link.set_leader(Some((l.node_id, l.addr.to_string())));
                if Some(l.addr) == self.link.connected_addr().as_deref() {
                    // Already on the new leader's edge: reconnecting would
                    // only churn the in-flight window. This frame answers no
                    // request, so nothing is held back either.
                    return Act::Continue;
                }
                Act::Reconnect(Some(l.addr.to_string()))
            }
            FrameType::Ping => {
                self.link.queue_control(
                    Header {
                        ty: FrameType::Pong,
                        flags: 0,
                        version: PROTOCOL_VERSION,
                        client_id: self.link.client_id,
                        seq: h.seq,
                    },
                    &[],
                );
                Act::Continue
            }
            // A PONG carries no state: having arrived is the point.
            FrameType::Pong => Act::Continue,
            FrameType::Unknown => {
                // The edge timed our slot out on its side. `resend_on_unknown`
                // decides who answers: re-send it (the default — the edge lost
                // it, we still hold it), or surface UNKNOWN to the caller.
                self.link.stats.unknown.fetch_add(1, Ordering::Relaxed);
                if self.link.cfg.resend_on_unknown {
                    if self.link.slots.is_live(h.seq) {
                        self.link.queue_retransmit(h.seq, Duration::ZERO);
                    }
                } else if let crate::slots::Resolve::Won { user_data } =
                    self.link.slots.resolve(h.seq)
                {
                    let _ = self
                        .link
                        .complete(Record::simple(user_data, OutcomeTag::Unknown), &[]);
                }
                Act::Continue
            }
            FrameType::HelloRefused => {
                let reason = HelloRefused::decode(&payload)
                    .map(|r| r.reason)
                    .unwrap_or(0);
                // Same split as the dial path: what the refusal is ABOUT
                // decides who it is terminal for. FAULTED/BUSY are statements
                // about THIS EDGE, so they cost one member; anything else
                // (APP_ID/VERSION) is about US and no member would answer
                // differently.
                if reason == HELLO_REFUSED_FAULTED || reason == HELLO_REFUSED_BUSY {
                    self.link
                        .stats
                        .refused_members
                        .fetch_add(1, Ordering::Relaxed);
                    return Act::Reconnect(None);
                }
                Act::Stop
            }
            _ => Act::Continue,
        }
    }
}

/// What the reader should do after a frame.
pub(crate) enum Act {
    Continue,
    Reconnect(Option<String>),
    /// Stop the reader for good: a mid-life HELLO_REFUSED that no other member
    /// would answer differently (APP_ID/VERSION — about us, not the edge).
    Stop,
}

/// Apply an absolute grant read off the connection `generation`. `credits`
/// MAY decrease (and binds the next admission when it does); `acked_seq` is
/// monotone and is a statistic plus a re-send hint, never the window itself —
/// see [`crate::engine::admissible`].
///
/// A grant from a superseded connection is **dropped**. It is an ABSOLUTE
/// count, so applying one that was still sitting in a `next_buffered` batch
/// when the connection was replaced would silently overwrite the new
/// connection's `HELLO_OK` window with the old edge's — over-admission
/// against an edge that never granted it. The check and the store are ONE
/// CAS on [`FlowWord`], not a load-then-store pair: the pair leaves a
/// few-instruction window in which the writer thread installs the fresh
/// connection between the two, which is exactly the case being refused.
/// `acked_seq` is applied only when that CAS won, and is dropped with the
/// grant otherwise: it is per-connection state riding the same frame.
pub(crate) fn credit_update(link: &Arc<Link>, generation: u64, credits: u32, acked_seq: u64) {
    if !link.flow.try_update(generation, credits) {
        return;
    }
    link.stats
        .max_credits_seen
        .fetch_max(credits, Ordering::Relaxed);
    link.acked_seq.fetch_max(acked_seq, Ordering::AcqRel);
    // A wider window may have unblocked the writer.
    link.out.wake().signal();
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
/// `start_idx`, following `REDIRECT`s at the handshake — and hopping to the
/// leader a `HELLO_OK` names before adopting the connection that named it.
fn dial(
    cfg: &RemoteConfig,
    client_id: u64,
    preferred: Option<&str>,
    start_idx: usize,
    stats: &StatCells,
    between: Option<&Arc<Link>>,
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
    // Every address actually dialed in this scan. It is what stops the
    // leader-hop below from ping-ponging between two edges that name each
    // other, without the `order`-membership test the REDIRECT path uses (every
    // member is already in `order`, so that test would never let a hop happen).
    let mut visited: Vec<String> = Vec::with_capacity(n + 1);
    // A connection to an edge that named a leader elsewhere, held open while
    // that leader is tried. If the hop fails — the named leader is the one
    // that just died, which is exactly the case a failover puts us in — this
    // is still a working connection to a live member, and adopting it beats
    // returning `NoMembersReachable` and sitting disconnected through an
    // election (the edge will redirect, which is slower but never stuck).
    let mut fallback: Option<(FramedConn, HelloInfo, usize, String)> = None;
    while i < order.len() {
        // BETWEEN EVERY ATTEMPT AND EVERY REDIRECT HOP (each is one iteration
        // of this loop): abandon the scan if the link was shut down, so
        // `shutdown` is not quantised to a whole pass —
        // `(members + 1 + MAX_REDIRECT_HOPS)` attempts, each up to
        // `connect_timeout`.
        //
        // The old `RemoteClient` also swept request deadlines here. On the
        // split client that would make the WRITER a second completion producer
        // (module header): the reader is the only one, and it sweeps on its own
        // tick inside `Reader::await_read_half` for the entire duration of this
        // scan, so the `request_timeout` invariant is unchanged.
        if let Some(link) = between
            && link.closed()
        {
            if let Some((conn, ..)) = fallback {
                conn.shutdown();
            }
            return Err(RemoteError::Closed);
        }
        let addr = order[i].clone();
        visited.push(addr.clone());
        match dial_one(cfg, client_id, &addr) {
            Dialed::Ok(conn, info) => {
                let idx = cfg
                    .members
                    .iter()
                    .position(|m| *m == addr)
                    .unwrap_or(start_idx % n);
                // `HELLO_OK` named a leader that is not this edge. Hop to it
                // BEFORE committing to this connection.
                //
                // This is a throughput property, not a correctness one — the
                // edge would answer every write with a `REDIRECT` anyway — but
                // the cost is not small: the pipelined window is flushed the
                // moment a connection is adopted, so adopting a member that
                // cannot serve costs one redirect frame PER PENDING REQUEST,
                // and then a reconnect. Hopping here costs one extra
                // handshake.
                //
                // `leader` is `None` when the edge named nobody, and carries
                // the dialed address itself when `HELLO_OK` advertised no
                // address or its own (`addr_or`), so `!= addr` is exactly "a
                // usable address elsewhere".
                if let Some((_, leader_addr)) = info.leader.as_ref()
                    && *leader_addr != addr
                    && hops < MAX_REDIRECT_HOPS
                    && !visited.contains(leader_addr)
                {
                    hops += 1;
                    order.insert(i + 1, leader_addr.clone());
                    // Nothing has been written on it, so keeping it costs
                    // nothing and no request can be left in doubt either way.
                    match fallback {
                        None => fallback = Some((conn, info, idx, addr)),
                        Some(_) => conn.shutdown(),
                    }
                    i += 1;
                    continue;
                }
                return Ok((conn, info, idx, addr));
            }
            Dialed::Redirect(to) => {
                if hops < MAX_REDIRECT_HOPS && !order.contains(&to) {
                    hops += 1;
                    order.insert(i + 1, to);
                }
                i += 1;
            }
            // `FAULTED` and `BUSY` are the EDGE's problem, not the client's:
            // that gateway has taken itself out of service (its node's shmem
            // instance restarted under it, and a supervisor has to restart it),
            // or it is already serving `max_connections`. Every other member
            // may be perfectly healthy, so this costs one member and the scan
            // goes on — only a full pass of failures is `NoMembersReachable`.
            Dialed::Refused {
                reason: HELLO_REFUSED_FAULTED | HELLO_REFUSED_BUSY,
                ..
            } => {
                stats.refused_members.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
            // `APP_ID` / `VERSION` are about US. No member would answer
            // differently, so trying the rest would only turn one clear error
            // into `NoMembersReachable` — the least useful thing to tell an
            // operator who has mistyped a cluster name.
            Dialed::Refused { reason, detail } => {
                return Err(RemoteError::HelloRefused { reason, detail });
            }
            Dialed::Failed => i += 1,
        }
    }
    // Nothing better turned up: take the edge that redirects over no edge.
    if let Some(f) = fallback {
        return Ok(f);
    }
    Err(RemoteError::NoMembersReachable)
}

/// One attempt: TCP connect, `HELLO`, and whatever the edge answers.
///
/// Both halves are bounded by [`RemoteConfig::connect_timeout`] — the connect
/// by `connect_timeout` itself, the handshake by the read/write timeouts and
/// the deadline below — deliberately as two separate budgets rather than one
/// combined one. See the note on [`RemoteConfig::request_timeout`]: a single
/// budget would starve the handshake on a link whose connect is slow but
/// healthy, and the cost of two is a bounded `2 x connect_timeout` worst case
/// for one attempt — the term that appears in the invariant documented on
/// [`RemoteConfig::request_timeout`].
fn dial_one(cfg: &RemoteConfig, client_id: u64, addr: &str) -> Dialed {
    let Some(sa) = addr.to_socket_addrs().ok().and_then(|mut it| it.next()) else {
        return Dialed::Failed;
    };
    let Ok(sock) = TcpStream::connect_timeout(&sa, cfg.connect_timeout) else {
        return Dialed::Failed;
    };
    let Ok(mut conn) = FramedConn::new(sock) else {
        return Dialed::Failed;
    };
    if conn.set_read_timeout(Some(cfg.connect_timeout)).is_err()
        || conn.set_write_timeout(Some(cfg.connect_timeout)).is_err()
    {
        return Dialed::Failed;
    }
    let mut out = Vec::new();
    Hello {
        app_id: &cfg.app_id,
    }
    .encode(&mut out);
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
        // The REMAINING budget, not the constant: `read_frame` grants a fresh
        // `max_stall` once a partial frame has started, so a peer dribbling
        // bytes could otherwise stretch the handshake to ~2 x connect_timeout
        // and one attempt to ~3 x — contradicting the bound documented on
        // `RemoteConfig::request_timeout`.
        match conn.read_frame(deadline.saturating_duration_since(Instant::now())) {
            Ok(Some((h, payload))) => {
                return match h.ty {
                    FrameType::HelloOk => match HelloOk::decode(&payload) {
                        Ok(ok) => {
                            let leader = ok.leader.map(|id| (id, addr_or(ok.leader_addr, addr)));
                            Dialed::Ok(
                                conn,
                                HelloInfo {
                                    credits: ok.credits,
                                    leader,
                                },
                            )
                        }
                        Err(_) => Dialed::Failed,
                    },
                    FrameType::HelloRefused => match HelloRefused::decode(&payload) {
                        Ok(r) => Dialed::Refused {
                            reason: r.reason,
                            detail: r.detail.to_string(),
                        },
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
    if advertised.is_empty() {
        fallback.to_string()
    } else {
        advertised.to_string()
    }
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
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    let v = h.finish();
    if v == 0 { 0x5DEE_CE66_D1CE_4B1D } else { v }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::HelloOk;
    use std::net::TcpListener;

    /// Just enough edge for the two properties this module owns on its own —
    /// the connection-generation gate and the after-close completion
    /// bail-out. It answers `HELLO` with `HELLO_OK{credits}` and `PING` with
    /// `PONG`, so a `Link` against it stays connected and neither property is
    /// confounded by a reconnect. (The scripted `FakeEdge` under `tests/`
    /// belongs to the test targets and cannot be reached from a unit test.)
    struct StubEdge {
        addr: String,
        stop: Arc<AtomicBool>,
        acceptor: Option<std::thread::JoinHandle<()>>,
    }

    impl StubEdge {
        fn spawn(credits: u32) -> StubEdge {
            let l = TcpListener::bind("127.0.0.1:0").expect("bind");
            let addr = l.local_addr().unwrap().to_string();
            l.set_nonblocking(true).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let s = Arc::clone(&stop);
            let acceptor = std::thread::spawn(move || {
                let mut conns: Vec<std::thread::JoinHandle<()>> = Vec::new();
                while !s.load(Ordering::SeqCst) {
                    match l.accept() {
                        Ok((sock, _)) => {
                            sock.set_nonblocking(false).unwrap();
                            let s2 = Arc::clone(&s);
                            conns.push(std::thread::spawn(move || serve_stub(sock, credits, s2)));
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        Err(_) => break,
                    }
                }
                for c in conns {
                    let _ = c.join();
                }
            });
            StubEdge {
                addr,
                stop,
                acceptor: Some(acceptor),
            }
        }
    }

    impl Drop for StubEdge {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            if let Some(h) = self.acceptor.take() {
                let _ = h.join();
            }
        }
    }

    fn serve_stub(sock: TcpStream, credits: u32, stop: Arc<AtomicBool>) {
        let Ok(mut c) = FramedConn::new(sock) else {
            return;
        };
        if c.set_read_timeout(Some(Duration::from_millis(20))).is_err() {
            return;
        }
        loop {
            match c.read_frame(Duration::from_secs(5)) {
                Ok(Some((h, _))) => {
                    let wrote = match h.ty {
                        FrameType::Hello => {
                            let mut out = Vec::new();
                            // An empty `leader_addr` resolves to the dialed
                            // address (`addr_or`), so the dial never hops.
                            HelloOk {
                                credits,
                                leader: Some(1),
                                leader_addr: "",
                            }
                            .encode(&mut out);
                            c.write_frame(stub_hdr(FrameType::HelloOk, h.client_id, 0), &out)
                        }
                        FrameType::Ping => {
                            c.write_frame(stub_hdr(FrameType::Pong, h.client_id, h.seq), &[])
                        }
                        _ => Ok(()),
                    };
                    if wrote.is_err() {
                        return;
                    }
                }
                Ok(None) => {
                    if stop.load(Ordering::SeqCst) {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    }

    fn stub_hdr(ty: FrameType, client_id: u64, seq: u64) -> Header {
        Header {
            ty,
            flags: 0,
            version: PROTOCOL_VERSION,
            client_id,
            seq,
        }
    }

    fn stub_cfg(addr: &str, max_inflight: u32) -> RemoteConfig {
        RemoteConfig {
            app_id: "stub".into(),
            members: vec![addr.to_string()],
            max_inflight,
            ping_interval: Duration::from_millis(50),
            dead_after: Duration::from_millis(500),
            ..Default::default()
        }
    }

    /// The generation gate, in isolation: a complaint that names a connection
    /// other than the current one must be ignored outright — not shut the live
    /// socket down, not cost a reconnect. This is the half of the race that
    /// cannot be scheduled from outside (both threads noticing the same dead
    /// connection is a timing accident), so it is asserted directly.
    #[test]
    fn a_redial_request_that_does_not_name_the_current_connection_is_ignored() {
        let edge = StubEdge::spawn(5);
        let link = Link::start(stub_cfg(&edge.addr, 8)).expect("connect");
        assert!(link.is_connected());
        assert_eq!(link.credits(), 5);
        let generation = link.generation();

        link.request_redial(generation.wrapping_add(1), None);
        std::thread::sleep(Duration::from_millis(150));

        assert!(
            link.is_connected(),
            "a stale redial tore down a healthy connection"
        );
        assert_eq!(
            link.stats().reconnects,
            0,
            "a stale redial cost a reconnect"
        );
        assert_eq!(link.stats().stale_redials, 1);
        assert_eq!(
            link.credits(),
            5,
            "the live connection's grant must survive"
        );

        // The gate is a filter, not a mute: a complaint that DOES name the
        // current connection still works.
        link.request_redial(link.generation(), None);
        let deadline = Instant::now() + Duration::from_secs(10);
        while link.stats().reconnects == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(
            link.stats().reconnects,
            1,
            "a current-generation redial must be honoured"
        );
        link.close();
    }

    /// A generation bump makes the previous generation's grant unapplicable —
    /// the absolute credit count from a connection that has been replaced must
    /// never overwrite the new connection's `HELLO_OK` window. `acked_seq`
    /// rides the same frame and is dropped with it.
    #[test]
    fn a_grant_from_a_superseded_generation_is_dropped() {
        let edge = StubEdge::spawn(5);
        let link = Link::start(stub_cfg(&edge.addr, 8)).expect("connect");
        let generation = link.generation();
        credit_update(&link, generation, 9, 3);
        assert_eq!(link.credits(), 9, "a current-generation grant applies");
        assert_eq!(link.acked_seq(), 3, "and so does the acked_seq riding it");
        credit_update(&link, generation.wrapping_sub(1), 4096, 99);
        assert_eq!(
            link.credits(),
            9,
            "a superseded grant must not overwrite the live window"
        );
        assert_eq!(
            link.acked_seq(),
            3,
            "nor may its acked_seq be applied on its own"
        );
        link.close();
    }

    /// The word itself, without a socket in the way: the generation and the
    /// grant move together or not at all. This is the half of the race that
    /// cannot be scheduled from outside — the writer installing a fresh
    /// connection between a reader's load and its store — so the primitive
    /// that closes it is asserted directly.
    #[test]
    fn the_flow_word_refuses_a_grant_from_a_replaced_connection() {
        let f = FlowWord::new(7, 4);
        assert_eq!(f.credits(), 4);
        assert!(f.try_update(7, 9), "a current-generation grant lands");
        assert_eq!(f.credits(), 9);
        assert!(!f.try_update(6, 4096), "a stale generation is refused");
        assert_eq!(f.credits(), 9);

        // The connection is replaced: the new grant is in force at once, and
        // the old connection's — arriving late out of a buffered batch — is
        // refused however large it is.
        f.install(8, 2);
        assert_eq!(f.credits(), 2);
        assert!(!f.try_update(7, 4096));
        assert_eq!(f.credits(), 2, "the fresh HELLO_OK window must survive");
        assert!(
            f.try_update(8, 3),
            "the new connection's own grant still applies"
        );
        assert_eq!(f.credits(), 3);

        // A grant MAY decrease, and zero is a legal grant (the edge is asking
        // for silence, not reporting an error).
        assert!(f.try_update(8, 0));
        assert_eq!(f.credits(), 0);

        // Only the low 32 bits identify a connection. Asserted rather than
        // hidden: a grant would have to outlive 2^32 redials to alias, which
        // no buffered frame does.
        assert!(
            f.try_update(8 + (1u64 << 32), 5),
            "the generation tag is the low 32 bits"
        );
        assert_eq!(f.credits(), 5);
    }

    /// After `close` there is no consumer left, so a completion that does not
    /// fit must be given up on rather than parked on: parking there wedges the
    /// join inside `close` itself. Nothing has ever drained this queue, so it
    /// fills and then has to refuse.
    #[test]
    fn a_completion_into_a_full_queue_after_close_gives_up_instead_of_parking() {
        let edge = StubEdge::spawn(2);
        // `max_inflight = 1` gives the completion queue its 16-slot floor —
        // small enough to fill by hand.
        let link = Link::start(stub_cfg(&edge.addr, 1)).expect("connect");
        link.close();
        assert!(link.closed());

        let t = Instant::now();
        let mut n = 0u64;
        while link.complete(Record::simple(n, OutcomeTag::Closed), &[]) {
            n += 1;
            assert!(n < 10_000, "the queue never filled");
        }
        assert!(
            n >= 16,
            "the queue should have taken at least its slot floor, took {n}"
        );
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "complete parked on a full queue after close"
        );

        // And the close path itself: a second `close` runs `abort_outstanding`
        // against that same full queue and must still return.
        let t = Instant::now();
        link.close();
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "close wedged on a full completion queue"
        );
    }
}
