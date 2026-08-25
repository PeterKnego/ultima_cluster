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
//! - The `OutRing` **producer** role (`push_frame`, `release_to`) is reachable
//!   only through [`Link::out_producer`], whose sole caller is
//!   `RemoteSendHalf` — `Send` but not `Sync`, so exactly one submitter
//!   thread.
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
use crate::engine::{outcome_of, RemoteCompletion, RemoteConfig, RemoteStats};
use crate::error::RemoteError;
use crate::frame::{
    encode_frame, FrameType, Header, Hello, HelloOk, HelloRefused, Leader, HELLO_REFUSED_BUSY,
    HELLO_REFUSED_FAULTED, PROTOCOL_VERSION,
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
#[allow(
    dead_code,
    reason = "task 8 applies this to a self-REDIRECT; the reader does not \
              understand REDIRECT yet"
)]
const SELF_REDIRECT_BACKOFF: Duration = Duration::from_millis(10);
const RECONNECT_BACKOFF_START: Duration = Duration::from_millis(5);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_millis(500);
/// Hops followed during one connect scan.
const MAX_REDIRECT_HOPS: usize = 8;
/// Completions handed out per `poll` call — a bounded duty cycle.
const POLL_BATCH: usize = 256;

#[derive(Default)]
pub(crate) struct StatCells {
    #[allow(dead_code, reason = "task 8 counts REDIRECT frames here")]
    pub(crate) redirects: AtomicU64,
    #[allow(dead_code, reason = "task 8 counts LEADER_CHANGED frames here")]
    pub(crate) leader_changes: AtomicU64,
    pub(crate) reconnects: AtomicU64,
    #[allow(dead_code, reason = "task 8 counts the ordered window re-send here")]
    pub(crate) resends: AtomicU64,
    #[allow(dead_code, reason = "task 8 counts honoured RETRY frames here")]
    pub(crate) retries: AtomicU64,
    #[allow(dead_code, reason = "task 9 counts UNKNOWN frames here")]
    pub(crate) unknown: AtomicU64,
    #[allow(dead_code, reason = "task 6 counts EXPIRED responses here")]
    pub(crate) expired: AtomicU64,
    pub(crate) max_credits_seen: AtomicU32,
    pub(crate) refused_members: AtomicU64,
    pub(crate) socket_writes: AtomicU64,
    #[allow(
        dead_code,
        reason = "task 6 counts the frames each batched write carried — the \
                  numerator of the batching factor this milestone exists to move"
    )]
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
            socket_writes: self.socket_writes.load(Ordering::Relaxed),
            frames_written: self.frames_written.load(Ordering::Relaxed),
        }
    }
}

/// The redial request + read-half handoff. The READER asks; the WRITER dials.
struct Reconnect {
    needed: bool,
    preferred: Option<String>,
    /// The read half of the connection the writer just dialled, waiting to be
    /// picked up by the reader.
    read_half: Option<FramedConn>,
    /// Bumped on every successful dial, so the reader can tell a fresh half
    /// from the one it already took.
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
    credits: AtomicU32,
    /// The highest `acked_seq` any frame advertised. Monotone.
    #[allow(
        dead_code,
        reason = "task 6's `try_submit` gates the credit window on this; the \
                  reader already keeps it current"
    )]
    acked_seq: AtomicU64,
    /// The current connection has answered something only a serving edge can
    /// answer. Until then the writer sends ONE frame (probe-before-flush).
    #[allow(dead_code, reason = "task 8 gates the flush on this")]
    proven: AtomicBool,
    /// The single seq written while unproven; `0` = none.
    #[allow(dead_code, reason = "task 8 gates the flush on this")]
    probe_seq: AtomicU64,
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
        stats.max_credits_seen.store(info.credits, Ordering::Relaxed);

        let link = Arc::new(Link {
            slots: SlotTable::new(cfg.max_inflight),
            out: OutRing::new(cfg.out_ring_bytes_resolved()),
            completions: CompletionQueue::new(
                cfg.max_inflight as usize,
                cfg.arena_bytes_resolved(),
            ),
            credits: AtomicU32::new(info.credits),
            acked_seq: AtomicU64::new(0),
            proven: AtomicBool::new(false),
            probe_seq: AtomicU64::new(0),
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

    pub(crate) fn credits(&self) -> u32 {
        self.credits.load(Ordering::Acquire)
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

    /// SUBMITTER ONLY: the outgoing ring's producer role (`push_frame`,
    /// `release_to`). `RemoteSendHalf` is `Send` but not `Sync`, so exactly
    /// one thread can ever be here.
    #[allow(
        dead_code,
        reason = "task 6's `try_submit` is the sole caller; this task only \
                  establishes the link, so nothing is submitted yet"
    )]
    pub(crate) fn out_producer(&self) -> &OutRing {
        &self.out
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
    /// dropping) while the poller is behind.
    ///
    /// The park terminates: a request is either a live slot or a queued
    /// record, never both, so `live_slots + queued <= max_inflight <=
    /// CompletionQueue::entries()` — the slot side can always take one more.
    fn complete(&self, r: Record, body: &[u8]) {
        loop {
            if self.completions.push(r, body) {
                return;
            }
            // The poller is behind. Publish what is queued so it has something
            // to do, then park on its drain signal. Dropping is not an option:
            // every accepted request owes exactly one completion.
            let observed = self.completions.drained().seq();
            self.completions.publish();
            if self.completions.push(r, body) {
                return;
            }
            self.completions.drained().park(observed, Duration::from_millis(1));
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
            self.complete(Record::simple(ud, OutcomeTag::TimedOut), &[]);
        }
        if n > 0 {
            self.completions.publish();
        }
        n
    }

    /// COMPLETION-QUEUE PRODUCER ONLY. Fail every still-live request with
    /// `Closed`. Idempotent — the slot table's single-CAS protocol makes a
    /// second pass a no-op.
    fn abort_outstanding(&self) {
        let mut aborted = Vec::new();
        self.slots.drain_abort(|ud| aborted.push(ud));
        for ud in aborted {
            self.complete(Record::simple(ud, OutcomeTag::Closed), &[]);
        }
        self.completions.publish();
    }

    /// Ask the writer thread for a fresh connection. Idempotent.
    pub(crate) fn request_redial(&self, preferred: Option<String>) {
        // Shut the doomed socket down FIRST, while `needed` is still unset. If
        // the flag went up first, the writer could complete a fresh dial and
        // install its socket before this line ran, and we would tear down the
        // NEW connection instead of the old one. With this order the writer
        // cannot have dialled yet (it only dials when `needed` is set), and a
        // shutdown that beats the flag simply makes the writer's next write
        // fail — which lands here again.
        if let Some(c) = self.sock.lock().unwrap().as_ref() {
            c.shutdown();
        }
        let mut g = self.reconnect.lock().unwrap();
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

    /// Ask the writer to write `seq`'s frame again, not before `delay`.
    #[allow(
        dead_code,
        reason = "task 8's RETRY / self-REDIRECT handling is the caller; the \
                  reader does not understand those frames yet"
    )]
    pub(crate) fn queue_retransmit(&self, seq: u64, delay: Duration) {
        self.slots.mark_sent(seq, false);
        self.slots.set_not_before(seq, self.now_ns() + delay.as_nanos() as u64);
        let mut g = self.retransmit.lock().unwrap();
        if !g.contains(&seq) {
            g.push(seq);
        }
        drop(g);
        self.out.wake().signal();
    }

    /// A backoff of `base` plus up to 25% jitter, floored and capped.
    #[allow(dead_code, reason = "task 8 jitters the RETRY backoff with this")]
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
            position: if rec.has_position { Some(rec.position) } else { None },
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
    scratch: Vec<u8>,
    last_write: Instant,
    _not_sync: PhantomData<Cell<()>>,
}

impl Writer {
    fn new(link: Arc<Link>, conn: FramedConn) -> Writer {
        Writer {
            link,
            conn,
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
                        self.link.request_redial(None);
                        continue;
                    }
                    self.link.stats.socket_writes.fetch_add(1, Ordering::Relaxed);
                    did_work = true;
                    self.last_write = Instant::now();
                }
            }
            // 2) the ring drain: everything admissible in ONE write per
            //    contiguous run (two only when a frame straddles the wrap).
            if self.drain_ring() {
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
                    self.link.request_redial(None);
                    continue;
                }
                self.last_write = Instant::now();
                continue;
            }
            // 4) nothing to do: park on the ring's wake word. Read the seq
            //    BEFORE re-checking for work, so a signal that lands in
            //    between is not slept through.
            let observed = self.link.out.wake().seq();
            if self.link.out.write_pos() > self.link.out.send_pos()
                || self.link.redial_needed()
                || self.link.closed()
            {
                continue;
            }
            self.link.out.wake().park(observed, WRITER_PARK);
        }
    }

    /// Write whatever the ring holds. Returns whether anything went out.
    /// TASK 8 extends this with the probe-before-flush limit and the
    /// retransmit queue; at this task it drains unconditionally.
    ///
    /// CONSUMER ROLE: `peek_upto`/`consume` are reachable only from here.
    fn drain_ring(&mut self) -> bool {
        let limit = self.link.out.write_pos();
        let mut wrote = false;
        while self.link.out.send_pos() < limit {
            let n = {
                let chunk = self.link.out.peek_upto(limit);
                if chunk.is_empty() {
                    break;
                }
                if self.conn.write_all_bytes(chunk).is_err() {
                    self.link.request_redial(None);
                    return wrote;
                }
                chunk.len()
            };
            self.link.out.consume(n);
            self.link.stats.socket_writes.fetch_add(1, Ordering::Relaxed);
            wrote = true;
        }
        wrote
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
                    let Ok(read_half) = fresh.try_clone() else { continue };
                    if read_half.set_read_timeout(Some(SWEEP_INTERVAL)).is_err()
                        || fresh.set_read_timeout(Some(SWEEP_INTERVAL)).is_err()
                        || fresh.set_write_timeout(Some(WRITE_TIMEOUT)).is_err()
                    {
                        continue;
                    }
                    let Ok(watch) = fresh.try_clone() else { continue };
                    if link.closed() {
                        fresh.shutdown();
                        return false;
                    }
                    link.member_idx.store(idx, Ordering::Relaxed);
                    *link.addr.lock().unwrap() = addr;
                    if info.leader.is_some() {
                        *link.leader.lock().unwrap() = info.leader;
                    }
                    link.stats.max_credits_seen.fetch_max(info.credits, Ordering::Relaxed);
                    // `credits` resets from HELLO_OK; `acked_seq` is carried
                    // across — it only ever moves forward and every live seq
                    // is strictly greater than it.
                    link.credits.store(info.credits, Ordering::Release);
                    link.proven.store(false, Ordering::Release);
                    link.probe_seq.store(0, Ordering::Release);
                    *link.sock.lock().unwrap() = Some(watch);
                    self.conn = fresh;
                    // TASK 8 inserts the ordered resend of the live window here.
                    let mut g = link.reconnect.lock().unwrap();
                    g.read_half = Some(read_half);
                    g.epoch += 1;
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
    last_recv: Instant,
    last_sweep: Instant,
    _not_sync: PhantomData<Cell<()>>,
}

impl Reader {
    fn new(link: Arc<Link>, rd: FramedConn) -> Reader {
        let now = Instant::now();
        Reader { link, rd, last_recv: now, last_sweep: now, _not_sync: PhantomData }
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
                            self.link.request_redial(preferred);
                            if !self.await_read_half() {
                                return;
                            }
                            continue;
                        }
                    }
                }
                Ok(None) => {}
                Err(_) => {
                    self.link.request_redial(None);
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
                self.link.request_redial(None);
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
            if let Some(fresh) = g.read_half.take() {
                self.rd = fresh;
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

    /// TASK 6 adds RESPONSE, TASK 7 the credit plumbing, TASK 8 REDIRECT /
    /// LEADER_CHANGED / RETRY, TASK 9 UNKNOWN and HELLO_REFUSED. At this task
    /// the reader understands liveness and STATUS only.
    fn on_frame(&mut self, h: Header, payload: bytes::Bytes) -> Act {
        match h.ty {
            FrameType::Status => {
                if let Ok(s) = crate::frame::Status::decode(&payload) {
                    credit_update(&self.link, s.credits, s.acked_seq);
                }
                Act::Continue
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
            _ => Act::Continue,
        }
    }
}

/// What the reader should do after a frame.
pub(crate) enum Act {
    Continue,
    Reconnect(Option<String>),
    #[allow(
        dead_code,
        reason = "task 9 stops the reader on a mid-life HELLO_REFUSED that no                   other member would answer differently"
    )]
    Stop,
}

/// Apply an absolute grant. `credits` MAY decrease; `acked_seq` is monotone.
pub(crate) fn credit_update(link: &Arc<Link>, credits: u32, acked_seq: u64) {
    link.stats.max_credits_seen.fetch_max(credits, Ordering::Relaxed);
    link.credits.store(credits, Ordering::Release);
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
                let idx = cfg.members.iter().position(|m| *m == addr).unwrap_or(start_idx % n);
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
            Dialed::Refused { reason: HELLO_REFUSED_FAULTED | HELLO_REFUSED_BUSY, .. } => {
                stats.refused_members.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
            // `APP_ID` / `VERSION` are about US. No member would answer
            // differently, so trying the rest would only turn one clear error
            // into `NoMembersReachable` — the least useful thing to tell an
            // operator who has mistyped a cluster name.
            Dialed::Refused { reason, detail } => {
                return Err(RemoteError::HelloRefused { reason, detail })
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
    h.write_u128(SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0));
    let v = h.finish();
    if v == 0 { 0x5DEE_CE66_D1CE_4B1D } else { v }
}
