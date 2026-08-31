// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The split client: `RemoteEngine::connect` returns a [`RemoteSendHalf`]
//! (one submitter thread, nonblocking) and a [`RemotePollHalf`] (one poller
//! thread), mirroring `uc_client`'s `Engine` over a TCP connection.
//!
//! # The contract
//!
//! An `Ok(())` from `RemoteSendHalf::try_submit` obligates the engine to
//! deliver **exactly one** [`RemoteCompletion`] for that `user_data` through
//! [`RemotePollHalf::poll`] — unless the poller has abandoned the queue.
//! (Once the link is closed, the poll half is being dropped or has stopped
//! draining, and an outcome that does not fit the queue is given up on rather
//! than parked on forever; parking there would wedge the very drop that would
//! have received it. See `link::Link::complete`.)
//! [`SubmitError::Backpressure`] means the request was never accepted — retry
//! it. Redirects, leader changes, retries and connection loss are absorbed by
//! the link's own threads and are never completions.
//!
//! # The window
//!
//! Admission is a **count**: a request goes exactly while the number of
//! accepted-but-unanswered requests — SUBMITs and QUERIES alike — is under
//! both the edge's current `credits` grant and the caller's `max_inflight`.
//! That is the edge's own rule (`Conn::reserve`, `uc_gateway/src/conn.rs`),
//! and it is NOT `seq <= acked_seq + credits`: `acked_seq` advances on SUBMIT
//! only, so a seq-based window would close permanently after `credits`
//! queries. See [`admissible`] for the full argument, and
//! [`RemoteSendHalf::acked_seq`] for what `acked_seq` is actually good for.
//!
//! `credits` is an absolute grant carried by `HELLO_OK`, by every `RESPONSE`
//! and by an unsolicited `STATUS`. It **may decrease** — the edge halves it
//! under back-pressure — and a reduction is honoured immediately for new
//! admissions, without invalidating anything already in flight. The reader
//! thread applies a grant only if it belongs to the connection currently
//! installed (`link::credit_update`).
//!
//! # Thread roles
//!
//! [`RemoteSendHalf`] is `Send` but **not** `Sync`: it is the outgoing ring's
//! sole producer, and it carries the submitter-local seq and reclaim cursors
//! in `Cell`s. [`RemotePollHalf`] is the completion queue's sole consumer.
//! Neither half can reach the roles the link's own threads own — the
//! `OutRing` and the `CompletionQueue` are private fields of [`crate::link`]'s
//! `Link`, which hands each role out by ownership. See that module's header.

use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::completion::OutcomeTag;
use crate::error::RemoteError;
use crate::link::Link;

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
    ///
    /// **The enforcement invariant**: a pending request is failed with
    /// [`RemoteError::TimedOut`] no later than
    /// `request_timeout + 2 x connect_timeout + SWEEP_INTERVAL` — *including*
    /// while the client is disconnected and reconnecting. The sweep that
    /// enforces the budget runs on the reader's tick, **and on that same tick
    /// while the reader waits for the writer thread to publish a fresh
    /// connection**, so neither an endless redirect chase (every dial
    /// succeeding, so the reader never returns to its tick) nor an
    /// unreachable cluster (every dial failing, so the writer sleeps and then
    /// scans) can starve it. The `2 x connect_timeout` term is the one dial
    /// attempt that may already be in flight when the budget expires — a
    /// sweep cannot interrupt a blocking connect — and it is doubled because
    /// **one in-flight dial attempt bounds connect and the `HELLO` reply
    /// separately, each by `connect_timeout`**.
    ///
    /// Two caveats, both bounded and both deliberate:
    ///
    /// - A peer that stalls **mid-frame** parks the reader inside
    ///   `read_frame_buffered(dead_after)`, so the sweep can be delayed by up
    ///   to `dead_after` instead of `SWEEP_INTERVAL`. That bound is what
    ///   `dead_after` is for; shortening it here would mean re-issuing reads
    ///   mid-frame, which is the wedge `FramedConn`'s `max_stall` exists to
    ///   prevent.
    /// - The old `RemoteClient`'s third caveat — a submitting thread's write
    ///   under the state lock — is gone: on the split client a submit takes no
    ///   lock and makes no syscall, and the socket is owned by the writer
    ///   thread alone.
    ///
    /// The separate bounding an attempt gives its connect and its handshake —
    /// the reason for the `2 x` above — is deliberate. A single combined
    /// per-attempt budget would leave a slow-but-healthy link (a connect
    /// eating most of the budget on a cross-region hop) too little to finish
    /// its handshake, which is a worse failure than a bounded delay.
    ///
    /// Note this is **not** the socket write timeout — that is the crate's own
    /// `WRITE_TIMEOUT` constant (2 s).
    pub request_timeout: Duration,
    /// Per-address TCP connect + `HELLO` budget.
    pub connect_timeout: Duration,
    /// Send a `PING` when nothing has been written for this long, so an idle
    /// connection still proves itself. Must be well under `dead_after`.
    pub ping_interval: Duration,
    /// Treat the connection as dead when nothing at all has been *received* for
    /// this long, and fail over. The edge's `STATUS` timer and the `PONG` to our
    /// `PING` both count as traffic. Must exceed `ping_interval`, which
    /// [`RemoteConfig::validate`] enforces. Doubles as the bound on a peer that
    /// vanishes in the middle of a frame ([`crate::FramedConn::read_frame`]).
    pub dead_after: Duration,
    /// `UNKNOWN` means "may or may not have committed". `true` (the default)
    /// re-sends — correct with the edge's session envelope on, and the only way
    /// to get a definite answer. `false` surfaces [`RemoteError::Unknown`].
    pub resend_on_unknown: bool,
    /// Bytes reserved for the outgoing frame ring. `None` derives it:
    /// `max_inflight x (HEADER_LEN + 1344)`, floored at `MAX_FRAME_LEN` and
    /// rounded up to a power of two — big enough for a full window of
    /// max-payload commands (the node's 1344-byte ceiling, see
    /// `docs/reference/remote-protocol.md`) and for any single frame this wire
    /// admits. A `try_submit` whose frame does not fit the free space is
    /// `Backpressure`; one that could never fit the whole ring is
    /// `PayloadTooLarge`.
    pub out_ring_bytes: Option<usize>,
    /// Bytes reserved for the completion queue's body arena. `None` derives
    /// it: `max_inflight x 256`, floored at `MAX_FRAME_LEN`, rounded up to a
    /// power of two. The floor is what guarantees any single response body can
    /// be delivered, so a slow poller only ever delays the reader.
    pub completion_arena_bytes: Option<usize>,
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
            ping_interval: Duration::from_secs(1),
            dead_after: Duration::from_secs(3),
            resend_on_unknown: true,
            out_ring_bytes: None,
            completion_arena_bytes: None,
        }
    }
}

impl RemoteConfig {
    /// Refuse a configuration that cannot work, by name — the same posture
    /// `uc_gateway`'s `EdgeConfig::validate` takes on the other side of the
    /// wire. Called at the top of [`RemoteEngine::connect`] (and of
    /// [`crate::RemoteClient::connect`]), before any socket is opened, so a
    /// mistake reads as a configuration error rather than as "the cluster is
    /// unreachable".
    ///
    /// The rules, and why each one is a refusal rather than a silent
    /// adjustment:
    ///
    /// - **`app_id` empty** — it is checked byte-for-byte by the edge, and an
    ///   empty one is a legal-but-almost-certainly-unintended cluster name that
    ///   every member would refuse with `HELLO_REFUSED_APP_ID`.
    /// - **`members` empty** — there is nowhere to dial.
    /// - **`max_inflight == 0`** — no request could ever be admitted; `submit`
    ///   would block until `request_timeout` and then report `TimedOut`,
    ///   forever.
    /// - **`dead_after <= ping_interval`** — the liveness pair is then
    ///   self-defeating: the connection is declared dead at or before the first
    ///   `PING` could have been answered, so a perfectly healthy edge is
    ///   churned on a timer.
    pub fn validate(&self) -> Result<(), RemoteError> {
        if self.app_id.is_empty() {
            return Err(RemoteError::Config(
                "app_id is empty: it must match the edge's app_id exactly".into(),
            ));
        }
        if self.members.is_empty() {
            return Err(RemoteError::Config(
                "members is empty: at least one gateway address is needed to dial".into(),
            ));
        }
        if self.max_inflight == 0 {
            return Err(RemoteError::Config(
                "max_inflight must be greater than zero: no request could ever be admitted".into(),
            ));
        }
        if self.dead_after <= self.ping_interval {
            return Err(RemoteError::Config(format!(
                "dead_after ({:?}) must exceed ping_interval ({:?}): a healthy connection would \
                 be declared dead before its own PING could be answered",
                self.dead_after, self.ping_interval
            )));
        }
        Ok(())
    }

    /// The outgoing ring's size, derived when not set. See
    /// [`RemoteConfig::out_ring_bytes`]; `OutRing::new` applies the
    /// power-of-two rounding.
    pub(crate) fn out_ring_bytes_resolved(&self) -> usize {
        self.out_ring_bytes.unwrap_or_else(|| {
            let per = crate::frame::HEADER_LEN + 1344;
            (self.max_inflight as usize)
                .saturating_mul(per)
                .max(crate::frame::MAX_FRAME_LEN as usize)
        })
    }

    /// The completion arena's size, derived when not set. See
    /// [`RemoteConfig::completion_arena_bytes`]; `CompletionQueue::new`
    /// applies the `MAX_FRAME_LEN` floor and the power-of-two rounding.
    pub(crate) fn arena_bytes_resolved(&self) -> usize {
        self.completion_arena_bytes.unwrap_or_else(|| {
            (self.max_inflight as usize)
                .saturating_mul(256)
                .max(crate::frame::MAX_FRAME_LEN as usize)
        })
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
    /// Members that refused with `HELLO_REFUSED{FAULTED}` or
    /// `HELLO_REFUSED{BUSY}` and were skipped — at the dial, or mid-life on an
    /// established connection. Both refusals are about *that edge* (its node's
    /// shmem instance restarted under it; it is at its `max_connections`
    /// ceiling), not about this client, so each costs one member rather than
    /// the whole dial.
    pub refused_members: u64,
    /// Redial requests ignored because they named a connection that had
    /// already been replaced. Both link threads can notice the same
    /// connection dying; only the first complaint costs a reconnect, and this
    /// counts the ones that would otherwise have torn down the fresh
    /// connection behind it. Steady state is a small number next to
    /// `reconnects`, not zero.
    pub stale_redials: u64,
    /// `write_all_bytes` calls the writer thread made. `frames_written /
    /// socket_writes` is the batching factor — 1.0 is the old client's
    /// one-write-per-submit behaviour, which is what M13b exists to fix.
    pub socket_writes: u64,
    /// Frames those writes carried, re-sends included.
    pub frames_written: u64,
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

// ---------------------------------------------------------------- the halves

/// Read consistency for `RemoteSendHalf::try_query`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    /// Routed through the node's quorum read-index barrier.
    Linearizable,
    /// Answered from the local replica without a barrier round-trip.
    Snapshot,
}

/// Why a `try_submit`/`try_query` was refused at the door. A refusal means the
/// request was never accepted: no seq was consumed, no completion will come.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SubmitError {
    #[error("backpressure: the credit window, the inflight cap or the outgoing ring is full")]
    Backpressure,
    #[error("client is closed")]
    Closed,
    #[error("payload too large for one frame")]
    PayloadTooLarge,
}

/// One resolved request, handed to the callback passed to [`RemotePollHalf::poll`].
pub struct RemoteCompletion<'a> {
    /// The opaque token the submitter passed in.
    pub user_data: u64,
    /// The log position a command was applied at; `None` for anything but a
    /// `RESPONSE` (and `Some(0)` for a query, which the edge answers with
    /// position 0).
    pub position: Option<u64>,
    /// What became of the request.
    pub outcome: RemoteOutcome<'a>,
}

/// What became of a request. Exactly one per accepted `try_submit`/`try_query`.
#[derive(Debug)]
pub enum RemoteOutcome<'a> {
    /// The state machine's answer, borrowed from the completion queue's arena.
    /// `replayed` is the edge's `FLAG_REPLAYED` (the session cache answered);
    /// `expired` is `FLAG_EXPIRED` (the dedup window had moved past this seq,
    /// so the outcome of a write is unknowable) and then `body` is empty.
    Response {
        body: &'a [u8],
        replayed: bool,
        expired: bool,
    },
    /// The edge timed the slot out and `resend_on_unknown` is false.
    Unknown,
    /// The node refused the payload. Never re-sent.
    PayloadTooLarge,
    /// The `request_timeout` budget ran out.
    TimedOut,
    /// The client was shut down with this request outstanding.
    Closed,
}

/// **The admission rule, in one place.** A request may go exactly while the
/// number of requests already accepted and not yet answered is under BOTH the
/// edge's current grant and the caller's own `max_inflight` cap.
///
/// It is a **count**, not a seq range, and that is the edge's own rule rather
/// than a simplification of it: `uc_gateway`'s `Conn::reserve`/`Conn::claim`
/// admit while `inflight < credits`, counting SUBMITs and QUERIES alike. The
/// obvious-looking alternative — `seq <= acked_seq + credits` — is wrong
/// against that edge, because `acked_seq` advances on **SUBMIT only**
/// (`Conn::claim`, and the test pinning it in `uc_gateway/src/conn.rs`): a
/// client that ran queries would push its seq stream forward while the left
/// edge of such a window stood still, and the window would close permanently
/// after `credits` queries. `a_run_of_queries_never_closes_the_window` in
/// `tests/engine_fake_edge.rs` is that defect, reproduced.
///
/// `credits` is an **absolute grant that MAY decrease** (the edge halves it
/// under back-pressure and relaxes it back), and a reduction binds new
/// admissions immediately: `inflight` above a freshly-lowered grant simply
/// refuses until completions drain it. Requests already accepted are never
/// invalidated by it — the edge reserved their slots before it squeezed.
///
/// `acked_seq` deliberately does not appear here. It is kept as a monotone
/// statistic ([`RemoteSendHalf::acked_seq`]) and as a hint for the re-send
/// path, nothing more.
///
/// The two loads behind this — `inflight`, then `credits` — are not one
/// atomic snapshot, so a grant that shrinks between them can let exactly one
/// request through over the new ceiling. That is deliberate, and it is not a
/// protocol violation: an edge whose window is full simply stops reading the
/// socket (`Conn::wait_for_credit`), so the cost of the race is one frame
/// waiting in a TCP buffer, not a dropped connection. Taking a lock on the
/// submit path to close it would cost far more than it saves.
pub(crate) fn admissible(inflight: u64, credits: u32, max_inflight: u32) -> bool {
    inflight < u64::from(credits) && inflight < u64::from(max_inflight)
}

/// Constructor namespace, like `uc_client::Engine`.
pub struct RemoteEngine;

impl RemoteEngine {
    /// Validate, dial the first reachable member (following a `REDIRECT` at
    /// the handshake and hopping to a leader a `HELLO_OK` names), start the
    /// writer and reader threads, and hand back the two halves.
    ///
    /// The error contract is exactly [`crate::RemoteClient::connect`]'s:
    /// [`RemoteError::Config`] before any socket is opened,
    /// [`RemoteError::HelloRefused`] for a refusal no other member would
    /// answer differently, [`RemoteError::NoMembersReachable`] only after a
    /// full pass.
    pub fn connect(cfg: RemoteConfig) -> Result<(RemoteSendHalf, RemotePollHalf), RemoteError> {
        let link = Link::start(cfg)?;
        Ok((
            RemoteSendHalf {
                link: Arc::clone(&link),
                next_seq: Cell::new(1),
                reclaim_seq: Cell::new(1),
                reclaim_pos: Cell::new(0),
                _not_sync: PhantomData,
            },
            RemotePollHalf { link },
        ))
    }
}

/// The submit side: `&self`, nonblocking, never sleeps, never syscalls.
/// `Send` but **not** `Sync` — one submitter thread owns it (it carries the
/// submitter-local seq and reclaim cursors, and it is the outgoing ring's only
/// producer).
pub struct RemoteSendHalf {
    pub(crate) link: Arc<Link>,
    /// The next seq to issue. Submitter-local: gap-free, from 1.
    pub(crate) next_seq: Cell<u64>,
    /// The lowest seq whose ring bytes have not been reclaimed yet.
    pub(crate) reclaim_seq: Cell<u64>,
    /// The ring offset that cursor sits at.
    pub(crate) reclaim_pos: Cell<u64>,
    pub(crate) _not_sync: PhantomData<Cell<()>>,
}

/// The completion side: single owner, `Send`.
pub struct RemotePollHalf {
    pub(crate) link: Arc<Link>,
}

impl RemoteSendHalf {
    /// Submit a command. Nonblocking: no syscall, no allocation, no lock — the
    /// frame is encoded straight into the outgoing ring and the writer thread
    /// takes it from there.
    ///
    /// `Ok(())` obligates the engine to deliver **exactly one**
    /// [`RemoteCompletion`] carrying this `user_data`. An `Err` means the
    /// request was never accepted: no seq was consumed, nothing will complete.
    ///
    /// **The window is a count** ([`admissible`]): this is admitted exactly
    /// while the requests already accepted and not yet answered — queries
    /// included — number fewer than both the edge's current grant
    /// ([`RemoteSendHalf::credits`]) and `max_inflight`. The grant is
    /// absolute and MAY decrease; a reduction binds the next admission
    /// immediately.
    ///
    /// [`SubmitError::Backpressure`] is the normal, expected refusal — the
    /// credit window, the local `max_inflight` cap or the ring is full. **The
    /// wait strategy is the caller's**, deliberately: this crate never sleeps
    /// on the submitting thread. Yield (`std::thread::yield_now`) if the
    /// caller has other work, or park on
    /// [`RemotePollHalf::wait_handle`]'s [`RemoteWaitHandle::park`] — which
    /// wakes on the completion that frees the window. A bare spin is never
    /// right: the thing that opens the window is another thread, and burning
    /// a core is how it is starved.
    pub fn try_submit(&self, user_data: u64, cmd: &[u8]) -> Result<(), SubmitError> {
        self.send(
            crate::frame::FrameType::Submit,
            0,
            crate::slots::ReqKind::Submit,
            user_data,
            cmd,
        )
    }

    /// Issue a read. Same contract, same nonblocking shape and same wait
    /// strategy as [`RemoteSendHalf::try_submit`] — and the same count-based
    /// window: a query occupies one unit of the grant while it is unanswered,
    /// exactly as a submit does, and releases it on completion. It never
    /// advances `acked_seq`, which is why the window cannot be a seq range
    /// (see [`admissible`]).
    ///
    /// [`Consistency::Linearizable`] goes through the node's quorum read
    /// barrier; [`Consistency::Snapshot`] is answered from the replica the
    /// edge sits on.
    pub fn try_query(
        &self,
        user_data: u64,
        consistency: Consistency,
        q: &[u8],
    ) -> Result<(), SubmitError> {
        let flags = match consistency {
            Consistency::Linearizable => crate::frame::FLAG_LINEARIZABLE,
            Consistency::Snapshot => 0,
        };
        self.send(
            crate::frame::FrameType::Query,
            flags,
            crate::slots::ReqKind::Query,
            user_data,
            q,
        )
    }

    /// Encode one request into the ring and record its slot. The whole submit
    /// path, shared by [`RemoteSendHalf::try_submit`] and
    /// [`RemoteSendHalf::try_query`].
    ///
    /// **Every refusal happens before anything is consumed.** The seq, the
    /// slot and the ring bytes are only committed once all four checks pass —
    /// closed, size, window, slot — so a `Backpressure` leaves no gap in the
    /// seq stream (which the edge's session dedup would stall behind) and no
    /// orphan bytes in the ring. That is sound because the submitter is the
    /// only producer: nothing else can invalidate a check under it, and the
    /// other threads can only ever move the checks in the permissive
    /// direction (free a slot, widen the window, drain the ring).
    fn send(
        &self,
        ty: crate::frame::FrameType,
        flags: u8,
        kind: crate::slots::ReqKind,
        user_data: u64,
        bytes: &[u8],
    ) -> Result<(), SubmitError> {
        use crate::frame::{HEADER_LEN, Header, MAX_FRAME_LEN, PROTOCOL_VERSION};

        let link = &self.link;
        if link.closed() {
            return Err(SubmitError::Closed);
        }
        // Give back whatever the completed prefix is holding before deciding
        // the ring is full.
        self.reclaim();
        let out = link.out_producer();
        let need = HEADER_LEN + bytes.len();
        // A frame that could never fit — this wire's ceiling, or this ring's —
        // is a permanent error, not backpressure: retrying it would spin
        // forever. A frame that merely does not fit the FREE space right now
        // is backpressure, and `stage_frame` is what tells the two apart.
        if need > MAX_FRAME_LEN as usize || need > out.capacity() {
            return Err(SubmitError::PayloadTooLarge);
        }
        let seq = self.next_seq.get();
        // The admission rule (see `admissible`) and the slot this seq lands
        // on, both checked before the seq is consumed. `claim` re-checks the
        // slot, but checking here is what keeps a refusal free of side
        // effects: `claim` runs only after the bytes are staged.
        let slots = link.slots();
        if !admissible(slots.inflight(), link.credits(), link.cfg.max_inflight)
            || !slots.is_free(seq)
        {
            return Err(SubmitError::Backpressure);
        }
        let h = Header {
            ty,
            flags,
            version: PROTOCOL_VERSION,
            client_id: link.client_id,
            seq,
        };
        let Some((off, len)) = out.stage_frame(h, bytes) else {
            return Err(SubmitError::Backpressure);
        };
        let deadline_ns = link.now_ns() + link.cfg.request_timeout.as_nanos() as u64;
        // PUBLISH THE SLOT BEFORE THE BYTES. The writer thread may put the
        // frame on the wire the instant `commit` lands and the edge may answer
        // it immediately; a RESPONSE for a slot that does not exist yet
        // resolves nothing, and the completion this call just promised would
        // be lost. `stage_frame` exists precisely so these two can be ordered.
        // A refusal here is unreachable by the argument above — but the whole
        // point of that argument is the promise this call is making, so the
        // release path refuses too rather than trusting it. Committing after
        // a failed claim would put a frame on the wire with no slot behind
        // it: a request that can never complete, against an `Ok(())` that
        // says it must. The staged bytes are not published, so they leave no
        // residue (see `OutRing::stage_frame`).
        let claimed = slots.claim(seq, user_data, kind, deadline_ns, off, len);
        debug_assert!(
            claimed,
            "the window and the slot were both checked free above, and only this thread claims"
        );
        if !claimed {
            return Err(SubmitError::Backpressure);
        }
        self.next_seq.set(seq + 1);
        slots.publish_next_seq(seq + 1);
        out.commit(len);
        Ok(())
    }

    /// Release the ring bytes below the oldest still-live request, so the
    /// space can be reused.
    ///
    /// Keyed on **slot liveness**, not on `acked_seq`: the edge advances
    /// `acked_seq` on SUBMIT only (`uc_gateway/src/conn.rs`), so it is not a
    /// contiguous prefix of everything issued and cannot drive reclaim. A
    /// slot, by contrast, is live exactly while its bytes may still be needed
    /// — for a re-send after a redial (task 8) as much as for a completion.
    ///
    /// The frontier is read off the oldest LIVE slot rather than accumulated
    /// from the dead ones it walked past, and that is deliberate: a resolved
    /// slot's `off`/`len` belong to whichever later seq has re-claimed that
    /// index (every `slot_count()` seqs), so summing dead extents would
    /// happily release ring bytes still holding live requests. `live_extent`
    /// answers `None` instead of stale, and the oldest live request's own
    /// offset is exactly the frontier that must be preserved.
    ///
    /// The walk stops at the first still-live seq, so the reclaimed region is
    /// always a prefix, which is what the ring's single `ack` cursor can
    /// express. `release_to` clamps to `send_pos` on top of that, so a
    /// completed-but-unsent frame (swept past its deadline before the writer
    /// got to it) cannot be overwritten either.
    fn reclaim(&self) {
        let link = &self.link;
        let next = self.next_seq.get();
        let mut seq = self.reclaim_seq.get();
        while seq < next && !link.slots().is_live(seq) {
            seq += 1;
        }
        self.reclaim_seq.set(seq);
        // The writer thread's re-send scan starts here: the oldest seq whose
        // frame bytes are still in the ring. Below it there is nothing to
        // re-send — a released byte may already have been overwritten — and
        // nothing that NEEDS re-sending either, because this walk stopped at
        // the oldest LIVE slot.
        link.oldest_unreclaimed
            .store(seq, std::sync::atomic::Ordering::Release);
        let pos = if seq < next {
            // Everything below the oldest live request may go. A `None` here
            // means it resolved between the two reads — keep the previous
            // frontier and let the next call move it; never guess.
            match link.slots().live_extent(seq) {
                Some((off, _)) => off,
                None => self.reclaim_pos.get(),
            }
        } else {
            // Nothing is live, so every committed byte may go. The clamp to
            // `send_pos` inside `release_to` is what keeps a frame the writer
            // has not put on the wire yet from being overwritten.
            link.out_producer().write_pos()
        };
        let pos = pos.max(self.reclaim_pos.get());
        self.reclaim_pos.set(pos);
        link.out_producer().release_to(pos);
    }

    /// The last absolute grant the edge advertised — the ceiling on
    /// unanswered requests of both kinds ([`admissible`]). It MAY decrease,
    /// and a decrease is in force for the very next admission.
    pub fn credits(&self) -> u32 {
        self.link.credits()
    }

    /// Requests accepted but not yet completed. This — not the seq stream — is
    /// what the edge's grant bounds; see [`RemoteSendHalf::try_submit`].
    pub fn inflight(&self) -> u64 {
        self.link.inflight()
    }

    /// The highest sequence number the edge has acknowledged. **A statistic
    /// and a re-send hint, not part of the admission rule**: the edge advances
    /// it on SUBMIT only (`Conn::claim` in `uc_gateway`), so a run of queries
    /// leaves it standing still while requests flow perfectly well. Monotone,
    /// and carried across a redial.
    pub fn acked_seq(&self) -> u64 {
        self.link.acked_seq()
    }

    /// Counters for what the link had to do to keep its promise.
    pub fn stats(&self) -> RemoteStats {
        self.link.stats()
    }

    /// The leader the current edge last named, if any.
    pub fn leader(&self) -> Option<(u32, String)> {
        self.link.leader()
    }

    /// The identity every frame asserts — the key the edge's session dedup is
    /// per.
    pub fn client_id(&self) -> u64 {
        self.link.client_id
    }

    /// Whether a connection is currently established. `false` is not a
    /// failure: the writer thread is re-dialling and the window will be
    /// re-sent.
    pub fn is_connected(&self) -> bool {
        self.link.is_connected()
    }

    /// The address currently connected to (may be a redirect target that is
    /// not in `members`).
    pub fn connected_addr(&self) -> Option<String> {
        self.link.connected_addr()
    }

    /// Close the link and complete every outstanding request with
    /// [`RemoteOutcome::Closed`] — unless the poller has abandoned the queue,
    /// in which case the outcomes it can no longer take are dropped rather
    /// than parked on (see the module header). Idempotent; dropping both
    /// halves does the same.
    pub fn shutdown(&self) {
        self.link.close();
    }
}

impl RemotePollHalf {
    /// Drain up to `POLL_BATCH` completions, invoking `cb` for each; returns
    /// the count. Nonblocking — see [`RemotePollHalf::wait_handle`] to park
    /// between batches.
    ///
    /// Each [`RemoteOutcome::Response`] body is borrowed from the queue's
    /// arena and is valid only for that call of `cb`; copy what you need to
    /// keep.
    ///
    /// **Do not call [`RemoteSendHalf::shutdown`] (or drop either half) from
    /// inside `cb`.** The drain holds the consumer's cursors unpublished until
    /// it returns — a drop guard republishes them even if `cb` panics — and
    /// `shutdown` joins the link's threads, one of which may be parked waiting
    /// for exactly that space. Set a flag and shut down after `poll` returns.
    pub fn poll(&mut self, cb: impl FnMut(RemoteCompletion<'_>)) -> usize {
        crate::link::drain_completions(&self.link, cb)
    }

    /// A handle a poller thread can park on until something completes.
    pub fn wait_handle(&self) -> RemoteWaitHandle {
        RemoteWaitHandle {
            link: Arc::clone(&self.link),
        }
    }

    /// Counters for what the link had to do to keep its promise.
    pub fn stats(&self) -> RemoteStats {
        self.link.stats()
    }
}

/// Park until a completion is available. `Clone + Send + Sync`.
#[derive(Clone)]
pub struct RemoteWaitHandle {
    pub(crate) link: Arc<Link>,
}

impl RemoteWaitHandle {
    /// Park for at most `timeout`. Returns immediately if a completion is
    /// already queued or if one is published between the check and the park.
    pub fn park(&self, timeout: Duration) {
        self.link.park_completions(timeout);
    }

    /// Wake every parked poller (used by a caller's own shutdown path).
    pub fn wake(&self) {
        self.link.wake_completions();
    }
}

impl Drop for RemotePollHalf {
    fn drop(&mut self) {
        self.link.close();
    }
}

impl Drop for RemoteSendHalf {
    fn drop(&mut self) {
        self.link.close();
    }
}

// `Debug` on both halves is not decoration: `RemoteEngine::connect` returns
// them inside a `Result`, so `unwrap`/`unwrap_err`/`{:?}` on that result — the
// natural way to assert the connect contract — needs it.
impl fmt::Debug for RemoteSendHalf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteSendHalf")
            .field("client_id", &self.link.client_id)
            .field("connected", &self.link.is_connected())
            .finish()
    }
}

impl fmt::Debug for RemotePollHalf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemotePollHalf")
            .field("client_id", &self.link.client_id)
            .field("connected", &self.link.is_connected())
            .finish()
    }
}

/// Map a queue record's tag back to the public outcome.
pub(crate) fn outcome_of(
    tag: OutcomeTag,
    body: &[u8],
    replayed: bool,
    expired: bool,
) -> RemoteOutcome<'_> {
    match tag {
        OutcomeTag::Response => RemoteOutcome::Response {
            body,
            replayed,
            expired,
        },
        OutcomeTag::Unknown => RemoteOutcome::Unknown,
        OutcomeTag::PayloadTooLarge => RemoteOutcome::PayloadTooLarge,
        OutcomeTag::TimedOut => RemoteOutcome::TimedOut,
        OutcomeTag::Closed => RemoteOutcome::Closed,
    }
}
#[cfg(test)]
mod window_tests {
    use super::admissible;

    /// The whole rule, exhaustively over the interesting corners: an
    /// unanswered request of EITHER kind occupies one unit of the grant, and
    /// nothing else enters into it. No seq appears anywhere — that is the
    /// point (see the function's own doc).
    #[test]
    fn the_grant_bounds_the_number_of_unanswered_requests() {
        for credits in 0..=8u32 {
            for inflight in 0..=10u64 {
                assert_eq!(
                    admissible(inflight, credits, 1024),
                    inflight < u64::from(credits),
                    "credits {credits}, inflight {inflight}"
                );
            }
        }
    }

    #[test]
    fn a_zero_grant_admits_nothing() {
        assert!(
            !admissible(0, 0, 1024),
            "an idle client with no grant still waits"
        );
        assert!(!admissible(5, 0, 1024));
    }

    /// The grant is an ABSOLUTE count and it MAY decrease — below what is
    /// already in flight, even. Nothing new goes until the completions drain
    /// under the new grant; the requests already accepted are unaffected (the
    /// edge reserved their slots before it squeezed).
    #[test]
    fn a_reduced_grant_refuses_until_the_completions_drain() {
        assert!(
            !admissible(4, 1, 1024),
            "four in flight against a grant of one"
        );
        assert!(!admissible(2, 1, 1024));
        assert!(!admissible(1, 1, 1024));
        assert!(
            admissible(0, 1, 1024),
            "the last completion re-opens the window"
        );
    }

    /// `max_inflight` is a local cap ON TOP of the grant; the tighter of the
    /// two binds, whichever it happens to be.
    #[test]
    fn the_tighter_of_the_grant_and_the_local_cap_binds() {
        assert!(admissible(7, 1000, 8), "the local cap has room");
        assert!(
            !admissible(8, 1000, 8),
            "max_inflight binds below a generous grant"
        );
        assert!(
            !admissible(8, 8, 1000),
            "the grant binds below a generous local cap"
        );
        for credits in 0..=6u32 {
            for max_inflight in 0..=6u32 {
                for inflight in 0..=8u64 {
                    assert_eq!(
                        admissible(inflight, credits, max_inflight),
                        inflight < u64::from(credits.min(max_inflight)),
                        "credits {credits}, max_inflight {max_inflight}, inflight {inflight}"
                    );
                }
            }
        }
    }
}
