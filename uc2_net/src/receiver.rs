// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Receiver agents (spec §3.1/§5).
//!
//! Follower: DATA datagrams land position-addressed in the local buffer;
//! contiguity is tracked over absolute positions (never scanned from buffer
//! bytes); the `append` counter advances (Release) only to the contiguous
//! frontier, so the local archive and any reader see exactly the leader's
//! committed-frame discipline. Gaps NAK after a randomized ~RTT delay;
//! heartbeats reveal tail loss; statuses advertise contiguous + window
//! (quarter-window cadence with a time floor).
//!
//! Leader: the same socket's inbound side — demuxes NAK/status to the sender
//! agent over a bounded channel (control is kHz; a full channel drops, and
//! NAK backoff / status refresh recover).

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Instant;

use uc2_log::buffer::LogBuffer;
use uc2_log::writer::PositionedWriter;
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_APPEND_POSITION, DGRAM_KIND_COMMIT_POSITION, DGRAM_KIND_DATA,
    DGRAM_KIND_HEARTBEAT, DGRAM_KIND_NAK, DGRAM_KIND_REQUEST_VOTE, DGRAM_KIND_STATUS,
    DGRAM_KIND_TERM_MAP, DGRAM_KIND_VOTE, DatagramHeader, MAX_TERM_MAP_WIRE_ENTRIES, NAK_BODY_LEN,
    NakBody, REQUEST_VOTE_BODY_LEN, RequestVoteBody, STATUS_BODY_LEN, StatusBody, TermMapEntryWire,
    VOTE_BODY_LEN, VoteBody, read_datagram_header, read_nak_body, read_request_vote_body,
    read_status_body, read_term_map_body, read_vote_body, write_datagram_header, write_nak_body,
    write_status_body,
};
use uc_protocol::v2::frame::{self, FRAME_TYPE_PADDING, HEADER_LEN, align_frame_len};

use crate::TermHandle;
use crate::fault::FaultSocket;
use crate::rebuild::{NakConfig, NakTimer, Rebuilt};
use crate::sender::CtrlMsg;

/// Consensus-plane events demuxed off the shared UDP socket and routed to the
/// consensus agent (Task 8) when a receiver is in NODE MODE
/// (`set_consensus_route`). Kinds 5–9 forward RAW — carrying their own term so
/// the state machine, not the data plane, does term filtering and adoption
/// (a higher-term `RequestVote` MUST reach the SM). `LeaderActivity` is the
/// data plane's rate-limited liveness signal: current-term DATA/HEARTBEAT was
/// seen this duty cycle, so the SM should not time out the leader.
#[derive(Debug, Clone)]
pub enum NetEvent {
    Report { from: SocketAddr, term: u32, durable: u64 },
    CommitGossip { term: u32, commit: u64 },
    RequestVote { from: SocketAddr, body: RequestVoteBody },
    Vote { from: SocketAddr, body: VoteBody },
    TermMap { term: u32, entries: Vec<TermMapEntryWire> },
    /// Any current-term leader traffic (data/heartbeat) seen — liveness.
    LeaderActivity { term: u32 },
}

/// Parse a consensus-plane datagram (kinds 5–9) into a [`NetEvent`], RAW — no
/// term filter (the SM adopts higher terms). `from` is the datagram's source
/// address (Report/RequestVote/Vote address their reply by it). Returns `None`
/// for a malformed body (too short, or a term-map that fails its own checks);
/// the caller drops it. Non-consensus kinds return `None` (never reached — the
/// callers pre-match the kind).
fn consensus_event(h: &DatagramHeader, d: &[u8], from: SocketAddr) -> Option<NetEvent> {
    let body = &d[DATAGRAM_HEADER_LEN..];
    match h.kind {
        DGRAM_KIND_APPEND_POSITION => {
            Some(NetEvent::Report { from, term: h.leadership_term_id, durable: h.position })
        }
        DGRAM_KIND_COMMIT_POSITION => {
            Some(NetEvent::CommitGossip { term: h.leadership_term_id, commit: h.position })
        }
        DGRAM_KIND_REQUEST_VOTE if body.len() >= REQUEST_VOTE_BODY_LEN => {
            Some(NetEvent::RequestVote { from, body: read_request_vote_body(body) })
        }
        DGRAM_KIND_VOTE if body.len() >= VOTE_BODY_LEN => {
            Some(NetEvent::Vote { from, body: read_vote_body(body) })
        }
        DGRAM_KIND_TERM_MAP => {
            let mut out = [TermMapEntryWire { term: 0, base: 0 }; MAX_TERM_MAP_WIRE_ENTRIES];
            let count = read_term_map_body(body, &mut out)?;
            Some(NetEvent::TermMap {
                term: h.leadership_term_id,
                entries: out[..count].to_vec(),
            })
        }
        _ => None,
    }
}

/// True iff `kind` is a consensus-plane datagram (kinds 5–9) — routed RAW to
/// the consensus agent in node mode, bypassing the data-plane term filter.
#[inline]
fn is_consensus_kind(kind: u8) -> bool {
    matches!(
        kind,
        DGRAM_KIND_APPEND_POSITION
            | DGRAM_KIND_COMMIT_POSITION
            | DGRAM_KIND_REQUEST_VOTE
            | DGRAM_KIND_VOTE
            | DGRAM_KIND_TERM_MAP
    )
}

/// Walk the frames of a DATA payload: total stream advance, or None if
/// malformed (torn frame, zero length, padding not last / not header-only).
pub(crate) fn walk_advance(body: &[u8]) -> Option<u64> {
    let mut o = 0usize;
    let mut adv = 0u64;
    while o < body.len() {
        if o + HEADER_LEN > body.len() {
            return None;
        }
        let h = frame::read_header(&body[o..]);
        if (h.length as usize) < HEADER_LEN {
            return None;
        }
        let aligned = align_frame_len(h.length as usize);
        adv += aligned as u64;
        if h.frame_type == FRAME_TYPE_PADDING {
            // padding is sent header-only and is always the run's last frame
            return (o + HEADER_LEN == body.len()).then_some(adv);
        }
        o += aligned;
        if o > body.len() {
            return None;
        }
    }
    Some(adv)
}

#[derive(Debug, Clone, Copy)]
pub struct FollowerConfig {
    pub leader: SocketAddr,
    pub seed: u64,
    pub nak: NakConfig,
    /// Cap per NAK request; a bigger gap is re-requested as it drains.
    pub nak_max_bytes: u32,
    pub status_floor_ns: u64,
    /// Status every this many rebuilt bytes (0 = capacity/4, spec §5's
    /// quarter-window).
    pub status_bytes: u64,
    /// AppendPosition is sent on durable advance; the floor bounds the gap
    /// between reports when durable is quiescent (spec §6).
    pub append_pos_floor_ns: u64,
}

impl FollowerConfig {
    /// The term is no longer part of the config — it is a live [`TermHandle`]
    /// passed to [`FollowerReceiver::new`] (the consensus agent bumps it).
    pub fn new(leader: SocketAddr) -> Self {
        Self {
            leader,
            seed: 1,
            nak: NakConfig::default(),
            nak_max_bytes: 65_536,
            status_floor_ns: 100_000_000,
            status_bytes: 0,
            append_pos_floor_ns: 100_000_000,
        }
    }
}

#[derive(Default)]
pub struct FollowerStats {
    pub datagrams: AtomicU64,
    pub bytes: AtomicU64,
    pub dropped_stale_term: AtomicU64,
    pub dropped_dup: AtomicU64,
    pub dropped_overrun: AtomicU64,
    pub dropped_malformed: AtomicU64,
    /// DATA dropped because the intake gate was CLOSED (M4 reconciliation
    /// window — the consensus agent holds it shut; see `set_intake_gate`).
    pub dropped_gated: AtomicU64,
    pub naks_sent: AtomicU64,
    pub statuses_sent: AtomicU64,
    pub append_positions_sent: AtomicU64,
    pub commits_received: AtomicU64,
}

pub struct FollowerReceiver {
    buffer: Arc<LogBuffer>,
    writer: PositionedWriter,
    sock: FaultSocket,
    cfg: FollowerConfig,
    status_bytes: u64,
    rebuilt: Rebuilt,
    nak: NakTimer,
    leader_append: u64,
    base: Instant,
    last_status_ns: u64,
    status_at: u64,
    /// Durable value last reported via AppendPosition.
    ap_reported: u64,
    last_ap_ns: u64,
    /// Highest commit gossip accepted (shadow of the counter — this thread is
    /// the counter's single writer, so a plain field avoids the re-load).
    commit_seen: u64,
    recv_buf: Vec<u8>,
    stats: Arc<FollowerStats>,
    /// Live leadership term (M4). The consensus agent (Task 8) is the sole
    /// writer; the data path only loads it (`Relaxed`) to term-filter DATA/
    /// HEARTBEAT and to stamp its own NAK/STATUS/AppendPosition datagrams.
    term: TermHandle,
    /// Node mode (M4): when set, kinds 5–9 are forwarded RAW to the consensus
    /// agent (bypassing the term filter) and the local COMMIT_POSITION store is
    /// disabled — the consensus agent owns the commit counter (single writer).
    /// `None` = legacy M3 behavior byte-for-byte.
    route: Option<mpsc::SyncSender<NetEvent>>,
    /// Leader-control demux (M4 node composition): when set, inbound NAK/STATUS
    /// (data-plane control a follower addresses to its leader) are forwarded to
    /// the sender's control channel — so the SAME receiver that accepts DATA as
    /// a follower also feeds the sender's retransmit + flow-pacing when this
    /// node is the leader. Term-filtered like the data plane (control is
    /// term-scoped). `None` = follower-only (a follower never receives these).
    sender_route: Option<mpsc::SyncSender<CtrlMsg>>,
    /// Intake gate (M4). `false` closes the DATA arm and suppresses
    /// AppendPosition entirely; see [`set_intake_gate`](Self::set_intake_gate).
    gate: Option<Arc<AtomicBool>>,
    /// Per-duty-cycle latch: emit at most one `LeaderActivity` per `do_work`.
    activity_emitted: bool,
}

impl FollowerReceiver {
    pub fn new(
        buffer: Arc<LogBuffer>,
        sock: FaultSocket,
        cfg: FollowerConfig,
        term: TermHandle,
    ) -> Self {
        let start = buffer.counters().append.load_acquire();
        let status_bytes =
            if cfg.status_bytes == 0 { buffer.capacity() / 4 } else { cfg.status_bytes };
        let writer = PositionedWriter::new(Arc::clone(&buffer));
        Self {
            buffer,
            writer,
            sock,
            status_bytes,
            rebuilt: Rebuilt::new(start),
            nak: NakTimer::new(cfg.nak, cfg.seed),
            cfg,
            leader_append: start,
            base: Instant::now(),
            last_status_ns: 0,
            status_at: start,
            ap_reported: start,
            last_ap_ns: 0,
            commit_seen: 0,
            recv_buf: vec![0u8; 65_536],
            stats: Arc::new(FollowerStats::default()),
            term,
            route: None,
            sender_route: None,
            gate: None,
            activity_emitted: false,
        }
    }

    pub fn stats(&self) -> Arc<FollowerStats> {
        Arc::clone(&self.stats)
    }

    /// Put the receiver in NODE MODE (M4): consensus datagrams (kinds 5–9) are
    /// forwarded RAW to `tx` (no term filter — the SM adopts higher terms), and
    /// the local COMMIT_POSITION counter store is DISABLED (the consensus agent
    /// becomes the commit counter's single writer). DATA/HEARTBEAT still drive
    /// the data plane and additionally emit a rate-limited [`NetEvent::
    /// LeaderActivity`]. Without this call the receiver is byte-for-byte M3.
    /// `tx` should be sized by Task 8; a full channel drops (votes re-fire on
    /// the election timeout, gossip/reports re-send on their floors).
    pub fn set_consensus_route(&mut self, tx: mpsc::SyncSender<NetEvent>) {
        self.route = Some(tx);
    }

    /// Install the leader-control demux (M4 node composition). When set, inbound
    /// NAK/STATUS datagrams (kinds 3/4 — data-plane control a follower sends TO
    /// its leader) are forwarded to the sender's `CtrlMsg` channel `tx`, so a
    /// single unified receiver drives the sender's retransmit + flow-pacing
    /// while this node is the leader. A follower never receives these, so the
    /// route sits idle in the follower role. Term-filtered like DATA (a full
    /// channel drops — NAK backoff / status floor recover). Without this call
    /// NAK/STATUS are ignored (the M3 follower posture).
    pub fn set_sender_route(&mut self, tx: mpsc::SyncSender<CtrlMsg>) {
        self.sender_route = Some(tx);
    }

    /// Install the intake gate (M4). When the gate reads `false` the DATA arm
    /// drops (counted `dropped_gated`) and AppendPosition emission is suppressed
    /// ENTIRELY — the single mechanism that implements the sim's
    /// [`uc2_sim::world::DataPlane::Gated`] contract's ambiguous-window half:
    /// the consensus agent closes the gate on adopting a new term and reopens it
    /// only after reconciliation (`TermMapReceived` + any truncation), so no
    /// divergent-prefix extension is accepted and no raw durable escapes toward
    /// commit ranking while the follower's tail is unconfirmed. Default (no gate
    /// / gate `true`) = current behavior.
    pub fn set_intake_gate(&mut self, gate: Arc<AtomicBool>) {
        self.gate = Some(gate);
    }

    #[inline]
    fn gate_open(&self) -> bool {
        self.gate.as_ref().is_none_or(|g| g.load(Ordering::Relaxed))
    }

    /// Emit one `LeaderActivity` per duty cycle (node mode only). A closed or
    /// full route is fine — the latch still trips so we never spam the channel.
    fn note_leader_activity(&mut self, term: u32) {
        if self.activity_emitted {
            return;
        }
        let emitted = match &self.route {
            Some(route) => {
                let _ = route.try_send(NetEvent::LeaderActivity { term });
                true
            }
            None => false,
        };
        if emitted {
            self.activity_emitted = true;
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.sock.local_addr().expect("bound socket")
    }

    fn now_ns(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    /// One duty cycle: drain up to 64 datagrams, then NAK/status upkeep.
    pub fn do_work(&mut self) -> bool {
        let mut did = false;
        self.activity_emitted = false; // one LeaderActivity per cycle (node mode)
        for _ in 0..64 {
            let mut buf = std::mem::take(&mut self.recv_buf);
            let r = self.sock.recv_from(&mut buf);
            let got = match r {
                Ok(Some((n, from))) => Some((n, from)),
                _ => None,
            };
            if let Some((n, from)) = got {
                self.on_datagram(&buf[..n], from);
                did = true;
            }
            self.recv_buf = buf;
            if got.is_none() {
                break;
            }
        }
        did |= self.upkeep();
        did
    }

    fn on_datagram(&mut self, d: &[u8], from: SocketAddr) {
        use Ordering::Relaxed;
        if d.len() < DATAGRAM_HEADER_LEN {
            self.stats.dropped_malformed.fetch_add(1, Relaxed);
            return;
        }
        let h = read_datagram_header(d);
        // Node mode: consensus kinds (5–9) are forwarded RAW to the consensus
        // agent — no data-plane term filter, since a higher-term RequestVote
        // MUST reach the SM. This also disables the local COMMIT_POSITION store
        // (kind 6 routes as CommitGossip; the consensus agent owns the counter).
        if self.route.is_some() && is_consensus_kind(h.kind) {
            if let Some(ev) = consensus_event(&h, d, from)
                && let Some(route) = &self.route
            {
                let _ = route.try_send(ev);
            }
            return;
        }
        let term = self.term.load(Relaxed);
        if h.leadership_term_id != term {
            self.stats.dropped_stale_term.fetch_add(1, Relaxed);
            return;
        }
        self.stats.datagrams.fetch_add(1, Relaxed);
        match h.kind {
            DGRAM_KIND_DATA => {
                // Intake gate (M4): during the ambiguous term-adoption window
                // the consensus agent holds the gate CLOSED so no
                // divergent-prefix extension is accepted. Still count the
                // leader's liveness (this is current-term traffic).
                if !self.gate_open() {
                    self.stats.dropped_gated.fetch_add(1, Relaxed);
                    self.note_leader_activity(h.leadership_term_id);
                    return;
                }
                let body = &d[DATAGRAM_HEADER_LEN..];
                let contiguous = self.rebuilt.contiguous();
                // Accept rule: never rewrite at-or-below the frontier —
                // readers may be reading those bytes. Partial overlaps are
                // re-requested from `contiguous` by the next NAK.
                if h.position < contiguous {
                    self.stats.dropped_dup.fetch_add(1, Relaxed);
                    return;
                }
                // Corrupt-header hardening (M2 final review): the wire has no
                // CRC, so a flipped position bit must fail closed. Misaligned
                // positions would corrupt reader framing; a position whose
                // sum with `advance` wraps u64 would sneak past the overrun
                // gate below as a tiny wrapped value.
                if !h.position.is_multiple_of(frame::FRAME_ALIGNMENT as u64) {
                    self.stats.dropped_malformed.fetch_add(1, Relaxed);
                    return;
                }
                let Some(advance) = walk_advance(body) else {
                    self.stats.dropped_malformed.fetch_add(1, Relaxed);
                    return;
                };
                // Empty-body DATA: walk_advance(&[]) == Some(0). It is
                // malformed, not an overrun; catching it here also guarantees
                // rebuilt.insert never sees a zero-length (pos, pos) range.
                if advance == 0 {
                    self.stats.dropped_malformed.fetch_add(1, Relaxed);
                    return;
                }
                // Overrun gate on the STREAM ADVANCE, not the wire bytes: a
                // wrap-padding run is header-only on the wire (32 B) yet
                // advances its full padding span, so write_run's bytes-based
                // guard under-checks it — a padding datagram could push the
                // frontier past durable + capacity and underflow the status
                // window. Enforce the spec guard on `advance` here; write_run's
                // own bytes guard stays as belt-and-suspenders.
                let durable = self.buffer.counters().durable.load_acquire();
                let Some(end) = h.position.checked_add(advance) else {
                    self.stats.dropped_malformed.fetch_add(1, Relaxed);
                    return;
                };
                if end > durable + self.buffer.capacity() {
                    self.stats.dropped_overrun.fetch_add(1, Relaxed);
                    return;
                }
                if !self.writer.write_run(h.position, body) {
                    // beyond durable + capacity (archive lagging) or wrap-
                    // crossing garbage: flow control should prevent the
                    // former; NAK/replay recovers either way
                    self.stats.dropped_overrun.fetch_add(1, Relaxed);
                    return;
                }
                self.stats.bytes.fetch_add(body.len() as u64, Relaxed);
                if self.rebuilt.insert(h.position, h.position + advance) {
                    self.buffer.counters().append.store_release(self.rebuilt.contiguous());
                }
                self.note_leader_activity(h.leadership_term_id);
            }
            DGRAM_KIND_HEARTBEAT => {
                self.leader_append = self.leader_append.max(h.position);
                self.note_leader_activity(h.leadership_term_id);
            }
            DGRAM_KIND_COMMIT_POSITION => {
                // Legacy mode ONLY: in node mode kind 6 is routed above as
                // CommitGossip and never reaches here (the consensus agent owns
                // the commit counter). Keep the local monotonic store for M3.
                // Monotonic max: UDP-reordered gossip never regresses. The
                // stored value is the CLUSTER commit; M5's apply agent clamps
                // to min(commit, local contiguous durable) at consumption
                // (spec §6) — the counter itself stays raw.
                self.stats.commits_received.fetch_add(1, Relaxed);
                if h.position > self.commit_seen {
                    self.commit_seen = h.position;
                    self.buffer.counters().commit.store_release(self.commit_seen);
                }
            }
            DGRAM_KIND_NAK if self.sender_route.is_some() => {
                // Leader role (M4): a follower's NAK, demuxed to our sender so it
                // retransmits the missing span. Term-checked above.
                let body = &d[DATAGRAM_HEADER_LEN..];
                if body.len() >= NAK_BODY_LEN
                    && let Some(route) = &self.sender_route
                {
                    let b = read_nak_body(body);
                    let _ = route.try_send(CtrlMsg::Nak { from, position: b.position, length: b.length });
                }
            }
            DGRAM_KIND_STATUS if self.sender_route.is_some() => {
                // Leader role (M4): a follower's flow-window advert, demuxed to
                // our sender's quorum pacing.
                let body = &d[DATAGRAM_HEADER_LEN..];
                if body.len() >= STATUS_BODY_LEN
                    && let Some(route) = &self.sender_route
                {
                    let b = read_status_body(body);
                    let _ = route.try_send(CtrlMsg::Status {
                        from,
                        contiguous: b.contiguous_position,
                        window: b.receive_window,
                    });
                }
            }
            _ => {} // control kinds for the consensus agent: M3
        }
    }

    fn upkeep(&mut self) -> bool {
        use Ordering::Relaxed;
        let mut did = false;
        let now = self.now_ns();
        let term = self.term.load(Relaxed); // stamps our NAK/STATUS/AppendPosition
        let contiguous = self.rebuilt.contiguous();

        // Gap = missing bytes before out-of-order data, else missing tail
        // revealed by the leader's heartbeat position.
        let gap = self.rebuilt.first_gap().or({
            if self.leader_append > contiguous {
                Some((contiguous, self.leader_append))
            } else {
                None
            }
        });
        // The timer arms on first sight of a gap and fires once its
        // randomized ~RTT delay has elapsed. Poll twice per duty cycle: the
        // first call arms a freshly-observed gap; the second, with a fresh
        // clock read, fires it if the delay has already passed. In production
        // the real delay (hundreds of µs) dwarfs the nanoseconds between the
        // two reads, so a due NAK fires on a later cycle, never early; the
        // backoff deadline set by a firing poll keeps the second poll from
        // double-sending.
        let fired = self.nak.poll(gap, now).or_else(|| self.nak.poll(gap, self.now_ns()));
        if let Some((start, end)) = fired {
            let len = (end - start).min(self.cfg.nak_max_bytes as u64) as u32;
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN + NAK_BODY_LEN];
            write_datagram_header(
                &mut d,
                &DatagramHeader {
                    position: 0,
                    leadership_term_id: term,
                    kind: DGRAM_KIND_NAK,
                    flags: 0,
                },
            );
            write_nak_body(&mut d[DATAGRAM_HEADER_LEN..], &NakBody { position: start, length: len });
            let _ = self.sock.send_to(&d, self.cfg.leader);
            self.stats.naks_sent.fetch_add(1, Relaxed);
            did = true;
        }

        // Single durable load reused by AppendPosition + status below.
        let durable = self.buffer.counters().durable.load_acquire();

        // AppendPosition (spec §6): report our durable on advance (block/
        // fsync granularity, ~kHz) or on the floor. Feeds the leader's
        // quorum commit ranking. SUPPRESSED ENTIRELY while the intake gate is
        // closed (M4): during term adoption our raw durable may cover a
        // divergent, unconfirmed tail — letting it escape toward commit ranking
        // is exactly the phantom-commit source the Gated contract forbids. The
        // ap_reported/last_ap_ns cursors are left untouched, so the first
        // AppendPosition after the gate reopens fires immediately.
        if self.gate_open()
            && (durable > self.ap_reported || now - self.last_ap_ns >= self.cfg.append_pos_floor_ns)
        {
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
            write_datagram_header(
                &mut d,
                &DatagramHeader {
                    position: durable,
                    leadership_term_id: term,
                    kind: DGRAM_KIND_APPEND_POSITION,
                    flags: 0,
                },
            );
            let _ = self.sock.send_to(&d, self.cfg.leader);
            self.ap_reported = durable;
            self.last_ap_ns = now;
            self.stats.append_positions_sent.fetch_add(1, Relaxed);
            did = true;
        }

        // Status: every quarter-window of progress, or on the time floor.
        if contiguous - self.status_at >= self.status_bytes
            || now - self.last_status_ns >= self.cfg.status_floor_ns
        {
            // The advance-guard in on_datagram makes underflow unreachable;
            // saturate anyway so a future guard regression degrades to a
            // window=0 backpressure signal rather than a bogus ~4 GiB window.
            let window = (durable + self.buffer.capacity()).saturating_sub(contiguous) as u32;
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN + STATUS_BODY_LEN];
            write_datagram_header(
                &mut d,
                &DatagramHeader {
                    position: 0,
                    leadership_term_id: term,
                    kind: DGRAM_KIND_STATUS,
                    flags: 0,
                },
            );
            write_status_body(
                &mut d[DATAGRAM_HEADER_LEN..],
                &StatusBody { contiguous_position: contiguous, receive_window: window },
            );
            let _ = self.sock.send_to(&d, self.cfg.leader);
            self.status_at = contiguous;
            self.last_status_ns = now;
            self.stats.statuses_sent.fetch_add(1, Relaxed);
            did = true;
        }
        did
    }
}

/// Leader-side inbound-demux counters. Shared via `Arc` so a supervising
/// thread can read them after the receiver has moved into its agent closure
/// (the same pattern as `FollowerStats` / `SenderStats`).
#[derive(Default)]
pub struct LeaderStats {
    /// Control dropped because the sender channel was full (recoverable).
    pub dropped_full: AtomicU64,
    /// Inbound control with a mismatched leadership term (M3 static term).
    pub dropped_stale_term: AtomicU64,
}

/// The leader-side inbound demux: NAK/status/AppendPosition → the sender's
/// channel. NAK/status are term-checked data-plane control (stale terms are
/// dropped and counted). In NODE MODE (`set_consensus_route`) the consensus
/// kinds 5–9 are forwarded RAW to the consensus agent instead — including
/// AppendPosition, which becomes a [`NetEvent::Report`] (the consensus agent,
/// not the sender's tracker, does commit ranking in M4).
pub struct LeaderReceiver {
    sock: UdpSocket,
    to_sender: mpsc::SyncSender<CtrlMsg>,
    term: TermHandle,
    recv_buf: Vec<u8>,
    stats: Arc<LeaderStats>,
    /// Node mode (M4): kinds 5–9 forwarded RAW to the consensus agent (no term
    /// filter). `None` = legacy M3 (AppendPosition → sender tracker).
    route: Option<mpsc::SyncSender<NetEvent>>,
}

impl LeaderReceiver {
    pub fn new(
        sock: UdpSocket,
        to_sender: mpsc::SyncSender<CtrlMsg>,
        term: TermHandle,
    ) -> io::Result<Self> {
        sock.set_nonblocking(true)?;
        Ok(Self {
            sock,
            to_sender,
            term,
            recv_buf: vec![0u8; 2048],
            stats: Arc::new(LeaderStats::default()),
            route: None,
        })
    }

    pub fn stats(&self) -> Arc<LeaderStats> {
        Arc::clone(&self.stats)
    }

    /// Put the receiver in NODE MODE (M4): kinds 5–9 (AppendPosition,
    /// CommitGossip, RequestVote, Vote, TermMap) are forwarded RAW to `tx` —
    /// no term filter, since the SM adopts higher terms. NAK/STATUS still go to
    /// the sender's channel (flow control is data-plane). Without this call the
    /// receiver is byte-for-byte M3.
    pub fn set_consensus_route(&mut self, tx: mpsc::SyncSender<NetEvent>) {
        self.route = Some(tx);
    }

    pub fn do_work(&mut self) -> bool {
        use Ordering::Relaxed;
        let mut did = false;
        for _ in 0..64 {
            let (n, from) = match self.sock.recv_from(&mut self.recv_buf) {
                Ok(x) => x,
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::ConnectionRefused
                        || e.kind() == io::ErrorKind::ConnectionReset =>
                {
                    break;
                }
                Err(_) => break,
            };
            did = true;
            if n < DATAGRAM_HEADER_LEN {
                continue;
            }
            let h = read_datagram_header(&self.recv_buf);
            // Node mode: consensus kinds (5–9) forwarded RAW (no term filter).
            if self.route.is_some() && is_consensus_kind(h.kind) {
                if let Some(ev) = consensus_event(&h, &self.recv_buf[..n], from)
                    && let Some(route) = &self.route
                {
                    let _ = route.try_send(ev);
                }
                continue;
            }
            if h.leadership_term_id != self.term.load(Relaxed) {
                self.stats.dropped_stale_term.fetch_add(1, Relaxed);
                continue;
            }
            let body = &self.recv_buf[DATAGRAM_HEADER_LEN..n];
            let msg = match h.kind {
                DGRAM_KIND_NAK if body.len() >= NAK_BODY_LEN => {
                    let b = read_nak_body(body);
                    Some(CtrlMsg::Nak { from, position: b.position, length: b.length })
                }
                DGRAM_KIND_STATUS if body.len() >= STATUS_BODY_LEN => {
                    let b = read_status_body(body);
                    Some(CtrlMsg::Status {
                        from,
                        contiguous: b.contiguous_position,
                        window: b.receive_window,
                    })
                }
                DGRAM_KIND_APPEND_POSITION => {
                    Some(CtrlMsg::AppendPos { from, durable: h.position })
                }
                _ => None,
            };
            if let Some(m) = msg
                && self.to_sender.try_send(m).is_err()
            {
                // safe: NAK backoff / status floor recover
                self.stats.dropped_full.fetch_add(1, Relaxed);
            }
        }
        did
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use uc2_log::buffer::{Appender, LogBuffer, SliceRead};
    use uc2_log::counters::LogCounters;
    use uc2_log::region::Region;
    use uc_protocol::v2::datagram::{
        read_nak_body, read_status_body, write_datagram_header, write_nak_body,
        write_status_body, DatagramHeader, DATAGRAM_HEADER_LEN, DGRAM_KIND_APPEND_POSITION,
        DGRAM_KIND_COMMIT_POSITION, DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT, DGRAM_KIND_NAK,
        DGRAM_KIND_STATUS, NAK_BODY_LEN, STATUS_BODY_LEN,
    };

    const TERM: u32 = 9;

    fn buffer() -> Arc<LogBuffer> {
        let counters = Arc::new(LogCounters::new());
        Arc::new(LogBuffer::new(Region::heap_zeroed(1 << 16), counters, 256))
    }

    fn term_handle(t: u32) -> TermHandle {
        Arc::new(std::sync::atomic::AtomicU32::new(t))
    }

    /// A fake leader endpoint: a raw socket we send DATA from and receive
    /// NAK/status on.
    struct FakeLeader {
        sock: FaultSocket,
    }
    impl FakeLeader {
        fn new() -> Self {
            Self { sock: FaultSocket::bind("127.0.0.1:0").unwrap() }
        }
        fn addr(&self) -> SocketAddr {
            self.sock.local_addr().unwrap()
        }
        fn send(&mut self, to: SocketAddr, kind: u8, position: u64, term: u32, body: &[u8]) {
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
            write_datagram_header(
                &mut d,
                &DatagramHeader { position, leadership_term_id: term, kind, flags: 0 },
            );
            d.extend_from_slice(body);
            self.sock.send_to(&d, to).unwrap();
        }
        fn recv(&self) -> Option<(DatagramHeader, Vec<u8>)> {
            let mut buf = [0u8; 2048];
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if let Some((n, _)) = self.sock.recv_from(&mut buf).unwrap() {
                    return Some((
                        read_datagram_header(&buf),
                        buf[DATAGRAM_HEADER_LEN..n].to_vec(),
                    ));
                }
                std::thread::yield_now();
            }
            None
        }
    }

    /// Frames as a leader buffer would produce them (via the real appender +
    /// run read — keeps wire bytes honest).
    fn frame_runs(payloads: &[&[u8]], chunk: usize) -> Vec<(u64, Vec<u8>, u64)> {
        let b = buffer();
        let mut a = Appender::new(Arc::clone(&b), TERM);
        for (i, p) in payloads.iter().enumerate() {
            a.append(4, i as u64, p).unwrap();
        }
        let mut runs = Vec::new();
        let mut pos = 0u64;
        let mut out = Vec::new();
        while let SliceRead::Run(r) = b.read_run_validated(pos, chunk, &mut out) {
            runs.push((pos, out[..r.bytes].to_vec(), r.advance));
            pos += r.advance;
        }
        runs
    }

    fn follower(b: &Arc<LogBuffer>, leader: SocketAddr) -> FollowerReceiver {
        let mut cfg = FollowerConfig::new(leader);
        cfg.nak = NakConfig { delay_min_ns: 1, delay_max_ns: 2, backoff_ns: 1_000_000 };
        cfg.status_floor_ns = u64::MAX; // no time-driven status in unit tests
        cfg.append_pos_floor_ns = u64::MAX; // advance-driven AppendPosition only
        FollowerReceiver::new(
            Arc::clone(b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            cfg,
            term_handle(TERM),
        )
    }

    fn drive_until<F: Fn() -> bool>(r: &mut FollowerReceiver, pred: F) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pred() {
            assert!(Instant::now() < deadline, "condition never reached");
            r.do_work();
            std::thread::yield_now();
        }
    }

    #[test]
    fn in_order_data_lands_and_advances_append() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let runs = frame_runs(&[b"aaaa", b"bb", b"cccccc"], 4096);
        assert_eq!(runs.len(), 1);
        let (pos, bytes, advance) = &runs[0];
        leader.send(to, DGRAM_KIND_DATA, *pos, TERM, bytes);
        drive_until(&mut r, || b.counters().append.load_acquire() == *advance);
        let s = b.recordable_slice(0, 1 << 20);
        assert_eq!(s.len(), *advance as usize);
        assert_eq!(&s[32..36], b"aaaa");
    }

    #[test]
    fn gap_naks_then_fill_converges() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let runs = frame_runs(&[&[1u8; 64], &[2u8; 64], &[3u8; 64]], 96); // one frame per run
        // deliver run 2, skip runs 0 and 1 -> gap [0, 192)
        leader.send(to, DGRAM_KIND_DATA, runs[2].0, TERM, &runs[2].1);
        // NAK must arrive, asking from the contiguous frontier (0)
        let mut got_nak = None;
        let deadline = Instant::now() + Duration::from_secs(5);
        while got_nak.is_none() {
            assert!(Instant::now() < deadline);
            r.do_work();
            if let Some((h, body)) = leader.recv()
                && h.kind == DGRAM_KIND_NAK
            {
                got_nak = Some(read_nak_body(&body));
            }
        }
        let nak = got_nak.unwrap();
        assert_eq!(nak.position, 0);
        assert_eq!(nak.length as u64, 192);
        // serve the retransmission -> converges, ooo run absorbed
        leader.send(to, DGRAM_KIND_DATA, runs[0].0, TERM, &runs[0].1);
        leader.send(to, DGRAM_KIND_DATA, runs[1].0, TERM, &runs[1].1);
        drive_until(&mut r, || b.counters().append.load_acquire() == 3 * 96);
        assert!(r.stats().naks_sent.load(std::sync::atomic::Ordering::Relaxed) >= 1);
    }

    #[test]
    fn heartbeat_reveals_tail_loss() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        // nothing delivered; heartbeat says the leader is at 192
        leader.send(to, DGRAM_KIND_HEARTBEAT, 192, TERM, &[]);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(Instant::now() < deadline, "no tail NAK");
            r.do_work();
            if let Some((h, body)) = leader.recv()
                && h.kind == DGRAM_KIND_NAK
            {
                let nak = read_nak_body(&body);
                assert_eq!((nak.position, nak.length), (0, 192));
                break;
            }
        }
    }

    #[test]
    fn drops_stale_term_dups_and_malformed() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let runs = frame_runs(&[&[1u8; 64], &[2u8; 64]], 4096);
        let (pos, bytes, advance) = &runs[0];
        leader.send(to, DGRAM_KIND_DATA, *pos, TERM - 1, bytes); // stale term
        leader.send(to, DGRAM_KIND_DATA, *pos, TERM, &bytes[..16]); // malformed (torn frame)
        leader.send(to, DGRAM_KIND_DATA, *pos, TERM, bytes); // good
        drive_until(&mut r, || b.counters().append.load_acquire() == *advance);
        leader.send(to, DGRAM_KIND_DATA, *pos, TERM, bytes); // full dup
        // the dup arrives asynchronously: wait for it to be counted, then
        // assert the log did not move
        let st = r.stats();
        use std::sync::atomic::Ordering::Relaxed;
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_dup.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "dup never observed");
            r.do_work();
        }
        assert_eq!(b.counters().append.load_acquire(), *advance);
        assert_eq!(st.dropped_stale_term.load(Relaxed), 1);
        assert_eq!(st.dropped_malformed.load(Relaxed), 1);
        assert_eq!(st.dropped_dup.load(Relaxed), 1);
    }

    #[test]
    fn status_advertises_contiguous_and_window() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut cfg = FollowerConfig::new(leader.addr());
        cfg.status_bytes = 96; // status on every frame's worth of progress
        cfg.status_floor_ns = u64::MAX;
        let mut r = FollowerReceiver::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            cfg,
            term_handle(TERM),
        );
        let to = r.local_addr();
        let runs = frame_runs(&[&[7u8; 64]], 4096);
        leader.send(to, DGRAM_KIND_DATA, runs[0].0, TERM, &runs[0].1);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(Instant::now() < deadline, "no status");
            r.do_work();
            if let Some((h, body)) = leader.recv()
                && h.kind == DGRAM_KIND_STATUS
            {
                let s = read_status_body(&body);
                assert_eq!(s.contiguous_position, 96);
                // durable 0 + capacity 65536 - contiguous 96
                assert_eq!(s.receive_window, 65536 - 96);
                break;
            }
        }
    }

    #[test]
    fn walk_advance_handles_messages_padding_and_garbage() {
        // real frames via the appender for honesty
        let runs = frame_runs(&[&[1u8; 64], &[2u8; 32]], 4096);
        let bytes = &runs[0].1;
        assert_eq!(walk_advance(bytes), Some(runs[0].2));
        assert_eq!(walk_advance(&bytes[..16]), None); // torn header
        assert_eq!(walk_advance(&[0u8; 32]), None); // zero length
        // padding-only run from a wrapping buffer
        let b = buffer();
        let c = Arc::clone(b.counters());
        let mut a = Appender::new(Arc::clone(&b), TERM);
        let per = 96u64;
        let cap = b.capacity();
        let fill = (cap / per) as usize; // 682 frames -> 65472, 64 short of the wrap
        for i in 0..fill {
            a.append(4, i as u64, &[0u8; 64]).unwrap();
            c.durable.store_release(a.position());
        }
        let pad_pos = a.position();
        a.append(4, 999, &[0u8; 64]).unwrap(); // forces padding
        let mut out = Vec::new();
        if let SliceRead::Run(r) = b.read_run_validated(pad_pos, 1392, &mut out) {
            assert!(r.advance > r.bytes as u64); // padding tail
            assert_eq!(walk_advance(&out[..r.bytes]), Some(r.advance));
        } else {
            panic!("expected padding run");
        }
    }

    /// F1 regression: a wrap-padding datagram whose ADVANCE (not wire bytes)
    /// violates `durable + capacity` is dropped as overrun, the frontier does
    /// not move, and the advertised receive window does not underflow.
    ///
    /// Pre-fix, the DATA path delegated the overrun check to
    /// `write_run`, which gates on `position + bytes.len()`. For the lap-2
    /// padding `bytes.len() == 32` but `advance == 64`: write_run's guard
    /// (`8128 + 32 == 8160`, not `> 8160`) PASSED, so the frontier advanced to
    /// 8192 and the status window computed `4064 + 4096 - 8192` which underflows
    /// `u32` to ~4.29e9 — a bogus huge window that defeats flow control. The
    /// advance-based guard added here rejects it (`8128 + 64 == 8192 > 8160`).
    #[test]
    fn padding_advance_overrun_dropped_window_no_underflow() {
        use std::sync::atomic::Ordering::Relaxed;
        // Dedicated small buffer: 4096 is a power of two and >= 4*max_claim
        // (max_payload 256 -> max_claim 576 -> 2304). 96-byte frames lap it in
        // 42 frames + a 64-byte wrap padding.
        fn small() -> Arc<LogBuffer> {
            let counters = Arc::new(LogCounters::new());
            Arc::new(LogBuffer::new(Region::heap_zeroed(4096), counters, 256))
        }

        // Two honest laps of leader wire runs. The buffer is only 4096 B, so
        // lap 2 overwrites lap 1's offsets — capture each run right after its
        // append, before it is clobbered. Glue durable to append so it laps.
        let leader = small();
        let mut a = Appender::new(Arc::clone(&leader), TERM);
        let mut runs: Vec<(u64, Vec<u8>, u64)> = Vec::new();
        let mut read_pos = 0u64;
        let mut out = Vec::new();
        for i in 0..85u64 {
            a.append(4, i, &[0u8; 64]).unwrap();
            leader.counters().durable.store_release(a.position());
            while let SliceRead::Run(r) = leader.read_run_validated(read_pos, 96, &mut out) {
                runs.push((read_pos, out[..r.bytes].to_vec(), r.advance));
                read_pos += r.advance;
            }
        }
        // Partition: lap1 (42 frames + padding) < 4096; lap2 frames
        // [4096,8128); lap2 padding at 8128; ignore the frame at 8192 that
        // forced the padding.
        let lap1: Vec<_> = runs.iter().filter(|(p, ..)| *p < 4096).cloned().collect();
        let lap2_frames: Vec<_> =
            runs.iter().filter(|(p, ..)| (4096..8128).contains(p)).cloned().collect();
        let pad2 = runs.iter().find(|(p, ..)| *p == 8128).cloned().expect("lap2 padding");
        // Honest-generation sanity.
        assert_eq!(lap1.len(), 43);
        assert_eq!(lap1.last().unwrap().0, 4032); // padding at 4032
        assert_eq!(lap1.last().unwrap().1.len(), 32); // header-only on the wire
        assert_eq!(lap1.last().unwrap().2, 64); // advances the full pad span
        assert_eq!(lap2_frames.len(), 42);
        assert_eq!((pad2.0, pad2.1.len(), pad2.2), (8128, 32, 64));

        // Follower on its own dedicated small buffer.
        let fb = small();
        let mut leader_ep = FakeLeader::new();
        let mut cfg = FollowerConfig::new(leader_ep.addr());
        cfg.nak = NakConfig { delay_min_ns: 1, delay_max_ns: 2, backoff_ns: 1_000_000 };
        cfg.status_floor_ns = u64::MAX;
        cfg.status_bytes = 96; // a status per frame's worth of progress
        let mut r = FollowerReceiver::new(
            Arc::clone(&fb),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            cfg,
            term_handle(TERM),
        );
        let to = r.local_addr();

        // (3) lap 1: [0,4096) rebuilt; ends exactly at durable(0)+capacity —
        // allowed, the boundary check is strict `>`.
        for (pos, bytes, _) in &lap1 {
            leader_ep.send(to, DGRAM_KIND_DATA, *pos, TERM, bytes);
        }
        drive_until(&mut r, || fb.counters().append.load_acquire() == 4096);

        // (4) the archive advances durable (simulating the local recorder).
        fb.counters().durable.store_release(4064);

        // (5) lap 2 frames [4096,8128): each ends <= 8160 = durable+capacity.
        for (pos, bytes, _) in &lap2_frames {
            leader_ep.send(to, DGRAM_KIND_DATA, *pos, TERM, bytes);
        }
        drive_until(&mut r, || fb.counters().append.load_acquire() == 8128);

        // (6) lap 2 padding: advance 64 pushes 8128 -> 8192 > 8160 -> dropped.
        leader_ep.send(to, DGRAM_KIND_DATA, pad2.0, TERM, &pad2.1);
        let deadline = Instant::now() + Duration::from_secs(5);
        while r.stats().dropped_overrun.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "padding overrun never counted");
            r.do_work();
            std::thread::yield_now();
        }
        // frontier did NOT move past the honest lap-2 tail
        assert_eq!(fb.counters().append.load_acquire(), 8128);

        // (7) a status reflecting the final state advertises a SANE window
        // (4064 + 4096 - 8128 = 32), not a ~4 GiB underflow.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(Instant::now() < deadline, "no final status");
            r.do_work();
            if let Some((h, body)) = leader_ep.recv()
                && h.kind == DGRAM_KIND_STATUS
            {
                let s = read_status_body(&body);
                if s.contiguous_position == 8128 {
                    assert_eq!(s.receive_window, 32);
                    break;
                }
            }
        }
    }

    /// F2 regression: an empty-body DATA datagram is malformed, not overrun.
    #[test]
    fn empty_body_data_is_malformed_not_overrun() {
        use std::sync::atomic::Ordering::Relaxed;
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        leader.send(to, DGRAM_KIND_DATA, 0, TERM, &[]); // empty body
        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_malformed.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "empty body never counted malformed");
            r.do_work();
            std::thread::yield_now();
        }
        assert_eq!(st.dropped_overrun.load(Relaxed), 0);
        assert_eq!(b.counters().append.load_acquire(), 0); // frontier unmoved
    }

    #[test]
    fn misaligned_wire_position_is_malformed() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let runs = frame_runs(&[&[1u8; 64]], 4096);
        // legit frame bytes, but position not on a 32-byte frame boundary
        leader.send(to, DGRAM_KIND_DATA, 16, TERM, &runs[0].1);
        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_malformed.load(std::sync::atomic::Ordering::Relaxed) < 1 {
            assert!(Instant::now() < deadline, "misaligned datagram never observed");
            r.do_work();
        }
        assert_eq!(b.counters().append.load_acquire(), 0, "misaligned position advanced the log");
    }

    #[test]
    fn position_overflow_is_malformed_not_accepted() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let runs = frame_runs(&[&[1u8; 64]], 4096);
        // u64 wrap: position + advance overflows; the wrapped sum must not
        // sneak past the overrun gate (accept-rule arithmetic escape)
        let pos = u64::MAX - 63; // 32-aligned (u64::MAX - 63 = ...FFC0), advance 96 wraps
        assert_eq!(pos % 32, 0);
        leader.send(to, DGRAM_KIND_DATA, pos, TERM, &runs[0].1);
        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_malformed.load(std::sync::atomic::Ordering::Relaxed) < 1 {
            assert!(Instant::now() < deadline, "overflowing datagram never observed");
            r.do_work();
        }
        assert_eq!(b.counters().append.load_acquire(), 0);
    }

    #[test]
    fn durable_advance_emits_append_position() {
        let b = buffer();
        let leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        // simulate the archive: durable advances by one block
        b.counters().durable.store_release(960);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(Instant::now() < deadline, "no AppendPosition");
            r.do_work();
            if let Some((h, body)) = leader.recv()
                && h.kind == DGRAM_KIND_APPEND_POSITION
            {
                assert_eq!(h.position, 960);
                assert_eq!(h.leadership_term_id, TERM);
                assert!(body.is_empty(), "AppendPosition is header-only");
                break;
            }
        }
        // capture the baseline IMMEDIATELY after the first send, then assert
        // it holds across 50 quiescent cycles: durable unchanged + floor
        // disabled (u64::MAX in the helper) must mean NO re-send — a bug that
        // forgets to update ap_reported re-sends every cycle and fails here
        let sent = r.stats().append_positions_sent.load(std::sync::atomic::Ordering::Relaxed);
        for _ in 0..50 {
            r.do_work();
        }
        assert_eq!(
            r.stats().append_positions_sent.load(std::sync::atomic::Ordering::Relaxed),
            sent,
            "re-sent AppendPosition without a durable advance"
        );
        b.counters().durable.store_release(1920); // next block
        let deadline = Instant::now() + Duration::from_secs(5);
        while r.stats().append_positions_sent.load(std::sync::atomic::Ordering::Relaxed) == sent {
            assert!(Instant::now() < deadline, "advance did not re-report");
            r.do_work();
        }
    }

    #[test]
    fn commit_position_gossip_is_stored_monotonically() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        leader.send(to, DGRAM_KIND_COMMIT_POSITION, 4096, TERM, &[]);
        let deadline = Instant::now() + Duration::from_secs(5);
        while b.counters().commit.load_acquire() < 4096 {
            assert!(Instant::now() < deadline, "commit gossip never landed");
            r.do_work();
        }
        // stale/reordered gossip must not regress; stale TERM must be dropped
        leader.send(to, DGRAM_KIND_COMMIT_POSITION, 1024, TERM, &[]);
        leader.send(to, DGRAM_KIND_COMMIT_POSITION, 9999, TERM - 1, &[]);
        let st = r.stats();
        let before_stale = st.dropped_stale_term.load(std::sync::atomic::Ordering::Relaxed);
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_stale_term.load(std::sync::atomic::Ordering::Relaxed) == before_stale {
            assert!(Instant::now() < deadline, "stale-term gossip never observed");
            r.do_work();
        }
        assert_eq!(b.counters().commit.load_acquire(), 4096);
        assert!(st.commits_received.load(std::sync::atomic::Ordering::Relaxed) >= 2);
    }

    #[test]
    fn leader_receiver_demuxes_control_to_sender_channel() {
        let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = sock.local_addr().unwrap();
        let (tx, rx) = mpsc::sync_channel(16);
        let mut lr = LeaderReceiver::new(sock, tx, term_handle(TERM)).unwrap();
        let mut f = FakeLeader::new(); // reuse as a fake follower endpoint
        let mut nb = [0u8; NAK_BODY_LEN];
        write_nak_body(&mut nb, &uc_protocol::v2::datagram::NakBody { position: 96, length: 192 });
        f.send(addr, DGRAM_KIND_NAK, 0, TERM, &nb);
        let mut sb = [0u8; STATUS_BODY_LEN];
        write_status_body(
            &mut sb,
            &uc_protocol::v2::datagram::StatusBody { contiguous_position: 4096, receive_window: 1 << 20 },
        );
        f.send(addr, DGRAM_KIND_STATUS, 0, TERM, &sb);
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut got = Vec::new();
        while got.len() < 2 {
            assert!(Instant::now() < deadline);
            lr.do_work();
            while let Ok(m) = rx.try_recv() {
                got.push(m);
            }
        }
        assert!(matches!(got[0], CtrlMsg::Nak { position: 96, length: 192, .. }));
        assert!(matches!(got[1], CtrlMsg::Status { contiguous: 4096, .. }));
    }

    #[test]
    fn leader_receiver_demuxes_append_position_and_drops_stale_term() {
        let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = sock.local_addr().unwrap();
        let (tx, rx) = mpsc::sync_channel(16);
        let mut lr = LeaderReceiver::new(sock, tx, term_handle(TERM)).unwrap();
        let lr_stats = lr.stats(); // capture the handle before any move
        let mut f = FakeLeader::new(); // reuse as a fake follower endpoint
        f.send(addr, DGRAM_KIND_APPEND_POSITION, 4096, TERM, &[]);
        f.send(addr, DGRAM_KIND_APPEND_POSITION, 9999, TERM - 1, &[]); // stale term
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut got = None;
        while got.is_none() {
            assert!(Instant::now() < deadline);
            lr.do_work();
            if let Ok(m) = rx.try_recv() {
                got = Some(m);
            }
        }
        assert!(matches!(got, Some(CtrlMsg::AppendPos { durable: 4096, .. })));
        // the stale one must be counted dropped, never demuxed
        use std::sync::atomic::Ordering::Relaxed;
        let deadline = Instant::now() + Duration::from_secs(5);
        while lr_stats.dropped_stale_term.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "stale-term control never observed");
            lr.do_work();
        }
        assert!(rx.try_recv().is_err(), "stale-term control reached the sender");
    }

    // ----------------------------------------------------------- M4 (Task 7)

    use std::sync::atomic::AtomicBool;
    use uc_protocol::v2::datagram::{
        write_request_vote_body, DGRAM_KIND_REQUEST_VOTE, NakBody, REQUEST_VOTE_BODY_LEN,
        RequestVoteBody,
    };

    /// A live term bump takes effect on the next datagram: DATA at the OLD term
    /// is dropped `dropped_stale_term` once the handle has moved forward.
    #[test]
    fn term_bump_drops_old_term_data() {
        use std::sync::atomic::Ordering::Relaxed;
        let b = buffer();
        let mut leader = FakeLeader::new();
        let handle = term_handle(TERM);
        let mut cfg = FollowerConfig::new(leader.addr());
        cfg.nak = NakConfig { delay_min_ns: 1, delay_max_ns: 2, backoff_ns: 1_000_000 };
        cfg.status_floor_ns = u64::MAX;
        cfg.append_pos_floor_ns = u64::MAX;
        let mut r = FollowerReceiver::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            cfg,
            Arc::clone(&handle),
        );
        let to = r.local_addr();
        let runs = frame_runs(&[&[1u8; 64], &[2u8; 64]], 96); // one frame per run
        // term TERM accepted while the handle reads TERM
        leader.send(to, DGRAM_KIND_DATA, runs[0].0, TERM, &runs[0].1);
        drive_until(&mut r, || b.counters().append.load_acquire() == runs[0].2);
        // consensus agent bumps the term; the same-term stream is now stale
        handle.store(TERM + 1, Relaxed);
        leader.send(to, DGRAM_KIND_DATA, runs[1].0, TERM, &runs[1].1);
        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_stale_term.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "old-term DATA never dropped after bump");
            r.do_work();
        }
        assert_eq!(b.counters().append.load_acquire(), runs[0].2, "stale DATA advanced the log");
    }

    /// Node mode: a HIGHER-term RequestVote (kind 7) reaches the consensus route
    /// RAW (not dropped by the data-plane term filter), and COMMIT_POSITION is
    /// routed as `CommitGossip` instead of being stored in the local counter.
    #[test]
    fn node_mode_routes_consensus_raw_and_disables_commit_store() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let (tx, rx) = mpsc::sync_channel::<NetEvent>(16);
        r.set_consensus_route(tx);
        let to = r.local_addr();

        // higher-term RequestVote must NOT be term-filtered
        let mut rvb = vec![0u8; REQUEST_VOTE_BODY_LEN];
        write_request_vote_body(
            &mut rvb,
            &RequestVoteBody { new_term: TERM + 5, last_term: TERM, last_durable: 320 },
        );
        leader.send(to, DGRAM_KIND_REQUEST_VOTE, 0, TERM + 5, &rvb);
        // commit gossip at the current term
        leader.send(to, DGRAM_KIND_COMMIT_POSITION, 4096, TERM, &[]);

        let mut saw_vote = false;
        let mut saw_gossip = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while !(saw_vote && saw_gossip) {
            assert!(Instant::now() < deadline, "consensus events never routed");
            r.do_work();
            while let Ok(ev) = rx.try_recv() {
                match ev {
                    NetEvent::RequestVote { body, .. } => {
                        assert_eq!(body.new_term, TERM + 5);
                        saw_vote = true;
                    }
                    NetEvent::CommitGossip { term, commit } => {
                        assert_eq!((term, commit), (TERM, 4096));
                        saw_gossip = true;
                    }
                    _ => {}
                }
            }
        }
        // the local commit counter is NOT written in node mode
        assert_eq!(b.counters().commit.load_acquire(), 0, "node mode still stored commit locally");
    }

    /// The intake gate CLOSED drops DATA (`dropped_gated`) and suppresses
    /// AppendPosition ENTIRELY; reopening it resumes both.
    #[test]
    fn intake_gate_closed_drops_data_and_suppresses_append_position() {
        use std::sync::atomic::Ordering::Relaxed;
        let b = buffer();
        let mut leader = FakeLeader::new();
        let handle = term_handle(TERM);
        let mut cfg = FollowerConfig::new(leader.addr());
        cfg.nak = NakConfig { delay_min_ns: 1, delay_max_ns: 2, backoff_ns: 1_000_000 };
        cfg.status_floor_ns = u64::MAX;
        cfg.append_pos_floor_ns = u64::MAX; // advance-driven AppendPosition only
        let mut r = FollowerReceiver::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            cfg,
            handle,
        );
        let gate = Arc::new(AtomicBool::new(false)); // CLOSED
        r.set_intake_gate(Arc::clone(&gate));
        let to = r.local_addr();
        let runs = frame_runs(&[&[1u8; 64]], 4096);

        // gate closed: DATA dropped, frontier unmoved
        b.counters().durable.store_release(960); // a durable advance to report
        leader.send(to, DGRAM_KIND_DATA, runs[0].0, TERM, &runs[0].1);
        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_gated.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "gated DATA never counted");
            r.do_work();
        }
        for _ in 0..50 {
            r.do_work();
        }
        assert_eq!(b.counters().append.load_acquire(), 0, "gated DATA advanced the log");
        assert_eq!(
            st.append_positions_sent.load(Relaxed),
            0,
            "AppendPosition escaped while the gate was closed"
        );

        // reopen: DATA accepted AND AppendPosition now flows
        gate.store(true, Relaxed);
        leader.send(to, DGRAM_KIND_DATA, runs[0].0, TERM, &runs[0].1);
        drive_until(&mut r, || b.counters().append.load_acquire() == runs[0].2);
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.append_positions_sent.load(Relaxed) == 0 {
            assert!(Instant::now() < deadline, "AppendPosition never resumed after reopen");
            r.do_work();
        }
    }

    /// Leader-role node composition (M4): the unified `FollowerReceiver` with a
    /// sender route installed demuxes inbound NAK/STATUS to the sender's control
    /// channel (leader retransmit + flow pacing), while consensus kinds still
    /// route to the consensus channel. A follower never receives NAK/STATUS, so
    /// the route is dormant in that role.
    #[test]
    fn sender_route_demuxes_nak_and_status() {
        let b = buffer();
        let mut peer = FakeLeader::new(); // stands in for a follower endpoint
        let mut r = follower(&b, peer.addr());
        let (tx, rx) = mpsc::sync_channel::<CtrlMsg>(16);
        r.set_sender_route(tx);
        let to = r.local_addr();

        let mut nb = [0u8; NAK_BODY_LEN];
        write_nak_body(&mut nb, &NakBody { position: 96, length: 192 });
        peer.send(to, DGRAM_KIND_NAK, 0, TERM, &nb);
        let mut sb = [0u8; STATUS_BODY_LEN];
        write_status_body(
            &mut sb,
            &StatusBody { contiguous_position: 4096, receive_window: 1 << 20 },
        );
        peer.send(to, DGRAM_KIND_STATUS, 0, TERM, &sb);
        // a stale-term NAK must be dropped, never demuxed
        peer.send(to, DGRAM_KIND_NAK, 0, TERM - 1, &nb);

        let mut nak = None;
        let mut status = None;
        let deadline = Instant::now() + Duration::from_secs(5);
        while nak.is_none() || status.is_none() {
            assert!(Instant::now() < deadline, "control never demuxed to the sender");
            r.do_work();
            while let Ok(m) = rx.try_recv() {
                match m {
                    CtrlMsg::Nak { .. } => nak = Some(m),
                    CtrlMsg::Status { .. } => status = Some(m),
                    _ => {}
                }
            }
        }
        assert!(matches!(nak, Some(CtrlMsg::Nak { position: 96, length: 192, .. })));
        assert!(matches!(status, Some(CtrlMsg::Status { contiguous: 4096, .. })));
        use std::sync::atomic::Ordering::Relaxed;
        let deadline = Instant::now() + Duration::from_secs(5);
        while r.stats().dropped_stale_term.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "stale-term NAK never observed");
            r.do_work();
        }
        assert!(rx.try_recv().is_err(), "stale-term control leaked to the sender");
    }

    /// Leader-side node mode: AppendPosition (kind 5) routes to consensus as a
    /// `Report` (raw), while NAK still reaches the sender's channel.
    #[test]
    fn leader_receiver_node_mode_routes_append_position_as_report() {
        let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = sock.local_addr().unwrap();
        let (tx_sender, rx_sender) = mpsc::sync_channel::<CtrlMsg>(16);
        let (tx_cons, rx_cons) = mpsc::sync_channel::<NetEvent>(16);
        let mut lr = LeaderReceiver::new(sock, tx_sender, term_handle(TERM)).unwrap();
        lr.set_consensus_route(tx_cons);
        let mut f = FakeLeader::new(); // fake follower endpoint
        // AppendPosition -> consensus Report (even at a higher term: raw)
        f.send(addr, DGRAM_KIND_APPEND_POSITION, 2048, TERM + 3, &[]);
        // NAK -> sender channel (data-plane control)
        let mut nb = [0u8; NAK_BODY_LEN];
        write_nak_body(&mut nb, &NakBody { position: 96, length: 192 });
        f.send(addr, DGRAM_KIND_NAK, 0, TERM, &nb);

        let mut report = None;
        let mut nak = None;
        let deadline = Instant::now() + Duration::from_secs(5);
        while report.is_none() || nak.is_none() {
            assert!(Instant::now() < deadline, "leader node-mode demux stuck");
            lr.do_work();
            if let Ok(ev) = rx_cons.try_recv() {
                report = Some(ev);
            }
            if let Ok(m) = rx_sender.try_recv() {
                nak = Some(m);
            }
        }
        assert!(matches!(report, Some(NetEvent::Report { term, durable: 2048, .. }) if term == TERM + 3));
        assert!(matches!(nak, Some(CtrlMsg::Nak { position: 96, length: 192, .. })));
    }
}
