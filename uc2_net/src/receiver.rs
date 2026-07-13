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

use std::io::{Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Instant;

use uc2_log::buffer::LogBuffer;
use uc2_log::writer::PositionedWriter;
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_APPEND_POSITION, DGRAM_KIND_COMMIT_POSITION, DGRAM_KIND_DATA,
    DGRAM_KIND_HEARTBEAT, DGRAM_KIND_NAK, DGRAM_KIND_READ_PROBE, DGRAM_KIND_READ_PROBE_ACK,
    DGRAM_KIND_REQUEST_VOTE, DGRAM_KIND_SNAP_BEGIN, DGRAM_KIND_SNAP_CHUNK, DGRAM_KIND_SNAP_DONE,
    DGRAM_KIND_SNAP_NAK, DGRAM_KIND_STATUS, DGRAM_KIND_TERM_MAP, DGRAM_KIND_VOTE, DatagramHeader,
    MAX_TERM_MAP_WIRE_ENTRIES, NAK_BODY_LEN, NakBody, REQUEST_VOTE_BODY_LEN, RequestVoteBody,
    SNAP_BEGIN_FIXED_LEN, SNAP_NAK_BODY_LEN, STATUS_BODY_LEN, SnapBeginBody, SnapNakBody, StatusBody,
    TermMapEntryWire, VOTE_BODY_LEN, VoteBody, read_datagram_header, read_nak_body,
    read_read_probe_body, read_request_vote_body, read_snap_begin_body, read_snap_nak_body,
    read_status_body, read_term_map_body, read_vote_body, write_datagram_header, write_nak_body,
    write_snap_begin_body, write_snap_nak_body, write_status_body,
};
use uc_protocol::v2::frame::{self, FRAME_TYPE_PADDING, HEADER_LEN, align_frame_len};

use crate::TermHandle;
use crate::fault::FaultSocket;
use crate::rebuild::{NakConfig, NakTimer, Rebuilt};
use crate::sender::CtrlMsg;

/// Consensus-plane events demuxed off the shared UDP socket and routed to the
/// consensus agent (Task 8) over the [`FollowerReceiver::new`] constructor's
/// mandatory route. Kinds 5–11 forward RAW — carrying their own term so the
/// state machine, not the data plane, does term filtering and adoption (a
/// higher-term `RequestVote` MUST reach the SM). `LeaderActivity` is the data
/// plane's rate-limited liveness signal: current-term DATA/HEARTBEAT was seen
/// this duty cycle, so the SM should not time out the leader.
#[derive(Debug, Clone)]
pub enum NetEvent {
    Report { from: SocketAddr, term: u32, durable: u64 },
    CommitGossip { from: SocketAddr, term: u32, commit: u64 },
    RequestVote { from: SocketAddr, body: RequestVoteBody },
    Vote { from: SocketAddr, body: VoteBody },
    TermMap { from: SocketAddr, term: u32, entries: Vec<TermMapEntryWire> },
    /// Read-barrier probe (M5 §7), leader → follower. `from` is the leader's
    /// node id (from the body — the reply is addressed by it), `term` is the
    /// datagram HEADER term the follower must match to ACK (the stale-leader
    /// filter). Routed RAW like the other consensus kinds.
    ReadProbe { nonce: u64, from: u32, term: u32 },
    /// Read-barrier ack, follower → leader. `from` is the acking follower's node
    /// id (from the body); the leader counts distinct ackers per nonce. No term
    /// field: the nonce is unique to one leader's read round, so a matching
    /// pending nonce already scopes the ack to this leader.
    ReadProbeAck { nonce: u64, from: u32 },
    /// Any current-term leader traffic (data/heartbeat) seen — liveness.
    LeaderActivity { term: u32 },
}

impl NetEvent {
    /// Dense index for the per-kind drop counters (`FollowerStats::net_drops`).
    /// Stable ordering — the array is read out positionally by observability.
    #[inline]
    pub fn kind_idx(&self) -> usize {
        match self {
            NetEvent::Report { .. } => 0,
            NetEvent::CommitGossip { .. } => 1,
            NetEvent::RequestVote { .. } => 2,
            NetEvent::Vote { .. } => 3,
            NetEvent::TermMap { .. } => 4,
            NetEvent::LeaderActivity { .. } => 5,
            NetEvent::ReadProbe { .. } => 6,
            NetEvent::ReadProbeAck { .. } => 7,
        }
    }
}

/// Number of [`NetEvent`] kinds (the width of the per-kind drop counters).
pub const NET_EVENT_KINDS: usize = 8;

/// Parse a consensus-plane datagram (kinds 5–11) into a [`NetEvent`], RAW — no
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
            Some(NetEvent::CommitGossip { from, term: h.leadership_term_id, commit: h.position })
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
                from,
                term: h.leadership_term_id,
                entries: out[..count].to_vec(),
            })
        }
        DGRAM_KIND_READ_PROBE => {
            // Carry the datagram HEADER term: the follower ACKs only if it still
            // equals its own current term (the node-side stale-leader filter).
            let b = read_read_probe_body(body)?;
            Some(NetEvent::ReadProbe { nonce: b.nonce, from: b.from, term: h.leadership_term_id })
        }
        DGRAM_KIND_READ_PROBE_ACK => {
            let b = read_read_probe_body(body)?;
            Some(NetEvent::ReadProbeAck { nonce: b.nonce, from: b.from })
        }
        _ => None,
    }
}

/// True iff `kind` is a consensus-plane datagram (kinds 5–11) — routed RAW to
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
            | DGRAM_KIND_READ_PROBE
            | DGRAM_KIND_READ_PROBE_ACK
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
    /// Consensus events (kinds 5–11) dropped because the consensus route channel
    /// was FULL (M4 node composition — the T7 observability concern), counted
    /// PER KIND (indexed by [`NetEvent::kind_idx`]) so a wedge is attributable to
    /// the specific traffic class starving. Safe: votes re-fire on the election
    /// timeout, reports/gossip on their floors. Surfaced so a wedged consensus
    /// agent is diagnosable rather than silent.
    pub net_drops: [AtomicU64; NET_EVENT_KINDS],
    pub naks_sent: AtomicU64,
    pub statuses_sent: AtomicU64,
    pub append_positions_sent: AtomicU64,
    /// Counter-regress resyncs (M4): times the receiver rebuilt its `rebuilt` gap
    /// tracker after the archive's `LogCounters::prime(to)` regressed the shared
    /// `append` counter below the tracker's frontier. Fires on a reconciliation
    /// truncation AND on a `BecomeLeader` collapse-to-base prime (both drive the
    /// counter backward); the resync is the correct recovery for either. See
    /// [`FollowerReceiver::resync_after_truncation`].
    pub truncation_resyncs: AtomicU64,
    /// Straddle drops (M6 Task 9): times a DATA datagram was discarded because a
    /// `LogCounters::prime(to)` re-primed the shared `append` counter DURING this
    /// datagram's processing (between the frontier read and the `store_release`).
    /// Storing the stale `rebuilt.contiguous()` would clobber the freshly primed
    /// floor with a value from the prior stream life. The drop is safe — the next
    /// `do_work` top resyncs the tracker to the primed floor and NAKs forward. See
    /// the generation recheck in the DATA arm.
    pub dropped_straddle: AtomicU64,
}

/// M6 Task 6: one in-flight INBOUND snapshot transfer (this node is receiving a
/// snapshot from the leader because its NAK fell below the purge floor). Chunks
/// land at their file offset in a pre-sized `.part`; a `Rebuilt` over the file's
/// byte space tracks contiguity + gaps (NAK'd like the main stream). On
/// completion the `.part` is fsync'd + atomically renamed to the final artifact.
struct SnapIntake {
    peer: SocketAddr,
    session: u32,
    snapshot_pos: u64,
    total_len: u64,
    file: std::fs::File,
    part_path: PathBuf,
    final_path: PathBuf,
    /// Contiguity over `[0, total_len)` file offsets.
    got: Rebuilt,
    nak: NakTimer,
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
    recv_buf: Vec<u8>,
    stats: Arc<FollowerStats>,
    /// Live leadership term (M4). The consensus agent (Task 8) is the sole
    /// writer; the data path only loads it (`Relaxed`) to term-filter DATA/
    /// HEARTBEAT and to stamp its own NAK/STATUS/AppendPosition datagrams.
    term: TermHandle,
    /// Consensus route (M4): kinds 5–11 are forwarded RAW to the consensus
    /// agent (bypassing the term filter — the SM adopts higher terms). The
    /// consensus agent is the sole writer of the commit counter; this receiver
    /// never stores commit locally (M4 carry #5 removed the M3 local
    /// COMMIT_POSITION store entirely).
    route: mpsc::SyncSender<NetEvent>,
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
    /// M6 Task 6: snapshot directory (`instance_dir/snapshots`) for inbound
    /// transfers. `None` = this node never receives snapshots (no intake).
    snap_dir: Option<PathBuf>,
    /// M6 Task 6: the in-flight inbound snapshot transfer, if any.
    snap_intake: Option<SnapIntake>,
    /// M6 Task 6: node-internal signal — the position of the newest COMPLETE
    /// inbound snapshot (written on rename). The consensus agent samples it to
    /// issue `ArchiveCmd::AdoptFloor` and mirror it to cnc. `None` in unit tests.
    incoming_snapshot_pos: Option<Arc<AtomicU64>>,
    /// M6 Task 6: config for the inbound-transfer NAK timer (RTT delay + seed).
    snap_nak_cfg: NakConfig,
    snap_seed: u64,
    /// M6 Task 8: a completed snapshot install awaiting the consensus agent's
    /// `AdoptFloor` FORWARD re-prime. Set to the snapshot position on
    /// `snap_complete`; once the buffer's `append` reaches it (the archive adopted
    /// the floor), the receiver rebuilds its gap tracker FORWARD to the new floor so
    /// it NAKs the retained `[floor, frontier)` tail instead of re-requesting the
    /// purged prefix. Cleared once applied. Distinct from the backward truncation
    /// re-prime, which is unambiguous from the counter alone.
    snap_adopt_pending: Option<u64>,
    /// M6 Task 9 (straddle hardening): a node-internal generation counter bumped
    /// by the archive/consensus agent on every `LogCounters::prime(to)` (truncate,
    /// AdoptFloor, BecomeLeader collapse). The DATA arm samples it before reading
    /// the frontier and rechecks it just before `append.store_release`; a change
    /// means a prime straddled this datagram, so the (now stale) contiguous value
    /// is dropped rather than published. `None` = no primes race this receiver
    /// (single-life unit tests). See [`set_prime_generation`](Self::set_prime_generation).
    prime_gen: Option<Arc<AtomicU64>>,
    /// Test-only hook fired between the frontier read and the generation recheck
    /// in the DATA arm, so a test can deterministically inject a straddling prime.
    #[cfg(test)]
    straddle_hook: Option<Box<dyn Fn() + Send>>,
}

impl FollowerReceiver {
    /// `route` carries consensus datagrams (kinds 5–11, spec §3.1) RAW to the
    /// consensus agent (no term filter — the SM adopts higher terms; a
    /// full/disconnected channel drops harmlessly — votes re-fire on the
    /// election timeout, gossip/reports re-send on their floors). DATA/
    /// HEARTBEAT still drive the data plane directly and additionally emit a
    /// rate-limited [`NetEvent::LeaderActivity`] over the same route.
    pub fn new(
        buffer: Arc<LogBuffer>,
        sock: FaultSocket,
        cfg: FollowerConfig,
        term: TermHandle,
        route: mpsc::SyncSender<NetEvent>,
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
            recv_buf: vec![0u8; 65_536],
            stats: Arc::new(FollowerStats::default()),
            term,
            route,
            sender_route: None,
            gate: None,
            activity_emitted: false,
            snap_dir: None,
            snap_intake: None,
            incoming_snapshot_pos: None,
            snap_nak_cfg: cfg.nak,
            snap_seed: cfg.seed,
            snap_adopt_pending: None,
            prime_gen: None,
            #[cfg(test)]
            straddle_hook: None,
        }
    }

    pub fn stats(&self) -> Arc<FollowerStats> {
        Arc::clone(&self.stats)
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

    /// M6 Task 9: install the prime-generation counter shared with the archive/
    /// consensus agent. The agent bumps it AFTER each `LogCounters::prime(to)`;
    /// the receiver uses it to detect a prime that straddles a DATA datagram's
    /// processing and drop the resulting stale frontier rather than clobber the
    /// freshly primed floor. Without this call the recheck is inert (single-life
    /// receivers never see a competing prime).
    pub fn set_prime_generation(&mut self, generation: Arc<AtomicU64>) {
        self.prime_gen = Some(generation);
    }

    /// Current prime generation (0 when no counter is installed — the recheck
    /// then always matches, so single-life receivers are unaffected).
    #[inline]
    fn prime_gen_val(&self) -> u64 {
        self.prime_gen.as_ref().map_or(0, |g| g.load(Ordering::Relaxed))
    }

    /// Test-only: install a hook fired in the DATA arm between the `rebuilt`
    /// insert and the generation recheck, so a test can deterministically inject
    /// a straddling `prime` + generation bump.
    #[cfg(test)]
    fn set_straddle_hook(&mut self, hook: Box<dyn Fn() + Send>) {
        self.straddle_hook = Some(hook);
    }

    /// M6 Task 6: enable INBOUND snapshot transfers. `snap_dir` is where the
    /// `.part`/final artifacts land (`instance_dir/snapshots`); `incoming` (if
    /// set) receives the position of each COMPLETED transfer for the consensus
    /// agent to adopt as an archive floor. Without this call kinds 12/13 are
    /// ignored (a node that never joins below a floor never receives snapshots).
    pub fn set_snapshot_intake(&mut self, snap_dir: PathBuf, incoming: Option<Arc<AtomicU64>>) {
        self.snap_dir = Some(snap_dir);
        self.incoming_snapshot_pos = incoming;
    }

    #[inline]
    fn gate_open(&self) -> bool {
        self.gate.as_ref().is_none_or(|g| g.load(Ordering::Relaxed))
    }

    /// Emit one `LeaderActivity` per duty cycle. A full/disconnected route is
    /// fine — the latch still trips so we never spam the channel.
    fn note_leader_activity(&mut self, term: u32) {
        if self.activity_emitted {
            return;
        }
        let _ = self.route.try_send(NetEvent::LeaderActivity { term });
        self.activity_emitted = true;
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.sock.local_addr().expect("bound socket")
    }

    fn now_ns(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    /// Truncation resync (M4): after a reconciliation truncation the ARCHIVE's
    /// `LogCounters::prime(to)` REGRESSES the shared `append` counter to `to`,
    /// but our private `rebuilt` tracker is already PAST `to` (we rebuilt to the
    /// divergent frontier before the consensus agent cut it). We store
    /// `append = rebuilt.contiguous()` only on a FORWARD `insert`, so the
    /// regressed counter is never re-asserted — every subsequent DATA landing
    /// below the stale `contiguous` is dropped as a dup (`h.position <
    /// contiguous`) and `append`/`durable` would wedge at the truncation point
    /// forever.
    ///
    /// A backward move of the shared counter below our tracker is the archive's
    /// `prime` signature: in the follower role we are the counter's sole FORWARD
    /// writer and `prime` is its only backward writer, so `append < contiguous` is
    /// unambiguous. It fires on a reconciliation truncation AND on a leader-open
    /// collapse (`BecomeLeader` primes to `base`) — this resync handles BOTH,
    /// harmless-to-beneficial: whatever regressed the counter, rebuilding the gap
    /// tracker from the re-primed frontier is exactly the right recovery. On
    /// detection, rebuild the tracker from the re-primed counter — reset `rebuilt`
    /// and `leader_append` to `append`
    /// (stale tail-gap state would otherwise NAK for pre-truncation positions)
    /// and disarm the NAK timer (fresh gap tracking; `poll(None, …)` clears the
    /// armed gap).
    ///
    /// The intake gate is held CLOSED for the entire truncation round-trip
    /// (`node.rs`: gate closed → `Truncate` → archive `truncate_to` + `prime` →
    /// ack → gate reopened), so no DATA lands mid-regress. Running this at the
    /// TOP of `do_work` guarantees the resync happens on the first post-reopen
    /// duty cycle BEFORE any datagram of the new tail is processed.
    fn resync_after_truncation(&mut self) {
        let append = self.buffer.counters().append.load_acquire();
        // A BACKWARD move of the shared counter below our tracker is the archive's
        // `prime` signature: in the follower role we are the counter's sole FORWARD
        // writer, and `prime` is its only backward writer, so `append < contiguous`
        // is unambiguous (a leader's own append legitimately runs AHEAD of its
        // receiver's tracker, so a `!=` test would misfire on every leader cycle).
        // It fires on a reconciliation truncation AND on a `BecomeLeader` collapse.
        // The FORWARD `AdoptFloor` re-prime (M6 Task 8) is NOT distinguishable here
        // and is handled separately by `resync_after_snapshot_install`.
        if append < self.rebuilt.contiguous() {
            self.rebuilt = Rebuilt::new(append);
            self.leader_append = append;
            self.nak.poll(None, self.now_ns()); // disarm: the old gap predates the re-prime
            // The report cursors shadow the frontier and must move with it:
            // `status_at` gates on `contiguous - status_at` (would underflow if
            // left above a regressed frontier), and `ap_reported` gates the
            // AppendPosition send so the first re-established durable reports
            // promptly toward the leader's commit ranking.
            self.status_at = append;
            self.ap_reported = append;
            self.stats.truncation_resyncs.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One duty cycle: drain up to 64 datagrams, then NAK/status upkeep.
    pub fn do_work(&mut self) -> bool {
        // FIRST, before any datagram: if the archive truncated and re-primed the
        // shared `append` counter below our rebuilt frontier, rebuild the tracker
        // so the re-shipped post-truncation tail is accepted, not dropped as dup.
        self.resync_after_truncation();
        // And, after a snapshot install, forward to the adopted floor (M6 Task 8).
        self.resync_after_snapshot_install();
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
        // Consensus kinds (5–11) are forwarded RAW to the consensus agent — no
        // data-plane term filter, since a higher-term RequestVote MUST reach
        // the SM. `DGRAM_KIND_COMMIT_POSITION` routes as `CommitGossip`; the
        // consensus agent is the sole commit-counter writer (M4 carry #5
        // removed the M3 local COMMIT_POSITION store).
        if is_consensus_kind(h.kind) {
            if let Some(ev) = consensus_event(&h, d, from) {
                let idx = ev.kind_idx();
                if self.route.try_send(ev).is_err() {
                    // A full consensus channel drops term-critical traffic; count
                    // it per kind (the consensus agent's cadence recovers —
                    // votes/reports re-fire).
                    self.stats.net_drops[idx].fetch_add(1, Ordering::Relaxed);
                }
            }
            return;
        }
        let term = self.term.load(Relaxed);
        if h.leadership_term_id != term {
            self.stats.dropped_stale_term.fetch_add(1, Relaxed);
            return;
        }
        self.stats.datagrams.fetch_add(1, Relaxed);
        // Learn the current leader's address from its own current-term traffic
        // (DATA/HEARTBEAT flow leader→follower). Our follower-role control
        // (NAK/STATUS/AppendPosition) is then addressed to whoever is actually
        // leading THIS term — so a failover retargets our reports
        // automatically, without any external leader-hint plumbing. Only
        // leader-origin kinds retarget; follower→leader kinds (NAK/STATUS/
        // AppendPosition) never do.
        if matches!(h.kind, DGRAM_KIND_DATA | DGRAM_KIND_HEARTBEAT) {
            self.cfg.leader = from;
        }
        match h.kind {
            DGRAM_KIND_DATA => {
                // Straddle guard (M6 Task 9): sample the prime generation BEFORE
                // reading the frontier. If the archive re-primes `append` to a new
                // floor anywhere during this datagram's processing, the recheck
                // just before `store_release` (below) drops the now-stale frontier
                // rather than clobbering the freshly primed value.
                let gen0 = self.prime_gen_val();
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
                    #[cfg(test)]
                    if let Some(hook) = self.straddle_hook.take() {
                        hook();
                        self.straddle_hook = Some(hook);
                    }
                    // Recheck the prime generation. A prime that straddled this
                    // datagram re-based `append` to a new floor; publishing the
                    // stale `rebuilt.contiguous()` would drag it backward. Drop
                    // instead — the next `do_work` resync realigns the tracker to
                    // the primed floor and NAKs forward. `rebuilt` is left as-is;
                    // that resync discards it (rebuilds from `append`).
                    if self.prime_gen_val() != gen0 {
                        self.stats.dropped_straddle.fetch_add(1, Relaxed);
                        return;
                    }
                    self.buffer.counters().append.store_release(self.rebuilt.contiguous());
                }
                self.note_leader_activity(h.leadership_term_id);
            }
            DGRAM_KIND_HEARTBEAT => {
                self.leader_append = self.leader_append.max(h.position);
                self.note_leader_activity(h.leadership_term_id);
            }
            // DGRAM_KIND_COMMIT_POSITION is unreachable here: `is_consensus_kind`
            // always intercepts it above and routes it as `CommitGossip` to the
            // consensus agent, the binary's sole commit-counter writer (see the
            // grep-provable postcondition in `uc2_node::node::exec`'s
            // `Action::AdvanceCommit` arm).
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
            // M6 Task 6 — INBOUND snapshot transfer (this node is the receiver:
            // its NAK fell below the leader's purge floor). Term-filtered above.
            DGRAM_KIND_SNAP_BEGIN => {
                if let Some(b) = read_snap_begin_body(&d[DATAGRAM_HEADER_LEN..]) {
                    self.snap_begin(from, b);
                }
            }
            DGRAM_KIND_SNAP_CHUNK => {
                self.snap_chunk(from, h.position, &d[DATAGRAM_HEADER_LEN..]);
            }
            // OUTBOUND session control (this node is the leader shipping a
            // snapshot): demux the peer's repair NAK / completion to our sender.
            DGRAM_KIND_SNAP_NAK if self.sender_route.is_some() => {
                let body = &d[DATAGRAM_HEADER_LEN..];
                if let Some(b) = read_snap_nak_body(body)
                    && let Some(route) = &self.sender_route
                {
                    let _ = route.try_send(CtrlMsg::SnapNak {
                        from,
                        session: b.session,
                        offset: b.offset,
                        length: b.length,
                    });
                }
            }
            DGRAM_KIND_SNAP_DONE if self.sender_route.is_some() => {
                let body = &d[DATAGRAM_HEADER_LEN..];
                if let Some(b) = read_snap_begin_body(body)
                    && let Some(route) = &self.sender_route
                {
                    let _ = route.try_send(CtrlMsg::SnapDone { from, session: b.session });
                }
            }
            _ => {} // NAK/STATUS with no sender_route installed (follower role)
        }
    }

    /// Begin an inbound snapshot transfer: pre-size a `.part` and start tracking
    /// contiguity. A duplicate BEGIN for the in-flight session is a no-op; a BEGIN
    /// for a different session replaces a stale one.
    fn snap_begin(&mut self, from: SocketAddr, b: SnapBeginBody) {
        let Some(dir) = self.snap_dir.clone() else {
            return; // this node does not receive snapshots
        };
        if let Some(cur) = &self.snap_intake
            && cur.peer == from
            && cur.session == b.session
        {
            return; // duplicate BEGIN — already in progress
        }
        if b.total_len == 0 {
            return;
        }
        let part_path = dir.join(format!("incoming-{}.part", b.snapshot_pos));
        let final_path = dir.join(format!("snap-{}.ultsnap", b.snapshot_pos));
        let Ok(file) = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&part_path)
        else {
            return;
        };
        if file.set_len(b.total_len).is_err() {
            return;
        }
        self.snap_intake = Some(SnapIntake {
            peer: from,
            session: b.session,
            snapshot_pos: b.snapshot_pos,
            total_len: b.total_len,
            file,
            part_path,
            final_path,
            got: Rebuilt::new(0),
            nak: NakTimer::new(self.snap_nak_cfg, self.snap_seed ^ b.session as u64),
        });
        self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
    }

    /// Land one snapshot chunk at its file offset; on completion fsync + rename.
    fn snap_chunk(&mut self, from: SocketAddr, offset: u64, payload: &[u8]) {
        let Some(intake) = self.snap_intake.as_mut() else {
            return;
        };
        if intake.peer != from || payload.is_empty() {
            return;
        }
        let Some(end) = offset.checked_add(payload.len() as u64) else {
            return;
        };
        if end > intake.total_len {
            return; // past EOF — corrupt/duplicate; drop
        }
        if intake.file.seek(SeekFrom::Start(offset)).is_err()
            || intake.file.write_all(payload).is_err()
        {
            return;
        }
        intake.got.insert(offset, end);
        if intake.got.contiguous() >= intake.total_len {
            self.snap_complete();
        }
    }

    /// The `.part` is contiguous: fsync, atomically rename to the final artifact,
    /// signal completion, and ack with SNAP_DONE.
    fn snap_complete(&mut self) {
        let Some(intake) = self.snap_intake.take() else {
            return;
        };
        // Durability + atomic publish: a torn `.part` is never renamed, so a
        // reader (the service gap guard, or AdoptFloor) only ever sees a complete
        // artifact.
        if intake.file.sync_all().is_err() {
            return;
        }
        drop(intake.file);
        if std::fs::rename(&intake.part_path, &intake.final_path).is_err() {
            return;
        }
        // Ack: echo the SnapBeginBody as SNAP_DONE so the leader closes its session.
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN + SNAP_BEGIN_FIXED_LEN]; // M7: may grow with config
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: 0,
                leadership_term_id: self.term.load(Ordering::Relaxed),
                kind: DGRAM_KIND_SNAP_DONE,
                flags: 0,
            },
        );
        write_snap_begin_body(
            &mut d[DATAGRAM_HEADER_LEN..],
            &SnapBeginBody {
                session: intake.session,
                snapshot_pos: intake.snapshot_pos,
                total_len: intake.total_len,
                config: vec![], // M7: empty for M6
            },
        );
        let _ = self.sock.send_to(&d, intake.peer);
        // Signal the consensus agent to adopt the floor + mirror observability.
        if let Some(slot) = &self.incoming_snapshot_pos {
            slot.store(intake.snapshot_pos, Ordering::Release);
        }
        // Arm the forward gap-tracker resync: once the consensus agent's AdoptFloor
        // re-primes the shared `append` up to this position, rebuild our tracker
        // forward so we NAK the retained `[floor, frontier)` tail (M6 Task 8).
        self.snap_adopt_pending = Some(intake.snapshot_pos);
        self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
    }

    /// M6 Task 8: after a snapshot install, the consensus agent's `AdoptFloor`
    /// re-primes the shared `append` counter FORWARD to the snapshot floor. Unlike
    /// the backward truncation re-prime, a forward move is indistinguishable from a
    /// leader's own append by the counter alone — so this resync is gated on an
    /// actual completed install (`snap_adopt_pending`). Once `append` has reached
    /// the adopted floor, rebuild the gap tracker there so the next NAK requests the
    /// retained tail (not the purged prefix, which would loop the session forever),
    /// then disarm.
    fn resync_after_snapshot_install(&mut self) {
        let Some(floor) = self.snap_adopt_pending else {
            return;
        };
        let append = self.buffer.counters().append.load_acquire();
        if append < floor {
            return; // AdoptFloor not applied yet
        }
        if append > self.rebuilt.contiguous() {
            self.rebuilt = Rebuilt::new(append);
            self.leader_append = self.leader_append.max(append);
            self.nak.poll(None, self.now_ns()); // disarm the stale below-floor gap
            self.status_at = append;
            self.ap_reported = append;
            self.stats.truncation_resyncs.fetch_add(1, Ordering::Relaxed);
        }
        self.snap_adopt_pending = None;
    }

    /// Emit a SNAP_NAK for the first gap in the inbound transfer (RTT-delayed,
    /// like the main stream). Called once per duty cycle from `upkeep`.
    fn snap_upkeep(&mut self, now: u64) -> bool {
        let Some(intake) = self.snap_intake.as_mut() else {
            return false;
        };
        let contiguous = intake.got.contiguous();
        let gap = intake.got.first_gap().or({
            if contiguous < intake.total_len {
                Some((contiguous, intake.total_len))
            } else {
                None
            }
        });
        let fired = intake.nak.poll(gap, now);
        if let Some((start, end)) = fired {
            let length = (end - start).min(self.cfg.nak_max_bytes as u64) as u32;
            let session = intake.session;
            let peer = intake.peer;
            let term = self.term.load(Ordering::Relaxed);
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN + SNAP_NAK_BODY_LEN];
            write_datagram_header(
                &mut d,
                &DatagramHeader {
                    position: 0,
                    leadership_term_id: term,
                    kind: DGRAM_KIND_SNAP_NAK,
                    flags: 0,
                },
            );
            write_snap_nak_body(
                &mut d[DATAGRAM_HEADER_LEN..],
                &SnapNakBody { session, offset: start, length },
            );
            let _ = self.sock.send_to(&d, peer);
            return true;
        }
        false
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

        // M6 Task 6: drive an inbound snapshot transfer's NAK repair. Poll twice
        // (arm then fire) like the main-stream NAK above.
        if self.snap_upkeep(now) || self.snap_upkeep(self.now_ns()) {
            did = true;
        }
        did
    }
}

// `LeaderReceiver` (the M2/M3 separate leader-side control-only actor) is
// deleted (M4 carry #5): the real `uc2_node::Node` never used it — it composes
// leader duty onto the SAME unified `FollowerReceiver` via `set_sender_route`
// (see `sender_route_demuxes_nak_and_status` below), which is now the only
// receiver type in the crate.

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use uc2_log::buffer::{Appender, LogBuffer, SliceRead};
    use uc2_log::cnc::{CncMeta, CncPage};
    use uc2_log::region::Region;
    use uc_protocol::v2::datagram::{
        read_nak_body, read_status_body, write_datagram_header, write_nak_body,
        write_status_body, DatagramHeader, DATAGRAM_HEADER_LEN, DGRAM_KIND_APPEND_POSITION,
        DGRAM_KIND_COMMIT_POSITION, DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT, DGRAM_KIND_NAK,
        DGRAM_KIND_STATUS, NAK_BODY_LEN, STATUS_BODY_LEN,
    };

    const TERM: u32 = 9;

    fn test_cnc(cap: u64) -> Arc<CncPage> {
        CncPage::heap(&CncMeta {
            node_id: 0,
            instance_id: 0,
            app_id: "test".into(),
            buffer_bytes: cap,
            max_payload: 256,
        })
    }

    fn buffer() -> Arc<LogBuffer> {
        Arc::new(LogBuffer::new(Region::heap_zeroed(1 << 16), test_cnc(1 << 16), 256))
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

    /// A dummy consensus route for tests that don't inspect [`NetEvent`]s (the
    /// receiver is always node-mode now — every constructor call needs one).
    /// The receiver is the only sender and this fn's `_rx` is dropped, so sends
    /// fail `Disconnected` — harmless, identical to a full channel (counted,
    /// never panics).
    fn dummy_route() -> mpsc::SyncSender<NetEvent> {
        let (tx, _rx) = mpsc::sync_channel(16);
        tx
    }

    fn follower(b: &Arc<LogBuffer>, leader: SocketAddr) -> FollowerReceiver {
        follower_routed(b, leader, dummy_route())
    }

    fn follower_routed(
        b: &Arc<LogBuffer>,
        leader: SocketAddr,
        route: mpsc::SyncSender<NetEvent>,
    ) -> FollowerReceiver {
        let mut cfg = FollowerConfig::new(leader);
        cfg.nak = NakConfig { delay_min_ns: 1, delay_max_ns: 2, backoff_ns: 1_000_000 };
        cfg.status_floor_ns = u64::MAX; // no time-driven status in unit tests
        cfg.append_pos_floor_ns = u64::MAX; // advance-driven AppendPosition only
        FollowerReceiver::new(
            Arc::clone(b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            cfg,
            term_handle(TERM),
            route,
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
            dummy_route(),
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
        let c = Arc::clone(b.cnc());
        let mut a = Appender::new(Arc::clone(&b), TERM);
        let per = 96u64;
        let cap = b.capacity();
        let fill = (cap / per) as usize; // 682 frames -> 65472, 64 short of the wrap
        for i in 0..fill {
            a.append(4, i as u64, &[0u8; 64]).unwrap();
            c.counters().durable.store_release(a.position());
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
            Arc::new(LogBuffer::new(Region::heap_zeroed(4096), test_cnc(4096), 256))
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
            dummy_route(),
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

    // `commit_position_gossip_is_stored_monotonically` (the FollowerReceiver's
    // own local COMMIT_POSITION counter store + its stale-term drop) and the
    // two `leader_receiver_*` tests (the deleted `LeaderReceiver`'s NAK/STATUS
    // and AppendPosition/stale-term demux) are DELETED — M4 carry #5. Their
    // properties are ported/covered as follows:
    //   * CommitGossip parsing + delivery to the consensus route: extended
    //     into `node_mode_routes_consensus_raw_and_disables_commit_store`
    //     below (renamed `consensus_kinds_route_raw_to_the_consensus_agent`).
    //   * Monotonic commit / stale-term-drop semantics for CommitGossip: now
    //     SM-level state, not wire state — pinned by
    //     `uc2_consensus::election::tests::follower_commit_gossip_is_monotonic_and_term_checked`.
    //   * NAK/STATUS demux to the sender's control channel: already covered
    //     by `sender_route_demuxes_nak_and_status` below (the M4 upgrade of
    //     the same property onto the unified receiver, pre-dating this task).
    //   * AppendPosition -> consensus `Report`: ported into
    //     `consensus_kinds_route_raw_to_the_consensus_agent` below.
    //   * AppendPosition stale-term drop: no longer receiver-level — node mode
    //     forwards ALL terms of consensus kinds raw by design ("the SM adopts
    //     higher terms"); the term check now lives in the SM and is pinned by
    //     `uc2_consensus::election::tests::higher_term_deposes_leader_and_stale_events_ignored`.

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
            dummy_route(),
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

    /// Consensus kinds (5–9) reach the consensus route RAW, bypassing the
    /// data-plane term filter entirely (a HIGHER-term RequestVote MUST reach
    /// the SM), and never touch the local commit counter — the consensus
    /// agent is the binary's sole writer of it (M4 carry #5: the M3 local
    /// COMMIT_POSITION store and the separate `LeaderReceiver` actor are both
    /// gone; this test ports the latter's AppendPosition-routing coverage).
    #[test]
    fn consensus_kinds_route_raw_to_the_consensus_agent() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let (tx, rx) = mpsc::sync_channel::<NetEvent>(16);
        let mut r = follower_routed(&b, leader.addr(), tx);
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
        // AppendPosition at a term ABOVE current — also raw, no term filter
        // (ports `leader_receiver_node_mode_routes_append_position_as_report`)
        leader.send(to, DGRAM_KIND_APPEND_POSITION, 2048, TERM + 3, &[]);

        let mut saw_vote = false;
        let mut saw_gossip = false;
        let mut saw_report = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while !(saw_vote && saw_gossip && saw_report) {
            assert!(Instant::now() < deadline, "consensus events never routed");
            r.do_work();
            while let Ok(ev) = rx.try_recv() {
                match ev {
                    NetEvent::RequestVote { body, .. } => {
                        assert_eq!(body.new_term, TERM + 5);
                        saw_vote = true;
                    }
                    NetEvent::CommitGossip { term, commit, .. } => {
                        assert_eq!((term, commit), (TERM, 4096));
                        saw_gossip = true;
                    }
                    NetEvent::Report { term, durable, .. } => {
                        assert_eq!((term, durable), (TERM + 3, 2048));
                        saw_report = true;
                    }
                    _ => {}
                }
            }
        }
        // the local commit counter is never written — the consensus agent owns it
        assert_eq!(b.counters().commit.load_acquire(), 0, "receiver stored commit locally");
    }

    /// Read-barrier kinds 10/11 route RAW to the consensus agent, carrying the
    /// probe's HEADER term (the node-side stale-leader filter reads it) and the
    /// body's `from` node id. Delivered even when the datagram term differs from
    /// the receiver's own — term adjudication is the SM's job, not the data
    /// plane's (a probe/ack must always reach the barrier).
    #[test]
    fn read_probe_kinds_route_raw_with_header_term() {
        use uc_protocol::v2::datagram::{
            write_read_probe_body, ReadProbeBody, READ_PROBE_BODY_LEN,
        };
        let b = buffer();
        let mut leader = FakeLeader::new();
        let (tx, rx) = mpsc::sync_channel::<NetEvent>(16);
        let mut r = follower_routed(&b, leader.addr(), tx);
        let to = r.local_addr();

        let mut body = vec![0u8; READ_PROBE_BODY_LEN];
        write_read_probe_body(&mut body, &ReadProbeBody { nonce: 0xABCD, from: 2 });
        // Probe at a term ABOVE the receiver's — must NOT be term-filtered.
        leader.send(to, DGRAM_KIND_READ_PROBE, 0, TERM + 4, &body);
        write_read_probe_body(&mut body, &ReadProbeBody { nonce: 0xABCD, from: 1 });
        leader.send(to, DGRAM_KIND_READ_PROBE_ACK, 0, TERM, &body);

        let (mut saw_probe, mut saw_ack) = (false, false);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !(saw_probe && saw_ack) {
            assert!(Instant::now() < deadline, "read-barrier events never routed");
            r.do_work();
            while let Ok(ev) = rx.try_recv() {
                match ev {
                    NetEvent::ReadProbe { nonce, from, term } => {
                        assert_eq!((nonce, from, term), (0xABCD, 2, TERM + 4));
                        saw_probe = true;
                    }
                    NetEvent::ReadProbeAck { nonce, from } => {
                        assert_eq!((nonce, from), (0xABCD, 1));
                        saw_ack = true;
                    }
                    _ => {}
                }
            }
        }
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
            dummy_route(),
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

    /// M4 truncation resync: after a reconciliation truncation the archive's
    /// `prime(to)` REGRESSES the shared `append` counter below the receiver's
    /// private `rebuilt.contiguous` frontier. The receiver must detect the
    /// regression at the TOP of its duty cycle, rebuild its tracker from the
    /// re-primed counter, and then ACCEPT DATA at the truncation point — pre-fix
    /// it dropped every such datagram as a dup (`position < contiguous`) and
    /// wedged `append`/`durable` at the truncation point forever.
    #[test]
    fn truncation_regression_resyncs_rebuilt_tracker() {
        use std::sync::atomic::Ordering::Relaxed;
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();

        // 1. drive the follower's contiguous frontier to 288 (three 96 B frames).
        let runs = frame_runs(&[&[1u8; 64], &[2u8; 64], &[3u8; 64]], 96);
        assert_eq!(runs.len(), 3);
        for (pos, bytes, _) in &runs {
            leader.send(to, DGRAM_KIND_DATA, *pos, TERM, bytes);
        }
        drive_until(&mut r, || b.counters().append.load_acquire() == 288);

        // 2. simulate the archive truncation round-trip: the consensus agent has
        //    closed the intake gate and the archive re-primed the counters back
        //    to 96 (a divergent tail past 96 was cut). No DATA lands mid-regress
        //    because the gate is closed; here we drive the counter directly. The
        //    receiver's private tracker is still at 288.
        b.counters().prime(96);
        assert_eq!(b.counters().append.load_acquire(), 96);

        // 3. one duty cycle detects the regression and resyncs. The check is at
        //    the top of do_work and synchronous on the counter, so exactly one
        //    call trips it (no DATA is pending).
        r.do_work();
        assert_eq!(r.stats().truncation_resyncs.load(Relaxed), 1);

        // 4. DATA at the truncation point (position 96) is now ACCEPTED and
        //    advances the frontier FROM 96 — pre-fix `96 < contiguous(288)` so it
        //    was dropped as a dup and the log never moved.
        let dup_before = r.stats().dropped_dup.load(Relaxed);
        leader.send(to, DGRAM_KIND_DATA, runs[1].0, TERM, &runs[1].1);
        drive_until(&mut r, || b.counters().append.load_acquire() == 192);
        assert_eq!(
            r.stats().dropped_dup.load(Relaxed),
            dup_before,
            "post-resync DATA at the truncation point was dropped as a dup"
        );

        // 5. idempotent: once the tracker matches the counter again, no further
        //    resync fires.
        r.do_work();
        assert_eq!(r.stats().truncation_resyncs.load(Relaxed), 1);
    }

    /// M6 Task 9 (straddle hardening): a `LogCounters::prime(to)` that lands
    /// BETWEEN the DATA arm's frontier read and its `append.store_release` must
    /// not be clobbered by the stale `rebuilt.contiguous()`. The archive agent
    /// re-primes `append` to a new floor (AdoptFloor after a snapshot install) and
    /// bumps a shared prime-generation counter; the receiver rechecks that counter
    /// just before storing and DROPS the straddled datagram (`dropped_straddle`)
    /// so the freshly primed floor survives. Pre-fix, the store would drag `append`
    /// back down to the old life's frontier — a below-floor re-request storm.
    #[test]
    fn straddling_prime_is_not_clobbered_by_stale_frontier() {
        use std::sync::atomic::Ordering::Relaxed;
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();

        // Install the shared prime-generation counter (as the node does).
        let prime_gen = Arc::new(AtomicU64::new(0));
        r.set_prime_generation(Arc::clone(&prime_gen));

        // The hook simulates the archive agent priming `append` FORWARD to a new
        // floor (960) and publishing the prime — exactly while this datagram sits
        // between its `rebuilt.insert` and `store_release`. One-shot: it primes
        // only on the first fire (generation still 0).
        let bh = Arc::clone(&b);
        let pgh = Arc::clone(&prime_gen);
        r.set_straddle_hook(Box::new(move || {
            if pgh.load(Relaxed) == 0 {
                bh.counters().prime(960);
                pgh.fetch_add(1, std::sync::atomic::Ordering::Release);
            }
        }));

        // Deliver one in-order DATA frame at position 0. It passes the gate and
        // frontier check, writes, and inserts (0, 96) — then the hook straddles.
        let runs = frame_runs(&[&[7u8; 64]], 96);
        let (pos, bytes, _) = &runs[0];
        leader.send(to, DGRAM_KIND_DATA, *pos, TERM, bytes);
        let st = r.stats();
        drive_until(&mut r, || st.dropped_straddle.load(Relaxed) == 1);

        // The primed floor (960) survives — the stale contiguous (96) was dropped.
        assert_eq!(
            b.counters().append.load_acquire(),
            960,
            "straddling prime clobbered by the stale frontier"
        );
        assert_eq!(prime_gen.load(Relaxed), 1);
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
                    _ => {} // snapshot-session control not exercised by this test
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

    // `leader_receiver_node_mode_routes_append_position_as_report` (the
    // deleted `LeaderReceiver`'s AppendPosition-as-Report + NAK-to-sender
    // demux) is DELETED — M4 carry #5. Its properties are covered above:
    // AppendPosition -> `NetEvent::Report` by
    // `consensus_kinds_route_raw_to_the_consensus_agent`, NAK -> the sender's
    // channel by `sender_route_demuxes_nak_and_status` (this file).
}
