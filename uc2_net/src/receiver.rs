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

use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Instant;

use uc2_crypto::{CryptoError, NodeId, ReceiveHalf};
use uc2_log::buffer::LogBuffer;
use uc2_log::writer::PositionedWriter;
use uc_protocol::v2::crypto::{CRYPTO_OVERHEAD, DGRAM_KIND_HS_INIT, DGRAM_KIND_HS_KEY, DGRAM_KIND_HS_RESP};
use uc_protocol::v2::datagram::{
    ConfigProposalBody, ConfigReplyBody, DATAGRAM_HEADER_LEN, DGRAM_KIND_APPEND_POSITION,
    DGRAM_KIND_COMMIT_POSITION,
    DGRAM_KIND_CONFIG_PROPOSAL, DGRAM_KIND_CONFIG_REPLY, DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT,
    DGRAM_KIND_NAK, DGRAM_KIND_READ_PROBE, DGRAM_KIND_READ_PROBE_ACK, DGRAM_KIND_REQUEST_VOTE,
    DGRAM_KIND_SNAP_BEGIN, DGRAM_KIND_SNAP_CHUNK, DGRAM_KIND_SNAP_DONE, DGRAM_KIND_SNAP_NAK,
    DGRAM_KIND_STATUS, DGRAM_KIND_TERM_MAP, DGRAM_KIND_VOTE, DatagramHeader,
    MAX_TERM_MAP_WIRE_ENTRIES, NAK_BODY_LEN, NakBody, REQUEST_VOTE_BODY_LEN, RequestVoteBody,
    SNAP_BEGIN_FIXED_LEN, SNAP_NAK_BODY_LEN, STATUS_BODY_LEN, SnapBeginBody, SnapNakBody, StatusBody,
    TermMapEntryWire, VOTE_BODY_LEN, VoteBody, read_config_proposal_body, read_config_reply_body,
    read_datagram_header, read_nak_body, read_read_probe_body, read_request_vote_body,
    read_snap_begin_body, read_snap_nak_body, read_status_body, read_term_map_body, read_vote_body,
    write_datagram_header, write_nak_body, write_snap_begin_body, write_snap_nak_body,
    write_status_body,
};
use uc_protocol::v2::frame::{self, FRAME_TYPE_PADDING, HEADER_LEN, align_frame_len};

use crate::TermHandle;
use crate::fault::FaultSocket;
use crate::rebuild::{NakConfig, NakTimer, Rebuilt};
use crate::sender::CtrlMsg;

/// M8 (Task 11): a handshake-plane datagram (kinds 18/19/20) forwarded off
/// the receive seam, `(from, kind, opened-body-if-any)`. `HS_INIT`/`HS_RESP`
/// (18/19, `Scope::Unsealed`) carry their body verbatim off the wire — no
/// session exists yet to open them under. `HS_KEY` (20, `Scope::Pairwise`)
/// carries its body AFTER `ReceiveHalf::open_slice` has already decrypted
/// it — see `crypto_admit`. Nothing in `uc2_net` drives the actual handshake
/// state machine from this route yet; that is Task 12's node-layer wiring
/// (`uc2_crypto::SharedTransport::initiate`/`on_handshake_message`/
/// `on_group_key_message`, T11's own plan-gap addition). Until a route is
/// installed via [`FollowerReceiver::set_handshake_route`], these are
/// dropped and counted (`FollowerStats::dropped_handshake`) — never silently
/// absorbed, and never fed to `on_datagram`, which has no idea what an
/// HS_INIT is.
pub type HandshakeDatagram = (SocketAddr, u8, Vec<u8>);

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
    /// M7 Task 7: a follower's forwarded membership proposal (kind 16, `uc2ctl`'s
    /// admin request forwarded by a non-leader that has a leader hint). `from` is
    /// the forwarding follower's address — the leader's reply (kind 17) is
    /// addressed back to it. Routed RAW like the other consensus kinds: a stale/
    /// not-yet-leader node just drops it (the follower's forward times out).
    ConfigProposal { from: SocketAddr, body: ConfigProposalBody },
    /// M7 Task 7: the leader's reply to a forwarded proposal (kind 17),
    /// follower-bound. Matched by the follower's 1-slot pending map on `nonce`.
    ConfigReply { body: ConfigReplyBody },
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
            NetEvent::ConfigProposal { .. } => 8,
            NetEvent::ConfigReply { .. } => 9,
        }
    }
}

/// Number of [`NetEvent`] kinds (the width of the per-kind drop counters).
pub const NET_EVENT_KINDS: usize = 10;

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
        DGRAM_KIND_CONFIG_PROPOSAL => {
            Some(NetEvent::ConfigProposal { from, body: read_config_proposal_body(body)? })
        }
        DGRAM_KIND_CONFIG_REPLY => Some(NetEvent::ConfigReply { body: read_config_reply_body(body)? }),
        _ => None,
    }
}

/// True iff `kind` is a consensus-plane datagram (kinds 5–11, plus the M7
/// admin-forward kinds 16/17) — routed RAW to the consensus agent in node
/// mode, bypassing the data-plane term filter.
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
            | DGRAM_KIND_CONFIG_PROPOSAL
            | DGRAM_KIND_CONFIG_REPLY
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

/// M8 (Task 11): minimum spacing between `note_cleartext_peer`'s operator
/// `eprintln!` for the SAME peer address — the counter still increments on
/// every occurrence; only the log line is throttled.
const CLEARTEXT_LOG_INTERVAL_NS: u64 = 30_000_000_000; // 30s

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
    /// M8 (Task 11): a sealed datagram failed AEAD authentication — wrong
    /// key, tampered ciphertext, tampered header, or an unresolvable sender
    /// (no `SocketAddr -> NodeId` mapping installed — see
    /// [`FollowerReceiver::set_peer_ids`] — is folded in here too: from the
    /// wire's point of view an unrecognized sender IS "could not
    /// authenticate this," the same bucket).
    pub dropped_auth_failed: AtomicU64,
    /// M8 (Task 11): a sealed datagram's counter was already seen under its
    /// (sender, epoch, salt) replay window — `uc2_crypto::CryptoError::Replayed`.
    pub dropped_replay: AtomicU64,
    /// M8 (Task 11): a sealed datagram named a group-key epoch this node
    /// does not hold (never minted, or rotated out) —
    /// `uc2_crypto::CryptoError::NoGroupKey`. Self-heals once `HS_KEY`
    /// lands for the epoch (`uc2_crypto::group`'s docs); never a signal to
    /// fall back to accepting cleartext.
    pub dropped_unknown_epoch: AtomicU64,
    /// M8 (Task 11): the specific, rate-limited flag-day-rollout diagnostic
    /// — a datagram arrived that is BOTH stamped `key_epoch == 0` (the wire
    /// format's documented cleartext sentinel) AND shorter than any validly
    /// sealed frame could ever be, while THIS node has crypto enabled. See
    /// `crypto_admit`'s doc for why both signals are required (a real,
    /// if numerically unlucky, epoch-0 SEALED datagram is never long
    /// enough to trip this). Distinct from `dropped_auth_failed` — the
    /// brief's own framing: "the likeliest operator error under flag-day
    /// rollout," diagnosable as such rather than a generic auth failure.
    pub peer_appears_cleartext: AtomicU64,
    /// M8 (Task 11): a would-be-sealed datagram arrived from a `SocketAddr`
    /// with no entry in [`FollowerReceiver::set_peer_ids`]'s map. Counted
    /// separately from `dropped_auth_failed` for operator diagnosability
    /// (a misconfigured peer map vs. a genuine forgery look identical on
    /// the wire but very different to fix) even though both are folded
    /// into `dropped_auth_failed` for the mandated counter's own semantics.
    pub dropped_unknown_peer: AtomicU64,
    /// M8 (Task 11): a handshake-plane datagram (kind 18/19/20) could not be
    /// forwarded — no route installed via `set_handshake_route`, or the
    /// route's channel was full/disconnected. Harmless by the same
    /// reasoning as `net_drops`: `Peers::tick`/a retry re-initiates.
    pub dropped_handshake: AtomicU64,
}

/// M7 Task 6: the `(position, config)` companion cells `set_snapshot_intake`
/// wires in — the position cell for `ArchiveCmd::AdoptFloor`, the config cell
/// for `adopt_snapshot_config`. Named to keep `set_snapshot_intake`'s signature
/// under clippy's type-complexity threshold.
pub type IncomingSnapshotSignal = (Arc<AtomicU64>, Arc<Mutex<Vec<u8>>>);

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
    /// M7 Task 6: the encoded `ConfigRecord.config` carried in `SNAP_BEGIN`
    /// (`v2::config::encode_config` bytes; empty if the leader shipped none).
    /// Forwarded to `incoming_snapshot_config` on completion, alongside
    /// `incoming_snapshot_pos`, for the consensus agent's install handler.
    config: Vec<u8>,
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
    /// M7 Task 6: companion cell for `incoming_snapshot_pos` — the encoded
    /// config carried by the SAME completed transfer. Written in `snap_complete`
    /// BEFORE `incoming_snapshot_pos` is stored (so the consensus agent's
    /// `Acquire` load of the position, once it observes the new value, is
    /// guaranteed to see this cell's matching content — the mutex lock/unlock
    /// pair is itself a release/acquire fence). `None` in unit tests.
    incoming_snapshot_config: Option<Arc<Mutex<Vec<u8>>>>,
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
    /// M8 (Task 11): wire crypto, if enabled. `None` = every inbound datagram
    /// is handled exactly as pre-M8 — `crypto_admit` becomes a pass-through.
    /// `Some` decrypts (or diagnoses/drops) a datagram BEFORE `on_datagram`
    /// ever sees it, so every parser in this file stays unaware crypto
    /// exists at all — see `crypto_admit`'s doc.
    ///
    /// A `ReceiveHalf`, not a whole `Transport`/`SharedTransport` — same
    /// ownership split as `Sender`'s `crypto: Option<SendHalf>` (T10, see
    /// its field doc and `uc2_crypto::transport`'s "M8 ownership
    /// correction" module docs): this receiver agent owns only the group-
    /// scope replay windows (receiver-exclusive, no lock); the shared
    /// handshake sessions/group-key plane/boot-salt live behind the
    /// `Arc<Mutex<_>>` a `uc2_crypto::SharedTransport` hands this half out
    /// from.
    crypto: Option<ReceiveHalf>,
    /// M8 (Task 11): `SocketAddr -> NodeId` map, needed to resolve `from`
    /// before `crypto.open_slice` can be called (the crypto layer identifies
    /// peers by `NodeId`; `uc2_net`'s wire layer has only ever known
    /// `SocketAddr`s — see `crypto_admit`'s doc and the module docs). `None`
    /// entries (an address not in this map) are dropped and counted
    /// (`dropped_unknown_peer`, folded into `dropped_auth_failed`) — never a
    /// panic on an unrecognized address, since that address is exactly as
    /// attacker-controlled as anything else arriving on this socket. Empty
    /// by default (every existing non-crypto call site is unaffected); the
    /// node layer (T12) installs it via [`FollowerReceiver::set_peer_ids`]
    /// from the SAME `id_to_addr` map it already builds for the sender's
    /// `sender_peer_slots` (`node.rs:640`), inverted.
    peer_ids: HashMap<SocketAddr, NodeId>,
    /// M8 (Task 11): handshake-plane route (kinds 18/19/20 — see
    /// [`HandshakeDatagram`]'s doc). `None` = dropped and counted
    /// (`dropped_handshake`) — the T11 scope is "route it somewhere safe,"
    /// not "drive the handshake" (Task 12's node-layer job).
    hs_route: Option<mpsc::SyncSender<HandshakeDatagram>>,
    /// M8 (Task 11): last-diagnosed-at (`now_ns`) per peer, for the
    /// rate-limited "peer appears cleartext" diagnostic (`note_cleartext_peer`).
    /// Bounded by construction: only ever gains an entry for an address that
    /// ALREADY resolved via `peer_ids` (a known, configured cluster peer) —
    /// never for an arbitrary spoofed source address — so this cannot be
    /// grown into an unbounded-memory vector by a flood of forged source
    /// addresses.
    cleartext_peer_log: HashMap<SocketAddr, u64>,
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
        Self::with_crypto(buffer, sock, cfg, term, route, None)
    }

    /// M8 (Task 11): the innermost constructor — `new` is a thin wrapper over
    /// this with `crypto: None`. Takes a `ReceiveHalf` (from
    /// `uc2_crypto::SharedTransport::receive_half`), never a whole
    /// `Transport`/`SharedTransport` — see the `crypto` field's doc for why.
    /// The caller (the node layer, T12) owns the `SharedTransport` and calls
    /// `receive_half()` exactly once per process; this constructor has no
    /// way to enforce that single-call discipline itself (it only ever sees
    /// the `ReceiveHalf` already handed out).
    pub fn with_crypto(
        buffer: Arc<LogBuffer>,
        sock: FaultSocket,
        cfg: FollowerConfig,
        term: TermHandle,
        route: mpsc::SyncSender<NetEvent>,
        crypto: Option<ReceiveHalf>,
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
            incoming_snapshot_config: None,
            snap_nak_cfg: cfg.nak,
            snap_seed: cfg.seed,
            snap_adopt_pending: None,
            prime_gen: None,
            #[cfg(test)]
            straddle_hook: None,
            crypto,
            peer_ids: HashMap::new(),
            hs_route: None,
            cleartext_peer_log: HashMap::new(),
        }
    }

    /// M8 (Task 11): install the `SocketAddr -> NodeId` map `crypto_admit`
    /// needs to resolve a datagram's sender before it can call
    /// `ReceiveHalf::open_slice`. Without this call every crypto-scoped
    /// datagram is dropped as `dropped_unknown_peer`/`dropped_auth_failed`
    /// (an empty map resolves nothing) — harmless on a node with crypto
    /// disabled (this is never consulted; see `crypto_admit`'s first line).
    pub fn set_peer_ids(&mut self, ids: impl IntoIterator<Item = (SocketAddr, NodeId)>) {
        self.peer_ids = ids.into_iter().collect();
    }

    /// M8 (Task 11): install the handshake-plane route (kinds 18/19/20 —
    /// see [`HandshakeDatagram`]'s doc). Without this call handshake
    /// datagrams are dropped and counted (`dropped_handshake`) — this
    /// receiver never drives `Peers`/`GroupPlane` itself.
    pub fn set_handshake_route(&mut self, tx: mpsc::SyncSender<HandshakeDatagram>) {
        self.hs_route = Some(tx);
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
    /// set) is `(position, config)`: the position cell receives each COMPLETED
    /// transfer's floor for the consensus agent to adopt as an archive floor,
    /// and (M7 Task 6) the config cell receives that SAME transfer's carried
    /// `SNAP_BEGIN.config` bytes for the agent's `adopt_snapshot_config` install
    /// handler. Without this call kinds 12/13 are ignored (a node that never
    /// joins below a floor never receives snapshots).
    pub fn set_snapshot_intake(&mut self, snap_dir: PathBuf, incoming: Option<IncomingSnapshotSignal>) {
        self.snap_dir = Some(snap_dir);
        if let Some((pos, config)) = incoming {
            self.incoming_snapshot_pos = Some(pos);
            self.incoming_snapshot_config = Some(config);
        }
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
                // M8 (Task 11): decrypt (or diagnose/drop) BEFORE on_datagram
                // ever sees the bytes — see `crypto_admit`'s doc. `Some(len)`
                // = admitted, plaintext, `buf[..len]` is what `on_datagram`
                // parses (byte-identical to the pre-M8 shape whether crypto
                // is on or off); `None` = dropped here, already counted.
                if let Some(len) = self.crypto_admit(&mut buf, n, from) {
                    self.on_datagram(&buf[..len], from);
                }
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

    /// M8 (Task 11): the receive-side counterpart to `Sender::seal_scratch`
    /// (T10) — the ONE place a datagram is decrypted, diagnosed, or dropped
    /// for a crypto-related reason, so `on_datagram` and everything it calls
    /// stays completely unaware crypto exists.
    ///
    /// `buf` is `self.recv_buf`'s full 64 KiB backing storage (see
    /// `do_work`); `n` is `recv_from`'s reported length, never `buf.len()`.
    /// Only `buf[..n]` is ever read; `ReceiveHalf::open_slice` decrypts in
    /// place there and reports how much of it is now plaintext — this is
    /// the zero-copy-on-the-hot-path property T9's `open_detached` (and its
    /// `ReceiveHalf::open_slice` wrapper) exist for: no `truncate(n) -> open
    /// -> resize(65536, 0)`, which would memset up to 64 KiB PER DATAGRAM
    /// (T5 review carry (d)) — the same order of cost as the AEAD open
    /// itself.
    ///
    /// Every untrusted-input path here ends in a drop-and-count, never a
    /// panic and never a propagated `Err` — see the module docs' binding
    /// rule ("a node must not be killable by a datagram").
    fn crypto_admit(&mut self, buf: &mut [u8], n: usize, from: SocketAddr) -> Option<usize> {
        use Ordering::Relaxed;
        if self.crypto.is_none() {
            return Some(n); // crypto disabled: byte-for-byte the pre-M8 path
        }
        if n < DATAGRAM_HEADER_LEN {
            // Too short even for a header. `on_datagram`'s own malformed
            // check (below `DATAGRAM_HEADER_LEN`) handles this identically
            // whether crypto is on or off — no need to duplicate it here.
            return Some(n);
        }
        let h = read_datagram_header(&buf[..n]);

        // ---- T17 TEMPORARY ALLOWANCE — grep "T17" ------------------------
        // SNAP_BEGIN/SNAP_CHUNK ship cleartext until Task 17 seals the
        // remaining pairwise sends in `uc2_net` (T10 left them unsealed:
        // pairwise sealing needs an established handshake session, which
        // nothing drives until Task 12; see `assemble_snap`'s doc in
        // `sender.rs` for the send-side half of this same disclosure).
        // Dropping them here — treating them as "must be sealed like
        // everything else" — would wedge snapshot transfer the moment
        // crypto is ON: a learner, a cold-started node, or a below-floor
        // follower can ONLY converge via a snapshot session, and this is
        // that session's entire wire path. This allowance is exactly what
        // makes forged-membership injection possible until T17 lands:
        // `SNAP_BEGIN` carries `SnapBeginBody.config` straight into
        // `maybe_adopt_incoming_snapshot`, so an on-path attacker can forge
        // a session and install attacker-chosen application state AND
        // attacker-chosen cluster membership on a joining/below-floor node.
        // Task 17 deletes this `if` in one edit and confirms the receive
        // path then refuses unsealed SNAP.
        if matches!(h.kind, DGRAM_KIND_SNAP_BEGIN | DGRAM_KIND_SNAP_CHUNK) {
            return Some(n);
        }

        // Handshake bootstrap (`Scope::Unsealed`, spec §5): no session and
        // no key exist yet for these — they are what CREATES a session.
        // Never goes through `open_slice`; hand the raw wire body to
        // whichever agent drives `Peers` (Task 12's node wiring).
        if matches!(h.kind, DGRAM_KIND_HS_INIT | DGRAM_KIND_HS_RESP) {
            self.route_handshake(from, h.kind, buf[DATAGRAM_HEADER_LEN..n].to_vec());
            return None;
        }

        // Everything else (`Scope::Group` + `Scope::Pairwise`, including
        // `HS_KEY`) needs a resolved sender identity before anything can be
        // authenticated at all.
        let Some(peer_id) = self.peer_id_of(from) else {
            self.stats.dropped_unknown_peer.fetch_add(1, Relaxed);
            self.stats.dropped_auth_failed.fetch_add(1, Relaxed);
            return None;
        };

        // Mixed-mode diagnostic (the brief's own framing: "the likeliest
        // operator error under flag-day rollout"). `key_epoch == 0` is
        // `uc_protocol`'s documented cleartext sentinel
        // (`v2::datagram::OFF_DGRAM_KEY_EPOCH`'s doc: "0 = cleartext") --
        // but `GroupPlane::next_epoch` starts at 0 too, so a fresh mint's
        // FIRST epoch can legitimately BE 0 (see `transport.rs`'s own
        // fixture traps, T9/T10). `key_epoch == 0` alone is therefore not
        // proof of anything. What IS proof: a genuinely cleartext datagram
        // carries no counter/tag, so it is STRICTLY SHORTER than any
        // validly sealed frame could ever be
        // (`n < DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD`) — a shape a real
        // sealed frame, even one sealed under epoch 0, can never take (the
        // minimal empty-payload sealed frame is EXACTLY
        // `DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD` bytes, never less).
        // Requiring BOTH signals means a real epoch-0 sealed datagram is
        // never misdiagnosed; only a datagram that is BOTH stamped 0 AND
        // too short to be lying about being sealed gets this specific
        // diagnostic instead of a generic auth failure.
        if h.key_epoch == 0 && n < DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD {
            self.note_cleartext_peer(from);
            return None;
        }

        let crypto = self.crypto.as_mut().expect("checked Some at the top of this function");
        match crypto.open_slice(peer_id, buf, n) {
            Ok(len) => {
                if h.kind == DGRAM_KIND_HS_KEY {
                    // Sealed, now opened -- route the PLAINTEXT body, not
                    // the wire bytes.
                    self.route_handshake(from, h.kind, buf[DATAGRAM_HEADER_LEN..len].to_vec());
                    None
                } else {
                    Some(len)
                }
            }
            Err(CryptoError::Replayed(_)) => {
                self.stats.dropped_replay.fetch_add(1, Relaxed);
                None
            }
            Err(CryptoError::NoGroupKey) => {
                self.stats.dropped_unknown_epoch.fetch_add(1, Relaxed);
                None
            }
            Err(_) => {
                // AuthFailed (wrong key / tampered), TooShort (past the
                // cleartext-shape check above -- long enough to CLAIM to be
                // sealed but still fails), NoSession (no established
                // pairwise session with this peer yet), UnsealedKind
                // (unreachable here: HS_INIT/HS_RESP already routed above),
                // MissingPeer (unreachable: `open_slice` never needs
                // `peer: Some`) -- every remaining case is "this did not
                // authenticate," the generic bucket the mixed-mode
                // diagnostic above exists to be distinguishable FROM.
                self.stats.dropped_auth_failed.fetch_add(1, Relaxed);
                None
            }
        }
    }

    /// The rate-limited "peer appears cleartext" diagnostic — see
    /// `crypto_admit`'s doc for the discriminating condition. The counter
    /// always increments (undercounting would hide the problem); the
    /// operator-facing `eprintln!` is throttled to once per
    /// [`CLEARTEXT_LOG_INTERVAL_NS`] per peer so a sustained mismatch (a
    /// whole node still on cleartext) cannot spam stderr at datagram rate.
    fn note_cleartext_peer(&mut self, from: SocketAddr) {
        use Ordering::Relaxed;
        self.stats.peer_appears_cleartext.fetch_add(1, Relaxed);
        let now = self.now_ns();
        let due = self
            .cleartext_peer_log
            .get(&from)
            .is_none_or(|&last| now.saturating_sub(last) >= CLEARTEXT_LOG_INTERVAL_NS);
        if due {
            self.cleartext_peer_log.insert(from, now);
            eprintln!(
                "uc2_net: peer {from} appears to be running with crypto disabled (datagram too \
                 short to be a sealed frame, key_epoch=0) -- this node has crypto enabled; check \
                 for a flag-day rollout mismatch"
            );
        }
    }

    /// Forwards a handshake-plane datagram (kind 18/19/20) to
    /// [`FollowerReceiver::set_handshake_route`]'s channel, if installed.
    /// Drops and counts (`dropped_handshake`) otherwise — no route, or a
    /// full/disconnected one (harmless: `Peers::tick`/a retry re-initiates,
    /// same reasoning as the consensus route's own full-channel drops).
    fn route_handshake(&mut self, from: SocketAddr, kind: u8, body: Vec<u8>) {
        let sent = self.hs_route.as_ref().is_some_and(|tx| tx.try_send((from, kind, body)).is_ok());
        if !sent {
            self.stats.dropped_handshake.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Resolves `from` to the `NodeId` [`ReceiveHalf::open_slice`] needs —
    /// see [`FollowerReceiver::set_peer_ids`].
    #[inline]
    fn peer_id_of(&self, from: SocketAddr) -> Option<NodeId> {
        self.peer_ids.get(&from).copied()
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
            config: b.config,
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
                key_epoch: 0,
            },
        );
        write_snap_begin_body(
            &mut d[DATAGRAM_HEADER_LEN..],
            &SnapBeginBody {
                session: intake.session,
                snapshot_pos: intake.snapshot_pos,
                total_len: intake.total_len,
                config: vec![], // the DONE ack carries no config — only SNAP_BEGIN ships it
            },
        );
        let _ = self.sock.send_to(&d, intake.peer);
        // M7 Task 6: publish the carried config BEFORE the position signal — the
        // consensus agent's install handler samples the position (Acquire) and
        // only then reads this cell, so publishing it first (the mutex itself is
        // a release fence) guarantees it sees THIS transfer's bytes, never a
        // stale or absent value from a prior/no session.
        if let Some(cell) = &self.incoming_snapshot_config {
            *cell.lock().unwrap() = intake.config.clone();
        }
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
                    key_epoch: 0,
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
                    key_epoch: 0,
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
                    key_epoch: 0,
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
                    key_epoch: 0,
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
                &DatagramHeader { position, leadership_term_id: term, kind, flags: 0, key_epoch: 0 },
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
        let s = b.recordable_slice(0, 1 << 20).unwrap();
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

    /// M7 Task 7: kinds 16/17 (the admin-forward proposal/reply) route RAW to
    /// the consensus agent exactly like the other consensus kinds — no term
    /// filter, since a follower forwards to whatever leader it currently has a
    /// hint for, and the leader replies to whichever follower forwarded, both
    /// independent of the receiver's own tracked term.
    #[test]
    fn config_kinds_route_raw_to_the_consensus_agent() {
        use uc_protocol::v2::datagram::{
            CONFIG_PROPOSAL_BODY_LEN, CONFIG_REPLY_BODY_LEN, write_config_proposal_body,
            write_config_reply_body,
        };
        let b = buffer();
        let mut leader = FakeLeader::new();
        let (tx, rx) = mpsc::sync_channel::<NetEvent>(16);
        let mut r = follower_routed(&b, leader.addr(), tx);
        let to = r.local_addr();

        let mut pbuf = vec![0u8; CONFIG_PROPOSAL_BODY_LEN];
        write_config_proposal_body(
            &mut pbuf,
            &ConfigProposalBody { nonce: 0x1122_3344, op: 1, id: 9, ip: 0x7F00_0001, port: 4000 },
        );
        // Term deliberately mismatched from the receiver's own — must still route.
        leader.send(to, DGRAM_KIND_CONFIG_PROPOSAL, 0, TERM + 9, &pbuf);

        let mut rbuf = vec![0u8; CONFIG_REPLY_BODY_LEN];
        write_config_reply_body(
            &mut rbuf,
            &ConfigReplyBody { nonce: 0x1122_3344, status: 0, reason: 0, version: 1 },
        );
        leader.send(to, DGRAM_KIND_CONFIG_REPLY, 0, TERM, &rbuf);

        let (mut saw_proposal, mut saw_reply) = (false, false);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !(saw_proposal && saw_reply) {
            assert!(Instant::now() < deadline, "config-forward events never routed");
            r.do_work();
            while let Ok(ev) = rx.try_recv() {
                match ev {
                    NetEvent::ConfigProposal { body, .. } => {
                        assert_eq!(body, ConfigProposalBody {
                            nonce: 0x1122_3344,
                            op: 1,
                            id: 9,
                            ip: 0x7F00_0001,
                            port: 4000,
                        });
                        saw_proposal = true;
                    }
                    NetEvent::ConfigReply { body } => {
                        assert_eq!(
                            body,
                            ConfigReplyBody { nonce: 0x1122_3344, status: 0, reason: 0, version: 1 }
                        );
                        saw_reply = true;
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

    // ----------------------------------------------------------- M8 (Task 11)
    // The receive seam: `crypto_admit` decrypts (or diagnoses/drops) BEFORE
    // `on_datagram` ever sees the bytes, so every parser above (proven by
    // the 19 tests above this section, unmodified) stays completely unaware
    // crypto exists. This section builds a real, two-node, established
    // Noise-IK session + a real minted-and-delivered group key over a
    // genuine UDP socket pair, using ONLY `uc2_crypto`'s public API (the
    // `initiate`/`on_handshake_message`/`on_group_key_message` forwarders
    // T11 added to `SharedTransport` for exactly this — see that crate's
    // `transport.rs` module docs for why nothing outside it could do this
    // before this task).

    use std::sync::atomic::AtomicU64 as StdAtomicU64;

    const PEER_ID: NodeId = 1; // the fake leader
    const RECV_ID: NodeId = 2; // the receiver under test
    const PRIV_PEER: [u8; 32] = [0x11; 32];
    const PRIV_RECV: [u8; 32] = [0x22; 32];

    fn crypto_scratch_dir(tag: &str) -> PathBuf {
        static SEQ: StdAtomicU64 = StdAtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::var("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc2_net_tests")
            })
            .join("uc2-net-receiver-crypto")
            .join(format!("{tag}-{seq}"));
        std::fs::create_dir_all(&dir).unwrap();
        assert!(!dir.starts_with("/tmp"), "test scratch must not live on tmpfs: {dir:?}");
        dir
    }

    fn write_key_file(path: &std::path::Path, private: [u8; 32]) {
        std::fs::write(path, private).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

    /// Derives a node's public key from its raw private key bytes, via a
    /// throwaway `uc2_crypto::identity::Identity` — `uc2_net` has no X25519
    /// dependency of its own (nor should it gain one just for test fixture
    /// plumbing); `Identity::public_bytes` is already the crate's own public
    /// accessor for exactly this.
    fn identity_public(tag: &str, private: [u8; 32]) -> [u8; 32] {
        let dir = crypto_scratch_dir(tag);
        let key_path = dir.join("node.key");
        write_key_file(&key_path, private);
        uc2_crypto::identity::Identity::load(&key_path).unwrap().public_bytes()
    }

    /// Minimal standard-alphabet base64 WITH padding, matching
    /// `uc2_crypto::identity`'s allowlist parser (which uses the `base64`
    /// crate's `STANDARD` engine) — hand-rolled here rather than adding a
    /// `base64` dev-dependency to `uc2_net` just for one test fixture's
    /// allowlist-file text. 32 bytes in, 44 base64 chars out (one trailing
    /// `=`), same as any other X25519 public key this codebase writes.
    fn b64_32(bytes: &[u8; 32]) -> String {
        const ALPHABET: &[u8] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = *chunk.get(1).unwrap_or(&0) as u32;
            let b2 = *chunk.get(2).unwrap_or(&0) as u32;
            let n = (b0 << 16) | (b1 << 8) | b2;
            out.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
            out.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
            out.push(if chunk.len() > 1 { ALPHABET[((n >> 6) & 0x3F) as usize] as char } else { '=' });
            out.push(if chunk.len() > 2 { ALPHABET[(n & 0x3F) as usize] as char } else { '=' });
        }
        out
    }

    fn crypto_shared(
        tag: &str,
        self_id: NodeId,
        private: [u8; 32],
        allow: &[(NodeId, [u8; 32])],
    ) -> uc2_crypto::SharedTransport {
        let dir = crypto_scratch_dir(tag);
        let key_path = dir.join("node.key");
        write_key_file(&key_path, private);
        let allow_path = dir.join("allowlist");
        let mut text = String::new();
        for (id, public) in allow {
            text.push_str(&format!("{id} {}\n", b64_32(public)));
        }
        std::fs::write(&allow_path, text).unwrap();
        let cfg = uc2_crypto::CryptoConfig::Enabled {
            key_path,
            allowlist_path: allow_path,
            rotation: uc2_crypto::rotation::RotationPolicy::default(),
        };
        uc2_crypto::SharedTransport::new(&cfg, self_id).unwrap().unwrap()
    }

    /// Builds a real PEER (`PEER_ID`) and RECEIVER (`RECV_ID`) `SharedTransport`
    /// pair, drives a genuine Noise-IK handshake between them to completion,
    /// then mints a group key on the peer and delivers it to the receiver —
    /// all through `SharedTransport`'s public forwarders only (`initiate`/
    /// `on_handshake_message`/`on_group_key_message`), never by reaching
    /// into private crate internals (this IS a different crate). Returns
    /// `(receiver's SharedTransport, peer's SendHalf, the real minted epoch)`.
    fn established_crypto_pair(tag: &str) -> (uc2_crypto::SharedTransport, uc2_crypto::SendHalf, u16) {
        let peer_pub = identity_public(&format!("{tag}-peer-pub"), PRIV_PEER);
        let recv_pub = identity_public(&format!("{tag}-recv-pub"), PRIV_RECV);
        let peer = crypto_shared(
            &format!("{tag}-peer"),
            PEER_ID,
            PRIV_PEER,
            &[(PEER_ID, peer_pub), (RECV_ID, recv_pub)],
        );
        let recv = crypto_shared(
            &format!("{tag}-recv"),
            RECV_ID,
            PRIV_RECV,
            &[(PEER_ID, peer_pub), (RECV_ID, recv_pub)],
        );

        let mut acts = peer.initiate(RECV_ID, 0);
        for _ in 0..8 {
            let mut next = Vec::new();
            for act in acts.drain(..) {
                if let uc2_crypto::HandshakeAction::Send { to, kind, body } = act {
                    if to == RECV_ID {
                        next.extend(recv.on_handshake_message(PEER_ID, kind, &body, 0));
                    } else if to == PEER_ID {
                        next.extend(peer.on_handshake_message(RECV_ID, kind, &body, 0));
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            acts = next;
        }

        // A vacuous throwaway mint first: `GroupPlane::next_epoch` starts at
        // 0 on a fresh process, so the FIRST-EVER mint's epoch is 0 --
        // indistinguishable from a header's zero-initialized `key_epoch`
        // field. Same trap `uc2_crypto::transport`'s own tests name
        // explicitly; hit for real in this harness's first draft.
        let _ = peer.mint_group_key(&[], 0);
        let (epoch, mint_acts) = peer.mint_group_key(&[RECV_ID], 0);
        assert_ne!(epoch, 0, "fixture must not accidentally observe the zero-init epoch");
        for act in mint_acts {
            let uc2_crypto::HandshakeAction::Send { to, body, .. } = act else {
                panic!("mint must emit a Send action")
            };
            assert_eq!(to, RECV_ID);
            let reply = recv.on_group_key_message(PEER_ID, &body);
            for r in reply {
                let uc2_crypto::HandshakeAction::Send { body: rbody, .. } = r else {
                    panic!("a well-formed delivery must ack back")
                };
                peer.on_group_key_message(RECV_ID, &rbody);
            }
        }

        let send = peer.send_half();
        (recv, send, epoch)
    }

    /// A crypto-capable fake leader endpoint: a real, established
    /// `uc2_crypto::SendHalf` plus a raw socket, able to send both
    /// correctly-sealed traffic and deliberately malformed/forged/replayed
    /// datagrams for T11's negative tests.
    struct CryptoPeer {
        sock: FaultSocket,
        send: uc2_crypto::SendHalf,
        epoch: u16,
    }

    impl CryptoPeer {
        fn header(position: u64, kind: u8, key_epoch: u16) -> DatagramHeader {
            DatagramHeader { position, leadership_term_id: TERM, kind, flags: 0, key_epoch }
        }

        /// A real, correctly-sealed DATA datagram — exactly what a peer with
        /// crypto enabled and an established session sends in production.
        fn send_sealed_data(&mut self, to: SocketAddr, position: u64, payload: &[u8]) {
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
            write_datagram_header(&mut d, &Self::header(position, DGRAM_KIND_DATA, 0));
            d.extend_from_slice(payload);
            let now = self.send.now_ns();
            self.send.seal(DGRAM_KIND_DATA, None, &mut d, now).unwrap();
            self.sock.send_to(&d, to).unwrap();
        }

        /// The header claims the REAL active epoch (so the receiver's
        /// schedule lookup succeeds and reaches the AEAD check at all), but
        /// the bytes are sealed under an unrelated, made-up key nobody
        /// installed — an on-path forgery, not a replay or an unknown epoch.
        fn send_sealed_with_wrong_key(&mut self, to: SocketAddr, position: u64, payload: &[u8]) {
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
            write_datagram_header(&mut d, &Self::header(position, DGRAM_KIND_DATA, self.epoch));
            d.extend_from_slice(payload);
            uc2_crypto::seal::seal_in_place(&mut d, &[0x99u8; 32], 1).unwrap();
            self.sock.send_to(&d, to).unwrap();
        }

        /// Sealed (under an arbitrary key -- it never gets that far), but
        /// stamped with an epoch the receiver never minted or received.
        fn send_sealed_under_epoch(&mut self, to: SocketAddr, epoch: u16, position: u64, payload: &[u8]) {
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
            write_datagram_header(&mut d, &Self::header(position, DGRAM_KIND_DATA, epoch));
            d.extend_from_slice(payload);
            uc2_crypto::seal::seal_in_place(&mut d, &[0x77u8; 32], 1).unwrap();
            self.sock.send_to(&d, to).unwrap();
        }

        /// The old, pre-M8 cleartext wire shape: no counter, no tag,
        /// `key_epoch` left at its zero-init default. Exactly what a peer
        /// running with crypto disabled sends.
        fn send_cleartext_data(&mut self, to: SocketAddr, position: u64, payload: &[u8]) {
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
            write_datagram_header(&mut d, &Self::header(position, DGRAM_KIND_DATA, 0));
            d.extend_from_slice(payload);
            self.sock.send_to(&d, to).unwrap();
        }

        /// Byte-for-byte capture-and-resend, or arbitrary garbage.
        fn send_raw(&mut self, to: SocketAddr, bytes: &[u8]) {
            self.sock.send_to(bytes, to).unwrap();
        }
    }

    /// The T11 fixture: a `FollowerReceiver` with crypto enabled, a real
    /// established `CryptoPeer`, and the shared `LogBuffer` (so tests can
    /// check whether admitted DATA actually landed).
    fn receiver_with_crypto() -> (FollowerReceiver, CryptoPeer, Arc<LogBuffer>) {
        let (recv_shared, peer_send, epoch) = established_crypto_pair("recv-with-crypto");
        let b = buffer();
        let peer_sock = FaultSocket::bind("127.0.0.1:0").unwrap();
        let peer_addr = peer_sock.local_addr().unwrap();

        let mut cfg = FollowerConfig::new(peer_addr);
        cfg.nak = NakConfig { delay_min_ns: 1, delay_max_ns: 2, backoff_ns: 1_000_000 };
        cfg.status_floor_ns = u64::MAX;
        cfg.append_pos_floor_ns = u64::MAX;

        let recv_half = recv_shared.receive_half();
        let mut r = FollowerReceiver::with_crypto(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            cfg,
            term_handle(TERM),
            dummy_route(),
            Some(recv_half),
        );
        r.set_peer_ids([(peer_addr, PEER_ID)]);

        (r, CryptoPeer { sock: peer_sock, send: peer_send, epoch }, b)
    }

    #[test]
    fn a_sealed_datagram_opens_and_dispatches_exactly_as_cleartext_did() {
        let (mut r, mut peer, b) = receiver_with_crypto();
        let to = r.local_addr();
        let runs = frame_runs(&[b"aaaa", b"bb", b"cccccc"], 4096);
        let (pos, bytes, advance) = &runs[0];
        peer.send_sealed_data(to, *pos, bytes);
        drive_until(&mut r, || b.counters().append.load_acquire() == *advance);
        let s = b.recordable_slice(0, 1 << 20).unwrap();
        assert_eq!(&s[32..36], b"aaaa", "downstream sees plaintext, byte-identical to the cleartext path");
    }

    #[test]
    fn a_forged_datagram_under_an_unknown_key_is_dropped_and_counted() {
        use Ordering::Relaxed;
        let (mut r, mut peer, b) = receiver_with_crypto();
        let to = r.local_addr();
        peer.send_sealed_with_wrong_key(to, 0, b"forged-payload");
        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_auth_failed.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "forged datagram never counted");
            r.do_work();
        }
        for _ in 0..50 {
            r.do_work();
        }
        assert_eq!(st.dropped_auth_failed.load(Relaxed), 1);
        assert_eq!(b.counters().append.load_acquire(), 0, "forged bytes never reach the log buffer");
    }

    #[test]
    fn a_replayed_datagram_is_dropped_and_counted() {
        use Ordering::Relaxed;
        let (mut r, mut peer, b) = receiver_with_crypto();
        let to = r.local_addr();
        let runs = frame_runs(&[b"aaaa"], 4096);
        let (pos, bytes, advance) = &runs[0];

        // Capture the exact sealed wire bytes by having the peer send once,
        // draining it off a mirror socket bound to the same recv address is
        // not needed -- send twice with the SAME counter would be a cleaner
        // "replay," but `SendHalf::seal` always advances the counter. Build
        // the sealed datagram once by hand (same shape `send_sealed_data`
        // uses) so the identical bytes can be captured and resent verbatim.
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(&mut d, &CryptoPeer::header(*pos, DGRAM_KIND_DATA, 0));
        d.extend_from_slice(bytes);
        let now = peer.send.now_ns();
        peer.send.seal(DGRAM_KIND_DATA, None, &mut d, now).unwrap();

        peer.send_raw(to, &d);
        drive_until(&mut r, || b.counters().append.load_acquire() == *advance);

        let st = r.stats();
        // Baseline BEFORE the replay, not just "append unchanged after" --
        // a mutant that treats a failed replay check as SUCCESS still
        // leaves `append` unchanged in this fixture, because the
        // AEAD-decrypted-but-wrongly-admitted bytes carry the ALREADY-
        // consumed position and `on_datagram`'s own `h.position < contiguous`
        // dup guard (or, depending on exact byte layout, its malformed-frame
        // guard) also happens to reject it -- "append didn't move" is true
        // either way, so it does not by itself prove the REPLAY check (not
        // some unrelated downstream guard) is what caught this. Pin that the
        // datagram is dropped HERE, before `on_datagram`, by checking that
        // NEITHER of `on_datagram`'s own drop counters moved at all -- if a
        // future change let a replayed-but-decrypted datagram reach
        // `on_datagram`, one of those would tick even though `append` stays
        // put.
        let dup0 = st.dropped_dup.load(Relaxed);
        let malformed0 = st.dropped_malformed.load(Relaxed);

        peer.send_raw(to, &d); // byte-for-byte capture and resend

        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_replay.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "replayed datagram never counted");
            r.do_work();
        }
        for _ in 0..20 {
            r.do_work();
        }
        assert_eq!(st.dropped_replay.load(Relaxed), 1);
        assert_eq!(b.counters().append.load_acquire(), *advance, "the replay did not double-apply");
        assert_eq!(
            st.dropped_dup.load(Relaxed), dup0,
            "the replay must never reach on_datagram at all, not merely be re-rejected there as a dup"
        );
        assert_eq!(
            st.dropped_malformed.load(Relaxed), malformed0,
            "the replay must never reach on_datagram at all, not merely be re-rejected there as malformed"
        );
    }

    #[test]
    fn a_cleartext_peer_is_diagnosed_specifically_not_as_a_generic_auth_failure() {
        // The likeliest operator error under flag-day rollout.
        use Ordering::Relaxed;
        let (mut r, mut peer, b) = receiver_with_crypto();
        let to = r.local_addr();
        peer.send_cleartext_data(to, 0, b"frames"); // key_epoch == 0, no counter/tag
        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.peer_appears_cleartext.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "cleartext peer never diagnosed");
            r.do_work();
        }
        for _ in 0..50 {
            r.do_work();
        }
        assert_eq!(st.peer_appears_cleartext.load(Relaxed), 1);
        assert_eq!(st.dropped_auth_failed.load(Relaxed), 0, "distinguishable from a generic auth failure");
        assert_eq!(b.counters().append.load_acquire(), 0);
    }

    #[test]
    fn an_unknown_epoch_is_dropped_without_killing_the_node() {
        use Ordering::Relaxed;
        let (mut r, mut peer, b) = receiver_with_crypto();
        let to = r.local_addr();
        peer.send_sealed_under_epoch(to, 999, 0, b"frames");
        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_unknown_epoch.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "unknown-epoch datagram never counted");
            r.do_work();
        }
        assert_eq!(st.dropped_unknown_epoch.load(Relaxed), 1);
        assert_eq!(b.counters().append.load_acquire(), 0);
        // "without killing the node" -- the receiver keeps functioning for
        // legitimate traffic afterwards.
        peer.send_sealed_data(to, 0, &frame_runs(&[b"ok"], 4096)[0].1);
        let advance = frame_runs(&[b"ok"], 4096)[0].2;
        drive_until(&mut r, || b.counters().append.load_acquire() == advance);
    }

    #[test]
    fn truncated_and_random_datagrams_never_panic() {
        // Anyone who can reach the port must not be able to kill the node.
        // The brief's own trace list: zero-length, 15-byte, 16-byte,
        // all-zeroes, and 64 KiB of garbage -- covered explicitly below,
        // plus a broader length sweep for good measure.
        let (mut r, mut peer, _b) = receiver_with_crypto();
        let to = r.local_addr();
        for len in [0usize, 1, 15, 16, 17, 39, 40, 1500] {
            peer.send_raw(to, &vec![0xABu8; len]);
            for _ in 0..3 {
                r.do_work();
            }
        }
        // All-zeroes, at both the header-only and a payload-bearing length
        // -- byte 12 (`kind`) is 0 (an unrecognized kind, `Scope::Pairwise`
        // by `scope_of`'s catch-all), `key_epoch` (bytes 14-15) is 0 too, so
        // this also exercises the mixed-mode cleartext-shape check's other
        // branch (the SHORT all-zero case) alongside the plain auth-failure
        // path (the LONG one, long enough to not trip that check).
        for len in [15usize, 16, 40, 1500] {
            peer.send_raw(to, &vec![0u8; len]);
            for _ in 0..3 {
                r.do_work();
            }
        }
        // A 64 KiB garbage datagram too, since the receive buffer is
        // exactly that size -- the boundary the "never memset the whole
        // buffer" property lives at.
        peer.send_raw(to, &vec![0xCDu8; 65_000]);
        for _ in 0..3 {
            r.do_work();
        }
    }

    #[test]
    fn a_datagram_from_an_unregistered_address_is_dropped_and_counted_not_authenticated() {
        // Every other test's peer is registered via `set_peer_ids` (built
        // into `receiver_with_crypto`). Nothing exercises the OTHER branch
        // of `peer_id_of` -- a real sealed-LOOKING datagram from a
        // `SocketAddr` this receiver has no `NodeId` mapping for -- until
        // this test. Uses a SECOND, entirely unregistered socket sending
        // the exact same sealed bytes a legitimate peer would.
        use Ordering::Relaxed;
        let (mut r, mut peer, b) = receiver_with_crypto();
        let to = r.local_addr();

        // A genuinely well-formed sealed DATA datagram (peer's real
        // SendHalf, real established session) -- the ONLY thing wrong with
        // it is who it arrives FROM.
        let runs = frame_runs(&[b"aaaa"], 4096);
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(&mut d, &CryptoPeer::header(runs[0].0, DGRAM_KIND_DATA, 0));
        d.extend_from_slice(&runs[0].1);
        let now = peer.send.now_ns();
        peer.send.seal(DGRAM_KIND_DATA, None, &mut d, now).unwrap();

        let mut stranger = FaultSocket::bind("127.0.0.1:0").unwrap();
        stranger.send_to(&d, to).unwrap();

        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_unknown_peer.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "datagram from an unregistered address never counted");
            r.do_work();
        }
        assert_eq!(st.dropped_unknown_peer.load(Relaxed), 1);
        assert_eq!(st.dropped_auth_failed.load(Relaxed), 1, "folded into the mandated auth-failed bucket too");
        assert_eq!(b.counters().append.load_acquire(), 0, "an unresolvable sender's bytes never land");
    }

    #[test]
    fn snap_datagrams_are_admitted_cleartext_even_with_crypto_enabled() {
        // The T17 temporary allowance (`crypto_admit`'s "T17 TEMPORARY
        // ALLOWANCE" comment, grep "T17"): SNAP_BEGIN/SNAP_CHUNK ship
        // cleartext until Task 17 seals the remaining pairwise sends.
        // Dropping them here would wedge snapshot transfer with crypto ON —
        // a learner, a cold node, or a below-floor follower can ONLY
        // converge via a snapshot session. Proven by `stats.datagrams`
        // (bumped in `on_datagram` for every non-consensus, current-term
        // datagram BEFORE the kind-specific dispatch) ticking at all — a
        // raw, unsealed SNAP_BEGIN reaching `on_datagram` proves
        // `crypto_admit` let it through without attempting to authenticate
        // it; SNAP_BEGIN's own body-too-short guard then drops it for an
        // entirely unrelated (non-crypto) reason, since this fixture never
        // configures snapshot intake — irrelevant to what this test pins.
        use Ordering::Relaxed;
        let (mut r, mut peer, _b) = receiver_with_crypto();
        let to = r.local_addr();
        let st = r.stats();
        let before = st.datagrams.load(Relaxed);

        let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(&mut d, &CryptoPeer::header(0, DGRAM_KIND_SNAP_BEGIN, 0));
        peer.send_raw(to, &d);

        let deadline = Instant::now() + Duration::from_secs(5);
        while st.datagrams.load(Relaxed) <= before {
            assert!(
                Instant::now() < deadline,
                "an unsealed SNAP_BEGIN never reached on_datagram -- the T17 allowance is broken \
                 (or crypto is wrongly authenticating SNAP traffic before T17 lands)"
            );
            r.do_work();
        }
        // And it was genuinely NOT run through authentication -- none of the
        // crypto drop counters fired for it.
        assert_eq!(st.dropped_auth_failed.load(Relaxed), 0);
        assert_eq!(st.dropped_unknown_epoch.load(Relaxed), 0);
        assert_eq!(st.peer_appears_cleartext.load(Relaxed), 0);
    }

    #[test]
    fn crypto_disabled_receiver_is_unaffected_by_the_new_seam() {
        // `crypto_admit` must be a complete pass-through when `crypto` is
        // `None` -- pins the "byte-for-byte the pre-M8 path" claim in its
        // own doc comment, at the seam, not just via the 19 unmodified
        // tests above (which never construct a crypto-enabled receiver at
        // all and so can't by themselves prove the new code path is inert).
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let runs = frame_runs(&[b"aaaa"], 4096);
        leader.send(to, DGRAM_KIND_DATA, runs[0].0, TERM, &runs[0].1);
        drive_until(&mut r, || b.counters().append.load_acquire() == runs[0].2);
    }
}
