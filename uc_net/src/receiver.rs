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
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use uc_crypto::{CryptoError, NodeId, ReceiveHalf};
use uc_log::buffer::LogBuffer;
use uc_log::writer::PositionedWriter;
use uc_protocol::v2::crypto::{
    CRYPTO_OVERHEAD, DGRAM_KIND_HS_INIT, DGRAM_KIND_HS_KEY, DGRAM_KIND_HS_RESP,
};
use uc_protocol::v2::datagram::{
    APPEND_POSITION_BODY_LEN, AppendPositionBody, ConfigProposalBody, ConfigReplyBody,
    DATAGRAM_HEADER_LEN, DGRAM_KIND_APPEND_POSITION, DGRAM_KIND_COMMIT_POSITION,
    DGRAM_KIND_CONFIG_PROPOSAL, DGRAM_KIND_CONFIG_REPLY, DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT,
    DGRAM_KIND_NAK, DGRAM_KIND_READ_PROBE, DGRAM_KIND_READ_PROBE_ACK, DGRAM_KIND_REQUEST_VOTE,
    DGRAM_KIND_SNAP_BEGIN, DGRAM_KIND_SNAP_CHUNK, DGRAM_KIND_SNAP_DONE, DGRAM_KIND_SNAP_NAK,
    DGRAM_KIND_STATUS, DGRAM_KIND_TERM_MAP, DGRAM_KIND_VOTE, DatagramHeader,
    MAX_TERM_MAP_WIRE_ENTRIES, NAK_BODY_LEN, NakBody, REQUEST_VOTE_BODY_LEN, RequestVoteBody,
    SNAP_BEGIN_FIXED_LEN, SNAP_BEGIN_LAYOUT_V3, SNAP_NAK_BODY_LEN, STATUS_BODY_LEN, SnapBeginBody,
    SnapNakBody, StatusBody, TermMapEntryWire, VOTE_BODY_LEN, VoteBody, read_append_position_body,
    read_config_proposal_body, read_config_reply_body, read_datagram_header, read_nak_body,
    read_read_probe_body, read_request_vote_body, read_snap_begin_body, read_snap_nak_body,
    read_status_body, read_term_map_body, read_vote_body, write_append_position_body,
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
/// it — see `crypto_admit`. Nothing in `uc_net` drives the actual handshake
/// state machine from this route yet; that is Task 12's node-layer wiring
/// (`uc_crypto::SharedTransport::initiate`/`on_handshake_message`/
/// `on_group_key_message`, T11's own plan-gap addition). Until a route is
/// supplied in [`CryptoIntake::handshake`] (T12 made it a constructor
/// argument, not an optional setter), these are dropped and counted
/// (`FollowerStats::dropped_handshake`) — never silently absorbed, and never
/// fed to `on_datagram`, which has no idea what an HS_INIT is.
pub type HandshakeDatagram = (SocketAddr, u8, Vec<u8>);

/// M8 (Task 12): the live `SocketAddr -> NodeId` map `crypto_admit` resolves
/// a datagram's sender through before it can authenticate anything, shared
/// with whoever owns membership (the node's consensus agent).
///
/// **Shared and versioned, not a snapshot.** Task 11 took the map by value at
/// construction, which is correct only for a cluster whose membership never
/// changes — and M7 changes membership at runtime. After an `add-learner`
/// commits, a node that still held the boot-time map would resolve nothing
/// for the joiner and drop every datagram it sends as `dropped_unknown_peer`
/// until the whole cluster restarted, defeating the very case §5 of the spec
/// exists to serve (the allowlist half of which is handled by
/// `Peers::allowlist_reload_if_stale`).
///
/// The receiver re-reads the map only when `generation` changes — one
/// `Relaxed` load per duty cycle (not per datagram), and the `Mutex` is
/// touched only on an actual membership change. `store` is the sole writer's
/// entry point; it replaces the map wholesale and then bumps `generation`
/// (that order matters: a reader that observes the new generation is
/// guaranteed to find the new map behind it).
#[derive(Clone, Default)]
pub struct PeerIds {
    generation: Arc<AtomicU64>,
    map: Arc<Mutex<HashMap<SocketAddr, NodeId>>>,
}

impl PeerIds {
    /// An empty map at generation 0.
    pub fn new() -> PeerIds {
        PeerIds::default()
    }

    /// Replaces the whole map and publishes it (bumping `generation`, which
    /// is what makes a running receiver pick it up). Called by the node's
    /// consensus agent at boot and on every adopted `ClusterConfig`.
    pub fn store(&self, ids: impl IntoIterator<Item = (SocketAddr, NodeId)>) {
        let next: HashMap<SocketAddr, NodeId> = ids.into_iter().collect();
        *self.map.lock().unwrap() = next;
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// A copy of the current map — for tests and diagnostics, never on a
    /// per-datagram path.
    pub fn snapshot(&self) -> HashMap<SocketAddr, NodeId> {
        self.map.lock().unwrap().clone()
    }

    /// The publication generation — bumped by `store`. A mirroring agent
    /// re-snapshots only when this changes (one `Acquire` load per duty
    /// cycle). `pub` since T17: `uc_net::sender::Sender` mirrors this map
    /// too, for the pairwise snapshot seals.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

/// M8 (Task 12): everything the receive path needs to run with crypto on,
/// taken as ONE value so none of the three pieces can be forgotten.
///
/// Task 11 shipped the half as a constructor argument and the other two as
/// separate `set_peer_ids`/`set_handshake_route` setters, documented but not
/// enforced ("if T12 forgets `set_peer_ids`, every crypto-scoped datagram
/// silently becomes `dropped_unknown_peer`" — T11's own hand-off note).
/// A silent, total, cluster-wide failure that nothing catches is not a
/// documentation problem; bundling the three makes the mistake a compile
/// error instead.
pub struct CryptoIntake {
    /// The process's single [`ReceiveHalf`] (`SharedTransport::receive_half`).
    pub half: ReceiveHalf,
    /// The live sender-identity map — see [`PeerIds`].
    pub peer_ids: PeerIds,
    /// Where handshake-plane datagrams (kinds 18/19/20) go — see
    /// [`HandshakeDatagram`]. Nothing in `uc_net` drives the handshake state
    /// machine; the node layer does.
    pub handshake: mpsc::SyncSender<HandshakeDatagram>,
    /// M8 (Task 17): the process's `SharedTransport`, for SEALING this
    /// receiver's own outgoing control datagrams (`NAK`, `STATUS`,
    /// `APPEND_POSITION`, `SNAP_NAK`, `SNAP_DONE` — every one of them
    /// `Scope::Pairwise`).
    ///
    /// A `SharedTransport` clone and NOT a second `SendHalf`, deliberately:
    /// `SharedTransport::send_half` is single-call by design and the one half
    /// went to the sender agent. A second half would start a second nonce
    /// counter at 0 under the SAME per-peer session key the sender already
    /// seals under — a repeated `(key, nonce)` pair under AES-256-GCM, which
    /// leaks the authentication subkey for every message ever sealed under
    /// that key, not just the repeat. `SharedTransport::seal_pairwise_control`
    /// draws from the process's one shared `Arc<AtomicU64>` instead.
    ///
    /// Locking here is free at these rates: NAK/STATUS/APPEND_POSITION are
    /// per-duty-cycle-at-most control traffic (kHz), not the per-datagram
    /// receive path, and the snapshot kinds are rarer still.
    pub transport: uc_crypto::SharedTransport,
}

/// Consensus-plane events demuxed off the shared UDP socket and routed to the
/// consensus agent (Task 8) over the [`FollowerReceiver::new`] constructor's
/// mandatory route. Kinds 5–11 forward RAW — carrying their own term so the
/// state machine, not the data plane, does term filtering and adoption (a
/// higher-term `RequestVote` MUST reach the SM). `LeaderActivity` is the data
/// plane's rate-limited liveness signal: current-term DATA/HEARTBEAT was seen
/// this duty cycle, so the SM should not time out the leader.
#[derive(Debug, Clone)]
pub enum NetEvent {
    /// A peer's durable report. `durable_term` is the content attestation
    /// added in protocol 0.5.0 (the term the sender attributes to the byte
    /// below `durable`); `0` means unattested — an empty log, or a pre-0.5.0
    /// peer whose report is header-only.
    Report {
        from: SocketAddr,
        term: u32,
        durable: u64,
        durable_term: u32,
    },
    CommitGossip {
        from: SocketAddr,
        term: u32,
        commit: u64,
    },
    RequestVote {
        from: SocketAddr,
        body: RequestVoteBody,
    },
    Vote {
        from: SocketAddr,
        body: VoteBody,
    },
    TermMap {
        from: SocketAddr,
        term: u32,
        entries: Vec<TermMapEntryWire>,
    },
    /// Read-barrier probe (M5 §7), leader → follower. `from` is the leader's
    /// node id (from the body — the reply is addressed by it), `term` is the
    /// datagram HEADER term the follower must match to ACK (the stale-leader
    /// filter). Routed RAW like the other consensus kinds.
    ReadProbe {
        nonce: u64,
        from: u32,
        term: u32,
    },
    /// Read-barrier ack, follower → leader. `from` is the acking follower's node
    /// id (from the body); the leader counts distinct ackers per nonce. No term
    /// field: the nonce is unique to one leader's read round, so a matching
    /// pending nonce already scopes the ack to this leader.
    ReadProbeAck {
        nonce: u64,
        from: u32,
    },
    /// Any current-term leader traffic (data/heartbeat) seen — liveness.
    LeaderActivity {
        term: u32,
    },
    /// M7 Task 7: a follower's forwarded membership proposal (kind 16, `uc2ctl`'s
    /// admin request forwarded by a non-leader that has a leader hint). `from` is
    /// the forwarding follower's address — the leader's reply (kind 17) is
    /// addressed back to it. Routed RAW like the other consensus kinds: a stale/
    /// not-yet-leader node just drops it (the follower's forward times out).
    ConfigProposal {
        from: SocketAddr,
        body: ConfigProposalBody,
    },
    /// M7 Task 7: the leader's reply to a forwarded proposal (kind 17),
    /// follower-bound. Matched by the follower's 1-slot pending map on `nonce`.
    ConfigReply {
        body: ConfigReplyBody,
    },
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
            Some(NetEvent::Report {
                from,
                term: h.leadership_term_id,
                durable: h.position,
                // Absent body (pre-0.5.0 peer) decodes as unattested.
                durable_term: read_append_position_body(body).map_or(0, |b| b.durable_term),
            })
        }
        DGRAM_KIND_COMMIT_POSITION => Some(NetEvent::CommitGossip {
            from,
            term: h.leadership_term_id,
            commit: h.position,
        }),
        DGRAM_KIND_REQUEST_VOTE if body.len() >= REQUEST_VOTE_BODY_LEN => {
            Some(NetEvent::RequestVote {
                from,
                body: read_request_vote_body(body)?,
            })
        }
        DGRAM_KIND_VOTE if body.len() >= VOTE_BODY_LEN => Some(NetEvent::Vote {
            from,
            body: read_vote_body(body)?,
        }),
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
            Some(NetEvent::ReadProbe {
                nonce: b.nonce,
                from: b.from,
                term: h.leadership_term_id,
            })
        }
        DGRAM_KIND_READ_PROBE_ACK => {
            let b = read_read_probe_body(body)?;
            Some(NetEvent::ReadProbeAck {
                nonce: b.nonce,
                from: b.from,
            })
        }
        DGRAM_KIND_CONFIG_PROPOSAL => Some(NetEvent::ConfigProposal {
            from,
            body: read_config_proposal_body(body)?,
        }),
        DGRAM_KIND_CONFIG_REPLY => Some(NetEvent::ConfigReply {
            body: read_config_reply_body(body)?,
        }),
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

/// M14c2 (T10a): an inbound snapshot intake that has seen no chunk for this
/// long is abandoned — its pre-sized `.part` files unlinked and the
/// abandonment counted ([`FollowerStats::snap_intake_abandoned`]).
///
/// Closes the M14c deferral *"a receiver-side intake timeout (a dead leader
/// leaves a full `.part` until the next session)"*: `open_snap_part` `set_len`s
/// each artifact to its FULL announced size, so a leader that dies mid-transfer
/// (or whose session is refused at its own end) leaves real disk consumed under
/// `snapshots/<id>/` with nothing that ever reclaims it — the only pre-existing
/// cleanup is a LATER session replacing the intake, which a joiner that never
/// gets one never sees.
///
/// Deliberately twice the sender's own `SNAP_SESSION_TIMEOUT_NS` (30 s): the
/// leader gives up first and re-cycles the slot, so this fires only when the
/// leader is gone rather than racing its normal recovery. Measured from the
/// last chunk, or from the newest artifact's `SNAP_BEGIN` — a slow but LIVE
/// transfer keeps refreshing it, however large the artifact or lossy the link.
pub const SNAP_INTAKE_TIMEOUT_NS: u64 = 60_000_000_000; // 60s

/// M14c2 (T10a): the minimum spacing between two attempts to publish an
/// intake's completed-but-unrenamed artifacts, per intake.
///
/// Closes M14c ruling M(1): `upkeep` calls [`FollowerReceiver::snap_upkeep`]
/// TWICE per poll iteration and the re-drive had no backoff, so a PERSISTENT
/// obstacle (a full or read-only snapshot dir, a stray directory on the final
/// path) cost one to two failing `fsync`/`rename` syscalls per busy-spin
/// iteration and made `snap_intake_io_failures` climb at the poll rate — while
/// the operator docs say to read a RISING count as the signal. Paced, the
/// counter reads as "one per quarter second the obstacle has been in place",
/// and clearing the obstacle still publishes within a quarter second.
pub const SNAP_REDRIVE_INTERVAL_NS: u64 = 250_000_000; // 250ms

/// Which half of a `SNAP_BEGIN` positional comparison failed — see
/// [`FollowerStats::identity_refusal`] / [`FollowerStats::version_refusal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusalKind {
    /// Row `r`'s FSM name hash disagreed between the two sides' `identity`
    /// arrays.
    Identity,
    /// The two sides' `identity` arrays agreed at every row, but the
    /// artifact's row is outside the sender's declared mask; `row` is the
    /// offending `service_id` clamped to 7. Distinct from `Identity` because
    /// a positional-array mismatch and an out-of-mask artifact id are
    /// different failures with the same counter but different causes.
    ArtifactId,
    /// Row `r`'s name agreed but both sides' nonzero packed version disagreed.
    Version,
}

/// Detail of the most recent `SNAP_BEGIN` identity/version refusal — wire
/// 0.7.0 (spec §5, §8). `row` is the first row at which the two sides'
/// arrays disagreed; `ours`/`theirs` are that row's identity hash on each
/// side; `ours_version`/`theirs_version` are that row's packed version
/// (0 = unknown; only populated meaningfully for `RefusalKind::Version`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityRefusal {
    pub row: u8,
    pub ours: u64,
    pub theirs: u64,
    pub ours_version: u32,
    pub theirs_version: u32,
    pub kind: RefusalKind,
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
    /// Term-change discards: times the receiver dropped its out-of-order runs
    /// because the term moved. A term boundary re-frames the stream above the
    /// commit point, so runs recorded under the old term may disagree with the
    /// new term's framing at the same positions — and `Rebuilt` unions spans,
    /// it does not resolve them. See
    /// [`FollowerReceiver::discard_ooo_on_term_change`].
    pub term_change_discards: AtomicU64,
    /// Times the receive frontier was rebased UP to the shared `append`
    /// counter because something else had moved it ahead of us — in practice
    /// this node's own appender during a leader stint. See
    /// [`FollowerReceiver::resync_after_truncation`].
    pub counter_ahead_resyncs: AtomicU64,
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
    /// [`CryptoIntake::peer_ids`] — is folded in here too: from the
    /// wire's point of view an unrecognized sender IS "could not
    /// authenticate this," the same bucket).
    pub dropped_auth_failed: AtomicU64,
    /// M8 (Task 11): a sealed datagram's counter was already seen under its
    /// (sender, epoch, salt) replay window — `uc_crypto::CryptoError::Replayed`.
    pub dropped_replay: AtomicU64,
    /// M8 (Task 11): a sealed datagram named a group-key epoch this node
    /// does not hold (never minted, or rotated out) —
    /// `uc_crypto::CryptoError::NoGroupKey`. Self-heals once `HS_KEY`
    /// lands for the epoch (`uc_crypto::group`'s docs); never a signal to
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
    /// with no entry in [`CryptoIntake::peer_ids`]'s map. Counted
    /// separately from `dropped_auth_failed` for operator diagnosability
    /// (a misconfigured peer map vs. a genuine forgery look identical on
    /// the wire but very different to fix) even though both are folded
    /// into `dropped_auth_failed` for the mandated counter's own semantics.
    pub dropped_unknown_peer: AtomicU64,
    /// M8 (Task 11): a handshake-plane datagram (kind 18/19/20) could not be
    /// forwarded — no route supplied in [`CryptoIntake::handshake`], or the
    /// route's channel was full/disconnected. Harmless by the same
    /// reasoning as `net_drops`: `Peers::tick`/a retry re-initiates.
    pub dropped_handshake: AtomicU64,
    /// M8 (Task 17): an OUTGOING datagram this receiver could not seal —
    /// `NAK`, `STATUS`, `APPEND_POSITION`, `SNAP_NAK` or `SNAP_DONE`, all
    /// `Scope::Pairwise`. Either no `SocketAddr -> NodeId` entry names the
    /// destination, or no established pairwise session with it exists yet.
    /// The datagram is DROPPED — never sent in the clear, which would make
    /// the whole feature optional per destination.
    ///
    /// Self-healing rather than fatal, and each kind for its own reason: a
    /// dropped `NAK` re-fires on the NAK timer's backoff; a dropped `STATUS`/
    /// `APPEND_POSITION` re-fires because its cursor is NOT advanced on
    /// failure (so the next duty cycle still sees the threshold crossed); a
    /// dropped `SNAP_NAK` re-fires on the snapshot NAK timer; a dropped
    /// `SNAP_DONE` costs only the leader's session slot, which times out —
    /// the local artifact is already renamed and installed by then.
    pub seal_failures: AtomicU64,
    /// A `SNAP_BEGIN` arrived whose `layout` byte is not
    /// [`SNAP_BEGIN_LAYOUT_V3`] — the named refusal **`peer wire ≤ 0.6.0`**.
    /// The session is dropped. NOTE this is the *defensive* half: a genuine
    /// pre-0.7.0 body is shorter than [`SNAP_BEGIN_FIXED_LEN`] and is usually
    /// dropped by `read_snap_begin_body`'s length check before it ever gets
    /// here; this fires when a legacy-shaped body happens to reach the 0.7.0
    /// fixed length (a long-enough carried config). The follower keeps
    /// NAKing; the operator sees the counter and finishes the flag day.
    pub snap_refused_legacy_peer: AtomicU64,
    /// Wire 0.7.0 (spec §5, §8): a `SNAP_BEGIN` arrived whose `identity`
    /// array differs from this node's own at some row (an identity/name
    /// mismatch), or that names a service id outside its own declared mask —
    /// the named refusal **`identity (name) mismatch at some row`**. The
    /// session is dropped: installing a set that does not name this node's
    /// FSMs, row for row, would strand one below an adopted floor, or attach
    /// the wrong service under a shared row. Declared identities must match
    /// cluster-wide, positionally. See [`FollowerStats::identity_refusal`]
    /// (written BEFORE this counter bumps, `Release`-ordered against it) for
    /// the offending row's detail.
    pub snap_refused_declared_mismatch: AtomicU64,
    /// Wire 0.7.0 (spec §5, §8): a `SNAP_BEGIN` arrived whose identity
    /// matched at every row but whose `version` differed from this node's own
    /// at some row where BOTH sides report a nonzero version — the named
    /// refusal **`version mismatch`**. An unversioned side (0) is never a
    /// mismatch: it means "unknown," not "empty." See
    /// [`FollowerStats::version_refusal`] (written BEFORE this counter
    /// bumps, `Release`-ordered against it) for the offending row's detail.
    pub snap_refused_version_mismatch: AtomicU64,
    /// Detail for the most recent IDENTITY refusal (`RefusalKind::Identity`)
    /// on this receiver — the row, both sides' hash, and which kind of
    /// mismatch it was (always `Identity` here). `None` until the first such
    /// refusal. Touched only on the (rare) refusal path, never on the hot
    /// chunk-receive path; a reader must poison-tolerate it
    /// (`.lock().unwrap_or_else(|e| e.into_inner())`) since it is
    /// diagnostic-only state that must never cascade a panic on this thread
    /// into a poisoned lock elsewhere. Kept in its own cell, separate from
    /// [`version_refusal`](Self::version_refusal), so an identity refusal and
    /// a version refusal landing in the same duty cycle each keep their own
    /// detail rather than one clobbering the other.
    pub identity_refusal: Mutex<Option<IdentityRefusal>>,
    /// The `RefusalKind::Version` counterpart of
    /// [`identity_refusal`](Self::identity_refusal) — detail for the most
    /// recent VERSION refusal only. Same poison-tolerance requirement.
    pub version_refusal: Mutex<Option<IdentityRefusal>>,
    /// M14c review round 2: a local I/O failure on the snapshot INTAKE path —
    /// a `.part` that could not be created/sized (`open_snap_part`), or a
    /// completed artifact whose `sync_all`/`rename` failed. None of these are
    /// the peer's fault and none are fatal (the transfer is retried: the
    /// missing BEGIN is re-sent on its cadence, and a synced-but-unrenamed
    /// `.part` is re-renamed on the next chunk), but a follower that can never
    /// finish an install is otherwise SILENT — it just keeps NAKing. This is
    /// the counter that names "the disk, not the wire" (full/read-only
    /// snapshot dir, a stray directory sitting on the final path, EPERM).
    pub snap_intake_io_failures: AtomicU64,
    /// M14c2 (T10a): inbound snapshot intakes abandoned after
    /// [`SNAP_INTAKE_TIMEOUT_NS`] with no chunk. The still-unpublished `.part`
    /// files are unlinked (they are `set_len`'d to the artifact's full size, so
    /// leaking them is real disk); an artifact this intake had ALREADY renamed
    /// stays on disk, to be adopted or superseded by the next session. Not an
    /// error on its own: the joiner is still below the floor, keeps NAKing, and
    /// the next session starts a clean intake.
    ///
    /// A rising count means transfers are never finishing. Read it WITH
    /// `snap_intake_io_failures`: on its own it points at the leader or the
    /// link, but if both rise together the cause is LOCAL — this node's
    /// snapshot directory blocks the install, the sender times out at 30 s,
    /// this timer fires at 60 s, and the whole set is re-downloaded on a ~60 s
    /// loop until the obstacle is cleared.
    pub snap_intake_abandoned: AtomicU64,
    /// M14c2 (T10a): `SNAP_BEGIN` bodies from a peer speaking a wire we cannot
    /// decode at all, counted ONCE PER SESSION (latched until a decodable
    /// BEGIN arrives).
    ///
    /// The companion of `snap_refused_legacy_peer`, which counts every such
    /// datagram: the leader re-sends its `SNAP_BEGIN` every 20 ms until a
    /// `SNAP_DONE` comes back, so on a flag day that counter climbs at the
    /// resend rate and says nothing about how many distinct sessions were
    /// refused. This one does — and it also separates the two halves of the
    /// `peer wire ≤ 0.6.0` refusal (a body too short to decode vs. a decodable
    /// body carrying `layout != SNAP_BEGIN_LAYOUT_V3`), which
    /// `snap_refused_legacy_peer` deliberately folds together.
    pub snap_begin_undecodable: AtomicU64,
}

/// M7 Task 6: the `(position, config)` companion cells `set_snapshot_intake`
/// wires in — the position cell for `ArchiveCmd::AdoptFloor`, the config cell
/// for `adopt_snapshot_config`. Named to keep `set_snapshot_intake`'s signature
/// under clippy's type-complexity threshold.
pub type IncomingSnapshotSignal = (Arc<AtomicU64>, Arc<Mutex<Vec<u8>>>);

/// M14c: the identity of the last INBOUND session that completed here — what
/// [`FollowerReceiver::snap_last_done`] re-acks a straggling `SNAP_BEGIN`
/// against. The artifact list is part of the identity on purpose: a leader that
/// restarts recycles its session ids from 1, so `(peer, session)` alone would
/// make a genuinely NEW session look like a re-send and wedge it. A BEGIN
/// naming an artifact this session did not carry opens a fresh intake instead.
struct SnapDone {
    peer: SocketAddr,
    session: u32,
    /// `(service_id, snapshot_pos)` of every artifact the session installed.
    artifacts: Vec<(u8, u64)>,
}

/// One artifact inside an inbound session. `base` is its first byte's
/// STREAM-GLOBAL offset — the session is one concatenated byte stream whose
/// artifact boundaries the BEGINs announce, so the repair path is unchanged.
struct SnapPart {
    service_id: u8,
    snapshot_pos: u64,
    base: u64,
    len: u64,
    /// Taken (dropped) just before the rename, mirroring the 0.5.0 discipline.
    file: Option<std::fs::File>,
    part_path: PathBuf,
    final_path: PathBuf,
    done: bool,
}

/// M6 Task 6 / M14c: one in-flight INBOUND snapshot transfer — a stream of one
/// artifact per declared FSM. Chunks land at their stream offset in the
/// pre-sized `.part` of whichever artifact contains that offset; one `Rebuilt`
/// over the stream's byte space tracks contiguity + gaps (NAK'd like the main
/// stream). Each artifact is fsync'd + atomically renamed the moment the
/// contiguous frontier passes its end; the FLOOR is adopted only once EVERY
/// declared id has landed, so no FSM is ever stranded below an adopted floor.
struct SnapIntake {
    peer: SocketAddr,
    session: u32,
    /// From the session's first `SNAP_BEGIN`; equals this node's own
    /// `own_identity` positionally (a difference is refused before an
    /// intake ever opens). The declared-FSM bitmask this session covers is
    /// derived from it via [`crate::sender::identity_mask`], never stored
    /// separately.
    identity: [u64; 8],
    /// From the session's first `SNAP_BEGIN`, alongside `identity` — rides
    /// back out on the `SNAP_DONE` echo.
    version: [u32; 8],
    /// Bit `i` set ⇔ id `i`'s artifact is complete and renamed.
    received: u64,
    /// Announced artifacts, ascending, contiguous in `base`.
    parts: Vec<SnapPart>,
    /// Sum of the announced artifacts' lengths — how far the stream is known
    /// to run (it grows as later BEGINs arrive).
    announced_len: u64,
    /// Contiguity over `[0, announced_len)` STREAM offsets.
    got: Rebuilt,
    nak: NakTimer,
    /// M7 Task 6: the encoded `ConfigRecord.config` carried in `SNAP_BEGIN`
    /// (`v2::config::encode_config` bytes; empty if the leader shipped none;
    /// identical on every BEGIN of a session — taken from the first).
    /// Forwarded to `incoming_snapshot_config` on completion, alongside
    /// `incoming_snapshot_pos`, for the consensus agent's install handler.
    config: Vec<u8>,
    /// M14c2 (T10a): when this intake last saw evidence the transfer is LIVE —
    /// a chunk that landed, or a new artifact's `SNAP_BEGIN`. The
    /// [`SNAP_INTAKE_TIMEOUT_NS`] deadline is measured from here, so an
    /// artifact of any size on a link of any quality stays alive as long as
    /// bytes keep arriving.
    last_chunk_ns: u64,
    /// M14c2 (T10a): when a publish attempt for this intake last FAILED —
    /// `None` = no failed attempt is outstanding. Both callers of
    /// `snap_publish_complete_parts` (the duty cycle AND every accepted chunk)
    /// gate on it: `None` publishes immediately, `Some(t)` waits out
    /// [`SNAP_REDRIVE_INTERVAL_NS`]. Stamping only on FAILURE is what keeps the
    /// normal completion path undelayed, and `None` rather than a `0` sentinel
    /// is what keeps it undelayed during the node's first quarter second (the
    /// process clock starts near zero). A call the cadence refuses does not
    /// push the next one out; a pass that blocks on nothing clears it.
    last_publish_try_ns: Option<u64>,
    /// M14c2 (T10a): this intake's `snap_chunk` write failure has been logged.
    /// The counter increments on every failure; the operator line is latched
    /// per intake so a full disk cannot write one per datagram.
    write_failure_logged: bool,
}

pub struct FollowerReceiver {
    buffer: Arc<LogBuffer>,
    writer: PositionedWriter,
    sock: FaultSocket,
    cfg: FollowerConfig,
    status_bytes: u64,
    rebuilt: Rebuilt,
    /// The last frontier this receiver stored into the shared `append`
    /// counter. If the counter is found BELOW it, a prime has intervened —
    /// see the publish-time guard in the DATA arm.
    last_published: u64,
    /// The term the current out-of-order runs in `rebuilt` were accepted
    /// under. A move discards them — see `discard_ooo_on_term_change`.
    ooo_term: u32,
    nak: NakTimer,
    leader_append: u64,
    base: Instant,
    last_status_ns: u64,
    status_at: u64,
    /// Durable value last reported via AppendPosition.
    ap_reported: u64,
    /// See [`Receiver::set_validated_frontier`]. `None` = report raw durable.
    validated_frontier: Option<Arc<AtomicU64>>,
    /// The term we attribute to the byte below the validated frontier — the
    /// content attestation shipped with every report (protocol 0.5.0).
    /// `None` (tests, sim) sends `0` = unattested.
    validated_term: Option<Arc<AtomicU32>>,
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
    /// M6 Task 6 / M14c: the snapshots ROOT (`instance_dir/snapshots`) for
    /// inbound transfers; each artifact lands under `<root>/<service_id>/`.
    /// `None` = this node never receives snapshots (no intake).
    snap_dir: Option<PathBuf>,
    /// Wire 0.7.0: this node's own row-`r` FSM identity hash, compared
    /// positionally against every session's `SNAP_BEGIN.identity`. Set by
    /// `set_snapshot_intake`.
    own_identity: [u64; 8],
    /// Wire 0.7.0: closure returning this node's own row-`r` attached
    /// service's packed version, read fresh on every `SNAP_BEGIN` (the cnc
    /// slot can change under a running node). `None` only in unit tests that
    /// construct a receiver without an intake — no version comparison is
    /// then possible or attempted. Set by `set_snapshot_intake`.
    own_versions: Option<Arc<dyn Fn() -> [u32; 8] + Send + Sync>>,
    /// M6 Task 6: the in-flight inbound snapshot transfer, if any.
    snap_intake: Option<SnapIntake>,
    /// M14c2 (T10a): an undecodable `SNAP_BEGIN` has already been counted (and
    /// logged) for the session in progress. Cleared by the next DECODABLE
    /// `SNAP_BEGIN` — the wire is speakable again, so a later refusal is a
    /// genuinely new one. See [`FollowerStats::snap_begin_undecodable`].
    snap_begin_undecodable_latched: bool,
    /// M14c: the last session that COMPLETED here. The sender keeps re-sending
    /// a `SNAP_BEGIN` on a 20 ms cadence until our `SNAP_DONE` reaches it, so a
    /// lost DONE would otherwise re-open (and re-download) a set we already
    /// installed. Instead we re-ack it, which is exactly what the leader is
    /// waiting for.
    snap_last_done: Option<SnapDone>,
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
    /// its field doc and `uc_crypto::transport`'s "M8 ownership
    /// correction" module docs): this receiver agent owns only the group-
    /// scope replay windows (receiver-exclusive, no lock); the shared
    /// handshake sessions/group-key plane/boot-salt live behind the
    /// `Arc<Mutex<_>>` a `uc_crypto::SharedTransport` hands this half out
    /// from.
    crypto: Option<ReceiveHalf>,
    /// M8 (Task 17): the SEND-side crypto handle for this receiver's own
    /// outgoing control datagrams — see [`CryptoIntake::transport`] for why
    /// this is a `SharedTransport` clone rather than a second `SendHalf`.
    /// `None` (crypto off) leaves every send byte-for-byte pre-M8.
    crypto_seal: Option<uc_crypto::SharedTransport>,
    /// M8 (Task 11): `SocketAddr -> NodeId` map, needed to resolve `from`
    /// before `crypto.open_slice` can be called (the crypto layer identifies
    /// peers by `NodeId`; `uc_net`'s wire layer has only ever known
    /// `SocketAddr`s — see `crypto_admit`'s doc and the module docs). `None`
    /// entries (an address not in this map) are dropped and counted
    /// (`dropped_unknown_peer`, folded into `dropped_auth_failed`) — never a
    /// panic on an unrecognized address, since that address is exactly as
    /// attacker-controlled as anything else arriving on this socket. Empty
    /// by default (every existing non-crypto call site is unaffected); the
    /// node layer (T12) supplies it in [`CryptoIntake`] from the SAME
    /// `id_to_addr` map it already builds for the sender's
    /// `sender_peer_slots` (`node.rs:640`), inverted.
    ///
    /// This is a LOCAL COPY, refreshed from `peer_ids_src` once per duty
    /// cycle (T12) rather than locked per datagram — see [`PeerIds`].
    peer_ids: HashMap<SocketAddr, NodeId>,
    /// M8 (Task 12): the shared, versioned source `peer_ids` mirrors, plus
    /// the generation last mirrored. `None` when crypto is off (nothing
    /// consults `peer_ids` then — `crypto_admit` returns on its first line).
    peer_ids_src: Option<PeerIds>,
    peer_ids_gen: u64,
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

/// M14c: the lowest id in `declared` strictly above `after` (`None` = the
/// lowest of all). The artifacts of a session are announced in exactly this
/// order, which is what lets the receiver place each one's STREAM base.
fn next_declared_id(declared: u64, after: Option<u8>) -> Option<u8> {
    let from = after.map_or(0u32, |id| id as u32 + 1);
    if from >= u64::BITS {
        return None;
    }
    let rest = declared >> from;
    if rest == 0 {
        return None;
    }
    Some((from + rest.trailing_zeros()) as u8)
}

/// M14c: abandon an inbound intake's artifacts — close each still-open `.part`
/// and delete it. An artifact already renamed (`done`) has none left to remove.
/// Called whenever an intake is dropped rather than completed (a named refusal,
/// or a new session replacing a stale one), so an abandoned session cannot
/// leave a pre-sized `incoming-*.part` under `snapshots/<id>/` forever — those
/// are `set_len`'d to the artifact's full size, so a leak is real disk.
fn discard_snap_parts(parts: Vec<SnapPart>) {
    for p in parts {
        if p.done {
            continue;
        }
        drop(p.file); // close before unlink
        let _ = std::fs::remove_file(&p.part_path);
    }
}

/// M14c: open the `.part` for one announced artifact under `<root>/<id>/`. Free
/// function so it borrows neither the receiver nor the intake.
fn open_snap_part(root: &Path, b: &SnapBeginBody, base: u64) -> Option<SnapPart> {
    let dir = root.join(b.service_id.to_string());
    std::fs::create_dir_all(&dir).ok()?;
    let part_path = dir.join(format!("incoming-{}.part", b.snapshot_pos));
    let final_path = dir.join(format!("snap-{}.ultsnap", b.snapshot_pos));
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(&part_path)
        .ok()?;
    file.set_len(b.total_len).ok()?;
    Some(SnapPart {
        service_id: b.service_id,
        snapshot_pos: b.snapshot_pos,
        base,
        len: b.total_len,
        file: Some(file),
        part_path,
        final_path,
        done: false,
    })
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
    /// this with `crypto: None`. Takes a [`ReceiveHalf`] (from
    /// `uc_crypto::SharedTransport::receive_half`), never a whole
    /// `Transport`/`SharedTransport` — see the `crypto` field's doc for why.
    /// The caller (the node layer, T12) owns the `SharedTransport` and calls
    /// `receive_half()` exactly once per process; this constructor has no
    /// way to enforce that single-call discipline itself (it only ever sees
    /// the `ReceiveHalf` already handed out).
    ///
    /// M8 (Task 12): the half arrives inside a [`CryptoIntake`], alongside
    /// the sender-identity map and the handshake route, so enabling crypto
    /// without wiring both of those is a compile error rather than a silent
    /// cluster-wide drop — see [`CryptoIntake`]'s doc.
    pub fn with_crypto(
        buffer: Arc<LogBuffer>,
        sock: FaultSocket,
        cfg: FollowerConfig,
        term: TermHandle,
        route: mpsc::SyncSender<NetEvent>,
        crypto: Option<CryptoIntake>,
    ) -> Self {
        let (crypto, peer_ids_src, hs_route, crypto_seal) = match crypto {
            Some(CryptoIntake {
                half,
                peer_ids,
                handshake,
                transport,
            }) => (Some(half), Some(peer_ids), Some(handshake), Some(transport)),
            None => (None, None, None, None),
        };
        let start = buffer.counters().append.load_acquire();
        let status_bytes = if cfg.status_bytes == 0 {
            buffer.capacity() / 4
        } else {
            cfg.status_bytes
        };
        let writer = PositionedWriter::new(Arc::clone(&buffer));
        Self {
            buffer,
            writer,
            sock,
            status_bytes,
            rebuilt: Rebuilt::new(start),
            last_published: start,
            ooo_term: term.load(Ordering::Relaxed),
            nak: NakTimer::new(cfg.nak, cfg.seed),
            cfg,
            leader_append: start,
            base: Instant::now(),
            last_status_ns: 0,
            status_at: start,
            ap_reported: start,
            validated_frontier: None,
            validated_term: None,
            last_ap_ns: 0,
            recv_buf: vec![0u8; 65_536],
            stats: Arc::new(FollowerStats::default()),
            term,
            route,
            sender_route: None,
            gate: None,
            activity_emitted: false,
            snap_dir: None,
            own_identity: [0; 8],
            own_versions: None,
            snap_intake: None,
            snap_begin_undecodable_latched: false,
            snap_last_done: None,
            incoming_snapshot_pos: None,
            incoming_snapshot_config: None,
            snap_nak_cfg: cfg.nak,
            snap_seed: cfg.seed,
            snap_adopt_pending: None,
            prime_gen: None,
            #[cfg(test)]
            straddle_hook: None,
            crypto,
            crypto_seal,
            peer_ids: peer_ids_src
                .as_ref()
                .map(PeerIds::snapshot)
                .unwrap_or_default(),
            peer_ids_gen: peer_ids_src.as_ref().map(PeerIds::generation).unwrap_or(0),
            peer_ids_src,
            hs_route,
            cleartext_peer_log: HashMap::new(),
        }
    }

    /// M8 (Task 12): mirror the shared [`PeerIds`] map if the writer has
    /// published a new generation since the last duty cycle. One `Acquire`
    /// load per cycle in the common (unchanged) case; the `Mutex` is touched
    /// only when membership actually changed. See [`PeerIds`] for why the
    /// map cannot simply be a boot-time snapshot.
    fn refresh_peer_ids(&mut self) {
        let Some(src) = self.peer_ids_src.as_ref() else {
            return;
        };
        let published = src.generation();
        if published != self.peer_ids_gen {
            self.peer_ids = src.snapshot();
            self.peer_ids_gen = published;
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
    /// [`uc_sim::world::DataPlane::Gated`] contract's ambiguous-window half:
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

    /// Install the node's VALIDATED frontier (`ElectionSm::validated_up_to`).
    /// `AppendPosition` reports are clamped to it, so what this node attests
    /// toward the leader's quorum ranking is bytes whose CONTENT is confirmed
    /// against that leader's history — not merely bytes it happens to hold.
    /// Reporting a raw durable that covered a deposed leader's tail let the
    /// leader certify a commit no live quorum backed (2026-08-16 hunt).
    /// Absent (tests, sim), reports fall back to the raw durable.
    pub fn set_validated_frontier(&mut self, frontier: Arc<AtomicU64>) {
        self.validated_frontier = Some(frontier);
    }

    /// Install the content attestation published alongside the frontier (the
    /// term covering the byte below it). See [`Receiver::set_validated_frontier`].
    pub fn set_validated_term(&mut self, term: Arc<AtomicU32>) {
        self.validated_term = Some(term);
    }

    /// Current prime generation (0 when no counter is installed — the recheck
    /// then always matches, so single-life receivers are unaffected).
    #[inline]
    fn prime_gen_val(&self) -> u64 {
        self.prime_gen
            .as_ref()
            .map_or(0, |g| g.load(Ordering::Relaxed))
    }

    /// Test-only: install a hook fired in the DATA arm between the `rebuilt`
    /// insert and the generation recheck, so a test can deterministically inject
    /// a straddling `prime` + generation bump.
    #[cfg(test)]
    fn set_straddle_hook(&mut self, hook: Box<dyn Fn() + Send>) {
        self.straddle_hook = Some(hook);
    }

    /// M6 Task 6 / M14c / wire 0.7.0: enable INBOUND snapshot transfers.
    /// `snap_root` is the `snapshots/` directory the per-id `.part`/final
    /// artifacts land under (`<root>/<id>/`); `own_identity` is this node's
    /// own row-`r` FSM identity hash, which every session's
    /// `SNAP_BEGIN.identity` must match POSITIONALLY (`identity (name)
    /// mismatch`); `own_versions` is a closure returning this node's own
    /// row-`r` attached service's packed version, read fresh on every
    /// `SNAP_BEGIN` for the version comparison (both sides nonzero and
    /// disagreeing = refused); `incoming` (if set) is `(position, config)`:
    /// the position cell receives each COMPLETED session's floor — the
    /// MINIMUM over the received artifact positions — for the consensus
    /// agent to adopt as an archive floor, and (M7 Task 6) the config cell
    /// receives that session's carried `SNAP_BEGIN.config` bytes for the
    /// agent's `adopt_snapshot_config` handler. Without this call kinds
    /// 12/13 are ignored (a node that never joins below a floor never
    /// receives snapshots).
    pub fn set_snapshot_intake(
        &mut self,
        snap_root: PathBuf,
        own_identity: [u64; 8],
        own_versions: Arc<dyn Fn() -> [u32; 8] + Send + Sync>,
        incoming: Option<IncomingSnapshotSignal>,
    ) {
        self.snap_dir = Some(snap_root);
        self.own_identity = own_identity;
        self.own_versions = Some(own_versions);
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
        // UPWARD: the shared counter is ahead of our receive frontier, which
        // means it was moved by something other than us — this node's OWN
        // appender during a leader stint (its appends push `append`, and the
        // archive pushes `durable` behind them, while our tracker sits where it
        // was when we last received). On step-down the stale-low tracker would
        // accept the next leader's DATA at positions the archive has ALREADY
        // RECORDED; `PositionedWriter::write_run` now refuses those writes, so
        // without this rebase the follower would NAK for recorded positions and
        // make no progress until something truncated. The counter is
        // authoritative for what this node holds; follow it.
        //
        // Deliberately NOT gated on role. On a LIVE leader this is a no-op that
        // fires each cycle — the receiver accepts no DATA in its own term, so
        // there is nothing to keep — and doing it unconditionally means the
        // tracker is already correct at the instant of step-down, with no window
        // to reason about. (The M6 snapshot-install path keeps its own
        // `resync_after_snapshot_install`: it is gated on an adopt-floor that
        // must have been applied first.)
        if append > self.rebuilt.contiguous() && self.snap_adopt_pending.is_none() {
            self.rebuilt = Rebuilt::new(append);
            self.last_published = append;
            self.leader_append = self.leader_append.max(append);
            // NOT `status_at`/`ap_reported`, unlike the downward branch. Those
            // are reset there because a REGRESSED frontier would underflow the
            // `contiguous - status_at` gate and would advertise positions that
            // were just cut. Moving UP has neither problem, and clearing
            // `ap_reported` here would swallow the very durable advance this
            // node owes the leader for commit ranking.
            self.stats
                .counter_ahead_resyncs
                .fetch_add(1, Ordering::Relaxed);
        }
        if append < self.rebuilt.contiguous() {
            self.rebuilt = Rebuilt::new(append);
            self.last_published = append;
            self.leader_append = append;
            self.nak.poll(None, self.now_ns()); // disarm: the old gap predates the re-prime
            // The report cursors shadow the frontier and must move with it:
            // `status_at` gates on `contiguous - status_at` (would underflow if
            // left above a regressed frontier), and `ap_reported` gates the
            // AppendPosition send so the first re-established durable reports
            // promptly toward the leader's commit ranking.
            self.status_at = append;
            self.ap_reported = append;
            // A truncation owes the leader an IMMEDIATE corrective report
            // (2026-08-16). The leader's per-follower slot now takes the
            // latest report rather than a high-water mark, so until our lower
            // durable reaches it, it still ranks us as backing bytes we just
            // dropped. Zeroing the send cadence makes the next duty cycle
            // report, instead of waiting out `append_pos_floor_ns`.
            self.last_ap_ns = 0;
            self.stats
                .truncation_resyncs
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Drop out-of-order runs recorded under a PREVIOUS term.
    ///
    /// A term boundary re-frames the stream above the commit point: the same
    /// position can carry a 96 B data frame in term T and the next term's 32 B
    /// NewTerm frame in T+1. Runs above the contiguous frontier are, by the
    /// accept rule (`position < contiguous` drops), writable more than once, and
    /// [`Rebuilt`] UNIONS overlapping spans rather than resolving them — it
    /// tracks positions only, deliberately ("no reliance on buffer contents",
    /// its module doc). So a stale old-term run and the new term's shorter frame
    /// at the same position combine into a span the buffer does not tile: the
    /// later `write_run` overwrites the head, the dead frame's tail stays as
    /// orphaned payload, and publishing that union puts `append` over bytes no
    /// frame walk can cross. The archive is the only reader that walks frames
    /// there, so it fail-stops (`RecorderCorrupt`) — the LUCKY outcome; a
    /// plausible-looking orphan word would instead have been recorded into the
    /// journal as current-term data and served on deep-NAK replay.
    ///
    /// Positions BELOW the frontier are untouched: those bytes were already
    /// published (and consistent when they were). If the new term also needs
    /// them cut, that arrives as a reconciliation truncation, whose prime
    /// [`resync_after_truncation`](Self::resync_after_truncation) handles —
    /// this is the same invalidation for the case where nothing regresses
    /// locally, which is why that resync cannot see it.
    ///
    /// No-op unless out-of-order state actually exists, so the ordinary
    /// in-order path is untouched.
    fn discard_ooo_on_term_change(&mut self, term: u32) {
        if term == self.ooo_term {
            return;
        }
        self.ooo_term = term;
        if self.rebuilt.highest() == self.rebuilt.contiguous() {
            return; // nothing out of order to invalidate
        }
        self.rebuilt = Rebuilt::new(self.rebuilt.contiguous());
        // The armed gap and the tail-loss reference both described the old
        // term's stream (same reasoning as the truncation resync).
        self.nak.poll(None, self.now_ns());
        self.leader_append = self.rebuilt.contiguous();
        self.stats
            .term_change_discards
            .fetch_add(1, Ordering::Relaxed);
    }

    /// One duty cycle: drain up to 64 datagrams, then NAK/status upkeep.
    pub fn do_work(&mut self) -> bool {
        // FIRST, before any datagram: if the archive truncated and re-primed the
        // shared `append` counter below our rebuilt frontier, rebuild the tracker
        // so the re-shipped post-truncation tail is accepted, not dropped as dup.
        self.resync_after_truncation();
        // And, after a snapshot install, forward to the adopted floor (M6 Task 8).
        self.resync_after_snapshot_install();
        // M8 (Task 12): pick up a membership change before authenticating
        // anything this cycle — a joiner added by M7 at runtime must be
        // resolvable without a restart (see `PeerIds`).
        self.refresh_peer_ids();
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
        // The `n < DATAGRAM_HEADER_LEN` pre-guard above already returned; the
        // reader is total on `&[u8]` (M12d), so the `else` arm is belt and
        // braces — a truncated datagram is admitted unchanged and dropped by
        // `on_datagram`'s own malformed check, exactly as before.
        let Some(h) = read_datagram_header(&buf[..n]) else {
            return Some(n);
        };

        // T11's temporary cleartext-SNAP allowance USED TO SIT HERE and was
        // DELETED by T17 (2026-07-29), which is what makes this comment worth
        // keeping. It admitted SNAP_BEGIN/SNAP_CHUNK unauthenticated, because
        // the send side (`sender.rs`'s `assemble_snap`) could not seal them
        // until a `SocketAddr -> NodeId` map and a driven handshake both
        // existed. Both now do, both directions of a snapshot session are
        // sealed, and SNAP is authenticated exactly like every other pairwise
        // kind by falling through to `open_slice` below. The hole it left
        // open — a forged `SNAP_BEGIN` carrying `SnapBeginBody.config`
        // straight into `maybe_adopt_incoming_snapshot`, i.e. attacker-chosen
        // application state AND attacker-chosen cluster membership on a
        // joining or below-floor node — is closed by that fall-through.
        // Pinned by `an_unsealed_snap_begin_is_refused_now_that_t17_landed`.

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

        let crypto = self
            .crypto
            .as_mut()
            .expect("checked Some at the top of this function");
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
                "uc_net: peer {from} appears to be running with crypto disabled (datagram too \
                 short to be a sealed frame, key_epoch=0) -- this node has crypto enabled; check \
                 for a flag-day rollout mismatch"
            );
        }
    }

    /// Forwards a handshake-plane datagram (kind 18/19/20) to
    /// [`CryptoIntake::handshake`]'s channel, if crypto is on.
    /// Drops and counts (`dropped_handshake`) otherwise — no route, or a
    /// full/disconnected one (harmless: `Peers::tick`/a retry re-initiates,
    /// same reasoning as the consensus route's own full-channel drops).
    fn route_handshake(&mut self, from: SocketAddr, kind: u8, body: Vec<u8>) {
        let sent = self
            .hs_route
            .as_ref()
            .is_some_and(|tx| tx.try_send((from, kind, body)).is_ok());
        if !sent {
            self.stats.dropped_handshake.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Resolves `from` to the `NodeId` [`ReceiveHalf::open_slice`] needs —
    /// see [`PeerIds`] and [`CryptoIntake::peer_ids`].
    #[inline]
    fn peer_id_of(&self, from: SocketAddr) -> Option<NodeId> {
        self.peer_ids.get(&from).copied()
    }

    /// M8 (Task 17): the send-side counterpart of `crypto_admit` — the ONE
    /// place this receiver's own outgoing datagrams are sealed. Every caller
    /// (`NAK`, `STATUS`, `APPEND_POSITION`, `SNAP_NAK`, `SNAP_DONE`) is
    /// `Scope::Pairwise`; `seal_pairwise_control` itself refuses anything
    /// else, so a future group-scope kind routed through here fails loudly in
    /// the counter rather than being sealed the wrong way.
    ///
    /// Returns whether the datagram reached the socket. `false` means it was
    /// DROPPED — there is deliberately no cleartext fallback (see
    /// [`FollowerStats::seal_failures`] for why each dropped kind is
    /// self-healing). With crypto off this is a pass-through: the same
    /// `sock.send_to` the pre-M8 code made, on the same bytes.
    fn seal_and_send(&mut self, to: SocketAddr, kind: u8, d: &mut Vec<u8>) -> bool {
        if let Some(transport) = self.crypto_seal.as_ref() {
            let Some(&peer_id) = self.peer_ids.get(&to) else {
                self.stats.seal_failures.fetch_add(1, Ordering::Relaxed);
                return false;
            };
            if transport.seal_pairwise_control(kind, peer_id, d).is_err() {
                self.stats.seal_failures.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }
        let _ = self.sock.send_to(d, to);
        true
    }

    fn on_datagram(&mut self, d: &[u8], from: SocketAddr) {
        use Ordering::Relaxed;
        if d.len() < DATAGRAM_HEADER_LEN {
            self.stats.dropped_malformed.fetch_add(1, Relaxed);
            return;
        }
        // The `d.len() < DATAGRAM_HEADER_LEN` pre-guard above already
        // returned; belt and braces (M12d: the reader is total on `&[u8]`).
        let Some(h) = read_datagram_header(d) else {
            self.stats.dropped_malformed.fetch_add(1, Relaxed);
            return;
        };
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
        // PER DATAGRAM, not once per duty cycle. A cycle drains up to 64
        // datagrams and the archive agent primes on its own thread, so a
        // collapse/truncation can land BETWEEN two datagrams of one drain —
        // after the top-of-cycle check has already passed. Every datagram after
        // it would then publish the pre-prime `rebuilt.contiguous()` over the
        // freshly primed floor, claiming a frontier for bytes this term never
        // wrote. `prime_generation` does not cover this: it catches a prime that
        // straddles a SINGLE datagram's processing, and here the prime is
        // complete before this datagram's `gen0` sample is even taken. Cost is
        // one acquire load per datagram; the check is false in steady state
        // (including on a leader, whose appender legitimately runs ahead).
        self.resync_after_truncation();
        // This datagram is current-term traffic; if the term MOVED since the
        // out-of-order runs were recorded, they are no longer trustworthy.
        self.discard_ooo_on_term_change(term);
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
                    // Belt to the generation recheck's braces, and the one that
                    // actually holds: re-evaluate the PRIME PREDICATE against the
                    // live counter, right here, immediately before the store.
                    // `append < contiguous` is the archive's prime signature (see
                    // `resync_after_truncation`) — only a prime drives the counter
                    // below our tracker. Checking it at the top of the cycle, or
                    // even per datagram, leaves a window; checking it in the same
                    // breath as the store does not. The generation recheck alone does not cover
                    // this: it compares against `gen0`, which is sampled a few
                    // instructions AFTER the per-datagram resync, so a prime
                    // landing in that window is already reflected in `gen0` and
                    // the recheck sees nothing to reject. Field evidence
                    // (2026-08-02): `append` published 101,024 B past the last
                    // real frame, over ring content from a previous lap — the
                    // frames beyond the failure carried term 1 while the live
                    // term was 45. Drop; the next duty cycle's resync rebases us
                    // to the primed floor and NAKs forward.
                    let live = self.buffer.counters().append.load_acquire();
                    if live < self.last_published {
                        self.stats.dropped_straddle.fetch_add(1, Relaxed);
                        return;
                    }
                    self.last_published = self.rebuilt.contiguous();
                    self.buffer
                        .counters()
                        .append
                        .store_release(self.rebuilt.contiguous());
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
            // grep-provable postcondition in `uc_node::node::exec`'s
            // `Action::AdvanceCommit` arm).
            DGRAM_KIND_NAK if self.sender_route.is_some() => {
                // Leader role (M4): a follower's NAK, demuxed to our sender so it
                // retransmits the missing span. Term-checked above.
                let body = &d[DATAGRAM_HEADER_LEN..];
                if body.len() >= NAK_BODY_LEN
                    && let Some(route) = &self.sender_route
                    && let Some(b) = read_nak_body(body)
                {
                    let _ = route.try_send(CtrlMsg::Nak {
                        from,
                        position: b.position,
                        length: b.length,
                    });
                }
            }
            DGRAM_KIND_STATUS if self.sender_route.is_some() => {
                // Leader role (M4): a follower's flow-window advert, demuxed to
                // our sender's quorum pacing.
                let body = &d[DATAGRAM_HEADER_LEN..];
                if body.len() >= STATUS_BODY_LEN
                    && let Some(route) = &self.sender_route
                    && let Some(b) = read_status_body(body)
                {
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
                } else if self.snap_dir.is_some() {
                    // A body we cannot even DECODE is the other, and more
                    // realistic, half of the `peer wire ≤ 0.6.0` refusal: the
                    // realistic undecodable body post-0.7.0 is a genuine 0.6.0
                    // `SNAP_BEGIN`, whose fixed part is 34 bytes plus its
                    // config, so it fails `read_snap_begin_body`'s
                    // `SNAP_BEGIN_FIXED_LEN` (122) check and would otherwise
                    // vanish with BOTH refusal counters at zero — exactly the
                    // flag-day symptom an operator needs to see. Same named
                    // refusal, same drop as a wrong `layout` byte. Only counted
                    // on a node that receives snapshots at all (elsewhere kinds
                    // 12/13 are ignored wholesale).
                    self.stats
                        .snap_refused_legacy_peer
                        .fetch_add(1, Ordering::Relaxed);
                    // M14c2 (T10a): ...and once per SESSION, because the
                    // leader re-sends its BEGIN every 20 ms — so the counter
                    // above measures the resend cadence, not the number of
                    // refused sessions, and cannot be read as "how bad is this
                    // flag day". The latch clears on the next decodable BEGIN.
                    if !self.snap_begin_undecodable_latched {
                        self.snap_begin_undecodable_latched = true;
                        self.stats
                            .snap_begin_undecodable
                            .fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            "uc_net: SNAP_BEGIN from {from} could not be decoded ({} body \
                             bytes) -- almost certainly a wire <= 0.6.0 peer; this node cannot \
                             install its snapshot set. Upgrade every node together (the \
                             node<->node wire is a flag day)",
                            d.len().saturating_sub(DATAGRAM_HEADER_LEN)
                        );
                    }
                    self.snap_drop_intake_from(from);
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
                    let _ = route.try_send(CtrlMsg::SnapDone {
                        from,
                        session: b.session,
                    });
                }
            }
            _ => {} // NAK/STATUS with no sender_route installed (follower role)
        }
    }

    /// Begin (or extend) an inbound snapshot transfer: pre-size this artifact's
    /// `.part` and start tracking it. A duplicate BEGIN for an artifact already
    /// announced in this session is a no-op (the sender re-sends one every
    /// 20 ms); a BEGIN for a different session replaces a stale one. Two named
    /// refusals drop the session outright.
    fn snap_begin(&mut self, from: SocketAddr, b: SnapBeginBody) {
        let Some(root) = self.snap_dir.clone() else {
            return; // this node does not receive snapshots
        };
        // Read once, up front: every liveness stamp below is taken under a
        // `&mut self.snap_intake` borrow that excludes `self.now_ns()`.
        let now = self.now_ns();
        if b.layout != SNAP_BEGIN_LAYOUT_V3 {
            // "peer wire ≤ 0.6.0" — a body whose discriminator we do not speak.
            self.stats
                .snap_refused_legacy_peer
                .fetch_add(1, Ordering::Relaxed);
            self.snap_drop_intake_from(from);
            return;
        }
        // The id must be inside the sender's own declared mask, and the two
        // sides' identities must agree at EVERY row, positionally by name —
        // not just at `service_id`'s row (spec §5, §8). `service_id` is
        // peer-controlled and NEVER bounds-checked by the wire format (any
        // `u8`, 0..=255, decodes) even though only rows 0..8 exist. A shift
        // by an id >= 64 is not representable — `checked_shl` folds that into
        // an always-false bit test rather than shifting UB — and the same
        // caution extends to the row we RECORD for diagnostics: `service_id`
        // must NEVER be used to index `identity`/`own_identity` directly
        // (a `SNAP_BEGIN` whose arrays are otherwise equal but whose
        // `service_id` is e.g. 9 would panic the receiver agent on an
        // out-of-bounds index — remotely, and with crypto off). Clamp it
        // into range first.
        let bit = 1u64.checked_shl(b.service_id as u32).unwrap_or(0);
        if b.identity != self.own_identity || b.declared_mask() & bit == 0 {
            let mismatch_row = (0..8).find(|&r| b.identity[r] != self.own_identity[r]);
            let row = mismatch_row.unwrap_or((b.service_id as usize).min(7)) as u8;
            // The arrays can agree at every row while still failing here —
            // `declared_mask() & bit == 0` — when the artifact names a row
            // outside the sender's own declared mask (e.g. a forged
            // `service_id`). That is a different failure from a positional
            // name mismatch, so it gets its own `RefusalKind` (Ruling 5)
            // even though both are counted under the same
            // `snap_refused_declared_mismatch` counter.
            let kind = if mismatch_row.is_none() {
                RefusalKind::ArtifactId
            } else {
                RefusalKind::Identity
            };
            *self
                .stats
                .identity_refusal
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(IdentityRefusal {
                row,
                ours: self.own_identity.get(row as usize).copied().unwrap_or(0),
                theirs: b.identity.get(row as usize).copied().unwrap_or(0),
                ours_version: 0,
                theirs_version: 0,
                kind,
            });
            self.stats
                .snap_refused_declared_mismatch
                .fetch_add(1, Ordering::Release);
            self.snap_drop_intake_from(from);
            return;
        }
        if let Some(own) = &self.own_versions {
            let ours = own();
            if let Some(r) =
                (0..8).find(|&r| ours[r] != 0 && b.version[r] != 0 && ours[r] != b.version[r])
            {
                *self
                    .stats
                    .version_refusal
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(IdentityRefusal {
                    row: r as u8,
                    ours: self.own_identity[r],
                    theirs: b.identity[r],
                    ours_version: ours[r],
                    theirs_version: b.version[r],
                    kind: RefusalKind::Version,
                });
                self.stats
                    .snap_refused_version_mismatch
                    .fetch_add(1, Ordering::Release);
                self.snap_drop_intake_from(from);
                return;
            }
        }
        if b.total_len == 0 {
            return;
        }
        // M14c2 (T10a): a body we can decode AND accept re-arms the
        // undecodable latch — the peer is speaking a wire we can install a set
        // over, so the next undecodable BEGIN is a genuinely new refused
        // session, worth its own count and its own log line. Deliberately
        // BELOW the three refusals above (fix round 1, advisory 6): a
        // decodable-but-REFUSED BEGIN says nothing of the kind.
        self.snap_begin_undecodable_latched = false;
        // A session that already completed HERE: our `SNAP_DONE` was lost and
        // the leader is still re-sending the last artifact's BEGIN while it
        // waits for one. Re-ack instead of re-opening (and re-downloading) a
        // set that is already renamed, installed and signalled.
        if self.snap_last_done.as_ref().is_some_and(|d| {
            d.peer == from
                && d.session == b.session
                && d.artifacts.contains(&(b.service_id, b.snapshot_pos))
        }) {
            self.snap_send_done(from, &b);
            return;
        }
        if let Some(cur) = self.snap_intake.as_mut()
            && cur.peer == from
            && cur.session == b.session
        {
            if cur
                .parts
                .iter()
                .any(|p| p.service_id == b.service_id && p.snapshot_pos == b.snapshot_pos)
            {
                return; // duplicate BEGIN — already announced (the sender re-sends)
            }
            // An artifact's STREAM base is the SUM of its predecessors' lengths,
            // so it can only be placed if it is the NEXT declared id: a BEGIN
            // that skips one means an earlier BEGIN was lost, and placing this
            // one at `announced_len` anyway would give two FSMs each other's
            // bytes — a silent mis-install, not a stall. Drop it and prompt.
            let expect = next_declared_id(
                crate::sender::identity_mask(&cur.identity),
                cur.parts.last().map(|p| p.service_id),
            );
            if Some(b.service_id) != expect {
                let (peer, session, at) = (cur.peer, cur.session, cur.announced_len);
                self.snap_probe_missing_begin(peer, session, at);
                return;
            }
            // The next artifact of the SAME session: it starts where the
            // announced stream currently ends.
            let Some(part) = open_snap_part(&root, &b, cur.announced_len) else {
                // Local I/O, not the peer: counted so a follower that can never
                // open a `.part` is diagnosable rather than a silent NAK loop.
                self.stats
                    .snap_intake_io_failures
                    .fetch_add(1, Ordering::Relaxed);
                return;
            };
            cur.announced_len += part.len;
            cur.parts.push(part);
            // A newly announced artifact is progress: the leader is alive and
            // shipping, so it refreshes the intake's timeout the same way a
            // chunk does (M14c2 T10a).
            cur.last_chunk_ns = now;
            self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // A new session (or one replacing a stale one). Same placement rule: a
        // session's FIRST artifact is the lowest declared id — anything else is
        // a session already under way whose opening BEGIN we never saw.
        if Some(b.service_id) != next_declared_id(b.declared_mask(), None) {
            self.snap_probe_missing_begin(from, b.session, 0);
            return;
        }
        // Replace any stale intake BEFORE opening this one: its `.part` files
        // must not leak, and the new session may re-use the very same path.
        self.snap_discard_intake();
        let Some(part) = open_snap_part(&root, &b, 0) else {
            self.stats
                .snap_intake_io_failures
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        let announced_len = part.len;
        self.snap_intake = Some(SnapIntake {
            peer: from,
            session: b.session,
            identity: b.identity,
            version: b.version,
            received: 0,
            parts: vec![part],
            announced_len,
            got: Rebuilt::new(0),
            nak: NakTimer::new(self.snap_nak_cfg, self.snap_seed ^ b.session as u64),
            config: b.config,
            last_chunk_ns: now,
            last_publish_try_ns: None,
            write_failure_logged: false,
        });
        self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
    }

    /// M14c: drop the in-flight intake, if any, deleting its `.part` files.
    fn snap_discard_intake(&mut self) {
        if let Some(intake) = self.snap_intake.take() {
            discard_snap_parts(intake.parts);
        }
    }

    /// M14c: a named `SNAP_BEGIN` refusal has just been counted — drop the
    /// session it refuses. ONLY this peer's intake: a forged or legacy BEGIN
    /// from any OTHER address must not tear down an unrelated live transfer
    /// (it is counted and the datagram discarded, nothing more). A refusal from
    /// the peer we are actually transferring with does drop the session — its
    /// leader is speaking a wire we cannot finish the transfer on.
    fn snap_drop_intake_from(&mut self, from: SocketAddr) {
        if self
            .snap_intake
            .as_ref()
            .is_some_and(|cur| cur.peer == from)
        {
            self.snap_discard_intake();
        }
    }

    /// M14c: a `SNAP_BEGIN` we cannot place — prompt the leader for the
    /// artifact that starts at STREAM offset `at` by NAKing one byte of it.
    /// The sender targets the artifact its head repair NAK falls in, so this
    /// makes the missing BEGIN the next cycle's re-send. Without it a BEGIN
    /// lost EARLY in the stream would strand the whole session until the
    /// leader's 30 s session timeout: its own 20 ms re-send cadence only ever
    /// covers the artifact it is currently shipping.
    fn snap_probe_missing_begin(&mut self, peer: SocketAddr, session: u32, at: u64) {
        self.send_snap_nak(peer, session, at, 1);
    }

    /// Land one snapshot chunk at its STREAM offset, in whichever announced
    /// artifact contains it; publish any artifact the contiguous frontier has
    /// now passed.
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
        // A chunk always sits inside ONE artifact (the sender never spans a
        // boundary). Anything else — past the announced stream, or for a BEGIN
        // we have not seen — is dropped; the sender re-sends that BEGIN on its
        // own cadence and the peer re-NAKs the bytes.
        let Some(i) = intake
            .parts
            .iter()
            .position(|p| offset >= p.base && end <= p.base + p.len)
        else {
            return;
        };
        let at = offset - intake.parts[i].base;
        let Some(file) = intake.parts[i].file.as_mut() else {
            return; // already renamed — a duplicate repair chunk
        };
        // M14c ruling M(2) / M14c2 (T10a): this is the likeliest ENOSPC site on
        // a joiner — `open_snap_part` `set_len`s the `.part` to its full size
        // but the file is SPARSE, so the blocks are only allocated here, as
        // chunks land. Before this the failure was silent: the bytes were never
        // recorded as landed (correct), the receiver re-NAKed them forever, and
        // the only thing that ever named the stall was the LEADER's 30 s
        // session timeout. Counted with the rest of the intake's local I/O; the
        // operator line is latched per intake so a full disk cannot write one
        // per datagram.
        // Nothing here allocates or clones on the SUCCESS path: the diagnostic
        // strings are built only inside the failure arm.
        if file.seek(SeekFrom::Start(at)).is_err() || file.write_all(payload).is_err() {
            let first = !intake.write_failure_logged;
            intake.write_failure_logged = true;
            if first {
                eprintln!(
                    "uc_net: cannot write snapshot chunk for service {} into {} -- the \
                     transfer cannot finish; check this node's snapshot directory (full, \
                     read-only, or obstructed)",
                    intake.parts[i].service_id,
                    intake.parts[i].part_path.display()
                );
            }
            self.stats
                .snap_intake_io_failures
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        // The clock is read HERE, not on entry: a chunk with no intake, for
        // the wrong peer, outside every announced artifact, or one whose write
        // failed costs no clock read at all. `self.base` is a disjoint field
        // from `self.snap_intake`, so this is `now_ns()`'s body inlined rather
        // than a second borrow of `*self`.
        let now = self.base.elapsed().as_nanos() as u64;
        intake.last_chunk_ns = now;
        intake.got.insert(offset, end);
        // M14c2 (T10a) fix round 1: paced like the duty-cycle re-drive.
        // Unconditional here, this walk re-attempted (and re-failed) a part
        // blocked on a rename for EVERY later chunk of a multi-artifact set —
        // one failing syscall and one counter bump per chunk. `None` (no
        // failed attempt outstanding) still publishes immediately, so a set
        // that completes normally is never delayed.
        let due = intake
            .last_publish_try_ns
            .is_none_or(|t| now.saturating_sub(t) >= SNAP_REDRIVE_INTERVAL_NS);
        if due {
            self.snap_publish_complete_parts(now);
        }
    }

    /// fsync + atomically rename every announced artifact the contiguous
    /// frontier has now covered end to end, then — once EVERY declared id has
    /// landed — complete the session. A torn `.part` is never renamed, so a
    /// reader (the service's gap guard, or AdoptFloor) only ever sees a
    /// complete artifact.
    ///
    /// M14c2 (T10a) fix round 1: this walk returns at the FIRST blocked part,
    /// so on a multi-artifact set it re-attempts (and re-fails) that one part
    /// for every later chunk that lands. It therefore owns
    /// [`SnapIntake::last_publish_try_ns`]: a FAILED attempt stamps `now`
    /// (arming [`SNAP_REDRIVE_INTERVAL_NS`] for both callers), a pass that
    /// blocks on nothing clears it back to `None`. A first attempt, and every
    /// attempt after a clean pass, is therefore immediate — the normal
    /// completion path is never delayed by the pacing.
    fn snap_publish_complete_parts(&mut self, now: u64) {
        let Some(intake) = self.snap_intake.as_mut() else {
            return;
        };
        let contiguous = intake.got.contiguous();
        // Indexed rather than `iter_mut`: the failure arms write
        // `intake.last_publish_try_ns`, which a live `&mut` into
        // `intake.parts` would forbid.
        for k in 0..intake.parts.len() {
            let p = &intake.parts[k];
            if p.done || contiguous < p.base + p.len {
                continue;
            }
            // M14c review round 2: BOTH steps are retried on the next call.
            // `file` is `None` here only when a previous call already synced
            // this artifact and the RENAME is what failed — the `.part` is
            // complete on disk, so the retry is just the rename. Returning with
            // `file: None, done: false` and no retry (the old shape) stranded
            // the artifact forever: no further chunk arrives for a part the
            // frontier has already covered, so nothing ever published it and
            // the whole session hung on `received != services_declared`.
            if let Some(file) = intake.parts[k].file.take() {
                if file.sync_all().is_err() {
                    intake.parts[k].file = Some(file);
                    intake.last_publish_try_ns = Some(now);
                    self.stats
                        .snap_intake_io_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                }
                drop(file);
            }
            if std::fs::rename(&intake.parts[k].part_path, &intake.parts[k].final_path).is_err() {
                intake.last_publish_try_ns = Some(now);
                self.stats
                    .snap_intake_io_failures
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            intake.parts[k].done = true;
            // id < 64: checked in `snap_begin`
            intake.received |= 1u64 << intake.parts[k].service_id;
        }
        // Nothing blocked this pass: disarm, so the next completion publishes
        // the instant its bytes land.
        intake.last_publish_try_ns = None;
        if intake.received != crate::sender::identity_mask(&intake.identity) {
            return; // the set is incomplete — no floor is adopted yet
        }
        self.snap_complete();
    }

    /// Every artifact of the session is renamed: ack with SNAP_DONE, publish the
    /// carried config, and signal the floor — the MINIMUM over the received
    /// positions, which is exactly the node floor the leader shipped from, so
    /// every FSM's own artifact sits at or above it.
    fn snap_complete(&mut self) {
        let Some(intake) = self.snap_intake.take() else {
            return;
        };
        let Some(last) = intake.parts.last() else {
            return;
        };
        let floor = intake
            .parts
            .iter()
            .map(|p| p.snapshot_pos)
            .min()
            .unwrap_or(0);
        // Ack: echo the LAST artifact's SnapBeginBody as SNAP_DONE so the leader
        // closes its session (it keys on `(peer, session)` alone).
        let ack = SnapBeginBody {
            session: intake.session,
            layout: SNAP_BEGIN_LAYOUT_V3,
            service_id: last.service_id,
            snapshot_pos: last.snapshot_pos,
            total_len: last.len,
            identity: intake.identity,
            version: intake.version,
            config: vec![], // the DONE ack carries no config — only SNAP_BEGIN ships it
        };
        // M8 (T17): sealed or dropped. A dropped DONE costs only the leader's
        // session slot (it times out); the local artifacts are already renamed,
        // so the install below must proceed either way. Latch the session as
        // completed first: the leader re-sends its BEGIN until a DONE lands, and
        // `snap_begin` re-acks from this latch instead of re-downloading.
        self.snap_last_done = Some(SnapDone {
            peer: intake.peer,
            session: intake.session,
            artifacts: intake
                .parts
                .iter()
                .map(|p| (p.service_id, p.snapshot_pos))
                .collect(),
        });
        self.snap_send_done(intake.peer, &ack);
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
            slot.store(floor, Ordering::Release);
        }
        // Arm the forward gap-tracker resync: once the consensus agent's AdoptFloor
        // re-primes the shared `append` up to this position, rebuild our tracker
        // forward so we NAK the retained `[floor, frontier)` tail (M6 Task 8).
        self.snap_adopt_pending = Some(floor);
        self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
    }

    /// Ack a session with `SNAP_DONE`. The leader keys the close on
    /// `(peer, session)` alone, so the body simply echoes the artifact the ack
    /// was prompted by (the session's last, or a re-sent BEGIN's own).
    fn snap_send_done(&mut self, peer: SocketAddr, b: &SnapBeginBody) {
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN + SNAP_BEGIN_FIXED_LEN];
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
                session: b.session,
                layout: SNAP_BEGIN_LAYOUT_V3,
                service_id: b.service_id,
                snapshot_pos: b.snapshot_pos,
                total_len: b.total_len,
                identity: b.identity,
                version: b.version,
                config: vec![], // the DONE ack carries no config — only SNAP_BEGIN ships it
            },
        );
        self.seal_and_send(peer, DGRAM_KIND_SNAP_DONE, &mut d);
    }

    /// Build, seal and send one `SNAP_NAK`. `false` = it could not be sealed and
    /// was dropped; the snapshot NAK timer re-fires.
    fn send_snap_nak(&mut self, peer: SocketAddr, session: u32, offset: u64, length: u32) -> bool {
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
            &SnapNakBody {
                session,
                offset,
                length,
            },
        );
        self.seal_and_send(peer, DGRAM_KIND_SNAP_NAK, &mut d)
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
            self.last_published = append;
            self.leader_append = self.leader_append.max(append);
            self.nak.poll(None, self.now_ns()); // disarm the stale below-floor gap
            self.status_at = append;
            self.ap_reported = append;
            self.last_ap_ns = 0; // report the new frontier promptly (see above)
            self.stats
                .truncation_resyncs
                .fetch_add(1, Ordering::Relaxed);
        }
        self.snap_adopt_pending = None;
    }

    /// Emit a SNAP_NAK for the first gap in the inbound transfer (RTT-delayed,
    /// like the main stream). Called once per duty cycle from `upkeep`.
    fn snap_upkeep(&mut self, now: u64) -> bool {
        // M14c review round 2: re-drive a publish that failed on local I/O. A
        // part whose bytes have ALL landed gets no further chunk, so
        // `snap_chunk` — the only other caller — can never retry it; the duty
        // cycle is what turns "the operator cleared the full disk / removed the
        // stray path" into a completed install instead of a permanent stall.
        // Cheap: one `contiguous()` read and a walk of the (single-digit) parts,
        // and only when something is actually outstanding.
        //
        // M14c2 (T10a): paced by [`SNAP_REDRIVE_INTERVAL_NS`] per intake
        // (ruling M(1)). `upkeep` calls this twice per poll iteration, so
        // unpaced a PERSISTENT obstacle cost one to two failing syscalls per
        // busy-spin iteration and made `snap_intake_io_failures` climb at the
        // poll rate — unreadable as the "rising count" signal the docs name.
        let due = self.snap_intake.as_ref().is_some_and(|i| {
            i.last_publish_try_ns
                .is_none_or(|t| now.saturating_sub(t) >= SNAP_REDRIVE_INTERVAL_NS)
                && {
                    let contiguous = i.got.contiguous();
                    i.parts
                        .iter()
                        .any(|p| !p.done && contiguous >= p.base + p.len)
                }
        });
        if due {
            self.snap_publish_complete_parts(now);
        }
        // M14c2 (T10a): a transfer with no sign of life for
        // [`SNAP_INTAKE_TIMEOUT_NS`] is abandoned — the leader died
        // mid-transfer, or its session was refused at its own end. Dropping the
        // intake unlinks its pre-sized `.part` files, which are `set_len`'d to
        // the artifacts' FULL size and would otherwise sit under
        // `snapshots/<id>/` until some later session happened to replace them.
        // The node is still below the floor: it keeps NAKing, and the next
        // session opens a clean intake. Placed AFTER the re-drive so a
        // publish that would have succeeded on this very call is never thrown
        // away.
        if let Some(peer) = self
            .snap_intake
            .as_ref()
            .filter(|i| now.saturating_sub(i.last_chunk_ns) >= SNAP_INTAKE_TIMEOUT_NS)
            .map(|i| i.peer)
        {
            self.snap_discard_intake();
            self.stats
                .snap_intake_abandoned
                .fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "uc_net: abandoning the inbound snapshot transfer from {peer} -- no chunk for \
                 {} s; its unfinished .part files have been removed (any artifact already \
                 published stays) and this node will keep NAKing for a fresh session. If \
                 snapshot intake I/O failures are rising too, the obstacle is THIS node's \
                 snapshot directory",
                SNAP_INTAKE_TIMEOUT_NS / 1_000_000_000
            );
            return false;
        }
        let Some(intake) = self.snap_intake.as_mut() else {
            return false;
        };
        let contiguous = intake.got.contiguous();
        let announced_len = intake.announced_len;
        let gap = intake
            .got
            .first_gap()
            .or({
                if contiguous < announced_len {
                    Some((contiguous, announced_len))
                } else {
                    None
                }
            })
            // M14c: everything ANNOUNCED has landed but the declared set is not
            // complete — we are waiting on the next artifact's `SNAP_BEGIN`.
            // NAK one byte past the announced end: the sender targets the
            // artifact its head NAK falls in, so this asks for that BEGIN. (It
            // is also the ordinary steady state a beat before the leader ships
            // it, where the probe costs one datagram and hurries it along.)
            .or({
                if intake.received != crate::sender::identity_mask(&intake.identity) {
                    Some((announced_len, announced_len + 1))
                } else {
                    None
                }
            });
        let fired = intake.nak.poll(gap, now);
        if let Some((start, end)) = fired {
            let length = (end - start).min(self.cfg.nak_max_bytes as u64) as u32;
            let session = intake.session;
            let peer = intake.peer;
            return self.send_snap_nak(peer, session, start, length);
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
        let fired = self
            .nak
            .poll(gap, now)
            .or_else(|| self.nak.poll(gap, self.now_ns()));
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
            write_nak_body(
                &mut d[DATAGRAM_HEADER_LEN..],
                &NakBody {
                    position: start,
                    length: len,
                },
            );
            // M8 (T17): sealed or dropped. A dropped NAK re-fires on the
            // timer's own backoff — the same recovery a lost NAK already has.
            let leader = self.cfg.leader;
            if self.seal_and_send(leader, DGRAM_KIND_NAK, &mut d) {
                self.stats.naks_sent.fetch_add(1, Relaxed);
                did = true;
            }
        }

        // Single durable load reused by AppendPosition + status below.
        let raw_durable = self.buffer.counters().durable.load_acquire();
        // What we ATTEST toward the leader's commit ranking is the validated
        // prefix, never the raw frontier (see `set_validated_frontier`).
        let durable = match &self.validated_frontier {
            Some(v) => raw_durable.min(v.load(Ordering::Acquire)),
            None => raw_durable,
        };

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
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN + APPEND_POSITION_BODY_LEN];
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
            // Content attestation (protocol 0.5.0): the term we attribute to
            // the byte below the position we are reporting. Sampled from the
            // same publisher as the frontier; a torn pair simply fails the
            // leader's check and is re-sent next cadence.
            write_append_position_body(
                &mut d[DATAGRAM_HEADER_LEN..],
                &AppendPositionBody {
                    durable_term: self
                        .validated_term
                        .as_ref()
                        .map_or(0, |t| t.load(Ordering::Acquire)),
                },
            );
            // M8 (T17): sealed or dropped. The cursors advance ONLY on a
            // real send — a dropped report whose cursor advanced anyway would
            // be lost until `durable` next moved, which on an idle follower
            // is never. Leaving them put means the next duty cycle still sees
            // the threshold crossed and retries, so the report resumes the
            // instant the pairwise session comes up.
            let leader = self.cfg.leader;
            if self.seal_and_send(leader, DGRAM_KIND_APPEND_POSITION, &mut d) {
                self.ap_reported = durable;
                self.last_ap_ns = now;
                self.stats.append_positions_sent.fetch_add(1, Relaxed);
                did = true;
            }
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
                &StatusBody {
                    contiguous_position: contiguous,
                    receive_window: window,
                },
            );
            // M8 (T17): sealed or dropped; cursors advance only on a real
            // send — see the AppendPosition site above for why.
            let leader = self.cfg.leader;
            if self.seal_and_send(leader, DGRAM_KIND_STATUS, &mut d) {
                self.status_at = contiguous;
                self.last_status_ns = now;
                self.stats.statuses_sent.fetch_add(1, Relaxed);
                did = true;
            }
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
// deleted (M4 carry #5): the real `uc_node::Node` never used it — it composes
// leader duty onto the SAME unified `FollowerReceiver` via `set_sender_route`
// (see `sender_route_demuxes_nak_and_status` below), which is now the only
// receiver type in the crate.

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use uc_log::buffer::{Appender, LogBuffer, SliceRead};
    use uc_log::cnc::{CncMeta, CncPage};
    use uc_log::region::Region;
    use uc_protocol::v2::datagram::{
        DATAGRAM_HEADER_LEN, DGRAM_KIND_APPEND_POSITION, DGRAM_KIND_COMMIT_POSITION,
        DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT, DGRAM_KIND_NAK, DGRAM_KIND_STATUS, DatagramHeader,
        NAK_BODY_LEN, STATUS_BODY_LEN, read_nak_body, read_status_body, write_datagram_header,
        write_nak_body, write_status_body,
    };

    const TERM: u32 = 9;

    fn test_cnc(cap: u64) -> Arc<CncPage> {
        CncPage::heap(&CncMeta {
            node_id: 0,
            instance_id: 0,
            app_id: "test".into(),
            buffer_bytes: cap,
            max_payload: 256,
            services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
        })
    }

    fn buffer() -> Arc<LogBuffer> {
        Arc::new(LogBuffer::new(
            Region::heap_zeroed(1 << 16),
            test_cnc(1 << 16),
            256,
        ))
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
            Self {
                sock: FaultSocket::bind("127.0.0.1:0").unwrap(),
            }
        }
        fn addr(&self) -> SocketAddr {
            self.sock.local_addr().unwrap()
        }
        fn send(&mut self, to: SocketAddr, kind: u8, position: u64, term: u32, body: &[u8]) {
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
            write_datagram_header(
                &mut d,
                &DatagramHeader {
                    position,
                    leadership_term_id: term,
                    kind,
                    flags: 0,
                    key_epoch: 0,
                },
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
                        read_datagram_header(&buf).unwrap(),
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

    /// A dummy handshake route for the crypto fixtures below — same
    /// dropped-receiver shape (and same harmlessness) as `dummy_route`.
    /// Tests that INSPECT handshake datagrams keep their own live receiver.
    fn dummy_handshake_route() -> mpsc::SyncSender<HandshakeDatagram> {
        let (tx, _rx) = mpsc::sync_channel(16);
        tx
    }

    fn peer_ids_of(ids: impl IntoIterator<Item = (SocketAddr, NodeId)>) -> PeerIds {
        let p = PeerIds::new();
        p.store(ids);
        p
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
        cfg.nak = NakConfig {
            delay_min_ns: 1,
            delay_max_ns: 2,
            backoff_ns: 1_000_000,
        };
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

    /// As [`follower`], but hands back the live [`TermHandle`] so a test can
    /// bump the term the way the consensus agent does.
    fn follower_with_term(
        b: &Arc<LogBuffer>,
        leader: SocketAddr,
        term: TermHandle,
    ) -> FollowerReceiver {
        let mut cfg = FollowerConfig::new(leader);
        cfg.nak = NakConfig {
            delay_min_ns: 1,
            delay_max_ns: 2,
            backoff_ns: 1_000_000,
        };
        cfg.status_floor_ns = u64::MAX;
        cfg.append_pos_floor_ns = u64::MAX;
        FollowerReceiver::new(
            Arc::clone(b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            cfg,
            term,
            dummy_route(),
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
                got_nak = Some(read_nak_body(&body).unwrap());
            }
        }
        let nak = got_nak.unwrap();
        assert_eq!(nak.position, 0);
        assert_eq!(nak.length as u64, 192);
        // serve the retransmission -> converges, ooo run absorbed
        leader.send(to, DGRAM_KIND_DATA, runs[0].0, TERM, &runs[0].1);
        leader.send(to, DGRAM_KIND_DATA, runs[1].0, TERM, &runs[1].1);
        drive_until(&mut r, || b.counters().append.load_acquire() == 3 * 96);
        assert!(
            r.stats()
                .naks_sent
                .load(std::sync::atomic::Ordering::Relaxed)
                >= 1
        );
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
                let nak = read_nak_body(&body).unwrap();
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
                let s = read_status_body(&body).unwrap();
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
            Arc::new(LogBuffer::new(
                Region::heap_zeroed(4096),
                test_cnc(4096),
                256,
            ))
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
        let lap2_frames: Vec<_> = runs
            .iter()
            .filter(|(p, ..)| (4096..8128).contains(p))
            .cloned()
            .collect();
        let pad2 = runs
            .iter()
            .find(|(p, ..)| *p == 8128)
            .cloned()
            .expect("lap2 padding");
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
        cfg.nak = NakConfig {
            delay_min_ns: 1,
            delay_max_ns: 2,
            backoff_ns: 1_000_000,
        };
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
                let s = read_status_body(&body).unwrap();
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
            assert!(
                Instant::now() < deadline,
                "empty body never counted malformed"
            );
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
        while st
            .dropped_malformed
            .load(std::sync::atomic::Ordering::Relaxed)
            < 1
        {
            assert!(
                Instant::now() < deadline,
                "misaligned datagram never observed"
            );
            r.do_work();
        }
        assert_eq!(
            b.counters().append.load_acquire(),
            0,
            "misaligned position advanced the log"
        );
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
        while st
            .dropped_malformed
            .load(std::sync::atomic::Ordering::Relaxed)
            < 1
        {
            assert!(
                Instant::now() < deadline,
                "overflowing datagram never observed"
            );
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
                // Protocol 0.5.0: the report carries its content attestation.
                // No frontier/term installed in this fixture, so it is the
                // "unattested" encoding (a well-formed body reading 0).
                assert_eq!(body.len(), APPEND_POSITION_BODY_LEN);
                assert_eq!(
                    read_append_position_body(&body),
                    Some(AppendPositionBody { durable_term: 0 })
                );
                break;
            }
        }
        // capture the baseline IMMEDIATELY after the first send, then assert
        // it holds across 50 quiescent cycles: durable unchanged + floor
        // disabled (u64::MAX in the helper) must mean NO re-send — a bug that
        // forgets to update ap_reported re-sends every cycle and fails here
        let sent = r
            .stats()
            .append_positions_sent
            .load(std::sync::atomic::Ordering::Relaxed);
        for _ in 0..50 {
            r.do_work();
        }
        assert_eq!(
            r.stats()
                .append_positions_sent
                .load(std::sync::atomic::Ordering::Relaxed),
            sent,
            "re-sent AppendPosition without a durable advance"
        );
        b.counters().durable.store_release(1920); // next block
        let deadline = Instant::now() + Duration::from_secs(5);
        while r
            .stats()
            .append_positions_sent
            .load(std::sync::atomic::Ordering::Relaxed)
            == sent
        {
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
    //     `uc_consensus::election::tests::follower_commit_gossip_is_monotonic_and_term_checked`.
    //   * NAK/STATUS demux to the sender's control channel: already covered
    //     by `sender_route_demuxes_nak_and_status` below (the M4 upgrade of
    //     the same property onto the unified receiver, pre-dating this task).
    //   * AppendPosition -> consensus `Report`: ported into
    //     `consensus_kinds_route_raw_to_the_consensus_agent` below.
    //   * AppendPosition stale-term drop: no longer receiver-level — node mode
    //     forwards ALL terms of consensus kinds raw by design ("the SM adopts
    //     higher terms"); the term check now lives in the SM and is pinned by
    //     `uc_consensus::election::tests::higher_term_deposes_leader_and_stale_events_ignored`.

    // ----------------------------------------------------------- M4 (Task 7)

    use std::sync::atomic::AtomicBool;
    use uc_protocol::v2::datagram::{
        DGRAM_KIND_REQUEST_VOTE, NakBody, REQUEST_VOTE_BODY_LEN, RequestVoteBody,
        write_request_vote_body,
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
        cfg.nak = NakConfig {
            delay_min_ns: 1,
            delay_max_ns: 2,
            backoff_ns: 1_000_000,
        };
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
            assert!(
                Instant::now() < deadline,
                "old-term DATA never dropped after bump"
            );
            r.do_work();
        }
        assert_eq!(
            b.counters().append.load_acquire(),
            runs[0].2,
            "stale DATA advanced the log"
        );
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
            &RequestVoteBody {
                new_term: TERM + 5,
                last_term: TERM,
                last_durable: 320,
            },
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
        assert_eq!(
            b.counters().commit.load_acquire(),
            0,
            "receiver stored commit locally"
        );
    }

    /// Read-barrier kinds 10/11 route RAW to the consensus agent, carrying the
    /// probe's HEADER term (the node-side stale-leader filter reads it) and the
    /// body's `from` node id. Delivered even when the datagram term differs from
    /// the receiver's own — term adjudication is the SM's job, not the data
    /// plane's (a probe/ack must always reach the barrier).
    #[test]
    fn read_probe_kinds_route_raw_with_header_term() {
        use uc_protocol::v2::datagram::{
            READ_PROBE_BODY_LEN, ReadProbeBody, write_read_probe_body,
        };
        let b = buffer();
        let mut leader = FakeLeader::new();
        let (tx, rx) = mpsc::sync_channel::<NetEvent>(16);
        let mut r = follower_routed(&b, leader.addr(), tx);
        let to = r.local_addr();

        let mut body = vec![0u8; READ_PROBE_BODY_LEN];
        write_read_probe_body(
            &mut body,
            &ReadProbeBody {
                nonce: 0xABCD,
                from: 2,
            },
        );
        // Probe at a term ABOVE the receiver's — must NOT be term-filtered.
        leader.send(to, DGRAM_KIND_READ_PROBE, 0, TERM + 4, &body);
        write_read_probe_body(
            &mut body,
            &ReadProbeBody {
                nonce: 0xABCD,
                from: 1,
            },
        );
        leader.send(to, DGRAM_KIND_READ_PROBE_ACK, 0, TERM, &body);

        let (mut saw_probe, mut saw_ack) = (false, false);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !(saw_probe && saw_ack) {
            assert!(
                Instant::now() < deadline,
                "read-barrier events never routed"
            );
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
            &ConfigProposalBody {
                nonce: 0x1122_3344,
                op: 1,
                id: 9,
                ip: 0x7F00_0001,
                port: 4000,
            },
        );
        // Term deliberately mismatched from the receiver's own — must still route.
        leader.send(to, DGRAM_KIND_CONFIG_PROPOSAL, 0, TERM + 9, &pbuf);

        let mut rbuf = vec![0u8; CONFIG_REPLY_BODY_LEN];
        write_config_reply_body(
            &mut rbuf,
            &ConfigReplyBody {
                nonce: 0x1122_3344,
                status: 0,
                reason: 0,
                version: 1,
            },
        );
        leader.send(to, DGRAM_KIND_CONFIG_REPLY, 0, TERM, &rbuf);

        let (mut saw_proposal, mut saw_reply) = (false, false);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !(saw_proposal && saw_reply) {
            assert!(
                Instant::now() < deadline,
                "config-forward events never routed"
            );
            r.do_work();
            while let Ok(ev) = rx.try_recv() {
                match ev {
                    NetEvent::ConfigProposal { body, .. } => {
                        assert_eq!(
                            body,
                            ConfigProposalBody {
                                nonce: 0x1122_3344,
                                op: 1,
                                id: 9,
                                ip: 0x7F00_0001,
                                port: 4000,
                            }
                        );
                        saw_proposal = true;
                    }
                    NetEvent::ConfigReply { body } => {
                        assert_eq!(
                            body,
                            ConfigReplyBody {
                                nonce: 0x1122_3344,
                                status: 0,
                                reason: 0,
                                version: 1
                            }
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
        cfg.nak = NakConfig {
            delay_min_ns: 1,
            delay_max_ns: 2,
            backoff_ns: 1_000_000,
        };
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

        // gate closed: DATA dropped, frontier unmoved. Start the node at a
        // frontier of 960 with a durable advance to report, the way a restart
        // leaves it — `prime` moves append/durable/sent together, because the
        // archive only ever records what has been appended. (This used to store
        // `durable = 960` with `append == 0`, a state the system cannot reach;
        // `write_run`'s lower bound now rejects writes below `durable`, so the
        // fixture had to become a reachable one.)
        b.counters().prime(960);
        let base = 960u64;
        leader.send(to, DGRAM_KIND_DATA, base, TERM, &runs[0].1);
        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_gated.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "gated DATA never counted");
            r.do_work();
        }
        for _ in 0..50 {
            r.do_work();
        }
        assert_eq!(
            b.counters().append.load_acquire(),
            base,
            "gated DATA advanced the log"
        );
        assert_eq!(
            st.append_positions_sent.load(Relaxed),
            0,
            "AppendPosition escaped while the gate was closed"
        );

        // reopen: DATA accepted AND AppendPosition now flows
        gate.store(true, Relaxed);
        leader.send(to, DGRAM_KIND_DATA, base, TERM, &runs[0].1);
        drive_until(&mut r, || {
            b.counters().append.load_acquire() == base + runs[0].2
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.append_positions_sent.load(Relaxed) == 0 {
            assert!(
                Instant::now() < deadline,
                "AppendPosition never resumed after reopen"
            );
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
    /// A node that has been LEADER has pushed `append`/`durable` far past its
    /// own receive frontier — its appender writes, its archive records, and its
    /// receiver accepts nothing meanwhile (no DATA arrives in a term it leads).
    /// On step-down that stale-low frontier let the next leader's DATA be
    /// accepted at positions the archive had ALREADY RECORDED, rewriting them
    /// under the archive's own cursor.
    ///
    /// Field evidence (2026-08-03, 7 of 50 soak hits, the `end=0` population):
    /// `durable` found exactly 32 B inside a 64 B frame with `sent` marking that
    /// frame's true start — the archive had recorded a 32 B NewTerm there and a
    /// 64 B data frame replaced it afterwards. The archive then fail-stops on
    /// the first frame of its OWN recorded region (`end=0`).
    ///
    /// Two things must hold: the recorded bytes survive, and the follower still
    /// converges (a guard that only refused the write would wedge it NAKing for
    /// recorded positions forever).
    #[test]
    fn a_leader_stint_leaves_no_stale_frontier_to_overwrite_recorded_bytes() {
        use std::sync::atomic::Ordering::Relaxed;
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let runs = frame_runs(&[&[1u8; 64], &[2u8; 64], &[3u8; 64]], 96);

        // Receive one frame the ordinary way: frontier at 96.
        leader.send(to, DGRAM_KIND_DATA, runs[0].0, TERM, &runs[0].1);
        drive_until(&mut r, || b.counters().append.load_acquire() == 96);

        // The leader stint: THIS node's appender writes 96..288 and its archive
        // records all of it. The receiver's tracker is untouched at 96.
        {
            let mut a = Appender::new(Arc::clone(&b), TERM);
            assert_eq!(a.position(), 96);
            a.append(4, 7, &[9u8; 64]).unwrap();
            a.append(4, 8, &[9u8; 64]).unwrap();
        }
        b.counters()
            .durable
            .store_release(b.counters().append.load_acquire());
        let recorded = b.counters().durable.load_acquire();
        assert_eq!(recorded, 288, "the stint appended two frames");
        let before = b.recordable_slice(96, 1 << 20).map(<[u8]>::to_vec);

        // Step down: the next leader replicates ITS framing at 96 — a position
        // this node has already journalled.
        let st = r.stats();
        leader.send(to, DGRAM_KIND_DATA, 96, TERM, &runs[1].1);
        drive_until(&mut r, || st.datagrams.load(Relaxed) == 2);

        // 1. the recorded region is untouched
        assert_eq!(
            b.recordable_slice(96, 1 << 20).map(<[u8]>::to_vec),
            before,
            "a recorded position was rewritten under the archive's cursor"
        );
        // 2. and the receiver has followed the counter, so it can still make
        //    progress rather than NAKing for recorded positions forever.
        assert!(
            st.counter_ahead_resyncs.load(Relaxed) >= 1,
            "the frontier never rebased to the counter — the follower would wedge"
        );
        leader.send(to, DGRAM_KIND_DATA, 288, TERM, &runs[2].1);
        drive_until(&mut r, || b.counters().append.load_acquire() == 384);
    }

    /// The receiver must never move `append` BACKWARD.
    ///
    /// The generation recheck rejects a prime that lands after `gen0` is
    /// sampled. It cannot see a prime that landed just BEFORE that sample —
    /// between the per-datagram resync and the sample, a few instructions wide,
    /// which a 4-core box running twelve busy-spin agents preempts happily.
    /// Then `gen0` already carries the post-prime generation, the recheck finds
    /// nothing to reject, and the pre-prime `rebuilt.contiguous()` is stored
    /// over the freshly primed floor.
    ///
    /// Field evidence (2026-08-02, unmutated `main`, both earlier fixes in
    /// place): `append` published 101,024 B past the last frame anyone actually
    /// wrote. The forensic walk showed 15 term-45 frames tiling 928 B from
    /// `from`, and the first parseable frame past the failure carried **term 1**
    /// — ring content from a previous lap, never written in this generation.
    /// The appender cannot produce that (it would have left term-45 frames), so
    /// the receiver published it.
    ///
    /// The invariant is simple and does not depend on catching the prime at all:
    /// only a prime moves `append` backward, and after a prime the tracker must
    /// be rebased before publishing — so a frontier below the live counter means
    /// a prime we have not caught, and the publish must be dropped.
    #[test]
    fn a_publish_never_moves_the_append_counter_backward() {
        use std::sync::atomic::Ordering::Relaxed;
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let prime_gen = Arc::new(AtomicU64::new(0));
        r.set_prime_generation(Arc::clone(&prime_gen));

        // Frontier to 96.
        let runs = frame_runs(&[&[1u8; 64], &[2u8; 64]], 96);
        leader.send(to, DGRAM_KIND_DATA, runs[0].0, TERM, &runs[0].1);
        drive_until(&mut r, || b.counters().append.load_acquire() == 96);

        // Prime DOWN to 0 without bumping the generation — standing in for a
        // prime whose generation bump was already visible when `gen0` was
        // sampled, i.e. one that landed just before the sample. The recheck has
        // nothing to compare against; only the backward guard can catch this.
        let bh = Arc::clone(&b);
        let fired = Arc::new(AtomicU64::new(0));
        let fh = Arc::clone(&fired);
        r.set_straddle_hook(Box::new(move || {
            if fh.load(Relaxed) == 0 {
                bh.counters().prime(0);
                fh.fetch_add(1, Relaxed);
            }
        }));

        let st = r.stats();
        leader.send(to, DGRAM_KIND_DATA, runs[1].0, TERM, &runs[1].1);
        drive_until(&mut r, || {
            fired.load(Relaxed) == 1 && st.datagrams.load(Relaxed) == 2
        });

        assert_eq!(
            b.counters().append.load_acquire(),
            0,
            "the pre-prime frontier was published over the primed floor — \
             `append` now covers bytes this generation never wrote"
        );
    }

    /// A prime landing BETWEEN two datagrams of one drain must not let the
    /// second publish the pre-prime frontier.
    ///
    /// `resync_after_truncation` ran once at the top of `do_work`, but a cycle
    /// drains up to 64 datagrams and the archive primes on its own thread. A
    /// collapse/truncation landing mid-drain is invisible to the top-of-cycle
    /// check (already passed) AND to `prime_generation` (which catches a prime
    /// straddling a SINGLE datagram — here the prime is complete before the next
    /// datagram's `gen0` is sampled). Every later datagram of that drain then
    /// stores the stale `rebuilt.contiguous()` over the primed floor: `append`
    /// claims a frontier for bytes this term never wrote, and the archive's
    /// frame walk fail-stops on the first byte past what was really written.
    ///
    /// Field signature (2026-08-02, unmutated `main`): `sent == durable == from`
    /// with a 32 B NewTerm at `from` — the prime fingerprint — and `append`
    /// ~88 KB beyond it while only ~1.3 KB is actually framed.
    #[test]
    fn a_prime_between_two_datagrams_of_one_drain_is_not_clobbered() {
        use std::sync::atomic::Ordering::Relaxed;
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let to = r.local_addr();
        let prime_gen = Arc::new(AtomicU64::new(0));
        r.set_prime_generation(Arc::clone(&prime_gen));

        // Frontier to 96 the ordinary way.
        let runs = frame_runs(&[&[1u8; 64], &[2u8; 64], &[3u8; 64]], 96);
        leader.send(to, DGRAM_KIND_DATA, runs[0].0, TERM, &runs[0].1);
        drive_until(&mut r, || b.counters().append.load_acquire() == 96);

        // A and B are BOTH in flight before the drain starts, so they are
        // processed in ONE `do_work` cycle. The hook primes the counters down to
        // 0 (a collapse) while A is between its insert and its store — A is
        // correctly dropped by the generation recheck. B then follows IN THE
        // SAME DRAIN, after the prime is already complete, so nothing straddles
        // it and the top-of-cycle resync ran before either.
        let bh = Arc::clone(&b);
        let pgh = Arc::clone(&prime_gen);
        r.set_straddle_hook(Box::new(move || {
            if pgh.load(Relaxed) == 0 {
                bh.counters().prime(0);
                pgh.fetch_add(1, std::sync::atomic::Ordering::Release);
            }
        }));
        // B sits exactly AT the (stale) frontier A left behind, so it is a
        // FORWARD insert and publishes — the accept rule only rejects positions
        // strictly BELOW the frontier.
        leader.send(to, DGRAM_KIND_DATA, runs[1].0, TERM, &runs[1].1);
        leader.send(to, DGRAM_KIND_DATA, runs[2].0, TERM, &runs[2].1);
        let st = r.stats();
        drive_until(&mut r, || st.datagrams.load(Relaxed) == 3);

        let append = b.counters().append.load_acquire();
        assert_eq!(
            append, 0,
            "append={append} was republished from the pre-prime frontier over a \
             primed floor of 0 — a frontier for bytes this term never wrote"
        );
    }

    /// A term boundary RE-FRAMES the stream above the commit point, so every
    /// out-of-order run recorded under the old term is unreliable: the same
    /// position can carry a 96 B data frame in term T and the next term's 32 B
    /// NewTerm frame in T+1. Both sit ABOVE the contiguous frontier, so the
    /// accept rule (`position < contiguous`) rejects neither, and [`Rebuilt`]
    /// UNIONS their spans because it tracks positions only ("no reliance on
    /// buffer contents", its module doc). The later `write_run` overwrites the
    /// first 32 B and the dead frame's trailing 32 B stay behind as orphaned
    /// payload — so the frontier is published over a span the buffer no longer
    /// tiles with whole frames, and the archive's `recordable_slice` walk (the
    /// only reader that crosses it) fail-stops the node.
    ///
    /// Found 2026-08-02 on unmutated `main`: `RecorderCorrupt`, ~2 hits in 8
    /// append-heavy partition-churn runs, trace `ooo-insert [X, X+64)` then
    /// `ooo-insert [X, X+32)` then a frontier LEAP to `X+64`. NOT issue #6 (a
    /// second cross-thread primer of the counters) — a different plane, in the
    /// receive path, which #6's fix merely stopped masking.
    ///
    /// The fail-stop is the LUCKY outcome: had the orphaned payload's first
    /// word passed as a plausible length, the archive would have recorded
    /// old-term bytes into the journal as current-term data and served them to
    /// any follower doing deep-NAK replay.
    #[test]
    fn a_term_change_discards_out_of_order_runs_framed_by_the_old_term() {
        use std::sync::atomic::Ordering::Relaxed;
        let b = buffer();
        let mut leader = FakeLeader::new();
        let term = term_handle(TERM);
        let mut r = follower_with_term(&b, leader.addr(), Arc::clone(&term));
        let to = r.local_addr();

        // In-order prefix under term T: frontier at 96.
        let runs = frame_runs(&[&[1u8; 64], &[2u8; 64]], 96);
        leader.send(to, DGRAM_KIND_DATA, runs[0].0, TERM, &runs[0].1);
        drive_until(&mut r, || b.counters().append.load_acquire() == 96);

        // Out-of-order, above the frontier: term T's 96 B frame at 192, held
        // behind the gap [96, 192).
        leader.send(to, DGRAM_KIND_DATA, 192, TERM, &runs[1].1);
        let st = r.stats();
        drive_until(&mut r, || st.bytes.load(Relaxed) == 192);

        // The consensus agent adopts term T+1, and its leader re-frames that
        // same position with a 32 B NewTerm frame.
        let new_term = TERM + 1;
        term.store(new_term, Relaxed);
        let nt = {
            let nb = buffer();
            let mut a = Appender::new(Arc::clone(&nb), new_term);
            a.append_new_term().unwrap();
            let mut out = Vec::new();
            let SliceRead::Run(rr) = nb.read_run_validated(0, 96, &mut out) else {
                panic!("new-term run")
            };
            assert_eq!(rr.bytes, 32, "a NewTerm frame is header-only");
            out[..rr.bytes].to_vec()
        };
        leader.send(to, DGRAM_KIND_DATA, 192, new_term, &nt);
        // Wait on the WIRE bytes landing (96 + 96 + 32), never on the fix's own
        // counter — this test must fail on the safety assertion below when the
        // discard is absent, not stall waiting for it.
        drive_until(&mut r, || st.bytes.load(Relaxed) == 224);

        // Fill the gap under the new term; the frontier advances and absorbs
        // whatever out-of-order state survived.
        leader.send(to, DGRAM_KIND_DATA, 96, new_term, &runs[1].1);
        drive_until(&mut r, || b.counters().append.load_acquire() >= 224);

        // The archive's OWN predicate: everything below `append` must walk as
        // whole frames. Pre-fix this is Err(RecordableCorrupt) at 224 with
        // append==288 — the union of the two framings.
        let append = b.counters().append.load_acquire();
        assert_eq!(append, 224, "the old term's 96 B span must not be counted");
        let slice = b.recordable_slice(0, 1 << 20).unwrap_or_else(|c| {
            panic!(
                "archive cannot walk [0, {append}): {}",
                b.corrupt_report(&c)
            )
        });
        assert_eq!(slice.len(), append as usize);
    }

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
        write_nak_body(
            &mut nb,
            &NakBody {
                position: 96,
                length: 192,
            },
        );
        peer.send(to, DGRAM_KIND_NAK, 0, TERM, &nb);
        let mut sb = [0u8; STATUS_BODY_LEN];
        write_status_body(
            &mut sb,
            &StatusBody {
                contiguous_position: 4096,
                receive_window: 1 << 20,
            },
        );
        peer.send(to, DGRAM_KIND_STATUS, 0, TERM, &sb);
        // a stale-term NAK must be dropped, never demuxed
        peer.send(to, DGRAM_KIND_NAK, 0, TERM - 1, &nb);

        let mut nak = None;
        let mut status = None;
        let deadline = Instant::now() + Duration::from_secs(5);
        while nak.is_none() || status.is_none() {
            assert!(
                Instant::now() < deadline,
                "control never demuxed to the sender"
            );
            r.do_work();
            while let Ok(m) = rx.try_recv() {
                match m {
                    CtrlMsg::Nak { .. } => nak = Some(m),
                    CtrlMsg::Status { .. } => status = Some(m),
                    _ => {} // snapshot-session control not exercised by this test
                }
            }
        }
        assert!(matches!(
            nak,
            Some(CtrlMsg::Nak {
                position: 96,
                length: 192,
                ..
            })
        ));
        assert!(matches!(
            status,
            Some(CtrlMsg::Status {
                contiguous: 4096,
                ..
            })
        ));
        use std::sync::atomic::Ordering::Relaxed;
        let deadline = Instant::now() + Duration::from_secs(5);
        while r.stats().dropped_stale_term.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "stale-term NAK never observed");
            r.do_work();
        }
        assert!(
            rx.try_recv().is_err(),
            "stale-term control leaked to the sender"
        );
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
    // genuine UDP socket pair, using ONLY `uc_crypto`'s public API (the
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
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_net_tests")
            })
            .join("uc2-net-receiver-crypto")
            .join(format!("{tag}-{seq}"));
        std::fs::create_dir_all(&dir).unwrap();
        assert!(
            !dir.starts_with("/tmp"),
            "test scratch must not live on tmpfs: {dir:?}"
        );
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
    /// throwaway `uc_crypto::identity::Identity` — `uc_net` has no X25519
    /// dependency of its own (nor should it gain one just for test fixture
    /// plumbing); `Identity::public_bytes` is already the crate's own public
    /// accessor for exactly this.
    fn identity_public(tag: &str, private: [u8; 32]) -> [u8; 32] {
        let dir = crypto_scratch_dir(tag);
        let key_path = dir.join("node.key");
        write_key_file(&key_path, private);
        uc_crypto::identity::Identity::load(&key_path)
            .unwrap()
            .public_bytes()
    }

    /// Minimal standard-alphabet base64 WITH padding, matching
    /// `uc_crypto::identity`'s allowlist parser (which uses the `base64`
    /// crate's `STANDARD` engine) — hand-rolled here rather than adding a
    /// `base64` dev-dependency to `uc_net` just for one test fixture's
    /// allowlist-file text. 32 bytes in, 44 base64 chars out (one trailing
    /// `=`), same as any other X25519 public key this codebase writes.
    fn b64_32(bytes: &[u8; 32]) -> String {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = *chunk.get(1).unwrap_or(&0) as u32;
            let b2 = *chunk.get(2).unwrap_or(&0) as u32;
            let n = (b0 << 16) | (b1 << 8) | b2;
            out.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
            out.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
            out.push(if chunk.len() > 1 {
                ALPHABET[((n >> 6) & 0x3F) as usize] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                ALPHABET[(n & 0x3F) as usize] as char
            } else {
                '='
            });
        }
        out
    }

    fn crypto_shared(
        tag: &str,
        self_id: NodeId,
        private: [u8; 32],
        allow: &[(NodeId, [u8; 32])],
    ) -> uc_crypto::SharedTransport {
        let dir = crypto_scratch_dir(tag);
        let key_path = dir.join("node.key");
        write_key_file(&key_path, private);
        let allow_path = dir.join("allowlist");
        let mut text = String::new();
        for (id, public) in allow {
            text.push_str(&format!("{id} {}\n", b64_32(public)));
        }
        std::fs::write(&allow_path, text).unwrap();
        let cfg = uc_crypto::CryptoConfig::Enabled {
            key_path,
            allowlist_path: allow_path,
            rotation: uc_crypto::rotation::RotationPolicy::default(),
        };
        uc_crypto::SharedTransport::new(&cfg, self_id)
            .unwrap()
            .unwrap()
    }

    /// Builds a real PEER (`PEER_ID`) and RECEIVER (`RECV_ID`) `SharedTransport`
    /// pair, drives a genuine Noise-IK handshake between them to completion,
    /// then mints a group key on the peer and delivers it to the receiver —
    /// all through `SharedTransport`'s public forwarders only (`initiate`/
    /// `on_handshake_message`/`on_group_key_message`), never by reaching
    /// into private crate internals (this IS a different crate). Returns
    /// `(receiver's SharedTransport, peer's SharedTransport, the real minted
    /// epoch)`. T17 returns the peer's whole `SharedTransport` rather than
    /// just its `SendHalf`: the receiver under test now SEALS its own
    /// outgoing control datagrams, so the fixture peer needs a `ReceiveHalf`
    /// to open them with as well.
    fn established_crypto_pair(
        tag: &str,
    ) -> (uc_crypto::SharedTransport, uc_crypto::SharedTransport, u16) {
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
                if let uc_crypto::HandshakeAction::Send { to, kind, body } = act {
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

        // `GroupPlane::next_epoch` now starts at 1, not 0 -- epoch 0 is
        // reserved as the wire's cleartext sentinel (`group.rs`'s
        // `GroupPlane::new` doc; the fix for the review finding that this
        // very fixture used to dodge by minting twice). A single mint
        // already gives a non-zero epoch; no throwaway mint needed anymore.
        let (epoch, mint_acts) = peer.mint_group_key(&[RECV_ID], 0);
        assert_ne!(epoch, 0, "epoch 0 is reserved and must never be minted");
        for act in mint_acts {
            let uc_crypto::HandshakeAction::Send { to, body, .. } = act else {
                panic!("mint must emit a Send action")
            };
            assert_eq!(to, RECV_ID);
            let reply = recv.on_group_key_message(PEER_ID, &body);
            for r in reply {
                let uc_crypto::HandshakeAction::Send { body: rbody, .. } = r else {
                    panic!("a well-formed delivery must ack back")
                };
                peer.on_group_key_message(RECV_ID, &rbody);
            }
        }

        (recv, peer, epoch)
    }

    /// T11 review round 1, finding 1: a mint-once-style fixture that
    /// reaches REAL epoch 0 on the receiver's schedule, despite
    /// `GroupPlane::mint` no longer being able to produce epoch 0 itself
    /// (fix 2 of the same review round — 0 is now reserved, see
    /// `group.rs`'s `GroupPlane::new` doc). This is not a synthetic
    /// shortcut: it hand-crafts a real, well-formed `HS_KEY` DELIVERY body
    /// for epoch 0, per `uc_crypto::group`'s own documented wire format
    /// (`[1B type=0][2B epoch LE][32B group key]`), and feeds it through
    /// the SAME `on_group_key_message` entry point a real opened HS_KEY
    /// datagram would use. `GroupPlane::on_key_message`'s install path has
    /// no "refuse epoch 0" guard (nor should it grow one just for this: the
    /// receive-side shape check this fixture exists to test must hold
    /// regardless of how epoch 0 ever got into a schedule -- a mismatched
    /// peer running older code, a bug elsewhere, or exactly this fixture --
    /// not merely because THIS crate's own minter now avoids it).
    ///
    /// Returns `(receiver's SharedTransport, the AES key that seals as
    /// PEER_ID under epoch 0)` — the caller manually seals with
    /// `uc_crypto::seal::seal_in_place` (like `send_sealed_under_epoch`
    /// already does for OTHER epochs) rather than through a `SendHalf`,
    /// since `SendHalf::seal`'s `sealing_epoch()` is driven by `mint`/fold
    /// state that (correctly, post-fix-2) can never point at epoch 0.
    fn established_crypto_pair_with_forced_epoch_zero(
        tag: &str,
    ) -> (uc_crypto::SharedTransport, [u8; 32]) {
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
                if let uc_crypto::HandshakeAction::Send { to, kind, body } = act {
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

        let key_bytes = [0x5Au8; 32];
        let mut body = vec![0u8]; // MSG_KEY = 0 (delivery)
        body.extend_from_slice(&0u16.to_le_bytes()); // epoch = 0
        body.extend_from_slice(&key_bytes);
        let reply = recv.on_group_key_message(PEER_ID, &body);
        assert!(!reply.is_empty(), "a well-formed delivery must ack back");

        // The AES key a real seal under (epoch 0, sender = PEER_ID) would
        // use -- derived exactly as `SendHalf::seal_group` derives it,
        // using the peer's REAL (OS-RNG, per-process) boot salt, reachable
        // via the `boot_salt()` getter added for exactly this purpose.
        let group_key = uc_crypto::schedule::GroupKey::new(key_bytes);
        let seal_key = uc_crypto::schedule::derive_send_key(&group_key, PEER_ID, &peer.boot_salt());

        (recv, seal_key)
    }

    /// A crypto-capable fake leader endpoint: a real, established
    /// `uc_crypto::SendHalf` plus a raw socket, able to send both
    /// correctly-sealed traffic and deliberately malformed/forged/replayed
    /// datagrams for T11's negative tests.
    struct CryptoPeer {
        sock: FaultSocket,
        send: uc_crypto::SendHalf,
        /// T17: opens what the RECEIVER seals (its NAK/STATUS/APPEND_POSITION
        /// /SNAP_NAK/SNAP_DONE are `Scope::Pairwise` sealed datagrams now).
        recv: uc_crypto::ReceiveHalf,
        epoch: u16,
        /// T17: datagrams of a kind `await_raw` was not asked for yet.
        stash: Vec<Vec<u8>>,
    }

    impl CryptoPeer {
        fn header(position: u64, kind: u8, key_epoch: u16) -> DatagramHeader {
            DatagramHeader {
                position,
                leadership_term_id: TERM,
                kind,
                flags: 0,
                key_epoch,
            }
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
            uc_crypto::seal::seal_in_place(&mut d, &[0x99u8; 32], 1).unwrap();
            self.sock.send_to(&d, to).unwrap();
        }

        /// Sealed (under an arbitrary key -- it never gets that far), but
        /// stamped with an epoch the receiver never minted or received.
        fn send_sealed_under_epoch(
            &mut self,
            to: SocketAddr,
            epoch: u16,
            position: u64,
            payload: &[u8],
        ) {
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
            write_datagram_header(&mut d, &Self::header(position, DGRAM_KIND_DATA, epoch));
            d.extend_from_slice(payload);
            uc_crypto::seal::seal_in_place(&mut d, &[0x77u8; 32], 1).unwrap();
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
        let (r, peer, b, _ids) = receiver_with_crypto_ids(true);
        (r, peer, b)
    }

    /// As `receiver_with_crypto`, but hands back the live [`PeerIds`] handle
    /// and lets the caller start with an EMPTY map — the M7 runtime-node-add
    /// shape (T12): a peer whose address this node had no mapping for at
    /// construction, published later without a restart.
    fn receiver_with_crypto_ids(
        registered: bool,
    ) -> (FollowerReceiver, CryptoPeer, Arc<LogBuffer>, PeerIds) {
        let (recv_shared, peer_shared, epoch) = established_crypto_pair("recv-with-crypto");
        let b = buffer();
        let peer_sock = FaultSocket::bind("127.0.0.1:0").unwrap();
        let peer_addr = peer_sock.local_addr().unwrap();

        let mut cfg = FollowerConfig::new(peer_addr);
        cfg.nak = NakConfig {
            delay_min_ns: 1,
            delay_max_ns: 2,
            backoff_ns: 1_000_000,
        };
        cfg.status_floor_ns = u64::MAX;
        cfg.append_pos_floor_ns = u64::MAX;

        let recv_half = recv_shared.receive_half();
        let peer_ids = if registered {
            peer_ids_of([(peer_addr, PEER_ID)])
        } else {
            PeerIds::new()
        };
        let r = FollowerReceiver::with_crypto(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            cfg,
            term_handle(TERM),
            dummy_route(),
            Some(CryptoIntake {
                half: recv_half,
                peer_ids: peer_ids.clone(),
                handshake: dummy_handshake_route(),
                transport: recv_shared.clone(),
            }),
        );

        let peer = CryptoPeer {
            sock: peer_sock,
            send: peer_shared.send_half(),
            recv: peer_shared.receive_half(),
            epoch,
            stash: Vec::new(),
        };
        (r, peer, b, peer_ids)
    }

    /// T11 review round 1, finding 1: a receiver whose schedule holds a
    /// REAL, genuinely epoch-0 group key (see
    /// `established_crypto_pair_with_forced_epoch_zero`'s doc) plus a raw
    /// peer socket and the AES key needed to seal AS `PEER_ID` under that
    /// epoch by hand (`uc_crypto::seal::seal_in_place`, same pattern
    /// `CryptoPeer::send_sealed_under_epoch` already uses for other
    /// epochs).
    fn receiver_with_forced_epoch_zero() -> (FollowerReceiver, FaultSocket, [u8; 32], Arc<LogBuffer>)
    {
        let (recv_shared, seal_key) =
            established_crypto_pair_with_forced_epoch_zero("forced-epoch-zero");
        let b = buffer();
        let peer_sock = FaultSocket::bind("127.0.0.1:0").unwrap();
        let peer_addr = peer_sock.local_addr().unwrap();

        let mut cfg = FollowerConfig::new(peer_addr);
        cfg.nak = NakConfig {
            delay_min_ns: 1,
            delay_max_ns: 2,
            backoff_ns: 1_000_000,
        };
        cfg.status_floor_ns = u64::MAX;
        cfg.append_pos_floor_ns = u64::MAX;

        let recv_half = recv_shared.receive_half();
        let r = FollowerReceiver::with_crypto(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            cfg,
            term_handle(TERM),
            dummy_route(),
            Some(CryptoIntake {
                half: recv_half,
                peer_ids: peer_ids_of([(peer_addr, PEER_ID)]),
                handshake: dummy_handshake_route(),
                transport: recv_shared.clone(),
            }),
        );

        (r, peer_sock, seal_key, b)
    }

    #[test]
    fn a_real_epoch_zero_sealed_data_datagram_is_admitted_not_diagnosed_as_cleartext() {
        // T11 review round 1, finding 1: the discriminating conjunct
        // (`key_epoch == 0 AND too-short-to-be-sealed`) was untested on its
        // OWN terms -- every existing fixture dodges epoch 0 entirely, so
        // the naive, WRONG guard (`key_epoch == 0` alone) passed all 70
        // tests. This is the "reaches real epoch 0" half: a genuinely
        // 46-byte SEALED DATA datagram (well past the 40-byte floor) under
        // epoch 0 must be admitted exactly like any other epoch, not
        // diagnosed as a cleartext peer.
        use Ordering::Relaxed;
        let (mut r, mut peer_sock, seal_key, b) = receiver_with_forced_epoch_zero();
        let to = r.local_addr();

        let runs = frame_runs(&[b"aaaa"], 4096);
        let (pos, bytes, advance) = &runs[0];
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: *pos,
                leadership_term_id: TERM,
                kind: DGRAM_KIND_DATA,
                flags: 0,
                key_epoch: 0,
            },
        );
        d.extend_from_slice(bytes);
        uc_crypto::seal::seal_in_place(&mut d, &seal_key, 1).unwrap();
        assert!(
            d.len() >= DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD,
            "must be a genuinely sealed-length datagram"
        );
        peer_sock.send_to(&d, to).unwrap();

        drive_until(&mut r, || b.counters().append.load_acquire() == *advance);
        let st = r.stats();
        assert_eq!(
            st.peer_appears_cleartext.load(Relaxed),
            0,
            "a real epoch-0 seal must not be misdiagnosed"
        );
        assert_eq!(st.dropped_auth_failed.load(Relaxed), 0);
        assert_eq!(st.dropped_unknown_epoch.load(Relaxed), 0);
    }

    #[test]
    fn a_real_epoch_zero_sealed_heartbeat_at_exactly_the_forty_byte_floor_is_admitted() {
        // The zero-margin boundary case: an empty-payload HEARTBEAT seals to
        // EXACTLY `DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD` (40) bytes -- the
        // shortest a validly sealed frame can ever be, and exactly the
        // length the cleartext-shape check's `n < 40` compares against. If
        // that boundary were off by one (`<=` instead of `<`), this is the
        // one case that would catch it: real, minimal, sealed, epoch 0.
        use Ordering::Relaxed;
        let (mut r, mut peer_sock, seal_key, _b) = receiver_with_forced_epoch_zero();
        let to = r.local_addr();

        let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: 0,
                leadership_term_id: TERM,
                kind: DGRAM_KIND_HEARTBEAT,
                flags: 0,
                key_epoch: 0,
            },
        );
        uc_crypto::seal::seal_in_place(&mut d, &seal_key, 1).unwrap();
        assert_eq!(
            d.len(),
            DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD,
            "the minimal sealed frame is exactly 40 bytes"
        );
        peer_sock.send_to(&d, to).unwrap();

        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        // HEARTBEAT carries no counter to drive `append`; `datagrams` is the
        // "reached on_datagram" signal (see the T17-allowance test's own
        // doc for the same idiom).
        while st.datagrams.load(Relaxed) < 1 {
            assert!(
                Instant::now() < deadline,
                "the 40-byte epoch-0 HEARTBEAT was never admitted"
            );
            r.do_work();
        }
        assert_eq!(
            st.peer_appears_cleartext.load(Relaxed),
            0,
            "a real epoch-0 seal must not be misdiagnosed"
        );
        assert_eq!(st.dropped_auth_failed.load(Relaxed), 0);
        assert_eq!(st.dropped_unknown_epoch.load(Relaxed), 0);
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
        assert_eq!(
            &s[32..36],
            b"aaaa",
            "downstream sees plaintext, byte-identical to the cleartext path"
        );
    }

    /// M8 (Task 12), the M7 runtime-node-add shape: a peer whose address had
    /// no `NodeId` mapping when this receiver was constructed must become
    /// resolvable the moment membership publishes it — WITHOUT a restart.
    ///
    /// Discriminating on purpose: the FIRST half asserts the datagram is
    /// dropped as `dropped_unknown_peer` (so the fixture genuinely starts
    /// with no mapping — a fixture that silently registered the peer would
    /// make the second half vacuous), and the second half asserts the very
    /// same peer's next datagram lands in the log buffer after nothing but a
    /// `PeerIds::store`. Reverting `refresh_peer_ids` to a no-op fails the
    /// second half; never publishing an empty map fails the first.
    #[test]
    fn a_peer_id_published_after_construction_is_resolvable_without_a_restart() {
        use Ordering::Relaxed;
        let (mut r, mut peer, b, ids) = receiver_with_crypto_ids(false);
        let to = r.local_addr();
        let peer_addr = peer.sock.local_addr().unwrap();
        let runs = frame_runs(&[b"aaaa", b"bb"], 4096);

        // Unregistered: a genuinely well-formed sealed DATA is refused.
        let (pos, bytes, _) = &runs[0];
        peer.send_sealed_data(to, *pos, bytes);
        let st = r.stats();
        let deadline = Instant::now() + Duration::from_secs(5);
        while st.dropped_unknown_peer.load(Relaxed) < 1 {
            assert!(Instant::now() < deadline, "unregistered peer never counted");
            r.do_work();
        }
        assert_eq!(
            b.counters().append.load_acquire(),
            0,
            "nothing was admitted"
        );

        // Membership publishes the joiner. No restart, no reconstruction.
        ids.store([(peer_addr, PEER_ID)]);
        let (pos, bytes, advance) = &runs[0];
        peer.send_sealed_data(to, *pos, bytes);
        drive_until(&mut r, || b.counters().append.load_acquire() == *advance);
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
        assert_eq!(
            b.counters().append.load_acquire(),
            0,
            "forged bytes never reach the log buffer"
        );
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
        assert_eq!(
            b.counters().append.load_acquire(),
            *advance,
            "the replay did not double-apply"
        );
        assert_eq!(
            st.dropped_dup.load(Relaxed),
            dup0,
            "the replay must never reach on_datagram at all, not merely be re-rejected there as a dup"
        );
        assert_eq!(
            st.dropped_malformed.load(Relaxed),
            malformed0,
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
        assert_eq!(
            st.dropped_auth_failed.load(Relaxed),
            0,
            "distinguishable from a generic auth failure"
        );
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
            assert!(
                Instant::now() < deadline,
                "unknown-epoch datagram never counted"
            );
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
        // Every other test's peer is registered via `CryptoIntake::peer_ids` (built
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
            assert!(
                Instant::now() < deadline,
                "datagram from an unregistered address never counted"
            );
            r.do_work();
        }
        assert_eq!(st.dropped_unknown_peer.load(Relaxed), 1);
        assert_eq!(
            st.dropped_auth_failed.load(Relaxed),
            1,
            "folded into the mandated auth-failed bucket too"
        );
        assert_eq!(
            b.counters().append.load_acquire(),
            0,
            "an unresolvable sender's bytes never land"
        );
    }

    // ======================================================================
    // M8 Task 17: this receiver's OWN outgoing control datagrams
    // ======================================================================

    impl CryptoPeer {
        /// A real, correctly-sealed PAIRWISE datagram (the shape T17's
        /// snapshot session actually uses on the leader side).
        fn send_sealed_pairwise(&mut self, to: SocketAddr, kind: u8, position: u64, body: &[u8]) {
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
            write_datagram_header(&mut d, &Self::header(position, kind, 0));
            d.extend_from_slice(body);
            let now = self.send.now_ns();
            self.send.seal(kind, Some(RECV_ID), &mut d, now).unwrap();
            self.sock.send_to(&d, to).unwrap();
        }

        /// Wait for a raw datagram of `kind` from the receiver under test,
        /// pumping it in between. Returns the WIRE bytes (still sealed).
        /// Datagrams of OTHER kinds read along the way are stashed, not
        /// discarded — the three control kinds this drains for are emitted in
        /// an order the test does not control, and dropping the ones that
        /// arrive early would make the test flaky rather than discriminating.
        fn await_raw(&mut self, r: &mut FollowerReceiver, kind: u8) -> Option<Vec<u8>> {
            if let Some(i) = self
                .stash
                .iter()
                .position(|d| read_datagram_header(d).unwrap().kind == kind)
            {
                return Some(self.stash.remove(i));
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut buf = [0u8; 4096];
            while Instant::now() < deadline {
                r.do_work();
                while let Some((n, _)) = self.sock.recv_from(&mut buf).unwrap() {
                    if n < DATAGRAM_HEADER_LEN {
                        continue;
                    }
                    if read_datagram_header(&buf[..n]).unwrap().kind == kind {
                        return Some(buf[..n].to_vec());
                    }
                    self.stash.push(buf[..n].to_vec());
                }
                std::thread::yield_now();
            }
            None
        }

        /// `await_raw`, then assert the datagram is a REAL seal by opening it
        /// on this peer's own `ReceiveHalf`. Returns `(wire bytes, plaintext
        /// body)`.
        fn await_sealed(&mut self, r: &mut FollowerReceiver, kind: u8) -> (Vec<u8>, Vec<u8>) {
            let wire = self
                .await_raw(r, kind)
                .unwrap_or_else(|| panic!("no kind-{kind} arrived"));
            let mut d = wire.clone();
            let n = d.len();
            let len = self
                .recv
                .open_slice(RECV_ID, &mut d, n)
                .unwrap_or_else(|e| panic!("kind {kind} did not open as a sealed datagram: {e}"));
            (wire, d[DATAGRAM_HEADER_LEN..len].to_vec())
        }
    }

    /// NAK, STATUS and APPEND_POSITION — the three the follower sends to its
    /// leader every duty cycle. All `Scope::Pairwise`, all cleartext before
    /// T17.
    ///
    /// Discriminating on the CIPHERTEXT REGION specifically, not on
    /// whole-datagram inequality: the 16-byte `DATAGRAM_HEADER_LEN` is
    /// cleartext in both modes, so `sealed != cleartext` would pass even if
    /// the body went out verbatim. Each assertion compares the opened plaintext against the
    /// body the pre-T17 code put on the wire, and separately asserts that
    /// exact body is NOT findable in the wire bytes.
    #[test]
    fn the_followers_nak_status_and_append_position_are_all_sealed() {
        let (mut r, mut peer, b) = receiver_with_crypto();
        // The shared fixture pins the STATUS/AppendPosition TIME floors to
        // `u64::MAX` (so its own tests are not perturbed by background
        // control traffic); this test needs all three kinds to actually fire.
        r.cfg.status_floor_ns = 1;
        r.cfg.append_pos_floor_ns = 1;
        let to = r.local_addr();

        // A gap: send frame 2 without frame 1, so the NAK timer arms.
        let runs = frame_runs(&[b"aaaa", b"bbbb"], 32);
        assert!(runs.len() >= 2, "fixture must produce two separate runs");
        peer.send_sealed_data(to, runs[1].0, &runs[1].1);

        let (nak_wire, nak_body) = peer.await_sealed(&mut r, DGRAM_KIND_NAK);
        let nak = read_nak_body(&nak_body).unwrap();
        assert_eq!(
            nak.position, 0,
            "the follower NAKs from its contiguous frontier"
        );
        let mut expect = vec![0u8; NAK_BODY_LEN];
        write_nak_body(
            &mut expect,
            &NakBody {
                position: nak.position,
                length: nak.length,
            },
        );
        assert!(
            !nak_wire
                .windows(NAK_BODY_LEN)
                .any(|w| w == expect.as_slice()),
            "the NAK body must not be readable on the wire"
        );
        assert_eq!(
            nak_wire.len(),
            DATAGRAM_HEADER_LEN + NAK_BODY_LEN + CRYPTO_OVERHEAD,
            "exactly the counter+tag overhead, nothing else"
        );

        let (st_wire, st_body) = peer.await_sealed(&mut r, DGRAM_KIND_STATUS);
        let status = read_status_body(&st_body).unwrap();
        let mut expect = vec![0u8; STATUS_BODY_LEN];
        write_status_body(&mut expect, &status);
        assert!(
            !st_wire
                .windows(STATUS_BODY_LEN)
                .any(|w| w == expect.as_slice()),
            "the STATUS body must not be readable on the wire"
        );

        // APPEND_POSITION's position rides in the HEADER, which stays
        // cleartext by design (the seal authenticates it as AAD rather than
        // hiding it). Since protocol 0.5.0 it also carries an 8-byte content
        // attestation body, which IS sealed like any other payload — so the
        // discriminating assertions are that it OPENS and that its length is
        // exactly header + body + crypto overhead.
        let (ap_wire, ap_body) = peer.await_sealed(&mut r, DGRAM_KIND_APPEND_POSITION);
        assert_eq!(ap_body.len(), APPEND_POSITION_BODY_LEN);
        assert_eq!(
            ap_wire.len(),
            DATAGRAM_HEADER_LEN + APPEND_POSITION_BODY_LEN + CRYPTO_OVERHEAD,
            "sealed frame is header + attestation body + counter + tag"
        );
        let _ = b;
    }

    /// A scratch directory on REAL DISK, never `/tmp` (RAM-backed tmpfs with no
    /// swap on the dev box — CLAUDE.md). `CARGO_TARGET_TMPDIR` is set only for
    /// integration-test binaries and these are inline `#[cfg(test)]` unit tests
    /// in the lib target, so this falls back to a package-relative `target/`
    /// directory — the same shape as `uc_node/src/audit.rs`'s helper.
    fn snap_scratch_dir() -> tempfile::TempDir {
        let root = std::env::var("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_net_tests")
            });
        assert!(
            !root.starts_with("/tmp"),
            "test scratch must not live on tmpfs: {}",
            root.display()
        );
        std::fs::create_dir_all(&root).expect("scratch root");
        tempfile::Builder::new()
            .prefix("uc2-snap-")
            .tempdir_in(&root)
            .expect("tempdir")
    }

    /// The other direction of a snapshot session — the receiving node's own
    /// replies. T11's allowance explicitly did NOT cover these, so before
    /// T17 a session stalled on its first lost chunk and never signalled
    /// completion.
    #[test]
    fn the_snapshot_intakes_snap_nak_and_snap_done_are_sealed() {
        let (mut r, mut peer, _b) = receiver_with_crypto();
        let dir = snap_scratch_dir();
        r.set_snapshot_intake(
            dir.path().to_path_buf(),
            ident(0b1),
            Arc::new(|| [0u32; 8]),
            None,
        );
        let to = r.local_addr();

        // Open a session of 64 bytes, then deliver ONLY the second half so a
        // gap at [0,32) forces a SNAP_NAK.
        const TOTAL: u64 = 64;
        let mut begin = vec![0u8; SNAP_BEGIN_FIXED_LEN];
        write_snap_begin_body(
            &mut begin,
            &SnapBeginBody {
                session: 7,
                layout: SNAP_BEGIN_LAYOUT_V3,
                service_id: 0,
                snapshot_pos: 4096,
                total_len: TOTAL,
                identity: ident(0b1),
                version: [0; 8],
                config: vec![],
            },
        );
        peer.send_sealed_pairwise(to, DGRAM_KIND_SNAP_BEGIN, 0, &begin);
        peer.send_sealed_pairwise(to, DGRAM_KIND_SNAP_CHUNK, 32, &[0xEEu8; 32]);

        let (nak_wire, nak_body) = peer.await_sealed(&mut r, DGRAM_KIND_SNAP_NAK);
        let nak = read_snap_nak_body(&nak_body).expect("a well-formed SNAP_NAK body");
        assert_eq!(nak.session, 7);
        assert_eq!(nak.offset, 0, "the gap is the first 32 bytes");
        assert_eq!(
            nak_wire.len(),
            DATAGRAM_HEADER_LEN + SNAP_NAK_BODY_LEN + CRYPTO_OVERHEAD,
            "exactly the counter+tag overhead"
        );

        // Fill the gap; the intake completes and acks SNAP_DONE.
        peer.send_sealed_pairwise(to, DGRAM_KIND_SNAP_CHUNK, 0, &[0xDDu8; 32]);
        let (done_wire, done_body) = peer.await_sealed(&mut r, DGRAM_KIND_SNAP_DONE);
        let done = read_snap_begin_body(&done_body).expect("a well-formed SNAP_DONE body");
        assert_eq!(done.session, 7);
        assert_eq!(done.snapshot_pos, 4096);
        assert_eq!(done.service_id, 0);
        assert_eq!(done.declared_mask(), 0b1);
        assert_eq!(
            done_wire.len(),
            DATAGRAM_HEADER_LEN + SNAP_BEGIN_FIXED_LEN + CRYPTO_OVERHEAD,
            "exactly the counter+tag overhead"
        );
        assert!(
            dir.path().join("0").join("snap-4096.ultsnap").exists(),
            "the sealed session actually completed end to end"
        );
    }

    /// Fail-closed: with no `SocketAddr -> NodeId` entry for the leader there
    /// is no pairwise key, and NOTHING may go out. The pre-T17 code would
    /// have sent all three in the clear.
    #[test]
    fn a_control_send_with_no_resolvable_peer_is_dropped_not_sent_in_the_clear() {
        use Ordering::Relaxed;
        // `registered: false` → the peer map is empty, so the leader address
        // resolves to nothing.
        let (mut r, peer, _b, _ids) = receiver_with_crypto_ids(false);
        let st = r.stats();
        // Force a gap so the NAK timer arms, and let the status/AP floors fire.
        r.leader_append = 4096;
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && st.seal_failures.load(Relaxed) == 0 {
            r.do_work();
            std::thread::yield_now();
        }
        assert!(
            st.seal_failures.load(Relaxed) > 0,
            "an unsealable control datagram must be counted, not silently sent"
        );
        assert_eq!(st.naks_sent.load(Relaxed), 0, "no NAK reached the wire");
        assert_eq!(
            st.statuses_sent.load(Relaxed),
            0,
            "no STATUS reached the wire"
        );
        assert_eq!(
            st.append_positions_sent.load(Relaxed),
            0,
            "no AppendPosition reached the wire"
        );
        let mut buf = [0u8; 4096];
        assert!(
            peer.sock.recv_from(&mut buf).unwrap().is_none(),
            "not one cleartext control datagram reached the leader"
        );
    }

    /// The deleted allowance, inverted. Before T17 this exact datagram was
    /// admitted unauthenticated and its `SnapBeginBody.config` flowed into
    /// `maybe_adopt_incoming_snapshot` — attacker-chosen membership.
    #[test]
    fn an_unsealed_snap_begin_is_refused_now_that_t17_landed() {
        use Ordering::Relaxed;
        let (mut r, mut peer, _b) = receiver_with_crypto();
        let dir = snap_scratch_dir();
        r.set_snapshot_intake(
            dir.path().to_path_buf(),
            ident(0b1),
            Arc::new(|| [0u32; 8]),
            None,
        );
        let to = r.local_addr();
        let st = r.stats();
        let before = st.datagrams.load(Relaxed);

        // A well-formed, entirely cleartext SNAP_BEGIN carrying a hostile
        // membership record — exactly the forgery T11's allowance admitted.
        let hostile = b"ATTACKER-CHOSEN-MEMBERSHIP".to_vec();
        let mut body = vec![0u8; SNAP_BEGIN_FIXED_LEN + hostile.len()];
        write_snap_begin_body(
            &mut body,
            &SnapBeginBody {
                session: 1,
                layout: SNAP_BEGIN_LAYOUT_V3,
                service_id: 0,
                snapshot_pos: 4096,
                total_len: 32,
                identity: ident(0b1),
                version: [0; 8],
                config: hostile.clone(),
            },
        );
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(&mut d, &CryptoPeer::header(0, DGRAM_KIND_SNAP_BEGIN, 0));
        d.extend_from_slice(&body);
        // Long enough that the mixed-mode "peer appears cleartext" shortcut
        // (which needs `n < DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD`) does NOT
        // fire — so this datagram genuinely reaches, and is rejected by, the
        // AEAD check rather than being screened out for its length.
        assert!(d.len() > DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD);
        peer.send_raw(to, &d);

        let deadline = Instant::now() + Duration::from_secs(3);
        while st.dropped_auth_failed.load(Relaxed) == 0 {
            assert!(
                Instant::now() < deadline,
                "an unsealed SNAP_BEGIN was neither authenticated nor rejected — the T17 \
                 allowance is still admitting it"
            );
            r.do_work();
        }
        assert_eq!(
            st.datagrams.load(Relaxed),
            before,
            "the forged SNAP_BEGIN must never reach on_datagram at all"
        );
        assert!(
            r.snap_intake.is_none(),
            "no snapshot intake may be opened by an unauthenticated SNAP_BEGIN"
        );
        assert_eq!(
            st.peer_appears_cleartext.load(Relaxed),
            0,
            "not the mixed-mode diagnostic"
        );
    }

    /// Same rule for `SNAP_CHUNK`: the artifact's bytes may not be written
    /// into an intake by anyone who can reach the UDP port.
    #[test]
    fn an_unsealed_snap_chunk_is_refused_now_that_t17_landed() {
        use Ordering::Relaxed;
        let (mut r, mut peer, _b) = receiver_with_crypto();
        let dir = snap_scratch_dir();
        r.set_snapshot_intake(
            dir.path().to_path_buf(),
            ident(0b1),
            Arc::new(|| [0u32; 8]),
            None,
        );
        let to = r.local_addr();
        let st = r.stats();

        // Open a REAL (sealed) session first, so an intake exists and the
        // only thing wrong with the chunk below is that it is unsealed.
        let mut begin = vec![0u8; SNAP_BEGIN_FIXED_LEN];
        write_snap_begin_body(
            &mut begin,
            &SnapBeginBody {
                session: 3,
                layout: SNAP_BEGIN_LAYOUT_V3,
                service_id: 0,
                snapshot_pos: 4096,
                total_len: 64,
                identity: ident(0b1),
                version: [0; 8],
                config: vec![],
            },
        );
        peer.send_sealed_pairwise(to, DGRAM_KIND_SNAP_BEGIN, 0, &begin);
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.snap_intake.is_none() {
            assert!(
                Instant::now() < deadline,
                "the sealed SNAP_BEGIN never opened an intake"
            );
            r.do_work();
        }

        let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(&mut d, &CryptoPeer::header(0, DGRAM_KIND_SNAP_CHUNK, 0));
        d.extend_from_slice(&[0xAAu8; 64]);
        let before_auth = st.dropped_auth_failed.load(Relaxed);
        peer.send_raw(to, &d);

        let deadline = Instant::now() + Duration::from_secs(3);
        while st.dropped_auth_failed.load(Relaxed) == before_auth {
            assert!(
                Instant::now() < deadline,
                "an unsealed SNAP_CHUNK was admitted"
            );
            r.do_work();
        }
        let intake = r
            .snap_intake
            .as_ref()
            .expect("the real session is still open");
        assert_eq!(
            intake.got.contiguous(),
            0,
            "no forged bytes landed in the .part"
        );
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

    // ---- M14c: the per-id intake's placement guard, probe and DONE latch ----

    /// A nonzero placeholder identity hash per set bit of `mask` — enough for
    /// `crate::sender::identity_mask(&ident(mask)) == mask` to hold, which is
    /// all these tests need (they exercise artifact/mask covering and
    /// placement, not real names).
    fn ident(mask: u64) -> [u64; 8] {
        let mut out = [0u64; 8];
        for (i, h) in out.iter_mut().enumerate() {
            if mask & (1 << i) != 0 {
                *h = 0xF00D_0000_0000_0000 | (i as u64 + 1);
            }
        }
        out
    }

    /// A `SNAP_BEGIN` body as the leader ships it (fixed part only, no config).
    fn snap_begin_wire(session: u32, id: u8, pos: u64, len: u64, declared: u64) -> Vec<u8> {
        let mut body = vec![0u8; SNAP_BEGIN_FIXED_LEN];
        write_snap_begin_body(
            &mut body,
            &SnapBeginBody {
                session,
                layout: SNAP_BEGIN_LAYOUT_V3,
                service_id: id,
                snapshot_pos: pos,
                total_len: len,
                identity: ident(declared),
                version: [0; 8],
                config: vec![],
            },
        );
        body
    }

    /// The ordering rule the placement guard is built on: artifacts are
    /// announced ascending over the DECLARED mask, so with `0b101` declared the
    /// artifact after id 0 is id **2**, and nothing follows it. (A sender that
    /// skips one, or a lost BEGIN, is what the guard in `snap_begin` catches.)
    #[test]
    fn next_declared_id_walks_a_sparse_declared_mask() {
        assert_eq!(next_declared_id(0b101, None), Some(0));
        assert_eq!(
            next_declared_id(0b101, Some(0)),
            Some(2),
            "id 1 is NOT declared"
        );
        assert_eq!(next_declared_id(0b101, Some(2)), None);
        assert_eq!(
            next_declared_id(0b110, None),
            Some(1),
            "a mask that does not start at 0"
        );
        assert_eq!(next_declared_id(1 << 63, None), Some(63));
        assert_eq!(
            next_declared_id(1 << 63, Some(63)),
            None,
            "no shift past bit 63"
        );
        assert_eq!(next_declared_id(0, None), None);
    }

    /// M14c: an artifact's STREAM base is the SUM of its predecessors' lengths,
    /// and the sender rotates to artifact k+1 once k's last chunk has been
    /// SENT — so a BEGIN that skips a declared id means an earlier BEGIN was
    /// lost. Placing it at `announced_len` anyway would hand two FSMs each
    /// other's bytes. It is dropped, and a 1-byte `SNAP_NAK` at `announced_len`
    /// prompts the leader for the artifact we are missing (its own 20 ms resend
    /// cadence only ever covers the artifact it is currently shipping).
    #[test]
    fn a_begin_that_skips_a_declared_id_is_not_placed_and_prompts_for_the_missing_one() {
        let (mut r, mut peer, _b) = receiver_with_crypto();
        let dir = snap_scratch_dir();
        r.set_snapshot_intake(
            dir.path().to_path_buf(),
            ident(0b111),
            Arc::new(|| [0u32; 8]),
            None,
        );
        // Pin the intake's own NAK timer far into the future: every SNAP_NAK
        // this test sees is then unambiguously the missing-BEGIN probe, never
        // an ordinary gap NAK (artifact 0's 64 bytes never arrive here).
        r.snap_nak_cfg = NakConfig {
            delay_min_ns: 1 << 40,
            delay_max_ns: 1 << 40,
            backoff_ns: 1 << 40,
        };
        let to = r.local_addr();

        peer.send_sealed_pairwise(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            &snap_begin_wire(11, 0, 4096, 64, 0b111),
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.snap_intake.is_none() {
            assert!(
                Instant::now() < deadline,
                "the first BEGIN never opened an intake"
            );
            r.do_work();
        }

        // id 1 is still un-announced, so id 2 cannot be placed.
        peer.send_sealed_pairwise(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            &snap_begin_wire(11, 2, 8192, 64, 0b111),
        );
        let (_, body) = peer.await_sealed(&mut r, DGRAM_KIND_SNAP_NAK);
        let nak = read_snap_nak_body(&body).expect("a well-formed SNAP_NAK body");
        assert_eq!(
            (nak.session, nak.offset, nak.length),
            (11, 64, 1),
            "a one-byte probe at announced_len — 'send me the BEGIN that starts here'"
        );
        assert!(
            !dir.path().join("2").exists(),
            "the skipped-ahead artifact is not placed"
        );
        assert_eq!(
            r.snap_intake.as_ref().unwrap().parts.len(),
            1,
            "still exactly one announced artifact"
        );

        // The prompted BEGIN lands: id 1 is placed at base 64...
        peer.send_sealed_pairwise(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            &snap_begin_wire(11, 1, 5120, 64, 0b111),
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.snap_intake.as_ref().unwrap().parts.len() < 2 {
            assert!(
                Instant::now() < deadline,
                "the prompted BEGIN was never placed"
            );
            r.do_work();
        }
        assert!(dir.path().join("1").join("incoming-5120.part").exists());

        // ...and the re-sent BEGIN(2) now places, at base 128.
        peer.send_sealed_pairwise(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            &snap_begin_wire(11, 2, 8192, 64, 0b111),
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.snap_intake.as_ref().unwrap().parts.len() < 3 {
            assert!(
                Instant::now() < deadline,
                "the re-sent BEGIN(2) was never placed"
            );
            r.do_work();
        }
        let intake = r.snap_intake.as_ref().unwrap();
        assert_eq!(
            intake
                .parts
                .iter()
                .map(|p| (p.service_id, p.base))
                .collect::<Vec<_>>(),
            vec![(0, 0), (1, 64), (2, 128)],
            "each artifact sits at the SUM of its predecessors' lengths"
        );
        assert_eq!(intake.announced_len, 192);
        assert!(dir.path().join("2").join("incoming-8192.part").exists());
    }

    /// M14c: the leader re-sends a `SNAP_BEGIN` every 20 ms until our
    /// `SNAP_DONE` reaches it. A lost DONE must therefore NOT re-open (and
    /// re-download) a set we already renamed and installed — it must be
    /// re-acked, which is exactly what the leader is waiting for.
    #[test]
    fn a_begin_re_sent_after_the_session_completed_is_re_acked_not_re_downloaded() {
        let (mut r, mut peer, _b) = receiver_with_crypto();
        let dir = snap_scratch_dir();
        r.set_snapshot_intake(
            dir.path().to_path_buf(),
            ident(0b1),
            Arc::new(|| [0u32; 8]),
            None,
        );
        let to = r.local_addr();
        let begin = snap_begin_wire(21, 0, 4096, 64, 0b1);

        peer.send_sealed_pairwise(to, DGRAM_KIND_SNAP_BEGIN, 0, &begin);
        peer.send_sealed_pairwise(to, DGRAM_KIND_SNAP_CHUNK, 0, &[0xABu8; 64]);
        let (_, first) = peer.await_sealed(&mut r, DGRAM_KIND_SNAP_DONE);
        assert_eq!(read_snap_begin_body(&first).unwrap().session, 21);
        let part = dir.path().join("0").join("incoming-4096.part");
        assert!(
            dir.path().join("0").join("snap-4096.ultsnap").exists(),
            "renamed"
        );
        assert!(!part.exists(), "the .part is gone once renamed");

        // The DONE was lost on the wire: the leader re-sends the BEGIN.
        peer.send_sealed_pairwise(to, DGRAM_KIND_SNAP_BEGIN, 0, &begin);
        let (_, second) = peer.await_sealed(&mut r, DGRAM_KIND_SNAP_DONE);
        assert_eq!(
            read_snap_begin_body(&second).unwrap().session,
            21,
            "a SECOND ack for the same session, not a fresh download"
        );
        assert!(!part.exists(), "no .part reappears");
        assert!(r.snap_intake.is_none(), "no intake was re-opened");
    }

    /// M14c: a named refusal drops the session it refuses — and ONLY that one.
    /// A forged/legacy `SNAP_BEGIN` from any other address must not tear down
    /// an unrelated live transfer; and a session that IS dropped must take its
    /// pre-sized `.part` with it rather than leaking a full-size file.
    #[test]
    fn a_refusal_from_another_peer_spares_the_live_intake_and_a_real_one_cleans_up() {
        use Ordering::Relaxed;
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut stranger = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let dir = snap_scratch_dir();
        r.set_snapshot_intake(
            dir.path().to_path_buf(),
            ident(0b1),
            Arc::new(|| [0u32; 8]),
            None,
        );
        let st = r.stats();
        let to = r.local_addr();

        leader.send(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            TERM,
            &snap_begin_wire(4, 0, 4096, 64, 0b1),
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.snap_intake.is_none() {
            assert!(
                Instant::now() < deadline,
                "the BEGIN never opened an intake"
            );
            r.do_work();
        }
        let part = dir.path().join("0").join("incoming-4096.part");
        assert!(part.exists());

        // A legacy-layout BEGIN from an unrelated address.
        let mut legacy = vec![0u8; SNAP_BEGIN_FIXED_LEN];
        write_snap_begin_body(
            &mut legacy,
            &SnapBeginBody {
                session: 4,
                layout: 0, // a pre-0.7.0-shaped body
                service_id: 0,
                snapshot_pos: 4096,
                total_len: 64,
                identity: ident(0b1),
                version: [0; 8],
                config: vec![],
            },
        );
        stranger.send(to, DGRAM_KIND_SNAP_BEGIN, 0, TERM, &legacy);
        let deadline = Instant::now() + Duration::from_secs(3);
        while st.snap_refused_legacy_peer.load(Relaxed) == 0 {
            assert!(
                Instant::now() < deadline,
                "the stranger's refusal was never counted"
            );
            r.do_work();
        }
        assert!(
            r.snap_intake.is_some(),
            "an unrelated peer may not kill a live transfer"
        );
        assert!(part.exists(), "nor delete its .part");

        // From the peer we are actually transferring with, it does drop the
        // session — and the abandoned .part goes with it.
        leader.send(to, DGRAM_KIND_SNAP_BEGIN, 0, TERM, &legacy);
        let deadline = Instant::now() + Duration::from_secs(3);
        while st.snap_refused_legacy_peer.load(Relaxed) < 2 {
            assert!(
                Instant::now() < deadline,
                "the leader's refusal was never counted"
            );
            r.do_work();
        }
        assert!(
            r.snap_intake.is_none(),
            "our own leader's legacy BEGIN drops the session"
        );
        assert!(
            !part.exists(),
            "an abandoned .part must not leak a full-size file"
        );
    }

    /// M14c: a NEW session from the same peer replaces a stale intake — and
    /// must not orphan the stale one's `.part`.
    #[test]
    fn a_replacing_session_removes_the_stale_intakes_part() {
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let dir = snap_scratch_dir();
        r.set_snapshot_intake(
            dir.path().to_path_buf(),
            ident(0b1),
            Arc::new(|| [0u32; 8]),
            None,
        );
        let to = r.local_addr();

        leader.send(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            TERM,
            &snap_begin_wire(1, 0, 4096, 64, 0b1),
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.snap_intake.is_none() {
            assert!(
                Instant::now() < deadline,
                "the first BEGIN never opened an intake"
            );
            r.do_work();
        }
        let stale = dir.path().join("0").join("incoming-4096.part");
        assert!(stale.exists());

        // A second session (the first one timed out at the leader), newer floor.
        leader.send(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            TERM,
            &snap_begin_wire(2, 0, 8192, 64, 0b1),
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.snap_intake.as_ref().is_none_or(|i| i.session != 2) {
            assert!(
                Instant::now() < deadline,
                "the replacing BEGIN never took over"
            );
            r.do_work();
        }
        assert!(
            dir.path().join("0").join("incoming-8192.part").exists(),
            "the new .part"
        );
        assert!(!stale.exists(), "the superseded .part must not be orphaned");
    }

    // ---- M14c2 (T10a): the intake's timeout, latches and re-drive cadence ----

    /// Open a live intake for one 64-byte artifact of FSM 0 and hand back the
    /// receiver, its fake leader, the scratch dir and the `.part`'s path. The
    /// shared prologue of the tests below.
    fn intake_open(
        b: &Arc<LogBuffer>,
        session: u32,
    ) -> (FollowerReceiver, FakeLeader, tempfile::TempDir, PathBuf) {
        let mut leader = FakeLeader::new();
        let mut r = follower(b, leader.addr());
        let dir = snap_scratch_dir();
        r.set_snapshot_intake(
            dir.path().to_path_buf(),
            ident(0b1),
            Arc::new(|| [0u32; 8]),
            None,
        );
        let to = r.local_addr();
        leader.send(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            TERM,
            &snap_begin_wire(session, 0, 4096, 64, 0b1),
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.snap_intake.is_none() {
            assert!(
                Instant::now() < deadline,
                "the BEGIN never opened an intake"
            );
            r.do_work();
        }
        let part = dir.path().join("0").join("incoming-4096.part");
        assert!(part.exists(), "the pre-sized .part is on disk");
        (r, leader, dir, part)
    }

    /// M14c2 (T10a): an intake that has seen no chunk for
    /// [`SNAP_INTAKE_TIMEOUT_NS`] is abandoned — its pre-sized `.part`
    /// unlinked, the abandonment counted once. Before this a leader that died
    /// mid-transfer (or a session refused at the far end) left a FULL-SIZE
    /// `.part` under `snapshots/<id>/` until some later session happened to
    /// replace it; on a joiner that never gets another one, forever.
    #[test]
    fn an_intake_with_no_chunk_for_the_timeout_is_abandoned_unlinked_and_counted() {
        let b = buffer();
        let (mut r, mut leader, dir, part) = intake_open(&b, 5);
        let st = r.stats();
        let to = r.local_addr();
        let opened = r.snap_intake.as_ref().unwrap().last_chunk_ns;

        // One nanosecond short of the bar: still live.
        r.snap_upkeep(opened + SNAP_INTAKE_TIMEOUT_NS - 1);
        assert!(r.snap_intake.is_some(), "not abandoned before the timeout");
        assert_eq!(st.snap_intake_abandoned.load(Ordering::Relaxed), 0);
        assert!(part.exists());

        // At the bar: dropped, unlinked, counted ONCE — `upkeep` calls
        // `snap_upkeep` twice per duty cycle, and the second call must find
        // nothing left to abandon.
        r.snap_upkeep(opened + SNAP_INTAKE_TIMEOUT_NS);
        r.snap_upkeep(opened + SNAP_INTAKE_TIMEOUT_NS);
        assert!(r.snap_intake.is_none(), "the dead intake is dropped");
        assert!(!part.exists(), "its pre-sized .part must not leak");
        assert_eq!(
            st.snap_intake_abandoned.load(Ordering::Relaxed),
            1,
            "counted exactly once"
        );

        // A later session opens a fresh intake, exactly as if none had existed.
        leader.send(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            TERM,
            &snap_begin_wire(6, 0, 8192, 64, 0b1),
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.snap_intake.is_none() {
            assert!(
                Instant::now() < deadline,
                "a later BEGIN must open a fresh intake"
            );
            r.do_work();
        }
        assert_eq!(r.snap_intake.as_ref().unwrap().session, 6);
        assert!(dir.path().join("0").join("incoming-8192.part").exists());
    }

    /// M14c2 (T10a): a chunk RESETS the timeout — a slow-but-live transfer must
    /// never be abandoned. (The control for the test above: without this the
    /// bar would be "60 s since the session opened", which a big artifact on a
    /// lossy link legitimately exceeds.)
    #[test]
    fn a_chunk_resets_the_intake_timeout() {
        let b = buffer();
        let (mut r, mut leader, _dir, _part) = intake_open(&b, 8);
        let to = r.local_addr();
        let opened = r.snap_intake.as_ref().unwrap().last_chunk_ns;

        leader.send(to, DGRAM_KIND_SNAP_CHUNK, 0, TERM, &[0xABu8; 32]);
        let deadline = Instant::now() + Duration::from_secs(3);
        while r
            .snap_intake
            .as_ref()
            .is_none_or(|i| i.got.contiguous() == 0)
        {
            assert!(Instant::now() < deadline, "the chunk never landed");
            r.do_work();
        }
        assert!(
            r.snap_intake.as_ref().unwrap().last_chunk_ns >= opened,
            "the chunk refreshed the deadline"
        );
        // Past the bar measured from the OPEN, but not from the chunk.
        let last = r.snap_intake.as_ref().unwrap().last_chunk_ns;
        r.snap_upkeep(last + SNAP_INTAKE_TIMEOUT_NS - 1);
        assert!(r.snap_intake.is_some(), "a live transfer is not abandoned");
    }

    /// M14c2 (T10a): a `SNAP_BEGIN` body we cannot even DECODE is the shape a
    /// real 0.5.0 flag day produces, and the leader re-sends its BEGIN every
    /// 20 ms — so `snap_refused_legacy_peer` climbs at the resend rate and says
    /// nothing about how many SESSIONS were refused. `snap_begin_undecodable`
    /// is latched: one per session, cleared the moment a decodable BEGIN
    /// arrives.
    #[test]
    fn an_undecodable_begin_is_counted_once_per_session() {
        use Ordering::Relaxed;
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let dir = snap_scratch_dir();
        r.set_snapshot_intake(
            dir.path().to_path_buf(),
            ident(0b1),
            Arc::new(|| [0u32; 8]),
            None,
        );
        let st = r.stats();
        let to = r.local_addr();

        // A genuine pre-0.7.0 fixed body (0.5.0's 26 bytes, 0.6.0's 34) is
        // too short to decode as 0.7.0 at all. The leader re-sends it; three
        // arrive.
        let short = vec![0u8; 26];
        for _ in 0..3 {
            leader.send(to, DGRAM_KIND_SNAP_BEGIN, 0, TERM, &short);
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while st.snap_refused_legacy_peer.load(Relaxed) < 3 {
            assert!(
                Instant::now() < deadline,
                "the three legacy bodies were never counted"
            );
            r.do_work();
        }
        assert_eq!(
            st.snap_begin_undecodable.load(Relaxed),
            1,
            "one per SESSION, however often the leader re-sends it"
        );

        // Advisory 6: a decodable body that is REFUSED must NOT re-arm the
        // latch — nothing about a refusal says the peer speaks a wire we can
        // install a set over. `total_len == 0` is the cheapest decodable
        // refusal, and it returns before the latch clear.
        leader.send(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            TERM,
            &snap_begin_wire(9, 0, 4096, 0, 0b1),
        );
        leader.send(to, DGRAM_KIND_SNAP_BEGIN, 0, TERM, &short);
        let deadline = Instant::now() + Duration::from_secs(3);
        while st.snap_refused_legacy_peer.load(Relaxed) < 4 {
            assert!(
                Instant::now() < deadline,
                "the post-refusal legacy body was never counted"
            );
            r.do_work();
        }
        assert!(
            r.snap_intake.is_none(),
            "a total_len == 0 BEGIN opens no intake"
        );
        assert_eq!(
            st.snap_begin_undecodable.load(Relaxed),
            1,
            "a decodable-but-REFUSED BEGIN must not re-arm the latch"
        );

        // A decodable, ACCEPTED BEGIN clears it: the wire is speakable again.
        leader.send(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            TERM,
            &snap_begin_wire(9, 0, 4096, 64, 0b1),
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.snap_intake.is_none() {
            assert!(
                Instant::now() < deadline,
                "the decodable BEGIN never opened an intake"
            );
            r.do_work();
        }
        assert_eq!(
            st.snap_begin_undecodable.load(Relaxed),
            1,
            "a decodable BEGIN counts nothing"
        );

        for _ in 0..2 {
            leader.send(to, DGRAM_KIND_SNAP_BEGIN, 0, TERM, &short);
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while st.snap_refused_legacy_peer.load(Relaxed) < 6 {
            assert!(
                Instant::now() < deadline,
                "the second legacy burst was never counted"
            );
            r.do_work();
        }
        assert_eq!(
            st.snap_begin_undecodable.load(Relaxed),
            2,
            "the cleared latch re-arms: a SECOND refused session is a second count"
        );
    }

    /// M14c ruling M(2) / M14c2 (T10a): a `seek`/`write_all` failure landing a
    /// chunk in the `.part` was SILENT — and it is the likeliest ENOSPC site on
    /// a joiner, because the `.part` is pre-sized sparse and its blocks are
    /// only allocated as chunks arrive. The stall was named only by the
    /// sender's 30 s session timeout, never by
    /// `uc2_snapshot_intake_io_failures_total`.
    ///
    /// Injection: the intake's write handle is swapped for a READ-ONLY handle
    /// on the very same `.part`. `write(2)` then really fails (EBADF) inside
    /// the real `snap_chunk` — a genuine I/O error through the shipped code
    /// path, not a stub.
    #[test]
    fn a_chunk_write_failure_is_counted_and_never_panics() {
        use Ordering::Relaxed;
        let b = buffer();
        let (mut r, mut leader, _dir, part) = intake_open(&b, 12);
        let st = r.stats();
        let to = r.local_addr();
        r.snap_intake.as_mut().unwrap().parts[0].file =
            Some(std::fs::File::open(&part).expect("a read-only handle on the same .part"));

        leader.send(to, DGRAM_KIND_SNAP_CHUNK, 0, TERM, &[0xABu8; 32]);
        let deadline = Instant::now() + Duration::from_secs(3);
        while st.snap_intake_io_failures.load(Relaxed) < 1 {
            assert!(
                Instant::now() < deadline,
                "the failed chunk write was never counted"
            );
            r.do_work();
        }
        assert_eq!(
            r.snap_intake.as_ref().unwrap().got.contiguous(),
            0,
            "bytes that were never written must not be recorded as landed"
        );

        leader.send(to, DGRAM_KIND_SNAP_CHUNK, 32, TERM, &[0xCDu8; 32]);
        let deadline = Instant::now() + Duration::from_secs(3);
        while st.snap_intake_io_failures.load(Relaxed) < 2 {
            assert!(
                Instant::now() < deadline,
                "the second failure was never counted"
            );
            r.do_work();
        }
        assert!(
            r.snap_intake.is_some(),
            "a write failure is not fatal to the intake"
        );
    }

    /// M14c ruling M(1) / M14c2 (T10a): `upkeep` calls `snap_upkeep` TWICE per
    /// poll iteration, so a PERSISTENT rename obstacle burned one to two
    /// failing syscalls per spin and `snap_intake_io_failures` climbed at the
    /// poll rate. The docs tell an operator to read a RISING count as the
    /// signal, which a poll-rate climb makes unreadable.
    /// [`SNAP_REDRIVE_INTERVAL_NS`] paces the re-drive to one attempt per
    /// interval per intake.
    #[test]
    fn the_publish_re_drive_is_paced_to_one_attempt_per_interval() {
        use Ordering::Relaxed;
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let dir = snap_scratch_dir();
        r.set_snapshot_intake(
            dir.path().to_path_buf(),
            ident(0b1),
            Arc::new(|| [0u32; 8]),
            None,
        );
        let st = r.stats();
        let to = r.local_addr();
        // A DIRECTORY on the artifact's final path: the rename of the completed
        // `.part` fails (EISDIR) on every attempt, deterministically, on every
        // filesystem — the stand-in for a read-only or obstructed snapshot dir.
        std::fs::create_dir_all(dir.path().join("0")).unwrap();
        std::fs::create_dir(dir.path().join("0").join("snap-4096.ultsnap")).unwrap();

        leader.send(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            TERM,
            &snap_begin_wire(14, 0, 4096, 64, 0b1),
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.snap_intake.is_none() {
            assert!(
                Instant::now() < deadline,
                "the BEGIN never opened an intake"
            );
            r.do_work();
        }
        leader.send(to, DGRAM_KIND_SNAP_CHUNK, 0, TERM, &[0xABu8; 64]);
        let deadline = Instant::now() + Duration::from_secs(3);
        while st.snap_intake_io_failures.load(Relaxed) < 1 {
            assert!(
                Instant::now() < deadline,
                "the blocked publish was never counted"
            );
            r.do_work();
        }

        // A virtual clock well past every real reading taken so far, so the
        // cadence this test measures is entirely its own.
        let t0 = r.now_ns() + 10_000_000_000;
        let base = st.snap_intake_io_failures.load(Relaxed);
        r.snap_upkeep(t0);
        r.snap_upkeep(t0 + 1_000_000); // 1 ms later
        assert_eq!(
            st.snap_intake_io_failures.load(Relaxed) - base,
            1,
            "two calls 1 ms apart attempt the publish ONCE"
        );
        r.snap_upkeep(t0 + 1_000_000 + SNAP_REDRIVE_INTERVAL_NS);
        assert_eq!(
            st.snap_intake_io_failures.load(Relaxed) - base,
            2,
            "past the interval, the re-drive attempts again"
        );
        assert!(
            r.snap_intake.is_some(),
            "the intake is still waiting on the obstacle"
        );
    }

    /// M14c2 (T10a) fix round 1, REQUIRED 1: the cadence must hold on the
    /// CHUNK path too. `snap_chunk` called `snap_publish_complete_parts`
    /// unconditionally on every accepted chunk, and that walk returns at the
    /// FIRST blocked part — so with artifact 0 stuck on a rename while
    /// artifacts 1..N are still streaming, every single chunk cost one failing
    /// `rename(2)` and one `snap_intake_io_failures` bump. The counter then
    /// climbed at CHUNK rate, which is exactly the reading the operator docs
    /// ("a rising count is the signal") cannot survive.
    ///
    /// A first attempt, and every attempt after a clean pass, stays immediate —
    /// only a FAILED attempt arms the interval, so the normal completion path
    /// is never delayed.
    #[test]
    fn a_blocked_publish_does_not_re_fail_on_every_incoming_chunk() {
        use Ordering::Relaxed;
        let b = buffer();
        let mut leader = FakeLeader::new();
        let mut r = follower(&b, leader.addr());
        let dir = snap_scratch_dir();
        r.set_snapshot_intake(
            dir.path().to_path_buf(),
            ident(0b11),
            Arc::new(|| [0u32; 8]),
            None,
        );
        let st = r.stats();
        let to = r.local_addr();
        // A DIRECTORY on artifact 0's final path: its rename fails on every
        // attempt, while artifact 1 keeps streaming behind it.
        std::fs::create_dir_all(dir.path().join("0")).unwrap();
        std::fs::create_dir(dir.path().join("0").join("snap-4096.ultsnap")).unwrap();

        // Artifact 0: 64 B at stream base 0. Artifact 1: 640 B at base 64.
        leader.send(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            TERM,
            &snap_begin_wire(21, 0, 4096, 64, 0b11),
        );
        leader.send(
            to,
            DGRAM_KIND_SNAP_BEGIN,
            0,
            TERM,
            &snap_begin_wire(21, 1, 5120, 640, 0b11),
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.snap_intake.as_ref().is_none_or(|i| i.parts.len() < 2) {
            assert!(Instant::now() < deadline, "both BEGINs were never placed");
            r.do_work();
        }

        // Artifact 0 completes: ONE publish attempt, which fails. This is the
        // first attempt, so it is immediate.
        leader.send(to, DGRAM_KIND_SNAP_CHUNK, 0, TERM, &[0xA0u8; 64]);
        let deadline = Instant::now() + Duration::from_secs(3);
        while st.snap_intake_io_failures.load(Relaxed) < 1 {
            assert!(
                Instant::now() < deadline,
                "the blocked rename was never counted"
            );
            r.do_work();
        }

        // Nine more chunks stream artifact 1, well inside one interval. Each
        // one used to re-drive the blocked part and bump the counter.
        //
        // The "inside one interval" premise is wall-clock: on a loaded runner
        // the burst below can itself outlast SNAP_REDRIVE_INTERVAL_NS (250 ms),
        // at which point a SECOND failing attempt is correct behaviour, not a
        // regression. So the exact-count assertion is gated on having actually
        // stayed inside the interval, with margin.
        let burst_start = Instant::now();
        for k in 0..9u64 {
            leader.send(to, DGRAM_KIND_SNAP_CHUNK, 64 + k * 64, TERM, &[0xB1u8; 64]);
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while r
            .snap_intake
            .as_ref()
            .is_none_or(|i| i.got.contiguous() < 640)
        {
            assert!(
                Instant::now() < deadline,
                "artifact 1's chunks never landed"
            );
            r.do_work();
        }
        let burst_elapsed = burst_start.elapsed();
        if burst_elapsed < Duration::from_millis(200) {
            assert_eq!(
                st.snap_intake_io_failures.load(Relaxed),
                1,
                "nine accepted chunks inside one interval must cost ZERO extra failing renames"
            );
        } else {
            eprintln!(
                "a_blocked_publish_does_not_re_fail_on_every_incoming_chunk: burst took {burst_elapsed:?} \
                 (>= 200 ms of the {} ms interval) on a loaded runner — skipping the exact in-interval count; \
                 the one-per-interval check below still runs",
                SNAP_REDRIVE_INTERVAL_NS / 1_000_000
            );
        }

        // Past the interval, the next chunk does re-drive it — EXACTLY once,
        // measured as a delta off whatever the count is now (so a skipped
        // assertion above never turns this one into a bare inequality).
        let before_redrive = st.snap_intake_io_failures.load(Relaxed);
        std::thread::sleep(Duration::from_millis(
            SNAP_REDRIVE_INTERVAL_NS / 1_000_000 + 10,
        ));
        leader.send(to, DGRAM_KIND_SNAP_CHUNK, 640, TERM, &[0xB1u8; 64]);
        let deadline = Instant::now() + Duration::from_secs(3);
        while st.snap_intake_io_failures.load(Relaxed) < before_redrive + 1 {
            assert!(
                Instant::now() < deadline,
                "the re-drive never fired past the interval"
            );
            r.do_work();
        }
        assert_eq!(
            st.snap_intake_io_failures.load(Relaxed),
            before_redrive + 1,
            "one attempt per interval, not one per chunk"
        );

        // Clearing the obstacle publishes both artifacts — the pacing delays a
        // blocked retry, it never strands a completed set.
        std::fs::remove_dir(dir.path().join("0").join("snap-4096.ultsnap")).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !dir.path().join("1").join("snap-5120.ultsnap").is_file() {
            assert!(
                Instant::now() < deadline,
                "the set never published once cleared"
            );
            r.do_work();
        }
        assert!(dir.path().join("0").join("snap-4096.ultsnap").is_file());
    }
}
