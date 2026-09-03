// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The sender agent (spec §3.1/§5): scans the log buffer from the `sent`
//! counter, packs complete frames MTU-full, and sends the identical datagram
//! to every follower (MDC-style: one scan, N sends). Serves NAKs by
//! re-reading the buffer (the buffer IS the retransmit buffer). Paced by the
//! quorum-th order statistic over follower status adverts. Batching is
//! structural — whatever whole frames accumulated, no linger. Frames are
//! COPIED out via a validated read before the syscall: with no CRC on the
//! wire, sending live ring memory could transmit silently corrupt bytes.

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Instant;

use uc_crypto::{NodeId, Scope, SendHalf, Transport};
use uc_journal::Journal;
use uc_log::archive::find_block;
use uc_log::buffer::{LogBuffer, SliceRead};
use uc_log::cnc::CncPage;
use uc_protocol::v2::crypto::CRYPTO_OVERHEAD;
#[cfg(test)]
use uc_protocol::v2::datagram::read_snap_begin_body;
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT, DGRAM_KIND_SNAP_BEGIN,
    DGRAM_KIND_SNAP_CHUNK, DatagramHeader, MTU_DEFAULT, SNAP_BEGIN_FIXED_LEN, SNAP_BEGIN_LAYOUT_V3,
    SnapBeginBody, write_datagram_header, write_snap_begin_body,
};
use uc_protocol::v2::frame::{
    FRAME_ALIGNMENT, FRAME_TYPE_PADDING, HEADER_LEN, align_frame_len, read_header,
};

use crate::TermHandle;
use crate::fault::FaultSocket;
use crate::flow::FlowControl;
use crate::receiver::PeerIds;

/// Datagrams a single served NAK may replay from the journal before yielding
/// (spec §5 "bounded, separately paced"). The follower's NAK backoff
/// re-requests whatever is still missing — that re-NAK IS the pacing, so one
/// serve stays a bounded duty cycle even when the gap spans a whole block.
const REPLAY_DGRAMS_PER_NAK: usize = 8;

/// Bound on queued NAK requests (M2 final review: a flooding/hostile
/// follower must not grow the deque unboundedly). Oldest entries drop first —
/// a re-NAK after backoff re-requests anything still missing, so dropping is
/// always recoverable. 1024 entries ≈ 24 KB; the worst storm observed in the
/// M2 gate was ~10k NAKs over a whole run.
const NAK_QUEUE_MAX: usize = 1024;

/// Snapshot-session chunk datagrams a single duty cycle may emit (M6 Task 6,
/// spec §5 "separately paced"). Strictly below live DATA + NAK-replay: the
/// session is driven LAST in `do_work`, after the fan-out and NAK budgets, and
/// capped here so a transfer can never starve the live stream — a session under
/// contention just takes more cycles (bounded catch-up is the gate's measure,
/// not a deadline).
const SNAP_DGRAMS_PER_CYCLE: usize = 4;

/// A snapshot session with no NAK and no DONE for this long is abandoned (the
/// peer died mid-transfer, or its DONE was lost). The peer re-NAKs below the
/// floor if it still needs the snapshot, reopening a fresh session.
const SNAP_SESSION_TIMEOUT_NS: u64 = 30_000_000_000;

/// A session re-sends the `SNAP_BEGIN` of the artifact it is currently working
/// on no more often than this. A lost BEGIN would otherwise strand every chunk
/// of that artifact (the receiver cannot place bytes for an artifact it was
/// never told about, and cannot NAK for one either) until the 30 s session
/// timeout. A duplicate BEGIN is a no-op at the receiver, so this costs at most
/// one datagram per 20 ms per session.
const SNAP_BEGIN_RESEND_NS: u64 = 20_000_000;

/// Control messages routed from the leader's receiver agent (Task 8).
/// Bounded channel; a dropped message is safe (NAK re-fires after backoff,
/// status re-sends on its floor).
///
/// `AppendPosition` (a follower's durable report, spec §6) is NOT a member
/// here: the consensus agent is the sole commit ranker, so AppendPosition is
/// routed RAW to it as [`crate::receiver::NetEvent::Report`] and never reaches
/// the sender's control channel (M4 carry #5 — the sender no longer ranks
/// commit at all).
#[derive(Debug, Clone)]
pub enum CtrlMsg {
    Nak {
        from: SocketAddr,
        position: u64,
        length: u32,
    },
    Status {
        from: SocketAddr,
        contiguous: u64,
        window: u32,
    },
    /// M6 Task 6: the snapshot-session peer requests a missing file range.
    SnapNak {
        from: SocketAddr,
        session: u32,
        offset: u64,
        length: u32,
    },
    /// M6 Task 6: the snapshot-session peer signals the file is complete.
    SnapDone { from: SocketAddr, session: u32 },
    /// M7 (spec 2026-07-13): the consensus agent adopted a new `ClusterConfig`
    /// (`Action::ConfigAdopted`) — rebuild the fan-out + flow control from it.
    /// `followers` and `learners` are DISJOINT sets (voters-minus-self,
    /// learners-minus-self — the same convention `NodeConfig`/`Consensus` use
    /// elsewhere), not the combined fan-out `Sender::with_learners` takes at
    /// construction; the handler recombines them for streaming. `cluster_size`
    /// is the VOTING cluster size (`ClusterConfig::voters.len()`).
    SetPeers {
        followers: Vec<SocketAddr>,
        learners: Vec<SocketAddr>,
        cluster_size: usize,
    },
}

/// M14c: one FSM's newest durable snapshot artifact, as offered to a session.
#[derive(Debug, Clone)]
pub struct SnapArtifact {
    pub service_id: u8,
    pub snapshot_pos: u64,
    pub path: PathBuf,
    pub len: u64,
}

/// M14c (spec §7.3/§14.3): everything one snapshot session ships — one
/// artifact per declared FSM, **ascending by `service_id`, non-empty, every
/// `len > 0`**, plus the declared bitmask and the config the session carries.
/// A source that cannot honour those invariants must return `None`: the
/// session is refused (the peer re-NAKs) rather than opened half-formed.
#[derive(Debug, Clone)]
pub struct SnapshotSet {
    /// The sender's declared FSM mask; DERIVED from `identity` via
    /// [`identity_mask`] — bit `r` set iff `identity[r] != 0`. Kept as an
    /// explicit field (rather than computed inline everywhere) because the
    /// invariant check below asserts the two agree.
    pub services_declared: u64,
    /// Row `r`'s FSM identity hash (0 = undeclared); rides every
    /// `SNAP_BEGIN` (wire 0.7.0) and is what the receiver compares against
    /// its own, positionally by name.
    pub identity: [u64; 8],
    /// Row `r`'s attached service's packed version (0 = unknown); rides
    /// every `SNAP_BEGIN` alongside `identity`.
    pub version: [u32; 8],
    /// M7: the encoded `ConfigRecord.config` at ship time — see below.
    pub config: Vec<u8>,
    pub artifacts: Vec<SnapArtifact>,
}

/// The declared-FSM bitmask implied by an `identity` array: bit `r` set iff
/// `identity[r] != 0`. Wire 0.7.0 (spec §5): `SnapshotSet::services_declared`
/// is derived from `identity` via this function, never set independently.
pub fn identity_mask(identity: &[u64; 8]) -> u64 {
    identity
        .iter()
        .enumerate()
        .fold(0u64, |m, (i, h)| if *h != 0 { m | (1 << i) } else { m })
}

/// M6 Task 6 / M14c: the newest durable snapshot SET the node is willing to
/// ship. The node wires this to each declared FSM's `SnapshotStore` filtered by
/// its PERSISTED floor marker (never a half-written file). `None` = nothing
/// shippable (the NAK stays an overrun). M7 Task 6: `config` is the
/// `v2::config::encode_config` bytes of the CURRENT `ConfigRecord.config` at
/// ship time — carried in every `SNAP_BEGIN` so a below-floor joiner adopts the
/// leader's membership alongside its lineage. Over-delivery (shipping to a peer
/// whose config is already current) is safe: the receiver adopts by fiat only
/// on a genuine install, and adoption is idempotent by version.
pub type SnapshotSource = Arc<dyn Fn() -> Option<SnapshotSet> + Send + Sync>;

/// One artifact inside an in-flight outbound session. `base` is its first
/// byte's STREAM-GLOBAL offset (the session is one concatenated byte stream
/// with artifact boundaries announced by the BEGINs), so `SNAP_NAK` repair is
/// byte-identical to 0.5.0.
struct SnapPart {
    service_id: u8,
    snapshot_pos: u64,
    base: u64,
    len: u64,
    file: std::fs::File,
    /// When this artifact's `SNAP_BEGIN` was last put on the wire; `None` =
    /// never. Re-sent on the [`SNAP_BEGIN_RESEND_NS`] cadence — see
    /// `drive_snap_session`.
    begun_ns: Option<u64>,
}

/// One in-flight outbound snapshot transfer (M6 Task 6; M14c: N artifacts). At
/// most one at a time — a second requester waits; sessions are rare by
/// construction (only a peer whose NAK fell below the purge floor triggers one).
struct SnapSession {
    peer: SocketAddr,
    session: u32,
    /// Row `r`'s FSM identity hash; rides every `SNAP_BEGIN` (wire 0.7.0).
    /// The declared-FSM mask a receiver checks against is derived from this
    /// via [`identity_mask`], never stored separately.
    identity: [u64; 8],
    /// Row `r`'s attached service's packed version; rides every `SNAP_BEGIN`.
    version: [u32; 8],
    /// Ascending by `service_id`, contiguous in `base`.
    parts: Vec<SnapPart>,
    /// Sum of the artifacts' lengths — the stream's byte space.
    stream_len: u64,
    /// Next sequential STREAM offset to ship (the contiguous fill cursor).
    cursor: u64,
    /// Peer-requested missing STREAM ranges (repair), served before the cursor.
    naks: VecDeque<(u64, u32)>,
    last_activity_ns: u64,
    /// M7 Task 6: the encoded `ConfigRecord.config` at the moment this session
    /// opened (from the `SnapshotSource` closure) — carried in every `SNAP_BEGIN`.
    config: Vec<u8>,
}

impl SnapSession {
    /// Index of the artifact containing stream offset `at`, or `None` past EOF.
    fn part_at(&self, at: u64) -> Option<usize> {
        self.parts
            .iter()
            .position(|p| at >= p.base && at < p.base + p.len)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SenderConfig {
    pub mtu: usize,
    pub term_id: u32,
    /// Heartbeat interval (also drives follower tail-loss NAKs). 100 ms
    /// default per spec §6's floor; tests shrink it.
    pub heartbeat_ns: u64,
    /// Follower limit assumed before its first status arrives.
    pub initial_window: u64,
    /// Max steady-state datagrams per duty cycle (bounded work).
    pub dgrams_per_cycle: usize,
    /// M8: whether this sender's DATA/HEARTBEAT datagrams are sealed under
    /// wire crypto. Kept as an explicit `SenderConfig` field — rather than
    /// derived from `Sender::crypto.is_some()` — so `crypto_overhead()` is
    /// computable on a bare `SenderConfig`, before any `Sender` exists (the
    /// MTU-budget math wants this independent of construction order).
    /// `Sender::with_crypto` asserts this always agrees with whether a
    /// `Transport` was actually supplied, so the two can never drift.
    pub crypto_enabled: bool,
}

impl SenderConfig {
    pub fn new(term_id: u32) -> Self {
        Self {
            mtu: MTU_DEFAULT,
            term_id,
            heartbeat_ns: 100_000_000,
            initial_window: 65_536,
            dgrams_per_cycle: 8,
            crypto_enabled: false,
        }
    }

    /// M8: bytes a sealed datagram adds beyond `DATAGRAM_HEADER_LEN` —
    /// `CRYPTO_OVERHEAD` (the 8-byte counter + 16-byte AEAD tag) when crypto
    /// is on, else 0 (cleartext: the wire is byte-for-byte pre-M8). Every
    /// site that sizes a run/replay/journal-chunk read against the MTU must
    /// subtract this, or a sealed datagram silently exceeds `mtu`.
    pub fn crypto_overhead(&self) -> usize {
        if self.crypto_enabled {
            CRYPTO_OVERHEAD
        } else {
            0
        }
    }
}

#[derive(Default)]
pub struct SenderStats {
    pub datagrams: AtomicU64,
    pub bytes: AtomicU64,
    pub naks_served: AtomicU64,
    pub heartbeats: AtomicU64,
    pub flow_stalls: AtomicU64,
    /// A NAK unservable from EITHER source — the requested bytes had scrolled out
    /// of the ring AND could not be replayed from the journal (no replay source
    /// wired, or the position is below the first archived block: purged; M6). With
    /// a replay source set for a still-archived position the seam IS served (see
    /// `replay_datagrams`) and is not counted here — this counter is strictly the
    /// "bytes gone from ring and journal" case.
    pub overruns: AtomicU64,
    /// DATA datagrams retransmitted from the JOURNAL to serve a deep NAK whose
    /// bytes had already scrolled out of the ring (M4 replay sessions). This is
    /// the proof the replay path ran; the M2 ring-served NAK path counts under
    /// `datagrams` / `naks_served` as before.
    pub replay_datagrams: AtomicU64,
    /// NAK requests dropped because the queue hit `NAK_QUEUE_MAX` (oldest
    /// dropped first); observability only — a re-NAK after backoff recovers.
    pub naks_dropped: AtomicU64,
    /// NAK positions that are provably corrupt: not a frame boundary — same
    /// fail-closed posture as the receiver's DATA guards. Rejected at ingestion
    /// before the position can reach the journal path (where a garbage length
    /// at an arbitrary offset would panic the sender agent); the wire has no
    /// CRC, so a bit-flip escaping the UDP checksum could misalign a position.
    pub naks_rejected: AtomicU64,
    /// M6 Task 6: snapshot sessions opened (a below-floor NAK upgraded to a file
    /// ship instead of counting an overrun).
    pub snap_sessions: AtomicU64,
    /// M6 Task 6: SNAP_CHUNK datagrams sent (cursor fill + repair).
    pub snap_chunks: AtomicU64,
    /// M6 Task 6: SNAP_CHUNK datagrams sent specifically to repair a peer NAK.
    pub snap_chunk_naks: AtomicU64,
    /// M14c2 (T10a): a snapshot set was refused because one of its artifact
    /// FILES could not be opened — the `File::open` TOCTOU. The
    /// `SnapshotSource` closure lists what each FSM's store says is durable
    /// below its persisted floor marker; between that listing and this open the
    /// file can be gone (a purge racing the session), unreadable (a permission
    /// or ownership change), or replaced by a directory (a hand-edited snapshot
    /// dir).
    ///
    /// Not fatal and not the peer's fault: the NAK stays a counted overrun and
    /// the peer re-NAKs, so a transient race self-heals. But it was the ONE
    /// refusal in `try_open_snap_session` with no counter at all —
    /// indistinguishable from the two ordinary ones (no source wired, a session
    /// already in flight), so a leader whose snapshot dir had gone bad looked
    /// exactly like a leader that simply never ships snapshots, while the
    /// joiner NAKed forever. A PERSISTENT count is "go and look at the
    /// LEADER's snapshot directory".
    pub snap_open_failed: AtomicU64,
    /// M8: an outgoing datagram this sender could not seal — dropped rather
    /// than sent. Covers both scopes (T17 widened it from T10's DATA/HEARTBEAT
    /// only):
    /// * `Scope::Group` (DATA/HEARTBEAT): `NoGroupKey`, an evicted epoch.
    /// * `Scope::Pairwise` (SNAP_BEGIN/SNAP_CHUNK, T17): `NoSession` with the
    ///   destination, or no `SocketAddr -> NodeId` entry to name one.
    ///
    /// Never fatal: a dropped DATA self-heals via NAK repair (the follower's
    /// contiguous frontier never advances past it); a dropped HEARTBEAT is
    /// superseded by the next one; a dropped SNAP chunk is re-requested by the
    /// peer's snapshot NAK timer, and a dropped SNAP_BEGIN is retried next
    /// duty cycle (the session does not latch as begun). Observability only —
    /// mirrored into the cnc band by `refresh_peer_obs`, since a PERSISTENT
    /// failure (crypto on, no key/session ever) is silent from outside.
    pub seal_failures: AtomicU64,
}

pub struct Sender {
    buffer: Arc<LogBuffer>,
    sock: FaultSocket,
    followers: Vec<SocketAddr>,
    flow: FlowControl,
    ctrl: mpsc::Receiver<CtrlMsg>,
    cfg: SenderConfig,
    sent: u64,
    /// Frame-run staging (read_run_validated output).
    run: Vec<u8>,
    /// Datagram assembly (header + run).
    scratch: Vec<u8>,
    naks: VecDeque<(SocketAddr, u64, u32)>,
    /// M7: the raw `(contiguous, window)` of the most recent `Status` from
    /// each source address, kept RAW (not the derived `contiguous + window`
    /// combined limit) so a `SetPeers` rebuild can re-feed `FlowControl::on_status`
    /// and have it re-classify each address as voter/learner under the NEW
    /// membership (a promoted learner's last advert becomes a voter limit, and
    /// vice versa) rather than replaying a stale derived value. An address no
    /// longer present after rebuild is silently ignored by `on_status` — no
    /// pruning needed.
    last_status: HashMap<SocketAddr, (u64, u32)>,
    base: Instant,
    last_heartbeat_ns: u64,
    stats: Arc<SenderStats>,
    /// Journal handle for serving deep NAKs whose bytes have left the ring
    /// (M4 replay sessions). `None` until `set_replay_source` wires the
    /// archive's journal in.
    replay: Option<Arc<Journal>>,
    /// Live leadership term (M4): stamps every DATA/HEARTBEAT datagram. The
    /// consensus agent (Task 8) is the sole writer; this thread only loads it
    /// (`Relaxed`). Distinct from `cfg.term_id`, which is retained for the
    /// legacy `SenderConfig` API but no longer used for stamping.
    term: TermHandle,
    /// Leader-role gate (M4 node composition): while this reads `false` the
    /// node is NOT the leader, so the sender streams no DATA, serves no NAKs,
    /// and emits no heartbeats — a demoted leader goes silent on the next duty
    /// cycle. The consensus agent (Task 8) is the sole writer.
    role: Arc<AtomicBool>,
    /// Last observed leader-role state. A `false → true` edge means this node
    /// was just promoted, so the send cursor resyncs to the (re-primed) `sent`
    /// counter before streaming — `BecomeLeader` collapsed volatile to the
    /// durable base, and a stale cached cursor would re-stream and regress the
    /// counter.
    was_leader: bool,
    /// M6 Task 6: newest shippable snapshot resolver (node-wired). `None` = this
    /// node never ships snapshots (a below-floor NAK stays an overrun).
    snapshot_source: Option<SnapshotSource>,
    /// M6 Task 6: the single in-flight outbound snapshot session, if any.
    snap: Option<SnapSession>,
    /// M14c2 (T10a): `snap_open_failed`'s operator `eprintln!` has fired. The
    /// counter increments on every occurrence (undercounting would hide the
    /// problem); the log line is latched for the life of the process, because
    /// a below-floor peer re-NAKs on its own backoff and a genuinely bad
    /// snapshot dir would otherwise write a line per NAK, forever. Cleared
    /// when a session DOES open — a set that opens proves the dir is readable
    /// again, so the next failure is worth naming.
    snap_open_failed_logged: bool,
    /// M6 Task 6: monotonic session-id generator — distinguishes a fresh session
    /// from a just-closed one so a stale SNAP_NAK/SNAP_DONE can't cross-talk.
    snap_session_seq: u32,
    /// M6 Task 9: cnc observability band + addr→slot map. `None` on nodes that
    /// never lead and in unit tests. Once per duty cycle the sender fills each
    /// peer's `advertised_limit` from its flow-control view (bounded — a pass over
    /// ≤8 slots, no per-datagram cnc writes). Diagnostics only.
    peer_obs: Option<PeerObs>,
    /// M8 (Task 10): wire crypto, if enabled. `None` = every datagram this
    /// sender assembles stays cleartext, byte-for-byte what pre-M8 code
    /// produced. `Some` seals every DATA/HEARTBEAT datagram once inside
    /// `assemble`/`seal_scratch` — `fan_out`'s loop still sends the SAME
    /// sealed bytes to every follower (one seal, N sends; see the module
    /// docs and `SenderConfig::crypto_enabled`'s doc for how this stays in
    /// sync with the MTU budget).
    ///
    /// A `SendHalf`, NOT a `Transport` (review round 1, 2026-07-29 ruling):
    /// `Sender` and `FollowerReceiver` (T11) are separate agents on separate
    /// threads, but a single process has exactly one set of handshake
    /// sessions, one group-key plane, and one boot salt. Owning a whole
    /// `Transport` by value here would make all of that permanently
    /// unreachable by the receiver and the node layer (T12) — see
    /// `uc_crypto::transport`'s "M8 ownership correction" module docs for
    /// the full account, including why the two naive fixes (a mutex around
    /// the whole `Transport`, or one `Transport` per agent) are both wrong.
    /// `SendHalf` owns only the nonce counter and the seal-cipher cache
    /// (sender-exclusive, no lock); the handshake/group-key state it reads
    /// through `seal` lives in the shared, `Arc<Mutex<_>>`-guarded state a
    /// `uc_crypto::SharedTransport` hands this half out from.
    crypto: Option<uc_crypto::SendHalf>,
    /// M8 (Task 17): `SocketAddr -> NodeId`, needed to seal the PAIRWISE-scope
    /// snapshot-session kinds (`SNAP_BEGIN`/`SNAP_CHUNK`). Group-scope kinds
    /// (`DATA`/`HEARTBEAT`) never consult this — the group key is the same for
    /// every destination, which is the whole point of the scope split.
    ///
    /// A LOCAL COPY, refreshed once per duty cycle from `peer_ids_src` rather
    /// than locked per datagram — identical discipline to
    /// `FollowerReceiver::peer_ids`; see [`PeerIds`] for why a boot-time
    /// snapshot is wrong (M7 changes membership at runtime, and a snapshot
    /// session's destination is exactly the joining node a stale map would
    /// not know about).
    peer_ids: HashMap<SocketAddr, NodeId>,
    /// The shared, versioned source `peer_ids` mirrors, plus the generation
    /// last mirrored. `None` when crypto is off (nothing consults `peer_ids`
    /// then — `assemble_snap` returns before it looks).
    peer_ids_src: Option<PeerIds>,
    peer_ids_gen: u64,
}

/// M8 (Task 17): everything the SEND path needs to run with crypto on, taken
/// as ONE value so neither piece can be forgotten — the same bundling
/// discipline (and the same reason) as `receiver::CryptoIntake`.
///
/// T10 shipped the half alone, which was sufficient only while every sealed
/// kind was `Scope::Group` (group sealing ignores the destination). T17 seals
/// the PAIRWISE snapshot kinds, which need a `NodeId` for the destination —
/// and a `Sender` handed a `SendHalf` with no way to resolve one would seal
/// nothing and drop every snapshot chunk, silently, forever. A compile error
/// instead.
pub struct SenderCrypto {
    /// The process's single [`SendHalf`] (`SharedTransport::send_half`).
    pub half: SendHalf,
    /// The live sender-identity map — see [`PeerIds`].
    pub peer_ids: PeerIds,
}

/// M6 Task 9: the sender's observability handle — the cnc page plus the
/// `(peer addr, slot index)` map it fills with `advertised_limit`.
type PeerObs = (Arc<CncPage>, Vec<(SocketAddr, usize)>);

impl Sender {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        buffer: Arc<LogBuffer>,
        sock: FaultSocket,
        followers: Vec<SocketAddr>,
        cluster_size: usize,
        ctrl: mpsc::Receiver<CtrlMsg>,
        cfg: SenderConfig,
        term: TermHandle,
        role: Arc<AtomicBool>,
    ) -> Sender {
        Self::with_learners(
            buffer,
            sock,
            followers,
            &[],
            cluster_size,
            ctrl,
            cfg,
            term,
            role,
        )
    }

    /// M6 Task 7: leader-side voter/learner split. `followers` is the full fan-out
    /// (voters-minus-self ++ learners, streamed identically); `learners` is the
    /// subset excluded from flow-control's quorum statistic. `cluster_size` is the
    /// VOTING cluster size.
    #[allow(clippy::too_many_arguments)]
    pub fn with_learners(
        buffer: Arc<LogBuffer>,
        sock: FaultSocket,
        followers: Vec<SocketAddr>,
        learners: &[SocketAddr],
        cluster_size: usize,
        ctrl: mpsc::Receiver<CtrlMsg>,
        cfg: SenderConfig,
        term: TermHandle,
        role: Arc<AtomicBool>,
    ) -> Sender {
        Self::with_crypto(
            buffer,
            sock,
            followers,
            learners,
            cluster_size,
            ctrl,
            cfg,
            term,
            role,
            None,
        )
    }

    /// M8 (Task 10): the innermost constructor — `with_learners`/`new` are thin
    /// wrappers over this with `crypto: None`. `crypto: Some(..)` seals
    /// every DATA/HEARTBEAT datagram `assemble` builds and (T17) every
    /// `SNAP_BEGIN`/`SNAP_CHUNK` `assemble_snap` builds; `cfg.crypto_enabled`
    /// MUST already agree with `crypto.is_some()` (asserted below) — the MTU
    /// budget every read site computes from `cfg.crypto_overhead()` would
    /// otherwise silently disagree with what this constructor is about to do.
    ///
    /// Takes a [`SenderCrypto`] (a `SendHalf` from
    /// `uc_crypto::SharedTransport::send_half`, plus the live [`PeerIds`]
    /// map T17's pairwise seals resolve destinations through), never a whole
    /// `Transport` — see the `crypto` field's doc above for why. The caller
    /// (the node layer, T12) owns the `SharedTransport` and calls
    /// `send_half()` exactly once per process; this constructor has no way
    /// to enforce that single-call discipline itself (it only ever sees the
    /// `SendHalf` already handed out).
    #[allow(clippy::too_many_arguments)]
    pub fn with_crypto(
        buffer: Arc<LogBuffer>,
        sock: FaultSocket,
        followers: Vec<SocketAddr>,
        learners: &[SocketAddr],
        cluster_size: usize,
        ctrl: mpsc::Receiver<CtrlMsg>,
        cfg: SenderConfig,
        term: TermHandle,
        role: Arc<AtomicBool>,
        crypto: Option<SenderCrypto>,
    ) -> Sender {
        let (crypto, peer_ids_src) = match crypto {
            Some(SenderCrypto { half, peer_ids }) => (Some(half), Some(peer_ids)),
            None => (None, None),
        };
        assert_eq!(
            cfg.crypto_enabled,
            crypto.is_some(),
            "SenderConfig::crypto_enabled must agree with whether a SendHalf was supplied \
             (mismatch here is exactly the class of bug that lets a sealed datagram overrun \
             the MTU: the budget math trusts crypto_enabled, not crypto.is_some())"
        );
        assert!(
            align_frame_len(HEADER_LEN + buffer.max_payload())
                + DATAGRAM_HEADER_LEN
                + cfg.crypto_overhead()
                <= cfg.mtu,
            "a max-size frame (+ crypto overhead, if enabled) must fit one datagram \
             (raise mtu — the jumbo-frame knob)"
        );
        // Voting followers pace commit; learners are fanned-out to but never enter
        // `limit()`.
        let voting: Vec<SocketAddr> = followers
            .iter()
            .copied()
            .filter(|a| !learners.contains(a))
            .collect();
        let flow = FlowControl::new(&voting, cluster_size, cfg.initial_window, learners);
        let sent = buffer.counters().sent.load_acquire();
        Sender {
            buffer,
            sock,
            followers,
            flow,
            ctrl,
            cfg,
            sent,
            run: Vec::with_capacity(cfg.mtu),
            scratch: Vec::with_capacity(cfg.mtu),
            naks: VecDeque::new(),
            last_status: HashMap::new(),
            base: Instant::now(),
            last_heartbeat_ns: 0,
            stats: Arc::new(SenderStats::default()),
            replay: None,
            term,
            role,
            was_leader: false,
            snapshot_source: None,
            snap: None,
            snap_open_failed_logged: false,
            snap_session_seq: 0,
            peer_obs: None,
            crypto,
            peer_ids: peer_ids_src
                .as_ref()
                .map(PeerIds::snapshot)
                .unwrap_or_default(),
            peer_ids_gen: peer_ids_src.as_ref().map(PeerIds::generation).unwrap_or(0),
            peer_ids_src,
        }
    }

    /// M8 (Task 17): mirror the shared [`PeerIds`] map if the writer has
    /// published a new generation since the last duty cycle. One `Acquire`
    /// load per cycle in the common (unchanged) case; the `Mutex` is touched
    /// only when membership actually changed. Identical to
    /// `FollowerReceiver::refresh_peer_ids` — see [`PeerIds`] for why the map
    /// cannot simply be a boot-time snapshot.
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

    pub fn stats(&self) -> Arc<SenderStats> {
        Arc::clone(&self.stats)
    }

    /// M6 Task 9: wire the cnc observability band + addr→slot map. The sender
    /// fills each peer's `advertised_limit` once per duty cycle. Without this call
    /// the band's sender-owned cells stay dormant (unit tests, non-leaders).
    pub fn set_peer_slots(&mut self, cnc: Arc<CncPage>, slots: Vec<(SocketAddr, usize)>) {
        self.peer_obs = Some((cnc, slots));
    }

    /// Wire the newest-shippable-snapshot resolver (M6 Task 6). Without it a
    /// below-floor NAK stays an overrun; with it that NAK upgrades to a snapshot
    /// session. The node supplies a closure over `SnapshotStore` filtered by its
    /// durable floor marker, so a session only ever ships a fully-published file.
    pub fn set_snapshot_source(&mut self, src: SnapshotSource) {
        self.snapshot_source = Some(src);
    }

    /// Wire the archive's journal in as the retransmit source for deep NAKs
    /// (positions that have already scrolled out of the ring). Without it a
    /// deep NAK counts an `overrun` and wedges the follower — WITH it the seam
    /// is served from durable storage (M4 replay sessions, closing M2's
    /// >1-ring-behind gap). One handle, shared `&self` with internal locking.
    pub fn set_replay_source(&mut self, journal: Arc<Journal>) {
        self.replay = Some(journal);
    }

    fn now_ns(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    /// One duty cycle: drain control, serve one NAK, stream up to
    /// `dgrams_per_cycle` datagrams, heartbeat on interval.
    pub fn do_work(&mut self) -> bool {
        let mut did = false;

        // M8 (Task 17): pick up a membership change before anything this
        // cycle can need to resolve a destination's `NodeId` (a snapshot
        // session's peer is very often exactly the node that just joined).
        self.refresh_peer_ids();

        while let Ok(m) = self.ctrl.try_recv() {
            match m {
                CtrlMsg::Status {
                    from,
                    contiguous,
                    window,
                } => {
                    self.last_status.insert(from, (contiguous, window));
                    self.flow.on_status(from, contiguous, window)
                }
                CtrlMsg::Nak {
                    from,
                    position,
                    length,
                } => {
                    // Fail closed against a corrupt/hostile position. A NAK
                    // position is a stream byte offset, and every frame boundary
                    // is 32-byte aligned; a position that is NOT frame-aligned
                    // can never name a real frame. Trusting it would drive
                    // `chunk_frames`/`read_header` to an arbitrary offset in an
                    // archived block, read a garbage length, and panic the
                    // sender agent (there is no wire CRC to catch the flip).
                    // Reject + count — the receiver's DATA path is equally
                    // fail-closed; a re-NAK from an honest follower is aligned.
                    if !position.is_multiple_of(FRAME_ALIGNMENT as u64) {
                        self.stats.naks_rejected.fetch_add(1, Ordering::Relaxed);
                        did = true;
                        continue;
                    }
                    // Coalesce per follower. A follower's NAK position is its
                    // current contiguous frontier — monotonic non-decreasing —
                    // so its latest NAK supersedes any earlier one still queued.
                    // Keeping ONE slot per follower is what makes deep-replay
                    // catch-up (M4) viable: without it a follower re-NAKing its
                    // stuck frontier every backoff piles hundreds of redundant
                    // retransmit requests behind its real progress, and the
                    // FIFO serve spends the whole duty budget re-sending bytes
                    // the follower already has (a self-inflicted NAK storm —
                    // measured ~0.8% goodput before this). The cap stays as a
                    // belt-and-suspenders guard against an unknown/spoofed flood
                    // (many distinct source addresses).
                    if let Some(slot) = self.naks.iter_mut().find(|(a, _, _)| *a == from) {
                        // Guard against a reordered ctrl delivery regressing the
                        // frontier: only a non-stale position (>= the queued
                        // one) replaces the slot.
                        if position >= slot.1 {
                            slot.1 = position;
                            slot.2 = length;
                        }
                    } else {
                        if self.naks.len() >= NAK_QUEUE_MAX {
                            self.naks.pop_front();
                            self.stats.naks_dropped.fetch_add(1, Ordering::Relaxed);
                        }
                        self.naks.push_back((from, position, length));
                    }
                }
                CtrlMsg::SnapNak {
                    from,
                    session,
                    offset,
                    length,
                } => {
                    // A repair request for the active session only (a stale
                    // session id — the peer NAKing a transfer we already closed —
                    // is dropped). Bounded implicitly by the file size / peer.
                    if let Some(s) = self.snap.as_mut()
                        && s.peer == from
                        && s.session == session
                    {
                        // M14c review: `last_activity_ns` is refreshed only by a
                        // request this session can actually SERVE. An offset at
                        // or past the stream is unservable — `part_at` returns
                        // `None`, `drive_snap_session` drops it without progress
                        // — so refreshing on it would let a peer that only ever
                        // NAKs garbage (or a stale/racing intake) pin the single
                        // session slot forever, exactly the failure mode the
                        // BEGIN-resend exclusion in `drive_snap_session` closes.
                        // A servable NAK needs no refresh here either: serving it
                        // sets `progress`, which refreshes on the real event.
                        if offset < s.stream_len {
                            s.last_activity_ns = self.base.elapsed().as_nanos() as u64;
                        }
                        s.naks.push_back((offset, length));
                    }
                }
                CtrlMsg::SnapDone { from, session } => {
                    // The peer has the whole file — close the session (frees the
                    // slot for the next requester).
                    if let Some(s) = self.snap.as_ref()
                        && s.peer == from
                        && s.session == session
                    {
                        self.snap = None;
                    }
                }
                CtrlMsg::SetPeers {
                    followers,
                    learners,
                    cluster_size,
                } => {
                    // Rebuild flow control from the new voting/learner split,
                    // re-feeding every surviving address's last raw advert so
                    // ranking does not restart from the bootstrap window (mirrors
                    // `ElectionSm::rebuild_membership`'s carried-reports rationale).
                    self.flow = FlowControl::new(
                        &followers,
                        cluster_size,
                        self.cfg.initial_window,
                        &learners,
                    );
                    for (&addr, &(contiguous, window)) in self.last_status.iter() {
                        self.flow.on_status(addr, contiguous, window);
                    }
                    // Full fan-out = the new voters-minus-self ++ learners-minus-self,
                    // streamed identically (same shape `with_learners` builds at
                    // construction).
                    self.followers = followers.into_iter().chain(learners).collect();
                    // The peer-observability slot mapping doesn't change here (M6
                    // Task 9's cnc band is keyed by NodeId, which SetPeers doesn't
                    // carry) — refresh whatever it already tracks against the new
                    // flow view immediately rather than waiting out a stale cycle.
                    self.refresh_peer_obs();
                }
            }
            did = true;
        }

        // Leader-role gate (M4): a follower drains control (above) but produces
        // NO leader output — no NAK service, no DATA stream, no heartbeats.
        let leader_role = self.role.load(Ordering::Relaxed);
        if leader_role && !self.was_leader {
            // Promotion edge: BecomeLeader re-primed `sent` to the durable base;
            // adopt it so we stream only the fresh term's tail, never re-stream
            // (which would regress the counter under a slower cached cursor).
            self.sent = self.buffer.counters().sent.load_acquire();
        }
        self.was_leader = leader_role;
        if !leader_role {
            return did;
        }

        if let Some((to, pos, len)) = self.naks.pop_front() {
            self.serve_nak(to, pos, len);
            did = true;
        }

        let append = self.buffer.counters().append.load_acquire();
        let limit = self.flow.limit();
        // M8: shrunk by crypto_overhead() when crypto is on, so a run's body
        // plus the counter+tag `assemble`/`seal_scratch` add never exceeds mtu.
        let budget = self.cfg.mtu - DATAGRAM_HEADER_LEN - self.cfg.crypto_overhead();
        let mut dgrams = 0;
        while dgrams < self.cfg.dgrams_per_cycle && self.sent < append && self.sent < limit {
            // don't read more than the flow limit allows in one datagram
            let flow_budget = (limit - self.sent).min(budget as u64) as usize;
            match self
                .buffer
                .read_run_validated(self.sent, flow_budget, &mut self.run)
            {
                SliceRead::Run(r) => {
                    if self.sent + r.advance > limit {
                        // a single frame overshoots the remaining window
                        // (read_run_validated always returns >= 1 frame):
                        // wait for the window to open
                        self.stats.flow_stalls.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    self.fan_out(self.sent, r.bytes);
                    self.sent += r.advance;
                    self.buffer.counters().sent.store_release(self.sent);
                    did = true;
                    dgrams += 1;
                }
                SliceRead::NotCommitted => break,
                SliceRead::Overrun => {
                    // The fan-out cursor lapped the ring — it cannot happen
                    // while `sent` tracks `append` closely (the steady state),
                    // and never fires in the paced integration flow. If it does
                    // (e.g. a sender constructed against an already-lapped ring)
                    // AND a replay source is wired, the gap is durable in the
                    // journal: resync the fan-out to `append` and let each
                    // follower NAK the skipped span (served from the journal,
                    // `serve_nak_from_journal`). Only WITHOUT a replay source is
                    // this an unrecoverable overrun worth counting.
                    if self.replay.is_some() {
                        self.sent = append;
                        self.buffer.counters().sent.store_release(self.sent);
                    } else {
                        self.stats.overruns.fetch_add(1, Ordering::Relaxed);
                    }
                    break;
                }
            }
        }
        if self.sent < append && self.sent >= limit {
            self.stats.flow_stalls.fetch_add(1, Ordering::Relaxed);
        }

        // M6 Task 6: drive the snapshot session LAST — strictly below live DATA
        // and NAK-replay (both already served above), capped per cycle, so a
        // transfer can never starve the live stream.
        if self.drive_snap_session() {
            did = true;
        }

        let now = self.now_ns();
        if now - self.last_heartbeat_ns >= self.cfg.heartbeat_ns {
            self.last_heartbeat_ns = now;
            // M8: a failed seal drops this heartbeat (counted inside
            // assemble/seal_scratch) rather than sending it half-built; the
            // interval marker above still advances, so a persistent NoGroupKey
            // condition doesn't spin — the next heartbeat simply retries.
            if self.assemble(append, DGRAM_KIND_HEARTBEAT, 0) {
                for &to in &self.followers {
                    let _ = self.sock.send_to(&self.scratch, to);
                }
                // CommitPosition gossip (spec §6, on-advance + the 100 ms floor) is
                // the consensus agent's job now (`Action::GossipCommit`) — the
                // sender no longer ranks or gossips commit at all (M4 carry #5).
                self.stats.heartbeats.fetch_add(1, Ordering::Relaxed);
            }
            did = true;
        }

        // M6 Task 9: refresh each peer's `advertised_limit` in the cnc band from
        // the flow-control view. Bounded (≤8 slots, once per cycle); leader-only
        // (this point is past the `!leader_role` return) so a demoted node stops
        // updating. Diagnostics — never gates the stream.
        self.refresh_peer_obs();
        did
    }

    /// M6 Task 9 (extracted M7): fill each tracked peer's `advertised_limit`
    /// cnc slot from the current `FlowControl` view, and (M8, Task 10 review
    /// round 1) mirror the cumulative seal-failure count into the cnc page.
    /// Called once per duty cycle AND immediately after a `SetPeers` rebuild
    /// (M7) so a reconfigured peer's slot doesn't wait out a stale cycle.
    /// Bounded (≤8 slots + 1 line); a no-op when `set_peer_slots` was never
    /// called (non-leader / unit tests).
    ///
    /// `seal_failures` specifically: `SenderStats::seal_failures` alone is
    /// process-internal — invisible to an operator or a monitoring agent
    /// outside this node. A PERSISTENT seal failure (e.g. crypto enabled but
    /// no group key ever activated) silently drops live DATA *and*
    /// HEARTBEAT, so a follower may never even learn there is a gap to NAK
    /// for — exactly the condition an operator must be able to see
    /// externally, the same way `advertised_limit` already is.
    fn refresh_peer_obs(&mut self) {
        if let Some((cnc, slots)) = &self.peer_obs {
            for (addr, idx) in slots {
                if let Some(limit) = self.flow.advertised_limit(*addr) {
                    cnc.peer_slot(*idx).advertised_limit.store_release(limit);
                }
            }
            cnc.store_seal_failures(self.stats.seal_failures.load(Ordering::Relaxed));
        }
    }

    /// Header + the first `body_bytes` of `self.run` into `self.scratch`, then
    /// seal it (M8) if crypto is enabled. Returns `false` if a seal attempt
    /// failed — `self.scratch` must then NOT be sent (see `seal_scratch`'s
    /// doc); every caller checks the return value before touching the socket.
    fn assemble(&mut self, position: u64, kind: u8, body_bytes: usize) -> bool {
        self.scratch.clear();
        self.scratch.resize(DATAGRAM_HEADER_LEN, 0);
        write_datagram_header(
            &mut self.scratch,
            &DatagramHeader {
                position,
                leadership_term_id: self.term.load(Ordering::Relaxed),
                kind,
                flags: 0,
                // M8: always written as 0 here — `Transport::seal`'s group
                // branch stamps the REAL epoch into this field itself, as the
                // last write before sealing (the header is AAD, so it must be
                // final before the AEAD call). Cleartext mode leaves it 0,
                // matching pre-M8 wire output exactly.
                key_epoch: 0,
            },
        );
        self.scratch.extend_from_slice(&self.run[..body_bytes]);
        self.seal_scratch(kind)
    }

    /// M8 (Task 10): seal `self.scratch` in place under crypto, if enabled.
    /// `kind` here is always `DGRAM_KIND_DATA` or `DGRAM_KIND_HEARTBEAT` — the
    /// only two kinds this module ever assembles — both `Scope::Group`, so
    /// `peer: None` is always the right call (group scope ignores it; the
    /// whole point of the group key is that the SAME sealed bytes go to every
    /// destination). Returns `true` when `self.scratch` is ready to send —
    /// always true in cleartext mode. Returns `false` on a seal failure
    /// (`NoGroupKey`, an evicted epoch, etc.): the datagram is dropped (never
    /// sent half-sealed or with stale plaintext) and `SenderStats::seal_failures`
    /// is bumped. This is never fatal to the agent — a dropped DATA self-heals
    /// via NAK repair once a group key is available; a dropped HEARTBEAT is
    /// superseded by the next interval.
    fn seal_scratch(&mut self, kind: u8) -> bool {
        // M8 review round 1 (2026-07-29, Minor): this function always calls
        // `seal` with `peer: None`, which is only correct for `Scope::Group`
        // kinds (Group ignores `peer`; Pairwise needs `Some(peer)` or gets
        // `MissingPeer` -> silently dropped in crypto mode while the SAME
        // call would work fine in cleartext, since cleartext never reaches
        // `seal` at all). Previously enforced only by a doc comment on this
        // function and on `assemble`/`fan_out`/`serve_nak`/`send_replay_dgram`
        // (every caller today IS `DGRAM_KIND_DATA` or `DGRAM_KIND_HEARTBEAT`,
        // both Group) — a future caller passing a Pairwise kind through this
        // path would compile fine and fail only at runtime, silently, in
        // crypto mode specifically. Pinned structurally instead.
        debug_assert!(
            matches!(Transport::scope_of(kind), Scope::Group),
            "seal_scratch is only ever correct for Scope::Group kinds (peer: None); \
             kind {kind} is not one — a Pairwise/Unsealed kind needs a real caller-supplied \
             peer and must not go through this path"
        );
        let Some(crypto) = self.crypto.as_mut() else {
            return true; // cleartext mode: nothing to do
        };
        // M8 review round 1 (2026-07-29): `now_ns` MUST come from the
        // SendHalf's own canonical clock (ultimately `SharedTransport`'s
        // single `base: Instant`, shared with every other half/agent that
        // touches crypto), never from `self.base` (this Sender agent's own,
        // otherwise-unrelated clock used for heartbeat cadence). Two
        // different clock origins would make `GroupPlane::sealing_epoch`'s
        // activation-grace-period comparison meaningless — see
        // `uc_crypto::transport`'s "One clock source" module docs.
        let now_ns = crypto.now_ns();
        match crypto.seal(kind, None, &mut self.scratch, now_ns) {
            Ok(()) => true,
            Err(_) => {
                self.stats.seal_failures.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// One scan, N sends (identical datagram to every follower). If sealing
    /// fails, nothing is sent this cycle to ANY follower (a half fan-out —
    /// some followers sealed, some not — is not a state this function ever
    /// produces): `assemble` already counted the failure.
    fn fan_out(&mut self, position: u64, body_bytes: usize) {
        if !self.assemble(position, DGRAM_KIND_DATA, body_bytes) {
            return;
        }
        for &to in &self.followers {
            let _ = self.sock.send_to(&self.scratch, to);
            self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
            self.stats
                .bytes
                .fetch_add(body_bytes as u64, Ordering::Relaxed);
        }
    }

    /// Retransmit [pos, pos+len) to ONE follower, MTU chunk by MTU chunk.
    /// `len` is capped by the follower (Task 8), so this is bounded work.
    fn serve_nak(&mut self, to: SocketAddr, pos: u64, len: u32) {
        let budget = self.cfg.mtu - DATAGRAM_HEADER_LEN - self.cfg.crypto_overhead();
        let end = pos + len as u64;
        let mut p = pos;
        while p < end {
            match self.buffer.read_run_validated(p, budget, &mut self.run) {
                SliceRead::Run(r) => {
                    // M8: a failed seal drops just this one retransmitted
                    // datagram (counted inside assemble/seal_scratch) — `p`
                    // still advances past it; the follower's contiguous
                    // frontier stays put and it re-NAKs, same as any other
                    // lost datagram.
                    if self.assemble(p, DGRAM_KIND_DATA, r.bytes) {
                        let _ = self.sock.send_to(&self.scratch, to);
                        self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
                        self.stats
                            .bytes
                            .fetch_add(r.bytes as u64, Ordering::Relaxed);
                    }
                    p += r.advance;
                }
                SliceRead::NotCommitted => break,
                SliceRead::Overrun => {
                    // Requested bytes have scrolled out of the ring. Serve the
                    // gap from the durable journal (M4 replay sessions); only
                    // when that is impossible (no source wired, or the position
                    // is below the first archived block — purged, M6) is it an
                    // unrecoverable overrun. Bounded to `REPLAY_DGRAMS_PER_NAK`
                    // datagrams — the follower re-NAKs the remainder.
                    if !self.serve_nak_from_journal(to, p, end) {
                        // Below the purge floor: gone from ring AND journal. M6
                        // Task 6 — upgrade to a snapshot session (ship the newest
                        // durable snapshot file to this peer) instead of counting
                        // an unrecoverable overrun. If none can be opened (no
                        // source, no file, or a session is already in flight) it
                        // stays an overrun and the peer re-NAKs.
                        if !self.try_open_snap_session(to) {
                            self.stats.overruns.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    break;
                }
            }
        }
        self.stats.naks_served.fetch_add(1, Ordering::Relaxed);
    }

    /// Serve a deep NAK for `[pos, end)` from the journal: walk the archived
    /// blocks that cover it, starting at `pos`'s frame boundary, replaying up to
    /// `REPLAY_DGRAMS_PER_NAK` DATA datagrams — byte-identical to the ring path
    /// (`chunk_frames` mirrors `read_run_validated`'s wire discipline). The
    /// budget spans block boundaries: under heavy streaming the archive records
    /// many small blocks, so a one-block cap would starve a deep catch-up.
    /// Returns `false` (caller counts an overrun) only when nothing at all was
    /// servable — no replay source, or `pos` is below the first archived block
    /// (purged — M6). The re-NAK paces whatever the budget left behind.
    fn serve_nak_from_journal(&mut self, to: SocketAddr, pos: u64, end: u64) -> bool {
        let Some(journal) = self.replay.clone() else {
            return false; // no replay source wired (M2/M3 posture)
        };
        // M8: chunk_frames -> send_replay_dgram seals these too (still DATA,
        // and scope is decided by kind, never by destination or by which
        // source — ring or journal — served it), so this budget shrinks by
        // crypto_overhead() exactly like serve_nak's ring-path budget above.
        let budget = self.cfg.mtu - DATAGRAM_HEADER_LEN - self.cfg.crypto_overhead();
        let mut p = pos;
        let mut emitted = 0usize;
        let mut served_any = false;
        while emitted < REPLAY_DGRAMS_PER_NAK && p < end {
            // Below the first archived block (purged) or a journal I/O error:
            // not servable. A read error is fail-stop territory elsewhere;
            // treating it as unserved here is safe (the follower re-NAKs) and
            // keeps this hot path infallible.
            let Some((seq, base)) = find_block(&journal, p).ok().flatten() else {
                break;
            };
            let Ok(Some((rbase, block))) = journal.read(seq) else {
                break;
            };
            debug_assert_eq!(rbase, base, "find_block seq/base must agree with the read");
            let block_end = base + block.len() as u64;
            if p >= block_end {
                // `p` sits at/beyond the durable frontier (last block fully
                // consumed): nothing archived here to serve.
                break;
            }
            served_any = true;
            chunk_frames(&block, base, p, budget, |dp, body| {
                if emitted >= REPLAY_DGRAMS_PER_NAK {
                    return; // budget spent; the re-NAK fetches the remainder
                }
                self.send_replay_dgram(to, dp, body);
                emitted += 1;
            });
            // If the budget wasn't spent, this whole block was replayed —
            // advance to the next block. If it was, `p` is irrelevant (the loop
            // exits) and the follower re-NAKs from where it actually got to.
            p = block_end;
        }
        served_any
    }

    // -- M6 Task 6: snapshot session -----------------------------------------

    /// Try to open a snapshot session to `to` in response to a below-floor NAK.
    /// Returns `false` (caller counts an overrun) when: a session is already in
    /// flight (one at a time — the peer re-NAKs and waits), no source is wired,
    /// no shippable snapshot set exists, the set breaks its invariants, or one
    /// of the files cannot be opened.
    fn try_open_snap_session(&mut self, to: SocketAddr) -> bool {
        if self.snap.is_some() {
            return false;
        }
        let Some(src) = self.snapshot_source.clone() else {
            return false;
        };
        let Some(set) = src() else {
            return false;
        };
        if set.artifacts.is_empty() {
            return false;
        }
        // M14c review: the set must COVER the declared mask exactly — one
        // artifact per declared id and no artifact outside it. Without this a
        // source that lost one FSM's file (or gained a stray one) opens a
        // session whose BEGINs advertise `services_declared` the stream can
        // never satisfy: the receiver's `received != services_declared` never
        // closes, it probes for a BEGIN that will never come, and the transfer
        // burns the slot until the timeout instead of staying an overrun the
        // peer re-NAKs.
        if set.services_declared != identity_mask(&set.identity)
            || set.artifacts.len() != set.services_declared.count_ones() as usize
            || set
                .artifacts
                .iter()
                .any(|a| a.service_id >= 64 || set.services_declared & (1u64 << a.service_id) == 0)
        {
            return false;
        }
        // Build the parts eagerly: every file must open and every invariant
        // must hold before a single datagram goes out, so a half-formed set
        // stays an overrun (the peer re-NAKs) rather than a session the
        // receiver can never complete.
        let mut parts = Vec::with_capacity(set.artifacts.len());
        let mut base = 0u64;
        let mut prev_id: Option<u8> = None;
        for a in &set.artifacts {
            if a.len == 0 || prev_id.is_some_and(|p| p >= a.service_id) {
                return false; // empty artifact, or not strictly ascending
            }
            prev_id = Some(a.service_id);
            let file = match std::fs::File::open(&a.path) {
                Ok(f) => f,
                Err(e) => {
                    // M14c2 (T10a): the TOCTOU between the store's listing and
                    // this open. Counted so a leader whose snapshot dir has
                    // gone bad is diagnosable instead of looking like a leader
                    // that simply never ships snapshots; see
                    // [`SenderStats::snap_open_failed`].
                    self.stats.snap_open_failed.fetch_add(1, Ordering::Relaxed);
                    if !self.snap_open_failed_logged {
                        self.snap_open_failed_logged = true;
                        eprintln!(
                            "uc_net: snapshot session refused -- cannot open artifact {} for \
                             service {}: {e} (the peer's below-floor NAK stays an overrun and \
                             will be re-sent; check this node's snapshot directory)",
                            a.path.display(),
                            a.service_id
                        );
                    }
                    return false;
                }
            };
            parts.push(SnapPart {
                service_id: a.service_id,
                snapshot_pos: a.snapshot_pos,
                base,
                len: a.len,
                file,
                begun_ns: None,
            });
            base += a.len;
        }
        let sid = self.snap_session_seq.wrapping_add(1);
        self.snap_session_seq = sid;
        self.snap = Some(SnapSession {
            peer: to,
            session: sid,
            identity: set.identity,
            version: set.version,
            parts,
            stream_len: base,
            cursor: 0,
            naks: VecDeque::new(),
            last_activity_ns: self.base.elapsed().as_nanos() as u64,
            config: set.config,
        });
        self.stats.snap_sessions.fetch_add(1, Ordering::Relaxed);
        // A set that opened proves the snapshot dir is readable again: re-arm
        // the open-failure log so the NEXT bad set is named once more.
        self.snap_open_failed_logged = false;
        true
    }

    /// Advance the in-flight snapshot session by at most [`SNAP_DGRAMS_PER_CYCLE`]
    /// chunk datagrams: make sure this cycle's target artifact has a live
    /// `SNAP_BEGIN`, then serve peer repair NAKs, then fill the cursor
    /// sequentially. Abandons the session after [`SNAP_SESSION_TIMEOUT_NS`] with
    /// no progress. Returns `true` iff it did work.
    fn drive_snap_session(&mut self) -> bool {
        let Some(mut sess) = self.snap.take() else {
            return false;
        };
        let now = self.base.elapsed().as_nanos() as u64;
        if now.saturating_sub(sess.last_activity_ns) >= SNAP_SESSION_TIMEOUT_NS {
            // Abandoned (peer died, or its DONE was lost): drop the session; the
            // slot frees for the next requester. `self.snap` stays `None`.
            return true;
        }

        // Which artifact this cycle's first datagram targets: the head repair
        // NAK's, else the cursor's, else the last one (the stream is fully sent
        // and we are waiting for the DONE — keep its BEGIN alive).
        let target = sess
            .naks
            .front()
            .and_then(|&(off, _)| sess.part_at(off))
            .or_else(|| sess.part_at(sess.cursor))
            .unwrap_or(sess.parts.len() - 1);

        // `did` = this cycle put a datagram on the wire (the agent's duty
        // signal). `progress` = it did something a *live* peer's transfer needs
        // — a first BEGIN or a chunk. A BEGIN RE-send is deliberately NOT
        // progress: it fires on its own 20 ms cadence, so counting it would
        // refresh `last_activity_ns` forever and a dead peer's session would
        // never hit [`SNAP_SESSION_TIMEOUT_NS`] — pinning the single session
        // slot against every other requester.
        let mut did = false;
        let mut progress = false;
        let first_ever = sess.parts[target].begun_ns.is_none();
        let stale = sess.parts[target]
            .begun_ns
            .is_none_or(|at| now.saturating_sub(at) >= SNAP_BEGIN_RESEND_NS);
        if stale {
            let p = &sess.parts[target];
            let (peer, session, service_id, pos, len) =
                (sess.peer, sess.session, p.service_id, p.snapshot_pos, p.len);
            let identity = sess.identity;
            let version = sess.version;
            // M8 (Task 17): `begun_ns` latches only on a datagram that actually
            // reached the wire. A seal failure (no session with this peer yet)
            // must leave the artifact un-begun so the NEXT cycle retries the
            // BEGIN — latching it unconditionally would ship chunks a receiver
            // with no intake for them can only drop.
            if self.send_snap_begin(
                peer,
                session,
                service_id,
                pos,
                len,
                &identity,
                &version,
                &sess.config,
            ) {
                sess.parts[target].begun_ns = Some(now);
                did = true;
                progress |= first_ever;
            } else if first_ever {
                // Nothing in this artifact can make progress until the peer has
                // its BEGIN; keep the slot and retry next cycle. `false` (no
                // work done) deliberately: a session whose peer has no key yet
                // must not keep the agent's duty loop hot, and
                // `last_activity_ns` is left un-refreshed so the session is
                // abandoned on the ordinary `SNAP_SESSION_TIMEOUT_NS` path if
                // the link never comes up.
                self.snap = Some(sess);
                return false;
            }
        }

        let mut emitted = 0usize;
        // Repair NAKs first (the peer is blocked on these).
        while emitted < SNAP_DGRAMS_PER_CYCLE {
            let Some((offset, length)) = sess.naks.pop_front() else {
                break;
            };
            // M14c2 (T10a): a request inside an artifact whose `SNAP_BEGIN` has
            // not gone out in this session is SKIPPED — not served, not an
            // error. The receiver can only place a chunk inside an artifact it
            // has already announced (`snap_chunk` drops everything else), so
            // serving one spends this cycle's chunk budget on datagrams that
            // are guaranteed to be discarded, while the peer stays blocked.
            // The BEGIN goes out on its own cadence (this cycle's `target` is
            // the HEAD request's artifact, so the head case fixes itself here);
            // dropping the request is the same shape a lost datagram already
            // takes — the peer's snapshot NAK timer re-fires.
            if sess
                .part_at(offset)
                .is_some_and(|i| sess.parts[i].begun_ns.is_none())
            {
                continue;
            }
            let n = self.send_snap_chunk(&mut sess, offset, true);
            if n == 0 {
                break; // outside every artifact / read error — drop the request
            }
            if (n as u32) < length {
                // Range spans multiple datagrams (or an artifact boundary):
                // re-queue the remainder.
                sess.naks.push_front((offset + n as u64, length - n as u32));
            }
            emitted += 1;
            did = true;
            progress = true;
        }
        // Then sequential cursor fill, artifact by artifact.
        while emitted < SNAP_DGRAMS_PER_CYCLE && sess.cursor < sess.stream_len {
            let at = sess.cursor;
            let Some(i) = sess.part_at(at) else {
                break;
            };
            if sess.parts[i].begun_ns.is_none() {
                // Crossed into the next artifact: its BEGIN goes out first, at
                // the top of the next cycle.
                break;
            }
            let n = self.send_snap_chunk(&mut sess, at, false);
            if n == 0 {
                break;
            }
            sess.cursor += n as u64;
            emitted += 1;
            did = true;
            progress = true;
        }

        if progress {
            sess.last_activity_ns = now;
        }
        self.snap = Some(sess);
        did
    }

    /// Read one MTU-sized chunk at STREAM offset `offset` from whichever
    /// artifact contains it and ship it as a SNAP_CHUNK (header `position` =
    /// the stream offset). A datagram never spans an artifact boundary — the
    /// receiver writes one datagram into exactly one `.part`. Returns bytes
    /// sent (0 past the stream / read error).
    fn send_snap_chunk(&mut self, sess: &mut SnapSession, offset: u64, is_nak: bool) -> usize {
        let Some(i) = sess.part_at(offset) else {
            return 0;
        };
        // M8 (Task 17): `- crypto_overhead()`. T10 deliberately left this
        // un-subtracted while SNAP was cleartext — the ONE of the four MTU
        // budget sites in this file that did not need it then. A sealed chunk
        // adds the 8-byte counter and the 16-byte tag, so without this the
        // datagram overruns `mtu` by exactly `CRYPTO_OVERHEAD` on every full
        // chunk (which, at the default 1408, is every chunk of a snapshot
        // bigger than one datagram — i.e. all of them).
        let budget = self.cfg.mtu - DATAGRAM_HEADER_LEN - self.cfg.crypto_overhead();
        let part_end = sess.parts[i].base + sess.parts[i].len;
        let want = ((part_end - offset) as usize).min(budget);
        let in_file = offset - sess.parts[i].base;
        let mut buf = vec![0u8; want];
        if sess.parts[i].file.seek(SeekFrom::Start(in_file)).is_err() {
            return 0;
        }
        if sess.parts[i].file.read_exact(&mut buf).is_err() {
            return 0;
        }
        if !self.assemble_snap(sess.peer, offset, DGRAM_KIND_SNAP_CHUNK, &buf) {
            // Sealed-or-dropped: never a cleartext fallback. Reported as 0
            // bytes sent, which leaves the sequential cursor exactly where it
            // was (retried next cycle) and drops a repair request (the peer's
            // snapshot NAK timer re-fires) — the same shape a lost datagram
            // already takes on this path.
            return 0;
        }
        let _ = self.sock.send_to(&self.scratch, sess.peer);
        self.stats.snap_chunks.fetch_add(1, Ordering::Relaxed);
        if is_nak {
            self.stats.snap_chunk_naks.fetch_add(1, Ordering::Relaxed);
        }
        want
    }

    /// Ship one artifact's SNAP_BEGIN (header `position` = 0; body carries
    /// session / layout / service_id / pos / len / declared / config). M7 Task 6:
    /// `config` is the encoded `ConfigRecord.config` this session's
    /// `SnapshotSource` closure captured at open time — the body grows to fit it.
    /// Returns `false` if the datagram could not be sealed and was therefore
    /// dropped (M8 Task 17) — the caller must NOT latch the artifact as begun.
    #[allow(clippy::too_many_arguments)]
    fn send_snap_begin(
        &mut self,
        peer: SocketAddr,
        session: u32,
        service_id: u8,
        snapshot_pos: u64,
        total_len: u64,
        identity: &[u64; 8],
        version: &[u32; 8],
        config: &[u8],
    ) -> bool {
        let mut body = vec![0u8; SNAP_BEGIN_FIXED_LEN + config.len()];
        write_snap_begin_body(
            &mut body,
            &SnapBeginBody {
                session,
                layout: SNAP_BEGIN_LAYOUT_V3,
                service_id,
                snapshot_pos,
                total_len,
                identity: *identity,
                version: *version,
                config: config.to_vec(),
            },
        );
        if !self.assemble_snap(peer, 0, DGRAM_KIND_SNAP_BEGIN, &body) {
            return false;
        }
        let _ = self.sock.send_to(&self.scratch, peer);
        true
    }

    /// Assemble a snapshot-session datagram (header + explicit body) into
    /// scratch, then seal it (M8 Task 17) if crypto is enabled. Returns
    /// `false` when a seal was attempted and failed — `self.scratch` must
    /// then NOT be sent; both callers check.
    ///
    /// **Why this seals, when T10 left it cleartext.** `SNAP_BEGIN`/
    /// `SNAP_CHUNK` are `Scope::Pairwise` (`Transport::scope_of`), so sealing
    /// them needs the destination's `NodeId` — and until T12 nothing in
    /// `uc_net` had a `SocketAddr -> NodeId` map, nor was any handshake
    /// driven for a pairwise session to exist under. Both now exist
    /// (`self.peer_ids`, mirrored from the node's live [`PeerIds`]), and the
    /// gap they left open was not a confidentiality footnote:
    ///
    /// * `send_snap_chunk` ships the raw bytes of the service-built snapshot
    ///   artifact — the complete serialized state machine — with the file
    ///   offset in the header, so a passive capture reassembles the whole
    ///   database with no work.
    /// * `send_snap_begin`'s body carries `SnapBeginBody.config`, the encoded
    ///   cluster `ConfigRecord`, and the receive path feeds that straight into
    ///   `maybe_adopt_incoming_snapshot`. Unsealed means UNAUTHENTICATED: an
    ///   on-path attacker forges a `SNAP_BEGIN` to a joining or below-floor
    ///   node and installs **attacker-chosen application state AND
    ///   attacker-chosen cluster membership** — a consensus-integrity
    ///   primitive.
    ///
    /// Sealed through this `Sender`'s own `SendHalf` (`peer: Some(id)`), NOT
    /// through a second half or a `SharedTransport` clone: the half already
    /// draws from the process's one shared nonce counter, so this path is
    /// disjoint from every other seal path by construction.
    ///
    /// An unresolvable destination (no `peer_ids` entry — a peer removed from
    /// the config mid-session, or a map not yet published) drops and counts,
    /// exactly like a failed seal. There is deliberately no cleartext
    /// fallback anywhere on this path: one would make the entire feature
    /// optional per destination, which is the same as not having it.
    fn assemble_snap(&mut self, peer: SocketAddr, position: u64, kind: u8, payload: &[u8]) -> bool {
        self.scratch.clear();
        self.scratch.resize(DATAGRAM_HEADER_LEN, 0);
        write_datagram_header(
            &mut self.scratch,
            &DatagramHeader {
                position,
                leadership_term_id: self.term.load(Ordering::Relaxed),
                kind,
                flags: 0,
                // Pairwise scope carries no epoch (the session key is per
                // handshake, not per group epoch), so this stays 0 in both
                // modes — see `crypto_admit`'s mixed-mode diagnostic, which
                // is why it requires BOTH `key_epoch == 0` AND a
                // too-short-to-be-sealed length before it concludes anything.
                key_epoch: 0,
            },
        );
        self.scratch.extend_from_slice(payload);

        debug_assert!(
            matches!(Transport::scope_of(kind), Scope::Pairwise),
            "assemble_snap seals with an explicit peer, which is only correct for \
             Scope::Pairwise kinds; kind {kind} is not one"
        );
        if self.crypto.is_none() {
            return true; // cleartext mode: byte-for-byte the pre-M8 output
        }
        let Some(&peer_id) = self.peer_ids.get(&peer) else {
            self.stats.seal_failures.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        let crypto = self.crypto.as_mut().expect("checked Some just above");
        // The SendHalf's own canonical clock, never `self.base` — see
        // `seal_scratch`'s doc and `uc_crypto::transport`'s "One clock
        // source" module docs. (The pairwise branch ignores `now_ns`; passing
        // the right one anyway keeps this call site correct if the scope of a
        // SNAP kind is ever reclassified.)
        let now_ns = crypto.now_ns();
        match crypto.seal(kind, Some(peer_id), &mut self.scratch, now_ns) {
            Ok(()) => true,
            Err(_) => {
                self.stats.seal_failures.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// Assemble a DATA datagram from an arbitrary body slice (a journal-replay
    /// run) and send it to one follower. Same framing as `fan_out`/`assemble`,
    /// but the body is copied from the caller's slice (the ring path stages it
    /// in `self.run`; here it lives in the journal block).
    ///
    /// M8: this is still `DGRAM_KIND_DATA`, so it is sealed exactly like
    /// `assemble`'s output — scope is decided by kind, never by which source
    /// (ring vs. journal) served the bytes. Does not call `assemble` itself
    /// (its body is a caller slice, not `self.run` — borrowing both at once
    /// would conflict), but shares the same `seal_scratch` step.
    fn send_replay_dgram(&mut self, to: SocketAddr, position: u64, body: &[u8]) {
        self.scratch.clear();
        self.scratch.resize(DATAGRAM_HEADER_LEN, 0);
        write_datagram_header(
            &mut self.scratch,
            &DatagramHeader {
                position,
                leadership_term_id: self.term.load(Ordering::Relaxed),
                kind: DGRAM_KIND_DATA,
                flags: 0,
                key_epoch: 0,
            },
        );
        self.scratch.extend_from_slice(body);
        if !self.seal_scratch(DGRAM_KIND_DATA) {
            return; // dropped: the follower re-NAKs, same as any lost datagram
        }
        let _ = self.sock.send_to(&self.scratch, to);
        self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        self.stats.replay_datagrams.fetch_add(1, Ordering::Relaxed);
    }
}

/// Walk the frames of an archived `block` (payload of the journal record whose
/// base stream position is `base`) starting at position `from` — a frame
/// boundary at/after `base` — grouping whole frames into MTU-`budget` runs and
/// emitting each as `(run_position, body)`. This reproduces EXACTLY the wire
/// discipline `LogBuffer::read_run_validated` produces off the live ring so the
/// journal-replay path and the ring path yield interchangeable datagrams:
///
/// - a run always carries at least one whole frame (a lone oversized frame is
///   emitted alone, as the sender's MTU assert makes impossible in practice);
/// - a run never crosses a padding frame; padding is emitted HEADER-ONLY
///   (`HEADER_LEN` bytes) and ends its run, though the walk advances the full
///   aligned span (padding fills to the ring wrap);
/// - a run is cut once the copied bytes would exceed `budget`.
///
/// Blocks are frame-aligned and CRC-validated on read, so — unlike the ring
/// path — there is no torn-frame / overwrite guard: every length read is sound.
pub(crate) fn chunk_frames(
    block: &[u8],
    base: u64,
    from: u64,
    budget: usize,
    mut emit: impl FnMut(u64, &[u8]),
) {
    debug_assert!(from >= base, "replay start must be within the block");
    let mut off = (from - base) as usize;
    while off < block.len() {
        let run_start = off;
        let run_pos = base + off as u64;
        let mut copied = 0usize;
        let mut run_end = off; // end of the bytes this run copies (may trail `off`)
        let mut bail = false;
        while off < block.len() {
            // Defense in depth (fail closed). Honest blocks are frame-aligned
            // and journal-CRC-validated, so these guards never trip on real
            // input — but a corrupt length word (or a misaligned start that
            // slipped an earlier check) must NOT drive an index past the block
            // and panic the sender agent. Bail on: not enough bytes left for a
            // header, or a frame whose aligned end overruns the block. Whatever
            // whole frames we already gathered are still emitted; the follower
            // re-NAKs and is served from the correct boundary or dropped again.
            if off + HEADER_LEN > block.len() {
                bail = true;
                break;
            }
            let hdr = read_header(&block[off..]);
            // A length below HEADER_LEN is provably corrupt. Zero is the
            // dangerous case: align_frame_len(0) == 0 advances nothing and
            // would livelock this loop forever (a silent cluster-wide wedge,
            // worse than a panic). Mirror walk_advance's guard.
            if (hdr.length as usize) < HEADER_LEN {
                bail = true;
                break;
            }
            let aligned = align_frame_len(hdr.length as usize);
            if off + aligned > block.len() {
                bail = true;
                break;
            }
            let is_padding = hdr.frame_type == FRAME_TYPE_PADDING;
            // Padding contributes only its 32-byte header to the wire (the rest
            // of its span is stale ring bytes); a message contributes its whole
            // aligned slot.
            let copy_len = if is_padding { HEADER_LEN } else { aligned };
            if copied > 0 && copied + copy_len > budget {
                break; // budget cut (the first frame of a run always fits)
            }
            off += aligned;
            run_end = run_start + copied + copy_len;
            copied += copy_len;
            if is_padding || copied >= budget {
                break; // padding ends the run at the wrap; budget ends it too
            }
        }
        if run_end > run_start {
            emit(run_pos, &block[run_start..run_end]);
        }
        if bail {
            return; // corrupt frame: stop walking (never index past the block)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use uc_log::buffer::Appender;
    use uc_log::cnc::{CncMeta, CncPage};
    use uc_log::region::Region;
    use uc_protocol::v2::crypto::{COUNTER_LEN, TAG_LEN, read_counter};
    use uc_protocol::v2::datagram::read_datagram_header;
    use uc_protocol::v2::frame::{
        FRAME_TYPE_MESSAGE, FrameHeader, HEADER_LEN, OFF_TYPE, read_header,
        write_header_except_length,
    };

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

    /// An always-on role flag for tests that don't exercise leader/follower
    /// gating (M4's node-composition role gate is now a mandatory constructor
    /// arg — every `Sender` needs one, even a standalone-leader test harness).
    fn always_leader() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(true))
    }

    struct Fake {
        sock: FaultSocket,
    }
    impl Fake {
        fn new() -> Self {
            Self {
                sock: FaultSocket::bind("127.0.0.1:0").unwrap(),
            }
        }
        fn addr(&self) -> SocketAddr {
            self.sock.local_addr().unwrap()
        }
        fn recv(&self) -> Option<(DatagramHeader, Vec<u8>)> {
            let mut buf = [0u8; 2048];
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if let Some((n, _)) = self.sock.recv_from(&mut buf).unwrap() {
                    let h = read_datagram_header(&buf).unwrap();
                    return Some((h, buf[DATAGRAM_HEADER_LEN..n].to_vec()));
                }
                std::thread::yield_now();
            }
            None
        }
        fn drain(&self) {
            let mut buf = [0u8; 2048];
            while self.sock.recv_from(&mut buf).unwrap().is_some() {}
        }
        /// M8 (Task 10): the raw wire bytes (header ++ whatever crypto did to
        /// the body), unlike `recv` which parses the header — the crypto-seam
        /// tests need to inspect/compare the actual bytes on the wire. Shorter
        /// deadline than `recv`'s: callers use this to drain "however many
        /// datagrams are queued right now" (`while let Some(d) = f.recv_raw()`),
        /// and a send always precedes the first call in the same thread, so
        /// datagrams are already queued on this loopback socket by the time
        /// this polls.
        fn recv_raw(&self) -> Option<Vec<u8>> {
            let mut buf = [0u8; 2048];
            let deadline = Instant::now() + Duration::from_millis(500);
            while Instant::now() < deadline {
                if let Some((n, _)) = self.sock.recv_from(&mut buf).unwrap() {
                    return Some(buf[..n].to_vec());
                }
                std::thread::yield_now();
            }
            None
        }
    }

    fn sender_to(followers: &[&Fake], b: &Arc<LogBuffer>) -> (Sender, mpsc::SyncSender<CtrlMsg>) {
        let (tx, rx) = mpsc::sync_channel(1024);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX; // no heartbeats: data-recv asserts must not race one
        let s = Sender::new(
            Arc::clone(b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            followers.iter().map(|f| f.addr()).collect(),
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
        );
        (s, tx)
    }

    #[test]
    fn streams_frames_to_all_followers_and_advances_sent() {
        let b = buffer();
        let (f1, f2) = (Fake::new(), Fake::new());
        let (mut s, _tx) = sender_to(&[&f1, &f2], &b);
        let mut a = Appender::new(Arc::clone(&b), 9);
        for i in 0..3 {
            a.append(4, i, &[i as u8; 64]).unwrap();
        }
        assert!(s.do_work());
        for f in [&f1, &f2] {
            let (h, body) = f.recv().expect("data datagram");
            assert_eq!(h.kind, DGRAM_KIND_DATA);
            assert_eq!(h.leadership_term_id, 9);
            assert_eq!(h.position, 0);
            assert_eq!(body.len(), 3 * 96); // all three frames packed in one datagram
            assert_eq!(read_header(&body[96..]).seq, 1);
            assert_eq!(
                &body[2 * 96 + HEADER_LEN..2 * 96 + HEADER_LEN + 64],
                &[2u8; 64]
            );
        }
        assert_eq!(b.counters().sent.load_acquire(), 3 * 96);
        assert_eq!(
            s.stats()
                .datagrams
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    /// M7: `SetPeers` swaps the fan-out + rebuilds `FlowControl` from the new
    /// membership, re-feeding each surviving address's last raw `Status` so
    /// the quorum ranking does not restart from the bootstrap window.
    #[test]
    fn set_peers_rebuilds_fanout_and_refeeds_last_status() {
        let b = buffer();
        let (f1, f2, f3) = (Fake::new(), Fake::new(), Fake::new());
        let (mut s, tx) = sender_to(&[&f1, &f2], &b); // cluster_size 3: leader + f1 + f2

        // Seed both followers' adverts before the reconfig.
        tx.send(CtrlMsg::Status {
            from: f1.addr(),
            contiguous: 1_000_000,
            window: 100_000,
        })
        .unwrap();
        tx.send(CtrlMsg::Status {
            from: f2.addr(),
            contiguous: 2_000_000,
            window: 50_000,
        })
        .unwrap();
        s.do_work();
        // cluster_size 3 -> needed = 1: the higher of the two followers.
        assert_eq!(s.flow.limit(), 2_050_000);

        // Grow the cluster: add f3 as a third voter (learners stay empty).
        tx.send(CtrlMsg::SetPeers {
            followers: vec![f1.addr(), f2.addr(), f3.addr()],
            learners: vec![],
            cluster_size: 4,
        })
        .unwrap();
        s.do_work();

        assert_eq!(s.followers.len(), 3, "fan-out grew to the new voter set");
        assert!(s.followers.contains(&f3.addr()));
        // needed = 2 now: f1's and f2's re-fed adverts must have survived the
        // rebuild (f3 has no status yet, so it sits at the bootstrap window
        // and cannot be the 2nd-highest).
        assert_eq!(
            s.flow.limit(),
            1_100_000,
            "f1's re-fed advert, not the bootstrap window"
        );
    }

    #[test]
    fn respects_flow_limit_and_resumes_on_status() {
        let b = buffer();
        let f1 = Fake::new();
        let (mut s, tx) = sender_to(&[&f1], &b);
        // shrink the follower's advertised limit to one datagram's worth
        tx.send(CtrlMsg::Status {
            from: f1.addr(),
            contiguous: 0,
            window: 96,
        })
        .unwrap();
        let mut a = Appender::new(Arc::clone(&b), 9);
        for i in 0..4 {
            a.append(4, i, &[0u8; 64]).unwrap();
        }
        s.do_work();
        let (h, body) = f1.recv().expect("first frame");
        assert_eq!((h.position, body.len()), (0, 96)); // only up to the limit
        assert!(
            s.stats()
                .flow_stalls
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0
        );
        f1.drain();
        // status advances -> the rest flows
        tx.send(CtrlMsg::Status {
            from: f1.addr(),
            contiguous: 96,
            window: 1 << 20,
        })
        .unwrap();
        s.do_work();
        let (h, body) = f1.recv().expect("remaining frames");
        assert_eq!((h.position, body.len()), (96, 3 * 96));
    }

    #[test]
    fn serves_nak_to_requester_only() {
        let b = buffer();
        let (f1, f2) = (Fake::new(), Fake::new());
        let (mut s, tx) = sender_to(&[&f1, &f2], &b);
        let mut a = Appender::new(Arc::clone(&b), 9);
        for i in 0..4 {
            a.append(4, i, &[0u8; 64]).unwrap();
        }
        s.do_work(); // steady stream to both
        f1.drain();
        f2.drain();
        tx.send(CtrlMsg::Nak {
            from: f2.addr(),
            position: 96,
            length: 192,
        })
        .unwrap();
        s.do_work();
        let (h, body) = f2.recv().expect("retransmission");
        assert_eq!(h.kind, DGRAM_KIND_DATA);
        assert_eq!(h.position, 96);
        assert!(body.len() >= 192);
        assert!(f1.recv().is_none(), "NAK service must not fan out");
        assert_eq!(
            s.stats()
                .naks_served
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn nak_from_one_follower_coalesces_to_its_latest_frontier() {
        // A follower's NAK position is its monotonic contiguous frontier, so a
        // flood of re-NAKs (the deep-replay backoff, M4) MUST collapse to one
        // queued request at the latest position — never pile 1100 redundant
        // retransmits (that self-inflicted storm throttled deep catch-up to
        // ~0.8% goodput before coalescing). Two frames are appended so the
        // coalesced NAK actually serves; its position proves it kept the latest.
        let b = buffer();
        let f1 = Fake::new();
        let (tx, rx) = mpsc::sync_channel(4096);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f1.addr()],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
        );
        let mut a = Appender::new(Arc::clone(&b), 9);
        for i in 0..2 {
            a.append(4, i, &[i as u8; 64]).unwrap(); // frames at 0 and 96
        }
        s.do_work(); // steady-stream both frames out
        f1.drain();
        // flood 1100 NAKs at ASCENDING positions; only the newest is live
        for i in 0..1100u64 {
            tx.send(CtrlMsg::Nak {
                from: f1.addr(),
                position: i * 96,
                length: 96,
            })
            .unwrap();
        }
        s.do_work(); // drains all 1100 into ONE coalesced slot, serves that slot
        assert_eq!(
            s.stats()
                .naks_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "same-follower NAKs coalesce to one slot — the cap never trips"
        );
        assert_eq!(
            s.stats()
                .naks_served
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the 1100 re-NAKs collapse to a single served request"
        );
        // The one served request was the LATEST frontier (1099*96), which is
        // beyond `append` (96*2): NotCommitted, so nothing is sent — proving the
        // slot held the newest position, not the oldest (which would have sent
        // frame 0).
        assert!(
            f1.recv().is_none(),
            "coalesced NAK kept the latest position, not the oldest"
        );
    }

    #[test]
    fn naks_from_distinct_followers_keep_separate_slots() {
        // Coalescing is PER follower — two followers each get their own queued
        // request (one serve apiece), so one node's re-NAKs never crowd out
        // another's recovery.
        let b = buffer();
        let (f1, f2) = (Fake::new(), Fake::new());
        let (mut s, tx) = sender_to(&[&f1, &f2], &b);
        let mut a = Appender::new(Arc::clone(&b), 9);
        for i in 0..4 {
            a.append(4, i, &[0u8; 64]).unwrap();
        }
        s.do_work();
        f1.drain();
        f2.drain();
        tx.send(CtrlMsg::Nak {
            from: f1.addr(),
            position: 0,
            length: 96,
        })
        .unwrap();
        tx.send(CtrlMsg::Nak {
            from: f2.addr(),
            position: 96,
            length: 96,
        })
        .unwrap();
        // two distinct slots -> two do_work cycles serve one NAK each
        s.do_work();
        s.do_work();
        assert_eq!(
            s.stats()
                .naks_served
                .load(std::sync::atomic::Ordering::Relaxed),
            2,
            "each follower's NAK is served on its own"
        );
        assert!(f1.recv().is_some(), "f1's NAK served");
        assert!(f2.recv().is_some(), "f2's NAK served");
    }

    #[test]
    fn nak_with_misaligned_position_is_rejected() {
        // A NAK whose position is not a 32-byte frame boundary can never name a
        // real frame; trusting it would drive the journal replay path to a
        // garbage length and panic the sender agent. Reject at ingestion (fail
        // closed) — count it, never queue it, send nothing to the requester.
        let b = buffer();
        let f1 = Fake::new();
        let (mut s, tx) = sender_to(&[&f1], &b);
        tx.send(CtrlMsg::Nak {
            from: f1.addr(),
            position: 100,
            length: 96,
        })
        .unwrap();
        s.do_work();
        assert_eq!(
            s.stats()
                .naks_rejected
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "misaligned NAK position must be rejected at ingestion"
        );
        assert_eq!(
            s.stats()
                .naks_served
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "a rejected NAK is never queued or served"
        );
        assert!(
            f1.recv().is_none(),
            "nothing is sent to the requester for a corrupt NAK"
        );
    }

    /// Hand-build a valid message frame of `total` bytes (header + payload,
    /// zero-filled) with correlation id `corr`.
    fn msg_frame(total: u32, corr: u32) -> Vec<u8> {
        let mut f = vec![0u8; align_frame_len(total as usize)];
        write_header_except_length(
            &mut f,
            &FrameHeader {
                length: total,
                frame_type: FRAME_TYPE_MESSAGE,
                flags: 0,
                leadership_term_id: 9,
                client_id: 0,
                seq: corr,
                time_ns: 0,
            },
        );
        f[..4].copy_from_slice(&total.to_le_bytes());
        f
    }

    #[test]
    fn chunk_frames_clamps_on_corrupt_length_word() {
        // A valid 96-byte frame at offset 0, then a GARBAGE length word at the
        // next frame boundary (offset 96) whose aligned span (~4 GiB) runs off
        // the end of a 128-byte block. chunk_frames must serve the intact frame,
        // refuse to index past the block on the corrupt one, and stop — no
        // panic, no emission whose end exceeds block.len().
        let mut block = msg_frame(96, 0);
        let mut garbage = vec![0u8; HEADER_LEN];
        garbage[..4].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        garbage[OFF_TYPE] = FRAME_TYPE_MESSAGE;
        block.extend_from_slice(&garbage); // 128 bytes; frame at 96 claims ~4 GiB
        let blen = block.len();

        // Walk from the start: the good frame is served, the corrupt one clamps.
        let mut emissions: Vec<(u64, usize)> = Vec::new();
        chunk_frames(&block, 0, 0, 65_536, |pos, body| {
            assert!(body.len() <= blen, "emission ran past the block");
            emissions.push((pos, body.len()));
        });
        assert_eq!(
            emissions,
            vec![(0, 96)],
            "only the intact frame is emitted; the corrupt length is clamped"
        );

        // Walk FROM the corrupt boundary directly: bail immediately, emit nothing.
        let mut count = 0usize;
        chunk_frames(&block, 0, 96, 65_536, |_pos, body| {
            assert!(body.len() <= blen);
            count += 1;
        });
        assert_eq!(
            count, 0,
            "starting on a corrupt frame emits nothing and does not panic"
        );

        // A ZERO length word (the re-review's livelock case): align_frame_len(0)
        // == 0 advances nothing, so without the below-HEADER_LEN bail the gather
        // loop would spin at the same offset forever — a silent sender-agent
        // wedge, worse than a panic. This test TERMINATING is the assertion;
        // the intact frame before the zero word is still served.
        let mut block = msg_frame(96, 0);
        let mut zeroed = vec![0u8; HEADER_LEN];
        zeroed[OFF_TYPE] = FRAME_TYPE_MESSAGE; // length word stays 0
        block.extend_from_slice(&zeroed);
        block.extend_from_slice(&msg_frame(96, 1)); // a frame BEYOND the corruption
        let blen = block.len();
        let mut emissions: Vec<(u64, usize)> = Vec::new();
        chunk_frames(&block, 0, 0, 65_536, |pos, body| {
            assert!(body.len() <= blen);
            emissions.push((pos, body.len()));
        });
        assert_eq!(
            emissions,
            vec![(0, 96)],
            "zero-length word: prior frames served, walk terminates, nothing beyond"
        );
        // and starting exactly ON the zero word: terminate with no emission
        let mut count = 0usize;
        chunk_frames(&block, 0, 96, 65_536, |_pos, _body| count += 1);
        assert_eq!(count, 0);
    }

    #[test]
    fn nak_queue_caps_across_distinct_sources() {
        // Coalescing keys per source, so distinct source addresses each claim
        // their own slot — the live flood-guard the M2 FIFO test covered (a
        // single follower can no longer overflow the cap, but a many-source
        // spoofed flood still must). NAK_QUEUE_MAX+K requests from that many
        // distinct addrs in ONE drain fill the cap and drop exactly the K
        // oldest; the queue keeps serving afterwards.
        const K: usize = 8;
        let b = buffer();
        let f1 = Fake::new();
        let (tx, rx) = mpsc::sync_channel(NAK_QUEUE_MAX + 64);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f1.addr()],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
        );
        for i in 0..(NAK_QUEUE_MAX + K) as u16 {
            // distinct sources; position 0 is frame-aligned (never rejected)
            let from = SocketAddr::from(([127, 0, 0, 1], 20_000 + i));
            tx.send(CtrlMsg::Nak {
                from,
                position: 0,
                length: 96,
            })
            .unwrap();
        }
        s.do_work(); // drains all NAK_QUEUE_MAX+K into the queue, serves one
        assert_eq!(
            s.stats()
                .naks_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
            K as u64,
            "exactly the K over-cap distinct-source NAKs drop"
        );
        assert_eq!(
            s.stats()
                .naks_rejected
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "aligned positions are not rejected"
        );
        let served = s
            .stats()
            .naks_served
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(served, 1, "the drain cycle served one queued NAK");
        s.do_work();
        assert!(
            s.stats()
                .naks_served
                .load(std::sync::atomic::Ordering::Relaxed)
                > served,
            "the capped queue keeps serving on later cycles"
        );
    }

    #[test]
    fn heartbeats_carry_append_position() {
        let b = buffer();
        let f1 = Fake::new();
        let (tx, rx) = mpsc::sync_channel(16);
        let _ = tx; // no control traffic in this test
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = 1; // fire every cycle
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f1.addr()],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
        );
        let mut a = Appender::new(Arc::clone(&b), 9);
        a.append(4, 0, &[0u8; 64]).unwrap();
        s.do_work();
        // first datagram is the data; a heartbeat follows within the cycle(s)
        let mut saw_heartbeat = false;
        for _ in 0..3 {
            s.do_work();
            while let Some((h, _)) = f1.recv() {
                if h.kind == DGRAM_KIND_HEARTBEAT {
                    assert_eq!(h.position, 96);
                    assert_eq!(h.leadership_term_id, 9);
                    saw_heartbeat = true;
                }
                if saw_heartbeat {
                    break;
                }
            }
            if saw_heartbeat {
                break;
            }
        }
        assert!(saw_heartbeat);
    }

    #[test]
    fn journal_replay_serves_deep_nak_with_identical_wire_format() {
        // leader with a TINY buffer (4096) laps it 3x while archiving; a NAK
        // for lap-0 positions must be served from the journal
        let b = Arc::new(LogBuffer::new(
            uc_log::region::Region::heap_zeroed(4096),
            test_cnc(4096),
            256,
        ));
        let dir = tempfile::tempdir().unwrap();
        let cfg = uc_log::archive::ArchiveConfig {
            segment_size_bytes: 4 * 1024 * 1024,
            ..uc_log::archive::ArchiveConfig::new(dir.path())
        };
        let mut arch = uc_log::archive::Archive::open(cfg).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 9);
        let mut n = 0u32;
        while a.position() < 3 * 4096 {
            match a.append(1, n, &[n as u8; 64]) {
                Ok(_) => n += 1,
                Err(uc_log::buffer::AppendError::WouldOverrun) => {
                    arch.do_work(&b).unwrap();
                }
                Err(e) => panic!("{e}"),
            }
        }
        while arch.do_work(&b).unwrap() {}

        let f1 = Fake::new();
        let (tx, rx) = mpsc::sync_channel(64);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f1.addr()],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
        );
        s.set_replay_source(arch.journal_arc());
        // NAK for position 0 (lapped long ago)
        tx.send(CtrlMsg::Nak {
            from: f1.addr(),
            position: 0,
            length: 4096,
        })
        .unwrap();
        s.do_work();
        // served from the journal: DATA datagrams, self-locating from 0,
        // frames byte-identical to the original appends
        let (h, body) = f1.recv().expect("replayed datagram");
        assert_eq!(h.kind, DGRAM_KIND_DATA);
        assert_eq!(h.position, 0);
        assert_eq!(read_header(&body).seq, 0);
        assert_eq!(&body[HEADER_LEN..HEADER_LEN + 64], &[0u8; 64]);
        assert!(
            s.stats()
                .replay_datagrams
                .load(std::sync::atomic::Ordering::Relaxed)
                >= 1
        );
        assert_eq!(
            s.stats()
                .overruns
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "the seam is now served, not counted"
        );
    }

    // ----------------------------------------------------------- M4 (Task 7)

    /// Item-6 verify: a DATA datagram's header term comes from the SENDER's
    /// TermHandle, while the FRAME header term comes from the APPENDER — two
    /// different actors, so they CAN legitimately differ transiently after a
    /// term bump. Safety (the archive term-observation scan, Task 7 item 5)
    /// reads FRAME terms; the datagram term is only the liveness/filter check.
    #[test]
    fn datagram_term_from_handle_frame_term_from_appender() {
        use std::sync::atomic::Ordering::Relaxed;
        let b = buffer();
        let f1 = Fake::new();
        let (tx, rx) = mpsc::sync_channel(16);
        let _ = tx;
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        let handle = term_handle(9);
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f1.addr()],
            3,
            rx,
            cfg,
            Arc::clone(&handle),
            always_leader(),
        );
        // the appender stamps its frames with term 7 (its own leadership term)
        let mut a = Appender::new(Arc::clone(&b), 7);
        a.append(4, 0, &[0u8; 64]).unwrap();
        // the consensus agent bumps the sender's handle to 8 before the send:
        // the DATA datagram must carry 8 while the frame inside still carries 7
        handle.store(8, Relaxed);
        s.do_work();
        let (h, body) = f1.recv().expect("data datagram");
        assert_eq!(h.kind, DGRAM_KIND_DATA);
        assert_eq!(
            h.leadership_term_id, 8,
            "datagram term must come from the sender handle"
        );
        assert_eq!(
            read_header(&body).leadership_term_id,
            7,
            "frame term must come from the appender (may differ from the datagram term)"
        );
    }

    /// Role-flag gating (M4 node composition): a sender whose role flag reads
    /// `false` produces NO leader output — even with appended data and an
    /// elapsed heartbeat interval it streams nothing, beats nothing, and does
    /// not advance `sent`. Flipping the flag `true` resumes streaming at once.
    #[test]
    fn role_flag_gates_streaming_and_heartbeats() {
        use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
        let b = buffer();
        let f1 = Fake::new();
        let (tx, rx) = mpsc::sync_channel(16);
        let _ = tx; // no control traffic
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = 1; // would fire every cycle if this node were leader
        let flag = Arc::new(AtomicBool::new(false)); // FOLLOWER
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f1.addr()],
            3,
            rx,
            cfg,
            term_handle(9),
            Arc::clone(&flag),
        );
        let mut a = Appender::new(Arc::clone(&b), 9);
        for i in 0..3 {
            a.append(4, i, &[0u8; 64]).unwrap();
        }
        for _ in 0..8 {
            s.do_work();
        }
        assert!(
            f1.recv().is_none(),
            "a follower-role sender must emit nothing"
        );
        assert_eq!(s.stats().datagrams.load(Relaxed), 0);
        assert_eq!(s.stats().heartbeats.load(Relaxed), 0);
        assert_eq!(
            b.counters().sent.load_acquire(),
            0,
            "follower role advanced sent"
        );
        // promote to leader: the pending frames now stream out
        flag.store(true, Relaxed);
        s.do_work();
        let (h, body) = f1.recv().expect("leader role must stream the pending data");
        assert_eq!(h.kind, DGRAM_KIND_DATA);
        assert_eq!(body.len(), 3 * 96);
        assert_eq!(b.counters().sent.load_acquire(), 3 * 96);
    }

    // ----------------------------------------------------------- M8 (Task 10)
    // The send seam: `assemble` seals once (Group scope — DATA/HEARTBEAT are
    // the only kinds this module ever assembles), `fan_out`'s loop still sends
    // the identical sealed bytes N times. See sender.rs's module docs and
    // `SenderConfig::crypto_enabled` for how the MTU budget stays in sync.

    #[cfg(test)]
    impl Sender {
        /// Test-only: inject a NAK directly into the queue, bypassing the
        /// ctrl channel (`sender_with_crypto_*` helpers below hand back no
        /// `Sender`-side channel handle) — the next `do_work()` serves it via
        /// the exact same `serve_nak` path a real `CtrlMsg::Nak` would.
        fn on_nak(&mut self, from: SocketAddr, position: u64, length: u32) {
            self.naks.push_back((from, position, length));
        }
    }

    /// A well-formed `Enabled` `uc_crypto::SharedTransport` with a fresh key
    /// file + an empty allowlist (same fixture discipline as
    /// `uc_crypto::transport::tests::node_transport`: real key material
    /// under `CARGO_TARGET_TMPDIR`, never `/tmp` — see CLAUDE.md).
    /// `mint_group_key(&[], 0)` mints with a vacuous peer set, which
    /// activates immediately (`all()` over an empty set), so the `SendHalf`
    /// this returns can seal `DGRAM_KIND_DATA`/`DGRAM_KIND_HEARTBEAT` right
    /// away — no handshake or ack needed for a sender-only unit test that
    /// never opens on the other end.
    ///
    /// Mints TWICE, not once: `GroupPlane::next_epoch` starts at 0 on a fresh
    /// process, so a single mint's epoch is 0 — indistinguishable from
    /// `assemble`'s zero-initialized `key_epoch` header field, which would
    /// make a stamp-then-fail-to-stamp mutant invisible to any test that
    /// checks `key_epoch != 0`. `uc_crypto::transport`'s own
    /// `seal_group_stamps_the_chosen_epoch_into_the_header` test names this
    /// exact trap and fixes it the same way (mint twice so the epoch under
    /// test is provably not the field's zero-init default). First caught
    /// here by `sealed_fan_out_seals_once_and_sends_identical_bytes_to_every_follower`
    /// actually failing red against a real (if accidental) epoch-0 fixture —
    /// not a hypothetical.
    ///
    /// Returns a `SendHalf`, not the `SharedTransport` itself — `Sender`
    /// (review round 1, 2026-07-29) owns only the send half, never a whole
    /// `Transport`/`SharedTransport`; letting the `SharedTransport` this
    /// function built go out of scope is fine, since `SendHalf` holds its
    /// own `Arc` clone of the shared key state, keeping it alive.
    fn crypto_transport(self_id: u32) -> SendHalf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_net_tests")
            })
            .join("uc2-net-sender-crypto")
            .join(format!("t{seq}"));
        std::fs::create_dir_all(&dir).unwrap();
        assert!(
            !dir.starts_with("/tmp"),
            "test scratch must not live on tmpfs: {dir:?}"
        );

        let key_path = dir.join("node.key");
        std::fs::write(&key_path, [0x77u8; 32]).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let allow_path = dir.join("allowlist");
        std::fs::write(&allow_path, "").unwrap();

        let cfg = uc_crypto::CryptoConfig::Enabled {
            key_path,
            allowlist_path: allow_path,
            rotation: uc_crypto::rotation::RotationPolicy::default(),
        };
        let shared = uc_crypto::SharedTransport::new(&cfg, self_id)
            .unwrap()
            .unwrap();
        shared.mint_group_key(&[], 0); // epoch 0 — indistinguishable from the unsealed default
        shared.mint_group_key(&[], 0); // epoch 1 — provably not zero-init
        shared.send_half()
    }

    /// `n` fresh followers, a `Sender` with crypto enabled fanning out to all
    /// of them, plus its buffer (tests append through it directly).
    fn sender_with_crypto_n(n: usize) -> (Sender, Vec<Fake>) {
        let b = buffer();
        let followers: Vec<Fake> = (0..n).map(|_| Fake::new()).collect();
        let (_tx, rx) = mpsc::sync_channel(16);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX; // no heartbeats racing the data-recv asserts
        cfg.crypto_enabled = true;
        let crypto = crypto_transport(1);
        let s = Sender::with_crypto(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            followers.iter().map(|f| f.addr()).collect(),
            &[],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
            Some(group_only_crypto(crypto)),
        );
        (s, followers)
    }

    fn sender_with_crypto_to_two_followers() -> (Sender, Fake, Fake) {
        let (s, mut fs) = sender_with_crypto_n(2);
        let f2 = fs.pop().unwrap();
        let f1 = fs.pop().unwrap();
        (s, f1, f2)
    }

    fn sender_with_crypto_to_one_follower() -> (Sender, Fake) {
        let (s, mut fs) = sender_with_crypto_n(1);
        (s, fs.pop().unwrap())
    }

    /// Bundles a `SendHalf` with an EMPTY [`PeerIds`] map — enough for every
    /// pre-T17 crypto test, all of which exercise `Scope::Group` kinds only
    /// (the group key is the same for every destination, so no `NodeId`
    /// resolution happens on those paths at all). T17's pairwise tests build
    /// a real map instead.
    fn group_only_crypto(half: SendHalf) -> SenderCrypto {
        SenderCrypto {
            half,
            peer_ids: PeerIds::new(),
        }
    }

    fn sender_without_crypto() -> (Sender, Fake) {
        let b = buffer();
        let f = Fake::new();
        let (_tx, rx) = mpsc::sync_channel(16);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        let s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f.addr()],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
        );
        (s, f)
    }

    /// Appends `payload` (chunked to the buffer's `max_payload` if it doesn't
    /// fit one frame) and drains it out via `do_work` — real frames through
    /// the real ring, so a NAK re-read of the same position (the retransmit
    /// test) sees the same committed bytes the first send did.
    fn append_and_flush(s: &mut Sender, payload: &[u8]) {
        let mut a = Appender::new(Arc::clone(&s.buffer), 9);
        let max = s.buffer.max_payload();
        for chunk in payload.chunks(max) {
            loop {
                match a.append(4, 0, chunk) {
                    Ok(_) => break,
                    Err(uc_log::buffer::AppendError::WouldOverrun) => {
                        s.do_work(); // drain some backlog, then retry the append
                    }
                    Err(e) => panic!("append_and_flush: {e}"),
                }
            }
        }
        s.do_work();
    }

    #[test]
    fn sealed_fan_out_seals_once_and_sends_identical_bytes_to_every_follower() {
        // The whole point of the group key: one seal, N sends. If this regresses to
        // per-peer sealing the datagrams would differ.
        let (mut s, f1, f2) = sender_with_crypto_to_two_followers();
        append_and_flush(&mut s, b"hello");
        let d1 = f1.recv_raw().expect("follower 1 got a datagram");
        let d2 = f2.recv_raw().expect("follower 2 got a datagram");
        assert_eq!(d1, d2, "byte-identical: sealed once, fanned out");

        // "payload is not cleartext": compare against what an UNSEALED sender
        // produces for the IDENTICAL payload, at the SAME length. Comparing
        // against the raw literal b"hello" would be vacuous either way — the
        // frame layer always wraps a 32-byte header even in cleartext mode
        // (see cleartext_mode_is_byte_identical_to_pre_m8_output below), so
        // the body never equals the bare payload regardless of whether
        // sealing actually ran. This is the same fix seal.rs's own
        // `payload_ciphertext_region_is_actually_sealed` applied to the
        // mandated T5 test for the identical reason.
        let (mut plain, pf) = sender_without_crypto();
        append_and_flush(&mut plain, b"hello");
        let plain_d = pf.recv_raw().expect("cleartext datagram");

        assert_eq!(
            d1.len(),
            plain_d.len() + CRYPTO_OVERHEAD,
            "sealing adds exactly the counter+tag overhead, nothing else"
        );
        let ct_start = DATAGRAM_HEADER_LEN + COUNTER_LEN;
        let ct_end = d1.len() - TAG_LEN;
        assert_ne!(
            &d1[ct_start..ct_end],
            &plain_d[DATAGRAM_HEADER_LEN..],
            "payload is not cleartext"
        );
        assert_ne!(
            read_datagram_header(&d1).unwrap().key_epoch,
            0,
            "stamped with the epoch"
        );
    }

    #[test]
    fn mtu_budget_shrinks_by_the_crypto_overhead_so_sealed_datagrams_still_fit() {
        let cfg_plain = SenderConfig::new(9);
        let mut cfg_sealed = SenderConfig::new(9);
        cfg_sealed.crypto_enabled = true;
        assert_eq!(
            cfg_sealed.mtu - DATAGRAM_HEADER_LEN - cfg_sealed.crypto_overhead(),
            cfg_plain.mtu - DATAGRAM_HEADER_LEN - CRYPTO_OVERHEAD
        );
        let (mut s, f) = sender_with_crypto_to_one_follower();
        // MANY MINIMAL (empty-payload, 32-byte-aligned) frames, not one big
        // chunked payload: `read_run_validated` packs whole frames into a run
        // up to `budget`, so coarse frames (e.g. 288 bytes, this buffer's
        // default max_payload) leave slack on both sides of the 24-byte
        // crypto overhead and can pack IDENTICALLY whether or not the
        // subtraction happened — never actually exercising the boundary this
        // test exists to pin (confirmed: the first draft of this test, using
        // a single 4096-byte payload, passed against a mutant that dropped
        // the `- self.cfg.crypto_overhead()` term entirely; see the task
        // report's mutation section). 32-byte frames make the packed run
        // size land exactly where 24 bytes is the difference between fitting
        // and not: budget 1368 (correct) packs 42 frames = 1344B body
        // (sealed 1384B, fits); budget 1392 (missing the subtraction) packs
        // 43 = 1376B body (sealed 1416B > the 1408B MTU).
        let mut a = Appender::new(Arc::clone(&s.buffer), 9);
        for i in 0..64u32 {
            a.append(4, i, &[]).unwrap();
        }
        s.do_work();
        let mut saw_any = false;
        while let Some(d) = f.recv_raw() {
            saw_any = true;
            assert!(
                d.len() <= cfg_sealed.mtu,
                "a sealed datagram must not exceed the MTU"
            );
        }
        assert!(
            saw_any,
            "fixture must actually produce datagrams for this to mean anything"
        );
    }

    #[test]
    fn serve_nak_mtu_budget_also_shrinks_by_the_crypto_overhead() {
        // The task brief names TWO known budget sites: the run-read budget in
        // `do_work` (pinned above) and `serve_nak`'s own budget — a mutant
        // that fixes only one would still pass a suite that only exercises
        // the live-stream path. Same fine-grained-frame construction as
        // above (needed for the same reason: coarse frames never straddle
        // the 24-byte boundary).
        let (mut s, f) = sender_with_crypto_to_one_follower();
        let mut a = Appender::new(Arc::clone(&s.buffer), 9);
        for i in 0..64u32 {
            a.append(4, i, &[]).unwrap();
        }
        s.do_work();
        f.drain(); // discard the live-stream copies; only the NAK-served retransmit matters here
        s.on_nak(f.addr(), 0, 64 * 32);
        s.do_work();
        let mut saw_any = false;
        while let Some(d) = f.recv_raw() {
            saw_any = true;
            assert!(
                d.len() <= s.cfg.mtu,
                "a NAK-served sealed datagram must not exceed the MTU"
            );
        }
        assert!(
            saw_any,
            "fixture must actually produce datagrams for this to mean anything"
        );
    }

    #[test]
    fn a_nak_retransmit_reuses_the_position_but_never_the_counter() {
        // The nonce hazard, pinned at the seam: position repeats, counters must not.
        let (mut s, f) = sender_with_crypto_to_one_follower();
        append_and_flush(&mut s, b"payload");
        let first = f.recv_raw().unwrap();
        s.on_nak(f.addr(), read_datagram_header(&first).unwrap().position, 7);
        s.do_work();
        let retx = f.recv_raw().unwrap();
        assert_eq!(
            read_datagram_header(&retx).unwrap().position,
            read_datagram_header(&first).unwrap().position
        );
        assert_ne!(
            read_counter(&retx[DATAGRAM_HEADER_LEN..]),
            read_counter(&first[DATAGRAM_HEADER_LEN..]),
            "a repeated position must not mean a repeated nonce"
        );
    }

    #[test]
    fn cleartext_mode_is_byte_identical_to_pre_m8_output() {
        // Flag-day safety: with crypto off, nothing on the wire changes — the
        // datagram is exactly DATAGRAM_HEADER_LEN + the frame layer's own
        // bytes (no crypto suffix), and the payload is readable in the clear
        // at its normal frame offset.
        let (mut s, f) = sender_without_crypto();
        append_and_flush(&mut s, b"hello");
        let d = f.recv_raw().unwrap();
        assert_eq!(
            d.len(),
            DATAGRAM_HEADER_LEN + align_frame_len(HEADER_LEN + 5),
            "no crypto overhead: exactly the datagram header + one aligned frame"
        );
        assert_eq!(
            &d[DATAGRAM_HEADER_LEN + HEADER_LEN..DATAGRAM_HEADER_LEN + HEADER_LEN + 5],
            b"hello"
        );
        assert_eq!(read_datagram_header(&d).unwrap().key_epoch, 0);
    }

    // ---- Mutation-testing companions (task instructions: "write two or
    // three wrong implementations ... check whether the suite catches
    // them"). These tests exist to KILL specific mutants; see the task
    // report for the red-then-green transcripts proving each one actually
    // fails against the mutant it targets.

    #[test]
    fn heartbeats_are_sealed_too_not_just_data() {
        // Every mandated test above drives DATA through fan_out. HEARTBEAT is
        // the other Group-scope kind assemble() ever builds (do_work's
        // heartbeat block) — a mutant that seals only inside fan_out (instead
        // of centrally in assemble/seal_scratch) would pass all four mandated
        // tests while leaving every HEARTBEAT cleartext.
        let (mut s, f) = sender_with_crypto_to_one_follower();
        s.cfg.heartbeat_ns = 1; // fire on the very next do_work
        s.do_work();
        let d = f.recv_raw().expect("heartbeat datagram");
        assert_eq!(read_datagram_header(&d).unwrap().kind, DGRAM_KIND_HEARTBEAT);
        assert_ne!(
            read_datagram_header(&d).unwrap().key_epoch,
            0,
            "heartbeat must be sealed too"
        );
        assert_eq!(
            d.len(),
            DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD,
            "empty-body heartbeat, sealed: header + counter + tag, nothing else"
        );
    }

    #[test]
    fn a_journal_replayed_nak_is_sealed_exactly_like_a_ring_served_one() {
        // Pins the "serve_nak and send_replay_dgram also build DATA
        // datagrams; both take the group key" requirement from the task
        // brief. A mutant that seals only inside assemble() (which
        // send_replay_dgram does NOT call — it builds scratch inline) would
        // pass every mandated test, since none of them lap the ring, and
        // would ship deep-NAK catch-up permanently cleartext even with
        // crypto on everywhere else — worse, the receiver (T11) would then
        // see a cleartext DATA where it expects a sealed one and reject it
        // as `peer_appears_cleartext`, silently wedging deep recovery.
        let b = Arc::new(LogBuffer::new(
            Region::heap_zeroed(4096),
            test_cnc(4096),
            256,
        ));
        // tempfile::tempdir() (not a fixed path): matches
        // journal_replay_serves_deep_nak_with_identical_wire_format above —
        // a fresh, auto-cleaned, guaranteed-unique dir per run, unlike the
        // key/allowlist fixtures (crypto_transport) which reuse the
        // CARGO_TARGET_TMPDIR discipline because they're a handful of bytes,
        // not journal segments.
        let dir = tempfile::tempdir().unwrap();
        let acfg = uc_log::archive::ArchiveConfig {
            segment_size_bytes: 4 * 1024 * 1024,
            ..uc_log::archive::ArchiveConfig::new(dir.path())
        };
        let mut arch = uc_log::archive::Archive::open(acfg).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 9);
        let mut n = 0u32;
        while a.position() < 3 * 4096 {
            match a.append(1, n, &[n as u8; 64]) {
                Ok(_) => n += 1,
                Err(uc_log::buffer::AppendError::WouldOverrun) => {
                    arch.do_work(&b).unwrap();
                }
                Err(e) => panic!("{e}"),
            }
        }
        while arch.do_work(&b).unwrap() {}

        let f1 = Fake::new();
        let (_tx, rx) = mpsc::sync_channel(64);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        cfg.crypto_enabled = true;
        let mut s = Sender::with_crypto(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f1.addr()],
            &[],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
            Some(group_only_crypto(crypto_transport(1))),
        );
        s.set_replay_source(arch.journal_arc());
        // NAK for position 0 (lapped long ago -> served from the journal)
        s.on_nak(f1.addr(), 0, 4096);
        s.do_work();
        let d = f1.recv_raw().expect("replayed datagram");
        let h = read_datagram_header(&d).unwrap();
        assert_eq!(h.kind, DGRAM_KIND_DATA);
        assert_eq!(h.position, 0);
        assert_ne!(h.key_epoch, 0, "journal-replayed DATA must be sealed too");
        assert!(
            s.stats().replay_datagrams.load(Ordering::Relaxed) >= 1,
            "fixture must actually exercise the journal-replay path"
        );
    }

    #[test]
    fn journal_replayed_nak_mtu_budget_also_shrinks_by_the_crypto_overhead() {
        // Review round 1 (2026-07-29) finding: serve_nak_from_journal's OWN
        // budget line (the third MTU-budget site, alongside do_work's
        // run-read budget and serve_nak's — this one added by this task
        // itself) had NO size assertion anywhere. The test above
        // (`a_journal_replayed_nak_is_sealed_exactly_like_a_ring_served_one`)
        // asserts the datagram is SEALED but never asserts its SIZE, and its
        // 64-byte payloads make 96-byte frames — at the default 1408 MTU,
        // floor(1368/96) == floor(1392/96) == 14, the SAME coarse-granularity
        // blindness fixed twice already at the other two budget sites.
        // Reproduced instead at cfg.mtu = 1360 (the reviewer's own repro
        // number): floor(1320/96) = 13 correct frames (1360-16-24-... =>
        // sealed 16+13*96+24=1288, fits) vs floor(1344/96) = 14 buggy frames
        // (sealed 16+14*96+24=1384 > 1360) — reverting only this site's
        // `- self.cfg.crypto_overhead()` panics here with exactly that
        // "1384 > mtu" overrun.
        let b = Arc::new(LogBuffer::new(
            Region::heap_zeroed(4096),
            test_cnc(4096),
            256,
        ));
        let dir = tempfile::tempdir().unwrap();
        let acfg = uc_log::archive::ArchiveConfig {
            segment_size_bytes: 4 * 1024 * 1024,
            ..uc_log::archive::ArchiveConfig::new(dir.path())
        };
        let mut arch = uc_log::archive::Archive::open(acfg).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 9);
        let mut n = 0u32;
        while a.position() < 3 * 4096 {
            match a.append(1, n, &[n as u8; 64]) {
                Ok(_) => n += 1,
                Err(uc_log::buffer::AppendError::WouldOverrun) => {
                    arch.do_work(&b).unwrap();
                }
                Err(e) => panic!("{e}"),
            }
        }
        while arch.do_work(&b).unwrap() {}

        let f1 = Fake::new();
        let (_tx, rx) = mpsc::sync_channel(64);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        cfg.crypto_enabled = true;
        cfg.mtu = 1360; // the exact boundary the review reproduced the overrun at
        let mut s = Sender::with_crypto(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f1.addr()],
            &[],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
            Some(group_only_crypto(crypto_transport(1))),
        );
        s.set_replay_source(arch.journal_arc());
        s.on_nak(f1.addr(), 0, 4096);
        s.do_work();
        let mut saw_any = false;
        while let Some(d) = f1.recv_raw() {
            saw_any = true;
            assert!(
                d.len() <= s.cfg.mtu,
                "a journal-replayed sealed datagram must not exceed the MTU"
            );
        }
        assert!(
            saw_any,
            "fixture must actually produce datagrams for this to mean anything"
        );
        assert!(
            s.stats().replay_datagrams.load(Ordering::Relaxed) >= 1,
            "fixture must actually exercise the journal-replay path"
        );
    }

    #[test]
    fn a_failed_group_seal_drops_the_datagram_rather_than_sending_it_half_built() {
        // Mutant target: sealing that mutates self.scratch before the key
        // lookup can fail (the exact class transport.rs's own review round 1
        // caught, F3) would ship a corrupted or partially-sealed datagram
        // instead of dropping it. Construct a Sender with crypto ENABLED but
        // whose SharedTransport never minted a group key — every seal
        // attempt must fail closed (NoGroupKey), and NOTHING must reach the
        // wire.
        let b = buffer();
        let f = Fake::new();
        let (_tx, rx) = mpsc::sync_channel(16);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        cfg.crypto_enabled = true;
        let mut s = Sender::with_crypto(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f.addr()],
            &[],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
            Some(group_only_crypto(unminted_crypto_send_half(1))),
        );
        append_and_flush(&mut s, b"hello");
        assert!(
            f.recv_raw().is_none(),
            "an unsealable datagram must never reach the wire"
        );
        assert!(
            s.stats().seal_failures.load(Ordering::Relaxed) > 0,
            "the failure must be counted, not silently swallowed"
        );
        assert_eq!(
            b.counters().sent.load_acquire(),
            align_frame_len(HEADER_LEN + 5) as u64,
            "the buffer cursor still advances (a seal failure is fire-and-forget \
             packet loss from the ring's point of view, same as a lost UDP \
             datagram — NAK repair recovers it, not a stalled cursor)"
        );
    }

    /// An `Enabled` `SendHalf` from a `SharedTransport` that NEVER minted a
    /// group key — every `Scope::Group` seal through it fails closed with
    /// `NoGroupKey`. Factored out of
    /// `a_failed_group_seal_drops_the_datagram_rather_than_sending_it_half_built`
    /// so `a_persistent_seal_failure_is_visible_in_the_cnc_band` (review
    /// round 1) can reuse the identical fixture rather than a third copy of
    /// the key/allowlist boilerplate.
    fn unminted_crypto_send_half(self_id: u32) -> SendHalf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_net_tests")
            })
            .join("uc2-net-sender-crypto-no-mint")
            .join(format!("t{seq}"));
        std::fs::create_dir_all(&dir).unwrap();
        let key_path = dir.join("node.key");
        std::fs::write(&key_path, [0x99u8; 32]).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let allow_path = dir.join("allowlist");
        std::fs::write(&allow_path, "").unwrap();
        let ccfg = uc_crypto::CryptoConfig::Enabled {
            key_path,
            allowlist_path: allow_path,
            rotation: uc_crypto::rotation::RotationPolicy::default(),
        };
        let shared = uc_crypto::SharedTransport::new(&ccfg, self_id)
            .unwrap()
            .unwrap(); // NOT minted
        shared.send_half()
    }

    #[test]
    fn a_persistent_seal_failure_is_visible_in_the_cnc_band() {
        // Review round 1 (2026-07-29), Minor: `seal_failures` was stats-only
        // (an `AtomicU64` invisible outside this process). A PERSISTENT
        // failure (this fixture: crypto on, no group key ever minted) drops
        // DATA *and* HEARTBEAT silently — exactly the condition an operator
        // must be able to see externally, the same way `advertised_limit`
        // already is via `set_peer_slots`/`refresh_peer_obs`.
        let b = buffer();
        let f = Fake::new();
        let (_tx, rx) = mpsc::sync_channel(16);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        cfg.crypto_enabled = true;
        let mut s = Sender::with_crypto(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f.addr()],
            &[],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
            Some(group_only_crypto(unminted_crypto_send_half(1))),
        );
        let cnc = test_cnc(1 << 16);
        s.set_peer_slots(Arc::clone(&cnc), vec![]);
        assert_eq!(cnc.seal_failures(), 0, "nothing has failed yet");
        append_and_flush(&mut s, b"hello");
        assert!(
            cnc.seal_failures() > 0,
            "a persistent seal failure must be visible in the cnc band, not just in-process stats"
        );
        assert_eq!(
            cnc.seal_failures(),
            s.stats().seal_failures.load(Ordering::Relaxed),
            "the cnc mirror must agree with the in-process counter it mirrors"
        );
    }

    // ======================================================================
    // M8 Task 17: the snapshot session's pairwise sends
    // ======================================================================
    //
    // T10 left `assemble_snap` cleartext because pairwise sealing needs an
    // ESTABLISHED handshake session and nothing drove `Peers` until T12.
    // `send_snap_chunk` ships the raw bytes of the service-built snapshot
    // artifact — the complete serialized state machine, with the file offset
    // in the header — and `send_snap_begin` ships `SnapBeginBody.config`, the
    // encoded cluster `ConfigRecord`, straight into the receiving node's
    // `maybe_adopt_incoming_snapshot`. Unsealed means UNAUTHENTICATED: an
    // on-path attacker forges a session and installs attacker-chosen
    // application state AND attacker-chosen membership.

    const T17_LEADER_ID: uc_crypto::NodeId = 1;
    const T17_PEER_ID: uc_crypto::NodeId = 2;
    const T17_PRIV_LEADER: [u8; 32] = [0x41; 32];
    const T17_PRIV_PEER: [u8; 32] = [0x42; 32];
    /// Deliberately bigger than one MTU-worth of chunk, so `send_snap_chunk`'s
    /// `want` is capped by the MTU budget (the term under test) rather than by
    /// the remaining file length.
    const T17_SNAP_LEN: usize = 8 * 1024;

    /// The snapshot artifact's bytes — a recognizable, non-repeating pattern
    /// so "is this on the wire in the clear?" is a real question.
    fn t17_snapshot_bytes() -> Vec<u8> {
        (0..T17_SNAP_LEN)
            .map(|i| (i.wrapping_mul(31).wrapping_add(7)) as u8)
            .collect()
    }

    /// The `ConfigRecord` bytes `SnapBeginBody.config` carries — the
    /// integrity half of this task.
    fn t17_config_bytes() -> Vec<u8> {
        b"CLUSTER-MEMBERSHIP-RECORD".to_vec()
    }

    /// A nonzero placeholder identity hash per set bit of `mask` — enough for
    /// `identity_mask(&ident(mask)) == mask` to hold, which is all these
    /// tests need (they exercise artifact/mask covering, not real names).
    fn ident(mask: u64) -> [u64; 8] {
        let mut out = [0u64; 8];
        for (i, h) in out.iter_mut().enumerate() {
            if mask & (1 << i) != 0 {
                *h = 0xF00D_0000_0000_0000 | (i as u64 + 1);
            }
        }
        out
    }

    /// A `Sender` with crypto on and a REAL established pairwise session with
    /// its one follower, plus a real snapshot file wired as the source.
    /// Returns the sender, the follower endpoint, the follower's
    /// `SharedTransport` (to open what the sender sealed), and the tempdir
    /// that owns the snapshot file.
    fn sender_with_crypto_and_established_session(
        tag: &str,
    ) -> (Sender, Fake, uc_crypto::SharedTransport, tempfile::TempDir) {
        use crate::crypto_testkit as tk;
        let leader_pub = tk::identity_public(&format!("{tag}-lpub"), T17_PRIV_LEADER);
        let peer_pub = tk::identity_public(&format!("{tag}-ppub"), T17_PRIV_PEER);
        let allow = [(T17_LEADER_ID, leader_pub), (T17_PEER_ID, peer_pub)];
        let area = "uc2-net-sender-t17";
        let leader = tk::shared_transport(
            area,
            &format!("{tag}-leader"),
            T17_LEADER_ID,
            T17_PRIV_LEADER,
            &allow,
        );
        let peer = tk::shared_transport(
            area,
            &format!("{tag}-peer"),
            T17_PEER_ID,
            T17_PRIV_PEER,
            &allow,
        );
        tk::establish(&leader, T17_LEADER_ID, &peer, T17_PEER_ID);
        tk::deliver_group_key(&leader, T17_LEADER_ID, &peer, T17_PEER_ID);

        let f = Fake::new();
        let peer_ids = PeerIds::new();
        peer_ids.store([(f.addr(), T17_PEER_ID)]);

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("snap-4096.ultsnap");
        std::fs::write(&snap_path, t17_snapshot_bytes()).unwrap();
        let total = T17_SNAP_LEN as u64;

        let b = buffer();
        // Prime far ahead so an injected NAK at 0 is below the ring floor →
        // unservable → upgrades to a snapshot session (the real trigger).
        b.counters().prime(4 * b.capacity());
        let (_tx, rx) = mpsc::sync_channel(16);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        cfg.crypto_enabled = true;
        let mut s = Sender::with_crypto(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f.addr()],
            &[],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
            Some(SenderCrypto {
                half: leader.send_half(),
                peer_ids,
            }),
        );
        s.set_snapshot_source(Arc::new(move || {
            Some(SnapshotSet {
                services_declared: 0b1,
                identity: ident(0b1),
                version: [0; 8],
                config: t17_config_bytes(),
                artifacts: vec![SnapArtifact {
                    service_id: 0,
                    snapshot_pos: 4096,
                    path: snap_path.clone(),
                    len: total,
                }],
            })
        }));
        (s, f, peer, dir)
    }

    /// Same shape, crypto OFF — the cleartext-parity control.
    fn sender_without_crypto_and_snapshot_source() -> (Sender, Fake, tempfile::TempDir) {
        let f = Fake::new();
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("snap-4096.ultsnap");
        std::fs::write(&snap_path, t17_snapshot_bytes()).unwrap();
        let total = T17_SNAP_LEN as u64;
        let b = buffer();
        b.counters().prime(4 * b.capacity());
        let (_tx, rx) = mpsc::sync_channel(16);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f.addr()],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
        );
        s.set_snapshot_source(Arc::new(move || {
            Some(SnapshotSet {
                services_declared: 0b1,
                identity: ident(0b1),
                version: [0; 8],
                config: t17_config_bytes(),
                artifacts: vec![SnapArtifact {
                    service_id: 0,
                    snapshot_pos: 4096,
                    path: snap_path.clone(),
                    len: total,
                }],
            })
        }));
        (s, f, dir)
    }

    /// M14c Task 4: two artifacts, ids 0 and 2, 2048 B and 3000 B —
    /// deliberately not a multiple of the MTU budget, so the artifact-boundary
    /// clamp is exercised. Same shape as
    /// `sender_without_crypto_and_snapshot_source` (primed ring, so an injected
    /// NAK at 0 is below the floor and upgrades to a session).
    fn sender_with_two_artifacts() -> (Sender, Fake, tempfile::TempDir) {
        let f = Fake::new();
        let dir = tempfile::tempdir().unwrap();
        let p0 = dir.path().join("snap-2048.ultsnap");
        let p2 = dir.path().join("snap-4096.ultsnap");
        std::fs::write(&p0, vec![0xA1u8; 2048]).unwrap();
        std::fs::write(&p2, vec![0xB2u8; 3000]).unwrap();
        let b = buffer();
        b.counters().prime(4 * b.capacity());
        let (_tx, rx) = mpsc::sync_channel(16);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f.addr()],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
        );
        s.set_snapshot_source(Arc::new(move || {
            Some(SnapshotSet {
                services_declared: 0b101,
                identity: ident(0b101),
                version: [0; 8],
                config: t17_config_bytes(),
                artifacts: vec![
                    SnapArtifact {
                        service_id: 0,
                        snapshot_pos: 2048,
                        path: p0.clone(),
                        len: 2048,
                    },
                    SnapArtifact {
                        service_id: 2,
                        snapshot_pos: 4096,
                        path: p2.clone(),
                        len: 3000,
                    },
                ],
            })
        }));
        (s, f, dir)
    }

    #[test]
    fn a_session_ships_one_begin_per_artifact_and_never_spans_a_boundary() {
        let (mut s, f, _dir) = sender_with_two_artifacts();
        let addr = f.addr();
        s.on_nak(addr, 0, 96); // below the ring floor → upgrades to a session
        let mut begins: Vec<SnapBeginBody> = Vec::new();
        let mut chunks: Vec<(u64, usize)> = Vec::new(); // (stream offset, payload len)
        // Four cycles: the first ships BEGIN(0) + all of artifact 0, the second
        // BEGIN(2) + all of artifact 2. `Fake::recv_raw` spins 500 ms on an
        // empty drain, so [`SNAP_BEGIN_RESEND_NS`] DOES elapse between cycles —
        // the trailing artifact's BEGIN is re-sent (a byte-identical no-op at
        // the receiver), which `dedup` below folds away.
        for _ in 0..4 {
            s.do_work();
            while let Some(d) = f.recv_raw() {
                let h = read_datagram_header(&d).unwrap();
                match h.kind {
                    DGRAM_KIND_SNAP_BEGIN => {
                        begins.push(read_snap_begin_body(&d[DATAGRAM_HEADER_LEN..]).unwrap())
                    }
                    DGRAM_KIND_SNAP_CHUNK => {
                        chunks.push((h.position, d.len() - DATAGRAM_HEADER_LEN))
                    }
                    _ => {}
                }
            }
        }
        for b in &begins {
            assert_eq!(b.layout, SNAP_BEGIN_LAYOUT_V3);
            assert_eq!(
                b.declared_mask(),
                0b101,
                "the declared mask rides EVERY begin"
            );
            assert_eq!(
                b.session, begins[0].session,
                "one session for the whole stream"
            );
            assert_eq!(
                b.config,
                t17_config_bytes(),
                "config rides every begin unchanged"
            );
        }
        // A re-send is byte-identical to its original, so distinct bodies ==
        // one per artifact, in ascending `service_id` order.
        begins.dedup();
        assert_eq!(
            begins.len(),
            2,
            "one distinct BEGIN per artifact: {begins:?}"
        );
        assert_eq!(
            (
                begins[0].service_id,
                begins[0].snapshot_pos,
                begins[0].total_len
            ),
            (0, 2048, 2048)
        );
        assert_eq!(
            (
                begins[1].service_id,
                begins[1].snapshot_pos,
                begins[1].total_len
            ),
            (2, 4096, 3000)
        );
        // Stream-global offsets, contiguous over [0, 5048), and no datagram
        // straddles the 2048 boundary (the receiver writes one datagram into
        // exactly one `.part`).
        chunks.sort_unstable();
        chunks.dedup();
        let mut want = 0u64;
        for &(off, len) in &chunks {
            assert_eq!(off, want, "chunks fill the stream contiguously: {chunks:?}");
            assert!(
                off >= 2048 || off + len as u64 <= 2048,
                "chunk [{off}, {}) spans the artifact boundary at 2048",
                off + len as u64
            );
            want = off + len as u64;
        }
        assert_eq!(want, 5048, "the whole 2048 + 3000 byte stream was sent");
    }

    /// M14c review round 2: the same shape as
    /// `sender_without_crypto_and_snapshot_source`, but the ctrl `SyncSender`
    /// is handed back (so a test can inject a real `CtrlMsg::SnapNak` through
    /// `do_work`'s own drain) and the declared mask / artifact ids are the
    /// caller's, so a deliberately non-covering set can be built.
    fn sender_with_snap_source_and_ctrl(
        services_declared: u64,
        ids: &[u8],
    ) -> (Sender, Fake, mpsc::SyncSender<CtrlMsg>, tempfile::TempDir) {
        let f = Fake::new();
        let dir = tempfile::tempdir().unwrap();
        let artifacts: Vec<SnapArtifact> = ids
            .iter()
            .map(|&id| {
                let path = dir.path().join(format!("snap-{id}-2048.ultsnap"));
                std::fs::write(&path, vec![0xC3u8; 2048]).unwrap();
                SnapArtifact {
                    service_id: id,
                    snapshot_pos: 2048,
                    path,
                    len: 2048,
                }
            })
            .collect();
        let b = buffer();
        b.counters().prime(4 * b.capacity());
        let (tx, rx) = mpsc::sync_channel(16);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f.addr()],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
        );
        s.set_snapshot_source(Arc::new(move || {
            Some(SnapshotSet {
                services_declared,
                identity: ident(services_declared),
                version: [0; 8],
                config: t17_config_bytes(),
                artifacts: artifacts.clone(),
            })
        }));
        (s, f, tx, dir)
    }

    /// M14c review round 2, finding 1: a peer that only ever asks for bytes
    /// PAST the end of the stream must not hold the single session slot open.
    /// Such a `SNAP_NAK` is unservable (`part_at` is `None`, the request is
    /// dropped without progress), so it must not refresh `last_activity_ns` —
    /// otherwise the session never reaches `SNAP_SESSION_TIMEOUT_NS` and every
    /// other below-floor requester is refused for as long as the dead peer
    /// keeps NAKing.
    #[test]
    fn an_unservable_snap_nak_does_not_keep_a_dead_session_alive() {
        let (mut s, f, tx, _dir) = sender_with_snap_source_and_ctrl(0b1, &[0]);
        let addr = f.addr();
        s.on_nak(addr, 0, 96); // below the ring floor -> upgrades to a session
        s.do_work();
        let (session, stream_len) = {
            let sess = s
                .snap
                .as_ref()
                .expect("the below-floor NAK opened a session");
            (sess.session, sess.stream_len)
        };
        f.drain();

        // Age the sender's own clock in 10 s steps (its only clock is
        // `base.elapsed()`), feeding an unservable repair request each step.
        for _ in 0..6 {
            tx.send(CtrlMsg::SnapNak {
                from: addr,
                session,
                offset: stream_len, // one past the last byte: unservable, forever
                length: 64,
            })
            .unwrap();
            s.base = s.base.checked_sub(Duration::from_secs(10)).unwrap();
            s.do_work();
            f.drain();
            if s.snap.is_none() {
                break;
            }
        }
        assert!(
            s.snap.is_none(),
            "a session fed nothing but unservable SNAP_NAKs must still be abandoned at \
             SNAP_SESSION_TIMEOUT_NS - only a SERVABLE request is liveness"
        );
    }

    /// M14c review round 2, finding 2: the sender refuses a set that does not
    /// COVER its own declared mask. Declaring `0b111` while shipping ids 0 and
    /// 2 would advertise a third artifact that never arrives: the receiver's
    /// `received != services_declared` never closes and it probes for a BEGIN
    /// that will never be sent, burning the session slot until the timeout.
    /// Staying an overrun (the peer re-NAKs) is the correct, recoverable shape.
    #[test]
    fn a_set_that_does_not_cover_its_declared_mask_never_opens_a_session() {
        let (mut s, f, _tx, _dir) = sender_with_snap_source_and_ctrl(0b111, &[0, 2]);
        let addr = f.addr();
        let before = s.stats().overruns.load(Ordering::Relaxed);
        s.on_nak(addr, 0, 96);
        for _ in 0..4 {
            s.do_work();
        }
        assert!(
            s.snap.is_none(),
            "a set that misses a declared id must not open a session"
        );
        assert!(
            f.recv_raw().is_none(),
            "not one datagram of a half-formed session goes out"
        );
        assert!(
            s.stats().overruns.load(Ordering::Relaxed) > before,
            "the refusal stays a counted overrun, not a silent drop"
        );

        // The control: the SAME artifacts with a mask they do cover DO open one.
        let (mut ok, ok_f, _tx2, _dir2) = sender_with_snap_source_and_ctrl(0b101, &[0, 2]);
        ok.on_nak(ok_f.addr(), 0, 96);
        ok.do_work();
        assert!(ok.snap.is_some(), "a covering set still opens a session");
    }

    /// Wire 0.7.0 (spec §5): `services_declared` must equal
    /// `identity_mask(&identity)` — the invariant `try_open_snap_session`
    /// gained when the mask stopped being an independently-set field and
    /// became derived. This set otherwise satisfies every OTHER invariant —
    /// two artifacts, exactly `services_declared.count_ones()` of them, both
    /// ids inside the mask — so if the new clause were ever removed this
    /// case alone would start opening a session; only ONE row of `identity`
    /// is actually non-zero (a bug in the caller, e.g.
    /// `uc_node::snapshot_set_for` disagreeing with itself), which must
    /// refuse the same way an uncovered artifact set does, before any
    /// artifact file is even opened.
    #[test]
    fn a_declared_mask_that_disagrees_with_the_identity_array_never_opens_a_session() {
        let (mut s, f) = sender_with_explicit_snapshot_set(SnapshotSet {
            services_declared: 0b11,
            identity: ident(0b1), // only row 0 is actually non-zero
            version: [0; 8],
            config: t17_config_bytes(),
            artifacts: vec![
                SnapArtifact {
                    service_id: 0,
                    snapshot_pos: 2048,
                    path: PathBuf::from("/nonexistent-never-opened-0"),
                    len: 2048,
                },
                SnapArtifact {
                    service_id: 1,
                    snapshot_pos: 2048,
                    path: PathBuf::from("/nonexistent-never-opened-1"),
                    len: 2048,
                },
            ],
        });
        assert_snap_session_refused(
            &mut s,
            &f,
            "services_declared disagrees with identity_mask(&identity)",
        );
    }

    // ==== M14c2 (T10a): `try_open_snap_session`'s refusal paths ==============
    //
    // M14c deferred the unit tests for these. Each refusal must (a) leave
    // `self.snap` empty, (b) put NOT ONE datagram of a half-formed session on
    // the wire, and (c) stay a counted `overruns` — the recoverable shape, the
    // peer re-NAKs. Only the covering-mask leg was pinned before (by
    // `a_set_that_does_not_cover_its_declared_mask_never_opens_a_session`).

    /// A scratch directory on REAL DISK, never `/tmp` (RAM-backed tmpfs on the
    /// dev box — CLAUDE.md). Mirrors `receiver.rs`'s helper of the same name:
    /// `CARGO_TARGET_TMPDIR` is set only for integration-test binaries and
    /// these are inline `#[cfg(test)]` unit tests in the lib target, so this
    /// falls back to a package-relative `target/` directory.
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
            .prefix("uc2-snap-send-")
            .tempdir_in(&root)
            .expect("tempdir")
    }

    /// A `Sender` whose `SnapshotSource` hands back exactly `set` — so a test
    /// can offer a deliberately broken one — with the ring primed so an
    /// injected NAK at 0 is below the floor and reaches the upgrade path.
    fn sender_with_explicit_snapshot_set(set: SnapshotSet) -> (Sender, Fake) {
        let f = Fake::new();
        let b = buffer();
        b.counters().prime(4 * b.capacity());
        let (_tx, rx) = mpsc::sync_channel(16);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f.addr()],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
        );
        s.set_snapshot_source(Arc::new(move || Some(set.clone())));
        (s, f)
    }

    /// Inject the below-floor NAK, pump a few cycles, and assert the three
    /// properties every refusal owes: no session, no datagram, one overrun.
    fn assert_snap_session_refused(s: &mut Sender, f: &Fake, why: &str) {
        let before = s.stats().overruns.load(Ordering::Relaxed);
        s.on_nak(f.addr(), 0, 96); // below the ring floor → the upgrade path
        for _ in 0..4 {
            s.do_work();
        }
        assert!(s.snap.is_none(), "{why}: no session may open");
        assert!(
            f.recv_raw().is_none(),
            "{why}: not one datagram of a half-formed session"
        );
        assert!(
            s.stats().overruns.load(Ordering::Relaxed) > before,
            "{why}: the refusal stays a counted overrun, not a silent drop"
        );
    }

    /// Refusal 1: the set does not COVER the declared mask. Both legs of the
    /// one guard — a declared id with no artifact, and an artifact for an id
    /// outside the mask (the count matches, the ids do not). Either way a
    /// `SNAP_BEGIN` would advertise a `services_declared` the stream can never
    /// satisfy: the receiver's `received != services_declared` never closes and
    /// it probes for a BEGIN that will never come.
    #[test]
    fn a_set_that_does_not_match_the_declared_ids_never_opens_a_session() {
        let dir = snap_scratch_dir();
        let make = |id: u8| {
            let path = dir.path().join(format!("snap-{id}.ultsnap"));
            std::fs::write(&path, vec![0xD4u8; 2048]).unwrap();
            SnapArtifact {
                service_id: id,
                snapshot_pos: 2048,
                path,
                len: 2048,
            }
        };

        // (a) ids 0 and 2 declared, only id 0 shipped.
        let (mut s, f) = sender_with_explicit_snapshot_set(SnapshotSet {
            services_declared: 0b101,
            identity: ident(0b101),
            version: [0; 8],
            config: t17_config_bytes(),
            artifacts: vec![make(0)],
        });
        assert_snap_session_refused(&mut s, &f, "a declared id with no artifact");
        assert_eq!(
            s.stats().snap_open_failed.load(Ordering::Relaxed),
            0,
            "refused before any file is opened"
        );

        // (b) the right NUMBER of artifacts, one of them for an undeclared id.
        let (mut s, f) = sender_with_explicit_snapshot_set(SnapshotSet {
            services_declared: 0b101,
            identity: ident(0b101),
            version: [0; 8],
            config: t17_config_bytes(),
            artifacts: vec![make(0), make(1)],
        });
        assert_snap_session_refused(&mut s, &f, "an artifact for an undeclared id");
        assert_eq!(s.stats().snap_open_failed.load(Ordering::Relaxed), 0);
    }

    /// Refusal 2: an artifact the store has not finished writing — the `.part`
    /// case. It is offered with a length of 0 (nothing durable in it yet), and
    /// a `SNAP_BEGIN` announcing `total_len == 0` is dropped outright by the
    /// receiver, so such a session could never complete. The same guard also
    /// rejects a set whose ids do not strictly ascend, since an artifact's
    /// STREAM base is the sum of its predecessors' lengths.
    #[test]
    fn a_zero_length_part_or_a_misordered_set_never_opens_a_session() {
        let dir = snap_scratch_dir();
        let part = dir.path().join("incoming-2048.part");
        std::fs::write(&part, b"").unwrap();
        let (mut s, f) = sender_with_explicit_snapshot_set(SnapshotSet {
            services_declared: 0b1,
            identity: ident(0b1),
            version: [0; 8],
            config: t17_config_bytes(),
            artifacts: vec![SnapArtifact {
                service_id: 0,
                snapshot_pos: 2048,
                path: part,
                len: 0,
            }],
        });
        assert_snap_session_refused(&mut s, &f, "a half-written .part offered with len 0");
        assert_eq!(
            s.stats().snap_open_failed.load(Ordering::Relaxed),
            0,
            "the file opens fine — this is the length guard, not the open guard"
        );

        // The ordering leg: ids 0 and 2 declared, shipped descending.
        let make = |id: u8| {
            let path = dir.path().join(format!("snap-{id}.ultsnap"));
            std::fs::write(&path, vec![0xD4u8; 2048]).unwrap();
            SnapArtifact {
                service_id: id,
                snapshot_pos: 2048,
                path,
                len: 2048,
            }
        };
        let (mut s, f) = sender_with_explicit_snapshot_set(SnapshotSet {
            services_declared: 0b101,
            identity: ident(0b101),
            version: [0; 8],
            config: t17_config_bytes(),
            artifacts: vec![make(2), make(0)],
        });
        assert_snap_session_refused(&mut s, &f, "a set whose ids do not strictly ascend");
    }

    /// Refusal 3: the `File::open` TOCTOU. The store listed an artifact its
    /// persisted floor marker says is durable and it is gone (or unreadable) by
    /// the time the session opens — a purge racing a session, a hand-edited
    /// snapshot dir, a permission change. Before M14c2 this was the ONE refusal
    /// with no counter at all, indistinguishable from the two ordinary ones
    /// ("no source wired", "a session is already in flight"): the operator saw
    /// a joiner NAKing forever and nothing naming the leader's disk.
    /// `snap_open_failed` names it.
    #[test]
    fn an_unopenable_artifact_counts_snap_open_failed_and_stays_an_overrun() {
        let dir = snap_scratch_dir();
        let gone = dir.path().join("snap-2048.ultsnap"); // deliberately never created
        let (mut s, f) = sender_with_explicit_snapshot_set(SnapshotSet {
            services_declared: 0b1,
            identity: ident(0b1),
            version: [0; 8],
            config: t17_config_bytes(),
            artifacts: vec![SnapArtifact {
                service_id: 0,
                snapshot_pos: 2048,
                path: gone,
                len: 2048,
            }],
        });
        assert_snap_session_refused(&mut s, &f, "an artifact whose file cannot be opened");
        assert_eq!(
            s.stats().snap_open_failed.load(Ordering::Relaxed),
            1,
            "one open failure per refused attempt — the counter that names the leader's disk"
        );
    }

    /// M14c2 (T10a): a repair `SNAP_NAK` whose range falls inside an artifact
    /// whose `SNAP_BEGIN` has NOT gone out in this session is SKIPPED — not
    /// served, not an error. The receiver can only place a chunk inside an
    /// artifact it has already announced (`snap_chunk` drops anything else), so
    /// serving one spends the cycle's chunk budget on datagrams guaranteed to
    /// be discarded — and does it while the peer is blocked. The BEGIN goes out
    /// on its own cadence; the same request is served the moment it has.
    ///
    /// The head repair NAK must sit in an artifact that IS begun:
    /// `drive_snap_session` targets the head NAK's artifact, so a head NAK in
    /// an un-begun one ships its BEGIN first, in the same cycle.
    #[test]
    fn a_repair_nak_inside_a_not_yet_begun_artifact_is_skipped_until_its_begin_goes_out() {
        let (mut s, f, _tx, _dir) = sender_with_snap_source_and_ctrl(0b101, &[0, 2]);
        s.on_nak(f.addr(), 0, 96); // below the ring floor → opens the session
        s.do_work(); // ...on the cycle that drains the NAK queue
        assert!(s.snap.is_some(), "the below-floor NAK opened a session");
        f.drain();

        {
            let sess = s.snap.as_mut().expect("the session is open");
            // `begun_ns` in the far future ⇒ `now - at` saturates to 0, so the
            // artifact reads as begun AND never stale: this cycle emits no
            // BEGIN at all, and every SNAP_CHUNK it does emit is attributable.
            sess.parts[0].begun_ns = Some(u64::MAX);
            sess.parts[1].begun_ns = None;
            sess.naks.push_back((0, 64)); // inside artifact 0, [0, 2048)
            sess.naks.push_back((2048 + 16, 64)); // inside artifact 2, un-begun
        }
        s.do_work();

        let mut begins = 0usize;
        let mut chunks: Vec<u64> = Vec::new();
        while let Some(d) = f.recv_raw() {
            let h = read_datagram_header(&d).unwrap();
            match h.kind {
                DGRAM_KIND_SNAP_BEGIN => begins += 1,
                DGRAM_KIND_SNAP_CHUNK => chunks.push(h.position),
                _ => {}
            }
        }
        assert_eq!(begins, 0, "neither artifact's BEGIN is due this cycle");
        assert!(
            chunks.iter().all(|&off| off < 2048),
            "no SNAP_CHUNK may go out for an artifact whose BEGIN has not been sent: {chunks:?}"
        );
        assert!(
            s.snap.as_ref().unwrap().naks.is_empty(),
            "the skipped request is dropped (the peer re-NAKs), never re-queued into a spin"
        );

        // The control: with artifact 2's BEGIN sent, the SAME request IS served.
        {
            let sess = s.snap.as_mut().unwrap();
            sess.parts[1].begun_ns = Some(u64::MAX);
            sess.naks.push_back((2048 + 16, 64));
        }
        s.do_work();
        let mut served: Vec<u64> = Vec::new();
        while let Some(d) = f.recv_raw() {
            let h = read_datagram_header(&d).unwrap();
            if h.kind == DGRAM_KIND_SNAP_CHUNK {
                served.push(h.position);
            }
        }
        assert!(
            served.contains(&(2048 + 16)),
            "once its BEGIN is out, the same NAK is served: {served:?}"
        );
    }

    /// Drive a snapshot session and collect the raw datagrams of `kind`.
    fn snap_datagrams(s: &mut Sender, f: &Fake, to: SocketAddr, kind: u8) -> Vec<Vec<u8>> {
        s.on_nak(to, 0, 96); // below the ring floor → upgrades to a session
        let mut out = Vec::new();
        for _ in 0..4 {
            s.do_work();
            while let Some(d) = f.recv_raw() {
                if d.len() >= DATAGRAM_HEADER_LEN && read_datagram_header(&d).unwrap().kind == kind
                {
                    out.push(d);
                }
            }
        }
        out
    }

    #[test]
    fn a_snapshot_chunk_is_sealed_and_respects_the_shrunken_mtu_budget() {
        let (mut s, f, peer, _dir) = sender_with_crypto_and_established_session("chunk-sealed");
        let mtu = s.cfg.mtu;
        let addr = f.addr();
        let chunks = snap_datagrams(&mut s, &f, addr, DGRAM_KIND_SNAP_CHUNK);
        assert!(
            !chunks.is_empty(),
            "fixture must actually produce snapshot chunks"
        );

        let raw = t17_snapshot_bytes();
        for d in &chunks {
            // The ciphertext region specifically — not whole-datagram
            // inequality, which the cleartext 16-byte `DATAGRAM_HEADER_LEN`
            // alone would satisfy even if the payload went out verbatim.
            let ct = &d[DATAGRAM_HEADER_LEN + COUNTER_LEN..d.len() - TAG_LEN];
            assert!(
                !raw.windows(ct.len().min(raw.len())).any(|w| w == ct),
                "the snapshot artifact's bytes must not be readable on the wire"
            );
            assert!(
                d.len() <= mtu,
                "a SEALED chunk must still fit the MTU (got {} > {mtu}) — the chunk budget \
                 must subtract CRYPTO_OVERHEAD",
                d.len()
            );
        }

        // And it is a REAL seal, not scrambling: the peer opens it, and what
        // comes out is exactly the file's bytes at the header's offset.
        let mut recv = peer.receive_half();
        let mut d = chunks[0].clone();
        let n = d.len();
        let off = read_datagram_header(&d).unwrap().position as usize;
        let len = recv
            .open_slice(T17_LEADER_ID, &mut d, n)
            .expect("the peer must open the sealed chunk under the pairwise session");
        assert_eq!(
            &d[DATAGRAM_HEADER_LEN..len],
            &raw[off..off + (len - DATAGRAM_HEADER_LEN)],
            "the opened chunk must be the artifact's bytes at the header's offset"
        );
    }

    #[test]
    fn a_snapshot_begin_is_sealed_so_its_carried_config_cannot_be_forged() {
        // The integrity half: `SnapBeginBody.config` reaches the receiving
        // node's `maybe_adopt_incoming_snapshot`. Unsealed = unauthenticated
        // = attacker-chosen membership.
        let (mut s, f, peer, _dir) = sender_with_crypto_and_established_session("begin-sealed");
        let addr = f.addr();
        let begins = snap_datagrams(&mut s, &f, addr, DGRAM_KIND_SNAP_BEGIN);
        assert!(!begins.is_empty(), "a session opens with a SNAP_BEGIN");
        let cfgb = t17_config_bytes();
        // M14c: the BEGIN is re-sent on the [`SNAP_BEGIN_RESEND_NS`] cadence
        // (`Fake::recv_raw`'s 500 ms empty-drain spin makes that elapse here),
        // so open EVERY one: each must be sealed, and all must be the SAME
        // body — a re-send is a byte-identical no-op at the receiver.
        let mut recv = peer.receive_half();
        let mut bodies = Vec::new();
        for d in &begins {
            assert!(
                !d.windows(cfgb.len()).any(|w| w == cfgb.as_slice()),
                "the cluster config must not be readable (or forgeable) on the wire"
            );
            let mut open = d.clone();
            let n = open.len();
            let len = recv
                .open_slice(T17_LEADER_ID, &mut open, n)
                .expect("the peer must open the sealed SNAP_BEGIN");
            bodies.push(
                read_snap_begin_body(&open[DATAGRAM_HEADER_LEN..len]).expect("well-formed body"),
            );
        }
        bodies.dedup();
        assert_eq!(
            bodies.len(),
            1,
            "exactly one DISTINCT SNAP_BEGIN opens a session"
        );
        let body = &bodies[0];
        assert_eq!(
            body.config, cfgb,
            "the config survives the round trip intact"
        );
        assert_eq!(body.total_len, T17_SNAP_LEN as u64);
        assert_eq!(body.layout, SNAP_BEGIN_LAYOUT_V3);
        assert_eq!(body.service_id, 0);
        assert_eq!(body.declared_mask(), 0b1);
    }

    #[test]
    fn cleartext_mode_snapshot_output_is_byte_identical_to_pre_m8() {
        // The other direction of the same discrimination: with crypto off,
        // the artifact's bytes ARE on the wire and the datagram is exactly
        // CRYPTO_OVERHEAD shorter. Without this, a mutant that seals
        // unconditionally (or one that never seals) is half-invisible.
        let (mut s, f, _dir) = sender_without_crypto_and_snapshot_source();
        let addr = f.addr();
        let chunks = snap_datagrams(&mut s, &f, addr, DGRAM_KIND_SNAP_CHUNK);
        assert!(!chunks.is_empty(), "fixture must produce cleartext chunks");
        let raw = t17_snapshot_bytes();
        let d = &chunks[0];
        let off = read_datagram_header(d).unwrap().position as usize;
        assert_eq!(
            &d[DATAGRAM_HEADER_LEN..],
            &raw[off..off + (d.len() - DATAGRAM_HEADER_LEN)],
            "cleartext mode ships the artifact verbatim, exactly as pre-M8"
        );

        let (mut sealed_s, sealed_f, _p, _d2) =
            sender_with_crypto_and_established_session("parity");
        let sealed_addr = sealed_f.addr();
        let sealed = snap_datagrams(&mut sealed_s, &sealed_f, sealed_addr, DGRAM_KIND_SNAP_CHUNK);
        assert_eq!(
            sealed[0].len(),
            d.len(),
            "the sealed chunk fills the SAME MTU as the cleartext one — it carries \
             CRYPTO_OVERHEAD fewer artifact bytes, not CRYPTO_OVERHEAD more datagram bytes"
        );
        assert_eq!(
            sealed[0].len() - DATAGRAM_HEADER_LEN - CRYPTO_OVERHEAD,
            d.len() - DATAGRAM_HEADER_LEN - CRYPTO_OVERHEAD,
            "same payload budget on both sides of the comparison"
        );
    }

    #[test]
    fn a_snapshot_send_to_an_unresolvable_peer_is_dropped_not_sent_in_the_clear() {
        // The fail-closed shape: no `SocketAddr -> NodeId` entry means no
        // pairwise key, which must mean NO DATAGRAM — never a cleartext
        // fallback, which would make the whole feature optional per peer.
        let (mut s, f, _peer, _dir) = sender_with_crypto_and_established_session("unresolvable");
        s.peer_ids.clear(); // membership map lost this address
        s.peer_ids_src = None; // ...and no refresh will bring it back
        let addr = f.addr();
        let before = s.stats().seal_failures.load(Ordering::Relaxed);
        let all = snap_datagrams(&mut s, &f, addr, DGRAM_KIND_SNAP_CHUNK);
        assert!(
            all.is_empty(),
            "an unsealed snapshot chunk must never reach the wire"
        );
        let begins = snap_datagrams(&mut s, &f, addr, DGRAM_KIND_SNAP_BEGIN);
        assert!(
            begins.is_empty(),
            "an unsealed SNAP_BEGIN must never reach the wire"
        );
        assert!(
            s.stats().seal_failures.load(Ordering::Relaxed) > before,
            "the drop must be counted, not silent"
        );
    }

    #[test]
    fn a_snapshot_send_with_no_established_session_is_dropped_not_sent_in_the_clear() {
        // Resolvable peer, but the handshake never completed — `NoSession`.
        // Same fail-closed rule, a different error path.
        use crate::crypto_testkit as tk;
        let leader_pub = tk::identity_public("nosess-lpub", T17_PRIV_LEADER);
        let peer_pub = tk::identity_public("nosess-ppub", T17_PRIV_PEER);
        let leader = tk::shared_transport(
            "uc2-net-sender-t17",
            "nosess-leader",
            T17_LEADER_ID,
            T17_PRIV_LEADER,
            &[(T17_LEADER_ID, leader_pub), (T17_PEER_ID, peer_pub)],
        );
        let f = Fake::new();
        let peer_ids = PeerIds::new();
        peer_ids.store([(f.addr(), T17_PEER_ID)]);
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("snap-4096.ultsnap");
        std::fs::write(&snap_path, t17_snapshot_bytes()).unwrap();
        let total = T17_SNAP_LEN as u64;
        let b = buffer();
        b.counters().prime(4 * b.capacity());
        let (_tx, rx) = mpsc::sync_channel(16);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = u64::MAX;
        cfg.crypto_enabled = true;
        let mut s = Sender::with_crypto(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f.addr()],
            &[],
            3,
            rx,
            cfg,
            term_handle(9),
            always_leader(),
            Some(SenderCrypto {
                half: leader.send_half(),
                peer_ids,
            }),
        );
        s.set_snapshot_source(Arc::new(move || {
            Some(SnapshotSet {
                services_declared: 0b1,
                identity: ident(0b1),
                version: [0; 8],
                config: t17_config_bytes(),
                artifacts: vec![SnapArtifact {
                    service_id: 0,
                    snapshot_pos: 4096,
                    path: snap_path.clone(),
                    len: total,
                }],
            })
        }));
        let addr = f.addr();
        s.on_nak(addr, 0, 96);
        for _ in 0..4 {
            s.do_work();
        }
        assert!(
            f.recv_raw().is_none(),
            "with no established session NOTHING goes out — never a cleartext fallback"
        );
        assert!(
            s.stats().seal_failures.load(Ordering::Relaxed) > 0,
            "counted, not silent"
        );
    }
}
