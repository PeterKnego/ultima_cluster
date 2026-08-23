// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Node composition + the consensus agent (Task 8).

use std::collections::HashMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Instant, SystemTime};

use uc2_consensus::config::{Addr, ClusterConfig, ConfigOp};
use uc2_consensus::election::{Action, ElectionConfig, ElectionSm, Event, NodeId, Role};
use uc2_log::agent::{AgentRunner, IdleStrategy};
use uc2_log::archive::{Archive, ArchiveConfig};
use uc2_log::buffer::{AppendError, Appender, LogBuffer};
use uc2_log::cnc::{AdminAuth, AdminReq, AdminResp, CncMeta, CncPage};
use uc2_log::counters::LogCounters;
use uc2_log::state::{ConfigRecord, NodeState, StoredConfig, StoredMember, TermMap, TermMapEntry, VoteRecord};
use uc2_net::TermHandle;
use uc2_net::fault::{FaultConfig, FaultSocket, PartitionHandle};
use uc2_net::receiver::{
    CryptoIntake, FollowerConfig, FollowerReceiver, HandshakeDatagram, NetEvent, PeerIds,
};
use uc2_net::sender::{CtrlMsg, Sender, SenderConfig, SenderCrypto};
use uc2_crypto::admin::{AdminMessage, AdminPolicy};
use uc2_crypto::{CryptoConfig, HandshakeAction, Scope, SharedTransport, Transport};
use uc_protocol::v2::crypto::DGRAM_KIND_HS_KEY;
use uc_protocol::ring::{
    BroadcastProducer, BroadcastRing, MpscConsumer, MpscRing, SpscProducer, SpscRing,
};
use uc_protocol::v2::cnc::{
    CNC_MAX_PEER_SLOTS, CNC_PEER_ROLE_LEARNER, CNC_PEER_ROLE_VOTER, NODE_FLAG_CAN_SERVE,
    NODE_FLAG_LEADER,
};
use uc_protocol::v2::config::{WireConfig, WireMember, decode_config, encode_config};
use uc_protocol::v2::frame::{FRAME_TYPE_CONFIG, align_frame_len};
use uc_protocol::v2::ipc::{
    FLAG_V2_LINEARIZABLE, MSG_V2_NOT_LEADER, MSG_V2_RETRY, MSG_V2_SVC_QUERY, client_from_extra,
    extra_client,
};

use crate::audit::{AuditLog, AuditOrigin, AuditOutcome, AuditRecord, op_name};
use crate::ipc::InstanceDir;
use crate::read_round::ProbeRound;
use uc2_log::buffer::FrameRead;
use uc_protocol::v2::datagram::{
    CONFIG_PROPOSAL_BODY_LEN, CONFIG_REPLY_BODY_LEN, ConfigProposalBody, ConfigReplyBody,
    DATAGRAM_HEADER_LEN, DGRAM_KIND_COMMIT_POSITION, DGRAM_KIND_CONFIG_PROPOSAL,
    DGRAM_KIND_CONFIG_REPLY, DGRAM_KIND_READ_PROBE, DGRAM_KIND_READ_PROBE_ACK,
    DGRAM_KIND_REQUEST_VOTE, DGRAM_KIND_TERM_MAP, DGRAM_KIND_VOTE, DatagramHeader,
    MAX_TERM_MAP_WIRE_ENTRIES, READ_PROBE_BODY_LEN, REQUEST_VOTE_BODY_LEN, ReadProbeBody,
    RequestVoteBody, TERM_MAP_ENTRY_LEN, TERM_MAP_HEADER_LEN, TermMapEntryWire, VOTE_BODY_LEN,
    VoteBody, write_config_proposal_body, write_config_reply_body, write_datagram_header,
    write_read_probe_body, write_request_vote_body, write_term_map_body, write_vote_body,
};

/// Single-slot truncation ack. One truncation is in flight at a time (the SM
/// latch serializes them), so a slot suffices and, unlike a bounded channel,
/// cannot drop an ack (M4 final-review carry: infallible ack send). Holds the
/// `(epoch, to)` of the most recently completed truncation until the consensus
/// agent takes it.
#[derive(Clone, Default)]
pub(crate) struct TruncationSlot(Arc<Mutex<Option<(u64, u64)>>>);

impl TruncationSlot {
    pub fn post(&self, epoch: u64, to: u64) {
        *self.0.lock().unwrap() = Some((epoch, to));
    }
    pub fn take(&self) -> Option<(u64, u64)> {
        self.0.lock().unwrap().take()
    }
}

/// A command from the consensus agent to the archive agent (M6 Task 4). Was a
/// bare `(epoch, to)` truncation tuple through M5; the snapshot floor adds a
/// second verb, so the channel carries a typed enum. `Truncate` keeps the exact
/// M4/M5 semantics (persist-map-before-truncate is done on the consensus side;
/// the archive just executes + acks via the [`TruncationSlot`]). `Purge` is
/// best-effort and needs no ack — a failed purge simply retries next interval,
/// and correctness never depends on any particular block still being present
/// (a reader below the floor recovers via the snapshot path).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArchiveCmd {
    /// Drop the divergent tail at/above `to`; ack `(epoch, to)` when done.
    Truncate { epoch: u64, to: u64 },
    /// Leader-open collapse (issue #6): drop the unreplicated volatile tail
    /// at/above `to` (`base`, the new leader's durable frontier) and re-prime
    /// there, then ack `(epoch, to)` on the SEPARATE collapse slot.
    ///
    /// Physically identical to [`ArchiveCmd::Truncate`] — the distinct variant
    /// (and distinct ack slot) exists because the two brackets are independent:
    /// a reconcile truncation can be in flight when an election is won, and
    /// `TruncationSlot` holds exactly one ack.
    ///
    /// This MUST route through the archive agent rather than priming the
    /// counters on the consensus thread. `base` is `ElectionSm::durable`, a
    /// value sampled in an EARLIER duty cycle than the vote drain that produced
    /// `BecomeLeader`, so the archive may have fsynced another block since —
    /// leaving its private `durable_pos` strictly ABOVE `base`. Priming behind
    /// the archive's back left its cursor mid-frame once the new leader rewrote
    /// the buffer with a different frame layout (the nightly `elle_partition`
    /// `RecorderCorrupt` fail-stop), and left the journal holding the discarded
    /// tail. Running the cut ON the archive thread serializes it against that
    /// agent's own `do_work` and resets `durable_pos` — the same discipline
    /// `Truncate` has always had.
    Collapse { epoch: u64, to: u64 },
    /// Drop whole journal blocks strictly below the block covering `below`
    /// (`Archive::purge_below`). No ack. Errors log-warn and drop.
    Purge { below: u64 },
    /// M6 Task 6: adopt `pos` as the archive floor WITHOUT bytes — the receiving
    /// side of a snapshot session (a learner) installed the state below `pos`
    /// from the shipped file, so the archive advances its frontier to `pos` and
    /// the counters prime there. No ack; a conflict logs + drops.
    AdoptFloor { pos: u64 },
}

/// Issue #6: a leader open awaiting its [`ArchiveCmd::Collapse`] ack. The SM has
/// already decided this node leads `term`; the physical half of the open (the
/// cut to `base`, then a fresh appender + the NewTerm frame) completes on the
/// ack, one duty cycle later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PendingLeaderOpen {
    pub epoch: u64,
    pub term: u32,
    pub base: u64,
}

/// Journal purge policy (M6 Task 4). **Default `Disabled` — purge is OFF by
/// default.** Every M6 bug class is "purged something someone still needed", so
/// a deployment opts in explicitly. `BelowSnapshot` purges journal blocks below
/// `snapshot_floor - slack_bytes`, never at/above the durable snapshot floor and
/// never into the block that covers it (the archive's `purge_below` keeps the
/// covering block; `Journal::purge_before` never drops the active segment — two
/// layers of slack). `slack_bytes` keeps a margin of still-replayable journal
/// below the floor so a follower whose NAK lands just under it can still be
/// served from the log instead of forcing a snapshot session.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PurgePolicy {
    #[default]
    Disabled,
    BelowSnapshot {
        slack_bytes: u64,
    },
}

/// Static-membership node configuration (M4: no discovery; M7 adds live
/// reconfiguration on top — see `members`/`learners` below).
#[derive(Debug)]
pub struct NodeConfig {
    pub id: NodeId,
    /// Every VOTING member INCLUDING self (if this node is a voter), as
    /// `(id, addr)`. Learners are NOT listed here.
    ///
    /// M7: this is the SEED config — authoritative only for a FRESH instance
    /// directory (no durable `ConfigRecord` yet). Once a node has booted once,
    /// the durable `ConfigRecord` (`uc2_log::state::NodeState::config_record`)
    /// plus the `FRAME_TYPE_CONFIG` stream own the cluster's actual membership;
    /// this field is then ignored (a restart with a stale/edited `members` list
    /// has no effect). A cluster that never appends a config frame behaves
    /// exactly as before M7 — the genesis record IS this seed, verbatim.
    pub members: Vec<(NodeId, SocketAddr)>,
    /// M6 Task 7: learner peers, as `(id, addr)`. Default empty. A learner is
    /// replicated-to (fan-out) but never counted (no vote, no quorum slot, no
    /// flow-control window, no read-quorum ack). If this node's OWN id is in
    /// `learners` it boots in learner mode (candidacy disabled). Learner ids must
    /// be disjoint from `members`.
    ///
    /// M7: the SEED, exactly like `members` above (same fresh-instance-dir-only
    /// caveat).
    pub learners: Vec<(NodeId, SocketAddr)>,
    pub bind: SocketAddr,
    /// The node's on-disk instance directory (flock'd; holds cnc page, log
    /// buffer, journal, state, and the IPC ring files). Reused across restarts.
    pub instance_dir: PathBuf,
    /// Application identity stamped into the cnc page; attaching parties
    /// (service, clients) must present the same `app_id` (else "wrong cluster").
    pub app_id: String,
    /// Ring capacity in bytes; power of two.
    pub buffer_bytes: usize,
    pub max_payload: usize,
    /// Ingress admission budget in bytes (`append - commit` backpressure gate,
    /// wired in Task 7). Default `256 * 1024`.
    pub admission_bytes: u64,
    pub election_timeout_min_ns: u64,
    pub election_timeout_max_ns: u64,
    pub seed: u64,
    pub faults: FaultConfig,
    /// M6 Task 4: journal purge policy. Default [`PurgePolicy::Disabled`] — a
    /// node never purges unless explicitly configured AND the service publishes
    /// a snapshot floor (a snapshot-incapable SM never registers one).
    pub purge: PurgePolicy,
    /// M6 Task 4: journal segment size in bytes (the archive rolls a new
    /// segment file at this granularity; purge drops whole non-active
    /// segments). Production default `64 MiB` ([`DEFAULT_JOURNAL_SEGMENT_BYTES`]);
    /// tests shrink it so a purge is observable without writing gigabytes.
    pub journal_segment_bytes: u64,
    /// M8 (Task 12): node-to-node wire crypto.
    /// [`CryptoConfig::Disabled`] — the `Default` — is byte-for-byte today's
    /// cleartext behavior for every M1–M7 deployment.
    ///
    /// `Enabled` is a **boot refusal** if the key or allowlist file is
    /// missing, malformed, or group/world-readable: [`Node::start`] returns
    /// `Err` before a single agent is spawned, exactly as the M7
    /// self-tombstone refusal does. A node configured to authenticate must
    /// never silently fall back to cleartext — that would make the whole
    /// feature opt-out per boot, by accident.
    pub crypto: CryptoConfig,
}

/// What a drain achieved before the node stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The archive recorded every appended byte; a restart replays nothing.
    Drained,
    /// The deadline expired first. The node stopped anyway — the un-recorded
    /// tail was never fsynced, so it was never acked; the restarted node
    /// simply re-fetches it.
    DeadlineExpired { append: u64, durable: u64 },
}

/// Production default journal segment size (matches `ArchiveConfig::new`).
pub const DEFAULT_JOURNAL_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Why a `submit` was refused: leader-only ingress. `Full` covers both a
/// saturated in-process queue and the admission window being closed
/// (`append - commit > admission_bytes`, Task 7) — either way the caller's
/// remedy is the same: back off and retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SubmitError {
    #[error("node is not a serving leader")]
    NotServing,
    #[error("ingress queue is full")]
    Full,
}

/// Process-level start options — the knobs that are NOT configuration data.
/// Split out from [`NodeConfig`] in M12b because both of these are live
/// process resources (a bound socket; loaded admin key material), not values
/// an operator writes into a TOML file and a harness clones around.
///
/// `Default` is exactly the pre-M12b behavior: bind a fresh socket at
/// `cfg.bind`, and authenticate admin requests by filesystem permissions
/// alone.
#[derive(Debug, Default)]
pub struct StartOpts {
    /// A pre-bound UDP socket to run the node on. `None` binds `cfg.bind`.
    /// The multi-node in-process harnesses bind every socket first so peers
    /// know all addresses before any agent runs.
    pub socket: Option<UdpSocket>,
    /// How this node authenticates admin requests arriving on the cnc admin
    /// band. [`AdminPolicy::Filesystem`] (the default) is the legacy posture:
    /// the instance directory's permissions ARE the admin boundary, and the
    /// cnc auth line is ignored entirely.
    pub admin: AdminPolicy,
}

/// Ingress queue depth (M5 replaces this with the client submit ring).
const INGRESS_CAPACITY: usize = 8192;
/// Consensus events drained per duty cycle (bounded work).
const NET_DRAIN_PER_CYCLE: usize = 4096;
/// Payloads appended per duty cycle (bounded work; plan §Task 8).
const INGRESS_PER_CYCLE: usize = 256;
/// NetEvent channel depth (T7 observability: a full channel counts a drop).
const NET_EVENT_CAPACITY: usize = 4096;
/// M8: handshake-plane channel depth (kinds 18/19/20, receiver → consensus).
/// Handshake traffic is a handful of datagrams per peer per link-up, so this
/// is deep enough to never fill in practice; a full channel drops and counts
/// (`FollowerStats::dropped_handshake`) and `Peers::tick` re-initiates.
const HANDSHAKE_CAPACITY: usize = 256;
/// M8: handshake datagrams drained per duty cycle (bounded work, like the
/// NetEvent drain).
const CRYPTO_HS_DRAIN_PER_CYCLE: usize = 64;
/// M8: minimum spacing between crypto maintenance passes (allowlist reload,
/// `Peers::tick`, rotation check). The consensus agent busy-spins, so this
/// keeps three mutex acquisitions off the per-cycle path; 20 ms is well
/// inside every deadline it feeds (handshake retry base 200 ms, allowlist
/// reload floor 1 s, group-key activation timeout 2 s).
const CRYPTO_MAINTENANCE_NS: u64 = 20_000_000;
/// M8: how often un-acked `HS_KEY` deliveries are re-sent while a minted
/// epoch is still outstanding. Matches the handshake's own retry base — a
/// lost key delivery is exactly as fatal to a link as a lost `HS_INIT`, and
/// `GroupPlane::mint` emits each delivery only once.
const CRYPTO_HS_KEY_REDELIVER_NS: u64 = 200_000_000;
/// M8: floor on operator-facing crypto diagnostics (per node, aggregate), so
/// a sustained fault cannot spam stderr at duty-cycle rate. The COUNTERS are
/// always exact; only the printing is throttled.
const CRYPTO_LOG_INTERVAL_NS: u64 = 1_000_000_000;
/// Query records drained per duty cycle (bounded work, like the ingress ring).
const QUERY_DRAIN_PER_CYCLE: usize = 64;
/// A linearizable read's confirmation deadline (spec §7): if the read-index
/// quorum + service catch-up do not complete within this, the read is answered
/// `MSG_V2_RETRY` (side-effect-free) and dropped. Same order as the election
/// timeout — a partitioned-away leader fails its reads within ~1s.
const READ_BARRIER_TIMEOUT_NS: u64 = 1_000_000_000;
/// Output-progress persist floor (Task 12 / spec §7): rate-limits the durable
/// `StableValue::store` (an fsync) to at most once per 100 ms even under a
/// change every cycle. The cheap in-page `output_completed` compare still runs
/// every cycle; only the durable persist (and its cnc mirror update) is
/// floored. A persist lag only WIDENS the next incarnation's at-least-once
/// replay window — never a correctness issue (see `NodeState`'s module doc).
const OUTPUT_PROGRESS_FLOOR_NS: u64 = 100_000_000;
/// Wire reason for a malformed/unknown admin op field (NOT a `ProposeError`:
/// codes 1-10 and 12 are the SM's; 11 is the node's own defensive catch-all,
/// previously a deliberate reuse of 6/NotFound).
const REASON_MALFORMED_OP: u32 = 11;

// ---- M12b: admin-authentication refusal reasons (spec §5.2) --------------
// Wire `reason` codes on an admin response whose `status` is 1 (refused).
// Disjoint from both the SM's `ProposeError` codes (1-10, 12) and the node's
// own `REASON_MALFORMED_OP` (11): a caller can tell "the cluster refused
// this change" from "the cluster refused to believe this was you" without
// consulting the policy. Only ever produced under `AdminPolicy::Hmac` —
// under `Filesystem` (the default) the auth line is not even read.
/// The policy is `Hmac` but the request carried no auth line at all
/// (`AdminAuth::ZERO`) — an unsigned client against a signed cluster.
pub const REASON_AUTH_MISSING: u32 = 20;
/// The auth line named a known key, but the HMAC tag does not verify over
/// this request's canonical bytes — a forged, tampered, or mis-signed
/// request (including one re-presented at a different `seq`).
pub const REASON_AUTH_BAD_TAG: u32 = 21;
/// The auth line's `expiry_ns` is outside the acceptance window: already
/// past (`expiry_ns <= now`), or implausibly far in the future
/// (`> now + 2 * ttl`) — a signed request whose validity window was stretched
/// by a clock game is refused just as hard as a stale one.
pub const REASON_AUTH_EXPIRED: u32 = 22;
/// The auth line's `key_name_hash` matches no key this node loaded — a
/// revoked key, a key this node has not been given, or a typo'd key name.
pub const REASON_AUTH_UNKNOWN_KEY: u32 = 23;
/// M12b §5.3: the admin audit record for this request could not be written
/// to `<instance_dir>/audit.jsonl` (a full or failing disk). Recording
/// precedes responding, so a node that cannot record refuses rather than
/// acting unaccountably. Unlike 20-23 this one is policy-independent — it can
/// occur under `Filesystem` too.
///
/// **It does not mean "nothing happened."** On the leader's accepted path the
/// config change has already been proposed and appended by the time the
/// record is attempted, so 24 means "this node cannot account for the
/// outcome" — check `uc2ctl status` for the live config version rather than
/// assuming the change was rejected. The node also logs `admin_audit_failed`
/// at `error` with the underlying io error.
pub const REASON_AUDIT_FAILED: u32 = 24;

/// Phase of an in-flight linearizable read (the ReadIndex barrier state
/// machine, spec §7 / v1 task14).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadPhase {
    /// Collecting distinct READ_PROBE_ACKs toward a quorum (the read-index
    /// confirmation that this leader still leads — the no-stale-read guarantee).
    AwaitQuorum,
    /// Quorum confirmed; waiting for the service to apply through `commit_at`.
    AwaitApplied,
}

/// One in-flight linearizable read, parked on the consensus agent between the
/// client's `query.ring` submission and the moment it is forwarded to the
/// service (or retried). The read index is `commit_at`, captured at admission.
struct PendingRead {
    client_id: u32,
    local_seq: u32,
    /// The raw query bytes (forwarded verbatim after `expected_epoch`).
    query: Vec<u8>,
    /// Rung A ordering gate (spec §3.2): the seq of the NEXT probe round at
    /// admission. A round with `round.seq >= round_seq` was issued after this
    /// read arrived and may certify it; a smaller/absent round may not.
    round_seq: u64,
    /// Read index: the commit position at admission. The read may only be
    /// answered once the service has applied at least this far.
    commit_at: u64,
    /// Absolute `now_ns` deadline; past it the read is retried.
    deadline_ns: u64,
    phase: ReadPhase,
}

/// The ingress admission door (Task 7, spec §7): open while the unconfirmed
/// backlog `append - commit` is within `budget`. A closed door leaves records
/// in the client ring for a later cycle (backpressuring the client's
/// `try_write` into `RingError::Full` once the ring itself fills) rather than
/// appending unboundedly ahead of quorum commit. `saturating_sub` makes the
/// transient `commit > append` snapshot (a stale/racy read across the two
/// independent atomics) open rather than panic.
#[inline]
fn admission_open(append: u64, commit: u64, budget: u64) -> bool {
    append.saturating_sub(commit) <= budget
}

/// The node's shared-memory IPC rings whose live halves are not otherwise
/// held, created fresh at every boot and kept for the node's life so the mmap'd
/// file stays live for the attaching service. `ingress`/`egress_node` (Task 7)
/// and now `query`/`svc_query` (Task 11) are split at boot: their node-side
/// halves (the ingress + query CONSUMERs, the egress_node + svc_query
/// PRODUCERs) are handed to the consensus agent, and the counterpart halves —
/// which attaching clients/service open the files for themselves — are dropped
/// in `create_rings`. `egress_service` is the service's producer ring; the node
/// only creates + retains the file so the service can open it.
#[allow(dead_code)]
struct Rings {
    egress_service: BroadcastRing,
}

pub struct Node {
    /// M10 (Task 4): this node's id, captured from `cfg.id` at boot — nothing
    /// else on `Node` retains it (the consensus SM has its own copy). Exposed
    /// via [`Node::observability`].
    node_id: NodeId,
    cnc: Arc<CncPage>,
    term_handle: TermHandle,
    leader_flag: Arc<AtomicBool>,
    can_serve_flag: Arc<AtomicBool>,
    ingress_tx: mpsc::SyncSender<Vec<u8>>,
    /// Ingress admission budget (`append - commit`), mirrored from
    /// `NodeConfig` so `submit` (the in-process path) enforces the same
    /// door as the client ring drain.
    admission_bytes: u64,
    buffer: Arc<LogBuffer>,
    truncations: Arc<AtomicU64>,
    /// M6 Task 8: count of wipe-and-rejoins (NoCommonPrefix → truncate-to-0). A
    /// subset of `truncations` (a wipe is also a truncate), tracked separately for
    /// observability and the wipe-safety tests.
    wipes: Arc<AtomicU64>,
    reports_implausible: Arc<AtomicU64>,
    /// Protocol 0.5.0: reports DECLINED because their content attestation
    /// (`durable_term`) disagreed with our own term map. Mirrored out of the
    /// SM each consensus duty cycle. Steady state is 0; brief nonzero runs
    /// around elections are expected (a follower's map catches up a round
    /// later). A SUSTAINED nonzero on a healthy cluster means honest reports
    /// are being declined — a liveness bug, or a mixed-version fleet (a
    /// pre-0.5.0 peer reports unattested and is never counted).
    reports_unattested: Arc<AtomicU64>,
    /// M6 Task 4: node-internal mirror of the archive's lowest replayable
    /// position (written by the archive agent). Exposed via
    /// [`Node::archive_first_base`] for purge-safety tests.
    archive_first_base: Arc<AtomicU64>,
    route_drops: Arc<uc2_net::receiver::FollowerStats>,
    /// M6 Task 9: the sender's stats (for the prefill-decision pin — a restarted
    /// leader serves a below-ring NAK from the journal, `replay_datagrams > 0`,
    /// rather than prefilling its ring).
    sender_stats: Arc<uc2_net::sender::SenderStats>,
    partition_handles: Vec<PartitionHandle>,
    /// Final-review fix (Item 1 test): a clone of the SAME `Arc<Mutex<Vec<u8>>>`
    /// the consensus agent's `config_bytes` field and the sender's
    /// `SnapshotSource` closure read — i.e. observing this from a test is
    /// observing exactly what a SNAP_BEGIN this node ships would carry, not a
    /// proxy for it. Exposed via [`Node::snapshot_config_bytes`].
    config_bytes: Arc<Mutex<Vec<u8>>>,
    /// M8 (Task 12): the newest group-key epoch this node has minted (0 =
    /// never). Written by the consensus agent; read by [`Node::crypto_epoch`].
    crypto_epoch: Arc<AtomicU32>,
    /// M8 Task 14 (adversarial tier): a clone of the same [`SharedTransport`]
    /// the consensus agent owns — `SharedTransport` is cheap to clone (an
    /// `Arc`-backed handle onto shared key state) and `is_established` reads
    /// through the same `Mutex` any other clone would, so this adds no new
    /// synchronization. `None` under [`CryptoConfig::Disabled`]. Exposed via
    /// [`Node::has_crypto_session_with`] for adversarial/integration tests
    /// that need to observe pairwise session state from outside the crate
    /// (e.g. "a peer revoked from the allowlist never re-establishes").
    crypto: Option<SharedTransport>,
    /// M8 Task 14: a clone of the same handshake-failure counter the
    /// consensus agent bumps on every refused `HS_INIT`/`HS_RESP` (bad
    /// claimed-id/transport-source binding, or a key not on the allowlist —
    /// `handshake.rs`'s `on_init`/`on_resp`). Exposed via
    /// [`Node::crypto_handshake_failures`] so an adversarial test can assert
    /// a forged or revoked handshake attempt was actually REFUSED, not just
    /// that it happened not to succeed within some timeout.
    crypto_handshake_failures: Arc<AtomicU64>,
    /// M10 (Task 4): mirrors `cfg.purge != PurgePolicy::Disabled` — neither
    /// retained elsewhere on `Node` (the policy itself lives on the consensus
    /// SM). Exposed via [`Node::observability`].
    purge_enabled: bool,
    /// M10 (Task 4): mirrors `cfg.journal_segment_bytes`, not otherwise kept
    /// on `Node`. Exposed via [`Node::observability`].
    journal_segment_bytes: u64,
    // Held for the node's life: the instance flock and the IPC ring mmaps.
    _instance: InstanceDir,
    _rings: Rings,
    agents: Vec<AgentRunner>,
}

impl Node {
    /// Recover state, prime counters, and spawn the four agents. Every node
    /// boots a FOLLOWER — leadership only ever comes from an election. Binds a
    /// fresh socket at `cfg.bind` and takes the default [`StartOpts`]
    /// (filesystem admin policy — the pre-M12b posture).
    pub fn start(cfg: NodeConfig) -> io::Result<Node> {
        Self::start_with(cfg, StartOpts::default())
    }

    /// As [`start`](Self::start) but over a pre-bound socket (the 3-node harness
    /// binds every node's socket first, then hands each in — so peers know all
    /// addresses before any agent runs).
    pub fn start_with_socket(cfg: NodeConfig, sock: UdpSocket) -> io::Result<Node> {
        Self::start_with(cfg, StartOpts { socket: Some(sock), ..Default::default() })
    }

    /// The one real constructor: [`start`](Self::start) and
    /// [`start_with_socket`](Self::start_with_socket) are thin wrappers over
    /// it. M12b added [`StartOpts::admin`] here rather than to [`NodeConfig`]
    /// deliberately — an [`AdminPolicy`] holds live key material, which has no
    /// business in a `Clone`-able, config-file-shaped struct that tests and
    /// harnesses copy around.
    pub fn start_with(cfg: NodeConfig, opts: StartOpts) -> io::Result<Node> {
        let StartOpts { socket, admin } = opts;
        let sock = match socket {
            Some(sock) => sock,
            None => UdpSocket::bind(cfg.bind)?,
        };
        let self_addr = sock.local_addr()?;

        // 1. flock FIRST — one node per instance dir. A contended lock (a live
        // node already owns this dir) surfaces as an io error whose Display
        // carries "AlreadyRunning" (the harness matches on it).
        // M8 (Task 12): build the crypto plane FIRST — ahead of the instance
        // flock, the archive, the cnc page, the rings, and every agent spawn.
        // An `Enabled` config whose key/allowlist file is missing, malformed
        // or group/world-readable is a clean early return, the same shape as
        // the M7 self-tombstone refusal below: an orchestrator sees a failed
        // unit rather than a node quietly running in the clear. Placed before
        // the flock specifically so a misconfigured node leaves NOTHING
        // behind — no lock, no instance files, nothing to clean up.
        // `Disabled` yields `None` and nothing downstream changes.
        let crypto = SharedTransport::new(&cfg.crypto, cfg.id).map_err(|e| {
            io::Error::other(format!(
                "node {} refuses to start: crypto is enabled but its key material is \
                 unusable ({e}); a node configured to authenticate must never fall back \
                 to cleartext",
                cfg.id
            ))
        })?;

        let instance = InstanceDir::acquire(&cfg.instance_dir).map_err(to_io)?;

        // M12b (spec §5.3): the admin audit file, opened for append BEFORE
        // any agent runs — a node that cannot record what it was told to do
        // does not start. Placed after the flock so the file belongs to the
        // instance directory this node actually owns.
        let audit = AuditLog::open(&instance.root).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "node {} refuses to start: cannot open the admin audit log {} ({e})",
                    cfg.id,
                    instance.root.join(crate::audit::AUDIT_FILE).display()
                ),
            )
        })?;

        // Three clones of the one node socket: the receiver recvs (and sends
        // follower-role control), the sender streams, the consensus agent sends
        // votes/gossip/term-maps/elections. Each wraps in a `FaultSocket` so its
        // sends honor the same seeded faults AND are partitionable.
        let mut recv_sock = FaultSocket::from_socket(sock.try_clone()?)?;
        let mut send_sock = FaultSocket::from_socket(sock.try_clone()?)?;
        let mut cons_sock = FaultSocket::from_socket(sock)?;
        recv_sock.set_faults(cfg.faults);
        send_sock.set_faults(cfg.faults);
        cons_sock.set_faults(cfg.faults);
        let partition_handles = vec![
            recv_sock.partition_handle(),
            send_sock.partition_handle(),
            cons_sock.partition_handle(),
        ];

        // 2. Recover durable state from the journal.
        let mut archive_cfg = ArchiveConfig {
            segment_size_bytes: cfg.journal_segment_bytes,
            durability: journal_durability_from_env().map_err(to_io)?,
            ..ArchiveConfig::new(instance.journal_dir())
        };
        if let Some(iv) = eventual_fsync_interval_from_env(archive_cfg.durability)
            .map_err(to_io)?
        {
            archive_cfg.eventual_fsync_interval = iv;
        }
        // Invariant: a block records as ONE journal record and must fit within a
        // segment. Keep the block cap comfortably below the segment size (never
        // above the production 1 MiB default). This lets tests shrink segments
        // to a few KiB to observe purge without the block cap overflowing them.
        archive_cfg.max_block_bytes = archive_cfg
            .max_block_bytes
            .min((cfg.journal_segment_bytes / 2) as usize)
            .max(4096);
        let mut archive = Archive::open(archive_cfg).map_err(to_io)?;
        let durable = archive.recovered_position();
        if std::env::var("UC2_TRUNC_TRACE").is_ok() {
            eprintln!("[trunc-trace n{}] BOOT recovered_durable={durable}", cfg.id);
        }
        // M6 Task 4: node-internal mirror of the archive's lowest replayable
        // position. Written by the archive agent (after a purge), read by the
        // consensus agent (the purge guard: never issue a purge that wouldn't
        // advance the floor) and exposed via `Node::archive_first_base` for tests.
        let archive_first_base = Arc::new(AtomicU64::new(archive.first_base()));
        let state = NodeState::open(&instance.state_dir()).map_err(to_io)?;

        // Recovery re-derivation (T4 carry 4): if the persisted term map does not
        // cover the durable journal frontier — a crash after the bytes fsynced
        // but before the term-map store landed — re-derive the missing stamps by
        // scanning journal frame headers, and persist the completed map BEFORE
        // the SM reads it. Closes the bytes-fsynced / map-lost window so the
        // vote credentials and reconciliation start from ground truth.
        let recovered_map = state.term_map();
        let rederived = rederive_term_map(&archive, &recovered_map).map_err(to_io)?;
        if rederived != to_pairs(&recovered_map) {
            state.store_term_map(&to_entries(&rederived)).map_err(to_io)?;
        }

        // 3. Re-create the cnc v2 page EVERY boot with a fresh random
        // `instance_id` (invalidates any stale attachment), then prime the
        // counters (which live cast onto the page) to the recovery point.
        let instance_id = rand::random::<u128>();
        let meta = CncMeta {
            node_id: cfg.id,
            instance_id,
            app_id: cfg.app_id.clone(),
            buffer_bytes: cfg.buffer_bytes as u64,
            max_payload: cfg.max_payload as u32,
        };
        let cnc = CncPage::create_file(&instance.cnc_path(), &meta).map_err(to_io)?;
        cnc.counters().prime(durable);

        // 4. Log buffer file: reuse the existing file when it already matches the
        // configured capacity (preserves ring bytes below `durable` across a
        // restart — free NAK-serving prefill), else create it fresh.
        let buffer = Arc::new(open_or_create_buffer(
            &instance.log_path(),
            cfg.buffer_bytes as u64,
            Arc::clone(&cnc),
            cfg.max_payload,
        )?);

        // 5. Durable output-progress marker (Task 12) → mirror onto the page for
        // attaching parties. `state.output_progress()` is whatever the output
        // loop last durably persisted (0 for a fresh instance dir, or if the
        // output loop has never run); the service's output agent reads this
        // SAME mirror at attach to seed its cursor (spec §7).
        let output_progress = state.output_progress();
        cnc.status().output_progress.store_release(output_progress);
        // M6 Task 4: same durable-then-mirror seeding for the snapshot floor —
        // the recovered value onto the fresh cnc page so an attaching reader
        // sees the real floor immediately (not 0), and the persister's
        // increase-only shadow starts from it.
        let state_snapshot_floor = state.snapshot_floor();
        cnc.snapshots().node_snapshot_floor.store_release(state_snapshot_floor);

        // 6. Rings created fresh each boot (stale files unlinked first — any
        // prior attachment is invalidated by the new instance_id anyway).
        // `ingress`/`egress_node` are pre-split: the consensus agent is the
        // sole owner of the consumer/producer half it drives (Task 7).
        let (rings, ingress_ring, egress_node, query_ring, svc_query) = create_rings(&instance)?;

        // M7 (spec 2026-07-13): recover the durable `ConfigRecord` — genesis-seed
        // on a fresh instance dir, T5-carry revert of a record ahead of durable,
        // and Step 3a forward re-derivation from the archive's own retained
        // CONFIG frames. See `recover_config_record`'s doc for the full rationale;
        // extracted to a free function so it's unit-testable without a full
        // two-thread `Node`.
        let config_rec =
            recover_config_record(&state, &archive, durable, &cfg.members, &cfg.learners)
                .map_err(to_io)?;

        let config = stored_to_cluster(&config_rec.config);
        // Post-M7 follow-up: a node whose OWN id is tombstoned in the
        // recovered config can never rejoin under this id (fresh-forever
        // ids) and would otherwise boot as a permanently-idle zombie — the
        // runtime HaltRemoved latch cannot re-fire (adoption is version-
        // gated; no higher-version ConfigObserved ever arrives for an
        // already-adopted removal). Fail loudly at construction: an
        // orchestrator sees a failed unit, not a healthy idle one. (The T8
        // truncation-revert edge — a durable-but-uncommitted self-removal
        // later truncated cluster-wide — previously recovered via restart;
        // its recourse is now wipe-and-rejoin, documented in the runbook.)
        if config.tombstones.contains(&cfg.id) {
            return Err(io::Error::other(format!(
                "node id {} is tombstoned in the recovered cluster config (v{}): \
                 this id was permanently removed and can never rejoin; \
                 decommission this instance dir, or wipe it and rejoin with a fresh id",
                cfg.id, config.version
            )));
        }
        let prev_config = stored_to_cluster(&config_rec.prev);
        // M7 Task 6: mirror the recovered version onto the FRESH cnc page
        // immediately — same durable-then-mirror discipline as the snapshot
        // floor / output-progress markers just above (`cnc` is re-created on
        // every boot, so without this an attaching reader sees a stale `0` for
        // an entire duty cycle even when the recovered record is not genesis).
        cnc.store_config_version(config.version);
        cnc.store_admission_bytes(cfg.admission_bytes);

        // M7 Task 6: the snapshot-session config-carry cache — the encoded
        // CURRENT `ConfigRecord.config` (`v2::config::encode_config` bytes), read
        // by the sender's `SnapshotSource` closure at ship time and refreshed by
        // `Action::ConfigAdopted`'s exec arm on every adoption (forward, revert,
        // or boot re-derivation alike). Seeded here from the just-recovered
        // record so a snapshot shipped before the first live adoption still
        // carries real bytes rather than an empty placeholder.
        let config_bytes =
            Arc::new(Mutex::new(config_wire_bytes(&config, config_rec.prev_position)));

        // Election SM over the recovered credentials + the recovered config.
        // M6 Task 7 / M7: a node whose own id is a learner in the ADOPTED
        // config boots in learner mode — replicated-to, never counted.
        // `ElectionSm` derives `can_vote` from `config.is_voter(id)` itself now
        // (M7 migration off the old `members`/`can_vote` `ElectionConfig` fields).
        let is_learner = config.is_learner(cfg.id);
        assert!(
            !is_learner || !config.is_voter(cfg.id),
            "a learner's id must not also be a voting member (id={})",
            cfg.id
        );
        let recovered_vote = state.vote().map(|v| (v.term, v.voted_for));
        let mut sm = ElectionSm::new(
            ElectionConfig {
                id: cfg.id,
                config: config.clone(),
                config_position: config_rec.position,
                election_timeout_min_ns: cfg.election_timeout_min_ns,
                election_timeout_max_ns: cfg.election_timeout_max_ns,
                // Idle re-gossip floor (spec §6): re-ship commit + term map every
                // 100ms even when commit is plateaued, so a divergent node
                // rejoining an idle cluster still reconciles. Not a NodeConfig
                // knob — the value is a protocol constant, not deployment-tuned.
                gossip_floor_ns: 100_000_000,
                seed: cfg.seed,
            },
            recovered_vote,
            &rederived,
            durable,
            0,
        );
        #[cfg(feature = "mutation-testing")]
        match crate::mutation::active() {
            Some(crate::mutation::Mutation::CommitQuorumMinusOne) => {
                sm.set_mutate_quorum_minus_one(true)
            }
            Some(crate::mutation::Mutation::SkipVoteOrderCheck) => {
                sm.set_mutate_skip_vote_order(true)
            }
            _ => {}
        }
        // Seed the recovered PREV level (T4/T5): a no-op identity restore at
        // genesis (`prev == config`, both at position 0) — real content only
        // when a prior life actually adopted a config.
        sm.restore_prev_config(prev_config, config_rec.prev_position);
        let boot_term = sm.current_term();

        // Shared, consensus-thread-written role snapshots + the term handle.
        let term_handle: TermHandle = Arc::new(AtomicU32::new(boot_term));
        let leader_flag = Arc::new(AtomicBool::new(false));
        let can_serve_flag = Arc::new(AtomicBool::new(false));
        // Finding #5 (lean gate doc 2026-07-16, leader-completeness effort):
        // boot-open gate + persisted vote over an unreconciled divergent tail =
        // phantom commit; boot closed iff vote_term > map_term — reconcile must
        // complete before this node's reports may certify. Term recovery is
        // `max(vote_term, map_term)` (`ElectionSm::new`), so a voter that
        // granted term T (vote persisted) and crashed BEFORE reconciling
        // reboots AT term T holding a tail the T-leader never validated; with
        // the gate open its 20 ms AppendPosition floor report
        // (`receiver.rs`) races the leader's 100 ms idle map re-ship and can
        // certify a commit over content it does not hold. `map_term >=
        // vote_term` is the safe complement: the map grows only via
        // DataTermObserved / become_leader, so a tail whose last mapped term
        // reaches the vote term was validated under that term's leader.
        // Reopen rides the EXISTING arms only (clean-reconcile, truncate-ack,
        // BecomeLeader): liveness cost is one extra reconcile round.
        let vote_term = recovered_vote.map(|(t, _)| t).unwrap_or(0);
        let map_term = rederived.last().map(|&(t, _)| t).unwrap_or(0);
        let boot_awaiting_reconcile = vote_term > map_term;
        let intake_gate = Arc::new(AtomicBool::new(!boot_awaiting_reconcile));
        let truncations = Arc::new(AtomicU64::new(0));
        let wipes = Arc::new(AtomicU64::new(0));
        let reports_implausible = Arc::new(AtomicU64::new(0));
        let reports_unattested = Arc::new(AtomicU64::new(0));
        // M8 (Task 12): the newest group epoch this node has minted, mirrored
        // off the consensus agent for `Node::crypto_epoch`.
        let crypto_epoch = Arc::new(AtomicU32::new(0));
        // M8 Task 14: named (rather than inlined at the `Consensus` literal)
        // so `Node` can hold its own clone for `Node::crypto_handshake_failures`.
        let crypto_handshake_failures = Arc::new(AtomicU64::new(0));

        // Peer maps and the follower set, derived from the adopted config —
        // shared with `Consensus::rebuild_peer_maps` (M7's live-reconfiguration
        // rebuild) via the free `derive_peer_maps` helper (this call happens
        // before any `Consensus` exists, so it cannot be a method call yet).
        let (id_to_addr, addr_to_id, peers, learner_ids, peer_band) =
            derive_peer_maps(&config, cfg.id);
        let learner_addrs: Vec<SocketAddr> =
            learner_ids.iter().map(|id| id_to_addr[id]).collect();
        // Leader fan-out = voters-minus-self ++ learners-minus-self (streamed
        // identically); the learner subset is excluded from flow control.
        let voting_followers: Vec<SocketAddr> = peers.iter().map(|id| id_to_addr[id]).collect();
        let followers: Vec<SocketAddr> =
            voting_followers.iter().chain(learner_addrs.iter()).copied().collect();

        // Channels.
        let (net_tx, net_rx) = mpsc::sync_channel::<NetEvent>(NET_EVENT_CAPACITY);
        let (ctrl_tx, ctrl_rx) = mpsc::sync_channel::<CtrlMsg>(1024);
        let (ingress_tx, ingress_rx) = mpsc::sync_channel::<Vec<u8>>(INGRESS_CAPACITY);
        let (obs_tx, obs_rx) = mpsc::sync_channel::<(u32, u64)>(1024);
        // M7: durably-recorded CONFIG-frame observations, `(frame-END position,
        // payload bytes)` — the archive agent's config scan (`take_config_observations`)
        // forwards here; the consensus agent decodes + feeds `Event::ConfigObserved`
        // (do_work step 1c). Same shape/rationale as `obs_tx`/`obs_rx` above.
        let (cfg_obs_tx, cfg_obs_rx) = mpsc::sync_channel::<(u64, Vec<u8>)>(1024);
        // Truncation command channel carries `(epoch, to)`; the ack rides an
        // infallible single slot (one truncation in flight — the SM latch).
        let (trunc_tx, trunc_rx) = mpsc::sync_channel::<ArchiveCmd>(64);
        let trunc_slot = TruncationSlot::default();
        // Issue #6: the leader-open collapse rides the SAME command channel (so
        // the archive applies both cuts in emission order) but its OWN ack slot
        // — a reconcile truncation can still be in flight when an election is
        // won, and one `TruncationSlot` holds exactly one ack.
        let collapse_slot = TruncationSlot::default();

        // Sender (streams when leader; commit ranking is entirely the
        // consensus agent's job — the sender never ranks or gossips commit).
        // M8 (Task 12): hand each half out EXACTLY ONCE, here, and give one to
        // each agent. `SharedTransport::send_half`/`receive_half` panic on a
        // second call (a second `SendHalf` would restart the nonce counter
        // under a live key; a second `ReceiveHalf` would split the replay
        // windows) — this is the one place in the process that calls either.
        let crypto_send = crypto.as_ref().map(|c| c.send_half());
        let crypto_recv = crypto.as_ref().map(|c| c.receive_half());
        // The handshake plane (kinds 18/19/20): the receiver demuxes them off
        // the socket, the consensus agent drives `Peers`/`GroupPlane`.
        let (hs_tx, hs_rx) = mpsc::sync_channel::<HandshakeDatagram>(HANDSHAKE_CAPACITY);
        // The live sender-identity map the receive seam resolves peers
        // through, seeded from the RECOVERED config (not the possibly-stale
        // seed) and republished by the consensus agent on every adoption.
        let crypto_peer_ids = crypto.as_ref().map(|_| {
            let ids = PeerIds::new();
            ids.store(addr_to_id.iter().map(|(a, i)| (*a, *i)).collect::<Vec<_>>());
            ids
        });

        let mut sender_cfg = SenderConfig::new(boot_term);
        sender_cfg.crypto_enabled = crypto_send.is_some();
        sender_cfg.heartbeat_ns = 20_000_000; // 20 ms: brisk tail-loss detection
        let journal = archive.journal_arc();
        // A learner never leads, so its sender streams to no one: give it a solo
        // (empty) fan-out with a cluster size of 1, which also sidesteps flow
        // control's leader-in-cluster invariant (from a learner's view every voter
        // is a follower, so `voters == cluster_size` would trip the assert). A
        // voter's sender gets the real fan-out = voters-minus-self ++ learners,
        // sized via `sender_cluster_size` (M7 Task 8) rather than the possibly-
        // stale seed's `cfg.members.len()` — the RECOVERED `config` is the
        // authoritative source (a restart after live reconfiguration may have a
        // materially different voter count than the seed), and using the seed
        // count would panic `FlowControl::new` for a node that recovers as a
        // genuine non-voter (a stale-seed joiner not yet in ANY list).
        let (sender_followers, sender_learners, sender_cluster) = if is_learner {
            (Vec::new(), Vec::new(), 1)
        } else {
            (followers, learner_addrs.clone(), sender_cluster_size(&config, cfg.id))
        };
        let mut sender = Sender::with_crypto(
            Arc::clone(&buffer),
            send_sock,
            sender_followers,
            &sender_learners,
            sender_cluster,
            ctrl_rx,
            sender_cfg,
            Arc::clone(&term_handle),
            Arc::clone(&leader_flag),
            // M8 (Task 17): the half travels with the live peer-id map — the
            // snapshot session's `SNAP_BEGIN`/`SNAP_CHUNK` are `Scope::Pairwise`
            // and need a `NodeId` for the destination. See `SenderCrypto`.
            crypto_send.map(|half| SenderCrypto {
                half,
                peer_ids: crypto_peer_ids.clone().expect("peer ids exist iff crypto does"),
            }),
        );
        sender.set_replay_source(journal);
        // M6 Task 6: snapshot session wiring. `snap_dir` holds the position-tagged
        // artifacts (shared with the service's builder); `incoming_snapshot` is the
        // node-internal signal the receiver raises on a completed inbound transfer.
        let snap_dir = cfg.instance_dir.join("snapshots");
        let _ = std::fs::create_dir_all(&snap_dir);
        let incoming_snapshot = Arc::new(AtomicU64::new(0));
        // M7 Task 6: companion cell for `incoming_snapshot` — the encoded config
        // carried by the SAME completed inbound transfer (`SnapBeginBody.config`),
        // stashed by the receiver in `snap_complete` and consumed by the
        // consensus agent's `maybe_adopt_incoming_snapshot`.
        let incoming_snapshot_config = Arc::new(Mutex::new(Vec::new()));
        // M6 Task 9 (straddle hardening): bumped by the archive agent AFTER each
        // `LogCounters::prime(to)` (truncate / AdoptFloor). The receiver samples it
        // around a DATA datagram to detect a prime that straddled its processing and
        // drop the stale frontier rather than clobber the freshly primed floor.
        let prime_generation = Arc::new(AtomicU64::new(0));
        // Offer ONLY the file at the node's durable floor: a session ships a
        // fully-published artifact (rename-atomic + validated as the floor marker).
        let src_cnc = Arc::clone(&cnc);
        let src_dir = snap_dir.clone();
        // M7 Task 6: the same cell `Action::ConfigAdopted`'s exec arm refreshes —
        // ships whatever config is CURRENT at the moment a peer's NAK opens a
        // session, never a boot-time snapshot of it.
        let src_config_bytes = Arc::clone(&config_bytes);
        sender.set_snapshot_source(Arc::new(move || {
            let floor = src_cnc.snapshots().node_snapshot_floor.load_acquire();
            if floor == 0 {
                return None;
            }
            let path = src_dir.join(format!("snap-{floor}.ultsnap"));
            let len = std::fs::metadata(&path).ok()?.len();
            let config = src_config_bytes.lock().unwrap().clone();
            Some((floor, path, len, config))
        }));

        // M6 Task 9: the per-peer observability band, cnc-slot order (voters
        // first, then learners), capped at the fixed slot count — already
        // derived above (`derive_peer_maps`). The consensus agent owns
        // `id_and_role` + `reported_durable`; the sender fills
        // `advertised_limit` from its flow-control view (bounded, once per cycle).
        let sender_peer_slots: Vec<(SocketAddr, usize)> =
            peer_band.iter().enumerate().map(|(i, (id, _))| (id_to_addr[id], i)).collect();
        sender.set_peer_slots(Arc::clone(&cnc), sender_peer_slots);

        // Receiver (unified follower-receiver + leader-control demux).
        let mut rcfg = FollowerConfig::new(self_addr); // auto-learns the real leader from DATA
        rcfg.seed = cfg.seed ^ 0x5DEE_CE66_1D0C_2A11;
        rcfg.status_floor_ns = 20_000_000;
        rcfg.append_pos_floor_ns = 20_000_000;
        // M8 (Task 12): the receive half, the sender-identity map, and the
        // handshake route travel together in one `CryptoIntake` — forgetting
        // any of the three is a compile error, not a silent cluster-wide
        // `dropped_unknown_peer`/`dropped_handshake`.
        // M8 (Task 17) adds the fourth piece: the `SharedTransport` itself,
        // for SEALING the receiver's own `NAK`/`STATUS`/`APPEND_POSITION`/
        // `SNAP_NAK`/`SNAP_DONE`. A clone, never a second `SendHalf` — see
        // `CryptoIntake::transport`.
        let receiver_crypto = crypto_recv.map(|half| CryptoIntake {
            half,
            peer_ids: crypto_peer_ids.clone().expect("peer ids exist iff crypto does"),
            handshake: hs_tx,
            transport: crypto.clone().expect("the receive half exists iff crypto does"),
        });
        let mut receiver = FollowerReceiver::with_crypto(
            Arc::clone(&buffer),
            recv_sock,
            rcfg,
            Arc::clone(&term_handle),
            net_tx,
            receiver_crypto,
        );
        // Cloned: the consensus agent keeps its own producer half to drive
        // `CtrlMsg::SetPeers` (M7 config adoption, `Consensus::exec`).
        receiver.set_sender_route(ctrl_tx.clone());
        receiver.set_intake_gate(Arc::clone(&intake_gate));
        receiver.set_snapshot_intake(
            snap_dir.clone(),
            Some((Arc::clone(&incoming_snapshot), Arc::clone(&incoming_snapshot_config))),
        );
        receiver.set_prime_generation(Arc::clone(&prime_generation));
        // Validated frontier, published by the consensus agent and read by the
        // receiver so its AppendPosition reports attest validated bytes only.
        let validated_frontier = Arc::new(AtomicU64::new(durable));
        let cons_validated = Arc::clone(&validated_frontier);
        receiver.set_validated_frontier(Arc::clone(&validated_frontier));
        // Its content attestation (protocol 0.5.0): the term covering the byte
        // below the frontier. Published together with it; a torn pair only
        // fails the leader's check and is re-sent on the next cadence.
        let validated_term = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let cons_validated_term = Arc::clone(&validated_term);
        receiver.set_validated_term(Arc::clone(&validated_term));
        let route_drops = receiver.stats();

        // Archive agent: archive commands first (don't record blocks about to be
        // dropped/purged), then record, then ship data-stamped term observations.
        let arc_buffer = Arc::clone(&buffer);
        let arc_cnc = Arc::clone(&cnc);
        let arc_slot = trunc_slot.clone();
        let arc_collapse_slot = collapse_slot.clone();
        let arc_first_base = Arc::clone(&archive_first_base);
        let arc_prime_gen = Arc::clone(&prime_generation);
        // Forensic trace for the 2026-08-16 acked-write-loss hunt: every journal
        // cut is rare (elections only), so an env-gated line per cut is free.
        let trunc_trace = std::env::var("UC2_TRUNC_TRACE").is_ok();
        let trace_id = cfg.id;
        // Diagnostic only: the consensus thread publishes its commit
        // provenance here each duty cycle so the archive thread's cut trace
        // can name where the commit it is cutting below came from.
        let trace_prov: Arc<Mutex<(&'static str, u32, u64)>> =
            Arc::new(Mutex::new(("none", 0, 0)));
        // Term-observation frontier: the archive agent publishes it after
        // handing observations to the consensus agent (see `refresh_durable`).
        // Seeded at the recovered position: at boot the map was recovered (and
        // re-derived) from the journal, so it already describes everything we
        // hold — clamping to 0 here would freeze the SM's durable until the
        // first archive cycle.
        let obs_frontier = Arc::new(AtomicU64::new(durable));
        let arc_obs_frontier = Arc::clone(&obs_frontier);
        let cons_obs_frontier = Arc::clone(&obs_frontier);
        let cons_trace_prov = Arc::clone(&trace_prov);
        let archive_agent = AgentRunner::spawn("uc2-archive", IdleStrategy::Yield, move || {
            let mut did = false;
            while let Ok(cmd) = trunc_rx.try_recv() {
                match cmd {
                    ArchiveCmd::Truncate { epoch, to } => {
                        if trunc_trace {
                            eprintln!(
                                "[trunc-trace n{}] RECONCILE cut to={to} pre_durable={} cnc_commit={} prov={:?}",
                                trace_id,
                                archive.recovered_position(),
                                arc_cnc.counters().commit.load_acquire(),
                                trace_prov.lock().map(|p| *p).unwrap_or(("lock", 0, 0)),
                            );
                        }
                        // First-block cuts (a contested first election, `to`
                        // at/inside block 0) are handled by the archive via
                        // `Journal::truncate_all` + prefix re-seed (M4 carry #3)
                        // and no longer fail-stop. Any remaining error is a
                        // genuine journal I/O fault — still fatal.
                        archive
                            .truncate_to(to)
                            .expect("archive truncate fail-stop (journal I/O)");
                        arc_cnc.counters().prime(to);
                        // Publish the prime so a straddling receiver drops the stale
                        // frontier instead of clobbering `to` (M6 Task 9).
                        arc_prime_gen.fetch_add(1, Ordering::Release);
                        // A truncation can drop the first block (first-block cut);
                        // republish the floor so the consensus guard stays honest.
                        arc_first_base.store(archive.first_base(), Ordering::Release);
                        // Infallible ack: a single slot cannot drop (one in flight).
                        arc_slot.post(epoch, to);
                    }
                    ArchiveCmd::Collapse { epoch, to } => {
                        // Issue #6: physically the same cut as `Truncate`, on
                        // the same thread, for the same reason — but acked on
                        // the collapse slot so the two brackets never share one
                        // single-ack slot. In the common (non-racy) leader open
                        // `to == archive.durable_pos` and `truncate_to` is a
                        // cheap `Ok(())` no-op; the prime is what leader open
                        // has always needed.
                        //
                        // CLAMP — DEAD DEFENCE, deliberately kept. `to` is
                        // `ElectionSm::durable`, which is clamped to a pending
                        // reconcile truncation's cut only when that cut's
                        // `Event::Truncated` ack is fed back. So a node that had
                        // a `Truncate { to: T }` in flight AND then opened a
                        // leader term would send `to > T`, the archive would
                        // apply them in channel order, and the second cut would
                        // answer `PositionPurged` — killing this agent and
                        // leaving the node a silent non-serving leader-elect
                        // (gate closed, no appender, and the SM already thinks it
                        // leads, so nothing re-elects).
                        //
                        // That interleaving is UNREACHABLE — a reconcile-
                        // truncating node cannot win an election; see
                        // `a_reconcile_truncating_node_cannot_also_open_a_leader_term`
                        // for the two independent reasons and for the guard that
                        // fails loudly if that ever stops being true. The clamp
                        // costs one `min` and converts that wedge into a benign
                        // subsumption (the earlier cut already removed everything
                        // above `T`), so the unreachable case degrades instead of
                        // fail-stopping. Ack the position ACTUALLY cut to, so the
                        // node opens its term at the real frontier.
                        let to = to.min(archive.recovered_position());
                        if trunc_trace {
                            eprintln!(
                                "[trunc-trace n{}] COLLAPSE cut to={to} pre_durable={} cnc_commit={}",
                                trace_id,
                                archive.recovered_position(),
                                arc_cnc.counters().commit.load_acquire(),
                            );
                        }
                        archive
                            .truncate_to(to)
                            .expect("archive leader-open collapse fail-stop (journal I/O)");
                        arc_cnc.counters().prime(to);
                        arc_prime_gen.fetch_add(1, Ordering::Release);
                        arc_first_base.store(archive.first_base(), Ordering::Release);
                        arc_collapse_slot.post(epoch, to);
                    }
                    ArchiveCmd::Purge { below } => {
                        // Best-effort: a failed purge logs + drops (retries next
                        // interval). Correctness never depends on a block still
                        // being present — a reader below the floor recovers via
                        // the snapshot path (Task 5 same-host, Task 6 remote).
                        match archive.purge_below(below) {
                            Ok(new_first) => {
                                arc_first_base.store(new_first, Ordering::Release);
                            }
                            Err(e) => {
                                eprintln!(
                                    "uc2_node: archive purge_below({below}) failed: {e} \
                                     (dropped; retries next interval)"
                                );
                            }
                        }
                    }
                    ArchiveCmd::AdoptFloor { pos } => {
                        // M6 Task 6: a learner installed the snapshot at `pos`.
                        // Advance the archive floor with no bytes and prime the
                        // counters there so the live stream (positions >= pos) is
                        // accepted. A conflict (real data below pos) logs + drops.
                        match archive.adopt_floor(pos) {
                            Ok(new_floor) => {
                                arc_cnc.counters().prime(new_floor);
                                arc_prime_gen.fetch_add(1, Ordering::Release);
                                arc_first_base.store(new_floor, Ordering::Release);
                            }
                            Err(e) => {
                                eprintln!("uc2_node: archive adopt_floor({pos}) failed: {e}");
                            }
                        }
                    }
                }
                did = true;
            }
            if archive.do_work(&arc_buffer).expect("archive fail-stop") {
                did = true;
            }
            // LOSSLESS since 2026-08-16. This was `let _ = try_send(obs)`,
            // justified as "term observations are idempotent and re-derivable
            // from commit gossip" — they are NOT re-derivable: `observe_terms`
            // scans each block exactly once on the recording pass, so a
            // dropped observation is gone until a full journal re-scan at
            // restart. An election storm bursts many NewTerm frames into one
            // block, overflows the channel, and leaves the node's term map
            // permanently missing those terms while it holds their bytes.
            // Every later reconcile then reads the leader's newer entries as
            // divergence and truncates bytes the node received from that very
            // leader — committed and applied ones included. Retain on a full
            // channel and retry next duty cycle, exactly like the config
            // observations below.
            let mut pending_obs = archive.take_term_observations();
            let mut unsent = Vec::new();
            for (i, obs) in pending_obs.drain(..).enumerate() {
                if !unsent.is_empty() {
                    unsent.push(obs); // preserve position order once blocked
                    continue;
                }
                match obs_tx.try_send(obs) {
                    Ok(()) => did = true,
                    Err(mpsc::TrySendError::Full(o)) => {
                        let _ = i;
                        unsent.push(o);
                    }
                    // Receiver gone (shutdown): stop feeding.
                    Err(mpsc::TrySendError::Disconnected(_)) => break,
                }
            }
            if unsent.is_empty() {
                // Everything observed so far is now in the consensus agent's
                // hands: publish the frontier those observations describe.
                // Ordering is the whole point — this store happens AFTER the
                // handoff, so a reader that sees it also sees the map entries.
                arc_obs_frontier.store(archive.recovered_position(), Ordering::Release);
            } else {
                archive.retain_term_observations(unsent);
            }
            // M7: forward durably-recorded CONFIG-frame observations (position-
            // ordered, detected in the same scan as the term observations above
            // via `Archive::observe_terms`).
            //
            // Post-M7 loose-end T5: deliver these LOSSLESSLY, unlike the term
            // observations above. Config observations are RARE (one per
            // single-server membership change, gated by `config_pending`) and
            // emitted EXACTLY ONCE — `observe_terms` detects each CONFIG frame on
            // the recording pass only and never re-derives it, so a dropped
            // observation is lost until the next full journal re-scan (a restart),
            // silently running stale membership with no steady-state repair path.
            // Term observations, by contrast, are idempotent and re-derivable from
            // commit gossip, so their `try_send` drop above is intentionally
            // tolerated. Here we block until the node's drain (`cfg_obs_rx`, a
            // DIFFERENT thread's `do_work`, non-blocking `try_recv`) makes space;
            // the 1024-deep channel only fills if that drain has wedged — itself a
            // bug — and stalling the archive agent is strictly safer than a silent
            // membership divergence (a stalled node is detectable; a diverged one
            // is not). A send error means the receiver is gone (node shutting
            // down): stop feeding.
            for obs in archive.take_config_observations() {
                if cfg_obs_tx.send(obs).is_err() {
                    break;
                }
                did = true;
            }
            did
        })?;

        let sender_stats = sender.stats();
        let sender_agent =
            AgentRunner::spawn("uc2-sender", IdleStrategy::Yield, move || sender.do_work())?;
        let receiver_agent =
            AgentRunner::spawn("uc2-receiver", IdleStrategy::Yield, move || receiver.do_work())?;

        // Consensus agent (the single writer of the term handle + commit counter).
        let mut consensus = Consensus {
            reports_unattested: Arc::clone(&reports_unattested),
            validated_frontier: cons_validated,
            validated_term: cons_validated_term,
            obs_frontier: cons_obs_frontier,
            pending_obs: Vec::new(),
            trace_prov: cons_trace_prov,
            trunc_trace,
            id: cfg.id,
            sm,
            state,
            cnc: Arc::clone(&cnc),
            buffer: Arc::clone(&buffer),
            appender: None,
            next_corr: 0,
            pending_ingress: None,
            ingress_ring,
            egress_node,
            query_ring,
            svc_query,
            pending_reads: Vec::new(),
            current_round: None,
            next_round_seq: 1,
            next_nonce: 0,
            admission_bytes: cfg.admission_bytes,
            pending_ring_ingress: None,
            sock: cons_sock,
            id_to_addr,
            addr_to_id,
            peers,
            learner_ids,
            peer_band,
            peer_reported: HashMap::new(),
            peer_band_published: false,
            net_rx,
            obs_rx,
            cfg_obs_rx,
            ingress_rx,
            trunc_tx,
            trunc_slot,
            collapse_slot,
            pending_leader_open: None,
            next_collapse_epoch: 0,
            sender_ctrl: ctrl_tx,
            term_handle: Arc::clone(&term_handle),
            leader_flag: Arc::clone(&leader_flag),
            can_serve_flag: Arc::clone(&can_serve_flag),
            intake_gate: Arc::clone(&intake_gate),
            truncations: Arc::clone(&truncations),
            wipes: Arc::clone(&wipes),
            reports_implausible: Arc::clone(&reports_implausible),
            base: Instant::now(),
            durable_seen: durable,
            adopted_term: boot_term,
            // Finding #5 (see the intake-gate boot init above): a recovered
            // vote term beyond the data-stamped map arms the reconcile latch
            // at boot, so the first clean reconcile / truncate-ack for the
            // recovered term is what reopens the gate.
            awaiting_reconcile: boot_awaiting_reconcile,
            pending_truncation: None,
            output_persisted_completed: output_progress,
            output_progress_last_persist_ns: None,
            purge_policy: cfg.purge,
            archive_first_base: Arc::clone(&archive_first_base),
            snapshot_persisted_floor: state_snapshot_floor,
            snapshot_floor_last_persist_ns: None,
            incoming_snapshot: Arc::clone(&incoming_snapshot),
            incoming_snapshot_config: Arc::clone(&incoming_snapshot_config),
            adopted_incoming: 0,
            last_leader_map: Vec::new(),
            halt_removed: false,
            last_flags: 0,
            config_bytes: Arc::clone(&config_bytes),
            admin,
            // C1: the SAME values written into `CncMeta` above — the tag is
            // bound to this node's boot-time state, never to what the
            // (writable) cnc page happens to say at request time.
            admin_instance_id: instance_id,
            admin_app_id: cfg.app_id.clone(),
            audit,
            last_admin_seq: 0,
            pending_admin_fwd: None,
            last_config_reply: None,
            config_proposal_non_member: 0,
            config_proposal_dedup_resend: 0,
            crypto: crypto.clone(),
            crypto_hs_rx: Some(hs_rx),
            crypto_peer_ids,
            crypto_epoch: Arc::clone(&crypto_epoch),
            crypto_last_maint_ns: None,
            crypto_maint_ns: CRYPTO_MAINTENANCE_NS,
            crypto_last_redeliver_ns: None,
            crypto_committed_config_version: None,
            crypto_peers_dirty: true,
            crypto_hs_key_seal_failures: Arc::new(AtomicU64::new(0)),
            crypto_unresolved_peer: Arc::new(AtomicU64::new(0)),
            crypto_handshake_failures: Arc::clone(&crypto_handshake_failures),
            crypto_seal_failures: Arc::new(AtomicU64::new(0)),
            crypto_last_log_ns: 0,
        };
        let consensus_agent =
            AgentRunner::spawn("uc2-consensus", IdleStrategy::Yield, move || consensus.do_work())?;

        Ok(Node {
            node_id: cfg.id,
            cnc,
            term_handle,
            leader_flag,
            can_serve_flag,
            ingress_tx,
            admission_bytes: cfg.admission_bytes,
            buffer,
            truncations,
            wipes,
            reports_implausible,
            reports_unattested,
            archive_first_base,
            route_drops,
            sender_stats,
            partition_handles,
            config_bytes: Arc::clone(&config_bytes),
            crypto_epoch,
            crypto,
            crypto_handshake_failures,
            purge_enabled: !matches!(cfg.purge, PurgePolicy::Disabled),
            journal_segment_bytes: cfg.journal_segment_bytes,
            _instance: instance,
            _rings: rings,
            // Stop order: consensus first (stops writing the term handle), then
            // the data plane, then the archive last (so a final block can flush).
            agents: vec![consensus_agent, sender_agent, receiver_agent, archive_agent],
        })
    }

    pub fn is_leader(&self) -> bool {
        self.leader_flag.load(Ordering::Acquire)
    }

    pub fn can_serve(&self) -> bool {
        self.can_serve_flag.load(Ordering::Acquire)
    }

    pub fn current_term(&self) -> u32 {
        self.term_handle.load(Ordering::Acquire)
    }

    pub fn counters(&self) -> &LogCounters {
        self.cnc.counters()
    }

    /// The service's applied position (cnc `service_applied`, offset 512). A
    /// reconstructing service catches up when this reaches `commit`; the M6 gate
    /// reads it to time below-floor reconstruction convergence.
    pub fn service_applied(&self) -> u64 {
        self.cnc.service().service_applied.load_acquire()
    }

    /// M6 Task 4: the archive's lowest still-replayable position (the purge
    /// floor's realized value). `0` when nothing has been purged. Exposed for
    /// purge-safety tests: after the service publishes a snapshot and the purge
    /// driver runs, this advances to at most the snapshot floor.
    pub fn archive_first_base(&self) -> u64 {
        self.archive_first_base.load(Ordering::Acquire)
    }

    /// M8 (Task 12): the newest node-to-node group-key epoch this node has
    /// MINTED, or `None` if it never has (a follower that has not yet led, or
    /// a node with [`CryptoConfig::Disabled`]).
    ///
    /// Minting is leader-only (spec §5) and `GroupPlane::sealing_epoch`
    /// answers only for an epoch this node itself minted, so this doubles as
    /// "can this node seal group-scope traffic at all yet".
    pub fn crypto_epoch(&self) -> Option<u16> {
        match self.crypto_epoch.load(Ordering::Acquire) {
            0 => None,
            e => Some(e as u16),
        }
    }

    /// M8 Task 16 (throughput gate): how many AEAD seals this node has
    /// performed across every path — forwards to [`SharedTransport::seal_count`].
    /// `0` under [`CryptoConfig::Disabled`]. Observability only; the gate
    /// harness uses it to show the measured load genuinely drove the seal path
    /// from more than one agent rather than assuming it did.
    pub fn crypto_seal_count(&self) -> u64 {
        self.crypto.as_ref().map_or(0, |c| c.seal_count())
    }

    /// M8 Task 14 (adversarial tier): whether this node currently has an
    /// established pairwise Noise session with `peer` — forwards to
    /// [`SharedTransport::is_established`]. `false` under
    /// [`CryptoConfig::Disabled`] (nothing to establish) as well as while
    /// crypto is enabled but no session with `peer` has completed (never
    /// attempted, still in flight, or refused — e.g. `peer` was removed from
    /// the allowlist and a fresh handshake attempt was rejected). Exposed
    /// for adversarial/integration tests that need to observe session state
    /// from outside the crate without inferring it indirectly from cluster
    /// liveness.
    pub fn has_crypto_session_with(&self, peer: NodeId) -> bool {
        self.crypto.as_ref().is_some_and(|c| c.is_established(peer))
    }

    /// M8 Task 14: handshakes this node's consensus agent has REFUSED —
    /// `handshake.rs`'s `on_init`/`on_resp` claimed-id/transport-source
    /// binding check or allowlist-key check failing. `0` under
    /// [`CryptoConfig::Disabled`]. Distinguishes "an attacker's handshake
    /// attempt was actively refused" from "nothing happened to arrive yet" —
    /// the difference between a discriminating adversarial test and a timeout
    /// racing an absence.
    pub fn crypto_handshake_failures(&self) -> u64 {
        self.crypto_handshake_failures.load(Ordering::Relaxed)
    }

    /// Protocol 0.5.0: reports declined for a failed content attestation.
    /// See the field docs — steady state is 0 on a single-version fleet.
    pub fn reports_unattested(&self) -> u64 {
        self.reports_unattested.load(Ordering::Relaxed)
    }

    /// M8 Task 14: this node's receive-path crypto drop counters
    /// (`dropped_replay`, `dropped_auth_failed`, `peer_appears_cleartext`,
    /// `dropped_unknown_peer`, …) — the same `Arc<FollowerStats>` the
    /// receiver agent bumps. Exposed for adversarial tests that need to
    /// prove a specific DEFENSE (the anti-replay window, the auth-failure
    /// path) actually engaged, not merely that an attack's payload effect
    /// was absent (which downstream idempotency could also explain).
    pub fn crypto_stats(&self) -> &uc2_net::receiver::FollowerStats {
        &self.route_drops
    }

    /// M7 Task 6: the cnc-mirrored `ConfigRecord.config.version` — bumped by
    /// `Action::ConfigAdopted` (ordinary adoption) AND by the snapshot-install
    /// fiat path (`maybe_adopt_incoming_snapshot`). Exposed for tests asserting
    /// a joiner's config converges with the leader's after a snapshot install.
    pub fn config_version(&self) -> u64 {
        self.cnc.config_version()
    }

    /// Final-review fix (Item 1 test): the raw encoded-`ClusterConfig` bytes
    /// currently cached for the sender's `SnapshotSource` closure — i.e.
    /// exactly what a SNAP_BEGIN this node ships right now would carry in
    /// `SnapBeginBody.config`. Exposed for tests asserting the snapshot-fiat
    /// install path (`maybe_adopt_incoming_snapshot`) refreshed this cache,
    /// not just the SM/record/cnc-version (which `rebuild_net_for_config`
    /// alone used to leave stale on that path — see the doc comment there).
    pub fn snapshot_config_bytes(&self) -> Vec<u8> {
        self.config_bytes.lock().unwrap().clone()
    }

    /// Read the committed message frame at `pos` (which must be a frame
    /// start) via the log buffer's validated read. Exposed for
    /// harness/embedded callers (e.g. the smoke test) that want to inspect a
    /// frame's stamped `session_id`/`correlation_id` end to end; the real
    /// service/client SDKs read frames off the shared-memory rings instead.
    pub fn read_frame_validated(&self, pos: u64, out: &mut Vec<u8>) -> FrameRead {
        self.buffer.read_frame_validated(pos, out)
    }

    /// Leader-only ingress: enqueue a payload for the consensus agent to append
    /// (harness/embedded use; the real path is the client ingress MPSC ring,
    /// Task 7). Refused unless the node is a serving leader, the admission
    /// window is open (same `append - commit <= admission_bytes` door the
    /// ring drain enforces), or the bounded in-process queue is saturated.
    pub fn submit(&self, payload: Vec<u8>) -> Result<(), SubmitError> {
        if !self.can_serve() {
            return Err(SubmitError::NotServing);
        }
        let append = self.cnc.counters().append.load_acquire();
        let commit = self.cnc.counters().commit.load_acquire();
        if !admission_open(append, commit, self.admission_bytes) {
            return Err(SubmitError::Full);
        }
        match self.ingress_tx.try_send(payload) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(_)) => Err(SubmitError::Full),
            Err(mpsc::TrySendError::Disconnected(_)) => Err(SubmitError::NotServing),
        }
    }

    /// Partition handles for every one of the node's outbound sockets (receiver,
    /// sender, consensus). Blocking all of them isolates the node in both
    /// directions — the harness (Task 9) scripts partitions through these.
    pub fn partition_handles(&self) -> Vec<PartitionHandle> {
        self.partition_handles.clone()
    }

    /// Truncations this node has performed (reconciliation after a divergence).
    pub fn truncations(&self) -> u64 {
        self.truncations.load(Ordering::Relaxed)
    }

    /// M6 Task 8: how many wipe-and-rejoins (NoCommonPrefix → truncate-to-0) this
    /// node has performed. A subset of [`Node::truncations`].
    pub fn wipes(&self) -> u64 {
        self.wipes.load(Ordering::Relaxed)
    }

    /// M6 Task 9: datagrams this node's leader has served from the JOURNAL (deep
    /// NAK / replay sessions) rather than the live ring. `> 0` after a follower
    /// catches up across a below-ring gap is the prefill-decision evidence: the
    /// stream is repaired on demand from durable storage, so a restarted node
    /// never prefills its ring.
    pub fn replay_datagrams(&self) -> u64 {
        self.sender_stats.replay_datagrams.load(Ordering::Relaxed)
    }

    /// Current-term follower Reports dropped at the implausibility guard
    /// (claimed durable beyond our own append — provably corrupt in a static
    /// term; the wire has no CRC). M4 I-1 carry: one bit-flipped datagram must
    /// never manufacture a commit. Observability only — the drop poisons
    /// nothing (a later legitimate report still ranks).
    pub fn reports_implausible(&self) -> u64 {
        self.reports_implausible.load(Ordering::Relaxed)
    }

    /// M10 (Task 4): a read-only bundle of this node's `Arc`-shared counters,
    /// flags, and config values, for a later task's metrics encoder to render
    /// into a series. A straight clone-and-collect — every `Arc` cloned here
    /// is the SAME allocation the owning agent writes through, so this adds
    /// no new synchronization and never goes stale relative to the source.
    ///
    /// `agents` is always exactly 4 entries in the FIXED order `consensus,
    /// sender, receiver, archive` — NOT spawn order (spawn order is archive,
    /// sender, receiver, consensus) — because a later task's metric labels
    /// are positional against this order.
    pub fn observability(&self) -> crate::obs::ObsSources {
        crate::obs::ObsSources {
            node_id: self.node_id,
            cnc: Arc::clone(&self.cnc),
            sender: Arc::clone(&self.sender_stats),
            receiver: Arc::clone(&self.route_drops),
            truncations: Arc::clone(&self.truncations),
            wipes: Arc::clone(&self.wipes),
            reports_unattested: Arc::clone(&self.reports_unattested),
            reports_implausible: Arc::clone(&self.reports_implausible),
            crypto_handshake_failures: Arc::clone(&self.crypto_handshake_failures),
            crypto_enabled: self.crypto.is_some(),
            purge_enabled: self.purge_enabled,
            journal_segment_bytes: self.journal_segment_bytes,
            agents: [("consensus", 0usize), ("sender", 1), ("receiver", 2), ("archive", 3)]
                .into_iter()
                .map(|(name, idx)| (name, self.agents[idx].finished_flag()))
                .collect(),
        }
    }

    /// Consensus events dropped because the NetEvent channel was full (T7
    /// observability), summed across kinds. Safe drops — votes/reports/gossip
    /// re-fire on their cadence — but a rising count signals a wedged consensus
    /// agent. Use [`net_event_drops_by_kind`](Self::net_event_drops_by_kind) to
    /// attribute the drops to a specific traffic class.
    pub fn net_event_drops(&self) -> u64 {
        self.route_drops.net_drops.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    /// Per-kind consensus-event drop counts, indexed by
    /// [`uc2_net::receiver::NetEvent::kind_idx`] (Report, CommitGossip,
    /// RequestVote, Vote, TermMap, LeaderActivity, ReadProbe, ReadProbeAck,
    /// ConfigProposal, ConfigReply).
    pub fn net_event_drops_by_kind(&self) -> [u64; uc2_net::receiver::NET_EVENT_KINDS] {
        let mut out = [0u64; uc2_net::receiver::NET_EVENT_KINDS];
        for (o, c) in out.iter_mut().zip(self.route_drops.net_drops.iter()) {
            *o = c.load(Ordering::Relaxed);
        }
        out
    }

    /// Graceful stop: signal every agent and join.
    pub fn stop(self) {
        for a in self.agents {
            a.stop();
        }
    }

    /// Graceful stop that first waits for the archive to catch up.
    ///
    /// [`stop`](Self::stop) signals the agents and each exits at the TOP of its
    /// next duty cycle, so bytes appended but not yet recorded are simply not in
    /// the journal at exit. That is safe — un-recorded means un-fsynced means
    /// never acked — but it makes the restarted node re-fetch them. Draining
    /// first is what makes a planned restart cheap.
    ///
    /// The predicate is `durable >= append`, and `durable` is advanced by the
    /// archive agent only AFTER the fsync completes
    /// (`uc2_log::archive`), so reaching it means the bytes are on disk, not
    /// merely queued.
    ///
    /// A shutdown that hangs is worse than one that costs a replay, so the wait
    /// is hard-bounded by `deadline`. Note that ingress is still open while this
    /// runs: a client submitting throughout can hold `append` ahead of `durable`
    /// indefinitely, and the deadline is what bounds that case.
    pub fn stop_draining(self, deadline: std::time::Duration) -> DrainOutcome {
        let start = std::time::Instant::now();
        let outcome = loop {
            let c = self.counters();
            let (append, durable) = (c.append.load_acquire(), c.durable.load_acquire());
            if durable >= append {
                break DrainOutcome::Drained;
            }
            if start.elapsed() >= deadline {
                break DrainOutcome::DeadlineExpired { append, durable };
            }
            std::thread::yield_now();
        };
        self.stop();
        outcome
    }

    /// Crash-stop for the harness: stop the agents WITHOUT any extra flushing —
    /// whatever the archive had already fsynced is the recovery point; the
    /// volatile ring tail and the commit counter are lost (as on a real kill).
    /// Threads cannot be force-killed in-process, so this still joins them; the
    /// distinction from `stop` is that no final flush/drain is attempted.
    pub fn crash(self) {
        drop(self.agents); // AgentRunner::drop signals stop + joins, no flush
    }
}

// ------------------------------------------------------------- consensus agent

/// M12b: the actor recorded for a request that failed authentication — it
/// is precisely the case where no key name was proven, so naming a key (or
/// `"filesystem"`, which would claim the request was trusted on directory
/// permissions) would be a lie. Only ever appears on a record whose outcome
/// is `refused` with reason 20-23.
const ACTOR_UNVERIFIED: &str = "unverified";

/// M12b: the identifying fields of the admin request an audit record is
/// about, gathered so the audit call sites all read the same way whether the
/// request came off the cnc band, out of the pending-forward slot, or off the
/// wire as a peer's proposal.
#[derive(Debug, Clone, Copy)]
struct AuditedReq {
    op: u32,
    id: u32,
    ip: u32,
    port: u16,
    seq: u64,
    nonce: u64,
}

impl From<&AdminReq> for AuditedReq {
    fn from(r: &AdminReq) -> AuditedReq {
        AuditedReq { op: r.op, id: r.id, ip: r.ip, port: r.port, seq: r.seq, nonce: r.nonce }
    }
}

impl From<&PendingAdminFwd> for AuditedReq {
    fn from(p: &PendingAdminFwd) -> AuditedReq {
        AuditedReq { op: p.op, id: p.id, ip: p.ip, port: p.port, seq: p.seq, nonce: p.nonce }
    }
}

/// M7: one in-flight admin request this (follower) node forwarded to the
/// leader as a `ConfigProposal`, kept so the leader's kind-17 reply can be
/// matched back to the response line that is waiting for it.
///
/// M12b carries the *whole* request rather than just `(seq, nonce)`: the
/// audit record written when the answer is finally published names the op,
/// the target and the operator (`actor`, verified on THIS node before the
/// forward), none of which the reply datagram carries back.
#[derive(Debug, Clone)]
struct PendingAdminFwd {
    seq: u64,
    nonce: u64,
    /// The verified admin key name, or `None` under `AdminPolicy::Filesystem`.
    actor: Option<String>,
    op: u32,
    id: u32,
    ip: u32,
    port: u16,
}

struct Consensus {
    id: NodeId,
    /// Mirror of `ElectionSm::reports_unattested` for `Node::reports_unattested`.
    reports_unattested: Arc<AtomicU64>,
    /// Mirror of `ElectionSm::validated_up_to` for the receiver's reports.
    validated_frontier: Arc<AtomicU64>,
    /// Mirror of `ElectionSm::validated_term` — the reports' attestation.
    validated_term: Arc<std::sync::atomic::AtomicU32>,
    /// Position through which the archive has HANDED OVER its term
    /// observations (see `refresh_durable`). Published by the archive agent
    /// after the handoff, so it never runs ahead of the map.
    obs_frontier: Arc<AtomicU64>,
    /// Term observations held while the SM is mid-truncation (its data-plane
    /// latch would drop them, and nothing re-derives a dropped observation).
    pending_obs: Vec<(u32, u64)>,
    /// Diagnostic (UC2_TRUNC_TRACE): last commit provenance, published for
    /// the archive thread's cut trace. Both fields are inert unless the env
    /// var is set.
    trace_prov: Arc<Mutex<(&'static str, u32, u64)>>,
    trunc_trace: bool,
    sm: ElectionSm,
    state: NodeState,
    /// The shared cnc v2 page — this agent is the single writer of `commit`,
    /// `term`, `flags`, `leader_hint`, and `node_heartbeat_ns`.
    cnc: Arc<CncPage>,
    buffer: Arc<LogBuffer>,
    appender: Option<Appender>,
    next_corr: u64,
    pending_ingress: Option<Vec<u8>>,
    /// The client ingress ring's consumer half (Task 7) — the consensus
    /// thread is its sole reader.
    ingress_ring: MpscConsumer,
    /// The node egress broadcast ring's producer half (Task 7) — the
    /// consensus agent is this ring's single writer (`MSG_V2_NOT_LEADER` for a
    /// non-leader submit/read, and `MSG_V2_RETRY` when a linearizable read's
    /// barrier deadline lapses or leadership is lost — Task 11).
    egress_node: BroadcastProducer,
    /// The client query ring's consumer half (Task 11, MPSC) — the consensus
    /// thread is its sole reader (`MSG_V2_QUERY`; `flags` bit 0 = linearizable).
    query_ring: MpscConsumer,
    /// The node→service query ring's producer half (Task 11, SPSC) — the
    /// consensus agent is its single writer (`MSG_V2_SVC_QUERY`; payload =
    /// `expected_epoch: u64 LE ++ query bytes`).
    svc_query: SpscProducer,
    /// In-flight linearizable reads parked between admission and forward/retry
    /// (the ReadIndex barrier state machine). Small — one entry per outstanding
    /// client read; walked every duty cycle (bounded by outstanding reads).
    pending_reads: Vec<PendingRead>,
    /// Rung A: the single in-flight READ_PROBE round, if any. At most one
    /// exists; it certifies exactly the reads waiting when it was issued.
    current_round: Option<ProbeRound>,
    /// Rung A: the seq the NEXT round will carry. Reads record it at
    /// admission; `maybe_issue_round` consumes-and-increments it.
    next_round_seq: u64,
    /// Monotonic per-node nonce — scopes each probe ROUND (no longer each
    /// read) so acks attribute to the right round on the wire.
    next_nonce: u64,
    /// Mirror of `NodeConfig::admission_bytes` (the `append - commit` door
    /// budget for the ring drain).
    admission_bytes: u64,
    /// A ring record held back by a prior `AppendError::WouldOverrun` before
    /// taking more from `ingress_ring` — the record was already consumed off
    /// the ring (its consumer position advanced), so it MUST be retried here
    /// rather than dropped. `(client_id, local_seq, payload)`.
    pending_ring_ingress: Option<(u32, u32, Vec<u8>)>,
    sock: FaultSocket,
    id_to_addr: HashMap<NodeId, SocketAddr>,
    addr_to_id: HashMap<SocketAddr, NodeId>,
    /// Voting peers (voting members minus self): solicited for votes, paced for
    /// quorum, targeted by READ_PROBE, and used for the read-quorum size.
    peers: Vec<NodeId>,
    /// M6 Task 7: learner peers this node fans gossip out to (all learners minus
    /// self). Learners receive DATA (via the sender), commit gossip, and term maps
    /// so they replicate + reconcile, but are NEVER counted for any quorum.
    learner_ids: Vec<NodeId>,
    /// M6 Task 9: the per-peer observability band as `(peer_id, role_bits)` in
    /// cnc-slot order (voters first, then learners, capped at `CNC_MAX_PEER_SLOTS`).
    /// The consensus agent publishes `id_and_role` (boot-once) + `reported_durable`
    /// (per cycle) into `cnc.peer_slot(i)`; the sender fills `advertised_limit`.
    peer_band: Vec<(NodeId, u8)>,
    /// M6 Task 9: newest durable position each peer has reported (in-memory,
    /// updated per Report; flushed to the cnc band once per duty cycle — the cnc
    /// store is bounded, not per-datagram).
    peer_reported: HashMap<NodeId, u64>,
    /// Latch: publish the static `id_and_role` cells once (first duty cycle).
    peer_band_published: bool,
    net_rx: mpsc::Receiver<NetEvent>,
    obs_rx: mpsc::Receiver<(u32, u64)>,
    /// M7: durably-recorded CONFIG-frame observations from the archive agent's
    /// scan, `(frame-END position, payload bytes)` — decoded + fed as
    /// `Event::ConfigObserved` in `do_work` step 1c.
    cfg_obs_rx: mpsc::Receiver<(u64, Vec<u8>)>,
    ingress_rx: mpsc::Receiver<Vec<u8>>,
    trunc_tx: mpsc::SyncSender<ArchiveCmd>,
    trunc_slot: TruncationSlot,
    /// Issue #6: ack slot for [`ArchiveCmd::Collapse`], the leader-open cut.
    /// Separate from `trunc_slot` because both brackets can be open at once.
    collapse_slot: TruncationSlot,
    /// Issue #6: the leader open that is waiting on its collapse ack. Set when
    /// `Action::BecomeLeader` emits the cut; consumed by `on_collapsed` on the
    /// matching ack, which finishes the open (fresh `Appender`, NewTerm frame,
    /// gate reopen). Cleared WITHOUT finishing if the node steps down or adopts
    /// a higher term first (`Action::BecomeFollower`) — a stale ack must never
    /// resurrect a leadership the SM has already abandoned.
    pending_leader_open: Option<PendingLeaderOpen>,
    /// Issue #6: monotonic epoch allocator for `pending_leader_open`. Purely
    /// node-local (unlike the reconcile epoch, which the SM allocates), since
    /// the SM has no notion of the collapse round-trip.
    next_collapse_epoch: u64,
    /// M7: this agent's own producer half of the sender's `CtrlMsg` channel —
    /// used to send `CtrlMsg::SetPeers` on config adoption (`Action::ConfigAdopted`).
    /// A clone of the same sender the receiver uses to route NAK/Status/SnapNak/
    /// SnapDone (`receiver.set_sender_route`); a dropped/full-channel send is
    /// silently ignored (nothing productive to do about a dead sender thread here).
    sender_ctrl: mpsc::SyncSender<CtrlMsg>,
    term_handle: TermHandle,
    leader_flag: Arc<AtomicBool>,
    can_serve_flag: Arc<AtomicBool>,
    intake_gate: Arc<AtomicBool>,
    truncations: Arc<AtomicU64>,
    /// M6 Task 8: wipe-and-rejoin count (NoCommonPrefix → truncate-to-0), bumped
    /// on `Action::CountWipe`. Shared with the `Node` handle for observability.
    wipes: Arc<AtomicU64>,
    reports_implausible: Arc<AtomicU64>,
    base: Instant,
    durable_seen: u64,
    adopted_term: u32,
    awaiting_reconcile: bool,
    /// The epoch of the truncation currently in flight (emit→ack bracket). `Some`
    /// from `Action::Truncate` exec until the matching slot ack; the intake-gate
    /// reopen discipline uses it to know a truncation is pending.
    pending_truncation: Option<u64>,
    /// Last `output_completed` value durably persisted to
    /// `state/output_progress.state` (Task 12) — lets `maybe_persist_output_progress`
    /// tell "changed since last persist" apart from "unchanged, nothing to do"
    /// with one cheap compare per cycle.
    output_persisted_completed: u64,
    /// Monotonic ns (via `now_ns`) of the last output-progress persist; `None`
    /// until the first one (so the very first observed change persists without
    /// waiting out the floor). The 100 ms floor rate-limits the fsync'd
    /// `StableValue::store`, not the cheap in-page compare that runs every cycle.
    output_progress_last_persist_ns: Option<u64>,
    /// M6 Task 4: journal purge policy (default `Disabled`). Gates the purge
    /// half of `maybe_persist_snapshot_floor`.
    purge_policy: PurgePolicy,
    /// M6 Task 4: node-internal mirror of the archive's lowest replayable
    /// position, written by the archive agent. Read here to gate purge (only
    /// issue a `Purge` that would actually advance the floor).
    archive_first_base: Arc<AtomicU64>,
    /// M6 Task 4: last snapshot floor durably persisted to `state/snapshot.state`.
    /// The increase-only high-water mark — seeded from the recovered durable
    /// floor at boot so a fresh cnc page's `service_snapshot_pos == 0` can never
    /// regress it (the marker-clobber lesson, exactly as `output_persisted_completed`).
    snapshot_persisted_floor: u64,
    /// M6 Task 4: monotonic ns of the last snapshot-floor persist; `None` until
    /// the first. Same 100 ms fsync floor as output-progress.
    snapshot_floor_last_persist_ns: Option<u64>,
    /// M6 Task 6: node-internal signal from the receiver — the position of the
    /// newest COMPLETE inbound snapshot transfer (0 = none). Sampled each cycle;
    /// on a new value we adopt it as the archive floor + mirror to cnc.
    incoming_snapshot: Arc<AtomicU64>,
    /// M7 Task 6: companion cell for `incoming_snapshot` — the encoded config
    /// carried by the SAME completed transfer (`SnapBeginBody.config`), decoded
    /// and adopted by fiat (`ElectionSm::adopt_snapshot_config`) alongside the
    /// archive-floor adoption in `maybe_adopt_incoming_snapshot`.
    incoming_snapshot_config: Arc<Mutex<Vec<u8>>>,
    /// M6 Task 6: last inbound-snapshot position already adopted (shadow, so the
    /// AdoptFloor command + cnc mirror fire once per completed transfer).
    adopted_incoming: u64,
    /// M6 Task 8: the leader's most-recently-shipped term-map (the wire tail),
    /// captured verbatim on every `TermMap` datagram. On a snapshot install
    /// (`AdoptFloor`) this authoritative lineage is seeded into the SM so a
    /// below-floor joiner's next reconcile finds the common prefix that otherwise
    /// lives hidden inside the snapshot. Empty until the first term-map arrives.
    last_leader_map: Vec<(u32, u64)>,
    /// M7: latched once this node observes itself removed from the adopted
    /// config while NOT a leader mid-self-removal (`Action::HaltRemoved`).
    /// `do_work` checks this FIRST and returns immediately once set — a
    /// permanent park, never cleared (fail-stop; a removed node's only path
    /// back in is a fresh join under a new id/config, not un-halting).
    halt_removed: bool,
    /// M10: the `flags` value `publish_status` last wrote to the cnc page —
    /// initialised to the boot value (0: neither leader nor serving until
    /// the first publish). Compared each cycle so `serving_changed` fires
    /// only on the `NODE_FLAG_CAN_SERVE` bit's edge, not every cycle.
    last_flags: u64,
    /// M7 Task 6: the snapshot-session config-carry cache — refreshed with the
    /// newly-adopted config's encoded bytes on every `Action::ConfigAdopted`;
    /// read by the sender's `SnapshotSource` closure (a separate `Arc` clone) so
    /// every SNAP_BEGIN ships whatever config is CURRENT at ship time.
    config_bytes: Arc<Mutex<Vec<u8>>>,
    /// M12b: how this node authenticates admin requests off the cnc admin
    /// band — `Filesystem` (the default: the instance dir's permissions are
    /// the boundary, and the auth line is never read) or `Hmac` (named keys +
    /// TTL). Read ONLY inside `verify_admin`, i.e. only when an admin request
    /// actually arrived: nothing on the duty-cycle hot path touches it.
    admin: AdminPolicy,
    /// M12b final review (C1): the `instance_id` this node's admin HMAC tags
    /// are bound to — captured from the value `start_with` generated and
    /// wrote into [`CncMeta`], NOT re-read from the cnc page per request.
    ///
    /// The page is a file in the instance directory that an actor with write
    /// access can edit, and `read_cnc_header` validates only the magic. Were
    /// `verify_admin` to take the binding values off the page, that actor
    /// could capture a signed `(auth, req)` pair, wait for (or induce) a
    /// restart — which resets `last_admin_seq` to 0 — write the CAPTURED
    /// `instance_id` back into `CNC_OFF_INSTANCE_LO/HI`, re-write the
    /// captured lines, and have the change applied a second time. Holding
    /// the value in the agent's own memory makes the page's copy purely
    /// informational to attaching parties and un-forgeable as a credential.
    admin_instance_id: u128,
    /// M12b final review (C1): the `app_id` admin HMAC tags are bound to —
    /// `NodeConfig::app_id`, this node's own boot-time state, for the same
    /// reason as [`Self::admin_instance_id`]. Owned once at start rather
    /// than re-allocated out of `CncPage::meta()` on every request.
    admin_app_id: String,
    /// M12b (spec §5.3): the append-only admin audit file, opened at node
    /// start and written by this agent alone. Every answer this node gives to
    /// an admin request is recorded here — with an fsync — BEFORE the answer
    /// is published; a record that fails to write turns the request into a
    /// [`REASON_AUDIT_FAILED`] refusal. Off every hot path: touched only when
    /// an admin request or a forwarded proposal actually arrives.
    audit: AuditLog,
    /// M7 Task 7: the last admin-request seq consumed off the cnc admin-req
    /// slot (`do_work` step 11's seqlock cursor into `read_admin_req`). `0` at
    /// boot — matches the freshly-zeroed cnc page (recreated every node boot),
    /// so an admin request from a prior life is never replayed.
    last_admin_seq: u64,
    /// M7 Task 7: this (follower) node's own in-flight forwarded proposal —
    /// the admin request we forwarded to the leader as a
    /// kind-16 `ConfigProposal`. A 1-slot pending map (one admin request in
    /// flight at a time, per the cnc admin band's own single-slot discipline):
    /// cleared once the matching-nonce `NetEvent::ConfigReply` (kind 17)
    /// arrives and its status/reason/version is written back to the response
    /// line. `None` = no forward outstanding.
    ///
    /// M12b: carries the verified actor (the admin key name that signed the
    /// request, `None` under `AdminPolicy::Filesystem`) alongside
    /// `(seq, nonce)`, so the audit record Task 4 writes when the leader's
    /// reply lands names the operator who asked, not just the change.
    pending_admin_fwd: Option<PendingAdminFwd>,
    /// M7 Task 7: leader-side nonce dedup — the last forwarded proposal's
    /// `(nonce, reply)` this node (as leader) answered. A repeat nonce (the
    /// follower's forward retried, or a genuine wire retry) gets the STORED
    /// reply re-sent rather than re-running `propose_config` a second time —
    /// idempotent under retry without relying on `ChangePending` to happen to
    /// refuse the repeat. `None` until the first forwarded proposal is handled.
    last_config_reply: Option<(u64, ConfigReplyBody)>,
    /// M12b review: count of `ConfigProposal` (kind 16) datagrams dropped by
    /// `on_config_proposal`'s membership guard because their source address
    /// resolves to no current member — see that function's doc comment. A
    /// non-member datagram must never reach `peer_actor`/`audit_admin`, so
    /// this is incremented and warned on BEFORE either is called.
    config_proposal_non_member: u64,
    /// M12b final review (I4): count of kind-16 `ConfigProposal` datagrams
    /// answered from the nonce-dedup cache (`last_config_reply`) — a
    /// byte-identical re-answer of a nonce this leader already recorded.
    /// Those re-answers deliberately do NOT write a second audit record (and
    /// so do NOT cost a second `fsync`), so this counter is what accounts for
    /// them; see `on_config_proposal`.
    config_proposal_dedup_resend: u64,

    // ---- M8 (Task 12): the crypto plane -----------------------------------
    /// The process's shared handshake/group-key/rotation state. `None` =
    /// [`CryptoConfig::Disabled`], and every field below is inert.
    ///
    /// This agent holds the `SharedTransport` itself (not a `SendHalf` — that
    /// went to the sender agent, and `send_half` is single-call by
    /// construction): it drives the handshake, mints and re-delivers group
    /// keys, and seals its own `HS_KEY` sends through
    /// `SharedTransport::seal_pairwise_control`.
    crypto: Option<SharedTransport>,
    /// Handshake-plane datagrams (kinds 18/19/20) demuxed off the receive
    /// seam. `HS_INIT`/`HS_RESP` arrive verbatim; `HS_KEY` arrives ALREADY
    /// OPENED (the receiver's `crypto_admit` decrypts it first).
    crypto_hs_rx: Option<mpsc::Receiver<HandshakeDatagram>>,
    /// The live `SocketAddr -> NodeId` map the receive seam resolves senders
    /// through. Republished by `rebuild_peer_maps` on every adopted config —
    /// M7 adds nodes at runtime, and a joiner this map does not know is a
    /// joiner whose every datagram is dropped as `dropped_unknown_peer`.
    crypto_peer_ids: Option<PeerIds>,
    /// Newest group epoch THIS node minted, mirrored for observability
    /// ([`Node::crypto_epoch`]). `0` = never minted; `GroupPlane` reserves
    /// epoch 0 as the wire's cleartext sentinel and never mints it, so 0 is
    /// an unambiguous "none".
    crypto_epoch: Arc<AtomicU32>,
    /// `SharedTransport::now_ns` of the last maintenance pass (`None` until
    /// the first, so the very first duty cycle runs one).
    crypto_last_maint_ns: Option<u64>,
    /// Spacing between maintenance passes — [`CRYPTO_MAINTENANCE_NS`] in
    /// production; the unit-test harness sets 0 so a single `do_work` is a
    /// full pass.
    crypto_maint_ns: u64,
    /// `SharedTransport::now_ns` of the last un-acked `HS_KEY` re-delivery.
    crypto_last_redeliver_ns: Option<u64>,
    /// `ClusterConfig::version` of the newest config observed COMMITTED —
    /// the edge that feeds `RotationState::on_committed_config`. `None`
    /// until the first observation.
    crypto_committed_config_version: Option<u64>,
    /// Set when the peer set changed (boot, or an adopted config): the next
    /// maintenance pass asks `Peers::initiate` for a link to everyone.
    crypto_peers_dirty: bool,
    /// `HS_KEY` deliveries that could not be sealed (no established pairwise
    /// session with that peer yet) and were therefore DROPPED, never sent in
    /// the clear. Self-healing: the re-delivery sweep retries once the
    /// handshake completes.
    crypto_hs_key_seal_failures: Arc<AtomicU64>,
    /// Datagrams dropped because an address and a `NodeId` could not be
    /// matched up — in EITHER direction: an inbound datagram from a source
    /// address this node has no id for (a stranger, or a peer removed from
    /// the config), or an outbound send naming a peer id with no address in
    /// the adopted config (a config change that raced it). All are the same
    /// operator-visible condition — "the crypto plane and the membership view
    /// disagree about who exists" — so they share one counter rather than
    /// several that would always have to be read together.
    ///
    /// T17 widened this past the handshake plane: `Consensus::send` and
    /// `Consensus::fan_out_group` both count here when a consensus datagram
    /// cannot be addressed. The fan-out's site fires in CLEARTEXT mode too
    /// (its address lookup is not crypto-gated), so a non-zero value on a
    /// node with crypto disabled is meaningful rather than a bug — it means
    /// the same membership inconsistency, seen without the crypto plane.
    crypto_unresolved_peer: Arc<AtomicU64>,
    /// `HandshakeAction::Failed` observations — a peer that is not (yet) in
    /// the allowlist, or whose handshake did not authenticate.
    crypto_handshake_failures: Arc<AtomicU64>,
    /// M8 (Task 17): CONSENSUS-plane datagrams this agent could not seal and
    /// therefore DROPPED — `READ_PROBE`, `COMMIT_POSITION`, `TERM_MAP`,
    /// `VOTE`, `REQUEST_VOTE`, `CONFIG_PROPOSAL`, `CONFIG_REPLY`. Kept
    /// separate from `crypto_hs_key_seal_failures` (the handshake plane)
    /// because the two mean different things to an operator: a handshake-plane
    /// drop is the ordinary bring-up transient, while a SUSTAINED
    /// consensus-plane drop means this node cannot participate in consensus
    /// at all — no votes, no gossip, no read barrier.
    ///
    /// Never a cleartext fallback. Self-healing on its own cadence: votes and
    /// `REQUEST_VOTE`s re-fire on the election timeout, gossip and term maps
    /// on the gossip floor, and a `READ_PROBE` round on the read path's own
    /// retry.
    crypto_seal_failures: Arc<AtomicU64>,
    /// `now_ns` of the last printed crypto diagnostic (see
    /// [`CRYPTO_LOG_INTERVAL_NS`]).
    crypto_last_log_ns: u64,
}

impl Consensus {
    /// One consensus duty cycle (binding order, plan §Task 8).
    fn do_work(&mut self) -> bool {
        // M7: a removed node fail-stops permanently (`Action::HaltRemoved`) —
        // checked FIRST, before anything else runs. The halting cycle itself
        // (the one that set the flag) already ran to completion — including
        // one last `publish_status` that zeroed LEADER/CAN_SERVE — so this
        // only ever short-circuits a SUBSEQUENT cycle.
        if self.halt_removed {
            return false;
        }
        let mut did = false;

        // 0. Absorb the durable counter BEFORE anything that reads our own log
        // as a credential — `Event::Tick` may start a candidacy in step 4, and
        // `start_election` advertises `ElectionSm::durable` as `last_durable`.
        // Step 2 keeps its own call for advances that land mid-cycle. See
        // `refresh_durable`.
        did |= self.refresh_durable();

        // 1. Drain the NetEvent channel → SM events.
        for _ in 0..NET_DRAIN_PER_CYCLE {
            match self.net_rx.try_recv() {
                Ok(ev) => {
                    self.feed_net(ev);
                    did = true;
                }
                Err(_) => break,
            }
        }

        // 1b. Drain data-stamped term observations (T4) → DataTermObserved, in
        // order (the archive ships them position-ordered).
        // The SM's truncating latch DROPS data-plane events, and a dropped
        // term observation is unrecoverable (see the archive's retain path):
        // the map would stay permanently short of the bytes we hold, and the
        // next reconcile would read the leader's newer entries as divergence
        // and cut committed data. So buffer while a truncation is in flight
        // and replay in position order once it acks (2026-08-16 hunt).
        while let Ok((term, base)) = self.obs_rx.try_recv() {
            self.pending_obs.push((term, base));
        }
        if self.sm.is_truncating() {
            // Hold them; `Event::Truncated` will let the next cycle through.
            // The truncation itself may invalidate some of these positions —
            // that is fine, `DataTermObserved` only extends the map and the
            // post-cut map is re-derived from what survives.
            if !self.pending_obs.is_empty() {
                did = true;
            }
        } else {
            for (term, base) in std::mem::take(&mut self.pending_obs) {
                self.feed(Event::DataTermObserved { term, base });
                did = true;
            }
        }
        // Observations and reconciles both move the validated frontier.
        self.publish_validated_frontier();

        // 1c. Drain durably-recorded CONFIG-frame observations (M7): decode +
        // feed `Event::ConfigObserved`. This is the follower / boot-recovery
        // half of config adoption — the leader's own append path
        // (`append_config_frame`) feeds itself directly at append time
        // (adopt-at-append); this is how everyone else learns once the frame
        // is durable (and how the leader itself re-confirms, harmlessly —
        // adoption is idempotent by version). A decode failure is fail-stop:
        // the archive's block is journal-CRC-covered, so a malformed payload
        // here is a BUG, never something to shrug off.
        while let Ok((position, payload)) = self.cfg_obs_rx.try_recv() {
            let wire = decode_config(&payload)
                .unwrap_or_else(|| panic!("corrupt CONFIG frame at {position}"));
            // Belt (post-M7 follow-up): observations are drained AFTER the
            // archive agent's do_work returned, and do_work store_release's
            // durable as its LAST step — so a durably-recorded CONFIG
            // frame's end position can never exceed the durable counter
            // here. A violation is a mis-based observation (recorder bug):
            // adopting it would park config_position above durable, where
            // config_pending could never clear. Skip + log, don't adopt.
            let durable = self.cnc.counters().durable.load_acquire();
            if position > durable {
                eprintln!(
                    "node {}: ignoring implausible ConfigObserved at {position} (durable {durable})",
                    self.id
                );
                did = true;
                continue;
            }
            let config = wire_to_cluster_config(&wire);
            if config_content_diverges(self.sm.config(), &config) {
                eprintln!(
                    "node {}: DIVERGENT config observed at {position}: version {} content differs from adopted",
                    self.id, config.version
                );
            }
            self.feed(Event::ConfigObserved { position, config });
            did = true;
        }

        // 1d. Drain the truncation ack slot (a later cycle after emitting
        // `Truncate`). The infallible single slot holds at most one ack.
        if let Some((epoch, to)) = self.trunc_slot.take() {
            self.on_truncated(epoch, to);
            did = true;
        }

        // 1e. Issue #6: drain the leader-open collapse ack (its own slot — a
        // reconcile truncation can be in flight at the same time). This is what
        // finishes a leader open: fresh appender, NewTerm frame, gate reopen.
        if let Some((epoch, to)) = self.collapse_slot.take() {
            self.on_collapsed(epoch, to);
            did = true;
        }

        // 2. Poll the durable counter; feed DurableAdvanced on change.
        //
        // Issue #6: NOT while a leader open is in flight. `ElectionSm::durable`
        // is a monotonic max (`durable = durable.max(d)`), and the whole premise
        // of the collapse is that the archive's frontier is ABOVE the `base` we
        // are collapsing to. Between phase 1 and the archive's ack the counter
        // still holds that higher value, so feeding it here would latch
        // `sm.durable` above a frontier we are about to cut away — and it never
        // comes back down (`Event::Truncated`'s `min` clamp is the reconcile
        // path's, not ours). The SM would then ship an inflated `last_durable`
        // vote credential and, worse, `rank_leader` would advance the commit
        // tracker with an `own_durable` this node does not physically hold: a
        // phantom commit. `base` IS `sm.durable`, so suppressing the feed leaves
        // the SM exactly where it already is; `on_collapsed` re-bases
        // `durable_seen` at the cut and the next cycle resumes from there.
        //
        // Pre-issue-#6 this could not arise: `prime(base)` ran synchronously in
        // step 1, so step 2 always read the already-collapsed value. Splitting
        // the open across two cycles is what opened the window.
        if self.refresh_durable() {
            did = true;
        }

        // 3. Drain the in-process ingress queue (leader && serving only, the
        // harness/embedded path), bounded.
        let serving = self.leader_flag.load(Ordering::Relaxed) && self.sm.can_serve();
        if serving {
            did |= self.drain_ingress();
        }

        // 3b. Drain the client ingress MPSC ring (Task 7): appends while
        // serving, subject to the admission window, or redirects each record
        // with `MSG_V2_NOT_LEADER` while not. Runs every cycle regardless of
        // role — bounded by `INGRESS_PER_CYCLE` either way so a saturated
        // ring cannot starve the rest of the duty cycle.
        did |= self.drain_ingress_ring(serving);

        // 3c. Drain the client query ring (Task 11): snapshot reads forward to
        // the service immediately (epoch check skipped); linearizable reads open
        // a ReadIndex barrier (parked awaiting certification by the shared probe
        // round — one nonce'd READ_PROBE round in flight at a time, issued at the
        // end of this drain and from advance_pending_reads) or are redirected
        // `MSG_V2_NOT_LEADER` while not serving. Bounded per cycle.
        did |= self.drain_query_ring();

        // 3d. Advance in-flight linearizable reads: quorum acks arrive via
        // `feed_net`; once quorum + service catch-up hold, forward the read to
        // the service; on deadline or lost leadership, answer `MSG_V2_RETRY`.
        did |= self.advance_pending_reads();

        // 4. Feed the tick — the ONLY place real time enters the SM.
        let now = self.now_ns();
        self.feed(Event::Tick { now_ns: now });

        // 5. Publish role/serving snapshots for the API (term is written on
        // transitions; keep can_serve fresh every cycle). M7 Task 8: masked
        // off `halt_removed` exactly like `publish_status`'s cnc mirror below —
        // otherwise a self-removing LEADER's `StepDownRemoved` halt (`exec`,
        // above these steps in this SAME cycle) would be silently undone right
        // here: the SM's own `serving` field is never cleared by step-down (it
        // has no reason to be), so an unconditional store would re-publish
        // `true` for `Node::can_serve()` the very cycle it just halted.
        self.can_serve_flag.store(!self.halt_removed && self.sm.can_serve(), Ordering::Release);

        // 6. Publish the node's status onto the shared cnc page for cross-process
        // attachers (service, clients). `term` + `flags` reflect the SM every
        // cycle; `node_heartbeat_ns` is wall-clock ns (SystemTime) so a service
        // in another process can compare it against its own clock for liveness.
        self.publish_status();

        // 7. Sample the service-written `output_completed` counter (Task 12);
        // on change, durably persist + mirror it, subject to the 100 ms floor.
        did |= self.maybe_persist_output_progress();

        // 8. Sample the service-written `service_snapshot_pos` (M6 Task 4); on a
        // validated increase, durably persist the snapshot floor + mirror it,
        // then (if the purge policy is on) command the archive to purge below it.
        did |= self.maybe_persist_snapshot_floor();

        // 9. Sample the receiver's completed-inbound-snapshot signal (M6 Task 6);
        // on a new value, adopt it as the archive floor and mirror it to cnc.
        did |= self.maybe_adopt_incoming_snapshot();

        // 10. Publish the per-peer observability band + archive floor (M6 Task 9).
        // Bounded: one pass over ≤8 slots per cycle, no per-datagram cnc writes.
        self.publish_peer_band();

        // 11. Admin slot (M7 Task 7): at most one request per cycle. Leader:
        // propose + append + reply on the response line. Follower: forward to
        // the leader hint as kind 16 (remembering seq/nonce for the eventual
        // kind-17 reply); no hint -> reply status=2 (retry). `read_admin_req`'s
        // seqlock cursor (`last_admin_seq`) makes this idempotent against a
        // duty cycle that runs before `uc2ctl`'s poll catches the response.
        if let Some(req) = self.cnc.read_admin_req(self.last_admin_seq) {
            self.last_admin_seq = req.seq;
            self.handle_admin(req);
            did = true;
        }

        // 12. M7: clear the cnc `config_pending` mirror once commit has crossed
        // the adopted config's position — the entry is no longer at risk of a
        // truncation revert. `sm.config_pending()` is the single source of truth
        // (`config_position > commit_seen`, updated on every commit advance/gossip);
        // mirrored here rather than re-deriving it from the raw cnc counters.
        if !self.sm.config_pending() && self.cnc.config_pending() != 0 {
            self.cnc.store_config_pending(false);
            did = true;
        }

        // 13. M8 (Task 12): the crypto plane — route inbound handshake
        // traffic, observe committed config changes (rotation trigger 3),
        // and run the rate-limited maintenance pass (allowlist reload,
        // `Peers::tick`, rotation + mint). Placed LAST so it observes this
        // cycle's committed config (step 1c's adoption + step 2's commit
        // advance) rather than the previous one's: a removal must revoke in
        // the same cycle its commit lands, not the next.
        did |= self.crypto_cycle();
        did
    }

    // ---- M8 (Task 12): the crypto plane ----------------------------------
    //
    // Everything below is a no-op when `crypto` is `None`. The ordering
    // inside `crypto_cycle` is load-bearing and pinned by
    // `a_committed_remove_rotates_but_a_committed_demote_does_not`: the
    // committed-config observation must precede the rotation check in the
    // SAME cycle, or a removal's revocation waits a whole maintenance
    // interval behind the commit that authorized it.

    /// The crypto duty cycle: route inbound handshake traffic, observe
    /// committed config changes (rotation trigger 3), then run the
    /// rate-limited maintenance pass (allowlist, `Peers::tick`, rotation).
    fn crypto_cycle(&mut self) -> bool {
        if self.crypto.is_none() {
            return false;
        }
        let mut did = self.crypto_drain_handshake_route();
        did |= self.crypto_observe_committed_config();
        did |= self.crypto_maintenance();
        did
    }

    /// The one clock the crypto plane is ever driven by — `SharedTransport`'s
    /// own `Instant` origin, shared with the `SendHalf`/`ReceiveHalf` the
    /// sender and receiver agents hold. NEVER `Consensus::now_ns` (this
    /// agent's independent base): `GroupPlane::sealing_epoch` compares
    /// `now_ns` against the MINT timestamp, so two unrelated origins make the
    /// activation grace period either never elapse or elapse instantly. This
    /// accessor exists so no call site here has to remember that.
    fn crypto_now_ns(&self) -> u64 {
        self.crypto.as_ref().map(|c| c.now_ns()).unwrap_or(0)
    }

    /// Drain the handshake-plane route into `Peers`/`GroupPlane` and execute
    /// whatever they ask for. Bounded per cycle like every other drain.
    fn crypto_drain_handshake_route(&mut self) -> bool {
        let mut actions: Vec<HandshakeAction> = Vec::new();
        let mut did = false;
        {
            let (Some(crypto), Some(rx)) = (self.crypto.as_ref(), self.crypto_hs_rx.as_ref())
            else {
                return false;
            };
            for _ in 0..CRYPTO_HS_DRAIN_PER_CYCLE {
                let Ok((from, kind, body)) = rx.try_recv() else {
                    break;
                };
                did = true;
                // The crypto layer identifies peers by `NodeId`; the wire
                // knows only addresses. An address with no mapping is a
                // stranger (or a removed peer) — counted and dropped, never
                // fed to the handshake state machine under a guessed id.
                let Some(&peer) = self.addr_to_id.get(&from) else {
                    self.crypto_unresolved_peer.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                let now = crypto.now_ns();
                match kind {
                    DGRAM_KIND_HS_KEY => {
                        // Already OPENED by the receive seam (kind 20 is
                        // `Scope::Pairwise`), so `from` is authenticated.
                        actions.extend(crypto.on_group_key_message(peer, &body));
                    }
                    _ => {
                        // `HS_INIT`/`HS_RESP` — raw wire bytes, and the first
                        // thing in this process to see them. `Peers::on_message`
                        // treats `peer` as a CLAIM until the pattern and the
                        // allowlist agree, and never panics on `body`.
                        actions.extend(crypto.on_handshake_message(peer, kind, &body, now));
                    }
                }
            }
        }
        self.crypto_exec(actions);
        did
    }

    /// Rotation trigger 3 (spec §5): feed `RotationState` the tombstone count
    /// of every config change that has COMMITTED — promotes and demotes
    /// included, not only removals.
    ///
    /// Feeding it only removals would break it: it establishes a baseline on
    /// its FIRST observation and latches only on strict GROWTH of the count,
    /// so a first-ever call carrying a removal would be swallowed as the
    /// baseline and revoke nothing.
    ///
    /// The commit edge is `!sm.config_pending()` — literally
    /// `config_position <= commit_seen`, the same crossing `rank_leader`
    /// computes for `StepDownRemoved`, observed here at the node layer so it
    /// is available to FOLLOWERS too (a follower must keep its baseline in
    /// step, or the epoch it eventually mints as a future leader would judge
    /// growth against a stale count). Keyed on `version` rather than a bare
    /// edge so it fires exactly once per committed change, and re-fires
    /// correctly across a truncation revert (which re-adopts a LOWER version:
    /// the count moves down, which is not growth, and the subsequent
    /// re-adoption forward then latches — over-rotating on a revert, never
    /// under-rotating).
    fn crypto_observe_committed_config(&mut self) -> bool {
        if self.sm.config_pending() {
            return false;
        }
        let version = self.sm.config().version;
        if self.crypto_committed_config_version == Some(version) {
            return false;
        }
        self.crypto_committed_config_version = Some(version);
        let tombstones = self.sm.config().tombstones.len();
        if let Some(crypto) = self.crypto.as_ref() {
            crypto.on_committed_config(tombstones);
        }
        true
    }

    /// The rate-limited maintenance pass — see [`CRYPTO_MAINTENANCE_NS`].
    fn crypto_maintenance(&mut self) -> bool {
        let now = self.crypto_now_ns();
        if let Some(last) = self.crypto_last_maint_ns
            && now.saturating_sub(last) < self.crypto_maint_ns
        {
            return false;
        }
        self.crypto_last_maint_ns = Some(now);

        // (a) Membership churn: publish the sender-identity map and ask for a
        //     link to every peer. `initiate` is idempotent — a peer with a
        //     session or an in-flight handshake produces no traffic.
        if self.crypto_peers_dirty {
            self.crypto_peers_dirty = false;
            let peers = self.gossip_targets();
            let acts = self
                .crypto
                .as_ref()
                .map(|c| peers.iter().flat_map(|&p| c.initiate(p, now)).collect::<Vec<_>>())
                .unwrap_or_default();
            self.crypto_exec(acts);
        }

        // (b) Notice an operator dropping a new peer's public key in. M7 adds
        //     nodes at runtime; without this caller the joiner would need a
        //     cluster-wide restart to be authorized (spec §5). Self-rate-
        //     limited to once a second and touches no disk before that gate,
        //     so calling it every pass is cheap.
        if let Some(crypto) = self.crypto.as_ref()
            && let Err(e) = crypto.allowlist_reload_if_stale(now)
        {
            self.crypto_log(now, format_args!("allowlist reload failed: {e}"));
        }

        // (c) Handshake upkeep: retransmit unanswered `HS_INIT`s with
        //     backoff, restart links asked for but not up, expire unproven
        //     pending sessions, announce promotions.
        let acts = self.crypto.as_ref().map(|c| c.tick(now)).unwrap_or_default();
        self.crypto_exec(acts);

        // (d) Rotation — LEADER ONLY. `GroupPlane::mint` is the leader's
        //     prerogative (spec §5); a follower simply lets its latches
        //     accumulate, and `on_became_leader` clears them all with one
        //     mint the moment it wins an election.
        if self.leader_flag.load(Ordering::Relaxed) {
            let due = self.crypto.as_ref().and_then(|c| c.rotation_due(now));
            match due {
                Some(reason) => {
                    let peers = self.gossip_targets();
                    let minted =
                        self.crypto.as_ref().map(|c| c.mint_group_key(&peers, now));
                    if let Some((epoch, acts)) = minted {
                        self.crypto_epoch.store(epoch as u32, Ordering::Release);
                        self.crypto_last_redeliver_ns = Some(now);
                        eprintln!(
                            "node {}: minted group key epoch {epoch} for {} peer(s) ({reason:?})",
                            self.id,
                            peers.len()
                        );
                        self.crypto_exec(acts);
                    }
                }
                None => self.crypto_redeliver_unacked(now),
            }
        }
        true
    }

    /// Re-send the newest minted epoch's `HS_KEY` to every peer that has not
    /// acked it. `GroupPlane::mint` emits each delivery exactly once and the
    /// datagram rides UDP, so without this a single drop leaves that peer
    /// unable to open ANY group-scope traffic until the next rotation — an
    /// hour away by default. The spec's "recovers through the existing NAK
    /// repair path once `HS_KEY` lands" is only true if something makes it
    /// land again: a NAK'd retransmit is itself `DATA`, sealed under the very
    /// epoch the peer is missing.
    fn crypto_redeliver_unacked(&mut self, now: u64) {
        if let Some(last) = self.crypto_last_redeliver_ns
            && now.saturating_sub(last) < CRYPTO_HS_KEY_REDELIVER_NS
        {
            return;
        }
        self.crypto_last_redeliver_ns = Some(now);
        let acts = self
            .crypto
            .as_ref()
            .map(|c| {
                // Ask against the CURRENT peer set, not the mint's delivery
                // list. `unacked_group_key_peers` can only name peers the mint
                // knew about, so a node that joined the peer set afterwards is
                // invisible to it — never unacked, never redelivered to, and
                // holding no group key for as long as this leader reigns, with
                // its fan-out dropped "no usable group key" throughout. The
                // ordinary boot sequence reaches this: a node elects itself
                // under a solo genesis config (mint correct, delivered to
                // nobody), then adopts the real multi-voter config.
                let missing = c.group_key_missing_peers(&self.gossip_targets());
                if missing.is_empty() { Vec::new() } else { c.redeliver_group_key_to(&missing) }
            })
            .unwrap_or_default();
        self.crypto_exec(acts);
    }

    /// Execute the crate's `HandshakeAction`s. The driver never touches a
    /// socket itself — that split is what keeps `Peers`/`GroupPlane` pure and
    /// unit-testable — so this is the one place actions become datagrams.
    fn crypto_exec(&mut self, actions: Vec<HandshakeAction>) {
        let mut queue = std::collections::VecDeque::from(actions);
        // FIFO, not LIFO: `Peers::on_init` emits `[Send(HS_RESP),
        // Established]` in that order, and popping from the back would send
        // this leader's `HS_KEY` re-key BEFORE the `HS_RESP` that lets the
        // peer complete the session and open it. Self-healing (the re-delivery
        // sweep retries within 200 ms) but a wasted round trip on every
        // link-up, for no reason other than the container's pop end.
        //
        // Belt: `Established` can enqueue a re-delivery, which cannot itself
        // enqueue anything, so the real bound is 2 rounds. A cap makes that
        // structural rather than argued.
        let mut budget = 4 * CRYPTO_HS_DRAIN_PER_CYCLE + 64;
        // Checked BEFORE the pop: decrementing after popping would silently
        // DISCARD the action that exhausted the budget rather than merely
        // stopping short of the rest of the queue. Only reachable on a mint to
        // >=320 peers (`MAX_MEMBERS` is 8), and the re-delivery sweep would
        // cover the loss anyway — but "drop one datagram, at a boundary, on a
        // path nobody exercises" is exactly the shape of bug that survives
        // until it matters.
        while budget > 0
            && let Some(act) = queue.pop_front()
        {
            budget -= 1;
            match act {
                HandshakeAction::Send { to, kind, body } => self.crypto_send(to, kind, body),
                HandshakeAction::Established { peer, boot_salt: _, confirmed: _ } => {
                    // NOTHING is cached from this action, deliberately.
                    //
                    // The plan told this task to "record the salt so
                    // `derive_send_key(group, peer, salt)` can open that
                    // peer's group-sealed traffic". That instruction is
                    // superseded: T9's review round 1 (findings F1/F2) moved
                    // that responsibility INSIDE `uc2_crypto` — see
                    // `transport.rs`'s "carried requirement #3". A cached
                    // salt is exactly the bug F2 demonstrated. The action's
                    // `confirmed: bool` is why: a session can be established
                    // but UNPROMOTED for up to 30s after a peer restart, so
                    // the salt in hand here is not necessarily the one in
                    // force, and `ReceiveHalf::open` re-reads the live one
                    // (current, then pending) on EVERY datagram instead.
                    //
                    // What DOES belong here is the other half of a peer
                    // restart: a peer whose session just came (back) up holds
                    // no group key at all — its new process minted none and
                    // was never delivered ours. Re-deliver, unconditionally:
                    // `GroupPlane` still counts it as acked from its previous
                    // life, so the un-acked sweep alone would never cover it.
                    if self.leader_flag.load(Ordering::Relaxed) {
                        let extra = self
                            .crypto
                            .as_ref()
                            .map(|c| c.redeliver_group_key_to(&[peer]))
                            .unwrap_or_default();
                        queue.extend(extra);
                    }
                }
                HandshakeAction::Failed { peer, reason } => {
                    self.crypto_handshake_failures.fetch_add(1, Ordering::Relaxed);
                    // Eager allowlist reload on a refused handshake: the
                    // likeliest cause is an M7 joiner whose key the operator
                    // has just dropped in but this node has not re-read yet
                    // (spec §5). `reload_if_stale` rate-limits itself to once
                    // a second, so an attacker cannot turn a stream of forged
                    // handshakes into a stream of disk reads. The peer's own
                    // `HS_INIT` retransmit (200 ms backoff, never gives up)
                    // is what retries the handshake once the reload lands.
                    let now = self.crypto_now_ns();
                    if let Some(crypto) = self.crypto.as_ref() {
                        let _ = crypto.allowlist_reload_if_stale(now);
                    }
                    self.crypto_log(
                        now,
                        format_args!("handshake with node {peer} refused: {reason}"),
                    );
                }
            }
        }
    }

    /// Build, seal (if the kind calls for it) and send one handshake-plane
    /// datagram. A seal failure DROPS the datagram — it is never sent in the
    /// clear, which would make the whole feature optional per datagram.
    fn crypto_send(&mut self, to: NodeId, kind: u8, body: Vec<u8>) {
        let Some(&addr) = self.id_to_addr.get(&to) else {
            self.crypto_unresolved_peer.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN + body.len()];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: 0,
                leadership_term_id: self.sm.current_term(),
                kind,
                flags: 0,
                key_epoch: 0,
            },
        );
        d[DATAGRAM_HEADER_LEN..].copy_from_slice(&body);

        // `HS_INIT`/`HS_RESP` are `Scope::Unsealed` BY DESIGN — they are what
        // creates the session there is nothing to seal under yet. `HS_KEY`
        // (kind 20) rides the ALREADY-established pairwise channel and is
        // sealed here, by the node layer: `GroupPlane` emits the body and
        // deliberately never touches a socket or a pairwise key. Which is
        // which comes from `Transport::scope_of`, the single place that rule
        // is encoded, rather than a second `kind` match here.
        if !matches!(Transport::scope_of(kind), Scope::Unsealed) {
            let sealed = self
                .crypto
                .as_ref()
                .map(|c| c.seal_pairwise_control(kind, to, &mut d));
            match sealed {
                Some(Ok(())) => {}
                other => {
                    self.crypto_hs_key_seal_failures.fetch_add(1, Ordering::Relaxed);
                    let now = self.crypto_now_ns();
                    self.crypto_log(
                        now,
                        format_args!(
                            "dropped a kind-{kind} datagram for node {to}: {}",
                            match other {
                                Some(Err(e)) => e.to_string(),
                                _ => "crypto disabled".to_string(),
                            }
                        ),
                    );
                    return;
                }
            }
        }
        let _ = self.sock.send_to(&d, addr);
    }

    /// Throttled operator diagnostic — see [`CRYPTO_LOG_INTERVAL_NS`]. The
    /// counters this accompanies are always exact; only printing is floored.
    fn crypto_log(&mut self, now: u64, args: std::fmt::Arguments<'_>) {
        if now.saturating_sub(self.crypto_last_log_ns) < CRYPTO_LOG_INTERVAL_NS
            && self.crypto_last_log_ns != 0
        {
            return;
        }
        self.crypto_last_log_ns = now;
        eprintln!("node {}: crypto: {args}", self.id);
    }

    /// Newest group epoch this node has minted, or `None` if it never has.
    #[cfg(test)]
    fn crypto_epoch(&self) -> Option<u16> {
        match self.crypto_epoch.load(Ordering::Acquire) {
            0 => None,
            e => Some(e as u16),
        }
    }

    /// `HS_KEY` deliveries dropped because they could not be sealed.
    #[cfg(test)]
    fn crypto_hs_key_seal_failures(&self) -> u64 {
        self.crypto_hs_key_seal_failures.load(Ordering::Relaxed)
    }

    /// M8 (Task 17): consensus-plane datagrams dropped because they could not
    /// be sealed.
    #[cfg(test)]
    fn crypto_seal_failures(&self) -> u64 {
        self.crypto_seal_failures.load(Ordering::Relaxed)
    }

    /// The live sender-identity map handed to the receive seam.
    #[cfg(test)]
    fn crypto_peer_ids(&self) -> Option<&PeerIds> {
        self.crypto_peer_ids.as_ref()
    }

    /// M6 Task 9: refresh the cnc observability band. Writes the static
    /// `id_and_role` cells once (first cycle), then each peer's `reported_durable`
    /// from the in-memory tracker, and mirrors the archive's first-retained base.
    /// Diagnostics only — never gates correctness.
    fn publish_peer_band(&mut self) {
        if !self.peer_band_published {
            for (i, (id, role)) in self.peer_band.iter().enumerate() {
                self.cnc
                    .peer_slot(i)
                    .id_and_role
                    .store_release(uc2_log::cnc::pack_id_and_role(*id, *role));
            }
            // M7 review finding (Task 11 gate authoring): a rebuild that
            // SHRINKS the peer band (a demote/remove-voter/remove-learner
            // that drops the total member count) must also zero every
            // trailing slot beyond the new length — otherwise a stale
            // `id_and_role` from the PREVIOUS, longer band lingers forever
            // (this loop previously only ever wrote `0..peer_band.len()`, never
            // clearing anything beyond it), producing a ghost duplicate entry
            // for a still-live id at its old index alongside its real, newly
            // rewritten slot. Diagnostics-only band (never gates correctness —
            // the real membership/quorum state lives in the SM's own
            // rebuilt tracker), but `uc2ctl status` and the runbook's
            // staleness warning read exactly this band, so a stale ghost
            // slot is a real, user-visible bug.
            for i in self.peer_band.len()..CNC_MAX_PEER_SLOTS {
                self.cnc.peer_slot(i).id_and_role.store_release(0);
            }
            self.peer_band_published = true;
        }
        for (i, (id, _)) in self.peer_band.iter().enumerate() {
            if let Some(d) = self.peer_reported.get(id) {
                self.cnc.peer_slot(i).reported_durable.store_release(*d);
            }
        }
        // Mirror the archive's first-retained base (the purge floor). Comparing it
        // against `node_snapshot_floor` is the "purge caught up to snapshot" check.
        let first_base = self.archive_first_base.load(Ordering::Acquire);
        self.cnc.archive_first_base().store_release(first_base);
    }

    /// M7: rebuild the node's own peer-address maps + observability band from
    /// a newly-adopted `ClusterConfig` (`Action::ConfigAdopted` — both a
    /// forward adoption and the SM's own truncation-revert re-adoption).
    /// Shares `derive_peer_maps` with `Node::start`'s construction-time
    /// seeding (the identical derivation, genesis or live) — a cluster that
    /// never reconfigures gets byte-identical maps either way.
    fn rebuild_peer_maps(&mut self, config: &ClusterConfig) {
        let (id_to_addr, addr_to_id, peers, learner_ids, peer_band) =
            derive_peer_maps(config, self.id);
        self.id_to_addr = id_to_addr;
        self.addr_to_id = addr_to_id;
        self.peers = peers;
        self.learner_ids = learner_ids;
        self.peer_band = peer_band;
        // Rung A §4/§5: ANY voter-set change voids the in-flight probe round —
        // a round whose quorum was captured under the old config must not
        // certify under the new one (a resized quorum could elect elsewhere
        // before the old-config ack count means anything; mirrors the
        // leader-lease brief's M7 invalidation rule). Pending reads are NOT
        // dropped — they wait for the next round, issued with a freshly
        // captured quorum by the same duty cycle's advance_pending_reads.
        self.current_round = None;
        // Re-publish `id_and_role` for the (possibly changed) membership next
        // cycle; prune reported-durable entries for ids no longer in the band.
        self.peer_band_published = false;
        let live: Vec<NodeId> = self.peer_band.iter().map(|(id, _)| *id).collect();
        self.peer_reported.retain(|id, _| live.contains(id));
        // M8 (Task 12): republish the receive seam's sender-identity map
        // SYNCHRONOUSLY, here — not on the next maintenance pass. M7 adds
        // nodes at runtime, and the joiner's first datagrams can arrive
        // before this node's next duty cycle; an address the map does not
        // know is dropped as `dropped_unknown_peer` before it is ever
        // authenticated. The handshake half (asking `Peers` for a link to the
        // new peer) is deferred to the maintenance pass — it takes the shared
        // lock and produces datagrams, neither of which belongs on an
        // adoption path that also runs during boot recovery.
        if let Some(ids) = self.crypto_peer_ids.as_ref() {
            ids.store(self.addr_to_id.iter().map(|(a, i)| (*a, *i)).collect::<Vec<_>>());
            self.crypto_peers_dirty = true;
        }
    }

    /// M7 Task 9: rebuild the sender's net layer (`CtrlMsg::SetPeers`) AND this
    /// node's own routing/observability (`rebuild_peer_maps` +
    /// `publish_peer_band`) for a newly-adopted `ClusterConfig`. Factored out of
    /// `Action::ConfigAdopted`'s exec arm so the snapshot-fiat install path in
    /// `maybe_adopt_incoming_snapshot` shares the IDENTICAL derivation — a
    /// below-floor joiner's installed config can differ from its boot seed
    /// (T7 shipped live reconfiguration), so it needs exactly this rebuild too,
    /// not a second hand-rolled copy that could drift from this one.
    ///
    /// Final-review fix: ALSO refreshes the `config_bytes` snapshot-session
    /// config-carry cache here (`config_wire_bytes(config, prev_position)`) —
    /// this is now the single site both live callers share, so the
    /// snapshot-fiat install path (which calls this but never used to touch
    /// the cache) can no longer leave it stale. `prev_position` is the
    /// audit-trail field the caller would otherwise pass to `cluster_to_wire`
    /// itself (the exec arm's adopted `prev_position`; the fiat path's own
    /// floor position, since a wholesale-replace install sets `prev == config`
    /// at that same position).
    fn rebuild_net_for_config(&mut self, config: &ClusterConfig, prev_position: u64) {
        // Rebuild the net layer: voters-minus-self / learners-minus-self,
        // DISJOINT sets (`CtrlMsg::SetPeers`'s documented convention —
        // the sender recombines them for its own fan-out).
        let followers: Vec<SocketAddr> = config
            .voters
            .iter()
            .filter(|(id, _)| *id != self.id)
            .map(|(_, a)| addr_of(*a))
            .collect();
        let learners: Vec<SocketAddr> = config
            .learners
            .iter()
            .filter(|(id, _)| *id != self.id)
            .map(|(_, a)| addr_of(*a))
            .collect();
        // M7 Task 8: `sender_cluster_size` (not the plain voter count) —
        // a LEADER mid-self-removal keeps rebuilding its OWN real sender
        // here while `config` no longer contains it, and a removed
        // FOLLOWER's now-moot sender is rebuilt one last time before it
        // halts; both need the "self occupies an uncounted +1 slot"
        // adjustment or `FlowControl::new`'s invariant assert panics the
        // sender thread on the spot (see the helper's doc). The fiat-install
        // caller's joiner is typically a learner (not yet a voter in its own
        // seed), exercising this same +1 branch.
        let _ = self.sender_ctrl.send(CtrlMsg::SetPeers {
            followers,
            learners,
            cluster_size: sender_cluster_size(config, self.id),
        });
        // Final-review fix: refresh the snapshot-session config-carry cache —
        // see the doc comment above for why this lives here now.
        *self.config_bytes.lock().unwrap() = config_wire_bytes(config, prev_position);
        // Refresh the node's own routing + observability.
        self.rebuild_peer_maps(config);
        self.publish_peer_band();
    }

    /// M6 Task 6. When the receiver completes an inbound snapshot transfer it
    /// raises `incoming_snapshot`; sample it and, on a new position, mirror it to
    /// the cnc observability slot and — when our own durable frontier is below it
    /// (the learner-join case) — command the archive to adopt it as the floor.
    fn maybe_adopt_incoming_snapshot(&mut self) -> bool {
        let pos = self.incoming_snapshot.load(Ordering::Acquire);
        if pos <= self.adopted_incoming {
            return false;
        }
        self.adopted_incoming = pos;
        self.cnc.snapshots().incoming_snapshot_pos.store_release(pos);
        // Only adopt when we don't already cover `pos` — a mid-life follower that
        // already holds the state ignores it (the archive agent no-ops it too, but
        // skipping the command keeps the channel quiet).
        let durable = self.cnc.counters().durable.load_acquire();
        if durable < pos {
            // M6 Task 8: seed the SM with the leader's authoritative lineage BEFORE
            // the archive adopts the floor. The snapshot IS the leader's committed
            // history up to `pos`, so its lineage — not our absent local bytes — is
            // the truth the next reconcile must match. Without this the shared
            // prefix lives inside the snapshot, invisible to reconcile, which would
            // clamp a truncate below the adopted floor (a PositionPurged fail-stop).
            // Persist-before-adopt: the seeded map is durable before the floor moves,
            // so a crash in the window recovers a map consistent with the floor.
            if !self.last_leader_map.is_empty() {
                self.sm.adopt_snapshot_lineage(&self.last_leader_map);
                let map = to_entries(self.sm.term_map());
                self.state.store_term_map(&map).expect("term-map persist fail-stop");
            }
            // M7 Task 6: the snapshot session carried the leader's config alongside
            // the lineage (`SnapBeginBody.config`) — adopt it by fiat for the
            // identical reason the lineage is: our own absent local bytes carry
            // nothing genuine to fall back to below the floor. Persist-before-
            // adopt-floor, same ordering discipline as the lineage seed above.
            // M7 Task 9: the installed config can DIFFER from this joiner's boot
            // seed — T7 shipped the admin propose path, so membership can have
            // changed live since the seed was drawn. Left as-is, this node would
            // keep routing off its stale seed (deaf to any member the seed
            // doesn't know about) until the next live config change reached it.
            // So after adopting the config we rebuild the net layer + our own
            // routing exactly as `Action::ConfigAdopted`'s exec arm does (shared
            // via `rebuild_net_for_config` — one derivation, not two that could
            // drift). The one-in-flight rule (`ElectionSm::propose_config`'s
            // ChangePending refusal) is what keeps the one-level ConfigRecord
            // history sufficient here — do not weaken it.
            let cfg_bytes = self.incoming_snapshot_config.lock().unwrap().clone();
            if !cfg_bytes.is_empty() {
                let wire = decode_config(&cfg_bytes)
                    .unwrap_or_else(|| panic!("corrupt snapshot-carried CONFIG at floor {pos}"));
                let cfg = wire_to_cluster_config(&wire);
                self.sm.adopt_snapshot_config(pos, cfg.clone());
                let rec = ConfigRecord {
                    position: pos,
                    config: cluster_to_stored(&cfg),
                    prev_position: pos,
                    prev: cluster_to_stored(&cfg),
                };
                self.state.store_config_record(&rec).expect("config persist fail-stop");
                self.cnc.store_config_version(cfg.version);
                // Post-M7 follow-up: a fiat install has no in-flight change
                // by construction (cur == prev at the floor) — clear the
                // pending mirror too, or a stale pre-crash `true` sticks
                // until the NEXT live change commits.
                self.cnc.store_config_pending(false);
                // Final-review fix: this call now ALSO refreshes `config_bytes`
                // (previously only `Action::ConfigAdopted`'s exec arm did, so a
                // below-floor rejoiner that later became leader would ship its
                // STALE pre-fall config in SNAP_BEGIN to the next joiner). `pos`
                // doubles as the prev_position audit field since this is a
                // wholesale-replace install: `rec.prev == rec.config` at `pos`.
                self.rebuild_net_for_config(&cfg, pos);
            }
            let _ = self.trunc_tx.try_send(ArchiveCmd::AdoptFloor { pos });
        }
        crate::obs_event!(Info, "snapshot_installed", node = self.id as u64, pos = pos);
        true
    }

    /// Sample `service().output_completed` (a cheap compare every cycle); on an
    /// INCREASE, once at least [`OUTPUT_PROGRESS_FLOOR_NS`] has elapsed since the
    /// last persist, durably store it via `NodeState::store_output_progress`
    /// THEN mirror it onto `status().output_progress` — durable-then-mirror,
    /// never the other order, so an attaching service can never observe a
    /// mirror value ahead of what actually survives a node crash. Returns
    /// `true` iff it persisted (drives the idle strategy).
    ///
    /// **Increase-only — the marker is a durable HIGH-WATER MARK.** The cnc
    /// page is re-created fresh every node boot, so `output_completed` restarts
    /// at 0 while `output_persisted_completed` is seeded from the recovered
    /// durable marker M. A plain not-equal check would treat that boot-time
    /// `0 != M` as a change and persist 0 on the very first cycle —
    /// deterministically regressing the on-disk marker after ANY node restart
    /// (at-least-once SAFE, but it defeats the marker's purpose: the next
    /// leader would replay outputs from 0/the purge floor). A fresh page's 0
    /// (or any other lower value) must never regress it.
    fn maybe_persist_output_progress(&mut self) -> bool {
        let completed = self.cnc.service().output_completed.load_acquire();
        if completed <= self.output_persisted_completed {
            return false;
        }
        let now = self.now_ns();
        let floor_elapsed = self
            .output_progress_last_persist_ns
            .is_none_or(|last| now.saturating_sub(last) >= OUTPUT_PROGRESS_FLOOR_NS);
        if !floor_elapsed {
            return false;
        }
        self.state
            .store_output_progress(completed)
            .expect("output_progress persist fail-stop (journal I/O)");
        self.cnc.status().output_progress.store_release(completed);
        self.output_persisted_completed = completed;
        self.output_progress_last_persist_ns = Some(now);
        true
    }

    /// M6 Task 4. Sample the service-written `snapshots().service_snapshot_pos`
    /// (a cheap compare every cycle); on a VALIDATED increase durably persist
    /// the snapshot floor via `NodeState::store_snapshot_floor` THEN mirror it
    /// onto `snapshots().node_snapshot_floor` (durable-then-mirror, same order
    /// as output-progress), and — when a purge policy is configured — command
    /// the archive to drop journal below `floor - slack`. Returns `true` iff it
    /// did fsync or purge work (drives the idle strategy).
    ///
    /// **Increase-only, and validated `<= durable`.** Increase-only is the same
    /// high-water-mark discipline as `maybe_persist_output_progress` (the cnc
    /// page is fresh every boot; `snapshot_persisted_floor` is seeded from the
    /// recovered durable floor so a boot-time `service_snapshot_pos == 0` cannot
    /// regress it) — but here regressing the floor would be a SAFETY bug, not
    /// mere at-least-once slack: a purge floor must never move backwards. The
    /// `<= durable` gate rejects a service value ahead of this node's fsync
    /// frontier (a torn/racy read, or a snapshot at a not-yet-durable position)
    /// — a purge floor is only ever a position whose covering journal block is
    /// itself durable here.
    ///
    /// The fsync + the archive command are throttled to [`OUTPUT_PROGRESS_FLOOR_NS`]
    /// (the cheap in-page compares still run every cycle); a purge that couldn't
    /// advance the floor (archive not yet caught up, or a prior best-effort
    /// purge that failed) simply retries on the next tick.
    fn maybe_persist_snapshot_floor(&mut self) -> bool {
        let service_pos = self.cnc.snapshots().service_snapshot_pos.load_acquire();
        let durable = self.cnc.counters().durable.load_acquire();
        let have_new_floor =
            service_pos > self.snapshot_persisted_floor && service_pos <= durable;
        let purge_on = matches!(self.purge_policy, PurgePolicy::BelowSnapshot { .. });
        // Cheap exit every cycle when there is neither a newer floor to persist
        // nor a purge policy that might have outstanding work.
        if !have_new_floor && !purge_on {
            return false;
        }
        let now = self.now_ns();
        let floor_elapsed = self
            .snapshot_floor_last_persist_ns
            .is_none_or(|last| now.saturating_sub(last) >= OUTPUT_PROGRESS_FLOOR_NS);
        if !floor_elapsed {
            return false;
        }

        let mut did = false;
        if have_new_floor {
            self.state
                .store_snapshot_floor(service_pos)
                .expect("snapshot floor persist fail-stop (journal I/O)");
            self.cnc.snapshots().node_snapshot_floor.store_release(service_pos);
            self.snapshot_persisted_floor = service_pos;
            did = true;
        }
        if let PurgePolicy::BelowSnapshot { slack_bytes } = self.purge_policy {
            let target = self.snapshot_persisted_floor.saturating_sub(slack_bytes);
            // Only command a purge that would actually advance the floor. The
            // archive acks by advancing `archive_first_base`; `try_send` (not
            // `send`) keeps a full channel from blocking the consensus loop — a
            // dropped purge is harmless (best-effort, retried next tick).
            if target > self.archive_first_base.load(Ordering::Acquire) {
                let _ = self.trunc_tx.try_send(ArchiveCmd::Purge { below: target });
                did = true;
            }
        }
        if did {
            self.snapshot_floor_last_persist_ns = Some(now);
        }
        did
    }

    /// Publish `term`, `flags` (leader/can_serve), and a fresh wall-clock
    /// heartbeat onto the cnc page. `leader_hint` is published event-driven
    /// (see `feed_net` / `exec`), not here.
    ///
    /// M7: once `halt_removed`, LEADER/CAN_SERVE are forced OFF regardless of
    /// `leader_flag`/`sm.can_serve()` — an attaching service/client must never
    /// mistake a removed, permanently-parked node for a live leader/server.
    fn publish_status(&mut self) {
        let status = self.cnc.status();
        let term = self.sm.current_term();
        status.term.store_release(term as u64);
        let mut flags = 0u64;
        if !self.halt_removed {
            if self.leader_flag.load(Ordering::Relaxed) {
                flags |= NODE_FLAG_LEADER;
            }
            if self.sm.can_serve() {
                flags |= NODE_FLAG_CAN_SERVE;
            }
        }
        status.flags.store_release(flags);
        // M10: edge-detect the CAN_SERVE bit only — one branch, no allocation
        // on the untaken (steady-state) path.
        if (flags ^ self.last_flags) & NODE_FLAG_CAN_SERVE != 0 {
            crate::obs_event!(
                Info,
                "serving_changed",
                node = self.id as u64,
                term = term as u64,
                can_serve = flags & NODE_FLAG_CAN_SERVE != 0
            );
        }
        self.last_flags = flags;
        let now_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        status.node_heartbeat_ns.store_release(now_ns);
    }

    fn now_ns(&self) -> u64 {
        self.base.elapsed().as_nanos() as u64
    }

    /// Append pending + queued payloads via the leader appender, bounded.
    fn drain_ingress(&mut self) -> bool {
        let mut did = false;
        // Retry a payload held back by a prior WouldOverrun before taking more.
        if let Some(p) = self.pending_ingress.take() {
            if !self.try_append(&p) {
                self.pending_ingress = Some(p);
                return did;
            }
            did = true;
        }
        for _ in 0..INGRESS_PER_CYCLE {
            match self.ingress_rx.try_recv() {
                Ok(p) => {
                    if !self.try_append(&p) {
                        self.pending_ingress = Some(p); // ring full: hold, retry next cycle
                        break;
                    }
                    did = true;
                }
                Err(_) => break,
            }
        }
        did
    }

    /// Append one payload; `false` = ring full (caller holds it). A too-large
    /// payload is dropped (a client contract violation, not backpressure).
    fn try_append(&mut self, payload: &[u8]) -> bool {
        let Some(app) = self.appender.as_mut() else { return false };
        match app.append(0, self.next_corr, payload) {
            Ok(_) => {
                self.next_corr += 1;
                true
            }
            Err(AppendError::WouldOverrun) => false,
            Err(AppendError::PayloadTooLarge) => {
                self.next_corr += 1;
                true // consumed (dropped) — do not wedge the queue on it
            }
        }
    }

    /// M7 leader append path: encode `new_cfg` as a `FRAME_TYPE_CONFIG` payload
    /// superseding the currently-adopted config (`self.sm.config_position()`
    /// is the wire `prev_position` audit field), append it via the leader
    /// appender, and adopt-at-append by feeding the event back to ourselves
    /// immediately — the archive re-observes the same durable frame later
    /// (`do_work` step 1c), which is a harmless no-op re-adoption (idempotent
    /// by version). Returns the frame-END position (the new
    /// `ConfigRecord.position`).
    ///
    /// Task 7's admin propose path (`ElectionSm::propose_config` ->
    /// `propose_and_append` -> this) is the caller. On `Err` (the ring is
    /// momentarily full, `AppendError::WouldOverrun`, or the vanishingly
    /// unlikely `PayloadTooLarge`) nothing has been appended and nothing has
    /// been fed to the SM — `propose_config` never mutated state either, so
    /// the caller's retry sees a byte-for-byte unchanged SM (see
    /// `propose_and_append`'s doc for the full argument).
    fn append_config_frame(&mut self, new_cfg: &ClusterConfig) -> Result<u64, AppendError> {
        let term = self.sm.current_term();
        let wire = cluster_to_wire(new_cfg, self.sm.config_position());
        let mut bytes = Vec::new();
        encode_config(&wire, &mut bytes);
        let position = self
            .appender
            .as_mut()
            .expect("append_config_frame is leader-only")
            .append_config(term, &bytes)?;
        self.feed(Event::ConfigObserved { position, config: new_cfg.clone() });
        Ok(position)
    }

    /// Drain the client ingress MPSC ring, bounded by `INGRESS_PER_CYCLE`
    /// whether `serving` or not (a saturated ring must never starve the rest
    /// of the duty cycle). While serving, each record is appended via the
    /// leader appender — gated by the admission window (`append - commit <=
    /// admission_bytes`); once the window closes, drainage stops for this
    /// cycle and the remaining records stay in the ring (backpressuring the
    /// client's `try_write` into `RingError::Full` once it also fills).
    /// While NOT serving, every drained record is answered with
    /// `MSG_V2_NOT_LEADER` on the node egress broadcast instead of being
    /// appended.
    fn drain_ingress_ring(&mut self, serving: bool) -> bool {
        let mut did = false;

        // Retry a record held back by a prior WouldOverrun before taking
        // more — it was already consumed off the ring, so it must not be
        // dropped. Only meaningful while serving (only path that appends);
        // a role flip while one is pending just carries it to the next
        // serving window (the ring itself has no memory of it any more).
        if serving && let Some((client_id, local_seq, payload)) = self.pending_ring_ingress.take() {
            if !self.try_append_client(client_id, local_seq, &payload) {
                self.pending_ring_ingress = Some((client_id, local_seq, payload));
                return did;
            }
            did = true;
        }

        for _ in 0..INGRESS_PER_CYCLE {
            if serving {
                let append = self.cnc.counters().append.load_acquire();
                let commit = self.cnc.counters().commit.load_acquire();
                if !admission_open(append, commit, self.admission_bytes) {
                    break; // door closed; leave the rest in the ring this cycle
                }
            }
            let mut buf = Vec::new();
            match self.ingress_ring.try_read(&mut buf) {
                Ok(Some(rec)) => {
                    let (client_id, local_seq) = client_from_extra(rec.header_extra);
                    if serving {
                        if !self.try_append_client(client_id, local_seq, &buf) {
                            self.pending_ring_ingress = Some((client_id, local_seq, buf));
                            break;
                        }
                    } else {
                        self.send_not_leader(client_id, local_seq);
                    }
                    did = true;
                }
                Ok(None) => break,
                // Corrupt record (bad crc/magic — the wire has no per-record
                // recovery once framing is suspect): stop this cycle rather
                // than risk misreading a subsequent slot; the next cycle
                // re-tries at the same (unread) consumer position.
                Err(_) => break,
            }
        }
        did
    }

    /// Append one client-stamped ring record; `false` = would overrun
    /// (caller holds it back for retry). A too-large payload is dropped (a
    /// client contract violation, not backpressure) — same policy as
    /// `try_append`.
    fn try_append_client(&mut self, client_id: u32, local_seq: u32, payload: &[u8]) -> bool {
        let Some(app) = self.appender.as_mut() else { return false };
        match app.append(client_id as u64, local_seq as u64, payload) {
            Ok(_) => true,
            Err(AppendError::WouldOverrun) => false,
            Err(AppendError::PayloadTooLarge) => true, // consumed (dropped)
        }
    }

    /// Answer a drained ingress record with `MSG_V2_NOT_LEADER` on the node
    /// egress broadcast, echoing the client's identity in `header_extra` so
    /// it can pick its own answer out of the shared broadcast (every client
    /// sees every record). Payload is the current `leader_hint`
    /// (`u64::MAX` = unknown) as 8 LE bytes. Best-effort: `BroadcastProducer`
    /// never blocks and a write failure here (e.g. an oversized frame, which
    /// this fixed 8-byte payload can never be) is not actionable.
    fn send_not_leader(&mut self, client_id: u32, local_seq: u32) {
        let leader_hint = self.cnc.status().leader_hint.load_acquire();
        let extra = extra_client(client_id, local_seq);
        let _ = self.egress_node.write(MSG_V2_NOT_LEADER, 0, extra, &leader_hint.to_le_bytes());
    }

    /// Answer a linearizable read with `MSG_V2_RETRY` on the node egress. Emitted
    /// ONLY on a barrier deadline or a leadership loss — BOTH pre-answer and
    /// side-effect-free: a query never mutates the SM, and this read is dropped
    /// before it is ever forwarded, so the client may safely re-issue (the
    /// cross-task RETRY-is-side-effect-free invariant, Task 10 review).
    fn send_retry(&mut self, client_id: u32, local_seq: u32) {
        let extra = extra_client(client_id, local_seq);
        let _ = self.egress_node.write(MSG_V2_RETRY, 0, extra, &[]);
    }

    /// Forward a barrier-passed (or snapshot) read to the service on
    /// `svc_query.ring`: payload = `expected_epoch: u64 LE ++ query bytes`,
    /// `header_extra` echoing the client identity so the service's answer routes
    /// back. `expected_epoch == 0` means "skip the epoch check" (snapshot reads).
    /// Returns `false` if the ring is momentarily full (the caller retries).
    fn forward_svc_query(
        &mut self,
        client_id: u32,
        local_seq: u32,
        expected_epoch: u64,
        query: &[u8],
    ) -> bool {
        let mut payload = Vec::with_capacity(8 + query.len());
        payload.extend_from_slice(&expected_epoch.to_le_bytes());
        payload.extend_from_slice(query);
        let extra = extra_client(client_id, local_seq);
        self.svc_query.try_write(MSG_V2_SVC_QUERY, 0, extra, &payload).is_ok()
    }

    /// Send a nonce'd READ_PROBE to every follower over the consensus socket,
    /// stamped with our current term (a follower acks only a probe whose term
    /// still equals its own — the no-stale-read filter lives on the follower).
    /// Gossip fan-out targets: voting peers ++ learners. Commit gossip and term
    /// maps go to both (learners replicate + reconcile); votes and READ_PROBEs go
    /// to voters only. Collected into an owned Vec so the caller can `self.send`
    /// inside the loop (which needs `&mut self`).
    ///
    /// M8 (Task 12) reuses this VERBATIM as the crypto peer set — every peer
    /// this node keeps a pairwise link with and delivers the group key to.
    /// That is the same set by definition (spec §5: "learners are peers like
    /// any other" — they replicate the fan-out, so they need the group key),
    /// and T12 originally had a character-identical private copy. One
    /// definition, so a future change to the fan-out cannot leave the crypto
    /// plane silently keyed for the old one.
    fn gossip_targets(&self) -> Vec<NodeId> {
        self.peers.iter().chain(self.learner_ids.iter()).copied().collect()
    }

    fn send_read_probe(&mut self, nonce: u64) {
        let term = self.sm.current_term();
        let mut body = [0u8; READ_PROBE_BODY_LEN];
        write_read_probe_body(&mut body, &ReadProbeBody { nonce, from: self.id });
        // M8 (T17): `Scope::Group` — one seal, N sends. Every voter gets the
        // byte-identical probe, so a per-destination seal here would be N
        // AEAD calls and N nonces for one logical round.
        let targets = self.peers.clone();
        self.fan_out_group(&targets, DGRAM_KIND_READ_PROBE, 0, term, &body);
    }

    /// Rung A §4: issue a probe round iff at least one read awaits quorum and
    /// no round is in flight. Self-clocking — called at the end of
    /// `drain_query_ring` and from `advance_pending_reads`, so a completed
    /// round is immediately followed by the next while demand persists (~1
    /// round per RTT, independent of read rate; a lone read still gets its
    /// own immediate round, one RTT, exactly today's latency).
    fn maybe_issue_round(&mut self) {
        if self.current_round.is_some() {
            return;
        }
        if !self.pending_reads.iter().any(|r| r.phase == ReadPhase::AwaitQuorum) {
            return;
        }
        let quorum = self.peers.len().div_ceil(2) + 1;
        if quorum <= 1 {
            // A legal 2->1 voter shrink can strand AwaitQuorum reads across
            // rebuild_peer_maps (which voids the round but keeps the reads).
            // A sole voter's majority is itself alone — no election can
            // succeed at any term without our vote, so we cannot be
            // unknowingly deposed — which is exactly admission's single-node
            // fast-path argument. Certify the stranded reads directly; no
            // round is created.
            for r in self.pending_reads.iter_mut() {
                if r.phase == ReadPhase::AwaitQuorum {
                    r.phase = ReadPhase::AwaitApplied;
                }
            }
            return;
        }
        let seq = self.next_round_seq;
        self.next_round_seq += 1;
        let nonce = self.next_nonce;
        self.next_nonce += 1;
        let round = ProbeRound::new(
            seq,
            nonce,
            quorum,
            self.id,
            self.sm.current_term(),
            self.cnc.counters().commit.load_acquire(),
            self.now_ns(),
        );
        self.current_round = Some(round);
        self.send_read_probe(nonce);
    }

    /// Follower side of a READ_PROBE: membership-check the probing leader, then
    /// reply a READ_PROBE_ACK **iff** the datagram's term still equals our
    /// current term. A stale leader's probe (a lower/older term) dies here — this
    /// is the teeth of the no-stale-read theorem: a deposed leader can never
    /// collect the read quorum, so it can never certify a linearizable read.
    fn on_read_probe(&mut self, nonce: u64, from: NodeId, term: u32) {
        let Some(&addr) = self.id_to_addr.get(&from) else { return };
        if term != self.sm.current_term() {
            return;
        }
        let mut body = [0u8; READ_PROBE_BODY_LEN];
        write_read_probe_body(&mut body, &ReadProbeBody { nonce, from: self.id });
        self.send(addr, DGRAM_KIND_READ_PROBE_ACK, 0, self.sm.current_term(), &body);
    }

    /// Leader side of a READ_PROBE_ACK: membership-check the acker, match the
    /// ONE in-flight round by nonce, and count DISTINCT ackers. On quorum the
    /// round certifies every read that was already waiting when it was issued
    /// (the §3.2 ordering rule) — never a read admitted mid-round.
    fn on_read_probe_ack(&mut self, nonce: u64, from: NodeId) {
        // The read quorum is over VOTERS only (M6 Task 7 constraint): re-check
        // membership so a learner's (or forged/misrouted) ack can never
        // complete a round. `peers` is the voting set minus self.
        if !self.peers.contains(&from) {
            return;
        }
        let Some(round) = self.current_round.as_mut() else { return };
        if round.nonce != nonce {
            return; // an abandoned/completed round's straggler ack
        }
        if !round.record_ack(from) {
            return;
        }
        // Quorum: consume the round and release its certification set.
        let round = self.current_round.take().expect("matched above");
        for r in self.pending_reads.iter_mut() {
            if r.phase == ReadPhase::AwaitQuorum && round.certifies(r.round_seq) {
                // §3.2 redundancy check (never the gate): commit is monotonic,
                // so a read waiting at issue has commit_at <= the round's.
                debug_assert!(
                    r.commit_at <= round.commit_at_issue,
                    "ordering rule implies the position bound"
                );
                r.phase = ReadPhase::AwaitApplied;
            }
        }
        // Mid-round arrivals (round_seq > seq) stay AwaitQuorum; the next
        // round — issued by advance_pending_reads this same duty cycle — will
        // cover them.
    }

    /// Drain the client query ring (bounded). Snapshot reads forward straight to
    /// the service; linearizable reads open a ReadIndex barrier (or redirect
    /// `MSG_V2_NOT_LEADER` while not serving).
    fn drain_query_ring(&mut self) -> bool {
        let mut did = false;
        for _ in 0..QUERY_DRAIN_PER_CYCLE {
            let mut buf = Vec::new();
            match self.query_ring.try_read(&mut buf) {
                Ok(Some(rec)) => {
                    did = true;
                    let (client_id, local_seq) = client_from_extra(rec.header_extra);
                    if rec.flags & FLAG_V2_LINEARIZABLE == 0 {
                        // Snapshot: forward immediately, epoch check skipped (0).
                        self.forward_svc_query(client_id, local_seq, 0, &buf);
                        continue;
                    }
                    // Linearizable: only a serving leader can confirm a read.
                    // Mutation tooth (`skip-read-barrier`): as an intentional bug,
                    // answer linearizable reads from LOCAL applied state with NO
                    // leadership check — an isolated/deposed leader then serves
                    // STALE reads (a real-time-only anomaly the strict model
                    // catches; see scripts/elle_mutation.sh). Reads only: writes
                    // stay gated by the real `can_serve` at the ingress drain.
                    //
                    // `!halt_removed`: a halt earlier in this SAME cycle does
                    // not clear the SM's serving field, so the raw check alone
                    // would admit reads no one can ever answer (Veil §5
                    // discharge, observation 1) — refuse them here instead.
                    let can_serve = self.sm.can_serve() && !self.halt_removed;
                    #[cfg(feature = "mutation-testing")]
                    let can_serve = can_serve
                        || matches!(
                            crate::mutation::active(),
                            Some(crate::mutation::Mutation::SkipReadBarrier)
                        );
                    if !can_serve {
                        self.send_not_leader(client_id, local_seq);
                        continue;
                    }
                    let n = self.peers.len() + 1;
                    let quorum = n / 2 + 1;
                    let commit_at = self.cnc.counters().commit.load_acquire();
                    let deadline_ns = self.now_ns() + READ_BARRIER_TIMEOUT_NS;
                    // Single-node (quorum 1): skip straight to AwaitApplied —
                    // unchanged by Rung A; such reads never touch a round.
                    let phase = if quorum <= 1 {
                        ReadPhase::AwaitApplied
                    } else {
                        ReadPhase::AwaitQuorum
                    };
                    #[cfg_attr(not(feature = "mutation-testing"), allow(unused_mut))]
                    let mut read = PendingRead {
                        client_id,
                        local_seq,
                        query: buf,
                        // Rung A §3.2: record the NEXT round's seq — only a
                        // round issued at-or-after this admission may certify.
                        round_seq: self.next_round_seq,
                        commit_at,
                        deadline_ns,
                        phase,
                    };
                    // Mutation tooth: skip the READ_PROBE quorum barrier entirely — the
                    // read is served from local applied state without confirming
                    // leadership. A deposed leader then answers stale reads (the elle
                    // partition pass catches this under the strict model).
                    #[cfg(feature = "mutation-testing")]
                    if matches!(
                        crate::mutation::active(),
                        Some(crate::mutation::Mutation::SkipReadBarrier)
                    ) {
                        read.phase = ReadPhase::AwaitApplied;
                    }
                    self.pending_reads.push(read);
                }
                Ok(None) => break,
                // Corrupt record: stop this cycle (retried at the same unread
                // position next cycle) — same posture as the ingress drain.
                Err(_) => break,
            }
        }
        // Rung A: one round for everything admitted this cycle (issue site 1
        // of 2; the other is advance_pending_reads, which chains rounds while
        // demand persists).
        self.maybe_issue_round();
        did
    }

    /// Advance every in-flight linearizable read one step. A read past its
    /// deadline, or held while leadership was lost, is answered `MSG_V2_RETRY`
    /// and dropped. An `AwaitApplied` read whose service has caught up to
    /// `commit_at` — verified with the capture-recheck epoch bracket (task14
    /// TOCTOU close) — is forwarded to the service and dropped.
    fn advance_pending_reads(&mut self) -> bool {
        if self.pending_reads.is_empty() {
            // Rung A: a round with no waiting reads certifies nobody — drop it
            // so the next admission starts a fresh round instead of waiting
            // out a stale one (its straggler acks no-op on the nonce check).
            self.current_round = None;
            return false;
        }
        let now = self.now_ns();
        // `!halt_removed`: sweep reads admitted after a same-cycle halt (the
        // SM's serving field survives step-down; Veil §5 discharge, obs. 1).
        let can_serve = self.sm.can_serve() && !self.halt_removed;
        // Rung A §4: a round never survives lost serving or a term change
        // (the voter-set trigger lives in rebuild_peer_maps). Checked against
        // the RAW can_serve, before the mutation shadow below: the tooth keeps
        // READS resolving on an isolated leader, but mutated reads bypass
        // rounds at admission, so no round should outlive real leadership.
        if let Some(round) = &self.current_round
            && (!can_serve || round.term != self.sm.current_term())
        {
            self.current_round = None;
        }
        // `skip-read-barrier` tooth: keep resolving reads from local applied
        // state even after leadership is lost, so an isolated leader answers
        // stale reads instead of RETRY-ing (matches the admission bypass above).
        #[cfg(feature = "mutation-testing")]
        let can_serve = can_serve
            || matches!(
                crate::mutation::active(),
                Some(crate::mutation::Mutation::SkipReadBarrier)
            );
        let mut did = false;
        let mut i = 0;
        while i < self.pending_reads.len() {
            // Deadline passed OR leadership lost → RETRY (side-effect-free),
            // drop. `swap_remove` reorders harmlessly (read order is irrelevant).
            if now >= self.pending_reads[i].deadline_ns || !can_serve {
                let r = self.pending_reads.swap_remove(i);
                self.send_retry(r.client_id, r.local_seq);
                did = true;
                continue;
            }
            if self.pending_reads[i].phase == ReadPhase::AwaitApplied {
                let commit_at = self.pending_reads[i].commit_at;
                // Capture-recheck bracket (verbatim per the brief): capture the
                // epoch `e`, require the service applied through `commit_at`, then
                // require the epoch is STILL `e`. A service restart mid-check
                // moves the epoch and fails the recheck — so a read is never
                // forwarded across a service incarnation boundary.
                let ready = {
                    let svc = self.cnc.service();
                    let e = svc.service_epoch.load_acquire();
                    let applied = svc.service_applied.load_acquire();
                    // Sentinel-collision guard (M5 final review IMPORTANT #1): a
                    // captured epoch of 0 is NOT ready — never forward on it.
                    // `expected_epoch == 0` is the wire sentinel for "skip the
                    // epoch check" (a snapshot read the service answers
                    // unconditionally — see `uc_protocol::v2::ipc` and
                    // `uc2_service::apply::drain_queries`). A fresh cnc page
                    // zeroes `service_epoch` on EVERY node boot, so the bracket
                    // could otherwise pass while NO live service has attached
                    // this generation — and a stale service incarnation still
                    // writing `service_applied` onto the recreated page (the
                    // crashtest's node_sigkill_recovery window) could push
                    // `applied >= commit_at` with `e == 0`. Forwarding then would
                    // (a) stamp the skip-the-check sentinel and (b) certify a
                    // read against an unattached / stale-incarnation SM. Require
                    // `e >= 1`: an unattached generation's read simply waits
                    // (deadline → RETRY as usual), and only a real attached
                    // incarnation (epoch bumped at attach) is ever forwarded —
                    // with its true epoch, so the service's own stale-epoch
                    // refusal still applies end-to-end.
                    if e >= 1 && applied >= commit_at && svc.service_epoch.load_acquire() == e {
                        Some(e)
                    } else {
                        None
                    }
                };
                if let Some(e) = ready {
                    let client_id = self.pending_reads[i].client_id;
                    let local_seq = self.pending_reads[i].local_seq;
                    let query = std::mem::take(&mut self.pending_reads[i].query);
                    if self.forward_svc_query(client_id, local_seq, e, &query) {
                        self.pending_reads.swap_remove(i);
                        did = true;
                        continue;
                    }
                    // svc_query ring momentarily full: restore the query bytes and
                    // retry next cycle (re-capturing the epoch bracket then).
                    self.pending_reads[i].query = query;
                }
            }
            i += 1;
        }
        // Rung A §4: re-probe a stuck round on the 2 ms interval (same seq,
        // same nonce — the certification set cannot widen), and chain the next
        // round the moment the previous completed while demand persists.
        let retransmit_nonce = self.current_round.as_mut().and_then(|round| {
            if round.should_retransmit(now) {
                round.mark_sent(now);
                Some(round.nonce)
            } else {
                None
            }
        });
        if let Some(nonce) = retransmit_nonce {
            self.send_read_probe(nonce);
        }
        self.maybe_issue_round();
        did
    }

    /// Translate a wire NetEvent into an SM event and feed it. Unknown source
    /// addresses (not a configured member) are dropped.
    fn feed_net(&mut self, ev: NetEvent) {
        let event = match ev {
            NetEvent::Report { from, term, durable, durable_term } => {
                let Some(id) = self.addr_to_id.get(&from).copied() else { return };
                // Implausibility guard (M4 I-1 carry, ported from the deleted
                // legacy sender arm): a follower cannot hold bytes the leader
                // never appended. The wire has no CRC, so a bit-flip that
                // escapes the UDP checksum could inflate this report; a
                // CURRENT-term report claiming more than our own append is
                // provably corrupt. DROP it whole (count it) rather than
                // clamp-to-append: clamping would still let one corrupt
                // datagram certify that the follower holds every appended
                // byte — {own, own, 0} ranks own at the quorum slot —
                // manufacturing a phantom commit on leader-only durability
                // and defeating the quorum-loss-stall theorem. Dropping
                // poisons nothing: the tracker slot is monotonic-max, so a
                // later legitimate report still advances it. The guard is
                // scoped to term == current on purpose: a HIGHER-term report
                // must always reach the SM (it triggers adopt_term — the one
                // legitimate way a follower leads our append, e.g. a restarted
                // leader re-primed below a still-ahead follower, arrives via
                // term machinery, never inside a static term). Stale terms
                // pass through too — the SM drops them itself. This guard is
                // node-side by design: the SM never sees the append counter.
                if term == self.sm.current_term()
                    && durable > self.cnc.counters().append.load_acquire()
                {
                    self.reports_implausible.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                // M6 Task 9: track the peer's newest durable for the cnc band
                // (in-memory, monotonic-max; flushed once per duty cycle).
                self.peer_reported
                    .entry(id)
                    .and_modify(|d| *d = (*d).max(durable))
                    .or_insert(durable);
                Event::Report { from: id, term, durable, durable_term }
            }
            NetEvent::CommitGossip { from, term, commit } => {
                self.learn_leader_hint(from, term);
                Event::CommitGossip { term, commit }
            }
            NetEvent::RequestVote { from, body } => {
                let Some(id) = self.addr_to_id.get(&from).copied() else { return };
                // Re-absorb the durable counter IMMEDIATELY before the grant
                // decision. `log_ok` compares the candidate against
                // `ElectionSm::durable`, and Raft's vote rule is sound only if
                // that reflects everything we have durably stored — which the
                // counter does and a copy taken earlier in this cycle may not.
                // Once per cycle is NOT enough: the archive can advance the
                // counter (and the receiver agent report the new value toward
                // the leader's commit ranking) between the top-of-cycle refresh
                // and this datagram's turn in the drain loop. See
                // `refresh_durable`.
                self.refresh_durable();
                Event::RequestVote {
                    from: id,
                    new_term: body.new_term,
                    last_term: body.last_term,
                    last_durable: body.last_durable,
                }
            }
            NetEvent::Vote { from, body } => {
                let Some(id) = self.addr_to_id.get(&from).copied() else { return };
                Event::Vote { from: id, term: body.term, granted: body.granted }
            }
            NetEvent::TermMap { from, term, entries } => {
                self.learn_leader_hint(from, term);
                let pairs: Vec<(u32, u64)> = entries.iter().map(|e| (e.term, e.base)).collect();
                // M6 Task 8: remember the leader's authoritative lineage for the
                // snapshot-install seed (below-floor join). Capture the newest.
                self.last_leader_map = pairs.clone();
                Event::TermMapReceived { term, entries: pairs }
            }
            NetEvent::ReadProbe { nonce, from, term } => {
                // Follower side: reply an ack iff still our term (handled inline,
                // never an SM event — the barrier is node-side only).
                self.on_read_probe(nonce, from, term);
                return;
            }
            NetEvent::ReadProbeAck { nonce, from } => {
                // Leader side: count a distinct acker toward a pending read's
                // quorum (node-side only, never an SM event).
                self.on_read_probe_ack(nonce, from);
                return;
            }
            NetEvent::LeaderActivity { term } => Event::LeaderSeen { term },
            NetEvent::ConfigProposal { from, body } => {
                // Leader side (M7 Task 7): a follower forwarded its admin
                // request. Handled inline — never an SM event (the propose/
                // append pipeline is node-side, like `append_config_frame`
                // itself).
                self.on_config_proposal(from, body);
                return;
            }
            NetEvent::ConfigReply { body } => {
                // Follower side (M7 Task 7): the leader's reply to OUR
                // forwarded proposal. Handled inline — matched against the
                // 1-slot pending map by nonce.
                self.on_config_reply(body);
                return;
            }
        };
        self.feed(event);
    }

    /// Learn (publish) the current leader's id from current-term leader traffic
    /// (commit gossip / term map). Only a datagram whose term MATCHES our
    /// already-adopted term identifies a confirmed leader: its source resolves
    /// to a member id via `addr_to_id`. A higher-term datagram is a *new* leader
    /// we have not adopted yet — the SM adopts it and `exec`'s `BecomeFollower`
    /// resets the hint to `u64::MAX` (unknown), and the next same-term datagram
    /// re-learns the id here.
    fn learn_leader_hint(&self, from: SocketAddr, term: u32) {
        if term != self.sm.current_term() {
            return;
        }
        if let Some(id) = self.addr_to_id.get(&from).copied() {
            self.cnc.status().leader_hint.store_release(id as u64);
        }
    }

    /// M7 Task 7: `do_work` step 11's admin-slot dispatcher. Leader: propose +
    /// append locally and answer the response line directly. Follower: forward
    /// to whoever the leader hint names (kind 16), remembering the request in
    /// `pending_admin_fwd` so the eventual `NetEvent::ConfigReply` (kind 17)
    /// can be matched back to this response line (and audited); no hint (or the hint resolves to no known address,
    /// e.g. mid-election) -> reply `status=2` (retry) immediately — side-effect-
    /// free, `uc2ctl` just polls again.
    fn handle_admin(&mut self, req: AdminReq) {
        // M12b: authenticate FIRST — leader and follower alike — so a follower
        // never forwards an unauthenticated proposal, and no unauthenticated
        // request ever reaches `propose_config`. Under the default
        // `AdminPolicy::Filesystem` this is a match arm returning `Ok(None)`:
        // byte-for-byte the pre-M12b path.
        let audited = AuditedReq::from(&req);
        let actor = match self.verify_admin(&req) {
            Ok(actor) => actor,
            Err(reason) => {
                // M12b: record the refusal BEFORE publishing it. The actor is
                // unknown by construction — this branch is only reachable
                // under `Hmac`, and it is exactly the case where the request
                // did not prove who it was.
                let version = self.cnc.config_version();
                let (status, reason) = self.audit_admin(
                    Some(ACTOR_UNVERIFIED),
                    AuditOrigin::Local,
                    audited,
                    1,
                    reason,
                    version,
                );
                self.write_admin_reply(req.seq, status, reason, version);
                return;
            }
        };
        if matches!(self.sm.role(), Role::Leader) {
            let (status, reason, version) = self.propose_and_append(req.op, req.id, req.ip, req.port);
            // Record before responding. On the accepted path the change is
            // already appended by now (see `crate::audit`'s module doc: the
            // record precedes the commit, and "accepted" means proposed and
            // appended) — what must not happen is an ANSWER that is not on
            // disk here, and that is what this ordering buys.
            let (status, reason) = self.audit_admin(
                actor.as_deref(),
                AuditOrigin::Local,
                audited,
                status,
                reason,
                version,
            );
            self.write_admin_reply(req.seq, status, reason, version);
            return;
        }
        let hint = self.cnc.status().leader_hint.load_acquire();
        let leader_addr = (hint != u64::MAX)
            .then(|| self.id_to_addr.get(&(hint as NodeId)).copied())
            .flatten();
        let Some(leader_addr) = leader_addr else {
            let version = self.cnc.config_version();
            let (status, reason) =
                self.audit_admin(actor.as_deref(), AuditOrigin::Local, audited, 2, 0, version);
            self.write_admin_reply(req.seq, status, reason, version);
            return;
        };
        // T7 review finding 2: a still-outstanding forward would otherwise be
        // silently overwritten, leaving its caller with a bare timeout and no
        // answer at all. Answer the superseded request with status=2 (retry)
        // before replacing the pending slot.
        if let Some(old) = self.pending_admin_fwd.take() {
            eprintln!("uc2_node: admin forward superseded by newer request");
            let version = self.cnc.config_version();
            let (status, reason) = self.audit_admin(
                old.actor.as_deref(),
                AuditOrigin::Local,
                AuditedReq::from(&old),
                2,
                0,
                version,
            );
            self.write_admin_reply(old.seq, status, reason, version);
        }
        self.pending_admin_fwd = Some(PendingAdminFwd {
            seq: req.seq,
            nonce: req.nonce,
            actor,
            op: req.op,
            id: req.id,
            ip: req.ip,
            port: req.port,
        });
        let body = ConfigProposalBody { nonce: req.nonce, op: req.op, id: req.id, ip: req.ip, port: req.port };
        let mut buf = [0u8; CONFIG_PROPOSAL_BODY_LEN];
        write_config_proposal_body(&mut buf, &body);
        let term = self.sm.current_term();
        self.send(leader_addr, DGRAM_KIND_CONFIG_PROPOSAL, 0, term, &buf);
    }

    /// M12b: authenticate one admin request. Returns the verified actor (the
    /// admin key name that signed it; `None` when the policy does not
    /// authenticate) or the wire `reason` code to refuse it with.
    ///
    /// Under [`AdminPolicy::Filesystem`] — the default, and the pre-M12b
    /// posture — this reads NOTHING: the instance directory's file
    /// permissions are the admin boundary, and a request carrying a stale or
    /// garbage auth line behaves exactly as it did before M12b.
    ///
    /// **Why there is no `(seq, nonce)` replay ring** (a ruled deviation from
    /// spec §5.2): the tag covers `seq`, and the consensus agent only ever
    /// acts on `seq > last_admin_seq` (`read_admin_req(self.last_admin_seq)`),
    /// so a captured request cannot be re-presented at its original `seq` —
    /// it is never even read — and re-presenting it at a higher `seq`
    /// invalidates the tag. Across a node restart `last_admin_seq` resets to
    /// 0, but `instance_id` changes, and the tag covers that too. A ring
    /// would therefore never refuse anything these checks already refuse.
    /// `expiry_ns` still bounds the window in which a live, correctly
    /// sequenced request could be delayed and only then applied.
    ///
    /// **The binding values come from this node's own boot-time state**
    /// ([`Self::admin_instance_id`] / [`Self::admin_app_id`]), never from
    /// `CncPage::meta()`. That is what makes the restart half of the argument
    /// above sound: the cnc page is a writable file, so an actor with
    /// instance-dir write access could otherwise put a captured
    /// `instance_id` back on the page after a restart and replay the capture
    /// against it (M12b final review, C1).
    fn verify_admin(&self, req: &AdminReq) -> Result<Option<String>, u32> {
        let (keys, ttl) = match &self.admin {
            AdminPolicy::Filesystem => return Ok(None),
            AdminPolicy::Hmac { keys, ttl } => (keys, ttl),
        };
        // Safe to read only here: `read_admin_req` returned `Some` for this
        // request, and its `seq` acquire-load is what publishes these bytes
        // (the auth line rides the admin-req seqlock — `CncPage`'s contract).
        let auth: AdminAuth = self.cnc.read_admin_auth();
        if auth.is_zero() {
            return Err(REASON_AUTH_MISSING);
        }
        // Expiry before key lookup and before the tag: a signed request whose
        // window is in the past — or stretched implausibly far into the
        // future, which is the same attack with the clock turned the other
        // way — is refused on the window alone.
        let now = crate::obs::metrics::now_unix_ns();
        let ttl_ns = u64::try_from(ttl.as_nanos()).unwrap_or(u64::MAX);
        let max_expiry = now.saturating_add(ttl_ns.saturating_mul(2));
        if auth.expiry_ns <= now || auth.expiry_ns > max_expiry {
            return Err(REASON_AUTH_EXPIRED);
        }
        let key = keys
            .iter()
            .find(|k| k.name_hash == auth.key_name_hash)
            .ok_or(REASON_AUTH_UNKNOWN_KEY)?;
        let m = AdminMessage {
            app_id: &self.admin_app_id,
            instance_id: self.admin_instance_id,
            seq: req.seq,
            nonce: req.nonce,
            op: req.op,
            id: req.id,
            ip: req.ip,
            port: req.port,
            expiry_ns: auth.expiry_ns,
        };
        if !uc2_crypto::admin::verify(key, &m, &auth.tag) {
            return Err(REASON_AUTH_BAD_TAG);
        }
        Ok(Some(key.name.clone()))
    }

    /// M7 Task 7: leader-only — decode the wire op fields, `propose_config`,
    /// and on `Ok` append + adopt-at-append (`append_config_frame`). Shared by
    /// the local admin-slot path and the network `ConfigProposal` forward path
    /// (one propose/append pipeline either way). Returns the wire reply triple
    /// `(status, reason, version)`:
    /// * `0, 0, new_version` — accepted.
    /// * `1, reason_code, current_version` — refused (`ProposeError`, or
    ///   `REASON_MALFORMED_OP` for a malformed/unknown op field — `uc2ctl`
    ///   never emits one, so this is a defensive catch-all, not a real path).
    /// * `2, 0, current_version` — retry: the ring was momentarily full
    ///   (`AppendError::WouldOverrun`). Safe to retry WHOLE: `propose_config`
    ///   itself never mutates SM state (it only reads `role`/`serving`/
    ///   `config_pending`/`commit_seen`/`last_reports` and returns a fresh
    ///   `ClusterConfig` clone) and adoption happens ONLY via the `ConfigObserved`
    ///   `append_config_frame` feeds back on a SUCCESSFUL append — so a failed
    ///   append leaves the SM bit-for-bit as it was before this call; nothing
    ///   here can leave a half-adopted config behind for the retry to trip over.
    fn propose_and_append(&mut self, op: u32, id: u32, ip: u32, port: u16) -> (u32, u32, u64) {
        let Some(config_op) = wire_to_config_op(op, id, ip, port) else {
            return (1, REASON_MALFORMED_OP, self.cnc.config_version());
        };
        match self.sm.propose_config(config_op, self.admission_bytes) {
            Ok(new_cfg) => {
                let version = new_cfg.version;
                match self.append_config_frame(&new_cfg) {
                    Ok(_position) => (0, 0, version),
                    Err(AppendError::WouldOverrun) | Err(AppendError::PayloadTooLarge) => {
                        (2, 0, self.cnc.config_version())
                    }
                }
            }
            Err(e) => (1, ClusterConfig::reason_code(&e), self.cnc.config_version()),
        }
    }

    /// M7 Task 7: leader-side handling of a follower-forwarded proposal (kind
    /// 16). A stale/not-yet-leader recipient just drops it — the forwarding
    /// follower's request times out and `uc2ctl` (or the follower's next admin
    /// cycle) re-learns the current leader hint and can re-forward. Nonce
    /// dedup: a repeat nonce gets the STORED reply re-sent rather than a fresh
    /// `propose_config` call (retry-idempotent while the change is pending) —
    /// and, since M12b's final review (I4), without a second audit record:
    /// the original answer was recorded, and a byte-identical re-answer of
    /// the same nonce is not a new admin event (it is counted as
    /// `config_proposal_dedup_resend` instead).
    fn on_config_proposal(&mut self, from: SocketAddr, body: ConfigProposalBody) {
        if !matches!(self.sm.role(), Role::Leader) {
            return;
        }
        // M12b review: a source address that resolves to no current member
        // is not a peer this leader can vouch for — `peer_actor` would have
        // to fall back to `peer:<addr>`, and everything past that point
        // (propose_config, an fsync'd audit record) runs on the consensus
        // thread. Drop it here, before either runs, rather than let an
        // unauthenticated/spoofed datagram drive real work off this thread.
        if !self.addr_to_id.contains_key(&from) {
            self.config_proposal_non_member += 1;
            let from_s = from.to_string();
            crate::obs_event!(
                Warn,
                "config_proposal_non_member",
                node = self.id as u64,
                from = from_s.as_str(),
            );
            return;
        }
        // M12b: the forwarding peer authenticated the operator locally before
        // it forwarded; what the leader can attest to is WHICH PEER asked, so
        // that is the actor it records. `seq` is 0 in these records — the
        // requesting node's admin-band sequence is local to it and the wire
        // proposal does not carry it; `nonce` is the join key between the two
        // nodes' records.
        let actor = self.peer_actor(from);
        let audited = AuditedReq {
            op: body.op,
            id: body.id,
            ip: body.ip,
            port: body.port,
            seq: 0,
            nonce: body.nonce,
        };
        if let Some((nonce, reply)) = &self.last_config_reply
            && *nonce == body.nonce
        {
            let reply = *reply;
            // M12b final review (I4): re-send the cached reply WITHOUT a
            // second audit record. The original answer was recorded; a
            // byte-identical re-answer of the same nonce is not a new admin
            // event, and recording one would let any member (or, with
            // `[crypto].enabled = false`, anything that can spoof a member's
            // source address) drive an unbounded stream of `fsync`s on the
            // consensus thread by re-sending one captured datagram. A
            // fresh-nonce proposal is still recorded — see below.
            self.config_proposal_dedup_resend += 1;
            self.send_config_reply(from, &reply);
            return;
        }
        let (status, reason, version) = self.propose_and_append(body.op, body.id, body.ip, body.port);
        // The dedup cache keeps the REAL answer, never an audit-failure one:
        // a later retry of the same nonce must be able to re-learn what
        // actually happened once the disk recovers.
        let reply = ConfigReplyBody { nonce: body.nonce, status, reason, version };
        self.last_config_reply = Some((body.nonce, reply));
        let (status, reason) = self.audit_admin(
            Some(&actor),
            AuditOrigin::Forwarded,
            audited,
            status,
            reason,
            version,
        );
        let reply = ConfigReplyBody { nonce: body.nonce, status, reason, version };
        self.send_config_reply(from, &reply);
    }

    fn send_config_reply(&mut self, to: SocketAddr, reply: &ConfigReplyBody) {
        let mut buf = [0u8; CONFIG_REPLY_BODY_LEN];
        write_config_reply_body(&mut buf, reply);
        let term = self.sm.current_term();
        self.send(to, DGRAM_KIND_CONFIG_REPLY, 0, term, &buf);
    }

    /// M7 Task 7: follower-side handling of the leader's reply (kind 17) to our
    /// forwarded proposal. Matched against the 1-slot pending map by nonce; a
    /// reply for any other nonce (stale, or a race with a since-superseded
    /// forward) is dropped rather than misattributed to the wrong response line.
    fn on_config_reply(&mut self, body: ConfigReplyBody) {
        let Some(pending) = &self.pending_admin_fwd else { return };
        if pending.nonce != body.nonce {
            return;
        }
        // M12b: the actor rode across the forward, so this node's own record
        // of the final answer names the operator who asked for it — `origin`
        // is `local` (the request came in on THIS node's admin band); the
        // leader wrote its own `forwarded` record under the same nonce.
        let pending = self.pending_admin_fwd.take().expect("checked just above");
        let (status, reason) = self.audit_admin(
            pending.actor.as_deref(),
            AuditOrigin::Local,
            AuditedReq::from(&pending),
            body.status,
            body.reason,
            body.version,
        );
        self.write_admin_reply(pending.seq, status, reason, body.version);
    }

    /// M12b: the actor a leader records for a peer-forwarded proposal. The
    /// leader cannot re-check the operator's signature (the canonical message
    /// is bound to the requesting node's own cnc band), so what it attests to
    /// is the peer that vouched for it: `peer:<id>` when the source address
    /// resolves to a member.
    ///
    /// M12b review: `on_config_proposal`'s membership guard now returns
    /// before calling this for any `from` outside `addr_to_id`, making the
    /// `peer:<addr>` fallback unreachable from that (its only) call site.
    /// Kept as a defensive branch rather than removed — `peer_actor` is a
    /// crisp, independently-correct "attest to what we can" function, and an
    /// `unwrap`/`expect` here would turn a future second caller's oversight
    /// into a panic on the consensus thread instead of a merely-untested path.
    fn peer_actor(&self, from: SocketAddr) -> String {
        match self.addr_to_id.get(&from) {
            Some(id) => format!("peer:{id}"),
            None => format!("peer:{from}"),
        }
    }

    /// M12b (spec §5.3): record one admin decision, then hand back the
    /// `(status, reason)` the caller must actually publish.
    ///
    /// **Record before respond.** Every site that answers an admin request —
    /// the cnc response line and the kind-17 config reply alike — goes
    /// through here first. If the record cannot be written, the answer
    /// becomes `(1, REASON_AUDIT_FAILED)`: a node that cannot say what it did
    /// refuses rather than acting unaccountably. On the leader's accepted
    /// path the config change has already been appended when that happens, so
    /// reason 24 means "the outcome is unrecorded, go look at `uc2ctl
    /// status`", not "nothing happened" — which is why it is a loud `error`
    /// event as well as a wire code.
    ///
    /// Cost: one `write` + one `sync_data` per admin request, on the
    /// consensus thread. Admin requests are operator-rate (`read_admin_req`
    /// only returns `Some` when a client actually wrote one), so the duty
    /// cycle's steady state never touches this.
    fn audit_admin(
        &mut self,
        actor: Option<&str>,
        origin: AuditOrigin,
        req: AuditedReq,
        status: u32,
        reason: u32,
        version: u64,
    ) -> (u32, u32) {
        let rec = AuditRecord {
            ts_ns: crate::obs::metrics::now_unix_ns(),
            // `None` is exactly `AdminPolicy::Filesystem`: nothing was
            // authenticated, the instance directory's permissions were the
            // whole boundary, and the record says so.
            actor: actor.unwrap_or("filesystem"),
            origin,
            op: req.op,
            op_name: op_name(req.op),
            id: req.id,
            // Only `AddLearner` carries an address; the id-only ops leave the
            // pair zeroed, which records as `null` rather than `0.0.0.0:0`.
            addr: (req.ip != 0 || req.port != 0).then_some((req.ip, req.port)),
            seq: req.seq,
            nonce: req.nonce,
            outcome: AuditOutcome::from_wire(status, reason),
            reason,
            config_version: version,
        };
        match self.audit.record(&rec) {
            Ok(()) => (status, reason),
            Err(e) => {
                let err = e.to_string();
                crate::obs_event!(
                    Error,
                    "admin_audit_failed",
                    node = self.id as u64,
                    seq = req.seq,
                    nonce = req.nonce,
                    op = req.op as u64,
                    status = status as u64,
                    err = err.as_str(),
                );
                (1, REASON_AUDIT_FAILED)
            }
        }
    }

    /// Write the admin response line (fields-then-seq/release; the T1 accessor
    /// enforces the discipline) for `seq`.
    fn write_admin_reply(&mut self, seq: u64, status: u32, reason: u32, version: u64) {
        self.cnc.write_admin_resp(&AdminResp { seq, status, reason, version });
    }

    /// T7 review finding 1: invalidate the admin nonce-dedup cache and any
    /// still-outstanding forwarded admin request across a role/truncation
    /// transition. Both are scoped to a single leader "life" — surviving one
    /// would let a duplicate kind-16 datagram (same nonce) replay a reply that
    /// belongs to a since-reverted or since-superseded world (e.g. an
    /// uncommitted config that a later truncate reverted). A pending forward
    /// gets an explicit status=2 (retry) admin reply written now, rather than
    /// leaving the caller to hang for the full timeout with no answer at all.
    fn invalidate_admin_caches(&mut self) {
        self.last_config_reply = None;
        if let Some(pending) = self.pending_admin_fwd.take() {
            // M12b: this is an ANSWER (a retry), so it is recorded like any
            // other — the operator's request ended here, in this node's life.
            let version = self.cnc.config_version();
            let (status, reason) = self.audit_admin(
                pending.actor.as_deref(),
                AuditOrigin::Local,
                AuditedReq::from(&pending),
                2,
                0,
                version,
            );
            self.write_admin_reply(pending.seq, status, reason, version);
        }
    }

    /// Feed one event into the SM and execute every resulting action IN ORDER;
    /// SM-local follow-ups (`NewTermAppended`) are processed after the batch
    /// (mirroring the uc2_sim driver's work queue). Also drives the intake-gate
    /// reconciliation discipline: a term-map that reconciles clean reopens the
    /// gate closed on term adoption.
    fn feed(&mut self, ev: Event) {
        let mut work = vec![ev];
        while let Some(e) = work.pop() {
            let tm_term = match &e {
                Event::TermMapReceived { term, .. } => Some(*term),
                _ => None,
            };
            let term_before = self.sm.current_term();
            let mut out = Vec::new();
            self.sm.step(e, &mut out);
            let produced_truncate = out.iter().any(|a| matches!(a, Action::Truncate { .. }));
            for act in out {
                self.exec(act, &mut work);
            }
            // Gate reopen (T7 discipline): a term-map that was actually processed
            // (term >= ours) and needed NO truncation completes reconciliation
            // for the adopted term.
            //
            // C-1 guard: also require that NO truncation is in flight
            // (`pending_truncation.is_none()`). A leader re-ships its map at kHz, so
            // a duplicate TermMap routinely lands AFTER we emitted `Action::Truncate`
            // but BEFORE the archive's slot ack. On that duplicate the SM's
            // truncating latch drops the event with ZERO actions, so
            // `produced_truncate` is false and the term hasn't moved — the old
            // heuristic would reopen the gate MID-TRUNCATION, letting the receiver
            // ship an AppendPosition stamped with the current term over the raw
            // divergent durable (counters not yet re-primed) → monotonic-max
            // poisons the leader's CommitTracker → phantom commit. The node's own
            // emit→ack marker (`pending_truncation`: set in `Action::Truncate`,
            // cleared in `on_truncated`) brackets that window exactly.
            if let Some(t) = tm_term
                && self.awaiting_reconcile
                && !produced_truncate
                && self.pending_truncation.is_none()
                && t >= term_before
                // Finding #9 (lean LC2, gate doc): reopen ONLY when the SM's
                // active term equals the data-plane term handle the receiver
                // filters DATA at (`adopted_term` == `term_handle`,
                // receiver.rs:635 `dropped_stale_term`). `Action::StartElection`
                // bumps `current_term` but stores NO handle (node.rs:2440-2450),
                // so a CANDIDATE runs its data plane at a LAGGING handle. Without
                // this check, a candidate that cleanly reconciles a HIGHER-term
                // leader's map (non-adopt: `t` not `> current_term`) reopens
                // intake for its stale handle-term stream and then accepts a
                // cross-stream `serveTail` byte its map never attributed —
                // Finding #9's candidate cross-stream accept (acked-write-loss,
                // §5.4.2 / #6b family). When the map ADOPTS a strictly higher
                // term, `BecomeFollower` re-keyed the handle to `t` first, so
                // `current_term == adopted_term` holds and the reopen fires as
                // before.
                && self.sm.current_term() == self.adopted_term
            {
                // Clean reconcile for the adopted term: reopen and clear the
                // awaiting-reconcile latch (M-3).
                self.awaiting_reconcile = false;
                self.open_gate();
            }
        }
    }

    /// `_work` is the re-entrant event queue an arm can push follow-up events
    /// onto, drained by [`Self::feed`]'s loop. It currently has no producer:
    /// issue #6 moved the only one (`BecomeLeader`'s `NewTermAppended`) into
    /// `on_collapsed`, which runs from the duty cycle rather than from inside a
    /// `feed` and so feeds directly. Kept because it is the arms' only way to
    /// chain an event without re-entering the SM mid-batch.
    fn exec(&mut self, act: Action, _work: &mut Vec<Event>) {
        match act {
            Action::PersistAndSendVote { to, vote } => {
                // Persist-before-answer: the store is durable on return, THEN the
                // datagram (self-votes skip the send — `to == self`).
                self.state
                    .store_vote(VoteRecord { term: vote.term, voted_for: vote.voted_for })
                    .expect("vote persist fail-stop");
                if to != self.id
                    && let Some(&addr) = self.id_to_addr.get(&to)
                {
                    let mut body = [0u8; VOTE_BODY_LEN];
                    write_vote_body(&mut body, &VoteBody { term: vote.term, granted: true });
                    self.send(addr, DGRAM_KIND_VOTE, 0, vote.term, &body);
                }
            }
            Action::SendVoteRejection { to, term } => {
                if let Some(&addr) = self.id_to_addr.get(&to) {
                    let mut body = [0u8; VOTE_BODY_LEN];
                    write_vote_body(&mut body, &VoteBody { term, granted: false });
                    self.send(addr, DGRAM_KIND_VOTE, 0, term, &body);
                }
            }
            Action::StartElection { new_term, last_term, last_durable } => {
                let mut body = [0u8; REQUEST_VOTE_BODY_LEN];
                write_request_vote_body(
                    &mut body,
                    &RequestVoteBody { new_term, last_term, last_durable },
                );
                for id in self.peers.clone() {
                    let addr = self.id_to_addr[&id];
                    self.send(addr, DGRAM_KIND_REQUEST_VOTE, 0, new_term, &body);
                }
            }
            Action::BecomeLeader { term, base } => {
                // T7 review finding 1: a stale nonce-dedup / forward cache must
                // not survive into this new leader life.
                self.invalidate_admin_caches();
                // Contract order (T3/T7, load-bearing): (a) term-map append +
                // persist durable; (b) collapse volatile to base — old bytes
                // above base must never be streamable; (c) fresh appender AFTER
                // the collapse; (d) append the NewTerm frame + feed it back; (e)
                // role flags.
                //
                // Issue #6: (b) is no longer a `prime(base)` on THIS thread. It
                // is an `ArchiveCmd::Collapse` executed by the archive agent,
                // which cuts its journal to `base`, resets its private
                // `durable_pos`, and primes there — all serialized against its
                // own `do_work`. Steps (c)-(e) therefore move to `on_collapsed`,
                // one duty cycle later. See `ArchiveCmd::Collapse`'s doc for the
                // corruption this ordering prevents.
                let map = to_entries(self.sm.term_map());
                self.state.store_term_map(&map).expect("term-map persist fail-stop");
                self.term_handle.store(term, Ordering::Release);
                // Explicit single-writer handoff (review hardening): the gate
                // is closed across the collapse so a UDP-reordered straggler that
                // cleared the old term filter cannot race the counter reset. It
                // reopens in `on_collapsed`, once the appender exists.
                self.close_gate();
                let epoch = self.next_collapse_epoch;
                self.next_collapse_epoch += 1;
                self.pending_leader_open = Some(PendingLeaderOpen { epoch, term, base });
                self.trunc_tx
                    .send(ArchiveCmd::Collapse { epoch, to: base })
                    .expect("archive channel closed");
                self.adopted_term = term;
                // A leader is the source of truth; no reconcile pending (M-3).
                self.awaiting_reconcile = false;
                // NB: `leader_hint` is published in phase 2, not here — pointing
                // clients at a node that cannot serve yet just bounces them back
                // with `MSG_V2_NOT_LEADER` naming ourselves.
                // M8 (Task 12), rotation trigger 1 (spec §5): a new leader
                // ALWAYS mints a fresh epoch. This one rule absorbs leader
                // self-removal (the outgoing leader steps down at the same
                // commit crossing, so it cannot be the rotator), crash
                // handoff, and any rotation a dead leader missed. It is also
                // what makes this node able to seal group traffic AT ALL:
                // `GroupPlane::sealing_epoch` answers only for an epoch this
                // node itself minted, so a leader that never minted returns
                // `NoGroupKey` for every `DATA`. The latch is consumed by the
                // next maintenance pass, which does the actual mint.
                if let Some(crypto) = self.crypto.as_ref() {
                    crypto.on_became_leader();
                }
                crate::obs_event!(
                    Info,
                    "became_leader",
                    node = self.id as u64,
                    term = term as u64,
                    base = base
                );
            }
            Action::BecomeFollower { term, leader } => {
                // T7 review finding 1: stepping down from leader (or adopting a
                // new term as follower) must not leave a stale nonce-dedup /
                // forward cache answerable by a later duplicate datagram.
                self.invalidate_admin_caches();
                self.term_handle.store(term, Ordering::Release);
                self.leader_flag.store(false, Ordering::Release);
                self.appender = None;
                // Issue #6: abandon any leader open still awaiting its collapse
                // ack. The cut itself is already commanded and remains correct
                // (it drops only this node's own unreplicated tail), but the
                // ack must not install an appender for a term we no longer lead.
                self.pending_leader_open = None;
                // Close the intake gate on adopting a strictly NEW term; it
                // reopens only after reconciliation for this term completes.
                if term > self.adopted_term {
                    self.close_gate();
                    self.awaiting_reconcile = true;
                    // A newly adopted term has no known leader yet — reset the
                    // hint to `unknown` until current-term leader traffic
                    // re-learns it (`learn_leader_hint`).
                    self.cnc.status().leader_hint.store_release(u64::MAX);
                }
                self.adopted_term = term;
                match leader {
                    Some(leader) => crate::obs_event!(
                        Info,
                        "became_follower",
                        node = self.id as u64,
                        term = term as u64,
                        leader = leader as u64
                    ),
                    None => crate::obs_event!(
                        Info,
                        "became_follower",
                        node = self.id as u64,
                        term = term as u64
                    ),
                }
            }
            Action::AdvanceCommit { commit } => {
                // Diagnostic only, and OFF the hot path unless UC2_TRUNC_TRACE
                // asked for it — commit advances every duty cycle under load.
                if self.trunc_trace
                    && let Ok(mut p) = self.trace_prov.lock()
                {
                    *p = self.sm.commit_provenance();
                }
                // The ONLY commit store in the binary (both roles). M4 carry #5
                // deleted uc2_net's two legacy sites (the sender's self-ranking
                // tracker and the receiver's local COMMIT_POSITION store) —
                // grep-provable: `grep -rn "commit.store_release" uc2_net/ |
                // wc -l` == 0.
                self.cnc.counters().commit.store_release(commit);
            }
            Action::GossipCommit { commit } => {
                let term = self.sm.current_term();
                // Voters AND learners: a learner advances its commit off this
                // gossip exactly like a follower (it just never gossips back a
                // Report that counts).
                // M8 (T17): `Scope::Group` — one seal, N sends.
                let targets = self.gossip_targets();
                self.fan_out_group(&targets, DGRAM_KIND_COMMIT_POSITION, commit, term, &[]);
            }
            Action::ShipTermMap { entries } => {
                let term = self.sm.current_term();
                let body = encode_term_map(&entries);
                // Voters AND learners: a learner reconciles against the shipped map
                // (the NoCommonPrefix → wipe path in Task 8 rides this too).
                for id in self.gossip_targets() {
                    let addr = self.id_to_addr[&id];
                    self.send(addr, DGRAM_KIND_TERM_MAP, 0, term, &body);
                }
            }
            Action::Truncate { epoch, to, new_map } => {
                // T7 review finding 1: a truncate can revert an uncommitted
                // config (see the persist-revert-before-truncate block below)
                // without necessarily passing through a BecomeLeader/Follower
                // transition first — invalidate the admin caches here too so
                // no stale reply outlives the config it was answering for.
                self.invalidate_admin_caches();
                // Persist-before-truncate ordering (M5): store the pruned map
                // DURABLY *before* commanding the physical truncate, so a crash in
                // the window recovers a map that is a valid prefix of the
                // truncated log — never a map that claims terms above the
                // still-present bytes. With `rederive_term_map` this is
                // self-healing: a persisted-but-not-truncated journal is corrected
                // by the reconcile that reissues the truncate on the next boot.
                self.state
                    .store_term_map(&to_entries(&new_map))
                    .expect("term-map persist fail-stop");
                // M7 Task 6: persist-revert-BEFORE-truncate — the SAME discipline as
                // the term-map persist immediately above. If this truncation drops
                // the currently-adopted config FRAME (`to` lands strictly below its
                // recorded position), the durable `ConfigRecord` must not survive
                // claiming a position the truncated log no longer backs. Revert it
                // NOW, synchronously, before the archive's physical truncate runs, so
                // a crash in the window between this persist and the truncate
                // recovers a record that is a valid predecessor of whatever the
                // truncated log ends up holding — never one ahead of it. The SM's OWN
                // `Truncated`-arm revert (T4) then re-emits `Action::ConfigAdopted` on
                // the matching-epoch ack, whose persist is an idempotent overwrite of
                // this exact same record — one adoption/persist path either way.
                if let Some(rec) = self.state.config_record()
                    && to < rec.position
                {
                    let reverted = if to == 0 {
                        // Wipe-and-rejoin (mirrors the SM's own wipe branch):
                        // config-by-fiat — keep the CURRENT operational config
                        // rather than dropping to a predecessor a wiped node has no
                        // further use for (same authority argument as
                        // `adopt_snapshot_lineage`/`adopt_snapshot_config`).
                        ConfigRecord {
                            position: 0,
                            config: rec.config.clone(),
                            prev_position: 0,
                            prev: rec.config,
                        }
                    } else {
                        // Ordinary truncation: revert one level — prev promoted to
                        // cur, prev duplicated (the one-level history is now
                        // exhausted: nothing below the reverted config to recover).
                        ConfigRecord {
                            position: rec.prev_position,
                            config: rec.prev.clone(),
                            prev_position: rec.prev_position,
                            prev: rec.prev,
                        }
                    };
                    self.state.store_config_record(&reverted).expect("config persist fail-stop");
                }
                // Pause intake and record the emit→ack bracket (the SM allocated
                // `epoch`; we transport it). The SM has already latched the data
                // plane. Emitting the truncate IS the reconcile decision for the
                // currently-adopted term, so clear `awaiting_reconcile` — a
                // higher term adopted while the truncate is in flight re-arms it
                // (BecomeFollower), keeping the gate closed until THAT term
                // reconciles.
                self.close_gate();
                self.pending_truncation = Some(epoch);
                self.awaiting_reconcile = false;
                self.trunc_tx
                    .send(ArchiveCmd::Truncate { epoch, to })
                    .expect("archive channel closed");
                crate::obs_event!(
                    Warn,
                    "log_truncated",
                    node = self.id as u64,
                    epoch = epoch,
                    to = to
                );
            }
            Action::PersistTermMap { new_map } => {
                self.state
                    .store_term_map(&to_entries(&new_map))
                    .expect("term-map persist fail-stop");
            }
            Action::Fatal { reason } => {
                panic!("consensus fatal (fail-stop): {reason}");
            }
            Action::CountWipe => {
                // M6 Task 8: a wipe-and-rejoin was decided; the substantive
                // `Truncate { to: 0 }` follows in the same action batch. Count it
                // (distinct from an ordinary truncate) for observability + tests.
                self.wipes.fetch_add(1, Ordering::Relaxed);
                crate::obs_event!(Warn, "log_wiped", node = self.id as u64);
            }
            Action::ConfigAdopted { position, config, prev_position, prev } => {
                // M7: the SM adopted a higher-version `ClusterConfig` — via the
                // leader's own append (`append_config_frame`), a follower's
                // archive-scan observation, boot recovery, OR the SM's own
                // truncation-revert re-adoption. One exec arm for all of them.
                //
                // Persist BEFORE any behavioral change (crash between persist
                // and rebuild = recovery re-adopts from the record via
                // `ElectionSm::new` + `restore_prev_config`: safe).
                let rec = ConfigRecord {
                    position,
                    config: cluster_to_stored(&config),
                    prev_position,
                    prev: cluster_to_stored(&prev),
                };
                self.state.store_config_record(&rec).expect("config persist fail-stop");
                // Rebuild the net layer + this node's own routing/observability
                // (and, since the final-review fix, the snapshot-session
                // config-carry cache too — so every SNAP_BEGIN a session opens
                // from here on ships THIS config; over-delivery to a peer that
                // doesn't need it is safe, adopt-by-version idempotence on the
                // receiving end). Shared with the snapshot-fiat install path in
                // `maybe_adopt_incoming_snapshot` (M7 Task 9) — one derivation for
                // "what changes when membership changes" everywhere it changes.
                self.rebuild_net_for_config(&config, prev_position);
                self.cnc.store_config_version(config.version);
                // Cleared once commit crosses `position` (do_work step 11).
                self.cnc.store_config_pending(true);
                // The crash-handoff wedge `propose_config`'s `SelfDemote` guard
                // can't close. That guard only blocks a SERVING leader from
                // PROPOSING its own demote; it can't stop a DIFFERENT leader from
                // proposing `DemoteVoter{B}` (legal — B isn't self), replicating
                // the CONFIG frame to B, and crashing before it commits. If B then
                // wins the election, B's own archive scan re-observes that frame
                // and adopts it here from the log — while B is `Role::Leader`. A
                // demote leaves no tombstone (only `Remove*` does), so the
                // `tombstones.contains(self.id)` latch never fires and
                // `StepDownRemoved` does not follow.
                //
                // Post-M7 loose-end T1 closed this wedge in the SM: `rank_leader`
                // now relinquishes leadership (a same-term `BecomeFollower`, the
                // generic arm above) once the demote COMMITS — B keeps appending
                // through the adoption window (C_new must be replicated by a
                // leader that still exists), then steps down to a live non-voting
                // learner-follower and the surviving C_new voters elect among
                // themselves. So this is now a TRANSIENT state, not a permanent
                // wedge. Still log it (once per adoption): a leader adopting its
                // own demote from the log is an operationally notable
                // crash-handoff event even though it now self-heals.
                if matches!(self.sm.role(), Role::Leader)
                    && !config.is_voter(self.id)
                    && !config.tombstones.contains(&self.id)
                {
                    eprintln!(
                        "node {}: leader adopted a config demoting itself from the log \
                         (crash-handoff of a demote a prior leader proposed against this id) \
                         — will relinquish leadership to a non-voting learner-follower once \
                         the demote commits (T1 self-heal); no operator action required",
                        self.id
                    );
                }
                crate::obs_event!(
                    Info,
                    "config_adopted",
                    node = self.id as u64,
                    position = position,
                    version = config.version,
                    prev_position = prev_position
                );
            }
            Action::HaltRemoved => {
                // M7: this node is not a member of the just-adopted config (and
                // is not a leader mid-self-removal — that case keeps serving
                // until its own removal commits). Fail-stop: park permanently.
                crate::obs_event!(
                    Error,
                    "halt_removed",
                    node = self.id as u64,
                    term = self.sm.current_term() as u64,
                    msg = "removed from cluster config — halting"
                );
                self.halt();
            }
            Action::StepDownRemoved => {
                // M7 Task 8: a LEADER's own removal has just COMMITTED (the SM
                // kept it serving through the adoption window; commit crossing
                // `config_position` means C_new itself now certifies the
                // removal). Nothing left to do but fail-stop exactly like
                // `HaltRemoved`. The remaining C_new voters elect among
                // themselves; this leader never depended on beyond the commit
                // that just landed.
                crate::obs_event!(
                    Warn,
                    "stepdown_removed",
                    node = self.id as u64,
                    term = self.sm.current_term() as u64,
                    msg = "removed from cluster (self-removal committed) — halting"
                );
                self.halt();
            }
        }
    }

    /// Shared fail-stop park for `HaltRemoved`/`StepDownRemoved` (M7 Task 8):
    /// set the permanent `halt_removed` latch (`do_work`'s entry check short-
    /// circuits every SUBSEQUENT cycle), AND clear the in-process
    /// `leader_flag`/`can_serve_flag` immediately rather than leaving them at
    /// whatever they last read. `publish_status` (still to run this SAME
    /// cycle, below `exec` in `do_work`) already masks the CNC-PAGE mirror
    /// off `halt_removed` regardless — but `Node::is_leader()`/`can_serve()`
    /// read these two atomics DIRECTLY, bypassing that mask entirely. Without
    /// this, `StepDownRemoved`'s leader case (the SM's `serving` field is
    /// never cleared by step-down — the read path's own checks conjoin
    /// `!halt_removed` for exactly that reason) would leave an embedded
    /// caller's `is_leader()`/`can_serve()` reporting stale `true` forever
    /// after a real halt. A
    /// removed FOLLOWER's flags were already `false` here (only a LEADER
    /// reaches `StepDownRemoved`), so this is a no-op there — the fix is
    /// entirely about the self-removal leader case Task 8 introduces.
    fn halt(&mut self) {
        self.halt_removed = true;
        self.leader_flag.store(false, Ordering::Release);
        self.can_serve_flag.store(false, Ordering::Release);
        // Veil §5 discharge, observation 1 (the parked-reads liveness
        // blemish): `do_work` short-circuits every SUBSEQUENT cycle, so a
        // read still parked here would never reach its deadline RETRY — the
        // client's own timeout would be its only recovery. Answer everything
        // in flight with the standard side-effect-free RETRY now, and drop
        // the probe round: the consensus agent is parked, so no ack could
        // ever complete it. Reads admitted LATER in this same halting cycle
        // (the raw `sm.can_serve()` stays true — step-down never clears the
        // SM's serving field) are refused/swept by the `halt_removed`
        // conjunctions in `drain_query_ring` and `advance_pending_reads`.
        self.current_round = None;
        for r in std::mem::take(&mut self.pending_reads) {
            self.send_retry(r.client_id, r.local_seq);
        }
    }

    /// Archive-truncation feedback (slot ack). The map was already persisted
    /// durably in `Action::Truncate` (persist-before-truncate), so nothing is
    /// stored here. The `Event::Truncated{epoch, to}` is fed UNCONDITIONALLY —
    /// physical truncation is the truth about durability and the SM's durable
    /// clamp must run even for a stale-epoch ack. Only the MATCHING epoch clears
    /// the emit→ack bracket, counts the truncation, and reopens the gate — and the
    /// reopen fires only if no newer term is itself awaiting reconcile (a term
    /// adopted mid-truncation re-armed `awaiting_reconcile`, and its fresh
    /// reconcile in the new term must complete first).
    /// Absorb the shared `durable` counter into `ElectionSm`. Returns whether
    /// anything moved.
    ///
    /// **This must run immediately before any decision that reads
    /// `ElectionSm::durable` as "our log".** That counter has two independent
    /// readers on two threads: the RECEIVER agent reads it directly and reports
    /// it to the leader, which ranks those reports into `commit`
    /// (`uc2_net/src/receiver.rs`, the `DGRAM_KIND_APPEND_POSITION` send); this
    /// is the consensus agent's absorbed copy. Raft's vote rule is only sound if
    /// a voter compares a candidate against everything the voter has DURABLY
    /// STORED — and the counter is that, while a stale copy of it is not.
    ///
    /// Granting on an UNDER-estimate of our own log is the unsafe direction: it
    /// lets a candidate that is behind a committed position collect our vote,
    /// win, and collapse the log below that commit — acked-write loss. See
    /// `a_vote_is_granted_against_a_stale_self_view_of_our_own_log`.
    ///
    /// Symmetry matters as much as freshness: `start_election` advertises this
    /// same field as `last_durable`, so refreshing only the GRANT side would
    /// leave candidates systematically under-advertising against voters who
    /// compare fresh — elections lost for no reason. Hence the call at the top
    /// of the duty cycle (before `Event::Tick` can start a candidacy) as well as
    /// the one immediately before answering a `RequestVote`.
    ///
    /// The `pending_leader_open` guard is issue #6's: mid-collapse the counter
    /// still holds the pre-cut frontier while `base` is the authoritative one,
    /// and `base` IS `sm.durable`, so leaving it alone is exactly right.
    fn refresh_durable(&mut self) -> bool {
        if self.pending_leader_open.is_some() {
            return false;
        }
        // Clamp to the TERM-OBSERVATION frontier (2026-08-16 hunt). The
        // archive publishes the durable counter inside `do_work` and only
        // afterwards hands over the term observations describing those same
        // bytes, so a raw read can be a duty cycle ahead of the map. Reconcile
        // compares the leader's map against OUR map bounded by this durable:
        // with the raw value, bytes we had just received from that very leader
        // — but not yet attributed — looked like an unexplained tail, i.e.
        // divergence, and got truncated. Committed and applied ones included:
        // that was the residual `prov=("gossip")` rewind. Feeding the observed
        // frontier instead makes the SM's durable and its term map describe
        // the same prefix. Lagging by at most one cycle is conservative
        // everywhere else it is used (vote credentials, commit ranking).
        let raw = self.cnc.counters().durable.load_acquire();
        let d = raw.min(self.obs_frontier.load(Ordering::Acquire));
        if d <= self.durable_seen {
            return false;
        }
        self.durable_seen = d;
        self.feed(Event::DurableAdvanced { durable: d });
        self.publish_validated_frontier();
        true
    }

    /// Mirror the SM's validated frontier for the receiver (its
    /// `AppendPosition` reports are clamped to it). Called wherever the
    /// frontier can move: a durable advance, and the end of every event drain.
    fn publish_validated_frontier(&self) {
        self.reports_unattested.store(self.sm.reports_unattested(), Ordering::Relaxed);
        // Term first, then position: a reader that samples between the two
        // sees an older position with a newer term, which fails the leader's
        // attestation check (the safe direction) rather than passing wrongly.
        self.validated_term.store(self.sm.validated_term(), Ordering::Release);
        self.validated_frontier.store(self.sm.validated_up_to(), Ordering::Release);
    }

    /// Issue #6, leader open phase 2: the archive finished the collapse to
    /// `base` — it cut its journal there, reset its own `durable_pos`, and
    /// primed the counters. Only NOW is it safe to write into the buffer: the
    /// archive's cursor sits exactly at `base`, so the new leader's frames
    /// cannot land mid-walk.
    ///
    /// A non-matching ack is dropped. That happens when the node stepped down or
    /// adopted a higher term while the collapse was in flight
    /// (`Action::BecomeFollower` clears `pending_leader_open`) — the physical cut
    /// still happened and is still correct (it only dropped this node's own
    /// unreplicated tail), but the leadership it was opening is gone.
    fn on_collapsed(&mut self, epoch: u64, to: u64) {
        // The archive re-primed the counters to `to`; keep our shadow in step so
        // we don't refeed a spurious DurableAdvanced (same as `on_truncated`).
        self.durable_seen = to;
        let Some(open) = self.pending_leader_open.take_if(|o| o.epoch == epoch) else {
            return;
        };
        // The ack carries the position the archive ACTUALLY cut to, which may be
        // BELOW the `base` we asked for: a reconcile truncation queued ahead of
        // this collapse already removed everything above its own cut, and the
        // `Collapse` arm clamps rather than fail-stopping (see it for why that
        // interleaving is reachable). Everything below is keyed off the acked
        // `to`, never `open.base` — the appender is built from the counters the
        // archive primed, so it opens the term at the real frontier.
        debug_assert!(open.base >= to, "the archive never cuts ABOVE the requested base");
        let mut appender = Appender::new(Arc::clone(&self.buffer), open.term);
        appender.append_new_term().expect("NewTerm append fail-stop");
        // The serving gate compares COMMIT (an end/frontier position) against
        // this value, so it must be the frame's END — feeding the start would
        // flip can_serve before the NewTerm frame is quorum-committed (at base
        // 0: instantly). Raft §5.4.2.
        let end = appender.position();
        self.appender = Some(appender);
        self.feed(Event::NewTermAppended { position: end });
        self.open_gate();
        self.leader_flag.store(true, Ordering::Release);
        // We ARE the leader of this term — published only now that we can act
        // like one (see the note in `Action::BecomeLeader`).
        self.cnc.status().leader_hint.store_release(self.id as u64);
    }

    fn on_truncated(&mut self, epoch: u64, to: u64) {
        // The archive re-primed the counters to `to`; keep our shadow in step so
        // we don't refeed a spurious DurableAdvanced.
        self.durable_seen = to;
        let matching = self.pending_truncation == Some(epoch);
        self.feed(Event::Truncated { epoch, to });
        if matching {
            self.pending_truncation = None;
            self.truncations.fetch_add(1, Ordering::Relaxed);
            // Finding #9 (lean LC2): same handle-keying as the clean-reconcile
            // reopen in `feed`. A CANDIDATE's truncating reconcile (handle lags
            // its bumped `current_term`) must NOT reopen intake for its stale
            // handle regime when the ack lands — `Action::Truncate` cleared
            // `awaiting_reconcile` (node.rs ~2599) so the old `!awaiting_reconcile`
            // gate alone would reopen. `current_term == adopted_term` fails for a
            // candidate (its handle still names the pre-bump term); it holds for a
            // follower whose adoption re-keyed the handle. The candidate stays
            // closed until it resolves (BecomeLeader / step-down / higher-term
            // adoption).
            // Issue #6: and not while a leader open is still awaiting its
            // collapse ack. Both predicates above hold during that window
            // (`BecomeLeader` clears `awaiting_reconcile` and sets
            // `adopted_term`), so without this a truncation ack landing there
            // would admit DATA between the archive's re-prime and `on_collapsed`
            // installing the appender — a second writer at the very positions
            // the NewTerm frame is about to take. `on_collapsed` does the reopen.
            //
            // Like the `ArchiveCmd::Collapse` clamp, this is DEAD DEFENCE today:
            // it needs a reconcile truncation and a leader open in flight at
            // once, which
            // `a_reconcile_truncating_node_cannot_also_open_a_leader_term`
            // shows cannot happen. Kept because the predicate is free and the
            // failure it prevents is a torn log, not a crash.
            if !self.awaiting_reconcile
                && self.sm.current_term() == self.adopted_term
                && self.pending_leader_open.is_none()
            {
                self.open_gate();
            }
        }
    }

    fn open_gate(&self) {
        self.intake_gate.store(true, Ordering::Release);
    }

    fn close_gate(&self) {
        self.intake_gate.store(false, Ordering::Release);
    }

    /// Stage one consensus datagram: cleartext header + body, `key_epoch`
    /// left at 0 (a group-scope seal stamps the real epoch itself, as the
    /// last write before the AEAD call, since the header is AAD; a
    /// pairwise-scope seal never uses the field at all).
    fn stage(kind: u8, position: u64, term: u32, body: &[u8]) -> Vec<u8> {
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN + body.len()];
        write_datagram_header(
            &mut d,
            &DatagramHeader { position, leadership_term_id: term, kind, flags: 0, key_epoch: 0 },
        );
        d[DATAGRAM_HEADER_LEN..].copy_from_slice(body);
        d
    }

    /// One **pairwise-scope** consensus datagram to one peer (`VOTE`,
    /// `REQUEST_VOTE`, `TERM_MAP`, `READ_PROBE_ACK`, `CONFIG_PROPOSAL`,
    /// `CONFIG_REPLY`), sealed (M8 Task 17) if crypto is enabled.
    ///
    /// Sealed through `SharedTransport::seal_pairwise_control`, NOT through a
    /// second `SendHalf`: `SharedTransport::send_half` is single-call by
    /// design and the one half went to the sender agent. Two halves would
    /// mean two nonce counters over one key, and a repeated nonce under
    /// AES-256-GCM leaks the authentication subkey rather than one message.
    /// The control path draws from the process's one shared counter.
    ///
    /// Fail-closed: a destination with no `NodeId` (`addr_to_id`) or no
    /// established session is DROPPED and counted, never sent in the clear.
    fn send(&mut self, to: SocketAddr, kind: u8, position: u64, term: u32, body: &[u8]) {
        debug_assert!(
            matches!(Transport::scope_of(kind), Scope::Pairwise),
            "Consensus::send seals with one destination's pairwise key; kind {kind} is not \
             Scope::Pairwise — a fan-out kind belongs in `fan_out_group`, which seals ONCE"
        );
        let mut d = Self::stage(kind, position, term, body);
        if self.crypto.is_some() {
            let Some(&peer) = self.addr_to_id.get(&to) else {
                self.crypto_unresolved_peer.fetch_add(1, Ordering::Relaxed);
                self.crypto_seal_failures.fetch_add(1, Ordering::Relaxed);
                return;
            };
            let sealed = self
                .crypto
                .as_ref()
                .expect("checked Some just above")
                .seal_pairwise_control(kind, peer, &mut d);
            if let Err(e) = sealed {
                self.crypto_seal_failures.fetch_add(1, Ordering::Relaxed);
                let now = self.crypto_now_ns();
                self.crypto_log(
                    now,
                    format_args!("dropped a kind-{kind} datagram for node {peer}: {e}"),
                );
                return;
            }
        }
        let _ = self.sock.send_to(&d, to);
    }

    /// One **group-scope** consensus datagram (`COMMIT_POSITION`,
    /// `READ_PROBE`) fanned out to `targets`: staged once, sealed ONCE, then
    /// the identical bytes sent N times — which is the entire reason group
    /// scope exists (spec §3: "a leader seals once and sends N times").
    ///
    /// `READ_PROBE` is why the group branch had to reach `SharedTransport` at
    /// all (`seal_group_control`, ruling 2026-07-29):
    /// `seal_pairwise_control` explicitly refuses group kinds, and routing
    /// the read barrier's probe through the sender agent would need a new
    /// cross-agent channel on the linearizable-read hot path.
    ///
    /// Fail-closed and ALL-OR-NOTHING: a failed seal sends to nobody. A
    /// half fan-out (some peers served, some not) is not a state this
    /// produces — same discipline as `Sender::fan_out`.
    fn fan_out_group(
        &mut self,
        targets: &[NodeId],
        kind: u8,
        position: u64,
        term: u32,
        body: &[u8],
    ) {
        debug_assert!(
            matches!(Transport::scope_of(kind), Scope::Group),
            "fan_out_group seals once for every destination, which is only correct for \
             Scope::Group kinds; kind {kind} is not one"
        );
        let mut d = Self::stage(kind, position, term, body);
        if self.crypto.is_some() {
            // `now_ns` from the crypto plane's own clock, never
            // `Consensus::base` — `GroupPlane::sealing_epoch` compares it
            // against the mint timestamp. See `crypto_now_ns`'s doc and
            // `uc2_crypto::transport`'s "One clock source" module docs.
            let now = self.crypto_now_ns();
            let sealed = self
                .crypto
                .as_ref()
                .expect("checked Some just above")
                .seal_control(kind, None, &mut d, now);
            if let Err(e) = sealed {
                self.crypto_seal_failures.fetch_add(1, Ordering::Relaxed);
                self.crypto_log(now, format_args!("dropped a kind-{kind} fan-out: {e}"));
                return;
            }
        }
        for id in targets {
            // T17 review, M4: count what is skipped. `Consensus::send` bumps
            // `crypto_unresolved_peer` on exactly this condition (a peer id
            // with no address in the adopted config — a config change that
            // raced the send); the fan-out silently dropped it. Counted
            // UNCONDITIONALLY, unlike `send`'s crypto-gated site: this lookup
            // runs in both modes, because the pre-T17 code was
            // `self.id_to_addr[&id]`, which PANICKED the consensus agent on a
            // miss. Turning that panic into a silent skip was the right
            // trade; turning it into an invisible one was not.
            let Some(&addr) = self.id_to_addr.get(id) else {
                self.crypto_unresolved_peer.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            let _ = self.sock.send_to(&d, addr);
        }
    }
}

// ------------------------------------------------------------------- helpers

/// Reuse the log-buffer file when it already matches the configured capacity
/// (preserves the ring bytes below `durable` across a restart — free
/// NAK-serving prefill), otherwise create it fresh at `capacity`.
fn open_or_create_buffer(
    path: &std::path::Path,
    capacity: u64,
    cnc: Arc<CncPage>,
    max_payload: usize,
) -> io::Result<LogBuffer> {
    let reuse = std::fs::metadata(path).map(|m| m.len() == capacity).unwrap_or(false);
    if reuse {
        LogBuffer::open_file(path, cnc, max_payload)
    } else {
        LogBuffer::create_file(path, capacity, cnc, max_payload)
    }
}

/// Create the node's shared-memory IPC ring files fresh (unlinking any stale
/// file first — a prior instance's attachment is invalidated by the new
/// instance_id anyway). Sizes are fixed by the spec: ingress 4 MiB, query
/// 1 MiB, svc_query 1 MiB, both broadcasts 4 MiB; 64 KiB max message each.
/// Returns the retained `Rings`, plus the four node-side ring halves the
/// consensus agent drives: the ingress ring's CONSUMER + the node egress ring's
/// PRODUCER (Task 7), and the query ring's CONSUMER + the svc_query ring's
/// PRODUCER (Task 11 — the node reads client queries and forwards barrier-passed
/// reads to the service). Every counterpart half (the ingress + query
/// producers, the egress_node consumer, the svc_query consumer) is dropped
/// here: the node never uses them, and attaching clients/service open the files
/// themselves to get their own halves.
fn create_rings(
    dir: &InstanceDir,
) -> io::Result<(Rings, MpscConsumer, BroadcastProducer, MpscConsumer, SpscProducer)> {
    const MIB: u64 = 1 << 20;
    const MAX_MSG: u32 = 64 << 10;
    for p in [
        dir.ingress_ring(),
        dir.query_ring(),
        dir.svc_query_ring(),
        dir.egress_service(),
        dir.egress_node(),
    ] {
        let _ = std::fs::remove_file(&p);
    }
    let ingress = MpscRing::create(&dir.ingress_ring(), 4 * MIB, MAX_MSG).map_err(to_io)?;
    let (_ingress_producer, ingress_consumer) = ingress.into_split();
    let egress_node = BroadcastRing::create(&dir.egress_node(), 4 * MIB, MAX_MSG).map_err(to_io)?;
    let egress_node_producer = egress_node.producer();
    // Query ring (clients → node, MPSC): keep the node's CONSUMER half.
    let query = MpscRing::create(&dir.query_ring(), MIB, MAX_MSG).map_err(to_io)?;
    let (_query_producer, query_consumer) = query.into_split();
    // svc_query ring (node → service, SPSC): keep the node's PRODUCER half.
    let svc_query = SpscRing::create(&dir.svc_query_ring(), MIB, MAX_MSG).map_err(to_io)?;
    let (svc_query_producer, _svc_query_consumer) = svc_query.into_split();
    Ok((
        Rings {
            egress_service: BroadcastRing::create(&dir.egress_service(), 4 * MIB, MAX_MSG)
                .map_err(to_io)?,
        },
        ingress_consumer,
        egress_node_producer,
        query_consumer,
        svc_query_producer,
    ))
}

// --------------------------------------------------------- M7 config helpers

/// The wire `Addr` tuple (`uc2_consensus::config::Addr = (ip: u32, port:
/// u16)`) as a real `SocketAddr` (IPv4-only — `uc2_consensus` stays dep-free,
/// so this conversion lives here). Inverse of `stored_member`'s ip/port
/// extraction below.
fn addr_of((ip, port): Addr) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(ip.to_be_bytes()), port))
}

/// A real `SocketAddr` (IPv4-only) as the wire `Addr` tuple. Used only at
/// genesis-seed construction (`Node::start`), where `NodeConfig::members`/
/// `learners` are still plain `(NodeId, SocketAddr)`. Inverse of `addr_of`.
fn addr_to_pair(a: SocketAddr) -> Addr {
    match a {
        SocketAddr::V4(v4) => (u32::from_be_bytes(v4.ip().octets()), v4.port()),
        SocketAddr::V6(_) => panic!("uc2 is IPv4-only (addr={a})"),
    }
}

/// Genesis-seed helper: `(NodeId, SocketAddr)` -> `StoredMember`.
fn stored_member(id: NodeId, addr: SocketAddr) -> StoredMember {
    let (ip, port) = addr_to_pair(addr);
    StoredMember { id, ip, port }
}

/// M7 Task 7: decode the cnc admin-req / `ConfigProposalBody` wire fields
/// (`op` 1..=5, `id`, `ip`, `port`) into a `ConfigOp` — the inverse of
/// `ClusterConfig::op_code`. `None` for an out-of-range `op` (never emitted by
/// `uc2ctl`; a defensive catch-all for a malformed/future-version request).
fn wire_to_config_op(op: u32, id: NodeId, ip: u32, port: u16) -> Option<ConfigOp> {
    match op {
        1 => Some(ConfigOp::AddLearner { id, addr: (ip, port) }),
        2 => Some(ConfigOp::PromoteLearner { id }),
        3 => Some(ConfigOp::DemoteVoter { id }),
        4 => Some(ConfigOp::RemoveLearner { id }),
        5 => Some(ConfigOp::RemoveVoter { id }),
        _ => None,
    }
}

/// `WireConfig` (the decoded `FRAME_TYPE_CONFIG` payload) -> `ClusterConfig`
/// (the SM's in-memory form). Purely numeric — `WireMember`'s `(ip, port)` IS
/// the `Addr` shape already, no `SocketAddr` involved.
fn wire_to_cluster_config(w: &WireConfig) -> ClusterConfig {
    ClusterConfig {
        version: w.version,
        voters: w.voters.iter().map(|m| (m.id, (m.ip, m.port))).collect(),
        learners: w.learners.iter().map(|m| (m.id, (m.ip, m.port))).collect(),
        tombstones: w.tombstones.clone(),
    }
}

/// Post-M7 follow-up: same version + different content = divergence (two
/// configs minted at one version — possible only via the wipe-fiat position
/// reset or a bug), never a benign re-observation. The version gate in
/// `Event::ConfigObserved` SILENTLY drops equal versions, so without this
/// check divergence is invisible.
pub(crate) fn config_content_diverges(current: &ClusterConfig, incoming: &ClusterConfig) -> bool {
    incoming.version == current.version && incoming != current
}

/// `ClusterConfig` -> the `WireConfig` to append as a `FRAME_TYPE_CONFIG`
/// payload. `prev_position` is an audit-trail field only (the durable
/// `ConfigRecord` keeps the authoritative prev) — the caller passes the
/// CURRENTLY-adopted config's position, the entry `c` supersedes.
fn cluster_to_wire(c: &ClusterConfig, prev_position: u64) -> WireConfig {
    WireConfig {
        version: c.version,
        prev_position,
        voters: c
            .voters
            .iter()
            .map(|(id, (ip, port))| WireMember { id: *id, ip: *ip, port: *port })
            .collect(),
        learners: c
            .learners
            .iter()
            .map(|(id, (ip, port))| WireMember { id: *id, ip: *ip, port: *port })
            .collect(),
        tombstones: c.tombstones.clone(),
    }
}

/// The single derivation of the snapshot-session config-carry cache's bytes
/// (final-review fix): `encode_config(&cluster_to_wire(..))`, called from
/// every site that refreshes `config_bytes` — boot-time construction AND
/// `rebuild_net_for_config` (shared in turn by the live-adoption exec arm and
/// the snapshot-fiat install path). One derivation means the three sites
/// cannot drift apart the way construction/exec-arm vs. fiat-install did
/// before this fix (fiat install rebuilt peers/routing but never refreshed
/// this cache, so a below-floor rejoiner that later became leader would ship
/// its STALE pre-fall config to the next joiner).
fn config_wire_bytes(config: &ClusterConfig, prev_position: u64) -> Vec<u8> {
    let mut wire_bytes = Vec::new();
    encode_config(&cluster_to_wire(config, prev_position), &mut wire_bytes);
    wire_bytes
}

/// `StoredConfig` (the durable `ConfigRecord`'s `config`/`prev`) -> `ClusterConfig`.
fn stored_to_cluster(s: &StoredConfig) -> ClusterConfig {
    ClusterConfig {
        version: s.version,
        voters: s.voters.iter().map(|m| (m.id, (m.ip, m.port))).collect(),
        learners: s.learners.iter().map(|m| (m.id, (m.ip, m.port))).collect(),
        tombstones: s.tombstones.clone(),
    }
}

/// `ClusterConfig` -> `StoredConfig`, for persisting a `ConfigRecord`.
fn cluster_to_stored(c: &ClusterConfig) -> StoredConfig {
    StoredConfig {
        version: c.version,
        voters: c
            .voters
            .iter()
            .map(|(id, (ip, port))| StoredMember { id: *id, ip: *ip, port: *port })
            .collect(),
        learners: c
            .learners
            .iter()
            .map(|(id, (ip, port))| StoredMember { id: *id, ip: *ip, port: *port })
            .collect(),
        tombstones: c.tombstones.clone(),
    }
}

/// M7 Task 8: the VOTING cluster size to pass as `FlowControl::new`'s
/// `cluster_size` for `id`'s OWN sender, over `config`. Mirrors
/// `ElectionSm::rebuild_membership`'s `CommitTracker` sizing convention
/// exactly (own_durable / self always occupies a ranking slot, member or
/// not): if `id` IS a voter in `config`, it already occupies one of
/// `config.voters.len()` slots and that count is the cluster size outright.
/// If `id` is NOT a voter — a learner (though callers special-case learners
/// with a dummy solo sender before ever reaching here), a genuinely unknown
/// orphan booting from a stale seed, or — Task 8's real case — a LEADER
/// mid-self-removal that must keep serving/replicating until its own removal
/// commits — it occupies an UNCOUNTED "+1" slot that
/// `FlowControl::new`'s `cluster_size > voting_followers.len()` assert
/// requires to exist (`followers`/`voting` in that case is the FULL voter
/// list, unfiltered, since `id` isn't in it to filter out — using the plain
/// voter count here would make `cluster_size == followers.len()` and panic
/// the instant a non-member's own sender is (re)built).
fn sender_cluster_size(config: &ClusterConfig, id: NodeId) -> usize {
    let n = config.voters.len();
    if config.is_voter(id) { n } else { n + 1 }
}

/// Derive the peer-address maps + observability band from a `ClusterConfig`
/// for `id` (voters first, then learners, cnc-slot order, capped at
/// `CNC_MAX_PEER_SLOTS`). A free function (not a `Consensus` method) because
/// `Node::start` needs it BEFORE any `Consensus` exists — the `Sender`'s
/// fan-out is derived from the same maps and must be constructed (and moved
/// into its own thread) first. `Consensus::rebuild_peer_maps` is a thin
/// wrapper that calls this and assigns the result to its own fields, so
/// construction-time seeding and live-reconfiguration rebuild share ONE
/// derivation (behavior-preservation for a cluster that never reconfigures).
#[allow(clippy::type_complexity)]
fn derive_peer_maps(
    config: &ClusterConfig,
    id: NodeId,
) -> (
    HashMap<NodeId, SocketAddr>,
    HashMap<SocketAddr, NodeId>,
    Vec<NodeId>,
    Vec<NodeId>,
    Vec<(NodeId, u8)>,
) {
    let mut id_to_addr = HashMap::new();
    let mut addr_to_id = HashMap::new();
    for (mid, addr) in config.voters.iter().chain(config.learners.iter()) {
        let sock = addr_of(*addr);
        id_to_addr.insert(*mid, sock);
        addr_to_id.insert(sock, *mid);
    }
    let peers: Vec<NodeId> = config.voter_ids().into_iter().filter(|i| *i != id).collect();
    let learner_ids: Vec<NodeId> =
        config.learners.iter().map(|(lid, _)| *lid).filter(|i| *i != id).collect();
    let peer_band: Vec<(NodeId, u8)> = peers
        .iter()
        .map(|i| (*i, CNC_PEER_ROLE_VOTER))
        .chain(learner_ids.iter().map(|i| (*i, CNC_PEER_ROLE_LEARNER)))
        .take(CNC_MAX_PEER_SLOTS)
        .collect();
    (id_to_addr, addr_to_id, peers, learner_ids, peer_band)
}

fn to_pairs(m: &TermMap) -> Vec<(u32, u64)> {
    m.iter().map(|e| (e.term, e.base)).collect()
}

fn to_entries(pairs: &[(u32, u64)]) -> TermMap {
    pairs.iter().map(|(term, base)| TermMapEntry { term: *term, base: *base }).collect()
}

/// Encode a term-map suffix (≤ `MAX_TERM_MAP_WIRE_ENTRIES`) into a datagram body.
fn encode_term_map(entries: &[(u32, u64)]) -> Vec<u8> {
    let n = entries.len().min(MAX_TERM_MAP_WIRE_ENTRIES);
    let wire: Vec<TermMapEntryWire> =
        entries[..n].iter().map(|(term, base)| TermMapEntryWire { term: *term, base: *base }).collect();
    let mut body = vec![0u8; TERM_MAP_HEADER_LEN + n * TERM_MAP_ENTRY_LEN];
    let written = write_term_map_body(&mut body, &wire);
    body.truncate(written);
    body
}

/// Re-derive the complete term map for the durable journal frontier by walking
/// frame headers from the last persisted term's base forward (T4 carry 4).
/// Returns the completed `(term, base)` map; equal to `recovered` when the
/// persisted map already covers the frontier.
fn rederive_term_map(
    archive: &Archive,
    recovered: &TermMap,
) -> Result<Vec<(u32, u64)>, uc2_log::archive::ArchiveError> {
    let mut map = to_pairs(recovered);
    let (start, mut last_term) = match map.last() {
        Some((t, b)) => (*b, *t),
        None => (0, 0),
    };
    // M6 Task 4: after a purge, the persisted map's last base may sit below the
    // archive's first retained block — replaying from it would be `PositionPurged`.
    // Everything below `first_base` is already in the persisted map (and covered
    // by the snapshot the purge floor represents); only the retained tail can
    // contain terms not yet stamped, so clamp the scan start to `first_base`.
    let start = start.max(archive.first_base());
    let mut replay = archive.replay_from(start)?;
    while let Some(frame) = replay.next()? {
        let t = frame.header.leadership_term_id;
        // A strictly higher term is a new leadership term opening at this frame.
        if t > last_term {
            map.push((t, frame.position));
            last_term = t;
        }
    }
    Ok(map)
}

/// M7 (spec 2026-07-13) boot recovery of the durable `ConfigRecord`, in order:
///
/// 1. **Genesis-seed** a fresh instance dir (no record yet) from `members`/
///    `learners` — authoritative only here (see `NodeConfig::members`'s doc);
///    every subsequent boot, and every live reconfiguration, is owned by the
///    durable record + the `FRAME_TYPE_CONFIG` stream from here on.
///    Behavior-preservation: a cluster that never appends a config frame gets
///    this genesis record (version 0, `prev == config`) verbatim forever —
///    nothing observably changes from the pre-M7 static wiring.
/// 2. **T5-carry revert** (M7 Task 6): the leader persists the `ConfigRecord`
///    SYNCHRONOUSLY at append (`append_config_frame` -> `Action::ConfigAdopted`'s
///    persist), strictly AHEAD of the archive's own (async) fsync of that same
///    frame's bytes. A crash in that window recovers a durable record whose
///    `position` the recovered archive frontier (`durable`) does NOT actually
///    back — the record is claiming survival for bytes that never made it.
///    Revert it to its own prev level (mirrors `uc2_sim::World::on_restart`'s
///    boot-revert); if THAT level is ALSO ahead of durable (a compounding
///    same-run crash window: two adoptions before any archive catch-up), there
///    is nothing genuine left in the record to trust, so fall back to a fresh
///    config-by-fiat genesis record — the same seed as step 1, just re-run
///    because the "existing" record turned out unusable.
/// 3. **Step 3a forward re-derivation**: the reverse gap. A FOLLOWER's archive
///    scan durably records a CONFIG frame and only afterward (a later duty
///    cycle) drains it into `Event::ConfigObserved` -> the persist. A crash in
///    THAT window durably archives the frame but never persists the record, so
///    recovery must scan the SAME retained window `rederive_term_map` scans
///    (`archive.first_base()`-clamped) for CONFIG frames above the (possibly
///    just-reverted) record's position and re-adopt them, exactly like a live
///    `ConfigObserved` would (`rederive_config`, idempotent by version).
///
/// Persists the record back to `state` whenever any step changes it, so the
/// returned value and `state.config_record()` always agree. Extracted to a
/// free function (rather than inlined in `Node::start_with_socket`) so it is
/// unit-testable against a real file-backed `Archive`/`NodeState` without
/// standing up a full two-thread `Node`.
pub(crate) fn recover_config_record(
    state: &NodeState,
    archive: &Archive,
    durable: u64,
    members: &[(NodeId, SocketAddr)],
    learners: &[(NodeId, SocketAddr)],
) -> io::Result<ConfigRecord> {
    let seed = || {
        let genesis = StoredConfig {
            version: 0,
            voters: members.iter().map(|(id, a)| stored_member(*id, *a)).collect(),
            learners: learners.iter().map(|(id, a)| stored_member(*id, *a)).collect(),
            tombstones: Vec::new(),
        };
        ConfigRecord { position: 0, config: genesis.clone(), prev_position: 0, prev: genesis }
    };
    if state.config_record().is_none() {
        state.store_config_record(&seed()).map_err(to_io)?;
    }
    let mut rec = state.config_record().expect("seeded immediately above if absent");

    if rec.position > durable {
        rec = if rec.prev_position > durable {
            seed()
        } else {
            ConfigRecord {
                position: rec.prev_position,
                config: rec.prev.clone(),
                prev_position: rec.prev_position,
                prev: rec.prev,
            }
        };
        state.store_config_record(&rec).map_err(to_io)?;
    }

    let rederived = rederive_config(archive, rec.clone()).map_err(to_io)?;
    if rederived != rec {
        state.store_config_record(&rederived).map_err(to_io)?;
    }
    Ok(rederived)
}

/// M7 Task 6 (Step 3a): recovery counterpart of `rederive_term_map` above for
/// the `ConfigRecord` — see the boot call site's doc for the crash window this
/// closes (a follower durably archives a `FRAME_TYPE_CONFIG` frame and only a
/// LATER duty cycle drains it into the persisted record; a crash in between
/// loses the persist but not the archived bytes). Scans archived frames from
/// `rec.position` (clamped to `archive.first_base()`, exactly like
/// `rederive_term_map`'s clamp — everything below it is either already folded
/// into `rec` or covered by a snapshot floor) forward, folding in every
/// `FRAME_TYPE_CONFIG` frame whose decoded version strictly exceeds the
/// currently-folded config's version (idempotent / monotone, exactly
/// `Event::ConfigObserved`'s own adoption guard). Produces the same one-level
/// prev/cur shape `Action::ConfigAdopted`'s exec arm persists, so even a
/// multi-hop scan (more than one adoption crash-exposed in the same window)
/// folds down to a valid one-level record.
pub(crate) fn rederive_config(
    archive: &Archive,
    rec: ConfigRecord,
) -> Result<ConfigRecord, uc2_log::archive::ArchiveError> {
    let start = rec.position.max(archive.first_base());
    let mut cur = rec;
    let mut replay = archive.replay_from(start)?;
    while let Some(frame) = replay.next()? {
        if frame.header.frame_type != FRAME_TYPE_CONFIG {
            continue;
        }
        let wire = decode_config(&frame.payload)
            .unwrap_or_else(|| panic!("corrupt CONFIG frame at {}", frame.position));
        if wire.version <= cur.config.version {
            continue; // idempotent / stale re-observation
        }
        let frame_end = frame.position + align_frame_len(frame.header.length as usize) as u64;
        let new_config = cluster_to_stored(&wire_to_cluster_config(&wire));
        cur = ConfigRecord {
            position: frame_end,
            prev_position: cur.position,
            prev: cur.config,
            config: new_config,
        };
    }
    Ok(cur)
}


#[cfg(test)]
mod journal_durability_env_tests {
    use super::journal_durability_from_env;
    use uc2_log::Durability;

    // Serialized by a lock: std::env is process-global (house pattern —
    // timeout_and_restart.rs isolates env in its own binary; a static lock
    // does the same job inside one).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn with_env(val: Option<&str>, f: impl FnOnce()) {
        let _g = ENV_LOCK.lock().unwrap();
        // SAFETY: guarded by ENV_LOCK; no other test in this binary touches
        // this variable outside `with_env`.
        unsafe {
            match val {
                Some(v) => std::env::set_var("UC2_JOURNAL_DURABILITY", v),
                None => std::env::remove_var("UC2_JOURNAL_DURABILITY"),
            }
        }
        f();
        unsafe { std::env::remove_var("UC2_JOURNAL_DURABILITY") }
    }

    #[test]
    fn unset_and_consistent_and_case_map_to_consistent() {
        with_env(None, || {
            assert_eq!(journal_durability_from_env().unwrap(), Durability::Consistent);
        });
        with_env(Some("consistent"), || {
            assert_eq!(journal_durability_from_env().unwrap(), Durability::Consistent);
        });
        with_env(Some("Consistent"), || {
            assert_eq!(journal_durability_from_env().unwrap(), Durability::Consistent);
        });
    }

    #[test]
    fn eventual_maps_to_eventual() {
        with_env(Some("eventual"), || {
            assert_eq!(journal_durability_from_env().unwrap(), Durability::Eventual);
        });
    }

    #[test]
    fn unrecognized_value_is_refused_not_guessed() {
        with_env(Some("fastest"), || {
            let err = journal_durability_from_env().unwrap_err();
            assert!(err.contains("fastest"), "{err}");
        });
    }

    fn with_interval_env(val: Option<&str>, f: impl FnOnce()) {
        let _g = ENV_LOCK.lock().unwrap();
        // SAFETY: same ENV_LOCK discipline as `with_env`.
        unsafe {
            match val {
                Some(v) => std::env::set_var("UC2_JOURNAL_EVENTUAL_FSYNC_MS", v),
                None => std::env::remove_var("UC2_JOURNAL_EVENTUAL_FSYNC_MS"),
            }
        }
        f();
        unsafe { std::env::remove_var("UC2_JOURNAL_EVENTUAL_FSYNC_MS") }
    }

    #[test]
    fn interval_unset_is_none_and_set_maps_under_eventual() {
        use super::eventual_fsync_interval_from_env;
        with_interval_env(None, || {
            assert_eq!(
                eventual_fsync_interval_from_env(Durability::Eventual).unwrap(),
                None
            );
        });
        with_interval_env(Some("5"), || {
            assert_eq!(
                eventual_fsync_interval_from_env(Durability::Eventual).unwrap(),
                Some(std::time::Duration::from_millis(5))
            );
        });
    }

    #[test]
    fn interval_under_consistent_is_ignored_with_warning() {
        use super::eventual_fsync_interval_from_env;
        with_interval_env(Some("5"), || {
            assert_eq!(
                eventual_fsync_interval_from_env(Durability::Consistent).unwrap(),
                None
            );
        });
    }

    #[test]
    fn interval_zero_or_garbage_is_refused() {
        use super::eventual_fsync_interval_from_env;
        with_interval_env(Some("0"), || {
            assert!(eventual_fsync_interval_from_env(Durability::Eventual).is_err());
        });
        with_interval_env(Some("fast"), || {
            assert!(eventual_fsync_interval_from_env(Durability::Eventual).is_err());
        });
    }
}

/// `UC2_JOURNAL_EVENTUAL_FSYNC_MS` — companion knob to
/// `UC2_JOURNAL_DURABILITY=eventual`: the journal writer's async-fsync
/// interval in milliseconds (default 1 — the tail-parity setting from the
/// 2026-08-17 interval retest). Only meaningful under Eventual;
/// if set while durability is Consistent it is IGNORED WITH A WARNING (the
/// value is inert there, unlike a durability typo, which is refused).
/// Zero or unparseable values are refused — fail-closed like the
/// durability knob itself.
fn eventual_fsync_interval_from_env(
    durability: uc2_log::Durability,
) -> Result<Option<std::time::Duration>, String> {
    match std::env::var("UC2_JOURNAL_EVENTUAL_FSYNC_MS") {
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(format!("UC2_JOURNAL_EVENTUAL_FSYNC_MS unreadable: {e}")),
        Ok(v) => {
            if durability != uc2_log::Durability::Eventual {
                eprintln!(
                    "uc2_node: UC2_JOURNAL_EVENTUAL_FSYNC_MS={v} ignored — \
                     durability is Consistent (interval is Eventual-only)"
                );
                return Ok(None);
            }
            match v.parse::<u64>() {
                Ok(ms) if ms > 0 => Ok(Some(std::time::Duration::from_millis(ms))),
                _ => Err(format!(
                    "UC2_JOURNAL_EVENTUAL_FSYNC_MS={v:?} invalid (expected a \
                     positive integer of milliseconds); refusing to guess"
                )),
            }
        }
    }
}

/// `UC2_JOURNAL_DURABILITY` — opt-in env knob for the archive's journal
/// durability (the house env-toggle pattern, like `UC_JOURNAL_PREALLOC`).
/// Unset or `consistent` = `Durability::Consistent` (fdatasync per block —
/// the default posture every gate and spec guarantee assumes). `eventual` =
/// `Durability::Eventual`: the durable counter advances on the buffered
/// write and durability comes from REPLICATION, not disk (power loss can
/// drop acked bytes; see `ArchiveConfig::durability` for the loss model) —
/// benchmark / explicit-deployment opt-in only. Any other value is refused
/// loudly (fail-closed: a typo must not silently pick a posture).
fn journal_durability_from_env() -> Result<uc2_log::Durability, String> {
    match std::env::var("UC2_JOURNAL_DURABILITY") {
        Err(std::env::VarError::NotPresent) => Ok(uc2_log::Durability::Consistent),
        Err(e) => Err(format!("UC2_JOURNAL_DURABILITY unreadable: {e}")),
        Ok(v) => match v.to_ascii_lowercase().as_str() {
            "" | "consistent" => Ok(uc2_log::Durability::Consistent),
            "eventual" => {
                eprintln!(
                    "uc2_node: UC2_JOURNAL_DURABILITY=eventual — journal fsync is \
                     ASYNCHRONOUS; acked positions may be lost on power failure \
                     (durability by replication). Opt-in posture; not the default."
                );
                Ok(uc2_log::Durability::Eventual)
            }
            other => Err(format!(
                "UC2_JOURNAL_DURABILITY={other:?} unrecognized (expected \
                 'consistent' or 'eventual'); refusing to guess a durability posture"
            )),
        },
    }
}

fn to_io<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc2_log::region::Region;

    /// Build a heap-backed cnc page for the bare-`Consensus` harness (no file,
    /// no flock — these tests drive `feed`/`exec` directly).
    fn test_cnc() -> Arc<CncPage> {
        CncPage::heap(&CncMeta {
            node_id: 1,
            instance_id: 0,
            app_id: "test".into(),
            buffer_bytes: 1 << 16,
            max_payload: 4096,
        })
    }

    /// Live-channel ends the test must keep alive so the `Consensus`'s owned
    /// senders/receivers don't disconnect while we drive `feed` directly.
    struct Harness {
        cons: Consensus,
        /// M8 Task 12: the producer half of the handshake route the receiver
        /// agent would own in a real node — lets a test inject a handshake
        /// datagram exactly as `crypto_admit` would deliver one.
        hs_tx: mpsc::SyncSender<HandshakeDatagram>,
        // Kept alive: dropping these would disconnect the consensus's endpoints.
        _net_tx: mpsc::SyncSender<NetEvent>,
        _obs_tx: mpsc::SyncSender<(u32, u64)>,
        _cfg_obs_tx: mpsc::SyncSender<(u64, Vec<u8>)>,
        _ingress_tx: mpsc::SyncSender<Vec<u8>>,
        _trunc_rx: mpsc::Receiver<ArchiveCmd>,
        _dir: tempfile::TempDir,
    }

    impl Harness {
        fn gate_open(&self) -> bool {
            self.cons.intake_gate.load(Ordering::Acquire)
        }

        /// Adopt `term` (higher-term RequestVote), then deliver a divergent term
        /// map that reconciles to a `Truncate` in flight. Drains the archive
        /// command so the channel stays clear.
        fn adopt_and_truncate(&mut self, term: u32, entries: Vec<(u32, u64)>) {
            self.cons.feed(Event::RequestVote {
                from: 0,
                new_term: term,
                last_term: 1,
                last_durable: 7000,
            });
            self.cons.feed(Event::TermMapReceived { term, entries });
            // Consume the archive command (the real archive agent would).
            let _ = self._trunc_rx.try_recv();
        }

        /// Issue #6: stand in for the archive agent's `ArchiveCmd::Collapse`
        /// arm (drain the command, cut+prime, ack) plus the consensus duty
        /// cycle that drains the collapse slot. This is what finishes a leader
        /// open now that the cut runs on the archive thread.
        fn complete_leader_open(&mut self) {
            let cmd = self._trunc_rx.try_recv().expect("leader open commanded a collapse");
            let ArchiveCmd::Collapse { epoch, to } = cmd else {
                panic!("expected Collapse, got {cmd:?}");
            };
            // What the archive agent does, minus the parts this harness has no
            // `Archive` for: the real arm calls `truncate_to(to)` first (so its
            // `PositionPurged` path is NOT covered here — see the arm's own
            // comment) and afterwards publishes `prime_generation` and
            // `first_base`, neither of which the consensus path reads.
            self.cons.cnc.counters().prime(to);
            self.cons.collapse_slot.post(epoch, to);
            let (e, t) = self.cons.collapse_slot.take().expect("the ack was just posted");
            self.cons.on_collapsed(e, t);
        }

        /// Simulate the archive completing the truncation and the consensus
        /// duty-cycle draining the infallible slot.
        fn post_ack_and_drain(&mut self, epoch: u64, to: u64) {
            self.cons.trunc_slot.post(epoch, to);
            if let Some((e, t)) = self.cons.trunc_slot.take() {
                self.cons.on_truncated(e, t);
            }
        }
    }

    /// Build a bare `Consensus` (no spawned agents) wired to real state + sockets
    /// so `feed`/`exec` run their true side effects (vote persist, gate flips,
    /// `trunc_tx` send). The SM is seeded as a healed ex-leader whose durable tail
    /// diverges from the current leader's map, so a term-map delivery truncates.
    fn harness() -> Harness {
        harness_with_crypto(None, &[])
    }

    /// As [`harness`], but with the crypto plane wired exactly as
    /// `Node::start_with_socket` wires it (M8 Task 12). `peer_override`
    /// replaces one member's address with a real, bound socket so a genuine
    /// handshake can be driven against this node.
    fn harness_with_crypto(
        crypto: Option<SharedTransport>,
        peer_override: &[(NodeId, SocketAddr)],
    ) -> Harness {
        // Reproduce the REAL gap between the two `Instant` origins (T12
        // review, M4). In `Node::start_with_socket` the `SharedTransport`'s
        // base is taken as the very first statement and `Consensus::base` only
        // after `Archive::open` (a journal segment scan plus recovery),
        // `rederive_term_map` (an archive replay), `NodeState::open`,
        // `CncPage::create_file`, the log-buffer mmap and every ring — on a
        // node with a large journal, comfortably seconds apart. A harness that
        // took both origins microseconds apart would make the one-clock rule
        // structurally untestable, which is exactly how the first round left
        // this mutant alive with a story instead of a test.
        if crypto.is_some() {
            std::thread::sleep(std::time::Duration::from_nanos(HARNESS_CRYPTO_CLOCK_GAP_NS));
        }
        let dir = tempfile::tempdir().unwrap();
        let cnc = test_cnc();
        let buffer = Arc::new(LogBuffer::new(
            Region::heap_zeroed(1 << 16),
            Arc::clone(&cnc),
            4096,
        ));
        // 6016 = 188 * 32: stream positions are always FRAME-aligned in the
        // real system, and a leader-election test appends the NewTerm frame at
        // this base (a misaligned base trips the buffer's alignment asserts).
        cnc.counters().prime(6016);
        let state = NodeState::open(dir.path()).unwrap();

        // id=1 in [0,1,2]; own map (1,0),(2,4096) at durable 6016 → boot_term 2.
        let members = [0u32, 1, 2];
        let mut id_to_addr = HashMap::new();
        let mut addr_to_id = HashMap::new();
        for (i, id) in members.iter().enumerate() {
            let addr: SocketAddr = match peer_override.iter().find(|(oid, _)| oid == id) {
                Some((_, oaddr)) => *oaddr,
                None => format!("127.0.0.1:{}", 9100 + i).parse().unwrap(),
            };
            id_to_addr.insert(*id, addr);
            addr_to_id.insert(addr, *id);
        }
        let peers = vec![0u32, 2];

        // M7: genesis config over the same static membership (no reconfiguration
        // in this harness's tests — pure migration off the old `members`/
        // `can_vote` `ElectionConfig` fields).
        let config = ClusterConfig::genesis(
            members.iter().map(|id| (*id, addr_to_pair(id_to_addr[id]))).collect(),
            Vec::new(),
        );
        // NOTE: the test harness deliberately does NOT wire mutation knobs onto
        // this ElectionSm. Mutations are exercised only via the production
        // construction path (elle_v2.rs drives real nodes). Wiring them here
        // would let a stray UC2_MUTATION silently corrupt the 16 harness-based
        // unit tests. Production wiring lives at the `ElectionSm::new` above.
        let sm = ElectionSm::new(
            ElectionConfig {
                id: 1,
                config,
                config_position: 0,
                election_timeout_min_ns: 150,
                election_timeout_max_ns: 300,
                gossip_floor_ns: u64::MAX,
                seed: 7,
            },
            None,
            &[(1, 0), (2, 4096)],
            6016,
            0,
        );
        let boot_term = sm.current_term();
        assert_eq!(boot_term, 2);

        let (net_tx, net_rx) = mpsc::sync_channel::<NetEvent>(64);
        let (obs_tx, obs_rx) = mpsc::sync_channel::<(u32, u64)>(64);
        let (cfg_obs_tx, cfg_obs_rx) = mpsc::sync_channel::<(u64, Vec<u8>)>(64);
        let (ingress_tx, ingress_rx) = mpsc::sync_channel::<Vec<u8>>(64);
        let (trunc_tx, trunc_rx) = mpsc::sync_channel::<ArchiveCmd>(64);
        let trunc_slot = TruncationSlot::default();
        let collapse_slot = TruncationSlot::default();
        // Not asserted on by any test in this module — a dropped receiver just
        // makes `sender_ctrl.send` return an ignored `Err` (`exec`'s `let _ =`).
        let (sender_ctrl, _sender_ctrl_rx) = mpsc::sync_channel::<CtrlMsg>(64);
        let (hs_tx, hs_rx) = mpsc::sync_channel::<HandshakeDatagram>(64);
        let crypto_peer_ids_seed = crypto.as_ref().map(|_| {
            let ids = PeerIds::new();
            ids.store(addr_to_id.iter().map(|(a, i)| (*a, *i)).collect::<Vec<_>>());
            ids
        });

        let sock = FaultSocket::from_socket(UdpSocket::bind("127.0.0.1:0").unwrap()).unwrap();
        let intake_gate = Arc::new(AtomicBool::new(true));

        // Real ring files under the harness tempdir (small — nothing in this
        // module's tests writes real ring traffic through them; that's
        // covered by uc2_node/tests/smoke.rs).
        let (_ingress_producer, ingress_ring) =
            MpscRing::create(&dir.path().join("ingress.ring"), 4096, 1024).unwrap().into_split();
        let egress_node =
            BroadcastRing::create(&dir.path().join("egress_node.broadcast"), 4096, 1024)
                .unwrap()
                .producer();
        let (_query_producer, query_ring) =
            MpscRing::create(&dir.path().join("query.ring"), 4096, 1024).unwrap().into_split();
        let (svc_query, _svc_query_consumer) =
            SpscRing::create(&dir.path().join("svc_query.ring"), 4096, 1024).unwrap().into_split();

        let cons = Consensus {
            reports_unattested: Arc::new(AtomicU64::new(0)),
            validated_frontier: Arc::new(AtomicU64::new(u64::MAX)),
            validated_term: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            obs_frontier: Arc::new(AtomicU64::new(u64::MAX)),
            pending_obs: Vec::new(),
            trace_prov: Arc::new(Mutex::new(("none", 0, 0))),
            trunc_trace: false,
            id: 1,
            sm,
            state,
            cnc: Arc::clone(&cnc),
            buffer,
            appender: None,
            next_corr: 0,
            pending_ingress: None,
            ingress_ring,
            egress_node,
            query_ring,
            svc_query,
            pending_reads: Vec::new(),
            current_round: None,
            next_round_seq: 1,
            next_nonce: 0,
            admission_bytes: 256 * 1024,
            pending_ring_ingress: None,
            sock,
            id_to_addr,
            addr_to_id,
            peers,
            learner_ids: Vec::new(),
            peer_band: Vec::new(),
            peer_reported: HashMap::new(),
            peer_band_published: false,
            net_rx,
            obs_rx,
            cfg_obs_rx,
            ingress_rx,
            trunc_tx,
            trunc_slot,
            collapse_slot,
            pending_leader_open: None,
            next_collapse_epoch: 0,
            sender_ctrl,
            term_handle: Arc::new(AtomicU32::new(boot_term)),
            leader_flag: Arc::new(AtomicBool::new(false)),
            can_serve_flag: Arc::new(AtomicBool::new(false)),
            intake_gate,
            truncations: Arc::new(AtomicU64::new(0)),
            wipes: Arc::new(AtomicU64::new(0)),
            reports_implausible: Arc::new(AtomicU64::new(0)),
            base: Instant::now(),
            durable_seen: 6016,
            adopted_term: boot_term,
            awaiting_reconcile: false,
            pending_truncation: None,
            output_persisted_completed: 0,
            output_progress_last_persist_ns: None,
            purge_policy: PurgePolicy::Disabled,
            archive_first_base: Arc::new(AtomicU64::new(0)),
            snapshot_persisted_floor: 0,
            snapshot_floor_last_persist_ns: None,
            incoming_snapshot: Arc::new(AtomicU64::new(0)),
            incoming_snapshot_config: Arc::new(Mutex::new(Vec::new())),
            adopted_incoming: 0,
            last_leader_map: Vec::new(),
            halt_removed: false,
            last_flags: 0,
            config_bytes: Arc::new(Mutex::new(Vec::new())),
            admin: AdminPolicy::Filesystem,
            // Inert under `Filesystem` (verify_admin returns before reading
            // them), but set to plausible values rather than junk.
            admin_instance_id: 0x1234_5678_9abc_def0_1234_5678_9abc_def0,
            admin_app_id: "harness".to_string(),
            audit: AuditLog::open(dir.path()).unwrap(),
            last_admin_seq: 0,
            pending_admin_fwd: None,
            last_config_reply: None,
            config_proposal_non_member: 0,
            config_proposal_dedup_resend: 0,
            // M8 Task 12. `crypto_maint_ns: 0` makes ONE `do_work` a full
            // maintenance pass — the production 20 ms floor is a hot-path
            // concern, not a behavior under test, and rate-limiting here
            // would only make these tests sleep.
            crypto_peer_ids: crypto_peer_ids_seed,
            crypto,
            crypto_hs_rx: Some(hs_rx),
            crypto_epoch: Arc::new(AtomicU32::new(0)),
            crypto_last_maint_ns: None,
            crypto_maint_ns: 0,
            crypto_last_redeliver_ns: None,
            crypto_committed_config_version: None,
            crypto_peers_dirty: true,
            crypto_hs_key_seal_failures: Arc::new(AtomicU64::new(0)),
            crypto_unresolved_peer: Arc::new(AtomicU64::new(0)),
            crypto_handshake_failures: Arc::new(AtomicU64::new(0)),
            crypto_seal_failures: Arc::new(AtomicU64::new(0)),
            crypto_last_log_ns: 0,
        };

        Harness {
            cons,
            hs_tx,
            _net_tx: net_tx,
            _obs_tx: obs_tx,
            _cfg_obs_tx: cfg_obs_tx,
            _ingress_tx: ingress_tx,
            _trunc_rx: trunc_rx,
            _dir: dir,
        }
    }

    /// C-1 regression: a DUPLICATE term map delivered AFTER `Action::Truncate`
    /// executed but BEFORE the archive's slot ack must leave the intake gate
    /// CLOSED. A leader re-ships its map at kHz, so this duplicate is the common
    /// case, not a rare one. On the duplicate the SM's truncating latch drops the
    /// event with zero actions; without the `pending_truncation.is_none()` guard
    /// the reopen heuristic (no truncate produced + term unchanged) would reopen
    /// the gate mid-truncation, letting the receiver ship an AppendPosition over
    /// the raw divergent durable → phantom commit.
    #[test]
    fn duplicate_term_map_mid_truncation_keeps_gate_closed() {
        let mut h = harness();
        assert!(h.gate_open(), "gate starts open");

        // Adopt term 3 (a higher term): closes the gate, arms reconciliation.
        h.cons.feed(Event::RequestVote {
            from: 0,
            new_term: 3,
            last_term: 1,
            last_durable: 7000,
        });
        assert!(!h.gate_open(), "adopting a new term closes the gate");
        assert!(h.cons.awaiting_reconcile);

        // Term-map #1: reconciles to a divergent tail → Action::Truncate. The node
        // persists the pruned map, records `pending_truncation`, closes the gate,
        // and commands the archive with `(epoch, to)`.
        h.cons.feed(Event::TermMapReceived { term: 3, entries: vec![(1, 0), (3, 4096)] });
        assert!(!h.gate_open(), "gate stays closed while the truncate is emitted");
        let epoch = h.cons.pending_truncation.expect("a truncation is now in flight");
        // The truncate command reached the archive channel with its epoch.
        assert_eq!(h._trunc_rx.try_recv().ok(), Some(ArchiveCmd::Truncate { epoch, to: 4096 }));

        // Term-map #2: the leader re-ships the SAME map while the archive
        // truncation is still in flight. The SM's truncating latch drops it with
        // zero actions. The gate MUST remain closed (C-1 guard).
        h.cons.feed(Event::TermMapReceived { term: 3, entries: vec![(1, 0), (3, 4096)] });
        assert!(
            !h.gate_open(),
            "a duplicate term map mid-truncation must NOT reopen the intake gate"
        );
        assert!(h.cons.pending_truncation.is_some(), "still truncating");

        // Only the archive's slot ack reopens the gate (reconciliation done).
        h.post_ack_and_drain(epoch, 4096);
        assert!(h.gate_open(), "the Truncated ack completes reconciliation and reopens");
        assert!(h.cons.pending_truncation.is_none());
    }

    /// T7 review finding 1: `last_config_reply` (leader-side nonce dedup) and
    /// `pending_admin_fwd` (follower-side in-flight forward) must not survive a
    /// role transition — a stale entry would let a duplicated kind-16 datagram
    /// replay an answer that belongs to a since-reverted world. Adopting a
    /// higher term as a follower (`Action::BecomeFollower`) must clear both,
    /// and the cleared forward must get an explicit status=2 (retry) admin
    /// reply rather than leaving its caller to a bare timeout.
    #[test]
    fn become_follower_invalidates_stale_admin_caches() {
        let mut h = harness();
        h.cons.last_config_reply =
            Some((42, ConfigReplyBody { nonce: 42, status: 0, reason: 0, version: 5 }));
        h.cons.pending_admin_fwd = Some(PendingAdminFwd {
            seq: 99,
            nonce: 42,
            actor: None,
            op: 1,
            id: 7,
            ip: 0,
            port: 0,
        });

        // Adopt a higher term as a follower -> Action::BecomeFollower.
        h.cons.feed(Event::RequestVote { from: 0, new_term: 3, last_term: 1, last_durable: 7000 });

        assert!(h.cons.last_config_reply.is_none(), "nonce-dedup cache must be cleared");
        assert!(h.cons.pending_admin_fwd.is_none(), "in-flight forward must be cleared");
        let resp = h.cons.cnc.read_admin_resp(99).expect("superseded forward gets an answer");
        assert_eq!(resp.status, 2, "cleared forward is answered with retry, not silence");
    }

    /// M6 Task 8: a `NoCommonPrefix` reconcile drives a WIPE-AND-REJOIN end to end
    /// through a real `Consensus`. The divergence predates the leader's shipped
    /// window (its earliest entry begins above our first byte — the purged-prefix
    /// case), so the SM emits a truncate-to-0 with an empty map plus a `CountWipe`
    /// tag. The node runs the true side effects: persists the empty map, closes the
    /// intake gate, commands the archive `Truncate { to: 0 }`, and counts the wipe
    /// distinctly from an ordinary truncate. The archive ack then reopens the gate.
    #[test]
    fn no_common_prefix_wipes_the_node_and_rejoins_empty() {
        let mut h = harness();
        assert_eq!(h.cons.wipes.load(Ordering::Relaxed), 0);

        // Adopt a far-higher term (closes the gate, arms reconciliation).
        h.cons.feed(Event::RequestVote { from: 0, new_term: 41, last_term: 40, last_durable: 9000 });
        assert!(!h.gate_open());

        // A leader map whose earliest shipped entry begins at 1<<20 — its window
        // slid past our first byte (base 0). own=[(1,0),(2,4096)] shares no prefix
        // ⇒ NoCommonPrefix ⇒ wipe.
        h.cons.feed(Event::TermMapReceived { term: 41, entries: vec![(40, 1 << 20), (41, 2 << 20)] });

        // The wipe was counted (distinct from a truncate) and the archive was
        // commanded to truncate to 0 (a full wipe), under an in-flight epoch.
        assert_eq!(h.cons.wipes.load(Ordering::Relaxed), 1, "wipe counted");
        let epoch = h.cons.pending_truncation.expect("wipe truncation in flight");
        assert_eq!(
            h._trunc_rx.try_recv().ok(),
            Some(ArchiveCmd::Truncate { epoch, to: 0 }),
            "a wipe is a truncate-to-0"
        );
        assert!(!h.gate_open(), "gate closed across the wipe");

        // The archive ack completes the wipe: the gate reopens (empty follower,
        // ready to refill from the live stream / snapshot session) and the
        // truncation is counted.
        h.post_ack_and_drain(epoch, 0);
        assert!(h.gate_open(), "the Truncated ack reopens intake after the wipe");
        assert_eq!(h.cons.pending_truncation, None);
        assert_eq!(h.cons.truncations.load(Ordering::Relaxed), 1, "a wipe is also a truncate");
    }

    /// M5 residual carry: a matching-epoch ack for a truncation whose adopted term
    /// was SUPERSEDED mid-flight must NOT reopen the gate (nor persist a stale
    /// map). The pruned map was already persisted at `Action::Truncate` time
    /// (persist-before-truncate), and the newer term's own reconcile must complete
    /// before intake reopens.
    #[test]
    fn stale_ack_after_adoption_does_not_reopen_gate_or_persist_stale_map() {
        let mut h = harness();
        // adopt term 3, receive divergent map → Truncate (epoch e1) in flight
        h.adopt_and_truncate(3, vec![(1, 0), (3, 4096)]);
        // higher term 4 adopted mid-truncation
        h.cons.feed(Event::RequestVote { from: 0, new_term: 4, last_term: 3, last_durable: 9000 });
        assert!(!h.gate_open());
        // the e1 ack arrives (archive finished the OLD truncation)
        h.post_ack_and_drain(/*epoch*/ 1, /*to*/ 4096);
        assert!(!h.gate_open(), "gate must stay closed: term 4 not yet reconciled");
        // clean reconcile in term 4 reopens
        h.cons.feed(Event::TermMapReceived { term: 4, entries: vec![(1, 0), (4, 4096)] });
        assert!(h.gate_open());
    }

    /// M4 I-1 carry, ported guard: a CURRENT-term follower Report claiming a
    /// durable beyond our own append is provably corrupt (no wire CRC — one
    /// bit-flip can inflate it) and must be DROPPED WHOLE at the node's
    /// `feed_net`, never ranked and never clamped — clamping would rank
    /// {own, own, 0} at the quorum slot and manufacture a phantom commit on
    /// leader-only durability. The drop poisons nothing (the tracker slot is
    /// monotonic-max: a later legitimate report still ranks), and the guard is
    /// term-scoped: a HIGHER-term report must still reach the SM to trigger
    /// adoption. This guard lived in the legacy M3 sender arm deleted by M4
    /// carry #5; this test pins its node-mode port (the SM cannot host it —
    /// it never sees the append counter).
    #[test]
    fn implausible_current_term_report_is_dropped_not_ranked() {
        let mut h = harness();
        let addr0: SocketAddr = "127.0.0.1:9100".parse().unwrap(); // member 0

        // Drive the harness node (id 1, boot term 2, durable/append primed at
        // 6016) to LEADER: election timeout fires -> candidate term 3; one
        // peer grant (2 of 3 with the self-vote) -> BecomeLeader, which appends
        // the 32 B NewTerm frame at base 6016 (append -> 6048).
        h.cons.feed(Event::Tick { now_ns: 301 });
        h.cons.feed(Event::Vote { from: 0, term: 3, granted: true });
        assert_eq!(h.cons.sm.current_term(), 3);
        h.complete_leader_open(); // issue #6: the open lands on the archive's ack
        assert!(h.cons.leader_flag.load(Ordering::Acquire), "election did not complete");
        let append = h.cons.cnc.counters().append.load_acquire();
        assert_eq!(append, 6048, "NewTerm frame must sit at [6016, 6048)");
        assert_eq!(h.cons.cnc.counters().commit.load_acquire(), 0);
        // Own archive covers the full append (feeds the tracker's own-durable).
        h.cons.feed(Event::DurableAdvanced { durable: append });

        // A forged/corrupt CURRENT-term report far beyond our append. Unguarded
        // it would rank {own=6048, 2^40, 0} -> 2nd highest = 2^40 -> bounded by
        // own = 6048 -> a PHANTOM commit of the whole log on leader-only
        // durability. Guarded: dropped whole + counted, commit stays 0.
        {
            let dt = h.cons.sm.term_at(1 << 40);
            h.cons.feed_net(NetEvent::Report { from: addr0, term: 3, durable: 1 << 40, durable_term: dt });
        }
        assert_eq!(
            h.cons.cnc.counters().commit.load_acquire(),
            0,
            "implausible report manufactured a phantom commit on leader-only durability"
        );
        assert_eq!(h.cons.reports_implausible.load(Ordering::Relaxed), 1, "drop must be counted");

        // The drop poisoned nothing: a legitimate report (durable == append)
        // from the same follower ranks normally -> quorum {6048, 6048, 0} ->
        // commit advances to 6048.
        {
            let dt = h.cons.sm.term_at(append);
            h.cons.feed_net(NetEvent::Report { from: addr0, term: 3, durable: append, durable_term: dt });
        }
        assert_eq!(
            h.cons.cnc.counters().commit.load_acquire(),
            append,
            "legitimate report after a dropped one must still rank (slot not poisoned)"
        );
        assert_eq!(h.cons.reports_implausible.load(Ordering::Relaxed), 1);

        // Term-scoping is load-bearing: a HIGHER-term report — even one with an
        // absurd durable — must NOT be eaten by the guard; it reaches the SM
        // and triggers term adoption (the legitimate follower-leads-our-append
        // case arrives via term machinery, never inside a static term).
        {
            let dt = h.cons.sm.term_at(1 << 40);
            h.cons.feed_net(NetEvent::Report { from: addr0, term: 7, durable: 1 << 40, durable_term: dt });
        }
        assert_eq!(
            h.cons.sm.current_term(),
            7,
            "higher-term report must reach the SM and adopt the term"
        );
        assert_eq!(h.cons.reports_implausible.load(Ordering::Relaxed), 1, "adoption not counted");
    }

    /// Task 7: the ingress admission door's pure decision function. Exercised
    /// directly (rather than via a live single-node commit race, which can't
    /// hold `commit` still to force the window shut) per the brief's Step 4.
    #[test]
    fn admission_guard_math() {
        // No backlog: wide open.
        assert!(admission_open(100, 100, 4096));
        // Exactly at budget: still open (the check is `<=`).
        assert!(admission_open(4096, 0, 4096));
        // One byte over budget: closed.
        assert!(!admission_open(4097, 0, 4096));
        // A much larger backlog: closed.
        assert!(!admission_open(1 << 20, 0, 4096));
        // Commit observed (transiently) ahead of append: `saturating_sub`
        // must not underflow/panic, and the door stays open.
        assert!(admission_open(0, 100, 4096));
        // Zero budget: only a perfectly caught-up door is open.
        assert!(admission_open(50, 50, 0));
        assert!(!admission_open(51, 50, 0));
    }

    /// Issue #6 regression: the leader-open collapse must be COMMANDED to the
    /// archive agent, never primed on the consensus thread. Priming here left
    /// the archive's private `durable_pos` above `base` whenever it had fsynced
    /// a block since the (stale) `ElectionSm::durable` sample `base` comes from
    /// — and the new leader then rewrote the buffer under its cursor
    /// (`RecorderCorrupt`; see `uc2_log`'s
    /// `become_leader_collapse_below_archive_cursor_corrupts_the_walk`).
    #[test]
    fn leader_open_routes_the_collapse_through_the_archive() {
        let mut h = harness();
        let before = h.cons.cnc.counters().append.load_acquire();
        assert_eq!(before, 6016, "boot frontier");

        h.cons.feed(Event::Tick { now_ns: 301 });
        h.cons.feed(Event::Vote { from: 0, term: 3, granted: true });

        // Phase 1: the SM leads, but NOTHING has touched the buffer yet — no
        // prime, no appender, no NewTerm frame, intake closed, not serving.
        assert_eq!(h.cons.sm.current_term(), 3);
        assert_eq!(
            h.cons.cnc.counters().append.load_acquire(),
            before,
            "the consensus thread must not prime or append before the archive cut"
        );
        assert!(h.cons.appender.is_none(), "no appender before the collapse ack");
        assert!(!h.cons.leader_flag.load(Ordering::Acquire));
        assert!(!h.gate_open(), "intake stays closed across the collapse");

        // The command the archive agent must receive.
        let cmd = h._trunc_rx.try_recv().expect("a collapse was commanded");
        let ArchiveCmd::Collapse { epoch, to } = cmd else { panic!("expected Collapse: {cmd:?}") };
        assert_eq!(to, 6016, "collapse to `base` = the SM's durable");
        assert_eq!(h.cons.pending_leader_open, Some(PendingLeaderOpen { epoch, term: 3, base: to }));

        // Phase 2: the archive cut+primed and acked. Now the open completes.
        h.cons.cnc.counters().prime(to);
        h.cons.collapse_slot.post(epoch, to);
        let (e, t) = h.cons.collapse_slot.take().unwrap();
        h.cons.on_collapsed(e, t);
        assert!(h.cons.appender.is_some());
        assert!(h.cons.leader_flag.load(Ordering::Acquire));
        assert!(h.gate_open());
        assert_eq!(
            h.cons.cnc.counters().append.load_acquire(),
            6048,
            "NewTerm frame appended only after the cut"
        );
        assert_eq!(h.cons.pending_leader_open, None);
    }

    /// Issue #6: a collapse ack that lands AFTER the node stepped down (or
    /// adopted a higher term) must not resurrect the abandoned leadership. The
    /// physical cut is still correct — it dropped only this node's own
    /// unreplicated tail — but no appender may be installed for a dead term.
    #[test]
    fn a_collapse_ack_after_stepping_down_does_not_resurrect_leadership() {
        let mut h = harness();
        h.cons.feed(Event::Tick { now_ns: 301 });
        h.cons.feed(Event::Vote { from: 0, term: 3, granted: true });
        let cmd = h._trunc_rx.try_recv().expect("a collapse was commanded");
        let ArchiveCmd::Collapse { epoch, to } = cmd else { panic!("expected Collapse: {cmd:?}") };

        // A higher term arrives while the collapse is in flight.
        h.cons.feed(Event::RequestVote { from: 0, new_term: 9, last_term: 3, last_durable: 9000 });
        assert!(h.cons.pending_leader_open.is_none(), "the open was abandoned");

        // The late ack lands. It must be inert.
        h.cons.cnc.counters().prime(to);
        h.cons.on_collapsed(epoch, to);
        assert!(h.cons.appender.is_none(), "no appender for the abandoned term");
        assert!(!h.cons.leader_flag.load(Ordering::Acquire), "not a leader");
        assert_eq!(
            h.cons.cnc.counters().append.load_acquire(),
            to,
            "no NewTerm frame was appended"
        );
    }

    /// REGRESSION (issue #6 follow-up): a node must not grant a vote by
    /// comparing the candidate against a STALE self-view of its own log.
    ///
    /// The shared `durable` counter has two independent readers on two threads.
    /// The RECEIVER agent reads it and reports it to the leader, which ranks
    /// those reports into `commit` (`uc2_net/src/receiver.rs`, the
    /// `DGRAM_KIND_APPEND_POSITION` send reads `counters().durable` directly).
    /// The CONSENSUS agent polls the same counter in `do_work` step 2 and feeds
    /// `DurableAdvanced` into `ElectionSm.durable` — the field `log_ok` compares
    /// against. Step 1 drains network events (including `RequestVote`) BEFORE
    /// step 2 refreshes that field, so a vote decision can use a self-view a
    /// full duty cycle behind what this node already reported for commit.
    ///
    /// Granting on an UNDER-estimate of our own log is the unsafe direction: it
    /// lets a candidate that is behind a committed position collect our vote,
    /// win, and collapse the log below that commit — acked-write loss.
    ///
    /// Driven through `feed_net`, not `feed`, because the fix is the fresh
    /// absorb on the network path; a test that fed `Event::RequestVote` directly
    /// would bypass it and pass either way.
    #[test]
    fn a_vote_is_refused_against_a_fresh_read_of_our_own_log() {
        let mut h = harness();
        let addr0: SocketAddr = "127.0.0.1:9100".parse().unwrap(); // member 0
        // Harness seed: term map [(1,0),(2,4096)], durable 6016 — the SM's
        // `(our_term, our_durable)` is `(2, 6016)`.
        let boot = 6016;
        // The archive fsyncs another block: the COUNTER moves. The receiver agent
        // reports 10112 to the leader on its own thread, and the leader may rank
        // it into `commit`. The consensus agent has not absorbed it yet.
        let fsynced = boot + 4096;
        h.cons.cnc.counters().durable.store_release(fsynced);

        // A candidate level with our STALE view and 4096 bytes behind our real
        // one. Pre-fix this was GRANTED (a tie under `log_ok_order`'s `>=`).
        h.cons.feed_net(NetEvent::RequestVote {
            from: addr0,
            body: RequestVoteBody { new_term: 9, last_term: 2, last_durable: boot },
        });
        assert_ne!(
            h.cons.state.vote().map(|v| (v.term, v.voted_for)),
            Some((9, 0)),
            "granted to a candidate at {boot} while our own durable counter stood at \
             {fsynced} — it could win and collapse below a commit our report certified"
        );

        // Nothing is broken about granting per se: a candidate that really is
        // caught up still gets the vote.
        h.cons.feed_net(NetEvent::RequestVote {
            from: addr0,
            body: RequestVoteBody { new_term: 10, last_term: 2, last_durable: fsynced },
        });
        assert_eq!(
            h.cons.state.vote().map(|v| (v.term, v.voted_for)),
            Some((10, 0)),
            "a candidate level with our REAL durable must still be granted"
        );
    }

    /// Issue #6: while a leader open is in flight the duty cycle must NOT feed
    /// `DurableAdvanced` from the archive's still-uncollapsed frontier.
    ///
    /// `ElectionSm::durable` is a monotonic max, and the collapse exists exactly
    /// because that frontier is ABOVE the `base` being collapsed to. Feeding it
    /// would latch `sm.durable` above bytes about to be cut away, with no path
    /// back down (the `min` clamp belongs to the reconcile path's
    /// `Event::Truncated`, which a collapse does not produce). The SM would then
    /// ship an inflated `last_durable` vote credential and — worse — advance the
    /// commit tracker with an `own_durable` this node does not physically hold.
    ///
    /// This window is a consequence of splitting leader open across two cycles:
    /// before issue #6 the prime was synchronous, so step 2 always read the
    /// already-collapsed value.
    #[test]
    fn a_pending_leader_open_suppresses_durable_feeds_from_the_stale_frontier() {
        let mut h = harness();
        h.cons.feed(Event::Tick { now_ns: 301 });
        h.cons.feed(Event::Vote { from: 0, term: 3, granted: true });
        let open = h.cons.pending_leader_open.expect("leader open in flight");
        let sm_durable_at_open = h.cons.sm.durable();
        assert_eq!(open.base, sm_durable_at_open, "base IS the SM's durable");

        // The archive has NOT processed the collapse yet, and publishes a
        // frontier above `base` — precisely the race this fix is about.
        let stale_frontier = open.base + 4096;
        h.cons.cnc.counters().durable.store_release(stale_frontier);
        h.cons.do_work();
        assert_eq!(
            h.cons.sm.durable(),
            sm_durable_at_open,
            "the SM must not latch a frontier that is about to be cut away"
        );

        // After the collapse lands, feeds resume from the real frontier.
        h.complete_leader_open();
        let after = h.cons.cnc.counters().append.load_acquire();
        h.cons.cnc.counters().durable.store_release(after);
        h.cons.do_work();
        assert_eq!(h.cons.sm.durable(), after, "feeds resume once the open completes");
    }

    /// Issue #6, load-bearing NEGATIVE result: a reconcile `Truncate` and a
    /// leader-open `Collapse` are never in flight together, because a node that
    /// is reconcile-truncating cannot win an election.
    ///
    /// Two independent reasons, both in `ElectionSm`: becoming a candidate needs
    /// `Event::Tick`, which the truncating latch's allow-list
    /// (`{RequestVote, Vote, Truncated}`) drops; and reaching
    /// `reconcile_term_map` at all means adopting a map, which for a strictly
    /// higher term runs `BecomeFollower` first, while a SAME-term map arriving
    /// at a candidate would mean two leaders in one term.
    ///
    /// This matters because `ElectionSm::durable` is clamped to a pending cut
    /// only when that cut's `Truncated` ack is fed back — so IF the interleaving
    /// were reachable, `BecomeLeader` would carry `base > to` and the archive
    /// would answer `PositionPurged`. The `ArchiveCmd::Collapse` arm clamps and
    /// `on_truncated` carries a `pending_leader_open.is_none()` predicate so
    /// that case degrades to subsumption instead of killing the archive agent —
    /// but both are DEAD DEFENCE as long as this test holds. If it ever fails,
    /// that defence has become live and needs real coverage.
    #[test]
    fn a_reconcile_truncating_node_cannot_also_open_a_leader_term() {
        let mut h = harness();
        // Adopt term 4 with a divergent map -> `Action::Truncate` in flight.
        h.cons.feed(Event::RequestVote { from: 0, new_term: 4, last_term: 1, last_durable: 7000 });
        h.cons.feed(Event::TermMapReceived { term: 4, entries: vec![(1, 0), (4, 4096)] });
        let trunc = h._trunc_rx.try_recv().expect("a reconcile truncate was commanded");
        assert!(matches!(trunc, ArchiveCmd::Truncate { .. }), "got {trunc:?}");
        assert!(h.cons.pending_truncation.is_some(), "truncation bracket open");
        assert!(!h.gate_open(), "intake closed for the truncation");

        // Adopting the map made us a FOLLOWER, so a grant cannot elect us; and a
        // `Tick` cannot make us a candidate while the latch holds.
        h.cons.feed(Event::Tick { now_ns: 10_000_000_000 });
        h.cons.feed(Event::Vote { from: 0, term: 4, granted: true });
        assert!(
            h.cons.pending_leader_open.is_none(),
            "a truncating node opened a leader term — the Collapse clamp and \
             on_truncated's pending_leader_open predicate are now LIVE and need \
             real coverage, not just the defensive comments they carry"
        );
        assert!(h._trunc_rx.try_recv().is_err(), "no second archive command");
    }

    /// Drive the harness node (id 1, boot term 2) to a SERVING leader of term 3:
    /// election timeout → candidate; one peer grant → BecomeLeader (NewTerm frame
    /// at [6016, 6048)); then advance commit past the frame via a follower's
    /// durable Report, which opens `can_serve`. Returns the append frontier 6048.
    fn drive_to_serving_leader(h: &mut Harness) -> u64 {
        h.cons.feed(Event::Tick { now_ns: 301 });
        h.cons.feed(Event::Vote { from: 0, term: 3, granted: true });
        // Issue #6: the open now completes on the archive's collapse ack.
        assert!(!h.cons.leader_flag.load(Ordering::Acquire), "leading before the collapse landed");
        h.complete_leader_open();
        assert!(h.cons.leader_flag.load(Ordering::Acquire), "election did not complete");
        let append = h.cons.cnc.counters().append.load_acquire();
        assert_eq!(append, 6048);
        h.cons.feed(Event::DurableAdvanced { durable: append });
        // Member 0's address as THIS harness knows it — `harness_with_crypto`
        // may have replaced it with a real bound socket, and a Report from an
        // address the node cannot resolve is (correctly) ignored.
        let addr0 = h.cons.id_to_addr[&0];
        {
            let dt = h.cons.sm.term_at(append);
            h.cons.feed_net(NetEvent::Report { from: addr0, term: 3, durable: append, durable_term: dt });
        }
        assert!(h.cons.sm.can_serve(), "commit did not open the serving gate");
        append
    }

    /// Push a linearizable read into the barrier for the harness node.
    fn mk_read(commit_at: u64, round_seq: u64, deadline_ns: u64) -> PendingRead {
        PendingRead {
            client_id: 7,
            local_seq: 1,
            query: Vec::new(),
            round_seq,
            commit_at,
            deadline_ns,
            phase: ReadPhase::AwaitQuorum,
        }
    }

    /// Task 11 barrier: READ_PROBE_ACKs count DISTINCT ackers (a duplicate from
    /// the same node does not advance), a non-member ack is ignored, quorum flips
    /// the read to `AwaitApplied`, and the capture-recheck bracket forwards to the
    /// service ONLY once `service_applied >= commit_at` (with the epoch stable).
    #[test]
    fn barrier_counts_distinct_ackers_then_forwards_on_service_catchup() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);

        // A read requiring a 3-way quorum, with a read index above the (zero)
        // service_applied so the forward waits for catch-up. The round is
        // constructed directly at quorum 3 (the harness cluster would compute
        // 2) so distinct-acker counting is observable across two peer acks.
        let far = h.cons.now_ns() + 10_000_000_000;
        h.cons.pending_reads.push(mk_read(6048, 1, far));
        let term = h.cons.sm.current_term();
        let now = h.cons.now_ns();
        h.cons.current_round =
            Some(crate::read_round::ProbeRound::new(1, 42, 3, h.cons.id, term, 6048, now));
        h.cons.next_round_seq = 2;

        // A non-member ack (id 99 is not in [0,1,2]) is dropped by the
        // membership check.
        h.cons.on_read_probe_ack(42, 99);
        assert_eq!(h.cons.current_round.as_ref().unwrap().acks(), 1);

        // Distinct acker 0, then a DUPLICATE 0 that must not advance the count.
        h.cons.on_read_probe_ack(42, 0);
        h.cons.on_read_probe_ack(42, 0);
        assert_eq!(h.cons.current_round.as_ref().unwrap().acks(), 2);
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitQuorum);

        // The second distinct acker reaches quorum 3 → the round completes,
        // certifies the waiting read, and is consumed.
        h.cons.on_read_probe_ack(42, 2);
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitApplied);
        assert!(h.cons.current_round.is_none(), "a completed round is consumed");

        // Service not yet caught up (applied 0 < commit_at 6048): parked.
        assert!(!h.cons.advance_pending_reads());
        assert_eq!(h.cons.pending_reads.len(), 1);

        // Caught up BUT service_epoch still 0: the sentinel-collision guard
        // keeps it parked (unchanged behavior).
        h.cons.cnc.service().service_applied.store_release(6048);
        assert!(!h.cons.advance_pending_reads());
        assert_eq!(h.cons.pending_reads.len(), 1, "epoch-0 must not forward");

        // A real incarnation attaches (epoch 1) → forwarded and dropped.
        h.cons.cnc.service().service_epoch.store_release(1);
        assert!(h.cons.advance_pending_reads());
        assert!(h.cons.pending_reads.is_empty(), "caught-up read must forward and drop");
    }

    /// Rung A §3.2, the crux: a round certifies ONLY reads admitted before it
    /// was issued. Read B, admitted mid-round, records the next seq and must
    /// stay AwaitQuorum when round 1 completes — then the follow-up round
    /// covers it.
    #[test]
    fn round_certifies_only_reads_admitted_before_issue() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        let far = h.cons.now_ns() + 10_000_000_000;

        // Read A admitted, then the round is issued (harness quorum: 2).
        h.cons.pending_reads.push(mk_read(6048, h.cons.next_round_seq, far));
        h.cons.maybe_issue_round();
        let round = h.cons.current_round.as_ref().expect("round issued");
        let (seq, nonce) = (round.seq, round.nonce);

        // Read B admitted MID-ROUND: records seq+1.
        h.cons.pending_reads.push(mk_read(6048, h.cons.next_round_seq, far));
        assert_eq!(h.cons.pending_reads[1].round_seq, seq + 1);

        // One peer ack reaches the harness quorum of 2 → round completes.
        h.cons.on_read_probe_ack(nonce, 0);
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitApplied, "A certified");
        assert_eq!(
            h.cons.pending_reads[1].phase,
            ReadPhase::AwaitQuorum,
            "B admitted mid-round must NOT be certified by this round"
        );
        assert!(h.cons.current_round.is_none());

        // The next round (fresh seq) covers B.
        h.cons.maybe_issue_round();
        let nonce2 = h.cons.current_round.as_ref().unwrap().nonce;
        assert_ne!(nonce2, nonce, "a new round, not a retransmit");
        h.cons.on_read_probe_ack(nonce2, 2);
        assert_eq!(h.cons.pending_reads[1].phase, ReadPhase::AwaitApplied);
    }

    /// Rung A §4: a voter-set change (any rebuild_peer_maps) voids the round;
    /// pending reads survive and the next round covers them under a fresh seq.
    #[test]
    fn voter_set_change_voids_round_but_keeps_reads() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        let far = h.cons.now_ns() + 10_000_000_000;
        h.cons.pending_reads.push(mk_read(6048, h.cons.next_round_seq, far));
        h.cons.maybe_issue_round();
        let old_nonce = h.cons.current_round.as_ref().unwrap().nonce;

        // Same-membership rebuild (the trigger is the rebuild itself — the
        // node cannot distinguish "same voters" cheaply and must not try).
        let members = [0u32, 1, 2];
        let config = ClusterConfig::genesis(
            members.iter().map(|id| (*id, addr_to_pair(h.cons.id_to_addr[id]))).collect(),
            Vec::new(),
        );
        h.cons.rebuild_peer_maps(&config);
        assert!(h.cons.current_round.is_none(), "voter-set change voids the round");
        assert_eq!(h.cons.pending_reads.len(), 1, "reads survive the void");
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitQuorum);

        // A straggler ack for the voided round is a no-op.
        h.cons.on_read_probe_ack(old_nonce, 0);
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitQuorum);

        // The next round (fresh seq + nonce, fresh-config quorum) covers it.
        h.cons.maybe_issue_round();
        let round = h.cons.current_round.as_ref().unwrap();
        assert_ne!(round.nonce, old_nonce);
        let nonce2 = round.nonce;
        h.cons.on_read_probe_ack(nonce2, 0);
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitApplied);
    }

    /// Rung A §4: a round stamped with a stale term is abandoned by
    /// advance_pending_reads (driven directly — the harness stamps a
    /// mismatched term rather than running a full re-election).
    #[test]
    fn stale_term_round_is_abandoned() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        let far = h.cons.now_ns() + 10_000_000_000;
        h.cons.pending_reads.push(mk_read(6048, 1, far));
        let stale_term = h.cons.sm.current_term() - 1;
        let now = h.cons.now_ns();
        h.cons.current_round =
            Some(crate::read_round::ProbeRound::new(1, 42, 2, h.cons.id, stale_term, 6048, now));
        h.cons.next_round_seq = 2;

        h.cons.advance_pending_reads();
        // The stale round is gone; the read survived (deadline far away) and a
        // FRESH round was chained in the same call (issue site 2 of 2).
        let round = h.cons.current_round.as_ref().expect("fresh round chained");
        assert_eq!(round.term, h.cons.sm.current_term());
        assert_ne!(round.nonce, 42);
    }

    /// Rung A: a round with no waiting reads is dropped, not waited out.
    #[test]
    fn round_with_no_waiting_reads_is_dropped() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        let term = h.cons.sm.current_term();
        let now = h.cons.now_ns();
        h.cons.current_round =
            Some(crate::read_round::ProbeRound::new(1, 42, 2, h.cons.id, term, 6048, now));
        assert!(!h.cons.advance_pending_reads());
        assert!(h.cons.current_round.is_none());
    }

    /// A legal 2->1 voter shrink strands AwaitQuorum reads (the round is
    /// voided, the reads kept): the sole-voter guard in maybe_issue_round
    /// must certify them directly instead of tripping the quorum>=2 assert
    /// or wedging them until the 1 s deadline.
    #[test]
    fn shrink_to_sole_voter_certifies_stranded_reads() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        let far = h.cons.now_ns() + 10_000_000_000;
        h.cons.pending_reads.push(mk_read(6048, h.cons.next_round_seq, far));
        h.cons.maybe_issue_round();
        assert!(h.cons.current_round.is_some());

        // Adopt a config whose only voter is self (id 1): the round is voided,
        // the read survives...
        let config = ClusterConfig::genesis(
            vec![(1u32, addr_to_pair(h.cons.id_to_addr[&1]))],
            Vec::new(),
        );
        h.cons.rebuild_peer_maps(&config);
        assert!(h.cons.current_round.is_none());
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitQuorum);

        // ...and the next issue attempt certifies it directly, creating no round.
        h.cons.maybe_issue_round();
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitApplied);
        assert!(h.cons.current_round.is_none(), "sole voter needs no round");
    }

    /// Task 11 barrier, sentinel-collision guard (M5 final review IMPORTANT #1):
    /// a linearizable read whose service has caught up (`service_applied >=
    /// commit_at`) must NOT be forwarded while `service_epoch == 0` (a fresh cnc
    /// page has no attached service this generation; epoch 0 is the wire
    /// "skip-the-check" sentinel). It forwards only once a real incarnation bumps
    /// the epoch to >= 1 — with that true epoch, so the service's own stale-epoch
    /// refusal still applies. (Red without the `e >= 1` guard: the read forwards
    /// on epoch 0, disabling the check.)
    #[test]
    fn barrier_does_not_forward_on_epoch_zero_sentinel() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);

        let far = h.cons.now_ns() + 10_000_000_000;
        // Single-node quorum (1) so the read is AwaitApplied immediately.
        let mut read = mk_read(/*commit_at*/ 6048, /*round_seq*/ 1, far);
        read.phase = ReadPhase::AwaitApplied;
        h.cons.pending_reads.push(read);

        // Service applied past the read index, but service_epoch is still 0
        // (no service attached this generation): the guard parks the read.
        h.cons.cnc.service().service_applied.store_release(6048);
        assert_eq!(h.cons.cnc.service().service_epoch.load_acquire(), 0);
        assert!(!h.cons.advance_pending_reads(), "must not forward on the epoch-0 sentinel");
        assert_eq!(h.cons.pending_reads.len(), 1);

        // Epoch bumps to 1 (a real incarnation attached) → forwarded with
        // expected_epoch 1 and dropped from the barrier.
        h.cons.cnc.service().service_epoch.store_release(1);
        assert!(h.cons.advance_pending_reads(), "must forward once epoch >= 1");
        assert!(h.cons.pending_reads.is_empty());
    }

    /// Task 11 barrier: a read past its deadline is answered `MSG_V2_RETRY`
    /// (side-effect-free) and dropped, even while still serving.
    #[test]
    fn barrier_retries_a_read_past_its_deadline() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        // deadline_ns = 0 is already in the past.
        let read = mk_read(0, 1, 0);
        h.cons.pending_reads.push(read);
        assert!(h.cons.advance_pending_reads());
        assert!(h.cons.pending_reads.is_empty(), "past-deadline read must retry + drop");
    }

    /// Task 11 barrier: leadership lost (a follower / non-serving node) retries
    /// every in-flight read immediately — a deposed leader can never certify a
    /// linearizable read.
    #[test]
    fn barrier_retries_all_reads_when_not_serving() {
        let mut h = harness(); // boots a FOLLOWER: can_serve == false
        assert!(!h.cons.sm.can_serve());
        let far = h.cons.now_ns() + 10_000_000_000;
        let read = mk_read(0, 1, far); // deadline far off; only depose fires
        h.cons.pending_reads.push(read);
        assert!(h.cons.advance_pending_reads());
        assert!(h.cons.pending_reads.is_empty(), "a non-serving node retries in-flight reads");
    }

    /// Veil §5 discharge, observation 1 (the parked-reads liveness blemish): a
    /// halt (`HaltRemoved`/`StepDownRemoved`) must answer every parked read
    /// with the standard side-effect-free RETRY and void the round — `do_work`
    /// short-circuits every subsequent cycle, so nothing else can ever reach
    /// their deadline path, and no ack can ever complete the round.
    #[test]
    fn halt_retries_parked_reads_and_voids_the_round() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        let far = h.cons.now_ns() + 10_000_000_000;
        h.cons.pending_reads.push(mk_read(6048, h.cons.next_round_seq, far));
        h.cons.maybe_issue_round();
        assert!(h.cons.current_round.is_some());

        h.cons.halt();
        assert!(h.cons.pending_reads.is_empty(), "parked reads must be RETRYed at halt");
        assert!(h.cons.current_round.is_none(), "nothing can ever complete a halted round");
    }

    /// A read that slips in AFTER `halt()` within the same duty cycle — the
    /// raw `sm.can_serve()` is still true, because step-down never clears the
    /// SM's serving field — must still be swept to RETRY by
    /// `advance_pending_reads`' halt gate before the cycle ends.
    #[test]
    fn read_admitted_after_halt_is_still_retried() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        h.cons.halt();
        assert!(h.cons.sm.can_serve(), "premise: the SM serving field survives halt");
        let far = h.cons.now_ns() + 10_000_000_000;
        h.cons.pending_reads.push(mk_read(6048, h.cons.next_round_seq, far));
        assert!(h.cons.advance_pending_reads());
        assert!(h.cons.pending_reads.is_empty(), "halt gate must RETRY same-cycle admissions");
    }

    // ---- M7 Task 6: persist-revert-BEFORE-truncate (`Action::Truncate` exec) ----

    /// A learner-adding v1 config bump over the harness's genesis, for the
    /// revert-before-truncate tests (mirrors `election::tests::v1_of`).
    fn v1_of(h: &Harness) -> ClusterConfig {
        let mut v1 = h.cons.sm.config().clone();
        v1.learners.push((9, (9, 1)));
        v1.version = 1;
        v1
    }

    /// M7 Task 6, ordinary case: a config adopted ABOVE the truncation target
    /// must have its durable `ConfigRecord` reverted to prev SYNCHRONOUSLY inside
    /// `exec`'s `Action::Truncate` arm — BEFORE the archive even receives the
    /// truncate command (`_trunc_rx` still holds it, unconsumed by any ack). The
    /// SM's own later revert (on the matching-epoch ack) then re-persists the
    /// identical record — an idempotent overwrite, not a second decision.
    #[test]
    fn truncate_exec_reverts_config_record_before_truncate() {
        let mut h = harness();
        let genesis = h.cons.sm.config().clone();
        let v1 = v1_of(&h);
        // Adopt v1 at position 5000 — ABOVE the divergent-map truncation target
        // (4096) the harness's own map/leader-map mismatch always produces.
        h.cons.feed(Event::ConfigObserved { position: 5000, config: v1.clone() });
        let rec = h.cons.state.config_record().expect("v1 persisted");
        assert_eq!(rec.position, 5000);
        assert_eq!(rec.config.version, 1);

        // Divergent term map -> Action::Truncate{to: 4096, ..} — strictly below
        // 5000, so the config frame backing v1 is removed by the cut.
        h.adopt_and_truncate(3, vec![(1, 0), (3, 4096)]);
        let epoch = h.cons.pending_truncation.expect("a truncation is in flight");

        // The record is ALREADY reverted — before any archive ack.
        let rec = h.cons.state.config_record().expect("record still present");
        assert_eq!(rec.position, 0, "reverted to prev's position");
        assert_eq!(rec.config.version, 0, "reverted to the genesis config");
        assert_eq!(rec.prev_position, 0);
        assert_eq!(rec.prev.version, 0, "prev duplicated (history exhausted)");
        assert_eq!(stored_to_cluster(&rec.config), genesis);

        // The SM's own matching-epoch revert then re-emits ConfigAdopted, whose
        // persist is an idempotent overwrite of the same reverted record.
        h.post_ack_and_drain(epoch, 4096);
        let rec = h.cons.state.config_record().expect("record still present");
        assert_eq!(rec.position, 0);
        assert_eq!(rec.config.version, 0);
    }

    /// M7 Task 6, wipe case (`to == 0`): the durable record resets to position 0
    /// but keeps the CURRENT operational config (config-by-fiat) rather than
    /// dropping to a genuine predecessor — a wiped node has no further use for
    /// history below a config it refills live from the leader's stream.
    #[test]
    fn truncate_exec_wipe_persists_fiat_record_before_truncate() {
        let mut h = harness();
        let v1 = v1_of(&h);
        h.cons.feed(Event::ConfigObserved { position: 3000, config: v1.clone() });
        assert_eq!(h.cons.state.config_record().unwrap().position, 3000);

        // NoCommonPrefix: the leader's shipped window begins at 1<<20, far past
        // our own map — wipe-and-rejoin, Truncate{to: 0}.
        h.cons.feed(Event::RequestVote { from: 0, new_term: 41, last_term: 40, last_durable: 9000 });
        h.cons.feed(Event::TermMapReceived {
            term: 41,
            entries: vec![(40, 1 << 20), (41, 2 << 20)],
        });
        let epoch = h.cons.pending_truncation.expect("wipe truncation in flight");

        // Fiat record already persisted before any archive ack: position 0,
        // config == prev == the CURRENT (v1) config, not genesis.
        let rec = h.cons.state.config_record().expect("record still present");
        assert_eq!(rec.position, 0);
        assert_eq!(rec.prev_position, 0);
        assert_eq!(stored_to_cluster(&rec.config), v1, "fiat keeps the CURRENT config");
        assert_eq!(rec.config, rec.prev, "prev duplicated (fiat, not a genuine predecessor)");

        h.post_ack_and_drain(epoch, 0);
        let rec = h.cons.state.config_record().unwrap();
        assert_eq!(stored_to_cluster(&rec.config), v1, "SM's own wipe-revert re-confirms the fiat");
    }

    /// M7 Task 6: `to == config_record().position` exactly preserves the frame
    /// (frame-END effect point; truncation keeps `[0, to)`) — the guard is a
    /// strict `<`, so no revert fires and the record is untouched by `exec`.
    #[test]
    fn truncate_exec_at_config_position_leaves_record_untouched() {
        let mut h = harness();
        let v1 = v1_of(&h);
        // Adopt v1 EXACTLY at the divergent-map scenario's truncation target.
        h.cons.feed(Event::ConfigObserved { position: 4096, config: v1.clone() });
        let before = h.cons.state.config_record().unwrap();

        h.adopt_and_truncate(3, vec![(1, 0), (3, 4096)]);
        let after = h.cons.state.config_record().unwrap();
        assert_eq!(after, before, "to == position: no revert, record byte-identical");
    }

    // ---- post-M7 follow-up (Task 6): ConfigObserved position<=durable belt ----

    /// The do_work step-1c drain must SKIP (not adopt) a `ConfigObserved`
    /// observation whose position exceeds the durable counter — the archive
    /// agent's ordering guarantee (`do_work` store_release's `durable` as its
    /// LAST step, strictly before draining `take_config_observations`; see
    /// node.rs:705/715 and archive.rs's `do_work`) means this can never
    /// legitimately happen for a real, durably-recorded CONFIG frame. This
    /// drives the real drain path through the harness's `cfg_obs` sender
    /// (rather than `feed` directly) so the belt itself is exercised. The
    /// SAME payload at a plausible position (<= durable) must still adopt —
    /// the belt discriminates on position, not payload.
    #[test]
    fn implausible_config_observation_is_ignored() {
        let mut h = harness();
        let durable = h.cons.cnc.counters().durable.load_acquire();
        let v1 = v1_of(&h);
        let mut bytes = Vec::new();
        encode_config(&cluster_to_wire(&v1, 0), &mut bytes);

        // Implausible: far above durable — must be skipped, not adopted.
        h._cfg_obs_tx.send((durable + 1_000_000, bytes.clone())).unwrap();
        h.cons.do_work();
        assert_eq!(h.cons.sm.config().version, 0, "implausible obs must not adopt");

        // Plausible: the SAME config, at position <= durable — must still adopt.
        h._cfg_obs_tx.send((durable, bytes)).unwrap();
        h.cons.do_work();
        assert_eq!(h.cons.sm.config().version, 1, "plausible obs must still adopt");
    }

    // ---- post-M7 follow-up (Task 7): equal-version content-divergence check ----

    /// Pure-function unit test: same version + different content diverges;
    /// identical configs (even at the same version) and different versions
    /// are NOT this check's job (that's `ConfigObserved`'s version gate).
    #[test]
    fn config_content_divergence_detector() {
        let a = ClusterConfig {
            version: 3,
            voters: vec![(1, (0, 0)), (2, (0, 0)), (3, (0, 0))],
            learners: Vec::new(),
            tombstones: Vec::new(),
        };
        let mut b = a.clone();
        assert!(!config_content_diverges(&a, &b), "identical: benign");
        b.voters.pop();
        assert!(config_content_diverges(&a, &b), "same version, different content");
        let mut c = a.clone();
        c.version += 1;
        assert!(!config_content_diverges(&a, &c), "different version: not this check's job");
    }

    /// A same-version-different-content `ConfigObserved` through the real
    /// drain path (`_cfg_obs_tx` + `do_work`) is dropped by the version gate
    /// exactly like a benign re-observation — this task only adds the loud
    /// eprintln signal (`config_content_diverges`), it does not change
    /// adoption behavior: the adopted config must be unchanged either way.
    #[test]
    fn equal_version_content_divergence_is_not_adopted() {
        let mut h = harness();
        let durable = h.cons.cnc.counters().durable.load_acquire();
        let v1 = v1_of(&h);
        h.cons.feed(Event::ConfigObserved { position: 0, config: v1.clone() });
        assert_eq!(h.cons.sm.config(), &v1, "v1 adopted directly via feed");

        // Same version (1) as the adopted config, different content: drop
        // the learner v1_of added.
        let mut divergent = v1.clone();
        divergent.learners.clear();
        let mut bytes = Vec::new();
        encode_config(&cluster_to_wire(&divergent, 0), &mut bytes);

        h._cfg_obs_tx.send((durable, bytes)).unwrap();
        h.cons.do_work();

        assert_eq!(
            h.cons.sm.config(),
            &v1,
            "version gate drops the equal-version divergent observation"
        );
    }

    // ---- final-review carry: crash-handoff self-demote wedge (silent, by design) ----

    /// Pins the exact wedge shape the final-review finding describes:
    /// `propose_config`'s `SelfDemote` guard only blocks a leader from
    /// PROPOSING its own demote — it can't stop a DIFFERENT (now-crashed)
    /// leader's `DemoteVoter{self}` proposal from reaching this node from the
    /// log after IT wins an election. Drive the harness's id-1 SM to leader
    /// (mirrors `election::tests::leader_term1`'s Tick-then-majority-Vote
    /// recipe: boot_term 2 -> election term 3, self-vote + one peer grant out
    /// of the 3-voter [0,1,2] cluster is a majority), then feed a
    /// `ConfigObserved` demoting id 1 (self) to a learner — a real archive-
    /// scan observation would look identical. `adopt_config` has no
    /// tombstone to key off (a demote never tombstones), so the SM adopts it
    /// silently: role stays Leader, self is no longer a voter, and neither
    /// `HaltRemoved` nor `StepDownRemoved` fires — the node keeps serving as
    /// an unelectable leader with zero signal from the SM itself. The
    /// `Action::ConfigAdopted` exec arm's eprintln (this task's node-side
    /// fix) is not asserted here — it's the OBSERVABLE wedge shape underneath
    /// it that's load-bearing and worth pinning.
    #[test]
    fn leader_adopting_own_demote_from_log_stays_leader_unelectable_no_halt() {
        let mut h = harness();
        assert!(matches!(h.cons.sm.role(), Role::Follower), "harness boots a follower");

        // Drive to leader of term 3: election timeout, then a majority vote
        // (self + peer 0, out of voters [0,1,2]).
        h.cons.feed(Event::Tick { now_ns: 301 });
        h.cons.feed(Event::Vote { from: 0, term: 3, granted: true });
        assert!(matches!(h.cons.sm.role(), Role::Leader), "majority vote elects");
        assert!(!h.cons.halt_removed);

        // Crash-handoff shape: a `DemoteVoter{1}` a prior (now-dead) leader
        // proposed and replicated, durably recorded, now observed from the
        // log by this node — which is ITSELF the target, and is now leader.
        let mut demoted = h.cons.sm.config().clone();
        let self_addr = demoted
            .voters
            .iter()
            .find(|(id, _)| *id == 1)
            .map(|(_, addr)| *addr)
            .expect("self is a genesis voter");
        demoted.voters.retain(|(id, _)| *id != 1);
        demoted.learners.push((1, self_addr));
        demoted.version += 1;
        h.cons.feed(Event::ConfigObserved { position: 40, config: demoted });

        // The wedge, exactly as the finding describes: still Leader, self
        // demoted out of the voting set, not tombstoned, and no fail-stop.
        assert!(
            matches!(h.cons.sm.role(), Role::Leader),
            "adopting a config from the log does not itself change role"
        );
        assert!(!h.cons.sm.config().is_voter(1), "self was demoted to a learner");
        assert!(
            !h.cons.sm.config().tombstones.contains(&1),
            "a demote (unlike a remove) never tombstones"
        );
        assert!(!h.cons.halt_removed, "the wedge is silent: no halt/step-down fires");
    }

    // ---- M12b review carry: on_config_proposal's membership guard ----

    /// A kind-16 `ConfigProposal` whose source address resolves to no
    /// current member must be dropped and counted BEFORE `peer_actor` or
    /// `audit_admin` ever runs — neither an attestation nor an fsync'd audit
    /// record should be produced for a datagram this leader cannot vouch
    /// for. Proven by the negative: `last_config_reply` (only ever set by
    /// `propose_and_append`'s caller past the guard) stays `None`.
    #[test]
    fn on_config_proposal_drops_a_non_member_source_before_auditing() {
        let mut h = harness();
        // Drive to leader of term 3 (mirrors the sibling test above: election
        // timeout, then a majority vote out of the 3-voter [0,1,2] cluster).
        h.cons.feed(Event::Tick { now_ns: 301 });
        h.cons.feed(Event::Vote { from: 0, term: 3, granted: true });
        assert!(matches!(h.cons.sm.role(), Role::Leader), "majority vote elects");

        let stranger: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        assert!(
            !h.cons.addr_to_id.contains_key(&stranger),
            "test setup: this address must actually be a non-member"
        );

        let body = ConfigProposalBody { nonce: 1, op: 1, id: 9, ip: 0, port: 0 };
        h.cons.on_config_proposal(stranger, body);

        assert_eq!(
            h.cons.config_proposal_non_member, 1,
            "the drop must be counted"
        );
        assert!(
            h.cons.last_config_reply.is_none(),
            "a dropped datagram must never reach propose_and_append, so no reply is ever recorded"
        );
    }

    /// M12b final review (I4): a repeat kind-16 nonce is answered from the
    /// dedup cache and must NOT write a second audit record — the original
    /// answer is already on disk, and re-recording would let one captured
    /// datagram, re-sent in a loop, drive an unbounded stream of `fsync`s on
    /// the consensus thread. The re-send is counted instead.
    #[test]
    fn a_dedup_re_send_is_counted_not_re_audited() {
        let mut h = harness();
        h.cons.feed(Event::Tick { now_ns: 301 });
        h.cons.feed(Event::Vote { from: 0, term: 3, granted: true });
        assert!(matches!(h.cons.sm.role(), Role::Leader), "majority vote elects");

        let member = *h
            .cons
            .addr_to_id
            .keys()
            .next()
            .expect("the harness cluster has members");
        let body = ConfigProposalBody { nonce: 77, op: 1, id: 9, ip: 0, port: 0 };

        // First presentation: a fresh nonce, recorded like any admin answer.
        h.cons.on_config_proposal(member, body);
        let after_first = std::fs::read_to_string(h.cons.audit.path()).unwrap();
        assert_eq!(
            after_first.lines().count(),
            1,
            "a fresh-nonce proposal is recorded: {after_first}"
        );
        assert_eq!(h.cons.config_proposal_dedup_resend, 0);
        assert!(h.cons.last_config_reply.is_some(), "the answer was cached for dedup");

        // Same nonce again, five times over: cached answer re-sent, nothing
        // recorded, every re-send counted.
        for _ in 0..5 {
            h.cons.on_config_proposal(member, body);
        }
        assert_eq!(h.cons.config_proposal_dedup_resend, 5, "every re-send is counted");
        assert_eq!(
            std::fs::read_to_string(h.cons.audit.path()).unwrap(),
            after_first,
            "a byte-identical re-answer of the same nonce is not a new admin event"
        );
    }

    // ---- post-M7 follow-up (Task 10 fix): fiat install clears the pending mirror ----

    /// Discriminating proof for the fiat `store_config_pending(false)` in
    /// `maybe_adopt_incoming_snapshot` (Task 10): the learner-join e2e test's
    /// canary cannot tell the fiat clear from do_work's periodic mirror-clear
    /// (step 12) — a fresh joiner's SM has nothing pending, so the periodic
    /// clear wipes any pre-seeded mirror within one duty cycle on its own.
    /// Here the fiat install itself makes `sm.config_pending()` TRUE (the
    /// adopted floor sits above `commit_seen`), which BLOCKS step 12's
    /// `!self.sm.config_pending()` guard in the very cycle the install runs —
    /// so the mirror can only read 0 afterwards if the fiat store line ran.
    /// The assert on `sm.config_pending()` pins that blocking condition.
    #[test]
    fn fiat_snapshot_install_clears_config_pending_mirror() {
        let mut h = harness();
        let v1 = v1_of(&h);
        // A completed inbound snapshot at a floor ABOVE our durable frontier
        // (the learner-join shape: `durable < pos` is what routes
        // `maybe_adopt_incoming_snapshot` into the adopt/fiat branch), with
        // the leader's config carried alongside — exactly the two slots the
        // receiver's `snap_complete` publishes (config cell BEFORE position).
        let floor = 1u64 << 20;
        assert!(floor > h.cons.cnc.counters().durable.load_acquire());
        *h.cons.incoming_snapshot_config.lock().unwrap() = config_wire_bytes(&v1, 0);
        h.cons.incoming_snapshot.store(floor, Ordering::Release);
        // A stale pre-crash `true` in the pending mirror.
        h.cons.cnc.store_config_pending(true);

        h.cons.do_work();

        assert_eq!(h.cons.sm.config().version, 1, "fiat install adopted the carried config");
        assert!(
            h.cons.sm.config_pending(),
            "floor > commit_seen: the periodic mirror-clear must be blocked this cycle"
        );
        assert_eq!(
            h.cons.cnc.config_pending(),
            0,
            "the fiat install itself must clear the pending mirror"
        );
    }

    // ---- M7 Task 6: boot recovery of the ConfigRecord ----

    /// A genesis `ClusterConfig` over a one-voter seed `[(0, addr)]`.
    fn seed_members() -> Vec<(NodeId, SocketAddr)> {
        vec![(0, "127.0.0.1:9200".parse().unwrap())]
    }

    /// Append a real `FRAME_TYPE_CONFIG` frame carrying `cfg` into `archive` (via
    /// a throwaway heap-backed buffer + `Appender`, exactly the bytes
    /// `Consensus::append_config_frame` would produce) and durably record it
    /// (`do_work` to exhaustion). Returns the frame-END position.
    fn append_and_archive_config(archive: &mut Archive, term: u32, cfg: &ClusterConfig) -> u64 {
        let cnc = test_cnc();
        let buffer = Arc::new(LogBuffer::new(Region::heap_zeroed(1 << 16), cnc, 4096));
        let mut appender = Appender::new(Arc::clone(&buffer), term);
        let mut bytes = Vec::new();
        encode_config(&cluster_to_wire(cfg, 0), &mut bytes);
        let end = appender.append_config(term, &bytes).expect("config frame append");
        while archive.do_work(&buffer).expect("archive do_work") {}
        end
    }

    /// Like `append_and_archive_config`, but appends TWO real `FRAME_TYPE_CONFIG`
    /// frames (`cfg1` then `cfg2`) onto the SAME buffer before draining —
    /// modeling two adoptions durably archived in the same run (unlike two
    /// separate calls to `append_and_archive_config`, which would each start a
    /// fresh buffer at position 0 and so could never land sequentially in one
    /// archive). Returns `(cfg1`'s frame-END, `cfg2`'s frame-END)`.
    fn append_and_archive_two_configs(
        archive: &mut Archive,
        term: u32,
        cfg1: &ClusterConfig,
        cfg2: &ClusterConfig,
    ) -> (u64, u64) {
        let cnc = test_cnc();
        let buffer = Arc::new(LogBuffer::new(Region::heap_zeroed(1 << 16), cnc, 4096));
        let mut appender = Appender::new(Arc::clone(&buffer), term);
        let mut bytes1 = Vec::new();
        encode_config(&cluster_to_wire(cfg1, 0), &mut bytes1);
        let end1 = appender.append_config(term, &bytes1).expect("v1 config frame append");
        let mut bytes2 = Vec::new();
        encode_config(&cluster_to_wire(cfg2, 0), &mut bytes2);
        let end2 = appender.append_config(term, &bytes2).expect("v2 config frame append");
        while archive.do_work(&buffer).expect("archive do_work") {}
        (end1, end2)
    }

    /// Step 3a: a follower durably archives a `FRAME_TYPE_CONFIG` frame (the data
    /// plane), but a crash before the NEXT duty cycle drains it means the
    /// `ConfigRecord` file itself never reflects the adoption — modeled here by
    /// deleting `config.state` outright from a stopped instance dir. On reboot,
    /// `recover_config_record` must NOT silently reseed genesis (losing the real
    /// adoption): the archive scan re-discovers the durably-archived frame and
    /// rebuilds the record to the SAME version it held before the file was lost.
    #[test]
    fn boot_rebuilds_config_record_from_archive_scan_after_config_state_loss() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("state")).unwrap();
        let members = seed_members();

        let mut archive = Archive::open(ArchiveConfig::new(dir.path().join("journal"))).unwrap();
        let genesis = ClusterConfig::genesis(
            members.iter().map(|(id, a)| (*id, addr_to_pair(*a))).collect(),
            Vec::new(),
        );
        let mut v1 = genesis.clone();
        v1.learners.push((9, (9, 1)));
        v1.version = 1;
        let end = append_and_archive_config(&mut archive, 1, &v1);
        let durable = archive.recovered_position();
        assert_eq!(durable, end, "the config frame is the only bytes recorded");

        // First life: the record legitimately reflects v1 (as `Action::ConfigAdopted`
        // would have persisted it), then config.state is lost.
        {
            let state = NodeState::open(&dir.path().join("state")).unwrap();
            let rec = ConfigRecord {
                position: end,
                config: cluster_to_stored(&v1),
                prev_position: 0,
                prev: cluster_to_stored(&genesis),
            };
            state.store_config_record(&rec).unwrap();
        }
        std::fs::remove_file(dir.path().join("state").join("config.state")).unwrap();
        // A stale `.bak`/rotation slot could also carry the old value back — a
        // real `StableValue` may keep more than one file; remove whatever exists.
        for entry in std::fs::read_dir(dir.path().join("state")).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name().to_string_lossy().starts_with("config.state") {
                let _ = std::fs::remove_file(entry.path());
            }
        }

        // Reboot: fresh `Archive::open` (recovers the durably-archived frame) +
        // fresh `NodeState::open` (config.state gone -> `config_record() == None`).
        let archive = Archive::open(ArchiveConfig::new(dir.path().join("journal"))).unwrap();
        let state = NodeState::open(&dir.path().join("state")).unwrap();
        assert!(state.config_record().is_none(), "the file is genuinely gone");

        let rec = recover_config_record(&state, &archive, durable, &members, &[]).unwrap();
        assert_eq!(rec.config.version, 1, "rebuilt to the SAME version the lost file held");
        assert_eq!(stored_to_cluster(&rec.config), v1);
        assert_eq!(rec.position, end);
        assert_eq!(stored_to_cluster(&rec.prev), genesis, "prev recovered as the genesis seed");
        // The rebuilt record is durable — a subsequent read agrees.
        assert_eq!(state.config_record().unwrap(), rec);
    }

    /// T5-carry: a record whose `position` is ABOVE the recovered archive
    /// frontier (the leader persists at append, strictly ahead of the archive's
    /// own fsync) must revert to its own prev level on boot — the position it
    /// claims is not actually backed by durable bytes.
    #[test]
    fn boot_reverts_a_config_record_persisted_ahead_of_durable() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("state")).unwrap();
        let members = seed_members();
        let archive = Archive::open(ArchiveConfig::new(dir.path().join("journal"))).unwrap();
        let durable = archive.recovered_position();
        assert_eq!(durable, 0, "empty journal: nothing durable");

        let genesis = ClusterConfig::genesis(
            members.iter().map(|(id, a)| (*id, addr_to_pair(*a))).collect(),
            Vec::new(),
        );
        let mut v1 = genesis.clone();
        v1.learners.push((9, (9, 1)));
        v1.version = 1;

        let state = NodeState::open(&dir.path().join("state")).unwrap();
        // Simulate the leader's append-time persist: position 9000, ABOVE durable
        // (0) — the archive never actually recorded that frame before the crash.
        // `prev` (genesis, position 0) is genuinely below durable, so this is the
        // ordinary (non-compounding) revert case.
        let ahead = ConfigRecord {
            position: 9000,
            config: cluster_to_stored(&v1),
            prev_position: 0,
            prev: cluster_to_stored(&genesis),
        };
        state.store_config_record(&ahead).unwrap();

        let rec = recover_config_record(&state, &archive, durable, &members, &[]).unwrap();
        assert_eq!(rec.position, 0, "reverted to prev's position");
        assert_eq!(stored_to_cluster(&rec.config), genesis, "reverted to the genuine predecessor");
        assert_eq!(state.config_record().unwrap(), rec, "the revert is itself persisted");
    }

    /// T5-carry, compounding case: BOTH the record's position AND its prev's
    /// position are ahead of durable (two adoptions in the same crash-exposed
    /// window) — there is nothing genuine left to revert to, so recovery falls
    /// back to a fresh config-by-fiat genesis seed.
    #[test]
    fn boot_falls_back_to_seed_when_both_levels_are_ahead_of_durable() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("state")).unwrap();
        let members = seed_members();
        let archive = Archive::open(ArchiveConfig::new(dir.path().join("journal"))).unwrap();
        let durable = archive.recovered_position();
        assert_eq!(durable, 0);

        let genesis = ClusterConfig::genesis(
            members.iter().map(|(id, a)| (*id, addr_to_pair(*a))).collect(),
            Vec::new(),
        );
        let mut v1 = genesis.clone();
        v1.learners.push((9, (9, 1)));
        v1.version = 1;
        let mut v2 = v1.clone();
        v2.version = 2;

        let state = NodeState::open(&dir.path().join("state")).unwrap();
        let ahead = ConfigRecord {
            position: 9000,
            config: cluster_to_stored(&v2),
            prev_position: 5000, // ALSO above durable (0)
            prev: cluster_to_stored(&v1),
        };
        state.store_config_record(&ahead).unwrap();

        let rec = recover_config_record(&state, &archive, durable, &members, &[]).unwrap();
        assert_eq!(rec.position, 0);
        assert_eq!(rec.config.version, 0, "fresh genesis-by-fiat, not either compromised level");
        assert_eq!(stored_to_cluster(&rec.config), genesis);
        assert_eq!(rec.config, rec.prev, "seed record: prev duplicated");
    }

    /// Ledger minor (k): composes the T5 revert shape
    /// (`boot_reverts_a_config_record_persisted_ahead_of_durable`) with the
    /// Step 3a rederive shape
    /// (`boot_rebuilds_config_record_from_archive_scan_after_config_state_loss`)
    /// in a single boot — the pair was previously only tested separately. The
    /// archive durably records TWO real config frames, v1 then v2 (both
    /// fsynced, so `durable` covers both) — but the persisted `ConfigRecord`
    /// claims a stale v3 whose `prev` level is v1, itself genuinely below
    /// `durable`. Revert-only recovery (skip the rederive step) would land on
    /// the reverted v1 and silently lose the already-durable v2; the
    /// discriminating assert is that recovery instead re-scans forward from
    /// the reverted v1 and picks v2 back up — landing on v2's version AND its
    /// frame-END position, not the reverted-to v1.
    #[test]
    fn boot_revert_then_journal_rederive_compose() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("state")).unwrap();
        let members = seed_members();

        let mut archive = Archive::open(ArchiveConfig::new(dir.path().join("journal"))).unwrap();
        let genesis = ClusterConfig::genesis(
            members.iter().map(|(id, a)| (*id, addr_to_pair(*a))).collect(),
            Vec::new(),
        );
        let mut v1 = genesis.clone();
        v1.learners.push((9, (9, 1)));
        v1.version = 1;
        let mut v2 = v1.clone();
        v2.learners.push((10, (10, 1)));
        v2.version = 2;

        let (end1, end2) = append_and_archive_two_configs(&mut archive, 1, &v1, &v2);
        let durable = archive.recovered_position();
        assert_eq!(durable, end2, "both v1 and v2 frames are durably archived");

        // Persisted record: a stale v3 claimed AHEAD of durable (the T5 crash
        // window — the leader's append-time persist raced the archive fsync),
        // whose `prev` level is v1 — itself genuinely below durable, so a
        // plain revert (no rederivation) lands exactly on v1, NOT v2.
        let mut v3 = v2.clone();
        v3.learners.push((11, (11, 1)));
        v3.version = 3;
        let state = NodeState::open(&dir.path().join("state")).unwrap();
        let ahead = ConfigRecord {
            position: durable + 9000,
            config: cluster_to_stored(&v3),
            prev_position: end1,
            prev: cluster_to_stored(&v1),
        };
        state.store_config_record(&ahead).unwrap();

        // Reboot: fresh `Archive::open` (recovers the durably-archived frames).
        let archive = Archive::open(ArchiveConfig::new(dir.path().join("journal"))).unwrap();
        let rec = recover_config_record(&state, &archive, durable, &members, &[]).unwrap();

        // The discriminating assert: recovery lands on the journal-rederived
        // v2 (version AND frame-END position), not the reverted-to v1 — this
        // fails if rederivation is skipped after the revert.
        assert_eq!(rec.config.version, 2, "must rederive forward past the revert, landing on v2");
        assert_eq!(rec.position, end2, "position must be v2's frame-END, not v1's (the reverted level)");
        assert_eq!(stored_to_cluster(&rec.config), v2);
        assert_eq!(stored_to_cluster(&rec.prev), v1, "prev is the level rederivation folded from");
        assert_eq!(state.config_record().unwrap(), rec, "the rederived record is itself persisted");
    }

    // ==================================================================
    // M8 Task 12: wire-crypto node wiring
    // ==================================================================
    //
    // Fixtures mirror `uc2_net::receiver`'s crypto fixtures (same key-file
    // and allowlist shapes) rather than inventing a second convention: real
    // `Identity` key files, a real allowlist, a real `SharedTransport`.
    // Nothing here fakes a session — the handshake test drives a genuine
    // Noise IK exchange over a real UDP socket pair.

    use uc2_crypto::schedule::epoch_is_newer;
    use uc_protocol::v2::crypto::{DGRAM_KIND_HS_INIT, DGRAM_KIND_HS_RESP};
    use uc_protocol::v2::datagram::read_datagram_header;

    /// The deliberate skew `harness_with_crypto` inserts between the
    /// `SharedTransport`'s `Instant` origin and the `Consensus` agent's own —
    /// see that function and
    /// `the_crypto_plane_reads_the_transports_clock_not_the_consensus_agents`.
    const HARNESS_CRYPTO_CLOCK_GAP_NS: u64 = 5_000_000;

    const T12_PRIV_SELF: [u8; 32] = [0x31; 32];
    const T12_PRIV_PEER: [u8; 32] = [0x32; 32];

    /// Scratch on real disk (`CARGO_TARGET_TMPDIR`), never `/tmp` — that is
    /// RAM-backed tmpfs with no swap on this box (CLAUDE.md).
    fn crypto_scratch_dir(tag: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::var("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc2_node_tests")
            })
            .join("uc2-node-crypto")
            .join(format!("{tag}-{seq}"));
        // Wipe first. `SEQ` is unique WITHIN a process but restarts at 0 on
        // the next `cargo test`, and test order is not deterministic, so a
        // dir that held a booted node's `instance.lock`/`cnc2.dat` in one run
        // can be handed to the boot-refusal test in the next — which asserts
        // exactly that those files are absent. Observed as a real
        // cross-run flake during the mutation campaign, not hypothesized.
        let _ = std::fs::remove_dir_all(&dir);
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

    /// Standard-alphabet base64 with padding — matches `uc2_crypto::identity`'s
    /// allowlist parser. Hand-rolled rather than adding a `base64`
    /// dev-dependency to `uc2_node` for one fixture.
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
            out.push(if chunk.len() > 1 {
                ALPHABET[((n >> 6) & 0x3F) as usize] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 { ALPHABET[(n & 0x3F) as usize] as char } else { '=' });
        }
        out
    }

    fn identity_public(tag: &str, private: [u8; 32]) -> [u8; 32] {
        let dir = crypto_scratch_dir(tag);
        let key_path = dir.join("node.key");
        write_key_file(&key_path, private);
        uc2_crypto::identity::Identity::load(&key_path).unwrap().public_bytes()
    }

    /// A real `CryptoConfig::Enabled` over freshly written key/allowlist files.
    fn enabled_crypto_config(
        tag: &str,
        private: [u8; 32],
        allow: &[(NodeId, [u8; 32])],
    ) -> CryptoConfig {
        let dir = crypto_scratch_dir(tag);
        let key_path = dir.join("node.key");
        write_key_file(&key_path, private);
        let allowlist_path = dir.join("allowlist");
        let mut text = String::new();
        for (id, public) in allow {
            text.push_str(&format!("{id} {}\n", b64_32(public)));
        }
        std::fs::write(&allowlist_path, text).unwrap();
        CryptoConfig::Enabled {
            key_path,
            allowlist_path,
            rotation: uc2_crypto::rotation::RotationPolicy::default(),
        }
    }

    /// The allowlist path inside an `Enabled` config.
    fn allowlist_path_of(cfg: &CryptoConfig) -> PathBuf {
        match cfg {
            CryptoConfig::Enabled { allowlist_path, .. } => allowlist_path.clone(),
            CryptoConfig::Disabled => unreachable!("fixture builds an Enabled config"),
        }
    }

    /// A minimal single-voter `NodeConfig` over a fresh instance dir.
    fn test_node_config() -> (NodeConfig, PathBuf) {
        let dir = crypto_scratch_dir("node-config");
        let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let cfg = NodeConfig {
            id: 1,
            members: vec![(1, bind)],
            learners: Vec::new(),
            bind,
            instance_dir: dir.clone(),
            app_id: "t12".into(),
            buffer_bytes: 1 << 20,
            max_payload: 256,
            admission_bytes: 256 * 1024,
            election_timeout_min_ns: 20_000_000,
            election_timeout_max_ns: 40_000_000,
            seed: 1,
            faults: FaultConfig::default(),
            purge: PurgePolicy::Disabled,
            journal_segment_bytes: DEFAULT_JOURNAL_SEGMENT_BYTES,
            crypto: CryptoConfig::Disabled,
        };
        (cfg, dir)
    }

    /// Mirrors the M7 self-tombstone boot refusal: a node that cannot
    /// authenticate must not silently fall back to cleartext.
    #[test]
    fn a_node_configured_for_crypto_with_unreadable_key_files_refuses_to_start() {
        let (mut cfg, _dir) = test_node_config();
        cfg.crypto = CryptoConfig::Enabled {
            key_path: "/nonexistent/key".into(),
            allowlist_path: "/nonexistent/allow".into(),
            rotation: Default::default(),
        };
        let err = match Node::start(cfg) {
            Err(e) => e,
            Ok(_) => panic!("a node with unreadable key files must refuse to start"),
        };
        assert!(
            err.to_string().contains("crypto is enabled"),
            "the refusal must name the crypto config, got: {err}"
        );
    }

    /// The refusal must be a CLEAN EARLY RETURN, before any agent is spawned
    /// and before any instance file is created — the same shape as the M7
    /// tombstone refusal. Discriminating: a valid key with an unreadable
    /// ALLOWLIST must still refuse (so the check is not just
    /// `Identity::load`), and the instance directory must be left untouched
    /// (a node that got as far as `CncPage::create_file` would leave one
    /// behind, and would have taken the flock).
    #[test]
    fn crypto_boot_refusal_leaves_no_instance_files_and_no_lock() {
        let (mut cfg, dir) = test_node_config();
        let good = enabled_crypto_config("refusal-good-key", T12_PRIV_SELF, &[]);
        let CryptoConfig::Enabled { key_path, .. } = good else { unreachable!() };
        cfg.crypto = CryptoConfig::Enabled {
            key_path,
            allowlist_path: "/nonexistent/allow".into(),
            rotation: Default::default(),
        };
        assert!(Node::start(cfg).is_err(), "an unreadable allowlist is also a refusal");
        // `cnc2.dat`, matching `InstanceDir::cnc_path` — asserting on a name
        // the node never writes would make this half of the test vacuous.
        assert!(
            !dir.join("cnc2.dat").exists(),
            "the refusal must precede instance-file creation"
        );
        assert!(
            !dir.join("instance.lock").exists(),
            "the refusal must precede taking the instance flock"
        );
    }

    #[test]
    fn default_config_is_disabled_so_existing_deployments_are_untouched() {
        assert!(matches!(test_node_config().0.crypto, CryptoConfig::Disabled));
        assert!(matches!(CryptoConfig::default(), CryptoConfig::Disabled));
    }

    /// A real, crypto-enabled single-voter node boots, elects itself, and
    /// mints a group epoch — the whole construction path (`SharedTransport`,
    /// both halves handed out exactly once, the receiver's `CryptoIntake`,
    /// the consensus agent's crypto cycle) exercised through `Node::start`.
    #[test]
    fn a_crypto_enabled_node_boots_and_mints_an_epoch_on_winning_its_election() {
        let (mut cfg, _dir) = test_node_config();
        cfg.crypto = enabled_crypto_config("solo-node", T12_PRIV_SELF, &[]);
        let node = Node::start(cfg).expect("a well-configured crypto node boots");
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        while node.crypto_epoch().is_none() {
            assert!(Instant::now() < deadline, "a fresh leader never minted a group epoch");
            std::thread::yield_now();
        }
        assert!(node.is_leader(), "the sole voter elected itself");
        node.stop();
    }

    // ---- the bare-`Consensus` crypto harness ----

    /// A [`Harness`] with crypto wired as `Node::start_with_socket` wires it,
    /// plus a live peer transport (node 0) reachable over a real UDP socket
    /// so a genuine handshake can be driven against the node under test.
    struct CryptoHarness {
        h: Harness,
        peer: SharedTransport,
        peer_recv: uc2_crypto::ReceiveHalf,
        peer_sock: UdpSocket,
        /// Datagrams read off the peer socket while hunting for a different
        /// kind. Kept rather than discarded: the node can emit an `HS_RESP`
        /// and an `HS_KEY` in the SAME duty cycle, and a helper that threw
        /// away whatever it was not looking for would silently eat the one a
        /// later assertion depends on (which it did, first time round).
        stash: Vec<Vec<u8>>,
        /// The node's own allowlist file — an operator's runtime edit.
        allowlist_path: PathBuf,
        /// Node 2's address, bound for real but deliberately NOT in the
        /// allowlist at construction: the M7 "operator drops a key in" case.
        peer2_sock: UdpSocket,
    }

    impl CryptoHarness {
        fn crypto_epoch(&self) -> Option<u16> {
            self.h.cons.crypto_epoch()
        }

        /// Election + first commit, then one duty cycle so the crypto
        /// maintenance pass runs (the mint `on_became_leader` latched).
        fn drive_to_leader(&mut self) {
            drive_to_serving_leader(&mut self.h);
            self.h.cons.do_work();
        }

        /// Adopt + commit a config demoting `id` to a learner — NO tombstone.
        fn commit_config_demoting(&mut self, id: NodeId) {
            let mut c = self.h.cons.sm.config().clone();
            let addr = c.voters.iter().find(|(v, _)| *v == id).map(|(_, a)| *a).unwrap();
            c.voters.retain(|(v, _)| *v != id);
            c.learners.push((id, addr));
            c.version += 1;
            self.h.cons.feed(Event::ConfigObserved { position: 40, config: c });
            self.h.cons.do_work();
        }

        /// Adopt + commit a config removing `id` — the tombstone set grows.
        fn commit_config_removing(&mut self, id: NodeId) {
            let mut c = self.h.cons.sm.config().clone();
            c.voters.retain(|(v, _)| *v != id);
            c.learners.retain(|(v, _)| *v != id);
            c.tombstones.push(id);
            c.version += 1;
            self.h.cons.feed(Event::ConfigObserved { position: 41, config: c });
            self.h.cons.do_work();
        }

        /// Inject a handshake-plane datagram exactly as the receiver's
        /// `crypto_admit` would deliver one (`HS_KEY` bodies arrive already
        /// opened), then run one duty cycle.
        fn deliver_handshake(&mut self, kind: u8, body: &[u8]) {
            let from = self.peer_sock.local_addr().unwrap();
            self.h.hs_tx.try_send((from, kind, body.to_vec())).unwrap();
            self.h.cons.do_work();
        }

        /// The next RAW datagram of `want` the node sent to the peer socket,
        /// checking the stash first and stashing anything else it reads.
        fn recv_kind_raw(&mut self, want: u8) -> Option<Vec<u8>> {
            if let Some(i) = self.stash.iter().position(|d| {
                d.len() >= DATAGRAM_HEADER_LEN && read_datagram_header(d).kind == want
            }) {
                return Some(self.stash.remove(i));
            }
            let deadline = Instant::now() + std::time::Duration::from_secs(5);
            let mut buf = vec![0u8; 65_536];
            while Instant::now() < deadline {
                let Ok((n, _)) = self.peer_sock.recv_from(&mut buf) else {
                    self.h.cons.do_work();
                    std::thread::yield_now();
                    continue;
                };
                if n < DATAGRAM_HEADER_LEN {
                    continue;
                }
                if read_datagram_header(&buf[..n]).kind == want {
                    return Some(buf[..n].to_vec());
                }
                self.stash.push(buf[..n].to_vec());
            }
            None
        }

        /// As [`CryptoHarness::recv_kind_raw`], but returns the datagram's
        /// BODY, opened if the kind is a sealed one — so a successful return
        /// is itself the assertion that the node sealed it correctly.
        fn recv_kind(&mut self, want: u8) -> Option<Vec<u8>> {
            let mut d = self.recv_kind_raw(want)?;
            let n = d.len();
            Some(if matches!(Transport::scope_of(want), Scope::Unsealed) {
                d[DATAGRAM_HEADER_LEN..n].to_vec()
            } else {
                let len = self
                    .peer_recv
                    .open_slice(1, &mut d, n)
                    .expect("the node's sealed datagram must open under our session");
                d[DATAGRAM_HEADER_LEN..len].to_vec()
            })
        }
    }

    /// The single `HandshakeAction::Send` body of `kind` in `acts`.
    fn expect_send(acts: &[HandshakeAction], kind: u8) -> Vec<u8> {
        acts.iter()
            .find_map(|a| match a {
                HandshakeAction::Send { kind: k, body, .. } if *k == kind => Some(body.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no Send of kind {kind} in {acts:?}"))
    }

    fn crypto_harness() -> CryptoHarness {
        let self_pub = identity_public("harness-self-pub", T12_PRIV_SELF);
        let peer_pub = identity_public("harness-peer-pub", T12_PRIV_PEER);
        // The harness node is id 1 and trusts node 0; node 0 trusts node 1.
        let self_cfg = enabled_crypto_config("harness-self", T12_PRIV_SELF, &[(0, peer_pub)]);
        let peer_cfg = enabled_crypto_config("harness-peer", T12_PRIV_PEER, &[(1, self_pub)]);
        let crypto = SharedTransport::new(&self_cfg, 1).unwrap().unwrap();
        let peer = SharedTransport::new(&peer_cfg, 0).unwrap().unwrap();
        let peer_recv = peer.receive_half();
        let peer_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        peer_sock.set_nonblocking(true).unwrap();
        let peer_addr = peer_sock.local_addr().unwrap();
        let peer2_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        peer2_sock.set_nonblocking(true).unwrap();
        let peer2_addr = peer2_sock.local_addr().unwrap();
        let h = harness_with_crypto(Some(crypto), &[(0, peer_addr), (2, peer2_addr)]);
        CryptoHarness {
            h,
            peer,
            peer_recv,
            peer_sock,
            stash: Vec::new(),
            allowlist_path: allowlist_path_of(&self_cfg),
            peer2_sock,
        }
    }

    /// **One clock source** (checklist item 6). Every crypto call must take
    /// its `now_ns` from `SharedTransport`'s origin — the one the sender and
    /// receiver agents' halves also read — never from `Consensus::base`.
    ///
    /// The first round of this task recorded the corresponding mutant as an
    /// undetectable survivor, on the reasoning that the two origins are
    /// "microseconds apart during the same `start_with_socket`". **That was
    /// wrong.** They bracket the whole of recovery — `Archive::open`,
    /// `rederive_term_map`, `NodeState::open`, `CncPage::create_file`, the
    /// log-buffer mmap, the rings — which on a node with a large journal is
    /// seconds, not microseconds. `harness_with_crypto` now reproduces that
    /// gap deliberately.
    ///
    /// The skew also has the HARMFUL sign, which is why this is a liveness
    /// bug and not a rounding error: the transport's origin is the EARLIER
    /// one, so its `now_ns` is the LARGER. A node driving `GroupPlane::mint`
    /// off `Consensus::now_ns` would stamp `minted_at` in the SMALLER base
    /// while the sender evaluates `sealing_epoch(shared_now)` in the larger —
    /// making every freshly minted epoch look as though its
    /// `ACTIVATION_TIMEOUT_NS` had already elapsed, and sealing under an
    /// epoch no peer has acked. That is precisely the "activation grace
    /// elapses instantly" failure the one-clock rule exists to prevent.
    ///
    /// Discriminating in both directions: `crypto_now_ns` must TRACK the
    /// transport's clock (within a millisecond of a direct read) and must
    /// DIVERGE from the agent's own by at least the injected gap.
    #[test]
    fn the_crypto_plane_reads_the_transports_clock_not_the_consensus_agents() {
        let h = crypto_harness();
        let transport_now = h.h.cons.crypto.as_ref().unwrap().now_ns();
        let crypto_now = h.h.cons.crypto_now_ns();
        let agent_now = h.h.cons.now_ns();

        assert!(
            crypto_now.abs_diff(transport_now) < 1_000_000,
            "crypto_now_ns must read the SharedTransport's origin \
             (transport {transport_now}, crypto_now_ns {crypto_now})"
        );
        assert!(
            crypto_now > agent_now,
            "the transport's origin is the EARLIER one, so its elapsed reading \
             must be the LARGER (crypto_now_ns {crypto_now}, Consensus::now_ns {agent_now})"
        );
        assert!(
            crypto_now - agent_now >= HARNESS_CRYPTO_CLOCK_GAP_NS,
            "the two clocks must differ by at least the construction gap \
             ({} ns observed, {HARNESS_CRYPTO_CLOCK_GAP_NS} ns injected)",
            crypto_now - agent_now
        );
    }

    /// Rotation trigger 1: a new leader always mints.
    #[test]
    fn winning_an_election_mints_a_fresh_epoch() {
        let mut h = crypto_harness();
        let before = h.crypto_epoch();
        assert!(before.is_none(), "a node that has never led has never minted");
        h.drive_to_leader();
        assert!(epoch_is_newer(h.crypto_epoch().unwrap(), before.unwrap_or(0)));
    }

    /// Rotation trigger 3: a committed `Remove*` revokes; a committed demote
    /// does not. Also the discriminating test for feeding
    /// `on_committed_config` on EVERY committed change rather than only
    /// removals — under "removals only" the first removal would merely seed
    /// `RotationState`'s baseline and rotate nothing.
    #[test]
    fn a_committed_remove_rotates_but_a_committed_demote_does_not() {
        let mut h = crypto_harness();
        h.drive_to_leader();
        let e0 = h.crypto_epoch().unwrap();

        h.commit_config_demoting(2);
        assert_eq!(h.crypto_epoch().unwrap(), e0, "a demote keeps the node replicating");

        h.commit_config_removing(3);
        assert!(epoch_is_newer(h.crypto_epoch().unwrap(), e0), "a removal revokes");
    }

    /// The mint's `HandshakeAction::Send`s must be CONSUMED by this layer:
    /// sealed pairwise and pushed at the socket. With no established session
    /// the seal fails — and the node must count that drop, never send the
    /// key in the clear. A node that ignored the mint's actions entirely
    /// leaves this counter at 0.
    #[test]
    fn a_mint_hands_every_peer_an_hs_key_delivery_this_layer_must_seal() {
        let mut h = crypto_harness();
        assert_eq!(h.h.cons.crypto_hs_key_seal_failures(), 0);
        h.drive_to_leader();
        assert_eq!(
            h.h.cons.crypto_hs_key_seal_failures(),
            2,
            "one HS_KEY per peer (nodes 0 and 2), neither sealable without a session"
        );
    }

    /// The whole handshake plane end to end: a real Noise IK exchange driven
    /// through the node's handshake route, then a real group-key delivery
    /// SEALED BY THIS LAYER over the established pairwise channel, opened and
    /// acked by the peer, with the ack folded back into the group plane.
    #[test]
    fn handshake_routing_establishes_a_session_then_delivers_a_sealed_group_key() {
        let mut h = crypto_harness();

        // 1. Peer 0 initiates; its HS_INIT arrives on the node's handshake
        //    route exactly as `crypto_admit` would deliver it.
        let acts = h.peer.initiate(1, h.peer.now_ns());
        let init = expect_send(&acts, DGRAM_KIND_HS_INIT);
        h.deliver_handshake(DGRAM_KIND_HS_INIT, &init);

        // 2. The node answered HS_RESP on its own socket, in the clear
        //    (`Scope::Unsealed` — there is nothing to seal under yet).
        let resp = h.recv_kind(DGRAM_KIND_HS_RESP).expect("the node answered the HS_INIT");
        let acts = h.peer.on_handshake_message(1, DGRAM_KIND_HS_RESP, &resp, h.peer.now_ns());
        assert!(
            acts.iter().any(|a| matches!(a, HandshakeAction::Established { peer: 1, .. })),
            "the peer's session with the node is up: {acts:?}"
        );
        assert!(h.h.cons.crypto.as_ref().unwrap().is_established(0), "and the node's with it");

        // 3. The node becomes leader and mints. The HS_KEY for peer 0 is now
        //    sealable over the established pairwise session — and `recv_kind`
        //    OPENS it, which is itself the assertion that it went out sealed.
        h.drive_to_leader();
        let epoch = h.crypto_epoch().expect("the new leader minted");
        let body = h.recv_kind(DGRAM_KIND_HS_KEY).expect("an HS_KEY was delivered");
        assert_eq!(
            u16::from_le_bytes([body[1], body[2]]),
            epoch,
            "the delivered epoch is the one the node minted"
        );

        // 4. The peer opens + installs it and acks; the ack rides back.
        let acts = h.peer.on_group_key_message(1, &body);
        let ack = expect_send(&acts, DGRAM_KIND_HS_KEY);
        h.deliver_handshake(DGRAM_KIND_HS_KEY, &ack);
    }

    /// A lost `HS_KEY` must be re-sent. `GroupPlane::mint` emits each
    /// delivery exactly once; the datagram rides UDP, and a peer that misses
    /// it can open no group-scope traffic at all — it cannot self-heal
    /// through NAK repair, because a NAK'd retransmit is itself `DATA` sealed
    /// under the very epoch it is missing.
    #[test]
    fn an_unacked_group_key_is_redelivered_not_lost_until_the_next_rotation() {
        let mut h = crypto_harness();

        // Establish a session with peer 0 (as above), then mint.
        let acts = h.peer.initiate(1, h.peer.now_ns());
        let init = expect_send(&acts, DGRAM_KIND_HS_INIT);
        h.deliver_handshake(DGRAM_KIND_HS_INIT, &init);
        let resp = h.recv_kind(DGRAM_KIND_HS_RESP).expect("HS_RESP");
        h.peer.on_handshake_message(1, DGRAM_KIND_HS_RESP, &resp, h.peer.now_ns());
        h.drive_to_leader();

        // Drop the first delivery on the floor (never acked), then let the
        // re-delivery timer come due.
        let first = h.recv_kind(DGRAM_KIND_HS_KEY).expect("the initial delivery");
        h.h.cons.crypto_last_redeliver_ns = None;
        h.h.cons.do_work();
        let again = h.recv_kind(DGRAM_KIND_HS_KEY).expect("an un-acked epoch is re-delivered");
        assert_eq!(again, first, "the same epoch's key, re-sent verbatim");

        // Once acked, the sweep goes quiet: nothing further is re-delivered.
        let acts = h.peer.on_group_key_message(1, &again);
        let ack = expect_send(&acts, DGRAM_KIND_HS_KEY);
        h.deliver_handshake(DGRAM_KIND_HS_KEY, &ack);
        // The GATE set and the DELIVERY set are now different questions, and
        // peer 2 — unreachable, no established session — answers them
        // differently. It never gated activation (option A, 2026-08-05: an
        // `HS_KEY` is sealed pairwise, so a peer with no session provably never
        // received the key and its ack can never come), so it is not
        // "unacked" for the pending epoch...
        let crypto = h.h.cons.crypto.as_ref().unwrap();
        assert!(
            crypto.unacked_group_key_peers().is_empty(),
            "peer 0 acked, and peer 2 was never in the activation set"
        );
        // ...but it is still OWED the key, and redelivery must keep targeting
        // it so it can open group traffic the moment its session exists.
        assert_eq!(
            crypto.group_key_missing_peers(&[0, 2]),
            vec![2],
            "the unreachable peer is still owed the key"
        );
    }

    /// A peer that RESTARTS holds no group key at all — its new process
    /// minted none and was never delivered ours — and `GroupPlane` still
    /// counts it as acked from its previous life, so the un-acked sweep
    /// alone would never re-key it. The leader must re-deliver off the fresh
    /// `HandshakeAction::Established`. Without this a restarted follower
    /// opens no `DATA` until the next rotation (an hour, by default).
    ///
    /// Discriminating: the peer here is a genuinely NEW `SharedTransport`
    /// (new boot salt, new session), and the delivery it receives is opened
    /// under the NEW session's key — which the OLD session's key would fail.
    #[test]
    fn a_peer_that_reestablishes_after_a_restart_is_re_keyed_immediately() {
        let mut h = crypto_harness();
        let self_pub = identity_public("restart-self-pub", T12_PRIV_SELF);

        // First life: handshake, then this node leads and mints.
        let acts = h.peer.initiate(1, h.peer.now_ns());
        let init = expect_send(&acts, DGRAM_KIND_HS_INIT);
        h.deliver_handshake(DGRAM_KIND_HS_INIT, &init);
        let resp = h.recv_kind(DGRAM_KIND_HS_RESP).expect("HS_RESP");
        h.peer.on_handshake_message(1, DGRAM_KIND_HS_RESP, &resp, h.peer.now_ns());
        h.drive_to_leader();
        let epoch = h.crypto_epoch().unwrap();
        let first = h.recv_kind(DGRAM_KIND_HS_KEY).expect("first-life delivery");
        let acts = h.peer.on_group_key_message(1, &first);
        let ack = expect_send(&acts, DGRAM_KIND_HS_KEY);
        h.deliver_handshake(DGRAM_KIND_HS_KEY, &ack);
        assert!(
            !h.h.cons.crypto.as_ref().unwrap().unacked_group_key_peers().contains(&0),
            "peer 0 has acked; the un-acked sweep will never name it again"
        );

        // The peer restarts: a brand-new process, new boot salt, new session.
        let peer_cfg =
            enabled_crypto_config("harness-peer-restarted", T12_PRIV_PEER, &[(1, self_pub)]);
        h.peer = SharedTransport::new(&peer_cfg, 0).unwrap().unwrap();
        h.peer_recv = h.peer.receive_half();
        let acts = h.peer.initiate(1, h.peer.now_ns());
        let init = expect_send(&acts, DGRAM_KIND_HS_INIT);
        h.deliver_handshake(DGRAM_KIND_HS_INIT, &init);
        let resp = h.recv_kind(DGRAM_KIND_HS_RESP).expect("HS_RESP after restart");
        h.peer.on_handshake_message(1, DGRAM_KIND_HS_RESP, &resp, h.peer.now_ns());
        h.h.cons.do_work();

        // A fresh `Established` for a peer that was already fully acked
        // produced a NEW `HS_KEY` on the wire. Asserted on the RAW datagram,
        // not an opened one: as responder the node parks the restarted
        // peer's session as `pending` (WireGuard-style — nothing has yet
        // PROVEN the peer adopted it), so `seal_pairwise` still uses the OLD
        // session's key here and the restarted peer cannot open THIS one.
        // The peer's own steady-state pairwise traffic promotes `pending`,
        // at which point `Peers::tick` re-announces
        // `Established { confirmed: true }` and this same path re-keys it
        // openably. What this test pins is the part that lives in THIS
        // layer: the re-delivery fires at all, which the un-acked sweep
        // alone would never do for an already-acked peer.
        let raw = h.recv_kind_raw(DGRAM_KIND_HS_KEY).expect("the restarted peer is re-keyed");
        assert_eq!(
            raw.len(),
            DATAGRAM_HEADER_LEN + 35 + uc_protocol::v2::crypto::CRYPTO_OVERHEAD,
            "a sealed 35-byte key delivery, not something else"
        );
        assert!(epoch > 0, "the epoch in force is a real minted one");
    }

    /// M7 runtime node-add: the `SocketAddr -> NodeId` map the receive seam
    /// resolves senders through must follow a committed config change, or the
    /// joiner's every datagram is dropped as `dropped_unknown_peer` until the
    /// whole cluster restarts — the case spec §5 exists to serve.
    #[test]
    fn a_committed_config_change_republishes_the_receivers_peer_id_map() {
        let mut h = crypto_harness();
        h.drive_to_leader();
        let before = h.h.cons.crypto_peer_ids().unwrap().snapshot();
        assert!(!before.values().any(|id| *id == 9), "node 9 is not a member yet");

        let mut c = h.h.cons.sm.config().clone();
        let addr: SocketAddr = "127.0.0.1:9109".parse().unwrap();
        c.learners.push((9, addr_to_pair(addr)));
        c.version += 1;
        h.h.cons.feed(Event::ConfigObserved { position: 42, config: c });

        // Synchronously, on the adoption itself — not one duty cycle later.
        let after = h.h.cons.crypto_peer_ids().unwrap().snapshot();
        assert_eq!(after.get(&addr), Some(&9), "the joiner is resolvable immediately");
    }

    /// Spec §5: "an operator drops in a key and `uc2ctl add-learner` works
    /// without restarting anything." Node 2 is a configured member whose
    /// public key is NOT in this node's allowlist at boot, so every
    /// handshake attempt toward it is refused. Appending its key to the
    /// allowlist file must be enough — no restart, no reconstruction.
    ///
    /// Discriminating: the FIRST half asserts refusals are actually
    /// happening (so the fixture reaches the case at all), and the second
    /// asserts an `HS_INIT` lands on node 2's real socket afterwards.
    /// Deleting the duty cycle's `allowlist_reload_if_stale` call fails the
    /// second half.
    ///
    /// Costs ~1s of wall clock by construction: `Allowlist::reload_if_stale`
    /// refuses to touch the disk more than once per second, and its clock is
    /// `SharedTransport`'s own `Instant`, which a test cannot advance.
    #[test]
    fn an_allowlist_edit_authorizes_a_new_peer_without_a_restart() {
        let mut h = crypto_harness();
        h.h.cons.do_work();
        assert!(
            h.h.cons.crypto_handshake_failures.load(Ordering::Relaxed) > 0,
            "node 2 starts unauthorized, so the initiate toward it is refused"
        );
        // Nothing HANDSHAKE-shaped goes to an unauthorized peer. The drain
        // filters by kind rather than asserting the socket is silent: T17
        // made every consensus datagram fail-closed too, so in practice
        // almost nothing reaches node 2 now (a `REQUEST_VOTE` toward it is
        // dropped as unsealable, counted in `crypto_seal_failures`) — but
        // "an HS_INIT must never appear" is the property this test is about,
        // and asserting silence would couple it to the unrelated question of
        // exactly which kinds the election path attempts this cycle.
        let mut probe = [0u8; 2048];
        while let Ok((n, _)) = h.peer2_sock.recv_from(&mut probe) {
            assert_ne!(
                read_datagram_header(&probe[..n]).kind,
                DGRAM_KIND_HS_INIT,
                "an unauthorized peer must not be handshaked with"
            );
        }

        // The operator drops node 2's key in.
        let peer2_pub = identity_public("allowlist-edit-peer2-pub", [0x37; 32]);
        let mut text = std::fs::read_to_string(&h.allowlist_path).unwrap();
        text.push_str(&format!("2 {}\n", b64_32(&peer2_pub)));
        std::fs::write(&h.allowlist_path, text).unwrap();

        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        loop {
            h.h.cons.do_work();
            if let Ok((n, _)) = h.peer2_sock.recv_from(&mut probe)
                && n >= DATAGRAM_HEADER_LEN
                && read_datagram_header(&probe[..n]).kind == DGRAM_KIND_HS_INIT
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the allowlist edit never took effect — a restart would have been required"
            );
            std::thread::yield_now();
        }
    }

    /// A handshake datagram from a source address this node has no `NodeId`
    /// for is counted and dropped — never fed to the handshake state machine
    /// under a guessed id.
    #[test]
    fn a_handshake_from_an_unmapped_address_is_dropped_and_counted() {
        let mut h = crypto_harness();
        h.h.cons.do_work(); // settle the boot-time `initiate` sweep first
        let failures = h.h.cons.crypto_handshake_failures.load(Ordering::Relaxed);
        let stranger: SocketAddr = "127.0.0.1:9199".parse().unwrap();
        h.h.hs_tx.try_send((stranger, DGRAM_KIND_HS_INIT, vec![0xAB; 116])).unwrap();
        h.h.cons.do_work();
        assert_eq!(h.h.cons.crypto_unresolved_peer.load(Ordering::Relaxed), 1);
        assert_eq!(
            h.h.cons.crypto_handshake_failures.load(Ordering::Relaxed),
            failures,
            "it never reached the handshake state machine at all"
        );
    }

    // ======================================================================
    // M8 Task 17: the node's OWN consensus sends
    // ======================================================================
    //
    // T10 sealed only what flows through `Sender::seal_scratch`. The
    // consensus agent emits `READ_PROBE`, `COMMIT_POSITION`, `TERM_MAP`,
    // `VOTE`, `REQUEST_VOTE` and the `CONFIG_*` pair on its OWN socket, and
    // T11's receive rule drops anything unsealed once crypto is on. Without
    // this task a crypto-enabled cluster has no elections, no commit gossip
    // and no linearizable reads — it does not run at all.

    impl CryptoHarness {
        /// A real Noise-IK session with peer 0, then leadership (which mints),
        /// then a real group-key delivery + ack — after this the peer can open
        /// BOTH scopes of the node's traffic. Factored out of the four T12
        /// tests that each spelled it inline.
        fn establish_and_key_peer(&mut self) {
            let acts = self.peer.initiate(1, self.peer.now_ns());
            let init = expect_send(&acts, DGRAM_KIND_HS_INIT);
            self.deliver_handshake(DGRAM_KIND_HS_INIT, &init);
            let resp = self.recv_kind(DGRAM_KIND_HS_RESP).expect("the node answered the HS_INIT");
            self.peer.on_handshake_message(1, DGRAM_KIND_HS_RESP, &resp, self.peer.now_ns());
            assert!(self.h.cons.crypto.as_ref().unwrap().is_established(0));

            self.drive_to_leader();
            let body = self.recv_kind(DGRAM_KIND_HS_KEY).expect("the new leader delivered a key");
            let acts = self.peer.on_group_key_message(1, &body);
            let ack = expect_send(&acts, DGRAM_KIND_HS_KEY);
            self.deliver_handshake(DGRAM_KIND_HS_KEY, &ack);

            // The node's own mint names BOTH configured peers (0 and 2), and
            // node 2 is deliberately unauthorized in this fixture, so it can
            // never ack — which leaves the epoch un-ACTIVATED until the full
            // `ACTIVATION_TIMEOUT_NS` (2 s) grace elapses, and
            // `GroupPlane::sealing_epoch` answers `None` until then. Re-mint
            // naming ONLY peer 0 so a single ack activates it at once, rather
            // than sleeping two real seconds in every group-scope test.
            // Delivered straight to the peer (the node's own HS_KEY seal/send
            // path is what the test above this one pins), and the ACK rides
            // back through the ordinary handshake route so the group plane
            // records it exactly as it would in production.
            let now = self.h.cons.crypto_now_ns();
            let (_epoch, acts) =
                self.h.cons.crypto.as_ref().unwrap().mint_group_key(&[0], now);
            for act in acts {
                let HandshakeAction::Send { body, .. } = act else {
                    panic!("a mint must emit a Send action")
                };
                let reply = self.peer.on_group_key_message(1, &body);
                let ack = expect_send(&reply, DGRAM_KIND_HS_KEY);
                self.deliver_handshake(DGRAM_KIND_HS_KEY, &ack);
            }
            assert!(
                self.h.cons.crypto.as_ref().unwrap().unacked_group_key_peers().is_empty(),
                "the re-minted epoch is fully acked, so it activates immediately"
            );
        }
    }

    /// Every kind the consensus agent emits, both scopes, opened on the
    /// peer's own `ReceiveHalf`.
    ///
    /// `recv_kind` opening the datagram IS the discriminating assertion, and
    /// it discriminates in every case: a CLEARTEXT `COMMIT_POSITION` (16
    /// bytes — `DATAGRAM_HEADER_LEN`, an empty body) or `VOTE`
    /// (16 + `VOTE_BODY_LEN` = 32) is shorter than the 40-byte minimum sealed
    /// frame (`DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD`) and comes back
    /// `TooShort`, while a cleartext `TERM_MAP`/`READ_PROBE` is long enough to
    /// *claim* to be sealed and comes back `AuthFailed`.
    /// The body checks below add the second half: the plaintext must not be
    /// findable in the wire bytes.
    #[test]
    fn every_consensus_datagram_the_node_emits_is_sealed() {
        let mut h = crypto_harness();
        h.establish_and_key_peer();

        // --- Scope::Group. Driven through `exec` rather than waited for on
        // the gossip cadence: the harness pins `gossip_floor_ns` to
        // `u64::MAX` (no idle re-gossip), so waiting would be a race on
        // whether a commit happened to advance this cycle.
        h.h.cons.exec(Action::GossipCommit { commit: 6016 }, &mut Vec::new());
        let commit_raw = h.recv_kind_raw(DGRAM_KIND_COMMIT_POSITION).expect("commit gossip");
        assert_ne!(
            read_datagram_header(&commit_raw).key_epoch,
            0,
            "a group-scope seal stamps the real epoch into the header"
        );
        let mut d = commit_raw.clone();
        let n = d.len();
        h.peer_recv
            .open_slice(1, &mut d, n)
            .expect("COMMIT_POSITION must open on the group path");

        // --- Scope::Pairwise, the term map that rides the same cadence.
        h.h.cons.exec(
            Action::ShipTermMap { entries: vec![(2, 0), (3, 6016)] },
            &mut Vec::new(),
        );
        let map_raw = h.recv_kind_raw(DGRAM_KIND_TERM_MAP).expect("term map");
        let mut d = map_raw.clone();
        let n = d.len();
        let len = h.peer_recv.open_slice(1, &mut d, n).expect("TERM_MAP must open pairwise");
        let map_body = d[DATAGRAM_HEADER_LEN..len].to_vec();
        assert!(!map_body.is_empty(), "a real term map, not an empty body");
        assert!(
            !map_raw.windows(map_body.len()).any(|w| w == map_body.as_slice()),
            "the term map's bytes must not be readable on the wire"
        );

        // --- Scope::Group, driven directly (a read round needs no client).
        h.h.cons.send_read_probe(0xABCD_1234);
        let probe = h.recv_kind(DGRAM_KIND_READ_PROBE).expect("READ_PROBE must open");
        assert_eq!(&probe[..8], &0xABCD_1234u64.to_le_bytes(), "the nonce survives the seal");

        // --- Scope::Pairwise, driven through `exec` exactly as the SM would.
        h.h.cons.exec(Action::SendVoteRejection { to: 0, term: 42 }, &mut Vec::new());
        let vote = h.recv_kind(DGRAM_KIND_VOTE).expect("VOTE must open");
        assert_eq!(vote.len(), VOTE_BODY_LEN);

        h.h.cons.exec(
            Action::StartElection { new_term: 43, last_term: 2, last_durable: 6016 },
            &mut Vec::new(),
        );
        let rv = h.recv_kind(DGRAM_KIND_REQUEST_VOTE).expect("REQUEST_VOTE must open");
        assert_eq!(rv.len(), REQUEST_VOTE_BODY_LEN);

        let peer_addr = h.peer_sock.local_addr().unwrap();
        h.h.cons.send_config_reply(
            peer_addr,
            &ConfigReplyBody { nonce: 77, status: 0, reason: 0, version: 5 },
        );
        let cr = h.recv_kind(DGRAM_KIND_CONFIG_REPLY).expect("CONFIG_REPLY must open");
        assert_eq!(cr.len(), CONFIG_REPLY_BODY_LEN);
    }

    /// T17 review, M4: a fan-out target with no address in the adopted config
    /// is SKIPPED — and counted. Before this it was skipped silently, and
    /// before T17 it was `self.id_to_addr[&id]`, which panicked the consensus
    /// agent outright.
    ///
    /// Deliberately driven with crypto OFF: this lookup is not crypto-gated
    /// (unlike `Consensus::send`'s), so the cleartext path is the one that
    /// would go uncounted if the increment sat inside a `crypto.is_some()`
    /// arm. Also asserts the RESOLVABLE targets in the same call still get
    /// their datagram, so "count it" cannot be satisfied by dropping the
    /// whole fan-out.
    #[test]
    fn a_fan_out_target_with_no_address_is_counted_not_silently_skipped() {
        // An EPHEMERAL port, overridden into node 0's slot in the member map
        // — never a hardcoded one, which would race any other test binary
        // cargo happens to run concurrently.
        let sink = UdpSocket::bind("127.0.0.1:0").unwrap();
        sink.set_nonblocking(true).unwrap();
        let mut h = harness_with_crypto(None, &[(0, sink.local_addr().unwrap())]);
        assert_eq!(h.cons.crypto_unresolved_peer.load(Ordering::Relaxed), 0);

        // Node 99 is in no config this harness ever adopted; node 0 is.
        h.cons.fan_out_group(&[0, 99], DGRAM_KIND_COMMIT_POSITION, 4096, 2, &[]);

        assert_eq!(
            h.cons.crypto_unresolved_peer.load(Ordering::Relaxed),
            1,
            "the unaddressable target must be counted, not dropped in silence"
        );
        let mut buf = [0u8; 2048];
        let (n, _) = sink.recv_from(&mut buf).expect("the RESOLVABLE target still got its datagram");
        assert_eq!(read_datagram_header(&buf[..n]).kind, DGRAM_KIND_COMMIT_POSITION);
        assert_eq!(read_datagram_header(&buf[..n]).position, 4096);
    }

    /// A group-scope kind is sealed ONCE and the identical bytes go to every
    /// peer — the reason group scope exists at all (spec §3), and the reason
    /// this had to reach `SharedTransport` rather than being sealed per
    /// destination inside `send`.
    ///
    /// Discriminating: per-destination sealing would draw a fresh counter for
    /// each peer, so the two datagrams would differ in their counter field
    /// even though everything else matched.
    #[test]
    fn a_group_scope_fan_out_seals_once_and_sends_identical_bytes_to_every_peer() {
        let mut h = crypto_harness();
        h.establish_and_key_peer();
        // Drain whatever is already queued, then force one fresh gossip.
        let mut sink = [0u8; 4096];
        while h.peer_sock.recv_from(&mut sink).is_ok() {}
        while h.peer2_sock.recv_from(&mut sink).is_ok() {}
        h.stash.clear();
        h.h.cons.exec(Action::GossipCommit { commit: 6016 }, &mut Vec::new());

        let a = h.recv_kind_raw(DGRAM_KIND_COMMIT_POSITION).expect("peer 0 got the gossip");
        let mut b = None;
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while Instant::now() < deadline && b.is_none() {
            if let Ok((n, _)) = h.peer2_sock.recv_from(&mut sink)
                && n >= DATAGRAM_HEADER_LEN
                && read_datagram_header(&sink[..n]).kind == DGRAM_KIND_COMMIT_POSITION
            {
                b = Some(sink[..n].to_vec());
            }
        }
        let b = b.expect("peer 2 got the gossip");
        assert_eq!(a, b, "byte-identical: sealed once, fanned out — not one seal per destination");
    }

    /// Fail-closed on the consensus plane: with no established session and no
    /// group key, every send is DROPPED and counted — never emitted in the
    /// clear, which is what a crypto-enabled cluster's peers would then have
    /// to accept for the cluster to work at all.
    #[test]
    fn a_consensus_send_that_cannot_be_sealed_is_dropped_and_counted() {
        let mut h = crypto_harness();
        // No handshake driven: no pairwise session, and this node has never
        // led so it holds no group key either.
        assert_eq!(h.h.cons.crypto_seal_failures(), 0);
        h.h.cons.exec(Action::SendVoteRejection { to: 0, term: 7 }, &mut Vec::new());
        h.h.cons.exec(Action::GossipCommit { commit: 6016 }, &mut Vec::new());
        assert!(
            h.h.cons.crypto_seal_failures() >= 2,
            "both the pairwise VOTE and the group gossip must be counted as dropped"
        );
        let mut buf = [0u8; 4096];
        while let Ok((n, _)) = h.peer_sock.recv_from(&mut buf) {
            let kind = read_datagram_header(&buf[..n]).kind;
            assert!(
                matches!(Transport::scope_of(kind), Scope::Unsealed),
                "only the handshake bootstrap kinds may leave this node unsealed, saw kind {kind}"
            );
        }
    }

    /// A garbage `HS_INIT` from a MAPPED peer reaches `Peers::on_message` and
    /// comes back as a refusal — counted, logged, never a panic. This is the
    /// node's first sight of attacker-controlled bytes.
    #[test]
    fn a_garbage_handshake_from_a_mapped_peer_is_refused_not_fatal() {
        let mut h = crypto_harness();
        h.h.cons.do_work(); // settle the boot-time `initiate` sweep first
        let failures = h.h.cons.crypto_handshake_failures.load(Ordering::Relaxed);
        let from = h.peer_sock.local_addr().unwrap();
        h.h.hs_tx.try_send((from, DGRAM_KIND_HS_INIT, vec![0xAB; 116])).unwrap();
        h.h.cons.do_work();
        assert!(
            h.h.cons.crypto_handshake_failures.load(Ordering::Relaxed) > failures,
            "a mapped peer's garbage HS_INIT reaches Peers::on_message and is refused"
        );
    }
}
