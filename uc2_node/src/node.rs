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
use uc2_log::cnc::{AdminReq, AdminResp, CncMeta, CncPage};
use uc2_log::counters::LogCounters;
use uc2_log::state::{ConfigRecord, NodeState, StoredConfig, StoredMember, TermMap, TermMapEntry, VoteRecord};
use uc2_net::TermHandle;
use uc2_net::fault::{FaultConfig, FaultSocket, PartitionHandle};
use uc2_net::receiver::{FollowerConfig, FollowerReceiver, NetEvent};
use uc2_net::sender::{CtrlMsg, Sender, SenderConfig};
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

use crate::ipc::InstanceDir;
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
    /// Drop whole journal blocks strictly below the block covering `below`
    /// (`Archive::purge_below`). No ack. Errors log-warn and drop.
    Purge { below: u64 },
    /// M6 Task 6: adopt `pos` as the archive floor WITHOUT bytes — the receiving
    /// side of a snapshot session (a learner) installed the state below `pos`
    /// from the shipped file, so the archive advances its frontier to `pos` and
    /// the counters prime there. No ack; a conflict logs + drops.
    AdoptFloor { pos: u64 },
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

/// Ingress queue depth (M5 replaces this with the client submit ring).
const INGRESS_CAPACITY: usize = 8192;
/// Consensus events drained per duty cycle (bounded work).
const NET_DRAIN_PER_CYCLE: usize = 4096;
/// Payloads appended per duty cycle (bounded work; plan §Task 8).
const INGRESS_PER_CYCLE: usize = 256;
/// NetEvent channel depth (T7 observability: a full channel counts a drop).
const NET_EVENT_CAPACITY: usize = 4096;
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
    /// Unique per read; scopes the READ_PROBE round so acks attribute correctly.
    nonce: u64,
    /// Read index: the commit position at admission. The read may only be
    /// answered once the service has applied at least this far.
    commit_at: u64,
    /// Distinct nodes that have confirmed this read's index (self seeded).
    ackers: Vec<NodeId>,
    /// Majority of the membership — the ack count that confirms the read index.
    quorum: usize,
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
    // Held for the node's life: the instance flock and the IPC ring mmaps.
    _instance: InstanceDir,
    _rings: Rings,
    agents: Vec<AgentRunner>,
}

impl Node {
    /// Recover state, prime counters, and spawn the four agents. Every node
    /// boots a FOLLOWER — leadership only ever comes from an election. Binds a
    /// fresh socket at `cfg.bind`; the harness variant is
    /// [`start_with_socket`](Self::start_with_socket).
    pub fn start(cfg: NodeConfig) -> io::Result<Node> {
        let sock = UdpSocket::bind(cfg.bind)?;
        Self::start_with_socket(cfg, sock)
    }

    /// As [`start`](Self::start) but over a pre-bound socket (the 3-node harness
    /// binds every node's socket first, then hands each in — so peers know all
    /// addresses before any agent runs).
    pub fn start_with_socket(cfg: NodeConfig, sock: UdpSocket) -> io::Result<Node> {
        let self_addr = sock.local_addr()?;

        // 1. flock FIRST — one node per instance dir. A contended lock (a live
        // node already owns this dir) surfaces as an io error whose Display
        // carries "AlreadyRunning" (the harness matches on it).
        let instance = InstanceDir::acquire(&cfg.instance_dir).map_err(to_io)?;

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
            ..ArchiveConfig::new(instance.journal_dir())
        };
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
        let prev_config = stored_to_cluster(&config_rec.prev);
        // M7 Task 6: mirror the recovered version onto the FRESH cnc page
        // immediately — same durable-then-mirror discipline as the snapshot
        // floor / output-progress markers just above (`cnc` is re-created on
        // every boot, so without this an attaching reader sees a stale `0` for
        // an entire duty cycle even when the recovered record is not genesis).
        cnc.store_config_version(config.version);

        // M7 Task 6: the snapshot-session config-carry cache — the encoded
        // CURRENT `ConfigRecord.config` (`v2::config::encode_config` bytes), read
        // by the sender's `SnapshotSource` closure at ship time and refreshed by
        // `Action::ConfigAdopted`'s exec arm on every adoption (forward, revert,
        // or boot re-derivation alike). Seeded here from the just-recovered
        // record so a snapshot shipped before the first live adoption still
        // carries real bytes rather than an empty placeholder.
        let mut config_wire_bytes = Vec::new();
        encode_config(&cluster_to_wire(&config, config_rec.prev_position), &mut config_wire_bytes);
        let config_bytes = Arc::new(Mutex::new(config_wire_bytes));

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
        // Seed the recovered PREV level (T4/T5): a no-op identity restore at
        // genesis (`prev == config`, both at position 0) — real content only
        // when a prior life actually adopted a config.
        sm.restore_prev_config(prev_config, config_rec.prev_position);
        let boot_term = sm.current_term();

        // Shared, consensus-thread-written role snapshots + the term handle.
        let term_handle: TermHandle = Arc::new(AtomicU32::new(boot_term));
        let leader_flag = Arc::new(AtomicBool::new(false));
        let can_serve_flag = Arc::new(AtomicBool::new(false));
        let intake_gate = Arc::new(AtomicBool::new(true)); // open until a term is adopted
        let truncations = Arc::new(AtomicU64::new(0));
        let wipes = Arc::new(AtomicU64::new(0));
        let reports_implausible = Arc::new(AtomicU64::new(0));

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

        // Sender (streams when leader; commit ranking is entirely the
        // consensus agent's job — the sender never ranks or gossips commit).
        let mut sender_cfg = SenderConfig::new(boot_term);
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
        let mut sender = Sender::with_learners(
            Arc::clone(&buffer),
            send_sock,
            sender_followers,
            &sender_learners,
            sender_cluster,
            ctrl_rx,
            sender_cfg,
            Arc::clone(&term_handle),
            Arc::clone(&leader_flag),
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
        let mut receiver = FollowerReceiver::new(
            Arc::clone(&buffer),
            recv_sock,
            rcfg,
            Arc::clone(&term_handle),
            net_tx,
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
        let route_drops = receiver.stats();

        // Archive agent: archive commands first (don't record blocks about to be
        // dropped/purged), then record, then ship data-stamped term observations.
        let arc_buffer = Arc::clone(&buffer);
        let arc_cnc = Arc::clone(&cnc);
        let arc_slot = trunc_slot.clone();
        let arc_first_base = Arc::clone(&archive_first_base);
        let arc_prime_gen = Arc::clone(&prime_generation);
        let archive_agent = AgentRunner::spawn("uc2-archive", IdleStrategy::Yield, move || {
            let mut did = false;
            while let Ok(cmd) = trunc_rx.try_recv() {
                match cmd {
                    ArchiveCmd::Truncate { epoch, to } => {
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
            for obs in archive.take_term_observations() {
                let _ = obs_tx.try_send(obs);
                did = true;
            }
            // M7: forward durably-recorded CONFIG-frame observations the same
            // way (position-ordered, one scan already did both in `do_work`
            // above via `Archive::observe_terms`).
            for obs in archive.take_config_observations() {
                let _ = cfg_obs_tx.try_send(obs);
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
            awaiting_reconcile: false,
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
            config_bytes: Arc::clone(&config_bytes),
            last_admin_seq: 0,
            pending_admin_fwd: None,
            last_config_reply: None,
        };
        let consensus_agent =
            AgentRunner::spawn("uc2-consensus", IdleStrategy::Yield, move || consensus.do_work())?;

        Ok(Node {
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
            archive_first_base,
            route_drops,
            sender_stats,
            partition_handles,
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

    /// M7 Task 6: the cnc-mirrored `ConfigRecord.config.version` — bumped by
    /// `Action::ConfigAdopted` (ordinary adoption) AND by the snapshot-install
    /// fiat path (`maybe_adopt_incoming_snapshot`). Exposed for tests asserting
    /// a joiner's config converges with the leader's after a snapshot install.
    pub fn config_version(&self) -> u64 {
        self.cnc.config_version()
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

struct Consensus {
    id: NodeId,
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
    /// Monotonic per-node read nonce (never reset — uniquely scopes each read's
    /// READ_PROBE round so acks attribute to the right pending read).
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
    /// M7 Task 6: the snapshot-session config-carry cache — refreshed with the
    /// newly-adopted config's encoded bytes on every `Action::ConfigAdopted`;
    /// read by the sender's `SnapshotSource` closure (a separate `Arc` clone) so
    /// every SNAP_BEGIN ships whatever config is CURRENT at ship time.
    config_bytes: Arc<Mutex<Vec<u8>>>,
    /// M7 Task 7: the last admin-request seq consumed off the cnc admin-req
    /// slot (`do_work` step 11's seqlock cursor into `read_admin_req`). `0` at
    /// boot — matches the freshly-zeroed cnc page (recreated every node boot),
    /// so an admin request from a prior life is never replayed.
    last_admin_seq: u64,
    /// M7 Task 7: this (follower) node's own in-flight forwarded proposal —
    /// `(seq, nonce)` of the admin request we forwarded to the leader as a
    /// kind-16 `ConfigProposal`. A 1-slot pending map (one admin request in
    /// flight at a time, per the cnc admin band's own single-slot discipline):
    /// cleared once the matching-nonce `NetEvent::ConfigReply` (kind 17)
    /// arrives and its status/reason/version is written back to the response
    /// line. `None` = no forward outstanding.
    pending_admin_fwd: Option<(u64, u64)>,
    /// M7 Task 7: leader-side nonce dedup — the last forwarded proposal's
    /// `(nonce, reply)` this node (as leader) answered. A repeat nonce (the
    /// follower's forward retried, or a genuine wire retry) gets the STORED
    /// reply re-sent rather than re-running `propose_config` a second time —
    /// idempotent under retry without relying on `ChangePending` to happen to
    /// refuse the repeat. `None` until the first forwarded proposal is handled.
    last_config_reply: Option<(u64, ConfigReplyBody)>,
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
        while let Ok((term, base)) = self.obs_rx.try_recv() {
            self.feed(Event::DataTermObserved { term, base });
            did = true;
        }

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
            self.feed(Event::ConfigObserved { position, config: wire_to_cluster_config(&wire) });
            did = true;
        }

        // 1d. Drain the truncation ack slot (a later cycle after emitting
        // `Truncate`). The infallible single slot holds at most one ack.
        if let Some((epoch, to)) = self.trunc_slot.take() {
            self.on_truncated(epoch, to);
            did = true;
        }

        // 2. Poll the durable counter; feed DurableAdvanced on change.
        let d = self.cnc.counters().durable.load_acquire();
        if d != self.durable_seen {
            self.durable_seen = d;
            self.feed(Event::DurableAdvanced { durable: d });
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
        // a ReadIndex barrier (nonce'd READ_PROBE to every follower) or are
        // redirected `MSG_V2_NOT_LEADER` while not serving. Bounded per cycle.
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
        did
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
        // Re-publish `id_and_role` for the (possibly changed) membership next
        // cycle; prune reported-durable entries for ids no longer in the band.
        self.peer_band_published = false;
        let live: Vec<NodeId> = self.peer_band.iter().map(|(id, _)| *id).collect();
        self.peer_reported.retain(|id, _| live.contains(id));
    }

    /// M7 Task 9: rebuild the sender's net layer (`CtrlMsg::SetPeers`) AND this
    /// node's own routing/observability (`rebuild_peer_maps` +
    /// `publish_peer_band`) for a newly-adopted `ClusterConfig`. Factored out of
    /// `Action::ConfigAdopted`'s exec arm so the snapshot-fiat install path in
    /// `maybe_adopt_incoming_snapshot` shares the IDENTICAL derivation — a
    /// below-floor joiner's installed config can differ from its boot seed
    /// (T7 shipped live reconfiguration), so it needs exactly this rebuild too,
    /// not a second hand-rolled copy that could drift from this one.
    fn rebuild_net_for_config(&mut self, config: &ClusterConfig) {
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
                self.rebuild_net_for_config(&cfg);
            }
            let _ = self.trunc_tx.try_send(ArchiveCmd::AdoptFloor { pos });
        }
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
    fn publish_status(&self) {
        let status = self.cnc.status();
        status.term.store_release(self.sm.current_term() as u64);
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
    fn gossip_targets(&self) -> Vec<NodeId> {
        self.peers.iter().chain(self.learner_ids.iter()).copied().collect()
    }

    fn send_read_probe(&mut self, nonce: u64) {
        let term = self.sm.current_term();
        let mut body = [0u8; READ_PROBE_BODY_LEN];
        write_read_probe_body(&mut body, &ReadProbeBody { nonce, from: self.id });
        for id in self.peers.clone() {
            let addr = self.id_to_addr[&id];
            self.send(addr, DGRAM_KIND_READ_PROBE, 0, term, &body);
        }
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
    /// pending read by nonce, and count DISTINCT ackers (a duplicate ack from the
    /// same node does not advance the count). On reaching quorum the read moves
    /// to `AwaitApplied`.
    fn on_read_probe_ack(&mut self, nonce: u64, from: NodeId) {
        // M6 Task 7 (the constraint block): the read quorum is over VOTERS only.
        // The probe loop already targets voting peers, but re-check membership here
        // so a learner's (or any non-voter's) ack can never complete a read quorum
        // — even a forged/misrouted one. `peers` is the voting set minus self.
        if !self.peers.contains(&from) {
            return;
        }
        for r in self.pending_reads.iter_mut() {
            if r.nonce == nonce && r.phase == ReadPhase::AwaitQuorum {
                if !r.ackers.contains(&from) {
                    r.ackers.push(from);
                }
                if r.ackers.len() >= r.quorum {
                    r.phase = ReadPhase::AwaitApplied;
                }
                return;
            }
        }
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
                    if !self.sm.can_serve() {
                        self.send_not_leader(client_id, local_seq);
                        continue;
                    }
                    let n = self.peers.len() + 1;
                    let quorum = n / 2 + 1;
                    let nonce = self.next_nonce;
                    self.next_nonce += 1;
                    let commit_at = self.cnc.counters().commit.load_acquire();
                    let deadline_ns = self.now_ns() + READ_BARRIER_TIMEOUT_NS;
                    let mut ackers = Vec::with_capacity(n);
                    ackers.push(self.id); // self-ack (acks: 1)
                    // Single-node (quorum 1): skip straight to AwaitApplied.
                    let phase = if ackers.len() >= quorum {
                        ReadPhase::AwaitApplied
                    } else {
                        ReadPhase::AwaitQuorum
                    };
                    let need_probe = phase == ReadPhase::AwaitQuorum;
                    self.pending_reads.push(PendingRead {
                        client_id,
                        local_seq,
                        query: buf,
                        nonce,
                        commit_at,
                        ackers,
                        quorum,
                        deadline_ns,
                        phase,
                    });
                    if need_probe {
                        self.send_read_probe(nonce);
                    }
                }
                Ok(None) => break,
                // Corrupt record: stop this cycle (retried at the same unread
                // position next cycle) — same posture as the ingress drain.
                Err(_) => break,
            }
        }
        did
    }

    /// Advance every in-flight linearizable read one step. A read past its
    /// deadline, or held while leadership was lost, is answered `MSG_V2_RETRY`
    /// and dropped. An `AwaitApplied` read whose service has caught up to
    /// `commit_at` — verified with the capture-recheck epoch bracket (task14
    /// TOCTOU close) — is forwarded to the service and dropped.
    fn advance_pending_reads(&mut self) -> bool {
        if self.pending_reads.is_empty() {
            return false;
        }
        let now = self.now_ns();
        let can_serve = self.sm.can_serve();
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
        did
    }

    /// Translate a wire NetEvent into an SM event and feed it. Unknown source
    /// addresses (not a configured member) are dropped.
    fn feed_net(&mut self, ev: NetEvent) {
        let event = match ev {
            NetEvent::Report { from, term, durable } => {
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
                Event::Report { from: id, term, durable }
            }
            NetEvent::CommitGossip { from, term, commit } => {
                self.learn_leader_hint(from, term);
                Event::CommitGossip { term, commit }
            }
            NetEvent::RequestVote { from, body } => {
                let Some(id) = self.addr_to_id.get(&from).copied() else { return };
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
    /// to whoever the leader hint names (kind 16), remembering `(seq, nonce)`
    /// so the eventual `NetEvent::ConfigReply` (kind 17) can be matched back to
    /// this response line; no hint (or the hint resolves to no known address,
    /// e.g. mid-election) -> reply `status=2` (retry) immediately — side-effect-
    /// free, `uc2ctl` just polls again.
    fn handle_admin(&mut self, req: AdminReq) {
        if matches!(self.sm.role(), Role::Leader) {
            let (status, reason, version) = self.propose_and_append(req.op, req.id, req.ip, req.port);
            self.write_admin_reply(req.seq, status, reason, version);
            return;
        }
        let hint = self.cnc.status().leader_hint.load_acquire();
        let leader_addr = (hint != u64::MAX)
            .then(|| self.id_to_addr.get(&(hint as NodeId)).copied())
            .flatten();
        let Some(leader_addr) = leader_addr else {
            self.write_admin_reply(req.seq, 2, 0, self.cnc.config_version());
            return;
        };
        // T7 review finding 2: a still-outstanding forward would otherwise be
        // silently overwritten, leaving its caller with a bare timeout and no
        // answer at all. Answer the superseded request with status=2 (retry)
        // before replacing the pending slot.
        if let Some((old_seq, _old_nonce)) = self.pending_admin_fwd.take() {
            eprintln!("uc2_node: admin forward superseded by newer request");
            self.write_admin_reply(old_seq, 2, 0, self.cnc.config_version());
        }
        self.pending_admin_fwd = Some((req.seq, req.nonce));
        let body = ConfigProposalBody { nonce: req.nonce, op: req.op, id: req.id, ip: req.ip, port: req.port };
        let mut buf = [0u8; CONFIG_PROPOSAL_BODY_LEN];
        write_config_proposal_body(&mut buf, &body);
        let term = self.sm.current_term();
        self.send(leader_addr, DGRAM_KIND_CONFIG_PROPOSAL, 0, term, &buf);
    }

    /// M7 Task 7: leader-only — decode the wire op fields, `propose_config`,
    /// and on `Ok` append + adopt-at-append (`append_config_frame`). Shared by
    /// the local admin-slot path and the network `ConfigProposal` forward path
    /// (one propose/append pipeline either way). Returns the wire reply triple
    /// `(status, reason, version)`:
    /// * `0, 0, new_version` — accepted.
    /// * `1, reason_code, current_version` — refused (`ProposeError`, or `6`/
    ///   `NotFound`'s code reused for a malformed/unknown op field — `uc2ctl`
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
            return (1, 6, self.cnc.config_version());
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
    /// `propose_config` call (retry-idempotent while the change is pending).
    fn on_config_proposal(&mut self, from: SocketAddr, body: ConfigProposalBody) {
        if !matches!(self.sm.role(), Role::Leader) {
            return;
        }
        if let Some((nonce, reply)) = &self.last_config_reply
            && *nonce == body.nonce
        {
            let reply = *reply;
            self.send_config_reply(from, &reply);
            return;
        }
        let (status, reason, version) = self.propose_and_append(body.op, body.id, body.ip, body.port);
        let reply = ConfigReplyBody { nonce: body.nonce, status, reason, version };
        self.last_config_reply = Some((body.nonce, reply));
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
        let Some((seq, nonce)) = self.pending_admin_fwd else { return };
        if nonce != body.nonce {
            return;
        }
        self.pending_admin_fwd = None;
        self.write_admin_reply(seq, body.status, body.reason, body.version);
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
        if let Some((seq, _nonce)) = self.pending_admin_fwd.take() {
            self.write_admin_reply(seq, 2, 0, self.cnc.config_version());
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
            {
                // Clean reconcile for the adopted term: reopen and clear the
                // awaiting-reconcile latch (M-3).
                self.awaiting_reconcile = false;
                self.open_gate();
            }
        }
    }

    fn exec(&mut self, act: Action, work: &mut Vec<Event>) {
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
                // persist durable; (b) collapse volatile via prime(base) — old
                // bytes above base must never be streamable; (c) fresh appender
                // AFTER prime; (d) append the NewTerm frame + feed it back; (e)
                // role flags.
                let map = to_entries(self.sm.term_map());
                self.state.store_term_map(&map).expect("term-map persist fail-stop");
                self.term_handle.store(term, Ordering::Release);
                // Explicit single-writer handoff (review hardening): the gate
                // is closed across the prime so a UDP-reordered straggler that
                // cleared the old term filter cannot race the counter reset.
                self.close_gate();
                self.cnc.counters().prime(base);
                let mut appender = Appender::new(Arc::clone(&self.buffer), term);
                appender.append_new_term().expect("NewTerm append fail-stop");
                // The serving gate compares COMMIT (an end/frontier position)
                // against this value, so it must be the frame's END — feeding
                // the start would flip can_serve before the NewTerm frame is
                // quorum-committed (at base 0: instantly). Raft §5.4.2.
                let end = appender.position();
                self.appender = Some(appender);
                work.push(Event::NewTermAppended { position: end });
                self.adopted_term = term;
                // A leader is the source of truth; no reconcile pending (M-3).
                self.awaiting_reconcile = false;
                self.open_gate();
                self.leader_flag.store(true, Ordering::Release);
                // We ARE the leader of this term (leader_hint published on the page).
                self.cnc.status().leader_hint.store_release(self.id as u64);
            }
            Action::BecomeFollower { term, .. } => {
                // T7 review finding 1: stepping down from leader (or adopting a
                // new term as follower) must not leave a stale nonce-dedup /
                // forward cache answerable by a later duplicate datagram.
                self.invalidate_admin_caches();
                self.term_handle.store(term, Ordering::Release);
                self.leader_flag.store(false, Ordering::Release);
                self.appender = None;
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
            }
            Action::AdvanceCommit { commit } => {
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
                for id in self.gossip_targets() {
                    let addr = self.id_to_addr[&id];
                    self.send(addr, DGRAM_KIND_COMMIT_POSITION, commit, term, &[]);
                }
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
                // M7 Task 6: refresh the snapshot-session config-carry cache so
                // every SNAP_BEGIN a session opens from here on ships THIS config
                // (over-delivery to a peer that doesn't need it is safe —
                // adopt-by-version idempotence on the receiving end).
                let mut wire_bytes = Vec::new();
                encode_config(&cluster_to_wire(&config, prev_position), &mut wire_bytes);
                *self.config_bytes.lock().unwrap() = wire_bytes;
                // Rebuild the net layer + this node's own routing/observability.
                // Shared with the snapshot-fiat install path in
                // `maybe_adopt_incoming_snapshot` (M7 Task 9) — one derivation for
                // "what changes when membership changes" everywhere it changes.
                self.rebuild_net_for_config(&config);
                self.cnc.store_config_version(config.version);
                // Cleared once commit crosses `position` (do_work step 11).
                self.cnc.store_config_pending(true);
            }
            Action::HaltRemoved => {
                // M7: this node is not a member of the just-adopted config (and
                // is not a leader mid-self-removal — that case keeps serving
                // until its own removal commits). Fail-stop: park permanently.
                eprintln!("node {}: removed from cluster config — halting", self.id);
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
                eprintln!("node {}: removed from cluster (self-removal committed) — halting", self.id);
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
    /// never cleared by step-down — it has no reason to be, nothing else
    /// reads it once halted) would leave an embedded caller's `is_leader()`/
    /// `can_serve()` reporting stale `true` forever after a real halt. A
    /// removed FOLLOWER's flags were already `false` here (only a LEADER
    /// reaches `StepDownRemoved`), so this is a no-op there — the fix is
    /// entirely about the self-removal leader case Task 8 introduces.
    fn halt(&mut self) {
        self.halt_removed = true;
        self.leader_flag.store(false, Ordering::Release);
        self.can_serve_flag.store(false, Ordering::Release);
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
    fn on_truncated(&mut self, epoch: u64, to: u64) {
        // The archive re-primed the counters to `to`; keep our shadow in step so
        // we don't refeed a spurious DurableAdvanced.
        self.durable_seen = to;
        let matching = self.pending_truncation == Some(epoch);
        self.feed(Event::Truncated { epoch, to });
        if matching {
            self.pending_truncation = None;
            self.truncations.fetch_add(1, Ordering::Relaxed);
            if !self.awaiting_reconcile {
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

    fn send(&mut self, to: SocketAddr, kind: u8, position: u64, term: u32, body: &[u8]) {
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN + body.len()];
        write_datagram_header(
            &mut d,
            &DatagramHeader { position, leadership_term_id: term, kind, flags: 0 },
        );
        d[DATAGRAM_HEADER_LEN..].copy_from_slice(body);
        let _ = self.sock.send_to(&d, to);
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
fn recover_config_record(
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
fn rederive_config(
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
            let addr: SocketAddr = format!("127.0.0.1:{}", 9100 + i).parse().unwrap();
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
        // Not asserted on by any test in this module — a dropped receiver just
        // makes `sender_ctrl.send` return an ignored `Err` (`exec`'s `let _ =`).
        let (sender_ctrl, _sender_ctrl_rx) = mpsc::sync_channel::<CtrlMsg>(64);

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
            config_bytes: Arc::new(Mutex::new(Vec::new())),
            last_admin_seq: 0,
            pending_admin_fwd: None,
            last_config_reply: None,
        };

        Harness {
            cons,
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
        h.cons.pending_admin_fwd = Some((99, 42));

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
        h.cons.feed_net(NetEvent::Report { from: addr0, term: 3, durable: 1 << 40 });
        assert_eq!(
            h.cons.cnc.counters().commit.load_acquire(),
            0,
            "implausible report manufactured a phantom commit on leader-only durability"
        );
        assert_eq!(h.cons.reports_implausible.load(Ordering::Relaxed), 1, "drop must be counted");

        // The drop poisoned nothing: a legitimate report (durable == append)
        // from the same follower ranks normally -> quorum {6048, 6048, 0} ->
        // commit advances to 6048.
        h.cons.feed_net(NetEvent::Report { from: addr0, term: 3, durable: append });
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
        h.cons.feed_net(NetEvent::Report { from: addr0, term: 7, durable: 1 << 40 });
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

    /// Drive the harness node (id 1, boot term 2) to a SERVING leader of term 3:
    /// election timeout → candidate; one peer grant → BecomeLeader (NewTerm frame
    /// at [6016, 6048)); then advance commit past the frame via a follower's
    /// durable Report, which opens `can_serve`. Returns the append frontier 6048.
    fn drive_to_serving_leader(h: &mut Harness) -> u64 {
        h.cons.feed(Event::Tick { now_ns: 301 });
        h.cons.feed(Event::Vote { from: 0, term: 3, granted: true });
        assert!(h.cons.leader_flag.load(Ordering::Acquire), "election did not complete");
        let append = h.cons.cnc.counters().append.load_acquire();
        assert_eq!(append, 6048);
        h.cons.feed(Event::DurableAdvanced { durable: append });
        let addr0: SocketAddr = "127.0.0.1:9100".parse().unwrap(); // member 0
        h.cons.feed_net(NetEvent::Report { from: addr0, term: 3, durable: append });
        assert!(h.cons.sm.can_serve(), "commit did not open the serving gate");
        append
    }

    /// Push a linearizable read into the barrier for the harness node.
    fn mk_read(cons: &Consensus, commit_at: u64, quorum: usize, deadline_ns: u64) -> PendingRead {
        PendingRead {
            client_id: 7,
            local_seq: 1,
            query: Vec::new(),
            nonce: 42,
            commit_at,
            ackers: vec![cons.id], // self-ack (acks: 1)
            quorum,
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
        // service_applied so the forward waits for catch-up.
        let far = h.cons.now_ns() + 10_000_000_000;
        let read = mk_read(&h.cons, /*commit_at*/ 6048, /*quorum*/ 3, far);
        h.cons.pending_reads.push(read);

        // A non-member ack (id 99 is not in [0,1,2]) is dropped by the
        // membership check.
        h.cons.on_read_probe_ack(42, 99);
        assert_eq!(h.cons.pending_reads[0].ackers, vec![1]);

        // Distinct acker 0, then a DUPLICATE 0 that must not advance the count.
        h.cons.on_read_probe_ack(42, 0);
        h.cons.on_read_probe_ack(42, 0);
        assert_eq!(h.cons.pending_reads[0].ackers, vec![1, 0]);
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitQuorum);

        // The second distinct acker reaches quorum 3 → AwaitApplied.
        h.cons.on_read_probe_ack(42, 2);
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitApplied);

        // Service not yet caught up (applied 0 < commit_at 6048): the read stays
        // parked, never forwarded.
        assert!(!h.cons.advance_pending_reads());
        assert_eq!(h.cons.pending_reads.len(), 1);

        // Service catches up (applied >= commit_at) BUT no service has attached
        // this generation yet (service_epoch still 0) — the sentinel-collision
        // guard (IMPORTANT #1) treats a captured epoch of 0 as NOT ready, so the
        // read stays parked, never forwarded on the skip-the-check sentinel.
        h.cons.cnc.service().service_applied.store_release(6048);
        assert!(!h.cons.advance_pending_reads());
        assert_eq!(h.cons.pending_reads.len(), 1, "epoch-0 must not forward");

        // A real incarnation attaches (epoch 1). NOW the capture-recheck bracket
        // passes with a live epoch → forwarded to svc_query and dropped.
        h.cons.cnc.service().service_epoch.store_release(1);
        assert!(h.cons.advance_pending_reads());
        assert!(h.cons.pending_reads.is_empty(), "caught-up read must forward and drop");
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
        let mut read = mk_read(&h.cons, /*commit_at*/ 6048, /*quorum*/ 1, far);
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
        let read = mk_read(&h.cons, 0, 2, 0);
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
        let read = mk_read(&h.cons, 0, 2, far); // deadline far off; only depose fires
        h.cons.pending_reads.push(read);
        assert!(h.cons.advance_pending_reads());
        assert!(h.cons.pending_reads.is_empty(), "a non-serving node retries in-flight reads");
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
}
