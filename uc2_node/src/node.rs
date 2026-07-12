// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Node composition + the consensus agent (Task 8).

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Instant, SystemTime};

use uc2_consensus::election::{Action, ElectionConfig, ElectionSm, Event, NodeId};
use uc2_log::agent::{AgentRunner, IdleStrategy};
use uc2_log::archive::{Archive, ArchiveConfig};
use uc2_log::buffer::{AppendError, Appender, LogBuffer};
use uc2_log::cnc::{CncMeta, CncPage};
use uc2_log::counters::LogCounters;
use uc2_log::state::{NodeState, TermMap, TermMapEntry, VoteRecord};
use uc2_net::TermHandle;
use uc2_net::fault::{FaultConfig, FaultSocket, PartitionHandle};
use uc2_net::receiver::{FollowerConfig, FollowerReceiver, NetEvent};
use uc2_net::sender::{CtrlMsg, Sender, SenderConfig};
use uc_protocol::ring::{
    BroadcastProducer, BroadcastRing, MpscConsumer, MpscRing, SpscProducer, SpscRing,
};
use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};
use uc_protocol::v2::ipc::{
    FLAG_V2_LINEARIZABLE, MSG_V2_NOT_LEADER, MSG_V2_RETRY, MSG_V2_SVC_QUERY, client_from_extra,
    extra_client,
};

use crate::ipc::InstanceDir;
use uc2_log::buffer::FrameRead;
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_COMMIT_POSITION, DGRAM_KIND_READ_PROBE,
    DGRAM_KIND_READ_PROBE_ACK, DGRAM_KIND_REQUEST_VOTE, DGRAM_KIND_TERM_MAP, DGRAM_KIND_VOTE,
    DatagramHeader, MAX_TERM_MAP_WIRE_ENTRIES, READ_PROBE_BODY_LEN, REQUEST_VOTE_BODY_LEN,
    ReadProbeBody, RequestVoteBody, TERM_MAP_ENTRY_LEN, TERM_MAP_HEADER_LEN, TermMapEntryWire,
    VOTE_BODY_LEN, VoteBody, write_datagram_header, write_read_probe_body, write_request_vote_body,
    write_term_map_body, write_vote_body,
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

/// Static-membership node configuration (M4: no discovery, no reconfiguration).
pub struct NodeConfig {
    pub id: NodeId,
    /// Every member INCLUDING self, as `(id, addr)`.
    pub members: Vec<(NodeId, SocketAddr)>,
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
    reports_implausible: Arc<AtomicU64>,
    /// M6 Task 4: node-internal mirror of the archive's lowest replayable
    /// position (written by the archive agent). Exposed via
    /// [`Node::archive_first_base`] for purge-safety tests.
    archive_first_base: Arc<AtomicU64>,
    route_drops: Arc<uc2_net::receiver::FollowerStats>,
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

        // Election SM over the recovered credentials.
        let members_ids: Vec<NodeId> = cfg.members.iter().map(|(id, _)| *id).collect();
        let recovered_vote = state.vote().map(|v| (v.term, v.voted_for));
        let sm = ElectionSm::new(
            ElectionConfig {
                id: cfg.id,
                members: members_ids.clone(),
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
        let boot_term = sm.current_term();

        // Shared, consensus-thread-written role snapshots + the term handle.
        let term_handle: TermHandle = Arc::new(AtomicU32::new(boot_term));
        let leader_flag = Arc::new(AtomicBool::new(false));
        let can_serve_flag = Arc::new(AtomicBool::new(false));
        let intake_gate = Arc::new(AtomicBool::new(true)); // open until a term is adopted
        let truncations = Arc::new(AtomicU64::new(0));
        let reports_implausible = Arc::new(AtomicU64::new(0));

        // Peer maps and the follower set.
        let mut id_to_addr = HashMap::new();
        let mut addr_to_id = HashMap::new();
        for (id, addr) in &cfg.members {
            id_to_addr.insert(*id, *addr);
            addr_to_id.insert(*addr, *id);
        }
        let peers: Vec<NodeId> = members_ids.iter().copied().filter(|id| *id != cfg.id).collect();
        let followers: Vec<SocketAddr> =
            peers.iter().map(|id| id_to_addr[id]).collect();

        // Channels.
        let (net_tx, net_rx) = mpsc::sync_channel::<NetEvent>(NET_EVENT_CAPACITY);
        let (ctrl_tx, ctrl_rx) = mpsc::sync_channel::<CtrlMsg>(1024);
        let (ingress_tx, ingress_rx) = mpsc::sync_channel::<Vec<u8>>(INGRESS_CAPACITY);
        let (obs_tx, obs_rx) = mpsc::sync_channel::<(u32, u64)>(1024);
        // Truncation command channel carries `(epoch, to)`; the ack rides an
        // infallible single slot (one truncation in flight — the SM latch).
        let (trunc_tx, trunc_rx) = mpsc::sync_channel::<ArchiveCmd>(64);
        let trunc_slot = TruncationSlot::default();

        // Sender (streams when leader; commit ranking is entirely the
        // consensus agent's job — the sender never ranks or gossips commit).
        let mut sender_cfg = SenderConfig::new(boot_term);
        sender_cfg.heartbeat_ns = 20_000_000; // 20 ms: brisk tail-loss detection
        let journal = archive.journal_arc();
        let mut sender = Sender::new(
            Arc::clone(&buffer),
            send_sock,
            followers,
            cfg.members.len(),
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
        // Offer ONLY the file at the node's durable floor: a session ships a
        // fully-published artifact (rename-atomic + validated as the floor marker).
        let src_cnc = Arc::clone(&cnc);
        let src_dir = snap_dir.clone();
        sender.set_snapshot_source(Arc::new(move || {
            let floor = src_cnc.snapshots().node_snapshot_floor.load_acquire();
            if floor == 0 {
                return None;
            }
            let path = src_dir.join(format!("snap-{floor}.ultsnap"));
            let len = std::fs::metadata(&path).ok()?.len();
            Some((floor, path, len))
        }));

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
        receiver.set_sender_route(ctrl_tx);
        receiver.set_intake_gate(Arc::clone(&intake_gate));
        receiver.set_snapshot_intake(snap_dir.clone(), Some(Arc::clone(&incoming_snapshot)));
        let route_drops = receiver.stats();

        // Archive agent: archive commands first (don't record blocks about to be
        // dropped/purged), then record, then ship data-stamped term observations.
        let arc_buffer = Arc::clone(&buffer);
        let arc_cnc = Arc::clone(&cnc);
        let arc_slot = trunc_slot.clone();
        let arc_first_base = Arc::clone(&archive_first_base);
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
            did
        })?;

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
            net_rx,
            obs_rx,
            ingress_rx,
            trunc_tx,
            trunc_slot,
            term_handle: Arc::clone(&term_handle),
            leader_flag: Arc::clone(&leader_flag),
            can_serve_flag: Arc::clone(&can_serve_flag),
            intake_gate: Arc::clone(&intake_gate),
            truncations: Arc::clone(&truncations),
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
            adopted_incoming: 0,
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
            reports_implausible,
            archive_first_base,
            route_drops,
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

    /// M6 Task 4: the archive's lowest still-replayable position (the purge
    /// floor's realized value). `0` when nothing has been purged. Exposed for
    /// purge-safety tests: after the service publishes a snapshot and the purge
    /// driver runs, this advances to at most the snapshot floor.
    pub fn archive_first_base(&self) -> u64 {
        self.archive_first_base.load(Ordering::Acquire)
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
    /// RequestVote, Vote, TermMap, LeaderActivity, ReadProbe, ReadProbeAck).
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
    peers: Vec<NodeId>,
    net_rx: mpsc::Receiver<NetEvent>,
    obs_rx: mpsc::Receiver<(u32, u64)>,
    ingress_rx: mpsc::Receiver<Vec<u8>>,
    trunc_tx: mpsc::SyncSender<ArchiveCmd>,
    trunc_slot: TruncationSlot,
    term_handle: TermHandle,
    leader_flag: Arc<AtomicBool>,
    can_serve_flag: Arc<AtomicBool>,
    intake_gate: Arc<AtomicBool>,
    truncations: Arc<AtomicU64>,
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
    /// M6 Task 6: last inbound-snapshot position already adopted (shadow, so the
    /// AdoptFloor command + cnc mirror fire once per completed transfer).
    adopted_incoming: u64,
}

impl Consensus {
    /// One consensus duty cycle (binding order, plan §Task 8).
    fn do_work(&mut self) -> bool {
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

        // 1c. Drain the truncation ack slot (a later cycle after emitting
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
        // transitions; keep can_serve fresh every cycle).
        self.can_serve_flag.store(self.sm.can_serve(), Ordering::Release);

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
        did
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
    fn publish_status(&self) {
        let status = self.cnc.status();
        status.term.store_release(self.sm.current_term() as u64);
        let mut flags = 0u64;
        if self.leader_flag.load(Ordering::Relaxed) {
            flags |= NODE_FLAG_LEADER;
        }
        if self.sm.can_serve() {
            flags |= NODE_FLAG_CAN_SERVE;
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
        if !self.id_to_addr.contains_key(&from) {
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
                Event::TermMapReceived {
                    term,
                    entries: entries.iter().map(|e| (e.term, e.base)).collect(),
                }
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
                for id in self.peers.clone() {
                    let addr = self.id_to_addr[&id];
                    self.send(addr, DGRAM_KIND_COMMIT_POSITION, commit, term, &[]);
                }
            }
            Action::ShipTermMap { entries } => {
                let term = self.sm.current_term();
                let body = encode_term_map(&entries);
                for id in self.peers.clone() {
                    let addr = self.id_to_addr[&id];
                    self.send(addr, DGRAM_KIND_TERM_MAP, 0, term, &body);
                }
            }
            Action::Truncate { epoch, to, new_map } => {
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
        let members = vec![0u32, 1, 2];
        let sm = ElectionSm::new(
            ElectionConfig {
                id: 1,
                members: members.clone(),
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

        let mut id_to_addr = HashMap::new();
        let mut addr_to_id = HashMap::new();
        for (i, id) in members.iter().enumerate() {
            let addr: SocketAddr = format!("127.0.0.1:{}", 9100 + i).parse().unwrap();
            id_to_addr.insert(*id, addr);
            addr_to_id.insert(addr, *id);
        }
        let peers = vec![0u32, 2];

        let (net_tx, net_rx) = mpsc::sync_channel::<NetEvent>(64);
        let (obs_tx, obs_rx) = mpsc::sync_channel::<(u32, u64)>(64);
        let (ingress_tx, ingress_rx) = mpsc::sync_channel::<Vec<u8>>(64);
        let (trunc_tx, trunc_rx) = mpsc::sync_channel::<ArchiveCmd>(64);
        let trunc_slot = TruncationSlot::default();

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
            net_rx,
            obs_rx,
            ingress_rx,
            trunc_tx,
            trunc_slot,
            term_handle: Arc::new(AtomicU32::new(boot_term)),
            leader_flag: Arc::new(AtomicBool::new(false)),
            can_serve_flag: Arc::new(AtomicBool::new(false)),
            intake_gate,
            truncations: Arc::new(AtomicU64::new(0)),
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
            adopted_incoming: 0,
        };

        Harness {
            cons,
            _net_tx: net_tx,
            _obs_tx: obs_tx,
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
}
