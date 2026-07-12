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
use uc_protocol::ring::{BroadcastRing, MpscRing, SpscRing};
use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};

use crate::ipc::InstanceDir;
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_COMMIT_POSITION, DGRAM_KIND_REQUEST_VOTE, DGRAM_KIND_TERM_MAP,
    DGRAM_KIND_VOTE, DatagramHeader, MAX_TERM_MAP_WIRE_ENTRIES, REQUEST_VOTE_BODY_LEN,
    RequestVoteBody, TERM_MAP_ENTRY_LEN, TERM_MAP_HEADER_LEN, TermMapEntryWire, VOTE_BODY_LEN,
    VoteBody, write_datagram_header, write_request_vote_body, write_term_map_body, write_vote_body,
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
}

/// Why a `submit` was refused. Leader-only ingress (M5 replaces this with the
/// client ring); a non-serving node or a saturated ingress queue rejects.
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

/// The node's shared-memory IPC rings (spec §7), created fresh at every boot.
/// Held for the node's life so the mmap'd files stay live for attaching
/// clients/service. Wired to dispatch agents in later M5 tasks — currently the
/// node is only the creator, so the handles are held but not yet polled.
#[allow(dead_code)]
struct Rings {
    ingress: MpscRing,
    query: MpscRing,
    svc_query: SpscRing,
    egress_service: BroadcastRing,
    egress_node: BroadcastRing,
}

pub struct Node {
    cnc: Arc<CncPage>,
    term_handle: TermHandle,
    leader_flag: Arc<AtomicBool>,
    can_serve_flag: Arc<AtomicBool>,
    ingress_tx: mpsc::SyncSender<Vec<u8>>,
    truncations: Arc<AtomicU64>,
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
        let mut archive =
            Archive::open(ArchiveConfig::new(instance.journal_dir())).map_err(to_io)?;
        let durable = archive.recovered_position();
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

        // 5. Durable output-progress marker → mirror onto the page for attaching
        // parties. There is no persisted marker until the output loop lands (a
        // later M5 task); the page's zeroed default (0) is the correct boot
        // value, published explicitly here as the mirror point.
        let output_progress = 0u64;
        cnc.status().output_progress.store_release(output_progress);

        // 6. Rings created fresh each boot (stale files unlinked first — any
        // prior attachment is invalidated by the new instance_id anyway).
        let rings = create_rings(&instance)?;

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
        let (trunc_tx, trunc_rx) = mpsc::sync_channel::<(u64, u64)>(64);
        let trunc_slot = TruncationSlot::default();

        // Sender (streams when leader; node-mode disables its own ranking).
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
        );
        sender.set_node_mode();
        sender.set_role_flag(Arc::clone(&leader_flag));
        sender.set_replay_source(journal);

        // Receiver (unified follower-receiver in node mode).
        let mut rcfg = FollowerConfig::new(self_addr); // auto-learns the real leader from DATA
        rcfg.seed = cfg.seed ^ 0x5DEE_CE66_1D0C_2A11;
        rcfg.status_floor_ns = 20_000_000;
        rcfg.append_pos_floor_ns = 20_000_000;
        let mut receiver =
            FollowerReceiver::new(Arc::clone(&buffer), recv_sock, rcfg, Arc::clone(&term_handle));
        receiver.set_consensus_route(net_tx);
        receiver.set_sender_route(ctrl_tx);
        receiver.set_intake_gate(Arc::clone(&intake_gate));
        let route_drops = receiver.stats();

        // Archive agent: truncate commands first (don't record blocks about to be
        // dropped), then record, then ship data-stamped term observations.
        let arc_buffer = Arc::clone(&buffer);
        let arc_cnc = Arc::clone(&cnc);
        let arc_slot = trunc_slot.clone();
        let archive_agent = AgentRunner::spawn("uc2-archive", IdleStrategy::Yield, move || {
            let mut did = false;
            while let Ok((epoch, to)) = trunc_rx.try_recv() {
                // First-block cuts (a contested first election, `to` at/inside
                // block 0) are handled by the archive via `Journal::truncate_all`
                // + prefix re-seed (M4 carry #3) and no longer fail-stop. Any
                // remaining error is a genuine journal I/O fault — still fatal.
                archive.truncate_to(to).expect("archive truncate fail-stop (journal I/O)");
                arc_cnc.counters().prime(to);
                // Infallible ack: a single slot cannot drop (one in flight).
                arc_slot.post(epoch, to);
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
            base: Instant::now(),
            durable_seen: durable,
            adopted_term: boot_term,
            awaiting_reconcile: false,
            pending_truncation: None,
        };
        let consensus_agent =
            AgentRunner::spawn("uc2-consensus", IdleStrategy::Yield, move || consensus.do_work())?;

        Ok(Node {
            cnc,
            term_handle,
            leader_flag,
            can_serve_flag,
            ingress_tx,
            truncations,
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

    /// Leader-only ingress: enqueue a payload for the consensus agent to append
    /// (M5 replaces this with the client submit ring). Refused unless the node
    /// is a serving leader; `Full` when the bounded queue is saturated.
    pub fn submit(&self, payload: Vec<u8>) -> Result<(), SubmitError> {
        if !self.can_serve() {
            return Err(SubmitError::NotServing);
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

    /// Consensus events dropped because the NetEvent channel was full (T7
    /// observability), summed across kinds. Safe drops — votes/reports/gossip
    /// re-fire on their cadence — but a rising count signals a wedged consensus
    /// agent. Use [`net_event_drops_by_kind`](Self::net_event_drops_by_kind) to
    /// attribute the drops to a specific traffic class.
    pub fn net_event_drops(&self) -> u64 {
        self.route_drops.net_drops.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    /// Per-kind consensus-event drop counts, indexed by
    /// [`uc2_net::receiver::NetEvent::kind_idx`]
    /// (Report, CommitGossip, RequestVote, Vote, TermMap, LeaderActivity).
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
    sock: FaultSocket,
    id_to_addr: HashMap<NodeId, SocketAddr>,
    addr_to_id: HashMap<SocketAddr, NodeId>,
    peers: Vec<NodeId>,
    net_rx: mpsc::Receiver<NetEvent>,
    obs_rx: mpsc::Receiver<(u32, u64)>,
    ingress_rx: mpsc::Receiver<Vec<u8>>,
    trunc_tx: mpsc::SyncSender<(u64, u64)>,
    trunc_slot: TruncationSlot,
    term_handle: TermHandle,
    leader_flag: Arc<AtomicBool>,
    can_serve_flag: Arc<AtomicBool>,
    intake_gate: Arc<AtomicBool>,
    truncations: Arc<AtomicU64>,
    base: Instant,
    durable_seen: u64,
    adopted_term: u32,
    awaiting_reconcile: bool,
    /// The epoch of the truncation currently in flight (emit→ack bracket). `Some`
    /// from `Action::Truncate` exec until the matching slot ack; the intake-gate
    /// reopen discipline uses it to know a truncation is pending.
    pending_truncation: Option<u64>,
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

        // 3. Drain the ingress queue (leader && serving only), bounded.
        if self.leader_flag.load(Ordering::Relaxed) && self.sm.can_serve() {
            did |= self.drain_ingress();
        }

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

    /// Translate a wire NetEvent into an SM event and feed it. Unknown source
    /// addresses (not a configured member) are dropped.
    fn feed_net(&mut self, ev: NetEvent) {
        let event = match ev {
            NetEvent::Report { from, term, durable } => {
                let Some(id) = self.addr_to_id.get(&from).copied() else { return };
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
                // The ONLY commit store in the binary (both roles).
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
                self.trunc_tx.send((epoch, to)).expect("archive channel closed");
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
fn create_rings(dir: &InstanceDir) -> io::Result<Rings> {
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
    Ok(Rings {
        ingress: MpscRing::create(&dir.ingress_ring(), 4 * MIB, MAX_MSG).map_err(to_io)?,
        query: MpscRing::create(&dir.query_ring(), MIB, MAX_MSG).map_err(to_io)?,
        svc_query: SpscRing::create(&dir.svc_query_ring(), MIB, MAX_MSG).map_err(to_io)?,
        egress_service: BroadcastRing::create(&dir.egress_service(), 4 * MIB, MAX_MSG)
            .map_err(to_io)?,
        egress_node: BroadcastRing::create(&dir.egress_node(), 4 * MIB, MAX_MSG).map_err(to_io)?,
    })
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
        _trunc_rx: mpsc::Receiver<(u64, u64)>,
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
        cnc.counters().prime(6000);
        let state = NodeState::open(dir.path()).unwrap();

        // id=1 in [0,1,2]; own map (1,0),(2,4096) at durable 6000 → boot_term 2.
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
            6000,
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
        let (trunc_tx, trunc_rx) = mpsc::sync_channel::<(u64, u64)>(64);
        let trunc_slot = TruncationSlot::default();

        let sock = FaultSocket::from_socket(UdpSocket::bind("127.0.0.1:0").unwrap()).unwrap();
        let intake_gate = Arc::new(AtomicBool::new(true));

        let cons = Consensus {
            id: 1,
            sm,
            state,
            cnc: Arc::clone(&cnc),
            buffer,
            appender: None,
            next_corr: 0,
            pending_ingress: None,
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
            base: Instant::now(),
            durable_seen: 6000,
            adopted_term: boot_term,
            awaiting_reconcile: false,
            pending_truncation: None,
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
        assert_eq!(h._trunc_rx.try_recv().ok(), Some((epoch, 4096)));

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
}
