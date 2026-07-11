// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Node composition + the consensus agent (Task 8).

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Instant;

use uc2_consensus::election::{Action, ElectionConfig, ElectionSm, Event, NodeId};
use uc2_log::agent::{AgentRunner, IdleStrategy};
use uc2_log::archive::{Archive, ArchiveConfig};
use uc2_log::buffer::{AppendError, Appender, LogBuffer};
use uc2_log::counters::LogCounters;
use uc2_log::region::Region;
use uc2_log::state::{NodeState, TermMap, TermMapEntry, VoteRecord};
use uc2_net::TermHandle;
use uc2_net::fault::{FaultConfig, FaultSocket, PartitionHandle};
use uc2_net::receiver::{FollowerConfig, FollowerReceiver, NetEvent};
use uc2_net::sender::{CtrlMsg, Sender, SenderConfig};
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_COMMIT_POSITION, DGRAM_KIND_REQUEST_VOTE, DGRAM_KIND_TERM_MAP,
    DGRAM_KIND_VOTE, DatagramHeader, MAX_TERM_MAP_WIRE_ENTRIES, REQUEST_VOTE_BODY_LEN,
    RequestVoteBody, TERM_MAP_ENTRY_LEN, TERM_MAP_HEADER_LEN, TermMapEntryWire, VOTE_BODY_LEN,
    VoteBody, write_datagram_header, write_request_vote_body, write_term_map_body, write_vote_body,
};

/// Journal + node-state directories for one node.
#[derive(Debug, Clone)]
pub struct NodeDirs {
    pub journal: PathBuf,
    pub state: PathBuf,
}

/// Static-membership node configuration (M4: no discovery, no reconfiguration).
pub struct NodeConfig {
    pub id: NodeId,
    /// Every member INCLUDING self, as `(id, addr)`.
    pub members: Vec<(NodeId, SocketAddr)>,
    pub bind: SocketAddr,
    pub dirs: NodeDirs,
    /// Ring capacity in bytes; power of two.
    pub buffer_bytes: usize,
    pub max_payload: usize,
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

pub struct Node {
    counters: Arc<LogCounters>,
    term_handle: TermHandle,
    leader_flag: Arc<AtomicBool>,
    can_serve_flag: Arc<AtomicBool>,
    ingress_tx: mpsc::SyncSender<Vec<u8>>,
    truncations: Arc<AtomicU64>,
    route_drops: Arc<uc2_net::receiver::FollowerStats>,
    partition_handles: Vec<PartitionHandle>,
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

        // Recover durable state.
        std::fs::create_dir_all(&cfg.dirs.journal)?;
        std::fs::create_dir_all(&cfg.dirs.state)?;
        let state = NodeState::open(&cfg.dirs.state).map_err(to_io)?;
        let mut archive =
            Archive::open(ArchiveConfig::new(&cfg.dirs.journal)).map_err(to_io)?;
        let durable = archive.recovered_position();

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

        // The shared ring + counters, primed to the recovery point.
        let counters = Arc::new(LogCounters::new());
        let buffer = Arc::new(LogBuffer::new(
            Region::heap_zeroed(cfg.buffer_bytes),
            Arc::clone(&counters),
            cfg.max_payload,
        ));
        counters.prime(durable);

        // Election SM over the recovered credentials.
        let members_ids: Vec<NodeId> = cfg.members.iter().map(|(id, _)| *id).collect();
        let recovered_vote = state.vote().map(|v| (v.term, v.voted_for));
        let sm = ElectionSm::new(
            ElectionConfig {
                id: cfg.id,
                members: members_ids.clone(),
                election_timeout_min_ns: cfg.election_timeout_min_ns,
                election_timeout_max_ns: cfg.election_timeout_max_ns,
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
        let (trunc_tx, trunc_rx) = mpsc::sync_channel::<u64>(64);
        let (ack_tx, ack_rx) = mpsc::sync_channel::<u64>(64);

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
        let arc_counters = Arc::clone(&counters);
        let archive_agent = AgentRunner::spawn("uc2-archive", IdleStrategy::Yield, move || {
            let mut did = false;
            while let Ok(to) = trunc_rx.try_recv() {
                archive.truncate_to(to).expect("archive truncate fail-stop");
                arc_counters.prime(to);
                let _ = ack_tx.try_send(to);
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
            counters: Arc::clone(&counters),
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
            ack_rx,
            term_handle: Arc::clone(&term_handle),
            leader_flag: Arc::clone(&leader_flag),
            can_serve_flag: Arc::clone(&can_serve_flag),
            intake_gate: Arc::clone(&intake_gate),
            truncations: Arc::clone(&truncations),
            base: Instant::now(),
            durable_seen: durable,
            adopted_term: boot_term,
            awaiting_reconcile: false,
            pending_new_map: None,
        };
        let consensus_agent =
            AgentRunner::spawn("uc2-consensus", IdleStrategy::Yield, move || consensus.do_work())?;

        Ok(Node {
            counters,
            term_handle,
            leader_flag,
            can_serve_flag,
            ingress_tx,
            truncations,
            route_drops,
            partition_handles,
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

    pub fn counters(&self) -> &Arc<LogCounters> {
        &self.counters
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
    /// observability). Safe drops — votes/reports/gossip re-fire on their
    /// cadence — but a rising count signals a wedged consensus agent.
    pub fn net_event_drops(&self) -> u64 {
        self.route_drops.route_drops.load(Ordering::Relaxed)
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
    counters: Arc<LogCounters>,
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
    trunc_tx: mpsc::SyncSender<u64>,
    ack_rx: mpsc::Receiver<u64>,
    term_handle: TermHandle,
    leader_flag: Arc<AtomicBool>,
    can_serve_flag: Arc<AtomicBool>,
    intake_gate: Arc<AtomicBool>,
    truncations: Arc<AtomicU64>,
    base: Instant,
    durable_seen: u64,
    adopted_term: u32,
    awaiting_reconcile: bool,
    pending_new_map: Option<Vec<(u32, u64)>>,
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

        // 1c. Drain truncation acks (a later cycle after emitting `Truncate`).
        while let Ok(to) = self.ack_rx.try_recv() {
            self.on_truncated(to);
            did = true;
        }

        // 2. Poll the durable counter; feed DurableAdvanced on change.
        let d = self.counters.durable.load_acquire();
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
        did
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
            NetEvent::CommitGossip { term, commit } => Event::CommitGossip { term, commit },
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
            NetEvent::TermMap { term, entries } => Event::TermMapReceived {
                term,
                entries: entries.iter().map(|e| (e.term, e.base)).collect(),
            },
            NetEvent::LeaderActivity { term } => Event::LeaderSeen { term },
        };
        self.feed(event);
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
            if let Some(t) = tm_term
                && self.awaiting_reconcile
                && !produced_truncate
                && t >= term_before
            {
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
                self.counters.prime(base);
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
                self.open_gate(); // a leader is the source of truth; no reconcile pending
                self.leader_flag.store(true, Ordering::Release);
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
                }
                self.adopted_term = term;
            }
            Action::AdvanceCommit { commit } => {
                // The ONLY commit store in the binary (both roles).
                self.counters.commit.store_release(commit);
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
            Action::Truncate { to, new_map } => {
                // Pause intake, stash the map to persist on ack, command the
                // archive. The SM has already latched out the data plane.
                self.close_gate();
                self.pending_new_map = Some(new_map);
                self.trunc_tx.send(to).expect("archive channel closed");
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

    /// Archive-truncation feedback: adopt + persist the reconciled map, reopen
    /// intake, release the SM latch, count the truncation.
    fn on_truncated(&mut self, to: u64) {
        if let Some(new_map) = self.pending_new_map.take() {
            self.state
                .store_term_map(&to_entries(&new_map))
                .expect("term-map persist fail-stop");
        }
        // The archive re-primed the counters to `to`; keep our shadow in step so
        // we don't refeed a spurious DurableAdvanced.
        self.durable_seen = to;
        self.open_gate();
        self.awaiting_reconcile = false;
        self.truncations.fetch_add(1, Ordering::Relaxed);
        self.feed(Event::Truncated { to });
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
