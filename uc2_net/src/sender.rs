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

use uc2_log::archive::find_block;
use uc2_log::buffer::{LogBuffer, SliceRead};
use uc2_log::cnc::CncPage;
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT, DGRAM_KIND_SNAP_BEGIN,
    DGRAM_KIND_SNAP_CHUNK, DatagramHeader, MTU_DEFAULT, SNAP_BEGIN_FIXED_LEN, SnapBeginBody,
    write_datagram_header, write_snap_begin_body,
};
use uc_protocol::v2::frame::{
    FRAME_ALIGNMENT, FRAME_TYPE_PADDING, HEADER_LEN, align_frame_len, read_header,
};
use ultima_journal::Journal;

use crate::TermHandle;
use crate::fault::FaultSocket;
use crate::flow::FlowControl;

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
    Nak { from: SocketAddr, position: u64, length: u32 },
    Status { from: SocketAddr, contiguous: u64, window: u32 },
    /// M6 Task 6: the snapshot-session peer requests a missing file range.
    SnapNak { from: SocketAddr, session: u32, offset: u64, length: u32 },
    /// M6 Task 6: the snapshot-session peer signals the file is complete.
    SnapDone { from: SocketAddr, session: u32 },
    /// M7 (spec 2026-07-13): the consensus agent adopted a new `ClusterConfig`
    /// (`Action::ConfigAdopted`) — rebuild the fan-out + flow control from it.
    /// `followers` and `learners` are DISJOINT sets (voters-minus-self,
    /// learners-minus-self — the same convention `NodeConfig`/`Consensus` use
    /// elsewhere), not the combined fan-out `Sender::with_learners` takes at
    /// construction; the handler recombines them for streaming. `cluster_size`
    /// is the VOTING cluster size (`ClusterConfig::voters.len()`).
    SetPeers { followers: Vec<SocketAddr>, learners: Vec<SocketAddr>, cluster_size: usize },
}

/// M6 Task 6: newest durable snapshot artifact the node is willing to ship —
/// `(snapshot_pos, path, total_len, config)`. The node wires this to
/// `SnapshotStore` filtered by its PERSISTED floor marker (never a
/// half-written file). `None` = no shippable snapshot (the NAK stays an
/// overrun). M7 Task 6: `config` is the `v2::config::encode_config` bytes of
/// the CURRENT `ConfigRecord.config` at ship time — carried in every
/// `SNAP_BEGIN` so a below-floor joiner adopts the leader's membership
/// alongside its lineage. Over-delivery (shipping it to a peer whose config is
/// already current) is safe: the receiver adopts by fiat only on a genuine
/// install, and adoption itself is idempotent by version.
pub type SnapshotSource = Arc<dyn Fn() -> Option<(u64, PathBuf, u64, Vec<u8>)> + Send + Sync>;

/// One in-flight outbound snapshot transfer (M6 Task 6). At most one at a time
/// — a second requester waits; sessions are rare by construction (only a peer
/// whose NAK fell below the purge floor triggers one).
struct SnapSession {
    peer: SocketAddr,
    session: u32,
    snapshot_pos: u64,
    file: std::fs::File,
    total_len: u64,
    /// Next sequential byte offset to ship (the contiguous fill cursor).
    cursor: u64,
    /// Peer-requested missing ranges (repair), served before the cursor.
    naks: VecDeque<(u64, u32)>,
    /// SNAP_BEGIN has been sent (first cycle sends it before any chunk).
    begun: bool,
    last_activity_ns: u64,
    /// M7 Task 6: the encoded `ConfigRecord.config` at the moment this session
    /// opened (from the `SnapshotSource` closure) — carried in `SNAP_BEGIN`.
    config: Vec<u8>,
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
}

impl SenderConfig {
    pub fn new(term_id: u32) -> Self {
        Self {
            mtu: MTU_DEFAULT,
            term_id,
            heartbeat_ns: 100_000_000,
            initial_window: 65_536,
            dgrams_per_cycle: 8,
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
    /// M6 Task 6: monotonic session-id generator — distinguishes a fresh session
    /// from a just-closed one so a stale SNAP_NAK/SNAP_DONE can't cross-talk.
    snap_session_seq: u32,
    /// M6 Task 9: cnc observability band + addr→slot map. `None` on nodes that
    /// never lead and in unit tests. Once per duty cycle the sender fills each
    /// peer's `advertised_limit` from its flow-control view (bounded — a pass over
    /// ≤8 slots, no per-datagram cnc writes). Diagnostics only.
    peer_obs: Option<PeerObs>,
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
        Self::with_learners(buffer, sock, followers, &[], cluster_size, ctrl, cfg, term, role)
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
        assert!(
            align_frame_len(HEADER_LEN + buffer.max_payload()) + DATAGRAM_HEADER_LEN <= cfg.mtu,
            "a max-size frame must fit one datagram (raise mtu — the jumbo-frame knob)"
        );
        // Voting followers pace commit; learners are fanned-out to but never enter
        // `limit()`.
        let voting: Vec<SocketAddr> =
            followers.iter().copied().filter(|a| !learners.contains(a)).collect();
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
            snap_session_seq: 0,
            peer_obs: None,
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

        while let Ok(m) = self.ctrl.try_recv() {
            match m {
                CtrlMsg::Status { from, contiguous, window } => {
                    self.last_status.insert(from, (contiguous, window));
                    self.flow.on_status(from, contiguous, window)
                }
                CtrlMsg::Nak { from, position, length } => {
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
                CtrlMsg::SnapNak { from, session, offset, length } => {
                    // A repair request for the active session only (a stale
                    // session id — the peer NAKing a transfer we already closed —
                    // is dropped). Bounded implicitly by the file size / peer.
                    if let Some(s) = self.snap.as_mut()
                        && s.peer == from
                        && s.session == session
                    {
                        s.naks.push_back((offset, length));
                        s.last_activity_ns = self.base.elapsed().as_nanos() as u64;
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
                CtrlMsg::SetPeers { followers, learners, cluster_size } => {
                    // Rebuild flow control from the new voting/learner split,
                    // re-feeding every surviving address's last raw advert so
                    // ranking does not restart from the bootstrap window (mirrors
                    // `ElectionSm::rebuild_membership`'s carried-reports rationale).
                    self.flow =
                        FlowControl::new(&followers, cluster_size, self.cfg.initial_window, &learners);
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
        let budget = self.cfg.mtu - DATAGRAM_HEADER_LEN;
        let mut dgrams = 0;
        while dgrams < self.cfg.dgrams_per_cycle && self.sent < append && self.sent < limit {
            // don't read more than the flow limit allows in one datagram
            let flow_budget = (limit - self.sent).min(budget as u64) as usize;
            match self.buffer.read_run_validated(self.sent, flow_budget, &mut self.run) {
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
            self.assemble(append, DGRAM_KIND_HEARTBEAT, 0);
            for &to in &self.followers {
                let _ = self.sock.send_to(&self.scratch, to);
            }
            // CommitPosition gossip (spec §6, on-advance + the 100 ms floor) is
            // the consensus agent's job now (`Action::GossipCommit`) — the
            // sender no longer ranks or gossips commit at all (M4 carry #5).
            self.stats.heartbeats.fetch_add(1, Ordering::Relaxed);
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
    /// cnc slot from the current `FlowControl` view. Called once per duty cycle
    /// AND immediately after a `SetPeers` rebuild (M7) so a reconfigured peer's
    /// slot doesn't wait out a stale cycle. Bounded (≤8 slots); a no-op when
    /// `set_peer_slots` was never called (non-leader / unit tests).
    fn refresh_peer_obs(&mut self) {
        if let Some((cnc, slots)) = &self.peer_obs {
            for (addr, idx) in slots {
                if let Some(limit) = self.flow.advertised_limit(*addr) {
                    cnc.peer_slot(*idx).advertised_limit.store_release(limit);
                }
            }
        }
    }

    /// Header + the first `body_bytes` of `self.run` into `self.scratch`.
    fn assemble(&mut self, position: u64, kind: u8, body_bytes: usize) {
        self.scratch.clear();
        self.scratch.resize(DATAGRAM_HEADER_LEN, 0);
        write_datagram_header(
            &mut self.scratch,
            &DatagramHeader {
                position,
                leadership_term_id: self.term.load(Ordering::Relaxed),
                kind,
                flags: 0,
            },
        );
        self.scratch.extend_from_slice(&self.run[..body_bytes]);
    }

    /// One scan, N sends (identical datagram to every follower).
    fn fan_out(&mut self, position: u64, body_bytes: usize) {
        self.assemble(position, DGRAM_KIND_DATA, body_bytes);
        for &to in &self.followers {
            let _ = self.sock.send_to(&self.scratch, to);
            self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
            self.stats.bytes.fetch_add(body_bytes as u64, Ordering::Relaxed);
        }
    }

    /// Retransmit [pos, pos+len) to ONE follower, MTU chunk by MTU chunk.
    /// `len` is capped by the follower (Task 8), so this is bounded work.
    fn serve_nak(&mut self, to: SocketAddr, pos: u64, len: u32) {
        let budget = self.cfg.mtu - DATAGRAM_HEADER_LEN;
        let end = pos + len as u64;
        let mut p = pos;
        while p < end {
            match self.buffer.read_run_validated(p, budget, &mut self.run) {
                SliceRead::Run(r) => {
                    self.assemble(p, DGRAM_KIND_DATA, r.bytes);
                    let _ = self.sock.send_to(&self.scratch, to);
                    self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
                    self.stats.bytes.fetch_add(r.bytes as u64, Ordering::Relaxed);
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
        let budget = self.cfg.mtu - DATAGRAM_HEADER_LEN;
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
    /// no shippable snapshot exists, or the file cannot be opened.
    fn try_open_snap_session(&mut self, to: SocketAddr) -> bool {
        if self.snap.is_some() {
            return false;
        }
        let Some(src) = self.snapshot_source.clone() else {
            return false;
        };
        let Some((pos, path, len, config)) = src() else {
            return false;
        };
        let Ok(file) = std::fs::File::open(&path) else {
            return false;
        };
        let sid = self.snap_session_seq.wrapping_add(1);
        self.snap_session_seq = sid;
        self.snap = Some(SnapSession {
            peer: to,
            session: sid,
            snapshot_pos: pos,
            file,
            total_len: len,
            cursor: 0,
            naks: VecDeque::new(),
            begun: false,
            last_activity_ns: self.base.elapsed().as_nanos() as u64,
            config,
        });
        self.stats.snap_sessions.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Advance the in-flight snapshot session by at most [`SNAP_DGRAMS_PER_CYCLE`]
    /// chunk datagrams: send SNAP_BEGIN once, then serve peer repair NAKs, then
    /// fill the cursor sequentially. Abandons the session after
    /// [`SNAP_SESSION_TIMEOUT_NS`] with no progress. Returns `true` iff it did work.
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

        let mut did = false;
        if !sess.begun {
            self.send_snap_begin(sess.peer, sess.session, sess.snapshot_pos, sess.total_len, &sess.config);
            sess.begun = true;
            did = true;
        }

        let mut emitted = 0usize;
        // Repair NAKs first (the peer is blocked on these).
        while emitted < SNAP_DGRAMS_PER_CYCLE {
            let Some((offset, length)) = sess.naks.pop_front() else {
                break;
            };
            let n = self.send_snap_chunk(&mut sess, offset, true);
            if n == 0 {
                break; // offset past EOF / read error — drop the request
            }
            if (n as u32) < length {
                // Range spans multiple datagrams: re-queue the remainder.
                sess.naks.push_front((offset + n as u64, length - n as u32));
            }
            emitted += 1;
            did = true;
        }
        // Then sequential cursor fill.
        while emitted < SNAP_DGRAMS_PER_CYCLE && sess.cursor < sess.total_len {
            let at = sess.cursor;
            let n = self.send_snap_chunk(&mut sess, at, false);
            if n == 0 {
                break;
            }
            sess.cursor += n as u64;
            emitted += 1;
            did = true;
        }

        if did {
            sess.last_activity_ns = now;
        }
        self.snap = Some(sess);
        did
    }

    /// Read one MTU-sized chunk from the snapshot file at `offset` and ship it as
    /// a SNAP_CHUNK (header `position` = file offset). Returns bytes sent (0 on
    /// EOF / read error).
    fn send_snap_chunk(&mut self, sess: &mut SnapSession, offset: u64, is_nak: bool) -> usize {
        if offset >= sess.total_len {
            return 0;
        }
        let want = ((sess.total_len - offset) as usize).min(self.cfg.mtu - DATAGRAM_HEADER_LEN);
        let mut buf = vec![0u8; want];
        if sess.file.seek(SeekFrom::Start(offset)).is_err() {
            return 0;
        }
        if sess.file.read_exact(&mut buf).is_err() {
            return 0;
        }
        self.assemble_snap(offset, DGRAM_KIND_SNAP_CHUNK, &buf);
        let _ = self.sock.send_to(&self.scratch, sess.peer);
        self.stats.snap_chunks.fetch_add(1, Ordering::Relaxed);
        if is_nak {
            self.stats.snap_chunk_naks.fetch_add(1, Ordering::Relaxed);
        }
        want
    }

    /// Ship SNAP_BEGIN (header `position` = 0; body carries session/pos/len/config).
    /// M7 Task 6: `config` is the encoded `ConfigRecord.config` this session's
    /// `SnapshotSource` closure captured at open time — the body grows to fit it.
    fn send_snap_begin(
        &mut self,
        peer: SocketAddr,
        session: u32,
        snapshot_pos: u64,
        total_len: u64,
        config: &[u8],
    ) {
        let mut body = vec![0u8; SNAP_BEGIN_FIXED_LEN + config.len()];
        write_snap_begin_body(
            &mut body,
            &SnapBeginBody { session, snapshot_pos, total_len, config: config.to_vec() },
        );
        self.assemble_snap(0, DGRAM_KIND_SNAP_BEGIN, &body);
        let _ = self.sock.send_to(&self.scratch, peer);
    }

    /// Assemble a snapshot-session datagram (header + explicit body) into scratch.
    fn assemble_snap(&mut self, position: u64, kind: u8, payload: &[u8]) {
        self.scratch.clear();
        self.scratch.resize(DATAGRAM_HEADER_LEN, 0);
        write_datagram_header(
            &mut self.scratch,
            &DatagramHeader {
                position,
                leadership_term_id: self.term.load(Ordering::Relaxed),
                kind,
                flags: 0,
            },
        );
        self.scratch.extend_from_slice(payload);
    }

    /// Assemble a DATA datagram from an arbitrary body slice (a journal-replay
    /// run) and send it to one follower. Same framing as `fan_out`/`assemble`,
    /// but the body is copied from the caller's slice (the ring path stages it
    /// in `self.run`; here it lives in the journal block).
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
            },
        );
        self.scratch.extend_from_slice(body);
        let _ = self.sock.send_to(&self.scratch, to);
        self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
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
    use uc2_log::buffer::Appender;
    use uc2_log::cnc::{CncMeta, CncPage};
    use uc2_log::region::Region;
    use uc_protocol::v2::datagram::read_datagram_header;
    use uc_protocol::v2::frame::{
        read_header, write_header_except_length, FrameHeader, FRAME_TYPE_MESSAGE, HEADER_LEN,
        OFF_TYPE,
    };

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
            Self { sock: FaultSocket::bind("127.0.0.1:0").unwrap() }
        }
        fn addr(&self) -> SocketAddr {
            self.sock.local_addr().unwrap()
        }
        fn recv(&self) -> Option<(DatagramHeader, Vec<u8>)> {
            let mut buf = [0u8; 2048];
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if let Some((n, _)) = self.sock.recv_from(&mut buf).unwrap() {
                    let h = read_datagram_header(&buf);
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
            assert_eq!(read_header(&body[96..]).correlation_id, 1);
            assert_eq!(&body[2 * 96 + HEADER_LEN..2 * 96 + HEADER_LEN + 64], &[2u8; 64]);
        }
        assert_eq!(b.counters().sent.load_acquire(), 3 * 96);
        assert_eq!(s.stats().datagrams.load(std::sync::atomic::Ordering::Relaxed), 2);
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
        tx.send(CtrlMsg::Status { from: f1.addr(), contiguous: 1_000_000, window: 100_000 })
            .unwrap();
        tx.send(CtrlMsg::Status { from: f2.addr(), contiguous: 2_000_000, window: 50_000 })
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
        assert_eq!(s.flow.limit(), 1_100_000, "f1's re-fed advert, not the bootstrap window");
    }

    #[test]
    fn respects_flow_limit_and_resumes_on_status() {
        let b = buffer();
        let f1 = Fake::new();
        let (mut s, tx) = sender_to(&[&f1], &b);
        // shrink the follower's advertised limit to one datagram's worth
        tx.send(CtrlMsg::Status { from: f1.addr(), contiguous: 0, window: 96 }).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 9);
        for i in 0..4 {
            a.append(4, i, &[0u8; 64]).unwrap();
        }
        s.do_work();
        let (h, body) = f1.recv().expect("first frame");
        assert_eq!((h.position, body.len()), (0, 96)); // only up to the limit
        assert!(s.stats().flow_stalls.load(std::sync::atomic::Ordering::Relaxed) > 0);
        f1.drain();
        // status advances -> the rest flows
        tx.send(CtrlMsg::Status { from: f1.addr(), contiguous: 96, window: 1 << 20 }).unwrap();
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
        tx.send(CtrlMsg::Nak { from: f2.addr(), position: 96, length: 192 }).unwrap();
        s.do_work();
        let (h, body) = f2.recv().expect("retransmission");
        assert_eq!(h.kind, DGRAM_KIND_DATA);
        assert_eq!(h.position, 96);
        assert!(body.len() >= 192);
        assert!(f1.recv().is_none(), "NAK service must not fan out");
        assert_eq!(s.stats().naks_served.load(std::sync::atomic::Ordering::Relaxed), 1);
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
            tx.send(CtrlMsg::Nak { from: f1.addr(), position: i * 96, length: 96 }).unwrap();
        }
        s.do_work(); // drains all 1100 into ONE coalesced slot, serves that slot
        assert_eq!(
            s.stats().naks_dropped.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "same-follower NAKs coalesce to one slot — the cap never trips"
        );
        assert_eq!(
            s.stats().naks_served.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the 1100 re-NAKs collapse to a single served request"
        );
        // The one served request was the LATEST frontier (1099*96), which is
        // beyond `append` (96*2): NotCommitted, so nothing is sent — proving the
        // slot held the newest position, not the oldest (which would have sent
        // frame 0).
        assert!(f1.recv().is_none(), "coalesced NAK kept the latest position, not the oldest");
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
        tx.send(CtrlMsg::Nak { from: f1.addr(), position: 0, length: 96 }).unwrap();
        tx.send(CtrlMsg::Nak { from: f2.addr(), position: 96, length: 96 }).unwrap();
        // two distinct slots -> two do_work cycles serve one NAK each
        s.do_work();
        s.do_work();
        assert_eq!(
            s.stats().naks_served.load(std::sync::atomic::Ordering::Relaxed),
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
        tx.send(CtrlMsg::Nak { from: f1.addr(), position: 100, length: 96 }).unwrap();
        s.do_work();
        assert_eq!(
            s.stats().naks_rejected.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "misaligned NAK position must be rejected at ingestion"
        );
        assert_eq!(
            s.stats().naks_served.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "a rejected NAK is never queued or served"
        );
        assert!(f1.recv().is_none(), "nothing is sent to the requester for a corrupt NAK");
    }

    /// Hand-build a valid message frame of `total` bytes (header + payload,
    /// zero-filled) with correlation id `corr`.
    fn msg_frame(total: u32, corr: u64) -> Vec<u8> {
        let mut f = vec![0u8; align_frame_len(total as usize)];
        write_header_except_length(
            &mut f,
            &FrameHeader {
                length: total,
                frame_type: FRAME_TYPE_MESSAGE,
                flags: 0,
                leadership_term_id: 9,
                session_id: 0,
                correlation_id: corr,
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
        assert_eq!(count, 0, "starting on a corrupt frame emits nothing and does not panic");

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
            tx.send(CtrlMsg::Nak { from, position: 0, length: 96 }).unwrap();
        }
        s.do_work(); // drains all NAK_QUEUE_MAX+K into the queue, serves one
        assert_eq!(
            s.stats().naks_dropped.load(std::sync::atomic::Ordering::Relaxed),
            K as u64,
            "exactly the K over-cap distinct-source NAKs drop"
        );
        assert_eq!(
            s.stats().naks_rejected.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "aligned positions are not rejected"
        );
        let served = s.stats().naks_served.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(served, 1, "the drain cycle served one queued NAK");
        s.do_work();
        assert!(
            s.stats().naks_served.load(std::sync::atomic::Ordering::Relaxed) > served,
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
            uc2_log::region::Region::heap_zeroed(4096),
            test_cnc(4096),
            256,
        ));
        let dir = tempfile::tempdir().unwrap();
        let cfg = uc2_log::archive::ArchiveConfig {
            segment_size_bytes: 4 * 1024 * 1024,
            ..uc2_log::archive::ArchiveConfig::new(dir.path())
        };
        let mut arch = uc2_log::archive::Archive::open(cfg).unwrap();
        let mut a = Appender::new(Arc::clone(&b), 9);
        let mut n = 0u64;
        while a.position() < 3 * 4096 {
            match a.append(1, n, &[n as u8; 64]) {
                Ok(_) => n += 1,
                Err(uc2_log::buffer::AppendError::WouldOverrun) => {
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
        tx.send(CtrlMsg::Nak { from: f1.addr(), position: 0, length: 4096 }).unwrap();
        s.do_work();
        // served from the journal: DATA datagrams, self-locating from 0,
        // frames byte-identical to the original appends
        let (h, body) = f1.recv().expect("replayed datagram");
        assert_eq!(h.kind, DGRAM_KIND_DATA);
        assert_eq!(h.position, 0);
        assert_eq!(read_header(&body).correlation_id, 0);
        assert_eq!(&body[HEADER_LEN..HEADER_LEN + 64], &[0u8; 64]);
        assert!(s.stats().replay_datagrams.load(std::sync::atomic::Ordering::Relaxed) >= 1);
        assert_eq!(
            s.stats().overruns.load(std::sync::atomic::Ordering::Relaxed),
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
        assert_eq!(h.leadership_term_id, 8, "datagram term must come from the sender handle");
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
        assert!(f1.recv().is_none(), "a follower-role sender must emit nothing");
        assert_eq!(s.stats().datagrams.load(Relaxed), 0);
        assert_eq!(s.stats().heartbeats.load(Relaxed), 0);
        assert_eq!(b.counters().sent.load_acquire(), 0, "follower role advanced sent");
        // promote to leader: the pending frames now stream out
        flag.store(true, Relaxed);
        s.do_work();
        let (h, body) = f1.recv().expect("leader role must stream the pending data");
        assert_eq!(h.kind, DGRAM_KIND_DATA);
        assert_eq!(body.len(), 3 * 96);
        assert_eq!(b.counters().sent.load_acquire(), 3 * 96);
    }

}
