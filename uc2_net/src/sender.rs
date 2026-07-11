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

use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Instant;

use uc2_consensus::commit::CommitTracker;
use uc2_log::buffer::{LogBuffer, SliceRead};
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_COMMIT_POSITION, DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT,
    DatagramHeader, MTU_DEFAULT, write_datagram_header,
};
use uc_protocol::v2::frame::{HEADER_LEN, align_frame_len};

use crate::fault::FaultSocket;
use crate::flow::FlowControl;

/// Bound on queued NAK requests (M2 final review: a flooding/hostile
/// follower must not grow the deque unboundedly). Oldest entries drop first —
/// a re-NAK after backoff re-requests anything still missing, so dropping is
/// always recoverable. 1024 entries ≈ 24 KB; the worst storm observed in the
/// M2 gate was ~10k NAKs over a whole run.
const NAK_QUEUE_MAX: usize = 1024;

/// Control messages routed from the leader's receiver agent (Task 8).
/// Bounded channel; a dropped message is safe (NAK re-fires after backoff,
/// status re-sends on its floor).
#[derive(Debug, Clone, Copy)]
pub enum CtrlMsg {
    Nak { from: SocketAddr, position: u64, length: u32 },
    Status { from: SocketAddr, contiguous: u64, window: u32 },
    /// A follower's AppendPosition report (spec §6): its durable position.
    AppendPos { from: SocketAddr, durable: u64 },
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
    /// Validated read lost the race with the appender: that follower needs a
    /// journal replay session (M4) — in M2 this only counts.
    pub overruns: AtomicU64,
    /// NAK requests dropped because the queue hit `NAK_QUEUE_MAX` (oldest
    /// dropped first); observability only — a re-NAK after backoff recovers.
    pub naks_dropped: AtomicU64,
    /// CommitPosition datagrams fanned out (on-advance + floor re-gossip).
    pub commit_gossips: AtomicU64,
    /// AppendPosition reports from an address not in the follower set — dropped
    /// at the membership guard, never ranked (forged/unknown source).
    pub append_pos_unknown_source: AtomicU64,
    /// AppendPosition reports (from a KNOWN follower) claiming positions beyond
    /// our own append are provably corrupt in a static term (a follower cannot
    /// hold bytes the leader never appended) — dropped whole, counted; M4's
    /// term/incarnation machinery revisits.
    pub append_pos_implausible: AtomicU64,
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
    base: Instant,
    last_heartbeat_ns: u64,
    /// Quorum commit ranking (spec §6) — this thread is the single writer of
    /// the leader's commit counter.
    tracker: CommitTracker,
    follower_idx: HashMap<SocketAddr, usize>,
    stats: Arc<SenderStats>,
}

impl Sender {
    pub fn new(
        buffer: Arc<LogBuffer>,
        sock: FaultSocket,
        followers: Vec<SocketAddr>,
        cluster_size: usize,
        ctrl: mpsc::Receiver<CtrlMsg>,
        cfg: SenderConfig,
    ) -> Sender {
        assert!(
            align_frame_len(HEADER_LEN + buffer.max_payload()) + DATAGRAM_HEADER_LEN <= cfg.mtu,
            "a max-size frame must fit one datagram (raise mtu — the jumbo-frame knob)"
        );
        let flow = FlowControl::new(&followers, cluster_size, cfg.initial_window);
        let sent = buffer.counters().sent.load_acquire();
        let tracker = CommitTracker::new(followers.len(), cluster_size);
        let follower_idx: HashMap<SocketAddr, usize> =
            followers.iter().enumerate().map(|(i, a)| (*a, i)).collect();
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
            base: Instant::now(),
            last_heartbeat_ns: 0,
            tracker,
            follower_idx,
            stats: Arc::new(SenderStats::default()),
        }
    }

    pub fn stats(&self) -> Arc<SenderStats> {
        Arc::clone(&self.stats)
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
                    self.flow.on_status(from, contiguous, window)
                }
                CtrlMsg::Nak { from, position, length } => {
                    if self.naks.len() >= NAK_QUEUE_MAX {
                        self.naks.pop_front();
                        self.stats.naks_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                    self.naks.push_back((from, position, length))
                }
                CtrlMsg::AppendPos { from, durable } => {
                    if let Some(&i) = self.follower_idx.get(&from) {
                        // A follower cannot hold bytes the leader never
                        // appended. The wire has no CRC, so a bit-flip that
                        // escapes the UDP checksum could inflate this report;
                        // a report claiming more than our own append is
                        // provably corrupt in a static term. DROP it whole
                        // (count it) rather than clamp-to-append: clamping
                        // would still let one corrupt datagram certify that the
                        // follower holds every appended byte — {own, own, 0}
                        // ranks own at the quorum slot — manufacturing a
                        // phantom commit on leader-only durability and
                        // defeating the quorum-loss-stall theorem. Dropping
                        // poisons nothing: the tracker slot is monotonic-max,
                        // so a later legitimate report still advances it. The
                        // one legitimate way a follower leads our append — a
                        // restarted leader whose append was re-primed below a
                        // still-ahead follower — is a future-incarnation case
                        // that M4's term/incarnation machinery handles; in a
                        // static term it cannot arise. Load append once per
                        // report is fine (control is kHz).
                        let own_append = self.buffer.counters().append.load_acquire();
                        if durable > own_append {
                            self.stats.append_pos_implausible.fetch_add(1, Ordering::Relaxed);
                        } else {
                            self.tracker.on_durable(i, durable);
                        }
                    } else {
                        self.stats.append_pos_unknown_source.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            did = true;
        }

        // Commit ranking (spec §6): once per duty cycle, quorum-th highest of
        // {own durable} ∪ reports, bounded by own durable, monotonic. Advances
        // at block/fsync granularity (reports and own durable both move per
        // archive block), so the on-advance gossip stays ~kHz — never
        // per-message.
        let own_durable = self.buffer.counters().durable.load_acquire();
        if let Some(c) = self.tracker.advance(own_durable) {
            self.buffer.counters().commit.store_release(c);
            self.gossip_commit(c);
            did = true;
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
                    // can't happen while sent tracks append closely; counted
                    // for the M4 replay-session seam
                    self.stats.overruns.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
        }
        if self.sent < append && self.sent >= limit {
            self.stats.flow_stalls.fetch_add(1, Ordering::Relaxed);
        }

        let now = self.now_ns();
        if now - self.last_heartbeat_ns >= self.cfg.heartbeat_ns {
            self.last_heartbeat_ns = now;
            self.assemble(append, DGRAM_KIND_HEARTBEAT, 0);
            for &to in &self.followers {
                let _ = self.sock.send_to(&self.scratch, to);
            }
            // CommitPosition floor (spec §6: same 100 ms floor as heartbeats)
            self.gossip_commit(self.tracker.commit());
            self.stats.heartbeats.fetch_add(1, Ordering::Relaxed);
            did = true;
        }
        did
    }

    /// Header + the first `body_bytes` of `self.run` into `self.scratch`.
    fn assemble(&mut self, position: u64, kind: u8, body_bytes: usize) {
        self.scratch.clear();
        self.scratch.resize(DATAGRAM_HEADER_LEN, 0);
        write_datagram_header(
            &mut self.scratch,
            &DatagramHeader { position, leadership_term_id: self.cfg.term_id, kind, flags: 0 },
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

    /// Header-only CommitPosition to every follower.
    fn gossip_commit(&mut self, commit: u64) {
        self.assemble(commit, DGRAM_KIND_COMMIT_POSITION, 0);
        for &to in &self.followers {
            let _ = self.sock.send_to(&self.scratch, to);
        }
        self.stats.commit_gossips.fetch_add(1, Ordering::Relaxed);
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
                    // requested bytes have left the buffer: this follower
                    // needs a journal replay session (M4)
                    self.stats.overruns.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
        }
        self.stats.naks_served.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use uc2_log::buffer::Appender;
    use uc2_log::counters::LogCounters;
    use uc2_log::region::Region;
    use uc_protocol::v2::datagram::read_datagram_header;
    use uc_protocol::v2::frame::{read_header, HEADER_LEN};

    fn buffer() -> Arc<LogBuffer> {
        let counters = Arc::new(LogCounters::new());
        Arc::new(LogBuffer::new(Region::heap_zeroed(1 << 16), counters, 256))
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
    fn nak_queue_is_capped_dropping_oldest() {
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
        );
        // flood 1100 NAKs in one control drain; the queue must trim to the cap
        for i in 0..1100u64 {
            tx.send(CtrlMsg::Nak { from: f1.addr(), position: i * 96, length: 96 }).unwrap();
        }
        s.do_work(); // drains all 1100, serves 1
        assert_eq!(
            s.stats().naks_dropped.load(std::sync::atomic::Ordering::Relaxed),
            1100 - NAK_QUEUE_MAX as u64,
            "overflow beyond the cap must be counted as dropped"
        );
    }

    fn ctrl_ap(from: SocketAddr, durable: u64) -> CtrlMsg {
        CtrlMsg::AppendPos { from, durable }
    }

    #[test]
    fn commit_advances_on_quorum_reports_and_gossips() {
        let b = buffer();
        let (f1, f2) = (Fake::new(), Fake::new());
        let (mut s, tx) = sender_to(&[&f1, &f2], &b);
        // Append 20 frames (append = 1920) so follower reports are plausible
        // (durable ≤ append, the real invariant); the leader's own archive lags
        // at durable = 960 (its own fsync trails its append).
        let mut a = Appender::new(Arc::clone(&b), 9);
        for i in 0..20 {
            a.append(4, i, &[0u8; 64]).unwrap();
        }
        b.counters().durable.store_release(960);
        // no reports -> {960, 0, 0} -> 2nd highest = 0 -> no commit
        s.do_work();
        assert_eq!(b.counters().commit.load_acquire(), 0);
        f1.drain();
        f2.drain();
        // one follower reports 480 -> {960, 480, 0} -> commit 480 + gossip
        tx.send(ctrl_ap(f1.addr(), 480)).unwrap();
        s.do_work();
        assert_eq!(b.counters().commit.load_acquire(), 480);
        let mut saw_gossip = 0;
        for f in [&f1, &f2] {
            while let Some((h, body)) = f.recv() {
                if h.kind == DGRAM_KIND_COMMIT_POSITION {
                    assert_eq!(h.position, 480);
                    assert_eq!(h.leadership_term_id, 9);
                    assert!(body.is_empty(), "CommitPosition is header-only");
                    saw_gossip += 1;
                    break;
                }
            }
        }
        assert_eq!(saw_gossip, 2, "commit gossip must fan out to both followers");
        // second follower overtakes: {960, 480, 700} -> commit 700
        tx.send(ctrl_ap(f2.addr(), 700)).unwrap();
        s.do_work();
        assert_eq!(b.counters().commit.load_acquire(), 700);
        // bounded by own durable: followers fully durable at append (1920, a
        // plausible report ≤ append) -> {1920, 1920, 960} -> 2nd = 1920 ->
        // min(own durable 960) -> commit = 960
        tx.send(ctrl_ap(f1.addr(), 1920)).unwrap();
        tx.send(ctrl_ap(f2.addr(), 1920)).unwrap();
        s.do_work();
        assert_eq!(b.counters().commit.load_acquire(), 960);
        assert!(s.stats().commit_gossips.load(std::sync::atomic::Ordering::Relaxed) >= 3);
    }

    #[test]
    fn unknown_source_report_is_ignored() {
        let b = buffer();
        let f1 = Fake::new();
        let ghost = Fake::new(); // not in the follower set
        let (mut s, tx) = sender_to(&[&f1], &b);
        b.counters().durable.store_release(960);
        tx.send(ctrl_ap(ghost.addr(), 960)).unwrap();
        s.do_work();
        assert_eq!(b.counters().commit.load_acquire(), 0, "unknown source advanced commit");
        assert_eq!(
            s.stats().append_pos_unknown_source.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "unknown-source report must be counted at the membership guard"
        );
    }

    #[test]
    fn implausible_append_position_is_dropped_not_ranked() {
        let b = buffer();
        let (f1, f2) = (Fake::new(), Fake::new());
        let (mut s, tx) = sender_to(&[&f1, &f2], &b);
        // own append = 960 (10 frames of 96 B); own durable = 960
        let mut a = Appender::new(Arc::clone(&b), 9);
        for i in 0..10 {
            a.append(4, i, &[0u8; 64]).unwrap();
        }
        b.counters().durable.store_release(960);
        // A corrupt/forged report from KNOWN f1 claims a durable far beyond our
        // append. If it were ranked — even clamped to append=960 — it would
        // certify f1 holds every appended byte: rank {960, 960, 0} -> 2nd = 960
        // -> a PHANTOM commit of 960 on leader-only durability (WITHOUT this
        // datagram the rank is {960, 0, 0} -> 2nd = 0 -> no commit). Dropped,
        // commit MUST stay 0.
        tx.send(ctrl_ap(f1.addr(), 1 << 40)).unwrap();
        s.do_work();
        assert_eq!(
            b.counters().commit.load_acquire(),
            0,
            "implausible report manufactured a phantom commit on leader-only durability"
        );
        assert_eq!(
            s.stats().append_pos_implausible.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "implausible report must be counted"
        );
        // The dropped report did NOT poison the slot: a later LEGITIMATE report
        // (480, within append) advances it normally -> {960, 480, 0} -> 480.
        tx.send(ctrl_ap(f1.addr(), 480)).unwrap();
        s.do_work();
        assert_eq!(
            b.counters().commit.load_acquire(),
            480,
            "the dropped report poisoned f1's tracker slot"
        );
    }

    #[test]
    fn heartbeat_block_regossips_commit_on_the_floor() {
        let b = buffer();
        let f1 = Fake::new();
        let (tx, rx) = mpsc::sync_channel(16);
        let mut cfg = SenderConfig::new(9);
        cfg.heartbeat_ns = 1; // fire every cycle
        let mut s = Sender::new(
            Arc::clone(&b),
            FaultSocket::bind("127.0.0.1:0").unwrap(),
            vec![f1.addr()],
            3,
            rx,
            cfg,
        );
        // append 10 frames (append = 960) so the report 480 is plausible
        let mut a = Appender::new(Arc::clone(&b), 9);
        for i in 0..10 {
            a.append(4, i, &[0u8; 64]).unwrap();
        }
        b.counters().durable.store_release(960);
        tx.send(ctrl_ap(f1.addr(), 480)).unwrap();
        s.do_work(); // advances commit to 480, gossips + heartbeats
        // drain until we have seen BOTH a heartbeat and >= 2 CommitPosition
        // datagrams (the on-advance gossip plus the floor re-gossip)
        let mut commits = 0;
        let mut heartbeats = 0;
        let deadline = Instant::now() + Duration::from_secs(5);
        while commits < 2 || heartbeats < 1 {
            assert!(Instant::now() < deadline, "floor re-gossip never arrived");
            s.do_work();
            while let Some((h, _)) = f1.recv() {
                match h.kind {
                    DGRAM_KIND_COMMIT_POSITION => {
                        assert_eq!(h.position, 480);
                        commits += 1;
                    }
                    DGRAM_KIND_HEARTBEAT => heartbeats += 1,
                    _ => {}
                }
                if commits >= 2 && heartbeats >= 1 {
                    break;
                }
            }
        }
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
}
