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
use uc2_log::archive::find_block;
use uc2_log::buffer::{LogBuffer, SliceRead};
use uc_protocol::v2::datagram::{
    DATAGRAM_HEADER_LEN, DGRAM_KIND_COMMIT_POSITION, DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT,
    DatagramHeader, MTU_DEFAULT, write_datagram_header,
};
use uc_protocol::v2::frame::{
    FRAME_ALIGNMENT, FRAME_TYPE_PADDING, HEADER_LEN, align_frame_len, read_header,
};
use ultima_journal::Journal;

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
    /// A NAK whose bytes had left the ring could NOT be served from the
    /// journal — either no replay source is wired, or the position is below
    /// the first archived block (purged; M6). With a replay source set for a
    /// still-archived position, the seam is served (see `replay_datagrams`),
    /// not counted here.
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
    /// Journal handle for serving deep NAKs whose bytes have left the ring
    /// (M4 replay sessions). `None` until `set_replay_source` wires the
    /// archive's journal in — M2/M3 call sites leave it unset (Overrun counts).
    replay: Option<Arc<Journal>>,
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
            replay: None,
        }
    }

    pub fn stats(&self) -> Arc<SenderStats> {
        Arc::clone(&self.stats)
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
                    // Requested bytes have scrolled out of the ring. Serve the
                    // gap from the durable journal (M4 replay sessions); only
                    // when that is impossible (no source wired, or the position
                    // is below the first archived block — purged, M6) is it an
                    // unrecoverable overrun. Bounded to `REPLAY_DGRAMS_PER_NAK`
                    // datagrams — the follower re-NAKs the remainder.
                    if !self.serve_nak_from_journal(to, p, end) {
                        self.stats.overruns.fetch_add(1, Ordering::Relaxed);
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
                leadership_term_id: self.cfg.term_id,
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
    use uc2_log::counters::LogCounters;
    use uc2_log::region::Region;
    use uc_protocol::v2::datagram::read_datagram_header;
    use uc_protocol::v2::frame::{
        read_header, write_header_except_length, FrameHeader, FRAME_TYPE_MESSAGE, HEADER_LEN,
        OFF_TYPE,
    };

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

    #[test]
    fn journal_replay_serves_deep_nak_with_identical_wire_format() {
        // leader with a TINY buffer (4096) laps it 3x while archiving; a NAK
        // for lap-0 positions must be served from the journal
        let counters = Arc::new(uc2_log::counters::LogCounters::new());
        let b = Arc::new(LogBuffer::new(
            uc2_log::region::Region::heap_zeroed(4096),
            counters,
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
}
