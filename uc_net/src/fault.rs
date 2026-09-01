// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Nonblocking UDP socket with a built-in, seeded fault layer (spec §8 L2:
//! native to own-UDP, day one). Faults are applied on the SEND side so a
//! seeded run is deterministic: drop skips the syscall, dup sends twice,
//! reorder holds one datagram back and flushes it after the next send (a
//! held datagram is therefore delayed by at most one send — heartbeats keep
//! sends coming, so nothing is held forever).

use std::collections::{HashSet, VecDeque};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::{Arc, RwLock};

/// Deterministic xorshift64 — no external RNG dependency.
pub(crate) struct XorShift64(u64);

impl XorShift64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }
    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    /// True with probability `per_million / 1_000_000`.
    pub(crate) fn chance(&mut self, per_million: u32) -> bool {
        self.next_u64() % 1_000_000 < per_million as u64
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FaultConfig {
    pub seed: u64,
    pub drop_per_million: u32,
    pub dup_per_million: u32,
    pub reorder_per_million: u32,
    /// M8 Task 14 (adversarial tier): flips one pseudo-random bit of the
    /// datagram before it hits the wire — an on-path attacker's bit-level
    /// corruption (or a lossy/noisy link's), not a benign fault. Applied to
    /// a private copy; the caller's buffer is never mutated. Zero (the
    /// default) costs nothing: the corrupt branch is skipped without ever
    /// touching the RNG, so it does not perturb the drop/dup/reorder draw
    /// sequence any existing seeded test depends on.
    pub corrupt_per_million: u32,
    /// M8 Task 14: with this probability, ALSO re-delivers one uniformly
    /// random previously-sent datagram from a bounded history (distinct
    /// from `dup_per_million`, which always re-sends the CURRENT datagram
    /// immediately) — an on-path attacker capturing and replaying old wire
    /// bytes at an arbitrary later time. Zero (the default) costs nothing:
    /// no history is even recorded when this is 0.
    pub replay_per_million: u32,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            seed: 1,
            drop_per_million: 0,
            dup_per_million: 0,
            reorder_per_million: 0,
            corrupt_per_million: 0,
            replay_per_million: 0,
        }
    }
}

/// The two scripted cut modes a [`PartitionHandle`] can hold per peer.
#[derive(Default)]
struct BlockTable {
    /// Fully cut: every datagram to these peers drops.
    all: HashSet<SocketAddr>,
    /// Muzzled: everything EXCEPT the election plane (`REQUEST_VOTE`/`VOTE`)
    /// drops. Exists so a test can construct "this node WON an election but
    /// none of its data ever landed" without racing the sender agent — that
    /// race is scheduling-dependent and on some hardware unwinnable (first
    /// measured on Graviton/Neoverse-V3, 2026-08-31: 0 wins in 264 tries of
    /// a race that is ~50/50 on x86).
    except_election: HashSet<SocketAddr>,
}

/// A shared, injectable partition table (M4, spec §8): the set of peer
/// addresses this socket is currently partitioned away from. Cloned into a
/// [`FaultSocket`] and driven from a test/consensus thread while the socket's
/// own agent keeps sending — every `send_to` consults it BEFORE the seeded
/// fault rolls (a partition is not a random fault; it is a hard, scriptable
/// link cut). Empty-set is the steady state, so `send_to` pays exactly one
/// uncontended read-lock (~20 ns) when nothing is blocked.
#[derive(Clone, Default)]
pub struct PartitionHandle(Arc<RwLock<BlockTable>>);

impl PartitionHandle {
    /// Partition this socket away from `addr`: subsequent `send_to(addr)` drop
    /// silently until [`unblock`](Self::unblock)/[`clear`](Self::clear).
    pub fn block(&self, addr: SocketAddr) {
        self.0.write().unwrap().all.insert(addr);
    }
    /// Muzzle this socket toward `addr`: only election datagrams
    /// (`REQUEST_VOTE`/`VOTE`) pass; DATA, heartbeats, durable reports,
    /// term-map gossip and everything else drop. A full [`block`](Self::block)
    /// on the same peer supersedes the muzzle.
    pub fn block_except_election(&self, addr: SocketAddr) {
        self.0.write().unwrap().except_election.insert(addr);
    }
    /// Reconnect a single peer (inverse of [`block`](Self::block); lifts a
    /// muzzle too).
    pub fn unblock(&self, addr: SocketAddr) {
        let mut t = self.0.write().unwrap();
        t.all.remove(&addr);
        t.except_election.remove(&addr);
    }
    /// Heal every partition (and muzzle) on this socket.
    pub fn clear(&self) {
        let mut t = self.0.write().unwrap();
        t.all.clear();
        t.except_election.clear();
    }
}

/// True iff `buf` starts with a datagram header whose kind is on the election
/// plane. A runt shorter than the header has no kind and is NOT election
/// traffic. Works crypto-on too: the header is AAD, sealed but not encrypted.
fn is_election_kind(buf: &[u8]) -> bool {
    use uc_protocol::v2::datagram::{DGRAM_KIND_REQUEST_VOTE, DGRAM_KIND_VOTE, OFF_DGRAM_KIND};
    matches!(
        buf.get(OFF_DGRAM_KIND),
        Some(&DGRAM_KIND_REQUEST_VOTE) | Some(&DGRAM_KIND_VOTE)
    )
}

/// Bound on the replay-history ring (see [`FaultSocket::remember`]) — old
/// enough to span a handful of datagrams' worth of "attacker captured this
/// a while ago", small enough that a sustained high-rate sender never grows
/// unbounded memory from it.
const REPLAY_HISTORY_CAP: usize = 32;

pub struct FaultSocket {
    sock: UdpSocket,
    cfg: FaultConfig,
    rng: XorShift64,
    held: Option<(Vec<u8>, SocketAddr)>,
    blocked: PartitionHandle,
    /// M8 Task 14: a bounded ring of recently-sent (datagram, destination)
    /// pairs, used only by the `replay_per_million` fault — empty and never
    /// grown while that knob is 0 (the default), so this costs nothing on
    /// the hot path in production.
    history: VecDeque<(Vec<u8>, SocketAddr)>,
}

impl FaultSocket {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Self::from_socket(UdpSocket::bind(addr)?)
    }

    pub fn from_socket(sock: UdpSocket) -> io::Result<Self> {
        sock.set_nonblocking(true)?;
        let cfg = FaultConfig::default();
        Ok(Self {
            sock,
            rng: XorShift64::new(cfg.seed),
            cfg,
            held: None,
            blocked: PartitionHandle::default(),
            history: VecDeque::new(),
        })
    }

    /// A shared handle to this socket's partition table (M4). Multiple handles
    /// alias the same set; driving any one of them affects this socket's sends.
    pub fn partition_handle(&self) -> PartitionHandle {
        self.blocked.clone()
    }

    pub fn set_faults(&mut self, cfg: FaultConfig) {
        self.rng = XorShift64::new(cfg.seed);
        self.cfg = cfg;
        // M8 Task 14 review (M-3): a reconfiguration is a fresh seeded run —
        // leftover `held`/`history` state from a PRIOR `FaultConfig` would
        // let a later `replay_per_million` pick up (or a later `reorder_
        // per_million` flush) a datagram sent under a completely different
        // configuration, breaking the "one seed, one deterministic sequence"
        // contract this whole module exists for.
        self.held = None;
        self.history.clear();
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// Clone the raw socket for a same-node agent that only receives (the
    /// leader's receiver shares the sender's socket).
    pub fn try_clone_raw(&self) -> io::Result<UdpSocket> {
        self.sock.try_clone()
    }

    pub fn send_to(&mut self, buf: &[u8], to: SocketAddr) -> io::Result<()> {
        // Partition check BEFORE the seeded fault rolls: a scripted link cut is
        // deterministic and must not consume RNG draws (that would desync a
        // seeded drop/dup/reorder run). Empty-set steady state = one
        // uncontended read-lock; a partitioned peer's datagram is dropped whole.
        {
            let t = self.blocked.0.read().unwrap();
            if !t.all.is_empty() && t.all.contains(&to) {
                return Ok(());
            }
            if !t.except_election.is_empty()
                && t.except_election.contains(&to)
                && !is_election_kind(buf)
            {
                return Ok(());
            }
        }
        if self.cfg.drop_per_million > 0 && self.rng.chance(self.cfg.drop_per_million) {
            return Ok(()); // dropped on the wire
        }
        if self.cfg.reorder_per_million > 0
            && self.held.is_none()
            && self.rng.chance(self.cfg.reorder_per_million)
        {
            self.held = Some((buf.to_vec(), to));
            return Ok(());
        }

        // Corrupt: flip one pseudo-random bit in a PRIVATE copy before it
        // ever reaches the wire. `chance` is only called when the knob is
        // set, so a 0 (the default) draws nothing from the RNG and leaves
        // the drop/dup/reorder sequence any existing seeded test depends on
        // byte-identical to before this task.
        let corrupted;
        let out: &[u8] = if self.cfg.corrupt_per_million > 0
            && !buf.is_empty()
            && self.rng.chance(self.cfg.corrupt_per_million)
        {
            let mut c = buf.to_vec();
            let i = (self.rng.next_u64() % c.len() as u64) as usize;
            let bit = 1u8 << (self.rng.next_u64() % 8);
            c[i] ^= bit;
            corrupted = c;
            &corrupted
        } else {
            buf
        };

        self.raw_send(out, to)?;
        if self.cfg.dup_per_million > 0 && self.rng.chance(self.cfg.dup_per_million) {
            self.raw_send(out, to)?;
        }
        let flushed_held = self.held.take();
        if let Some((b, a)) = &flushed_held {
            self.raw_send(b, *a)?;
        }
        // Replay: independently of everything above, re-deliver one
        // uniformly random datagram from the bounded history — an on-path
        // attacker resending old captured ciphertext at an unrelated later
        // time. Drawn BEFORE this call's own datagram(s) are remembered, so
        // a datagram can never "replay itself" on the very call that sent
        // it for the first time — only a datagram from a STRICTLY EARLIER
        // call is eligible. A no-op (not just a no-draw) until at least one
        // datagram from an earlier call has ever been remembered.
        if self.cfg.replay_per_million > 0
            && self.rng.chance(self.cfg.replay_per_million)
            && let Some((rb, ra)) = self.pick_replay()
        {
            self.raw_send(&rb, ra)?;
        }
        if self.cfg.replay_per_million > 0 {
            self.remember(out, to);
            if let Some((b, a)) = flushed_held {
                self.remember(&b, a);
            }
        }
        Ok(())
    }

    /// Push `(buf, to)` onto the bounded replay history, evicting the oldest
    /// entry once full. Only ever called while `replay_per_million > 0`.
    fn remember(&mut self, buf: &[u8], to: SocketAddr) {
        if self.history.len() == REPLAY_HISTORY_CAP {
            self.history.pop_front();
        }
        self.history.push_back((buf.to_vec(), to));
    }

    /// A uniformly random entry from the replay history, or `None` if
    /// nothing has been remembered yet.
    fn pick_replay(&mut self) -> Option<(Vec<u8>, SocketAddr)> {
        if self.history.is_empty() {
            return None;
        }
        let i = (self.rng.next_u64() % self.history.len() as u64) as usize;
        self.history.get(i).cloned()
    }

    fn raw_send(&self, buf: &[u8], to: SocketAddr) -> io::Result<()> {
        match self.sock.send_to(buf, to) {
            Ok(_) => Ok(()),
            // ICMP unreachable from an earlier send surfaces here on Linux;
            // UDP is fire-and-forget for us.
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Nonblocking receive: `None` when the socket is empty.
    pub fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(usize, SocketAddr)>> {
        match self.sock.recv_from(buf) {
            Ok(x) => Ok(Some(x)),
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::ConnectionRefused
                    || e.kind() == io::ErrorKind::ConnectionReset =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn recv_all(sock: &FaultSocket, expect: usize) -> Vec<Vec<u8>> {
        let mut buf = [0u8; 2048];
        let mut got = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        while got.len() < expect && Instant::now() < deadline {
            match sock.recv_from(&mut buf).unwrap() {
                Some((n, _)) => got.push(buf[..n].to_vec()),
                None => std::thread::yield_now(),
            }
        }
        got
    }

    #[test]
    fn xorshift_is_deterministic_and_chance_bounded() {
        let mut a = XorShift64::new(42);
        let mut b = XorShift64::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        let mut r = XorShift64::new(7);
        assert!((0..1000).all(|_| !r.chance(0)));
        let mut r = XorShift64::new(7);
        assert!((0..1000).all(|_| r.chance(1_000_000)));
    }

    #[test]
    fn clean_roundtrip_and_wouldblock() {
        let rx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let mut buf = [0u8; 16];
        assert!(rx.recv_from(&mut buf).unwrap().is_none()); // nonblocking
        tx.send_to(b"ping", rx.local_addr().unwrap()).unwrap();
        let got = recv_all(&rx, 1);
        assert_eq!(got, vec![b"ping".to_vec()]);
    }

    #[test]
    fn drop_is_deterministic_by_seed() {
        let rx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let to = rx.local_addr().unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        tx.set_faults(FaultConfig {
            seed: 99,
            drop_per_million: 500_000,
            ..Default::default()
        });
        for i in 0..100u8 {
            tx.send_to(&[i], to).unwrap();
        }
        // loopback is lossless: received = exactly the non-dropped set.
        // Re-derive it from the same seed.
        let mut rng = XorShift64::new(99);
        let expected: Vec<Vec<u8>> = (0..100u8)
            .filter(|_| !rng.chance(500_000))
            .map(|i| vec![i])
            .collect();
        assert!(!expected.is_empty() && expected.len() < 100);
        let got = recv_all(&rx, expected.len());
        assert_eq!(got, expected);
    }

    #[test]
    fn dup_duplicates_and_reorder_swaps() {
        let rx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let to = rx.local_addr().unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        tx.set_faults(FaultConfig {
            seed: 1,
            dup_per_million: 1_000_000,
            ..Default::default()
        });
        tx.send_to(b"a", to).unwrap();
        assert_eq!(recv_all(&rx, 2), vec![b"a".to_vec(), b"a".to_vec()]);

        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        tx.set_faults(FaultConfig {
            seed: 1,
            reorder_per_million: 1_000_000,
            ..Default::default()
        });
        tx.send_to(b"first", to).unwrap(); // held back
        tx.send_to(b"second", to).unwrap(); // goes out, then flushes "first"
        assert_eq!(
            recv_all(&rx, 2),
            vec![b"second".to_vec(), b"first".to_vec()]
        );
    }

    #[test]
    fn muzzle_passes_election_traffic_only() {
        use uc_protocol::v2::datagram::{
            DATAGRAM_HEADER_LEN, DGRAM_KIND_DATA, DGRAM_KIND_HEARTBEAT, DGRAM_KIND_REQUEST_VOTE,
            DGRAM_KIND_VOTE, OFF_DGRAM_KIND,
        };
        fn dgram(kind: u8) -> Vec<u8> {
            let mut b = vec![0u8; DATAGRAM_HEADER_LEN];
            b[OFF_DGRAM_KIND] = kind;
            b
        }
        let rx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let to = rx.local_addr().unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let part = tx.partition_handle();

        // Muzzled: the data plane (DATA/HEARTBEAT — and anything else that is
        // not an election kind, including a runt shorter than the header) is
        // dropped whole; REQUEST_VOTE and VOTE pass.
        part.block_except_election(to);
        tx.send_to(&dgram(DGRAM_KIND_DATA), to).unwrap();
        tx.send_to(&dgram(DGRAM_KIND_HEARTBEAT), to).unwrap();
        tx.send_to(b"runt", to).unwrap();
        tx.send_to(&dgram(DGRAM_KIND_REQUEST_VOTE), to).unwrap();
        tx.send_to(&dgram(DGRAM_KIND_VOTE), to).unwrap();
        let got = recv_all(&rx, 2);
        assert_eq!(got.len(), 2, "exactly the two election datagrams pass");
        assert_eq!(got[0][OFF_DGRAM_KIND], DGRAM_KIND_REQUEST_VOTE);
        assert_eq!(got[1][OFF_DGRAM_KIND], DGRAM_KIND_VOTE);

        // A full block supersedes the muzzle: now even election kinds drop.
        part.block(to);
        tx.send_to(&dgram(DGRAM_KIND_VOTE), to).unwrap();
        let mut buf = [0u8; 32];
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            rx.recv_from(&mut buf).unwrap().is_none(),
            "full block must drop votes too"
        );

        // clear() heals the muzzle along with the blocks.
        part.clear();
        tx.send_to(&dgram(DGRAM_KIND_DATA), to).unwrap();
        assert_eq!(recv_all(&rx, 1).len(), 1, "clear() must lift the muzzle");
    }

    #[test]
    fn partition_blocks_then_unblocks_one_peer() {
        let rx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let to = rx.local_addr().unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let part = tx.partition_handle();

        // clean baseline: delivery works
        tx.send_to(b"a", to).unwrap();
        assert_eq!(recv_all(&rx, 1), vec![b"a".to_vec()]);

        // block the peer: sends are dropped whole (no RNG consumed, no fault)
        part.block(to);
        for _ in 0..10 {
            tx.send_to(b"blocked", to).unwrap();
        }
        // nothing arrives within a short settle
        let mut buf = [0u8; 16];
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            rx.recv_from(&mut buf).unwrap().is_none(),
            "partitioned peer still received"
        );

        // unblock: delivery resumes
        part.unblock(to);
        tx.send_to(b"b", to).unwrap();
        assert_eq!(recv_all(&rx, 1), vec![b"b".to_vec()]);

        // clear() heals everything too
        part.block(to);
        part.clear();
        tx.send_to(b"c", to).unwrap();
        assert_eq!(recv_all(&rx, 1), vec![b"c".to_vec()]);
    }

    #[test]
    fn empty_partition_set_does_not_consume_rng() {
        // A partition check on an empty set must not touch the RNG, or a seeded
        // drop run would desync. Two sockets, identical drop seed: one with an
        // (empty) partition handle queried, one untouched — identical output.
        let rx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let to = rx.local_addr().unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        tx.set_faults(FaultConfig {
            seed: 99,
            drop_per_million: 500_000,
            ..Default::default()
        });
        let _part = tx.partition_handle(); // exists but empty — must be inert
        for i in 0..100u8 {
            tx.send_to(&[i], to).unwrap();
        }
        let mut rng = XorShift64::new(99);
        let expected: Vec<Vec<u8>> = (0..100u8)
            .filter(|_| !rng.chance(500_000))
            .map(|i| vec![i])
            .collect();
        let got = recv_all(&rx, expected.len());
        assert_eq!(
            got, expected,
            "empty partition set perturbed the seeded drop sequence"
        );
    }

    // ================================================================
    // M8 Task 14: corrupt/replay knobs (adversarial tier)
    // ================================================================

    #[test]
    fn corrupt_flips_exactly_one_bit_and_leaves_length_unchanged() {
        let rx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let to = rx.local_addr().unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        // Certainty (1_000_000/million): every send is corrupted.
        tx.set_faults(FaultConfig {
            seed: 3,
            corrupt_per_million: 1_000_000,
            ..Default::default()
        });
        let original = vec![0xAAu8; 32];
        tx.send_to(&original, to).unwrap();
        let got = recv_all(&rx, 1);
        assert_eq!(got.len(), 1);
        let corrupted = &got[0];
        assert_eq!(
            corrupted.len(),
            original.len(),
            "corruption must not change the length"
        );
        let diff_bits: u32 = corrupted
            .iter()
            .zip(&original)
            .map(|(a, b)| (a ^ b).count_ones())
            .sum();
        assert_eq!(
            diff_bits, 1,
            "corruption must flip EXACTLY one bit, got {diff_bits}"
        );
    }

    #[test]
    fn replay_redelivers_a_stashed_datagram_in_addition_to_the_current_one() {
        let rx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let to = rx.local_addr().unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        // Certainty: every send after the first also re-delivers a past one.
        tx.set_faults(FaultConfig {
            seed: 5,
            replay_per_million: 1_000_000,
            ..Default::default()
        });
        tx.send_to(b"one", to).unwrap();
        tx.send_to(b"two", to).unwrap();
        // "one" once (its own send), "two" once (its own send) plus at least
        // one replay of something already seen ("one" is the only candidate
        // for the replay following "two"'s send).
        let got = recv_all(&rx, 3);
        assert_eq!(got, vec![b"one".to_vec(), b"two".to_vec(), b"one".to_vec()]);
    }

    #[test]
    fn corrupt_and_replay_knobs_at_zero_are_byte_for_byte_inert() {
        // The discriminating property for the "costs nothing by default"
        // claim on BOTH new knobs at once (merged from two near-duplicate
        // tests during T14 review — each on its own was byte-for-byte the
        // pre-existing `empty_partition_set_does_not_consume_rng`, testing
        // only that leaving the OTHER (drop) knob's sequence undisturbed;
        // the only genuinely new signal was `history.is_empty()`, kept
        // here): with `corrupt_per_million`/`replay_per_million` at their
        // Default (0), the received bytes are byte-identical to a plain
        // seeded drop run, the drop-sequence RNG draw is unperturbed, and no
        // replay history is ever recorded.
        let rx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let to = rx.local_addr().unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        tx.set_faults(FaultConfig {
            seed: 99,
            drop_per_million: 500_000,
            ..Default::default()
        });
        for i in 0..100u8 {
            tx.send_to(&[i], to).unwrap();
        }
        let mut rng = XorShift64::new(99);
        let expected: Vec<Vec<u8>> = (0..100u8)
            .filter(|_| !rng.chance(500_000))
            .map(|i| vec![i])
            .collect();
        let got = recv_all(&rx, expected.len());
        assert_eq!(
            got, expected,
            "corrupt/replay at 0 must be byte-for-byte inert"
        );
        assert!(
            tx.history.is_empty(),
            "replay_per_million=0 must never record history"
        );
    }
}
