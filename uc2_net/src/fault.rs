// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Nonblocking UDP socket with a built-in, seeded fault layer (spec §8 L2:
//! native to own-UDP, day one). Faults are applied on the SEND side so a
//! seeded run is deterministic: drop skips the syscall, dup sends twice,
//! reorder holds one datagram back and flushes it after the next send (a
//! held datagram is therefore delayed by at most one send — heartbeats keep
//! sends coming, so nothing is held forever).

use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};

/// Deterministic xorshift64 — no external RNG dependency.
pub(crate) struct XorShift64(u64);

impl XorShift64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed })
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
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self { seed: 1, drop_per_million: 0, dup_per_million: 0, reorder_per_million: 0 }
    }
}

pub struct FaultSocket {
    sock: UdpSocket,
    cfg: FaultConfig,
    rng: XorShift64,
    held: Option<(Vec<u8>, SocketAddr)>,
}

impl FaultSocket {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Self::from_socket(UdpSocket::bind(addr)?)
    }

    pub fn from_socket(sock: UdpSocket) -> io::Result<Self> {
        sock.set_nonblocking(true)?;
        let cfg = FaultConfig::default();
        Ok(Self { sock, rng: XorShift64::new(cfg.seed), cfg, held: None })
    }

    pub fn set_faults(&mut self, cfg: FaultConfig) {
        self.rng = XorShift64::new(cfg.seed);
        self.cfg = cfg;
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
        self.raw_send(buf, to)?;
        if self.cfg.dup_per_million > 0 && self.rng.chance(self.cfg.dup_per_million) {
            self.raw_send(buf, to)?;
        }
        if let Some((b, a)) = self.held.take() {
            self.raw_send(&b, a)?;
        }
        Ok(())
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
        tx.set_faults(FaultConfig { seed: 99, drop_per_million: 500_000, ..Default::default() });
        for i in 0..100u8 {
            tx.send_to(&[i], to).unwrap();
        }
        // loopback is lossless: received = exactly the non-dropped set.
        // Re-derive it from the same seed.
        let mut rng = XorShift64::new(99);
        let expected: Vec<Vec<u8>> =
            (0..100u8).filter(|_| !rng.chance(500_000)).map(|i| vec![i]).collect();
        assert!(!expected.is_empty() && expected.len() < 100);
        let got = recv_all(&rx, expected.len());
        assert_eq!(got, expected);
    }

    #[test]
    fn dup_duplicates_and_reorder_swaps() {
        let rx = FaultSocket::bind("127.0.0.1:0").unwrap();
        let to = rx.local_addr().unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        tx.set_faults(FaultConfig { seed: 1, dup_per_million: 1_000_000, ..Default::default() });
        tx.send_to(b"a", to).unwrap();
        assert_eq!(recv_all(&rx, 2), vec![b"a".to_vec(), b"a".to_vec()]);

        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        tx.set_faults(FaultConfig { seed: 1, reorder_per_million: 1_000_000, ..Default::default() });
        tx.send_to(b"first", to).unwrap(); // held back
        tx.send_to(b"second", to).unwrap(); // goes out, then flushes "first"
        assert_eq!(recv_all(&rx, 2), vec![b"second".to_vec(), b"first".to_vec()]);
    }
}
