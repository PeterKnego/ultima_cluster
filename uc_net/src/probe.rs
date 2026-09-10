// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Path-MTU discovery state (jumbo spec §5.1–§5.3), shared by the sender
//! agent (sends due probes), the receiver agent (records acks, answers
//! probes) and the consensus agent (the leader's commit rule). Not a hot
//! path: a handful of updates per peer per process lifetime, so one mutex.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use uc_protocol::v2::datagram::{MTU_BOUND, RUNGS, is_rung};

/// Spec §5.1: retry every `fast_ns` for the first `fast_attempts`, then every
/// `slow_ns`, until the peer's top rung is verified.
#[derive(Debug, Clone, Copy)]
pub struct ProbeCadence {
    pub fast_ns: u64,
    pub fast_attempts: u32,
    pub slow_ns: u64,
}

impl Default for ProbeCadence {
    fn default() -> Self {
        Self {
            fast_ns: 1_000_000_000,
            fast_attempts: 5,
            slow_ns: 30_000_000_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PeerProbe {
    /// The largest rung this node's probe to the peer was acked at; 0 = none.
    pub verified: u32,
    /// The peer's own minimum, from its latest ack (spec §5.2); 0 = unknown
    /// or the peer itself is unresolved.
    pub advertised: u32,
    pub attempts: u32,
    pub next_due_ns: u64,
}

pub struct ProbeTable {
    cadence: ProbeCadence,
    peers: Mutex<HashMap<SocketAddr, PeerProbe>>,
    unsent: AtomicU64,
}

impl ProbeTable {
    pub fn new(cadence: ProbeCadence) -> Arc<ProbeTable> {
        Arc::new(ProbeTable {
            cadence,
            peers: Mutex::new(HashMap::new()),
            unsent: AtomicU64::new(0),
        })
    }

    /// Replace the peer set: a new peer starts unresolved and due now; a peer
    /// no longer in `peers` is forgotten (spec §5.1, membership change).
    pub fn set_peers(&self, peers: &[SocketAddr]) {
        let mut g = self.peers.lock().unwrap();
        g.retain(|a, _| peers.contains(a));
        for &p in peers {
            g.entry(p).or_default();
        }
    }

    pub fn peers(&self) -> Vec<SocketAddr> {
        self.peers.lock().unwrap().keys().copied().collect()
    }

    pub fn get(&self, peer: SocketAddr) -> Option<PeerProbe> {
        self.peers.lock().unwrap().get(&peer).copied()
    }

    /// The peers whose next probe is due at `now_ns`, each with the rungs
    /// still above its `verified`. Bumps `attempts` and schedules the next
    /// due time, so a caller that sends what it is handed needs nothing else.
    ///
    /// A peer is DONE — and drops out of every later pass — only once BOTH
    /// halves of the pair have nothing left to tell each other: our probes
    /// reached its top rung, and the minimum it advertises back is no lower
    /// than what we verified. Both halves are needed, because `advertised` is
    /// only ever refreshed by an ack and an ack only ever comes back for a
    /// probe WE send. Stopping on `verified` alone latches whatever the peer
    /// happened to know at that instant, forever:
    ///
    /// * A peer that answered our first pass necessarily answered it before it
    ///   had verified anything itself, so its `own_min_rung` on that ack is 0.
    /// * A peer answering mid-ladder advertises a rung it has since climbed
    ///   past — a STALE-LOW value, and the one the loopback test caught under
    ///   load (`uc_net/tests/probe.rs`).
    ///
    /// Either way `table_min` (spec §5.3, the leader's commit rule) takes
    /// `min(verified, advertised)` and refuses a zero, so a latched
    /// advertisement means the cluster never raises its MTU at all.
    ///
    /// So whenever the advertisement is behind what we verified, the pass
    /// APPENDS the verified rung to the rungs it was going to try anyway —
    /// not only when the ladder has nothing left above it. On a CAPPED path
    /// that distinction is the whole feature: every rung above `verified` is
    /// exactly a size that path drops, so without the appended rung no ack can
    /// ever come back and `advertised` never refreshes. The refresh rung goes
    /// LAST, so the freshest advertisement is the one that lands last.
    ///
    /// Cost, per slow tick, while a peer's advertisement stays behind: one
    /// extra datagram of a size the path is already known to carry — three
    /// instead of two on a 1408-capped path, two instead of one on an
    /// 8832-capped one. A GENUINELY asymmetric peer — one whose own minimum is
    /// legitimately below our path to it, because it has a narrow peer of its
    /// own — pays that for as long as that stays true. It is also what
    /// re-discovers a path that later improves.
    pub fn due(&self, now_ns: u64) -> Vec<(SocketAddr, Vec<u32>)> {
        let mut out = Vec::new();
        let mut g = self.peers.lock().unwrap();
        for (&addr, p) in g.iter_mut() {
            let resolved = p.verified as usize >= MTU_BOUND && p.advertised >= p.verified;
            if resolved || now_ns < p.next_due_ns {
                continue;
            }
            let mut rungs: Vec<u32> = RUNGS.iter().copied().filter(|&r| r > p.verified).collect();
            if p.verified > 0 && p.advertised < p.verified {
                // The peer's view of its own path is behind ours of it: ask
                // again at the size we know the path carries. On a capped path
                // this is the ONLY rung in the pass that can be delivered.
                rungs.push(p.verified);
            }
            p.attempts += 1;
            let step = if p.attempts < self.cadence.fast_attempts {
                self.cadence.fast_ns
            } else {
                self.cadence.slow_ns
            };
            p.next_due_ns = now_ns + step;
            out.push((addr, rungs));
        }
        out
    }

    /// An ack from `from`: `rung` was carried whole (only a ladder rung
    /// counts — a forged or garbled value is ignored), and the peer's own
    /// minimum is `own_min_rung`. An ack from an unknown address is ignored.
    pub fn on_ack(&self, from: SocketAddr, rung: u32, own_min_rung: u32) {
        if !is_rung(rung) {
            return;
        }
        let mut g = self.peers.lock().unwrap();
        if let Some(p) = g.get_mut(&from) {
            p.verified = p.verified.max(rung);
            p.advertised = own_min_rung;
        }
    }

    /// Spec §5.2: min over peers of `verified`; 0 while any peer is
    /// unresolved; `MTU_BOUND` for a node with no peers (a solo cluster's
    /// path is loopback).
    pub fn own_min_rung(&self) -> u32 {
        let g = self.peers.lock().unwrap();
        if g.is_empty() {
            return MTU_BOUND as u32;
        }
        g.values().map(|p| p.verified).min().unwrap_or(0)
    }

    /// Spec §5.3: the leader's table minimum over `members` — `Some` only when
    /// every member has an entry whose `verified` and `advertised` are both
    /// non-zero (every pair answered). An empty `members` is `Some(MTU_BOUND)`.
    pub fn table_min(&self, members: &[SocketAddr]) -> Option<u32> {
        let g = self.peers.lock().unwrap();
        let mut min = MTU_BOUND as u32;
        for m in members {
            let p = g.get(m)?;
            if p.verified == 0 || p.advertised == 0 {
                return None;
            }
            min = min.min(p.verified).min(p.advertised);
        }
        Some(min)
    }

    pub fn note_unsent(&self) {
        self.unsent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn unsent(&self) -> u64 {
        self.unsent.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn fast() -> ProbeCadence {
        ProbeCadence {
            fast_ns: 10,
            fast_attempts: 2,
            slow_ns: 100,
        }
    }

    #[test]
    fn a_new_peer_is_due_now_with_every_rung_then_follows_the_cadence() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1)]);
        let d = t.due(0);
        assert_eq!(d, vec![(a(1), vec![1408, 8832, 8960])]);
        assert!(t.due(5).is_empty(), "not due again before fast_ns");
        assert_eq!(t.due(10).len(), 1); // attempt 2 (still fast)
        assert!(t.due(25).is_empty(), "attempt 3 is on the slow cadence");
        assert_eq!(t.due(120).len(), 1);
    }

    #[test]
    fn an_ack_raises_verified_and_narrows_the_next_probe() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1)]);
        t.due(0);
        // A LEVEL advertisement (`own_min_rung` == the rung it acked), so this
        // test stays about narrowing alone — the refresh rung a peer whose
        // advertisement is behind also gets has its own test below.
        t.on_ack(a(1), 8832, 8832);
        assert_eq!(t.get(a(1)).unwrap().verified, 8832);
        assert_eq!(t.due(10), vec![(a(1), vec![8960])]);
        // Fully resolved: top rung verified AND the peer's own minimum is no
        // lower (Task 4 sharpened the rule — see `due`; the value here was
        // 8832, an advertisement the pair would still be reconciling).
        t.on_ack(a(1), 8960, 8960);
        assert!(t.due(1_000).is_empty(), "resolved: probing stops");
        // A lower late ack never lowers; a non-rung is ignored.
        t.on_ack(a(1), 1408, 8832);
        t.on_ack(a(1), 1500, 8832);
        assert_eq!(t.get(a(1)).unwrap().verified, 8960);
        // An ack from a stranger is ignored.
        t.on_ack(a(9), 8960, 8960);
        assert!(t.get(a(9)).is_none());
    }

    /// Task 4 (found by `uc_net/tests/probe.rs`): a peer whose advertisement is
    /// BEHIND what we verified must keep being probed at the verified rung.
    /// Both latches are real — a fresh cluster's first exchange necessarily
    /// carries `own_min_rung() == 0`, and a mid-ladder answer carries a rung
    /// the peer has since climbed past — and either one leaves `table_min`
    /// (spec §5.3) permanently low or `None`.
    #[test]
    fn a_peer_whose_advertisement_is_behind_is_still_probed_at_its_verified_rung() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1)]);
        t.due(0);
        t.on_ack(a(1), MTU_BOUND as u32, 0); // the peer had verified nothing yet
        assert_eq!(
            t.due(1_000),
            vec![(a(1), vec![MTU_BOUND as u32])],
            "top rung verified, advertisement unknown: re-ask at the verified size"
        );
        t.on_ack(a(1), MTU_BOUND as u32, 8832); // mid-ladder: stale-low
        assert_eq!(
            t.due(2_000),
            vec![(a(1), vec![MTU_BOUND as u32])],
            "advertisement still below what we verified: keep asking"
        );
        t.on_ack(a(1), MTU_BOUND as u32, MTU_BOUND as u32);
        assert!(t.due(3_000).is_empty(), "resolved: probing stops");
    }

    /// The same rule on a CAPPED path — the case the whole feature exists for,
    /// and the one where it actually bites: every rung ABOVE `verified` is a
    /// size the path drops, so the appended verified rung is the only datagram
    /// in the pass that can draw an ack back at all. Without it `advertised`
    /// stays 0 forever and this peer's `table_min` is permanently `None`.
    #[test]
    fn a_capped_path_is_re_probed_at_its_verified_rung_too_not_only_at_the_top() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1)]);
        t.due(0);
        t.on_ack(a(1), 8832, 0); // 8960 was dropped by the path; nobody has acked us yet
        assert_eq!(
            t.due(1_000),
            vec![(a(1), vec![8960, 8832])],
            "the untried rung above, THEN the verified rung — freshest ack last"
        );
        t.on_ack(a(1), 8832, 8832); // the peer's view caught up
        assert_eq!(
            t.due(2_000),
            vec![(a(1), vec![8960])],
            "advertisement level: only the untried rung above is left"
        );
        assert_eq!(t.table_min(&[a(1)]), Some(8832));
    }

    #[test]
    fn own_min_is_zero_while_any_peer_is_unresolved() {
        let t = ProbeTable::new(fast());
        assert_eq!(t.own_min_rung(), MTU_BOUND as u32, "no peers: loopback");
        t.set_peers(&[a(1), a(2)]);
        assert_eq!(t.own_min_rung(), 0);
        t.on_ack(a(1), 8960, 0);
        assert_eq!(t.own_min_rung(), 0);
        t.on_ack(a(2), 8832, 0);
        assert_eq!(t.own_min_rung(), 8832);
    }

    #[test]
    fn table_min_needs_every_member_verified_and_advertising() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1), a(2)]);
        let members = [a(1), a(2)];
        assert_eq!(t.table_min(&members), None);
        t.on_ack(a(1), 8960, 8960);
        assert_eq!(t.table_min(&members), None, "a(2) silent");
        t.on_ack(a(2), 8960, 0);
        assert_eq!(t.table_min(&members), None, "a(2) itself unresolved");
        t.on_ack(a(2), 8960, 8832);
        assert_eq!(
            t.table_min(&members),
            Some(8832),
            "a(2)'s own path is the min"
        );
        // A member not in the table (never set as a peer) blocks the rule.
        assert_eq!(t.table_min(&[a(1), a(2), a(3)]), None);
        assert_eq!(t.table_min(&[]), Some(MTU_BOUND as u32));
    }

    #[test]
    fn set_peers_forgets_removed_and_keeps_known() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1), a(2)]);
        t.on_ack(a(1), 8960, 8960);
        t.set_peers(&[a(1), a(3)]);
        assert_eq!(t.get(a(1)).unwrap().verified, 8960);
        assert!(t.get(a(2)).is_none());
        assert_eq!(t.get(a(3)).unwrap(), PeerProbe::default());
    }
}
