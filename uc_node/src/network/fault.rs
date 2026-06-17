//! Test-only network fault injection (cargo feature `fault-injection`).
//!
//! A [`FaultTable`] is a shared set of blocked node-pairs, consulted at the QUIC
//! send chokepoint ([`super::instance::QuicRaftNetwork`]) to simulate a network
//! partition: a blocked `(src, dst)` pair makes the outbound RPC fail with
//! [`super::NetworkError::Disconnected`] before it reaches the wire, which
//! openraft treats as a normal unreachable peer.
//!
//! Partitions are **symmetric**: [`FaultTable::set_partition`] / [`FaultTable::isolate`]
//! insert both directions, so blocking A↔B drops A→B and B→A. The whole module is
//! compiled out without the `fault-injection` feature.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::raft::NodeId;

/// Shared table of blocked ordered node-pairs. Clone the `Arc` into every node in
/// a test cluster so the harness can change the partition for all nodes at once.
///
/// Beyond whole-link partitions (`blocked`), the table also carries per-link
/// *segment-level* faults — a drop probability (`loss`) and a fixed added
/// latency (`delay_ms`) — applied at the UDP mux receive chokepoint so they
/// exercise the channel's NAK/retransmit path rather than failing the whole RPC.
#[derive(Debug, Default)]
pub struct FaultTable {
    blocked: Mutex<HashSet<(NodeId, NodeId)>>,
    /// Per-link inbound-segment drop probability in `[0.0, 1.0]`. Unset ⇒ 0.0.
    loss: Mutex<HashMap<(NodeId, NodeId), f64>>,
    /// Per-link added inbound-segment latency in milliseconds. Unset ⇒ 0.
    delay_ms: Mutex<HashMap<(NodeId, NodeId), u64>>,
}

impl FaultTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// True if `src` is currently forbidden from sending to `dst`.
    pub fn is_blocked(&self, src: NodeId, dst: NodeId) -> bool {
        self.blocked.lock().unwrap().contains(&(src, dst))
    }

    /// Replace the partition with one where nodes in different `groups` cannot
    /// talk (both directions), while nodes within a group can. Expresses any
    /// split, including a three-way `[[1],[2],[3]]` total quorum loss.
    pub fn set_partition(&self, groups: &[Vec<NodeId>]) {
        let mut b = self.blocked.lock().unwrap();
        b.clear();
        for (gi, g) in groups.iter().enumerate() {
            for (hi, h) in groups.iter().enumerate() {
                if gi == hi {
                    continue;
                }
                for &a in g {
                    for &c in h {
                        b.insert((a, c));
                    }
                }
            }
        }
    }

    /// Isolate `node` from every other node in `all` (both directions). Leaves
    /// any existing blocks in place (additive).
    pub fn isolate(&self, node: NodeId, all: &[NodeId]) {
        let mut b = self.blocked.lock().unwrap();
        for &other in all {
            if other == node {
                continue;
            }
            b.insert((node, other));
            b.insert((other, node));
        }
    }

    /// Clear all blocks — heal the network.
    pub fn heal(&self) {
        self.blocked.lock().unwrap().clear();
    }

    /// Set the inbound-segment drop probability for the `src → dst` link.
    pub fn set_loss(&self, src: NodeId, dst: NodeId, drop_prob: f64) {
        self.loss.lock().unwrap().insert((src, dst), drop_prob);
    }

    /// Drop probability for `src → dst` (0.0 if unset).
    pub fn loss(&self, src: NodeId, dst: NodeId) -> f64 {
        self.loss
            .lock()
            .unwrap()
            .get(&(src, dst))
            .copied()
            .unwrap_or(0.0)
    }

    /// Set the added inbound-segment latency (ms) for the `src → dst` link.
    pub fn set_delay(&self, src: NodeId, dst: NodeId, ms: u64) {
        self.delay_ms.lock().unwrap().insert((src, dst), ms);
    }

    /// Added latency (ms) for `src → dst` (0 if unset).
    pub fn delay(&self, src: NodeId, dst: NodeId) -> u64 {
        self.delay_ms
            .lock()
            .unwrap()
            .get(&(src, dst))
            .copied()
            .unwrap_or(0)
    }

    /// True if a segment on `src → dst` should be dropped given a random draw
    /// `roll` in `[0.0, 1.0)`. The caller supplies the draw so the decision is
    /// deterministic/seedable for tests.
    pub fn should_drop(&self, src: NodeId, dst: NodeId, roll: f64) -> bool {
        roll < self.loss(src, dst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_blocks_nothing() {
        let t = FaultTable::new();
        assert!(!t.is_blocked(1, 2));
        assert!(!t.is_blocked(2, 1));
    }

    #[test]
    fn isolate_blocks_both_directions_only_for_node() {
        let t = FaultTable::new();
        t.isolate(1, &[1, 2, 3]);
        assert!(t.is_blocked(1, 2));
        assert!(t.is_blocked(2, 1));
        assert!(t.is_blocked(1, 3));
        assert!(t.is_blocked(3, 1));
        assert!(!t.is_blocked(2, 3));
        assert!(!t.is_blocked(3, 2));
    }

    #[test]
    fn set_partition_two_groups() {
        let t = FaultTable::new();
        t.set_partition(&[vec![1], vec![2, 3]]);
        assert!(t.is_blocked(1, 2));
        assert!(t.is_blocked(2, 1));
        assert!(t.is_blocked(1, 3));
        assert!(t.is_blocked(3, 1));
        assert!(!t.is_blocked(2, 3));
        assert!(!t.is_blocked(3, 2));
    }

    #[test]
    fn set_partition_three_way_blocks_all_cross_pairs() {
        let t = FaultTable::new();
        t.set_partition(&[vec![1], vec![2], vec![3]]);
        for (a, b) in [(1, 2), (2, 1), (1, 3), (3, 1), (2, 3), (3, 2)] {
            assert!(t.is_blocked(a, b), "({a},{b}) should be blocked");
        }
    }

    #[test]
    fn loss_probability_threshold() {
        let t = FaultTable::new();
        t.set_loss(1, 2, 0.5);
        assert!(t.should_drop(1, 2, 0.4)); // below prob → drop
        assert!(!t.should_drop(1, 2, 0.6)); // above prob → pass
        assert!(!t.should_drop(2, 1, 0.1)); // unset pair → never drop
    }

    #[test]
    fn delay_lookup() {
        let t = FaultTable::new();
        t.set_delay(1, 2, 25);
        assert_eq!(t.delay(1, 2), 25);
        assert_eq!(t.delay(2, 1), 0);
    }

    #[test]
    fn set_partition_replaces_and_heal_clears() {
        let t = FaultTable::new();
        t.isolate(1, &[1, 2, 3]);
        t.set_partition(&[vec![1, 2, 3]]); // single group → nothing blocked
        assert!(!t.is_blocked(1, 2));
        t.set_partition(&[vec![1], vec![2, 3]]);
        assert!(t.is_blocked(1, 2));
        t.heal();
        assert!(!t.is_blocked(1, 2));
    }
}
