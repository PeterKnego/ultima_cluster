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

use std::collections::HashSet;
use std::sync::Mutex;

use crate::raft::NodeId;

/// Shared table of blocked ordered node-pairs. Clone the `Arc` into every node in
/// a test cluster so the harness can change the partition for all nodes at once.
#[derive(Debug, Default)]
pub struct FaultTable {
    blocked: Mutex<HashSet<(NodeId, NodeId)>>,
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
