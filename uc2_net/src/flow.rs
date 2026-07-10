// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Quorum-paced flow control (spec §5): each follower advertises
//! `contiguous + receive_window`; the sender's limit is the quorum-th order
//! statistic over those — a slow follower never stalls what the quorum could
//! legally commit (deliberately NOT min/lockstep). It recovers via NAK or,
//! below the buffer tail, via a replay session (M4).

use std::net::SocketAddr;

pub struct FlowControl {
    /// (follower, latest advertised limit = contiguous + window).
    followers: Vec<(SocketAddr, u64)>,
    /// Followers needed beyond the leader for a quorum.
    needed: usize,
}

impl FlowControl {
    pub fn new(followers: &[SocketAddr], cluster_size: usize, initial_window: u64) -> Self {
        assert!(cluster_size > followers.len(), "leader + followers exceed cluster");
        let needed = (cluster_size / 2 + 1).saturating_sub(1);
        assert!(needed <= followers.len(), "not enough followers for a quorum");
        Self { followers: followers.iter().map(|a| (*a, initial_window)).collect(), needed }
    }

    /// Latest-wins (windows legitimately shrink as a receiver fills).
    pub fn on_status(&mut self, from: SocketAddr, contiguous: u64, window: u32) {
        if let Some(f) = self.followers.iter_mut().find(|(a, _)| *a == from) {
            f.1 = contiguous + window as u64;
        }
    }

    /// The sender may not send at or beyond this position.
    pub fn limit(&self) -> u64 {
        if self.needed == 0 {
            return u64::MAX; // solo cluster: nothing to pace against
        }
        let mut limits: Vec<u64> = self.followers.iter().map(|(_, l)| *l).collect();
        limits.sort_unstable_by(|a, b| b.cmp(a));
        limits[self.needed - 1]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    #[test]
    fn three_node_limit_is_the_faster_follower() {
        let (a, b) = (addr(1), addr(2));
        let mut f = FlowControl::new(&[a, b], 3, 65536);
        // bootstrap: both unknown at (0, initial) -> limit = initial
        assert_eq!(f.limit(), 65536);
        f.on_status(a, 1_000_000, 100_000);
        assert_eq!(f.limit(), 1_100_000); // max(1.1M, 64k)
        f.on_status(b, 2_000_000, 50_000);
        assert_eq!(f.limit(), 2_050_000); // the faster of the two
        // a slow follower's shrinking window never drags the limit down
        f.on_status(a, 1_000_000, 0);
        assert_eq!(f.limit(), 2_050_000);
        // statuses are latest-wins, not max: the fast one's window can shrink
        f.on_status(b, 2_000_000, 10_000);
        assert_eq!(f.limit(), 2_010_000);
    }

    #[test]
    fn five_node_limit_is_second_highest() {
        let fs: Vec<SocketAddr> = (1..=4).map(addr).collect();
        let mut f = FlowControl::new(&fs, 5, 1000);
        for (i, a) in fs.iter().enumerate() {
            f.on_status(*a, (i as u64 + 1) * 1000, 0);
        }
        // limits: 1000 2000 3000 4000; quorum 3 needs 2 followers -> 2nd highest
        assert_eq!(f.limit(), 3000);
    }

    #[test]
    fn unknown_source_is_ignored() {
        let a = addr(1);
        let mut f = FlowControl::new(&[a], 2, 500);
        f.on_status(addr(9), 1 << 40, 1 << 20); // not a configured follower
        assert_eq!(f.limit(), 500);
    }
}
