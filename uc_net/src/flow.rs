// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Quorum-paced flow control (spec §5): each follower advertises
//! `contiguous + receive_window`; the sender's limit is the quorum-th order
//! statistic over those — a slow follower never stalls what the quorum could
//! legally commit (deliberately NOT min/lockstep). It recovers via NAK or,
//! below the buffer tail, via a replay session (M4).

use std::net::SocketAddr;

pub struct FlowControl {
    /// (voting follower, latest advertised limit = contiguous + window). ONLY
    /// these enter the quorum order statistic in `limit()`.
    followers: Vec<(SocketAddr, u64)>,
    /// M6 Task 7: learner adverts, kept for observability ONLY. A learner is
    /// replicated-to but never counted, so its window must never move `limit()` —
    /// it lives in this parallel list, not `followers`. Stored as the latest
    /// advertised contiguous position (`learner_position`) for the per-learner
    /// cnc band (Task 9).
    learners: Vec<(SocketAddr, u64)>,
    /// Voting followers needed beyond the leader for a quorum.
    needed: usize,
}

impl FlowControl {
    /// `voting_followers` are the peers whose adverts pace commit; `learners` are
    /// fanned-out to identically but excluded from `limit()`. `cluster_size` is
    /// the VOTING cluster size (learners are not members).
    pub fn new(
        voting_followers: &[SocketAddr],
        cluster_size: usize,
        initial_window: u64,
        learners: &[SocketAddr],
    ) -> Self {
        assert!(
            cluster_size > voting_followers.len(),
            "leader + voters exceed cluster"
        );
        let needed = (cluster_size / 2 + 1).saturating_sub(1);
        assert!(
            needed <= voting_followers.len(),
            "not enough voters for a quorum"
        );
        Self {
            followers: voting_followers
                .iter()
                .map(|a| (*a, initial_window))
                .collect(),
            learners: learners.iter().map(|a| (*a, 0)).collect(),
            needed,
        }
    }

    /// Latest-wins (windows legitimately shrink as a receiver fills). Routes by
    /// membership: a voter's advert updates its quorum-relevant limit; a learner's
    /// is recorded for observability but never enters `limit()`; an unknown source
    /// is ignored.
    pub fn on_status(&mut self, from: SocketAddr, contiguous: u64, window: u32) {
        if let Some(f) = self.followers.iter_mut().find(|(a, _)| *a == from) {
            f.1 = contiguous + window as u64;
        } else if let Some(l) = self.learners.iter_mut().find(|(a, _)| *a == from) {
            l.1 = contiguous;
        }
    }

    /// M6 Task 7/9: the latest contiguous position advertised by a learner, for
    /// the per-learner cnc observability band. `None` if not a configured learner.
    pub fn learner_position(&self, addr: SocketAddr) -> Option<u64> {
        self.learners
            .iter()
            .find(|(a, _)| *a == addr)
            .map(|(_, p)| *p)
    }

    /// M6 Task 9: the per-peer flow value for the cnc observability band — a
    /// voter's advertised ceiling (`contiguous + window`) or a learner's latest
    /// contiguous position. `None` if `addr` is neither a follower nor a learner.
    pub fn advertised_limit(&self, addr: SocketAddr) -> Option<u64> {
        self.followers
            .iter()
            .chain(self.learners.iter())
            .find(|(a, _)| *a == addr)
            .map(|(_, l)| *l)
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
        let mut f = FlowControl::new(&[a, b], 3, 65536, &[]);
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
        let mut f = FlowControl::new(&fs, 5, 1000, &[]);
        for (i, a) in fs.iter().enumerate() {
            f.on_status(*a, (i as u64 + 1) * 1000, 0);
        }
        // limits: 1000 2000 3000 4000; quorum 3 needs 2 followers -> 2nd highest
        assert_eq!(f.limit(), 3000);
    }

    #[test]
    fn unknown_source_is_ignored() {
        let a = addr(1);
        let mut f = FlowControl::new(&[a], 2, 500, &[]);
        f.on_status(addr(9), 1 << 40, 1 << 20); // not a configured follower
        assert_eq!(f.limit(), 500);
    }

    #[test]
    fn learner_status_never_moves_the_limit() {
        // Two voters + one learner. The learner adverts a sky-high window; the
        // quorum statistic must stay the voters' — a learner is replicated-to but
        // never counted (M6 Task 7).
        let (a, b, l) = (addr(1), addr(2), addr(3));
        let mut f = FlowControl::new(&[a, b], 3, 1000, &[l]);
        f.on_status(a, 5_000, 0);
        f.on_status(b, 4_000, 0);
        assert_eq!(f.limit(), 5_000); // faster of the two voters
        // Learner screams the maximum: limit is unmoved, but it is recorded.
        f.on_status(l, 1 << 40, 1 << 20);
        assert_eq!(
            f.limit(),
            5_000,
            "a learner advert must never enter the quorum statistic"
        );
        assert_eq!(
            f.learner_position(l),
            Some(1 << 40),
            "learner position tracked for observability"
        );
        // A voter slowing does move the limit; the learner still does not.
        f.on_status(a, 2_000, 0);
        assert_eq!(f.limit(), 4_000);
    }
}
