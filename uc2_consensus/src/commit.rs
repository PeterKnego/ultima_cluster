// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Quorum commit ranking (spec §6): commit = the quorum-th highest of
//! {leader's own durable} ∪ {reported follower durables}, bounded by the
//! leader's own durable, monotonic. "Commit means quorum-fsync'd."
//!
//! Bounded-by-own is not redundant with the rank: followers can legitimately
//! out-fsync the leader (their archives run independently), making the
//! quorum-th highest exceed what the leader itself holds durably — and the
//! leader must never declare committed what it could not itself serve.
//!
//! A member without a tracked follower slot counts as a permanently-zero
//! report: conservative by construction — an untracked member can never help
//! reach quorum, only a real report can.
//!
//! Pure and allocation-free after construction: the agent (uc2_net's sender
//! duty cycle in M3) feeds reports in and stores the result out.

pub struct CommitTracker {
    /// Latest reported durable per follower index; monotonic per slot
    /// (stale UDP-reordered reports never regress).
    reported: Vec<u64>,
    /// Reusable ranking scratch: {own} ∪ reported.
    scratch: Vec<u64>,
    quorum: usize,
    commit: u64,
}

impl CommitTracker {
    pub fn new(n_followers: usize, cluster_size: usize) -> Self {
        // The leader is a member, so cluster_size must exceed the follower
        // count; and the rank below indexes scratch[quorum-1], so there must
        // be enough tracked members to ever reach quorum. n_followers MAY be
        // smaller than cluster_size - 1: an untracked member is a
        // permanently-zero report — conservative, it can never help commit.
        assert!(
            cluster_size > n_followers,
            "cluster_size must exceed n_followers (the leader is a member)"
        );
        assert!(
            n_followers + 1 > cluster_size / 2,
            "not enough tracked followers to ever reach quorum"
        );
        Self {
            reported: vec![0; n_followers],
            scratch: Vec::with_capacity(cluster_size),
            quorum: cluster_size / 2 + 1,
            commit: 0,
        }
    }

    /// Record a follower's reported durable position (AppendPosition).
    pub fn on_durable(&mut self, follower_idx: usize, durable: u64) {
        let r = &mut self.reported[follower_idx];
        *r = (*r).max(durable);
    }

    #[inline]
    pub fn commit(&self) -> u64 {
        self.commit
    }

    /// Clear per-follower reports (term transition: stale-term reports must
    /// not certify bytes in the new term). Commit itself stays monotonic.
    pub fn reset_reports(&mut self) {
        for r in &mut self.reported {
            *r = 0;
        }
    }

    /// Rank the quorum. Returns `Some(new_commit)` iff commit advanced.
    pub fn advance(&mut self, own_durable: u64) -> Option<u64> {
        self.scratch.clear();
        self.scratch.push(own_durable);
        self.scratch.extend_from_slice(&self.reported);
        self.scratch.sort_unstable_by(|a, b| b.cmp(a));
        let ranked = self.scratch[self.quorum - 1].min(own_durable);
        if ranked > self.commit {
            self.commit = ranked;
            Some(ranked)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_node_commit_is_second_highest_bounded_by_own() {
        let mut t = CommitTracker::new(2, 3);
        assert_eq!(t.commit(), 0);
        // no reports yet: {own=1000, 0, 0} -> 2nd highest = 0
        assert_eq!(t.advance(1000), None);
        assert_eq!(t.commit(), 0);
        // one follower at 400: {1000, 400, 0} -> 2nd = 400
        t.on_durable(0, 400);
        assert_eq!(t.advance(1000), Some(400));
        // second follower at 700: {1000, 400, 700} -> 2nd = 700
        t.on_durable(1, 700);
        assert_eq!(t.advance(1000), Some(700));
        // followers ahead of the leader's own durable: bounded by own.
        // (Possible in practice: the leader's archive can lag its own sends.)
        t.on_durable(0, 5000);
        t.on_durable(1, 5000);
        assert_eq!(t.advance(1000), Some(1000));
        assert_eq!(t.advance(1000), None); // no re-advance without movement
        // own durable catches up -> commit follows
        assert_eq!(t.advance(4000), Some(4000));
    }

    #[test]
    fn reports_are_monotonic_per_follower_and_commit_never_regresses() {
        let mut t = CommitTracker::new(2, 3);
        t.on_durable(0, 800);
        t.on_durable(1, 900);
        assert_eq!(t.advance(1000), Some(900));
        // a stale, UDP-reordered report must not regress anything
        t.on_durable(1, 100);
        assert_eq!(t.advance(1000), None);
        assert_eq!(t.commit(), 900);
    }

    #[test]
    fn four_node_even_cluster_commit_is_third_highest() {
        // even cluster: 4/2 + 1 = 3 = a real majority of 4, not 2 —
        // regression guard on the quorum formula for the even case
        let mut t = CommitTracker::new(3, 4);
        t.on_durable(0, 90);
        t.on_durable(1, 80);
        // {own=100, 90, 80, 0} -> quorum 3 -> 3rd highest = 80
        assert_eq!(t.advance(100), Some(80));
        // the silent fourth member alone must never complete the quorum:
        // with only one report, {100, 90, 0, 0} -> 3rd = 0
        let mut t2 = CommitTracker::new(3, 4);
        t2.on_durable(0, 90);
        assert_eq!(t2.advance(100), None);
    }

    #[test]
    fn five_node_commit_is_third_highest() {
        let mut t = CommitTracker::new(4, 5);
        // {own=100, 90, 80, 70, 0} -> quorum 3 -> 3rd highest = 80
        t.on_durable(0, 90);
        t.on_durable(1, 80);
        t.on_durable(2, 70);
        assert_eq!(t.advance(100), Some(80));
    }

    #[test]
    fn quorum_loss_never_commits_on_own_durable_alone() {
        // 3 nodes, both followers silent forever: {own, 0, 0} -> 2nd = 0.
        // The no-phantom-commits property under quorum loss.
        let mut t = CommitTracker::new(2, 3);
        assert_eq!(t.advance(u64::MAX), None);
        assert_eq!(t.commit(), 0);
    }

    #[test]
    fn untracked_member_counts_as_permanent_zero() {
        // 3-node cluster, only 1 tracked follower (the sender-test shape):
        // quorum 2 over {own, f1, missing=0} -> commit = min(own, f1)
        let mut t = CommitTracker::new(1, 3);
        t.on_durable(0, 700);
        assert_eq!(t.advance(1000), Some(700));
        t.on_durable(0, 2000);
        assert_eq!(t.advance(1000), Some(1000)); // still bounded by own
    }

    #[test]
    fn reset_reports_clears_slots_but_keeps_commit() {
        let mut t = CommitTracker::new(2, 3);
        // both followers ahead of own; commit bounded by own = 1000
        t.on_durable(0, 5000);
        t.on_durable(1, 5000);
        assert_eq!(t.advance(1000), Some(1000));
        assert_eq!(t.commit(), 1000);
        // term transition: stale-term reports must not certify the new term
        t.reset_reports();
        // own advances to 6000 but followers are silent (cleared to 0):
        // {6000, 0, 0} -> quorum-2 = 0 -> no advance. (Without the clear, the
        // stale 5000/5000 would wrongly certify 5000.)
        assert_eq!(t.advance(6000), None);
        assert_eq!(t.commit(), 1000);
    }

    #[test]
    #[should_panic(expected = "cluster_size")]
    fn leader_must_be_a_member() {
        let _ = CommitTracker::new(3, 3);
    }

    #[test]
    #[should_panic(expected = "quorum")]
    fn too_few_tracked_followers_is_rejected() {
        let _ = CommitTracker::new(1, 5); // quorum 3 > 2 tracked members
    }
}
