// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Cluster membership (M7 spec): `ClusterConfig` + the pure single-server
//! transition `ClusterConfig::apply`. Every membership-change path — leader
//! proposal, follower/leader adoption via `ElectionSm::adopt_config`, boot
//! recovery — shares this ONE function, so every safety precondition
//! (tombstone permanence, structural presence/role, zero-voters, the 8-member
//! cap) is enforced in exactly one place.
//!
//! Pure and sync by construction: no I/O, no clock, no allocation beyond the
//! `Vec`s the config itself owns. Dep-free — `uc2_consensus` has no path
//! dependencies, so `Addr` is a bare `(u32, u16)` tuple; converting it to a
//! real `SocketAddr` is `uc2_node`'s job.

use crate::election::NodeId;

/// (ipv4, port) — deliberately not `SocketAddr`: `uc2_consensus` stays
/// dep-free, so the socket-address conversion lives in `uc2_node`.
pub type Addr = (u32, u16);

/// The 8-member cap (voters + learners, tombstones excluded): keeps the
/// term-map/config wire encoding and the quorum-ranking scratch space
/// bounded.
const MAX_MEMBERS: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    pub version: u64,
    pub voters: Vec<(NodeId, Addr)>,
    pub learners: Vec<(NodeId, Addr)>,
    /// Permanent tombstones: an id that was ever removed (voter or learner)
    /// can never rejoin the cluster under the same id. Checked FIRST in
    /// `apply`, ahead of every other precondition.
    pub tombstones: Vec<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigOp {
    AddLearner { id: NodeId, addr: Addr }, // wire op = 1
    PromoteLearner { id: NodeId },         // 2
    DemoteVoter { id: NodeId },            // 3
    RemoveLearner { id: NodeId },          // 4
    RemoveVoter { id: NodeId },            // 5
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader,                // wire reason = 1
    NotServing,               // 2  (serving gate = the single-server-change precondition)
    ChangePending,            // 3  (one in flight)
    Tombstoned,               // 4
    AlreadyPresent,           // 5
    NotFound,                 // 6
    WrongRole,                // 7  (promote a voter / demote a learner)
    ZeroVoters,               // 8
    TooManyMembers,           // 9  (MAX_MEMBERS = 8)
    NotCaughtUp { gap: u64 }, // 10
}

/// The id a `ConfigOp` targets, regardless of variant.
fn op_id(op: &ConfigOp) -> NodeId {
    match *op {
        ConfigOp::AddLearner { id, .. } => id,
        ConfigOp::PromoteLearner { id } => id,
        ConfigOp::DemoteVoter { id } => id,
        ConfigOp::RemoveLearner { id } => id,
        ConfigOp::RemoveVoter { id } => id,
    }
}

impl ClusterConfig {
    pub fn genesis(voters: Vec<(NodeId, Addr)>, learners: Vec<(NodeId, Addr)>) -> Self {
        Self { version: 0, voters, learners, tombstones: Vec::new() }
    }

    /// PURE single-server transition — every proposal path shares it.
    /// Precondition order: tombstone -> presence/role -> zero-voters -> cap.
    pub fn apply(&self, op: ConfigOp) -> Result<ClusterConfig, ProposeError> {
        let id = op_id(&op);
        if self.tombstones.contains(&id) {
            return Err(ProposeError::Tombstoned);
        }
        let mut next = self.clone();
        match op {
            ConfigOp::AddLearner { id, addr } => {
                if next.contains(id) {
                    return Err(ProposeError::AlreadyPresent);
                }
                if next.voters.len() + next.learners.len() >= MAX_MEMBERS {
                    return Err(ProposeError::TooManyMembers);
                }
                next.learners.push((id, addr));
            }
            ConfigOp::PromoteLearner { id } => match next.learners.iter().position(|(lid, _)| *lid == id) {
                Some(pos) => {
                    let (_, addr) = next.learners.remove(pos);
                    next.voters.push((id, addr));
                }
                None if next.is_voter(id) => return Err(ProposeError::WrongRole),
                None => return Err(ProposeError::NotFound),
            },
            ConfigOp::DemoteVoter { id } => match next.voters.iter().position(|(vid, _)| *vid == id) {
                Some(_) if next.voters.len() <= 1 => return Err(ProposeError::ZeroVoters),
                Some(pos) => {
                    let (_, addr) = next.voters.remove(pos);
                    next.learners.push((id, addr));
                }
                None if next.is_learner(id) => return Err(ProposeError::WrongRole),
                None => return Err(ProposeError::NotFound),
            },
            ConfigOp::RemoveLearner { id } => match next.learners.iter().position(|(lid, _)| *lid == id) {
                Some(pos) => {
                    next.learners.remove(pos);
                    next.tombstones.push(id);
                }
                None if next.is_voter(id) => return Err(ProposeError::WrongRole),
                None => return Err(ProposeError::NotFound),
            },
            ConfigOp::RemoveVoter { id } => match next.voters.iter().position(|(vid, _)| *vid == id) {
                Some(_) if next.voters.len() <= 1 => return Err(ProposeError::ZeroVoters),
                Some(pos) => {
                    next.voters.remove(pos);
                    next.tombstones.push(id);
                }
                None if next.is_learner(id) => return Err(ProposeError::WrongRole),
                None => return Err(ProposeError::NotFound),
            },
        }
        next.version += 1;
        Ok(next)
    }

    pub fn voter_ids(&self) -> Vec<NodeId> {
        self.voters.iter().map(|(id, _)| *id).collect()
    }

    pub fn is_voter(&self, id: NodeId) -> bool {
        self.voters.iter().any(|(vid, _)| *vid == id)
    }

    pub fn is_learner(&self, id: NodeId) -> bool {
        self.learners.iter().any(|(lid, _)| *lid == id)
    }

    /// Voter or learner (does NOT count a tombstone as present).
    pub fn contains(&self, id: NodeId) -> bool {
        self.is_voter(id) || self.is_learner(id)
    }

    /// Wire discriminant for `op` (see the variant comments above).
    pub fn op_code(op: &ConfigOp) -> u32 {
        match op {
            ConfigOp::AddLearner { .. } => 1,
            ConfigOp::PromoteLearner { .. } => 2,
            ConfigOp::DemoteVoter { .. } => 3,
            ConfigOp::RemoveLearner { .. } => 4,
            ConfigOp::RemoveVoter { .. } => 5,
        }
    }

    /// Wire discriminant for `e` (see the variant comments above).
    pub fn reason_code(e: &ProposeError) -> u32 {
        match e {
            ProposeError::NotLeader => 1,
            ProposeError::NotServing => 2,
            ProposeError::ChangePending => 3,
            ProposeError::Tombstoned => 4,
            ProposeError::AlreadyPresent => 5,
            ProposeError::NotFound => 6,
            ProposeError::WrongRole => 7,
            ProposeError::ZeroVoters => 8,
            ProposeError::TooManyMembers => 9,
            ProposeError::NotCaughtUp { .. } => 10,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_enforces_every_precondition() {
        let g = ClusterConfig::genesis(vec![(1, (1, 1)), (2, (2, 2)), (3, (3, 3))], vec![]);
        // add + promote + demote + remove round trip
        let c1 = g.apply(ConfigOp::AddLearner { id: 5, addr: (5, 5) }).unwrap();
        assert_eq!(c1.version, 1);
        assert!(c1.is_learner(5));
        let c2 = c1.apply(ConfigOp::PromoteLearner { id: 5 }).unwrap();
        assert!(c2.is_voter(5));
        let c3 = c2.apply(ConfigOp::DemoteVoter { id: 5 }).unwrap();
        assert!(c3.is_learner(5));
        let c4 = c3.apply(ConfigOp::RemoveLearner { id: 5 }).unwrap();
        assert!(!c4.contains(5));
        assert!(c4.tombstones.contains(&5));
        // tombstone permanence
        assert_eq!(
            c4.apply(ConfigOp::AddLearner { id: 5, addr: (5, 5) }),
            Err(ProposeError::Tombstoned)
        );
        // structural refusals
        assert_eq!(
            g.apply(ConfigOp::AddLearner { id: 1, addr: (9, 9) }),
            Err(ProposeError::AlreadyPresent)
        );
        assert_eq!(g.apply(ConfigOp::PromoteLearner { id: 1 }), Err(ProposeError::WrongRole));
        assert_eq!(g.apply(ConfigOp::RemoveVoter { id: 9 }), Err(ProposeError::NotFound));
        let solo = ClusterConfig::genesis(vec![(1, (1, 1))], vec![]);
        assert_eq!(solo.apply(ConfigOp::RemoveVoter { id: 1 }), Err(ProposeError::ZeroVoters));
        assert_eq!(solo.apply(ConfigOp::DemoteVoter { id: 1 }), Err(ProposeError::ZeroVoters));
        // 8-cap
        let mut big = g.clone();
        for i in 10..15u32 {
            big = big.apply(ConfigOp::AddLearner { id: i, addr: (i, 1) }).unwrap();
        }
        assert_eq!(
            big.apply(ConfigOp::AddLearner { id: 20, addr: (20, 1) }),
            Err(ProposeError::TooManyMembers)
        );
    }

    #[test]
    fn op_code_and_reason_code_match_the_wire_table() {
        assert_eq!(ClusterConfig::op_code(&ConfigOp::AddLearner { id: 1, addr: (1, 1) }), 1);
        assert_eq!(ClusterConfig::op_code(&ConfigOp::PromoteLearner { id: 1 }), 2);
        assert_eq!(ClusterConfig::op_code(&ConfigOp::DemoteVoter { id: 1 }), 3);
        assert_eq!(ClusterConfig::op_code(&ConfigOp::RemoveLearner { id: 1 }), 4);
        assert_eq!(ClusterConfig::op_code(&ConfigOp::RemoveVoter { id: 1 }), 5);

        assert_eq!(ClusterConfig::reason_code(&ProposeError::NotLeader), 1);
        assert_eq!(ClusterConfig::reason_code(&ProposeError::NotServing), 2);
        assert_eq!(ClusterConfig::reason_code(&ProposeError::ChangePending), 3);
        assert_eq!(ClusterConfig::reason_code(&ProposeError::Tombstoned), 4);
        assert_eq!(ClusterConfig::reason_code(&ProposeError::AlreadyPresent), 5);
        assert_eq!(ClusterConfig::reason_code(&ProposeError::NotFound), 6);
        assert_eq!(ClusterConfig::reason_code(&ProposeError::WrongRole), 7);
        assert_eq!(ClusterConfig::reason_code(&ProposeError::ZeroVoters), 8);
        assert_eq!(ClusterConfig::reason_code(&ProposeError::TooManyMembers), 9);
        assert_eq!(ClusterConfig::reason_code(&ProposeError::NotCaughtUp { gap: 1 }), 10);
    }
}
