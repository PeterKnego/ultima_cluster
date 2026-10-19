// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Wire-level spec §4.1: the raise predicate, pure so the leader (auto),
//! the admin door (`uc2ctl wire raise`) and `uc2ctl wire show` all answer
//! the same question from the same inputs.

use std::collections::HashMap;
use uc_consensus::election::NodeId;

/// Why the committed wire level cannot be raised right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireHoldout {
    /// Every member reported and none is above the committed level.
    AtLevel,
    /// A member in the committed membership has not reported (this term), or
    /// reported zero.
    NoReport(NodeId),
    /// The first member (membership order, self included with its own max)
    /// whose max is at or below the committed level while another member's
    /// is above it.
    Below { id: NodeId, reported: u8 },
}

/// `Ok(target)` iff every member has a non-zero report and `target =
/// min(own_max, reported…) > committed`. Members are checked in `members`
/// order with `self_id` read from `own_max`, so the holdout named is
/// deterministic. When the minimum is not above `committed` but some member's
/// max is, the first member at or below `committed` is named (`Below`);
/// otherwise nothing is above the committed level (`AtLevel`).
pub fn wire_raise_target(
    own_max: u8,
    committed: u8,
    members: &[NodeId],
    self_id: NodeId,
    reported: &HashMap<NodeId, u8>,
) -> Result<u8, WireHoldout> {
    let max_of = |id: NodeId| -> u8 {
        if id == self_id {
            own_max
        } else {
            reported.get(&id).copied().unwrap_or(0)
        }
    };
    let mut target = own_max;
    let mut highest = own_max;
    for &id in members {
        let max = max_of(id);
        if max == 0 {
            return Err(WireHoldout::NoReport(id));
        }
        target = target.min(max);
        highest = highest.max(max);
    }
    if target > committed {
        return Ok(target);
    }
    if highest > committed
        && let Some(&id) = members.iter().find(|&&id| max_of(id) <= committed)
    {
        return Err(WireHoldout::Below {
            id,
            reported: max_of(id),
        });
    }
    Err(WireHoldout::AtLevel)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reported(pairs: &[(NodeId, u8)]) -> HashMap<NodeId, u8> {
        pairs.iter().copied().collect()
    }

    #[test]
    fn raisable_when_every_member_reports_the_next_level() {
        let r = reported(&[(2, 13), (3, 13)]);
        assert_eq!(wire_raise_target(13, 12, &[1, 2, 3], 1, &r), Ok(13));
    }

    #[test]
    fn at_level_when_nothing_is_above_the_committed_level() {
        let r = reported(&[(2, 12), (3, 12)]);
        assert_eq!(
            wire_raise_target(12, 12, &[1, 2, 3], 1, &r),
            Err(WireHoldout::AtLevel)
        );
    }

    #[test]
    fn wire_raise_target_blocks_on_a_silent_learner() {
        // Review Focus 3: a member (learner 4) with no report this term blocks.
        let r = reported(&[(2, 13), (3, 13)]);
        assert_eq!(
            wire_raise_target(13, 12, &[1, 2, 3, 4], 1, &r),
            Err(WireHoldout::NoReport(4))
        );
    }

    #[test]
    fn the_first_holdout_below_target_is_named_with_its_byte() {
        let r = reported(&[(2, 12), (3, 13)]);
        assert_eq!(
            wire_raise_target(13, 12, &[1, 2, 3], 1, &r),
            Err(WireHoldout::Below {
                id: 2,
                reported: 12
            })
        );
    }

    #[test]
    fn the_target_is_the_minimum_and_at_most_one_above() {
        // self at 14 (impossible under one-level-back, but the function must
        // still answer min): members at 13 → target 13.
        let r = reported(&[(2, 13), (3, 14)]);
        assert_eq!(wire_raise_target(14, 12, &[1, 2, 3], 1, &r), Ok(13));
    }

    #[test]
    fn the_leaders_own_max_counts() {
        let r = reported(&[(2, 13), (3, 13)]);
        assert_eq!(
            wire_raise_target(12, 12, &[1, 2, 3], 1, &r),
            Err(WireHoldout::Below {
                id: 1,
                reported: 12
            })
        );
    }
}
