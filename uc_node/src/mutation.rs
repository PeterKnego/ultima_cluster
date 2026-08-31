// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Elle mutation-testing fault injection (design spec 2026-07-15,
//! scripts/elle_mutation.sh). Compiled ONLY under `--features
//! mutation-testing`; even then inert unless `UC2_MUTATION` names a mutation.
//! The env var is read exactly once (OnceLock); an unknown value panics so a
//! typo'd mutation run can never silently test nothing.

use std::sync::OnceLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mutation {
    /// CommitTracker ranks at quorum-1: commit without a real quorum.
    CommitQuorumMinusOne,
    /// ElectionSm grants votes ignoring the (last_term, last_durable) order.
    SkipVoteOrderCheck,
    /// Linearizable reads skip the READ_PROBE quorum barrier (stale reads —
    /// a pure real-time anomaly, caught only by the strict elle model).
    SkipReadBarrier,
}

fn parse(v: Option<&str>) -> Option<Mutation> {
    match v {
        None | Some("") => None,
        Some("commit-quorum-minus-one") => Some(Mutation::CommitQuorumMinusOne),
        Some("skip-vote-order-check") => Some(Mutation::SkipVoteOrderCheck),
        Some("skip-read-barrier") => Some(Mutation::SkipReadBarrier),
        Some(other) => panic!("unknown UC2_MUTATION value: {other:?}"),
    }
}

/// The active mutation, if any. Env read once, process-wide.
pub(crate) fn active() -> Option<Mutation> {
    static ACTIVE: OnceLock<Option<Mutation>> = OnceLock::new();
    *ACTIVE.get_or_init(|| parse(std::env::var("UC2_MUTATION").ok().as_deref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_maps_known_values() {
        assert_eq!(parse(None), None);
        assert_eq!(parse(Some("")), None);
        assert_eq!(
            parse(Some("commit-quorum-minus-one")),
            Some(Mutation::CommitQuorumMinusOne)
        );
        assert_eq!(
            parse(Some("skip-vote-order-check")),
            Some(Mutation::SkipVoteOrderCheck)
        );
        assert_eq!(
            parse(Some("skip-read-barrier")),
            Some(Mutation::SkipReadBarrier)
        );
    }

    #[test]
    #[should_panic(expected = "unknown UC2_MUTATION")]
    fn parse_panics_on_unknown() {
        parse(Some("skip-everything"));
    }
}
