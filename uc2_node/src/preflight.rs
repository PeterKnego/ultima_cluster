// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Startup refusals.
//!
//! Every rule here exists because the misconfiguration it catches currently
//! fails LATER and looks like something else. A node that refuses to start
//! with a message naming the offending field is strictly better than a
//! cluster that elects a leader and never commits.

use uc2_consensus::election::NodeId;
// The cnc PeerSlots band holds 8 entries; voters + learners share it. Reuse
// the wire crate's constant rather than a local literal — `uc_protocol`
// enforces this same cap at config-frame proposal AND at decode, and a
// second copy here could drift out of agreement with the wire.
use uc_protocol::v2::config::MAX_MEMBERS;

use crate::NodeConfig;

#[derive(Debug, thiserror::Error)]
pub enum PreflightError {
    #[error("buffer_bytes must be a power of two, got {0}")]
    BufferNotPowerOfTwo(usize),
    #[error("max_payload ({max_payload}) must be well below buffer_bytes ({buffer_bytes})")]
    PayloadTooLarge { max_payload: usize, buffer_bytes: usize },
    #[error("this node's id ({0}) appears in neither members nor learners")]
    SelfNotAMember(NodeId),
    #[error("learners and members must be disjoint; id {0} appears in both")]
    LearnerIsAlsoMember(NodeId),
    #[error(
        "bind ({bind}) must be identical to this node's own members entry ({expected}) — \
         not a wildcard, not 0.0.0.0; a mismatch elects a leader whose followers never commit"
    )]
    BindMismatch { bind: String, expected: String },
    #[error("duplicate id {0} in members/learners")]
    DuplicateId(NodeId),
    #[error("cluster has {0} total members (voters + learners); the hard cap is 8")]
    TooManyMembers(usize),
    #[error("election_timeout_min_ns ({min}) must be < election_timeout_max_ns ({max})")]
    ElectionWindow { min: u64, max: u64 },
}

/// Pure semantic checks over a built config. Filesystem checks are separate
/// (Task 3) so this stays unit-testable without touching disk.
pub fn check_semantics(cfg: &NodeConfig) -> Result<(), PreflightError> {
    if !cfg.buffer_bytes.is_power_of_two() {
        return Err(PreflightError::BufferNotPowerOfTwo(cfg.buffer_bytes));
    }
    // Stated as a division, NOT `max_payload * 4 > buffer_bytes`: the multiply
    // overflows for a large `max_payload`, which panics in debug and WRAPS in
    // release — silently admitting the very worst value the check exists to
    // refuse. `buffer_bytes` is a power of two by the check above, so the
    // division is exact.
    if cfg.max_payload > cfg.buffer_bytes / 4 {
        return Err(PreflightError::PayloadTooLarge {
            max_payload: cfg.max_payload,
            buffer_bytes: cfg.buffer_bytes,
        });
    }
    if cfg.election_timeout_min_ns >= cfg.election_timeout_max_ns {
        return Err(PreflightError::ElectionWindow {
            min: cfg.election_timeout_min_ns,
            max: cfg.election_timeout_max_ns,
        });
    }

    let mut seen = std::collections::HashSet::new();
    for (id, _) in cfg.members.iter().chain(cfg.learners.iter()) {
        if !seen.insert(*id) {
            // A learner id colliding with a member id is the more specific
            // (and more confusing) case, so name it as such.
            if cfg.members.iter().any(|(m, _)| m == id)
                && cfg.learners.iter().any(|(l, _)| l == id)
            {
                return Err(PreflightError::LearnerIsAlsoMember(*id));
            }
            return Err(PreflightError::DuplicateId(*id));
        }
    }
    if seen.len() > MAX_MEMBERS {
        return Err(PreflightError::TooManyMembers(seen.len()));
    }

    let own = cfg
        .members
        .iter()
        .chain(cfg.learners.iter())
        .find(|(id, _)| *id == cfg.id)
        .ok_or(PreflightError::SelfNotAMember(cfg.id))?;
    if own.1 != cfg.bind {
        return Err(PreflightError::BindMismatch {
            bind: cfg.bind.to_string(),
            expected: own.1.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CryptoConfig, PurgePolicy};
    use std::net::SocketAddr;

    fn base() -> NodeConfig {
        NodeConfig {
            id: 1,
            members: vec![
                (1, "10.0.0.1:9100".parse::<SocketAddr>().unwrap()),
                (2, "10.0.0.2:9100".parse::<SocketAddr>().unwrap()),
            ],
            learners: Vec::new(),
            bind: "10.0.0.1:9100".parse().unwrap(),
            instance_dir: std::path::PathBuf::from("/srv/uc2/n1"),
            app_id: "myapp".into(),
            buffer_bytes: 1 << 26,
            max_payload: 1 << 20,
            admission_bytes: 256 * 1024,
            election_timeout_min_ns: 150_000_000,
            election_timeout_max_ns: 300_000_000,
            seed: 7,
            faults: Default::default(),
            purge: PurgePolicy::Disabled,
            journal_segment_bytes: crate::DEFAULT_JOURNAL_SEGMENT_BYTES,
            crypto: CryptoConfig::Disabled,
        }
    }

    #[test]
    fn a_valid_config_passes() {
        assert!(check_semantics(&base()).is_ok());
    }

    #[test]
    fn buffer_bytes_must_be_power_of_two() {
        let mut c = base();
        c.buffer_bytes = (1 << 26) + 1;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("buffer_bytes"), "got: {msg}");
    }

    #[test]
    fn max_payload_must_fit_the_buffer() {
        let mut c = base();
        c.max_payload = c.buffer_bytes;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("max_payload"), "got: {msg}");
    }

    /// The check must REFUSE a payload so large that `max_payload * 4`
    /// overflows `usize` — a wrapping multiply would silently ADMIT the worst
    /// possible value. Debug builds would panic instead; both are wrong for a
    /// function whose entire job is refusing bad input.
    #[test]
    fn an_overflowing_max_payload_is_refused_not_wrapped() {
        let mut c = base();
        c.max_payload = usize::MAX / 2;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("max_payload"), "got: {msg}");
    }

    #[test]
    fn own_id_must_appear_in_members_or_learners() {
        let mut c = base();
        c.id = 99;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("99"), "error must name the id, got: {msg}");
    }

    #[test]
    fn learner_ids_must_be_disjoint_from_members() {
        let mut c = base();
        c.learners = vec![(2, "10.0.0.9:9100".parse().unwrap())];
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("learners"), "got: {msg}");
    }

    #[test]
    fn bind_must_equal_this_nodes_own_member_entry() {
        // The failure this prevents: a leader elects, but followers never
        // advance durable/commit, because datagrams arrive from a source
        // address matching no member entry. See how-to/run-a-cluster.md.
        let mut c = base();
        c.bind = "0.0.0.0:9100".parse().unwrap();
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("bind"), "got: {msg}");
        assert!(msg.contains("10.0.0.1:9100"), "error must show the expected addr, got: {msg}");
    }

    #[test]
    fn duplicate_member_ids_are_refused() {
        let mut c = base();
        c.members.push((1, "10.0.0.3:9100".parse().unwrap()));
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("duplicate"), "got: {msg}");
    }

    #[test]
    fn total_membership_is_capped_at_eight() {
        let mut c = base();
        c.members = (1..=9)
            .map(|i| (i, format!("10.0.0.{i}:9100").parse().unwrap()))
            .collect();
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains('8'), "got: {msg}");
    }

    /// The cap must track `uc_protocol`'s wire-enforced `MAX_MEMBERS`, not a
    /// local copy of the literal. If the PeerSlots band ever grows, a stale
    /// duplicate here would refuse configs the wire would happily accept.
    #[test]
    fn the_cap_is_the_protocols_cap() {
        assert_eq!(MAX_MEMBERS, uc_protocol::v2::config::MAX_MEMBERS);
    }

    #[test]
    fn election_window_must_be_ordered() {
        let mut c = base();
        c.election_timeout_min_ns = c.election_timeout_max_ns + 1;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("election_timeout"), "got: {msg}");
    }
}
