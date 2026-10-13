//! Backup tier (spec `2026-10-10-uc2-backup-tier-design.md`): the off-node
//! copy of the cluster's agreed history — format, target, chain, retention,
//! the `uc2-backup` writer and the offline restore builder.
pub mod chain;
pub mod layout;
pub mod materialize;
pub mod object;
pub mod retention;
pub mod source;
pub mod target;
pub mod writer;

/// Backup-tier 3b spec §5.2: the journal-purge gate for `purge_requires_tiers
/// = k` — the K-th highest confirmed `through` among the committed tiers
/// (`None` for K = 0: no gate); `0` when fewer than K tiers are registered,
/// which holds every purge. Pure: no I/O, the node and `tier show` share it.
pub fn gate_position(k: u8, tiers: &[uc_protocol::v2::backup::BackupTierEntry]) -> Option<u64> {
    if k == 0 {
        return None;
    }
    let mut through: Vec<u64> = tiers.iter().map(|t| t.through).collect();
    through.sort_unstable_by(|a, b| b.cmp(a));
    Some(through.get(k as usize - 1).copied().unwrap_or(0))
}

#[derive(Debug, thiserror::Error)]
pub enum TierError {
    #[error("tier io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tier object {what}: {detail}")]
    Corrupt { what: String, detail: String },
    #[error("tier target {0} belongs to app_id {1}, not {2} (backup_target_foreign)")]
    Foreign(String, String, String),
    /// 3b spec §6.1: a `[tier_source]` whose `TIER` names another cluster.
    /// Distinct from [`Foreign`](Self::Foreign) so an operator can tell the
    /// read-only source from the writer's target.
    #[error(
        "tier source {path} belongs to app_id {tier_app_id}, not {app_id} (tier_source_foreign)"
    )]
    SourceForeign {
        path: String,
        tier_app_id: String,
        app_id: String,
    },
    #[error("tier overlap mismatch at {0} (tier_overlap_mismatch)")]
    OverlapMismatch(String),
    #[error("{0}")]
    Refused(String),
    /// Ruling RF1: the target carries `fence/<Q>` — a restore rolled this
    /// tier's history back to Q, so no writer may extend it.
    #[error(
        "tier target {0}: its history was rolled back at {1} by a restore (fence/{1:020}); point \
         [backup] target at a new, empty directory (backup_target_fenced)"
    )]
    Fenced(String, u64),
    /// C5: the target's filesystem cannot hard-link (SMB/CIFS and some FUSE
    /// mounts), which `put_if_absent` needs for its atomic "iff absent".
    #[error(
        "tier target {0} cannot hard-link ({1}); the backup target needs a filesystem with hard \
         links (target_no_hardlinks)"
    )]
    NoHardlinks(String, String),
    /// The node's own journal could not supply a range (a read error, a
    /// block purged under the cut) — not the target's fault.
    #[error("journal: {0}")]
    Journal(String),
}

#[cfg(test)]
mod gate_tests {
    use super::gate_position;
    use uc_protocol::v2::backup::BackupTierEntry;

    fn e(tier_id: u64, through: u64) -> BackupTierEntry {
        BackupTierEntry {
            tier_id,
            oldest_set: 0,
            newest_set: 0,
            through,
            reporter: 1,
            updated_pos: 0,
        }
    }

    #[test]
    fn gate_position_for_0_1_2_and_4_tiers_k_above_the_count_and_a_forgotten_tier() {
        assert_eq!(gate_position(0, &[]), None);
        assert_eq!(gate_position(0, &[e(1, 500)]), None);
        assert_eq!(gate_position(1, &[e(1, 500)]), Some(500));
        let two = [e(1, 500), e(2, 900)];
        assert_eq!(gate_position(1, &two), Some(900));
        assert_eq!(gate_position(2, &two), Some(500));
        let four = [e(1, 700), e(2, 300), e(3, 900), e(4, 100)];
        assert_eq!(gate_position(4, &four), Some(100));
        assert_eq!(gate_position(3, &four), Some(300));
        assert_eq!(
            gate_position(3, &two),
            Some(0),
            "K above the count holds all"
        );
        assert_eq!(gate_position(2, &two[..1]), Some(0), "a forgotten tier");
    }
}
