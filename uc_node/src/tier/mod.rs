//! Backup tier (spec `2026-10-10-uc2-backup-tier-design.md`): the off-node
//! copy of the cluster's agreed history — format, target, chain, retention,
//! the `uc2-backup` writer and the offline restore builder.
pub mod chain;
pub mod layout;
pub mod object;
pub mod target;
pub mod writer;

#[derive(Debug, thiserror::Error)]
pub enum TierError {
    #[error("tier io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tier object {what}: {detail}")]
    Corrupt { what: String, detail: String },
    #[error("tier target {0} belongs to app_id {1}, not {2} (backup_target_foreign)")]
    Foreign(String, String, String),
    #[error("tier overlap mismatch at {0} (tier_overlap_mismatch)")]
    OverlapMismatch(String),
    #[error("{0}")]
    Refused(String),
}
