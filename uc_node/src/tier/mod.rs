//! Backup tier (spec `2026-10-10-uc2-backup-tier-design.md`): the off-node
//! copy of the cluster's agreed history — format, target, chain, retention,
//! the `uc2-backup` writer and the offline restore builder.
pub mod chain;
pub mod layout;
pub mod materialize;
pub mod object;
pub mod retention;
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
