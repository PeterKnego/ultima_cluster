//! Startup reconciliation helpers.
//!
//! openraft replays committed entries through `RaftStateMachine::apply` on its
//! own when restarted. This module just verifies the durable state is consistent
//! before handing off to openraft.

use crate::ClusterError;
use crate::raft::log_storage::JournalLogStorage;

/// Sanity-check the durable state of [`JournalLogStorage`] before handing off
/// to openraft. Catches obvious corruption (manual file deletion, partial dir
/// copy, etc.) by verifying:
///
///   `last_seq >= last_purged.index`
///
/// If the journal's last sequence number is lower than what we claim to have
/// purged through, the data dir is internally inconsistent.
pub fn assert_consistent(storage: &JournalLogStorage) -> Result<(), ClusterError> {
    let last_seq = storage.journal.last_seq();
    let last_purged = storage
        .last_purged
        .load()
        .map_err(|e| ClusterError::Recovery(format!("read last_purged: {e}")))?;

    if let (Some(seq), Some(purged)) = (last_seq, last_purged.as_ref())
        && seq < purged.index
    {
        return Err(ClusterError::Recovery(format!(
            "journal last_seq={seq} is below last_purged.index={} — data dir corrupt",
            purged.index
        )));
    }
    Ok(())
}
