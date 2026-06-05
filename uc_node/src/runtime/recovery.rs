//! Startup reconciliation helpers.
//!
//! openraft replays committed entries through `RaftStateMachine::apply` on its
//! own when restarted. This module just verifies the durable state is consistent
//! before handing off to openraft.

use crate::ClusterError;
use crate::raft::log_storage::JournalLogStorage;

/// Reconcile the durable state of [`JournalLogStorage`] before handing off
/// to openraft. Catches obvious corruption (manual file deletion, partial dir
/// copy, etc.) by verifying:
///
///   `last_seq >= last_purged.index`
///
/// If the journal's last sequence number is lower than what we claim to have
/// purged through, the data dir is internally inconsistent.
///
/// Also repairs the Eventual-log / Consistent-committed power-loss inversion:
/// a fsynced `committed` can exceed the journal's recovered tail after a crash.
/// In that case, `committed` is clamped down to the durable log tail; the node
/// re-learns the true commit from the leader via normal Raft catch-up.
pub fn reconcile(storage: &JournalLogStorage) -> Result<(), ClusterError> {
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

    // M5 invariant: output_progress must not race ahead of last_applied.
    // If the output marker is past what's been applied, replay would skip
    // committed-but-unoutput entries.
    let last_applied_idx = storage
        .last_applied
        .load()
        .map_err(|e| ClusterError::Recovery(format!("read last_applied: {e}")))?
        .map(|l| l.index)
        .unwrap_or(0);
    let output_progress = storage
        .output_progress
        .load()
        .map_err(|e| ClusterError::Recovery(format!("read output_progress: {e}")))?
        .unwrap_or(0);
    if output_progress > last_applied_idx {
        return Err(ClusterError::Recovery(format!(
            "output_progress ({output_progress}) > last_applied ({last_applied_idx}) — data dir corrupt"
        )));
    }

    // Eventual-log / Consistent-committed inversion repair: a power loss can leave
    // a fsynced `committed` ahead of the eventual log's recovered (page-cache-lost)
    // tail. Clamp committed down to the durable log tail; the node re-learns the
    // true commit from the leader via normal Raft catch-up. Lowering committed is
    // always safe.
    let durable_last = storage.journal.last_seq().unwrap_or(0);
    if let Some(c) = storage
        .committed
        .load()
        .map_err(|e| ClusterError::Recovery(format!("read committed: {e}")))?
        && c.index > durable_last
    {
        match storage.last_log_id_at(durable_last)? {
            Some(id) => storage
                .committed
                .store(&id)
                .map_err(|e| ClusterError::Recovery(format!("clamp committed: {e}")))?
                .wait()
                .map_err(|e| ClusterError::Recovery(format!("clamp committed wait: {e}")))?,
            None => storage
                .committed
                .clear()
                .map_err(|e| ClusterError::Recovery(format!("clear committed: {e}")))?
                .wait()
                .map_err(|e| ClusterError::Recovery(format!("clear committed wait: {e}")))?,
        }
    }
    Ok(())
}
