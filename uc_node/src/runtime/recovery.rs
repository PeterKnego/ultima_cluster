//! Startup reconciliation helpers.
//!
//! openraft replays committed entries through `RaftStateMachine::apply` on its
//! own when restarted. This module just verifies the durable state is consistent
//! before handing off to openraft.

use crate::ClusterError;
use crate::raft::RaftLogId;
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
/// a fsynced `committed` can exceed the durable state after a crash. In that
/// case, `committed` is clamped down to the durable floor — the greater of the
/// log tail (`last_seq`) and the purge/snapshot point (`last_purged`); the node
/// re-learns the true commit from the leader via normal Raft catch-up.
///
/// Finally, clamps `output_progress` down to the durable `last_applied` when it
/// has raced ahead (the durable `last_applied` only advances at snapshot time,
/// so it legitimately lags the per-output `output_progress` on a restart before
/// the first snapshot). Outputs in the gap re-run on replay (at-least-once).
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

    // M5: reconcile output_progress against the durable last_applied.
    //
    // The durable `last_applied` StableValue lags `output_progress`: in shmem
    // mode it is only persisted at snapshot time (`SnapshotPolicy::LogsSinceLast`,
    // default 5000), whereas `output_progress` is fsynced per committed output.
    // So after a restart before the first snapshot, the durable `last_applied`
    // can sit well below `output_progress` even though nothing is corrupt — the
    // applied entries live in the log and openraft re-applies them on startup.
    //
    // Clamp `output_progress` down to the durable `last_applied`. After openraft
    // replays the log, the output dispatcher re-drives `on_committed` for the
    // (clamped, last_applied] gap — at-least-once, idempotent by the user's
    // contract — so no committed-but-unoutput entry is ever skipped. Raising the
    // marker (output_progress < last_applied) is the normal steady state and is
    // left untouched.
    //
    // Cost note: durable `last_applied` advances only on snapshot install, so on
    // a cold restart before the first snapshot the clamp can re-drive up to
    // `snapshot_policy_logs_since_last` (default 5000) outputs. Correct (at-least-
    // once) but a surprise for heavy side-effects; persisting `last_applied` more
    // frequently would bound it — tracked as a follow-up.
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
        tracing::warn!(
            output_progress,
            last_applied = last_applied_idx,
            "output_progress ahead of durable last_applied (snapshot-lag); clamping down — \
             outputs in the gap re-run on replay (at-least-once)"
        );
        storage
            .output_progress
            .store(&last_applied_idx)
            .map_err(|e| ClusterError::Recovery(format!("clamp output_progress: {e}")))?
            .wait()
            .map_err(|e| ClusterError::Recovery(format!("clamp output_progress wait: {e}")))?;
    }

    // Eventual-log / Consistent-committed inversion repair: a power loss can leave
    // a fsynced `committed` ahead of the durable state. The durable floor is the
    // greater of the log tail (`last_seq`) and the purge/snapshot point
    // (`last_purged`) — entries at/below `last_purged` are durable via the snapshot.
    // Clamp committed down to that floor; lowering committed is always safe (the
    // node re-learns the true commit from the leader via normal Raft catch-up).
    let last_seq_idx = last_seq.unwrap_or(0);
    let last_purged_idx = last_purged.as_ref().map(|p| p.index).unwrap_or(0);
    let durable_last = last_seq_idx.max(last_purged_idx);
    if let Some(c) = storage
        .committed
        .load()
        .map_err(|e| ClusterError::Recovery(format!("read committed: {e}")))?
        && c.index > durable_last
    {
        // The log_id at the durable floor: prefer the log tail's own record; when
        // the log is empty/shorter than the snapshot point, use `last_purged`.
        let target: Option<RaftLogId> = if last_seq_idx >= last_purged_idx && last_seq_idx > 0 {
            storage.last_log_id_at(last_seq_idx)?
        } else {
            last_purged
        };
        match target {
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
