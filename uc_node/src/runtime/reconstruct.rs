//! Service-state reconstruction (Phase 1, mid-life reattach).
//!
//! When the node detects a service reattach (service_epoch change), it replays
//! committed entries `(service_last_applied, node_frontier]` from the journal to
//! the reattached (possibly fresh in-memory) service. `node_frontier` is the
//! node's LIVE in-memory applied index — the range openraft will not re-drive.
//!
//! The pure replay decision lives in [`plan_replay`], which the live apply-path
//! (`state_machine_shmem::drive_catchup`) calls directly. The `NeedsSnapshot`
//! branch is surfaced as a loud error in Phase 1 and wired to snapshot-install
//! in Phase 2.

/// What to replay to bring a reattached service up to `up_to` (the node's live
/// frontier / the parked entry). The replay ALWAYS includes `up_to` — it must be
/// (re-)applied and acked even if the service claims to be at/above it (idempotent;
/// log_index is the idempotency key).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ReplayPlan {
    /// Replay committed entries with index in `(from, to]` from the journal.
    Replay { from: u64, to: u64 },
    /// The gap is below the purge boundary; needs snapshot-install (Phase 2).
    NeedsSnapshot { service_last: u64, last_purged: u64 },
}

/// Pure decision. `service_last` = the reattached service's reported last_applied,
/// `up_to` = the node's live frontier (the parked entry, always >= 1),
/// `last_purged` = highest purged log index (0 if none).
pub(crate) fn plan_replay(service_last: u64, up_to: u64, last_purged: u64) -> ReplayPlan {
    debug_assert!(up_to >= 1, "up_to is a Normal entry index, always >= 1");
    if service_last < last_purged {
        ReplayPlan::NeedsSnapshot {
            service_last,
            last_purged,
        }
    } else {
        // Always include up_to: if the service is already at/above it, still
        // replay just up_to (idempotent re-apply of the parked entry).
        let from = if service_last >= up_to {
            up_to - 1
        } else {
            service_last
        };
        ReplayPlan::Replay { from, to: up_to }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replays_full_range_for_fresh_service() {
        assert_eq!(plan_replay(0, 5, 0), ReplayPlan::Replay { from: 0, to: 5 });
        assert_eq!(plan_replay(3, 5, 0), ReplayPlan::Replay { from: 3, to: 5 });
    }
    #[test]
    fn always_includes_up_to_when_service_at_or_above() {
        assert_eq!(plan_replay(5, 5, 0), ReplayPlan::Replay { from: 4, to: 5 });
        assert_eq!(plan_replay(7, 5, 0), ReplayPlan::Replay { from: 4, to: 5 });
    }
    #[test]
    fn replayable_at_purge_boundary() {
        assert_eq!(plan_replay(3, 5, 3), ReplayPlan::Replay { from: 3, to: 5 });
    }
    #[test]
    fn needs_snapshot_below_purge() {
        assert_eq!(
            plan_replay(2, 5, 5),
            ReplayPlan::NeedsSnapshot {
                service_last: 2,
                last_purged: 5
            }
        );
    }
}
