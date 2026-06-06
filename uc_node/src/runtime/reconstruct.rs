//! Service-state reconstruction (Phase 1, mid-life reattach).
//!
//! When the node detects a service reattach (service_epoch change), it replays
//! committed entries `(service_last_applied, node_frontier]` from the journal to
//! the reattached (possibly fresh in-memory) service. `node_frontier` is the
//! node's LIVE in-memory applied index — the range openraft will not re-drive.

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CatchupSource {
    Nothing,
    LogReplay { from: u64, to: u64 },
    NeedsSnapshot { service_last_applied: u64, last_purged: u64 },
}

/// Pure decision. `last_purged` is the highest purged log index (0 if none).
pub(crate) fn decide_catchup_source(
    service_last_applied: u64,
    node_frontier: u64,
    last_purged: u64,
) -> CatchupSource {
    if service_last_applied >= node_frontier {
        CatchupSource::Nothing
    } else if service_last_applied < last_purged {
        CatchupSource::NeedsSnapshot { service_last_applied, last_purged }
    } else {
        CatchupSource::LogReplay { from: service_last_applied, to: node_frontier }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nothing_when_current_or_ahead_or_empty() {
        assert_eq!(decide_catchup_source(10, 10, 0), CatchupSource::Nothing);
        assert_eq!(decide_catchup_source(12, 10, 0), CatchupSource::Nothing);
        assert_eq!(decide_catchup_source(0, 0, 0), CatchupSource::Nothing);
    }
    #[test]
    fn log_replay_above_purge_incl_boundary() {
        assert_eq!(decide_catchup_source(3, 10, 0), CatchupSource::LogReplay { from: 3, to: 10 });
        assert_eq!(decide_catchup_source(5, 10, 5), CatchupSource::LogReplay { from: 5, to: 10 });
    }
    #[test]
    fn needs_snapshot_below_purge() {
        assert_eq!(decide_catchup_source(2, 10, 5),
            CatchupSource::NeedsSnapshot { service_last_applied: 2, last_purged: 5 });
    }
}
