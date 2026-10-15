// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The document's `attention` list (spec §5): only conditions the node
//! already detects, each named by a stable code the reference page maps to
//! its runbook section.

use super::TIER_BEHIND_BYTES;
use super::model::{Attention, ClusterPart, NodeEntry, SetStateView};

pub fn from_cluster(part: &ClusterPart) -> Vec<Attention> {
    let mut out = Vec::new();
    if part.catalog.empty {
        out.push(Attention::code("catalog_empty"));
    }
    for s in &part.catalog.sets {
        // `stalled` is already "commanded and superseded" (PR2).
        if s.stalled && s.state == SetStateView::Commanded {
            let mut a = Attention::code("set_stalled");
            a.set = Some(s.position);
            out.push(a);
        }
        for row in &s.diverged_rows {
            let mut a = Attention::code("set_diverged");
            a.set = Some(s.position);
            a.row = Some(row.clone());
            out.push(a);
        }
    }
    for t in &part.tiers {
        if let Some(lag) = t.lag_bytes.filter(|l| *l > TIER_BEHIND_BYTES) {
            let mut a = Attention::code("tier_behind");
            a.tier = Some(t.id.clone());
            a.lag_bytes = Some(lag);
            out.push(a);
        }
    }
    if part.purge_gate.released == Some(true) {
        out.push(Attention::code("purge_gate_released"));
    }
    out
}

pub fn node_stale(nodes: &[NodeEntry]) -> Vec<Attention> {
    nodes
        .iter()
        .filter(|n| n.stale)
        .map(|n| {
            let mut a = Attention::code("node_stale");
            a.node = Some(n.id);
            a
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_fsm::ClusterState;
    use crate::view::build::cluster_part;

    #[test]
    fn a_genesis_state_reports_catalog_empty_only() {
        let part = cluster_part(
            &ClusterState::genesis_empty(),
            &Default::default(),
            Some(0),
            Some(false),
        );
        let codes: Vec<String> = from_cluster(&part).into_iter().map(|a| a.code).collect();
        assert_eq!(codes, vec!["catalog_empty".to_string()]);
    }

    #[test]
    fn diverged_stalled_tier_behind_and_gate_release_are_each_reported_with_context() {
        use uc_protocol::v2::backup::BackupTierEntry;
        use uc_protocol::v2::catalog::{RowVerdict, SetEntry, SetKind, SetState};
        let mut st = ClusterState::genesis_empty();
        let mut a = SetEntry::commanded(1024, SetKind::Full, 0);
        a.state = SetState::Complete;
        a.cluster.verdict = RowVerdict::Diverged;
        st.catalog = vec![
            a,
            SetEntry::commanded(2048, SetKind::Full, 0),
            SetEntry::commanded(3072, SetKind::Full, 0),
        ];
        st.backups = vec![BackupTierEntry {
            tier_id: 1,
            oldest_set: 0,
            newest_set: 0,
            through: 0,
            reporter: 1,
            updated_pos: 0,
        }];
        let commit = crate::view::TIER_BEHIND_BYTES + 1;
        let part = cluster_part(&st, &Default::default(), Some(commit), Some(true));
        let att = from_cluster(&part);
        let json = serde_json::to_string(&att).unwrap();
        assert!(
            json.contains(r#"{"code":"set_diverged","set":1024,"row":"cluster"}"#),
            "{json}"
        );
        assert!(
            json.contains(r#"{"code":"set_stalled","set":2048}"#),
            "{json}"
        );
        assert!(
            !json.contains(r#""set":3072"#),
            "the newest commanded set is not stalled: {json}"
        );
        assert!(
            json.contains(&format!(
                r#"{{"code":"tier_behind","tier":"0x0000000000000001","lag_bytes":{commit}}}"#
            )),
            "{json}"
        );
        assert!(json.contains(r#"{"code":"purge_gate_released"}"#), "{json}");
        assert!(!json.contains("catalog_empty"), "{json}");
    }

    #[test]
    fn a_tier_exactly_at_the_threshold_is_not_behind() {
        use uc_protocol::v2::backup::BackupTierEntry;
        let mut st = ClusterState::genesis_empty();
        st.backups = vec![BackupTierEntry {
            tier_id: 1,
            oldest_set: 0,
            newest_set: 0,
            through: 0,
            reporter: 1,
            updated_pos: 0,
        }];
        let part = cluster_part(
            &st,
            &Default::default(),
            Some(crate::view::TIER_BEHIND_BYTES),
            None,
        );
        assert!(from_cluster(&part).iter().all(|a| a.code != "tier_behind"));
    }

    #[test]
    fn an_offline_tier_with_null_lag_is_not_behind() {
        use uc_protocol::v2::backup::BackupTierEntry;
        let mut st = ClusterState::genesis_empty();
        st.backups = vec![BackupTierEntry {
            tier_id: 1,
            oldest_set: 0,
            newest_set: 0,
            through: 0,
            reporter: 1,
            updated_pos: 0,
        }];
        let part = cluster_part(&st, &Default::default(), None, None);
        assert!(from_cluster(&part).iter().all(|a| a.code != "tier_behind"));
    }

    #[test]
    fn a_never_heard_member_is_node_stale() {
        use crate::view::model::{MemberRole, NodeEntry};
        let n = |id, stale| NodeEntry {
            id,
            addr: "a".into(),
            role: MemberRole::Voter,
            holdings: None,
            age_ms: None,
            stale,
        };
        let att = node_stale(&[n(1, false), n(2, true)]);
        assert_eq!(
            serde_json::to_string(&att).unwrap(),
            r#"[{"code":"node_stale","node":2}]"#
        );
    }

    #[test]
    fn the_threshold_matches_the_alert_rule() {
        let rules = include_str!("../../../packaging/prometheus/uc2-alerts.yml");
        let needle = format!("uc2_backup_lag_bytes > {}", crate::view::TIER_BEHIND_BYTES);
        assert!(
            rules.contains(&needle),
            "Uc2BackupBehind no longer reads `{needle}`"
        );
    }
}
