// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The document built from the newest cluster artifact, for `uc2ctl` on a
//! stopped node. Same `build` as the live path; no `self`, `nodes` or
//! services, and no live-only attention codes.

use std::io;
use std::path::Path;

use super::build::{RowNames, cluster_part};
use super::model::*;
use crate::cluster_fsm::ClusterState;
use uc_protocol::v2::catalog::SetState;

pub fn build_offline_doc(
    st: &ClusterState,
    names: &RowNames,
    artifact_position: u64,
    now_ns: u64,
) -> ClusterViewDoc {
    let mut cluster = cluster_part(st, names, None, None);
    // Ruling P32 (amending P28): the artifact at P is cut AT the instant,
    // before P's completion commits, so it can never record its own set as
    // complete. Emptiness is therefore known offline only when an EARLIER
    // set completed (`false`); otherwise it is unknown (`null`) — never
    // `true`. That also keeps `catalog_empty`, which fires on `true` only,
    // a live-only code (spec §6.3).
    cluster.catalog.empty = st
        .catalog
        .iter()
        .any(|e| e.position < artifact_position && e.state == SetState::Complete)
        .then_some(false);
    let attention = super::attention::from_cluster(&cluster);
    ClusterViewDoc {
        schema: super::SCHEMA.to_string(),
        source: Source::Artifact,
        node_id: None,
        role: None,
        joined: None,
        leader: None,
        view_position: artifact_position,
        commit: None,
        read_at: uc_obs::time::rfc3339_ns(now_ns),
        cluster,
        self_part: None,
        nodes: None,
        nodes_from: None,
        attention,
    }
}

pub fn render_offline(instance_dir: &Path, names: &RowNames) -> io::Result<Option<ClusterViewDoc>> {
    let Some(st) = crate::cluster_agent::read_committed_state(instance_dir)? else {
        return Ok(None);
    };
    let pos = st.applied;
    Ok(Some(build_offline_doc(
        &st,
        names,
        pos,
        crate::obs::metrics::now_unix_ns(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_offline_document_has_no_live_parts() {
        let d = build_offline_doc(&ClusterState::genesis_empty(), &Default::default(), 4096, 0);
        let v = serde_json::to_value(&d).unwrap();
        assert_eq!(v["source"], "artifact");
        assert_eq!(v["view_position"], 4096);
        for k in [
            "self",
            "nodes",
            "role",
            "joined",
            "leader",
            "commit",
            "nodes_from",
        ] {
            assert!(v[k].is_null(), "{k} must be null offline");
        }
        assert!(v["cluster"]["tiers"].as_array().unwrap().is_empty());
        assert!(v["cluster"]["purge_gate"]["released"].is_null());
    }

    fn commanded_at_p() -> ClusterState {
        use uc_protocol::v2::catalog::{SetEntry, SetKind};
        let mut st = ClusterState::genesis_empty();
        st.catalog = vec![SetEntry::commanded(1024, SetKind::Full, 0)];
        st
    }

    /// P32: a catalog whose only entry is the set commanded at the
    /// artifact's own position reads `empty: null` (unknown offline) and
    /// raises no `catalog_empty`.
    #[test]
    fn an_offline_document_with_only_its_own_set_reads_empty_as_unknown() {
        let d = build_offline_doc(&commanded_at_p(), &Default::default(), 1024, 0);
        assert_eq!(d.cluster.catalog.empty, None);
        let v = serde_json::to_value(&d).unwrap();
        assert!(
            v["cluster"]["catalog"]
                .as_object()
                .unwrap()
                .get("empty")
                .is_some_and(|e| e.is_null()),
            "`empty` serializes as null, not skipped: {v}"
        );
        assert!(
            d.attention.iter().all(|a| a.code != "catalog_empty"),
            "{:?}",
            d.attention
        );
    }

    /// P32: an earlier complete set proves the catalog is not empty.
    #[test]
    fn an_earlier_complete_set_reads_empty_false_offline() {
        use uc_protocol::v2::catalog::{SetEntry, SetKind};
        let mut st = commanded_at_p();
        let mut a = SetEntry::commanded(512, SetKind::Full, 0);
        a.state = SetState::Complete;
        st.catalog.insert(0, a);
        let d = build_offline_doc(&st, &Default::default(), 1024, 0);
        assert_eq!(d.cluster.catalog.empty, Some(false));
    }

    /// P32: never `true` offline — not from genesis, not from an earlier
    /// set that is still commanded, not from a (structurally impossible)
    /// complete set at P itself.
    #[test]
    fn an_offline_document_never_reads_empty_true() {
        use uc_protocol::v2::catalog::{SetEntry, SetKind};
        let g = build_offline_doc(&ClusterState::genesis_empty(), &Default::default(), 4096, 0);
        assert_eq!(g.cluster.catalog.empty, None);
        let mut st = commanded_at_p();
        st.catalog
            .insert(0, SetEntry::commanded(512, SetKind::Full, 0));
        let d = build_offline_doc(&st, &Default::default(), 1024, 0);
        assert_eq!(
            d.cluster.catalog.empty, None,
            "a commanded earlier set proves nothing"
        );
        let mut st = commanded_at_p();
        st.catalog[0].state = SetState::Complete;
        let d = build_offline_doc(&st, &Default::default(), 1024, 0);
        assert_eq!(d.cluster.catalog.empty, None, "only sets BEFORE P count");
        for d in [g, d] {
            assert!(d.attention.iter().all(|a| a.code != "catalog_empty"));
        }
    }
}
