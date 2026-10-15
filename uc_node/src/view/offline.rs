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

pub fn build_offline_doc(
    st: &ClusterState,
    names: &RowNames,
    artifact_position: u64,
    now_ns: u64,
) -> ClusterViewDoc {
    let cluster = cluster_part(st, names, None, None);
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
}
