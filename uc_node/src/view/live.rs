// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The live document: everything `build` renders from `ClusterState`, plus
//! the header, this node's holdings and services, and — on the leader only —
//! every member's holdings. `gather` reads the obs sources once; the build
//! is a pure function of what it gathered, so tests drive it directly.

use uc_protocol::v2::catalog::SetEntry;
use uc_protocol::v2::cnc::{CNC_MAX_SERVICES, NODE_FLAG_LEADER, NODE_FLAG_LEARNER};
use uc_protocol::v2::datagram::Holdings;

use super::build::{RowNames, cluster_part, row_name};
use super::model::*;
use crate::catalog::{CatalogQuery, SoftTable};
use crate::cluster_fsm::ClusterState;
use crate::obs::ObsSources;

/// Everything one live document is built from, read once by [`gather`].
pub struct LiveInputs {
    pub node_id: u32,
    /// The cnc node-flags word (`NODE_FLAG_*`).
    pub flags: u64,
    /// The cnc leader hint; `u64::MAX` = no known leader.
    pub leader_hint: u64,
    pub commit: u64,
    /// `ClusterView::consumed`, the cluster agent's walk cursor (PR5).
    pub consumed: u64,
    /// The cnc `services_declared` mask (PR7's `joined`).
    pub declared: u64,
    pub names: RowNames,
    pub state: ClusterState,
    /// `ClusterView::catalog_version`: the content hash a holder's
    /// `Holdings.catalog_position` must equal for its `sets_held` to index
    /// THIS catalog.
    pub catalog_version: u64,
    pub own: Holdings,
    pub soft: SoftTable,
    pub soft_stale_ns: u64,
    pub gate_released: bool,
    pub services: Vec<ServiceView>,
}

/// One node's holdings. `sets_held` is resolved to set positions only when
/// `h` was computed against the catalog rendered (`catalog_version`); a bit
/// index into an older catalog would name the wrong sets, so it is `null`.
pub fn holdings_view(
    h: &Holdings,
    names: &RowNames,
    declared_rows: u64,
    sets: &[SetEntry],
    catalog_version: u64,
) -> HoldingsView {
    HoldingsView {
        journal_first: h.journal_first,
        durable: h.durable,
        commit: h.commit,
        applied: (0..CNC_MAX_SERVICES as u8)
            .filter(|r| declared_rows & (1 << r) != 0)
            .map(|r| RowAppliedView {
                row: r,
                name: row_name(names, r),
                applied: h.applied[r as usize],
            })
            .collect(),
        free_bytes: h.free_bytes,
        journal_bytes: h.journal_bytes,
        snapshots_bytes: h.snapshots_bytes,
        sets_held: (h.catalog_position == catalog_version).then(|| {
            sets.iter()
                .enumerate()
                .filter(|(i, _)| *i < 64 && h.sets_held & (1 << i) != 0)
                .map(|(_, e)| e.position)
                .collect()
        }),
    }
}

pub fn build_live_doc(i: &LiveInputs, now_ns: u64) -> ClusterViewDoc {
    let is_leader = i.flags & NODE_FLAG_LEADER != 0;
    let role = if is_leader {
        Role::Leader
    } else if i.flags & NODE_FLAG_LEARNER != 0 {
        Role::Learner
    } else {
        Role::Follower
    };
    let declared_rows = i.state.declared_mask();
    let holdings = |h: &Holdings| {
        holdings_view(
            h,
            &i.names,
            declared_rows,
            &i.state.catalog,
            i.catalog_version,
        )
    };
    let mut cluster = cluster_part(&i.state, &i.names, Some(i.commit), Some(i.gate_released));
    let mut nodes = None;
    // The soft table is filled only while this node leads, and a demoted
    // leader's copy goes stale in place — so the leader FLAG gates it, never
    // the table's contents.
    if is_leader {
        let q = CatalogQuery {
            sets: &i.state.catalog,
            catalog_position: i.catalog_version,
            soft: &i.soft,
            now_ns,
            stale_ns: i.soft_stale_ns,
        };
        for s in &mut cluster.catalog.sets {
            s.held_by = Some(q.holders(s.position));
        }
        // Ruling P8: every committed member, heard from or not.
        nodes = Some(
            cluster
                .membership
                .members
                .iter()
                .map(|m| match i.soft.by_node.get(&m.id) {
                    Some(e) => {
                        let age_ns = now_ns.saturating_sub(e.last_seen_ns);
                        NodeEntry {
                            id: m.id,
                            addr: m.addr.clone(),
                            role: m.role,
                            holdings: Some(holdings(&e.holdings)),
                            age_ms: Some(age_ns / 1_000_000),
                            stale: age_ns > i.soft_stale_ns,
                        }
                    }
                    None => NodeEntry {
                        id: m.id,
                        addr: m.addr.clone(),
                        role: m.role,
                        holdings: None,
                        age_ms: None,
                        stale: true,
                    },
                })
                .collect::<Vec<_>>(),
        );
    }
    let mut attention = super::attention::from_cluster(&cluster);
    if let Some(n) = &nodes {
        attention.extend(super::attention::node_stale(n));
    }
    let leader = (i.leader_hint != u64::MAX).then(|| {
        let id = i.leader_hint as u32;
        LeaderRef {
            id,
            addr: cluster
                .membership
                .members
                .iter()
                .find(|m| m.id == id)
                .map(|m| m.addr.clone()),
        }
    });
    ClusterViewDoc {
        schema: super::SCHEMA.to_string(),
        source: Source::Live,
        node_id: Some(i.node_id),
        role: Some(role),
        // PR7: mirrors `uc_service::attach::node_booting`.
        joined: Some(!(i.declared == 0 && i.names.iter().any(Option::is_some))),
        leader,
        view_position: i.consumed,
        commit: Some(i.commit),
        read_at: uc_obs::time::rfc3339_ns(now_ns),
        cluster,
        self_part: Some(SelfPart {
            holdings: holdings(&i.own),
            services: i.services.clone(),
        }),
        nodes,
        nodes_from: (!is_leader).then(|| "leader".to_string()),
        attention,
    }
}

/// Read the obs sources once. Called on the obs thread only.
pub fn gather(s: &ObsSources, now_ns: u64) -> LiveInputs {
    use std::sync::atomic::Ordering;
    // Best effort across sources: `state` is one consistent snapshot, but the
    // cnc words (flags, commit, consumed, ...) are read beside it, not with it.
    let status = s.cnc.status();
    let state = s.cluster_view.to_state();
    let names: RowNames = s
        .cnc
        .service_names()
        .map(|n| n.map(|n| n.as_str().to_string()));
    let own = *s
        .snapshot_holdings
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let wire = s
        .soft_wire
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    let soft = crate::node::soft_table_with_self(&wire, &state.membership, s.node_id, own, now_ns);
    let commit = s.cnc.counters().commit.load_acquire();
    // One read of the declared mask, so `joined` and the services list agree.
    let declared = s.cnc.services_declared();
    LiveInputs {
        node_id: s.node_id,
        flags: status.flags.load_acquire(),
        leader_hint: status.leader_hint.load_acquire(),
        commit,
        consumed: s.cluster_view.consumed.load(Ordering::Acquire),
        declared,
        services: services(s, &names, declared, commit),
        names,
        // Derived from the catalog actually rendered, never the separate
        // `catalog_version` atomic: `ClusterView::publish` stores that word
        // after the catalog, so a render between the two would pair holders'
        // bit indices with the wrong set list (review focus 2).
        catalog_version: crate::cluster_fsm::catalog_version_of(&state.catalog),
        own,
        soft,
        soft_stale_ns: s.soft_stale_ns,
        gate_released: s.backup_gate_released_level.load(Ordering::Acquire),
        state,
    }
}

/// Per declared row, read exactly as `/metrics`' `service_rows` reads it.
fn services(s: &ObsSources, names: &RowNames, declared: u64, commit: u64) -> Vec<ServiceView> {
    use uc_log::cnc::{PinRead, unpack_service_status};
    (0..CNC_MAX_SERVICES)
        .filter(|id| declared & (1 << id) != 0)
        .map(|id| {
            let slot = s.cnc.service_slot(id);
            let (_, attached, _) = unpack_service_status(slot.status.load_acquire());
            let applied = slot.applied.load_acquire();
            // Through `pin()`, the seqlock-consistent read `/metrics` uses;
            // `Contended` renders as no pin, as it does there.
            let (pinned, pinned_version) = match slot.status.pin() {
                PinRead::Pinned { to, .. } => (true, Some(VersionView::of(to))),
                _ => (false, None),
            };
            ServiceView {
                row: id as u8,
                name: names[id].clone(),
                attached,
                version: attached.then(|| VersionView::of(slot.status.version())),
                applied,
                lag_bytes: commit.saturating_sub(applied),
                timers_pending: slot.identity.timers_pending(),
                pinned,
                pinned_version,
            }
        })
        .collect()
}

/// `GET /cluster`'s body: the live document, pretty-printed, newline-ended.
pub fn render_live(s: &ObsSources) -> String {
    let now = crate::obs::metrics::now_unix_ns();
    let doc = build_live_doc(&gather(s, now), now);
    let mut body = serde_json::to_string_pretty(&doc).expect("the view serializes");
    body.push('\n');
    body
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_fsm::ClusterState;
    use uc_protocol::v2::catalog::{RowEntry, RowVerdict, SetEntry, SetKind, SetState};
    use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER, NODE_FLAG_LEARNER};

    const NOW: u64 = 1_700_000_100_000_000_000;

    fn names() -> RowNames {
        let mut n: RowNames = Default::default();
        n[0] = Some("kv".into());
        n
    }

    /// A 3-voter cluster with one agreed set at 1024, held by nodes 1 and 2.
    fn inputs(flags: u64) -> LiveInputs {
        let mut st = ClusterState::genesis_empty();
        st.membership.version = 3;
        st.membership.voters = vec![
            (1, (0x7f00_0001, 7001)),
            (2, (0x7f00_0001, 7002)),
            (3, (0x7f00_0001, 7003)),
        ];
        st.running[0] = Some(uc_protocol::v2::upgrade::RowRunning {
            row: 0,
            version: 0,
            record_pos: 64,
        });
        let mut a = SetEntry::commanded(1024, SetKind::Full, NOW - 5_000_000_000);
        a.state = SetState::Complete;
        a.rows[0] = RowEntry {
            version: 0,
            hash: 0xaa,
            verdict: RowVerdict::Agreed,
            size: 10,
        };
        a.cluster = RowEntry {
            version: 0,
            hash: 0xbb,
            verdict: RowVerdict::Agreed,
            size: 5,
        };
        st.catalog = vec![a];
        let held = Holdings {
            commit: 4096,
            catalog_position: 0xc0ffee,
            sets_held: 1,
            ..Default::default()
        };
        let mut soft = crate::catalog::SoftTable::default();
        soft.record(1, held, NOW);
        soft.record(2, held, NOW - 100_000_000);
        soft.record(
            3,
            Holdings {
                catalog_position: 0xc0ffee,
                ..Default::default()
            },
            NOW - 5_000_000_000,
        );
        LiveInputs {
            node_id: 1,
            flags,
            leader_hint: 1,
            commit: 4096,
            consumed: 4000,
            declared: 1,
            names: names(),
            state: st,
            catalog_version: 0xc0ffee,
            own: held,
            soft,
            soft_stale_ns: 900_000_000,
            gate_released: false,
            services: vec![],
        }
    }

    #[test]
    fn the_leader_fills_nodes_held_by_and_marks_a_silent_member_stale() {
        let doc = build_live_doc(&inputs(NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE), NOW);
        assert_eq!(doc.role, Some(Role::Leader));
        assert_eq!(doc.joined, Some(true));
        assert_eq!(
            doc.view_position, 4000,
            "view_position is the walk cursor, not the view tag"
        );
        assert_eq!(doc.commit, Some(4096));
        let nodes = doc.nodes.as_ref().expect("leader prints nodes");
        assert_eq!(
            nodes.iter().map(|n| n.id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(nodes[0].age_ms, Some(0));
        assert_eq!(nodes[1].age_ms, Some(100));
        assert!(!nodes[1].stale);
        assert!(nodes[2].stale);
        assert!(
            nodes[2].holdings.is_some(),
            "a stale entry still shows what it said"
        );
        assert_eq!(doc.nodes_from, None);
        assert_eq!(doc.cluster.catalog.sets[0].held_by, Some(vec![1, 2]));
        assert!(
            doc.attention
                .iter()
                .any(|a| a.code == "node_stale" && a.node == Some(3))
        );
        assert_eq!(
            doc.self_part.as_ref().unwrap().holdings.sets_held,
            Some(vec![1024])
        );
    }

    /// Ruling P8: `nodes` lists every committed member — one the leader has
    /// never heard from is listed with null holdings and age, and is stale.
    #[test]
    fn a_member_the_leader_never_heard_from_is_listed_stale_with_null_holdings() {
        let mut i = inputs(NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE);
        i.state.membership.voters.push((4, (0x7f00_0001, 7004)));
        i.state.membership.learners.push((5, (0x7f00_0001, 7005)));
        let doc = build_live_doc(&i, NOW);
        let nodes = doc.nodes.as_ref().unwrap();
        assert_eq!(
            nodes.iter().map(|n| n.id).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        for silent in &nodes[3..] {
            assert_eq!(silent.holdings, None, "node {}", silent.id);
            assert_eq!(silent.age_ms, None, "node {}", silent.id);
            assert!(silent.stale, "node {}", silent.id);
        }
        assert_eq!(nodes[4].role, MemberRole::Learner);
        for id in [4, 5] {
            assert!(
                doc.attention
                    .iter()
                    .any(|a| a.code == "node_stale" && a.node == Some(id)),
                "node {id} is not flagged"
            );
        }
        let v = serde_json::to_value(&doc).unwrap();
        assert!(v["nodes"][3]["holdings"].is_null());
        assert!(v["nodes"][3]["age_ms"].is_null());
    }

    #[test]
    fn a_follower_prints_no_nodes_and_says_ask_the_leader() {
        let mut i = inputs(0);
        i.node_id = 2;
        let doc = build_live_doc(&i, NOW);
        assert_eq!(doc.role, Some(Role::Follower));
        assert_eq!(doc.nodes, None);
        assert_eq!(doc.nodes_from.as_deref(), Some("leader"));
        assert_eq!(doc.cluster.catalog.sets[0].held_by, None);
        assert_eq!(
            doc.leader.as_ref().map(|l| l.addr.clone()),
            Some(Some("127.0.0.1:7001".to_string()))
        );
        assert!(doc.attention.iter().all(|a| a.code != "node_stale"));
    }

    #[test]
    fn no_known_leader_renders_null_and_an_unknown_leader_has_a_null_addr() {
        let mut i = inputs(0);
        i.leader_hint = u64::MAX;
        assert_eq!(build_live_doc(&i, NOW).leader, None);
        i.leader_hint = 9;
        let l = build_live_doc(&i, NOW).leader.unwrap();
        assert_eq!((l.id, l.addr), (9, None));
    }

    #[test]
    fn a_demoted_leader_never_prints_its_old_soft_table() {
        // The soft table still holds every member (it was filled while this
        // node led); the leader flag is what gates `nodes`, not the table.
        let doc = build_live_doc(&inputs(0), NOW);
        assert_eq!(doc.nodes, None);
        assert_eq!(doc.cluster.catalog.sets[0].held_by, None);
    }

    #[test]
    fn a_learner_is_a_learner_and_an_unjoined_node_says_so() {
        let mut i = inputs(NODE_FLAG_LEARNER);
        i.declared = 0;
        let doc = build_live_doc(&i, NOW);
        assert_eq!(doc.role, Some(Role::Learner));
        assert_eq!(doc.joined, Some(false));
    }

    /// Review focus 2.
    #[test]
    fn holdings_against_an_older_catalog_render_no_sets_held() {
        let mut i = inputs(NODE_FLAG_LEADER);
        let stale = Holdings {
            catalog_position: 0xdead,
            sets_held: 1,
            ..Default::default()
        };
        i.own = stale;
        i.soft.record(1, stale, NOW);
        i.soft.record(2, stale, NOW);
        let doc = build_live_doc(&i, NOW);
        assert_eq!(doc.self_part.as_ref().unwrap().holdings.sets_held, None);
        let nodes = doc.nodes.as_ref().unwrap();
        assert_eq!(nodes[0].holdings.as_ref().unwrap().sets_held, None);
        assert_eq!(nodes[1].holdings.as_ref().unwrap().sets_held, None);
        assert_eq!(doc.cluster.catalog.sets[0].held_by, Some(Vec::<u32>::new()));
    }

    #[test]
    fn the_golden_example_matches() {
        let mut i = inputs(NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE);
        i.services = vec![ServiceView {
            row: 0,
            name: Some("kv".into()),
            attached: true,
            version: Some(VersionView::of(0)),
            applied: 4096,
            lag_bytes: 0,
            timers_pending: 0,
            pinned: false,
            pinned_version: None,
        }];
        let rendered = serde_json::to_string_pretty(&build_live_doc(&i, NOW)).unwrap() + "\n";
        let golden = include_str!("example.json");
        assert_eq!(
            rendered, golden,
            "the cluster view's shape changed; if intended, regenerate \
             uc_node/src/view/example.json and follow the /1 stability rules"
        );
    }

    /// The key-material check needs a node that actually HOLDS keys, so it
    /// lives beside the crypto fixtures (`tests/crypto_cluster.rs`,
    /// `a_rendered_cluster_view_carries_no_key_material`). This pins the
    /// obs-source path itself: one live document, pretty, newline-ended.
    #[test]
    fn render_live_renders_a_live_document_off_the_obs_sources() {
        let s = crate::obs::ObsSources::for_tests();
        let body = render_live(&s);
        assert!(body.ends_with("}\n"), "{body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["schema"], crate::view::SCHEMA);
        assert_eq!(v["source"], "live");
        assert_eq!(v["node_id"], 7);
        assert_eq!(v["role"], "follower");
        assert_eq!(v["nodes_from"], "leader");
        assert!(v["self"]["holdings"].is_object());
    }

    /// Review fix: the version a holder must match is computed from the
    /// catalog `gather` rendered, not loaded from the separately-published
    /// atomic (which can lag or lead it).
    #[test]
    fn gather_derives_the_catalog_version_from_the_rendered_catalog() {
        let s = crate::obs::ObsSources::for_tests();
        s.cluster_view
            .catalog_version
            .store(0xbad, std::sync::atomic::Ordering::Release);
        let i = gather(&s, NOW);
        assert_eq!(
            i.catalog_version,
            crate::cluster_fsm::catalog_version_of(&i.state.catalog)
        );
        assert_ne!(i.catalog_version, 0xbad);
    }

    /// Ruling P9: every bound the document can grow along, filled at once.
    #[test]
    fn the_worst_case_document_stays_small() {
        use uc_protocol::v2::backup::{BackupTierEntry, MAX_BACKUP_TIERS};
        use uc_protocol::v2::catalog::MAX_CATALOG_SETS;
        use uc_protocol::v2::config::MAX_MEMBERS;
        use uc_protocol::v2::schedule::{MAX_SCHEDULE_ENTRIES, ScheduleEntry, ScheduleRule};
        use uc_protocol::v2::upgrade::{SnapshotReport, UpgradePin};
        let big = u64::MAX - 1;
        let mut i = inputs(NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE);
        i.declared = 0xff;
        i.commit = big;
        i.consumed = big;
        i.gate_released = true;
        for r in 0..CNC_MAX_SERVICES as u8 {
            i.names[r as usize] = Some(format!("row-name-of-thirty-two-bytes-{r:02}"));
            i.state.running[r as usize] = Some(uc_protocol::v2::upgrade::RowRunning {
                row: r,
                version: u32::MAX,
                record_pos: big,
            });
        }
        i.state.membership.voters = (1..=MAX_MEMBERS as u32)
            .map(|id| (id, (u32::MAX, u16::MAX - id as u16)))
            .collect();
        let entry = |verdict| RowEntry {
            version: u32::MAX,
            hash: u64::MAX,
            verdict,
            size: big,
        };
        i.state.catalog = (0..MAX_CATALOG_SETS as u64)
            .map(|k| {
                let mut e = SetEntry::commanded(big - 1024 * (64 - k), SetKind::Standby, big);
                e.rows = std::array::from_fn(|_| entry(RowVerdict::Diverged));
                e.cluster = entry(RowVerdict::Diverged);
                e
            })
            .collect();
        i.state.backups = (0..MAX_BACKUP_TIERS as u64)
            .map(|t| BackupTierEntry {
                tier_id: u64::MAX - t,
                oldest_set: 0,
                newest_set: big,
                through: 0,
                reporter: u64::MAX,
                updated_pos: big,
            })
            .collect();
        i.state.settings.purge_requires_tiers = MAX_BACKUP_TIERS as u8;
        i.state.table.entries = (0..MAX_SCHEDULE_ENTRIES as u64)
            .map(|t| ScheduleEntry {
                identity_hash: uc_protocol::identity::FsmName::parse(
                    i.names[(t % 8) as usize].as_deref().unwrap(),
                )
                .unwrap()
                .hash(),
                timer_id: u64::MAX - t,
                rule: ScheduleRule::Every {
                    period_ns: big,
                    anchor_ns: big,
                },
            })
            .collect();
        i.state.pins = (0..CNC_MAX_SERVICES as u8)
            .flat_map(|r| {
                (0..crate::cluster_fsm::MAX_PINS_PER_ROW as u64).map(move |k| UpgradePin {
                    row: r,
                    from: u32::MAX - 1,
                    to: u32::MAX,
                    origin: big - k,
                })
            })
            .collect();
        i.state.reports = (0..CNC_MAX_SERVICES as u8)
            .map(|r| SnapshotReport {
                row: r,
                position: big,
                hashes: (1..=MAX_MEMBERS as u32)
                    .map(|n| (n, u64::MAX - n as u64 % 3, big))
                    .collect(),
            })
            .collect();
        let full = Holdings {
            journal_first: big,
            durable: big,
            commit: big,
            applied: [big; CNC_MAX_SERVICES],
            free_bytes: big,
            journal_bytes: big,
            snapshots_bytes: big,
            catalog_position: i.catalog_version,
            sets_held: u64::MAX,
        };
        i.own = full;
        i.soft = crate::catalog::SoftTable::default();
        for id in 1..=MAX_MEMBERS as u32 {
            i.soft.record(id, full, NOW);
        }
        i.services = (0..CNC_MAX_SERVICES as u8)
            .map(|r| ServiceView {
                row: r,
                name: i.names[r as usize].clone(),
                attached: true,
                version: Some(VersionView::of(u32::MAX)),
                applied: big,
                lag_bytes: big,
                timers_pending: big,
                pinned: true,
                pinned_version: Some(VersionView::of(u32::MAX)),
            })
            .collect();
        let doc = build_live_doc(&i, NOW);
        // Every bound actually reached the document.
        assert_eq!(doc.cluster.catalog.sets.len(), MAX_CATALOG_SETS);
        assert!(
            doc.cluster
                .catalog
                .sets
                .iter()
                .all(|s| s.held_by.as_ref().map(Vec::len) == Some(MAX_MEMBERS)
                    && s.rows.len() == CNC_MAX_SERVICES)
        );
        assert_eq!(doc.nodes.as_ref().unwrap().len(), MAX_MEMBERS);
        assert_eq!(
            doc.self_part
                .as_ref()
                .unwrap()
                .holdings
                .sets_held
                .as_ref()
                .map(Vec::len),
            Some(MAX_CATALOG_SETS)
        );
        assert_eq!(doc.cluster.tiers.len(), MAX_BACKUP_TIERS);
        assert_eq!(doc.cluster.schedule.entries.len(), MAX_SCHEDULE_ENTRIES);
        assert!(doc.cluster.upgrades.iter().all(|u| u.pins.len()
            == crate::cluster_fsm::MAX_PINS_PER_ROW
            && u.report.is_some()));
        assert!(doc.attention.len() > MAX_CATALOG_SETS * CNC_MAX_SERVICES);
        let body = serde_json::to_string_pretty(&doc).unwrap();
        eprintln!("cluster view worst case: {} bytes", body.len());
        assert!(
            body.len() < 512 * 1024,
            "worst case is {} bytes",
            body.len()
        );
    }
}
