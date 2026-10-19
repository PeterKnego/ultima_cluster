// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `ClusterState` → the document's `cluster` part. Pure: the same function
//! serves the live view and uc2ctl's offline artifact view, so they cannot
//! drift. Every derived field uses the query the node itself acts on.

use uc_protocol::v2::backup::BackupTierEntry;
use uc_protocol::v2::catalog::{CLUSTER_ROW, RowEntry, RowVerdict, SetEntry, SetKind, SetState};
use uc_protocol::v2::cnc::CNC_MAX_SERVICES;
use uc_protocol::v2::schedule::ScheduleRule;
use uc_protocol::v2::settings::Target;
use uc_protocol::v2::upgrade::verdict;

use super::model::*;
use crate::catalog::{CatalogQuery, SoftTable};
use crate::cluster_fsm::ClusterState;

pub type RowNames = [Option<String>; CNC_MAX_SERVICES];

pub fn row_name(names: &RowNames, row: u8) -> Option<String> {
    if row == CLUSTER_ROW {
        return Some("cluster".to_string());
    }
    names.get(row as usize).cloned().flatten()
}

/// The text a diverged row is listed by: its name, else `row<N>`.
fn row_label(names: &RowNames, row: u8) -> String {
    row_name(names, row).unwrap_or_else(|| format!("row{row}"))
}

pub fn name_for_hash(names: &RowNames, hash: u64) -> Option<String> {
    names
        .iter()
        .flatten()
        .find(|n| {
            uc_protocol::identity::FsmName::parse(n)
                .ok()
                .map(|f| f.hash())
                == Some(hash)
        })
        .cloned()
}

/// PR2: commanded and superseded by a newer catalog entry.
pub fn stalled(sets: &[SetEntry], i: usize) -> bool {
    sets[i].state == SetState::Commanded && i + 1 < sets.len()
}

fn some_nonzero(v: u64) -> Option<u64> {
    (v != 0).then_some(v)
}

fn row_entry(names: &RowNames, row: u8, e: &RowEntry) -> RowEntryView {
    let reported = e.verdict != RowVerdict::Unreported;
    RowEntryView {
        row,
        name: row_name(names, row),
        version: reported.then(|| VersionView::of(e.version)),
        hash: reported.then_some(Hex64(e.hash)),
        verdict: match e.verdict {
            RowVerdict::Unreported => VerdictView::Unreported,
            RowVerdict::Agreed => VerdictView::Agreed,
            RowVerdict::Diverged => VerdictView::Diverged,
            RowVerdict::NoMajority => VerdictView::NoMajority,
        },
        size: if reported { some_nonzero(e.size) } else { None },
    }
}

pub fn cluster_part(
    st: &ClusterState,
    names: &RowNames,
    commit: Option<u64>,
    gate_released: Option<bool>,
) -> ClusterPart {
    let declared = st.declared_mask();
    let empty_soft = SoftTable::default();
    let q = CatalogQuery {
        sets: &st.catalog,
        catalog_position: 0,
        soft: &empty_soft,
        now_ns: 0,
        stale_ns: 0,
    };
    let diverged = q.diverged();
    let sets = st
        .catalog
        .iter()
        .enumerate()
        .map(|(i, e)| SetView {
            position: e.position,
            kind: match e.kind {
                SetKind::Full => SetKindView::Full,
                SetKind::Standby => SetKindView::Standby,
            },
            commanded_at: uc_obs::time::rfc3339_ns(e.time_ns),
            state: match e.state {
                SetState::Commanded => SetStateView::Commanded,
                SetState::Complete => SetStateView::Complete,
            },
            agreed: e.is_agreed(),
            stalled: stalled(&st.catalog, i),
            diverged_rows: diverged
                .iter()
                .filter(|(p, _)| *p == e.position)
                .map(|(_, r)| row_label(names, *r))
                .collect(),
            size: some_nonzero(e.total_size()),
            rows: (0..CNC_MAX_SERVICES as u8)
                .filter(|r| declared & (1 << r) != 0)
                .map(|r| row_entry(names, r, &e.rows[r as usize]))
                .collect(),
            cluster: row_entry(names, CLUSTER_ROW, &e.cluster),
            held_by: None,
        })
        .collect();

    let k = st.settings.purge_requires_tiers;
    ClusterPart {
        membership: membership(st),
        settings: settings(st),
        schedule: schedule(st, names),
        upgrades: upgrades(st, names),
        catalog: CatalogView {
            retain_sets: st.retain_sets(),
            empty: Some(st.catalog_empty()),
            floor: q.newest_agreed(u64::MAX),
            sets,
        },
        tiers: st.backups.iter().map(|t| tier(t, commit)).collect(),
        purge_gate: PurgeGateView {
            requires_tiers: k,
            position: crate::tier::gate_position(k, &st.backups),
            tiers_reporting: st.backups.len(),
            released: gate_released,
        },
    }
}

fn tier(t: &BackupTierEntry, commit: Option<u64>) -> TierView {
    TierView {
        id: format!("0x{:016x}", t.tier_id),
        oldest_set: t.oldest_set,
        newest_set: t.newest_set,
        through: t.through,
        reporter: t.reporter,
        updated_position: t.updated_pos,
        lag_bytes: commit.map(|c| c.saturating_sub(t.through)),
    }
}

fn membership(st: &ClusterState) -> MembershipView {
    let m = &st.membership;
    let mut members: Vec<MemberView> = m
        .voters
        .iter()
        .map(|(id, a)| MemberView {
            id: *id,
            addr: crate::node::addr_of(*a).to_string(),
            role: MemberRole::Voter,
        })
        .chain(m.learners.iter().map(|(id, a)| MemberView {
            id: *id,
            addr: crate::node::addr_of(*a).to_string(),
            role: MemberRole::Learner,
        }))
        .collect();
    members.sort_by_key(|x| x.id);
    MembershipView {
        config_version: m.version,
        members,
    }
}

fn settings(st: &ClusterState) -> SettingsView {
    let s = &st.settings;
    SettingsView {
        position: st.settings_position,
        fsm_lag: match s.fsm_lag_bytes {
            0 => FsmLagView::Derived,
            u64::MAX => FsmLagView::Lockstep,
            b => FsmLagView::Bytes { bytes: DecU64(b) },
        },
        admission_bytes: DecU64(s.admission_bytes),
        snapshot_interval_bytes: DecU64(s.snapshot_interval_bytes),
        snapshot_target: match s.snapshot_target {
            Target::All => TargetView::All,
            Target::Learners => TargetView::Learners,
        },
        datagram_mtu: s.datagram_mtu,
        datagram_mtu_rung: if s.datagram_mtu == 0 {
            "baseline"
        } else {
            "discovered"
        }
        .to_string(),
        retain_sets: s.retain_sets,
        auto_fetch: s.auto_fetch,
        backup_retain_sets: s.backup_retain_sets,
        backup_retain_age_secs: DecU64(s.backup_retain_age_secs),
        purge_requires_tiers: s.purge_requires_tiers,
        wire_level: s.wire_level,
        wire_raise: s.wire_raise,
    }
}

fn schedule(st: &ClusterState, names: &RowNames) -> ScheduleView {
    ScheduleView {
        position: st.table_position,
        entries: st
            .table
            .entries
            .iter()
            .map(|e| ScheduleEntryView {
                row_name: name_for_hash(names, e.identity_hash),
                identity_hash: Hex64(e.identity_hash),
                timer_id: DecU64(e.timer_id),
                rule: match e.rule {
                    ScheduleRule::Every {
                        period_ns,
                        anchor_ns,
                    } => RuleView::Every {
                        period_ns: DecU64(period_ns),
                        anchor: uc_obs::time::rfc3339_ns(anchor_ns),
                        anchor_ns: DecU64(anchor_ns),
                    },
                    ScheduleRule::DailyAt { secs_of_day } => RuleView::DailyAt { secs_of_day },
                    ScheduleRule::Once { at_ns } => RuleView::Once {
                        at: uc_obs::time::rfc3339_ns(at_ns),
                        at_ns: DecU64(at_ns),
                    },
                },
            })
            .collect(),
    }
}

fn upgrades(st: &ClusterState, names: &RowNames) -> Vec<UpgradeRowView> {
    (0..CNC_MAX_SERVICES as u8)
        .filter_map(|row| {
            let pins: Vec<PinView> = st
                .pins
                .iter()
                .filter(|p| p.row == row)
                .map(|p| PinView {
                    from: VersionView::of(p.from),
                    to: VersionView::of(p.to),
                    origin: p.origin,
                })
                .collect();
            let running = st.running[row as usize].map(|r| RunningView {
                version: VersionView::of(r.version),
                record_pos: r.record_pos,
            });
            let report = st.reports.iter().find(|r| r.row == row).map(|r| {
                let v = verdict(r);
                ReportView {
                    position: r.position,
                    verdict: match (v.agreed, v.majority_hash) {
                        (true, _) => HashVerdictView::Agreed,
                        (false, Some(_)) => HashVerdictView::Diverged,
                        (false, None) => HashVerdictView::NoMajority,
                    },
                    majority_hash: v.majority_hash.map(Hex64),
                    minority: v.minority,
                    nodes: r
                        .hashes
                        .iter()
                        .map(|(n, h, s)| ReportNodeView {
                            node: *n,
                            hash: Hex64(*h),
                            size: some_nonzero(*s),
                        })
                        .collect(),
                }
            });
            (!pins.is_empty() || running.is_some() || report.is_some()).then(|| UpgradeRowView {
                row,
                name: row_name(names, row),
                pins,
                running,
                report,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::v2::catalog::{RowEntry, RowVerdict, SetEntry, SetKind, SetState};

    fn names() -> RowNames {
        let mut n: RowNames = Default::default();
        n[0] = Some("kv".into());
        n
    }

    fn state_with_one_agreed_and_one_commanded() -> ClusterState {
        let mut st = ClusterState::genesis_empty();
        st.running[0] = Some(uc_protocol::v2::upgrade::RowRunning {
            row: 0,
            version: 0,
            record_pos: 64,
        });
        let mut a = SetEntry::commanded(1024, SetKind::Full, 1_700_000_000_000_000_000);
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
        let b = SetEntry::commanded(2048, SetKind::Standby, 1_700_000_001_000_000_000);
        st.catalog = vec![a, b];
        st
    }

    #[test]
    fn the_catalog_reports_floor_agreement_size_and_declared_rows_only() {
        let st = state_with_one_agreed_and_one_commanded();
        let p = cluster_part(&st, &names(), Some(4096), Some(false));
        assert_eq!(p.catalog.floor, Some(1024));
        assert_eq!(p.catalog.empty, Some(false));
        assert_eq!(p.catalog.sets.len(), 2);
        let s0 = &p.catalog.sets[0];
        assert!(s0.agreed);
        assert_eq!(s0.size, Some(15));
        assert_eq!(s0.rows.len(), 1, "only declared rows are listed");
        assert_eq!(s0.rows[0].name.as_deref(), Some("kv"));
        assert_eq!(s0.rows[0].hash, Some(Hex64(0xaa)));
        assert_eq!(s0.cluster.row, uc_protocol::v2::catalog::CLUSTER_ROW);
        assert_eq!(s0.cluster.name.as_deref(), Some("cluster"));
        assert_eq!(s0.commanded_at, "2023-11-14T22:13:20.000000000Z");
        let s1 = &p.catalog.sets[1];
        assert_eq!(s1.state, SetStateView::Commanded);
        assert_eq!(s1.rows[0].verdict, VerdictView::Unreported);
        assert_eq!(s1.rows[0].version, None, "unreported rows print no version");
        assert_eq!(s1.rows[0].hash, None);
        assert_eq!(s1.size, None);
        assert!(
            !s1.stalled,
            "the newest commanded set is in progress, not stalled"
        );
    }

    #[test]
    fn a_commanded_set_superseded_by_a_newer_one_is_stalled() {
        let mut st = state_with_one_agreed_and_one_commanded();
        st.catalog.push(SetEntry::commanded(
            3072,
            SetKind::Full,
            1_700_000_002_000_000_000,
        ));
        let p = cluster_part(&st, &names(), None, None);
        assert!(p.catalog.sets[1].stalled);
        assert!(!p.catalog.sets[2].stalled);
    }

    #[test]
    fn a_diverged_row_is_named_and_the_cluster_artifact_is_called_cluster() {
        let mut st = state_with_one_agreed_and_one_commanded();
        st.catalog[0].rows[0].verdict = RowVerdict::Diverged;
        st.catalog[0].cluster.verdict = RowVerdict::Diverged;
        let p = cluster_part(&st, &names(), None, None);
        assert_eq!(
            p.catalog.sets[0].diverged_rows,
            vec!["kv".to_string(), "cluster".to_string()]
        );
        assert!(!p.catalog.sets[0].agreed);
    }

    #[test]
    fn a_row_without_a_name_renders_null() {
        let st = state_with_one_agreed_and_one_commanded();
        let p = cluster_part(&st, &Default::default(), None, None);
        assert_eq!(p.catalog.sets[0].rows[0].name, None);
        let mut st2 = st.clone();
        st2.catalog[0].rows[0].verdict = RowVerdict::Diverged;
        let p2 = cluster_part(&st2, &Default::default(), None, None);
        assert_eq!(p2.catalog.sets[0].diverged_rows, vec!["row0".to_string()]);
    }

    #[test]
    fn tiers_lag_against_the_given_commit_and_the_gate_follows_gate_position() {
        use uc_protocol::v2::backup::BackupTierEntry;
        let mut st = ClusterState::genesis_empty();
        st.settings.purge_requires_tiers = 2;
        st.backups = vec![BackupTierEntry {
            tier_id: 0xfeed,
            oldest_set: 1024,
            newest_set: 2048,
            through: 3000,
            reporter: 1,
            updated_pos: 2500,
        }];
        let p = cluster_part(&st, &Default::default(), Some(5000), Some(true));
        assert_eq!(p.tiers[0].id, "0x000000000000feed");
        assert_eq!(p.tiers[0].lag_bytes, Some(2000));
        assert_eq!(p.purge_gate.requires_tiers, 2);
        assert_eq!(
            p.purge_gate.position,
            Some(0),
            "fewer than K tiers: holds every purge"
        );
        assert_eq!(p.purge_gate.tiers_reporting, 1);
        assert_eq!(p.purge_gate.released, Some(true));
        let off = cluster_part(&st, &Default::default(), None, None);
        assert_eq!(off.tiers[0].lag_bytes, None);
        st.settings.purge_requires_tiers = 0;
        assert_eq!(
            cluster_part(&st, &Default::default(), None, None)
                .purge_gate
                .position,
            None
        );
    }

    #[test]
    fn settings_schedule_and_upgrades_round_trip_through_the_converters() {
        use uc_protocol::v2::schedule::{ScheduleEntry, ScheduleRule};
        use uc_protocol::v2::upgrade::{SnapshotReport, UpgradePin};
        let mut st = ClusterState::genesis_empty();
        st.settings.fsm_lag_bytes = u64::MAX;
        st.settings.datagram_mtu = 8960;
        st.settings.admission_bytes = u64::MAX - 1;
        st.settings.snapshot_interval_bytes = u64::MAX - 1;
        st.settings.backup_retain_age_secs = u64::MAX - 1;
        st.settings_position = 77;
        st.table_position = 99;
        st.table.entries = vec![
            ScheduleEntry {
                identity_hash: 0xabc,
                timer_id: u64::MAX,
                rule: ScheduleRule::Every {
                    period_ns: u64::MAX,
                    anchor_ns: 1_700_000_000_123_456_789,
                },
            },
            ScheduleEntry {
                identity_hash: 0xabc,
                timer_id: 2,
                rule: ScheduleRule::DailyAt {
                    secs_of_day: 50_400,
                },
            },
            ScheduleEntry {
                identity_hash: 0xdef,
                timer_id: 3,
                rule: ScheduleRule::Once { at_ns: 5 },
            },
        ];
        st.pins = vec![
            UpgradePin {
                row: 0,
                from: 1,
                to: 2,
                origin: 100,
            },
            UpgradePin {
                row: 0,
                from: 2,
                to: 3,
                origin: 200,
            },
        ];
        st.reports = vec![SnapshotReport {
            row: 0,
            position: 300,
            hashes: vec![(1, 0xaa, 9), (2, 0xaa, 9), (3, 0xbb, 9)],
        }];
        st.running[0] = Some(uc_protocol::v2::upgrade::RowRunning {
            row: 0,
            version: 3,
            record_pos: 200,
        });
        let p = cluster_part(&st, &names(), None, None);
        assert_eq!(super::super::convert::settings(&p.settings), st.settings);
        assert_eq!(super::super::convert::table(&p.schedule), st.table);
        let (pins, reports, running) = super::super::convert::upgrade(&p.upgrades);
        assert_eq!(pins, st.pins);
        assert_eq!(reports, st.reports);
        assert_eq!(running, st.running);
        assert_eq!(p.settings.position, 77);
        assert_eq!(p.schedule.position, 99);
        assert_eq!(
            p.upgrades[0].report.as_ref().unwrap().verdict,
            HashVerdictView::Diverged
        );
    }

    /// Task 9 / Task 3 review: the JSON leg (what `uc2ctl` actually reads)
    /// loses nothing — u64 extremes and ns above 2^53 survive serialize,
    /// parse and convert. This exercises serde losslessness ONLY: its
    /// positions above 2^53 (a report position, a pin origin, a record
    /// position) are byte offsets no log reaches in practice, so they are
    /// not a claim that such a document honours the 2^53 rule — that is
    /// `no_number_in_a_rendered_document_exceeds_2_pow_53`'s job.
    #[test]
    fn the_converters_round_trip_through_json_text() {
        use uc_protocol::v2::schedule::{ScheduleEntry, ScheduleRule};
        use uc_protocol::v2::upgrade::{RowRunning, SnapshotReport, UpgradePin};
        let mut st = ClusterState::genesis_empty();
        st.settings.fsm_lag_bytes = u64::MAX - 1;
        st.settings.admission_bytes = u64::MAX - 1;
        st.settings.snapshot_interval_bytes = u64::MAX - 1;
        st.settings.backup_retain_age_secs = u64::MAX - 1;
        st.table.entries = vec![
            ScheduleEntry {
                identity_hash: u64::MAX,
                timer_id: u64::MAX - 2,
                rule: ScheduleRule::Every {
                    period_ns: (1u64 << 53) + 1,
                    anchor_ns: 1_700_000_000_123_456_789,
                },
            },
            ScheduleEntry {
                identity_hash: 0xabc,
                timer_id: (1u64 << 53) + 3,
                rule: ScheduleRule::Once {
                    at_ns: 1_800_000_000_987_654_321,
                },
            },
            ScheduleEntry {
                identity_hash: 0xabc,
                timer_id: 5,
                rule: ScheduleRule::DailyAt { secs_of_day: 7 },
            },
        ];
        st.pins = vec![UpgradePin {
            row: 0,
            from: u32::MAX - 1,
            to: u32::MAX,
            origin: u64::MAX - 1,
        }];
        st.reports = vec![SnapshotReport {
            row: 0,
            position: (1u64 << 53) + 7,
            // `(3, 5, 0)`: an unknown size renders null and converts back to 0.
            hashes: vec![(1, u64::MAX, u64::MAX - 1), (2, 1, 2), (3, 5, 0)],
        }];
        st.running[0] = Some(RowRunning {
            row: 0,
            version: u32::MAX,
            record_pos: u64::MAX - 5,
        });
        let part = cluster_part(&st, &names(), None, None);
        let text = serde_json::to_string(&part).unwrap();
        let back: super::super::model::ClusterPart = serde_json::from_str(&text).unwrap();
        assert_eq!(super::super::convert::settings(&back.settings), st.settings);
        assert_eq!(super::super::convert::table(&back.schedule), st.table);
        let (pins, reports, running) = super::super::convert::upgrade(&back.upgrades);
        assert_eq!(pins, st.pins);
        assert_eq!(reports, st.reports);
        assert_eq!(running, st.running);
    }

    /// Review focus 1: no JSON number above 2^53 anywhere, even with every
    /// SENTINEL-bearing u64 at its extreme: replicated settings, schedule
    /// ids and times, hashes, tier ids, set times, packed versions, a
    /// report's unknown (`0`) size. Positions, sizes and byte counts
    /// (catalog, pins' `origin`, reports' `position`, `running.record_pos`,
    /// tiers' `oldest_set`/`newest_set`/`through`, every holdings word) are
    /// byte offsets or measured byte counts, which cannot exceed 2^53
    /// (8 PiB) in practice, so they stay at realistic values here; `reporter`
    /// is a node id.
    #[test]
    fn no_number_in_a_rendered_document_exceeds_2_pow_53() {
        use uc_protocol::v2::backup::BackupTierEntry;
        use uc_protocol::v2::schedule::{ScheduleEntry, ScheduleRule};
        use uc_protocol::v2::upgrade::{RowRunning, SnapshotReport, UpgradePin};
        let mut st = state_with_one_agreed_and_one_commanded();
        st.settings.fsm_lag_bytes = u64::MAX - 1;
        st.table.entries = vec![ScheduleEntry {
            identity_hash: u64::MAX,
            timer_id: u64::MAX,
            rule: ScheduleRule::Once { at_ns: u64::MAX },
        }];
        st.catalog[0].time_ns = u64::MAX;
        st.settings.admission_bytes = u64::MAX - 1;
        st.settings.snapshot_interval_bytes = u64::MAX - 1;
        st.settings.backup_retain_age_secs = u64::MAX - 1;
        st.pins = vec![UpgradePin {
            row: 0,
            from: u32::MAX - 1,
            to: u32::MAX,
            origin: 1 << 40,
        }];
        st.reports = vec![SnapshotReport {
            row: 0,
            position: 1 << 40,
            hashes: vec![(1, u64::MAX, 0), (2, u64::MAX, 1 << 40)],
        }];
        st.running[0] = Some(RowRunning {
            row: 0,
            version: u32::MAX,
            record_pos: 1 << 40,
        });
        st.backups = vec![BackupTierEntry {
            tier_id: u64::MAX,
            oldest_set: 1 << 40,
            newest_set: 1 << 40,
            through: 1 << 40,
            reporter: 3,
            updated_pos: 1 << 40,
        }];
        let part = cluster_part(&st, &names(), Some(1 << 40), Some(false));
        assert_eq!(part.tiers[0].id, "0xffffffffffffffff");
        assert_eq!(
            part.upgrades[0].report.as_ref().unwrap().nodes[0].size,
            None
        );
        let v = serde_json::to_value(&part).unwrap();
        fn walk(v: &serde_json::Value, path: &str) {
            match v {
                serde_json::Value::Number(n) => {
                    if let Some(u) = n.as_u64() {
                        assert!(u <= 1u64 << 53, "{path} = {u} exceeds 2^53");
                    }
                }
                serde_json::Value::Array(a) => a
                    .iter()
                    .enumerate()
                    .for_each(|(i, x)| walk(x, &format!("{path}[{i}]"))),
                serde_json::Value::Object(o) => {
                    o.iter().for_each(|(k, x)| walk(x, &format!("{path}.{k}")))
                }
                _ => {}
            }
        }
        walk(&v, "cluster");
    }
}
