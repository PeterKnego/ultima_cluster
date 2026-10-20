// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! View sub-objects back to the domain types, so `uc2ctl`'s three `show`
//! commands print through their existing renderers (planning ruling PR9).
//! Lossless by construction: `build`'s round-trip test pins it.

use uc_protocol::v2::cnc::CNC_MAX_SERVICES;
use uc_protocol::v2::schedule::{ScheduleEntry, ScheduleRule, ScheduleTable};
use uc_protocol::v2::settings::{Settings, Target};
use uc_protocol::v2::upgrade::{RowRunning, SnapshotReport, UpgradePin};

use super::model::*;

pub fn settings(v: &SettingsView) -> Settings {
    Settings {
        fsm_lag_bytes: match v.fsm_lag {
            FsmLagView::Derived => 0,
            FsmLagView::Lockstep => u64::MAX,
            FsmLagView::Bytes { bytes } => bytes.0,
        },
        admission_bytes: v.admission_bytes.0,
        snapshot_interval_bytes: v.snapshot_interval_bytes.0,
        snapshot_target: match v.snapshot_target {
            TargetView::All => Target::All,
            TargetView::Learners => Target::Learners,
        },
        datagram_mtu: v.datagram_mtu,
        retain_sets: v.retain_sets,
        auto_fetch: v.auto_fetch,
        backup_retain_sets: v.backup_retain_sets,
        backup_retain_age_secs: v.backup_retain_age_secs.0,
        purge_requires_tiers: v.purge_requires_tiers,
        wire_level: v.wire_level,
        wire_raise: v.wire_raise,
    }
}

pub fn table(v: &ScheduleView) -> ScheduleTable {
    ScheduleTable {
        entries: v
            .entries
            .iter()
            .map(|e| ScheduleEntry {
                identity_hash: e.identity_hash.0,
                timer_id: e.timer_id.0,
                rule: match &e.rule {
                    RuleView::Every {
                        period_ns,
                        anchor_ns,
                        ..
                    } => ScheduleRule::Every {
                        period_ns: period_ns.0,
                        anchor_ns: anchor_ns.0,
                    },
                    RuleView::DailyAt { secs_of_day } => ScheduleRule::DailyAt {
                        secs_of_day: *secs_of_day,
                    },
                    RuleView::Once { at_ns, .. } => ScheduleRule::Once { at_ns: at_ns.0 },
                },
            })
            .collect(),
    }
}

pub type UpgradeParts = (
    Vec<UpgradePin>,
    Vec<SnapshotReport>,
    [Option<RowRunning>; CNC_MAX_SERVICES],
);

/// The pins, reports and running records the document carries, back in the
/// domain types. They round-trip PER ROW only: the document is grouped by
/// row, while `ClusterState::pins`/`reports` are commit-ordered across rows,
/// so the cross-row order is not recovered — which is all `upgrade show`
/// (a per-row renderer) needs.
pub fn upgrade(rows: &[UpgradeRowView]) -> UpgradeParts {
    let mut pins = Vec::new();
    let mut reports = Vec::new();
    let mut running: [Option<RowRunning>; CNC_MAX_SERVICES] = [None; CNC_MAX_SERVICES];
    for r in rows {
        pins.extend(r.pins.iter().map(|p| UpgradePin {
            row: r.row,
            from: p.from.packed,
            to: p.to.packed,
            origin: p.origin,
        }));
        if let Some(rep) = &r.report {
            reports.push(SnapshotReport {
                row: r.row,
                position: rep.position,
                hashes: rep
                    .nodes
                    .iter()
                    .map(|n| (n.node, n.hash.0, n.size.unwrap_or(0)))
                    .collect(),
            });
        }
        if let Some(run) = &r.running {
            running[r.row as usize] = Some(RowRunning {
                row: r.row,
                version: run.version.packed,
                record_pos: run.record_pos,
            });
        }
    }
    (pins, reports, running)
}
