// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
#![no_main]

use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::frame::{ClusterKind, read_cluster_prefix};
use uc_protocol::v2::{config::decode_config, schedule::decode_schedule_table, settings::decode_settings};
use uc_protocol::v2::upgrade::{decode_snapshot_report, decode_upgrade_pin};

// The CLUSTER body every node decodes off the log, kind-dispatched: total on any slice.
fuzz_target!(|data: &[u8]| {
    if let Some((kind, payload)) = read_cluster_prefix(data) {
        match kind {
            ClusterKind::Membership => {
                let _ = decode_config(payload);
            }
            ClusterKind::ScheduleTable => {
                let _ = decode_schedule_table(payload);
            }
            ClusterKind::Settings => {
                let _ = decode_settings(payload);
            }
            ClusterKind::UpgradePin => {
                let _ = decode_upgrade_pin(payload);
            }
            ClusterKind::SnapshotReport => {
                let _ = decode_snapshot_report(payload);
            }
        }
    }
});
