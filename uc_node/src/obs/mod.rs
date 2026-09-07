// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Observability: structured logging (M10).
//!
//! Records are emitted through [`crate::obs_event!`] at consensus
//! transition sites; [`log::LogLevel`] is read from the config file. The
//! record format itself lives in [`uc_obs`], shared with `uc2-gateway`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};

use uc_log::cnc::CncPage;
use uc_protocol::v2::cnc::CNC_MAX_SERVICES;

pub mod http;
pub mod metrics;

pub use metrics::SnapshotFreezeStats;

/// The structured-log core, re-exported from [`uc_obs`] so that
/// `uc_node::obs::log::…` keeps naming it. It moved out of this crate so
/// `uc2-gateway` (which must not depend on `uc_node`) can emit the same
/// record format — see the `uc_obs` crate docs.
pub use uc_obs::log;

/// A read-only bundle of the `Arc`-shared counters, flags, and config values
/// a later task's metrics encoder renders into a series — a straight
/// clone-and-collect over the fields [`crate::node::Node`] already owns, with
/// no new synchronization: every `Arc` here is the SAME allocation the owning
/// agent writes through.
///
/// `admission_bytes` is deliberately NOT here — the encoder reads it from the
/// live cnc page (`CncPage::admission_bytes()`) instead of a config-copied
/// snapshot, so there is exactly one source of truth for it.
///
/// `Clone`: every field is an `Arc` or a scalar, so cloning is cheap and
/// never diverges from the source — a clone shares the SAME underlying
/// `Arc` allocations, not a snapshot. Task 6 (`obs::http`) needs this: the
/// HTTP server thread owns one `ObsSources` by value (`ObsServer::serve`
/// takes it, not a reference — no lock shared with the hot-path agents),
/// while its tests keep a second clone to poke flags/heartbeats the running
/// server observes.
#[derive(Clone)]
pub struct ObsSources {
    pub node_id: u32,
    pub cnc: Arc<CncPage>,
    pub sender: Arc<uc_net::sender::SenderStats>,
    pub receiver: Arc<uc_net::receiver::FollowerStats>,
    pub truncations: Arc<AtomicU64>,
    pub wipes: Arc<AtomicU64>,
    /// Time-and-timers §6: per-row `fired`/`late` counters, the SAME
    /// allocation the consensus agent bumps.
    pub timer_stats: Arc<crate::timers::TimerStats>,
    /// Time-and-timers plan 2 (§6): the adopted schedule table's frame-END
    /// position (0 = none) and its entry count, published by the consensus
    /// agent whenever the COMMITTED table changes — the cluster FSM applies
    /// the `CLUSTER kind=ScheduleTable` command at commit and publishes the
    /// view, and `Consensus::refresh_from_view` re-arms the rows and these
    /// two gauges off it (at boot from the recovered cluster artifact, and
    /// per pass thereafter).
    pub schedule_table_position: Arc<AtomicU64>,
    pub schedule_entries: Arc<AtomicU64>,
    /// Plan 2: `schedule apply` requests this node refused, for any reason.
    pub schedule_apply_refused: Arc<AtomicU64>,
    /// Cluster FSM (spec §9): the cluster FSM's published view — the SAME
    /// allocation the `uc2-cluster` agent publishes into. Two gauges are read
    /// straight off its atomics AT SCRAPE TIME (`uc2_cluster_fsm_position`
    /// from `position`, `uc2_settings_position` from `settings_position`),
    /// one `Acquire` load each and no lock: deliberately NOT published into
    /// the consensus pass the way `schedule_table_position` is, because that
    /// pass is a measured hot path and a scrape is not.
    pub cluster_view: Arc<crate::cluster_fsm::ClusterView>,
    pub reports_unattested: Arc<AtomicU64>,
    pub reports_implausible: Arc<AtomicU64>,
    pub crypto_handshake_failures: Arc<AtomicU64>,
    /// Coordinated-snapshot spec §9: the last instant this node COMMANDED as
    /// leader, `0` if it never has (`uc2_snapshot_instant_position`). Leader-
    /// local — a follower's reading is stale, whatever it last commanded in
    /// some earlier term.
    pub snapshot_instant_position: Arc<AtomicU64>,
    /// Coordinated-snapshot spec §5.3/§9: the position of the newest
    /// COMPLETE snapshot set this node holds, `0` until the first one
    /// (`uc2_snapshot_set_position`) — must agree cluster-wide once caught
    /// up. Alerts: `Uc2SnapshotStalled`, `Uc2SnapshotSetDiverged`.
    pub snapshot_set_position: Arc<AtomicU64>,
    /// Spec §9: `uc2_snapshot_row_incomplete_total{row}` — instants row
    /// `row` failed to reach before being superseded.
    pub snapshot_row_incomplete: [Arc<AtomicU64>; CNC_MAX_SERVICES],
    /// Spec §5.7 item 4: the newest set this node FETCHED whole from a
    /// learner (`uc2_snapshot_fetched_position`), `0` if it never has.
    pub snapshot_fetched_position: Arc<AtomicU64>,
    /// Spec §9: process-local per-row freeze-duration bookkeeping the
    /// exporter derives from the row's cnc slot word each scrape — see
    /// [`metrics::SnapshotFreezeStats`].
    pub snapshot_freeze: Arc<SnapshotFreezeStats>,
    pub crypto_enabled: bool,
    pub purge_enabled: bool,
    pub journal_segment_bytes: u64,
    /// One entry per polling agent, in the FIXED order `consensus, sender,
    /// receiver, archive, cluster` regardless of spawn order — a later task's
    /// metric labels are positional against this order, so it must not drift
    /// with `Node::start`'s internal spawn sequence. `cluster` is the fifth
    /// agent (`uc2-cluster`, the cluster FSM's apply loop), added with the
    /// cluster FSM; it is labelled like its four siblings, without the
    /// `uc2-` thread-name prefix.
    pub agents: Vec<(&'static str, Arc<AtomicBool>)>,
}

#[cfg(any(test, fuzzing))]
impl ObsSources {
    /// A fully-formed `ObsSources` over a HEAP-backed cnc page and fresh
    /// zeroed stats — no instance directory, no node, no sockets. For tests
    /// and for the `uc_node_http` fuzz target, which needs to call the
    /// router several million times a second and cannot stage a live node.
    ///
    /// **Not API**: `cfg(any(test, fuzzing))` only. The values are fixed and
    /// deliberately boring; the router's job under fuzz is to survive the
    /// REQUEST bytes, not to report anything in particular. Note that both
    /// heartbeats are left at zero, so `/healthz` and `/readyz` take their
    /// stale-heartbeat (503) branches — which is what makes the 503 arm of
    /// the target's status assertion non-vacuous.
    pub fn for_tests() -> ObsSources {
        let meta = uc_log::cnc::CncMeta {
            node_id: 7,
            instance_id: 0x1122_3344_5566_7788,
            app_id: "fuzz".into(),
            buffer_bytes: 1 << 20,
            max_payload: 1200,
            services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
        };
        ObsSources {
            node_id: 7,
            cnc: CncPage::heap(&meta),
            sender: Arc::new(uc_net::sender::SenderStats::default()),
            receiver: Arc::new(uc_net::receiver::FollowerStats::default()),
            truncations: Arc::new(AtomicU64::new(0)),
            wipes: Arc::new(AtomicU64::new(0)),
            timer_stats: Arc::new(crate::timers::TimerStats::default()),
            schedule_table_position: Arc::new(AtomicU64::new(0)),
            schedule_entries: Arc::new(AtomicU64::new(0)),
            schedule_apply_refused: Arc::new(AtomicU64::new(0)),
            cluster_view: Arc::new(crate::cluster_fsm::ClusterView::new(
                &crate::cluster_fsm::ClusterState::genesis_empty(),
            )),
            reports_unattested: Arc::new(AtomicU64::new(0)),
            reports_implausible: Arc::new(AtomicU64::new(0)),
            crypto_handshake_failures: Arc::new(AtomicU64::new(0)),
            snapshot_instant_position: Arc::new(AtomicU64::new(0)),
            snapshot_set_position: Arc::new(AtomicU64::new(0)),
            snapshot_row_incomplete: std::array::from_fn(|_| Arc::new(AtomicU64::new(0))),
            snapshot_fetched_position: Arc::new(AtomicU64::new(0)),
            snapshot_freeze: Arc::new(SnapshotFreezeStats::default()),
            crypto_enabled: false,
            purge_enabled: false,
            journal_segment_bytes: 64 << 20,
            agents: vec![
                ("consensus", Arc::new(AtomicBool::new(false))),
                ("sender", Arc::new(AtomicBool::new(false))),
                ("receiver", Arc::new(AtomicBool::new(false))),
                ("archive", Arc::new(AtomicBool::new(false))),
                ("cluster", Arc::new(AtomicBool::new(false))),
            ],
        }
    }
}
