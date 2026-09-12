// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Prometheus text-format encoder over [`ObsSources`] — the M10 series
//! contract, exactly. See the plan's "The series contract" table for the
//! canonical row-by-row source of truth; [`CONTRACT_SERIES`] is its
//! machine-readable mirror (one entry per metric FAMILY name — a labeled
//! family such as `uc2_peer_reported_durable_bytes` appears once here even
//! though it renders multiple labeled samples).
//!
//! Pure function, no I/O: [`render_prometheus`] only reads through the
//! `Arc`s and atomics [`ObsSources`] already holds. All positions are byte
//! positions — this system has no indices.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use uc_log::cnc::unpack_service_status;
use uc_protocol::v2::cnc::{
    CNC_MAX_PEER_SLOTS, CNC_MAX_SERVICES, CNC_PEER_ROLE_LEARNER, CNC_PEER_ROLE_VOTER,
    NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER,
};

use super::ObsSources;
use crate::timers::{NS_BUCKET_SLOTS, NS_BUCKETS, NsHistogramSnapshot};

/// Unix nanoseconds "now" — shared with Task 6's `/healthz`/`/readyz` probes.
pub fn now_unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Every metric family name the encoder emits, in the order it renders
/// them. Task 8's alert rules reference only names from this list; Task
/// 10's coverage row scrapes a live `/metrics` and asserts every entry is
/// present.
pub const CONTRACT_SERIES: &[&str] = &[
    "uc2_build_info",
    "uc_node_id",
    "uc2_is_leader",
    "uc2_can_serve",
    "uc2_term",
    "uc2_leader_hint",
    "uc2_config_version",
    "uc2_config_pending",
    "uc_crypto_enabled",
    "uc2_purge_enabled",
    "uc2_admission_bytes",
    "uc2_journal_segment_bytes",
    "uc2_free_disk_bytes",
    "uc2_agent_alive",
    "uc2_append_bytes",
    "uc2_durable_bytes",
    "uc2_sent_bytes",
    "uc2_commit_bytes",
    // M14c (spec §9): the per-FSM band. The first four are the M10
    // aggregates, which now also carry one `service="<name>",row="<row>"`
    // sample per declared row in the SAME family block. Task 7 (spec §4.5)
    // added the identity-hash and version gauges.
    "uc_service_applied_bytes",
    "uc_service_epoch",
    "uc_service_snapshot_pos_bytes",
    "uc_service_heartbeat_age_seconds",
    "uc_service_attached",
    "uc_service_lag_bytes",
    "uc_service_lag_waits_total",
    "uc2_service_identity_hash",
    "uc2_service_version",
    "uc2_timers_pending",
    "uc2_timers_fired_total",
    "uc2_timers_late_total",
    // Coordinated-snapshot spec §9: the per-row snapshot families, plus the
    // three unlabeled positions below (fetched/instant/set).
    "uc2_snapshot_row_incomplete_total",
    "uc2_snapshot_freeze_seconds_max",
    "uc2_snapshot_freeze_seconds_sum",
    "uc2_snapshot_freeze_seconds_count",
    "uc2_snapshot_instant_position",
    "uc2_snapshot_standby_instant_position",
    "uc2_snapshot_set_position",
    "uc2_snapshot_fetched_position",
    // Plan 2 (spec §6): the replicated schedule table.
    "uc2_schedule_table_position",
    // Cluster FSM (spec §9): the cluster row's own two positions.
    "uc2_cluster_fsm_position",
    "uc2_settings_position",
    "uc2_schedule_entries",
    "uc2_schedule_apply_refused_total",
    "uc_services_declared",
    "uc2_fsm_lag_bytes",
    "uc2_log_time_ns",
    "uc2_log_time_lag_seconds",
    "uc2_log_clock_smear_ns",
    // Jumbo frames (spec §9): the discovered ceiling and the ladder's
    // activity. The first three are the door this cluster is actually
    // serving; the last four are how it got there and whether the paths
    // still carry it.
    "uc2_datagram_mtu_bytes",
    "uc2_payload_ceiling_bytes",
    "uc2_probe_min_mtu_bytes",
    "uc2_probe_sent_total",
    "uc2_probe_acked_total",
    "uc2_send_emsgsize_total",
    "uc2_commands_over_standard_total",
    "uc2_output_completed_bytes",
    "uc2_output_progress_bytes",
    "uc_node_snapshot_floor_bytes",
    "uc2_incoming_snapshot_pos_bytes",
    "uc2_archive_first_base_bytes",
    "uc2_commit_lag_bytes",
    "uc2_apply_lag_bytes",
    "uc2_admission_saturation",
    "uc_node_heartbeat_age_seconds",
    "uc2_peer_reported_durable_bytes",
    "uc2_peer_replication_lag_bytes",
    "uc2_peer_advertised_limit_bytes",
    "uc2_truncations_total",
    "uc2_wipes_total",
    "uc2_ingress_holes_skipped_total",
    "uc2_query_holes_skipped_total",
    "uc2_reports_unattested_total",
    "uc2_snapshot_refused_legacy_peer_total",
    "uc2_snapshot_refused_declared_set_total",
    "uc2_snapshot_refused_version_total",
    // Final wave M2: added with instants and rendered from the first commit,
    // but left out of the contract — so `m10_fleet_gate.py`'s family-coverage
    // check never saw them. Contracted now, with the three above them.
    "uc2_snapshot_refused_position_total",
    "uc2_snapshot_refused_fetch_expired_total",
    "uc2_snapshot_intake_io_failures_total",
    // M14c2 (T10a): the three counters that close M14c's snapshot-session
    // deferrals — the leader's `File::open` TOCTOU, the joiner's abandoned
    // intake, and the once-per-session undecodable `SNAP_BEGIN`.
    "uc2_snapshot_open_failed_total",
    "uc2_snapshot_intake_abandoned_total",
    "uc2_snapshot_begin_undecodable_total",
    "uc2_reports_implausible_total",
    "uc_crypto_handshake_failures_total",
    "uc2_sender_seal_failures_total",
    "uc2_receiver_seal_failures_total",
    "uc2_unknown_source_datagrams_total",
    "uc2_cleartext_peer_datagrams_total",
    "uc2_naks_sent_total",
    "uc2_naks_served_total",
    "uc2_naks_dropped_total",
    "uc2_naks_rejected_total",
    "uc2_replay_datagrams_total",
    "uc2_flow_stalls_total",
    "uc2_overruns_total",
    "uc2_heartbeats_sent_total",
    "uc2_sender_datagrams_total",
    "uc2_sender_bytes_total",
    "uc2_receiver_datagrams_total",
    "uc2_receiver_bytes_total",
    "uc2_snapshot_sessions_total",
    "uc2_snapshot_chunks_total",
    "uc2_snapshot_chunk_naks_total",
    "uc2_receiver_dropped_total",
    "uc2_truncation_resyncs_total",
    "uc2_term_change_discards_total",
    "uc2_counter_ahead_resyncs_total",
    "uc_net_event_drops_total",
];

/// Coordinated-snapshot spec §9: process-local per-row freeze-duration
/// bookkeeping, derived from the row's cnc slot word (`ServiceIdentityLine`'s
/// `freeze_ns`, service-written) once per SCRAPE, never per consensus-agent
/// pass. No histogram type exists in this encoder (`push_gauge`/
/// `push_gauge_f64`/`push_counter` only), so three series stand in for one:
///
/// - `uc2_snapshot_freeze_seconds_max{row}` (gauge): the longest freeze this
///   row has reported since [`Node::snapshot_instant_position`]'s reading
///   last ADVANCED — reset to 0 the scrape after that happens, so a stuck
///   row's stale worst-case does not linger forever once the leader moves on.
/// - `uc2_snapshot_freeze_seconds_sum{row}` / `_count{row}` (counters):
///   cumulative total and number of freezes observed, giving a mean.
///
/// A freeze is "observed" when the cnc word's value differs from what the
/// PREVIOUS scrape saw for that row — there is no edge/sequence number on
/// the word itself, so a row that freezes for the exact same duration twice
/// in a row is a known blind spot of this word-change detection, not a
/// double count. `#[derive(Default)]` zero-initializes every row.
///
/// Final wave M5/T8, spelled out because `_count`'s name invites the wrong
/// reading: the error is always an **UNDER**-count, never an over-count.
/// Two sources — identical consecutive durations (one sample), and N freezes
/// landing between two scrapes (one sample, the last value). And because this
/// struct is process-local derived state rather than a cnc word, every
/// counter here **resets to 0 when the node restarts**, so a `rate()` across
/// a restart reads as a drop to zero and back, the same as any other
/// process-local counter on this endpoint. The FIRST scrape after a restart
/// also folds whatever the cnc word happens to hold (`last_seen_ns` starts at
/// 0, so any nonzero reading differs) — one sample for a freeze that may have
/// happened before the restart. That is a deliberate trade: the alternative,
/// seeding `last_seen_ns` from the word at construction, would miss a genuine
/// freeze that lands during startup.
#[derive(Default)]
pub struct SnapshotFreezeStats {
    last_seen_ns: [AtomicU64; CNC_MAX_SERVICES],
    max_ns: [AtomicU64; CNC_MAX_SERVICES],
    sum_ns: [AtomicU64; CNC_MAX_SERVICES],
    count: [AtomicU64; CNC_MAX_SERVICES],
    last_instant_pos: AtomicU64,
}

impl SnapshotFreezeStats {
    /// Call once per scrape, before any [`SnapshotFreezeStats::observe_row`]
    /// call for that scrape: if `instant_pos`
    /// (`uc2_snapshot_instant_position`'s reading) has moved since the last
    /// scrape, every row's running max resets to 0 — "reset per instant".
    fn observe_instant(&self, instant_pos: u64) {
        if self.last_instant_pos.swap(instant_pos, Ordering::AcqRel) != instant_pos {
            for m in &self.max_ns {
                m.store(0, Ordering::Relaxed);
            }
        }
    }

    /// Fold this scrape's raw `freeze_ns` reading for `row` into the running
    /// stats and return the up-to-date `(max_ns, sum_ns, count)` triple to
    /// render. Must run after this scrape's [`SnapshotFreezeStats::observe_instant`]
    /// call, so a freeze that lands in the SAME scrape as the instant
    /// advancing folds into the fresh (just-reset) max.
    fn observe_row(&self, row: usize, raw_ns: u64) -> (u64, u64, u64) {
        if self.last_seen_ns[row].swap(raw_ns, Ordering::AcqRel) != raw_ns {
            self.sum_ns[row].fetch_add(raw_ns, Ordering::Relaxed);
            self.count[row].fetch_add(1, Ordering::Relaxed);
            self.max_ns[row].fetch_max(raw_ns, Ordering::Relaxed);
        }
        (
            self.max_ns[row].load(Ordering::Relaxed),
            self.sum_ns[row].load(Ordering::Relaxed),
            self.count[row].load(Ordering::Relaxed),
        )
    }
}

fn push_family_header(out: &mut String, name: &str, help: &str, ty: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(ty);
    out.push('\n');
}

fn push_gauge(out: &mut String, name: &str, help: &str, value: u64) {
    push_family_header(out, name, help, "gauge");
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn push_gauge_f64(out: &mut String, name: &str, help: &str, value: f64) {
    push_family_header(out, name, help, "gauge");
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn push_counter(out: &mut String, name: &str, help: &str, value: u64) {
    push_family_header(out, name, help, "counter");
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

/// Emit a labeled family: one `# HELP`/`# TYPE` pair followed by one sample
/// line per `(labels, value)` pair (`labels` is the pre-formatted
/// `key="val",key2="val2"` body, no braces). An empty `samples` renders the
/// header with no sample lines — the correct shape for e.g. no peers
/// occupied.
fn push_labeled(out: &mut String, name: &str, help: &str, ty: &str, samples: &[(String, u64)]) {
    push_family_header(out, name, help, ty);
    for (labels, value) in samples {
        out.push_str(name);
        out.push('{');
        out.push_str(labels);
        out.push_str("} ");
        out.push_str(&value.to_string());
        out.push('\n');
    }
}

/// M14c (spec §9): one row per DECLARED service id, gathered in a single
/// pass over page 2 so every per-FSM family renders from the same snapshot.
///
/// Unlike the peer band (which skips unoccupied slots), a declared id gets a
/// row even when nothing has ever attached to it: an absent FSM must show up
/// as `uc_service_attached{service="k",row="k"} 0`, not as a missing series —
/// `Uc2ServiceAbsent` cannot alert on a series that is not there. A harness
/// page (`services_declared == 0`) yields no rows, and the families then
/// render as headers alone, exactly like a node with no occupied peer slots.
///
/// Task 7 (spec §4.5): the row keeps its cluster-wide positional meaning
/// (`row`), but a service is FOUND by name, so every per-FSM label also
/// carries `service="<name>"` — the row's declared FSM name, empty string
/// for an unnamed line (only a harness page, `ServiceIdentityLine::name()`
/// returning `None`).
struct ServiceRow {
    /// The declared row's id — the index into the per-row stat arrays
    /// (`TimerStats::lateness_ns`) the histogram families read.
    id: usize,
    /// Pre-formatted label body, no braces: `service="kv",row="0"`.
    labels: String,
    attached: u64,
    applied: u64,
    epoch: u64,
    snapshot_pos: u64,
    /// `commit - applied`, saturating: the two counters are independent
    /// atomics read microseconds apart, so `applied > commit` is a normal
    /// racy snapshot, not an error.
    lag_bytes: u64,
    lag_waits: u64,
    heartbeat_age: f64,
    /// FNV-1a 64 of the row's declared name (line 7, written once at boot).
    identity_hash: u64,
    /// Packed semantic version the attached service last wrote (0 = none).
    version: u64,
    /// Time-and-timers §6: pending scheduled timers for this row (cnc slot
    /// line 7's live word, refreshed once per consensus-agent pass).
    timers_pending: u64,
    /// TIMER frames this node appended as leader for the row.
    fired: u64,
    /// Fired timers whose stamp exceeded their deadline.
    late: u64,
    /// Coordinated-snapshot spec §9: instants this row failed to reach
    /// before being superseded.
    snapshot_row_incomplete: u64,
    /// The longest freeze this row has reported since the last instant
    /// advance, in seconds (`uc2_snapshot_freeze_seconds_max`).
    freeze_max_seconds: f64,
    /// Cumulative freeze duration this row has reported, in seconds
    /// (`uc2_snapshot_freeze_seconds_sum`).
    freeze_sum_seconds: f64,
    /// Distinct freeze durations sampled for this row
    /// (`uc2_snapshot_freeze_seconds_count`) — a LOWER bound on freezes; see
    /// [`SnapshotFreezeStats`].
    freeze_count: u64,
}

fn service_rows(s: &ObsSources, commit: u64, now: u64) -> Vec<ServiceRow> {
    let declared = s.cnc.services_declared();
    // Coordinated-snapshot spec §9: reset every row's running freeze max if
    // the leader's last-commanded instant has advanced since the last
    // scrape — BEFORE any row's raw reading folds in below, so a freeze that
    // lands in the same scrape as the advance counts toward the fresh max.
    //
    // Ruling P13(b) split that reading in two, so the edge is the max of
    // both: a VOTER's rows freeze for the FULL instants it commands (the
    // standby gauge is 0 there), and a LEARNER's rows freeze for the STANDBY
    // instants its uc2-cluster agent acts on (it never leads, so the full
    // gauge is 0 or a stale reading from an earlier term). Taking the max
    // keeps one edge detector for both roles; positions only grow, so it
    // moves exactly when the instant this node's rows are working on does.
    s.snapshot_freeze.observe_instant(
        s.snapshot_instant_position
            .load(Ordering::Relaxed)
            .max(s.snapshot_standby_instant_position.load(Ordering::Acquire)),
    );
    let mut rows = Vec::new();
    for id in 0..CNC_MAX_SERVICES as u8 {
        if declared & (1u64 << id) == 0 {
            continue;
        }
        let slot = s.cnc.service_slot(id as usize);
        let (_, attached, _) = unpack_service_status(slot.status.load_acquire());
        let applied = slot.applied.load_acquire();
        let hb = slot.heartbeat_ns.load_acquire();
        let name = slot
            .identity
            .name()
            .map(|n| n.as_str().to_string())
            .unwrap_or_default();
        let (freeze_max_ns, freeze_sum_ns, freeze_count) = s
            .snapshot_freeze
            .observe_row(id as usize, slot.identity.freeze_ns());
        rows.push(ServiceRow {
            id: id as usize,
            labels: format!("service=\"{name}\",row=\"{id}\""),
            attached: attached as u64,
            applied,
            epoch: slot.epoch.load_acquire(),
            snapshot_pos: slot.snapshot_pos.load_acquire(),
            lag_bytes: commit.saturating_sub(applied),
            lag_waits: slot.lag_waits.load_acquire(),
            heartbeat_age: now.saturating_sub(hb) as f64 / 1e9,
            identity_hash: slot.identity.hash(),
            version: slot.status.version() as u64,
            timers_pending: slot.identity.timers_pending(),
            fired: s.timer_stats.fired[id as usize].load(Ordering::Relaxed),
            late: s.timer_stats.late[id as usize].load(Ordering::Relaxed),
            snapshot_row_incomplete: s.snapshot_row_incomplete[id as usize].load(Ordering::Relaxed),
            freeze_max_seconds: freeze_max_ns as f64 / 1e9,
            freeze_sum_seconds: freeze_sum_ns as f64 / 1e9,
            freeze_count,
        });
    }
    rows
}

/// One family block carrying BOTH the unlabeled aggregate sample and one
/// labeled sample per declared FSM. They are the same metric FAMILY, so they
/// must share a single `# HELP`/`# TYPE` pair — a second header for a name
/// already seen in the same scrape is a parse error on Prometheus's side.
/// The query-side consequence, documented in `monitor-a-cluster.md`:
/// `sum(<name>)` double counts, so "the aggregate" is `<name>{service=""}`
/// and "per FSM" is `<name>{service!=""}`.
fn push_gauge_with_services(
    out: &mut String,
    name: &str,
    help: &str,
    aggregate: u64,
    rows: &[ServiceRow],
    pick: impl Fn(&ServiceRow) -> u64,
) {
    push_family_header(out, name, help, "gauge");
    out.push_str(name);
    out.push(' ');
    out.push_str(&aggregate.to_string());
    out.push('\n');
    for r in rows {
        out.push_str(name);
        out.push('{');
        out.push_str(&r.labels);
        out.push_str("} ");
        out.push_str(&pick(r).to_string());
        out.push('\n');
    }
}

/// [`push_gauge_with_services`] for an `f64` family (the heartbeat ages).
fn push_gauge_f64_with_services(
    out: &mut String,
    name: &str,
    help: &str,
    aggregate: f64,
    rows: &[ServiceRow],
    pick: impl Fn(&ServiceRow) -> f64,
) {
    push_family_header(out, name, help, "gauge");
    out.push_str(name);
    out.push(' ');
    out.push_str(&aggregate.to_string());
    out.push('\n');
    for r in rows {
        out.push_str(name);
        out.push('{');
        out.push_str(&r.labels);
        out.push_str("} ");
        out.push_str(&pick(r).to_string());
        out.push('\n');
    }
}

/// A per-FSM family with no aggregate twin (`attached`, `lag_bytes`,
/// `lag_waits_total`): header plus one labeled sample per declared id.
fn push_service_labeled(
    out: &mut String,
    name: &str,
    help: &str,
    ty: &str,
    rows: &[ServiceRow],
    pick: impl Fn(&ServiceRow) -> u64,
) {
    let samples: Vec<(String, u64)> = rows.iter().map(|r| (r.labels.clone(), pick(r))).collect();
    push_labeled(out, name, help, ty, &samples);
}

/// [`push_labeled`] for an `f64` sample value.
fn push_labeled_f64(out: &mut String, name: &str, help: &str, ty: &str, samples: &[(String, f64)]) {
    push_family_header(out, name, help, ty);
    for (labels, value) in samples {
        out.push_str(name);
        out.push('{');
        out.push_str(labels);
        out.push_str("} ");
        out.push_str(&value.to_string());
        out.push('\n');
    }
}

/// [`push_service_labeled`] for an `f64` family with no aggregate twin —
/// `uc2_snapshot_freeze_seconds_max`/`_sum` (spec §9), which have no
/// histogram type to render as.
fn push_service_labeled_f64(
    out: &mut String,
    name: &str,
    help: &str,
    ty: &str,
    rows: &[ServiceRow],
    pick: impl Fn(&ServiceRow) -> f64,
) {
    let samples: Vec<(String, f64)> = rows.iter().map(|r| (r.labels.clone(), pick(r))).collect();
    push_labeled_f64(out, name, help, ty, &samples);
}

/// Emit one Prometheus HISTOGRAM family: the `# HELP`/`# TYPE` pair, then per
/// sample a `_bucket` line for every [`NS_BUCKETS`] bound plus `le="+Inf"`,
/// then `_sum` and `_count`. `labels` is the pre-formatted label body without
/// braces (empty for a node-level histogram, which then renders
/// `name_bucket{le="…"}` and bare `name_sum` / `name_count`).
///
/// The bucket counts must already be CUMULATIVE — [`NsHistogram::snapshot`]
/// makes them so — because `le` means "at or below", not "in this band".
///
/// These two families are deliberately NOT in [`CONTRACT_SERIES`]: that list
/// drives `bench-infra/scripts/m10_fleet_gate.py`'s coverage row, which
/// queries each entry as an instant Prometheus expression, and a histogram's
/// FAMILY name is not a queryable series (only `_bucket`/`_sum`/`_count`
/// are). Contracting the family name would fail that row for a series that is
/// present and correct; contracting its three rendered names instead would
/// break the list's one-entry-per-family rule. The `_max` gauges beside them
/// are ordinary gauges and could be contracted, but are left out with them so
/// the two histograms are documented as one unit.
fn push_histogram(
    out: &mut String,
    name: &str,
    help: &str,
    samples: &[(String, NsHistogramSnapshot)],
) {
    push_family_header(out, name, help, "histogram");
    for (labels, h) in samples {
        let sep = if labels.is_empty() { "" } else { "," };
        for (i, bound) in NS_BUCKETS.iter().enumerate() {
            out.push_str(name);
            out.push_str("_bucket{");
            out.push_str(labels);
            out.push_str(sep);
            out.push_str("le=\"");
            out.push_str(&bound.to_string());
            out.push_str("\"} ");
            out.push_str(&h.cumulative[i].to_string());
            out.push('\n');
        }
        out.push_str(name);
        out.push_str("_bucket{");
        out.push_str(labels);
        out.push_str(sep);
        out.push_str("le=\"+Inf\"} ");
        out.push_str(&h.cumulative[NS_BUCKET_SLOTS - 1].to_string());
        out.push('\n');
        for (suffix, value) in [("_sum", h.sum), ("_count", h.count)] {
            out.push_str(name);
            out.push_str(suffix);
            if labels.is_empty() {
                out.push(' ');
            } else {
                out.push('{');
                out.push_str(labels);
                out.push_str("} ");
            }
            out.push_str(&value.to_string());
            out.push('\n');
        }
    }
}

/// M14c (spec §9): every per-FSM family, aggregates included, as one
/// contiguous block. `commit` and `now` are this scrape's single samples,
/// threaded in so the block is consistent with the rest of the render.
///
/// The aggregates are page 1's `min` over the declared ids, published once
/// per cycle by the node (`crate::services::service_mins` /
/// `Consensus::publish_service_mins`) — they now mean "the slowest FSM".
/// `uc_service_epoch`'s aggregate is the exception: it is FSM 0's epoch
/// (M14a retired page 1's `service_epoch`), not a min.
fn push_service_families(out: &mut String, s: &ObsSources, commit: u64, now: u64) {
    let rows = service_rows(s, commit, now);
    let service = s.cnc.service();
    let snapshots = s.cnc.snapshots();
    let status = s.cnc.status();

    push_gauge_with_services(
        out,
        "uc_service_applied_bytes",
        "Position the service state machine has applied through (unlabeled = the SLOWEST declared FSM; one labeled sample per declared FSM).",
        service.service_applied.load_acquire(),
        &rows,
        |r| r.applied,
    );
    push_gauge_with_services(
        out,
        "uc_service_epoch",
        "Service incarnation counter, bumped each attach (unlabeled = FSM 0's, the M10 series; one labeled sample per declared FSM).",
        s.cnc.service_slot(0).epoch.load_acquire(),
        &rows,
        |r| r.epoch,
    );
    push_gauge_with_services(
        out,
        "uc_service_snapshot_pos_bytes",
        "Position of the newest complete service-built snapshot, 0 = none (unlabeled = the min over declared FSMs, which is the purge floor).",
        snapshots.service_snapshot_pos.load_acquire(),
        &rows,
        |r| r.snapshot_pos,
    );
    push_gauge_f64_with_services(
        out,
        "uc_service_heartbeat_age_seconds",
        "Seconds since a service heartbeat was last stamped, unlabeled = the stalest declared FSM (a never-written heartbeat reads as a huge age, by design).",
        now.saturating_sub(status.service_heartbeat_ns.load_acquire()) as f64 / 1e9,
        &rows,
        |r| r.heartbeat_age,
    );
    push_service_labeled(
        out,
        "uc_service_attached",
        "1 if this declared FSM's slot has the ATTACHED bit set. A declared FSM that never started reads 0 here and holds admission closed.",
        "gauge",
        &rows,
        |r| r.attached,
    );
    push_service_labeled(
        out,
        "uc_service_lag_bytes",
        "commit - this FSM's applied position (saturating). Pinned at uc2_fsm_lag_bytes means this FSM is pacing the cluster.",
        "gauge",
        &rows,
        |r| r.lag_bytes,
    );
    push_service_labeled(
        out,
        "uc_service_lag_waits_total",
        "Times this FSM's apply loop waited at the lag barrier for a sibling.",
        "counter",
        &rows,
        |r| r.lag_waits,
    );
    push_service_labeled(
        out,
        "uc2_service_identity_hash",
        "FNV-1a 64 of the row's declared FSM name; must be identical on every node. Stored as a float64 sample on the wire (exact — 64-bit integers up to 2^53 round-trip losslessly, and any two real FNV-1a 64 hashes would have to agree in their top 53 bits to collide after scraping). Alert: Uc2ServiceIdentityDrift (packaging/prometheus/uc2-alerts.yml) — `count by (row) (count_values(\"hash\", uc2_service_identity_hash) by (row)) > 1`; a bare `count by (row) (uc2_service_identity_hash) > 1` counts SERIES (instances), not distinct values, and pages permanently on any multi-node cluster.",
        "gauge",
        &rows,
        |r| r.identity_hash,
    );
    push_service_labeled(
        out,
        "uc2_service_version",
        "Packed semantic version of the attached service (0 = none/unversioned). Alert: Uc2ServiceVersionDrift (packaging/prometheus/uc2-alerts.yml) — `count by (row) (count_values(\"version\", uc2_service_version > 0) by (row)) > 1`; a bare `count by (row, service) (uc2_service_version > 0) > 1` counts SERIES, not distinct values, and pages permanently on any multi-node cluster.",
        "gauge",
        &rows,
        |r| r.version,
    );
    push_service_labeled(
        out,
        "uc2_timers_pending",
        "Pending scheduled timers for this row (time-and-timers spec §6, cluster-FSM spec §4.9): the heap is leader-only, so this is the leader's count — a follower always exports 0.",
        "gauge",
        &rows,
        |r| r.timers_pending,
    );
    push_service_labeled(
        out,
        "uc2_timers_fired_total",
        "TIMER frames this node appended as leader for the row.",
        "counter",
        &rows,
        |r| r.fired,
    );
    push_service_labeled(
        out,
        "uc2_timers_late_total",
        "Fired timers whose stamp exceeded their deadline (post-failover or scheduled in the past).",
        "counter",
        &rows,
        |r| r.late,
    );
    // Time-and-timers gate row c: the lateness distribution per row and the
    // consensus-pass length that its bar is stated against ("p99 lateness <=
    // 2 x the measured pass length on the rig"). Both are leader-side by
    // construction — a follower fires no timers and takes no pass interval —
    // and both are rendered here, beside the counters they explain, even
    // though the pass histogram carries no row label.
    let lateness: Vec<(String, NsHistogramSnapshot)> = rows
        .iter()
        .map(|r| (r.labels.clone(), s.timer_stats.lateness_ns[r.id].snapshot()))
        .collect();
    push_histogram(
        out,
        "uc2_timer_lateness_ns",
        "WALL-CLOCK lateness of every TIMER frame this node appended as leader for the row: the pass clock minus the fired deadline, i.e. how far past its deadline the pass that PLACED the frame ran (time-and-timers gate row c). NOT the on-the-wire time_ns - deadline_ns, which is 0 for every on-time fire by construction. Bounded below by the consensus-pass length, so read it against uc2_consensus_pass_ns.",
        &lateness,
    );
    push_labeled(
        out,
        "uc2_timer_lateness_ns_max",
        "The largest uc2_timer_lateness_ns observation for this row since the node started (process-local, so it resets on restart and never decreases while it runs).",
        "gauge",
        &lateness
            .iter()
            .map(|(l, h)| (l.clone(), h.max))
            .collect::<Vec<_>>(),
    );
    let pass = s.timer_stats.pass_ns.snapshot();
    push_histogram(
        out,
        "uc2_consensus_pass_ns",
        "Interval between consecutive consensus-pass clock readings while this node LEADS (time-and-timers gate row c's denominator). Derived from the one wall-clock reading the pass already takes, never from a clock read of its own; the first pass of a leadership term is skipped, so a demotion/promotion cycle contributes no giant sample. A follower's histogram stops advancing rather than reading 0.",
        &[(String::new(), pass)],
    );
    push_gauge(
        out,
        "uc2_consensus_pass_ns_max",
        "The longest uc2_consensus_pass_ns interval since the node started (process-local, so it resets on restart and never decreases while it runs).",
        pass.max,
    );
    push_service_labeled(
        out,
        "uc2_snapshot_row_incomplete_total",
        "Instants this row OWED a freeze for and failed to reach before being superseded (coordinated-snapshot spec §9): the row never froze in time, and the next instant supersedes it. A superseded STANDBY instant on a node that is not a learner is NOT counted here (ruling P13): a voter's row is supposed not to freeze for one.",
        "counter",
        &rows,
        |r| r.snapshot_row_incomplete,
    );
    push_service_labeled_f64(
        out,
        "uc2_snapshot_freeze_seconds_max",
        "The longest freeze() call this row has reported since the instant its node's rows are working on last advanced (coordinated-snapshot spec §5.7/§9) — max(uc2_snapshot_instant_position, uc2_snapshot_standby_instant_position), so the FULL instant on a voter and the STANDBY one on a learner; reset to 0 on the scrape after that moves. No histogram type exists in this encoder, so this and _sum/_count stand in for one.",
        "gauge",
        &rows,
        |r| r.freeze_max_seconds,
    );
    push_service_labeled_f64(
        out,
        "uc2_snapshot_freeze_seconds_sum",
        "Cumulative freeze() duration this row has reported, in seconds (coordinated-snapshot spec §9); paired with _count for a mean.",
        "counter",
        &rows,
        |r| r.freeze_sum_seconds,
    );
    push_service_labeled(
        out,
        "uc2_snapshot_freeze_seconds_count",
        "DISTINCT freeze durations sampled from this row's cnc word at scrape boundaries (coordinated-snapshot spec §9); paired with _sum for a mean. Read it as a LOWER BOUND on freezes, not a count of them: the word carries no sequence number, so two identical consecutive durations count once and N freezes between two scrapes count once. It also resets to 0 when the node process restarts (it is process-local, derived state, not a cnc word).",
        "counter",
        &rows,
        |r| r.freeze_count,
    );
    push_gauge(
        out,
        "uc2_snapshot_instant_position",
        "The last snapshot instant this node COMMANDED as leader, 0 if never (coordinated-snapshot spec §9); leader-local — a follower's reading is whatever it last commanded in some earlier term.",
        s.snapshot_instant_position.load(Ordering::Relaxed),
    );
    push_gauge(
        out,
        "uc2_snapshot_standby_instant_position",
        "The last STANDBY snapshot instant this node's uc2-cluster agent ACTED on, 0 if it never has (coordinated-snapshot spec §5.7/§9, ruling P13). LEARNER-ONLY: a voter skips every standby instant by design, so it exports 0 — which is what keeps Uc2StandbySnapshotStalled off voters. Alert: Uc2StandbySnapshotStalled.",
        s.snapshot_standby_instant_position.load(Ordering::Acquire),
    );
    push_gauge(
        out,
        "uc2_snapshot_set_position",
        "The position of the newest COMPLETE snapshot set this node holds, 0 until the first one; must agree cluster-wide once caught up (coordinated-snapshot spec §5.3/§9). Alerts: Uc2SnapshotStalled, Uc2SnapshotSetDiverged.",
        s.snapshot_set_position.load(Ordering::Acquire),
    );
    push_gauge(
        out,
        "uc2_snapshot_fetched_position",
        "The newest set this node FETCHED whole from a learner (coordinated-snapshot spec §5.7 item 4), 0 if it never has.",
        s.snapshot_fetched_position.load(Ordering::Acquire),
    );
    push_gauge(
        out,
        "uc2_schedule_table_position",
        "Frame-END position of the schedule table this node's cluster FSM has APPLIED (0 = none); the table is cluster-FSM state applied at COMMIT, so this is identical on every node once caught up (cluster-FSM spec §4.3). Alert: Uc2ScheduleTableDiverged.",
        s.schedule_table_position.load(Ordering::Relaxed),
    );
    push_gauge(
        out,
        "uc2_cluster_fsm_position",
        "Frame-END position this node's cluster FSM has CONSUMED the log up to (its `applied`, cluster-FSM spec §4.3) — the position tag on the view it publishes and on the artifact it writes. It advances with the agent's walk, not only on CLUSTER commands, so it is a per-node liveness reading (compare it against uc2_commit_bytes on the SAME node: a stalled uc2-cluster agent is one whose position sits still while commit moves) and NOT a cluster-wide constant — Uc2ScheduleTableDiverged keys on uc2_schedule_table_position for exactly that reason.",
        s.cluster_view.position.load(Ordering::Acquire),
    );
    push_gauge(
        out,
        "uc2_settings_position",
        "Frame-END position of the last `CLUSTER kind=Settings` command this node's cluster FSM applied; 0 = the genesis record from `[settings]` in node.toml, which never crossed the log. Identical on every node once caught up, exactly like uc2_schedule_table_position.",
        s.cluster_view.settings_position.load(Ordering::Acquire),
    );
    push_gauge(
        out,
        "uc2_schedule_entries",
        "Entries in the COMMITTED schedule table the cluster FSM's view holds that name a row this node declares — read from the view, not from the timer heap, so it is identical on leader and follower alike (the heap is leader-only since the cluster FSM). A parked `once` (already delivered) still counts here, unlike uc2_timers_pending.",
        s.schedule_entries.load(Ordering::Relaxed),
    );
    push_counter(
        out,
        "uc2_schedule_apply_refused_total",
        "`uc2ctl schedule apply` requests this node refused (bad digest, missing or undecodable staged file, or an entry naming an undeclared FSM). Retries are NOT counted: neither the one a follower answers (the staged file is node-local, so the request is never forwarded) nor the one the leader answers while the previous cluster command is still above commit (single-in-flight).",
        s.schedule_apply_refused.load(Ordering::Relaxed),
    );
    push_gauge(
        out,
        "uc_services_declared",
        "Bitmask of declared rows (contiguous from 0); must match cluster-wide; a mismatch refuses snapshot sessions.",
        s.cnc.services_declared(),
    );
    push_gauge(
        out,
        "uc2_fsm_lag_bytes",
        "The configured FSM lag bound in bytes; 0 means lockstep.",
        s.cnc.fsm_lag_bytes(),
    );

    let is_leader = status.flags.load_acquire() & NODE_FLAG_LEADER != 0;
    let log_time = s.cnc.log_time_ns();
    push_gauge(
        out,
        "uc2_log_time_ns",
        "The highest leader stamp the archive has recorded: the log's clock, identical on every replica once caught up (time-and-timers spec §3).",
        log_time,
    );
    let lag_s = if is_leader && log_time > 0 {
        now.saturating_sub(log_time) / 1_000_000_000
    } else {
        0
    };
    push_gauge(
        out,
        "uc2_log_time_lag_seconds",
        "Leader only (0 elsewhere): wall clock minus the log's clock, floored at 0. Grows only when nothing is being appended — since 2.12.0 a backward wall-clock step no longer parks it (the log clock smears instead; see uc2_log_clock_smear_ns). Alert: Uc2LogTimeFrozen.",
        lag_s,
    );
    push_gauge(
        out,
        "uc2_log_clock_smear_ns",
        "Per node: nanoseconds of a backward step of THIS node's wall clock that its log clock is still retiring by running 500 ppm slow (spec 2026-09-08 §5.3). On the leader this is the log's clock; on a follower it is the clock that node would lead with after a failover — every node's clock is a candidate leader clock, so it is reported everywhere. The log clock is AHEAD of wall time while this is nonzero, so uc2_log_time_lag_seconds reads 0 — this gauge is the only sign of a smear.",
        s.log_clock_smear_ns.load(Ordering::Relaxed),
    );

    // Jumbo frames (spec §9). The rung and the ceiling come off the same two
    // sources every other reader uses — the committed cluster view and the
    // cnc word — so a scrape can never disagree with what the appender
    // enforces. The four counters are the agents' own cells, read here and
    // published nowhere else.
    push_gauge(
        out,
        "uc2_datagram_mtu_bytes",
        "The committed datagram rung this node applies (jumbo spec §5.3); 1408 = the baseline every cluster starts from. Must agree cluster-wide once caught up.",
        s.datagram_mtu() as u64,
    );
    push_gauge(
        out,
        "uc2_payload_ceiling_bytes",
        "The live command payload ceiling in bytes — min(this node's max_payload bound, payload_ceiling(committed rung, crypto)), the same value clients read from the cnc page.",
        s.cnc.payload_ceiling(),
    );
    // ONE encoding of "nothing is proven", which is 0. `own_min_rung` answers
    // MTU_BOUND for an empty peer map — errata 4's parenthetical, which the
    // commit rule's neighbourhood depends on and which stays exactly as it is
    // — but that is the helper's internal convention, not this series'
    // contract: exported raw it would make a solo cluster (which has measured
    // nothing) read identically to one that genuinely verified the top rung,
    // and would leave `Uc2MtuDiscoveryStalled` firing forever on every
    // one-node deployment.
    push_gauge(
        out,
        "uc2_probe_min_mtu_bytes",
        "This node's own verified minimum over its PEERS (jumbo spec §5.2). 0 means NOTHING IS PROVEN — either this node has no peers (a solo cluster has measured no path) or some peer is still unresolved. The leader's commit rule consults the stricter table minimum, which also requires every member's advertised half.",
        if s.probe.peers().is_empty() {
            0
        } else {
            s.probe.own_min_rung() as u64
        },
    );
    push_counter(
        out,
        "uc2_probe_sent_total",
        "PROBE datagrams this node put on the wire (jumbo spec §5.1). A round that put nothing on the wire — no pairwise crypto session yet, or every rung refused for size — is not counted here.",
        s.sender.probes_sent.load(Ordering::Relaxed),
    );
    push_counter(
        out,
        "uc2_probe_acked_total",
        "PROBE_ACK datagrams this node received and credited into its probe table.",
        s.receiver.probe_acks.load(Ordering::Relaxed),
    );
    push_counter(
        out,
        "uc2_send_emsgsize_total",
        "Non-probe datagrams the kernel refused for size under do-not-fragment: a path that degraded BELOW the committed rung, which the rung itself never lowers. Must be 0 (alert: Uc2PathBelowMtu). Probe refusals are counted separately and are a normal part of the ladder.",
        s.sender.emsgsize.load(Ordering::Relaxed),
    );
    push_counter(
        out,
        "uc2_commands_over_standard_total",
        "Frames this node appended above the standard 1312 B ceiling: nonzero means this deployment now depends on jumbo-frame support (jumbo spec §8). Leader-only by construction — only a leader appends — so read it summed across the fleet.",
        s.commands_over_standard.load(Ordering::Relaxed),
    );
}

/// Render the full M10 series contract over one snapshot of `s`'s `Arc`s
/// and atomics. Pure — no I/O; all allocation (the labeled-sample `Vec`s,
/// the per-line `format!` calls, and the returned `String` itself) happens
/// only on the calling (exporter) thread, never on any hot-path agent
/// thread.
pub fn render_prometheus(s: &ObsSources) -> String {
    let mut out = String::with_capacity(8 * 1024);

    push_family_header(
        &mut out,
        "uc2_build_info",
        "Static build version info.",
        "gauge",
    );
    out.push_str(&format!(
        "uc2_build_info{{version=\"{}\"}} 1\n",
        env!("CARGO_PKG_VERSION")
    ));

    push_gauge(
        &mut out,
        "uc_node_id",
        "This node's configured id.",
        s.node_id as u64,
    );

    let status = s.cnc.status();
    let flags = status.flags.load_acquire();
    push_gauge(
        &mut out,
        "uc2_is_leader",
        "1 if this node currently holds NODE_FLAG_LEADER.",
        (flags & NODE_FLAG_LEADER != 0) as u64,
    );
    push_gauge(
        &mut out,
        "uc2_can_serve",
        "1 if this node currently holds NODE_FLAG_CAN_SERVE.",
        (flags & NODE_FLAG_CAN_SERVE != 0) as u64,
    );
    push_gauge(
        &mut out,
        "uc2_term",
        "Current consensus term.",
        status.term.load_acquire(),
    );

    let leader_hint = status.leader_hint.load_acquire();
    if leader_hint != u64::MAX {
        push_gauge(
            &mut out,
            "uc2_leader_hint",
            "Last known leader node id.",
            leader_hint,
        );
    }

    push_gauge(
        &mut out,
        "uc2_config_version",
        "Adopted cluster-config version.",
        s.cnc.config_version(),
    );
    push_gauge(
        &mut out,
        "uc2_config_pending",
        "1 if a config change is uncommitted.",
        s.cnc.config_pending(),
    );

    push_gauge(
        &mut out,
        "uc_crypto_enabled",
        "1 if wire crypto (M8) is enabled on this node.",
        s.crypto_enabled as u64,
    );
    push_gauge(
        &mut out,
        "uc2_purge_enabled",
        "1 if journal/log purge is enabled on this node.",
        s.purge_enabled as u64,
    );

    let admission_bytes = s.cnc.admission_bytes();
    push_gauge(
        &mut out,
        "uc2_admission_bytes",
        "Configured admission window in bytes.",
        admission_bytes,
    );
    push_gauge(
        &mut out,
        "uc2_journal_segment_bytes",
        "Configured journal segment size in bytes.",
        s.journal_segment_bytes,
    );

    // M11 (Task 5): omitted when 0 — the daemon's derived-events pass is the
    // only writer, so an in-process library user (no daemon loop) or a
    // pre-M11 node reads the sentinel, same convention as `uc2_leader_hint`.
    let free_disk_bytes = s.cnc.free_disk_bytes();
    if free_disk_bytes != 0 {
        push_gauge(
            &mut out,
            "uc2_free_disk_bytes",
            "Free bytes on the filesystem backing the instance dir, as of the daemon's last ~1s poll.",
            free_disk_bytes,
        );
    }

    let agent_samples: Vec<(String, u64)> = s
        .agents
        .iter()
        .map(|(name, flag)| {
            let alive = if flag.load(Ordering::Acquire) { 0 } else { 1 };
            (format!("agent=\"{name}\""), alive)
        })
        .collect();
    push_labeled(
        &mut out,
        "uc2_agent_alive",
        "1 if the named polling agent's worker loop is still running.",
        "gauge",
        &agent_samples,
    );

    let counters = s.cnc.counters();
    let append = counters.append.load_acquire();
    let durable = counters.durable.load_acquire();
    let sent = counters.sent.load_acquire();
    let commit = counters.commit.load_acquire();
    push_gauge(
        &mut out,
        "uc2_append_bytes",
        "Local append counter.",
        append,
    );
    push_gauge(
        &mut out,
        "uc2_durable_bytes",
        "Local archive-durable counter.",
        durable,
    );
    push_gauge(
        &mut out,
        "uc2_sent_bytes",
        "Local replication-sent counter.",
        sent,
    );
    push_gauge(
        &mut out,
        "uc2_commit_bytes",
        "Cluster commit counter.",
        commit,
    );

    let service = s.cnc.service();
    let service_applied = service.service_applied.load_acquire();
    // M14c (spec §9): the whole per-FSM band — the four M10 aggregates (now
    // "slowest FSM") each with their labelled twins, plus attached/lag/
    // lag_waits/declared/fsm_lag. `now` moves up from the heartbeat block
    // below so one clock sample covers both.
    let now = now_unix_ns();
    push_service_families(&mut out, s, commit, now);
    push_gauge(
        &mut out,
        "uc2_output_completed_bytes",
        "Durable (increase-only) output-handler progress marker.",
        service.output_completed.load_acquire(),
    );

    push_gauge(
        &mut out,
        "uc2_output_progress_bytes",
        "Node-observed output-handler progress marker.",
        status.output_progress.load_acquire(),
    );

    let snapshots = s.cnc.snapshots();
    push_gauge(
        &mut out,
        "uc_node_snapshot_floor_bytes",
        "Node-side mirror of the snapshot floor the purge driver honors.",
        snapshots.node_snapshot_floor.load_acquire(),
    );
    push_gauge(
        &mut out,
        "uc2_incoming_snapshot_pos_bytes",
        "Position of the newest complete inbound snapshot this node landed.",
        snapshots.incoming_snapshot_pos.load_acquire(),
    );

    push_gauge(
        &mut out,
        "uc2_archive_first_base_bytes",
        "The archive's first-retained log position (purge floor).",
        s.cnc.archive_first_base().load_acquire(),
    );

    let commit_lag = append.saturating_sub(commit);
    let apply_lag = commit.saturating_sub(service_applied);
    push_gauge(
        &mut out,
        "uc2_commit_lag_bytes",
        "append - commit: bytes appended locally but not yet cluster-committed.",
        commit_lag,
    );
    push_gauge(
        &mut out,
        "uc2_apply_lag_bytes",
        "commit - service_applied: bytes committed but not yet applied.",
        apply_lag,
    );

    let admission_saturation = if admission_bytes == 0 {
        0.0
    } else {
        commit_lag as f64 / admission_bytes as f64
    };
    push_gauge_f64(
        &mut out,
        "uc2_admission_saturation",
        "commit_lag / admission_bytes; how full the admission window is.",
        admission_saturation,
    );

    let node_hb = status.node_heartbeat_ns.load_acquire();
    push_gauge_f64(
        &mut out,
        "uc_node_heartbeat_age_seconds",
        "Seconds since this node's own heartbeat was last stamped (a never-written heartbeat reads as a huge age, by design).",
        now.saturating_sub(node_hb) as f64 / 1e9,
    );

    let mut reported_samples = Vec::new();
    let mut lag_samples = Vec::new();
    let mut limit_samples = Vec::new();
    for i in 0..CNC_MAX_PEER_SLOTS {
        let slot = s.cnc.peer_slot(i);
        let id_and_role = slot.id_and_role.load_acquire();
        if id_and_role == 0 {
            continue; // unoccupied slot
        }
        let peer_id = id_and_role >> 8;
        let role_bits = (id_and_role & 0xFF) as u8;
        let role = match role_bits {
            CNC_PEER_ROLE_VOTER => "voter",
            CNC_PEER_ROLE_LEARNER => "learner",
            _ => "unknown",
        };
        let labels = format!("peer=\"{peer_id}\",role=\"{role}\"");
        let reported_durable = slot.reported_durable.load_acquire();
        reported_samples.push((labels.clone(), reported_durable));
        lag_samples.push((labels.clone(), commit.saturating_sub(reported_durable)));
        limit_samples.push((labels, slot.advertised_limit.load_acquire()));
    }
    push_labeled(
        &mut out,
        "uc2_peer_reported_durable_bytes",
        "Newest durable position this peer last reported.",
        "gauge",
        &reported_samples,
    );
    push_labeled(
        &mut out,
        "uc2_peer_replication_lag_bytes",
        "commit - peer's reported_durable.",
        "gauge",
        &lag_samples,
    );
    push_labeled(
        &mut out,
        "uc2_peer_advertised_limit_bytes",
        "The receive window this peer last advertised.",
        "gauge",
        &limit_samples,
    );

    push_counter(
        &mut out,
        "uc2_truncations_total",
        "Times this node truncated its own uncommitted log tail.",
        s.truncations.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_wipes_total",
        "Times this node wiped and rejoined (NoCommonPrefix).",
        s.wipes.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_ingress_holes_skipped_total",
        "Claims abandoned on the client INGRESS ring (submits): a producer that died or stalled past the hole timeout between its claim and its commit.",
        s.cnc.ingress_holes_skipped(),
    );
    push_counter(
        &mut out,
        "uc2_query_holes_skipped_total",
        "Claims abandoned on the client QUERY ring (reads): a producer that died or stalled past the hole timeout between its claim and its commit.",
        s.cnc.query_holes_skipped(),
    );
    push_counter(
        &mut out,
        "uc2_reports_unattested_total",
        "Durable reports declined for lacking a wire-0.5.0 term attestation.",
        s.reports_unattested.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_snapshot_refused_legacy_peer_total",
        "Snapshot sessions refused because the sender's SNAP_BEGIN was a pre-0.7.0 body (too short, or layout ≤ 1): the fleet is mixed-version; upgrade every node (spec §14.3).",
        s.receiver.snap_refused_legacy_peer.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_snapshot_refused_declared_set_total",
        "Snapshot sessions refused because the sender's row names/identity (positional mismatch) differ from this node's declared set — a joiner is stuck until the sets match (spec §8).",
        s.receiver
            .snap_refused_declared_mismatch
            .load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_snapshot_refused_version_total",
        "Snapshot sessions refused because the sender declared a different version than this node's attached service for at least one row — a joiner is stuck until every node runs the same version per row (spec §4.5, §9).",
        s.receiver
            .snap_refused_version_mismatch
            .load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_snapshot_refused_position_total",
        "Snapshot sessions refused because the sender's SNAP_BEGINs disagreed about the set's position — a set is the artifacts at ONE instant, and a source that mixes two would install a cluster image and a row image taken at different points of the log (coordinated-snapshot spec §5.6).",
        s.receiver
            .snap_refused_position_mismatch
            .load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_snapshot_refused_fetch_expired_total",
        "Snapshot sessions refused because they answered a `snapshot fetch` this node had already given up on: the set is neither stored nor installed, and the operator's verb is re-runnable (coordinated-snapshot spec §5.7, Ruling P11).",
        s.receiver
            .snap_refused_fetch_expired
            .load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_snapshot_intake_io_failures_total",
        "Local I/O failures on the snapshot INTAKE path: a `.part` that could not be created/sized or written to, or a completed artifact whose fsync/rename failed. Retried, but a persistent count means this node's snapshot dir is full, read-only, or obstructed (spec §14.3). Since 2.8.1 a failed publish is retried at most once per 250 ms per transfer — on the duty cycle AND on the chunk path — so a standing obstacle makes this climb at about four per second, not at the poll or chunk rate.",
        s.receiver.snap_intake_io_failures.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_snapshot_intake_abandoned_total",
        "Inbound snapshot transfers abandoned after 60 s with no chunk: the transfer's unfinished .part files are removed (an artifact it had already published stays, to be adopted or superseded by the next session) and this node keeps NAKing for a fresh session. A rising count means transfers never finish — on its own, look at the leader or the link. But if uc2_snapshot_intake_io_failures_total is rising at the same time the cause is local — this node's snapshot directory — and the set is being re-downloaded on a ~60 s loop until it is cleared (M14c2).",
        s.receiver.snap_intake_abandoned.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_snapshot_begin_undecodable_total",
        "Snapshot SESSIONS refused because their SNAP_BEGIN body could not be decoded at all — the realistic wire-0.5.0 flag-day shape. Counted once per session (uc2_snapshot_refused_legacy_peer_total counts every datagram, and the leader re-sends one every 20 ms). Nonzero means the fleet is mixed-version; upgrade every node (M14c2).",
        s.receiver.snap_begin_undecodable.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_reports_implausible_total",
        "Durable reports declined for disagreeing with this node's term map.",
        s.reports_implausible.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc_crypto_handshake_failures_total",
        "M8 Noise IK handshake failures.",
        s.crypto_handshake_failures.load(Ordering::Relaxed),
    );

    push_counter(
        &mut out,
        "uc2_sender_seal_failures_total",
        "Outgoing datagrams the sender could not seal (M8).",
        s.sender.seal_failures.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_receiver_seal_failures_total",
        "Outgoing datagrams the receiver could not seal (M8).",
        s.receiver.seal_failures.load(Ordering::Relaxed),
    );

    push_counter(
        &mut out,
        "uc2_unknown_source_datagrams_total",
        "Sealed datagrams from a SocketAddr with no known peer id.",
        s.receiver.dropped_unknown_peer.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_cleartext_peer_datagrams_total",
        "Datagrams that look like the flag-day cleartext-vs-crypto mismatch.",
        s.receiver.peer_appears_cleartext.load(Ordering::Relaxed),
    );

    push_counter(
        &mut out,
        "uc2_naks_sent_total",
        "NAKs this follower sent requesting repair.",
        s.receiver.naks_sent.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_naks_served_total",
        "NAKs this sender served from the ring.",
        s.sender.naks_served.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_naks_dropped_total",
        "NAK requests dropped because the queue was full.",
        s.sender.naks_dropped.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_naks_rejected_total",
        "NAK positions rejected as not a frame boundary.",
        s.sender.naks_rejected.load(Ordering::Relaxed),
    );

    push_counter(
        &mut out,
        "uc2_replay_datagrams_total",
        "DATA datagrams retransmitted from the journal (deep NAK).",
        s.sender.replay_datagrams.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_flow_stalls_total",
        "Times the sender stalled on quorum-paced flow control.",
        s.sender.flow_stalls.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_overruns_total",
        "NAKs unservable from both ring and journal.",
        s.sender.overruns.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_heartbeats_sent_total",
        "HEARTBEAT datagrams sent.",
        s.sender.heartbeats.load(Ordering::Relaxed),
    );

    push_counter(
        &mut out,
        "uc2_sender_datagrams_total",
        "Total datagrams sent by this node's sender agent.",
        s.sender.datagrams.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_sender_bytes_total",
        "Total bytes sent by this node's sender agent.",
        s.sender.bytes.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_receiver_datagrams_total",
        "Total datagrams received by this node's receiver agent.",
        s.receiver.datagrams.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_receiver_bytes_total",
        "Total bytes received by this node's receiver agent.",
        s.receiver.bytes.load(Ordering::Relaxed),
    );

    push_counter(
        &mut out,
        "uc2_snapshot_sessions_total",
        "Snapshot sessions opened (below-floor NAK upgrades).",
        s.sender.snap_sessions.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_snapshot_chunks_total",
        "SNAP_CHUNK datagrams sent.",
        s.sender.snap_chunks.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_snapshot_chunk_naks_total",
        "SNAP_CHUNK datagrams sent to repair a peer NAK.",
        s.sender.snap_chunk_naks.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_snapshot_open_failed_total",
        "Snapshot sessions this node refused to open because an artifact file could not be opened (the store listed it, then it was purged, made unreadable, or replaced). The peer's below-floor NAK stays a counted overrun and is re-sent, so a transient race self-heals; a persistent count means this node's OWN snapshot directory is bad while a peer is trying to join (M14c2).",
        s.sender.snap_open_failed.load(Ordering::Relaxed),
    );

    let dropped_samples: Vec<(String, u64)> = [
        (
            "stale_term",
            s.receiver.dropped_stale_term.load(Ordering::Relaxed),
        ),
        ("dup", s.receiver.dropped_dup.load(Ordering::Relaxed)),
        (
            "overrun",
            s.receiver.dropped_overrun.load(Ordering::Relaxed),
        ),
        (
            "malformed",
            s.receiver.dropped_malformed.load(Ordering::Relaxed),
        ),
        ("gated", s.receiver.dropped_gated.load(Ordering::Relaxed)),
        (
            "straddle",
            s.receiver.dropped_straddle.load(Ordering::Relaxed),
        ),
        (
            "auth_failed",
            s.receiver.dropped_auth_failed.load(Ordering::Relaxed),
        ),
        ("replay", s.receiver.dropped_replay.load(Ordering::Relaxed)),
        (
            "unknown_epoch",
            s.receiver.dropped_unknown_epoch.load(Ordering::Relaxed),
        ),
        (
            "unknown_peer",
            s.receiver.dropped_unknown_peer.load(Ordering::Relaxed),
        ),
        (
            "handshake",
            s.receiver.dropped_handshake.load(Ordering::Relaxed),
        ),
    ]
    .into_iter()
    .map(|(reason, v)| (format!("reason=\"{reason}\""), v))
    .collect();
    push_labeled(
        &mut out,
        "uc2_receiver_dropped_total",
        "DATA datagrams dropped by the receiver, by reason.",
        "counter",
        &dropped_samples,
    );

    push_counter(
        &mut out,
        "uc2_truncation_resyncs_total",
        "Times the receiver rebuilt its gap tracker after a reconciliation truncation.",
        s.receiver.truncation_resyncs.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_term_change_discards_total",
        "Times the receiver discarded out-of-order runs on a term change.",
        s.receiver.term_change_discards.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_counter_ahead_resyncs_total",
        "Times the receive frontier was rebased up to a counter that moved ahead of it.",
        s.receiver.counter_ahead_resyncs.load(Ordering::Relaxed),
    );

    let net_event_drops: u64 = s
        .receiver
        .net_drops
        .iter()
        .map(|c| c.load(Ordering::Relaxed))
        .sum();
    push_counter(
        &mut out,
        "uc_net_event_drops_total",
        "Consensus events dropped because the route channel was full, summed across kinds.",
        net_event_drops,
    );

    out
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use uc_log::cnc::{CncMeta, CncPage, pack_id_and_role, pack_service_status};
    use uc_protocol::identity::FsmName;
    use uc_protocol::v2::cnc::CNC_PEER_ROLE_VOTER;

    use super::*;
    use uc_net::receiver::FollowerStats;
    use uc_net::sender::SenderStats;

    /// A PUBLISHED cluster view (cluster-FSM spec §9) with both position
    /// words non-zero, so `uc2_cluster_fsm_position` and
    /// `uc2_settings_position` render real samples in the fixture rather
    /// than a vacuous `0` that any bug would also produce.
    fn test_cluster_view() -> Arc<crate::cluster_fsm::ClusterView> {
        let st = crate::cluster_fsm::ClusterState {
            settings_position: 2048,
            applied: 4096,
            ..crate::cluster_fsm::ClusterState::genesis_empty()
        };
        Arc::new(crate::cluster_fsm::ClusterView::new(&st))
    }

    fn synthetic_sources() -> ObsSources {
        let meta = CncMeta {
            node_id: 7,
            instance_id: 0x1122_3344_5566_7788,
            app_id: "test-app".into(),
            buffer_bytes: 1 << 20,
            max_payload: 1200,
            services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
        };
        let cnc = CncPage::heap(&meta);
        // Give the base fixture a non-unknown leader hint and one occupied
        // peer slot, so `every_contract_series_is_present` sees every
        // family (both `uc2_leader_hint` and the three `uc2_peer_*`
        // families are conditionally emitted).
        cnc.status().leader_hint.store_release(0);
        cnc.peer_slot(0)
            .id_and_role
            .store_release(pack_id_and_role(9, CNC_PEER_ROLE_VOTER));
        // M11 (Task 5): non-zero so `every_contract_series_is_present` sees
        // `uc2_free_disk_bytes` — it's conditionally emitted, same as
        // `uc2_leader_hint` above.
        cnc.store_free_disk_bytes(1);
        // M14c: declare FSM 0 and a lag bound so the per-service families
        // render at least one LABELED sample in the base fixture —
        // `every_contract_series_is_present` would otherwise be satisfied by
        // the bare family header alone (see `series_present`'s own doc on
        // why vacuous presence checks are the hazard here).
        cnc.store_services_declared(0b1);
        cnc.store_fsm_lag_bytes(1 << 20);

        ObsSources {
            node_id: 7,
            cnc,
            sender: Arc::new(SenderStats::default()),
            receiver: Arc::new(FollowerStats::default()),
            truncations: Arc::new(AtomicU64::new(0)),
            wipes: Arc::new(AtomicU64::new(0)),
            timer_stats: Arc::new(crate::timers::TimerStats::default()),
            schedule_table_position: Arc::new(AtomicU64::new(0)),
            schedule_entries: Arc::new(AtomicU64::new(0)),
            log_clock_smear_ns: Arc::new(AtomicU64::new(0)),
            schedule_apply_refused: Arc::new(AtomicU64::new(0)),
            cluster_view: test_cluster_view(),
            probe: uc_net::probe::ProbeTable::new(uc_net::probe::ProbeCadence::default()),
            commands_over_standard: Arc::new(AtomicU64::new(0)),
            reports_unattested: Arc::new(AtomicU64::new(0)),
            reports_implausible: Arc::new(AtomicU64::new(0)),
            crypto_handshake_failures: Arc::new(AtomicU64::new(0)),
            snapshot_instant_position: Arc::new(AtomicU64::new(0)),
            snapshot_standby_instant_position: Arc::new(AtomicU64::new(0)),
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
            jumbo_gate_pending: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Cluster FSM (spec §9): the cluster row's own two gauges, and the
    /// FIFTH `uc2_agent_alive` sample. `uc2_cluster_fsm_position` is the
    /// view's position word (the FSM's `applied`) and `uc2_settings_position`
    /// the frame-END of the last Settings command it applied — both read
    /// straight off the view's atomics AT SCRAPE TIME, never published into
    /// the consensus pass (M14a's lesson about a hot loop's body).
    #[test]
    fn the_cluster_gauges_and_the_fifth_agent_sample_are_exported() {
        let s = synthetic_sources();
        let text = render_prometheus(&s);
        assert!(
            series_present(&text, "uc2_cluster_fsm_position"),
            "missing uc2_cluster_fsm_position: {text}"
        );
        assert!(
            series_present(&text, "uc2_settings_position"),
            "missing uc2_settings_position: {text}"
        );
        assert!(
            text.contains("\nuc2_cluster_fsm_position 4096\n"),
            "the view's position word, verbatim: {text}"
        );
        assert!(
            text.contains("\nuc2_settings_position 2048\n"),
            "the view's settings position, verbatim: {text}"
        );
        // The four M10 agents plus `uc2-cluster` — every sample alive (the
        // fixture's finished flags are all false).
        for agent in ["consensus", "sender", "receiver", "archive", "cluster"] {
            assert!(
                text.contains(&format!("uc2_agent_alive{{agent=\"{agent}\"}} 1")),
                "missing the {agent} agent sample: {text}"
            );
        }
    }

    #[test]
    fn log_clock_smear_gauge_renders_the_published_value() {
        let s = synthetic_sources();
        s.log_clock_smear_ns.store(123_456, Ordering::Relaxed);
        let text = render_prometheus(&s);
        assert!(
            text.contains("\n# TYPE uc2_log_clock_smear_ns gauge\n"),
            "{text}"
        );
        assert!(text.contains("\nuc2_log_clock_smear_ns 123456\n"), "{text}");
        assert!(CONTRACT_SERIES.contains(&"uc2_log_clock_smear_ns"));
    }

    /// Final wave M5 (parked as T8): [`SnapshotFreezeStats`]'s edge detection
    /// is subtle enough that a regression would only show up on a fleet run —
    /// the reset-on-instant-advance, the no-double-count-on-equal-values
    /// blind spot, and the ordering rule that a freeze landing in the SAME
    /// scrape as an instant advance must fold into the FRESH max.
    ///
    /// Driven directly rather than through `render_prometheus` so each rule is
    /// asserted on its own, in the order the exporter calls them.
    #[test]
    fn snapshot_freeze_stats_resets_per_instant_and_never_double_counts_a_value() {
        let st = SnapshotFreezeStats::default();

        // Scrape 1: first reading for row 0 — one observation.
        st.observe_instant(4096);
        assert_eq!(st.observe_row(0, 500), (500, 500, 1));
        // ...and row 1 is independent.
        assert_eq!(st.observe_row(1, 900), (900, 900, 1));

        // Scrape 2, same instant, IDENTICAL word: the known blind spot —
        // nothing folds, and the max is held (not reset: the instant has not
        // moved).
        st.observe_instant(4096);
        assert_eq!(
            st.observe_row(0, 500),
            (500, 500, 1),
            "an unchanged cnc word is not a new freeze"
        );

        // Scrape 3, same instant, a SHORTER freeze: counted, summed, and the
        // running max is unchanged because it is a max.
        st.observe_instant(4096);
        assert_eq!(st.observe_row(0, 100), (500, 600, 2));

        // Scrape 4: the instant advances. Every row's max resets — and,
        // crucially, `observe_instant` runs BEFORE the rows, so a freeze read
        // in this same scrape folds into the fresh max rather than a stale
        // one. `_sum`/`_count` are counters and never reset.
        st.observe_instant(8192);
        assert_eq!(
            st.observe_row(0, 300),
            (300, 900, 3),
            "the new max is this scrape's reading, not the old 500"
        );
        assert_eq!(
            st.observe_row(1, 900),
            (0, 900, 1),
            "row 1's max reset too, and its unchanged word folded nothing"
        );

        // A gauge that goes BACKWARDS still resets (`changes`, not `>`): the
        // standby gauge can be lower than a stale full-instant reading, and
        // the exporter feeds the max of the two.
        st.observe_instant(4096);
        assert_eq!(st.observe_row(0, 300), (0, 900, 3), "max reset, word same");
    }

    /// P5: `monitor-a-cluster.md` states the contract's SIZE — "89 families"
    /// — and nothing made that number a build-time fact, so adding a series
    /// silently made the doc wrong. This pins the count; a deliberate change
    /// updates both together.
    #[test]
    fn the_contract_has_the_number_of_families_the_docs_state() {
        assert_eq!(
            CONTRACT_SERIES.len(),
            107,
            "if this is intentional, update the family count in \
             docs/how-to/monitor-a-cluster.md in the same commit"
        );
    }

    /// Time-and-timers gate row c: both histograms render the standard
    /// Prometheus shape — one `_bucket` line per `le` bound plus `+Inf`,
    /// CUMULATIVE counts, `_sum`, `_count`, and the companion `_max` gauge.
    /// The per-row family carries the same labels the counters beside it do;
    /// the node-level one carries only `le`.
    #[test]
    fn the_two_ns_histograms_render_cumulative_buckets_sum_count_and_max() {
        let s = synthetic_sources();
        // Row 0 is the declared row in `synthetic_sources`.
        s.timer_stats.lateness_ns[0].observe(5_000); // le=10000
        s.timer_stats.lateness_ns[0].observe(30_000); // le=50000
        s.timer_stats.lateness_ns[0].observe(300_000_000); // +Inf only
        s.timer_stats.pass_ns.observe(120_000); // le=200000
        let text = render_prometheus(&s);

        assert!(
            text.contains("\n# TYPE uc2_timer_lateness_ns histogram\n"),
            "{text}"
        );
        let l = "service=\"\",row=\"0\"";
        for (bound, want) in [
            ("100", 0),
            ("1000", 0),
            ("5000", 1),
            ("10000", 1),
            ("20000", 1),
            ("50000", 2),
            ("100000000", 2),
            ("+Inf", 3),
        ] {
            let line = format!("uc2_timer_lateness_ns_bucket{{{l},le=\"{bound}\"}} {want}\n");
            assert!(text.contains(&line), "missing {line:?} in\n{text}");
        }
        assert!(text.contains(&format!(
            "uc2_timer_lateness_ns_sum{{{l}}} {}\n",
            5_000 + 30_000 + 300_000_000u64
        )));
        assert!(text.contains(&format!("uc2_timer_lateness_ns_count{{{l}}} 3\n")));
        assert!(text.contains(&format!("uc2_timer_lateness_ns_max{{{l}}} 300000000\n")));

        assert!(
            text.contains("\n# TYPE uc2_consensus_pass_ns histogram\n"),
            "{text}"
        );
        assert!(
            text.contains("uc2_consensus_pass_ns_bucket{le=\"100000\"} 0\n"),
            "{text}"
        );
        assert!(
            text.contains("uc2_consensus_pass_ns_bucket{le=\"200000\"} 1\n"),
            "{text}"
        );
        assert!(
            text.contains("uc2_consensus_pass_ns_bucket{le=\"+Inf\"} 1\n"),
            "{text}"
        );
        assert!(
            text.contains("uc2_consensus_pass_ns_sum 120000\n"),
            "{text}"
        );
        assert!(text.contains("uc2_consensus_pass_ns_count 1\n"), "{text}");
        assert!(
            text.contains("uc2_consensus_pass_ns_max 120000\n"),
            "{text}"
        );

        // Every bucket line for a family sits under exactly ONE header pair.
        assert_eq!(text.matches("# TYPE uc2_timer_lateness_ns ").count(), 1);
        assert_eq!(text.matches("# TYPE uc2_consensus_pass_ns ").count(), 1);
    }

    /// A node that has never fired a timer (or never led) still renders both
    /// families, all zeroes — an absent series cannot be graphed, and the
    /// gate's row c reads `_count` to decide whether it has samples at all.
    #[test]
    fn the_two_ns_histograms_render_at_zero_before_any_observation() {
        let text = render_prometheus(&synthetic_sources());
        assert!(text.contains("uc2_consensus_pass_ns_count 0\n"), "{text}");
        assert!(
            text.contains("uc2_timer_lateness_ns_count{service=\"\",row=\"0\"} 0\n"),
            "{text}"
        );
    }

    #[test]
    fn every_contract_series_is_present() {
        let s = synthetic_sources();
        let text = render_prometheus(&s);
        for name in CONTRACT_SERIES {
            assert!(series_present(&text, name), "missing series {name}");
        }
    }

    /// Review fix round 1: `synthetic_sources()` always declares FSM 0, so
    /// nothing else in this module exercises `services_declared == 0` — the
    /// exact shape of a real M14a-unaware / no-`[services]` harness page.
    /// Must not panic, and every per-FSM family still renders its header
    /// (and, for the four aggregates, its bare unlabelled sample off slot 0,
    /// which is always valid memory even when never declared) with zero
    /// `service="..."` samples anywhere.
    #[test]
    fn a_harness_page_with_nothing_declared_renders_headers_and_no_labelled_samples() {
        let s = synthetic_sources();
        s.cnc.store_services_declared(0);
        let text = render_prometheus(&s);
        for name in [
            "uc_service_applied_bytes",
            "uc_service_epoch",
            "uc_service_snapshot_pos_bytes",
            "uc_service_heartbeat_age_seconds",
            "uc_service_attached",
            "uc_service_lag_bytes",
            "uc_service_lag_waits_total",
            "uc_services_declared",
            "uc2_fsm_lag_bytes",
        ] {
            assert_eq!(
                text.matches(&format!("# TYPE {name} ")).count(),
                1,
                "exactly one family header for {name}: {text}"
            );
        }
        assert!(!text.contains(r#"service=""#), "no id is declared: {text}");
        assert!(text.contains("\nuc_service_applied_bytes 0\n"), "{text}");
        assert!(text.contains("\nuc_service_epoch 0\n"), "{text}");
        assert!(
            text.contains("\nuc_service_snapshot_pos_bytes 0\n"),
            "{text}"
        );
        // M14c2 T10b: this used to be
        // `contains("uc_service_heartbeat_age_seconds ")`, which the family's
        // OWN `# HELP` and `# TYPE` lines satisfy — a header-only render passed
        // it, i.e. the assertion could not fail for the thing it was checking.
        // Match the bare unlabelled SAMPLE line at a line boundary and read its
        // VALUE, so headers alone are a failure. (The value is an age in
        // seconds against the wall clock, so it is asserted as a well-formed
        // non-negative number, not as a constant.)
        let hb = text
            .lines()
            .find(|l| l.starts_with("uc_service_heartbeat_age_seconds "))
            .unwrap_or_else(|| {
                panic!("no bare heartbeat-age SAMPLE line — headers are not a sample: {text}")
            });
        let age: f64 = hb["uc_service_heartbeat_age_seconds ".len()..]
            .trim()
            .parse()
            .unwrap_or_else(|e| panic!("heartbeat age must render as a number ({hb:?}): {e}"));
        assert!(
            age.is_finite() && age >= 0.0,
            "heartbeat age must be a real age: {hb:?}"
        );
        assert!(text.contains("\nuc_services_declared 0\n"), "{text}");
    }

    /// Boundary-matched family presence: `\n{name} ` (a bare gauge/counter
    /// value line), `\n{name}{{` (a labeled sample line), or
    /// `\n# TYPE {name} ` (the family header, unconditionally emitted
    /// whenever the family renders at all, including a labeled family with
    /// zero occupied samples). A plain substring check is live-vacuous-able:
    /// `uc2_term` is a prefix of `uc2_term_change_discards_total`, so
    /// `contains("\n{name}")` alone would pass even if `uc2_term` itself
    /// never rendered.
    fn series_present(text: &str, name: &str) -> bool {
        text.contains(&format!("\n{name} "))
            || text.contains(&format!("\n{name}{{"))
            || text.contains(&format!("\n# TYPE {name} "))
    }

    /// Final review, Important 2: TWO series, one per client-facing ring —
    /// not one aggregate. The independence is the point: an operator must be
    /// able to tell a losing submit path from a losing read path.
    #[test]
    fn ingress_and_query_holes_skipped_are_two_independent_series() {
        let s = synthetic_sources();
        assert!(render_prometheus(&s).contains("uc2_ingress_holes_skipped_total 0"));
        assert!(render_prometheus(&s).contains("uc2_query_holes_skipped_total 0"));
        s.cnc.store_ingress_holes_skipped(2);
        s.cnc.store_query_holes_skipped(5);
        let text = render_prometheus(&s);
        assert!(text.contains("uc2_ingress_holes_skipped_total 2"), "{text}");
        assert!(text.contains("uc2_query_holes_skipped_total 5"), "{text}");
    }

    #[test]
    fn derived_lags_saturate_and_saturation_divides() {
        let s = synthetic_sources();
        s.cnc.counters().append.store_release(1_000_000);
        s.cnc.counters().commit.store_release(400_000);
        s.cnc.service().service_applied.store_release(150_000);
        s.cnc.store_admission_bytes(262_144);
        let text = render_prometheus(&s);
        assert!(text.contains("uc2_commit_lag_bytes 600000"), "{text}");
        assert!(text.contains("uc2_apply_lag_bytes 250000"), "{text}");
        assert!(
            text.contains("uc2_admission_saturation 2.288818359375"),
            "{text}"
        );
    }

    #[test]
    fn peer_slots_export_only_occupied_with_labels() {
        let s = synthetic_sources();
        s.cnc
            .peer_slot(0)
            .id_and_role
            .store_release(pack_id_and_role(2, CNC_PEER_ROLE_VOTER));
        s.cnc.peer_slot(0).reported_durable.store_release(1234);
        let text = render_prometheus(&s);
        assert!(
            text.contains(r#"uc2_peer_reported_durable_bytes{peer="2",role="voter"} 1234"#),
            "{text}"
        );
        assert!(
            !text.contains(r#"peer="0""#),
            "unoccupied slots must not appear: {text}"
        );
    }

    /// M14c (spec §9): one labelled sample per DECLARED id — including an id
    /// nothing has ever attached to, which must read `attached 0` rather
    /// than vanish (an absent series is not alertable).
    #[test]
    fn per_fsm_families_render_one_sample_per_declared_id() {
        let s = synthetic_sources();
        s.cnc.store_services_declared(0b101); // ids 0 and 2; id 1 NOT declared
        s.cnc.store_fsm_lag_bytes(65_536);
        s.cnc.counters().commit.store_release(10_000);
        let s0 = s.cnc.service_slot(0);
        s0.status.store_release(pack_service_status(0, true, 3));
        s0.applied.store_release(9_000);
        s0.epoch.store_release(7);
        s0.snapshot_pos.store_release(4_096);
        s0.lag_waits.store_release(12);
        // id 2: declared, never attached — every field stays zero.
        let text = render_prometheus(&s);
        assert!(
            text.contains(r#"uc_service_applied_bytes{service="",row="0"} 9000"#),
            "{text}"
        );
        assert!(
            text.contains(r#"uc_service_epoch{service="",row="0"} 7"#),
            "{text}"
        );
        assert!(
            text.contains(r#"uc_service_snapshot_pos_bytes{service="",row="0"} 4096"#),
            "{text}"
        );
        assert!(
            text.contains(r#"uc_service_attached{service="",row="0"} 1"#),
            "{text}"
        );
        assert!(
            text.contains(r#"uc_service_attached{service="",row="2"} 0"#),
            "{text}"
        );
        assert!(
            text.contains(r#"uc_service_lag_bytes{service="",row="0"} 1000"#),
            "{text}"
        );
        assert!(
            text.contains(r#"uc_service_lag_bytes{service="",row="2"} 10000"#),
            "{text}"
        );
        assert!(
            text.contains(r#"uc_service_lag_waits_total{service="",row="0"} 12"#),
            "{text}"
        );
        assert!(
            text.contains(r#"uc_service_heartbeat_age_seconds{service="",row="2"}"#),
            "{text}"
        );
        assert!(text.contains("\nuc_services_declared 5\n"), "{text}");
        assert!(text.contains("\nuc2_fsm_lag_bytes 65536\n"), "{text}");
        assert!(!text.contains(r#"row="1""#), "id 1 is not declared: {text}");
    }

    /// The four M10 aggregates keep their bare names (now "slowest FSM") and
    /// share ONE family header with their labelled twins — two `# HELP`
    /// lines for one family is a scrape Prometheus rejects.
    #[test]
    fn the_aggregates_keep_their_bare_names_in_one_family_block() {
        let s = synthetic_sources();
        s.cnc.store_services_declared(0b11);
        s.cnc.service().service_applied.store_release(1_234);
        let text = render_prometheus(&s);
        assert!(text.contains("\nuc_service_applied_bytes 1234\n"), "{text}");
        for name in [
            "uc_service_applied_bytes",
            "uc_service_epoch",
            "uc_service_snapshot_pos_bytes",
            "uc_service_heartbeat_age_seconds",
        ] {
            assert_eq!(
                text.matches(&format!("# TYPE {name} ")).count(),
                1,
                "exactly one family header for {name}: {text}"
            );
        }
    }

    /// `commit - applied` saturates: a slot that reports past this scrape's
    /// commit sample (two independent atomics, read microseconds apart)
    /// reads 0, never a wrapped 18-exabyte lag.
    #[test]
    fn service_lag_bytes_saturates_when_applied_is_past_commit() {
        let s = synthetic_sources();
        s.cnc.store_services_declared(0b1);
        s.cnc.counters().commit.store_release(500);
        s.cnc.service_slot(0).applied.store_release(900);
        assert!(
            render_prometheus(&s).contains(r#"uc_service_lag_bytes{service="",row="0"} 0"#),
            "{}",
            render_prometheus(&s)
        );
    }

    /// M14c (spec §14.3): the two named snapshot-session refusals render as
    /// counters off the receiver stats the node already shares with /metrics.
    #[test]
    fn snapshot_refusal_counters_render_from_receiver_stats() {
        let s = synthetic_sources();
        s.receiver
            .snap_refused_legacy_peer
            .fetch_add(3, Ordering::Relaxed);
        s.receiver
            .snap_refused_declared_mismatch
            .fetch_add(1, Ordering::Relaxed);
        s.receiver
            .snap_refused_version_mismatch
            .fetch_add(1, Ordering::Relaxed);
        let text = render_prometheus(&s);
        assert!(
            text.contains("uc2_snapshot_refused_legacy_peer_total 3\n"),
            "{text}"
        );
        assert!(
            text.contains("uc2_snapshot_refused_declared_set_total 1\n"),
            "{text}"
        );
        assert!(
            text.contains("uc2_snapshot_refused_version_total 1\n"),
            "{text}"
        );
        // Coordinated-snapshot spec §5.6: and the fourth, the one-position
        // rule's — a source that mixed two instants; plus the fifth (Ruling
        // P11), a learner answering a fetch this node gave up on.
        s.receiver
            .snap_refused_position_mismatch
            .fetch_add(2, Ordering::Relaxed);
        s.receiver
            .snap_refused_fetch_expired
            .fetch_add(4, Ordering::Relaxed);
        let text = render_prometheus(&s);
        assert!(
            text.contains("uc2_snapshot_refused_position_total 2\n"),
            "{text}"
        );
        assert!(
            text.contains("uc2_snapshot_refused_fetch_expired_total 4\n"),
            "{text}"
        );
    }

    /// Task 7 (spec §4.5): a service is found by NAME, so per-FSM labels
    /// carry `service="<name>"` alongside the row's positional `row="<r>"`
    /// — and the identity-hash / version gauges render alongside the
    /// existing per-FSM families.
    #[test]
    fn per_fsm_labels_carry_name_and_the_identity_version_gauges_render() {
        let kv = FsmName::parse("kv").unwrap();
        let orders = FsmName::parse("orders").unwrap();
        let mut services = [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES];
        services[0] = Some(kv);
        services[1] = Some(orders);
        let meta = CncMeta {
            node_id: 7,
            instance_id: 0x1122_3344_5566_7788,
            app_id: "test-app".into(),
            buffer_bytes: 1 << 20,
            max_payload: 1200,
            services,
        };
        let cnc = CncPage::heap(&meta);
        cnc.store_services_declared(0b11);
        cnc.service_slot(1).status.store_version(0x0102_0003);

        let s = ObsSources {
            node_id: 7,
            cnc,
            sender: Arc::new(SenderStats::default()),
            receiver: Arc::new(FollowerStats::default()),
            truncations: Arc::new(AtomicU64::new(0)),
            wipes: Arc::new(AtomicU64::new(0)),
            timer_stats: Arc::new(crate::timers::TimerStats::default()),
            schedule_table_position: Arc::new(AtomicU64::new(0)),
            schedule_entries: Arc::new(AtomicU64::new(0)),
            log_clock_smear_ns: Arc::new(AtomicU64::new(0)),
            schedule_apply_refused: Arc::new(AtomicU64::new(0)),
            cluster_view: test_cluster_view(),
            probe: uc_net::probe::ProbeTable::new(uc_net::probe::ProbeCadence::default()),
            commands_over_standard: Arc::new(AtomicU64::new(0)),
            reports_unattested: Arc::new(AtomicU64::new(0)),
            reports_implausible: Arc::new(AtomicU64::new(0)),
            crypto_handshake_failures: Arc::new(AtomicU64::new(0)),
            snapshot_instant_position: Arc::new(AtomicU64::new(0)),
            snapshot_standby_instant_position: Arc::new(AtomicU64::new(0)),
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
            jumbo_gate_pending: Arc::new(AtomicBool::new(false)),
        };

        let text = render_prometheus(&s);
        assert!(
            text.contains("uc_service_attached{service=\"orders\",row=\"1\"} 0\n"),
            "{text}"
        );
        assert!(
            text.contains(&format!(
                "uc2_service_identity_hash{{service=\"orders\",row=\"1\"}} {}\n",
                FsmName::parse("orders").unwrap().hash()
            )),
            "{text}"
        );
        assert!(
            text.contains("uc2_service_version{service=\"orders\",row=\"1\"} 16908291\n"),
            "{text}"
        );
        assert!(
            text.contains("# HELP uc2_snapshot_refused_version_total"),
            "{text}"
        );
        // Review fix: the HELP text must carry the REAL PromQL — the naive
        // `count by (row) (uc2_service_identity_hash) > 1` counts SERIES
        // (instances), not distinct VALUES, so it would page permanently on
        // any multi-node cluster. `count_values` is the operator that
        // actually groups by value; the shipped alert rule
        // (`packaging/prometheus/uc2-alerts.yml`) uses it and the HELP text
        // must match, not the naive form.
        assert!(
            text.contains(
                "count by (row) (count_values(\"hash\", uc2_service_identity_hash) by (row)) > 1"
            ),
            "{text}"
        );
        assert!(
            text.contains("float64"),
            "identity-hash HELP must disclose the wire-encoding precision note: {text}"
        );
        assert!(
            text.contains(
                "count by (row) (count_values(\"version\", uc2_service_version > 0) by (row)) > 1"
            ),
            "{text}"
        );
    }

    /// M14c review round 2, finding 1: the intake-side I/O counter — the
    /// receiver half of the same story ("the disk, not the wire").
    #[test]
    fn snapshot_intake_io_failures_render_from_receiver_stats() {
        let s = synthetic_sources();
        s.receiver
            .snap_intake_io_failures
            .fetch_add(4, Ordering::Relaxed);
        let text = render_prometheus(&s);
        assert!(
            text.contains("uc2_snapshot_intake_io_failures_total 4\n"),
            "{text}"
        );
    }

    /// M14c2 (T10a): the three counters that close M14c's snapshot-session
    /// deferrals. `every_contract_series_is_present` proves the families are
    /// EXPORTED; this proves each one is wired to the right stats cell —
    /// distinct values, so a copy-paste of the wrong field is caught.
    #[test]
    fn the_m14c2_snapshot_counters_render_from_their_own_stats_cells() {
        let s = synthetic_sources();
        s.sender.snap_open_failed.fetch_add(2, Ordering::Relaxed);
        s.receiver
            .snap_intake_abandoned
            .fetch_add(5, Ordering::Relaxed);
        s.receiver
            .snap_begin_undecodable
            .fetch_add(7, Ordering::Relaxed);
        let text = render_prometheus(&s);
        assert!(
            text.contains("uc2_snapshot_open_failed_total 2\n"),
            "{text}"
        );
        assert!(
            text.contains("uc2_snapshot_intake_abandoned_total 5\n"),
            "{text}"
        );
        assert!(
            text.contains("uc2_snapshot_begin_undecodable_total 7\n"),
            "{text}"
        );
    }

    #[test]
    fn leader_hint_is_omitted_when_unknown() {
        let s = synthetic_sources();
        s.cnc.status().leader_hint.store_release(u64::MAX);
        assert!(!render_prometheus(&s).contains("uc2_leader_hint"));
        s.cnc.status().leader_hint.store_release(1);
        assert!(render_prometheus(&s).contains("uc2_leader_hint 1"));
    }

    #[test]
    fn free_disk_omitted_when_zero() {
        let s = synthetic_sources();
        s.cnc.store_free_disk_bytes(0);
        assert!(!render_prometheus(&s).contains("uc2_free_disk_bytes"));
    }

    #[test]
    fn free_disk_present_when_stored() {
        let s = synthetic_sources();
        s.cnc.store_free_disk_bytes(123_456_789);
        assert!(render_prometheus(&s).contains("uc2_free_disk_bytes 123456789"));
    }

    #[test]
    fn a_dead_agent_reads_zero() {
        let s = synthetic_sources();
        s.agents[1].1.store(true, Ordering::Release);
        let text = render_prometheus(&s);
        assert!(
            text.contains(r#"uc2_agent_alive{agent="sender"} 0"#),
            "{text}"
        );
        assert!(
            text.contains(r#"uc2_agent_alive{agent="consensus"} 1"#),
            "{text}"
        );
    }
}
