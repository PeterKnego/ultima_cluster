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

use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use uc_protocol::v2::cnc::{
    CNC_MAX_PEER_SLOTS, CNC_PEER_ROLE_LEARNER, CNC_PEER_ROLE_VOTER, NODE_FLAG_CAN_SERVE,
    NODE_FLAG_LEADER,
};

use super::ObsSources;

/// Unix nanoseconds "now" — shared with Task 6's `/healthz`/`/readyz` probes.
pub fn now_unix_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

/// Every metric family name the encoder emits, in the order it renders
/// them. Task 8's alert rules reference only names from this list; Task
/// 10's coverage row scrapes a live `/metrics` and asserts every entry is
/// present.
pub const CONTRACT_SERIES: &[&str] = &[
    "uc2_build_info",
    "uc2_node_id",
    "uc2_is_leader",
    "uc2_can_serve",
    "uc2_term",
    "uc2_leader_hint",
    "uc2_config_version",
    "uc2_config_pending",
    "uc2_crypto_enabled",
    "uc2_purge_enabled",
    "uc2_admission_bytes",
    "uc2_journal_segment_bytes",
    "uc2_agent_alive",
    "uc2_append_bytes",
    "uc2_durable_bytes",
    "uc2_sent_bytes",
    "uc2_commit_bytes",
    "uc2_service_applied_bytes",
    "uc2_service_epoch",
    "uc2_output_completed_bytes",
    "uc2_output_progress_bytes",
    "uc2_service_snapshot_pos_bytes",
    "uc2_node_snapshot_floor_bytes",
    "uc2_incoming_snapshot_pos_bytes",
    "uc2_archive_first_base_bytes",
    "uc2_commit_lag_bytes",
    "uc2_apply_lag_bytes",
    "uc2_admission_saturation",
    "uc2_node_heartbeat_age_seconds",
    "uc2_service_heartbeat_age_seconds",
    "uc2_peer_reported_durable_bytes",
    "uc2_peer_replication_lag_bytes",
    "uc2_peer_advertised_limit_bytes",
    "uc2_truncations_total",
    "uc2_wipes_total",
    "uc2_reports_unattested_total",
    "uc2_reports_implausible_total",
    "uc2_crypto_handshake_failures_total",
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
    "uc2_net_event_drops_total",
];

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

/// Render the full M10 series contract over one snapshot of `s`'s `Arc`s
/// and atomics. Pure — no I/O, no allocation beyond the returned `String`.
pub fn render_prometheus(s: &ObsSources) -> String {
    let mut out = String::with_capacity(8 * 1024);

    push_family_header(&mut out, "uc2_build_info", "Static build version info.", "gauge");
    out.push_str(&format!("uc2_build_info{{version=\"{}\"}} 1\n", env!("CARGO_PKG_VERSION")));

    push_gauge(&mut out, "uc2_node_id", "This node's configured id.", s.node_id as u64);

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
    push_gauge(&mut out, "uc2_term", "Current consensus term.", status.term.load_acquire());

    let leader_hint = status.leader_hint.load_acquire();
    if leader_hint != u64::MAX {
        push_gauge(&mut out, "uc2_leader_hint", "Last known leader node id.", leader_hint);
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
        "uc2_crypto_enabled",
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
    push_gauge(&mut out, "uc2_append_bytes", "Local append counter.", append);
    push_gauge(&mut out, "uc2_durable_bytes", "Local archive-durable counter.", durable);
    push_gauge(&mut out, "uc2_sent_bytes", "Local replication-sent counter.", sent);
    push_gauge(&mut out, "uc2_commit_bytes", "Cluster commit counter.", commit);

    let service = s.cnc.service();
    let service_applied = service.service_applied.load_acquire();
    push_gauge(
        &mut out,
        "uc2_service_applied_bytes",
        "Position the service state machine has applied through.",
        service_applied,
    );
    push_gauge(
        &mut out,
        "uc2_service_epoch",
        "Service incarnation counter, bumped each service attach.",
        service.service_epoch.load_acquire(),
    );
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
        "uc2_service_snapshot_pos_bytes",
        "Position of the newest complete service-built snapshot (0 = none).",
        snapshots.service_snapshot_pos.load_acquire(),
    );
    push_gauge(
        &mut out,
        "uc2_node_snapshot_floor_bytes",
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

    let admission_saturation =
        if admission_bytes == 0 { 0.0 } else { commit_lag as f64 / admission_bytes as f64 };
    push_gauge_f64(
        &mut out,
        "uc2_admission_saturation",
        "commit_lag / admission_bytes; how full the admission window is.",
        admission_saturation,
    );

    let now = now_unix_ns();
    let node_hb = status.node_heartbeat_ns.load_acquire();
    let service_hb = status.service_heartbeat_ns.load_acquire();
    push_gauge_f64(
        &mut out,
        "uc2_node_heartbeat_age_seconds",
        "Seconds since this node's own heartbeat was last stamped (a never-written heartbeat reads as a huge age, by design).",
        now.saturating_sub(node_hb) as f64 / 1e9,
    );
    push_gauge_f64(
        &mut out,
        "uc2_service_heartbeat_age_seconds",
        "Seconds since the attached service's heartbeat was last stamped (a never-written heartbeat reads as a huge age, by design).",
        now.saturating_sub(service_hb) as f64 / 1e9,
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
        "uc2_reports_unattested_total",
        "Durable reports declined for lacking a wire-0.5.0 term attestation.",
        s.reports_unattested.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_reports_implausible_total",
        "Durable reports declined for disagreeing with this node's term map.",
        s.reports_implausible.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_crypto_handshake_failures_total",
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

    let dropped_samples: Vec<(String, u64)> = [
        ("stale_term", s.receiver.dropped_stale_term.load(Ordering::Relaxed)),
        ("dup", s.receiver.dropped_dup.load(Ordering::Relaxed)),
        ("overrun", s.receiver.dropped_overrun.load(Ordering::Relaxed)),
        ("malformed", s.receiver.dropped_malformed.load(Ordering::Relaxed)),
        ("gated", s.receiver.dropped_gated.load(Ordering::Relaxed)),
        ("straddle", s.receiver.dropped_straddle.load(Ordering::Relaxed)),
        ("auth_failed", s.receiver.dropped_auth_failed.load(Ordering::Relaxed)),
        ("replay", s.receiver.dropped_replay.load(Ordering::Relaxed)),
        ("unknown_epoch", s.receiver.dropped_unknown_epoch.load(Ordering::Relaxed)),
        ("unknown_peer", s.receiver.dropped_unknown_peer.load(Ordering::Relaxed)),
        ("handshake", s.receiver.dropped_handshake.load(Ordering::Relaxed)),
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

    let net_event_drops: u64 = s.receiver.net_drops.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    push_counter(
        &mut out,
        "uc2_net_event_drops_total",
        "Consensus events dropped because the route channel was full, summed across kinds.",
        net_event_drops,
    );

    out
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use uc2_log::cnc::{CncMeta, CncPage, pack_id_and_role};
    use uc_protocol::v2::cnc::CNC_PEER_ROLE_VOTER;

    use super::*;
    use uc2_net::receiver::FollowerStats;
    use uc2_net::sender::SenderStats;

    fn synthetic_sources() -> ObsSources {
        let meta = CncMeta {
            node_id: 7,
            instance_id: 0x1122_3344_5566_7788,
            app_id: "test-app".into(),
            buffer_bytes: 1 << 20,
            max_payload: 1200,
        };
        let cnc = CncPage::heap(&meta);
        // Give the base fixture a non-unknown leader hint and one occupied
        // peer slot, so `every_contract_series_is_present` sees every
        // family (both `uc2_leader_hint` and the three `uc2_peer_*`
        // families are conditionally emitted).
        cnc.status().leader_hint.store_release(0);
        cnc.peer_slot(0).id_and_role.store_release(pack_id_and_role(9, CNC_PEER_ROLE_VOTER));

        ObsSources {
            node_id: 7,
            cnc,
            sender: Arc::new(SenderStats::default()),
            receiver: Arc::new(FollowerStats::default()),
            truncations: Arc::new(AtomicU64::new(0)),
            wipes: Arc::new(AtomicU64::new(0)),
            reports_unattested: Arc::new(AtomicU64::new(0)),
            reports_implausible: Arc::new(AtomicU64::new(0)),
            crypto_handshake_failures: Arc::new(AtomicU64::new(0)),
            crypto_enabled: false,
            purge_enabled: false,
            journal_segment_bytes: 64 << 20,
            agents: vec![
                ("consensus", Arc::new(AtomicBool::new(false))),
                ("sender", Arc::new(AtomicBool::new(false))),
                ("receiver", Arc::new(AtomicBool::new(false))),
                ("archive", Arc::new(AtomicBool::new(false))),
            ],
        }
    }

    #[test]
    fn every_contract_series_is_present() {
        let s = synthetic_sources();
        let text = render_prometheus(&s);
        for name in CONTRACT_SERIES {
            assert!(
                text.contains(&format!("\n{name}")) || text.starts_with(name),
                "missing series {name}"
            );
        }
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
        assert!(text.contains("uc2_admission_saturation 2.288818359375"), "{text}");
    }

    #[test]
    fn peer_slots_export_only_occupied_with_labels() {
        let s = synthetic_sources();
        s.cnc.peer_slot(0).id_and_role.store_release(pack_id_and_role(2, CNC_PEER_ROLE_VOTER));
        s.cnc.peer_slot(0).reported_durable.store_release(1234);
        let text = render_prometheus(&s);
        assert!(
            text.contains(r#"uc2_peer_reported_durable_bytes{peer="2",role="voter"} 1234"#),
            "{text}"
        );
        assert!(!text.contains(r#"peer="0""#), "unoccupied slots must not appear: {text}");
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
    fn a_dead_agent_reads_zero() {
        let s = synthetic_sources();
        s.agents[1].1.store(true, Ordering::Release);
        let text = render_prometheus(&s);
        assert!(text.contains(r#"uc2_agent_alive{agent="sender"} 0"#), "{text}");
        assert!(text.contains(r#"uc2_agent_alive{agent="consensus"} 1"#), "{text}");
    }
}
