// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The gateway's `/metrics`, `/healthz` and `/readyz` (#173), served by the
//! `uc2-gateway` daemon on `[metrics] bind` over the same listener `uc_node`
//! uses ([`uc_obs::http`]) — not by depending on `uc_node`, which would pull
//! consensus, transport and crypto into a front-door process.
//!
//! - `/metrics` — one `uc2_gateway_<field>_total` counter per [`EdgeStats`]
//!   field, plus `uc2_gateway_grant_outstanding` against
//!   `uc2_gateway_grant_budget` (the M13 grant budget: the live sum of every
//!   ready connection's credits, and the ceiling that sum is held under).
//! - `/healthz` — 200 while the daemon's tick loop is alive (it stamps a
//!   heartbeat every tick; the listener answers 503 once that heartbeat is
//!   [`HEARTBEAT_STALE_NS`] old).
//! - `/readyz` — `/healthz`, AND the edge is still attached (not faulted),
//!   AND the node underneath is alive: its own cnc heartbeat is fresh. The
//!   edge cannot tell more than that — a follower's gateway is ready too,
//!   because answering `REDIRECT` is its job — and the heartbeat is the one
//!   sign it has of a node that died without its supervisor noticing (the
//!   page's `CAN_SERVE` bit stays set on a dead node).

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use uc_obs::http::{HttpServer, Response, get_router};
use uc_obs::prom::{push_counter, push_gauge};

use crate::{EdgeObs, EdgeStats};

/// The staleness bar for both heartbeats `/healthz`/`/readyz` read — the
/// same 3 s `uc_node`'s `/readyz` holds its own heartbeats to.
pub const HEARTBEAT_STALE_NS: u64 = 3_000_000_000;

/// Every family [`render`] emits, in render order.
pub const GATEWAY_SERIES: &[&str] = &[
    "uc2_gateway_connections_total",
    "uc2_gateway_submits_total",
    "uc2_gateway_queries_total",
    "uc2_gateway_responses_total",
    "uc2_gateway_redirects_total",
    "uc2_gateway_retries_total",
    "uc2_gateway_unknown_total",
    "uc2_gateway_backpressure_events_total",
    "uc2_gateway_leader_changes_total",
    "uc2_gateway_leader_changed_frames_total",
    "uc2_gateway_status_frames_total",
    "uc2_gateway_refused_busy_total",
    "uc2_gateway_grant_changes_total",
    "uc2_gateway_grant_outstanding",
    "uc2_gateway_grant_budget",
];

/// Unix nanoseconds now — `uc_obs::time`'s, re-exported for the daemon.
pub use uc_obs::time::now_unix_ns;

/// The Prometheus text body: pure, no I/O.
pub fn render(s: &EdgeStats, grant_outstanding: u64, grant_budget: u32) -> String {
    let mut out = String::with_capacity(4096);
    let counters: [(&str, &str, u64); 13] = [
        (
            "uc2_gateway_connections_total",
            "Client connections this edge accepted (including ones since closed).",
            s.connections,
        ),
        (
            "uc2_gateway_submits_total",
            "SUBMIT frames the node's Engine accepted.",
            s.submits,
        ),
        (
            "uc2_gateway_queries_total",
            "QUERY frames the node's Engine accepted.",
            s.queries,
        ),
        (
            "uc2_gateway_responses_total",
            "RESPONSE frames written back to clients.",
            s.responses,
        ),
        (
            "uc2_gateway_redirects_total",
            "REDIRECT frames written back to clients.",
            s.redirects,
        ),
        (
            "uc2_gateway_retries_total",
            "RETRY frames written back to clients, every reason.",
            s.retries,
        ),
        (
            "uc2_gateway_unknown_total",
            "UNKNOWN frames written back (the Engine timed a request out).",
            s.unknown,
        ),
        (
            "uc2_gateway_backpressure_events_total",
            "Times the Engine reported Backpressure and a connection's credits were halved.",
            s.backpressure_events,
        ),
        (
            "uc2_gateway_leader_changes_total",
            "Leader transitions the edge observed (can_serve flipped or the leader hint changed).",
            s.leader_changes,
        ),
        (
            "uc2_gateway_leader_changed_frames_total",
            "LEADER_CHANGED frames successfully written to clients.",
            s.leader_changed_frames,
        ),
        (
            "uc2_gateway_status_frames_total",
            "Standalone STATUS frames written to clients (idle liveness and credit announcements).",
            s.status_frames,
        ),
        (
            "uc2_gateway_refused_busy_total",
            "Connections refused with HELLO_REFUSED BUSY at max_connections.",
            s.refused_busy,
        ),
        (
            "uc2_gateway_grant_changes_total",
            "Times a connection's grant was recomputed to a different value.",
            s.grant_changes,
        ),
    ];
    for (name, help, value) in counters {
        push_counter(&mut out, name, help, value);
    }
    push_gauge(
        &mut out,
        "uc2_gateway_grant_outstanding",
        "Live sum of every ready connection's credits; held at or under uc2_gateway_grant_budget \
         except when max_connections exceeds the budget.",
        grant_outstanding,
    );
    push_gauge(
        &mut out,
        "uc2_gateway_grant_budget",
        "The edge's total grant budget: max_inflight less its 1/8 headroom.",
        u64::from(grant_budget),
    );
    out
}

/// `/healthz`: the daemon's tick loop stamped `tick_ns` recently.
fn healthz(tick_ns: u64, now: u64) -> Response {
    if now.saturating_sub(tick_ns) >= HEARTBEAT_STALE_NS {
        return (503, "text/plain", "gateway tick loop stalled\n".to_string());
    }
    (200, "text/plain", "ok\n".to_string())
}

/// `/readyz`: [`healthz`], the edge not faulted, and the node's heartbeat
/// fresh.
fn readyz(tick_ns: u64, faulted: bool, node_hb_ns: u64, now: u64) -> Response {
    let h = healthz(tick_ns, now);
    if h.0 != 200 {
        return h;
    }
    if faulted {
        return (
            503,
            "text/plain",
            "edge faulted: the node instance restarted under it\n".to_string(),
        );
    }
    if now.saturating_sub(node_hb_ns) >= HEARTBEAT_STALE_NS {
        return (503, "text/plain", "node heartbeat stale\n".to_string());
    }
    (200, "text/plain", "ok\n".to_string())
}

/// Serve the three routes on `bind` from one "uc2-gw-obs" thread. `tick` is
/// the daemon's tick-loop heartbeat (Unix ns), stamped every tick. Binds
/// NOW, so a port conflict is a startup failure.
pub fn serve(bind: SocketAddr, edge: EdgeObs, tick: Arc<AtomicU64>) -> io::Result<HttpServer> {
    let router = get_router(move |path| {
        let now = now_unix_ns();
        let tick_ns = tick.load(Ordering::Relaxed);
        Some(match path {
            "/metrics" => (
                200,
                "text/plain; version=0.0.4",
                render(&edge.stats(), edge.grant_outstanding(), edge.grant_budget()),
            ),
            "/healthz" => healthz(tick_ns, now),
            "/readyz" => readyz(tick_ns, edge.is_faulted(), edge.node_heartbeat_ns(), now),
            _ => return None,
        })
    });
    HttpServer::serve("uc2-gw-obs", bind, router)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stats_text_renders_a_counter_per_field_and_the_grant_gauges() {
        let s = EdgeStats {
            submits: 42,
            refused_busy: 3,
            ..EdgeStats::default()
        };
        let text = render(&s, 17, 3584);
        assert!(
            text.contains(
                "\n# TYPE uc2_gateway_submits_total counter\nuc2_gateway_submits_total 42\n"
            ),
            "{text}"
        );
        assert!(
            text.contains("\nuc2_gateway_refused_busy_total 3\n"),
            "{text}"
        );
        assert!(
            text.contains(
                "# TYPE uc2_gateway_grant_outstanding gauge\nuc2_gateway_grant_outstanding 17\n"
            ),
            "{text}"
        );
        assert!(text.contains("\nuc2_gateway_grant_budget 3584\n"), "{text}");
    }

    /// The series list is the render, family for family and in order — the
    /// same pin `uc_node`'s `CONTRACT_SERIES` carries.
    #[test]
    fn gateway_series_is_exactly_what_render_emits() {
        let text = render(&EdgeStats::default(), 0, 0);
        let families: Vec<&str> = text
            .lines()
            .filter_map(|l| l.strip_prefix("# TYPE "))
            .map(|l| l.split(' ').next().unwrap())
            .collect();
        assert_eq!(families, GATEWAY_SERIES);
        assert_eq!(text.matches("# HELP ").count(), GATEWAY_SERIES.len());
    }

    #[test]
    fn healthz_is_the_tick_loop_and_readyz_adds_the_edge_and_the_node() {
        let now = 10 * HEARTBEAT_STALE_NS;
        let fresh = now - 1;
        let stale = now - HEARTBEAT_STALE_NS;
        assert_eq!(healthz(fresh, now).0, 200);
        assert_eq!(healthz(stale, now).0, 503);
        assert_eq!(readyz(fresh, false, fresh, now).0, 200);
        assert_eq!(
            readyz(stale, false, fresh, now).0,
            503,
            "not live, not ready"
        );
        assert_eq!(readyz(fresh, true, fresh, now).0, 503, "faulted edge");
        assert_eq!(readyz(fresh, false, stale, now).0, 503, "dead node");
    }
}
