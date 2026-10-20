// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M10 Task 6: the observability HTTP endpoint (`/metrics`, `/healthz`,
//! `/readyz`) and the role-aware readiness rule the spec calls out by name —
//! `flags == NODE_FLAG_LEADER` alone (the `0x01` state: elected, not yet
//! `CAN_SERVE`) is NOT ready, even though the naive probe ("is this node the
//! leader?") would say yes.
//!
//! The transport — the accept loop, the bounded request read and the
//! response write — lives in [`uc_obs::http`] since #173, shared with
//! `uc2-gateway`; this module keeps the node's routes and its two doors.
//!
//! `std`-only: no tokio, no HTTP crate. One thread ("uc2-obs") polling up to
//! two non-blocking listeners on a 100 ms cadence so [`ObsServer::stop`] is
//! prompt: the `[metrics]` `TcpListener` when one is configured, and (cluster
//! view, project 4) the always-on unix socket `<instance_dir>/obs.sock`
//! ([`super::socket`]). Both doors serve the same router — `/metrics`,
//! `/healthz`, `/readyz` and `/cluster` — GET-only, `Connection: close` (no
//! keep-alive). The handler reads through [`ObsSources`]'s `Arc`s and
//! atomics and takes two short locks, each held only for a copy: the
//! holdings cache (one `Holdings` copy — the consensus agent takes the same
//! lock at most every 10 ms to refresh it, the `uc2-holdings` thread once
//! per probe) and, for `/cluster`, the soft table wire (an at most 8-entry
//! clone — the sender agent takes the same lock once per `STATUS` it
//! handles). No hot-path agent does work for the handler; at worst an agent
//! waits out one such copy.
//!
//! The write phase's bound is per write call (`SO_SNDTIMEO`), not a
//! wall-clock deadline: a peer that reads a large `/cluster` body a little
//! at a time, just often enough, can hold this one thread — and every other
//! request — for several of `uc_obs::http`'s 2 s `CONN_DEADLINE` periods. `[metrics]` belongs on
//! the ops network.
//!
//! The accept loop is single-threaded and synchronous — `uc_obs::http::handle_conn` runs
//! to completion before the next connection is even accepted — so every
//! connection is bound by a hard wall-clock deadline (`uc_obs::http`'s 2 s `CONN_DEADLINE`),
//! not just a per-`read()`-call timeout: a client that trickles bytes just
//! often enough to keep dodging each individual read's timeout would
//! otherwise stall every other client (and [`ObsServer::stop`]) for as long
//! as it kept trickling. Both doors get the same deadlines: `/cluster` can
//! outgrow a unix socket's send buffer, so a unix client that never reads is
//! the same write-side stall risk a TCP one is.

use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use uc_obs::http::{HttpServer, Response, not_found, request_line};
use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};

use super::ObsSources;
use super::metrics::{now_unix_ns, render_prometheus};

/// Liveness/readiness heartbeat staleness bar. Anything older than this is
/// treated as "the writer stopped stamping it", not "the writer is just slow".
///
/// Pinned equal to [`crate::services::SERVICE_STALE_NS`] (M14c) by
/// `the_readiness_bar_and_the_detach_bar_agree` below.
const HEARTBEAT_STALE_NS: u64 = 3_000_000_000;

/// A running observability HTTP server: one thread, up to two listeners
/// (TCP and the unix socket), no other state shared with the node's hot path
/// beyond the read-only [`ObsSources`] bundle handed to
/// [`ObsServer::serve_on`].
pub struct ObsServer {
    server: HttpServer,
    unix_path: Option<PathBuf>,
}

impl ObsServer {
    /// Bind `bind` and start serving on TCP only — [`ObsServer::serve_on`]
    /// with no unix socket. Binds NOW — a port conflict or permission error
    /// surfaces here, at startup, not silently on the first scrape.
    pub fn serve(sources: ObsSources, bind: SocketAddr) -> io::Result<ObsServer> {
        ObsServer::serve_on(sources, Some(bind), None)
    }

    /// Bind the TCP listener at `tcp` (if any) and the unix socket
    /// `<unix_dir>/obs.sock` (if any — the caller must hold that dir's
    /// `instance.lock`, since a stale socket file is replaced), then serve
    /// both from one "uc2-obs" thread. Both binds happen NOW, so either
    /// failure surfaces at startup.
    pub fn serve_on(
        sources: ObsSources,
        tcp: Option<SocketAddr>,
        unix_dir: Option<&Path>,
    ) -> io::Result<ObsServer> {
        let tcp = tcp.map(TcpListener::bind).transpose()?;
        let unix = unix_dir.map(super::socket::bind).transpose()?;
        let unix_path = unix_dir.map(|d| d.join(super::socket::OBS_SOCKET_FILE));

        let router = move |buf: &[u8]| route(buf, &sources);
        let server = match HttpServer::serve_on("uc2-obs", tcp, unix, router) {
            Ok(s) => s,
            Err(e) => {
                if let Some(p) = &unix_path {
                    let _ = std::fs::remove_file(p);
                }
                return Err(e);
            }
        };

        Ok(ObsServer { server, unix_path })
    }

    /// The TCP listener's bound address, when one was served (meaningful
    /// when bound with port 0).
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        self.server.tcp_addr()
    }

    /// The TCP listener's bound address. Panics on a server started without
    /// TCP — use [`ObsServer::tcp_addr`] there.
    pub fn local_addr(&self) -> SocketAddr {
        self.tcp_addr().expect("served without TCP")
    }

    /// Signal the accept loop to stop and join its thread, then unlink the
    /// socket file this server bound. Returns once the thread has actually
    /// exited (bounded by the 100 ms accept-poll cadence plus at most one
    /// in-flight connection's handling time).
    pub fn stop(self) {
        self.server.stop();
        if let Some(p) = &self.unix_path {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// Parse only the request line (`METHOD SPACE PATH ...`) and dispatch.
/// Anything that isn't `GET /metrics`, `GET /healthz`, `GET /readyz` or
/// `GET /cluster` (the cluster view, `uc2.cluster-view/1` JSON) —
/// including a non-GET method, an unparseable line, or an unknown path —
/// is a 404 (chosen over 405 for a single-purpose scrape/probe endpoint
/// with no other verbs to advertise).
fn route(buf: &[u8], sources: &ObsSources) -> Response {
    // An unparseable line is a 404 here, not `uc_obs::http::get_router`'s
    // 400: this router's status set is pinned at `200 | 404 | 503` (the
    // `uc_node_http` fuzz target asserts it).
    let Some(("GET", path)) = request_line(buf) else {
        return not_found();
    };

    match path {
        "/metrics" => (200, "text/plain; version=0.0.4", render_prometheus(sources)),
        "/healthz" => healthz(sources),
        "/readyz" => readyz(sources),
        "/cluster" => (200, "application/json", crate::view::render_live(sources)),
        _ => not_found(),
    }
}

/// [`route`] exposed for tests and for the `uc_node_http` fuzz target — the
/// router with the socket taken away. **Not API**: `cfg(any(test, fuzzing))`
/// only, so it does not exist in a shipped build.
///
/// `route` is where every byte an unauthenticated scrape client sends first
/// lands (`uc_obs::http::handle_conn` reads into a 4096-byte-capped buffer and calls
/// straight through), so it must be total on `&[u8]`. The status set it can
/// return is exactly `200 | 404 | 503` — see `uc_obs::http`'s `write_response` reason
/// table, which is the other half of the same contract.
#[cfg(any(test, fuzzing))]
pub fn route_raw(buf: &[u8], sources: &ObsSources) -> (u16, &'static str, String) {
    route(buf, sources)
}

/// Liveness: are this node's five polling agents still running (the four
/// data-plane ones plus `uc2-cluster` since the cluster FSM), and is the
/// node itself still stamping its own heartbeat? Deliberately independent of
/// role or `CAN_SERVE` — an elected-but-not-yet-serving leader (the `0x01`
/// state) is alive; it just isn't ready. See [`readyz`].
fn healthz(sources: &ObsSources) -> (u16, &'static str, String) {
    if let Some(name) = first_dead_agent(sources) {
        return (503, "text/plain", format!("agent {name} fail-stopped\n"));
    }

    let node_hb = sources.cnc.status().node_heartbeat_ns.load_acquire();
    if now_unix_ns().saturating_sub(node_hb) >= HEARTBEAT_STALE_NS {
        return (503, "text/plain", "node heartbeat stale\n".to_string());
    }

    (200, "text/plain", "ok\n".to_string())
}

/// Readiness: role-aware, and the whole reason this endpoint exists apart
/// from `/healthz`. `flags == NODE_FLAG_LEADER` alone (elected, not yet
/// `CAN_SERVE` — the window before the new term's first entry is
/// quorum-committed) is explicitly NOT ready, even though a naive
/// "am I the leader" probe would say yes.
fn readyz(sources: &ObsSources) -> (u16, &'static str, String) {
    if let Some(name) = first_dead_agent(sources) {
        return (503, "text/plain", format!("agent {name} fail-stopped\n"));
    }

    // Jumbo spec §6: a node holding a startup gate does not answer readiness,
    // whatever its role. Checked before the role logic below, which refuses a
    // clear CAN_SERVE bit only for a leader — a gated FOLLOWER would otherwise
    // read as ready while `Node::can_serve` says it is not. Deliberately NOT
    // "503 whenever !can_serve": that would change readiness for every
    // follower in the fleet.
    if sources.jumbo_gate_pending.load(Ordering::Acquire) {
        return (
            503,
            "text/plain",
            "jumbo startup gate pending: this node has not proven the cluster's datagram rung\n"
                .to_string(),
        );
    }

    let status = sources.cnc.status();
    let flags = status.flags.load_acquire();
    let is_leader = flags & NODE_FLAG_LEADER != 0;
    let can_serve = flags & NODE_FLAG_CAN_SERVE != 0;

    if is_leader && !can_serve {
        return (
            503,
            "text/plain",
            "elected, NewTerm not yet quorum-committed\n".to_string(),
        );
    }

    let now = now_unix_ns();
    let node_hb = status.node_heartbeat_ns.load_acquire();
    if now.saturating_sub(node_hb) >= HEARTBEAT_STALE_NS {
        return (503, "text/plain", "node heartbeat stale\n".to_string());
    }

    let service_hb = status.service_heartbeat_ns.load_acquire();
    if now.saturating_sub(service_hb) >= HEARTBEAT_STALE_NS {
        return (503, "text/plain", "service heartbeat stale\n".to_string());
    }

    let role = if is_leader { "leader" } else { "follower" };
    (
        200,
        "text/plain",
        format!("ok role={role} can_serve={can_serve}\n"),
    )
}

fn first_dead_agent(sources: &ObsSources) -> Option<&'static str> {
    sources
        .agents
        .iter()
        .find(|(_, flag)| flag.load(Ordering::Acquire))
        .map(|(name, _)| *name)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M14c: readiness and the `service_detached` transition record use the
    /// same 3 s staleness bar. Two constants, one number, one test.
    #[test]
    fn the_readiness_bar_and_the_detach_bar_agree() {
        assert_eq!(HEARTBEAT_STALE_NS, crate::services::SERVICE_STALE_NS);
    }
}
