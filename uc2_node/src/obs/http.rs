// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M10 Task 6: the observability HTTP endpoint (`/metrics`, `/healthz`,
//! `/readyz`) and the role-aware readiness rule the spec calls out by name —
//! `flags == NODE_FLAG_LEADER` alone (the `0x01` state: elected, not yet
//! `CAN_SERVE`) is NOT ready, even though the naive probe ("is this node the
//! leader?") would say yes.
//!
//! `std`-only: no tokio, no HTTP crate. One thread ("uc2-obs"), a
//! `TcpListener` polled non-blocking on a 100 ms cadence so [`ObsServer::stop`]
//! is prompt, GET-only, `Connection: close` (no keep-alive). The handler only
//! reads through [`ObsSources`]'s `Arc`s and atomics — it shares no lock with
//! the hot-path agents and cannot perturb them.

use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};

use super::ObsSources;
use super::metrics::{now_unix_ns, render_prometheus};

/// Liveness/readiness heartbeat staleness bar. Anything older than this is
/// treated as "the writer stopped stamping it", not "the writer is just slow".
const HEARTBEAT_STALE_NS: u64 = 3_000_000_000;

/// Cap on the bytes read while hunting for the end of the request headers —
/// this server only ever parses a request LINE, so a well-formed request
/// never gets close to this.
const REQUEST_CAP: usize = 4096;

/// The accept-loop poll cadence: how promptly [`ObsServer::stop`] returns.
const ACCEPT_POLL: Duration = Duration::from_millis(100);

/// Per-connection read timeout — a client that opens a socket and never
/// sends bytes doesn't get to hold a "uc2-obs" thread hostage.
const READ_TIMEOUT: Duration = Duration::from_secs(1);

/// A running observability HTTP server: one thread, one `TcpListener`, no
/// other state shared with the node's hot path beyond the read-only
/// [`ObsSources`] bundle handed to [`ObsServer::serve`].
pub struct ObsServer {
    thread: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    local_addr: SocketAddr,
}

impl ObsServer {
    /// Bind `bind` and start serving `/metrics`, `/healthz`, `/readyz` on a
    /// dedicated "uc2-obs" thread. Binds NOW — a port conflict or permission
    /// error surfaces here, at startup, not silently on the first scrape.
    pub fn serve(sources: ObsSources, bind: SocketAddr) -> io::Result<ObsServer> {
        let listener = TcpListener::bind(bind)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;

        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);

        let thread = thread::Builder::new().name("uc2-obs".to_string()).spawn(move || {
            let sources = sources;
            while !stop_thread.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _peer)) => handle_conn(stream, &sources),
                    Err(e) if e.kind() == ErrorKind::WouldBlock => thread::sleep(ACCEPT_POLL),
                    // Any other accept error (e.g. a transient EMFILE) is not
                    // fatal to the server loop — back off the same as an
                    // empty poll and try again.
                    Err(_) => thread::sleep(ACCEPT_POLL),
                }
            }
        })?;

        Ok(ObsServer { thread: Some(thread), stop, local_addr })
    }

    /// The bound address — meaningful when `serve` was called with port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Signal the accept loop to stop and join its thread. Returns once the
    /// thread has actually exited (bounded by the 100 ms accept-poll
    /// cadence plus at most one in-flight connection's handling time).
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

/// Handle one connection end to end: read the request (line-only parsing,
/// capped and timed out), route it, write the response, close. No
/// keep-alive — every response carries `Connection: close`.
fn handle_conn(mut stream: TcpStream, sources: &ObsSources) {
    if stream.set_read_timeout(Some(READ_TIMEOUT)).is_err() {
        return;
    }

    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    loop {
        if buf.len() >= REQUEST_CAP || has_header_terminator(&buf) {
            break;
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                break;
            }
            Err(_) => return,
        }
    }

    let (status, content_type, body) = route(&buf, sources);
    write_response(&mut stream, status, content_type, &body);
}

fn has_header_terminator(buf: &[u8]) -> bool {
    buf.windows(4).any(|w| w == b"\r\n\r\n")
}

/// Parse only the request line (`METHOD SPACE PATH ...`) and dispatch.
/// Anything that isn't `GET /metrics`, `GET /healthz`, or `GET /readyz` —
/// including a non-GET method, an unparseable line, or an unknown path —
/// is a 404 (chosen over 405 for a single-purpose scrape/probe endpoint
/// with no other verbs to advertise).
fn route(buf: &[u8], sources: &ObsSources) -> (u16, &'static str, String) {
    let request_line =
        std::str::from_utf8(buf).ok().and_then(|text| text.lines().next()).unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("").split('?').next().unwrap_or("");

    if method != "GET" {
        return not_found();
    }

    match path {
        "/metrics" => (200, "text/plain; version=0.0.4", render_prometheus(sources)),
        "/healthz" => healthz(sources),
        "/readyz" => readyz(sources),
        _ => not_found(),
    }
}

fn not_found() -> (u16, &'static str, String) {
    (404, "text/plain", "not found\n".to_string())
}

/// Liveness: are this node's four polling agents still running, and is the
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

    let status = sources.cnc.status();
    let flags = status.flags.load_acquire();
    let is_leader = flags & NODE_FLAG_LEADER != 0;
    let can_serve = flags & NODE_FLAG_CAN_SERVE != 0;

    if is_leader && !can_serve {
        return (503, "text/plain", "elected, NewTerm not yet quorum-committed\n".to_string());
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
    (200, "text/plain", format!("ok role={role} can_serve={can_serve}\n"))
}

fn first_dead_agent(sources: &ObsSources) -> Option<&'static str> {
    sources
        .agents
        .iter()
        .find(|(_, flag)| flag.load(Ordering::Acquire))
        .map(|(name, _)| *name)
}

fn write_response(stream: &mut TcpStream, status: u16, content_type: &str, body: &str) {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    // Best-effort: a client that hangs up mid-write doesn't get retried —
    // this is a scrape/probe endpoint, not a reliable transport.
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body.as_bytes());
    let _ = stream.flush();
}
