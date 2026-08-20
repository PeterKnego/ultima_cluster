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
//!
//! The accept loop is single-threaded and synchronous — `handle_conn` runs
//! to completion before the next connection is even accepted — so every
//! connection is bound by a hard wall-clock deadline ([`CONN_DEADLINE`]),
//! not just a per-`read()`-call timeout: a client that trickles bytes just
//! often enough to keep dodging each individual read's timeout would
//! otherwise stall every other client (and [`ObsServer::stop`]) for as long
//! as it kept trickling.

use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

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

/// Per-`read()`-call timeout. On its own this does NOT bound a connection's
/// total duration — `SO_RCVTIMEO` budgets each syscall independently, so a
/// client that trickles a byte every ~900ms never trips any single call's
/// timer and never reaches [`REQUEST_CAP`] either. [`CONN_DEADLINE`] is the
/// wall-clock backstop that actually bounds the connection.
const READ_TIMEOUT: Duration = Duration::from_secs(1);

/// Hard wall-clock cap on one connection's whole read phase, checked once
/// per read-loop iteration alongside the per-call [`READ_TIMEOUT`] (which is
/// also shrunk to whatever's left of this budget each iteration, so the
/// last read can't itself overshoot it). Because [`ObsServer::serve`]'s
/// accept loop handles connections synchronously, one connection that never
/// bounds itself stalls the WHOLE server — every other client's
/// `/metrics`/`/healthz`/`/readyz`, and [`ObsServer::stop`]'s join. Also
/// reused as the flat `SO_SNDTIMEO` budget for [`write_response`] — a client
/// that never reads its response is the same stall risk on the write side.
const CONN_DEADLINE: Duration = Duration::from_secs(2);

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
/// capped, per-call-timed-out, AND wall-clock-deadlined), route it, write
/// the response, close. No keep-alive — every response carries
/// `Connection: close`.
///
/// A connection that never completes a request within [`CONN_DEADLINE`] is
/// dropped outright (no response written) rather than routed on a partial
/// buffer — the client got no bytes, same as if it had never connected.
fn handle_conn(mut stream: TcpStream, sources: &ObsSources) {
    let deadline = Instant::now() + CONN_DEADLINE;

    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    let mut terminated = false;
    loop {
        if buf.len() >= REQUEST_CAP || terminated {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            // Wall-clock budget exhausted — a client trickling bytes just
            // often enough to keep dodging each individual read's timeout
            // (SO_RCVTIMEO budgets each syscall independently, not the
            // connection as a whole) cannot hold this thread, and therefore
            // the accept loop, hostage. Drop it and move on.
            return;
        }
        if stream.set_read_timeout(Some(remaining.min(READ_TIMEOUT))).is_err() {
            return;
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                // Only the tail spanning the boundary between the old and
                // new bytes can contain a terminator that wasn't already
                // ruled out — no need to rescan bytes already checked.
                let scan_from = buf.len().saturating_sub(3);
                buf.extend_from_slice(&chunk[..n]);
                terminated = buf[scan_from..].windows(4).any(|w| w == b"\r\n\r\n");
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                break;
            }
            Err(_) => return,
        }
    }

    let (status, content_type, body) = route(&buf, sources);
    write_response(&mut stream, status, content_type, &body);
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
    // A client that never reads (or trickles reads just slowly enough)
    // would otherwise block `write_all` forever — and because the accept
    // loop is single-threaded and synchronous, that stalls every other
    // client AND `ObsServer::stop()`'s join (the daemon's SIGTERM handler
    // hangs). Bound it with the same budget as the read phase's wall-clock
    // deadline (`CONN_DEADLINE`); best-effort beyond that — a client that
    // hangs up (or stalls) mid-write doesn't get retried, this is a
    // scrape/probe endpoint, not a reliable transport.
    let _ = stream.set_write_timeout(Some(CONN_DEADLINE));
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body.as_bytes());
    let _ = stream.flush();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    /// IMPORTANT-2 regression: `write_response`'s `write_all` had no
    /// timeout, so a client that connects and never reads its response
    /// could block it forever — and because `ObsServer`'s accept loop is
    /// single-threaded and synchronous, that stalls `ObsServer::stop()`'s
    /// join too (the daemon's SIGTERM path hangs). A real `/metrics` scrape
    /// is only a few KB, well within default OS socket buffers, so this
    /// drives `write_response` directly (it's private to this module) with
    /// a body deliberately sized (32 MiB) to exceed any default socket
    /// buffer, against a real peer socket that is held open and never
    /// read — the only honest way to make the pre-fix hang actually
    /// reproduce in a unit test.
    #[test]
    fn write_response_does_not_block_forever_on_a_client_that_never_reads() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let client = TcpStream::connect(addr).expect("client connect");
        let (mut server_side, _peer) = listener.accept().expect("accept");
        // `client` is held open (not dropped) and never read from — that's
        // the whole point. `server_side` is this test's stand-in for the
        // stream `handle_conn` would have passed to `write_response`.

        let body = "x".repeat(32 * 1024 * 1024);
        let start = Instant::now();
        write_response(&mut server_side, 200, "text/plain", &body);
        let elapsed = start.elapsed();

        // `write_all`'s header write + the 32 MiB body write can each need
        // their own `CONN_DEADLINE`-bounded stall before the kernel gives
        // up on a peer that never drains its window (observed ~6.1s
        // locally — a small fixed multiple of CONN_DEADLINE, not
        // unbounded), so the bound here is generous. What actually
        // distinguishes this test: with `set_write_timeout` removed, this
        // same call hangs past 15s (verified manually — the kernel's own
        // retransmission give-up is on the order of minutes), so 15s here
        // is still well short of "unbounded" while comfortably clear of
        // the bounded case.
        assert!(
            elapsed < Duration::from_secs(15),
            "write_response took {elapsed:?} against a client that never reads its \
             response — past its wall-clock write-timeout budget (CONN_DEADLINE={CONN_DEADLINE:?})"
        );
        drop(client);
    }
}
