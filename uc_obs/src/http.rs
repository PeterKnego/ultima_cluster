// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The observability HTTP **transport** every UC daemon serves its
//! `/metrics`, `/healthz` and `/readyz` on — lifted out of `uc_node`'s
//! `obs::http` (#173) so `uc2-gateway`, which must not depend on `uc_node`,
//! serves the same listener rather than a copy of it. What moved is the
//! transport only: the accept loop, the bounded request read and
//! [`write_response`]. Each daemon keeps its own routes and hands them in as
//! a [`Router`].
//!
//! `std`-only: no tokio, no HTTP crate. One thread polling up to two
//! non-blocking listeners (a TCP one and, for `uc_node`, a unix socket) on a
//! 100 ms cadence so [`HttpServer::stop`] is prompt. GET-only by convention
//! (the router decides), `Connection: close`, no keep-alive.
//!
//! The accept loop is single-threaded and synchronous — a connection is
//! handled to completion before the next one is accepted — so every
//! connection is bound by a hard wall-clock deadline ([`CONN_DEADLINE`]),
//! not just a per-`read()`-call timeout: a client that trickles bytes just
//! often enough to keep dodging each individual read's timeout would
//! otherwise stall every other client (and [`HttpServer::stop`]) for as long
//! as it kept trickling. Both doors get the same deadlines.
//!
//! The write phase's bound is per write call (`SO_SNDTIMEO`), not a
//! wall-clock deadline: a peer that reads a large body a little at a time,
//! just often enough, can hold this one thread — and every other request —
//! for several [`CONN_DEADLINE`] periods. The listener belongs on the ops
//! network.

use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Cap on the bytes read while hunting for the end of the request headers —
/// a router only ever parses a request LINE, so a well-formed request never
/// gets close to this.
pub const REQUEST_CAP: usize = 4096;

/// The accept-loop poll cadence: how promptly [`HttpServer::stop`] returns.
pub const ACCEPT_POLL: Duration = Duration::from_millis(100);

/// Per-`read()`-call timeout. On its own this does NOT bound a connection's
/// total duration — `SO_RCVTIMEO` budgets each syscall independently, so a
/// client that trickles a byte every ~900ms never trips any single call's
/// timer and never reaches [`REQUEST_CAP`] either. [`CONN_DEADLINE`] is the
/// wall-clock backstop that actually bounds the connection.
pub const READ_TIMEOUT: Duration = Duration::from_secs(1);

/// Hard wall-clock cap on one connection's whole read phase, checked once
/// per read-loop iteration alongside the per-call [`READ_TIMEOUT`] (which is
/// also shrunk to whatever's left of this budget each iteration, so the
/// last read can't itself overshoot it). Because the accept loop handles
/// connections synchronously, one connection that never bounds itself
/// stalls the WHOLE server. Also reused as the flat `SO_SNDTIMEO` budget for
/// [`write_response`] — a client that never reads its response is the same
/// stall risk on the write side.
pub const CONN_DEADLINE: Duration = Duration::from_secs(2);

/// One routed answer: `(status, content type, body)`.
pub type Response = (u16, &'static str, String);

/// A daemon's routes: the raw request bytes in (at most [`REQUEST_CAP`] plus
/// one read chunk, possibly unterminated — a request is routed on whatever
/// arrived before EOF or the read timeout), one [`Response`] out. Must be
/// total on `&[u8]`: every byte an unauthenticated scraper sends lands here.
///
/// Any `Fn(&[u8]) -> Response + Send + 'static` closure is a router.
pub trait Router: Send + 'static {
    fn route(&self, request: &[u8]) -> Response;
}

impl<F: Fn(&[u8]) -> Response + Send + 'static> Router for F {
    fn route(&self, request: &[u8]) -> Response {
        self(request)
    }
}

/// A stream one connection is served over: the TCP door or the unix door.
/// Exists so [`handle_conn`]/[`write_response`] — and with them the read and
/// write deadlines — are ONE code path for both.
pub trait Conn: Read + Write {
    fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()>;
    fn set_write_timeout(&self, d: Option<Duration>) -> io::Result<()>;
}

impl Conn for TcpStream {
    fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        TcpStream::set_read_timeout(self, d)
    }
    fn set_write_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        TcpStream::set_write_timeout(self, d)
    }
}

impl Conn for UnixStream {
    fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        UnixStream::set_read_timeout(self, d)
    }
    fn set_write_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        UnixStream::set_write_timeout(self, d)
    }
}

/// A running observability HTTP server: one thread, up to two listeners.
pub struct HttpServer {
    thread: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    local_addr: Option<SocketAddr>,
}

impl HttpServer {
    /// Bind `bind` and serve `router` on TCP only, from a thread named
    /// `thread_name`. Binds NOW — a port conflict or permission error
    /// surfaces here, at startup, not silently on the first scrape.
    pub fn serve<R: Router>(thread_name: &str, bind: SocketAddr, router: R) -> io::Result<Self> {
        let l = TcpListener::bind(bind)?;
        HttpServer::serve_on(thread_name, Some(l), None, router)
    }

    /// Serve `router` on already-bound listeners (either may be absent),
    /// from one thread named `thread_name`. Both listeners are switched to
    /// non-blocking here.
    pub fn serve_on<R: Router>(
        thread_name: &str,
        tcp: Option<TcpListener>,
        unix: Option<UnixListener>,
        router: R,
    ) -> io::Result<Self> {
        if let Some(l) = &tcp {
            l.set_nonblocking(true)?;
        }
        if let Some(l) = &unix {
            l.set_nonblocking(true)?;
        }
        let local_addr = tcp.as_ref().map(|l| l.local_addr()).transpose()?;

        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);

        let thread = thread::Builder::new()
            .name(thread_name.to_string())
            .spawn(move || {
                let router = router;
                while !stop_thread.load(Ordering::Acquire) {
                    let mut served = false;
                    // Any accept error — WouldBlock (nothing pending) or a
                    // transient one such as EMFILE — is an empty poll: not
                    // fatal to the loop, back off and try again.
                    if let Some(l) = &tcp
                        && let Ok((s, _)) = l.accept()
                    {
                        let _ = s.set_nonblocking(false);
                        handle_conn(s, &router);
                        served = true;
                    }
                    if let Some(l) = &unix
                        && let Ok((s, _)) = l.accept()
                    {
                        let _ = s.set_nonblocking(false);
                        handle_conn(s, &router);
                        served = true;
                    }
                    if !served {
                        thread::sleep(ACCEPT_POLL);
                    }
                }
            })?;

        Ok(HttpServer {
            thread: Some(thread),
            stop,
            local_addr,
        })
    }

    /// The TCP listener's bound address, when one was served (meaningful
    /// when bound with port 0).
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    /// Signal the accept loop to stop and join its thread. Returns once the
    /// thread has actually exited (bounded by the 100 ms accept-poll cadence
    /// plus at most one in-flight connection's handling time).
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

/// Handle one connection end to end: read the request (capped,
/// per-call-timed-out, AND wall-clock-deadlined), route it, write the
/// response, close. No keep-alive — every response carries
/// `Connection: close`.
///
/// A connection that never completes a request within [`CONN_DEADLINE`] is
/// dropped outright (no response written) rather than routed on a partial
/// buffer — the client got no bytes, same as if it had never connected.
pub fn handle_conn<C: Conn, R: Router + ?Sized>(mut stream: C, router: &R) {
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
            // cannot hold this thread, and therefore the accept loop,
            // hostage. Drop it and move on.
            return;
        }
        if stream
            .set_read_timeout(Some(remaining.min(READ_TIMEOUT)))
            .is_err()
        {
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

    let (status, content_type, body) = router.route(&buf);
    write_response(&mut stream, status, content_type, &body);
}

/// The request line's `(method, path)`, the path stripped of any query
/// string — or `None` when the bytes are not UTF-8 or the first line does
/// not hold at least `METHOD SP PATH`.
pub fn request_line(buf: &[u8]) -> Option<(&str, &str)> {
    let line = std::str::from_utf8(buf).ok()?.lines().next()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?.split('?').next().unwrap_or("");
    Some((method, path))
}

/// A GET-only router over `routes`: an unparseable request line is a 400, a
/// non-GET or a path `routes` does not know (`None`) is a 404 (chosen over
/// 405 for a single-purpose scrape/probe endpoint with no other verbs to
/// advertise).
pub fn get_router<F>(routes: F) -> impl Router
where
    F: Fn(&str) -> Option<Response> + Send + 'static,
{
    move |buf: &[u8]| match request_line(buf) {
        None => bad_request(),
        Some(("GET", path)) => routes(path).unwrap_or_else(not_found),
        Some(_) => not_found(),
    }
}

/// The 404 every router answers an unknown request with.
pub fn not_found() -> Response {
    (404, "text/plain", "not found\n".to_string())
}

/// The 400 [`get_router`] answers an unparseable request line with.
pub fn bad_request() -> Response {
    (400, "text/plain", "bad request\n".to_string())
}

/// Write one `Connection: close` response. Best-effort: a client that hangs
/// up (or stalls) mid-write is not retried — this is a scrape/probe
/// endpoint, not a reliable transport.
pub fn write_response<C: Conn + ?Sized>(
    stream: &mut C,
    status: u16,
    content_type: &str,
    body: &str,
) {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
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
    // client AND `HttpServer::stop()`'s join (the daemon's SIGTERM handler
    // hangs). Bound it with the same budget as the read phase's wall-clock
    // deadline (`CONN_DEADLINE`).
    let _ = stream.set_write_timeout(Some(CONN_DEADLINE));
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body.as_bytes());
    let _ = stream.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(addr: SocketAddr, request: &[u8]) -> (u16, String) {
        let mut s = TcpStream::connect(addr).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.write_all(request).expect("write request");
        let mut out = String::new();
        s.read_to_string(&mut out).expect("read response");
        let (head, body) = out.split_once("\r\n\r\n").expect("header terminator");
        let code = head
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .expect("status code");
        (code, body.to_string())
    }

    fn server() -> HttpServer {
        let router = get_router(|path| match path {
            "/hello" => Some((200, "text/plain", "hi\n".to_string())),
            _ => None,
        });
        HttpServer::serve("uc-obs-test", "127.0.0.1:0".parse().unwrap(), router).expect("serve")
    }

    #[test]
    fn a_loopback_get_round_trips_through_the_router() {
        let srv = server();
        let addr = srv.tcp_addr().unwrap();
        let (code, body) = raw(addr, b"GET /hello?x=1 HTTP/1.1\r\nHost: t\r\n\r\n");
        assert_eq!((code, body.as_str()), (200, "hi\n"));
        srv.stop();
    }

    #[test]
    fn an_unparseable_request_line_is_a_400() {
        let srv = server();
        let addr = srv.tcp_addr().unwrap();
        let (code, _) = raw(addr, b"GARBAGE\r\n\r\n");
        assert_eq!(code, 400);
        let (code, _) = raw(addr, b"\xff\xfe /hello\r\n\r\n");
        assert_eq!(code, 400, "non-UTF-8 is unparseable too");
        srv.stop();
    }

    #[test]
    fn an_unknown_path_or_a_non_get_is_a_404() {
        let srv = server();
        let addr = srv.tcp_addr().unwrap();
        assert_eq!(raw(addr, b"GET /nope HTTP/1.1\r\n\r\n").0, 404);
        assert_eq!(raw(addr, b"POST /hello HTTP/1.1\r\n\r\n").0, 404);
        srv.stop();
    }

    /// IMPORTANT-2 regression (moved here with `write_response` from
    /// `uc_node::obs::http`): `write_all` had no timeout, so a client that
    /// connects and never reads its response could block it forever — and
    /// because the accept loop is single-threaded and synchronous, that
    /// stalls `stop()`'s join too. A body deliberately sized (32 MiB) to
    /// exceed any default socket buffer, against a real peer socket held
    /// open and never read, is the only honest way to make the pre-fix hang
    /// reproduce.
    #[test]
    fn write_response_does_not_block_forever_on_a_client_that_never_reads() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let client = TcpStream::connect(addr).expect("client connect");
        let (mut server_side, _peer) = listener.accept().expect("accept");

        let body = "x".repeat(32 * 1024 * 1024);
        let start = Instant::now();
        write_response(&mut server_side, 200, "text/plain", &body);
        let elapsed = start.elapsed();

        // The header write + the 32 MiB body write can each need their own
        // `CONN_DEADLINE`-bounded stall (observed ~6.1s locally); without
        // `set_write_timeout` this same call hangs past 15s.
        assert!(
            elapsed < Duration::from_secs(15),
            "write_response took {elapsed:?} against a client that never reads its \
             response — past its wall-clock write-timeout budget (CONN_DEADLINE={CONN_DEADLINE:?})"
        );
        drop(client);
    }

    /// A body that outgrows a unix socket's send buffer, to a same-host
    /// client that asks and never reads, must cost at most the write
    /// deadline, not the server (moved here from `uc_node::obs::http`,
    /// where `/cluster` is the route that can produce such a body).
    #[test]
    fn a_unix_client_that_never_reads_costs_only_the_write_deadline() {
        let (mut server_side, lazy) = UnixStream::pair().expect("pair");
        let body = "x".repeat(8 * 1024 * 1024);
        let start = Instant::now();
        write_response(&mut server_side, 200, "application/json", &body);
        let elapsed = start.elapsed();
        assert!(
            elapsed < CONN_DEADLINE * 3,
            "write_response held the obs thread {elapsed:?} against a unix client that never \
             reads (CONN_DEADLINE={CONN_DEADLINE:?})"
        );
        drop(lazy);
    }
}
