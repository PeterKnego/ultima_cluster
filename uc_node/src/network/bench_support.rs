//! Public shim for the inter-node transport microbench (`uc_autobench`).
//!
//! Stands up a single echo server + client for QUIC and UDP so the bench can
//! measure pure transport RPC cost — no consensus, no journal, no openraft.
//! Each transport exposes an identical [`EchoClient::rpc`] surface so the
//! microbench can A/B them directly.
//!
//! This module is `#[doc(hidden)] pub` test/bench-only surface: it reuses the
//! same [`Frame`]/[`MessageType`] wire framing, the same [`UdpMux`], and the
//! same QUIC TLS/stream machinery the real transports use, so the measured
//! round-trip exercises the actual transport code path.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;

use super::NetworkError;
use super::frame::{Frame, MessageType};
use super::udp::UdpTuning;
use super::udp::mux::UdpMux;

/// RPC timeout for the bench echo path. Loopback round-trips are sub-ms; a
/// generous ceiling avoids spurious timeouts under a saturated load generator.
const ECHO_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Uniform client handle over either transport. `rpc` round-trips one body and
/// returns the echoed body.
pub struct EchoClient {
    inner: EchoClientInner,
    /// Monotonic per-call request-id source (correlator).
    next_id: AtomicU64,
}

enum EchoClientInner {
    Udp {
        client: Arc<UdpMux>,
        server_addr: SocketAddr,
    },
    Quic {
        conn: quinn::Connection,
        /// Held so the client endpoint outlives in-flight RPCs.
        _endpoint: quinn::Endpoint,
    },
    BareTokioUdp {
        sock: std::sync::Arc<tokio::net::UdpSocket>,
        /// Held for documentation; the socket is already `connect`ed to this address.
        #[allow(dead_code)]
        peer: std::net::SocketAddr,
    },
    /// Busy-spin UDP: requests are sent to a pinned thread over an mpsc; replies
    /// come back over a per-call oneshot. The pinned thread owns a non-blocking
    /// socket and busy-polls for responses.
    BareBusyspinUdp {
        tx: tokio::sync::mpsc::Sender<(bytes::Bytes, tokio::sync::oneshot::Sender<bytes::Bytes>)>,
        /// Held so the thread handle joins on drop.
        #[allow(dead_code)]
        thread: std::thread::JoinHandle<()>,
    },
}

impl EchoClient {
    /// Round-trip `body` through the echo server, returning the echoed body.
    pub async fn rpc(&self, body: Bytes) -> Result<Bytes, NetworkError> {
        let request_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        match &self.inner {
            EchoClientInner::Udp {
                client,
                server_addr,
            } => {
                let req = Frame::new_request(MessageType::AppendEntriesReq, request_id, body);
                let resp = client
                    .rpc(*server_addr, "bench", req, ECHO_RPC_TIMEOUT)
                    .await?;
                Ok(resp.body)
            }
            EchoClientInner::Quic { conn, .. } => {
                let (mut send, mut recv) = conn
                    .open_bi()
                    .await
                    .map_err(|e| NetworkError::Stream(format!("open_bi: {e}")))?;
                let req = Frame::new_request(MessageType::AppendEntriesReq, request_id, body);
                let encoded = req.encode().freeze();
                send.write_all(&encoded)
                    .await
                    .map_err(|e| NetworkError::Stream(format!("write: {e}")))?;
                send.finish()
                    .map_err(|e| NetworkError::Stream(format!("finish: {e}")))?;
                let resp = tokio::time::timeout(ECHO_RPC_TIMEOUT, Frame::read_async(&mut recv))
                    .await
                    .map_err(|_| NetworkError::Timeout)??;
                if resp.request_id != request_id {
                    return Err(NetworkError::Decode(format!(
                        "request_id mismatch: expected {request_id} got {}",
                        resp.request_id
                    )));
                }
                Ok(resp.body)
            }
            EchoClientInner::BareTokioUdp { sock, .. } => {
                sock.send(&body).await.map_err(NetworkError::Io)?;
                let mut buf = vec![0u8; 64 * 1024];
                let n = sock.recv(&mut buf).await.map_err(NetworkError::Io)?;
                Ok(bytes::Bytes::copy_from_slice(&buf[..n]))
            }
            EchoClientInner::BareBusyspinUdp { tx, .. } => {
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                tx.send((body, reply_tx))
                    .await
                    .map_err(|_| NetworkError::Disconnected)?;
                reply_rx.await.map_err(|_| NetworkError::Disconnected)
            }
        }
    }
}

/// Server handle. `shutdown` releases the listener / socket.
pub struct EchoServer {
    inner: EchoServerInner,
}

enum EchoServerInner {
    Udp(Arc<UdpMux>),
    Quic {
        endpoint: quinn::Endpoint,
        accept_task: tokio::task::JoinHandle<()>,
    },
    BareTokioUdp {
        handle: tokio::task::JoinHandle<()>,
        local: std::net::SocketAddr,
    },
    BareBusyspinUdp {
        /// Sending a value signals the server thread to stop.
        stop_tx: tokio::sync::oneshot::Sender<()>,
        thread: std::thread::JoinHandle<()>,
        local: std::net::SocketAddr,
    },
}

impl EchoServer {
    /// The socket address the server is actually bound to. Useful when the
    /// caller bound `:0` (ephemeral port) and needs to tell a client where to
    /// connect (the loopback `_pair()` path, and SIGINT-driven harness logs).
    pub fn local_addr(&self) -> Result<SocketAddr, NetworkError> {
        match &self.inner {
            EchoServerInner::Udp(mux) => mux.local_addr().map_err(NetworkError::Io),
            EchoServerInner::Quic { endpoint, .. } => {
                endpoint.local_addr().map_err(NetworkError::Io)
            }
            EchoServerInner::BareTokioUdp { local, .. } => Ok(*local),
            EchoServerInner::BareBusyspinUdp { local, .. } => Ok(*local),
        }
    }

    /// Stop the echo server and free its socket / listener.
    pub async fn shutdown(self) {
        match self.inner {
            EchoServerInner::Udp(mux) => mux.shutdown().await,
            EchoServerInner::Quic {
                endpoint,
                accept_task,
            } => {
                endpoint.close(0u32.into(), b"shutdown");
                let _ = accept_task.await;
            }
            EchoServerInner::BareTokioUdp { handle, .. } => {
                handle.abort();
                let _ = handle.await;
            }
            EchoServerInner::BareBusyspinUdp { stop_tx, thread, .. } => {
                // Signal the server thread to exit, then join.
                let _ = stop_tx.send(());
                let _ = tokio::task::spawn_blocking(|| thread.join()).await;
            }
        }
    }
}

/// Bind a UDP echo server on `listen`. The server echoes each request body
/// back as a response frame. Use [`EchoServer::local_addr`] to discover the
/// bound port when `listen` uses an ephemeral (`:0`) port. Pairs with
/// [`udp_echo_client`] (possibly in a different process / on a different host).
pub async fn udp_echo_server(listen: SocketAddr) -> Result<EchoServer, NetworkError> {
    let server = UdpMux::bind(listen, UdpTuning::default()).await?;
    // Echo handler: reply with the same body, response flag set.
    server.set_request_handler(Arc::new(|req: Frame| {
        Box::pin(async move {
            Frame::new_response(MessageType::AppendEntriesResp, req.request_id, req.body)
        })
    }));
    Ok(EchoServer {
        inner: EchoServerInner::Udp(server),
    })
}

/// Build a UDP echo client that `rpc`s to `connect`. The client binds its own
/// ephemeral local socket and remembers `connect` as its fixed RPC target.
pub async fn udp_echo_client(connect: SocketAddr) -> Result<EchoClient, NetworkError> {
    // Bind a wildcard local socket in the same address family as the target.
    let bind: SocketAddr = if connect.is_ipv6() {
        "[::]:0".parse().unwrap()
    } else {
        "0.0.0.0:0".parse().unwrap()
    };
    let client = UdpMux::bind(bind, UdpTuning::default()).await?;
    Ok(EchoClient {
        inner: EchoClientInner::Udp {
            client,
            server_addr: connect,
        },
        next_id: AtomicU64::new(1),
    })
}

/// Bind a UDP echo server + client on loopback. Convenience for the in-process
/// (`--role both`) bench path: starts a server on `127.0.0.1:0`, reads its bound
/// address, and connects a client to it. Reimplemented in terms of
/// [`udp_echo_server`] / [`udp_echo_client`].
pub async fn udp_echo_pair() -> Result<(EchoClient, EchoServer), NetworkError> {
    let server = udp_echo_server("127.0.0.1:0".parse().unwrap()).await?;
    let server_addr = server.local_addr()?;
    let client = udp_echo_client(server_addr).await?;
    Ok((client, server))
}

/// Bind a QUIC echo server on `listen`. Mirrors the real QUIC machinery (fresh
/// self-signed cert, `quinn` server endpoint, accept → bi-stream `Frame`-echo
/// loop) but echoes frames instead of dispatching to Raft. Use
/// [`EchoServer::local_addr`] to discover the bound port when `listen` is `:0`.
/// Pairs with [`quic_echo_client`] (possibly cross-process / cross-host — the
/// client trusts the self-signed cert via an accept-any verifier).
pub async fn quic_echo_server(listen: SocketAddr) -> Result<EchoServer, NetworkError> {
    use super::quic::tls;

    // Fresh throwaway self-signed cert (in-memory; no files on disk).
    let (cert_pem, key_pem) = tls::generate_self_signed("uc-bench")?;
    let cert = {
        let mut r = cert_pem.as_bytes();
        rustls_pemfile::certs(&mut r)
            .next()
            .ok_or_else(|| NetworkError::Cert("no cert".into()))?
            .map_err(|e| NetworkError::Cert(format!("parse cert: {e}")))?
    };
    let key = {
        let mut r = key_pem.as_bytes();
        let k = rustls_pemfile::pkcs8_private_keys(&mut r)
            .next()
            .ok_or_else(|| NetworkError::Cert("no key".into()))?
            .map_err(|e| NetworkError::Cert(format!("parse key: {e}")))?;
        rustls::pki_types::PrivateKeyDer::Pkcs8(k)
    };
    let server_cfg = tls::build_server_config(cert, key)?;

    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(server_cfg.as_ref().clone())
        .map_err(|e| NetworkError::Tls(format!("quic server cfg: {e}")))?;
    let quic_server_cfg = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    let server_ep = quinn::Endpoint::server(quic_server_cfg, listen)
        .map_err(|e| NetworkError::Connect(format!("endpoint: {e}")))?;

    let accept_ep = server_ep.clone();
    let accept_task = tokio::spawn(async move {
        while let Some(incoming) = accept_ep.accept().await {
            tokio::spawn(async move {
                let conn = match incoming.await {
                    Ok(c) => c,
                    Err(_) => return,
                };
                while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                    tokio::spawn(async move {
                        // One stream may carry multiple frames; echo each.
                        loop {
                            let frame = match Frame::read_async(&mut recv).await {
                                Ok(f) => f,
                                Err(_) => break,
                            };
                            let resp = Frame::new_response(
                                MessageType::AppendEntriesResp,
                                frame.request_id,
                                frame.body,
                            );
                            if send.write_all(&resp.encode().freeze()).await.is_err() {
                                break;
                            }
                        }
                    });
                }
            });
        }
    });

    Ok(EchoServer {
        inner: EchoServerInner::Quic {
            endpoint: server_ep,
            accept_task,
        },
    })
}

/// Build a QUIC echo client connected to `connect`. Uses
/// [`tls::build_client_config`] (accept-any-cert verifier) so it trusts a
/// remote self-signed echo server. Each [`EchoClient::rpc`] opens a fresh
/// bi-stream and round-trips one `Frame`.
pub async fn quic_echo_client(connect: SocketAddr) -> Result<EchoClient, NetworkError> {
    use super::quic::tls;

    let client_cfg = tls::build_client_config()?;
    // Bind a wildcard local socket in the same address family as the target.
    let bind: SocketAddr = if connect.is_ipv6() {
        "[::]:0".parse().unwrap()
    } else {
        "0.0.0.0:0".parse().unwrap()
    };
    let mut client_ep = quinn::Endpoint::client(bind)
        .map_err(|e| NetworkError::Connect(format!("client endpoint: {e}")))?;
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(client_cfg.as_ref().clone())
        .map_err(|e| NetworkError::Tls(format!("quic client cfg: {e}")))?;
    client_ep.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
    let conn = client_ep
        .connect(connect, "uc-bench")
        .map_err(|e| NetworkError::Connect(format!("connect: {e}")))?
        .await
        .map_err(|e| NetworkError::Connect(format!("handshake: {e}")))?;

    Ok(EchoClient {
        inner: EchoClientInner::Quic {
            conn,
            _endpoint: client_ep,
        },
        next_id: AtomicU64::new(1),
    })
}

/// Bind a QUIC echo server + connect a client, on loopback. Convenience for the
/// in-process (`--role both`) bench path. Reimplemented in terms of
/// [`quic_echo_server`] / [`quic_echo_client`].
pub async fn quic_echo_pair() -> Result<(EchoClient, EchoServer), NetworkError> {
    let server = quic_echo_server("127.0.0.1:0".parse().unwrap()).await?;
    let server_addr = server.local_addr()?;
    let client = quic_echo_client(server_addr).await?;
    Ok((client, server))
}

/// Bind a bare tokio UDP echo server + client on loopback.
///
/// No [`Frame`] framing, no protocol — the server simply echoes every received
/// datagram back to the sender. This is the raw async-socket latency floor:
/// the cheapest possible UDP round-trip, with zero protocol overhead.
/// Use this as the bottom rung of the latency ladder.
pub async fn bare_tokio_udp_echo_pair() -> Result<(EchoClient, EchoServer), NetworkError> {
    let srv = tokio::net::UdpSocket::bind("127.0.0.1:0").await.map_err(NetworkError::Io)?;
    let local = srv.local_addr().map_err(NetworkError::Io)?;
    let handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 64 * 1024];
        while let Ok((n, from)) = srv.recv_from(&mut buf).await {
            let _ = srv.send_to(&buf[..n], from).await;
        }
    });
    let cli = tokio::net::UdpSocket::bind("127.0.0.1:0").await.map_err(NetworkError::Io)?;
    cli.connect(local).await.map_err(NetworkError::Io)?;
    let client = EchoClient {
        inner: EchoClientInner::BareTokioUdp {
            sock: std::sync::Arc::new(cli),
            peer: local,
        },
        next_id: AtomicU64::new(1),
    };
    let server = EchoServer {
        inner: EchoServerInner::BareTokioUdp { handle, local },
    };
    Ok((client, server))
}

/// Bind a bare busy-spin UDP echo server + client on loopback.
///
/// Both sides use a blocking `std::net::UdpSocket` (`set_nonblocking(true)`)
/// in a dedicated `std::thread` pinned to a core via `core_affinity`. The
/// server thread echoes every received datagram back to the sender. The client
/// thread owns its socket; [`EchoClient::rpc`] hands the request over a
/// `tokio::sync::mpsc` channel, the pinned thread busy-polls for the response,
/// and sends the reply back via a `tokio::sync::oneshot`.
///
/// This is the userspace busy-spin latency floor: the cheapest possible
/// round-trip without any async task scheduler overhead on the hot path.
pub async fn bare_busyspin_udp_echo_pair() -> Result<(EchoClient, EchoServer), NetworkError> {
    // Bind the server socket on an ephemeral port.
    let srv_sock = std::net::UdpSocket::bind("127.0.0.1:0").map_err(NetworkError::Io)?;
    srv_sock.set_nonblocking(true).map_err(NetworkError::Io)?;
    let local = srv_sock.local_addr().map_err(NetworkError::Io)?;

    // Collect core ids for pinning; fall back to unpinned if <2 cores.
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let server_core = core_ids.first().copied();
    let client_core = core_ids.get(1).copied();

    // ── Server thread ──────────────────────────────────────────────────────
    // Receives a oneshot stop signal; checks it each spin iteration via
    // `try_recv` so we can exit cleanly without blocking forever.
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();

    let server_thread = std::thread::spawn(move || {
        if let Some(core) = server_core {
            core_affinity::set_for_current(core);
        }
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match srv_sock.recv_from(&mut buf) {
                Ok((n, from)) => {
                    let _ = srv_sock.send_to(&buf[..n], from);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Check for stop signal without blocking.
                    if stop_rx.try_recv().is_ok() {
                        break;
                    }
                    std::hint::spin_loop();
                }
                Err(_) => break,
            }
        }
    });

    // ── Client thread ──────────────────────────────────────────────────────
    // Owns its own non-blocking socket (already `connect`ed to the server).
    // Receives (payload, reply_tx) from the async side; busy-sends and
    // busy-recvs, then fires the oneshot back to the awaiting async task.
    let (req_tx, mut req_rx) =
        tokio::sync::mpsc::channel::<(bytes::Bytes, tokio::sync::oneshot::Sender<bytes::Bytes>)>(
            16,
        );

    let client_thread = std::thread::spawn(move || {
        if let Some(core) = client_core {
            core_affinity::set_for_current(core);
        }
        let cli_sock = std::net::UdpSocket::bind("127.0.0.1:0").expect("client bind");
        cli_sock.set_nonblocking(true).expect("set_nonblocking");
        cli_sock.connect(local).expect("client connect");
        let mut buf = vec![0u8; 64 * 1024];

        // Process requests until the mpsc sender is dropped (client dropped).
        while let Some((body, reply_tx)) = req_rx.blocking_recv() {
            // Busy-send (retry on WouldBlock, which is rare on loopback).
            loop {
                match cli_sock.send(&body) {
                    Ok(_) => break,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::hint::spin_loop();
                    }
                    Err(_) => break,
                }
            }
            // Busy-recv until we get the echoed datagram back.
            let echoed = loop {
                match cli_sock.recv(&mut buf) {
                    Ok(n) => break bytes::Bytes::copy_from_slice(&buf[..n]),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::hint::spin_loop();
                    }
                    Err(_) => break bytes::Bytes::new(),
                }
            };
            let _ = reply_tx.send(echoed);
        }
    });

    let client = EchoClient {
        inner: EchoClientInner::BareBusyspinUdp {
            tx: req_tx,
            thread: client_thread,
        },
        next_id: AtomicU64::new(1),
    };
    let server = EchoServer {
        inner: EchoServerInner::BareBusyspinUdp {
            stop_tx,
            thread: server_thread,
            local,
        },
    };
    Ok((client, server))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bare_busyspin_udp_echo_roundtrips() {
        let (client, server) = bare_busyspin_udp_echo_pair().await.unwrap();
        let resp = client.rpc(bytes::Bytes::from_static(b"spin-payload")).await.unwrap();
        assert_eq!(&resp[..], b"spin-payload");
        server.shutdown().await;
    }

    #[tokio::test]
    async fn bare_tokio_udp_echo_roundtrips() {
        let (client, server) = bare_tokio_udp_echo_pair().await.unwrap();
        let resp = client.rpc(bytes::Bytes::from_static(b"ping-payload")).await.unwrap();
        assert_eq!(&resp[..], b"ping-payload");
        server.shutdown().await;
    }

    #[tokio::test]
    async fn udp_echo_round_trips() {
        let (client, server) = udp_echo_pair().await.unwrap();
        let resp = client.rpc(Bytes::from_static(b"ping")).await.unwrap();
        assert_eq!(&resp[..], b"ping");
        server.shutdown().await;
    }

    #[tokio::test]
    async fn quic_echo_round_trips() {
        let (client, server) = quic_echo_pair().await.unwrap();
        let resp = client.rpc(Bytes::from_static(b"hello")).await.unwrap();
        assert_eq!(&resp[..], b"hello");
        // A second RPC reuses the connection (fresh stream).
        let resp2 = client.rpc(Bytes::from_static(b"world")).await.unwrap();
        assert_eq!(&resp2[..], b"world");
        server.shutdown().await;
    }
}
