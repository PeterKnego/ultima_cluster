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
}

impl EchoServer {
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
        }
    }
}

/// Bind a UDP echo server + client on loopback. The server echoes each request
/// body back as a response frame; the returned [`EchoClient`] already knows the
/// server's bound address.
pub async fn udp_echo_pair() -> Result<(EchoClient, EchoServer), NetworkError> {
    let server = UdpMux::bind("127.0.0.1:0".parse().unwrap(), UdpTuning::default()).await?;
    let server_addr = server.local_addr().map_err(NetworkError::Io)?;
    // Echo handler: reply with the same body, response flag set.
    server.set_request_handler(Arc::new(|req: Frame| {
        Box::pin(async move {
            Frame::new_response(MessageType::AppendEntriesResp, req.request_id, req.body)
        })
    }));

    let client = UdpMux::bind("127.0.0.1:0".parse().unwrap(), UdpTuning::default()).await?;

    let echo_client = EchoClient {
        inner: EchoClientInner::Udp {
            client,
            server_addr,
        },
        next_id: AtomicU64::new(1),
    };
    Ok((
        echo_client,
        EchoServer {
            inner: EchoServerInner::Udp(server),
        },
    ))
}

/// Bind a QUIC echo server + connect a client, on loopback. Mirrors the real
/// QUIC machinery (self-signed cert, `quinn` server/client endpoints, `Frame`
/// stream round-trip) but echoes frames instead of dispatching to Raft.
pub async fn quic_echo_pair() -> Result<(EchoClient, EchoServer), NetworkError> {
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
    let client_cfg = tls::build_client_config()?;

    // --- Server endpoint: accept connections, echo each bi-stream's frames. ---
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(server_cfg.as_ref().clone())
        .map_err(|e| NetworkError::Tls(format!("quic server cfg: {e}")))?;
    let quic_server_cfg = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    let server_ep = quinn::Endpoint::server(quic_server_cfg, "127.0.0.1:0".parse().unwrap())
        .map_err(|e| NetworkError::Connect(format!("endpoint: {e}")))?;
    let server_addr = server_ep.local_addr().map_err(NetworkError::Io)?;

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

    // --- Client endpoint: connect to the server. ---
    let mut client_ep = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap())
        .map_err(|e| NetworkError::Connect(format!("client endpoint: {e}")))?;
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(client_cfg.as_ref().clone())
        .map_err(|e| NetworkError::Tls(format!("quic client cfg: {e}")))?;
    client_ep.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
    let conn = client_ep
        .connect(server_addr, "uc-bench")
        .map_err(|e| NetworkError::Connect(format!("connect: {e}")))?
        .await
        .map_err(|e| NetworkError::Connect(format!("handshake: {e}")))?;

    let echo_client = EchoClient {
        inner: EchoClientInner::Quic {
            conn,
            _endpoint: client_ep,
        },
        next_id: AtomicU64::new(1),
    };
    Ok((
        echo_client,
        EchoServer {
            inner: EchoServerInner::Quic {
                endpoint: server_ep,
                accept_task,
            },
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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
