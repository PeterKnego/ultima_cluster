//! Per-peer QUIC connection wrapper.
//!
//! Each RPC opens a fresh bidirectional stream on the shared QUIC connection,
//! sends the request frame, reads the response frame, and closes the stream.
//! No out-of-order correlation is needed; request_id is verified defensively
//! against the response frame.
//!
//! Approach choice: "one stream per RPC" (vs. "persistent per-class streams").
//! QUIC stream open/close is cheap (small frame, no handshake). Future perf
//! work can promote to persistent streams if profiling justifies it.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use quinn::{ClientConfig as QuicClientConfig, Endpoint};

use super::NetworkError;
use super::frame::{Frame, MessageType};

pub struct PeerConn {
    pub(crate) inner: Arc<PeerConnInner>,
}

pub(crate) struct PeerConnInner {
    conn: quinn::Connection,
    request_id: AtomicU64,
}

impl PeerConn {
    /// Establish a new QUIC connection to a peer using a shared client endpoint.
    ///
    /// The endpoint is owned by the factory (one UDP socket per process). This
    /// method builds a per-connect `quinn::ClientConfig` from the supplied
    /// rustls config and uses `Endpoint::connect_with` to attach it to a new
    /// connection without mutating the endpoint's default client config.
    pub async fn connect(
        endpoint: &Endpoint,
        rustls_client_cfg: Arc<rustls::ClientConfig>,
        peer_addr: SocketAddr,
        server_name: &str,
    ) -> Result<Self, NetworkError> {
        let crypto =
            quinn::crypto::rustls::QuicClientConfig::try_from(rustls_client_cfg.as_ref().clone())
                .map_err(|e| NetworkError::Tls(format!("quic client cfg: {e}")))?;
        let mut client_cfg = QuicClientConfig::new(Arc::new(crypto));
        let mut transport = quinn::TransportConfig::default();
        transport.max_idle_timeout(Some(
            std::time::Duration::from_secs(30)
                .try_into()
                .expect("30s fits in IdleTimeout"),
        ));
        client_cfg.transport_config(Arc::new(transport));

        let conn = endpoint
            .connect_with(client_cfg, peer_addr, server_name)
            .map_err(|e| NetworkError::Connect(format!("connect: {e}")))?
            .await
            .map_err(|e| NetworkError::Connect(format!("handshake: {e}")))?;

        Ok(Self {
            inner: Arc::new(PeerConnInner {
                conn,
                request_id: AtomicU64::new(1),
            }),
        })
    }

    /// Send one request frame and await its matching response frame.
    /// Opens a fresh bi-stream per RPC; the server's lockstep dispatch
    /// returns exactly one response per stream.
    pub async fn request(
        &self,
        msg_type: MessageType,
        body: bytes::Bytes,
        response_type: MessageType,
        timeout: std::time::Duration,
    ) -> Result<bytes::Bytes, NetworkError> {
        let request_id = self.inner.request_id.fetch_add(1, Ordering::Relaxed);

        // Open a fresh bidirectional stream for this RPC.
        let (mut send, mut recv) = self
            .inner
            .conn
            .open_bi()
            .await
            .map_err(|e| NetworkError::Stream(format!("open_bi: {e}")))?;

        // Send the request frame.
        let req_frame = Frame::new_request(msg_type, request_id, body);
        let encoded = req_frame.encode().freeze();
        send.write_all(&encoded)
            .await
            .map_err(|e| NetworkError::Stream(format!("write: {e}")))?;
        send.finish()
            .map_err(|e| NetworkError::Stream(format!("finish: {e}")))?;

        // Read the response frame with the caller-supplied timeout.
        let response = tokio::time::timeout(timeout, Frame::read_async(&mut recv))
            .await
            .map_err(|_| NetworkError::Timeout)??;

        if response.msg_type != response_type {
            return Err(NetworkError::Decode(format!(
                "expected {response_type:?} got {:?}",
                response.msg_type
            )));
        }
        if response.request_id != request_id {
            return Err(NetworkError::Decode(format!(
                "request_id mismatch: expected {request_id} got {}",
                response.request_id
            )));
        }

        Ok(response.body)
    }
}

impl Clone for PeerConn {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
