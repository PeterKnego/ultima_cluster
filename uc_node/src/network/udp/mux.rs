//! Shared-UDP-socket multiplexer: one `tokio::net::UdpSocket` per process,
//! demuxed to per-peer [`UdpSession`]s by `session_id`; RPC request/response
//! correlation by `Frame.request_id`.
//!
//! Session-id agreement: the initiator (client) derives a stable, order-
//! independent id from the local/peer socket-addr pair via
//! [`UdpMux::session_id_for`] and stamps it into every segment it sends. The
//! receiver routes purely by the wire `seg.session_id` (authoritative) — it
//! never recomputes — so both ends key the registry by the same id.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, oneshot};

use super::UdpTuning;
use super::session::{SessionTx, UdpSession};
use super::wire::Segment;
use crate::network::NetworkError;
use crate::network::frame::Frame;

/// Server-side inbound-request handler. Sync to set (pull-forward of a later
/// fix that avoids a set-race and lets the round-trip test register without
/// `.await`); the returned `BoxFuture` is awaited on the recv path.
pub type Handler = Arc<dyn Fn(Frame) -> BoxFuture<'static, Frame> + Send + Sync>;

/// [`SessionTx`] over the shared socket to a fixed peer.
struct SocketTx {
    sock: Arc<UdpSocket>,
    peer: SocketAddr,
}

#[async_trait::async_trait]
impl SessionTx for SocketTx {
    async fn send_to(&self, datagram: Bytes) {
        let _ = self.sock.send_to(&datagram, self.peer).await;
    }
}

pub struct UdpMux {
    sock: Arc<UdpSocket>,
    tuning: UdpTuning,
    /// Registry keyed by the wire `session_id`; value carries the peer addr.
    sessions: Mutex<HashMap<u32, (Arc<UdpSession>, SocketAddr)>>,
    /// In-flight RPCs awaiting a response, keyed by `Frame.request_id`.
    pending: Mutex<HashMap<u64, oneshot::Sender<Frame>>>,
    /// Server-side request handler. `parking_lot` so set/read are sync.
    handler: parking_lot::Mutex<Option<Handler>>,
    /// Recv-loop task handle, so `shutdown` can abort it and free the socket.
    recv_task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl UdpMux {
    pub async fn bind(addr: SocketAddr, tuning: UdpTuning) -> Result<Arc<Self>, NetworkError> {
        let sock = Arc::new(UdpSocket::bind(addr).await.map_err(NetworkError::Io)?);
        let mux = Arc::new(Self {
            sock,
            tuning,
            sessions: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            handler: parking_lot::Mutex::new(None),
            recv_task: parking_lot::Mutex::new(None),
        });
        let recv_task = mux.clone().spawn_recv_loop();
        *mux.recv_task.lock() = Some(recv_task);
        Ok(mux)
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.sock.local_addr()
    }

    /// Abort the recv loop so a stopped node stops processing inbound segments.
    /// (Per-session ticker tasks and the socket `Arc` persist until process
    /// exit — an accepted v1 leak for a fixed peer set; aborting the recv loop
    /// is what matters to stop processing.)
    pub fn shutdown(&self) {
        if let Some(h) = self.recv_task.lock().take() {
            h.abort();
        }
    }

    /// Register the server-side inbound-request handler. Sync.
    pub fn set_request_handler(&self, h: Handler) {
        *self.handler.lock() = Some(h);
    }

    /// Order-independent session id so both ends agree: sort the addr pair,
    /// CRC32 over both string forms. Used by the client side only.
    fn session_id_for(local: SocketAddr, peer: SocketAddr) -> u32 {
        let (a, b) = if local <= peer {
            (local, peer)
        } else {
            (peer, local)
        };
        let mut h = crc32fast::Hasher::new();
        h.update(a.to_string().as_bytes());
        h.update(b.to_string().as_bytes());
        h.finalize()
    }

    async fn get_or_create_session(&self, sid: u32, peer: SocketAddr) -> Arc<UdpSession> {
        let mut s = self.sessions.lock().await;
        if let Some((sess, _)) = s.get(&sid) {
            return sess.clone();
        }
        let tx = Arc::new(SocketTx {
            sock: self.sock.clone(),
            peer,
        });
        let sess = UdpSession::new(sid, tx, self.tuning.clone());
        s.insert(sid, (sess.clone(), peer));

        // Spawn a detached periodic ticker for this session. The task holds an
        // Arc<UdpSession> clone and lives until the Arc's refcount drops to zero.
        // v1 has no session eviction; the fixed small peer set bounds the number
        // of live ticker tasks — a documented tradeoff.
        let ticker = sess.clone();
        let interval_ms = self
            .tuning
            .heartbeat_ms
            .min(self.tuning.sm_interval_ms)
            .max(1);
        tokio::spawn(async move {
            let mut ticker_int =
                tokio::time::interval(std::time::Duration::from_millis(interval_ms));
            ticker_int.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker_int.tick().await; // first tick fires immediately; skip it
            loop {
                ticker_int.tick().await;
                ticker.tick().await;
            }
        });

        sess
    }

    pub async fn open_session(
        &self,
        peer: SocketAddr,
        _app_id: &str,
    ) -> Result<Arc<UdpSession>, NetworkError> {
        let sid = Self::session_id_for(self.local_addr()?, peer);
        Ok(self.get_or_create_session(sid, peer).await)
    }

    pub async fn rpc(
        &self,
        peer: SocketAddr,
        app_id: &str,
        req: Frame,
        timeout: Duration,
    ) -> Result<Frame, NetworkError> {
        let request_id = req.request_id;
        let sess = self.open_session(peer, app_id).await?;
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(request_id, tx);
        let encoded = req.encode().freeze();
        if let Err(e) = sess.send_message(encoded).await {
            self.pending.lock().await.remove(&request_id);
            return Err(e);
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(frame)) => Ok(frame),
            Ok(Err(_)) => Err(NetworkError::Disconnected),
            Err(_) => {
                self.pending.lock().await.remove(&request_id);
                Err(NetworkError::Timeout)
            }
        }
    }

    fn spawn_recv_loop(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let (n, peer) = match self.sock.recv_from(&mut buf).await {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let seg = match Segment::decode(&buf[..n]) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let sid = seg.session_id;
                let sess = self.get_or_create_session(sid, peer).await;
                // Drive the session, then drain any completed inbound messages.
                sess.process(seg).await;
                while let Some(msg) = sess.try_recv_message() {
                    self.clone().route_inbound_message(sess.clone(), msg);
                }
            }
        })
    }

    fn route_inbound_message(self: Arc<Self>, sess: Arc<UdpSession>, msg: Bytes) {
        tokio::spawn(async move {
            let mut b = msg;
            let frame = match Frame::decode(&mut b) {
                Ok(f) => f,
                Err(_) => return,
            };
            if frame.is_response() {
                if let Some(tx) = self.pending.lock().await.remove(&frame.request_id) {
                    let _ = tx.send(frame);
                }
            } else if let Some(h) = {
                let g = self.handler.lock();
                g.clone()
            } {
                let resp = h(frame).await;
                let _ = sess.send_message(resp.encode().freeze()).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::frame::{Frame, MessageType};
    use bytes::Bytes;
    use std::time::Duration;

    #[tokio::test]
    async fn rpc_round_trip_over_loopback() {
        let server = UdpMux::bind("127.0.0.1:0".parse().unwrap(), UdpTuning::default())
            .await
            .unwrap();
        let server_addr = server.local_addr().unwrap();
        // Echo handler: respond with the same body, response flag set.
        server.set_request_handler(std::sync::Arc::new(|req: Frame| {
            Box::pin(async move {
                Frame::new_response(MessageType::AppendEntriesResp, req.request_id, req.body)
            })
        }));

        let client = UdpMux::bind("127.0.0.1:0".parse().unwrap(), UdpTuning::default())
            .await
            .unwrap();
        let req = Frame::new_request(
            MessageType::AppendEntriesReq,
            99,
            Bytes::from_static(b"ping"),
        );
        let resp = client
            .rpc(server_addr, "test-app", req, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(resp.request_id, 99);
        assert!(resp.is_response());
        assert_eq!(&resp.body[..], b"ping");
    }
}
