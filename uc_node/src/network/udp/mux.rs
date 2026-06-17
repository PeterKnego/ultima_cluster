//! Shared-UDP-socket multiplexer: one `tokio::net::UdpSocket` per process,
//! demuxed to per-peer [`UdpSession`]s by `session_id`; RPC request/response
//! correlation by `Frame.request_id`.
//!
//! Session-id agreement: the initiator (client) derives a stable, order-
//! independent id from the local/peer socket-addr pair via
//! [`UdpMux::session_id_for`], XORs in its per-process `epoch`, and stamps the
//! result into every segment it sends. The receiver routes purely by the wire
//! `seg.session_id` (authoritative) — it never recomputes — so both ends key
//! the registry by the same id. The epoch makes a restarted node (same addr,
//! reset seq) present a NEW session_id, so survivors build a fresh session
//! rather than dropping its from-0 segments against a stale Reassembler.
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
#[cfg(feature = "fault-injection")]
use crate::raft::NodeId;

/// Server-side inbound-request handler. Sync to set (pull-forward of a later
/// fix that avoids a set-race and lets the round-trip test register without
/// `.await`); the returned `BoxFuture` is awaited on the recv path.
pub type Handler = Arc<dyn Fn(Frame) -> BoxFuture<'static, Frame> + Send + Sync>;

/// Registry entry: the session, its peer addr, and the per-session ticker's
/// `JoinHandle` (aborted on `shutdown`).
type SessionEntry = (Arc<UdpSession>, SocketAddr, tokio::task::JoinHandle<()>);

/// [`SessionTx`] over the shared socket to a fixed peer.
///
/// Holds a [`Weak`] to the socket — NOT a strong `Arc`. A session (hence its
/// `SocketTx`) can outlive `shutdown` (e.g. a detached route task parked in
/// `send_message`, or openraft replication issuing a late RPC after
/// `raft.shutdown()` returned). If `SocketTx` held a strong `Arc<UdpSocket>`,
/// any such straggler would pin the fd past rebind → `EADDRINUSE`. With a
/// `Weak`, once the mux drops its sole strong ref in `shutdown` the fd is
/// released immediately; stragglers upgrade-and-find-`None` and silently drop
/// their datagram (the node is going away anyway).
struct SocketTx {
    sock: std::sync::Weak<UdpSocket>,
    peer: SocketAddr,
}

#[async_trait::async_trait]
impl SessionTx for SocketTx {
    async fn send_to(&self, datagram: Bytes) {
        if let Some(sock) = self.sock.upgrade() {
            let _ = sock.send_to(&datagram, self.peer).await;
        }
    }
}

pub struct UdpMux {
    /// The single shared socket. Wrapped in an `Option` so `shutdown` can
    /// `take()` the mux's SOLE strong `Arc` and free the fd, even though the
    /// `UdpMux` struct itself may outlive shutdown (it is kept alive by the
    /// factory clients inside openraft's `Raft`, which can linger past
    /// `raft.shutdown()`). All other holders use a `Weak` (see `SocketTx`), so
    /// dropping this strong ref releases the fd promptly → no `EADDRINUSE` on a
    /// same-addr restart.
    sock: parking_lot::Mutex<Option<Arc<UdpSocket>>>,
    tuning: UdpTuning,
    /// Per-process session epoch, XORed into the initiator's outbound
    /// `session_id`. A restarted node binding the SAME addr gets a fresh random
    /// epoch → a NEW `session_id` for its outbound RPCs → survivors create a
    /// FRESH `UdpSession` (Reassembler at next=0) instead of silently dropping
    /// the restarted node's fresh-from-0 segments against the stale session.
    epoch: u32,
    /// Registry keyed by the wire `session_id`; value carries the peer addr and
    /// the per-session ticker's `JoinHandle` (so `shutdown` can abort it).
    sessions: Mutex<HashMap<u32, SessionEntry>>,
    /// In-flight RPCs awaiting a response, keyed by `Frame.request_id`.
    pending: Mutex<HashMap<u64, oneshot::Sender<Frame>>>,
    /// Server-side request handler. `parking_lot` so set/read are sync.
    handler: parking_lot::Mutex<Option<Handler>>,
    /// Recv-loop task handle, so `shutdown` can abort it and free the socket.
    recv_task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Detached inbound-routing tasks. Each captures an `Arc<UdpSession>` (→
    /// `SocketTx` → `Arc<UdpSocket>`) and may park indefinitely in
    /// `send_message`'s flow-control spin once the peer goes away — so they
    /// pin the socket fd unless aborted. Tracked here so `shutdown` can abort
    /// them; finished handles are pruned on each spawn to bound the vec.
    route_tasks: parking_lot::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// (test-only) This node's id (= `dst` for inbound segments) and the shared
    /// fault table. Consulted in the recv loop to drop/delay inbound DATA
    /// segments so the channel's NAK/retransmit path is exercised.
    #[cfg(feature = "fault-injection")]
    fault: parking_lot::Mutex<(NodeId, Option<Arc<crate::network::fault::FaultTable>>)>,
    /// (test-only) Maps a peer socket-addr to the source NodeId so an inbound
    /// segment can be keyed against the NodeId-keyed fault table.
    #[cfg(feature = "fault-injection")]
    peer_nodes: parking_lot::Mutex<HashMap<SocketAddr, NodeId>>,
}

impl UdpMux {
    pub async fn bind(addr: SocketAddr, tuning: UdpTuning) -> Result<Arc<Self>, NetworkError> {
        let sock = Arc::new(UdpSocket::bind(addr).await.map_err(NetworkError::Io)?);
        let mux = Arc::new(Self {
            sock: parking_lot::Mutex::new(Some(sock)),
            tuning,
            epoch: rand::random::<u32>().max(1), // never 0: epoch==0 ⇒ session_id==base ⇒ restart reuses stale session
            sessions: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            handler: parking_lot::Mutex::new(None),
            recv_task: parking_lot::Mutex::new(None),
            route_tasks: parking_lot::Mutex::new(Vec::new()),
            #[cfg(feature = "fault-injection")]
            fault: parking_lot::Mutex::new((0, None)),
            #[cfg(feature = "fault-injection")]
            peer_nodes: parking_lot::Mutex::new(HashMap::new()),
        });
        let recv_task = mux.clone().spawn_recv_loop();
        *mux.recv_task.lock() = Some(recv_task);
        Ok(mux)
    }

    /// Current strong `Arc` to the socket, or `None` once `shutdown` took it.
    fn sock(&self) -> Option<Arc<UdpSocket>> {
        self.sock.lock().clone()
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.sock()
            .ok_or_else(|| std::io::Error::other("udp mux socket closed"))?
            .local_addr()
    }

    /// Stop processing inbound segments AND release the socket fd so a restarted
    /// node can rebind the same addr.
    ///
    /// The hard part is that the `UdpMux` struct can outlive `shutdown`: the
    /// network factory clients live inside openraft's `Raft`, and openraft
    /// (alpha) can keep issuing outbound RPCs — and thus resurrect sessions —
    /// for a short while after `raft.shutdown()` returns. Each such client holds
    /// an `Arc<UdpMux>`, so the mux's refcount does not reach zero at rebind
    /// time. We therefore do NOT rely on the mux Arc dropping. Instead the mux
    /// holds the ONLY strong `Arc<UdpSocket>` (every `SocketTx` holds a `Weak`),
    /// so taking that strong ref here closes the fd immediately regardless of
    /// how many mux/session stragglers remain. A late RPC then upgrades the
    /// `Weak`, finds `None`, and silently drops its datagram.
    ///
    /// We also tear down the in-process state that pins live `Arc<UdpSession>`s
    /// (so they stop touching the socket and to bound resource use): drop the
    /// handler (cutting the mux→handler→`Arc<Raft>`→client→mux cycle), abort +
    /// join the recv loop and the per-session tickers, drain the session
    /// registry, and abort the detached route tasks. `abort()` only *schedules*
    /// cancellation, so we join to ensure those Arcs are actually dropped.
    pub async fn shutdown(&self) {
        // Free the fd up front: take the mux's sole strong socket Arc. Every
        // other holder is a `Weak`, so the underlying `UdpSocket` drops now and
        // the addr is immediately rebindable — even while mux/session
        // stragglers linger.
        let _sock = self.sock.lock().take();
        // Drop the handler, breaking the mux→handler→Arc<Raft>→client→mux cycle
        // so the factory clients (and their mux Arcs) can eventually drop.
        let _ = self.handler.lock().take();
        // Abort + join the recv loop (holds a `self: Arc<UdpMux>` clone).
        let recv = self.recv_task.lock().take();
        if let Some(h) = recv {
            h.abort();
            let _ = h.await;
        }
        // Abort + join each per-session ticker (each holds an `Arc<UdpSession>`),
        // then drain the registry (dropping its own session Arcs).
        let drained: Vec<_> = {
            let mut s = self.sessions.lock().await;
            s.drain().collect()
        };
        for (_sid, (_sess, _peer, ticker)) in drained {
            ticker.abort();
            let _ = ticker.await;
        }
        // Abort + join the detached route tasks (a parked one holds an
        // `Arc<UdpSession>`). Drain-then-join outside the lock.
        let route: Vec<_> = std::mem::take(&mut *self.route_tasks.lock());
        for h in route {
            h.abort();
            let _ = h.await;
        }
    }

    /// Register the server-side inbound-request handler. Sync.
    pub fn set_request_handler(&self, h: Handler) {
        *self.handler.lock() = Some(h);
    }

    /// (test-only) Install this node's id and the shared fault table so the
    /// recv loop can drop/delay inbound segments. Interior mutability, since
    /// the mux is held behind an `Arc` by the factory.
    #[cfg(feature = "fault-injection")]
    pub fn set_fault_injection(
        &self,
        source: NodeId,
        fault_table: Option<Arc<crate::network::fault::FaultTable>>,
    ) {
        *self.fault.lock() = (source, fault_table);
    }

    /// (test-only) Record `addr → node` so an inbound segment from `addr` can be
    /// keyed against the NodeId-keyed fault table.
    #[cfg(feature = "fault-injection")]
    pub fn register_peer(&self, addr: SocketAddr, node: NodeId) {
        self.peer_nodes.lock().insert(addr, node);
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
        if let Some((sess, _, _)) = s.get(&sid) {
            return sess.clone();
        }
        let tx = Arc::new(SocketTx {
            // Weak ref: a session must not pin the socket fd past shutdown.
            sock: self
                .sock
                .lock()
                .as_ref()
                .map(Arc::downgrade)
                .unwrap_or_default(),
            peer,
        });
        let sess = UdpSession::new(sid, tx, self.tuning.clone());

        // Spawn a detached periodic ticker for this session. The task holds an
        // Arc<UdpSession> clone and lives until the Arc's refcount drops to zero
        // (or the JoinHandle is aborted on shutdown). v1 has no session
        // eviction; the fixed small peer set bounds the number of live ticker
        // tasks — a documented tradeoff. The JoinHandle is stored in the
        // registry so `shutdown` can abort it (bounding resource use; the fd
        // itself is freed by taking the strong socket Arc — see `shutdown`).
        let ticker = sess.clone();
        let interval_ms = self
            .tuning
            .heartbeat_ms
            .min(self.tuning.sm_interval_ms)
            .max(1);
        let ticker_handle = tokio::spawn(async move {
            let mut ticker_int =
                tokio::time::interval(std::time::Duration::from_millis(interval_ms));
            ticker_int.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker_int.tick().await; // first tick fires immediately; skip it
            loop {
                ticker_int.tick().await;
                ticker.tick().await;
            }
        });

        s.insert(sid, (sess.clone(), peer, ticker_handle));
        sess
    }

    pub async fn open_session(
        &self,
        peer: SocketAddr,
        _app_id: &str,
    ) -> Result<Arc<UdpSession>, NetworkError> {
        // Initiator stamps `base ^ its_epoch`. The receiver adopts the wire id
        // verbatim (never recomputes), so no handshake is needed — and a
        // restart (fresh epoch) yields a fresh session_id, dodging the stale
        // Reassembler wedge.
        let sid = Self::session_id_for(self.local_addr()?, peer) ^ self.epoch;
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
        // Both the send AND the response wait are bounded by `timeout`. Without
        // this, `send_message` can park indefinitely in its flow-control spin
        // (waiting for an inbound Sm ack to drain the window) if the peer goes
        // silent with a full send window — the RPC would wedge with no escape.
        // If `timeout` fires while `send_message` is parked, nothing was
        // transmitted and cancellation is clean. If it fires mid-fragmentation
        // a partial run may have been sent; the peer reassembler holds an
        // incomplete message that is never completed — acceptable, because
        // openraft retries the whole AppendEntries as a fresh RPC with new
        // fragment IDs, and the orphaned partial is bounded.
        let outcome = tokio::time::timeout(timeout, async move {
            sess.send_message(encoded).await?;
            rx.await.map_err(|_| NetworkError::Disconnected)
        })
        .await;
        match outcome {
            Ok(Ok(frame)) => Ok(frame),
            Ok(Err(e)) => {
                self.pending.lock().await.remove(&request_id);
                Err(e)
            }
            Err(_elapsed) => {
                self.pending.lock().await.remove(&request_id);
                Err(NetworkError::Timeout)
            }
        }
    }

    fn spawn_recv_loop(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                // Re-fetch the socket each iteration rather than holding a
                // strong Arc for the loop's lifetime: shutdown takes the strong
                // ref to free the fd, and we must not pin it here. `None` ⇒
                // shut down ⇒ exit (the handle is also aborted on shutdown).
                let sock = match self.sock() {
                    Some(s) => s,
                    None => return,
                };
                let (n, peer) = match sock.recv_from(&mut buf).await {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let seg = match Segment::decode(&buf[..n]) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                // (test-only) Segment-level fault injection: drop or delay the
                // inbound segment per the NodeId-keyed fault table. Dropping a
                // DATA segment creates a gap → the receiver NAKs → the sender
                // retransmits, genuinely exercising the recovery path. A map
                // miss (peer not yet registered) ⇒ no fault (fail-open).
                // Faults ALL inbound segment types (DATA, NAK, SM, Heartbeat) —
                // realistic; restrict via seg.seg_type here if a test needs it.
                #[cfg(feature = "fault-injection")]
                {
                    let (source, table) = {
                        let g = self.fault.lock();
                        (g.0, g.1.clone())
                    };
                    if let Some(table) = table {
                        let src_node = self.peer_nodes.lock().get(&peer).copied();
                        if let Some(src_node) = src_node {
                            if table.should_drop(src_node, source, rand::random::<f64>()) {
                                continue;
                            }
                            let d = table.delay(src_node, source);
                            if d > 0 {
                                tokio::time::sleep(Duration::from_millis(d)).await;
                            }
                        }
                    }
                }
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
        let tracker = self.clone();
        let handle = tokio::spawn(async move {
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
        // Track the handle so `shutdown` can abort it. A handler reply can park
        // forever in `send_message`'s flow-control spin once the peer is gone,
        // pinning `sess` (→ socket fd); aborting on shutdown frees it. Prune
        // finished handles here to keep the vec bounded for a long-lived node.
        let mut v = tracker.route_tasks.lock();
        v.retain(|h| !h.is_finished());
        v.push(handle);
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
