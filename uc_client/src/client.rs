//! Public `Client` SDK for ultima_cluster (M4).

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::Mutex as PlMutex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::oneshot;

use uc_protocol::cnc::NodeStatus;
use uc_protocol::frames::client::{
    MSG_TYPE_CLIENT_QUERY, MSG_TYPE_CLIENT_QUERY_RESP, MSG_TYPE_NOT_LEADER_RESP, MSG_TYPE_SUBMIT,
    MSG_TYPE_SUBMIT_RESPONSE, encode_extra_client, encode_flags_client,
};
use uc_protocol::frames::query::QueryKind;
use uc_protocol::ring::RingError;

use crate::ClientError;
use crate::cnc::CncAttach;
use crate::rings::{ClientRings, InFlight, RawResponse, spawn_broadcast_reader};
use crate::session::SessionHandle;
use crate::watchers::{StallWatchers, spawn_stall_watchers};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const BACKPRESSURE_GRACE: Duration = Duration::from_secs(1);
const RING_FULL_RETRY: Duration = Duration::from_micros(100);

pub struct Client {
    cnc: Arc<CncAttach>,
    rings: PlMutex<ClientRings>,
    next_local_seq: AtomicU32,
    in_flight: InFlight,
    session: PlMutex<Option<SessionHandle>>,
    broadcast_reader: PlMutex<Option<crate::rings::BroadcastReaderHandle>>,
    watchers: PlMutex<Option<StallWatchers>>,
    shut_down: AtomicBool,
}

impl Client {
    pub async fn connect(instance_dir: &Path, app_id: &str) -> Result<Self, ClientError> {
        let cnc = Arc::new(CncAttach::attach(instance_dir, app_id)?);

        let clients_dir = instance_dir.join("clients");
        let (rings, response_consumer) = ClientRings::open(&clients_dir)?;
        let in_flight: InFlight = Arc::new(DashMap::new());

        let session = SessionHandle::create(&clients_dir.join("sessions.dir"), cnc.client_id)?;
        let broadcast_reader =
            spawn_broadcast_reader(response_consumer, cnc.client_id, in_flight.clone());

        // SAFETY: cnc Arc keeps the mmap alive until shutdown joins
        // the watchers via Drop or explicit shutdown().
        let watchers = unsafe {
            spawn_stall_watchers(cnc.node_status(), cnc.service_status())
        };

        Ok(Client {
            cnc,
            rings: PlMutex::new(rings),
            next_local_seq: AtomicU32::new(0),
            in_flight,
            session: PlMutex::new(Some(session)),
            broadcast_reader: PlMutex::new(Some(broadcast_reader)),
            watchers: PlMutex::new(Some(watchers)),
            shut_down: AtomicBool::new(false),
        })
    }

    pub fn client_id(&self) -> u32 {
        self.cnc.client_id
    }
    pub fn instance_id(&self) -> u128 {
        self.cnc.instance_id
    }

    pub fn current_leader(&self) -> Option<u64> {
        // SAFETY: cnc.node_status() returns a pointer valid for the cnc
        // mmap lifetime, which is tied to self.
        let ns: &NodeStatus = unsafe { &*self.cnc.node_status() };
        let id = ns.leader_node_id.load(Ordering::Relaxed);
        if id == u64::MAX { None } else { Some(id) }
    }

    pub fn last_applied(&self) -> u64 {
        let ns: &NodeStatus = unsafe { &*self.cnc.node_status() };
        ns.last_applied.load(Ordering::Relaxed)
    }

    pub async fn submit<C: Serialize, R: DeserializeOwned>(
        &self,
        cmd: &C,
    ) -> Result<R, ClientError> {
        if self.shut_down.load(Ordering::Relaxed) {
            return Err(ClientError::ShutDown);
        }
        let payload = bincode::serde::encode_to_vec(cmd, bincode::config::standard())?;
        let flags = encode_flags_client(0, None);
        let raw = self
            .send_and_await(MSG_TYPE_SUBMIT, payload, flags, /*on_query_ring*/ false)
            .await?;
        match raw {
            RawResponse::Record { msg_type: MSG_TYPE_SUBMIT_RESPONSE, payload } => {
                let (resp, _) = bincode::serde::decode_from_slice::<R, _>(
                    payload.as_ref(),
                    bincode::config::standard(),
                )?;
                Ok(resp)
            }
            RawResponse::Record { msg_type: MSG_TYPE_NOT_LEADER_RESP, payload } => {
                let (hint, _) = bincode::serde::decode_from_slice::<Option<u64>, _>(
                    payload.as_ref(),
                    bincode::config::standard(),
                )?;
                Err(ClientError::NotLeader { hint })
            }
            RawResponse::Record { msg_type, .. } => Err(ClientError::Decode(format!(
                "unexpected msg_type {msg_type} on submit response"
            ))),
            RawResponse::Overwritten => Err(ClientError::ResponseOverwritten),
            RawResponse::ShutDown => Err(ClientError::ShutDown),
        }
    }

    pub async fn query_snapshot<Q: Serialize, QR: DeserializeOwned>(
        &self,
        q: &Q,
    ) -> Result<QR, ClientError> {
        self.submit_query::<Q, QR>(q, QueryKind::Snapshot).await
    }

    pub async fn query_linearizable<Q: Serialize, QR: DeserializeOwned>(
        &self,
        q: &Q,
    ) -> Result<QR, ClientError> {
        self.submit_query::<Q, QR>(q, QueryKind::Linearizable).await
    }

    async fn submit_query<Q: Serialize, QR: DeserializeOwned>(
        &self,
        q: &Q,
        kind: QueryKind,
    ) -> Result<QR, ClientError> {
        if self.shut_down.load(Ordering::Relaxed) {
            return Err(ClientError::ShutDown);
        }
        let payload = bincode::serde::encode_to_vec(q, bincode::config::standard())?;
        let flags = encode_flags_client(0, Some(kind));
        let raw = self
            .send_and_await(MSG_TYPE_CLIENT_QUERY, payload, flags, /*on_query_ring*/ true)
            .await?;
        match raw {
            RawResponse::Record { msg_type: MSG_TYPE_CLIENT_QUERY_RESP, payload } => {
                let (resp, _) = bincode::serde::decode_from_slice::<QR, _>(
                    payload.as_ref(),
                    bincode::config::standard(),
                )?;
                Ok(resp)
            }
            RawResponse::Record { msg_type: MSG_TYPE_NOT_LEADER_RESP, payload } => {
                let (hint, _) = bincode::serde::decode_from_slice::<Option<u64>, _>(
                    payload.as_ref(),
                    bincode::config::standard(),
                )?;
                Err(ClientError::NotLeader { hint })
            }
            RawResponse::Record { msg_type, .. } => Err(ClientError::Decode(format!(
                "unexpected msg_type {msg_type} on query response"
            ))),
            RawResponse::Overwritten => Err(ClientError::ResponseOverwritten),
            RawResponse::ShutDown => Err(ClientError::ShutDown),
        }
    }

    async fn send_and_await(
        &self,
        msg_type: u16,
        payload: Vec<u8>,
        flags: u16,
        on_query_ring: bool,
    ) -> Result<RawResponse, ClientError> {
        let local_seq = self.next_local_seq.fetch_add(1, Ordering::Relaxed);
        let extra = encode_extra_client(self.cnc.client_id, local_seq);

        let (tx, mut rx): (oneshot::Sender<RawResponse>, oneshot::Receiver<RawResponse>) =
            oneshot::channel();
        self.in_flight.insert(local_seq, tx);

        // Write — retry on Full up to BACKPRESSURE_GRACE.
        let write_deadline = std::time::Instant::now() + BACKPRESSURE_GRACE;
        loop {
            let result = {
                let g = self.rings.lock();
                if on_query_ring {
                    g.query_producer
                        .try_write(msg_type, flags, extra, &payload)
                } else {
                    g.submit_producer
                        .try_write(msg_type, flags, extra, &payload)
                }
            };
            match result {
                Ok(()) => break,
                Err(RingError::Full) if std::time::Instant::now() < write_deadline => {
                    tokio::time::sleep(RING_FULL_RETRY).await;
                }
                Err(RingError::Full) => {
                    self.in_flight.remove(&local_seq);
                    return Err(ClientError::BackpressureFull);
                }
                Err(e) => {
                    self.in_flight.remove(&local_seq);
                    return Err(ClientError::Submission(format!("ring write: {e}")));
                }
            }
        }

        // Await response with stall + timeout selectors.
        let timeout = tokio::time::sleep(DEFAULT_REQUEST_TIMEOUT);
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                biased;
                resp = &mut rx => {
                    return match resp {
                        Ok(r) => Ok(r),
                        Err(_) => Err(ClientError::ShutDown), // sender dropped without sending
                    };
                }
                _ = &mut timeout => {
                    self.in_flight.remove(&local_seq);
                    return Err(ClientError::Timeout(DEFAULT_REQUEST_TIMEOUT));
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Check stall flags.
                    let watchers_g = self.watchers.lock();
                    if let Some(w) = watchers_g.as_ref() {
                        if w.node_stalled.load(Ordering::Relaxed) {
                            drop(watchers_g);
                            self.in_flight.remove(&local_seq);
                            return Err(ClientError::NodeStalled);
                        }
                        if w.service_stalled.load(Ordering::Relaxed) {
                            drop(watchers_g);
                            self.in_flight.remove(&local_seq);
                            return Err(ClientError::ServiceStalled);
                        }
                    }
                }
            }
        }
    }

    /// Stop background tasks without cleaning up the session file.
    ///
    /// Called automatically by `Drop`. Also called at the start of
    /// `shutdown()` so the async join can proceed on the already-stopped tasks.
    fn stop_background_tasks(&self) {
        self.shut_down.store(true, Ordering::Relaxed);

        // Stop the session heartbeat ticker. The session *file* is NOT removed
        // here — that is the caller's responsibility (done in `shutdown()`).
        // Stopping the ticker means the heartbeat_seq stops advancing, so the
        // node-side session_gc will unlink the file after STALE_AFTER.
        if let Some(s) = self.session.lock().as_ref() {
            s.stop.store(true, Ordering::Relaxed);
        }

        // Stop the broadcast reader and stall watchers.
        if let Some(r) = self.broadcast_reader.lock().as_ref() {
            r.stop.store(true, Ordering::Relaxed);
        }
        if let Some(w) = self.watchers.lock().as_ref() {
            w.stop.store(true, Ordering::Relaxed);
        }
    }

    pub async fn shutdown(self) -> Result<(), ClientError> {
        self.shut_down.store(true, Ordering::Relaxed);

        let session = self.session.lock().take();
        if let Some(s) = session {
            s.stop.store(true, Ordering::Relaxed);
            let _ = s.join.await;
            // Unlink the session file (best-effort).
            let _ = std::fs::remove_file(&s.path);
        }

        let reader = self.broadcast_reader.lock().take();
        if let Some(r) = reader {
            r.stop.store(true, Ordering::Relaxed);
            let _ = r.join.await;
        }

        let watchers = self.watchers.lock().take();
        if let Some(w) = watchers {
            w.stop.store(true, Ordering::Relaxed);
            let _ = w.join_node.await;
            let _ = w.join_service.await;
        }

        // Drain any leftover in-flights with ShutDown.
        let keys: Vec<u32> = self.in_flight.iter().map(|e| *e.key()).collect();
        for k in keys {
            if let Some((_, tx)) = self.in_flight.remove(&k) {
                let _ = tx.send(RawResponse::ShutDown);
            }
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test-helpers"))]
impl Client {
    /// Test-only: pause the broadcast reader so responses pile up in the
    /// broadcast ring without being consumed. Simulates a slow consumer.
    pub fn _test_pause_broadcast_reader(&self) {
        if let Some(r) = self.broadcast_reader.lock().as_ref() {
            r.paused.store(true, Ordering::Relaxed);
        }
    }

    /// Test-only: resume the broadcast reader paused via
    /// `_test_pause_broadcast_reader`.
    pub fn _test_resume_broadcast_reader(&self) {
        if let Some(r) = self.broadcast_reader.lock().as_ref() {
            r.paused.store(false, Ordering::Relaxed);
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // Signal all background tasks to stop. For the stall watchers we must
        // also abort their JoinHandles: the watchers hold `&'static` raw
        // pointers into the cnc mmap, and the mmap (owned by `self.cnc`) is
        // freed when *this* Drop returns. Setting the stop flag is not enough —
        // the tasks are polling in a sleep loop and might not observe the flag
        // before the next `await` yields control back to them after the mmap
        // has been freed. `JoinHandle::abort()` is sync and immediately
        // cancels the task at its next await point.
        self.stop_background_tasks();

        // Abort the watcher tasks to prevent use-after-free of the cnc mmap.
        if let Some(w) = self.watchers.lock().as_ref() {
            w.join_node.abort();
            w.join_service.abort();
        }
        // Abort the broadcast reader and session ticker as well (belt-and-
        // suspenders: their data is on the heap so the segfault risk is lower,
        // but aborting them avoids spurious wakeups after the client is gone).
        if let Some(r) = self.broadcast_reader.lock().as_ref() {
            r.join.abort();
        }
        if let Some(s) = self.session.lock().as_ref() {
            s.join.abort();
        }
    }
}
