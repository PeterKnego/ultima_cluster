// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The sync shmem client (M5 Task 10, spec §7). Attaches to a running node's
//! cnc v2 page + IPC rings under `instance_dir`, allocates a `client_id` off
//! the cnc page's `next_client_id` counter, and drives `submit`/`query_*` as
//! blocking calls over the MPSC ingress/query rings + a broadcast matcher
//! thread that correlates answers back by `(client_id, local_seq)`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde::de::DeserializeOwned;

use uc2_log::agent::AgentRunner;
use uc2_log::cnc::CncPage;
use uc_protocol::ring::{BroadcastRing, MpscProducer, MpscRing, RingError};
use uc_protocol::v2::ipc::{FLAG_V2_LINEARIZABLE, MSG_V2_QUERY, MSG_V2_SUBMIT, extra_client};

use crate::engine::{CNC_FILE, EGRESS_NODE, EGRESS_SERVICE, INGRESS_RING, QUERY_RING};
use crate::error::ClientError;
use crate::matcher::{
    Pending, RawResp, RegKind, Registrations, decode_response, drain_with_shutdown, spawn_matcher,
};

/// Default per-request timeout; override with `UC2_CLIENT_TIMEOUT_MS`.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How long `submit`/`query_*` retries a `RingError::Full` ingress/query
/// write before giving up with [`ClientError::BackpressureFull`].
const BACKPRESSURE_GRACE: Duration = Duration::from_secs(1);
const RING_FULL_RETRY: Duration = Duration::from_micros(100);

/// Sync shmem client SDK. Cheap to clone-and-share via `Arc<Client>` for
/// concurrent `submit`/`query_*` calls from multiple threads (every method
/// here takes `&self`); `shutdown` takes `self` by value — the intended usage
/// is a single owner tearing the client down once every other caller is done.
pub struct Client {
    cnc: Arc<CncPage>,
    client_id: u32,
    instance_id: u128,
    ingress: Mutex<MpscProducer>,
    query_ring: Mutex<MpscProducer>,
    next_local_seq: AtomicU32,
    registrations: Registrations,
    /// Count of stale kind-mismatched `MSG_V2_RESPONSE` records the matcher
    /// dropped (T14 defense in depth). Shared with the matcher thread; exposed
    /// via [`Client::kind_mismatch_drops`].
    kind_mismatch_drops: Arc<AtomicU64>,
    /// `None` only after `shutdown` has taken it (never observable from
    /// outside this module: `shutdown` consumes `self`).
    matcher: Option<AgentRunner>,
    request_timeout: Duration,
}

impl Client {
    /// Attach: open the cnc page (validates `app_id`/protocol version),
    /// allocate `client_id` via `next_client_id.fetch_add`, open the
    /// ingress/query MPSC producers and subscribe both egress broadcasts
    /// (BEFORE spawning the matcher, so no answer published from this point
    /// on is missed), then spawn the matcher thread.
    pub fn connect(instance_dir: &Path, app_id: &str) -> Result<Client, ClientError> {
        let cnc = CncPage::open_file(&instance_dir.join(CNC_FILE), app_id)?;
        let client_id = cnc.status().next_client_id.fetch_add(1) as u32;
        let instance_id = cnc.meta().instance_id;

        let (ingress_producer, _ingress_consumer) =
            MpscRing::open(&instance_dir.join(INGRESS_RING))?.into_split();
        let (query_producer, _query_consumer) =
            MpscRing::open(&instance_dir.join(QUERY_RING))?.into_split();

        let egress_service = BroadcastRing::open(&instance_dir.join(EGRESS_SERVICE))?.subscribe();
        let egress_node = BroadcastRing::open(&instance_dir.join(EGRESS_NODE))?.subscribe();

        let registrations: Registrations = Arc::new(Mutex::new(HashMap::new()));
        let kind_mismatch_drops = Arc::new(AtomicU64::new(0));
        // `AgentRunner::spawn` only fails on OS thread-spawn exhaustion; there
        // is no dedicated `ClientError` variant for that (near-impossible in
        // practice), so it rides along on `Ring` via `RingError::Io` — the
        // closest existing "attach-time io problem" bucket.
        let matcher = spawn_matcher(
            client_id,
            egress_service,
            egress_node,
            Arc::clone(&registrations),
            Arc::clone(&kind_mismatch_drops),
        )
        .map_err(RingError::Io)?;

        Ok(Client {
            cnc,
            client_id,
            instance_id,
            ingress: Mutex::new(ingress_producer),
            query_ring: Mutex::new(query_producer),
            next_local_seq: AtomicU32::new(0),
            registrations,
            kind_mismatch_drops,
            matcher: Some(matcher),
            request_timeout: read_request_timeout(),
        })
    }

    pub fn client_id(&self) -> u32 {
        self.client_id
    }

    pub fn instance_id(&self) -> u128 {
        self.instance_id
    }

    /// Number of stale, kind-mismatched `MSG_V2_RESPONSE` records the matcher
    /// has dropped (a submit response delivered to a pending query, or vice
    /// versa — a T14 cross-generation `(client_id, local_seq)` collision). A
    /// diagnostic stat: nonzero means the defense-in-depth kind check fired, so
    /// a stale response was correctly discarded rather than misrouted.
    pub fn kind_mismatch_drops(&self) -> u64 {
        self.kind_mismatch_drops.load(Ordering::Relaxed)
    }

    /// The cnc page's current `leader_hint` (`u64::MAX` sentinel → `None`).
    pub fn leader_hint(&self) -> Option<u32> {
        let hint = self.cnc.status().leader_hint.load_acquire();
        if hint == u64::MAX { None } else { Some(hint as u32) }
    }

    /// Submit a command; blocks until the matching commit response arrives
    /// (or an error — see [`ClientError`]).
    pub fn submit<C: Serialize, R: DeserializeOwned>(&self, cmd: &C) -> Result<R, ClientError> {
        let payload = bincode::serde::encode_to_vec(cmd, bincode::config::standard())
            .map_err(|e| ClientError::Decode(e.to_string()))?;
        self.send_and_await(&self.ingress, MSG_V2_SUBMIT, 0, RegKind::Submit, &payload)
    }

    /// Snapshot (non-linearizable) read.
    pub fn query_snapshot<Q: Serialize, QR: DeserializeOwned>(
        &self,
        q: &Q,
    ) -> Result<QR, ClientError> {
        self.query(q, 0)
    }

    /// Linearizable read (routed through the node's read-index barrier).
    pub fn query_linearizable<Q: Serialize, QR: DeserializeOwned>(
        &self,
        q: &Q,
    ) -> Result<QR, ClientError> {
        self.query(q, FLAG_V2_LINEARIZABLE)
    }

    fn query<Q: Serialize, QR: DeserializeOwned>(
        &self,
        q: &Q,
        flags: u16,
    ) -> Result<QR, ClientError> {
        let payload = bincode::serde::encode_to_vec(q, bincode::config::standard())
            .map_err(|e| ClientError::Decode(e.to_string()))?;
        self.send_and_await(&self.query_ring, MSG_V2_QUERY, flags, RegKind::Query, &payload)
    }

    /// The shared send/register/await core for both `submit` and `query_*`:
    /// allocate the next `local_seq`, register a response channel, write to
    /// `ring` (retrying `RingError::Full` for up to [`BACKPRESSURE_GRACE`]),
    /// then block on the channel for up to `self.request_timeout`. On
    /// timeout, re-reads the cnc header: a changed `instance_id` means the
    /// node restarted mid-flight ([`ClientError::InstanceRestart`]) rather
    /// than a plain [`ClientError::Timeout`].
    fn send_and_await<R: DeserializeOwned>(
        &self,
        ring: &Mutex<MpscProducer>,
        msg_type: u16,
        flags: u16,
        kind: RegKind,
        payload: &[u8],
    ) -> Result<R, ClientError> {
        let local_seq = self.next_local_seq.fetch_add(1, Ordering::Relaxed);
        let extra = extra_client(self.client_id, local_seq);

        let (tx, rx) = mpsc::sync_channel::<RawResp>(1);
        self.registrations.lock().unwrap().insert(local_seq, Pending { kind, tx });

        let write_deadline = Instant::now() + BACKPRESSURE_GRACE;
        loop {
            let result = ring.lock().unwrap().try_write(msg_type, flags, extra, payload);
            match result {
                Ok(()) => break,
                Err(RingError::Full) if Instant::now() < write_deadline => {
                    std::thread::sleep(RING_FULL_RETRY);
                }
                Err(RingError::Full) => {
                    self.registrations.lock().unwrap().remove(&local_seq);
                    return Err(ClientError::BackpressureFull);
                }
                Err(e) => {
                    self.registrations.lock().unwrap().remove(&local_seq);
                    return Err(ClientError::Ring(e));
                }
            }
        }

        match rx.recv_timeout(self.request_timeout) {
            Ok(raw) => decode_response(raw),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.registrations.lock().unwrap().remove(&local_seq);
                // Non-panicking header re-read (M5 final review #2b): the node
                // may be recreating the cnc file IN PLACE right now (truncate →
                // set_len → rewrite header), so our mmap can observe a zeroed or
                // torn header. `meta()` would `expect`-panic on that — a panic
                // reachable from a plain client API call by a concurrent node
                // restart. `try_instance_id` returns `None` for that torn
                // window; we classify it as `InstanceRestart` (the node IS being
                // recreated — the more accurate signal than a bare timeout), with
                // `current: 0` as the documented "header unreadable / node
                // mid-recreate" sentinel. A clean read that differs is a genuine
                // restart; a clean matching read is an honest `Timeout`.
                match self.cnc.try_instance_id() {
                    Some(current) if current != self.instance_id => {
                        Err(ClientError::InstanceRestart { attached: self.instance_id, current })
                    }
                    None => {
                        Err(ClientError::InstanceRestart { attached: self.instance_id, current: 0 })
                    }
                    Some(_) => Err(ClientError::Timeout(self.request_timeout)),
                }
            }
            // Unreachable in normal operation: the one `SyncSender` lives in
            // the registration table until either it sends (this arm never
            // fires — the value is already buffered for `recv`) or a
            // write-failure path above removes-and-drops it BEFORE we ever
            // call `recv_timeout`. Kept as a safe fallback, not a panic.
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(ClientError::ShutDown),
        }
    }

    /// Stop the matcher thread, then fail every still-registered in-flight
    /// request with [`ClientError::ShutDown`].
    pub fn shutdown(mut self) {
        if let Some(m) = self.matcher.take() {
            m.stop();
        }
        drain_with_shutdown(&self.registrations);
    }
}

fn read_request_timeout() -> Duration {
    std::env::var("UC2_CLIENT_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_REQUEST_TIMEOUT)
}
