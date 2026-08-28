// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The client SDK: submit commands and run reads against a cluster.
//!
//! Synchronous and blocking, over shared memory — no async runtime, no
//! `openraft`, no `quinn`. A client attaches to one node's instance directory;
//! writes are leader-only, [`Client::query_linearizable`] goes through the
//! node's quorum read barrier, and [`Client::query_snapshot`] is answered from
//! the local replica. See `docs/QUICKSTART.md` for a worked example.
//!
//! As of the pipelined-client rework (spec
//! `docs/superpowers/specs/2026-08-13-uc2-pipelined-client-design.md`),
//! `Client` is a thin blocking shim over [`crate::PipelinedClient`] — one
//! code path underlies both this blocking SDK and the pipelined SDK exposed
//! directly; there is no separate matcher thread or registration table.
//! [`Client::connect`] builds a [`crate::PipelinedConfig`] pinned to the
//! pre-rework `Client`'s observable behavior (`serving_gate: false` — writes
//! regardless of `CAN_SERVE`, learning `NOT_LEADER` from the wire, same as
//! before; `request_timeout` from `UC2_CLIENT_TIMEOUT_MS`, default 10s, read
//! at `connect`), and every call below is a one-line `submit(..)?.wait()`
//! (or the `query_*` equivalent) against the underlying engine.
//!
//! Spec: `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md`
//! §7; plan `docs/superpowers/plans/2026-07-11-uc2-m5-sdk.md` Task 10.

use std::path::Path;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::ClientError;
use crate::pipelined::{PipelinedClient, PipelinedConfig};
use crate::wait::WaitStrategy;

/// Default per-request timeout; override with `UC2_CLIENT_TIMEOUT_MS`.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Inflight window handed to the underlying engine — any value at least
/// covering the old unbounded behavior's practical use; blocking callers
/// rarely have more outstanding requests than caller threads.
const MAX_INFLIGHT: u32 = 1024;

/// Sync shmem client SDK — a thin blocking shim over [`PipelinedClient`] (see
/// the module docs). Cheap to clone-and-share via `Arc<Client>` for
/// concurrent `submit`/`query_*` calls from multiple threads (every method
/// here takes `&self`); `shutdown` takes `self` by value — the intended usage
/// is a single owner tearing the client down once every other caller is done.
pub struct Client {
    inner: PipelinedClient,
}

impl Client {
    /// Attach: see [`PipelinedClient::connect`] for the underlying attach-time
    /// contract (cnc app_id/protocol validation, egress subscription order
    /// before the driver thread spawns). Config is pinned to the pre-rework
    /// `Client`'s observable behavior — see the module docs.
    pub fn connect(instance_dir: &Path, app_id: &str) -> Result<Client, ClientError> {
        let request_timeout = std::env::var("UC2_CLIENT_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_REQUEST_TIMEOUT);
        let inner = PipelinedClient::connect(
            instance_dir,
            app_id,
            PipelinedConfig {
                driver_wait: WaitStrategy::Park,
                max_inflight: MAX_INFLIGHT,
                request_timeout,
                serving_gate: false, // pinned: pre-rework Client submits regardless of CAN_SERVE
            },
        )?;
        Ok(Client { inner })
    }

    pub fn client_id(&self) -> u32 {
        self.inner.client_id()
    }

    pub fn instance_id(&self) -> u128 {
        self.inner.instance_id()
    }

    /// Count of stale, kind-mismatched `MSG_V2_RESPONSE` records the engine
    /// has dropped (T14 defense in depth: a submit response delivered to a
    /// pending query, or vice versa — a cross-generation `(client_id,
    /// local_seq)` collision). A diagnostic stat: nonzero means the
    /// defense-in-depth kind check fired, so a stale response was correctly
    /// discarded rather than misrouted.
    pub fn kind_mismatch_drops(&self) -> u64 {
        self.inner.stats().kind_mismatch
    }

    /// The cnc page's current `leader_hint` (`None` = unknown).
    pub fn leader_hint(&self) -> Option<u32> {
        self.inner.leader_hint()
    }

    /// Submit a command; blocks until the matching commit response arrives
    /// (or an error — see [`ClientError`]).
    pub fn submit<C: Serialize, R: DeserializeOwned>(&self, cmd: &C) -> Result<R, ClientError> {
        self.inner.submit(cmd)?.wait()
    }

    /// Snapshot (non-linearizable) read.
    pub fn query_snapshot<Q: Serialize, QR: DeserializeOwned>(
        &self,
        q: &Q,
    ) -> Result<QR, ClientError> {
        self.inner.query_snapshot(q)?.wait()
    }

    /// Linearizable read (routed through the node's quorum read-index barrier).
    pub fn query_linearizable<Q: Serialize, QR: DeserializeOwned>(
        &self,
        q: &Q,
    ) -> Result<QR, ClientError> {
        self.inner.query_linearizable(q)?.wait()
    }

    /// M14b: the attached node's declared-FSM set (bit i ⇔ FSM i is
    /// declared), `0b1` on a single-service node.
    pub fn declared(&self) -> u64 {
        self.inner.declared()
    }

    /// M14b: submit a command; FSM `id` answers. Blocks like [`Self::submit`].
    pub fn submit_to<C: Serialize, R: DeserializeOwned>(
        &self,
        id: u8,
        cmd: &C,
    ) -> Result<R, ClientError> {
        self.inner.submit_to(id, cmd)?.wait()
    }

    /// M14b: submit a command and block for EVERY declared FSM's answer,
    /// ascending by service id.
    pub fn submit_all<C: Serialize, R: DeserializeOwned>(
        &self,
        cmd: &C,
    ) -> Result<Vec<(u8, R)>, ClientError> {
        self.inner.submit_all(cmd)?.wait()
    }

    /// M14b: snapshot (non-linearizable) read against FSM `id`.
    pub fn query_snapshot_on<Q: Serialize, QR: DeserializeOwned>(
        &self,
        id: u8,
        q: &Q,
    ) -> Result<QR, ClientError> {
        self.inner.query_snapshot_on(id, q)?.wait()
    }

    /// M14b: linearizable read against FSM `id` (quorum read barrier).
    pub fn query_linearizable_on<Q: Serialize, QR: DeserializeOwned>(
        &self,
        id: u8,
        q: &Q,
    ) -> Result<QR, ClientError> {
        self.inner.query_linearizable_on(id, q)?.wait()
    }

    /// Stop the driver thread and fail every still-inflight request with
    /// [`ClientError::ShutDown`].
    pub fn shutdown(self) {
        self.inner.shutdown();
    }
}
