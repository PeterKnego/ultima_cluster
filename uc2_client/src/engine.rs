// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The pipelined engine (spec §4): bytes-level, io_uring-shaped attach +
//! send/poll split. [`Engine::attach`] opens a node's shmem IPC (cnc v2
//! page, ingress/query MPSC rings, node/service broadcast egress) under an
//! instance directory and returns a [`SendHalf`] (cheap to `Clone`, one per
//! submitter thread) paired with a single-owner [`PollHalf`] that drains
//! completions.
//!
//! ## The central contract
//!
//! **Every accepted `try_submit`/`try_query` produces exactly one completion
//! for its `user_data`, in bounded time.** All the hidden machinery — the
//! slot table's exactly-once resolution, the deadline sweep, drain-on-death —
//! exists to enforce this: nothing accepted may leak, double-complete, or
//! hang forever.
//!
//! ## The byte contract
//!
//! The engine itself is format-free: it never inspects submitted or returned
//! payload bytes, and the node/log treat `AppCommand` as opaque `Bytes`
//! end-to-end. The only format constraint is imposed by the OTHER endpoint —
//! the target service's apply boundary — and it belongs to `uc2_service`, not
//! to the engine: today's typed `StateMachine` trait runs bincode (standard
//! config) at the apply boundary, so against today's SDK, submitted bytes
//! must decode as `bincode(Command)` and query bytes as `bincode(Query)`.
//! `Outcome::Response` bytes (Task 4) are the egress payload with its
//! `position: u64 LE` prefix stripped; the body underneath is
//! `bincode(Response)` against today's SDK. See spec §4:
//! `docs/superpowers/specs/2026-08-13-uc2-pipelined-client-design.md`.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use uc2_log::cnc::CncPage;
use uc_protocol::ring::{BroadcastConsumer, BroadcastRing, MpscProducer, MpscRing, RingError};
use uc_protocol::v2::cnc::NODE_FLAG_CAN_SERVE;
use uc_protocol::v2::ipc::{FLAG_V2_LINEARIZABLE, MSG_V2_QUERY, MSG_V2_SUBMIT, extra_client};

use crate::error::ClientError;
use crate::slots::{ReqKind, SlotTable};

/// Well-known file names under the instance dir (the shared contract with
/// `uc2_node::InstanceDir` — see `uc2_node/src/ipc.rs`). Moved here from
/// `client.rs` in Task 3; `client.rs` now imports these rather than
/// redefining them.
pub(crate) const CNC_FILE: &str = "cnc2.dat";
pub(crate) const INGRESS_RING: &str = "ingress.ring";
pub(crate) const QUERY_RING: &str = "query.ring";
pub(crate) const EGRESS_SERVICE: &str = "egress_service.broadcast";
pub(crate) const EGRESS_NODE: &str = "egress_node.broadcast";

/// Engine attach configuration.
pub struct EngineConfig {
    /// Inflight window; the slot table sizes to the next power of two ≥ this.
    pub max_inflight: u32,
    /// Per-request deadline, enforced by the engine's deadline sweep (Task 4).
    pub request_timeout: Duration,
    /// Optional client-side payload cap, checked before the ring write so an
    /// oversized submit fails loud here instead of being silently dropped by
    /// the node. `None` leaves the bound to the ring's own `TooLarge`.
    pub max_payload: Option<usize>,
    /// Refuse `try_submit`/`try_query` when the node's `NODE_FLAG_CAN_SERVE`
    /// is clear (`SubmitError::NotServing`) instead of free-running into a
    /// dead/non-leader node.
    pub serving_gate: bool,
    /// Test hook: seed the slot table's sequence counter (default 0), used to
    /// exercise u32-wrap behavior deterministically. Not part of the stable
    /// public contract.
    #[doc(hidden)]
    pub start_seq: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            max_inflight: 4096,
            request_timeout: Duration::from_secs(10),
            max_payload: None,
            serving_gate: true,
            start_seq: 0,
        }
    }
}

/// Read consistency for [`SendHalf::try_query`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    /// Routed through the node's quorum read-index barrier.
    Linearizable,
    /// Answered from the local replica without a barrier round-trip.
    Snapshot,
}

/// Why a `try_submit`/`try_query` call was refused at the door. Refusal here
/// means the slot was never claimed (or was claimed and released) — the
/// caller's window/backpressure accounting is unaffected.
#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    #[error("backpressure: inflight window or ingress ring full")]
    Backpressure,
    #[error("node is not a serving leader (CAN_SERVE clear)")]
    NotServing,
    #[error("payload too large: {len} > {max}")]
    PayloadTooLarge { len: usize, max: usize },
    #[error("node instance restarted: attached {attached:#x}, now {current:#x}")]
    InstanceRestart { attached: u128, current: u128 },
    #[error("ring error: {0}")]
    Ring(RingError),
}

/// Per-field completion counters, `Relaxed`-loaded into an [`EngineStats`]
/// snapshot. Only `accepted` is written as of Task 3; the rest land as the
/// poll side (Task 4) is filled in.
#[derive(Default)]
struct StatCells {
    accepted: AtomicU64,
    responses: AtomicU64,
    duplicates: AtomicU64,
    kind_mismatch: AtomicU64,
    overwritten: AtomicU64,
    corrupt: AtomicU64,
    not_leader: AtomicU64,
    retry: AtomicU64,
    timed_out: AtomicU64,
    restarts: AtomicU64,
}

impl StatCells {
    fn snapshot(&self) -> EngineStats {
        EngineStats {
            accepted: self.accepted.load(Ordering::Relaxed),
            responses: self.responses.load(Ordering::Relaxed),
            duplicates: self.duplicates.load(Ordering::Relaxed),
            kind_mismatch: self.kind_mismatch.load(Ordering::Relaxed),
            overwritten: self.overwritten.load(Ordering::Relaxed),
            corrupt: self.corrupt.load(Ordering::Relaxed),
            not_leader: self.not_leader.load(Ordering::Relaxed),
            retry: self.retry.load(Ordering::Relaxed),
            timed_out: self.timed_out.load(Ordering::Relaxed),
            restarts: self.restarts.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time snapshot of an engine's counters (see [`SendHalf::stats`]).
#[derive(Debug, Default, Clone, Copy)]
pub struct EngineStats {
    pub accepted: u64,
    pub responses: u64,
    pub duplicates: u64,
    pub kind_mismatch: u64,
    pub overwritten: u64,
    pub corrupt: u64,
    pub not_leader: u64,
    pub retry: u64,
    pub timed_out: u64,
    pub restarts: u64,
}

/// State shared between a [`SendHalf`] (cloned, one per submitter thread) and
/// its single [`PollHalf`].
struct Shared {
    cnc: Arc<CncPage>,
    client_id: u32,
    instance_id: u128,
    table: SlotTable,
    stats: StatCells,
    dead: AtomicBool,
    restart: Mutex<Option<(u128, u128)>>,
    t0: Instant,
    timeout_ns: u64,
    max_payload: Option<usize>,
    serving_gate: bool,
}

/// Namespace for [`Engine::attach`].
pub struct Engine;

/// The submit side: `&self`, nonblocking, never sleeps. Cheap to `Clone` (one
/// clone per submitter thread — `MpscProducer`'s per-clone write-position
/// cache makes `SendHalf` `Send` but not `Sync`; the supported usage is to
/// clone rather than share one `&SendHalf` across threads).
pub struct SendHalf {
    shared: Arc<Shared>,
    ingress: MpscProducer,
    query: MpscProducer,
}

/// The completion side: single owner, `Send`. `poll` (Task 4) drains
/// completions in one bounded, zero-alloc duty cycle.
pub struct PollHalf {
    #[allow(dead_code)] // read starting Task 4
    shared: Arc<Shared>,
    #[allow(dead_code)] // read starting Task 4
    egress_service: BroadcastConsumer,
    #[allow(dead_code)] // read starting Task 4
    egress_node: BroadcastConsumer,
    #[allow(dead_code)] // scratch decode buffer, used starting Task 4
    buf: Vec<u8>,
    #[allow(dead_code)] // duty-cycle counter, used starting Task 4
    cycle: u64,
}

impl Engine {
    /// Attach to a running node's instance directory: open the cnc page
    /// (validates `app_id`/protocol version), allocate `client_id` off
    /// `next_client_id`, open the ingress/query MPSC producers, and subscribe
    /// both egress broadcasts BEFORE returning — so nothing published from
    /// this point on is missable (mirrors `Client::connect`'s attach order).
    pub fn attach(
        instance_dir: &Path,
        app_id: &str,
        cfg: EngineConfig,
    ) -> Result<(SendHalf, PollHalf), ClientError> {
        let cnc = CncPage::open_file(&instance_dir.join(CNC_FILE), app_id)?;
        let client_id = cnc.status().next_client_id.fetch_add(1) as u32;
        let instance_id = cnc.meta().instance_id;
        let (ingress, _ic) = MpscRing::open(&instance_dir.join(INGRESS_RING))?.into_split();
        let (query, _qc) = MpscRing::open(&instance_dir.join(QUERY_RING))?.into_split();
        let egress_service = BroadcastRing::open(&instance_dir.join(EGRESS_SERVICE))?.subscribe();
        let egress_node = BroadcastRing::open(&instance_dir.join(EGRESS_NODE))?.subscribe();
        let shared = Arc::new(Shared {
            cnc,
            client_id,
            instance_id,
            table: SlotTable::new(cfg.max_inflight, cfg.start_seq),
            stats: StatCells::default(),
            dead: AtomicBool::new(false),
            restart: Mutex::new(None),
            t0: Instant::now(),
            timeout_ns: cfg.request_timeout.as_nanos() as u64,
            max_payload: cfg.max_payload,
            serving_gate: cfg.serving_gate,
        });
        Ok((
            SendHalf { shared: Arc::clone(&shared), ingress, query },
            PollHalf { shared, egress_service, egress_node, buf: Vec::new(), cycle: 0 },
        ))
    }
}

impl SendHalf {
    fn send(
        &self,
        ring: &MpscProducer,
        msg_type: u16,
        flags: u16,
        kind: ReqKind,
        user_data: u64,
        bytes: &[u8],
    ) -> Result<(), SubmitError> {
        let s = &self.shared;
        if s.dead.load(Ordering::Acquire) {
            let (attached, current) = s.restart.lock().unwrap().unwrap_or((s.instance_id, 0));
            return Err(SubmitError::InstanceRestart { attached, current });
        }
        if s.serving_gate && s.cnc.status().flags.load_acquire() & NODE_FLAG_CAN_SERVE == 0 {
            return Err(SubmitError::NotServing);
        }
        if let Some(max) = s.max_payload
            && bytes.len() > max
        {
            return Err(SubmitError::PayloadTooLarge { len: bytes.len(), max });
        }
        let deadline_ns = s.t0.elapsed().as_nanos() as u64 + s.timeout_ns;
        let seq = s
            .table
            .claim(user_data, kind, deadline_ns)
            .map_err(|_| SubmitError::Backpressure)?; // WindowFull and SlotBusy alike
        match ring.try_write(msg_type, flags, extra_client(s.client_id, seq as u32), bytes) {
            Ok(()) => {
                s.stats.accepted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                s.table.release(seq);
                match e {
                    RingError::Full => Err(SubmitError::Backpressure),
                    RingError::TooLarge { len, max } => {
                        Err(SubmitError::PayloadTooLarge { len, max })
                    }
                    other => Err(SubmitError::Ring(other)),
                }
            }
        }
    }

    /// Submit a command; nonblocking. See the module's central contract: an
    /// `Ok(())` here obligates the engine to eventually deliver exactly one
    /// completion for `user_data` via [`PollHalf::poll`] (Task 4).
    pub fn try_submit(&self, user_data: u64, cmd_bytes: &[u8]) -> Result<(), SubmitError> {
        self.send(&self.ingress, MSG_V2_SUBMIT, 0, ReqKind::Submit, user_data, cmd_bytes)
    }

    /// Issue a read; nonblocking. `query_bytes` must decode as
    /// `bincode(Query)` against today's SDK (see the module's byte contract).
    pub fn try_query(
        &self,
        user_data: u64,
        query_bytes: &[u8],
        c: Consistency,
    ) -> Result<(), SubmitError> {
        let flags = match c {
            Consistency::Linearizable => FLAG_V2_LINEARIZABLE,
            Consistency::Snapshot => 0,
        };
        self.send(&self.query, MSG_V2_QUERY, flags, ReqKind::Query, user_data, query_bytes)
    }

    pub fn client_id(&self) -> u32 {
        self.shared.client_id
    }

    pub fn instance_id(&self) -> u128 {
        self.shared.instance_id
    }

    /// The cnc page's current `leader_hint` (`u64::MAX` sentinel → `None`).
    pub fn leader_hint(&self) -> Option<u32> {
        let hint = self.shared.cnc.status().leader_hint.load_acquire();
        if hint == u64::MAX { None } else { Some(hint as u32) }
    }

    /// Whether the node's `NODE_FLAG_CAN_SERVE` is currently set.
    pub fn can_serve(&self) -> bool {
        self.shared.cnc.status().flags.load_acquire() & NODE_FLAG_CAN_SERVE != 0
    }

    /// A point-in-time snapshot of this engine's counters.
    pub fn stats(&self) -> EngineStats {
        self.shared.stats.snapshot()
    }

    /// Current inflight count (claimed, not-yet-completed slots).
    pub fn inflight(&self) -> u64 {
        self.shared.table.inflight()
    }
}

impl Clone for SendHalf {
    fn clone(&self) -> Self {
        SendHalf {
            shared: Arc::clone(&self.shared),
            ingress: self.ingress.clone(), // per-clone producer cache (MpscProducer contract)
            query: self.query.clone(),
        }
    }
}

impl PollHalf {
    /// Drain and dispatch completions in one bounded duty cycle. Stub as of
    /// Task 3 — always reports zero; Task 4 fills in the real drain
    /// (egress broadcast decode, slot resolution, deadline sweep).
    pub fn poll(&mut self) -> usize {
        0
    }
}
