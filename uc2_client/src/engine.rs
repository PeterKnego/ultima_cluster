// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The pipelined engine (spec §4): bytes-level, io_uring-shaped attach +
//! send/poll split. [`Engine::attach`] opens a node's shmem IPC (cnc v2
//! page, ingress/query MPSC rings, the node broadcast and — M14b — one
//! service broadcast per declared FSM) under an instance directory and
//! returns a [`SendHalf`] (cheap to `Clone`, one per submitter thread)
//! paired with a single-owner [`PollHalf`] that drains completions.
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

use std::cell::RefCell;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use uc2_log::cnc::CncPage;
use uc_protocol::ring::{
    BroadcastConsumer, BroadcastRing, MpscProducer, MpscRing, RecordHeader, RingError,
};
use uc_protocol::v2::cnc::{CNC_MAX_SERVICES, NODE_FLAG_CAN_SERVE};
use uc_protocol::v2::ipc::{
    FLAG_V2_IS_QUERY, FLAG_V2_LINEARIZABLE, MSG_V2_BAD_SERVICE, MSG_V2_NOT_LEADER, MSG_V2_QUERY,
    MSG_V2_RESPONSE, MSG_V2_RETRY, MSG_V2_SUBMIT, client_from_extra, extra_client,
    write_query_payload,
};

use crate::error::ClientError;
use crate::slots::{ReqKind, Resolve, SlotTable};

/// Well-known file names under the instance dir (the shared contract with
/// `uc2_node::InstanceDir` — see `uc2_node/src/ipc.rs`). Moved here from
/// `client.rs` in Task 3; `client.rs` now imports these rather than
/// redefining them.
pub(crate) const CNC_FILE: &str = "cnc2.dat";
pub(crate) const INGRESS_RING: &str = "ingress.ring";
pub(crate) const QUERY_RING: &str = "query.ring";
pub(crate) const EGRESS_NODE: &str = "egress_node.broadcast";

/// M14b: FSM `id`'s response ring. The engine opens one per declared id.
pub(crate) fn egress_service_ring(id: u8) -> String {
    format!("egress_service.{id}.broadcast")
}

/// Engine attach configuration.
pub struct EngineConfig {
    /// Inflight window; the slot table sizes to the next power of two ≥ this.
    pub max_inflight: u32,
    /// Per-request deadline, enforced by the engine's deadline sweep (Task 4).
    pub request_timeout: Duration,
    /// Client-side payload cap, checked before the ring write so an oversized
    /// submit fails loud here instead of being silently dropped downstream.
    /// `None` (the default) INHERITS the attached node's own bound —
    /// `cnc.meta().max_payload` — at `Engine::attach` time; `Some(n)` is an
    /// explicit override. Inheriting matters because the node's bound is
    /// typically MTU-bounded (a few hundred bytes — well under the ring's own
    /// ~64 KiB `TooLarge` ceiling): without it, a submit that clears the
    /// ring's door but exceeds the node's own `max_payload` is silently
    /// dropped by the node rather than rejected here, and the caller only
    /// finds out via the request timing out.
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
/// means the slot was never claimed (or was claimed and SUCCESSFULLY
/// released) — the caller's window/backpressure accounting is unaffected. A
/// claim whose release LOST the race (some poller already completed the
/// slot first — see `finish_write`'s doc comment) is never surfaced as a
/// refusal; that path reports `Ok(())` instead, because a real completion
/// already exists for it.
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
    /// M14b: `id` is not in the attached node's declared set (or `>= 8`).
    /// Refused at the door — no slot claimed, nothing written.
    #[error("service id {id} is not declared on this node (declared set 0b{declared:b})")]
    ServiceNotDeclared { id: u8, declared: u64 },
}

/// Per-field completion counters, `Relaxed`-loaded into an [`EngineStats`]
/// snapshot. All counters are wired (submit side: `accepted`; poll side:
/// the rest, via `handle_record`/`maintenance`).
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
    wrong_ring: AtomicU64,
    bad_service: AtomicU64,
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
            wrong_ring: self.wrong_ring.load(Ordering::Relaxed),
            bad_service: self.bad_service.load(Ordering::Relaxed),
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
    /// M14b: a RESPONSE from a ring the request did not expect — a sibling
    /// FSM's answer to a request that named another; dropped.
    pub wrong_ring: u64,
    /// M14b: `MSG_V2_BAD_SERVICE` answers (the node has no ring for the id).
    pub bad_service: u64,
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
    /// M14b: bit `i` set ⇔ FSM `i` exists on the attached node. A page
    /// reading 0 (a harness node) folds to `0b1`.
    declared: u64,
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
    /// M14b: assembly buffer for a query's `service_id ++ query` wire payload
    /// (one `try_write` takes one slice). `SendHalf` is `!Sync`, so this
    /// `RefCell` is never contended.
    scratch: RefCell<Vec<u8>>,
}

/// The completion side: single owner, `Send`. `poll` drains completions in
/// one bounded duty cycle (zero-alloc except a fan-in's buffered pieces).
pub struct PollHalf {
    shared: Arc<Shared>,
    /// One consumer per declared FSM, ascending id (FSM 0 first).
    egress_services: Vec<(u8, BroadcastConsumer)>,
    egress_node: BroadcastConsumer,
    buf: Vec<u8>,
    /// M14b fan-in buffer, one entry per slot index: the pieces of a
    /// `try_submit_all` that have arrived so far. Keyed by the slot's wire
    /// seq so a reused index never mixes generations; a swept slot's stale
    /// pieces stay until the index is reused (bounded: ≤ 8 pieces per slot).
    fanin: Vec<FanIn>,
    cycle: u64,
}

/// The pieces of one in-flight fan-in, buffered until the last one lands.
#[derive(Default)]
struct FanIn {
    seq: u32,
    position: u64,
    /// `Bytes`, not `Vec<u8>`: refcounted from here to the caller (deviation 1).
    parts: Vec<(u8, Bytes)>,
}

impl FanIn {
    /// Record one piece; a different wire seq means the slot index was
    /// reused — start over for the new generation.
    fn push_piece(&mut self, seq: u32, position: u64, ring: u8, body: &[u8]) {
        if self.seq != seq || self.parts.is_empty() {
            self.seq = seq;
            self.position = position;
            self.parts.clear();
        }
        self.parts.push((ring, Bytes::copy_from_slice(body)));
    }
}

/// One resolved completion, handed to the callback passed to [`PollHalf::poll`].
/// `position` is `Some` only for [`Outcome::Response`] and
/// [`Outcome::Responses`] (the wire's `u64` LE prefix, stripped from
/// `outcome`'s borrowed payload; for a fan-in, the first piece to arrive —
/// one submitted frame commits at one position, so every piece carries it).
pub struct Completion<'a> {
    pub user_data: u64,
    pub position: Option<u64>,
    pub outcome: Outcome<'a>,
}

/// What became of a request. See the module's central contract: every
/// accepted `try_submit`/`try_query` produces exactly one of these.
#[derive(Debug)]
pub enum Outcome<'a> {
    /// A `MSG_V2_RESPONSE` payload, borrowed from the engine's read buffer
    /// (the wire's `position: u64 LE` prefix already stripped).
    Response(&'a [u8]),
    /// M14b: a completed `try_submit_all` — one `(service_id, response)` per
    /// declared FSM, ascending by id, all for the same log position. `Bytes`
    /// so the pieces travel refcounted to the caller (deviation 1).
    Responses(&'a [(u8, Bytes)]),
    /// A `MSG_V2_NOT_LEADER` redirect; `hint` is `None` when the node doesn't
    /// know the current leader (wire sentinel `u64::MAX`).
    NotLeader { hint: Option<u32> },
    /// A `MSG_V2_RETRY` transient signal: no side effect happened yet.
    Retry,
    /// M14b: the node has no ring for the requested id (`MSG_V2_BAD_SERVICE`).
    /// Pre-side-effect, like `Retry`.
    BadService { id: u8 },
    /// The per-request deadline elapsed with no answer (includes anything
    /// lost to a broadcast overwrite — see `drain_ring`'s `Overwritten` arm).
    TimedOut,
    /// The node's instance restarted while this request was in flight.
    InstanceRestart { attached: u128, current: u128 },
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
        // Door default: explicit `Some` overrides; `None` inherits the
        // attached node's own bound from the cnc page (see EngineConfig::
        // max_payload's doc for why this matters).
        let max_payload = cfg.max_payload.or(Some(cnc.meta().max_payload as usize));
        let (ingress, _ic) = MpscRing::open(&instance_dir.join(INGRESS_RING))?.into_split();
        let (query, _qc) = MpscRing::open(&instance_dir.join(QUERY_RING))?.into_split();
        // M14b: which FSMs exist here. A page reading 0 is a harness node
        // (nothing declared, FSM 0 ringed) — the same fold every attacher uses.
        let declared = match cnc.services_declared() {
            0 => 0b1,
            d => d,
        };
        let mut egress_services = Vec::new();
        for id in 0..CNC_MAX_SERVICES as u8 {
            if declared & (1u64 << id) != 0 {
                let ring =
                    BroadcastRing::open(&instance_dir.join(egress_service_ring(id)))?.subscribe();
                egress_services.push((id, ring));
            }
        }
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
            max_payload,
            serving_gate: cfg.serving_gate,
            declared,
        });
        let slots = shared.table.slot_count();
        Ok((
            SendHalf {
                shared: Arc::clone(&shared),
                ingress,
                query,
                scratch: RefCell::new(Vec::new()),
            },
            PollHalf {
                shared,
                egress_services,
                egress_node,
                buf: Vec::new(),
                fanin: (0..slots).map(|_| FanIn::default()).collect(),
                cycle: 0,
            },
        ))
    }
}

impl SendHalf {
    #[allow(clippy::too_many_arguments)]
    fn send(
        &self,
        ring: &MpscProducer,
        msg_type: u16,
        flags: u16,
        kind: ReqKind,
        user_data: u64,
        bytes: &[u8],
        expected: u8,
        fan_in: bool,
        prefix: Option<u8>,
    ) -> Result<(), SubmitError> {
        let s = &self.shared;
        if s.dead.load(Ordering::Acquire) {
            let (attached, current) = s.restart.lock().unwrap().unwrap_or((s.instance_id, 0));
            return Err(SubmitError::InstanceRestart { attached, current });
        }
        if s.serving_gate && s.cnc.status().flags.load_acquire() & NODE_FLAG_CAN_SERVE == 0 {
            return Err(SubmitError::NotServing);
        }
        // The cap describes the WIRE payload (deviation 6): a query carries
        // its one-byte service id.
        let wire_len = bytes.len() + usize::from(prefix.is_some());
        if let Some(max) = s.max_payload
            && wire_len > max
        {
            return Err(SubmitError::PayloadTooLarge { len: wire_len, max });
        }
        let deadline_ns = s.t0.elapsed().as_nanos() as u64 + s.timeout_ns;
        let seq = s
            .table
            .claim(user_data, kind, deadline_ns, expected, fan_in)
            .map_err(|_| SubmitError::Backpressure)?; // WindowFull and SlotBusy alike
        let extra = extra_client(s.client_id, seq as u32);
        let write_result = match prefix {
            None => ring.try_write(msg_type, flags, extra, bytes),
            Some(id) => {
                // One `try_write` takes one slice: assemble `id ++ bytes` in
                // this half's scratch (SendHalf is !Sync; the RefCell is never
                // contended).
                let mut scratch = self.scratch.borrow_mut();
                write_query_payload(id, bytes, &mut scratch);
                ring.try_write(msg_type, flags, extra, &scratch)
            }
        };
        finish_write(&s.table, &s.stats, seq, write_result)
    }

    /// The ring bitmask for a declared id, or the door refusal.
    fn expect_one(&self, id: u8) -> Result<u8, SubmitError> {
        let declared = self.shared.declared;
        if (id as usize) < CNC_MAX_SERVICES && declared & (1u64 << id) != 0 {
            Ok(1u8 << id)
        } else {
            Err(SubmitError::ServiceNotDeclared { id, declared })
        }
    }

    /// The declared set this engine attached to (bit i ⇔ FSM i), `0b1` on a
    /// harness node.
    pub fn declared(&self) -> u64 {
        self.shared.declared
    }

    /// Submit a command; FSM 0 answers. Nonblocking. See the module's central
    /// contract: an `Ok(())` here obligates the engine to eventually deliver
    /// exactly one completion for `user_data` via [`PollHalf::poll`].
    pub fn try_submit(&self, user_data: u64, cmd_bytes: &[u8]) -> Result<(), SubmitError> {
        self.try_submit_to(user_data, 0, cmd_bytes)
    }

    /// M14b: submit a command; FSM `id` answers (every declared FSM applies
    /// it — only the named one's response is awaited).
    pub fn try_submit_to(
        &self,
        user_data: u64,
        id: u8,
        cmd_bytes: &[u8],
    ) -> Result<(), SubmitError> {
        let expected = self.expect_one(id)?;
        self.send(
            &self.ingress,
            MSG_V2_SUBMIT,
            0,
            ReqKind::Submit,
            user_data,
            cmd_bytes,
            expected,
            false,
            None,
        )
    }

    /// M14b: submit a command and collect EVERY declared FSM's answer
    /// ([`Outcome::Responses`], ascending by id, one completion).
    pub fn try_submit_all(&self, user_data: u64, cmd_bytes: &[u8]) -> Result<(), SubmitError> {
        let expected = self.shared.declared as u8; // ids < 8 ⇒ the mask fits
        self.send(
            &self.ingress,
            MSG_V2_SUBMIT,
            0,
            ReqKind::Submit,
            user_data,
            cmd_bytes,
            expected,
            true,
            None,
        )
    }

    /// Issue a read against FSM 0; nonblocking. `query_bytes` must decode as
    /// `bincode(Query)` against today's SDK (see the module's byte contract).
    pub fn try_query(
        &self,
        user_data: u64,
        query_bytes: &[u8],
        c: Consistency,
    ) -> Result<(), SubmitError> {
        self.try_query_on(user_data, 0, query_bytes, c)
    }

    /// M14b: issue a read against FSM `id`. The wire payload is `id ++ query`.
    pub fn try_query_on(
        &self,
        user_data: u64,
        id: u8,
        query_bytes: &[u8],
        c: Consistency,
    ) -> Result<(), SubmitError> {
        let expected = self.expect_one(id)?;
        let flags = match c {
            Consistency::Linearizable => FLAG_V2_LINEARIZABLE,
            Consistency::Snapshot => 0,
        };
        self.send(
            &self.query,
            MSG_V2_QUERY,
            flags,
            ReqKind::Query,
            user_data,
            query_bytes,
            expected,
            false,
            Some(id),
        )
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

/// `send`'s post-claim tail: report the outcome of the ring write, or — the
/// case this function exists to get right — the outcome of a ring write that
/// FAILED but whose slot had ALREADY been completed by something else.
///
/// `claim()` publishes the slot (with `user_data` already stored) before
/// `send()` ever attempts the ring write, so there is a real window between
/// claim and write during which a concurrent `resolve`/`sweep`/`drain_abort`
/// can legitimately win the slot's single-CAS completion protocol (slots.rs
/// invariant 3) — e.g. an instance-restart drain, or a deadline sweep racing
/// a slow write. When that happens, `table.release(seq)` LOSES its own CAS
/// (the slot is already `FREE`) and returns `false`: whoever won already
/// called the completion callback with this exact `user_data`, which for
/// `pipelined.rs`'s driver means `Arc::from_raw` already ran for it.
///
/// If this function then reported the write failure as a refusal
/// (`Err(SubmitError::Backpressure)` etc.), the caller would treat the
/// request as "never accepted" and reclaim `user_data` itself — a SECOND
/// `Arc::from_raw` on a pointer already reclaimed, i.e. a double-free /
/// use-after-free on the ticket the first completion already resolved. So a
/// lost release is reported as accepted (`Ok(())`) instead: the completion
/// already exists (or is already in flight to the caller), same as a real
/// `Ok(())` from the ring write — the `accepted` stat is bumped to match, so
/// `accepted` stays the true count of requests the engine took ownership of,
/// not just of ring writes that physically succeeded.
fn finish_write(
    table: &SlotTable,
    stats: &StatCells,
    seq: u64,
    write_result: Result<(), RingError>,
) -> Result<(), SubmitError> {
    match write_result {
        Ok(()) => {
            stats.accepted.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(e) => {
            if !table.release(seq) {
                // Lost the CAS: see the doc comment above. Already completed
                // — not a refusal.
                stats.accepted.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
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

impl Clone for SendHalf {
    fn clone(&self) -> Self {
        SendHalf {
            shared: Arc::clone(&self.shared),
            ingress: self.ingress.clone(), // per-clone producer cache (MpscProducer contract)
            query: self.query.clone(),
            scratch: RefCell::new(Vec::new()), // per-clone assembly buffer
        }
    }
}

impl PollHalf {
    /// Drain and dispatch completions in one bounded duty cycle: up to 128
    /// records off each broadcast (every declared FSM's ring in ascending id
    /// order, then the node ring), plus — every 64th call — the amortized
    /// restart check and deadline sweep. `cb` is called once per resolved
    /// completion; returns the total emitted.
    pub fn poll(&mut self, mut cb: impl FnMut(Completion<'_>)) -> usize {
        self.cycle += 1;
        let maint = self.cycle.is_multiple_of(64);
        let PollHalf { shared, egress_services, egress_node, buf, fanin, .. } = self;
        let mut emitted = 0usize;
        if maint {
            emitted += maintenance(shared, &mut cb);
        }
        for (id, ring) in egress_services.iter_mut() {
            emitted += drain_ring(ring, Some(*id), shared, buf, fanin, &mut cb);
        }
        emitted += drain_ring(egress_node, None, shared, buf, fanin, &mut cb);
        emitted
    }

    /// Fail every still-inflight request via `cb` (used on shutdown).
    pub fn drain_abort(&mut self, cb: impl FnMut(u64)) {
        self.shared.table.drain_abort(cb);
    }

    /// Wait handle for FSM 0's egress broadcast — the default responder's
    /// ring — for a caller that wants to park (rather than busy-poll) between
    /// duty cycles.
    ///
    /// M14b deviation 3: a `RingWaitHandle` is ONE futex, so a parked driver
    /// is woken by FSM 0's publishes only; a completion that lands solely on
    /// another FSM's ring resolves at the park timeout instead (≤ 1 ms in the
    /// pipelined driver), not at the publish. The gateway only ever issues
    /// FSM-0 requests, so its driver is unaffected. (On a node that does not
    /// declare FSM 0, this is the lowest declared id's ring — the first entry
    /// of the ascending list.)
    pub fn wait_handle(&self) -> uc_protocol::ring::RingWaitHandle {
        self.egress_services[0].1.wait_handle()
    }

    /// A point-in-time snapshot of this engine's counters.
    pub fn stats(&self) -> EngineStats {
        self.shared.stats.snapshot()
    }
}

/// Drain up to 128 records off one broadcast (bounded work per call).
fn drain_ring(
    ring: &mut BroadcastConsumer,
    ring_id: Option<u8>,
    shared: &Shared,
    buf: &mut Vec<u8>,
    fanin: &mut [FanIn],
    cb: &mut impl FnMut(Completion<'_>),
) -> usize {
    let mut emitted = 0usize;
    for _ in 0..128 {
        match ring.try_read(buf) {
            Ok(Some(rec)) => emitted += handle_record(shared, fanin, ring_id, &rec, buf, cb),
            Ok(None) => break,
            Err(RingError::Overwritten) => {
                // Spec §4 item 6: a stat, NOT an eager fail-all — the engine
                // cannot know which responses were in the lost window; the
                // deadline sweep backstops anything actually lost.
                shared.stats.overwritten.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                shared.stats.corrupt.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    emitted
}

/// One record's routing. Returns 1 if a completion was emitted, else 0.
fn handle_record(
    shared: &Shared,
    fanin: &mut [FanIn],
    ring_id: Option<u8>,
    rec: &RecordHeader,
    buf: &[u8],
    cb: &mut impl FnMut(Completion<'_>),
) -> usize {
    let (cid, wire_seq) = client_from_extra(rec.header_extra);
    if cid != shared.client_id {
        return 0; // every client sees every broadcast record
    }
    match rec.msg_type {
        MSG_V2_RESPONSE => {
            let Some(ring) = ring_id else {
                return 0; // a RESPONSE never travels on the node ring
            };
            if buf.len() < 8 {
                // Malformed (missing position prefix): count, do NOT resolve —
                // the slot stays live and the deadline backstops the request.
                shared.stats.corrupt.fetch_add(1, Ordering::Relaxed);
                return 0;
            }
            let delivered = if rec.flags & FLAG_V2_IS_QUERY != 0 {
                ReqKind::Query
            } else {
                ReqKind::Submit
            };
            let position = u64::from_le_bytes(buf[..8].try_into().unwrap());
            match shared.table.resolve(wire_seq, Some(delivered), Some(ring)) {
                Resolve::Won { user_data, fan_in: false } => {
                    shared.stats.responses.fetch_add(1, Ordering::Relaxed);
                    cb(Completion {
                        user_data,
                        position: Some(position),
                        outcome: Outcome::Response(&buf[8..]),
                    });
                    1
                }
                Resolve::Won { user_data, fan_in: true } => {
                    // The last piece: buffer it beside the earlier ones, emit
                    // the whole set ordered by id, then drop the pieces (the
                    // `Bytes` refcounts travelled to the caller by value).
                    let f = &mut fanin[shared.table.slot_index(wire_seq)];
                    f.push_piece(wire_seq, position, ring, &buf[8..]);
                    f.parts.sort_by_key(|p| p.0);
                    shared.stats.responses.fetch_add(1, Ordering::Relaxed);
                    cb(Completion {
                        user_data,
                        position: Some(f.position),
                        outcome: Outcome::Responses(&f.parts),
                    });
                    f.parts.clear();
                    1
                }
                Resolve::Partial => {
                    fanin[shared.table.slot_index(wire_seq)]
                        .push_piece(wire_seq, position, ring, &buf[8..]);
                    0
                }
                Resolve::WrongRing => {
                    // A sibling FSM answering a request that named another
                    // ring (or a fan-in generation that never expected it).
                    shared.stats.wrong_ring.fetch_add(1, Ordering::Relaxed);
                    0
                }
                Resolve::KindMismatch => {
                    // T14: stale cross-generation collision — drop, count,
                    // leave the slot for the real answer.
                    shared.stats.kind_mismatch.fetch_add(1, Ordering::Relaxed);
                    0
                }
                Resolve::Miss => {
                    shared.stats.duplicates.fetch_add(1, Ordering::Relaxed);
                    0
                }
            }
        }
        MSG_V2_NOT_LEADER => {
            // Defensive hint decode (malformed payload -> unknown, never panic)
            // — copied from the old matcher.rs.
            let hint_raw = u64::from_le_bytes(
                buf.get(..8).and_then(|s| s.try_into().ok()).unwrap_or([0xff; 8]),
            );
            let hint = if hint_raw == u64::MAX { None } else { Some(hint_raw as u32) };
            match shared.table.resolve(wire_seq, None, None) {
                // kind-agnostic: a pre-side-effect signal
                Resolve::Won { user_data, .. } => {
                    shared.stats.not_leader.fetch_add(1, Ordering::Relaxed);
                    cb(Completion { user_data, position: None, outcome: Outcome::NotLeader { hint } });
                    1
                }
                _ => 0, // stale redirect for an already-resolved slot: no side effect to guard
            }
        }
        MSG_V2_RETRY => match shared.table.resolve(wire_seq, None, None) {
            Resolve::Won { user_data, .. } => {
                shared.stats.retry.fetch_add(1, Ordering::Relaxed);
                cb(Completion { user_data, position: None, outcome: Outcome::Retry });
                1
            }
            _ => 0,
        },
        MSG_V2_BAD_SERVICE => {
            // M14b: ring-less and kind-agnostic, like RETRY — it ends the
            // whole request (a partial fan-in included; the stale pieces left
            // in `fanin[idx]` are discarded by the next generation's first
            // `push_piece`, which sees a different wire seq).
            let id = buf.first().copied().unwrap_or(u8::MAX);
            match shared.table.resolve(wire_seq, None, None) {
                Resolve::Won { user_data, .. } => {
                    shared.stats.bad_service.fetch_add(1, Ordering::Relaxed);
                    cb(Completion {
                        user_data,
                        position: None,
                        outcome: Outcome::BadService { id },
                    });
                    1
                }
                _ => 0,
            }
        }
        _ => 0, // not a client-facing msg_type
    }
}

/// Amortized slot-liveness pass (every 64 poll cycles): restart check first,
/// then the deadline sweep.
fn maintenance(shared: &Shared, cb: &mut impl FnMut(Completion<'_>)) -> usize {
    let mut emitted = 0usize;
    if !shared.dead.load(Ordering::Acquire) {
        // Read ONCE. None = torn/zeroed header while the node recreates the
        // cnc in place (M5 final-review semantics) -> current = 0 sentinel.
        let observed = shared.cnc.try_instance_id();
        if observed != Some(shared.instance_id) {
            let current = observed.unwrap_or(0);
            *shared.restart.lock().unwrap() = Some((shared.instance_id, current));
            shared.dead.store(true, Ordering::Release);
            shared.stats.restarts.fetch_add(1, Ordering::Relaxed);
            shared.table.drain_abort(|user_data| {
                cb(Completion {
                    user_data,
                    position: None,
                    outcome: Outcome::InstanceRestart { attached: shared.instance_id, current },
                });
                emitted += 1;
            });
        }
    }
    let now_ns = shared.t0.elapsed().as_nanos() as u64;
    shared.table.sweep(now_ns, |user_data| {
        shared.stats.timed_out.fetch_add(1, Ordering::Relaxed);
        cb(Completion { user_data, position: None, outcome: Outcome::TimedOut });
        emitted += 1;
    });
    emitted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slots::ReqKind;

    /// `finish_write` deterministically, WITHOUT needing to win a real race
    /// against a second thread: `claim` a slot exactly as `send()` would,
    /// complete it out from under ourselves via `drain_abort` (standing in
    /// for a concurrent `resolve`/`sweep`/`drain_abort` that wins the slot's
    /// completion between `send()`'s own `claim()` and its ring write — the
    /// window `finish_write`'s doc comment describes), then call
    /// `finish_write` with a write failure for that exact `seq` and assert
    /// it reports ACCEPTED, not refused, without double-decrementing
    /// `inflight` or double-counting `accepted`.
    ///
    /// This is the same shape as `slots.rs`'s
    /// `release_after_the_slot_was_already_completed_elsewhere_loses_cleanly`
    /// (which proves the underlying `SlotTable::release` primitive loses its
    /// CAS cleanly) but exercises `finish_write` itself — the actual code
    /// `send()` calls — rather than the primitive alone. A true end-to-end
    /// reproduction would need a second OS thread to land inside the few
    /// instructions between `send()`'s `claim()` and its `ring.try_write()`,
    /// which isn't something a unit test can force deterministically without
    /// adding a `#[cfg(test)]` hook into `send()` itself; driving the same
    /// post-claim tail (`finish_write`) directly against a real,
    /// already-raced `SlotTable` gets full coverage of the fixed branch
    /// without that.
    #[test]
    fn lost_release_after_a_concurrent_completion_reports_accepted_not_refused() {
        let table = SlotTable::new(8, 0);
        let stats = StatCells::default();
        let seq = table.claim(0xABCD, ReqKind::Submit, u64::MAX, 0b1, false).unwrap();

        // Something else (instance-restart drain, or the deadline sweep)
        // wins the slot's completion before our write ever lands.
        let mut drained = Vec::new();
        table.drain_abort(|ud| drained.push(ud));
        assert_eq!(drained, vec![0xABCD]);
        assert_eq!(table.inflight(), 0);

        // send()'s ring write then fails (RingError::Full, same as a
        // genuinely full ingress ring) — finish_write must NOT report this
        // as a refusal: the completion already happened.
        let result = finish_write(&table, &stats, seq, Err(RingError::Full));
        assert!(matches!(result, Ok(())), "lost release must report accepted: {result:?}");
        assert_eq!(stats.accepted.load(Ordering::Relaxed), 1, "accepted must count this request");
        assert_eq!(table.inflight(), 0, "must not double-decrement on a lost release");
    }

    /// The ordinary counterpart: a write failure whose release WINS (no
    /// race) must still refuse, exactly as before the fix.
    #[test]
    fn ordinary_write_failure_with_an_uncontested_release_still_refuses() {
        let table = SlotTable::new(8, 0);
        let stats = StatCells::default();
        let seq = table.claim(0xBEEF, ReqKind::Submit, u64::MAX, 0b1, false).unwrap();

        let result = finish_write(&table, &stats, seq, Err(RingError::Full));
        assert!(matches!(result, Err(SubmitError::Backpressure)), "{result:?}");
        assert_eq!(stats.accepted.load(Ordering::Relaxed), 0);
        assert_eq!(table.inflight(), 0, "an uncontested release still frees the slot");
    }
}
