// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The service-side SDK traits (M5 spec §7, M12a spec §3.1). The user
//! implements [`StateMachine`] (typed) or [`RawStateMachine`] (bytes-in/
//! bytes-out, the raw tier) — exactly one of the two — and optionally
//! [`OutputHandler`] / [`RawOutputHandler`]; the framework drives them off
//! the committed log.
//!
//! v2 differences from the v1 `uc_service` traits: apply is keyed by an
//! absolute byte **`position`** (the v2 log_index analog and the idempotency
//! key), not a log index; there are no snapshot methods (M5 reconstruction
//! replays the log instead — Task 9).
//!
//! M6 adds one OPTIONAL capability trait, [`SnapshotStateMachine`], for SMs that
//! can serialize/restore their full state (what gates log purge). Existing SMs
//! that don't implement it are untouched.

use uc_protocol::identity::FsmIdentity;
use uc_protocol::v2::ipc::{SchedOp, SchedRecord};

use crate::config::SnapshotError;
use crate::ids::IdGen;

/// A request a state machine made during one apply (time-and-timers §3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerReq {
    Schedule { id: u64, at_ns: u64 },
    Cancel { id: u64 },
}

/// A fired timer, as delivered to `on_timer` (time-and-timers §4.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimerEvent {
    pub id: u64,
    pub deadline_ns: u64,
    /// Fired from the replicated schedule table (plan 2), not from `schedule`.
    pub table: bool,
}
impl TimerEvent {
    /// The leader could not place this timer at its deadline (spec §4.3's
    /// post-failover case): `ctx.time_ns > deadline_ns`.
    pub fn late(&self, ctx: &ApplyCtx) -> bool {
        ctx.time_ns > self.deadline_ns
    }
}

/// Everything the framework knows about the committed frame being applied
/// (spec §3.3). Built by the apply loop and by journal replay, once per
/// frame; a state machine constructs one only in its own unit tests.
/// `#[non_exhaustive]`: the timestamps/scheduler design adds fields here
/// without changing `apply`'s signature again.
#[non_exhaustive]
#[derive(Debug)]
pub struct ApplyCtx {
    /// The frame's absolute byte position (the idempotency key).
    pub position: u64,
    /// The frame's leader stamp: ns since the Unix epoch, non-decreasing along
    /// the log, identical on every replica (time-and-timers §3). "Now".
    pub time_ns: u64,
    /// The frame's `leadership_term_id`.
    pub term: u32,
    identity: FsmIdentity,
    timers: Vec<TimerReq>,
    consumed: Vec<(u64, u64)>,
    consumed_table: Vec<(u64, u64)>,
}

impl ApplyCtx {
    pub fn new(position: u64, identity: FsmIdentity) -> ApplyCtx {
        ApplyCtx {
            position,
            time_ns: 0,
            term: 0,
            identity,
            timers: Vec::new(),
            consumed: Vec::new(),
            consumed_table: Vec::new(),
        }
    }
    /// Convenience for a state machine's own unit tests: `ApplyCtx::for_sm::<MySm>(pos)`
    /// builds the context with `S::IDENTITY`, the same identity the real apply
    /// loop stamps for `S`, so a test never has to spell
    /// `<MySm as RawStateMachine>::IDENTITY` itself (spec §3.3).
    pub fn for_sm<S: RawStateMachine>(position: u64) -> ApplyCtx {
        ApplyCtx::new(position, S::IDENTITY)
    }
    /// Test builder; the apply loop sets the field from the frame header.
    pub fn with_time(mut self, time_ns: u64) -> ApplyCtx {
        self.time_ns = time_ns;
        self
    }
    /// Test builder; the apply loop sets the field from the frame header.
    pub fn with_term(mut self, term: u32) -> ApplyCtx {
        self.term = term;
        self
    }
    pub fn identity(&self) -> FsmIdentity {
        self.identity
    }
    /// The deterministic ID generator for THIS apply call (spec §3.4).
    pub fn ids(&self) -> IdGen {
        IdGen::new(self.position, self.identity)
    }
    /// Ask for `on_timer(id)` at `at_ns` (log time). Re-scheduling a pending id
    /// replaces its deadline. Deterministic: an output of apply, replayed
    /// identically on every replica (time-and-timers §4.4).
    pub fn schedule(&mut self, id: u64, at_ns: u64) {
        self.timers.push(TimerReq::Schedule { id, at_ns });
    }
    pub fn cancel(&mut self, id: u64) {
        self.timers.push(TimerReq::Cancel { id });
    }
    /// What this apply has asked so far, in order (read by `Timed`).
    pub fn timers(&self) -> &[TimerReq] {
        &self.timers
    }
    /// `Timed` only: this instance was delivered or dropped; the node may clear it.
    pub(crate) fn consumed(&mut self, id: u64, deadline_ns: u64) {
        self.consumed.push((id, deadline_ns));
    }
    /// `Timed` only: a **table** tick (plan 2's replicated schedule table)
    /// was delivered or dropped; the node advances that entry's
    /// `last_delivered` from this instead of `Consumed` (`RowTimers::
    /// table_delivered`, Task 4), so a re-fired tick never re-advances it.
    pub(crate) fn consumed_table(&mut self, id: u64, deadline_ns: u64) {
        self.consumed_table.push((id, deadline_ns));
    }
    /// Apply loop only: drain both lists as wire records, requests first.
    pub(crate) fn take_sched_records(&mut self) -> Vec<SchedRecord> {
        let mut out =
            Vec::with_capacity(self.timers.len() + self.consumed.len() + self.consumed_table.len());
        for r in self.timers.drain(..) {
            out.push(match r {
                TimerReq::Schedule { id, at_ns } => SchedRecord {
                    op: SchedOp::Schedule,
                    timer_id: id,
                    deadline_ns: at_ns,
                },
                TimerReq::Cancel { id } => SchedRecord {
                    op: SchedOp::Cancel,
                    timer_id: id,
                    deadline_ns: 0,
                },
            });
        }
        for (id, dl) in self.consumed.drain(..) {
            out.push(SchedRecord {
                op: SchedOp::Consumed,
                timer_id: id,
                deadline_ns: dl,
            });
        }
        for (id, dl) in self.consumed_table.drain(..) {
            out.push(SchedRecord {
                op: SchedOp::TableConsumed,
                timer_id: id,
                deadline_ns: dl,
            });
        }
        out
    }
    /// Test-only alias of [`Self::take_sched_records`] for the integration
    /// test in `uc_service/tests/timed.rs`, which cannot see the `pub(crate)`
    /// method.
    #[doc(hidden)]
    pub fn take_sched_records_for_test(&mut self) -> Vec<SchedRecord> {
        self.take_sched_records()
    }
}

/// The user's deterministic business logic.
///
/// **`apply` is sync, deterministic, no I/O, no clock of its own: `ctx.time_ns`
/// is the log's, no randomness.** The signature enforces it: a `&mut self`
/// transition with no `async`, no context handle beyond `ApplyCtx`. This is
/// non-negotiable for state-machine-replication correctness — every replica
/// must reach the same state from the same committed log.
pub trait StateMachine: Send + 'static {
    /// The FSM's identity — the same wherever this type attaches (spec §3).
    const NAME: &'static str;
    /// Packed semantic version of this FSM's logic (`identity::pack_version`);
    /// `0` = unversioned. Equality-checked cluster-wide, never an ID input.
    const VERSION: u32 = 0;

    type Command: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
    type Response: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
    type Query: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;
    type QueryResponse: serde::Serialize + serde::de::DeserializeOwned + Send + 'static;

    /// Apply one committed command. `ctx.position` is the frame's absolute
    /// byte position (the v2 log_index analog and the natural idempotency
    /// key); `ctx.ids()` is the deterministic ID generator for this call.
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Self::Command) -> Self::Response;

    /// Answer a read. Same method whether the framework routes it linearizable
    /// or snapshot (Task 11) — the IPC boundary carries typed queries, not
    /// closures.
    fn query(&self, q: Self::Query) -> Self::QueryResponse;

    /// Position of the last applied frame; `None` = fresh (nothing applied).
    /// Under-reporting is safe (the apply loop's idempotent-skip re-applies
    /// nothing already seen); over-reporting above the journal frontier is
    /// refused at attach ([`ServiceError::Drift`](crate::ServiceError::Drift)).
    fn last_applied(&self) -> Option<u64>;

    /// A timer this FSM scheduled (or the schedule table fired) has reached
    /// its position on the log. `ctx.time_ns` is the frame's stamp — the
    /// deadline unless `ev.late(ctx)`. Advance `last_applied` from
    /// `ctx.position` exactly as in `apply`. Default: ignore timers.
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        let _ = (ctx, ev);
    }
}

/// The core state-machine contract: bytes in, bytes out. The framework hands
/// `apply` the committed frame payload exactly as it sits in the log buffer
/// and reuses `out` across calls — no decode, no allocation in steady state.
/// Implement this directly for SBE / flatbuffers / hand-laid frames; or
/// implement [`StateMachine`] (typed, serde + bincode) and get this for free
/// via the blanket impl below. A type implements ONE of the two.
pub trait RawStateMachine: Send + 'static {
    /// The FSM's identity — the same wherever this type attaches (spec §3).
    const NAME: &'static str;
    /// Packed semantic version of this FSM's logic (`identity::pack_version`);
    /// `0` = unversioned. Equality-checked cluster-wide, never an ID input.
    const VERSION: u32 = 0;
    /// Provided; evaluated (and validated) at first use — a bad `NAME` is a
    /// compile-time error where `IDENTITY` is first named.
    const IDENTITY: FsmIdentity = FsmIdentity::parse(Self::NAME, Self::VERSION);

    /// Apply the committed command at `ctx.position` (the absolute log byte
    /// offset, the idempotency key). Write the response bytes into `out`
    /// (cleared by the caller). Deterministic, sync, no I/O.
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>);
    /// Answer a read. `out` is cleared by the caller.
    fn query(&self, q: &[u8], out: &mut Vec<u8>);
    /// Highest position applied so far (`None` before the first).
    fn last_applied(&self) -> Option<u64>;

    /// A timer this FSM scheduled (or the schedule table fired) has reached
    /// its position on the log. `ctx.time_ns` is the frame's stamp — the
    /// deadline unless `ev.late(ctx)`. Advance `last_applied` from
    /// `ctx.position` exactly as in `apply`. Default: ignore timers.
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        let _ = (ctx, ev);
    }

    /// Framework hook (time-and-timers §4.8): the pending instances a wrapper
    /// holds, re-announced to the node after attach and after replay. Only
    /// `Timed` overrides it; a bare state machine has none.
    fn pending_timers(&self) -> Vec<(u64, u64)> {
        Vec::new()
    }

    /// Framework hook (time-and-timers plan 2): the table ticks this wrapper
    /// has delivered (`id -> last delivered deadline`), re-announced to the
    /// node after attach and after replay so a node rebuilds `last_delivered`
    /// for every table entry from the service that actually delivered them —
    /// the same purpose `pending_timers` serves for the programmatic set.
    /// Only `Timed` overrides it; a bare state machine has none. NOT
    /// forwarded by the blanket `StateMachine` impl (typed SMs never see the
    /// table directly — only a `Timed` wrapper does).
    fn table_delivered(&self) -> Vec<(u64, u64)> {
        Vec::new()
    }
}

/// Every typed state machine is a raw one: decode with bincode-standard,
/// apply, encode the response with bincode-standard — exactly the codec the
/// framework used through v2.5.0, so the wire is byte-identical.
impl<S: StateMachine> RawStateMachine for S {
    const NAME: &'static str = S::NAME;
    const VERSION: u32 = S::VERSION;

    #[inline]
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        let (cmd, _) =
            bincode::serde::decode_from_slice::<S::Command, _>(cmd, bincode::config::standard())
                .expect("corrupt committed frame (fail-stop)");
        let resp = StateMachine::apply(self, ctx, cmd);
        bincode::serde::encode_into_std_write(&resp, out, bincode::config::standard())
            .expect("response bincode-encode (fail-stop)");
    }
    #[inline]
    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        let (q, _) =
            bincode::serde::decode_from_slice::<S::Query, _>(q, bincode::config::standard())
                .expect("corrupt query frame (fail-stop)");
        let qr = StateMachine::query(self, q);
        bincode::serde::encode_into_std_write(&qr, out, bincode::config::standard())
            .expect("query-response bincode-encode (fail-stop)");
    }
    #[inline]
    fn last_applied(&self) -> Option<u64> {
        StateMachine::last_applied(self)
    }
    #[inline]
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        StateMachine::on_timer(self, ctx, ev)
    }
}

/// Optional capability: state machines that can serialize their full state and
/// restore it wholesale. This is what lets the framework **purge** the log — a
/// deployment whose SM does not implement it never purges (M6; documented), and
/// below-floor reconstruction (Task 5) installs a snapshot instead of replaying
/// from the archive.
///
/// It is a *separate* trait from [`RawStateMachine`] on purpose: v2's base trait
/// carries no snapshot methods (M5 reconstruction replayed the log), so existing
/// SMs are untouched — a snapshot-capable SM opts in by also implementing this.
/// The supertrait is the CORE contract ([`RawStateMachine`]), so a raw-tier SM
/// can be snapshot-capable too; a typed [`StateMachine`] satisfies it through
/// the blanket impl above, so existing implementations are unchanged.
///
/// ## Position-as-version
///
/// The `u64`s here are absolute byte **positions** in the log (the v2 log-index
/// analog), NOT dense 1,2,3 indexes: they are sparse, strictly-increasing
/// artifact tags. `freeze` pins the current position; `install_snapshot` is told
/// the position `S` the artifact was tagged with and must land the restored
/// state exactly there.
pub trait SnapshotStateMachine: RawStateMachine {
    /// An opaque, consistent handle to the frozen state. `Send + 'static` so it
    /// can cross to the off-thread streaming step.
    type SnapshotHandle: Send + 'static;

    /// O(1) consistent pin of the current state; returns `(handle, position)`
    /// where `position == self.last_applied().unwrap_or(0)` at pin time. Called
    /// on the APPLY thread with the SM lock held, so the position cannot move
    /// underneath the pin.
    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), SnapshotError>;

    /// Stream the pinned state to `dst`. Runs OFF the apply thread with NO SM
    /// lock held (the v1 rule) — the handle carries a consistent, immutable view
    /// so concurrent applies cannot corrupt the stream.
    fn stream_snapshot(
        handle: Self::SnapshotHandle,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), SnapshotError>;

    /// Replace the state wholesale from a stream produced by
    /// [`stream_snapshot`](Self::stream_snapshot), landing it at `position`
    /// (the artifact tag `S` — the caller supplies it; the bare byte stream does
    /// not carry it). Returns the post-install position, which MUST equal
    /// `position`. Runs on the apply thread with the SM lock held.
    ///
    /// Deviation from the M6 brief's literal trait block: the brief sketched a
    /// no-argument `install_snapshot(&mut self, src)` that recovered `S` from a
    /// "stream trailer". No such trailer exists in the ULTSNAP wire format, so
    /// the honest shape passes `position` explicitly — the caller already knows
    /// `S` (it is the snapshot artifact's tag).
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, SnapshotError>;
}

/// Optional leader-only, at-least-once side-effect handler (Task 12 wires the
/// output agent; defined here so the builder's default type parameter exists).
#[allow(async_fn_in_trait)]
pub trait OutputHandler<S: StateMachine>: Send + 'static {
    /// Run after a command commits, on the leader only. `Retryable` is retried
    /// while still leader; `Permanent` advances the progress marker anyway.
    async fn on_committed(
        &self,
        position: u64,
        cmd: &S::Command,
        state: &S,
    ) -> Result<(), OutputError>;
}

/// The default no-op output handler (no side effects).
pub struct NoopOutput;

impl<S: StateMachine> OutputHandler<S> for NoopOutput {
    async fn on_committed(
        &self,
        _position: u64,
        _cmd: &S::Command,
        _state: &S,
    ) -> Result<(), OutputError> {
        Ok(())
    }
}

/// Raw-tier output handler: sees the committed command bytes. The typed
/// [`OutputHandler`] is adapted onto this by [`TypedOutput`].
#[allow(async_fn_in_trait)]
pub trait RawOutputHandler<S: RawStateMachine>: Send + 'static {
    async fn on_committed(&self, position: u64, cmd: &[u8], state: &S) -> Result<(), OutputError>;
}

impl<S: RawStateMachine> RawOutputHandler<S> for NoopOutput {
    async fn on_committed(
        &self,
        _position: u64,
        _cmd: &[u8],
        _state: &S,
    ) -> Result<(), OutputError> {
        Ok(())
    }
}

/// Adapts a typed [`OutputHandler`] to the raw tier (one bincode decode per
/// committed command, as the output agent did through v2.5.0).
pub struct TypedOutput<O>(pub O);

impl<S: StateMachine, O: OutputHandler<S>> RawOutputHandler<S> for TypedOutput<O> {
    async fn on_committed(&self, position: u64, cmd: &[u8], state: &S) -> Result<(), OutputError> {
        let (cmd, _) =
            bincode::serde::decode_from_slice::<S::Command, _>(cmd, bincode::config::standard())
                .expect("corrupt committed frame (fail-stop)");
        self.0.on_committed(position, &cmd, state).await
    }
}

/// Why an `on_committed` side effect did not complete.
#[derive(Debug, Clone, thiserror::Error)]
pub enum OutputError {
    #[error("retryable: {0}")]
    Retryable(String),
    #[error("permanent: {0}")]
    Permanent(String),
}
