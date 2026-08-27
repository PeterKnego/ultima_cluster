// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The service SDK: where your state machine runs.
//!
//! This is the crate you write against. Implement [`StateMachine`] — a
//! synchronous, deterministic `apply` and `query` — and
//! [`ServiceBuilder::start`] attaches it to a running node and begins applying
//! the committed log. See `docs/QUICKSTART.md` for a complete example, and
//! `examples/counter/src/lib.rs` for the smallest useful state machine.
//!
//! `apply` must be deterministic: same state plus same command must produce the
//! same next state on every replica, forever. No clocks, no randomness, no I/O.
//! Side effects that genuinely need the outside world belong in
//! [`OutputHandler`] (async, leader-only, at-least-once).
//!
//! The user implements [`StateMachine`] (sync, deterministic `apply`/`query`)
//! and optionally [`OutputHandler`] (async, leader-only side effects, Task 12).
//! [`ServiceBuilder::start`] attaches to a running node's shared-memory IPC
//! (the cnc page, the log buffer, and the egress/query rings under the node's
//! instance directory) and spawns the apply agent — a single polling thread
//! that follows the committed log, applies each command, and publishes the
//! response onto the egress broadcast.
//!
//! Spec: `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md`
//! §7; plan `docs/superpowers/plans/2026-07-11-uc2-m5-sdk.md` Task 8.
//!
//! **Semver:** see `docs/reference/semver-policy.md`. Promised surface:
//! [`RawStateMachine`], [`StateMachine`], [`SnapshotStateMachine`],
//! [`OutputHandler`]/[`RawOutputHandler`], and
//! [`Sessioned`]/[`SessionConfig`].

mod apply;
mod attach;
mod builder_agent;
mod config;
mod egress;
mod output;
mod replay;
mod session;
/// Position-tagged on-disk snapshot files (M6 Task 3) — `SnapshotStore`'s
/// atomic-publish + keep-newest-2 retention. Public: tests and operational
/// tooling read the snapshot directory directly (e.g. the M6 Task 3 e2e test
/// cross-checks the cnc marker against `SnapshotStore::newest`).
pub mod snapshots;
mod traits;

/// Reference [`StateMachine`] + [`SnapshotStateMachine`] adapter backed by an
/// [`ultima_db::Store`] (Cargo feature `ultima_db`, off by default).
#[cfg(feature = "ultima_db")]
pub mod ultima_db;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::time::Duration;

use uc2_log::agent::{AgentRunner, IdleStrategy};
use uc2_log::cnc::CncPage;
use uc2_log::reader::LogFollower;

use crate::apply::{FreezeFn, SnapshotTrigger, apply_cycle};
use crate::builder_agent::{BuildJob, BuilderState, builder_cycle};
use crate::output::{OutputState, output_cycle};
use crate::snapshots::SnapshotStore;

pub use crate::config::{ServiceConfig, ServiceError, SnapshotError, SnapshotPolicy};
pub use crate::traits::{
    NoopOutput, OutputError, OutputHandler, RawOutputHandler, RawStateMachine,
    SnapshotStateMachine, StateMachine, TypedOutput,
};
pub use crate::session::{
    SESSION_HEADER_LEN, SessionConfig, Sessioned, TAG_EXPIRED, TAG_FRESH, TAG_REPLAYED,
};

/// Default idle strategy for the apply thread: a short sleep between empty
/// cycles (a busy-spin knob comes later). Background-grade politeness that
/// still keeps sub-ms apply latency under load.
const APPLY_IDLE: IdleStrategy = IdleStrategy::Sleep(Duration::from_micros(50));
/// Idle strategy for the output thread (Task 12): side effects are leader-only
/// and inherently bursty (commit-triggered), so the same short-sleep cadence
/// as apply is plenty responsive without spinning a core for a mostly-idle
/// duty cycle.
const OUTPUT_IDLE: IdleStrategy = IdleStrategy::Sleep(Duration::from_micros(50));

/// Builds and starts a [`Service`]. `O` defaults to [`NoopOutput`]; call
/// [`output_handler`](Self::output_handler) to install a real one (Task 12).
pub struct ServiceBuilder<S: RawStateMachine, O: RawOutputHandler<S> = NoopOutput> {
    cfg: ServiceConfig,
    sm: S,
    output: O,
}

impl<S: RawStateMachine> ServiceBuilder<S, NoopOutput> {
    pub fn new(cfg: ServiceConfig, sm: S) -> Self {
        Self { cfg, sm, output: NoopOutput }
    }
}

impl<S: RawStateMachine, O: RawOutputHandler<S>> ServiceBuilder<S, O> {
    /// Install a leader-only, typed output handler (Task 12 spawns its agent).
    /// The handler is adapted onto the raw tier by [`TypedOutput`] — one
    /// bincode decode per committed command, on the output thread, exactly as
    /// before M12a. The user-facing API is unchanged.
    pub fn output_handler<O2: OutputHandler<S>>(self, h: O2) -> ServiceBuilder<S, TypedOutput<O2>>
    where
        S: StateMachine,
    {
        ServiceBuilder { cfg: self.cfg, sm: self.sm, output: TypedOutput(h) }
    }

    /// Install a leader-only RAW output handler: it sees the committed command
    /// bytes straight from the log, with no codec in the way (the raw tier's
    /// counterpart to [`output_handler`](Self::output_handler)).
    pub fn raw_output_handler<O2: RawOutputHandler<S>>(self, h: O2) -> ServiceBuilder<S, O2> {
        ServiceBuilder { cfg: self.cfg, sm: self.sm, output: h }
    }

    /// Attach and spawn the agent threads (sync). Steps 1–5 run the attach
    /// discipline; step 6 spawns the apply thread (and, for a real handler,
    /// the output thread — Task 12) here.
    pub fn start(self) -> Result<Service<S>, ServiceError> {
        let ServiceBuilder { cfg, sm, output } = self;

        let attached = attach::attach(&cfg, sm)?;
        let buffer = attached.buffer;
        let cnc = attached.cnc;
        let instance_id = attached.instance_id;
        let epoch = attached.epoch;
        let poisoned = Arc::clone(&attached.poisoned);
        let service_id = attached.service_id;

        // 6. Spawn the apply thread. `AgentRunner::drop` already signals+joins,
        //    so a spawn failure below cannot leak a running thread. Keep a shared
        //    handle to the state machine so the `Service` can answer direct
        //    queries (test/embedded path until the client query ring, Task 11)
        //    AND so the output thread (below) sees apply's effects.
        let mut state = attached.apply_state;
        let sm = Arc::clone(&state.sm);
        let journal_dir = state.journal_dir.clone();

        // Task 12: spawn the output thread ONLY for a real (non-Noop) handler
        // — `O`'s bound (`Send + 'static`) makes it `TypeId`-comparable, so this
        // check is resolved per-monomorphization. The comparison is on the
        // CONCRETE type, not on how it reached the builder, so an explicit
        // `.output_handler(NoopOutput)` skips the spawn exactly like the
        // default (no `.output_handler` call) path — there is no way to force
        // a pure no-op thread into existence. `TypedOutput<NoopOutput>` is
        // named too, because M12a routes a typed handler through that adapter:
        // without it the `.output_handler(NoopOutput)` case would start
        // spawning a thread that can only ever run a no-op duty cycle.
        let mut agents = Vec::with_capacity(2);
        if !is_noop_output::<O>() {
            // Own cursor over the SAME log buffer, seeded from the node's
            // durable output-progress marker (Task 12 module doc).
            let start_pos = cnc.status().output_progress.load_acquire();
            let output_follower = LogFollower::new(Arc::clone(&buffer), start_pos);
            let mut output_state = OutputState::new(
                output_follower,
                Arc::clone(&sm),
                Arc::clone(&cnc),
                output,
                journal_dir,
                instance_id,
                service_id,
            )?;
            let output_agent =
                AgentRunner::spawn("uc2-output", OUTPUT_IDLE, move || output_cycle(&mut output_state))?;
            // Stop order (`Service::stop`/`Drop`): output before apply — a
            // side-effect thread is lower-priority to keep running through
            // teardown than the apply thread it depends on for `state: &S`.
            agents.push(output_agent);
        }
        let apply_agent = AgentRunner::spawn("uc2-apply", APPLY_IDLE, move || apply_cycle(&mut state))?;
        agents.push(apply_agent);

        Ok(Service { agents, sm, _cnc: cnc, instance_id, epoch, poisoned, service_id })
    }

    /// Like [`start`](Self::start), but ALSO spawns the M6 Task 3 snapshot
    /// builder thread — explicit opt-in for `S: SnapshotStateMachine`.
    ///
    /// **Controller-resolved deviation from the M6 Task 3 brief.** The brief's
    /// implicit shape was "spawn the builder whenever the SM is capable", but
    /// Rust cannot specialize a generic function's behavior on an *optional*
    /// trait bound at runtime — `start`'s `S: RawStateMachine` bound alone gives
    /// the compiler no way to conditionally call `S::freeze` only "if `S`
    /// happens to also implement `SnapshotStateMachine`". The resolution
    /// mirrors the existing `.output_handler(..)` opt-in pattern: a caller
    /// whose SM implements [`SnapshotStateMachine`] but does NOT want the
    /// builder thread (e.g. it never intends to configure a non-default
    /// [`SnapshotPolicy`](crate::SnapshotPolicy), or wants to defer opting in)
    /// simply calls [`start`](Self::start) instead — the builder thread only
    /// ever exists because THIS method was called, never as a side effect of
    /// the SM's capability alone.
    pub fn start_with_snapshots(self) -> Result<Service<S>, ServiceError>
    where
        S: SnapshotStateMachine,
    {
        let ServiceBuilder { cfg, sm, output } = self;

        let attached = attach::attach(&cfg, sm)?;
        let buffer = attached.buffer;
        let cnc = attached.cnc;
        let instance_id = attached.instance_id;
        let epoch = attached.epoch;
        let poisoned = Arc::clone(&attached.poisoned);
        let service_id = attached.service_id;

        let mut state = attached.apply_state;
        let sm = Arc::clone(&state.sm);
        let journal_dir = state.journal_dir.clone();

        let mut agents = Vec::with_capacity(3);
        if !is_noop_output::<O>() {
            let start_pos = cnc.status().output_progress.load_acquire();
            let output_follower = LogFollower::new(Arc::clone(&buffer), start_pos);
            let mut output_state = OutputState::new(
                output_follower,
                Arc::clone(&sm),
                Arc::clone(&cnc),
                output,
                journal_dir,
                instance_id,
                service_id,
            )?;
            let output_agent =
                AgentRunner::spawn("uc2-output", OUTPUT_IDLE, move || output_cycle(&mut output_state))?;
            agents.push(output_agent);
        }

        // Snapshot builder wiring: a 1-slot channel handing a (position,
        // type-erased streaming job) job from the apply thread (freeze, SM
        // lock held briefly — `crate::apply::maybe_build_snapshot`) to the
        // builder thread (stream + publish, off-lock —
        // `crate::builder_agent::builder_cycle`). `busy` gates BOTH
        // directions of "one in-flight build max": the apply thread checks it
        // before even calling `freeze()`, and the builder thread holds it for
        // the full stream+publish duration, not just while a job sits in the
        // channel.
        let store = SnapshotStore::open(&cfg.instance_dir)?;
        let busy = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::sync_channel::<(u64, BuildJob)>(1);
        // Seed the interval basis from whatever the cnc marker already holds
        // (0 on a fresh page) — a service reattaching after a prior
        // incarnation already built snapshots doesn't immediately re-trigger.
        let last_snapshot_pos = attach::slot(&cnc, cfg.service_id).snapshot_pos.load_acquire();
        let freeze: FreezeFn<S> = Box::new(|sm: &S| {
            let (handle, pos) = sm.freeze()?;
            let job: BuildJob = Box::new(move |w: &mut dyn std::io::Write| S::stream_snapshot(handle, w));
            Ok((job, pos))
        });
        state.snapshot_trigger = Some(SnapshotTrigger {
            policy: cfg.snapshot_policy,
            last_snapshot_pos,
            busy: Arc::clone(&busy),
            tx,
            freeze,
        });
        // M6 Task 5: the mirror capability — install a covering snapshot when
        // journal replay would otherwise fall below the purge floor. Built here
        // for the same reason as `freeze`: `S: SnapshotStateMachine` is only in
        // scope in this method. The reconstruction path (apply thread, SM lock
        // held) uses its own `SnapshotStore` clone to locate the newest covering
        // artifact.
        let install: crate::apply::InstallFn<S> =
            Box::new(|sm: &mut S, pos: u64, src: &mut dyn std::io::Read| sm.install_snapshot(pos, src));
        state.snapshot_restore = Some(crate::apply::SnapshotRestore {
            store: store.clone(),
            install,
        });
        let mut builder_state = BuilderState { rx, store, cnc: Arc::clone(&cnc), busy, service_id };
        let builder_agent =
            AgentRunner::spawn("uc2-snapshot-builder", APPLY_IDLE, move || builder_cycle(&mut builder_state))?;

        let apply_agent = AgentRunner::spawn("uc2-apply", APPLY_IDLE, move || apply_cycle(&mut state))?;
        agents.push(apply_agent);
        // Builder pushed LAST: `Service::stop`'s loop stops agents in
        // insertion order, so the builder is joined last — any build already
        // in flight when teardown starts gets to finish cleanly (the atomic
        // publish is never aborted mid-write; `AgentRunner::stop`/`Drop` only
        // signal BETWEEN duty cycles) after apply has already stopped feeding
        // it new work.
        agents.push(builder_agent);

        Ok(Service { agents, sm, _cnc: cnc, instance_id, epoch, poisoned, service_id })
    }
}

/// Is `O` a handler that can only ever do nothing — [`NoopOutput`] itself, or
/// the typed [`NoopOutput`] routed through the [`TypedOutput`] adapter? Both
/// mean "do not spawn the output thread" (see the call sites).
fn is_noop_output<O: 'static>() -> bool {
    let id = std::any::TypeId::of::<O>();
    id == std::any::TypeId::of::<NoopOutput>()
        || id == std::any::TypeId::of::<TypedOutput<NoopOutput>>()
}

/// A running service: the agent thread(s) plus the handles that keep the
/// shared-memory mappings alive.
pub struct Service<S: RawStateMachine> {
    agents: Vec<AgentRunner>,
    /// Set when the apply thread poisons this incarnation (log rewound
    /// beneath the applied frontier). See [`Service::is_alive`].
    poisoned: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Shared handle to the apply thread's state machine, for direct queries.
    sm: Arc<Mutex<S>>,
    /// Held for the service's life so the mmap'd cnc page stays mapped.
    _cnc: Arc<CncPage>,
    instance_id: u128,
    epoch: u64,
    /// M14a: which declared FSM slot this process is (`cfg.service_id`).
    service_id: u8,
}

impl<S: RawStateMachine> Service<S> {
    /// The node instance this service attached to (a change means the node
    /// restarted since attach — a reconstruction trigger, Task 9).
    pub fn instance_id(&self) -> u128 {
        self.instance_id
    }

    /// This service incarnation's epoch (the value it bumped this FSM's slot
    /// `epoch` to at attach).
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// M14a: which declared FSM slot this process is (`ServiceConfig::service_id`).
    pub fn service_id(&self) -> u8 {
        self.service_id
    }

    /// Direct, synchronous raw query against the live state machine on the
    /// apply thread — bytes in, bytes out, `out` cleared first. This is the
    /// test/embedded read path only — the real client query path (linearizable
    /// barrier + egress answer) lands in Task 10/11. Hidden from docs: not part
    /// of the public read contract.
    #[doc(hidden)]
    pub fn query_raw(&self, q: &[u8], out: &mut Vec<u8>) {
        out.clear();
        self.sm.lock().unwrap().query(q, out);
    }

    /// Typed convenience over [`query_raw`](Self::query_raw): encodes the query
    /// and decodes the answer with the same bincode-standard codec the blanket
    /// [`RawStateMachine`] impl uses. Hidden from docs, like `query_raw`.
    #[doc(hidden)]
    pub fn query(&self, q: S::Query) -> S::QueryResponse
    where
        S: StateMachine,
    {
        let q = bincode::serde::encode_to_vec(&q, bincode::config::standard()).expect("encode");
        let mut out = Vec::new();
        self.query_raw(&q, &mut out);
        bincode::serde::decode_from_slice(&out, bincode::config::standard()).expect("decode").0
    }

    /// Are all of this incarnation's agents still running? `false` means one
    /// fail-stopped (instance-mismatch, or the log-rewind contract) and this
    /// incarnation is finished: its SM may hold state from a truncated
    /// timeline, so the answer is to respawn a fresh service against the same
    /// instance dir, which reconstructs from the journal. A supervisor polls
    /// this; without it the death is only discovered at teardown, when
    /// [`stop`] re-raises the panic.
    ///
    /// [`stop`]: Self::stop
    pub fn is_alive(&self) -> bool {
        !self.poisoned.load(std::sync::atomic::Ordering::Acquire)
            && self.agents.iter().all(|a| !a.is_finished())
    }

    /// Graceful stop: clear this incarnation's attached bit (the incarnation
    /// counter survives — a fresh attach on the same page bumps it, not this),
    /// then signal every agent and join, propagating a work-closure panic
    /// (fail-loud in teardown). `crash()` deliberately leaves the bit set — a
    /// crash is indistinguishable from a kill; the heartbeat ages instead
    /// (spec §8).
    pub fn stop(self) {
        let s = attach::slot(&self._cnc, self.service_id);
        let (_, _, inc) = uc2_log::cnc::unpack_service_status(s.status.load_acquire());
        s.status.store_release(uc2_log::cnc::pack_service_status(self.service_id, false, inc));
        for a in self.agents {
            a.stop();
        }
    }

    /// Crash-stop (test hook): signal + join WITHOUT any final counter
    /// publishes — a simulated hard death. Threads cannot be force-killed
    /// in-process, so this still joins them (via `AgentRunner::drop`); the
    /// distinction from [`stop`](Self::stop) is that no teardown work runs
    /// (relevant once later tasks add graceful-stop publishes).
    pub fn crash(self) {
        drop(self.agents);
    }
}
