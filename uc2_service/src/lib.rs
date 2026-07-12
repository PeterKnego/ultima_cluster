// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UC v2 service-side SDK (M5, spec §7).
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

mod apply;
mod attach;
mod config;
mod egress;
mod output;
mod replay;
mod traits;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use uc2_log::agent::{AgentRunner, IdleStrategy};
use uc2_log::cnc::CncPage;
use uc2_log::reader::LogFollower;

use crate::apply::apply_cycle;
use crate::output::{OutputState, output_cycle};

pub use crate::config::{ServiceConfig, ServiceError};
pub use crate::traits::{NoopOutput, OutputError, OutputHandler, StateMachine};

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
pub struct ServiceBuilder<S: StateMachine, O: OutputHandler<S> = NoopOutput> {
    cfg: ServiceConfig,
    sm: S,
    output: O,
}

impl<S: StateMachine> ServiceBuilder<S, NoopOutput> {
    pub fn new(cfg: ServiceConfig, sm: S) -> Self {
        Self { cfg, sm, output: NoopOutput }
    }
}

impl<S: StateMachine, O: OutputHandler<S>> ServiceBuilder<S, O> {
    /// Install a leader-only output handler (Task 12 spawns its agent).
    pub fn output_handler<O2: OutputHandler<S>>(self, h: O2) -> ServiceBuilder<S, O2> {
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
        // a pure no-op thread into existence.
        let mut agents = Vec::with_capacity(2);
        if std::any::TypeId::of::<O>() != std::any::TypeId::of::<NoopOutput>() {
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

        Ok(Service { agents, sm, _cnc: cnc, instance_id, epoch })
    }
}

/// A running service: the agent thread(s) plus the handles that keep the
/// shared-memory mappings alive.
pub struct Service<S: StateMachine> {
    agents: Vec<AgentRunner>,
    /// Shared handle to the apply thread's state machine, for direct queries.
    sm: Arc<Mutex<S>>,
    /// Held for the service's life so the mmap'd cnc page stays mapped.
    _cnc: Arc<CncPage>,
    instance_id: u128,
    epoch: u64,
}

impl<S: StateMachine> Service<S> {
    /// The node instance this service attached to (a change means the node
    /// restarted since attach — a reconstruction trigger, Task 9).
    pub fn instance_id(&self) -> u128 {
        self.instance_id
    }

    /// This service incarnation's epoch (the value it bumped `service_epoch`
    /// to at attach).
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Direct, synchronous query against the live state machine on the apply
    /// thread. This is the test/embedded read path only — the real client query
    /// path (linearizable barrier + egress answer) lands in Task 10/11. Hidden
    /// from docs: not part of the public read contract.
    #[doc(hidden)]
    pub fn query(&self, q: S::Query) -> S::QueryResponse {
        self.sm.lock().unwrap().query(q)
    }

    /// Graceful stop: signal every agent and join, propagating a work-closure
    /// panic (fail-loud in teardown).
    pub fn stop(self) {
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
