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
mod replay;
mod traits;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use uc2_log::agent::{AgentRunner, IdleStrategy};
use uc2_log::cnc::CncPage;

use crate::apply::apply_cycle;

pub use crate::config::{ServiceConfig, ServiceError};
pub use crate::traits::{NoopOutput, OutputError, OutputHandler, StateMachine};

/// Default idle strategy for the apply thread: a short sleep between empty
/// cycles (a busy-spin knob comes later). Background-grade politeness that
/// still keeps sub-ms apply latency under load.
const APPLY_IDLE: IdleStrategy = IdleStrategy::Sleep(Duration::from_micros(50));

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
    /// discipline; step 6 spawns the apply thread here.
    pub fn start(self) -> Result<Service<S>, ServiceError> {
        let ServiceBuilder { cfg, sm, output } = self;
        // Task 12 spawns the output agent from this handler; unused in M5 Task 8.
        let _ = output;

        let attached = attach::attach(&cfg, sm)?;
        let cnc = attached.cnc;
        let instance_id = attached.instance_id;
        let epoch = attached.epoch;

        // 6. Spawn the apply thread. `AgentRunner::drop` already signals+joins,
        //    so a spawn failure below cannot leak a running thread. Keep a shared
        //    handle to the state machine so the `Service` can answer direct
        //    queries (test/embedded path until the client query ring, Task 11).
        let mut state = attached.apply_state;
        let sm = Arc::clone(&state.sm);
        let apply_agent = AgentRunner::spawn("uc2-apply", APPLY_IDLE, move || apply_cycle(&mut state))?;

        Ok(Service { agents: vec![apply_agent], sm, _cnc: cnc, instance_id, epoch })
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
