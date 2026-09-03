// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The `ultima_cluster` node — the process that participates in consensus,
//! replicates the log, and makes it durable.
//!
//! [`Node::start`] spawns the agent threads and returns immediately; the node
//! then runs until dropped. A service (`uc_service`) and clients
//! (`uc_client`) attach to it through the instance directory's shared
//! memory. See `docs/ARCHITECTURE.md` for how the pieces fit together, and
//! `docs/QUICKSTART.md` for a runnable cluster.
//!
//! One [`Node`] owns four single-writer polling agents over a shared
//! [`uc_log::buffer::LogBuffer`]:
//!
//! - **consensus** — drives the [`uc_consensus::election::ElectionSm`], the
//!   SOLE writer of the leadership-term handle AND the commit counter (both
//!   roles), executes every SM action honoring the persist/ordering contracts,
//!   and owns the [`uc_log::buffer::Appender`] while leader.
//! - **sender** — streams DATA / serves NAKs / heartbeats when leader
//!   (role-gated); inert as a follower.
//! - **receiver** — a single unified follower-receiver: accepts DATA and NAKs
//!   as a follower, demuxes inbound NAK/STATUS to the sender as a leader, and
//!   routes all consensus datagrams (kinds 5–9) to the consensus agent.
//! - **archive** — records durable blocks, feeds data-stamped term
//!   observations to the consensus agent, and services truncation commands.
//!
//! Spec: `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md`
//! §3.2 / §6; plan `docs/superpowers/plans/2026-07-11-uc2-m4-elections.md`
//! Task 8.
//!
//! **Semver:** see `docs/reference/semver-policy.md`. Promised surface:
//! [`NodeConfig`] and [`Node::start`]/[`Node::start_with`], plus the
//! `node.toml` file `NodeConfig` mirrors. The other modules (`audit`,
//! `backup`, `ipc`, `obs`, `preflight`, `recovery`) are internal.

pub mod audit;
pub mod backup;
pub mod config_file;
pub mod ipc;
#[cfg(feature = "mutation-testing")]
pub(crate) mod mutation;
mod node;
pub mod obs;

/// Re-exported so the 33 in-crate `crate::obs_event!(…)` call sites — and
/// anything outside naming `uc_node::obs_event!` — keep resolving after the
/// macro moved to [`uc_obs`].
pub use uc_obs::obs_event;
pub mod preflight;
mod read_round;
pub mod recovery;
mod schedule_state;
pub mod services;
pub(crate) mod timers;

pub use config_file::load_from_path;
pub use ipc::{InstanceDir, IpcError};
pub use node::{
    DEFAULT_JOURNAL_SEGMENT_BYTES, DrainOutcome, Node, NodeConfig, PurgePolicy,
    REASON_AUDIT_FAILED, REASON_AUTH_BAD_TAG, REASON_AUTH_EXPIRED, REASON_AUTH_MISSING,
    REASON_AUTH_UNKNOWN_KEY, REASON_SCHEDULE_DECODE, REASON_SCHEDULE_DIGEST,
    REASON_SCHEDULE_MISSING, REASON_SCHEDULE_UNKNOWN_FSM, StartOpts, SubmitError,
};
/// Time-and-timers plan 2 (spec §5): the durable schedule-table record, the
/// staged-file name, and the digest. Re-exported because `uc2ctl` must stage
/// the file the node reads, compute the SAME digest over it, and read the
/// record back for `schedule show` — two implementations of that digest
/// would make every apply refuse with [`REASON_SCHEDULE_DIGEST`].
pub use schedule_state::{
    SCHEDULE_PENDING_FILE, SCHEDULE_STATE_FILE, ScheduleRecord, schedule_digest,
};
pub use services::{FsmLag, ServicesConfig};
/// Time-and-timers §6: the per-row timer counters carried by
/// [`obs::ObsSources`]. Re-exported (the module itself stays crate-private —
/// `RowTimers` is consensus-agent internals) so an out-of-crate caller that
/// builds an `ObsSources` by hand can name the type.
pub use timers::TimerStats;
/// M8: node-to-node wire crypto configuration, re-exported so a deployment
/// that only depends on `uc_node` can build a [`NodeConfig`] without naming
/// `uc_crypto` directly. `CryptoConfig::Disabled` (the `Default`) is exactly
/// the pre-M8 cleartext behavior.
pub use uc_crypto::CryptoConfig;
/// M12b: admin-request authentication, re-exported so a deployment that only
/// depends on `uc_node` can build a [`StartOpts`] without naming
/// `uc_crypto` directly. [`AdminPolicy::Filesystem`] (the `Default`) is
/// exactly the pre-M12b posture: the instance directory's permissions are the
/// admin boundary and the cnc auth line is ignored.
pub use uc_crypto::admin::{AdminKey, AdminPolicy};
