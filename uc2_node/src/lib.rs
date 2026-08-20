// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The `ultima_cluster` node — the process that participates in consensus,
//! replicates the log, and makes it durable.
//!
//! [`Node::start`] spawns the agent threads and returns immediately; the node
//! then runs until dropped. A service (`uc2_service`) and clients
//! (`uc2_client`) attach to it through the instance directory's shared
//! memory. See `docs/ARCHITECTURE.md` for how the pieces fit together, and
//! `docs/QUICKSTART.md` for a runnable cluster.
//!
//! One [`Node`] owns four single-writer polling agents over a shared
//! [`uc2_log::buffer::LogBuffer`]:
//!
//! - **consensus** — drives the [`uc2_consensus::election::ElectionSm`], the
//!   SOLE writer of the leadership-term handle AND the commit counter (both
//!   roles), executes every SM action honoring the persist/ordering contracts,
//!   and owns the [`uc2_log::buffer::Appender`] while leader.
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

pub mod backup;
pub mod config_file;
pub mod ipc;
#[cfg(feature = "mutation-testing")]
pub(crate) mod mutation;
mod node;
pub mod obs;
pub mod preflight;
mod read_round;

pub use config_file::load_from_path;
pub use ipc::{InstanceDir, IpcError};
pub use node::{
    DEFAULT_JOURNAL_SEGMENT_BYTES, DrainOutcome, Node, NodeConfig, PurgePolicy, SubmitError,
};
/// M8: node-to-node wire crypto configuration, re-exported so a deployment
/// that only depends on `uc2_node` can build a [`NodeConfig`] without naming
/// `uc2_crypto` directly. `CryptoConfig::Disabled` (the `Default`) is exactly
/// the pre-M8 cleartext behavior.
pub use uc2_crypto::CryptoConfig;
