// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UC v2 minimal node composition (M4, spec §3.2).
//!
//! The seed of the composition crate: agent wiring and role switching only —
//! no discovery dir, no `instance.lock`, no cnc mmap, no client IPC (all M5).
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

pub mod ipc;
#[cfg(feature = "mutation-testing")]
pub(crate) mod mutation;
mod node;
mod read_round;

pub use ipc::{InstanceDir, IpcError};
pub use node::{DEFAULT_JOURNAL_SEGMENT_BYTES, Node, NodeConfig, PurgePolicy, SubmitError};
/// M8: node-to-node wire crypto configuration, re-exported so a deployment
/// that only depends on `uc2_node` can build a [`NodeConfig`] without naming
/// `uc2_crypto` directly. `CryptoConfig::Disabled` (the `Default`) is exactly
/// the pre-M8 cleartext behavior.
pub use uc2_crypto::CryptoConfig;
