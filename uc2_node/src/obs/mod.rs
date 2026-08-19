// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Observability: structured logging (M10).
//!
//! Zero-dependency, `std`-only structured-log core. Later M10 tasks emit
//! records through [`crate::obs_event!`] at consensus transition sites and
//! read [`log::LogLevel`] from the config file.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};

use uc2_log::cnc::CncPage;

pub mod log;

/// A read-only bundle of the `Arc`-shared counters, flags, and config values
/// a later task's metrics encoder renders into a series — a straight
/// clone-and-collect over the fields [`crate::node::Node`] already owns, with
/// no new synchronization: every `Arc` here is the SAME allocation the owning
/// agent writes through.
///
/// `admission_bytes` is deliberately NOT here — the encoder reads it from the
/// live cnc page (`CncPage::admission_bytes()`) instead of a config-copied
/// snapshot, so there is exactly one source of truth for it.
pub struct ObsSources {
    pub node_id: u32,
    pub cnc: Arc<CncPage>,
    pub sender: Arc<uc2_net::sender::SenderStats>,
    pub receiver: Arc<uc2_net::receiver::FollowerStats>,
    pub truncations: Arc<AtomicU64>,
    pub wipes: Arc<AtomicU64>,
    pub reports_unattested: Arc<AtomicU64>,
    pub reports_implausible: Arc<AtomicU64>,
    pub crypto_handshake_failures: Arc<AtomicU64>,
    pub crypto_enabled: bool,
    pub purge_enabled: bool,
    pub journal_segment_bytes: u64,
    /// One entry per polling agent, in the FIXED order `consensus, sender,
    /// receiver, archive` regardless of spawn order — a later task's metric
    /// labels are positional against this order, so it must not drift with
    /// `Node::start`'s internal spawn sequence.
    pub agents: Vec<(&'static str, Arc<AtomicBool>)>,
}
