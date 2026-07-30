// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The replication data plane: reliable UDP with NAK repair, quorum-paced flow
//! control, and journal replay sessions.
//!
//! Datagrams are self-locating — each carries the absolute stream position of
//! its first byte — so a follower writes them straight into its own log buffer
//! at the right offset, and duplicates or reordering are idempotent by
//! construction. Loss is repaired by NAK, served by re-reading the log buffer.
//!
//! Spec: `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md` §5.

use std::sync::Arc;
use std::sync::atomic::AtomicU32;

/// Shared, mutable leadership term (M4). The consensus agent (Task 8) is the
/// SOLE writer; the data-path agents (sender/receivers) only LOAD it (`Relaxed`)
/// — per datagram (stamping) or per duty cycle (the receiver term filter). A
/// term bump is a single atomic store, so a mid-flight datagram may carry the
/// term that was live when it was stamped; that transient skew is exactly what
/// the liveness/filter checks tolerate (safety rides on FRAME terms, not
/// datagram terms — see `sender`'s node-mode notes).
pub type TermHandle = Arc<AtomicU32>;

/// Test-only crypto fixtures shared by `sender`'s and `receiver`'s unit
/// tests (M8 Task 17) — see the module docs. Never compiled into the library.
#[cfg(test)]
pub(crate) mod crypto_testkit;

pub mod fault;
pub mod flow;
pub mod rebuild;
pub mod receiver;
pub mod sender;
