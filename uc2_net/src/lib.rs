// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UC v2 replication data plane (M2).
//! Spec: docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md §5.

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

pub mod fault;
pub mod flow;
pub mod rebuild;
pub mod receiver;
pub mod sender;
