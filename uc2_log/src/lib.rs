// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The replicated log: a shared-memory ring buffer, and the archive agent that
//! makes it durable.
//!
//! The buffer is one mmap'd power-of-2 ring per node, addressed by absolute
//! byte position rather than by entry index — which is what lets replication be
//! a byte-stream fan-out and lets the buffer double as the retransmit buffer.
//! The archive agent records it to [`ultima_journal`] in CRC'd blocks with one
//! `fdatasync` per block; that is the only fsync site in the system.
//!
//! Spec: `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md` §4.
//!
//! **Semver:** see `docs/reference/semver-policy.md`. Promised surface: **none** —
//! every module here is public for the workspace's own use and may change
//! in any release.

pub mod agent;
/// Re-export: `ArchiveConfig::durability`'s type, so archive callers can
/// name the posture without a direct `ultima_journal` dependency.
pub use ultima_journal::Durability;

pub mod archive;
pub mod buffer;
pub mod cnc;
pub mod counters;
pub mod reader;
pub mod region;
pub mod state;
pub mod writer;
