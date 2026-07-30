// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The consensus safety core: commit ranking, elections, term maps, and
//! truncation.
//!
//! **Pure and synchronous by construction** — no I/O, no threads, no clock, no
//! allocation on the hot path. Time and messages enter as explicit inputs; the
//! state machine emits actions, and the agent driving it
//! ([`uc2_node`](https://docs.rs/uc2_node)) performs every side effect. That is
//! what makes this crate exhaustively testable by the deterministic simulator
//! (`uc2_sim`) and directly mirrorable in Lean.
//!
//! The kernels here — `CommitTracker`, `reconcile`, and vote freshness — are
//! the ones carrying machine-checked proofs, replayed against their Lean model
//! vector-by-vector by the conformance rig. See `docs/VERIFICATION.md`.

pub mod commit;
pub mod config;
pub mod election;
pub mod reconcile;
