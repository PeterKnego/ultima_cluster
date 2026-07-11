// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UC v2 consensus state machine (M3: commit ranking only).
//! Spec: docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md §6.
//! Pure and sync by construction: no I/O, no threads, no clock, no
//! allocation on the hot path — the agent driving it does all I/O. The M4
//! election SM (votes, terms, truncation) grows in this crate around this
//! module, gated by the deterministic simulation (uc2_sim).

pub mod commit;
pub mod election;
pub mod reconcile;
