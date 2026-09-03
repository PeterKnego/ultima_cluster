// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Deterministic election simulation (spec §8 L1).
//!
//! `uc_sim` drives the pure [`uc_consensus::election::ElectionSm`] (plus its
//! reconciliation core) inside a single-threaded, virtual-time discrete-event
//! world with seeded message faults (drop / duplicate / delay / reorder),
//! network partitions, and injected crash/restart. After **every** event the
//! five safety invariants of spec §8 are checked; any violation aborts the run
//! with a `(seed, step)`-tagged [`invariants::InvariantViolation`].
//!
//! This crate exists — and must be green — *before* any networked election
//! (uc_net, Task 8) is trusted: it is the exhaustive gate on the election
//! machinery. A failing fuzz seed is a bug to fix, never an invariant to
//! weaken (pin it as a named regression test).
//!
//! ## The binding model
//!
//! A node's log CONTENT is fully described by `(term_map, position)` pairs —
//! within a term the bytes are identical cluster-wide because a single leader
//! wrote them (spec §6). So divergence, truncation, and leader completeness are
//! all decidable on `(term, base)` maps and byte positions alone; the sim
//! carries no byte arrays. See [`world`] for the node/event model.

pub mod invariants;
pub mod timers;
pub mod world;
