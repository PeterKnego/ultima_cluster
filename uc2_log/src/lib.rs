// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UC v2 log buffer + archive (M1).
//! Spec: docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md §4.

pub mod agent;
pub mod archive;
pub mod buffer;
pub mod cnc;
pub mod counters;
pub mod region;
pub mod state;
pub mod writer;
