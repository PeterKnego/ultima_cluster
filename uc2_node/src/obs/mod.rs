// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Observability: structured logging (M10).
//!
//! Zero-dependency, `std`-only structured-log core. Later M10 tasks emit
//! records through [`crate::obs_event!`] at consensus transition sites and
//! read [`log::LogLevel`] from the config file.

pub mod log;
