// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared test scaffolding for the `uc_remote` client tests.
//!
//! Compiled independently into each test binary, so a helper used by only
//! some of them reads as dead in the others — allow it module-wide.
#![allow(dead_code)]

pub mod fake_edge;
