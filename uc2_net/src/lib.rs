// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UC v2 replication data plane (M2).
//! Spec: docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md §5.

pub mod fault;
pub mod flow;
pub mod rebuild;
pub mod sender;
