// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UC v2 protocol layouts (spec: docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md).
//! Core-only modules — the multi-language gate for protocol v2.

pub mod cnc;
pub mod config;
pub mod crypto;
pub mod datagram;
pub mod frame;
pub mod ipc;
