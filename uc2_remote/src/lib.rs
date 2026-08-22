// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2_remote`: the ultima_cluster remote protocol v1 (framed TCP) — a
//! deliberately tiny wire codec (`bytes`, `thiserror` only) meant to be easy
//! to re-implement in a non-Rust gateway port. See
//! `docs/reference/remote-protocol.md` and design spec §4.2.

pub mod client;
pub mod conn;
pub mod error;
pub mod frame;

pub use client::{RemoteClient, RemoteConfig, RemoteResponse, RemoteStats, Ticket};
pub use conn::FramedConn;
pub use error::{FrameError, RemoteError};
