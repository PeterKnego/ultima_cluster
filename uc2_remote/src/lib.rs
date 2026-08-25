// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2_remote`: the ultima_cluster remote protocol v1 (framed TCP) — a
//! deliberately tiny wire codec (`bytes`, `thiserror` only) meant to be easy
//! to re-implement in a non-Rust gateway port. See
//! `docs/reference/remote-protocol.md` and design spec §4.2.
//!
//! **Semver:** see `docs/reference/semver-policy.md`. Promised surface: the
//! **wire format** — remote protocol v1 ([`frame::PROTOCOL_VERSION`]) — and
//! [`RemoteClient`]. The Rust items that encode the wire are not themselves
//! promised; a port re-implements the format, not this API.

pub mod client;
pub(crate) mod completion;
pub mod conn;
pub mod error;
pub mod frame;
pub(crate) mod outgoing;
pub(crate) mod park;

pub use client::{RemoteClient, RemoteConfig, RemoteResponse, RemoteStats, Ticket};
pub use conn::FramedConn;
pub use error::{FrameError, RemoteError};
