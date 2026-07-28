// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UC v2 wire crypto (spec: docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md).
//!
//! Pure-sync, like `uc2_consensus`: no `async`, no sockets, no clock reads —
//! time enters as an explicit `now_ns: u64` so the deterministic simulator
//! can drive this crate exactly as it drives `ElectionSm`. The only I/O is
//! reading key/allowlist files in constructors (`Identity::load`,
//! `Allowlist::load`/`reload_if_stale`).
//!
//! Every function that touches network-derived bytes returns [`CryptoError`]
//! rather than panicking — this crate is the first thing to see attacker
//! input off the wire, and a panic there is a remote DoS.
//!
//! Module layout (this task ships `identity`; later M8 tasks slot in beside
//! it without disturbing this file beyond `pub mod` lines and `CryptoError`
//! variants):
//! - `identity` (T2, this task): X25519 node keys + the peer allowlist.
//! - `schedule` (T3): group-key epoch schedule.
//! - `replay` (T4): per-peer counter replay window.
//! - `seal` (T5): AES-256-GCM seal/open over the wire envelope.
//! - `handshake` (T6): Noise IK.
//! - `group` (T7): group-key distribution.
//! - `rotation` (T8): key-epoch rotation.
//! - `transport` (T9): wiring into `uc2_net`.

pub mod identity;

/// Node identifier — matches `uc2_consensus::election::NodeId` (both `u32`,
/// intentionally not re-exported from there: this crate stays dependency-thin
/// and must not pull in `uc2_consensus`).
pub type NodeId = u32;

/// Errors this crate returns instead of panicking. Untrusted bytes (key
/// files on disk, and — from T5/T6 onward — datagrams off the wire) are
/// adversarial input; every fallible path here ends in `Result`, never
/// `unwrap`/`expect`/index-panic. Deliberately not `#[non_exhaustive]`: later
/// tasks (T5 `AuthFailed`/`TooShort`, T7 `NoGroupKey`) add variants directly,
/// and this crate has no external consumers to protect from that yet.
#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("key file {path} has mode {mode:o}; must not be group- or world-readable")]
    KeyFilePermissions { path: String, mode: u32 },
    #[error("key file {0} is unreadable or not 32 bytes")]
    KeyFileInvalid(String),
    #[error("allowlist line {line} is malformed")]
    MalformedAllowlist { line: usize },
    #[error("allowlist lists node id {0} more than once")]
    DuplicateAllowlistId(NodeId),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
