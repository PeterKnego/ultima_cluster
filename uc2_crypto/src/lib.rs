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
//! - `transport` (T9): the facade — scope-by-kind, config, boot refusal,
//!   the single `seal`/`open` entry point `uc2_net` (T10/T11) calls.

pub mod group;
pub mod handshake;
pub mod identity;
pub mod replay;
pub mod rotation;
pub mod schedule;
pub mod seal;
pub mod transport;

pub use handshake::HandshakeAction;
pub use transport::{CryptoConfig, Scope, Transport};

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
    /// AEAD tag verification failed: wrong key, tampered ciphertext, or a
    /// tampered header (the header is authenticated as associated data —
    /// see `seal.rs`). Deliberately carries no detail: telling an attacker
    /// *which* check failed narrows their next guess for free.
    ///
    /// `seal_in_place` also maps its (effectively unreachable — only past
    /// AES-GCM's ~64 GiB plaintext ceiling, never reachable for an
    /// MTU-bounded UC datagram) encrypt-side failure to this same variant.
    /// That path never runs against attacker-controlled input, so an
    /// operator using `AuthFailed` as an attack indicator should read it as
    /// "someone tampered with or forged a datagram," not worry about the
    /// dead encrypt-side branch muddying that signal.
    #[error("AEAD authentication failed")]
    AuthFailed,
    /// A sealed datagram (or a buffer claiming to be one) is shorter than
    /// `DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD` — too short to hold even an
    /// empty payload's header, counter, and tag. Checked before any
    /// indexing into the buffer, never after.
    #[error("datagram too short to be a sealed frame")]
    TooShort,
    /// No pairwise session with this peer is usable yet (`handshake.rs`, T6):
    /// the handshake has not completed, or every session for the peer was
    /// dropped. The caller's move is to drop the datagram and let
    /// `Peers::tick` bring the link up — not to fall back to sending in the
    /// clear, which would make the whole feature optional per datagram.
    #[error("no established pairwise session with node {0}")]
    NoSession(NodeId),
    /// The datagram opened correctly but its counter has already been seen
    /// under this key (`replay::ReplayWindow`). AEAD stops forgery, not
    /// replay, and a replayed `VOTE` or admin datagram is not harmless.
    #[error("counter {0} was already seen under this key")]
    Replayed(u64),
    /// Sealing (or opening) a `Scope::Group` datagram with no usable group
    /// key: no epoch has ever activated yet (send side), or the epoch named
    /// in a received datagram's header is not one this node holds (receive
    /// side — a peer on a newer epoch we have not received `HS_KEY` for, or
    /// a rotated-out one). Both cases self-heal through the existing NAK
    /// repair path once `HS_KEY` lands (`group.rs`, T7/T9); this variant is
    /// never a signal to fall back to sending or accepting cleartext.
    #[error("no usable group key")]
    NoGroupKey,
    /// `Transport::seal` was called with a `Scope::Pairwise`-classified
    /// `kind` (`u8`, carried for the operator log) and `peer: None`. A
    /// caller-contract violation (`kind`/`peer` are the caller's own routing
    /// decision, never bytes off the wire), but returned as a `Result`
    /// rather than panicked (T9 review round 1, adjudicated): `scope_of`'s
    /// deliberate `_ => Pairwise` catch-all exists precisely so that an
    /// unclassified FUTURE kind degrades to a missed optimization, never a
    /// crash — panicking here would convert exactly that miss into a node
    /// crash the moment a new fan-out kind is added to `uc2_net` without
    /// updating `scope_of`, defeating the design rationale sitting right
    /// beside it.
    #[error("Transport::seal: Pairwise-scope kind {0} requires Some(peer)")]
    MissingPeer(u8),
    /// `Transport::seal`/`open` was called with a `Scope::Unsealed` kind
    /// (`u8`, carried for the operator log) — the `HS_INIT`/`HS_RESP`
    /// handshake bootstrap kinds (spec §5). No session and no key exist yet
    /// for these; they are what CREATES a session and must be driven
    /// directly via `Peers::initiate`/`Peers::on_message`, never through
    /// this facade. Added T9 review round 1 (F4): the pre-fix `scope_of`
    /// left these kinds unclassified, falling through to the `Pairwise`
    /// catch-all — which fails closed today (no session exists, so
    /// `NoSession`/`AuthFailed`), but for the wrong stated reason, and a
    /// future refactor of the catch-all's behavior could silently change
    /// that. Named explicitly instead.
    #[error("Transport::seal/open: kind {0} is Scope::Unsealed — never goes through seal/open")]
    UnsealedKind(u8),
}
