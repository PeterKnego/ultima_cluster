// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Optional authenticated and encrypted node-to-node transport.
//!
//! Off by default. X25519 identities on a runtime-reloadable allowlist, a Noise
//! `IK` handshake for pairwise keys, and a rotating cluster group key for the
//! byte-identical fan-out traffic so the leader seals once and sends N times.
//! The threat model is a network-path adversary; a compromised host and a
//! malicious cluster member are explicitly out of model — see
//! `docs/VERIFICATION.md` §11 and `docs/security/threat-model.md`.
//!
//! Pure-sync, like `uc_consensus`: no `async`, no sockets, no clock reads —
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
//!   the single `seal`/`open` entry point `uc_net` (T10/T11) calls.
//!
//! **Semver:** see `docs/reference/semver-policy.md`. Promised surface: **none** —
//! every module here is public for the workspace's own use and may change
//! in any release.

pub mod admin;
pub mod group;
pub mod handshake;
pub mod identity;
pub mod replay;
pub mod rotation;
pub mod schedule;
pub mod seal;
pub mod transport;

pub use handshake::HandshakeAction;
pub use transport::{CryptoConfig, ReceiveHalf, Scope, SendHalf, SharedTransport, Transport};

/// Node identifier — matches `uc_consensus::election::NodeId` (both `u32`,
/// intentionally not re-exported from there: this crate stays dependency-thin
/// and must not pull in `uc_consensus`).
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
    /// crash the moment a new fan-out kind is added to `uc_net` without
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
    /// [`transport::SharedTransport::seal_pairwise_control`] (T12 — the node
    /// layer's own seal path for rare unicast control datagrams) was called
    /// with a [`Scope::Group`](transport::Scope::Group) `kind`. Group sealing
    /// belongs on the fan-out path, which owns the epoch stamping and the
    /// per-epoch cipher cache; routing a fan-out kind through the control
    /// path would seal it correctly but bypass both, so it is refused rather
    /// than quietly done the slow way. A caller-contract violation (`kind`
    /// is the caller's own routing decision, never bytes off the wire) —
    /// returned as a `Result` rather than panicked, for the same reason
    /// [`CryptoError::MissingPeer`] is.
    #[error("seal_pairwise_control: kind {0} is Scope::Group — seal it on the fan-out path")]
    NotPairwiseKind(u8),
    /// `admin::AdminKey::load` read a key file that is not exactly
    /// [`admin::ADMIN_KEY_LEN`] bytes. Refused outright rather than
    /// truncated or zero-padded — a short/long key file is almost always an
    /// operator mistake (wrong file, partial write) and silently coercing
    /// it to length would sign/verify under a key the operator didn't
    /// intend.
    #[error(
        "admin key file {path} is {len} bytes, must be exactly {} bytes",
        admin::ADMIN_KEY_LEN
    )]
    AdminKeyLength { path: String, len: usize },
    /// `admin::generate_key_file` was asked to write to a path that already
    /// exists. Never overwrites a key file — an operator regenerating by
    /// accident could otherwise invalidate every client still holding the
    /// old key with no warning.
    #[error("admin key file {path} already exists; refusing to overwrite")]
    AdminKeyExists { path: String },
}
