//! Canonical wire spec for ultima_cluster shared-memory IPC.
//!
//! Protocol-version constants, magic bytes, and stable error codes, plus the
//! lock-free ring buffer primitives (SPSC/MPSC/Broadcast) and the UC v2 wire
//! spec (`v2`): the `cnc.dat` page layout, self-locating datagram header, and
//! per-message frame layouts.
//!
//! The pure data types in `version.rs`, `magic.rs`, `error_codes.rs`, and
//! `identity.rs` remain `core`-only-compatible — they don't import anything
//! outside `core`. The ring buffer primitives require `std::sync::atomic` and
//! `memmap2`.
//!
//! **Semver:** see `docs/reference/semver-policy.md`. Promised surface: **none** as
//! Rust API — this is the wire spec, governed by the flag-day rule
//! (`version::CURRENT` and the cnc page version), not by semver.

pub mod error_codes;
pub mod identity;
pub mod magic;
pub mod ring;
pub mod v2;
pub mod version;

pub use error_codes::ErrorCode;
pub use identity::{FsmIdentity, FsmName};
pub use version::{MIN_COMPATIBLE, ProtocolVersion};
