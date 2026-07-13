//! Canonical wire spec for ultima_cluster shared-memory IPC.
//!
//! Protocol-version constants, magic bytes, and stable error codes, plus the
//! lock-free ring buffer primitives (SPSC/MPSC/Broadcast) and the UC v2 wire
//! spec (`v2`): the `cnc.dat` page layout, self-locating datagram header, and
//! per-message frame layouts.
//!
//! The pure data types in `version.rs`, `magic.rs`, and `error_codes.rs` remain
//! `core`-only-compatible — they don't import anything outside `core`. The ring
//! buffer primitives require `std::sync::atomic` and `memmap2`.

pub mod error_codes;
pub mod magic;
pub mod ring;
pub mod v2;
pub mod version;

pub use error_codes::ErrorCode;
pub use version::{MIN_COMPATIBLE, ProtocolVersion};
