//! Canonical wire spec for ultima_cluster shared-memory IPC.
//!
//! M1 shipped protocol-version constants, magic bytes, and stable error codes.
//! M3 adds the ring buffer primitives (SPSC/MPSC/Broadcast), the `cnc.dat`
//! shared-memory control file layout, per-RPC frame types, and the heartbeat
//! + handshake helpers.
//!
//! The crate was `no_std`-friendly in M1; M3 relaxes that posture because ring
//! buffer primitives require `std::sync::atomic` and `memmap2`. The pure data
//! types in `version.rs`, `magic.rs`, and `error_codes.rs` remain
//! `core`-only-compatible — they don't import anything outside `core`.

pub mod cnc;
pub mod error_codes;
pub mod frames;
pub mod handshake;
pub mod liveness;
pub mod magic;
pub mod probes;
pub mod ring;
pub mod snapshot_region;
pub mod v2;
pub mod version;

pub use error_codes::ErrorCode;
pub use version::{MIN_COMPATIBLE, ProtocolVersion};
