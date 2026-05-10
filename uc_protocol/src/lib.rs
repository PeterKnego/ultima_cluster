//! Canonical wire spec for ultima_cluster shared-memory IPC.
//!
//! M1 only ships protocol-version constants, magic bytes, and stable error codes.
//! Ring buffer types and frame layouts arrive in M3.

#![cfg_attr(not(test), no_std)]

pub mod error_codes;
pub mod magic;
pub mod version;

pub use error_codes::ErrorCode;
pub use version::{MIN_COMPATIBLE, ProtocolVersion};
