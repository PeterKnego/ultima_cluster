// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Error types: the wire-level [`FrameError`] and the client-facing
//! [`RemoteError`] (fully fleshed out in Task 7 — for now it only wraps
//! [`FrameError`] and I/O errors).

use thiserror::Error;

/// Errors from decoding/encoding a single frame or typed payload.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("short buffer: need {need}, have {have}")]
    Short { need: usize, have: usize },
    #[error("frame too long: {0} bytes")]
    TooLong(u32),
    #[error("bad frame type: {0}")]
    BadType(u8),
    #[error("bad protocol version: {0}")]
    BadVersion(u16),
    #[error("bad payload: {0}")]
    BadPayload(&'static str),
}

/// Client-facing error (Task 7 grows this further).
#[derive(Debug, Error)]
pub enum RemoteError {
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
