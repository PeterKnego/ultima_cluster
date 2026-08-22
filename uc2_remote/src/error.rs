// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Error types: the wire-level [`FrameError`] and the client-facing
//! [`RemoteError`].

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

/// Client-facing error.
///
/// The variants below `Io`/`Frame` are the *outcomes* of a request: a
/// [`crate::Ticket`] resolves to exactly one of `Ok(RemoteResponse)`,
/// [`RemoteError::Expired`], [`RemoteError::Unknown`],
/// [`RemoteError::PayloadTooLarge`], [`RemoteError::TimedOut`] or
/// [`RemoteError::Closed`]. Redirects, leader changes, retries and connection
/// loss are handled inside the client and are never surfaced as errors.
#[derive(Debug, Error)]
pub enum RemoteError {
    /// [`crate::RemoteConfig::validate`] refused the configuration, by name.
    /// Returned by [`crate::RemoteClient::connect`] before a socket is opened,
    /// so it can never be confused with "the cluster is unreachable".
    #[error("remote client configuration: {0}")]
    Config(String),
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The edge refused the handshake: wrong `app_id`, or an unsupported
    /// protocol version. Trying another member will not help.
    #[error("hello refused (reason {reason}): {detail}")]
    HelloRefused { reason: u8, detail: String },
    /// The `Sessioned` dedup window had already moved past this `seq`: whether
    /// the write committed is unknowable from here.
    #[error("session entry expired: the outcome of this request is unknowable")]
    Expired,
    /// The edge's `Engine` timed the slot out and the client was told not to
    /// re-send (`resend_on_unknown = false`): the write may or may not have
    /// committed.
    #[error("outcome unknown: the edge timed the request out")]
    Unknown,
    /// The payload exceeds what the node accepts. Never re-sent.
    #[error("payload too large")]
    PayloadTooLarge,
    /// The request's `request_timeout` budget ran out.
    #[error("request timed out")]
    TimedOut,
    /// No member could be reached — every address failed a full pass.
    #[error("no cluster member could be reached")]
    NoMembersReachable,
    /// The client was shut down (or dropped) with the request outstanding.
    #[error("client is closed")]
    Closed,
}
