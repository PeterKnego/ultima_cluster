// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The client's error taxonomy (spec §7 / M5 Task 10).

use std::time::Duration;

use uc2_log::cnc::CncError;
use uc_protocol::ring::RingError;

/// Why a `connect`, `submit`, or `query_*` call failed.
#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    /// A cnc-page attach problem other than app_id/version (bad magic/length/
    /// crc, or the underlying io error) — see the `From<CncError>` impl below,
    /// which routes [`CncError::AppIdMismatch`]/[`CncError::VersionMismatch`]
    /// to this enum's own flattened variants instead of wrapping them here.
    #[error("cnc attach error: {0}")]
    Cnc(CncError),
    /// A ring file could not be attached (missing/corrupt/bad-magic ring
    /// file under the instance dir).
    #[error("ring attach error: {0}")]
    Ring(#[from] RingError),
    #[error("cnc app_id mismatch: expected {expected:?}, got {actual:?}")]
    AppIdMismatch { expected: String, actual: String },
    #[error("cnc protocol version mismatch: local {local:#010x}, peer {peer:#010x}")]
    VersionMismatch { local: u32, peer: u32 },
    /// A request timed out AND the cnc header's `instance_id` no longer
    /// matches the value observed at `connect` — the node restarted mid-flight
    /// (a fresh boot re-creates the cnc page with a new random `instance_id`).
    /// Re-attaching is a v2.0 decision (out of scope here); the caller's only
    /// recourse today is a fresh `Client::connect`.
    #[error("node instance restarted: attached to {attached:#034x}, now {current:#034x}")]
    InstanceRestart { attached: u128, current: u128 },
    /// The node answered but is not a serving leader; `hint` is its best guess
    /// at the current leader (`None` = unknown).
    #[error("not leader (hint: {hint:?})")]
    NotLeader { hint: Option<u32> },
    /// No response arrived within the request timeout (default 10s, override
    /// via `UC2_CLIENT_TIMEOUT_MS`) and the node's `instance_id` is unchanged
    /// (so [`InstanceRestart`](Self::InstanceRestart) does not apply).
    #[error("request timed out after {0:?}")]
    Timeout(Duration),
    /// The client's broadcast consumer fell behind and the producer
    /// overwrote unread records before this request's answer (or a NOT_LEADER/
    /// RETRY addressed to it) could be read. Every other in-flight request on
    /// this client fails the same way in the same instant (lapped records are
    /// unrecoverable — v1 semantics: "unknown losses").
    #[error("response overwritten: consumer fell behind the broadcast ring (lapped, unknown losses)")]
    ResponseOverwritten,
    #[error("ingress backpressure: ring stayed full past the retry window")]
    BackpressureFull,
    /// A transient failure the caller should retry. `MSG_V2_RETRY` is only
    /// ever emitted before a command reaches the apply barrier, so retrying
    /// is side-effect-free (relevant for a lincheck driver classifying this
    /// as a no-op, not an indeterminate outcome).
    #[error("transient retry (no side effect applied yet)")]
    Retry,
    /// A (de)serialization failure — either bincode-encoding the outgoing
    /// command/query, or bincode-decoding the returned response.
    #[error("(de)serialization error: {0}")]
    Decode(String),
    /// The client was shut down while this request was still in flight.
    #[error("client shut down")]
    ShutDown,
}

/// Route [`CncError::AppIdMismatch`]/[`CncError::VersionMismatch`] to their
/// own flattened `ClientError` variants (nicer for callers to match on
/// directly); everything else (bad header/crc, io) wraps opaquely in
/// [`ClientError::Cnc`]. Written by hand (not `#[from]` on this variant)
/// specifically so it can special-case those two variants.
impl From<CncError> for ClientError {
    fn from(e: CncError) -> Self {
        match e {
            CncError::AppIdMismatch { expected, actual } => {
                ClientError::AppIdMismatch { expected, actual }
            }
            CncError::VersionMismatch { local, peer } => ClientError::VersionMismatch { local, peer },
            other => ClientError::Cnc(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_id_and_version_mismatch_flatten_to_dedicated_variants() {
        let e = ClientError::from(CncError::AppIdMismatch {
            expected: "a".into(),
            actual: "b".into(),
        });
        assert!(matches!(e, ClientError::AppIdMismatch { .. }));

        let e = ClientError::from(CncError::VersionMismatch { local: 1, peer: 2 });
        assert!(matches!(e, ClientError::VersionMismatch { local: 1, peer: 2 }));
    }

    #[test]
    fn other_cnc_errors_wrap_opaquely() {
        let e = ClientError::from(CncError::BadHeader);
        assert!(matches!(e, ClientError::Cnc(CncError::BadHeader)));
    }
}
