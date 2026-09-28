// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Shared test-only `WholeStateSnapshot` fixture for #67 Task 2.
//!
//! `apply.rs`, `reconstruction.rs`, `output.rs` and `query.rs` each declare
//! their own private `CountSm { total: u64, last_applied: Option<u64> }` (same
//! shape, four distinct types — one per integration-test binary), so a single
//! `impl` can't cover them; this macro expands one at each call site instead.
//! Encodes the tuple with bincode, the same codec every one of those files
//! already pulls in for its `Cmd`.

#![allow(dead_code, unused_macros)] // not every file that includes this module uses the macro

/// `impl_count_sm_snapshot!(Type)` for any `{ total: u64, last_applied:
/// Option<u64> }` shape: encodes/decodes `(total, last_applied)` via bincode,
/// including the cursor, so the SDK's cursor check (`decode_state` must
/// restore `last_applied`) is satisfied for free.
macro_rules! impl_count_sm_snapshot {
    ($ty:ty) => {
        impl uc_service::WholeStateSnapshot for $ty {
            fn encode_state(&self) -> Result<Vec<u8>, uc_service::SnapshotError> {
                bincode::serde::encode_to_vec(
                    (self.total, self.last_applied),
                    bincode::config::standard(),
                )
                .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))
            }
            fn decode_state(&mut self, bytes: &[u8]) -> Result<(), uc_service::SnapshotError> {
                let ((total, last_applied), _): ((u64, Option<u64>), usize) =
                    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
                        .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
                self.total = total;
                self.last_applied = last_applied;
                Ok(())
            }
        }
    };
}
