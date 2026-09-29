// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! A replicated counter — the smallest useful `ultima_cluster` state machine.
//!
//! This file is the part worth reading. Everything else in this crate is
//! process wiring; the state machine itself is the four associated types and
//! three methods below.

use serde::{Deserialize, Serialize};
use uc_service::{ApplyCtx, StateMachine};

/// What clients send. Commands go through consensus and are applied on every
/// replica, in the same order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    /// Add `n` to the counter (negative to subtract).
    Add(i64),
    /// Set the counter back to zero.
    Reset,
}

/// What a client gets back from `submit`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Applied {
    /// The counter's value *after* this command was applied.
    pub value: i64,
    /// The absolute byte position this command occupies in the replicated log.
    /// Stable forever, and the natural idempotency key.
    pub position: u64,
}

/// What clients ask. Queries do not go through consensus — they are answered
/// from local state, optionally behind a read barrier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Query {
    Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub value: i64,
}

/// The replicated state: one integer.
#[derive(Default)]
pub struct CounterSm {
    value: i64,
    last_applied: Option<u64>,
}

impl StateMachine for CounterSm {
    const NAME: &'static str = "counter";

    type Command = Command;
    type Response = Applied;
    type Query = Query;
    type QueryResponse = QueryResponse;

    /// Called on **every** replica, for every committed command, in log order.
    ///
    /// This function must be deterministic: same state plus same command must
    /// produce the same next state on every node, forever. No clocks, no
    /// randomness, no I/O, no `HashMap` iteration order, no floating point
    /// where you care about the last bit. Two replicas that disagree by one bit
    /// have silently forked, and no consensus layer can detect that for you.
    ///
    /// Note `wrapping_add` rather than `+`. Plain `+` panics on overflow in
    /// debug builds and wraps in release — the same command producing different
    /// behaviour depending on how a replica was compiled is exactly the kind of
    /// nondeterminism that fractures a cluster. It is a contrived risk for a
    /// counter and a very real one in a matching engine.
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Command) -> Applied {
        match cmd {
            Command::Add(n) => self.value = self.value.wrapping_add(n),
            Command::Reset => self.value = 0,
        }
        let position = ctx.position;
        self.last_applied = Some(position);
        Applied {
            value: self.value,
            position,
        }
    }

    /// Answer a read from local state. Whether the caller gets a linearizable
    /// or a snapshot read is decided by the client and enforced by the
    /// framework — this method is the same either way.
    fn query(&self, q: Query) -> QueryResponse {
        match q {
            Query::Value => QueryResponse { value: self.value },
        }
    }

    /// Where this state machine left off, so the framework knows what to replay
    /// on restart. Under-reporting is safe (already-applied frames are skipped);
    /// claiming to be further along than the log is refused at attach.
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

/// Snapshots are required (#67). A counter's whole state is two numbers, so
/// the simple helper fits: encode the state, decode it back, and the SDK
/// handles the rest. A state machine with a large state should implement
/// `SnapshotStateMachine` itself instead — see `docs/reference/state-machine-contract.md`.
impl uc_service::WholeStateSnapshot for CounterSm {
    fn encode_state(&self) -> Result<Vec<u8>, uc_service::SnapshotError> {
        bincode::serde::encode_to_vec((self.value, self.last_applied), bincode::config::standard())
            .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))
    }
    fn decode_state(&mut self, bytes: &[u8]) -> Result<(), uc_service::SnapshotError> {
        let ((value, last_applied), _): ((i64, Option<u64>), _) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard())
                .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
        self.value = value;
        self.last_applied = last_applied;
        Ok(())
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use uc_service::SnapshotStateMachine;

    #[test]
    fn counter_round_trips_value_and_cursor() {
        let c = CounterSm {
            value: -5,
            last_applied: Some(640),
        };
        let (h, pos) = c.freeze().unwrap();
        let mut bytes = Vec::new();
        CounterSm::stream_snapshot(h, &mut bytes).unwrap();
        let mut d = CounterSm::default();
        d.install_snapshot(pos + 64, &mut &bytes[..]).unwrap();
        assert_eq!((d.value, d.last_applied), (-5, Some(640)));
    }
}
