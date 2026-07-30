// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! A replicated counter — the smallest useful `ultima_cluster` state machine.
//!
//! This file is the part worth reading. Everything else in this crate is
//! process wiring; the state machine itself is the four associated types and
//! three methods below.

use serde::{Deserialize, Serialize};
use uc2_service::StateMachine;

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
    fn apply(&mut self, position: u64, cmd: Command) -> Applied {
        match cmd {
            Command::Add(n) => self.value = self.value.wrapping_add(n),
            Command::Reset => self.value = 0,
        }
        self.last_applied = Some(position);
        Applied { value: self.value, position }
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
