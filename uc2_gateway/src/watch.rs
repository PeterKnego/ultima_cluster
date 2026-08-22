// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The **leader watch** (spec §4.3): the edge's one piece of proactive
//! behaviour.
//!
//! Everything else the edge does is reactive — a client asks, the edge
//! answers. That is enough for a client with a request in flight, which learns
//! about a leader change from the `REDIRECT` its next `SUBMIT` earns. It is
//! *not* enough for a client that is connected and idle, or one whose requests
//! are all parked on a `RETRY` backoff: nothing would ever tell it the cluster
//! had moved, and it would sit on a follower's edge until it happened to try
//! again.
//!
//! So the driver samples two words off the cnc page — `can_serve` and
//! `leader_hint` — and pushes `LEADER_CHANGED` to every ready connection when
//! either changes.
//!
//! ## Why the transition, and not the state, is the trigger
//!
//! Sampling is cheap (two atomic loads); *acting* on a sample is not — it is
//! one frame per connection. Pushing on every sample would be a frame storm
//! proportional to the poll rate; pushing on a transition is one frame per
//! connection per actual change. [`LeaderWatch`] exists precisely to hold that
//! edge-triggered discipline in one place, with the state it needs (the last
//! sample) and nothing else.
//!
//! ## What gets pushed
//!
//! The **current leader**, never "this node is not the leader". The frame the
//! client acts on is `Leader { node_id, addr }`: an address it can reconnect
//! to. When the hint names a member we know the gateway address of, that is
//! what it gets; when the hint is unknown (mid-election, or a node id missing
//! from the static member map) it gets the `Leader { u32::MAX, "" }` sentinel,
//! which `RemoteClient` reads as "leader unknown: reconnect and re-`HELLO`" —
//! the same sentinel the instance-restart path already uses.

use uc2_client::SendHalf;

/// The last `(can_serve, leader_hint)` the driver saw, so a change can be
/// distinguished from a re-observation.
pub(crate) struct LeaderWatch {
    last: (bool, Option<u32>),
}

impl LeaderWatch {
    /// Seed the watch with the state as it is *now*, so an edge that starts on
    /// a healthy leader does not report a transition on its first poll. (The
    /// alternative — seeding `(false, None)` — would count one phantom leader
    /// change per edge start, and, worse, push it at any client that connected
    /// in between.)
    pub fn new(send: &SendHalf) -> Self {
        LeaderWatch { last: sample(send) }
    }

    /// Sample the cnc page and report a transition, if there was one.
    ///
    /// Takes the `SendHalf` by reference because that is the type that owns
    /// the cnc mapping; the driver has its own clone (`SendHalf` is `Send` but
    /// not `Sync`), so this costs no coordination with the reader threads.
    pub fn poll(&mut self, send: &SendHalf) -> Option<(bool, Option<u32>)> {
        let (can_serve, hint) = sample(send);
        self.observe(can_serve, hint)
    }

    /// The pure core: the transition rule, with no cnc page and no cluster
    /// behind it, so it can be tested for what it is.
    pub fn observe(&mut self, can_serve: bool, hint: Option<u32>) -> Option<(bool, Option<u32>)> {
        let now = (can_serve, hint);
        if now == self.last {
            return None;
        }
        self.last = now;
        Some(now)
    }
}

/// One sample of the two cnc words.
///
/// They are separate atomics, so a sample can catch a half-applied change (the
/// hint updated, `can_serve` not yet). That is harmless and self-correcting:
/// the worst case is one extra `LEADER_CHANGED` per connection, and the very
/// next poll — microseconds later — observes the settled pair and pushes the
/// truth. Reading them under a lock would be strictly worse: it would put a
/// lock on the node's hot path to save a frame nobody is harmed by.
fn sample(send: &SendHalf) -> (bool, Option<u32>) {
    (send.can_serve(), send.leader_hint())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `LeaderWatch` seeded with an explicit state, for the pure tests: the
    /// real constructor needs a `SendHalf`, which needs an attached node.
    fn watch(last: (bool, Option<u32>)) -> LeaderWatch {
        LeaderWatch { last }
    }

    #[test]
    fn an_unchanged_sample_is_not_a_transition() {
        let mut w = watch((true, Some(3)));
        assert_eq!(w.observe(true, Some(3)), None);
        assert_eq!(w.observe(true, Some(3)), None, "still nothing, however often it is polled");
    }

    #[test]
    fn a_can_serve_flip_is_a_transition() {
        let mut w = watch((false, Some(3)));
        assert_eq!(w.observe(true, Some(3)), Some((true, Some(3))), "this node started serving");
        assert_eq!(w.observe(true, Some(3)), None, "and reports it exactly once");
        assert_eq!(w.observe(false, Some(3)), Some((false, Some(3))), "and again when it stops");
    }

    #[test]
    fn a_hint_change_is_a_transition_including_to_and_from_unknown() {
        let mut w = watch((false, Some(1)));
        // Mid-election: the node adopted a new term and cleared its hint.
        assert_eq!(w.observe(false, None), Some((false, None)));
        assert_eq!(w.observe(false, None), None);
        // The election settled on a different member.
        assert_eq!(w.observe(false, Some(2)), Some((false, Some(2))));
        // Same hint, still not serving: nothing to say.
        assert_eq!(w.observe(false, Some(2)), None);
    }

    #[test]
    fn both_words_changing_at_once_is_one_transition() {
        let mut w = watch((false, None));
        assert_eq!(w.observe(true, Some(7)), Some((true, Some(7))));
        assert_eq!(w.observe(true, Some(7)), None);
    }
}
