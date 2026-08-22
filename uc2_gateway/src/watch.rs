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
//! the pair changes **and the new hint names a member we have an address
//! for**.
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
//! ## Why an unresolvable hint says NOTHING
//!
//! A transition can land on a hint that resolves to no gateway address:
//! mid-election, when the node has adopted a new term and cleared its hint, or
//! (misconfiguration) a node id absent from the static member map. There *is*
//! a wire frame for "leader unknown" — `Leader { u32::MAX, "" }` — and pushing
//! it here would be actively harmful: `RemoteClient` reads an empty address as
//! "reconnect round-robin", so every election would make every connected
//! client on every edge drop a working connection and replay its whole
//! in-flight window at a member picked at random. That is precisely the churn
//! [`crate::edge`]'s `redirect_or_retry` refuses to cause when it answers
//! `RETRY{not_serving}` instead of inventing a target.
//!
//! So an unresolvable transition is **observed but not announced**: `last`
//! moves, so the *next* transition — the one that resolves, a few hundred
//! milliseconds later when the election settles — is what fires, and the idle
//! client gets exactly one push, naming a leader it can actually reach. The
//! unknown-leader sentinel survives in one place only, `on_instance_restart`,
//! where the edge really is telling every client "not here, not any more, go
//! and find out where".

use uc2_client::SendHalf;

/// What one poll of the watch found.
pub(crate) struct Transition<T> {
    /// The observed `(can_serve, leader_hint)` pair changed. Counted whether
    /// or not anything was announced — it is a fact about the *cluster*, not
    /// about this edge's clients.
    pub changed: bool,
    /// The leader to announce, as resolved by the caller's map. `None` means
    /// say nothing: either nothing changed, or the new hint names nowhere we
    /// can send a client (see the module doc).
    pub announce: Option<T>,
}

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

    /// Sample the cnc page and report what changed, resolving any new hint
    /// through `resolve` (the edge's static member map).
    ///
    /// Takes the `SendHalf` by reference because that is the type that owns
    /// the cnc mapping; the driver has its own clone (`SendHalf` is `Send` but
    /// not `Sync`), so this costs no coordination with the reader threads.
    pub fn poll<T>(
        &mut self,
        send: &SendHalf,
        resolve: impl FnOnce(u32) -> Option<T>,
    ) -> Transition<T> {
        let (can_serve, hint) = sample(send);
        self.observe(can_serve, hint, resolve)
    }

    /// The pure core: the transition rule and the announce rule, with no cnc
    /// page and no member map behind them, so they can be tested for what they
    /// are.
    ///
    /// Note the ordering that matters: `last` is updated on **every** change,
    /// including one that resolves to nothing. Skipping the update instead
    /// would leave the watch comparing against a stale pair, and the eventual
    /// resolvable state would then look like no change at all — the idle
    /// client would never be told.
    pub fn observe<T>(
        &mut self,
        can_serve: bool,
        hint: Option<u32>,
        resolve: impl FnOnce(u32) -> Option<T>,
    ) -> Transition<T> {
        let now = (can_serve, hint);
        if now == self.last {
            return Transition { changed: false, announce: None };
        }
        self.last = now;
        Transition { changed: true, announce: hint.and_then(resolve) }
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

    /// A stand-in for the edge's static member map: nodes 1 and 2 have gateway
    /// addresses, nothing else does.
    fn known(id: u32) -> Option<&'static str> {
        match id {
            1 => Some("host1:9100"),
            2 => Some("host2:9100"),
            _ => None,
        }
    }

    fn observe(
        w: &mut LeaderWatch,
        can_serve: bool,
        hint: Option<u32>,
    ) -> (bool, Option<&'static str>) {
        let t = w.observe(can_serve, hint, known);
        (t.changed, t.announce)
    }

    #[test]
    fn an_unchanged_sample_is_not_a_transition() {
        let mut w = watch((true, Some(1)));
        assert_eq!(observe(&mut w, true, Some(1)), (false, None));
        assert_eq!(
            observe(&mut w, true, Some(1)),
            (false, None),
            "still nothing, however often it is polled"
        );
    }

    #[test]
    fn a_can_serve_flip_is_a_transition() {
        let mut w = watch((false, Some(1)));
        assert_eq!(
            observe(&mut w, true, Some(1)),
            (true, Some("host1:9100")),
            "this node started serving"
        );
        assert_eq!(observe(&mut w, true, Some(1)), (false, None), "and reports it exactly once");
        assert_eq!(
            observe(&mut w, false, Some(1)),
            (true, Some("host1:9100")),
            "and again when it stops"
        );
    }

    #[test]
    fn a_hint_change_is_a_transition() {
        let mut w = watch((false, Some(1)));
        assert_eq!(observe(&mut w, false, Some(2)), (true, Some("host2:9100")));
        assert_eq!(observe(&mut w, false, Some(2)), (false, None));
    }

    #[test]
    fn both_words_changing_at_once_is_one_transition() {
        let mut w = watch((false, None));
        assert_eq!(observe(&mut w, true, Some(1)), (true, Some("host1:9100")));
        assert_eq!(observe(&mut w, true, Some(1)), (false, None));
    }

    /// The rule the module doc argues for: an election is TWO transitions, and
    /// only the second one — the one that names a reachable leader — puts a
    /// frame on the wire.
    #[test]
    fn an_unresolvable_hint_is_observed_but_never_announced() {
        let mut w = watch((true, Some(1)));
        // Mid-election: the node adopted a new term and cleared its hint.
        assert_eq!(
            observe(&mut w, false, None),
            (true, None),
            "a change worth counting, and nowhere to send anyone"
        );
        // A member id we have no gateway address for — a misconfigured member
        // map, and equally unusable as a redirect target.
        assert_eq!(observe(&mut w, false, Some(7)), (true, None));
        // The election settles on a member we can actually name.
        assert_eq!(
            observe(&mut w, false, Some(2)),
            (true, Some("host2:9100")),
            "the resolving transition is the one that fires"
        );
        assert_eq!(observe(&mut w, false, Some(2)), (false, None), "and only once");
    }

    /// The trap the `last`-always-moves rule avoids: if an unresolvable
    /// transition did not update `last`, coming back to the PREVIOUS leader
    /// would compare equal and announce nothing at all.
    #[test]
    fn returning_to_the_previous_leader_after_an_unresolvable_gap_still_fires() {
        let mut w = watch((false, Some(1)));
        assert_eq!(observe(&mut w, false, None), (true, None));
        assert_eq!(observe(&mut w, false, Some(1)), (true, Some("host1:9100")));
    }
}
