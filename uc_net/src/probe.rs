// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Path-MTU discovery state (jumbo spec §5.1–§5.3), shared by the sender
//! agent (sends due probes), the receiver agent (records acks, answers
//! probes) and the consensus agent (the leader's commit rule). Not a hot
//! path: a handful of updates per peer per process lifetime, so one mutex.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use uc_protocol::v2::datagram::{MTU_BOUND, RUNGS, is_rung};

/// Spec §5.1: retry every `fast_ns` for the first `fast_attempts`, then every
/// `slow_ns`, until the peer's top rung is verified.
#[derive(Debug, Clone, Copy)]
pub struct ProbeCadence {
    pub fast_ns: u64,
    pub fast_attempts: u32,
    pub slow_ns: u64,
}

impl Default for ProbeCadence {
    fn default() -> Self {
        Self {
            fast_ns: 1_000_000_000,
            fast_attempts: 5,
            slow_ns: 30_000_000_000,
        }
    }
}

/// What this node knows about ONE peer's path (spec §5.1–§5.2): the two rungs
/// that make the pair resolvable and the cadence bookkeeping that gets them
/// there. `Default` is the "fresh peer" state — nothing verified, nothing
/// advertised, due immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PeerProbe {
    /// The largest rung this node's probe to the peer was acked at; 0 = none.
    pub verified: u32,
    /// The peer's own minimum, from its latest ack (spec §5.2); 0 = unknown
    /// or the peer itself is unresolved.
    pub advertised: u32,
    /// Probe rounds [`ProbeTable::due`] has handed out for this peer since it
    /// was last reset, whether or not they were answered. Compared against
    /// [`ProbeCadence::fast_attempts`] to pick the cadence, and by
    /// [`PeerProbe::spent_fast_ladder`] to tell "not proven yet" from "tried
    /// and failed". [`ProbeTable::note_unsent_for`] gives one back.
    pub attempts: u32,
    /// LATCHED once [`ProbeTable::due`] has handed out the whole fast ladder
    /// for this peer (`attempts >= cadence.fast_attempts`). Two things clear
    /// it: [`ProbeTable::set_peers`] FORGETTING the peer — the one event that
    /// makes it new again (it comes back as `Default`) — and
    /// [`ProbeTable::note_unsent_for`] giving back the very round that earned
    /// it, because a round that put nothing on the wire is not a round that
    /// tried. A latch earned by earlier rounds that DID go out is never given
    /// back, and a retained peer keeps it across a membership change, exactly
    /// as it keeps `verified`.
    /// [`ProbeTable::narrow_peers`] reads THIS, not the live
    /// `attempts` comparison, because [`ProbeTable::on_peer_seen`] resets
    /// `attempts` to 0: a peer whose ladder was spent could be pulled back
    /// under the threshold between two 100 ms gate polls, which delayed the
    /// §5.4 refusal by another ladder's worth of seconds each time a `PROBE`
    /// arrived. A latch is a statement about what WAS tried, which is what
    /// the predicate means.
    pub spent_fast_ladder: bool,
    /// Rounds [`ProbeTable::due`] has issued since this peer last ACKED
    /// anything; `0` means it answered during (or after) the newest round.
    /// [`ProbeTable::narrow_peers`] requires this to be at most 1, which is
    /// what makes its verdict a statement about the path NOW rather than about
    /// an ack that arrived once and was then outlived by the peer itself.
    ///
    /// The regression that bought this field: under `[crypto] enabled = true`,
    /// a peer that acked the baseline rung and was then restarted (the
    /// lin_v2/crypto capstone kills the leader every second) has a stale
    /// pairwise session, so every later sealed probe to it is dropped and
    /// nothing is ever acked again. With only `verified`/`spent_fast_ladder`
    /// to go on, that reads EXACTLY like a path that carries 1408 and drops
    /// 8832 — and §5.4 fail-stopped a healthy node on loopback for it.
    pub rounds_since_ack: u32,
    /// When the next round for this peer is due, on the caller's clock. `0`
    /// means "now" — the value a fresh or reset peer carries. A REFUNDED
    /// round ([`ProbeTable::note_unsent_for`]) is rescheduled one `fast_ns`
    /// tick out instead, so a session-less or dead peer cannot turn the
    /// sender's pass into a busy loop.
    pub next_due_ns: u64,
}

/// The discovery ledger: one per node, shared by the three agents that touch
/// it (module docs). Holds a [`PeerProbe`] per configured peer plus the
/// lock-free `earliest_due_ns` fast path the sender's busy loop reads every
/// pass. Every mutator republishes that word under the `peers` lock, so the
/// only rule a caller has to keep is the one
/// [`ProbeTable::note_unsent_for`] states: at most one give-back per round.
pub struct ProbeTable {
    cadence: ProbeCadence,
    peers: Mutex<HashMap<SocketAddr, PeerProbe>>,
    /// Rounds a [`ProbeTable::due`] call scheduled that were given their
    /// attempt back by [`ProbeTable::note_unsent_for`] — i.e. rounds where
    /// nothing left the host and no rung was refused by the kernel for SIZE:
    /// every rung was skipped at assembly (no pairwise crypto session for this
    /// peer yet — the usual case), **or** every rung was assembled and its
    /// `send_to` failed for a reason other than `EMSGSIZE` (`ENOBUFS`,
    /// `EPERM`, a transient route error). ROUND-scoped, not rung-scoped: a round
    /// tries every rung above a peer's `verified` size at once, and one
    /// give-back covers however many of those rungs never left the host, so
    /// this counts "how many rounds needed the give-back", not "how many
    /// individual probes were refused".
    ///
    /// Deliberately NOT "rounds that put nothing on the wire": a round the
    /// kernel refused for size (`EMSGSIZE`) also puts nothing on the wire, and
    /// is no longer counted here, because it SPENDS its attempt rather than
    /// being refunded (see [`ProbeTable::note_unsent_for`]). That refusal has
    /// its own counter on the sender's stats.
    unsent: AtomicU64,
    /// The soonest `next_due_ns` over the UNRESOLVED peers, or `u64::MAX`
    /// when every peer is resolved (or there are none). Maintained under
    /// `peers` by every method that can change a `next_due_ns` or a peer's
    /// resolved-ness, and read WITHOUT the lock by [`ProbeTable::due`] —
    /// which the sender's busy loop calls every pass, and which must
    /// therefore cost one `Relaxed` load and nothing else on the overwhelming
    /// majority of passes. A stale-HIGH reading is bounded by cache coherence
    /// — `on_peer_seen`/`set_peers` lower this from the receiver thread and the
    /// sender's `Relaxed` load may see the older, higher value for a few
    /// coherence cycles — so it costs at most one extra pass before the probe
    /// goes out, never a lost probe. A stale-LOW reading costs one needless
    /// lock, and likewise never misses one.
    earliest_due_ns: AtomicU64,
}

impl ProbeTable {
    /// An empty table on `cadence`. Returned in an `Arc` because the three
    /// agents share one instance; seed the peer set with
    /// [`ProbeTable::set_peers`] BEFORE handing the table to either agent —
    /// [`ProbeTable::own_min_rung`] answers `MTU_BOUND` for an empty map
    /// (nothing to compare), and a transient too-high advertisement would be
    /// committed irreversibly by the monotone commit rule.
    pub fn new(cadence: ProbeCadence) -> Arc<ProbeTable> {
        Arc::new(ProbeTable {
            cadence,
            peers: Mutex::new(HashMap::new()),
            unsent: AtomicU64::new(0),
            earliest_due_ns: AtomicU64::new(u64::MAX),
        })
    }

    /// Replace the peer set: a new peer starts unresolved and due now; a peer
    /// no longer in `peers` is forgotten (spec §5.1, membership change).
    pub fn set_peers(&self, peers: &[SocketAddr]) {
        let mut g = self.peers.lock().unwrap();
        g.retain(|a, _| peers.contains(a));
        for &p in peers {
            g.entry(p).or_default();
        }
        self.publish_earliest(&g);
    }

    /// Recompute [`Self::earliest_due_ns`] from the map. Called under the
    /// lock by every mutator; `Self::resolved` is the same predicate `due`
    /// skips on, so a resolved peer never holds the fast path open.
    fn publish_earliest(&self, g: &HashMap<SocketAddr, PeerProbe>) {
        let e = g
            .values()
            .filter(|p| !Self::resolved(p))
            .map(|p| p.next_due_ns)
            .min()
            .unwrap_or(u64::MAX);
        self.earliest_due_ns.store(e, Ordering::Relaxed);
    }

    /// The soonest instant at which [`ProbeTable::due`] can return anything;
    /// `u64::MAX` once every peer is resolved. Exposed for tests and for the
    /// caller that wants to see the ladder has finished.
    pub fn earliest_due_ns(&self) -> u64 {
        self.earliest_due_ns.load(Ordering::Relaxed)
    }

    /// Nothing left for this pair to tell each other — see [`ProbeTable::due`]
    /// for why BOTH halves are required.
    fn resolved(p: &PeerProbe) -> bool {
        p.verified as usize >= MTU_BOUND && p.advertised >= p.verified
    }

    /// The configured peer set, in no particular order — a snapshot, so a
    /// membership change can land before the caller reads it.
    pub fn peers(&self) -> Vec<SocketAddr> {
        self.peers.lock().unwrap().keys().copied().collect()
    }

    /// This node's ledger entry for one peer, or `None` if that address is not
    /// in the current member set. A copy, not a handle: read it once per
    /// decision rather than re-reading fields expecting them to agree.
    pub fn get(&self, peer: SocketAddr) -> Option<PeerProbe> {
        self.peers.lock().unwrap().get(&peer).copied()
    }

    /// The peers whose next probe is due at `now_ns`, each with the rungs
    /// still above its `verified`. Bumps `attempts` and schedules the next
    /// due time, so a caller that sends what it is handed needs nothing else.
    ///
    /// A peer is DONE — and drops out of every later pass — only once BOTH
    /// halves of the pair have nothing left to tell each other: our probes
    /// reached its top rung, and the minimum it advertises back is no lower
    /// than what we verified. Both halves are needed, because `advertised` is
    /// only ever refreshed by an ack and an ack only ever comes back for a
    /// probe WE send. Stopping on `verified` alone latches whatever the peer
    /// happened to know at that instant, forever:
    ///
    /// * A peer that answered our first pass necessarily answered it before it
    ///   had verified anything itself, so its `own_min_rung` on that ack is 0.
    /// * A peer answering mid-ladder advertises a rung it has since climbed
    ///   past — a STALE-LOW value, and the one the loopback test caught under
    ///   load (`uc_net/tests/probe.rs`).
    ///
    /// Either way `table_min` (spec §5.3, the leader's commit rule) takes
    /// `min(verified, advertised)` and refuses a zero, so a latched
    /// advertisement means the cluster never raises its MTU at all.
    ///
    /// So whenever the advertisement is behind what we verified, the pass
    /// APPENDS the verified rung to the rungs it was going to try anyway —
    /// not only when the ladder has nothing left above it. On a CAPPED path
    /// that distinction is the whole feature: every rung above `verified` is
    /// exactly a size that path drops, so without the appended rung no ack can
    /// ever come back and `advertised` never refreshes. The refresh rung goes
    /// LAST, so the freshest advertisement is the one that lands last.
    ///
    /// Cost, per slow tick, while a peer's advertisement stays behind: one
    /// extra datagram of a size the path is already known to carry — three
    /// instead of two on a 1408-capped path, two instead of one on an
    /// 8832-capped one. A GENUINELY asymmetric peer — one whose own minimum is
    /// legitimately below our path to it, because it has a narrow peer of its
    /// own — pays that for as long as that stays true. It is also what
    /// re-discovers a path that later improves.
    pub fn due(&self, now_ns: u64) -> Vec<(SocketAddr, Vec<u32>)> {
        // The sender polls this every pass and the answer is almost always
        // "nothing": one Relaxed load, no mutex, no allocation. `Vec::new`
        // does not allocate.
        if now_ns < self.earliest_due_ns.load(Ordering::Relaxed) {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut g = self.peers.lock().unwrap();
        for (&addr, p) in g.iter_mut() {
            if Self::resolved(p) || now_ns < p.next_due_ns {
                continue;
            }
            let mut rungs: Vec<u32> = RUNGS.iter().copied().filter(|&r| r > p.verified).collect();
            if p.verified > 0 {
                // The REFRESH rung, sent on EVERY round of an unresolved peer
                // (not only when its advertisement is behind): it is the only
                // rung in the round a capped path can deliver, so it is both
                // how `advertised` refreshes and — since the crypto-restart
                // regression — the LIVENESS evidence `narrow_peers` requires.
                // Without it, "the jumbo rungs went unanswered" cannot be told
                // apart from "this peer answers nothing at all", and a peer
                // that acked the baseline once and then restarted (a fresh
                // pairwise session; every sealed probe dropped) read as a
                // proven-narrow path and fail-stopped a healthy node.
                rungs.push(p.verified);
            }
            p.attempts += 1;
            p.rounds_since_ack = p.rounds_since_ack.saturating_add(1);
            // The LATCH (review minor 6): "this peer's fast ladder has been
            // handed out" is a fact about the past, so it is recorded here.
            // `note_unsent_for` clears it again for a round that put NOTHING on
            // the wire — a round that was never tried is not a round that
            // failed — and `set_peers` clears it by forgetting the peer.
            p.spent_fast_ladder |= p.attempts >= self.cadence.fast_attempts;
            let step = if p.attempts < self.cadence.fast_attempts {
                self.cadence.fast_ns
            } else {
                self.cadence.slow_ns
            };
            p.next_due_ns = now_ns + step;
            out.push((addr, rungs));
        }
        self.publish_earliest(&g);
        out
    }

    /// An ack from `from`: `rung` was carried whole (only a ladder rung
    /// counts — a forged or garbled value is ignored), and the peer's own
    /// minimum is `own_min_rung`. An ack from an unknown address is ignored.
    pub fn on_ack(&self, from: SocketAddr, rung: u32, own_min_rung: u32) {
        if !is_rung(rung) {
            return;
        }
        let mut g = self.peers.lock().unwrap();
        if let Some(p) = g.get_mut(&from) {
            p.verified = p.verified.max(rung);
            p.advertised = own_min_rung;
            // This peer is answering NOW, which is what `narrow_peers` needs
            // before it calls a path narrow (see `rounds_since_ack`).
            p.rounds_since_ack = 0;
            // An ack can RESOLVE this peer, which takes its deadline out of
            // the fast path's minimum (and, when it was the last unresolved
            // peer, closes the door for good).
            self.publish_earliest(&g);
        }
    }

    /// Task 8b: a PROBE received from `from` is proof the peer is alive. If we
    /// had backed off on it (slow cadence, unresolved), restart its fast ladder
    /// now so a rejoining member's raise lands in seconds. A peer still on the
    /// fast cadence, a resolved peer, or an unknown address is a no-op — two
    /// nodes booting together must not keep resetting each other.
    pub fn on_peer_seen(&self, from: SocketAddr) {
        let mut g = self.peers.lock().unwrap();
        let Some(p) = g.get_mut(&from) else {
            return;
        };
        if Self::resolved(p) || p.attempts < self.cadence.fast_attempts {
            return;
        }
        p.attempts = 0;
        p.next_due_ns = 0;
        self.publish_earliest(&g);
    }

    /// Spec §5.2: min over peers of `verified`; 0 while any peer is
    /// unresolved; `MTU_BOUND` for a node with no peers (a solo cluster's
    /// path is loopback).
    pub fn own_min_rung(&self) -> u32 {
        let g = self.peers.lock().unwrap();
        if g.is_empty() {
            return MTU_BOUND as u32;
        }
        g.values().map(|p| p.verified).min().unwrap_or(0)
    }

    /// Jumbo spec §5.4: the peers whose path is PROVEN narrower than
    /// `committed` — `0 < verified < committed` AND the peer is past its fast
    /// ladder ([`PeerProbe::spent_fast_ladder`], a latch rather than a live
    /// `attempts` comparison: see that field for why). Sorted by address, so
    /// the answer is stable across calls and across nodes.
    ///
    /// Both halves are load-bearing, and the second one is the whole reason
    /// this lives on the table rather than in the node:
    ///
    /// - `verified == 0` is SILENCE — a member that is down, slow, or still
    ///   replaying a cold start. Nothing is known about its path, so it is
    ///   never narrow at any attempt count.
    /// - `verified` BELOW `committed` is the ordinary MID-LADDER state of a
    ///   perfectly healthy peer: [`RUNGS`] starts at `MTU_DEFAULT`, a round
    ///   puts one datagram per rung above `verified` on the wire, and
    ///   [`ProbeTable::on_ack`] raises `verified` as each ack arrives — so
    ///   `verified == 1408` with the two jumbo rungs still in flight is what
    ///   discovery looks like while it is working, and one lost probe or ack
    ///   holds that state for a full tick. A caller that treated it as
    ///   degradation would fail-stop healthy nodes; the fast ladder having run
    ///   out is what turns "not proven yet" into "tried and failed".
    ///
    /// The cadence is private to this type, which is why the predicate is here
    /// and not at the call site — and why the spent-ladder half is a LATCH:
    /// `on_peer_seen` puts `attempts` back to 0 on a peer that is alive but
    /// still narrow, so a live comparison could be cleared between two of the
    /// node's 100 ms gate polls and push the §5.4 refusal out by another
    /// ladder every time a `PROBE` arrived.
    ///
    /// A THIRD half, bought by the crypto regression this predicate caused:
    /// the evidence must be CURRENT ([`PeerProbe::rounds_since_ack`] ≤ 1 —
    /// the peer acked the refresh rung in the newest round or the one before
    /// it). A peer that acked once and then stopped answering altogether — a
    /// killed member, a restarted one whose pairwise session is stale, a path
    /// that went away — is not a narrow path, it is a silent one, and silence
    /// never refuses. Every round of an unresolved peer carries the refresh
    /// rung precisely so a genuinely narrow path KEEPS answering and stays
    /// reportable (see [`ProbeTable::due`]).
    pub fn narrow_peers(&self, committed: u32) -> Vec<(SocketAddr, u32)> {
        let g = self.peers.lock().unwrap();
        let mut out: Vec<(SocketAddr, u32)> = g
            .iter()
            .filter(|(_, p)| {
                p.verified > 0
                    && p.verified < committed
                    && p.spent_fast_ladder
                    && p.rounds_since_ack <= 1
            })
            .map(|(&addr, p)| (addr, p.verified))
            .collect();
        out.sort_unstable();
        out
    }

    /// Is any peer ANSWERING below `committed` — `0 < verified < committed`
    /// AND still answering ([`PeerProbe::rounds_since_ack`] ≤ 1, the same
    /// freshness test [`ProbeTable::narrow_peers`] applies)? The join gate's
    /// "is there anything to wait for" test (jumbo spec §5.4).
    ///
    /// `false` means every peer short of the rung is SILENT — it never
    /// answered, or it answered once and has since stopped — and silence is no
    /// evidence: there is nothing a longer hold can turn into a verdict, so the
    /// gate passes rather than holding `can_serve` down. That matters for
    /// availability, not tidiness — holding on silence meant that on a jumbo
    /// cluster with one member down, a restarted survivor could not serve at
    /// all until the dead member returned, and the lin_v2 capstones (which kill
    /// the leader every second and then wait for a serving survivor) had no
    /// servable node for the whole window.
    ///
    /// The freshness half is not symmetry for its own sake (review round 3,
    /// Important): WITHOUT it, a peer that acked 1408 and then went stale — the
    /// crypto-restart state this whole rule exists for — keeps `verified = 1408`
    /// forever, so the gate held `can_serve` false for the full
    /// `JUMBO_GATE_WINDOW` and reinstated that same 30 s hold on a different
    /// race ordering (a node that learns the committed rung AFTER the acks
    /// landed, which is exactly the `verified: 1408, advertised: 8960` ledger
    /// the failing run showed).
    ///
    /// `true` is the genuine MID-LADDER state — a peer answered the baseline
    /// and its jumbo rungs are still in flight — which is worth holding for,
    /// briefly, because it resolves one way or the other within a ladder. Such
    /// a peer answers the refresh rung EVERY round (see [`ProbeTable::due`]),
    /// so the freshness test never shortens a legitimate hold.
    pub fn answered_below(&self, committed: u32) -> bool {
        let g = self.peers.lock().unwrap();
        g.values()
            .any(|p| p.verified > 0 && p.verified < committed && p.rounds_since_ack <= 1)
    }

    /// Spec §5.3 (erratum 4): the leader's table minimum over `members` —
    /// `Some` only when every member has an entry whose `verified` and
    /// `advertised` are both non-zero (every pair answered).
    ///
    /// An EMPTY `members` is `None`: no evidence, not universal evidence. A
    /// solo cluster has measured nothing, so it stays at the baseline door
    /// until a peer joins and is probed. The alternative — answering
    /// `MTU_BOUND` because the only path is loopback — would have a one-node
    /// cluster commit the top rung on zero measurements, and the FSM's rung is
    /// MONOTONE: the grow-from-one path (start one node, `add-learner`,
    /// promote) would then wedge on a standard-MTU network, the joiner's
    /// snapshot chunks cut at the leader's 8960 B budget and never arriving,
    /// with a wipe as the only remedy.
    pub fn table_min(&self, members: &[SocketAddr]) -> Option<u32> {
        if members.is_empty() {
            return None;
        }
        let g = self.peers.lock().unwrap();
        let mut min = MTU_BOUND as u32;
        for m in members {
            let p = g.get(m)?;
            if p.verified == 0 || p.advertised == 0 {
                return None;
            }
            min = min.min(p.verified).min(p.advertised);
        }
        Some(min)
    }

    /// A round for `peer` whose rungs the KERNEL never saw: no pairwise
    /// session yet, so every rung was skipped at assembly. Counts the miss AND
    /// gives the peer its attempt back, so the ladder is not consumed by the
    /// crypto handshake's own latency. Call this AT MOST ONCE per round, never
    /// once per rung — see the caller, `Sender::send_due_probes`, for why a
    /// per-rung call would over-decrement.
    ///
    /// A rung the kernel REFUSED for size (`EMSGSIZE`, DF set) is NOT this
    /// case and must not be refunded (review fix 3): it is a proven fact about
    /// this host's own interface MTU, as conclusive as a remote drop. Refunding
    /// it held such a peer below `fast_attempts` forever, so
    /// [`ProbeTable::narrow_peers`] never reported it and the jumbo join gate
    /// pended indefinitely instead of refusing by name.
    ///
    /// Why a decrement and not "don't count until sent": `due()` bumps
    /// `attempts` and schedules the next deadline before the caller knows
    /// whether the send succeeds, and moving that bookkeeping after the send
    /// would put the mutex back on the send path — the one thing plan 1's
    /// fast path removed.
    ///
    /// `now_ns` is the SENDER PASS's clock reading (the same one it handed
    /// `due`), and the peer stays SCHEDULED on it: the refunded round comes
    /// back one `fast_ns` tick later, never immediately. An earlier revision
    /// set `next_due_ns = 0` here, which republished `earliest_due_ns = 0` and
    /// so handed the peer straight back to the very next sender pass: with
    /// `[crypto] enabled = true` that made EVERY pass of the handshake (and
    /// every pass for as long as a peer stayed down) take the table mutex
    /// twice, allocate two `Vec`s, walk the peer map and attempt three
    /// `assemble_probe`s, while `unsent()` climbed by millions per second and
    /// stopped meaning anything. Refund the attempt, keep the cadence.
    pub fn note_unsent_for(&self, peer: SocketAddr, now_ns: u64) {
        self.unsent.fetch_add(1, Ordering::Relaxed);
        let mut g = self.peers.lock().unwrap();
        if let Some(p) = g.get_mut(&peer) {
            // A round that put NOTHING on the wire is not a round that tried,
            // so it gives back the ladder LATCH as well as the attempt: the
            // latch must mean "the higher rungs actually went out and went
            // unacked". Only when THIS round is the one that earned it, though
            // (review round 3, minor 1): a plain assignment would also clear a
            // latch earned by earlier real rounds, because `on_peer_seen` puts
            // `attempts` back to 0 on a live-but-still-narrow peer — which is
            // the very regression the latch was introduced to prevent.
            // `attempts` equal to the threshold means this round is the one that
            // crossed it, so the ladder has at most `fast_attempts - 1` rounds
            // that actually went out.
            if p.attempts == self.cadence.fast_attempts {
                p.spent_fast_ladder = false;
            }
            p.attempts = p.attempts.saturating_sub(1);
            // `due()` has already counted this round in `rounds_since_ack`,
            // which is deliberate — a refunded round is still a round in which
            // the peer did not answer.
            p.next_due_ns = now_ns + self.cadence.fast_ns;
        }
        self.publish_earliest(&g);
    }

    /// Rounds that were given their attempt back — skipped at assembly, or
    /// assembled and failed for a reason other than `EMSGSIZE`. See the
    /// `unsent` field's doc: ROUND-scoped, not rung-scoped, and a round the
    /// kernel refused for SIZE is NOT one of these.
    pub fn unsent(&self) -> u64 {
        self.unsent.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn fast() -> ProbeCadence {
        ProbeCadence {
            fast_ns: 10,
            fast_attempts: 2,
            slow_ns: 100,
        }
    }

    /// Jumbo spec §5.4: what counts as a PROVEN narrow path. The mid-ladder
    /// case (second assert) is the one that matters — treating it as
    /// degradation fail-stopped a healthy restarting node, because
    /// `verified == RUNGS[0]` with the jumbo rungs still in flight is what
    /// normal discovery looks like.
    #[test]
    fn narrow_peers_needs_an_answer_and_a_spent_fast_ladder() {
        let t = ProbeTable::new(fast()); // fast_attempts = 2
        let (p1, p2) = (a(7001), a(7002));
        t.set_peers(&[p1, p2]);

        // Nothing answered yet: silence is never narrow.
        assert!(t.narrow_peers(8960).is_empty());
        t.due(0); // attempts = 1
        t.due(1_000); // attempts = 2 — ladder spent, still silent
        assert!(
            t.narrow_peers(8960).is_empty(),
            "a peer that answered NOTHING is never narrow, at any attempt count"
        );

        // p1 answers the baseline rung, mid-ladder.
        let t = ProbeTable::new(fast());
        t.set_peers(&[p1, p2]);
        t.due(0); // attempts = 1, inside the fast ladder
        t.on_ack(p1, 1408, 1408);
        assert!(
            t.narrow_peers(8960).is_empty(),
            "verified = 1408 with attempts = 1 is MID-DISCOVERY, not degraded"
        );

        // One more round spends the fast ladder: now it is a proven fact.
        t.due(1_000); // attempts = 2 == fast_attempts
        assert_eq!(
            t.narrow_peers(8960),
            vec![(p1, 1408)],
            "past the fast ladder, an answer below the rung is degradation"
        );

        // A peer at or above `committed` is never narrow.
        t.on_ack(p1, 8960, 8960);
        assert!(t.narrow_peers(8960).is_empty(), "8960 >= committed");
        t.on_ack(p2, 8960, 8960);
        assert!(
            t.narrow_peers(8832).is_empty(),
            "above committed either way"
        );
    }

    /// The CRITICAL regression the fix wave shipped and this closes: a peer
    /// that answered the baseline rung once and then stopped answering
    /// ALTOGETHER is silent, not narrow. Under `[crypto] enabled = true` a
    /// restarted member is exactly that — a stale pairwise session means every
    /// later sealed probe is dropped — and `narrow_peers` reported it, so §5.4
    /// fail-stopped a healthy node on loopback
    /// (`lin_v2 linearizable_under_failover_with_crypto`).
    ///
    /// The discriminator is `rounds_since_ack`: a genuinely narrow path keeps
    /// acking the refresh rung every round, a gone one does not.
    #[test]
    fn a_peer_that_stopped_answering_is_silent_not_narrow() {
        let t = ProbeTable::new(fast()); // fast_attempts = 2
        t.set_peers(&[a(1)]);
        t.due(0);
        t.on_ack(a(1), 1408, 8960); // the only ack this peer will ever send
        t.due(10); // ladder spent, and the peer answered in the round before
        assert_eq!(
            t.narrow_peers(8960),
            vec![(a(1), 1408)],
            "fresh evidence: it is answering at 1408 while 8832/8960 go unacked"
        );

        // It goes away (killed, restarted under a new session, path gone), so
        // the next round goes unanswered too and the verdict must lapse — the
        // evidence window is "acked in the newest round or the one before it".
        // (The ladder is spent, so rounds are on the SLOW cadence now.)
        t.due(110);
        assert!(
            t.narrow_peers(8960).is_empty(),
            "two rounds with no answer: silence, which never refuses"
        );
        assert!(
            !t.answered_below(8960),
            "and the join gate treats it as silent too (review round 3, \
             Important): an ack this peer has outlived must not hold \
             `can_serve` down for the window either"
        );

        // And it comes back: the refresh rung is acked again, so the path's
        // narrowness is a current fact once more.
        t.on_ack(a(1), 1408, 8960);
        assert_eq!(t.narrow_peers(8960), vec![(a(1), 1408)]);
    }

    /// The coordinator's required test, with the mechanism the diagnosis
    /// actually needed: rounds that put NOTHING on the wire (no pairwise
    /// session yet — every rung skipped at assembly) never make a peer narrow,
    /// however many of them pass, because they give back the ladder latch as
    /// well as the attempt. Rounds that DO go out, against a peer that keeps
    /// answering the refresh rung, do.
    #[test]
    fn refunded_rounds_never_make_a_peer_narrow() {
        let t = ProbeTable::new(fast()); // fast_attempts = 2, fast_ns = 10
        t.set_peers(&[a(1)]);
        // One round landed before the session went away, so the peer HAS
        // answered the baseline: `verified > 0`, the first half of the
        // predicate. Everything after this is a session-less refund.
        t.due(0);
        t.on_ack(a(1), 1408, 8960);
        let mut now = 10;
        for round in 0..8 {
            assert_eq!(t.due(now).len(), 1, "round {round} is due");
            t.note_unsent_for(a(1), now);
            assert!(
                !t.get(a(1)).unwrap().spent_fast_ladder,
                "round {round}: a round that put nothing on the wire is not a \
                 round that tried, so the latch is given back with the attempt"
            );
            assert!(
                t.narrow_peers(8960).is_empty(),
                "round {round}: never narrow on rounds that never went out"
            );
            now += fast().fast_ns;
        }

        // The handshake completes: rounds go out for real and the peer keeps
        // answering only the baseline. NOW it is a proven narrow path.
        for _ in 0..fast().fast_attempts {
            t.due(now);
            t.on_ack(a(1), 1408, 8960);
            now += fast().fast_ns;
        }
        assert!(t.get(a(1)).unwrap().spent_fast_ladder);
        assert_eq!(t.narrow_peers(8960), vec![(a(1), 1408)]);
    }

    /// `answered_below` is the join gate's "is there anything to wait for"
    /// test: silence is not, a mid-ladder answer is.
    #[test]
    fn answered_below_separates_silence_from_a_mid_ladder_answer() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1), a(2)]);
        assert!(!t.answered_below(8960), "both silent: nothing to wait for");
        t.due(0);
        t.due(10);
        assert!(
            !t.answered_below(8960),
            "a spent ladder does not turn silence into evidence"
        );
        t.on_ack(a(1), 1408, 1408);
        assert!(t.answered_below(8960), "a(1) answered below the rung");
        t.on_ack(a(1), 8960, 8960);
        assert!(
            !t.answered_below(8960),
            "at the rung, and a(2) has still said nothing"
        );
    }

    /// Sorted by address, so "the first offender" is the same peer on every
    /// call and on every node.
    #[test]
    fn narrow_peers_is_sorted_by_address() {
        let t = ProbeTable::new(fast());
        let (hi, lo) = (a(7100), a(7010));
        t.set_peers(&[hi, lo]);
        t.due(0);
        t.due(1_000);
        t.on_ack(hi, 1408, 1408);
        t.on_ack(lo, 8832, 8832);
        assert_eq!(t.narrow_peers(8960), vec![(lo, 8832), (hi, 1408)]);
    }

    /// Errata-adjacent (final review, plan 1): a probe that never left the host
    /// — no pairwise session yet — must not spend one of the five fast
    /// attempts, or a peer whose handshake takes longer than the fast window
    /// is on the 30 s cadence before its first probe ever goes out.
    #[test]
    fn an_unsent_probe_does_not_spend_a_fast_attempt() {
        let t = ProbeTable::new(fast()); // fast_attempts: 2, fast_ns: 10
        t.set_peers(&[a(1)]);
        let mut now = 0;
        for round in 0..4 {
            let due = t.due(now);
            assert_eq!(due.len(), 1, "still due: nothing was ever sent");
            t.note_unsent_for(a(1), now);
            // The attempt is REFUNDED but the peer stays SCHEDULED: the round
            // comes back one fast tick later, not on the very next sender pass.
            // Setting `next_due_ns = 0` here turned a session-less or dead peer
            // into a per-pass busy loop — two mutex acquisitions, two `Vec`s
            // and three `assemble_probe`s every pass, forever.
            assert!(
                t.due(now).is_empty(),
                "round {round}: refunded, not re-due at the same instant"
            );
            assert!(
                t.due(now + fast().fast_ns - 1).is_empty(),
                "round {round}: nor anywhere inside the fast tick"
            );
            assert_eq!(
                t.earliest_due_ns(),
                now + fast().fast_ns,
                "round {round}: the published fast path agrees"
            );
            now += fast().fast_ns;
        }
        assert_eq!(t.get(a(1)).unwrap().attempts, 0);
        assert_eq!(t.unsent(), 4);
        // Once a probe DOES go out, the cadence advances as before.
        t.due(now);
        assert_eq!(t.get(a(1)).unwrap().attempts, 1);
    }

    /// Review minor 6: the spent-ladder half of `narrow_peers` is a LATCH, so
    /// an `on_peer_seen` reset (a `PROBE` from a live but still-narrow peer)
    /// cannot take a proven-narrow peer back out of the refusing set. Before
    /// the latch this cleared the predicate for another whole ladder each time
    /// a probe arrived, delaying §5.4's refusal by ~5 s a round.
    #[test]
    fn a_spent_fast_ladder_latches_through_an_on_peer_seen_reset() {
        let t = ProbeTable::new(fast()); // fast_attempts = 2
        t.set_peers(&[a(1)]);
        t.due(0);
        t.on_ack(a(1), 1408, 1408);
        t.due(10); // attempts = 2: the ladder is spent
        assert_eq!(t.narrow_peers(8960), vec![(a(1), 1408)]);

        t.on_peer_seen(a(1)); // alive — back to the fast cadence
        assert_eq!(t.get(a(1)).unwrap().attempts, 0, "the cadence did reset");
        assert!(
            t.get(a(1)).unwrap().spent_fast_ladder,
            "but the fact that the ladder was tried is latched"
        );
        assert_eq!(
            t.narrow_peers(8960),
            vec![(a(1), 1408)],
            "so the peer stays proven-narrow"
        );

        // Review round 3, minor 1: and a REFUNDED round after that reset does
        // not clear it either. `note_unsent_for` gives back only the round that
        // EARNED the latch; with `attempts` back at 0 a plain assignment would
        // wipe a latch earned by rounds that really went out — which is the
        // on_peer_seen regression again, by another route.
        let now = 20;
        assert_eq!(t.due(now).len(), 1, "the reset made it due");
        t.note_unsent_for(a(1), now);
        assert!(
            t.get(a(1)).unwrap().spent_fast_ladder,
            "a latch earned by rounds that went out survives a later refund"
        );

        // Forgetting the peer is the one thing that makes it new again.
        t.set_peers(&[]);
        t.set_peers(&[a(1)]);
        assert_eq!(t.get(a(1)).unwrap(), PeerProbe::default());
        assert!(t.narrow_peers(8960).is_empty());
    }

    #[test]
    fn a_new_peer_is_due_now_with_every_rung_then_follows_the_cadence() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1)]);
        let d = t.due(0);
        assert_eq!(d, vec![(a(1), vec![1408, 8832, 8960])]);
        assert!(t.due(5).is_empty(), "not due again before fast_ns");
        assert_eq!(t.due(10).len(), 1); // attempt 2 (still fast)
        assert!(t.due(25).is_empty(), "attempt 3 is on the slow cadence");
        assert_eq!(t.due(120).len(), 1);
    }

    #[test]
    fn an_ack_raises_verified_and_narrows_the_next_probe() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1)]);
        t.due(0);
        // A LEVEL advertisement (`own_min_rung` == the rung it acked), so this
        // test stays about narrowing alone — the refresh rung a peer whose
        // advertisement is behind also gets has its own test below.
        t.on_ack(a(1), 8832, 8832);
        assert_eq!(t.get(a(1)).unwrap().verified, 8832);
        // The untried rung above, then the REFRESH rung — which every round of
        // an unresolved peer now carries, level advertisement or not: it is the
        // liveness evidence `narrow_peers` requires (see `due`).
        assert_eq!(t.due(10), vec![(a(1), vec![8960, 8832])]);
        // Fully resolved: top rung verified AND the peer's own minimum is no
        // lower (Task 4 sharpened the rule — see `due`; the value here was
        // 8832, an advertisement the pair would still be reconciling).
        t.on_ack(a(1), 8960, 8960);
        assert!(t.due(1_000).is_empty(), "resolved: probing stops");
        // A lower late ack never lowers; a non-rung is ignored.
        t.on_ack(a(1), 1408, 8832);
        t.on_ack(a(1), 1500, 8832);
        assert_eq!(t.get(a(1)).unwrap().verified, 8960);
        // An ack from a stranger is ignored.
        t.on_ack(a(9), 8960, 8960);
        assert!(t.get(a(9)).is_none());
    }

    /// Task 4 (found by `uc_net/tests/probe.rs`): a peer whose advertisement is
    /// BEHIND what we verified must keep being probed at the verified rung.
    /// Both latches are real — a fresh cluster's first exchange necessarily
    /// carries `own_min_rung() == 0`, and a mid-ladder answer carries a rung
    /// the peer has since climbed past — and either one leaves `table_min`
    /// (spec §5.3) permanently low or `None`.
    #[test]
    fn a_peer_whose_advertisement_is_behind_is_still_probed_at_its_verified_rung() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1)]);
        t.due(0);
        t.on_ack(a(1), MTU_BOUND as u32, 0); // the peer had verified nothing yet
        assert_eq!(
            t.due(1_000),
            vec![(a(1), vec![MTU_BOUND as u32])],
            "top rung verified, advertisement unknown: re-ask at the verified size"
        );
        t.on_ack(a(1), MTU_BOUND as u32, 8832); // mid-ladder: stale-low
        assert_eq!(
            t.due(2_000),
            vec![(a(1), vec![MTU_BOUND as u32])],
            "advertisement still below what we verified: keep asking"
        );
        t.on_ack(a(1), MTU_BOUND as u32, MTU_BOUND as u32);
        assert!(t.due(3_000).is_empty(), "resolved: probing stops");
    }

    /// The same rule on a CAPPED path — the case the whole feature exists for,
    /// and the one where it actually bites: every rung ABOVE `verified` is a
    /// size the path drops, so the appended verified rung is the only datagram
    /// in the pass that can draw an ack back at all. Without it `advertised`
    /// stays 0 forever and this peer's `table_min` is permanently `None`.
    #[test]
    fn a_capped_path_is_re_probed_at_its_verified_rung_too_not_only_at_the_top() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1)]);
        t.due(0);
        t.on_ack(a(1), 8832, 0); // 8960 was dropped by the path; nobody has acked us yet
        assert_eq!(
            t.due(1_000),
            vec![(a(1), vec![8960, 8832])],
            "the untried rung above, THEN the verified rung — freshest ack last"
        );
        t.on_ack(a(1), 8832, 8832); // the peer's view caught up
        assert_eq!(
            t.due(2_000),
            vec![(a(1), vec![8960, 8832])],
            "advertisement level, but the refresh rung still rides every round: \
             it is the only datagram a capped path can answer, and that answer \
             is what keeps this peer reportable as narrow"
        );
        assert_eq!(t.table_min(&[a(1)]), Some(8832));
    }

    /// The sender's busy loop calls `due` every pass, so an idle pass must
    /// not take the peer mutex at all. Proved by HOLDING that mutex: a `due`
    /// that locked would block until the guard drops, and the receive below
    /// would time out instead of returning.
    #[test]
    fn due_before_the_earliest_deadline_takes_no_lock() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1), a(2)]);
        assert_eq!(t.earliest_due_ns(), 0, "a new peer is due now");
        assert_eq!(t.due(0).len(), 2);
        assert_eq!(t.earliest_due_ns(), 10, "both rescheduled at fast_ns");

        let guard = t.peers.lock().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let t2 = Arc::clone(&t);
        std::thread::spawn(move || {
            let _ = tx.send(t2.due(5));
        });
        let got = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("due(now < earliest) must not take the peer lock");
        assert!(got.is_empty());
        drop(guard);

        // Resolving every peer closes the door: no lock, ever again.
        t.on_ack(a(1), MTU_BOUND as u32, MTU_BOUND as u32);
        assert_eq!(t.earliest_due_ns(), 10, "a(2) is still unresolved");
        t.on_ack(a(2), MTU_BOUND as u32, MTU_BOUND as u32);
        assert_eq!(t.earliest_due_ns(), u64::MAX);
        assert!(t.due(u64::MAX - 1).is_empty());
    }

    #[test]
    fn own_min_is_zero_while_any_peer_is_unresolved() {
        let t = ProbeTable::new(fast());
        assert_eq!(t.own_min_rung(), MTU_BOUND as u32, "no peers: loopback");
        t.set_peers(&[a(1), a(2)]);
        assert_eq!(t.own_min_rung(), 0);
        t.on_ack(a(1), 8960, 0);
        assert_eq!(t.own_min_rung(), 0);
        t.on_ack(a(2), 8832, 0);
        assert_eq!(t.own_min_rung(), 8832);
    }

    #[test]
    fn table_min_needs_every_member_verified_and_advertising() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1), a(2)]);
        let members = [a(1), a(2)];
        assert_eq!(t.table_min(&members), None);
        t.on_ack(a(1), 8960, 8960);
        assert_eq!(t.table_min(&members), None, "a(2) silent");
        t.on_ack(a(2), 8960, 0);
        assert_eq!(t.table_min(&members), None, "a(2) itself unresolved");
        t.on_ack(a(2), 8960, 8832);
        assert_eq!(
            t.table_min(&members),
            Some(8832),
            "a(2)'s own path is the min"
        );
        // A member not in the table (never set as a peer) blocks the rule.
        assert_eq!(t.table_min(&[a(1), a(2), a(3)]), None);
        // An EMPTY member set is NO evidence, not universal evidence: a solo
        // cluster must stay at the baseline door until a peer joins and is
        // probed. `Some(MTU_BOUND)` here would let a one-node cluster commit
        // the top rung on zero measurements — irreversibly, the FSM's rung
        // being monotone — and the grow-from-one path (start one node,
        // `add-learner`, promote) would then wedge on a standard-MTU network:
        // the joiner's snapshot chunks are cut at the leader's 8960 B budget
        // and never arrive, with a wipe as the only remedy.
        assert_eq!(t.table_min(&[]), None);
    }

    #[test]
    fn set_peers_forgets_removed_and_keeps_known() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1), a(2)]);
        t.on_ack(a(1), 8960, 8960);
        t.set_peers(&[a(1), a(3)]);
        assert_eq!(t.get(a(1)).unwrap().verified, 8960);
        assert!(t.get(a(2)).is_none());
        assert_eq!(t.get(a(3)).unwrap(), PeerProbe::default());
    }

    /// Task 8b: a PROBE from a peer we had backed off on is proof of life —
    /// pull it back to the fast cadence so a rejoining member's raise lands
    /// in seconds, not at the next 30 s tick.
    #[test]
    fn a_probe_from_a_backed_off_peer_resets_its_cadence() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1)]);
        t.due(0); // attempt 1 (fast)
        t.due(10); // attempt 2 → now on the slow cadence, next due at 110
        assert!(t.due(50).is_empty(), "slow cadence: not due yet");
        t.on_peer_seen(a(1));
        assert_eq!(t.get(a(1)).unwrap().attempts, 0);
        assert_eq!(t.due(50).len(), 1, "reset: due now");
        // A peer still on the fast cadence is left alone.
        let t2 = ProbeTable::new(fast());
        t2.set_peers(&[a(2)]);
        t2.due(0);
        let before = t2.get(a(2)).unwrap();
        t2.on_peer_seen(a(2));
        assert_eq!(t2.get(a(2)).unwrap(), before, "fast cadence: no reset");
        // A resolved peer and a stranger are no-ops.
        t2.on_ack(a(2), 8960, 8960);
        t2.on_peer_seen(a(2));
        assert!(t2.due(1_000).is_empty(), "resolved stays resolved");
        t2.on_peer_seen(a(9));
        assert!(t2.get(a(9)).is_none());
    }
}
