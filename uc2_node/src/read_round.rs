// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Rung A (batch-probe coalescing): the probe-round state machine and the
//! ordering-rule certification predicate. Pure — no I/O, no clock reads; the
//! node wires it to the wire and the duty cycle.
//!
//! Spec: `docs/superpowers/specs/2026-07-26-uc2-rung-a-batch-probe-design.md`.
//! Plain-language account: `docs/notes/uc2-read-barrier-explained.md`.

use uc2_consensus::election::NodeId;

/// Retransmit interval for the in-flight round. Batching concentrates loss —
/// today a lost probe datagram delays ONE read until its 1 s deadline; under
/// batching it would stall every waiting read — so the round re-probes on a
/// short interval. 2 ms is ~13x the fleet-measured single-read barrier p50
/// (0.163 ms, docs/benchmarks/uc2-read-profile-2026-07-26.md), comfortably
/// clear of spurious fires, while recovering ~500x faster than the deadline.
/// Retransmits reuse `seq` AND `nonce`, so they can never widen the
/// certification set; acks are idempotent and followers answer statelessly.
pub(crate) const PROBE_RETRANSMIT_NS: u64 = 2_000_000;

/// The single in-flight READ_PROBE round (Rung A, spec §4-§5). At most one
/// exists at a time; it certifies exactly the reads that were already waiting
/// when it was issued (`certifies`, the §3.2 ordering rule).
pub(crate) struct ProbeRound {
    /// Monotonic issue number — the certification gate. Never reused, never
    /// changed by retransmission.
    pub(crate) seq: u64,
    /// Wire-level ack matching: the existing READ_PROBE nonce, now per-round.
    pub(crate) nonce: u64,
    /// Issue-time term — a §4 abandon trigger (a round never crosses terms).
    pub(crate) term: u32,
    /// Commit position at issue. Used ONLY for the §3.2 redundancy
    /// `debug_assert!` (commit is monotonic, so a read waiting at issue always
    /// has `commit_at <= commit_at_issue`) — never as the certification gate.
    pub(crate) commit_at_issue: u64,
    /// Distinct voting ackers, self-seeded (acks: 1) — same discipline as the
    /// per-read barrier this replaces.
    ackers: Vec<NodeId>,
    /// Voter majority captured at issue time. A voter-set change voids the
    /// whole round (the node's `rebuild_peer_maps` hook), so this never goes
    /// stale while the round is live.
    quorum: usize,
    /// Last (re)send, for `should_retransmit`.
    last_send_ns: u64,
}

impl ProbeRound {
    pub(crate) fn new(
        seq: u64,
        nonce: u64,
        quorum: usize,
        self_id: NodeId,
        term: u32,
        commit_at_issue: u64,
        now_ns: u64,
    ) -> ProbeRound {
        // quorum == 1 never reaches a round: admission fast-paths single-node
        // reads straight to AwaitApplied (node.rs, unchanged by Rung A).
        debug_assert!(quorum >= 2, "single-node reads bypass rounds entirely");
        ProbeRound {
            seq,
            nonce,
            term,
            commit_at_issue,
            ackers: vec![self_id],
            quorum,
            last_send_ns: now_ns,
        }
    }

    /// Count a DISTINCT voter ack (duplicates and self never advance — self is
    /// pre-seeded). Returns true iff quorum is reached; the caller consumes
    /// the round on the first true. Membership (voters-only) is the CALLER's
    /// check — it needs the live peer set.
    pub(crate) fn record_ack(&mut self, from: NodeId) -> bool {
        if !self.ackers.contains(&from) {
            self.ackers.push(from);
        }
        self.ackers.len() >= self.quorum
    }

    pub(crate) fn acks(&self) -> usize {
        self.ackers.len()
    }

    /// The §3.2 ordering rule: this round certifies exactly the reads already
    /// waiting when it was issued — a read admitted mid-round recorded
    /// `seq + 1` (the issue incremented the counter) and must wait for the
    /// next round, because this round's confirmation may predate its
    /// admission. NEVER replace this with a position comparison (spec §3.1).
    pub(crate) fn certifies(&self, read_round_seq: u64) -> bool {
        read_round_seq <= self.seq
    }

    pub(crate) fn should_retransmit(&self, now_ns: u64) -> bool {
        now_ns.saturating_sub(self.last_send_ns) >= PROBE_RETRANSMIT_NS
    }

    pub(crate) fn mark_sent(&mut self, now_ns: u64) {
        self.last_send_ns = now_ns;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round() -> ProbeRound {
        // seq 5, nonce 42, quorum 3 (needs self + two distinct voters),
        // self id 1, term 7, commit-at-issue 6048, issued at t=1000.
        ProbeRound::new(5, 42, 3, 1, 7, 6048, 1000)
    }

    #[test]
    fn self_seeds_one_ack_and_distinct_acks_reach_quorum() {
        let mut r = round();
        assert_eq!(r.acks(), 1, "self-seeded (acks: 1), same discipline as today");
        assert!(!r.record_ack(0), "second ack of three is not quorum");
        assert!(r.record_ack(2), "third distinct ack reaches quorum 3");
        assert_eq!(r.acks(), 3);
    }

    #[test]
    fn duplicate_and_self_acks_do_not_advance_the_count() {
        let mut r = round();
        assert!(!r.record_ack(0));
        assert!(!r.record_ack(0), "duplicate voter must not advance");
        assert!(!r.record_ack(1), "self is pre-seeded; a self ack must not advance");
        assert_eq!(r.acks(), 2);
    }

    #[test]
    fn ack_after_quorum_still_reports_quorum() {
        // The caller consumes the round on the first `true`; the pure type is
        // simply idempotent about the fact of quorum (>=, not ==).
        let mut r = round();
        r.record_ack(0);
        assert!(r.record_ack(2));
        assert!(r.record_ack(3), "a late extra voter still reports quorum reached");
    }

    #[test]
    fn certifies_exactly_the_reads_waiting_at_issue() {
        let r = round(); // seq 5
        assert!(r.certifies(4), "admitted before an earlier round: released");
        assert!(r.certifies(5), "admitted before THIS round was issued: released");
    }

    #[test]
    fn does_not_certify_a_mid_round_admission() {
        // THE case the parent brief's position rule got wrong (spec §3.1): a
        // read admitted while round 5 is in flight records round_seq 6 and must
        // wait for round 6 — this round's confirmation may predate its
        // admission.
        let r = round();
        assert!(!r.certifies(6));
    }

    #[test]
    fn retransmit_fires_at_the_interval_and_resets_on_send() {
        let mut r = round(); // last_send_ns = 1000
        assert!(!r.should_retransmit(1000 + PROBE_RETRANSMIT_NS - 1));
        assert!(r.should_retransmit(1000 + PROBE_RETRANSMIT_NS));
        r.mark_sent(1000 + PROBE_RETRANSMIT_NS);
        assert!(!r.should_retransmit(1000 + PROBE_RETRANSMIT_NS + 1));
        assert!(r.should_retransmit(1000 + 2 * PROBE_RETRANSMIT_NS));
    }
}
