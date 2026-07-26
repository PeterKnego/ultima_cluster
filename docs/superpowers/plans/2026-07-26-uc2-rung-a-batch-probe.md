# Rung A Batch-Probe Coalescing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the per-read READ_PROBE quorum round with a single shared in-flight round that certifies every read already waiting when it was issued.

**Architecture:** A new pure module `uc2_node/src/read_round.rs` owns the round state machine (`ProbeRound`: distinct-acker counting, the ordering-rule certification predicate, retransmit timing) with exhaustive unit tests. `uc2_node/src/node.rs` wires it into the existing barrier: admission records a round sequence instead of sending a probe; `on_read_probe_ack` targets the one live round; `advance_pending_reads` owns the round lifecycle (abandon, retransmit, next-round issue). Wire protocol, follower side, frontier wait, epoch backstop, and the 1 s deadline are untouched.

**Tech Stack:** Rust workspace, no new dependencies. Spec: `docs/superpowers/specs/2026-07-26-uc2-rung-a-batch-probe-design.md`. Companion explainer: `docs/notes/uc2-read-barrier-explained.md`.

## Global Constraints

- **Only `uc2_node` changes** (`src/lib.rs` one line, `src/node.rs`, new `src/read_round.rs`, `tests/query_barrier.rs`). No other crate, no Cargo.toml edits, no new dependencies, no wire-protocol change (`DGRAM_KIND_READ_PROBE`/`ACK` and `ReadProbeBody { nonce, from }` reused verbatim).
- **The certification gate is the ordering rule** (spec §3.2): a round releases exactly the `AwaitQuorum` reads with `round_seq <= round.seq`. The position comparison (`commit_at <= P_confirmed`) MUST NOT be the gate — spec §3.1 rejects it as unsafe. A `debug_assert!` of the position redundancy is required where the plan places it, and nowhere else.
- `PROBE_RETRANSMIT_NS = 2_000_000` (2 ms), verbatim from spec §4. Retransmits reuse the round's `seq` and `nonce` unchanged — a retransmit can never widen a certification set.
- A round is voided (never certifying anyone) on: `can_serve` false, term change, or **any voter-set change** (`rebuild_peer_maps`). Pending reads survive a voter-set void and wait for the next round; the existing RETRY path handles the other two.
- Single-node fast path unchanged: `quorum == 1` reads go straight to `AwaitApplied`, no round. `ProbeRound` debug-asserts `quorum >= 2`.
- The `skip-read-barrier` mutation tooth keeps its exact semantics (admission forces `AwaitApplied`; the `#[cfg(feature = "mutation-testing")]` blocks are preserved byte-for-byte where the plan doesn't explicitly move them).
- `cargo clippy --workspace --all-targets -- -D warnings` clean at every commit.
- Never write test/scratch artifacts to `/tmp` (RAM tmpfs, no swap). The elle harness (Task 5) needs `ELLE_DIR` under `/home/claude` — never `/tmp`.
- Execution happens in a git worktree; never run git commands in `/home/claude/ultima/ultima_cluster` itself (another session's checkout).

---

### Task 1: The pure round module (`read_round.rs`)

**Files:**
- Create: `uc2_node/src/read_round.rs`
- Modify: `uc2_node/src/lib.rs:30` (one `mod` line)
- Test: inline `#[cfg(test)] mod tests` in `read_round.rs`

**Interfaces:**
- Consumes: `uc2_consensus::election::NodeId` (a `u32`).
- Produces (all `pub(crate)`, used by Tasks 2–3):
  - `const PROBE_RETRANSMIT_NS: u64 = 2_000_000`
  - `struct ProbeRound { pub(crate) seq: u64, pub(crate) nonce: u64, pub(crate) term: u32, pub(crate) commit_at_issue: u64, /* private: */ ackers, quorum, last_send_ns }`
  - `ProbeRound::new(seq: u64, nonce: u64, quorum: usize, self_id: NodeId, term: u32, commit_at_issue: u64, now_ns: u64) -> ProbeRound`
  - `fn record_ack(&mut self, from: NodeId) -> bool` — true iff quorum is now reached
  - `fn acks(&self) -> usize`
  - `fn certifies(&self, read_round_seq: u64) -> bool`
  - `fn should_retransmit(&self, now_ns: u64) -> bool`
  - `fn mark_sent(&mut self, now_ns: u64)`

- [ ] **Step 1: Declare the module**

In `uc2_node/src/lib.rs`, after `mod node;` (line 30), add:

```rust
mod read_round;
```

- [ ] **Step 2: Write the failing tests**

Create `uc2_node/src/read_round.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Rung A (batch-probe coalescing): the probe-round state machine and the
//! ordering-rule certification predicate. Pure — no I/O, no clock reads; the
//! node wires it to the wire and the duty cycle.
//!
//! Spec: `docs/superpowers/specs/2026-07-26-uc2-rung-a-batch-probe-design.md`.
//! Plain-language account: `docs/notes/uc2-read-barrier-explained.md`.

use uc2_consensus::election::NodeId;

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
        // wait for round 6 — this round's confirmation predates its admission.
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p uc2_node read_round`
Expected: FAIL — `cannot find struct ProbeRound` / `PROBE_RETRANSMIT_NS` not found.

- [ ] **Step 4: Implement**

Insert between the `use` line and the test module:

```rust
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p uc2_node read_round`
Expected: PASS, 6 tests. (`node.rs` is untouched, so the build emits no new warnings; `ProbeRound` being unused outside tests is fine because the test module uses it — if a `dead_code` warning appears anyway, do NOT suppress it; Task 2 consumes the type and clears it. Note it in the commit message instead.)

- [ ] **Step 6: Commit**

```bash
git add uc2_node/src/lib.rs uc2_node/src/read_round.rs
git commit -m "feat(read-round): pure probe-round state machine for Rung A

Distinct-acker counting (self-seeded), the ordering-rule certification
predicate (a round certifies exactly the reads waiting at issue; a
mid-round admission records seq+1 and is NOT certified — the case the
parent brief's position rule got wrong), and 2 ms retransmit timing.
Pure and exhaustively unit-tested; the node wires it in next."
```

---

### Task 2: Wire the round into the node (admission, ack, certify)

> **AMENDED during execution (2026-07-26): Tasks 2 and 3 land as ONE commit.**
> The original claim below — that a stuck round "falls through to the existing
> 1 s deadline → RETRY path, so every existing test still passes" — is FALSE,
> and `lin_partition_v2::linearizable_under_lossy_links` proved it (~1/4 pass
> on a Task-2-only tree vs 3/3 on base). The deadline path recovers the READS,
> but nothing in Task 2 recovers the ROUND: its only clearing site is quorum
> completion, so a round whose probes/acks are lost under the test's 10% loss
> wedges forever, `maybe_issue_round` refuses from then on, and every later
> linearizable read RETRYs — which also flattens writes, because lincheck
> client threads retry an op before proceeding to their next. Task 3's
> retransmit is load-bearing liveness, not hardening. The task split below is
> retained for its step-by-step content; execute both before committing.

Replaces the per-read probe with the shared round on the happy path.

**Files:**
- Modify: `uc2_node/src/node.rs` — `PendingRead` (~line 218), `Consensus` fields (~1053), construction (~790), `drain_query_ring` (~1979-2022), `on_read_probe_ack` (~1924-1943), in-file tests (`mk_read` ~3675, `barrier_counts_distinct_ackers_then_forwards_on_service_catchup` ~3690, and the epoch-sentinel test that also uses `mk_read`)

**Interfaces:**
- Consumes: `crate::read_round::ProbeRound` (Task 1's exact signatures).
- Produces (Task 3 relies on): `Consensus::current_round: Option<ProbeRound>`, `Consensus::next_round_seq: u64`, `fn maybe_issue_round(&mut self)`, `PendingRead::round_seq: u64` (fields `nonce`/`ackers`/`quorum` GONE from `PendingRead`).

- [ ] **Step 1: Reshape `PendingRead`**

At `node.rs:218-235`, replace the struct with:

```rust
struct PendingRead {
    client_id: u32,
    local_seq: u32,
    /// The raw query bytes (forwarded verbatim after `expected_epoch`).
    query: Vec<u8>,
    /// Rung A ordering gate (spec §3.2): the seq of the NEXT probe round at
    /// admission. A round with `round.seq >= round_seq` was issued after this
    /// read arrived and may certify it; a smaller/absent round may not.
    round_seq: u64,
    /// Read index: the commit position at admission. The read may only be
    /// answered once the service has applied at least this far.
    commit_at: u64,
    /// Absolute `now_ns` deadline; past it the read is retried.
    deadline_ns: u64,
    phase: ReadPhase,
}
```

(`nonce`, `ackers`, `quorum` move to the round — the per-read `Vec<NodeId>` allocation disappears.)

- [ ] **Step 2: Add the round fields**

Near the top of `node.rs`, add the import alongside the existing `crate::` uses:

```rust
use crate::read_round::ProbeRound;
```

At the `Consensus` struct (~line 1053), directly after `pending_reads`, replace the `next_nonce` field + doc with:

```rust
    /// Rung A: the single in-flight READ_PROBE round, if any. At most one
    /// exists; it certifies exactly the reads waiting when it was issued.
    current_round: Option<ProbeRound>,
    /// Rung A: the seq the NEXT round will carry. Reads record it at
    /// admission; `maybe_issue_round` consumes-and-increments it.
    next_round_seq: u64,
    /// Monotonic per-node nonce — scopes each probe ROUND (no longer each
    /// read) so acks attribute to the right round on the wire.
    next_nonce: u64,
```

At the construction site (~line 790, `pending_reads: Vec::new(),`), add on the next lines:

```rust
            current_round: None,
            next_round_seq: 1,
```

- [ ] **Step 3: Rework admission in `drain_query_ring`**

Replace the linearizable-admission block (`node.rs:1979-2022`, from `let n = self.peers.len() + 1;` through the `if need_probe { self.send_read_probe(nonce); }` close) with:

```rust
                    let n = self.peers.len() + 1;
                    let quorum = n / 2 + 1;
                    let commit_at = self.cnc.counters().commit.load_acquire();
                    let deadline_ns = self.now_ns() + READ_BARRIER_TIMEOUT_NS;
                    // Single-node (quorum 1): skip straight to AwaitApplied —
                    // unchanged by Rung A; such reads never touch a round.
                    let phase = if quorum <= 1 {
                        ReadPhase::AwaitApplied
                    } else {
                        ReadPhase::AwaitQuorum
                    };
                    #[cfg_attr(not(feature = "mutation-testing"), allow(unused_mut))]
                    let mut read = PendingRead {
                        client_id,
                        local_seq,
                        query: buf,
                        // Rung A §3.2: record the NEXT round's seq — only a
                        // round issued at-or-after this admission may certify.
                        round_seq: self.next_round_seq,
                        commit_at,
                        deadline_ns,
                        phase,
                    };
                    // Mutation tooth: skip the READ_PROBE quorum barrier entirely — the
                    // read is served from local applied state without confirming
                    // leadership. A deposed leader then answers stale reads (the elle
                    // partition pass catches this under the strict model).
                    #[cfg(feature = "mutation-testing")]
                    if matches!(
                        crate::mutation::active(),
                        Some(crate::mutation::Mutation::SkipReadBarrier)
                    ) {
                        read.phase = ReadPhase::AwaitApplied;
                    }
                    self.pending_reads.push(read);
```

Then, immediately before `drain_query_ring`'s final `did` return, add:

```rust
        // Rung A: one round for everything admitted this cycle (issue site 1
        // of 2; the other is advance_pending_reads, which chains rounds while
        // demand persists).
        self.maybe_issue_round();
```

- [ ] **Step 4: Add `maybe_issue_round`**

Directly after `send_read_probe` (~line 1903):

```rust
    /// Rung A §4: issue a probe round iff at least one read awaits quorum and
    /// no round is in flight. Self-clocking — called at the end of
    /// `drain_query_ring` and from `advance_pending_reads`, so a completed
    /// round is immediately followed by the next while demand persists (~1
    /// round per RTT, independent of read rate; a lone read still gets its
    /// own immediate round, one RTT, exactly today's latency).
    fn maybe_issue_round(&mut self) {
        if self.current_round.is_some() {
            return;
        }
        if !self.pending_reads.iter().any(|r| r.phase == ReadPhase::AwaitQuorum) {
            return;
        }
        let quorum = (self.peers.len() + 1) / 2 + 1;
        debug_assert!(quorum >= 2, "AwaitQuorum reads cannot exist at quorum 1");
        let seq = self.next_round_seq;
        self.next_round_seq += 1;
        let nonce = self.next_nonce;
        self.next_nonce += 1;
        let round = ProbeRound::new(
            seq,
            nonce,
            quorum,
            self.id,
            self.sm.current_term(),
            self.cnc.counters().commit.load_acquire(),
            self.now_ns(),
        );
        self.current_round = Some(round);
        self.send_read_probe(nonce);
    }
```

- [ ] **Step 5: Rework `on_read_probe_ack`**

Replace the body (`node.rs:1924-1943`) with:

```rust
    /// Leader side of a READ_PROBE_ACK: membership-check the acker, match the
    /// ONE in-flight round by nonce, and count DISTINCT ackers. On quorum the
    /// round certifies every read that was already waiting when it was issued
    /// (the §3.2 ordering rule) — never a read admitted mid-round.
    fn on_read_probe_ack(&mut self, nonce: u64, from: NodeId) {
        // The read quorum is over VOTERS only (M6 Task 7 constraint): re-check
        // membership so a learner's (or forged/misrouted) ack can never
        // complete a round. `peers` is the voting set minus self.
        if !self.peers.contains(&from) {
            return;
        }
        let Some(round) = self.current_round.as_mut() else { return };
        if round.nonce != nonce {
            return; // an abandoned/completed round's straggler ack
        }
        if !round.record_ack(from) {
            return;
        }
        // Quorum: consume the round and release its certification set.
        let round = self.current_round.take().expect("matched above");
        for r in self.pending_reads.iter_mut() {
            if r.phase == ReadPhase::AwaitQuorum && round.certifies(r.round_seq) {
                // §3.2 redundancy check (never the gate): commit is monotonic,
                // so a read waiting at issue has commit_at <= the round's.
                debug_assert!(
                    r.commit_at <= round.commit_at_issue,
                    "ordering rule implies the position bound"
                );
                r.phase = ReadPhase::AwaitApplied;
            }
        }
        // Mid-round arrivals (round_seq > seq) stay AwaitQuorum; the next
        // round — issued by advance_pending_reads this same duty cycle — will
        // cover them.
    }
```

- [ ] **Step 6: Rewrite the in-file unit tests to the new shape**

The harness tests construct `PendingRead` directly. Replace `mk_read` (~line 3675) with:

```rust
    /// Push a linearizable read into the barrier for the harness node.
    fn mk_read(commit_at: u64, round_seq: u64, deadline_ns: u64) -> PendingRead {
        PendingRead {
            client_id: 7,
            local_seq: 1,
            query: Vec::new(),
            round_seq,
            commit_at,
            deadline_ns,
            phase: ReadPhase::AwaitQuorum,
        }
    }
```

Rewrite `barrier_counts_distinct_ackers_then_forwards_on_service_catchup`, preserving every original assertion's meaning (distinct-acker counting, non-member drop, quorum flip, service catch-up + epoch bracket):

```rust
    #[test]
    fn barrier_counts_distinct_ackers_then_forwards_on_service_catchup() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);

        // A read requiring a 3-way quorum, with a read index above the (zero)
        // service_applied so the forward waits for catch-up. The round is
        // constructed directly at quorum 3 (the harness cluster would compute
        // 2) so distinct-acker counting is observable across two peer acks.
        let far = h.cons.now_ns() + 10_000_000_000;
        h.cons.pending_reads.push(mk_read(6048, 1, far));
        let term = h.cons.sm.current_term();
        let now = h.cons.now_ns();
        h.cons.current_round =
            Some(crate::read_round::ProbeRound::new(1, 42, 3, h.cons.id, term, 6048, now));
        h.cons.next_round_seq = 2;

        // A non-member ack (id 99 is not in [0,1,2]) is dropped by the
        // membership check.
        h.cons.on_read_probe_ack(42, 99);
        assert_eq!(h.cons.current_round.as_ref().unwrap().acks(), 1);

        // Distinct acker 0, then a DUPLICATE 0 that must not advance the count.
        h.cons.on_read_probe_ack(42, 0);
        h.cons.on_read_probe_ack(42, 0);
        assert_eq!(h.cons.current_round.as_ref().unwrap().acks(), 2);
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitQuorum);

        // The second distinct acker reaches quorum 3 → the round completes,
        // certifies the waiting read, and is consumed.
        h.cons.on_read_probe_ack(42, 2);
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitApplied);
        assert!(h.cons.current_round.is_none(), "a completed round is consumed");

        // Service not yet caught up (applied 0 < commit_at 6048): parked.
        assert!(!h.cons.advance_pending_reads());
        assert_eq!(h.cons.pending_reads.len(), 1);

        // Caught up BUT service_epoch still 0: the sentinel-collision guard
        // keeps it parked (unchanged behavior).
        h.cons.cnc.service().service_applied.store_release(6048);
        assert!(!h.cons.advance_pending_reads());
        assert_eq!(h.cons.pending_reads.len(), 1, "epoch-0 must not forward");

        // A real incarnation attaches (epoch 1) → forwarded and dropped.
        h.cons.cnc.service().service_epoch.store_release(1);
        assert!(h.cons.advance_pending_reads());
        assert!(h.cons.pending_reads.is_empty(), "caught-up read must forward and drop");
    }
```

Update the epoch-sentinel test that follows (it also calls `mk_read`): the mechanical rule is to replace the middle argument — formerly `quorum` — with a `round_seq` of `1` (its value is irrelevant to that test: the read is exercised on the `AwaitApplied`/epoch path, never against a round). Change nothing else in that test; every assertion stays byte-identical.

- [ ] **Step 7: Add the mid-round-admission harness test**

Immediately after the rewritten test:

```rust
    /// Rung A §3.2, the crux: a round certifies ONLY reads admitted before it
    /// was issued. Read B, admitted mid-round, records the next seq and must
    /// stay AwaitQuorum when round 1 completes — then the follow-up round
    /// covers it.
    #[test]
    fn round_certifies_only_reads_admitted_before_issue() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        let far = h.cons.now_ns() + 10_000_000_000;

        // Read A admitted, then the round is issued (harness quorum: 2).
        h.cons.pending_reads.push(mk_read(6048, h.cons.next_round_seq, far));
        h.cons.maybe_issue_round();
        let round = h.cons.current_round.as_ref().expect("round issued");
        let (seq, nonce) = (round.seq, round.nonce);

        // Read B admitted MID-ROUND: records seq+1.
        h.cons.pending_reads.push(mk_read(6048, h.cons.next_round_seq, far));
        assert_eq!(h.cons.pending_reads[1].round_seq, seq + 1);

        // One peer ack reaches the harness quorum of 2 → round completes.
        h.cons.on_read_probe_ack(nonce, 0);
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitApplied, "A certified");
        assert_eq!(
            h.cons.pending_reads[1].phase,
            ReadPhase::AwaitQuorum,
            "B admitted mid-round must NOT be certified by this round"
        );
        assert!(h.cons.current_round.is_none());

        // The next round (fresh seq) covers B.
        h.cons.maybe_issue_round();
        let nonce2 = h.cons.current_round.as_ref().unwrap().nonce;
        assert_ne!(nonce2, nonce, "a new round, not a retransmit");
        h.cons.on_read_probe_ack(nonce2, 2);
        assert_eq!(h.cons.pending_reads[1].phase, ReadPhase::AwaitApplied);
    }
```

- [ ] **Step 8: Build, run the crate's tests, verify**

Run: `cargo test -p uc2_node` (unit + integration; the existing `query_barrier` test drives a real 3-node cluster through the new round path — its healthy read exercises issue→ack→certify live, and its partitioned read exercises the deadline fallback, since abandon/retransmit don't exist yet).
Expected: PASS, no new warnings.

Run: `cargo clippy -p uc2_node --all-targets -- -D warnings`
Expected: clean.

Run: `cargo clippy -p uc2_node --features mutation-testing --all-targets -- -D warnings`
Expected: clean. (This task edits code adjacent to the `#[cfg(feature = "mutation-testing")]` blocks, and the default build never compiles them — a broken cfg block would otherwise hide until weekly CI.)

- [ ] **Step 9: Commit**

```bash
git add uc2_node/src/node.rs uc2_node/src/lib.rs
git commit -m "feat(node): batch READ_PROBE rounds — one shared round certifies waiting reads

Admission records the next round seq instead of sending a per-read probe;
one round per demand burst is issued at the end of the query drain;
on_read_probe_ack targets the single in-flight round and, at quorum,
releases exactly the reads already waiting at issue (ordering rule, never
the position comparison — spec §3.1/§3.2, debug_assert pins the
redundancy). Per-read nonce/ackers/quorum state deleted.

Lifecycle hardening (retransmit, abandon triggers) lands next; until then
a stuck round falls through to the unchanged 1 s deadline → RETRY path."
```

---

### Task 3: Round lifecycle — abandon triggers, retransmit, chaining

**Files:**
- Modify: `uc2_node/src/node.rs` — `advance_pending_reads` (~2038), `rebuild_peer_maps` (~1412), in-file tests

**Interfaces:**
- Consumes: `current_round`, `maybe_issue_round`, `ProbeRound::{term, should_retransmit, mark_sent, nonce, new}` from Tasks 1–2.
- Produces: nothing new — this completes the spec §4 lifecycle.

- [ ] **Step 1: Abandon + empty-drop at the top of `advance_pending_reads`**

Replace the function's opening (through the `let can_serve = self.sm.can_serve();` line and the `#[cfg(feature = "mutation-testing")]` shadow that follows it) with:

```rust
    fn advance_pending_reads(&mut self) -> bool {
        if self.pending_reads.is_empty() {
            // Rung A: a round with no waiting reads certifies nobody — drop it
            // so the next admission starts a fresh round instead of waiting
            // out a stale one (its straggler acks no-op on the nonce check).
            self.current_round = None;
            return false;
        }
        let now = self.now_ns();
        let can_serve = self.sm.can_serve();
        // Rung A §4: a round never survives lost serving or a term change
        // (the voter-set trigger lives in rebuild_peer_maps). Checked against
        // the RAW can_serve, before the mutation shadow below: the tooth keeps
        // READS resolving on an isolated leader, but mutated reads bypass
        // rounds at admission, so no round should outlive real leadership.
        if let Some(round) = &self.current_round {
            if !can_serve || round.term != self.sm.current_term() {
                self.current_round = None;
            }
        }
        // `skip-read-barrier` tooth: keep resolving reads from local applied
        // state even after leadership is lost, so an isolated leader answers
        // stale reads instead of RETRY-ing (matches the admission bypass above).
        #[cfg(feature = "mutation-testing")]
        let can_serve = can_serve
            || matches!(
                crate::mutation::active(),
                Some(crate::mutation::Mutation::SkipReadBarrier)
            );
```

(The rest of the function body — the `while i < self.pending_reads.len()` loop — is unchanged.)

- [ ] **Step 2: Retransmit + chaining at the bottom**

Immediately before `advance_pending_reads`' final `did` return, add:

```rust
        // Rung A §4: re-probe a stuck round on the 2 ms interval (same seq,
        // same nonce — the certification set cannot widen), and chain the next
        // round the moment the previous completed while demand persists.
        let retransmit_nonce = self.current_round.as_mut().and_then(|round| {
            if round.should_retransmit(now) {
                round.mark_sent(now);
                Some(round.nonce)
            } else {
                None
            }
        });
        if let Some(nonce) = retransmit_nonce {
            self.send_read_probe(nonce);
        }
        self.maybe_issue_round();
```

- [ ] **Step 3: Void the round on any voter-set change**

In `rebuild_peer_maps` (~line 1412), after `self.peer_band = peer_band;`, add:

```rust
        // Rung A §4/§5: ANY voter-set change voids the in-flight probe round —
        // a round whose quorum was captured under the old config must not
        // certify under the new one (a resized quorum could elect elsewhere
        // before the old-config ack count means anything; mirrors the
        // leader-lease brief's M7 invalidation rule). Pending reads are NOT
        // dropped — they wait for the next round, issued with a freshly
        // captured quorum by the same duty cycle's advance_pending_reads.
        self.current_round = None;
```

- [ ] **Step 4: Harness tests for the lifecycle**

Add after `round_certifies_only_reads_admitted_before_issue`:

```rust
    /// Rung A §4: a voter-set change (any rebuild_peer_maps) voids the round;
    /// pending reads survive and the next round covers them under a fresh seq.
    #[test]
    fn voter_set_change_voids_round_but_keeps_reads() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        let far = h.cons.now_ns() + 10_000_000_000;
        h.cons.pending_reads.push(mk_read(6048, h.cons.next_round_seq, far));
        h.cons.maybe_issue_round();
        let old_nonce = h.cons.current_round.as_ref().unwrap().nonce;

        // Same-membership rebuild (the trigger is the rebuild itself — the
        // node cannot distinguish "same voters" cheaply and must not try).
        let members = [0u32, 1, 2];
        let config = ClusterConfig::genesis(
            members.iter().map(|id| (*id, addr_to_pair(h.cons.id_to_addr[id]))).collect(),
            Vec::new(),
        );
        h.cons.rebuild_peer_maps(&config);
        assert!(h.cons.current_round.is_none(), "voter-set change voids the round");
        assert_eq!(h.cons.pending_reads.len(), 1, "reads survive the void");
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitQuorum);

        // A straggler ack for the voided round is a no-op.
        h.cons.on_read_probe_ack(old_nonce, 0);
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitQuorum);

        // The next round (fresh seq + nonce, fresh-config quorum) covers it.
        h.cons.maybe_issue_round();
        let round = h.cons.current_round.as_ref().unwrap();
        assert_ne!(round.nonce, old_nonce);
        let nonce2 = round.nonce;
        h.cons.on_read_probe_ack(nonce2, 0);
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitApplied);
    }

    /// Rung A §4: a round stamped with a stale term is abandoned by
    /// advance_pending_reads (driven directly — the harness stamps a
    /// mismatched term rather than running a full re-election).
    #[test]
    fn stale_term_round_is_abandoned() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        let far = h.cons.now_ns() + 10_000_000_000;
        h.cons.pending_reads.push(mk_read(6048, 1, far));
        let stale_term = h.cons.sm.current_term() - 1;
        let now = h.cons.now_ns();
        h.cons.current_round =
            Some(crate::read_round::ProbeRound::new(1, 42, 2, h.cons.id, stale_term, 6048, now));
        h.cons.next_round_seq = 2;

        h.cons.advance_pending_reads();
        // The stale round is gone; the read survived (deadline far away) and a
        // FRESH round was chained in the same call (issue site 2 of 2).
        let round = h.cons.current_round.as_ref().expect("fresh round chained");
        assert_eq!(round.term, h.cons.sm.current_term());
        assert_ne!(round.nonce, 42);
    }

    /// Rung A: a round with no waiting reads is dropped, not waited out.
    #[test]
    fn round_with_no_waiting_reads_is_dropped() {
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        let term = h.cons.sm.current_term();
        let now = h.cons.now_ns();
        h.cons.current_round =
            Some(crate::read_round::ProbeRound::new(1, 42, 2, h.cons.id, term, 6048, now));
        assert!(!h.cons.advance_pending_reads());
        assert!(h.cons.current_round.is_none());
    }
```

(If `ClusterConfig`/`addr_to_pair` are not already in the test module's scope, import them the way the harness construction at ~line 3310 does — it builds a genesis config with exactly these helpers.)

- [ ] **Step 5: Run the tests**

Run: `cargo test -p uc2_node`
Expected: PASS (all in-file tests + `query_barrier` + the other integration tests).

Run: `cargo clippy -p uc2_node --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add uc2_node/src/node.rs
git commit -m "feat(node): probe-round lifecycle — abandon, 2 ms retransmit, chaining

A round is voided on lost serving, a term change, or ANY voter-set change
(rebuild_peer_maps — a round never straddles configs; reads survive and
wait for a fresh-config round). A stuck round re-probes every 2 ms with
the same seq+nonce so batching cannot concentrate datagram loss into the
1 s deadline. advance_pending_reads chains the next round the moment the
previous completes, and drops a round nobody waits on."
```

---

### Task 4: Concurrent-reads integration test

Proves the wiring end to end on a real 3-node loopback cluster: a burst of concurrent linearizable reads (batched through shared rounds) all return correct values, and after partitioning the leader a concurrent burst is answered RETRY/NOT_LEADER — never a stale value.

**Files:**
- Modify: `uc2_node/tests/query_barrier.rs` (reuses `spawn_cluster`, `await_single_leader`, `cut`, `drive_submits`, `CountSm`)

**Interfaces:**
- Consumes: the public API only (`Client::query_linearizable`, `ClientError::{Retry, NotLeader}`) — this test observes behavior, not internals.

- [ ] **Step 1: Write the test**

Append to `uc2_node/tests/query_barrier.rs`:

```rust
/// Rung A capstone: a burst of CONCURRENT linearizable reads — batched through
/// shared probe rounds on the leader — all observe the committed total, and
/// after the leader is partitioned a concurrent burst is answered
/// RETRY/NOT_LEADER, never a stale value. One client per thread: `Client` ops
/// are per-connection sequential, and concurrency is the point here.
#[test]
fn concurrent_batched_reads_stay_linearizable_across_partition() {
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 30);
    let leader_dir = c.dirs[leader].clone();

    let svc = ServiceBuilder::new(ServiceConfig::new(&leader_dir, APP), CountSm::default())
        .start()
        .unwrap();
    let client = Client::connect(&leader_dir, APP).unwrap();
    drive_submits(&client, 100);

    // Healthy burst: 8 readers x 5 linearizable reads, all concurrent. Every
    // read must observe the full committed total (no writes are in flight, so
    // any other value is a stale or torn answer).
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let dir = leader_dir.clone();
                s.spawn(move || {
                    let cl = Client::connect(&dir, APP).unwrap();
                    for _ in 0..5 {
                        let v: u64 = cl.query_linearizable(&()).expect("healthy lin read");
                        assert_eq!(v, 100, "stale/torn linearizable read");
                    }
                    cl.shutdown();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    });

    // Partition the leader from BOTH followers, then fire a concurrent burst.
    // No round can complete; every read must resolve RETRY (or NOT_LEADER once
    // the leader steps down) within the ~1 s barrier deadline — never Ok.
    for f in (0..3).filter(|&i| i != leader) {
        cut(&c.nodes, leader, f, &c.members);
    }
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let dir = leader_dir.clone();
                s.spawn(move || {
                    let cl = Client::connect(&dir, APP).unwrap();
                    let started = Instant::now();
                    let res: Result<u64, ClientError> = cl.query_linearizable(&());
                    let elapsed = started.elapsed();
                    assert!(
                        matches!(res, Err(ClientError::Retry) | Err(ClientError::NotLeader { .. })),
                        "isolated leader answered a batched linearizable read (got {res:?})"
                    );
                    assert!(elapsed < Duration::from_secs(5), "took {elapsed:?}");
                    cl.shutdown();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    });

    client.shutdown();
    svc.stop();
    for n in c.nodes.drain(..) {
        n.stop();
    }
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p uc2_node --test query_barrier`
Expected: PASS, both tests. (This test verifies existing behavior of the completed feature — if the healthy burst hangs or the partitioned burst returns `Ok`, that is a Rung A bug: report it, do not weaken the assertions.)

- [ ] **Step 3: Commit**

```bash
git add uc2_node/tests/query_barrier.rs
git commit -m "test(query-barrier): concurrent batched reads across a partition

8 concurrent readers against a healthy leader all observe the committed
total (batched through shared rounds); the same burst against a
partitioned leader resolves RETRY/NOT_LEADER within the deadline, never a
stale value."
```

---

### Task 5: Full gates

**Files:** none new — verification only, plus any fixes it forces.

- [ ] **Step 1: Workspace suite**

Check the box first: `uptime && free -g` — if load is high or available memory is under ~3 GB (a concurrent session may be running), wait; UC's integration tests are election-timing-sensitive and flake under contention (adjudicated history in the repo).

Run: `cargo test --workspace`
Expected: green. Pay attention to `lin_v2` and `lin_partition_v2` — they drive the read path under failover and partition and are the spec §6.3 outer net.

- [ ] **Step 2: Clippy, both feature states**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

Run: `cargo clippy -p uc2_node --features mutation-testing --all-targets -- -D warnings`
Expected: clean (the tooth's cfg blocks compile only under this feature).

- [ ] **Step 3: Elle clean tier**

Run: `ELLE_DIR=/home/claude/elle-out scripts/elle_check.sh` (needs java+jq; `ELLE_DIR` on real disk, NEVER `/tmp`).
Expected: 5 list-append passes GREEN under both models. (The mutation tier — including the `skip-read-barrier` tooth this change is adjacent to — runs in weekly CI; do not run it locally unless a clean-tier failure implicates the read path.)

- [ ] **Step 4: Commit any fixes; otherwise no commit**

If the gates forced changes, commit them with a message naming which gate and why. A green run produces no commit.

---

## Post-plan (explicitly NOT part of this plan)

- **Fleet re-measurement** — user-approval-gated (it spends money): re-run the
  read-profile harness on the same 3-host topology and sweep
  (`bench-infra/scripts/m6_fleet_gate.py --fleet --read-profile`, same
  pre-committed `decide --rungs` rule) and record the before/after against
  `docs/benchmarks/uc2-read-profile-2026-07-26.md`. Expected shape: the
  lin/snap ratio rises materially at high concurrency; probe traffic stops
  scaling with read rate. Consider extending the ladder past 1024 readers —
  the baseline's linearizable arm had not plateaued.
- Rung B remains parked behind the Veil V2 coherence-window result.
