# UC v2 — Rung A: batch-probe coalescing for the linearizable read path

**Date:** 2026-07-26
**Status:** Design spec, approved for implementation.
**Motivation:** the 2026-07-26 fleet measurement
(`docs/benchmarks/uc2-read-profile-2026-07-26.md`): the ReadIndex barrier costs
~58% of read capacity (244,052 vs 585,414 reads/s, read-only arm, 3-host AWS
fleet), the alternatives (query-drain cap, load generator) were ruled out by
data, and the pre-committed decision rule returned **Rung A JUSTIFIED** on both
write mixes.
**Parent brief:** `docs/superpowers/specs/2026-07-24-uc2-leader-lease-design.md`
§3 — an exploratory sketch, which this spec supersedes for Rung A. One of the
sketch's rules is rejected here on safety grounds (§3.1).
**Companion explainer:** `docs/notes/uc2-read-barrier-explained.md` — the
plain-language account of the barrier and of why the ordering rule replaces the
position rule.
**Out of scope:** Rung B (time-based leader lease) — sequenced behind the Veil
V2 coherence-window result per the parent brief §5. Nothing here introduces a
clock into read safety.

---

## 1. The problem

Every linearizable read today runs its own quorum probe round.
`drain_query_ring` allocates a nonce, captures `commit_at`, and calls
`send_read_probe(nonce)` **per read, inside the drain loop**
(`uc2_node/src/node.rs:1979-2022`). At `QUERY_DRAIN_PER_CYCLE = 64` that is up
to 64 independent probe fan-outs per duty cycle, each asking every voting peer
the same question — "am I still the leader?" — and each ack processed
individually by `on_read_probe_ack`'s per-read nonce scan
(`node.rs:1932-1943`).

One confirmed round certifies leadership for every read that was already
waiting when it went out. Rung A stops repeating the question: probe traffic
drops from `O(reads · peers)` to `O(rounds · peers)`, with rounds self-limited
to ~1 per RTT (§4).

**The certification rule is the existing barrier rule applied to a set.** No
new wire kinds, no follower-side change, no safety-model change — with one
sharpening the parent brief missed, next.

## 2. What does not change

Everything except step 1 (prove current-term leadership) of the barrier:

- The **apply-frontier wait** (`AwaitApplied`, `service_applied >= commit_at`),
  the **capture-recheck bracket**, and the **service-epoch backstop** —
  unchanged (`advance_pending_reads`, `node.rs:2038-2117`;
  `uc2_service/src/apply.rs:316-344`).
- The **follower side**: `on_read_probe`'s ack-iff-term-matches test
  (`node.rs:1910-1918`) is the teeth of the no-stale-read guarantee and is not
  touched. Probes still go to voters only; `on_read_probe_ack` still counts
  distinct voters only.
- The **per-read 1 s deadline** (`READ_BARRIER_TIMEOUT_NS`) and the
  `can_serve`-drop-to-`MSG_V2_RETRY` path — unchanged, and they remain the
  outer backstop for everything below.
- The **single-node fast path**: `quorum == 1` still goes straight to
  `AwaitApplied` with no round at all (`node.rs:1987-1992`).
- The **wire protocol**: `DGRAM_KIND_READ_PROBE`/`ACK` and
  `ReadProbeBody { nonce, from }` are reused verbatim. A follower cannot tell a
  batched round from today's per-read probe.
- The **`skip-read-barrier` mutation tooth** keeps its meaning: it forces
  `AwaitApplied` at admission, bypassing rounds entirely.

## 3. The certification rule (the crux)

### 3.1 Why the parent brief's position rule is rejected

The brief's sketch certifies every pending read with
`commit_at <= P_confirmed`, where `P_confirmed` is the commit position captured
when the round was sent. **This is unsafe.** A round's acks can all arrive
*before* a read is admitted:

```
t0  round R sent, P captured
t1  quorum acks R           — leadership confirmed AS OF t1
t2  leader partitioned/deposed, does not know yet
t3  read r admitted, commit_at_r captured
```

If no commits landed between t0 and t3, `commit_at_r == P` and the position
test passes — but the confirmation predates the read, and the leader may have
been deposed in the gap. That is precisely the isolated-leader scenario the
barrier exists to defeat. ReadIndex requires the leadership confirmation to
complete **at or after** the read's index is captured; a position comparison
cannot encode that ordering, because a quiet period makes the positions equal
while the times differ.

### 3.2 The ordering rule

Certify by **which reads were already waiting when the round was issued**:

- Rounds carry a monotonic sequence number `seq`.
- At admission, a read records `round_seq = next_round_seq` — the seq of the
  *next* round to be issued.
- When issuing a round: `round.seq = next_round_seq; next_round_seq += 1`.
- A round reaching quorum releases every `AwaitQuorum` read with
  `read.round_seq <= round.seq` to `AwaitApplied`.

The ordering property is then arithmetic: a read admitted while round S is in
flight records `S+1` (the increment happened at issue), so S cannot release it;
a read admitted before S was issued records `<= S` and is released — and S was,
by construction, issued after that read arrived. No timestamps, no position
comparison, no clock.

The position test becomes redundant rather than wrong: commit is monotonic, so
a read pending at issue time necessarily has `commit_at <=` whatever the round
confirmed. Implementations MUST NOT reintroduce it as the gate; a
`debug_assert!` of the redundancy is welcome.

**Retransmits reuse `seq` and `nonce` unchanged** (§4), so a retransmit can
never widen a round's certification set. Extra acks for an already-confirmed
guarantee only strengthen it.

## 4. Round lifecycle

At most **one round in flight at a time** — self-clocking, no timer, no knob:

- **Issue** when at least one `AwaitQuorum` read is pending and
  `current_round.is_none()`. Two call sites: the end of `drain_query_ring` (all
  reads drained this cycle share one round) and `advance_pending_reads` (the
  next round starts the moment the previous completes, picking up reads that
  arrived mid-round). Probe traffic self-limits to ~1 round per RTT; at the
  fleet's measured ~0.16 ms single-read p50 that is ~6k rounds/s worst case,
  independent of read rate. Under low load a lone read gets its own immediate
  round — one RTT, no added latency versus today. Worst-case added wait for a
  read that just missed a round: one round (≈1 RTT).
- **Retransmit** the same round — same `seq`, same `nonce`, same body — if
  quorum has not been reached after `PROBE_RETRANSMIT_NS = 2_000_000` (2 ms),
  measured from `last_send_ns`; update `last_send_ns` on each send. Rationale:
  batching concentrates loss — today a lost probe datagram delays one read
  until its 1 s deadline; under batching it would stall the whole waiting set.
  2 ms is ~13× the measured probe RTT (clear of spurious fires) and recovers
  ~500× faster than the deadline backstop. Repeated retransmit is fine: acks
  are idempotent (distinct-acker counting) and followers answer statelessly.
- **Complete** on quorum: release qualifying reads per §3.2, set
  `current_round = None`, and immediately issue the next round if `AwaitQuorum`
  reads remain (the mid-round arrivals).
- **Abandon** — set `current_round = None` without certifying anyone — on any
  of: `can_serve` going false, a term change, or **a voter-set change** (the M7
  case, §5). For the first two, the existing `advance_pending_reads` logic
  already drops the pending reads to `MSG_V2_RETRY`; for a voter-set change the
  reads stay pending and wait for the next round, issued under the new config.
  A round object must never survive into a new term, a re-election, or a new
  voter set.

## 5. Data structures

`PendingRead` (`node.rs:218-235`) **loses** `nonce`, `ackers`, `quorum` (now
round-level) and **gains** `round_seq: u64`. Keeps `client_id`, `local_seq`,
`query`, `commit_at`, `deadline_ns`, `phase`. Net per-read state shrinks — the
per-read `Vec<NodeId>` allocation disappears entirely.

New, on the consensus agent:

```rust
/// The single in-flight probe round (Rung A). At most one exists at a time.
struct ProbeRound {
    /// Monotonic issue number — the certification gate (§3.2).
    seq: u64,
    /// Wire-level ack matching; the existing READ_PROBE nonce, now per-round.
    nonce: u64,
    /// Issue-time term — carrier for §4's term-change abandon trigger.
    term: u32,
    /// Commit position at issue — used ONLY for §3.2's redundancy
    /// `debug_assert!`, never as the certification gate.
    commit_at_issue: u64,
    /// Distinct voting ackers, self-seeded (acks: 1), same discipline as today.
    ackers: Vec<NodeId>,
    /// Voter majority at issue time.
    quorum: usize,
    /// Last (re)send, for the 2 ms retransmit.
    last_send_ns: u64,
}
```

plus `current_round: Option<ProbeRound>` and `next_round_seq: u64` fields, and
the existing `next_nonce` counter now advances per round rather than per read.

`on_read_probe_ack` changes from a linear scan of `pending_reads` to: match
`current_round` by nonce, count the distinct voter, and on quorum run the §3.2
release sweep. Membership re-checks (voters-only, learner ack can never count)
carry over verbatim.

**M7 note:** `quorum` is captured at issue time from the then-current voter
set, same as today's per-read capture. **Any voter-set change voids the
in-flight round** — add it to §4's abandon triggers explicitly, do not assume
it arrives as a term/`can_serve` transition: a promote/demote can resize the
voter set under a still-serving leader, and a round whose `quorum` was captured
under the old config must not certify under the new one (a resized quorum could
elect elsewhere before the old-config ack count means anything). This mirrors
the parent brief's lease-invalidation rule for M7 ("any `config.state`
transition that changes voters must void the current lease"); pending reads are
NOT dropped — they simply wait for the next round, issued under the new config
with a freshly captured quorum.

## 6. Verification

Per the decision taken during design (the sim has no read plane, so a sim
invariant would require building one — disproportionate here):

1. **Pure certification function, exhaustively unit-tested.** Extract the
   release decision into a pure function of `(round_seq, &[PendingRead])` →
   released set, with tests pinning: release of all reads with
   `round_seq <= S`; **non-release of a read admitted mid-round**
   (`round_seq == S+1` — the exact case the parent brief's rule got wrong);
   idempotence under retransmitted acks; empty-set and all-set edges.
2. **In-process integration test** in the `query_barrier.rs` style: concurrent
   linearizable reads across a leadership change, asserting reads admitted
   after depose are never answered from the old leader's round, plus the
   batched happy path (N concurrent reads, one round observed).
3. **Existing tiers as the outer net**: `lin_v2`, `lin_partition_v2`, elle
   clean passes, and the `skip-read-barrier` mutation tooth must all stay
   green, unchanged.
4. **Re-measure with the read-profile harness** (fleet, same pre-committed
   rule, same `decide --rungs`) after landing — the before/after is directly
   comparable to `docs/benchmarks/uc2-read-profile-2026-07-26.md`. Expected
   shape: the lin/snap ratio rises materially at high concurrency; the probe
   round-rate ceases to scale with read rate.

## 7. Risks

- **Batching concentrates datagram loss** — mitigated by the 2 ms retransmit
  (§4); the 1 s per-read deadline remains the backstop.
- **A mis-scoped release is a stale read**, visible only under partition — the
  §6.1 pure-function tests target exactly the mis-scoping, and
  `lin_partition_v2` exercises the partition setting.
- **Head-of-line effect**: a round certifies at quorum speed; one slow follower
  does not matter (quorum, not all), and a dead follower is already today's
  problem.

## Appendix — code anchors

| Concern | Location |
| --- | --- |
| Per-read probe today (to be replaced) | `uc2_node/src/node.rs:1979-2022` |
| `PendingRead` / `ReadPhase` | `node.rs:207-235` |
| Probe send / follower ack / ack counting | `node.rs:1895-1943` |
| Advance + deadline + `can_serve` drop | `node.rs:2038-2117` |
| Single-node fast path | `node.rs:1987-1992` |
| Read-barrier timeout (1 s) | `node.rs:191` |
| Mutation tooth (`skip-read-barrier`) | `node.rs:2005-2015` |
| Integration-test precedent | `uc2_node/tests/query_barrier.rs` |
| Measurement harness | `uc2_node/examples/read_profile.rs` |
