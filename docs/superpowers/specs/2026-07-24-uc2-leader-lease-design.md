# UC v2 — Leader Lease & Read-Barrier Throughput: Exploratory Design Brief

> **STATUS (2026-07-26): RESOLVED — Rung A shipped and re-measured; Rung B
> discharged for the LAN goal. Do not pick up Rung B from this brief.**
>
> The measure-first sequencing this brief mandated (§6.1) ran to completion,
> two days after the brief was written:
>
> - **Measurement:** `docs/benchmarks/uc2-read-profile-2026-07-26.md` — the
>   barrier cost ~58% of read capacity; the pre-committed rule returned
>   "Rung A JUSTIFIED" on both write mixes.
> - **Rung A built:** spec
>   `docs/superpowers/specs/2026-07-26-uc2-rung-a-batch-probe-design.md`,
>   merged `16eff8f`. NOTE: this brief's §3 certification sketch ("piggyback
>   if their `commit_at` is already `<= P_confirmed`") was **rejected as
>   unsafe** during design — a round's confirmation can predate a read's
>   admission while a quiet period makes the positions equal. The shipped rule
>   certifies by ordering (round seq at admission), not position. See that
>   spec's §3.1 and `docs/notes/uc2-read-barrier-explained.md`.
> - **Re-measurement:**
>   `docs/benchmarks/uc2-read-profile-2026-07-26-after-rung-a.md` — barrier
>   cost now ~0% (lin/snap 100.2% read-only, 96.7% mixed; ~953k linearizable
>   reads/s at p50 1.08 ms on the 3-host fleet).
> - **Rung B verdict, per §2's own rule ("measure after A before committing
>   to B"): no remaining LAN justification.** The residual 0–3.3% is inside
>   run noise; a bounded-clock-drift assumption in read safety would buy
>   nothing measurable. Rung B would be revived only by the WAN/cross-region
>   motivation §7 explicitly scopes out (the async-learner / Standby story),
>   as its own project. Its verification prerequisite (the Veil
>   coherence-window work, §5) has since been satisfied — the V2 hunt came
>   back exhaustive-clean — so if WAN ever becomes the goal, only the WAN
>   case itself needs making.
>
> The text below is retained unamended as the historical record of the
> pre-measurement reasoning.

**Date:** 2026-07-24
**Status:** superseded — see the resolution block above. Originally: exploratory
brief (NOT an implementation spec), no commitment to build.
**Motivation:** LAN read throughput (co-located cluster, high read rates).
**Author:** design dialogue, Peter Knego + Claude.

---

## 1. Problem & goal

Every linearizable read in UC today pays a **quorum probe round-trip** — the
ReadIndex barrier. On a co-located LAN this round-trip is sub-millisecond, so the
per-read *latency* it costs is small. The cost we actually care about at high read
rates is **the probe traffic itself and the load it places on the single-writer
polling agents**:

- Each linearizable read allocates a nonce and fans a `DGRAM_KIND_READ_PROBE` out
  to every voting peer (`uc2_node/src/node.rs:1983`, `send_read_probe` at
  `node.rs:1895-1903`).
- Each follower's ack is processed on the leader's consensus agent
  (`on_read_probe_ack`, `node.rs:1924-1943`), and each follower spends receiver-agent
  work on `on_read_probe` (`node.rs:1910-1918`).

So read throughput consumes CPU and datagram bandwidth on exactly the contended
resources UC's design works hardest to keep uncontended: the consensus, sender, and
receiver single-writer agents. At `R` reads/sec across `n` nodes, that is
`O(R · n)` probe datagrams and their ack processing — pure coordination overhead
that carries no state change.

**Goal:** reduce or eliminate per-read probe overhead **without weakening the
linearizable guarantee** the barrier provides, and without disturbing UC's
clock-independent safety story more than necessary.

**Non-goal:** cross-region / WAN read latency, DR failover, or any consistency
weakening. Those are explicitly out of scope (§7).

### Baseline recap: what the barrier proves today

The read path is a ReadIndex design in three steps (traced in the codebase; see
`uc2_node/tests/query_barrier.rs` for the capstone):

1. **Prove current-term leadership** — capture `commit_at = commit.load_acquire()`
   at admission (`node.rs:1983`), then confirm a quorum of *voters* are still in
   this leader's term via the probe round. The teeth: a follower acks only if the
   probe's term equals its own (`on_read_probe`, `node.rs:1910-1913`); the leader
   counts distinct voter acks to quorum (`on_read_probe_ack`, `node.rs:1929-1939`).
2. **Wait for the service frontier** — block until the apply agent publishes
   `service_applied >= commit_at` (`advance_pending_reads`, `node.rs:2038-2117`;
   publish at `uc2_service/src/apply.rs:186`).
3. **Epoch/term guards** — the capture-recheck bracket + `e >= 1` guard
   (`node.rs:2071-2099`) and the service-epoch backstop (`apply.rs:316-344`),
   which defend the crashing-service TOCTOU.

Any optimization below touches **only step 1**. Steps 2 and 3 are about the
*service incarnation*, not leadership, and are unchanged by everything in this
brief. On leadership loss or the `READ_BARRIER_TIMEOUT_NS` (1 s, `node.rs:191`)
deadline, a read drops to a side-effect-free `send_retry` (`node.rs:2058-2062`) —
never a stale value.

---

## 2. The two-rung answer

Two independent optimizations, in ROI order for the LAN-throughput goal:

- **Rung A — Batch-probe coalescing (clock-free).** Amortize one probe round over
  all reads in flight. Recommended first: it captures most of the win at high read
  concurrency with **zero change to the safety model**.
- **Rung B — Time-based leader lease, fast-path-only.** Eliminate the residual
  probe traffic that A still pays, by proving current-term leadership from a
  *time bound* instead of a live quorum. Built **on top of A** — the batched-probe
  path becomes the lease's fallback. Cost: a bounded-clock-drift assumption enters
  read reasoning.

The ordering is deliberate. At high read rates there are, by definition, many
reads concurrently in flight — which is exactly the regime where A's amortization
is most effective and where B's marginal gain over A is smallest. Measure after A
before committing to B (§6).

A third option — **epoch/liveness-based leases (CockroachDB-style)** — was
considered and declined for this goal: it shifts clock-dependence into a
node-liveness lease record but adds substantial machinery, and buys little over B
on a co-located cluster. Recorded here as considered-not-chosen.

---

## 3. Rung A — Batch-probe coalescing

### Idea

A single probe confirmed at committed position `P` certifies leadership for the
whole cluster as of that probe round. That certification is **not specific to one
read** — it validates *every* pending read whose captured read index satisfies
`commit_at <= P`. Today UC throws that away by issuing a fresh nonce and probe per
read.

### Mechanics (sketch)

- Instead of `send_read_probe` per admitted read, the consensus agent issues **one
  probe per duty cycle** (or per burst) while any linearizable reads are pending —
  a single nonce, a single quorum round.
- When that probe reaches quorum, record the confirmed position `P_confirmed` (the
  `commit_at` captured when the probe was sent). Every pending read with
  `commit_at <= P_confirmed` transitions from `AwaitQuorum` to `AwaitApplied` in
  one sweep.
- Reads admitted *after* the in-flight probe was sent wait for the next probe
  round (bounded by the duty-cycle cadence), or piggyback if their `commit_at`
  is already `<= P_confirmed`.

This splices into the existing `PendingRead` / `ReadPhase` state machine
(`node.rs:204-235`) and `advance_pending_reads` (`node.rs:2038-2117`) — the phase
transition becomes set-valued rather than per-read. No new wire kinds; the probe
datagram is unchanged.

### Correctness

The certification rule is exactly the barrier's existing rule applied to a set:
"a quorum of voters in my current term at probe time ⇒ my `commit_at` is a valid
linearization point." That holds for any `commit_at <= P_confirmed` unchanged.
**No new assumption; no proof-model change.** The distinct-acker and voters-only
checks (`node.rs:1929-1934`) carry over verbatim.

### Cost & risk

- Adds at most one duty-cycle of latency to a read that just missed the current
  probe round — negligible on a busy-spin LAN cluster, and only on the *latency*
  axis we already deprioritized.
- Purely additive to the state machine; the fallback and retry semantics are
  untouched.

**Expected win:** probe datagrams drop from `O(R · n)` to `O(cycles · n)`, i.e.
bounded by the probe cadence rather than the read rate. At high `R` this is the
dominant reduction.

---

## 4. Rung B — Time-based leader lease (fast-path-only)

### Idea

A lease lets the leader prove *"I am the only leader right now"* from a clock bound
instead of a live quorum probe. While the lease is valid, a linearizable read skips
step 1 entirely and enters `AwaitApplied` directly — **zero probe traffic**.

Critically, this is a **fast path only**: whenever the lease is not currently valid,
reads fall through to the (batched, from Rung A) probe path. The proven barrier
remains the safety floor; the clock assumption buys latency/throughput, never
correctness-of-last-resort.

### Lease derivation — reuse the quorum order-statistic

UC already computes a quorum order-statistic over follower **durable positions**
for flow control. The lease is the *same order-statistic shape over follower ack
timestamps*:

- Each time a follower ack lands (already processed by the sender/consensus path),
  stamp it with the leader's local monotonic clock (`now_ns()`, `node.rs:1693`,
  off `Instant::now()` — already threaded through the duty cycle as
  `Tick { now_ns }`).
- `lease_expiry = (quorum-th most-recent ack time) + election_timeout − ε`, where
  `election_timeout` is `election_timeout_min_ns` (`node.rs:147`) and `ε` is a
  clock-drift budget.
- Renewal rides existing replication traffic — no new heartbeat type, no new
  round-trip.

### Safety inequality

A follower will not start a campaign until `election_timeout` after it last heard
from the leader (UC's existing election-liveness rule). So if a quorum acked at
leader-times `{t_i}`, no new leader can be elected before
`min over quorum(t_i) + election_timeout` **by the followers' clocks**. The `−ε`
slack absorbs leader-vs-follower drift. While `now_ns() < lease_expiry`, the leader
is provably the unique current-term leader ⇒ it may skip the probe. Provisioning
requirement: `election_timeout >= lease_duration + ε`.

### Read-path splice

In `drain_query_ring` (`node.rs:1956-2031`), for a linearizable read:

```
if !can_serve()          -> MSG_V2_NOT_LEADER        (unchanged)
else if lease.valid_now() -> push PendingRead directly in AwaitApplied,
                             commit_at = commit.load_acquire()
                             // no nonce, no send_read_probe, no AwaitQuorum
else                      -> Rung-A batched-probe path (the fallback)
```

`advance_pending_reads` needs **no change** for lease reads — an `AwaitApplied`
read already does the `service_applied >= commit_at` wait and the epoch bracket.

### Invalidation rules (the correctness-critical part)

- **New term:** on `become_leader`, the lease starts **empty**. A lease is never
  inherited across terms; the first reads in a new term take the probe path until a
  quorum has acked *in the new term*.
- **M7 config change voids the lease.** The lease order-statistic is over the voter
  set (`self.peers`). A promote/demote/remove mutates that set and could let a
  resized quorum elect a new leader before an old lease expires. Any `config.state`
  transition that changes voters **must void the current lease** and force a
  re-probe. This is a direct hook into the M7 reconfiguration machinery.
- **Deposed-leader stop rule:** the instant `can_serve()` goes false *or* the lease
  expires, the fast path stops; pending lease-reads drop to `send_retry`
  (`node.rs:2058`). Never a stale answer.

### Cost & risk

- **New machinery:** per-follower ack timestamps in the sender/consensus state; a
  lease recompute on each quorum-ack event; a lease-validity check on the read hot
  path.
- **The clock is already present** for liveness — the novelty is that read
  *safety* now leans on bounded drift (§5).
- Marginal gain over Rung A is only the residual `O(cycles · n)` probe traffic.
  On a LAN with a tight probe cadence this may be small — measure first.

---

## 5. Correctness / proof delta

- **Rung A: none.** It is the existing barrier rule applied set-wise. The Lean model
  and the sim invariants are untouched.

- **Rung B: introduces a bounded-clock-drift assumption into linearizable-read
  safety.** Today UC's safety — including this read barrier — holds under arbitrary
  clock behavior; clocks drive only liveness. A time-based lease makes read safety
  conditional on `|drift| < ε`. This is the whole reason B is sequenced *after* the
  outstanding verification work (leader-completeness closure et al.) rather than
  before it.

  The **fast-path-only framing is what keeps this palatable**: the lease is modeled
  as a refinement whose *failure* degrades to the proven probe path, so the
  clock-drift assumption bounds a *latency* optimization, not the safety floor. In
  the Lean model, the lease-read would be admissible only under an explicit
  bounded-drift hypothesis, and the model must continue to carry the probe path as
  the unconditional guarantee. The interaction with the election-coherence-window
  work (Veil spike) should be checked before B is built.

---

## 6. When it's worth building / open questions

1. **Measure before B (arguably before A).** Is per-read probe traffic actually a
   throughput bottleneck at target read rates, or are the single-writer agents
   bottlenecked elsewhere (apply frontier, egress broadcast)? A probe-datagram /
   agent-CPU profile under a read-heavy load decides whether either rung pays.
2. **`ε` budget on a LAN.** What drift bound is defensible for the target
   deployment (same rack, PTP vs NTP)? This sets `lease_duration` and the
   `election_timeout` provisioning. If `ε` can't be bounded operationally, B is off
   the table and A is the whole answer.
3. **Probe cadence for A.** One probe per duty cycle vs. per-burst; the tradeoff
   between residual latency and datagram reduction. Cheap to tune empirically.
4. **Interaction with `READ_BARRIER_TIMEOUT_NS`.** Batched/leased reads must still
   honor the 1 s deadline and the `can_serve` drop; confirm no pending-read starves
   across probe rounds.
5. **Read concurrency in practice.** A's win scales with in-flight read
   concurrency; confirm the client/workload actually issues reads concurrently
   rather than serially per connection.

---

## 7. Out of scope

- **Cross-region / WAN read latency** and the async cross-region learner / Standby
  story. Leases are *transformative* there (the probe is a full WAN RTT), but that
  is a different motivation with a different cost/benefit and is not this brief.
- **DR failover** and any stale-read / bounded-staleness read mode.
- **Any weakening of the linearizable guarantee.** Both rungs preserve
  linearizability exactly; B conditions it on bounded drift only as a fast path over
  the unconditional barrier.
- **Epoch/liveness-based leases (option C).** Considered and declined for the LAN
  goal; would be revisited only if the WAN motivation became primary.

---

## Appendix — code anchors

| Concern | Location |
| --- | --- |
| Admission fork (linearizable vs snapshot) | `uc2_node/src/node.rs:1956` |
| Capture read index `commit_at` | `node.rs:1983` |
| Send probe (stamped with current term) | `node.rs:1895-1903` |
| Follower ack teeth (term match) | `node.rs:1910-1913` |
| Count distinct voter acks to quorum | `node.rs:1924-1943` |
| `PendingRead` / `ReadPhase` state | `node.rs:204-235` |
| Wait-for-frontier + epoch bracket | `node.rs:2038-2117` |
| Service publishes `service_applied` | `uc2_service/src/apply.rs:186` |
| Service-epoch backstop | `uc2_service/src/apply.rs:316-344` |
| Monotonic clock / duty-cycle tick | `node.rs:1693`, `node.rs:1305-1306` |
| Election timeout config | `node.rs:147-148` |
| Read-barrier timeout (1 s) | `node.rs:191` |
| Capstone test | `uc2_node/tests/query_barrier.rs` |
