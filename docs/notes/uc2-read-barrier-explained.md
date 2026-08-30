# The linearizable read barrier, explained

**Audience:** developers new to UC's read path, or anyone reviewing a change to
it. No prior Raft knowledge assumed.
**Status:** explanatory. The normative descriptions live in the v2 design spec
(§7) and the code (`uc_node/src/node.rs`); if this note and those disagree,
they win and this note is stale.

---

## What the read barrier is for

When a client asks the leader "what's the current value?", the leader can't just
answer from memory. It might have been deposed a moment ago — network
partitioned, another node elected — and not know it yet. If it answers anyway,
the client gets a stale value that a newer leader has already overwritten. That
is the bug the barrier prevents.

So before answering, the leader does a quick check: it asks the other nodes "am
I still the leader?" and waits for a majority to say yes. Each node only says yes
if it hasn't moved on to a newer leader. Once a majority confirms, the leader
knows it was genuinely in charge, and can safely answer.

That confirmation round is the barrier. In UC it is a `READ_PROBE` datagram to
every voting peer and a `READ_PROBE_ACK` back; the follower's "only if I haven't
moved on" test is a term comparison, and it is the teeth of the whole thing — a
deposed leader can never collect the quorum, so it can never certify a read.

## What it costs

Measured on a 3-host AWS fleet (`docs/benchmarks/uc2-read-profile-2026-07-26.md`):
linearizable reads sustained 244,052/s against 585,414/s for the same reads with
the barrier removed. **The barrier costs roughly 58% of read capacity.**

The reason is that today *every single read does its own check*. A thousand reads
arriving together fire a thousand separate "am I still leader?" rounds — asking
the same question a thousand times and getting the same answer.

## The batching idea (Rung A)

Ask once, use the answer for everyone waiting. One round of confirmation covers
the whole batch.

That is the entire idea, and it is why it needs no change to the safety story:
it is the same check, just not repeated pointlessly. The certification rule is
the existing rule applied to a set rather than to one read.

## The subtle part: order of operations

The check has to happen **after** the read shows up. Not before. This is the one
thing to get right, and it is easy to get backwards.

Picture a timeline:

```
10:00:00   Leader asks "am I still leader?"
10:00:01   Majority replies "yes, you are"
10:00:02   ← leader gets partitioned off, doesn't know yet
10:00:03   A read arrives
```

If the leader answers that read using the confirmation from 10:00:01, it is
answering with a *stale reassurance*. It was leader a second ago. It isn't now.
The read gets a stale value — exactly the failure the barrier exists to prevent.

### Why "compare the positions" is not enough

A tempting rule is: certify any waiting read whose position is at or below the
position the round confirmed. It is tempting because it sounds like the usual
read-index reasoning, and because positions are right there.

It does not work. In the timeline above, if nothing was written between 10:00:00
and 10:00:03, the two positions are **identical**. The comparison passes. The
stale read goes out. Position ordering cannot see a gap in *time*, and the hazard
here is a gap in time.

### The rule that does work

Certify by *who was waiting*, not by position.

When a round of confirmation goes out, it vouches for exactly the reads already
sitting in the queue at that moment — nothing that arrives afterward. A read that
shows up at 10:00:03 waits for the next round.

Concretely: number the rounds 1, 2, 3… Each arriving read notes which round is
next. When round 5 comes back confirmed, it releases every read that was waiting
for round 5 or earlier. Reads that arrived during round 5 wait for round 6.

The ordering requirement is then structural — built into the rule rather than
inferred from a number — and the position comparison becomes redundant, because
commit positions only move forward: a read that was already waiting when the
round went out necessarily has a position at or below what the round confirmed.

The cost is that a read can wait up to one extra round. Sub-millisecond on a LAN,
and latency is not what this optimization targets.

## What the barrier does NOT cover

Worth knowing, because it is a common source of confusion when reading the code:

- **Waiting for the service to catch up is a separate step.** Confirming
  leadership tells you the read's position is a valid answer point; it does not
  mean the service has applied that far yet. The read then waits for the apply
  frontier to reach its position. Two different waits, two different purposes.
- **A snapshot (non-linearizable) read skips both.** It is forwarded straight to
  the service. That is what makes it a useful experimental control — the same
  path minus the barrier — and it is how the 58% figure above was measured.
- **The barrier is not what protects against a crashed service.** That is the
  service-epoch backstop and the capture-recheck bracket, which defend a
  different race (the service restarting mid-query).

## Further reading

- Design spec §7 — the normative description of the read path.
- `docs/superpowers/specs/2026-07-24-uc2-leader-lease-design.md` — Rung A
  (batching, clock-free) and Rung B (time-based leader lease), and why B is
  sequenced behind verification work.
- `docs/benchmarks/uc2-read-profile-2026-07-26.md` — the measurement, including
  the alternatives that were ruled out (the per-cycle query drain cap, and the
  load generator itself).
