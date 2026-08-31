# The broadcast seqlock, and the bug a model found in it

*Written 2026-08-31, after `uc_protocol/tests/loom_broadcast.rs` failed on its
first run and turned up a real defect in shipped code. This is the
plain-language version: what the seqlock is for, why it was wrong, why no test
we run could have caught it, and what the fix costs.*

## The setting: one ring with no brakes

UC has four ring buffers. Three of them have **backpressure** — the producer
watches a `consumer_position` and stops rather than overwrite bytes a reader
has not taken yet. The SPSC ring (service ↔ node) and the MPSC ring
(clients → node) both work that way.

The **Broadcast** ring does not. It is the node → clients response path, and
its whole point is that a slow client must not be able to stall the node. So
the single producer never waits. It writes, publishes, and moves on. If a
consumer is slow, the producer laps it and the consumer's bytes are gone.

That is a deliberate design choice, and it creates a problem the other three
rings do not have: **a consumer is reading memory that may be overwritten
while it reads.**

## The seqlock: copy first, ask questions after

You cannot lock — that would reintroduce the stall. The standard answer is a
*seqlock*: copy the data optimistically, then check whether the copy was
valid, and throw it away if it was not.

Broadcast's version uses `publish_position`, the producer's cursor, as the
sequence number:

```text
producer (single)                    consumer (many, each with its own head)
─────────────────                    ──────────────────────────────────────
write record bytes at pos            p1 = publish_position.load(Acquire)
publish_position.store(              if head == p1            -> nothing yet
    pos + advance, Release)          if p1 - head >= capacity -> Overwritten
                                     ...copy the record's bytes...
                                     fence(Acquire)
                                     p2 = publish_position.load(Acquire)
                                     if p2 - head >= capacity -> Overwritten
                                     otherwise: the copy is good
```

The reasoning is positional. The producer writes into
`slot = pos % capacity`, so the only way it can touch the slot a consumer is
reading at `head` is to reach `head + capacity` — a full lap. If, *after* the
copy, the producer is still less than a capacity ahead, it cannot have
started overwriting that slot, so the bytes are whole.

Written out, the property the check depends on is:

> **If lap N+1's bytes are visible to me, then `publish_position >= N+1` is
> visible to me too.**

Because the producer publishes *after* writing, that reads as obviously true.
It is not.

## Where it breaks

`Release` on a store means: *nothing that comes before this store may be
observed after it.* It is a one-way barrier. It says **nothing** about
operations that come *after* it.

So the producer's real instruction stream is:

```text
write body of record N        ─┐
publish.store(N+1, Release)   ─┘ these two are ordered
write body of record N+1      ←── this may be observed BEFORE the store above
publish.store(N+2, Release)
```

The body writes for record N+1 are ordinary stores. Nothing stops a weakly
ordered machine from making them visible to another core *before* the
`Release` store that publishes record N.

Now replay the consumer. It is parked at head 0 in a two-record ring. It reads
`publish_position` and sees 1 — fine, not lapped. It starts copying slot 0.
Meanwhile the producer races ahead and begins writing record 2, which lands in
slot 0. The consumer's copy picks up the first word from lap 0 and the second
word from lap 2. Then it re-checks `publish_position` — and still sees 1,
because the store that would have said 3 has not become visible to it yet,
even though the *bytes* that store was supposed to guard already have.

`1 - 0 = 1`, less than the capacity of 2. The check passes. **A torn record is
accepted.**

That is exactly the counterexample loom printed:

```text
accepted a read at head 0 whose words are [1, 3], expected [1; 2]
```

word 0 carrying lap 0's value, word 1 carrying lap 2's.

## Why the existing tests could not find it

`uc_protocol/src/ring/broadcast.rs` already has two tests aimed squarely at
this: `wrap_no_torn_read` and `overwrite_during_read_never_tears`. They spin
real threads, hammer the ring, and pass.

They pass because of the machine they run on. **x86 is TSO** — its memory
model forbids reordering two stores. On x86 the producer's body writes for
record N+1 simply cannot be observed before the publish of record N, so the
scenario above is unreachable, no matter how long you hammer.

**aarch64 permits it.** And `CLAUDE.md` records the relevant fact:

> aarch64 binaries are built but never executed in CI

So the bug lived in a gap between two true statements: the platform where it
can happen is built and shipped, and the platform where the tests run is the
one where it cannot happen. Stress testing was never going to close that gap.
A model checker explores the *memory model*, not the hardware, which is why it
found in one run what hammering could not find at all.

## What it would have looked like in production

The egress broadcast is the node → client response path. A torn read that
escapes the barrier reaches `try_read_record_at`'s CRC, and:

- **almost always**, the CRC fails and the caller gets `BadCrc` — a *wrong
  error*. The code's own comment says this is the thing the barrier prevents:
  "A torn read otherwise escapes as a hard `BadCrc`." The defined answer for a
  lapped reader is `Overwritten`, which callers handle by resyncing.
- **rarely** — a CRC32 collision, so on the order of one in four billion — the
  torn record passes the CRC and a client sees a corrupt response.

Not a cluster-safety bug: consensus, the log and the journal are untouched.
A client-visible correctness bug on one platform.

## The fix, and what it costs

One `Release` fence at the top of `BroadcastProducer::write`, between the
previous record's publish store and this record's body writes. A release
*fence* — unlike a release *store* — orders prior accesses against the writes
that follow it, which is the direction that was missing.

The cost, measured with `rustc --emit asm` on both targets rather than
assumed:

| target | without the fence | with the fence |
|---|---|---|
| `x86_64` | `movq`, `movq`, `retq` | `#MEMBARRIER` (a pseudo-op, **no instruction**), `movq`, `movq`, `retq` |
| `aarch64` | `str`, `stlr`, `ret` | `dmb ish`, `str`, `stlr`, `ret` |

So it is free on the platform where the bug cannot occur, and costs one
barrier on the platform where it can. That is the ideal shape for a fix like
this: you are not paying for someone else's memory model.

SPSC and MPSC need nothing — their producers cannot lap a reader at all, so
the property this fence establishes is one they never rely on.

## Keeping the model honest

A model that proves nothing also passes. `loom_broadcast.rs` therefore ships
two mutations, each removing exactly one step and each required to fail:

- **M1** removes the producer fence — the pre-fix state, and the
  counterexample above.
- **M2** removes the consumer's post-copy re-check — the pre-check alone is a
  snapshot taken *before* the copy, so it cannot say anything about what
  happened during it.

Both are `#[should_panic]`, so a green run means the model explored enough to
catch a broken protocol, not that it explored nothing.

One modelling choice is worth calling out: a "record" in the model is **two**
words, not one. With a single word, a lapped read returns some *whole* value
and the model could only ever catch a **stale** record. Two words written
separately is the smallest thing that can be **torn**, which is the actual
failure mode. The bug is invisible to a one-word model.

## The lesson worth keeping

The seqlock's argument was stated in the code and it was persuasive — publish
happens after the write, so seeing the write implies seeing the publish. It
was persuasive and wrong, because "happens after" in program order is not
"becomes visible after" on a weak machine.

That class of error is not reachable by testing on x86, and it is not
reachable by reading the code carefully either, because the code reads
correctly. It is reachable by a model checker, and essentially only by a model
checker. This is the second time the project has cashed that in — the M13 MPSC
model found the padding-marker publication path sitting outside the
exactly-one-winner guarantee, in the same way.

Still uncovered, and stated rather than implied: **SPSC, the futex layer, and
the mapping itself** have no loom model.
See [`docs/VERIFICATION.md`](../VERIFICATION.md) §6 and §11.
