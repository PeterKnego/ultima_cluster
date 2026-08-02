# Two framings, one position

*Plain-language explainer for the follower-side archive fail-stop found on
unmutated `main`, 2026-08-02. Companion to
[`uc2-who-may-rewind-the-log.md`](uc2-who-may-rewind-the-log.md), which covers a
different plane: who may move the log COUNTERS backward. This one is about who
may write the log BYTES, and what a reader is entitled to assume about them.*

## The symptom

A node's archive agent fail-stops:

```
archive fail-stop: RecorderCorrupt(RecordableCorrupt {
    from: 2034432, append: 2035648, end: 1184, claimed_len: 1023098624 })
```

`recordable_slice` walks frames across `[from, append)` — everything the node
has published — and 1184 bytes in it reads a length word of about a gigabyte.
There is no torn write, no memory-ordering bug, and no corrupt datagram. Every
byte in that region was written exactly as some leader sent it.

## What a position means, and what it doesn't

UC addresses the log by absolute byte position. A follower's receiver tracks
which byte RANGES have landed (`Rebuilt`) and publishes `append` to the
contiguous frontier — the point below which there are no gaps. That tracker is
deliberately blind to buffer contents; its own module doc says so:

> gap tracking over absolute positions (no reliance on buffer contents — stale
> bytes from a previous lap can hold nonzero length words, so contiguity must be
> tracked here, not scanned)

That is the right call. Scanning is exactly how you get fooled by a previous
lap's leftovers. But it carries a hidden premise: **that a position's framing is
a property of the position.** Range arithmetic is only sound if "bytes 192
through 288 arrived" means the same thing every time it is said.

Across a term boundary it does not. A leader opens its term by collapsing its
log to a base and writing a 32-byte NewTerm frame there. The previous term may
have had a 96-byte data frame at that same base. Both statements are true of
position 192:

- term T: "192 begins a 96-byte frame, so 192..288 arrived"
- term T+1: "192 begins a 32-byte frame, so 192..224 arrived"

## How the two get combined

Two rules meet badly:

1. **The accept rule** rejects rewrites at-or-below the contiguous frontier
   (`position < contiguous` → drop as duplicate). Positions ABOVE the frontier —
   data held out-of-order behind a gap — are writable more than once.
2. **`Rebuilt::insert` unions overlapping spans.** It has no way to notice that
   the second claim contradicts the first, because it never looks at bytes.

So a follower that is holding term T's 96-byte frame at 192 behind a gap, and
then receives term T+1's 32-byte NewTerm at 192, ends up with:

- **in the buffer:** a 32-byte NewTerm (the later `write_run` overwrote the
  head), followed by 32 bytes of the dead frame's payload, orphaned
- **in the tracker:** one range, 192..288, the union

When the gap ahead of it fills, the frontier absorbs that union and `append` is
published over a span **the buffer no longer tiles with whole frames**. The
orphaned payload sits exactly where the next length word should be.

## Why the archive is the one that dies

Nobody else walks frames across that region. Clients read by position through
the service; NAK retransmits serve ranges the sender frames from its own log.
The archive is the only reader that must tile the whole span to record it — so
it is the only one that notices, and it fail-stops rather than record a block it
cannot parse.

**The fail-stop is the lucky outcome.** The orphaned bytes are payload, and
payload is arbitrary. Had that word happened to look like a plausible frame
length, the archive would have recorded old-term bytes into the journal as
current-term data — and the journal is what serves deep-NAK replay to a follower
that has fallen behind. Silent divergence, propagated on request. A node that
kills itself is the good case.

## The fix, part one

`FollowerReceiver::discard_ooo_on_term_change`. The receiver remembers which
term its out-of-order runs were accepted under; when the term moves, they are
dropped and re-requested. Positions below the frontier are untouched — those
bytes were already published, and if the new term needs them cut, that arrives
as a reconciliation truncation, which has always been handled.

Cheap: one `u32` compare per accepted datagram, and the discard itself only runs
when out-of-order state actually exists, which the in-order steady state never
has.

## …and part two, which the first fix exposed

Shipping only the above and re-running the hunt still reproduced it — a useful
reminder that a mechanism you can demonstrate in a unit test is not necessarily
the *only* mechanism producing a field signature. The second failure had a
different fingerprint: `sent == durable == from`, a NewTerm frame sitting at
`from`, and `append` claiming ~88 KB where only ~1.3 KB was really framed. All
three counters equal is the signature of `LogCounters::prime` — a collapse or a
truncation.

The receiver rebases its tracker when it sees a prime, in
`resync_after_truncation`, which ran **once at the top of each duty cycle**. A
cycle drains up to 64 datagrams, and the archive primes on its own thread. So a
prime landing *between two datagrams of one drain* is invisible to it: the
top-of-cycle check has already passed. It is equally invisible to the
`prime_generation` straddle guard, which catches a prime overlapping a *single*
datagram's processing — here the prime completes before the next datagram's
generation sample is even taken.

Every datagram after the prime in that drain then publishes the pre-prime
`rebuilt.contiguous()` over the freshly primed floor. Same end state as part
one, reached from the other side: `append` covering bytes this term never wrote.

The fix is to run the resync per datagram rather than per cycle. One acquire
load, and the condition is false in steady state — including on a leader, whose
appender legitimately runs ahead of its own receiver's tracker, which is exactly
why the check is `append < contiguous` and not `!=`.

## …and part three, which is the one that mattered

Parts one and two were both real, both red/green tested — and the field hunt
still reproduced at the same rate. That is worth dwelling on: **a mechanism you
can demonstrate is not evidence that it is THE mechanism.** Two unit tests going
green proved two holes were closed, and nothing more.

Getting the third required admitting that the instrument was the problem. The
per-event trace that cracked part one now suppressed the race completely — 0
hits in 36 traced runs against 3 in 14 untraced. A shared `SEQ.fetch_add` per
append is a contended RMW on the hot path; making the ring per-thread was not
enough either. So the tracing was removed entirely and replaced with **zero-cost
forensics**: the post-mortem, which runs once at the failure, walks the frames
itself. Every frame header carries its term, so the buffer records who wrote
what without any runtime cost at all.

That produced the answer on the first hit:

```
frame walk from `from`:  15 frames, all term 45, tiling 928 B
forward scan past the failure: pos=5640544 len=37 type=1 term=1
counters: append=5740608  durable=sent=5639584
```

`append` claimed 101,024 bytes; 928 were written. The frames beyond the failure
were **term 1** — ring content from a previous lap, never written in this
generation. The appender cannot produce that (it would have left term-45
frames), so the receiver published it, *with both earlier fixes in place*.

The window was the placement of part two's resync: it runs a few instructions
BEFORE `gen0` is sampled. A prime landing in that gap is already reflected in
`gen0`, so the generation recheck finds nothing to reject, and the pre-prime
frontier is stored over the fresh floor. Instructions apart — and a 4-core box
running twelve busy-spin agents preempts there happily.

The fix does not try to catch the prime at all:

> The receiver records the frontier it last published. If the live counter is
> found BELOW that, a prime has intervened — drop the publish.

Only a prime moves the counter backward, and after one the tracker must be
rebased before publishing. Checking that in the same breath as the store leaves
no window, because it is not a race against the prime — it is an observation of
its result.

## Where the guards sit now

| Prime lands… | Caught by |
| --- | --- |
| before the duty cycle | top-of-cycle `resync_after_truncation` |
| between two datagrams of one drain | per-datagram resync (part two) |
| between the resync and `gen0` | publish-time backward guard (part three) |
| during the datagram | `prime_generation` recheck (M6 Task 9) |

And, orthogonally, a term change discards out-of-order runs (part one).

## Why the existing defence did not cover it

`resync_after_truncation` performs exactly this invalidation — but it fires on
`append < contiguous`, the signature of a local prime driving the counter
backward. In this failure nothing regresses locally: the node's own log is
untouched, and it is the *stream above the frontier* that gets re-framed. The
resync is looking for the wrong event, and its own doc comment explains why it
must be narrow ("a leader's own append legitimately runs AHEAD of its receiver's
tracker, so a `!=` test would misfire on every leader cycle").

So the two invalidations are siblings, keyed on different evidence: one on the
counter regressing, one on the term moving.

## The lesson, which is not new

Issue #6 was a safety argument that reasoned about one of five *writers* of a
counter. Issue #7 is two *readers* of a counter modelled as one. This one is a
*position* treated as carrying a single framing when it carries one per term.

Each time, the defect lived in a distinction the design had collapsed — and each
time the collapse looked like a simplification rather than a proof obligation.
`Rebuilt`'s blindness to buffer contents is still the right design. What was
missing is that the ranges it stores are only comparable within a term, which
makes the term part of the range's identity.
