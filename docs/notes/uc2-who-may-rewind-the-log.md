# Who is allowed to rewind the log?

*Written 2026-07-30, after issue #6 — the nightly `elle_partition` archive
fail-stop. Plain-language companion to the resolution entry in
`docs/benchmarks/uc2-m7-gate-2026-07-13.md`.*

## The shape of the bug

UC's log is a byte stream. Positions only ever go **up** — except twice, when
the node discovers that some suffix of what it wrote was never real:

- **Reconciliation truncation.** A follower learns its tail diverges from the
  leader's and cuts it off.
- **Leader-open collapse.** A node wins an election and throws away its own
  unreplicated tail, so its stream restarts cleanly at its durable frontier.

Both are the same physical act: pick a position `to`, drop everything above it,
and re-point the counters there. UC has always done the first one correctly and
the second one wrongly, and the difference is *which thread does the pointing*.

## Why one thread matters so much

The archive agent walks the log frame by frame. It keeps a private cursor —
`durable_pos`, the position it will read next — and every cycle it asks the
buffer for "whole frames between my cursor and the append counter."

That question only has a sensible answer if the cursor sits on a **frame
boundary**. A frame's first four bytes are its length; land one byte off and you
read somebody's payload as a length and walk into nonsense.

Reconciliation truncation preserved that property for free, because the cut ran
*on the archive's own thread*: the archive cut its journal, moved its own cursor
to `to`, re-primed the counters, and only then went back to walking. Cursor and
bytes moved together, because nothing else was running in between.

The leader-open collapse did the same job from the **consensus** thread, with a
bare `counters().prime(base)`. It never told the archive. And that is the whole
bug, because of one extra detail:

**`base` is stale.** It comes from `ElectionSm::durable`, which the consensus
agent refreshes in step 2 of its duty cycle — but an election is won during the
vote drain in step 1. So `base` is a snapshot of the durable frontier from up to
a full cycle ago. In that cycle the archive may have fsynced another block and
pushed its cursor *past* `base`.

Now the new leader starts appending from `base`, laying down frames with
completely different lengths than the old term's. The archive wakes up, resumes
at its own cursor — which is now sitting in the middle of one of those new
frames — and reads a payload byte as a length:

```
RecorderCorrupt { from: 143520, append: 143552, end: 0, claimed_len: 3120235264 }
```

`end: 0` means it failed on the very first word. `claimed_len` of ~2.9 GiB is
just four bytes of somebody's value.

## The part that isn't a crash

The panic is the *lucky* half. It is a fail-stop that a previous investigation
added precisely because this had been seen once and never explained — so the
node dies loudly instead of recording garbage.

The unlucky half is silent. Because leader open never went through the archive,
**the journal was never cut at `base` either**. So the span between `base` and
wherever the archive's cursor had reached ends up rewritten in the buffer by the
new leader, but never re-recorded — the journal still holds the *old* term's
bytes for a range the buffer now holds *new* bytes for. A follower that falls far
enough behind is served from the journal. That is the real hazard; the crash was
just the tripwire that led us to it.

## An open question this left behind — since investigated, and it was a real bug

*Resolved 2026-07-30, after this note was first written. The section below is the
original open question; the answer follows it. Both are kept because the shape of
the wrong guess is instructive.*



If `base` is a stale durable sample, the collapse discards bytes this node had
already made durable. Were any of them **committed**? A new leader whose log is
missing committed entries is a leader-completeness violation, and it would go on
to reuse those byte positions for different commands.

The tempting argument for safety is that the election protects us: the candidate
advertised `last_durable` in its `RequestVote` from the *same* `ElectionSm::durable`
field `base` comes from, voters apply the lexicographic `(last_term, last_durable)`
rule, so winning means a majority was no further along than the advertised figure —
hence `commit <= base`.

**That argument does not hold.** The durable position this node *reports to the
leader* (the receiver agent's `AppendPosition`) is read straight from the buffer
counters on the receiver's own thread. `ElectionSm::durable` is a separate
absorption of the same counter, one duty cycle behind. So a leader can commit a
position this node genuinely reported while this node's own election state still
names something lower — and then `commit > base` when it wins.

That is a *different* bug from the one this note is about, in a different plane
(vote credentials vs. the archive cursor), and it is not proven reachable — only
un-excluded, because the obvious safety argument fails. It predates the fix
described below and is unaffected by it. Tracked as a follow-up on issue #6.

### The answer: real, and I had the location wrong

It is reachable, and the demonstration is deterministic. But the framing above
puts the defect in the wrong place, which is worth correcting because the wrong
version is the more natural one to reach for.

**The collapse is not the bug.** A stale-low `base` is *conservative* for the
candidate — it makes it look less caught-up than it is. The bug is in the
**grant**. `log_ok` compares a candidate against `ElectionSm::durable`, and Raft's
vote rule is sound only if a voter compares against everything the voter has
durably stored. The shared counter is that. An absorbed copy of it, taken up to a
duty cycle earlier, is not. Granting on an under-estimate of your own log is the
unsafe direction: it lets a candidate that is behind a committed position collect
your vote. The collapse then merely executes the loss that the grant authorised.

The full chain, three nodes all in term 2:

1. B's archive fsyncs to 1000. B's *receiver* agent reports 1000 to leader A on
   its own thread. B's `ElectionSm.durable` still says 900 — the consensus agent
   drains network events before it polls the counter.
2. A (own durable 1000) ranks B's report and commits 1000. The client is acked.
3. C, honestly at 900, campaigns advertising 900.
4. A refuses. **B grants** — `(2,900) >= (2,900)` is a tie, and `log_ok_order`
   grants on `>=`. B compared against its stale self-view, not the 1000 it had
   already reported.
5. C wins on B's vote alone and collapses to 900. The acked write is gone.

**Fixed** by re-absorbing the counter immediately before the grant decision, and
at the top of the duty cycle so that candidates advertise on the same footing —
fixing only the grant side would leave candidates systematically
under-advertising against voters who compare fresh, losing elections for no
reason. Regression test
`a_vote_is_refused_against_a_fresh_read_of_our_own_log`, red-verified.

### Why nothing caught it, which is the more interesting half

Four layers of correctness machinery, and the bug was invisible to all of them —
not by bad luck, but because each one collapses the very distinction that carries
it.

**The simulator** advances a node's durable and feeds `DurableAdvanced` into the
state machine as consecutive statements in one archive event, and derives the
follower's commit report from that same value. Its scheduler only interleaves at
event boundaries, and it has no consensus-agent event at all — so "the receiver
reads the counter, the consensus agent reads it a cycle later" has no counterpart
to schedule apart. Its invariants are perfectly adequate: `committed-never-truncated`
and `leader completeness` are exactly Raft State-Machine-Safety and are fed real
node state. The oracles would have convicted. The world simply cannot produce the
trace.

**The Lean model** does the same collapse, and there it is sharper. `PNode.durable`
is one number playing all four roles — reported, compared, advertised, collapsed
to. The lemma that a reported position is at most the reporter's durable is
discharged *by reflexivity*, because in the model they are the same term. That
lemma is then composed with `log_ok` to derive precisely the informal safety
argument the bug refutes. A `leader_completeness` proof completed over that model
would be completed over a model that assumes the bug away.

And the model had already learned this lesson once. `dataTerm` exists as a
separate field because collapsing the node's *term handle* into `currentTerm` hid
a real hole. The durable counter has exactly the same shape — one model field
standing for two independently-read node-level values — and had not been split.

**The conformance harness** drives three pure kernels and never builds a state
machine or an event sequence, so it operates entirely below the level at which
the behaviour exists. It covers the `900 >= 900` tie thoroughly; the gap is
*which* durable gets passed in.

The generalizable lesson is the same one as the archive cursor, one level up.
There, the mistake was reasoning about one of five writers of a counter. Here, it
is modelling two readers of a counter as one. **A concurrent system's bugs live
in the distinctions its model erases** — so when a model collapses two things
into one, that collapse is a proof obligation, not a simplification.

## The fix, and the rule it encodes

Leader open now sends `ArchiveCmd::Collapse { epoch, to }` and finishes on the
ack — the same emit-and-wait shape reconciliation has always used. The archive
does the cut, moves its own cursor, cuts the journal, primes the counters, and
bumps the generation counter the receiver watches. Only then does the new leader
build its appender and write its NewTerm frame.

Splitting the open across two duty cycles is not free, and the bill came due in
review. The node's consensus agent polls the durable counter every cycle and
feeds it to the election state machine, which keeps a *monotonic max*. During the
new in-flight window that counter still holds the archive's uncollapsed frontier
— which, by the whole premise of this bug, sits **above** the position we are
collapsing to. So the state machine would latch a durable frontier covering bytes
that were about to be cut away, and nothing would ever bring it back down: the
downward clamp lives on the reconciliation path's `Truncated` event, which a
collapse does not produce.

That is not cosmetic. That number is the node's `last_durable` vote credential,
and it is the node's own contribution to the commit quorum — so an inflated one
lets a leader certify a commit at a position it does not physically hold. A
phantom commit, introduced by the fix for a corruption. The remedy is small: skip
the durable poll entirely while a leader open is pending. The collapse target
*is* the state machine's durable, so skipping leaves it exactly where it already
was.

The lesson generalizes past this bug: when you convert a synchronous step into an
emit-and-wait, audit every periodic poll that runs in the gap. The old code was
correct partly by accident — the prime happened before the poll could observe
anything else.

The rule underneath is worth stating plainly, because it is easy to violate
again:

> **Only the archive agent may move the log counters backward.** Anyone else who
> needs a rewind must ask it to, and wait.

Not "backward moves must be careful". Not "guard it with a flag". The archive is
the only party that knows where its own cursor is, so it is the only party that
can move both at once.

## Why the earlier audit missed it

The post-M7 investigation checked exactly this hypothesis — "H2, the truncation
seam" — and concluded it was sound, with this reasoning:

> the archive agent serializes `truncate_to`+`prime` against its own `do_work` on
> one thread

Every word of that is true. It is also about `ArchiveCmd::Truncate`, and there
were *two* primers. The audit enumerated the call site it was looking at rather
than every writer of the thing being protected.

The same gap explains why the dedicated stress harness (`archive_stress.rs`)
never reproduced it across ~1800 stress-seconds: it has appender, truncate, and
reopen arms, all correct, and no leader-open collapse from a second thread. A
harness can only find races between the actors you gave it.

The generalizable lesson is a grep, not an insight: when a safety argument says
"X is the only writer of Y", the argument is only as good as an exhaustive search
for writers of Y. `prime(` had five call sites; the audit reasoned about one.
