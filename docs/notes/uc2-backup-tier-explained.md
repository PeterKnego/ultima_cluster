# The backup tier, explained

This note says why the tier is built the way it is. To use it, read
[How to back up to a tier](../how-to/back-up-to-a-tier.md).

## Why not copy a node's disk?

`uc2ctl backup` copies one node's instance directory. That is a fine tool,
and it has four holes.

- Nothing records how far it reaches. After a loss, nobody can say what was lost.
- It is one node's disk. A node's journal ends with bytes that are durable
  but not yet committed. An election can throw them away. A cluster restored
  from that copy would bring them back.
- Its `state/` holds that node's private vote. Restoring every node from one
  copy clones the vote.
- It is a point. The next copy overwrites it.

So the tier does not hold a node's disk. It holds **the cluster's agreed
history**: only bytes that were committed, and only snapshot sets that every
copy agrees on. Any node can write it. The nodes' private parts, the vote and
the term, are never in it. A restore writes those fresh.

## The format

Everything in the tier is written once and never changed.

```
TIER                         who owns this tier
sets/<P>/row-<id>.ultsnap    one snapshot artifact per row at position P
sets/<P>/cluster.ultcluster  the cluster's own state at P
sets/<P>/SET                 written last; names the files above
journal/<from>-<to>.ultj     committed log bytes from..to
fence/<Q>                    a restore rolled the history back to Q
```

`SET` goes in last. A set without it is not a set. A reader that finds `SET`
can rely on every file it names being there. That is how a crash halfway
through a copy, or a deletion halfway through, never shows as a good set. A
`SET` that exists but does not parse marks its set corrupt: it is listed
apart and never used as a start, and the rest of the tier stays usable. One
bad object must not take every other set down with it.

Objects are split into 1 MiB blocks. Each block has its length and a CRC of
its raw bytes, and zstd compresses the block. The check is on the raw bytes,
so compressed and uncompressed tiers compare equal.

Two writers can write the same object. The second finds it there and
compares. If they agree it is a silent success. If they disagree the writer
stops and raises an alert. It never overwrites.

## The chain and the reach

A **chunk** holds the log from `from` to `to`. Chunks may overlap. Starting
at a set at position S, follow the chunk that covers S, then the one that
covers where that ended, and so on. That is a **chain**. Where the chain stops
is its **reach**.

You can restore to any frame end after S, up to the reach. Not to S itself:
a journal that starts at S and ends at S is empty, and an empty journal
reopens as an empty log. So the restorable range prints as `(S, reach]`.

If the tier lacks a stretch of log (the writer's node purged it before the
copy), the chain breaks. The tier starts a new chain from a later set. The old
reach stays where it was, and `uc2_backup_chain_gaps_total` counts the break.

A newer chain can also appear while some writer still holds the missing
stretch: one writer lags, and another, whose journal starts later (a learner
that joined from a snapshot), starts a chain at its own set. The lagging
writer then fills the gap first, cutting toward the newer chain's set until
the two join, and until they do it holds its node's purge at its own chain
end, not at the newer chain's end. Otherwise it would extend from the newer
chain's end, and its node would purge exactly the stretch only it could
still copy.

## The watermark

The writer tells the cluster how far the tier reaches, and the leader commits
that as a record in the cluster's own state: `oldest_set`, `newest_set` and
`through`. Because it is committed, every node answers "how far are we
backed up?" the same way, and `uc2_backup_through` agrees on all of them.

A tier can have several writers. They each report. The cluster keeps the
furthest claim for each field, and the oldest never passes the newest.

The purge hold does not read the committed watermark. The writer's node
holds its own purge back to the end of the chain the writer last confirmed in
its own listing of the tier, which is current; the replicated watermark lags.
While a gap it can fill lies below the tier's reach, that end is the lower
chain's end.

## Restore

A restore picks the newest complete set strictly below Q whose chain reaches Q.
It checks every artifact against the hashes in `SET`. It builds a journal
holding exactly `[S, Q)`, sets the snapshot floor to S, and writes the state
files fresh. There is no vote file. Each restored node comes up knowing
nothing about who it voted for, so the first election is ordinary. The cost
is stated plainly in the how-to: whatever the cluster committed after Q is
gone.

A fork is the same build, with two differences: its output progress is
set to Q, so side effects at or below Q do not happen again, and the result
is cut down to one voter under a new `app_id`.

A restore below the reach is a rollback. The restored cluster writes a new
history past Q, and the tier's `(Q, reach]` holds the old one. Every later
check — set hashes, CRCs, overlap compares — looks only at what is in the
tier, so a writer that kept appending would mix the two histories without
any check noticing, and a later restore would build state that never
existed. So the restore leaves a fence, `fence/<Q>`, and every writer
refuses a fenced target by name. Writers move to a new, empty target; the
fenced tier stays readable for further restores.

## What we learned building it

**A backup that feeds itself never goes quiet.** The writer reports to the
cluster, and the report is a log frame. The frame grows the log. The log's
growth looked like new history to back up. On an idle cluster each report made
the next cut, forever. The rule now: a span that holds only padding and
watermark frames is not worth a chunk. The same thought shaped the
alert. `Uc2BackupBehind` looks at how many bytes the tier is behind, not at
"the log moved and the tier did not", because an idle log still creeps.

The skip created the opposite fault. After a fresh set on an idle cluster,
only bookkeeping frames followed it, so nothing was restorable even though
the watermark claimed the set. So the writer makes one small cut after each
new set.

**Cut to the set.** At first the writer copied a set only when its chain had
already passed that set. With snapshot instants more frequent than chunk cuts
and a small `backup_retain_sets`, every set was deleted by the node before the
chain reached it. The tier never got a second set. Restores always replayed
from the first. Now the writer ends its next chunk exactly at the set, then
copies it. A few more small objects bought a tier that actually advances.

**Never delete what a half-written set needs.** Retention can race the copy
of a set. The writer used to write `SET` over files retention had just removed.
Now it re-lists the set's directory and confirms each file is present, at the
right size, immediately before it writes `SET`.

**Count only what was checked.** Retention decides which set is the newest,
and the keep window by count and age, over the sets the writer has verified:
read back, or confirmed by the committed watermark. An unverified newest set
never causes an older verified set to be deleted, and is never deleted itself.
It is the operator's, and `tier verify` names it.

**Do not trust a lagging view.** Retention protects the rollback point of an
unfinished upgrade pin. A node whose view had not yet seen the pin would not
protect it. So retention runs only once the writer's view has consumed past the
tier's newest set.

**Byte-exact, except for bytes nothing reads.** Chunks copy the journal
exactly, with two exceptions. A padding frame travels as a header with no
body, so each node fills the body with whatever its own buffer held. And
every frame is padded to a 32-byte boundary; those alignment bytes are not
replicated either. Two writers on one tier would fail the overlap check over
bytes that mean nothing. So padding bodies and alignment slack are both
zeroed.

**Say what is not done.** 3a shipped without a tier-seeded replacement node
because its lineage would start at the set, and a live leader cannot find a
common point with that. 3b fixes it by recording the lineage; see below.

## The tier as a holder

3a made the tier a copy that a person restores from. 3b lets the cluster lean
on it: a node can be built from it and join, purge can wait for it, and a node
can read from it instead of asking a peer.

**Why a node needs the lineage.** A node's term map says which term wrote
which stretch of the log. The node itself does not need it to run. The
*leader* needs it: when a follower reconnects, the leader compares the two
maps to find the last point they agree on (log matching), and truncates the
follower back to it. The map is a receipt the leader checks, not state the
node uses. A node restored at a set S used to have a map that began at S. The
leader's map began at 0. They share no entry, the leader finds no point of
agreement, and the node was cut to nothing. So each `SET` now records the
writer's map below S (its `lineage`, entries with `base < S`), and restore
starts the node's map from it. The two maps agree on every term below S, and
the node keeps its log.

**Why the leader carries the verdict.** A new learner has an empty catalog: it
has never seen the cluster's agreed hashes, so it cannot tell a good tier set
from a bad one. When the leader asks a below-floor follower to install from
its tier, it names the set and sends what it knows about it: each artifact's
agreed hash, version and size. The follower checks every copied object against
that. The leader's committed catalog is the authority, as it is for a snapshot
session; the tier is shared storage and is trusted for bytes, not for what
they should be. For the same reason an install asks nothing of the tier's
committed registration: the verdict is the proof.

**The gate.** Purge deletes the only local copy of the log below a node's
floor. With `purge_requires_tiers = K`, every node holds its floor at or below
the K-th highest `through` the cluster has confirmed among its tiers. The gate
is one more hold: the journal purge target is the minimum over the floor minus
slack, the writer's own hold and the gate. For that
to be safe the confirmation has to mean something, so the writer reads back
each object it creates before it counts. The price is that older history then
lives only in the tier, and its retention sets how far back a restore goes. A
node about to run out of disk lets the gate go, because a stopped node is a
worse loss than a gap in the tier, and an alert says so.

**The source.** A node with a `[tier_source]` (or a `[backup]`, which implies
one) tries the tier before peers, both for a store-only fetch and when it falls
below the leader's floor. That keeps a large copy off the nodes that are
serving. A failure never blocks: the node falls back to a peer at once. An
install that succeeded gets a short settling grace. The follower's next report
still shows the old position for a moment, and without the grace the leader
would read that as a failure and open a snapshot session for a node that had
already installed.

**One thing to know about the lineage an install adopts.** It is the tier
set's own line. The rows and the cluster artifact are hash-verified against
the leader's verdict, but the term-map line is parsed and range-checked, not
covered by a hash. That is acceptable because a wrong line cannot make the
node serve wrong data: it can only make the next reconcile cut the node
further back.
