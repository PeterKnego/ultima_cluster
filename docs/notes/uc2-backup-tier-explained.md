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
```

`SET` goes in last. A set without it is not a set. A reader that finds `SET`
can rely on every file it names being there. That is how a crash halfway
through a copy, or a deletion halfway through, never shows as a good set.

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

## The watermark

The writer tells the cluster how far the tier reaches, and the leader commits
that as a record in the cluster's own state: `oldest_set`, `newest_set` and
`through`. Because it is committed, every node answers "how far are we
backed up?" the same way, and `uc2_backup_through` agrees on all of them.

A tier can have several writers. They each report. The cluster keeps the
furthest claim for each field, and the oldest never passes the newest.

The watermark is also what the purge hold reads. The writer's node holds its
own purge back to the end of the chain it last confirmed, using its own
record, which is current. The replicated one lags.

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

**Do not trust a lagging view.** Retention protects the rollback point of an
unfinished upgrade pin. A node whose view had not yet seen the pin would not
protect it. So retention runs only once the writer's view has consumed past the
tier's newest set.

**Byte-exact, except for padding.** Chunks copy the journal exactly. Padding is
the one exception, because it travels as a header with no body: each node
fills the gap with whatever its own buffer held. Two writers on one tier would
fail the overlap check over bytes that mean nothing. So padding bodies are
zeroed.

**Say what is not done.** A replacement node seeded from the tier looks easy
and is not. Its lineage would start at the set, and a live leader cannot find
a common point with it. Joining from peers already works for a replacement
node, so this ships later, on its own design.
