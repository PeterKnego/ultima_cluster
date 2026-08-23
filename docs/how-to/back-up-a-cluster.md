# How to back up a cluster

Take a filesystem copy of a node's durable state while it keeps running, prove
the copy is restorable before you trust it, and restore it onto a fresh host.

This is the routine-maintenance half of survivability. If you have already
lost a majority of voters and are looking for the procedure that gets a
cluster serving again with data loss stated up front, that is
[Recover from quorum loss](recover-from-quorum-loss.md) instead — read
[what a backup is not](#what-a-backup-is-not) at the bottom of this page
before you decide which one you need.

## What gets copied

`uc2ctl backup` copies exactly the node's **durable** subdirectories —
`journal/`, `state/`, `snapshots/` — into a fresh artifact directory. It never
touches `cnc2.dat`, `log.buf`, the ring files, or `instance.lock`: those are
volatile, and a node's next boot recreates them unconditionally regardless of
what a backup or restore did. **`audit.jsonl` is not part of the artifact
either** — the admin audit trail is node-local and append-only, it is not
state a restore reconstructs from, and a restored node starts a fresh one.
Copy it separately (a plain `cp`, any time — it is append-only) if you want
to keep the trail. See
[Instance directory](../reference/instance-directory.md#durability-classes)
for the full durable/volatile split.

The node **may be running throughout**. There is no quiesce step, no admission
hold, nothing to coordinate with your service or clients.

```bash
uc2ctl backup --instance-dir /srv/uc2/n1 --out /srv/uc2-backups/n1-2026-08-20
```

Output is `key=value` lines — the recovered positions of the artifact you just
made, not of the live node (a backup taken under load is, by design, short of
the source's current frontier):

```
journal_first_base=0
journal_last_pos=5242880
newest_snapshot=none
snapshot_floor=0
healed_torn_tail=true
files=3
```

(`healed_torn_tail=true` here is the ordinary case, not a sign of trouble —
see the note under "Verify before you trust it" below.)

`backup` refuses an `--out` that already exists and is non-empty
(`ArtifactExists`) rather than merging into it — always point it at a fresh
directory.

## The ordering rule, and why it matters

The three directories copy in one fixed order, one fully before the next
starts: **`journal/` → `state/` → `snapshots/`**. This is not incidental — it
is the entire correctness argument for taking a backup while purge is running
concurrently underneath it:

> first_base only advances (purge), the newest snapshot position only
> advances (publish is atomic, retention keeps the newest 2, and purge only
> runs below a durably persisted floor that some retained snapshot covers) —
> so a snapshot copied AFTER the journal always covers any purge that
> happened BEFORE the journal copy. The reverse order can capture a snapshot
> set from before a purge that the journal copy then reflects: a hole.

Get the order backwards — copy `snapshots/` first, then let a purge run, then
copy `journal/` — and you can build an artifact whose journal has already lost
the prefix that only the *old* snapshot set covered. `uc2ctl verify-backup`
checks for exactly this (the "coverage invariant," below) on every artifact,
not only ones `uc2ctl backup` produced, so a hand-assembled or wrong-order copy
is caught rather than silently trusted.

## Under load, safely — including against a racing purge

Taking a backup while the node is live and purging means the copy loop can see
a source file vanish out from under it mid-copy: purge unlinks a contiguous
run of old journal segments, and snapshot retention (keep-newest-2) unlinks an
old snapshot, both while `backup` is still listing or copying that same
directory.

`backup` handles this by retrying the **whole directory**, not by skipping the
one file that vanished. A per-file skip is unsafe: purge's removal loop and
the copy loop are two independent, unsynchronized passes over the same
directory, so a file mid-run can vanish while an earlier name (already copied)
and a later name (not yet reached) both survive — a copy with a gap in the
middle of the journal that no later check can detect. A whole-directory retry
instead guarantees whatever set of files a given attempt succeeds in copying
is exactly what the source looked like at some single instant, never a splice
of two different instants. Retries are bounded (five attempts); exhausting
them is a named `Io` error pointing at the directory, because purge/retention
cadence tracks snapshot cadence — orders of magnitude slower than copying a
handful of files — so a real race resolves within a retry or two, and running
out points at something else being wrong.

The upshot: **run `uc2ctl backup` against a live, loaded node with no
caveats beyond the next section.** It has been raced against a purge
deliberately and repeatedly in the test suite, and every resulting artifact
verified clean.

## Verify before you trust it

A backup you have not verified is a directory, not a backup. Verification is
read-only except for one permitted heal. `verify-backup` (and `restore`, for
its artifact argument) refuse a path that looks like a **running** node's
instance directory — a stopped node's own directory verifies fine in place,
but the heal above is a real write, and racing it against a live node's own
writer is exactly the hazard these tools must not create.

```bash
uc2ctl verify-backup /srv/uc2-backups/n1-2026-08-20
```

Verify:

1. Opens the artifact's journal, which may heal a torn active-segment
   tail — a copy taken mid-append looks exactly like a crash, and heals the
   same way a restart does (a physical shrink-only truncate, never a grow).
   This is the **one** mutation verify is allowed to make, and it is always
   reported as `healed_torn_tail=true`, never hidden. Seeing
   `healed_torn_tail=true` is **routine, not alarming**: with segment
   preallocation on (the default), a node's active segment file is
   physically larger than the position it has actually recovered to — the
   zero-filled preallocated tail beyond the real data — so a backup taken of
   a perfectly healthy, cleanly-running node reports `true` just as often as
   one taken genuinely mid-crash-equivalent append. It is not, by itself, a
   sign anything went wrong.
2. Opens all five `state/*.state` files read-only.
3. Lists `snapshots/snap-*.ultsnap` and finds the newest.
4. Checks the **coverage invariant**: if the journal's `first_base` is above
   zero, some retained snapshot must cover it (`newest_snapshot >=
   first_base`). If not, the artifact is unsafe to restore from — an install
   from it would have a hole below the snapshot floor with nothing to fill
   it — and `verify-backup` fails with `Hole`.
5. If a `MANIFEST` file is present (every `uc2ctl backup` artifact writes
   one), cross-checks its recorded positions against what verify just
   recovered — catching tampering or bitrot at the metadata level. A
   mismatch fails with `ManifestMismatch`.

Beyond the one permitted heal, `verify-backup` never writes anything, and a
second run against an already-clean artifact never changes a file's size
again.

## Restore onto a new host

```bash
uc2ctl restore /srv/uc2-backups/n1-2026-08-20 --instance-dir /srv/uc2/n1-new
```

Restore runs `verify-backup` on the artifact **first** — a hole, a manifest
mismatch, a corrupt artifact, or the artifact looking like a running node's
own instance dir aborts before anything is copied — then refuses if the
target's `journal/`, `state/`, or `snapshots/` already contain anything
(`TargetNotEmpty`): restore never merges or overwrites. Volatile leftovers in
the target (a stale `cnc2.dat`, ring files) are fine and left alone; boot
recreates them regardless. The target itself is also refused if a node is
currently running in it — cheap insurance for the narrow window right after
`InstanceDir` is acquired, before a fresh boot has written anything, where
`journal/`/`state/`/`snapshots/` are still empty and `TargetNotEmpty` alone
would not catch it.

Once restored, start a node against `--instance-dir` as normal. First boot
does everything else: a fresh `cnc2.dat` and `instance_id`, config/vote/term
recovery from the copied `state/`, and — if this id is a minority of a still-
healthy quorum — rejoin and repair over the ordinary replication path.

## The minority-restore rule

**Restoring at most a minority of voters against a live majority is safe:**
the healthy quorum repairs or neutralizes any log/vote rollback the restored
node brings back. A restored voter's stale vote can only matter in a term
where the healthy majority has already moved on; its granted-alone vote
certifies nothing at a 2-of-3 quorum.

**Restoring a majority is a different operation** — you are not repairing a
minority against a still-live truth, you are re-establishing what truth *is*.
That is the domain of [Recover from quorum loss](recover-from-quorum-loss.md),
and it carries its own data-loss statement. Do not treat a majority restore as
"the same thing, just more nodes" — it is not silently equivalent, and this
page's soundness argument does not cover it.

## The `MANIFEST` file

Every `uc2ctl backup` artifact carries a hand-formatted `key=value` file at
its root, `MANIFEST`, not copied into a restored instance directory (it is
metadata about the artifact, not part of a node's layout — boot never looks
for it):

| Key | Meaning |
|---|---|
| `format` | `uc2-backup-v1` |
| `journal_first_base` | lowest position still covered by the artifact's journal; `0` = unpurged/empty |
| `journal_last_pos` | the artifact's recovered durable frontier |
| `newest_snapshot` | highest `snap-<pos>.ultsnap` position found, or `none` |
| `snapshot_floor` | the durably-persisted snapshot floor from `state/snapshot.state` |
| `healed_torn_tail` | whether making this artifact healed a torn active-segment tail |
| `created_unix_ns` | wall-clock creation time |

## What a backup is not

- **Not a substitute for a healthy quorum.** A backup lets you restore a
  *minority* member safely; it says nothing about what to do when the
  majority itself is gone. See
  [Recover from quorum loss](recover-from-quorum-loss.md).
- **Not something to script blind trust into.** Always `verify-backup` before
  you rely on an artifact, even one your own automation produced —
  verification is cheap, read-only beyond the one permitted heal, and the
  whole point of the coverage invariant is that a backup can look complete
  and still have a hole.

## Where to go next

- [Recover from quorum loss](recover-from-quorum-loss.md) — when a majority of
  voters is gone, not just one.
- [Instance directory](../reference/instance-directory.md) — the durable/
  volatile split these three directories rely on.
- [`uc2ctl`](../reference/uc2ctl.md#offline-commands) — full flag reference
  for `backup`, `verify-backup`, `restore`.
