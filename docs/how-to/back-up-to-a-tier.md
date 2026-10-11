# How to back up to a tier

A **backup tier** is a directory outside your nodes that holds the cluster's
agreed history: coordinated snapshot sets and the committed journal between
them. One node, the **writer**, copies both into the tier as the cluster
runs. You can then restore the whole cluster to any point the tier covers,
start a separate cluster from a point in the past (a **fork**), or cut a
diff-replay corpus from it.

This is the recommended way to take backups. The per-node tool in
[Back up a cluster](back-up-a-cluster.md) still works and is the low-level
one: it copies one node's disk by hand and records nothing about how far it
reaches. For why the tier holds the cluster's history and not a node's disk,
read [the explainer](../notes/uc2-backup-tier-explained.md).

## 1. Choose a writer

Any member can be a writer. Pick a **learner**. A learner is not counted in
any quorum, so a slow target can never slow a commit. Give it enough disk to
hold the journal it has not yet copied (see
[Purge and the writer's hold](#purge-and-the-writers-hold)).

Several nodes may share one target. They write the same bytes, so a second
writer is a spare, not a conflict. If two nodes disagree about an object
already in the tier, the writer that sees it **stops** and raises an alert.
If one writer falls behind and another, whose journal no longer reaches back
that far, starts a newer chain, the writer that still holds the missing span
fills it first, so the two chains join (and its node does not purge that
span meanwhile). The committed `oldest_set` may already have moved to the
newer chain's set and does not move back: it then understates how far back
the tier reaches, never overstates it.

The target is a directory path. It must be a mounted filesystem you trust to
keep files (a network mount, an attached volume). The writer never rewrites
an object it has written. **The filesystem must support hard links**: every
object is placed with a hard link, which is what makes "write only if
absent" atomic. SMB/CIFS and some FUSE mounts do not; on one, the writer
stops with `uc2_backup_errors_total{reason="no_hardlinks"}`
(`target_no_hardlinks` in its log).

## 2. Configure the writer

Add a `[backup]` section to the writer's `node.toml`. A node without the
section runs no writer. The section is node-local; it is not replicated.

```toml
[backup]
target = "/mnt/uc-backup"   # required: the tier's root directory
chunk_bytes = "64MiB"       # cut a journal chunk at this much new history
quiet_interval = "60s"      # ...or after this long with any uncut history
compression = "zstd"        # "zstd" (default) or "none"
compression_level = 3       # zstd level, 1 to 19
```

| Key | Default | Meaning |
|---|---|---|
| `target` | none (required) | The tier's root. First use creates the tier; later use checks it belongs to this cluster. |
| `chunk_bytes` | `64MiB` | A chunk is cut when this much committed log is uncopied. Minimum `1MiB`. |
| `quiet_interval` | `60s` | A chunk is also cut when any uncopied history is older than this. Must be above zero. Units `ms`, `s`, `m`, `h`. |
| `compression` | `"zstd"` | Compresses every object this writer writes, per 1 MiB block. Readers handle both. |
| `compression_level` | `3` | zstd level, `1..=19`. |

Restart the node. The writer starts as its own thread, `uc2-backup`. Look for
its log lines, and for `uc2_backup_bytes_written_total` to start rising.

A tier belongs to one cluster. If the target already holds a tier for another
`app_id`, the writer refuses it, writes nothing and stops
(`uc2_backup_errors_total{reason="foreign"}`).

### What the writer copies

- **Sets.** Every coordinated snapshot set that the cluster agrees on. The
  writer checks its local files against the agreed hashes before copying. A
  set that does not match is skipped.
- **Journal chunks.** The committed log, byte for byte, with two spans
  zeroed: a padding frame's body, and every frame's alignment slack (the
  bytes between a frame's length and its 32-byte-aligned end). Neither is
  replicated content, so each node's copy of them differs; nothing reads
  them.

Snapshot instants decide where sets come from. If you have not set
`snapshot_interval_bytes`, take instants yourself with `uc2ctl snapshot`
(see [Bound journal growth](bound-journal-growth.md)). With no sets there is
nothing to restore from. When a new set appears, the writer cuts a chunk
that ends exactly at it, so every set is reachable.

An **idle** cluster gets no new chunks, with one exception: after each new
set the writer cuts one small chunk, so the set is restorable.

## 3. Set retention

The tier grows until you tell it to trim. Two replicated settings control
it. Both default to `0`, which means "keep everything".

| Setting | Meaning |
|---|---|
| `backup_retain_sets` | Always keep at least this many of the newest complete sets. |
| `backup_retain_age` | Always keep sets younger than this (`30d`, `12h`, `90m`, `600s`), measured back from the newest set's log time, not from now. Units `s`, `m`, `h`, `d`. |

The two duration keys take different units on purpose:
`[backup] quiet_interval` is `ms|s|m|h` (it is short), `backup_retain_age` is
`s|m|h|d` (it is long). Neither takes a fraction or a space.

A set is deleted only when it falls outside **both** limits. The newest
complete set is never deleted. A set that an unfinished upgrade pin needs as
its rollback point is protected too, with the journal up to the pin.

Seed them at first boot in `[settings]` of `node.toml` (they take effect only
at cluster genesis), or change them on a live cluster:

```toml
# settings.toml
backup_retain_sets = 7
backup_retain_age = "30d"
```

```bash
uc2ctl settings apply settings.toml --instance-dir D --app-id A --admin-key ops.key
```

Retention runs on the writer, and only once its view of the cluster has
caught up. Deleting is idempotent: any writer may do it. The set's marker
file is removed first, so a reader never sees a half-deleted set as whole.

## 4. Read the tier

`uc2ctl tier show` reads the tier's files directly. It needs no running
node. Add `--instance-dir` of a live node to print that node's committed
view beside it.

```bash
uc2ctl tier show --target /mnt/uc-backup --instance-dir /srv/uc2/n0
```

It prints the tier's id (as `0x…`) and `app_id`, every set (position, kind,
time, rows), every chunk range, the watermark, any **fence** (see
[After a restore below the reach](#after-a-restore-below-the-reach)), and the
**restorable range**, written `(S, reach]`. A set whose marker file does not
parse is listed as `CORRUPT` and is never a restore start; the rest of the
tier lists and restores as usual. A restore point is a frame end `Q` with
`S < Q <= reach`. A set's own position is not a restore point: the restored
journal would be empty. Take the next frame end after it.

The watermark is the cluster's committed record of how far the tier reaches.
It has three positions: `oldest_set`, `newest_set` and `through`. Everything
up to `through` is in the tier as an unbroken chain from `oldest_set`.

### Metrics and alerts

Only the three gauges (`uc2_backup_through`, `uc2_backup_oldest_set`,
`uc2_backup_lag_bytes`) carry a `tier` label. Its value is the id from
`tier show` as 16 hex digits, without the `0x` that `tier show` prints. The
other series carry the labels shown, or none.

| Series | Meaning |
|---|---|
| `uc2_backup_through` | The committed watermark's `through`. Same on every node once caught up. |
| `uc2_backup_oldest_set` | The oldest restorable set. |
| `uc2_backup_lag_bytes` | Committed log the tier does not hold yet. Leader only. |
| `uc2_backup_bytes_written_total{kind}` | Stored bytes this writer wrote, `set` or `journal`. |
| `uc2_backup_errors_total{reason}` | `foreign`, `unavailable`, `overlap_mismatch`, `hash_mismatch`, `journal_unavailable`, `fenced`, `no_hardlinks`. |
| `uc2_backup_writer_stopped` | `1` while this node's writer has stopped for good; `0` otherwise and on a node with no writer. |
| `uc2_backup_hold_released` | `1` while low disk has this node's purge hold released. |
| `uc2_backup_chain_gaps_total` | Times the tier's chain restarted from a later set. |
| `uc2_backup_hold_released_total` | Times the writer let go of its purge hold for lack of disk. |
| `uc2_backup_retention_deleted_total` | Objects this writer deleted from the tier, by `kind` (`set` or `chunk`). |

Four alerts ship in `packaging/prometheus/uc2-alerts.yml`:

- `Uc2BackupBehind`: the tier is over 256 MiB of committed log behind for 15
  minutes. An idle cluster does not trip it.
- `Uc2BackupHoldReleased`: a writer node ran low on disk and let its tier
  fall behind its purge. The tier will have a gap.
- `Uc2BackupWriterError`: the writer counted an error. `foreign`,
  `overlap_mismatch`, `fenced` and `no_hardlinks` stop the writer for good.
  This rule looks at the last 10 minutes only, so it goes quiet while the
  writer stays stopped.
- `Uc2BackupWriterStopped`: the writer has stopped for good and stays
  stopped (`uc2_backup_writer_stopped == 1` for a minute). It keeps firing
  until you fix the target and restart the node.

What to do for each is in the
[runbook](../ops/uc2-runbook.md#when-something-is-wrong).

## 5. Verify the tier

```bash
uc2ctl tier verify --target /mnt/uc-backup
uc2ctl tier verify --target /mnt/uc-backup --deep
```

`verify` is offline. It checks each chunk's CRCs and frame walk, that
overlapping chunks agree, and that every set's marker names files that are
present at the stated size. It prints each finding and exits `1` on any. A
marker that does not parse is the finding `tier_set_label_corrupt`. A fence
is printed as an `info:` line, not a finding.
`--deep` also re-hashes every artifact against its marker. Run `--deep` on a
schedule and before you rely on a restore.

## 6. Restore

All restores are offline commands that **build an instance directory**. They
refuse a directory a node holds, and a directory whose `journal/`, `state/`
or `snapshots/` is not empty. A `RESTORED` file in the result records where
it came from (see
[Instance directory](../reference/instance-directory.md)). If a restore is
interrupted, a `RESTORING` marker is left and the next restore refuses by
name until you remove what was half-placed.

### Whole-cluster restore

Every member is restored: voters, learners and writers alike, all from the
**same tier** with the **same `--at`**. A member started on its old data
next to restored ones brings back the history you are discarding.

1. Stop every service, then every node.
2. On each node, move its old instance directory aside. Do not delete it
   until the restored cluster has been verified; it is your way back. A
   restore refuses a directory whose `journal/`, `state/` or `snapshots/` is
   not empty.
3. On each node, restore into the now-empty path:

   ```bash
   mv /srv/uc2/nN /srv/uc2/nN.before-restore
   uc2ctl tier restore --target /mnt/uc-backup --at Q \
       --instance-dir /srv/uc2/nN --confirm-cluster APP
   ```

   Without `--at`, Q is the tier's furthest reach. Pick Q once (`tier show`)
   and pass it to every node: two members restored at different points
   disagree about history.
4. If Q lies below the tier's reach, point every writer's `[backup] target`
   at a **new, empty** directory before you start it (see
   [After a restore below the reach](#after-a-restore-below-the-reach)).
5. Start every node, then attach services and clients.

**The data-loss boundary: everything the cluster committed after Q is gone.**
Typing the tier's `app_id` after `--confirm-cluster` is how you acknowledge
that. Each node comes up with identical history and **no vote**, so the first
election is ordinary. This is the same domain as
[quorum-loss recovery](recover-from-quorum-loss.md), not the minority-restore
rule.

**Side effects replay.** A restored node's output progress is the start set
S, not Q, so `on_committed` runs again for every command in `(S, Q]` — the
usual at-least-once contract. Make side effects idempotent, or fork instead
(a fork replays none).

### After a restore below the reach

A restore at a Q below the tier's reach rolls history back: the restored
cluster writes a new history past Q, which the tier's `(Q, reach]` does not
hold. If a writer kept the same target, it would append that new history to
the old chain, and a later restore would build state that never existed.

So such a restore leaves a **fence** in the tier, `fence/<Q>`, written after
the instance directory is placed. A writer **refuses a fenced target**: it
logs `backup_target_fenced`, counts
`uc2_backup_errors_total{reason="fenced"}`, stops and holds nothing
(`Uc2BackupWriterStopped`). Point every writer at a new, empty directory;
the new tier starts with the first set the restored cluster agrees on.

The old tier stays readable: `tier show` lists the fence, `tier verify`
prints it as `info`, and you can restore from it again (any Q it still
covers). Restoring several nodes at the same Q writes one fence. A restore at
the reach writes none, because the history continues unchanged; a fork never
writes one.

### Fork

A fork starts a **separate** cluster from the tier as of Q. Use it to
investigate the past, or to rehearse an upgrade against real history.

```bash
uc2ctl tier fork --target /mnt/uc-backup --at Q \
    --instance-dir /srv/fork/n0 --app-id APP-FORK --node-id 0
```

- `--app-id` must differ from the tier's, or the fork is refused
  (`fork_same_app_id`).
- The fork keeps one member, `--node-id`, as its single voter. Grow it
  with `uc2ctl add-learner` and `promote`.
- **A fork never replays outputs.** Its output progress starts at Q, so no
  side effect at or below Q (no email sent, no call made) happens again.
  Anything the service does from Q on is new work in a new cluster.
- The fork carries the source tier's watermark entry in its cluster state,
  and that entry **alerts**: nothing advances it on the fork, so
  `uc2_backup_lag_bytes` grows with every commit and `Uc2BackupBehind` fires
  once it passes 256 MiB. Run `uc2ctl tier forget --tier-id 0x…` on the fork
  right after its first leader is up. The same holds for a whole-cluster
  restore whose writers moved to a new target: forget the old tier's id
  once the new one reports.
- Right after boot, membership changes answer `retry` until the fork's first
  config commits. Retry after a moment.

### Export a corpus

```bash
uc2ctl tier export-corpus --target /mnt/uc-backup --row 0 --from S --to Q --out ./corpus
```

This builds a diff-replay corpus: the snapshot at `S` plus the journal span
`[S, Q)`, with a `CORPUS` manifest. `uc2-diffreplay` reads it unchanged. See
[Diff replay an FSM change](diff-replay.md). Without `--from`, S is the
newest complete set below `--to` that the chain reaches.

### Roll back a bad upgrade

If a pin has committed and you must go back, the tier's protected rollback
set is the way. See
[Upgrade an application](upgrade-an-application.md#rolling-back).

## Purge and the writer's hold

Purge is off by default. Where it is on, the writer's node will not purge
journal the tier does not yet hold. Three consequences:

- Another node's purge is unaffected; only the writer's node holds.
- If the writer's disk runs low, it lets go of the hold and purges anyway
  (`Uc2BackupHoldReleased`). The tier then has a gap and restores to points
  inside it are no longer possible.
- A writer that has stopped (a foreign or fenced target, a mismatch, a
  filesystem without hard links) releases its hold at once. Fix the cause
  and restart the node.

## What the tier does not do yet

- **Nodes never delete anything because of the tier.** Local snapshot and
  journal retention are exactly as before. Nothing is purged sooner because
  the tier has a copy.
- **No tier-seeded new node.** `uc2ctl tier restore --new-node` is refused
  (`new_node_unsupported`). A replacement node joins from its peers, with a
  fresh, empty instance directory, the usual way. Seeding a node from the
  tier is planned work.
- **Nodes do not fetch from the tier.** A node that falls behind still
  catches up from peers.
- **Offline commands only.** `show`, `verify` and the restores read files.
  Only `tier forget` talks to a node (leader only).

## Known limits

- A set taken before a row applied anything cannot start a service. If you
  restore to such a set, the row's service refuses to attach
  ([#98](https://github.com/PeterKnego/ultima_cluster/issues/98)). Sets taken
  after the first command applied are fine.
- A cluster registers at most four tiers. `uc2ctl tier forget` frees a slot;
  the tier's files stay.
- A writer never rewrites an object. A target that silently changes a
  stored object is found by `verify --deep`, not repaired.

Dev-box smoke, not a measurement: on a three-node test run with highly
repetitive synthetic commands, 769 344 bytes of journal were stored as 18 491
bytes with zstd; real data compresses far less.
