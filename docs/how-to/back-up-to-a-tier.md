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
| `uc2_backup_errors_total{reason}` | `foreign`, `unavailable`, `overlap_mismatch`, `hash_mismatch`, `journal_unavailable`, `fenced`, `no_hardlinks`, `readback`. |
| `uc2_backup_writer_stopped` | `1` while this node's writer has stopped for good; `0` otherwise and on a node with no writer. |
| `uc2_backup_hold_released` | `1` while low disk has this node's purge hold released. |
| `uc2_backup_chain_gaps_total` | Times the tier's chain restarted from a later set. |
| `uc2_backup_hold_released_total` | Times the writer let go of its purge hold for lack of disk. |
| `uc2_backup_retention_deleted_total` | Objects this writer deleted from the tier, by `kind` (`set` or `chunk`). |
| `uc2_purge_gate_position` | The position every node's purge is held at or below when `purge_requires_tiers` is on (header only while it is `0`). |
| `uc2_backup_gate_released`, `uc2_backup_gate_released_total` | The level / count of low-disk releases of that gate. |
| `uc2_snapshot_fetch_total{source}` | Store-only fetches by where they came from, `tier` or `peer`. |
| `uc2_tier_install_total{outcome}` | Below-floor installs from the tier: `installed`, `declined` or `timeout`. |

Five alerts ship in `packaging/prometheus/uc2-alerts.yml`:

- `Uc2BackupBehind`: the tier is over 256 MiB of committed log behind for 15
  minutes. An idle cluster does not trip it.
- `Uc2BackupHoldReleased`: a writer node ran low on disk and let its tier
  fall behind its purge. The tier will have a gap.
- `Uc2BackupGateReleased`: a node let go of the backup-gated purge for lack
  of disk and purged past the tier.
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

### Replace a node from the tier

A node that is lost for good can be rebuilt from the tier and joined to the
live cluster, with no copy of the log from a peer.

```bash
uc2ctl tier restore --target /mnt/uc-backup --new-node --node-id N \
    --instance-dir /srv/uc2/nN
```

Start the node, then add it the usual way:

```bash
uc2ctl add-learner N ...
uc2ctl promote N ...        # if it should vote
```

`--new-node` refuses an id that is in the membership the node would boot with
(`restore_id_reused`): use a fresh id, or remove the old member first. The
restored node keeps its log up to Q, which it holds from the set S the
restore started at, and replication continues from there. It works because
every set in the tier records the cluster's term history below it (its
*lineage*), and restore writes that into the node's term map; see
[the tier as a holder](../notes/uc2-backup-tier-explained.md#the-tier-as-a-holder).
A set backed up by a pre-3b writer has no lineage and is refused
(`set_lineage_missing`).

A node with a `[tier_source]` that is added empty needs no restore at all:
see [The tier as a source](#the-tier-as-a-source).

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

- Another node's purge is unaffected by default; only the writer's node holds
  (see [Backup-gated purge](#backup-gated-purge) to make every node hold).
- If the writer's disk runs low, it lets go of the hold and purges anyway
  (`Uc2BackupHoldReleased`). The tier then has a gap and restores to points
  inside it are no longer possible.
- A writer that has stopped (a foreign or fenced target, a mismatch, a
  filesystem without hard links) releases its hold at once. Fix the cause
  and restart the node.

### Backup-gated purge

By default only the writer's node holds its purge at the tier. To make
**every** node hold, set the replicated setting `purge_requires_tiers` to K
(1 to 4; `0`, the default, is off):

```toml
# settings.toml
purge_requires_tiers = 1
```

```bash
uc2ctl settings apply settings.toml ...
uc2ctl settings show ...
```

With K on, every node keeps its purge floor at or below the K-th highest
confirmed `through` among the tiers registered in the cluster (`uc2ctl tier
show --instance-dir` marks the K entries that count). Fewer than K tiers
registered means no gate position, and **no purge beyond the other holds**:
raising K above the registered count stops purge cleanly and it resumes when
a tier registers. The gate only holds purge back. It never causes one, and
purge still needs `[purge]` on.

**Read this before turning it on.** With the gate on, history older than a
node's newest agreed set lives **only in the tier**. The tier's retention
(`backup_retain_sets`, `backup_retain_age`) therefore decides how far back a
restore can go: what the tier prunes is gone from the cluster altogether.

Two behaviours to expect:

- **The read-back.** The writer reads back every object it creates, right
  after writing it, and an object counts only when it reads back clean. An
  object it finds already in place is re-read only if it lies above the
  committed `through` (chunks) or `newest_set` (sets), so a restarted writer
  does not re-read the tier's history. A failure leaves the report where it
  was, counts `uc2_backup_errors_total{reason="readback"}`, logs
  `backup_readback_failed` naming the object, and fires
  `Uc2BackupWriterError`. The writer retries at its next pass. A persistently
  bad object is yours: `uc2ctl tier verify` names it.
- **The low-space release.** A node whose free space falls below one
  `chunk_bytes` (64 MiB on a node that is not a writer) stops applying the
  gate, purges as its other holds allow, and logs `backup_gate_released` once
  per episode (`backup_gate_rearmed` when it comes back). `Uc2BackupGateReleased`
  fires. The tier then has a gap, as it does for a writer's release.

`uc2_purge_gate_position` shows the gate each node computes (it is exported
with K = 0 too, header only).

## The tier as a source

A node can read sets from the tier instead of asking a peer, which keeps a
large copy off the nodes that are serving.

```toml
[tier_source]
target = "/mnt/uc-backup"
```

The section is node-local and read-only. A node with `[backup]` already reads
its own `target` this way and needs no `[tier_source]`. At boot the node opens
`TIER`; an **explicit** `[tier_source]` whose `app_id` is another cluster's
refuses to start (`tier_source_foreign`). A source implied from `[backup]`
that is foreign does not refuse: it logs `tier_source_unavailable` and is not
used, and the writer's own `backup_target_foreign` stop applies as before.
An unreachable tier is the same: the node starts and falls back to peers.

What the node takes from it:

- **A store-only fetch** (auto-fetch, `uc2ctl snapshot fetch`) tries the tier
  first, then peers. The set must be one the tier's registered entry covers
  (never beyond its `through`). Each artifact is copied to a temp name
  (`incoming-<P>.tier.part`), renamed into place and checked against the
  cluster's agreed hash and version, so the cluster's catalog, not the tier,
  is the authority. Any failure removes the copy and falls back to a peer,
  logging `tier_fetch_declined{position, reason}`. `uc2_snapshot_fetch_total{source}`
  counts tier and peer fetches.
- **A below-floor install.** When a follower's NAK falls below the leader's
  purge floor the leader asks its tier first (`TIER_INSTALL`) instead of
  opening a snapshot session. The leader names the set and carries its
  catalog verdict; the follower copies, verifies, installs and adopts the
  set's lineage, then answers. A follower with no source answers `declined`
  at once. An install needs no committed registration of the follower's tier,
  only that the tier belongs to this cluster. Events: `tier_install_started`,
  `tier_install_done`, `tier_install_declined{reason}`; a node that is asked
  but is not below the floor logs `tier_install_ignored`.
  `uc2_tier_install_total{outcome}` counts them.

Timings, per peer: the leader falls back to a snapshot session if the follower
does not answer in **2 s** or shows no progress for **10 s**. A decline or a
timeout holds the peer off the tier for **30 s**. After `installed`, a **10 s
settling grace** applies: a below-floor NAK from that peer opens nothing
while its next report arrives; after 10 s it is treated like a decline (a
session), and after 30 s it is asked again.

A new learner with a mounted source therefore catches up from the tier on its
first below-floor NAK and the leader ships it nothing large. A tier set the
writer could not yet place logs `tier_set_deferred` after ten deferrals.

### `auto_fetch = false` with a source on every voter

`auto_fetch = false` is a supported configuration when every voter has a tier
source: voters stop copying the learner-built (standby) sets, which can be
large, and a voter restarted below the floor catches up through the tier
install above. Without a source on a voter, `auto_fetch = false` leaves that
voter on the peer session, as before.

## What the tier does not do yet

- **Local snapshot retention ignores the tier.** Sets are kept per
  `retain_sets` as before. Journal purge follows the tier only when
  `purge_requires_tiers` is on.
- **The tier is not a live log.** A node reads sets from it, never its
  journal; the log above a peer's purge floor always comes from peers.
- **Offline commands only.** `show`, `verify` and the restores read files.
  Only `tier forget` talks to a node (leader only).

## Known limits

- A cluster registers at most four tiers. `uc2ctl tier forget` frees a slot;
  the tier's files stay.
- A writer never rewrites an object. A target that silently changes a
  stored object is found by `verify --deep`, not repaired.

Dev-box smoke, not a measurement: on a three-node test run with highly
repetitive synthetic commands, 769 344 bytes of journal were stored as 18 491
bytes with zstd; real data compresses far less.
