# How to keep the journal from growing without bound

By default a node's journal grows forever: purging is off, and an unpurged
cluster is always safe. To bound it you need a **complete snapshot set** to
purge below — and since 2.11 (pending) a set is something the cluster takes
together, at one log position, on command.

This is also a prerequisite for reconfiguring a cluster under sustained write
load — see [Change cluster membership](change-cluster-membership.md).

## Confirm every declared FSM can snapshot

Your `StateMachine` must also implement `SnapshotStateMachine`, giving it
`freeze` (pin the state cheaply and hand back a handle), `stream_snapshot`
(write that handle's bytes, off the apply thread) and `install_snapshot`, and
the service must be started with
`start_with_snapshots()` rather than `start()` — that is what publishes the
row's **snapshot-capability bit**. `uc_lincheck`'s `RegisterSm` and
`ListAppendSm` are small worked examples of the pair.

Every declared row has to be capable, not just row 0: a set at a position is
complete only when every row *and* the cluster FSM have published an artifact
at it. A cluster with one non-snapshotting row is legitimate — it simply never
snapshots — so you are told rather than left watching a floor that never
moves: `uc2ctl snapshot` refuses `48 snapshot_unsupported`, naming the row.

See [State-machine contract § Snapshots](../reference/state-machine-contract.md#snapshots-the-instant-the-envelope-and-the-exclusive-frontier)
for what `freeze` must and must not do — in particular, keep it O(1) and put
the O(state) work in `stream_snapshot`.

## Take a snapshot: command an instant, or set a cadence

There is no per-service byte trigger any more (`SnapshotPolicy` is retired). Two
triggers, both leader-only:

**On command.** One instant, now:

```bash
uc2ctl snapshot --instance-dir D --app-id A --admin-key ops.key
# instant=73792
```

The leader appends a `SNAPSHOT` frame; its frame-end position **P** — the
number printed — is the instant. Every declared row and the cluster FSM
freeze there, having applied everything below P, and publish
`snapshots/<row>/snap-<P>.ultsnap` (the cluster FSM,
`snapshots/cluster/snap-<P>.ultcluster`).

**On a cadence.** Set the replicated `snapshot_interval_bytes` and the leader
issues one itself every time that much log has accrued:

```bash
cat > settings.toml <<'TOML'
snapshot_interval_bytes = 1073741824   # 1 GiB of log between instants
snapshot_target         = "all"        # or "learners" — see below
TOML
uc2ctl settings apply settings.toml --instance-dir D --app-id A --admin-key ops.key
```

`0` (the default) means no cadence: operator-commanded only. See
[`uc2ctl` § `snapshot`](../reference/uc2ctl.md#snapshot) and
[Configuration § `[settings]`](../reference/configuration.md#settings).

**A freeze costs the cluster commit progress while it runs.** Every row's
apply thread is inside `freeze()` at the same position, and a node's durable
report is capped at `min_applied + fsm_lag`, so a slow freeze on a quorum
stalls commit at `P + fsm_lag` until the slowest one ends. If your state is
large enough for that to matter, take **standby** instants instead:
`uc2ctl snapshot --standby` (or `snapshot_target = "learners"`) freezes only
the learners, and a voter pulls the finished set with
[`uc2ctl snapshot fetch --from <learner-id>`](../reference/uc2ctl.md#snapshot-fetch),
which writes the artifacts without touching its state machines. The trade is
explicit: a voter's floor moves when an operator fetches, not on its own.

## Choose a slack and turn purging on

Set `purge: PurgePolicy::BelowSnapshot { slack_bytes }` in `NodeConfig`.

Purge follows the **set**: a node's floor moves to P only once every declared
row's artifact at P and the cluster artifact at P are on disk, and the node
prunes the journal below that floor. Retention is the node's too, and it only
deletes: it keeps the set at the persisted floor plus everything newer.

`slack_bytes` retains a tail below the snapshot floor so that a
slightly-behind follower can still catch up by ordinary journal replay instead
of needing a full snapshot install. Size it to your worst-case follower lag: too
small and ordinary lag triggers snapshot sessions, too large and you keep
journal you meant to reclaim.

## Confirm it is working

```bash
uc2ctl snapshot show --instance-dir D --app-id A
# row=0 name=orders newest=73792
# row=1 name=kv newest=73792
# cluster newest=73792
# set=73792
```

`set=` is the newest position present in **every** row's directory and in
`snapshots/cluster/` — the intersection, so a row that has already frozen the
next instant does not make an established set vanish from the reading. A row
whose `newest=` sits below the others is the row holding the floor back.

Then watch two counters on the cnc page:

```bash
uc2ctl status --instance-dir D --app-id A
```

`archive_first_base` should rise toward `node_snapshot_floor`. If it lags
forever, the archive purge is failing — check the node logs. Purge errors are
logged and retried, never fatal, so the symptom is silence rather than a crash.

On a cluster with `/metrics` on, `Uc2SnapshotStalled` makes the "one broken FSM
silently stops all purging" case loud, and
`uc2_snapshot_row_incomplete_total{row}` names the row. On a
`snapshot.target = learners` cluster watch `Uc2StandbySnapshotStalled` on the
learners instead: the leader is a voter there, and its own set is *supposed*
not to complete until `uc2ctl snapshot fetch` runs —
see [Monitor a cluster § The snapshot families](monitor-a-cluster.md#the-snapshot-families-211-pending).

## What happens to a node that falls below the floor

A follower or learner whose NAK falls below `archive_first_base` is served a
snapshot session and then tail-replays. This is automatic. Watch
`incoming_snapshot_pos` on the receiving node to see it happen. A session
ships one complete set at one position, or none at all: a node that cannot
assemble one declines by name rather than shipping half.

If the node that would serve it is a voter that has not fetched a standby set
— or one restored from a backup taken before its own floor — it answers with a
**redirect** to a learner that does hold the set, and the joiner asks there.

A restarted node does **not** prefill its send ring from the journal. A
below-ring catch-up gap is served on demand by deep-NAK replay instead.

## Where to go next

- Field meanings for the counters above: [The cnc control page](../reference/cnc-page.md)
- The verbs: [`uc2ctl` § `snapshot`](../reference/uc2ctl.md#snapshot)
- The policy type and its default: [Configuration](../reference/configuration.md)
- Why an instant is one position, and what standby buys:
  [The cluster FSM, explained § Instants](../notes/uc2-cluster-fsm-explained.md#instants-one-position-one-set)
