# How to keep the journal from growing without bound

By default a node's journal grows forever: purging is off, and an unpurged
cluster is always safe. To bound it you need a **complete snapshot set** to
purge below — and since 2.11.0 a set is something the cluster takes
together, at one log position, on command.

This is also a prerequisite for reconfiguring a cluster under sustained write
load — see [Change cluster membership](change-cluster-membership.md).

## Every declared FSM can already snapshot

Snapshot support is required (#67): `ServiceBuilder::start()` only compiles
for `S: SnapshotStateMachine`, so any row built with the current SDK already
has it, either through the `WholeStateSnapshot` helper (encode/decode the
whole state, the SDK does the rest) or a hand-written `freeze` /
`stream_snapshot` / `install_snapshot`. `uc_lincheck`'s `RegisterSm` and
`ListAppendSm` are small worked examples of the hand-written pair;
`examples/counter` is the helper.

Every declared row has to be capable, not just row 0: a set at a position is
complete only when every row *and* the cluster FSM have published an artifact
at it. The only way to see a row without the capability bit is a service
attached without the current Rust SDK (a non-Rust attacher, or a binary
built before #67) — `uc2ctl snapshot` refuses `48 snapshot_unsupported`,
naming the row, rather than leaving you watching a floor that never moves.

See [State-machine contract § Snapshots](../reference/state-machine-contract.md#snapshots-required-the-instant-the-envelope-and-the-exclusive-frontier)
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
which writes the artifacts without touching its state machines. With `auto_fetch` on (the default) a voter fetches the set itself — see
[below](#learner-only-clusters-voters-fetch-and-purge-by-default); with it off,
a voter's floor moves only when an operator fetches.

## Learner-only clusters: voters fetch and purge by default

With `[settings] snapshot_target = "learners"` (or `uc2ctl snapshot
--standby`) only learners freeze. Every node — voters included — then
fetches the newest **agreed** set in the background (`auto_fetch`, on by
default), holds it, and purges its journal below it; no `uc2ctl snapshot
fetch` is needed. Watch `uc2_snapshot_auto_fetch_total{outcome}` and
`Uc2SnapshotWontFit`: a node that cannot fit the set with headroom
(`free < size + max(size/4, 1 GiB)`) skips it and does not purge.

**One learner proves little.** A standby set is agreed over the learners
that reported it; with ONE learner that is one reporter, and every voter
then fetches and purges on a set nobody cross-checked. A node logs
`snapshot_fetch_single_reporter` (warn) once per such set. Run two learners
if the purge floor must rest on agreement.

**Turning it off** (`uc2ctl settings apply` with `auto_fetch = false`):
voters on a learner-only cluster then hold no set and never purge — the
journal grows until you run `uc2ctl snapshot fetch` on each voter.

## Choose a slack and turn purging on

Set `purge: PurgePolicy::BelowSnapshot { slack_bytes }` in `NodeConfig`.

Purge follows the **set** — but, since `0.11.0` (the snapshot catalog,
unreleased), not file presence alone. A node's floor moves to P only once P
is the newest set the cluster's replicated catalog **agrees** on (every
declared row's and the cluster artifact's reported hash match, spec §4.4)
*and* this node holds that set complete on disk. A set every row built but
that diverged across nodes is never the floor, whatever `uc2ctl snapshot
show`'s `set=` says — `set=` is the node-local, **on-disk** reading (a file
listing); the floor is the catalog's, and the two can legitimately differ.
Before the first instant completes (the `Empty` state, `uc2_catalog_empty`),
the floor falls back to today's behaviour: the newest complete set this
node holds, file presence alone. On a learner-only cluster (`snapshot_target
= "learners"`) a voter's effective floor does not move until it fetches
(below) — that is the normal state of such a voter, not a stall. The node
prunes the journal below its effective floor. Retention is the node's too,
and it only deletes: it keeps the set at the persisted floor plus
everything newer. `retain_sets` (default `1`) is how many agreed sets the
replicated catalog keeps at all, plus every pinned origin (a row's newest
upgrade pin keeps its origin's set in addition, never counted toward
`retain_sets`) — see [Configuration §
`[settings]`](../reference/configuration.md#settings).

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
see [Monitor a cluster § The snapshot families](monitor-a-cluster.md#the-snapshot-families-2110).

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
