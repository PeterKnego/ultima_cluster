# How to keep the journal from growing without bound

By default a node's journal grows forever: purging is off, and an unpurged
cluster is always safe. To bound it you need snapshots first, because purging
is only safe once there is a snapshot to purge below.

This is also a prerequisite for reconfiguring a cluster under sustained write
load — see [Change cluster membership](change-cluster-membership.md).

## Confirm your state machine can snapshot

Your `StateMachine` must also implement `SnapshotStateMachine`, giving it
`build_snapshot` and `install_snapshot`. The `ultima_db` `StoreStateMachine`
adapter does.

Without it, `service_snapshot_pos` never advances, there is no floor, and
nothing can be purged. If you turn purging on anyway, nothing breaks — nothing
happens.

## Choose a slack and turn it on

Set `purge: PurgePolicy::BelowSnapshot { slack_bytes }` in `NodeConfig`.

`slack_bytes` retains a tail below the snapshot floor so that a
slightly-behind follower can still catch up by ordinary journal replay instead
of needing a full snapshot install. Size it to your worst-case follower lag: too
small and ordinary lag triggers snapshot sessions, too large and you keep
journal you meant to reclaim.

## Confirm it is working

Watch two counters on the cnc page:

```bash
uc2ctl status --instance-dir D --app-id A
```

`archive_first_base` should rise toward `node_snapshot_floor`. If it lags
forever, the archive purge is failing — check the node logs. Purge errors are
logged and retried, never fatal, so the symptom is silence rather than a crash.

## What happens to a node that falls below the floor

A follower or learner whose NAK falls below `archive_first_base` is served a
snapshot session and then tail-replays. This is automatic. Watch
`incoming_snapshot_pos` on the receiving node to see it happen.

A restarted node does **not** prefill its send ring from the journal. A
below-ring catch-up gap is served on demand by deep-NAK replay instead.

## Where to go next

- Field meanings for the counters above: [The cnc control page](../reference/cnc-page.md)
- The policy type and its default: [Configuration](../reference/configuration.md)
