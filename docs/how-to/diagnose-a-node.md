# How to diagnose a node that is not serving

Work out what a live node believes about itself and the cluster, when clients
are failing, replication has stalled, or nobody appears to be leading.

Everything below is readable from the node's cnc page while it runs. Start with
`uc2ctl status`; drop to raw offsets when you need a field it does not print,
or when you cannot run a binary on the host.

```bash
uc2ctl status --instance-dir D --app-id A
```

Attach read-only with `CncPage`, or read the fixed offsets with `xxd` — the
layout is pinned and does not drift. Field-by-field detail is in
[The cnc control page](../reference/cnc-page.md).

## Is anyone leading?

Read `flags` at offset 768.

| Value | Meaning |
|---|---|
| `0x03` | this node is the serving leader |
| `0x01` | elected, but not yet serving — its NewTerm frame is not yet quorum-committed |
| `0x00` | follower or learner |

A cluster where every node reads `0x00` has no leader. A node stuck at `0x01`
has won an election but cannot get its first frame committed, which usually
means it cannot reach a quorum.

`leader_hint` at 832 gives the last known leader id; `u64::MAX` means unknown.

## Is the node alive, and is its service alive?

Compare `node_heartbeat_ns` (896) and `service_heartbeat_ns` (960) against your
own clock. They are separate processes and fail separately: a frozen service
heartbeat with a live node heartbeat means the apply loop is wedged, not the
cluster.

## Is replication moving?

Read the counters at 256, 320, 384, 448, 512. On a healthy leader:

```
append ≥ durable ≥ commit,  service_applied trailing commit by the apply lag
```

If `commit` is not advancing while `append` climbs, the leader is not getting
acknowledgements from a quorum. Look at the per-peer band next.

## Which peer is behind?

Only the leader publishes the per-peer band at offset 1408; on a follower the
whole band reads zero. Slots are voting followers first, then learners.

For each slot, per-peer replication lag is `commit − reported_durable`.

A peer whose `reported_durable` is pinned at 0 has never been heard from at
all. The usual cause is an address mismatch rather than a network fault — see
the bind check in [Run a cluster](run-a-cluster.md#bind-the-exact-address-you-advertise).

## Is purging keeping up?

Compare `archive_first_base` (1344) with `node_snapshot_floor` (1216).

| Observation | Meaning |
|---|---|
| `archive_first_base` climbing toward `node_snapshot_floor` | purge is working |
| `archive_first_base` pinned at 0 while the floor advances | purge is off, or not running |
| `archive_first_base` lagging indefinitely with purge enabled | the archive purge is failing — check node logs; errors are logged and retried, never fatal |

If purging is meant to be on, see
[Keep the journal from growing without bound](bound-journal-growth.md).

## Is a joiner recovering by snapshot?

Watch `incoming_snapshot_pos` (1280) on the joining node. It advances when a
below-floor member installs a snapshot before tail-replaying.

## A node that truncated to zero and rejoined

If a rejoining node's log has no common prefix with the leader — because the
leader purged past the point where they diverged — the node truncates to 0 and
rejoins from the snapshot floor. Its `wipes()` counter increments.

This is automatic and safe. It is not a fault to investigate, though a node
doing it repeatedly is worth understanding.

## If crypto is enabled

Check the drop counters, and judge by the followers rather than the leader —
the leader's `seal_failures` climbs benignly. The table is in
[Encrypt traffic between nodes](encrypt-node-traffic.md#confirm-it-is-healthy).

A non-zero `cleartext_peer` means some node in the cluster is still running
cleartext.

## If reads are failing but writes are not

A burst of reads resolving `RETRY` after about a second means the read barrier
cannot reach quorum — a partition, or the leader has been deposed. Sub-second
read stalls under packet loss should not happen; the barrier retransmits every
2 ms. See [Linearizable read path](../reference/read-path.md#diagnostic-signatures).
