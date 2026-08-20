# `uc2ctl`

The administrative CLI. It performs live cluster reconfiguration and reports
cluster state.

`uc2ctl` communicates with a running node through the admin band of that node's
`cnc2.dat` control page. It does not open a network socket and it does not read
the replicated log.

To perform a membership change, see
[Change cluster membership](../how-to/change-cluster-membership.md).

## Admin-band commands vs. offline commands

Every command in [Sub-commands](#sub-commands) below (`add-learner` through
`status`) is an **admin-band** command: it needs a running node, reaches it
purely through the cnc page's admin request/response slots, and both
`--instance-dir` and `--app-id` are checked against that live node.

The four commands under [Offline commands](#offline-commands) are different in
kind, not just in name: `backup`, `verify-backup`, and `restore` are
filesystem-only and never touch a running node's cnc admin band at all (the
node may be running throughout a `backup`, since it never talks to it);
`force-single-member` takes the instance directory's exclusive `flock`
directly and **refuses if a node is running**. None of the four accept
`--app-id` in the same sense as the admin-band commands — `backup`/`restore`
don't take it at all (there is nothing on disk to check it against),
and `force-single-member` takes it purely as a typed confirmation guard, not
as anything validated against a live node.

## Synopsis

```
uc2ctl <COMMAND> --instance-dir <DIR> --app-id <ID> [command options]
```

## Common arguments

Every sub-command takes both.

**`--instance-dir <DIR>`**
The node's on-disk instance directory — the same path passed to `Node::start`.
`uc2ctl` opens `<DIR>/cnc2.dat`.

**`--app-id <ID>`**
Application identity. Must match the running node's `app_id`; the page open
fails otherwise.

## Sub-commands

Sub-commands are: `add-learner`, `promote`, `demote`, `remove-learner`,
`remove-voter`, `status`.

### `add-learner`

Adds a fresh learner. Wire op `1`.

- `--id <U32>` — the new member's node id.
- `--addr <IP:PORT>` — the new member's replication-socket bind address.

Returns the accepted/refused/retry outcome described under
[Response statuses](#response-statuses).

### `promote`

Promotes a caught-up learner to voter. Wire op `2`.

- `--id <U32>`

### `demote`

Demotes a voter to learner. Wire op `3`.

- `--id <U32>`

### `remove-learner`

Permanently removes a learner. The id is tombstoned. Wire op `4`.

- `--id <U32>`

### `remove-voter`

Permanently removes a voter. The id is tombstoned. Wire op `5`.

- `--id <U32>`

### `status`

Prints the node's current config version and pending state, per-member
peer-slot observability, and the leader/serving flags. Read-only: it writes no
admin request.

- `--admission-bytes <U64>` — override for the staleness warning's admission
  window. Since wire protocol 0.3.0 the node publishes its configured value on
  the cnc page; this flag is needed only against pre-0.3.0 nodes, whose page
  reads `0`.

Output fields:

| Field | Meaning |
|---|---|
| `leader` | `NODE_FLAG_LEADER` is set |
| `can_serve` | `NODE_FLAG_CAN_SERVE` is set |
| `term` | current term |
| `leader_hint` | the id this node believes leads; `unknown` when the raw value is `u64::MAX` |
| `log: commit / durable / append` | the three log counters, in bytes |
| `members` | one line per occupied peer slot: `id`, `role`, `reported_durable`, and a staleness marker when `commit - reported_durable` exceeds the admission window |

## Offline commands

`backup`, `verify-backup`, `restore` (M11 Task 2) and `force-single-member`
(M11 Task 4). See [the offline-vs-admin-band distinction](#admin-band-commands-vs-offline-commands)
above. Task walkthroughs: [Back up a cluster](../how-to/back-up-a-cluster.md),
[Recover from quorum loss](../how-to/recover-from-quorum-loss.md).

### `backup`

Ordered-copy an instance directory's durable `journal/` → `state/` →
`snapshots/` into a fresh backup artifact, then runs `verify-backup` on the
copy and writes a `MANIFEST`. Filesystem-only; the node may be running
throughout.

- `--instance-dir <DIR>` — the node's on-disk instance directory.
- `--out <DIR>` — destination for the new artifact. Refused if it already
  exists and is non-empty (`ArtifactExists`).

Prints the resulting report as `key=value` lines: `journal_first_base`,
`journal_last_pos`, `newest_snapshot` (or `none`), `snapshot_floor`,
`healed_torn_tail`, `files`. Nonzero exit on error.

### `verify-backup`

Read-only verification of a backup artifact — recovers its positions, checks
the coverage invariant (a journal whose `first_base > 0` must be covered by a
retained snapshot, else `Hole`), and cross-checks a `MANIFEST` if present
(`ManifestMismatch` on tamper/bitrot). May heal the artifact's own torn
active-segment tail (a shrink-only truncate, reported as
`healed_torn_tail=true`); everything else is read-only.

```
uc2ctl verify-backup <ARTIFACT>
```

Same `key=value` report as `backup`. Nonzero exit on error, including `Hole`
and `ManifestMismatch`.

### `restore`

Verify a backup artifact, then copy its three durable directories into a
fresh instance directory.

```
uc2ctl restore <ARTIFACT> --instance-dir <DIR>
```

Refuses (`TargetNotEmpty`) if `--instance-dir`'s `journal/`, `state/`, or
`snapshots/` already contain anything — restore never merges or overwrites.
Volatile leftovers (`cnc2.dat`, `log.buf`, rings, `instance.lock`) are fine.
The target should not have a node running in it. Same `key=value` report as
`backup` (the artifact's positions, not a property re-derived from the new
instance directory).

### `force-single-member`

Quorum-loss recovery: forces the given `--instance-dir` to a single-voter
cluster naming `--node-id` the sole member. OFFLINE like the three commands
above, but the refusal mechanism differs — it takes the instance's exclusive
`flock` directly, so **a running node refuses it**, rather than there being
nothing on disk to conflict with.

```
uc2ctl force-single-member --instance-dir <DIR> --app-id <A> --node-id <N> --confirm-cluster <A>
```

- `--instance-dir <DIR>` — the surviving node's own instance directory. Must
  not have a node currently running in it.
- `--app-id <ID>` — the cluster's application identity, printed in the
  data-loss statement.
- `--node-id <U32>` — the surviving node's own id. Must be a voter or learner,
  and not tombstoned, in the recovered config.
- `--confirm-cluster <ID>` — must equal `--app-id` exactly, or the command
  refuses before touching anything. A typed second confirmation, not a
  network-facing identity check.

Refuses without writing anything if: a node is running; `--confirm-cluster`
doesn't match `--app-id`; no durable config record exists yet (a never-booted
instance dir); the "doubly-ahead" crash window applies (two config adoptions
durably persisted in the same crash, before any archive catch-up — nothing
genuine left to revert to); `--node-id` is tombstoned; or `--node-id` is not a
member of the recovered config.

On success, prints the exact data-loss statement first, then the resulting
report (`old_version`, `new_version`, `durable`, `dropped_peers`). Every peer
but the survivor is **dropped from the config, not tombstoned** — they
wipe-and-rejoin later as fresh learners; see
[Recover from quorum loss](../how-to/recover-from-quorum-loss.md). A one-way
door: there is no "undo" verb.

## Response statuses

Mutating commands write an admin request and poll for the response line.

| Status | Printed as | Process outcome |
|---|---|---|
| `0` | `accepted: config version now <N>` | exit 0 |
| `1` | `refused: <reason>` | error exit |
| `2` | `retry: leader unknown or the append ring was momentarily full` | error exit |

## Refusal reasons

The `reason` field of a status-`1` response. These are the discriminants of
`uc2_consensus::config::ProposeError`, which is the authoritative table.

| Code | Reason |
|---|---|
| 1 | `NotLeader` |
| 2 | `NotServing` — a change is still settling |
| 3 | `ChangePending` — one membership change in flight at a time |
| 4 | `Tombstoned` — the id was permanently removed and cannot rejoin |
| 5 | `AlreadyPresent` |
| 6 | `NotFound` |
| 7 | `WrongRole` — promoting a voter, or demoting a learner |
| 8 | `ZeroVoters` — the change would leave the cluster with no voters |
| 9 | `TooManyMembers` — the 8-member cap |
| 10 | `NotCaughtUp` — the learner is too far behind commit to promote safely |
| 11 | malformed or unknown op — the node did not recognise the request |
| 12 | `SelfDemote` — a leader cannot demote itself |

Code `0` is not a `ProposeError`. It is the CLI's own malformed-op sentinel.

## Timeouts and limits

| Limit | Value |
|---|---|
| Response poll timeout | 10 s |
| Response poll interval | 20 ms |
| Total cluster members | 8 (voters plus learners) |
| Membership changes in flight | 1 |

On timeout, `uc2ctl` reports that a newer admin request may have superseded
this one. `uc2ctl status` shows the authoritative config version.

## Concurrency

One admin client per instance directory at a time. This includes `uc2ctl`,
`m7_gate`, and any direct caller of `write_admin_req`. Concurrent invocations
against the same instance directory may produce a malformed request.
