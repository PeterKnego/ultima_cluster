# `uc2ctl`

The administrative CLI. It performs live cluster reconfiguration and reports
cluster state.

`uc2ctl` communicates with a running node through the admin band of that node's
`cnc2.dat` control page. It does not open a network socket and it does not read
the replicated log.

To perform a membership change, see
[Change cluster membership](../how-to/change-cluster-membership.md).

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
