# Instance directory

One node owns one instance directory. The service and any clients attach to the
same directory, on the same host, through shared memory.

The directory path is passed to `Node::start` and to every `uc2ctl` invocation.

## Files

| Path | Owner | Contents |
|---|---|---|
| `instance.lock` | node | Exclusive `flock`. A second node on the same directory is refused with `AlreadyRunning`. Service and clients take a shared lock as a liveness probe. |
| `cnc2.dat` | node | The 8 KiB control page (page 1: the M1–M13 layout; page 2: the per-FSM service-slot band since M14). See [The cnc control page](cnc-page.md). |
| `log.buf` | node | The log ring buffer, `buffer_bytes` long. Recreated on each boot. |
| `journal/` | node | Segmented durable log (`ultima_journal`). Survives restarts; the source for replay and purge. |
| `state/` | node | Raft durables, held as `StableValue`s: vote, term map, output progress, snapshot floor, and the config record. |
| `snapshots/<id>/` | service and node | `snap-<pos>.ultsnap` artifacts for FSM `id`, one directory per declared id since M14. The service builds them; the node ships and installs them. `<pos>` is the absolute log byte position the snapshot represents. |
| `ingress.ring` | clients → node | MPSC submit ring. Per-record commit format (`ULTRNG2` magic) since 2.7.0. |
| `query.ring` | clients → node | Query submissions, both linearizable and snapshot reads. Same format as `ingress.ring`. |
| `svc_query.<id>.ring` | node → service | Forwarded queries for FSM `id`. One per declared id since M14. |
| `egress_service.<id>.broadcast` | node → service | Apply and output stream to FSM `id`'s service. One per declared id since M14. |
| `egress_node.broadcast` | node → clients | Submit responses broadcast to clients. |
| `service.<id>.lock` | service | Exclusive `flock`, held for FSM `id`'s service process's life — one process per declared id (M14). |
| `audit.jsonl` | node | Append-only record of every admin request this node answered, one JSON line each, fsynced before the answer is published. One exception: a byte-identical re-send of an already-answered, already-recorded proposal (same nonce) is counted, not re-recorded — it repeats an answer already in the file rather than being a new admin event. Never rotated or truncated by the node. See [Change cluster membership](../how-to/change-cluster-membership.md). |

Since M14, the per-service files are named by id: `svc_query.<id>.ring` and
`egress_service.<id>.broadcast`, one pair per declared FSM. The pre-M14
singular names (`svc_query.ring`, `egress_service.broadcast`) no longer
exist — the node unlinks any leftover from a pre-M14 instance directory at
boot rather than mistaking it for FSM 0's ring.

Every IPC file lives directly under the instance directory. There is no
`/dev/shm` discovery directory.

**Ring file format (2.7.0).** The two client-facing MPSC rings changed
format: each record now carries its own commit word (a lap stamp plus a
length) instead of being published in claim order through a shared cursor,
which is what removed the producer convoy documented in
[the convoy explainer](../notes/uc2-m13-mpsc-publish-convoy-explained.md).
The file magic changed with it (`ULTRNG2`), so a process built before 2.7.0
and one built after **cannot share an instance directory**: the older one's
ring file is refused with a magic mismatch rather than misread. The node,
the service, the gateway and every shmem client on a host therefore restart
together on this upgrade — see
[Upgrade a cluster](../how-to/upgrade-a-cluster.md). The rings are volatile
(recreated on boot), so there is nothing to migrate.

## Durability classes

| Class | Paths | Requirement |
|---|---|---|
| Durable | `journal/`, `state/`, `snapshots/` | Must survive power loss. |
| Durable, node-local | `audit.jsonl` | Must survive power loss; **not** replicated and not part of a backup's consistency story — each node records only what it itself answered. |
| Volatile-safe | `cnc2.dat`, `log.buf`, all `*.ring` and `*.broadcast` files | Rebuilt or re-primed on boot. |

`audit.jsonl` is opened `O_APPEND` at node start (a node that cannot open it
refuses to start) and every record is `fsync`ed **before** the answer it
describes is published, so an answer that reached an operator is always on
disk here. There is no rotation: admin operations are operator-rate — tens a
year on a busy cluster, ~200 bytes each — so the file does not grow without
an operator's own actions. Truncating or archiving it is a deliberate,
offline decision; the node never does it. The `fsync` is paid on the
consensus thread, once per admin request, and nothing on the duty-cycle hot
path touches the file.

`state/` holds the vote and term map. It must never be discarded or reset
while the node retains its id.

These three durable paths are exactly what [Back up a cluster](../how-to/back-up-a-cluster.md)
copies — in that order, `journal/` fully before `state/` before `snapshots/`,
which is load-bearing, not incidental (see that page for why). The volatile
row below is never copied and never needs to be: a node's next boot recreates
every file in it unconditionally, whether after an ordinary restart or after
a restore.

The durable paths all live under the instance directory, so the directory as a
whole must sit on a real filesystem. An instance directory on `tmpfs` makes
every `fsync` a no-op.

`bench-infra/scripts/m6_fleet_gate.py` enforces this: it runs `stat -f` on
every instance-directory parent, local and remote, and refuses to run on
`tmpfs` or `ramfs`.

Splitting the rings onto `tmpfs` while keeping the durable subdirectories on
disk requires bind mounts.

## Limits

| Limit | Value |
|---|---|
| Free space needed before boot | `buffer_bytes` + 14 MiB of rings + **5 MiB × (N − 1)** for N declared FSMs (`svc_query.<id>.ring` 1 MiB + `egress_service.<id>.broadcast` 4 MiB each) + 4 KiB for the second cnc page — ~78 MiB at the defaults with one FSM, ~113 MiB with eight; reserved at startup — see below |
| Nodes per instance directory | 1, enforced by `instance.lock` |
| Admin clients per instance directory | 1 at a time |

## On-disk footprint

`cnc2.dat`, `log.buf`, and every `*.ring`/`*.broadcast` file have their blocks
**reserved when the node creates them**, not allocated lazily as they are
written. They are memory-mapped, and a write to a page with no block behind it
raises `SIGBUS` — a hard process kill that no error path can intercept, taking
the node, a service, or a client with it. Reserving up front moves that
failure to `fallocate`, where it is an ordinary `ENOSPC` the daemon reports as
a startup refusal.

The practical consequence: `du` on a fresh instance directory shows the full
`buffer_bytes` plus ring sizes immediately, and the filesystem must have that
much free before the node will boot.
