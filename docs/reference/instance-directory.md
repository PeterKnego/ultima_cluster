# Instance directory

One node owns one instance directory. The service and any clients attach to the
same directory, on the same host, through shared memory.

The directory path is passed to `Node::start` and to every `uc2ctl` invocation.

## Files

| Path | Owner | Contents |
|---|---|---|
| `instance.lock` | node | Exclusive `flock`. A second node on the same directory is refused with `AlreadyRunning`. Service and clients take a shared lock as a liveness probe. |
| `cnc2.dat` | node | The 4 KiB control page. See [The cnc control page](cnc-page.md). |
| `log.buf` | node | The log ring buffer, `buffer_bytes` long. Recreated on each boot. |
| `journal/` | node | Segmented durable log (`ultima_journal`). Survives restarts; the source for replay and purge. |
| `state/` | node | Raft durables, held as `StableValue`s: vote, term map, output progress, snapshot floor, and the config record. |
| `snapshots/` | service and node | `snap-<pos>.ultsnap` artifacts. The service builds them; the node ships and installs them. `<pos>` is the absolute log byte position the snapshot represents. |
| `ingress.ring` | clients → node | MPSC submit ring. |
| `query.ring` | clients → node | Query submissions, both linearizable and snapshot reads. |
| `svc_query.ring` | node → service | Forwarded queries. |
| `egress_service.broadcast` | node → service | Apply and output stream to the service. |
| `egress_node.broadcast` | node → clients | Submit responses broadcast to clients. |

Every IPC file lives directly under the instance directory. There is no
`/dev/shm` discovery directory.

## Durability classes

| Class | Paths | Requirement |
|---|---|---|
| Durable | `journal/`, `state/`, `snapshots/` | Must survive power loss. |
| Volatile-safe | `cnc2.dat`, `log.buf`, all `*.ring` and `*.broadcast` files | Rebuilt or re-primed on boot. |

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
| Nodes per instance directory | 1, enforced by `instance.lock` |
| Admin clients per instance directory | 1 at a time |
