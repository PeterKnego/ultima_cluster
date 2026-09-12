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
| `journal/` | node | Segmented durable log (`uc_journal`). Survives restarts; the source for replay and purge. |
| `state/` | node | Raft durables, held as `StableValue`s: vote, term map, output progress, snapshot floor, and the config record. These five are exactly `backup`'s `STATE_FILES` checklist. All five are **node data** under the cluster FSM's line (2.11.0): local, never replicated, never snapshotted. `config.state` is the one that looks like an exception and is not — it is the consensus kernel's *durable-time* membership shadow, a different reader at a different time base from the cluster FSM's committed view ([the cluster FSM explainer](../notes/uc2-cluster-fsm-explained.md)). There is **no** `schedules.state`: the schedule table is cluster data and lives in the cluster FSM's artifact. |
| `snapshots/<id>/` | service and node | `snap-<pos>.ultsnap` artifacts for FSM `id`, one directory per declared id since M14. The service builds them; the node ships, installs and **deletes** them. `<pos>` is the absolute log byte position the snapshot represents — an **exclusive** frontier since coordinated instants (2.11.0): the image covers every frame strictly below it. Every file starts with a 16-byte UC envelope (below). A receiver's in-flight download sits beside them as `incoming-<pos>.part`, pre-sized and renamed into place as the contiguous frontier passes its end; an abandoned intake's part files are unlinked. |
| `snapshots/cluster/` | node (`uc2-cluster` agent) | `snap-<pos>.ultcluster` — the **cluster FSM's** artifact (2.11.0): membership, the schedule table and the settings record as of `<pos>`, with a `UCCLUST1` magic, an image version and a trailing CRC32. Written by the node itself, not by a service, and shipped on the snapshot session under the reserved `service_id = 255` so a below-floor joiner installs it before its floor advances. Also what `uc2ctl schedule show`, `uc2ctl settings show` and `uc2ctl status`'s `schedule_position=` read. Retention is the node's, as it is for every row (below); the second-newest is what you fall back to if the newest is corrupt. |
| `ingress.ring` | clients → node | MPSC submit ring. Per-record commit format (`ULTRNG2` magic) since 2.7.0. |
| `query.ring` | clients → node | Query submissions, both linearizable and snapshot reads. Payload is `service_id: u8` — which FSM answers (M14) — followed by the query bytes; same record framing as `ingress.ring`. |
| `svc_query.<id>.ring` | node → service | Forwarded queries for FSM `id`. One per declared id since M14. |
| `svc_sched.<id>.ring` | service → node | Schedule/cancel/consumed requests for FSM `id`'s timers (time-and-timers spec §4.4). One per declared id. Since the cluster FSM (2.11.0) the timer heap is **leader-only**: the service writes to this ring only while its node holds `NODE_FLAG_LEADER`, and only a leading node drains it. A follower's ring therefore stays empty by construction, which is what keeps `write_sched`'s ring-full spin out of a follower's apply thread. |
| `egress_service.<id>.broadcast` | node → service | Apply and output stream to FSM `id`'s service. One per declared id since M14. A client opens every declared id's ring and accepts a response only from the FSM(s) its request named. |
| `egress_node.broadcast` | node → clients | Node-originated answers to clients: `MSG_V2_NOT_LEADER` (with the leader hint), `MSG_V2_RETRY`, and `MSG_V2_BAD_SERVICE` (the query named an id this node has no ring for). Submit and query *responses* come from the FSMs' own rings. |
| `service.<id>.lock` | service | Exclusive `flock`, held for FSM `id`'s service process's life — one process per declared id (M14). |
| `schedules.pending` | admin client → node | The staged schedule table `uc2ctl schedule apply` writes (mode `0600`, fsync, rename) before sending the admin request that carries its digest. Transient: the node reads it, checks the digest, and **deletes it after a successful append**. A refused or timed-out apply leaves it in place so a retry needs nothing re-staged. Present only between a stage and a successful apply. |
| `settings.pending` | admin client → node | The same, for `uc2ctl settings apply` (2.11.0): the encoded 29-byte settings record, staged and digested identically. |
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

### The artifact envelope, and who deletes artifacts

Two things about `snapshots/` changed with coordinated snapshot instants
(2.11.0) and are worth knowing before you touch the directory by hand.

**Every artifact starts with a 16-byte envelope.** `ULTSNAP1` then the
position it was built at, `u64` LE — written by the framework
(`uc_service::snapshots::SnapshotStore::publish`), ahead of whatever bytes the
state machine itself streamed. UC still prescribes **no** payload encoding;
it owns this header only. It exists because the tag is an exclusive frontier,
which makes a mis-tagged artifact undetectable from the payload: an image
built at some earlier `P0` and renamed to `snap-<P>.ultsnap` passes any check
a state machine could write, and installing it would silently leave every
frame in `(P0, P)` unapplied. So every install path strips and verifies the
envelope first — the service's own reconstruction, a joiner's receive (the
session ships the file's bytes verbatim, envelope included), and
`uc2ctl verify-backup`. An artifact that fails is refused by name (too short,
bad magic, or built-at ≠ presented-as), never installed. Artifacts written by
a pre-2.11 build have no envelope and are refused: on a developer box that
means clearing `snapshots/` once, which is part of the same flag day as the
wire bump.

**Retention is the node's, and it only ever deletes.** The node keeps the
complete set at its **persisted** snapshot floor plus everything newer, and
unlinks everything below, matching `snap-<pos>.ultsnap` /
`snap-<pos>.ultcluster` by exact name — so a builder's `.tmp` or a receiver's
`.part` is invisible to the sweep and can never be raced. The old per-writer
"keep the newest 2" pruners are gone from both the service and the cluster
agent, because neither can see a *set*: two abandoned instants in a row would
have had a per-row pruner delete the artifact at the floor, and the ship gate
— "the complete set at my floor" — would then decline every joiner
`missing artifact` forever. The node never writes an artifact; it only ever
deletes one it can prove is superseded.

## Durability classes

| Class | Paths | Requirement |
|---|---|---|
| Durable | `journal/`, `state/`, `snapshots/` | Must survive power loss. |
| Durable, node-local | `audit.jsonl` | Must survive power loss; **not** replicated and not part of a backup's consistency story — each node records only what it itself answered. |
| Volatile-safe | `cnc2.dat`, `log.buf`, all `*.ring` and `*.broadcast` files | Rebuilt or re-primed on boot. |
| Transient request payload | `schedules.pending`, `settings.pending` | Not durable state and not backed up. Losing either costs a re-run of the corresponding `uc2ctl … apply`; the committed table and settings live in the cluster FSM's artifact and on the log. |

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

**`snapshots/cluster/` is copied by `backup` (2.11.0)**, as an artifact
family of its own beside the numeric `snapshots/<id>/` ones — `cluster` is not
a `u8`, so the two are walked separately. `verify-backup` decodes the newest
`snap-<pos>.ultcluster` through the same image decoder a joiner installs it
with (magic, image version, CRC32, every bounds check) and refuses a corrupt
one by name; where the family is present and the journal is purged, its newest
artifact must cover `first_base`, reported as `hole: service 255`. A purged
journal with **no** cluster family is deliberately not a hole. A live node no
longer produces that shape — the purge floor is bounded at `0` until the
`uc2-cluster` agent has written an artifact — but `verify-backup` runs over
directories it did not produce, so it refuses to call one a hole on that
evidence alone.

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
| Free space needed before boot | `buffer_bytes` + 15 MiB of rings + **6 MiB × (N − 1)** for N declared FSMs (`svc_query.<id>.ring` 1 MiB + `svc_sched.<id>.ring` 1 MiB + `egress_service.<id>.broadcast` 4 MiB each) + 4 KiB for the second cnc page — ~79 MiB at the defaults with one FSM, ~121 MiB with eight; reserved at startup — see below |
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
