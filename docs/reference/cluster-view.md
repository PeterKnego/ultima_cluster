# The cluster view

The cluster view is one versioned JSON document that answers "what is this
cluster, and what is this node holding?": the committed cluster state
(membership, settings, the schedule table, per-row upgrade state, the snapshot
catalog, backup tiers, the purge gate), this node's own holdings and services,
and — on the leader — every member's holdings. It is read-only and carries an
`attention` list naming the conditions the node already detects. The schema
string is `uc2.cluster-view/1`.

Two doors serve the same document:

- **`<instance_dir>/obs.sock`** — a unix socket, mode `0600`, always bound by
  the `uc2-node` daemon. Trust is host access plus the file mode, the model
  `cnc2.dat` and `uc2ctl` already use. `GET /cluster` over it.
- **`GET /cluster` on `[metrics]`** — the same route on the existing
  unauthenticated metrics listener (only when `[metrics]` is configured). See
  [Monitor a cluster](../how-to/monitor-a-cluster.md).

`uc2ctl cluster` prints the whole document (text or `--json`) and `uc2ctl
catalog` its storage part; both fall back to the newest cluster artifact when
the node is stopped. See [`uc2ctl cluster`](uc2ctl.md#cluster) and
[`uc2ctl catalog`](uc2ctl.md#catalog). An application that embeds `Node`
without the daemon calls `uc_node::view::render_live(&ObsSources)` and serves
the string however it likes.

## Conventions

- **Hashes** are `"0x"` plus 16 lowercase hex digits. **Tier ids** are the
  same shape (`"0x"` plus 16 lowercase hex digits), as `uc2ctl tier show`
  prints them and `uc2ctl tier forget --tier-id` takes them.
- **`DecU64`**: a `u64` that can exceed 2^53 is a decimal **string**
  (`"18446744073709551615"`): `settings.admission_bytes`,
  `snapshot_interval_bytes`, `backup_retain_age_secs`, `fsm_lag.bytes`, a
  schedule entry's `timer_id`, `period_ns`, `anchor_ns` and `at_ns`. Parse
  these as integers, not floats.
- **Positions and byte counts** are JSON numbers. They are exact in every
  parser up to 2^53 (9007199254740992), which is 8 PiB (about 9 PB) of log; the
  document never carries a number above it.
- **Times** are RFC 3339 UTC with nine fraction digits, always:
  `2023-11-14T22:15:00.000000000Z`. Durations are integers with the unit in
  the name (`age_ms`, `period_ns`, `backup_retain_age_secs`).
- **Words, not codes.** Enums are lowercase snake_case strings. Rows carry
  both `row` (the id) and `name`.
- **`null` means absent**, never a sentinel `0`.
- **`attention` context** (`set`, `row`, `tier`, `lag_bytes`, `node`) is
  **omitted** when it does not apply to a code, not `null`.
- Unknown fields may appear under the same `schema` (see
  [Stability](#stability)); a reader ignores what it does not know.

## Field reference

### Top level

| Field | Meaning |
|---|---|
| `schema` | `"uc2.cluster-view/1"`. |
| `source` | `"live"` (a running node) or `"artifact"` (built offline from the newest cluster artifact). |
| `node_id` | This node's id; `null` offline. |
| `role` | `"leader"`, `"follower"` or `"learner"`; `null` offline. |
| `joined` | Whether this node has joined its cluster (a leader is known, a commit is learned, the cluster FSM has walked to it) — the gate every attach door waits on. Mirrors `services_declared`: `false` while the node has row names but has declared none. `null` offline. |
| `leader` | `{ id, addr }` of the leader this node believes in; `addr` is `null` when the leader is not a member this node knows. `null` offline or with no leader. |
| `view_position` | The cluster FSM's walk cursor (`consumed`): the log position its state reflects. Offline, the artifact's position. |
| `commit` | This node's commit position; `null` offline. |
| `read_at` | When the document was rendered (RFC 3339). |
| `cluster` | The committed cluster state, below. |
| `self` | This node's holdings and services; `null` offline. |
| `nodes` | Every member's holdings; **leader only** (see [Who fills what](#who-fills-what)), else `null`. |
| `nodes_from` | `"leader"` on a live non-leader — "ask the leader for `nodes`" — else `null`. |
| `attention` | A list of `{ code, ...context }`; see [Attention codes](#attention-codes). |

### `cluster.membership`

| Field | Meaning |
|---|---|
| `config_version` | The committed membership's config version. |
| `members[]` | `{ id, addr, role }`; `role` is `"voter"` or `"learner"`. |

### `cluster.settings`

The committed settings record (`uc2ctl settings show`).

| Field | Meaning |
|---|---|
| `position` | Frame-end position of the committed record. |
| `fsm_lag` | `{ "mode": "derived" }`, `{ "mode": "lockstep" }` or `{ "mode": "bytes", "bytes": "<DecU64>" }`. |
| `admission_bytes` | The admission window (`DecU64`; `"0"` = derived). |
| `snapshot_interval_bytes` | Snapshot cadence (`DecU64`; `"0"` = no cadence). |
| `snapshot_target` | `"all"` or `"learners"`. |
| `datagram_mtu` | The raw replicated word; `0` is the baseline rung. |
| `datagram_mtu_rung` | `"baseline"` when `datagram_mtu` is `0`, else `"discovered"`. |
| `retain_sets` | How many agreed snapshot sets the cluster keeps. |
| `auto_fetch` | Whether every node fetches the newest agreed set in the background. |
| `backup_retain_sets` | Backup tier retention by count (`0` = no rule). |
| `backup_retain_age_secs` | Backup tier retention by age (`DecU64`; `"0"` = no rule). |
| `purge_requires_tiers` | K, the number of backup tiers a purge waits for (`0` = off). |
| `wire_level` | The committed cluster wire level, the wire minor every member must speak; wire-level spec §2.2. |
| `wire_raise` | Who raises the level: `1` = auto (the leader), `2` = manual (`uc2ctl wire raise`); `0` never appears in the view. |

### `cluster.schedule`

| Field | Meaning |
|---|---|
| `position` | Frame-end position of the committed table. |
| `entries[]` | `{ row_name, identity_hash, timer_id, rule }`. `row_name` is the row whose identity hash matches, `null` when no name matches. `timer_id` is a `DecU64`. |

`rule` is tagged by `kind`: `{ "kind": "every", period_ns, anchor, anchor_ns }`,
`{ "kind": "daily_at", secs_of_day }` (UTC) or `{ "kind": "once", at, at_ns }`.
`anchor` and `at` are the RFC 3339 renderings of `anchor_ns` and `at_ns`.

### `cluster.upgrades[]`

One element per declared row.

| Field | Meaning |
|---|---|
| `row`, `name` | The row id and its name (`null` when unknown). |
| `pins[]` | Every pin on the row, oldest first: `{ from, to, origin }`. `from`/`to` are versions; `origin` is the pin's ORIGIN position. |
| `running` | `{ version, record_pos }`: the row's committed running version and the position of the record that set it; `null` before any record. |
| `report` | The row's live snapshot-hash verdict, or `null`: `{ position, verdict, majority_hash, minority, nodes[] }`. `verdict` is `"agreed"`, `"diverged"` or `"no_majority"`; `minority` lists node ids off the majority; `nodes[]` is `{ node, hash, size }`, `size` `null` when the report carries none (a pre-lifecycle record). |

A **version** is `{ text, packed }`: the text an operator reads
(`unversioned`, or a `major.minor` form) and the packed word tools compare.

### `cluster.catalog`

| Field | Meaning |
|---|---|
| `retain_sets` | The retention setting. |
| `empty` | Live: `true` while no set has ever completed, else `false`. Offline: `false` when the artifact records an earlier set as complete, else `null` — **never `true`**, because the artifact cannot record its own set's completion ([Offline](#offline)). |
| `floor` | The newest agreed set (the cluster purge floor); `null` when none. |
| `sets[]` | Oldest first; at most `retain_sets` plus the commanded ones. |

Each **set**:

| Field | Meaning |
|---|---|
| `position` | The instant's position **P**. |
| `kind` | `"full"` or `"standby"` (learners-only). |
| `commanded_at` | When the instant was commanded (RFC 3339). |
| `state` | `"commanded"` or `"complete"`. |
| `agreed` | Every declared row and the cluster artifact reported the same hash. |
| `stalled` | Commanded **and superseded** by a newer entry. |
| `diverged_rows` | Names of rows whose verdict is `diverged` (`"cluster"` for the cluster artifact, `"row<N>"` for a row with no known name). |
| `size` | Total bytes, or `null` while unreported. |
| `rows[]` | Declared rows only: `{ row, name, version, hash, verdict, size }`. `verdict` is `"unreported"`, `"agreed"`, `"diverged"` or `"no_majority"`; `version`, `hash` and `size` are `null` while unreported. |
| `cluster` | The cluster artifact's entry, same shape; `row` is `255`. |
| `held_by` | Leader only: node ids holding this set complete on disk. `null` elsewhere. |

### `cluster.tiers[]`

One element per registered backup tier (at most 4).

| Field | Meaning |
|---|---|
| `id` | The tier id, `"0x"` plus 16 lowercase hex digits. |
| `oldest_set`, `newest_set` | The tier's watermark range of sets. |
| `through` | The log position the tier confirms. |
| `reporter` | The node that last reported the watermark. |
| `updated_position` | The log position of that report. |
| `lag_bytes` | `commit - through`, computed against **this node's** commit (the header's `commit`) on every node; `null` offline. |

### `cluster.purge_gate`

| Field | Meaning |
|---|---|
| `requires_tiers` | K (`purge_requires_tiers`). |
| `position` | `null` when K = 0; otherwise the gate position. `0` means "holds every purge": fewer than K tiers report (see `tiers_reporting`). |
| `tiers_reporting` | How many tiers have reported. |
| `released` | This node's low-space release level is set (it purged past the gate); `null` offline. |

### `self`

| Field | Meaning |
|---|---|
| `holdings` | This node's holdings, below. |
| `services[]` | One per declared row; **empty until the node has joined**. |

**Holdings**: `journal_first`, `durable`, `commit`, `applied[]`
(`{ row, name, applied }`), `free_bytes`, `journal_bytes`,
`snapshots_bytes`, and `sets_held`. `free_bytes` is `null` until the node's
first successful free-space reading (a reading is never `0`).
`journal_bytes` (an estimate from the archive's positions, rounded to whole
segments) and `snapshots_bytes` are byte counts in which `0` is a real
reading — an empty journal, an empty `snapshots/` — so they are never
`null`; they also read `0` in the moment before the node's first disk
probe, which runs as the node starts. `sets_held` — the set positions this node holds
complete on disk. `sets_held` is `null` when the holdings were computed
against a different catalog than the one rendered; such a node also appears
in no `held_by`, rather than being mislabelled.

**Service**: `row`, `name`, `attached`, `version` (`null` when unattached),
`applied`, `lag_bytes`, `timers_pending`, `pinned` (a pin is in force) and
`pinned_version`.

### `nodes[]`

| Field | Meaning |
|---|---|
| `id`, `addr`, `role` | From the committed membership. |
| `holdings` | The member's last-reported holdings (same shape as `self.holdings`); `null` for a member the leader has never heard from. |
| `age_ms` | Milliseconds since the leader last heard it; `null` when never heard. |
| `stale` | `true` when not heard for more than three maximum election timeouts, and for a member never heard from. |

## Who fills what

- **`self`, `cluster` and the header** are filled on every live node. A
  follower's `cluster` is its own committed view, so it can trail the
  leader's by a moment; `view_position` and `commit` say where.
- **`nodes` and every set's `held_by` are leader only.** The leader lists
  **every committed member**: one it has never heard from has `holdings:
  null`, `age_ms: null`, `stale: true`. Any other live node has `nodes: null`
  and `nodes_from: "leader"`; a **demoted** leader prints none (its old soft
  table is never shown).
- **`self.services` is empty until `joined`** — before the node has joined
  its cluster it has declared no rows.
- **`view_position` versus `commit`.** `view_position` is how far the
  cluster FSM has applied; `commit` is how far this node knows the log is
  committed. They differ while the cluster FSM catches up, and `view_position`
  also trails `commit` whenever the log ends in frames the cluster FSM has
  not consumed yet.
- To see the whole cluster, ask the leader (`leader.addr` on any node's
  document names it).

## Attention codes

Only conditions the node already detects, each with a stable code. A new
code is an additive change.

| Code | Context | Meaning | Source | Read |
|---|---|---|---|---|
| `catalog_empty` | — | No set has ever completed. Live only; never raised from an offline read. | `uc2_catalog_empty` | [runbook](../ops/uc2-runbook.md#cluster-view-attention-codes) |
| `set_stalled` | `set` | The set is commanded and a newer entry exists: it was superseded without completing. The newest commanded set is in progress, not stalled, so this is narrower than `uc2_catalog_stalled`, which counts every commanded entry. | `Uc2SnapshotStalled` | [runbook](../ops/uc2-runbook.md#cluster-view-attention-codes) |
| `set_diverged` | `set`, `row` | Replicas reported different hashes for that row of that set. | `Uc2SnapshotSetDiverged` | [runbook](../ops/uc2-runbook.md#cluster-view-attention-codes) |
| `purge_gate_released` | — | This node released the backup-gated purge hold for lack of disk. | `Uc2BackupGateReleased` | [runbook](../ops/uc2-runbook.md#when-something-is-wrong) |
| `tier_behind` | `tier`, `lag_bytes` | `commit - through` exceeds 256 MiB (`TIER_BEHIND_BYTES`, pinned to the alert's `uc2_backup_lag_bytes > 268435456`; the alert adds `for: 15m`). | `Uc2BackupBehind` | [runbook](../ops/uc2-runbook.md#when-something-is-wrong) |
| `node_stale` | `node` | Leader's document only: a member's holdings are stale or never heard. | the soft table's staleness bar | [runbook](../ops/uc2-runbook.md#cluster-view-attention-codes) |

## Size bound

Every list is bounded: sets by `retain_sets` plus the commanded ones (at most
`MAX_CATALOG_SETS = 64`), rows by 8, members by 8, tiers by 4, schedule
entries by 32. The worst case — 64 sets of 8 rows, 8 members, 4 tiers, a full
schedule, full pins and reports, 8 services — renders **411342 bytes**; a test
renders it and fails above 512 KiB. A response is built per request on the obs
thread, never on a hot-path agent.

## Example

A leader's document (`uc_node/src/view/example.json`, which a golden test
pins):

```json
{
  "schema": "uc2.cluster-view/1",
  "source": "live",
  "node_id": 1,
  "role": "leader",
  "joined": true,
  "leader": {
    "id": 1,
    "addr": "127.0.0.1:7001"
  },
  "view_position": 4000,
  "commit": 4096,
  "read_at": "2023-11-14T22:15:00.000000000Z",
  "cluster": {
    "membership": {
      "config_version": 3,
      "members": [
        {
          "id": 1,
          "addr": "127.0.0.1:7001",
          "role": "voter"
        },
        {
          "id": 2,
          "addr": "127.0.0.1:7002",
          "role": "voter"
        },
        {
          "id": 3,
          "addr": "127.0.0.1:7003",
          "role": "voter"
        }
      ]
    },
    "settings": {
      "position": 0,
      "fsm_lag": {
        "mode": "derived"
      },
      "admission_bytes": "0",
      "snapshot_interval_bytes": "0",
      "snapshot_target": "all",
      "datagram_mtu": 0,
      "datagram_mtu_rung": "baseline",
      "retain_sets": 1,
      "auto_fetch": true,
      "backup_retain_sets": 0,
      "backup_retain_age_secs": "0",
      "purge_requires_tiers": 0,
      "wire_level": 12,
      "wire_raise": 1
    },
    "schedule": {
      "position": 0,
      "entries": []
    },
    "upgrades": [
      {
        "row": 0,
        "name": "kv",
        "pins": [],
        "running": {
          "version": {
            "text": "unversioned",
            "packed": 0
          },
          "record_pos": 64
        },
        "report": null
      }
    ],
    "catalog": {
      "retain_sets": 1,
      "empty": false,
      "floor": 1024,
      "sets": [
        {
          "position": 1024,
          "kind": "full",
          "commanded_at": "2023-11-14T22:14:55.000000000Z",
          "state": "complete",
          "agreed": true,
          "stalled": false,
          "diverged_rows": [],
          "size": 15,
          "rows": [
            {
              "row": 0,
              "name": "kv",
              "version": {
                "text": "unversioned",
                "packed": 0
              },
              "hash": "0x00000000000000aa",
              "verdict": "agreed",
              "size": 10
            }
          ],
          "cluster": {
            "row": 255,
            "name": "cluster",
            "version": {
              "text": "unversioned",
              "packed": 0
            },
            "hash": "0x00000000000000bb",
            "verdict": "agreed",
            "size": 5
          },
          "held_by": [
            1,
            2
          ]
        }
      ]
    },
    "tiers": [],
    "purge_gate": {
      "requires_tiers": 0,
      "position": null,
      "tiers_reporting": 0,
      "released": false
    }
  },
  "self": {
    "holdings": {
      "journal_first": 0,
      "durable": 0,
      "commit": 4096,
      "applied": [
        {
          "row": 0,
          "name": "kv",
          "applied": 0
        }
      ],
      "free_bytes": null,
      "journal_bytes": 0,
      "snapshots_bytes": 0,
      "sets_held": [
        1024
      ]
    },
    "services": [
      {
        "row": 0,
        "name": "kv",
        "attached": true,
        "version": {
          "text": "unversioned",
          "packed": 0
        },
        "applied": 4096,
        "lag_bytes": 0,
        "timers_pending": 0,
        "pinned": false,
        "pinned_version": null
      }
    ]
  },
  "nodes": [
    {
      "id": 1,
      "addr": "127.0.0.1:7001",
      "role": "voter",
      "holdings": {
        "journal_first": 0,
        "durable": 0,
        "commit": 4096,
        "applied": [
          {
            "row": 0,
            "name": "kv",
            "applied": 0
          }
        ],
        "free_bytes": null,
        "journal_bytes": 0,
        "snapshots_bytes": 0,
        "sets_held": [
          1024
        ]
      },
      "age_ms": 0,
      "stale": false
    },
    {
      "id": 2,
      "addr": "127.0.0.1:7002",
      "role": "voter",
      "holdings": {
        "journal_first": 0,
        "durable": 0,
        "commit": 4096,
        "applied": [
          {
            "row": 0,
            "name": "kv",
            "applied": 0
          }
        ],
        "free_bytes": null,
        "journal_bytes": 0,
        "snapshots_bytes": 0,
        "sets_held": [
          1024
        ]
      },
      "age_ms": 100,
      "stale": false
    },
    {
      "id": 3,
      "addr": "127.0.0.1:7003",
      "role": "voter",
      "holdings": {
        "journal_first": 0,
        "durable": 0,
        "commit": 0,
        "applied": [
          {
            "row": 0,
            "name": "kv",
            "applied": 0
          }
        ],
        "free_bytes": null,
        "journal_bytes": 0,
        "snapshots_bytes": 0,
        "sets_held": []
      },
      "age_ms": 5000,
      "stale": true
    }
  ],
  "nodes_from": null,
  "attention": [
    {
      "code": "node_stale",
      "node": 3
    }
  ]
}
```

## Stability

The serde types in `uc_node::view::model` **are** the schema.

- **`/1` additive rules.** Adding a field or an `attention` code keeps
  `uc2.cluster-view/1`. A reader must ignore unknown fields and unknown codes.
- **`/2`.** Renaming or removing a field, or changing a field's meaning or
  type, bumps the schema to `uc2.cluster-view/2`. `uc2ctl` reads exactly one
  schema and **refuses a foreign one by name** (it says which schema the node
  served and which it reads), rather than misparsing it.
- A checked-in example and a golden test fail on any accidental change.

## For a GUI or an agent

Supply your own list of node URLs, as a Prometheus scrape config does — the
cluster does not serve a roster for you. Poll each node's `/cluster` (or the
socket); the **leader's** document is the only one with `nodes` and `held_by`,
so find it from any node's `leader.addr`. A GUI should expect the leader to
change, and treat a node's absence as a data point. The document discloses
the same class of information as `/metrics` in more detail (member addresses,
settings, hashes, disk figures); keep `[metrics]` on the ops network.

The node serves both doors from **one thread**, one connection at a time. A
connection's read phase is bounded by a 2 s wall-clock deadline, but the
write phase is bounded per write call: a peer that reads a large `/cluster`
body a little at a time, just often enough, can hold that thread — and with
it every `/metrics`, `/healthz` and `/readyz` request — for several of those
2 s periods. That is one more reason to keep `[metrics]` on the ops network
and to poll the view at a scrape's pace, not in a tight loop.

There are **no writes over HTTP**: changes still go through `uc2ctl` and the
signed admin band. The open questions for an HTTP write path (authentication,
replay, TLS, CORS) are the backlog item [Admin writes over HTTP (for a
GUI)](../BACKLOG.md).

## Offline

With no live node, `uc2ctl` builds the same document from the newest cluster
artifact (`snapshots/cluster/snap-<pos>.ultcluster`) through the same builder:

- `source: "artifact"`; `view_position` is the artifact's position.
- `node_id`, `role`, `joined`, `leader`, `commit`, `self`, `nodes` and
  `nodes_from` are `null`; `tier.lag_bytes` and `purge_gate.released` are
  `null`.
- Row names come from a stale `cnc2.dat` when it opens; otherwise names are
  `null` and the text says `row<N>`.
- The artifact at P is cut **at** the instant, so it records its own set P as
  `commanded` — that set's completion commits after P. Offline `floor` and
  the newest set's `state` therefore trail by one set, and the text marks
  that set `(cut at this artifact; its completion is not visible offline)`.
- For the same reason offline `empty` is `false` when the artifact records a
  set before P as complete, and `null` (unknown) otherwise — never `true`,
  since P itself may well have completed. The text prints
  `empty=unknown (offline)`, and the offline document never raises
  `catalog_empty`.
- `attention` is limited to what the artifact alone shows.
- With neither a node nor an artifact, `uc2ctl` says `no cluster artifact
  yet`.
