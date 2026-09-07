# Run work on a schedule

You want the cluster to do something at a wall-clock time it survives a
restart, a failover and a node rebuild — a nightly reconciliation, an hourly
sweep — without a cron box outside the cluster deciding when.

The mechanism is the **replicated schedule table**: a small table of
recurrences that lives in the log, is applied identically by every node, rides
the snapshot session to a joiner, and fires into your state machine's
`on_timer`. Since the cluster FSM (2.11 pending) the table is a record inside
the node's own internal state machine — applied at commit, snapshotted in the
cluster artifact — which is why the paragraphs below say "applied" where an
earlier draft said "adopted from the archive walk". Nothing in the procedure
changed; see [the cluster FSM explainer](../notes/uc2-cluster-fsm-explained.md). This guide is the operator half. The FSM side — implementing
`on_timer`, and scheduling from inside `apply` — is
[Schedule work inside a state machine](schedule-work-in-a-service.md).

**Prerequisite:** the FSM must already implement `on_timer`. A table entry
naming an FSM that ignores timers is applied and ticks into a default method
that does nothing.

## Write the table

One TOML file describes the **whole** table. There is no add or remove verb —
applying replaces everything.

```toml
[[schedule]]
fsm    = "orders"                 # a name from the cluster's [services] names
id     = 1000                     # the id this FSM's on_timer will see
every  = "1h"
anchor = "2026-01-01T00:00:00Z"   # RFC 3339, UTC — occurrences step from here

[[schedule]]
fsm = "orders"
id  = 1001
at  = "14:00"                     # daily, UTC

[[schedule]]
fsm  = "billing"
id   = 1002
once = "2026-12-24T18:00:00Z"     # fires once, then parks
```

Exactly one of `every` / `at` / `once` per entry. Full syntax, including the
duration suffixes and what is refused, is
[`uc2ctl` § `schedule apply`](../reference/uc2ctl.md#schedule-apply).

Two things to decide before you write it:

- **Reserve an id range.** Table ids share the FSM's id space with the ids its
  `apply` schedules programmatically. Colliding with a programmatic id means
  one replaces the other. Picking a high band — 1000+ above — costs nothing.
- **Everything is UTC.** There are no timezones and no cron syntax. "14:00
  local" is not expressible; convert it yourself, and remember that a fixed UTC
  time drifts against local wall-clock across a DST boundary.

## Apply it against the leader

```sh
uc2ctl schedule apply schedule.toml --instance-dir /srv/uc2/n0 --app-id myapp
```

This is **leader-only**, and not because of a policy choice: `uc2ctl` stages
the encoded table as a node-local file (`<instance_dir>/schedules.pending`) and
signs its digest into the admin request, so a follower has no way to read the
file it would be forwarding. A follower answers `retry` with a leader hint —
re-run the same command against the node it names.

The same `retry` comes back if a previous table frame is still above the commit
position (only one table may be in flight). `uc2ctl` does not poll through it;
it prints the staged path and exits non-zero. Re-run the command.

On success it prints a `version` word that is the **frame-end position of the
new table**, not a config version. Note it: that number is what every node
should converge on.

## Verify every node adopted it

Adoption is per node, so check more than the leader:

```sh
uc2ctl schedule show --instance-dir /srv/uc2/n0 --app-id myapp
```

```
position=8192
fsm=orders id=1000 rule=every 1h anchor 2026-01-01T00:00:00Z
fsm=orders id=1001 rule=at 14:00:00
```

`position` must match the `version` the apply printed, on **every** node once
caught up. The Prometheus equivalent — and the right thing to alert on — is
`uc2_schedule_table_position`, with `Uc2ScheduleTableDiverged` firing when
nodes disagree. See
[Monitor a cluster](monitor-a-cluster.md#the-log-clock-and-the-timer-families-211-pending).

`schedule show` reads the node's newest **cluster artifact**
(`snapshots/cluster/`), not the staged file. That is a file beside the running
node, so it lags: an artifact is written at a **snapshot instant** — one the
operator commanded with `uc2ctl snapshot`, or one the replicated
`snapshot_interval_bytes` cadence issued — and until the first of those the
command prints `no cluster artifact yet` even though the table is committed and
ticking. On a cluster that is not snapshotting yet,
`uc2_schedule_table_position` from `/metrics` is the live reading — it is
published straight off the cluster FSM's view. See
[Keep the journal from growing without bound](bound-journal-growth.md#take-a-snapshot-command-an-instant-or-set-a-cadence).

## When an apply is refused

Every outcome, accepted or refused, lands in `audit.jsonl` as `schedule_apply`
— with the digest rendered into the `id`/`addr` fields rather than an address.
The four refusals specific to this op are `40 schedule_digest`,
`41 schedule_missing`, `42 schedule_decode` and `43 schedule_unknown_fsm`,
tabulated with their remedies in
[`uc2ctl` § refusal codes](../reference/uc2ctl.md).

The one worth understanding rather than looking up is **43**: an entry naming
an FSM this cluster does not declare refuses the **whole** table, never
partially. A partially-adopted table would leave you believing a timer is armed
that no row will ever fire. `uc2ctl` resolves names locally before staging
anything, so a typo is normally caught before a request is sent at all.

A refused or timed-out apply **leaves the staged file in place**, so a retry
needs nothing re-staged.

## What happens across restarts and failures

- **Downtime produces one catch-up tick per entry, not a backlog.** A due entry
  fires at the *latest* occurrence at or before the log's clock. A cluster down
  for an hour with a one-minute rule fires **one** tick on recovery and
  continues from it.
- **A restart may duplicate one tick per entry.** `uc_service::Timed<S>` drops
  it; an FSM without the wrapper sees it.
- **A `once` entry parks after firing.** It stays in the table as delivered, so
  re-applying the same file does not re-fire it. Changing its time or its id
  makes it a new entry, which does fire.
- **A joiner installs the table from the snapshot session** before it can serve
  or lead — inside the cluster FSM's own artifact, tagged with the position it
  was committed at. The two ship-side windows an earlier draft of this feature
  documented (a restarted node under-shipping, and a wiped node's table not
  propagating) are **closed**: there is no live read and no commit gate on the
  ship path any more.

Keeping the TOML in version control beside your `node.toml` is still worth
doing — a re-apply is the remedy for a refusal, and the file is the only record
of what you meant to schedule.

## Capacity

**32 entries**, across every FSM, is the hard cap (`MAX_SCHEDULE_ENTRIES`, a
source constant with no knob). A full table is 1064 bytes, which is what makes
it fit one datagram — the reason the cap exists. Per-tick work is bounded by
the same `TIMERS_PER_PASS` = 64 as programmatic timers.

## Related

- [Schedule work inside a state machine](schedule-work-in-a-service.md) — the
  `on_timer` these ticks arrive at.
- [`uc2ctl` reference](../reference/uc2ctl.md) — full `schedule apply` /
  `schedule show` syntax and the refusal table.
- [Monitor a cluster](monitor-a-cluster.md) — the `uc2_schedule_*` metrics and
  the divergence alert.
- [Log time and timers, explained § The schedule
  table](../notes/uc2-log-time-and-timers-explained.md#the-schedule-table) —
  why the table is replicated through the log rather than configured per node.
- [Limits](../reference/limits.md) — the caps and the documented failure
  windows in one place.
