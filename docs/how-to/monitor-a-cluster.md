# How to monitor a cluster

Wire a running cluster into Prometheus and Grafana, and read the structured
records nodes print to stderr. All of it is optional and off by default — a
node with no `[log]`/`[metrics]` sections behaves exactly as before M10.

## Turn on the endpoint

Add a `[metrics]` section to the node's config file:

```toml
[metrics]
bind = "127.0.0.1:9600"
```

`bind` defaults to `127.0.0.1:9600` if the section is present but empty
(`[metrics]` with no `bind` key). Absent section means no endpoint at all —
the node never opens the port. See
[`packaging/node.example.toml`](../../packaging/node.example.toml) for the
annotated copy.

A bind failure (port already held, say) is a **runtime failure, exit 1** —
the same retried-by-systemd class as any other post-preflight startup error,
not the config-refusal exit 2. See
[Run a cluster on real hosts](run-a-cluster.md#supervise-the-processes) for
what the two exit codes mean to the packaged unit.

### Security note

`/metrics`, `/healthz`, and `/readyz` are **unauthenticated, read-only,
GET-only**, and the server sends `Connection: close` on every response — no
keep-alive, no other verbs to probe. There is no plan to add authentication;
the endpoint is meant to sit behind the same trust boundary as the rest of
the node.

Bind it to loopback or a private address and firewall it at the network
layer, the same posture as an unencrypted cluster's replication port. The
surface it exposes is operational, not payload: byte positions, term
numbers, peer addresses and ids, and counters. No application data, no
command bytes, no client identities ever appear on it.

## Scrape it with Prometheus

One target per node, all on port 9600 (or whatever `bind` you chose):

```yaml
scrape_configs:
  - job_name: uc2
    metrics_path: /metrics
    static_configs:
      - targets:
          - 10.0.0.10:9600
          - 10.0.0.11:9600
          - 10.0.0.12:9600
```

`/metrics` serves `text/plain; version=0.0.4` — standard Prometheus text
exposition. The full series contract — 99 families — is the
`CONTRACT_SERIES` array in
[`uc_node/src/obs/metrics.rs`](../../uc_node/src/obs/metrics.rs); a test
pins every family in that array against what the renderer actually emits, so
it cannot drift silently. This page names only the load-bearing subset: the
lag/saturation/heartbeat-age/peer-lag gauges and the `agent_alive` gauge that
the alert rules below key on, plus the counters they watch for edges. For
everything else — snapshot/session counters, sender/receiver datagram and
byte totals, resync counters — read the source array; each family carries
its own one-line doc comment there.

Five of the snapshot-session counters answer the question "why is this
joiner not converging?", and they split it cleanly between the two ends:

| series | end | means |
|---|---|---|
| `uc2_snapshot_intake_io_failures_total` | joiner | this node's disk: a `.part` that could not be created or written, or a completed artifact whose fsync/rename failed. Retried, so a *rising* count — not a nonzero one — is the signal. Since 2.8.1 a failed publish is retried at most once per 250 ms per transfer, on the duty cycle **and** on the chunk path, so a standing obstacle makes this climb at about four per second — not at the poll rate, and not once per arriving chunk. A transfer whose later artifacts are still streaming past a blocked earlier one is the case that used to make it climb per chunk. |
| `uc2_snapshot_intake_abandoned_total` | joiner | a transfer saw no chunk for 60 s and was dropped: its **unfinished** `.part` files are removed, while an artifact it had already published stays on disk to be adopted or superseded by the next session. On its own this says the leader (or the link) went away mid-transfer, and this node keeps NAKing for a fresh session. **But if `uc2_snapshot_intake_io_failures_total` is rising at the same time the cause is local** — this node's snapshot directory — and the set is being re-downloaded on a ~60 s loop until it is cleared: the bytes land, the install blocks, no further chunk arrives, the sender times out at 30 s, this timer fires at 60 s, and a fresh session starts the whole set again. |
| `uc2_snapshot_open_failed_total` | leader | this node could not open an artifact its own snapshot store had just listed, so it refused to ship the set. A one-off is a purge racing a session; a persistent count means look at *this* node's snapshot directory while a peer is trying to join. |
| `uc2_snapshot_begin_undecodable_total` | joiner | one refused session per count, because the sender's `SNAP_BEGIN` could not be decoded at all — the realistic wire-0.5.0 flag-day shape. Nonzero means the fleet is mixed-version; upgrade every node together. |
| `uc2_snapshot_refused_legacy_peer_total` | joiner | every such datagram, not every session — the leader re-sends a `SNAP_BEGIN` every 20 ms, so this one measures the resend cadence. Read the row above it for "how many sessions". |

Two families worth calling out because their shape is easy to misread:
`uc2_ingress_holes_skipped_total` and `uc2_query_holes_skipped_total`
(both since 2.7.0) are **one counter per client-facing ring** —
`ingress.ring` (submits) and `query.ring` (reads) respectively. They are
deliberately NOT summed: which ring is losing records tells you which
client path to go and look at. Neither has an alert rule of its own;
both are counters you watch for an edge.

They count only the *recoverable* case: a claim abandoned past
`hole_timeout` (default 1 s) by a producer that died or stalled. Read a
nonzero value as **"at least one producer's claim was abandoned"**, not
as "a submit was dropped" — a timed-out tail-padding claim also counts
here and loses nothing at all, and a producer that is still alive is
told `Skipped` when its commit is refused rather than losing the record
silently.

Two *unrecoverable* cases are not counters at all, but fail-stops: a
producer that dies in the few instructions between claiming a slot and
stamping its claim word (`IngressRingWedged`), and a record at the
consumer position that does not decode (`IngressRingCorrupt`). See
[Diagnose a node: my node just fail-stopped with `IngressRingWedged`](diagnose-a-node.md#my-node-just-fail-stopped-with-ingressringwedged).

### The per-FSM families (M14)

Since FSM identity (2.11 pending), the labels below carry the FSM's name too.
A node runs one FSM per declared row (`[services] names`), and every
service family carries a `service="<name>",row="<r>"` label pair per
declared row (before FSM identity this was `service="<id>"` alone — the
`row` label keeps the same meaning the old bare `service` label had, and
existing dashboards' grouping keys by row still work):
`uc_service_applied_bytes`, `uc_service_epoch`,
`uc_service_snapshot_pos_bytes`, `uc_service_heartbeat_age_seconds`,
`uc_service_attached`, `uc_service_lag_bytes` (= `commit − applied`),
`uc_service_lag_waits_total`. Two node-scalar gauges describe the set
itself: `uc_services_declared` (the bitmask — bit *k* = row *k*) and
`uc2_fsm_lag_bytes` (the lag bound; **0 means lockstep**).

**Two new gauges per row (FSM identity, 2.11 pending), exported from the cnc
slot band**: `uc2_service_identity_hash{service="<name>",row="<r>"}` (the
FNV-1a 64 of the row's declared name — an exact float64 sample: 64-bit
integers up to 2^53 round-trip losslessly on the wire, and any two real
FNV-1a 64 hashes would have to agree in their top 53 bits to collide after
scraping) and `uc2_service_version{service="<name>",row="<r>"}` (the
attached service's packed version; `0` = none/unversioned). These are the
**early** guard for the late `SNAP_BEGIN` cross-node check (§2.1 of the
spec) — a cross-node query compares them in steady state, before any
snapshot session ever runs.

Two shapes to know before writing a query:

- **The first four families also carry an unlabeled sample**, which is the
  M10 series and now means **the slowest declared FSM** (page 1's `min` over
  the declared ids — the number the purge floor, the admission door and
  `/readyz` all key on). Aggregate and per-FSM samples live in the same
  family, so `sum(uc_service_applied_bytes)` double counts. Say
  `{service=""}` for the aggregate and `{service!=""}` for the per-FSM rows.
- **A declared FSM that has never attached still renders a row**, reading
  `uc_service_attached{service="k"} 0` with zeros beside it. That is
  deliberate: you cannot alert on a series that is absent, and "declared but
  never started" is the state that silently closes admission cluster-wide.

`uc_service_lag_waits_total{service}` counts wait EPISODES at the lag
barrier — one increment per park, however long the park lasts, so read its
rate as "how often this FSM is held", never as a duration. Under lockstep
that is ≈ one per frame. Under bounded mode it was an **undercount before
2.8.1**: the apply loop counted a wait only when the barrier cap fell on a
frame boundary, so an FSM parked at a cap that sits mid-frame — the common
case, since a byte bound rarely divides the frame stream — reported 0. Since
2.8.1 that park counts too. For the pinned-at-bound signal, use
`uc_service_lag_bytes{service}` — the series `Uc2ServicePinnedAtLagBound`
actually keys on.

Declared sets must match across nodes (spec §8). There is no alert rule for
the bitmask's drift, because it is a query over the fleet rather than a
per-node condition — `count(count_values("v", uc_services_declared))` is `1`
on a healthy cluster and `> 1` the moment two nodes disagree. The dashboard
ships it as the "Declared sets agreeing" stat.

**Since FSM identity (2.11 pending), the same class of drift *does* have
dedicated alert rules**, keyed on the two new gauges above, because they
carry per-row identity rather than a set-membership bit:
`Uc2ServiceIdentityDrift` — `count by (row) (count_values("hash",
uc2_service_identity_hash) by (row)) > 1` — fires the moment two nodes'
row-`r` FSM names disagree, and `Uc2ServiceVersionDrift` — `count by (row)
(count_values("version", uc2_service_version > 0) by (row)) > 1` — fires
when two nodes' row-`r` attached versions disagree (excluding the
unattached/unversioned `0` case, so a joiner whose service hasn't started
yet does not page). Both are per-row fleet queries like the declared-set one
above, **not** a bare `count by (row) (uc2_service_identity_hash) > 1` —
that counts *series* (one per node instance), not distinct values, and pages
permanently on any multi-node cluster.

### The log clock and the timer families (2.11 pending)

Since log time and timers, every log frame carries a leader-written timestamp
and a state machine can schedule callbacks on it
([the explainer](../notes/uc2-log-time-and-timers-explained.md)). Eight new
contract families — five for the clock and the timer set, three for the
replicated schedule table — plus four **off-contract** timing families
(the two histograms below and their `_max` gauges):

| family | type | labels | meaning |
|---|---|---|---|
| `uc2_log_time_ns` | gauge | none | the highest leader stamp the archive on **this** node has recorded: the log's clock, in ns since the Unix epoch. Identical on every node once caught up |
| `uc2_log_time_lag_seconds` | gauge | none | **leader only** (rendered `0` on followers): wall clock minus `uc2_log_time_ns` |
| `uc2_timers_pending` | gauge | `service`, `row` | pending scheduled timers for that row **on the leader**. The timer heap is leader-only since the cluster FSM (2.11 pending), so a follower always exports `0` — that is the healthy reading, not a gap, and there is deliberately no divergence alert over this family |
| `uc2_timers_fired_total` | counter | `service`, `row` | `TIMER` frames this node appended **as leader** for that row |
| `uc2_timers_late_total` | counter | `service`, `row` | fires whose stamp exceeded their deadline (post-failover, or a deadline already in the past when scheduled) |
| `uc2_timer_lateness_ns` | histogram | `service`, `row` | **off-contract** (see below): wall-clock lateness of every `TIMER` frame this node appended **as leader** for that row — the pass clock minus the fired deadline, i.e. how far past its deadline the pass that *placed* the frame ran. Not the on-the-wire `time_ns - deadline_ns`, which is `0` for every on-time fire by construction. Companion gauge `uc2_timer_lateness_ns_max` |
| `uc2_consensus_pass_ns` | histogram | none | **off-contract**: the interval between consecutive consensus-pass clock readings while this node **leads**, derived from the reading the pass already takes (no clock read of its own). The first pass of a leadership term is skipped, so a promotion contributes no giant sample; a follower's histogram simply stops advancing. Companion gauge `uc2_consensus_pass_ns_max` |
| `uc2_schedule_table_position` | gauge | none | frame-END position of the schedule table this node's **cluster FSM** has applied; `0` = none. The table is cluster-FSM state applied at commit, so this must be identical on every node once caught up |
| `uc2_schedule_entries` | gauge | none | entries in that committed table naming a row **this node declares** — read from the cluster FSM's view, not from the timer heap, so it reads identically on leader and follower even though the heap is leader-only. A parked `once` — one that has already fired — still counts here, unlike `uc2_timers_pending` |
| `uc2_schedule_apply_refused_total` | counter | none | `uc2ctl schedule apply` requests this node refused. **Retries are not counted**: neither a follower's (the staged file is node-local, so the request is never forwarded) nor the leader's while a previous table frame is still above commit |

**One alert rule**, `Uc2LogTimeFrozen` (warning, `for: 30s`):
`uc2_log_time_lag_seconds > 5 and on(instance) uc2_is_leader == 1`. The
`uc2_is_leader` join is what makes it meaningful: the lag series is
leader-only, so without the join a follower's constant `0` would be
indistinguishable from a healthy leader. A firing rule means the leader's wall
clock stepped **backwards** (stamps hold flat at their last value until wall
time catches up, by the monotone clamp) or nothing is being appended at all.
Either way the log's clock has frozen relative to wall time, and every pending
timer is waiting on it.

There is **no per-fire log record**. `timer_late` is emitted only when a fire
is late, because a `stderr` write per timer would sit on the consensus agent's
hot path; the on-time signal is `rate(uc2_timers_fired_total[..])`. There is no
re-arm record either: the heap is leader-only since the cluster FSM, so a
demotion **discards** it rather than re-arming, and a promotion rebuilds it
from the service's re-announce plus the cluster FSM's table view.
`uc2_timers_rearmed_total` and the `timers_rearmed` record are **retired**.

A rising `uc2_timers_late_total` on a cluster that is not changing leaders is
worth a look: either more than `TIMERS_PER_PASS` (64) timers are coming due per
consensus pass, or the leader's passes are being delayed.

**Reading the two timing histograms.** `uc2_timer_lateness_ns` is bounded
below by the pass length — a timer can only be noticed by a pass — so the
useful reading is the pair: `histogram_quantile(0.99, ...uc2_timer_lateness_ns_bucket...)`
against `histogram_quantile(0.5, ...uc2_consensus_pass_ns_bucket...)`. That
ratio is exactly the
[time-and-timers gate's row c](../benchmarks/uc2-time-and-timers-gate-2026-09-03.md)
bar (p99 lateness ≤ 2 × the measured pass length): a p99 well above it means
passes are being delayed, not that the timer heap is slow. Both families are
deliberately **outside** `CONTRACT_SERIES` — that list drives the M10 fleet
gate's coverage row, which queries each entry as an instant Prometheus
expression, and a histogram's family name is not a queryable series (only its
`_bucket`/`_sum`/`_count` are). Both are process-local: they reset when the
node restarts.

**A second alert rule**, `Uc2ScheduleTableDiverged` (warning, `for: 60s`):
`count(count_values("p", uc2_schedule_table_position)) > 1`. The table is
**cluster-FSM state applied at commit**, so every node reaches the same
position by the same route and more than one distinct value across the fleet
means one node is not running the schedule the others are. That matters
cluster-wide rather than per node: only the leader appends timer frames, so if
the node holding the odd table wins an election, every scheduled recurrence in
the cluster stops.

Since the cluster FSM (2.11 pending) this is a **narrow** alert, because the
mechanisms that used to make it fire are gone:

- there is no `state/schedules.state` and no `ScheduleRecord` (retired), so
  the crash-between-record-and-persist window is closed — the cluster agent
  replays the journal;
- there is no revert-on-truncation and no wipe keep-alive, because an
  uncommitted frame is never applied in the first place; the
  `uc2_schedule_entries > 0` with `uc2_schedule_table_position == 0` "wipe
  signature" no longer exists, and neither does the `schedule_table_reverted`
  record;
- a **below-floor join** is not a cause: the snapshot session carries the
  cluster FSM's own artifact (`service_id = 255`), installed before the
  joiner's floor advances, so it holds the cluster's table before it can serve
  a read or win an election. There is no ship-time freshness gate left to get
  wrong.

So a node reading a different position now means its **cluster FSM is not
caught up**: read `uc2_schedule_table_position` beside `uc2_commit_bytes` on
the same node — a table position that is stale while commit is moving is a
`uc2-cluster` agent that is not applying. A node reading `0` while its peers
read nonzero has applied no table at all. The remedy is unchanged: re-run
`uc2ctl schedule apply`, which appends a fresh frame every node applies.

**Two more gauges beside it**, both read off the cluster FSM's published view
at scrape time:

- `uc2_cluster_fsm_position` — the position this node's cluster FSM has
  **consumed** the log up to (its `applied`). Unlike the table and settings
  positions, this advances with the agent's walk over ordinary traffic, so it
  is **not** a fleet-wide constant and nothing alerts on it diverging: it is
  the per-node stall reading. A position sitting still while that same node's
  `uc2_commit_bytes` climbs is a `uc2-cluster` agent that is not applying.
- `uc2_settings_position` — the frame-END of the last `settings apply` this
  node's cluster FSM applied; `0` means the genesis record from `[settings]`
  in `node.toml`, which never crossed the log. Fleet-wide identical once
  caught up, exactly like `uc2_schedule_table_position`. `uc2ctl settings
  show` still prints the settings themselves.

The `uc2_agent_alive` family covers **five** agents — `consensus`, `sender`,
`receiver`, `archive`, and `cluster` (the `uc2-cluster` agent, labelled like
its four siblings without the thread-name prefix).

**Six records** go with them — three at info, three at warn:

| Event | Level | Fields | Means, and what to do |
|---|---|---|---|
| `schedule_table_adopted` | info | `node`, `position`, `entries`, `source` | this node's cluster FSM applied a table at `position` holding `entries` that name a declared row. `source` is `"cluster_fsm"` — the single path since 2.11 (pending), whether the command arrived off the log, off a journal replay, or inside an installed cluster artifact. Nothing to do; this is the healthy signal |
| `schedule_apply_refused` | warn | `node`, `reason` | an `uc2ctl schedule apply` was refused; `reason` is the same 40–43 code [`uc2ctl` prints](../reference/uc2ctl.md#refusal-reasons). Read the code, fix the file or re-run against the leader. Retries (a follower's, or the leader's single-in-flight one) are **not** refusals and do not appear here |
| `schedule_staged_file_kept` | warn | `node`, `position`, `file`, `err` | the command *was* appended, but the staged file could not be deleted afterwards. `file` names which — `schedules.pending` or `settings.pending`, since both apply ops share this path. Deleting it is what normally makes a re-presented request refuse `schedule_missing`/`settings_missing` instead of appending the same payload a second time — remove the file by hand |
| `settings_apply_refused` | warn | `node`, `reason` | a `uc2ctl settings apply` was refused; `reason` is the same 44–47 code [`uc2ctl` prints](../reference/uc2ctl.md#refusal-reasons) |
| `cluster_command_applied` | info | `position`, `kind`, `accepted`, `reason` | this node's cluster FSM applied a `CLUSTER` command at frame-end `position`. `kind` is `1` Membership / `2` ScheduleTable / `3` Settings; `accepted` is `1` or `0`, with `reason` naming the refusal code when it is `0`. A refusal here is **deterministic and identical on every node** — it is the FSM's own validation, not a node-local judgement |
| `cluster_artifact_installed` | info | `position`, `path` | a snapshot session's cluster artifact was installed by fiat at `position`; this node now holds the cluster's membership, schedule table and settings as of that position, before its purge floor advances |

### The snapshot families (2.11 pending)

Since coordinated snapshot instants, a snapshot is something the whole cluster
takes at one log position **P** on the leader's command
([the explainer](../notes/uc2-cluster-fsm-explained.md#instants-one-position-one-set)),
and the purge floor moves only when the **complete set** at P is on disk.
Eight families:

| family | type | labels | meaning |
|---|---|---|---|
| `uc2_snapshot_instant_position` | gauge | none | the last **full** instant this node **commanded as leader**, `0` if never. Leader-local: a follower's reading is whatever it last commanded in some earlier term, so never compare it across instances. A `--standby` instant does **not** advance it — see the next row |
| `uc2_snapshot_standby_instant_position` | gauge | none | the last **standby** instant this node's `uc2-cluster` agent *acted on*, `0` if never. **Learner-only**: a voter skips every standby frame by design, so a voter always reads `0`. This is the gauge to watch on a `snapshot.target = learners` cluster — the leader is a voter, so its own instant gauge and set position tell you nothing about whether the standby work is happening |
| `uc2_snapshot_set_position` | gauge | none | the newest **complete set** this node holds — its purge floor once persisted. `0` until the first one. **Must agree cluster-wide once caught up** |
| `uc2_snapshot_fetched_position` | gauge | none | the newest set this node pulled whole from a learner with `uc2ctl snapshot fetch`, `0` if it never has. The standby return path's progress reading |
| `uc2_snapshot_row_incomplete_total` | counter | `service`, `row` | instants this row **owed a freeze for** and failed to reach before the next one superseded it. The row whose counter climbs is the row stopping all purging. A superseded standby instant on a voter is not counted — that row is *supposed* not to freeze for one |
| `uc2_snapshot_freeze_seconds_max` | gauge | `service`, `row` | the longest `freeze()` this row has reported since the instant its node's rows are working on last advanced — the full one on a voter, the standby one on a learner; reset to `0` on the scrape after that moves |
| `uc2_snapshot_freeze_seconds_sum` | counter | `service`, `row` | cumulative `freeze()` seconds for this row |
| `uc2_snapshot_freeze_seconds_count` | counter | `service`, `row` | DISTINCT freeze durations sampled from this row's cnc word at scrape boundaries — a **lower bound** on freezes, not a count of them (see below) |

The last three are a **stand-in for a histogram**: this exposition encoder has
no histogram type, so a max gauge plus a sum/count pair carries the
distribution's shape (`_sum / _count` is the mean, `_max` the tail). All three
are derived from the cnc slot's `freeze_ns` word once per **scrape**, never
once per pass — two artefacts follow from that. A freeze is counted when the
word's value **differs from what the previous scrape saw**, and the word
carries no sequence number, so a row that freezes for the *exact same*
duration on two consecutive instants is counted once: a known blind spot, and
an under-count rather than a double count. The same mechanism means **N
freezes between two scrapes count once**, at the last one's duration — so on a
cluster whose cadence is faster than the scrape interval, `_count` is well
below the number of instants, and `_sum / _count` is a mean over the freezes
that happened to be *visible*, not over all of them. Both errors point the
same way: `_count` is a floor. And the running totals live in the
node process, so a **node restart resets `_sum`/`_count` to zero** — an
ordinary counter reset, which `rate()`/`increase()` already handle, but not
something to read as "the freezes were undone". The first scrape after a
restart also folds whatever the cnc word happens to hold, which may describe a
freeze from before the restart; that is deliberate, because seeding from the
word instead would miss a genuine freeze landing during startup.

`_max` is the one of the three that resets on purpose: it goes back to `0` on
the scrape after the instant this node's rows are working on advances — the
FULL instant on a voter (`uc2_snapshot_instant_position`), the STANDBY one on
a learner (`uc2_snapshot_standby_instant_position`), whichever is higher.

**Three alert rules.**

`Uc2SnapshotStalled` (warning, `for: 0m`):
`changes(uc2_snapshot_instant_position[30m]) >= 2 and changes(uc2_snapshot_set_position[30m]) == 0`.
This is "one broken FSM silently stops all purging", made loud. It is a
`changes()` count rather than a subtraction on purpose: the two gauges are
**not** comparable positions — the instant position is leader-local and the
set position is each node's own — so the only honest question is whether each
series is *moving*, on its own instance. Remedy: run
`uc2ctl snapshot show` on the leader and look for the row whose `newest=` is
behind, then check `uc2_snapshot_row_incomplete_total{row}` for it. A row that
is merely lagging catches up on its own — a replayed span acts on its last
`SNAPSHOT` frame — so a *persistent* stall means a row that is not
snapshot-capable, busy forever, or whose service process is dead.

`Uc2StandbySnapshotStalled` (warning, `for: 0m`):
`changes(uc2_snapshot_standby_instant_position[30m]) >= 2 and changes(uc2_snapshot_set_position[30m]) == 0`
— the same shape, one gauge over. It exists because
`snapshot.target = learners` makes the *healthy* steady state look like the
failure above: the leader is a **voter**, so it commands instants its own rows
are supposed not to freeze for, and its own set does not complete until
someone runs `uc2ctl snapshot fetch`. So the full-instant rule deliberately
ignores standby instants, and the standby work is watched on the node that
actually does it. A voter exports `0` for the standby gauge and therefore
cannot fire this rule — no role label needed. Firing means one of *that
learner's* rows is not reaching P; the remedy is the same
`uc2ctl snapshot show`, run on the learner. A learner whose standby gauge is
**frozen** is a different fault (it is not receiving the frames at all) and
shows up as `Uc2ReplicationStalled`/`Uc2PeerLagging` on that node.

`Uc2SnapshotSetDiverged` (warning, `for: 60s`):
`count(count_values("p", uc2_snapshot_set_position)) > 1` — the
`Uc2ScheduleTableDiverged` idiom verbatim, over the set position. The newest
complete set's position must agree cluster-wide once every node is caught up,
because it *is* the node's purge floor: two nodes disagreeing means one has
pruned (or will prune) a different prefix than the other. A node behind is
either still catching up (transient) or stuck — and on a cluster taking
`--standby` instants it is also the reading that tells you a voter has not run
`uc2ctl snapshot fetch` yet.

**Snapshot-session refusals.** Five named counters drop a session outright and
leave the joiner NAKing rather than installing a wrong or half set —
`uc2_snapshot_refused_legacy_peer_total`,
`uc2_snapshot_refused_declared_set_total`,
`uc2_snapshot_refused_version_total`, plus two added with instants:
`uc2_snapshot_refused_position_total` (the sender's `SNAP_BEGIN`s disagreed
about the set's position, so it was mixing two instants) and
`uc2_snapshot_refused_fetch_expired_total` (a straggling answer to a
`snapshot fetch` this node had already given up on — nothing is stored or
installed, and the verb is simply re-runnable). All five are in
`CONTRACT_SERIES` and counted in the 99 above. Any of them non-zero means a joiner is stuck; the consensus
agent names each one in a `snapshot_session_refused` record as it happens.

**Eleven record names** go with the families, six at info and five at warn
(paired names share a row below):

| Event | Level | Fields | Means, and what to do |
|---|---|---|---|
| `snapshot_commanded` | info | `node`, `position`, `term`, `standby`, `operator` | this leader appended a `SNAPSHOT` frame at `position`. `standby` says whether only learners freeze; `operator` distinguishes `uc2ctl snapshot` from the cadence. The healthy signal |
| `snapshot_set_complete` | info | `node`, `position`, `source` | the set at `position` is complete on this node and the floor may move. `source` is `"local"` (this node built it) or `"fetch"` (it pulled it whole from a learner) |
| `snapshot_instant_abandoned` | warn | `node`, `position`, `rows` | a new instant superseded one whose set never completed; `rows` names the rows that never arrived. One is ordinary (a row was catching up); a repeat for the same row is what `Uc2SnapshotStalled` pages on |
| `snapshot_cadence_refused` | warn | `node`, `reason`, `detail` | the `snapshot_interval_bytes` cadence tried to issue an instant and was refused — `48`/`49`, with `detail` naming the row. **Latched**: a cluster with one non-snapshotting row would otherwise emit this every pass, forever |
| `snapshot_set_retained` | info | `node`, `position`, `removed`, `errors` | the node's retention sweep unlinked `removed` artifacts below the persisted floor at `position`. `errors` non-zero means a file it meant to delete would not go — check permissions and free space |
| `snapshot_set_held_above_durable` | warn | `node`, `position`, `durable` | a set is on disk at a position above what this node has made durable, so the floor is deliberately **not** moved yet. Transient while the node catches up |
| `snapshot_fetch_requested` / `snapshot_fetch_stored` | info | `node`, `from`, `position` | a `uc2ctl snapshot fetch` was sent to learner `from`, and later landed. The pair brackets the pull |
| `snapshot_fetch_timeout` | warn | `node`, `from`, `position` | the pull got no answer inside the 60 s intake deadline. Nothing was stored; re-run the verb |
| `snapshot_redirect_followed` / `snapshot_redirect_unknown` | info / warn | `node`, `from`, `position` | this joiner was redirected to node `from` for the set at `position` and followed it — or was redirected to a node it does not know, which it dropped. The **sending** side has no record of its own (the redirect leaves `uc_net`, which carries no logging dependency); its witness is the leader's `snap_redirects` counter |

## Install the alert rules

[`packaging/prometheus/uc2-alerts.yml`](../../packaging/prometheus/uc2-alerts.yml)
is a ready-to-load rule file, group `uc2`, evaluated every 15s. Point your
Prometheus (or a remote-write-fed Mimir/Thanos ruler) at it:

```yaml
rule_files:
  - /etc/prometheus/uc2-alerts.yml
```

Verify it loads before shipping it — `promtool` ships in the Prometheus
release tarball (the same class of external dependency as elle's `java`; it
is not part of this workspace's own toolchain):

```bash
promtool check rules packaging/prometheus/uc2-alerts.yml
```

Every rule's `expr` uses only names from `CONTRACT_SERIES` above, and every
`for:` follows the interpretations in
[Diagnose a node](diagnose-a-node.md) — these interpretations ship as alert
rules, not independent judgment calls, so if you disagree with a threshold,
change the rule rather than re-deriving the reasoning from scratch. The
table:

| Alert | Fires when | Severity |
|---|---|---|
| `Uc2AgentDead` | any polling agent's `uc2_agent_alive` reads 0 | critical |
| `Uc2NoLeader` | no node reports `uc2_is_leader == 1`, 30s sustained | critical |
| `Uc2LeaderNotServing` | a node is leader but `can_serve == 0` — the `0x01` flags state | critical |
| `Uc2ServiceWedged` | service heartbeat stale while the node heartbeat is fresh — the apply loop, not the cluster, is stuck | critical |
| `Uc2ReplicationStalled` | append is advancing but commit is not, for 1m — no quorum acknowledging | critical |
| `Uc2PeerNeverHeard` | a peer's reported-durable position has sat at 0 for 2m — usually the bind-address mismatch, not a network fault | warning |
| `Uc2PeerLagging` | a peer's replication lag exceeds the admission window, for 5m | warning |
| `Uc2AdmissionSaturated` | the ingress admission window is ≥90% consumed for 1m — commit is not keeping up with append | warning |
| `Uc2PurgeStalled` | purge is enabled but the journal head lags the snapshot floor by more than 2 segments, for 10m | warning |
| `Uc2RepeatedWipes` | a node wiped-and-rejoined more than once in 10m | warning |
| `Uc2UnattestedReports` | a pre-0.5.0 peer's un-attested durable reports are being counted — a flag-day violation; commits will stall | critical |
| `Uc2CleartextPeer` | cleartext datagrams arrived from a peer while crypto is on — a node missed the wire-crypto flag day | critical |
| `Uc2FollowerSealFailures` | outgoing control frames a **follower** could not seal (check pairwise sessions/allowlist) — a leader's own climb is benign and excluded by the rule | warning |
| `Uc2DiskLow` | `uc2_free_disk_bytes` has sat below 4 journal segments' worth of free space for 2m — the archive fail-stops at `ENOSPC`; purge or grow the disk | warning |
| `Uc2ServiceAbsent` | a declared FSM's `uc_service_attached` has read 0 for 30s — it was never started, or it stopped. Admission is closed and this node's durable report is capped at the lag bound, so the cluster stalls by design until it attaches | critical |
| `Uc2ServicePinnedAtLagBound` | a declared FSM that **is attached** has had its `uc_service_lag_bytes` at or above `uc2_fsm_lag_bytes` for 30s in bounded mode — that FSM is running, just slower than the log, and is pacing the whole cluster | warning |
| `Uc2ServiceIdentityDrift` (FSM identity, 2.11 pending) | two nodes disagree on row `r`'s declared FSM name (its exported hash differs) — a config edit landed on some hosts and not others, or in a different order; the row's SNAP_BEGIN sessions will refuse each other the moment one runs | critical |
| `Uc2ServiceVersionDrift` (FSM identity, 2.11 pending) | two nodes' attached services at row `r` report different non-zero packed versions — a rolling upgrade in progress, or a mis-deployed binary; refused on the snapshot path, **not** prevented on the live commit path (§7) | warning |
| `Uc2SnapshotStalled` (coordinated snapshots, 2.11 pending) | this node has commanded **full** snapshot instants at least twice in 30m with no complete set landing — one FSM is silently stopping all purging | warning |
| `Uc2StandbySnapshotStalled` (coordinated snapshots, 2.11 pending) | this **learner** has acted on standby snapshot instants at least twice in 30m with no complete set landing — one of its rows is silently stopping the standby set. Cannot fire on a voter (a voter exports `uc2_snapshot_standby_instant_position = 0`) | warning |
| `Uc2SnapshotSetDiverged` (coordinated snapshots, 2.11 pending) | nodes disagree on the newest complete snapshot set's position, i.e. on their purge floors, for 60s | warning |

The per-peer band (`uc2_peer_reported_durable_bytes`, `uc2_peer_replication_lag_bytes`) is leader-authoritative — only the leader receives `AppendPosition` reports, so a follower's own scrape always reads 0 for every peer regardless of health (see [Diagnose a node](diagnose-a-node.md)); `Uc2PeerNeverHeard` and `Uc2PeerLagging` are scoped to `uc2_is_leader == 1` for exactly this reason, and the dashboard's per-peer panel does the same.

`Uc2ServiceWedged` selects the aggregate explicitly
(`uc_service_heartbeat_age_seconds{service=""}`) — the same family now
carries a labelled sample per FSM, and the rule is about the node's slowest
one. `Uc2ServiceAbsent` and `Uc2ServicePinnedAtLagBound` are per-FSM: they
fire once per offending `service` label, on whichever node declares it — and
never both for the same FSM. An FSM that is **absent** also has its lag climb
to the bound and sit there, so `Uc2ServicePinnedAtLagBound` is guarded with
`and on(instance, service) uc_service_attached == 1`: a detached FSM pages
once, as the critical `Uc2ServiceAbsent`, and "pinned at the bound" always
means an FSM that is actually running.

### Watching the disk before `ENOSPC` hits it

`uc2_free_disk_bytes` is `statvfs`'d against the instance directory's
filesystem on the daemon's ~1s outer-loop cadence and gauged directly — **it
is daemon-published only**: a node run in-process without the `uc2-node`
daemon (a library user, a test harness) never writes it, and the field reads
`0` and is **omitted from the scrape entirely**, same convention as
`uc2_leader_hint`'s omission at `u64::MAX`. Do not read its absence from a
scrape as "the disk is full" — it means no daemon is publishing it here.

`Uc2DiskLow` (table above) fires 2 minutes after free space drops below four
journal segments' worth — chosen because that is the fail-stop the archive
actually hits: any write or fsync error on the journal, `ENOSPC` included,
halts the writer, the archive agent panics, and the daemon exits 1 for
systemd to restart. This is *asserted*, not merely documented — see
`examples/uc_crashtest/tests/enospc.rs`. Purging (or growing the disk) before
this alert escalates is the whole point of watching it; see
[Keep the journal from growing without bound](bound-journal-growth.md).

**One documented asymmetry, worth knowing before you rely on either side going
quiet:** the journal's own write path is fail-stop, but the *service's*
snapshot-publish path is not. A snapshot build or publish that fails under
disk pressure is logged and dropped — the snapshot marker simply does not
advance, and the next policy interval tries again — rather than crashing
anything. A disk running low can therefore degrade purge's snapshot cadence
silently, well before the journal itself ever reaches `ENOSPC` and forces the
loud failure. `Uc2DiskLow` and `Uc2PurgeStalled` (table above) are the two
signals that catch this quiet half; do not assume a stalled purge will
announce itself the way a fail-stopped archive does.

## Import the dashboard

[`packaging/grafana/uc2-dashboard.json`](../../packaging/grafana/uc2-dashboard.json)
is a hand-written, minimal, importable dashboard — `uid` `uc2-cluster`,
schema version 39. It declares one templated datasource variable,
`${DS_PROMETHEUS}`; Grafana's import flow prompts you to map it to your
Prometheus datasource at import time, and every panel's query rides that
variable rather than a hardcoded datasource id.

Six panels: commit/apply lag, cluster throughput, per-peer replication lag,
a cluster stat row (term, leader elected, every agent alive, config
version), heartbeat ages, and repair/drop counters (NAKs sent, replay
datagrams, receiver drops). Each is a straight PromQL expression over
contract series — nothing pre-aggregated beyond what the query itself does.

## The probe endpoints

Two boolean-shaped HTTP probes, meant for a load balancer or an orchestrator
rather than a human — for the operator's own diagnosis, prefer
[Diagnose a node](diagnose-a-node.md), which reads the same underlying state
with more explanation.

| Probe | Answers | 200 when | 503 when |
|---|---|---|---|
| `/healthz` | should this process be restarted? | all five agents alive and the node heartbeat is fresh (<3s) | any agent fail-stopped, or the node heartbeat is stale |
| `/readyz` | should traffic be routed here? | role-aware: a leader needs `can_serve` too; a follower/learner needs only to be healthy — both need a fresh **service** heartbeat as well | any `/healthz` failure, OR a leader with `can_serve == 0` (elected but its NewTerm frame isn't yet quorum-committed — flags `0x01`), OR a stale service heartbeat |

`/healthz` is deliberately role- and `can_serve`-blind: an elected-but-not-
yet-serving leader (flags `0x01`) is alive and should not be restarted, only
routed around. `/readyz` is where that distinction lives — it is why the
flags table in [Diagnose a node](diagnose-a-node.md#is-anyone-leading) now
also drives an HTTP probe, not just `uc2ctl status` and the raw cnc page.
The `0x01` case's body names `"NewTerm"` explicitly, so a `curl` against a
stuck node is self-explanatory without cross-referencing this page.

`GET /metrics`, `/healthz`, `/readyz` are the only three routes; anything
else — a non-`GET` method, an unknown path — is `404`.

## Structured records

Nodes print one JSON object per line to stderr, filtered by `[log]`'s
`level` (`error` < `warn` < `info`, each level including the ones before it;
default `info`):

```toml
[log]
level = "info"
```

```json
{"ts_ns":1755600000000000000,"level":"info","event":"became_leader","node":0,"term":3,"base":1048576}
```

Keys always appear in the order `ts_ns`, `level`, `event`, then the event's
own fields in the order the call site names them — this is a machine log, so
key order is a contract, not a cosmetic choice.

**One stream, one format.** Everything `uc2-node` says from startup onward
is a JSON record on **stderr**; it writes nothing at all to stdout, so a log
consumer never merges two streams (twelve-factor
[#11](https://12factor.net/logs); see
[the assessment](../notes/uc2-twelve-factor-assessment.md)). The one stated
exception is the handful of **pre-start refusal lines** (`refusing to start:
…`, the volatile-fs override `WARNING`), which stay human prose on stderr:
they are emitted before `[log] level` has been read, they are addressed to
whoever is reading `systemctl status`, and their machine-readable half is the
exit code — **2** for a refused config (systemd's `RestartPreventExitStatus=2`
will not retry it) and **1** for a runtime failure (retried).

Two families of sites emit records. **Consensus-driven** records fire
exactly on the state transition they name, at the point in the code where it
happens — no polling, no delay. **Derived** records come from the daemon's
own ~1s pass over the counters (`uc2-node.rs`'s poll loop): edge-triggered
(only fire on a change since the last pass) and rate-limited to at most once
per 10s per event, so a sustained condition prints periodically instead of
flooding.

| Event | Fields | Means |
|---|---|---|
| `became_leader` | `node`, `term`, `base` | this node won an election for `term`; `base` is the position its term begins at |
| `became_follower` | `node`, `term`, `leader`? | adopted `term`, following `leader` (the field is absent when the leader is not yet known) |
| `serving_changed` | `node`, `term`, `can_serve` | edge-triggered on the `CAN_SERVE` cnc flag flipping — fires once per transition, not once per cycle |
| `log_truncated` | `node`, `epoch`, `to` | the log was cut back to position `to` as part of reconciliation epoch `epoch` |
| `log_wiped` | `node` | a stronger case of the above: no common prefix with the leader, so the node truncated to 0 and will rejoin from the snapshot floor (`wipes_total` also increments) |
| `snapshot_installed` | `node`, `pos`, `table_position` | the incoming-snapshot floor advanced to `pos`. **This fires whenever the floor marker moves, including the sub-case where the node already held the bytes and only the marker advanced** — it means "this node adopted a snapshot floor," not necessarily "a snapshot transfer happened." Don't read it as proof of a wire transfer. `table_position` (`2.11.0`) is the schedule-table position this node holds once the install is done: the carried table's on the fiat path a below-floor joiner takes, and this node's own, unchanged, on the mid-life path that adopts nothing. |
| `config_adopted` | `node`, `position`, `version`, `prev_position` | a new `ClusterConfig` (version `version`) was adopted at `position`, superseding the one at `prev_position` |
| `halt_removed` | `node`, `term`, `msg` | this node is not a member of the just-adopted config and has fail-stopped (parked permanently; the process keeps running but never serves again) |
| `stepdown_removed` | `node`, `term`, `msg` | this node's own self-removal just committed while it was leader; it fail-stopped the same way as `halt_removed` |
| `nak_storm` (derived) | `node`, `naks_dropped`, `naks_served` | the NAK-drop counter advanced in the last ~1s window — repair traffic is being shed |
| `seal_failures` (derived) | `node`, `count`, `is_leader` | `count` sealed-datagram failures since the *last emitted* record (not cumulative); on a leader this is expected and benign — see [Encrypt traffic between nodes](encrypt-node-traffic.md#confirm-it-is-healthy) |
| `snapshot_published` (derived) | `node`, `pos` | this node's own service-side snapshot position advanced to `pos` |
| `admin_op` | `actor`, `origin`, `op`, `op_name`, `id`, `addr`, `seq`, `nonce`, `outcome`, `reason`, `config_version` | this node answered an admin request (membership change). A **mirror** of the line already written to `<instance_dir>/audit.jsonl`, which is the record of record: the file is fsynced *before* the answer is published, this stream is best-effort. `actor` is the admin key name that signed it, `filesystem` when the node authenticates nothing (`auth = "none"`), `unverified` on a request that failed authentication, or `peer:<id>` on a proposal a follower forwarded (`origin: forwarded`). `outcome` is `accepted` (proposed and appended — not necessarily committed) / `refused` / `retry`. |
| `admin_audit_failed` | `node`, `seq`, `nonce`, `op`, `status`, `err` | the audit record for an admin request could NOT be written, so the request was refused with reason 24 rather than answered unrecorded. On an otherwise-accepted change this means the change may still be in the log — check `uc2ctl status`. Alert on this: it means the node's disk is failing or full. |
| `agent_failstopped` | `agent` | one of the five polling agents panicked (`cluster` is the fifth, since 2.11); the daemon logs this and then **exits 1 without draining**, so systemd restarts it and the replay path (not reconstruction) picks the node back up |
| `config_loaded` | `path`, `sha256` | the config file that was read, and plain SHA-256 over its bytes — the config half of a release identity, checkable with `sha256sum`. See [Record a release](record-a-release.md). |
| `config_env_override` | `var`, `value` | one `UC2_*` [environment override](../reference/configuration.md#environment-overrides) took effect, so this value did NOT come from the config file. Emitted before `[log] level` is applied, so it appears even at `warn`. |
| `node_listening` | `node`, `bind` | the node is up and its UDP socket is bound to `bind`. The first record of a healthy boot. |
| `metrics_listening` | `node`, `url` | the observability endpoint is bound; `url` is the exact `/metrics` address. Absent when no `[metrics]` section is configured. |
| `statvfs_failed` (derived) | `node`, `dir`, `err` | the ~1s pass could not stat the instance dir's filesystem, so `uc2_free_disk_bytes` still holds its previous value rather than a misleading zero. Rate-limited like the other derived records. |
| `draining` | `node` | SIGTERM/SIGINT received; the observability endpoint is closed and the archive is draining to the `--drain-timeout-secs` deadline |
| `stopped` | `node`, `outcome`, and on a timeout `unrecorded`, `append`, `durable` | the daemon's last record. `outcome` is `drained` (the archive caught up; a clean exit 0) or `drain_deadline_expired` (it did not, and the node stopped anyway — `unrecorded` is how many bytes the restarted node will re-fetch). One event name with an outcome field, so a consumer greps `"event":"stopped"` and reads `outcome`, rather than matching two differently-shaped lines. |

`uc2-gateway` emits the same format on the same stream, with its own
event names so a merged journal stays unambiguous:

| Event | Fields | Means |
|---|---|---|
| `gateway_listening` | `bind` | the edge's TCP front door is bound |
| `gateway_stats` | `conns`, `submits`, `queries`, `responses`, `redirects`, `retries`, `unknown`, `backpressure`, `grant_changes`, `leader_changes`, `status`, `refused_busy` | the 10 s counter sample — see [Run a gateway § Stats record](run-a-gateway.md#stats-record) for what each one means |
| `gateway_edge_faulted` | `reason` | the node's instance restarted underneath the gateway, so its attach is void; the daemon exits 1 for the supervisor to restart it against the new instance |
| `gateway_signal_handler_failed` | `err` | the signal handler could not be installed at startup; the daemon stops rather than run unstoppable |
| `gateway_stopped` | — | SIGTERM/SIGINT received; the edge is stopping. The gateway holds no durable state, so there is no drain and no outcome field |

`agent_failstopped` is a behavior change worth calling out on its own: before
M10, a mid-run agent panic could leave the process running with a healthy-
looking exterior — a zombie node still holding its instance-directory lock,
still answering `uc2ctl status` with stale-but-plausible-looking numbers,
while the rest of the cluster quietly lost a member. Now the crash is loud
and the process dies, which is what makes `/healthz` and `Uc2AgentDead`
meaningful signals rather than a race against a silent hang.

## Where to go next

- [Diagnose a node that is not serving](diagnose-a-node.md) — the same
  underlying state, read by hand, with the reasoning behind each threshold.
- [Configuration](../reference/configuration.md#log-and-metrics) — the
  `[log]`/`[metrics]` schema.
- [Run a cluster on real hosts](run-a-cluster.md) — where the observability
  endpoint fits into process supervision and exit codes.
