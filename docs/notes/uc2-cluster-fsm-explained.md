# The cluster FSM, explained

*Written 2026-09-06 for the cluster-FSM work (plan 1); extended 2026-09-07
with the "Instants" section below when plan 2 (coordinated and standby
snapshot instants, §5) landed. Release on hold. Spec:
`docs/superpowers/specs/2026-09-05-uc2-cluster-fsm-and-coordinated-snapshot-design.md`
— this note carries §2–§5's argument in plain language.*

## The problem in one sentence

Before this work, cluster-wide state reached a node that had fallen behind by
three different mechanisms, and two of them were **live reads of a running
node's memory at the moment it happened to be shipping** — which is a
different question from the one a joiner is actually asking.

## The bug that made it visible

A node that joins **below the purge floor** cannot read the log prefix that
built the cluster's state: those bytes are gone. So the node serving it has to
hand the state over some other way. Until this change there were three ways:

- **membership** rode on the `SNAP_BEGIN` datagram, read live off the shipper
  at the moment it opened the session, with no freshness gate at all (M6);
- **the schedule table** rode on its own `SNAP_TABLE` datagram, read live, and
  because "live" is obviously unsafe for something an operator applied, it was
  gated on the shipper's **commit counter**;
- **every user FSM's state** rode as a snapshot artifact — an immutable file,
  tagged with the log position it represents.

Only the third is right, and the second is where it showed. The cnc page's
commit counter is deliberately **not primed at boot** (`uc_log/src/counters.rs`):
a freshly restarted node reads `0` there until its first commit advance. So a
restarted node's commit gate could not clear the node's own table record, and
it shipped the previous one, or nothing. That is the "restarted node
under-ships for one window" residual, and it was written down as a limitation
rather than fixed, because there was nowhere better to put the answer.

The residual was a symptom. The cause is that UC had **no node-owned notion of
"cluster state as of position P"** — nothing to freeze, nothing to name, so
every carry became a live read of whatever was in memory.

## The line: node data and cluster data

The fix starts by drawing one line, with one test:

> **Would the cluster be *wrong* if two nodes disagreed about this?**

If yes, it is **cluster data**: replicated through the log, applied at the same
position on every node, and captured in a snapshot artifact. If no, it is
**node data**: local, never replicated, never snapshotted, rederived on the
node that needs it.

| node data | cluster data |
|---|---|
| the vote (`state/vote.state`) | **membership** |
| the term map (`state/term_map.state`) | **the schedule table** |
| the snapshot floor (`state/snapshot.state`) | **settings** (`fsm_lag`, `admission_bytes`, snapshot cadence) |
| output progress (`state/output_progress.state`) | every user FSM's state |
| the `log_time_ns` clamp (cnc page `+4048`) | `Sessioned<S>`'s session table |
| crypto session keys and the key epoch | `Timed<S>`'s pending timer set |
| the node's timer heap (`RowTimers`) | |

Two nodes with different votes are normal Raft. Two nodes with different
memberships are a split brain. That is the whole test.

The right-hand column already had a mechanism — the state machine — and the
right-hand column's newest member, `Timed<S>`'s pending timer set, is the
existence proof: it has been snapshot-and-log state since the timers work
landed, and it has never had a carry problem. Only the node's own copies were
broken.

## The mechanism: one more state machine, an internal one

So cluster data that was not in an FSM goes into one. `uc_node::cluster_fsm`
is a state machine like any other — it implements `RawStateMachine` and
`SnapshotStateMachine`, its identity is `const NAME = "uc_cluster"` — holding
exactly three records: membership, the schedule table, and settings.

It is **in-process**, not a service: consensus cannot depend on an external
process being alive to know what its own quorum is. It runs on a fifth polling
agent, `uc2-cluster`, beside consensus/sender/receiver/archive, with its own
apply loop over the same log buffer. It has no cnc slot (page 2 is exactly
eight service slots, with no ninth), and it is **outside the lag policy** —
a stalled user FSM must not stall the node's view of its own configuration.

Changing it is a command on the log. One frame type carries all three:

```
FRAME_TYPE_CLUSTER = 4        (reuses the retired CONFIG's number)
body: kind: u8 ‖ reserved [u8; 7] ‖ payload
  kind 1 = Membership     payload = the ClusterConfig encoding CONFIG carried
  kind 2 = ScheduleTable  payload = encode_schedule_table (≤ 1064 B)
  kind 3 = Settings       payload = the 29-byte Settings record
```

The log is a **broadcast** log — it carries no service id and does no routing
— so the frame type is the only router there is. User apply loops act on
`MESSAGE` and their own `TIMER` frames and yield everything else, so they skip
`CLUSTER` for free; the cluster agent's loop is the mirror image.

And it snapshots like any FSM: `snapshots/cluster/snap-<pos>.ultcluster`,
written at the position it has consumed the log up to. That file is the thing
that was missing. "What was in force at P" now has an answer that is a **file**
and not a memory read, and the snapshot session ships it as one more artifact
under the reserved id **255**, after the declared rows. A below-floor joiner
installs it **before** its purge floor advances — before it can serve a read or
win an election — so it holds the cluster's membership, table and settings
before it can act on any of them.

## Membership: one frame, two readers, and why that is safe

The one place this is genuinely subtle. Raft's single-server-change safety
depends on a node using the **newest configuration in its log, committed or
not** (Raft §4.1). An FSM applies at **commit**. Those cannot be the same
reader, so membership has two:

- **the consensus kernel** keeps its durable-time view, fed by the archive
  walk exactly as before, with `state/config.state`, its one-level `prev` and
  its revert on truncation. Everything downstream of the Raft rule runs on
  this: commit ranking's quorum, vote grant and count, the `is_voter` report
  filter, the own-id tombstone halt, the reconfiguration-in-flight gate, and
  the sender's fan-out targets. That view is **node data with a replicated
  source**: it may run ahead of commit, it may be truncated, and two nodes may
  legitimately disagree for a moment.
- **the cluster FSM** applies the same frame at commit and is the **snapshot
  authority**. A joiner installing the artifact at P gets the committed
  membership at P — which is correct, because P is committed — and then tail
  replays.

The invariant that keeps them honest, and which `uc_sim` now sweeps after
every event as **inv12**: *the cluster FSM's membership is always a committed
prefix of the kernel's.* At every step the FSM's membership equals the
kernel's durable-time config at some position at or below commit, and at every
committed position the kernel's config equals what the FSM applied there.

The schedule table and settings have no durable-time consumer at all, so they
are simply committed-view state.

Everything else that reads cluster state — the timer heap's arming, the
ingress door's admission window, the cnc `fsm_lag` word — reads a
position-tagged **view** the cluster FSM publishes at the end of every apply
batch. The test for whether a reader may use the committed view is whether a
momentary skew between nodes could change *state* rather than *timing*. These
change timing only. The consensus kernel fails that test, which is why it does
not use the view.

Anything node-local is **clamped at the point of use, never refused in
`apply`**: `apply` cannot see this host, so a replicated `admission_bytes`
larger than a small host's ring is clamped to `buffer_bytes / 2` there and
diverges in throughput, not in state.

## The timer heap goes leader-only

A second consequence of the line. The node's per-row timer heap is a *cache*,
derived entirely from cluster data: the row's own `Timed<S>` pending set and
the schedule table in the view. It used to be maintained on every node so a
new leader would hold it on its first pass. Under §3's rule that cache is
legitimate but unjustified — everything in it is reconstructible, through the
re-announce path that already serves a service restart.

So the heap is now **leader-only**. A follower's service writes nothing to its
`svc_sched.<row>.ring` (the gate is the same `NODE_FLAG_LEADER` read the apply
loop already does once per cycle), the node drains that ring only while
leading, and on demotion the heap is **discarded** rather than re-armed. On
promotion the service sees the leader flag's rising edge and re-announces its
whole pending set; the node arms the table from the view.

Gating the ring write is not tidiness. `write_sched` on a full ring spins
forever by design — a ring nobody drains does not fill to a high-water mark,
it eventually blocks the follower's apply thread for good.

The cost is stated rather than hidden: a timer due inside the promotion window
fires one service cycle plus one ring round trip later than it would have.
Failover already makes timers late (`ev.late(ctx)` says so), so the semantics
are unchanged and only the width of an existing window moves.
`uc2_timers_pending` changes meaning with it — it is now the **leader's**
count, and a follower exports `0`.

## What this retires

Everything below is gone from the tree, not deprecated:

| gone | what replaced it |
|---|---|
| `DGRAM_KIND_SNAP_TABLE` (21) and its body reader | the cluster artifact, id 255, on the same session |
| `SnapBeginBody.config` (the carried membership tail) | the same artifact; `SNAP_BEGIN` is now fixed-length, layout **V4** |
| `FRAME_TYPE_SCHEDULE_TABLE` (6) | `CLUSTER kind = 2` |
| `state/schedules.state`, `ScheduleRecord` and its `prev`/revert | FSM state applied at commit — a committed frame is never truncated, so there is nothing to revert |
| `ScheduleShip` / `shippable_schedule` / the commit gate | the artifact is committed by construction, through apply's own gate |
| the follower's timer heap, `rearm_timers`, `uc2_timers_rearmed_total` | the leader-only heap and the rising-edge re-announce |
| `uc2_snapshot_table_stray_total` | there is no side-channel datagram left to be stray |

`state/config.state` **stays**, and that is the one thing on this list that
looks like it should have gone. It is the kernel's durable-time shadow, and
§4.6 above is the reason: it answers a different question, at a different time
base, for a reader Raft does not allow to wait for commit.

## Instants: one position, one set

Plan 1 wrote the cluster artifact on a **bridging trigger** — snapshot once
every declared row happens to have snapshotted, and my own applied position
has reached the lowest of theirs. That works, but it makes a "set" a
lowest-common-floor: a coincidence of independent per-service timers, whose
position no one chose. Plan 2 replaces it with something the cluster does
deliberately.

**The command.** The leader appends a `SNAPSHOT` frame — frame type 7, an
empty body, because the frame's own end position **P** is the whole payload.
An operator asks for one with `uc2ctl snapshot`, or the replicated
`snapshot_interval_bytes` cadence issues one when enough log has accrued.
Either way only a leader appends one, because only a leader appends anything.

**What every row does at P.** The frame is broadcast, like every frame: each
declared row's apply loop reaches it, and so does the cluster FSM. Each,
having applied everything strictly below P, calls `freeze()` and hands the
build to the same builder thread that has always written artifacts. The file
is `snapshots/<row>/snap-<P>.ultsnap`; the cluster FSM's is
`snapshots/cluster/snap-<P>.ultcluster`. When every declared row and the
cluster FSM have published at P, that node holds the **complete set at P**.
The old per-service `SnapshotPolicy { interval_bytes }` is gone; nothing
triggers a snapshot but the log.

**Why the set is committed by construction.** This is the part worth
following, because it is where a counter used to be. A row freezes at P only
after *applying* up to P, and apply is gated on `min(commit, durable)`. So an
artifact at P exists only if P was committed on that node when it was built —
and committed bytes are never truncated. A complete set therefore *implies* a
committed P, with no counter to consult, no boot window in which one reads
zero, and no freshness gate to get wrong. That is the same argument the whole
cluster-FSM change rests on, arriving one level down: the ship gate becomes
"the complete set at my floor", and everything it needs is already true.

Two consequences follow immediately. A `SNAPSHOT` frame truncated by a leader
change needs no revert record: nothing was adopted, and any artifact built at
that P is simply an orphan. And a row that was behind when the instant went
past does not lose it — replaying a span, it acts on that span's **last**
`SNAPSHOT` frame and freezes there, provided P is above both what it has
already snapshotted and what it has already applied.

**Retention is node-owned, and it only ever deletes.** The node keeps the set
at its persisted floor plus everything newer and unlinks the rest, matching
file names exactly. It used to be per-writer — the service kept its newest
two, the cluster agent kept its newest two — and that cannot work once a set
is the unit: two abandoned instants in a row would have a per-row pruner
delete the artifact at the floor, and the ship gate would then decline every
joiner forever, looking for a file its own retention had removed. Only the
node can see a set. Symmetrically, the node never *writes* an artifact: it
deletes what it can prove is superseded, and nothing else.

**The ship gate, and the envelope.** A session ships the artifacts at one
position or none. The sender looks each one up **by file name** at the target
position rather than by whatever a row's live `snapshot_pos` word reads — that
word runs ahead the moment a later instant completes — and a receiver refuses
a session whose `SNAP_BEGIN`s disagree about the position. That closes the
"set assembled from two different instants" case by name rather than by luck.

The file name is only a name, though, and a rename or a mis-copied backup can
make an artifact built at some earlier `P0` claim P. Nothing in the payload
can catch that, because the tag is an **exclusive** frontier: the image covers
everything strictly below P, so a state machine's own cursor legitimately sits
*below* the tag and an image from `P0` looks entirely plausible. So the
framework took the guarantee: every artifact file now begins with sixteen
bytes it owns — `ULTSNAP1` and P — checked on every install path and by
`verify-backup`. UC still prescribes nothing about the payload.

**Standby instants, and the pull that follows.** A freeze runs on the row's
apply thread and is as long as the service's state is big — UC cannot bound
it, because both the state and the code are the service's. Meanwhile a node's
durable report is capped at `min_applied + fsm_lag`. Put those together and a
coordinated instant has a cost the old accidental staggering hid: with every
row on a quorum frozen at the same P, every report caps at `P + fsm_lag` and
**commit stalls cluster-wide** until the slowest freeze ends. For a small
state that is invisible. For a large one it is Aeron's snapshot pause arriving
through the back door.

The only lever that works for any state size is not freezing the voters. So a
`SNAPSHOT` frame carries a flag, `FLAG_SNAPSHOT_STANDBY`, and a node acts on a
flagged instant only if it is a **learner** — which it learns from a new bit,
`NODE_FLAG_LEARNER`, in the same cnc word its apply loop already reads the
leader flag from once per cycle. A voter's rows yield the frame like any other
node-only frame and pay nothing. This is Aeron's shape, and it is Aeron's
shape for the same reason: the standby snapshot there is the same action with
a flag, which a member's services skip.

That leaves the set on the learner, and a voter still needs it — for its own
purge floor, and to serve a joiner. The return path is a **pull**:
`uc2ctl snapshot fetch --from <learner-id>` sends a `SNAP_REQUEST`, the
learner's sender opens an ordinary session from its own artifacts, and the
voter's receiver takes it **store-only** — the files are written and the set is
marked complete, and nothing is installed. That distinction matters: a voter
above P storing a set at P is not a joiner, and treating it as one would roll
its state machines backwards. Its floor then moves through the ordinary
completeness path, exactly as if it had frozen. A fetch binds the position it
asked for and is refused if that position is above what this node has made
durable — a node must not adopt a floor above its own durable frontier.

Until a voter has fetched, its floor sits where it was, and a joiner that
needs a lower set is **redirected**: the node that cannot serve answers with
"ask learner *k* for the set at P", and the joiner asks there. In practice
that also covers a case nothing to do with standby — a node restored from a
backup taken before its own floor, whose cnc page names a floor whose files
are not on disk.

**Two things this deliberately does not do.**

*Automatic replication after a standby instant.* A voter pulls when an
operator tells it to, not when the learner finishes. Making it automatic needs
the learner's "complete at P" to be visible cluster-wide, and the honest
channel for that is a cluster-FSM command the leader appends on the learner's
behalf — a fourth kind, and a design of its own. Aeron's open-source half
defers it the same way: there, a member replicates a standby snapshot when an
operator flips a toggle. The trade is stated rather than hidden: you do not
pay the freeze on voters, and in exchange a voter's purge floor waits for you.

*Timezones and cron.* Still out, and still the schedule table's business, not
the snapshot instant's — an instant is a byte position, not a time.

## What plan 1 did not do, and plan 2 did not either

`uc2ctl schedule show`, `uc2ctl settings show` and `uc2ctl status`'s
`schedule_position=` still read the newest cluster **artifact** — a file
beside the running node, not its live view — so they lag, and say "no cluster
artifact yet" until the first instant has completed. One process cannot read
another's memory; a live reading needs the response-on-the-egress-broadcast
path the spec left to a phase 2.

## Where to go next

- [Configuration § `[settings]`](../reference/configuration.md#settings) — the
  genesis seed, and the two keys that are now refused by name.
- [`uc2ctl` § `settings apply`](../reference/uc2ctl.md#settings-apply) — how a
  settings change is staged, signed and applied.
- [Wire protocol § Log frames](../reference/wire-protocol.md#log-frames) — the
  `CLUSTER` frame and its kinds.
- [Log time and timers, explained § The schedule table](uc2-log-time-and-timers-explained.md#the-schedule-table)
  — what the table does once the cluster FSM holds it.
- [Instance directory § Files](../reference/instance-directory.md#files) —
  `snapshots/cluster/` and `settings.pending`.
- [Keep the journal from growing without bound](../how-to/bound-journal-growth.md)
  — commanding an instant, setting a cadence, and turning purge on.
- [`uc2ctl` § `snapshot`](../reference/uc2ctl.md#snapshot) — the three verbs
  and their refusals.
- [Monitor a cluster § The snapshot families](../how-to/monitor-a-cluster.md#the-snapshot-families-211-pending)
  — the seven series, the two alerts, and the records.
