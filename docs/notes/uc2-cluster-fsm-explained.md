# The cluster FSM, explained

*Written 2026-09-06 for the cluster-FSM work (plan 1); extended 2026-09-07
with the "Instants" section below when plan 2 (coordinated and standby
snapshot instants, §5) landed; extended again 2026-09-20 with "Pins and
reports" when the FSM upgrade lifecycle (plan B1) took `CLUSTER` kinds 4 and
5. Release on hold. Spec:
`docs/superpowers/specs/2026-09-05-uc2-cluster-fsm-and-coordinated-snapshot-design.md`
— this note carries §2–§5's argument in plain language; the FSM upgrade
lifecycle's own spec is
`docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md`.*

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
exactly five records: membership, the schedule table, settings, and — since
`2.13.0` — the per-row upgrade pins and the per-row snapshot reports.

It is **in-process**, not a service: consensus cannot depend on an external
process being alive to know what its own quorum is. It runs on a fifth polling
agent, `uc2-cluster`, beside consensus/sender/receiver/archive, with its own
apply loop over the same log buffer. It has no cnc slot (page 2 is exactly
eight service slots, with no ninth), and it is **outside the lag policy** —
a stalled user FSM must not stall the node's view of its own configuration.

Changing it is a command on the log. One frame type carries all five:

```
FRAME_TYPE_CLUSTER = 4        (reuses the retired CONFIG's number)
body: kind: u8 ‖ reserved [u8; 7] ‖ payload
  kind 1 = Membership     payload = the ClusterConfig encoding CONFIG carried
  kind 2 = ScheduleTable  payload = encode_schedule_table (≤ 1064 B)
  kind 3 = Settings       payload = the Settings record (33 B since 2.12.0;
                                    a 29-byte v1 record still decodes)
  kind 4 = UpgradePin     payload = 20 B  (row ‖ from ‖ to ‖ origin)
  kind 5 = SnapshotReport payload = 16–112 B  (row ‖ count ‖ position ‖
                                    count × (node_id ‖ hash))
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

## What Aeron already does, and why UC did not have it

This shape is not invented here. Aeron Cluster snapshots node state and
service state **at one log position, as sibling recordings in one
recording-log entry** — checked in the Java source
(`aeron-cluster/src/main/java/io/aeron/cluster/`, the lines the spec's §2
cites):

- `ConsensusModuleAgent.takeSnapshot(timestamp, logPosition, serviceAcks)`
  (`ConsensusModuleAgent.java:3153`) records the consensus module's **own**
  snapshot and every service's, all at the same `logPosition`, each appended
  to the recording log under a `serviceId` — the consensus module's own entry
  uses `ConsensusModule.Configuration.SERVICE_ID = Aeron.NULL_VALUE`
  (`ConsensusModule.java:450`). That is the same idea as UC's reserved
  `service_id = 255`.
- `snapshotState(publication, logPosition, leadershipTermId)`
  (`ConsensusModuleAgent.java:3221`) brackets the module's state with
  `markBegin`/`markEnd` **stamped with `logPosition`**, and writes the
  sessions, `timerService.snapshot(snapshotTaker)` — the whole timer set —
  and the pending service-message trackers between them.
- The instant is a **log entry**, not a side channel: the leader calls
  `appendAction(ClusterAction.SNAPSHOT, timestamp, flags)`
  (`ConsensusModuleAgent.java:2566`), every node replays it at the same
  position (`:1581`), and each service sees it through
  `ClusteredServiceAgent.onServiceAction` (`:1063`).
- A node that needs a snapshot **replicates the recording**
  (`SnapshotReplication.java:67`, `MultipleRecordingReplication`) — an
  archive-to-archive copy of an immutable artifact, never a live read.

So there is no ship-time gate in Aeron because there is nothing live to gate.
"What was in force at P" is answered by the artifact.

UC took the other path for one structural reason: **its snapshot artifacts are
entirely the service's bytes.** UC ships no store and prescribes no encoding,
so there was never a node-owned artifact to put node state in. When the
schedule table needed to reach a below-floor joiner there was nowhere
structural to put it, so it got a side channel carrying live state, which
needed a freshness gate, which reached for the one counter that is zeroed at
boot. Membership had carried the same way since M6, with no gate at all.

The answer is *not* a node artifact — that was this design's first draft, and
it was wrong. Under the line above there is **no node data to snapshot**.
Everything the draft wanted to put in one is cluster data, and cluster data
already has a mechanism. So: one more FSM, internal, and one command that
makes every FSM freeze together. What UC gets that Aeron had to build
separately is every mechanism the user FSMs already have — journal replay
after a restart, snapshot-plus-tail-replay for a below-floor joiner, the
hard-crash reconstruction path, `Timed<S>` if the cluster FSM ever schedules,
the lincheck oracle. Three hand-written `StableValue`-plus-`prev` chains
became one `SnapshotStateMachine` impl.

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
| `SnapshotPolicy` / `ServiceConfig::snapshot_policy` (the per-service byte cadence) | the commanded instant, and the replicated `snapshot.interval_bytes` for a cadence |
| both per-writer `retain_newest(2)` pruners | node-owned, delete-only retention — only the node can see a *set* |
| the artifact's **bridging trigger** (`maybe_build_snapshot`) | the `SNAPSHOT` frame: one position, commanded |

`state/config.state` **stays**, and that is the one thing on this list that
looks like it should have gone. It is the kernel's durable-time shadow, and
§4.6 above is the reason: it answers a different question, at a different time
base, for a reader Raft does not allow to wait for commit. `uc_sim`'s **inv12**
is what relates the two.

None of this is deprecation: the names are gone from the tree, and
`uc_node/tests/retired.rs` fails the build if any of them comes back — a
`git grep`, deliberately, so a re-introduction in a comment, a script or a doc
is caught as well as one in code.

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

Two consequences of that shape are worth stating, because both were nearly
shipped as defects.

The first is a **monitoring** one. On a standby cluster the leader is a voter,
so it commands instants whose sets only the learners build, and its own set
stays where it was until someone runs the fetch. Read through the obvious
metrics, that healthy steady state is *indistinguishable* from the failure the
snapshot alert exists to catch: instants keep being commanded, no set ever
completes. So the two are kept apart by construction rather than by a
threshold. A superseded standby instant is not an abandonment on a node that
was never going to build its set — no counter moves — and the commanded-instant
gauge counts **full** instants only. The standby half gets its own gauge,
written at the one place that decides whether this node acts on a standby
frame at all, which by the rule above is only ever a learner. So the standby
alert watches the node doing the work, and cannot fire on a voter: not because
a label excludes it, but because a voter never writes the series.

The second is a **security** one. The pull request is the one snapshot path
that is deliberately not leader-gated — the source is a learner, and a learner
never leads. Which means, with wire crypto off, that a 28-byte datagram
claiming to come from anywhere elicits a whole snapshot set sent to that
address. UC's crypto-off posture already concedes the cluster to a
network-path adversary, but this one hands a reflector to an attacker aimed at
somebody else entirely, which is not the cluster's to concede. So the request
is served only to an address in the current membership — voters and learners,
which is who could legitimately ask — and anything else is a named, counted
drop. The redirect is left ungated on purpose: the worst a forged one achieves
is making a joiner ask a real member for a set it will verify anyway.

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
behalf — a further kind (kinds `4` and `5` are since spoken for, § Pins and
reports below), and a design of its own. Aeron's open-source half
defers it the same way: there, a member replicates a standby snapshot when an
operator flips a toggle. The trade is stated rather than hidden: you do not
pay the freeze on voters, and in exchange a voter's purge floor waits for you.

*Timezones and cron.* Still out, and still the schedule table's business, not
the snapshot instant's — an instant is a byte position, not a time.

## Pins and reports (2.13.0)

The cluster FSM's third data kind (spec §2.5, plan B1) is the first one that
is not "the current state of X". Membership, the schedule table and settings
are all **overwritten records**: applying a new one replaces the old one, and
the FSM only ever needs to hold the latest. An FSM upgrade needs the opposite
— what happened, in what order — which is why it gets two new `CLUSTER`
kinds instead of a fourth field on Settings.

**`UpgradePin` is an event, not a setting.** "At position `origin`, row `row`
went from `from` to `to`" names a fact about history, not a tunable an
operator dials: the whole point is the *sequence*, so a fresh pin never
overwrites the last one, it is appended after it. The FSM keeps a **per-row
history of at most 4** such events — enough for `uc2ctl upgrade show` to
print a trail without the state growing without bound — and republishes the
newest one into the row's cnc status line on every view publish, the same
edge every other cnc word rides.

**Why a pin exists at all.** A pin names the coordinated snapshot instant
(`origin`, a `SNAPSHOT` frame's own END position — see [Instants](#instants-one-position-one-set)
above) whose complete set a row should install the next time it attaches,
rather than tail-replaying the log from wherever it last left off. That
matters exactly when tail-replay would be unsound: a state machine whose
`apply` logic changed between `from` and `to` cannot be trusted to reproduce
`to`'s state by re-applying `from`'s history, so the pin is what lets a
restarting service skip straight to a verified snapshot instead. (What
*acts* on a pin at attach time — the unconditional install and its attach
refusal — is plan B2's job, not this one; this plan only gets the fact
recorded, replicated and observable.)

**The refusals split at the door, on purpose.** Nine numbers, `52`–`59`,
cover this feature (`51` was already `schedule_too_large`). Three checks read
inputs that are this node's own, never the FSM's, and so are refused **at
the door** — in `Consensus::apply_upgrade_pin`, before anything is proposed
— rather than round-tripping to commit only to fail there identically on
every node:

| reason | name | checked | why it is node-local |
|---|---|---|---|
| 52 | `pin_row_undeclared` | door | `row` must be one *this* node declares (`[services] names`'s length) |
| 53 | `pin_from_mismatch` | door (no pin yet) **or** replicated (a pin exists) | with no history for the row, `from` is checked against the row's own **attached version word** on the cnc page — a purely local read; once a pin exists, the FSM checks `from` against its own last-recorded `to` instead, which is replicated state every node computes identically |
| 54 | `pin_no_set` | door | `origin` must equal *this node's* newest complete set (`uc2_snapshot_set_position`) — see below for why "newest", not "any retained" |
| 55 | `pin_not_monotone` | replicated | a new pin's `origin` must be strictly greater than the row's last one — the FSM's own check, since only it knows the history |
| 56 | `pin_digest` | door | the staged `upgrade.pending` file changed between staging and applying |
| 57 | `pin_missing` | door | no staged file on this node |
| 58 | `pin_decode` | door | the staged file is not a 20-byte `UpgradePin` record |
| 59 | `report_stale` | replicated | a `SnapshotReport` below the row's held report position |

The door reads are **advisory**, not authoritative: `to_state()` can pair a
freshly-read `applied` position with pins that are a tick stale, so a door
check can occasionally miss a case the FSM would have caught. That never
makes an unsound pin *accepted* — every node re-runs the replicated half
(53's existing-pin case, 55) at apply, so a command that slips past a stale
door check is simply refused a moment later, identically everywhere, with
`55` instead of a door number. The three-way split exists to make the common
case (an operator's own mistake) fail fast and locally, not because the door
is trusted for correctness.

**`pin_no_set` names the newest set, deliberately not "any retained set".**
Retention is delete-only (above), so the moment a pin is accepted its
`origin` becomes exempt from pruning at every row — but *before* that
moment, an older complete set can vanish out from under a check-then-commit
race: the door reads `uc2_snapshot_set_position` (this node's newest), the
retention sweep runs, and by the time the command would commit an older set
named at the door is gone. The newest set cannot be pruned out from under
you this way, because nothing is newer to make it the not-newest, which is
why `pin_no_set` accepts only it.

**The cnc words, and why they need a seqlock.** The pin
republishes onto the row's service status line as three new words, `+16`
`upgrade_origin`, `+24` `pinned_version` and `+32` `pin_seq` — not slot line 7, which is
already seven of its eight words deep (`name`, four words from `+448`,
`identity_hash` at `+480`, `timers_pending` at `+488`, `freeze_ns` at
`+496`) and so has exactly one free word left, at `+504` — well short of the
three a pin needs. (`log_time_ns` is not one of line 7's occupants: it is the
unrelated page-1 global word at offset 4048.) `upgrade_origin == 0` is "no
pin", the
gate every reader checks first. The pair itself is published under a
**seqlock**: the node-side writer (`ServiceStatusLine::store_pin`) bumps
`pin_seq` to ODD, stores `pinned_version`, stores `upgrade_origin`, and
bumps `pin_seq` back to EVEN, every step `Release`; a reader loads
`pin_seq`, both words, then `pin_seq` again, and accepts the pair only if
the first read was EVEN and the two reads agree.

Why a third word, rather than just writing the version before the origin?
Because that order alone only covers the **first** pin a row ever gets (`0`
→ a non-zero origin), and re-reading the origin does not extend it to a
**re-pin**. A writer that has stored `version_new` but has not yet stored
`origin_new` leaves the origin *stable* — so a reader that loads the origin,
the version, and the origin again sees no movement and returns
`(origin_old, version_new)`, a pair that never existed, on its first
attempt. Two atomics with no shared sequence cannot be read consistently by
re-reading one of them; the sequence has to be its own word. What
`ServiceStatusLine::pin()` guarantees now is exactly that: it returns a pair
that was stored together, or `None` — including after 64 collided attempts,
where an honest "this row reads as unpinned right now" beats a fabricated
pair that plan B2's attach would install an artifact from. Every reader
(`/metrics`, `uc2ctl status`, and plan B2's attach) goes through it rather
than through the raw word accessors.

**`SnapshotReport` holds observations, not a verdict.** `(row, position,
hashes: Vec<(node_id, hash)>)` is everything the leader collected for one
`(row, position)` pair — 1 to 8 entries, node ids strictly increasing so
identical observations always encode identically. The three-way reading
(all equal / a majority names a minority / no majority at all) is
[`verdict`](../../uc_protocol/src/v2/upgrade.rs), a **pure function** every
reader recomputes from the stored hashes, not a field carried in the FSM's
own state — the same reasoning as `uc2ctl upgrade show`'s history print:
store what was observed, derive what it means, so two readers can never
disagree about the derivation itself, only about which bytes they read.

**The N = 2 case is not a bug.** With exactly two reporters disagreeing,
neither hash is held by *strictly more than half*, so `verdict` reports
`agreed: false, majority_hash: None, minority: []` — no verdict, not "one
of them is wrong." A human reading `uc2ctl upgrade show`'s
`hash_verdict=NO_MAJORITY nodes=2` output could easily read that as a
missing feature; it is the correct answer to "which one is the majority"
when there isn't one. A three-reporter cluster (or any odd count) is what
lets 2-of-3 actually name the minority.

**Until plan B3, nothing produces a report.** This plan gives
`SnapshotReport` its wire kind, its codec, its FSM state, its refusal, its
gauge and its `uc2ctl upgrade show` rendering — the whole replicated and
observable half. No node yet computes a per-node artifact hash or appends
the `CLUSTER kind = 5` command that would carry one; that is spec §6.5.2
items 1–3, left to plan B3. Until then `uc2ctl upgrade show` prints nothing
under `hash_verdict=` for any row, which is the correct behaviour for a
feature whose producer has not shipped yet, not a defect in this plan.

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
- [Monitor a cluster § The snapshot families](../how-to/monitor-a-cluster.md#the-snapshot-families-2110)
  — the eight families, the three alerts, and the records.
- [`uc2ctl` § `upgrade pin` / `upgrade show`](../reference/uc2ctl.md#upgrade-pin)
  — the refusals by name, and how to read a pin's history.
- [Monitor a cluster § The log clock and the timer families](../how-to/monitor-a-cluster.md#the-log-clock-and-the-timer-families-2110)
  — `uc2_upgrade_pin_origin`/`_version`, `uc2_snapshot_hash_mismatch` and
  `Uc2SnapshotHashDiverged`.
- `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md`
  §2.5 — the FSM upgrade lifecycle's own spec, with its as-built errata.
