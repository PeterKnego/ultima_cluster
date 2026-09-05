# The cluster FSM and coordinated snapshots — design

*2026-09-05. Status: design accepted by the maintainer in session; this is the
written record. Supersedes the snapshot-carry parts of the time-and-timers
spec (§5 errata, plan 3) and the membership-carry shape M7 gave the snapshot
session. Ships inside the still-unreleased `2.11.0` flag day.*

## 1. Goal and locked decisions

The `2.11.0` work left the cluster with **three** kinds of cluster-wide state
that reach a below-floor joiner by three different mechanisms — membership
carried live on `SNAP_BEGIN`, the schedule table carried live on its own
`SNAP_TABLE` datagram behind a commit gate, and every user FSM's state carried
as a snapshot artifact at whatever position that FSM last chose. Only the last
is right. The first two are **live reads at ship time** of state the joiner
should have received *as of a position*, and the commit gate the table needed
reads a counter (`commit`) that is deliberately not primed at boot
(`uc_log/src/counters.rs:55`), which is how the "restarted node under-ships
for one window" residual came to exist. That residual is a symptom; the cause
is that UC had no node-owned notion of "cluster state as of position P".

This spec removes the category. It draws one line — **node data is local and
never replicated or snapshotted; cluster data is replicated through the log
and snapshotted at one cluster-wide instant** — and builds two things on it:
an internal state machine that owns every piece of non-user cluster data, and a
leader-commanded snapshot instant at which every state machine, internal and
user, freezes at the same log position.

| decision | choice | why (§) |
|---|---|---|
| the line | **node data**: vote, term map, snapshot floor, output progress, `log_time_ns` clamp, crypto session keys, the timer heap. **cluster data**: membership, the schedule table, settings, every user FSM's state. Test: *would the cluster be wrong if two nodes disagreed?* | §3 |
| where non-user cluster data lives | one internal state machine, the **cluster FSM** (`const NAME = "uc_cluster"`), in-node, applied like any FSM, snapshotted like any FSM | §4 |
| how it is changed | **commands on the log**: one frame type `FRAME_TYPE_CLUSTER = 4` (reusing `CONFIG`'s number) with a command-kind byte; admin ops become commands | §4.3 |
| membership's two time bases | one `Membership` command frame, **two consumers**: the consensus kernel keeps its durable-time view (Raft), the cluster FSM applies at commit and is the snapshot authority | §4.6 |
| the snapshot instant | `FRAME_TYPE_SNAPSHOT = 7`, appended by the leader; every user row and the cluster FSM freeze at its frame-end position P | §5 |
| what triggers it | `uc2ctl snapshot [--standby]` (admin op 8) or `settings.snapshot.interval_bytes` of log since the last complete instant, evaluated by the leader from the **replicated** settings record | §5.5, §6 |
| standby instants | `FLAG_SNAPSHOT_STANDBY` in the frame's header flags: only **learners** freeze; voters ignore it and never pay the freeze. Voters obtain the set by a **pull** (`uc2ctl snapshot fetch`, `SNAP_REQUEST` kind 22, a learner serving the session). The freeze-on-a-quorum commit stall (§5.7) is why this is in plan 2, not a door | §5.7 |
| what a set is | every declared row's artifact at P **plus** the cluster FSM's artifact at P; complete or nothing | §5.3 |
| the ship gate | "the complete set at my floor" — committed by construction through apply's own gate, durable across restarts; no counter | §5.4 |
| failure | an incomplete instant is **abandoned**; the next supersedes it; commit, apply and replication never wait on a snapshot; loud metrics | §10 |
| settings | a third replicated record: `fsm_lag`, `admission_bytes`, `snapshot.interval_bytes`; moved keys refused by name in `node.toml`; genesis-seed only | §6 |
| bootstrap boundary | a setting that is a precondition for *reading the log* stays in `node.toml`, enforced by refusal: `app_id`, `[crypto]`, `[services] names`, `max_payload`, `[[members]]` (genesis) | §3.3 |
| retired before shipping | `FRAME_TYPE_SCHEDULE_TABLE = 6`, `DGRAM_KIND_SNAP_TABLE = 21`, `SnapBeginBody.config`, `state/schedules.state`, `ScheduleShip`/`shippable_schedule`/`known_committed`, per-service `SnapshotPolicy` | §7 |
| flag day | inside `2.11.0`'s existing unreleased flag day: wire `0.7.0`, cnc `3.1`. Zero extra cost | §12 |

## 2. Why this shape, and what Aeron does

Aeron Cluster snapshots node state and service state **at one log position,
as sibling recordings in one recording-log entry.** Verified in the Java
source (`aeron-cluster/src/main/java/io/aeron/cluster/`):

- `ConsensusModuleAgent.takeSnapshot(timestamp, logPosition, serviceAcks)`
  (`:3153`) records the consensus module's own snapshot **and** every
  service's snapshot with the same `logPosition`, appending each to the
  recording log with a `serviceId` — the consensus module's entry uses
  `ConsensusModule.Configuration.SERVICE_ID = Aeron.NULL_VALUE`
  (`ConsensusModule.java:450`).
- `snapshotState(publication, logPosition, leadershipTermId)` (`:3221`)
  brackets the consensus module's state with `markBegin`/`markEnd` **stamped
  with `logPosition`**, and between them writes sessions,
  `timerService.snapshot(snapshotTaker)` — the whole timer set — and the
  pending service-message trackers. `ConsensusModuleSnapshotTaker` exposes
  `snapshotTimer(correlationId, deadline)`.
- The instant is a **log entry**: the leader calls
  `appendAction(ClusterAction.SNAPSHOT, timestamp, flags)` (`:2566`); every
  node replays it at the same position (`:1581`); each service sees it via
  `ClusteredServiceAgent.onServiceAction` → `ClusterAction.SNAPSHOT`
  (`:1063`).
- A node that needs a snapshot **replicates the recording**
  (`SnapshotReplication.java:67`, `MultipleRecordingReplication`) — an
  archive-to-archive copy of an immutable artifact, never a live read.

So there is no ship-time gate in Aeron because there is nothing live to gate.
"What was in force at P" is answered by the artifact.

UC took the other path for one structural reason: its snapshot artifacts are
**entirely the service's bytes** — UC ships no store and prescribes no
encoding (CLAUDE.md) — so there was never a node-owned artifact to put node
state in. When plan 3 needed to move the schedule table to a joiner it had
nowhere structural to put it and invented a side channel carrying live state,
which then needed a freshness gate, which then reached for the one counter
that is zeroed at boot. The same live read has carried membership since M6
(`uc_node/src/node.rs:7748`, one line above the table's at `:7751`), with no
gate at all.

The fix is not a node artifact — that was this spec's first draft, and it was
wrong. Under the node/cluster line there is **no node data to snapshot**.
Everything the draft put in a node artifact is cluster data, and cluster data
already has a mechanism: the FSM. So the design is one more FSM, internal, and
one command that makes every FSM freeze together.

What UC gets that Aeron does not have to build: every mechanism the user FSMs
already have becomes the cluster data's mechanism for free — journal replay
after a restart, snapshot + tail-replay for a below-floor joiner, the
hard-crash reconstruction path, `Timed<S>` if it ever schedules, the lincheck
oracle. Three hand-written `StableValue`-plus-`prev` chains become one
`SnapshotStateMachine` impl.

## 3. Node data and cluster data

### 3.1 The inventory, classified

| item | class | durable at | how it reaches a joiner |
|---|---|---|---|
| vote | node | `state/vote.state` | never |
| term map | node (derived from the log, converges by reconcile) | `state/term_map.state` | rederived |
| snapshot floor | node | `state/snapshot.state` | never |
| output progress | node | `state/output_progress.state` | never |
| `log_time_ns` clamp | node (derived) | cnc page 1 `+4048`, re-seeded from a journal walk at boot | rederived |
| crypto session keys, epoch | node | — | handshake |
| timer heap (`RowTimers`) | node (a cache) | — | rebuilt from the service's re-announce + the table |
| **membership** | **cluster** — with a node-side shadow, §4.6 | cluster FSM artifact; shadow in `state/config.state` | cluster FSM artifact at P, then tail replay |
| **schedule table** | **cluster** | cluster FSM artifact | same |
| **settings** | **cluster** | cluster FSM artifact | same |
| user FSM state | cluster | that FSM's artifact | that FSM's artifact at P, then tail replay |
| `Sessioned<S>` session table | cluster (inside the FSM) | inside the artifact | inside the artifact |
| `Timed<S>` pending set | cluster (inside the FSM) | inside the artifact | inside the artifact |

`Timed<S>` is the existence proof: the timer **pending set** has been
snapshot+log state since plan 1 and has never had a carry problem. Only the
node's copies were broken.

### 3.2 The rule

Every cluster-wide setting or datum is a **command on the log**, applied at
its frame position on every node, and captured in the cluster FSM's artifact
at every snapshot instant. Its frame's position is the instant it took
effect; its frame's `time_ns` is the cluster time. Nothing cluster-wide is
read from a per-host file after genesis.

### 3.3 The boundary

A setting that is a precondition for *reading the log* cannot be in the log.
These stay in `node.toml`, and agreement is enforced by refusal at the door
exactly as today:

| bootstrap-class | why |
|---|---|
| `app_id`, protocol and cnc versions | identify the cluster before any frame is decodable |
| `[crypto] enabled` + key paths | the node must open datagrams before it can read a frame |
| `[services] names` | rings and rows exist before the log is read; a joiner needs them to *receive* the set |
| `max_payload` | a peer's frame must be decodable before it can carry a setting |
| `[[members]]` / `[[learners]]` | genesis seed only — already the M7 shape (`recover_config_record`, `node.rs:790`) |

Per-node by nature and untouched: `id`, `bind`, `instance_dir`, `seed`,
`buffer_bytes`, `journal_segment_bytes`, `[log]`, `[metrics]`, `[admin]`
(keys do not belong on a log), `faults`, `election_timeout_{min,max}_ns`
(per-node jitter is desirable — `seed`'s own doc says identical timing splits
the vote), `[purge]` (a local disk decision; with floors aligned, each node
purging under its own slack is still safe), `crypto.rotation_*` (crypto config
is one section and security-sensitive).

## 4. The cluster FSM

### 4.1 Placement, identity, apply loop

**In-node, no cnc slot.** It must be in-process — consensus cannot depend on
an external service being alive to know its quorum — and page 2 is exactly
`ServiceSlot[8]` with no ninth slot (`uc_protocol/src/v2/cnc.rs:33`). It runs
its **own apply loop** over the same `LogBuffer`, on its own thread, publishes
`applied` and `snapshot_pos` in-process, and keeps its artifacts under
`<instance_dir>/snapshots/cluster/`.

It is **outside the lag policy** (`fsm_lag`, lockstep). That is a feature: a
stalled user FSM must not stall the node's view of its own configuration. Its
frames are few and small, so it is never the slow one.

Identity: `const NAME: &'static str = "uc_cluster"`, `const VERSION` bumped
with its state encoding. The `uc_` prefix is **reserved**: `[services] names`
refuses any entry starting with it, by name.

Reuse: `uc_service`'s runtime dependencies are `uc_protocol`, `uc_log`,
`uc_journal` only (`uc_service/Cargo.toml`), so `uc_node → uc_service` is a
legal edge. It flips the crates.io publish order (`uc_service` before
`uc_node`); `uc_service`'s dev-dependency on `uc_node` stays a dev-only cycle
under the existing unversioned idiom. Whether the apply loop in
`uc_service/src/apply.rs` separates cleanly from the cnc slot it publishes to
is a **planning-time check** (§15); the fallback is a small internal loop that
walks frames with the same filter and calls the same trait.

### 4.2 State

```rust
struct ClusterState {
    membership: ClusterConfig,      // uc_consensus::config::ClusterConfig — version, voters, learners, tombstones
    schedule:   ScheduleTable,      // uc_protocol::v2::schedule::ScheduleTable, plus the position it was adopted at
    settings:   Settings,           // §6
}
```

All three are cluster data under §3; nothing else is. The FSM implements
`RawStateMachine` (bytes in, bytes out — its commands are already wire
encodings) and `SnapshotStateMachine` (`freeze` clones the state under the
same lock discipline as a user FSM, `stream_snapshot` writes a versioned
image, `install_snapshot` replaces it at the given position). Timers: none.
`on_timer` stays the provided no-op; table ticks are node-fired (§4.9).

### 4.3 Commands: `FRAME_TYPE_CLUSTER` and its kinds

M14's log is a **broadcast** log: "the log carries no service id and does no
routing" (`docs/notes/uc2-m14-multi-service-explained.md:13`). The frame type
is the only router. So the cluster FSM's commands get **one frame type** and
a kind byte:

```
FRAME_TYPE_CLUSTER = 4          // reuses CONFIG's number; a flag day anyway
body: kind: u8 ‖ reserved: [u8; 7] ‖ payload
kind 1 = Membership     payload = the ClusterConfig encoding CONFIG carries today
kind 2 = ScheduleTable  payload = encode_schedule_table (≤ 1064 B)
kind 3 = Settings       payload = the Settings encoding, §6
```

User apply loops already act on `MESSAGE` and their own `TIMER`s only
(`uc_service/src/apply.rs:480`) and yield everything else; they skip
`CLUSTER` frames for free. The cluster FSM's loop is the mirror image: it acts
on `CLUSTER` and yields the rest. The archive walk that today recognises
`FRAME_TYPE_CONFIG` and `FRAME_TYPE_SCHEDULE_TABLE` in one header scan
(`uc_log/src/archive.rs:475`) recognises `CLUSTER` and reads the kind byte —
no dearer than decoding a `CONFIG` payload is now.

**Retired:** `FRAME_TYPE_SCHEDULE_TABLE = 6` (never shipped),
`append_schedule_table` (`uc_log/src/buffer.rs:807`). `append_config`
(`:732`) becomes `append_cluster(kind, payload)`.

Three kinds and one frame type, not one combined record: the records have
different sizes and change rates — the table alone is 1064 B against the
1344 B payload ceiling — and an operator changing one setting must not
re-sign the whole table. The *unification* is at the snapshot instant: three
records, one artifact, one position.

### 4.4 Validation and responses

Admin ops stop being node code paths that build special frames. `uc2ctl`
validates what only it can — name resolution against the local cnc name lines,
the staged file's digest — signs, and sends; the leader appends the command.

**Validation that decides acceptance lives in `apply`, deterministically, on
FSM state only:** one reconfiguration in flight; a table entry's FSM name
resolves against the membership *the FSM holds*; settings within static
bounds. Anything node-local — `admission_bytes` against this host's
`buffer_bytes` — is a **clamp at the point of use**, never a rejection,
because `apply` cannot see this host.

`apply` returns a response: accepted, or refused with the same reason codes
the admin plane uses today (`40 schedule_digest` … `43 schedule_unknown_fsm`,
and the M7 reconfig refusals). **Phase 1** keeps a pre-append check on the
leader so `uc2ctl` still gets an immediate answer — but that check **is the
FSM's `apply` validation, called on a clone of the FSM's current state**: one
function, not a parallel implementation. On replay the FSM's verdict is
therefore identical by construction, and a disagreement is a fail-stop naming
the position, never a silent divergence.
**Phase 2** (§13) publishes the response on the egress broadcast keyed by
position, so `uc2ctl` waits for the command's real outcome instead of the
leader's guess.

### 4.5 The published view

The node reads cluster state through a **position-tagged view** the cluster
FSM publishes at the end of every `apply` — an `ArcSwap` or the rings'
seqlock idiom, chosen at planning time. Readers:

| reader | reads | today |
|---|---|---|
| timer heap arming (consensus agent) | `schedule` | `state/schedules.state` via `ScheduleShip` |
| the ingress door (consensus agent) | `settings.admission_bytes`, clamped to local `buffer_bytes` | `NodeConfig` at boot (`node.rs:499`) |
| the service apply loops, via the cnc `fsm_lag` word | `settings.fsm_lag` | computed **once at boot** (`node.rs:503`, `:749`) |
| the leader's snapshot cadence | `settings.snapshot` | — |

None of these decides *what is applied*; a momentary skew between nodes can
change timing, never state. That is the test for whether a reader may use the
committed view. The consensus kernel fails it, and does not (§4.6).

### 4.6 Membership: one frame, two consumers

Raft's single-server-change safety depends on a node using the newest
configuration **in its log, committed or not**. That is why `ConfigObserved`
is fed from the archive walk at durability and adopted at any role
(`uc_consensus/src/election.rs:404`, `:966`), with the truncating-latch
machinery fixed at `cc4e321`/`d9a3ac2`. An FSM applies at commit. The two
cannot be the same reader.

**Resolution:** a `Membership` command is a cluster-FSM command like any
other. The cluster FSM applies it at commit and is the **snapshot authority**:
a joiner installing the artifact at P gets the committed membership at P —
correct for a committed P — then tail-replays. *Additionally*, the archive
walk keeps feeding `ConfigObserved` at durability to `ElectionSm`, exactly as
today. That view is **node data with a replicated source**: it can run ahead
of commit, it can be truncated, two nodes may legitimately disagree for a
moment. It keeps `state/config.state`, its one-level `prev`, and its revert.

What runs on the durable-time shadow, and must — commit ranking's quorum
(`election.rs:1768`), vote grant and count, `is_voter` report filtering
(`:67`, `:1052`), the own-id tombstone halt (`:1056`, `:1723`), the
reconfig-in-flight gate, and the sender's fan-out targets and `id_to_addr`.
Every one is downstream of the Raft rule.

The invariant that keeps the two readers honest, in `uc_sim`'s vocabulary
(§11): **at every step, the cluster FSM's membership equals the kernel's
durable-time config at some position ≤ commit, and at every committed position
the kernel's config equals what the FSM applied there.** The FSM's history is
always a committed prefix of the kernel's.

The schedule table and settings have **no** durable-time consumer. Today the
table mirrors `CONFIG`'s leader-at-append/follower-at-walk adoption "for
uniformity" (plan 2); it never needed to be ahead of commit, and under this
spec it is not.

### 4.7 Genesis and recovery

A fresh instance dir seeds the cluster FSM's initial state from `node.toml`:
`[[members]]`/`[[learners]]` for membership (the M7 genesis rule, verbatim),
`[settings]` for settings (§6), an empty table. After the first frame exists
the file's copies are ignored, as `[[members]]` is today.

Recovery is the FSM's own: newest complete set's cluster artifact, then
journal replay of `CLUSTER` frames above it. `state/config.state` recovers
the kernel's shadow separately, as `recover_config_record` does now; the two
reconcile by the invariant in §4.6 and a mismatch is a fail-stop with both
positions named.

### 4.8 Snapshot of the cluster FSM

At a `SNAPSHOT` frame (§5) the cluster FSM freezes at P like every row and
writes `snapshots/cluster/snap-{P}.ultcluster`: magic, image version, the
three records, CRC. Installed by a joiner through `install_snapshot(P, …)`.
A fuzz target covers the decoder, since a joiner installs it by fiat.

### 4.9 What stays node-fired

Table ticks. A tick must be **delivered to the target row** as a `TIMER`
frame, and an FSM cannot append frames, so the node still arms per-row
entries from the view's table and fires them exactly as plan 2 built
(`RowTimers::table_fire_deadline`, `FLAG_TIMER_TABLE`, the one-tick catch-up).
The table simply has one source of truth now.

## 5. Coordinated snapshot instants

### 5.1 The command

```
FRAME_TYPE_SNAPSHOT = 7        // empty body; the position is the identity
header flags: FLAG_SNAPSHOT_STANDBY = 0x01   // the same header byte FLAG_TIMER_TABLE rides in
```

Appended by the leader through the same path as any leader frame, stamped
like every frame. **Broadcast**: every user row *and* the cluster FSM act on
it — subject to the standby flag (§5.7). Its frame-end position is **P**, the
instant.

### 5.2 What every row does at P

The apply loop gains one arm beside `MESSAGE` and `TIMER`
(`uc_service/src/apply.rs:480`): on frame type 7, having applied everything
below its frame-end P, it calls `freeze()` and hands the build to the existing
builder thread, which writes `snapshots/<row>/snap-{P}.ultsnap` and stores
`snapshot_pos = P` in the row's cnc slot — the mechanism that exists today
(`uc_service/src/builder_agent.rs:67`), with the trigger moved from a local
byte counter to the log.

`SnapshotPolicy { interval_bytes }` and `maybe_build_snapshot`
(`apply.rs:627`) are **deleted**. `start_with_snapshots`
(`uc_service/src/lib.rs:225`) stays as the capability opt-in and now also
sets a **capability bit** in the slot's status word (`CNC_SVC_STATUS_SNAPSHOT
= 1 << 9`; bits 9..31 are free, `cnc.rs:277`). `Timed<S>` and `Sessioned<S>`
already wrap `freeze()`; untouched. A row started with plain `start()` has no
capability and **ignores** the frame; if one exists the set is simply
incomplete, which is why §5.5 refuses to command an instant on such a cluster
in the first place.

Determinism is by construction: a row's artifact at P is a function of the
log below P, on every node.

### 5.3 The set, completeness, the floor

A **set** at P = every declared row's artifact at P + the cluster FSM's
artifact at P. Completeness is detected per node by the consensus agent
polling what it already polls: every declared slot's `snapshot_pos == P`
(`node.rs:7702` reads them today) and the cluster FSM's in-process
`snapshot_pos == P`. No acks cross the wire; each node completes its own set
independently, and sets are position-aligned because P is.

On completion, `node_snapshot_floor := P` and purge may proceed below it.
Retention keeps the newest complete set plus any newer in-progress artifacts;
everything else — including orphans from abandoned instants — is garbage.

### 5.4 The ship gate

**"The complete set at my floor."** Assembly (`node.rs:7702`) changes from
"each row's newest artifact" to "the artifacts at the floor".

Why that is committed by construction, and not by the floor rule: the floor
rule is `service_pos <= durable` (`node.rs:3816`), *not* committed. The
guarantee comes one level down. A row freezes at P only after **applying** up
to P, and apply is gated on `min(commit, durable)` — so an artifact at P exists
only if P was committed on that node when it was built. Committed bytes are
never truncated (sim inv4), so P stays committed. A complete set therefore
implies committed P, the floor set from it is ≤ commit, and the floor is
durable across restarts. There is no counter to consult and no boot window in
which one reads zero.

`SnapshotSet.config`, `SnapshotSet.table`, `shippable_schedule`,
`ScheduleShip`, `known_committed` and the position-0 rules **all go**.

### 5.5 Triggers, single in flight, capability

Leader-only, two triggers:

- **`uc2ctl snapshot [--standby]`** — admin op **8**, `snapshot`, the flag
  carried in the request. On a follower: `retry` with the leader hint, as
  `schedule apply` does. Audited as `snapshot`. `--standby` is refused
  `49 snapshot_no_learner` when the cluster FSM's membership has no learner.
- **Cadence** — the leader appends `SNAPSHOT` when
  `settings.snapshot.interval_bytes` of log has accrued since the last
  *complete* instant on the leader, flagged per `settings.snapshot.target`
  (§6). Read from the replicated settings, so the cadence and the target are
  the same on whichever node leads. With `target = learners`, "complete"
  means complete on the leader **by fetch** (§5.7), so cadence does not
  outrun the pull.

Both are **refused with a named reason** (`48 snapshot_unsupported`, naming
the row) if any declared row lacks the capability bit — a cluster with a
non-snapshotting FSM can never complete a set, so it should be told rather
than left with a floor that never moves. That cluster is legitimate (purge is
off by default) and simply never snapshots.

**Single in flight, with supersession:** the leader appends no new
`SNAPSHOT` while its own set at the last commanded P is incomplete — until
either that frame is truncated, or a further `interval_bytes` of log has
accrued since it, at which point the incomplete instant is **abandoned** and a
new one issued. `uc2ctl snapshot` always supersedes. The completeness signal
of §5.3 is the gate; no new protocol. This is what makes "more than two
instants behind" (§9's alert) a reachable condition rather than a stall the
leader itself would never move past.

### 5.6 The session

`SNAP_BEGIN` (layout **V4**) carries **one** `snapshot_pos` for the set — every
`SNAP_BEGIN` of a session must agree, refused otherwise — and **loses**
`config: Vec<u8>`. The cluster FSM's artifact streams as one more artifact
under the reserved `service_id = 255`, ascending order preserved (255 last).
The set-coverage invariant (`uc_net/src/sender.rs:1177`, which today refuses
any `service_id >= 64` or one outside the declared mask) gains an explicit
carve-out: exactly one artifact with id 255 is **required**, and it is not
part of the declared mask.
`DGRAM_KIND_SNAP_TABLE = 21` is **retired before it ever ships**. The joiner
writes the artifacts, installs the cluster artifact through the cluster FSM's
`install_snapshot(P, …)`, adopts floor P, and the kernel's shadow is seeded
from the installed membership (a joiner below the floor has no durable-time
history to be ahead with).

### 5.7 Standby instants — who freezes, and how the set comes back

**The problem a plain instant creates.** A row's `freeze()` runs on its apply
thread — the reference typed-tier implementation serialises the whole state
inline under the lock (`uc_lincheck/src/register.rs:78–84`) — so a freeze is
O(state) during which that row's `applied` does not move. A node's durable
report is capped at `min(validated_up_to, min_applied + fsm_lag_eff)`
(`uc_node/src/services.rs:351`, M14a's report ceiling). M6's per-service
byte triggers staggered by accident; a coordinated instant freezes **every
row on every node at the same P**, so a quorum's reports all cap at
`P + fsm_lag` and **commit stalls cluster-wide** until the slowest freeze
ends. With `fsm_lag = buffer_bytes / 4` and a 64 MiB buffer that is 16 MiB of
runway — of the order of 150 ms at 100 MB/s of appended log (an illustration,
not a measurement; §11's gate row makes it one). UC cannot bound the freeze,
because the artifact and the freeze are the service's own bytes and code.
That is Aeron's pause arriving through the back door, and the only lever that
works for any state size is not freezing the voters.

**Aeron's shape, verified** (`ConsensusModuleAgent.java:2575–2583`,
`:1581`; `service/ClusteredServiceAgent.java:852`, `:1094–1097`): the standby
snapshot is the **same** `SNAPSHOT` action with a flag. A member's consensus
module never enters the `SNAPSHOT` state for it (the replay handler pauses
only on default flags), and a member's services skip it (`shouldSnapshot`
accepts the standby flag only when attached to a standby log). The return
path is a **pull**: the standby announces its recordings to the leader
(`:1283`), and a member copies them archive-to-archive when an operator flips
the `REPLICATE_STANDBY_SNAPSHOT` toggle (`:2671–2690`) or at startup
(`:3603`). The producing standby node is not in the open-source tree; only
the consumer half is.

**UC's version.**

1. **The flag.** `FLAG_SNAPSHOT_STANDBY` in the frame's header flags byte.
   The body stays empty.
2. **Role visible to the row.** The node writes one cnc status bit per row,
   `CNC_SVC_STATUS_LEARNER = 1 << 10`, from the kernel's durable-time
   membership shadow (role is consensus-plane), republished on every
   adoption. A row acts on a standby-flagged instant **only if that bit is
   set**; a voter's rows yield the frame like any other node-only frame. The
   cluster FSM follows the same rule from the same shadow.
3. **Completeness is unchanged.** §5.3 already works per node: the learner
   completes its own set at P and its floor moves. A voter has no set at P
   and its floor does not move — yet.
4. **The return path.** A voter needs the set for its purge floor and to
   serve joiners (§5.6). The snapshot session already carries a set node to
   node; what is missing is a trigger other than a below-floor NAK, and a
   serving side other than the leader. So: `DGRAM_KIND_SNAP_REQUEST = 22`
   (`session ‖ position`), sent by a voter to a learner; the learner's
   sender — every node's sender already holds the `SnapshotSource`
   (`node.rs:1077`) — opens an ordinary session for the set at that position
   from its own artifacts, and the voter's receiver takes it in a new
   **store-only** mode: artifacts are written and the set is marked complete
   at P, but nothing is installed by fiat and the floor is adopted through
   §5.3's ordinary completion path, not through `snap_complete`'s install
   (`uc_net/src/receiver.rs:2415`). A voter above P storing a set at P is
   not a joiner; it must not be treated as one.
5. **Who initiates the pull.** `uc2ctl snapshot fetch --from <learner-id>
   [--position P]` (admin op **9**, `snapshot_fetch`, runs on the voter it is
   pointed at; leader-local, not a cluster command — it changes nothing
   cluster-wide). Default position: the learner's newest complete set. That is
   Aeron's open-source half exactly: flagged instants plus an operator-driven
   replicate. **Automatic** replication — a voter fetching on its own once the
   learner announces completion — is deferred (§13): it needs the learner to
   publish "complete at P" somewhere voters can read it, and the honest
   channel for that is a cluster-FSM command the leader appends on the
   learner's behalf, which is a fourth kind and a design of its own.
6. **Until a voter has fetched**, its floor stays where it was and a joiner
   is served by whichever node holds the set — the leader answers a
   below-floor NAK it cannot serve with a **redirect** to a learner that can
   (`SNAP_REDIRECT`, kind 23, `learner id ‖ position`), and the joiner sends
   its `SNAP_REQUEST` there. Purge on voters waits for the fetch, which is
   the operator's trade for not paying the freeze.

`settings.snapshot.target = all | learners` (§6) selects the flag for
cadence-issued instants; `uc2ctl snapshot --standby` overrides per command.

## 6. The settings record

```rust
struct Settings {
    version:         u32,           // encoding version
    fsm_lag:         FsmLag,        // was [services] fsm_lag — per-host, "must match cluster-wide"
    admission_bytes: u64,           // was top-level admission_bytes — the leader's ingress window
    snapshot:        SnapshotCadence { interval_bytes: u64, target: Target /* All | Learners */ },   // 0 = on demand only
}
```

Three settings, chosen from the full `NodeConfig` field list
(`node.rs:167–229`) as the only cluster-wide policy living per-host:
`fsm_lag`, which CLAUDE.md says "must match cluster-wide" — a check this
spec's author could not find, and which becomes true by construction;
`admission_bytes`, whose effective value silently changed on failover; and the
new cadence. The moved keys are **refused by name** outside `[settings]`,
pointing at `uc2ctl settings apply` — the `services.ids` posture. `[settings]`
in `node.toml` is a **genesis seed only** (§4.7); its absence seeds defaults.

`uc2ctl settings apply <file.toml>` is `schedule apply`'s shape verbatim:
staged file, first 80 bits of its SHA-256 signed into the admin line, admin op
**7** `settings_apply`, leader-only, single in flight; `uc2ctl settings show`
prints the adopted record. Refusals `44 settings_digest`, `45
settings_missing`, `46 settings_decode`, `47 settings_bounds`.

## 7. Wire, cnc and SDK changes — one flag day

All inside `2.11.0`'s unreleased `0.7.0` / `3.1`:

| surface | change |
|---|---|
| frame types | `4` becomes `CLUSTER` (was `CONFIG`); `6` retired; `7` = `SNAPSHOT` with header flag `FLAG_SNAPSHOT_STANDBY = 0x01` |
| datagrams | `SNAP_BEGIN` layout V4 (one `snapshot_pos`, no `config`); kind `21` retired; `22` = `SNAP_REQUEST`, `23` = `SNAP_REDIRECT` |
| cnc 3.1 | status bit 9 = snapshot-capable, bit 10 = this node is a learner; the `fsm_lag` word becomes node-republished on settings change |
| admin ops | `7 settings_apply`, `8 snapshot` (with `--standby`), `9 snapshot_fetch`; refusals `44–49` |
| service SDK | `SnapshotPolicy` and `interval_bytes` **removed**; `start_with_snapshots` sets the capability bit; no trait change |
| `node.toml` | `[settings]` (genesis seed); `admission_bytes` and `services.fsm_lag` refused outside it; `uc_` names refused |
| instance dir | `snapshots/cluster/`; `state/schedules.state` never exists; `schedules.pending` and a new `settings.pending` |
| crates | `uc_node` depends on `uc_service` (publish order flips) |

The SDK removal is a breaking change inside a minor and rides the
FSM-identity carve-out in `docs/reference/semver-policy.md` (no external
users; the maintainer's standing ruling).

## 8. `uc2ctl`

| verb | op | notes |
|---|---|---|
| `snapshot [--standby]` | 8 | leader-only; `retry` + hint on a follower; refused `48` naming the row, `49` if `--standby` and no learner |
| `snapshot fetch --from <id> [--position P]` | 9 | runs on the voter it targets; pulls a learner's complete set store-only (§5.7) |
| `settings apply <file>` | 7 | `schedule apply`'s shape; `settings show` reads the adopted record |
| `schedule apply` / `show` | 6 | unchanged surface; now a `CLUSTER kind=2` command |
| `add-learner` … `remove-voter` | 1–5 | unchanged surface; now `CLUSTER kind=1` commands |
| `snapshot show` | — | the newest complete set's position and each artifact's presence; the diagnostic for a stalled instant |

## 9. Observability

Metrics: `uc2_snapshot_instant_position` (last commanded P, leader),
`uc2_snapshot_set_position` (last complete set, every node — **must agree
cluster-wide once caught up**), `uc2_snapshot_row_incomplete_total{row}`
(instants a row failed to reach), `uc2_snapshot_freeze_seconds{row}` (a
histogram of freeze duration — the number §5.7's stall argument turns on),
`uc2_snapshot_fetched_position` (a voter's newest pulled set),
`uc2_cluster_fsm_position` (its `applied`), `uc2_settings_position`. Alerts: `Uc2SnapshotStalled` when commanded and
complete diverge for more than two instants — "one broken FSM silently stops
all purging", made loud; `Uc2SnapshotSetDiverged`, the
`Uc2ScheduleTableDiverged` shape over `uc2_snapshot_set_position`.
`Uc2ScheduleTableDiverged` itself keys on the cluster FSM's position instead
of a table position. Both new rules get `m10_alert_fire.sh` builders and
scenarios, so the M10 gate's completeness cross-check stays green.

Log records: `snapshot_commanded` (leader), `snapshot_set_complete`,
`snapshot_instant_abandoned` (with the rows that never arrived),
`cluster_command_applied` (kind, position, accepted/refused).

## 10. Failure modes

| case | behaviour |
|---|---|
| a row never reaches P (freeze error, slow, service process dead) | the set stays incomplete; nothing advances; the next instant supersedes; retention drops the orphans; `uc2_snapshot_row_incomplete_total{row}` counts it |
| `SNAPSHOT` frame truncated by a leader change | any artifact built at that P is an orphan; the new leader's next instant is a fresh P |
| cluster FSM stalls | the node's view stops advancing: stale table/settings, timing only; the kernel's shadow is unaffected; `uc2_cluster_fsm_position` shows it |
| service restarts mid-build | `busy` clears with the process; the row is incomplete for that instant, exactly as above |
| joiner served a set, then the shipper restarts | the set is at the shipper's durable floor; no window, no counter — the residual this spec exists to remove |
| `[settings]` disagrees between hosts at genesis | the first frame wins; the file is ignored thereafter; `uc2ctl settings show` is the truth |
| a `uc_`-prefixed name in `[services]` | startup refusal by name |
| a freeze on a quorum outlasts `fsm_lag` of appended log | commit stalls at `P + fsm_lag` until the slowest freeze ends (§5.7) — by design, and the reason standby instants exist; `uc2_snapshot_freeze_seconds` shows it |
| a standby instant with the only learner down | no node completes the set; the instant is abandoned like any other; voters were never asked to freeze |
| a voter purges below a P it fetched, then the learner that produced it is lost | nothing: the voter **holds** the set, which is why the floor moves only on fetch and never on a learner's announcement |

What this spec does **not** claim to fix: the sub-millisecond window between
the archive recording a `CLUSTER` frame and the cluster FSM's state being
durable is the ordinary "crash between apply and snapshot" case every FSM has,
closed by replay — it is no longer a special table-adoption window, but it is
not zero either, and the cluster FSM's recovery (§4.7) is what closes it.

## 11. Test plan and acceptance

- **Sim** (`uc_sim`): inv11, *set alignment* — every node's sequence of
  complete-set positions is a prefix of one common sequence; inv12, *two
  readers* — the §4.6 invariant between the cluster FSM's membership and the
  kernel's durable-time config. A directed scenario staging a `SNAPSHOT`
  frame truncated by a leader change. A red twin that feeds the kernel from
  the committed view (the wrong reader) and pins that inv7 or inv4 fires.
- **Learner** (`uc_node/tests/learner.rs`): the residual, exactly — a leader
  ships a set, is **restarted**, and serves a joiner before its first commit
  advance; the joiner must install the table and membership at P. **Red
  today** (`(0, 0, [])` is shipped), green after. Plus: a joiner with a
  `uc_`-prefixed name is refused; a cluster with one non-snapshotting row
  refuses `uc2ctl snapshot` with `48`.
- **Timers** (`uc_node/tests/timers.rs`): the existing nine pass unchanged
  in outcome; the table is read from the view.
- **lincheck** (`lin_v2`): the purge/snapshot-churn capstone re-run with
  commanded instants driving the churn instead of per-service intervals.
- **Hard crash** (`uc_crashtest`): SIGKILL a service mid-build at P; assert
  the instant is abandoned, the next completes, and the history stays
  linearizable.
- **Fuzz**: `uc_node_cluster_artifact` (the cluster image decoder),
  `uc_protocol_cluster_frame` (the kind-dispatched body), `SNAP_BEGIN` V4
  seeds in `uc_protocol_datagram`.
- **Standby** (`uc_node/tests/learner.rs`): a standby-flagged instant on a
  3-voter + 1-learner cluster leaves every voter's `applied` moving and
  completes only on the learner; `uc2ctl snapshot fetch` lands the set on a
  voter store-only with its FSMs untouched and its floor advanced; a joiner
  below the voters' floor is redirected to the learner and converges.
- **Gate rows** added to `uc2-time-and-timers-gate-2026-09-03.md`: commanded
  instants under the throughput load (cost of the extra arm in the apply
  hot path — A/B'd, per M14a); a below-floor join with the shipper restarted
  mid-window (time to converge, and that it converges at all); and **freeze
  duration vs commit stall** — a deliberately large state (hundreds of MiB)
  under the throughput load, measuring `uc2_snapshot_freeze_seconds` against
  the observed commit pause for an all-nodes instant, then the same instant
  `--standby`, so the report-ceiling interaction is a number rather than an
  argument.
- **Acceptance**: every retired symbol in §7 is gone from the tree; the
  backlog's under-ship and wiped-node residuals are closed by name; the
  differential timer test from `6ea3325` still passes.

## 12. Release and docs

Ships in `2.11.0`, whose release was **stopped** for this on 2026-09-05
(version already bumped in-tree, no tag). `RELEASES.md`'s drafted section
gains a fourth feature bullet and loses the `SNAP_TABLE` prose; the
explainers `uc2-log-time-and-timers-explained.md` (schedule-table section)
and `uc2-m14-multi-service-explained.md` (snapshot set) are amended; a new
explainer `uc2-cluster-fsm-explained.md` carries §2–§4's argument;
`state-machine-contract.md` loses `SnapshotPolicy`; `configuration.md` gains
`[settings]` and the refusals; `uc2ctl.md` gains the verbs; `limits.md` drops
the two closed residuals; `upgrade-a-cluster.md`'s 2.11 section names the
`node.toml` edits. The how-to
`docs/how-to/run-work-on-a-schedule.md` stays correct as written.

## 13. Out of scope and doors left open

- **Phase 2, command responses via egress** (§4.4): `uc2ctl` waiting on the
  FSM's verdict, and `* show` as linearizable queries of the cluster FSM.
- A replicated form of `election_timeout_*`, `[purge]` or `crypto.rotation_*`
  if per-host ever bites; each has a stated reason to stay local (§3.3).
- **Automatic standby replication** — a voter pulling a learner's set
  without an operator's `snapshot fetch`. Needs the learner's completion to
  be visible cluster-wide (a cluster-FSM command the leader appends on the
  learner's behalf) — a design of its own, deferred as Aeron's open-source
  half defers it (§5.7).
- Timezones and cron in the schedule table: unchanged, still out.

## 14. Implementation order

Three plans, each shippable to `main` on its own with the tree green:

1. **The cluster FSM** — the internal apply loop, `FRAME_TYPE_CLUSTER`, the
   three kinds, the view, genesis, the two-reader invariant in the sim, the
   settings record and `uc2ctl settings`. Membership and the table move in;
   `SNAP_TABLE` and `schedules.state` go; the session ships the cluster
   artifact under id 255 at *the cluster FSM's newest* position, relying on
   the existing M14c per-row resume rule (each row installs its own artifact
   and resumes from its own position) — per-row independence remains for one
   plan, and the cluster artifact is committed-by-construction on its own
   (§5.4's argument holds per row).
2. **Coordinated instants** — `FRAME_TYPE_SNAPSHOT`, the apply-loop arm, the
   capability bit, set completeness, the floor, the V4 session, `uc2ctl
   snapshot`, cadence, retention, the alerts, the learner test that is red
   today — **and standby instants** (§5.7): the flag, the learner bit,
   `SNAP_REQUEST`/`SNAP_REDIRECT`, the store-only receive, `snapshot fetch`.
   In this plan, not a later one, because without it a coordinated instant
   is a cluster-wide commit pause for any state large enough to matter.
3. **Retirement and proof** — every §7 symbol deleted, the gate rows, the
   explainer, the release writeup.

## 15. Planning-time checks

Facts this spec rests on that were not verified in session, each to be
settled before the corresponding task is written:

1. Whether `uc_service/src/apply.rs`'s loop separates from the cnc slot
   cleanly enough to run in-node without a slot (§4.1). Fallback named.
2. Whether any consensus-plane reader of membership exists that §4.6's list
   missed — the list came from `election.rs` and `node.rs` greps for
   `config()`; the sender's target derivation should be read, not grepped.
3. The `fsm_lag` cnc word's consumers in the service apply loop, so its
   republish on a settings change is observed at a safe point.
4. That `install_snapshot` on a user FSM tolerates a set position equal to
   the FSM's own `applied` (a row that snapshotted at P and then installs
   P — a no-op today?).
