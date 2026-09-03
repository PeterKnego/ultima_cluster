# Log time and timers, explained

A state machine in `ultima_cluster` may not read a clock. That rule is not
negotiable: two replicas that call `SystemTime::now()` inside `apply` get two
different answers, compute two different states, and have silently forked.
No consensus layer can detect that for you.

So how does a replicated state machine express "expire this lease in 30
seconds", or "charge this subscription at 14:00"? It reads the clock the
*log* carries, and it asks the log to wake it up.

This page explains how that works. The design spec is
[`docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md`](../superpowers/specs/2026-09-02-uc2-time-and-timers-design.md);
this page is the plain-language version. The companion page on how an FSM is
named, and how it mints deterministic IDs, is
[the FSM identity explainer](uc2-fsm-identity-and-deterministic-ids-explained.md).

## Time is data on the tape

The whole idea fits in one sentence: **the leader writes the current time
into every frame it appends, and the state machine reads it back off the
frame instead of asking the operating system.**

Everything else follows from that. Here it is in seven parts.

### 1. The leader writes the clock into the log

Every log frame has a 32-byte header. Since wire `0.7.0` that header carries
a `time_ns` field: nanoseconds since the Unix epoch, read from the leader's
wall clock. The leader reads its clock **once per pass** of its consensus
agent, not once per frame, so a burst of a thousand commands appended in one
pass all carry the same stamp. That is fine. Equal stamps are allowed and
expected. Order comes from the byte position, never from the stamp.

The field cost nothing on the wire. The old header had two 64-bit id fields
of which the client only ever filled 32 bits each, so narrowing them to
`client_id: u32` and `seq: u32` freed exactly the 8 bytes `time_ns` needed.
The header is still 32 bytes and the command payload ceiling is still 1344
bytes (1312 with wire crypto on). The layout is in
[the wire protocol reference](../reference/wire-protocol.md#log-frames).

One rule guards it: **the log's time never goes backwards.** The stamp
written is `max(now, last_stamp)`. If the leader's clock steps backwards, or
a new leader has a clock behind the old one's, the stamps hold flat at their
last value until wall time catches up. They never decrease. That clamp lives
inside `uc_log::Appender`, the one place every frame type is written, so it
is a property of the log rather than of any caller.

The clamp survives a restart: the archive recovers the last stamp from the
journal when it opens, and the node publishes it into the control page before
any agent runs — so a node that reboots and immediately wins an election
still clamps against the previous leader's last stamp. The only restart with
no seed is a fresh instance directory, where the journal is empty and the
first stamp is plain wall time.

### 2. The state machine reads the tape

`apply` receives an `&mut ApplyCtx`. It carries `ctx.position` (the frame's
absolute byte offset, the idempotency key) and now also `ctx.time_ns` and
`ctx.term`. `ctx.time_ns` is the frame's stamp, exactly as the leader wrote
it, exactly as every other replica sees it.

That is your `now()`. It is deterministic because it is a field of a
committed frame, the same on the leader, on every follower, on a replica
rebuilt from the journal, and on a replica rebuilt from a snapshot.

`query` gets no time. A read has no position that means the same thing on
every replica, and a time would be no better defined than a position.

### 3. A schedule is a note in your own state

`ctx.schedule(id, at_ns)` says "call me back at this log time, with this id".
`ctx.cancel(id)` withdraws it. The id is yours: a `u64` you pick, unique per
FSM. Re-scheduling an id that is already pending replaces its deadline.

The timer carries **no payload**. If a timer needs context, the state machine
keeps that context in its own state, keyed by the id, the way it keeps
everything else. This is deliberate: a payload would put service bytes in the
node's memory and under the payload ceiling, for nothing the FSM cannot look
up itself.

Because `schedule` and `cancel` are calls made *inside* `apply`, they are
outputs of a deterministic function of the log. Every replica's `apply`
makes the same calls in the same order. So every node builds the same set of
pending timers, without any of them talking to each other about it.

Recurrences an *operator* owns rather than the code — "02:00 daily", "every
ten minutes" — come from the other direction, a replicated table applied with
`uc2ctl`. Same heap, same frame, same `on_timer`: see
[The schedule table](#the-schedule-table) below.

### 4. The leader places the wake-up at the right spot on the tape

Each node keeps the pending set in memory as a heap, per FSM row, whatever
its role. Only the leader ever fires from it. When it does, it appends a
`TIMER` frame (frame type 5) whose 24-byte body is
`identity_hash ‖ timer_id ‖ deadline_ns`: the FSM this timer belongs to, the
id it asked for, and the deadline it asked for.

The interesting part is *where in the log* that frame lands. Each leader
pass runs in this order:

1. read the clock once, into `now`;
2. while the earliest pending deadline is `<= now`, append a `TIMER` frame
   stamped with that **deadline** and pop it (bounded at 64 per pass);
3. only then append this pass's client commands, stamped with `now`.

That ordering buys the property that makes timers useful: **when a timer
fires on time, no frame before it on the log claims a time after its
deadline.** The next section walks the argument.

### 5. Every replica gets the wake-up by playing the tape

A follower does nothing special. It receives the `TIMER` frame like any other
frame, writes it at its position, records it, and its service's apply loop
delivers it. The delivery is `on_timer(&mut self, ctx, ev)`, a **provided**
trait method on both the typed and the raw tier, defaulting to a no-op. An
FSM that does not care about timers compiles and behaves exactly as before.

A `TIMER` frame is the first per-FSM frame this system has. Ordinary commands
are broadcast: every declared FSM applies every `MESSAGE` frame. A `TIMER`
frame instead names one FSM by its identity hash, and every other row's apply
loop skips it (while still counting it as a yielded frame, so lag and
lockstep accounting is unchanged).

`ctx.position` is the timer frame's own position, and the state machine
advances `last_applied` from it exactly as it does in `apply`. That is what
makes re-entry after a restart idempotent.

### 6. Two safety nets, because a leader can die mid-fire

The node layer promises **at least once**. If a leader appends a `TIMER`
frame and then loses leadership before the service has confirmed it, the
instance is put back into the pending set, and the next leader fires it
again. That is a deliberate choice: it makes a *missed* timer impossible.
The only way an instance leaves every node's heap is a confirmation from a
service that actually saw the frame on the log.

The service layer turns that into **exactly once**, if you want it.
`uc_service::Timed<S>` wraps your state machine and keeps the pending set
that your own `schedule`/`cancel` calls implied. It delivers a `TIMER` frame
to your `on_timer` only when `(id, deadline)` is still pending, then removes
it. A duplicate finds nothing pending and is dropped. Every replica drops it,
identically, because the decision reads nothing but committed frames.

Without `Timed` your state machine gets at-least-once timers. That is the
same trade as running without `Sessioned`: correct under a contract you have
to honour, weaker than the wrapper.

### 7. Snapshots carry the pending list

`Timed<S>` implements `SnapshotStateMachine` the way `Sessioned<S>` does: it
writes its two small maps ahead of the inner state machine's artifact, and
reads them back on install. A replica that installs a snapshot therefore
knows which timers were pending at the snapshot position, and a replica that
replays the journal instead rebuilds the same maps by re-running `apply`.
Both paths end at the same state, by construction.

The node's own heap is *not* persisted anywhere. It is a cache of what the
services know. After an attach or a replay the service re-announces its whole
pending set to the node over the schedule ring, and the heap converges. A
node whose service is still catching up fires a timer late, never not at all.

## Why an on-time timer is never overtaken

This is the argument the ordering rests on. It is short, because the clock is
read only once per pass.

A timer has deadline `D`. It fires in pass `k`.

- In pass `k-1`, step 2 ran and did **not** fire it. So that pass's `now` was
  below `D`, and every client frame in pass `k-1` and every pass before it is
  stamped below `D`.
- In pass `k`, step 2 fires it before step 3 appends any client frame. So the
  `TIMER` frame is on the log before anything stamped `now >= D`.

```
pass k-1:  client frames stamped t1        t1 <  D
pass k:    TIMER        stamped D
           client frames stamped t2        t2 >= D
```

If step 2 hits its per-pass bound of 64 timers, step 3 is skipped entirely
for that pass. Interleaving one client frame between two due timers would
stamp it above a later timer's deadline, which is exactly what this is
protecting. Clients see one pass of backpressure, the same shape they see on
a `WouldOverrun` today.

### The late case, which is not a correctness loss

There is one situation the invariant cannot hold, and the design says so out
loud instead of hiding it.

Suppose the leader stamped frames past `D` and then died before firing the
timer. The new leader's `last_stamp` is already above `D`. It cannot write
`D` without making the log's time go backwards, which is the one thing the
clamp forbids. So it writes `last_stamp` in the header and leaves `D` in the
body:

```
header time_ns = last_stamp   (what the log's clock says now)
body   deadline_ns = D        (what was asked for)
```

`ev.late(ctx)` is `ctx.time_ns > ev.deadline_ns`, so the state machine can see
exactly how late it is, in nanoseconds, and decide what that means for its
own logic. A deadline that was already in the past when it was scheduled is
handled the same way: it fires next pass, marked late.

This is an operating-system timer firing late under load, not a broken
guarantee. The log's time stays monotone and no frame before the timer claims
a time after the timer's own stamp.

**The contract, in one line:** a timer is never delivered early; when it is
delivered on time, no earlier frame is stamped past its deadline; when it is
late, it says so.

## The schedule table

Everything above is *programmatic*: a state machine schedules its own
callbacks, from inside `apply`, because it decided to. That covers a lease
expiry or a retry backoff, where the FSM already knows when it wants to be
woken. It does not cover the other half of the problem — "run the nightly
reconciliation at 02:00", "sweep stale carts every ten minutes" — where the
person who knows the schedule is the operator, not the code.

For that there is a **replicated schedule table**: a small list of
recurrences, applied once by an administrator, adopted by every node from the
log, and turned into ordinary `TIMER` frames by whichever node is leading.
Applying it is one command:

```
uc2ctl schedule apply schedules.toml --instance-dir /var/lib/uc2 --app-id prod --admin-key ops.key
```

```toml
[[schedule]]
fsm    = "orders"
id     = 1
every  = "10m"
anchor = "2026-01-01T00:00:00Z"

[[schedule]]
fsm = "orders"
id  = 2
at  = "02:00"           # daily, UTC

[[schedule]]
fsm  = "kv"
id   = 100
once = "2026-12-24T18:00:00Z"
```

The FSM sees these in the same `on_timer` it already has, with `ev.table`
true, and `ev.id` the id the file gave. Three rules exist and no more:
`every` (a period from an anchor), `at` (daily, UTC) and `once`. There is no
timezone and no cron syntax; a cron-shaped rule would be a fourth kind byte,
and that door is deliberately left open rather than walked through now.

### How a table becomes a set of ticks

The table is a frame on the log, exactly like the cluster config is.
`FRAME_TYPE_SCHEDULE_TABLE = 6` carries the encoded table — an 8-byte header
plus one 33-byte entry per rule, at most `MAX_SCHEDULE_ENTRIES = 32`, so a
full table is 1064 bytes and always fits one datagram. The leader adopts it at
append; every other node adopts it from the archive's header walk, the same
path that adopts a `CONFIG` frame. Each node then persists it as a
`ScheduleRecord` in `state/schedules.state` and arms every entry.

From there a table entry behaves like a programmatic timer: it sits in the
same per-row heap, fires as the same `TIMER` frame, and reaches the same
`on_timer`. Only the flag differs — `FLAG_TIMER_TABLE` is set — and three
things follow from where a table entry's *next* deadline comes from.

**The node advances the entry at append, not at delivery.** A programmatic
timer instance is created by the FSM and confirmed by the FSM; the node's heap
is a cache of what the service told it. A table entry is not: it exists
because an operator said so, and the leader must keep it on schedule whether
or not its own service is keeping up. So when the leader appends a table tick,
it immediately computes the next occurrence and re-arms. Followers cannot do
that — they never append — so they advance instead on the `TableConsumed`
report their own service sends after delivering the tick. That is what makes a
new leader start from what its own service last saw rather than from wherever
the old leader's clock had got to. A leader whose service lagged may re-fire a
tick the old leader already fired; `Timed` drops it, the same at-least-once /
exactly-once split every timer here has.

**A truncated table tick is not re-fired.** When a leader loses leadership
with programmatic instances in flight, they are re-armed, because a
programmatic timer has no successor: nothing else will ever produce it, so
dropping it would mean losing it. A table tick does have a successor — the
next occurrence, which is already armed. Re-arming a cut tick would put two
deadlines in the heap for one entry and fire the entry twice for one period.
So a truncated table tick is simply gone, and the next tick arrives on
schedule.

**A due entry fires at the latest occurrence, not the next one.** This is the
one-tick catch-up, and it is the difference between a cluster that comes back
from an hour of downtime and a cluster that spends the next hour replaying it.
Suppose a `every = "1s"` rule and a cluster that was down for an hour. Its log
clock is an hour behind wall time — it is the stamp of the last frame anyone
recorded. If the entry armed at "one second after the last delivered tick" and
advanced by one period per fire, the leader would append 3 600 ticks as fast
as it could, each of them late, before catching up to now. Instead
`RowTimers::table_fire_deadline` picks the **latest** occurrence at or before
the leader's clock. One tick fires, marked late, and the entry continues from
*it*. Which occurrence is due is the one clock-driven choice the determinism
rule permits, and even that is only made once, by the leader: the chosen
deadline rides the frame, so every replica advances from the same number.

### Why a `once` parks instead of leaving

A `once` rule has exactly one occurrence. The obvious implementation is to
drop the entry after it fires. That would be wrong in a way that only shows up
the second time an operator applies a file.

Applying a table **replaces** the whole table — there is no add or delete
verb, and removing an entry means applying a file without it. So an operator
who adds one new `every` rule re-applies a file that still contains last
week's `once`. If the `once` had left the table on firing, adopting the new
table would arm it again, and it would fire a second time — from a file the
operator did not think they were changing.

So a fired `once` **parks**: it stays in the table, marked delivered, with no
next deadline. Re-adopting a table containing the same `(fsm, id, time)` finds
it already delivered and leaves it parked. Changing its time, or its id, makes
it a different entry, and that one arms and fires. A parked entry still counts
in `uc2_schedule_entries` — it is in the table — but not in
`uc2_timers_pending`, which counts deadlines that are actually waiting.

### Removing an entry, and reading the table back

To remove one entry, apply a file without it. To empty the table, apply a file
with no `[[schedule]]` entries at all. To see what a node currently holds:

```
$ uc2ctl schedule show --instance-dir /var/lib/uc2 --app-id prod
position=8192 time_ns=1788000000000000000
fsm=orders id=1 rule=every 10m anchor 2026-01-01T00:00:00Z
fsm=orders id=2 rule=at 02:00:00
```

`schedule show` reads `state/schedules.state`, so it tells you what this node
**adopted**, not what somebody staged. `uc2ctl status`'s `config:` line prints
the same position as `schedule_position=`, and `uc2_schedule_table_position`
exports it.

### How the bytes get to the leader safely

The admin request line in the control page is 64 fixed bytes, and the HMAC
covers exactly those bytes. A 1064-byte table cannot ride it. So the table
takes a second path and the request carries a **fingerprint** of it:

1. `uc2ctl` parses the TOML, resolving each `fsm = "…"` name to its identity
   hash through the node's own cnc name lines — so an undeclared name is
   refused locally, with the entry index, before anything is written.
2. It encodes the table and writes it to `<instance_dir>/schedules.pending`,
   mode `0600`, fsync, rename, so the node never reads a half-written file.
3. It sends admin op 6, whose signed `id ‖ ip ‖ port` fields carry the first
   80 bits of SHA-256 over those bytes.
4. The node verifies the request as it verifies every admin request, reads the
   staged file back, recomputes the digest, and refuses unless it matches.

Under `[admin] auth = "hmac"` that makes the table's *contents* authenticated,
not just the request: the file the operator signed is the file the cluster
adopts. Under the filesystem policy it is trusted the way every admin request
already is — anyone who can write the instance directory is host-compromise
equivalent, and always was.

Two properties of that path are worth knowing before you script around it.
**Apply is leader-only**: the staged file is node-local, so a follower can
neither forward the request (the leader has no such file) nor act on it
(a follower cannot append). It answers `retry` with the leader hint. And
**one table may be in flight at a time**: the leader also answers `retry`
while the previous table frame is still above the commit position. That is
what makes one level of `ScheduleRecord.prev` enough — a committed frame is
never truncated, so at most one table frame is ever truncation-exposed, and a
truncation reverts the record to its single predecessor.

A refused or timed-out apply deliberately leaves the staged file alone; the
node deletes it only after a successful append. So a retry needs nothing
re-staged, and a request re-presented after a success refuses
`schedule_missing` rather than appending the same table twice. The four
refusals are `40 schedule_digest`, `41 schedule_missing`,
`42 schedule_decode` and `43 schedule_unknown_fsm`, and every outcome —
accepted, refused or retried — lands in `audit.jsonl` as a `schedule_apply`
record whose `id`/`addr` fields render the digest rather than an address.

`schedule_unknown_fsm` refuses the **whole** table, never part of it. An entry
naming an FSM this cluster does not declare is a typo or a stale name, and
adopting the rest would leave an operator believing a timer is armed that no
row will ever fire.

### Known limits of the table

These are stated because an operator will meet them, not because they are
about to be fixed.

- **The table is not in the snapshot stream.** With purge on, a node that
  joins below the floor and whose table frame was purged runs with no table
  until the next apply. Its `uc2_schedule_table_position` sits at `0` while
  everyone else's holds the adopted position — which is exactly what
  `Uc2ScheduleTableDiverged` is for. The remedy is to re-apply, and it is not
  a remedy you can leave for later: only the leader appends timer frames, so
  **if that node later wins an election, every scheduled recurrence in the
  cluster stops** — every FSM on every node, not just the new leader's — until
  an operator re-applies. That is why the alert is worth having rather than a
  gauge you glance at. The same
  absence shapes what a **wipe** does: a node truncated to 0 with no common
  prefix **keeps** its table armed and zeroes only the position, by the rule
  `ConfigRecord` already uses, because dropping it would leave that node
  ticking nothing while its peers tick on. So `uc2_schedule_entries > 0` with
  `uc2_schedule_table_position == 0` is the wipe signature, and a
  `schedule_table_reverted` record with `position=0` names it.
- **A crash in one narrow window loses one adoption.** There is no journal
  re-scan for type-6 frames on the recovery path, so a node that dies between
  the archive recording a table frame and the consensus agent persisting
  `state/schedules.state` comes back without it. The window is
  sub-millisecond, the symptom is the same divergence gauge, and the remedy is
  the same: re-apply.
- **A restart may re-append one tick per entry.** Boot arming reads the
  durable record and the log's clock; it has no delivered set until the
  service attaches and announces its `table_last`. So a restarted node may
  append the latest occurrence of every entry once — a parked `once`
  included. `Timed` drops the duplicate, exactly as it drops a re-fired
  programmatic timer. Without `Timed`, this is the at-least-once behaviour you
  already accepted.
- **No timezones, no cron.** `at` is UTC. A rule that means "02:00 local, with
  DST" is not expressible, and deliberately so: a timezone database is
  replicated state that would have to agree on every node and across every
  upgrade. Cron-style rules are a possible fourth kind byte later.

## What Aeron Cluster does

UC did not invent this shape. Aeron Cluster puts time on the log in the same
place, and the facts below were read from Aeron's own source on 2026-09-02
(tag state `1.53.0-5-gf0366beca8`), not from its documentation.

- **The stamp lives in the log record.** `SessionMessageHeader` carries a
  `timestamp` field, an `int64` epoch time. The unit is configurable
  (`ClusterTimeUnit` is `MILLIS | MICROS | NANOS`), recorded in the snapshot
  marks and adopted by the service at load. The default clock is
  milliseconds; a nanosecond clock ships and is selected by property.
- **The leader stamps at append, once per ingress message**, and the service
  never reads a clock: `Cluster.time()` returns a field that is overwritten
  from each log record's stamp as that record is delivered.
- **There is no monotone clamp.** A search for a max over time values, and
  for any last-timestamp field in the consensus module and the log publisher,
  found none. A new Aeron leader with a lagging clock writes a lower stamp
  than its predecessor did. UC's clamp is an addition, not a copy.
- **Timers are programmatic and id-only.** `Cluster.scheduleTimer(id,
  deadline)` and `cancelTimer(id)` are called from the service; the request is
  accepted by the consensus module on **every** node, not just the leader, and
  only the leader fires. Pending timers go into the consensus module's
  snapshot.
- **A fired timer is stamped with the fire time, not the deadline.** Aeron
  promises "at or after the deadline". It does not promise the ordering
  property of the previous section.
- **Replayed timer frames are deduplicated by the consensus module**, with a
  per-correlation-id counter, rather than on the service side.

So UC takes Aeron's placement (leader stamps at append, service reads the
frame, every node keeps the heap, leader fires, id-only frames) and adds
three things: the monotone clamp, the deadline-stamped in-order placement,
and exactly-once delivery decided on the service side from log content alone.

## Failure modes

Every one of these is a designed-for case, not a bug report.

| Situation | What happens |
|---|---|
| Leader dies after stamping past a deadline, before firing | The next leader fires it **late**: `time_ns` clamped to `last_stamp`, `deadline_ns` unchanged, `ev.late(ctx)` true |
| A deadline is already in the past when it is scheduled | Fires next pass, marked late the same way |
| The leader's clock steps **backward** | Stamps hold at `last_stamp`; the log's time freezes; `uc2_log_time_lag_seconds` grows and `Uc2LogTimeFrozen` fires; timers resume when wall time catches up |
| The leader's clock steps **forward** | Log time jumps; every timer between the old and new time fires over the next passes, all "on time". Clock discipline (NTP) is the operator's job, as it is in Aeron |
| Leadership is lost with timers appended but not yet confirmed | They are re-armed and fired again by the next leader; `Timed` drops the duplicate; `uc2_timers_rearmed_total` counts it |
| A cancel races a fire that is already on the log | The frame arrives, the instance is no longer pending, the frame is dropped. Cancel wins, identically on every replica |
| The service restarts between `schedule` and confirmation | Replay re-runs `apply`, the request is re-made, the set is re-announced, the node's heap converges |
| A node restarts | Its heap is empty until its service re-announces (from a snapshot or from replay); timers due in that gap fire late if that node is leader |
| The schedule ring is full | The apply loop spins, exactly as it does on the egress ring; the node drains every ring every pass |
| A state machine without `Timed` | At-least-once timers. Documented, and the same trade as running without `Sessioned` |
| A `TIMER` frame for an FSM this node does not declare | Skipped by every apply loop, and counted as a yielded frame for lag accounting |
| More than 64 timers come due in one pass | No client frames that pass, so the ordering holds; clients are backpressured for one pass |
| A table entry's tick is truncated by a leader change | Not re-fired — the next occurrence is already armed. (A *programmatic* instance IS re-armed, because nothing else would ever produce it) |
| The cluster was down past several occurrences of a table rule | **One** tick per entry on recovery, at the latest occurrence at or before the log's clock, marked late; the entry continues from that tick |
| A table file is re-applied unchanged after its `once` fired | Nothing fires. The `once` parked in the table as delivered; only changing its time or its id makes it a new entry |
| `uc2ctl schedule apply` is run against a follower | `retry`, with the leader hint. The staged file is node-local, so nothing is forwarded and nothing is half-done — re-run against the leader |
| A node joins below the purge floor and its table frame was purged | It runs with no table; `uc2_schedule_table_position` reads 0, `Uc2ScheduleTableDiverged` fires, and re-applying the table fixes it |
| A `2.10.0` node meets this wire | The header relayout is a flag day. A mixed cluster stalls rather than misreading frames. Upgrade every node together |

## What an operator sees

- `uc2_log_time_ns` on every node: the highest leader stamp the archive has
  recorded. This is the log's clock.
- `uc2_log_time_lag_seconds` on the leader only (rendered `0` elsewhere):
  wall clock minus the log's clock. `Uc2LogTimeFrozen` fires when it exceeds
  5 s for 30 s on the leader.
- `uc2_timers_pending{service,row}`, plus `uc2_timers_fired_total`,
  `uc2_timers_late_total` and `uc2_timers_rearmed_total` per row.
- `uc2ctl status` prints `log_time_ns=` on the services line and
  `timers_pending=` on each per-FSM row.
- Two structured records: `timer_late` when a fire is late (with the row, id,
  deadline, stamp and position), and `timers_rearmed` on a leadership loss.
  There is deliberately **no per-fire record**: an on-time fire is the steady
  state, and a `stderr` write per timer on the consensus agent is not a cost
  the hot path should pay. `uc2_timers_fired_total` is the signal for that.

- `uc2_schedule_table_position` on every node: the frame-end position of the
  schedule table it has adopted, `0` for none. It must be the same number
  everywhere, and `Uc2ScheduleTableDiverged` fires when it is not.
  `uc2_schedule_entries` is that table's entry count (parked `once` entries
  included) and `uc2_schedule_apply_refused_total` counts refused applies —
  retries are not refusals and are not counted.
- `uc2ctl status` prints `schedule_position=` on the `config:` line;
  `uc2ctl schedule show` prints the adopted table itself.
- Five more structured records. `schedule_table_adopted` (info) on every
  adoption, including the one a node makes at boot from its own durable
  record — the healthy signal. Four at warn:
  `schedule_apply_refused` {`node`, `reason`}, carrying the 40–43 code, so
  read the code and fix the file or re-run against the leader;
  `schedule_table_reverted` {`node`, `position`, `entries`, `to`}, when a
  truncation or the leader-open collapse cut the adopted frame and the record
  fell back — expected right after a leader change, but a `position=0` means
  this node holds no adopted position and will light
  `Uc2ScheduleTableDiverged`, so re-apply if it does not clear;
  `schedule_record_unreadable` {`node`, `err`, and `position` when there is
  one}, when the boot-time load of `state/schedules.state` failed — the node
  boots with nothing armed rather than refusing to start, and picks the table
  up from the next table frame it sees, so re-apply on a quiet cluster; and
  `schedule_staged_file_kept` {`node`, `position`, `err`}, when the append
  succeeded but `schedules.pending` could not be deleted — remove it by hand,
  or a re-presented request appends the same table twice.

See [Monitor a cluster](../how-to/monitor-a-cluster.md#the-per-fsm-families-m14)
for the queries and
[the cnc control page](../reference/cnc-page.md) for the two words these are
read from.

## Where to go next

- [The state-machine contract](../reference/state-machine-contract.md) for
  `on_timer`, `Timed<S>` and the two tiers.
- [The wire protocol](../reference/wire-protocol.md#log-frames) for the frame
  header and the `TIMER` body.
- [The FSM identity explainer](uc2-fsm-identity-and-deterministic-ids-explained.md)
  for the identity hash a `TIMER` frame names, and for `IdGen` (which can take
  `ctx.time_ns` as a prefix if you want a Snowflake-shaped id).
- [`uc2ctl` § `schedule apply`](../reference/uc2ctl.md#schedule-apply) for the
  TOML shape, the refusal reasons, and what `schedule show` prints.
- [The wire protocol § `SCHEDULE_TABLE` body](../reference/wire-protocol.md#schedule_table-body-wire-070)
  for the table's frozen byte layout.
- [The design spec](../superpowers/specs/2026-09-02-uc2-time-and-timers-design.md)
  for the decisions, the rejected alternatives, and the as-built errata on
  both plans.
