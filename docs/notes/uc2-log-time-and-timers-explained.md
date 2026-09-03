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
- [The design spec](../superpowers/specs/2026-09-02-uc2-time-and-timers-design.md)
  for the decisions, the rejected alternatives, and plan 2 (the replicated
  schedule table, which is not built yet).
