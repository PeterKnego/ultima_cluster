# Schedule work inside a state machine

Your state machine needs to do something later — expire a reservation, retry a
stalled order, sweep a table. Before `2.11.0` there was nowhere to put that: an
FSM had no clock and no way to ask to be woken, and reaching for
`SystemTime::now()` inside `apply` diverges the replicas.

This guide is the developer half. For running work on a wall-clock schedule an
operator controls — daily at 14:00, every hour — see
[Run work on a schedule](run-work-on-a-schedule.md); the two meet at the same
`on_timer` method.

## Name your state machine first

Timers are addressed to an FSM by identity, so the name is a prerequisite, not
a separate feature. Declare it on the trait impl:

```rust
impl StateMachine for Orders {
    const NAME: &'static str = "orders";   // required
    const VERSION: u32 = 3;                // optional, defaults to 0
    // …
}
```

`NAME` is `1..=32` bytes of lowercase ASCII letters, digits, `_` or `-`,
starting with a letter. It belongs **in code**, not in deployment config: it is
part of what your logic *is*, and a node refuses a joiner whose row names
disagree with its own. The cluster's `node.toml` lists the same names in row
order under `[services] names` — see
[Configuration § `[services]`](../reference/configuration.md). The service
finds its own row by scanning for `S::NAME`; it never states a row number.

Bump `VERSION` when the FSM's behaviour changes in a way that would make two
versions apply the same command differently. It is compared on the snapshot
session, so a joiner running the wrong build is refused by name and version
rather than silently installing a mismatched artifact.

## Ask to be woken: `ctx.schedule`

`apply` now receives an [`ApplyCtx`](../reference/state-machine-contract.md).
Scheduling is a method on it:

```rust
fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> Resp {
    match cmd {
        Cmd::Reserve { item, hold_ms } => {
            let id = self.next_slot();
            self.holds.insert(id, item);
            // ctx.time_ns is the frame's leader stamp — the same value on
            // every replica. This is the ONLY clock apply may read.
            ctx.schedule(id, ctx.time_ns + hold_ms * 1_000_000);
            Resp::Held(id)
        }
        Cmd::Confirm { id } => {
            self.holds.remove(&id);
            ctx.cancel(id);          // no longer needs waking
            Resp::Ok
        }
    }
}
```

Three properties worth internalising:

- **`ctx.time_ns` is "now", and it is replicated.** The leader reads its clock
  once per pass and stamps every frame `max(now, last)`, so it never goes
  backwards along the log and every replica applying that frame sees the same
  number. Deadlines you compute from it are therefore identical everywhere.
- **`schedule`/`cancel` are outputs of `apply`, not side effects.** They are
  replayed identically on every replica, including during journal replay after
  a restart. That is why they take no callback and carry no payload.
- **One pending instance per `(FSM, id)`.** Scheduling an id that is already
  pending *replaces* its deadline. There is no queue per id.

A timer carries no payload — only `(identity, id, deadline)` fits the wire
body. Keep the timer's context in your own state, keyed by the id, exactly as
`self.holds` does above.

## Handle the wake-up: `on_timer`

`on_timer` is a **provided** method on both tiers, so adding timers to an
existing state machine does not break it — the default does nothing.

```rust
fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
    if let Some(item) = self.holds.remove(&ev.id) {
        self.release(item);
    }
    if ev.late(ctx) {
        // ctx.time_ns > ev.deadline_ns: the leader could not place this
        // timer at its deadline. Normal after a failover — not a defect.
        self.late_releases += 1;
    }
}
```

`on_timer` runs under the same rules as `apply`: sync, deterministic, no I/O.
It may schedule and cancel through the same `ctx`, so a periodic sweep is a
timer that re-arms itself.

`ev.table` distinguishes a tick from the operator's
[replicated schedule table](run-work-on-a-schedule.md) from one your own
`apply` asked for. Keep the two id ranges apart.

## Decide whether you need `Timed<S>`

**The node layer delivers at-least-once.** The node's timer heap is
**leader-only** (the cluster FSM, 2.11.0): a demoted leader discards it,
and a newly promoted one rebuilds it from your service's re-announce of its own
pending set plus the cluster's schedule table. An instance that was in flight
when the old leader lost leadership is still in that pending set, so the new
leader fires it again and it can fire twice. That is deliberate: the
alternative loses timers.

The cost of the leader-only heap is one extra round trip inside the promotion
window — a timer due there fires one service cycle plus one ring hop later than
it would have. A failover already made timers late (`ev.late(ctx)` says so), so
the semantics are unchanged; only the width of an existing window moved.

`uc_service::Timed<S>` wraps either tier and makes delivery **exactly-once**.
It keeps the pending set your FSM asked for, rebuilt from the log on replay and
carried in the snapshot, and calls the inner `on_timer` only if the fired
`(id, deadline)` is still pending. Every replica decides identically, because
the decision reads nothing but committed frames.

```rust
let service = ServiceBuilder::new(cfg, Timed::new(Orders::default())).start()?;
```

Take the wrapper unless your `on_timer` is genuinely idempotent. It is the same
trade as [`Sessioned<S>`](../reference/state-machine-contract.md) for client
retries: the wrapper costs a `BTreeMap` and a snapshot field, and skipping it
means accepting duplicates.

## Generating IDs without breaking determinism

A UUID from an RNG diverges replicas. `ctx.ids()` hands you an
[`IdGen`](../notes/uc2-fsm-identity-and-deterministic-ids-explained.md) scoped
to **this apply call**, deriving each id from `position ‖ ordinal ‖
fold32(identity)`:

```rust
let mut ids = ctx.ids();
let order_id = ids.next();     // u128, same on every replica
```

Never stash an `IdGen` across calls — it is `!Send` precisely so the obvious
attempt fails to compile.

## Check it is working

`uc2_timers_pending`, `uc2_timers_fired_total` and `uc2_timers_late_total` are
exported per row; see
[Monitor a cluster](monitor-a-cluster.md#the-log-clock-and-the-timer-families-2110).
`uc2_timers_pending` is the **leader's** count and a follower exports `0`, so
do not alert on the fleet disagreeing about it — that is the healthy reading.
(`uc2_timers_rearmed_total` (retired) existed in an earlier draft of this
feature: with a leader-only heap there is nothing to re-arm on demotion.)

A rising `uc2_timers_late_total` on a cluster that is **not** changing leaders
is worth investigating; after a failover it is expected.

## What this does not give you

- **No precision guarantee.** The contract is "never early; on time or marked
  late". Up to `TIMERS_PER_PASS` = 64 timers fire per leader pass, and at that
  bound the pass appends no client frames at all.
- **No per-timer payload**, and no timer that outlives the FSM's own state.
- **Leader clock discipline is the operator's.** A backward step freezes the
  log's clock (and alerts `Uc2LogTimeFrozen`); a forward step is not detectable
  in-band and fires every timer in between. Run NTP.

The full list is [Limits](../reference/limits.md); the reasoning behind the
design is [Log time and timers,
explained](../notes/uc2-log-time-and-timers-explained.md).

## A worked implementation

There is no timer example under `examples/` yet. The reference implementation
that the correctness suites drive is
[`uc_lincheck/src/timer.rs`](../../uc_lincheck/src/timer.rs) — a `TimerSm` that
records every fire, plus its `Timed<TimerSm>` wrapping in
[`uc_node/tests/timers.rs`](../../uc_node/tests/timers.rs), which is also the
clearest place to see the exactly-once behaviour asserted.

## Related

- [Write a service binary](write-a-service-binary.md) — the process lifecycle
  around the state machine you just gave a clock to.
- [The state-machine contract](../reference/state-machine-contract.md) — the
  trait signatures, `ApplyCtx`, and the `on_timer`/`Timed<S>` reference.
- [Run work on a schedule](run-work-on-a-schedule.md) — the operator-driven
  half, arriving through the same `on_timer`.
- [Log time and timers, explained](../notes/uc2-log-time-and-timers-explained.md)
  — why the leader stamps time and what breaks if it does not.
