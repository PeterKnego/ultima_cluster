# UC v2 — Time and timers: leader-stamped log time and a deterministic scheduler

**Date:** 2026-09-02
**Status:** design brainstormed in chat 2026-09-02 (Sections A–C presented
and walked through in turn; the ordering argument in §4.3 was the
maintainer's own question and is answered there). Awaiting the
maintainer's review of this written spec. Next: the implementation plan.
**Baseline:** local `main` 17d5c6b — FSM identity merged (wire `0.7.0`,
cnc `3.1`, `apply(&mut self, ctx: &mut ApplyCtx, ..)`), **not yet
released**; `2.10.0` is what is shipped (wire `0.6.0`, cnc `3.0`).
**Requested by:** the maintainer, 2026-09-02 ("time should be recorded when
command is put in log, so it gets replicated and is available as synthetic
`time.now()` in FSM"; "a scheduled command should be inserted in the
command log with exact timestamp … in-order"). Not a ranked `docs/BACKLOG.md`
item; the identity spec §11 recorded it as "being designed separately".

## 1. Goal and locked decisions

A state machine may not read a clock (`uc_service::traits`, the apply
contract: "no clock, no randomness"). Today it has no substitute: the log
frame header carries no time, `ApplyCtx` carries only `position` and the
identity, and nothing in the shipped stack needs time — the session layer's
`EXPIRED` is a sequence window, not a TTL. A TTL, a rate window, an expiry,
or "do X at 14:00" is therefore impossible to express deterministically.

This spec puts time **on the log**, where every replica reads it
identically, and builds one thing on top of it: a scheduler whose fired
timers are frames on the same log, placed by the leader so that the
timestamp series stays in order across them.

| decision | choice | why (§) |
|---|---|---|
| what carries time | a `u64` nanoseconds-since-epoch stamp in **every** log frame's header, written by the leader at append | §3.1, §3.2 |
| granularity | nanoseconds; equal stamps are allowed; **position is the order**, time is never a tie-breaker | §2, §3.2 |
| monotonicity | `stamp = max(now, last_stamp)` — the log's time never goes backwards; Aeron has no such clamp (verified) | §3.2 |
| wire cost | **none**: the header's two `u64` id fields carry 32 bits each today; the relayout frees 8 bytes; header stays 32 B, payload ceiling stays 1344/1312 | §3.1 |
| FSM surface | `ApplyCtx` gains `time_ns` + `term` (fields) and `schedule`/`cancel` (methods); a **provided** `on_timer` on both tiers; no signature change | §3.3, §4.7 |
| scheduler shape | node-derived heap on every node, **leader fires**; timer frame carries `(identity hash, id, deadline)` — id-only, no payload | §4.1, §4.2 |
| ordering guarantee | a fired timer is stamped with its **deadline** and appended before any client frame of that leader pass; no earlier frame carries a stamp above the deadline unless the frame is marked late | §4.3 |
| exactly-once | node layer is **at-least-once** (re-arms on leadership loss); the service-side `Timed<S>` wrapper makes delivery **exactly-once** per scheduled instance, deterministically | §4.5, §4.6 |
| declarative surface | an **admin-applied, replicated schedule table** (a log frame adopted by every node), not per-node `node.toml` | §5 |
| release | **bundled into the pending `2.11.0` flag day** (wire `0.7.0`, cnc `3.1` — both unreleased); the two-release form is recorded as the alternative | §9 |

### What does not change

Consensus (`CommitTracker`, `ElectionSm`), elections, replication, the
commit pipeline, crypto framing, the ingress/egress rings, the client
`Engine`, the remote protocol (v1), the snapshot artifact's bytes, `IdGen`'s
inputs, the lag policy. Followers, the archive and replay copy frame headers
verbatim, so the stamp costs them nothing. The Lean model, conformance
vectors and loom models are untouched — the stamp is data carried by
consensus, not a consensus decision — and §8 re-runs them as regression
rather than arguing it.

## 2. Why this shape, and what Aeron does

**Time is data on the tape.** Every replica plays the same log; if the
leader writes its clock into each frame, "what time is it" becomes a read
of the frame, identical everywhere. That is the only construction that
makes time deterministic, and it is Aeron Cluster's.

**Equal stamps are not a defect.** At > 1 M commands/s thousands of frames
share a millisecond and many share a microsecond. Position already gives a
unique, strictly increasing order; a state machine that orders or keys by
time is wrong at *any* granularity, because two clock reads can return the
same value and a batch is stamped from one read. Time answers "when was
this accepted", which is what a TTL or a window needs. Nanoseconds are
chosen because they cost nothing (§3.1), not because they make stamps
unique — a strictly-increasing `prev + 1` rule was considered and rejected:
it turns time into a second sequence number, and position is already one.

**What Aeron Cluster does** (read 2026-09-02 from `~/ultima/aeron`, tag
state `1.53.0-5-gf0366beca8`):

- The log's `SessionMessageHeader` carries `timestamp` typed `time_t`, an
  `int64` "epoch time since 1 Jan 1970 UTC"
  (`aeron-cluster-codecs.xml:71,137`). The **unit is configurable**:
  `ClusterTimeUnit` is `MILLIS | MICROS | NANOS` (`:58-62`), recorded in
  the snapshot marks and adopted by the service at load
  (`ClusteredServiceAgent.java:985`). The default clock is
  `MillisecondClusterClock` = `System.currentTimeMillis()`; a
  `NanosecondClusterClock` ships and is selected with
  `-Daeron.cluster.clock=io.aeron.cluster.NanosecondClusterClock`
  (`ConsensusModule.java:988`, since 1.44.0) or `Context.clusterClock(..)`.
- The leader stamps **once per ingress message at append**
  (`ConsensusModuleAgent.onIngressMessage`, `:840-842`). The service never
  reads a clock: `Cluster.time()` returns a field overwritten from each log
  record's stamp as it is delivered (`ClusteredServiceAgent.java:494-602`).
- **No monotone clamp.** A grep for `Math.max` over time values and for any
  last-timestamp field in the consensus module and log publisher found
  none; a new leader with a lagging clock writes a lower stamp. §3.2's
  clamp is an addition over Aeron, not a copy.
- **Timers** are programmatic only (`Cluster.scheduleTimer(id, deadline)`
  / `cancelTimer(id)` from the service), id-only on the log, accepted by
  the consensus module on **every** node (`onScheduleTimer`, `:1449`, not
  role-gated) and fired by the leader only (`:2485-2489`). A fired timer is
  stamped with the **fire time**, not the deadline (`appendTimer(..,
  clusterClock.time())`, `:891`) — Aeron promises "at or after the
  deadline", not the ordering §4.3 gives. Replayed timer frames are
  deduplicated against re-scheduling by a per-id counter
  (`expiredTimerCountByCorrelationIdMap`, `:1539-1545`); pending timers are
  written into the consensus module's snapshot.

UC takes Aeron's placement (leader stamps at append; service reads the
frame; every node keeps the heap; leader fires; id-only frames) and adds
three things Aeron does not have: the clamp, the deadline-stamped in-order
placement, and exactly-once delivery decided on the service side from log
content alone.

## 3. The timestamp

### 3.1 Frame header relayout (`uc_protocol::v2::frame`)

The header is 32 B and stays 32 B. Today `session_id` and `correlation_id`
are `u64` on the frame but the client fills 32 bits of each: the shmem
ingress record packs `client_id: u32 ‖ local_seq: u32` into one 8-byte word
(`ipc.rs` `extra_client`/`client_from_extra`) and the leader widens them
(`node.rs`, `app.append(client_id as u64, local_seq as u64, ..)`). The
client's slot table is 32-bit on the wire by design (`uc_client/src/slots.rs`
invariant 4). The upper halves are always zero. Reclaiming them:

| offset | field | width | today |
|---|---|---|---|
| 0 | `length` (commit word) | u32 | same |
| 4 | `type` | u8 | same |
| 5 | `flags` | u8 | same |
| 6 | reserved (zero) | u16 | same |
| 8 | `leadership_term_id` | u32 | same |
| 12 | **`client_id`** | u32 | was reserved |
| 16 | **`seq`** | u32 | was `session_id` lo |
| 20 | reserved (zero) | u32 | was `session_id` hi |
| 24 | **`time_ns`** | u64 | was `correlation_id` |

`FrameHeader { length, frame_type, flags, leadership_term_id, client_id:
u32, seq: u32, time_ns: u64 }`. `Appender::append(client_id: u32, seq:
u32, payload)`; the service's egress publish narrows nothing because it
already re-packs the two into the 8-byte `header_extra`. The one caller
that passes a `u64` counter as the correlation id is the in-process ingress
path (`node.rs` `try_append(0, self.next_corr, ..)`); it becomes a `u32`
that wraps, which is harmless there for the same reason the client's
sequence may wrap: correlation is by value within a bounded window.
`FRAME_ALIGNMENT`, `HEADER_LEN`, `MTU_DEFAULT` and the payload ceiling
(1344 crypto-off / 1312 crypto-on, `docs/security/attack-surface.md` §3)
are unchanged. `docs/reference/wire-protocol.md` "Log frames" is
rewritten; the golden layout test in `frame.rs` is replaced.

**Every frame type is stamped** — MESSAGE, NEW_TERM, CONFIG, PADDING and
the two types §4/§5 add — so the log's time is defined at every position
and the seed in §3.2 is always the last frame, whatever its type.

### 3.2 Stamping (leader, `uc_log::Appender` + `uc_node`)

- **One clock read per leader pass**, not per frame: at the top of the
  consensus agent's ingress drain (`node.rs` `drain_ingress`, the step §4.3
  builds on). The source is the wall clock (`SystemTime`, epoch ns) — the
  node already reads it for `node_heartbeat_ns`; its monotonic `base:
  Instant` is for intervals and is the wrong source here.
- **Clamp:** `stamp = max(now, last_stamp)`. `last_stamp` lives in the
  `Appender`, which stamps every frame type in one place; the clamp is
  therefore a property of the log, not of any caller. It is *non-strict*:
  equal stamps are allowed (§2).
- **Seed at leader open.** The appender is created after the leader-open
  collapse (the Issue #6 path: fresh appender at the archived frontier).
  The archive agent already walks every frame header it records (it reads
  the term and detects CONFIG frames, `uc_log/src/archive.rs:372`); it now
  also carries the last recorded `time_ns` forward into one cnc word,
  **`log_time_ns`** (§6). A new leader seeds `last_stamp` from that word
  after the collapse ack, when the frontier *is* the archived frontier. On
  a fresh cluster the word is 0 and the first stamp is `now`.
- **Followers do nothing.** The receiver writes frames verbatim at their
  position; the archive records them verbatim; replay reads them verbatim.
- **Cost claim, to be measured, not asserted:** one vDSO clock read and one
  heap peek per leader pass. The fleet gate (§8) carries a null bar against
  the harness's measured build-to-build resolution.

### 3.3 The FSM surface (`uc_service::ApplyCtx`)

`ApplyCtx` is `#[non_exhaustive]` for exactly this (identity spec §3.3).
Additions:

```rust
pub struct ApplyCtx {
    pub position: u64,          // as today
    pub time_ns: u64,           // NEW: the frame's stamp (§3.2) — "now", deterministically
    pub term: u32,              // NEW: the frame's leadership_term_id
    identity: FsmIdentity,      // as today
    timers: Vec<TimerReq>,      // NEW, private: schedule/cancel requests made during this apply
}
impl ApplyCtx {
    pub fn new(position: u64, identity: FsmIdentity) -> Self;   // unchanged; time_ns = 0, term = 0
    pub fn for_sm<S: RawStateMachine>(position: u64) -> Self;   // unchanged
    pub fn with_time(self, time_ns: u64) -> Self;               // NEW, for tests
    pub fn with_term(self, term: u32) -> Self;                  // NEW, for tests
    pub fn ids(&self) -> IdGen;                                 // unchanged
    pub fn schedule(&mut self, id: u64, at_ns: u64);            // NEW (§4.4)
    pub fn cancel(&mut self, id: u64);                          // NEW (§4.4)
    pub fn timers(&self) -> &[TimerReq];                        // NEW: what this apply has asked so far (read by `Timed`, §4.6)
    pub(crate) fn consumed(&mut self, id: u64, deadline_ns: u64); // NEW: `Timed` only (§4.6)
    pub(crate) fn take_timers(&mut self) -> Vec<TimerReq>;      // NEW, apply loop only
}

`TimerReq` is `Schedule { id, at_ns } | Cancel { id } | Consumed { id, deadline_ns }`;
only the first two are constructible from outside the crate.
```

The three build sites (`apply.rs:390`, `replay.rs:169`, and the snapshot
tail-replay through the same loop) fill `time_ns` and `term` from the
frame header they already hold. Existing state machines compile unchanged.
**`query` receives no time.** A read has no position that means the same
thing on every replica, and the same argument applies to time; a
"time of the read barrier position" is a door (§10), not a decision.

`IdGen` does not take the stamp as an input (its input is
`position ‖ ordinal ‖ fold32(hash)`, identity spec §3.4). Position is
already time-correlated and strictly ordered; a state machine that wants
a Snowflake-style ID now has `ctx.time_ns` to prefix with. The identity
spec's §3.4 sentence "the FSM has no clock" and its §11 "no requester"
line become errata pointing here (§9).

## 4. The scheduler primitive

### 4.1 Three places the pending set could live

1. **Node-derived heap on every node; the leader fires. Chosen.** A
   schedule or cancel is an *output of apply*, so every replica's node
   derives the same heap from the same log, and a new leader already holds
   it. Aeron's shape (§2).
2. *The leader's service submits the timer through the ingress ring like a
   client.* No new ring, no node change — and no ordering: the timer lands
   as an ordinary command stamped with arrival time, behind whatever client
   frames were already queued. Fails the requirement. Rejected.
3. *Pending set only in the log; the node reconstructs it by reading frame
   payloads.* Binds the consensus agent to service semantics and needs a
   log reader it does not have. Rejected.

### 4.2 The timer frame

`FRAME_TYPE_TIMER = 5`. Header: `client_id = 0`, `seq = 0`, `flags` bit 0
= **table timer** (§5), `time_ns` per §4.3. Body, fixed 24 B (frame total
64 B after alignment):

| field | width |
|---|---|
| `identity_hash` | u64 — the FSM this timer belongs to (identity spec §3.2) |
| `timer_id` | u64 — the FSM's own id for it |
| `deadline_ns` | u64 — what was asked for |

**Hash, not row.** Under named rows the row is a cluster-wide index and a
row would work today; but a log frame outlives a row reorder (a per-FSM
flag day, identity spec §8) and the hash does not. Seven extra bytes in a
fixed body cost nothing. **Id-only, no payload:** the FSM keeps whatever it
needs keyed by `timer_id` in its own state. A payload-carrying variant was
considered and cut: it would put service bytes in the node's memory and
under the payload ceiling for nothing the FSM cannot get from its own map.

**One pending instance per `(fsm, id)`.** Scheduling an id that is already
pending replaces its deadline (Aeron's per-correlation-id semantics). A
*scheduled instance* is therefore `(hash, id, deadline)`, and that triple is
what §4.6 makes exactly-once.

### 4.3 The leader pass — where the ordering comes from

The leader's clock is read **once per pass** (§3.2), so the log's time
advances in steps, one per pass, and between two readings no frame can be
stamped with any other value. Each pass, in this order:

1. read the clock → `now`;
2. **while** the earliest pending timer's `deadline ≤ now`: append a TIMER
   frame stamped `max(deadline, last_stamp)` and pop it — bounded by
   `TIMERS_PER_PASS` (plan constant, order 64);
3. **only then** append this pass's client frames, stamped `max(now,
   last_stamp)`.

If step 2 hits its bound, **step 3 is skipped for this pass**: interleaving
one client frame between two due timers would stamp it above a later
timer's deadline. Clients see one pass of backpressure (the ingress ring
holds them, as it does on `WouldOverrun` today).

**Why no earlier frame carries a stamp past the deadline.** A timer with
deadline `D` fires in pass `k`. In pass `k−1`, step 2 ran and did not fire
it, so that pass's `now` was `< D`, and every client frame from pass `k−1`
and earlier is stamped `< D`. In pass `k` the timer is appended in step 2,
before any client frame receives `now ≥ D`:

```
pass k-1:  client frames stamped t1        t1 <  D
pass k:    TIMER        stamped D
           client frames stamped t2        t2 >= D
```

Neither "move the deadline to after the last stamp" nor "fire early and
watch every command" is needed: the check is one comparison between two
steps that already exist, and it is exact because time only moves in
step 1.

**The one case where the invariant cannot hold.** If the previous leader
stamped frames past `D` and died before firing, the new leader's
`last_stamp > D`. It cannot write `D` without making the log's time go
backwards, so the clamp writes `last_stamp`, and the frame carries both:
`deadline_ns = D` in the body, `time_ns = last_stamp` in the header. The
FSM sees exactly how late it is (`TimerEvent::late()`, §4.7). This is an
operating-system timer firing late under load, not a correctness loss: the
log's time stays monotone and no frame before the timer claims a time after
the timer's stamp. A deadline already in the past when scheduled is handled
the same way — fires next pass, marked late.

The rule the FSM can rely on: **a timer is never delivered early; when it
is delivered on time, no earlier frame is stamped past its deadline; when
it is late, it says so.**

### 4.4 Requests, service → node: the schedule ring

There is no service → node ring today (`svc_query` is node → service;
`egress_service` is service → clients; `ipc.rs` module doc). One new SPSC
ring per row, **`svc_sched.<row>.ring`**, created by the node in
`create_rings` beside `svc_query_ring_for` and opened as producer by the
service at attach. `MSG_V2_SCHED = 8`; record payload:

| field | width |
|---|---|
| `op` | u8 — `1` schedule, `2` cancel, `3` consumed (§4.6) |
| `timer_id` | u64 |
| `deadline_ns` | u64 (`0` for cancel) |

The apply loop drains `ctx.take_timers()` **after** each `apply`/`on_timer`
returns and writes the records — which is what the `&mut ApplyCtx` seam
was carried into the identity spec for. A full ring is handled as the
egress ring is (spin; the node drains every pass, so it is transient) and
counted (§6). The consensus agent polls the eight rings once per pass; an
empty SPSC poll is one load.

### 4.5 The node heap — at-least-once

Per row, on **every** node regardless of role: `pending: HashMap<id,
deadline>` + a min-heap on `(deadline, id)` with lazy deletion (an entry
whose `pending[id]` no longer matches is skipped). Schedule inserts or
replaces; cancel removes; **consumed** removes.

- **Leader, on append (step 2):** the entry moves to `in_flight: HashMap<id,
  (deadline, position)>`; it leaves `in_flight` on the service's `consumed`
  record.
- **Leadership lost** (any exit from the leader role, including a
  truncation that cuts an in-flight frame): every `in_flight` entry is
  **re-armed** into `pending`. The next leader may therefore fire it
  again. That is allowed — §4.6 drops the duplicate — and it is what makes
  a *missed* fire impossible: the only way an instance leaves every node's
  heap is a `consumed` from a service that saw it on the log.
- **Follower:** never pops by time; entries leave only by cancel/consumed.
  A follower that becomes leader fires whatever is due — including instances
  the old leader already appended but whose frame this node's service has
  not applied yet; duplicates, dropped in §4.6.
- **Restart:** the heap is empty until the row's service re-announces
  (§4.8). A leader whose service is still catching up fires late, never
  not at all.

No node-side persistence: the heap is a cache of what the services know.

### 4.6 The service side — `Timed<S>`, exactly-once

`uc_service::timed::Timed<S>`, a wrapper shaped like `Sessioned<S>`
(`session.rs`): forwards `NAME`/`VERSION`, composes with `Sessioned`
(`Timed<Sessioned<S>>`), and holds two small deterministic maps plus
`max_pos_seen`:

- `pending: BTreeMap<id, deadline>` — programmatic instances. `apply`
  forwards to the inner SM, then reads the requests the inner apply left in
  `ctx` (`ctx.timers()`, a read accessor) and updates the map: schedule
  inserts/replaces, cancel removes.
- `table_last: BTreeMap<id, deadline>` — for table timers (§5), the last
  delivered deadline per id.
- `on_timer(ctx, ev)`: a **programmatic** frame is delivered to the inner
  `on_timer` iff `pending[ev.id] == ev.deadline`, then removed; a **table**
  frame (flags bit 0) iff `ev.deadline > table_last[ev.id]`, then recorded.
  Either way the wrapper pushes `consumed(id, deadline)` into `ctx` so the
  node clears `in_flight`. A frame that fails the test is dropped —
  identically on every replica, because the decision is a function of the
  log alone.

**Why the split.** Making the *node* exactly-once would need it to learn
commit and apply state of frames it never reads; making the *service*
exactly-once needs only its own log-derived map. So the node promises "at
least once" and the wrapper turns it into "exactly once".

**Without the wrapper** a raw state machine receives at-least-once timers
(duplicates after a leadership loss). Documented as the same trade as
running without `Sessioned`: correct under the stated contract, weaker.

### 4.7 Delivery — `on_timer`

The apply loop's one type check (`apply.rs:381`, "NEW_TERM and any future
non-MESSAGE type … not applied") gains a branch: a TIMER frame whose
`identity_hash == S::IDENTITY.hash` and whose position is above
`last_applied()` calls a **provided** trait method, default no-op, on both
tiers:

```rust
pub struct TimerEvent { pub id: u64, pub deadline_ns: u64, pub table: bool }
impl TimerEvent { pub fn late(&self, ctx: &ApplyCtx) -> bool { ctx.time_ns > self.deadline_ns } }

// RawStateMachine and StateMachine alike:
fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {}
```

`ctx.time_ns` is the frame's stamp (= the deadline unless late, §4.3);
`ctx.position` is the frame's position and the state machine **advances
`last_applied` from it exactly as from `apply`** — the same idempotent
re-entry rule. Nothing is published to egress: no client is waiting. A
TIMER frame for a hash no local row declares is skipped by every apply
loop, like any foreign frame, and — because every FSM's `last_applied`
must pass it — lockstep/bounded lag accounting counts it as a yielded frame
exactly as NEW_TERM is counted today (the comment at `apply.rs:374-380`).

This is the **first per-FSM frame in a broadcast log** (M14 spec:
"commands are broadcast; every FSM applies every frame"). The M14 spec
gets an as-built erratum saying so (§9).

### 4.8 Snapshot and replay

- `Timed<S>` implements `SnapshotStateMachine` the way `Sessioned<S>` does
  (`session.rs:289-345`): a length-prefixed blob (both maps, bounded by
  the same sanity ceiling) ahead of the inner artifact; `freeze` returns
  the inner position; `install_snapshot` reads the blob, then the inner.
- **Re-announce.** After `install_snapshot` and after journal replay
  completes (the apply loop knows both boundaries — they are where it
  transitions to live polling), the loop asks the wrapper for its pending
  set — `trait TimerSource { fn pending_timers(&self) -> Vec<(u64, u64)> }`,
  implemented by `Timed`, a no-op default for everything else — and writes
  one `schedule` record per entry to the ring. That is how a restarted or
  freshly joined node's heap converges, and why a new leader whose service
  is behind fires late rather than never.
- Journal replay re-runs `apply`, so the inner SM's schedule calls are
  re-made and the wrapper's maps are rebuilt from the log; TIMER frames in
  the replayed range re-run the §4.6 test. Both paths end at the same maps
  — a snapshot-installed replica and a journal-replayed one hold identical
  state, the identity spec's per-apply argument applied to timers.

## 5. The declarative schedule table

**Chosen form: an admin-applied, replicated table.** An operator writes a
TOML file and applies it with a signed `uc2ctl schedule apply
<file.toml>` (HMAC admin auth, audit-logged, forwarded to the leader like
every M7/M12 admin op). The leader appends it as **`FRAME_TYPE_SCHEDULE_TABLE
= 6`**; every node adopts it through the archive's existing header walk,
the same path that adopts CONFIG frames today (`archive.rs:372`, effective
at the frame's end position), and persists it as a `StableValue` in
`state/schedules.state` beside the cluster-config record. Applying replaces
the whole table. No per-host file, hence no per-host mismatch to detect —
the reason the alternative, a `[schedules]` section in `node.toml` checked
cluster-wide like the lag policy, was rejected as the primary form (it
turns a schedule edit into a rolling edit plus a leader change; it can be
added later as a boot-time convenience that calls the admin op).

```toml
# schedules.toml — applied with `uc2ctl schedule apply`
[[schedule]]
fsm   = "orders"        # an FsmName; refused if not in [services] names
id    = 1               # the timer id the FSM sees; unique per fsm in the table
every = "1h"            # from `anchor` (RFC 3339, UTC); OR
# at  = "14:00"         # daily at this UTC time
anchor = "2026-01-01T00:00:00Z"
```

- Payload: bincode of `ScheduleTable { entries: Vec<Entry> }`, **≤ 64
  entries** so it always fits the 1344 B payload ceiling (a config frame
  has the same bound). Two rules only: `every` (period + anchor) and `at`
  (seconds-of-day, UTC). No cron expressions, no time zones (§10).
- **Next deadline is computed from the deadline just fired**, never from
  the clock: `next = fired + period`, or the next day's `at`. So recurrence
  never drifts and every node computes the same value. On adoption the
  first deadline is the first occurrence `≥` the table frame's own
  `time_ns` — again from the tape, not from a local clock.
- Table timers go through the same heap (§4.5) and the same TIMER frame
  with `flags` bit 0 set; the FSM sees them in the same `on_timer` with
  `ev.table = true`. Exactly-once is the `table_last` rule (§4.6): the FSM
  never asked for these, so they cannot be in `pending`.
- Ids share the FSM's id space with programmatic timers; the docs say to
  reserve a range. The wrapper's two maps never confuse them because the
  frame flag routes them.

**Sequencing:** the primitive and the programmatic surface are plan 1; the
table is plan 2 of this spec. Plan 1 already gives "declarative" in the
FSM-level sense — an FSM can schedule its own recurrence from `on_timer`,
bootstrapped by one command — so plan 2 is the operator-facing convenience
on top, and the part most likely to change shape once plan 1 has been used.

## 6. Observability

Node-side, per row (labels `service="<name>",row="<r>"` as the identity
spec set): `uc2_timers_pending` (gauge), `uc2_timers_fired_total`,
`uc2_timers_late_total` (stamp > deadline at append),
`uc2_timers_rearmed_total` (leadership loss), `uc2_sched_ring_full_total`.
Cluster-side: **`uc2_log_time_ns`** (the `log_time_ns` cnc word, §3.2, on
every node) and, on the leader only, **`uc2_log_time_lag_seconds`** = wall
clock − last stamp. The lag is the alert for a leader whose clock stepped
backwards: the log's time then freezes until the wall clock catches up
(§7). Alert rule: lag > 5 s on the leader.

`uc2ctl status` prints `log_time=<rfc3339>` and `timers_pending=<n>` per
row. `obs_event!` records: `timer_fired {name, id, deadline_ns, time_ns,
late}`, `schedule_table_adopted {position, entries}`.

**cnc:** `log_time_ns` is one u64 word on page 1, written by the archive
agent (the frame-walk owner). cnc `3.1` is unreleased, so the word joins it
with no further version bump; the plan places it on a line whose existing
writer is the archive agent or on the boot-once `4032` line (a boot-once
word and a later single writer never race), pinned in both `uc_protocol`
and `uc_log` with the offset-assertion tests. Service-side duplicate-drop
counts would need a service-written word; the identity spec reserves 24 B
in slot line 7, and whether to spend 8 of them is left to the plan.

## 7. Failure modes

| situation | outcome |
|---|---|
| leader dies after stamping past a deadline, before firing | next leader fires it **late**: `time_ns` clamped to `last_stamp`, `deadline_ns` unchanged, `late()` true |
| deadline already past when scheduled | fires next pass, marked late the same way |
| leader clock steps **backward** | stamps hold at `last_stamp`; log time freezes; `uc2_log_time_lag_seconds` alert; timers fire when wall time catches up |
| leader clock steps **forward** | log time jumps; every timer between old and new time fires in the next passes, all "on time"; clock discipline (NTP) is the operator's, as in Aeron |
| leadership lost with timers appended, not yet `consumed` | re-armed, fired again by the next leader; `Timed` drops the duplicate |
| cancel races a fire already on the log | the frame arrives, the instance is no longer pending → dropped; cancel wins on every replica |
| service restarts between `schedule` and `consumed` | replay re-runs apply → request re-made → re-announced; heap converges |
| node restarts | heap empty until its service re-announces (snapshot or replay); timers due in the gap fire late if it is leader |
| `svc_sched` ring full | apply loop spins as on egress; node drains every pass; counted |
| state machine without `Timed` | at-least-once timers; documented, same trade as no `Sessioned` |
| TIMER frame for a hash no local row declares | skipped by every apply loop; counted as a yielded frame for lag accounting |
| step 2 hits `TIMERS_PER_PASS` | no client frames this pass (ordering preserved); clients backpressured one pass |
| schedule table names an FSM not in `[services]` | `uc2ctl schedule apply` refused by name before anything is appended |
| a shipped-`2.10.0` node against this wire | header relayout → flag day; mixed cluster stalls (standing rule) |

## 8. Test plan and acceptance

**Unit tier** (each test written first and watched red; the plan records
which fix each is reverted against):

- `uc_protocol`: header relayout golden vectors + offset pins (`frame.rs`,
  and the `uc_log` mirror); TIMER body and SCHEDULE_TABLE round trips and a
  short-body reject; `MSG_V2_SCHED` record round trip; `log_time_ns` cnc
  offset pinned in both crates; three fuzz targets
  (`uc_protocol_timer_frame`, `uc_protocol_sched_record`,
  `uc_protocol_schedule_table`) added to `scripts/fuzz_smoke.sh`.
- `uc_log`: the clamp (`max(now, last)`), the seed from `log_time_ns`, the
  archive walk carrying `time_ns` forward; **the pass-order property**
  against the appender directly — after any interleaving of due timers and
  client frames, no frame before a TIMER carries a stamp above that
  timer's deadline unless the TIMER is late (`time_ns > deadline_ns`);
  `TIMERS_PER_PASS` skipping step 3.
- `uc_service`: `ApplyCtx` fields filled at all three build sites (one
  assertion per path); `take_timers` drained after apply and after
  `on_timer`; `Timed`: schedule/replace/cancel, duplicate drop, table
  `last` rule, `consumed` emitted on both deliver and drop, snapshot round
  trip incl. `Timed<Sessioned<S>>`, `pending_timers` after install and
  after replay; `on_timer` default no-op leaves every existing SM compiling
  and every existing suite green.
- `uc_node`: heap semantics (replace, lazy delete, `in_flight`, re-arm on
  every leader-exit path incl. truncation); firing order and stamps in a
  single-node harness; ring creation per row; table adoption + `StableValue`
  + first-deadline-from-frame-time; `uc2ctl schedule apply` refusals;
  metrics and the alert rule; `uc2ctl status` output.

**Sim tier.** Stamping and firing touch neither `CommitTracker` nor
`ElectionSm`; the Lean model, conformance vectors and loom models are
re-run as regression only. `uc_sim` gains the §4.3 invariant as a
world-level check and a seeded fault scenario in which leaders die
mid-fire, asserting every scheduled instance appears on the committed log
at least once and is delivered (through `Timed`) exactly once.

**Capstones.** A `uc_lincheck::TimerSm` whose commands schedule and
cancel, and whose `on_timer` appends to its history, run through `lin_v2`
(failover and purge/snapshot churn) and the hard-crash harness, asserting:
exactly-once delivery per instance, at the same position on every replica;
the §4.3 ordering over the whole history; `late()` only after a leader
change. Elle: unchanged.

**Fleet gate** (`docs/benchmarks/uc2-time-and-timers-gate-<date>.md`,
bars pre-committed, honest-failure protocol): a null throughput bar
(steady-window rows from `m14_fleet_gate.py`) bounded by the harness's
measured build-to-build resolution (`scripts/hop1_ab.sh` same-source
rebuild control); and a **timer-precision row**: the distribution of
`time_ns − deadline_ns` under load for on-time timers, its bar set from a
*measured* drain-pass length on the rig, not a hoped-for number.

## 9. Release and docs

**Recommendation: bundle into the pending `2.11.0`.** The identity work
merged to `main` with wire `0.7.0` and cnc `3.1` **unreleased** and the
release on hold "because more changes are planned on the branch first"
(`CLAUDE.md`, 2026-09-02). Nothing shipped speaks `0.7.0`, so the header
relayout, the two frame types and the `log_time_ns` word join that flag day
at no extra cost, and `docs/reference/wire-protocol.md`/`upgrade-a-cluster.md`
describe one `0.6.0 → 0.7.0` step. The alternative — ship identity as
`0.7.0`, time as `0.8.0` — costs one more version number and one more
upgrade entry and is otherwise equivalent; it is the fallback if the
maintainer releases `2.11.0` before this lands.

**API:** additive on `uc_service` (`#[non_exhaustive]` fields, a provided
method, a new wrapper, a new trait with a default impl). `uc_protocol`'s
`FrameHeader` field change is breaking; it rides the identity release's
existing carve-out (`docs/reference/semver-policy.md`, "next minor, not
`3.0.0`").

**Docs (the standing rule, before tagging):** a `RELEASES.md` section
(two feature bullets — log time, timers/schedule table — each linking its
doc); the `docs/releases.md` entry; QUICKSTART / how-to / reference sweep;
`docs/reference/wire-protocol.md` (header + two frame types),
`docs/reference/cnc-page.md` (`log_time_ns`), `docs/ops/uc2-runbook.md`
(log time in cnc decode, `uc2ctl schedule`, `uc2ctl status` fields),
`docs/how-to/upgrade-a-cluster.md`, `docs/security/attack-surface.md`
(the two new frame bodies and the admin op), `docs/VERIFICATION.md` (the
new sim invariant, capstone, fuzz targets). One plain-language explainer,
`docs/notes/uc2-log-time-and-timers-explained.md` — the "time is data on
the tape" explanation, the §4.3 argument with its timeline, the late case,
the at-least-once/exactly-once split, what Aeron does. Errata: the M14
spec (first per-FSM frame in a broadcast log); the identity spec §3.4
("the FSM has no clock") and §11 ("will follow as their own wire
release").

## 10. Out of scope and doors left open

- **Time for queries.** A read could be given the stamp at its barrier
  position; not designed, not needed by any requester.
- **Timer payloads.** Cut (§4.2); the FSM keys its own state by id.
- **Cron expressions, time zones, calendars.** Two rules, UTC only (§5).
- **Per-timer precision guarantees.** The gate *measures* precision; the
  contract is "never early; on time or marked late".
- **Leader clock discipline.** Backward steps are clamped and alerted;
  forward steps are not detectable in-band. NTP is the operator's, as in
  Aeron.
- **Remote clients.** A remote client schedules nothing directly; it
  submits a command whose apply schedules. Remote protocol stays v1.
- **Versions across a fire.** A timer scheduled under one `const VERSION`
  and fired under another is backlog item 3's rolling-upgrade question.
- **`node.toml` `[schedules]`** as a boot-time convenience that calls the
  admin op (§5) — a door, no flag day.
- **A service-written duplicate-drop counter** in the reserved slot bytes
  (§6) — plan decision.

## 11. Implementation order (two plans)

**Plan 1 — log time + the scheduler primitive**

1. `uc_protocol`: header relayout + golden test; `FRAME_TYPE_TIMER` +
   body codec; `MSG_V2_SCHED` record; `log_time_ns` cnc word (+ `uc_log`
   pin); fuzz targets.
2. `uc_log`: `Appender` stamps every frame type with the clamp; seed API;
   `append_timer`; archive walk carries `time_ns` → cnc word; the pass-order
   property test.
3. `uc_service`: `ApplyCtx` fields/methods; `TimerEvent` + provided
   `on_timer` on both tiers (+ blanket, `Sessioned`, `Tagged` forwarding);
   `take_timers` at the build sites; `Timed<S>` incl. snapshot and
   `TimerSource`; the `svc_sched` producer at attach; re-announce after
   install/replay.
4. `uc_node`: `svc_sched` ring creation; per-row heap with `in_flight` and
   re-arm on every leader-exit path; the §4.3 pass; seed on leader open;
   metrics, alert rule, `uc2ctl status`.
5. Sim invariant + fault scenario; `TimerSm` capstone through `lin_v2` and
   the hard-crash harness; loom/Lean/conform regression.
6. Docs + release writeup; fleet gate rows.

**Plan 2 — the schedule table**

7. `uc_protocol`: `FRAME_TYPE_SCHEDULE_TABLE` + codec + fuzz target.
8. `uc_node`: adoption via the archive walk; `state/schedules.state`;
   next-deadline arithmetic; `uc2ctl schedule apply` (admin auth, audit).
9. `uc_service`: the `table_last` rule in `Timed`; `ev.table`.
10. Tests, runbook, attack-surface entry, release bullet.
