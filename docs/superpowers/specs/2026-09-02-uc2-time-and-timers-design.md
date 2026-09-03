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

  After the leader-open collapse the archive has cut its journal to `base`, but
  `log_time_ns` may still hold the stamp of a frame above `base` that the cut
  discarded. Seeding from it is **monotone-safe**: the new leader's first stamps
  are at or above what any replica could have seen, never below. A stamp that is
  slightly ahead of wall time for one pass is the same "late" case §4.3 already
  accepts; a stamp that goes backwards would not be. The archive never lowers the
  word.
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

`TimerReq` is `Schedule { id, at_ns } | Cancel { id }` — the two things a state
machine may ask. `consumed` is not a variant: `Timed` reports it through a
`pub(crate)` method that pushes into a private list, and the apply loop takes
both lists at once as wire records (`ApplyCtx::take_sched_records() ->
Vec<SchedRecord>`, `uc_protocol::v2::ipc::SchedRecord`). Nothing outside the
crate can forge a `consumed`.
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

`svc_sched` is the first per-row ring the node **consumes** — `svc_query`'s
consumer half is dropped at creation (`node.rs` `create_rings`) — so the
consensus agent's drain of it is new code beside `drain_query_ring`, not a
refactor of it. The node keeps `Vec<Option<SpscConsumer>>` by row, the shape
`svc_query` uses for its producers.

### 4.5 The node heap — at-least-once

Per row, on **every** node regardless of role: `pending: HashMap<id,
deadline>` + a min-heap on `(deadline, id)` with lazy deletion (an entry
whose `pending[id]` no longer matches is skipped). Schedule inserts or
replaces; cancel removes; **consumed** removes. Plan 2's table entries are
the exception: they never leave, they advance (§5).

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
- **Re-announce.** The apply loop carries `announce_pending: bool`, set at
  attach, and set again whenever `replay_into` returns (which is also the only
  path a snapshot install takes, so install is covered without a second hook).
  When the flag is set, the top of the next `apply_cycle` asks the wrapper for
  its pending set — as built a **provided `RawStateMachine::pending_timers(&self)
  -> Vec<(u64, u64)>`** with an empty default, overridden by `Timed`, rather
  than the separate `TimerSource` trait this line first proposed (§8 erratum)
  — and writes one `Schedule` record per entry before delivering any frame. That is
  how a restarted or freshly joined node's heap converges, and why a new leader
  whose service is behind fires late rather than never.
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
`state/schedules.state` (`ScheduleRecord { position, time_ns, table }` —
**as built it also carries `prev`; erratum 2 below**; at
boot the node re-arms every entry from the record and the recovered log
clock before its service attaches — an FSM with no attached service still
ticks on the leader; the tick is dropped on delivery only if no service
ever attaches, which is the operator's problem to see through
`uc2_timers_pending` and the attach gauges) beside the cluster-config
record. Applying replaces the whole table. No per-host file, hence no
per-host mismatch to detect — the reason the alternative, a `[schedules]`
section in `node.toml` checked cluster-wide like the lag policy, was
rejected as the primary form (it turns a schedule edit into a rolling
edit plus a leader change; it can be added later as a boot-time
convenience that calls the admin op).

**How the bytes reach the leader.** The cnc admin request line is 64
bytes of fixed fields (`seq, nonce, op, id, ip, port`), and the HMAC
covers exactly those fields — a table cannot ride it. `uc2ctl schedule
apply` therefore (1) encodes the table, (2) writes the bytes to
`<instance_dir>/schedules.pending` (0600, fsync, rename), and (3) writes
an admin request with `op = 6` whose `id ‖ ip ‖ port` fields carry the
first 80 bits of SHA-256 over the encoded bytes. The node verifies the
request as it does every admin op, reads the staged file, recomputes the
digest, and refuses by name on a mismatch (reason `schedule_digest`).
Under `[admin] auth = "hmac"` the payload is thereby authenticated
through the signed digest; under the filesystem policy it is trusted the
way every admin request already is. **Apply is leader-only**: a follower
answers `status = 2` with the leader hint, as it does when it cannot
forward — there is nothing to forward but the digest, and the leader
cannot read the follower's file.

```toml
# schedules.toml — applied with `uc2ctl schedule apply`
[[schedule]]
fsm   = "orders"        # an FsmName; refused if not in [services] names
id    = 1               # the timer id the FSM sees; unique per fsm in the table
every = "1h"            # from `anchor` (RFC 3339, UTC); OR
# at  = "14:00"         # daily at this UTC time
anchor = "2026-01-01T00:00:00Z"
```

- Payload: a hand-laid, bounded, total codec (`uc_protocol::v2::schedule`,
  the same style as `v2::config`'s), **≤ 32 entries**, each keyed by the
  FSM's **identity hash** rather than its name (`hash u64 ‖ id u64 ‖ kind u8
  ‖ a u64 ‖ b u64` = 33 B; 32 entries + an 8-byte header = 1064 B, inside
  the 1312 B crypto-on ceiling). Names appear only in the operator's TOML;
  `uc2ctl` resolves each against the node's cnc name lines and refuses an
  undeclared one before any request is written. Three rules: `every`
  (`period_ns`, `anchor_ns`), `at` (`secs_of_day`, UTC) and `once`
  (`at_ns`, one fixed deadline). A `once` entry fires one tick and then
  **parks**: it stays in the table as delivered, with no next deadline, so
  re-applying the same file does not fire it again; changing its time (or
  its id) does. After a restart the node may re-append a past `once` tick —
  boot arming has no delivered set until the service announces its
  `table_last` — and `Timed` drops the duplicate (§4.6), the same
  at-least-once/exactly-once split every table tick has.
- **Next deadline is computed from the deadline just fired**, never from
  the clock: `next = fired + period`, or the next day's `at`, or — for a
  `once` — **no next deadline at all: the entry parks in the table as
  delivered** (erratum 3 below). So recurrence never drifts and every node
  computes the same value. **As built**, on adoption the first deadline is
  the LATEST occurrence at or before the log clock the frame is adopted
  against (the one-tick catch-up), which then fires at the latest occurrence
  at or before the firing pass's clock — not the first occurrence `≥` the
  frame's own `time_ns`, as this bullet originally said. Either way it comes
  from the tape, not from a local clock; see erratum 1 below.
- Table timers go through the same heap (§4.5) and the same TIMER frame
  with `flags` bit 0 set; the FSM sees them in the same `on_timer` with
  `ev.table = true`. Exactly-once is the `table_last` rule (§4.6). Three
  differences from a programmatic instance, all deliberate:
  - **The node advances the entry at append** (`next = rule.next_after
    (fired)`; for `once` that is *no* deadline, and the entry parks), so a
    leader keeps a table on schedule without waiting for its service; a follower advances on the service's `TableConsumed
    (id, deadline)` report, so a new leader starts from what its own
    service last delivered. A leader whose service lagged may re-fire ticks
    the old leader already fired; `Timed` drops them (at-least-once, as
    §4.5).
  - **No re-arm on leadership loss.** A table tick whose frame was
    truncated is not fired again; the next tick is. (A programmatic
    instance IS re-armed, because it has no successor.)
  - **One-tick catch-up.** When an entry is armed — at adoption, at boot
    from `state/schedules.state`, or when the service announces its
    `table_last` — its next deadline is the LATEST occurrence at or below
    the log's clock if that occurrence is newer than the last delivered
    one, else the first occurrence after it. A cluster that was down for an
    hour with a one-second rule fires one tick on recovery, not 3 600.
    (**As built the catch-up is applied again at FIRE time, not only at arm
    time — erratum 1 below.** Arming alone is not enough.)
- Ids share the FSM's id space with programmatic timers; the docs say to
  reserve a range. The wrapper's two maps never confuse them because the
  frame flag routes them.

**Sequencing:** the primitive and the programmatic surface are plan 1; the
table is plan 2 of this spec. Plan 1 already gives "declarative" in the
FSM-level sense — an FSM can schedule its own recurrence from `on_timer`,
bootstrapped by one command — so plan 2 is the operator-facing convenience
on top, and the part most likely to change shape once plan 1 has been used.

**As-built errata (plan 2, 2026-09-03) — three execution rulings that amend
this section.** Implemented across `b71a1f6..a0c2cd8`; §5's text above is left
as written and annotated rather than rewritten.

1. **The one-tick catch-up is applied at FIRE time, not only at arm time.**
   This section arms an entry to "the latest occurrence at or below the log's
   clock" and then advances one period per fire. That is correct only if the
   clock the entry was armed against is close to the clock the leader is
   stamping with. It is not, on the path that matters: after a restart the
   node arms from `cnc.log_time_ns()`, the clock **as of the last recorded
   frame**, which can be hours behind the wall clock a newly elected leader
   reads. Advancing one period per fire from that armed value would replay the
   whole downtime — exactly the backlog this bullet promises never happens. As
   built, `RowTimers::table_fire_deadline(id, now_ns)` recomputes at the
   moment of firing: `next.max(rule.latest_at_or_before(now_ns))`, so the
   deadline that actually rides the frame is the latest missed occurrence, and
   `table_fired` advances from **that** value rather than from the armed one
   (hence its `<=` guard instead of an equality). Which occurrence is due is
   the one clock-driven choice the determinism rule allows, and it is made
   once, by the leader; every replica advances from the chosen deadline
   because it is on the frame. `ScheduleRule::{next_after,
   latest_at_or_before, arm}` are the pure arithmetic; all three return
   `Option<u64>` — `next_after` and `arm` because a delivered `Once` has no
   successor (erratum 3), `latest_at_or_before` because a clock before the
   first occurrence has no predecessor.

2. **`ScheduleRecord` carries one level of `prev`, applies are
   single-in-flight, and a truncation reverts.** This section's record is
   `{ position, time_ns, table }`, which cannot survive its own frame being
   cut: a truncation below the adopted position would leave a record claiming
   a position the log no longer backs. As built the record is
   `{ position, time_ns, table, prev: Option<Box<ScheduleRecord>> }` — exactly
   `uc_log::state::ConfigRecord`'s discipline, for exactly its reason — and
   both truncation paths revert to `prev`: `Truncate` and the leader-open
   `Collapse`. **One** level suffices only because a second rule was added
   with it: `Consensus::apply_schedule_table` refuses (`status 2`, retry,
   side-effect-free) while `schedule_position > commit`, so a second table can
   never be appended while the first is still truncation-exposed. Committed
   frames are never truncated, so at most one table frame is ever exposed.
   This is the config path's `ChangePending` idea, placed in the node layer
   because the schedule table is not a state-machine concern.

3. **`once` is a third rule kind, and it parks rather than leaving.** This
   section's TOML sketch shows two rules; the payload bullet already names
   three, and `once {at_ns}` (`kind = 3`) is what shipped. The ruling worth
   recording is the *park*: a fired `once` stays in the table marked
   delivered, with `next = None`. Dropping it would be wrong the second time
   an operator applies a file, because applying replaces the whole table — an
   operator adding one `every` rule re-applies a file that still contains last
   week's `once`, and a dropped entry would arm and fire again from a file
   they did not think they were changing. Parked entries are excluded from
   `pending_len()` (so from the cnc `timers_pending` word and
   `uc2_timers_pending`) and included in `table_len()` (so in
   `uc2_schedule_entries`). Changing a `once`'s time or its id makes it a
   different entry, which arms normally.

- **Errata (plan 3, snapshot carry).** The table IS carried by the snapshot
  session: the leader sends a `SNAP_TABLE` datagram (kind 18, body `session ‖
  position ‖ time_ns ‖ table_len ‖ table`, ≤ 1086 B) after every `SNAP_BEGIN`
  of a session; the receiver withholds `SNAP_DONE` until it has one and
  publishes it to the consensus agent before the floor signal, which installs
  it by fiat (a wholesale replace, `prev = None`, like the carried config). A
  below-floor joiner therefore holds the cluster's table before it can serve
  or lead. Position `0` with an empty table means "the leader has none" and
  is installed as such. This supersedes the "not carried in the snapshot
  stream" limitation recorded above.

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

**As-built erratum (plan 1, 2026-09-03) — two corrections to this paragraph.**

1. There is **no `timer_fired` record**. The per-fire record as built is
   `timer_late {node, row, timer_id, deadline_ns, time_ns, position}`, emitted
   **only when the fire is late**. The spec's version would have put a
   `stderr` write per fired timer on the consensus agent, the single writer
   that also drives commit and elections, at up to `TIMERS_PER_PASS` per pass.
   Demoting it to a Debug level was considered and is not available: `uc_obs`
   has no Debug level. An on-time fire is the steady state and stays visible
   through `uc2_timers_fired_total`; a late fire is the operational signal and
   is rare by construction, so it earns the write. A second record,
   `timers_rearmed {node, row, count}`, is emitted on leadership loss.
2. `uc2ctl status` prints **`log_time_ns=<n>`, raw nanoseconds**, not RFC 3339.
   `uc_ctl` carries no date formatter and adding a dependency for one status
   line was not worth it; the raw value is also exactly what `uc2_log_time_ns`
   and the cnc word hold, so the three agree literally.

**cnc:** two words, both inside the unreleased cnc `3.1`. `log_time_ns` is page
1 offset `4048` (the third word of the boot-once `4032` line; `4032`/`4040` are
written once before publish and never again, so the archive agent is the line's
only live writer). `timers_pending` is slot line 7 offset `+488` (the word after
`identity_hash`; line 7's writer is the node, and the consensus agent is the
node). Offsets are pinned in both `uc_protocol` and `uc_log`. `uc2_timers_pending`
and `uc2ctl status` read the slot word; the fired/late/re-armed counters are
process-local atomics in `ObsSources`. `uc2_sched_ring_full_total` is **not**
exported in plan 1: it would need a service-written word and the reserved slot
bytes are not spent here; the service counts it in a log record instead.

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
| node restarts and wins the next election before recording a block | **as-built (final-review C1):** the cnc page is recreated zeroed every boot, so the seed cannot come from the page. `Archive::open` walks the last recorded block's frame headers (back up to 8 blocks past padding-only ones) and the node writes `recovered_log_time_ns()` into `log_time_ns` before any agent runs. The clamp therefore survives a restart; only a fresh instance dir (empty journal) starts from wall time. |
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

**Sim tier.** `uc_sim`'s world has no frames — a command is a 96-byte append
counter and the only per-position fact is the term map — so the §4.3 invariant
cannot be a world-level check without inventing a frame model the world does not
need. It is instead a **pure model of the leader pass** in `uc_sim::timers`:
a virtual clock, random client appends and timer deadlines, leader changes with
lagging and leading clocks, driven by the same seeded RNG, asserting the §4.3
property and the clamp on every step. Stamping and firing touch neither
`CommitTracker` nor `ElectionSm`, so the Lean model, conformance vectors and
loom models are re-run as regression only. The "leaders die mid-fire" scenario
runs against real code in the `lin_v2` capstone (§8, Capstones), where it
belongs.

**As-built erratum (plan 1, 2026-09-03).** `uc_sim::timers::PassModel::check`
has **five** rules, not the four this section implies. Rules 1-4 are the
spec's (stamps non-decreasing; no client frame between two due timers; a
timer's own stamp at or above its deadline; never early). Rule 5, **"lateness
must pre-date the pass"**, was added during execution because the first four
cannot distinguish a genuine clients-before-timers order swap from legitimate
lateness once the clamp has been applied: the two produce the same stamp
relation. Rule 5 keys on the fact that every frame a single pass produces
carries that pass's `now`, so a timer marked late must have been late
*before* the pass began; an order swap is caught, a real post-failover late
fire is not. Rules 2 and 3 are logical consequences of rule 1 and are run
**before** it, deliberately, so their more specific message wins when they
name the same violation.

**Capstones.** A `uc_lincheck::TimerSm` whose commands schedule and
cancel, and whose `on_timer` appends to its history, run through `lin_v2`
(failover and purge/snapshot churn) and the hard-crash harness, asserting:
exactly-once delivery per instance, at the same position on every replica;
the §4.3 ordering over the whole history; `late()` only after a leader
change. Elle: unchanged.

**As-built erratum (plan 1, 2026-09-03) — the oracle is shared, and
"exactly-once" needed a completeness half.** The assertions above are not
written twice: they live in one function,
`uc_lincheck::timer::assert_timer_report(tag, &TimerReport) -> TimerStats`,
called by both `lin_v2`'s `two_fsm_timer_churn_under_failover` and the
hard-crash harness's `two_fsm_timer_service_sigkill`. It checks never-early,
the §4.3 order, replication equivalence, and **exactly-once as two halves**:
no duplicate `(id, deadline)`, *and* no loss (every scheduled instance that
was not cancelled and not superseded has a fire). The no-loss half needs a
completeness margin, because a run's history ends while instances are
legitimately still in flight: `COMPLETENESS_MARGIN_NS = 250 ms`, and an
instance whose deadline is within that of the last observed stamp is skipped
rather than reported lost. 250 ms is sized by the asynchronous service → node
hop a schedule request makes (the `svc_sched` SPSC ring, drained once per
consensus pass), not by the timer machinery, which is why it is generous.
Without the no-loss half the duplicate check alone is nearly unfalsifiable: a
state machine that silently dropped every timer would pass it.

**As-built erratum (plan 1, 2026-09-03) — `TimerSource` did not survive
contact.** §4.8's `trait TimerSource { fn pending_timers(&self) -> Vec<(u64,
u64)> }` is as built a **provided method on `RawStateMachine`** with a
`Vec::new()` default, overridden by `Timed`. A separate trait would have made
the apply loop carry a second bound on every generic path it already threads
`S: RawStateMachine` through, for a hook whose default is "nothing". The
semantics are the spec's, unchanged.

**Fleet gate** (`docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md`,
committed as a bars-only skeleton with an added row d, the isolated apply-hop
A/B M14a's codegen lesson demands;
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
   metrics, alert rule, `uc2ctl status`. Re-arm runs on both leader-exit paths:
   `Action::BecomeFollower` (which already drops the appender) and `halt()`
   (removed from the cluster). The pending ingress payloads carry across a role
   flip by design and are not touched.
5. Sim invariant + fault scenario; `TimerSm` capstone through `lin_v2` and
   the hard-crash harness; loom/Lean/conform regression.
6. Docs + release writeup; fleet gate rows.

**Plan 2 — the schedule table** — **AS BUILT 2026-09-03**, plan
`docs/superpowers/plans/2026-09-03-uc2-time-and-timers-plan2.md` (T0–T8),
commits `b71a1f6..a0c2cd8` on the same unreleased `2.11.0` flag day. Every
item below is done; see the as-built errata at the end of §5 for the three
rulings that amend this section's design.

7. `uc_protocol`: `FRAME_TYPE_SCHEDULE_TABLE` + codec + fuzz target. **Done**
   (`d5fc6a3`, `b38abb0`, `bdf3e63`): `FRAME_TYPE_SCHEDULE_TABLE = 6`,
   `FLAG_TIMER_TABLE` un-reserved, the frozen 33-byte-entry codec with
   `Every`/`DailyAt`/`Once` arithmetic, `SchedOp::TableConsumed = 4`,
   `ADMIN_OP_SCHEDULE_APPLY = 6`, and `uc_protocol_schedule_table` as the
   eighteenth fuzz target — which found the saturating-arithmetic overflows at
   the top of the `u64` range.
8. `uc_node`: adoption via the archive walk; `state/schedules.state`;
   next-deadline arithmetic; `uc2ctl schedule apply` (admin auth, audit).
   **Done** (`3776f3a`, `5bb51a8`, `09c3ed4`, `46c3143`, `e133aa1`,
   `f3e6e29`, `59c79d8`): `uc_log::Appender::append_schedule_table` + the
   archive's table observations; `RowTimers`' table half; adoption, the
   `ScheduleRecord` with `prev`, boot arming, firing with the flag, the admin
   op with its four refusals, and the three metric families; then `uc2ctl
   schedule apply` / `schedule show`.
9. `uc_service`: the `table_last` rule in `Timed`; `ev.table`. **Done**
   (`e282a0a`): `Timed` reports table ticks as `TableConsumed` — including
   for a dropped duplicate, so the node clears its state either way — and
   announces `table_last` after attach and after replay.
10. Tests, runbook, attack-surface entry, release bullet. **Done**
    (`d19053f`, `a0c2cd8`, and this docs pass): two end-to-end tests in
    `uc_node/tests/timers.rs`, the signed/digest-checked/leader-only/audited
    admin test, four `RowTimers` unit tests, clause (7) of the shared
    `assert_timer_report` oracle exercised by
    `two_fsm_timer_churn_under_failover`, the fuzz target, and the reference /
    runbook / attack-surface / explainer / release / gate-row-e writeup.
