# Design: Event-Driven Ring Wakeups for `uc_*` shmem IPC

**Date:** 2026-06-04
**Status:** Draft for review
**Crates:** `uc_protocol` (ring layer), `uc_node`, `uc_service`, `uc_client`

## Problem

The task09 full-path attribution (`bench-out/reference/attribution.csv`) measured
the single-node commit path and found the floor is **IPC wakeup latency on the
lock-free shmem rings**, not fsync. At **inflight=1** (no queueing), total p99 is
~4.5 ms, spread across the commit-path ring hops:

| hop | inflight=1 contribution |
|-----|------------------------|
| submit (client→node) | ~2.1 ms |
| resp_ring + broadcast_to_client | ~2 ms each |
| journal_fsync (real ext4) | ~0.85 ms (4.3% of total) |

Each ring consumer today **poll-sleeps**: `try_read`; on empty,
`sleep(IDLE/POLL/EMPTY_BACKOFF)` (~100 µs–2 ms). A request handed to an idle
consumer waits up to one backoff interval *per hop*; across submit → apply →
apply_resp → broadcast that compounds into the ~4.5 ms floor.

This design replaces poll-sleep on the commit-path rings with **event-driven
wakeups** (Aeron-style): the consumer parks on a futex over a shared-memory word
and the producer wakes it on publish. Goal: collapse the per-hop wakeup latency
from ~2 ms to the underlying hand-off cost (tens of µs).

### Updated baseline — the Eventual + batched journal (d55922c + ultima_journal)

The numbers above were captured under `Durability::Consistent`. Since then,
`uc_node` defaults the Raft **log** journal to `Durability::Eventual`
(`d55922c`), backed by `ultima_journal`'s background-writer model: `append`
returns a `Notifier` that fires **after the buffered page-cache write** (not
fsync), and fsync is **batched/coalesced periodically (~50 ms idle-fsync)** on
the writer thread, advancing a separate `durable_seq` watermark (durability via
quorum replication). **Net effect on this work:** fsync is now *off* the commit
critical path, so `journal_durable` ≈ 0 on the path and the inflight=1 floor is
**almost entirely IPC poll-sleep wakeup latency** — this change *strengthens* the
case for event-driven wakeups rather than competing with it. The two are
**orthogonal and complementary**: the journal change removes fsync from the log
path (`ultima_journal`); this design removes poll-sleep from the four IPC commit
rings (`uc_protocol`). No code overlap.

Two consequences for measurement:
- The committed `bench-out/reference/attribution.csv` is a **stale (Consistent)**
  baseline. Re-capture it under the current Eventual default *before* the wakeup
  work so the wakeup win is isolated from the journal-durability win.
- The probe was renamed `JournalFsynced → JournalDurable` (`92f9bca`, task10
  work) and the attribution stage is now `journal_durable` — it fires at the
  buffered write, measuring the bg-writer hand-off, not an fsync. The plan's
  measurement `awk` filters use `journal_durable`.

## Scope decisions (from brainstorming)

- **Target:** the low-concurrency *latency floor*. Throughput under concurrency is
  a separate, larger cost — queueing through the serial commit/apply pipeline
  (`submit_to_node` grows 2 ms → 20 ms as inflight goes 1 → 16). **Pipeline
  de-serialization is explicitly deferred** to a follow-up; this spec does not
  touch it. The attribution will isolate it cleanly once the wakeup floor is gone.
- **Rings in scope:** the four commit-path hops — submit (MPSC client→node),
  apply (SPSC node→service), apply_resp (SPSC service→node), response
  (Broadcast node→clients). **The query/read path is out of scope.**
- **Platform:** Linux futex now, **behind an abstraction seam** (`RingParker`
  trait) so a portable fallback can slot in later without touching call sites. A
  `PollParker` (today's sleep-backoff) is the reference fallback impl.

## Non-Goals

- Pipeline de-serialization (concurrent dispatch / parallel apply). Deferred.
- Query/query_resp read-path rings. Deferred.
- A non-Linux `RingParker` beyond the `PollParker` fallback. Deferred (YAGNI).
- Any change to record framing, wire format, or `RING_HEADER_LEN`.

## Architecture

### Component 1: the wakeup word — reuse `publish_position`

A futex operates on a `u32`. `RingHeader` (`uc_protocol/src/ring/common.rs`,
`#[repr(C, align(64))]`) already has a cache-line-isolated
`publish_position: AtomicU64` that increments on every publish. Consumers futex-wait
on its **low 32 bits**: "the word changed" == "a record was published." No new
field for the wait address. (32 bits is ample; wraparound every 4 B publishes is
harmless for a wait-and-recheck primitive — a spurious wake at worst.)

### Component 2: `waiters` flag — reclaimed from existing pad (no protocol bump)

To avoid a `FUTEX_WAKE` syscall on every publish, add a `waiters: AtomicU32`
**carved from an existing `_pad` region**, on the **consumer's cache line**
(adjacent to `consumer_position`, i.e. from `_pad_4`): the consumer writes it,
the producer reads it — same access pattern as `consumer_position`, so no new
false sharing. Reusing pad keeps `RING_HEADER_LEN` unchanged → **no `cnc.dat` /
ring on-disk change and no protocol-version bump.** For SPSC/MPSC `waiters` is
0/1 (single consumer); for Broadcast it is a *count* of parked consumers.

### Component 3: the `RingParker` seam

```rust
/// Cross-process consumer parking over a ring's shared-memory wakeup word.
pub trait RingParker {
    /// Producer: wake parked consumer(s) iff `waiters > 0`.
    fn signal(&self);
    /// Consumer: block until the wakeup word leaves `expected_seq`, or `timeout`.
    fn park(&self, expected_seq: u32, timeout: Duration);
}
```

- `FutexParker` (Linux) — `rustix::thread::futex` `Wait`/`Wake` on
  `&publish_position` low-32; `signal()` does `Wake(1)` for SPSC/MPSC, `Wake(i32::MAX)`
  for Broadcast, gated on `waiters > 0`.
- `PollParker` — `park()` = `sleep(min(timeout, backoff))`, `signal()` = no-op.
  The current behavior, kept as fallback and as a test oracle.

The parker is constructed from the ring's mapped header (it borrows the
`publish_position` / `waiters` atomics). Lives in `uc_protocol::ring`.

### Component 4: idle strategy (spin-then-park)

Consumers `try_read` a few iterations (catches in-flight messages at ~zero
latency), then `park`. Aeron backoff-idle. Steady-state idle CPU ≈ 0 (parked).

### Component 5: per-ring wiring & the async bridge

| Ring (type) | producer `signal()` site | consumer `park()` site | consumer context |
|---|---|---|---|
| submit (MPSC) | `uc_client` submit write | node `client_dispatcher` | async (node rt) |
| apply (SPSC) | node `publish_apply` | service `apply_loop` | sync (std::thread) |
| apply_resp (SPSC) | service apply-resp write | node `await_apply_resp` | async (sm worker) |
| response (Broadcast) | node `broadcast_record` | `uc_client` broadcast reader | async (client rt) |

- **Sync consumer** (service `apply_loop`): call `parker.park(seq, timeout)`
  directly in place of `std::thread::sleep`.
- **Async consumers** (3 of 4): a blocking `FUTEX_WAIT` cannot run on a
  `current_thread` tokio task without stalling the runtime, so each async-consumed
  ring gets **one dedicated bridge OS thread** that loops `parker.park(...)` and
  fires a `tokio::sync::Notify`. The async consumer becomes:
  ```rust
  loop {
      match consumer.try_read(&mut buf) {
          Ok(Some(rec)) => { /* handle */ }
          Ok(None) => parked.notified().await,
          Err(e) => /* existing error path */,
      }
  }
  ```
  The bridge thread is the only blocking point; the async task only awaits a
  `Notify`. One bridge per async-consumed ring: submit (node), apply_resp (node),
  response (client).

Producer signal sites coincide with the existing commit-path probe stamp points.

## Correctness

### Lost-wakeup protocol

Producer publishes between the consumer's last `try_read` and its `FUTEX_WAIT` →
wakeup missed. Closed by the futex `expected` value + arm-then-recheck:

```rust
// consumer
loop {
    if let Some(r) = try_read()? { return Ok(Some(r)); }
    let seq = publish_low32();                 // snapshot the wait word
    waiters.fetch_add(1, AcqRel);
    if let Some(r) = try_read()? {             // re-check AFTER arming
        waiters.fetch_sub(1, AcqRel);
        return Ok(Some(r));
    }
    parker.park(seq, T_max);                    // FUTEX_WAIT(expected = seq); EAGAIN if moved
    waiters.fetch_sub(1, AcqRel);
}
```

```rust
// producer
publish_position.store(new, Release);          // record visible first
if waiters.load(Acquire) > 0 { parker.signal(); }
```

If the producer publishes after the snapshot, `publish_position != seq` →
`FUTEX_WAIT` returns `EAGAIN` immediately → loop re-reads. Publish-then-check on
the producer + arm-then-recheck on the consumer is the standard futex condition
pattern and admits no lost wakeup.

### Shutdown interruption

Parking must never block shutdown (we just fixed one shutdown deadlock —
`m3_shutdown_dead_service`). Two mechanisms: (a) the **timeout backstop** caps
every park at `T_max`; (b) shutdown sets the existing stop flag (per consumer)
and issues a `parker.signal()` so a parked consumer/bridge thread wakes
immediately, observes stop, and exits. Bridge threads join on the existing
dispatcher/loop stop path. This composes with `node.shutdown()`'s
`signal_shutdown()` and the apply-interrupt work already in `state_machine_shmem`.

### Timeout backstop

Every `park` takes a bounded timeout (the current backoff ceiling, ~1–2 ms). Even
if a wakeup is missed or a producer crashes mid-publish, the consumer re-polls
within `T_max`. **Poll-sleep becomes the worst case, not the steady state** —
correctness never depends on a wakeup arriving, only latency does.

### `unsafe` / memory ordering

The futex syscall wrapper (`rustix`) and the `publish_position` low-32 reinterpret
are the only syscall/`unsafe` surface, confined to `FutexParker` in
`uc_protocol::ring`, reviewed under the project's unsafe-shmem conventions.
Acquire/Release pairing as shown above; `waiters` on the consumer cache line.

## Dependencies

- `rustix` (or `nix`) added to `uc_protocol` for `futex` — `uc_protocol`'s ring
  module already requires `std` (mmap, atomics), so this is consistent with the
  existing posture; the pure-data modules (`version`/`magic`/`error_codes`) stay
  `core`-only.

## Testing

**Unit (`uc_protocol::ring`, mechanism in isolation):**
- *Lost-wakeup stress*: producer/consumer threads with random tiny gaps over
  SPSC/MPSC; assert every record received and the consumer never sleeps past
  `T_max` with data available. Many iterations — this is the race that matters.
- *Wake-one vs wake-all*: SPSC/MPSC wake exactly one parked consumer; Broadcast
  wakes all N.
- *`waiters` gating*: producer issues zero wakes when `waiters == 0`.
- *Timeout backstop*: a consumer with no producer returns from `park` within `T_max`.
- *`PollParker` fallback*: the same suite passes against the poll impl (proves the seam).
- Existing `ring_torture` stays green (no read/write semantic change).

**Integration:**
- All existing commit-path tests stay green (m1/m3 shmem, m4 client, m5 output).
- *Shutdown-still-clean*: `m3_shutdown_dead_service` + `m3_service_crash` stay
  green — parker/bridge threads don't reintroduce a shutdown hang.
- *Idle CPU*: an idle node+service is parked, not busy-polling (assert/inspect).

**Success criterion (measured via the harness):**
**First re-capture the reference under the current Eventual default** (it is
currently a stale Consistent capture) so the wakeup win is isolated from the
journal-durability win — expect `journal_durable` ≈ 0 and the floor essentially all
IPC poll-sleep. Then re-run `attribution-bench` at **inflight=1** and diff
against that fresh Eventual reference. Target: per-hop wakeup latency collapses
from ~2 ms to tens of µs; **total p99 at inflight=1 from ~3–4 ms (Eventual, IPC
floor) toward sub-millisecond.** Capture the post-wakeup reference. At high
inflight the queueing term remains (the deferred pipeline work) — and the
attribution will now
show it isolated, which is the data we want before starting that follow-up.

## Deliverables

- `uc_protocol/src/ring/common.rs`: `waiters: AtomicU32` reclaimed from `_pad_4`;
  `publish_position` low-32 wait accessor; `RingParker` trait + `FutexParker` /
  `PollParker`; producer `signal()` on the publish path; spin-then-park + the
  arm-recheck loop in the shared consumer read helper.
- `uc_protocol/Cargo.toml`: `rustix` dep (ring/futex).
- `uc_node`: bridge thread + `Notify` for submit (`client_dispatcher`) and
  apply_resp (`state_machine_shmem::await_apply_resp`); `signal()` at
  `publish_apply` and `broadcast_record`; shutdown wires `signal()` + stop.
- `uc_service`: `apply_loop` parks directly; apply-resp write `signal()`.
- `uc_client`: bridge thread + `Notify` for the broadcast reader; submit write `signal()`.
- `docs/tasks/task11_event_driven_ring_wakeups.md`: mechanism, correctness model,
  before/after attribution numbers.
- `cargo clippy --workspace --all-targets -- -D warnings` clean; workspace tests green.

## Open risks / notes

- **Async↔futex bridge is the fiddly part.** Contained behind one bridge thread
  per async-consumed ring; the timeout backstop bounds any bug to added latency,
  never a hang.
- **Bridge thread lifecycle.** Each must be joined on shutdown and woken via
  `signal()`+stop; covered by the shutdown-clean tests.
- **32-bit wait word wraparound** is benign for wait-and-recheck (worst case: one
  spurious wake every 4 B publishes).
- **`rustix` futex API surface** is small and stable; if unavailable, `nix` or a
  thin `libc::syscall(SYS_futex)` wrapper behind `FutexParker` is equivalent.
