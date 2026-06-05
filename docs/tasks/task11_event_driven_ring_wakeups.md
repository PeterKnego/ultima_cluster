# Task 11 — Event-Driven Ring Wakeups (commit-path latency floor)

**Status:** Shipped. Latency floor collapsed ~4.6× (inflight=1); deferred items noted below.
**Crates:** `uc_protocol` (ring layer), `uc_node`, `uc_service`, `uc_client`.
**Supersedes (consolidates):** `docs/superpowers/{specs,plans}/2026-06-0{4,5}-event-driven-ring-wakeups*` (deleted per the CLAUDE.md feature workflow).

## Why

The task09 full-path attribution showed the single-node commit floor was **IPC
wakeup latency on the lock-free shmem rings**, not fsync. Every ring consumer
poll-slept: `try_read`; on empty, `sleep(~100 µs)`. Because tokio's timer
granularity is ~1 ms, each empty poll actually cost ~1 ms, and across the four
commit-path hops (submit → apply → apply_resp → broadcast) that compounded into
a multi-millisecond floor. With the Raft log journal now defaulting to
`Eventual` (task10 — fsync batched off the commit path), the floor was
**~99% IPC poll-sleep**: at inflight=1 on real disk, `journal_durable` ≈ 63 µs
while `submit_to_node`, `resp_ring`, `broadcast_to_client` were each ~2 ms.

## Mechanism

Replace poll-sleep on the four commit-path rings with **futex wakeups**
(Aeron-style: consumer parks on a shared-memory word, producer wakes it on
publish).

- **Wakeup word = `publish_position` low 32 bits.** A futex operates on a `u32`;
  `RingHeader.publish_position` (`AtomicU64`) already increments on every
  publish, so its low half *is* the "a record arrived" signal. No new wire field
  (`RingHeader::wake_word()`, guarded by a little-endian `compile_error!`).
- **`waiters: AtomicU32` reclaimed from `_pad_4`** (consumer cache line) so the
  producer skips the `FUTEX_WAKE` syscall when nobody is parked. Reusing pad
  keeps `RING_HEADER_LEN == 256` → **no `cnc.dat`/ring on-disk change, no
  protocol-version bump.**
- **Cross-process futex** via `libc::syscall(SYS_futex, …)` with **no
  `FUTEX_PRIVATE_FLAG`** — client/node/service are separate processes sharing the
  ring mmap; a private futex would not wake across processes.
- **`ParkMode` seam** (`Futex` default on Linux / `Poll` fallback) so the whole
  mechanism is swappable and testable; `Poll` reuses the old sleep-backoff.
- **Spin-then-park** (`SPIN_TRIES` `try_read` spins before parking) catches
  in-flight records at ~zero latency; **`PARK_CEIL` (2 ms) timeout backstop**
  makes poll-sleep the *worst* case, never a hang.
- **Producers signal automatically**: `signal()` is folded into the producer
  publish path (`SpscProducer::try_write`, `MpscProducer::try_write`,
  `BroadcastProducer::write`) right after the real-record `publish_position`
  store — SPSC/MPSC wake one, **Broadcast wakes all** (`futex_wake(i32::MAX)`).

## Sync vs async consumers

| Ring (type) | consumer | context | wakeup path |
|---|---|---|---|
| submit (MPSC client→node) | node `client_dispatcher` | async | `NotifyBridge` |
| apply (SPSC node→service) | service `apply_loop` | sync std::thread | direct `read_or_park` |
| apply_resp (SPSC service→node) | node `await_apply_resp` | async (openraft SM worker) | `NotifyBridge` |
| response (Broadcast node→clients) | client broadcast reader | async | `NotifyBridge` |

- **Sync** (`SpscConsumer::read_or_park`): spin → snapshot `current_seq` →
  `arm()` → re-`try_read` → `park(seq, PARK_CEIL)` → `disarm()`. The
  arm-then-recheck + futex `expected` value closes the lost-wakeup race (if the
  producer published in the gap, `FUTEX_WAIT` returns `EAGAIN` immediately).
- **Async** (`NotifyBridge`, one per async-consumed ring): a dedicated parker OS
  thread blocks on the futex and fires a `tokio::sync::Notify`; the async
  consumer loops `match try_read { None => bridge.notified().await }`. The
  blocking `FUTEX_WAIT` lives only on the bridge thread, never on the runtime.
  `tokio::Notify`'s stored-permit semantics close the gap between the consumer's
  `try_read==None` and its `notified().await`. Bridge `shutdown()` keeps a cloned
  `RingWaitHandle` and force-wakes the parker (`RingWaitHandle::wake()`) so the
  thread join returns promptly instead of stalling up to `PARK_CEIL`.
  `uc_client` carries its own copy of `NotifyBridge` (it cannot depend on
  `uc_node`; a future shared `uc_ipc` crate could de-duplicate it).

## Correctness

- **Lost wakeups**: sync path uses arm-then-recheck + futex `expected` (no miss).
  Async bridge path is bounded by the `PARK_CEIL` backstop in the rare
  snapshot-race window; correctness never depends on the wake, only latency.
- **Shutdown**: every park has the `PARK_CEIL` cap; `node.shutdown()` sets stop
  flags and bridge `Drop`/`shutdown()` force-wakes + joins the parker thread.
  This composes with the prior shmem shutdown-deadlock fix (task04 follow-up):
  `await_apply_resp` still checks its `shutdown` `AtomicBool` first and returns
  `Interrupted`. Verified by `m3_shutdown_dead_service` staying green.
- **Tests**: `ring::futex` (wake/EAGAIN/timeout); SPSC `lost_wakeup_stress`
  (both `Futex` and `Poll` modes, 2000 records, ordering asserted);
  Broadcast `wake_all_unblocks_two_consumers` (tight timeout + prompt-wake
  assertion that actually catches a wake-one regression). Full
  `cargo test --workspace` green (default parallel and `--test-threads=1`);
  `cargo clippy --workspace --all-targets -- -D warnings` clean.

## Results (attribution, p99, 64 B, vs the Eventual baseline)

| config | total before | total after | factor |
|---|---:|---:|---:|
| disk, inflight=1 (the floor) | 5.02 ms | **1.08 ms** | **4.6×** |
| disk, inflight=8 | 16.9 ms | 4.5 ms | 3.8× |
| tmpfs, inflight=8 | 12.3 ms | 1.41 ms | 8.7× |

At inflight=1 the IPC hops collapsed 27–45× (`submit_to_node` 2134→65 µs,
`resp_ring` 2105→77 µs, `broadcast_to_client` 2078→46 µs). The **new floor is
`commit_to_apply_enq` (~672 µs)** — openraft's internal commit→apply handoff, not
a ring hop. Baseline + post references are in `bench-out/reference/attribution.csv`
(reproduce with `attribution-bench`; `TMPDIR=<ext4 path>` selects the disk axis).

## Deferred (data now isolates them)

- **Pipeline de-serialization** (concurrent dispatch / parallel apply). At
  inflight=8 the residual cost is `submit_to_node` queueing behind the serial
  `client_dispatcher` (one `client_write` awaited at a time) — the attribution
  now shows this cleanly (it grows with inflight; ~3.8 ms at if8 on disk). This is
  the next lever for *throughput*, distinct from the latency floor this task fixed.
- **The new `commit_to_apply_enq` floor** (~672 µs) — openraft commit→apply.
- **Query/read-path rings** — not wired (out of scope).
- **Non-Linux `RingParker`** — only the `Poll` fallback exists.
- **Hot-path `waiters` load**: `signal()` does one `Acquire` load of `waiters`
  (consumer cache line) per publish; negligible at commit-path rates, noted for
  any future saturation-throughput tuning.
