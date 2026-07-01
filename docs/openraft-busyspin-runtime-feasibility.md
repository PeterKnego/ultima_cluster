# openraft on a CPU-pinned busy-spin runtime — feasibility (demonstrated)

**Date:** 2026-06-27 · **Branch:** `spike/openraft-hotpath-runtime` · **openraft:** alpha.25
**Spec:** `docs/superpowers/specs/2026-06-27-openraft-hotpath-runtime-collapse-spike-design.md`

## Question

Can openraft run on a CPU-pinned, multi-threaded, **busy-spin** runtime whose
scheduler never parks — eliminating the cross-thread futex wakeups that the
floor decomposition attributed ~73% of the commit floor to — **without forking
openraft's consensus logic**?

## Verdict: yes (demonstrated end-to-end in UC for the single-node case)

A ~3-file runtime crate (`uc-rt-busyspin`) implementing openraft's `AsyncRuntime`
boots UC and commits writes with openraft's internal tasks driven entirely by a
never-park busy-poll executor. The edge is **not** openraft — it is the tokio
I/O reactor that quinn needs at the network boundary.

## Why it's possible (architecture audit, alpha.25)

- `AsyncRuntime` is **channel-complete**: `Mpsc`/`Watch`/`Oneshot`/`Mutex` are
  associated types alongside `spawn`/timers (`rt/src/async_runtime.rs`).
- openraft **core has no direct tokio dependency** (only `openraft-rt` +
  optional `openraft-rt-tokio`); `default-features = false` removes tokio
  entirely. No `tokio::` in RaftCore/replication/sm/engine.
- Precedent: three backends ship (`rt-tokio`, `rt-monoio` thread-per-core,
  `rt-compio`). A backend is ~450 LOC / 6 files. Binding is one line in
  `declare_raft_types!`.
- rt-monoio is **not** our target: it parks on io_uring, is single-threaded
  (`!Send`), reuses tokio's Watch/Mutex, and would force an io_uring rewrite of
  UC's quinn/journal I/O. Useful as a file-layout template only.

## The crate (`uc-rt-busyspin`)

- **executor.rs** — busy-poll worker pool. Workers re-poll their tasks in a tight
  loop with a no-op waker and never park. Round-robin task distribution via
  `std::mpsc`. `JoinHandle` is waker-correct (works when awaited from tokio).
  *Skeleton gaps:* no CPU-affinity pinning yet (one-line `core_affinity` add,
  marked TODO), no work-stealing, task panics not caught. `UC_BUSYSPIN_WORKERS`
  sets the pool size.
- **timer.rs** — poll-based `Instant`/`Sleep`/`Timeout`; compares the monotonic
  clock per poll. No timer wheel, **no reactor dependency**.
- **lib.rs** — `UcBusySpinRuntime: AsyncRuntime`. Channels/mutex are **reused
  from `openraft-rt-tokio`** (tokio `sync` is runtime-agnostic and waker-correct).

### Key insight: the win is the *executor*, not the rings

openraft uses the same `AsyncRuntime` channel types at the **tokio<->openraft API
boundary** (e.g. `client_write().await` on a tokio thread awaits a oneshot
completed by RaftCore on a busy-spin worker) and **internally**. A no-waker
busy-spin ring awaited on the tokio side would return `Pending`, tokio parks it,
and our side never wakes it -> hang. So channels must stay **waker-correct**. The
futex-elimination comes from the **busy-poll executor** (re-polls its tasks every
iteration; the openraft-internal hop becomes a same-loop re-poll, ~ns, instead of
a ~8.8 µs cross-thread futex wake). Replacing the hot *internal* channels with
`Send` busy-spin rings is a later, boundary-aware optimization — not required for
the win.

## Results (gates)

| Gate | Result |
|---|---|
| openraft `AsyncRuntime` conformance `Suite::<UcBusySpinRuntime>::test_all()` | ✅ green |
| `uc_node` compiles with `AsyncRuntime = UcBusySpinRuntime` (1-line swap) | ✅ |
| Single-node M1 boot + commit + restart (`m1_single_node`, both tests) | ✅ green on busy-spin |
| Multi-node (`m2_multi_node`) | ✅ **with the hybrid reactor** (below) — election, replication, failover all green on busy-spin |

### The boundary (the only blocker, and it's expected)

```
thread 'uc-busyspin-0' panicked at quinn-0.11.9/src/runtime/tokio.rs:27:9:
there is no reactor running, must be called from the context of a Tokio 1.x runtime
```

openraft awaits UC's network/storage futures **on this runtime**. The synchronous
journal is fine (single-node boots with no reactor). But quinn's `append_entries`
future, polled on a busy-spin worker, needs a tokio reactor. Two resolutions,
both still open:

1. **Hybrid:** run a co-resident tokio reactor thread and `Handle::enter()` it on
   the busy-spin workers, so reactor-bound I/O futures find a driver. Wakers fire
   from the reactor; the busy-poll loop re-polls regardless.
2. **Ring-decouple (UC-idiomatic):** UC's network adapter submits the RPC to a
   tokio thread over a ring and awaits a oneshot, so the busy-spin executor never
   polls a reactor-bound future. The physical 27% (wire RTT + fsync) lives there
   anyway, so a futex hop on that boundary is harmless.

## Hybrid reactor — multi-node now works (2026-06-27, follow-up)

The boundary above is resolved by **resolution 1**: the busy-spin workers enter
the application's *own* ambient tokio runtime. `executor::capture_reactor()`
grabs `Handle::try_current()` at openraft's first `spawn` (which runs inside
`Raft::new`, i.e. within UC's runtime), and each worker lazily `Handle::enter()`s
it for its lifetime. Entering UC's *own* runtime (not a separate reactor) keeps
quinn's socket ownership consistent — the endpoint is created on that same
runtime. quinn's I/O, its internal `tokio::spawn` connection drivers, and tokio
timers then all find a driver; the busy-poll loop re-polls regardless of wakers.

Result: real 3-node consensus runs on the busy-spin executor —
`three_node_cluster_elects_leader`, `three_node_replication` (writes replicated
over quinn), and `leader_failover` all pass (each in a fresh process).

### Multi-runtime handle refresh (the bug the suite found)

First cut captured the app runtime in a write-once `OnceLock`. But each
`#[tokio::test]` builds and drops its **own** tokio runtime, so from the second
test on the workers were entered into a **dropped** runtime and quinn I/O hung
(live-task count climbing, never draining). Fix: the captured handle is
**refreshable** — `REACTOR_HANDLE` is a `Mutex<Option<Handle>>` with a generation
counter; it is **cleared when the pool drains to zero** (all nodes stopped) and
re-captured on the next spawn, and workers re-enter when the generation changes.
A `LIVE_TASKS` counter drives the drain detection (also handy via
`UC_BUSYSPIN_DEBUG`). In production (one long-lived runtime; the pool never idles
to zero while a node runs) it captures once and never changes.

With that, the **full `m2_multi_node` suite passes serialized** (and
`m4_client_three_node`, etc.): all five — elect, replication, failover, snapshot
install, membership removal — green on busy-spin.

### Remaining limitations (skeleton)

- **Run in-process multi-node suites with `--test-threads=1`.** The handle is one
  process-global slot; *parallel* tests run *concurrent* runtimes that can't all
  be the captured one. Serialized is required — and matches UC's existing
  convention for in-process multi-node suites (e.g. `fault-injection`).
- **Busy-spin workers peg cores.** On the 4-core box even two workers starve
  tokio's I/O thread, so the default is **one** worker; idle pacing uses
  `yield_now()` (coexists with tokio) unless `UC_BUSYSPIN_PURE=1`. Multi-worker
  wants dedicated/pinned cores (`UC_BUSYSPIN_WORKERS`).
- **Process-global pool, no per-task cancellation.** Sufficient now (openraft
  self-terminates its tasks on `Raft::shutdown`, and the drain-to-zero refresh
  handles sequential runtimes). A genuine **per-node executor** would also unlock
  parallel in-process tests — deferred.

## How to run

```bash
cargo test -p uc-rt-busyspin                                              # conformance Suite
cargo test -p uc_node --features busyspin-runtime --test m1_single_node   # single-node boot on busy-spin
# multi-node: serialized (one process-global runtime handle slot)
cargo test -p uc_node --features busyspin-runtime --test m2_multi_node -- --test-threads=1
```

The swap is **feature-gated** (`busyspin-runtime`, default off) so the default
build/test stays on tokio and green. Flipping it is the one `#[cfg]`'d line in
`uc_node/src/raft/mod.rs`.

## Next steps (not yet done)

1. ~~Resolve the tokio-reactor boundary~~ — **done** (hybrid `Handle::enter()`;
   multi-node consensus green on busy-spin).
2. **Per-node executor** (drop the process-global singleton + per-node runtime
   handle). The drain-to-zero handle refresh already makes the full in-process
   multi-node suites pass *serialized*; a per-node executor would additionally
   unblock running them *in parallel* (the default `cargo test`).
3. CPU-affinity pinning in the executor (`core_affinity`), worker-per-core, with
   a clear core-reservation story so busy-spin workers don't starve tokio's I/O.
4. Only then: replace the hot internal channels with `Send` busy-spin rings and
   **measure against the floor decomposition** (the actual perf payoff — still
   unmeasured).
