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
| Multi-node (`m2_multi_node`) | ❌ panics at the I/O boundary (below) |

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

## How to run

```bash
cargo test -p uc-rt-busyspin                                              # conformance Suite
cargo test -p uc_node --features busyspin-runtime --test m1_single_node   # single-node boot on busy-spin
UC_BUSYSPIN_WORKERS=4 cargo test -p uc_node --features busyspin-runtime --test m1_single_node
```

The swap is **feature-gated** (`busyspin-runtime`, default off) so the default
build/test stays on tokio and green. Flipping it is the one `#[cfg]`'d line in
`uc_node/src/raft/mod.rs`.

## Next steps (not yet done)

1. Resolve the tokio-reactor boundary (hybrid `Handle::enter()` is the smaller
   step) and get `m2_multi_node` green on busy-spin.
2. CPU-affinity pinning in the executor (`core_affinity`), worker-per-core.
3. Only then: replace the hot internal channels with `Send` busy-spin rings and
   measure against the floor decomposition (the actual perf payoff).
