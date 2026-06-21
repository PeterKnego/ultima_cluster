# O1 prototype: busy-spin the service apply consumer — design

**Date:** 2026-06-21
**Type:** prototype (perf spike, env-gated, default-off)
**Origin:** opportunity **O1** in `docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md`

## Motivation

The investigation measured a futex park/wake at **~8.8 µs/wakeup** vs busy-spin **~29 ns**
(~300×), and identified the intra-host ring consumers as the place to reclaim that — the
intra-host analog of Aeron's polling consumers (explicitly **not** the settled-negative
cross-host busy-poll). This prototype targets the **single hottest hop**: the service apply
consumer (commit-path hops #5/#6), where the service reads `ApplyFrame`s from the SPSC
`apply.ring`.

## Scope

- **In:** the service apply consumer only (`uc_service` apply loop + the `uc_protocol` SPSC
  consumer it drives).
- **Out (noted for later):** the node-side `submit`/`apply_resp` `NotifyBridge` parker threads
  (T-1/T-4); O3 adaptive spin→park backoff.

## Mechanism — configurable spin budget, then park (B)

The consumer is *already* spin-then-park (`SPIN_TRIES = 64`, then `FUTEX_WAIT` up to
`PARK_CEIL = 2 ms`). The prototype makes the spin window configurable, with a sentinel for
pure busy-spin:

- **finite budget `N`** (default `N = SPIN_TRIES = 64`): spin `N` tries on `try_read`, then fall
  back to the **existing, unchanged** arm-recheck-futex-park path.
- **sentinel `u32::MAX` (busy mode):** spin a fixed chunk on `try_read`, then return `Ok(None)`
  **without parking**, so the apply loop immediately re-calls — continuous busy-spin, **no
  `FUTEX_WAIT` ever**.

Busy mode = a strict superset: at budget `= ∞` it is pure O1 busy-spin; finite budgets are
idle-safe. The default (env unset) is **byte-for-byte the current behavior**.

## Components / files

1. **`uc_protocol/src/ring/spsc.rs`** — `SpscConsumer` gains `spin_budget: u32` (default
   `SPIN_TRIES`) + `set_spin_budget(u32)`. `read_or_park` loops `0..spin_budget` (was the
   const), and on exhaustion: park (finite) or return `Ok(None)` (sentinel). `uc_protocol`
   stays env-free — it only exposes the knob.
2. **`uc_service/src/runtime/apply_loop.rs`** — reads `UC_APPLY_SPIN_BUDGET` (matching the
   `UC_JOURNAL_PREALLOC` env convention): unset → default 64; `<N>` → N then park; `busy` →
   sentinel pure-spin. Configures the consumer once before the loop. Logs the chosen mode.

## Correctness

- Busy mode is strictly "poll `try_read` in a loop"; it never parks, so the arm-then-recheck
  lost-wakeup machinery (which only guards the *park*) is not engaged — no new race.
- Shutdown stays prompt: the apply loop already treats `Ok(None)` as "re-check the stop flag,"
  and busy mode returns `Ok(None)` frequently (sooner than `PARK_CEIL`).
- Default path (env unset) unchanged → all existing apply-path / lincheck / crash tests must
  stay green without setting the env.

## Testing + measurement

- **Correctness unit tests** (`uc_protocol` SPSC): (a) a small finite budget still reads records
  and parks/returns correctly; (b) busy mode reads records and honors a stop signal without
  hanging.
- **Direction microbench** (local, sandbox, `uc_protocol/examples/`): consume latency over a
  **real `SpscRing`** in park mode vs busy mode — validates the ~8.8 µs → ~tens-of-ns drop on the
  actual code path.
- **Explicitly fleet-only (not claimed locally):** the ~10k/s throughput-ceiling win. The local
  bench shows the per-wakeup latency direction, not the ceiling.

## Success criteria

1. `UC_APPLY_SPIN_BUDGET` unset → behavior + tests identical to today.
2. `busy` mode: apply consumer never parks; correctness tests green; clean shutdown.
3. Microbench shows busy-mode consume latency materially below park-mode on the real ring.
4. `cargo clippy --workspace -D warnings` clean.

## Rollback

Env-gated, default-off. Remove the env var (or unset it) → original behavior.
