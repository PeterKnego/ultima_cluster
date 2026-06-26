# O1 extension: busy-spin the node-side ring bridges — design

**Date:** 2026-06-21
**Type:** prototype extension (perf spike, env-gated, default-off)
**Origin:** opportunity **O1** in `docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md`;
extends the apply-consumer prototype (`docs/superpowers/specs/2026-06-21-o1-busyspin-apply-consumer-design.md`)
to the node-side hops T-1 (submit) and T-4 (apply_resp).

## Motivation

The apply-consumer prototype busy-spun the single hottest intra-host hop (service apply,
#5/#6). The remaining intra-host hops UC owns are the node-side `NotifyBridge` parker threads:
**submit** (client→node ingress, T-1) and **apply_resp** (service→node response, T-4). Each
parker `FUTEX_WAIT`s on its ring (~8.8 µs/wakeup) before firing a tokio `Notify` to its async
consumer. Busy-spinning the parker removes that futex park.

## Scope

- **In:** the **submit** and **apply_resp** bridge parkers.
- **Out:** the **snapshot_resp** bridge (not on the steady-state commit hot path) — always parks.
- **Out (architectural follow-up):** eliminating the parker + `Notify` entirely by polling the
  ring in the async consumer (removes the residual reschedule wakeup too). See the honesty caveat.

## Mechanism — spin budget on the parker (approach A)

`NotifyBridge` gains a **spin budget** (same vocabulary as the apply consumer's
`UC_APPLY_SPIN_BUDGET`). The parker loop watches the ring's wakeup word (`current_seq`):

- **`0` (default, env unset)** → park immediately = **today's exact behavior**.
- **`u32::MAX` (busy)** → never `FUTEX_WAIT`; spin on `current_seq`, fire `Notify` **only on a real
  change** (a spurious notify every spin would churn the async consumer into a try_read/await
  loop), checking the stop flag each spin.
- **finite `N`** → spin `N` times, then park (the adaptive form; falls out of the same loop).

Approach A is runtime-flavor-agnostic and contained: the parker is a dedicated OS thread, so
busy-spinning it never stalls a tokio worker (the reason the parker exists). Approach B (poll the
ring in the async task) was rejected — on a `current_thread` runtime it starves all other tasks,
including the commit tasks the submit loop spawns; flavor-dependent and architectural-tier.

## The honesty caveat (carried into spec + bench labels)

Unlike the apply consumer (a sync thread, where busy-spin removed the *only* wakeup — measured
~32 µs in the spaced regime), the node consumers are **async tasks** woken via a tokio `Notify`.
Busy-spinning the parker removes the **ring futex park (~8.8 µs)** but the **`Notify`→tokio
runtime-unpark wakeup remains**. So the node-side per-hop win is **partial** (~µs, not the full
~32 µs). Eliminating the residual wakeup needs the architectural follow-up (out of scope). This
must not be overclaimed in the bench output or spec.

## Components / files

1. **`uc_node/src/ipc/ring_bridge.rs`** — `NotifyBridge::spawn(handle, name, spin_budget: u32)`.
   The parker loop spins-then-parks per the budget; notify-on-real-`current_seq`-change in the spin
   phase; existing arm/park/disarm + force-wake shutdown preserved for the park phase.
2. **Call sites:**
   - `uc_node/src/ipc/client_dispatcher.rs` — submit bridge: pass the env-derived budget.
   - `uc_node/src/raft/state_machine_shmem.rs` — apply_resp bridge: pass the env-derived budget;
     snapshot_resp bridge: pass `0`.
   - A pure parser `parse_bridge_spin_budget(Option<&str>) -> u32` + `bridge_spin_budget()` env
     reader for `UC_NODE_BRIDGE_SPIN_BUDGET` (unset→`0`; `busy`/`max` case-insensitive→`u32::MAX`;
     `<N>`→N; unparseable→`0`), placed where both call sites can reach it (e.g. in `ring_bridge.rs`).
3. **Tests/bench:**
   - Correctness: a busy-mode bridge fires `Notify` on a publish and shuts down cleanly (no hang,
     stop flag honored without a wakeup that never comes).
   - Direction bench (`uc_node/examples/`): full publish→`notified()` latency, park vs busy, over a
     real `NotifyBridge` on a tokio runtime — the label states it includes the residual
     `Notify`→reschedule, so the busy win is the futex-park removal, not a full wakeup elimination.

## Correctness

- Busy mode never parks → the arm/recheck/futex machinery is engaged only on the finite/0 (park)
  path, unchanged. Notify-on-change only → no spurious async churn.
- Shutdown: `shutdown()` sets the stop flag, force-wakes the parker (`waker.wake()`), and notifies.
  In busy mode the parker checks the stop flag every spin, so it exits promptly; the force-wake is
  a harmless no-op (nothing parked). Default (budget 0) shutdown path byte-for-byte unchanged.
- Default behavior (env unset → budget 0 for submit/apply_resp; 0 for snapshot_resp) is
  byte-for-byte the current parker behavior. All existing node tests pass without the env.

## Success criteria

1. `UC_NODE_BRIDGE_SPIN_BUDGET` unset → behavior + tests identical to today (the parker parks
   immediately, as now).
2. `busy` mode: submit + apply_resp parkers never `FUTEX_WAIT`; notify on publish; clean shutdown;
   snapshot_resp still parks.
3. Bench shows busy-mode publish→notified latency below park-mode (by ~the futex-park cost), with
   the residual-`Notify` caveat stated.
4. `cargo clippy --workspace -- -D warnings` clean; existing `uc_node` tests green.

## Rollback

Env-gated, default-off. Unset `UC_NODE_BRIDGE_SPIN_BUDGET` → original behavior.
