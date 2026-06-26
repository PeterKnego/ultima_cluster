# Task 18 — Busy-spin intra-host ring consumers (O1 prototypes)

**Date:** 2026-06-21.
**Status:** Two prototypes built, reviewed, env-gated default-off. NOT merged to main (kept on
branch `prototype/o1-busyspin-apply-consumer`, 13 commits). Local microbenches confirm the
mechanism + regime; the headline throughput-ceiling payoff was measured on an AWS NVMe fleet
(2026-06-21) and is **NULL end-to-end** (§4a). A follow-on floor decomposition + replication probe
(§7, 2026-06-25) explains why and closes the whole µs-optimization thread: the ~1–2 ms commit floor is
**~73% structural** (openraft async + 3-proc IPC), so there is no cheap win — keep default-off.
**Branch:** `prototype/o1-busyspin-apply-consumer`.

**Provenance / scaffolding.** Originates from opportunity **O1** in the Aeron-vs-UC threading/copying
investigation (`docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md`). Design/plan artifacts
are retained under `docs/superpowers/`:
`specs/2026-06-21-o1-busyspin-apply-consumer-design.md` + `plans/2026-06-21-o1-busyspin-apply-consumer.md`
(apply consumer), and `specs/2026-06-21-o1-node-bridge-busyspin-design.md` +
`plans/2026-06-21-o1-node-bridge-busyspin.md` (node bridges). This doc is the canonical record and
stands on its own.

---

## 1. Motivation

The Aeron-vs-UC investigation (Task-adjacent, see the benchmark doc above) established that UC's gap
to Aeron on the commit hot path is a **threading/wakeup** story, not a copying one: a futex park/wake
costs **~8.8 µs** vs **~29 ns** for a busy-spin (~300×), while a payload copy is single-digit-to-~40 ns
(~200–2000× smaller than one wakeup). Aeron Cluster runs consensus + service as **polling agents in one
process** (0 intra-host parks); UC splits node/service into separate processes bridged by **futex rings**
and routes consensus through openraft's async model, so each intra-host stage transition pays a futex
park + reschedule.

**O1** = busy-spin the **intra-host ring consumers UC owns**, the intra-host analog of Aeron's polling
consumers. This is explicitly distinct from the settled-negative **cross-host** busy-poll (task17
Phase B: "network was never the bottleneck — fsync/IPC dwarf RTT"). The honest target is the **~10k/s
throughput ceiling** and the per-commit pipeline cost beneath the 5 ms `api_batch_linger` — NOT the
linger-bound headline p50 (at linger=5ms, eliminating every intra-host wakeup moves p50 <1%).

Two prototypes were built, covering the intra-host consumers on the commit path:
1. the **service apply consumer** (the single hottest hop, #5/#6);
2. the **node-side `submit` + `apply_resp` bridges** (T-1 ingress, T-4 response).

---

## 2. Shared mechanism — a configurable spin budget

Both prototypes share one idea: **make the spin-before-park window configurable**, with a sentinel
`u32::MAX` for pure busy-spin (never enter `FUTEX_WAIT`). Both are **env-gated, default-off, and
byte-for-byte unchanged when the env var is unset.** Busy-spin trades a burned core for removed wakeup
latency — viable only on a bounded number of hot hops (Aeron's low-latency profile burns 3 dedicated
cores).

### 2.1 Service apply consumer (`uc_protocol` + `uc_service`)

The apply consumer is a **sync `std::thread`** calling `SpscConsumer::read_or_park` — already
spin-then-park (`SPIN_TRIES = 64`, then `FUTEX_WAIT` up to `PARK_CEIL = 2 ms`).

- `uc_protocol/src/ring/spsc.rs`: `SpscConsumer` gains a private `spin_budget: u32` (default
  `SPIN_TRIES`) + `set_spin_budget(&mut self, u32)`. `read_or_park` uses it; on the `u32::MAX`
  sentinel it polls in `BUSY_SPIN_CHUNK = 256`-sized bursts and returns `Ok(None)` between bursts
  (never parks) so the caller re-checks its stop flag — keeping shutdown prompt with no `FUTEX_WAIT`.
  `uc_protocol` stays env-free (wire layer); it only exposes the knob.
- `uc_service/src/runtime/apply_loop.rs`: reads `UC_APPLY_SPIN_BUDGET` via a pure
  `parse_spin_budget(Option<&str>) -> u32` (unset → `SPIN_TRIES`; `busy`/`max` case-insensitive →
  `u32::MAX`; `<N>` → N; unparseable → `SPIN_TRIES`) and calls `set_spin_budget` once before the loop.

### 2.2 Node-side bridges (`uc_node`)

The node consumers are **async tasks** woken via a tokio `Notify`: a dedicated `NotifyBridge` parker
OS-thread `FUTEX_WAIT`s on the ring and fires `Notify`; the async consumer does `try_read`; on `None`
it `bridge.notified().await`.

- `uc_node/src/ipc/ring_bridge.rs`: `NotifyBridge::spawn(handle, name, spin_budget: u32)`. Parker loop:
  `0` → park immediately (today's behavior); `u32::MAX` → busy-spin on `current_seq`, fire `Notify`
  **only on a real change** (a notify-per-spin would churn the async consumer), checking the stop flag
  each spin; finite `N` → spin N then park. **Busy mode must NOT `arm()` the ring** (see §3).
  `parse_bridge_spin_budget`/`bridge_spin_budget` read `UC_NODE_BRIDGE_SPIN_BUDGET` (unset → `0`;
  `busy`/`max` → `u32::MAX`; `<N>` → N; unparseable → `0`).
- Call sites: `client_dispatcher.rs` (submit) and `state_machine_shmem.rs` (apply_resp) pass the env
  budget; **snapshot_resp** passes `0` (not on the steady-state hot path).

**Why the two defaults differ** (`UC_APPLY_SPIN_BUDGET` defaults to 64, `UC_NODE_BRIDGE_SPIN_BUDGET`
to 0): each value reproduces *today's* behavior for that component — the apply consumer already
spin-then-parks (64), the bridge parker has no spin phase today (0). Approach B (eliminate the parker,
poll in the async task) was rejected: on a `current_thread` runtime it starves all other tasks,
including the commit tasks the submit loop spawns — flavor-dependent and architectural-tier.

---

## 3. Correctness

- **Default path byte-for-byte unchanged.** Apply: field defaults to `SPIN_TRIES`, the finite/park
  branch is the original arm→recheck→futex-park→disarm. Bridge: budget `0` → zero-iteration spin →
  the original arm/park/disarm/notify. Env unset → defaults. All existing tests pass without the env.
- **Busy-mode shutdown stays prompt.** Apply: `read_or_park` returns `Ok(None)` every `BUSY_SPIN_CHUNK`
  so the loop re-checks `stop`. Bridge: the busy parker checks `stop` each spin. Neither waits on a
  wakeup that never comes.
- **No lost wakeup (bridge busy mode).** The parker tracks `last` vs `current_seq` (= the producer's
  `publish_position`, which the consumer never mutates); the consumer drains to empty before each
  `notified().await`, and tokio `Notify` stores one permit. Any publish the consumer misses before
  awaiting still differs from `last` when the parker next reads it → a notify follows.
- **The bug the final review caught (fixed `a7d76be`).** The first node-bridge implementation had the
  busy parker call `handle.arm()` at startup and only `disarm()` at shutdown. `arm()` leaves the ring's
  `waiters > 0`, and the SPSC producer's `signal()` only *skips* its `FUTEX_WAKE` syscall when
  `waiters == 0` — so a busy bridge made the **producer fire a useless `FUTEX_WAKE` on every publish**,
  relocating the futex cost to the producer hot path and partly defeating the optimization. Fix: gate
  `arm()`/`disarm()` on `spin_budget != u32::MAX` (busy mode leaves the ring un-armed — it never parks,
  so it needs no waiter registration). The apply half never had this (its busy branch returns before
  `arm()`).

---

## 4. Measured results (sandbox, 4 vCPU — ratios are the signal, absolutes are inflated)

All from dependency-free benches (criterion/`atomic-wait` are not vendored; the sandbox is offline).

**Per-wakeup mechanism** (`uc_protocol/examples/handoff_wakeup_bench.rs`, min-of-5): futex park/wake
**~8.8 µs/wakeup** vs busy-spin **~29 ns** (~300×). Reconciles with the storage handoff doc's ~22–32 µs
two-wakeup-per-commit handoff.

**Apply consumer** (`uc_protocol/examples/apply_spin_consume_bench.rs`, real `SpscRing`, two regimes):
- **saturated** (hot producer): park ≈ busy, **delta near-zero and sign-varying** — busy-spin can
  *regress* when hot, because `SPIN_TRIES=64` already dodges the futex and busy mode's 256-spin chunk
  costs more than park's short spin.
- **spaced** (~200 µs gap → consumer parks): **park ~33 µs/rt vs busy ~0.8 µs/rt (~32 µs win)** — busy
  skips the futex wakeup + the reschedule of a fully-descheduled sync thread.

**Node bridge** (`uc_node/src/ipc/ring_bridge.rs` `measure_publish_to_notified_park_vs_busy`, a
print-based `#[tokio::test]`): publish→notified **park ~19.5 µs vs busy ~5.7 µs (~3.4×)**. The node-side
win is **partial**: busy removes the ~14 µs ring futex park, but busy still pays ~5.7 µs because the
parker→async **`Notify`→tokio-reschedule wakeup remains** (unlike the apply consumer's sync thread,
~0.8 µs). Removing that residual needs the architectural follow-up (poll in the async task / co-locate
node+service).

**Takeaway:** busy-spin is a **low-rate-latency / throughput-ceiling** lever, NOT a free win — it helps
only when consumers actually park (the shallow-pipeline / low-rate regime), and can regress under
saturated load. The ~10k/s throughput-ceiling payoff is **fleet-only and unmeasured** here.

---

## 4a. Fleet A/B result — NULL end-to-end (AWS, measured 2026-06-21)

The "fleet-only and unmeasured" payoff above was **measured** on AWS and is **null at the e2e cluster
level**. Setup: 3× `c6id.4xlarge` (16 vCPU, local **NVMe** instance store mounted at the journal
`--data-dir`), single-AZ cluster placement group, us-east-1, QUIC, `durability=consistent`,
`UC_API_BATCH_LINGER_MS=0` (linger=0 chosen deliberately — §1: at linger=5 the effect is <1%, so
linger=0 is the only setting that could expose busy-spin). **A arm** = defaults (busy-spin off);
**B arm** = `UC_APPLY_SPIN_BUDGET=busy` + `UC_NODE_BRIDGE_SPIN_BUDGET=busy`. Built from this branch via
`bench-infra` rsync mode; env vars threaded through the `run` role (`bench-infra/ansible/roles/run/tasks/main.yml`
+ `group_vars/all.yml`). Driver: `commit-path-load`, payload 64 B, inflight 128.

**Result.** Across the sustainable ladder both arms are **indistinguishable**, and run-to-run variance
**dwarfs** any A/B delta:
- **≤10 k/s** (cluster not saturated): both arms achieve target, p50 sub-ms to single-digit-ms; A≈B.
- **~12 k/s** (onset of congestion collapse): both hit ~12 k achieved, but p50 for the *same* arm-A
  config swung **1.31 ms ↔ 69.5 ms** between two reps — pure noise, no arm ordering.
- **~14 k/s** (collapse): achieved bounces 10.4 k–14 k with **no consistent arm preference** (B faster
  one rep, A faster the next); p50 in the 100 ms–1 s range.
- A first coarse run's apparent "+5.8 % ceiling" for B (17.7 k vs 18.7 k achieved @ 20 k target) fell
  **inside** the ±13 % same-config run-to-run band (a repeat of arm A alone gave 15.6 k vs 17.7 k @ 20 k)
  — i.e. not a real effect. Pushing the ladder to 24 k–28 k drove congestion collapse that crashed the
  load driver, confirming the real sustainable ceiling is ~12 k/s, set by the commit floor, not by
  intra-host consumer wakeups.

**Why null (expected).** The e2e per-commit floor is **~0.85–1.4 ms** (Raft replication RTT to a
majority + NVMe journal fsync + openraft async scheduling). Busy-spin removes the intra-host ring
wakeups — ~32 µs on the apply consumer (full win) + a *partial* win on the node bridges (the
parker→async `Notify`→tokio reschedule residual remains, §4) — call it **~40 µs/commit**, which is
**~3–5 % of a millisecond-scale commit** and invisible under the noise. This **reinforces the same
pattern** as task17 Phase B (cross-host busy-poll → null: "network was never the bottleneck") and the
journal prealloc/fdatasync A/Bs (null e2e): the cluster commit path masks µs-scale intra-host/transport
micro-optimizations under linger + replication + fsync. **Bottom line: keep busy-spin env-gated and
default-off; it is not worth enabling for cluster throughput/latency.** A real win would require
attacking the millisecond floor itself (the residual-`Notify` removal / node+service co-location in §6,
plus replication RTT) — not the intra-host wakeups in isolation.

Reproduce: `cd bench-infra && make up-uc`, then per arm
`cd ansible && ansible-playbook bench.yml -e aeron_enabled=false -e uc_api_batch_linger_ms=0 [-e uc_apply_spin_budget=busy -e uc_node_bridge_spin_budget=busy]`;
results land in `bench-out/dist/<ts>/node0/uc_sweep.csv`. `make destroy` when done.

---

## 5. Configuration & rollback

| env var | component | unset (default) | `busy`/`max` | `<N>` |
|---|---|---|---|---|
| `UC_APPLY_SPIN_BUDGET` | service apply consumer | `SPIN_TRIES` (64, spin-then-park) | `u32::MAX` (pure busy-spin) | spin N then park |
| `UC_NODE_BRIDGE_SPIN_BUDGET` | submit + apply_resp bridges | `0` (park immediately) | `u32::MAX` (busy parker) | spin N then park |

Both default-off; rollback = unset the env var. snapshot_resp always uses `0` regardless.

---

## 6. Status & follow-ups

- **Done + reviewed (final review READY TO MERGE after the `a7d76be` fix):** both prototypes, env-gated,
  default-off; `cargo clippy --workspace -- -D warnings` clean; `uc_protocol` 70 + `uc_service` 15 +
  `uc_node` ring_bridge 4 tests green.
- **O3 — adaptive spin→park** (`BackoffIdleStrategy`-style): the finite-`N` path already exists in both
  components as scaffolding; the remaining work is choosing N adaptively under load so cores aren't
  burned at idle.
- **Fleet run** against the ~10k/s throughput ceiling — **DONE 2026-06-21, result NULL** (see §4a):
  no measurable e2e throughput/latency win on a 3× `c6id` NVMe fleet at linger=0; the ~40 µs/commit
  intra-host saving is dwarfed by the ~1 ms Raft-commit floor. Keep default-off.
- **Architectural (residual-`Notify` removal)** on the node side — poll the ring in the async consumer
  or co-locate node+service so the node-side hop loses its second wakeup too. Rewrite-class; weigh only
  if a sub-ms latency floor becomes a product goal (and recall Raft replication RTT is the wall after
  that).

---

## 7. Closing verdict — the floor is structural; the µs hunt is over (2026-06-25)

O1's null fleet result (§4a) raised the obvious question: if removing intra-host wakeups does nothing,
**what is the ~1–2 ms commit floor actually made of?** That was settled by a follow-on decomposition
(full record: `docs/benchmarks/floor-decomposition-2026-06-25.md`), which closes not just O1 but the
whole µs-scale optimization thread (O1 busy-spin, task17 Phase B busy-poll, journal prealloc, fdatasync —
all NULL e2e for the same reason).

**Floor decomposition** (AWS 3× `c6id` NVMe, linger=0, inflight=1, layered e2e-delta over
{1,3}node × {eventual,consistent}; cross-check additive within 0.7%):

| bucket | p50 | % of the ~1.88 ms floor | nature |
|---|---:|---:|---|
| base — IPC rings + openraft commit→apply + apply | 0.86 ms | 46% | software |
| replication — leader→majority | 0.70 ms | 37% | mostly software |
| fsync — local NVMe journal | 0.33 ms | 18% | physical I/O |

Raw LAN RTT is only ~0.19 ms, so re-cut by *nature*: **~73% software/structural** (openraft async +
3-proc IPC + replication pipeline), **~27% physical** (fsync 18% + wire 10%).

**Replication sub-probe** (leader-side `append_entries` round-trip timing, this branch's
`uc-bench-probes` addition): the RPC is **p50 184 µs**, wire-dominated — UC's own network software is
~tens of µs. So of the 0.70 ms replication bucket, the RPC is ~0.18 ms (~26%) and **~0.52 ms (~74%) is
openraft async choreography** (RaftCore↔ReplicationCore↔apply task hops *outside* the RPC). A QUIC
stream-pooling / leaner-codec optimization would save microseconds — it is **not worth it**.

**The verdict:**
1. **Every µs-scale micro-opt was correctly null** — busy-spin (this task), busy-poll, journal prealloc,
   fdatasync all nibble the ~27% physical slice while ~73% of the floor is openraft-async + IPC structure.
2. **There is no cheap win left.** Not fsync (18%), not wire (10%), not UC's RPC code (~0.03 ms). The cost
   is the openraft duty-cycle and the 3-process split themselves.
3. **The only levers that move the floor** are (a) openraft-internal changes — fewer core↔replication↔apply
   async hops (a fork/upstream effort, not a UC config; `UC_PIPELINE_DEPTH` only hides RTT under load), or
   (b) the co-location / polling rewrite (§6, rewrite-class). Both are product-level decisions.
4. **Canonical design point:** UC's commit floor is **≈ 1–2 ms, ~73% structural**, and that is by design
   for a 3-process SMR server on openraft. The busy-spin prototypes stay **env-gated, default-off**; keep
   them as scaffolding for the architectural path, not as a shipped optimization.

This is the end of the latency-floor investigation that began with the Aeron-vs-UC parity work: the gap
is structural, it is now quantified, and chasing it further requires changing the structure.
