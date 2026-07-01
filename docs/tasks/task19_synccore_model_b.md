# Task 19 — SyncCore (Model-B): synchronous, disruptor-native consensus

**Status:** Implementation complete + measured to a verdict (2026-06-30). Awaiting a
keep / merge / shelve disposition decision (see §8).
**Where the code lives:** the openraft fork `PeterKnego/openraft`, branch `sync-core`, behind
the `sync-core` Cargo feature (default OFF). UC consumes it via the `../openraft` path-dep;
`uc_node`/`uc_autobench` expose a `sync-core` feature passthrough. Branch range
`041c427a..a6a590ec` (+ the earlier 3a/3b commits), pushed to `fork/sync-core`.

This doc is the canonical record. The design/plan scaffolding under
`docs/superpowers/specs|plans/` (the `2026-06-2x-synccore-*` files) is retained history; the
benchmark evidence is under `docs/benchmarks/synccore-*` and the throughput series below.

---

## 1. Why — the thesis and the pivot

Follow-on to the aeron-vs-UC investigation (task13) and the floor decomposition
(`docs/benchmarks/floor-decomposition-2026-06-25.md`): the ~1–2 ms commit floor is ~73%
software/structural (openraft async commit→apply choreography + 3-proc IPC + replication
pipeline) and only ~27% physical (fsync + wire). The dominant cost is **scheduling** — many
tokio task hops, each a futex park (~8.8 µs) when the consumer is parked.

Two models were considered for removing that:
- **Model A — async on a busy-spin runtime** (a custom never-park `AsyncRuntime`). Built and
  proven to work (single + multi-node via a hybrid reactor), but **declared NO-GO**: even fully
  busy-spun, the async path floors at tens of µs per hop (~1000× a disruptor ring's ~32 ns).
  Worth it only as the measurement proving the async ceiling.
- **Model B — disruptor-native** (THIS task): drive openraft's pure-synchronous `Engine`
  (feed events → drain the 17 `Command` variants → execute) from a synchronous loop on a
  pinned/busy-spin thread, with I/O isolated to reactor-free ring-fed consumer threads. The
  `Engine` is `pub(crate)`, has no async in its logic, and exposes a clean
  `handle_*` (input) / `pop_command` (output) API; `RaftCore` is just the async harness around
  it that Model B replaces.

**Key enabler (the reason this was tractable):** build `SyncCore` *behind the unchanged `Raft`
handle* and reuse openraft's own ~180-test integration suite + UC's lincheck/partition suites
as the executable spec — every step is correctness-gated against the same oracle, and the
feature-off path stays byte-for-byte identical.

---

## 2. What was built (the phase ladder)

All behind `#[cfg(feature="sync-core")]`; default RaftCore path unchanged at every step.

- **3a — own the loop.** `SyncCore` (in `openraft/src/core/`) wraps `RaftCore` and owns the
  entire event loop + command-drain orchestration (`do_main`/`runtime_loop`/`process_raft_msg`/
  `process_notification`/`run_engine_commands`), delegating only per-message execution.
- **3b.1 — own `run_command`.** `SyncCore` runs the 9 storage/apply/pure-sync command arms
  inline; the 8 task-spawning arms still delegated to RaftCore.
- **3b.2 — durability off the consensus thread.** Log I/O runs on a **reactor-free disruptor
  consumer** (`sync_durability.rs`): a write ring + a reader-vending request channel
  (`GatedLogReader` + a readability watermark), so the consensus thread never blocks on log I/O.
  The reactor-free `block_on` (noop-waker poll loop) drives async storage traits to completion
  via sync syscalls — "reactor-free sync" = how we drive + what the impls do, not removing
  `async` from signatures.
- **3d — synchronous consensus loop + completion-as-notification.** The loop became a
  synchronous `std::thread` (try_recv inputs + reactor-free completion handling), off the tokio
  scheduler. The **hybrid reactor** (the thread enters the app's ambient tokio runtime via
  `rt_handle.enter()` but never parks) lets delegated replication's quinn I/O find a driver. The
  redesign made append/save_committed fire-and-forget with flush flowing back as a `LocalIO`
  notification (so the loop does useful work instead of busy-waiting on I/O).
- **3c.1 — replication severed from RaftCore.** Per-peer **busy-spin "network consumer"
  threads** (`sync_network.rs`, `PeerExecutor`) port `ReplicationCore`'s append/heartbeat/
  snapshot/vote paths, driving `RaftNetworkV2` reactor-free, reproducing openraft's exact
  `Notification` ack contract. The 8 task-spawning `run_command` arms are relocated; the
  delegation block is removed (the match is exhaustive over all 17 `Command` variants); dead
  RaftCore twins are `cfg_attr`-annotated, not deleted. Vote/transfer-leader fan out off-loop
  via `C::spawn` (rare election path).
- **3c.2 — disruptor input ring.** Hot sync notifications (per-peer acks + durability io-done)
  ride one `build_multi_producer` MPSC ring, drained by the consensus loop via `EventPoller`;
  the `io_completion_forwarder` tokio task is removed from the sync-core path (cfg-gated, since
  it's shared with default RaftCore). Async producers (vote responses, tick, SM worker) stay on
  the tokio notification channel — full single-ring unification is gated on a future 3e (UDP
  reactor-free inbound).

Each phase was executed subagent-driven (fresh implementer + two-stage review per task) and
passed a final whole-branch opus review.

---

## 3. Architecture (end state)

```
clients / QUIC server (async) ──tokio mpsc rx_api──┐
tick + sm-worker apply (async) ──tokio mpsc────────┤  (try_recv each loop)
                                                    ▼
                  ┌──── CONSENSUS CORE (SyncCore, synchronous, busy-spin std::thread) ────┐
                  │ loop { poll input ring; try_recv rx_api/rx_notif; engine.handle_*;     │
                  │        for c in pop_command: dispatch }                                 │
                  └──┬──────────────┬─────────────────────▲──────────────────────────────┘
       durability ring│  per-peer    │ send rings           │ INPUT RING (disruptor MPSC)
                      ▼  send rings   ▼                      │ {network acks | io-done}
              DURABILITY consumer   per-peer NETWORK consumers (busy-spin, hybrid-reactor quinn)
              (reactor-free)          (reactor-free PeerExecutor)
                      └── io-done ────┴── acks/results ─────▶ INPUT RING
```

- **Consensus core**: synchronous, never parks; drives the pure-sync `Engine`.
- **Reactor-free I/O consumers**: durability (log) + per-peer network, each a busy-spin thread
  using `block_on` over the async storage/network traits (no tokio reactor in the hot path).
- **Hybrid reactor**: busy-spin threads enter the app's tokio runtime so quinn + timers work,
  while never yielding to the executor.
- **Disruptor rings** (disruptor-rs 4.3) for all hot hand-offs; chosen over hand-rolled rings
  per the `hi-perf-cmp` study (~17–33% faster, gap widens under burst).

---

## 4. Correctness (the guard, green throughout)

- openraft integration `cargo test -p tests --features sync-core` = **180/0** at every step
  (replication / membership / snapshot / election are the real oracle for the ported executor +
  ack contract) + lib 5xx/0; default RaftCore lib 494/0; clippy clean both feature states.
- **UC linearizability**: `uc_node` lincheck `lin_register` **3/3** (Linearizable, incl.
  `linearizable_under_failover`) and partition `lin_partition` **4/4** (behind
  `fault-injection`) on the final 3c branch — the reimplemented replication preserves
  linearizability under churn + network faults.
- **Feature-off byte-for-byte**: all sync-core code is `#[cfg]`-gated; shared edits (e.g. the
  io-completion watch/forwarder) are paired `cfg(not(sync-core))`, verified.

---

## 5. Performance — the verdict

Two regimes, opposite answers (the central finding):

- **Latency (inflight-1), POSITIVE.** Controlled microbench + a multi-node injected-latency
  run: SyncCore beats RaftCore by **+35–45%** once the commit path has any realistic per-op
  blocking I/O, because the never-park re-poll removes the futex choreography on the serial
  critical path. (`docs/benchmarks/synccore-3c-commit-latency-2026-06-30.md`,
  `synccore-latency-injected-2026-06-29.md`, `synccore-3d-*`.)
- **Throughput (the decision metric), NULL.** Denoised 3-node fleet A/B (real QUIC + fsync,
  dedicated cores): **knee identical at 5,000 msg/s for both arms, all 6 reps**; p99@knee ~10%
  worse under SyncCore. (`docs/benchmarks/synccore-3c-fleet-throughput-2026-06-30.md`.) Under
  load, batching/pipelining amortizes the per-commit choreography that busy-spin removes, so it
  is **not the throughput bottleneck**. Confirms the floor-decomposition + aeron conclusion.

**Why the split:** inflight-1 is the serial critical path where futex hops are the whole cost;
throughput is gated by the slowest *bandwidth* resource (group-commit/linger + cross-host
replication), which Model B does not touch.

---

## 6. The throughput attribution this spawned (and the win it produced)

Because SyncCore was throughput-null, the throughput ceiling was attributed directly
(`docs/benchmarks/throughput-knee-attribution-2026-06-30.md`, same-instance bisection):

- **5k knee = linger (2×) × replication (2×)**, compounding. **NOT** fsync (turning it off
  didn't move the knee — group-commit amortizes it), **NOT** consensus (this A/B), **NOT**
  pipeline depth (`pipeline-depth-sweep-2026-06-30.md`: depth 8→64 all pinned at 10k).
- **First 2× — LANDED**: `api_batch_linger_ms` default **5 → 2 ms** (`uc_node` `f262c2f`). A
  Pareto win — **2× the throughput knee (5k→10k)** *and* lower latency at every load — because
  linger only buys fsync batching and fsync isn't the bottleneck.
  (`linger-pipeline-frontier-2026-06-30.md`.)
- **Second 2× (10k→~20k):** *initially* attributed to structural cross-host replication
  latency — **but see the correction below; this was substantially a measurement artifact.**

**Strategic note (as first written):** the aeron-investigation throughput target was ~10k/s;
the linger change moved the 3-node knee to 10k, so the target looked **met**.

### 6a. CORRECTION (2026-06-30, leader profiling + inflight sweep)

The "structural 10k ceiling" was re-examined by profiling the leader at the knee and sweeping
the load generator's `inflight`. Doc: `docs/benchmarks/leader-profile-inflight-2026-06-30.md`.

- **The leader is ~90% idle at the 10k knee** — no thread pegged (busiest ~30% of a core),
  box `%Cpu(s) ~93–96% idle` while sustaining ~9.8k commits/s. So 10k is **not** a saturated
  resource and **not** CPU/Engine-bound (killing the "single-threaded Engine" hypothesis).
- **The knee rises with `inflight`**: at the *same* 10k load, raising `inflight` 128→256
  *lowered* p50 from 26.5 ms to 5.8 ms (so 128 was the binding limit, not the server), and the
  sustainable knee moved from ~10k to ~14.6k (p50 187 ms). `throughput = inflight ÷ latency`,
  and **we had held `inflight=128` (the bench default) through every measurement this session.**
- **⇒ the 10k "ceiling" was concurrency-starvation**, not structural. The system was being
  under-driven across a ms-latency commit path. The "throughput target met at 10k" and
  "replication 2× is structural" claims above are therefore **softened**: more throughput
  (≥15k shown) is available by driving more concurrency, at a tail-latency cost; the true
  ceiling is **unknown** (inflight ≥512 destabilized the runs and is unmeasured).
- **The real ceiling IS a named hot path (chase, re-profile at inflight=256/14k):** the knee
  plateaus at **~15k** (inflight 256→384 no gain), where **one thread saturates** (86% and
  climbing while the box stays ~90% idle). Its perf call-graph: **21.6% `read()` of the journal's
  ext4 segment files** (+ page-copy + ~15% faults) and **22% crc32**. ⇒ the leader's
  **single-threaded log-read path for replication/apply** — it re-reads just-appended entries
  back from disk and recomputes CRC to build AppendEntries — saturates one core. This
  **supersedes the "structural replication wall."** Concrete UC-side fixes (NOT structural, NOT
  openraft-core): (1) in-memory recent-entry cache (serve replication reads from RAM, not ext4
  — kills ~27%), (2) skip CRC re-validation on own entries (~22%), (3) zero-copy entry bytes.
  Eliminating ~40–50% of that thread's CPU should push the ceiling well past 15k. Caveat:
  inflight ≥512 destabilizes (separate, unmeasured). Doc:
  `docs/benchmarks/leader-profile-inflight-2026-06-30.md` §5.

### 6b. CORRECTION #2 (2026-07-01, log-entry-cache fleet A/B is NULL)

The log entry cache (built to serve those reads from RAM) was measured on the fleet: **NULL —
knee 15k with cache ON and OFF, `read()`+`crc32` unchanged.** Root cause: the bench runs **shmem
mode** and the hot journal reads are the **UC apply/output/replay path reading the journal
DIRECTLY** (`state_machine_shmem.rs:1153 journal.iter_range`, `output_replay.rs:132`), which
BYPASS the cache (it only intercepts openraft `try_get_log_entries`). Driver: **599k `output_chan
full` warnings** — the service can't keep up at 15k, so the node constantly replays committed
entries from the journal into the service. **So §6a's "leader re-reads log FOR REPLICATION" was
wrong for shmem mode — it's the apply/output pipeline (service falling behind).** The cache is
correct + merge-ready but ineffective for shmem; the real throughput bottleneck is the shmem
apply/output pipeline (re-diagnose the service apply rate + the `output_chan`/output_dispatcher).
Doc: `docs/benchmarks/log-cache-win-null-2026-07-01.md`.

---

## 7. Known limitations / deferred (all documented)

- **Detach-on-drop** of peer consumers (correct fix = cancellable backoff + join); threads exit
  within one bounded drive, no accumulation (final-review verified).
- **`block_on_yielding` micro-sleep** (50 µs off-CPU on Pending) — a CPU-for-latency trade for
  3c.1; perf phase.
- **Per-append `MultiProducer::clone` mutex** on the io-done hot path (3c.2 review) — moot for
  throughput (ceiling isn't ring-bound), but the first thing to revisit if ever measured.
- **Per-peer thread per follower** (single-multiplexed is a 3e/UDP optimization).
- **3e not built**: UDP reactor-free inbound → single unified input ring + apply hop.
- A couple of missing unit tests (PeerExecutor inflight lifecycle, snapshot epoch cancel) —
  integ-covered.

---

## 8. Disposition decision (to be made)

SyncCore is correct, opus-reviewed, merge-ready — and **latency-positive but throughput-null**.
The throughput case for shipping it is not made; the latency case is real for
low-concurrency / latency-SLA workloads. Options:

- **(A) Keep as a default-off latency mode.** Merge `sync-core` (feature stays OFF by default)
  as an opt-in for latency-sensitive deployments; maintain it alongside RaftCore. Cost: ongoing
  fork-divergence maintenance (it tracks an openraft alpha + reimplements replication).
- **(B) Shelve.** Leave `sync-core` on the fork as a proven reference + this task doc; do not
  merge or maintain. Lowest cost; the linger win (the actual payoff) already shipped
  independently of SyncCore.
- **(C) Merge as default.** Not justified — throughput-null + slightly worse tail under load +
  higher maintenance; only sensible if low-concurrency latency becomes the primary product goal.

**Recommendation: (B) shelve**, unless low-concurrency latency is a committed product goal — in
which case (A). The session's durable value (linger 2×, bench-infra hardening, the complete
throughput attribution) is already merged on `main`-track independent of this decision.

---

## References

- Code: `fork/sync-core` `openraft/src/core/{sync_core,sync_durability,sync_network,sync_input}.rs`;
  UC passthrough `uc_node`/`uc_autobench` `sync-core` features.
- Design history: `docs/superpowers/specs/2026-06-2{7,8,9}-synccore-*`,
  `docs/superpowers/plans/2026-06-2{9,30}-synccore-3c*`.
- Evidence: `docs/benchmarks/synccore-*`, `throughput-knee-attribution-*`,
  `linger-pipeline-frontier-*`, `pipeline-depth-sweep-*`, `floor-decomposition-2026-06-25.md`.
- SDD ledger: `openraft/.superpowers/sdd/progress.md`.


### 6c. SyncCore A/B on top of the purge fix (2026-07-01) — THROUGHPUT NULL, confirms ceiling is pipeline-bound

After root-causing + fixing the real ~15k throughput ceiling (`Journal::purge_before` full-segment scan; `ultima_cluster` commit aa031e8, ceiling -> ~24-27k), re-ran the SyncCore-vs-RaftCore A/B **on top of the purge fix** (both arms carry it; openraft branch sync-core@a6a590ec, built via `-e uc_sync_core=true`; sync-core+purge-fix lin_register 3/3 green). **Throughput NULL at the ceiling** (RaftCore 24.2k vs SyncCore 24.9k @25k offered / inflight-256), because the ceiling is now the **apply/output/journal pipeline** (congestion collapse past ~256 concurrency) — orthogonal to the consensus choreography SyncCore optimizes. SyncCore edges that showed: **p99 2288ms vs 3708ms** (its latency signature) and NO congestion-collapse at the 30k rung where RaftCore collapsed to 2.6k (single noisy point, needs 3-5 repeats). Net: **the purge fix, not SyncCore, was the throughput win** — reinforces the shelve/decide-later disposition (SyncCore = latency play, not throughput). Doc: `docs/benchmarks/throughput-ceiling-root-cause-2026-07-01.md`.