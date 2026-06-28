# SyncCore / Model-B — Phase 3 decisions & sequencing (checkpoint)

**Date:** 2026-06-28 · **Status:** living decisions record
**Repo of work:** openraft fork `PeterKnego/openraft` branch `sync-core` (consumed by `ultima_cluster` via path-dep)
**Related:** `2026-06-28-synccore-disruptor-pipeline-design.md` (architecture), `docs/openraft-busyspin-runtime-feasibility.md`, `docs/benchmarks/floor-decomposition-2026-06-25.md`, the SDD ledger at `../openraft/.superpowers/sdd/progress.md`.

## Where we are (done, suite-guarded 180/0 throughout, pushed)

- **3a** — SyncCore owns the event loop (`do_main`/`runtime_loop`) behind the unchanged `Raft` handle; reuses openraft's own 180-test suite as the spec. opus whole-branch review clean.
- **3b.1** — SyncCore owns `run_command`: the 9 storage/apply/pure-sync arms execute inline; the 8 task-spawning (replication) arms delegate to RaftCore.
- **3b.2** — **log I/O runs off the consensus thread on a reactor-free consumer** (disruptor write ring + reader-vending request channel; `Option<LS>` cfg-extraction; `get_log_reader` rerouted). All 5 write ops are **uniform-await** (the fire-and-forget Append race is fixed). Spike module deleted (Task 4). Feature-OFF path byte-for-byte unchanged.
- **3b.2 Tasks 2–3 DEFERRED** — Append fire-and-forget *with a readability gate*; SaveVote consumer-side. Their pipelining benefit is gated on the consensus loop going synchronous (3d), so they fold into 3d (see below).

## Locked decisions

1. **Model B over Model A.** Async-on-busy-spin (Model A, the `uc-rt-busyspin` runtime) is a **no-go for the consensus path**: even fully reactor-free it floors at ~tens of µs (boundary futex + per-future overhead), ~1000× a disruptor ring hop. Model B = drive openraft's pure-sync `Engine` from a synchronous core, reusing openraft's algorithm + test suite. (Model A is retained as the *measurement* that proved the ceiling.)
2. **Ring transport = `disruptor-rs` 4.3.** Evidence: the `hi-perf-cmp` study — disruptor beats a lean hand-rolled ring ~17–33% (gap widens with burst); both crush channels. Spike (3b.2) confirmed the API shape: disruptor requires `E: Send+Sync` and lends only `&event`, so move-only payloads ride a `Mutex<Option<_>>` slot (uncontended via the sequence barrier).
3. **Fully-synchronous, reactor-free I/O consumers.** No tokio reactor / futex scheduler in the hot path. **Keep openraft's async storage/network trait seam** (required for the 180-suite + UC adapter reuse) but **drive it reactor-free** with a no-runtime `block_on` (never-park poll loop) — the impls complete via sync syscalls, never awaiting a reactor. "Reactor-free sync" = how we drive + what the impls do, not removing `async` from the signatures.
4. **Network transport: UDP-first, benchmarked.** Raw UDP p50 ~34.5 µs vs QUIC ~70.6 µs (~2× on c6id, `hi-perf-cmp` journal 20260627T071950Z). BUT task16's reliable-UDP A/B found the *end-to-end* edge is network-dependent. So UDP is a **benchmarked backend swap inside the network consumer (Phase 3e), not a precondition**; quinn stays the fallback behind the same `RaftNetworkV2` seam.

## Sequencing decision (this checkpoint's main call): **3d before 3c**

The floor decomposition: commit floor ≈ **consensus base 46% + replication 37% + fsync 18%**.

- **Replication (3c) is NOT the most critical hot-path.** It's ~37% — a close second to the consensus base (~46%), not dominant.
- **We have measured nothing of the realized SyncCore win.** 3b.2 moved durability (the 18% fsync bucket) off-thread, but the *largest* bucket — the consensus commit→apply choreography — is uncut because the loop is still async.
- Therefore: **do the consensus-loop-synchronous step (3d) next.** It cuts the biggest bucket, is the lowest-risk completion of 3a/3b, is unambiguously disruptor-native (no compromise), and — decisively — **is what finally produces a measurement.** That measurement, not a prediction, decides whether 3c needs the cheap A or the expensive B.

### The 3c fork (A vs B) — DEFERRED until the 3d measurement

- **A = reuse openraft's replication tasks, run them reactor-free** (`block_on` on reactor-free threads instead of tokio; openraft's own channels stay; **no disruptor in replication internals** — they publish acks to the consensus ring at the seam). It's Model-A's technique scoped to replication; defensible because the ~180 µs wire RTT dwarfs the tens-of-µs async residual on *latency*. Cheap, low-risk, reuses the hardest/most-correctness-critical subsystem (inflight/batch/retry/snapshot).
- **B = reimplement replication as disruptor consumers** — the only fully disruptor-native option; the full *throughput* ceiling; but re-derives the riskiest subsystem (~60% new).
- **Decision: choose after 3d is measured.** If the sync consensus core lands and we're wire+fsync-bound with adequate throughput → **A**. If throughput is still software-limited and the replication async residual is binding → **B**. Picking B now = committing to reimplement the riskiest subsystem on a *prediction*. The replication subsystem is **fully decoupled from RaftCore** (channel-only contracts), which makes A genuinely feasible.

## 3d design (what the source map established)

- **Why the loop is async = structural, NOT QUIC.** `runtime_loop` is an async task that `select!`s over tokio channels (`rx_api`/`rx_notification`) and `.await`s I/O completions. QUIC lives only in the replication tasks (a separate layer) and feeds the loop via channels. So making the loop synchronous is **independent of the QUIC/replication question**.
- **The async↔sync boundary is transparent.** `Raft::client_write` sends a `RaftMsg` + awaits a oneshot responder; the loop calls `responder.on_complete()`. A oneshot doesn't care if the signaller is a tokio task or a pinned `std::thread`. So the `Raft` async API works unchanged — no API change, no producer rework needed for the minimal version.
- **Minimal 3d:** spawn SyncCore on `std::thread` (not `C::spawn`); replace the `select!` with `try_recv()` polling; keep tick / `sm::Worker` / notification producers as tokio tasks the loop polls. Off the tokio scheduler.
- **The complication (important):** a sync loop can't `.await`, so `run_command`'s durability/apply completion `await` becomes `block_on` — which **busy-spins the consensus thread during I/O**, re-serializing it (partly undoing 3b.2). The *real* base-bucket win (consensus thread *free* during I/O) needs completions to come back as **later inputs (notification/ring)** rather than inline waits — which means moving the per-op **after-work to completion-notification-triggered** (= the deferred 3b.2 Tasks 2–3 + the readability gate). **So a clean 3d and the deferred pipelining are one combined step.**
- **Apply hop deferred.** `sm::Worker` (async tokio task, calls the async `RaftStateMachine::apply`) stays as-is in minimal 3d (loop forwards `Command::Apply`, polls the result notification). Pulling apply inline needs a reactor-free SM apply (or `block_on` requiring the user SM be reactor-free) → Phase 3e.

## Measurement plan

- **Primary (cheap, isolates the win):** an openraft-layer commit→apply microbench — SyncCore-sync vs RaftCore-async, in-memory network, measuring the *software choreography* (the ~672 µs base) reduction. No UC integration needed.
- **Later (full picture):** wire UC to build openraft with `--features sync-core` and run UC's bench / fleet against the floor decomposition. **Prerequisite/risk:** UC's real adapters (ultima_journal, quinn) must complete reactor-free under the durability consumer's `block_on` (memstore does; UC's journal may use blocking/tokio I/O that needs verifying). Untested — flag before relying on a UC fleet number.

## Next concrete step (when resumed)

**Spike the minimal sync loop** (`std::thread` + `try_recv` inputs + `block_on` completions, suite green) — validates the transparent boundary and the sync-loop foundation cheaply, accepting the busy-spin-on-I/O limitation. Then the **completion-as-notification redesign** (folding in deferred 3b.2 Tasks 2–3) for the real win, then **measure**. Then return to the 3c A/B decision with data in hand.

## Open questions / risks to carry

- The completion-as-notification redesign (after-work triggered by IO-completion notification instead of inline) is the subtlety-dense part of a clean 3d — the suite's purge/snapshot/membership tests guard ordering.
- UC adapter reactor-free-ness (journal/quinn) — unverified; gates the UC fleet measurement.
- Two 3b.2 Minors banked: `spawn_replication_stream` reader-request uses `.expect()` (latent panic post-teardown); `await_completion` dropped-consumer → generic `StorageError`.
- Fork maintenance: `sync-core` branch diverges further from upstream alpha; the 180-suite keeps it honest, but rebasing costs grow.
