# Spike: openraft hot-path runtime collapse (futex → same-thread poll)

**Date:** 2026-06-27
**Status:** design (validation spike, kill-early)
**Branch:** `spike/openraft-hotpath-runtime`
**openraft pin:** `0.10.0-alpha.25` (local path-dep `../openraft`; see bump notes below)

## 1. Question

After task11 collapsed the commit floor to ~1 ms and a string of µs-scale
micro-optimizations all came back NULL end-to-end, the floor decomposition
(`docs/benchmarks/floor-decomposition-2026-06-25.md`) established that the
~1–2 ms commit floor is **~73% software/structural** and only ~27% physical
(NVMe fsync 18% + wire RTT 10%). The structural cost is **openraft's async
choreography**: the §3b replication sub-probe showed the `append_entries` RPC
itself is wire-dominated (~0.03 ms of UC software) while **~0.52 ms (~74% of the
replication bucket) is openraft's RaftCore↔ReplicationCore↔apply async
duty-cycle** — "each a tokio task wakeup (~8.8 µs each, and there are many)".

That verdict named two remaining levers: (a) **openraft-internal** — fewer/cheaper
core↔replication async hops; (b) **structural co-location rewrite**. This spike
tests the cheapest credible form of lever (a) **without forking openraft's
consensus logic**.

## 2. Hypothesis and cost model

The per-commit choreography cost is:

```
choreography ≈ Σ(internal task hops) × (per-hop wakeup cost)
```

openraft alpha.25 spawns, per node: RaftCore loop, the state-machine worker
(`sm::Worker`), an io-completion forwarder, a heartbeat tick, and one
ReplicationCore task **per peer**. A committed write crosses several of these via
mpsc/watch channels (e.g. RaftCore → `sm::Worker` for apply: `raft_core.rs` send
→ `sm/worker.rs` recv; replication ack: ReplicationCore → RaftCore `tx_notify`).

On a **multi-thread** tokio runtime, each hop where the woken task lives on a
different worker thread is a **cross-thread park/unpark = a futex wake (~8.8 µs
on the reference hardware, per the aeron investigation)**. If all of one node's
openraft tasks run on **one thread**, those same hops become **same-thread
local-queue polls (~ns)** — the scheduler just re-polls the next task, no futex.

**Hypothesis:** forcing a node's openraft tasks onto a single thread removes the
cross-thread futex component of the choreography, reducing the commit→apply and
replication latency by a meaningful fraction of the ~0.86 ms base + ~0.52 ms
replication software.

This is the *same* futex→busy-spin lever the whole investigation crowned —
applied to openraft's **internal** tasks, which task18 never touched (it
busy-spun only UC's own ring consumers, which is why it came back null).

## 3. Treatment: A0 (and a possible A1)

**A0 — single-thread tokio (the cheap rung).** Run the node on a
`current_thread` tokio runtime instead of `multi_thread`. openraft's
`TokioRuntime::spawn` calls `tokio::spawn`, which on a `current_thread` runtime
keeps every spawned task on that one thread. **No openraft `single-threaded`
feature, no `OptionalSend`/type-bound changes, no custom executor** — purely the
runtime flavor chosen by the node harness.

- Safe regime: M1 embedded apply is a fast in-proc call and (eventual durability)
  there is no inline fsync on the async thread, so nothing blocks the single
  thread. `spawn_blocking` work (if any) still uses tokio's blocking pool on
  separate threads and is unaffected.

**A1 — busy-poll executor (only if A0 wins).** A custom `AsyncRuntime` pinned to
a core that never parks, removing the residual idle-park latency A0 leaves. More
code, pins a core at 100%. **Kill-early logic: if A0 does not move the floor, A1
cannot save it** (busy-poll only shaves the smaller residual on top of A0).

> alpha.25 note: a custom `AsyncRuntime` (A1) implements the renamed
> `mpsc_recv_deadline` (was `mpsc_recv_timeout_at`). Not needed for A0.

## 4. Why staged — what each stage can and cannot show

A **single-node** setup has the *fewest* cross-thread hops (no replication
round-trips), so it is the **weakest** place to see the win; the bulk of the
"many × 8.8 µs" lives in the **replication** path, which needs ≥3 nodes. Hence:

### Stage 0 — is the premise reproducible on this box? (nearly free)

A ~30-line bare-tokio microbench: ping-pong a value across two tasks via mpsc at
inflight=1, measure per-hop wake latency on `multi_thread` vs `current_thread`.

- multi_thread ≈ µs/hop and current_thread ≈ ns/hop → headroom exists, local
  spike is viable.
- similar on both → this box does not reproduce the futex cost; a local spike
  cannot show the lever → go straight to fleet or abort.

Dispositive and almost free; it also re-confirms the ~8.8 µs figure on the
current hardware and frames every later number.

### Stage 1 — single-node UC, base commit→apply

In-process **single-node M1** UC node (apply = direct in-proc call, no
shmem/service noise), in-memory `RegisterSm` (from `uc-lincheck`), driver at
**inflight=1**, measure **submit→apply p50** (and submit→response).

- Baseline arm: node on `multi_thread` runtime (futex present — fleet-faithful).
- A0 arm: node on `current_thread` runtime.

Shows whether A0 moves the ~0.86 ms / ~672 µs base commit→apply handoff.

> The existing single-process attribution harness is already `current_thread`,
> so it **cannot** serve as the baseline (its baseline ≈ A0). The baseline arm
> **must** be multi_thread. Verify the harness runtime flavor when wiring this.

### Stage 2 — 3-node replication choreography (only if Stage 1 positive)

Repurpose the in-process 3-node scaffolding from the lin/partition tests; run
**each node on its own single-threaded runtime** (intra-node tasks share a
thread; inter-node traffic stays on the network). Measure commit→apply at
inflight=1, A0 vs baseline. Captures the larger ~0.52 ms replication slice.
Local first (in-proc / loopback QUIC); a fleet A/B only if it graduates.

## 5. Kill-gates

| Gate | Result | Action |
|---|---|---|
| Stage 0 | no per-hop difference locally | can't spike locally → fleet-only or abort |
| Stage 1 | null | base unaffected (weak kill: replication may still win) → explicit go/no-go on Stage 2 |
| Stage 1 | positive | proceed to Stage 2 |
| Stage 2 | null | lever (a) is null end-to-end → report, stop, document |
| Stage 2 | positive | quantify; decide A1 busy-poll and/or production-bound A0 (M3, fleet A/B) |

**What counts as "positive" (decision threshold).** Tie the verdict to the cost
model, not a fixed percentage: A0 is positive on a stage if the submit→apply p50
drop is (a) statistically clear of the run-to-run noise band (target ≥3× the
measured noise), **and** (b) of the order predicted by removing the cross-thread
hops on that path — `Δ ≈ (hops removed) × (Stage-0 per-hop futex cost)`.

**Effect-size expectation (why Stage 1 is a weak discriminator).** The
single-node base path crosses only a handful of cross-thread hops (RaftCore →
`sm::Worker` → apply, plus the io-completion forwarder), so the *predicted* A0
saving is only **~tens of µs** — likely within the noise band of a ~0.86 ms base.
Therefore a small or null Stage 1 is a **weak** kill (it may just be below the
noise floor), while a clear Stage 1 win is strong motivation. The **replication**
path (Stage 2) crosses *many* hops per commit, so its predicted saving (~hundreds
of µs) is the path where A0 can clear the noise — Stage 2 is the real test.

Every stage — **including null ones** — writes a short result note under
`docs/benchmarks/`. Nulls are reported, not buried (consistent with the prior
micro-opt nulls).

## 6. Scope / non-goals

- **In scope:** measuring whether single-threading a node's openraft runtime
  reduces the single-shot (inflight=1) commit→apply + replication floor.
- **Out of scope (this spike):** the A1 busy-poll executor (gated on A0),
  M3-shmem single-threading (apply awaits the service over shmem — single-thread
  is only safe once that interaction is analyzed), forking openraft's core loop
  to collapse the `sm::Worker` hop (lever b), and the co-location rewrite.
- **Throughput:** the spike targets the **latency floor** (inflight=1). A0 may
  trade throughput (one thread) for latency; throughput is a Stage-2+ concern,
  not a gate.

## 7. Dependency bump (prerequisite, done)

openraft bumped alpha.21 → alpha.25 on this branch (local path-dep to
`../openraft`; alpha.25 is not yet on crates.io). UC-side fixes: `transfer_leader`
now returns `TransferLeaderResponse` (`uc_node/src/network/pipelined.rs`);
`RaftMetrics::committed` → `cluster_committed`
(`uc_node/src/ipc/metrics_publisher.rs`). Verified: workspace build (0 warnings),
clippy (`-D warnings`), full default test suite + in-process lincheck capstone,
and the `fault-injection` partition/quorum-loss linearizability suite — all green.
The alpha.25 changes are consensus correctness/features (Pre-Vote, local- vs
cluster-committed, leadership-transfer hardening, recovery API); the
`AsyncRuntime` trait and `sm::Worker` commit→apply hop this spike depends on are
structurally unchanged.

> Cloud caveat: the path-dep means a fleet A/B (Stage 2 graduation) must rsync
> the `../openraft` checkout alongside `ultima_cluster`, or switch to a
> `[patch.crates-io]`/vendored pin. Does not affect the local stages.

## 8. Deliverables

- Stage 0 microbench (bare tokio, `multi_thread` vs `current_thread` wake latency).
- Stage 1 single-node M1 latency probe with a runtime-flavor toggle.
- (Conditional) Stage 2 3-node in-process probe.
- Result note(s) under `docs/benchmarks/` and a go/no-go verdict feeding the
  canonical floor-decomposition record.
