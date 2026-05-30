# Aeron IPC vs ultima_cluster Commit-Path Benchmark & Gap Decomposition

**Date:** 2026-05-30
**Status:** Design — approved section-by-section, pending spec review
**Author:** Peter Knego (with Claude Code)

## 1. Goal & framing

Produce a defensible **latency-vs-throughput** comparison of ultima_cluster's
full commit path against Aeron same-host IPC, then use the **decomposed** gap to
drive a prioritized UC optimization backlog.

The two systems do different jobs: Aeron is a tuned message *transport*; UC is an
*SMR cluster* (Raft + journal + state machine) whose IPC layer is an
architectural cousin of Aeron's. A single head-to-head number would be
misleading, so the deliverable is a **gap decomposition by layer**, not "UC is
Nx slower than Aeron."

### Why decomposition, not a single number

UC's own e2e gate already establishes the dominant cost. From
`uc_autobench/src/bin/shmem-e2e.rs` and task08:

- Full single-node commit path: **p50 ≈ 36 ms, p99 ≈ 40–41 ms**,
  **~100 round-trips/s** aggregate (4 clients × 500 reqs, in-process fixture).
- This is **Raft-commit-dominated** — attributed to the journal group-commit
  window (~38 ms/committed entry). **This floor has never been optimized** (only
  flagged). task08 optimized the shmem rings to a ~15 ns SPSC p99 floor — a
  layer that is currently <0.001% of the commit-path latency budget.

Aeron IPC round-trips are single-digit microseconds. So the raw transport gap is
~1000–10000×, and essentially **all** of it is consensus + journal fsync, not
transport. The benchmark's value is in attributing the gap to layers:

| Layer | Representative measurement | Expected scale |
|---|---|---|
| Pure transport (same-host RT) | Aeron C IPC RT; UC ring RT (existing microbench) | ~1–15 µs / ~15 ns ring |
| + Consensus + journal fsync | UC single-node commit path (tmpfs, then real disk) | ~tens of ms |
| + Replication / quorum | UC 3-node loopback commit path | + network/quorum |
| + Real state machine | KV workload (ultima_db writes) layered on UC | apply cost |

Aeron sets the **transport floor** (what is physically achievable same-host);
UC's layers show where the time actually goes. "Improve UC" then means attacking
the dominant layer first (almost certainly journal group-commit), mining Aeron's
techniques for the transport layer only once it matters.

### Scope boundary (YAGNI)

**In scope:** measure, decompose, and produce a prioritized optimization
backlog. **Out of scope:** implementing the optimizations (that is the follow-on,
and the `uc_autobench` autoresearch loop is the vehicle for it); modifying
upstream Aeron; cross-host real-NIC testing; Java/.NET Aeron clients (C only).

## 2. Measurement methodology (shared, open-loop)

Both systems run the **same load-stepping protocol** and emit **HDR histograms**
so curves overlay directly.

- **Open-loop load stepper:** fixed target rate per step, stepping a rate ladder
  (e.g. 100, 1k, 10k, 50k, 100k, … msgs/s) until saturation. Latency timestamps
  taken at *intended* send time, not actual send time — coordinated-omission-free,
  matching Aeron's own latency tooling.
- **Per step:** warmup, then a fixed measurement window; record into an HDR
  histogram. Aeron side: `org.HdrHistogram` (C: `hdr_histogram`, already vendored
  under `cppbuild/`). UC side: `hdrhistogram` crate — identical percentile math.
- **Output schema (both sides, CSV):**
  `target_rate, achieved_rate, p50, p99, p99_9, p99_99, max, payload_bytes, inflight`
- **Headline per system/config:** the **knee** — the offered load where p99
  hockey-sticks and achieved_rate falls below target.

**Fairness controls (disclosed in the report, not hidden):** identical payload
sizes; same machine + CPU pinning; same warmup/window; fsync target disclosed
(real disk vs tmpfs); in-flight concurrency disclosed and swept; build profiles
(`cargo --release` with the workspace `lto=thin, codegen-units=1`; Aeron
`Release`). Platform is Apple Silicon arm64 (matches task08); note that
`io_uring` is Linux-only and therefore out of scope for the measurement host.

## 3. System configurations & workload

### UC configs (both)

1. **Single-node** — one `uc_node`, Raft commit path active. Two fsync targets
   measured separately: **tmpfs** (isolates consensus from disk) and **real
   disk** (adds fsync cost). Isolates consensus + fsync from networking.
2. **3-node loopback** — three `uc_node` over QUIC on localhost, real quorum.
   Adds replication/quorum cost. The loopback caveat (not representative of real
   NIC latency) is stated explicitly in the report.

### Aeron config (reference line, not a variable)

C IPC request/response (ping/pong shape) over two IPC streams, single host, no
media-driver network. This is the transport floor.

### Workload (realistic mixed, KV-style)

- **UC:** the `kv_service` / `StoreStateMachine` adapter (real `ultima_db`
  writes), so apply cost is in the loop. Small fixed key space, fixed value size
  (64 B baseline), write-heavy with a read mix (linearizable + snapshot queries)
  to exercise both query-routing paths.
- **Aeron:** the *same payload bytes* echoed back, no state machine (Aeron is the
  transport floor). The report explicitly attributes KV apply cost as a UC-only
  layer.

### Attribution chain

`Aeron IPC (bytes only)` → `UC ring RT (microbench)` → `UC single-node tmpfs` →
`UC single-node real-fsync` → `UC 3-node loopback`, with the KV apply cost broken
out as its own layer.

### Concurrency sweep (critical)

At ~38 ms/commit with a single in-flight request, an open-loop ladder saturates
UC almost immediately. UC must pipeline/batch concurrent in-flight commits
(group commit). The UC driver therefore **sweeps in-flight concurrency** as a
parameter; the achievable-throughput knee is set by group-commit batching
efficiency — which is also the #1 optimization target.

## 4. Components & build sequence

### New components — UC side

- `uc_autobench/src/bin/commit-path-load.rs` — standalone open-loop load driver:
  rate ladder, swept in-flight concurrency, HDR histogram per step → CSV.
  **Separate from** the autoresearch `run-iter` gate; reuses
  `uc_node::test_support::ClusterFixture` and `uc_client`. Flag selects
  single-node vs 3-node-loopback mode and tmpfs vs real-disk journal dir.
  - Must use a `current_thread` tokio runtime for the in-process fixture path
    (per project memory `feedback_m3_test_runtime_flavor` — `multi_thread`
    intermittently times out the shmem handshake during fixture bring-up).
- A run script that drives the rate ladder across all UC configs and dumps CSVs.

### New components — Aeron side

- A small Aeron **C** IPC ping/pong latency tool adapted from
  `aeron-samples/src/main/c` (e.g. `cping.c`/`cpong.c`), emitting the **same CSV
  schema**. Lives in a scratch dir in the aeron tree; **not** contributed
  upstream.

### Shared / analysis

- A plotting/aggregation script (**Python + matplotlib**): overlay
  latency-vs-throughput curves; per-layer decomposition bar at a fixed offered
  load. Consumes the shared-schema CSVs from both sides.
- **Report** in `docs/tasks/` (UC convention — superpowers specs are ephemeral
  scaffolding, consolidated into a `taskNN_*.md` on completion). Contains: the
  gap-decomposition table, overlay plots, and the **prioritized optimization
  backlog** keyed to the dominant layer.

### Build sequence

1. Aeron C IPC latency tool; verify CSV schema. Establishes the floor number
   early.
2. UC single-node load driver; sanity-check against the known ~38 ms commit
   floor.
3. Add tmpfs vs real-disk fsync runs; add 3-node loopback mode.
4. Wire KV workload (`StoreStateMachine`) into the UC driver; concurrency sweep.
5. Run both; collect CSVs; generate overlay plots + decomposition table.
6. Write the report + prioritized optimization backlog.

## 5. Success criteria

- Both systems produce CSVs in the shared schema from the same open-loop ladder.
- An overlay latency-vs-throughput plot per UC config with the Aeron floor line.
- A decomposition table attributing the commit-path latency to transport /
  consensus / fsync / replication / apply layers, with numbers.
- A prioritized optimization backlog ranked by measured layer contribution
  (expectation: journal group-commit / fsync pipelining dominates; shmem-ring
  work is already near its floor per task08).

## 6. Risks & open questions

- **Loopback ≠ real NIC.** 3-node numbers over-credit the network layer; stated
  as a caveat, not corrected for.
- **In-process fixture vs real processes.** Phase 1 (single-node) runs
  **in-process** via the existing `ClusterFixture` (single-node only — verified).
  Phase 2 (3-node) uses **real multi-process** launch. NOTE (corrected during
  planning recon): there is **no `multi-process-tests` cargo feature** and there
  are **no standalone `uc_node`/`uc_service`/`uc_client` binaries** — those
  references in `CLAUDE.md` and earlier specs are stale. Phase 2 therefore
  requires authoring a small `uc-node-launch` binary wrapping `NodeBuilder` with
  `BootstrapConfig::Peers` + `IpcMode::Shmem` (pattern from
  `uc_node/tests/m2_multi_node.rs` + `examples/counter_loop`). See the plan's
  Phase 2 for the verified `NodeConfig`/`ServiceConfig` fields.
- **Aeron apples-to-apples.** Aeron carries bytes only; the report must never
  present the Aeron line as if it included a state machine.
- **io_uring is Linux-only.** Any group-commit/fsync optimization that depends on
  it cannot be measured on the arm64 macOS host; the backlog will note which
  optimizations are platform-gated.
