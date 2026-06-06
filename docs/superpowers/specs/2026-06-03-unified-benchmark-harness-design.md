# Unified Benchmark Harness — Design

**Date:** 2026-06-03
**Status:** Design (pre-implementation)
**Scope:** A consistent, layer-by-layer benchmarking approach for `ultima_cluster` whose purpose is *optimization*: every change anywhere in the stack can be measured against a committed baseline, and improvements are validated against where time is actually spent.

## 1. Purpose & framing

`ultima_cluster` already has two point-solution benchmarks:

- **`uc_autobench`** — a Claude-Code-driven optimization loop for the shmem rings. Frozen `shmem-microbench` (ring SPSC/MPSC/Broadcast latency+throughput), `shmem-e2e` (in-process commit-path Goodhart gate), `run-iter` (build → torture → microbench → conditional e2e → one JSON verdict), `results.tsv` run log with integer-ns baselines, median-of-N.
- **Aeron-vs-UC commit-path benchmark** — open-loop HDR-histogram load-stepping of the full single-node commit path, gap decomposed by layer (Aeron IPC floor → UC ring → tmpfs → real fsync → 3-node loopback). CSVs in `bench-out/`.

These cover *one layer deeply* (shmem rings) and *one cross-layer decomposition* (commit path). This design **generalizes** them into a single harness so any layer — journal group-commit, apply/state-machine, QUIC transport — gets the same treatment, and so the two tiers cross-check each other.

The deliverable is not "UC is N× slower than X." It is: for any optimization, a trustworthy answer to *"did this change move the number, and is that number on the critical path?"*

## 2. Architecture: two tiers + a reconciliation artifact

```
┌─ MICRO TIER (fast loop, low-noise) ─────────────────────────┐
│  per-layer fitness fn + frozen conformance gate              │
│  shmem ✓   journal   apply/SM   quic   …                     │
│  → results.tsv per layer, integer-ns baselines, median-of-N  │
└──────────────────────────────────────────────────────────────┘
            ▲ cross-check ▼
┌─ FULL-PATH TIER (integration truth) ────────────────────────┐
│  open-loop driver + checkpoint timestamps → per-stage budget │
│  → attribution.csv: stage × {p50,p99,p99.9}                  │
└──────────────────────────────────────────────────────────────┘
```

- **Micro tier** — isolated fitness functions per layer, driven in a tight loop with no other layers in the path. Cheap, low-noise, fast iteration. Used while actively optimizing a layer. Risk: Goodhart / over-fitting / missing cross-layer interaction.
- **Full-path tier** — a single open-loop driver runs the *real* commit path under load and attributes latency to each stage via checkpoint timestamps. The integration truth. Catches cross-layer regressions the micros miss.
- **Reconciliation artifact** — the per-stage attribution table. A micro improvement is "real" only if the corresponding stage in the full-path attribution moves. This is the cross-check that prevents optimizing a layer that is not on the critical path (the existing situation: 15 ns rings under a ~38 ms journal-fsync floor).

### Decisions locked during brainstorming

1. **Unify across all layers** (not just attack the dominant cost, not methodology-only).
2. **Tiered: both** micro-fitness fns and the instrumented full-path. They cross-check.
3. **Hybrid crate ownership** for sibling crates (`ultima_journal`, `ultima_db`): UC-owned black-box micros pin the *integration* config; sibling repos keep their own *internal* micros; the full-path attribution is the shared truth across all three.
4. **Checkpoint timestamps, feature-gated** for full-path instrumentation (not tracing spans, not sampling profiler).

## 3. Checkpoint instrumentation (full-path mechanism)

A fixed checkpoint set along the commit path. Each in-flight request carries a small timestamp vector stamped at boundaries; the bench aggregates by `log_index` into per-stage deltas.

Checkpoints (in path order):

```
SUBMIT          client stamps when it writes SubmitFrame
NODE_DEQUEUE    node client_dispatcher reads the submit frame
RAFT_ACCEPTED   openraft client_write accepted / appended to local log
JOURNAL_APPENDED  record written to journal buffer
JOURNAL_FSYNCED   group-commit fsync returned
COMMITTED       openraft marks committed (= local for single node)
APPLY_ENQUEUE   ApplyFrame written to service/apply.ring
APPLY_START     service apply_loop dequeues, before state_machine.apply
APPLY_DONE      after apply, ApplyRespFrame written
RESP_DEQUEUE    node reads apply_resp
BROADCAST       node writes SubmitResponse to broadcast ring
CLIENT_RECV     client reads response
```

Per-stage deltas are consecutive differences; the named stages reported in `attribution.csv` are derived from these (e.g. `journal_fsync = JOURNAL_FSYNCED − JOURNAL_APPENDED`, `apply = APPLY_DONE − APPLY_START`, `submit_to_node = NODE_DEQUEUE − SUBMIT`).

### Properties

- **Feature-gated by `uc-bench-probes` (cargo feature, default off).** When off: stamp sites compile to nothing and frames carry no extra bytes — zero production cost. This is non-negotiable given `uc_protocol`'s `no_std` / zero-copy posture. The frame layout MUST NOT change shape when the feature is off.
- **Aggregation key.** `log_index` is the join key. Because `log_index` is assigned by Raft (not known at `SUBMIT`), the request carries a client-assigned `request_id`; the framework already correlates `request_id ↔ log_index` (submit carries the id; the response broadcast carries `log_index`). The timestamp vector rides with the request through the rings; the node accumulates the in-node checkpoints onto the same in-flight record; the final vector is reported at `CLIENT_RECV`.
- **Clock coherence is a hard boundary.**
  - *In-process `current_thread` fixture* — one process, one coherent clock. All 11 deltas are precise. **This is the canonical decomposition.**
  - *Multi-process / 3-node* — clocks are not comparable across processes. Each process reports its own internal stage deltas; cross-process hops are lumped into a single "transfer" bucket and measured as RTT. Documented as coarser — "replication-layer cost at process granularity," not a sub-stage decomposition.
- **Runtime flavor.** `current_thread` for both tests and `#[tokio::main]` bench binaries. `multi_thread` (even single-runtime) intermittently times out the shmem handshake — see `feedback_m3_test_runtime_flavor`.

## 4. Micro tier — generalize the shmem template

Extract the existing shmem structure into a reusable shape. Each layer gets:

```
uc_autobench/tasks/<layer>/
  program.md     mutable paths, metrics, TSV schema, layer-specific constraints
  results.tsv    run log: commit | <layer metrics> | memory_kb | status | description
```

plus a `<layer>-microbench` binary and a **frozen** conformance/torture test (the equivalent of `ring_torture.rs`). `run-iter` grows a `--task <layer>` dispatch that selects the right micro + gate + baseline.

Per the hybrid decision:

- **UC-owned micros** (live in `uc_autobench`) pin the *integration* config — what UC actually does:
  - **journal** — drive `ultima_journal::Journal` with UC's exact group-commit settings and per-record term in the `meta` slot.
  - **apply / state-machine** — drive `StoreStateMachine.apply` with representative commands (default 64 B kv write). Pure CPU, deterministic, no I/O → lowest-noise layer.
  - **quic** — quinn stream round-trip between two loopback endpoints using UC's `write_chunks` scatter-gather framing.
- **Sibling-repo micros** (`ultima_journal`, `ultima_db`) own their *internal* tuning, independent of UC.
- Both reconcile against the full-path attribution (§3).

**Raft note.** The openraft `client_write → commit` step on a single node is hard to isolate from the journal beneath it; initially it is measured via full-path attribution (the `RAFT_ACCEPTED → COMMITTED` deltas minus journal) rather than a standalone micro. A dedicated raft micro is out of scope for v1.

### Environment control

- **`--storage {tmpfs, disk}` axis** on every storage-touching bench. `tmpfs` removes device variance (for CPU/logic optimization); `disk` with real fsync is the I/O-bound truth.
- Median-of-5 for latency, median-of-9 for throughput.
- Warmup + fixed iteration counts.
- **CRC always kept** — removing the frame CRC is a Goodhart trap (see `shmem_ring_optimizations`).
- No `Date::now` / `rand` in bench logic; vary by index for any needed variation.

## 5. Schema & baseline-comparison model

Three artifacts, each with a committed reference so a change produces a **diff**, not just a number.

### (a) Micro results — `uc_autobench/tasks/<layer>/results.tsv`

```
commit | <layer metrics, integer ns> | memory_kb | status | description
```

Baseline = the integer-ns values of the current champion (`status=keep`) row. `run-iter` compares new run → champion, gates at the per-layer threshold (shmem uses 5%), emits one JSON verdict. Unchanged mechanism, parameterized by `--task`.

### (b) Full-path load curves — `bench-out/*.csv` (unchanged)

```
system,config,workload,payload_bytes,inflight,target_rate,achieved_rate,p50_ns,p99_ns,p99_9_ns,p99_99_ns,max_ns,count
```

The Aeron-comparable saturation data. `system ∈ {uc, aeron}`, `config ∈ {single_tmpfs, single_disk, 3node_loopback, ipc}`, `workload ∈ {kv, bytes}`.

### (c) Per-stage attribution (new) — `bench-out/attribution.csv`

```
config,workload,payload_bytes,inflight,stage,p50_ns,p99_ns,p99_9_ns,count
```

One row per checkpoint-delta stage. A committed `bench-out/reference/attribution.csv` is the baseline; after a change, diff the whole decomposition and see which stage moved.

**This is the table that links the two tiers.** `stage=journal_fsync` here must track the journal-micro's `fsync_p99`; if the micro improves but this row does not, the change did not land on the critical path.

### Conventions (generalized from current practice)

Integer ns only; median-of-5 latency / median-of-9 throughput; warmup + fixed iterations; CRC always kept; `current_thread` runtime; no `Date::now`/`rand`. One baselines registry so every layer is gated identically.

## 6. Build sequencing

Ordered so the data drives the next step. Each phase is independently shippable and leaves the harness usable.

| Phase | Deliverable | Rationale |
|---|---|---|
| **0** | Generalize `run-iter` + extract `tasks/<layer>/` template from shmem | Cheap refactor; unlocks every later micro. No new measurement. |
| **1** | Checkpoint instrumentation + `attribution.csv` in the in-process fixture | **Keystone.** Turns assumed "~38 ms ≈ fsync" into a measured per-stage budget; tells you which micro is worth building. |
| **2** | Journal micro (UC integration config; tmpfs + disk) | Attribution will confirm the dominant cost — optimize where the time is. |
| **3** | Apply / state-machine micro | Pure-CPU, deterministic, lowest-noise → easiest real wins; natural second target. |
| **4** | Extend attribution to multi-process / 3-node + QUIC micro | Replication/transport cost only matters multi-node; do it once single-node is fully decomposed. |

Phase 1 is the keystone — everything after is "the attribution said stage X dominates, so build micro X."

### Folding in existing work

- The **shmem task** becomes the first instance of the generalized micro template (§4).
- The **Aeron commit-path bench** becomes the full-path tier's load-curve half (artifact b), with attribution (artifact c) layered onto its existing open-loop driver.

## 7. Non-goals (v1)

- A standalone Raft-only micro (measured via attribution instead).
- Sub-stage decomposition across process boundaries (clocks not coherent; lumped + RTT).
- Replacing `criterion` where sibling crates already use it for internal tuning.
- A general-purpose tracing/flamegraph subsystem (checkpoint timestamps only).
- Cross-host (real-NIC) benchmarking — loopback QUIC only for v1.

## 8. Open questions for implementation planning

- Exact `request_id ↔ log_index` correlation hook in the node's in-flight tracking — confirm the existing structure exposes a place to hang the timestamp vector.
- Whether the timestamp vector rides inside the frame (zero-copy, feature-gated frame region) or in a side `log_index`-keyed bench ring drained by an aggregator. Frame-embed is preferred for single-process; revisit for multi-process.
- Per-layer gate thresholds (shmem = 5%; journal/apply may warrant different tolerances given different noise floors).
