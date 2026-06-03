# Task 09 — Unified benchmark harness (Phase 0 + Phase 1)

**Status:** Phase 0 + Phase 1 shipped. Phases 2–4 deferred (data-driven; see below).
**Supersedes (consolidates):** the `2026-06-03-unified-benchmark-harness` design + plan
(deleted from `docs/superpowers/` per the CLAUDE.md feature workflow).

## 1. Why

`ultima_cluster` had two point-solution benchmarks: `uc_autobench` (a fast,
low-noise optimization loop for the shmem rings) and an Aeron-vs-UC commit-path
load-stepper. They covered *one layer deeply* and *one cross-layer
decomposition*. This task generalizes them into a single harness whose purpose
is **optimization**: for any change anywhere in the stack, answer *"did this
move the number, and is that number on the critical path?"*

## 2. Two-tier model

```
MICRO TIER (fast, low-noise)        FULL-PATH TIER (integration truth)
per-layer fitness fn + frozen   <-> open-loop driver + checkpoint stamps
conformance gate                     -> attribution.csv (stage x p50/p99/p99.9)
shmem ✓  journal  apply  quic …
-> tasks/<task>/results.tsv
```

A micro improvement is "real" only if the corresponding stage in the full-path
attribution moves. That cross-check is the whole point — it stops us optimizing
a layer that isn't on the critical path (the prior situation: ~90 ns rings
under a multi-millisecond IPC floor).

## 3. Phase 0 — the `TaskSpec` seam

`run-iter` was hardcoded to `shmem` (`shmem-microbench` / `spsc_p99_ns` /
`shmem-e2e` / `submit_to_resp_p99_ns`). It now dispatches via a `TaskSpec` row:

- `uc_autobench/src/task_spec.rs` — `TaskSpec { task, microbench_bin,
  primary_metric, gate_bin, gate_metric }` + `task_spec(name) -> Option<TaskSpec>`.
- `run-iter` looks the task up; unknown tasks emit a clean
  `{"status":"unknown_task",...}` JSON verdict instead of `exit(2)`.
- A task with `gate_bin: None` passes through with `gate.ran = false`.

**Adding a benchmark task = adding a `TaskSpec` row, not forking `run-iter`.**
See `uc_autobench/tasks/TEMPLATE.md` for the per-task layout + conventions.

## 4. Phase 1 — full-path attribution (the keystone)

### Mechanism: feature-gated probe sink

`uc_protocol` gained a `uc-bench-probes` cargo feature (default **off**) and a
`probes` module. When **off**, every `stamp_*`/`bridge` call is an
`#[inline(always)]` no-op — zero production cost, frame layout unchanged
(non-negotiable given `uc_protocol`'s zero-copy posture). When **on**,
timestamps land in a process-local sink.

**Why not frame-embedded timestamps?** The 8-byte `header_extra` is already full
(it carries the `(client_id, local_seq)` / `log_index` correlation ids). So
timestamps do *not* ride the frames. Instead the sink is keyed by the
correlation ids that already flow through the system:

- early path is keyed by `(client_id, local_seq)` (client-side stages),
- mid path is keyed by `log_index` (journal + apply stages),
- a **bridge** `(client_id, local_seq) -> log_index` is recorded at the
  dispatcher once `client_write` returns, and `drain_joined()` merges the two
  keyspaces into one row per request.

**Hard limitation:** this is valid **only for the single-process in-process
fixture** — one process, one coherent `Instant` clock. Multi-process / 3-node
attribution (Phase 4) needs a different mechanism (per-process internal deltas +
an RTT "transfer" bucket); it is explicitly out of scope here.

### Checkpoint set (path order)

`Submit → NodeDequeue → JournalAppended → JournalFsynced → ApplyEnqueue →
ApplyStart → ApplyDone → RespDequeue → Broadcast → ClientRecv`

Reported stages (consecutive deltas) in `attribution.csv`:
`submit_to_node, node_to_append, journal_fsync, commit_to_apply_enq, apply_ring,
apply, resp_ring, resp_to_broadcast, broadcast_to_client, total`.

### Wiring sites (the `stamp_*` calls)

| Stage boundary | File |
|---|---|
| Submit, ClientRecv | `uc_client/src/{client,rings}.rs` |
| NodeDequeue, bridge, Broadcast | `uc_node/src/ipc/client_dispatcher.rs` |
| JournalAppended, JournalFsynced | `uc_node/src/raft/log_storage.rs` |
| ApplyEnqueue, RespDequeue | `uc_node/src/raft/state_machine_shmem.rs` |
| ApplyStart, ApplyDone | `uc_service/src/runtime/apply_loop.rs` |

Acceptance: `uc_autobench/tests/attribution_probes.rs` drives 64 real requests
through `ClusterFixture` and asserts every checkpoint is populated and joins.

## 5. Artifacts

| Artifact | What |
|---|---|
| `uc_autobench/tasks/<task>/results.tsv` | per-layer micro run log, integer-ns, median-of-N |
| `bench-out/*.csv` | full-path load curves (Aeron-comparable saturation) |
| `bench-out/attribution.csv` | per-stage decomposition (this task's new output) |
| `bench-out/reference/attribution.csv` | **committed baseline** — diff future runs against it |

## 6. Running `attribution-bench`

```bash
# tmpfs (default TMPDIR on this host -> fsync is free; isolates CPU/IPC cost):
cargo run -p uc_autobench --features uc-bench-probes --bin attribution-bench \
  --release -- --config single_tmpfs --inflight 8 --count 5000

# real disk (storage axis): redirect the fixture's journal TempDir to ext4.
TMPDIR=/home/<you>/uc-bench-data cargo run -p uc_autobench \
  --features uc-bench-probes --bin attribution-bench --release -- \
  --config single_disk --inflight 8 --count 5000 \
  --out bench-out/reference/attribution.csv
```

The binary does **not** relocate the journal itself — the storage axis is the
`TMPDIR` env var (the fixture's `tempfile::TempDir` honors it), labelled by
`--config`. `--features uc-bench-probes` turns on `uc_protocol`'s sink across the
whole graph via cargo feature unification, so a single instrumented build of
`uc_protocol` is shared by `uc_node`/`uc_service`/`uc_client`.

The default `cargo build --workspace` (no feature) stays clean: the bin carries
`required-features = ["uc-bench-probes"]`, so cargo skips it without the feature.

## 7. Headline reference numbers (the keystone result)

Single-node, in-process fixture, 64-byte payload, inflight=8, 5000 requests,
p99 ns. Committed in `bench-out/reference/attribution.csv`.

| stage | single_disk (ext4) p99 | single_tmpfs p99 |
|---|---:|---:|
| **submit_to_node** | **16,015,359** | **9,510,911** |
| resp_ring | 2,101,247 | 2,107,391 |
| broadcast_to_client | 1,968,127 | 2,125,823 |
| journal_fsync | 847,871 | 42,271 |
| commit_to_apply_enq | 705,023 | 108,351 |
| apply_ring | 162,815 | 160,127 |
| apply | 2,565 | 2,012 |
| **total** | **19,709,951** | **12,197,887** |

**The long-held "~38 ms commit path ≈ fsync" assumption is corrected by
measurement.** The dominant cost is **`submit_to_node`** — the client→node
submit-ring hop — at ~16 ms p99 on disk (~81% of total), and the next two costs
(`resp_ring`, `broadcast_to_client`, ~2 ms each) are also ring hops. All three
are **poll-sleep IPC latency** (idle-backoff ring polling), not I/O.
`journal_fsync` is only ~0.85 ms on real ext4 (~4% of total) and ~42 µs on
tmpfs — fsync is **not** the floor. This matches the independent diagnosis from
the earlier commit-path work: the real lever is **event-driven ring wakeups**
(futex/eventfd) to kill poll-sleep latency, an architecture change — not
group-commit/fsync tuning.

## 8. Deferred (Phases 2–4) — now data-driven

The attribution above re-targets the roadmap:

- **Phase 2 (journal micro):** *de-prioritized* — fsync is ~4% of the path, not
  the dominant cost. A journal micro is still worth having for the storage axis,
  but it is not where the wall-clock is.
- **Phase 3 (apply/SM micro):** apply is ~2.5 µs — lowest-noise layer, easy
  real wins, but tiny relative to the IPC floor.
- **Phase 4 (multi-process / 3-node + QUIC):** needs the coarser cross-process
  attribution mechanism (§4 limitation).
- **The actual next target the data points to:** the ring-hop poll-sleep floor
  (`submit_to_node` / `resp_ring` / `broadcast_to_client`) — event-driven
  wakeups. That is its own design, not a micro in this harness.
```
