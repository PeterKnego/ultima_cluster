# Unified Benchmark Harness — Phase 0 + Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generalize the per-layer optimization harness (`run-iter` task seam) and build the keystone full-path latency-attribution tier — feature-gated checkpoint probes along the commit path that emit a per-stage `attribution.csv`.

**Architecture:** Two changes. (Phase 0) Extract a `TaskSpec` table so `run-iter` is parameterized by task instead of hardcoded to `shmem`, plus a task-template doc. (Phase 1) A `uc-bench-probes` cargo feature on `uc_protocol` exposing a process-local **probe sink**: every commit-path stage calls a `stamp_*` function (a zero-cost no-op unless the feature is on). Because the in-process fixture is a single process with one coherent clock, timestamps need not ride the frames (the 8-byte `header_extra` is already full); they are keyed by the correlation ids that already flow — `(client_id, local_seq)` early, `log_index` mid-path — and joined by a bridge recorded at the dispatcher. A new `attribution-bench` binary drives the fixture, drains the sink, and writes per-stage percentiles.

**Tech Stack:** Rust, `parking_lot`, `hdrhistogram`, `futures` (`buffer_unordered`), `tokio` (current_thread), `clap`, the existing `ClusterFixture` (`uc_node` `test-support` feature).

**Scope note:** This plan covers spec §6 Phase 0 + Phase 1 only. Phases 2–4 (journal micro, apply micro, multi-node/QUIC) each get their own plan once Phase 1's attribution data identifies the dominant stage to target — that data-driven sequencing is the whole point of doing attribution first.

**Resolves spec §8 open question:** timestamps are *not* frame-embedded (no room in `header_extra`); they use a process-local sink keyed by existing correlation ids + a bridge. This is valid only for the single-process fixture (the spec's canonical decomposition); multi-process attribution (Phase 4) will need a different mechanism and is out of scope here.

---

## File Structure

**Phase 0**
- Create: `uc_autobench/src/task_spec.rs` — `TaskSpec` struct + `task_spec(task)` lookup table. One responsibility: map a task name to its fitness/gate binary names and metric keys.
- Modify: `uc_autobench/src/lib.rs` — add `pub mod task_spec;`.
- Modify: `uc_autobench/src/bin/run-iter.rs` — consult `task_spec` instead of hardcoding `shmem-microbench` / `spsc_p99_ns`.
- Create: `uc_autobench/tasks/TEMPLATE.md` — the per-task layout/convention doc.

**Phase 1**
- Modify: `uc_protocol/Cargo.toml` — add `[features] uc-bench-probes = []`.
- Create: `uc_protocol/src/probes.rs` — checkpoint enum, sink, `stamp_*`/`bridge`/`reset`/`drain_joined`/`stage_deltas`. Single responsibility: probe capture + join. ~150 lines.
- Modify: `uc_protocol/src/lib.rs` — add `pub mod probes;`.
- Modify: `uc_client/src/client.rs` — SUBMIT probe.
- Modify: `uc_client/src/rings.rs` — CLIENT_RECV probe.
- Modify: `uc_node/src/ipc/client_dispatcher.rs` — NODE_DEQUEUE + bridge + BROADCAST probes.
- Modify: `uc_node/src/raft/log_storage.rs` — JOURNAL_APPENDED + JOURNAL_FSYNCED probes.
- Modify: `uc_node/src/raft/state_machine_shmem.rs` — APPLY_ENQUEUE + RESP_DEQUEUE probes.
- Modify: `uc_service/src/runtime/apply_loop.rs` — APPLY_START + APPLY_DONE probes.
- Modify: `Cargo.toml` (workspace) — add `hdrhistogram = "7"` to `[workspace.dependencies]`.
- Modify: `uc_autobench/Cargo.toml` — add `uc-bench-probes` feature, `hdrhistogram`, `futures` deps, `attribution-bench` bin.
- Create: `uc_autobench/src/bin/attribution-bench.rs` — driver + drain + CSV/JSON emit.
- Create: `uc_autobench/tests/attribution_probes.rs` — end-to-end acceptance test (gates all wiring).

---

## Phase 0 — Generalize the harness seam

### Task 0.1: TaskSpec lookup table

**Files:**
- Create: `uc_autobench/src/task_spec.rs`
- Modify: `uc_autobench/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `uc_autobench/src/task_spec.rs`:

```rust
//! Per-task descriptors for `run-iter`: which fitness/gate binaries to run and
//! which JSON metric keys to read. Adding a benchmark task = adding a TaskSpec
//! row, not forking run-iter. See
//! docs/superpowers/specs/2026-06-03-unified-benchmark-harness-design.md §4.

/// Immutable description of one optimization task's measurement binaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSpec {
    /// Task identifier (matches `tasks/<task>/`).
    pub task: &'static str,
    /// Cargo `--bin` name of the isolated fitness function.
    pub microbench_bin: &'static str,
    /// JSON key in the microbench stdout used for the KEEP/DISCARD gate.
    pub primary_metric: &'static str,
    /// Cargo `--bin` name of the Goodhart end-to-end gate, if any.
    pub gate_bin: Option<&'static str>,
    /// JSON key in the gate binary's stdout, if any.
    pub gate_metric: Option<&'static str>,
}

/// Look up the spec for a task name. `None` => unknown task.
pub fn task_spec(task: &str) -> Option<TaskSpec> {
    match task {
        "shmem" => Some(TaskSpec {
            task: "shmem",
            microbench_bin: "shmem-microbench",
            primary_metric: "spsc_p99_ns",
            gate_bin: Some("shmem-e2e"),
            gate_metric: Some("submit_to_resp_p99_ns"),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shmem_spec_resolves() {
        let s = task_spec("shmem").expect("shmem is known");
        assert_eq!(s.microbench_bin, "shmem-microbench");
        assert_eq!(s.primary_metric, "spsc_p99_ns");
        assert_eq!(s.gate_bin, Some("shmem-e2e"));
        assert_eq!(s.gate_metric, Some("submit_to_resp_p99_ns"));
    }

    #[test]
    fn unknown_task_is_none() {
        assert!(task_spec("does-not-exist").is_none());
    }
}
```

Add to `uc_autobench/src/lib.rs` (append after the existing doc comment):

```rust
pub mod task_spec;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p uc_autobench task_spec`
Expected: FAIL — `task_spec` module not found / not declared, until lib.rs change is saved. (If both files are saved together, expect PASS; in that case still run to confirm green.)

- [ ] **Step 3: Run test to verify it passes**

Run: `cargo test -p uc_autobench task_spec`
Expected: PASS — `shmem_spec_resolves` and `unknown_task_is_none` both green.

- [ ] **Step 4: Commit**

```bash
git add uc_autobench/src/task_spec.rs uc_autobench/src/lib.rs
git commit -m "feat(autobench): TaskSpec lookup table for run-iter generalization"
```

---

### Task 0.2: run-iter consults TaskSpec

**Files:**
- Modify: `uc_autobench/src/bin/run-iter.rs`

- [ ] **Step 1: Add the task lookup at the top of main**

In `uc_autobench/src/bin/run-iter.rs`, after `let args = Args::parse();`, add the spec resolution and a clean error for unknown tasks:

```rust
    let spec = match uc_autobench::task_spec::task_spec(&args.task) {
        Some(s) => s,
        None => {
            let out = Output {
                status: "unknown_task".to_string(),
                stage: "setup".to_string(),
                stderr_tail: Some(format!(
                    "unknown task {:?}; known tasks: shmem",
                    args.task
                )),
                ..Default::default()
            };
            emit_and_exit(&out);
        }
    };
```

- [ ] **Step 2: Use `spec.microbench_bin` in the microbench stage**

In the microbench stage (the `Command::new("cargo")` block around lines 348–387), replace the hardcoded `"shmem-microbench"` argument with `spec.microbench_bin`:

```rust
    let mut mb_cmd = Command::new("cargo");
    mb_cmd.args([
        "run",
        "-p",
        "uc_autobench",
        "--bin",
        spec.microbench_bin,
        "--release",
        "--quiet",
        "--",
        "--json",
    ]);
```

- [ ] **Step 3: Use `spec.primary_metric` when extracting the gate metric**

In the metric-extraction block (around lines 394–408), replace the hardcoded `"spsc_p99_ns"` key with `spec.primary_metric`:

```rust
    let spsc_p99_ns = out
        .metrics
        .as_ref()
        .and_then(|m| m.get(spec.primary_metric))
        .and_then(extract_u64);
```

- [ ] **Step 4: Use `spec.gate_bin` for the e2e stage**

In the e2e stage block (around lines 390–490), guard on `spec.gate_bin` and use it for the `--bin` argument:

```rust
    let Some(gate_bin) = spec.gate_bin else {
        // No Goodhart gate for this task: pass through with gate.ran = false.
        out.gate = Gate {
            ran: false,
            reason: Some("no_gate_bin_for_task".to_string()),
            ..Default::default()
        };
        out.status = "pass".to_string();
        emit_and_exit(&out);
    };
    let mut e2e_cmd = Command::new("cargo");
    e2e_cmd.args([
        "run",
        "-p",
        "uc_autobench",
        "--bin",
        gate_bin,
        "--release",
        "--quiet",
        "--",
        "--json",
    ]);
```

And where the gate reads `submit_to_resp_p99_ns` from the e2e JSON (around lines 467–476), source the key from `spec.gate_metric`:

```rust
    let gate_metric = spec.gate_metric.unwrap_or("submit_to_resp_p99_ns");
    let submit_to_resp_p99_ns = e2e_json.get(gate_metric).and_then(extract_u64);
```

- [ ] **Step 5: Verify shmem still works end-to-end**

Run: `cargo build -p uc_autobench --release`
Expected: builds clean.

Run: `cargo run -p uc_autobench --bin run-iter --release -- --task shmem --json`
Expected: one JSON line on stdout with `"status"` field; no `unknown_task`.

Run: `cargo run -p uc_autobench --bin run-iter --release -- --task bogus --json`
Expected: one JSON line with `"status":"unknown_task"`.

- [ ] **Step 6: Commit**

```bash
git add uc_autobench/src/bin/run-iter.rs
git commit -m "feat(autobench): run-iter dispatches via TaskSpec instead of hardcoded shmem"
```

---

### Task 0.3: Per-task template doc

**Files:**
- Create: `uc_autobench/tasks/TEMPLATE.md`

- [ ] **Step 1: Write the template**

Create `uc_autobench/tasks/TEMPLATE.md`:

```markdown
# Task template

Every optimization task lives in `uc_autobench/tasks/<task>/` and is registered
by adding one `TaskSpec` row in `uc_autobench/src/task_spec.rs`.

## Files per task

- `program.md` — mutable paths, frozen paths, the primary/secondary metrics,
  the TSV schema, and task-specific constraints. Modeled on `tasks/shmem/program.md`.
- `results.tsv` — committed run log, tab-separated. First column `commit`, last
  two columns `status` (keep|discard|crash) and `description`. Metric columns in
  between. Integer nanoseconds only; values are median-of-N (note N in the
  description).

## Conventions (all tasks)

- Integer ns baselines; median-of-5 for latency, median-of-9 for throughput.
- Warmup + fixed iteration counts; never single-sample a noisy percentile.
- `current_thread` tokio runtime for any in-process fixture (multi_thread flakes
  the shmem handshake).
- No `Date`/wall-clock and no `rand` in bench logic; vary by index.
- The frame CRC is never removed to win a number (Goodhart trap).
- A change is KEEP only if it beats the champion beyond run-to-run noise on the
  primary metric without regressing the secondary or the Goodhart gate.

## Registering a task

Add to `task_spec()`:

    "journal" => Some(TaskSpec {
        task: "journal",
        microbench_bin: "journal-microbench",
        primary_metric: "fsync_p99_ns",
        gate_bin: Some("shmem-e2e"),
        gate_metric: Some("submit_to_resp_p99_ns"),
    }),
```

- [ ] **Step 2: Commit**

```bash
git add uc_autobench/tasks/TEMPLATE.md
git commit -m "docs(autobench): per-task template + conventions"
```

---

## Phase 1 — Full-path attribution tier

### Task 1.1: Probe feature + sink (TDD'd in isolation)

**Files:**
- Modify: `uc_protocol/Cargo.toml`
- Create: `uc_protocol/src/probes.rs`
- Modify: `uc_protocol/src/lib.rs`

- [ ] **Step 1: Add the feature to uc_protocol/Cargo.toml**

In `uc_protocol/Cargo.toml`, after the `[dependencies]` section, add:

```toml
[features]
# Commit-path latency probes. OFF by default: when off, every `stamp_*` is a
# zero-cost inline no-op and frame layout is unchanged. Enabled only for bench
# builds (e.g. uc_autobench's attribution-bench).
uc-bench-probes = []
```

- [ ] **Step 2: Write probes.rs with a failing unit test**

Create `uc_protocol/src/probes.rs`:

```rust
//! Feature-gated commit-path latency probes. See
//! docs/superpowers/specs/2026-06-03-unified-benchmark-harness-design.md §3.
//!
//! Callers in uc_node / uc_service / uc_client invoke `stamp_*`/`bridge`
//! unconditionally. Without the `uc-bench-probes` feature the bodies are empty
//! `#[inline(always)]` no-ops. With the feature, timestamps land in a
//! process-local sink keyed by the correlation ids that already flow through
//! the system — valid only for the single-process in-process fixture.

/// Commit-path checkpoints, in path order. Used to index a per-request row.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Checkpoint {
    Submit = 0,
    NodeDequeue = 1,
    JournalAppended = 2,
    JournalFsynced = 3,
    ApplyEnqueue = 4,
    ApplyStart = 5,
    ApplyDone = 6,
    RespDequeue = 7,
    Broadcast = 8,
    ClientRecv = 9,
}

/// Number of checkpoints (length of a per-request stamp row).
pub const N_CHECKPOINTS: usize = 10;

#[cfg(not(feature = "uc-bench-probes"))]
mod imp {
    use super::Checkpoint;
    #[inline(always)]
    pub fn stamp_client(_client_id: u32, _local_seq: u32, _cp: Checkpoint) {}
    #[inline(always)]
    pub fn stamp_log(_log_index: u64, _cp: Checkpoint) {}
    #[inline(always)]
    pub fn bridge(_client_id: u32, _local_seq: u32, _log_index: u64) {}
}

#[cfg(feature = "uc-bench-probes")]
mod imp {
    use super::{Checkpoint, N_CHECKPOINTS};
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::OnceLock;
    use std::time::Instant;

    type Row = [Option<u64>; N_CHECKPOINTS];

    struct Sink {
        base: Instant,
        /// Keyed by (client_id<<32 | local_seq): Submit/NodeDequeue/Broadcast/ClientRecv.
        client_rows: Mutex<HashMap<u64, Row>>,
        /// Keyed by log_index: the journal + apply stages.
        log_rows: Mutex<HashMap<u64, Row>>,
        /// client-key -> log_index, recorded at the dispatcher once both are known.
        bridge: Mutex<HashMap<u64, u64>>,
    }

    static SINK: OnceLock<Sink> = OnceLock::new();

    fn sink() -> &'static Sink {
        SINK.get_or_init(|| Sink {
            base: Instant::now(),
            client_rows: Mutex::new(HashMap::new()),
            log_rows: Mutex::new(HashMap::new()),
            bridge: Mutex::new(HashMap::new()),
        })
    }

    fn now_ns(s: &Sink) -> u64 {
        s.base.elapsed().as_nanos() as u64
    }

    fn client_key(client_id: u32, local_seq: u32) -> u64 {
        ((client_id as u64) << 32) | (local_seq as u64)
    }

    pub fn stamp_client(client_id: u32, local_seq: u32, cp: Checkpoint) {
        let s = sink();
        let t = now_ns(s);
        s.client_rows
            .lock()
            .entry(client_key(client_id, local_seq))
            .or_insert([None; N_CHECKPOINTS])[cp as usize] = Some(t);
    }

    pub fn stamp_log(log_index: u64, cp: Checkpoint) {
        let s = sink();
        let t = now_ns(s);
        s.log_rows
            .lock()
            .entry(log_index)
            .or_insert([None; N_CHECKPOINTS])[cp as usize] = Some(t);
    }

    pub fn bridge(client_id: u32, local_seq: u32, log_index: u64) {
        let s = sink();
        s.bridge
            .lock()
            .insert(client_key(client_id, local_seq), log_index);
    }

    /// Clear all captured stamps. Call before a measured run.
    pub fn reset() {
        let s = sink();
        s.client_rows.lock().clear();
        s.log_rows.lock().clear();
        s.bridge.lock().clear();
    }

    /// Drain and join client-keyed + log-keyed rows into one row per request.
    /// Requests missing a bridge entry or a matching log row are dropped.
    pub fn drain_joined() -> Vec<Row> {
        let s = sink();
        let client_rows = std::mem::take(&mut *s.client_rows.lock());
        let log_rows = std::mem::take(&mut *s.log_rows.lock());
        let bridge = std::mem::take(&mut *s.bridge.lock());
        let mut out = Vec::new();
        for (ckey, crow) in client_rows {
            let Some(&li) = bridge.get(&ckey) else { continue };
            let Some(lrow) = log_rows.get(&li) else { continue };
            let mut merged = crow;
            for i in 0..N_CHECKPOINTS {
                if merged[i].is_none() {
                    merged[i] = lrow[i];
                }
            }
            out.push(merged);
        }
        out
    }

    /// Named per-stage deltas (ns) for one joined row. Stages whose endpoints
    /// are missing, or where end < start, are omitted.
    pub fn stage_deltas(row: &Row) -> Vec<(&'static str, u64)> {
        use Checkpoint::*;
        const STAGES: &[(&str, Checkpoint, Checkpoint)] = &[
            ("submit_to_node", Submit, NodeDequeue),
            ("node_to_append", NodeDequeue, JournalAppended),
            ("journal_fsync", JournalAppended, JournalFsynced),
            ("commit_to_apply_enq", JournalFsynced, ApplyEnqueue),
            ("apply_ring", ApplyEnqueue, ApplyStart),
            ("apply", ApplyStart, ApplyDone),
            ("resp_ring", ApplyDone, RespDequeue),
            ("resp_to_broadcast", RespDequeue, Broadcast),
            ("broadcast_to_client", Broadcast, ClientRecv),
            ("total", Submit, ClientRecv),
        ];
        let mut out = Vec::with_capacity(STAGES.len());
        for (name, a, b) in STAGES {
            if let (Some(ta), Some(tb)) = (row[*a as usize], row[*b as usize]) {
                if tb >= ta {
                    out.push((*name, tb - ta));
                }
            }
        }
        out
    }
}

pub use imp::{bridge, stamp_client, stamp_log};

#[cfg(feature = "uc-bench-probes")]
pub use imp::{drain_joined, reset, stage_deltas};

#[cfg(all(test, feature = "uc-bench-probes"))]
mod tests {
    use super::*;

    #[test]
    fn join_and_stage_deltas_cover_full_path() {
        reset();
        // One request: client_id=7, local_seq=0, log_index=100.
        stamp_client(7, 0, Checkpoint::Submit);
        stamp_client(7, 0, Checkpoint::NodeDequeue);
        bridge(7, 0, 100);
        stamp_log(100, Checkpoint::JournalAppended);
        stamp_log(100, Checkpoint::JournalFsynced);
        stamp_log(100, Checkpoint::ApplyEnqueue);
        stamp_log(100, Checkpoint::ApplyStart);
        stamp_log(100, Checkpoint::ApplyDone);
        stamp_log(100, Checkpoint::RespDequeue);
        stamp_client(7, 0, Checkpoint::Broadcast);
        stamp_client(7, 0, Checkpoint::ClientRecv);

        let rows = drain_joined();
        assert_eq!(rows.len(), 1, "one joined request");
        let row = &rows[0];
        for i in 0..N_CHECKPOINTS {
            assert!(row[i].is_some(), "checkpoint {i} present after join");
        }
        let deltas = stage_deltas(row);
        let names: Vec<&str> = deltas.iter().map(|(n, _)| *n).collect();
        assert!(names.contains(&"journal_fsync"));
        assert!(names.contains(&"apply"));
        assert!(names.contains(&"total"));
        // total spans the whole path: >= every sub-stage individually.
        let total = deltas.iter().find(|(n, _)| *n == "total").unwrap().1;
        for (name, d) in &deltas {
            if *name != "total" {
                assert!(*d <= total, "{name} delta {d} <= total {total}");
            }
        }
    }

    #[test]
    fn request_without_bridge_is_dropped() {
        reset();
        stamp_client(9, 1, Checkpoint::Submit);
        stamp_client(9, 1, Checkpoint::ClientRecv);
        // No bridge, no log row.
        assert!(drain_joined().is_empty());
    }
}
```

Add to `uc_protocol/src/lib.rs` (in the `pub mod` list):

```rust
pub mod probes;
```

- [ ] **Step 3: Run the test to verify it fails without the feature, passes with it**

Run: `cargo test -p uc_protocol probes`
Expected: compiles; the `tests` module is `#[cfg(feature = "uc-bench-probes")]`, so 0 probe tests run (filtered out). This confirms the no-op build compiles.

Run: `cargo test -p uc_protocol --features uc-bench-probes probes`
Expected: PASS — `join_and_stage_deltas_cover_full_path` and `request_without_bridge_is_dropped` green.

- [ ] **Step 4: Verify the no-op build is clean**

Run: `cargo build -p uc_protocol`
Expected: builds; no unused-import or dead-code warnings (the `not(feature)` `imp` module is fully used via `pub use`).

Run: `cargo clippy -p uc_protocol --features uc-bench-probes -- -D warnings`
Expected: zero warnings.

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/Cargo.toml uc_protocol/src/probes.rs uc_protocol/src/lib.rs
git commit -m "feat(protocol): uc-bench-probes feature + commit-path probe sink"
```

---

> **Tasks 1.2–1.6 are mechanical instrumentation.** Each adds one or two `stamp_*` calls at an exact site. They are individually verified by `cargo build` in *both* feature modes (the no-op path must keep compiling). Their *behavioral* acceptance is the end-to-end test in Task 1.7, which asserts every checkpoint is populated after a real run. Add the `use uc_protocol::probes::Checkpoint;` import to each file you touch.

### Task 1.2: SUBMIT + CLIENT_RECV probes (uc_client)

**Files:**
- Modify: `uc_client/src/client.rs`
- Modify: `uc_client/src/rings.rs`

- [ ] **Step 1: Stamp SUBMIT in client.rs**

In `uc_client/src/client.rs::submit`, immediately after `local_seq` is computed (the `let local_seq = self.next_local_seq.fetch_add(1, Ordering::Relaxed);` line) and before the frame is written, add:

```rust
        uc_protocol::probes::stamp_client(
            self.cnc.client_id,
            local_seq,
            uc_protocol::probes::Checkpoint::Submit,
        );
```

- [ ] **Step 2: Stamp CLIENT_RECV in rings.rs**

In `uc_client/src/rings.rs`, in the broadcast reader after `let (cid, local_seq) = decode_extra_client(rec.header_extra);` and the `if cid != my_client_id { continue; }` filter — i.e. once we know this response is ours, before delivering it — add:

```rust
        uc_protocol::probes::stamp_client(
            cid,
            local_seq,
            uc_protocol::probes::Checkpoint::ClientRecv,
        );
```

- [ ] **Step 3: Verify both build modes compile**

Run: `cargo build -p uc_client`
Expected: clean (no-op probes).

Run: `cargo build -p uc_client --features uc_protocol/uc-bench-probes`
Expected: clean (real probes).

- [ ] **Step 4: Commit**

```bash
git add uc_client/src/client.rs uc_client/src/rings.rs
git commit -m "feat(client): SUBMIT and CLIENT_RECV commit-path probes"
```

---

### Task 1.3: NODE_DEQUEUE + bridge + BROADCAST probes (client_dispatcher)

**Files:**
- Modify: `uc_node/src/ipc/client_dispatcher.rs`

- [ ] **Step 1: Stamp NODE_DEQUEUE on submit dequeue**

In `uc_node/src/ipc/client_dispatcher.rs`, in the `Ok(Some(rec)) if rec.msg_type == MSG_TYPE_SUBMIT` arm, right after `let extra = rec.header_extra;`, decode the correlation id and stamp:

```rust
            let (probe_cid, probe_seq) = uc_protocol::frames::client::decode_extra_client(extra);
            uc_protocol::probes::stamp_client(
                probe_cid,
                probe_seq,
                uc_protocol::probes::Checkpoint::NodeDequeue,
            );
```

(Use the existing `decode_extra_client` path already imported in this file; if it is imported under a shorter name, call that instead — the goal is `(client_id, local_seq)` from `extra`.)

- [ ] **Step 2: Record the bridge after client_write returns**

In the `Ok(resp) =>` arm of `match raft.client_write(app_command).await`, before `broadcast_record(...)`, record the client-key → log_index bridge:

```rust
                    uc_protocol::probes::bridge(probe_cid, probe_seq, resp.log_id.index);
```

- [ ] **Step 3: Stamp BROADCAST after the response is published**

Immediately after the `broadcast_record(...).await;` call in that same arm, add:

```rust
                    uc_protocol::probes::stamp_client(
                        probe_cid,
                        probe_seq,
                        uc_protocol::probes::Checkpoint::Broadcast,
                    );
```

- [ ] **Step 4: Verify both build modes compile**

Run: `cargo build -p uc_node --features test-support`
Expected: clean (no-op probes).

Run: `cargo build -p uc_node --features test-support,uc_protocol/uc-bench-probes`
Expected: clean (real probes; `resp.log_id.index` resolves on `ClientWriteResponse<TypeConfig>`).

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/ipc/client_dispatcher.rs
git commit -m "feat(node): NODE_DEQUEUE, bridge, BROADCAST commit-path probes"
```

---

### Task 1.4: JOURNAL_APPENDED + JOURNAL_FSYNCED probes (log_storage)

**Files:**
- Modify: `uc_node/src/raft/log_storage.rs`

- [ ] **Step 1: Stamp JOURNAL_APPENDED per entry and capture the last index**

In `uc_node/src/raft/log_storage.rs::append`, in the loop that calls `self.journal.append(seq, term, &payload)`, after the successful append (`last_notifier = Some(notifier);`), stamp and remember the index:

```rust
            uc_protocol::probes::stamp_log(seq, uc_protocol::probes::Checkpoint::JournalAppended);
            probe_last_seq = Some(seq);
```

Declare `probe_last_seq` before the loop (alongside `last_notifier`):

```rust
        let mut probe_last_seq: Option<u64> = None;
```

- [ ] **Step 2: Stamp JOURNAL_FSYNCED in the fsync completion callback**

In the `notifier.on_complete(move |result| { ... })` closure (the one that calls `callback.io_completed(...)`), capture `probe_last_seq` into the closure and stamp on completion. Update the closure to move `probe_last_seq` in and add the stamp before `callback.io_completed(io_result);`:

```rust
        if let (Some(notifier), Some(probe_seq)) = (last_notifier, probe_last_seq) {
            notifier.on_complete(move |result| {
                uc_protocol::probes::stamp_log(
                    probe_seq,
                    uc_protocol::probes::Checkpoint::JournalFsynced,
                );
                let io_result: Result<(), io::Error> = result.map_err(io::Error::other);
                callback.io_completed(io_result);
            });
        }
```

If the existing code unconditionally has `Some(notifier)`, keep its original control flow but fold the `probe_seq` capture in; in the no-op build `probe_last_seq` is still tracked but the stamp compiles away, so behavior is unchanged. (Note: `seq` is the journal sequence = the raft log index, matching the `log_index` keyspace used by the apply-path stamps.)

- [ ] **Step 3: Verify both build modes compile**

Run: `cargo build -p uc_node --features test-support`
Expected: clean.

Run: `cargo build -p uc_node --features test-support,uc_protocol/uc-bench-probes`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/raft/log_storage.rs
git commit -m "feat(node): JOURNAL_APPENDED and JOURNAL_FSYNCED probes"
```

---

### Task 1.5: APPLY_ENQUEUE + RESP_DEQUEUE probes (state_machine_shmem)

**Files:**
- Modify: `uc_node/src/raft/state_machine_shmem.rs`

- [ ] **Step 1: Stamp APPLY_ENQUEUE in publish_apply**

In `uc_node/src/raft/state_machine_shmem.rs::publish_apply`, in the `Ok(())` arm of the `match result` (i.e. after `try_write` succeeds), before `return Ok(())`, add:

```rust
            Ok(()) => {
                uc_protocol::probes::stamp_log(
                    log_index,
                    uc_protocol::probes::Checkpoint::ApplyEnqueue,
                );
                return Ok(());
            }
```

- [ ] **Step 2: Stamp RESP_DEQUEUE in await_apply_resp**

In `await_apply_resp`, in the `Ok(Some(rec)) if rec.msg_type == MSG_TYPE_APPLY_RESP` arm, after the `li != expected_log_index` check passes and before `return Ok(...)`, add:

```rust
                uc_protocol::probes::stamp_log(
                    expected_log_index,
                    uc_protocol::probes::Checkpoint::RespDequeue,
                );
```

- [ ] **Step 3: Verify both build modes compile**

Run: `cargo build -p uc_node --features test-support`
Expected: clean.

Run: `cargo build -p uc_node --features test-support,uc_protocol/uc-bench-probes`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add uc_node/src/raft/state_machine_shmem.rs
git commit -m "feat(node): APPLY_ENQUEUE and RESP_DEQUEUE probes"
```

---

### Task 1.6: APPLY_START + APPLY_DONE probes (apply_loop)

**Files:**
- Modify: `uc_service/src/runtime/apply_loop.rs`

- [ ] **Step 1: Stamp APPLY_START and APPLY_DONE around the user apply**

In `uc_service/src/runtime/apply_loop.rs::apply_thread_body`, in the `Ok(Some(rec)) if rec.msg_type == MSG_TYPE_APPLY` arm, after `let log_index = decode_extra_apply(rec.header_extra);`, stamp the start; then stamp done right after `guard.apply(...)` returns:

```rust
                let log_index = decode_extra_apply(rec.header_extra);
                uc_protocol::probes::stamp_log(
                    log_index,
                    uc_protocol::probes::Checkpoint::ApplyStart,
                );
                let (cmd, _) = bincode::serde::decode_from_slice::<S::Command, _>(
                    &payload_buf,
                    bincode_standard(),
                )?;
                let resp = {
                    let mut guard = sm.blocking_write();
                    guard.apply(log_index, cmd)
                };
                uc_protocol::probes::stamp_log(
                    log_index,
                    uc_protocol::probes::Checkpoint::ApplyDone,
                );
```

(Match the surrounding code's exact existing lines; only the two `stamp_log` calls are added. `ApplyStart` is stamped after dequeue+decode, before the bincode decode of the command, so it captures decode + apply; if you want apply-only, move the `ApplyStart` stamp to just before `guard.apply`. For v1 keep it as shown — decode cost is part of the apply stage budget.)

- [ ] **Step 2: Verify both build modes compile**

Run: `cargo build -p uc_service`
Expected: clean.

Run: `cargo build -p uc_service --features uc_protocol/uc-bench-probes`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add uc_service/src/runtime/apply_loop.rs
git commit -m "feat(service): APPLY_START and APPLY_DONE probes"
```

---

### Task 1.7: attribution-bench binary + end-to-end acceptance test

**Files:**
- Modify: `Cargo.toml` (workspace)
- Modify: `uc_autobench/Cargo.toml`
- Create: `uc_autobench/src/bin/attribution-bench.rs`
- Create: `uc_autobench/tests/attribution_probes.rs`

- [ ] **Step 1: Add workspace + crate dependencies and the feature/bin**

In the workspace `Cargo.toml` `[workspace.dependencies]`, add:

```toml
hdrhistogram = "7"
```

In `uc_autobench/Cargo.toml`, add the feature, deps, and bin target:

```toml
[features]
# Turns on uc_protocol's probe sink across the whole dependency graph (feature
# unification makes uc_node/uc_service/uc_client use the same instrumented build).
uc-bench-probes = ["uc_protocol/uc-bench-probes"]

[dependencies]
# ... existing deps unchanged ...
hdrhistogram = { workspace = true }
futures = { workspace = true }

[[bin]]
name = "attribution-bench"
path = "src/bin/attribution-bench.rs"
```

- [ ] **Step 2: Write the end-to-end acceptance test FIRST**

Create `uc_autobench/tests/attribution_probes.rs`:

```rust
//! Acceptance test for the full-path probe wiring (Tasks 1.2–1.6). Runs real
//! requests through the in-process fixture and asserts every checkpoint is
//! captured and joins into a complete per-request row. Only compiled with the
//! probe feature.
#![cfg(feature = "uc-bench-probes")]

use std::io::{Read, Write};

use futures::stream::{self, StreamExt};
use uc_node::test_support::ClusterFixture;
use uc_service::StateMachine;
use uc_service::SnapshotError;

#[derive(Default)]
struct Echo {
    counter: u64,
    last_applied: Option<u64>,
}

impl StateMachine for Echo {
    type Command = Vec<u8>;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, log_index: u64, cmd: Vec<u8>) -> u64 {
        self.counter = self.counter.wrapping_add(cmd.len() as u64);
        self.last_applied = Some(log_index);
        self.counter
    }
    fn query(&self, _: ()) -> u64 {
        self.counter
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
    fn build_snapshot(&self, _: &mut dyn Write) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
    fn install_snapshot(&mut self, _: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn full_path_probes_capture_every_checkpoint() {
    let fixture = ClusterFixture::<Echo>::single_node(1)
        .await
        .expect("spawn single-node cluster");
    let client = fixture.client(0);

    uc_protocol::probes::reset();

    const N: usize = 64;
    let payload = vec![0u8; 64];
    stream::iter(0..N)
        .map(|_| {
            let p = payload.clone();
            async move {
                let _r: u64 = client.submit(&p).await.expect("submit");
            }
        })
        .buffer_unordered(8)
        .for_each(|_| async {})
        .await;

    let rows = uc_protocol::probes::drain_joined();
    assert!(
        rows.len() >= N - 2,
        "expected ~{N} joined rows, got {}",
        rows.len()
    );
    // Every joined row must have all checkpoints, and `total` must be the
    // largest stage.
    for row in &rows {
        for i in 0..uc_protocol::probes::N_CHECKPOINTS {
            assert!(row[i].is_some(), "checkpoint {i} missing in a joined row");
        }
        let deltas = uc_protocol::probes::stage_deltas(row);
        let total = deltas.iter().find(|(n, _)| *n == "total").unwrap().1;
        assert!(total > 0, "total latency must be positive");
    }

    fixture.shutdown().await.expect("shutdown");
}
```

- [ ] **Step 3: Run the acceptance test — expect it to pass (all wiring is in place)**

Run: `cargo test -p uc_autobench --features uc-bench-probes --test attribution_probes -- --test-threads=1`
Expected: PASS. (`--test-threads=1` because the probe sink is a process-global; a single test runs alone here, but pin it to avoid cross-test interference if more are added.)

If it FAILS with missing checkpoints, the failure names which index is `None` — map index→checkpoint via the `Checkpoint` enum order and fix the corresponding wiring task (1.2–1.6).

- [ ] **Step 4: Write the attribution-bench binary**

Create `uc_autobench/src/bin/attribution-bench.rs`:

```rust
//! Full-path latency attribution: drives the in-process fixture under bounded
//! concurrency, drains the probe sink, and writes per-stage percentiles to
//! attribution.csv. Build with `--features uc-bench-probes`.
//!
//! Storage axis: this binary does NOT relocate the journal itself. Control
//! tmpfs vs disk by exporting TMPDIR before running (the fixture's TempDir
//! honors it), and pass the matching --config label, e.g.:
//!   TMPDIR=/dev/shm cargo run -p uc_autobench --features uc-bench-probes \
//!     --bin attribution-bench --release -- --config single_tmpfs --inflight 8

use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;

use clap::Parser;
use futures::stream::{self, StreamExt};
use hdrhistogram::Histogram;
use uc_node::test_support::ClusterFixture;
use uc_service::{SnapshotError, StateMachine};

#[derive(Parser, Debug)]
#[command(name = "attribution-bench")]
struct Args {
    /// CSV `config` label (e.g. single_tmpfs, single_disk).
    #[arg(long, default_value = "single_tmpfs")]
    config: String,
    /// Concurrency depth (in-flight submits).
    #[arg(long, default_value_t = 8)]
    inflight: usize,
    /// Total requests to issue.
    #[arg(long, default_value_t = 5000)]
    count: usize,
    /// Payload size in bytes.
    #[arg(long, default_value_t = 64)]
    payload_bytes: usize,
    /// Output CSV path.
    #[arg(long, default_value = "bench-out/attribution.csv")]
    out: PathBuf,
    /// Warmup requests (not measured).
    #[arg(long, default_value_t = 500)]
    warmup: usize,
}

#[derive(Default)]
struct Echo {
    counter: u64,
    last_applied: Option<u64>,
}

impl StateMachine for Echo {
    type Command = Vec<u8>;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, log_index: u64, cmd: Vec<u8>) -> u64 {
        self.counter = self.counter.wrapping_add(cmd.len() as u64);
        self.last_applied = Some(log_index);
        self.counter
    }
    fn query(&self, _: ()) -> u64 {
        self.counter
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
    fn build_snapshot(&self, _: &mut dyn Write) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
    fn install_snapshot(&mut self, _: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
}

async fn drive(client: &uc_client::Client, n: usize, inflight: usize, payload_bytes: usize) {
    let payload = vec![0u8; payload_bytes];
    stream::iter(0..n)
        .map(|_| {
            let p = payload.clone();
            async move {
                let _r: u64 = client.submit(&p).await.expect("submit");
            }
        })
        .buffer_unordered(inflight)
        .for_each(|_| async {})
        .await;
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = Args::parse();
    let fixture = ClusterFixture::<Echo>::single_node(1)
        .await
        .expect("spawn single-node cluster");
    let client = fixture.client(0);

    // Warmup (prime caches; not measured).
    drive(client, args.warmup, args.inflight, args.payload_bytes).await;

    uc_protocol::probes::reset();
    drive(client, args.count, args.inflight, args.payload_bytes).await;
    let rows = uc_protocol::probes::drain_joined();

    // One histogram per stage name, preserving first-seen order.
    let mut order: Vec<&'static str> = Vec::new();
    let mut hists: std::collections::HashMap<&'static str, Histogram<u64>> =
        std::collections::HashMap::new();
    for row in &rows {
        for (name, delta) in uc_protocol::probes::stage_deltas(row) {
            let h = hists.entry(name).or_insert_with(|| {
                order.push(name);
                Histogram::<u64>::new(3).expect("hist")
            });
            h.record(delta).ok();
        }
    }

    if let Some(parent) = args.out.parent() {
        std::fs::create_dir_all(parent).expect("create out dir");
    }
    let mut f = File::create(&args.out).expect("create csv");
    writeln!(
        f,
        "config,workload,payload_bytes,inflight,stage,p50_ns,p99_ns,p99_9_ns,count"
    )
    .unwrap();
    for name in &order {
        let h = &hists[name];
        writeln!(
            f,
            "{},bytes,{},{},{},{},{},{},{}",
            args.config,
            args.payload_bytes,
            args.inflight,
            name,
            h.value_at_quantile(0.50),
            h.value_at_quantile(0.99),
            h.value_at_quantile(0.999),
            h.len(),
        )
        .unwrap();
    }

    // JSON summary on stdout for run-iter / quick inspection.
    let total = hists.get("total");
    let dominant = order
        .iter()
        .filter(|n| **n != "total")
        .max_by_key(|n| hists[**n].value_at_quantile(0.99))
        .copied()
        .unwrap_or("none");
    let summary = serde_json::json!({
        "n_requests": rows.len(),
        "total_p99_ns": total.map(|h| h.value_at_quantile(0.99)).unwrap_or(0),
        "dominant_stage": dominant,
        "dominant_stage_p99_ns": hists.get(dominant)
            .map(|h| h.value_at_quantile(0.99)).unwrap_or(0),
        "out": args.out.to_string_lossy(),
    });
    println!("{summary}");

    fixture.shutdown().await.expect("shutdown");
}
```

- [ ] **Step 5: Run the bench and verify the CSV + summary**

Run: `cargo run -p uc_autobench --features uc-bench-probes --bin attribution-bench --release -- --config single_disk --inflight 8 --count 3000`
Expected: a JSON summary line with `n_requests` ≈ 3000, a non-zero `total_p99_ns`, and `dominant_stage` naming the journal/raft bucket (likely `journal_fsync` or `commit_to_apply_enq`).

Run: `cat bench-out/attribution.csv`
Expected: header + one row per stage (`submit_to_node`, `node_to_append`, `journal_fsync`, `commit_to_apply_enq`, `apply_ring`, `apply`, `resp_ring`, `resp_to_broadcast`, `broadcast_to_client`, `total`), each with `count` ≈ 3000.

- [ ] **Step 6: Confirm clippy is clean**

Run: `cargo clippy -p uc_autobench --features uc-bench-probes --bin attribution-bench -- -D warnings`
Expected: zero warnings.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml uc_autobench/Cargo.toml uc_autobench/src/bin/attribution-bench.rs uc_autobench/tests/attribution_probes.rs
git commit -m "feat(autobench): attribution-bench full-path latency decomposition + acceptance test"
```

---

### Task 1.8: Capture reference attribution + consolidate task doc

**Files:**
- Create: `bench-out/reference/attribution.csv`
- Create: `docs/tasks/task08_benchmark_harness.md`

- [ ] **Step 1: Capture a committed reference decomposition**

Run a representative sweep and save it as the baseline to diff future changes against:

```bash
mkdir -p bench-out/reference
cargo run -p uc_autobench --features uc-bench-probes --bin attribution-bench --release -- \
  --config single_disk --inflight 8 --count 5000 --out bench-out/reference/attribution.csv
```

- [ ] **Step 2: Write the consolidated task doc**

Create `docs/tasks/task08_benchmark_harness.md` recording: the two-tier model, the `TaskSpec` seam (Phase 0), the probe mechanism + checkpoint set + the single-process clock-coherence limitation (Phase 1), the three artifacts (`results.tsv`, `bench-out/*.csv` load curves, `bench-out/attribution.csv`), how to run `attribution-bench` (including the TMPDIR storage axis), and the headline reference numbers from Step 1 (which stage dominates and by how much). Note that Phases 2–4 (journal micro, apply micro, multi-node/QUIC) are deferred and will be targeted by the dominant stage this reference identifies.

- [ ] **Step 3: Delete the ephemeral superpowers artifacts for this feature**

Per CLAUDE.md (superpowers artifacts are scaffolding; `docs/tasks/` is the permanent record), remove this plan and its spec once consolidated:

```bash
git rm docs/superpowers/specs/2026-06-03-unified-benchmark-harness-design.md \
       docs/superpowers/plans/2026-06-03-unified-benchmark-harness-phase0-1.md
```

- [ ] **Step 4: Commit**

```bash
git add bench-out/reference/attribution.csv docs/tasks/task08_benchmark_harness.md
git commit -m "docs(autobench): consolidate benchmark-harness task doc + reference attribution"
```

---

## Final verification

- [ ] Default build is clean (probes are no-ops, frame layout unchanged):
  - `cargo build --workspace` → clean
  - `cargo clippy --workspace -- -D warnings` → zero warnings
- [ ] Probe build is correct:
  - `cargo test -p uc_protocol --features uc-bench-probes probes` → green
  - `cargo test -p uc_autobench --features uc-bench-probes --test attribution_probes -- --test-threads=1` → green
- [ ] Existing harness untouched:
  - `cargo run -p uc_autobench --bin run-iter --release -- --task shmem --json` → valid JSON, `status` present
  - `cargo test -p uc_autobench --test ring_torture` → 6 tests green
- [ ] `bench-out/attribution.csv` decomposition matches the established ~38 ms commit-path floor, with the journal/raft bucket dominating — confirming (or correcting) the long-held assumption with measured numbers.
