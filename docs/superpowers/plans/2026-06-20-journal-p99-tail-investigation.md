# Journal depth-1 p99 tail — root-cause investigation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Attribute the journal's ~5.2 ms depth-1-serial `append_consistent_prealloc_p99` tail to one of {device flush, scheduler/C-state wakeup, per-append alloc/fan-out, sampling artifact} and emit a go/no-go on the proposed `SeqWatermark` transplant — without shipping the transplant unless the evidence demands it.

**Architecture:** Two small, throwaway-grade instrumentation additions built and tested locally (Phase 1), then a tiered measurement runbook executed on the AWS c6id fleet with a pre-registered decision rule that stops at the first decisive tier (Phase 2). The investigation's only durable artifact is a benchmark report; the transplant is a gated Tier-3 prototype, not a merge.

**Tech Stack:** Rust (criterion-free raw-sample microbenches via `batched_samples_ns`/`percentile`), `ultima_journal` (in-tree workspace member of ultima_cluster), `ultima_db` `BenchWal` (sibling repo `../ultima_db`), Linux `perf` (`perf sched`), `cpupower`, `taskset`, AWS EC2 `c6id.4xlarge` via `bench-infra`.

## Global Constraints

- **No production code change ships in this investigation.** Instrumentation is investigation-only; the Tier-3 transplant is a prototype + A/B on a throwaway branch, never merged here.
- **No default-flip of WAL preallocation** — `WalSinkKind::CoalescedPrealloc` stays opt-in.
- **No shared journal/store-WAL preallocation code** — the journal left the ultima_db workspace; keep them decoupled.
- **Real disk only.** All fsync-bearing measurement runs on the c6id local NVMe `/opt/bench` (`/dev/nvme1n1`, ext4). `fsync` on tmpfs is a no-op and makes every number meaningless; both harnesses already assert non-tmpfs via `diskcheck::assert_real_disk`.
- **The fleet costs money.** Fleet bring-up (`make -C bench-infra up`) is a cost gate requiring explicit operator confirmation; tear down (`make -C bench-infra destroy`) the moment the verdict is recorded.
- **Build on the fleet as root** with the root-owned toolchain:
  `sudo env PATH=/opt/bench/.cargo/bin:/usr/bin:/bin CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup CARGO_TARGET_DIR=<dir> cargo ...`
- **Pre-registered decision rules are binding** — record the verdict the data dictates, even when it kills the transplant.

---

## File Structure

**Phase 1 — instrumentation (local, committable without the fleet):**

- `uc_autobench/src/journal_bench.rs` (modify) — add an env-gated raw-per-sample dump to the `append_consistent_prealloc` arm so the ~4 slow samples can be located in a `perf` trace by timestamp.
- `uc_autobench/src/sample_dump.rs` (create) — tiny `dump_samples(path, &[f64])` helper + unit test. Shared by the journal arm; mirrored format in the WAL bin.
- `../ultima_db/autobench/src/bin/wal-depth1-microbench.rs` (create) — matched store-WAL depth-1 serial single-commit p99 microbench: same 400 samples, same `CoalescedPrealloc` path, same raw-dump format as the journal arm.
- `../ultima_db/autobench/src/wal_depth1.rs` (create) — the testable loop function the bin calls (so the bin's `main` stays a thin shell).

**Phase 2 — measurement (runbook; deliverable is recorded numbers + verdict):**

- `docs/benchmarks/journal-p99-tail-investigation-2026-06-20.md` (create) — the report: c6id Tier-0 numbers, slow-sample classification, knob result, journal-vs-WAL matched comparison, one-line verdict → transplant decision.

---

## Phase 1 — Instrumentation (local)

### Task 1: Raw-per-sample dump helper (journal side)

**Files:**
- Create: `uc_autobench/src/sample_dump.rs`
- Modify: `uc_autobench/src/lib.rs` (add `pub mod sample_dump;`)
- Test: `uc_autobench/src/sample_dump.rs` (inline `#[cfg(test)]`)

**Interfaces:**
- Produces: `pub fn dump_samples(path: &std::path::Path, samples: &[f64]) -> std::io::Result<()>` — writes one `f64` nanosecond sample per line, in order. `pub fn dump_path_from_env(key: &str) -> Option<std::path::PathBuf>` — returns a path if env var `key` is set and non-empty.

- [ ] **Step 1: Write the failing test**

```rust
// uc_autobench/src/sample_dump.rs
//! Investigation-only: dump raw per-sample latencies so a percentile tail can
//! be correlated against a `perf` trace by ordinal/timestamp. Not used by the
//! autoresearch loop; gated behind an env var at the call site.

use std::io::Write;
use std::path::{Path, PathBuf};

pub fn dump_samples(path: &Path, samples: &[f64]) -> std::io::Result<()> {
    let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
    for s in samples {
        writeln!(f, "{s}")?;
    }
    f.flush()
}

pub fn dump_path_from_env(key: &str) -> Option<PathBuf> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Some(PathBuf::from(v)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dump_writes_one_line_per_sample_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("s.txt");
        dump_samples(&p, &[10.0, 20.5, 30.0]).unwrap();
        let body = std::fs::read_to_string(&p).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines, ["10", "20.5", "30"]);
    }

    #[test]
    fn env_path_none_when_unset_or_empty() {
        // A key that is not set returns None.
        assert!(dump_path_from_env("UC_DUMP_DEFINITELY_UNSET_KEY").is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p uc_autobench sample_dump`
Expected: FAIL — `error[E0583]: file not found for module \`sample_dump\`` (module not yet declared in `lib.rs`).

- [ ] **Step 3: Wire the module**

Add to `uc_autobench/src/lib.rs` alongside the other `pub mod` lines:

```rust
pub mod sample_dump;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p uc_autobench sample_dump`
Expected: PASS — `test result: ok. 2 passed`.

- [ ] **Step 5: Commit**

```bash
git add uc_autobench/src/sample_dump.rs uc_autobench/src/lib.rs
git commit -m "test(journal-bench): add raw per-sample dump helper for p99 tail correlation"
```

---

### Task 2: Wire the dump into the journal `append_consistent_prealloc` arm

**Files:**
- Modify: `uc_autobench/src/journal_bench.rs:194-210` (the `append_consistent_prealloc` block)
- Test: manual run (the arm's behavior is exercised by the existing `microbench_smoke` test; this step adds an opt-in side effect with no metric change)

**Interfaces:**
- Consumes: `crate::sample_dump::{dump_samples, dump_path_from_env}` (Task 1).
- Produces: when `UC_JOURNAL_DUMP_PREALLOC` is set to a path, the arm writes its raw 400-sample latency vector there; metrics emitted are unchanged.

- [ ] **Step 1: Add the dump after the samples are collected**

In `uc_autobench/src/journal_bench.rs`, locate the `append_consistent_prealloc` block (currently `journal_bench.rs:194-210`). After `let mut s = batched_samples_ns(...)` collects the vector and **before** the `percentile` calls consume it, insert:

```rust
        // Investigation-only (UC_JOURNAL_DUMP_PREALLOC=path): persist the raw
        // per-sample latencies so the ~4 samples above p99 (4th-worst of 400)
        // can be located in a `perf sched` trace by ordinal. No effect on the
        // emitted percentiles; absent the env var this is a no-op.
        if let Some(p) = crate::sample_dump::dump_path_from_env("UC_JOURNAL_DUMP_PREALLOC") {
            crate::sample_dump::dump_samples(&p, &s).expect("dump prealloc samples");
        }
```

(`s` is `Vec<f64>`; `percentile` takes `&mut [f64]` and sorts in place, so the dump must run **before** the `percentile(&mut s, 50.0)` / `percentile(&mut s, 99.0)` calls to preserve sample order.)

- [ ] **Step 2: Verify it compiles and the smoke test still passes**

Run: `cargo test -p uc_autobench --bin journal-microbench 2>/dev/null; cargo build -p uc_autobench`
Expected: clean build. (No new test; the metric output is unchanged.)

- [ ] **Step 3: Manually verify the dump fires (quick config, local tmp is fine here — no fsync correctness needed for the dump-shape check)**

Run:
```bash
AUTOBENCH_QUICK=1 UC_JOURNAL_DUMP_PREALLOC=/tmp/jdump.txt \
  cargo run -p uc_autobench --bin journal-microbench --release 2>/dev/null >/dev/null
wc -l /tmp/jdump.txt
```
Expected: `20 /tmp/jdump.txt` (quick config = 20 consistent samples). Standard config will emit 400.

- [ ] **Step 4: Commit**

```bash
git add uc_autobench/src/journal_bench.rs
git commit -m "feat(journal-bench): opt-in raw-sample dump on append_consistent_prealloc arm"
```

---

### Task 3: Matched store-WAL depth-1 microbench loop (`../ultima_db`)

**Files:**
- Create: `../ultima_db/autobench/src/wal_depth1.rs`
- Modify: `../ultima_db/autobench/src/lib.rs` (add `pub mod wal_depth1;`)
- Test: `../ultima_db/autobench/src/wal_depth1.rs` (inline `#[cfg(test)]`)

**Interfaces:**
- Consumes: `ultima_db::wal::{BenchWal, WalEntry, WalOp, WalSinkKind}`; `crate::sampling::{batched_samples_ns, percentile}`; `crate::diskcheck::{bench_root, assert_real_disk}`.
- Produces: `pub fn measure_wal_depth1_prealloc(samples: usize, payload: usize, allow_tmpfs: bool) -> Vec<f64>` — runs `samples` serial `Consistent` single-commit `CoalescedPrealloc` WAL commits and returns the raw per-commit latency vector (ns), in order. This is the apples-to-apples analogue of the journal's `append_consistent_prealloc` loop (`uc_autobench/src/journal_bench.rs:194`).

- [ ] **Step 1: Write the failing test**

```rust
// ../ultima_db/autobench/src/wal_depth1.rs
//! Matched store-WAL analogue of the journal's append_consistent_prealloc arm:
//! depth-1 serial single durable commits on a CoalescedPrealloc WAL, raw
//! per-sample timings, same sample count as the journal microbench. Used by the
//! p99-tail investigation to rule out a sampling/scheduling artifact common to
//! both engines (a 400-sample p99 is the 4th-worst sample — far more tail-
//! sensitive than the YCSB aggregate the store WAL's "no tail" was inferred from).

use std::time::Instant;

use ultima_db::wal::{BenchWal, WalEntry, WalOp, WalSinkKind};

use crate::diskcheck;
use crate::sampling::batched_samples_ns;

fn make_entry(version: u64, payload: usize) -> WalEntry {
    WalEntry {
        version,
        ops: vec![WalOp::Insert {
            table: "bench".to_string(),
            id: version,
            data: vec![0u8; payload],
        }],
    }
}

pub fn measure_wal_depth1_prealloc(samples: usize, payload: usize, allow_tmpfs: bool) -> Vec<f64> {
    let root = diskcheck::bench_root("autobench-wal-depth1");
    diskcheck::assert_real_disk(&root, allow_tmpfs);
    let dir = tempfile::Builder::new()
        .prefix("wd1")
        .tempdir_in(&root)
        .unwrap();
    let wal = BenchWal::new(dir.path(), /* consistent */ true, WalSinkKind::CoalescedPrealloc).unwrap();
    // Prime one commit so the first measured sample is steady-state (mirrors the
    // journal fsync_prealloc arm's warm-up).
    let mut version = 0u64;
    version += 1;
    wal.commit_consistent(make_entry(version, payload)).unwrap();
    batched_samples_ns(samples, 1, || {
        version += 1;
        wal.commit_consistent(make_entry(version, payload)).unwrap();
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_requested_sample_count() {
        // allow_tmpfs=true: this shape test runs anywhere; the fsync-bearing
        // measurement run on the fleet uses allow_tmpfs=false on /opt/bench.
        let s = measure_wal_depth1_prealloc(8, 1024, true);
        assert_eq!(s.len(), 8);
        assert!(s.iter().all(|&x| x >= 0.0));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `../ultima_db`): `cargo test -p ultima-autobench wal_depth1`
Expected: FAIL — `file not found for module \`wal_depth1\`` (not declared in `lib.rs`). If `BenchWal`/`WalSinkKind` are not re-exported from `ultima_db::wal`, this surfaces here too — confirm against `src/wal.rs:1489` (`pub struct BenchWal`) and `WalSinkKind`.

- [ ] **Step 3: Wire the module**

Add to `../ultima_db/autobench/src/lib.rs`:

```rust
pub mod wal_depth1;
```

- [ ] **Step 4: Run test to verify it passes**

Run (from `../ultima_db`): `cargo test -p ultima-autobench wal_depth1 --features persistence`
Expected: PASS — `test result: ok. 1 passed`. (The `persistence` feature gates `BenchWal`; `autobench/Cargo.toml:10` already enables it on the `ultima-db` dep, but pass it explicitly to be safe.)

- [ ] **Step 5: Commit**

```bash
cd ../ultima_db
git add autobench/src/wal_depth1.rs autobench/src/lib.rs
git commit -m "test(wal-bench): matched depth-1 prealloc single-commit microbench loop"
cd -
```

---

### Task 4: Matched store-WAL microbench binary (emits p50/p99 + raw dump)

**Files:**
- Create: `../ultima_db/autobench/src/bin/wal-depth1-microbench.rs`
- Test: manual run

**Interfaces:**
- Consumes: `ultima_autobench::wal_depth1::measure_wal_depth1_prealloc` (Task 3); `ultima_autobench::sampling::percentile`.
- Produces: a binary `wal-depth1-microbench` that prints one JSON object `{"wal_depth1_prealloc_p50_ns":..,"wal_depth1_prealloc_p99_ns":..,"samples":N}` on stdout, and — when `UC_WAL_DUMP_DEPTH1` is a path — writes the raw per-sample vector there in the **same one-value-per-line format** as the journal dump (Task 1), for apples-to-apples comparison.

- [ ] **Step 1: Write the binary**

```rust
// ../ultima_db/autobench/src/bin/wal-depth1-microbench.rs
//! Matched store-WAL depth-1 p99 microbench for the journal-p99-tail
//! investigation. Mirrors uc_autobench's append_consistent_prealloc arm:
//! 400 serial Consistent CoalescedPrealloc single commits, p50/p99, raw dump.

use std::io::Write;

use ultima_autobench::sampling::percentile;
use ultima_autobench::wal_depth1::measure_wal_depth1_prealloc;

fn main() {
    // Standard run = 400 samples (matches journal_bench Config::standard()).
    // AUTOBENCH_QUICK=1 drops to 20 for a fast shape check.
    let quick = std::env::var("AUTOBENCH_QUICK").as_deref() == Ok("1");
    let samples = if quick { 20 } else { 400 };
    let payload = 1024usize;

    let mut s = measure_wal_depth1_prealloc(samples, payload, /* allow_tmpfs */ quick);

    // Same dump format as the journal side (UC_JOURNAL_DUMP_PREALLOC): one f64
    // ns per line, in sample order, BEFORE percentile sorts the vec in place.
    if let Ok(p) = std::env::var("UC_WAL_DUMP_DEPTH1") {
        if !p.is_empty() {
            let mut f = std::io::BufWriter::new(std::fs::File::create(&p).unwrap());
            for x in &s {
                writeln!(f, "{x}").unwrap();
            }
            f.flush().unwrap();
        }
    }

    let p50 = percentile(&mut s, 50.0);
    let p99 = percentile(&mut s, 99.0);
    println!(
        "{{\"wal_depth1_prealloc_p50_ns\":{p50},\"wal_depth1_prealloc_p99_ns\":{p99},\"samples\":{samples}}}"
    );
}
```

- [ ] **Step 2: Build it**

Run (from `../ultima_db`): `cargo build -p ultima-autobench --bin wal-depth1-microbench --features persistence`
Expected: clean build.

- [ ] **Step 3: Manual shape check (quick config; tmpfs allowed for the shape check only)**

Run:
```bash
cd ../ultima_db
AUTOBENCH_QUICK=1 UC_WAL_DUMP_DEPTH1=/tmp/wdump.txt \
  cargo run -p ultima-autobench --bin wal-depth1-microbench --features persistence --release 2>/dev/null
wc -l /tmp/wdump.txt
cd -
```
Expected: a JSON line with `"samples":20`, and `20 /tmp/wdump.txt`.

- [ ] **Step 4: Commit**

```bash
cd ../ultima_db
git add autobench/src/bin/wal-depth1-microbench.rs
git commit -m "feat(wal-bench): matched depth-1 p99 microbench binary with raw dump"
cd -
```

---

## Phase 2 — Fleet measurement (runbook)

> Phase 2 tasks are a **runbook**, not TDD: each task's deliverable is a set of recorded numbers plus a pre-registered decision. Execute top-to-bottom; **stop at the first task whose decision rule is decisive** and jump to Task 9 (report + teardown). Record every number you read into the Task 9 report as you go.

### Task 5: Fleet bring-up + Tier 0 (device-flush check)

**Files:** none (records numbers into the Task 9 report).

- [ ] **Step 1: Bring the fleet up (COST GATE — confirm with operator first)**

Run: `make -C bench-infra up`
Then: `make -C bench-infra inventory` and confirm 3× `c6id.4xlarge` in `bench-infra/inventory/hosts.yml`. Note node0's IP.

- [ ] **Step 2: Sync both repos to node0 and build the journal microbench on the NVMe**

```bash
NODE0=<node0-ip>
ssh -i /home/claude/.ssh/id_ed25519 ubuntu@$NODE0 'mkdir -p /opt/bench/src'
rsync -az -e "ssh -i /home/claude/.ssh/id_ed25519" --exclude target /home/claude/ultima/ultima_cluster ubuntu@$NODE0:/opt/bench/src/
rsync -az -e "ssh -i /home/claude/.ssh/id_ed25519" --exclude target /home/claude/ultima/ultima_db ubuntu@$NODE0:/opt/bench/src/
```

- [ ] **Step 3: Run the journal microbench on the c6id NVMe and read the Tier-0 isolation metrics**

```bash
ssh -i /home/claude/.ssh/id_ed25519 ubuntu@$NODE0 \
 'cd /opt/bench/src/ultima_cluster && \
  sudo env PATH=/opt/bench/.cargo/bin:/usr/bin:/bin CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup \
   CARGO_TARGET_DIR=/opt/bench/target ULTIMA_BENCH_DIR=/opt/bench \
   cargo run -p uc_autobench --bin journal-microbench --release 2>/dev/null'
```
Read from the emitted JSON: `fsync_prealloc_p99_ns`, `write_only_p99_ns`, `append_consistent_prealloc_p99_ns`, `append_consistent_prealloc_p50_ns`. Record all four in the report.

- [ ] **Step 2 DECISION RULE (Tier 0):**
  - If `fsync_prealloc_p99_ns ≈ append_consistent_prealloc_p99_ns` (same order, ~5 ms) → **verdict = `device`** (the bare `sync_data` barrier itself tails; the journal machinery adds nothing). **STOP → Task 9, transplant = no-go.**
  - Else (device flush p99 ≪ 5 ms, e.g. tens of µs) → machinery tail confirmed → proceed to Task 6.

---

### Task 6: Tier 1 — localize the slow samples (`perf sched` + correlation)

**Files:** none (records classification into the Task 9 report).

- [ ] **Step 1: Enable perf for the session**

```bash
ssh -i /home/claude/.ssh/id_ed25519 ubuntu@$NODE0 'sudo sysctl kernel.perf_event_paranoid=-1; perf --version'
```
Expected: a perf version string. If `perf` is missing, install (`sudo apt-get install -y linux-tools-$(uname -r)`); if that fails on the EC2 kernel, fall back to BCC `offcputime-bpfcc` (note the fallback in the report).

- [ ] **Step 2: Run the journal arm under perf sched with the raw dump on**

```bash
ssh -i /home/claude/.ssh/id_ed25519 ubuntu@$NODE0 \
 'cd /opt/bench/src/ultima_cluster && \
  sudo env PATH=/opt/bench/.cargo/bin:/usr/bin:/bin CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup \
   CARGO_TARGET_DIR=/opt/bench/target ULTIMA_BENCH_DIR=/opt/bench UC_JOURNAL_DUMP_PREALLOC=/opt/bench/jdump.txt \
   perf sched record -o /opt/bench/perf.sched -- \
   cargo run -p uc_autobench --bin journal-microbench --release 2>/dev/null'
```

- [ ] **Step 3: Identify the slow samples and inspect their windows**

```bash
ssh -i /home/claude/.ssh/id_ed25519 ubuntu@$NODE0 \
 'sort -g -r /opt/bench/jdump.txt | head -5; echo ---; \
  cd /opt/bench && sudo perf sched timehist -i perf.sched | sort -k4 -g -r | head -20; echo ---; \
  sudo perf sched latency -i /opt/bench/perf.sched | head -20'
```
The `sort -g -r jdump.txt | head -5` gives the ~5 tail latencies (ns). `perf sched latency` gives per-thread max wakeup→run delay; `perf sched timehist` rows with large `wait time`/`sch delay` are the off-CPU stalls. Record the top tail values and the dominant blocking attribution.

- [ ] **Step 4: Classify each tail sample** into exactly one of:
  - **malloc/alloc** — on-CPU in `__libc_malloc`/`mmap`/page-fault inside `append()` (would need `perf record -g --call-graph dwarf` on the same run to confirm a stack);
  - **fdatasync** — in `sync_data` on the writer thread;
  - **off-CPU run-queue** — waiter runnable but `sch delay` ≈ tail magnitude;
  - **idle-C-state-exit** — both threads idle between samples, wake latency ≈ tail magnitude.

  Record the classification. (Prior autoresearch verdict for this path was *park/VM-scheduling-bound*, NOT alloc/lock — expect off-CPU run-queue or C-state to dominate; if so, the transplant — which still parks on a condvar — cannot touch it.)

---

### Task 7: Tier 2 — confirm by knob (pin + C-state disable)

**Files:** none (records into the Task 9 report).

- [ ] **Step 1: Disable deep C-states and re-run pinned**

```bash
ssh -i /home/claude/.ssh/id_ed25519 ubuntu@$NODE0 \
 'sudo cpupower idle-set -D 1 2>&1 | tail -2 || echo "cpupower idle-set unsupported on this instance"; \
  cd /opt/bench/src/ultima_cluster && \
  sudo env PATH=/opt/bench/.cargo/bin:/usr/bin:/bin CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup \
   CARGO_TARGET_DIR=/opt/bench/target ULTIMA_BENCH_DIR=/opt/bench \
   taskset -c 2,3 cargo run -p uc_autobench --bin journal-microbench --release 2>/dev/null'
```
Record the new `append_consistent_prealloc_p99_ns`.

- [ ] **Step 1 fallback:** if `cpupower idle-set` reports unsupported (likely under EC2 virtualization), re-run with thread pinning only AND a busy-poll comparison; note in the report that the C-state knob was unavailable and pinning is the scheduler-isolation signal.

- [ ] **Step 2 DECISION RULE (Tier 2):**
  - If `append_consistent_prealloc_p99_ns` collapses toward p50 under pin/C-state-disable → **verdict = `scheduler-cstate`** → **STOP → Task 9, transplant = no-go** (recommended follow-up: thread pinning / C-state policy, not the watermark transplant).
  - Else (tail persists) → not scheduler/C-state → proceed to Task 8 (store-WAL comparison) before considering Tier 3.

---

### Task 8: Store-WAL matched comparison + YCSB cross-check

**Files:** none (records into the Task 9 report).

- [ ] **Step 1: Run the matched store-WAL depth-1 microbench on the same NVMe**

```bash
ssh -i /home/claude/.ssh/id_ed25519 ubuntu@$NODE0 \
 'cd /opt/bench/src/ultima_db && \
  sudo env PATH=/opt/bench/.cargo/bin:/usr/bin:/bin CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup \
   CARGO_TARGET_DIR=/opt/bench/target ULTIMA_BENCH_DIR=/opt/bench UC_WAL_DUMP_DEPTH1=/opt/bench/wdump.txt \
   cargo run -p ultima-autobench --bin wal-depth1-microbench --features persistence --release 2>/dev/null'
```
Record `wal_depth1_prealloc_p50_ns` and `wal_depth1_prealloc_p99_ns` (400 samples).

- [ ] **Step 2: YCSB-A cross-check (high-sample-count secondary signal)**

```bash
ssh -i /home/claude/.ssh/id_ed25519 ubuntu@$NODE0 \
 'cd /opt/bench/src/ultima_db && \
  sudo env PATH=/opt/bench/.cargo/bin:/usr/bin:/bin CARGO_HOME=/opt/bench/.cargo RUSTUP_HOME=/opt/bench/.rustup \
   CARGO_TARGET_DIR=/opt/bench/target ULTIMA_BENCH_DIR=/opt/bench ULTIMA_BENCH_PREALLOC=1 \
   cargo bench --bench ycsb_bench --features persistence -- A 2>/dev/null | tail -20'
```
Record the YCSB-A ON figure; confirm it remains the tight ~36 ms/iter (no 1%@5 ms inflation).

- [ ] **Step 2 DECISION RULE (sampling artifact):**
  - If `wal_depth1_prealloc_p99_ns` is **also ~5 ms** (same order as the journal) → **verdict = `sampling-artifact`**: a 400-sample p99 captures rare scheduling/device events common to both engines; there is no journal-specific tail. **STOP → Task 9, transplant = no-go.**
  - If the WAL's matched 400-sample p99 stays sub-ms while the journal's is ~5 ms → the tail is genuinely journal-specific → proceed to Task 9's Tier-3 gate.

---

### Task 9: Verdict, report, and teardown

**Files:**
- Create: `docs/benchmarks/journal-p99-tail-investigation-2026-06-20.md`

- [ ] **Step 1: Decide the Tier-3 gate**

Tier 3 (prototype the `SeqWatermark` transplant + A/B `append_consistent_prealloc_p50/p99`) is reached **only if** Tasks 5–8 did not produce a terminal verdict AND Task 6 classified the tail as **malloc/alloc or completion-fan-out** (the only mechanisms the transplant removes — both `Notifier::wait` and `SeqWatermark::wait` park on a condvar, so a park/scheduler tail is out of scope). If reached, prototype on a throwaway branch in `ultima_cluster/ultima_journal` routing `append().wait()` through `SeqWatermark`, re-run the journal microbench, and record before/after. **Verdict = `alloc-fan-out`** only if the p99 materially collapses; transplant = go.

- [ ] **Step 2: Write the report**

Create `docs/benchmarks/journal-p99-tail-investigation-2026-06-20.md` with:
- The c6id Tier-0 table: `write_only_p99`, `fsync_prealloc_p99`, `append_consistent_prealloc_p50/p99`.
- Tier-1 slow-sample classification (top-5 tail values + dominant blocking attribution from `perf sched`).
- Tier-2 knob result (pinned / C-state p99 vs baseline; note if the knob was unavailable).
- Store-WAL matched `wal_depth1_prealloc_p50/p99` + YCSB-A cross-check.
- The one-line **verdict** (`device` | `scheduler-cstate` | `sampling-artifact` | `alloc-fan-out`) and the resulting transplant **go/no-go**, mapped via the spec's verdict table.

- [ ] **Step 3: Commit the report**

```bash
git add docs/benchmarks/journal-p99-tail-investigation-2026-06-20.md
git commit -m "docs(bench): journal depth-1 p99 tail investigation results + transplant verdict"
```

- [ ] **Step 4: Tear the fleet down (COST GATE)**

Run: `make -C bench-infra destroy`
Then confirm: `make -C bench-infra status` shows no live instances.

---

## Self-Review

**Spec coverage:**
- Tier 0 (device check) → Task 5. ✓
- Tier 1 (localize) → Task 6. ✓
- Tier 2 (knob confirm) → Task 7. ✓
- Tier 3 (gated transplant prototype) → Task 9 Step 1. ✓
- Comparative store-WAL matched microbench (decision C primary) → Tasks 3, 4, 8. ✓
- YCSB cross-check (decision C secondary) → Task 8 Step 2. ✓
- Per-sample latency capture (Tier-1 prerequisite) → Tasks 1, 2 (journal), 4 (WAL). ✓
- Pre-registered decision rules / verdict table → decision-rule steps in Tasks 5, 7, 8 + Task 9 report. ✓
- Deliverable report under `docs/benchmarks/` → Task 9. ✓
- Environment/logistics (root build env, /opt/bench NVMe, fleet up/destroy) → Global Constraints + Tasks 5, 9. ✓
- Non-goals (no transplant merge, no default-flip, no shared code) → Global Constraints. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code; commands have expected outputs; the one intentionally conditional task (Tier 3) is explicitly gated, not a placeholder.

**Type consistency:** `dump_samples(&Path, &[f64])` / `dump_path_from_env(&str)` (Task 1) used consistently in Task 2. `measure_wal_depth1_prealloc(usize, usize, bool) -> Vec<f64>` (Task 3) consumed unchanged by the Task 4 binary. Dump file format (one `f64` ns per line, pre-sort) identical across journal (Task 1) and WAL (Task 4). Env var names consistent: `UC_JOURNAL_DUMP_PREALLOC` (journal), `UC_WAL_DUMP_DEPTH1` (WAL), `ULTIMA_BENCH_PREALLOC` (YCSB toggle, per handoff).
