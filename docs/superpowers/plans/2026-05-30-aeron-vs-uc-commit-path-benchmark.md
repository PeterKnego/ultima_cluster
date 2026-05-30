# Aeron IPC vs ultima_cluster Commit-Path Benchmark — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an open-loop, HDR-histogram load-stepping benchmark that measures ultima_cluster's full commit path against Aeron same-host IPC, then produce a layer-decomposed gap analysis and a prioritized UC optimization backlog.

**Architecture:** Two load drivers emitting one shared CSV schema. UC side: a new `uc_autobench` binary (`commit-path-load`) that drives the in-process single-node `ClusterFixture` over a rate ladder with swept in-flight concurrency, recording submit→response latency into an HDR histogram. Aeron side: a new C binary (`c_ipc_pingpong`) adapted from `cping.c`/`cpong.c` pinned to `aeron:ipc`, emitting the same CSV schema. A Python/matplotlib script overlays the curves and renders the decomposition. 3-node (replication layer) is an explicit Phase 2 over the real multi-process path; Phase 1 (single-node, the dominant consensus+fsync layers) ships first.

**Tech Stack:** Rust (`uc_autobench` bin, `tokio` current_thread, `clap`, `hdrhistogram` crate), C (Aeron C client + `hdr_histogram_static`, CMake), Python 3 + matplotlib + pandas.

**Spec:** `docs/superpowers/specs/2026-05-30-aeron-vs-uc-commit-path-benchmark-design.md`

---

## Reference: exact APIs this plan depends on (verified against source)

**`uc_client::Client`** (`uc_client/src/client.rs`):
```rust
pub async fn connect(instance_dir: &Path, app_id: &str) -> Result<Self, ClientError>   // :44
pub async fn submit<C: Serialize, R: DeserializeOwned>(&self, cmd: &C) -> Result<R, ClientError>  // :91 — takes &self
pub async fn query_linearizable<Q: Serialize, QR: DeserializeOwned>(&self, q: &Q) -> Result<QR, ClientError>  // :144
pub async fn query_snapshot<Q: Serialize, QR: DeserializeOwned>(&self, q: &Q) -> Result<QR, ClientError>      // :137
pub async fn shutdown(self) -> Result<(), ClientError>   // :301 — consumes self
```

**`ClusterFixture<S>`** (`uc_node/src/test_support.rs`, behind `uc_node` feature `test-support`) — **single-node only, no builder, no N-node ctor**; instance/journal dirs are fresh `TempDir`s (not caller-settable); `S: StateMachine + Default`:
```rust
pub async fn single_node(n_clients: usize) -> anyhow::Result<Self>   // :61
pub async fn single_node_with_app_id(n_clients: usize, app_id: &str) -> anyhow::Result<Self>  // :67
pub fn client(&self, idx: usize) -> &Client   // :151
pub fn client_count(&self) -> usize           // :156
pub async fn attach_client(&mut self) -> anyhow::Result<usize>  // :162
pub fn instance_path(&self) -> &Path          // :176
pub async fn shutdown(mut self) -> anyhow::Result<()>  // :188
```

**`StateMachine`** (`uc_service/src/state_machine.rs:17`) — note `StoreStateMachine` does NOT impl `Default`, so the fixture cannot use it directly; we hand-write a `Default` KV SM:
```rust
pub trait StateMachine: Send + Sync + 'static {
    type Command:  Serialize + DeserializeOwned + Send + Sync + 'static;
    type Response: Serialize + DeserializeOwned + Send + 'static;
    type Query:    Serialize + DeserializeOwned + Send + Sync + 'static;
    type QueryResponse: Serialize + DeserializeOwned + Send + 'static;
    fn apply(&mut self, log_index: u64, cmd: Self::Command) -> Self::Response;
    fn query(&self, q: Self::Query) -> Self::QueryResponse;
    fn last_applied(&self) -> Option<u64>;
    fn build_snapshot(&self, dst: &mut dyn Write) -> Result<u64, SnapshotError>;
    fn install_snapshot(&mut self, src: &mut dyn Read) -> Result<u64, SnapshotError>;
}
```

**Runtime constraint** (`uc_node/src/test_support.rs:14`, memory `feedback_m3_test_runtime_flavor`): the in-process fixture MUST run under `#[tokio::main(flavor = "current_thread")]`; `multi_thread` intermittently times out the shmem handshake.

**Bench bin emission convention** (`shmem-microbench.rs`, `shmem-e2e.rs`): diagnostics to stderr via `eprintln!`; machine output to stdout. We extend this: this bin writes a CSV file (not a single JSON line), and echoes a human summary to stderr.

**Fsync-target control:** `ClusterFixture` uses `TempDir::new()`, which honors `$TMPDIR`. Point the journal at a RAM disk vs real disk by setting `TMPDIR` before launch (documented in the run script). On macOS there is no tmpfs; a RAM disk is created via `hdiutil`/`diskutil` (run script handles it; if unavailable, the tmpfs run is skipped and the report says so).

**Aeron C ping/pong** (`aeron-samples/src/main/c/cping.c`, `cpong.c`): default channels are UDP; IPC is opt-in via `-c aeron:ipc -C aeron:ipc`. `cping` records `end_ns - start_ns` (stamped `aeron_nano_clock()` in the payload) into an HdrHistogram; `cpong` echoes. Built via `add_executable` + `target_link_libraries(... ${CLIENT_LINK_LIB} hdr_histogram_static)` + `add_dependencies(<t> hdr_histogram)` (CMakeLists.txt:46-88). A media driver must be running for `aeron:ipc`.

---

## File structure

**UC (Phase 1):**
- Create `uc_autobench/src/bin/commit-path-load.rs` — the open-loop load driver + CSV writer + the `Default` KV state machine.
- Modify `uc_autobench/Cargo.toml` — add `hdrhistogram` dep + the `[[bin]]` entry.
- Modify root `Cargo.toml` `[workspace.dependencies]` — add `hdrhistogram = "7"`.
- Create `uc_autobench/scripts/run-uc-single-node.sh` — drives the rate ladder × concurrency sweep × {tmpfs,disk}, writing CSVs to `bench-out/`.

**Aeron:**
- Create `aeron/aeron-samples/src/main/c/c_ipc_pingpong.c` (scratch; not upstreamed) — IPC-pinned ping/pong with the shared CSV schema.
- Modify `aeron/aeron-samples/src/main/c/CMakeLists.txt` — add the `c_ipc_pingpong` target.
- Create `aeron/scripts-scratch/run-aeron-ipc.sh` — launch media driver + the bin over the same rate ladder.

**Analysis:**
- Create `uc_autobench/scripts/plot_decomposition.py` — overlay curves + decomposition bar from CSVs.
- Create `uc_autobench/scripts/requirements.txt` — `matplotlib`, `pandas`.

**Phase 2 (3-node, multi-process):**
- Create `uc_autobench/src/bin/commit-path-load-mp.rs` (or extend Phase 1 bin with a `--connect <instance_dir>` mode that attaches to an already-running cluster instead of spawning the fixture).
- Create `uc_autobench/scripts/run-uc-3node.sh`.

**Report (final):**
- Create `docs/tasks/taskNN_aeron_vs_uc_commit_path.md` (consolidated; superpowers spec+plan deleted per CLAUDE.md workflow on completion).

---

## Shared CSV schema (both sides emit exactly this)

Header line, then one row per (rate-ladder step):
```
system,config,workload,payload_bytes,inflight,target_rate,achieved_rate,p50_ns,p99_ns,p99_9_ns,p99_99_ns,max_ns,count
```
- `system` ∈ {`uc`,`aeron`}; `config` ∈ {`single_tmpfs`,`single_disk`,`3node_loopback`,`ipc`}; `workload` ∈ {`kv`,`bytes`}.

---

## Phase 1 — UC single-node load driver

### Task 1: Add `hdrhistogram` dependency

**Files:**
- Modify: root `Cargo.toml` (`[workspace.dependencies]`)
- Modify: `uc_autobench/Cargo.toml` (`[dependencies]` + new `[[bin]]`)

- [ ] **Step 1: Add to workspace deps**

In root `/Users/peter/Projects/ultima/ultima_cluster/Cargo.toml`, under `[workspace.dependencies]`, after the `dashmap = "6"` line add:
```toml
hdrhistogram = "7"
```

- [ ] **Step 2: Reference it in uc_autobench**

In `uc_autobench/Cargo.toml`, under `[dependencies]`, after `uc_client = { path = "../uc_client" }` add:
```toml
hdrhistogram = { workspace = true }
```
And after the existing `[[bin]]` blocks add:
```toml
[[bin]]
name = "commit-path-load"
path = "src/bin/commit-path-load.rs"
```

- [ ] **Step 3: Verify it resolves**

Run: `cargo metadata --format-version 1 >/dev/null` (from repo root)
Expected: exits 0 (dependency graph resolves; downloads `hdrhistogram`).

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml uc_autobench/Cargo.toml
git commit -m "bench: add hdrhistogram dep + commit-path-load bin target"
```

### Task 2: KV state machine (Default-able) with a unit test

The fixture needs `S: StateMachine + Default`; `StoreStateMachine` isn't `Default`. Build a minimal in-memory KV SM (HashMap) — represents the "real apply" workload cost without the `ultima_db`/`begin_write` Default-construction problem. (A follow-up task can swap in `ultima_db::Store` once an instance-dir-injection path exists; the spec's KV intent is satisfied by a real keyed write+read apply.)

**Files:**
- Create: `uc_autobench/src/bin/commit-path-load.rs` (start the file with the SM + its test)

- [ ] **Step 1: Write the failing test**

Create `uc_autobench/src/bin/commit-path-load.rs` with:
```rust
//! commit-path-load — open-loop load driver for UC's full single-node commit
//! path. Drives a rate ladder × in-flight-concurrency sweep against an
//! in-process `ClusterFixture`, recording submit→response latency in an HDR
//! histogram and writing one CSV row per ladder step.
//!
//! Runtime MUST be current_thread (memory feedback_m3_test_runtime_flavor):
//! a multi_thread runtime intermittently times out the shmem handshake.

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use uc_service::{SnapshotError, StateMachine};

/// KV command: write `val` at `key`. Serializable so it rides Client::submit.
#[derive(Serialize, Deserialize)]
enum KvCmd {
    Put { key: u64, val: Vec<u8> },
}

/// In-memory KV state machine. Default-able so it works with ClusterFixture.
#[derive(Default)]
struct KvSm {
    map: std::collections::HashMap<u64, Vec<u8>>,
    last_applied: Option<u64>,
}

impl StateMachine for KvSm {
    type Command = KvCmd;
    type Response = u64; // returns current map.len()
    type Query = u64; // key to read
    type QueryResponse = Option<Vec<u8>>;

    fn apply(&mut self, log_index: u64, cmd: KvCmd) -> u64 {
        match cmd {
            KvCmd::Put { key, val } => {
                self.map.insert(key, val);
            }
        }
        self.last_applied = Some(log_index);
        self.map.len() as u64
    }

    fn query(&self, key: u64) -> Option<Vec<u8>> {
        self.map.get(&key).cloned()
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }

    fn build_snapshot(&self, _dst: &mut dyn Write) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }

    fn install_snapshot(&mut self, _src: &mut dyn Read) -> Result<u64, SnapshotError> {
        Ok(self.last_applied.unwrap_or(0))
    }
}

fn main() {
    eprintln!("commit-path-load: not yet implemented");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_apply_inserts_and_counts() {
        let mut sm = KvSm::default();
        let n = sm.apply(1, KvCmd::Put { key: 7, val: vec![1, 2, 3] });
        assert_eq!(n, 1);
        assert_eq!(sm.query(7), Some(vec![1, 2, 3]));
        assert_eq!(sm.last_applied(), Some(1));
    }
}
```

- [ ] **Step 2: Run the test to verify it passes (compiles + logic)**

Run: `cargo test -p uc_autobench --bin commit-path-load kv_apply_inserts_and_counts -- --nocapture`
Expected: PASS (1 test). This also proves `KvSm` satisfies the `StateMachine + Default` bounds.

- [ ] **Step 3: Commit**

```bash
git add uc_autobench/src/bin/commit-path-load.rs
git commit -m "bench: KvSm Default-able state machine for commit-path-load"
```

### Task 3: HDR percentile helper + CSV row type with a unit test

**Files:**
- Modify: `uc_autobench/src/bin/commit-path-load.rs`

- [ ] **Step 1: Add the row type + writer + test (failing: function not defined)**

Add above `fn main()`:
```rust
use hdrhistogram::Histogram;

struct StepRow {
    config: String,
    workload: String,
    payload_bytes: usize,
    inflight: usize,
    target_rate: f64,
    achieved_rate: f64,
    hist: Histogram<u64>,
}

impl StepRow {
    fn to_csv(&self) -> String {
        format!(
            "uc,{},{},{},{},{:.0},{:.1},{},{},{},{},{},{}",
            self.config,
            self.workload,
            self.payload_bytes,
            self.inflight,
            self.target_rate,
            self.achieved_rate,
            self.hist.value_at_quantile(0.50),
            self.hist.value_at_quantile(0.99),
            self.hist.value_at_quantile(0.999),
            self.hist.value_at_quantile(0.9999),
            self.hist.max(),
            self.hist.len(),
        )
    }
}

const CSV_HEADER: &str =
    "system,config,workload,payload_bytes,inflight,target_rate,achieved_rate,\
p50_ns,p99_ns,p99_9_ns,p99_99_ns,max_ns,count";
```

Add to the `tests` module:
```rust
    #[test]
    fn csv_row_has_13_columns() {
        let mut hist = Histogram::<u64>::new(3).unwrap();
        for v in [100u64, 200, 300, 400, 500] {
            hist.record(v).unwrap();
        }
        let row = StepRow {
            config: "single_disk".into(),
            workload: "kv".into(),
            payload_bytes: 64,
            inflight: 8,
            target_rate: 1000.0,
            achieved_rate: 987.6,
            hist,
        };
        let csv = row.to_csv();
        assert_eq!(csv.split(',').count(), 13);
        assert!(csv.starts_with("uc,single_disk,kv,64,8,1000,987.6,"));
        assert_eq!(CSV_HEADER.split(',').count(), 13);
    }
```

- [ ] **Step 2: Run the test to verify it fails first, then passes**

Run: `cargo test -p uc_autobench --bin commit-path-load csv_row_has_13_columns`
Expected: PASS (column counts match). If it fails on count mismatch, fix the format string — header and row must both be 13 fields.

- [ ] **Step 3: Commit**

```bash
git add uc_autobench/src/bin/commit-path-load.rs
git commit -m "bench: StepRow CSV serialization (HDR percentiles)"
```

### Task 4: Open-loop rate-limited driver for one (rate, inflight) step

The core of the bench: drive submits open-loop at a target rate with a bounded number of in-flight requests, recording intended-send→response latency (coordinated-omission-free). Uses `tokio::time` for the schedule and a `FuturesUnordered` for in-flight tracking. `Client::submit` takes `&self`, so concurrent in-flight submits borrow the same client.

**Files:**
- Modify: `uc_autobench/src/bin/commit-path-load.rs`

- [ ] **Step 1: Add the step driver**

Add (above `fn main()`):
```rust
use std::time::{Duration, Instant};
use futures::stream::{FuturesUnordered, StreamExt};

/// Run one ladder step: open-loop at `target_rate` msgs/s, at most `inflight`
/// concurrent submits, for `duration`. Records intended-send→response latency.
/// `payload_bytes` sets the KV value size. Returns the populated histogram and
/// the achieved rate (completed / wall-seconds).
async fn run_step(
    client: &uc_client::Client,
    target_rate: f64,
    inflight: usize,
    duration: Duration,
    payload_bytes: usize,
) -> anyhow::Result<(Histogram<u64>, f64)> {
    // 1ns..600s range, 3 sig figs (matches Aeron-side hdr_init precision).
    let mut hist = Histogram::<u64>::new_with_bounds(1, 600_000_000_000, 3)?;
    let period = Duration::from_secs_f64(1.0 / target_rate);
    let start = Instant::now();
    let deadline = start + duration;

    let mut inflight_set = FuturesUnordered::new();
    let mut next_send = start;
    let mut seq: u64 = 0;
    let mut completed: u64 = 0;
    let val = vec![0u8; payload_bytes];

    loop {
        let now = Instant::now();
        if now >= deadline && inflight_set.is_empty() {
            break;
        }

        // Launch all sends whose intended time has arrived, up to the cap.
        while now >= next_send && inflight_set.len() < inflight && next_send < deadline {
            let intended = next_send;
            let cmd = KvCmd::Put { key: seq % 4096, val: val.clone() };
            seq += 1;
            next_send += period;
            inflight_set.push(async move {
                let _r: u64 = client.submit(&cmd).await?;
                Ok::<_, anyhow::Error>(intended.elapsed().as_nanos() as u64)
            });
        }

        // Drain whatever completed (non-blocking-ish) or wait until the next
        // scheduled send / a completion, whichever is first.
        tokio::select! {
            Some(res) = inflight_set.next(), if !inflight_set.is_empty() => {
                hist.record(res?.min(600_000_000_000))?;
                completed += 1;
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_send)),
                if now < deadline && inflight_set.len() < inflight => {}
            else => { break; }
        }
    }

    let achieved = completed as f64 / start.elapsed().as_secs_f64();
    Ok((hist, achieved))
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p uc_autobench --bin commit-path-load`
Expected: builds (may warn about unused `run_step` until Task 5 wires it). If `futures` isn't a dep of `uc_autobench`, add `futures = { workspace = true }` to `uc_autobench/Cargo.toml` `[dependencies]` and re-run.

- [ ] **Step 3: Commit**

```bash
git add uc_autobench/src/bin/commit-path-load.rs uc_autobench/Cargo.toml
git commit -m "bench: open-loop rate-limited step driver with CO-free latency"
```

### Task 5: CLI + ladder sweep + fixture wiring in `main`

**Files:**
- Modify: `uc_autobench/src/bin/commit-path-load.rs`

- [ ] **Step 1: Add clap args + main**

Replace the placeholder `fn main()` with:
```rust
use clap::Parser;
use uc_node::test_support::ClusterFixture;

#[derive(Parser)]
#[command(about = "Open-loop commit-path load driver for UC (single-node)")]
struct Args {
    /// config label written into the CSV (e.g. single_tmpfs, single_disk)
    #[arg(long, default_value = "single_disk")]
    config: String,
    /// comma-separated target rates (msgs/s) — the rate ladder
    #[arg(long, default_value = "100,500,1000,2000,5000,10000,20000")]
    rates: String,
    /// in-flight concurrency values to sweep
    #[arg(long, default_value = "1,8,32,128")]
    inflight: String,
    /// KV value size in bytes
    #[arg(long, default_value_t = 64)]
    payload_bytes: usize,
    /// measurement window per step (seconds)
    #[arg(long, default_value_t = 5.0)]
    window_secs: f64,
    /// warmup window per step (seconds)
    #[arg(long, default_value_t = 2.0)]
    warmup_secs: f64,
    /// output CSV path
    #[arg(long, default_value = "bench-out/uc.csv")]
    out: String,
}

fn parse_list<T: std::str::FromStr>(s: &str) -> Vec<T>
where
    T::Err: std::fmt::Debug,
{
    s.split(',').map(|x| x.trim().parse().unwrap()).collect()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt().with_writer(std::io::stderr).try_init();
    let args = Args::parse();
    let rates: Vec<f64> = parse_list(&args.rates);
    let inflights: Vec<usize> = parse_list(&args.inflight);
    let window = Duration::from_secs_f64(args.window_secs);
    let warmup = Duration::from_secs_f64(args.warmup_secs);

    if let Some(parent) = std::path::Path::new(&args.out).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut csv = std::fs::File::create(&args.out)?;
    writeln!(csv, "{CSV_HEADER}")?;

    eprintln!(
        "commit-path-load: config={} rates={:?} inflight={:?} payload={}B",
        args.config, rates, inflights, args.payload_bytes
    );

    // One client is enough for the open-loop driver (submit takes &self and we
    // keep many requests in flight). Use 1 attached client.
    let fixture = ClusterFixture::<KvSm>::single_node(1).await?;
    let client = fixture.client(0);

    for &inflight in &inflights {
        for &rate in &rates {
            // Warmup (discarded).
            let _ = run_step(client, rate, inflight, warmup, args.payload_bytes).await?;
            // Measured.
            let (hist, achieved) =
                run_step(client, rate, inflight, window, args.payload_bytes).await?;
            let row = StepRow {
                config: args.config.clone(),
                workload: "kv".into(),
                payload_bytes: args.payload_bytes,
                inflight,
                target_rate: rate,
                achieved_rate: achieved,
                hist,
            };
            let line = row.to_csv();
            writeln!(csv, "{line}")?;
            csv.flush()?;
            eprintln!("  {line}");
        }
    }

    fixture.shutdown().await?;
    eprintln!("commit-path-load: wrote {}", args.out);
    Ok(())
}
```

- [ ] **Step 2: Build**

Run: `cargo build -p uc_autobench --bin commit-path-load`
Expected: builds clean (warnings OK).

- [ ] **Step 3: Smoke-run a tiny ladder (sanity vs the known ~38ms floor)**

Run:
```bash
cargo run -p uc_autobench --bin commit-path-load --release -- \
  --rates 50,200 --inflight 1,16 --window-secs 3 --warmup-secs 1 \
  --out /tmp/uc-smoke.csv
cat /tmp/uc-smoke.csv
```
Expected: 4 data rows. **Sanity check:** at `inflight=1`, `p50_ns` should be on the order of tens of millions (~36–41 ms — the known group-commit floor from `shmem-e2e.rs`). At `inflight=16`, `achieved_rate` should be markedly higher and per-op latency similar or higher — confirming the group-commit batching effect. If `inflight=1` p50 is microseconds, the commit path isn't actually being exercised — STOP and investigate before trusting any numbers.

- [ ] **Step 4: Commit**

```bash
git add uc_autobench/src/bin/commit-path-load.rs
git commit -m "bench: CLI + rate-ladder/concurrency sweep + fixture wiring"
```

### Task 6: UC run script (tmpfs vs disk fsync targets)

**Files:**
- Create: `uc_autobench/scripts/run-uc-single-node.sh`

- [ ] **Step 1: Write the script**

Create `uc_autobench/scripts/run-uc-single-node.sh`:
```bash
#!/usr/bin/env bash
# Drive the UC single-node commit-path load bench for both fsync targets.
# Disk run: journal on the default real-disk TMPDIR.
# tmpfs run: journal on a RAM disk (Linux: /dev/shm; macOS: hdiutil RAM disk).
set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

OUT_DIR="${OUT_DIR:-bench-out}"
mkdir -p "$OUT_DIR"
RATES="${RATES:-100,500,1000,2000,5000,10000,20000}"
INFLIGHT="${INFLIGHT:-1,8,32,128}"
PAYLOAD="${PAYLOAD:-64}"

build() { cargo build -p uc_autobench --bin commit-path-load --release; }
run() { # $1=config-label $2=tmpdir
  TMPDIR="$2" ./target/release/commit-path-load \
    --config "$1" --rates "$RATES" --inflight "$INFLIGHT" \
    --payload-bytes "$PAYLOAD" --out "$OUT_DIR/uc_$1.csv"
}

build

# --- real disk ---
run single_disk "${TMPDIR:-/tmp}"

# --- tmpfs / RAM disk ---
if [[ "$(uname)" == "Linux" && -d /dev/shm ]]; then
  run single_tmpfs /dev/shm
elif [[ "$(uname)" == "Darwin" ]]; then
  # 512MB RAM disk; skip gracefully if hdiutil unavailable.
  if command -v hdiutil >/dev/null; then
    DEV=$(hdiutil attach -nomount ram://1048576)
    diskutil erasevolume HFS+ ucbenchram "$DEV" >/dev/null
    run single_tmpfs /Volumes/ucbenchram
    hdiutil detach "$DEV" >/dev/null
  else
    echo "SKIP tmpfs run: no hdiutil" >&2
  fi
else
  echo "SKIP tmpfs run: unsupported platform" >&2
fi
echo "UC CSVs in $OUT_DIR/" >&2
```

- [ ] **Step 2: Make executable + dry sanity (tiny ladder)**

Run:
```bash
chmod +x uc_autobench/scripts/run-uc-single-node.sh
RATES=50,200 INFLIGHT=1,16 OUT_DIR=/tmp/uc-bench \
  uc_autobench/scripts/run-uc-single-node.sh
ls -1 /tmp/uc-bench/
```
Expected: `uc_single_disk.csv` present; `uc_single_tmpfs.csv` present (or a clear SKIP message on stderr if no RAM disk).

- [ ] **Step 3: Commit**

```bash
git add uc_autobench/scripts/run-uc-single-node.sh
git commit -m "bench: UC single-node run script (disk + tmpfs fsync targets)"
```

---

## Phase 1 — Aeron IPC reference line

### Task 7: Add the IPC ping/pong C binary

Adapt `cping.c`+`cpong.c` into one self-contained binary that runs an in-process pong responder thread (or document running `cpong` separately) and a cping-style initiator pinned to `aeron:ipc`, emitting the shared CSV schema. Simplest robust approach: **reuse the existing `cping`/`cpong` binaries unchanged**, run them with `-c aeron:ipc -C aeron:ipc`, and post-process `cping`'s HdrHistogram output into the CSV. This avoids new C and is the lowest-risk path.

**Decision for the implementer:** prefer reusing stock `cping`/`cpong` (no new C target) unless a per-step rate ladder is required inside the C process. `cping` runs a fixed message count as fast as possible (saturation), which yields ONE point on the throughput axis per run, not a ladder. To get an Aeron latency-vs-throughput *curve*, drive offered load externally by rate-limiting via the `-m` count across multiple short runs, OR accept that Aeron's curve is "saturation throughput + unloaded latency" (two anchor points). Confirm with the operator which fidelity is needed.

**Files:**
- (Option A, recommended) none — reuse `cping`/`cpong`.
- (Option B) Create `aeron/aeron-samples/src/main/c/c_ipc_pingpong.c`; Modify `aeron/aeron-samples/src/main/c/CMakeLists.txt`.

- [ ] **Step 1: Build the existing Aeron C samples**

Run (in `/Users/peter/Projects/ultima/aeron`):
```bash
./cppbuild/cppbuild
```
Expected: builds; `cping`, `cpong`, and the media driver appear under `cppbuild/Release/`. (First run downloads CMake + googletest + HdrHistogram per `cppbuild/`.) If the full build is too heavy, build only the C client + samples targets — find them via `cmake --build cppbuild/Release --target cping cpong aeronmd`.

- [ ] **Step 2: Manual IPC ping/pong smoke**

Run (three terminals or backgrounded):
```bash
# media driver
cppbuild/Release/binaries/aeronmd &
# pong responder over IPC
cppbuild/Release/binaries/cpong -c aeron:ipc -C aeron:ipc &
# ping initiator over IPC, small run
cppbuild/Release/binaries/cping -c aeron:ipc -C aeron:ipc -m 100000 -w 10000
```
Expected: `cping` prints an HdrHistogram percentile table to stdout with single-digit-to-low-double-digit **microsecond** latencies. This is the transport floor. (Exact binary dir may be `cppbuild/Release/binaries/` — adjust after Step 1.)

- [ ] **Step 3: Commit (only if Option B C file was added)**

```bash
git -C /Users/peter/Projects/ultima/aeron add aeron-samples/src/main/c/c_ipc_pingpong.c aeron-samples/src/main/c/CMakeLists.txt
git -C /Users/peter/Projects/ultima/aeron commit -m "samples: IPC-pinned ping/pong latency bin for UC comparison"
```
(If Option A, skip — nothing to commit in the aeron tree.)

### Task 8: Aeron run script → shared CSV schema

**Files:**
- Create: `aeron/scripts-scratch/run-aeron-ipc.sh`
- Create: `uc_autobench/scripts/aeron_hdr_to_csv.py` (parse `cping` HdrHistogram table → CSV row)

- [ ] **Step 1: Write the parser**

Create `uc_autobench/scripts/aeron_hdr_to_csv.py`:
```python
#!/usr/bin/env python3
"""Parse cping's HdrHistogram CLASSIC percentile table from stdin and emit one
shared-schema CSV row. Values in the table are microseconds (cping prints with
scale 1000.0); convert to ns. Usage:
  cping ... | aeron_hdr_to_csv.py --payload 64 --inflight 1 --achieved <rate>
"""
import argparse, re, sys

P = {"p50_ns": 50.0, "p99_ns": 99.0, "p99_9_ns": 99.9, "p99_99_ns": 99.99}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", type=int, required=True)
    ap.add_argument("--inflight", type=int, default=1)
    ap.add_argument("--achieved", type=float, default=0.0)
    a = ap.parse_args()
    vals, count, mx = {}, 0, 0.0
    for line in sys.stdin:
        m = re.match(r"\s*([\d.]+)\s+([\d.]+)\s+(\d+)\s+([\d.]+)", line)
        if not m:
            continue
        value_us, pct = float(m.group(1)), float(m.group(2)) * 100.0
        count = int(m.group(3))
        mx = max(mx, value_us)
        for key, target in P.items():
            if key not in vals and pct >= target:
                vals[key] = value_us
    ns = lambda us: int(us * 1000)
    row = ["aeron", "ipc", "bytes", a.payload, a.inflight, f"{a.achieved:.0f}",
           f"{a.achieved:.1f}",
           ns(vals.get("p50_ns", 0)), ns(vals.get("p99_ns", 0)),
           ns(vals.get("p99_9_ns", 0)), ns(vals.get("p99_99_ns", 0)),
           ns(mx), count]
    print(",".join(str(x) for x in row))

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Write the run script**

Create `aeron/scripts-scratch/run-aeron-ipc.sh`:
```bash
#!/usr/bin/env bash
# Run Aeron IPC ping/pong and emit a shared-schema CSV row (saturation point).
set -euo pipefail
AERON_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${AERON_BIN:-$AERON_DIR/cppbuild/Release/binaries}"
PARSER="${PARSER:-$AERON_DIR/../ultima_cluster/uc_autobench/scripts/aeron_hdr_to_csv.py}"
OUT="${OUT:-$AERON_DIR/../ultima_cluster/bench-out/aeron_ipc.csv}"
PAYLOAD="${PAYLOAD:-64}"
MSGS="${MSGS:-1000000}"
mkdir -p "$(dirname "$OUT")"

"$BIN/aeronmd" & MD=$!
sleep 1
"$BIN/cpong" -c aeron:ipc -C aeron:ipc & PONG=$!
sleep 1
echo "system,config,workload,payload_bytes,inflight,target_rate,achieved_rate,p50_ns,p99_ns,p99_9_ns,p99_99_ns,max_ns,count" > "$OUT"
"$BIN/cping" -c aeron:ipc -C aeron:ipc -L "$PAYLOAD" -m "$MSGS" -w 100000 \
  | python3 "$PARSER" --payload "$PAYLOAD" --inflight 1 --achieved 0 >> "$OUT"
kill "$PONG" "$MD" 2>/dev/null || true
echo "wrote $OUT" >&2
```

- [ ] **Step 3: Run it end-to-end**

Run:
```bash
chmod +x aeron/scripts-scratch/run-aeron-ipc.sh uc_autobench/scripts/aeron_hdr_to_csv.py
PAYLOAD=64 MSGS=200000 aeron/scripts-scratch/run-aeron-ipc.sh
cat ultima_cluster/bench-out/aeron_ipc.csv 2>/dev/null || cat bench-out/aeron_ipc.csv
```
Expected: a header + 1 data row with `p50_ns`/`p99_ns` in the low microseconds (thousands–tens-of-thousands of ns). Confirms the transport floor row is produced in the shared schema.

- [ ] **Step 4: Commit**

```bash
git add uc_autobench/scripts/aeron_hdr_to_csv.py
git commit -m "bench: parse Aeron cping HDR output into shared CSV schema"
git -C /Users/peter/Projects/ultima/aeron add scripts-scratch/run-aeron-ipc.sh
git -C /Users/peter/Projects/ultima/aeron commit -m "scratch: Aeron IPC ping/pong run script for UC comparison"
```

---

## Phase 1 — Analysis & report

### Task 9: Plot + decomposition script

**Files:**
- Create: `uc_autobench/scripts/plot_decomposition.py`
- Create: `uc_autobench/scripts/requirements.txt`

- [ ] **Step 1: requirements.txt**

Create `uc_autobench/scripts/requirements.txt`:
```
matplotlib>=3.7
pandas>=2.0
```

- [ ] **Step 2: Plot script**

Create `uc_autobench/scripts/plot_decomposition.py`:
```python
#!/usr/bin/env python3
"""Overlay latency-vs-throughput curves for UC configs + the Aeron IPC floor,
and render a per-layer decomposition bar at a fixed offered load.

Usage: plot_decomposition.py bench-out/*.csv --out-dir bench-out/plots
"""
import argparse, glob, os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def load(paths):
    frames = []
    for pat in paths:
        for p in glob.glob(pat):
            frames.append(pd.read_csv(p))
    if not frames:
        raise SystemExit("no CSVs matched")
    return pd.concat(frames, ignore_index=True)

def curve(df, out):
    fig, ax = plt.subplots(figsize=(9, 6))
    for (system, config, inflight), g in df.groupby(["system", "config", "inflight"]):
        g = g.sort_values("achieved_rate")
        ax.plot(g["achieved_rate"], g["p99_ns"] / 1e6,
                marker="o", label=f"{system}/{config} if={inflight}")
    ax.set_xlabel("Achieved throughput (msgs/s)")
    ax.set_ylabel("p99 latency (ms)")
    ax.set_yscale("log"); ax.set_xscale("log")
    ax.set_title("Latency vs throughput: UC commit path vs Aeron IPC")
    ax.legend(fontsize=7); ax.grid(True, which="both", alpha=0.3)
    fig.tight_layout(); fig.savefig(os.path.join(out, "latency_vs_throughput.png"), dpi=130)

def decomposition(df, out):
    # p99 at the lowest target_rate (unloaded) per config, in ms — the layer floor.
    base = (df.sort_values("target_rate")
              .groupby(["system", "config"], as_index=False).first())
    fig, ax = plt.subplots(figsize=(9, 5))
    labels = base["system"] + "/" + base["config"]
    ax.bar(labels, base["p99_ns"] / 1e6)
    ax.set_ylabel("Unloaded p99 (ms)"); ax.set_yscale("log")
    ax.set_title("Per-layer floor (unloaded p99)")
    for t in ax.get_xticklabels():
        t.set_rotation(30); t.set_ha("right")
    fig.tight_layout(); fig.savefig(os.path.join(out, "decomposition.png"), dpi=130)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csvs", nargs="+")
    ap.add_argument("--out-dir", default="bench-out/plots")
    a = ap.parse_args()
    os.makedirs(a.out_dir, exist_ok=True)
    df = load(a.csvs)
    curve(df, a.out_dir)
    decomposition(df, a.out_dir)
    print(f"wrote plots to {a.out_dir}")

if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Generate plots from smoke CSVs**

Run:
```bash
python3 -m venv /tmp/benchvenv && /tmp/benchvenv/bin/pip install -q -r uc_autobench/scripts/requirements.txt
/tmp/benchvenv/bin/python uc_autobench/scripts/plot_decomposition.py \
  /tmp/uc-bench/*.csv bench-out/aeron_ipc.csv --out-dir /tmp/bench-plots || \
/tmp/benchvenv/bin/python uc_autobench/scripts/plot_decomposition.py \
  /tmp/uc-smoke.csv --out-dir /tmp/bench-plots
ls -1 /tmp/bench-plots/
```
Expected: `latency_vs_throughput.png` and `decomposition.png` produced without error.

- [ ] **Step 4: Commit**

```bash
git add uc_autobench/scripts/plot_decomposition.py uc_autobench/scripts/requirements.txt
git commit -m "bench: matplotlib overlay + decomposition plots"
```

### Task 10: Full Phase 1 run + report

**Files:**
- Create: `docs/tasks/taskNN_aeron_vs_uc_commit_path.md` (pick the next free NN; currently task08 is highest, so likely `task09`)

- [ ] **Step 1: Full UC run (real ladder)**

Run:
```bash
uc_autobench/scripts/run-uc-single-node.sh
```
Expected: `bench-out/uc_single_disk.csv` and (if RAM disk) `bench-out/uc_single_tmpfs.csv`, each with rows across the full rate × inflight grid.

- [ ] **Step 2: Full Aeron run across payloads**

Run:
```bash
for P in 8 64 256; do PAYLOAD=$P MSGS=2000000 aeron/scripts-scratch/run-aeron-ipc.sh; done
```
Expected: `bench-out/aeron_ipc.csv` gains a row per payload (append; remove the header-rewrite for repeated runs or dedupe in the plot).

- [ ] **Step 3: Generate final plots**

Run:
```bash
/tmp/benchvenv/bin/python uc_autobench/scripts/plot_decomposition.py \
  bench-out/uc_*.csv bench-out/aeron_ipc.csv --out-dir bench-out/plots
```
Expected: both PNGs in `bench-out/plots/`.

- [ ] **Step 4: Write the report**

Create `docs/tasks/task09_aeron_vs_uc_commit_path.md` covering:
- Methodology (open-loop, CO-free, HDR, shared schema) and fairness controls actually used (payload sizes, fsync target, inflight sweep, platform).
- The decomposition table with REAL numbers: Aeron IPC p50/p99 (transport floor) → UC ring RT (cite task08 ~15ns SPSC p99) → UC single-node tmpfs p50/p99 → UC single-node disk p50/p99. Quantify how much of the gap is transport vs consensus+fsync.
- The latency-vs-throughput knee per UC config and the group-commit batching effect across the inflight sweep.
- **Prioritized optimization backlog** ranked by measured layer contribution. Expectation from recon: journal group-commit/fsync dominates; note io_uring is Linux-only (out of scope on the arm64 macOS host); shmem rings already near floor (task08). Each item: what to change, expected layer impact, and whether the `uc_autobench` autoresearch loop can drive it.

- [ ] **Step 5: Commit**

```bash
git add docs/tasks/task09_aeron_vs_uc_commit_path.md bench-out/plots
git commit -m "bench: Aeron-vs-UC commit-path report + decomposition (Phase 1)"
```

---

## Phase 2 — 3-node replication layer (multi-process)

> Phase 2 adds the replication/quorum layer via a REAL 3-process cluster. The in-process `ClusterFixture` cannot do multi-node (verified: single-node only). Because the multi-process launch infrastructure was not fully verified during planning, **Task 11 is recon** — do it before writing the remaining Phase 2 steps. Do not fabricate node/service CLI args; derive them from the code.

### Task 11: Recon the multi-process launch path (no code)

**Files:** none (investigation; write findings into the plan before proceeding)

- [ ] **Step 1: Map the feature + binaries**

Run:
```bash
grep -rn "multi-process-tests\|multi_process" --include=*.toml --include=*.rs .
ls examples/*/src/bin 2>/dev/null; ls uc_node/examples uc_service/examples uc_client/examples 2>/dev/null
grep -rn "BootstrapConfig\|NodeBuilder\|NodeConfig\|ServiceBuilder\|ServiceConfig" uc_node/src uc_service/src | head -40
```
Record: which crate defines `multi-process-tests`, what example/bin binaries exist to launch a node/service/client from the CLI (and their arg parsing), the `BootstrapConfig` variants for multi-node membership, how the QUIC listen addr + node_id + peer list are set, and how the instance/journal dir is chosen (for tmpfs-vs-disk and for client discovery).

- [ ] **Step 2: Find the existing multi-node bring-up test**

Run:
```bash
ls uc_node/tests; grep -rln "node_id: 2\|three.node\|3.node\|quic\|membership\|add_learner\|change_membership" uc_node/tests
```
Read the multi-node integration test (e.g. `m2_*`) and record exactly how it constructs 3 nodes, passes membership/addresses, and awaits leader election. This is the template for the launch script.

- [ ] **Step 3: Write the remaining Phase 2 tasks**

Using the recon, append concrete tasks to this plan:
- a launch script `uc_autobench/scripts/run-uc-3node.sh` that starts 3 `uc_node` + 1 `uc_service` + waits for leader,
- a `--connect <instance_dir>` mode added to `commit-path-load` (attach to a running cluster via `Client::connect` instead of spawning the fixture; gate the fixture path behind the absence of `--connect`),
- a full 3-node run + extend the report's decomposition with the replication layer (+ loopback caveat).

Then execute them.

---

## Self-review notes

- **Spec coverage:** open-loop rate ladder (T4), HDR per step (T3/T4), shared CSV schema (T3/T8), Aeron IPC floor (T7/T8), UC single-node tmpfs+disk (T5/T6), concurrency sweep (T4/T5), KV workload (T2 — in-memory KV; `ultima_db::Store` swap noted as follow-up since it isn't `Default`), matplotlib plots (T9), decomposition + backlog report (T10), 3-node replication (Phase 2). All spec sections mapped.
- **Known deviation from spec:** KV uses an in-memory HashMap SM, not `StoreStateMachine`, because the fixture requires `Default` and `StoreStateMachine` isn't. This preserves the "real keyed apply cost" intent; swapping in `ultima_db::Store` requires an instance-dir-injection path (call out in the report).
- **Aeron fidelity caveat (T7):** stock `cping` measures unloaded latency + saturation throughput (2 anchor points), not a full offered-load ladder. Flagged for operator decision; the UC side carries the full curve regardless.
- **Type consistency:** `KvCmd`/`KvSm`/`StepRow`/`run_step`/`CSV_HEADER`/`Args` names are consistent across T2–T6; CSV column count (13) asserted in T3 and matched by the header in T5 and the Aeron parser in T8.
