# uc_autobench — CC-Driven Autoresearch Rework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rework `uc_autobench` from an API-driven Rust orchestrator into a karpathy/autoresearch-shape loop driven by Claude Code itself — deleting ~2000 lines of orchestrator/LLM-client/proposal/sandbox/leaderboard/persist code, adding a tiny `run-iter` consolidation binary, and replacing the declarative TOML task spec with markdown loop instructions (`program.md` + per-task overlay).

**Architecture:** Claude Code reads `uc_autobench/program.md` + `uc_autobench/tasks/<task>/program.md`, executes the loop directly using Edit/Bash/Read + plain git. A single Rust helper (`run-iter`) chains build → ring_torture → shmem-microbench → conditional shmem-e2e and emits one consolidated JSON. Branch advances on win, `git checkout --` reverts on loss, TSV log is committed each iteration. Fitness binaries (`shmem-microbench`, `shmem-e2e`) and the `ring_torture` correctness suite are unchanged (frozen for the agent).

**Tech Stack:** Rust (workspace member, no_std-friendly target crate), `clap` (CLI parsing), `serde_json` (JSON output), `wait-timeout` (subprocess hard timeouts), plain `std::process::Command` (subprocess spawning).

**Spec:** `docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`.

**Working branch:** `auto_bench_shmem` (already checked out).

---

## Task 1: Delete API-driven Rust code

**Files:**
- Delete: `uc_autobench/src/leaderboard.rs`
- Delete: `uc_autobench/src/llm.rs`
- Delete: `uc_autobench/src/orchestrator.rs`
- Delete: `uc_autobench/src/outcome.rs`
- Delete: `uc_autobench/src/persist.rs`
- Delete: `uc_autobench/src/prompt.rs`
- Delete: `uc_autobench/src/proposal.rs`
- Delete: `uc_autobench/src/sandbox.rs`
- Delete: `uc_autobench/src/task.rs`
- Delete: `uc_autobench/src/tasks/mod.rs`
- Delete: `uc_autobench/src/tasks/shmem.rs`
- Delete: `uc_autobench/src/tasks/` (directory after files gone)
- Delete: `uc_autobench/src/bin/auto-bench.rs`
- Delete: `uc_autobench/tasks/shmem/task.toml`
- Delete: `uc_autobench/tests/leaderboard_diverse_pick.rs`
- Delete: `uc_autobench/tests/llm_stub.rs`
- Delete: `uc_autobench/tests/orchestrator_stub_loop.rs`
- Delete: `uc_autobench/tests/outcome_roundtrip.rs`
- Delete: `uc_autobench/tests/persist_replay.rs`
- Delete: `uc_autobench/tests/prompt_snapshot.rs`
- Delete: `uc_autobench/tests/proposal_apply.rs`
- Delete: `uc_autobench/tests/proposal_static_checks.rs`
- Delete: `uc_autobench/tests/sandbox_timeout.rs`
- Delete: `uc_autobench/tests/task_spec_parse.rs`
- Modify: `uc_autobench/src/lib.rs`
- Modify: `uc_autobench/Cargo.toml`

**Surviving files (do NOT touch):**
- `uc_autobench/src/bin/shmem-microbench.rs`
- `uc_autobench/src/bin/shmem-e2e.rs`
- `uc_autobench/tests/ring_torture.rs`

- [ ] **Step 1: Delete the source files, test files, and TOML**

```bash
cd /Users/peter/Projects/ultima/worktrees/ultima_cluster-auto_bench_shmem
git rm \
  uc_autobench/src/leaderboard.rs \
  uc_autobench/src/llm.rs \
  uc_autobench/src/orchestrator.rs \
  uc_autobench/src/outcome.rs \
  uc_autobench/src/persist.rs \
  uc_autobench/src/prompt.rs \
  uc_autobench/src/proposal.rs \
  uc_autobench/src/sandbox.rs \
  uc_autobench/src/task.rs \
  uc_autobench/src/tasks/mod.rs \
  uc_autobench/src/tasks/shmem.rs \
  uc_autobench/src/bin/auto-bench.rs \
  uc_autobench/tasks/shmem/task.toml \
  uc_autobench/tests/leaderboard_diverse_pick.rs \
  uc_autobench/tests/llm_stub.rs \
  uc_autobench/tests/orchestrator_stub_loop.rs \
  uc_autobench/tests/outcome_roundtrip.rs \
  uc_autobench/tests/persist_replay.rs \
  uc_autobench/tests/prompt_snapshot.rs \
  uc_autobench/tests/proposal_apply.rs \
  uc_autobench/tests/proposal_static_checks.rs \
  uc_autobench/tests/sandbox_timeout.rs \
  uc_autobench/tests/task_spec_parse.rs
rmdir uc_autobench/src/tasks
```

Expected: `rmdir` succeeds (directory empty after `git rm`).

- [ ] **Step 2: Replace `uc_autobench/src/lib.rs` with a minimal stub**

Write to `uc_autobench/src/lib.rs`:

```rust
//! uc_autobench — Claude-Code-driven autoresearch loop helpers.
//!
//! See `docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`
//! for the design and `program.md` for the loop the agent executes.
//!
//! This crate exposes only fitness binaries (`shmem-microbench`, `shmem-e2e`),
//! the consolidation helper (`run-iter`), and the frozen `ring_torture`
//! conformance suite. The orchestration loop itself lives in `program.md`
//! and is executed directly by Claude Code.
```

(No `pub mod` lines — the binaries don't need a shared library at this point. If `run-iter` later needs to share types with tests, the lib.rs is the place to grow.)

- [ ] **Step 3: Run cargo build to surface dep-pruning targets**

Run: `cargo build -p uc_autobench --release 2>&1 | tail -30`

Expected: clean build, OR errors only from deps no longer used by surviving code (e.g. unused-dep warnings if `lints.unused-crate-dependencies` is on). Record any unresolved import errors — those signal a surviving file references a deleted module.

- [ ] **Step 4: Slim `uc_autobench/Cargo.toml`**

Replace the `[dependencies]` block with the minimal set the surviving binaries need (verify against `cargo build` output from Step 3 before committing):

```toml
[package]
name = "uc_autobench"
edition.workspace = true
version.workspace = true
license.workspace = true
authors.workspace = true

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
anyhow = { workspace = true }
clap = { workspace = true }
tempfile = { workspace = true }
tokio = { workspace = true }
uc_protocol = { path = "../uc_protocol" }
uc_node = { path = "../uc_node", features = ["test-support"] }
uc_service = { path = "../uc_service" }

[dev-dependencies]
crc32fast = { workspace = true }
bytes = { workspace = true }

[[bin]]
name = "shmem-microbench"
path = "src/bin/shmem-microbench.rs"

[[bin]]
name = "shmem-e2e"
path = "src/bin/shmem-e2e.rs"
```

(Dropped: `toml`, `thiserror`, `tracing`, `tracing-subscriber`, `reqwest`, `sha2`, `jiff`, `uc_client`. The `[[bin]]` entry for `auto-bench` is removed; `run-iter` is added in Task 4.)

- [ ] **Step 5: Verify build + clippy + ring_torture all green**

Run:
```bash
cargo build --workspace --release 2>&1 | tail -20
cargo clippy --workspace --release -- -D warnings 2>&1 | tail -20
cargo test -p uc_autobench --test ring_torture --release 2>&1 | tail -20
```

Expected: all three exit 0. `ring_torture` runs its 6 conformance tests, all pass.

If any dep was prematurely dropped: re-add it to `Cargo.toml`, re-run.

- [ ] **Step 6: Commit**

```bash
git add -A uc_autobench/
git commit -m "refactor(uc_autobench): remove API-driven loop, prep for CC-driven autoresearch

Deletes ~2000 lines of orchestrator/LLM-client/proposal/sandbox/leaderboard/
persist/prompt code along with their tests and the declarative task.toml.
Slims Cargo.toml to the minimal dep set the surviving fitness binaries need.

Surviving: shmem-microbench (frozen), shmem-e2e (frozen), ring_torture
(frozen correctness suite), uc_node::test_support::ClusterFixture.

run-iter consolidation binary and program.md loop spec land in follow-up
commits per the implementation plan."
```

---

## Task 2: Delete `create-autobench-task` skill

**Files:**
- Delete: `.claude/skills/create-autobench-task/` (entire dir)

- [ ] **Step 1: Confirm the skill directory's contents (sanity check)**

Run: `ls .claude/skills/create-autobench-task/`

Expected: a `SKILL.md` (possibly plus auxiliary files). Note the contents in case anything needs salvaging — but per the spec, nothing here is being preserved.

- [ ] **Step 2: Delete the directory**

```bash
git rm -r .claude/skills/create-autobench-task/
```

- [ ] **Step 3: Verify Claude Code's other skills are unaffected**

Run: `ls .claude/skills/`

Expected: the directory listing no longer contains `create-autobench-task/`. Other skill directories (if any) are untouched.

- [ ] **Step 4: Commit**

```bash
git add -A .claude/
git commit -m "chore(.claude): remove create-autobench-task skill (superseded by program.md model)"
```

---

## Task 3: `run-iter` skeleton — CLI args + output types

**Files:**
- Create: `uc_autobench/src/bin/run-iter.rs`
- Modify: `uc_autobench/Cargo.toml` (add `[[bin]]` entry for run-iter, add `wait-timeout` dep)

- [ ] **Step 1: Add `wait-timeout` to the workspace and `uc_autobench` deps**

Check the workspace `Cargo.toml` for `wait-timeout`. If not present, add to the root `Cargo.toml`'s `[workspace.dependencies]`:

```toml
wait-timeout = "0.2"
```

Then add to `uc_autobench/Cargo.toml` under `[dependencies]`:

```toml
wait-timeout = { workspace = true }
```

And add the `[[bin]]` entry at the bottom of `uc_autobench/Cargo.toml`:

```toml
[[bin]]
name = "run-iter"
path = "src/bin/run-iter.rs"
```

- [ ] **Step 2: Create `uc_autobench/src/bin/run-iter.rs` with CLI parsing + output types only**

Write to `uc_autobench/src/bin/run-iter.rs`:

```rust
//! run-iter — single-command per-iteration harness for uc_autobench.
//!
//! Spawns build → ring_torture → shmem-microbench → (conditional) shmem-e2e,
//! emits one JSON object on stdout describing the outcome. The agent reads
//! the JSON `status` field, not the exit code. Exit 0 even on stage failure;
//! non-zero only on this binary's own internal bug.
//!
//! See `docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`.

use clap::Parser;
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(name = "run-iter")]
struct Args {
    /// Task identifier. Only `shmem` is supported in v1.
    #[arg(long)]
    task: String,

    /// Emit machine-readable JSON on stdout (currently the only mode).
    #[arg(long)]
    json: bool,

    /// Latest committed best spsc_p99_ns. Optional on the first iteration.
    #[arg(long)]
    baseline_spsc_p99_ns: Option<u64>,

    /// Latest committed best submit_to_resp_p99_ns. Optional on the first iteration.
    #[arg(long)]
    baseline_e2e_p99_ns: Option<u64>,
}

#[derive(Serialize, Debug, Default)]
struct Output {
    /// One of: pass, build_failed, torture_failed, microbench_failed,
    /// e2e_failed, timeout.
    status: String,
    /// Stage that produced `status`: build, torture, microbench, e2e.
    stage: String,
    duration_s: Durations,
    metrics: Option<serde_json::Value>,
    gate: Gate,
    /// Last ~50 lines of stderr on failure; null on pass.
    stderr_tail: Option<String>,
}

#[derive(Serialize, Debug, Default)]
struct Durations {
    build: f64,
    torture: f64,
    microbench: f64,
    e2e: f64,
}

#[derive(Serialize, Debug, Default)]
struct Gate {
    ran: bool,
    /// None when the gate didn't run; Some(true/false) when it did.
    e2e_passed: Option<bool>,
    submit_to_resp_p99_ns: Option<u64>,
    baseline: Option<u64>,
    regress_pct: Option<f64>,
    /// Why the gate didn't run, if it didn't.
    reason: Option<String>,
}

fn main() {
    let args = Args::parse();

    if args.task != "shmem" {
        eprintln!("run-iter: unknown task {:?}; v1 supports only `shmem`", args.task);
        std::process::exit(2);
    }
    if !args.json {
        eprintln!("run-iter: only --json output mode is supported in v1");
        std::process::exit(2);
    }

    // Placeholder: emit an empty stub object so the JSON shape can be
    // verified before the real stages land.
    let out = Output {
        status: "pass".to_string(),
        stage: "microbench".to_string(),
        ..Output::default()
    };
    println!("{}", serde_json::to_string(&out).expect("serialize Output"));
}
```

- [ ] **Step 3: Verify the binary builds and emits well-formed JSON**

Run:
```bash
cargo build -p uc_autobench --bin run-iter --release 2>&1 | tail -10
cargo run -p uc_autobench --bin run-iter --release --quiet -- --task shmem --json | python3 -m json.tool
```

Expected: build clean. JSON output has top-level keys `status`, `stage`, `duration_s`, `metrics`, `gate`, `stderr_tail`. `status` is `"pass"`. `gate` is `{"ran": false, ...}` shape.

- [ ] **Step 4: Verify the unknown-task and missing-json paths exit with code 2**

Run:
```bash
cargo run -p uc_autobench --bin run-iter --release --quiet -- --task other --json; echo "exit=$?"
cargo run -p uc_autobench --bin run-iter --release --quiet -- --task shmem; echo "exit=$?"
```

Expected: both print a diagnostic to stderr and `exit=2`.

- [ ] **Step 5: Commit**

```bash
git add uc_autobench/Cargo.toml uc_autobench/src/bin/run-iter.rs ../Cargo.toml
git commit -m "feat(uc_autobench): run-iter skeleton with CLI + JSON output types

CLI parses --task, --json, --baseline-spsc-p99-ns, --baseline-e2e-p99-ns.
Stub main() emits a well-formed but empty Output JSON so the shape can be
validated by jq before the real stages land in follow-up commits."
```

(If the workspace root `Cargo.toml` was not modified, omit it from `git add`.)

---

## Task 4: `run-iter` pure logic — gate decision, regress percent, stderr tail (TDD)

**Files:**
- Modify: `uc_autobench/src/bin/run-iter.rs`

These three helpers are the only pure-logic decisions in `run-iter`. They get real unit tests inside the binary's own `#[cfg(test)] mod tests`.

- [ ] **Step 1: Add failing tests for the three helpers**

Append to `uc_autobench/src/bin/run-iter.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_runs_when_no_baseline_supplied() {
        // First iteration of a new run: no committed best yet, so the gate
        // always runs to establish baseline.
        let d = gate_decision(195, None);
        assert_eq!(d, GateDecision::Run);
    }

    #[test]
    fn gate_runs_when_microbench_plausibly_wins() {
        // 195 ns vs 200 ns baseline: clear improvement, gate runs.
        let d = gate_decision(195, Some(200));
        assert_eq!(d, GateDecision::Run);
    }

    #[test]
    fn gate_runs_when_microbench_within_5pct_of_baseline() {
        // 209 ns vs 200 ns baseline: 4.5% over, still plausibly noise, gate runs.
        let d = gate_decision(209, Some(200));
        assert_eq!(d, GateDecision::Run);
    }

    #[test]
    fn gate_skips_when_microbench_clearly_regresses() {
        // 215 ns vs 200 ns baseline: 7.5% over, not a winner, skip gate.
        match gate_decision(215, Some(200)) {
            GateDecision::Skip(reason) => {
                assert!(reason.contains("not_plausible"), "reason={reason}");
            }
            GateDecision::Run => panic!("expected Skip, got Run"),
        }
    }

    #[test]
    fn regress_pct_basic() {
        assert!((regress_pct(105, 100) - 5.0).abs() < 1e-9);
        assert!((regress_pct(100, 100) - 0.0).abs() < 1e-9);
        assert!((regress_pct(95, 100) - (-5.0)).abs() < 1e-9);
    }

    #[test]
    fn regress_pct_zero_baseline_is_zero() {
        // Defensive: if baseline is somehow 0, return 0.0 instead of NaN/inf.
        assert_eq!(regress_pct(100, 0), 0.0);
    }

    #[test]
    fn tail_lines_short_input_returned_whole() {
        let s = "a\nb\nc\n";
        assert_eq!(tail_lines(s, 50), "a\nb\nc\n");
    }

    #[test]
    fn tail_lines_returns_last_n_lines() {
        let s = "a\nb\nc\nd\ne\nf\n";
        // Last 3 lines: d, e, f
        assert_eq!(tail_lines(s, 3), "d\ne\nf\n");
    }

    #[test]
    fn tail_lines_no_trailing_newline() {
        let s = "a\nb\nc";
        assert_eq!(tail_lines(s, 2), "b\nc");
    }
}
```

- [ ] **Step 2: Run tests, verify they fail (functions don't exist yet)**

Run: `cargo test -p uc_autobench --bin run-iter --release 2>&1 | tail -20`

Expected: compile error — `gate_decision`, `regress_pct`, `tail_lines`, `GateDecision` all undefined.

- [ ] **Step 3: Add minimal implementations to make tests pass**

Add above the `fn main()` line in `uc_autobench/src/bin/run-iter.rs`:

```rust
/// Outcome of the gate-run decision. The Goodhart e2e gate is expensive
/// (~40s); we skip it when the microbench shows the variant clearly isn't
/// a winner — saves wall time per iteration.
#[derive(Debug, PartialEq, Eq)]
enum GateDecision {
    Run,
    Skip(String),
}

/// Decide whether to run the e2e Goodhart gate.
///
/// - No baseline (first iter): always run, to establish baseline.
/// - With baseline: run iff `spsc_p99_ns` is at least within 5% of baseline
///   (i.e. not a clear regression). The agent's TSV decision logic also
///   requires `spsc_p99_ns < baseline` for a KEEP, so this threshold is
///   intentionally permissive: it filters obvious losers, not marginal ones.
fn gate_decision(spsc_p99_ns: u64, baseline_spsc_p99_ns: Option<u64>) -> GateDecision {
    match baseline_spsc_p99_ns {
        None => GateDecision::Run,
        Some(baseline) => {
            // Allow up to 5% over baseline; reject only clear losers.
            let threshold = (baseline as f64) * 1.05;
            if (spsc_p99_ns as f64) <= threshold {
                GateDecision::Run
            } else {
                GateDecision::Skip("skipped_microbench_not_plausible".to_string())
            }
        }
    }
}

/// Percent change of `value` relative to `baseline`. Positive = regression
/// (assuming minimize-direction metrics). Returns 0.0 when baseline is 0.
fn regress_pct(value: u64, baseline: u64) -> f64 {
    if baseline == 0 {
        return 0.0;
    }
    ((value as f64) - (baseline as f64)) / (baseline as f64) * 100.0
}

/// Return the last `n` lines of `s`. If `s` has fewer than `n` lines, the
/// whole input is returned. A trailing newline is preserved if present.
fn tail_lines(s: &str, n: usize) -> String {
    let mut lines: Vec<&str> = s.split_inclusive('\n').collect();
    if lines.len() > n {
        lines = lines.split_off(lines.len() - n);
    }
    lines.concat()
}
```

- [ ] **Step 4: Run tests, verify they pass**

Run: `cargo test -p uc_autobench --bin run-iter --release 2>&1 | tail -20`

Expected: 9 tests pass.

- [ ] **Step 5: Commit**

```bash
git add uc_autobench/src/bin/run-iter.rs
git commit -m "feat(uc_autobench): run-iter pure logic (gate decision, regress pct, tail)

GateDecision::{Run, Skip} captures whether the e2e Goodhart gate should
run for a given microbench result vs. the committed baseline. regress_pct
and tail_lines are small pure helpers used by stage-failure paths and the
final JSON assembly. All three unit-tested."
```

---

## Task 5: `run-iter` build + torture stages

**Files:**
- Modify: `uc_autobench/src/bin/run-iter.rs`

- [ ] **Step 1: Add a subprocess runner helper**

Add above `fn main()` in `uc_autobench/src/bin/run-iter.rs`:

```rust
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use wait_timeout::ChildExt;

/// Result of running a subprocess.
struct StageRun {
    /// Process exit status, or None if killed by timeout.
    exit_ok: bool,
    /// Combined stderr (and stdout for non-JSON stages).
    stderr: String,
    /// Stdout, captured separately so JSON-emitting stages can parse it.
    stdout: String,
    /// Wall-clock duration.
    duration_s: f64,
    /// True if the process was killed by the watchdog.
    timed_out: bool,
}

/// Spawn a command, capture stdout+stderr, enforce a hard wall-clock timeout.
/// On timeout, kill the process tree and return `timed_out = true`.
fn run_stage(mut cmd: Command, timeout: Duration) -> StageRun {
    let started = Instant::now();
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return StageRun {
                exit_ok: false,
                stderr: format!("failed to spawn: {e}"),
                stdout: String::new(),
                duration_s: 0.0,
                timed_out: false,
            };
        }
    };

    let status = match child.wait_timeout(timeout).expect("wait_timeout") {
        Some(s) => Some(s),
        None => {
            let _ = child.kill();
            let _ = child.wait();
            None
        }
    };

    let stdout = child
        .stdout
        .take()
        .map(|mut p| {
            use std::io::Read;
            let mut s = String::new();
            let _ = p.read_to_string(&mut s);
            s
        })
        .unwrap_or_default();
    let stderr = child
        .stderr
        .take()
        .map(|mut p| {
            use std::io::Read;
            let mut s = String::new();
            let _ = p.read_to_string(&mut s);
            s
        })
        .unwrap_or_default();

    StageRun {
        exit_ok: matches!(status, Some(s) if s.success()),
        stderr,
        stdout,
        duration_s: started.elapsed().as_secs_f64(),
        timed_out: status.is_none(),
    }
}
```

- [ ] **Step 2: Replace the stub `main()` with build + torture orchestration**

Replace the existing `fn main() { ... }` body in `uc_autobench/src/bin/run-iter.rs`:

```rust
fn main() {
    let args = Args::parse();

    if args.task != "shmem" {
        eprintln!("run-iter: unknown task {:?}; v1 supports only `shmem`", args.task);
        std::process::exit(2);
    }
    if !args.json {
        eprintln!("run-iter: only --json output mode is supported in v1");
        std::process::exit(2);
    }

    let mut out = Output::default();

    // Stage 1: build
    let mut build_cmd = Command::new("cargo");
    build_cmd
        .args(["build", "-p", "uc_protocol", "-p", "uc_autobench", "--release"]);
    let build = run_stage(build_cmd, Duration::from_secs(600));
    out.duration_s.build = build.duration_s;
    if build.timed_out {
        out.status = "timeout".into();
        out.stage = "build".into();
        out.stderr_tail = Some(tail_lines(&build.stderr, 50));
        emit_and_exit(&out);
    }
    if !build.exit_ok {
        out.status = "build_failed".into();
        out.stage = "build".into();
        out.stderr_tail = Some(tail_lines(&build.stderr, 50));
        emit_and_exit(&out);
    }

    // Stage 2: ring_torture conformance
    let mut torture_cmd = Command::new("cargo");
    torture_cmd.args([
        "test",
        "-p",
        "uc_autobench",
        "--test",
        "ring_torture",
        "--release",
    ]);
    let torture = run_stage(torture_cmd, Duration::from_secs(300));
    out.duration_s.torture = torture.duration_s;
    if torture.timed_out {
        out.status = "timeout".into();
        out.stage = "torture".into();
        out.stderr_tail = Some(tail_lines(
            &format!("{}\n--- stdout ---\n{}", torture.stderr, torture.stdout),
            50,
        ));
        emit_and_exit(&out);
    }
    if !torture.exit_ok {
        out.status = "torture_failed".into();
        out.stage = "torture".into();
        out.stderr_tail = Some(tail_lines(
            &format!("{}\n--- stdout ---\n{}", torture.stderr, torture.stdout),
            50,
        ));
        emit_and_exit(&out);
    }

    // Microbench + e2e gate land in Tasks 6 and 7.
    out.status = "pass".into();
    out.stage = "torture".into();
    emit_and_exit(&out);
}

fn emit_and_exit(out: &Output) -> ! {
    println!("{}", serde_json::to_string(out).expect("serialize Output"));
    std::process::exit(0);
}
```

- [ ] **Step 3: Verify build still passes and unit tests still pass**

Run: `cargo test -p uc_autobench --bin run-iter --release 2>&1 | tail -20`

Expected: 9 unit tests pass (the new code is mostly subprocess-orchestration; pure helpers are unchanged).

- [ ] **Step 4: Smoke-test the happy path (build + torture succeed)**

Run: `cargo run -p uc_autobench --bin run-iter --release --quiet -- --task shmem --json 2>/dev/null | python3 -m json.tool`

Expected: JSON output, `"status": "pass"`, `"stage": "torture"`, `duration_s.build` and `duration_s.torture` non-zero, `stderr_tail` is `null`.

(Walltime: ~30s if release artifacts already cached, longer on cold build.)

- [ ] **Step 5: Smoke-test the build_failed path**

Temporarily induce a build error: append `let _ = undefined_symbol;` inside any function in `uc_protocol/src/ring/spsc.rs`.

Run: `cargo run -p uc_autobench --bin run-iter --release --quiet -- --task shmem --json 2>/dev/null | python3 -m json.tool`

Expected: JSON output, `"status": "build_failed"`, `"stage": "build"`, `stderr_tail` is a non-null string ending in the cargo error message about `undefined_symbol`.

Revert the temporary edit: `git checkout -- uc_protocol/src/ring/spsc.rs`.

- [ ] **Step 6: Commit**

```bash
git add uc_autobench/src/bin/run-iter.rs
git commit -m "feat(uc_autobench): run-iter build + ring_torture stages

run_stage() wraps Command spawn + wait_timeout + stdout/stderr capture.
main() now drives cargo build (600s timeout) and cargo test ring_torture
(300s timeout), short-circuiting to JSON on first failure. Microbench and
e2e gate land in follow-up commits."
```

---

## Task 6: `run-iter` microbench stage

**Files:**
- Modify: `uc_autobench/src/bin/run-iter.rs`

- [ ] **Step 1: Insert the microbench stage between torture and the final pass-emit**

In `uc_autobench/src/bin/run-iter.rs`, replace the lines:

```rust
    // Microbench + e2e gate land in Tasks 6 and 7.
    out.status = "pass".into();
    out.stage = "torture".into();
    emit_and_exit(&out);
}
```

with:

```rust
    // Stage 3: shmem-microbench
    let mut mb_cmd = Command::new("cargo");
    mb_cmd.args([
        "run",
        "-p",
        "uc_autobench",
        "--bin",
        "shmem-microbench",
        "--release",
        "--quiet",
        "--",
        "--json",
    ]);
    let mb = run_stage(mb_cmd, Duration::from_secs(180));
    out.duration_s.microbench = mb.duration_s;
    if mb.timed_out {
        out.status = "timeout".into();
        out.stage = "microbench".into();
        out.stderr_tail = Some(tail_lines(&mb.stderr, 50));
        emit_and_exit(&out);
    }
    if !mb.exit_ok {
        out.status = "microbench_failed".into();
        out.stage = "microbench".into();
        out.stderr_tail = Some(tail_lines(&mb.stderr, 50));
        emit_and_exit(&out);
    }
    // The microbench writes one JSON object to stdout. Parse it.
    let mb_json: serde_json::Value = match serde_json::from_str(mb.stdout.trim()) {
        Ok(v) => v,
        Err(e) => {
            out.status = "microbench_failed".into();
            out.stage = "microbench".into();
            out.stderr_tail = Some(format!(
                "microbench stdout was not valid JSON: {e}\n--- raw ---\n{}",
                tail_lines(&mb.stdout, 30)
            ));
            emit_and_exit(&out);
        }
    };
    out.metrics = Some(mb_json);

    // E2E gate lands in Task 7.
    out.status = "pass".into();
    out.stage = "microbench".into();
    emit_and_exit(&out);
}
```

- [ ] **Step 2: Verify unit tests still pass**

Run: `cargo test -p uc_autobench --bin run-iter --release 2>&1 | tail -10`

Expected: 9 tests pass.

- [ ] **Step 3: Smoke-test the happy path**

Run: `cargo run -p uc_autobench --bin run-iter --release --quiet -- --task shmem --json 2>/dev/null | python3 -m json.tool`

Expected: JSON output, `"status": "pass"`, `"stage": "microbench"`, `metrics` is an object containing `spsc_p99_ns` and other ring metrics (keys per `shmem-microbench`'s output schema), `duration_s.microbench` non-zero.

(Walltime: ~30–90s for the microbench.)

- [ ] **Step 4: Commit**

```bash
git add uc_autobench/src/bin/run-iter.rs
git commit -m "feat(uc_autobench): run-iter shmem-microbench stage

180s hard timeout. Microbench stdout (one JSON object) is parsed and stored
on Output.metrics. Stage failures (spawn error, non-zero exit, malformed
JSON, timeout) surface as microbench_failed/timeout with stderr_tail set."
```

---

## Task 7: `run-iter` conditional e2e gate + final assembly

**Files:**
- Modify: `uc_autobench/src/bin/run-iter.rs`

- [ ] **Step 1: Insert the e2e gate stage**

In `uc_autobench/src/bin/run-iter.rs`, replace the lines:

```rust
    // E2E gate lands in Task 7.
    out.status = "pass".into();
    out.stage = "microbench".into();
    emit_and_exit(&out);
}
```

with:

```rust
    // Stage 4: e2e Goodhart gate (conditional)
    //
    // Pull spsc_p99_ns out of the microbench metrics for the gate decision.
    let spsc_p99_ns = out
        .metrics
        .as_ref()
        .and_then(|m| m.get("spsc_p99_ns"))
        .and_then(|v| v.as_u64());
    let Some(spsc_p99_ns) = spsc_p99_ns else {
        // Microbench succeeded but didn't emit the primary metric — treat
        // as a microbench failure rather than running the gate blind.
        out.status = "microbench_failed".into();
        out.stage = "microbench".into();
        out.stderr_tail = Some(
            "microbench stdout JSON missing required key `spsc_p99_ns`".to_string(),
        );
        emit_and_exit(&out);
    };

    match gate_decision(spsc_p99_ns, args.baseline_spsc_p99_ns) {
        GateDecision::Skip(reason) => {
            out.gate = Gate {
                ran: false,
                e2e_passed: None,
                submit_to_resp_p99_ns: None,
                baseline: args.baseline_e2e_p99_ns,
                regress_pct: None,
                reason: Some(reason),
            };
            out.status = "pass".into();
            out.stage = "microbench".into();
            emit_and_exit(&out);
        }
        GateDecision::Run => {
            let mut e2e_cmd = Command::new("cargo");
            e2e_cmd.args([
                "run",
                "-p",
                "uc_autobench",
                "--bin",
                "shmem-e2e",
                "--release",
                "--quiet",
                "--",
                "--json",
            ]);
            let e2e = run_stage(e2e_cmd, Duration::from_secs(300));
            out.duration_s.e2e = e2e.duration_s;
            if e2e.timed_out {
                out.status = "timeout".into();
                out.stage = "e2e".into();
                out.stderr_tail = Some(tail_lines(&e2e.stderr, 50));
                emit_and_exit(&out);
            }
            if !e2e.exit_ok {
                out.status = "e2e_failed".into();
                out.stage = "e2e".into();
                out.stderr_tail = Some(tail_lines(&e2e.stderr, 50));
                emit_and_exit(&out);
            }
            let e2e_json: serde_json::Value = match serde_json::from_str(e2e.stdout.trim()) {
                Ok(v) => v,
                Err(e) => {
                    out.status = "e2e_failed".into();
                    out.stage = "e2e".into();
                    out.stderr_tail = Some(format!(
                        "e2e stdout was not valid JSON: {e}\n--- raw ---\n{}",
                        tail_lines(&e2e.stdout, 30)
                    ));
                    emit_and_exit(&out);
                }
            };

            let submit_to_resp_p99_ns = e2e_json
                .get("submit_to_resp_p99_ns")
                .and_then(|v| v.as_u64());
            let regress_pct_val = match (submit_to_resp_p99_ns, args.baseline_e2e_p99_ns) {
                (Some(v), Some(baseline)) => Some(regress_pct(v, baseline)),
                _ => None,
            };
            let e2e_passed = match regress_pct_val {
                // No baseline (first iter): gate is informational; passes by default.
                None => Some(true),
                // 5% regression tolerance per the spec.
                Some(p) => Some(p <= 5.0),
            };

            out.gate = Gate {
                ran: true,
                e2e_passed,
                submit_to_resp_p99_ns,
                baseline: args.baseline_e2e_p99_ns,
                regress_pct: regress_pct_val,
                reason: None,
            };
            out.status = "pass".into();
            out.stage = "e2e".into();
            emit_and_exit(&out);
        }
    }
}
```

- [ ] **Step 2: Verify unit tests still pass**

Run: `cargo test -p uc_autobench --bin run-iter --release 2>&1 | tail -10`

Expected: 9 tests pass.

- [ ] **Step 3: Smoke-test the happy path (no baselines → gate runs)**

Run: `cargo run -p uc_autobench --bin run-iter --release --quiet -- --task shmem --json 2>/dev/null | python3 -m json.tool`

Expected: JSON output, `"status": "pass"`, `"stage": "e2e"`, `metrics` populated, `gate.ran: true`, `gate.e2e_passed: true`, `gate.submit_to_resp_p99_ns` is a non-zero integer, `duration_s.e2e` ≈ 20–40s.

(Total walltime: 1–3 min depending on cache state.)

- [ ] **Step 4: Smoke-test the gate-skip path (baselines force skip)**

Run: `cargo run -p uc_autobench --bin run-iter --release --quiet -- --task shmem --json --baseline-spsc-p99-ns 10 --baseline-e2e-p99-ns 38000 2>/dev/null | python3 -m json.tool`

(`--baseline-spsc-p99-ns 10` is impossibly low, so any real microbench result will exceed the 5% threshold and trigger Skip.)

Expected: JSON output, `"status": "pass"`, `"stage": "microbench"`, `gate.ran: false`, `gate.reason: "skipped_microbench_not_plausible"`, `duration_s.e2e: 0.0`.

- [ ] **Step 5: Commit**

```bash
git add uc_autobench/src/bin/run-iter.rs
git commit -m "feat(uc_autobench): run-iter conditional e2e Goodhart gate

Gate runs iff microbench spsc_p99_ns is within 5% of the supplied baseline
(or unconditionally on first iter when no baseline is given). 300s hard
timeout. submit_to_resp_p99_ns parsed from the e2e binary's JSON output;
regress_pct vs. baseline determines e2e_passed (5% tolerance per the spec).

Completes the run-iter consolidation helper."
```

---

## Task 8: Add `program.md` and per-task overlay

**Files:**
- Create: `uc_autobench/program.md`
- Create: `uc_autobench/tasks/shmem/program.md`

- [ ] **Step 1: Write `uc_autobench/program.md`**

Write the contents shown in design spec §5.1 (`docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`). The full body is:

```markdown
# uc_autobench

Autoresearch loop: you (Claude Code) propose code changes to optimize a target,
the harness measures, and you advance the branch on wins / revert on losses.

## Setup (run once at the start of a new run)

1. Confirm task: read `uc_autobench/tasks/<TASK>/program.md` for task-specific
   constraints (mutable paths, metric, baselines, special instructions).
2. Confirm working tree is clean (`git status`). If not, stop and ask.
3. Propose a run tag based on today's date (e.g. `may29`). The branch
   `autoresearch/<TASK>-<tag>` must not already exist.
4. Create the branch: `git checkout -b autoresearch/<TASK>-<tag>` from `main`.
5. If `uc_autobench/tasks/<TASK>/results.tsv` does not exist, create it with
   the header row described in the task overlay.
6. Confirm setup with the human, then begin the loop.

## The loop

LOOP FOREVER:

1. Read state: `cat uc_autobench/tasks/<TASK>/results.tsv`. Find current best.
2. Form a hypothesis. Write a one-line description.
3. Edit ONLY files in `mutable_paths` (see task overlay). Never touch
   `frozen_paths`. Never edit the microbench, e2e, torture, or fixture code.
4. Run: `cargo run -p uc_autobench --bin run-iter --release -- \
     --task <TASK> --json \
     --baseline-spsc-p99-ns <best> --baseline-e2e-p99-ns <e2e_best> \
     > /tmp/run-iter.json 2>&1`
5. Parse: `jq '.status, .metrics, .gate, .stderr_tail' /tmp/run-iter.json`.
6. Decide:
   - status=="pass" AND primary metric improved AND gate passed → KEEP:
     append TSV row, `git add -A`, `git commit -m "<TASK>: <description>"`.
   - status=="pass" but no improvement OR gate failed → DISCARD:
     append TSV row with status=discard, revert mutable paths with
     `git checkout -- <mutable_paths>`, then `git add` the TSV and commit
     `discard: <description>`.
   - status starts with "*_failed" or "timeout" → CRASH:
     same as DISCARD but status=crash and description includes the failure
     stage. If the failure looks like a trivial fix (typo, missing import),
     attempt up to 2 quick fixes before giving up.
7. GOTO 1.

## Rules

- NEVER stop on your own. The human will interrupt. If you're stuck, think
  harder: re-read the in-scope files, look for combinations of prior
  near-misses, try more radical structural changes.
- NEVER touch frozen files. NEVER add a dependency to `Cargo.toml`. NEVER
  modify `run-iter`, microbench, e2e, or torture binaries.
- Simplicity wins: a small gain that adds ugly complexity is not worth it.
  A wash that deletes code is always a keep.
- Use `git checkout --` for reverts, not `git reset --hard` (which would
  drop the TSV row you just added).
```

- [ ] **Step 2: Write `uc_autobench/tasks/shmem/program.md`**

Write the contents shown in design spec §5.2. The full body is:

```markdown
# Task: shmem-rings

Optimize uc_protocol shared-memory ring buffers for latency and throughput.

## Mutable paths

- uc_protocol/src/ring/spsc.rs
- uc_protocol/src/ring/mpsc.rs
- uc_protocol/src/ring/broadcast.rs
- uc_protocol/src/ring/common.rs

## Frozen paths (never edit)

- uc_protocol/src/lib.rs
- uc_protocol/src/ring/mod.rs       (public API surface)
- uc_autobench/tests/ring_torture.rs
- uc_autobench/src/bin/shmem-microbench.rs
- uc_autobench/src/bin/shmem-e2e.rs
- uc_autobench/src/bin/run-iter.rs
- uc_node/src/test_support.rs

## Metric

- Primary: `spsc_p99_ns` (minimize)
- Goodhart gate: `submit_to_resp_p99_ns` from shmem-e2e
  (5% regression tolerance vs current branch best)
- Floor: ring_torture must pass (6 tests, zero failures)

## TSV schema

`uc_autobench/tasks/shmem/results.tsv`, tab-separated:

    commit	spsc_p99_ns	e2e_p99_ns	memory_kb	status	description

Statuses: keep, discard, crash. Use 0 for metrics that didn't run.

## Constraints specific to this task

- `uc_protocol` is `no_std`-friendly: `core` only. No `std::`, no `tokio`,
  no `serde`, no allocations on the hot path beyond what the existing API
  already does.
- Ring buffer correctness: SPSC, MPSC (N producers, 1 consumer), Broadcast
  (1 producer, N subscribers). All operations must be lock-free in the
  steady state (memory-fence reorderings are fair game; locks aren't).
- Variable-length records with atomic-after-write length prefix. Reader
  sees length=0 → spin/yield.
- Ideas to consider (non-exhaustive, no obligation): cache-line padding of
  head/tail indices, weakening unnecessary `SeqCst` to `AcqRel`/`Release`,
  batched MPSC reservation, broadcast slot reuse strategies, prefetch hints,
  exponential vs linear backoff in spin loops.

## Operator-supplied baselines (filled in by the human at start of run)

- baseline_spsc_p99_ns: <TBD on first run>
- baseline_e2e_p99_ns:  <TBD on first run>
```

- [ ] **Step 3: Verify the files exist and contain the expected headers**

Run:
```bash
head -1 uc_autobench/program.md
head -1 uc_autobench/tasks/shmem/program.md
```

Expected: `# uc_autobench` and `# Task: shmem-rings` respectively.

- [ ] **Step 4: Commit**

```bash
git add uc_autobench/program.md uc_autobench/tasks/shmem/program.md
git commit -m "docs(uc_autobench): program.md loop spec + shmem task overlay

Generic loop instructions at uc_autobench/program.md describe the
karpathy-shape autoresearch flow Claude Code executes. Per-task overlay
at tasks/shmem/program.md declares mutable/frozen paths, the metric, the
TSV schema, and task-specific constraints (no_std posture, lock-free
correctness, hot-path allocation ban)."
```

---

## Task 9: Initialize empty `results.tsv` + clean up `.gitignore`

**Files:**
- Create: `uc_autobench/tasks/shmem/results.tsv`
- Modify: `.gitignore` (remove `auto-bench-runs/` entry if present)

- [ ] **Step 1: Create the TSV with the header row only**

Write to `uc_autobench/tasks/shmem/results.tsv` (TAB-separated — no spaces between column names):

```
commit	spsc_p99_ns	e2e_p99_ns	memory_kb	status	description
```

Then verify the file is actually tab-separated:

Run: `cat -A uc_autobench/tasks/shmem/results.tsv`

Expected: `commit^Ispsc_p99_ns^Ie2e_p99_ns^Imemory_kb^Istatus^Idescription$`. The `^I` markers are tabs; if you see spaces instead, rewrite the file.

- [ ] **Step 2: Remove `auto-bench-runs/` from `.gitignore` if present**

Run: `grep -n "auto-bench-runs" .gitignore`

If a line matches, remove it. (The directory is no longer used; the per-task TSV + branch carry all state.) If no line matches, skip — nothing to do here.

- [ ] **Step 3: Commit**

```bash
git add uc_autobench/tasks/shmem/results.tsv .gitignore
git commit -m "feat(uc_autobench): initialize shmem results.tsv, drop auto-bench-runs ignore

Empty TSV with the 6-column header lets the loop's first iteration find a
file to read. auto-bench-runs/ entry removed from .gitignore — the
directory is no longer produced now that branch + TSV carry run state."
```

---

## Task 10: Rewrite `uc_autobench/CLAUDE.md`

**Files:**
- Modify: `uc_autobench/CLAUDE.md`

- [ ] **Step 1: Replace `uc_autobench/CLAUDE.md` with the operator manual for the new flow**

Write to `uc_autobench/CLAUDE.md`:

```markdown
# uc_autobench

Claude-Code-driven autoresearch loop for ultima_cluster / ultima_db. Karpathy/autoresearch shape: Claude Code (you) edits a target file, runs a fixed harness (`run-iter`), commits or reverts via plain git, and iterates indefinitely.

For design rationale, see `../docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`.

## Running a task

Start a new run interactively from Claude Code:

```
Read uc_autobench/program.md and uc_autobench/tasks/shmem/program.md, then start a run.
```

That's it. There is no `auto-bench` binary, no API key, no orchestration daemon. The loop is the conversation: Claude Code reads `program.md`, executes the loop, the human watches and interrupts when satisfied.

## Reading a run

A run is one branch (`autoresearch/<task>-<tag>`) and one TSV (`tasks/<task>/results.tsv`).

- **What won?** The current best: the row in `results.tsv` with the lowest primary metric (`spsc_p99_ns` for shmem) and `status=keep`. The commit hash points to the source state.
- **Why was variant X rejected?** Read the row with `status=discard` or `status=crash` and the matching commit message in `git log`.
- **What did the loop try?** Tail the TSV: every iteration appended a row.

## Adding a new task

There is no scaffolding skill (yet). To add task `<id>`:

1. Write `tasks/<id>/program.md` — task overlay (see `tasks/shmem/program.md` as a reference).
2. Add a per-task fitness binary `src/bin/<id>-microbench.rs` (emits one JSON object on stdout, keys per the metric list in the overlay).
3. Add a per-task Goodhart gate `src/bin/<id>-e2e.rs` (also JSON-emitting). Skip if your task genuinely cannot be e2e-benched, but say so explicitly in the task overlay.
4. Add a frozen behavioral suite `tests/<id>_torture.rs`. **Do not skip this.**
5. Extend `src/bin/run-iter.rs`'s `--task` match arm to dispatch to the new bench binaries.
6. Initialize `tasks/<id>/results.tsv` with the header row matching the TSV schema declared in the overlay.

## The `run-iter` consolidation helper

```
cargo run -p uc_autobench --bin run-iter --release -- \
  --task shmem --json \
  --baseline-spsc-p99-ns <n> --baseline-e2e-p99-ns <n>
```

Drives build → ring_torture → shmem-microbench → conditional shmem-e2e. Emits one JSON object on stdout. The e2e gate is skipped if the microbench result is clearly not a winner (>5% over baseline). See the design spec §4 for the full output schema.

## The Goodhart gate

Every task should have an e2e gate. Without it, the LLM (you) will exploit any quirk of the microbench. The gate runs only on variants that microbench-plausibly win, so the cost amortizes.

If your task genuinely cannot be e2e-benched, document why in the task overlay so the loop is at least asked to avoid known cheap-shots.

## Cost & runtime expectations

- **API cost:** $0 — the loop uses your existing Claude Code subscription.
- **Walltime per iter:** dominated by build + test + microbench + e2e. ~2–4 min/iter for shmem, varies by task.

## Failure modes

The `status` field in the run-iter JSON output is one of:

| Status | Meaning | Action |
|--------|---------|--------|
| `pass` | All stages ran; check `gate.e2e_passed` and `metrics.spsc_p99_ns` to decide keep/discard |
| `build_failed` | `cargo build` failed | Read `stderr_tail`; usually a syntax error |
| `torture_failed` | `ring_torture` correctness suite failed | The proposal broke ring semantics; revert |
| `microbench_failed` | Microbench spawn/exit/JSON-parse failed | Read `stderr_tail`; if it's a panic in your edited code, that's a crash |
| `e2e_failed` | Same as above for the e2e gate | Usually a panic in the in-process node |
| `timeout` | Some stage exceeded its hard wall-clock budget | Stage is in `stage`; either your code spins forever or the timeout needs raising |

`build_failed` / `torture_failed` / `microbench_failed` / `e2e_failed` / `timeout` all map to TSV `status=crash`. `pass` with no improvement or failed gate maps to `status=discard`. `pass` with improvement + passed gate maps to `status=keep`.

## When NOT to use this framework

- You don't have a fast, deterministic fitness function.
- Correctness isn't crisply verifiable by tests.
- The search space has one obvious answer — write it by hand.
- You don't trust your own benchmarks. **The loop will optimize whatever you measure**, including measurement noise.

## Conventions

- Microbench binary name: `<task>-microbench`.
- E2E binary name: `<task>-e2e`.
- Torture test file: `tests/<task>_torture.rs`.
- All bench binaries emit one JSON object on stdout; everything else goes to stderr.
- Metric keys in JSON output must match what the task overlay declares (typo = silent miss).

## Pointers

- **Design:** `../docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`
- **Loop spec:** `program.md`
- **First task overlay:** `tasks/shmem/program.md`
```

- [ ] **Step 2: Verify the file is readable and references the right design doc**

Run: `head -5 uc_autobench/CLAUDE.md; grep -c "2026-05-29-uc-autobench-cc-driven-design" uc_autobench/CLAUDE.md`

Expected: header `# uc_autobench`; grep returns `2` (referenced twice: top + Pointers).

- [ ] **Step 3: Commit**

```bash
git add uc_autobench/CLAUDE.md
git commit -m "docs(uc_autobench): CLAUDE.md operator manual for CC-driven flow

Replaces the API-driven manual. Describes the program.md-based start
sequence, the run-iter JSON schema, branch+TSV state model, and the
status-to-TSV mapping (build_failed → crash, etc.)."
```

---

## Task 11: Rewrite `docs/tasks/task07_uc_autobench.md` + delete stale plan

**Files:**
- Modify: `docs/tasks/task07_uc_autobench.md`
- Delete: `docs/superpowers/plans/2026-05-24-uc-autobench.md`

(`docs/superpowers/specs/2026-05-24-uc-autobench-design.md` is preserved as historical record per the design spec §2.)

- [ ] **Step 1: Rewrite `docs/tasks/task07_uc_autobench.md`**

Write to `docs/tasks/task07_uc_autobench.md`:

```markdown
# Task 07 — uc_autobench: Claude-Code-driven autoresearch loop + shmem task

**Status:** Reworked on branch `auto_bench_shmem` (supersedes the prior API-driven design). The first real shmem autoresearch run has **not** yet been executed — it requires a human to kick off a Claude Code session, point it at `program.md`, and let it iterate.

`uc_autobench` is a karpathy/autoresearch-shape loop where Claude Code itself is the orchestrator: it reads `uc_autobench/program.md` + `uc_autobench/tasks/<task>/program.md`, edits the mutable target files, runs `cargo run -p uc_autobench --bin run-iter -- --task <task> --json`, parses the consolidated JSON, commits on win and `git checkout --` reverts on loss, appends a row to the committed per-task `results.tsv`, and loops indefinitely until the human interrupts. The first task optimizes the `uc_protocol` shmem ring buffers.

## Why the rework

The original design (`docs/superpowers/specs/2026-05-24-uc-autobench-design.md`) had a Rust orchestrator drive an Anthropic API client directly, which required a separately-billed `ANTHROPIC_API_KEY` outside the human's Claude Code subscription. The reworked design (`docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`) has Claude Code itself drive the loop, eliminating the separate API spend and ~2000 lines of orchestrator/LLM-client/proposal/sandbox/leaderboard/persist code in favor of a 50-line markdown loop spec plus a tiny `run-iter` consolidation helper.

## Working artifacts (consolidate, then delete, per project workflow)

Per CLAUDE.md's "Feature Development Workflow", these are ephemeral scaffolding. Once the rework lands and the first real run produces evidence, consolidate the design + plan into this file and delete the scaffolding:

- Design spec: `docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`
- Implementation plan: `docs/superpowers/plans/2026-05-29-uc-autobench-cc-driven.md`
- Historical record (do not delete): `docs/superpowers/specs/2026-05-24-uc-autobench-design.md` — the original API-driven design

## Code shipped (post-rework)

- `uc_autobench/` crate:
  - `program.md` — generic loop instructions (operator-facing).
  - `tasks/shmem/program.md` — per-task overlay (mutable paths, metric, TSV schema, task-specific constraints).
  - `tasks/shmem/results.tsv` — committed run log (header-only at init; grows row-per-iter).
  - `src/bin/run-iter.rs` — consolidation helper (build → ring_torture → microbench → conditional e2e gate → one JSON).
  - `src/bin/shmem-microbench.rs` — frozen fitness binary (8 metrics, batched-sample sub-tick latency).
  - `src/bin/shmem-e2e.rs` — frozen Goodhart gate (in-process node+service+4 clients via the M4 fixture).
  - `tests/ring_torture.rs` — frozen 6-test behavioral conformance suite.
  - `CLAUDE.md` — operator manual.
- `uc_node/src/test_support.rs` — `ClusterFixture` (behind the `test-support` feature), unchanged from the M4 extract; reused by `shmem-e2e`.

## Running the first shmem run

In a Claude Code session at this repo:

```
Read uc_autobench/program.md and uc_autobench/tasks/shmem/program.md, then start a run.
```

Claude Code will propose a run tag, create `autoresearch/shmem-<tag>` from `main`, run a baseline iteration to populate `results.tsv`, and then iterate. The branch + TSV are the only state. Interrupt with Ctrl-C when satisfied; review the winning row's commit; PR to `main`.

## Known limitations / retrospective inputs

These were surfaced during the original implementation and remain true under the rework. They should be revisited after the first real run:

- **The e2e Goodhart gate is Raft-commit-dominated.** End-to-end submit→response latency (~38 ms) is dominated by the journal group-commit window, not by ns-scale shmem ring time. So the e2e gate functions as a *throughput-collapse / correctness guard*, not a sensitive latency-regression detector for shmem changes. The microbench is the real fitness signal for the ring; the e2e gate's default sample is small (2k round-trips) accordingly.
- **Microbench latency resolution.** The host monotonic clock is coarse (~42 ns), so latency sub-benches use batched-sample timing (per-batch mean over many samples) to get sub-tick resolution. Run-to-run `spsc_p99_ns` is sensitive to background machine load; for trustworthy comparisons the loop should run benches without concurrent compilation.
- **Not yet implemented (deferred):** parallel variant execution (would need per-variant branches), opt-in loom verification of the champion, core-pinning for bench reproducibility, and a task-scaffolding skill (defer until a second task actually exists).

## Out of scope here

- Tranche 4: post-shmem framework retrospective + framework v1.1. Its own future task, executed after the first real shmem run produces evidence.
```

- [ ] **Step 2: Delete the stale `2026-05-24-uc-autobench.md` plan**

Run: `git rm docs/superpowers/plans/2026-05-24-uc-autobench.md`

(The 2026-05-24 *spec* is kept as historical record. Only the implementation *plan* for the dead design is deleted.)

- [ ] **Step 3: Verify task07 references the right docs**

Run: `grep -E "2026-05-(24|29)" docs/tasks/task07_uc_autobench.md`

Expected: at least one mention of `2026-05-29-uc-autobench-cc-driven-design.md` (the canonical new design) and one mention of `2026-05-24-uc-autobench-design.md` (the historical record). No mention of the deleted `2026-05-24-uc-autobench.md` plan.

- [ ] **Step 4: Commit**

```bash
git add docs/tasks/task07_uc_autobench.md docs/superpowers/plans/2026-05-24-uc-autobench.md
git commit -m "docs(tasks): rewrite task07 for CC-driven uc_autobench

Replaces the original task07 (API-driven framework) with the reworked
shape: program.md + run-iter, no Anthropic API spend, branch+TSV state
model. Deletes the stale 2026-05-24 implementation plan; keeps the
2026-05-24 design spec as historical record."
```

---

## Task 12: End-to-end manual smoke test

**Files:** none (smoke-only; no commits)

- [ ] **Step 1: Confirm working tree is clean**

Run: `git status --short`

Expected: empty output (or only untracked artifacts irrelevant to the rework).

- [ ] **Step 2: Cut a throwaway smoke branch**

```bash
git checkout -b autoresearch/shmem-smoke
```

- [ ] **Step 3: Make a trivial no-op edit to `spsc.rs`**

Append a single blank line to `uc_protocol/src/ring/spsc.rs` (do not modify any logic).

Run: `git diff uc_protocol/src/ring/spsc.rs` → confirm exactly one line added, no other changes.

- [ ] **Step 4: Run `run-iter` happy-path with explicit baselines**

Run:
```bash
cargo run -p uc_autobench --bin run-iter --release --quiet -- \
  --task shmem --json \
  --baseline-spsc-p99-ns 1000000 --baseline-e2e-p99-ns 1000000000 \
  > /tmp/run-iter-smoke.json 2>&1
jq '.status, .stage, .gate.ran, .gate.e2e_passed, .duration_s' /tmp/run-iter-smoke.json
```

(The huge baselines force the gate to run regardless of microbench result.)

Expected: `"pass"`, `"e2e"`, `true`, `true`, and a `duration_s` object with non-zero `build`, `torture`, `microbench`, `e2e` fields.

- [ ] **Step 5: Run `run-iter` skip-gate path with crushingly tight baseline**

Run:
```bash
cargo run -p uc_autobench --bin run-iter --release --quiet -- \
  --task shmem --json \
  --baseline-spsc-p99-ns 1 --baseline-e2e-p99-ns 1 \
  > /tmp/run-iter-skip.json 2>&1
jq '.status, .stage, .gate.ran, .gate.reason' /tmp/run-iter-skip.json
```

Expected: `"pass"`, `"microbench"`, `false`, `"skipped_microbench_not_plausible"`. `.duration_s.e2e` is `0`.

- [ ] **Step 6: Induce a build_failed result**

Append `let _ = undefined_symbol;` to a function body in `uc_protocol/src/ring/spsc.rs`.

Run:
```bash
cargo run -p uc_autobench --bin run-iter --release --quiet -- --task shmem --json \
  > /tmp/run-iter-fail.json 2>&1
jq '.status, .stage, .stderr_tail' /tmp/run-iter-fail.json
```

Expected: `"build_failed"`, `"build"`, and `stderr_tail` is a non-null string containing `undefined_symbol`.

- [ ] **Step 7: Clean up the smoke branch**

```bash
git checkout -- uc_protocol/src/ring/spsc.rs
git checkout auto_bench_shmem
git branch -D autoresearch/shmem-smoke
```

- [ ] **Step 8: Final verification**

Run:
```bash
git status --short
cargo build --workspace --release 2>&1 | tail -5
cargo clippy --workspace --release -- -D warnings 2>&1 | tail -5
cargo test -p uc_autobench --test ring_torture --release 2>&1 | tail -5
```

Expected: clean status; build clean; clippy clean; ring_torture passes (6 tests).

No commit for Task 12 — the smoke test is verification-only.

---

## Implementation order summary

| # | Task | Commit message |
|---|------|----------------|
| 1 | Delete API-driven Rust code | `refactor(uc_autobench): remove API-driven loop, prep for CC-driven autoresearch` |
| 2 | Delete `create-autobench-task` skill | `chore(.claude): remove create-autobench-task skill (superseded by program.md model)` |
| 3 | `run-iter` skeleton | `feat(uc_autobench): run-iter skeleton with CLI + JSON output types` |
| 4 | `run-iter` pure logic + tests | `feat(uc_autobench): run-iter pure logic (gate decision, regress pct, tail)` |
| 5 | `run-iter` build + torture stages | `feat(uc_autobench): run-iter build + ring_torture stages` |
| 6 | `run-iter` microbench stage | `feat(uc_autobench): run-iter shmem-microbench stage` |
| 7 | `run-iter` e2e gate + final | `feat(uc_autobench): run-iter conditional e2e Goodhart gate` |
| 8 | `program.md` + per-task overlay | `docs(uc_autobench): program.md loop spec + shmem task overlay` |
| 9 | Empty TSV + `.gitignore` cleanup | `feat(uc_autobench): initialize shmem results.tsv, drop auto-bench-runs ignore` |
| 10 | Rewrite `uc_autobench/CLAUDE.md` | `docs(uc_autobench): CLAUDE.md operator manual for CC-driven flow` |
| 11 | Rewrite task07 + delete stale plan | `docs(tasks): rewrite task07 for CC-driven uc_autobench` |
| 12 | End-to-end manual smoke test | (no commit) |
