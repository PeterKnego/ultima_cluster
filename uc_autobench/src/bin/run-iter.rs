//! run-iter — single-command per-iteration harness for uc_autobench.
//!
//! Spawns build → ring_torture → shmem-microbench → (conditional) shmem-e2e,
//! emits one JSON object on stdout describing the outcome. The agent reads
//! the JSON `status` field, not the exit code. Exit 0 even on stage failure;
//! non-zero only on this binary's own internal bug.
//!
//! See `docs/superpowers/specs/2026-05-29-uc-autobench-cc-driven-design.md`.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use clap::Parser;
use serde::Serialize;
use wait_timeout::ChildExt;

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

/// Outcome of the gate-run decision. The Goodhart e2e gate is expensive
/// (~40s); we skip it when the microbench shows the variant clearly isn't
/// a winner — saves wall time per iteration.
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)] // wired into main() in Task 7
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
#[allow(dead_code)] // wired into main() in Task 7
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
#[allow(dead_code)] // wired into main() in Task 7
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

fn emit_and_exit(out: &Output) -> ! {
    println!("{}", serde_json::to_string(out).expect("serialize Output"));
    std::process::exit(0);
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
