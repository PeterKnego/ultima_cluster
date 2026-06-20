//! run-iter — single-command per-iteration harness for uc_autobench.
//!
//! Spawns build → <task>-torture → <task>-microbench → (conditional) e2e gate
//! (binaries/tests chosen per `task_spec`), emits one JSON object on stdout
//! describing the outcome. The agent reads
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
///
/// stdout and stderr are drained concurrently in background threads so the
/// child can't block on a full pipe buffer. shmem-e2e in particular emits
/// torrential WARN traces on stderr; without concurrent drainage the child
/// runs ~10× slower and overshoots the 300s gate budget.
fn run_stage(mut cmd: Command, timeout: Duration) -> StageRun {
    use std::io::Read;
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

    let mut stdout_pipe = child.stdout.take().expect("stdout piped");
    let mut stderr_pipe = child.stderr.take().expect("stderr piped");
    let stdout_handle = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = stdout_pipe.read_to_string(&mut s);
        s
    });
    let stderr_handle = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = stderr_pipe.read_to_string(&mut s);
        s
    });

    let status = match child.wait_timeout(timeout).expect("wait_timeout") {
        Some(s) => Some(s),
        None => {
            let _ = child.kill();
            let _ = child.wait();
            None
        }
    };

    let stdout = stdout_handle.join().unwrap_or_default();
    let stderr = stderr_handle.join().unwrap_or_default();

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

/// Extract a u64 from a JSON value. Accepts an integer (`u64`) or a float
/// that is rounded to the nearest integer (the microbench emits its
/// percentile timings as fractional nanoseconds).
fn extract_u64(v: &serde_json::Value) -> Option<u64> {
    if let Some(n) = v.as_u64() {
        return Some(n);
    }
    if let Some(f) = v.as_f64()
        && f.is_finite()
        && f >= 0.0
    {
        return Some(f.round() as u64);
    }
    None
}

fn main() {
    let args = Args::parse();

    let spec = match uc_autobench::task_spec::task_spec(&args.task) {
        Some(s) => s,
        None => {
            let out = Output {
                status: "unknown_task".to_string(),
                stage: "setup".to_string(),
                stderr_tail: Some(format!("unknown task {:?}; known tasks: shmem", args.task)),
                ..Default::default()
            };
            emit_and_exit(&out);
        }
    };
    if !args.json {
        eprintln!("run-iter: only --json output mode is supported in v1");
        std::process::exit(2);
    }

    let mut out = Output::default();

    // Stage 1: build
    let mut build_cmd = Command::new("cargo");
    build_cmd.args([
        "build",
        "-p",
        "uc_protocol",
        "-p",
        "uc_autobench",
        "--release",
    ]);
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

    // Stage 2: per-task conformance (frozen torture suite)
    let mut torture_cmd = Command::new("cargo");
    torture_cmd.args([
        "test",
        "-p",
        "uc_autobench",
        "--test",
        spec.torture_test,
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
        spec.microbench_bin,
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

    // Stage 4: e2e Goodhart gate (conditional)
    //
    // Pull spsc_p99_ns out of the microbench metrics for the gate decision.
    // The microbench emits this as a float; round to u64 for gate_decision().
    let spsc_p99_ns = out
        .metrics
        .as_ref()
        .and_then(|m| m.get(spec.primary_metric))
        .and_then(extract_u64);
    let Some(spsc_p99_ns) = spsc_p99_ns else {
        // Microbench succeeded but didn't emit the primary metric — treat
        // as a microbench failure rather than running the gate blind.
        out.status = "microbench_failed".into();
        out.stage = "microbench".into();
        out.stderr_tail = Some(format!(
            "microbench stdout JSON missing required key `{}`",
            spec.primary_metric
        ));
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
            let Some(gate_bin) = spec.gate_bin else {
                // No Goodhart gate for this task: pass through with gate.ran = false.
                out.gate = Gate {
                    ran: false,
                    reason: Some("no_gate_bin_for_task".to_string()),
                    ..Default::default()
                };
                out.status = "pass".to_string();
                out.stage = "microbench".to_string();
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

            let gate_metric = spec.gate_metric.unwrap_or("submit_to_resp_p99_ns");
            let submit_to_resp_p99_ns = e2e_json.get(gate_metric).and_then(extract_u64);
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
