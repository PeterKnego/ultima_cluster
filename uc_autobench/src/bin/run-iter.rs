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
#[allow(dead_code)] // wired into main() in Task 7
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
