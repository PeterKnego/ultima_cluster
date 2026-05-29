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
