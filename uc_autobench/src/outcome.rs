//! Outcome of one variant attempt + the event log type written to events.jsonl.

use crate::task::BenchResult;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    /// Patch touched a frozen path or failed fmt/`cargo check`. Cheap; does not
    /// count against the improvement plateau.
    StaticReject { reason: String },
    /// `cargo test` or torture suite failed. Includes truncated stderr.
    TestFail { reason: String },
    /// Microbench ran but did not beat current best by > noise threshold.
    BenchRegression { metrics: BenchResult },
    /// Microbench beat best, but e2e gate regressed > regress_pct.
    GoodhartReject {
        microbench: BenchResult,
        e2e: BenchResult,
        regress_pct: f64,
    },
    /// Promoted to current best.
    Promoted {
        microbench: BenchResult,
        e2e: Option<BenchResult>,
    },
    /// Iteration started but never completed (process killed mid-flight).
    /// Synthesized at resume time.
    ResumedAborted,
}

/// One line in `events.jsonl`. Written **before** the work it represents starts
/// (proposal, gate, microbench, e2e) and **after** for `outcome` and lifecycle events.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LoopEvent {
    RunStarted {
        t: String,
        run_id: String,
        task: String,
        git_head: String,
    },
    VariantProposed {
        t: String,
        variant: String,
        proposal_path: PathBuf,
    },
    StaticCheck {
        t: String,
        variant: String,
        ok: bool,
    },
    CorrectnessGate {
        t: String,
        variant: String,
        ok: bool,
        duration_ms: u64,
    },
    Microbench {
        t: String,
        variant: String,
        ok: bool,
        #[serde(default)]
        metrics: Option<BenchResult>,
    },
    E2eGate {
        t: String,
        variant: String,
        ok: bool,
        #[serde(default)]
        metrics: Option<BenchResult>,
    },
    OutcomeRecorded {
        t: String,
        variant: String,
        outcome: Outcome,
        prev_best: Option<String>,
    },
    PlateauTemperature {
        t: String,
        new_temp: f32,
        reason: String,
    },
    RunEnded {
        t: String,
        reason: String,
        best: Option<String>,
    },
}

impl LoopEvent {
    pub fn variant_id(&self) -> Option<&str> {
        match self {
            LoopEvent::VariantProposed { variant, .. }
            | LoopEvent::StaticCheck { variant, .. }
            | LoopEvent::CorrectnessGate { variant, .. }
            | LoopEvent::Microbench { variant, .. }
            | LoopEvent::E2eGate { variant, .. }
            | LoopEvent::OutcomeRecorded { variant, .. } => Some(variant),
            _ => None,
        }
    }
}
