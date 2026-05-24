//! Task specification types and the [`OptimizationTask`] trait (added in Task 7).
//!
//! `TaskSpec` mirrors the TOML schema in spec §3.1.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskSpec {
    pub task: TaskMeta,
    pub contract: Contract,
    pub gates: Gates,
    pub microbench: BenchCfg,
    #[serde(default)]
    pub e2e_gate: Option<BenchCfg>,
    pub budget: Budget,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskMeta {
    pub id: String,
    pub description: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Contract {
    pub mode: ContractMode,
    pub mutable_paths: Vec<PathBuf>,
    pub frozen_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContractMode {
    RustApi,
    RustApiPlusWire,
    BehaviorOnly,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Gates {
    pub test_cmd: String,
    pub torture_cmd: String,
    pub build_timeout_s: u64,
    pub test_timeout_s: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BenchCfg {
    pub cmd: String,
    /// Required for the primary microbench. `e2e_gate` may have a single primary
    /// only (its `metrics` list, if present, is optional metadata).
    #[serde(default)]
    pub metrics: Vec<String>,
    pub primary: String,
    pub primary_dir: Direction,
    /// Only meaningful for `e2e_gate`. Percentage regression vs current best
    /// that auto-rejects the variant as Goodhart.
    #[serde(default)]
    pub regress_pct: Option<f64>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Minimize,
    Maximize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Budget {
    pub max_iterations: u32,
    pub plateau_window: u32,
    pub wall_clock_hours: f64,
}

impl TaskSpec {
    pub fn from_toml_str(s: &str) -> anyhow::Result<Self> {
        toml::from_str(s).map_err(Into::into)
    }
}

/// Output of one bench invocation. Keys are metric names; values are f64.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct BenchResult {
    pub metrics: BTreeMap<String, f64>,
}

impl BenchResult {
    pub fn from_json_line(line: &str) -> anyhow::Result<Self> {
        let metrics: BTreeMap<String, f64> = serde_json::from_str(line.trim())?;
        Ok(Self { metrics })
    }

    pub fn primary(&self, name: &str) -> Option<f64> {
        self.metrics.get(name).copied()
    }
}
