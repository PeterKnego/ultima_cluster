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
