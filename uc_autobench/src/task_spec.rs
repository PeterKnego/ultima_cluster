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
    /// `--test` name of the frozen conformance suite run before the microbench.
    pub torture_test: &'static str,
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
            torture_test: "ring_torture",
            gate_bin: Some("shmem-e2e"),
            gate_metric: Some("submit_to_resp_p99_ns"),
        }),
        // Moved in with the ultima_journal crate. primary_metric is a
        // *maximize* (throughput) metric, which run-iter's lower-is-better
        // gate-trigger can't reason about — so `gate_bin: None` skips the
        // auto e2e gate; the journal_torture floor + microbench are the signal.
        "journal-commit" => Some(TaskSpec {
            task: "journal-commit",
            microbench_bin: "journal-microbench",
            primary_metric: "group_commit_throughput",
            torture_test: "journal_torture",
            gate_bin: None,
            gate_metric: None,
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
        assert_eq!(s.torture_test, "ring_torture");
        assert_eq!(s.gate_bin, Some("shmem-e2e"));
        assert_eq!(s.gate_metric, Some("submit_to_resp_p99_ns"));

        let j = task_spec("journal-commit").expect("journal-commit is known");
        assert_eq!(j.microbench_bin, "journal-microbench");
        assert_eq!(j.primary_metric, "group_commit_throughput");
        assert_eq!(j.torture_test, "journal_torture");
        assert_eq!(j.gate_bin, None);
    }

    #[test]
    fn unknown_task_is_none() {
        assert!(task_spec("does-not-exist").is_none());
    }
}
