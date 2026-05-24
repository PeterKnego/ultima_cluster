use uc_autobench::task::TaskSpec;

#[test]
fn shmem_task_toml_parses() {
    let s = std::fs::read_to_string("tasks/shmem/task.toml").unwrap();
    let spec = TaskSpec::from_toml_str(&s).unwrap();
    assert_eq!(spec.task.id, "shmem-rings");
    assert_eq!(spec.microbench.primary, "spsc_p99_ns");
    assert!(spec.e2e_gate.is_some());
    let e2e = spec.e2e_gate.as_ref().unwrap();
    assert_eq!(e2e.regress_pct, Some(5.0));
    assert_eq!(spec.contract.mutable_paths.len(), 4);
}

#[test]
fn bench_result_parses_json_line() {
    let line = r#"{"spsc_p99_ns": 1180.0, "spsc_throughput_msgs": 8400000.0}"#;
    let r = uc_autobench::task::BenchResult::from_json_line(line).unwrap();
    assert_eq!(r.primary("spsc_p99_ns"), Some(1180.0));
    assert_eq!(r.primary("missing"), None);
}
