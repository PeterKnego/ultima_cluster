use uc_autobench::outcome::{LoopEvent, Outcome};
use uc_autobench::task::BenchResult;

#[test]
fn outcome_promoted_roundtrips() {
    let mut metrics = std::collections::BTreeMap::new();
    metrics.insert("spsc_p99_ns".to_string(), 1080.0);
    let o = Outcome::Promoted {
        microbench: BenchResult {
            metrics: metrics.clone(),
        },
        e2e: None,
    };
    let s = serde_json::to_string(&o).unwrap();
    let back: Outcome = serde_json::from_str(&s).unwrap();
    assert_eq!(o, back);
}

#[test]
fn event_round_trip_jsonl_line() {
    let evt = LoopEvent::RunStarted {
        t: "2026-05-24T16:30:01Z".into(),
        run_id: "abc12".into(),
        task: "shmem-rings".into(),
        git_head: "965d1ec".into(),
    };
    let line = serde_json::to_string(&evt).unwrap();
    assert!(line.contains("\"kind\":\"run_started\""));
    let back: LoopEvent = serde_json::from_str(&line).unwrap();
    assert!(matches!(back, LoopEvent::RunStarted { .. }));
}
