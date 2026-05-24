use std::collections::BTreeMap;
use std::path::PathBuf;
use uc_autobench::leaderboard::Entry;
use uc_autobench::prompt::{PromptContext, render_system_prompt, render_user_message};
use uc_autobench::task::{BenchResult, TaskSpec};

fn load_shmem_spec() -> TaskSpec {
    let s = std::fs::read_to_string("tasks/shmem/task.toml").unwrap();
    TaskSpec::from_toml_str(&s).unwrap()
}

fn entry(id: &str, p: f64) -> Entry {
    Entry {
        variant_id: id.into(),
        primary_metric: p,
        diversity_tag: [0; 32],
        hypothesis: format!("hyp-{id}"),
    }
}

#[test]
fn system_prompt_mentions_contract_paths() {
    let spec = load_shmem_spec();
    let extra = "Remember: atomic-after-write length prefix.";
    let s = render_system_prompt(&spec, extra);
    assert!(s.contains("spsc.rs"));
    assert!(s.contains("ring/mod.rs"));
    assert!(s.contains("spsc_p99_ns"));
    assert!(s.contains("atomic-after-write"));
}

#[test]
fn user_message_includes_current_best_and_diverse_leaders_and_rejections() {
    let spec = load_shmem_spec();
    let mut current_files = BTreeMap::new();
    current_files.insert(
        PathBuf::from("uc_protocol/src/ring/spsc.rs"),
        "// current\n".into(),
    );
    let mut metrics = std::collections::BTreeMap::new();
    metrics.insert("spsc_p99_ns".into(), 1180.0);
    let ctx = PromptContext {
        spec: &spec,
        current_best_id: "0042-baseline",
        current_best_files: &current_files,
        current_best_metrics: &BenchResult { metrics },
        diverse_leaders: vec![entry("0017", 1310.0), entry("0033", 1240.0)],
        recent_rejections: vec!["#0061 GOODHART: ...".into(), "#0060 TEST_FAIL: ...".into()],
        temperature: 0.7,
        temperature_explanation: "plateau 18 iters",
    };
    let msg = render_user_message(&ctx);
    assert!(msg.contains("0042-baseline"));
    assert!(msg.contains("// current"));
    assert!(msg.contains("0017"));
    assert!(msg.contains("hyp-0033"));
    assert!(msg.contains("GOODHART"));
    assert!(msg.contains("0.7"));
}
