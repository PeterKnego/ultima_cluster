use std::collections::BTreeMap;
use std::path::PathBuf;
use uc_autobench::proposal::{static_checks, StaticCheckResult, VariantProposal};

fn proposal_touching(paths: &[&str]) -> VariantProposal {
    let mut files = BTreeMap::new();
    for p in paths {
        files.insert(PathBuf::from(p), "// new content\n".to_string());
    }
    VariantProposal {
        hypothesis: "h".into(),
        rationale: "r".into(),
        expected_outcome: serde_json::json!({}),
        risk_notes: "n".into(),
        files,
    }
}

#[test]
fn rejects_when_frozen_path_touched() {
    let p = proposal_touching(&["uc_protocol/src/lib.rs"]);
    let r = static_checks(
        &p,
        &[PathBuf::from("uc_protocol/src/ring/spsc.rs")],
        &[PathBuf::from("uc_protocol/src/lib.rs")],
    );
    assert!(matches!(r, StaticCheckResult::Reject { .. }), "got {r:?}");
}

#[test]
fn rejects_when_path_not_in_mutable_set() {
    let p = proposal_touching(&["uc_protocol/src/something_else.rs"]);
    let r = static_checks(
        &p,
        &[PathBuf::from("uc_protocol/src/ring/spsc.rs")],
        &[PathBuf::from("uc_protocol/src/lib.rs")],
    );
    assert!(matches!(r, StaticCheckResult::Reject { .. }));
}

#[test]
fn accepts_when_only_mutable_paths_touched() {
    let p = proposal_touching(&["uc_protocol/src/ring/spsc.rs"]);
    let r = static_checks(
        &p,
        &[
            PathBuf::from("uc_protocol/src/ring/spsc.rs"),
            PathBuf::from("uc_protocol/src/ring/mpsc.rs"),
        ],
        &[PathBuf::from("uc_protocol/src/lib.rs")],
    );
    assert!(matches!(r, StaticCheckResult::Ok));
}

#[test]
fn empty_files_map_rejected() {
    let p = proposal_touching(&[]);
    let r = static_checks(&p, &[PathBuf::from("any")], &[]);
    assert!(matches!(r, StaticCheckResult::Reject { .. }));
}
