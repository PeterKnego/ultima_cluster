use uc_autobench::llm::{LlmClient, StubClient};

#[test]
fn stub_returns_canned_proposals_in_order() {
    let p1 = make_proposal("first");
    let p2 = make_proposal("second");
    let client = StubClient::with_canned(vec![p1.clone(), p2.clone()]);
    let r1 = client.propose("sys", "user", 0.5).unwrap();
    let r2 = client.propose("sys", "user", 0.5).unwrap();
    assert_eq!(r1.hypothesis, "first");
    assert_eq!(r2.hypothesis, "second");
}

#[test]
fn stub_panics_when_exhausted() {
    let client = StubClient::with_canned(vec![]);
    let r = client.propose("s", "u", 0.5);
    assert!(r.is_err());
}

fn make_proposal(h: &str) -> uc_autobench::proposal::VariantProposal {
    uc_autobench::proposal::VariantProposal {
        hypothesis: h.into(),
        rationale: "r".into(),
        expected_outcome: serde_json::json!({}),
        risk_notes: "n".into(),
        files: std::collections::BTreeMap::new(),
    }
}
