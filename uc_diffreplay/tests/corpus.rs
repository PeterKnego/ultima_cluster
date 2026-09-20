mod common;
use uc_diffreplay::corpus::{Corpus, CorpusManifest};

#[test]
fn manifest_roundtrips_through_a_corpus_dir() {
    let dir = common::tempdir();
    let m = CorpusManifest {
        app_id: "app".into(),
        row: 3,
        origin: 4096,
        end: 8192,
        version: 0x0102_0003,
    };
    std::fs::create_dir_all(dir.path().join("journal")).unwrap();
    m.write(dir.path()).unwrap();
    let c = Corpus::open(dir.path()).unwrap();
    assert_eq!(c.manifest, m);
    assert_eq!(c.journal_dir(), dir.path().join("journal"));
    assert_eq!(
        c.artifact(),
        dir.path()
            .join("snapshots")
            .join("3")
            .join("snap-4096.ultsnap")
    );
}

use uc_client::Client;
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};
use uc_service::{ServiceBuilder, ServiceConfig};

/// Drive a single node with RegisterSm: N writes, an instant at P, M more
/// writes. Returns `(P, u64::MAX)` — `Node` has no "applied frontier"
/// accessor in this plan's scope, so the end is left to the caller (the
/// driver stops at the journal's last frame instead of naming Q precisely).
pub fn build_register_history(dir: &std::path::Path, app_id: &str, n: u64, m: u64) -> (u64, u64) {
    let node = common::start_single_node(dir, app_id, common::register_name());
    common::wait_until(|| node.can_serve());
    let cfg = ServiceConfig::new(dir.to_path_buf(), app_id.to_string());
    let svc = ServiceBuilder::new(cfg, RegisterSm::default())
        .start_with_snapshots()
        .unwrap();
    let client = Client::connect(dir, app_id).unwrap();
    for v in 0..n {
        let _: CmdResp = client.submit(&Cmd::Write(v)).unwrap();
    }
    let p = common::command_instant(&node);
    // The instant completes when the row's artifact appears.
    let art = dir
        .join("snapshots")
        .join("0")
        .join(format!("snap-{p}.ultsnap"));
    common::wait_until(|| art.is_file());
    for v in n..n + m {
        let _: CmdResp = client.submit(&Cmd::Write(v)).unwrap();
    }
    client.shutdown();
    svc.stop();
    node.stop();
    (p, u64::MAX)
}

#[test]
fn export_captures_artifact_and_journal() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = build_register_history(inst.path(), "corp", 5, 5);
    // Q: use the journal's own extent — the end is "everything archived".
    let c = Corpus::export(inst.path(), "corp", 0, p, u64::MAX, 0, out.path()).unwrap();
    assert!(c.artifact().is_file());
    assert!(c.journal_dir().is_dir());
    assert_eq!(c.manifest.origin, p);
}
