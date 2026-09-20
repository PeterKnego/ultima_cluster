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

#[test]
fn export_captures_artifact_and_journal() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "corp", 5, 5);
    // Q: use the journal's own extent — the end is "everything archived".
    let c = Corpus::export(inst.path(), "corp", 0, p, u64::MAX, 0, out.path()).unwrap();
    assert!(c.artifact().is_file());
    assert!(c.journal_dir().is_dir());
    assert_eq!(c.manifest.origin, p);
}
