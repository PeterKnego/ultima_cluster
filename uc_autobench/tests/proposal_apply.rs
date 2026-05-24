use std::collections::BTreeMap;
use std::path::PathBuf;
use tempfile::TempDir;
use uc_autobench::proposal::{apply_patch, restore_snapshot, snapshot_files, VariantProposal};

fn write(root: &std::path::Path, rel: &str, content: &str) {
    let abs = root.join(rel);
    std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
    std::fs::write(abs, content).unwrap();
}
fn read(root: &std::path::Path, rel: &str) -> String {
    std::fs::read_to_string(root.join(rel)).unwrap()
}

#[test]
fn apply_then_restore_yields_original_content() {
    let dir = TempDir::new().unwrap();
    let root = dir.path();
    write(root, "a/b.rs", "ORIGINAL\n");
    write(root, "a/c.rs", "ORIG_C\n");

    let snap = snapshot_files(root, &[PathBuf::from("a/b.rs"), PathBuf::from("a/c.rs")]).unwrap();

    let mut files = BTreeMap::new();
    files.insert(PathBuf::from("a/b.rs"), "MUTATED_B\n".into());
    let proposal = VariantProposal {
        hypothesis: "h".into(),
        rationale: "r".into(),
        expected_outcome: serde_json::json!({}),
        risk_notes: "n".into(),
        files,
    };
    apply_patch(root, &proposal).unwrap();
    assert_eq!(read(root, "a/b.rs"), "MUTATED_B\n");
    assert_eq!(read(root, "a/c.rs"), "ORIG_C\n");

    restore_snapshot(root, &snap).unwrap();
    assert_eq!(read(root, "a/b.rs"), "ORIGINAL\n");
    assert_eq!(read(root, "a/c.rs"), "ORIG_C\n");
}

#[test]
fn snapshot_handles_missing_files_by_marking_them() {
    let dir = TempDir::new().unwrap();
    let root = dir.path();
    write(root, "exists.rs", "X\n");
    let snap = snapshot_files(
        root,
        &[PathBuf::from("exists.rs"), PathBuf::from("does_not_exist.rs")],
    )
    .unwrap();
    // Mutate exists and create does_not_exist
    std::fs::write(root.join("exists.rs"), "MUT\n").unwrap();
    std::fs::write(root.join("does_not_exist.rs"), "NEW\n").unwrap();
    restore_snapshot(root, &snap).unwrap();
    assert_eq!(read(root, "exists.rs"), "X\n");
    assert!(
        !root.join("does_not_exist.rs").exists(),
        "missing-before file should be removed on restore"
    );
}
