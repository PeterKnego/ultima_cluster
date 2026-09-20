//! Every checked-in corpus under `tests/corpora/`, replayed on every
//! `cargo test` (spec §5.9): `determinism` mode against THIS build, plus a
//! content check that the corpus still records what its name claims.
//!
//! The content check is not decoration. A determinism run compares one build
//! against itself, so a corpus generated with the wrong command bytes —
//! missing the `Sessioned` envelope, say — replays as garbage on both sides
//! and passes anyway. Asserting the projection at the origin and after the
//! span is what makes the pass mean something.
//!
//! Needs two binaries beside each other in the cargo target directory:
//!
//!     cargo build -p uc_diffreplay -p kv_store

use std::path::{Path, PathBuf};
use std::process::Command;

use uc_diffreplay::attribute::Declaration;
use uc_diffreplay::corpus::Corpus;
use uc_diffreplay::trace::Trace;

fn corpora_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("corpora")
}

fn corpora() -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(corpora_root())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.join("CORPUS").is_file())
        .collect();
    v.sort();
    assert!(!v.is_empty(), "no corpora under {:?}", corpora_root());
    v
}

fn kv_service() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_kv-service"))
}

/// `uc2-diffreplay` is a *dependency's* binary, so `cargo test -p kv_store`
/// does not build it and there is no `CARGO_BIN_EXE_` for it. Assert rather
/// than skip (a silently skipped regression gate is not a gate) and name the
/// command that fixes it.
fn uc2_diffreplay() -> PathBuf {
    let mut p = kv_service();
    p.set_file_name("uc2-diffreplay");
    assert!(
        p.exists(),
        "missing {} — run `cargo build -p uc_diffreplay` first",
        p.display()
    );
    p
}

fn scratch() -> PathBuf {
    let d = Path::new(env!("CARGO_TARGET_TMPDIR")).join("kv-regression-corpora");
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn every_regression_corpus_is_deterministic_under_this_build() {
    let (bin, diffreplay) = (kv_service(), uc2_diffreplay());
    for c in corpora() {
        // "A corpus without its intent.toml is a recording, not a test"
        // (README) — and a declaration that does not parse is worse than
        // none, because nothing reads it until an upgrade run needs it.
        let decl = c.join("intent.toml");
        assert!(decl.is_file(), "{} has no intent.toml", c.display());
        Declaration::from_toml(&std::fs::read_to_string(&decl).unwrap()).unwrap();

        // The report lands beside the corpus (`tests/corpora/.gitignore`
        // covers it and the traces directory `uc2-diffreplay` puts next to
        // it), and is removed on success — a leftover means a failed run.
        let report = c.join("determinism.report.json");
        let st = Command::new(&diffreplay)
            .arg("determinism")
            .arg("--corpus")
            .arg(&c)
            .arg("--bin")
            .arg(&bin)
            .arg("--report")
            .arg(&report)
            .status()
            .unwrap();
        assert!(st.success(), "determinism failed for {}", c.display());
        let _ = std::fs::remove_file(&report);
        let _ = std::fs::remove_dir_all(c.join("determinism.report.traces"));
    }
}

/// The content check, for the one corpus whose name is a claim about its
/// content: the artifact at the origin holds both puts, and replaying the
/// span above the origin removes the deleted key and only that key.
#[test]
fn the_put_then_delete_corpus_records_two_puts_and_one_delete() {
    let dir = corpora_root().join("put-then-delete");
    let c = Corpus::open(&dir).unwrap();

    // `key=61` is b"a", `key=62` is b"b" (KvSm::project hex-encodes keys).
    let out = Command::new(kv_service())
        .arg("project")
        .arg("--artifact")
        .arg(c.artifact())
        .arg("--position")
        .arg(c.manifest.origin.to_string())
        .output()
        .unwrap();
    assert!(out.status.success(), "project: {:?}", out.status);
    let at_origin = String::from_utf8(out.stdout).unwrap();
    assert!(at_origin.contains("\nkey=61 "), "at origin:\n{at_origin}");
    assert!(at_origin.contains("\nkey=62 "), "at origin:\n{at_origin}");

    let trace_path = scratch().join("put-then-delete.trace.json");
    let st = Command::new(kv_service())
        .arg("replay")
        .arg("--corpus")
        .arg(&dir)
        .arg("--out")
        .arg(&trace_path)
        .status()
        .unwrap();
    assert!(st.success(), "replay: {st}");
    let trace = Trace::read_json(std::fs::File::open(&trace_path).unwrap()).unwrap();

    // One command above the origin, and it is the delete: the tag is the
    // 16-byte session envelope then `FORMAT_VERSION ‖ OP_DELETE` — the two
    // bytes `intent.toml`'s `[tags]` names at `tag_offset = 16`.
    assert_eq!(trace.entries.len(), 1, "{:?}", trace.entries);
    let tag = &trace.entries[0].tag;
    assert!(tag.len() > 17, "tag too short for an envelope: {tag:?}");
    assert_eq!(
        &tag[16..18],
        &[kv_store::wire::FORMAT_VERSION, kv_store::wire::OP_DELETE]
    );
    // …and this corpus's own declaration resolves that tag to the arm it
    // names. This is the one assertion that exercises `tag_offset = 16`
    // end to end: at offset 0 the tag reads as a client id and matches
    // nothing.
    let decl =
        Declaration::from_toml(&std::fs::read_to_string(dir.join("intent.toml")).unwrap()).unwrap();
    assert_eq!(decl.tag_offset, 16);
    assert_eq!(decl.arm_of(tag), Some("delete"));

    let at_end = trace.projection_at_end.unwrap();
    assert!(!at_end.contains("\nkey=61 "), "at end:\n{at_end}");
    assert!(at_end.contains("\nkey=62 "), "at end:\n{at_end}");
}
