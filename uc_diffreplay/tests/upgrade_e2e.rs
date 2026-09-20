mod common;
use std::path::PathBuf;
use std::process::Command;

use uc_diffreplay::corpus::Corpus;

fn bin(name: &str) -> PathBuf {
    // Built by cargo for this test binary's profile: target/<profile>/<name>.
    let mut p = PathBuf::from(env!("CARGO_BIN_EXE_uc2-diffreplay"));
    p.set_file_name(name);
    p
}

fn register_replay_bin() -> PathBuf {
    let p = bin("register-replay");
    assert!(
        p.exists(),
        "build it first: cargo build -p uc_lincheck --features replay-bin --bin register-replay ({})",
        p.display()
    );
    p
}

#[test]
fn same_binary_twice_passes_with_an_empty_declaration() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "e2e", 4, 4);
    Corpus::export(inst.path(), "e2e", 0, p, u64::MAX, 0, out.path()).unwrap();
    let report = out.path().join("det.json");
    let st = Command::new(env!("CARGO_BIN_EXE_uc2-diffreplay"))
        .args(["determinism", "--corpus"])
        .arg(out.path())
        .arg("--bin")
        .arg(register_replay_bin())
        .arg("--report")
        .arg(&report)
        .status()
        .unwrap();
    assert!(st.success());
    let r: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&report).unwrap()).unwrap();
    assert_eq!(r["summary"]["pass"], 0);
    assert_eq!(r["summary"]["unexplained"], 0);
}

#[test]
fn upgrade_with_a_declared_absent_change_fails_with_absent() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "up", 4, 4);
    Corpus::export(inst.path(), "up", 0, p, u64::MAX, 0, out.path()).unwrap();
    let decl = out.path().join("intent.toml");
    std::fs::write(
        &decl,
        "[tags]\n\"00\" = \"write\"\n[touched]\narms = [\"write\"]\n[[expect]]\nsurface = \"projection_end\"\nnote = \"values doubled\"\n",
    )
    .unwrap();
    let report = out.path().join("up.json");
    let st = Command::new(env!("CARGO_BIN_EXE_uc2-diffreplay"))
        .args(["upgrade", "--corpus"])
        .arg(out.path())
        .arg("--old")
        .arg(register_replay_bin())
        .arg("--new")
        .arg(register_replay_bin()) // same binary: nothing changes
        .arg("--declare")
        .arg(&decl)
        .arg("--report")
        .arg(&report)
        .status()
        .unwrap();
    assert_eq!(st.code(), Some(1));
    let r: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(&report).unwrap()).unwrap();
    assert_eq!(r["summary"]["absent"], 1);
}

/// Spec §6.1 `--around <pos>`: the newest complete artifact AT OR BELOW `pos`
/// is the origin. `Corpus::export_around` otherwise has no caller — this is
/// the CLI's only exercise of it.
#[test]
fn corpus_export_around_picks_the_newest_artifact_at_or_below_pos() {
    let inst = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(inst.path(), "ar", 4, 4);
    let around = p + 1000;
    let output = Command::new(env!("CARGO_BIN_EXE_uc2-diffreplay"))
        .args(["corpus", "export"])
        .arg("--instance-dir")
        .arg(inst.path())
        .arg("--app-id")
        .arg("ar")
        .arg("--row")
        .arg("0")
        .arg("--around")
        .arg(around.to_string())
        .arg("--out")
        .arg(out.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&format!("origin {p} ")),
        "expected origin {p} in {stdout:?}"
    );
    let c = Corpus::open(out.path()).unwrap();
    assert_eq!(c.manifest.origin, p);
}
