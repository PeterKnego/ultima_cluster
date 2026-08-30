// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//
//! `packaging/quickstart-local.sh`, run for real.
//!
//! The script is the "no toolchain" adoption path: an operator downloads a
//! release tarball, unpacks `uc2-<ver>-<target>/{bin,packaging}/`, runs
//! `packaging/quickstart-local.sh`, and has a three-node cluster with three
//! gateways answering a remote client a few seconds later. Nothing in that
//! story involves cargo, this repository, or a Rust compiler — so the only
//! honest test of it is to *run it*, from a directory that holds nothing but
//! the five binaries.
//!
//! That is what this test does: it builds a bin dir out of symlinks
//! (`uc2-node`, `uc2ctl`, `uc2-gateway`, `counter-service`, `counter-remote`),
//! points the script at it with `--bin-dir`, and asserts exit 0 plus the two
//! strings the script promises — `value=10` (two `add 5`s after a `reset`,
//! read back linearizably through a gateway) and `PASS`.
//!
//! # Why it is feature-gated
//!
//! It starts eleven processes and waits on two elections' worth of wall clock.
//! `cargo test` has to stay fast, so this lives behind `quickstart-tests` and
//! runs in `nightly.yml`'s `quickstart` job — the same posture as
//! `uc_crashtest`'s `hard-crash-tests`.
//!
//! # Binary discovery
//!
//! `CARGO_BIN_EXE_*` only covers *this* package's own `[[bin]]` targets, so
//! `counter-service` and `counter-remote` come from the env vars and the three
//! daemons are built on demand via `cargo build -p <pkg> --bin <bin>
//! --message-format=json`, whose emitted artifact path is parsed out by hand
//! (the technique `escargot` automates; copied from
//! `examples/uc_crashtest/tests/enospc.rs` so this crate needs no new
//! dev-dependency).
#![cfg(feature = "quickstart-tests")]

use std::path::{Path, PathBuf};
use std::process::Command;

/// Build `<pkg>`'s `<bin>` target and return the executable cargo emitted.
fn cargo_bin(pkg: &str, bin: &str) -> PathBuf {
    let out = Command::new(env!("CARGO"))
        .args(["build", "-p", pkg, "--bin", bin, "--message-format=json"])
        .output()
        .unwrap_or_else(|e| panic!("spawn `cargo build -p {pkg} --bin {bin}`: {e}"));
    assert!(
        out.status.success(),
        "cargo build -p {pkg} --bin {bin} failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let needle = format!("\"name\":\"{bin}\"");
    for line in stdout.lines() {
        if line.contains(&needle)
            && let Some(rest) = line.split("\"executable\":\"").nth(1)
            && let Some(end) = rest.find('"')
        {
            return PathBuf::from(&rest[..end]);
        }
    }
    panic!("cargo build --message-format=json for {bin} produced no executable artifact:\n{stdout}");
}

/// A directory holding exactly the five binaries the script needs, under the
/// names it looks for — i.e. the `bin/` of an extracted release tarball.
fn tarball_bin_dir(at: &Path) -> PathBuf {
    let dir = at.join("bin");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bin dir");
    let bins: [(PathBuf, &str); 5] = [
        (cargo_bin("uc_node", "uc2-node"), "uc2-node"),
        (cargo_bin("uc_ctl", "uc2ctl"), "uc2ctl"),
        (cargo_bin("uc_gateway", "uc2-gateway"), "uc2-gateway"),
        (PathBuf::from(env!("CARGO_BIN_EXE_counter-service")), "counter-service"),
        (PathBuf::from(env!("CARGO_BIN_EXE_counter-remote")), "counter-remote"),
    ];
    for (src, name) in bins {
        assert!(src.exists(), "{} does not exist", src.display());
        std::os::unix::fs::symlink(&src, dir.join(name))
            .unwrap_or_else(|e| panic!("symlink {} -> {name}: {e}", src.display()));
    }
    dir
}

/// Everything the script wrote to `$ROOT/logs`, for a failure message that can
/// actually be diagnosed from CI output alone.
fn dump_logs(root: &Path) -> String {
    let mut s = String::new();
    let Ok(entries) = std::fs::read_dir(root.join("logs")) else {
        return format!("(no logs directory under {})", root.display());
    };
    let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
    paths.sort();
    for p in paths {
        let body = std::fs::read_to_string(&p).unwrap_or_default();
        // Tail: a wedged node can write a lot, and the last lines are the
        // ones that say why.
        let tail: Vec<&str> = body.lines().rev().take(40).collect();
        s.push_str(&format!("\n----- {} -----\n", p.display()));
        for line in tail.into_iter().rev() {
            s.push_str(line);
            s.push('\n');
        }
    }
    s
}

#[test]
fn quickstart_script_brings_up_a_cluster_and_a_remote_client_reads_ten() {
    let scratch = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("qs-{}", std::process::id()));
    std::fs::create_dir_all(&scratch).expect("create scratch");
    let bin_dir = tarball_bin_dir(&scratch);
    let root = scratch.join("run");

    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../packaging/quickstart-local.sh")
        .canonicalize()
        .expect("packaging/quickstart-local.sh must exist");

    let out = Command::new(&script)
        .arg("--bin-dir")
        .arg(&bin_dir)
        .arg("--root")
        .arg(&root)
        .output()
        .unwrap_or_else(|e| panic!("spawn {}: {e}", script.display()));

    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    println!("--- quickstart-local.sh stdout ---\n{stdout}");
    println!("--- quickstart-local.sh stderr ---\n{stderr}");

    let ok = out.status.success()
        && stdout.contains("value=10")
        && stdout.lines().any(|l| l.trim() == "PASS");
    assert!(
        ok,
        "quickstart-local.sh did not pass (exit {:?})\n--- stdout ---\n{stdout}\n--- stderr ---\n\
         {stderr}\n--- logs ---{}",
        out.status.code(),
        dump_logs(&root)
    );

    // Only on success: a failed run's instance dirs and logs are the evidence.
    let _ = std::fs::remove_dir_all(&scratch);
}
