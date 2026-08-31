// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2-gateway` binary smoke tests: the named-refusal exit codes, without
//! needing a running cluster.

use std::process::Command;

fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-gw-smoke-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

/// A missing config file is exit 2 (a named config refusal), with the
/// binary's name prefixed on stderr.
#[test]
fn a_missing_config_file_exits_2() {
    let out = Command::new(env!("CARGO_BIN_EXE_uc2-gateway"))
        .args(["--config", "/definitely/does/not/exist.toml"])
        .output()
        .expect("run uc2-gateway");

    assert_eq!(
        out.status.code(),
        Some(2),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("uc2-gateway:"),
        "stderr must be prefixed, got: {stderr}"
    );
}

/// A config that parses and validates but points at a node instance
/// directory that does not exist fails to attach: exit 1 (a runtime start
/// failure), not 2 — the config itself was fine.
#[test]
fn a_valid_config_with_no_node_running_exits_1() {
    let dir = tempdir();
    let cfg_path = dir.path().join("gateway.toml");
    let instance_dir = dir.path().join("no-such-node");
    std::fs::write(
        &cfg_path,
        format!(
            r#"
[local]
instance_dir = "{}"
app_id = "myapp"
listen = "127.0.0.1:0"

[[members]]
node_id = 0
gateway = "10.0.0.10:9200"
"#,
            instance_dir.display()
        ),
    )
    .expect("write config");

    let out = Command::new(env!("CARGO_BIN_EXE_uc2-gateway"))
        .args(["--config", cfg_path.to_str().unwrap()])
        .output()
        .expect("run uc2-gateway");

    assert_eq!(
        out.status.code(),
        Some(1),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("uc2-gateway:"),
        "stderr must be prefixed, got: {stderr}"
    );
}
