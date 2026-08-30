// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M12b (spec §3.3, §5.1): `[crypto]`/`[admin]` are explicit choices — the
//! real `uc2-node` daemon binary must refuse to start (exit 2, a named
//! stderr message) when either is absent, and must refuse a bad admin key
//! file the same way `[crypto].key_path` already does.
//!
//! Construction mirrors `lifecycle.rs`'s daemon tests: a scratch dir under
//! `CARGO_TARGET_TMPDIR` (ext4, never the RAM-backed `/tmp` — see CLAUDE.md
//! "Local box"), the real `uc2-node` binary via `CARGO_BIN_EXE_uc2-node`.

use std::path::{Path, PathBuf};
use std::process::Command;

fn scratch() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-daemon-refusals-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

/// Write a single-voter `node.toml` with `{extra}` prepended (document root,
/// same convention as `lifecycle.rs::daemon_config`) — the caller supplies
/// whatever `[crypto]`/`[admin]` text (or none at all) the test needs.
fn write_config(dir: &Path, port: u16, extra: &str) -> (PathBuf, PathBuf) {
    let inst = dir.join("n1");
    std::fs::create_dir_all(&inst).unwrap();
    let cfg = dir.join("node.toml");
    std::fs::write(
        &cfg,
        format!(
            r#"id = 1
bind = "127.0.0.1:{port}"
instance_dir = "{}"
app_id = "daemon-refusals"

[[members]]
id = 1
addr = "127.0.0.1:{port}"

{extra}
"#,
            inst.display()
        ),
    )
    .unwrap();
    (cfg, inst)
}

fn run(cfg: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_uc2-node")).arg("--config").arg(cfg).output().unwrap()
}

/// A `node.toml` with `[crypto]` but no `[admin]` at all must be refused
/// naming the missing section, exit 2 — same family as every other config
/// refusal this binary makes (`daemon_refuses_a_config_with_a_bind_mismatch`
/// in `lifecycle.rs`).
#[test]
fn daemon_refuses_a_config_missing_the_admin_section() {
    let dir = scratch();
    let (cfg, _inst) = write_config(dir.path(), 19801, "[crypto]\nenabled = false\n");

    let out = run(&cfg);
    assert!(!out.status.success(), "must refuse to start");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("[admin] section is required"),
        "refusal must name the missing section, got: {err}"
    );
    assert_eq!(out.status.code(), Some(2), "a config refusal must exit 2, got {:?}", out.status);
}

/// Symmetric with the above: no `[crypto]` at all.
#[test]
fn daemon_refuses_a_config_missing_the_crypto_section() {
    let dir = scratch();
    let (cfg, _inst) = write_config(dir.path(), 19802, "[admin]\nauth = \"none\"\n");

    let out = run(&cfg);
    assert!(!out.status.success(), "must refuse to start");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("[crypto] section is required"),
        "refusal must name the missing section, got: {err}"
    );
    assert_eq!(out.status.code(), Some(2), "a config refusal must exit 2, got {:?}", out.status);
}

/// `auth = "hmac"` naming a group/world-readable (0644) key file must be
/// refused by the DAEMON (not just the config loader — the key is loaded
/// only after preflight passes), exit 2, stderr naming both the key and the
/// permissions problem (`AdminKey::load` -> `CryptoError::KeyFilePermissions`,
/// wrapped in `uc2-node: admin key <name> at <path>: ...`).
#[test]
fn daemon_refuses_an_hmac_admin_key_file_that_is_world_readable() {
    let dir = scratch();
    let key_path = dir.path().join("alice.key");
    std::fs::write(&key_path, [0x11u8; 32]).unwrap();
    // 0644: readable by group/world — the same rule `[crypto].key_path`
    // already enforces (`uc_crypto::admin::check_key_file_perms`).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    }

    let extra = format!(
        "[crypto]\nenabled = false\n\n[admin]\nauth = \"hmac\"\nkeys = [{{ name = \"ops-alice\", \
         key_path = \"{}\" }}]\n",
        key_path.display()
    );
    let (cfg, _inst) = write_config(dir.path(), 19803, &extra);

    let out = run(&cfg);
    assert!(!out.status.success(), "must refuse to start");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("admin key"), "refusal must name it as an admin-key problem, got: {err}");
    assert!(err.contains("ops-alice"), "refusal must name the key, got: {err}");
    assert_eq!(out.status.code(), Some(2), "a bad admin key file must exit 2, got {:?}", out.status);
}
