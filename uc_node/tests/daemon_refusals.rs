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

/// Write a single-voter `node.toml` with `{extra}` appended after
/// `[[members]]` (so a `[table]` header in it captures nothing it shouldn't)
/// — the caller supplies whatever `[crypto]`/`[admin]` text (or none at all)
/// the test needs.
fn write_config(dir: &Path, port: u16, extra: &str) -> (PathBuf, PathBuf) {
    write_config_rooted(dir, port, "", extra)
}

/// [`write_config`] with a second block spliced in at the DOCUMENT ROOT,
/// above `[[members]]`. A bare key placed in `extra` would be read as a field
/// of the `[[members]]` table and refused as an unknown field (the loader is
/// `deny_unknown_fields`), which is not the refusal a top-level-key test means
/// to observe.
fn write_config_rooted(dir: &Path, port: u16, root: &str, extra: &str) -> (PathBuf, PathBuf) {
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
{root}

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

/// The `[crypto]`/`[admin]`/`[services]` block every config below needs to get
/// PAST the M12b explicit-choice refusals and reach the cluster-FSM ones.
const VALID_TAIL: &str =
    "[crypto]\nenabled = false\n\n[admin]\nauth = \"none\"\n\n[services]\nnames = [\"kv\"]\n";

fn run(cfg: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_uc2-node"))
        .arg("--config")
        .arg(cfg)
        .output()
        .unwrap()
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
    assert_eq!(
        out.status.code(),
        Some(2),
        "a config refusal must exit 2, got {:?}",
        out.status
    );
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
    assert_eq!(
        out.status.code(),
        Some(2),
        "a config refusal must exit 2, got {:?}",
        out.status
    );
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
         key_path = \"{}\" }}]\n\n[services]\nnames = [\"sm\"]\n",
        key_path.display()
    );
    let (cfg, _inst) = write_config(dir.path(), 19803, &extra);

    let out = run(&cfg);
    assert!(!out.status.success(), "must refuse to start");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("admin key"),
        "refusal must name it as an admin-key problem, got: {err}"
    );
    assert!(
        err.contains("ops-alice"),
        "refusal must name the key, got: {err}"
    );
    assert_eq!(
        out.status.code(),
        Some(2),
        "a bad admin key file must exit 2, got {:?}",
        out.status
    );
}

// ---- the cluster FSM (spec §3.3, §6): two keys moved cluster-wide --------

/// `admission_bytes` at the document root and `fsm_lag` under `[services]`
/// were per-host `node.toml` keys until the cluster FSM made both REPLICATED
/// settings. They are not silently ignored and not silently honoured: the
/// DAEMON refuses by name, exit 2, and the message points at where the value
/// lives now (`[settings]` to seed genesis, `uc2ctl settings apply` to change
/// a running cluster).
///
/// The daemon, not just the loader: `config_file`'s own unit tests already
/// cover `load_str`, but an operator meets this through the binary, and exit 2
/// is a contract with the shipped systemd unit
/// (`RestartPreventExitStatus`) — a refusal that exited 1 would be a restart
/// loop instead of a loud stop.
#[test]
fn daemon_refuses_a_top_level_admission_bytes_pointing_at_settings_apply() {
    let dir = scratch();
    let (cfg, _inst) = write_config_rooted(dir.path(), 19804, "admission_bytes = 4096", VALID_TAIL);

    let out = run(&cfg);
    assert!(!out.status.success(), "must refuse to start");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("admission_bytes"),
        "refusal must name the field, got: {err}"
    );
    assert!(
        err.contains("uc2ctl settings apply"),
        "refusal must point at the replacement, got: {err}"
    );
    assert_eq!(
        out.status.code(),
        Some(2),
        "a config refusal must exit 2, got {:?}",
        out.status
    );
}

/// The twin, under `[services]`.
#[test]
fn daemon_refuses_a_services_fsm_lag_pointing_at_settings_apply() {
    let dir = scratch();
    let extra = "[crypto]\nenabled = false\n\n[admin]\nauth = \"none\"\n\n[services]\n\
                 names = [\"kv\"]\nfsm_lag = \"1MiB\"\n";
    let (cfg, _inst) = write_config(dir.path(), 19805, extra);

    let out = run(&cfg);
    assert!(!out.status.success(), "must refuse to start");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("services.fsm_lag"),
        "refusal must name the field, got: {err}"
    );
    assert!(
        err.contains("uc2ctl settings apply"),
        "refusal must point at the replacement, got: {err}"
    );
    assert_eq!(
        out.status.code(),
        Some(2),
        "a config refusal must exit 2, got {:?}",
        out.status
    );
}

/// The other half of the same rule, and the half that makes the two refusals
/// above actionable rather than a dead end: the SAME two keys under
/// `[settings]` are accepted, and the daemon starts.
///
/// "Starts" is observed as a bounded poll, never a bare sleep: the daemon
/// creates its cnc page only after the config has loaded and preflight has
/// passed, so `cnc2.dat` appearing under the instance dir while the child is
/// still alive IS the start. Then SIGTERM and assert a clean exit, so a node
/// that started and immediately died cannot pass.
#[test]
fn daemon_starts_with_the_same_two_keys_under_settings() {
    use std::time::{Duration, Instant};

    let dir = scratch();
    let extra = "[crypto]\nenabled = false\n\n[admin]\nauth = \"none\"\n\n[services]\n\
                 names = [\"kv\"]\n\n[settings]\nadmission_bytes = 4096\nfsm_lag = \"1MiB\"\n";
    let (cfg, inst) = write_config(dir.path(), 19806, extra);

    let child = Command::new(env!("CARGO_BIN_EXE_uc2-node"))
        .arg("--config")
        .arg(&cfg)
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    // Every `panic!`/`assert!` in the poll below unwinds out of this test —
    // and a `Child` that is merely dropped leaves the daemon RUNNING, holding
    // its instance dir, its port and four busy-spin threads for the rest of
    // the suite (the tempdir unlinks under a live node, so it is not even
    // visible as a leak). The guard kills it on the way out; the happy path
    // disarms it and takes the child back for the SIGTERM assertions below.
    struct KillOnDrop(Option<std::process::Child>);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            if let Some(mut c) = self.0.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }
    let mut guard = KillOnDrop(Some(child));

    let deadline = Instant::now() + Duration::from_secs(30);
    let page = inst.join("cnc2.dat");
    loop {
        if page.exists() {
            break;
        }
        match guard.0.as_mut().expect("armed").try_wait().unwrap() {
            Some(status) => panic!(
                "the daemon exited before creating its cnc page: {status:?} — \
                 `[settings]` must be an ACCEPTED home for these keys"
            ),
            None => assert!(
                Instant::now() < deadline,
                "timeout waiting for the daemon to create its cnc page"
            ),
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    // Disarmed: from here the child is reaped by `wait_with_output` below.
    let child = guard.0.take().expect("armed");
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let out = child.wait_with_output().unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "the node started, so SIGTERM must exit 0, got {:?}; stderr: {err}",
        out.status
    );
    // The page could in principle be a leftover; the daemon's own records
    // cannot be. It announced its listener and it drained on the signal.
    assert!(
        err.contains("\"event\":\"node_listening\""),
        "the daemon must have reached its listening record, got: {err}"
    );
    assert!(
        err.contains("\"event\":\"stopped\""),
        "…and stopped cleanly, got: {err}"
    );
}

/// Cluster-FSM spec §4.1: `uc_` is reserved for the internal state machines,
/// so a `node.toml` that claims `uc_cluster` as a user row is a named startup
/// refusal from the daemon. The message itself is `ServicesConfig`'s
/// (`services.rs`'s `a_uc_prefixed_fsm_name_is_reserved_and_refused_by_name`
/// pins its wording at that door); this pins that an operator meets it
/// through the binary, with the same exit 2 as every other config refusal.
#[test]
fn daemon_refuses_a_uc_prefixed_service_name() {
    let dir = scratch();
    let extra = "[crypto]\nenabled = false\n\n[admin]\nauth = \"none\"\n\n[services]\nnames = [\"uc_cluster\"]\n";
    let (cfg, _inst) = write_config(dir.path(), 19807, extra);

    let out = run(&cfg);
    assert!(!out.status.success(), "must refuse to start");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("services.names"),
        "refusal must name the field, got: {err}"
    );
    assert!(err.contains("reserved"), "refusal must say why, got: {err}");
    assert_eq!(
        out.status.code(),
        Some(2),
        "a config refusal must exit 2, got {:?}",
        out.status
    );
}
