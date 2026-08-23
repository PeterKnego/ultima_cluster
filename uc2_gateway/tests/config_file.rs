// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `gateway.toml` deserialisation (Task 10): a one-to-one mirror of
//! [`EdgeConfig`], `deny_unknown_fields` per section so a typo is a named
//! startup refusal, exactly the `uc2-node` posture (see
//! `uc2_node/src/config_file.rs`).

use std::time::Duration;

use uc2_gateway::config_file::{ConfigFileError, load_from_path, parse_str};
use uc2_gateway::{ConfigError, Member};

/// A temp dir on real disk — `CARGO_TARGET_TMPDIR` lives under `target/`
/// (ext4), never the RAM-backed `/tmp` (see CLAUDE.md "Local box").
fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new().prefix("uc2-gw-cfg-").tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("tempdir")
}

fn write(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
    let p = dir.join("gateway.toml");
    std::fs::write(&p, body).unwrap();
    p
}

const MINIMAL: &str = r#"
[local]
instance_dir = "/srv/uc2/n0"
app_id = "myapp"
listen = "0.0.0.0:9200"

[[members]]
node_id = 0
gateway = "10.0.0.10:9200"
"#;

#[test]
fn a_minimal_toml_parses_with_defaults_filled() {
    let dir = tempdir();
    let p = write(dir.path(), MINIMAL);
    let cfg = load_from_path(&p).expect("minimal config must parse and validate");

    assert_eq!(cfg.instance_dir.to_str().unwrap(), "/srv/uc2/n0");
    assert_eq!(cfg.app_id, "myapp");
    assert_eq!(cfg.listen, "0.0.0.0:9200".parse().unwrap());
    assert_eq!(cfg.members, vec![Member { node_id: 0, gateway: "10.0.0.10:9200".into() }]);

    // Defaults must match EdgeConfig::defaults() exactly — an operator who
    // states only the required fields gets the documented behaviour.
    assert!(cfg.session_envelope);
    assert_eq!(cfg.max_inflight, 4096);
    assert_eq!(cfg.per_conn_inflight, 256);
    assert_eq!(cfg.status_interval, Duration::from_millis(200));
    assert_eq!(cfg.request_timeout, Duration::from_secs(10));
    assert_eq!(cfg.max_connections, 1024);
}

/// The shipped example must parse and validate — a packaged example that
/// drifts out of agreement with the loader is exactly the failure this task
/// exists to prevent.
#[test]
fn the_packaged_example_config_is_valid() {
    let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../packaging/gateway.example.toml");
    load_from_path(&p).expect("packaging/gateway.example.toml must parse and validate");
}

#[test]
fn full_toml_overrides_every_default() {
    let dir = tempdir();
    let p = write(
        dir.path(),
        r#"
[local]
instance_dir = "/srv/uc2/n0"
app_id = "myapp"
listen = "0.0.0.0:9200"

[[members]]
node_id = 0
gateway = "10.0.0.10:9200"
[[members]]
node_id = 1
gateway = "10.0.0.11:9200"

[limits]
max_inflight = 1024
per_conn_inflight = 64
request_timeout_ms = 5000
status_interval_ms = 100
max_connections = 32

[session]
envelope = false
"#,
    );
    let cfg = load_from_path(&p).expect("full config must parse and validate");
    assert_eq!(cfg.members.len(), 2);
    assert!(!cfg.session_envelope);
    assert_eq!(cfg.max_inflight, 1024);
    assert_eq!(cfg.per_conn_inflight, 64);
    assert_eq!(cfg.status_interval, Duration::from_millis(100));
    assert_eq!(cfg.request_timeout, Duration::from_millis(5000));
    assert_eq!(cfg.max_connections, 32);
}

#[test]
fn an_unknown_key_is_refused_by_name() {
    let dir = tempdir();
    let p = write(
        dir.path(),
        r#"
[local]
instance_dir = "/srv/uc2/n0"
app_id = "myapp"
listen = "0.0.0.0:9200"
lisetn_typo = "oops"

[[members]]
node_id = 0
gateway = "10.0.0.10:9200"
"#,
    );
    let err = load_from_path(&p).unwrap_err();
    assert!(err.to_string().contains("lisetn_typo"), "error must name the typo, got: {err}");
}

#[test]
fn an_unknown_top_level_section_is_refused_by_name() {
    let dir = tempdir();
    let p = write(dir.path(), &format!("{MINIMAL}\n[telemetry]\nlevel = \"info\"\n"));
    let err = load_from_path(&p).unwrap_err();
    assert!(err.to_string().contains("telemetry"), "error must name the section, got: {err}");
}

#[test]
fn an_unknown_key_inside_limits_is_refused_by_name() {
    let dir = tempdir();
    let p = write(dir.path(), &format!("{MINIMAL}\n[limits]\nmax_inflightt = 10\n"));
    let err = load_from_path(&p).unwrap_err();
    assert!(err.to_string().contains("max_inflightt"), "error must name the typo, got: {err}");
}

#[test]
fn duplicate_node_id_is_refused_by_name() {
    let dir = tempdir();
    let p = write(
        dir.path(),
        r#"
[local]
instance_dir = "/srv/uc2/n0"
app_id = "myapp"
listen = "0.0.0.0:9200"

[[members]]
node_id = 0
gateway = "10.0.0.10:9200"
[[members]]
node_id = 0
gateway = "10.0.0.11:9200"
"#,
    );
    let err = load_from_path(&p).unwrap_err();
    match err {
        ConfigFileError::Invalid(ConfigError::DuplicateMember(0)) => {}
        other => panic!("expected DuplicateMember(0), got: {other}"),
    }
}

#[test]
fn per_conn_inflight_over_max_inflight_is_refused_by_name() {
    let dir = tempdir();
    let p = write(dir.path(), &format!("{MINIMAL}\n[limits]\nmax_inflight = 8\nper_conn_inflight = 9\n"));
    let err = load_from_path(&p).unwrap_err();
    match err {
        ConfigFileError::Invalid(ConfigError::PerConnExceedsMax { per_conn: 9, max: 8 }) => {}
        other => panic!("expected PerConnExceedsMax{{9,8}}, got: {other}"),
    }
}

#[test]
fn an_unparsable_listen_address_is_refused() {
    let dir = tempdir();
    let p = write(
        dir.path(),
        r#"
[local]
instance_dir = "/srv/uc2/n0"
app_id = "myapp"
listen = "not-an-address"

[[members]]
node_id = 0
gateway = "10.0.0.10:9200"
"#,
    );
    let err = load_from_path(&p).unwrap_err();
    assert!(matches!(err, ConfigFileError::Parse { .. }), "expected a Parse error, got: {err}");
}

#[test]
fn a_nonexistent_file_is_a_named_read_refusal() {
    let err = load_from_path(std::path::Path::new("/definitely/does/not/exist.toml")).unwrap_err();
    assert!(matches!(err, ConfigFileError::Read { .. }), "expected a Read error, got: {err}");
}

/// M12d: `parse_str` is `load_from_path` without the file — the shipped
/// example must parse through it, and an unknown key must be refused exactly
/// as the file loader refuses it. This is the seam the `uc2_gateway_toml`
/// fuzz target drives; if it stopped being the same code the loader runs, the
/// target would be fuzzing a fiction.
#[test]
fn parse_str_is_the_loader_without_the_file() {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../packaging/gateway.example.toml"
    ))
    .expect("read packaging/gateway.example.toml");

    let cfg = parse_str(&text).expect("the shipped example parses");
    assert!(!cfg.members.is_empty(), "example must define members");

    // The SAME answer the file loader gives.
    let dir = tempdir();
    let p = write(dir.path(), &text);
    let from_file = load_from_path(&p).expect("example parses from file");
    assert_eq!(cfg.app_id, from_file.app_id);
    assert_eq!(cfg.listen, from_file.listen);
    assert_eq!(cfg.members, from_file.members);
    assert_eq!(cfg.max_inflight, from_file.max_inflight);

    // An unknown key is refused, by the same variant either way.
    let bad = "[local]\ninstance_dir = \"/srv/uc2/n0\"\napp_id = \"a\"\n\
               listen = \"127.0.0.1:9500\"\nunknown_key = 1\n";
    assert!(
        matches!(parse_str(bad), Err(ConfigFileError::Parse { .. })),
        "unknown key must be a Parse refusal"
    );
    let p = write(dir.path(), bad);
    assert!(matches!(load_from_path(&p), Err(ConfigFileError::Parse { .. })));

    // The file loader still names the real path.
    let Err(ConfigFileError::Parse { path, .. }) = load_from_path(&p) else {
        panic!("expected Parse")
    };
    assert_eq!(path, p, "load_from_path must still name the file it read");

    // A semantic refusal (EdgeConfig::validate) comes through unchanged too.
    let empty_app = "[local]\ninstance_dir = \"/srv/uc2/n0\"\napp_id = \"\"\n\
                     listen = \"127.0.0.1:9500\"\n";
    assert!(matches!(parse_str(empty_app), Err(ConfigFileError::Invalid(_))));
}
