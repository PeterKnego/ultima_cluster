// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! TOML configuration for the `uc2-node` daemon.
//!
//! A one-to-one mirror of [`NodeConfig`], deserialised with
//! `deny_unknown_fields` so a typo is a startup refusal rather than a
//! silently-ignored setting — the same posture as M8's crypto boot refusal.
//! This module does deserialisation ONLY; every semantic rule lives in
//! [`crate::preflight`].
//!
//! `[log]` and `[metrics]` are M10's observability sections. Both are
//! optional and, like `[purge]`/`[crypto]`, ABSENT means the feature is off:
//! no `[log]` means the default level (`info`); no `[metrics]` means no
//! endpoint. `deny_unknown_fields` applies to their contents too — a typo
//! inside either section is a startup refusal naming the key, not a
//! silently-ignored setting.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use uc2_consensus::election::NodeId;
use uc2_crypto::rotation::RotationPolicy;
use uc2_net::fault::FaultConfig;

use crate::obs::log::LogLevel;
use crate::preflight::{ObsOptions, StartupOptions};
use crate::{CryptoConfig, DEFAULT_JOURNAL_SEGMENT_BYTES, NodeConfig, PurgePolicy};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config file {path}: {source}")]
    Read { path: PathBuf, source: std::io::Error },
    #[error("invalid config file {path}: {source}")]
    Parse { path: PathBuf, source: toml::de::Error },
    /// A field passed `deny_unknown_fields`/type-checking but failed its own
    /// semantic parse (today: only `log.level`). `detail` already names the
    /// field and echoes the bad value — see `LogLevel::from_str` — so this
    /// variant must not repeat `field` into the message and double it up.
    #[error("{detail}")]
    Invalid { field: &'static str, detail: String },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    id: NodeId,
    addr: SocketAddr,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PurgeSection {
    below_snapshot_slack_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CryptoSection {
    key_path: PathBuf,
    allowlist_path: PathBuf,
    #[serde(default)]
    rotation_interval_ns: Option<u64>,
    #[serde(default)]
    rotation_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogSectionFile {
    #[serde(default)]
    level: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricsSectionFile {
    #[serde(default)]
    bind: Option<SocketAddr>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeConfigFile {
    id: NodeId,
    bind: SocketAddr,
    instance_dir: PathBuf,
    app_id: String,
    members: Vec<Member>,
    #[serde(default)]
    learners: Vec<Member>,
    #[serde(default = "default_buffer_bytes")]
    buffer_bytes: usize,
    #[serde(default = "default_max_payload")]
    max_payload: usize,
    #[serde(default = "default_admission_bytes")]
    admission_bytes: u64,
    #[serde(default = "default_election_min_ns")]
    election_timeout_min_ns: u64,
    #[serde(default = "default_election_max_ns")]
    election_timeout_max_ns: u64,
    #[serde(default)]
    seed: Option<u64>,
    #[serde(default = "default_journal_segment_bytes")]
    journal_segment_bytes: u64,
    #[serde(default)]
    purge: Option<PurgeSection>,
    #[serde(default)]
    crypto: Option<CryptoSection>,
    /// TEST/DEV ONLY — see [`crate::preflight::StartupOptions`]. Never
    /// silences the startup warning.
    #[serde(default)]
    allow_volatile_fs: bool,
    /// Structured logging. Absent means the default level (`info`).
    #[serde(default)]
    log: Option<LogSectionFile>,
    /// The `/metrics`, `/healthz`, `/readyz` endpoint. Absent means no
    /// endpoint — the same absent-means-disabled convention as
    /// `[purge]`/`[crypto]`.
    #[serde(default)]
    metrics: Option<MetricsSectionFile>,
}

fn default_buffer_bytes() -> usize {
    1 << 26 // 64 MiB — a production default, not the examples' 4 MiB
}
fn default_max_payload() -> usize {
    // 512 B, matching the examples' NODE_MAX_PAYLOAD. A max-size frame plus its
    // headers (and any crypto tag) must fit ONE datagram — `uc2_net`'s Sender
    // asserts it at construction. The previous 1 MiB default was ~700x over the
    // 1408 B MTU budget and panicked the daemon on first boot.
    512
}
fn default_admission_bytes() -> u64 {
    256 * 1024
}
fn default_election_min_ns() -> u64 {
    150_000_000
}
fn default_election_max_ns() -> u64 {
    300_000_000
}
fn default_journal_segment_bytes() -> u64 {
    DEFAULT_JOURNAL_SEGMENT_BYTES
}

/// Per-node election-timeout seed.
///
/// Lifted verbatim from `examples/counter/src/bin/counter-node.rs`: without a
/// distinct stream per node every member runs the identical randomised
/// sequence, times out at the same instant, splits the vote, and the cluster
/// livelocks instead of electing anyone. The daemon must not require the
/// operator to know this, so `seed` is optional and defaults to this.
pub fn default_seed_for(id: NodeId) -> u64 {
    1 ^ (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Read and deserialise a node config file. Performs NO validation beyond what
/// the type system enforces — call [`crate::preflight::check`] next, passing it
/// the returned [`StartupOptions`].
pub fn load_from_path(path: &Path) -> Result<(NodeConfig, StartupOptions), ConfigError> {
    let text = std::fs::read_to_string(path)
        .map_err(|source| ConfigError::Read { path: path.to_path_buf(), source })?;
    let f: NodeConfigFile = toml::from_str(&text)
        .map_err(|source| ConfigError::Parse { path: path.to_path_buf(), source })?;

    let purge = match f.purge {
        Some(p) => PurgePolicy::BelowSnapshot { slack_bytes: p.below_snapshot_slack_bytes },
        None => PurgePolicy::Disabled,
    };
    let crypto = match f.crypto {
        Some(c) => {
            let d = RotationPolicy::default();
            CryptoConfig::Enabled {
                key_path: c.key_path,
                allowlist_path: c.allowlist_path,
                rotation: RotationPolicy {
                    interval_ns: c.rotation_interval_ns.unwrap_or(d.interval_ns),
                    bytes: c.rotation_bytes.unwrap_or(d.bytes),
                },
            }
        }
        None => CryptoConfig::Disabled,
    };
    let log_level = match f.log.and_then(|l| l.level) {
        None => LogLevel::default(),
        Some(s) => s
            .parse::<LogLevel>()
            .map_err(|e| ConfigError::Invalid { field: "log.level", detail: e })?,
    };
    let metrics_bind = f.metrics.map(|m| m.bind.unwrap_or_else(|| "127.0.0.1:9600".parse().unwrap()));

    Ok((NodeConfig {
        id: f.id,
        members: f.members.into_iter().map(|m| (m.id, m.addr)).collect(),
        learners: f.learners.into_iter().map(|m| (m.id, m.addr)).collect(),
        bind: f.bind,
        instance_dir: f.instance_dir,
        app_id: f.app_id,
        buffer_bytes: f.buffer_bytes,
        max_payload: f.max_payload,
        admission_bytes: f.admission_bytes,
        election_timeout_min_ns: f.election_timeout_min_ns,
        election_timeout_max_ns: f.election_timeout_max_ns,
        seed: f.seed.unwrap_or_else(|| default_seed_for(f.id)),
        faults: FaultConfig::default(),
        purge,
        journal_segment_bytes: f.journal_segment_bytes,
        crypto,
    },
    StartupOptions {
        allow_volatile_fs: f.allow_volatile_fs,
        obs: ObsOptions { log_level, metrics_bind },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
        let p = dir.join("node.toml");
        std::fs::write(&p, body).unwrap();
        p
    }

    /// A single-voter config with none of the optional sections — the
    /// minimal document every optional-section test appends to.
    const MINIMAL: &str = r#"
id = 1
bind = "10.0.0.1:9100"
instance_dir = "/srv/uc2/n1"
app_id = "myapp"

[[members]]
id = 1
addr = "10.0.0.1:9100"
"#;

    fn load_str(body: &str) -> Result<(NodeConfig, StartupOptions), ConfigError> {
        let dir = tempfile::tempdir().unwrap();
        let p = write(dir.path(), body);
        load_from_path(&p)
    }

    #[test]
    fn minimal_config_maps_to_node_config_with_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            r#"
id = 1
bind = "10.0.0.1:9100"
instance_dir = "/srv/uc2/n1"
app_id = "myapp"

[[members]]
id = 1
addr = "10.0.0.1:9100"

[[members]]
id = 2
addr = "10.0.0.2:9100"
"#,
        );
        let (cfg, opts) = load_from_path(&p).unwrap();
        assert!(!opts.allow_volatile_fs);
        assert_eq!(cfg.id, 1);
        assert_eq!(cfg.members.len(), 2);
        assert_eq!(cfg.app_id, "myapp");
        // Defaults that must NOT require the operator to state them.
        assert_eq!(cfg.buffer_bytes, 1 << 26);
        assert_eq!(cfg.journal_segment_bytes, crate::DEFAULT_JOURNAL_SEGMENT_BYTES);
        assert!(matches!(cfg.purge, PurgePolicy::Disabled));
        assert!(matches!(cfg.crypto, CryptoConfig::Disabled));
        assert!(cfg.learners.is_empty());
    }

    /// The defaults must produce a config the node can actually START on.
    /// This is what a shipped 1 MiB max_payload default failed: every field
    /// deserialised perfectly and the daemon then panicked inside the sender.
    #[test]
    fn the_defaults_pass_preflight() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            r#"
id = 1
bind = "10.0.0.1:9100"
instance_dir = "/srv/uc2/n1"
app_id = "myapp"

[[members]]
id = 1
addr = "10.0.0.1:9100"
"#,
        );
        let (cfg, _opts) = load_from_path(&p).unwrap();
        crate::preflight::check_semantics(&cfg).expect("stated defaults must be startable");
    }

    #[test]
    fn unknown_field_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            r#"
id = 1
bind = "10.0.0.1:9100"
instance_dir = "/srv/uc2/n1"
app_id = "myapp"
buffer_bytez = 4096

[[members]]
id = 1
addr = "10.0.0.1:9100"
"#,
        );
        let err = load_from_path(&p).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("buffer_bytez"), "error must name the typo, got: {msg}");
    }

    #[test]
    fn seed_defaults_per_node_and_differs_between_ids() {
        // Identical seeds livelock a cluster through vote splits — see
        // counter-node.rs's seed_for comment. The daemon must not make the
        // operator know this.
        let a = default_seed_for(1);
        let b = default_seed_for(2);
        assert_ne!(a, b);
    }

    #[test]
    fn purge_and_crypto_sections_map_to_their_enums() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            r#"
id = 1
bind = "10.0.0.1:9100"
instance_dir = "/srv/uc2/n1"
app_id = "myapp"

[[members]]
id = 1
addr = "10.0.0.1:9100"

[purge]
below_snapshot_slack_bytes = 1048576

[crypto]
key_path = "/etc/uc2/node.key"
allowlist_path = "/etc/uc2/allowlist.toml"
"#,
        );
        let (cfg, _opts) = load_from_path(&p).unwrap();
        assert!(matches!(cfg.purge, PurgePolicy::BelowSnapshot { slack_bytes: 1048576 }));
        match cfg.crypto {
            CryptoConfig::Enabled { ref key_path, ref allowlist_path, rotation } => {
                assert_eq!(key_path.to_str().unwrap(), "/etc/uc2/node.key");
                assert_eq!(allowlist_path.to_str().unwrap(), "/etc/uc2/allowlist.toml");
                // Unstated rotation takes the crate default (1 h / 1 TiB).
                assert_eq!(rotation.interval_ns, 3_600_000_000_000);
            }
            _ => panic!("crypto section must produce CryptoConfig::Enabled"),
        }
    }

    #[test]
    fn allow_volatile_fs_defaults_to_false_and_is_settable() {
        let dir = tempfile::tempdir().unwrap();
        let body = r#"
id = 1
bind = "10.0.0.1:9100"
instance_dir = "/srv/uc2/n1"
app_id = "myapp"

[[members]]
id = 1
addr = "10.0.0.1:9100"
"#;
        let p = write(dir.path(), body);
        let (_cfg, opts) = load_from_path(&p).unwrap();
        assert!(!opts.allow_volatile_fs, "production default must be refuse");

        // PREPENDED, not appended: `body` ends with a [[members]] table, so a
        // key added after it belongs to that table, not to the document root.
        let p2 = write(dir.path(), &format!("allow_volatile_fs = true\n{body}"));
        let (_cfg, opts2) = load_from_path(&p2).unwrap();
        assert!(opts2.allow_volatile_fs);
    }

    /// The SHIPPED example config must parse and pass semantic preflight.
    /// A packaged example that drifts out of agreement with the loader is the
    /// exact failure M9 exists to prevent: it looks authoritative and fails at
    /// the operator's first boot, not ours.
    #[test]
    fn the_packaged_example_config_is_valid() {
        let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../packaging/node.example.toml");
        let (cfg, opts) = load_from_path(&p).expect("packaging/node.example.toml must parse");
        assert!(!opts.allow_volatile_fs, "the shipped example must not override durability");
        crate::preflight::check_semantics(&cfg).expect("the shipped example must be startable");
    }

    /// `[log]`/`[metrics]` parse into typed [`ObsOptions`] — see the
    /// `*_sections_parse_into_obs_options` / `absent_sections_mean_off_and_info`
    /// / `a_bare_metrics_section_gets_the_default_bind` tests below.
    ///
    /// Naming exactly two top-level sections must not open the door
    /// generally — `deny_unknown_fields` is still the posture for everything
    /// else at the document root.
    #[test]
    fn an_unreserved_unknown_section_is_still_refused() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            r#"
id = 1
bind = "10.0.0.1:9100"
instance_dir = "/srv/uc2/n1"
app_id = "myapp"

[[members]]
id = 1
addr = "10.0.0.1:9100"

[telemetry]
level = "info"
"#,
        );
        let err = load_from_path(&p).unwrap_err();
        assert!(
            err.to_string().contains("telemetry"),
            "an unreserved section must still be refused by name, got: {err}"
        );
    }

    #[test]
    fn log_and_metrics_sections_parse_into_obs_options() {
        let (_cfg, opts) =
            load_str(&format!("{MINIMAL}\n[log]\nlevel = \"warn\"\n[metrics]\nbind = \"127.0.0.1:9601\"\n"))
                .unwrap();
        assert_eq!(opts.obs.log_level, LogLevel::Warn);
        assert_eq!(opts.obs.metrics_bind, Some("127.0.0.1:9601".parse().unwrap()));
    }

    #[test]
    fn a_bare_metrics_section_gets_the_default_bind() {
        let (_cfg, opts) = load_str(&format!("{MINIMAL}\n[metrics]\n")).unwrap();
        assert_eq!(opts.obs.metrics_bind, Some("127.0.0.1:9600".parse().unwrap()));
    }

    #[test]
    fn absent_sections_mean_off_and_info() {
        let (_cfg, opts) = load_str(MINIMAL).unwrap();
        assert_eq!(opts.obs.log_level, LogLevel::Info);
        assert_eq!(opts.obs.metrics_bind, None);
    }

    #[test]
    fn a_bad_log_level_is_a_refusal_naming_the_field() {
        let e = load_str(&format!("{MINIMAL}\n[log]\nlevel = \"verbose\"\n")).unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("log.level") && msg.contains("verbose"), "{msg}");
    }

    #[test]
    fn a_typo_inside_log_or_metrics_is_now_refused() {
        // M9 accepted arbitrary keys here (schema undefined); M10 defines it, so
        // deny_unknown_fields applies.
        let e = load_str(&format!("{MINIMAL}\n[metrics]\nport = 9600\n")).unwrap_err();
        assert!(e.to_string().contains("port"), "{e}");
    }
}
