// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! TOML configuration for the `uc2-node` daemon.
//!
//! A one-to-one mirror of [`NodeConfig`], deserialised with
//! `deny_unknown_fields` so a typo is a startup refusal rather than a
//! silently-ignored setting — the same posture as M8's crypto boot refusal.
//! This module does deserialisation ONLY; every semantic rule lives in
//! [`crate::preflight`] — with the same one exception `log.level` already
//! carried pre-M12b: `[crypto]`/`[admin]` are EXPLICIT CHOICES (spec §3.3),
//! and validating them (`enabled` vs. the paths it requires, `auth` vs. the
//! keys it requires) is inseparable from turning the file's `Option`s into
//! the `CryptoConfig`/[`AdminSection`] values this module already owns
//! producing, so those checks live here too rather than splitting one
//! section's rule across two modules.
//!
//! `[log]` and `[metrics]` are M10's observability sections. Both are
//! optional and, like `[purge]`, ABSENT means the feature is off: no `[log]`
//! means the default level (`info`); no `[metrics]` means no endpoint.
//! `deny_unknown_fields` applies to their contents too — a typo inside
//! either section is a startup refusal naming the key, not a
//! silently-ignored setting. `[crypto]` and `[admin]` (M12b) are NOT
//! optional in this sense — ABSENT is itself a refusal
//! ([`ConfigError::CryptoChoiceRequired`]/[`ConfigError::AdminChoiceRequired`]);
//! see [`AdminSection`].

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
    /// semantic parse (`log.level`; the M12b `crypto.*`/`admin.*`
    /// cross-field rules below). `detail` already names the field and echoes
    /// the bad value — see `LogLevel::from_str` — so this variant must not
    /// repeat `field` into the message and double it up.
    #[error("{detail}")]
    Invalid { field: &'static str, detail: String },
    /// M12b (spec §3.3): `[crypto]` is an explicit choice, not an
    /// absent-means-off section like `[purge]` — a `node.toml` must say
    /// `enabled = false` (cleartext) or `enabled = true` with the key paths.
    #[error(
        "[crypto] section is required: set enabled = false for cleartext (the default posture) \
         or enabled = true with key_path/allowlist_path"
    )]
    CryptoChoiceRequired,
    /// M12b (spec §3.3): `[admin]` is likewise an explicit choice — a
    /// `node.toml` must say `auth = "hmac"` with `keys = [...]` or
    /// `auth = "none"` (today's posture: filesystem access is the boundary).
    #[error(
        "[admin] section is required: auth = \"hmac\" with keys = [...] or auth = \"none\" \
         (filesystem access is the boundary)"
    )]
    AdminChoiceRequired,
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

/// M12b (spec §3.3, §5.4): `enabled` is now REQUIRED — no default — so a
/// `[crypto]` section that forgot it is a parse error naming the field
/// (`toml`'s own "missing field `enabled`" message), the same shape as any
/// other missing-required-field refusal in this file. `key_path`/
/// `allowlist_path` become optional at the TYPE level (so `enabled = false`
/// can omit them) — `load_from_path` enforces the real rule: `enabled =
/// true` requires both, `enabled = false` must not carry either.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CryptoSection {
    enabled: bool,
    #[serde(default)]
    key_path: Option<PathBuf>,
    #[serde(default)]
    allowlist_path: Option<PathBuf>,
    #[serde(default)]
    rotation_interval_ns: Option<u64>,
    #[serde(default)]
    rotation_bytes: Option<u64>,
}

/// M12b (spec §5.1): one named HMAC admin key, as it appears in
/// `[admin].keys`. Not the loaded [`uc2_crypto::admin::AdminKey`] — this is
/// only the file's `(name, key_path)` pointer; `uc2-node`'s `main` is what
/// calls `AdminKey::load` on each entry once preflight has passed.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminKeyEntry {
    pub name: String,
    pub key_path: PathBuf,
}

/// M12b (spec §5.1): `[admin].auth`. `None` is today's pre-M12b posture
/// (filesystem access on the instance directory is the whole boundary);
/// `Hmac` opts into named, TTL-bounded request signatures — see
/// [`uc2_crypto::admin::AdminPolicy`], which this maps onto in the daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AdminAuthMode {
    None,
    Hmac,
}

/// M12b (spec §3.3, §5.1): `[admin]`, typed. REQUIRED — no default,
/// [`ConfigError::AdminChoiceRequired`] on absence, mirroring `[crypto]`.
/// `auth = "hmac"` requires at least one (uniquely-named) entry in `keys`;
/// `auth = "none"` requires `keys` to be empty (a stray key list under
/// `"none"` would just be silently ignored otherwise — refused by name
/// instead). `request_ttl_ms` applies under either mode and must be
/// `>= 1000` regardless — see `load_from_path`'s validation, not this
/// struct's own (structural-only) deserialisation.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminSection {
    pub auth: AdminAuthMode,
    #[serde(default)]
    pub keys: Vec<AdminKeyEntry>,
    #[serde(default = "default_admin_ttl_ms")]
    pub request_ttl_ms: u64,
}

/// A hand-written [`Default`] (not derived: `AdminAuthMode` has no `Default`
/// of its own, deliberately — a config file must always state `auth`
/// explicitly). Used only by test/harness code that needs a valid
/// [`crate::preflight::StartupOptions`] without going through
/// [`load_from_path`]; the value matches this same module's `request_ttl_ms`
/// default so it is never accidentally the thing a `< 1000` test is testing.
impl Default for AdminSection {
    fn default() -> Self {
        AdminSection { auth: AdminAuthMode::None, keys: Vec::new(), request_ttl_ms: default_admin_ttl_ms() }
    }
}

fn default_admin_ttl_ms() -> u64 {
    30_000
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
    /// M12b: no longer absent-means-off like `[purge]` — see
    /// [`ConfigError::CryptoChoiceRequired`].
    #[serde(default)]
    crypto: Option<CryptoSection>,
    /// M12b: required — see [`ConfigError::AdminChoiceRequired`].
    #[serde(default)]
    admin: Option<AdminSection>,
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
    // M12b (spec §3.3, §5.4): explicit choice — absent `[crypto]` is itself
    // a refusal, not "off".
    let crypto_section = f.crypto.ok_or(ConfigError::CryptoChoiceRequired)?;
    let crypto = if crypto_section.enabled {
        let key_path = crypto_section.key_path.ok_or_else(|| ConfigError::Invalid {
            field: "crypto.key_path",
            detail: "crypto.enabled = true requires crypto.key_path".to_string(),
        })?;
        let allowlist_path = crypto_section.allowlist_path.ok_or_else(|| ConfigError::Invalid {
            field: "crypto.allowlist_path",
            detail: "crypto.enabled = true requires crypto.allowlist_path".to_string(),
        })?;
        let d = RotationPolicy::default();
        CryptoConfig::Enabled {
            key_path,
            allowlist_path,
            rotation: RotationPolicy {
                interval_ns: crypto_section.rotation_interval_ns.unwrap_or(d.interval_ns),
                bytes: crypto_section.rotation_bytes.unwrap_or(d.bytes),
            },
        }
    } else {
        // Ruling 1: `enabled = false` while still naming key material is
        // refused rather than silently ignored — a config that LOOKS
        // encrypted must not actually run cleartext.
        if crypto_section.key_path.is_some() || crypto_section.allowlist_path.is_some() {
            return Err(ConfigError::Invalid {
                field: "crypto.enabled",
                detail: "enabled = false but key_path/allowlist_path given — remove them or set \
                         enabled = true"
                    .to_string(),
            });
        }
        CryptoConfig::Disabled
    };
    // M12b (spec §3.3, §5.1): same explicit-choice posture for `[admin]`.
    let admin = f.admin.ok_or(ConfigError::AdminChoiceRequired)?;
    if admin.request_ttl_ms < 1000 {
        return Err(ConfigError::Invalid {
            field: "admin.request_ttl_ms",
            detail: format!("admin.request_ttl_ms must be >= 1000, got {}", admin.request_ttl_ms),
        });
    }
    match admin.auth {
        AdminAuthMode::None => {
            if !admin.keys.is_empty() {
                return Err(ConfigError::Invalid {
                    field: "admin.keys",
                    detail: "admin.auth = \"none\" but admin.keys is non-empty — remove the keys \
                             or set auth = \"hmac\""
                        .to_string(),
                });
            }
        }
        AdminAuthMode::Hmac => {
            if admin.keys.is_empty() {
                return Err(ConfigError::Invalid {
                    field: "admin.keys",
                    detail: "admin.auth = \"hmac\" requires at least one entry in admin.keys"
                        .to_string(),
                });
            }
            let mut seen = std::collections::HashSet::new();
            // M12b final review (M5): also reject an empty name, and a
            // collision in the FNV-1a-64 name hash. The hash — not the name —
            // is what travels on the 64-byte cnc auth line and what
            // `verify_admin` looks the key up by, so two distinct names that
            // hash alike would make the node verify against whichever entry
            // it found first. Vanishingly unlikely by accident; refused
            // loudly at load rather than silently mis-attributed in
            // `audit.jsonl` at 3 a.m.
            let mut seen_hash = std::collections::HashMap::new();
            for k in &admin.keys {
                if k.name.is_empty() {
                    return Err(ConfigError::Invalid {
                        field: "admin.keys",
                        detail: "an entry in admin.keys has an empty name — every admin \
                                 key needs a name (it is what uc2ctl passes as \
                                 --admin-key-name and what audit.jsonl records as the actor)"
                            .to_string(),
                    });
                }
                if !seen.insert(k.name.as_str()) {
                    return Err(ConfigError::Invalid {
                        field: "admin.keys",
                        detail: format!(
                            "duplicate name {:?} in admin.keys — key names must be unique",
                            k.name
                        ),
                    });
                }
                let h = uc2_crypto::admin::fnv1a64(&k.name);
                if let Some(prev) = seen_hash.insert(h, k.name.as_str()) {
                    return Err(ConfigError::Invalid {
                        field: "admin.keys",
                        detail: format!(
                            "admin.keys entries {prev:?} and {:?} collide in the 64-bit \
                             FNV-1a name hash ({h:#018x}) that identifies a key on the \
                             wire — rename one of them",
                            k.name
                        ),
                    });
                }
            }
        }
    }
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
        admin,
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

    /// A single-voter config with none of the OPTIONAL sections — the
    /// minimal document every optional-section test appends to. `[crypto]`
    /// and `[admin]` are NOT optional (M12b, spec §3.3) so both are stated
    /// here at their cleartext/filesystem defaults — the same posture every
    /// pre-M12b fixture had implicitly.
    const MINIMAL: &str = r#"
id = 1
bind = "10.0.0.1:9100"
instance_dir = "/srv/uc2/n1"
app_id = "myapp"

[[members]]
id = 1
addr = "10.0.0.1:9100"

[crypto]
enabled = false

[admin]
auth = "none"
"#;

    /// Like `MINIMAL` but WITHOUT `[crypto]`/`[admin]` — the base every
    /// explicit-choice test below appends its own version of one or both
    /// sections to, since `MINIMAL` itself already states them.
    const MINIMAL_NO_CRYPTO_ADMIN: &str = r#"
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

[crypto]
enabled = false

[admin]
auth = "none"
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

[crypto]
enabled = false

[admin]
auth = "none"
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
enabled = true
key_path = "/etc/uc2/node.key"
allowlist_path = "/etc/uc2/allowlist.toml"

[admin]
auth = "none"
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

[crypto]
enabled = false

[admin]
auth = "none"
"#;
        let p = write(dir.path(), body);
        let (_cfg, opts) = load_from_path(&p).unwrap();
        assert!(!opts.allow_volatile_fs, "production default must be refuse");

        // PREPENDED, not appended: `body` ends with a table ([admin]), so a
        // key added after it would belong to that table, not the document root.
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

    // ---- M12b (spec §3.3): [crypto]/[admin] are explicit choices ----

    #[test]
    fn absent_crypto_section_is_an_explicit_choice_refusal() {
        let body = format!("{MINIMAL_NO_CRYPTO_ADMIN}\n[admin]\nauth = \"none\"\n");
        let err = load_str(&body).unwrap_err();
        assert!(matches!(err, ConfigError::CryptoChoiceRequired), "got: {err:?}");
        assert!(err.to_string().contains("[crypto]"), "{err}");
    }

    #[test]
    fn crypto_section_without_enabled_is_a_parse_error_naming_the_field() {
        let body = format!(
            "{MINIMAL_NO_CRYPTO_ADMIN}\n[crypto]\nkey_path = \"/etc/uc2/node.key\"\n\
             allowlist_path = \"/etc/uc2/allowlist.toml\"\n[admin]\nauth = \"none\"\n"
        );
        let err = load_str(&body).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("enabled"), "must name the missing field, got: {msg}");
    }

    #[test]
    fn crypto_enabled_without_key_path_names_it() {
        let body =
            format!("{MINIMAL_NO_CRYPTO_ADMIN}\n[crypto]\nenabled = true\n[admin]\nauth = \"none\"\n");
        let err = load_str(&body).unwrap_err();
        match err {
            ConfigError::Invalid { field, .. } => assert_eq!(field, "crypto.key_path"),
            other => panic!("expected Invalid{{field: \"crypto.key_path\"}}, got {other:?}"),
        }
    }

    #[test]
    fn crypto_enabled_without_allowlist_path_names_it() {
        let body = format!(
            "{MINIMAL_NO_CRYPTO_ADMIN}\n[crypto]\nenabled = true\nkey_path = \"/etc/uc2/node.key\"\n\
             [admin]\nauth = \"none\"\n"
        );
        let err = load_str(&body).unwrap_err();
        match err {
            ConfigError::Invalid { field, .. } => assert_eq!(field, "crypto.allowlist_path"),
            other => panic!("expected Invalid{{field: \"crypto.allowlist_path\"}}, got {other:?}"),
        }
    }

    /// Ruling 1: `enabled = false` while still naming key material must be
    /// refused, not silently ignored — a config that LOOKS encrypted must
    /// never actually run cleartext.
    #[test]
    fn crypto_disabled_with_paths_given_is_refused() {
        let body = format!(
            "{MINIMAL_NO_CRYPTO_ADMIN}\n[crypto]\nenabled = false\n\
             key_path = \"/etc/uc2/node.key\"\n[admin]\nauth = \"none\"\n"
        );
        let err = load_str(&body).unwrap_err();
        match err {
            ConfigError::Invalid { field, .. } => assert_eq!(field, "crypto.enabled"),
            other => panic!("expected Invalid{{field: \"crypto.enabled\"}}, got {other:?}"),
        }
    }

    #[test]
    fn absent_admin_section_is_an_explicit_choice_refusal() {
        let body = format!("{MINIMAL_NO_CRYPTO_ADMIN}\n[crypto]\nenabled = false\n");
        let err = load_str(&body).unwrap_err();
        assert!(matches!(err, ConfigError::AdminChoiceRequired), "got: {err:?}");
        assert!(err.to_string().contains("[admin]"), "{err}");
    }

    #[test]
    fn admin_hmac_with_no_keys_is_refused() {
        let body =
            format!("{MINIMAL_NO_CRYPTO_ADMIN}\n[crypto]\nenabled = false\n[admin]\nauth = \"hmac\"\n");
        let err = load_str(&body).unwrap_err();
        match err {
            ConfigError::Invalid { field, .. } => assert_eq!(field, "admin.keys"),
            other => panic!("expected Invalid{{field: \"admin.keys\"}}, got {other:?}"),
        }
    }

    #[test]
    fn admin_hmac_duplicate_key_names_are_refused() {
        let body = format!(
            "{MINIMAL_NO_CRYPTO_ADMIN}\n[crypto]\nenabled = false\n[admin]\nauth = \"hmac\"\n\
             keys = [{{ name = \"ops-alice\", key_path = \"/etc/uc2/admin/a.key\" }}, \
             {{ name = \"ops-alice\", key_path = \"/etc/uc2/admin/b.key\" }}]\n"
        );
        let err = load_str(&body).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ops-alice"), "must name the duplicate key, got: {msg}");
        match err {
            ConfigError::Invalid { field, .. } => assert_eq!(field, "admin.keys"),
            other => panic!("expected Invalid{{field: \"admin.keys\"}}, got {other:?}"),
        }
    }

    /// M12b final review (M5): a key with an empty name is refused. The name
    /// is what `uc2ctl` passes as `--admin-key-name`, what the FNV-1a name
    /// hash on the wire is derived from, and what `audit.jsonl` records as
    /// the actor — an empty one makes all three meaningless. (The sibling
    /// name-hash-collision refusal in the same loop has no test: exhibiting
    /// a real 64-bit FNV-1a collision needs ~2^32 work, so the check is
    /// carried on its argument, not on a fixture.)
    #[test]
    fn admin_hmac_empty_key_name_is_refused() {
        let body = format!(
            "{MINIMAL_NO_CRYPTO_ADMIN}\n[crypto]\nenabled = false\n[admin]\nauth = \"hmac\"\n\
             keys = [{{ name = \"\", key_path = \"/etc/uc2/admin/a.key\" }}]\n"
        );
        let err = load_str(&body).unwrap_err();
        match err {
            ConfigError::Invalid { field, ref detail } => {
                assert_eq!(field, "admin.keys");
                // A wrapped string literal without a `\` continuation keeps
                // every leading space of the next source line, which reaches
                // the operator's terminal as a long run of blanks mid-sentence.
                assert!(
                    !detail.contains("  "),
                    "the refusal message carries source indentation: {detail:?}"
                );
            }
            other => panic!("expected Invalid{{field: \"admin.keys\"}}, got {other:?}"),
        }
    }

    #[test]
    fn an_unknown_key_inside_admin_is_refused_by_name() {
        let body = format!(
            "{MINIMAL_NO_CRYPTO_ADMIN}\n[crypto]\nenabled = false\n[admin]\nauth = \"none\"\nbogus = 1\n"
        );
        let err = load_str(&body).unwrap_err();
        assert!(err.to_string().contains("bogus"), "got: {err}");
    }

    #[test]
    fn admin_none_with_keys_is_refused() {
        let body = format!(
            "{MINIMAL_NO_CRYPTO_ADMIN}\n[crypto]\nenabled = false\n[admin]\nauth = \"none\"\n\
             keys = [{{ name = \"ops-alice\", key_path = \"/etc/uc2/admin/a.key\" }}]\n"
        );
        let err = load_str(&body).unwrap_err();
        match err {
            ConfigError::Invalid { field, .. } => assert_eq!(field, "admin.keys"),
            other => panic!("expected Invalid{{field: \"admin.keys\"}}, got {other:?}"),
        }
    }

    #[test]
    fn admin_ttl_below_1000_is_refused() {
        let body = format!(
            "{MINIMAL_NO_CRYPTO_ADMIN}\n[crypto]\nenabled = false\n[admin]\nauth = \"none\"\n\
             request_ttl_ms = 999\n"
        );
        let err = load_str(&body).unwrap_err();
        match err {
            ConfigError::Invalid { field, .. } => assert_eq!(field, "admin.request_ttl_ms"),
            other => panic!("expected Invalid{{field: \"admin.request_ttl_ms\"}}, got {other:?}"),
        }
    }

    #[test]
    fn admin_hmac_maps_into_startup_options() {
        let body = format!(
            "{MINIMAL_NO_CRYPTO_ADMIN}\n[crypto]\nenabled = false\n[admin]\nauth = \"hmac\"\n\
             keys = [{{ name = \"ops-alice\", key_path = \"/etc/uc2/admin/alice.key\" }}]\n\
             request_ttl_ms = 5000\n"
        );
        let (_cfg, opts) = load_str(&body).unwrap();
        assert!(matches!(opts.admin.auth, AdminAuthMode::Hmac));
        assert_eq!(opts.admin.keys.len(), 1);
        assert_eq!(opts.admin.keys[0].name, "ops-alice");
        assert_eq!(opts.admin.request_ttl_ms, 5000);
    }
}
