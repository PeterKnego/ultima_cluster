// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! TOML configuration for the `uc2-gateway` binary.
//!
//! A one-to-one mirror of [`EdgeConfig`], deserialised with
//! `deny_unknown_fields` on every section (top level included) so a typo is a
//! startup refusal, not a silently-ignored setting — the same posture as
//! `uc_node`'s `config_file` module. This module does deserialisation and
//! defaulting ONLY; every semantic rule lives in [`EdgeConfig::validate`].
//!
//! `[limits]` and `[session]` are both optional sections, and every field
//! inside them is independently optional too — an absent section is
//! identical to a present-but-empty one, and both mean "take
//! [`EdgeConfig::defaults`]".

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::config::{ConfigError, EdgeConfig, Member};

/// Why a `gateway.toml` could not be turned into an [`EdgeConfig`].
#[derive(Debug, thiserror::Error)]
pub enum ConfigFileError {
    #[error("cannot read config file {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid config file {path}: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("{0}")]
    Invalid(#[from] ConfigError),
    /// An environment override ([`ENV_OVERRIDES`]) did not parse. Separate
    /// from [`ConfigFileError::Invalid`] on purpose: the message names the
    /// VARIABLE, because the file is fine and pointing the operator at
    /// `local.listen` would send them to edit the wrong thing.
    #[error("{0}")]
    Env(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalSection {
    instance_dir: PathBuf,
    app_id: String,
    listen: SocketAddr,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemberFile {
    node_id: u32,
    gateway: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LimitsSection {
    #[serde(default = "default_max_inflight")]
    max_inflight: u32,
    #[serde(default = "default_per_conn_inflight")]
    per_conn_inflight: u32,
    #[serde(default = "default_request_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default = "default_status_interval_ms")]
    status_interval_ms: u64,
    #[serde(default = "default_max_connections")]
    max_connections: u32,
}

impl Default for LimitsSection {
    fn default() -> Self {
        LimitsSection {
            max_inflight: default_max_inflight(),
            per_conn_inflight: default_per_conn_inflight(),
            request_timeout_ms: default_request_timeout_ms(),
            status_interval_ms: default_status_interval_ms(),
            max_connections: default_max_connections(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionSection {
    #[serde(default = "default_envelope")]
    envelope: bool,
}

impl Default for SessionSection {
    fn default() -> Self {
        SessionSection {
            envelope: default_envelope(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EdgeConfigFile {
    local: LocalSection,
    #[serde(default)]
    members: Vec<MemberFile>,
    #[serde(default)]
    limits: Option<LimitsSection>,
    #[serde(default)]
    session: Option<SessionSection>,
}

// These mirror EdgeConfig::defaults() exactly (uc_gateway/src/config.rs) —
// keep the two in lockstep by hand, the same way uc_node's config_file
// default_* functions track NodeConfig's.
fn default_max_inflight() -> u32 {
    4096
}
fn default_per_conn_inflight() -> u32 {
    256
}
fn default_request_timeout_ms() -> u64 {
    10_000
}
fn default_status_interval_ms() -> u64 {
    200
}
fn default_max_connections() -> u32 {
    1024
}
fn default_envelope() -> bool {
    true
}

/// Read and deserialise a gateway config file, then run
/// [`EdgeConfig::validate`] — unlike `uc_node`'s loader, there is no separate
/// semantic-preflight step here, so this function is the whole named-refusal
/// path for `uc2-gateway`.
pub fn load_from_path(path: &Path) -> Result<EdgeConfig, ConfigFileError> {
    let text = std::fs::read_to_string(path).map_err(|source| ConfigFileError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    // `parse_str` has no path to name, so it stamps [`IN_MEMORY_CONFIG`];
    // re-stamp the real one here so a refusal still tells the operator which
    // file to edit.
    parse_str_with_env(&text, |k| std::env::var(k).ok()).map_err(|e| match e {
        ConfigFileError::Parse { source, .. } => ConfigFileError::Parse {
            path: path.to_path_buf(),
            source,
        },
        other => other,
    })
}

/// The path [`ConfigFileError::Parse`] carries when the text did not come
/// from a file — see [`parse_str`].
pub const IN_MEMORY_CONFIG: &str = "<in-memory config>";

/// The file loader's pure inner: turn `gateway.toml` **text** into the same
/// [`EdgeConfig`], with the same defaulting, the same
/// [`EdgeConfig::validate`] call and the same [`ConfigFileError`] variants,
/// minus the file read.
///
/// This is what [`load_from_path`] runs, not a parallel implementation — the
/// only difference is that a [`ConfigFileError::Parse`] from here names
/// [`IN_MEMORY_CONFIG`] instead of a real path, which `load_from_path`
/// re-stamps. Public so a caller holding config text (a test, an embedder, or
/// the `uc_gateway_toml` fuzz target) can reach the parser without staging a
/// temporary file.
pub fn parse_str(text: &str) -> Result<EdgeConfig, ConfigFileError> {
    parse_str_with_env(text, |_| None)
}

/// The environment variables that override `gateway.toml`. Same posture and
/// same reasoning as `uc_node::config_file::ENV_OVERRIDES` — only keys that
/// vary between deploys of one image, and no key material.
pub const ENV_OVERRIDES: &[(&str, &str)] = &[
    ("UC2_GATEWAY_INSTANCE_DIR", "local.instance_dir"),
    ("UC2_GATEWAY_APP_ID", "local.app_id"),
    ("UC2_GATEWAY_LISTEN", "local.listen"),
    ("UC2_GATEWAY_MEMBERS", "members"),
];

/// [`parse_str`] plus the twelve-factor environment layer, environment
/// winning over file. `env` is an explicit lookup so [`parse_str`] stays a
/// pure function of its text (it is the `uc_gateway_toml` fuzz target's entry
/// point) and so tests need not call the `unsafe` `std::env::set_var`.
pub fn parse_str_with_env(
    text: &str,
    env: impl Fn(&str) -> Option<String>,
) -> Result<EdgeConfig, ConfigFileError> {
    let mut f: EdgeConfigFile = toml::from_str(text).map_err(|source| ConfigFileError::Parse {
        path: PathBuf::from(IN_MEMORY_CONFIG),
        source,
    })?;

    fn note(var: &'static str, value: &str) {
        uc_obs::obs_event!(Info, "config_env_override", var = var, value = value);
    }
    if let Some(v) = env("UC2_GATEWAY_INSTANCE_DIR") {
        f.local.instance_dir = PathBuf::from(&v);
        note("UC2_GATEWAY_INSTANCE_DIR", &v);
    }
    if let Some(v) = env("UC2_GATEWAY_APP_ID") {
        f.local.app_id = v.clone();
        note("UC2_GATEWAY_APP_ID", &v);
    }
    if let Some(v) = env("UC2_GATEWAY_LISTEN") {
        f.local.listen = v.parse().map_err(|_| {
            ConfigFileError::Env(format!(
                "UC2_GATEWAY_LISTEN=\"{v}\" is not a socket address (host:port)"
            ))
        })?;
        note("UC2_GATEWAY_LISTEN", &v);
    }
    if let Some(v) = env("UC2_GATEWAY_MEMBERS") {
        // `node_id@host:port` pairs, comma-separated. REPLACES the file's
        // table outright: the redirect map has to agree across gateways, so a
        // half-overridden one is never what anyone means.
        let mut out = Vec::new();
        for entry in v.split(',').map(str::trim) {
            let bad = ConfigFileError::Env;
            if entry.is_empty() {
                return Err(bad(
                    "UC2_GATEWAY_MEMBERS has an empty entry — expected node_id@host:port pairs"
                        .to_string(),
                ));
            }
            let (id_s, gw) = entry.split_once('@').ok_or_else(|| {
                bad(format!(
                    "UC2_GATEWAY_MEMBERS entry \"{entry}\" has no '@' — expected node_id@host:port"
                ))
            })?;
            let node_id = id_s.parse().map_err(|_| {
                bad(format!(
                    "UC2_GATEWAY_MEMBERS entry \"{entry}\": \"{id_s}\" is not a node id (u32)"
                ))
            })?;
            out.push(MemberFile {
                node_id,
                gateway: gw.to_string(),
            });
        }
        f.members = out;
        note("UC2_GATEWAY_MEMBERS", &v);
    }

    let limits = f.limits.unwrap_or_default();
    let session = f.session.unwrap_or_default();

    let cfg = EdgeConfig {
        instance_dir: f.local.instance_dir,
        app_id: f.local.app_id,
        listen: f.local.listen,
        members: f
            .members
            .into_iter()
            .map(|m| Member {
                node_id: m.node_id,
                gateway: m.gateway,
            })
            .collect(),
        session_envelope: session.envelope,
        max_inflight: limits.max_inflight,
        per_conn_inflight: limits.per_conn_inflight,
        status_interval: Duration::from_millis(limits.status_interval_ms),
        request_timeout: Duration::from_millis(limits.request_timeout_ms),
        max_connections: limits.max_connections,
    };
    cfg.validate()?;
    Ok(cfg)
}
