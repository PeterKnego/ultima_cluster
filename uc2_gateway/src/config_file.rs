// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! TOML configuration for the `uc2-gateway` binary.
//!
//! A one-to-one mirror of [`EdgeConfig`], deserialised with
//! `deny_unknown_fields` on every section (top level included) so a typo is a
//! startup refusal, not a silently-ignored setting — the same posture as
//! `uc2_node`'s `config_file` module. This module does deserialisation and
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
    Read { path: PathBuf, source: std::io::Error },
    #[error("invalid config file {path}: {source}")]
    Parse { path: PathBuf, source: toml::de::Error },
    #[error("{0}")]
    Invalid(#[from] ConfigError),
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
}

impl Default for LimitsSection {
    fn default() -> Self {
        LimitsSection {
            max_inflight: default_max_inflight(),
            per_conn_inflight: default_per_conn_inflight(),
            request_timeout_ms: default_request_timeout_ms(),
            status_interval_ms: default_status_interval_ms(),
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
        SessionSection { envelope: default_envelope() }
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

// These mirror EdgeConfig::defaults() exactly (uc2_gateway/src/config.rs) —
// keep the two in lockstep by hand, the same way uc2_node's config_file
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
fn default_envelope() -> bool {
    true
}

/// Read and deserialise a gateway config file, then run
/// [`EdgeConfig::validate`] — unlike `uc2_node`'s loader, there is no separate
/// semantic-preflight step here, so this function is the whole named-refusal
/// path for `uc2-gateway`.
pub fn load_from_path(path: &Path) -> Result<EdgeConfig, ConfigFileError> {
    let text = std::fs::read_to_string(path)
        .map_err(|source| ConfigFileError::Read { path: path.to_path_buf(), source })?;
    let f: EdgeConfigFile = toml::from_str(&text)
        .map_err(|source| ConfigFileError::Parse { path: path.to_path_buf(), source })?;

    let limits = f.limits.unwrap_or_default();
    let session = f.session.unwrap_or_default();

    let cfg = EdgeConfig {
        instance_dir: f.local.instance_dir,
        app_id: f.local.app_id,
        listen: f.local.listen,
        members: f.members.into_iter().map(|m| Member { node_id: m.node_id, gateway: m.gateway }).collect(),
        session_envelope: session.envelope,
        max_inflight: limits.max_inflight,
        per_conn_inflight: limits.per_conn_inflight,
        status_interval: Duration::from_millis(limits.status_interval_ms),
        request_timeout: Duration::from_millis(limits.request_timeout_ms),
    };
    cfg.validate()?;
    Ok(cfg)
}
