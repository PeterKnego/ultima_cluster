// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! [`EdgeConfig`] — everything an [`crate::Edge`] needs to know, and the
//! named refusals it produces when something is missing.
//!
//! Two decisions are worth calling out because they are *not* obvious:
//!
//! - **The node-id → gateway-address map is static.** The cnc page carries
//!   node ids and roles but no addresses, so an edge that wants to answer
//!   `REDIRECT{leader_node_id, addr}` has to be told the addresses out of
//!   band. That is `[[members]]` in `gateway.toml`, and it is the same shape
//!   as Aeron's `ingressEndpoints` string (spec §4.3).
//! - **`EdgeConfig::defaults()` is deliberately invalid.** It exists so tests
//!   and builders can write `..EdgeConfig::defaults()` and only name the
//!   fields they care about; the two fields with no sane default
//!   (`instance_dir`, `members`) come back empty and [`EdgeConfig::validate`]
//!   refuses them by name. A default that silently pointed somewhere would be
//!   worse than one that refuses to start.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// One cluster member's *gateway* address, keyed by its cluster node id.
///
/// `gateway` is the address a **client** dials (the peer edge's `listen`), not
/// the node's UDP replication `bind`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub node_id: u32,
    pub gateway: String,
}

/// Why an [`EdgeConfig`] was refused. Every variant names the field, so a
/// misconfigured `uc2-gateway` fails at startup with a sentence an operator
/// can act on rather than a panic or a silent default.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("instance_dir is empty: point it at the node's instance directory")]
    MissingInstanceDir,
    #[error("app_id is empty: it must match the node's app_id exactly")]
    MissingAppId,
    #[error("members is empty: an edge needs the node_id -> gateway address map to redirect")]
    NoMembers,
    #[error("members lists node_id {0} twice")]
    DuplicateMember(u32),
    #[error("member node_id {0} has an empty gateway address")]
    EmptyGateway(u32),
    #[error("max_inflight must be greater than zero")]
    ZeroMaxInflight,
    #[error("per_conn_inflight must be greater than zero")]
    ZeroPerConnInflight,
    #[error("per_conn_inflight ({per_conn}) exceeds max_inflight ({max}): one connection could \
             exhaust the whole engine window")]
    PerConnExceedsMax { per_conn: u32, max: u32 },
    #[error("status_interval must be greater than zero: it is also the edge->client liveness tick")]
    ZeroStatusInterval,
    #[error("request_timeout must be greater than zero")]
    ZeroRequestTimeout,
}

/// How one edge process is wired up.
#[derive(Clone, Debug)]
pub struct EdgeConfig {
    /// The local node's instance directory (the `Engine` attaches here).
    pub instance_dir: PathBuf,
    /// Must match the node's `app_id`, and the `app_id` clients send in
    /// `HELLO`.
    pub app_id: String,
    /// TCP address to accept remote clients on. Port `0` binds an ephemeral
    /// port — read it back with [`crate::Edge::local_addr`].
    pub listen: SocketAddr,
    /// Static node-id → gateway-address map, used for `REDIRECT` and
    /// `LEADER_CHANGED`.
    pub members: Vec<Member>,
    /// Prepend the 16-byte LE `client_id ++ seq` header to SUBMIT payloads and
    /// lift the `Sessioned` tag off the response into `RESPONSE` flags.
    /// `false` = raw pass-through (dedup becomes the application's problem).
    pub session_envelope: bool,
    /// The `Engine`'s inflight window, shared across every connection.
    pub max_inflight: u32,
    /// Initial credits granted to each connection in `HELLO_OK`. Shrinks under
    /// `Backpressure` and relaxes back up to this ceiling.
    pub per_conn_inflight: u32,
    /// A connection that has had no write for this long gets a standalone
    /// `STATUS`. Doubles as the edge→client liveness tick, so it must stay
    /// well under a client's `dead_after`.
    pub status_interval: Duration,
    /// The `Engine`'s per-request deadline. A request that blows it completes
    /// as `TimedOut` and the client is told `UNKNOWN`.
    pub request_timeout: Duration,
}

impl EdgeConfig {
    /// The documented defaults, with `instance_dir` and `members` left empty —
    /// see the module doc for why that is deliberate. Meant to be used as
    /// `EdgeConfig { instance_dir, app_id, listen, members, ..EdgeConfig::defaults() }`.
    pub fn defaults() -> Self {
        EdgeConfig {
            instance_dir: PathBuf::new(),
            app_id: String::new(),
            listen: SocketAddr::from(([0, 0, 0, 0], 0)),
            members: Vec::new(),
            session_envelope: true,
            max_inflight: 4096,
            per_conn_inflight: 256,
            status_interval: Duration::from_millis(200),
            request_timeout: Duration::from_secs(10),
        }
    }

    /// Refuse a configuration that cannot work, by name.
    ///
    /// `listen` is already a parsed [`SocketAddr`] by the time it gets here —
    /// the string form is parsed by whoever built the struct (the TOML loader,
    /// or the test's `"127.0.0.1:0".parse()`), so there is nothing left to
    /// check about it.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.instance_dir.as_os_str().is_empty() {
            return Err(ConfigError::MissingInstanceDir);
        }
        if self.app_id.is_empty() {
            return Err(ConfigError::MissingAppId);
        }
        if self.members.is_empty() {
            return Err(ConfigError::NoMembers);
        }
        let mut seen: Vec<u32> = Vec::with_capacity(self.members.len());
        for m in &self.members {
            if m.gateway.is_empty() {
                return Err(ConfigError::EmptyGateway(m.node_id));
            }
            if seen.contains(&m.node_id) {
                return Err(ConfigError::DuplicateMember(m.node_id));
            }
            seen.push(m.node_id);
        }
        if self.max_inflight == 0 {
            return Err(ConfigError::ZeroMaxInflight);
        }
        if self.per_conn_inflight == 0 {
            return Err(ConfigError::ZeroPerConnInflight);
        }
        if self.per_conn_inflight > self.max_inflight {
            return Err(ConfigError::PerConnExceedsMax {
                per_conn: self.per_conn_inflight,
                max: self.max_inflight,
            });
        }
        if self.status_interval.is_zero() {
            return Err(ConfigError::ZeroStatusInterval);
        }
        if self.request_timeout.is_zero() {
            return Err(ConfigError::ZeroRequestTimeout);
        }
        Ok(())
    }
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self::defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok() -> EdgeConfig {
        EdgeConfig {
            instance_dir: PathBuf::from("/var/lib/uc2/node0"),
            app_id: "app".into(),
            listen: "127.0.0.1:0".parse().unwrap(),
            members: vec![Member { node_id: 0, gateway: "h0:9100".into() }],
            ..EdgeConfig::defaults()
        }
    }

    #[test]
    fn a_complete_config_validates() {
        assert_eq!(ok().validate(), Ok(()));
    }

    #[test]
    fn defaults_alone_are_refused_by_name() {
        // The whole point of `defaults()`: it is a spread base, not a config.
        assert_eq!(EdgeConfig::defaults().validate(), Err(ConfigError::MissingInstanceDir));
        let c = EdgeConfig { instance_dir: "/x".into(), ..EdgeConfig::defaults() };
        assert_eq!(c.validate(), Err(ConfigError::MissingAppId));
        let c = EdgeConfig { app_id: "app".into(), ..c };
        assert_eq!(c.validate(), Err(ConfigError::NoMembers));
    }

    #[test]
    fn duplicate_and_empty_members_are_refused() {
        let c = EdgeConfig {
            members: vec![
                Member { node_id: 1, gateway: "a:1".into() },
                Member { node_id: 1, gateway: "b:2".into() },
            ],
            ..ok()
        };
        assert_eq!(c.validate(), Err(ConfigError::DuplicateMember(1)));
        let c = EdgeConfig {
            members: vec![Member { node_id: 3, gateway: String::new() }],
            ..ok()
        };
        assert_eq!(c.validate(), Err(ConfigError::EmptyGateway(3)));
    }

    #[test]
    fn per_conn_credits_may_not_exceed_the_engine_window() {
        let c = EdgeConfig { max_inflight: 8, per_conn_inflight: 9, ..ok() };
        assert_eq!(c.validate(), Err(ConfigError::PerConnExceedsMax { per_conn: 9, max: 8 }));
        let c = EdgeConfig { per_conn_inflight: 0, ..ok() };
        assert_eq!(c.validate(), Err(ConfigError::ZeroPerConnInflight));
        let c = EdgeConfig { max_inflight: 0, ..ok() };
        assert_eq!(c.validate(), Err(ConfigError::ZeroMaxInflight));
    }

    #[test]
    fn zero_durations_are_refused() {
        let c = EdgeConfig { status_interval: Duration::ZERO, ..ok() };
        assert_eq!(c.validate(), Err(ConfigError::ZeroStatusInterval));
        let c = EdgeConfig { request_timeout: Duration::ZERO, ..ok() };
        assert_eq!(c.validate(), Err(ConfigError::ZeroRequestTimeout));
    }
}
