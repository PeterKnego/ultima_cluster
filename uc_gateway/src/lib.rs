// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc_gateway`: the **edge** — the process that lets a client reach an
//! ultima_cluster node from somewhere other than the node's own machine.
//!
//! A node's native client interface is shared memory: fast, and strictly
//! same-host. The edge terminates the framed TCP remote protocol
//! ([`uc_remote`]) on the node's host and pumps it through one
//! [`uc_client::Engine`] over that shmem, so a remote client gets the
//! cluster's semantics — leader redirection, credit-paced pipelining,
//! exactly-once writes via the session envelope — without linking any of the
//! cluster stack.
//!
//! ```text
//!   remote client ──TCP (uc_remote v1)──▶ Edge ──shmem──▶ uc_node
//! ```
//!
//! The edge holds **no durable state**. It is a pure relay: kill it and every
//! client reconnects to another member's edge per its static member list,
//! losing nothing that was acknowledged.
//!
//! ```no_run
//! use uc_gateway::{Edge, EdgeConfig, Member};
//!
//! let edge = Edge::start(EdgeConfig {
//!     instance_dir: "/var/lib/uc2/node0".into(),
//!     app_id: "myapp".into(),
//!     listen: "0.0.0.0:9100".parse().unwrap(),
//!     members: vec![Member { node_id: 0, gateway: "host0:9100".into() }],
//!     ..EdgeConfig::defaults()
//! })
//! .unwrap();
//! println!("serving on {}", edge.local_addr());
//! edge.stop();
//! ```
//!
//! Spec: `docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md` §4.3.
//!
//! **Semver:** see `docs/reference/semver-policy.md`. Promised surface: the
//! `gateway.toml` file and the [`EdgeConfig`]/[`Member`] types that mirror
//! it. [`Edge`] itself is a reference implementation, not a stable API.

mod config;
pub mod config_file;
mod conn;
mod edge;
mod watch;

pub use config::{ConfigError, EdgeConfig, Member};
pub use edge::{BUDGET_HEADROOM_DIV, Edge, EdgeError, EdgeStats, budget_for, grant_for};
