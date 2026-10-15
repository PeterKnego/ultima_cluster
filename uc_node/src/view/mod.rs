// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The cluster view (storage-service project 4, spec
//! `docs/superpowers/specs/2026-10-14-uc2-cluster-view-design.md`): one
//! versioned JSON document an operator or an AI reads — the committed
//! cluster state, this node's holdings and services, and (on the leader)
//! every member's holdings. Served as `GET /cluster`; `uc2ctl` builds the
//! same document offline from the newest cluster artifact.
//!
//! The serde types in [`model`] ARE the schema: adding a field keeps
//! [`SCHEMA`]; renaming, removing or changing a field's meaning bumps it.

pub mod build;
pub mod convert;
pub mod model;

pub use model::ClusterViewDoc;

/// The document's schema tag.
pub const SCHEMA: &str = "uc2.cluster-view/1";

/// `tier_behind`'s threshold: the `Uc2BackupBehind` alert's level
/// (`uc2_backup_lag_bytes > 268435456`), without the alert's `for: 15m`.
pub const TIER_BEHIND_BYTES: u64 = 256 * 1024 * 1024;
