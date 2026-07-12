// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Service configuration + error type.

use std::path::PathBuf;

/// Where the service attaches and which cluster it belongs to. The service
/// resolves the node's well-known IPC paths under `instance_dir` and presents
/// `app_id` at the cnc-page attach check (a mismatch = wrong cluster).
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub instance_dir: PathBuf,
    pub app_id: String,
}

impl ServiceConfig {
    pub fn new(instance_dir: impl Into<PathBuf>, app_id: impl Into<String>) -> Self {
        Self { instance_dir: instance_dir.into(), app_id: app_id.into() }
    }
}

/// Why a service could not attach or start.
#[derive(thiserror::Error, Debug)]
pub enum ServiceError {
    #[error("cnc attach error: {0}")]
    Cnc(#[from] uc2_log::cnc::CncError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("ring error: {0}")]
    Ring(String),
    /// The state machine reports a `last_applied` position beyond what the
    /// node's log holds — provably not this cluster's state (a stale or
    /// wrong-app on-disk SM). Refuse rather than replay off a phantom cursor.
    #[error("state-machine/journal drift: service last_applied={service}, journal frontier={journal}")]
    Drift { service: u64, journal: u64 },
}
