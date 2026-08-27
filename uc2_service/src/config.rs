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
    /// M6 Task 3: the snapshot-building cadence. Default `SnapshotPolicy::default()`
    /// (`interval_bytes: 0`, "never") — see that type's doc.
    pub snapshot_policy: SnapshotPolicy,
    /// M14a: which declared FSM slot this process is; default 0. Refused at
    /// attach if not declared on the node's page.
    pub service_id: u8,
}

impl ServiceConfig {
    pub fn new(instance_dir: impl Into<PathBuf>, app_id: impl Into<String>) -> Self {
        Self {
            instance_dir: instance_dir.into(),
            app_id: app_id.into(),
            snapshot_policy: SnapshotPolicy::default(),
            service_id: 0,
        }
    }

    /// Builder-pattern setter (mirrors [`ServiceBuilder::output_handler`](crate::ServiceBuilder::output_handler)):
    /// install a non-default snapshot cadence. Only observed by
    /// [`ServiceBuilder::start_with_snapshots`](crate::ServiceBuilder::start_with_snapshots) —
    /// plain [`start`](crate::ServiceBuilder::start) never spawns the builder
    /// thread, so a policy set here is simply unused on that path.
    pub fn snapshot_policy(mut self, policy: SnapshotPolicy) -> Self {
        self.snapshot_policy = policy;
        self
    }

    /// M14a: declare which FSM slot this process is (default 0).
    pub fn service_id(mut self, id: u8) -> Self {
        self.service_id = id;
        self
    }
}

/// The snapshot-building cadence knob (M6 Task 3). The apply thread triggers a
/// build once `service_applied` has advanced at least `interval_bytes` past the
/// position of the last snapshot attempt.
///
/// **Default is `interval_bytes: 0` = "never".** This is the purge-off-by-
/// default starting point (spec M6): with no interval configured, the builder
/// thread (when spawned via `start_with_snapshots`) never trips, so no
/// snapshot file is ever written, `cnc.snapshots().service_snapshot_pos` stays
/// `0`, and (once Task 4 lands) the purge driver never advances — a
/// snapshot-capable SM that is never asked to snapshot behaves exactly like one
/// that isn't, from the log-retention perspective.
///
/// The derived `Default` yields `interval_bytes: 0` — the "never" case above.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotPolicy {
    pub interval_bytes: u64,
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
    /// A journal-replay reconstruction (Task 9) could not read the archived
    /// log — a genuine journal I/O error (a torn/half-flushed record is handled
    /// conservatively by the read-only `TailReader`, not surfaced here). This is
    /// fail-stop: the service cannot rebuild its state.
    #[error("journal replay error: {0}")]
    Replay(String),
    /// M6 Task 5: the journal has been purged below the position the service
    /// needs (`first_available > needed`) AND the state machine cannot install a
    /// snapshot to fill the gap — either it does not implement
    /// [`SnapshotStateMachine`](crate::SnapshotStateMachine), or no on-disk
    /// snapshot covers the floor. Fail-stop: reconstruction is impossible, so the
    /// apply agent dies with the contract named rather than replaying a partial
    /// prefix onto a phantom cursor (the silent-gap bug class).
    #[error(
        "SnapshotRequired: journal purged below the service frontier \
         (needs {needed}, first available {first_available}) and the state \
         machine cannot install a covering snapshot"
    )]
    SnapshotRequired { needed: u64, first_available: u64 },
}

/// Why a [`SnapshotStateMachine`](crate::SnapshotStateMachine) freeze/stream/
/// install failed. Mirrors the v1 `uc_service::SnapshotError` shape (an I/O
/// failure or a codec/serialization failure), re-exported at the crate root.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec: {0}")]
    Codec(String),
}
