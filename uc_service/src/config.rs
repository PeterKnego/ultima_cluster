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
        Self {
            instance_dir: instance_dir.into(),
            app_id: app_id.into(),
        }
    }
}

/// Why a service could not attach or start.
#[derive(thiserror::Error, Debug)]
pub enum ServiceError {
    #[error("cnc attach error: {0}")]
    Cnc(#[from] uc_log::cnc::CncError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("ring error: {0}")]
    Ring(String),
    /// The state machine reports a `last_applied` position beyond what the
    /// node's log holds — provably not this cluster's state (a stale or
    /// wrong-app on-disk SM). Refuse rather than replay off a phantom cursor.
    #[error(
        "state-machine/journal drift: service last_applied={service}, journal frontier={journal}"
    )]
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
    /// Coordinated-snapshot ruling P6: an on-disk snapshot artifact's
    /// framework envelope does not verify — it is truncated, is not a UC
    /// artifact at all, or (the case this exists for) names a DIFFERENT
    /// instant than the file name claims. A renamed or mis-copied artifact
    /// installed under a newer tag would leave every frame between the two
    /// positions unapplied — a silent state gap, the class
    /// [`SnapshotRequired`](Self::SnapshotRequired) exists to fail-stop on.
    /// Refuse by name instead. Plan B2 T3: also the artifact's `S::VERSION`
    /// cross-check — an UNPINNED install (the reconstruction gap guard,
    /// `replay.rs`) requires the artifact to have been built by THIS
    /// incarnation's own `S::VERSION`; a pinned install (`attach`, Task 4)
    /// requires it to match the pin's `from` instead. Either mismatch is this
    /// same variant, with an [`EnvelopeError::VersionMismatch`
    /// ](crate::snapshots::EnvelopeError::VersionMismatch) source — the §2.3
    /// counterfactual (a newer binary silently installing and tail-replaying
    /// an older artifact under its own, possibly different, `apply`) refused
    /// by name rather than "succeeding".
    #[error("MistaggedSnapshot: {path}: {source}")]
    MistaggedSnapshot {
        path: String,
        #[source]
        source: crate::snapshots::EnvelopeError,
    },
    /// FSM identity (spec §4.3): the attaching type's `S::IDENTITY.name` is
    /// not declared on the node's page (`CncPage::row_of` found nothing).
    #[error(
        "FSM {name:?} is not declared on this node (declared, in row order: \
         {declared:?}); add it to [services] names on the node, or attach \
         the service that is"
    )]
    UnknownFsm { name: String, declared: Vec<String> },
    /// Same as [`UnknownFsm`](Self::UnknownFsm), but the page declares no
    /// names at all — the node is far more likely to be older than cnc 3.1
    /// than genuinely configured with an empty `[services]`. A separate
    /// variant (rather than branching one shared string at runtime) so the
    /// hint is exact and the common case's message stays unchanged.
    #[error(
        "FSM {name:?} is not declared on this node (declared, in row order: \
         []); the node's page carries no names — is the node older than cnc 3.1?"
    )]
    UnknownFsmNoNames { name: String },
    /// The node is mid-boot: its page carries FSM names on line 7 but
    /// `services_declared` still reads 0. `create_file` publishes a complete,
    /// crc-valid header (names included) and the node stores the declared set
    /// and lag policy a few statements later; a page with names and no
    /// declared set exists ONLY in that gap — a configured node always
    /// declares a nonzero set, and a harness page has no names. Attaching on
    /// it would read the harness `(0, _) => Off` lag arm and fix `LagMode::Off`
    /// for the attachment's life, silently, on a lockstep cluster.
    #[error(
        "the node is still initialising its cnc page (FSM names published, \
         declared set not yet) — it is booting; retry the attach"
    )]
    NodeBooting,
    /// M14a: another live process holds `service.<row>.lock`.
    #[error(
        "another process already holds FSM {name:?} at row {row} on this \
         instance dir (service.{row}.lock)"
    )]
    AlreadyAttached { name: String, row: u8 },
    /// Plan B2 T4 (spec §3 S4 step 5): the row carries a committed upgrade
    /// pin naming a target version, and this binary is not it. The pin is the
    /// cluster's decision about which version may serve the row from the
    /// origin onward, so a stale binary rejoining afterwards — the one thing
    /// the pin exists to stop — is refused BY NAME, before any slot word is
    /// written.
    #[error(
        "FSM {name:?} at row {row} is pinned to version {pinned:#010x} from \
         origin {origin}, but this binary is {mine:#010x}; a stale binary \
         cannot rejoin after `uc2ctl upgrade pin`"
    )]
    PinnedVersionMismatch {
        name: String,
        row: u8,
        origin: u64,
        pinned: u32,
        mine: u32,
    },
    /// The row's four pin words could not be read consistently through the
    /// `pin_seq` seqlock ([`uc_log::cnc::PinRead::Contended`]). A reader that
    /// must DECIDE never treats that as "no pin": attaching unpinned off a
    /// half-published triple would skip an install the cluster requires.
    /// Transient by construction — the next attach converges.
    #[error(
        "row {row}'s pin words could not be read consistently (the \
         uc2-cluster agent is mid-publish); retry the attach"
    )]
    PinUnreadable { row: u8 },
    /// A pinned row MUST install the artifact at its origin, and only
    /// [`ServiceBuilder::start_with_snapshots`](crate::ServiceBuilder::start_with_snapshots)
    /// carries the install capability (`S: SnapshotStateMachine`). A plain
    /// `start()` on a pinned row would replay the origin's prefix under THIS
    /// version instead — the §2.3 counterfactual — so it is refused.
    #[error(
        "FSM {name:?} at row {row} is pinned to origin {origin} but was \
         started with start(); a pinned row must install snap-{origin} and \
         needs start_with_snapshots()"
    )]
    PinRequiresSnapshots { name: String, row: u8, origin: u64 },
    /// The pin names an origin whose artifact is not on this node: the
    /// complete set at that instant was pruned, or this node never received
    /// it. There is no sound fallback (a different artifact is a different
    /// instant; genesis is the counterfactual), so this is a refusal.
    #[error(
        "row {row} is pinned to origin {origin} but {path} does not exist on \
         this node — the set at the origin was pruned or never fetched; take \
         `uc2ctl snapshot fetch` or re-pin at a retained instant"
    )]
    PinnedArtifactMissing { row: u8, origin: u64, path: String },
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
