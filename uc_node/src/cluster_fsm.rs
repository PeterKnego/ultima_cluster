// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The cluster FSM (cluster-FSM spec §4): one internal state machine owning
//! every piece of non-user cluster data — membership, the schedule table,
//! settings — changed only by `CLUSTER` commands on the log, applied at
//! commit by `cluster_agent`, snapshotted through `SnapshotStateMachine`
//! like any FSM. `apply` reads nothing but its own state and the command;
//! anything node-local is clamped at use by the reader of [`ClusterView`].

use std::io::{Read, Write};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU16, AtomicU32, AtomicU64, Ordering};

use uc_consensus::config::{ClusterConfig, ProposeError};
use uc_protocol::v2::catalog::{
    CLUSTER_ROW, MAX_CATALOG_SETS, MAX_RETAIN_SETS, RowEntry, RowVerdict, SetEntry, SetKind,
    SetState, decode_set_list, encode_set_list,
};
use uc_protocol::v2::cluster_image::{
    ClusterImageParts, decode_cluster_image, encode_cluster_image,
};
use uc_protocol::v2::cnc::CNC_MAX_SERVICES;
use uc_protocol::v2::config::{decode_config, encode_config};
use uc_protocol::v2::frame::{ClusterKind, read_cluster_prefix};
use uc_protocol::v2::schedule::{
    MAX_SCHEDULE_ENTRIES, ScheduleTable, decode_schedule_table, encode_schedule_table,
};
use uc_protocol::v2::settings::{
    FSM_LAG_LOCKSTEP, MIN_FSM_LAG_BYTES, Settings, decode_settings, encode_settings,
};
use uc_protocol::v2::upgrade::{
    RowGenesis, RowRunning, SnapshotReport, UpgradePin, decode_pin_list, decode_report_list,
    decode_row_genesis, decode_running_list, decode_snapshot_report, decode_upgrade_pin,
    encode_pin_list, encode_report_list, encode_row_genesis, encode_running_list,
    encode_snapshot_report, encode_upgrade_pin,
};
use uc_service::{ApplyCtx, RawStateMachine, SnapshotError, SnapshotStateMachine};

use crate::node::{cluster_to_wire, wire_to_cluster_config};

/// Plan 3 (spec §4.8) moved the image codec itself to
/// `uc_protocol::v2::cluster_image` — a `core`-friendly leaf a fuzz target
/// can reach without `uc_node` — so a fuzz target can exercise the decoder
/// directly; re-exported here under their original names since nothing in
/// this crate's public surface should have to change to follow the move. See
/// [`uc_protocol::v2::cluster_image::CLUSTER_IMAGE_VERSION`] for the layout
/// history: the image is now version `4` (#33 added the trailing per-row
/// `running` blob in v3; the snapshot catalog added the trailing `catalog`
/// blob in v4), and versions `1`–`3` are still read — a pre-#33 image
/// migrates each pinned row's running version from its newest pin
/// (`install_snapshot`, #33 spec §5.3), and a pre-catalog image installs
/// with an EMPTY catalog (catalog spec §4.5's `Empty` state).
pub use uc_protocol::v2::cluster_image::{CLUSTER_IMAGE_MAGIC, CLUSTER_IMAGE_VERSION};

/// The staged table file an admin client writes under the instance directory
/// before sending `ADMIN_OP_SCHEDULE_APPLY`. Relative to `<instance_dir>`,
/// NOT to `state/` — it is a request payload, not durable node state, and the
/// node deletes it once the command is appended.
pub const SCHEDULE_PENDING_FILE: &str = "schedules.pending";
/// The same, for `ADMIN_OP_SETTINGS_APPLY` (spec §6).
pub const SETTINGS_PENDING_FILE: &str = "settings.pending";
/// The same, for `ADMIN_OP_UPGRADE_PIN` (spec §2.5, plan B1).
pub const UPGRADE_PENDING_FILE: &str = "upgrade.pending";

/// Spec §2.5: "a small bounded per-row history … a handful of entries".
pub const MAX_PINS_PER_ROW: usize = 4;

/// The first TEN bytes of SHA-256 over `bytes`, read little-endian as an
/// admin request's `(id, ip, port)` fields — 80 bits of collision resistance
/// against an operator staging one file and signing another, which is all
/// those three fields have room for.
///
/// FROZEN: `uc2ctl` computes this over the file it stages and the node
/// recomputes it over the file it read. Changing the byte selection or the
/// endianness makes every apply refuse with
/// [`crate::node::REASON_SCHEDULE_DIGEST`] (or its settings twin).
///
/// Shared by both staged-file ops — the digest is a property of the FILE, not
/// of what is in it. It lives here rather than in `uc_protocol`: that crate is
/// a `core`-friendly, dependency-light leaf with no `sha2`.
pub fn staged_digest(bytes: &[u8]) -> (u32, u32, u16) {
    use sha2::{Digest, Sha256};
    let h = Sha256::digest(bytes);
    (
        u32::from_le_bytes(h[0..4].try_into().expect("4 bytes")),
        u32::from_le_bytes(h[4..8].try_into().expect("4 bytes")),
        u16::from_le_bytes(h[8..10].try_into().expect("2 bytes")),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterState {
    pub membership: ClusterConfig,
    pub table: ScheduleTable,
    /// Frame-END position of the command that installed `table`; 0 = none.
    pub table_position: u64,
    pub settings: Settings,
    /// Frame-END position of the command that installed `settings`; 0 = none
    /// (the genesis record, which came from `[settings]` in `node.toml` and
    /// never crossed the log). `table_position`'s twin, and exported as
    /// `uc2_settings_position` (spec §9). Like the table's, it is a property
    /// of the STATE — it rides the image, so a restarted node and a joiner
    /// installing the artifact both report the position the settings
    /// actually came from rather than 0.
    pub settings_position: u64,
    /// Frame-END position this FSM has CONSUMED the log up to — and
    /// therefore the view's position tag and the artifact's position. It
    /// advances on two things: a CLUSTER command applied here (accepted or
    /// refused alike), and the apply loop's cursor after a batch, via
    /// [`ClusterFsm::set_consumed`]. Task 4's brief: "the follower's cursor
    /// after a batch is also a frame-end; `applied` is that cursor".
    ///
    /// It has to be the cursor, not just the last command. This position tags
    /// the artifact, and the node's purge floor is bounded BY the artifact
    /// (`maybe_persist_snapshot_floor`). CLUSTER frames are operator actions —
    /// a cluster can run for days without one — while the rows' snapshot floor
    /// climbs with ordinary traffic, so an `applied` that only moved on CLUSTER
    /// frames would pin the purge floor at the last reconfiguration forever and
    /// leave the cluster artifact permanently below the set a joiner needs.
    ///
    /// The two are consistent because the cursor is only ever advanced over
    /// frames actually WALKED: since Ruling R18 an overrun replays the gap from
    /// the journal rather than skipping it, so a position recorded here is
    /// always one whose every CLUSTER frame this FSM has seen.
    pub applied: u64,
    /// FSM upgrade lifecycle (spec §2.5): every accepted `UpgradePin`, in
    /// apply order, at most [`MAX_PINS_PER_ROW`] per row (the oldest for
    /// that row is dropped). Rides the image, so a below-floor joiner holds
    /// the pin BEFORE its service attaches (§3 S4).
    pub pins: Vec<UpgradePin>,
    /// Spec §6.5.2: the newest `SnapshotReport` per row — the observed
    /// `(node, hash)` vector, never the verdict, which is
    /// `uc_protocol::v2::upgrade::verdict`'s to recompute.
    ///
    /// Every report held here arrived through `decode_snapshot_report` (from
    /// an applied kind-5 frame, or from the image's report blob at install),
    /// so each one satisfies that decoder's shape — `1 ≤ count ≤ MAX_MEMBERS`,
    /// node ids strictly increasing — and therefore RE-ENCODES. `query` and
    /// `freeze` both rely on that: neither can refuse a record it is only
    /// serialising.
    pub reports: Vec<SnapshotReport>,
    /// #33 spec §4.1: per row, the version it RUNS and the frame-END of the
    /// last accepted record that set it (genesis or pin). `None` = no record
    /// yet — distinct from `Some(version: 0)`, a recorded unversioned FSM.
    /// The `row` field inside an entry always equals its index.
    pub running: [Option<RowRunning>; CNC_MAX_SERVICES],
    /// Catalog spec §4: the replicated snapshot catalog — one [`SetEntry`]
    /// per coordinated instant still listed, OLDEST FIRST (strictly
    /// increasing `position`). Fed by the `SNAPSHOT` frame
    /// ([`Self::on_snapshot_frame`], a *commanded* entry, D3) and by
    /// `SnapshotReport`s (per-row verdicts; row [`CLUSTER_ROW`] fills
    /// `cluster`), and trimmed by [`Self::retire`]. Empty at genesis and
    /// after installing a v1–v3 image: the `Empty` state (§4.5).
    pub catalog: Vec<SetEntry>,
}

impl ClusterState {
    /// The genesis state: a membership and a settings record, an empty
    /// schedule table, and nothing applied — what a node seeds the FSM with
    /// on a fresh instance directory (`node.toml`'s `[services]` members and
    /// `[settings]`), and what every offline reader hands
    /// [`crate::cluster_agent::recover`] before it overwrites it with the
    /// newest artifact. Both positions are `0`: neither record crossed the
    /// log.
    ///
    /// Catalog spec errata: genesis seeds `retain_sets = 1` (newest-only)
    /// when the seed record leaves it unset (`0`) — the door refuses `0`, so
    /// a genesis state must not hold one either.
    pub fn genesis(membership: ClusterConfig, mut settings: Settings) -> ClusterState {
        if settings.retain_sets == 0 {
            settings.retain_sets = 1;
        }
        ClusterState {
            membership,
            table: ScheduleTable {
                entries: Vec::new(),
            },
            table_position: 0,
            settings,
            settings_position: 0,
            applied: 0,
            pins: Vec::new(),
            reports: Vec::new(),
            running: [None; CNC_MAX_SERVICES],
            catalog: Vec::new(),
        }
    }

    /// #33: the row's running version, if one has been recorded. An
    /// out-of-range row reads as `None`.
    pub fn running_for(&self, row: u8) -> Option<RowRunning> {
        self.running.get(row as usize).copied().flatten()
    }

    /// The row's newest pin, if any.
    pub fn pin_for(&self, row: u8) -> Option<&UpgradePin> {
        self.pins.iter().rev().find(|p| p.row == row)
    }

    /// The row's held report, if any — one per row (spec §6.5.2).
    pub fn report_for(&self, row: u8) -> Option<&SnapshotReport> {
        self.reports.iter().find(|r| r.row == row)
    }

    /// Record an accepted pin, dropping that row's OLDEST entry once the row
    /// holds more than [`MAX_PINS_PER_ROW`]. Bounding per row rather than
    /// over the whole `Vec` keeps one busy row from evicting another's
    /// history — and keeps the image's pin blob bounded either way.
    fn push_pin(&mut self, p: UpgradePin) {
        self.pins.push(p);
        if self.pins.iter().filter(|q| q.row == p.row).count() > MAX_PINS_PER_ROW {
            let oldest = self
                .pins
                .iter()
                .position(|q| q.row == p.row)
                .expect("just pushed one");
            self.pins.remove(oldest);
        }
    }

    /// Hold `r` as the row's report, replacing whatever that row held, and
    /// fold it into the catalog entry at `r.position` (catalog spec §4.2).
    ///
    /// The per-row `reports` list is the diagnostic matrix `uc2ctl upgrade
    /// show` reads, unchanged for rows `< CNC_MAX_SERVICES`; a report for the
    /// cluster row ([`CLUSTER_ROW`]) does NOT enter it — it exists only to
    /// fill a catalog entry's `cluster` field.
    ///
    /// Catalog ruling R25: when the report turns an entry `Complete` (agreed or
    /// not), every OLDER `Commanded` entry that is not a pinned origin is
    /// dropped. A `Commanded` entry older than a completed one can only be
    /// an instant that will never complete — typically a pre-flag-day
    /// `SNAPSHOT` frame one node replayed (its newest v3 artifact was older)
    /// and another did not; keeping it until an AGREED set passes it would
    /// diverge the cluster row for as long as retention takes. The cost: a
    /// stalled instant stays visible only until the NEXT set completes.
    fn put_report(&mut self, r: SnapshotReport) {
        let was_complete = self
            .catalog
            .iter()
            .any(|e| e.position == r.position && e.state == SetState::Complete);
        self.fold_into_catalog(&r);
        let now_complete = self
            .catalog
            .iter()
            .any(|e| e.position == r.position && e.state == SetState::Complete);
        if now_complete && !was_complete {
            let pinned = self.pinned_origins();
            self.catalog.retain(|e| {
                e.position >= r.position
                    || e.state != SetState::Commanded
                    || pinned.contains(&e.position)
            });
        }
        if r.row != CLUSTER_ROW {
            match self.reports.iter().position(|q| q.row == r.row) {
                Some(i) => self.reports[i] = r,
                None => self.reports.push(r),
            }
        }
    }

    /// Catalog spec §4.2: set `rows[r.row]` (or `cluster`) on the entry at
    /// `r.position` from today's `verdict()`. A report whose position has no
    /// entry (it was retired, or never commanded on this log) and a report
    /// for an UNDECLARED row are ignored. When every row declared AT THIS
    /// REPORT and the cluster artifact are reported the entry becomes
    /// `Complete` — the one place the live [`Self::declared_mask`] is
    /// consulted; agreement is then frozen in the entry (ruling R9,
    /// [`SetEntry::is_agreed`]). When it is agreed, retention runs (§4.4).
    fn fold_into_catalog(&mut self, r: &SnapshotReport) {
        let v = uc_protocol::v2::upgrade::verdict(r);
        let verdict = if v.agreed {
            RowVerdict::Agreed
        } else if v.majority_hash.is_some() {
            RowVerdict::Diverged
        } else {
            RowVerdict::NoMajority
        };
        let entry = RowEntry {
            version: self.version_at(r.row, r.position),
            hash: v.majority_hash.unwrap_or(0),
            verdict,
            // Snapshot-lifecycle spec §7.2 / plan ruling P7.
            size: uc_protocol::v2::upgrade::majority_size(r, &v),
        };
        let declared = self.declared_mask();
        let Some(e) = self.catalog.iter_mut().find(|e| e.position == r.position) else {
            return;
        };
        if r.row == CLUSTER_ROW {
            e.cluster = entry;
        } else if (r.row as usize) < CNC_MAX_SERVICES && declared & (1 << r.row) != 0 {
            e.rows[r.row as usize] = entry;
        } else {
            return; // undeclared row: not required, not recorded
        }
        let complete = e.cluster.verdict != RowVerdict::Unreported
            && (0..CNC_MAX_SERVICES)
                .all(|i| declared & (1 << i) == 0 || e.rows[i].verdict != RowVerdict::Unreported);
        if complete && e.state == SetState::Commanded {
            e.state = SetState::Complete;
        }
        if e.is_agreed() {
            self.retire();
        }
    }

    /// The FSM version IN FORCE STRICTLY BELOW `p` for `row` — the version
    /// that built the row's artifact at `p`, which the catalog records
    /// (ledger ruling R4). A pin at origin P commits AFTER P, so a report
    /// for P can land after it; the running version at report-apply time
    /// would then name the pin's `to` for an artifact its `from` built.
    ///
    /// Derived from the pin history, oldest → newest: the EARLIEST pin with
    /// `origin >= p` names its `from` (that pin's install had not taken
    /// effect at `p`); otherwise the LATEST pin's `to`; otherwise the row's
    /// running version (`0` with none recorded). The cluster row is `0` —
    /// its artifact is versioned by the image layout, not an FSM version.
    ///
    /// The history is bounded at [`MAX_PINS_PER_ROW`] per row, so a set
    /// older than four pins back reads the oldest RETAINED pin's `from`,
    /// which may not be the version that built it.
    pub fn version_at(&self, row: u8, p: u64) -> u32 {
        if row == CLUSTER_ROW {
            return 0;
        }
        let mut row_pins = self.pins.iter().filter(|q| q.row == row);
        if let Some(q) = row_pins.clone().find(|q| q.origin >= p) {
            return q.from;
        }
        if let Some(q) = row_pins.next_back() {
            return q.to;
        }
        self.running_for(row).map_or(0, |r| r.version)
    }

    /// Catalog spec §4.4: how many AGREED sets the cluster keeps. Never `0`
    /// in the state (catalog ruling R24: a v1/v2 record decodes as `1`, and genesis,
    /// apply and `install_snapshot` all normalise a `0` to `1`); the `max`
    /// is a last guard, not a reading anyone relies on.
    pub fn retain_sets(&self) -> u16 {
        self.settings.retain_sets.max(1)
    }

    /// The rows a set must agree on: bit `r` ⇔ row `r` has a recorded
    /// running version (catalog spec errata — the FSM does not hold
    /// `[services] names`; since #33 every declared row has a committed
    /// running version, genesis or pin, before it serves).
    pub fn declared_mask(&self) -> u64 {
        self.running
            .iter()
            .enumerate()
            .filter(|(_, r)| r.is_some())
            .fold(0, |m, (i, _)| m | (1 << i))
    }

    /// D3: a `SNAPSHOT` frame ending at `end` records a *commanded* set.
    /// Idempotent on `end` (a replayed frame adds nothing). Deterministic:
    /// every input is on the frame (`end`, its standby flag, its log time).
    pub fn on_snapshot_frame(&mut self, end: u64, standby: bool, time_ns: u64) {
        if self.catalog.iter().any(|e| e.position == end) {
            return;
        }
        let kind = if standby {
            SetKind::Standby
        } else {
            SetKind::Full
        };
        self.catalog.push(SetEntry::commanded(end, kind, time_ns));
        self.catalog.sort_by_key(|e| e.position);
        // A frame below the oldest kept agreed set is history on arrival.
        self.retire();
        self.cap_catalog();
    }

    /// Every row's newest pin origin whose pin is not yet COMPLETE — the
    /// sets retention must keep (D5). Pin completion ruling C3: once the
    /// catalog lists a completion set (an agreed set the pin's `to` line
    /// built above the pin record, [`crate::catalog::pin_complete`]) the
    /// origin is an ordinary agreed set, counted toward `retain_sets` and
    /// retired like any other. Deterministic: a pure function of the
    /// replicated pins, running records and catalog.
    fn pinned_origins(&self) -> Vec<u64> {
        (0..CNC_MAX_SERVICES as u8)
            .filter_map(|row| {
                let gate = crate::catalog::pin_gate(&self.pins, &self.running, row)?;
                (!crate::catalog::pin_complete(&self.catalog, row, &gate)).then_some(gate.origin)
            })
            .collect()
    }

    /// Catalog spec §4.4 (as amended, rulings R3, R9 and R21): keep the
    /// newest `retain_sets` agreed sets, PLUS every pinned origin. Let `U`
    /// be the agreed entries that are NOT pinned origins (any row's newest
    /// pin), in position order — a pinned origin is never counted toward
    /// `retain_sets` and is never the victim. While `|U| > retain_sets()`,
    /// remove the oldest entry of `U` that is not the newest agreed set;
    /// stop when none remains. Then drop every entry (any state) older than
    /// the oldest remaining agreed set — except a pinned origin, which is
    /// never dropped here (belt-and-braces for D5). The newest agreed set is
    /// never removed — it is the cluster floor.
    fn retire(&mut self) {
        let pinned = self.pinned_origins();
        let retain = self.retain_sets() as usize;
        loop {
            let newest = self.catalog.iter().rposition(SetEntry::is_agreed);
            let unpinned: Vec<usize> = (0..self.catalog.len())
                .filter(|&i| {
                    self.catalog[i].is_agreed() && !pinned.contains(&self.catalog[i].position)
                })
                .collect();
            if unpinned.len() <= retain {
                break;
            }
            let Some(&victim) = unpinned.iter().find(|&&i| Some(i) != newest) else {
                break;
            };
            self.catalog.remove(victim);
        }
        if let Some(floor) = self
            .catalog
            .iter()
            .find(|e| e.is_agreed())
            .map(|e| e.position)
        {
            self.catalog
                .retain(|e| e.position >= floor || pinned.contains(&e.position));
        }
    }

    /// Bound the list at [`MAX_CATALOG_SETS`] (the image's set-list limit),
    /// ruling R8. The NEWEST entry by position (the instant just commanded)
    /// is never evicted — evicting it would drop every later instant on
    /// arrival and freeze the floor. Evict the oldest non-agreed entry
    /// first; if the list is still over, the oldest agreed entry that is
    /// neither a pinned origin nor the newest agreed set. With
    /// `retain_sets <= MAX_RETAIN_SETS` the second step is a backstop that
    /// [`Self::retire`] normally makes unreachable.
    fn cap_catalog(&mut self) {
        let pinned = self.pinned_origins();
        while self.catalog.len() > MAX_CATALOG_SETS {
            let last = self.catalog.len() - 1;
            let newest_agreed = self.catalog.iter().rposition(|e| e.is_agreed());
            let victim = self.catalog[..last]
                .iter()
                .position(|e| !e.is_agreed())
                .or_else(|| {
                    (0..last).find(|&i| {
                        Some(i) != newest_agreed && !pinned.contains(&self.catalog[i].position)
                    })
                });
            let Some(i) = victim else {
                break;
            };
            self.catalog.remove(i);
        }
    }

    /// The newest AGREED set at or below `at_most`, if any.
    pub fn newest_agreed_at_most(&self, at_most: u64) -> Option<u64> {
        self.catalog
            .iter()
            .rev()
            .find(|e| e.position <= at_most && e.is_agreed())
            .map(|e| e.position)
    }

    /// Catalog spec §4.5 as amended by catalog ruling R26: the `Empty`
    /// state — no listed entry has reached `Complete`, so every reader takes
    /// today's fallback. NOT "no agreed set": a catalog whose complete sets
    /// all diverged is not `Empty` (the floor then stays where the last
    /// agreed set put it; D4 — a diverged set is never the floor).
    pub fn catalog_empty(&self) -> bool {
        !self.catalog.iter().any(|e| e.state == SetState::Complete)
    }

    /// [`Self::genesis`] with an EMPTY membership and the default settings —
    /// the seed for a reader that only wants what the artifact holds
    /// (`install_snapshot` replaces every field, and never consults the
    /// declared hashes), and for test fixtures that need a published view.
    pub fn genesis_empty() -> ClusterState {
        ClusterState::genesis(
            ClusterConfig::genesis(Vec::new(), Vec::new()),
            Settings::genesis_default(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterCommand {
    Membership(ClusterConfig),
    ScheduleTable(ScheduleTable),
    Settings(Settings),
    UpgradePin(UpgradePin),
    SnapshotReport(SnapshotReport),
    /// #33 spec §4.1 / §6.1: the leader's own attached version for a row
    /// with no running version yet — a recorded fact, never a change.
    RowGenesis(RowGenesis),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterRefusal {
    Membership(ProposeError),
    ScheduleUnknownFsm {
        entry: usize,
    },
    ScheduleTooLarge,
    SettingsBounds(&'static str),
    PinFromMismatch,
    PinNotMonotone,
    ReportStale,
    /// #33: a `RowGenesis` for a row that already has a running version.
    VersionAlreadySet,
}

impl ClusterRefusal {
    /// The same numbers the admin plane already speaks (uc2ctl.md's table).
    ///
    /// The pin/report codes are the 52–59 band, plan B1; 52/54/56–58 are
    /// door-only and live in `uc_node::node`, so they never appear here.
    /// 60 (#33) is `version_already_set`, a `RowGenesis` for a row that
    /// already runs a recorded version.
    pub fn reason_code(&self) -> u32 {
        match self {
            ClusterRefusal::Membership(e) => ClusterConfig::reason_code(e),
            ClusterRefusal::ScheduleUnknownFsm { .. } => 43,
            ClusterRefusal::ScheduleTooLarge => 42,
            ClusterRefusal::SettingsBounds(_) => 47,
            ClusterRefusal::PinFromMismatch => 53,
            ClusterRefusal::PinNotMonotone => 55,
            ClusterRefusal::ReportStale => 59,
            ClusterRefusal::VersionAlreadySet => 60,
        }
    }
}

pub struct ClusterFsm {
    state: ClusterState,
    /// The declared rows' identity hashes, from `[services] names` — the
    /// only node-local input, fixed at boot.
    ///
    /// Read by [`ClusterFsm::validate`] and NOT by
    /// [`ClusterFsm::validate_replicated`], so it never decides what a
    /// committed command does (Ruling R24). Nothing enforces that two hosts'
    /// lists agree in steady state — the positional identity comparison on
    /// `SNAP_BEGIN` fires on the joiner path only — which is precisely why
    /// `apply` must not consult it.
    declared_hashes: Vec<u64>,
}

impl ClusterFsm {
    pub fn new(genesis: ClusterState, declared_hashes: Vec<u64>) -> ClusterFsm {
        ClusterFsm {
            state: genesis,
            declared_hashes,
        }
    }
    /// Advance the consumed position to `position` — the apply loop's cursor
    /// after a batch, which is a frame-END like every command's. Monotone: a
    /// lower value is ignored, so this can never walk the artifact's tag (or
    /// the view's position tag) backwards. Changes no cluster STATE, which is
    /// why the agent does not publish the view for it.
    pub fn set_consumed(&mut self, position: u64) {
        if position > self.state.applied {
            self.state.applied = position;
        }
    }

    pub fn state(&self) -> &ClusterState {
        &self.state
    }

    /// D3: see [`ClusterState::on_snapshot_frame`]. The cluster agent calls
    /// this for every `SNAPSHOT` frame it walks (there is no `state_mut`).
    pub fn on_snapshot_frame(&mut self, end: u64, standby: bool, time_ns: u64) {
        self.state.on_snapshot_frame(end, standby, time_ns);
    }

    /// The recorded running versions in row order — the shape query 6 and
    /// the image's `running` blob both carry.
    fn running_list(&self) -> Vec<RowRunning> {
        self.state.running.iter().flatten().copied().collect()
    }

    /// The LEADER's pre-append acceptance decision on THIS state, no
    /// mutation — [`Self::validate_replicated`] plus the one check that reads
    /// node-local input (the declared-hash set). The leader's pre-append
    /// check calls exactly this on a clone (spec §4.4).
    ///
    /// It is deliberately NOT what `apply` runs: see
    /// [`Self::validate_replicated`] for why the two differ, and by exactly
    /// how much.
    pub fn validate(&self, cmd: &ClusterCommand) -> Result<(), ClusterRefusal> {
        self.validate_replicated(cmd)?;
        // Ruling R10, the DOOR half: no operator may apply a `0`.
        // `validate_replicated` accepts one (it refuses only the upper
        // bound), and `apply` reads it as `1` from the record alone
        // (catalog ruling R24; a pre-catalog record already decodes as `1`).
        if let ClusterCommand::Settings(s) = cmd
            && s.retain_sets == 0
        {
            return Err(ClusterRefusal::SettingsBounds("retain_sets"));
        }
        // NODE-LOCAL, leader-only (Ruling R24). `declared_hashes` is this
        // host's `[services] names`; running it inside `apply` would let two
        // nodes with different lists reach opposite verdicts on the same
        // command at the same position. Here it decides only whether an
        // operator's request is appended AT ALL, so every replica still sees
        // one command with one outcome.
        if let ClusterCommand::ScheduleTable(t) = cmd {
            for (i, e) in t.entries.iter().enumerate() {
                if !self.declared_hashes.contains(&e.identity_hash) {
                    return Err(ClusterRefusal::ScheduleUnknownFsm { entry: i });
                }
            }
        }
        Ok(())
    }

    /// The REPLICATED half of the acceptance decision: FSM state and
    /// compile-time constants only, never this host. This is what `apply`
    /// runs, on every node, so its verdict is identical everywhere by
    /// construction — spec §4.4's rule verbatim ("validation that decides
    /// acceptance lives in `apply`, deterministically, on FSM state only").
    ///
    /// What it deliberately does NOT check is a table entry naming an FSM
    /// this node has not declared. That set is `[services] names`, node-local
    /// input: if two hosts' lists differed, one replica would ACCEPT a
    /// committed `ScheduleTable` and another REFUSE it, both would advance
    /// `applied` past it, and they would then hold different
    /// `table`/`table_position` at the same position and write divergent
    /// artifacts under the same tag — a joiner getting whichever shipper it
    /// reached. Nothing fail-stops it: the refusal path is `out.push(43)` and
    /// carry on. Adopting unconditionally instead costs nothing: a row this
    /// node does not declare simply never arms a timer, which is node-local
    /// and harmless.
    ///
    /// The check itself is not lost — it stays in the leader's pre-append
    /// [`Self::validate`], which `Consensus::apply_schedule_table` runs
    /// before appending, so an operator still gets `43 schedule_unknown_fsm`
    /// immediately and no unvalidated table reaches the log in the first
    /// place.
    pub fn validate_replicated(&self, cmd: &ClusterCommand) -> Result<(), ClusterRefusal> {
        match cmd {
            ClusterCommand::Membership(next) => {
                // Version chaining IS the one-in-flight rule: the next record
                // must be exactly adopted+1 — a stale or ahead version means
                // either a change is already pending or the proposal was
                // built off a superseded config.
                if next.version != self.state.membership.version + 1 {
                    return Err(ClusterRefusal::Membership(ProposeError::ChangePending));
                }
                Ok(())
            }
            ClusterCommand::ScheduleTable(t) => {
                if t.entries.len() > MAX_SCHEDULE_ENTRIES {
                    return Err(ClusterRefusal::ScheduleTooLarge);
                }
                Ok(())
            }
            ClusterCommand::Settings(s) => {
                // Jumbo spec §5.5: 0 (baseline) or a ladder rung, nothing else.
                if s.datagram_mtu != 0 && !uc_protocol::v2::datagram::is_rung(s.datagram_mtu) {
                    return Err(ClusterRefusal::SettingsBounds("datagram_mtu"));
                }
                if s.admission_bytes == u64::MAX {
                    return Err(ClusterRefusal::SettingsBounds("admission_bytes"));
                }
                if s.snapshot_interval_bytes == u64::MAX {
                    return Err(ClusterRefusal::SettingsBounds("snapshot.interval_bytes"));
                }
                // I1: the one settings value whose too-SMALL side is
                // unrecoverable. `report_ceiling` caps an attested frontier
                // at `min_applied + fsm_lag`; below one max-size frame that
                // ceiling can sit strictly inside the next frame forever, so
                // nothing applies, commit stops cluster-wide, and the only
                // channel that could change a replicated setting is a
                // `CLUSTER` frame that has to COMMIT. Everything else here is
                // clamped at the point of use (spec §4.4) and still is — the
                // clamp in `services::fsm_lag_from_setting` is two-sided —
                // but a value that would have bricked the cluster is worth an
                // operator being TOLD about rather than silently corrected.
                //
                // `MIN_FSM_LAG_BYTES` is a compile-time constant (the widest
                // frame the transport can carry on ANY host), never this
                // host's `max_payload`, so this verdict stays a function of
                // FSM state and constants alone. `0` ("derive this node's
                // boot value") and `FSM_LAG_LOCKSTEP` (`u64::MAX`) are
                // sentinels, not byte bounds, and keep their meanings.
                if s.fsm_lag_bytes != 0
                    && s.fsm_lag_bytes != FSM_LAG_LOCKSTEP
                    && s.fsm_lag_bytes < MIN_FSM_LAG_BYTES
                {
                    return Err(ClusterRefusal::SettingsBounds("fsm_lag"));
                }
                // Rulings R8/R10: above `MAX_RETAIN_SETS` the image could not
                // carry the list with headroom for commanded instants —
                // refused (47). `0` is NOT refused here: `apply` reads it as
                // `1` (catalog ruling R24); the leader's door (`validate`)
                // refuses it from an operator.
                if s.retain_sets > MAX_RETAIN_SETS {
                    return Err(ClusterRefusal::SettingsBounds("retain_sets"));
                }
                Ok(())
            }
            ClusterCommand::UpgradePin(p) => {
                // Spec §2.5, replicated half only: the row's history is FSM
                // state. `row_undeclared` (52), the no-running-version
                // half of `pin_from_mismatch` (53, against the attached
                // version WORD) and `pin_no_set` (54, this leader's
                // filesystem) are node-local and stay at the door
                // (`Consensus::apply_upgrade_pin`).
                if let Some(cur) = self.state.pin_for(p.row)
                    && p.origin <= cur.origin
                {
                    return Err(ClusterRefusal::PinNotMonotone);
                }
                // #33 spec §4.1: a pin starts from the row's RUNNING line
                // (patch ignored, `same_line`). This replaces the old exact
                // `from == previous pin's to` rule and covers it: an accepted
                // pin sets `running` to its `to`, so a row whose last record
                // was a pin is checked against that pin's line.
                if let Some(r) = self.state.running_for(p.row)
                    && !uc_protocol::identity::same_line(p.from, r.version)
                {
                    return Err(ClusterRefusal::PinFromMismatch);
                }
                Ok(())
            }
            ClusterCommand::SnapshotReport(r) => {
                if self
                    .state
                    .report_for(r.row)
                    .is_some_and(|held| r.position < held.position)
                {
                    return Err(ClusterRefusal::ReportStale);
                }
                Ok(())
            }
            ClusterCommand::RowGenesis(g) => {
                // #33 spec §4.1: genesis records a fact once and never
                // changes one; after it only a pin moves the version.
                if self.state.running_for(g.row).is_some() {
                    return Err(ClusterRefusal::VersionAlreadySet);
                }
                Ok(())
            }
        }
    }

    pub fn decode_command(kind: ClusterKind, payload: &[u8]) -> Option<ClusterCommand> {
        Some(match kind {
            ClusterKind::Membership => {
                ClusterCommand::Membership(wire_to_cluster_config(&decode_config(payload)?))
            }
            ClusterKind::ScheduleTable => {
                ClusterCommand::ScheduleTable(decode_schedule_table(payload)?)
            }
            ClusterKind::Settings => ClusterCommand::Settings(decode_settings(payload)?),
            ClusterKind::UpgradePin => ClusterCommand::UpgradePin(decode_upgrade_pin(payload)?),
            ClusterKind::SnapshotReport => {
                ClusterCommand::SnapshotReport(decode_snapshot_report(payload)?)
            }
            ClusterKind::RowGenesis => ClusterCommand::RowGenesis(decode_row_genesis(payload)?),
        })
    }

    /// Writes the PAYLOAD only; the caller writes the prefix.
    pub fn encode_command(cmd: &ClusterCommand, out: &mut Vec<u8>) -> ClusterKind {
        match cmd {
            ClusterCommand::Membership(c) => {
                // prev_position is the kernel's concern; the FSM carries 0
                // here and the leader's append path fills it from its own
                // record.
                encode_config(&cluster_to_wire(c, 0), out);
                ClusterKind::Membership
            }
            ClusterCommand::ScheduleTable(t) => {
                encode_schedule_table(t, out);
                ClusterKind::ScheduleTable
            }
            ClusterCommand::Settings(s) => {
                encode_settings(s, out);
                ClusterKind::Settings
            }
            ClusterCommand::UpgradePin(p) => {
                encode_upgrade_pin(p, out);
                ClusterKind::UpgradePin
            }
            ClusterCommand::SnapshotReport(r) => {
                encode_snapshot_report(r, out).expect(
                    "a SnapshotReport in FSM state or built by the leader is encodable: \
                     non-empty, <= MAX_MEMBERS, ids increasing",
                );
                ClusterKind::SnapshotReport
            }
            ClusterCommand::RowGenesis(g) => {
                encode_row_genesis(g, out);
                ClusterKind::RowGenesis
            }
        }
    }
}

impl RawStateMachine for ClusterFsm {
    const NAME: &'static str = "uc_cluster";
    const VERSION: u32 = 1;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        out.clear();
        self.state.applied = ctx.position;
        let Some((kind, payload)) = read_cluster_prefix(cmd) else {
            out.push(42); // undecodable: refused, applied still advances
            return;
        };
        let Some(command) = ClusterFsm::decode_command(kind, payload) else {
            out.push(42);
            return;
        };
        // The REPLICATED half only (Ruling R24): every node must reach this
        // verdict identically, so nothing node-local may enter it. The
        // leader's own pre-append check is the fuller `validate`.
        if let Err(r) = self.validate_replicated(&command) {
            out.push(r.reason_code() as u8);
            return;
        }
        match command {
            ClusterCommand::Membership(c) => self.state.membership = c,
            ClusterCommand::ScheduleTable(t) => {
                self.state.table = t;
                self.state.table_position = ctx.position;
            }
            ClusterCommand::Settings(s) => {
                // Jumbo spec §5.5: the rung is monotone in the FSM itself, so
                // an operator record (absent keys = 0) cannot lower it, and
                // every replica computes the same value.
                let keep = self.state.settings.datagram_mtu.max(s.datagram_mtu);
                // Catalog ruling R24 (supersedes R10's "keep current"): a replayed
                // pre-catalog record decodes with `retain_sets = 1` and wins
                // like every other replayed field. A `0` can only arrive on
                // a crafted v3 record (the leader's door refuses it); it is
                // read as `1` from the record ALONE — never from the current
                // state, which differs between a node that installed an old
                // image and one that walked from genesis.
                let mut s = s;
                if s.retain_sets == 0 {
                    s.retain_sets = 1;
                }
                self.state.settings = s;
                self.state.settings.datagram_mtu = keep;
                self.state.settings_position = ctx.position;
                // §4.2: a new `retain_sets` runs retention (lowering it
                // retires at this apply; raising it retires nothing).
                self.state.retire();
            }
            ClusterCommand::UpgradePin(p) => {
                self.state.push_pin(p);
                // #33 spec §4.1: a pin also sets the row's running version.
                self.state.running[p.row as usize] = Some(RowRunning {
                    row: p.row,
                    version: p.to,
                    record_pos: ctx.position,
                });
            }
            ClusterCommand::SnapshotReport(r) => self.state.put_report(r),
            ClusterCommand::RowGenesis(g) => {
                self.state.running[g.row as usize] = Some(RowRunning {
                    row: g.row,
                    version: g.version,
                    record_pos: ctx.position,
                });
            }
        }
        out.push(0);
    }

    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        out.clear();
        match q.first() {
            Some(1) => encode_config(&cluster_to_wire(&self.state.membership, 0), out),
            Some(2) => {
                out.extend_from_slice(&self.state.table_position.to_le_bytes());
                encode_schedule_table(&self.state.table, out);
            }
            Some(3) => encode_settings(&self.state.settings, out),
            Some(4) => encode_pin_list(&self.state.pins, out),
            Some(5) => {
                encode_report_list(&self.state.reports, out).expect("held reports are encodable");
            }
            // #33: the running list, one entry per recorded row, row order.
            Some(6) => encode_running_list(&self.running_list(), out),
            _ => {}
        }
    }

    fn last_applied(&self) -> Option<u64> {
        (self.state.applied > 0).then_some(self.state.applied)
    }
}

/// The frozen image: magic ‖ version u32 ‖ applied u64 ‖ table_position u64
/// ‖ settings_position u64 ‖ membership (u32 len ‖ encode_config) ‖ table
/// (u32 len ‖ encode_schedule_table) ‖ settings (one whole record — the
/// decoder accepts all four settings versions: v1 (`SETTINGS_LEN_V1`, 29
/// B), v2 (`SETTINGS_LEN_V2`, 33 B), v3 (`SETTINGS_LEN_V3`, 35 B) and v4
/// (`SETTINGS_LEN`, 36 B, the only one `freeze` writes); v1/v2 read
/// `retain_sets = 1` (catalog ruling R24), v1–v3 read `auto_fetch = true`) ‖
/// pins (u32 len ‖ `encode_pin_list`) ‖ reports (u32 len ‖
/// `encode_report_list`) ‖ running (u32 len ‖ `encode_running_list`, #33,
/// layout **v3**) ‖ catalog (u32 len ‖ `encode_set_list`, the snapshot
/// catalog, layout **v4**; empty for an `Empty` catalog) ‖ crc32 of
/// everything before it. The pin and report blobs are
/// image layout **v2** (plan B1): a v1 image — one a `2.12.0` node wrote
/// before this release, which a restarting node still reads off its own
/// disk — carries neither and installs with both histories EMPTY, the same
/// precedent the settings record's v1/v2 split set. The codec for this layout lives in
/// `uc_protocol::v2::cluster_image` (plan 3, spec §4.8) — this impl owns only
/// the state ⇄ `ClusterImageParts` conversion and the two membership/table
/// records' own codecs (`config`/`schedule`), which the leaf does not know
/// about.
pub type ClusterImage = Vec<u8>;

impl SnapshotStateMachine for ClusterFsm {
    type SnapshotHandle = ClusterImage;

    fn freeze(&self) -> Result<(ClusterImage, u64), SnapshotError> {
        let mut m = Vec::new();
        encode_config(&cluster_to_wire(&self.state.membership, 0), &mut m);
        let mut t = Vec::new();
        encode_schedule_table(&self.state.table, &mut t);
        let mut s = Vec::new();
        encode_settings(&self.state.settings, &mut s);
        let mut pins = Vec::new();
        encode_pin_list(&self.state.pins, &mut pins);
        let mut reports = Vec::new();
        encode_report_list(&self.state.reports, &mut reports)
            .ok_or_else(|| SnapshotError::Codec("cluster image: unencodable report".into()))?;
        let mut running = Vec::new();
        encode_running_list(&self.running_list(), &mut running);
        // An EMPTY catalog is an empty blob (not a zero-count list), exactly
        // as `running` is: the image of a cluster in the `Empty` state is
        // then a v3 image plus one zero length prefix.
        let mut catalog = Vec::new();
        if !self.state.catalog.is_empty() {
            encode_set_list(&self.state.catalog, &mut catalog).ok_or_else(|| {
                SnapshotError::Codec("cluster image: catalog exceeds MAX_CATALOG_SETS".into())
            })?;
        }
        let mut img = Vec::new();
        encode_cluster_image(
            &ClusterImageParts {
                applied: self.state.applied,
                table_position: self.state.table_position,
                settings_position: self.state.settings_position,
                membership: &m,
                table: &t,
                settings: &s,
                pins: &pins,
                reports: &reports,
                running: &running,
                catalog: &catalog,
            },
            &mut img,
        )
        .ok_or_else(|| {
            SnapshotError::Codec(
                "cluster image: membership or schedule table exceeds u32::MAX bytes".into(),
            )
        })?;
        Ok((img, self.state.applied))
    }

    fn stream_snapshot(handle: ClusterImage, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle).map_err(SnapshotError::from)
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn Read,
    ) -> Result<u64, SnapshotError> {
        let mut img = Vec::new();
        src.read_to_end(&mut img).map_err(SnapshotError::from)?;
        let bad = |what: &'static str| SnapshotError::Codec(what.into());
        // Total, CRC-checked, exact-framing decode of the outer image — see
        // `uc_protocol::v2::cluster_image::decode_cluster_image`'s doc for
        // the bounds-checking posture. Its `None` covers every wire-level
        // refusal at once (magic/version/crc/length-prefix/framing); the one
        // refusal this caller can name that the leaf cannot is the position
        // mismatch below, since the expected position is not part of the
        // wire image.
        let parts = decode_cluster_image(&img).ok_or_else(|| bad("cluster image"))?;
        // Snapshot-lifecycle spec §7.2: a v1–v4 image stored the report and
        // catalog blobs UNSIZED; they decode with every size 0 (unknown). The
        // catalog needs the version to pick its width; the reports do not.
        let sized = uc_protocol::v2::cluster_image::cluster_image_version(&img)
            .ok_or_else(|| bad("cluster image"))?
            >= 5;
        if parts.applied != position {
            return Err(bad("cluster image position"));
        }
        let membership = wire_to_cluster_config(
            &decode_config(parts.membership).ok_or_else(|| bad("cluster image membership"))?,
        );
        let table = decode_schedule_table(parts.table).ok_or_else(|| bad("cluster image table"))?;
        let mut settings =
            decode_settings(parts.settings).ok_or_else(|| bad("cluster image settings"))?;
        // Catalog ruling R24, belt and braces: a v1/v2 settings tail already decodes
        // as `1`; any `0` that still arrives is normalised the same way, so
        // an installed image and a genesis walk hold the same bytes.
        if settings.retain_sets == 0 {
            settings.retain_sets = 1;
        }
        // Empty for a v1 image (the leaf hands both back as empty slices),
        // which decodes to an empty history rather than a refusal.
        //
        // NOT re-bounded here, deliberately. The list decoders enforce each
        // RECORD's shape, but nothing below re-checks the collection
        // invariants `apply` maintains — at most `MAX_PINS_PER_ROW` pins per
        // row (`push_pin`), and one report per row (`report_for`'s "the
        // newest"). An image is not arbitrary input in the way a frame is:
        // it is written by `freeze` from a state that held those bounds, and
        // a corrupt or crafted one is already refused by the outer CRC and
        // exact framing. What a hostile image could buy is a longer list,
        // not an unsound state: every reader of `pins`/`reports` is a scan.
        //
        // One consequence worth naming: `pin_for` returns the LAST matching
        // entry, and "last = newest" is a property of the ORDER, not of any
        // field in the record. That order is apply order, which is
        // `origin` order (the FSM refuses a non-monotone `origin`, reason
        // 55), and `freeze` writes the list in that same order — so every
        // replica installing these bytes agrees on which pin is newest for
        // exactly the reason it agrees on everything else here: it is
        // reading the same bytes in the same order.
        let pins = decode_pin_list(parts.pins).ok_or_else(|| bad("cluster image pins"))?;
        // Ruling R2: `decode_snapshot_report` infers the entry width, so a
        // v1–v4 image's unsized blob reads with every size 0 here exactly as
        // a replayed pre-lifecycle kind-5 record does in `apply`.
        let reports =
            decode_report_list(parts.reports).ok_or_else(|| bad("cluster image reports"))?;
        let mut running = [None; CNC_MAX_SERVICES];
        if parts.running.is_empty() {
            // v1/v2 image (#33 spec §5.3): a pinned row runs its newest pin's
            // `to`; the pin's own record position is not in the image, so use
            // `applied` — at or above it, which is all attach needs. A row
            // with no pin stays `None`, for the leader's genesis to fill. A
            // v3 image with no rows recorded also has an empty blob and lands
            // here too, harmlessly: such an image also holds no pins.
            for (row, slot) in running.iter_mut().enumerate() {
                let row = row as u8;
                if let Some(p) = pins.iter().rev().find(|p| p.row == row) {
                    *slot = Some(RowRunning {
                        row,
                        version: p.to,
                        record_pos: parts.applied,
                    });
                }
            }
        } else {
            // The decoder refuses an out-of-range or repeated row, so
            // indexing by `r.row` is in bounds and never overwrites.
            for r in
                decode_running_list(parts.running).ok_or_else(|| bad("cluster image running"))?
            {
                running[r.row as usize] = Some(r);
            }
        }
        // Empty for a v1–v3 image: the catalog's `Empty` state (§4.5); a v4
        // image's catalog is unsized (every size 0, see `sized` above).
        let catalog = if parts.catalog.is_empty() {
            Vec::new()
        } else if sized {
            decode_set_list(parts.catalog).ok_or_else(|| bad("cluster image: catalog"))?
        } else {
            uc_protocol::v2::catalog::decode_set_list_unsized(parts.catalog)
                .ok_or_else(|| bad("cluster image: catalog"))?
        };
        // The list is keyed by position, oldest first; `retire` and every
        // reader rely on that order, so an image that breaks it is refused
        // rather than installed (and the state is left untouched).
        if catalog.windows(2).any(|w| w[0].position >= w[1].position) {
            return Err(bad("cluster image: catalog out of order"));
        }
        self.state = ClusterState {
            membership,
            table,
            table_position: parts.table_position,
            settings,
            settings_position: parts.settings_position,
            applied: parts.applied,
            pins,
            reports,
            running,
            catalog,
        };
        Ok(parts.applied)
    }
}

/// The position-tagged view the consensus agent reads (spec §4.5). Scalars
/// are atomics so the per-pass reads are one load each; the structured parts
/// sit behind a mutex taken only when `position` changed.
///
/// Two readers, two costs. The **consensus pass** touches only the atomics —
/// one load each, never the mutex, which is the whole point of the split.
/// The **`/metrics` scrape** is no longer lock-free: since the pin words and
/// the snapshot-hash mismatch gauge it takes `inner` once per scrape to read
/// `pins`/`reports`. That is a scrape-time cost on the HTTP thread, off the
/// hot path entirely; the only writer it can contend with is the
/// `uc2-cluster` agent, which takes the lock only on an APPLIED `CLUSTER`
/// frame — rare by construction, since cluster commands are
/// single-in-flight.
pub struct ClusterView {
    pub position: AtomicU64,
    /// Plan B3 T5: the agent's WALK cursor — [`ClusterState::applied`] as of
    /// the end of its last duty cycle, whether or not anything applied.
    ///
    /// Deliberately NOT [`Self::position`], which is the published VIEW's tag
    /// and therefore moves only on a pass that applied a `CLUSTER` frame or
    /// installed an artifact (see [`Self::publish`] and the `set_consumed`
    /// comment in `cluster_agent::do_work`). On a cluster that commits
    /// ordinary traffic and no cluster commands — the normal case — `position`
    /// sits still while commit climbs, so "has the cluster FSM caught up with
    /// commit?" cannot be asked of it. It can be asked of this word.
    ///
    /// One `fetch_max` per cluster-agent duty cycle writes it and nothing on
    /// the consensus hot path reads it in steady state (the declared-set gate
    /// reads it only while it is still closed, once per incarnation), so it
    /// costs neither loop the mutex `position` would have cost them.
    pub consumed: AtomicU64,
    /// Spec §9: `uc2_settings_position`, the frame-END of the last Settings
    /// command applied (0 = the genesis record). An atomic beside the five
    /// settings scalars, for the same reason they are: `/metrics` reads it
    /// at SCRAPE time with one load and no lock, so nothing about this gauge
    /// costs the consensus pass anything.
    pub settings_position: AtomicU64,
    pub admission_bytes: AtomicU64,
    pub fsm_lag_bytes: AtomicU64,
    pub snapshot_interval_bytes: AtomicU64,
    pub snapshot_target: AtomicU8,
    /// Jumbo spec §5.5: the committed datagram rung (`0` = baseline). Beside
    /// the other settings scalars for the same reason — the sender agent
    /// reads it per pass with one load and no lock.
    pub datagram_mtu: AtomicU32,
    /// #33: bit `r` set ⇔ row `r` has a running version. Stored by
    /// [`Self::publish`] AFTER the structured parts (so a reader that sees a
    /// bit and then locks finds the row's entry) and BEFORE `position`. One
    /// load answers "does this row have a version yet?" without the lock —
    /// the genesis trigger's question (spec §6.1).
    pub versioned: AtomicU8,
    /// Catalog spec §4.4: the committed `retain_sets`, as
    /// [`ClusterState::retain_sets`] reads it (so never `0`). A settings
    /// scalar like its siblings above, and here for the same reason they
    /// are: [`Self::to_state`] rebuilds the settings record from the
    /// atomics, and a leader that re-proposes the committed record (the
    /// jumbo rung raise) must re-propose a `retain_sets` the door accepts.
    pub retain_sets: AtomicU16,
    /// Snapshot-lifecycle spec §6: the committed `auto_fetch` switch — read
    /// by the consensus agent once per pass (one load, no lock), and by
    /// [`Self::to_state`], so a re-proposed record carries it unchanged.
    pub auto_fetch: AtomicBool,
    /// Catalog spec §4.5: the newest AGREED set's position; `0` = nothing
    /// agreed (the cluster floor moves nothing). NOT the `Empty` test since
    /// catalog ruling R26 — that is [`Self::catalog_has_complete`].
    pub catalog_agreed_position: AtomicU64,
    /// Catalog ruling R26: `true` ⇔ some listed entry is `Complete` (agreed
    /// or not) — [`ClusterState::catalog_empty`] negated. `false` is the
    /// `Empty` state, in which the node's floor falls back to its own newest
    /// complete set; `uc2_catalog_empty` reads it, and so does the node.
    pub catalog_has_complete: AtomicBool,
    /// How many sets the catalog lists (any state).
    pub catalog_len: AtomicU64,
    /// How many listed sets are still `Commanded` (catalog spec errata: no
    /// timeout here — the timeout judgement is the `stalled()` query's).
    pub catalog_stalled: AtomicU64,
    /// How many row entries (the cluster artifact's included) across the
    /// listed sets read `Diverged` or `NoMajority`.
    pub catalog_diverged: AtomicU64,
    /// Snapshot-lifecycle spec §7.4: the newest AGREED set's total size
    /// ([`SetEntry::total_size`]); `0` when unknown or none —
    /// `uc2_snapshot_newest_agreed_bytes`, and the auto-fetch space check's
    /// input.
    pub catalog_newest_agreed_bytes: AtomicU64,
    /// Catalog ruling R16: a CONTENT hash of the listed sets
    /// ([`catalog_version_of`]) — the stamp a node's `Holdings.sets_held`
    /// is computed against, and the one a leader's query compares. Equal on
    /// every node whose catalog is equal, whatever its walk cursor.
    pub catalog_version: AtomicU64,
    inner: Mutex<ClusterViewInner>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterViewInner {
    pub membership: ClusterConfig,
    pub table: ScheduleTable,
    pub table_position: u64,
    /// Spec §2.5 / §6.5.2: the pin and report histories, beside the
    /// membership and the table under the SAME lock — the leader's
    /// pre-append check reads them through [`ClusterView::to_state`], so
    /// they have to move with the rest of the structured state, not a tick
    /// behind it.
    pub pins: Vec<UpgradePin>,
    pub reports: Vec<SnapshotReport>,
    /// #33: the per-row running versions, under the same lock for the same
    /// reason — the pin door validates against [`ClusterView::to_state`].
    pub running: [Option<RowRunning>; CNC_MAX_SERVICES],
    /// Catalog spec §4: the catalog's set list, under the same lock — the
    /// query module and the node's floor reader take it with the rest of
    /// the structured state.
    pub catalog: Vec<SetEntry>,
}

impl ClusterViewInner {
    /// The committed `SnapshotReport` position for one row — `None` when no
    /// record for it is held. The ONE statement of the rule, shared by
    /// [`ClusterView::report_position_for`] (under the lock) and by the
    /// node's re-offer (on its clone): a user row reads its `reports` entry;
    /// the cluster row ([`CLUSTER_ROW`]) has none, so it reads the newest
    /// catalog entry whose `cluster` verdict is recorded.
    pub fn report_position_for(&self, row: u8) -> Option<u64> {
        if row == CLUSTER_ROW {
            return self
                .catalog
                .iter()
                .rev()
                .find(|e| e.cluster.verdict != RowVerdict::Unreported)
                .map(|e| e.position);
        }
        self.reports
            .iter()
            .find(|r| r.row == row)
            .map(|r| r.position)
    }
}

/// Catalog ruling R16: FNV-1a-64 over the set list's wire encoding
/// ([`encode_set_list`]) — a content hash, so two nodes holding the same
/// catalog agree on it regardless of where their walks stand. Run by
/// [`ClusterView::publish`] on the cluster agent, never on the consensus
/// pass. A list too long to encode (never produced: retention caps it)
/// hashes as its length alone.
pub fn catalog_version_of(sets: &[SetEntry]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut bytes = Vec::with_capacity(2 + sets.len() * 64);
    if encode_set_list(sets, &mut bytes).is_none() {
        bytes.clear();
        bytes.extend_from_slice(&(sets.len() as u64).to_le_bytes());
    }
    bytes
        .iter()
        .fold(OFFSET, |h, b| (h ^ u64::from(*b)).wrapping_mul(PRIME))
}

impl ClusterView {
    pub fn new(genesis: &ClusterState) -> ClusterView {
        let v = ClusterView {
            position: AtomicU64::new(0),
            // Plan B3 T5: seeded from the recovered artifact, so a node whose
            // `uc2-cluster` agent has not run a cycle yet still reports the
            // walk it inherited rather than 0.
            consumed: AtomicU64::new(genesis.applied),
            settings_position: AtomicU64::new(0),
            admission_bytes: AtomicU64::new(0),
            fsm_lag_bytes: AtomicU64::new(0),
            snapshot_interval_bytes: AtomicU64::new(0),
            snapshot_target: AtomicU8::new(0),
            datagram_mtu: AtomicU32::new(0),
            versioned: AtomicU8::new(0),
            retain_sets: AtomicU16::new(0),
            auto_fetch: AtomicBool::new(true),
            catalog_agreed_position: AtomicU64::new(0),
            catalog_has_complete: AtomicBool::new(false),
            catalog_len: AtomicU64::new(0),
            catalog_stalled: AtomicU64::new(0),
            catalog_diverged: AtomicU64::new(0),
            catalog_newest_agreed_bytes: AtomicU64::new(0),
            catalog_version: AtomicU64::new(0),
            inner: Mutex::new(ClusterViewInner {
                membership: genesis.membership.clone(),
                table: genesis.table.clone(),
                table_position: genesis.table_position,
                pins: genesis.pins.clone(),
                reports: genesis.reports.clone(),
                running: genesis.running,
                catalog: genesis.catalog.clone(),
            }),
        };
        v.publish(genesis);
        v
    }

    /// Structured parts first, then `versioned` (#33), position LAST with
    /// Release, so a reader that sees the new position (or a `versioned`
    /// bit) and then locks sees the new inner.
    pub fn publish(&self, st: &ClusterState) {
        {
            let mut g = self.inner.lock().unwrap();
            g.membership = st.membership.clone();
            g.table = st.table.clone();
            g.table_position = st.table_position;
            g.pins.clone_from(&st.pins);
            g.reports.clone_from(&st.reports);
            g.running = st.running;
            g.catalog.clone_from(&st.catalog);
        }
        // `CNC_MAX_SERVICES == 8` rows, so the mask fits a byte.
        self.versioned
            .store(st.declared_mask() as u8, Ordering::Release);
        self.settings_position
            .store(st.settings_position, Ordering::Release);
        self.admission_bytes
            .store(st.settings.admission_bytes, Ordering::Release);
        self.fsm_lag_bytes
            .store(st.settings.fsm_lag_bytes, Ordering::Release);
        self.snapshot_interval_bytes
            .store(st.settings.snapshot_interval_bytes, Ordering::Release);
        self.snapshot_target
            .store(st.settings.snapshot_target as u8, Ordering::Release);
        self.datagram_mtu
            .store(st.settings.datagram_mtu, Ordering::Release);
        self.retain_sets.store(st.retain_sets(), Ordering::Release);
        self.auto_fetch
            .store(st.settings.auto_fetch, Ordering::Release);
        // Catalog gauges — like everything above, BEFORE `position`.
        let stalled = st
            .catalog
            .iter()
            .filter(|e| e.state == SetState::Commanded)
            .count();
        let diverged: usize = st
            .catalog
            .iter()
            .map(|e| {
                e.rows
                    .iter()
                    .chain(std::iter::once(&e.cluster))
                    .filter(|r| matches!(r.verdict, RowVerdict::Diverged | RowVerdict::NoMajority))
                    .count()
            })
            .sum();
        self.catalog_agreed_position.store(
            st.newest_agreed_at_most(u64::MAX).unwrap_or(0),
            Ordering::Release,
        );
        self.catalog_has_complete
            .store(!st.catalog_empty(), Ordering::Release);
        self.catalog_len
            .store(st.catalog.len() as u64, Ordering::Release);
        self.catalog_stalled
            .store(stalled as u64, Ordering::Release);
        self.catalog_diverged
            .store(diverged as u64, Ordering::Release);
        self.catalog_newest_agreed_bytes.store(
            st.catalog
                .iter()
                .rev()
                .find(|e| e.is_agreed())
                .map_or(0, SetEntry::total_size),
            Ordering::Release,
        );
        self.catalog_version
            .store(catalog_version_of(&st.catalog), Ordering::Release);
        self.position.store(st.applied, Ordering::Release);
    }

    /// Plan B3 T5: publish the agent's walk cursor (see [`Self::consumed`]).
    ///
    /// Deliberately NOT called from [`Self::publish`], which writes the
    /// structured parts BEFORE `ClusterAgent::publish_view` writes the pin
    /// words: a `consumed` store in there would be visible while the pins it
    /// promises are not. The agent calls this itself, last.
    ///
    /// `fetch_max`, not `store`: an artifact install replaces the whole FSM
    /// state, and a word whose only promise is "monotone" must not depend on
    /// the caller having checked that first.
    pub fn note_consumed(&self, applied: u64) {
        self.consumed.fetch_max(applied, Ordering::Release);
    }

    /// #33: one row's running version, under the inner lock (pin door,
    /// genesis) — a scalar read, like [`Self::report_position_for`], rather
    /// than a whole [`Self::to_state`] clone.
    pub fn running_for(&self, row: u8) -> Option<RowRunning> {
        self.inner
            .lock()
            .unwrap()
            .running
            .get(row as usize)
            .copied()
            .flatten()
    }

    pub fn snapshot_inner(&self) -> ClusterViewInner {
        self.inner.lock().unwrap().clone()
    }

    /// Ruling R38-1: the log-time stamp (`SetEntry::time_ns`, the SNAPSHOT
    /// frame's own stamp) of the committed catalog's set at `position`, or
    /// `None` when the catalog does not list it. A scalar read under the
    /// inner lock, like [`Self::report_position_for`], rather than a
    /// [`Self::snapshot_inner`] clone: the leader's report collector asks it
    /// once per pending instant, not per pass.
    pub fn catalog_time_at(&self, position: u64) -> Option<u64> {
        self.inner
            .lock()
            .unwrap()
            .catalog
            .iter()
            .find(|e| e.position == position)
            .map(|e| e.time_ns)
    }

    /// The committed `SnapshotReport` position for one row — `None` when the
    /// cluster FSM holds no record for it yet.
    ///
    /// A scalar read under the same lock, rather than
    /// [`Self::to_state`]: the leader's collector asks this question once per
    /// received report and once per ready row at append, and `to_state`
    /// clones the membership, the schedule table, the pin history and the
    /// report list to answer it (final review, minor 7). Nothing about the
    /// answer needs the rest of the state, and the allocation it avoided
    /// grows with the pin history.
    ///
    /// The cluster row ([`CLUSTER_ROW`]) has no per-row report entry (its
    /// report only fills a catalog entry's `cluster` field), so for it the
    /// answer is the newest catalog position whose `cluster` verdict is
    /// recorded — the same "already on the log" frontier a row's entry
    /// gives, which keeps the leader's `position <= held` staleness guard
    /// meaningful for row 255 rather than vacuous.
    pub fn report_position_for(&self, row: u8) -> Option<u64> {
        self.inner.lock().unwrap().report_position_for(row)
    }

    /// The view as a [`ClusterState`] — the inner clone plus the settings scalar
    /// atomics, with `applied` taken from `position`.
    ///
    /// This is what the leader's PRE-APPEND check runs `ClusterFsm::validate`
    /// against (spec §4.4, Ruling R5): the leader answers the admin request
    /// from the newest COMMITTED state it can see, so a command it accepts is
    /// one every replica's apply loop will also accept — and there is exactly
    /// ONE acceptance function, never a parallel node-side reimplementation of
    /// it. A read of the settings atomics can straddle a concurrent `publish`
    /// (they are stored one at a time), which costs nothing here: they are
    /// only ever bounds-checked, and the authoritative decision is the apply
    /// loop's on the committed state.
    pub fn to_state(&self) -> ClusterState {
        let inner = self.snapshot_inner();
        ClusterState {
            membership: inner.membership,
            table: inner.table,
            table_position: inner.table_position,
            settings: Settings {
                fsm_lag_bytes: self.fsm_lag_bytes.load(Ordering::Acquire),
                admission_bytes: self.admission_bytes.load(Ordering::Acquire),
                snapshot_interval_bytes: self.snapshot_interval_bytes.load(Ordering::Acquire),
                snapshot_target: match self.snapshot_target.load(Ordering::Acquire) {
                    1 => uc_protocol::v2::settings::Target::Learners,
                    _ => uc_protocol::v2::settings::Target::All,
                },
                datagram_mtu: self.datagram_mtu.load(Ordering::Acquire),
                // The NORMALISED value (`ClusterState::retain_sets`), so a
                // state that installed a v1–v3 image's unset `0` reads back
                // as `1` here — the one field `to_state` does not return
                // verbatim, and deliberately: a record rebuilt from this view
                // and re-proposed must pass the door's `1..=64` bound.
                retain_sets: self.retain_sets.load(Ordering::Acquire),
                auto_fetch: self.auto_fetch.load(Ordering::Acquire),
            },
            settings_position: self.settings_position.load(Ordering::Acquire),
            applied: self.position.load(Ordering::Acquire),
            pins: inner.pins,
            reports: inner.reports,
            running: inner.running,
            catalog: inner.catalog,
        }
    }
    pub fn membership(&self) -> ClusterConfig {
        self.inner.lock().unwrap().membership.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use uc_consensus::config::{Addr, ClusterConfig, ConfigOp};
    use uc_protocol::identity::pack_version;
    use uc_protocol::v2::frame::{CLUSTER_BODY_PREFIX_LEN, write_cluster_prefix};
    use uc_protocol::v2::schedule::{ScheduleEntry, ScheduleRule};
    use uc_protocol::v2::upgrade::{
        RowGenesis, RowRunning, SnapshotReport, UpgradePin, Verdict, decode_pin_list,
        decode_report_list, decode_running_list, verdict,
    };
    use uc_service::{ApplyCtx, RawStateMachine, SnapshotStateMachine};

    use super::*;

    fn genesis() -> ClusterState {
        ClusterState {
            membership: ClusterConfig::genesis(
                vec![(0, addr(0)), (1, addr(1)), (2, addr(2))],
                vec![],
            ),
            table: ScheduleTable { entries: vec![] },
            table_position: 0,
            settings: Settings::genesis_default(),
            settings_position: 0,
            applied: 0,
            pins: vec![],
            reports: vec![],
            running: [None; CNC_MAX_SERVICES],
            catalog: vec![],
        }
    }
    /// `Addr = (ip: u32 network-order, port: u16)` — the same encoding
    /// `uc_node::node::addr_to_pair` uses (`u32::from_be_bytes(octets)`).
    fn addr(i: u32) -> Addr {
        (u32::from_be_bytes([127, 0, 0, i as u8]), 9100 + i as u16)
    }
    fn fsm() -> ClusterFsm {
        ClusterFsm::new(genesis(), vec![0xF5A0, 0xF5A1])
    }
    fn body(cmd: &ClusterCommand) -> Vec<u8> {
        let mut payload = Vec::new();
        let kind = ClusterFsm::encode_command(cmd, &mut payload);
        let mut b = vec![0u8; CLUSTER_BODY_PREFIX_LEN];
        write_cluster_prefix(&mut b, kind);
        b.extend_from_slice(&payload);
        b
    }

    #[test]
    fn identity_is_the_reserved_name() {
        assert_eq!(<ClusterFsm as RawStateMachine>::NAME, "uc_cluster");
        assert_eq!(<ClusterFsm as RawStateMachine>::VERSION, 1);
    }

    #[test]
    fn membership_command_applies_through_the_kernels_own_rules() {
        let mut f = fsm();
        let next = f
            .state()
            .membership
            .apply(ConfigOp::AddLearner {
                id: 3,
                addr: addr(3),
            })
            .unwrap();
        let cmd = ClusterCommand::Membership(next.clone());
        assert!(f.validate(&cmd).is_ok());
        let mut out = Vec::new();
        f.apply(
            &mut ApplyCtx::for_sm::<ClusterFsm>(100),
            &body(&cmd),
            &mut out,
        );
        assert_eq!(out, [0]);
        assert_eq!(f.state().membership, next);
        assert_eq!(f.state().applied, 100);
    }

    #[test]
    fn a_second_reconfig_while_one_is_pending_is_refused_deterministically() {
        // `ClusterConfig::apply` already encodes "one at a time" via version
        // chaining: a command whose version is not adopted+1 is refused.
        let mut f = fsm();
        let mut stale = f.state().membership.clone();
        stale.version += 2;
        let cmd = ClusterCommand::Membership(stale);
        let r = f.validate(&cmd).unwrap_err();
        assert!(matches!(r, ClusterRefusal::Membership(_)));
        let mut out = Vec::new();
        f.apply(
            &mut ApplyCtx::for_sm::<ClusterFsm>(200),
            &body(&cmd),
            &mut out,
        );
        assert_ne!(out, [0]);
        assert_eq!(
            f.state().membership.version,
            genesis().membership.version,
            "refused: unchanged"
        );
        assert_eq!(
            f.state().applied,
            200,
            "a refused command still advances applied"
        );
    }

    /// The LEADER's door refuses the whole table when any entry names an FSM
    /// this node has not declared — all-or-nothing, so an operator never gets
    /// a partially applied table. Ruling R24 moved this out of `apply`; what
    /// `apply` does with a table that reached the log anyway is pinned by
    /// `a_committed_table_naming_an_undeclared_fsm_is_adopted_but_refused_at_the_door`.
    #[test]
    fn schedule_table_naming_an_undeclared_fsm_refuses_the_whole_table_at_the_door() {
        let f = fsm();
        let t = ScheduleTable {
            entries: vec![
                ScheduleEntry {
                    identity_hash: 0xF5A0,
                    timer_id: 1,
                    rule: ScheduleRule::Once { at_ns: 5 },
                },
                ScheduleEntry {
                    identity_hash: 0xDEAD,
                    timer_id: 2,
                    rule: ScheduleRule::Once { at_ns: 5 },
                },
            ],
        };
        let cmd = ClusterCommand::ScheduleTable(t);
        assert!(matches!(
            f.validate(&cmd),
            Err(ClusterRefusal::ScheduleUnknownFsm { entry: 1 })
        ));
        assert_eq!(f.validate(&cmd).unwrap_err().reason_code(), 43);
        // Refusing entry 1 rejects entry 0 with it: the table is one record.
        assert!(f.state().table.entries.is_empty());
    }

    /// I3 (Ruling R24): `apply`'s acceptance must be a function of FSM STATE
    /// and the frame, nothing else. `declared_hashes` is this host's
    /// `[services] names` — node-local — so leaving the unknown-FSM check in
    /// `apply` meant two nodes whose lists differ would reach opposite
    /// verdicts on the SAME command at the SAME position, both advance
    /// `applied` past it, hold different `table`/`table_position`, and write
    /// divergent artifacts under the same tag. Nothing fail-stops: the
    /// refusal path is `out.push(43)` and carry on.
    ///
    /// So a COMMITTED table is adopted unconditionally, and the declared-hash
    /// check stays where it decides nothing replicated: the leader's
    /// pre-append `validate`, which is what `apply_schedule_table` runs
    /// before anything reaches the log. A row this node does not declare
    /// simply never arms — node-local and harmless.
    #[test]
    fn a_committed_table_naming_an_undeclared_fsm_is_adopted_but_refused_at_the_door() {
        let mut f = ClusterFsm::new(genesis(), vec![0xF5A0]);
        let t = ScheduleTable {
            entries: vec![ScheduleEntry {
                identity_hash: 0xF5A1, // NOT declared here
                timer_id: 7,
                rule: ScheduleRule::Once { at_ns: 5 },
            }],
        };
        let cmd = ClusterCommand::ScheduleTable(t.clone());

        // The LEADER's door still refuses it, by name and by reason code.
        assert!(matches!(
            f.validate(&cmd),
            Err(ClusterRefusal::ScheduleUnknownFsm { entry: 0 })
        ));
        assert_eq!(f.validate(&cmd).unwrap_err().reason_code(), 43);

        // But `apply` — which every replica runs, including one whose
        // `[services] names` the operator fat-fingered — ADOPTS it.
        let mut out = Vec::new();
        f.apply(
            &mut ApplyCtx::for_sm::<ClusterFsm>(320),
            &body(&cmd),
            &mut out,
        );
        assert_eq!(out, [0], "accepted");
        assert_eq!(f.state().table, t);
        assert_eq!(f.state().table_position, 320);

        // The determinism that buys: a node with a DIFFERENT declared set
        // reaches the identical state at the identical position.
        let mut g = ClusterFsm::new(genesis(), vec![0xF5A1]);
        let mut out2 = Vec::new();
        g.apply(
            &mut ApplyCtx::for_sm::<ClusterFsm>(320),
            &body(&cmd),
            &mut out2,
        );
        assert_eq!(out, out2);
        assert_eq!(f.state(), g.state());
    }

    #[test]
    fn schedule_table_records_its_own_position() {
        let mut f = fsm();
        let t = ScheduleTable {
            entries: vec![ScheduleEntry {
                identity_hash: 0xF5A0,
                timer_id: 1,
                rule: ScheduleRule::Once { at_ns: 5 },
            }],
        };
        let mut out = Vec::new();
        let mut ctx = ApplyCtx::for_sm::<ClusterFsm>(400);
        f.apply(
            &mut ctx,
            &body(&ClusterCommand::ScheduleTable(t.clone())),
            &mut out,
        );
        assert_eq!(out, [0]);
        assert_eq!(f.state().table, t);
        // frame-END position, CONFIG's convention: the loop passes the END as
        // `position` for this FSM (Task 4), so `table_position == ctx.position`.
        assert_eq!(f.state().table_position, 400);
    }

    #[test]
    fn settings_bounds_are_checked_in_apply_not_against_this_host() {
        let f = fsm();
        let bad = Settings {
            admission_bytes: u64::MAX,
            ..Settings::genesis_default()
        };
        assert!(matches!(
            f.validate(&ClusterCommand::Settings(bad)),
            Err(ClusterRefusal::SettingsBounds(_))
        ));
        let ok = Settings {
            admission_bytes: 1 << 40,
            fsm_lag_bytes: 1 << 40,
            ..Settings::genesis_default()
        };
        // Larger than any host's buffer — ACCEPTED here; clamped at use (spec §4.4).
        assert!(f.validate(&ClusterCommand::Settings(ok)).is_ok());
    }

    /// I1: a sub-frame `fsm_lag` is refused AT THE DOOR, by name, rather than
    /// silently clamped. It is the one settings value whose too-SMALL side is
    /// unrecoverable — a lag below one frame pins the report ceiling inside
    /// the next frame, commit stops, and the only way to change a replicated
    /// setting is a `CLUSTER` frame that has to commit. The clamp at the point
    /// of use (`services::fsm_lag_from_setting`) still exists as the last
    /// line of defence for a record that came from genesis or from a host
    /// with a smaller frame; this refusal is so an operator is TOLD.
    ///
    /// The bound is [`MIN_FSM_LAG_BYTES`], a compile-time constant — the
    /// widest frame the transport can carry, on any host — so the verdict
    /// stays a function of FSM state and constants, never of this host.
    #[test]
    fn a_sub_frame_fsm_lag_is_refused_by_name_and_the_two_sentinels_are_not() {
        let f = fsm();
        let with_lag = |b: u64| {
            ClusterCommand::Settings(Settings {
                fsm_lag_bytes: b,
                ..Settings::genesis_default()
            })
        };
        for bad in [1u64, 32, MIN_FSM_LAG_BYTES - 1] {
            assert!(
                matches!(
                    f.validate(&with_lag(bad)),
                    Err(ClusterRefusal::SettingsBounds("fsm_lag"))
                ),
                "fsm_lag = {bad} must be refused"
            );
            assert_eq!(f.validate(&with_lag(bad)).unwrap_err().reason_code(), 47);
        }
        // `0` keeps its "derive this node's boot value" meaning and lockstep
        // keeps its own; neither is a byte bound, so neither is refused.
        assert!(f.validate(&with_lag(0)).is_ok());
        assert!(f.validate(&with_lag(FSM_LAG_LOCKSTEP)).is_ok());
        // Exactly one max-size frame is legal.
        assert!(f.validate(&with_lag(MIN_FSM_LAG_BYTES)).is_ok());
        assert!(f.validate(&with_lag(16 << 20)).is_ok());
    }

    /// Jumbo spec §5.5: `datagram_mtu` must be 0 or a rung.
    #[test]
    fn settings_datagram_mtu_must_be_a_rung() {
        let fsm = ClusterFsm::new(ClusterState::genesis_empty(), Vec::new());
        let mut s = Settings::genesis_default();
        s.datagram_mtu = 1500;
        assert_eq!(
            fsm.validate_replicated(&ClusterCommand::Settings(s)),
            Err(ClusterRefusal::SettingsBounds("datagram_mtu"))
        );
        assert_eq!(
            fsm.validate_replicated(&ClusterCommand::Settings(s))
                .unwrap_err()
                .reason_code(),
            47
        );
        s.datagram_mtu = 8832;
        assert!(
            fsm.validate_replicated(&ClusterCommand::Settings(s))
                .is_ok()
        );
        s.datagram_mtu = 0;
        assert!(
            fsm.validate_replicated(&ClusterCommand::Settings(s))
                .is_ok()
        );
    }

    /// Jumbo spec §5.5: the FSM keeps the rung monotone — an operator's
    /// `settings apply` (whose absent keys encode as 0) cannot lower it.
    #[test]
    fn settings_apply_never_lowers_datagram_mtu() {
        let mut fsm = ClusterFsm::new(ClusterState::genesis_empty(), Vec::new());
        let mut out = Vec::new();
        let mut raise = Settings::genesis_default();
        raise.datagram_mtu = 8960;
        fsm.apply(
            &mut ApplyCtx::for_sm::<ClusterFsm>(64),
            &body(&ClusterCommand::Settings(raise)),
            &mut out,
        );
        assert_eq!(out, [0]);
        assert_eq!(fsm.state().settings.datagram_mtu, 8960);
        // An operator record with datagram_mtu = 0 and a new admission value.
        let mut op = Settings::genesis_default();
        op.admission_bytes = 4096;
        fsm.apply(
            &mut ApplyCtx::for_sm::<ClusterFsm>(128),
            &body(&ClusterCommand::Settings(op)),
            &mut out,
        );
        assert_eq!(out, [0]);
        assert_eq!(
            fsm.state().settings.admission_bytes,
            4096,
            "the operator's field landed"
        );
        assert_eq!(
            fsm.state().settings.datagram_mtu,
            8960,
            "the rung did not move"
        );
        // A lower rung is likewise kept at the max.
        let mut lower = op;
        lower.datagram_mtu = 8832;
        fsm.apply(
            &mut ApplyCtx::for_sm::<ClusterFsm>(192),
            &body(&ClusterCommand::Settings(lower)),
            &mut out,
        );
        assert_eq!(fsm.state().settings.datagram_mtu, 8960);
        // And the monotone value is what reaches the view (Task 2 reads it).
        let view = ClusterView::new(fsm.state());
        assert_eq!(view.datagram_mtu.load(Ordering::Acquire), 8960);
        assert_eq!(view.to_state().settings.datagram_mtu, 8960);
    }

    /// Spec §9: `settings_position` is `table_position`'s twin — set by the
    /// Settings command's own frame-END, untouched by any other kind, and
    /// carried on the image, so a restarted node (and a joiner installing the
    /// artifact) exports `uc2_settings_position` as the position the settings
    /// really came from rather than 0.
    #[test]
    fn settings_position_tracks_the_settings_command_and_rides_the_image() {
        let mut f = fsm();
        let mut out = Vec::new();
        assert_eq!(f.state().settings_position, 0, "genesis: never on the log");
        f.apply(
            &mut ApplyCtx::for_sm::<ClusterFsm>(320),
            &body(&ClusterCommand::Settings(Settings {
                snapshot_interval_bytes: 7,
                ..Settings::genesis_default()
            })),
            &mut out,
        );
        assert_eq!(f.state().settings_position, 320);
        // A table command moves `table_position` and `applied`, never this.
        f.apply(
            &mut ApplyCtx::for_sm::<ClusterFsm>(640),
            &body(&ClusterCommand::ScheduleTable(ScheduleTable {
                entries: vec![],
            })),
            &mut out,
        );
        assert_eq!(f.state().settings_position, 320);
        assert_eq!(f.state().table_position, 640);

        let (handle, _) = f.freeze().unwrap();
        let mut img = Vec::new();
        ClusterFsm::stream_snapshot(handle, &mut img).unwrap();
        let mut g = ClusterFsm::new(genesis(), vec![0xF5A0, 0xF5A1]);
        g.install_snapshot(640, &mut img.as_slice()).unwrap();
        assert_eq!(g.state().settings_position, 320);

        // And it reaches the view, where `/metrics` reads it (spec §9).
        let view = ClusterView::new(g.state());
        assert_eq!(view.settings_position.load(Ordering::Acquire), 320);
        assert_eq!(view.position.load(Ordering::Acquire), 640);
        assert_eq!(view.to_state().settings_position, 320);
    }

    #[test]
    fn image_roundtrips_and_refuses_bad_magic_version_and_crc() {
        let mut f = fsm();
        let mut out = Vec::new();
        f.apply(
            &mut ApplyCtx::for_sm::<ClusterFsm>(500),
            &body(&ClusterCommand::Settings(Settings {
                snapshot_interval_bytes: 7,
                ..Settings::genesis_default()
            })),
            &mut out,
        );
        let (handle, pos) = f.freeze().unwrap();
        assert_eq!(pos, 500);
        let mut img = Vec::new();
        ClusterFsm::stream_snapshot(handle, &mut img).unwrap();
        assert_eq!(&img[0..8], CLUSTER_IMAGE_MAGIC);
        let mut g = ClusterFsm::new(genesis(), vec![0xF5A0, 0xF5A1]);
        assert_eq!(g.install_snapshot(500, &mut img.as_slice()).unwrap(), 500);
        assert_eq!(g.state(), f.state());
        let mut bad_crc = img.clone();
        *bad_crc.last_mut().unwrap() ^= 1;
        assert!(g.install_snapshot(500, &mut bad_crc.as_slice()).is_err());
        let mut bad_ver = img.clone();
        bad_ver[8] = 99;
        assert!(g.install_snapshot(500, &mut bad_ver.as_slice()).is_err());
        assert!(
            g.install_snapshot(501, &mut img.as_slice()).is_err(),
            "position mismatch refused"
        );
    }

    #[test]
    fn install_refuses_a_crc_valid_image_with_a_lying_length_prefix() {
        // CRC32 is a public checksum, not a MAC — a below-floor joiner
        // installs an artifact received from a peer over the wire, so a
        // tampered-but-checksum-consistent image must be refused, not panic.
        let mut f = fsm();
        let mut out = Vec::new();
        f.apply(
            &mut ApplyCtx::for_sm::<ClusterFsm>(500),
            &body(&ClusterCommand::Settings(Settings {
                snapshot_interval_bytes: 7,
                ..Settings::genesis_default()
            })),
            &mut out,
        );
        let (handle, _pos) = f.freeze().unwrap();
        let mut img = Vec::new();
        ClusterFsm::stream_snapshot(handle, &mut img).unwrap();

        // Layout: magic(8) | version u32(4) | applied u64(8) |
        // table_position u64(8) | settings_position u64(8) | ml u32(4) |
        // ... — overwrite `ml` with a value far past the image's actual
        // length, then recompute the CRC so the tampered image still clears
        // the checksum gate.
        const ML_OFFSET: usize = 8 + 4 + 8 + 8 + 8;
        img[ML_OFFSET..ML_OFFSET + 4].copy_from_slice(&9999u32.to_le_bytes());
        let body_len = img.len() - 4;
        let crc = crc32fast::hash(&img[..body_len]);
        img[body_len..].copy_from_slice(&crc.to_le_bytes());

        let mut g = ClusterFsm::new(genesis(), vec![0xF5A0, 0xF5A1]);
        let before = g.state().clone();
        assert!(g.install_snapshot(500, &mut img.as_slice()).is_err());
        assert_eq!(
            g.state(),
            &before,
            "a refused install must not mutate state"
        );
    }

    /// FROZEN (moved here from the deleted `schedule_state` module, body
    /// unchanged): the digest is the first TEN bytes of SHA-256 over the
    /// staged bytes, read little-endian as the admin request's
    /// `(id, ip, port)` fields. Pinned against the canonical `SHA-256("abc")`
    /// vector `ba7816bf 8f01cfea 414140de 5dae2223 …`, so `uc2ctl` and the
    /// node can never drift: they must compute the same three numbers or every
    /// apply is refused with `REASON_SCHEDULE_DIGEST`.
    #[test]
    fn staged_digest_is_the_first_ten_bytes_of_sha256_le() {
        let (id, ip, port) = staged_digest(b"abc");
        assert_eq!(id, 0xbf16_78ba, "bytes 0..4 LE");
        assert_eq!(ip, 0xeacf_018f, "bytes 4..8 LE");
        assert_eq!(port, 0x4141, "bytes 8..10 LE");
        // Any other bytes give a different triple (the whole point).
        assert_ne!(staged_digest(b"abd"), (id, ip, port));
        assert_ne!(staged_digest(b""), (id, ip, port));
    }

    /// Ruling R5: `ClusterView::to_state` is what the leader validates
    /// against, so it must reconstruct EVERY field the FSM's `validate` can
    /// read — the structured half from the mutex and the settings half from
    /// the four atomics — with `applied` taken from the view's position tag.
    #[test]
    fn to_state_round_trips_the_published_state() {
        let f = fsm();
        let mut st = f.state().clone();
        st.applied = 900;
        st.table_position = 640;
        st.settings = Settings {
            fsm_lag_bytes: 1 << 20,
            admission_bytes: 4096,
            snapshot_interval_bytes: 1 << 30,
            snapshot_target: uc_protocol::v2::settings::Target::Learners,
            datagram_mtu: 8832,
            retain_sets: 3,
            auto_fetch: false,
        };
        let v = ClusterView::new(&st);
        assert_eq!(v.to_state(), st);
        // And it is a genuine snapshot, not a handle: a later publish moves it.
        st.settings.admission_bytes = 8192;
        st.applied = 1000;
        v.publish(&st);
        assert_eq!(v.to_state(), st);
    }

    #[test]
    fn view_publish_is_position_tagged_and_scalars_are_lock_free() {
        let f = fsm();
        let v = ClusterView::new(f.state());
        assert_eq!(v.position.load(Ordering::Acquire), 0);
        let mut st = f.state().clone();
        st.applied = 900;
        st.settings.admission_bytes = 123;
        v.publish(&st);
        assert_eq!(v.position.load(Ordering::Acquire), 900);
        assert_eq!(v.admission_bytes.load(Ordering::Acquire), 123);
        assert_eq!(v.membership(), st.membership);
    }

    /// Catalog ruling R16: `catalog_version` is a CONTENT hash of the set
    /// list — two FSMs whose catalogs are equal publish the same value
    /// whatever their walk cursors, and a one-entry difference changes it.
    #[test]
    fn catalog_version_is_a_content_hash_of_the_set_list() {
        let a = ClusterView::new(&ClusterState::genesis_empty());
        let b = ClusterView::new(&ClusterState::genesis_empty());
        let mut sa = ClusterState::genesis_empty();
        sa.on_snapshot_frame(1000, false, 5);
        sa.on_snapshot_frame(2000, true, 6);
        sa.applied = 2100;
        let mut sb = sa.clone();
        sb.applied = 9999; // a different walk cursor (trailing MESSAGE frames)
        a.publish(&sa);
        b.publish(&sb);
        let va = a.catalog_version.load(Ordering::Acquire);
        assert_eq!(va, b.catalog_version.load(Ordering::Acquire));
        assert_eq!(va, catalog_version_of(&sa.catalog));
        sb.on_snapshot_frame(3000, false, 7);
        b.publish(&sb);
        assert_ne!(
            va,
            b.catalog_version.load(Ordering::Acquire),
            "one more entry"
        );
        assert_ne!(
            catalog_version_of(&[]),
            va,
            "the empty catalog has its own version"
        );
    }

    // ------------------------------------------------------ FSM upgrade lifecycle

    fn pin(row: u8, from: u32, to: u32, origin: u64) -> ClusterCommand {
        ClusterCommand::UpgradePin(UpgradePin {
            row,
            from,
            to,
            origin,
        })
    }
    fn report(row: u8, position: u64, hashes: &[(u32, u64, u64)]) -> ClusterCommand {
        ClusterCommand::SnapshotReport(SnapshotReport {
            row,
            position,
            hashes: hashes.to_vec(),
        })
    }
    fn apply_at(f: &mut ClusterFsm, pos: u64, cmd: &ClusterCommand) -> u8 {
        let mut out = Vec::new();
        let mut ctx = ApplyCtx::new(pos, ClusterFsm::IDENTITY);
        f.apply(&mut ctx, &body(cmd), &mut out);
        out[0]
    }

    #[test]
    fn a_pin_is_applied_and_the_newest_per_row_is_found() {
        let mut f = fsm();
        assert_eq!(apply_at(&mut f, 100, &pin(0, 1, 2, 50)), 0);
        assert_eq!(apply_at(&mut f, 200, &pin(1, 7, 8, 150)), 0);
        assert_eq!(apply_at(&mut f, 300, &pin(0, 2, 3, 250)), 0);
        assert_eq!(
            f.state().pin_for(0).map(|p| (p.from, p.to, p.origin)),
            Some((2, 3, 250))
        );
        assert_eq!(f.state().pin_for(1).map(|p| p.origin), Some(150));
        assert_eq!(f.state().pin_for(2), None);
        assert_eq!(f.state().pins.len(), 3, "history is kept in apply order");
        assert_eq!(f.state().applied, 300);
    }

    #[test]
    fn pin_refusals_are_replicated_state_only() {
        // #33: versions are packed on distinct LINES here, because 53 is now
        // `same_line(from, running)` (patch ignored) rather than the old exact
        // `from == previous pin's to` — bare 1/2/3 are all line 0.0.
        let (v1, v2, v3) = (
            pack_version(1, 0, 0),
            pack_version(2, 0, 0),
            pack_version(3, 0, 0),
        );
        let mut f = fsm();
        assert_eq!(apply_at(&mut f, 100, &pin(0, v1, v2, 50)), 0);
        // 55: origin not above the row's current pin (equal, then below).
        assert_eq!(apply_at(&mut f, 200, &pin(0, v2, v3, 50)), 55);
        assert_eq!(apply_at(&mut f, 300, &pin(0, v2, v3, 40)), 55);
        // 53: `from` is not on the line the row runs (the last pin's `to`).
        assert_eq!(apply_at(&mut f, 400, &pin(0, v1, v3, 90)), 53);
        // A row with NO running version accepts any `from` here — that half
        // of 53 is the leader's door check against the attached version word.
        assert_eq!(apply_at(&mut f, 500, &pin(1, 42, 43, 90)), 0);
        assert_eq!(f.state().applied, 500, "a refusal still advances applied");
        assert_eq!(
            f.state().pin_for(0).map(|p| p.to),
            Some(v2),
            "nothing changed on refusal"
        );
        assert_eq!(f.state().running_for(0).map(|r| r.version), Some(v2));
    }

    #[test]
    fn pin_history_is_bounded_per_row() {
        let mut f = fsm();
        for i in 1..=6u64 {
            assert_eq!(
                apply_at(&mut f, i * 100, &pin(0, i as u32, i as u32 + 1, i * 10)),
                0
            );
        }
        assert_eq!(apply_at(&mut f, 700, &pin(1, 0, 1, 5)), 0);
        let row0: Vec<u64> = f
            .state()
            .pins
            .iter()
            .filter(|p| p.row == 0)
            .map(|p| p.origin)
            .collect();
        assert_eq!(
            row0,
            vec![30, 40, 50, 60],
            "MAX_PINS_PER_ROW = 4, oldest dropped"
        );
        assert_eq!(MAX_PINS_PER_ROW, 4);
        assert_eq!(f.state().pins.len(), 5, "row 1's entry is untouched");
    }

    #[test]
    fn a_report_is_held_newest_per_row_and_a_stale_one_is_refused() {
        let mut f = fsm();
        assert_eq!(
            apply_at(
                &mut f,
                100,
                &report(0, 50, &[(0, 1, 0), (1, 1, 0), (2, 2, 0)])
            ),
            0
        );
        assert_eq!(f.state().report_for(0).map(|r| r.position), Some(50));
        assert_eq!(
            apply_at(&mut f, 200, &report(0, 40, &[(0, 1, 0)])),
            59,
            "below the held position"
        );
        assert_eq!(
            apply_at(&mut f, 300, &report(0, 50, &[(0, 1, 0), (1, 1, 0)])),
            0,
            "equal replaces (a fuller vector for the same instant)"
        );
        assert_eq!(f.state().report_for(0).map(|r| r.hashes.len()), Some(2));
        assert_eq!(apply_at(&mut f, 400, &report(3, 10, &[(0, 9, 0)])), 0);
        assert_eq!(f.state().reports.len(), 2, "one entry per row");
        assert_eq!(
            verdict(f.state().report_for(0).unwrap()),
            Verdict {
                agreed: true,
                majority_hash: Some(1),
                minority: vec![]
            }
        );
    }

    #[test]
    fn pins_and_reports_ride_the_image_and_an_old_image_installs_empty() {
        let mut f = fsm();
        apply_at(&mut f, 100, &pin(0, 1, 2, 50));
        apply_at(
            &mut f,
            200,
            &report(0, 50, &[(0, 1, 0), (1, 2, 0), (2, 2, 0)]),
        );
        let (img, pos) = f.freeze().unwrap();
        assert_eq!(pos, 200);
        let mut g = ClusterFsm::new(genesis(), vec![]);
        assert_eq!(g.install_snapshot(200, &mut &img[..]).unwrap(), 200);
        assert_eq!(g.state(), f.state());
        // A version-1 image (no blobs) installs with empty histories.
        let v1 = {
            let mut m = Vec::new();
            encode_config(&cluster_to_wire(&genesis().membership, 0), &mut m);
            let mut s = Vec::new();
            encode_settings(&Settings::genesis_default(), &mut s);
            let mut b = Vec::new();
            b.extend_from_slice(b"UCCLUST1");
            b.extend_from_slice(&1u32.to_le_bytes());
            b.extend_from_slice(&7u64.to_le_bytes());
            b.extend_from_slice(&0u64.to_le_bytes());
            b.extend_from_slice(&0u64.to_le_bytes());
            b.extend_from_slice(&(m.len() as u32).to_le_bytes());
            b.extend_from_slice(&m);
            b.extend_from_slice(&8u32.to_le_bytes());
            b.extend_from_slice(&1u32.to_le_bytes()); // table version
            b.extend_from_slice(&0u32.to_le_bytes()); // table count
            b.extend_from_slice(&s);
            let crc = crc32fast::hash(&b);
            b.extend_from_slice(&crc.to_le_bytes());
            b
        };
        let mut h = fsm();
        assert_eq!(h.install_snapshot(7, &mut &v1[..]).unwrap(), 7);
        assert!(h.state().pins.is_empty() && h.state().reports.is_empty());
    }

    #[test]
    fn queries_4_and_5_return_the_lists() {
        // Reusing one `out` across the queries below is deliberate and safe:
        // `ClusterFsm::query` starts with `out.clear()`, so each answer is
        // the whole answer, never appended to the previous one.
        let mut f = fsm();
        apply_at(&mut f, 100, &pin(2, 1, 2, 50));
        let mut out = Vec::new();
        f.query(&[4], &mut out);
        assert_eq!(decode_pin_list(&out).unwrap(), f.state().pins);
        f.query(&[5], &mut out);
        assert_eq!(decode_report_list(&out).unwrap(), vec![]);
    }

    // ------------------------------------------------------ #33 running version

    fn genesis_cmd(row: u8, version: u32) -> ClusterCommand {
        ClusterCommand::RowGenesis(RowGenesis { row, version })
    }

    /// A version-2 image (plan B1's layout: pins and reports, no `running`
    /// blob) holding one pin. Provenance: `encode_cluster_image` writes v3,
    /// whose only difference from v2 is the trailing `u32` running-length
    /// prefix (plus its bytes, empty here) before the CRC, and the version
    /// word — so this takes a v3 image with an EMPTY running blob, drops that
    /// 4-byte zero prefix, writes version 2 and re-seals the CRC. That is
    /// byte-for-byte the v2 framing `uc_protocol`'s `PLAN_B1_V2_FIXTURE`
    /// pins (test-module constants are not reachable from this crate).
    fn v2_image_with_pin(p: UpgradePin, applied: u64) -> Vec<u8> {
        let mut m = Vec::new();
        encode_config(&cluster_to_wire(&genesis().membership, 0), &mut m);
        let mut t = Vec::new();
        encode_schedule_table(&ScheduleTable { entries: vec![] }, &mut t);
        let mut s = Vec::new();
        encode_settings(&Settings::genesis_default(), &mut s);
        let mut pins = Vec::new();
        encode_pin_list(&[p], &mut pins);
        let mut img = Vec::new();
        encode_cluster_image(
            &ClusterImageParts {
                applied,
                table_position: 0,
                settings_position: 0,
                membership: &m,
                table: &t,
                settings: &s,
                pins: &pins,
                reports: &[],
                running: &[],
                catalog: &[],
            },
            &mut img,
        )
        .unwrap();
        // Strip the CRC, the empty catalog blob's zero length prefix (v4)
        // and the empty running blob's zero length prefix.
        img.truncate(img.len() - 4);
        let catalog_prefix = img.split_off(img.len() - 4);
        assert_eq!(catalog_prefix, 0u32.to_le_bytes(), "empty catalog blob");
        let prefix = img.split_off(img.len() - 4);
        assert_eq!(prefix, 0u32.to_le_bytes(), "empty running blob");
        img[8..12].copy_from_slice(&2u32.to_le_bytes());
        let crc = crc32fast::hash(&img);
        img.extend_from_slice(&crc.to_le_bytes());
        img
    }

    #[test]
    fn genesis_sets_a_rows_running_version_once_and_refuses_60_after() {
        let mut f = fsm();
        assert_eq!(f.state().running_for(1), None);
        assert_eq!(
            apply_at(&mut f, 640, &genesis_cmd(1, pack_version(1, 0, 0))),
            0
        );
        assert_eq!(
            f.state().running_for(1),
            Some(RowRunning {
                row: 1,
                version: pack_version(1, 0, 0),
                record_pos: 640
            })
        );
        assert_eq!(
            apply_at(&mut f, 1280, &genesis_cmd(1, pack_version(2, 0, 0))),
            60
        );
        assert_eq!(ClusterRefusal::VersionAlreadySet.reason_code(), 60);
        assert_eq!(
            f.state().running_for(1).unwrap(),
            RowRunning {
                row: 1,
                version: pack_version(1, 0, 0),
                record_pos: 640
            },
            "a refused record changes nothing"
        );
        assert_eq!(f.state().applied, 1280, "a refusal still advances applied");
        // `Some(0)` is a recorded unversioned FSM, distinct from `None` (D4).
        assert_eq!(apply_at(&mut f, 1920, &genesis_cmd(2, 0)), 0);
        assert_eq!(f.state().running_for(2).map(|r| r.version), Some(0));
        assert_eq!(apply_at(&mut f, 2560, &genesis_cmd(2, 0)), 60);
    }

    #[test]
    fn a_pin_sets_running_and_must_start_from_the_running_line() {
        let mut f = fsm();
        assert_eq!(
            apply_at(&mut f, 640, &genesis_cmd(0, pack_version(1, 4, 2))),
            0
        );
        // Off the running line (1.3 vs 1.4): refused 53, and nothing moves.
        let off_line = pin(0, pack_version(1, 3, 0), pack_version(2, 0, 0), 512);
        assert_eq!(apply_at(&mut f, 1280, &off_line), 53);
        assert_eq!(f.state().pin_for(0), None, "refused: no pin recorded");
        assert_eq!(
            f.state().running_for(0).unwrap().record_pos,
            640,
            "refused: running untouched"
        );
        // Same line, different patch: accepted (D3), and running follows `to`.
        let patch_from = pin(0, pack_version(1, 4, 7), pack_version(2, 0, 0), 512);
        assert_eq!(apply_at(&mut f, 1920, &patch_from), 0);
        assert_eq!(
            f.state().running_for(0).unwrap(),
            RowRunning {
                row: 0,
                version: pack_version(2, 0, 0),
                record_pos: 1920
            }
        );
        // Rollback is just another pin, from the running line (spec §4.1).
        let back = pin(0, pack_version(2, 0, 0), pack_version(1, 4, 2), 1024);
        assert_eq!(apply_at(&mut f, 2560, &back), 0);
        assert_eq!(
            f.state().running_for(0).map(|r| (r.version, r.record_pos)),
            Some((pack_version(1, 4, 2), 2560))
        );
        // A pin on a row with NO running version sets it (the door owns the
        // attached-word check for that case).
        assert_eq!(
            apply_at(&mut f, 3200, &pin(3, 9, pack_version(0, 2, 0), 64)),
            0
        );
        assert_eq!(
            f.state().running_for(3).map(|r| r.version),
            Some(pack_version(0, 2, 0))
        );
        // A pin refused 55 (origin not monotone) leaves running alone too.
        let stale = pin(0, pack_version(1, 4, 2), pack_version(3, 0, 0), 1024);
        assert_eq!(apply_at(&mut f, 3840, &stale), 55);
        assert_eq!(
            f.state().running_for(0).map(|r| r.version),
            Some(pack_version(1, 4, 2))
        );
    }

    #[test]
    fn freeze_install_round_trips_running_and_a_v2_image_migrates_from_pins() {
        let mut f = fsm();
        apply_at(&mut f, 640, &genesis_cmd(2, 7));
        apply_at(&mut f, 1280, &genesis_cmd(0, pack_version(1, 0, 0)));
        let (img, pos) = f.freeze().unwrap();
        let mut g = fsm();
        g.install_snapshot(pos, &mut &img[..]).unwrap();
        assert_eq!(g.state().running, f.state().running);
        assert_eq!(g.state(), f.state());
        // v2 image with a pin for row 0 and nothing for row 1: row 0 migrates
        // to the pin's `to` at record_pos = applied; row 1 stays None.
        let img_v2 = v2_image_with_pin(
            UpgradePin {
                row: 0,
                from: 1,
                to: 2,
                origin: 512,
            },
            4096,
        );
        let mut h = fsm();
        h.install_snapshot(4096, &mut &img_v2[..]).unwrap();
        assert_eq!(
            h.state().running_for(0),
            Some(RowRunning {
                row: 0,
                version: 2,
                record_pos: 4096
            })
        );
        assert_eq!(h.state().running_for(1), None);
        assert_eq!(h.state().pin_for(0).map(|p| p.to), Some(2));
    }

    #[test]
    fn query_6_returns_the_running_list() {
        let mut f = fsm();
        apply_at(&mut f, 640, &genesis_cmd(3, 5));
        apply_at(&mut f, 1280, &genesis_cmd(1, 9));
        let mut out = Vec::new();
        f.query(&[6], &mut out);
        assert_eq!(
            decode_running_list(&out).unwrap(),
            vec![
                RowRunning {
                    row: 1,
                    version: 9,
                    record_pos: 1280
                },
                RowRunning {
                    row: 3,
                    version: 5,
                    record_pos: 640
                },
            ]
        );
    }

    #[test]
    fn the_view_publishes_running_and_the_versioned_mask() {
        let mut f = fsm();
        let v = ClusterView::new(f.state());
        assert_eq!(v.versioned.load(Ordering::Acquire), 0);
        assert_eq!(v.running_for(0), None);
        apply_at(&mut f, 640, &genesis_cmd(0, 4));
        apply_at(&mut f, 1280, &genesis_cmd(5, 0));
        v.publish(f.state());
        assert_eq!(v.versioned.load(Ordering::Acquire), 0b0010_0001);
        assert_eq!(v.running_for(5), f.state().running_for(5));
        assert_eq!(v.running_for(1), None);
        assert_eq!(v.to_state(), *f.state());
        // `new` publishes too: a recovered state's mask is live at once.
        let w = ClusterView::new(f.state());
        assert_eq!(w.versioned.load(Ordering::Acquire), 0b0010_0001);
    }

    #[test]
    fn the_view_publishes_pins_and_reports() {
        let mut f = fsm();
        apply_at(&mut f, 100, &pin(0, 1, 2, 50));
        apply_at(&mut f, 200, &report(0, 50, &[(0, 1, 0)]));
        let v = ClusterView::new(&genesis());
        v.publish(f.state());
        let st = v.to_state();
        assert_eq!(st.pins, f.state().pins);
        assert_eq!(st.reports, f.state().reports);
        assert_eq!(v.position.load(Ordering::Acquire), 200);
    }

    // ---- Catalog spec §4 (Task 5): the catalog inside the cluster FSM ----

    // The catalog types come in through `super::*`.

    fn genesis_row(f: &mut ClusterFsm, row: u8, pos: u64) {
        assert_eq!(
            apply_at(
                f,
                pos,
                &ClusterCommand::RowGenesis(RowGenesis {
                    row,
                    version: pack_version(1, 0, 0)
                })
            ),
            0
        );
    }
    /// One agreed set at `p`: the SNAPSHOT frame, then row 0 and the cluster row report one hash each.
    fn agreed_set(f: &mut ClusterFsm, p: u64, at: u64) {
        f.on_snapshot_frame(p, false, p);
        assert_eq!(apply_at(f, at, &report(0, p, &[(0, 1, 0)])), 0);
        assert_eq!(
            apply_at(f, at + 10, &report(CLUSTER_ROW, p, &[(0, 1, 0)])),
            0
        );
    }
    fn positions(f: &ClusterFsm) -> Vec<u64> {
        f.state().catalog.iter().map(|e| e.position).collect()
    }
    fn settings_with_retain(f: &ClusterFsm, retain_sets: u16) -> ClusterCommand {
        let mut s = f.state().settings;
        s.retain_sets = retain_sets;
        ClusterCommand::Settings(s)
    }

    #[test]
    fn a_snapshot_frame_records_a_commanded_set() {
        let mut f = fsm();
        f.on_snapshot_frame(4096, true, 77);
        let e = &f.state().catalog[0];
        assert_eq!(
            (e.position, e.kind, e.state, e.time_ns),
            (4096, SetKind::Standby, SetState::Commanded, 77)
        );
        assert!(f.state().catalog_empty(), "commanded is not agreed");
    }

    #[test]
    fn a_replayed_snapshot_frame_does_not_duplicate_the_entry() {
        let mut f = fsm();
        f.on_snapshot_frame(4096, false, 1);
        f.on_snapshot_frame(4096, false, 1);
        assert_eq!(f.state().catalog.len(), 1);
    }

    #[test]
    fn reports_complete_a_set_and_the_cluster_row_is_required() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(4096, false, 1);
        assert_eq!(
            apply_at(&mut f, 4200, &report(0, 4096, &[(0, 7, 0), (1, 7, 0)])),
            0
        );
        assert_eq!(
            f.state().catalog[0].state,
            SetState::Commanded,
            "cluster row still unreported"
        );
        assert_eq!(
            apply_at(
                &mut f,
                4300,
                &report(CLUSTER_ROW, 4096, &[(0, 9, 0), (1, 9, 0)])
            ),
            0
        );
        let e = &f.state().catalog[0];
        assert_eq!(e.state, SetState::Complete);
        assert_eq!(
            e.rows[0],
            RowEntry {
                version: pack_version(1, 0, 0),
                hash: 7,
                verdict: RowVerdict::Agreed,
                size: 0,
            }
        );
        assert_eq!((e.cluster.hash, e.cluster.verdict), (9, RowVerdict::Agreed));
        assert!(!f.state().catalog_empty());
        assert_eq!(f.state().newest_agreed_at_most(u64::MAX), Some(4096));
    }

    #[test]
    fn a_diverged_row_completes_but_never_agrees() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(4096, false, 1);
        assert_eq!(
            apply_at(
                &mut f,
                4200,
                &report(0, 4096, &[(0, 7, 0), (1, 8, 0), (2, 7, 0)])
            ),
            0
        );
        assert_eq!(
            apply_at(
                &mut f,
                4300,
                &report(CLUSTER_ROW, 4096, &[(0, 9, 0), (1, 9, 0), (2, 9, 0)])
            ),
            0
        );
        let e = &f.state().catalog[0];
        assert_eq!(
            (e.state, e.rows[0].verdict, e.rows[0].hash),
            (SetState::Complete, RowVerdict::Diverged, 7)
        );
        assert_eq!(f.state().newest_agreed_at_most(u64::MAX), None);
        assert!(
            !f.state().catalog_empty(),
            "ruling R26: a Complete-but-diverged set is not Empty"
        );
        let v = ClusterView::new(f.state());
        assert_eq!(v.catalog_agreed_position.load(Ordering::Acquire), 0);
        assert!(v.catalog_has_complete.load(Ordering::Acquire));
    }

    #[test]
    fn a_report_for_an_undeclared_row_is_ignored() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(4096, false, 1);
        assert_eq!(apply_at(&mut f, 4200, &report(0, 4096, &[(0, 7, 0)])), 0);
        assert_eq!(apply_at(&mut f, 4250, &report(5, 4096, &[(0, 7, 0)])), 0); // row 5 never declared
        assert_eq!(
            apply_at(&mut f, 4300, &report(CLUSTER_ROW, 4096, &[(0, 9, 0)])),
            0
        );
        let e = &f.state().catalog[0];
        assert_eq!(e.rows[5].verdict, RowVerdict::Unreported);
        assert_eq!(e.state, SetState::Complete, "row 5 is not required");
    }

    #[test]
    fn retention_keeps_retain_sets_agreed_sets_and_drops_the_rest() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        let cmd = settings_with_retain(&f, 2);
        assert_eq!(apply_at(&mut f, 200, &cmd), 0);
        agreed_set(&mut f, 1000, 1100);
        agreed_set(&mut f, 2000, 2100);
        agreed_set(&mut f, 3000, 3100);
        assert_eq!(
            positions(&f),
            vec![2000, 3000],
            "1000 retired: beyond retain_sets = 2"
        );
        agreed_set(&mut f, 4000, 4100);
        assert_eq!(positions(&f), vec![3000, 4000]);
        assert_eq!(f.state().newest_agreed_at_most(3500), Some(3000));
    }

    #[test]
    fn lowering_retention_never_retires_a_pinned_origin() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        let cmd = settings_with_retain(&f, 4);
        assert_eq!(apply_at(&mut f, 200, &cmd), 0);
        agreed_set(&mut f, 1000, 1100);
        agreed_set(&mut f, 2000, 2100);
        let pin = UpgradePin {
            row: 0,
            origin: 1000,
            from: pack_version(1, 0, 0),
            to: pack_version(1, 1, 0),
        };
        assert_eq!(apply_at(&mut f, 2500, &ClusterCommand::UpgradePin(pin)), 0);
        let cmd = settings_with_retain(&f, 1);
        assert_eq!(apply_at(&mut f, 2600, &cmd), 0);
        assert_eq!(positions(&f), vec![1000, 2000], "the pinned origin stays");
        assert_eq!(f.state().newest_agreed_at_most(1500), Some(1000));
    }

    /// Report row 0 and the cluster row for the set at `p` (commanded
    /// earlier), one hash each — the second half of [`agreed_set`].
    fn agree(f: &mut ClusterFsm, p: u64, at: u64) {
        assert_eq!(apply_at(f, at, &report(0, p, &[(0, 1, 0)])), 0);
        assert_eq!(
            apply_at(f, at + 10, &report(CLUSTER_ROW, p, &[(0, 1, 0)])),
            0
        );
    }

    /// Ruling R21: a pinned origin is kept IN ADDITION to `retain_sets`,
    /// never counted toward it — while the pin is INCOMPLETE. With
    /// `retain_sets = 2` and the oldest set pinned, `[pin, A, B]` keeps all
    /// three; a fourth agreed set retires A (the oldest UNPINNED one), never
    /// the pin. A, B and C are commanded BELOW the pin record (agreed after
    /// it), so none of them completes the pin (ruling C1's record bound).
    #[test]
    fn a_pinned_origin_does_not_count_toward_retain_sets() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        let cmd = settings_with_retain(&f, 2);
        assert_eq!(apply_at(&mut f, 200, &cmd), 0);
        agreed_set(&mut f, 1000, 1100);
        for p in [2000, 3000, 4000] {
            f.on_snapshot_frame(p, false, p);
        }
        assert_eq!(
            apply_at(
                &mut f,
                4500,
                &pin(0, pack_version(1, 0, 0), pack_version(1, 1, 0), 1000)
            ),
            0
        );
        agree(&mut f, 2000, 4600);
        agree(&mut f, 3000, 4700);
        assert_eq!(
            positions(&f),
            vec![1000, 2000, 3000, 4000],
            "the pin is kept beside two retained sets"
        );
        agree(&mut f, 4000, 4800);
        assert_eq!(positions(&f), vec![1000, 3000, 4000], "A (2000) retires");
    }

    /// Pin completion ruling C3, the FSM half: an agreed set ABOVE the pin
    /// record, on `to`'s line, completes the pin, and from that apply the
    /// origin is an ordinary agreed set — counted toward `retain_sets` and
    /// retired like any other. Deterministic: every replica applies the same
    /// record and computes the same predicate.
    #[test]
    fn a_completed_pin_no_longer_protects_its_origin() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        let cmd = settings_with_retain(&f, 2);
        assert_eq!(apply_at(&mut f, 200, &cmd), 0);
        agreed_set(&mut f, 1000, 1100);
        agreed_set(&mut f, 2000, 2100);
        assert_eq!(
            apply_at(
                &mut f,
                2500,
                &pin(0, pack_version(1, 0, 0), pack_version(1, 1, 0), 2000)
            ),
            0
        );
        assert_eq!(positions(&f), vec![1000, 2000]);
        // The first set above the record completes the pin: `[1000, 2000,
        // 3000]` are now all unpinned, so the oldest retires.
        agreed_set(&mut f, 3000, 3100);
        let st = f.state();
        let gate = crate::catalog::pin_gate(&st.pins, &st.running, 0).unwrap();
        assert_eq!(gate.record_pos, 2500);
        assert!(crate::catalog::pin_complete(&st.catalog, 0, &gate));
        assert_eq!(positions(&f), vec![2000, 3000]);
        agreed_set(&mut f, 4000, 4100);
        assert_eq!(
            positions(&f),
            vec![3000, 4000],
            "the completed pin's origin retires like any set"
        );
    }

    #[test]
    fn retain_sets_zero_and_above_the_bound_are_refused_with_47() {
        let f = fsm();
        // `0`: refused at the leader's door, accepted by the replicated half
        // (apply reads it as `1` — catalog ruling R24).
        let zero = settings_with_retain(&f, 0);
        assert_eq!(
            f.validate(&zero),
            Err(ClusterRefusal::SettingsBounds("retain_sets"))
        );
        assert_eq!(f.validate_replicated(&zero), Ok(()));
        // Above `MAX_RETAIN_SETS`: refused by both (ruling R8).
        let over = settings_with_retain(&f, MAX_RETAIN_SETS + 1);
        for r in [f.validate(&over), f.validate_replicated(&over)] {
            assert_eq!(r, Err(ClusterRefusal::SettingsBounds("retain_sets")));
        }
        assert_eq!(
            f.validate(&settings_with_retain(&f, MAX_RETAIN_SETS)),
            Ok(())
        );
        assert_eq!(
            ClusterRefusal::SettingsBounds("retain_sets").reason_code(),
            47
        );
    }

    #[test]
    fn a_commanded_entry_older_than_the_oldest_kept_agreed_set_is_dropped() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(500, false, 1); // never completes
        assert_eq!(positions(&f), vec![500]);
        agreed_set(&mut f, 1000, 1100);
        assert_eq!(
            positions(&f),
            vec![1000],
            "500 is older than the oldest kept agreed set"
        );
        f.on_snapshot_frame(1500, false, 2); // a stall ABOVE the floor stays visible
        agreed_set(&mut f, 2000, 2100); // retain_sets unset → 1: 1000 goes, and 1500 with it
        assert_eq!(positions(&f), vec![2000]);
    }

    #[test]
    fn two_fsms_fed_the_same_sequence_freeze_byte_equal_images() {
        let mut a = fsm();
        let mut b = fsm();
        for f in [&mut a, &mut b] {
            genesis_row(f, 0, 100);
            f.on_snapshot_frame(1000, true, 5);
            assert_eq!(apply_at(f, 1100, &report(0, 1000, &[(2, 1, 0)])), 0);
            assert_eq!(
                apply_at(f, 1110, &report(CLUSTER_ROW, 1000, &[(2, 1, 0)])),
                0
            );
            f.set_consumed(1200);
        }
        let (ia, _) = a.freeze().unwrap();
        let (ib, _) = b.freeze().unwrap();
        assert_eq!(ia, ib);
        let mut c = fsm();
        assert_eq!(c.install_snapshot(1200, &mut &ia[..]).unwrap(), 1200);
        assert_eq!(c.state().catalog, a.state().catalog);
    }

    #[test]
    fn a_v3_image_installs_with_an_empty_catalog() {
        // A v3 image = a v4 image minus the trailing empty-catalog prefix, with
        // the version word rewritten and the CRC recomputed.
        let mut f = fsm();
        f.set_consumed(300);
        let (img, _) = f.freeze().unwrap();
        let body_end = img.len() - 4;
        let mut v3 = img[..body_end - 4].to_vec(); // drop the 4-byte empty-catalog length prefix
        v3[8..12].copy_from_slice(&3u32.to_le_bytes());
        let crc = crc32fast::hash(&v3);
        v3.extend_from_slice(&crc.to_le_bytes());
        let mut g = fsm();
        assert_eq!(g.install_snapshot(300, &mut &v3[..]).unwrap(), 300);
        assert!(g.state().catalog.is_empty());
        assert!(g.state().catalog_empty());
        assert_eq!(
            g.state().retain_sets(),
            1,
            "a v3 image's retain_sets reads as 1"
        );
    }

    /// Snapshot-lifecycle spec §6: the view carries `auto_fetch`, and
    /// `to_state` returns it — a leader that re-proposes the committed record
    /// (the jumbo rung raise) must not silently turn the switch back on.
    #[test]
    fn the_view_publishes_auto_fetch_and_to_state_returns_it() {
        let mut st = fsm().state().clone();
        st.settings.auto_fetch = false;
        let v = ClusterView::new(&st);
        assert!(!v.auto_fetch.load(Ordering::Acquire));
        assert!(!v.to_state().settings.auto_fetch);
        st.settings.auto_fetch = true;
        v.publish(&st);
        assert!(v.to_state().settings.auto_fetch);
    }

    #[test]
    fn the_view_publishes_the_catalog_and_its_gauges() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        let v = ClusterView::new(f.state());
        assert_eq!(
            v.catalog_agreed_position.load(Ordering::Acquire),
            0,
            "Empty"
        );
        assert_eq!(v.retain_sets.load(Ordering::Acquire), 1);
        agreed_set(&mut f, 1000, 1100);
        f.on_snapshot_frame(1500, false, 2); // stalled
        f.on_snapshot_frame(2000, false, 3);
        assert_eq!(
            apply_at(&mut f, 2100, &report(0, 2000, &[(0, 1, 0), (1, 2, 0)])),
            0
        ); // NoMajority
        v.publish(f.state());
        assert_eq!(v.catalog_agreed_position.load(Ordering::Acquire), 1000);
        assert_eq!(v.catalog_len.load(Ordering::Acquire), 3);
        assert_eq!(v.catalog_stalled.load(Ordering::Acquire), 2);
        assert_eq!(v.catalog_diverged.load(Ordering::Acquire), 1);
        assert_eq!(v.snapshot_inner().catalog, f.state().catalog);
        assert_eq!(v.to_state(), *f.state());
    }

    #[test]
    fn the_catalog_records_the_version_that_built_the_set_not_the_one_after_a_pin() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(4096, false, 1);
        let up = UpgradePin {
            row: 0,
            origin: 4096,
            from: pack_version(1, 0, 0),
            to: pack_version(1, 1, 0),
        };
        assert_eq!(apply_at(&mut f, 4200, &ClusterCommand::UpgradePin(up)), 0);
        assert_eq!(apply_at(&mut f, 4300, &report(0, 4096, &[(0, 7, 0)])), 0);
        assert_eq!(
            apply_at(&mut f, 4400, &report(CLUSTER_ROW, 4096, &[(0, 9, 0)])),
            0
        );
        let e = f
            .state()
            .catalog
            .iter()
            .find(|e| e.position == 4096)
            .unwrap();
        assert_eq!(e.rows[0].version, pack_version(1, 0, 0), "from built it");
        // A later set, reported after the pin, was built by `to`.
        f.on_snapshot_frame(8192, false, 2);
        assert_eq!(apply_at(&mut f, 8300, &report(0, 8192, &[(0, 8, 0)])), 0);
        assert_eq!(
            apply_at(&mut f, 8400, &report(CLUSTER_ROW, 8192, &[(0, 9, 0)])),
            0
        );
        let e = f
            .state()
            .catalog
            .iter()
            .find(|e| e.position == 8192)
            .unwrap();
        assert_eq!(e.rows[0].version, pack_version(1, 1, 0));
    }

    #[test]
    fn an_image_whose_catalog_is_out_of_order_is_refused() {
        let mut f = fsm();
        f.on_snapshot_frame(1000, false, 1);
        f.on_snapshot_frame(2000, true, 2);
        f.set_consumed(3000);
        let (img, _) = f.freeze().unwrap();
        // The catalog blob is the image's tail before the CRC:
        // count u16 ‖ 2 × SET_ENTRY_LEN.
        use uc_protocol::v2::catalog::SET_ENTRY_LEN;
        let body_end = img.len() - 4;
        let first = body_end - 2 * SET_ENTRY_LEN;
        let mut bad = img[..body_end].to_vec();
        let a = bad[first..first + SET_ENTRY_LEN].to_vec();
        let b = bad[first + SET_ENTRY_LEN..body_end].to_vec();
        bad[first..first + SET_ENTRY_LEN].copy_from_slice(&b);
        bad[first + SET_ENTRY_LEN..body_end].copy_from_slice(&a);
        let crc = crc32fast::hash(&bad);
        bad.extend_from_slice(&crc.to_le_bytes());
        let mut g = fsm();
        let before = g.state().clone();
        match g.install_snapshot(3000, &mut &bad[..]) {
            Err(SnapshotError::Codec(m)) => assert_eq!(m, "cluster image: catalog out of order"),
            other => panic!("expected the order refusal, got {other:?}"),
        }
        assert_eq!(*g.state(), before, "a refused install changes nothing");
        // Control: the unswapped image installs.
        assert_eq!(g.install_snapshot(3000, &mut &img[..]).unwrap(), 3000);
    }

    #[test]
    fn retain_sets_at_the_bound_never_wedges_the_catalog() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        let cmd = settings_with_retain(&f, MAX_RETAIN_SETS);
        assert_eq!(apply_at(&mut f, 200, &cmd), 0);
        let n = MAX_RETAIN_SETS as u64;
        for k in 1..=n {
            agreed_set(&mut f, k * 1000, k * 1000 + 100);
        }
        assert_eq!(f.state().catalog.len(), MAX_RETAIN_SETS as usize);
        assert_eq!(f.state().newest_agreed_at_most(u64::MAX), Some(n * 1000));
        let next = (n + 1) * 1000;
        agreed_set(&mut f, next, next + 100);
        assert_eq!(
            f.state().newest_agreed_at_most(u64::MAX),
            Some(next),
            "the next instant lands and the floor advances"
        );
        assert_eq!(f.state().catalog.len(), MAX_RETAIN_SETS as usize);
        assert_eq!(f.state().catalog[0].position, 2000, "the oldest retired");
    }

    #[test]
    fn the_cap_never_evicts_the_newest_entry() {
        let mut f = fsm();
        for k in 1..=(MAX_CATALOG_SETS as u64 + 1) {
            f.on_snapshot_frame(k * 1000, false, k);
        }
        let p = positions(&f);
        assert_eq!(p.len(), MAX_CATALOG_SETS);
        assert_eq!(*p.last().unwrap(), (MAX_CATALOG_SETS as u64 + 1) * 1000);
        assert_eq!(p[0], 2000, "the oldest commanded entry went");
    }

    /// The differentiating case for `cap_catalog`'s newest-entry guard: a
    /// list FULL of agreed sets (an installed image whose `retain_sets`
    /// keeps them all), then one more `SNAPSHOT` frame. The new commanded
    /// entry is the ONLY non-agreed entry, so "evict the oldest non-agreed
    /// entry" over the whole list would evict the instant just commanded —
    /// and every later one on arrival. With the guard the oldest agreed set
    /// goes instead and the newest survives.
    #[test]
    fn the_cap_keeps_a_new_instant_over_a_full_list_of_agreed_sets() {
        let mut seed = fsm();
        genesis_row(&mut seed, 0, 100);
        let mut st = seed.state().clone();
        st.settings.retain_sets = MAX_CATALOG_SETS as u16; // hand-built: past the door's bound
        st.catalog = (1..=MAX_CATALOG_SETS as u64)
            .map(|k| {
                let mut e = SetEntry::commanded(k * 1000, SetKind::Full, k);
                e.state = SetState::Complete;
                e.cluster.verdict = RowVerdict::Agreed;
                e.rows[0].verdict = RowVerdict::Agreed;
                e
            })
            .collect();
        st.applied = 100_000;
        let (img, at) = ClusterFsm::new(st, vec![]).freeze().unwrap();
        let mut f = fsm();
        assert_eq!(f.install_snapshot(at, &mut &img[..]).unwrap(), at);
        assert_eq!(f.state().catalog.len(), MAX_CATALOG_SETS);
        let next = (MAX_CATALOG_SETS as u64 + 1) * 1000;
        f.on_snapshot_frame(next, false, 99);
        let p = positions(&f);
        assert_eq!(p.len(), MAX_CATALOG_SETS);
        assert_eq!(
            *p.last().unwrap(),
            next,
            "the instant just commanded survives"
        );
        assert_eq!(p[0], 2000, "the oldest agreed set was evicted instead");
    }

    #[test]
    fn a_row_added_later_does_not_unagree_earlier_sets_or_drop_a_pinned_origin() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        let cmd = settings_with_retain(&f, 4);
        assert_eq!(apply_at(&mut f, 200, &cmd), 0);
        agreed_set(&mut f, 1000, 1100);
        agreed_set(&mut f, 2000, 2100);
        genesis_row(&mut f, 1, 2500);
        assert!(
            f.state().catalog.iter().all(|e| e.is_agreed()),
            "agreement is frozen"
        );
        assert!(!f.state().catalog_empty());
        assert_eq!(f.state().newest_agreed_at_most(u64::MAX), Some(2000));
        // A set at 3000 must now cover rows 0 AND 1.
        f.on_snapshot_frame(3000, false, 3);
        // Row 0 pinned at 1000 with its record ABOVE the frame, so the set
        // at 3000 sits below the pin record and does not complete it (ruling
        // C1) — the pinned origin must stay through retention below.
        assert_eq!(
            apply_at(
                &mut f,
                3050,
                &pin(0, pack_version(1, 0, 0), pack_version(1, 1, 0), 1000)
            ),
            0
        );
        assert_eq!(apply_at(&mut f, 3100, &report(0, 3000, &[(0, 1, 0)])), 0);
        assert_eq!(
            apply_at(&mut f, 3110, &report(CLUSTER_ROW, 3000, &[(0, 1, 0)])),
            0
        );
        assert_eq!(
            f.state().catalog.last().unwrap().state,
            SetState::Commanded,
            "row 1 unreported"
        );
        assert_eq!(apply_at(&mut f, 3120, &report(1, 3000, &[(0, 1, 0)])), 0);
        assert_eq!(f.state().newest_agreed_at_most(u64::MAX), Some(3000));
        let cmd = settings_with_retain(&f, 1);
        assert_eq!(apply_at(&mut f, 3200, &cmd), 0);
        assert_eq!(
            positions(&f),
            vec![1000, 3000],
            "pinned 1000 stays, 2000 goes"
        );
    }

    /// Catalog ruling R24 (supersedes R10): a replayed pre-catalog (v2)
    /// `Settings` record carries `retain_sets = 1` and wins like every other
    /// replayed field — it does NOT keep the current value, which would make
    /// the result depend on how this node reached its state.
    #[test]
    fn a_replayed_pre_catalog_settings_record_sets_retention_to_one() {
        let mut f = fsm();
        let cmd = settings_with_retain(&f, 3);
        assert_eq!(apply_at(&mut f, 100, &cmd), 0);
        let mut old = f.state().settings;
        old.admission_bytes = 12345;
        let mut out = Vec::new();
        let mut ctx = ApplyCtx::new(200, ClusterFsm::IDENTITY);
        f.apply(&mut ctx, &v2_settings_body(old), &mut out);
        assert_eq!(out, vec![0]);
        assert_eq!(f.state().settings.admission_bytes, 12345);
        assert_eq!(f.state().retain_sets(), 1);
        assert_eq!(f.state().settings.retain_sets, 1);
        // A crafted v3 `0` (the door refuses an operator's) reads as 1 too.
        let mut zero = f.state().settings;
        zero.retain_sets = 0;
        assert_eq!(apply_at(&mut f, 300, &ClusterCommand::Settings(zero)), 0);
        assert_eq!(f.state().settings.retain_sets, 1);
    }

    #[test]
    fn a_report_for_a_position_with_no_entry_is_ignored() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(4096, false, 1);
        let before = f.state().catalog.clone();
        assert_eq!(apply_at(&mut f, 4200, &report(0, 5000, &[(0, 7, 0)])), 0);
        assert_eq!(
            apply_at(&mut f, 4300, &report(CLUSTER_ROW, 5000, &[(0, 7, 0)])),
            0
        );
        assert_eq!(f.state().catalog, before);
        // The per-row diagnostic matrix still holds the row-0 report.
        assert_eq!(f.state().report_for(0).map(|r| r.position), Some(5000));
    }

    #[test]
    fn a_cluster_row_report_does_not_enter_the_per_row_reports() {
        let mut f = fsm();
        f.on_snapshot_frame(4096, false, 1);
        assert_eq!(
            apply_at(&mut f, 4200, &report(CLUSTER_ROW, 4096, &[(0, 9, 0)])),
            0
        );
        assert!(f.state().reports.is_empty());
        assert_eq!(f.state().report_for(CLUSTER_ROW), None);
        assert_eq!(f.state().catalog[0].cluster.verdict, RowVerdict::Agreed);
    }

    #[test]
    fn a_standby_set_agrees_exactly_like_a_full_one() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(4096, true, 1);
        assert_eq!(
            apply_at(&mut f, 4200, &report(0, 4096, &[(3, 7, 0), (4, 7, 0)])),
            0
        );
        assert_eq!(
            apply_at(
                &mut f,
                4300,
                &report(CLUSTER_ROW, 4096, &[(3, 9, 0), (4, 9, 0)])
            ),
            0
        );
        let e = &f.state().catalog[0];
        assert_eq!((e.kind, e.state), (SetKind::Standby, SetState::Complete));
        assert!(e.is_agreed());
        assert_eq!(f.state().newest_agreed_at_most(u64::MAX), Some(4096));
    }

    #[test]
    fn retention_removes_a_younger_unpinned_set_past_a_pinned_origin() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        agreed_set(&mut f, 1000, 1100);
        // Both younger sets are commanded below the pin record, so neither
        // completes the pin (ruling C1) and the origin stays protected.
        f.on_snapshot_frame(2000, false, 2000);
        f.on_snapshot_frame(3000, false, 3000);
        assert_eq!(
            apply_at(
                &mut f,
                3050,
                &pin(0, pack_version(1, 0, 0), pack_version(1, 1, 0), 1000)
            ),
            0
        );
        agree(&mut f, 2000, 3100);
        assert_eq!(
            positions(&f),
            vec![1000, 2000, 3000],
            "2000 is the newest agreed"
        );
        agree(&mut f, 3000, 3200);
        assert_eq!(positions(&f), vec![1000, 3000]);
    }

    // ------------------------------------------------- final fix wave (C1/I1)

    /// A REAL pre-flag-day (layout v3) image of `f`'s state whose settings
    /// tail is the 33 B **v2** record a `0.10.0` node wrote — not a v3 record
    /// inside a v3 image. Provenance: `f.freeze()` (v4), re-sealed with the
    /// settings record truncated to `SETTINGS_LEN_V2` and its version word
    /// set to 2, the empty catalog blob's zero length prefix dropped, the
    /// layout word set to 3 and the CRC recomputed.
    fn v3_image_with_v2_settings(f: &ClusterFsm) -> Vec<u8> {
        let (v4, _) = f.freeze().unwrap();
        let parts = decode_cluster_image(&v4).expect("own image decodes");
        assert!(parts.catalog.is_empty(), "a v3 image carries no catalog");
        let mut s = parts.settings.to_vec();
        s.truncate(uc_protocol::v2::settings::SETTINGS_LEN_V2);
        s[0..4].copy_from_slice(&2u32.to_le_bytes());
        let mut img = Vec::new();
        encode_cluster_image(
            &ClusterImageParts {
                settings: &s,
                ..parts
            },
            &mut img,
        )
        .unwrap();
        img.truncate(img.len() - 4); // CRC
        let catalog_prefix = img.split_off(img.len() - 4);
        assert_eq!(catalog_prefix, 0u32.to_le_bytes(), "empty catalog blob");
        img[8..12].copy_from_slice(&3u32.to_le_bytes());
        let crc = crc32fast::hash(&img);
        img.extend_from_slice(&crc.to_le_bytes());
        img
    }

    /// The same `Settings` command as a pre-flag-day v2 record on the wire
    /// (33 B payload, version word 2) — what a journal replay of a `0.10.0`
    /// `uc2ctl settings apply` hands `apply`.
    fn v2_settings_body(s: Settings) -> Vec<u8> {
        let mut b = body(&ClusterCommand::Settings(s));
        b.truncate(CLUSTER_BODY_PREFIX_LEN + uc_protocol::v2::settings::SETTINGS_LEN_V2);
        b[CLUSTER_BODY_PREFIX_LEN..CLUSTER_BODY_PREFIX_LEN + 4]
            .copy_from_slice(&2u32.to_le_bytes());
        b
    }

    /// C1 (ruling R24): a node that installed a pre-flag-day v3 image whose
    /// settings tail is a v2 record and a node that reached the same
    /// position from genesis must hold the SAME cluster state — byte-equal
    /// images at every later instant — and both read `retain_sets = 1`.
    /// Before the fix the installed node carried `retain_sets = 0` forever
    /// (a replayed v2 record kept "the current value"), so the cluster row
    /// diverged on every instant.
    #[test]
    fn a_v3_image_with_a_v2_settings_tail_converges_with_genesis() {
        let mut a = fsm();
        genesis_row(&mut a, 0, 100);
        a.set_consumed(300);
        let v3 = v3_image_with_v2_settings(&a);
        let mut b = fsm();
        assert_eq!(b.install_snapshot(300, &mut &v3[..]).unwrap(), 300);
        for f in [&mut a, &mut b] {
            // A replayed pre-flag-day `settings apply`, then two instants.
            let mut s = f.state().settings;
            s.admission_bytes = 4 << 20;
            let mut out = Vec::new();
            let mut ctx = ApplyCtx::new(400, ClusterFsm::IDENTITY);
            f.apply(&mut ctx, &v2_settings_body(s), &mut out);
            assert_eq!(out, vec![0]);
            agreed_set(f, 1000, 1100);
            agreed_set(f, 2000, 2100);
            f.set_consumed(2200);
        }
        for f in [&a, &b] {
            assert_eq!(f.state().retain_sets(), 1);
            assert_eq!(f.state().settings.retain_sets, 1);
        }
        assert_eq!(
            a.freeze().unwrap().0,
            b.freeze().unwrap().0,
            "a v3-image node and a genesis node froze different cluster images"
        );
    }

    /// I1 (ruling R25): when an entry turns `Complete`, every OLDER
    /// `Commanded` entry that is not a pinned origin is dropped — so a node
    /// that replayed two pre-flag-day `SNAPSHOT` frames (never reported,
    /// never completing) and a node that did not converge on the first
    /// completion above them, agreed or not.
    #[test]
    fn stale_commanded_entries_drop_when_a_newer_set_completes() {
        let mut a = fsm();
        let mut b = fsm();
        for f in [&mut a, &mut b] {
            genesis_row(f, 0, 100);
        }
        a.on_snapshot_frame(500, false, 1);
        a.on_snapshot_frame(700, false, 2);
        assert_eq!(positions(&a), vec![500, 700]);
        for f in [&mut a, &mut b] {
            // P = 1000 completes DIVERGED (row 0's two hashes split 1–1–1):
            // Complete, not agreed — the stale entries must still go.
            f.on_snapshot_frame(1000, false, 3);
            assert_eq!(
                apply_at(
                    f,
                    1100,
                    &report(0, 1000, &[(0, 1, 0), (1, 2, 0), (2, 3, 0)])
                ),
                0
            );
            assert_eq!(
                apply_at(f, 1110, &report(CLUSTER_ROW, 1000, &[(0, 1, 0)])),
                0
            );
            f.set_consumed(1200);
        }
        assert_eq!(a.state().catalog[0..].len(), 1);
        assert_eq!(a.state().catalog, b.state().catalog);
        assert_eq!(a.freeze().unwrap().0, b.freeze().unwrap().0);
    }

    /// I1: a pinned origin is never the stale entry dropped.
    #[test]
    fn a_pinned_commanded_origin_survives_a_newer_completion() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(500, false, 1);
        assert_eq!(
            apply_at(
                &mut f,
                600,
                &pin(0, pack_version(1, 0, 0), pack_version(1, 1, 0), 500)
            ),
            0
        );
        f.on_snapshot_frame(700, false, 2);
        f.on_snapshot_frame(1000, false, 3);
        assert_eq!(
            apply_at(
                &mut f,
                1100,
                &report(0, 1000, &[(0, 1, 0), (1, 2, 0), (2, 3, 0)])
            ),
            0
        );
        assert_eq!(
            apply_at(&mut f, 1110, &report(CLUSTER_ROW, 1000, &[(0, 1, 0)])),
            0
        );
        assert_eq!(
            positions(&f),
            vec![500, 1000],
            "700 dropped, pinned 500 kept"
        );
    }

    /// Snapshot-lifecycle spec §7.2: the row entry records the size reported
    /// with the majority hash; the cluster row likewise.
    #[test]
    fn the_catalog_records_the_majority_hashs_size() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(4096, false, 1);
        assert_eq!(
            apply_at(
                &mut f,
                4200,
                &report(0, 4096, &[(0, 7, 40), (1, 7, 40), (2, 8, 99)])
            ),
            0
        );
        assert_eq!(
            apply_at(
                &mut f,
                4300,
                &report(CLUSTER_ROW, 4096, &[(0, 9, 300), (1, 9, 300), (2, 9, 300)])
            ),
            0
        );
        let e = &f.state().catalog[0];
        assert_eq!(
            (e.rows[0].verdict, e.rows[0].size),
            (RowVerdict::Diverged, 40)
        );
        assert_eq!(e.cluster.size, 300);
    }

    /// Snapshot-lifecycle spec §7.2: a v4 image (unsized report and catalog
    /// blobs) installs with every size 0 — no refusal, no wipe.
    #[test]
    fn a_v4_image_installs_with_every_size_zero() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(1000, false, 1);
        assert_eq!(apply_at(&mut f, 1100, &report(0, 1000, &[(0, 1, 40)])), 0);
        assert_eq!(
            apply_at(&mut f, 1110, &report(CLUSTER_ROW, 1000, &[(0, 2, 300)])),
            0
        );
        f.set_consumed(1200);
        let (img, _) = f.freeze().unwrap();
        let v4 = rewrite_image_as_v4(&img);
        let mut g = fsm();
        assert_eq!(g.install_snapshot(1200, &mut &v4[..]).unwrap(), 1200);
        let e = &g.state().catalog[0];
        assert_eq!((e.rows[0].hash, e.rows[0].size), (1, 0));
        assert_eq!((e.cluster.hash, e.cluster.size), (2, 0));
        assert_eq!(
            g.state().report_for(0).map(|r| r.hashes.clone()),
            Some(vec![(0, 1, 0)])
        );
        assert!(
            e.is_agreed(),
            "agreement survives the migration; only sizes are unknown"
        );
    }

    /// Review focus 4: the gauge word is 0 while the newest agreed set's size
    /// is unknown — a pre-lifecycle set never reads as "too big".
    #[test]
    fn the_newest_agreed_bytes_word_is_zero_when_any_size_is_unknown() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(1000, false, 1);
        assert_eq!(apply_at(&mut f, 1100, &report(0, 1000, &[(0, 1, 40)])), 0);
        assert_eq!(
            apply_at(&mut f, 1110, &report(CLUSTER_ROW, 1000, &[(0, 2, 0)])),
            0
        );
        let v = ClusterView::new(f.state());
        assert_eq!(v.catalog_newest_agreed_bytes.load(Ordering::Acquire), 0);
        f.on_snapshot_frame(2000, false, 2);
        assert_eq!(apply_at(&mut f, 2100, &report(0, 2000, &[(0, 1, 40)])), 0);
        assert_eq!(
            apply_at(&mut f, 2110, &report(CLUSTER_ROW, 2000, &[(0, 2, 300)])),
            0
        );
        v.publish(f.state());
        assert_eq!(v.catalog_newest_agreed_bytes.load(Ordering::Acquire), 340);
    }

    /// Task 3 review R1: `auto_fetch = false` is replicated state, so it
    /// must survive a freeze/install — a node that installed the image and
    /// the node that froze it then freeze byte-identical images (the
    /// divergence class catalog ruling R24 fixed for `retain_sets`).
    #[test]
    fn auto_fetch_false_survives_an_install_and_the_images_stay_byte_equal() {
        let mut a = fsm();
        genesis_row(&mut a, 0, 100);
        let mut s = a.state().settings;
        s.auto_fetch = false;
        assert_eq!(apply_at(&mut a, 200, &ClusterCommand::Settings(s)), 0);
        assert!(!a.state().settings.auto_fetch);
        a.set_consumed(300);
        let (img, _) = a.freeze().unwrap();
        let mut b = fsm();
        assert!(
            b.state().settings.auto_fetch,
            "a fresh FSM starts with auto_fetch on"
        );
        assert_eq!(b.install_snapshot(300, &mut &img[..]).unwrap(), 300);
        assert!(
            !b.state().settings.auto_fetch,
            "the install kept auto_fetch = false"
        );
        for f in [&mut a, &mut b] {
            agreed_set(f, 1000, 1100);
            f.set_consumed(1200);
        }
        assert!(!b.state().settings.auto_fetch);
        assert_eq!(
            a.freeze().unwrap().0,
            b.freeze().unwrap().0,
            "an installed node and the node that froze the image diverged"
        );
    }

    /// Ruling R2: a kind-5 record written before sizes existed (12 B
    /// entries) is replayed from the journal above the newest cluster
    /// artifact on a node that crossed the flag day, while another node
    /// installs a v4 image holding the same report. Same log, same state:
    /// both read every size 0 and freeze byte-identical images.
    #[test]
    fn an_unsized_report_frame_and_a_v4_image_install_converge() {
        fn unsized_report_body(cmd: &ClusterCommand) -> Vec<u8> {
            use uc_protocol::v2::upgrade::{
                SNAPSHOT_REPORT_ENTRY_LEN, SNAPSHOT_REPORT_ENTRY_LEN_UNSIZED,
                SNAPSHOT_REPORT_HEADER_LEN,
            };
            let b = body(cmd);
            let rec = &b[CLUSTER_BODY_PREFIX_LEN..];
            let mut old = b[..CLUSTER_BODY_PREFIX_LEN + SNAPSHOT_REPORT_HEADER_LEN].to_vec();
            for e in rec[SNAPSHOT_REPORT_HEADER_LEN..].chunks(SNAPSHOT_REPORT_ENTRY_LEN) {
                old.extend_from_slice(&e[..SNAPSHOT_REPORT_ENTRY_LEN_UNSIZED]);
            }
            old
        }
        let row0 = report(0, 1000, &[(0, 1, 40), (1, 1, 40)]);
        let cluster = report(CLUSTER_ROW, 1000, &[(0, 2, 300), (1, 2, 300)]);
        // Node A replays the pre-lifecycle frames.
        let mut a = fsm();
        genesis_row(&mut a, 0, 100);
        a.on_snapshot_frame(1000, false, 1);
        for (pos, cmd) in [(1100, &row0), (1110, &cluster)] {
            let mut out = Vec::new();
            let mut ctx = ApplyCtx::new(pos, ClusterFsm::IDENTITY);
            a.apply(&mut ctx, &unsized_report_body(cmd), &mut out);
            assert_eq!(out, vec![0], "a 12 B-entry kind-5 record applies");
        }
        a.set_consumed(1200);
        // Node B installs a v4 image of the same state.
        let mut c = fsm();
        genesis_row(&mut c, 0, 100);
        c.on_snapshot_frame(1000, false, 1);
        assert_eq!(apply_at(&mut c, 1100, &row0), 0);
        assert_eq!(apply_at(&mut c, 1110, &cluster), 0);
        c.set_consumed(1200);
        let v4 = rewrite_image_as_v4(&c.freeze().unwrap().0);
        let mut b = fsm();
        assert_eq!(b.install_snapshot(1200, &mut &v4[..]).unwrap(), 1200);
        assert_eq!(a.state().catalog, b.state().catalog);
        assert_eq!(a.state().reports, b.state().reports);
        assert_eq!(
            a.state().report_for(0).map(|r| r.hashes.clone()),
            Some(vec![(0, 1, 0), (1, 1, 0)])
        );
        assert_eq!(
            a.freeze().unwrap().0,
            b.freeze().unwrap().0,
            "a replaying node and an installing node froze different images"
        );
    }

    /// Re-frame a v5 image as v4: the report and catalog blobs back to their
    /// unsized widths, the version word to 4, the CRC recomputed.
    fn rewrite_image_as_v4(img: &[u8]) -> Vec<u8> {
        use uc_protocol::v2::catalog::{ROW_ENTRY_LEN, ROW_ENTRY_LEN_UNSIZED};
        use uc_protocol::v2::cluster_image::{decode_cluster_image, encode_cluster_image};
        use uc_protocol::v2::upgrade::{
            SNAPSHOT_REPORT_ENTRY_LEN, SNAPSHOT_REPORT_ENTRY_LEN_UNSIZED,
            SNAPSHOT_REPORT_HEADER_LEN,
        };
        let parts = decode_cluster_image(img).unwrap();
        let mut reports = Vec::new();
        let mut o = 0;
        while o < parts.reports.len() {
            let len = u32::from_le_bytes(parts.reports[o..o + 4].try_into().unwrap()) as usize;
            let rec = &parts.reports[o + 4..o + 4 + len];
            let mut old = rec[..SNAPSHOT_REPORT_HEADER_LEN].to_vec();
            for e in rec[SNAPSHOT_REPORT_HEADER_LEN..].chunks(SNAPSHOT_REPORT_ENTRY_LEN) {
                old.extend_from_slice(&e[..SNAPSHOT_REPORT_ENTRY_LEN_UNSIZED]);
            }
            reports.extend_from_slice(&(old.len() as u32).to_le_bytes());
            reports.extend_from_slice(&old);
            o += 4 + len;
        }
        let mut catalog = parts.catalog[..2].to_vec();
        for set in parts.catalog[2..].chunks(18 + 9 * ROW_ENTRY_LEN) {
            catalog.extend_from_slice(&set[..18]);
            for r in set[18..].chunks(ROW_ENTRY_LEN) {
                catalog.extend_from_slice(&r[..ROW_ENTRY_LEN_UNSIZED]);
            }
        }
        let mut out = Vec::new();
        encode_cluster_image(
            &uc_protocol::v2::cluster_image::ClusterImageParts {
                reports: &reports,
                catalog: &catalog,
                ..parts
            },
            &mut out,
        )
        .unwrap();
        let body_end = out.len() - 4;
        out.truncate(body_end);
        out[8..12].copy_from_slice(&4u32.to_le_bytes());
        let crc = crc32fast::hash(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }
}
