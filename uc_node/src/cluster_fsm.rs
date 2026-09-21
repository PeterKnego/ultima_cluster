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
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

use uc_consensus::config::{ClusterConfig, ProposeError};
use uc_protocol::v2::cluster_image::{
    ClusterImageParts, decode_cluster_image, encode_cluster_image,
};
use uc_protocol::v2::config::{decode_config, encode_config};
use uc_protocol::v2::frame::{ClusterKind, read_cluster_prefix};
use uc_protocol::v2::schedule::{
    MAX_SCHEDULE_ENTRIES, ScheduleTable, decode_schedule_table, encode_schedule_table,
};
use uc_protocol::v2::settings::{
    FSM_LAG_LOCKSTEP, MIN_FSM_LAG_BYTES, Settings, decode_settings, encode_settings,
};
use uc_protocol::v2::upgrade::{
    SnapshotReport, UpgradePin, decode_pin_list, decode_report_list, decode_snapshot_report,
    decode_upgrade_pin, encode_pin_list, encode_report_list, encode_snapshot_report,
    encode_upgrade_pin,
};
use uc_service::{ApplyCtx, RawStateMachine, SnapshotError, SnapshotStateMachine};

use crate::node::{cluster_to_wire, wire_to_cluster_config};

/// Plan 3 (spec §4.8) moved the image codec itself to
/// `uc_protocol::v2::cluster_image` — a `core`-friendly leaf a fuzz target
/// can reach without `uc_node` — so a fuzz target can exercise the decoder
/// directly; re-exported here under their original names since nothing in
/// this crate's public surface should have to change to follow the move. See
/// [`uc_protocol::v2::cluster_image::CLUSTER_IMAGE_VERSION`] for the "why
/// still `1`" note.
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
}

impl ClusterState {
    /// The genesis state: a membership and a settings record, an empty
    /// schedule table, and nothing applied — what a node seeds the FSM with
    /// on a fresh instance directory (`node.toml`'s `[services]` members and
    /// `[settings]`), and what every offline reader hands
    /// [`crate::cluster_agent::recover`] before it overwrites it with the
    /// newest artifact. Both positions are `0`: neither record crossed the
    /// log.
    pub fn genesis(membership: ClusterConfig, settings: Settings) -> ClusterState {
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
        }
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

    /// Hold `r` as the row's report, replacing whatever that row held.
    fn put_report(&mut self, r: SnapshotReport) {
        match self.reports.iter().position(|q| q.row == r.row) {
            Some(i) => self.reports[i] = r,
            None => self.reports.push(r),
        }
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterRefusal {
    Membership(ProposeError),
    ScheduleUnknownFsm { entry: usize },
    ScheduleTooLarge,
    SettingsBounds(&'static str),
    PinFromMismatch,
    PinNotMonotone,
    ReportStale,
}

impl ClusterRefusal {
    /// The same numbers the admin plane already speaks (uc2ctl.md's table).
    ///
    /// The pin/report codes are the 52–59 band, plan B1; 52/54/56–58 are
    /// door-only and live in `uc_node::node`, so they never appear here.
    pub fn reason_code(&self) -> u32 {
        match self {
            ClusterRefusal::Membership(e) => ClusterConfig::reason_code(e),
            ClusterRefusal::ScheduleUnknownFsm { .. } => 43,
            ClusterRefusal::ScheduleTooLarge => 42,
            ClusterRefusal::SettingsBounds(_) => 47,
            ClusterRefusal::PinFromMismatch => 53,
            ClusterRefusal::PinNotMonotone => 55,
            ClusterRefusal::ReportStale => 59,
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
                Ok(())
            }
            ClusterCommand::UpgradePin(p) => {
                // Spec §2.5, replicated half only: the row's history is FSM
                // state. `pin_row_undeclared` (52), the no-pin half of
                // `pin_from_mismatch` (53, against the attached version
                // WORD) and `pin_no_set` (54, this leader's filesystem) are
                // node-local and stay at the door (`Consensus::apply_upgrade_pin`).
                if let Some(cur) = self.state.pin_for(p.row) {
                    if p.origin <= cur.origin {
                        return Err(ClusterRefusal::PinNotMonotone);
                    }
                    if p.from != cur.to {
                        return Err(ClusterRefusal::PinFromMismatch);
                    }
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
                self.state.settings = s;
                self.state.settings.datagram_mtu = keep;
                self.state.settings_position = ctx.position;
            }
            ClusterCommand::UpgradePin(p) => self.state.push_pin(p),
            ClusterCommand::SnapshotReport(r) => self.state.put_report(r),
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
/// decoder accepts a v1 or a v2 one, `SETTINGS_LEN_V1` or `SETTINGS_LEN`) ‖
/// pins (u32 len ‖ `encode_pin_list`) ‖ reports (u32 len ‖
/// `encode_report_list`) ‖ crc32 of everything before it. The two blobs are
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
        if parts.applied != position {
            return Err(bad("cluster image position"));
        }
        let membership = wire_to_cluster_config(
            &decode_config(parts.membership).ok_or_else(|| bad("cluster image membership"))?,
        );
        let table = decode_schedule_table(parts.table).ok_or_else(|| bad("cluster image table"))?;
        let settings =
            decode_settings(parts.settings).ok_or_else(|| bad("cluster image settings"))?;
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
        let reports =
            decode_report_list(parts.reports).ok_or_else(|| bad("cluster image reports"))?;
        self.state = ClusterState {
            membership,
            table,
            table_position: parts.table_position,
            settings,
            settings_position: parts.settings_position,
            applied: parts.applied,
            pins,
            reports,
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
            inner: Mutex::new(ClusterViewInner {
                membership: genesis.membership.clone(),
                table: genesis.table.clone(),
                table_position: genesis.table_position,
                pins: genesis.pins.clone(),
                reports: genesis.reports.clone(),
            }),
        };
        v.publish(genesis);
        v
    }

    /// Structured parts first, position LAST with Release, so a reader that
    /// sees the new position and then locks sees the new inner.
    pub fn publish(&self, st: &ClusterState) {
        {
            let mut g = self.inner.lock().unwrap();
            g.membership = st.membership.clone();
            g.table = st.table.clone();
            g.table_position = st.table_position;
            g.pins.clone_from(&st.pins);
            g.reports.clone_from(&st.reports);
        }
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

    pub fn snapshot_inner(&self) -> ClusterViewInner {
        self.inner.lock().unwrap().clone()
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
    pub fn report_position_for(&self, row: u8) -> Option<u64> {
        self.inner
            .lock()
            .unwrap()
            .reports
            .iter()
            .find(|r| r.row == row)
            .map(|r| r.position)
    }

    /// The view as a [`ClusterState`] — the inner clone plus the five scalar
    /// atomics, with `applied` taken from `position`.
    ///
    /// This is what the leader's PRE-APPEND check runs `ClusterFsm::validate`
    /// against (spec §4.4, Ruling R5): the leader answers the admin request
    /// from the newest COMMITTED state it can see, so a command it accepts is
    /// one every replica's apply loop will also accept — and there is exactly
    /// ONE acceptance function, never a parallel node-side reimplementation of
    /// it. A read of the five atomics can straddle a concurrent `publish`
    /// (they are stored one at a time), which costs nothing here: the five are
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
            },
            settings_position: self.settings_position.load(Ordering::Acquire),
            applied: self.position.load(Ordering::Acquire),
            pins: inner.pins,
            reports: inner.reports,
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
    use uc_protocol::v2::frame::{CLUSTER_BODY_PREFIX_LEN, write_cluster_prefix};
    use uc_protocol::v2::schedule::{ScheduleEntry, ScheduleRule};
    use uc_protocol::v2::upgrade::{
        SnapshotReport, UpgradePin, Verdict, decode_pin_list, decode_report_list, verdict,
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

    // ------------------------------------------------------ FSM upgrade lifecycle

    fn pin(row: u8, from: u32, to: u32, origin: u64) -> ClusterCommand {
        ClusterCommand::UpgradePin(UpgradePin {
            row,
            from,
            to,
            origin,
        })
    }
    fn report(row: u8, position: u64, hashes: &[(u32, u64)]) -> ClusterCommand {
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
        let mut f = fsm();
        assert_eq!(apply_at(&mut f, 100, &pin(0, 1, 2, 50)), 0);
        // 55: origin not above the row's current pin (equal, then below).
        assert_eq!(apply_at(&mut f, 200, &pin(0, 2, 3, 50)), 55);
        assert_eq!(apply_at(&mut f, 300, &pin(0, 2, 3, 40)), 55);
        // 53: `from` is not what the row's pin says it is at.
        assert_eq!(apply_at(&mut f, 400, &pin(0, 1, 3, 90)), 53);
        // A row with NO pin accepts any `from` here — that half of 53 is
        // the leader's door check against the attached version word.
        assert_eq!(apply_at(&mut f, 500, &pin(1, 42, 43, 90)), 0);
        assert_eq!(f.state().applied, 500, "a refusal still advances applied");
        assert_eq!(
            f.state().pin_for(0).map(|p| p.to),
            Some(2),
            "nothing changed on refusal"
        );
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
            apply_at(&mut f, 100, &report(0, 50, &[(0, 1), (1, 1), (2, 2)])),
            0
        );
        assert_eq!(f.state().report_for(0).map(|r| r.position), Some(50));
        assert_eq!(
            apply_at(&mut f, 200, &report(0, 40, &[(0, 1)])),
            59,
            "below the held position"
        );
        assert_eq!(
            apply_at(&mut f, 300, &report(0, 50, &[(0, 1), (1, 1)])),
            0,
            "equal replaces (a fuller vector for the same instant)"
        );
        assert_eq!(f.state().report_for(0).map(|r| r.hashes.len()), Some(2));
        assert_eq!(apply_at(&mut f, 400, &report(3, 10, &[(0, 9)])), 0);
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
        apply_at(&mut f, 200, &report(0, 50, &[(0, 1), (1, 2), (2, 2)]));
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

    #[test]
    fn the_view_publishes_pins_and_reports() {
        let mut f = fsm();
        apply_at(&mut f, 100, &pin(0, 1, 2, 50));
        apply_at(&mut f, 200, &report(0, 50, &[(0, 1)]));
        let v = ClusterView::new(&genesis());
        v.publish(f.state());
        let st = v.to_state();
        assert_eq!(st.pins, f.state().pins);
        assert_eq!(st.reports, f.state().reports);
        assert_eq!(v.position.load(Ordering::Acquire), 200);
    }
}
