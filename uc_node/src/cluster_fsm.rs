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
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use uc_consensus::config::{ClusterConfig, ProposeError};
use uc_protocol::v2::config::{decode_config, encode_config};
use uc_protocol::v2::frame::{ClusterKind, read_cluster_prefix};
use uc_protocol::v2::schedule::{
    MAX_SCHEDULE_ENTRIES, ScheduleTable, decode_schedule_table, encode_schedule_table,
};
use uc_protocol::v2::settings::{SETTINGS_LEN, Settings, decode_settings, encode_settings};
use uc_service::{ApplyCtx, RawStateMachine, SnapshotError, SnapshotStateMachine};

use crate::node::{cluster_to_wire, wire_to_cluster_config};

pub const CLUSTER_IMAGE_MAGIC: &[u8; 8] = b"UCCLUST1";
pub const CLUSTER_IMAGE_VERSION: u32 = 1;

/// The staged table file an admin client writes under the instance directory
/// before sending `ADMIN_OP_SCHEDULE_APPLY`. Relative to `<instance_dir>`,
/// NOT to `state/` — it is a request payload, not durable node state, and the
/// node deletes it once the command is appended.
pub const SCHEDULE_PENDING_FILE: &str = "schedules.pending";
/// The same, for `ADMIN_OP_SETTINGS_APPLY` (spec §6).
pub const SETTINGS_PENDING_FILE: &str = "settings.pending";

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
    /// Frame-END position of the last CLUSTER command applied (accepted or
    /// refused) — the view's position tag and the artifact's position.
    pub applied: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterCommand {
    Membership(ClusterConfig),
    ScheduleTable(ScheduleTable),
    Settings(Settings),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterRefusal {
    Membership(ProposeError),
    ScheduleUnknownFsm { entry: usize },
    ScheduleTooLarge,
    SettingsBounds(&'static str),
}

impl ClusterRefusal {
    /// The same numbers the admin plane already speaks (uc2ctl.md's table).
    pub fn reason_code(&self) -> u32 {
        match self {
            ClusterRefusal::Membership(e) => ClusterConfig::reason_code(e),
            ClusterRefusal::ScheduleUnknownFsm { .. } => 43,
            ClusterRefusal::ScheduleTooLarge => 42,
            ClusterRefusal::SettingsBounds(_) => 47,
        }
    }
}

pub struct ClusterFsm {
    state: ClusterState,
    /// The declared rows' identity hashes, from `[services] names` — the
    /// only node-local input, fixed at boot, identical cluster-wide by the
    /// bootstrap boundary (spec §3.3).
    declared_hashes: Vec<u64>,
}

impl ClusterFsm {
    pub fn new(genesis: ClusterState, declared_hashes: Vec<u64>) -> ClusterFsm {
        ClusterFsm {
            state: genesis,
            declared_hashes,
        }
    }
    pub fn state(&self) -> &ClusterState {
        &self.state
    }

    /// Pure: the acceptance decision on THIS state, no mutation. The leader's
    /// pre-append check calls exactly this on a clone (spec §4.4).
    pub fn validate(&self, cmd: &ClusterCommand) -> Result<(), ClusterRefusal> {
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
                for (i, e) in t.entries.iter().enumerate() {
                    if !self.declared_hashes.contains(&e.identity_hash) {
                        return Err(ClusterRefusal::ScheduleUnknownFsm { entry: i });
                    }
                }
                Ok(())
            }
            ClusterCommand::Settings(s) => {
                if s.admission_bytes == u64::MAX {
                    return Err(ClusterRefusal::SettingsBounds("admission_bytes"));
                }
                if s.snapshot_interval_bytes == u64::MAX {
                    return Err(ClusterRefusal::SettingsBounds("snapshot.interval_bytes"));
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
        if let Err(r) = self.validate(&command) {
            out.push(r.reason_code() as u8);
            return;
        }
        match command {
            ClusterCommand::Membership(c) => self.state.membership = c,
            ClusterCommand::ScheduleTable(t) => {
                self.state.table = t;
                self.state.table_position = ctx.position;
            }
            ClusterCommand::Settings(s) => self.state.settings = s,
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
            _ => {}
        }
    }

    fn last_applied(&self) -> Option<u64> {
        (self.state.applied > 0).then_some(self.state.applied)
    }
}

/// The frozen image: magic ‖ version u32 ‖ applied u64 ‖ table_position u64
/// ‖ membership (u32 len ‖ encode_config) ‖ table (u32 len ‖
/// encode_schedule_table) ‖ settings (SETTINGS_LEN) ‖ crc32 of everything
/// before it.
pub type ClusterImage = Vec<u8>;

impl SnapshotStateMachine for ClusterFsm {
    type SnapshotHandle = ClusterImage;

    fn freeze(&self) -> Result<(ClusterImage, u64), SnapshotError> {
        let mut img = Vec::new();
        img.extend_from_slice(CLUSTER_IMAGE_MAGIC);
        img.extend_from_slice(&CLUSTER_IMAGE_VERSION.to_le_bytes());
        img.extend_from_slice(&self.state.applied.to_le_bytes());
        img.extend_from_slice(&self.state.table_position.to_le_bytes());
        let mut m = Vec::new();
        encode_config(&cluster_to_wire(&self.state.membership, 0), &mut m);
        img.extend_from_slice(&(m.len() as u32).to_le_bytes());
        img.extend_from_slice(&m);
        let mut t = Vec::new();
        encode_schedule_table(&self.state.table, &mut t);
        img.extend_from_slice(&(t.len() as u32).to_le_bytes());
        img.extend_from_slice(&t);
        encode_settings(&self.state.settings, &mut img);
        let crc = crc32fast::hash(&img);
        img.extend_from_slice(&crc.to_le_bytes());
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
        if img.len() < 8 + 4 + 8 + 8 + 4 + 4 + SETTINGS_LEN + 4 || &img[0..8] != CLUSTER_IMAGE_MAGIC
        {
            return Err(bad("cluster image magic"));
        }
        let (body, crc) = img.split_at(img.len() - 4);
        if crc32fast::hash(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
            return Err(bad("cluster image crc"));
        }
        // CRC32 is a public checksum, not a MAC: a crafted-or-corrupt body can
        // still match it, so every length-prefixed and fixed-width read below
        // is bounds-checked with `.get(..)` before slicing — no declared
        // length, however wrong, may panic (mirrors
        // `uc_protocol::v2::config::decode_config`'s check-before-index
        // posture).
        let u32_at = |o: usize| -> Result<u32, SnapshotError> {
            body.get(o..o + 4)
                .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
                .ok_or_else(|| bad("cluster image truncated"))
        };
        let u64_at = |o: usize| -> Result<u64, SnapshotError> {
            body.get(o..o + 8)
                .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                .ok_or_else(|| bad("cluster image truncated"))
        };
        let mut o = 8;
        if u32_at(o)? != CLUSTER_IMAGE_VERSION {
            return Err(bad("cluster image version"));
        }
        o += 4;
        let applied = u64_at(o)?;
        o += 8;
        if applied != position {
            return Err(bad("cluster image position"));
        }
        let table_position = u64_at(o)?;
        o += 8;
        let ml = u32_at(o)? as usize;
        o += 4;
        let m_bytes = o
            .checked_add(ml)
            .and_then(|end| body.get(o..end))
            .ok_or_else(|| bad("cluster image membership length"))?;
        let membership = wire_to_cluster_config(
            &decode_config(m_bytes).ok_or_else(|| bad("cluster image membership"))?,
        );
        o += ml;
        let tl = u32_at(o)? as usize;
        o += 4;
        let t_bytes = o
            .checked_add(tl)
            .and_then(|end| body.get(o..end))
            .ok_or_else(|| bad("cluster image table length"))?;
        let table = decode_schedule_table(t_bytes).ok_or_else(|| bad("cluster image table"))?;
        o += tl;
        // `decode_settings` is itself exact-length (no trailing bytes
        // tolerated), so require the remainder to be exactly `SETTINGS_LEN`
        // rather than handing it a slice that could run past `body`'s end.
        if o.checked_add(SETTINGS_LEN) != Some(body.len()) {
            return Err(bad("cluster image settings length"));
        }
        let settings = decode_settings(&body[o..]).ok_or_else(|| bad("cluster image settings"))?;
        self.state = ClusterState {
            membership,
            table,
            table_position,
            settings,
            applied,
        };
        Ok(applied)
    }
}

/// The position-tagged view the consensus agent reads (spec §4.5). Scalars
/// are atomics so the per-pass reads are one load each; the structured parts
/// sit behind a mutex taken only when `position` changed.
pub struct ClusterView {
    pub position: AtomicU64,
    pub admission_bytes: AtomicU64,
    pub fsm_lag_bytes: AtomicU64,
    pub snapshot_interval_bytes: AtomicU64,
    pub snapshot_target: AtomicU8,
    inner: Mutex<ClusterViewInner>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterViewInner {
    pub membership: ClusterConfig,
    pub table: ScheduleTable,
    pub table_position: u64,
}

impl ClusterView {
    pub fn new(genesis: &ClusterState) -> ClusterView {
        let v = ClusterView {
            position: AtomicU64::new(0),
            admission_bytes: AtomicU64::new(0),
            fsm_lag_bytes: AtomicU64::new(0),
            snapshot_interval_bytes: AtomicU64::new(0),
            snapshot_target: AtomicU8::new(0),
            inner: Mutex::new(ClusterViewInner {
                membership: genesis.membership.clone(),
                table: genesis.table.clone(),
                table_position: genesis.table_position,
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
        }
        self.admission_bytes
            .store(st.settings.admission_bytes, Ordering::Release);
        self.fsm_lag_bytes
            .store(st.settings.fsm_lag_bytes, Ordering::Release);
        self.snapshot_interval_bytes
            .store(st.settings.snapshot_interval_bytes, Ordering::Release);
        self.snapshot_target
            .store(st.settings.snapshot_target as u8, Ordering::Release);
        self.position.store(st.applied, Ordering::Release);
    }

    pub fn snapshot_inner(&self) -> ClusterViewInner {
        self.inner.lock().unwrap().clone()
    }

    /// The view as a [`ClusterState`] — the inner clone plus the four scalar
    /// atomics, with `applied` taken from `position`.
    ///
    /// This is what the leader's PRE-APPEND check runs `ClusterFsm::validate`
    /// against (spec §4.4, Ruling R5): the leader answers the admin request
    /// from the newest COMMITTED state it can see, so a command it accepts is
    /// one every replica's apply loop will also accept — and there is exactly
    /// ONE acceptance function, never a parallel node-side reimplementation of
    /// it. A read of the four atomics can straddle a concurrent `publish`
    /// (they are stored one at a time), which costs nothing here: the four are
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
            },
            applied: self.position.load(Ordering::Acquire),
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
            applied: 0,
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

    #[test]
    fn schedule_table_naming_an_undeclared_fsm_refuses_the_whole_table() {
        let mut f = fsm();
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
        let mut out = Vec::new();
        f.apply(
            &mut ApplyCtx::for_sm::<ClusterFsm>(300),
            &body(&cmd),
            &mut out,
        );
        assert_eq!(out, [43]);
        assert!(f.state().table.entries.is_empty());
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
        // table_position u64(8) | ml u32(4) | ... — overwrite `ml` with a
        // value far past the image's actual length, then recompute the CRC
        // so the tampered image still clears the checksum gate.
        const ML_OFFSET: usize = 8 + 4 + 8 + 8;
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
}
