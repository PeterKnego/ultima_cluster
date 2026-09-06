// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The replicated settings record (cluster-FSM spec §6): the three
//! cluster-wide policies that used to live per host in `node.toml`. Carried
//! as a `CLUSTER kind=Settings` payload and inside the cluster FSM's image.
//! `core` + `alloc` (the encoder appends into a `Vec<u8>`), like `v2::schedule`.

/// Encoding version, first word of the payload. Bumped when the layout
/// changes; a reader refuses any version it does not know.
pub const SETTINGS_VERSION: u32 = 1;
/// The exact encoded length — no trailing bytes are tolerated.
pub const SETTINGS_LEN: usize = 4 + 8 + 8 + 8 + 1;
/// `fsm_lag_bytes` value meaning lockstep.
///
/// NOT the cnc page's `0` sentinel: in THIS record `0` already means "derive
/// the default at use" (`buffer_bytes / 4`), and one word cannot carry both
/// meanings. `u64::MAX` is the sentinel here — no real byte bound can reach
/// it (`fsm_lag_from_setting` clamps every finite value below half the ring),
/// so the two readings stay disjoint. `uc_node::services::page_lag_from_setting`
/// is the one place that maps it back onto the page's `0`.
pub const FSM_LAG_LOCKSTEP: u64 = u64::MAX;

/// The smallest byte bound `fsm_lag_bytes` may name: **one max-size frame**,
/// as the transport bounds it.
///
/// A lag below one frame is not a tighter policy, it is a WEDGE. The report
/// ceiling is `min_applied + lag` (`uc_node::services::report_ceiling`) and a
/// log follower refuses to yield a frame whose END exceeds that head, so a
/// sub-frame lag can pin the ceiling permanently inside the next frame:
/// nothing applies, `min_applied` never moves, commit never moves — and the
/// only way to change a replicated setting is a `CLUSTER` frame that has to
/// COMMIT. Operators who want the tightest possible pacing want
/// [`FSM_LAG_LOCKSTEP`], which is a barrier rather than a byte bound.
///
/// This is the CLUSTER-WIDE bound, so it must hold on every host and cannot
/// read any host's `max_payload`: it is the largest aligned frame the UDP
/// data plane can carry at all (`MTU_DEFAULT - DATAGRAM_HEADER_LEN`, floored
/// to `FRAME_ALIGNMENT`) = 1376 B. A host whose own `max_payload` is smaller
/// clamps up to its own one-frame floor at the point of use
/// (`uc_node::services::fsm_lag_from_setting`), per spec §4.4; this constant
/// is only what the leader's pre-append `validate` refuses BELOW, so an
/// operator is told rather than silently clamped.
pub const MIN_FSM_LAG_BYTES: u64 = ((crate::v2::datagram::MTU_DEFAULT
    - crate::v2::datagram::DATAGRAM_HEADER_LEN)
    & !(crate::v2::frame::FRAME_ALIGNMENT - 1)) as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Target {
    All = 0,
    Learners = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    /// `0` = "derive `buffer_bytes / 4` at use"; [`FSM_LAG_LOCKSTEP`]
    /// (`u64::MAX`) = lockstep; anything else is a byte bound, clamped at use
    /// into the range from one max-size frame up to half the ring — and a
    /// bound below [`MIN_FSM_LAG_BYTES`] is refused at the leader's door
    /// (`47 settings_bounds`) rather than silently clamped, because adopting
    /// one would wedge commit cluster-wide and permanently.
    pub fsm_lag_bytes: u64,
    /// `0` = derive at use (the node's `NodeConfig::admission_bytes` default).
    pub admission_bytes: u64,
    /// `0` = on demand only (plan 2 reads it; plan 1 carries it).
    pub snapshot_interval_bytes: u64,
    pub snapshot_target: Target,
}

impl Settings {
    pub const fn genesis_default() -> Settings {
        Settings {
            fsm_lag_bytes: 0,
            admission_bytes: 0,
            snapshot_interval_bytes: 0,
            snapshot_target: Target::All,
        }
    }
}

pub fn encode_settings(s: &Settings, out: &mut Vec<u8>) {
    out.extend_from_slice(&SETTINGS_VERSION.to_le_bytes());
    out.extend_from_slice(&s.fsm_lag_bytes.to_le_bytes());
    out.extend_from_slice(&s.admission_bytes.to_le_bytes());
    out.extend_from_slice(&s.snapshot_interval_bytes.to_le_bytes());
    out.push(s.snapshot_target as u8);
}

/// Total: `None` on any length other than [`SETTINGS_LEN`], an unknown
/// version, or an unknown target byte. The exact-length check up front makes
/// every subsequent slice index infallible.
pub fn decode_settings(buf: &[u8]) -> Option<Settings> {
    if buf.len() != SETTINGS_LEN {
        return None;
    }
    let version = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if version != SETTINGS_VERSION {
        return None;
    }
    let fsm_lag_bytes = u64::from_le_bytes(buf[4..12].try_into().unwrap());
    let admission_bytes = u64::from_le_bytes(buf[12..20].try_into().unwrap());
    let snapshot_interval_bytes = u64::from_le_bytes(buf[20..28].try_into().unwrap());
    let snapshot_target = match buf[28] {
        0 => Target::All,
        1 => Target::Learners,
        _ => return None,
    };
    Some(Settings {
        fsm_lag_bytes,
        admission_bytes,
        snapshot_interval_bytes,
        snapshot_target,
    })
}

const _: () = assert!(SETTINGS_LEN + crate::v2::frame::CLUSTER_BODY_PREFIX_LEN <= 1344);
// The lockstep sentinel must stay clear of the byte-bound floor, or
// `ClusterFsm::validate` would refuse lockstep as a sub-frame bound.
const _: () = assert!(FSM_LAG_LOCKSTEP > MIN_FSM_LAG_BYTES);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_layout_is_frozen() {
        // FROZEN once shipped (spec §6/§7): version u32 @0, fsm_lag_bytes u64 @4,
        // admission_bytes u64 @12, snapshot_interval_bytes u64 @20, target u8 @28.
        assert_eq!(SETTINGS_VERSION, 1);
        assert_eq!(SETTINGS_LEN, 29);
        // The lockstep sentinel is a WORD VALUE in this record, not the cnc
        // page's `0` — `0` here is already "derive the default at use".
        assert_eq!(FSM_LAG_LOCKSTEP, u64::MAX);
        // One max-size frame on the widest path the transport allows: 1408
        // MTU - 16 datagram header = 1392, floored to the 32-byte frame
        // alignment. Pinned so a reader can check the arithmetic without
        // recomputing it, and so a change to either input is a test failure
        // rather than a silent widening of the settings door.
        assert_eq!(MIN_FSM_LAG_BYTES, 1376);
        let s = Settings {
            fsm_lag_bytes: 16 << 20,
            admission_bytes: 4 << 20,
            snapshot_interval_bytes: 1 << 30,
            snapshot_target: Target::Learners,
        };
        let mut out = Vec::new();
        encode_settings(&s, &mut out);
        assert_eq!(out.len(), SETTINGS_LEN);
        assert_eq!(&out[0..4], &1u32.to_le_bytes());
        assert_eq!(out[28], 1);
        assert_eq!(decode_settings(&out), Some(s));
    }

    #[test]
    fn decode_is_total_and_refuses_unknown_version_and_target() {
        assert!(decode_settings(&[]).is_none());
        let mut out = Vec::new();
        encode_settings(&Settings::genesis_default(), &mut out);
        let mut v2 = out.clone();
        v2[0] = 2;
        assert!(decode_settings(&v2).is_none());
        let mut t9 = out.clone();
        t9[28] = 9;
        assert!(decode_settings(&t9).is_none());
        out.push(0); // trailing byte: refused, the length is exact
        assert!(decode_settings(&out).is_none());
    }

    #[test]
    fn genesis_default_means_derive_at_use() {
        let d = Settings::genesis_default();
        assert_eq!(d.fsm_lag_bytes, 0);
        assert_eq!(d.admission_bytes, 0);
        assert_eq!(d.snapshot_interval_bytes, 0);
        assert_eq!(d.snapshot_target, Target::All);
    }
}
