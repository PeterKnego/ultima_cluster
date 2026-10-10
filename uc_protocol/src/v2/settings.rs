// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The replicated settings record (cluster-FSM spec §6): the three
//! cluster-wide policies that used to live per host in `node.toml`. Carried
//! as a `CLUSTER kind=Settings` payload and inside the cluster FSM's image.
//! `core` + `alloc` (the encoder appends into a `Vec<u8>`), like `v2::schedule`.

/// Encoding version, first word of the payload. `4` since the snapshot-lifecycle
/// spec (`auto_fetch`); `1`–`3` decode with `auto_fetch = true`, the default.
/// `3` was the catalog spec
/// (`retain_sets`); a reader ACCEPTS `2` (the jumbo-flag-day shape, mapped to
/// `retain_sets = 1`) and `1` (the `2.11.0` shape, mapped to `datagram_mtu =
/// 0` and `retain_sets = 1`) because cluster artifacts and committed frames
/// written by earlier releases persist across the upgrade — catalog spec §7,
/// jumbo spec §5.5. `1` is the retention a pre-catalog cluster actually ran
/// (newest-only), and it is a function of the record alone, so a node that
/// installed an old image and a node that started from genesis read the
/// same value (ruling R24; a `0` here made their images diverge forever).
pub const SETTINGS_VERSION: u32 = 4;
/// The exact encoded length of a version-4 record — no trailing bytes.
pub const SETTINGS_LEN: usize = 4 + 8 + 8 + 8 + 1 + 4 + 2 + 1; // 36
/// The exact length of a version-3 record (`retain_sets`, no `auto_fetch`),
/// accepted on decode only.
pub const SETTINGS_LEN_V3: usize = 4 + 8 + 8 + 8 + 1 + 4 + 2; // 35
/// The exact length of a version-2 record, accepted on decode only.
pub const SETTINGS_LEN_V2: usize = 4 + 8 + 8 + 8 + 1 + 4; // 33
/// The exact length of a version-1 record, accepted on decode only.
pub const SETTINGS_LEN_V1: usize = 4 + 8 + 8 + 8 + 1; // 29
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
/// This is the CLUSTER-WIDE bound, floored to the baseline rung, so it must
/// hold on every host at the WORST case: it is the largest aligned frame the
/// UDP data plane can carry at the `MTU_DEFAULT` baseline rung
/// (`MTU_DEFAULT - DATAGRAM_HEADER_LEN`, floored to `FRAME_ALIGNMENT`) =
/// 1376 B. A host's own one-frame floor at the point of use
/// (`uc_node::services::fsm_lag_from_setting`) now clamps against that host's
/// LIVE discovered `payload_ceiling` (jumbo-frame MTU discovery, spec §7.2),
/// not a fixed `max_payload` — on a jumbo cluster that live ceiling can be
/// larger than this constant, so the per-host clamp can go UP relative to it
/// (up to 8928 B at the top rung), never down. This constant is only what the
/// leader's pre-append `validate` refuses BELOW, so an operator is told
/// rather than silently clamped.
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
    /// Jumbo spec §5.5: the committed datagram rung (`uc_protocol::v2::
    /// datagram::RUNGS`), `0` = the baseline `MTU_DEFAULT`. Leader-owned:
    /// written by discovery, never by an operator file; the FSM keeps it
    /// monotone (`max(committed, incoming)`).
    pub datagram_mtu: u32,
    /// Catalog spec §4.4: how many AGREED snapshot sets the cluster keeps,
    /// pinned origins kept in addition (ruling R21). A v1/v2 record decodes
    /// as `1` — today's newest-only retention (ruling R24).
    /// `1..=MAX_RETAIN_SETS`; the leader's door refuses `0` and anything
    /// above (47), apply refuses only the latter, and the cluster FSM
    /// normalises any `0` that still arrives to `1`.
    pub retain_sets: u16,
    /// Snapshot-lifecycle spec §6: every node keeps its own copy of the
    /// newest agreed snapshot set by fetching it in the background. `true`
    /// by default and for every v1–v3 record. Off, a voter on a learner-only
    /// cluster holds no set and never purges (spec §6, the documented trade).
    pub auto_fetch: bool,
}

impl Settings {
    pub const fn genesis_default() -> Settings {
        Settings {
            fsm_lag_bytes: 0,
            admission_bytes: 0,
            snapshot_interval_bytes: 0,
            snapshot_target: Target::All,
            datagram_mtu: 0,
            // Catalog spec errata (2026-10-04): genesis seeds `1` — today's
            // newest-only retention — because the FSM's door refuses `0`.
            // A v1/v2 record decodes as `1` too (ruling R24).
            retain_sets: 1,
            auto_fetch: true,
        }
    }
}

pub fn encode_settings(s: &Settings, out: &mut Vec<u8>) {
    out.extend_from_slice(&SETTINGS_VERSION.to_le_bytes());
    out.extend_from_slice(&s.fsm_lag_bytes.to_le_bytes());
    out.extend_from_slice(&s.admission_bytes.to_le_bytes());
    out.extend_from_slice(&s.snapshot_interval_bytes.to_le_bytes());
    out.push(s.snapshot_target as u8);
    out.extend_from_slice(&s.datagram_mtu.to_le_bytes());
    out.extend_from_slice(&s.retain_sets.to_le_bytes());
    out.push(s.auto_fetch as u8);
}

/// Total: `None` on any (version, length) pair other than the four this
/// reader knows — `(1, SETTINGS_LEN_V1)`, `(2, SETTINGS_LEN_V2)`,
/// `(3, SETTINGS_LEN_V3)` and `(4, SETTINGS_LEN)` — an unknown target byte,
/// or an `auto_fetch` byte other than 0/1. The length is EXACT PER
/// VERSION, so a header with another version's length (or the reverse) is
/// refused rather than read as a prefix; the pair check up front makes every
/// subsequent slice index infallible.
pub fn decode_settings(buf: &[u8]) -> Option<Settings> {
    if buf.len() < 4 {
        return None;
    }
    let version = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let (datagram_mtu, retain_sets, auto_fetch) = match (version, buf.len()) {
        (1, SETTINGS_LEN_V1) => (0, 1, true),
        (2, SETTINGS_LEN_V2) => (u32::from_le_bytes(buf[29..33].try_into().unwrap()), 1, true),
        (3, SETTINGS_LEN_V3) => (
            u32::from_le_bytes(buf[29..33].try_into().unwrap()),
            u16::from_le_bytes(buf[33..35].try_into().unwrap()),
            true,
        ),
        (4, SETTINGS_LEN) => (
            u32::from_le_bytes(buf[29..33].try_into().unwrap()),
            u16::from_le_bytes(buf[33..35].try_into().unwrap()),
            match buf[35] {
                0 => false,
                1 => true,
                _ => return None,
            },
        ),
        _ => return None,
    };
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
        datagram_mtu,
        retain_sets,
        auto_fetch,
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
        // FROZEN once shipped (cluster-FSM spec §6/§7, jumbo spec §5.5,
        // catalog spec §7): version u32 @0, fsm_lag_bytes u64 @4,
        // admission_bytes u64 @12, snapshot_interval_bytes u64 @20, target
        // u8 @28, datagram_mtu u32 @29, retain_sets u16 @33, auto_fetch u8 @35.
        assert_eq!(SETTINGS_VERSION, 4);
        assert_eq!(SETTINGS_LEN, 36);
        assert_eq!(SETTINGS_LEN_V3, 35);
        assert_eq!(SETTINGS_LEN_V2, 33);
        assert_eq!(SETTINGS_LEN_V1, 29);
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
            datagram_mtu: 8960,
            retain_sets: 3,
            auto_fetch: false,
        };
        let mut out = Vec::new();
        encode_settings(&s, &mut out);
        assert_eq!(out.len(), SETTINGS_LEN);
        assert_eq!(&out[0..4], &4u32.to_le_bytes());
        assert_eq!(out[28], 1);
        assert_eq!(&out[29..33], &8960u32.to_le_bytes());
        assert_eq!(&out[33..35], &3u16.to_le_bytes());
        assert_eq!(out[35], 0);
        assert_eq!(decode_settings(&out), Some(s));
    }

    /// Catalog spec §7: `retain_sets` round-trips on a v3 record; a v2
    /// record (one word shorter, version 2) decodes with retain_sets = 1 —
    /// today's newest-only retention (ruling R24); a v3 HEADER on a v2
    /// LENGTH is refused — the length is exact per version, so a header
    /// cannot borrow another version's size.
    #[test]
    fn settings_v3_round_trips_retain_sets_and_v2_reads_as_one() {
        let s = Settings {
            fsm_lag_bytes: 0,
            admission_bytes: 0,
            snapshot_interval_bytes: 0,
            snapshot_target: Target::All,
            datagram_mtu: 0,
            retain_sets: 3,
            auto_fetch: true,
        };
        let mut b = Vec::new();
        encode_settings(&s, &mut b);
        assert_eq!(b.len(), SETTINGS_LEN);
        assert_eq!(&b[0..4], &4u32.to_le_bytes(), "version 4");
        assert_eq!(&b[33..35], &3u16.to_le_bytes(), "retain_sets @33");
        assert_eq!(decode_settings(&b), Some(s));
        // the same fields as a v3 record (35 B, version 3) round-trip
        let mut v3 = b[..SETTINGS_LEN_V3].to_vec();
        v3[0..4].copy_from_slice(&3u32.to_le_bytes());
        assert_eq!(decode_settings(&v3), Some(s));
        // a v2 record (33 B, version 2) decodes with retain_sets = 1
        let mut v2 = b[..33].to_vec();
        v2[0..4].copy_from_slice(&2u32.to_le_bytes());
        assert_eq!(decode_settings(&v2).map(|s| s.retain_sets), Some(1));
        // a v3 header on a v2 length is refused
        let mut bad = b[..33].to_vec();
        bad[0..4].copy_from_slice(&3u32.to_le_bytes());
        assert_eq!(decode_settings(&bad), None);
    }

    /// Jumbo spec §5.5: the first flag day in which a cluster artifact and
    /// committed CLUSTER frames persist across the upgrade. A v1 record
    /// decodes, with the new field at its baseline meaning.
    #[test]
    fn a_version_1_record_decodes_with_datagram_mtu_zero() {
        let mut v1 = Vec::new();
        v1.extend_from_slice(&1u32.to_le_bytes());
        v1.extend_from_slice(&(16u64 << 20).to_le_bytes());
        v1.extend_from_slice(&(4u64 << 20).to_le_bytes());
        v1.extend_from_slice(&(1u64 << 30).to_le_bytes());
        v1.push(1);
        assert_eq!(v1.len(), SETTINGS_LEN_V1);
        let s = decode_settings(&v1).expect("v1 decodes");
        assert_eq!(s.datagram_mtu, 0);
        assert_eq!(s.retain_sets, 1, "ruling R24: a v1 record reads as 1");
        assert_eq!(s.fsm_lag_bytes, 16 << 20);
        assert_eq!(s.snapshot_target, Target::Learners);
        // A v1 header with a v2 length (or the reverse) is refused: the
        // length is exact per version.
        v1.extend_from_slice(&[0, 0, 0, 0]);
        assert!(decode_settings(&v1).is_none());
        let mut short_v3 = Vec::new();
        encode_settings(&Settings::genesis_default(), &mut short_v3);
        short_v3.truncate(SETTINGS_LEN_V1);
        assert!(decode_settings(&short_v3).is_none());
    }

    /// Snapshot-lifecycle spec §6: v4 appends `auto_fetch u8 @35` (0/1, any
    /// other byte refused); v1–v3 records decode with `auto_fetch = true`.
    #[test]
    fn settings_v4_round_trips_auto_fetch_and_v1_to_v3_read_true() {
        assert_eq!(
            (SETTINGS_VERSION, SETTINGS_LEN, SETTINGS_LEN_V3),
            (4, 36, 35)
        );
        assert!(Settings::genesis_default().auto_fetch, "default on");
        let s = Settings {
            auto_fetch: false,
            ..Settings::genesis_default()
        };
        let mut b = Vec::new();
        encode_settings(&s, &mut b);
        assert_eq!(b.len(), SETTINGS_LEN);
        assert_eq!(&b[0..4], &4u32.to_le_bytes());
        assert_eq!(b[35], 0, "auto_fetch @35");
        assert_eq!(decode_settings(&b), Some(s));
        let mut v3 = b[..SETTINGS_LEN_V3].to_vec();
        v3[0..4].copy_from_slice(&3u32.to_le_bytes());
        assert_eq!(decode_settings(&v3).map(|s| s.auto_fetch), Some(true));
        let mut v2 = b[..SETTINGS_LEN_V2].to_vec();
        v2[0..4].copy_from_slice(&2u32.to_le_bytes());
        assert_eq!(decode_settings(&v2).map(|s| s.auto_fetch), Some(true));
        let mut v1 = b[..SETTINGS_LEN_V1].to_vec();
        v1[0..4].copy_from_slice(&1u32.to_le_bytes());
        assert_eq!(decode_settings(&v1).map(|s| s.auto_fetch), Some(true));
        let mut bad = b.clone();
        bad[35] = 2;
        assert_eq!(
            decode_settings(&bad),
            None,
            "auto_fetch byte must be 0 or 1"
        );
        let mut short = b[..SETTINGS_LEN_V3].to_vec();
        short[0..4].copy_from_slice(&4u32.to_le_bytes());
        assert_eq!(decode_settings(&short), None, "a v4 header on a v3 length");
    }

    #[test]
    fn decode_is_total_and_refuses_unknown_version_and_target() {
        assert!(decode_settings(&[]).is_none());
        let mut out = Vec::new();
        encode_settings(&Settings::genesis_default(), &mut out);
        // `1`..=`4` are all REAL versions now, so the unknown-version
        // case is `5`.
        let mut v5 = out.clone();
        v5[0] = 5;
        assert!(decode_settings(&v5).is_none());
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
        assert_eq!(d.retain_sets, 1, "genesis seeds newest-only retention");
        assert_eq!(d.datagram_mtu, 0);
    }
}
