// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The replicated settings record (cluster-FSM spec §6): the three
//! cluster-wide policies that used to live per host in `node.toml`. Carried
//! as a `CLUSTER kind=Settings` payload and inside the cluster FSM's image.
//! `core`-only, like every codec in `v2`.

/// Encoding version, first word of the payload. Bumped when the layout
/// changes; a reader refuses any version it does not know.
pub const SETTINGS_VERSION: u32 = 1;
/// The exact encoded length — no trailing bytes are tolerated.
pub const SETTINGS_LEN: usize = 4 + 8 + 8 + 8 + 1;
/// `fsm_lag_bytes` value meaning lockstep — the cnc page's existing sentinel,
/// reused so the word the service apply loops read and the setting agree.
pub const FSM_LAG_LOCKSTEP: u64 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Target {
    All = 0,
    Learners = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    /// `0` at genesis = "derive `buffer_bytes / 4` at use"; `FSM_LAG_LOCKSTEP`
    /// once set explicitly means lockstep. The two zeros are distinguishable
    /// by `Settings::has_fsm_lag`, set by `settings apply`.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_layout_is_frozen() {
        // FROZEN once shipped (spec §6/§7): version u32 @0, fsm_lag_bytes u64 @4,
        // admission_bytes u64 @12, snapshot_interval_bytes u64 @20, target u8 @28.
        assert_eq!(SETTINGS_VERSION, 1);
        assert_eq!(SETTINGS_LEN, 29);
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
