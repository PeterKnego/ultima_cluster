// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The snapshot catalog (catalog spec §4, §5, §7): the replicated list of
//! AGREED/DIVERGED snapshot SETS the cluster FSM keeps, one [`SetEntry`] per
//! coordinated instant. This list rides inside the cluster IMAGE
//! (`v2::cluster_image`) as an opaque length-prefixed blob — it is NOT a
//! `CLUSTER` frame payload, which is why [`MAX_CATALOG_SETS`] is a
//! *retention* bound (how many sets the image keeps), not a datagram
//! ceiling (spec §4 "Sizing"). `core` + `alloc`, like its neighbours
//! `v2::schedule`, `v2::settings` and `v2::upgrade`.
//!
//! Layout. One [`RowEntry`] is 21 B: `version u32 ‖ hash u64 ‖ verdict u8 ‖
//! size u64`. One [`SetEntry`] is [`SET_ENTRY_LEN`] = 207 B: `position u64 @0
//! ‖ kind u8 @8 ‖ state u8 @9 ‖ time_ns u64 @10 ‖ rows[0..CNC_MAX_SERVICES]
//! (21 B each) @18 ‖ cluster (one more [`RowEntry`]) @186`. The list itself is
//! `count u16 ‖ count × SET_ENTRY_LEN`, nothing after — exact framing, like
//! every other list codec in this crate. A v1–v4 cluster image stored 13 B
//! row entries (135 B sets); [`decode_set_list_unsized`] reads those with
//! every `size` 0.
//!
//! A `Retiring` set state existed in an earlier draft and was dropped
//! before shipping (spec errata): a retired set simply LEAVES the list
//! rather than passing through a third state.

use crate::v2::cnc::CNC_MAX_SERVICES;

pub use super::upgrade::{CLUSTER_ROW, is_report_row};

/// Ruling R42: the suffix of the zero-byte marker
/// `snapshots/cluster/snap-<P>.foreign` that says the set at `P` was NOT
/// built on this node — it arrived whole through a snapshot session (a
/// below-floor install, or `uc2ctl snapshot fetch`). The receiver writes it
/// (and makes it durable) BEFORE it renames ANY of the session's artifacts
/// into place, so neither a complete set nor an aborted session's row copy is
/// ever unmarked; the node's retention
/// removes it with that artifact. A node never reports a marked set's hashes
/// as its own observation: a copy is not an independent builder.
pub const FOREIGN_SET_SUFFIX: &str = ".foreign";

/// Retention bound on the catalog's own list (spec §4 "Sizing") — NOT a
/// datagram ceiling, since the list rides the cluster image, never a
/// `CLUSTER` frame.
pub const MAX_CATALOG_SETS: usize = 64;
/// The largest `retain_sets` the cluster FSM's door accepts (rulings R8,
/// R29): `MAX_CATALOG_SETS` less sixteen entries of headroom. Since R21 a
/// pinned origin is kept IN ADDITION to `retain_sets` (up to one per row,
/// eight), and commanded instants still in flight need room beside them, so
/// a full complement of retained agreed sets plus eight pins plus eight
/// commanded entries still fits the image's list bound — `cap_catalog`
/// never has to evict a retained agreed set.
pub const MAX_RETAIN_SETS: u16 = (MAX_CATALOG_SETS - 16) as u16;

// R29: retained sets + one pinned origin per row + as many commanded
// entries again must fit the list bound.
const _: () = assert!(MAX_RETAIN_SETS as usize + 2 * CNC_MAX_SERVICES <= MAX_CATALOG_SETS);
/// `version u32 ‖ hash u64 ‖ verdict u8 ‖ size u64` (snapshot-lifecycle
/// spec §7.2 appended `size`).
pub const ROW_ENTRY_LEN: usize = 4 + 8 + 1 + 8; // 21
/// The row entry a v1–v4 cluster image stored, without `size`. Read only by
/// [`decode_set_list_unsized`].
pub const ROW_ENTRY_LEN_UNSIZED: usize = 4 + 8 + 1; // 13
/// `position u64 ‖ kind u8 ‖ state u8 ‖ time_ns u64 ‖ rows[0..CNC_MAX_SERVICES]
/// ‖ cluster` — `CNC_MAX_SERVICES` row entries plus one more for `cluster`,
/// i.e. `9 * ROW_ENTRY_LEN` since `CNC_MAX_SERVICES == 8`.
pub const SET_ENTRY_LEN: usize = 8 + 1 + 1 + 8 + 9 * ROW_ENTRY_LEN; // 207
/// The set entry a v1–v4 cluster image stored.
pub const SET_ENTRY_LEN_UNSIZED: usize = 8 + 1 + 1 + 8 + 9 * ROW_ENTRY_LEN_UNSIZED; // 135

const _: () = assert!(SET_ENTRY_LEN == 207);
const _: () = assert!(SET_ENTRY_LEN_UNSIZED == 135);
const _: () = assert!(CNC_MAX_SERVICES == 8);

/// Whether a coordinated instant froze every declared row and the cluster
/// FSM (`Full`), or only the learners (`Standby`, spec §5 "standby
/// instants") — mirrors `FLAG_SNAPSHOT_STANDBY` on the `SNAPSHOT` frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SetKind {
    Full = 0,
    Standby = 1,
}

/// A set's life cycle in the catalog. There is no `Retiring` state (spec
/// errata — an earlier draft had one): a retired set is removed from the
/// list outright rather than passing through a third state first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SetState {
    Commanded = 0,
    Complete = 1,
}

/// One row's (or the cluster artifact's) standing within a set, recomputed
/// from the reports the leader collected — never a field anyone writes
/// directly, mirroring `v2::upgrade::verdict`'s "the state holds only what
/// was observed" posture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum RowVerdict {
    #[default]
    Unreported = 0,
    Agreed = 1,
    Diverged = 2,
    NoMajority = 3,
}

/// One row's (or `cluster`'s) entry within a [`SetEntry`]: the version that
/// built the artifact, its hash, and the verdict over the reports collected
/// for it. `Default` is the all-`Unreported` entry — a row the leader has
/// not heard from yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RowEntry {
    pub version: u32,
    pub hash: u64,
    pub verdict: RowVerdict,
    /// Snapshot-lifecycle spec §7.2: the artifact's byte size reported with
    /// the majority hash; `0` = unknown (a set catalogued before sizes).
    pub size: u64,
}

/// One coordinated snapshot instant's catalog record: the position it
/// froze at, whether it was a full or standby instant, when it was
/// commanded, its life-cycle state, and one [`RowEntry`] per declared row
/// plus one for the cluster artifact itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetEntry {
    pub position: u64,
    pub kind: SetKind,
    pub time_ns: u64,
    pub state: SetState,
    pub rows: [RowEntry; CNC_MAX_SERVICES],
    pub cluster: RowEntry,
}

impl SetEntry {
    /// A freshly-commanded set: every row and the cluster artifact start
    /// `Unreported` ([`RowEntry::default`]), state [`SetState::Commanded`].
    pub fn commanded(position: u64, kind: SetKind, time_ns: u64) -> Self {
        SetEntry {
            position,
            kind,
            time_ns,
            state: SetState::Commanded,
            rows: [RowEntry::default(); CNC_MAX_SERVICES],
            cluster: RowEntry::default(),
        }
    }

    /// Agreement is FROZEN at completion (ruling R9): a `Complete` entry
    /// whose cluster artifact and every REPORTED row are `Agreed`. Which
    /// rows a set had to cover is judged once, when the entry turns
    /// `Complete` (against the declared rows at that report); a row
    /// declared LATER is `Unreported` in the entry and does not un-agree
    /// it — otherwise adding a row would retroactively empty the catalog
    /// and let retention drop a pinned origin. A `Commanded` entry is never
    /// agreed.
    /// Snapshot-lifecycle spec §7.2: the set's byte size — every REPORTED
    /// row's size plus the cluster artifact's. `0` = unknown: the cluster
    /// artifact is unreported, or any reported component's size is `0` (a set
    /// catalogued before sizes existed).
    pub fn total_size(&self) -> u64 {
        if self.cluster.verdict == RowVerdict::Unreported || self.cluster.size == 0 {
            return 0;
        }
        let mut total = self.cluster.size;
        for r in &self.rows {
            if r.verdict == RowVerdict::Unreported {
                continue;
            }
            if r.size == 0 {
                return 0;
            }
            total = total.saturating_add(r.size);
        }
        total
    }

    pub fn is_agreed(&self) -> bool {
        self.state == SetState::Complete
            && self.cluster.verdict == RowVerdict::Agreed
            && self
                .rows
                .iter()
                .all(|r| matches!(r.verdict, RowVerdict::Unreported | RowVerdict::Agreed))
    }
}

fn encode_row_entry(r: &RowEntry, out: &mut Vec<u8>) {
    out.extend_from_slice(&r.version.to_le_bytes());
    out.extend_from_slice(&r.hash.to_le_bytes());
    out.push(r.verdict as u8);
    out.extend_from_slice(&r.size.to_le_bytes());
}

fn decode_row_entry(buf: &[u8], sized: bool) -> Option<RowEntry> {
    let version = u32::from_le_bytes(buf.get(0..4)?.try_into().ok()?);
    let hash = u64::from_le_bytes(buf.get(4..12)?.try_into().ok()?);
    let verdict = match *buf.get(12)? {
        0 => RowVerdict::Unreported,
        1 => RowVerdict::Agreed,
        2 => RowVerdict::Diverged,
        3 => RowVerdict::NoMajority,
        _ => return None,
    };
    let size = if sized {
        u64::from_le_bytes(buf.get(13..21)?.try_into().ok()?)
    } else {
        0
    };
    Some(RowEntry {
        version,
        hash,
        verdict,
        size,
    })
}

fn encode_set_entry(s: &SetEntry, out: &mut Vec<u8>) {
    out.extend_from_slice(&s.position.to_le_bytes());
    out.push(s.kind as u8);
    out.push(s.state as u8);
    out.extend_from_slice(&s.time_ns.to_le_bytes());
    for r in &s.rows {
        encode_row_entry(r, out);
    }
    encode_row_entry(&s.cluster, out);
}

fn decode_set_entry(buf: &[u8], sized: bool) -> Option<SetEntry> {
    let (row_len, set_len) = if sized {
        (ROW_ENTRY_LEN, SET_ENTRY_LEN)
    } else {
        (ROW_ENTRY_LEN_UNSIZED, SET_ENTRY_LEN_UNSIZED)
    };
    if buf.len() != set_len {
        return None;
    }
    let position = u64::from_le_bytes(buf.get(0..8)?.try_into().ok()?);
    let kind = match *buf.get(8)? {
        0 => SetKind::Full,
        1 => SetKind::Standby,
        _ => return None,
    };
    let state = match *buf.get(9)? {
        0 => SetState::Commanded,
        1 => SetState::Complete,
        _ => return None,
    };
    let time_ns = u64::from_le_bytes(buf.get(10..18)?.try_into().ok()?);
    let mut rows = [RowEntry::default(); CNC_MAX_SERVICES];
    let mut o = 18;
    for row in &mut rows {
        *row = decode_row_entry(buf.get(o..o + row_len)?, sized)?;
        o += row_len;
    }
    let cluster = decode_row_entry(buf.get(o..o + row_len)?, sized)?;
    o += row_len;
    debug_assert_eq!(o, set_len);
    Some(SetEntry {
        position,
        kind,
        time_ns,
        state,
        rows,
        cluster,
    })
}

/// `None` if `sets.len() > MAX_CATALOG_SETS` — the retention bound, checked
/// before anything is written.
pub fn encode_set_list(sets: &[SetEntry], out: &mut Vec<u8>) -> Option<()> {
    if sets.len() > MAX_CATALOG_SETS {
        return None;
    }
    out.extend_from_slice(&(sets.len() as u16).to_le_bytes());
    for s in sets {
        encode_set_entry(s, out);
    }
    Some(())
}

/// Exact framing: `count u16 ‖ count × SET_ENTRY_LEN`, nothing after. A
/// count above [`MAX_CATALOG_SETS`] is refused (the encoder can never
/// produce one), and every per-entry enum byte is bounds-checked — an
/// unknown `kind`/`state`/`verdict` value refuses the whole list rather
/// than being silently coerced.
pub fn decode_set_list(buf: &[u8]) -> Option<Vec<SetEntry>> {
    decode_set_list_with(buf, true)
}

/// A v1–v4 cluster image's catalog blob: `count u16 ‖ count ×
/// SET_ENTRY_LEN_UNSIZED`, every `size` read as `0` (unknown).
pub fn decode_set_list_unsized(buf: &[u8]) -> Option<Vec<SetEntry>> {
    decode_set_list_with(buf, false)
}

fn decode_set_list_with(buf: &[u8], sized: bool) -> Option<Vec<SetEntry>> {
    let set_len = if sized {
        SET_ENTRY_LEN
    } else {
        SET_ENTRY_LEN_UNSIZED
    };
    let count = u16::from_le_bytes(buf.get(0..2)?.try_into().ok()?) as usize;
    if count > MAX_CATALOG_SETS || buf.len() != 2 + count * set_len {
        return None;
    }
    let mut out = Vec::with_capacity(count);
    let mut o = 2;
    for _ in 0..count {
        out.push(decode_set_entry(buf.get(o..o + set_len)?, sized)?);
        o += set_len;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_entry_layout_is_frozen() {
        let mut e = SetEntry::commanded(4096, SetKind::Standby, 77);
        e.rows[2] = RowEntry {
            version: 0x0102_0003,
            hash: 0xAB,
            verdict: RowVerdict::Agreed,
            size: 0,
        };
        e.cluster = RowEntry {
            version: 0,
            hash: 0xCD,
            verdict: RowVerdict::Diverged,
            size: 0,
        };
        let mut b = Vec::new();
        encode_set_list(&[e.clone()], &mut b).unwrap();
        assert_eq!(b.len(), 2 + SET_ENTRY_LEN);
        assert_eq!(&b[0..2], &1u16.to_le_bytes());
        assert_eq!(&b[2..10], &4096u64.to_le_bytes(), "position");
        assert_eq!(b[10], 1, "kind standby");
        assert_eq!(b[11], 0, "state commanded");
        assert_eq!(&b[12..20], &77u64.to_le_bytes(), "time_ns");
        let row2 = 20 + 2 * ROW_ENTRY_LEN;
        assert_eq!(&b[row2..row2 + 4], &0x0102_0003u32.to_le_bytes());
        assert_eq!(
            b[2 + SET_ENTRY_LEN - 9],
            2,
            "cluster verdict diverged precedes the 8-byte size"
        );
        assert_eq!(
            &b[2 + SET_ENTRY_LEN - 8..],
            &0u64.to_le_bytes(),
            "size is the last 8 bytes"
        );
        assert_eq!(decode_set_list(&b), Some(vec![e]));
    }

    #[test]
    fn the_list_is_bounded_and_exact() {
        let e = SetEntry::commanded(1, SetKind::Full, 0);
        let too_many = vec![e.clone(); MAX_CATALOG_SETS + 1];
        assert_eq!(encode_set_list(&too_many, &mut Vec::new()), None);
        let mut b = Vec::new();
        encode_set_list(&[e], &mut b).unwrap();
        b.push(0);
        assert_eq!(decode_set_list(&b), None, "trailing byte");
        assert_eq!(decode_set_list(&b[..b.len() - 2]), None, "short");
        let mut bad = b.clone();
        bad.pop();
        bad[11] = 9;
        assert_eq!(decode_set_list(&bad), None, "unknown state byte");
        assert_eq!(decode_set_list(&0u16.to_le_bytes()), Some(vec![]));
    }

    #[test]
    fn a_complete_entry_is_agreed_iff_its_cluster_and_every_reported_row_agree() {
        let mut e = SetEntry::commanded(1, SetKind::Full, 0);
        let ok = RowEntry {
            version: 1,
            hash: 1,
            verdict: RowVerdict::Agreed,
            size: 0,
        };
        e.rows[0] = ok;
        e.rows[1] = ok;
        e.cluster = ok;
        assert!(!e.is_agreed(), "a Commanded entry is never agreed");
        e.state = SetState::Complete;
        assert!(e.is_agreed());
        assert_eq!(e.rows[2].verdict, RowVerdict::Unreported);
        assert!(e.is_agreed(), "an Unreported row does not un-agree it");
        e.cluster.verdict = RowVerdict::Unreported;
        assert!(!e.is_agreed(), "the cluster artifact must agree");
        e.cluster = ok;
        e.rows[1].verdict = RowVerdict::Diverged;
        assert!(!e.is_agreed());
        e.rows[1].verdict = RowVerdict::NoMajority;
        assert!(!e.is_agreed());
    }

    #[test]
    fn max_retain_sets_leaves_sixteen_entries_of_headroom() {
        assert_eq!(MAX_RETAIN_SETS, 48);
        assert_eq!(MAX_RETAIN_SETS as usize + 16, MAX_CATALOG_SETS);
    }

    /// Snapshot-lifecycle spec §7.2: a row entry is 21 B and a set entry 207 B.
    #[test]
    fn row_entries_carry_sizes_and_the_set_entry_is_207_bytes() {
        assert_eq!((ROW_ENTRY_LEN, SET_ENTRY_LEN), (21, 207));
        let mut e = SetEntry::commanded(4096, SetKind::Full, 77);
        e.rows[0] = RowEntry {
            version: 1,
            hash: 0xAB,
            verdict: RowVerdict::Agreed,
            size: 0x0102_0304_0506_0708,
        };
        let mut b = Vec::new();
        encode_set_list(&[e.clone()], &mut b).unwrap();
        assert_eq!(b.len(), 2 + SET_ENTRY_LEN);
        let row0 = 2 + 18;
        assert_eq!(
            &b[row0 + 13..row0 + 21],
            &0x0102_0304_0506_0708u64.to_le_bytes(),
            "size @13 of a row entry"
        );
        assert_eq!(decode_set_list(&b), Some(vec![e]));
    }

    /// A v1-v4 image's catalog blob decodes through the unsized decoder with
    /// every size 0.
    #[test]
    fn an_unsized_set_list_decodes_with_every_size_zero() {
        let mut e = SetEntry::commanded(4096, SetKind::Standby, 5);
        e.state = SetState::Complete;
        e.rows[0] = RowEntry {
            version: 1,
            hash: 9,
            verdict: RowVerdict::Agreed,
            size: 777,
        };
        e.cluster = RowEntry {
            version: 0,
            hash: 3,
            verdict: RowVerdict::Agreed,
            size: 55,
        };
        let mut sized = Vec::new();
        encode_set_list(&[e.clone()], &mut sized).unwrap();
        let mut old = sized[..2 + 18].to_vec();
        for r in sized[2 + 18..].chunks(ROW_ENTRY_LEN) {
            old.extend_from_slice(&r[..ROW_ENTRY_LEN_UNSIZED]);
        }
        assert_eq!(old.len(), 2 + SET_ENTRY_LEN_UNSIZED);
        assert_eq!(
            decode_set_list(&old),
            None,
            "the live decoder refuses the old width"
        );
        let got = decode_set_list_unsized(&old).unwrap();
        assert_eq!(got[0].rows[0].size, 0);
        assert_eq!(got[0].cluster.size, 0);
        assert_eq!((got[0].rows[0].hash, got[0].cluster.hash), (9, 3));
        assert_eq!(got[0].total_size(), 0, "unknown");
    }

    /// Spec §7.2: a set's size is its reported rows' plus the cluster
    /// artifact's; any unknown (0) component makes the total unknown.
    #[test]
    fn total_size_sums_reported_rows_and_the_cluster_artifact() {
        let mut e = SetEntry::commanded(1, SetKind::Full, 0);
        e.state = SetState::Complete;
        e.rows[0] = RowEntry {
            version: 1,
            hash: 1,
            verdict: RowVerdict::Agreed,
            size: 100,
        };
        e.rows[3] = RowEntry {
            version: 1,
            hash: 1,
            verdict: RowVerdict::Agreed,
            size: 20,
        };
        e.cluster = RowEntry {
            version: 0,
            hash: 1,
            verdict: RowVerdict::Agreed,
            size: 3,
        };
        assert_eq!(e.total_size(), 123, "Unreported rows contribute nothing");
        e.rows[3].size = 0;
        assert_eq!(e.total_size(), 0, "one unknown row makes the set unknown");
        e.rows[3].size = 20;
        e.cluster.verdict = RowVerdict::Unreported;
        assert_eq!(e.total_size(), 0, "no cluster artifact report: unknown");
    }
}
