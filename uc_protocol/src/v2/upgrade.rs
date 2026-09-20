// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! FSM upgrade lifecycle (spec §2.5, §6.5.2): the two `CLUSTER` payloads
//! the cluster FSM applies at commit, the list codecs the cluster IMAGE
//! embeds them in, and the pure `verdict` over a `SnapshotReport`.
//!
//! `core`-friendly like its siblings (`settings`, `schedule`): no I/O, no
//! allocation beyond the `Vec`s the callers hand in.

use crate::v2::cnc::CNC_MAX_SERVICES;
use crate::v2::config::MAX_MEMBERS;

/// `row u8 @0 ‖ reserved [u8; 3] @1 ‖ from u32 @4 ‖ to u32 @8 ‖ origin u64
/// @12` — exactly 20 bytes, `CLUSTER kind = 4`.
pub const UPGRADE_PIN_LEN: usize = 20;

/// "At position `origin`, row `row` went from `from` to `to`" — an EVENT,
/// not a tunable (spec §2.5): the sequence is what matters, which is why
/// it is its own kind and not a Settings field. `from`/`to` are packed
/// versions (`crate::identity::pack_version`); `origin` is the frame-END
/// position of the coordinated instant whose complete set the row will
/// install at its next attach — never 0, which is the cnc words' "no pin".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpgradePin {
    pub row: u8,
    pub from: u32,
    pub to: u32,
    pub origin: u64,
}

pub fn encode_upgrade_pin(p: &UpgradePin, out: &mut Vec<u8>) {
    out.push(p.row);
    out.extend_from_slice(&[0, 0, 0]);
    out.extend_from_slice(&p.from.to_le_bytes());
    out.extend_from_slice(&p.to.to_le_bytes());
    out.extend_from_slice(&p.origin.to_le_bytes());
}

/// Exact-length, reserved-zero, `row < CNC_MAX_SERVICES`, `origin > 0`.
pub fn decode_upgrade_pin(buf: &[u8]) -> Option<UpgradePin> {
    if buf.len() != UPGRADE_PIN_LEN || buf[1..4] != [0, 0, 0] {
        return None;
    }
    let row = buf[0];
    if row as usize >= CNC_MAX_SERVICES {
        return None;
    }
    let from = u32::from_le_bytes(buf[4..8].try_into().ok()?);
    let to = u32::from_le_bytes(buf[8..12].try_into().ok()?);
    let origin = u64::from_le_bytes(buf[12..20].try_into().ok()?);
    if origin == 0 {
        return None;
    }
    Some(UpgradePin {
        row,
        from,
        to,
        origin,
    })
}

/// `row u8 @0 ‖ count u8 @1 ‖ reserved [u8; 6] @2 ‖ position u64 @8`.
pub const SNAPSHOT_REPORT_HEADER_LEN: usize = 16;
/// `node_id u32 ‖ hash u64`.
pub const SNAPSHOT_REPORT_ENTRY_LEN: usize = 12;
/// One entry per member at most — the leader collects one hash per node.
pub const MAX_SNAPSHOT_REPORT_NODES: usize = MAX_MEMBERS;
/// 16 + 8 × 12: inside the 1312 B crypto-on ceiling at the baseline rung.
pub const SNAPSHOT_REPORT_MAX_LEN: usize =
    SNAPSHOT_REPORT_HEADER_LEN + MAX_SNAPSHOT_REPORT_NODES * SNAPSHOT_REPORT_ENTRY_LEN;

/// The node-side artifact hashes the leader collected for `(row, position)`
/// — `CLUSTER kind = 5` (spec §6.5.2 item 3). The verdict is NOT a field:
/// it is [`verdict`], a pure function every reader recomputes, so the
/// state holds only what was observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotReport {
    pub row: u8,
    pub position: u64,
    /// `(node_id, hash)`, strictly increasing by node id — the canonical
    /// order, so identical observations always encode identically.
    pub hashes: Vec<(u32, u64)>,
}

fn ids_strictly_increasing(hashes: &[(u32, u64)]) -> bool {
    hashes.windows(2).all(|w| w[0].0 < w[1].0)
}

/// `None` when the report is not encodable: empty, more than
/// [`MAX_SNAPSHOT_REPORT_NODES`] entries, ids not strictly increasing,
/// `row` out of range, or `position == 0`.
pub fn encode_snapshot_report(r: &SnapshotReport, out: &mut Vec<u8>) -> Option<()> {
    let n = r.hashes.len();
    if n == 0
        || n > MAX_SNAPSHOT_REPORT_NODES
        || !ids_strictly_increasing(&r.hashes)
        || r.row as usize >= CNC_MAX_SERVICES
        || r.position == 0
    {
        return None;
    }
    out.push(r.row);
    out.push(n as u8);
    out.extend_from_slice(&[0; 6]);
    out.extend_from_slice(&r.position.to_le_bytes());
    for (id, h) in &r.hashes {
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&h.to_le_bytes());
    }
    Some(())
}

/// Exact framing: `count` must match the length, reserved must be zero,
/// and every rule `encode_snapshot_report` enforces holds on read too.
pub fn decode_snapshot_report(buf: &[u8]) -> Option<SnapshotReport> {
    if buf.len() < SNAPSHOT_REPORT_HEADER_LEN || buf[2..8] != [0; 6] {
        return None;
    }
    let row = buf[0];
    let n = buf[1] as usize;
    if row as usize >= CNC_MAX_SERVICES
        || n == 0
        || n > MAX_SNAPSHOT_REPORT_NODES
        || buf.len() != SNAPSHOT_REPORT_HEADER_LEN + n * SNAPSHOT_REPORT_ENTRY_LEN
    {
        return None;
    }
    let position = u64::from_le_bytes(buf[8..16].try_into().ok()?);
    if position == 0 {
        return None;
    }
    let mut hashes = Vec::with_capacity(n);
    let mut o = SNAPSHOT_REPORT_HEADER_LEN;
    for _ in 0..n {
        let id = u32::from_le_bytes(buf[o..o + 4].try_into().ok()?);
        let h = u64::from_le_bytes(buf[o + 4..o + 12].try_into().ok()?);
        hashes.push((id, h));
        o += SNAPSHOT_REPORT_ENTRY_LEN;
    }
    if !ids_strictly_increasing(&hashes) {
        return None;
    }
    Some(SnapshotReport {
        row,
        position,
        hashes,
    })
}

/// The deterministic reading of one report (spec §6.5.2 item 4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    /// Every reporter's hash is the same.
    pub agreed: bool,
    /// The hash strictly more than half the reporters hold, if any.
    pub majority_hash: Option<u64>,
    /// Node ids whose hash differs from the majority's; empty when there
    /// is no majority to differ from (two nodes disagreeing names nobody).
    pub minority: Vec<u32>,
}

/// **Precondition: `r.hashes` is non-empty.** Every path that produces a
/// `SnapshotReport` goes through [`decode_snapshot_report`], which refuses
/// `count == 0`, so the only way to violate this is to hand-build the
/// struct. On an empty report `agreed` would read `true` (vacuously, from
/// `windows(2)`) with `majority_hash = None` — an "agreed but nothing
/// agreed on" pair that `uc2ctl upgrade show`'s match treats as
/// unreachable. Debug builds assert it rather than producing that pair.
pub fn verdict(r: &SnapshotReport) -> Verdict {
    debug_assert!(
        !r.hashes.is_empty(),
        "verdict on an empty report: decode_snapshot_report refuses count == 0, \
         so this report was hand-built"
    );
    let n = r.hashes.len();
    let agreed = r.hashes.windows(2).all(|w| w[0].1 == w[1].1);
    let majority_hash = r
        .hashes
        .iter()
        .map(|(_, h)| *h)
        .find(|h| r.hashes.iter().filter(|(_, x)| x == h).count() * 2 > n);
    let minority = match majority_hash {
        Some(m) => r
            .hashes
            .iter()
            .filter(|(_, h)| *h != m)
            .map(|(id, _)| *id)
            .collect(),
        None => Vec::new(),
    };
    Verdict {
        agreed,
        majority_hash,
        minority,
    }
}

/// The image's pin blob: `count × UPGRADE_PIN_LEN`, in apply order.
pub fn encode_pin_list(pins: &[UpgradePin], out: &mut Vec<u8>) {
    for p in pins {
        encode_upgrade_pin(p, out);
    }
}

pub fn decode_pin_list(buf: &[u8]) -> Option<Vec<UpgradePin>> {
    if !buf.len().is_multiple_of(UPGRADE_PIN_LEN) {
        return None;
    }
    buf.chunks_exact(UPGRADE_PIN_LEN)
        .map(decode_upgrade_pin)
        .collect()
}

/// The image's report blob: each entry `len u32 ‖ report`, exact framing.
pub fn encode_report_list(reports: &[SnapshotReport], out: &mut Vec<u8>) -> Option<()> {
    for r in reports {
        let mut b = Vec::with_capacity(SNAPSHOT_REPORT_MAX_LEN);
        encode_snapshot_report(r, &mut b)?;
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(&b);
    }
    Some(())
}

pub fn decode_report_list(buf: &[u8]) -> Option<Vec<SnapshotReport>> {
    let mut out = Vec::new();
    let mut o = 0;
    while o < buf.len() {
        let len = u32::from_le_bytes(buf.get(o..o + 4)?.try_into().ok()?) as usize;
        o += 4;
        let end = o.checked_add(len)?;
        out.push(decode_snapshot_report(buf.get(o..end)?)?);
        o = end;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin() -> UpgradePin {
        UpgradePin {
            row: 3,
            from: 0x0100_0000,
            to: 0x0101_0000,
            origin: 8192,
        }
    }

    #[test]
    fn pin_layout_is_frozen() {
        let mut b = Vec::new();
        encode_upgrade_pin(&pin(), &mut b);
        assert_eq!(b.len(), UPGRADE_PIN_LEN);
        assert_eq!(UPGRADE_PIN_LEN, 20);
        assert_eq!(b[0], 3, "row @0");
        assert_eq!(&b[1..4], &[0, 0, 0], "reserved @1 written as zero");
        assert_eq!(&b[4..8], &0x0100_0000u32.to_le_bytes(), "from @4");
        assert_eq!(&b[8..12], &0x0101_0000u32.to_le_bytes(), "to @8");
        assert_eq!(&b[12..20], &8192u64.to_le_bytes(), "origin @12");
        assert_eq!(decode_upgrade_pin(&b), Some(pin()));
    }

    #[test]
    fn pin_decode_is_exact_and_refuses_reserved_row_and_origin() {
        let mut b = Vec::new();
        encode_upgrade_pin(&pin(), &mut b);
        assert_eq!(decode_upgrade_pin(&b[..19]), None, "short");
        let mut long = b.clone();
        long.push(0);
        assert_eq!(decode_upgrade_pin(&long), None, "trailing byte");
        let mut r = b.clone();
        r[2] = 1;
        assert_eq!(decode_upgrade_pin(&r), None, "non-zero reserved");
        let mut row = b.clone();
        row[0] = 8;
        assert_eq!(decode_upgrade_pin(&row), None, "row >= CNC_MAX_SERVICES");
        let mut zero = b.clone();
        zero[12..20].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(
            decode_upgrade_pin(&zero),
            None,
            "origin 0 is 'no pin', never a record"
        );
    }

    fn report(hashes: &[(u32, u64)]) -> SnapshotReport {
        SnapshotReport {
            row: 0,
            position: 4096,
            hashes: hashes.to_vec(),
        }
    }

    #[test]
    fn report_layout_is_frozen() {
        let r = report(&[(0, 0xAA), (1, 0xAA), (2, 0xBB)]);
        let mut b = Vec::new();
        assert_eq!(encode_snapshot_report(&r, &mut b), Some(()));
        assert_eq!(
            b.len(),
            SNAPSHOT_REPORT_HEADER_LEN + 3 * SNAPSHOT_REPORT_ENTRY_LEN
        );
        assert_eq!(
            (SNAPSHOT_REPORT_HEADER_LEN, SNAPSHOT_REPORT_ENTRY_LEN),
            (16, 12)
        );
        assert_eq!(SNAPSHOT_REPORT_MAX_LEN, 112);
        assert_eq!(MAX_SNAPSHOT_REPORT_NODES, 8);
        assert_eq!(b[0], 0, "row @0");
        assert_eq!(b[1], 3, "count @1");
        assert_eq!(&b[2..8], &[0; 6], "reserved @2");
        assert_eq!(&b[8..16], &4096u64.to_le_bytes(), "position @8");
        assert_eq!(&b[16..20], &0u32.to_le_bytes(), "node_id of entry 0");
        assert_eq!(&b[20..28], &0xAAu64.to_le_bytes(), "hash of entry 0");
        assert_eq!(decode_snapshot_report(&b), Some(r));
    }

    #[test]
    fn report_encoding_is_canonical() {
        let mut b = Vec::new();
        assert_eq!(
            encode_snapshot_report(&report(&[(1, 1), (0, 1)]), &mut b),
            None,
            "node ids must be strictly increasing"
        );
        assert_eq!(
            encode_snapshot_report(&report(&[(1, 1), (1, 2)]), &mut b),
            None,
            "duplicate id"
        );
        assert_eq!(encode_snapshot_report(&report(&[]), &mut b), None, "empty");
        let nine: Vec<(u32, u64)> = (0..9).map(|i| (i, 7)).collect();
        assert_eq!(
            encode_snapshot_report(&report(&nine), &mut b),
            None,
            "count > MAX_MEMBERS"
        );
        // The decoder enforces the same rules on the wire.
        let mut ok = Vec::new();
        encode_snapshot_report(&report(&[(0, 1), (2, 1)]), &mut ok).unwrap();
        let mut swapped = ok.clone();
        swapped[16..20].copy_from_slice(&5u32.to_le_bytes()); // ids now 5, 2
        assert_eq!(decode_snapshot_report(&swapped), None);
        let mut zero_pos = ok.clone();
        zero_pos[8..16].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(decode_snapshot_report(&zero_pos), None);
        let mut short = ok.clone();
        short.pop();
        assert_eq!(decode_snapshot_report(&short), None);
        let mut count = ok.clone();
        count[1] = 1;
        assert_eq!(
            decode_snapshot_report(&count),
            None,
            "count disagrees with length"
        );
    }

    #[test]
    fn verdict_all_equal_majority_and_tie() {
        let v = verdict(&report(&[(0, 9), (1, 9), (2, 9)]));
        assert_eq!(
            (v.agreed, v.majority_hash, v.minority),
            (true, Some(9), vec![])
        );
        let v = verdict(&report(&[(0, 9), (1, 9), (2, 4)]));
        assert_eq!(
            (v.agreed, v.majority_hash, v.minority),
            (false, Some(9), vec![2])
        );
        let v = verdict(&report(&[(0, 9), (1, 4)]));
        assert_eq!(
            (v.agreed, v.majority_hash, v.minority),
            (false, None, vec![])
        );
        let v = verdict(&report(&[(0, 1), (1, 2), (2, 3), (3, 3)]));
        assert_eq!(
            (v.agreed, v.majority_hash, v.minority),
            (false, None, vec![]),
            "2 of 4 is not a majority"
        );
        let v = verdict(&report(&[(0, 5)]));
        assert_eq!(
            (v.agreed, v.majority_hash, v.minority),
            (true, Some(5), vec![])
        );
    }

    #[test]
    fn lists_roundtrip_with_exact_framing() {
        let pins = vec![
            pin(),
            UpgradePin {
                row: 0,
                from: 1,
                to: 2,
                origin: 100,
            },
        ];
        let mut b = Vec::new();
        encode_pin_list(&pins, &mut b);
        assert_eq!(b.len(), 2 * UPGRADE_PIN_LEN);
        assert_eq!(decode_pin_list(&b), Some(pins));
        assert_eq!(decode_pin_list(&b[..39]), None, "not a multiple of 20");
        assert_eq!(decode_pin_list(&[]), Some(vec![]));

        let reports = vec![
            report(&[(0, 1), (1, 1)]),
            SnapshotReport {
                row: 1,
                position: 8192,
                hashes: vec![(0, 2)],
            },
        ];
        let mut b = Vec::new();
        assert_eq!(encode_report_list(&reports, &mut b), Some(()));
        assert_eq!(decode_report_list(&b), Some(reports));
        b.push(0);
        assert_eq!(decode_report_list(&b), None, "trailing byte");
        assert_eq!(decode_report_list(&[]), Some(vec![]));
        assert_eq!(
            decode_report_list(&[9, 0, 0, 0, 1]),
            None,
            "length prefix past the end"
        );
    }
}
