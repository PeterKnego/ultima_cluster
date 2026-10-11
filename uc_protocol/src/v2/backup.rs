// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Backup tier wire codecs (backup-tier spec §3a): `CLUSTER kind = 7
//! BackupWatermark` and the pairwise `BACKUP_REPORT` (datagram 27) body.

/// At most this many registered backup tiers per cluster.
pub const MAX_BACKUP_TIERS: usize = 4;
/// `op u8 ‖ reserved [u8; 7] ‖ tier_id ‖ oldest_set ‖ newest_set ‖ through ‖
/// reporter` (five `u64 LE`).
pub const BACKUP_WATERMARK_LEN: usize = 48;
/// `tier_id ‖ oldest_set ‖ newest_set ‖ through ‖ reporter` (five `u64 LE`).
pub const BACKUP_REPORT_BODY_LEN: usize = 40;
/// One tier's committed watermark in the cluster image's `backups` blob.
pub const BACKUP_TIER_ENTRY_LEN: usize = 48;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatermarkOp {
    Report = 0,
    Forget = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackupWatermark {
    pub op: WatermarkOp,
    pub tier_id: u64,
    pub oldest_set: u64,
    pub newest_set: u64,
    pub through: u64,
    pub reporter: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackupTierEntry {
    pub tier_id: u64,
    pub oldest_set: u64,
    pub newest_set: u64,
    pub through: u64,
    pub reporter: u64,
    pub updated_pos: u64,
}

fn rd(buf: &[u8], i: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&buf[i..i + 8]);
    u64::from_le_bytes(a)
}

fn valid(w: &BackupWatermark) -> bool {
    w.tier_id != 0
        && (w.op == WatermarkOp::Forget
            || (w.oldest_set <= w.newest_set && w.newest_set <= w.through))
}

pub fn encode_backup_watermark(w: &BackupWatermark, out: &mut Vec<u8>) {
    out.push(w.op as u8);
    out.extend_from_slice(&[0; 7]);
    for v in [w.tier_id, w.oldest_set, w.newest_set, w.through, w.reporter] {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

/// Exact length, reserved zero, `op` in {0, 1}, `tier_id != 0`; a `Report`
/// also needs `oldest_set <= newest_set <= through`.
pub fn decode_backup_watermark(buf: &[u8]) -> Option<BackupWatermark> {
    if buf.len() != BACKUP_WATERMARK_LEN || buf[1..8] != [0; 7] {
        return None;
    }
    let op = match buf[0] {
        0 => WatermarkOp::Report,
        1 => WatermarkOp::Forget,
        _ => return None,
    };
    let w = BackupWatermark {
        op,
        tier_id: rd(buf, 8),
        oldest_set: rd(buf, 16),
        newest_set: rd(buf, 24),
        through: rd(buf, 32),
        reporter: rd(buf, 40),
    };
    valid(&w).then_some(w)
}

/// `u8 count ‖ count × 48-byte entries`.
pub fn encode_backup_list(l: &[BackupTierEntry], out: &mut Vec<u8>) {
    out.push(l.len() as u8);
    for e in l {
        for v in [
            e.tier_id,
            e.oldest_set,
            e.newest_set,
            e.through,
            e.reporter,
            e.updated_pos,
        ] {
            out.extend_from_slice(&v.to_le_bytes());
        }
    }
}

/// Exact length, `count <= MAX_BACKUP_TIERS`, unique tier ids.
pub fn decode_backup_list(buf: &[u8]) -> Option<Vec<BackupTierEntry>> {
    let (&count, rest) = buf.split_first()?;
    let count = count as usize;
    if count > MAX_BACKUP_TIERS || rest.len() != count * BACKUP_TIER_ENTRY_LEN {
        return None;
    }
    let mut out: Vec<BackupTierEntry> = Vec::with_capacity(count);
    for c in rest.chunks_exact(BACKUP_TIER_ENTRY_LEN) {
        let e = BackupTierEntry {
            tier_id: rd(c, 0),
            oldest_set: rd(c, 8),
            newest_set: rd(c, 16),
            through: rd(c, 24),
            reporter: rd(c, 32),
            updated_pos: rd(c, 40),
        };
        if out.iter().any(|p| p.tier_id == e.tier_id) {
            return None;
        }
        out.push(e);
    }
    Some(out)
}

/// Write a `Report` body into the first 40 bytes of `buf` (panics if shorter).
pub fn write_backup_report_body(buf: &mut [u8], w: &BackupWatermark) {
    for (i, v) in [w.tier_id, w.oldest_set, w.newest_set, w.through, w.reporter]
        .iter()
        .enumerate()
    {
        buf[i * 8..i * 8 + 8].copy_from_slice(&v.to_le_bytes());
    }
}

/// Exact 40 bytes, same validity as [`decode_backup_watermark`]; `op = Report`.
pub fn read_backup_report_body(buf: &[u8]) -> Option<BackupWatermark> {
    if buf.len() != BACKUP_REPORT_BODY_LEN {
        return None;
    }
    let w = BackupWatermark {
        op: WatermarkOp::Report,
        tier_id: rd(buf, 0),
        oldest_set: rd(buf, 8),
        newest_set: rd(buf, 16),
        through: rd(buf, 24),
        reporter: rd(buf, 32),
    };
    valid(&w).then_some(w)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wm() -> BackupWatermark {
        BackupWatermark {
            op: WatermarkOp::Report,
            tier_id: 1,
            oldest_set: 2,
            newest_set: 3,
            through: 4,
            reporter: 5,
        }
    }

    #[test]
    fn watermark_golden_bytes_and_strict_decode() {
        let mut b = Vec::new();
        encode_backup_watermark(&wm(), &mut b);
        let mut want = vec![0u8; 8];
        for v in [1u64, 2, 3, 4, 5] {
            want.extend_from_slice(&v.to_le_bytes());
        }
        assert_eq!(b, want);
        assert_eq!(b.len(), BACKUP_WATERMARK_LEN);
        assert_eq!(decode_backup_watermark(&b), Some(wm()));
        assert_eq!(decode_backup_watermark(&b[..47]), None);
        let mut long = b.clone();
        long.push(0);
        assert_eq!(decode_backup_watermark(&long), None);
        let mut bad = b.clone();
        bad[3] = 1; // reserved
        assert_eq!(decode_backup_watermark(&bad), None);
        let mut bad = b.clone();
        bad[0] = 2; // op
        assert_eq!(decode_backup_watermark(&bad), None);
        let mut z = wm();
        z.tier_id = 0;
        let mut zb = Vec::new();
        encode_backup_watermark(&z, &mut zb);
        assert_eq!(decode_backup_watermark(&zb), None);
        for (o, n, t) in [(4, 3, 4), (2, 5, 4)] {
            let mut w = wm();
            w.oldest_set = o;
            w.newest_set = n;
            w.through = t;
            let mut v = Vec::new();
            encode_backup_watermark(&w, &mut v);
            assert_eq!(decode_backup_watermark(&v), None);
        }
    }

    #[test]
    fn forget_skips_ordering_checks() {
        let w = BackupWatermark {
            op: WatermarkOp::Forget,
            tier_id: 7,
            oldest_set: 9,
            newest_set: 0,
            through: 0,
            reporter: 0,
        };
        let mut b = Vec::new();
        encode_backup_watermark(&w, &mut b);
        assert_eq!(b[0], 1);
        assert_eq!(decode_backup_watermark(&b), Some(w));
    }

    fn entry(t: u64) -> BackupTierEntry {
        BackupTierEntry {
            tier_id: t,
            oldest_set: 1,
            newest_set: 2,
            through: 3,
            reporter: 4,
            updated_pos: 5,
        }
    }

    #[test]
    fn list_round_trip_and_bounds() {
        let l = vec![entry(1), entry(2)];
        let mut b = Vec::new();
        encode_backup_list(&l, &mut b);
        assert_eq!(b.len(), 1 + 2 * BACKUP_TIER_ENTRY_LEN);
        assert_eq!(decode_backup_list(&b), Some(l));
        let mut e = Vec::new();
        encode_backup_list(&[], &mut e);
        assert_eq!(decode_backup_list(&e), Some(vec![]));
        let five: Vec<_> = (1..=5).map(entry).collect();
        let mut b5 = Vec::new();
        encode_backup_list(&five, &mut b5);
        assert_eq!(decode_backup_list(&b5), None);
        let mut dup = Vec::new();
        encode_backup_list(&[entry(1), entry(1)], &mut dup);
        assert_eq!(decode_backup_list(&dup), None);
        let mut b = Vec::new();
        encode_backup_list(&[entry(1)], &mut b);
        b.push(0);
        assert_eq!(decode_backup_list(&b), None);
        assert_eq!(decode_backup_list(&b[..b.len() - 2]), None);
    }

    #[test]
    fn report_body_round_trip() {
        assert_eq!(BACKUP_REPORT_BODY_LEN, 40);
        let mut buf = [0u8; BACKUP_REPORT_BODY_LEN];
        write_backup_report_body(&mut buf, &wm());
        assert_eq!(&buf[..8], &1u64.to_le_bytes());
        assert_eq!(read_backup_report_body(&buf), Some(wm()));
        assert_eq!(read_backup_report_body(&buf[..39]), None);
        buf[8..16].copy_from_slice(&9u64.to_le_bytes()); // oldest > newest
        assert_eq!(read_backup_report_body(&buf), None);
    }
}
