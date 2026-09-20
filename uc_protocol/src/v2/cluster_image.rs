// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The cluster FSM's frozen snapshot image codec (cluster-FSM spec §4.7,
//! §4.8): magic ‖ version u32 ‖ applied u64 ‖ table_position u64 ‖
//! settings_position u64 ‖ membership (u32 len ‖ bytes) ‖ table (u32 len ‖
//! bytes) ‖ settings (one whole [`SETTINGS_LEN`] or [`SETTINGS_LEN_V1`]
//! record — the record is self-versioned and exact-length per version) ‖
//! crc32 of everything before it. That is layout v1, still ACCEPTED on
//! read. Layout v2 (plan B1 T3) appends two more length-prefixed blobs
//! after the settings record, before the CRC: pins (u32 len ‖ bytes) ‖
//! reports (u32 len ‖ bytes) — the upgrade-pin and snapshot-report records
//! (`v2::upgrade`'s list codecs), carried here as opaque bytes; this leaf
//! does not decode them.
//!
//! Moved out of `uc_node::cluster_fsm` (plan 3, spec §4.8) so a fuzz target
//! can reach the decoder without pulling in `ClusterFsm` — a below-floor
//! joiner installs this artifact BY FIAT off a snapshot session, and a
//! restarted node reads it off disk, so it is untrusted input like any other
//! wire codec here. `core`-friendly like its neighbours `v2::schedule` and
//! `v2::settings`: no I/O, no `sha2` — `crc32fast` is already a dependency of
//! this crate.
//!
//! The byte layout is unchanged from the plan-1/plan-2 `ClusterFsm::freeze`
//! this replaces — see `cluster_image_roundtrips_and_layout_is_frozen`'s
//! fixture, captured from the pre-move `freeze` output.
//!
//! `membership`, `table` and `settings` are returned as opaque byte slices,
//! not decoded here: the caller (`uc_node::cluster_fsm`) already owns
//! `config::decode_config`, `schedule::decode_schedule_table` and
//! `settings::decode_settings`, and decoding them here would just duplicate
//! that dispatch for no gain — the image codec's own job is only the outer
//! framing and the CRC.

use super::settings::{SETTINGS_LEN, SETTINGS_LEN_V1};

pub const CLUSTER_IMAGE_MAGIC: &[u8; 8] = b"UCCLUST1";
/// The image layout's version, refused by [`decode_cluster_image`] when
/// unknown.
///
/// Bumped to 2 by plan B1 for the pin and report blobs; a version-1 image
/// is still ACCEPTED on read, with both blobs empty — the settings v1/v2
/// precedent, since a restarting `2.12.0` node reads its own artifact.
pub const CLUSTER_IMAGE_VERSION: u32 = 2;

/// Bytes fixed before the two length-prefixed payloads: magic(8) ‖
/// version(4) ‖ applied(8) ‖ table_position(8) ‖ settings_position(8).
const FIXED_HEADER_LEN: usize = 8 + 4 + 8 + 8 + 8;
/// The offset of the membership length prefix within the body (everything
/// but the trailing CRC) — named so a test can target it directly, mirroring
/// `uc_node::cluster_fsm`'s prior `ML_OFFSET`.
pub const MEMBERSHIP_LEN_OFFSET: usize = FIXED_HEADER_LEN;
/// The smallest possible total image: the fixed header, two zero-length
/// prefixes, the SHORTEST settings record a decode accepts ([`SETTINGS_LEN_V1`]
/// — a `2.11.0` artifact carries one, jumbo spec §5.5) and the CRC.
///
/// This is a v1-shaped minimum and is left unchanged: it only guards the
/// initial length + magic read, before the version word is even
/// inspected, and both versions share that guard. A v2 image is at least 8
/// bytes longer (its two extra length prefixes), which the v2-specific
/// decode path below checks for itself.
const MIN_IMAGE_LEN: usize = FIXED_HEADER_LEN + 4 + 4 + SETTINGS_LEN_V1 + 4;

/// The three replicated records inside a cluster image, plus the two
/// positions that ride alongside them (spec §4.2, §4.8). `membership` and
/// `table` are still wire-encoded (`config::encode_config` /
/// `schedule::encode_schedule_table`); `settings` is the fixed
/// `settings::encode_settings` output. `applied` is the position the image
/// was frozen at (and, on install, the position the caller must confirm it
/// was asked to install — see `uc_node::cluster_fsm::install_snapshot`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterImageParts<'a> {
    pub applied: u64,
    pub table_position: u64,
    pub settings_position: u64,
    pub membership: &'a [u8],
    pub table: &'a [u8],
    pub settings: &'a [u8],
    /// The `v2::upgrade` pin-list bytes (opaque here; empty for a v1
    /// image or a cluster with no pins recorded).
    pub pins: &'a [u8],
    /// The `v2::upgrade` snapshot-report-list bytes (opaque here; empty for
    /// a v1 image or a cluster with no reports recorded).
    pub reports: &'a [u8],
}

/// Append the encoded image (magic through the trailing CRC) to `out`. The
/// CRC covers exactly the bytes this call appends — not any bytes already in
/// `out` before it — so a caller may compose this into a larger buffer
/// without the checksum picking up unrelated prefix bytes.
///
/// `None` if `p.membership`, `p.table`, `p.pins` or `p.reports` is longer
/// than `u32::MAX` bytes — each rides a `u32` length prefix on the wire, so
/// a payload that long cannot be represented at all and must be refused
/// rather than silently truncated by an `as u32` cast. None of these ever
/// approaches this in practice; the check exists so the cast at the call
/// site is provably safe rather than merely believed to be. `out` is left
/// untouched if `p.membership` or `p.table` is oversized (checked before
/// anything is written); a pins/reports refusal is caught only after the
/// fixed header, membership, table and settings have already been appended
/// — an acceptable asymmetry given how far into the multi-gigabyte range a
/// payload would have to be to trip it at all.
/// The one place a payload length becomes a wire prefix: `None` if it does
/// not fit the `u32` prefix, so [`encode_cluster_image`] REFUSES an oversized
/// payload rather than truncating it the way an `as u32` cast would. Kept as
/// its own function so the refusal is testable without materialising a
/// multi-gigabyte slice.
pub fn payload_len_prefix(len: usize) -> Option<u32> {
    len.try_into().ok()
}

pub fn encode_cluster_image(p: &ClusterImageParts<'_>, out: &mut Vec<u8>) -> Option<()> {
    let membership_len = payload_len_prefix(p.membership.len())?;
    let table_len = payload_len_prefix(p.table.len())?;
    let start = out.len();
    out.extend_from_slice(CLUSTER_IMAGE_MAGIC);
    out.extend_from_slice(&CLUSTER_IMAGE_VERSION.to_le_bytes());
    out.extend_from_slice(&p.applied.to_le_bytes());
    out.extend_from_slice(&p.table_position.to_le_bytes());
    out.extend_from_slice(&p.settings_position.to_le_bytes());
    out.extend_from_slice(&membership_len.to_le_bytes());
    out.extend_from_slice(p.membership);
    out.extend_from_slice(&table_len.to_le_bytes());
    out.extend_from_slice(p.table);
    out.extend_from_slice(p.settings);
    let pins_len = payload_len_prefix(p.pins.len())?;
    let reports_len = payload_len_prefix(p.reports.len())?;
    out.extend_from_slice(&pins_len.to_le_bytes());
    out.extend_from_slice(p.pins);
    out.extend_from_slice(&reports_len.to_le_bytes());
    out.extend_from_slice(p.reports);
    let crc = crc32fast::hash(&out[start..]);
    out.extend_from_slice(&crc.to_le_bytes());
    Some(())
}

/// Total, CRC-checked, exact-framing decode. `None` covers every refusal
/// alike (too short, bad magic, bad CRC, unknown version, a length prefix
/// that runs past the buffer, trailing bytes after the settings record) —
/// like `config::decode_config` and `settings::decode_settings`, this leaf
/// carries no reason string; `uc_node::cluster_fsm::install_snapshot` names
/// the one caller-known refusal (a position mismatch) itself, since the
/// expected position is not part of the wire image.
///
/// CRC32 is a public checksum, not a MAC: a crafted-or-corrupt body can
/// still match it, so every length-prefixed and fixed-width read below is
/// bounds-checked with `.get(..)` before slicing — no declared length,
/// however wrong, may panic.
pub fn decode_cluster_image(buf: &[u8]) -> Option<ClusterImageParts<'_>> {
    if buf.len() < MIN_IMAGE_LEN || &buf[0..8] != CLUSTER_IMAGE_MAGIC {
        return None;
    }
    let (body, crc) = buf.split_at(buf.len() - 4);
    if crc32fast::hash(body) != u32::from_le_bytes(crc.try_into().ok()?) {
        return None;
    }
    let u32_at = |o: usize| -> Option<u32> {
        body.get(o..o + 4)
            .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
    };
    let u64_at = |o: usize| -> Option<u64> {
        body.get(o..o + 8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
    };
    let mut o = 8;
    let version = u32_at(o)?;
    if version != 1 && version != CLUSTER_IMAGE_VERSION {
        return None;
    }
    o += 4;
    let applied = u64_at(o)?;
    o += 8;
    let table_position = u64_at(o)?;
    o += 8;
    let settings_position = u64_at(o)?;
    o += 8;
    debug_assert_eq!(o, MEMBERSHIP_LEN_OFFSET);
    let ml = u32_at(o)? as usize;
    o += 4;
    let membership = o.checked_add(ml).and_then(|end| body.get(o..end))?;
    o += ml;
    let tl = u32_at(o)? as usize;
    o += 4;
    let table = o.checked_add(tl).and_then(|end| body.get(o..end))?;
    o += tl;
    let (settings, pins, reports) = if version == 1 {
        // 2.11.0/2.12.0 layout: the remainder is exactly one settings
        // record, self-versioned and exact-length per version
        // (`settings::decode_settings`) — never a slice that could run past
        // `body`'s end. A 2.11.0 artifact carries v1 — jumbo spec §5.5.
        let rest = body.len().checked_sub(o)?;
        if rest != SETTINGS_LEN && rest != SETTINGS_LEN_V1 {
            return None;
        }
        (&body[o..], &body[body.len()..], &body[body.len()..])
    } else {
        // v2: the settings record is sized by ITS OWN version word (the
        // record is exact-length per version), then two length-prefixed
        // blobs, then nothing.
        let sl = match u32_at(o)? {
            1 => SETTINGS_LEN_V1,
            2 => SETTINGS_LEN,
            _ => return None,
        };
        let settings = o.checked_add(sl).and_then(|end| body.get(o..end))?;
        o += sl;
        let pl = u32_at(o)? as usize;
        o += 4;
        let pins = o.checked_add(pl).and_then(|end| body.get(o..end))?;
        o += pl;
        let rl = u32_at(o)? as usize;
        o += 4;
        let reports = o.checked_add(rl).and_then(|end| body.get(o..end))?;
        o += rl;
        if o != body.len() {
            return None;
        }
        (settings, pins, reports)
    };
    Some(ClusterImageParts {
        applied,
        table_position,
        settings_position,
        membership,
        table,
        settings,
        pins,
        reports,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Captured 2026-09-07 by temporarily instrumenting
    /// `uc_node::cluster_fsm::ClusterFsm::freeze` on this worktree BEFORE the
    /// leaf move (`ClusterFsm::new(ClusterState::genesis_empty(), ..)`,
    /// `set_consumed(500)`, then `freeze()`), printing the resulting bytes,
    /// and pasting them here as a `const`. Originally pinned that this
    /// leaf's `encode_cluster_image` reproduced the plan-1/plan-2-era byte
    /// layout exactly (Q4: "the byte layout on `main` NOW is the layout");
    /// since plan B1 T3, `encode_cluster_image` always writes the v2 layout
    /// (two trailing length-prefixed pin/report blobs), so this fixture now
    /// pins the DECODE side only — a version-1 image with no such blobs must
    /// still decode, with both fields empty (the "v1 accepted on read"
    /// requirement).
    ///
    /// Provenance, byte for byte (all fixed-width fields little-endian):
    ///   magic          "UCCLUST1"                                  (8 B)
    ///   version        1u32                                        (4 B)
    ///   applied        500u64                                      (8 B)
    ///   table_position 0u64                                        (8 B)
    ///   settings_pos   0u64                                        (8 B)
    ///   membership len 22u32, then `encode_config` of the wire form of
    ///                  `genesis_empty()`'s membership (`ClusterConfig`
    ///                  default: `version: 0`, no voters/learners/
    ///                  tombstones): config-version=0u64(8) ‖
    ///                  prev_position=0u64(8) ‖ nv=0u16(2) ‖ nl=0u16(2) ‖
    ///                  nt=0u16(2) = 22 bytes, all zero
    ///   table len      8u32, then `encode_schedule_table` of an empty
    ///                  table: version=1u32(4) ‖ count=0u32(4) = 8 bytes
    ///   settings       the VERSION-1 settings record `2.11.0` wrote:
    ///                  version=1u32(4) ‖ fsm_lag=0u64(8) ‖ admission=0u64(8)
    ///                  ‖ snapshot_interval=0u64(8) ‖ target=All=0u8(1) = 29 B.
    ///                  `encode_settings` writes v2 (33 B) since the jumbo
    ///                  flag day, so the fixture's tail is built by
    ///                  `v1_settings_blob` — jumbo spec §5.5.
    ///   crc32          0x9D3283FF LE, of everything above
    #[rustfmt::skip]
    const PLAN1_FIXTURE: &[u8] = &[
        // magic "UCCLUST1"
        0x55, 0x43, 0x43, 0x4C, 0x55, 0x53, 0x54, 0x31,
        // version = 1
        0x01, 0x00, 0x00, 0x00,
        // applied = 500
        0xF4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // table_position = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // settings_position = 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // membership len = 22
        0x16, 0x00, 0x00, 0x00,
        // membership: config-version=0, prev_position=0, nv=0, nl=0, nt=0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // table len = 8
        0x08, 0x00, 0x00, 0x00,
        // table: version=1, count=0
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // settings: version=1
        0x01, 0x00, 0x00, 0x00,
        // settings: fsm_lag_bytes=0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // settings: admission_bytes=0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // settings: snapshot_interval_bytes=0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // settings: target = All = 0
        0x00,
        // crc32 of everything above, LE
        0xFF, 0x83, 0x32, 0x9D,
    ];

    /// The 29-byte VERSION-1 settings record a `2.11.0` node wrote — the tail
    /// of every cluster artifact that survives the jumbo flag day, and the
    /// tail [`PLAN1_FIXTURE`] pins. Hand-built because `encode_settings` now
    /// emits v2; jumbo spec §5.5.
    fn v1_settings_blob() -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&1u32.to_le_bytes());
        v.extend_from_slice(&0u64.to_le_bytes());
        v.extend_from_slice(&0u64.to_le_bytes());
        v.extend_from_slice(&0u64.to_le_bytes());
        v.push(0); // Target::All
        assert_eq!(v.len(), SETTINGS_LEN_V1);
        v
    }

    /// A version-2 (33 B, jumbo `datagram_mtu` field included) settings
    /// record: `encode_settings(&Settings::genesis_default())`'s bytes.
    fn v2_settings() -> Vec<u8> {
        use super::super::settings::{Settings, encode_settings};
        let mut v = Vec::new();
        encode_settings(&Settings::genesis_default(), &mut v);
        assert_eq!(v.len(), SETTINGS_LEN);
        v
    }

    fn genesis_parts() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        use super::super::config::{WireConfig, encode_config};
        use super::super::schedule::{ScheduleTable, encode_schedule_table};
        use super::super::settings::{Settings, encode_settings};

        // `version: 0`: `uc_node::cluster_fsm::ClusterState::genesis_empty`'s
        // `ClusterConfig` default, matching the fixture below.
        let mut membership = Vec::new();
        encode_config(
            &WireConfig {
                version: 0,
                prev_position: 0,
                voters: vec![],
                learners: vec![],
                tombstones: vec![],
            },
            &mut membership,
        );
        let mut table = Vec::new();
        encode_schedule_table(&ScheduleTable { entries: vec![] }, &mut table);
        let mut settings = Vec::new();
        encode_settings(&Settings::genesis_default(), &mut settings);
        (membership, table, settings)
    }

    #[test]
    fn cluster_image_roundtrips_and_layout_is_frozen() {
        let (membership, table, _) = genesis_parts();
        // The OUTER framing is what this fixture freezes; the settings tail
        // inside it is the v1 record `2.11.0` wrote, which this codec must
        // still frame and decode after the jumbo flag day (§5.5).
        let settings = v1_settings_blob();
        let parts = ClusterImageParts {
            applied: 500,
            table_position: 0,
            settings_position: 0,
            membership: &membership,
            table: &table,
            settings: &settings,
            pins: &[],
            reports: &[],
        };
        // encode_cluster_image now always writes the v2 layout (with two
        // trailing, empty, length-prefixed pin/report blobs), so it no
        // longer reproduces PLAN1_FIXTURE byte-for-byte; that pinning moves
        // to the decode side below, which is the "v1 accepted on read"
        // requirement this test exists to cover.
        let mut img = Vec::new();
        encode_cluster_image(&parts, &mut img).expect("well under u32::MAX");
        assert_eq!(&img[0..8], CLUSTER_IMAGE_MAGIC, "magic at 0");
        assert_eq!(
            &img[8..12],
            &CLUSTER_IMAGE_VERSION.to_le_bytes(),
            "version at 8"
        );
        let decoded = decode_cluster_image(&img).expect("a well-formed image decodes");
        assert_eq!(decoded, parts);

        // And the plan-1-era fixture itself still installs under this leaf,
        // with both new fields empty.
        let from_v1 = decode_cluster_image(PLAN1_FIXTURE).expect("a plan-1-era image decodes");
        assert_eq!(from_v1, parts);
        assert!(from_v1.pins.is_empty() && from_v1.reports.is_empty());
    }

    /// Jumbo spec §5.5: a `2.11.0` artifact's tail is a 29-byte v1 settings
    /// record and must keep framing and decoding — with the new field at its
    /// baseline meaning — while a v2 tail (33 B) frames alongside it. Any
    /// OTHER remainder is refused: the length is exact per version, so a
    /// truncated or padded tail can never be read as a prefix.
    #[test]
    fn a_v1_settings_tail_still_frames_and_an_off_length_tail_is_refused() {
        use super::super::settings::decode_settings;

        let (membership, table, v2) = genesis_parts();
        let v1 = v1_settings_blob();
        for tail in [&v1, &v2] {
            let parts = ClusterImageParts {
                applied: 640,
                table_position: 0,
                settings_position: 320,
                membership: &membership,
                table: &table,
                settings: tail,
                pins: &[],
                reports: &[],
            };
            let mut img = Vec::new();
            encode_cluster_image(&parts, &mut img).expect("well under u32::MAX");
            let d = decode_cluster_image(&img).expect("both record versions frame");
            assert_eq!(d.settings.len(), tail.len());
            assert_eq!(decode_settings(d.settings).unwrap().datagram_mtu, 0);
        }
        assert_eq!(v1.len(), 29);
        assert_eq!(v2.len(), 33);

        // 31 bytes: neither version's length. The CRC is correct — this is
        // the framing check refusing it, not corruption.
        for bad_len in [28usize, 31, 34] {
            let mut tail = v2.clone();
            tail.resize(bad_len, 0);
            let parts = ClusterImageParts {
                applied: 640,
                table_position: 0,
                settings_position: 320,
                membership: &membership,
                table: &table,
                settings: &tail,
                pins: &[],
                reports: &[],
            };
            let mut img = Vec::new();
            encode_cluster_image(&parts, &mut img).expect("well under u32::MAX");
            assert_eq!(
                decode_cluster_image(&img),
                None,
                "a {bad_len}-byte settings tail is neither version's exact length"
            );
        }
    }

    #[test]
    fn every_single_byte_corruption_is_refused() {
        let (membership, table, settings) = genesis_parts();
        let parts = ClusterImageParts {
            applied: 12345,
            table_position: 640,
            settings_position: 320,
            membership: &membership,
            table: &table,
            settings: &settings,
            pins: &[],
            reports: &[],
        };
        let mut img = Vec::new();
        encode_cluster_image(&parts, &mut img).expect("well under u32::MAX");
        assert_eq!(decode_cluster_image(&img), Some(parts));

        for pos in 0..img.len() {
            for bit in 0..8u8 {
                let mut c = img.clone();
                c[pos] ^= 1 << bit;
                // The CRC guarantees this: flipping any bit anywhere,
                // INCLUDING inside the trailing CRC field itself, changes
                // either the body's computed checksum or the stored one
                // (never both in a way that cancels), so every single-bit
                // corruption is refused. The one theoretical exception —
                // a flip that leaves the checksum matching by coincidence —
                // does not occur for a CRC32 under a single-bit change: a
                // single-bit flip in the body always changes its CRC32 (CRC
                // polynomials detect all single-bit errors by construction),
                // and a single-bit flip in the stored CRC field changes the
                // stored value without touching the (unaffected) body, so it
                // no longer matches either.
                assert_eq!(
                    decode_cluster_image(&c),
                    None,
                    "byte {pos} bit {bit} must be refused"
                );
            }
        }
    }

    #[test]
    fn encode_refuses_a_payload_longer_than_u32_max_rather_than_truncating() {
        // M10: an `as u32` cast at the call site would silently truncate a
        // length one past `u32::MAX` to 0 and write a well-formed image with a
        // lying prefix. The refusal lives in `payload_len_prefix`, which the
        // encoder consults BEFORE touching a byte of either payload, so it is
        // tested directly — no oversized slice is ever constructed (a dangling
        // `from_raw_parts` of that length would violate the slice contract).
        assert_eq!(payload_len_prefix(u32::MAX as usize), Some(u32::MAX));
        assert_eq!(payload_len_prefix(u32::MAX as usize + 1), None);
        assert_eq!(payload_len_prefix(usize::MAX), None);
        // And the layout stays exactly what a fitting length produces.
        let (membership, table, settings) = genesis_parts();
        let mut out = Vec::new();
        let parts = ClusterImageParts {
            applied: 1,
            table_position: 0,
            settings_position: 0,
            membership: &membership,
            table: &table,
            settings: &settings,
            pins: &[],
            reports: &[],
        };
        assert_eq!(encode_cluster_image(&parts, &mut out), Some(()));
        assert!(decode_cluster_image(&out).is_some());
    }

    #[test]
    fn v2_roundtrips_pins_and_reports_and_is_exact() {
        let pins = [1u8; 40]; // two 20-byte pin records' worth of bytes: the leaf does not decode them
        let reports = [2u8; 7];
        let p = ClusterImageParts {
            applied: 500,
            table_position: 0,
            settings_position: 400,
            membership: &[9, 9],
            table: &[],
            settings: &v2_settings(),
            pins: &pins,
            reports: &reports,
        };
        let mut img = Vec::new();
        encode_cluster_image(&p, &mut img).unwrap();
        assert_eq!(&img[8..12], &2u32.to_le_bytes(), "version 2");
        let d = decode_cluster_image(&img).unwrap();
        assert_eq!(
            (
                d.applied,
                d.settings_position,
                d.membership,
                d.pins,
                d.reports
            ),
            (500, 400, &[9u8, 9][..], &pins[..], &reports[..])
        );
        // Exact framing: a pins length that runs into the reports, or past
        // the CRC, is refused; so is a byte after the reports blob.
        let mut bad = img.clone();
        // magic(8) + version(4) + applied/table_pos/settings_pos(24) +
        // membership len prefix(4) + membership bytes(2) + table len
        // prefix(4) + table bytes(0) + the settings record.
        #[allow(clippy::identity_op)]
        let pins_len_off = 8 + 4 + 24 + 4 + 2 + 4 + 0 + SETTINGS_LEN;
        bad[pins_len_off..pins_len_off + 4].copy_from_slice(&41u32.to_le_bytes());
        fix_crc(&mut bad);
        assert!(decode_cluster_image(&bad).is_none());
        let mut trailing = img.clone();
        let l = trailing.len();
        trailing.insert(l - 4, 0);
        fix_crc(&mut trailing);
        assert!(decode_cluster_image(&trailing).is_none());
    }

    #[test]
    fn v2_carries_a_v1_settings_record_by_its_own_version_word() {
        // A 2.11.0 settings record (29 B) inside a v2 image: the decoder
        // sizes the record from its version word, never from "the rest".
        let p = ClusterImageParts {
            applied: 1,
            table_position: 0,
            settings_position: 0,
            membership: &[],
            table: &[],
            settings: &v1_settings_blob(),
            pins: &[],
            reports: &[],
        };
        let mut img = Vec::new();
        encode_cluster_image(&p, &mut img).unwrap();
        let d = decode_cluster_image(&img).unwrap();
        assert_eq!(d.settings, &v1_settings_blob()[..]);
    }

    /// Recompute the trailing CRC after a deliberate mutation.
    fn fix_crc(img: &mut [u8]) {
        let l = img.len();
        let crc = crc32fast::hash(&img[..l - 4]);
        img[l - 4..].copy_from_slice(&crc.to_le_bytes());
    }
}
