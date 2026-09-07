// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The cluster FSM's frozen snapshot image codec (cluster-FSM spec §4.7,
//! §4.8): magic ‖ version u32 ‖ applied u64 ‖ table_position u64 ‖
//! settings_position u64 ‖ membership (u32 len ‖ bytes) ‖ table (u32 len ‖
//! bytes) ‖ settings (fixed [`SETTINGS_LEN`]) ‖ crc32 of everything before
//! it.
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

use super::settings::SETTINGS_LEN;

pub const CLUSTER_IMAGE_MAGIC: &[u8; 8] = b"UCCLUST1";
/// The image layout's version, refused by [`decode_cluster_image`] when
/// unknown.
///
/// Still `1` even though the layout changed twice during plan 1's
/// development: nothing was released at any intermediate shape, so there is
/// no artifact in the world to be compatible with. A pre-release image
/// therefore fails the membership-length or CRC check rather than a version
/// refusal — a fine outcome for an artifact that only exists on a developer's
/// disk, and not a reason to burn a version number. Bump it for the first
/// change made AFTER a release.
pub const CLUSTER_IMAGE_VERSION: u32 = 1;

/// Bytes fixed before the two length-prefixed payloads: magic(8) ‖
/// version(4) ‖ applied(8) ‖ table_position(8) ‖ settings_position(8).
const FIXED_HEADER_LEN: usize = 8 + 4 + 8 + 8 + 8;
/// The offset of the membership length prefix within the body (everything
/// but the trailing CRC) — named so a test can target it directly, mirroring
/// `uc_node::cluster_fsm`'s prior `ML_OFFSET`.
pub const MEMBERSHIP_LEN_OFFSET: usize = FIXED_HEADER_LEN;
/// The smallest possible total image: the fixed header, two zero-length
/// prefixes, a zero-length settings record (never actually zero — settings
/// is fixed-length — kept only for the length floor check) and the CRC.
const MIN_IMAGE_LEN: usize = FIXED_HEADER_LEN + 4 + 4 + SETTINGS_LEN + 4;

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
}

/// Append the encoded image (magic through the trailing CRC) to `out`. The
/// CRC covers exactly the bytes this call appends — not any bytes already in
/// `out` before it — so a caller may compose this into a larger buffer
/// without the checksum picking up unrelated prefix bytes.
pub fn encode_cluster_image(p: &ClusterImageParts<'_>, out: &mut Vec<u8>) {
    let start = out.len();
    out.extend_from_slice(CLUSTER_IMAGE_MAGIC);
    out.extend_from_slice(&CLUSTER_IMAGE_VERSION.to_le_bytes());
    out.extend_from_slice(&p.applied.to_le_bytes());
    out.extend_from_slice(&p.table_position.to_le_bytes());
    out.extend_from_slice(&p.settings_position.to_le_bytes());
    out.extend_from_slice(&(p.membership.len() as u32).to_le_bytes());
    out.extend_from_slice(p.membership);
    out.extend_from_slice(&(p.table.len() as u32).to_le_bytes());
    out.extend_from_slice(p.table);
    out.extend_from_slice(p.settings);
    let crc = crc32fast::hash(&out[start..]);
    out.extend_from_slice(&crc.to_le_bytes());
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
    if u32_at(o)? != CLUSTER_IMAGE_VERSION {
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
    // `settings::decode_settings` is itself exact-length (no trailing bytes
    // tolerated), so require the remainder to be exactly `SETTINGS_LEN`
    // rather than handing the caller a slice that could run past `body`'s
    // end.
    if o.checked_add(SETTINGS_LEN) != Some(body.len()) {
        return None;
    }
    let settings = &body[o..];
    Some(ClusterImageParts {
        applied,
        table_position,
        settings_position,
        membership,
        table,
        settings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Captured 2026-09-07 by temporarily instrumenting
    /// `uc_node::cluster_fsm::ClusterFsm::freeze` on this worktree BEFORE the
    /// leaf move (`ClusterFsm::new(ClusterState::genesis_empty(), ..)`,
    /// `set_consumed(500)`, then `freeze()`), printing the resulting bytes,
    /// and pasting them here as a `const`. Pins that this leaf's
    /// `encode_cluster_image` reproduces the plan-1/plan-2-era byte layout
    /// exactly (Q4: "the byte layout on `main` NOW is the layout").
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
    ///   settings       `encode_settings(Settings::genesis_default())`:
    ///                  version=1u32(4) ‖ fsm_lag=0u64(8) ‖ admission=0u64(8)
    ///                  ‖ snapshot_interval=0u64(8) ‖ target=All=0u8(1) = 29 B
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
        let (membership, table, settings) = genesis_parts();
        let parts = ClusterImageParts {
            applied: 500,
            table_position: 0,
            settings_position: 0,
            membership: &membership,
            table: &table,
            settings: &settings,
        };
        let mut img = Vec::new();
        encode_cluster_image(&parts, &mut img);

        assert_eq!(&img[0..8], CLUSTER_IMAGE_MAGIC, "magic at 0");
        assert_eq!(
            &img[8..12],
            &CLUSTER_IMAGE_VERSION.to_le_bytes(),
            "version at 8"
        );
        assert_eq!(
            img, PLAN1_FIXTURE,
            "encode_cluster_image must reproduce the plan-1-era byte layout, CRC included, exactly"
        );

        let decoded = decode_cluster_image(&img).expect("a well-formed image decodes");
        assert_eq!(decoded, parts);
        // And the fixture itself installs, pinning that a plan-1-era
        // artifact still loads under this leaf.
        assert_eq!(decode_cluster_image(PLAN1_FIXTURE), Some(parts));
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
        };
        let mut img = Vec::new();
        encode_cluster_image(&parts, &mut img);
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
}
