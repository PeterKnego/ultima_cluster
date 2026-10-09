// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The cluster FSM's frozen snapshot image codec (cluster-FSM spec §4.7,
//! §4.8): magic ‖ version u32 ‖ applied u64 ‖ table_position u64 ‖
//! settings_position u64 ‖ membership (u32 len ‖ bytes) ‖ table (u32 len ‖
//! bytes) ‖ settings (one whole [`SETTINGS_LEN`], [`SETTINGS_LEN_V2`] or
//! [`SETTINGS_LEN_V1`] record — the record is self-versioned and
//! exact-length per version) ‖
//! crc32 of everything before it. That is layout v1, still ACCEPTED on
//! read. Layout v2 (plan B1 T3) appends two more length-prefixed blobs
//! after the settings record, before the CRC: pins (u32 len ‖ bytes) ‖
//! reports (u32 len ‖ bytes) — the upgrade-pin and snapshot-report records
//! (`v2::upgrade`'s list codecs), carried here as opaque bytes; this leaf
//! does not decode them. Layout v3 (#33 task 3) appends one more
//! length-prefixed blob after `reports`, before the CRC: running (u32 len ‖
//! bytes) — the per-row running-version records (`v2::upgrade::RowRunning`'s
//! list codec), also opaque here. v1 and v2 images are still ACCEPTED on
//! read, with `running` empty. Layout v4 (catalog spec §7) appends one more
//! length-prefixed blob after `running`, before the CRC: catalog (u32 len ‖
//! bytes) — the snapshot catalog's set list (`v2::catalog`'s
//! `encode_set_list`), opaque here. v1–v3 images are still ACCEPTED on read,
//! with `catalog` empty. Layout v5 (snapshot-lifecycle spec §7.2) changes no
//! framing: it marks that the opaque `reports` and `catalog` blobs carry sizes.
//!
//! Moved out of `uc_node::cluster_fsm` (plan 3, spec §4.8) so a fuzz target
//! can reach the decoder without pulling in `ClusterFsm` — a below-floor
//! joiner installs this artifact BY FIAT off a snapshot session, and a
//! restarted node reads it off disk, so it is untrusted input like any other
//! wire codec here. `core`-friendly like its neighbours `v2::schedule` and
//! `v2::settings`: no I/O, no `sha2` — `crc32fast` is already a dependency of
//! this crate.
//!
//! Layout v1 IS the byte layout the plan-1/plan-2 `ClusterFsm::freeze` this
//! module replaced produced — see `cluster_image_roundtrips_and_layout_is_frozen`'s
//! `PLAN1_FIXTURE`, captured from that pre-move `freeze` output. It is now
//! **read-only compatibility**: `encode_cluster_image` always emits v2,
//! which appends the two length-prefixed pin and report blobs after the
//! settings record, so a fresh artifact no longer reproduces that fixture
//! byte for byte. The fixture still has to DECODE, because a node restarting
//! across the `2.13.0` flag day reads its own pre-upgrade artifact off disk.
//!
//! `membership`, `table` and `settings` are returned as opaque byte slices,
//! not decoded here: the caller (`uc_node::cluster_fsm`) already owns
//! `config::decode_config`, `schedule::decode_schedule_table` and
//! `settings::decode_settings`, and decoding them here would just duplicate
//! that dispatch for no gain — the image codec's own job is only the outer
//! framing and the CRC.

use super::settings::{SETTINGS_LEN, SETTINGS_LEN_V1, SETTINGS_LEN_V2};

pub const CLUSTER_IMAGE_MAGIC: &[u8; 8] = b"UCCLUST1";
/// The image layout's version, refused by [`decode_cluster_image`] when
/// unknown.
///
/// Bumped to 2 by plan B1 for the pin and report blobs; a version-1 image
/// is still ACCEPTED on read, with both blobs empty — the settings v1/v2
/// precedent, since a restarting `2.12.0` node reads its own artifact.
/// Bumped to 3 (#33): a trailing length-prefixed `running` blob after
/// `reports`. A v1 or v2 image is still ACCEPTED on read, with `running`
/// empty — the same precedent, since a restarting `2.13.0` node reads its
/// own pre-upgrade artifact.
/// Bumped to 4 (catalog spec §7): a trailing length-prefixed `catalog` blob
/// after `running`. A v1–v3 image is still ACCEPTED on read, with `catalog`
/// empty — the `Empty` state of catalog spec §4.5, since the flag day does
/// not migrate sets built before the catalog existed.
/// Bumped to 5 (snapshot-lifecycle spec §7.2): the `reports` blob's entries
/// and the `catalog` blob's row entries carry a `size u64`. The OUTER framing
/// is identical to v4 — the blobs are opaque here — so a v1–v4 image is still
/// ACCEPTED; its reader picks the blob layout from [`cluster_image_version`]
/// (`uc_node::cluster_fsm` decodes v1–v4 blobs with every size `0`).
pub const CLUSTER_IMAGE_VERSION: u32 = 5;

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
    /// The `v2::upgrade` running-version-list bytes (opaque here; empty for
    /// a v1/v2 image or a cluster with no running versions recorded).
    pub running: &'a [u8],
    /// The `v2::catalog` set-list bytes (opaque here; empty for a v1–v3
    /// image or a cluster whose catalog holds no set).
    pub catalog: &'a [u8],
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
    let running_len = payload_len_prefix(p.running.len())?;
    out.extend_from_slice(&running_len.to_le_bytes());
    out.extend_from_slice(p.running);
    let catalog_len = payload_len_prefix(p.catalog.len())?;
    out.extend_from_slice(&catalog_len.to_le_bytes());
    out.extend_from_slice(p.catalog);
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
    if !(1..=CLUSTER_IMAGE_VERSION).contains(&version) {
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
    let (settings, pins, reports, running, catalog) = if version == 1 {
        // 2.11.0/2.12.0 layout: the remainder is exactly one settings
        // record, self-versioned and exact-length per version
        // (`settings::decode_settings`) — never a slice that could run past
        // `body`'s end. A 2.11.0 artifact carries v1 — jumbo spec §5.5.
        // Size it by ITS OWN version word, exactly as the v2 branch does,
        // and require the remainder to be that length and nothing else.
        // Accepting `rest == SETTINGS_LEN || rest == SETTINGS_LEN_V1`
        // without consulting the word would admit a 33-byte tail that says
        // `version = 1`: `decode_settings` would then read a 29-byte record
        // and the 4 trailing bytes would vanish on re-encode, so decode and
        // re-encode would not round-trip for an input we accepted.
        let sl = match u32_at(o)? {
            1 => SETTINGS_LEN_V1,
            2 => SETTINGS_LEN_V2,
            3 => SETTINGS_LEN,
            _ => return None,
        };
        let rest = body.len().checked_sub(o)?;
        if rest != sl {
            return None;
        }
        (
            &body[o..],
            &body[body.len()..],
            &body[body.len()..],
            &body[body.len()..],
            &body[body.len()..],
        )
    } else {
        // v2/v3/v4: the settings record is sized by ITS OWN version word
        // (the record is exact-length per version), then two length-prefixed
        // blobs (pins, reports), then — v3 and later — a third
        // length-prefixed blob (running), then — v4 only — a fourth
        // (catalog), then nothing.
        let sl = match u32_at(o)? {
            1 => SETTINGS_LEN_V1,
            2 => SETTINGS_LEN_V2,
            3 => SETTINGS_LEN,
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
        let running = if version >= 3 {
            let nl = u32_at(o)? as usize;
            o += 4;
            let r = o.checked_add(nl).and_then(|end| body.get(o..end))?;
            o += nl;
            r
        } else {
            &body[body.len()..]
        };
        let catalog = if version >= 4 {
            let cl = u32_at(o)? as usize;
            o += 4;
            let c = o.checked_add(cl).and_then(|end| body.get(o..end))?;
            o += cl;
            c
        } else {
            &body[body.len()..]
        };
        if o != body.len() {
            return None;
        }
        (settings, pins, reports, running, catalog)
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
        running,
        catalog,
    })
}

/// The version word of an image [`decode_cluster_image`] ACCEPTS, else
/// `None`. The cluster FSM reads it to choose the sized (v5) or unsized
/// (v1–v4) blob decoders; kept out of [`ClusterImageParts`] so a decoded image
/// still re-encodes to parts that compare equal (the fuzz target's property).
pub fn cluster_image_version(buf: &[u8]) -> Option<u32> {
    decode_cluster_image(buf)?;
    Some(u32::from_le_bytes(buf.get(8..12)?.try_into().ok()?))
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

    /// Captured 2026-09-27 (#33 task 3, BEFORE the v3 encoder change) by
    /// temporarily instrumenting this test module to call the then-current
    /// (v2) `encode_cluster_image` with genesis membership/table, a v2
    /// (33 B) settings record, one `UpgradePin` (`row: 2, from:
    /// 0x0100_0000, to: 0x0102_0000, origin: 4096`) as the pins blob, and no
    /// reports, then printing the resulting bytes and pasting them here as a
    /// `const`. This pins the "a v2 image decodes with an empty `running`"
    /// requirement against a fixture the v3 encoder never touched — a
    /// fixture produced by the v3 encoder would prove nothing about v2
    /// decode compatibility.
    #[rustfmt::skip]
    const PLAN_B1_V2_FIXTURE: &[u8] = &[
        0x55, 0x43, 0x43, 0x4C, 0x55, 0x53, 0x54, 0x31, 0x02, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00,
        0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x02, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xE5, 0xFB, 0x03,
    ];

    fn v2_fixture_image() -> &'static [u8] {
        PLAN_B1_V2_FIXTURE
    }

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

    /// The CURRENT (`SETTINGS_LEN` B, version 3, `retain_sets` field
    /// included) settings record: `encode_settings(&Settings::
    /// genesis_default())`'s bytes — `encode_settings` always emits the
    /// latest version, so this is no longer the 33 B v2 shape the name
    /// recalls; it names the role ("the settings record `genesis_parts`
    /// carries"), not a frozen byte count.
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
            running: &[],
            catalog: &[],
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

    /// Jumbo spec §5.5 / catalog spec §7: a `2.11.0` artifact's tail is a
    /// 29-byte v1 settings record and must keep framing and decoding — with
    /// the new fields at their baseline meaning — while the CURRENT tail
    /// (`SETTINGS_LEN` B) frames alongside it. Any OTHER remainder is
    /// refused: the length is exact per version, so a truncated or padded
    /// tail can never be read as a prefix.
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
                running: &[],
                catalog: &[],
            };
            let mut img = Vec::new();
            encode_cluster_image(&parts, &mut img).expect("well under u32::MAX");
            let d = decode_cluster_image(&img).expect("both record versions frame");
            assert_eq!(d.settings.len(), tail.len());
            assert_eq!(decode_settings(d.settings).unwrap().datagram_mtu, 0);
        }
        assert_eq!(v1.len(), 29);
        assert_eq!(v2.len(), SETTINGS_LEN);

        // 28/31/34 bytes: none of the three valid settings-record lengths
        // (29, 33, 35). The CRC is correct — this is the framing check
        // refusing it, not corruption.
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
                running: &[],
                catalog: &[],
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

    /// The v1 branch sizes the settings tail by the record's OWN version
    /// word, not by "any accepted length": a v1-FRAMED image whose
    /// `SETTINGS_LEN`-byte tail claims `version = 1` is not a v1 record
    /// padded out, it is a length the codec cannot re-encode, so it is
    /// refused rather than silently truncated to 29. (Name kept from when
    /// `SETTINGS_LEN` was 33; it is 35 now, catalog spec §7 — the test's
    /// point is unchanged.)
    #[test]
    fn a_v1_image_whose_33_byte_tail_claims_version_1_is_refused() {
        let (membership, table, _) = genesis_parts();
        let mut tail = v1_settings_blob();
        tail.resize(SETTINGS_LEN, 0); // SETTINGS_LEN bytes, version word still 1
        assert_eq!(tail.len(), SETTINGS_LEN);
        assert_eq!(&tail[0..4], &1u32.to_le_bytes());

        let frame_v1 = |settings: &[u8]| -> Vec<u8> {
            // Hand-framed as a VERSION-1 image (encode_cluster_image only
            // emits v2), so the tail reaches the v1 branch.
            let mut body = Vec::new();
            body.extend_from_slice(CLUSTER_IMAGE_MAGIC);
            body.extend_from_slice(&1u32.to_le_bytes());
            body.extend_from_slice(&640u64.to_le_bytes()); // applied
            body.extend_from_slice(&0u64.to_le_bytes()); // table_position
            body.extend_from_slice(&320u64.to_le_bytes()); // settings_position
            body.extend_from_slice(&(membership.len() as u32).to_le_bytes());
            body.extend_from_slice(&membership);
            body.extend_from_slice(&(table.len() as u32).to_le_bytes());
            body.extend_from_slice(&table);
            body.extend_from_slice(settings);
            let crc = crc32fast::hash(&body);
            body.extend_from_slice(&crc.to_le_bytes());
            body
        };

        assert_eq!(
            decode_cluster_image(&frame_v1(&tail)),
            None,
            "the CRC is correct; the version word disagrees with the length"
        );

        // The control: the same framing with the honest 29-byte v1 tail
        // decodes, so it is the length rule refusing above, not the frame.
        let ok = frame_v1(&v1_settings_blob());
        let d = decode_cluster_image(&ok).expect("an honest v1 tail decodes");
        assert_eq!(d.settings.len(), SETTINGS_LEN_V1);
        assert!(d.pins.is_empty() && d.reports.is_empty());
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
            running: &[],
            catalog: &[],
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
            running: &[],
            catalog: &[],
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
            running: &[],
            catalog: &[],
        };
        let mut img = Vec::new();
        encode_cluster_image(&p, &mut img).unwrap();
        assert_eq!(
            &img[8..12],
            &CLUSTER_IMAGE_VERSION.to_le_bytes(),
            "current version"
        );
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
            running: &[],
            catalog: &[],
        };
        let mut img = Vec::new();
        encode_cluster_image(&p, &mut img).unwrap();
        let d = decode_cluster_image(&img).unwrap();
        assert_eq!(d.settings, &v1_settings_blob()[..]);
    }

    /// #33 task 3: layout v3 appends a trailing length-prefixed `running`
    /// blob after `reports`. A v1 or v2 image (this crate's own v2
    /// encoder's prior output, captured BEFORE this change as
    /// `PLAN_B1_V2_FIXTURE`) still decodes, with `running` empty.
    #[test]
    fn v3_image_round_trips_the_running_blob_and_v2_reads_empty() {
        let running = [7u8; 16];
        let p = ClusterImageParts {
            applied: 4096,
            table_position: 0,
            settings_position: 0,
            membership: b"m",
            table: b"t",
            settings: &v2_settings(),
            pins: &[],
            reports: &[],
            running: &running,
            catalog: &[],
        };
        let mut img = Vec::new();
        encode_cluster_image(&p, &mut img).unwrap();
        assert_eq!(
            u32::from_le_bytes(img[8..12].try_into().unwrap()),
            CLUSTER_IMAGE_VERSION,
            "encode_cluster_image writes the current layout (v3 or later)"
        );
        assert_eq!(decode_cluster_image(&img).unwrap().running, &running[..]);
        // A v2 image (the existing v2 fixture, captured before this task's
        // encoder change) decodes with empty running — and its pins/reports
        // (the v2-era fields) survive untouched: the fixture was captured
        // with one UpgradePin and no reports (see PLAN_B1_V2_FIXTURE's doc
        // comment for its exact provenance).
        use super::super::upgrade::{UpgradePin, encode_upgrade_pin};
        let mut expected_pins = Vec::new();
        encode_upgrade_pin(
            &UpgradePin {
                row: 2,
                from: 0x0100_0000,
                to: 0x0102_0000,
                origin: 4096,
            },
            &mut expected_pins,
        );
        let v2 = v2_fixture_image();
        let d = decode_cluster_image(v2).unwrap();
        assert_eq!(
            u32::from_le_bytes(v2[8..12].try_into().unwrap()),
            2,
            "the fixture itself is a genuine v2 image, not a v3 one"
        );
        assert_eq!(d.running, &[] as &[u8]);
        assert_eq!(
            d.pins,
            &expected_pins[..],
            "the v2 fixture's one pin record survives decode intact"
        );
        assert_eq!(d.reports, &[] as &[u8]);
        // And the plan-1-era v1 fixture too.
        assert_eq!(
            decode_cluster_image(PLAN1_FIXTURE).unwrap().running,
            &[] as &[u8]
        );
    }

    /// Catalog spec §7: layout v4 appends a trailing length-prefixed
    /// `catalog` blob after `running` (opaque here — the `v2::catalog`
    /// set-list bytes). A v3 image (a v4 image with no catalog prefix and
    /// version word 3) still decodes, with `catalog` empty, and a catalog
    /// length that runs past the CRC is refused.
    #[test]
    fn v4_image_round_trips_a_catalog_blob_and_v3_reads_empty() {
        let running = [7u8; 12];
        let catalog = [5u8; 137];
        let p = ClusterImageParts {
            applied: 8192,
            table_position: 0,
            settings_position: 0,
            membership: b"m",
            table: b"t",
            settings: &v2_settings(),
            pins: &[],
            reports: &[],
            running: &running,
            catalog: &catalog,
        };
        let mut img = Vec::new();
        encode_cluster_image(&p, &mut img).unwrap();
        assert_eq!(
            &img[8..12],
            &CLUSTER_IMAGE_VERSION.to_le_bytes(),
            "current layout"
        );
        assert_eq!(decode_cluster_image(&img), Some(p));

        // The same parts without a catalog, re-framed as v3: drop the
        // 4-byte zero catalog prefix, write version 3, re-seal.
        let mut v4_empty = Vec::new();
        encode_cluster_image(&ClusterImageParts { catalog: &[], ..p }, &mut v4_empty).unwrap();
        let l = v4_empty.len();
        assert_eq!(&v4_empty[l - 8..l - 4], &0u32.to_le_bytes());
        let mut v3 = v4_empty[..l - 8].to_vec();
        v3[8..12].copy_from_slice(&3u32.to_le_bytes());
        let crc = crc32fast::hash(&v3);
        v3.extend_from_slice(&crc.to_le_bytes());
        let d = decode_cluster_image(&v3).expect("a v3 image still decodes");
        assert_eq!(d.running, &running[..]);
        assert_eq!(d.catalog, &[] as &[u8]);

        // A catalog length one past what is there is refused (exact framing).
        let mut bad = img.clone();
        let off = img.len() - 4 - catalog.len() - 4;
        bad[off..off + 4].copy_from_slice(&(catalog.len() as u32 + 1).to_le_bytes());
        fix_crc(&mut bad);
        assert_eq!(decode_cluster_image(&bad), None);
    }

    /// Recompute the trailing CRC after a deliberate mutation.
    fn fix_crc(img: &mut [u8]) {
        let l = img.len();
        let crc = crc32fast::hash(&img[..l - 4]);
        img[l - 4..].copy_from_slice(&crc.to_le_bytes());
    }

    /// Snapshot-lifecycle spec §7.2: images are written v5; the version word
    /// of an accepted image is readable.
    #[test]
    fn images_are_v5_and_the_version_word_of_an_accepted_image_is_readable() {
        assert_eq!(CLUSTER_IMAGE_VERSION, 5);
        let (membership, table, settings) = genesis_parts();
        let parts = ClusterImageParts {
            applied: 300,
            table_position: 0,
            settings_position: 0,
            membership: &membership,
            table: &table,
            settings: &settings,
            pins: &[],
            reports: &[],
            running: &[],
            catalog: &[],
        };
        let mut img = Vec::new();
        encode_cluster_image(&parts, &mut img).unwrap();
        assert_eq!(cluster_image_version(&img), Some(5));
        let body_end = img.len() - 4;
        let mut v4 = img[..body_end].to_vec();
        v4[8..12].copy_from_slice(&4u32.to_le_bytes());
        let crc = crc32fast::hash(&v4);
        v4.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(cluster_image_version(&v4), Some(4));
        assert_eq!(
            decode_cluster_image(&v4),
            Some(parts),
            "v4 frames identically"
        );
        let mut bad = img.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert_eq!(
            cluster_image_version(&bad),
            None,
            "only an ACCEPTED image has a version"
        );
    }
}
