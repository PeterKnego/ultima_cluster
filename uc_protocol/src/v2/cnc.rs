// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `cnc.dat` v2 page layout (M5 spec §7-ish; see
//! docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md). One
//! fixed-size 4 KiB page shared node/service/clients, replacing the M1-M4
//! heap-side `LogCounters` with an mmap'd layout so every process sees the
//! same atomics.
//!
//! Layout (byte offsets):
//!
//! ```text
//! 0     header (magic, version, node_id, instance_id, app_id, created_ns,
//!               buffer_bytes, max_payload, crc32)      124 B fields + crc @124
//! 256   LogCounters   (append, durable, sent, commit)       4 × 64 B lines
//! 512   ServiceProgress (service_applied, service_epoch,
//!                        output_completed)                  3 × 64 B lines
//! 704   NodeStatusV2  (term, flags, leader_hint,
//!                      node_heartbeat_ns, service_heartbeat_ns,
//!                      output_progress, next_client_id)     7 × 64 B lines
//! 1152..4096  reserved (zero) — M6 per-follower observability slots
//! ```
//!
//! **crc split (deliberate):** this module is `core`-only (no_std-friendly —
//! part of the multi-language protocol gate) and carries no crc dependency,
//! so [`write_cnc_header`]/[`read_cnc_header`] are crc-agnostic: they read
//! and write every fixed header field EXCEPT the crc32 word at
//! [`CNC_OFF_HEADER_CRC`], which they leave untouched. `uc2_log::cnc`
//! computes/writes the crc32 (workspace `crc32fast`) after
//! `write_cnc_header`, and validates it before trusting a page written by
//! another process. This mirrors the v1 `cnc.rs` split, just made explicit
//! for v2's no_std posture.

/// Magic bytes at the start of every v2 cnc page.
pub const CNC_MAGIC: &[u8; 8] = b"UC2CNC\0\0";
/// Fixed total page size — one page, shared node/service/clients.
pub const CNC_PAGE_LEN: usize = 4096;
/// Packed like `uc_protocol::ProtocolVersion`: `(major << 24) | (minor << 16) | patch`.
#[allow(clippy::identity_op)] // (0 << 16) spells out the packing explicitly (minor = 0)
pub const CNC_V2_VERSION: u32 = (2 << 24) | (0 << 16);

// ---- header (byte offsets) ------------------------------------------------
pub const CNC_OFF_MAGIC: usize = 0; // [u8; 8]
pub const CNC_OFF_VERSION: usize = 8; // u32 LE
pub const CNC_OFF_NODE_ID: usize = 12; // u32 LE
pub const CNC_OFF_INSTANCE_LO: usize = 16; // u64 LE
pub const CNC_OFF_INSTANCE_HI: usize = 24; // u64 LE
pub const CNC_OFF_APP_ID: usize = 32; // [u8; 64] utf-8, NUL-padded
pub const CNC_OFF_CREATED_NS: usize = 96; // u64 LE
pub const CNC_OFF_BUFFER_BYTES: usize = 104; // u64 LE (log-buffer capacity — geometry for attachers)
pub const CNC_OFF_MAX_PAYLOAD: usize = 112; // u32 LE
pub const CNC_OFF_HEADER_CRC: usize = 124; // u32 LE, crc32 over [0..124) — written/checked by uc2_log

// ---- counter lines (each one 64-byte cache line, single writer noted) -----
pub const CNC_OFF_APPEND: usize = 256; // writer: leader appender / follower receiver
pub const CNC_OFF_DURABLE: usize = 320; // writer: archive agent
pub const CNC_OFF_SENT: usize = 384; // writer: sender agent
pub const CNC_OFF_COMMIT: usize = 448; // writer: consensus agent
pub const CNC_OFF_SERVICE_APPLIED: usize = 512; // writer: service apply agent
pub const CNC_OFF_SERVICE_EPOCH: usize = 576; // writer: service (attach-time bump)
pub const CNC_OFF_OUTPUT_COMPLETED: usize = 640; // writer: service output loop
pub const CNC_OFF_TERM: usize = 704; // writer: consensus agent
pub const CNC_OFF_NODE_FLAGS: usize = 768; // writer: consensus; bit0=leader, bit1=can_serve
pub const CNC_OFF_LEADER_HINT: usize = 832; // writer: consensus; u64::MAX = unknown
pub const CNC_OFF_NODE_HEARTBEAT_NS: usize = 896; // writer: consensus agent
pub const CNC_OFF_SERVICE_HEARTBEAT_NS: usize = 960; // writer: service apply agent
pub const CNC_OFF_OUTPUT_PROGRESS: usize = 1024; // writer: consensus (persisted marker mirror)
pub const CNC_OFF_NEXT_CLIENT_ID: usize = 1088; // fetch_add by clients; init 1
// 1152..4096 reserved (zero) — M6 per-follower observability slots land here

pub const NODE_FLAG_LEADER: u64 = 1;
pub const NODE_FLAG_CAN_SERVE: u64 = 2;

/// Decoded fixed header fields (crc-agnostic — see module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CncHeader {
    pub version: u32,
    pub node_id: u32,
    pub instance_id: u128,
    pub created_ns: u64,
    pub buffer_bytes: u64,
    pub max_payload: u32,
}

/// Write every fixed header field (magic, version, node_id, instance_id,
/// app_id, created_ns, buffer_bytes, max_payload) EXCEPT the crc32 word at
/// [`CNC_OFF_HEADER_CRC`] — that byte range is left untouched; `uc2_log`
/// computes and writes it immediately after calling this.
///
/// `page` must be at least [`CNC_PAGE_LEN`] bytes. Panics if `app_id` is
/// longer than 63 bytes (the field is 64 bytes and must hold a NUL
/// terminator so [`read_cnc_app_id`] can find the end).
pub fn write_cnc_header(page: &mut [u8], h: &CncHeader, app_id: &str) {
    assert!(page.len() >= CNC_PAGE_LEN, "cnc page too small");
    assert!(app_id.len() <= 63, "app_id must fit in 63 bytes (NUL-padded 64-byte field)");

    page[CNC_OFF_MAGIC..CNC_OFF_MAGIC + 8].copy_from_slice(CNC_MAGIC);
    page[CNC_OFF_VERSION..CNC_OFF_VERSION + 4].copy_from_slice(&h.version.to_le_bytes());
    page[CNC_OFF_NODE_ID..CNC_OFF_NODE_ID + 4].copy_from_slice(&h.node_id.to_le_bytes());
    page[CNC_OFF_INSTANCE_LO..CNC_OFF_INSTANCE_LO + 8]
        .copy_from_slice(&(h.instance_id as u64).to_le_bytes());
    page[CNC_OFF_INSTANCE_HI..CNC_OFF_INSTANCE_HI + 8]
        .copy_from_slice(&((h.instance_id >> 64) as u64).to_le_bytes());

    let mut app_id_bytes = [0u8; 64];
    app_id_bytes[..app_id.len()].copy_from_slice(app_id.as_bytes());
    page[CNC_OFF_APP_ID..CNC_OFF_APP_ID + 64].copy_from_slice(&app_id_bytes);

    page[CNC_OFF_CREATED_NS..CNC_OFF_CREATED_NS + 8].copy_from_slice(&h.created_ns.to_le_bytes());
    page[CNC_OFF_BUFFER_BYTES..CNC_OFF_BUFFER_BYTES + 8]
        .copy_from_slice(&h.buffer_bytes.to_le_bytes());
    page[CNC_OFF_MAX_PAYLOAD..CNC_OFF_MAX_PAYLOAD + 4]
        .copy_from_slice(&h.max_payload.to_le_bytes());
}

/// Decode the fixed header fields. Returns `None` for a short page or a
/// magic mismatch. Does NOT check the crc32 word (see module doc) — callers
/// that need attach-time integrity (an mmap shared with another process)
/// must validate the crc themselves (`uc2_log::cnc` does this).
pub fn read_cnc_header(page: &[u8]) -> Option<CncHeader> {
    if page.len() < CNC_PAGE_LEN {
        return None;
    }
    if &page[CNC_OFF_MAGIC..CNC_OFF_MAGIC + 8] != CNC_MAGIC {
        return None;
    }
    let version = u32::from_le_bytes(page[CNC_OFF_VERSION..CNC_OFF_VERSION + 4].try_into().ok()?);
    let node_id = u32::from_le_bytes(page[CNC_OFF_NODE_ID..CNC_OFF_NODE_ID + 4].try_into().ok()?);
    let lo = u64::from_le_bytes(
        page[CNC_OFF_INSTANCE_LO..CNC_OFF_INSTANCE_LO + 8].try_into().ok()?,
    );
    let hi = u64::from_le_bytes(
        page[CNC_OFF_INSTANCE_HI..CNC_OFF_INSTANCE_HI + 8].try_into().ok()?,
    );
    let created_ns =
        u64::from_le_bytes(page[CNC_OFF_CREATED_NS..CNC_OFF_CREATED_NS + 8].try_into().ok()?);
    let buffer_bytes = u64::from_le_bytes(
        page[CNC_OFF_BUFFER_BYTES..CNC_OFF_BUFFER_BYTES + 8].try_into().ok()?,
    );
    let max_payload = u32::from_le_bytes(
        page[CNC_OFF_MAX_PAYLOAD..CNC_OFF_MAX_PAYLOAD + 4].try_into().ok()?,
    );
    Some(CncHeader {
        version,
        node_id,
        instance_id: ((hi as u128) << 64) | (lo as u128),
        created_ns,
        buffer_bytes,
        max_payload,
    })
}

/// Read the NUL-trimmed app_id string. Returns an empty string on bad utf-8
/// (never panics — this reads bytes from a page potentially written by a
/// mismatched/foreign process).
pub fn read_cnc_app_id(page: &[u8]) -> &str {
    let bytes = &page[CNC_OFF_APP_ID..CNC_OFF_APP_ID + 64];
    let nul = bytes.iter().position(|&b| b == 0).unwrap_or(64);
    core::str::from_utf8(&bytes[..nul]).unwrap_or("")
}

/// Compatible if same major and `peer`'s minor is not newer than `local`'s
/// (same rule as `ProtocolVersion::compatible_with`, spelled out for the
/// packed `u32` directly since this module is core-only).
pub const fn version_compatible(local: u32, peer: u32) -> bool {
    let local_major = local >> 24;
    let peer_major = peer >> 24;
    let local_minor = (local >> 16) & 0xFF;
    let peer_minor = (peer >> 16) & 0xFF;
    local_major == peer_major && peer_minor <= local_minor
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(node_id: u32) -> CncHeader {
        CncHeader {
            version: CNC_V2_VERSION,
            node_id,
            instance_id: 0xABCD_EF01_2345_6789,
            created_ns: 123,
            buffer_bytes: 1 << 20,
            max_payload: 256,
        }
    }

    #[test]
    fn header_roundtrip() {
        let h = header(7);
        let mut page = vec![0u8; CNC_PAGE_LEN];
        write_cnc_header(&mut page, &h, "kv");
        let out = read_cnc_header(&page).expect("well-formed header");
        assert_eq!(out, h);
        assert_eq!(read_cnc_app_id(&page), "kv");
    }

    #[test]
    fn header_write_pins_literal_bytes_0_16() {
        // Pin the ABSOLUTE wire layout with literal bytes — this module is
        // frozen once M5 IPC lands, so a write/read round trip alone isn't
        // enough (both sides could agree on a wrong layout). Same style as
        // the v2::datagram pins.
        let h = header(7);
        let mut page = vec![0u8; CNC_PAGE_LEN];
        write_cnc_header(&mut page, &h, "kv");
        // magic
        assert_eq!(&page[0..8], b"UC2CNC\0\0");
        // version = (2<<24)|(0<<16) = 0x0200_0000 -> LE [0,0,0,2]
        assert_eq!(&page[8..12], &[0x00, 0x00, 0x00, 0x02]);
        // node_id = 7 -> LE [7,0,0,0]
        assert_eq!(&page[12..16], &[7, 0, 0, 0]);
    }

    #[test]
    fn instance_id_high_bits_pin_the_lo_hi_split() {
        // An instance_id with DIFFERENT non-zero halves, pinned as literal
        // bytes at both offsets. A write/read roundtrip alone cannot catch
        // a consistently-swapped lo/hi (it would roundtrip fine); only the
        // absolute byte pin does.
        let h = CncHeader {
            instance_id: (0xFEED_FACE_CAFE_BEEF_u128 << 64) | 0x0123_4567_89AB_CDEF_u128,
            ..header(1)
        };
        let mut page = vec![0u8; CNC_PAGE_LEN];
        write_cnc_header(&mut page, &h, "kv");
        // low half 0x0123_4567_89AB_CDEF -> LE at CNC_OFF_INSTANCE_LO (16)
        assert_eq!(
            &page[CNC_OFF_INSTANCE_LO..CNC_OFF_INSTANCE_LO + 8],
            &[0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01]
        );
        // high half 0xFEED_FACE_CAFE_BEEF -> LE at CNC_OFF_INSTANCE_HI (24)
        assert_eq!(
            &page[CNC_OFF_INSTANCE_HI..CNC_OFF_INSTANCE_HI + 8],
            &[0xEF, 0xBE, 0xFE, 0xCA, 0xCE, 0xFA, 0xED, 0xFE]
        );
        assert_eq!(read_cnc_header(&page).map(|o| o.instance_id), Some(h.instance_id));
    }

    #[test]
    fn app_id_63_bytes_is_the_longest_legal_value() {
        // Boundary success case: 63 bytes fills the field up to the
        // mandatory NUL terminator (64 bytes panics — covered separately).
        let h = header(1);
        let mut page = vec![0u8; CNC_PAGE_LEN];
        let app_id = "x".repeat(63);
        write_cnc_header(&mut page, &h, &app_id);
        assert_eq!(read_cnc_app_id(&page), app_id);
        // the final byte of the 64-byte field is the NUL terminator
        assert_eq!(page[CNC_OFF_APP_ID + 63], 0);
    }

    #[test]
    fn read_rejects_bad_magic_or_short_page() {
        // all-zero page: magic mismatch
        let page = vec![0u8; CNC_PAGE_LEN];
        assert!(read_cnc_header(&page).is_none());
        // too short to even hold the header
        let short = vec![0u8; 16];
        assert!(read_cnc_header(&short).is_none());
    }

    #[test]
    fn read_is_crc_agnostic_by_design() {
        // Documents the crc split (module doc): flipping the crc byte at
        // CNC_OFF_HEADER_CRC does NOT affect read_cnc_header — this module
        // never looks at it. `uc2_log::cnc::CncPage::open_file` is where a
        // flipped crc byte is actually rejected (its own test covers that).
        let h = header(7);
        let mut page = vec![0u8; CNC_PAGE_LEN];
        write_cnc_header(&mut page, &h, "kv");
        page[CNC_OFF_HEADER_CRC] ^= 0xFF;
        assert_eq!(read_cnc_header(&page), Some(h));
    }

    #[test]
    fn app_id_read_handles_bad_utf8_and_full_64_bytes() {
        let mut page = vec![0u8; CNC_PAGE_LEN];
        // no NUL anywhere in the 64-byte field, and not valid utf-8
        page[CNC_OFF_APP_ID..CNC_OFF_APP_ID + 64].fill(0xFF);
        assert_eq!(read_cnc_app_id(&page), "");
    }

    #[test]
    #[should_panic(expected = "app_id must fit")]
    fn write_rejects_app_id_over_63_bytes() {
        let h = header(0);
        let mut page = vec![0u8; CNC_PAGE_LEN];
        write_cnc_header(&mut page, &h, &"x".repeat(64));
    }

    #[test]
    fn version_compatible_same_major_lower_peer_minor_ok() {
        let local = (2u32 << 24) | (5 << 16);
        let peer = (2u32 << 24) | (3 << 16);
        assert!(version_compatible(local, peer));
    }

    #[test]
    fn version_compatible_rejects_newer_peer_minor_or_different_major() {
        let local = (2u32 << 24) | (3 << 16);
        let newer_minor = (2u32 << 24) | (5 << 16);
        assert!(!version_compatible(local, newer_minor));
        let other_major = (3u32 << 24) | (0 << 16);
        assert!(!version_compatible(local, other_major));
    }

    #[test]
    fn offsets_do_not_overlap() {
        // header
        assert_eq!(CNC_OFF_MAGIC, 0);
        assert_eq!(CNC_OFF_VERSION, 8);
        assert_eq!(CNC_OFF_NODE_ID, 12);
        assert_eq!(CNC_OFF_INSTANCE_LO, 16);
        assert_eq!(CNC_OFF_INSTANCE_HI, 24);
        assert_eq!(CNC_OFF_APP_ID, 32);
        assert_eq!(CNC_OFF_CREATED_NS, 96);
        assert_eq!(CNC_OFF_BUFFER_BYTES, 104);
        assert_eq!(CNC_OFF_MAX_PAYLOAD, 112);
        assert_eq!(CNC_OFF_HEADER_CRC, 124);
        // counter lines, 64 B apart
        assert_eq!(CNC_OFF_APPEND, 256);
        assert_eq!(CNC_OFF_DURABLE - CNC_OFF_APPEND, 64);
        assert_eq!(CNC_OFF_SENT - CNC_OFF_APPEND, 128);
        assert_eq!(CNC_OFF_COMMIT - CNC_OFF_APPEND, 192);
        assert_eq!(CNC_OFF_SERVICE_APPLIED, 512);
        assert_eq!(CNC_OFF_SERVICE_EPOCH - CNC_OFF_SERVICE_APPLIED, 64);
        assert_eq!(CNC_OFF_OUTPUT_COMPLETED - CNC_OFF_SERVICE_APPLIED, 128);
        assert_eq!(CNC_OFF_TERM, 704);
        assert_eq!(CNC_OFF_NODE_FLAGS - CNC_OFF_TERM, 64);
        assert_eq!(CNC_OFF_LEADER_HINT - CNC_OFF_TERM, 128);
        assert_eq!(CNC_OFF_NODE_HEARTBEAT_NS - CNC_OFF_TERM, 192);
        assert_eq!(CNC_OFF_SERVICE_HEARTBEAT_NS - CNC_OFF_TERM, 256);
        assert_eq!(CNC_OFF_OUTPUT_PROGRESS - CNC_OFF_TERM, 320);
        assert_eq!(CNC_OFF_NEXT_CLIENT_ID - CNC_OFF_TERM, 384);
        assert_eq!(NODE_FLAG_LEADER, 1);
        assert_eq!(NODE_FLAG_CAN_SERVE, 2);
    }
}
