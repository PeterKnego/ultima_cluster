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
//! 1152  SnapshotSlots (service_snapshot_pos, node_snapshot_floor, incoming_snapshot_pos) 3 × 64 B lines
//! 1344  archive_first_base                                  1 × 64 B line
//! 1408  PeerSlots[8]   (per peer: id_and_role, reported_durable,
//!                       advertised_limit, naks_served_plus_replay) 8 × 256 B
//! 3456  config/admin/observability band: config_version, config_pending,
//!       admin_req, admin_resp, admission_bytes, seal_failures,
//!       free_disk_bytes (each field's own doc comment below pins its exact
//!       offset and "next free" note — this line is deliberately not kept
//!       byte-exact, to stop it drifting stale again as fields land)
//! 3904..4096  reserved (zero)
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
// M6 Task 3: SnapshotSlots (service_snapshot_pos, node_snapshot_floor). ONE new
// slot landing now; the rest of the 1152.. band is Task 9 (peer/observability).
pub const CNC_OFF_SERVICE_SNAPSHOT_POS: usize = 1152; // writer: service snapshot builder thread; position S of the newest COMPLETE on-disk snapshot; 0 = none
pub const CNC_OFF_NODE_SNAPSHOT_FLOOR: usize = 1216; // writer: consensus (Task 4 mirror); init 0
pub const CNC_OFF_INCOMING_SNAPSHOT_POS: usize = 1280; // writer: consensus (Task 6 mirror of the receiver's completed inbound snapshot); observability; 0 = none
// M6 Task 9: ops observability band. `archive_first_base` mirrors the archive
// agent's first-retained log position (the purge floor); comparing it against
// `node_snapshot_floor` is the "purge caught up to snapshot" health check.
pub const CNC_OFF_ARCHIVE_FIRST_BASE: usize = 1344; // writer: consensus (mirrors the archive agent's first-base atomic); init = boot first_base

// M6 Task 9: per-peer observability slots. `CNC_MAX_PEER_SLOTS` fixed-stride
// records; the leader's consensus + sender agents publish one slot per peer
// (bounded write: once per duty cycle, never per datagram). A slot is dormant
// (all-zero) on a follower and for unused peer indices. Layout per slot (each
// field its own 64-byte cache line to keep single-writer stores contention-free):
//   +0   peer_id_and_role  u64 = (peer_id << 8) | role_bits   writer: consensus (boot-once)
//   +64  reported_durable  u64                                writer: consensus (Report intake)
//   +128 advertised_limit  u64                                writer: sender    (STATUS intake)
//   +192 naks_plus_replay  u64 = (naks_served << 32) | replay writer: sender
pub const CNC_OFF_PEER_SLOTS: usize = 1408;
pub const CNC_PEER_SLOT_STRIDE: usize = 256;
pub const CNC_MAX_PEER_SLOTS: usize = 8;
// Per-slot field offsets (relative to the slot base).
pub const CNC_PEER_OFF_ID_AND_ROLE: usize = 0;
pub const CNC_PEER_OFF_REPORTED_DURABLE: usize = 64;
pub const CNC_PEER_OFF_ADVERTISED_LIMIT: usize = 128;
pub const CNC_PEER_OFF_NAKS_PLUS_REPLAY: usize = 192;
// Role bits packed into the low byte of `peer_id_and_role`.
pub const CNC_PEER_ROLE_VOTER: u8 = 1;
pub const CNC_PEER_ROLE_LEARNER: u8 = 2;
// The whole band must fit within the page.
const _: () = assert!(
    CNC_OFF_PEER_SLOTS + CNC_MAX_PEER_SLOTS * CNC_PEER_SLOT_STRIDE <= CNC_PAGE_LEN,
    "peer-slot band overruns the cnc page"
);

/// M7 — adopted cluster-config version. Writer: consensus agent.
pub const CNC_OFF_CONFIG_VERSION: usize = 3456;
/// M7 — 1 while a config change is uncommitted (pending), else 0.
/// Writer: consensus agent.
pub const CNC_OFF_CONFIG_PENDING: usize = 3520;
/// M7 — admin REQUEST line (writer: uc2ctl, same-host). seq u64 @+0 is the
/// commit word — the admin writes the fields, then seq, with release; the
/// consensus agent acts on seq > last-seen. Fields: nonce u64 @+8, op u32
/// @+16, id u32 @+20, ip u32 @+24, port u32 @+28.
pub const CNC_OFF_ADMIN_REQ: usize = 3584;
/// M7 — admin RESPONSE line (writer: consensus agent). seq u64 @+0 echoes the
/// request seq (written LAST, release); status u32 @+8, reason u32 @+12,
/// version u64 @+16.
pub const CNC_OFF_ADMIN_RESP: usize = 3648;

const _: () = assert!(CNC_OFF_ADMIN_RESP + 64 <= CNC_PAGE_LEN);

/// Post-M7 (0.3.0): the node's configured admission window
/// (`NodeConfig::admission_bytes`), published once at boot. 0 = written by a
/// pre-0.3.0 node (readers fall back to their own default). Next free
/// reserved-band offset after this line: 3776.
pub const CNC_OFF_ADMISSION_BYTES: usize = 3712;
const _: () = assert!(CNC_OFF_ADMISSION_BYTES + 64 <= CNC_PAGE_LEN);

/// M8 (Task 10 review round 1, 2026-07-29): cumulative count of DATA/HEARTBEAT
/// datagrams the sender agent dropped because `Transport::seal` failed
/// (`NoGroupKey`, an evicted epoch, etc.) — mirrors `SenderStats::seal_failures`.
/// Writer: sender agent, once per duty cycle (same cadence as the peer-slot
/// band's `advertised_limit`, never per-datagram). 0 on a cleartext node or one
/// that has never hit a seal failure. Exists because a PERSISTENT seal failure
/// is exactly the condition an operator must see externally: it silently drops
/// live DATA *and* HEARTBEAT, so a follower may never even learn there is a gap
/// to NAK for — `seal_failures` alone (process-internal `AtomicU64` stats) is
/// invisible to anything outside the node process. Next free reserved-band
/// offset after this line: 3840.
pub const CNC_OFF_SEAL_FAILURES: usize = 3776;
const _: () = assert!(CNC_OFF_SEAL_FAILURES + 64 <= CNC_PAGE_LEN);

/// M11 (Task 5): free bytes on the filesystem backing the instance dir, as of
/// the daemon's last ~1s derived-events pass (`statvfs`, `f_bavail *
/// f_frsize`). Writer: the `uc2-node` daemon's main loop only — none of the
/// four polling agents gain a syscall for this. 0 = never published (a
/// library/in-process user with no daemon loop, or a pre-M11 node) — readers
/// must treat 0 as "unknown", not "zero bytes free". Exists because the
/// archive fail-stops at ENOSPC and that must be visible externally BEFORE it
/// hits, the same "make the wall visible" motivation as `seal_failures`
/// above. Next free reserved-band offset after this line: 3904.
pub const CNC_OFF_FREE_DISK_BYTES: usize = 3840;
const _: () = assert!(CNC_OFF_FREE_DISK_BYTES + 64 <= CNC_PAGE_LEN);

/// M12b: admin-request authentication line. Writer: `uc2ctl` (same-host,
/// same discipline as `CNC_OFF_ADMIN_REQ` — this line is written BEFORE
/// `req.seq`'s release store, so a reader that observed a fresh `req.seq`
/// via acquire has also observed a fresh auth line). Reader: the consensus
/// agent, only after `read_admin_req` has returned `Some` for that seq.
/// Layout: `tag[32] @+0` = `HMAC-SHA256(key, app_id ‖ instance_id ‖ seq ‖
/// nonce ‖ op ‖ id ‖ ip ‖ port ‖ expiry_ns)`; `expiry_ns u64 @+32` (LE);
/// `key_name_hash u64 @+40` (LE, FNV-1a 64 of the admin key's name); 16
/// bytes reserved @+48. All-zero = no auth attached (an `auth = "none"`
/// deployment, or a pre-M12b `uc2ctl`). Next free reserved-band offset
/// after this line: 3968.
pub const CNC_OFF_ADMIN_AUTH: usize = 3904;
const _: () = assert!(CNC_OFF_ADMIN_AUTH + 64 <= CNC_PAGE_LEN);

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
        let other_major = 3u32 << 24; // major 3, minor 0
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
        // M6 Task 3: SnapshotSlots.
        assert_eq!(CNC_OFF_SERVICE_SNAPSHOT_POS, 1152);
        assert_eq!(CNC_OFF_NODE_SNAPSHOT_FLOOR - CNC_OFF_SERVICE_SNAPSHOT_POS, 64);
        assert_eq!(CNC_OFF_INCOMING_SNAPSHOT_POS - CNC_OFF_NODE_SNAPSHOT_FLOOR, 64);
        assert_eq!(CNC_OFF_INCOMING_SNAPSHOT_POS, 1280);
        // M6 Task 9: observability band — archive_first_base + per-peer slots.
        assert_eq!(CNC_OFF_ARCHIVE_FIRST_BASE, 1344);
        assert_eq!(CNC_OFF_ARCHIVE_FIRST_BASE - CNC_OFF_INCOMING_SNAPSHOT_POS, 64);
        assert_eq!(CNC_OFF_PEER_SLOTS, 1408);
        assert_eq!(CNC_PEER_SLOT_STRIDE, 256);
        assert_eq!(CNC_MAX_PEER_SLOTS, 8);
        assert_eq!(CNC_PEER_OFF_ID_AND_ROLE, 0);
        assert_eq!(CNC_PEER_OFF_REPORTED_DURABLE, 64);
        assert_eq!(CNC_PEER_OFF_ADVERTISED_LIMIT, 128);
        assert_eq!(CNC_PEER_OFF_NAKS_PLUS_REPLAY, 192);
        assert_eq!(CNC_PEER_ROLE_VOTER, 1);
        assert_eq!(CNC_PEER_ROLE_LEARNER, 2);
        // The band ends at 3456, inside the 4096-byte page (the `<= CNC_PAGE_LEN`
        // bound is const-asserted at module scope).
        assert_eq!(CNC_OFF_PEER_SLOTS + CNC_MAX_PEER_SLOTS * CNC_PEER_SLOT_STRIDE, 3456);
        // M7: config band.
        assert_eq!(CNC_OFF_CONFIG_VERSION, 3456);
        assert_eq!(CNC_OFF_CONFIG_PENDING, 3520);
        assert_eq!(CNC_OFF_ADMIN_REQ, 3584);
        assert_eq!(CNC_OFF_ADMIN_RESP, 3648);
        // Post-M7 (0.3.0): admission_bytes.
        assert_eq!(CNC_OFF_ADMISSION_BYTES, 3712);
        // M8 (Task 10 review round 1): seal_failures.
        assert_eq!(CNC_OFF_SEAL_FAILURES, 3776);
        // M11 (Task 5): free_disk_bytes.
        assert_eq!(CNC_OFF_FREE_DISK_BYTES, 3840);
        // M12b: admin_auth.
        assert_eq!(CNC_OFF_ADMIN_AUTH, 3904);
    }
}
