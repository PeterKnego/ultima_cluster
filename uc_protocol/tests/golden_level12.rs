//! Wire-level spec §6.1: the bytes a level-12 binary emits, pinned. From
//! 0.13.0 on this file is what "encode at the committed level" means in
//! practice: a release that changes a level-12 encoding — even by one
//! byte — fails here, which is the regression class the 2.11.0 relayout
//! cost 27 %. Refresh the hex ONLY in the task that bumps the wire minor,
//! and only for the NEW level's file (`golden_level13.rs`), never this one.
//!
//! What is pinned here (issue #180): every record and frame kind a level-12
//! binary EMITS — each frame header type and body, every live datagram
//! header kind and body, every `CLUSTER` kind's payload through its own
//! encoder, the list encoders that ride in the cluster artifact, and the
//! cluster artifact (`encode_cluster_image`) itself, which a snapshot session
//! ships to a joiner as `service_id = 255`. Frame headers are pinned as the 32
//! bytes `write_header_except_length` produces: the length word (the commit
//! word) is written by the runtime, last, and stays zero here.
//!
//! The other half of a level-12 reader's inputs is what OLDER binaries wrote.
//! Those fixtures are not duplicated here; they live in
//! `uc_protocol/src/v2/settings.rs` (`mod tests`, private, so unreachable
//! from an integration test): `settings_layout_is_frozen` (v7),
//! `settings_v3_round_trips_retain_sets_and_v2_reads_as_one` (v3/v2),
//! `a_version_1_record_decodes_with_datagram_mtu_zero` (v1),
//! `settings_v4_round_trips_auto_fetch_and_v1_to_v3_read_true` (v4),
//! `settings_v5_round_trips_backup_retention_and_v1_to_v4_read_zero` (v5),
//! `settings_v6_round_trips_purge_requires_tiers_and_v1_to_v5_read_zero` (v6)
//! and `settings_v7_round_trips_the_wire_fields_and_v1_to_v6_read_first_level_auto`
//! (v7). Older cluster-image versions (v1–v5) are covered by
//! `uc_protocol/src/v2/cluster_image.rs`'s own tests.
use uc_protocol::v2::backup::{
    BACKUP_REPORT_BODY_LEN, BackupTierEntry, BackupWatermark, WatermarkOp, encode_backup_list,
    encode_backup_watermark, write_backup_report_body,
};
use uc_protocol::v2::catalog::{
    RowEntry, RowVerdict, SetEntry, SetKind, SetState, encode_set_list,
};
use uc_protocol::v2::cluster_image::{ClusterImageParts, encode_cluster_image};
use uc_protocol::v2::config::{WireConfig, WireMember, encode_config};
use uc_protocol::v2::crypto::{DGRAM_KIND_HS_INIT, DGRAM_KIND_HS_KEY, DGRAM_KIND_HS_RESP};
use uc_protocol::v2::datagram::*;
use uc_protocol::v2::frame::{
    CLUSTER_BODY_PREFIX_LEN, ClusterKind, FLAG_SNAPSHOT_STANDBY, FLAG_TIMER_TABLE,
    FRAME_TYPE_CLUSTER, FRAME_TYPE_MESSAGE, FRAME_TYPE_NEW_TERM, FRAME_TYPE_PADDING,
    FRAME_TYPE_SNAPSHOT, FRAME_TYPE_TIMER, FrameHeader, HEADER_LEN, NEW_TERM_BODY_LEN, OFF_LENGTH,
    TIMER_BODY_LEN, TimerBody, read_header, write_cluster_prefix, write_header_except_length,
    write_new_term_body, write_timer_body,
};
use uc_protocol::v2::schedule::{
    ScheduleEntry, ScheduleRule, ScheduleTable, encode_schedule_table,
};
use uc_protocol::v2::settings::{
    Settings, Target, WIRE_RAISE_AUTO, WIRE_RAISE_MANUAL, encode_settings,
};
use uc_protocol::v2::upgrade::{
    CLUSTER_ROW, RowGenesis, RowRunning, SnapshotReport, UpgradePin, encode_pin_list,
    encode_report_list, encode_row_genesis, encode_running_list, encode_snapshot_report,
    encode_upgrade_pin,
};

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn settings_v7_genesis_at_level_12() {
    let mut s = Settings::genesis_default();
    s.wire_level = 12;
    s.wire_raise = WIRE_RAISE_AUTO;
    let mut b = Vec::new();
    encode_settings(&s, &mut b);
    assert_eq!(
        hex(&b),
        "07000000000000000000000000000000000000000000000000000000000000000001000100000000000000000000000c01"
    );
}

#[test]
fn datagram_header_data_at_level_12() {
    let h = DatagramHeader {
        position: 0x40,
        leadership_term_id: 1,
        kind: DGRAM_KIND_DATA,
        wire: 12,
        key_epoch: 0,
    };
    let mut b = [0u8; DATAGRAM_HEADER_LEN];
    write_datagram_header(&mut b, &h);
    assert_eq!(hex(&b), "400000000000000001000000010c0000");
}

#[test]
fn new_term_body_is_eight_zero_bytes() {
    let mut b = [0xffu8; NEW_TERM_BODY_LEN];
    write_new_term_body(&mut b);
    assert_eq!(hex(&b), "0000000000000000");
}

#[test]
fn cluster_kinds_known_at_level_12() {
    // 1..=7 decode; 8 does not. A kind added by a later level starts at 8.
    for k in 1..=7u8 {
        assert!(ClusterKind::from_u8(k).is_some(), "{k}");
    }
    assert!(ClusterKind::from_u8(8).is_none());
    assert_eq!(CLUSTER_BODY_PREFIX_LEN, 8);
}

// ---------------------------------------------------------------- helpers

fn pin(got: &[u8], want: &str) {
    assert_eq!(hex(got), want);
}

/// The 32 header bytes `write_header_except_length` writes (length word zero).
fn frame_header(frame_type: u8, flags: u8) -> [u8; HEADER_LEN] {
    let h = FrameHeader {
        length: 0,
        frame_type,
        flags,
        leadership_term_id: 0x0102_0304,
        client_id: 0x1112_1314,
        seq: 0x2122_2324,
        time_ns: 0x3132_3334_3536_3738,
    };
    // 0xff, not 0: the hex then shows the length word UNWRITTEN (ffffffff)
    // and every reserved byte written to zero by the encoder itself.
    let mut b = [0xffu8; HEADER_LEN];
    write_header_except_length(&mut b, &h);
    b
}

fn cluster_body(kind: ClusterKind, payload: &[u8]) -> Vec<u8> {
    let mut b = vec![0xffu8; CLUSTER_BODY_PREFIX_LEN];
    write_cluster_prefix(&mut b, kind);
    b.extend_from_slice(payload);
    b
}

// ------------------------------------------------------------ frame headers

#[test]
fn frame_header_message_except_length() {
    pin(
        &frame_header(FRAME_TYPE_MESSAGE, 0),
        "ffffffff01000000040302011413121124232221000000003837363534333231",
    );
}

#[test]
fn frame_header_padding_except_length() {
    pin(
        &frame_header(FRAME_TYPE_PADDING, 0),
        "ffffffff02000000040302011413121124232221000000003837363534333231",
    );
}

#[test]
fn frame_new_term_header_and_body() {
    let mut b = frame_header(FRAME_TYPE_NEW_TERM, 0).to_vec();
    let mut body = [0xffu8; NEW_TERM_BODY_LEN];
    write_new_term_body(&mut body);
    b.extend_from_slice(&body);
    pin(
        &b,
        "ffffffff030000000403020114131211242322210000000038373635343332310000000000000000",
    );
}

#[test]
fn frame_cluster_header_and_settings_prefix() {
    let mut b = frame_header(FRAME_TYPE_CLUSTER, 0).to_vec();
    let mut prefix = [0xffu8; CLUSTER_BODY_PREFIX_LEN];
    write_cluster_prefix(&mut prefix, ClusterKind::Settings);
    b.extend_from_slice(&prefix);
    pin(
        &b,
        "ffffffff040000000403020114131211242322210000000038373635343332310300000000000000",
    );
}

#[test]
fn frame_timer_table_header_and_body() {
    let mut b = frame_header(FRAME_TYPE_TIMER, FLAG_TIMER_TABLE).to_vec();
    let mut body = [0xffu8; TIMER_BODY_LEN];
    write_timer_body(
        &mut body,
        &TimerBody {
            identity_hash: 0x4142_4344_4546_4748,
            timer_id: 0x5152_5354_5556_5758,
            deadline_ns: 0x6162_6364_6566_6768,
        },
    );
    b.extend_from_slice(&body);
    pin(
        &b,
        "ffffffff05010000040302011413121124232221000000003837363534333231484746454443424158575655545352516867666564636261",
    );
}

#[test]
fn frame_header_snapshot_standby_except_length() {
    pin(
        &frame_header(FRAME_TYPE_SNAPSHOT, FLAG_SNAPSHOT_STANDBY),
        "ffffffff07010000040302011413121124232221000000003837363534333231",
    );
}

/// The length word is the commit word a level-12 reader keys on: a `u32`
/// little-endian at offset 0, read back by `read_header`.
#[test]
fn frame_length_word_is_u32_le_at_offset_zero() {
    assert_eq!(OFF_LENGTH, 0);
    let mut b = frame_header(FRAME_TYPE_MESSAGE, 0);
    b[OFF_LENGTH..OFF_LENGTH + 4].copy_from_slice(&0x2a_u32.to_le_bytes());
    assert_eq!(&hex(&b)[..8], "2a000000");
    assert_eq!(read_header(&b).length, 0x2a);
}

// ---------------------------------------------------------- datagram headers

#[test]
fn datagram_header_every_live_kind_at_level_12() {
    // One row per live datagram kind: `v2::datagram`'s 1..=17 and 22..=29,
    // plus 18..=20 (`DGRAM_KIND_HS_*`), which come from `v2::crypto`.
    // (21 is retired). Only the HS_* headers are pinned; their payloads are
    // Noise messages outside UC's encoders.
    // The row count is checked against the SOURCE, not a typed number, so a
    // kind added later (a new `DGRAM_KIND_*` in either module) fails here
    // until it has a row. The retired kind 21 has a constant and no row.
    let live_kinds: usize = [
        include_str!("../src/v2/datagram.rs"),
        include_str!("../src/v2/crypto.rs"),
    ]
    .iter()
    .map(|src| src.matches("pub const DGRAM_KIND_").count())
    .sum::<usize>()
        - 1;
    let table: [(u8, &str); 28] = [
        (DGRAM_KIND_DATA, "88776655443322110d0c0b0a010c0500"),
        (DGRAM_KIND_HEARTBEAT, "88776655443322110d0c0b0a020c0500"),
        (DGRAM_KIND_NAK, "88776655443322110d0c0b0a030c0500"),
        (DGRAM_KIND_STATUS, "88776655443322110d0c0b0a040c0500"),
        (
            DGRAM_KIND_APPEND_POSITION,
            "88776655443322110d0c0b0a050c0500",
        ),
        (
            DGRAM_KIND_COMMIT_POSITION,
            "88776655443322110d0c0b0a060c0500",
        ),
        (DGRAM_KIND_REQUEST_VOTE, "88776655443322110d0c0b0a070c0500"),
        (DGRAM_KIND_VOTE, "88776655443322110d0c0b0a080c0500"),
        (DGRAM_KIND_TERM_MAP, "88776655443322110d0c0b0a090c0500"),
        (DGRAM_KIND_READ_PROBE, "88776655443322110d0c0b0a0a0c0500"),
        (
            DGRAM_KIND_READ_PROBE_ACK,
            "88776655443322110d0c0b0a0b0c0500",
        ),
        (DGRAM_KIND_SNAP_BEGIN, "88776655443322110d0c0b0a0c0c0500"),
        (DGRAM_KIND_SNAP_CHUNK, "88776655443322110d0c0b0a0d0c0500"),
        (DGRAM_KIND_SNAP_NAK, "88776655443322110d0c0b0a0e0c0500"),
        (DGRAM_KIND_SNAP_DONE, "88776655443322110d0c0b0a0f0c0500"),
        (
            DGRAM_KIND_CONFIG_PROPOSAL,
            "88776655443322110d0c0b0a100c0500",
        ),
        (DGRAM_KIND_CONFIG_REPLY, "88776655443322110d0c0b0a110c0500"),
        (DGRAM_KIND_HS_INIT, "88776655443322110d0c0b0a120c0500"),
        (DGRAM_KIND_HS_RESP, "88776655443322110d0c0b0a130c0500"),
        (DGRAM_KIND_HS_KEY, "88776655443322110d0c0b0a140c0500"),
        (DGRAM_KIND_SNAP_REQUEST, "88776655443322110d0c0b0a160c0500"),
        (DGRAM_KIND_SNAP_REDIRECT, "88776655443322110d0c0b0a170c0500"),
        (DGRAM_KIND_PROBE, "88776655443322110d0c0b0a180c0500"),
        (DGRAM_KIND_PROBE_ACK, "88776655443322110d0c0b0a190c0500"),
        (DGRAM_KIND_SNAP_REPORT, "88776655443322110d0c0b0a1a0c0500"),
        (DGRAM_KIND_BACKUP_REPORT, "88776655443322110d0c0b0a1b0c0500"),
        (DGRAM_KIND_TIER_INSTALL, "88776655443322110d0c0b0a1c0c0500"),
        (
            DGRAM_KIND_TIER_INSTALL_STATUS,
            "88776655443322110d0c0b0a1d0c0500",
        ),
    ];
    assert_eq!(table.len(), live_kinds, "a datagram kind has no golden row");
    let mut seen = std::collections::BTreeSet::new();
    for (kind, want) in table {
        assert_ne!(kind, DGRAM_KIND_SNAP_TABLE_RETIRED);
        assert!(seen.insert(kind), "kind {kind} listed twice");
        let h = DatagramHeader {
            position: 0x1122_3344_5566_7788,
            leadership_term_id: 0x0A0B_0C0D,
            kind,
            wire: 12,
            key_epoch: 5,
        };
        let mut b = [0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(&mut b, &h);
        assert_eq!(hex(&b), want, "kind {kind}");
    }
}

// ---------------------------------------------------------- datagram bodies

#[test]
fn append_position_body() {
    let mut b = [0xffu8; APPEND_POSITION_BODY_LEN];
    write_append_position_body(
        &mut b,
        &AppendPositionBody {
            durable_term: 0x0102_0304,
        },
    );
    pin(&b, "0403020100000000");
}

#[test]
fn read_probe_body_bytes() {
    let mut b = [0xffu8; READ_PROBE_BODY_LEN];
    write_read_probe_body(
        &mut b,
        &ReadProbeBody {
            nonce: 0x1122_3344_5566_7788,
            from: 0x0A0B_0C0D,
        },
    );
    pin(&b, "88776655443322110d0c0b0a00000000");
}

#[test]
fn config_proposal_body_bytes() {
    let mut b = [0xffu8; CONFIG_PROPOSAL_BODY_LEN];
    write_config_proposal_body(
        &mut b,
        &ConfigProposalBody {
            nonce: 0x1122_3344_5566_7788,
            op: 3, // `uc_consensus::ConfigOp` discriminant (uc_protocol does not depend on it)
            id: 7,
            ip: 0x0A00_0009,
            port: 19101,
        },
    );
    pin(&b, "887766554433221103000000070000000900000a9d4a");
}

#[test]
fn config_reply_body_bytes() {
    let mut b = [0xffu8; CONFIG_REPLY_BODY_LEN];
    write_config_reply_body(
        &mut b,
        &ConfigReplyBody {
            nonce: 0x1122_3344_5566_7788,
            status: 1,
            reason: 3,
            version: 0x0102_0304_0506_0708,
        },
    );
    pin(&b, "887766554433221101000000030000000807060504030201");
}

#[test]
fn snap_begin_body_layout_v4() {
    let mut b = [0xffu8; SNAP_BEGIN_FIXED_LEN];
    write_snap_begin_body(
        &mut b,
        &SnapBeginBody {
            session: 0x0102_0304,
            layout: SNAP_BEGIN_LAYOUT_V4,
            service_id: 5,
            snapshot_pos: 0x1122_3344_5566_7788,
            total_len: 0x2122_2324_2526_2728,
            identity: core::array::from_fn(|i| 0xA100_0000_0000_0001 + ((i as u64) << 40)),
            version: core::array::from_fn(|i| 0x0001_0000 + i as u32 * 0x0101 + 1),
        },
    );
    pin(
        &b,
        "04030201030500008877665544332211282726252423222101000000000000a101000000000100a101000000000200a101000000000300a101000000000400a101000000000500a101000000000600a101000000000700a10100010002010100030201000403010005040100060501000706010008070100",
    );
}

#[test]
fn probe_rung_body() {
    let mut b = [0xffu8; PROBE_RUNG_LEN];
    write_probe_rung(&mut b, RUNGS[1]);
    pin(&b, "80220000");
}

#[test]
fn probe_ack_body_bytes() {
    let mut b = [0xffu8; PROBE_ACK_BODY_LEN];
    write_probe_ack_body(
        &mut b,
        &ProbeAckBody {
            rung: RUNGS[2],
            own_min_rung: RUNGS[1],
        },
    );
    pin(&b, "0023000080220000");
}

#[test]
fn snap_report_body_bytes() {
    let mut b = [0xffu8; SNAP_REPORT_BODY_LEN];
    write_snap_report_body(
        &mut b,
        &SnapReportBody {
            row: 3,
            node_id: 0x0102_0304,
            position: 0x1122_3344_5566_7788,
            hash: 0x2122_2324_2526_2728,
            size: 0x3132_3334_3536_3738,
        },
    );
    pin(
        &b,
        "0300000004030201887766554433221128272625242322213837363534333231",
    );
}

#[test]
fn backup_report_body_bytes() {
    let mut b = [0xffu8; BACKUP_REPORT_BODY_LEN];
    write_backup_report_body(
        &mut b,
        &BackupWatermark {
            op: WatermarkOp::Report,
            tier_id: 0x0102_0304_0506_0708,
            oldest_set: 0x1112_1314_1516_1718,
            newest_set: 0x2122_2324_2526_2728,
            through: 0x3132_3334_3536_3738,
            reporter: 0x4142_4344_4546_4748,
        },
    );
    pin(
        &b,
        "08070605040302011817161514131211282726252423222138373635343332314847464544434241",
    );
}

#[test]
fn tier_install_body_bytes() {
    let mut b = [0xffu8; TIER_INSTALL_BODY_LEN];
    write_tier_install_body(
        &mut b,
        &TierInstallBody {
            floor: 0x0100,
            commit: 0x0300,
            s: 0x0200,
            row_hash: core::array::from_fn(|i| 0xB100_0000_0000_0001 + ((i as u64) << 40)),
            row_version: core::array::from_fn(|i| 0x0002_0000 + i as u32 * 0x0101 + 1),
            row_size: core::array::from_fn(|i| 0xC100 + i as u64 * 0x0101),
            cluster_hash: 0xD1D2_D3D4_D5D6_D7D8,
            cluster_size: 0xE1E2_E3E4,
        },
    );
    pin(
        &b,
        "00010000000000000003000000000000000200000000000001000000000000b101000000000100b101000000000200b101000000000300b101000000000400b101000000000500b101000000000600b101000000000700b1010002000201020003020200040302000504020006050200070602000807020000c100000000000001c200000000000002c300000000000003c400000000000004c500000000000005c600000000000006c700000000000007c8000000000000d8d7d6d5d4d3d2d1e4e3e2e100000000",
    );
}

#[test]
fn tier_install_status_body_bytes() {
    let mut b = [0xffu8; TIER_INSTALL_STATUS_BODY_LEN];
    write_tier_install_status_body(
        &mut b,
        &TierInstallStatusBody {
            state: TierInstallState::Declined,
            reason: TierInstallReason::Verify,
            position: 0x1122_3344_5566_7788,
            copied: 0x2122_2324_2526_2728,
        },
    );
    pin(&b, "000300000000000088776655443322112827262524232221");
}

#[test]
fn snap_nak_body_bytes() {
    let mut b = [0xffu8; SNAP_NAK_BODY_LEN];
    write_snap_nak_body(
        &mut b,
        &SnapNakBody {
            session: 0x0102_0304,
            offset: 0x1122_3344_5566_7788,
            length: 0x0A0B_0C0D,
        },
    );
    pin(&b, "0403020188776655443322110d0c0b0a");
}

#[test]
fn snap_request_body_bytes() {
    let mut b = [0xffu8; SNAP_REQUEST_BODY_LEN];
    write_snap_request_body(
        &mut b,
        &SnapRequestBody {
            session: 0x0102_0304,
            position: 0x1122_3344_5566_7788,
        },
    );
    pin(&b, "040302018877665544332211");
}

#[test]
fn snap_redirect_body_bytes() {
    let mut b = [0xffu8; SNAP_REDIRECT_BODY_LEN];
    write_snap_redirect_body(
        &mut b,
        &SnapRedirectBody {
            session: 0x0102_0304,
            learner_id: 0x0A0B_0C0D,
            position: 0x1122_3344_5566_7788,
        },
    );
    pin(&b, "040302010d0c0b0a8877665544332211");
}

#[test]
fn request_vote_body_bytes() {
    let mut b = [0xffu8; REQUEST_VOTE_BODY_LEN];
    write_request_vote_body(
        &mut b,
        &RequestVoteBody {
            new_term: 0x0102_0304,
            last_term: 0x0A0B_0C0D,
            last_durable: 0x1122_3344_5566_7788,
        },
    );
    pin(&b, "040302010d0c0b0a8877665544332211");
}

#[test]
fn vote_body_bytes() {
    let mut b = [0xffu8; VOTE_BODY_LEN];
    write_vote_body(
        &mut b,
        &VoteBody {
            term: 0x0102_0304,
            granted: true,
        },
    );
    pin(&b, "04030201010000000000000000000000");
}

#[test]
fn term_map_body_two_entries() {
    let mut b = [0xffu8; TERM_MAP_HEADER_LEN + 2 * TERM_MAP_ENTRY_LEN];
    let n = write_term_map_body(
        &mut b,
        &[
            TermMapEntryWire {
                term: 0x0102_0304,
                base: 0x1122_3344_5566_7788,
            },
            TermMapEntryWire {
                term: 0x0A0B_0C0D,
                base: 0x2122_2324_2526_2728,
            },
        ],
    );
    assert_eq!(n, b.len());
    pin(
        &b,
        "0200000000000000040302010000000088776655443322110d0c0b0a000000002827262524232221",
    );
}

#[test]
fn nak_body_bytes() {
    let mut b = [0xffu8; NAK_BODY_LEN];
    write_nak_body(
        &mut b,
        &NakBody {
            position: 0x1122_3344_5566_7788,
            length: 0x0A0B_0C0D,
        },
    );
    pin(&b, "88776655443322110d0c0b0a00000000");
}

#[test]
fn status_body_layout_v2() {
    let mut b = [0xffu8; STATUS_BODY_LEN];
    write_status_body(
        &mut b,
        &StatusBody {
            contiguous_position: 0x1122_3344_5566_7788,
            receive_window: 0x0102_0304,
            holdings: Holdings {
                journal_first: 0x2101,
                durable: 0x2202,
                commit: 0x2303,
                applied: core::array::from_fn(|i| 0x3100 + i as u64 * 0x0101 + 1),
                free_bytes: 0x4101,
                journal_bytes: 0x4202,
                snapshots_bytes: 0x4303,
                catalog_position: 0x4404,
                sets_held: 0x4505,
            },
        },
    );
    assert_eq!(STATUS_LAYOUT_V2, 2);
    pin(
        &b,
        "887766554433221104030201020000000121000000000000022200000000000003230000000000000131000000000000023200000000000003330000000000000434000000000000053500000000000006360000000000000737000000000000083800000000000001410000000000000242000000000000034300000000000004440000000000000545000000000000",
    );
}

// ------------------------------------------------------------ cluster records

fn sample_config() -> WireConfig {
    WireConfig {
        version: 0x0102_0304_0506_0708,
        prev_position: 0x1122_3344_5566_7788,
        voters: vec![
            WireMember {
                id: 1,
                ip: u32::from_be_bytes([10, 0, 0, 1]),
                port: 19101,
            },
            WireMember {
                id: 2,
                ip: u32::from_be_bytes([10, 0, 0, 2]),
                port: 19102,
            },
        ],
        learners: vec![WireMember {
            id: 3,
            ip: u32::from_be_bytes([10, 0, 0, 3]),
            port: 19103,
        }],
        tombstones: vec![9, 11],
    }
}

fn sample_table() -> ScheduleTable {
    ScheduleTable {
        entries: vec![
            ScheduleEntry {
                identity_hash: 0x1111_2222_3333_4444,
                timer_id: 7,
                rule: ScheduleRule::Every {
                    period_ns: 0x0000_0002_540B_E400,
                    anchor_ns: 0x1122_3344_5566_7788,
                },
            },
            ScheduleEntry {
                identity_hash: 0x5555_6666_7777_8888,
                timer_id: 9,
                rule: ScheduleRule::DailyAt {
                    secs_of_day: 45_296,
                },
            },
            ScheduleEntry {
                identity_hash: 0x9999_AAAA_BBBB_CCCC,
                timer_id: 11,
                rule: ScheduleRule::Once {
                    at_ns: 0x2122_2324_2526_2728,
                },
            },
        ],
    }
}

fn sample_settings() -> Settings {
    Settings {
        fsm_lag_bytes: 0x0100_0000,
        admission_bytes: 0x0020_0000,
        snapshot_interval_bytes: 0x4000_0000,
        snapshot_target: Target::Learners,
        datagram_mtu: RUNGS[1],
        retain_sets: 5,
        auto_fetch: false,
        backup_retain_sets: 7,
        backup_retain_age_secs: 3 * 86_400,
        purge_requires_tiers: 2,
        wire_level: 12,
        wire_raise: WIRE_RAISE_MANUAL,
    }
}

fn sample_pins() -> Vec<UpgradePin> {
    vec![
        UpgradePin {
            row: 1,
            from: 0x0001_0002,
            to: 0x0001_0003,
            origin: 0x1122_3344_5566_7788,
        },
        UpgradePin {
            row: 4,
            from: 0x0002_0001,
            to: 0x0003_0001,
            origin: 0x2122_2324_2526_2728,
        },
    ]
}

fn sample_reports() -> Vec<SnapshotReport> {
    vec![
        SnapshotReport {
            row: 2,
            position: 0x1122_3344_5566_7788,
            hashes: vec![
                (1, 0xA1A2_A3A4_A5A6_A7A8, 0x1001),
                (2, 0xB1B2_B3B4_B5B6_B7B8, 0x1002),
                (3, 0xC1C2_C3C4_C5C6_C7C8, 0x1003),
            ],
        },
        SnapshotReport {
            row: CLUSTER_ROW,
            position: 0x2122_2324_2526_2728,
            hashes: vec![
                (1, 0xD1D2_D3D4_D5D6_D7D8, 0x2001),
                (4, 0xE1E2_E3E4_E5E6_E7E8, 0x2002),
            ],
        },
    ]
}

fn sample_running() -> Vec<RowRunning> {
    vec![
        RowRunning {
            row: 2,
            version: 0x0001_0002,
            record_pos: 0x1122_3344_5566_7788,
        },
        RowRunning {
            row: 3,
            version: 0x0002_0003,
            record_pos: 0x2122_2324_2526_2728,
        },
    ]
}

fn sample_set() -> SetEntry {
    let verdicts = [
        RowVerdict::Agreed,
        RowVerdict::Diverged,
        RowVerdict::NoMajority,
        RowVerdict::Unreported,
    ];
    let mut s = SetEntry::commanded(
        0x1122_3344_5566_7788,
        SetKind::Standby,
        0x2122_2324_2526_2728,
    );
    s.state = SetState::Complete;
    for (i, r) in s.rows.iter_mut().enumerate() {
        *r = RowEntry {
            version: 0x0003_0000 + i as u32 * 0x0101 + 1,
            hash: 0xF100_0000_0000_0001 + ((i as u64) << 40),
            verdict: verdicts[i % 4],
            size: 0x5100 + i as u64 * 0x0101,
        };
    }
    s.cluster = RowEntry {
        version: 0x0004_0001,
        hash: 0xF9F8_F7F6_F5F4_F3F2,
        verdict: RowVerdict::Agreed,
        size: 0x6199,
    };
    s
}

fn sample_backup_entries() -> Vec<BackupTierEntry> {
    vec![
        BackupTierEntry {
            tier_id: 1,
            oldest_set: 0x10,
            newest_set: 0x20,
            through: 0x30,
            reporter: 0x40,
            updated_pos: 0x1122_3344_5566_7788,
        },
        BackupTierEntry {
            tier_id: 2,
            oldest_set: 0x11,
            newest_set: 0x21,
            through: 0x31,
            reporter: 0x41,
            updated_pos: 0x2122_2324_2526_2728,
        },
    ]
}

#[test]
fn cluster_membership_record() {
    let mut p = Vec::new();
    encode_config(&sample_config(), &mut p);
    pin(
        &cluster_body(ClusterKind::Membership, &p),
        "010000000000000008070605040302018877665544332211020001000200010000000100000a9d4a020000000200000a9e4a030000000300000a9f4a090000000b000000",
    );
}

#[test]
fn cluster_schedule_table_record() {
    let mut p = Vec::new();
    encode_schedule_table(&sample_table(), &mut p);
    pin(
        &cluster_body(ClusterKind::ScheduleTable, &p),
        "02000000000000000100000003000000444433332222111107000000000000000100e40b540200000088776655443322118888777766665555090000000000000002f0b00000000000000000000000000000ccccbbbbaaaa99990b000000000000000328272625242322210000000000000000",
    );
}

#[test]
fn cluster_settings_record_non_genesis() {
    let mut p = Vec::new();
    encode_settings(&sample_settings(), &mut p);
    pin(
        &cluster_body(ClusterKind::Settings, &p),
        "0300000000000000070000000000000100000000000020000000000000000040000000000180220000050000070080f4030000000000020c02",
    );
}

#[test]
fn cluster_upgrade_pin_record() {
    let mut p = Vec::new();
    encode_upgrade_pin(&sample_pins()[0], &mut p);
    pin(
        &cluster_body(ClusterKind::UpgradePin, &p),
        "04000000000000000100000002000100030001008877665544332211",
    );
}

#[test]
fn cluster_snapshot_report_record() {
    let mut p = Vec::new();
    assert_eq!(
        encode_snapshot_report(&sample_reports()[0], &mut p),
        Some(())
    );
    pin(
        &cluster_body(ClusterKind::SnapshotReport, &p),
        "05000000000000000203000000000000887766554433221101000000a8a7a6a5a4a3a2a1011000000000000002000000b8b7b6b5b4b3b2b1021000000000000003000000c8c7c6c5c4c3c2c10310000000000000",
    );
}

#[test]
fn cluster_row_genesis_record() {
    let mut p = Vec::new();
    encode_row_genesis(
        &RowGenesis {
            row: 5,
            version: 0x0102_0304,
        },
        &mut p,
    );
    pin(
        &cluster_body(ClusterKind::RowGenesis, &p),
        "06000000000000000500000004030201",
    );
}

#[test]
fn cluster_backup_watermark_record() {
    let mut p = Vec::new();
    encode_backup_watermark(
        &BackupWatermark {
            op: WatermarkOp::Report,
            tier_id: 3,
            oldest_set: 0x10,
            newest_set: 0x20,
            through: 0x30,
            reporter: 0x40,
        },
        &mut p,
    );
    pin(
        &cluster_body(ClusterKind::BackupWatermark, &p),
        "0700000000000000000000000000000003000000000000001000000000000000200000000000000030000000000000004000000000000000",
    );
}

#[test]
fn cluster_backup_watermark_forget_record() {
    let mut p = Vec::new();
    encode_backup_watermark(
        &BackupWatermark {
            op: WatermarkOp::Forget,
            tier_id: 3,
            oldest_set: 0x11,
            newest_set: 0x21,
            through: 0x31,
            reporter: 0x41,
        },
        &mut p,
    );
    pin(
        &cluster_body(ClusterKind::BackupWatermark, &p),
        "0700000000000000010000000000000003000000000000001100000000000000210000000000000031000000000000004100000000000000",
    );
}

// -------------------------------------------- list encoders (cluster artifact)

#[test]
fn running_list_bytes() {
    let mut p = Vec::new();
    encode_running_list(&sample_running(), &mut p);
    pin(
        &p,
        "0200000002000100887766554433221103000000030002002827262524232221",
    );
}

#[test]
fn pin_list_bytes() {
    let mut p = Vec::new();
    encode_pin_list(&sample_pins(), &mut p);
    pin(
        &p,
        "01000000020001000300010088776655443322110400000001000200010003002827262524232221",
    );
}

#[test]
fn report_list_bytes() {
    let mut p = Vec::new();
    assert_eq!(encode_report_list(&sample_reports(), &mut p), Some(()));
    pin(
        &p,
        "4c0000000203000000000000887766554433221101000000a8a7a6a5a4a3a2a1011000000000000002000000b8b7b6b5b4b3b2b1021000000000000003000000c8c7c6c5c4c3c2c1031000000000000038000000ff02000000000000282726252423222101000000d8d7d6d5d4d3d2d1012000000000000004000000e8e7e6e5e4e3e2e10220000000000000",
    );
}

#[test]
fn backup_list_bytes() {
    let mut p = Vec::new();
    encode_backup_list(&sample_backup_entries(), &mut p);
    pin(
        &p,
        "02010000000000000010000000000000002000000000000000300000000000000040000000000000008877665544332211020000000000000011000000000000002100000000000000310000000000000041000000000000002827262524232221",
    );
}

#[test]
fn set_list_bytes() {
    let mut p = Vec::new();
    assert_eq!(encode_set_list(&[sample_set()], &mut p), Some(()));
    pin(
        &p,
        "01008877665544332211010128272625242322210100030001000000000000f10100510000000000000201030001000000000100f10201520000000000000302030001000000000200f10302530000000000000403030001000000000300f10003540000000000000504030001000000000400f10104550000000000000605030001000000000500f10205560000000000000706030001000000000600f10306570000000000000807030001000000000700f100075800000000000001000400f2f3f4f5f6f7f8f9019961000000000000",
    );
}

#[test]
fn cluster_image_with_one_of_everything() {
    let mut membership = Vec::new();
    encode_config(&sample_config(), &mut membership);
    let mut table = Vec::new();
    encode_schedule_table(&sample_table(), &mut table);
    let mut settings = Vec::new();
    encode_settings(&sample_settings(), &mut settings);
    let mut pins = Vec::new();
    encode_pin_list(&sample_pins(), &mut pins);
    let mut reports = Vec::new();
    assert_eq!(
        encode_report_list(&sample_reports(), &mut reports),
        Some(())
    );
    let mut running = Vec::new();
    encode_running_list(&sample_running(), &mut running);
    let mut catalog = Vec::new();
    assert_eq!(encode_set_list(&[sample_set()], &mut catalog), Some(()));
    let mut backups = Vec::new();
    encode_backup_list(&sample_backup_entries(), &mut backups);
    let parts = ClusterImageParts {
        applied: 0x1122_3344_5566_7788,
        table_position: 0x2122_2324_2526_2728,
        settings_position: 0x3132_3334_3536_3738,
        membership: &membership,
        table: &table,
        settings: &settings,
        pins: &pins,
        reports: &reports,
        running: &running,
        catalog: &catalog,
        backups: &backups,
    };
    let mut out = Vec::new();
    assert_eq!(encode_cluster_image(&parts, &mut out), Some(()));
    pin(
        &out,
        "5543434c55535431060000008877665544332211282726252423222138373635343332313c00000008070605040302018877665544332211020001000200010000000100000a9d4a020000000200000a9e4a030000000300000a9f4a090000000b0000006b0000000100000003000000444433332222111107000000000000000100e40b540200000088776655443322118888777766665555090000000000000002f0b00000000000000000000000000000ccccbbbbaaaa99990b000000000000000328272625242322210000000000000000070000000000000100000000000020000000000000000040000000000180220000050000070080f4030000000000020c0228000000010000000200010003000100887766554433221104000000010002000100030028272625242322218c0000004c0000000203000000000000887766554433221101000000a8a7a6a5a4a3a2a1011000000000000002000000b8b7b6b5b4b3b2b1021000000000000003000000c8c7c6c5c4c3c2c1031000000000000038000000ff02000000000000282726252423222101000000d8d7d6d5d4d3d2d1012000000000000004000000e8e7e6e5e4e3e2e10220000000000000200000000200000002000100887766554433221103000000030002002827262524232221d100000001008877665544332211010128272625242322210100030001000000000000f10100510000000000000201030001000000000100f10201520000000000000302030001000000000200f10302530000000000000403030001000000000300f10003540000000000000504030001000000000400f10104550000000000000605030001000000000500f10205560000000000000706030001000000000600f10306570000000000000807030001000000000700f100075800000000000001000400f2f3f4f5f6f7f8f90199610000000000006100000002010000000000000010000000000000002000000000000000300000000000000040000000000000008877665544332211020000000000000011000000000000002100000000000000310000000000000041000000000000002827262524232221283afafa",
    );
}
