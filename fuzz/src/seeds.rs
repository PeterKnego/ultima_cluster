// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Deterministic seed corpora for the fuzz targets.
//!
//! Every seed is built with the REAL encoders in `uc_protocol::v2` from fixed
//! literals — no clock, no randomness, no environment. Regenerating always
//! produces byte-identical files, so `cargo +nightly run --bin seed-corpus`
//! is idempotent and the committed corpus is reviewable in a diff.

use uc_protocol::v2::datagram::*;

/// When a seed is (re)written.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum Regen {
    /// Rebuilt from fixed literals every run — byte-identical each time.
    Always,
    /// Written only if the file is absent. For the handful of seeds captured
    /// from a code path that draws from the OS RNG (a Noise `IK` message 1
    /// carries a fresh ephemeral key; a minted group key is 32 random bytes).
    /// Those are produced by the REAL path, which is the point of having
    /// them, but their bytes cannot be reproduced — so once committed they
    /// are never churned, and re-running the generator leaves the tree clean.
    IfAbsent,
}

/// One corpus entry: the file name (`NN-<name>`) and its bytes.
pub struct Seed {
    pub name: &'static str,
    pub bytes: Vec<u8>,
    pub regen: Regen,
}

impl Seed {
    /// A seed built from fixed literals.
    pub fn fixed(name: &'static str, bytes: Vec<u8>) -> Seed {
        Seed { name, bytes, regen: Regen::Always }
    }
    /// A seed captured from a real, RNG-drawing code path.
    pub fn captured(name: &'static str, bytes: Vec<u8>) -> Seed {
        Seed { name, bytes, regen: Regen::IfAbsent }
    }
}

fn datagram(kind: u8, position: u64, term: u32, body: &[u8]) -> Vec<u8> {
    let mut d = vec![0u8; DATAGRAM_HEADER_LEN + body.len()];
    write_datagram_header(
        &mut d,
        &DatagramHeader {
            position,
            leadership_term_id: term,
            kind,
            flags: 0,
            key_epoch: 0,
        },
    );
    d[DATAGRAM_HEADER_LEN..].copy_from_slice(body);
    d
}

/// Seeds for the `uc_protocol_datagram` target: one representative, valid
/// encoding per datagram kind the pre-auth parse path can meet.
pub fn uc_protocol_datagram() -> Vec<Seed> {
    let mut seeds = Vec::new();

    // A DATA datagram: header + a run of payload bytes (the frame reader is a
    // separate target; here the payload is opaque tail bytes).
    seeds.push(Seed::fixed("01-data", datagram(DGRAM_KIND_DATA, 4096, 3, &[0xABu8; 64])));

    // Header-only HEARTBEAT: the shortest legal datagram.
    seeds.push(Seed::fixed("02-heartbeat", datagram(DGRAM_KIND_HEARTBEAT, 8192, 3, &[])));

    // AppendPosition with the wire-0.5.0 8-byte content attestation.
    let mut b = [0u8; APPEND_POSITION_BODY_LEN];
    write_append_position_body(&mut b, &AppendPositionBody { durable_term: 3 });
    seeds.push(Seed::fixed("03-append-position", datagram(DGRAM_KIND_APPEND_POSITION, 4096, 3, &b)));

    let mut b = [0u8; NAK_BODY_LEN];
    write_nak_body(&mut b, &NakBody { position: 2048, length: 1024 });
    seeds.push(Seed::fixed("04-nak", datagram(DGRAM_KIND_NAK, 0, 3, &b)));

    let mut b = [0u8; STATUS_BODY_LEN];
    write_status_body(&mut b, &StatusBody { contiguous_position: 2048, receive_window: 65536 });
    seeds.push(Seed::fixed("05-status", datagram(DGRAM_KIND_STATUS, 0, 3, &b)));

    let mut b = [0u8; REQUEST_VOTE_BODY_LEN];
    write_request_vote_body(
        &mut b,
        &RequestVoteBody { new_term: 4, last_term: 3, last_durable: 4096 },
    );
    seeds.push(Seed::fixed("06-request-vote", datagram(DGRAM_KIND_REQUEST_VOTE, 0, 4, &b)));

    let mut b = [0u8; VOTE_BODY_LEN];
    write_vote_body(&mut b, &VoteBody { term: 4, granted: true });
    seeds.push(Seed::fixed("07-vote", datagram(DGRAM_KIND_VOTE, 0, 4, &b)));

    // A term map with 3 entries — exercises the count/length cross-check.
    let entries = [
        TermMapEntryWire { term: 1, base: 0 },
        TermMapEntryWire { term: 2, base: 4096 },
        TermMapEntryWire { term: 3, base: 8192 },
    ];
    let mut b = vec![0u8; TERM_MAP_HEADER_LEN + entries.len() * TERM_MAP_ENTRY_LEN];
    let n = write_term_map_body(&mut b, &entries);
    b.truncate(n);
    seeds.push(Seed::fixed("08-term-map-3", datagram(DGRAM_KIND_TERM_MAP, 0, 3, &b)));

    let mut b = [0u8; READ_PROBE_BODY_LEN];
    write_read_probe_body(&mut b, &ReadProbeBody { nonce: 0x0102_0304_0506_0708, from: 2 });
    seeds.push(Seed::fixed("09-read-probe", datagram(DGRAM_KIND_READ_PROBE, 0, 3, &b)));

    // SNAP_BEGIN with a NON-EMPTY config — the only variable-length body on
    // this path, and the one whose `config_len` the reader must re-check
    // against the buffer it actually got.
    let config = vec![0x11u8, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
    let mut b = vec![0u8; SNAP_BEGIN_FIXED_LEN + config.len()];
    write_snap_begin_body(
        &mut b,
        &SnapBeginBody { session: 7, snapshot_pos: 8192, total_len: 1 << 20, config },
    );
    seeds.push(Seed::fixed("10-snap-begin-config", datagram(DGRAM_KIND_SNAP_BEGIN, 0, 3, &b)));

    let mut b = [0u8; SNAP_NAK_BODY_LEN];
    write_snap_nak_body(&mut b, &SnapNakBody { session: 7, offset: 65536, length: 4096 });
    seeds.push(Seed::fixed("11-snap-nak", datagram(DGRAM_KIND_SNAP_NAK, 0, 3, &b)));

    // ConfigProposal (kind 16) and its reply (kind 17) — the M7 admin band.
    let mut b = [0u8; CONFIG_PROPOSAL_BODY_LEN];
    write_config_proposal_body(
        &mut b,
        &ConfigProposalBody {
            nonce: 0x0BAD_F00D_DEAD_BEEF,
            op: 1,
            id: 5,
            ip: 0x0A00_0005,
            port: 9500,
        },
    );
    seeds.push(Seed::fixed("12-config-proposal", datagram(DGRAM_KIND_CONFIG_PROPOSAL, 0, 3, &b)));

    let mut b = [0u8; CONFIG_REPLY_BODY_LEN];
    write_config_reply_body(
        &mut b,
        &ConfigReplyBody { nonce: 0x0BAD_F00D_DEAD_BEEF, status: 0, reason: 0, version: 12 },
    );
    seeds.push(Seed::fixed("13-config-reply", datagram(DGRAM_KIND_CONFIG_REPLY, 0, 3, &b)));

    seeds
}

// ===========================================================================
// Task 2 targets
// ===========================================================================

/// `uc2_remote_frame` — one frame per type with a real encoded body, plus the
/// two header-length edges around `MAX_FRAME_LEN`.
pub fn uc2_remote_frame() -> Vec<Seed> {
    use uc2_remote::frame::*;

    fn frame(ty: FrameType, flags: u8, client_id: u64, seq: u64, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode_frame(
            &mut out,
            Header { ty, flags, version: PROTOCOL_VERSION, client_id, seq },
            body,
        );
        out
    }
    fn body_of(f: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut v = Vec::new();
        f(&mut v);
        v
    }

    let mut seeds = Vec::new();

    seeds.push(Seed::fixed(
        "01-hello",
        frame(FrameType::Hello, 0, 0, 0, &body_of(|o| Hello { app_id: "fuzz" }.encode(o))),
    ));
    seeds.push(Seed::fixed(
        "02-hello-ok",
        frame(
            FrameType::HelloOk,
            0,
            7,
            0,
            &body_of(|o| HelloOk { credits: 64, leader: Some(2), leader_addr: "10.0.0.2:9500" }.encode(o)),
        ),
    ));
    for (n, reason) in [
        ("03-hello-refused-app-id", HELLO_REFUSED_APP_ID),
        ("04-hello-refused-version", HELLO_REFUSED_VERSION),
        ("05-hello-refused-faulted", HELLO_REFUSED_FAULTED),
        ("06-hello-refused-busy", HELLO_REFUSED_BUSY),
    ] {
        seeds.push(Seed::fixed(
            n,
            frame(
                FrameType::HelloRefused,
                0,
                0,
                0,
                &body_of(|o| HelloRefused { reason, detail: "refused" }.encode(o)),
            ),
        ));
    }
    seeds.push(Seed::fixed(
        "07-response-meta",
        frame(
            FrameType::Response,
            FLAG_ENVELOPED,
            7,
            3,
            &body_of(|o| ResponseMeta { credits: 64, acked_seq: 3, position: 4096 }.encode(o)),
        ),
    ));
    seeds.push(Seed::fixed(
        "08-status",
        frame(FrameType::Status, 0, 0, 0, &body_of(|o| Status { acked_seq: 9, credits: 128 }.encode(o))),
    ));
    seeds.push(Seed::fixed(
        "09-redirect",
        frame(
            FrameType::Redirect,
            0,
            0,
            0,
            &body_of(|o| Leader { node_id: 3, addr: "10.0.0.3:9500" }.encode(o)),
        ),
    ));
    seeds.push(Seed::fixed(
        "09b-leader-changed",
        frame(
            FrameType::LeaderChanged,
            0,
            0,
            0,
            &body_of(|o| Leader { node_id: 4, addr: "10.0.0.4:9500" }.encode(o)),
        ),
    ));
    for (n, reason) in [
        ("10-retry-not-serving", RETRY_NOT_SERVING),
        ("11-retry-instance-restart", RETRY_INSTANCE_RESTART),
        ("12-retry-service-unavailable", RETRY_SERVICE_UNAVAILABLE),
        ("13-retry-payload-too-large", RETRY_PAYLOAD_TOO_LARGE),
    ] {
        seeds.push(Seed::fixed(
            n,
            frame(FrameType::Retry, 0, 7, 3, &body_of(|o| Retry { reason, retry_after_us: 5_000 }.encode(o))),
        ));
    }
    seeds.push(Seed::fixed(
        "14-submit-linearizable",
        frame(FrameType::Submit, FLAG_LINEARIZABLE | FLAG_IS_QUERY, 7, 4, b"a command"),
    ));

    // The two header-length edges: a header claiming exactly MAX_FRAME_LEN
    // (accepted) and one byte more (must be `TooLong`). Hand-built, since
    // `encode_frame` will not produce an oversized frame.
    for (n, len) in [("15-len-max-frame", MAX_FRAME_LEN), ("16-len-max-frame-plus-1", MAX_FRAME_LEN + 1)] {
        let mut v = Vec::with_capacity(HEADER_LEN);
        v.extend_from_slice(&len.to_le_bytes());
        v.push(FrameType::Submit as u8);
        v.push(0);
        v.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
        v.extend_from_slice(&7u64.to_le_bytes());
        v.extend_from_slice(&5u64.to_le_bytes());
        seeds.push(Seed::fixed(n, v));
    }
    // And a header claiming a length BELOW the header size — the third
    // rejection branch in `decode_header`.
    let mut v = Vec::with_capacity(HEADER_LEN);
    v.extend_from_slice(&8u32.to_le_bytes());
    v.push(FrameType::Submit as u8);
    v.push(0);
    v.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    v.extend_from_slice(&0u64.to_le_bytes());
    v.extend_from_slice(&0u64.to_le_bytes());
    seeds.push(Seed::fixed("17-len-below-header", v));

    seeds
}

/// `uc2_crypto_open` — genuine sealed datagrams under the target's fixed key,
/// plus truncated and bit-flipped variants (the shapes an on-path attacker
/// actually produces).
pub fn uc2_crypto_open() -> Vec<Seed> {
    use uc_protocol::v2::datagram::*;
    const KEY: [u8; 32] = [7u8; 32];

    fn staged(payload: &[u8]) -> Vec<u8> {
        let mut v = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(
            &mut v,
            &DatagramHeader {
                position: 4096,
                leadership_term_id: 3,
                kind: DGRAM_KIND_DATA,
                flags: 0,
                key_epoch: 1,
            },
        );
        v.extend_from_slice(payload);
        v
    }

    let mut seeds = Vec::new();
    for (i, (name, counter)) in
        [("01-sealed-ctr-0", 0u64), ("02-sealed-ctr-1", 1), ("03-sealed-ctr-max", u64::MAX)]
            .into_iter()
            .enumerate()
    {
        let mut v = staged(b"a replication payload");
        uc2_crypto::seal::seal_in_place(&mut v, &KEY, counter).expect("seal seed");
        if i == 0 {
            // 1-byte-truncated: the tag no longer covers the ciphertext.
            let mut t = v.clone();
            t.pop();
            seeds.push(Seed::fixed("04-sealed-truncated-1", t));
            // Last tag byte flipped.
            let mut f = v.clone();
            let last = f.len() - 1;
            f[last] ^= 0x01;
            seeds.push(Seed::fixed("05-sealed-tag-flipped", f));
            // A header byte flipped — the header is authenticated as AAD.
            let mut a = v.clone();
            a[OFF_DGRAM_KIND] ^= 0x01;
            seeds.push(Seed::fixed("06-sealed-aad-flipped", a));
            // Header only: too short to hold a counter and a tag at all.
            seeds.push(Seed::fixed("07-header-only", staged(b"")));
        }
        seeds.push(Seed::fixed(name, v));
    }
    seeds
}

/// `uc2_crypto_handshake` — a genuine Noise `IK` message 1 from the initiator
/// side, prefixed with the kind byte the target consumes, plus fixed
/// non-handshake and malformed kinds.
pub fn uc2_crypto_handshake() -> Vec<Seed> {
    use uc2_crypto::HandshakeAction;
    use uc_protocol::v2::crypto::{DGRAM_KIND_HS_INIT, DGRAM_KIND_HS_KEY, DGRAM_KIND_HS_RESP};

    let mut seeds = Vec::new();

    // The real thing: drive an initiator-side `Peers` and capture what it
    // puts on the wire. Message 1 carries a fresh X25519 ephemeral from the
    // OS RNG, so this is a `captured` seed (see `Regen`).
    let mut initiator = crate::initiator_peers();
    for act in initiator.initiate(crate::A_ID, 1_000_000) {
        if let HandshakeAction::Send { kind, body, .. } = act {
            let mut v = vec![kind];
            v.extend_from_slice(&body);
            seeds.push(Seed::captured("01-real-hs-init", v));
            break;
        }
    }

    // Fixed shapes: the right kinds with empty and near-length bodies, and a
    // kind that is not a handshake kind at all (must be dropped silently).
    seeds.push(Seed::fixed("02-hs-init-empty", vec![DGRAM_KIND_HS_INIT]));
    seeds.push(Seed::fixed("03-hs-resp-empty", vec![DGRAM_KIND_HS_RESP]));
    seeds.push(Seed::fixed("04-hs-key-kind", {
        let mut v = vec![DGRAM_KIND_HS_KEY];
        v.extend_from_slice(&[0u8; 35]);
        v
    }));
    seeds.push(Seed::fixed("05-hs-init-zeros", {
        let mut v = vec![DGRAM_KIND_HS_INIT];
        v.extend_from_slice(&[0u8; 64]);
        v
    }));
    seeds.push(Seed::fixed("06-not-a-handshake-kind", vec![1u8, 2, 3, 4]));

    seeds
}

/// `uc2_crypto_group_key` — the two message shapes that ride kind 20, at the
/// exact lengths the reader accepts and one either side.
pub fn uc2_crypto_group_key() -> Vec<Seed> {
    // Wire layout, from `group.rs`'s module docs: a leading tag byte (0 = key
    // delivery, 1 = ack), then a u16 LE epoch, then — for a delivery — 32 key
    // bytes. Hand-encoded from those literals rather than by calling
    // `GroupPlane::mint`, which draws the key from `OsRng` and so cannot
    // produce a reproducible seed; a `captured` mint sample follows below and
    // pins that this layout really is the one the rotation path emits.
    const MSG_KEY: u8 = 0;
    const MSG_ACK: u8 = 1;

    let mut seeds = Vec::new();

    let mut key_msg = vec![MSG_KEY];
    key_msg.extend_from_slice(&7u16.to_le_bytes());
    key_msg.extend_from_slice(&[0x5Au8; 32]);
    seeds.push(Seed::fixed("01-key-delivery", key_msg.clone()));

    let mut ack = vec![MSG_ACK];
    ack.extend_from_slice(&7u16.to_le_bytes());
    seeds.push(Seed::fixed("02-ack", ack.clone()));

    // Length edges either side of both accepted lengths.
    let mut short = key_msg.clone();
    short.pop();
    seeds.push(Seed::fixed("03-key-delivery-short", short));
    let mut long = key_msg.clone();
    long.push(0);
    seeds.push(Seed::fixed("04-key-delivery-long", long));
    let mut ack_long = ack.clone();
    ack_long.push(0);
    seeds.push(Seed::fixed("05-ack-long", ack_long));
    seeds.push(Seed::fixed("06-unknown-tag", vec![9u8, 0, 0]));
    seeds.push(Seed::fixed("07-empty", Vec::new()));

    // A genuine kind-20 delivery straight off the real rotation path.
    let mut plane = uc2_crypto::group::GroupPlane::new(crate::A_ID);
    let (_epoch, acts) = plane.mint(&[crate::B_ID], 1_000_000);
    for act in acts {
        if let uc2_crypto::HandshakeAction::Send { body, .. } = act {
            seeds.push(Seed::captured("08-real-minted-key", body));
            break;
        }
    }

    seeds
}

/// `uc2_crypto_admin` — the target splits its input into nine fields, so the
/// seeds are simply field layouts worth starting from.
pub fn uc2_crypto_admin() -> Vec<Seed> {
    let mut seeds = Vec::new();
    // Eight leading length bytes drive `split(data, 9)`; the rest is field
    // content. These give the fuzzer a shaped starting point rather than a
    // pile of empty fields.
    let mut v = vec![4u8, 8, 8, 8, 8, 4, 4, 4];
    v.extend_from_slice(b"fuzz");
    v.extend_from_slice(&[0x11u8; 8]);
    v.extend_from_slice(&[0x22u8; 8]);
    v.extend_from_slice(&3u64.to_le_bytes());
    v.extend_from_slice(&0xDEADu64.to_le_bytes());
    v.extend_from_slice(&1u32.to_le_bytes());
    v.extend_from_slice(&5u32.to_le_bytes());
    v.extend_from_slice(&0x0A00_0005u32.to_le_bytes());
    seeds.push(Seed::fixed("01-typical", v));
    seeds.push(Seed::fixed("02-empty-fields", vec![0u8; 8]));
    seeds.push(Seed::fixed("03-empty", Vec::new()));
    seeds
}

/// `ultima_journal_record` — a segment header, records at three payload sizes,
/// and the torn tails recovery actually meets.
pub fn ultima_journal_record() -> Vec<Seed> {
    use ultima_journal::fuzz_seams::{SegmentHeader, encode_header, encode_record};

    let mut seeds = Vec::new();
    seeds.push(Seed::fixed(
        "01-segment-header",
        encode_header(&SegmentHeader { format_ver: 1, base_seq: 0, created_at: 1_700_000_000 })
            .to_vec(),
    ));
    seeds.push(Seed::fixed(
        "02-segment-header-base-42",
        encode_header(&SegmentHeader { format_ver: 1, base_seq: 42, created_at: 1_700_000_001 })
            .to_vec(),
    ));

    let small = encode_record(1, 0, b"x");
    let medium = encode_record(2, 4096, &[0xABu8; 256]);
    let large = encode_record(3, 8192, &[0xCDu8; 4096]);
    seeds.push(Seed::fixed("03-record-1b", small.clone()));
    seeds.push(Seed::fixed("04-record-256b", medium.clone()));
    seeds.push(Seed::fixed("05-record-4k", large));

    // A TORN TAIL: a valid record truncated mid-body. This is the `Ok(None)`
    // path — the single most important journal behaviour to keep panic-free,
    // because it is what a crash mid-append leaves behind.
    let mut torn = medium.clone();
    torn.truncate(medium.len() / 2);
    seeds.push(Seed::fixed("06-torn-tail-mid-body", torn));
    // Torn in the length prefix itself.
    seeds.push(Seed::fixed("07-torn-length-prefix", medium[..3].to_vec()));
    // A zero length prefix — a preallocated segment's unwritten tail.
    seeds.push(Seed::fixed("08-zero-prefix-unwritten", vec![0u8; 64]));
    // A complete record with its CRC corrupted — confirmed corruption, `Err`.
    let mut bad_crc = small.clone();
    let last = bad_crc.len() - 1;
    bad_crc[last] ^= 0xFF;
    seeds.push(Seed::fixed("09-record-bad-crc", bad_crc));
    // Two records back to back — the shape the recovery loop walks.
    let mut two = small.clone();
    two.extend_from_slice(&medium);
    seeds.push(Seed::fixed("10-two-records", two));

    seeds
}

/// `ultima_journal_stable_value` — the rotating two-slot durable value.
pub fn ultima_journal_stable_value() -> Vec<Seed> {
    use ultima_journal::stable_value::{SvHeader, SvSlot, encode_header, encode_slot};

    let mut seeds = Vec::new();
    seeds.push(Seed::fixed(
        "01-header-slot-256",
        encode_header(&SvHeader { format_ver: 1, slot_size: 256 }).to_vec(),
    ));
    seeds.push(Seed::fixed(
        "02-header-slot-4k",
        encode_header(&SvHeader { format_ver: 1, slot_size: 4096 }).to_vec(),
    ));

    let small = encode_slot(&SvSlot { r#gen: 1, state: 1, bytes: b"vote".to_vec() }, 256)
        .expect("encode small slot");
    let big = encode_slot(&SvSlot { r#gen: 9, state: 1, bytes: vec![0x7Fu8; 512] }, 1024)
        .expect("encode big slot");
    seeds.push(Seed::fixed("03-slot-256", small.clone()));
    seeds.push(Seed::fixed("04-slot-1k", big));
    seeds.push(Seed::fixed(
        "05-slot-absent",
        encode_slot(&SvSlot { r#gen: 2, state: 0, bytes: Vec::new() }, 256).expect("encode absent"),
    ));

    // Torn/corrupt shapes: half a slot, and a slot with a flipped payload byte
    // (CRC must catch it and read as absent, never panic).
    seeds.push(Seed::fixed("06-slot-truncated", small[..small.len() / 2].to_vec()));
    let mut flipped = small.clone();
    flipped[13] ^= 0xFF;
    seeds.push(Seed::fixed("07-slot-payload-flipped", flipped));
    seeds.push(Seed::fixed("08-below-min-len", vec![0u8; 16]));

    seeds
}

/// `uc_protocol_cnc` — real 4 KiB pages, plus the short-page edge the M12d
/// `read_cnc_app_id` fix made total.
pub fn uc_protocol_cnc() -> Vec<Seed> {
    use uc_protocol::v2::cnc::*;

    fn page(version: u32, app_id: &str) -> Vec<u8> {
        let mut p = vec![0u8; CNC_PAGE_LEN];
        write_cnc_header(
            &mut p,
            &CncHeader {
                version,
                node_id: 1,
                instance_id: 0xABCD_EF01_2345_6789,
                created_ns: 1_700_000_000_000_000_000,
                buffer_bytes: 64 << 20,
                max_payload: 1 << 20,
            },
            app_id,
        );
        p
    }

    let mut seeds = Vec::new();
    seeds.push(Seed::fixed("01-valid-page", page(CNC_V2_VERSION, "fuzz")));
    seeds.push(Seed::fixed("02-future-version", page(CNC_V2_VERSION + (1 << 16), "fuzz")));

    let mut bad_magic = page(CNC_V2_VERSION, "fuzz");
    bad_magic[CNC_OFF_MAGIC] ^= 0xFF;
    seeds.push(Seed::fixed("03-bad-magic", bad_magic));

    // 95 bytes: one short of the app_id field's end, the exact edge the
    // totality fix in this task covers.
    seeds.push(Seed::fixed("04-page-95-bytes", page(CNC_V2_VERSION, "fuzz")[..95].to_vec()));
    seeds.push(Seed::fixed("05-page-96-bytes", page(CNC_V2_VERSION, "fuzz")[..96].to_vec()));
    seeds.push(Seed::fixed("06-empty", Vec::new()));

    seeds
}

/// `uc_protocol_log_frame` — one header per frame type the log buffer carries.
pub fn uc_protocol_log_frame() -> Vec<Seed> {
    use uc_protocol::v2::frame::*;

    fn header(frame_type: u8, length: u32) -> Vec<u8> {
        let mut buf = vec![0u8; HEADER_LEN];
        // The real appender writes everything but the length, then stores the
        // length atomically last (that ordering is the torn-record guard);
        // this reproduces both halves.
        write_header_except_length(
            &mut buf,
            &FrameHeader {
                length,
                frame_type,
                flags: 0,
                leadership_term_id: 3,
                session_id: 7,
                correlation_id: 11,
            },
        );
        buf[OFF_LENGTH..OFF_LENGTH + 4].copy_from_slice(&length.to_le_bytes());
        buf
    }

    vec![
        Seed::fixed("01-message", header(FRAME_TYPE_MESSAGE, HEADER_LEN as u32 + 64)),
        Seed::fixed("02-padding", header(FRAME_TYPE_PADDING, HEADER_LEN as u32)),
        Seed::fixed("03-new-term", header(FRAME_TYPE_NEW_TERM, HEADER_LEN as u32)),
        Seed::fixed("04-config", header(FRAME_TYPE_CONFIG, HEADER_LEN as u32 + 32)),
        Seed::fixed("05-length-max", header(FRAME_TYPE_MESSAGE, u32::MAX)),
        Seed::fixed("06-length-zero", header(FRAME_TYPE_MESSAGE, 0)),
    ]
}

/// `uc2_service_session` — the exactly-once envelope, including a genuine
/// frozen snapshot of a two-client table.
pub fn uc2_service_session() -> Vec<Seed> {
    use uc2_service::RawStateMachine;
    use uc2_service::SnapshotStateMachine;
    use uc2_service::{SESSION_HEADER_LEN, SessionConfig, Sessioned};

    fn envelope(client_id: u64, seq: u64, body: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(SESSION_HEADER_LEN + body.len());
        v.extend_from_slice(&client_id.to_le_bytes());
        v.extend_from_slice(&seq.to_le_bytes());
        v.extend_from_slice(body);
        v
    }

    // The target splits its input into three parts using leading length bytes,
    // so a seed is `[len0, len1] ++ part0 ++ part1 ++ part2`.
    fn three(a: &[u8], b: &[u8], c: &[u8]) -> Vec<u8> {
        let mut v = vec![a.len() as u8, b.len() as u8];
        v.extend_from_slice(a);
        v.extend_from_slice(b);
        v.extend_from_slice(c);
        v
    }

    let fresh = envelope(1, 1, b"cmd");
    let short = vec![0u8; SESSION_HEADER_LEN - 1]; // shorter than the header: EXPIRED

    // A GENUINE snapshot: apply two clients through the real `Sessioned`, then
    // freeze and stream it exactly as the framework does. Deterministic —
    // nothing here draws from a clock or an RNG.
    let mut sm = Sessioned::new(crate::NoopSm, SessionConfig::default());
    let mut out = Vec::new();
    sm.apply(1, &envelope(1, 1, b"one"), &mut out);
    out.clear();
    sm.apply(2, &envelope(2, 1, b"two"), &mut out);
    let (handle, _pos) = sm.freeze().expect("freeze session table");
    let mut artifact = Vec::new();
    <Sessioned<crate::NoopSm> as SnapshotStateMachine>::stream_snapshot(handle, &mut artifact)
        .expect("stream session snapshot");

    vec![
        // FRESH then a REPLAY of the same envelope, with a real snapshot.
        Seed::fixed("01-fresh-then-replay", three(&fresh, &fresh, &artifact)),
        // FRESH then a NEW seq.
        Seed::fixed("02-fresh-then-next-seq", three(&fresh, &envelope(1, 2, b"cmd2"), &artifact)),
        // An EXPIRED (too short to be an envelope at all) command.
        Seed::fixed("03-short-envelope", three(&short, &fresh, &artifact)),
        // A lower seq after a higher one — the EXPIRED-by-window path.
        Seed::fixed(
            "04-out-of-window",
            three(&envelope(1, 9_000, b"hi"), &envelope(1, 1, b"old"), &artifact),
        ),
        // The snapshot alone, with empty commands.
        Seed::fixed("05-snapshot-only", three(&[], &[], &artifact)),
        // A snapshot whose length prefix is intact but whose blob is truncated.
        Seed::fixed("06-snapshot-truncated", three(&[], &[], &artifact[..artifact.len() / 2])),
    ]
}
