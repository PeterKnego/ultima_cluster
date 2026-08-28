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

    // The target's framing: byte 0 = datagram kind, byte 1 = the CLAIMED
    // sender id, bytes 2..10 = `now_ns` (LE), body from byte 10. Every seed
    // must be laid out this way or the fuzzer starts from garbage — a real
    // Noise message 1 shifted by nine bytes reaches snow as noise.
    fn framed(kind: u8, from: u8, now_ns: u64, body: &[u8]) -> Vec<u8> {
        let mut v = vec![kind, from];
        v.extend_from_slice(&now_ns.to_le_bytes());
        v.extend_from_slice(body);
        v
    }

    // The real thing: drive an initiator-side `Peers` and capture what it
    // puts on the wire. Message 1 carries a fresh X25519 ephemeral from the
    // OS RNG, so this is a `captured` seed (see `Regen`).
    let mut initiator = crate::initiator_peers();
    for act in initiator.initiate(crate::A_ID, 1_000_000) {
        if let HandshakeAction::Send { kind, body, .. } = act {
            // `from` is B (the initiator's own id) — the id A's allowlist
            // expects this key to be claiming.
            seeds.push(Seed::captured(
                "01-real-hs-init",
                framed(kind, crate::B_ID as u8, 1_000_000, &body),
            ));
            break;
        }
    }

    // Fixed shapes: the right kinds with empty and near-length bodies, and a
    // kind that is not a handshake kind at all (must be dropped silently).
    seeds.push(Seed::fixed(
        "02-hs-init-empty",
        framed(DGRAM_KIND_HS_INIT, crate::B_ID as u8, 1_000_000, &[]),
    ));
    seeds.push(Seed::fixed(
        "03-hs-resp-empty",
        framed(DGRAM_KIND_HS_RESP, crate::B_ID as u8, 1_000_000, &[]),
    ));
    seeds.push(Seed::fixed(
        "04-hs-key-kind",
        framed(DGRAM_KIND_HS_KEY, crate::B_ID as u8, 1_000_000, &[0u8; 35]),
    ));
    seeds.push(Seed::fixed(
        "05-hs-init-zeros",
        framed(DGRAM_KIND_HS_INIT, crate::B_ID as u8, 1_000_000, &[0u8; 64]),
    ));
    seeds.push(Seed::fixed("06-not-a-handshake-kind", framed(1, 2, 1_000_000, &[3, 4])));
    // An id NOT in the allowlist, and our OWN id (an explicit early refusal).
    seeds.push(Seed::fixed(
        "07-unknown-peer-id",
        framed(DGRAM_KIND_HS_INIT, 200, 1_000_000, &[0u8; 48]),
    ));
    seeds.push(Seed::fixed(
        "08-claims-our-own-id",
        framed(DGRAM_KIND_HS_INIT, crate::A_ID as u8, 1_000_000, &[0u8; 48]),
    ));
    // now_ns at both extremes — the expiry and reload-rate-limit comparisons.
    seeds.push(Seed::fixed(
        "09-now-zero",
        framed(DGRAM_KIND_HS_INIT, crate::B_ID as u8, 0, &[0u8; 48]),
    ));
    seeds.push(Seed::fixed(
        "10-now-max",
        framed(DGRAM_KIND_HS_INIT, crate::B_ID as u8, u64::MAX, &[0u8; 48]),
    ));

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

    // The target's framing: byte 0 = the CLAIMED sender id, body from byte 1.
    fn framed(from: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![from];
        v.extend_from_slice(body);
        v
    }

    let mut seeds = Vec::new();

    // The epoch `uc2_fuzz::group_plane_with_pending` actually mints. Taken
    // from the real `GroupPlane` rather than pasted as a literal: an ack for
    // any OTHER epoch folds into a no-op, which is how the first version of
    // `02-ack-*` (hard-coded epoch 7 against a plane that mints 1) silently
    // stopped testing `on_ack` at all. The KEY is random, the epoch NUMBER is
    // a deterministic counter, so reading it here keeps the seeds fixed.
    let pending_epoch = {
        let mut probe = uc2_crypto::group::GroupPlane::new(crate::A_ID);
        let (epoch, _acts) = probe.mint(&[crate::B_ID], 1_000_000);
        epoch
    };

    let mut key_msg = vec![MSG_KEY];
    key_msg.extend_from_slice(&pending_epoch.to_le_bytes());
    key_msg.extend_from_slice(&[0x5Au8; 32]);
    seeds.push(Seed::fixed("01-key-delivery", framed(crate::B_ID as u8, &key_msg)));

    let mut ack = vec![MSG_ACK];
    ack.extend_from_slice(&pending_epoch.to_le_bytes());
    // An ack from a peer the pending epoch DOES target, and one from a peer
    // it does not — `on_ack` ranks acks per peer.
    seeds.push(Seed::fixed("02-ack-from-target", framed(crate::B_ID as u8, &ack)));
    seeds.push(Seed::fixed("02b-ack-from-stranger", framed(200, &ack)));

    // Length edges either side of both accepted lengths.
    let mut short = key_msg.clone();
    short.pop();
    seeds.push(Seed::fixed("03-key-delivery-short", framed(crate::B_ID as u8, &short)));
    let mut long = key_msg.clone();
    long.push(0);
    seeds.push(Seed::fixed("04-key-delivery-long", framed(crate::B_ID as u8, &long)));
    let mut ack_long = ack.clone();
    ack_long.push(0);
    seeds.push(Seed::fixed("05-ack-long", framed(crate::B_ID as u8, &ack_long)));
    seeds.push(Seed::fixed("06-unknown-tag", framed(crate::B_ID as u8, &[9u8, 0, 0])));
    seeds.push(Seed::fixed("07-empty", Vec::new()));

    // A genuine kind-20 delivery straight off the real rotation path.
    let mut plane = uc2_crypto::group::GroupPlane::new(crate::A_ID);
    let (_epoch, acts) = plane.mint(&[crate::B_ID], 1_000_000);
    for act in acts {
        if let uc2_crypto::HandshakeAction::Send { body, .. } = act {
            seeds.push(Seed::captured("08-real-minted-key", framed(crate::B_ID as u8, &body)));
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

/// `ring_mpsc_record` — the MPSC slot decision + decode path. Every seed is
/// built with the REAL record writer (`write_record_body_at`) and the real
/// commit-word encoder, so the "valid" seeds are byte-exact what a producer
/// writes.
pub fn ring_mpsc_record() -> Vec<Seed> {
    use uc_protocol::ring::common::{
        FRAME_HEADER_LEN, FRAME_TRAILER_LEN, PADDING_MSG_TYPE, encode_commit_word,
        write_padding_body_at, write_record_body_at,
    };

    /// One fuzz input: commit word, expected lap, then the slot bytes.
    fn input(word: u32, expected_lap: u32, slot: &[u8]) -> Vec<u8> {
        let mut out = word.to_le_bytes().to_vec();
        out.extend_from_slice(&expected_lap.to_le_bytes());
        out.extend_from_slice(slot);
        out
    }

    fn record(msg_type: u16, flags: u16, extra: [u8; 8], payload: &[u8]) -> Vec<u8> {
        let total = FRAME_HEADER_LEN + payload.len() + FRAME_TRAILER_LEN;
        let mut slot = vec![0u8; total];
        // SAFETY: `slot` is exactly `total` bytes and exclusively owned here;
        // `write_record_body_at` writes bytes 4..total.
        unsafe { write_record_body_at(slot.as_mut_ptr(), 0, msg_type, flags, extra, payload) };
        slot
    }

    let mut seeds = Vec::new();

    // A committed record, lap 3, decoding cleanly.
    let r = record(1, 0, [7; 8], b"submit-payload");
    seeds.push(Seed::fixed(
        "01-committed-record",
        input(encode_commit_word(3, r.len() as u32, false), 3, &r),
    ));

    // The same record with a CLAIMED word: the consumer stops, decodes nothing.
    seeds.push(Seed::fixed(
        "02-claimed",
        input(encode_commit_word(3, r.len() as u32, true), 3, &r),
    ));

    // The same record under the WRONG lap: reads as Empty.
    seeds.push(Seed::fixed(
        "03-foreign-lap",
        input(encode_commit_word(2, r.len() as u32, false), 3, &r),
    ));

    // A tail-wrap padding marker.
    let mut pad = vec![0u8; 24];
    // SAFETY: 24 bytes >= the 6 the padding body writes.
    unsafe { write_padding_body_at(pad.as_mut_ptr(), 0) };
    seeds.push(Seed::fixed("04-padding", input(encode_commit_word(0, 24, false), 0, &pad)));

    // A length far beyond the bytes present.
    seeds.push(Seed::fixed("05-overlong-length", input(encode_commit_word(3, 0x3FFFF, false), 3, &r)));

    // A record whose crc has been flipped.
    let mut bad = r.clone();
    let last = bad.len() - 1;
    bad[last] ^= 0xFF;
    seeds.push(Seed::fixed(
        "06-bad-crc",
        input(encode_commit_word(3, bad.len() as u32, false), 3, &bad),
    ));

    // A record header claiming a payload shorter than the frame header.
    seeds.push(Seed::fixed("07-truncated", input(encode_commit_word(3, 8, false), 3, &r[..8])));

    // An all-zero slot: the fresh-ring state.
    seeds.push(Seed::fixed("08-zero", input(0, 0, &[0u8; 24])));

    // A padding msg_type in a slot too short to hold one.
    let mut tiny = vec![0u8; 6];
    tiny[4..6].copy_from_slice(&PADDING_MSG_TYPE.to_le_bytes());
    seeds.push(Seed::fixed("09-min-padding", input(encode_commit_word(0, 6, false), 0, &tiny)));

    // M14b: a committed QUERY record with the service-id prefix, and one with
    // an EMPTY payload (no id byte) — the node's split must be total on both.
    let q = record(2, 0, [9; 8], &[1u8, b'q', b'r', b'y']);
    seeds.push(Seed::fixed("10-query-with-id", input(encode_commit_word(3, q.len() as u32, false), 3, &q)));
    let q0 = record(2, 0, [9; 8], &[]);
    seeds.push(Seed::fixed("11-query-empty", input(encode_commit_word(3, q0.len() as u32, false), 3, &q0)));

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

    // The target consumes a leading SELECTOR byte (bit 0 picks the shipped
    // default `SessionConfig` vs a tiny derived one), then splits the rest
    // into nine parts by leading length bytes: eight commands and a snapshot.
    // A seed is `[sel] ++ [len0..len7] ++ part0 ++ .. ++ part8`.
    fn seed(sel: u8, cmds: &[&[u8]], snap: &[u8]) -> Vec<u8> {
        let mut v = vec![sel];
        let mut lens = [0u8; 8];
        for (i, c) in cmds.iter().take(8).enumerate() {
            lens[i] = c.len() as u8;
        }
        v.extend_from_slice(&lens);
        for c in cmds.iter().take(8) {
            v.extend_from_slice(c);
        }
        v.extend_from_slice(snap);
        v
    }
    /// Shipped-default config (selector bit 0 clear) — the shape the genuine
    /// frozen snapshot below was produced under, so it installs rather than
    /// hitting the config-mismatch branch.
    const DEFAULT_CFG: u8 = 0;
    /// A tiny derived config (bit 0 set) — drives eviction and the window trim.
    const TINY_CFG: u8 = 0b0000_0111;

    let fresh = envelope(1, 1, b"cmd");
    let short = vec![0u8; SESSION_HEADER_LEN - 1]; // shorter than the header: EXPIRED

    // A GENUINE snapshot: apply two clients through the real `Sessioned`, then
    // freeze and stream it exactly as the framework does. Deterministic —
    // nothing here draws from a clock or an RNG. Built over `EchoSm` because
    // that is what the target wraps, so the artifact's trailing inner-SM bytes
    // are the ones `install_snapshot` will actually try to read.
    let mut sm = Sessioned::new(crate::EchoSm::default(), SessionConfig::default());
    let mut out = Vec::new();
    sm.apply(1, &envelope(1, 1, b"one"), &mut out);
    out.clear();
    sm.apply(2, &envelope(2, 1, b"two"), &mut out);
    let (handle, _pos) = sm.freeze().expect("freeze session table");
    let mut artifact = Vec::new();
    <Sessioned<crate::EchoSm> as SnapshotStateMachine>::stream_snapshot(handle, &mut artifact)
        .expect("stream session snapshot");

    // Eight distinct clients: with `max_clients` of 1..4 under the tiny
    // config this forces `evict_clients_over_capacity` repeatedly.
    let many_clients: Vec<Vec<u8>> =
        (1..=8u64).map(|c| envelope(c, 1, b"cmd")).collect();
    let many_refs: Vec<&[u8]> = many_clients.iter().map(|v| v.as_slice()).collect();
    // One client, eight rising seqs: with `window` of 1..4 this forces the
    // per-client window trim on every apply after the first few.
    let rising: Vec<Vec<u8>> = (1..=8u64).map(|q| envelope(1, q, b"cmd")).collect();
    let rising_refs: Vec<&[u8]> = rising.iter().map(|v| v.as_slice()).collect();
    // Eight clients whose command bodies are 24 bytes each. `EchoSm` echoes
    // the body, so each CACHED response is 24 bytes and `Sessioned`'s
    // `total_bytes` reaches 8 x 24 = 192 — well past the tiny config's
    // `max_bytes` of 64 (`16 + (3 % 8) * 16`, from TINY_CFG's `b = 3`), and
    // still past it after `max_clients = 4` has trimmed the table to 4 x 24 =
    // 96. That is what makes `evict_bytes_over_budget` fire rather than merely
    // be called: under `NoopSm` every response was zero-length, `total_bytes`
    // stayed 0, and the budget branch was unreachable at ANY `max_bytes`.
    let fat: Vec<Vec<u8>> = (1..=8u64).map(|c| envelope(c, 1, &[0xEEu8; 24])).collect();
    let fat_refs: Vec<&[u8]> = fat.iter().map(|v| v.as_slice()).collect();

    vec![
        // FRESH then a REPLAY of the same envelope, with a real snapshot,
        // under the DEFAULT config so the snapshot actually installs.
        Seed::fixed(
            "01-fresh-then-replay",
            seed(DEFAULT_CFG, &[&fresh, &fresh, &envelope(1, 2, b"cmd2")], &artifact),
        ),
        // An EXPIRED (too short to be an envelope at all) command.
        Seed::fixed("02-short-envelope", seed(DEFAULT_CFG, &[&short, &fresh], &artifact)),
        // A lower seq after a higher one — the EXPIRED-by-window path.
        Seed::fixed(
            "03-out-of-window",
            seed(DEFAULT_CFG, &[&envelope(1, 9_000, b"hi"), &envelope(1, 1, b"old")], &artifact),
        ),
        // The snapshot alone, no commands.
        Seed::fixed("04-snapshot-only", seed(DEFAULT_CFG, &[], &artifact)),
        // A snapshot whose length prefix is intact but whose blob is truncated.
        Seed::fixed(
            "05-snapshot-truncated",
            seed(DEFAULT_CFG, &[], &artifact[..artifact.len() / 2]),
        ),
        // Eight clients under max_clients 1..4: client eviction.
        Seed::fixed("06-many-clients-evict", seed(TINY_CFG, &many_refs, &artifact)),
        // Eight rising seqs under window 1..4: the per-client window trim.
        Seed::fixed("07-window-trim", seed(TINY_CFG, &rising_refs, &artifact)),
        // Echoed 24-byte responses x 8 clients = 192 cached bytes against a
        // 64-byte budget: `evict_bytes_over_budget`.
        Seed::fixed("08-byte-budget-evict", seed(TINY_CFG, &fat_refs, &artifact)),
        // The tiny config against a DEFAULT-config snapshot: the
        // replicated-config mismatch branch, which must refuse, not panic.
        Seed::fixed("09-config-mismatch", seed(TINY_CFG, &[&fresh], &artifact)),
    ]
}

// ===========================================================================
// Task 3 targets — TOML configs and the obs HTTP router
// ===========================================================================

/// The `node.toml` the quickstart renders (`packaging/quickstart-local.sh`'s
/// heredoc, with the shell interpolations resolved to node 0's values). Kept
/// verbatim in shape because it is the config a first-time user actually
/// runs, and it exercises `[crypto] enabled = false` + `[admin] auth = "hmac"`
/// together — the two M12b explicit-choice sections.
const QUICKSTART_NODE_TOML: &str = r#"id = 0
bind = "127.0.0.1:9100"
instance_dir = "/tmp/uc2-quickstart/n0"
app_id = "counter"

[[members]]
id = 0
addr = "127.0.0.1:9100"

[[members]]
id = 1
addr = "127.0.0.1:9101"

[[members]]
id = 2
addr = "127.0.0.1:9102"

[crypto]
enabled = false

[admin]
auth = "hmac"
keys = [{ name = "admin", key_path = "/tmp/uc2-quickstart/admin.key" }]
"#;

/// The `gateway.toml` the quickstart renders, same provenance.
const QUICKSTART_GATEWAY_TOML: &str = r#"[local]
instance_dir = "/tmp/uc2-quickstart/n0"
app_id = "counter"
listen = "127.0.0.1:9500"

[[members]]
node_id = 0
gateway = "127.0.0.1:9500"

[[members]]
node_id = 1
gateway = "127.0.0.1:9501"

[[members]]
node_id = 2
gateway = "127.0.0.1:9502"

[session]
envelope = false
"#;

fn packaging(name: &str) -> String {
    let p = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../packaging").join(name);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// `uc2_node_toml` — the shipped example, the quickstart's rendered file, and
/// the shapes that must be REFUSED by name rather than accepted or panicked.
pub fn uc2_node_toml() -> Vec<Seed> {
    let example = packaging("node.example.toml");

    // Every section present EXCEPT `[admin]` — the M12b
    // `AdminChoiceRequired` refusal, and the exact shape a v2.5.0 config has
    // when it meets a v2.6.0 binary.
    let missing_admin = "id = 1\n\
        bind = \"10.0.0.1:9100\"\n\
        instance_dir = \"/srv/uc2/n1\"\n\
        app_id = \"myapp\"\n\
        \n[[members]]\nid = 1\naddr = \"10.0.0.1:9100\"\n\
        \n[purge]\nbelow_snapshot_slack_bytes = 1048576\n\
        \n[crypto]\nenabled = false\n\
        \n[log]\nlevel = \"info\"\n";
    // …and the mirror image: no `[crypto]`, which is `CryptoChoiceRequired`.
    let missing_crypto = "id = 1\n\
        bind = \"10.0.0.1:9100\"\n\
        instance_dir = \"/srv/uc2/n1\"\n\
        app_id = \"myapp\"\n\
        \n[[members]]\nid = 1\naddr = \"10.0.0.1:9100\"\n\
        \n[admin]\nauth = \"none\"\n";

    vec![
        Seed::fixed("01-packaging-example", example.into_bytes()),
        Seed::fixed("02-quickstart-rendered", QUICKSTART_NODE_TOML.as_bytes().to_vec()),
        Seed::fixed("03-empty", Vec::new()),
        Seed::fixed("04-missing-admin-section", missing_admin.as_bytes().to_vec()),
        Seed::fixed("05-missing-crypto-section", missing_crypto.as_bytes().to_vec()),
        Seed::fixed("06-unknown-key", b"id = 1\nunknown_key = 1\n".to_vec()),
        // crypto.enabled = true without the key paths: the cross-field rule.
        Seed::fixed(
            "07-crypto-on-no-keys",
            b"id = 1\nbind = \"10.0.0.1:9100\"\ninstance_dir = \"/srv/uc2/n1\"\n\
              app_id = \"a\"\n\n[[members]]\nid = 1\naddr = \"10.0.0.1:9100\"\n\
              \n[crypto]\nenabled = true\n\n[admin]\nauth = \"none\"\n"
                .to_vec(),
        ),
        // A bad enum value behind an otherwise valid file.
        Seed::fixed(
            "08-bad-log-level",
            b"id = 1\nbind = \"10.0.0.1:9100\"\ninstance_dir = \"/srv/uc2/n1\"\n\
              app_id = \"a\"\n\n[[members]]\nid = 1\naddr = \"10.0.0.1:9100\"\n\
              \n[crypto]\nenabled = false\n\n[admin]\nauth = \"none\"\n\
              \n[log]\nlevel = \"chatty\"\n"
                .to_vec(),
        ),
        // Structurally broken TOML — the tokenizer's problem, not the schema's.
        Seed::fixed("09-not-toml", b"[[[[\n\"unterminated".to_vec()),
    ]
}

/// `uc2_gateway_toml` — same shape for the edge's config.
pub fn uc2_gateway_toml() -> Vec<Seed> {
    let example = packaging("gateway.example.toml");
    vec![
        Seed::fixed("01-packaging-example", example.into_bytes()),
        Seed::fixed("02-quickstart-rendered", QUICKSTART_GATEWAY_TOML.as_bytes().to_vec()),
        Seed::fixed("03-empty", Vec::new()),
        // `[local]` is mandatory — absent is a named refusal.
        Seed::fixed("04-no-local-section", b"[[members]]\nnode_id = 1\ngateway = \"127.0.0.1:9500\"\n".to_vec()),
        Seed::fixed(
            "05-unknown-key",
            b"[local]\ninstance_dir = \"/srv/uc2/n0\"\napp_id = \"a\"\n\
              listen = \"127.0.0.1:9500\"\nunknown_key = 1\n"
                .to_vec(),
        ),
        // Passes deserialisation, fails `EdgeConfig::validate` (empty app_id).
        Seed::fixed(
            "06-empty-app-id",
            b"[local]\ninstance_dir = \"/srv/uc2/n0\"\napp_id = \"\"\n\
              listen = \"127.0.0.1:9500\"\n"
                .to_vec(),
        ),
        // Every optional section present, at non-default values.
        Seed::fixed(
            "07-all-sections",
            b"[local]\ninstance_dir = \"/srv/uc2/n0\"\napp_id = \"a\"\n\
              listen = \"127.0.0.1:9500\"\n\n[[members]]\nnode_id = 1\n\
              gateway = \"127.0.0.1:9500\"\n\n[limits]\nmax_inflight = 64\n\
              per_conn_inflight = 8\nrequest_timeout_ms = 500\n\
              status_interval_ms = 50\nmax_connections = 16\n\
              \n[session]\nenvelope = true\n"
                .to_vec(),
        ),
        Seed::fixed("08-not-toml", b"[local\n= = =\n".to_vec()),
    ]
}

/// `uc2_node_http` — the request shapes an unauthenticated scraper can send.
pub fn uc2_node_http() -> Vec<Seed> {
    let mut seeds = vec![
        Seed::fixed("01-get-metrics", b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n".to_vec()),
        Seed::fixed("02-get-healthz", b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n".to_vec()),
        Seed::fixed("03-get-readyz", b"GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n".to_vec()),
        // A non-GET method: 404 by design, not 405.
        Seed::fixed("04-post-metrics", b"POST /metrics HTTP/1.1\r\nHost: x\r\n\r\n".to_vec()),
        // An unknown path.
        Seed::fixed("05-get-unknown", b"GET /nope HTTP/1.1\r\nHost: x\r\n\r\n".to_vec()),
        // A query string — the router splits it off before matching.
        Seed::fixed("06-metrics-query", b"GET /metrics?x=1 HTTP/1.1\r\nHost: x\r\n\r\n".to_vec()),
        // No header terminator at all: `handle_conn` hands over what it read.
        Seed::fixed("07-no-terminator", b"GET /metrics HTTP/1.1".to_vec()),
        // Bare request line, no version, no CRLF.
        Seed::fixed("08-bare-line", b"GET /healthz".to_vec()),
        // Empty, and whitespace-only.
        Seed::fixed("09-empty", Vec::new()),
        Seed::fixed("10-whitespace", b"   \r\n\r\n".to_vec()),
        // Invalid UTF-8 in the request line — `route` decodes lossily-or-not
        // at all, and must still answer.
        Seed::fixed("11-invalid-utf8", vec![0xFF, 0xFE, b' ', b'/', 0x80, b'\r', b'\n']),
        // NUL bytes inside an otherwise valid line.
        Seed::fixed("12-embedded-nul", b"GET /met\0rics HTTP/1.1\r\n\r\n".to_vec()),
    ];
    // A 5 KiB request line — past `REQUEST_CAP` (4096), which is where the
    // server truncates, so this seeds the boundary the target clamps at.
    let mut big = b"GET /".to_vec();
    big.extend(std::iter::repeat_n(b'a', 5 * 1024));
    big.extend_from_slice(b" HTTP/1.1\r\n\r\n");
    seeds.push(Seed::fixed("13-oversized-request-line", big));
    seeds
}
