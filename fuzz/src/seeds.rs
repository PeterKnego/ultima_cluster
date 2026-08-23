// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Deterministic seed corpora for the fuzz targets.
//!
//! Every seed is built with the REAL encoders in `uc_protocol::v2` from fixed
//! literals — no clock, no randomness, no environment. Regenerating always
//! produces byte-identical files, so `cargo +nightly run --bin seed-corpus`
//! is idempotent and the committed corpus is reviewable in a diff.

use uc_protocol::v2::datagram::*;

/// One corpus entry: the file name (`NN-<name>`) and its bytes.
pub struct Seed {
    pub name: &'static str,
    pub bytes: Vec<u8>,
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
    seeds.push(Seed {
        name: "01-data",
        bytes: datagram(DGRAM_KIND_DATA, 4096, 3, &[0xABu8; 64]),
    });

    // Header-only HEARTBEAT: the shortest legal datagram.
    seeds.push(Seed { name: "02-heartbeat", bytes: datagram(DGRAM_KIND_HEARTBEAT, 8192, 3, &[]) });

    // AppendPosition with the wire-0.5.0 8-byte content attestation.
    let mut b = [0u8; APPEND_POSITION_BODY_LEN];
    write_append_position_body(&mut b, &AppendPositionBody { durable_term: 3 });
    seeds.push(Seed {
        name: "03-append-position",
        bytes: datagram(DGRAM_KIND_APPEND_POSITION, 4096, 3, &b),
    });

    let mut b = [0u8; NAK_BODY_LEN];
    write_nak_body(&mut b, &NakBody { position: 2048, length: 1024 });
    seeds.push(Seed { name: "04-nak", bytes: datagram(DGRAM_KIND_NAK, 0, 3, &b) });

    let mut b = [0u8; STATUS_BODY_LEN];
    write_status_body(&mut b, &StatusBody { contiguous_position: 2048, receive_window: 65536 });
    seeds.push(Seed { name: "05-status", bytes: datagram(DGRAM_KIND_STATUS, 0, 3, &b) });

    let mut b = [0u8; REQUEST_VOTE_BODY_LEN];
    write_request_vote_body(
        &mut b,
        &RequestVoteBody { new_term: 4, last_term: 3, last_durable: 4096 },
    );
    seeds.push(Seed { name: "06-request-vote", bytes: datagram(DGRAM_KIND_REQUEST_VOTE, 0, 4, &b) });

    let mut b = [0u8; VOTE_BODY_LEN];
    write_vote_body(&mut b, &VoteBody { term: 4, granted: true });
    seeds.push(Seed { name: "07-vote", bytes: datagram(DGRAM_KIND_VOTE, 0, 4, &b) });

    // A term map with 3 entries — exercises the count/length cross-check.
    let entries = [
        TermMapEntryWire { term: 1, base: 0 },
        TermMapEntryWire { term: 2, base: 4096 },
        TermMapEntryWire { term: 3, base: 8192 },
    ];
    let mut b = vec![0u8; TERM_MAP_HEADER_LEN + entries.len() * TERM_MAP_ENTRY_LEN];
    let n = write_term_map_body(&mut b, &entries);
    b.truncate(n);
    seeds.push(Seed { name: "08-term-map-3", bytes: datagram(DGRAM_KIND_TERM_MAP, 0, 3, &b) });

    let mut b = [0u8; READ_PROBE_BODY_LEN];
    write_read_probe_body(&mut b, &ReadProbeBody { nonce: 0x0102_0304_0506_0708, from: 2 });
    seeds.push(Seed { name: "09-read-probe", bytes: datagram(DGRAM_KIND_READ_PROBE, 0, 3, &b) });

    // SNAP_BEGIN with a NON-EMPTY config — the only variable-length body on
    // this path, and the one whose `config_len` the reader must re-check
    // against the buffer it actually got.
    let config = vec![0x11u8, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
    let mut b = vec![0u8; SNAP_BEGIN_FIXED_LEN + config.len()];
    write_snap_begin_body(
        &mut b,
        &SnapBeginBody { session: 7, snapshot_pos: 8192, total_len: 1 << 20, config },
    );
    seeds.push(Seed { name: "10-snap-begin-config", bytes: datagram(DGRAM_KIND_SNAP_BEGIN, 0, 3, &b) });

    let mut b = [0u8; SNAP_NAK_BODY_LEN];
    write_snap_nak_body(&mut b, &SnapNakBody { session: 7, offset: 65536, length: 4096 });
    seeds.push(Seed { name: "11-snap-nak", bytes: datagram(DGRAM_KIND_SNAP_NAK, 0, 3, &b) });

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
    seeds.push(Seed {
        name: "12-config-proposal",
        bytes: datagram(DGRAM_KIND_CONFIG_PROPOSAL, 0, 3, &b),
    });

    let mut b = [0u8; CONFIG_REPLY_BODY_LEN];
    write_config_reply_body(
        &mut b,
        &ConfigReplyBody { nonce: 0x0BAD_F00D_DEAD_BEEF, status: 0, reason: 0, version: 12 },
    );
    seeds.push(Seed { name: "13-config-reply", bytes: datagram(DGRAM_KIND_CONFIG_REPLY, 0, 3, &b) });

    seeds
}
