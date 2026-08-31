// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Replication datagram layout (spec §5). Core-only, like `v2::frame`.
//!
//! Every UDP datagram starts with this 16-byte header. DATA datagrams are
//! **self-locating**: `position` is the absolute stream position of the first
//! payload byte, and the payload is a run of complete, offset-contiguous
//! frames (a padding frame, if present, is last and sent header-only).
//! HEARTBEAT carries the leader's append position in `position` (liveness +
//! tail-loss detection) and has no payload. NAK and STATUS carry fixed-size
//! little-endian bodies. One UDP socket per node carries everything (control
//! rides the same socket, demuxed by `kind`).

/// Fixed datagram header size; payload (if any) follows immediately.
pub const DATAGRAM_HEADER_LEN: usize = 16;
/// Default datagram budget (spec §5); jumbo-frame deployments raise it.
pub const MTU_DEFAULT: usize = 1408;

pub const OFF_DGRAM_POSITION: usize = 0; // u64 LE — meaning depends on kind
pub const OFF_DGRAM_TERM_ID: usize = 8; // u32 LE — leadership_term_id
pub const OFF_DGRAM_KIND: usize = 12; // u8
pub const OFF_DGRAM_FLAGS: usize = 13; // u8
/// u16 LE — M8 key epoch (0 = cleartext). Was `OFF_DGRAM_RESERVED`; the v2
/// spec set this slot aside for exactly this purpose.
pub const OFF_DGRAM_KEY_EPOCH: usize = 14;

/// Payload = run of complete frames starting at `position`.
pub const DGRAM_KIND_DATA: u8 = 1;
/// No payload; `position` = sender's append position.
pub const DGRAM_KIND_HEARTBEAT: u8 = 2;
/// Payload = `NakBody`.
pub const DGRAM_KIND_NAK: u8 = 3;
/// Payload = `StatusBody`.
pub const DGRAM_KIND_STATUS: u8 = 4;
/// Header-only (spec §6): `position` = the sender's DURABLE position.
/// Follower → leader, on durable advance (block/fsync granularity) plus a
/// 100 ms floor. Feeds the leader's quorum commit ranking.
pub const DGRAM_KIND_APPEND_POSITION: u8 = 5;
/// Header-only (spec §6): `position` = the cluster COMMIT position (quorum-
/// fsync'd). Leader → followers, on commit advance plus the same floor.
pub const DGRAM_KIND_COMMIT_POSITION: u8 = 6;
/// Body = `RequestVoteBody` (spec §6): candidate solicits a vote for
/// `new_term` carrying its log position credentials. The header's
/// `leadership_term_id` also carries `new_term` (body is authoritative).
pub const DGRAM_KIND_REQUEST_VOTE: u8 = 7;
/// Body = `VoteBody`: the response. Granted votes are PERSISTED by the
/// granter before this datagram is sent (spec §6).
pub const DGRAM_KIND_VOTE: u8 = 8;
/// Body = term-map suffix (count + entries): the leader's term history for
/// follower reconciliation (spec §6). Ships at most
/// `MAX_TERM_MAP_WIRE_ENTRIES` most-recent entries; a follower whose common
/// prefix is older than the suffix falls back to full replay from 0.
pub const DGRAM_KIND_TERM_MAP: u8 = 9;

/// Body = `ReadProbeBody` (M5 §7 linearizable-read barrier): the leader's
/// nonce'd read-index confirmation solicitation. Leader → every follower. The
/// header's `leadership_term_id` carries the leader's current term — a follower
/// ACKs ONLY if that term still equals its own (a stale leader's probe dies
/// there; the no-stale-read theorem's teeth). `position` is unused (zero).
pub const DGRAM_KIND_READ_PROBE: u8 = 10;
/// Body = `ReadProbeBody`: the follower's confirmation. Follower → the probing
/// leader (addressed by the leader id carried in the probe body's `from`).
/// The leader counts DISTINCT ackers per nonce toward its read quorum.
pub const DGRAM_KIND_READ_PROBE_ACK: u8 = 11;

/// Length of an [`AppendPositionBody`] (protocol 0.5.0).
pub const APPEND_POSITION_BODY_LEN: usize = 8;

/// **Content attestation for a durable report** (protocol 0.5.0).
///
/// `DGRAM_KIND_APPEND_POSITION` used to be header-only: the header's
/// `position` said "I hold this many bytes" and nothing about WHICH bytes. A
/// leader ranking those reports was therefore taking a POSITION quorum, not a
/// CONTENT one — a replica holding a deposed leader's copy of the same byte
/// range counted toward committing the current leader's history (2026-08-16
/// hunt). This body carries the term the SENDER attributes to the byte
/// immediately below `position`, so the leader can check it against its own
/// term map: equal terms at the same position imply identical prefixes (Log
/// Matching), which is exactly Raft's `(index, term)` pair. A mismatch means
/// the report attests other bytes and must not be counted.
///
/// `durable_term == 0` means "nothing attested" (an empty log at position 0);
/// the leader treats it as a zero-length report.
/// LE: durable_term 0..4, 4..8 zero (reserved, keeps the body 8-byte aligned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendPositionBody {
    pub durable_term: u32,
}

pub fn write_append_position_body(buf: &mut [u8], b: &AppendPositionBody) {
    buf[0..4].copy_from_slice(&b.durable_term.to_le_bytes());
    buf[4..8].fill(0);
}

/// Decode an append-position body. `None` if the buffer is shorter than
/// [`APPEND_POSITION_BODY_LEN`] — which is also how a 0.4.0 peer's header-only
/// report decodes, so callers treat `None` as "unattested" rather than
/// malformed and simply decline to count it.
pub fn read_append_position_body(buf: &[u8]) -> Option<AppendPositionBody> {
    if buf.len() < APPEND_POSITION_BODY_LEN {
        return None;
    }
    Some(AppendPositionBody {
        durable_term: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
    })
}

pub const READ_PROBE_BODY_LEN: usize = 16;

/// Read-barrier probe/ack body: a `nonce` scoping the round to one read, plus
/// the sender's own node id in `from` (so the receiver addresses its reply, and
/// the leader attributes each distinct acker). LE: nonce 0..8, from 8..12,
/// 12..16 zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadProbeBody {
    pub nonce: u64,
    pub from: u32,
}

pub fn write_read_probe_body(buf: &mut [u8], b: &ReadProbeBody) {
    buf[0..8].copy_from_slice(&b.nonce.to_le_bytes());
    buf[8..12].copy_from_slice(&b.from.to_le_bytes());
    buf[12..16].fill(0);
}

/// Decode a read-probe body, or `None` if the buffer is shorter than
/// [`READ_PROBE_BODY_LEN`] (the caller drops a malformed datagram). A longer
/// buffer decodes its 16-byte prefix (trailing bytes ignored).
pub fn read_read_probe_body(buf: &[u8]) -> Option<ReadProbeBody> {
    if buf.len() < READ_PROBE_BODY_LEN {
        return None;
    }
    Some(ReadProbeBody {
        nonce: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        from: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
    })
}

// -- M6 Task 6: snapshot session (kinds 12–15) --------------------------------
// A bounded, strictly-lower-priority unicast file transfer: the leader ships a
// snapshot artifact to ONE peer whose NAK fell below the journal purge floor.
// MDC-free (single peer), NAK-repaired like the main stream, budget-bounded.

/// leader → peer: opens one artifact of a session. Body = [`SnapBeginBody`];
/// header `position` = 0. M14c: one BEGIN per declared FSM, ascending by id.
pub const DGRAM_KIND_SNAP_BEGIN: u8 = 12;
/// leader → peer: one file chunk. Header `position` = the STREAM-GLOBAL
/// offset — the session is one concatenated byte stream over its artifacts (a
/// chunk's offset within its own file is `position - <that artifact's base>`),
/// NOT the file offset; payload = the raw bytes at that stream offset. A
/// datagram never spans an artifact boundary, so one chunk lands in exactly
/// one `.part`.
pub const DGRAM_KIND_SNAP_CHUNK: u8 = 13;
/// peer → leader: request a missing byte range. Body = [`SnapNakBody`].
pub const DGRAM_KIND_SNAP_NAK: u8 = 14;
/// peer → leader: EVERY artifact of the session is complete (echoes the last
/// artifact's [`SnapBeginBody`] as the ack).
pub const DGRAM_KIND_SNAP_DONE: u8 = 15;

/// M7: follower→leader forwarded membership proposal (`uc2ctl` wrote the
/// local admin slot on a non-leader). Body = `ConfigProposalBody`.
pub const DGRAM_KIND_CONFIG_PROPOSAL: u8 = 16;
/// M7: leader→follower reply for a forwarded proposal. Body = `ConfigReplyBody`.
pub const DGRAM_KIND_CONFIG_REPLY: u8 = 17;

/// Fixed part of a [`SnapBeginBody`] (wire 0.6.0, M14c). 0.5.0's was 26; the
/// 0.6.0 body reuses the old 4-byte pad for `layout` + `service_id` and
/// inserts an 8-byte `services_declared` word before `config_len`. A 0.5.0
/// body is therefore *shorter* than this and is dropped by
/// [`read_snap_begin_body`]'s length check.
pub const SNAP_BEGIN_FIXED_LEN: usize = 34;

/// The value [`SnapBeginBody::layout`] carries on wire 0.6.0. `0` is what a
/// 0.5.0 sender's pad byte reads as — see [`read_snap_begin_body`].
pub const SNAP_BEGIN_LAYOUT_V2: u8 = 1;

pub const CONFIG_PROPOSAL_BODY_LEN: usize = 22;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigProposalBody {
    pub nonce: u64,
    /// `uc_consensus::config::ConfigOp` discriminant (1..=5).
    pub op: u32,
    pub id: u32,
    pub ip: u32,
    pub port: u16,
}

pub fn write_config_proposal_body(buf: &mut [u8], b: &ConfigProposalBody) {
    buf[0..8].copy_from_slice(&b.nonce.to_le_bytes());
    buf[8..12].copy_from_slice(&b.op.to_le_bytes());
    buf[12..16].copy_from_slice(&b.id.to_le_bytes());
    buf[16..20].copy_from_slice(&b.ip.to_le_bytes());
    buf[20..22].copy_from_slice(&b.port.to_le_bytes());
}

pub fn read_config_proposal_body(buf: &[u8]) -> Option<ConfigProposalBody> {
    if buf.len() < CONFIG_PROPOSAL_BODY_LEN {
        return None;
    }
    Some(ConfigProposalBody {
        nonce: u64::from_le_bytes(buf[0..8].try_into().ok()?),
        op: u32::from_le_bytes(buf[8..12].try_into().ok()?),
        id: u32::from_le_bytes(buf[12..16].try_into().ok()?),
        ip: u32::from_le_bytes(buf[16..20].try_into().ok()?),
        port: u16::from_le_bytes(buf[20..22].try_into().ok()?),
    })
}

pub const CONFIG_REPLY_BODY_LEN: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigReplyBody {
    pub nonce: u64,
    /// 0 = accepted; 1 = refused (see `reason`); 2 = retry (leader unknown/changed).
    pub status: u32,
    /// `uc_consensus::config::ProposeError` discriminant when refused.
    pub reason: u32,
    /// New config version when accepted; current version otherwise.
    pub version: u64,
}

pub fn write_config_reply_body(buf: &mut [u8], b: &ConfigReplyBody) {
    buf[0..8].copy_from_slice(&b.nonce.to_le_bytes());
    buf[8..12].copy_from_slice(&b.status.to_le_bytes());
    buf[12..16].copy_from_slice(&b.reason.to_le_bytes());
    buf[16..24].copy_from_slice(&b.version.to_le_bytes());
}

pub fn read_config_reply_body(buf: &[u8]) -> Option<ConfigReplyBody> {
    if buf.len() < CONFIG_REPLY_BODY_LEN {
        return None;
    }
    Some(ConfigReplyBody {
        nonce: u64::from_le_bytes(buf[0..8].try_into().ok()?),
        status: u32::from_le_bytes(buf[8..12].try_into().ok()?),
        reason: u32::from_le_bytes(buf[12..16].try_into().ok()?),
        version: u64::from_le_bytes(buf[16..24].try_into().ok()?),
    })
}

/// Opens (and, echoed back, acks) one artifact of a snapshot session.
///
/// **M14c / wire 0.6.0.** A session is a *stream of artifacts* — one BEGIN per
/// declared FSM, ascending by id, each followed by that artifact's chunks;
/// chunk offsets are stream-global, so `SNAP_NAK` repair is byte-identical to
/// 0.5.0 (spec §14.3). `session` scopes chunk/NAK traffic to one transfer;
/// `layout` is the body discriminator (`SNAP_BEGIN_LAYOUT_V2` on 0.6.0);
/// `service_id` names which FSM's artifact this is; `snapshot_pos` is the
/// artifact's tag `S`; `total_len` is THAT artifact's file size (the receiver
/// pre-sizes its `.part` to it); `services_declared` is the sender's declared
/// FSM bitmask, which the receiver compares against its own and which tells it
/// how many artifacts complete the session; `config` is the length-prefixed
/// encoded config (M7, empty for M6), identical on every BEGIN of a session.
///
/// LE: session 0..4, layout 4, service_id 5, 6..8 zero (u64 alignment for
/// `snapshot_pos`), snapshot_pos 8..16, total_len 16..24,
/// services_declared 24..32, config_len u16 32..34, config bytes 34...
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapBeginBody {
    pub session: u32,
    pub layout: u8,
    pub service_id: u8,
    pub snapshot_pos: u64,
    pub total_len: u64,
    pub services_declared: u64,
    pub config: Vec<u8>,
}

/// Encode a snap-begin body. `layout` is written verbatim — production callers
/// pass [`SNAP_BEGIN_LAYOUT_V2`]; a test forging a legacy-discriminator body
/// passes 0.
pub fn write_snap_begin_body(buf: &mut [u8], b: &SnapBeginBody) {
    buf[0..4].copy_from_slice(&b.session.to_le_bytes());
    buf[4] = b.layout;
    buf[5] = b.service_id;
    buf[6..8].fill(0);
    buf[8..16].copy_from_slice(&b.snapshot_pos.to_le_bytes());
    buf[16..24].copy_from_slice(&b.total_len.to_le_bytes());
    buf[24..32].copy_from_slice(&b.services_declared.to_le_bytes());
    buf[32..34].copy_from_slice(&(b.config.len() as u16).to_le_bytes());
    if !b.config.is_empty() {
        buf[34..34 + b.config.len()].copy_from_slice(&b.config);
    }
}

/// Decode a snap-begin body, or `None` if the buffer is shorter than
/// [`SNAP_BEGIN_FIXED_LEN`] or than the `config_len` it declares (the caller
/// drops a malformed datagram).
///
/// **Total for every `layout` value, including 0.** Deciding what an unknown
/// discriminator means is the receiving node's job, not the decoder's: it
/// counts a named refusal (`peer wire 0.5.0`) and drops the session, which is
/// diagnosable, where a silent `None` here would be indistinguishable from a
/// truncated datagram.
pub fn read_snap_begin_body(buf: &[u8]) -> Option<SnapBeginBody> {
    if buf.len() < SNAP_BEGIN_FIXED_LEN {
        return None;
    }
    let config_len = u16::from_le_bytes(buf[32..34].try_into().ok()?) as usize;
    if buf.len() < SNAP_BEGIN_FIXED_LEN + config_len {
        return None;
    }
    Some(SnapBeginBody {
        session: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
        layout: buf[4],
        service_id: buf[5],
        snapshot_pos: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        total_len: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
        services_declared: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
        config: buf[34..34 + config_len].to_vec(),
    })
}

pub const SNAP_NAK_BODY_LEN: usize = 16;

/// Requests a missing chunk of the snapshot file: `[offset, offset+length)`.
/// `session` scopes it to the active transfer. LE: session 0..4, offset 4..12,
/// length 12..16.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapNakBody {
    pub session: u32,
    pub offset: u64,
    pub length: u32,
}

pub fn write_snap_nak_body(buf: &mut [u8], b: &SnapNakBody) {
    buf[0..4].copy_from_slice(&b.session.to_le_bytes());
    buf[4..12].copy_from_slice(&b.offset.to_le_bytes());
    buf[12..16].copy_from_slice(&b.length.to_le_bytes());
}

/// Decode a snap-NAK body, or `None` if shorter than [`SNAP_NAK_BODY_LEN`].
pub fn read_snap_nak_body(buf: &[u8]) -> Option<SnapNakBody> {
    if buf.len() < SNAP_NAK_BODY_LEN {
        return None;
    }
    Some(SnapNakBody {
        session: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
        offset: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
        length: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
    })
}

pub const REQUEST_VOTE_BODY_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestVoteBody {
    pub new_term: u32,
    pub last_term: u32,
    pub last_durable: u64,
}

pub fn write_request_vote_body(buf: &mut [u8], b: &RequestVoteBody) {
    buf[0..4].copy_from_slice(&b.new_term.to_le_bytes());
    buf[4..8].copy_from_slice(&b.last_term.to_le_bytes());
    buf[8..16].copy_from_slice(&b.last_durable.to_le_bytes());
}

/// Decode a request-vote body, or `None` if the buffer is shorter than
/// [`REQUEST_VOTE_BODY_LEN`]. The receiver still guards the length before
/// calling (belt and braces); the reader is total so that no datagram, however
/// truncated, can panic a node.
pub fn read_request_vote_body(buf: &[u8]) -> Option<RequestVoteBody> {
    if buf.len() < REQUEST_VOTE_BODY_LEN {
        return None;
    }
    Some(RequestVoteBody {
        new_term: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
        last_term: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
        last_durable: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
    })
}

pub const VOTE_BODY_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteBody {
    pub term: u32,
    pub granted: bool,
}

pub fn write_vote_body(buf: &mut [u8], b: &VoteBody) {
    buf[0..4].copy_from_slice(&b.term.to_le_bytes());
    buf[4] = b.granted as u8;
    buf[5..16].fill(0);
}

/// Decode a vote body, or `None` if the buffer is shorter than
/// [`VOTE_BODY_LEN`]. The receiver still guards the length before calling
/// (belt and braces); the reader is total so that no datagram, however
/// truncated, can panic a node.
pub fn read_vote_body(buf: &[u8]) -> Option<VoteBody> {
    if buf.len() < VOTE_BODY_LEN {
        return None;
    }
    Some(VoteBody {
        term: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
        granted: buf[4] != 0,
    })
}

pub const TERM_MAP_HEADER_LEN: usize = 8;
pub const TERM_MAP_ENTRY_LEN: usize = 16;
/// 64 × 16 + 8 = 1032 B — fits the 1392 B MTU body budget with room.
pub const MAX_TERM_MAP_WIRE_ENTRIES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TermMapEntryWire {
    pub term: u32,
    pub base: u64,
}

/// Writes header + entries; returns bytes written. `entries.len()` must be
/// ≤ `MAX_TERM_MAP_WIRE_ENTRIES` (caller ships a suffix).
pub fn write_term_map_body(buf: &mut [u8], entries: &[TermMapEntryWire]) -> usize {
    debug_assert!(entries.len() <= MAX_TERM_MAP_WIRE_ENTRIES);
    buf[0..4].copy_from_slice(&(entries.len() as u32).to_le_bytes());
    buf[4..8].copy_from_slice(&0u32.to_le_bytes());
    let mut o = TERM_MAP_HEADER_LEN;
    for e in entries {
        buf[o..o + 4].copy_from_slice(&e.term.to_le_bytes());
        buf[o + 4..o + 8].copy_from_slice(&0u32.to_le_bytes());
        buf[o + 8..o + 16].copy_from_slice(&e.base.to_le_bytes());
        o += TERM_MAP_ENTRY_LEN;
    }
    o
}

/// Returns the entry count read into `out`, or None if malformed (short
/// buffer, count over the cap, or trailing garbage length).
pub fn read_term_map_body(buf: &[u8], out: &mut [TermMapEntryWire]) -> Option<usize> {
    if buf.len() < TERM_MAP_HEADER_LEN {
        return None;
    }
    let count = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    if count > MAX_TERM_MAP_WIRE_ENTRIES || count > out.len() {
        return None;
    }
    if buf.len() != TERM_MAP_HEADER_LEN + count * TERM_MAP_ENTRY_LEN {
        return None;
    }
    let mut o = TERM_MAP_HEADER_LEN;
    for slot in out.iter_mut().take(count) {
        *slot = TermMapEntryWire {
            term: u32::from_le_bytes(buf[o..o + 4].try_into().unwrap()),
            base: u64::from_le_bytes(buf[o + 8..o + 16].try_into().unwrap()),
        };
        o += TERM_MAP_ENTRY_LEN;
    }
    Some(count)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatagramHeader {
    pub position: u64,
    pub leadership_term_id: u32,
    pub kind: u8,
    pub flags: u8,
    pub key_epoch: u16,
}

/// `buf` must be at least `DATAGRAM_HEADER_LEN` bytes.
pub fn write_datagram_header(buf: &mut [u8], h: &DatagramHeader) {
    buf[OFF_DGRAM_POSITION..OFF_DGRAM_POSITION + 8].copy_from_slice(&h.position.to_le_bytes());
    buf[OFF_DGRAM_TERM_ID..OFF_DGRAM_TERM_ID + 4]
        .copy_from_slice(&h.leadership_term_id.to_le_bytes());
    buf[OFF_DGRAM_KIND] = h.kind;
    buf[OFF_DGRAM_FLAGS] = h.flags;
    buf[OFF_DGRAM_KEY_EPOCH..OFF_DGRAM_KEY_EPOCH + 2].copy_from_slice(&h.key_epoch.to_le_bytes());
}

/// Decode a datagram header, or `None` if the buffer is shorter than
/// [`DATAGRAM_HEADER_LEN`]. Every receiver still guards the length before
/// calling (belt and braces); the reader is total so that no datagram, however
/// truncated, can panic a node — this is the pre-auth parse an unauthenticated
/// network path reaches first.
pub fn read_datagram_header(buf: &[u8]) -> Option<DatagramHeader> {
    if buf.len() < DATAGRAM_HEADER_LEN {
        return None;
    }
    Some(DatagramHeader {
        position: u64::from_le_bytes(
            buf[OFF_DGRAM_POSITION..OFF_DGRAM_POSITION + 8]
                .try_into()
                .unwrap(),
        ),
        leadership_term_id: u32::from_le_bytes(
            buf[OFF_DGRAM_TERM_ID..OFF_DGRAM_TERM_ID + 4]
                .try_into()
                .unwrap(),
        ),
        kind: buf[OFF_DGRAM_KIND],
        flags: buf[OFF_DGRAM_FLAGS],
        key_epoch: u16::from_le_bytes(
            buf[OFF_DGRAM_KEY_EPOCH..OFF_DGRAM_KEY_EPOCH + 2]
                .try_into()
                .unwrap(),
        ),
    })
}

/// NAK: "retransmit `length` bytes from `position`" (position is the
/// receiver's contiguous frontier — always a frame start).
pub const NAK_BODY_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NakBody {
    pub position: u64,
    pub length: u32,
}

pub fn write_nak_body(buf: &mut [u8], b: &NakBody) {
    buf[0..8].copy_from_slice(&b.position.to_le_bytes());
    buf[8..12].copy_from_slice(&b.length.to_le_bytes());
    buf[12..16].copy_from_slice(&0u32.to_le_bytes());
}

/// Decode a NAK body, or `None` if the buffer is shorter than
/// [`NAK_BODY_LEN`]. The receiver still guards the length before calling
/// (belt and braces); the reader is total so that no datagram, however
/// truncated, can panic a node.
pub fn read_nak_body(buf: &[u8]) -> Option<NakBody> {
    if buf.len() < NAK_BODY_LEN {
        return None;
    }
    Some(NakBody {
        position: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        length: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
    })
}

/// Status: flow-control advert (spec §5) — contiguous-rebuilt position +
/// receive window (bytes the receiver can still accept beyond it: its own
/// archive gate, `durable + capacity − contiguous`; capacity ≤ 2^31 so it
/// fits u32).
pub const STATUS_BODY_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusBody {
    pub contiguous_position: u64,
    pub receive_window: u32,
}

pub fn write_status_body(buf: &mut [u8], b: &StatusBody) {
    buf[0..8].copy_from_slice(&b.contiguous_position.to_le_bytes());
    buf[8..12].copy_from_slice(&b.receive_window.to_le_bytes());
    buf[12..16].copy_from_slice(&0u32.to_le_bytes());
}

/// Decode a status body, or `None` if the buffer is shorter than
/// [`STATUS_BODY_LEN`]. The receiver still guards the length before calling
/// (belt and braces); the reader is total so that no datagram, however
/// truncated, can panic a node.
pub fn read_status_body(buf: &[u8]) -> Option<StatusBody> {
    if buf.len() < STATUS_BODY_LEN {
        return None;
    }
    Some(StatusBody {
        contiguous_position: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
        receive_window: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M12d: every datagram reader is TOTAL on `&[u8]` — a short buffer is
    /// `None`, never a slice-index panic. The receivers still pre-guard the
    /// length (belt and braces), so this pins the readers themselves, which
    /// is what the `uc_protocol_datagram` fuzz target hammers directly.
    #[test]
    fn short_inputs_are_none_not_panics() {
        for n in 0..DATAGRAM_HEADER_LEN {
            assert!(
                read_datagram_header(&vec![0u8; n]).is_none(),
                "header len {n}"
            );
        }
        assert!(read_datagram_header(&[0u8; DATAGRAM_HEADER_LEN]).is_some());
        for n in 0..REQUEST_VOTE_BODY_LEN {
            assert!(
                read_request_vote_body(&vec![0u8; n]).is_none(),
                "request_vote len {n}"
            );
        }
        for n in 0..VOTE_BODY_LEN {
            assert!(read_vote_body(&vec![0u8; n]).is_none(), "vote len {n}");
        }
        for n in 0..NAK_BODY_LEN {
            assert!(read_nak_body(&vec![0u8; n]).is_none(), "nak len {n}");
        }
        for n in 0..STATUS_BODY_LEN {
            assert!(read_status_body(&vec![0u8; n]).is_none(), "status len {n}");
        }
        assert!(read_request_vote_body(&[0u8; 3]).is_none());
        assert!(read_vote_body(&[0u8; 3]).is_none());
        assert!(read_nak_body(&[0u8; 3]).is_none());
        assert!(read_status_body(&[0u8; 3]).is_none());
    }

    #[test]
    fn header_roundtrip_and_offsets() {
        let h = DatagramHeader {
            position: 0xDEAD_BEEF_0000_0040,
            leadership_term_id: 9,
            kind: DGRAM_KIND_DATA,
            flags: 0x5a,
            key_epoch: 0,
        };
        let mut buf = [0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(&mut buf, &h);
        assert_eq!(read_datagram_header(&buf).unwrap(), h);
        // Pin the ABSOLUTE wire layout with a literal LE byte array — this
        // module is frozen once M3 peers speak it, so a write/read round
        // trip alone isn't enough (both sides could agree on a wrong
        // layout). position=0xDEAD_BEEF_0000_0040 -> LE
        // [0x40,0,0,0,0xEF,0xBE,0xAD,0xDE]; term=9 -> [9,0,0,0]; kind=1;
        // flags=0x5a; key_epoch=0 -> [0,0].
        assert_eq!(
            buf,
            [
                0x40, 0x00, 0x00, 0x00, 0xEF, 0xBE, 0xAD, 0xDE, 9, 0, 0, 0, 1, 0x5a, 0, 0
            ]
        );
        // cleartext epoch stays zero here (M8: this slot is key_epoch)
        assert_eq!(&buf[OFF_DGRAM_KEY_EPOCH..OFF_DGRAM_KEY_EPOCH + 2], &[0, 0]);
        // layout: position(8) term(4) kind(1) flags(1) key_epoch(2) = 16
        assert_eq!(OFF_DGRAM_POSITION, 0);
        assert_eq!(OFF_DGRAM_TERM_ID, 8);
        assert_eq!(OFF_DGRAM_KIND, 12);
        assert_eq!(OFF_DGRAM_FLAGS, 13);
        assert_eq!(OFF_DGRAM_KEY_EPOCH, 14);
        assert_eq!(DATAGRAM_HEADER_LEN, 16);
    }

    #[test]
    fn key_epoch_occupies_the_reserved_slot_and_round_trips() {
        let mut buf = [0u8; DATAGRAM_HEADER_LEN];
        let h = DatagramHeader {
            position: 4096,
            leadership_term_id: 7,
            kind: DGRAM_KIND_DATA,
            flags: 0,
            key_epoch: 0xBEEF,
        };
        write_datagram_header(&mut buf, &h);
        // Pinned at the old reserved offset — the slot the v2 spec set aside.
        assert_eq!(
            &buf[OFF_DGRAM_KEY_EPOCH..OFF_DGRAM_KEY_EPOCH + 2],
            &0xBEEFu16.to_le_bytes()
        );
        assert_eq!(OFF_DGRAM_KEY_EPOCH, 14);
        assert_eq!(read_datagram_header(&buf).unwrap(), h);
    }

    #[test]
    fn cleartext_datagrams_carry_epoch_zero() {
        let mut buf = [0u8; DATAGRAM_HEADER_LEN];
        let h = DatagramHeader {
            position: 0,
            leadership_term_id: 0,
            kind: DGRAM_KIND_HEARTBEAT,
            flags: 0,
            key_epoch: 0,
        };
        write_datagram_header(&mut buf, &h);
        assert_eq!(buf[OFF_DGRAM_KEY_EPOCH], 0);
        assert_eq!(buf[OFF_DGRAM_KEY_EPOCH + 1], 0);
    }

    #[test]
    fn control_bodies_roundtrip() {
        let n = NakBody {
            position: 4096,
            length: 65536,
        };
        let mut buf = [0u8; NAK_BODY_LEN];
        write_nak_body(&mut buf, &n);
        assert_eq!(read_nak_body(&buf).unwrap(), n);
        // Absolute wire pin, not just internal write/read consistency:
        // position=4096=0x1000 -> LE [0,16,0,0,0,0,0,0];
        // length=65536=0x10000 -> LE [0,0,1,0]; reserved=[0,0,0,0].
        assert_eq!(buf, [0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0]);

        let s = StatusBody {
            contiguous_position: 1 << 33,
            receive_window: 1 << 28,
        };
        let mut buf = [0u8; STATUS_BODY_LEN];
        write_status_body(&mut buf, &s);
        assert_eq!(read_status_body(&buf).unwrap(), s);
        // Absolute wire pin: contiguous_position=1<<33=0x2_0000_0000 -> LE
        // [0,0,0,0,2,0,0,0]; receive_window=1<<28=0x1000_0000 -> LE
        // [0,0,0,16]; reserved=[0,0,0,0].
        assert_eq!(buf, [0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 0]);
    }

    #[test]
    fn config_proposal_and_reply_bodies_roundtrip_and_pin_layout() {
        let p = ConfigProposalBody {
            nonce: 7,
            op: 2,
            id: 5,
            ip: 0x0a000005,
            port: 19100,
        };
        let mut buf = [0u8; CONFIG_PROPOSAL_BODY_LEN];
        write_config_proposal_body(&mut buf, &p);
        assert_eq!(&buf[0..8], &7u64.to_le_bytes());
        assert_eq!(&buf[8..12], &2u32.to_le_bytes());
        assert_eq!(&buf[12..16], &5u32.to_le_bytes());
        assert_eq!(&buf[16..20], &0x0a000005u32.to_le_bytes());
        assert_eq!(&buf[20..22], &19100u16.to_le_bytes());
        assert_eq!(read_config_proposal_body(&buf), Some(p));

        let r = ConfigReplyBody {
            nonce: 7,
            status: 0,
            reason: 0,
            version: 4,
        };
        let mut buf = [0u8; CONFIG_REPLY_BODY_LEN];
        write_config_reply_body(&mut buf, &r);
        assert_eq!(&buf[0..8], &7u64.to_le_bytes());
        assert_eq!(&buf[16..24], &4u64.to_le_bytes());
        assert_eq!(read_config_reply_body(&buf), Some(r));
    }

    #[test]
    fn kind_codes_are_stable() {
        assert_eq!(DGRAM_KIND_DATA, 1);
        assert_eq!(DGRAM_KIND_HEARTBEAT, 2);
        assert_eq!(DGRAM_KIND_NAK, 3);
        assert_eq!(DGRAM_KIND_STATUS, 4);
        assert_eq!(DGRAM_KIND_APPEND_POSITION, 5);
        // 0.5.0: the attested-report body rides behind the unchanged header.
        assert_eq!(APPEND_POSITION_BODY_LEN, 8);
        assert_eq!(DATAGRAM_HEADER_LEN, 16);
        assert_eq!(DGRAM_KIND_COMMIT_POSITION, 6);
        assert_eq!(DGRAM_KIND_REQUEST_VOTE, 7);
        assert_eq!(DGRAM_KIND_VOTE, 8);
        assert_eq!(DGRAM_KIND_TERM_MAP, 9);
        assert_eq!(DGRAM_KIND_READ_PROBE, 10);
        assert_eq!(DGRAM_KIND_READ_PROBE_ACK, 11);
        assert_eq!(DGRAM_KIND_SNAP_BEGIN, 12);
        assert_eq!(DGRAM_KIND_SNAP_CHUNK, 13);
        assert_eq!(DGRAM_KIND_SNAP_NAK, 14);
        assert_eq!(DGRAM_KIND_SNAP_DONE, 15);
        assert_eq!(DGRAM_KIND_CONFIG_PROPOSAL, 16);
        assert_eq!(DGRAM_KIND_CONFIG_REPLY, 17);
    }

    #[test]
    fn snap_begin_body_roundtrips_and_pins_layout() {
        assert_eq!(SNAP_BEGIN_FIXED_LEN, 34, "0.6.0 fixed part (spec §14.3)");
        let b = SnapBeginBody {
            session: 0x0A0B_0C0D,
            layout: SNAP_BEGIN_LAYOUT_V2,
            service_id: 2,
            snapshot_pos: 0x1000,
            total_len: 300 * 1024,
            services_declared: 0b101,
            config: vec![],
        };
        let mut buf = vec![0u8; SNAP_BEGIN_FIXED_LEN];
        write_snap_begin_body(&mut buf, &b);
        assert_eq!(read_snap_begin_body(&buf), Some(b));
        // session=0x0A0B0C0D -> LE [0x0D,0x0C,0x0B,0x0A]; layout=1; service_id=2;
        // [6..8] zero; snapshot_pos=0x1000 -> LE [0,0x10,0,0,0,0,0,0];
        // total_len=307200=0x0004_B000 -> LE [0x00,0xB0,0x04,0,0,0,0,0];
        // services_declared=0b101 -> LE [5,0,0,0,0,0,0,0]; config_len=0 -> LE [0,0].
        assert_eq!(
            &buf[..],
            &[
                0x0D, 0x0C, 0x0B, 0x0A, 1, 2, 0, 0, 0x00, 0x10, 0, 0, 0, 0, 0, 0, 0x00, 0xB0, 0x04,
                0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
        // Short buffer is rejected (caller drops the datagram).
        assert_eq!(read_snap_begin_body(&buf[..SNAP_BEGIN_FIXED_LEN - 1]), None);
    }

    /// A wire-0.5.0 sender's SNAP_BEGIN is dropped by the LENGTH check, before
    /// `layout` is even looked at: its fixed part is 26 bytes. The `layout`
    /// refusal on the receiving node (M14c, `uc_net`) is therefore defensive —
    /// it catches a body that is 0.6.0-SHAPED (>= 34 B, which a 0.5.0 body with
    /// an 8-byte-or-longer config reaches) yet carries layout 0.
    #[test]
    fn a_wire_050_shaped_snap_begin_is_too_short_and_a_layout_zero_body_decodes() {
        // The exact 26 bytes a 0.5.0 `write_snap_begin_body` produced.
        let legacy: [u8; 26] = [
            0x0D, 0x0C, 0x0B, 0x0A, 0, 0, 0, 0, 0x00, 0x10, 0, 0, 0, 0, 0, 0, 0x00, 0xB0, 0x04, 0,
            0, 0, 0, 0, 0, 0,
        ];
        assert_eq!(
            read_snap_begin_body(&legacy),
            None,
            "26 bytes is below the 0.6.0 fixed part"
        );
        // 34 bytes with layout 0 DOES decode — the reader is total and hands the
        // discriminator to the caller, which is what decides (spec §14.3).
        let b = SnapBeginBody {
            session: 1,
            layout: 0,
            service_id: 0,
            snapshot_pos: 4096,
            total_len: 64,
            services_declared: 0,
            config: vec![],
        };
        let mut buf = vec![0u8; SNAP_BEGIN_FIXED_LEN];
        write_snap_begin_body(&mut buf, &b);
        let got = read_snap_begin_body(&buf).expect("a 34-byte body always decodes");
        assert_eq!(got.layout, 0);
        assert_eq!(got, b);
    }

    /// `config` still rides at the end and its length is still re-checked
    /// against the buffer actually received.
    #[test]
    fn snap_begin_config_rides_past_the_fixed_part() {
        let cfg = vec![0x11u8, 0x22, 0x33, 0x44];
        let b = SnapBeginBody {
            session: 7,
            layout: SNAP_BEGIN_LAYOUT_V2,
            service_id: 1,
            snapshot_pos: 8192,
            total_len: 1 << 20,
            services_declared: 0b11,
            config: cfg.clone(),
        };
        let mut buf = vec![0u8; SNAP_BEGIN_FIXED_LEN + cfg.len()];
        write_snap_begin_body(&mut buf, &b);
        assert_eq!(&buf[32..34], &[4, 0], "config_len at [32..34]");
        assert_eq!(read_snap_begin_body(&buf), Some(b));
        // Truncated config: refused, not silently short-read.
        assert_eq!(read_snap_begin_body(&buf[..buf.len() - 1]), None);
    }

    #[test]
    fn snap_nak_body_roundtrips_and_pins_layout() {
        let b = SnapNakBody {
            session: 0x0102_0304,
            offset: 0x1_0000,
            length: 1408,
        };
        let mut buf = [0u8; SNAP_NAK_BODY_LEN];
        write_snap_nak_body(&mut buf, &b);
        assert_eq!(read_snap_nak_body(&buf), Some(b));
        // session=0x01020304 -> LE [0x04,0x03,0x02,0x01];
        // offset=0x10000 -> LE [0,0,1,0,0,0,0,0]; length=1408=0x0580 -> LE [0x80,0x05,0,0].
        assert_eq!(
            buf,
            [
                0x04, 0x03, 0x02, 0x01, 0, 0, 1, 0, 0, 0, 0, 0, 0x80, 0x05, 0, 0
            ]
        );
        assert_eq!(read_snap_nak_body(&buf[..SNAP_NAK_BODY_LEN - 1]), None);
    }

    #[test]
    fn vote_bodies_roundtrip_and_pin_layout() {
        let rv = RequestVoteBody {
            new_term: 7,
            last_term: 6,
            last_durable: 0x0000_0001_0000_0040,
        };
        let mut buf = [0u8; REQUEST_VOTE_BODY_LEN];
        write_request_vote_body(&mut buf, &rv);
        assert_eq!(read_request_vote_body(&buf).unwrap(), rv);
        // literal LE pin: new_term 7, last_term 6, last_durable 2^32+64
        assert_eq!(buf, [7, 0, 0, 0, 6, 0, 0, 0, 0x40, 0, 0, 0, 1, 0, 0, 0]);

        let v = VoteBody {
            term: 7,
            granted: true,
        };
        let mut buf = [0u8; VOTE_BODY_LEN];
        write_vote_body(&mut buf, &v);
        assert_eq!(read_vote_body(&buf).unwrap(), v);
        assert_eq!(buf, [7, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let v = VoteBody {
            term: 7,
            granted: false,
        };
        write_vote_body(&mut buf, &v);
        assert_eq!(buf[4], 0);
    }

    #[test]
    fn read_probe_body_roundtrips_and_pins_layout() {
        let b = ReadProbeBody {
            nonce: 0x0102_0304_0506_0708,
            from: 0x0A0B_0C0D,
        };
        let mut buf = [0u8; READ_PROBE_BODY_LEN];
        write_read_probe_body(&mut buf, &b);
        assert_eq!(read_read_probe_body(&buf), Some(b));
        // Absolute LE wire pin (not just a round trip — both sides could agree
        // on a swapped field order): nonce 0x0102030405060708 -> LE bytes
        // 0..8; from 0x0A0B0C0D -> LE bytes 8..12; bytes 12..16 zero.
        assert_eq!(
            buf,
            [
                0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x0D, 0x0C, 0x0B, 0x0A, 0, 0, 0, 0
            ]
        );
        // A longer buffer decodes the same 16-byte prefix (trailing bytes ignored).
        let mut long = [0xFFu8; READ_PROBE_BODY_LEN + 8];
        write_read_probe_body(&mut long[..READ_PROBE_BODY_LEN], &b);
        assert_eq!(read_read_probe_body(&long), Some(b));
        // Malformed: any buffer shorter than the 16-byte body -> None (must not
        // panic on slicing). Pin every boundary below the length.
        for short in 0..READ_PROBE_BODY_LEN {
            assert!(
                read_read_probe_body(&buf[..short]).is_none(),
                "len {short} must reject"
            );
        }
        assert!(read_read_probe_body(&[]).is_none());
        // The two new kind codes are stable.
        assert_eq!(DGRAM_KIND_READ_PROBE, 10);
        assert_eq!(DGRAM_KIND_READ_PROBE_ACK, 11);
    }

    #[test]
    fn append_position_body_roundtrip_and_short_buffer() {
        let mut buf = [0xAAu8; APPEND_POSITION_BODY_LEN];
        write_append_position_body(
            &mut buf,
            &AppendPositionBody {
                durable_term: 4_000_000_007,
            },
        );
        assert_eq!(
            read_append_position_body(&buf),
            Some(AppendPositionBody {
                durable_term: 4_000_000_007
            })
        );
        // Reserved tail is zeroed, not left as the caller's garbage.
        assert_eq!(&buf[4..8], &[0, 0, 0, 0]);
        // A 0.4.0 peer's header-only report: no body at all -> unattested.
        assert_eq!(read_append_position_body(&[]), None);
        assert_eq!(read_append_position_body(&buf[..7]), None);
    }

    #[test]
    fn term_map_body_roundtrips_and_pins_layout() {
        let entries = [
            TermMapEntryWire { term: 1, base: 0 },
            TermMapEntryWire {
                term: 3,
                base: 4096,
            },
        ];
        let mut buf = [0u8; TERM_MAP_HEADER_LEN + 2 * TERM_MAP_ENTRY_LEN];
        let n = write_term_map_body(&mut buf, &entries);
        assert_eq!(n, 8 + 32);
        let mut out = [TermMapEntryWire { term: 0, base: 0 }; MAX_TERM_MAP_WIRE_ENTRIES];
        let m = read_term_map_body(&buf[..n], &mut out).expect("well-formed");
        assert_eq!(&out[..m], &entries);
        // literal pin: count 2, reserved 0, entry0 {1, rsvd, base 0}, entry1 {3, rsvd, base 4096}
        assert_eq!(
            &buf[..n],
            &[
                2, 0, 0, 0, 0, 0, 0, 0, // count + reserved
                1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // term 1, base 0
                3, 0, 0, 0, 0, 0, 0, 0, 0, 0x10, 0, 0, 0, 0, 0, 0, // term 3, base 4096
            ][..]
        );
        // malformed: truncated entry -> None
        assert!(read_term_map_body(&buf[..n - 1], &mut out).is_none());
        // malformed: count beyond the cap -> None
        let mut big = [0u8; TERM_MAP_HEADER_LEN];
        big[0..4].copy_from_slice(&(MAX_TERM_MAP_WIRE_ENTRIES as u32 + 1).to_le_bytes());
        assert!(read_term_map_body(&big, &mut out).is_none());
        // malformed: buffer shorter than the 8-byte header -> None (the
        // third documented failure mode; must not panic on slicing)
        assert!(read_term_map_body(&buf[..TERM_MAP_HEADER_LEN - 1], &mut out).is_none());
        assert!(read_term_map_body(&[], &mut out).is_none());
    }
}
