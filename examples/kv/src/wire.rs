//! The store's own wire format: the bytes inside a UC `SUBMIT`/`QUERY`
//! payload and inside a `RESPONSE` body. Normative layout in
//! `WIRE-FORMAT.md`; this module is its one implementation, used by the
//! state machine (decode commands, encode replies) and by `kv` (the reverse).
//!
//! Every integer is little-endian. v2 added `APPEND`/`LIST` and two statuses;
//! every v1 byte is unchanged. Every frame is self-delimiting from the
//! front, so a later format version can append fields.

use bytes::Bytes;

/// Format version byte at offset 0 of every command and query.
pub const FORMAT_VERSION: u8 = 1;

/// Longest key, in bytes. Keys are opaque and non-empty.
pub const MAX_KEY: usize = 256;
/// Longest value, in bytes. Values are opaque and may be empty. Also the
/// longest list element (v2): an element rides one `APPEND` command.
pub const MAX_VALUE: usize = 1024;

/// v2: most elements one list may hold. Bounds the `LIST` reply, which
/// must fit UC's 1 MiB remote frame (`MAX_FRAME_LEN`) with a wide margin.
pub const MAX_LIST_LEN: usize = 4096;
/// v2: most bytes one list may hold, summed over its elements.
pub const MAX_LIST_BYTES: usize = 65536;
/// The largest possible `LIST` reply: status ‖ version ‖ count ‖ 4096 lengths ‖ bytes.
pub const MAX_LIST_REPLY: usize = 1 + 8 + 4 + MAX_LIST_LEN * 4 + MAX_LIST_BYTES;
const _: () = assert!(MAX_LIST_REPLY < 1 << 20);

/// The platform's standard command ceiling — the one size that holds on every
/// cluster (baseline 1408 B datagram rung, wire crypto on). Pinned to
/// `uc_remote::engine::STANDARD_PAYLOAD` by a test.
pub const STANDARD_PAYLOAD: usize = 1312;
/// The gateway prepends `client_id: u64 ‖ seq: u64` to every `SUBMIT`.
pub const SESSION_ENVELOPE: usize = 16;
/// What an application command may occupy once the envelope is paid for.
pub const COMMAND_BUDGET: usize = STANDARD_PAYLOAD - SESSION_ENVELOPE;

// The derived limits fit the budget with room to spare (DESIGN.md § 5).
const _: () = assert!(12 + MAX_KEY + MAX_VALUE <= COMMAND_BUDGET);

// Command ops (SUBMIT).
pub const OP_PUT: u8 = 1;
pub const OP_DELETE: u8 = 2;
pub const OP_CAS: u8 = 3;
/// v2.
pub const OP_APPEND: u8 = 4;

// Query ops (QUERY).
pub const QOP_GET: u8 = 1;
pub const QOP_DIGEST: u8 = 2;
/// v2.
pub const QOP_LIST: u8 = 3;

// Response status byte.
pub const ST_OK: u8 = 0;
pub const ST_NOT_FOUND: u8 = 1;
pub const ST_VERSION_MISMATCH: u8 = 2;
pub const ST_BAD_REQUEST: u8 = 3;
/// v2: the key holds the other shape (a list where a value was expected, or
/// vice versa). Nothing was changed.
pub const ST_WRONG_SHAPE: u8 = 4;
/// v2: `APPEND` would exceed `MAX_LIST_LEN` or `MAX_LIST_BYTES`. Nothing was changed.
pub const ST_LIST_FULL: u8 = 5;

// BAD_REQUEST reason byte.
pub const BAD_TRUNCATED: u8 = 1;
pub const BAD_FORMAT_VERSION: u8 = 2;
pub const BAD_UNKNOWN_OP: u8 = 3;
pub const BAD_KEY_LEN: u8 = 4;
pub const BAD_VALUE_LEN: u8 = 5;
pub const BAD_TRAILING: u8 = 6;

/// A decoded command, borrowing from the frame.
#[derive(Debug, PartialEq, Eq)]
pub enum Command<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
    Cas { key: &'a [u8], expected: u64, value: &'a [u8] },
    /// v2.
    Append { key: &'a [u8], value: &'a [u8] },
}

/// A decoded query, borrowing from the frame.
#[derive(Debug, PartialEq, Eq)]
pub enum Query<'a> {
    Get { key: &'a [u8] },
    Digest,
    /// v2.
    List { key: &'a [u8] },
}

/// What a client sees back from a `Put`/`Delete`/`CAS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteReply {
    /// `Put`/`CAS`: the new version. `Delete`: the version that was removed.
    Ok { version: u64 },
    /// `Delete` of an absent key.
    NotFound,
    /// `CAS` whose expectation did not hold; `current` is `0` when absent.
    VersionMismatch { current: u64 },
    BadRequest(u8),
    /// v2: the key is a list.
    WrongShape,
}

/// What a client sees back from a `Get`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetReply {
    Found { version: u64, value: Bytes },
    NotFound,
    BadRequest(u8),
    /// v2: the key is a list.
    WrongShape,
}

/// v2: what a client sees back from an `Append`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendReply {
    /// The list's new version and length.
    Ok { version: u64, len: u32 },
    /// The key is a value.
    WrongShape,
    /// The list is at a cap; `len` is its unchanged length.
    ListFull { len: u32 },
    BadRequest(u8),
}

/// v2: what a client sees back from a `List`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListReply {
    Found { version: u64, items: Vec<Bytes> },
    NotFound,
    /// The key is a value.
    WrongShape,
    BadRequest(u8),
}

impl ListReply {
    pub fn len(&self) -> usize {
        match self {
            ListReply::Found { items, .. } => items.len(),
            _ => 0,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// What a client sees back from a `Digest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DigestReply {
    pub count: u64,
    pub digest: u64,
    /// The replying replica's `last_applied` (0 before any apply).
    pub last_applied: u64,
}

/// Why a client-side encoder or reply decoder refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    KeyLen(usize),
    ValueLen(usize),
    /// A reply that does not parse: the status byte, or `None` if empty.
    BadReply(Option<u8>),
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireError::KeyLen(n) => write!(f, "key length {n} is outside 1..={MAX_KEY}"),
            WireError::ValueLen(n) => write!(f, "value length {n} exceeds {MAX_VALUE}"),
            WireError::BadReply(Some(s)) => write!(f, "unparseable reply (status byte {s})"),
            WireError::BadReply(None) => write!(f, "empty reply"),
        }
    }
}
impl std::error::Error for WireError {}

// ---------------------------------------------------------------- encoders (client side)

fn check_key(key: &[u8]) -> Result<(), WireError> {
    if key.is_empty() || key.len() > MAX_KEY { Err(WireError::KeyLen(key.len())) } else { Ok(()) }
}
fn check_value(value: &[u8]) -> Result<(), WireError> {
    if value.len() > MAX_VALUE { Err(WireError::ValueLen(value.len())) } else { Ok(()) }
}

fn head(op: u8, key: &[u8], extra: usize) -> Vec<u8> {
    let mut f = Vec::with_capacity(4 + key.len() + extra);
    f.push(FORMAT_VERSION);
    f.push(op);
    f.extend_from_slice(&(key.len() as u16).to_le_bytes());
    f.extend_from_slice(key);
    f
}

pub fn try_encode_put(key: &[u8], value: &[u8]) -> Result<Vec<u8>, WireError> {
    check_key(key)?;
    check_value(value)?;
    let mut f = head(OP_PUT, key, value.len());
    f.extend_from_slice(value);
    Ok(f)
}

pub fn try_encode_delete(key: &[u8]) -> Result<Vec<u8>, WireError> {
    check_key(key)?;
    Ok(head(OP_DELETE, key, 0))
}

pub fn try_encode_cas(key: &[u8], expected: u64, value: &[u8]) -> Result<Vec<u8>, WireError> {
    check_key(key)?;
    check_value(value)?;
    let mut f = head(OP_CAS, key, 8 + value.len());
    f.extend_from_slice(&expected.to_le_bytes());
    f.extend_from_slice(value);
    Ok(f)
}

/// v2. Same framing as PUT with `op = 4`.
pub fn try_encode_append(key: &[u8], value: &[u8]) -> Result<Vec<u8>, WireError> {
    check_key(key)?;
    check_value(value)?;
    let mut f = head(OP_APPEND, key, value.len());
    f.extend_from_slice(value);
    Ok(f)
}

/// v2.
pub fn try_encode_list(key: &[u8]) -> Result<Vec<u8>, WireError> {
    check_key(key)?;
    Ok(head(QOP_LIST, key, 0))
}

pub fn try_encode_get(key: &[u8]) -> Result<Vec<u8>, WireError> {
    check_key(key)?;
    Ok(head(QOP_GET, key, 0))
}

pub fn encode_digest() -> Vec<u8> {
    vec![FORMAT_VERSION, QOP_DIGEST]
}

/// Panicking conveniences for callers that already checked the sizes (tests).
pub fn encode_put(key: &[u8], value: &[u8]) -> Vec<u8> { try_encode_put(key, value).expect("sizes checked") }
pub fn encode_delete(key: &[u8]) -> Vec<u8> { try_encode_delete(key).expect("sizes checked") }
pub fn encode_cas(key: &[u8], expected: u64, value: &[u8]) -> Vec<u8> { try_encode_cas(key, expected, value).expect("sizes checked") }
pub fn encode_get(key: &[u8]) -> Vec<u8> { try_encode_get(key).expect("sizes checked") }
pub fn encode_append(key: &[u8], value: &[u8]) -> Vec<u8> { try_encode_append(key, value).expect("sizes checked") }
pub fn encode_list(key: &[u8]) -> Vec<u8> { try_encode_list(key).expect("sizes checked") }

// ---------------------------------------------------------------- decoders (state-machine side)

/// `Err(reason)` is a `BAD_REQUEST` reason byte, never a panic: the bytes
/// come from an unauthenticated remote client through the gateway.
fn decode_head(frame: &[u8]) -> Result<(u8, &[u8], &[u8]), u8> {
    if frame.len() < 2 {
        return Err(BAD_TRUNCATED);
    }
    if frame[0] != FORMAT_VERSION {
        return Err(BAD_FORMAT_VERSION);
    }
    Ok((frame[1], &[], &frame[2..]))
}

fn take_key(rest: &[u8]) -> Result<(&[u8], &[u8]), u8> {
    if rest.len() < 2 {
        return Err(BAD_TRUNCATED);
    }
    let n = u16::from_le_bytes([rest[0], rest[1]]) as usize;
    if n == 0 || n > MAX_KEY {
        return Err(BAD_KEY_LEN);
    }
    let rest = &rest[2..];
    if rest.len() < n {
        return Err(BAD_TRUNCATED);
    }
    Ok((&rest[..n], &rest[n..]))
}

fn take_u64(rest: &[u8]) -> Result<(u64, &[u8]), u8> {
    if rest.len() < 8 {
        return Err(BAD_TRUNCATED);
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&rest[..8]);
    Ok((u64::from_le_bytes(b), &rest[8..]))
}

fn take_value(rest: &[u8]) -> Result<&[u8], u8> {
    if rest.len() > MAX_VALUE { Err(BAD_VALUE_LEN) } else { Ok(rest) }
}

pub fn decode_command(frame: &[u8]) -> Result<Command<'_>, u8> {
    let (op, _, rest) = decode_head(frame)?;
    match op {
        OP_PUT => {
            let (key, rest) = take_key(rest)?;
            Ok(Command::Put { key, value: take_value(rest)? })
        }
        OP_DELETE => {
            let (key, rest) = take_key(rest)?;
            if !rest.is_empty() {
                return Err(BAD_TRAILING);
            }
            Ok(Command::Delete { key })
        }
        OP_CAS => {
            let (key, rest) = take_key(rest)?;
            let (expected, rest) = take_u64(rest)?;
            Ok(Command::Cas { key, expected, value: take_value(rest)? })
        }
        OP_APPEND => {
            let (key, rest) = take_key(rest)?;
            Ok(Command::Append { key, value: take_value(rest)? })
        }
        _ => Err(BAD_UNKNOWN_OP),
    }
}

pub fn decode_query(frame: &[u8]) -> Result<Query<'_>, u8> {
    let (op, _, rest) = decode_head(frame)?;
    match op {
        QOP_GET => {
            let (key, rest) = take_key(rest)?;
            if !rest.is_empty() {
                return Err(BAD_TRAILING);
            }
            Ok(Query::Get { key })
        }
        QOP_DIGEST => {
            if !rest.is_empty() {
                return Err(BAD_TRAILING);
            }
            Ok(Query::Digest)
        }
        QOP_LIST => {
            let (key, rest) = take_key(rest)?;
            if !rest.is_empty() {
                return Err(BAD_TRAILING);
            }
            Ok(Query::List { key })
        }
        _ => Err(BAD_UNKNOWN_OP),
    }
}

// ---------------------------------------------------------------- reply encoders (state-machine side)

pub fn put_status(out: &mut Vec<u8>, status: u8) {
    out.push(status);
}
pub fn put_status_u64(out: &mut Vec<u8>, status: u8, v: u64) {
    out.push(status);
    out.extend_from_slice(&v.to_le_bytes());
}
pub fn put_bad_request(out: &mut Vec<u8>, reason: u8) {
    out.push(ST_BAD_REQUEST);
    out.push(reason);
}

// ---------------------------------------------------------------- reply decoders (client side)

fn reply_u64(b: &[u8]) -> Option<u64> {
    if b.len() < 8 {
        return None;
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8]);
    Some(u64::from_le_bytes(a))
}

pub fn decode_write_reply(b: &[u8]) -> Result<WriteReply, WireError> {
    let bad = || WireError::BadReply(b.first().copied());
    match b.first().copied() {
        Some(ST_OK) => Ok(WriteReply::Ok { version: reply_u64(&b[1..]).ok_or_else(bad)? }),
        Some(ST_NOT_FOUND) => Ok(WriteReply::NotFound),
        Some(ST_VERSION_MISMATCH) => Ok(WriteReply::VersionMismatch { current: reply_u64(&b[1..]).ok_or_else(bad)? }),
        Some(ST_BAD_REQUEST) => Ok(WriteReply::BadRequest(*b.get(1).ok_or_else(bad)?)),
        Some(ST_WRONG_SHAPE) => Ok(WriteReply::WrongShape),
        _ => Err(bad()),
    }
}

/// v2.
pub fn decode_append_reply(b: &[u8]) -> Result<AppendReply, WireError> {
    let bad = || WireError::BadReply(b.first().copied());
    let u32_at = |o: usize| -> Option<u32> { b.get(o..o + 4).map(|x| u32::from_le_bytes([x[0], x[1], x[2], x[3]])) };
    match b.first().copied() {
        Some(ST_OK) => Ok(AppendReply::Ok { version: reply_u64(&b[1..]).ok_or_else(bad)?, len: u32_at(9).ok_or_else(bad)? }),
        Some(ST_WRONG_SHAPE) => Ok(AppendReply::WrongShape),
        Some(ST_LIST_FULL) => Ok(AppendReply::ListFull { len: u32_at(1).ok_or_else(bad)? }),
        Some(ST_BAD_REQUEST) => Ok(AppendReply::BadRequest(*b.get(1).ok_or_else(bad)?)),
        _ => Err(bad()),
    }
}

/// v2.
pub fn decode_list_reply(b: &[u8]) -> Result<ListReply, WireError> {
    let bad = || WireError::BadReply(b.first().copied());
    match b.first().copied() {
        Some(ST_OK) => {
            let version = reply_u64(&b[1..]).ok_or_else(bad)?;
            let count = b.get(9..13).map(|x| u32::from_le_bytes([x[0], x[1], x[2], x[3]])).ok_or_else(bad)? as usize;
            let mut items = Vec::with_capacity(count.min(MAX_LIST_LEN));
            let mut off = 13;
            for _ in 0..count {
                let n = b.get(off..off + 4).map(|x| u32::from_le_bytes([x[0], x[1], x[2], x[3]])).ok_or_else(bad)? as usize;
                off += 4;
                items.push(Bytes::copy_from_slice(b.get(off..off + n).ok_or_else(bad)?));
                off += n;
            }
            if off != b.len() {
                return Err(bad());
            }
            Ok(ListReply::Found { version, items })
        }
        Some(ST_NOT_FOUND) => Ok(ListReply::NotFound),
        Some(ST_WRONG_SHAPE) => Ok(ListReply::WrongShape),
        Some(ST_BAD_REQUEST) => Ok(ListReply::BadRequest(*b.get(1).ok_or_else(bad)?)),
        _ => Err(bad()),
    }
}

pub fn decode_get_reply(b: &[u8]) -> Result<GetReply, WireError> {
    let bad = || WireError::BadReply(b.first().copied());
    match b.first().copied() {
        Some(ST_OK) => {
            let version = reply_u64(&b[1..]).ok_or_else(bad)?;
            Ok(GetReply::Found { version, value: Bytes::copy_from_slice(&b[9..]) })
        }
        Some(ST_NOT_FOUND) => Ok(GetReply::NotFound),
        Some(ST_BAD_REQUEST) => Ok(GetReply::BadRequest(*b.get(1).ok_or_else(bad)?)),
        Some(ST_WRONG_SHAPE) => Ok(GetReply::WrongShape),
        _ => Err(bad()),
    }
}

pub fn decode_digest_reply(b: &[u8]) -> Result<DigestReply, WireError> {
    let bad = || WireError::BadReply(b.first().copied());
    if b.first().copied() != Some(ST_OK) || b.len() != 25 {
        return Err(bad());
    }
    Ok(DigestReply {
        count: reply_u64(&b[1..]).ok_or_else(bad)?,
        digest: reply_u64(&b[9..]).ok_or_else(bad)?,
        last_applied: reply_u64(&b[17..]).ok_or_else(bad)?,
    })
}
