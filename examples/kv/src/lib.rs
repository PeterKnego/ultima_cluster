//! A replicated key-value store on `ultima_cluster`: the state machine.
//!
//! `KvSm` implements the raw tier (`RawStateMachine`) — it owns its own wire
//! format (`wire`) so the bytes a non-Rust client sends are exactly the
//! bytes `apply` sees — and `SnapshotStateMachine`, so a cluster can bound
//! its journal. In production it runs wrapped in `uc_service::Sessioned`
//! (see `bin/kv-service.rs`), which is what makes a retried write apply once.
//!
//! v2 (`KV_VERSION = 2.0.0`): a key is one of two **shapes** — a value
//! (Put/Get/CAS) or a list (Append/List). Shapes are strict: an operation of
//! the other shape is refused `WRONG_SHAPE` and changes nothing; only Delete
//! removes either. The snapshot image moved to version 2 and this binary
//! still reads version 1 (`WIRE-FORMAT.md` § 5).
//!
//! Determinism rules (docs/DESIGN.md § 1): no clock, no randomness, no I/O,
//! no `HashMap`, no panic on any input. Everything here is a pure function
//! of the committed log.

pub mod wire;

use std::io::{BufRead, BufReader, BufWriter, Read, Write};

use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;
use uc_service::{ApplyCtx, RawStateMachine, SnapshotError, SnapshotStateMachine};

use wire::{Command, Query};

/// What a key holds. Plain std collections behind an `Arc` on the map (see
/// `KvSm::map`): the example carries no persistent-map dependency — `im` is
/// unmaintained with an open unsoundness advisory (RUSTSEC-2023-0126), and
/// this is the code people copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Shape {
    Value(Bytes),
    /// Oldest first.
    List(Vec<Bytes>),
}

/// One stored key and the version (log position) that last wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub version: u64,
    pub shape: Shape,
}

/// The replicated state. `Default` is the genesis state (empty, nothing applied).
#[derive(Default)]
pub struct KvSm {
    map: Arc<BTreeMap<Bytes, Entry>>,
    last_applied: Option<u64>,
    /// XOR of `entry_hash` over every entry: order-independent, O(1) to
    /// maintain, and what `DIGEST` reports so replicas can be compared.
    digest: u64,
}

/// Packed semantic version `major:8 ‖ minor:8 ‖ patch:16`
/// (`uc_protocol::identity::pack_version`'s frozen layout). **`2.0.0`**:
/// major, because a v1 binary cannot read a v2 image and does not know
/// `APPEND`. v1 was `1.0.0` (`0x0100_0000`).
pub const KV_VERSION: u32 = pack_version(2, 0, 0);

pub const fn pack_version(major: u8, minor: u8, patch: u16) -> u32 {
    ((major as u32) << 24) | ((minor as u32) << 16) | patch as u32
}

/// The snapshot image's own version, independent of `KV_VERSION`.
/// 1 = v1 (values only), 2 = v2 (a shape byte per entry). Written: 2. Read: 1 and 2.
const IMAGE_VERSION_V1: u32 = 1;
const IMAGE_VERSION_V2: u32 = 2;
const IMAGE_VERSION: u32 = IMAGE_VERSION_V2;
/// "No cursor" in the image: nothing was ever applied.
const NO_CURSOR: u64 = u64::MAX;
/// Shape byte in a v2 image entry.
const SHAPE_VALUE: u8 = 0;
const SHAPE_LIST: u8 = 1;

/// FNV-1a 64 — chosen because it is trivial to reimplement in any language
/// and has no architecture-dependent behaviour.
pub fn fnv1a64(parts: &[&[u8]]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for p in parts {
        for &b in *p {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    h
}

/// A value entry hashes exactly as in v1 (so a v1 image's digest verifies);
/// a list entry is domain-separated by a leading `b"L"`.
fn entry_hash(key: &[u8], e: &Entry) -> u64 {
    match &e.shape {
        Shape::Value(v) => fnv1a64(&[
            &(key.len() as u16).to_le_bytes(),
            key,
            &e.version.to_le_bytes(),
            &(v.len() as u32).to_le_bytes(),
            v,
        ]),
        Shape::List(items) => {
            let mut parts: Vec<&[u8]> = Vec::with_capacity(4 + 2 * items.len());
            let kl = (key.len() as u16).to_le_bytes();
            let ver = e.version.to_le_bytes();
            let n = (items.len() as u32).to_le_bytes();
            parts.extend_from_slice(&[b"L", &kl, key, &ver, &n]);
            let lens: Vec<[u8; 4]> = items
                .iter()
                .map(|i| (i.len() as u32).to_le_bytes())
                .collect();
            for (i, l) in items.iter().zip(&lens) {
                parts.push(l);
                parts.push(i);
            }
            fnv1a64(&parts)
        }
    }
}

impl KvSm {
    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.map.get(key)
    }
    pub fn digest(&self) -> u64 {
        self.digest
    }

    /// O(n) recomputation of the incremental digest — a test oracle and the
    /// check `install_snapshot` runs on an image.
    pub fn recompute_digest(&self) -> u64 {
        Self::digest_of(&self.map)
    }

    fn digest_of(map: &BTreeMap<Bytes, Entry>) -> u64 {
        map.iter().fold(0u64, |d, (k, e)| d ^ entry_hash(k, e))
    }

    fn insert(&mut self, key: &[u8], entry: Entry) -> Option<Entry> {
        let new_hash = entry_hash(key, &entry);
        let old = Arc::make_mut(&mut self.map).insert(Bytes::copy_from_slice(key), entry);
        if let Some(old) = &old {
            self.digest ^= entry_hash(key, old);
        }
        self.digest ^= new_hash;
        old
    }

    fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        let old = Arc::make_mut(&mut self.map).remove(key);
        if let Some(old) = &old {
            self.digest ^= entry_hash(key, old);
        }
        old
    }
}

fn write_list(out: &mut Vec<u8>, version: u64, items: &[Bytes]) {
    wire::put_status_u64(out, wire::ST_OK, version);
    out.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for i in items {
        out.extend_from_slice(&(i.len() as u32).to_le_bytes());
        out.extend_from_slice(i);
    }
}

impl RawStateMachine for KvSm {
    const NAME: &'static str = "kv";
    const VERSION: u32 = KV_VERSION;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        let position = ctx.position;
        // A user frame never applies at position 0 (L11); `0` is the CAS
        // "absent" sentinel, so this is the one place that assumption bites.
        debug_assert!(position != 0, "a command applied at log position 0");
        match wire::decode_command(cmd) {
            Ok(Command::Put { key, value }) => match self.map.get(key) {
                Some(Entry {
                    shape: Shape::List(_),
                    ..
                }) => wire::put_status(out, wire::ST_WRONG_SHAPE),
                _ => {
                    self.insert(
                        key,
                        Entry {
                            version: position,
                            shape: Shape::Value(Bytes::copy_from_slice(value)),
                        },
                    );
                    wire::put_status_u64(out, wire::ST_OK, position);
                }
            },
            Ok(Command::Delete { key }) => match self.remove(key) {
                Some(old) => wire::put_status_u64(out, wire::ST_OK, old.version),
                None => wire::put_status(out, wire::ST_NOT_FOUND),
            },
            Ok(Command::Cas {
                key,
                expected,
                value,
            }) => match self.map.get(key) {
                Some(Entry {
                    shape: Shape::List(_),
                    ..
                }) => wire::put_status(out, wire::ST_WRONG_SHAPE),
                cur => {
                    let current = cur.map(|e| e.version).unwrap_or(0);
                    if current == expected {
                        self.insert(
                            key,
                            Entry {
                                version: position,
                                shape: Shape::Value(Bytes::copy_from_slice(value)),
                            },
                        );
                        wire::put_status_u64(out, wire::ST_OK, position);
                    } else {
                        wire::put_status_u64(out, wire::ST_VERSION_MISMATCH, current);
                    }
                }
            },
            Ok(Command::Append { key, value }) => {
                let items = match self.map.get(key) {
                    Some(Entry {
                        shape: Shape::List(items),
                        ..
                    }) => items.clone(),
                    Some(Entry {
                        shape: Shape::Value(_),
                        ..
                    }) => {
                        wire::put_status(out, wire::ST_WRONG_SHAPE);
                        self.last_applied = Some(position);
                        return;
                    }
                    None => Vec::new(),
                };
                let bytes: usize = items.iter().map(|i| i.len()).sum();
                if items.len() >= wire::MAX_LIST_LEN || bytes + value.len() > wire::MAX_LIST_BYTES {
                    out.push(wire::ST_LIST_FULL);
                    out.extend_from_slice(&(items.len() as u32).to_le_bytes());
                } else {
                    let mut items = items;
                    items.push(Bytes::copy_from_slice(value));
                    let len = items.len() as u32;
                    self.insert(
                        key,
                        Entry {
                            version: position,
                            shape: Shape::List(items),
                        },
                    );
                    wire::put_status_u64(out, wire::ST_OK, position);
                    out.extend_from_slice(&len.to_le_bytes());
                }
            }
            Err(reason) => wire::put_bad_request(out, reason),
        }
        // The frame was consumed whatever it contained: the cursor moves.
        self.last_applied = Some(position);
    }

    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        match wire::decode_query(q) {
            Ok(Query::Get { key }) => match self.map.get(key) {
                Some(Entry {
                    version,
                    shape: Shape::Value(v),
                }) => {
                    wire::put_status_u64(out, wire::ST_OK, *version);
                    out.extend_from_slice(v);
                }
                Some(Entry {
                    shape: Shape::List(_),
                    ..
                }) => wire::put_status(out, wire::ST_WRONG_SHAPE),
                None => wire::put_status(out, wire::ST_NOT_FOUND),
            },
            Ok(Query::List { key }) => match self.map.get(key) {
                Some(Entry {
                    version,
                    shape: Shape::List(items),
                }) => write_list(out, *version, items),
                Some(Entry {
                    shape: Shape::Value(_),
                    ..
                }) => wire::put_status(out, wire::ST_WRONG_SHAPE),
                None => wire::put_status(out, wire::ST_NOT_FOUND),
            },
            Ok(Query::Digest) => {
                out.push(wire::ST_OK);
                out.extend_from_slice(&(self.map.len() as u64).to_le_bytes());
                out.extend_from_slice(&self.digest.to_le_bytes());
                out.extend_from_slice(&self.last_applied.unwrap_or(0).to_le_bytes());
            }
            Err(reason) => wire::put_bad_request(out, reason),
        }
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

/// A frozen view: an O(1) `Arc` clone of the map plus the two scalars —
/// exactly the "clone an `Arc`" shape `state-machine-contract.md` § Snapshots
/// asks for. The O(n) copy happens instead on the FIRST write after a freeze
/// (`Arc::make_mut` in `insert`/`remove`), once per instant, on the apply
/// thread; see `docs/DESIGN.md` § "Why `Arc<BTreeMap>`".
pub struct Frozen {
    map: Arc<BTreeMap<Bytes, Entry>>,
    last_applied: Option<u64>,
    digest: u64,
}

fn read_u64(r: &mut impl Read) -> Result<u64, SnapshotError> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b).map_err(SnapshotError::Io)?;
    Ok(u64::from_le_bytes(b))
}
fn read_u32(r: &mut impl Read) -> Result<u32, SnapshotError> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b).map_err(SnapshotError::Io)?;
    Ok(u32::from_le_bytes(b))
}
fn read_u16(r: &mut impl Read) -> Result<u16, SnapshotError> {
    let mut b = [0u8; 2];
    r.read_exact(&mut b).map_err(SnapshotError::Io)?;
    Ok(u16::from_le_bytes(b))
}
fn read_blob(
    r: &mut impl Read,
    len: usize,
    max: usize,
    what: &str,
) -> Result<Bytes, SnapshotError> {
    if len > max {
        return Err(SnapshotError::Codec(format!(
            "{what} length {len} exceeds {max}"
        )));
    }
    let mut v = vec![0u8; len];
    r.read_exact(&mut v).map_err(SnapshotError::Io)?;
    Ok(Bytes::from(v))
}

impl SnapshotStateMachine for KvSm {
    type SnapshotHandle = Frozen;

    /// O(1): clone the persistent map (shared structure, no copy).
    fn freeze(&self) -> Result<(Frozen, u64), SnapshotError> {
        let h = Frozen {
            map: self.map.clone(),
            last_applied: self.last_applied,
            digest: self.digest,
        };
        Ok((h, self.last_applied.unwrap_or(0)))
    }

    /// Image layout v2 (`WIRE-FORMAT.md` § 5), all LE:
    /// `image_version u32 = 2 ‖ cursor u64 ‖ digest u64 ‖ count u64 ‖ entries…`,
    /// entry = `key_len u16 ‖ key ‖ version u64 ‖ shape u8 ‖ body`, body =
    /// value: `len u32 ‖ bytes`; list: `count u32 ‖ (len u32 ‖ bytes)…`.
    fn stream_snapshot(h: Frozen, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        let mut w = BufWriter::new(dst);
        let io = SnapshotError::Io;
        w.write_all(&IMAGE_VERSION.to_le_bytes()).map_err(io)?;
        w.write_all(&h.last_applied.unwrap_or(NO_CURSOR).to_le_bytes())
            .map_err(io)?;
        w.write_all(&h.digest.to_le_bytes()).map_err(io)?;
        w.write_all(&(h.map.len() as u64).to_le_bytes())
            .map_err(io)?;
        for (k, e) in h.map.iter() {
            w.write_all(&(k.len() as u16).to_le_bytes()).map_err(io)?;
            w.write_all(k).map_err(io)?;
            w.write_all(&e.version.to_le_bytes()).map_err(io)?;
            match &e.shape {
                Shape::Value(v) => {
                    w.write_all(&[SHAPE_VALUE]).map_err(io)?;
                    w.write_all(&(v.len() as u32).to_le_bytes()).map_err(io)?;
                    w.write_all(v).map_err(io)?;
                }
                Shape::List(items) => {
                    w.write_all(&[SHAPE_LIST]).map_err(io)?;
                    w.write_all(&(items.len() as u32).to_le_bytes())
                        .map_err(io)?;
                    for i in items {
                        w.write_all(&(i.len() as u32).to_le_bytes()).map_err(io)?;
                        w.write_all(i).map_err(io)?;
                    }
                }
            }
        }
        w.flush().map_err(io)
    }

    /// `position` is the exclusive frontier P; the cursor restored is the
    /// image's own, which must be strictly below P. Nothing in `self` changes
    /// unless the whole image decodes and its digest verifies.
    ///
    /// Reads image version 1 (v1: every entry a value, no shape byte) and 2.
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn Read,
    ) -> Result<u64, SnapshotError> {
        let mut r = BufReader::new(src);
        let image_version = read_u32(&mut r)?;
        if image_version != IMAGE_VERSION_V1 && image_version != IMAGE_VERSION_V2 {
            return Err(SnapshotError::Codec(format!(
                "unknown kv image version {image_version} (this binary reads {IMAGE_VERSION_V1} and {IMAGE_VERSION_V2})"
            )));
        }
        let cursor = read_u64(&mut r)?;
        let cursor = if cursor == NO_CURSOR {
            None
        } else {
            Some(cursor)
        };
        if let Some(c) = cursor
            && c >= position
        {
            return Err(SnapshotError::Codec(format!(
                "image cursor {c} is not below the artifact tag {position}: mis-tagged artifact"
            )));
        }
        let digest = read_u64(&mut r)?;
        let count = read_u64(&mut r)?;
        let mut map = BTreeMap::new();
        for i in 0..count {
            let kl = read_u16(&mut r)? as usize;
            if kl == 0 || kl > wire::MAX_KEY {
                return Err(SnapshotError::Codec(format!(
                    "entry {i}: key length {kl} out of range"
                )));
            }
            let key = read_blob(&mut r, kl, wire::MAX_KEY, "key")?;
            let version = read_u64(&mut r)?;
            let shape_byte = if image_version == IMAGE_VERSION_V1 {
                SHAPE_VALUE
            } else {
                let mut b = [0u8; 1];
                r.read_exact(&mut b).map_err(SnapshotError::Io)?;
                b[0]
            };
            let shape = match shape_byte {
                SHAPE_VALUE => {
                    let vl = read_u32(&mut r)? as usize;
                    Shape::Value(read_blob(&mut r, vl, wire::MAX_VALUE, "value")?)
                }
                SHAPE_LIST => {
                    let n = read_u32(&mut r)? as usize;
                    if n > wire::MAX_LIST_LEN {
                        return Err(SnapshotError::Codec(format!(
                            "entry {i}: list length {n} exceeds {}",
                            wire::MAX_LIST_LEN
                        )));
                    }
                    let mut items = Vec::new();
                    let mut total = 0usize;
                    for _ in 0..n {
                        let l = read_u32(&mut r)? as usize;
                        total += l;
                        if total > wire::MAX_LIST_BYTES {
                            return Err(SnapshotError::Codec(format!(
                                "entry {i}: list bytes exceed {}",
                                wire::MAX_LIST_BYTES
                            )));
                        }
                        items.push(read_blob(&mut r, l, wire::MAX_VALUE, "list element")?);
                    }
                    Shape::List(items)
                }
                other => {
                    return Err(SnapshotError::Codec(format!(
                        "entry {i}: unknown shape byte {other}"
                    )));
                }
            };
            if map.insert(key, Entry { version, shape }).is_some() {
                return Err(SnapshotError::Codec(format!("entry {i}: duplicate key")));
            }
        }
        if !r.fill_buf().map_err(SnapshotError::Io)?.is_empty() {
            return Err(SnapshotError::Codec(
                "trailing bytes after the last entry".into(),
            ));
        }
        let recomputed = Self::digest_of(&map);
        if recomputed != digest {
            return Err(SnapshotError::Codec(format!(
                "image digest {digest:#018x} != recomputed {recomputed:#018x}"
            )));
        }
        self.map = Arc::new(map);
        self.digest = digest;
        self.last_applied = cursor;
        Ok(position)
    }

    /// Diff replay projection (spec §5.8): canonical text, one entry per
    /// line, in key order — `OrdMap` iterates sorted, so this is canonical
    /// for free. Same fields the image carries, human-readable.
    fn project(&self, out: &mut dyn Write) -> Result<(), SnapshotError> {
        let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
        writeln!(out, "count={}", self.map.len())?;
        match self.last_applied {
            Some(c) => writeln!(out, "cursor={c}")?,
            None => writeln!(out, "cursor=none")?,
        }
        writeln!(out, "digest={:#018x}", self.digest)?;
        for (k, e) in self.map.iter() {
            match &e.shape {
                Shape::Value(v) => writeln!(
                    out,
                    "key={} version={} shape=value bytes={}",
                    hex(k),
                    e.version,
                    hex(v)
                )?,
                Shape::List(items) => {
                    let items: Vec<String> = items.iter().map(|i| hex(i)).collect();
                    writeln!(
                        out,
                        "key={} version={} shape=list items={}",
                        hex(k),
                        e.version,
                        items.join(",")
                    )?
                }
            }
        }
        Ok(())
    }
}
