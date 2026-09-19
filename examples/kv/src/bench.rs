//! Bench-only twins of `KvSm`: the same PUT path over `std` maps.
//!
//! `KvSm` keeps its state in an `Arc<BTreeMap>` so that `SnapshotStateMachine::
//! freeze` is an O(1) `Arc` clone on the apply thread; the O(n) copy is paid
//! by the FIRST write after a freeze (`Arc::make_mut`), once per instant.
//! These twins run the identical PUT path — decode, shape check, incremental
//! digest, `Bytes` copies of key and value, insert — over a plain
//! `std::collections::BTreeMap` and `HashMap`, whose `freeze` is a
//! stop-the-world clone. The difference between an arm here and `KvSm` is
//! therefore WHERE the copy lands (freeze vs first post-freeze write) and
//! what the map itself costs, on both sides: per write (`uc_node`'s
//! `apply_bench --sm`) and per snapshot (this crate's `snap_bench` example). Only PUT is implemented:
//! the drivers send nothing else. Not used by `kv-service`.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufReader, Read, Write};

use bytes::Bytes;
use uc_service::{ApplyCtx, RawStateMachine, SnapshotError, SnapshotStateMachine};

use crate::wire::{self, Command};
use crate::{Entry, Shape, entry_hash, read_blob, read_u16, read_u32, read_u64, write_image};

/// The map under [`KvStd`]: what the PUT path and the snapshot path need.
pub trait KvMap: Default + Clone + Send + 'static {
    fn get(&self, key: &[u8]) -> Option<&Entry>;
    fn insert(&mut self, key: Bytes, e: Entry) -> Option<Entry>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn iter<'a>(&'a self) -> Box<dyn ExactSizeIterator<Item = (&'a Bytes, &'a Entry)> + 'a>;
}
impl KvMap for BTreeMap<Bytes, Entry> {
    fn get(&self, key: &[u8]) -> Option<&Entry> {
        BTreeMap::get(self, key)
    }
    fn insert(&mut self, key: Bytes, e: Entry) -> Option<Entry> {
        BTreeMap::insert(self, key, e)
    }
    fn len(&self) -> usize {
        BTreeMap::len(self)
    }
    fn iter<'a>(&'a self) -> Box<dyn ExactSizeIterator<Item = (&'a Bytes, &'a Entry)> + 'a> {
        Box::new(BTreeMap::iter(self))
    }
}
impl KvMap for HashMap<Bytes, Entry> {
    fn get(&self, key: &[u8]) -> Option<&Entry> {
        HashMap::get(self, key)
    }
    fn insert(&mut self, key: Bytes, e: Entry) -> Option<Entry> {
        HashMap::insert(self, key, e)
    }
    fn len(&self) -> usize {
        HashMap::len(self)
    }
    fn iter<'a>(&'a self) -> Box<dyn ExactSizeIterator<Item = (&'a Bytes, &'a Entry)> + 'a> {
        Box::new(HashMap::iter(self))
    }
}

/// `KvSm`'s PUT path over a `std` map. See the module doc.
#[derive(Default)]
pub struct KvStd<M: KvMap> {
    map: M,
    last: Option<u64>,
    digest: u64,
}

pub type KvBTree = KvStd<BTreeMap<Bytes, Entry>>;
pub type KvHash = KvStd<HashMap<Bytes, Entry>>;

impl<M: KvMap> KvStd<M> {
    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }
    pub fn digest(&self) -> u64 {
        self.digest
    }
    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.map.get(key)
    }
}

impl<M: KvMap> RawStateMachine for KvStd<M> {
    /// Shared by BOTH instantiations on purpose: `FsmIdentity` is a function
    /// of `NAME` + `VERSION`, not of the Rust type, so `KvBTree` and `KvHash`
    /// are identity-indistinguishable and must never be attached as two rows
    /// of one cluster. The benches wrap them in `TaggedRaw`, which overrides
    /// the name per row, or drive them standalone.
    const NAME: &'static str = "kv-std";

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        let position = ctx.position;
        match wire::decode_command(cmd) {
            Ok(Command::Put { key, value }) => match self.map.get(key) {
                Some(Entry {
                    shape: Shape::List(_),
                    ..
                }) => wire::put_status(out, wire::ST_WRONG_SHAPE),
                _ => {
                    let entry = Entry {
                        version: position,
                        shape: Shape::Value(Bytes::copy_from_slice(value)),
                    };
                    let new_hash = entry_hash(key, &entry);
                    if let Some(old) = self.map.insert(Bytes::copy_from_slice(key), entry) {
                        self.digest ^= entry_hash(key, &old);
                    }
                    self.digest ^= new_hash;
                    wire::put_status_u64(out, wire::ST_OK, position);
                }
            },
            Ok(_) => wire::put_bad_request(out, wire::BAD_UNKNOWN_OP),
            Err(reason) => wire::put_bad_request(out, reason),
        }
        self.last = Some(position);
    }
    /// The same `DIGEST` answer `KvSm` gives (`ST_OK ‖ len ‖ digest ‖
    /// last_applied`), so a driver can content-check either type alike.
    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        match wire::decode_query(q) {
            Ok(wire::Query::Digest) => {
                out.push(wire::ST_OK);
                out.extend_from_slice(&(self.map.len() as u64).to_le_bytes());
                out.extend_from_slice(&self.digest.to_le_bytes());
                out.extend_from_slice(&self.last.unwrap_or(0).to_le_bytes());
            }
            Ok(_) => wire::put_bad_request(out, wire::BAD_UNKNOWN_OP),
            Err(reason) => wire::put_bad_request(out, reason),
        }
    }
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}

/// The stop-the-world handle: a full clone of the map, taken inside `freeze`
/// on the apply thread. `KvSm` pays the same O(n) copy, but on the first
/// write after the freeze instead of inside it.
pub struct FrozenStd<M> {
    map: M,
    last: Option<u64>,
    digest: u64,
}

impl<M: KvMap> SnapshotStateMachine for KvStd<M> {
    type SnapshotHandle = FrozenStd<M>;

    fn freeze(&self) -> Result<(FrozenStd<M>, u64), SnapshotError> {
        let h = FrozenStd {
            map: self.map.clone(),
            last: self.last,
            digest: self.digest,
        };
        Ok((h, self.last.unwrap_or(0)))
    }

    fn stream_snapshot(h: FrozenStd<M>, dst: &mut dyn Write) -> Result<(), SnapshotError> {
        write_image(dst, h.last, h.digest, h.map.iter())
    }

    /// Reads the v2 image only, values only — the shapes this twin writes.
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn Read,
    ) -> Result<u64, SnapshotError> {
        let mut r = BufReader::new(src);
        let image_version = read_u32(&mut r)?;
        if image_version != 2 {
            return Err(SnapshotError::Codec(format!(
                "bench twin reads image version 2 only, got {image_version}"
            )));
        }
        let cursor = read_u64(&mut r)?;
        let cursor = (cursor != u64::MAX).then_some(cursor);
        if let Some(c) = cursor
            && c >= position
        {
            return Err(SnapshotError::Codec(format!(
                "image cursor {c} is not below the artifact tag {position}"
            )));
        }
        let digest = read_u64(&mut r)?;
        let count = read_u64(&mut r)?;
        let mut map = M::default();
        for i in 0..count {
            let kl = read_u16(&mut r)? as usize;
            if kl == 0 || kl > wire::MAX_KEY {
                return Err(SnapshotError::Codec(format!(
                    "entry {i}: key length {kl} out of range"
                )));
            }
            let key = read_blob(&mut r, kl, wire::MAX_KEY, "key")?;
            let version = read_u64(&mut r)?;
            let mut shape = [0u8; 1];
            r.read_exact(&mut shape).map_err(SnapshotError::Io)?;
            if shape[0] != 0 {
                return Err(SnapshotError::Codec(format!(
                    "entry {i}: bench twin reads value entries only"
                )));
            }
            let vl = read_u32(&mut r)? as usize;
            let value = read_blob(&mut r, vl, wire::MAX_VALUE, "value")?;
            let dup = map
                .insert(
                    key,
                    Entry {
                        version,
                        shape: Shape::Value(value),
                    },
                )
                .is_some();
            if dup {
                return Err(SnapshotError::Codec(format!("entry {i}: duplicate key")));
            }
        }
        let mut b = [0u8; 1];
        if r.read(&mut b).map_err(SnapshotError::Io)? != 0 {
            return Err(SnapshotError::Codec(
                "trailing bytes after the image".into(),
            ));
        }
        let computed = map.iter().fold(0u64, |d, (k, e)| d ^ entry_hash(k, e));
        if computed != digest {
            return Err(SnapshotError::Codec(format!(
                "image digest {digest:#x} does not match the entries ({computed:#x})"
            )));
        }
        self.map = map;
        self.last = cursor;
        self.digest = digest;
        Ok(position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KvSm;
    use uc_service::ApplyCtx;

    fn put_all<S: RawStateMachine>(sm: &mut S, n: u64) -> u64 {
        let mut pos = 1u64;
        let mut out = Vec::new();
        for k in 0..n {
            let frame = wire::encode_put(&k.to_le_bytes(), &[k as u8; 16]);
            out.clear();
            sm.apply(&mut ApplyCtx::for_sm::<S>(pos), &frame, &mut out);
            assert_eq!(out[0], wire::ST_OK);
            pos += 96;
        }
        pos
    }

    fn image<S: SnapshotStateMachine>(sm: &S) -> Vec<u8> {
        let (h, _) = sm.freeze().unwrap();
        let mut v = Vec::new();
        S::stream_snapshot(h, &mut v).unwrap();
        v
    }

    /// The ordered twin and `KvSm` (also a `BTreeMap`) write byte-identical
    /// images for the same state, which is what lets a fleet mix arms.
    #[test]
    fn ordered_twin_image_is_byte_identical_to_kvsm() {
        let (mut a, mut b) = (KvSm::default(), KvBTree::default());
        put_all(&mut a, 500);
        put_all(&mut b, 500);
        assert_eq!(a.digest(), b.digest());
        assert_eq!(image(&a), image(&b));
    }

    /// The hash twin's image holds the same entries in its own order: not
    /// byte-identical, but it installs into every other arm with the same
    /// digest, length and lookups, and theirs install into it.
    #[test]
    fn hash_twin_image_installs_everywhere_and_vice_versa() {
        let (mut sm, mut hb) = (KvSm::default(), KvHash::default());
        let end = put_all(&mut sm, 500);
        put_all(&mut hb, 500);
        assert_eq!(sm.digest(), hb.digest());
        let (img_sm, img_h) = (image(&sm), image(&hb));
        assert_ne!(img_sm, img_h, "a hash map's order is not sorted order");

        let mut into_sm = KvSm::default();
        assert_eq!(into_sm.install_snapshot(end, &mut &img_h[..]).unwrap(), end);
        let mut into_bt = KvBTree::default();
        assert_eq!(into_bt.install_snapshot(end, &mut &img_h[..]).unwrap(), end);
        let mut into_h = KvHash::default();
        assert_eq!(into_h.install_snapshot(end, &mut &img_sm[..]).unwrap(), end);
        for k in [0u64, 7, 499] {
            let key = k.to_le_bytes();
            let want = sm.get(&key).unwrap();
            assert_eq!(into_sm.get(&key).unwrap(), want);
            assert_eq!(into_bt.get(&key).unwrap(), want);
            assert_eq!(into_h.get(&key).unwrap(), want);
        }
        assert_eq!(
            (into_sm.len(), into_bt.len(), into_h.len()),
            (500, 500, 500)
        );
        assert_eq!(into_h.digest(), sm.digest());
    }

    /// The twin refuses what `KvSm` refuses: a zero-length key.
    #[test]
    fn twin_install_refuses_a_zero_length_key() {
        let mut hb = KvHash::default();
        put_all(&mut hb, 3);
        let mut img = image(&hb);
        // header = version u32 + cursor u64 + digest u64 + count u64 = 28 B;
        // the first entry's key_len u16 follows.
        img[28] = 0;
        img[29] = 0;
        let mut fresh = KvHash::default();
        let err = fresh
            .install_snapshot(1_000_000, &mut &img[..])
            .unwrap_err();
        assert!(matches!(err, SnapshotError::Codec(m) if m.contains("key length 0")));
        assert_eq!(fresh.len(), 0, "nothing lands on a refused image");
    }
}
