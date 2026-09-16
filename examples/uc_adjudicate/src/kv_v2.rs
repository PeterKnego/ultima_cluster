//! The KV v2 adapter, written from the builder's v2 `WIRE-FORMAT.md` (sandbox
//! `~/ultima/kv_store`, v2 at `Cargo` version `2.0.0`). v2 keeps every v1
//! byte and adds two list operations, so the value ops (`Put`/`Get`/`Delete`/
//! `CAS`), the digest and the size limits are delegated verbatim to
//! [`crate::kv_v1::KvV1`]; this adapter adds `Append`, `List`, and the v2
//! snapshot image (a shape byte per entry, and it still reads a v1 image).
//!
//! **A gap this adapter surfaced:** the v2 page's § 3.1 command table lists
//! only PUT/DELETE/CAS — the **APPEND row (op 4) is missing**. Its op byte
//! and framing ("same as PUT with op=4") appear only in § 3.3's reply table
//! and the prose, so a client author working from § 3.1 alone could not send
//! an Append. Recorded as a B1-v2 finding; the op byte used here (4) is the
//! one the reply table and the builder's `wire.rs` agree on.

use super::adapter::{Adapter, Caps, Decoded, Digest, Image, KvOp};
use super::kv_v1::{KvV1, MAX_KEY};

pub struct KvV2;

const FORMAT: u8 = 1;
const OP_APPEND: u8 = 4;
const Q_LIST: u8 = 3;
const ST_OK: u8 = 0;
const ST_NOT_FOUND: u8 = 1;
const ST_BAD_REQUEST: u8 = 3;
const ST_WRONG_SHAPE: u8 = 4;
const ST_LIST_FULL: u8 = 5;

fn u64_le(b: &[u8], off: usize) -> Result<u64, String> {
    b.get(off..off + 8)
        .map(|s| u64::from_le_bytes(s.try_into().unwrap()))
        .ok_or_else(|| format!("reply truncated at u64 {off} of {}", b.len()))
}
fn u32_le(b: &[u8], off: usize) -> Result<u32, String> {
    b.get(off..off + 4)
        .map(|s| u32::from_le_bytes(s.try_into().unwrap()))
        .ok_or_else(|| format!("reply truncated at u32 {off} of {}", b.len()))
}

impl Adapter for KvV2 {
    fn name(&self) -> &'static str {
        "kv-v2"
    }
    fn fsm_name(&self) -> &'static str {
        "kv"
    }
    fn sessioned(&self) -> bool {
        true
    }
    fn caps(&self) -> Caps {
        Caps {
            keys: true,
            delete: true,
            cas_absent: true,
            digest: true,
            snapshots: true,
            append: true,
        }
    }
    fn service_args(&self) -> Vec<String> {
        Vec::new()
    }

    // Value ops (Put/Get/Delete/CAS) are byte-identical to v1.
    fn encode(&self, key: &[u8], op: &KvOp, cas_version: u64) -> Vec<u8> {
        KvV1.encode(key, op, cas_version)
    }
    fn decode(&self, op: &KvOp, bytes: &[u8]) -> Result<Decoded, String> {
        // A WRONG_SHAPE (4) on a value op would mean the WGL workload hit a
        // list key — it never does (value keys and list keys are disjoint),
        // so surface it as an error rather than mis-decode.
        if bytes.first() == Some(&ST_WRONG_SHAPE) {
            return Err(format!(
                "WRONG_SHAPE on value op {op:?} (a value/list key collision)"
            ));
        }
        KvV1.decode(op, bytes)
    }
    fn encode_digest(&self) -> Option<Vec<u8>> {
        KvV1.encode_digest()
    }
    fn decode_digest(&self, bytes: &[u8]) -> Result<Digest, String> {
        KvV1.decode_digest(bytes)
    }
    fn encode_put_bytes(&self, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
        KvV1.encode_put_bytes(key, value)
    }

    // v2 list ops.
    fn encode_append(&self, key: &[u8], val: u64) -> Option<Vec<u8>> {
        assert!(!key.is_empty() && key.len() <= MAX_KEY);
        let mut f = Vec::with_capacity(4 + key.len() + 8);
        f.push(FORMAT);
        f.push(OP_APPEND);
        f.extend_from_slice(&(key.len() as u16).to_le_bytes());
        f.extend_from_slice(key);
        f.extend_from_slice(&val.to_le_bytes()); // an 8-byte element
        Some(f)
    }
    fn encode_list_read(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut f = Vec::with_capacity(4 + key.len());
        f.push(FORMAT);
        f.push(Q_LIST);
        f.extend_from_slice(&(key.len() as u16).to_le_bytes());
        f.extend_from_slice(key);
        Some(f)
    }
    fn decode_list(&self, b: &[u8]) -> Result<Vec<u64>, String> {
        match b.first().copied() {
            Some(ST_OK) => {
                // version:u64 ‖ count:u32 ‖ count×(len:u32 ‖ bytes)
                let count = u32_le(b, 9)? as usize;
                let mut off = 13;
                let mut out = Vec::with_capacity(count);
                for i in 0..count {
                    let len = u32_le(b, off)? as usize;
                    off += 4;
                    let elem = b
                        .get(off..off + len)
                        .ok_or_else(|| format!("list elem {i} truncated"))?;
                    off += len;
                    // the Elle workload appends 8-byte u64 elements
                    if len != 8 {
                        return Err(format!("list elem {i} is {len} B, the workload appends 8"));
                    }
                    out.push(u64::from_le_bytes(elem.try_into().unwrap()));
                }
                if off != b.len() {
                    return Err(format!(
                        "{} trailing bytes after {count} list elems",
                        b.len() - off
                    ));
                }
                Ok(out)
            }
            Some(ST_NOT_FOUND) => Ok(Vec::new()),
            Some(ST_WRONG_SHAPE) => Err("WRONG_SHAPE on a list read (a value key)".into()),
            Some(ST_LIST_FULL) => Err("LIST_FULL on a read (impossible)".into()),
            Some(ST_BAD_REQUEST) => {
                Err(format!("BAD_REQUEST reason {:?} on a list read", b.get(1)))
            }
            other => Err(format!("list read status {other:?}")),
        }
    }

    /// v2 image: `image_version:u32 ‖ cursor:u64 ‖ digest:u64 ‖ count:u64 ‖
    /// count × (key_len:u16 ‖ key ‖ version:u64 ‖ shape:u8 ‖ body)`, shape 0
    /// value (`value_len:u32 ‖ value`), shape 1 list (`n:u32 ‖ n×(elem_len:u32
    /// ‖ elem)`). image_version 1 is the v1 image (no shape byte, all values)
    /// — a v2 binary reads both, so the adapter does too. A list entry's body
    /// is canonicalised (length-prefixed concat) so the divergence diff can
    /// compare it for equality like a value.
    fn parse_snapshot_image(&self, b: &[u8]) -> Result<Image, String> {
        let iv = u32_le(b, 0)?;
        if iv != 1 && iv != 2 {
            return Err(format!("image_version {iv}, this adapter reads 1 and 2"));
        }
        let has_shape = iv == 2;
        let count = u64_le(b, 20)?;
        let mut off = 28usize;
        let need = |b: &[u8], off: usize, n: usize| -> Result<(), String> {
            if off + n > b.len() {
                Err(format!("image truncated at {off}+{n} of {}", b.len()))
            } else {
                Ok(())
            }
        };
        let mut out = Image::new();
        for _ in 0..count {
            need(b, off, 2)?;
            let kl = u16::from_le_bytes(b[off..off + 2].try_into().unwrap()) as usize;
            off += 2;
            need(b, off, kl)?;
            let key = b[off..off + kl].to_vec();
            off += kl;
            let version = u64_le(b, off)?;
            off += 8;
            let shape = if has_shape {
                let s = *b.get(off).ok_or("image truncated at shape byte")?;
                off += 1;
                s
            } else {
                0 // v1: everything is a value
            };
            let body = match shape {
                0 => {
                    let vl = u32_le(b, off)? as usize;
                    off += 4;
                    need(b, off, vl)?;
                    let v = b[off..off + vl].to_vec();
                    off += vl;
                    // value body: tag 'V' then bytes
                    let mut c = vec![b'V'];
                    c.extend_from_slice(&v);
                    c
                }
                1 => {
                    let n = u32_le(b, off)? as usize;
                    off += 4;
                    // list body: tag 'L' then n then each length-prefixed elem
                    let mut c = vec![b'L'];
                    c.extend_from_slice(&(n as u32).to_le_bytes());
                    for _ in 0..n {
                        let el = u32_le(b, off)? as usize;
                        off += 4;
                        need(b, off, el)?;
                        c.extend_from_slice(&(el as u32).to_le_bytes());
                        c.extend_from_slice(&b[off..off + el]);
                        off += el;
                    }
                    c
                }
                s => return Err(format!("unknown shape byte {s}")),
            };
            if out.insert(key, (version, body)).is_some() {
                return Err("duplicate key in image".into());
            }
        }
        if off != b.len() {
            return Err(format!(
                "{} trailing bytes after {count} entries",
                b.len() - off
            ));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_list_layout() {
        let a = KvV2.encode_append(b"ev", 0x0102030405060708).unwrap();
        // format=1 op=4 key_len=2 "ev" then 8 LE value bytes
        assert_eq!(a, vec![1, 4, 2, 0, b'e', b'v', 8, 7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(
            KvV2.encode_list_read(b"ev").unwrap(),
            vec![1, 3, 2, 0, b'e', b'v']
        );
    }

    #[test]
    fn decode_list_reply() {
        // status OK, version(8), count=2, [len=8, 1u64][len=8, 2u64]
        let mut r = vec![0u8];
        r.extend_from_slice(&99u64.to_le_bytes());
        r.extend_from_slice(&2u32.to_le_bytes());
        for v in [1u64, 2u64] {
            r.extend_from_slice(&8u32.to_le_bytes());
            r.extend_from_slice(&v.to_le_bytes());
        }
        assert_eq!(KvV2.decode_list(&r).unwrap(), vec![1, 2]);
        assert_eq!(KvV2.decode_list(&[1]).unwrap(), Vec::<u64>::new()); // NOT_FOUND -> empty
        assert!(KvV2.decode_list(&[4]).unwrap_err().contains("WRONG_SHAPE"));
    }

    #[test]
    fn parse_v1_and_v2_images() {
        // v1 image: version=1, cursor, digest, count=1, (key "a" v=8 value "x")
        let mut v1 = Vec::new();
        v1.extend_from_slice(&1u32.to_le_bytes());
        v1.extend_from_slice(&5u64.to_le_bytes());
        v1.extend_from_slice(&0u64.to_le_bytes());
        v1.extend_from_slice(&1u64.to_le_bytes());
        v1.extend_from_slice(&1u16.to_le_bytes());
        v1.push(b'a');
        v1.extend_from_slice(&8u64.to_le_bytes());
        v1.extend_from_slice(&1u32.to_le_bytes());
        v1.push(b'x');
        let m1 = KvV2.parse_snapshot_image(&v1).unwrap();
        assert_eq!(m1.get(&b"a"[..]).unwrap(), &(8, vec![b'V', b'x']));
        // v2 image: version=2, one value + one list
        let mut v2 = Vec::new();
        v2.extend_from_slice(&2u32.to_le_bytes());
        v2.extend_from_slice(&5u64.to_le_bytes());
        v2.extend_from_slice(&0u64.to_le_bytes());
        v2.extend_from_slice(&2u64.to_le_bytes());
        // value key "a"
        v2.extend_from_slice(&1u16.to_le_bytes());
        v2.push(b'a');
        v2.extend_from_slice(&8u64.to_le_bytes());
        v2.push(0); // shape value
        v2.extend_from_slice(&1u32.to_le_bytes());
        v2.push(b'x');
        // list key "b" with 2 elements
        v2.extend_from_slice(&1u16.to_le_bytes());
        v2.push(b'b');
        v2.extend_from_slice(&16u64.to_le_bytes());
        v2.push(1); // shape list
        v2.extend_from_slice(&2u32.to_le_bytes());
        for e in [b"hi".as_ref(), b"yo".as_ref()] {
            v2.extend_from_slice(&(e.len() as u32).to_le_bytes());
            v2.extend_from_slice(e);
        }
        let m2 = KvV2.parse_snapshot_image(&v2).unwrap();
        assert_eq!(m2.get(&b"a"[..]).unwrap().0, 8);
        assert_eq!(m2.get(&b"b"[..]).unwrap().0, 16);
        assert!(m2.get(&b"b"[..]).unwrap().1.starts_with(b"L"));
    }
}
