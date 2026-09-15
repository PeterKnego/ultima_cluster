//! The KV v1 adapter, written from the builder's normative wire-format page
//! (`app/WIRE-FORMAT.md` in the builder's sandbox, v1 at sandbox commit
//! `268bd33`), NOT from the builder's `src/wire.rs` — the page is the
//! builder's published contract, and a disagreement between the two is a
//! finding, not something to paper over here.
//!
//! What the page had to state for this adapter to exist (the ticket asked
//! for this list): the op byte and layout of every command and query, the
//! status byte of every reply and the bytes after it per (request, status),
//! the meaning of "version" (a log position; 0 = absent for CAS), the key
//! and value bounds, the digest query and its reply, and the snapshot image
//! layout. It had all of them. What it did NOT need to state: anything
//! about the session envelope or the `Sessioned` tag (the gateway strips
//! both), or the remote protocol itself.
//!
//! Layer 3 of that page, all integers little-endian:
//!
//! ```text
//! command: format=1 ‖ op ‖ key_len:u16 ‖ key ‖ …    op 1 PUT, 2 DELETE, 3 CAS
//!   PUT    … = value
//!   DELETE … = nothing
//!   CAS    … = expected_version:u64 ‖ value
//! query:   format=1 ‖ op ‖ …                        op 1 GET (key_len ‖ key), 2 DIGEST
//! reply:   status ‖ …   0 OK, 1 NOT_FOUND, 2 VERSION_MISMATCH, 3 BAD_REQUEST(reason)
//!   PUT/CAS/DELETE OK   … = version:u64
//!   CAS MISMATCH        … = current:u64
//!   GET OK              … = version:u64 ‖ value
//!   DIGEST OK           … = count:u64 ‖ digest:u64 ‖ last_applied:u64
//! ```

use super::adapter::{Adapter, Caps, Decoded, Digest, Image, KvOp, KvResp};

pub struct KvV1;

const FORMAT: u8 = 1;
const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;
const OP_CAS: u8 = 3;
const Q_GET: u8 = 1;
const Q_DIGEST: u8 = 2;
const ST_OK: u8 = 0;
const ST_NOT_FOUND: u8 = 1;
const ST_VERSION_MISMATCH: u8 = 2;
const ST_BAD_REQUEST: u8 = 3;

/// The page's bounds. The harness never generates a key or value outside
/// them; a service refusing an in-bound frame is a finding.
pub const MAX_KEY: usize = 256;
pub const MAX_VALUE: usize = 1024;

fn prefix(op: u8, key: &[u8]) -> Vec<u8> {
    assert!(
        !key.is_empty() && key.len() <= MAX_KEY,
        "key length {} out of 1..=256",
        key.len()
    );
    let mut v = Vec::with_capacity(4 + key.len() + 8 + 8);
    v.push(FORMAT);
    v.push(op);
    v.extend_from_slice(&(key.len() as u16).to_le_bytes());
    v.extend_from_slice(key);
    v
}

fn u64_at(b: &[u8], off: usize) -> Result<u64, String> {
    b.get(off..off + 8)
        .map(|s| u64::from_le_bytes(s.try_into().unwrap()))
        .ok_or_else(|| format!("reply truncated: need {} bytes, have {}", off + 8, b.len()))
}

/// The harness's values are u64s on the wire as 8 LE bytes.
pub fn value_bytes(v: u64) -> [u8; 8] {
    v.to_le_bytes()
}

fn value_from(b: &[u8]) -> Result<u64, String> {
    if b.len() != 8 {
        return Err(format!("value is {} bytes, the harness wrote 8", b.len()));
    }
    Ok(u64::from_le_bytes(b.try_into().unwrap()))
}

impl Adapter for KvV1 {
    fn name(&self) -> &'static str {
        "kv-v1"
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
            append: false,
        }
    }
    fn service_args(&self) -> Vec<String> {
        Vec::new()
    }

    fn encode(&self, key: &[u8], op: &KvOp, cas_version: u64) -> Vec<u8> {
        match op {
            KvOp::Put(v) => {
                let mut f = prefix(OP_PUT, key);
                f.extend_from_slice(&value_bytes(*v));
                f
            }
            KvOp::Delete => prefix(OP_DELETE, key),
            KvOp::Cas { new, .. } => {
                let mut f = prefix(OP_CAS, key);
                f.extend_from_slice(&cas_version.to_le_bytes());
                f.extend_from_slice(&value_bytes(*new));
                f
            }
            KvOp::Get => {
                let mut f = Vec::with_capacity(4 + key.len());
                f.push(FORMAT);
                f.push(Q_GET);
                f.extend_from_slice(&(key.len() as u16).to_le_bytes());
                f.extend_from_slice(key);
                f
            }
        }
    }

    fn decode(&self, op: &KvOp, b: &[u8]) -> Result<Decoded, String> {
        let status = *b.first().ok_or("empty reply")?;
        if status == ST_BAD_REQUEST {
            return Err(format!("BAD_REQUEST reason {:?} for {op:?}", b.get(1)));
        }
        match (op, status) {
            (KvOp::Put(_), ST_OK) => Ok(Decoded {
                resp: KvResp::Ack,
                version: Some(u64_at(b, 1)?),
            }),
            (KvOp::Delete, ST_OK) => Ok(Decoded {
                resp: KvResp::Deleted(true),
                version: Some(u64_at(b, 1)?),
            }),
            (KvOp::Delete, ST_NOT_FOUND) => Ok(Decoded {
                resp: KvResp::Deleted(false),
                version: None,
            }),
            (KvOp::Cas { .. }, ST_OK) => Ok(Decoded {
                resp: KvResp::CasOk(true),
                version: Some(u64_at(b, 1)?),
            }),
            (KvOp::Cas { .. }, ST_VERSION_MISMATCH) => Ok(Decoded {
                resp: KvResp::CasOk(false),
                version: Some(u64_at(b, 1)?),
            }),
            (KvOp::Get, ST_OK) => Ok(Decoded {
                resp: KvResp::Value(Some(value_from(&b[9.min(b.len())..])?)),
                version: Some(u64_at(b, 1)?),
            }),
            (KvOp::Get, ST_NOT_FOUND) => Ok(Decoded {
                resp: KvResp::Value(None),
                version: None,
            }),
            _ => Err(format!(
                "status {status} is not a documented reply to {op:?}"
            )),
        }
    }

    fn encode_put_bytes(&self, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
        assert!(value.len() <= MAX_VALUE);
        let mut f = prefix(OP_PUT, key);
        f.extend_from_slice(value);
        Some(f)
    }
    fn encode_digest(&self) -> Option<Vec<u8>> {
        Some(vec![FORMAT, Q_DIGEST])
    }
    fn decode_digest(&self, b: &[u8]) -> Result<Digest, String> {
        if b.first() != Some(&ST_OK) {
            return Err(format!("digest reply status {:?}", b.first()));
        }
        Ok(Digest {
            count: u64_at(b, 1)?,
            digest: u64_at(b, 9)?,
            last_applied: u64_at(b, 17)?,
        })
    }

    fn encode_append(&self, _key: &[u8], _val: u64) -> Option<Vec<u8>> {
        None // v1 has no Append; the v2 adapter will.
    }
    fn encode_list_read(&self, _key: &[u8]) -> Option<Vec<u8>> {
        None
    }
    fn decode_list(&self, _b: &[u8]) -> Result<Vec<u64>, String> {
        Err("kv-v1 has no list kind".into())
    }

    /// Page § 5: `image_version:u32=1 ‖ cursor:u64 ‖ digest:u64 ‖ count:u64 ‖
    /// count × (key_len:u16 ‖ key ‖ version:u64 ‖ value_len:u32 ‖ value)`.
    fn parse_snapshot_image(&self, b: &[u8]) -> Result<Image, String> {
        let need = |off: usize, n: usize| -> Result<&[u8], String> {
            b.get(off..off + n)
                .ok_or_else(|| format!("image truncated at {off}+{n} of {}", b.len()))
        };
        let iv = u32::from_le_bytes(need(0, 4)?.try_into().unwrap());
        if iv != 1 {
            return Err(format!("image_version {iv}, this adapter reads 1"));
        }
        let count = u64_at(b, 20)?;
        let mut off = 28;
        let mut out = Image::new();
        for _ in 0..count {
            let kl = u16::from_le_bytes(need(off, 2)?.try_into().unwrap()) as usize;
            off += 2;
            let key = need(off, kl)?.to_vec();
            off += kl;
            let version = u64_at(b, off)?;
            off += 8;
            let vl = u32::from_le_bytes(need(off, 4)?.try_into().unwrap()) as usize;
            off += 4;
            let value = need(off, vl)?.to_vec();
            off += vl;
            if out.insert(key, (version, value)).is_some() {
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
    fn put_layout_matches_the_page() {
        // format=1, op=1, key_len=2 LE, "ab", value (8 LE bytes of 0x0102030405060708)
        let f = KvV1.encode(b"ab", &KvOp::Put(0x0102030405060708), 0);
        assert_eq!(f, vec![1, 1, 2, 0, b'a', b'b', 8, 7, 6, 5, 4, 3, 2, 1]);
    }

    #[test]
    fn delete_and_cas_layouts() {
        assert_eq!(KvV1.encode(b"k", &KvOp::Delete, 0), vec![1, 2, 1, 0, b'k']);
        let f = KvV1.encode(b"k", &KvOp::Cas { old: None, new: 9 }, 0);
        // expected_version 0 = must be absent, then the 8-byte value 9
        assert_eq!(
            f,
            vec![
                1, 3, 1, 0, b'k', 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0
            ]
        );
        let f = KvV1.encode(
            b"k",
            &KvOp::Cas {
                old: Some(1),
                new: 9,
            },
            0x80,
        );
        assert_eq!(&f[5..13], &[0x80, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn query_layouts() {
        assert_eq!(
            KvV1.encode(b"key", &KvOp::Get, 0),
            vec![1, 1, 3, 0, b'k', b'e', b'y']
        );
        assert_eq!(KvV1.encode_digest(), Some(vec![1, 2]));
    }

    #[test]
    fn reply_decoding_per_status_table() {
        let ok128 = {
            let mut v = vec![0u8];
            v.extend_from_slice(&128u64.to_le_bytes());
            v
        };
        assert_eq!(
            KvV1.decode(&KvOp::Put(1), &ok128).unwrap(),
            Decoded {
                resp: KvResp::Ack,
                version: Some(128)
            }
        );
        assert_eq!(
            KvV1.decode(&KvOp::Delete, &ok128).unwrap(),
            Decoded {
                resp: KvResp::Deleted(true),
                version: Some(128)
            }
        );
        assert_eq!(
            KvV1.decode(&KvOp::Delete, &[1]).unwrap(),
            Decoded {
                resp: KvResp::Deleted(false),
                version: None
            }
        );
        assert_eq!(
            KvV1.decode(&KvOp::Cas { old: None, new: 1 }, &ok128)
                .unwrap(),
            Decoded {
                resp: KvResp::CasOk(true),
                version: Some(128)
            }
        );
        let mut mm = vec![2u8];
        mm.extend_from_slice(&192u64.to_le_bytes());
        assert_eq!(
            KvV1.decode(&KvOp::Cas { old: None, new: 1 }, &mm).unwrap(),
            Decoded {
                resp: KvResp::CasOk(false),
                version: Some(192)
            }
        );
        let mut get = vec![0u8];
        get.extend_from_slice(&128u64.to_le_bytes());
        get.extend_from_slice(&77u64.to_le_bytes());
        assert_eq!(
            KvV1.decode(&KvOp::Get, &get).unwrap(),
            Decoded {
                resp: KvResp::Value(Some(77)),
                version: Some(128)
            }
        );
        assert_eq!(
            KvV1.decode(&KvOp::Get, &[1]).unwrap(),
            Decoded {
                resp: KvResp::Value(None),
                version: None
            }
        );
        assert!(
            KvV1.decode(&KvOp::Get, &[3, 4])
                .unwrap_err()
                .contains("BAD_REQUEST")
        );
        assert!(
            KvV1.decode(&KvOp::Put(1), &[1]).is_err(),
            "NOT_FOUND is not a PUT reply"
        );
    }

    #[test]
    fn digest_reply() {
        let mut d = vec![0u8];
        for x in [3u64, 0xdead, 384] {
            d.extend_from_slice(&x.to_le_bytes());
        }
        assert_eq!(
            KvV1.decode_digest(&d).unwrap(),
            Digest {
                count: 3,
                digest: 0xdead,
                last_applied: 384
            }
        );
    }

    #[test]
    fn snapshot_image_roundtrip_and_refusals() {
        let mut img = Vec::new();
        img.extend_from_slice(&1u32.to_le_bytes());
        img.extend_from_slice(&500u64.to_le_bytes()); // cursor
        img.extend_from_slice(&7u64.to_le_bytes()); // digest (not recomputed here)
        img.extend_from_slice(&2u64.to_le_bytes()); // count
        for (k, ver, v) in [(&b"a"[..], 128u64, &b"x"[..]), (b"b", 256, b"yz")] {
            img.extend_from_slice(&(k.len() as u16).to_le_bytes());
            img.extend_from_slice(k);
            img.extend_from_slice(&ver.to_le_bytes());
            img.extend_from_slice(&(v.len() as u32).to_le_bytes());
            img.extend_from_slice(v);
        }
        let m = KvV1.parse_snapshot_image(&img).unwrap();
        assert_eq!(m.get(&b"a"[..]).unwrap(), &(128, b"x".to_vec()));
        assert_eq!(m.get(&b"b"[..]).unwrap(), &(256, b"yz".to_vec()));
        let mut trailing = img.clone();
        trailing.push(0);
        assert!(
            KvV1.parse_snapshot_image(&trailing)
                .unwrap_err()
                .contains("trailing")
        );
        let mut v2 = img.clone();
        v2[0] = 2;
        assert!(
            KvV1.parse_snapshot_image(&v2)
                .unwrap_err()
                .contains("image_version")
        );
    }
}
