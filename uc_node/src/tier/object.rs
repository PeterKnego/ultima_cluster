//! Tier object codec (spec §4.1.1): a 48-byte header, then CRC-checked
//! blocks of at most [`MAX_BLOCK_RAW`] raw bytes, zstd-compressed or stored.
//!
//! Header (LE): `magic(8) ‖ codec(1) ‖ reserved(7)=0 ‖ raw_len u64 ‖ crc u32
//! ‖ reserved u32=0 ‖ from u64 ‖ to u64`. Block: `stored_len u32 ‖ raw_len
//! u32 ‖ crc32(raw) u32 ‖ bytes`; `stored_len == raw_len` means stored raw.

use super::TierError;
use std::io::{self, Read, Seek, SeekFrom, Write};

pub const OBJECT_HEADER_LEN: usize = 48;
pub const BLOCK_HEADER_LEN: usize = 12;
pub const MAX_BLOCK_RAW: usize = 1 << 20;
pub const MAGIC_ARTIFACT: [u8; 8] = *b"ULTTOBJ1";
pub const MAGIC_CHUNK: [u8; 8] = *b"ULTJCHK1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    None = 0,
    Zstd = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectHeader {
    pub magic: [u8; 8],
    pub codec: Codec,
    pub raw_len: u64,
    pub crc: u32,
    pub from: u64,
    pub to: u64,
}

impl ObjectHeader {
    fn encode(&self) -> [u8; OBJECT_HEADER_LEN] {
        let mut b = [0u8; OBJECT_HEADER_LEN];
        b[..8].copy_from_slice(&self.magic);
        b[8] = self.codec as u8;
        b[16..24].copy_from_slice(&self.raw_len.to_le_bytes());
        b[24..28].copy_from_slice(&self.crc.to_le_bytes());
        b[32..40].copy_from_slice(&self.from.to_le_bytes());
        b[40..48].copy_from_slice(&self.to.to_le_bytes());
        b
    }

    fn decode(b: &[u8; OBJECT_HEADER_LEN]) -> Result<Self, TierError> {
        let bad = |detail: &str| TierError::Corrupt {
            what: "header".into(),
            detail: detail.into(),
        };
        let magic: [u8; 8] = b[..8].try_into().unwrap();
        if magic != MAGIC_ARTIFACT && magic != MAGIC_CHUNK {
            return Err(bad("magic"));
        }
        let codec = match b[8] {
            0 => Codec::None,
            1 => Codec::Zstd,
            _ => return Err(bad("codec")),
        };
        if b[9..16].iter().any(|&x| x != 0) || b[28..32].iter().any(|&x| x != 0) {
            return Err(bad("reserved"));
        }
        let u64_at = |o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap());
        Ok(Self {
            magic,
            codec,
            raw_len: u64_at(16),
            crc: u32::from_le_bytes(b[24..28].try_into().unwrap()),
            from: u64_at(32),
            to: u64_at(40),
        })
    }
}

fn corrupt(what: &str, detail: &str) -> TierError {
    TierError::Corrupt {
        what: what.into(),
        detail: detail.into(),
    }
}

/// Read until `buf` is full or EOF; returns bytes read.
fn fill(src: &mut dyn Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match src.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

#[allow(clippy::too_many_arguments)]
pub fn write_object<W: Write + Seek>(
    dst: &mut W,
    magic: [u8; 8],
    codec: Codec,
    level: i32,
    from: u64,
    to: u64,
    raw: &mut dyn Read,
    raw_len: u64,
) -> Result<u64, TierError> {
    let start = dst.stream_position()?;
    dst.write_all(&[0u8; OBJECT_HEADER_LEN])?;
    let mut total = OBJECT_HEADER_LEN as u64;
    let mut seen = 0u64;
    let mut whole = crc32fast::Hasher::new();
    let mut buf = vec![0u8; MAX_BLOCK_RAW];
    loop {
        let n = fill(raw, &mut buf)?;
        if n == 0 {
            break;
        }
        let piece = &buf[..n];
        seen += n as u64;
        whole.update(piece);
        let crc = crc32fast::hash(piece);
        let packed = match codec {
            Codec::Zstd => Some(zstd::bulk::compress(piece, level)?),
            Codec::None => None,
        };
        let stored: &[u8] = match &packed {
            Some(c) if c.len() < n => c,
            _ => piece,
        };
        dst.write_all(&(stored.len() as u32).to_le_bytes())?;
        dst.write_all(&(n as u32).to_le_bytes())?;
        dst.write_all(&crc.to_le_bytes())?;
        dst.write_all(stored)?;
        total += (BLOCK_HEADER_LEN + stored.len()) as u64;
    }
    if seen != raw_len {
        return Err(corrupt("source", "length"));
    }
    let end = dst.stream_position()?;
    let header = ObjectHeader {
        magic,
        codec,
        raw_len,
        crc: whole.finalize(),
        from,
        to,
    };
    dst.seek(SeekFrom::Start(start))?;
    dst.write_all(&header.encode())?;
    dst.seek(SeekFrom::Start(end))?;
    Ok(total)
}

pub struct ObjectReader<R: Read> {
    src: R,
    header: ObjectHeader,
    buf: Vec<u8>,
    pos: usize,
    blocks: u64,
    total: u64,
    whole: crc32fast::Hasher,
    done: bool,
}

impl<R: Read> ObjectReader<R> {
    pub fn open(mut src: R) -> Result<Self, TierError> {
        let mut h = [0u8; OBJECT_HEADER_LEN];
        src.read_exact(&mut h)?;
        let header = ObjectHeader::decode(&h)?;
        Ok(Self {
            src,
            header,
            buf: Vec::new(),
            pos: 0,
            blocks: 0,
            total: 0,
            whole: crc32fast::Hasher::new(),
            done: false,
        })
    }

    pub fn header(&self) -> &ObjectHeader {
        &self.header
    }

    fn invalid(msg: String) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, msg)
    }

    /// Load the next block into `buf`; `Ok(false)` at a verified EOF.
    fn next_block(&mut self) -> io::Result<bool> {
        let mut bh = [0u8; BLOCK_HEADER_LEN];
        let got = fill(&mut self.src, &mut bh)?;
        if got == 0 {
            if self.total != self.header.raw_len {
                return Err(Self::invalid("object length".into()));
            }
            if self.whole.clone().finalize() != self.header.crc {
                return Err(Self::invalid("object crc".into()));
            }
            self.done = true;
            return Ok(false);
        }
        if got < BLOCK_HEADER_LEN {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let n = self.blocks;
        let stored_len = u32::from_le_bytes(bh[0..4].try_into().unwrap()) as usize;
        let raw_len = u32::from_le_bytes(bh[4..8].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(bh[8..12].try_into().unwrap());
        if raw_len > MAX_BLOCK_RAW {
            return Err(Self::invalid(format!("block {n} raw_len")));
        }
        if stored_len > raw_len || (stored_len < raw_len && self.header.codec == Codec::None) {
            return Err(Self::invalid(format!("block {n} stored_len")));
        }
        let mut stored = vec![0u8; stored_len];
        self.src.read_exact(&mut stored)?;
        let raw = if stored_len == raw_len {
            stored
        } else {
            zstd::bulk::decompress(&stored, raw_len)
                .map_err(|e| Self::invalid(format!("block {n} zstd: {e}")))?
        };
        if raw.len() != raw_len || crc32fast::hash(&raw) != crc {
            return Err(Self::invalid(format!("block {n} crc")));
        }
        self.whole.update(&raw);
        self.total += raw_len as u64;
        self.blocks += 1;
        self.buf = raw;
        self.pos = 0;
        Ok(true)
    }
}

impl<R: Read> Read for ObjectReader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        while self.pos >= self.buf.len() {
            if self.done || !self.next_block()? {
                return Ok(0);
            }
        }
        let n = out.len().min(self.buf.len() - self.pos);
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;
    use std::io::{Cursor, Read};

    fn roundtrip(codec: Codec, raw: &[u8]) -> (ObjectHeader, Vec<u8>, usize) {
        let mut sink = Cursor::new(Vec::new());
        let stored = write_object(
            &mut sink,
            MAGIC_CHUNK,
            codec,
            3,
            64,
            64 + raw.len() as u64,
            &mut Cursor::new(raw),
            raw.len() as u64,
        )
        .unwrap();
        let out = sink.into_inner();
        assert_eq!(stored as usize, out.len());
        let mut r = ObjectReader::open(Cursor::new(out.clone())).unwrap();
        let mut back = Vec::new();
        r.read_to_end(&mut back).unwrap();
        (*r.header(), back, out.len())
    }

    #[test]
    fn both_codecs_round_trip_and_header_is_exact() {
        let raw: Vec<u8> = (0..3 * MAX_BLOCK_RAW + 17)
            .map(|i| (i % 32) as u8)
            .collect();
        for codec in [Codec::None, Codec::Zstd] {
            let (h, back, _) = roundtrip(codec, &raw);
            assert_eq!(back, raw);
            assert_eq!(h.codec, codec);
            assert_eq!(h.raw_len, raw.len() as u64);
            assert_eq!(h.crc, crc32fast::hash(&raw));
            assert_eq!((h.from, h.to), (64, 64 + raw.len() as u64));
        }
    }

    #[test]
    fn zstd_shrinks_repetitive_data_and_stores_incompressible_blocks_raw() {
        let rep = vec![7u8; MAX_BLOCK_RAW];
        let (_, _, z) = roundtrip(Codec::Zstd, &rep);
        assert!(z < MAX_BLOCK_RAW / 10, "repetitive block compressed: {z}");
        // Incompressible: SHA-256 chained bytes.
        let mut noise = Vec::with_capacity(MAX_BLOCK_RAW);
        let mut h = [0u8; 32];
        while noise.len() < MAX_BLOCK_RAW {
            h = sha2::Sha256::digest(h).into();
            noise.extend_from_slice(&h);
        }
        let (_, back, z) = roundtrip(Codec::Zstd, &noise);
        assert_eq!(back, noise);
        assert_eq!(
            z,
            OBJECT_HEADER_LEN + BLOCK_HEADER_LEN + noise.len(),
            "stored raw"
        );
    }

    #[test]
    fn empty_object_round_trips() {
        let (h, back, len) = roundtrip(Codec::Zstd, &[]);
        assert!(back.is_empty());
        assert_eq!(h.raw_len, 0);
        assert_eq!(len, OBJECT_HEADER_LEN);
    }

    #[test]
    fn a_corrupted_block_fails_by_name_and_a_partial_read_stops_at_a_block() {
        let raw: Vec<u8> = (0..2 * MAX_BLOCK_RAW).map(|i| (i % 251) as u8).collect();
        let mut sink = Cursor::new(Vec::new());
        write_object(
            &mut sink,
            MAGIC_ARTIFACT,
            Codec::None,
            0,
            0,
            0,
            &mut Cursor::new(&raw),
            raw.len() as u64,
        )
        .unwrap();
        let mut out = sink.into_inner();
        // Flip a byte inside the SECOND block's payload.
        let second = OBJECT_HEADER_LEN + BLOCK_HEADER_LEN + MAX_BLOCK_RAW + BLOCK_HEADER_LEN + 5;
        out[second] ^= 1;
        let mut r = ObjectReader::open(Cursor::new(out)).unwrap();
        let mut first = vec![0u8; MAX_BLOCK_RAW];
        r.read_exact(&mut first).unwrap(); // block 1 intact
        assert_eq!(first, raw[..MAX_BLOCK_RAW]);
        let err = r.read_to_end(&mut Vec::new()).unwrap_err();
        assert!(err.to_string().contains("block 1 crc"), "{err}");
    }

    #[test]
    fn decode_refuses_bad_magic_codec_reserved_and_truncation() {
        let mut sink = Cursor::new(Vec::new());
        write_object(
            &mut sink,
            MAGIC_CHUNK,
            Codec::None,
            0,
            0,
            32,
            &mut Cursor::new(vec![0u8; 32]),
            32,
        )
        .unwrap();
        let out = sink.into_inner();
        for (i, v) in [(0usize, b'X'), (8, 9u8), (9, 1u8)] {
            let mut bad = out.clone();
            bad[i] = v;
            assert!(ObjectReader::open(Cursor::new(bad)).is_err(), "byte {i}");
        }
        let mut short = out.clone();
        short.truncate(out.len() - 1);
        let mut r = ObjectReader::open(Cursor::new(short)).unwrap();
        assert!(r.read_to_end(&mut Vec::new()).is_err());
    }
}
