//! `snapshot.region`: a separate file under the instance dir carrying snapshot
//! bytes between node and service (Phase 2a). Header: magic, format_ver, byte_len,
//! snapshot last_log_id index, crc32 of the bytes. Cross-process ordering is the
//! CALLER's responsibility via the snapshot control-ring ack (write region → send
//! BUILT/INSTALL frame → peer reads region only after receiving the frame).

use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;

const MAGIC: [u8; 8] = *b"UCSNAPRG";
const FORMAT_VER: u32 = 1;
/// magic(8) + format_ver(4) + _pad(4) + byte_len(8) + snapshot_index(8) + crc32(4) + _pad(4)
const HEADER_LEN: usize = 40;

#[derive(Debug, thiserror::Error)]
pub enum SnapshotRegionError {
    #[error("snapshot region io: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot region bad magic")]
    BadMagic,
    #[error("snapshot region unsupported format {0}")]
    BadFormat(u32),
    #[error("snapshot region crc mismatch: header {header:#x} computed {computed:#x}")]
    CrcMismatch { header: u32, computed: u32 },
    #[error("snapshot region truncated: header says {expected} bytes, file has {actual}")]
    Truncated { expected: u64, actual: u64 },
}

/// Write `bytes` (a snapshot at `snapshot_index`) to `path`, replacing any prior
/// content. Truncates the file to exactly HEADER_LEN + bytes.len().
pub fn write(path: &Path, snapshot_index: u64, bytes: &[u8]) -> Result<(), SnapshotRegionError> {
    let crc = crc32fast::hash(bytes);
    let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(path)?;
    let mut header = [0u8; HEADER_LEN];
    header[0..8].copy_from_slice(&MAGIC);
    header[8..12].copy_from_slice(&FORMAT_VER.to_le_bytes());
    header[16..24].copy_from_slice(&(bytes.len() as u64).to_le_bytes());
    header[24..32].copy_from_slice(&snapshot_index.to_le_bytes());
    header[32..36].copy_from_slice(&crc.to_le_bytes());
    f.write_all(&header)?;
    f.write_all(bytes)?;
    f.flush()?;
    Ok(())
}

/// Read + validate the region. Returns `(snapshot_index, bytes)`.
pub fn read(path: &Path) -> Result<(u64, Vec<u8>), SnapshotRegionError> {
    let mut f = OpenOptions::new().read(true).open(path)?;
    let mut header = [0u8; HEADER_LEN];
    f.read_exact(&mut header)?;
    if header[0..8] != MAGIC {
        return Err(SnapshotRegionError::BadMagic);
    }
    let fmt = u32::from_le_bytes(header[8..12].try_into().unwrap());
    if fmt != FORMAT_VER {
        return Err(SnapshotRegionError::BadFormat(fmt));
    }
    let byte_len = u64::from_le_bytes(header[16..24].try_into().unwrap());
    let snapshot_index = u64::from_le_bytes(header[24..32].try_into().unwrap());
    let crc = u32::from_le_bytes(header[32..36].try_into().unwrap());
    // Cap the pre-allocation hint: a corrupt header `byte_len` must not trigger a
    // multi-GB allocation (OOM abort) before the Truncated/CrcMismatch checks fire.
    // `read_to_end` grows past the hint for legitimately larger snapshots.
    let cap_hint = (byte_len as usize).min(256 * 1024 * 1024);
    let mut bytes = Vec::with_capacity(cap_hint);
    f.read_to_end(&mut bytes)?;
    if bytes.len() as u64 != byte_len {
        return Err(SnapshotRegionError::Truncated { expected: byte_len, actual: bytes.len() as u64 });
    }
    let computed = crc32fast::hash(&bytes);
    if computed != crc {
        return Err(SnapshotRegionError::CrcMismatch { header: crc, computed });
    }
    Ok((snapshot_index, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round_trip() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let data = vec![7u8; 5000];
        write(tmp.path(), 123, &data).unwrap();
        let (idx, got) = read(tmp.path()).unwrap();
        assert_eq!(idx, 123);
        assert_eq!(got, data);
    }
    #[test]
    fn crc_mismatch_detected() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        write(tmp.path(), 1, b"hello").unwrap();
        let mut buf = std::fs::read(tmp.path()).unwrap();
        let n = buf.len();
        buf[n - 1] ^= 0xFF;
        std::fs::write(tmp.path(), &buf).unwrap();
        assert!(matches!(read(tmp.path()), Err(SnapshotRegionError::CrcMismatch { .. })));
    }
    #[test]
    fn rewrite_shrinks() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        write(tmp.path(), 1, &vec![1u8; 9000]).unwrap();
        write(tmp.path(), 2, b"tiny").unwrap();
        let (idx, got) = read(tmp.path()).unwrap();
        assert_eq!(idx, 2);
        assert_eq!(got, b"tiny");
    }
}
