//! cnc.dat layout — the shared-memory control plane between node, service,
//! and clients.
//!
//! The file is one mmap'd region laid out as:
//!
//! ```text
//! offset   contents                              size
//! ──────   ──────────────────────────────────    ────
//!   0      CncHeader                              256
//! 256      NodeStatus (atomic counters)            64
//! 320      ServiceStatus (atomic counters)         64
//! 384      control_to_service ring (RingHeader + slot region)
//!  …       control_to_node ring (RingHeader + slot region)
//! ```
//!
//! Sub-buffer offsets and sizes are recorded in `CncHeader::sub_buffer_*` so
//! future versions can shift the layout without breaking attached parties
//! (subject to the protocol-version handshake).
//!
//! M5 additions (not populated in M3): counters_metadata, counters_values,
//! error_log, and the clients/* sub-buffers.

use std::sync::atomic::{AtomicU32, AtomicU64};

use crate::ProtocolVersion;
use crate::ring::common::{RING_HEADER_LEN, RingError, init_ring_header};

pub const CNC_HEADER_LEN: usize = 256;
pub const STATUS_BLOCK_LEN: usize = 64;
/// Capacity (slot-region bytes) for the two cnc control rings. Power of two
/// to keep wrap-mask math cheap.
pub const CNC_CONTROL_RING_CAP: u64 = 16 * 1024;
pub const CNC_CONTROL_RING_MAX_MSG: u32 = 1024;

/// Offset of the `header_crc32` field within `CncHeader`. The CRC covers
/// bytes `[0..CNC_HEADER_CRC_OFFSET)` — everything in the header except the
/// CRC field itself and trailing padding.
pub const CNC_HEADER_CRC_OFFSET: usize = 248;

/// Fixed-offset header at the start of cnc.dat. Exactly 256 bytes
/// (enforced by static assertion).
#[repr(C, align(64))]
pub struct CncHeader {
    pub magic: [u8; 8],
    pub protocol_version: u32,
    pub page_size: u32,
    pub cnc_size_bytes: u64,
    pub instance_id_low: u64,
    pub instance_id_high: u64,
    pub app_id: [u8; 64], // utf-8, null-padded
    pub node_id: u64,
    pub created_at_unix_ns: u64,
    pub sub_buffer_offsets: [u64; 8],
    pub sub_buffer_sizes: [u64; 8],
    pub header_crc32: u32,
    pub _pad: [u8; 4],
}

const _: () = {
    assert!(std::mem::size_of::<CncHeader>() == CNC_HEADER_LEN);
    assert!(std::mem::offset_of!(CncHeader, header_crc32) == CNC_HEADER_CRC_OFFSET);
};

#[repr(C, align(64))]
pub struct NodeStatus {
    pub role: AtomicU32, // 0=Init 1=Follower 2=Candidate 3=Leader 4=Shutting
    pub current_term: AtomicU64,
    pub leader_node_id: AtomicU64, // u64::MAX = unknown
    pub last_applied: AtomicU64,
    pub last_committed: AtomicU64,
    pub heartbeat_seq: AtomicU64,
    pub heartbeat_at_ns: AtomicU64,
    pub _pad: [u8; 8],
}

const _: () = {
    assert!(std::mem::size_of::<NodeStatus>() == STATUS_BLOCK_LEN);
};

#[repr(C, align(64))]
pub struct ServiceStatus {
    pub state: AtomicU32, // 0=Disconnected 1=Handshaking 2=Ready 3=Snapshotting 4=Stalled
    pub _pad_1: u32,
    pub last_applied: AtomicU64,
    pub last_output_ack: AtomicU64,
    pub heartbeat_seq: AtomicU64,
    pub heartbeat_at_ns: AtomicU64,
    pub service_pid: AtomicU32,
    pub _pad_2: [u8; 20],
}

const _: () = {
    assert!(std::mem::size_of::<ServiceStatus>() == STATUS_BLOCK_LEN);
};

/// Sub-buffer indices in `CncHeader::sub_buffer_{offsets,sizes}`. Stable
/// across protocol-version-minor bumps — additions go to higher indices.
pub mod sub {
    pub const NODE_STATUS: usize = 0;
    pub const SERVICE_STATUS: usize = 1;
    pub const CONTROL_TO_SERVICE: usize = 2;
    pub const CONTROL_TO_NODE: usize = 3;
    pub const CONTROL_TO_CLIENTS: usize = 4; // M4
    pub const COUNTERS_METADATA: usize = 5; // M5
    pub const COUNTERS_VALUES: usize = 6; // M5
    pub const ERROR_LOG: usize = 7; // M5
}

/// Service-state encoding for `ServiceStatus::state`.
pub mod service_state {
    pub const DISCONNECTED: u32 = 0;
    pub const HANDSHAKING: u32 = 1;
    pub const READY: u32 = 2;
    pub const SNAPSHOTTING: u32 = 3;
    pub const STALLED: u32 = 4;
}

/// Node-role encoding for `NodeStatus::role`.
pub mod node_role {
    pub const INIT: u32 = 0;
    pub const FOLLOWER: u32 = 1;
    pub const CANDIDATE: u32 = 2;
    pub const LEADER: u32 = 3;
    pub const SHUTTING: u32 = 4;
}

/// Compute the total cnc.dat file size for the M3 layout (header + two
/// status blocks + two control rings).
pub const fn cnc_file_size() -> usize {
    CNC_HEADER_LEN
        + STATUS_BLOCK_LEN // node_status
        + STATUS_BLOCK_LEN // service_status
        + RING_HEADER_LEN + CNC_CONTROL_RING_CAP as usize // control_to_service
        + RING_HEADER_LEN + CNC_CONTROL_RING_CAP as usize // control_to_node
}

/// Initialize a freshly-created cnc.dat mmap.
/// Writes header (with CRC), zeroes status blocks, initializes both control
/// MPSC rings.
///
/// `mmap` must be at least [`cnc_file_size()`] bytes and properly aligned
/// (mmap of a freshly-truncated file from `MmapMut::map_mut` satisfies this).
pub fn init_cnc(
    mmap: &mut [u8],
    app_id: &str,
    node_id: u64,
    instance_id: u128,
) -> Result<(), RingError> {
    let file_size = cnc_file_size();
    if mmap.len() < file_size {
        return Err(RingError::Corrupt(format!(
            "cnc mmap too small: {} < {file_size}",
            mmap.len()
        )));
    }

    let mut app_id_bytes = [0u8; 64];
    let copy_len = app_id.len().min(64);
    app_id_bytes[..copy_len].copy_from_slice(&app_id.as_bytes()[..copy_len]);

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let page_size_val = page_size::get() as u32;

    let off_node_status = CNC_HEADER_LEN as u64;
    let off_service_status = off_node_status + STATUS_BLOCK_LEN as u64;
    let off_control_to_service = off_service_status + STATUS_BLOCK_LEN as u64;
    let off_control_to_node =
        off_control_to_service + RING_HEADER_LEN as u64 + CNC_CONTROL_RING_CAP;

    let mut sub_buffer_offsets = [0u64; 8];
    let mut sub_buffer_sizes = [0u64; 8];
    sub_buffer_offsets[sub::NODE_STATUS] = off_node_status;
    sub_buffer_sizes[sub::NODE_STATUS] = STATUS_BLOCK_LEN as u64;
    sub_buffer_offsets[sub::SERVICE_STATUS] = off_service_status;
    sub_buffer_sizes[sub::SERVICE_STATUS] = STATUS_BLOCK_LEN as u64;
    sub_buffer_offsets[sub::CONTROL_TO_SERVICE] = off_control_to_service;
    sub_buffer_sizes[sub::CONTROL_TO_SERVICE] = RING_HEADER_LEN as u64 + CNC_CONTROL_RING_CAP;
    sub_buffer_offsets[sub::CONTROL_TO_NODE] = off_control_to_node;
    sub_buffer_sizes[sub::CONTROL_TO_NODE] = RING_HEADER_LEN as u64 + CNC_CONTROL_RING_CAP;

    let header = CncHeader {
        magic: crate::magic::CNC_MAGIC,
        protocol_version: ProtocolVersion::new(0, 1, 0).0,
        page_size: page_size_val,
        cnc_size_bytes: file_size as u64,
        instance_id_low: instance_id as u64,
        instance_id_high: (instance_id >> 64) as u64,
        app_id: app_id_bytes,
        node_id,
        created_at_unix_ns: now_ns,
        sub_buffer_offsets,
        sub_buffer_sizes,
        header_crc32: 0,
        _pad: [0; 4],
    };
    // SAFETY: mmap is at least `file_size` bytes (checked above) and is
    // page-aligned (mmap-from-file), exceeding `align_of::<CncHeader>() == 64`.
    // No other thread can observe the bytes until this function returns.
    unsafe {
        std::ptr::write(mmap.as_mut_ptr().cast::<CncHeader>(), header);
    }

    // Compute CRC over the leading bytes of the header (everything before
    // the CRC field).
    let crc = crc32fast::hash(&mmap[..CNC_HEADER_CRC_OFFSET]);
    let crc_bytes = crc.to_le_bytes();
    // SAFETY: same range/alignment as above.
    unsafe {
        std::ptr::copy_nonoverlapping(
            crc_bytes.as_ptr(),
            mmap.as_mut_ptr().add(CNC_HEADER_CRC_OFFSET),
            4,
        );
    }

    // Zero out the status blocks (atomics start at zero).
    let ns_lo = off_node_status as usize;
    mmap[ns_lo..ns_lo + STATUS_BLOCK_LEN].fill(0);
    let ss_lo = off_service_status as usize;
    mmap[ss_lo..ss_lo + STATUS_BLOCK_LEN].fill(0);

    // Initialize the two MPSC control rings in place.
    init_ring_header(
        &mut mmap[off_control_to_service as usize..],
        CNC_CONTROL_RING_CAP,
        CNC_CONTROL_RING_MAX_MSG,
        0,
    )?;
    init_ring_header(
        &mut mmap[off_control_to_node as usize..],
        CNC_CONTROL_RING_CAP,
        CNC_CONTROL_RING_MAX_MSG,
        0,
    )?;

    Ok(())
}

/// Validate an existing cnc.dat header (magic + CRC). Used by attaching
/// parties (service, clients).
pub fn validate_cnc(mmap: &[u8]) -> Result<&CncHeader, RingError> {
    if mmap.len() < CNC_HEADER_LEN {
        return Err(RingError::Corrupt("cnc mmap too small for header".into()));
    }
    // SAFETY: mmap is at least CNC_HEADER_LEN bytes and page-aligned.
    let header = unsafe { &*mmap.as_ptr().cast::<CncHeader>() };
    if header.magic != crate::magic::CNC_MAGIC {
        return Err(RingError::BadMagic);
    }
    let crc_expected = crc32fast::hash(&mmap[..CNC_HEADER_CRC_OFFSET]);
    if header.header_crc32 != crc_expected {
        return Err(RingError::BadCrc);
    }
    Ok(header)
}

impl CncHeader {
    pub fn instance_id(&self) -> u128 {
        ((self.instance_id_high as u128) << 64) | (self.instance_id_low as u128)
    }

    pub fn app_id_str(&self) -> &str {
        let null = self.app_id.iter().position(|&b| b == 0).unwrap_or(64);
        std::str::from_utf8(&self.app_id[..null]).unwrap_or("")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use memmap2::MmapMut;
    use tempfile::NamedTempFile;

    fn mmap_file(len: usize) -> (MmapMut, NamedTempFile) {
        let tmp = NamedTempFile::new().unwrap();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmp.path())
            .unwrap();
        f.set_len(len as u64).unwrap();
        let mmap = unsafe { MmapMut::map_mut(&f).unwrap() };
        (mmap, tmp)
    }

    #[test]
    fn init_then_validate_cnc() {
        let (mut mmap, _tmp) = mmap_file(cnc_file_size());
        init_cnc(&mut mmap[..], "test-app", 42, 0xdeadbeef_cafebabe).expect("init");
        let header = validate_cnc(&mmap[..]).expect("validate");
        assert_eq!(header.app_id_str(), "test-app");
        assert_eq!(header.node_id, 42);
        assert_eq!(header.instance_id(), 0xdeadbeef_cafebabe);
        assert_eq!(header.cnc_size_bytes as usize, cnc_file_size());
        assert_eq!(header.protocol_version, ProtocolVersion::new(0, 1, 0).0);
        // Sub-buffer offsets are populated.
        assert_eq!(
            header.sub_buffer_offsets[sub::NODE_STATUS],
            CNC_HEADER_LEN as u64
        );
        assert_eq!(
            header.sub_buffer_offsets[sub::SERVICE_STATUS],
            (CNC_HEADER_LEN + STATUS_BLOCK_LEN) as u64
        );
    }

    #[test]
    fn validate_rejects_bad_magic() {
        let (mmap, _tmp) = mmap_file(cnc_file_size());
        let r = validate_cnc(&mmap[..]);
        assert!(matches!(r, Err(RingError::BadMagic)));
    }

    #[test]
    fn validate_rejects_bad_crc() {
        let (mut mmap, _tmp) = mmap_file(cnc_file_size());
        init_cnc(&mut mmap[..], "x", 0, 0).expect("init");
        // Flip a byte in the protected region (page_size offset).
        mmap[12] ^= 0xff;
        let r = validate_cnc(&mmap[..]);
        assert!(matches!(r, Err(RingError::BadCrc)));
    }

    #[test]
    fn app_id_truncated_at_64_bytes() {
        let (mut mmap, _tmp) = mmap_file(cnc_file_size());
        let long = "x".repeat(200);
        init_cnc(&mut mmap[..], &long, 1, 1).expect("init");
        let header = validate_cnc(&mmap[..]).expect("validate");
        // 64 'x's, no null terminator inside the buffer → returns all 64.
        assert_eq!(header.app_id_str().len(), 64);
    }

    #[test]
    fn status_blocks_zeroed_on_init() {
        let (mut mmap, _tmp) = mmap_file(cnc_file_size());
        // Pre-fill the status region with garbage.
        let ns_lo = CNC_HEADER_LEN;
        let ss_hi = CNC_HEADER_LEN + 2 * STATUS_BLOCK_LEN;
        mmap[ns_lo..ss_hi].fill(0xaa);
        init_cnc(&mut mmap[..], "x", 0, 0).expect("init");
        assert!(mmap[ns_lo..ss_hi].iter().all(|&b| b == 0));
    }
}
