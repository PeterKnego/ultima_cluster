// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `cnc.dat` v2 page: the mmap'd (or heap, for tests/embedded) 4 KiB control
//! page shared node/service/clients (M5). Wraps `uc_protocol::v2::cnc`'s
//! offsets + crc-agnostic header codec with the runtime bits the protocol
//! crate can't have: crc32 (workspace `crc32fast`), file I/O, and the
//! `#[repr(C)]` atomic structs (`ServiceProgress`, `NodeStatusV2`) cast
//! directly onto the page.
//!
//! **crc split (see `uc_protocol::v2::cnc` module doc for the full
//! rationale):** `uc_protocol::v2::cnc::{write,read}_cnc_header` never touch
//! the crc32 word at `CNC_OFF_HEADER_CRC` — that dependency (`crc32fast`)
//! doesn't belong in the no_std protocol crate. This module computes it on
//! write (`CncPage::init`) and validates it on attach (`CncPage::validate`),
//! same as `uc_protocol::cnc` (v1) does internally, just split across the
//! crate boundary.

use std::path::Path;
use std::sync::Arc;

use uc_protocol::v2::cnc::{
    self, CNC_OFF_APPEND, CNC_OFF_HEADER_CRC, CNC_OFF_SERVICE_APPLIED, CNC_OFF_TERM, CNC_PAGE_LEN,
    CNC_V2_VERSION, CncHeader,
};

use crate::counters::{LogCounters, PaddedAtomicU64};
use crate::region::Region;

/// Decoded cnc-page metadata (header fields + app_id), the runtime-facing
/// counterpart of `uc_protocol::v2::cnc::CncHeader` (which has no `String`
/// since it's core-only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CncMeta {
    pub node_id: u32,
    pub instance_id: u128,
    pub app_id: String,
    pub buffer_bytes: u64,
    pub max_payload: u32,
}

#[derive(thiserror::Error, Debug)]
pub enum CncError {
    #[error("cnc page io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("bad cnc header (magic, length, or crc32 mismatch)")]
    BadHeader,
    #[error("cnc app_id mismatch: expected {expected:?}, got {actual:?}")]
    AppIdMismatch { expected: String, actual: String },
    #[error("cnc protocol version mismatch: local {local:#010x}, peer {peer:#010x}")]
    VersionMismatch { local: u32, peer: u32 },
}

/// Service-side apply/output progress, cast at `CNC_OFF_SERVICE_APPLIED`.
/// One writer (the service apply agent) per field except `service_epoch`
/// (bumped by the service at attach time via `fetch_add`).
#[repr(C)]
pub struct ServiceProgress {
    pub service_applied: PaddedAtomicU64,
    pub service_epoch: PaddedAtomicU64,
    pub output_completed: PaddedAtomicU64,
}

const _: () = assert!(std::mem::size_of::<ServiceProgress>() == 192);
const _: () = assert!(std::mem::offset_of!(ServiceProgress, service_applied) == 0);
const _: () = assert!(std::mem::offset_of!(ServiceProgress, service_epoch) == 64);
const _: () = assert!(std::mem::offset_of!(ServiceProgress, output_completed) == 128);

/// Node/consensus status, cast at `CNC_OFF_TERM`. `next_client_id` is the
/// one field clients themselves write to (via `fetch_add`); every other
/// field is written only by the consensus/apply agents (see
/// `uc_protocol::v2::cnc`'s per-offset writer comments).
#[repr(C)]
pub struct NodeStatusV2 {
    pub term: PaddedAtomicU64,
    pub flags: PaddedAtomicU64,
    pub leader_hint: PaddedAtomicU64,
    pub node_heartbeat_ns: PaddedAtomicU64,
    pub service_heartbeat_ns: PaddedAtomicU64,
    pub output_progress: PaddedAtomicU64,
    pub next_client_id: PaddedAtomicU64,
}

const _: () = assert!(std::mem::size_of::<NodeStatusV2>() == 448);
const _: () = assert!(std::mem::offset_of!(NodeStatusV2, term) == 0);
const _: () = assert!(std::mem::offset_of!(NodeStatusV2, flags) == 64);
const _: () = assert!(std::mem::offset_of!(NodeStatusV2, leader_hint) == 128);
const _: () = assert!(std::mem::offset_of!(NodeStatusV2, node_heartbeat_ns) == 192);
const _: () = assert!(std::mem::offset_of!(NodeStatusV2, service_heartbeat_ns) == 256);
const _: () = assert!(std::mem::offset_of!(NodeStatusV2, output_progress) == 320);
const _: () = assert!(std::mem::offset_of!(NodeStatusV2, next_client_id) == 384);

/// The mmap'd (or heap) cnc v2 page. `Region` is `Send + Sync`, so this is
/// too — every accessor casts a `&self`-borrowed reference at a pinned
/// offset; no interior mutability beyond the atomics themselves.
pub struct CncPage {
    region: Region,
}

impl CncPage {
    /// Common constructor: asserts the region is exactly one page and that
    /// its base pointer is aligned for the `#[repr(C)]` atomic-struct casts
    /// (64 B — the coarsest alignment any of `LogCounters` /
    /// `ServiceProgress` / `NodeStatusV2` need; both backings satisfy it:
    /// `Region::heap_zeroed` allocates with `align(64)`, and a memory-mapped
    /// file is page-aligned (4096), a stricter multiple of 64).
    fn new(region: Region) -> Self {
        assert_eq!(region.len(), CNC_PAGE_LEN, "cnc page must be exactly {CNC_PAGE_LEN} bytes");
        let base = unsafe { region.ptr_at(0) } as usize;
        assert_eq!(base % 64, 0, "cnc page base must be 64-byte aligned");
        Self { region }
    }

    /// Whole-page byte view for header codec calls.
    fn page(&self) -> &[u8] {
        // SAFETY: region.len() == CNC_PAGE_LEN (asserted in `new`); the
        // returned slice's lifetime is tied to `&self`.
        unsafe { std::slice::from_raw_parts(self.region.ptr_at(0), CNC_PAGE_LEN) }
    }

    /// `&mut self` (not just `&self`) is deliberate: only called from
    /// `init`, which itself only runs on a freshly-constructed, not-yet-
    /// shared `CncPage` (before it's wrapped in the `Arc` every accessor
    /// method hands out `&LogCounters`/etc. through) — so `&mut self` here
    /// can never alias a live `&LogCounters`/`&ServiceProgress`/`&NodeStatusV2`.
    fn page_mut(&mut self) -> &mut [u8] {
        // SAFETY: region.len() == CNC_PAGE_LEN (asserted in `new`).
        unsafe { std::slice::from_raw_parts_mut(self.region.ptr_at(0), CNC_PAGE_LEN) }
    }

    /// Write the header for a freshly-created page: fixed fields via
    /// `uc_protocol::v2::cnc::write_cnc_header`, then this crate's crc32
    /// over `[0..CNC_OFF_HEADER_CRC)`, then the fixed initial values
    /// (`leader_hint = u64::MAX`, `next_client_id = 1`; every other atomic
    /// stays at its zeroed default).
    fn init(&mut self, meta: &CncMeta) {
        let created_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let header = CncHeader {
            version: CNC_V2_VERSION,
            node_id: meta.node_id,
            instance_id: meta.instance_id,
            created_ns,
            buffer_bytes: meta.buffer_bytes,
            max_payload: meta.max_payload,
        };
        let page = self.page_mut();
        cnc::write_cnc_header(page, &header, &meta.app_id);
        let crc = crc32fast::hash(&page[..CNC_OFF_HEADER_CRC]);
        page[CNC_OFF_HEADER_CRC..CNC_OFF_HEADER_CRC + 4].copy_from_slice(&crc.to_le_bytes());

        self.status().leader_hint.store_release(u64::MAX);
        self.status().next_client_id.store_release(1);
    }

    /// Validate an attached (not just-created) page: magic/length (via
    /// `read_cnc_header`), crc32, protocol version compatibility, and
    /// `app_id` match. This is the crc-check home per the module doc split.
    fn validate(&self, expected_app_id: &str) -> Result<(), CncError> {
        let page = self.page();
        let header = cnc::read_cnc_header(page).ok_or(CncError::BadHeader)?;
        let crc_expected = crc32fast::hash(&page[..CNC_OFF_HEADER_CRC]);
        let crc_actual = u32::from_le_bytes(
            page[CNC_OFF_HEADER_CRC..CNC_OFF_HEADER_CRC + 4].try_into().unwrap(),
        );
        if crc_actual != crc_expected {
            return Err(CncError::BadHeader);
        }
        if !cnc::version_compatible(CNC_V2_VERSION, header.version) {
            return Err(CncError::VersionMismatch { local: CNC_V2_VERSION, peer: header.version });
        }
        let actual_app_id = cnc::read_cnc_app_id(page);
        if actual_app_id != expected_app_id {
            return Err(CncError::AppIdMismatch {
                expected: expected_app_id.to_string(),
                actual: actual_app_id.to_string(),
            });
        }
        Ok(())
    }

    /// Create (or truncate) the cnc file at exactly one page, map it, and
    /// write a fresh header + initial atomic values.
    pub fn create_file(path: &Path, meta: &CncMeta) -> Result<Arc<CncPage>, CncError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(CNC_PAGE_LEN as u64)?;
        // SAFETY: exclusive logical ownership per the instance-dir contract
        // (one node per instance dir; instance.lock is the flock gate,
        // uc2_node territory — this call is the creating party).
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
        let mut page = Self::new(Region::from_mmap(mmap));
        page.init(meta);
        Ok(Arc::new(page))
    }

    /// Map an existing cnc file and validate it belongs to this app
    /// (magic/crc/version/app_id). Used by attaching parties (service,
    /// clients, a reconnecting node).
    pub fn open_file(path: &Path, expected_app_id: &str) -> Result<Arc<CncPage>, CncError> {
        let file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
        // Attach-validation contract: bad magic/len/crc → BadHeader (a typed
        // error, never a panic). The file length here is EXTERNAL input (a
        // torn create_file crash, wrong path, corruption), so it must be
        // rejected before reaching `Self::new`, whose length assert guards
        // only the internally-controlled construction paths (create_file
        // just set_len'd the file; heap allocated exactly one page).
        if file.metadata()?.len() != CNC_PAGE_LEN as u64 {
            return Err(CncError::BadHeader);
        }
        // SAFETY: attaching to a page created by `create_file` (possibly in
        // another process) via a shared mmap of the same file.
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
        let page = Self::new(Region::from_mmap(mmap));
        page.validate(expected_app_id)?;
        Ok(Arc::new(page))
    }

    /// Heap-backed page (tests / embedded single-process use) — no file, no
    /// mmap; same header + init path as `create_file`.
    pub fn heap(meta: &CncMeta) -> Arc<CncPage> {
        let mut page = Self::new(Region::heap_zeroed(CNC_PAGE_LEN));
        page.init(meta);
        Arc::new(page)
    }

    /// `LogCounters` cast at `CNC_OFF_APPEND` (4 lines: append/durable/sent/commit).
    pub fn counters(&self) -> &LogCounters {
        // SAFETY: region.len() == CNC_PAGE_LEN and base is 64-byte aligned
        // (both asserted in `new`); CNC_OFF_APPEND (256) is a 64-byte-aligned
        // offset and size_of::<LogCounters>() == 256 fits within the page
        // from there (256 + 256 == 512 <= 4096). `LogCounters` is
        // `#[repr(C)]` over `PaddedAtomicU64` fields, so the cast is
        // layout-pinned (cross-checked against `uc_protocol` in this
        // module's tests). The reference borrows `self`.
        unsafe { &*(self.region.ptr_at(CNC_OFF_APPEND) as *const LogCounters) }
    }

    /// `ServiceProgress` cast at `CNC_OFF_SERVICE_APPLIED`.
    pub fn service(&self) -> &ServiceProgress {
        // SAFETY: as `counters()` — offset 512, size 192, 512+192=704<=4096.
        unsafe { &*(self.region.ptr_at(CNC_OFF_SERVICE_APPLIED) as *const ServiceProgress) }
    }

    /// `NodeStatusV2` cast at `CNC_OFF_TERM`.
    pub fn status(&self) -> &NodeStatusV2 {
        // SAFETY: as `counters()` — offset 704, size 448, 704+448=1152<=4096.
        unsafe { &*(self.region.ptr_at(CNC_OFF_TERM) as *const NodeStatusV2) }
    }

    /// Decode the header + app_id back into an owned `CncMeta`.
    pub fn meta(&self) -> CncMeta {
        let page = self.page();
        let header = cnc::read_cnc_header(page)
            .expect("cnc page header must be valid after construction (init/validate ran)");
        CncMeta {
            node_id: header.node_id,
            instance_id: header.instance_id,
            app_id: cnc::read_cnc_app_id(page).to_string(),
            buffer_bytes: header.buffer_bytes,
            max_payload: header.max_payload,
        }
    }

    /// Test-only: the page's base address, for pointer-offset assertions
    /// (`accessor() as *const _ as usize - page_base_for_tests()`).
    #[doc(hidden)]
    pub fn page_base_for_tests(&self) -> usize {
        // SAFETY: off=0 < len (CNC_PAGE_LEN > 0).
        unsafe { self.region.ptr_at(0) as usize }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_meta() -> CncMeta {
        CncMeta {
            node_id: 1,
            instance_id: 0x1122_3344_5566_7788,
            app_id: "test-app".into(),
            buffer_bytes: 1 << 20,
            max_payload: 256,
        }
    }

    #[test]
    fn cnc_offsets_match_protocol_constants() {
        use uc_protocol::v2::cnc::*;
        assert_eq!(size_of::<LogCounters>(), 256);
        assert_eq!(CNC_OFF_DURABLE - CNC_OFF_APPEND, 64);
        assert_eq!(size_of::<ServiceProgress>(), 192);
        assert_eq!(size_of::<NodeStatusV2>(), 448);
        assert_eq!(CNC_OFF_NEXT_CLIENT_ID - CNC_OFF_TERM, 384);
        let page = CncPage::heap(&test_meta());
        let base = page.page_base_for_tests();
        assert_eq!(page.counters() as *const _ as usize - base, CNC_OFF_APPEND);
        assert_eq!(page.service() as *const _ as usize - base, CNC_OFF_SERVICE_APPLIED);
        assert_eq!(page.status() as *const _ as usize - base, CNC_OFF_TERM);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real cnc file, mmap'd
    fn cnc_file_roundtrip_and_attach_checks() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("cnc2.dat");
        let meta = CncMeta {
            node_id: 3,
            instance_id: 0xABCD_EF01_2345_6789_u128,
            app_id: "kv".into(),
            buffer_bytes: 1 << 20,
            max_payload: 256,
        };
        let page = CncPage::create_file(&p, &meta).unwrap();
        page.counters().append.store_release(4096);
        let re = CncPage::open_file(&p, "kv").unwrap();
        assert_eq!(re.meta().instance_id, meta.instance_id);
        assert_eq!(re.counters().append.load_acquire(), 4096, "same mapped page, not a copy");
        assert!(matches!(CncPage::open_file(&p, "other"), Err(CncError::AppIdMismatch { .. })));
        assert_eq!(re.status().leader_hint.load_acquire(), u64::MAX);
        assert_eq!(re.status().next_client_id.load_acquire(), 1);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real cnc file, mmap'd
    fn open_file_rejects_flipped_crc_byte() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("cnc2.dat");
        let _page = CncPage::create_file(&p, &test_meta()).unwrap();
        // Flip a byte in the protected header region (node_id, offset 12 —
        // well before the crc word itself at CNC_OFF_HEADER_CRC) directly on
        // disk, behind the page's back.
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new().write(true).open(&p).unwrap();
            f.seek(SeekFrom::Start(12)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }
        let r = CncPage::open_file(&p, "test-app").map(|_| ());
        assert!(matches!(r, Err(CncError::BadHeader)), "flipped crc-protected byte must be rejected: {r:?}");
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real cnc file, mmap'd
    fn open_file_rejects_wrong_length_file_without_panicking() {
        // A cnc file of any non-4096 length (torn create_file crash, wrong
        // path, corruption) is EXTERNAL input: the attach contract is
        // "bad magic/len/crc → BadHeader", never a panic in the attaching
        // process. (CncPage::new's length assert is only for the
        // internally-controlled create_file/heap paths.)
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("cnc2.dat");
        std::fs::write(&p, vec![0u8; 1000]).unwrap();
        let r = CncPage::open_file(&p, "test-app").map(|_| ());
        assert!(matches!(r, Err(CncError::BadHeader)), "{r:?}");
        // Also the too-long side, same class of corruption.
        std::fs::write(&p, vec![0u8; CNC_PAGE_LEN + 1]).unwrap();
        let r = CncPage::open_file(&p, "test-app").map(|_| ());
        assert!(matches!(r, Err(CncError::BadHeader)), "{r:?}");
    }

    #[test]
    fn meta_roundtrips_high_instance_bits_and_63_byte_app_id() {
        // instance_id with non-zero HIGH 64 bits pins the INSTANCE_LO/HI
        // split (a swapped-half bug is invisible to ids that fit in u64),
        // and a 63-byte app_id is the longest legal one (64-byte field,
        // NUL-terminated).
        let app_id: String = "a".repeat(63);
        let meta = CncMeta {
            node_id: 9,
            instance_id: (0xFEED_FACE_CAFE_BEEF_u128 << 64) | 0x0123_4567_89AB_CDEF_u128,
            app_id: app_id.clone(),
            buffer_bytes: 1 << 22,
            max_payload: 512,
        };
        let page = CncPage::heap(&meta);
        let out = page.meta();
        assert_eq!(out, meta);
        assert_eq!(out.app_id.len(), 63);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real cnc file, mmap'd
    fn open_file_rejects_bad_magic() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("cnc2.dat");
        std::fs::write(&p, vec![0u8; CNC_PAGE_LEN]).unwrap();
        let r = CncPage::open_file(&p, "test-app");
        assert!(matches!(r, Err(CncError::BadHeader)));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // real cnc file, mmap'd
    fn open_file_rejects_incompatible_version() {
        use uc_protocol::v2::cnc::CNC_OFF_VERSION;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("cnc2.dat");
        let page = CncPage::create_file(&p, &test_meta()).unwrap();
        // Write a newer minor version directly, then recompute crc so only
        // the version check trips (not crc).
        {
            let mut raw = std::fs::read(&p).unwrap();
            let newer = CNC_V2_VERSION | (1 << 16); // minor bumped
            raw[CNC_OFF_VERSION..CNC_OFF_VERSION + 4].copy_from_slice(&newer.to_le_bytes());
            let crc = crc32fast::hash(&raw[..CNC_OFF_HEADER_CRC]);
            raw[CNC_OFF_HEADER_CRC..CNC_OFF_HEADER_CRC + 4].copy_from_slice(&crc.to_le_bytes());
            std::fs::write(&p, &raw).unwrap();
        }
        drop(page);
        let r = CncPage::open_file(&p, "test-app").map(|_| ());
        assert!(matches!(r, Err(CncError::VersionMismatch { .. })), "{r:?}");
    }

    #[test]
    fn heap_page_next_client_id_allocates_with_fetch_add() {
        let page = CncPage::heap(&test_meta());
        let first = page.status().next_client_id.fetch_add(1);
        assert_eq!(first, 1, "starts at 1 per the M4 next_client_id contract");
        assert_eq!(page.status().next_client_id.load_acquire(), 2);
    }

    #[test]
    fn service_epoch_bumps_with_fetch_add() {
        let page = CncPage::heap(&test_meta());
        assert_eq!(page.service().service_epoch.load_acquire(), 0);
        let prev = page.service().service_epoch.fetch_add(1);
        assert_eq!(prev, 0);
        assert_eq!(page.service().service_epoch.load_acquire(), 1);
    }
}
