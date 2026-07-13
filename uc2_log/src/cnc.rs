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
    self, CNC_MAX_PEER_SLOTS, CNC_OFF_ADMIN_REQ, CNC_OFF_ADMIN_RESP, CNC_OFF_APPEND,
    CNC_OFF_ARCHIVE_FIRST_BASE, CNC_OFF_CONFIG_PENDING, CNC_OFF_CONFIG_VERSION, CNC_OFF_HEADER_CRC,
    CNC_OFF_PEER_SLOTS, CNC_OFF_SERVICE_APPLIED, CNC_OFF_SERVICE_SNAPSHOT_POS, CNC_OFF_TERM,
    CNC_PAGE_LEN, CNC_PEER_SLOT_STRIDE, CNC_V2_VERSION, CncHeader,
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

/// M6 Task 3: the snapshot marker slots, cast at `CNC_OFF_SERVICE_SNAPSHOT_POS`
/// (1152). `service_snapshot_pos`: writer = the service snapshot builder
/// thread; position of the newest COMPLETE on-disk snapshot, `0` = none — set
/// ONLY after `SnapshotStore::publish`'s atomic rename completes (a torn build
/// is never visible here). `node_snapshot_floor`: writer = consensus (Task 4
/// mirrors the same value onto the node side so the purge driver never has to
/// cross into the service's write); init `0`.
#[repr(C)]
pub struct SnapshotSlots {
    pub service_snapshot_pos: PaddedAtomicU64,
    pub node_snapshot_floor: PaddedAtomicU64,
    /// M6 Task 6: position of the newest COMPLETE inbound snapshot the receiver
    /// landed (writer: consensus, mirrored from the receiver's node-internal
    /// signal). Observability; the AdoptFloor decision reads the same signal.
    pub incoming_snapshot_pos: PaddedAtomicU64,
}

const _: () = assert!(std::mem::size_of::<SnapshotSlots>() == 192);
const _: () = assert!(std::mem::offset_of!(SnapshotSlots, service_snapshot_pos) == 0);
const _: () = assert!(std::mem::offset_of!(SnapshotSlots, node_snapshot_floor) == 64);
const _: () = assert!(std::mem::offset_of!(SnapshotSlots, incoming_snapshot_pos) == 128);

/// M6 Task 9: one per-peer observability record (leader-published), cast at
/// `CNC_OFF_PEER_SLOTS + i * CNC_PEER_SLOT_STRIDE`. Each field is its own
/// 64-byte cache line so the two distinct writers (consensus, sender) never
/// false-share. A dormant slot (unused index, or any slot on a follower) reads
/// all-zero. Bounded update: the leader refreshes a slot at most once per duty
/// cycle, never per datagram — this band is diagnostics, off the hot path.
#[repr(C)]
pub struct PeerSlot {
    /// `(peer_id as u64) << 8 | role_bits`; role_bits ∈ {VOTER, LEARNER}.
    /// Writer: consensus (boot-once). Zero = unused slot.
    pub id_and_role: PaddedAtomicU64,
    /// Newest durable position this peer reported (writer: consensus, Report
    /// intake — includes a learner's cell, which never counts toward commit).
    pub reported_durable: PaddedAtomicU64,
    /// The receive window this peer last advertised via STATUS (writer: sender).
    pub advertised_limit: PaddedAtomicU64,
    /// `(naks_served as u64) << 32 | (replay_datagrams as u32 as u64)` — the
    /// leader's retransmit effort toward this peer (writer: sender). RESERVED /
    /// dormant in M6: the sender's `naks_served`/`replay_datagrams` counters are
    /// aggregate (in `SenderStats`), not per-peer; a per-peer split would add a
    /// hot-path (per-datagram) counter to the retransmit loop, so it is deferred.
    /// The cache line is pinned so a later fill needs no layout change.
    pub naks_plus_replay: PaddedAtomicU64,
}

const _: () = assert!(std::mem::size_of::<PeerSlot>() == 256);
const _: () = assert!(std::mem::size_of::<PeerSlot>() == CNC_PEER_SLOT_STRIDE);
const _: () = assert!(std::mem::offset_of!(PeerSlot, id_and_role) == 0);
const _: () = assert!(std::mem::offset_of!(PeerSlot, reported_durable) == 64);
const _: () = assert!(std::mem::offset_of!(PeerSlot, advertised_limit) == 128);
const _: () = assert!(std::mem::offset_of!(PeerSlot, naks_plus_replay) == 192);

/// Pack a peer id + role byte into the `id_and_role` cell.
#[inline]
pub fn pack_id_and_role(peer_id: u32, role_bits: u8) -> u64 {
    ((peer_id as u64) << 8) | role_bits as u64
}

/// Pack the sender's per-peer retransmit counters into `naks_plus_replay`.
#[inline]
pub fn pack_naks_plus_replay(naks_served: u32, replay_datagrams: u32) -> u64 {
    ((naks_served as u64) << 32) | replay_datagrams as u64
}

/// M7 admin request record (seqlock discipline).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminReq {
    pub seq: u64,
    pub nonce: u64,
    pub op: u32,
    pub id: u32,
    pub ip: u32,
    pub port: u16,
}

/// M7 admin response record (seqlock discipline).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminResp {
    pub seq: u64,
    pub status: u32,
    pub reason: u32,
    pub version: u64,
}

/// The mmap'd (or heap) cnc v2 page. `Region` is `Send + Sync`, so this is
/// too — every accessor casts a `&self`-borrowed reference at a pinned
/// offset; no interior mutability beyond the atomics themselves.
pub struct CncPage {
    region: Region,
}

/// Pick the initial value of a freshly-created page's `next_client_id`
/// allocation counter. A random `u32` in `[1, 2^31)` (nonzero — `0` is the
/// reserved "no id" sentinel; top bit forced clear).
///
/// **Why random, not `1` (load-bearing — T14 MAJOR; the next reader MUST
/// understand this):** `client_id` is allocated per cnc-page *generation*
/// (`fetch_add` off this counter), and a page is recreated from scratch on
/// every node (re)boot. A client's request/response identity on the shared
/// broadcast rings is the pair `(client_id, local_seq)` and NOTHING else — the
/// matcher correlates purely on it. Meanwhile the service's live-ring catch-up
/// path (`uc2_service::apply`) legitimately RE-PUBLISHES historical responses
/// stamped with their ORIGINAL `(client_id, local_seq)` when a fresh service
/// incarnation walks the committed log from cursor 0 (this is the at-least-once
/// delivery a client waits on across a service-only crash — it must NOT be
/// suppressed). If `next_client_id` restarted at `1` every generation, an old
/// generation's re-published response `(id, seq)` would collide with a live
/// client of the SAME low `(id, seq)` in a later generation and be misdelivered
/// to it — the pinned T14 linearizability defect (a stale submit response
/// answering a live query). Seeding each generation at an independent random
/// base makes such a collision require two generations' random bases to land
/// within `local_seq`-range (a few thousand) of each other: with bases spread
/// over `2^31` that is ~`2^-31·k` per generation pair — negligible.
///
/// The top bit is forced clear so a generation's allocations (base + a few
/// thousand ids) have ~`2^31` of headroom before they could wrap `u32` into
/// another generation's likely-occupied range; a `local_seq`-range wrap into a
/// neighbouring random base is the same negligible event as above.
///
/// **Accepted residual (v2.0):** a *same-kind* cross-generation collision (a
/// stale submit → a live submit of the identically-random `(id, seq)`) remains
/// theoretically possible at that ~`2^-31` scale and is accepted; the
/// [`uc2_client`] matcher's kind check (submit-vs-query, `FLAG_V2_IS_QUERY`) is
/// the second belt that kills the observed *cross*-kind (submit→query) confusion
/// class even under such a residual collision.
fn gen_unique_client_id_base() -> u32 {
    (rand::random::<u32>() >> 1) | 1
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
    /// (`leader_hint = u64::MAX`, `next_client_id` = a per-generation random
    /// base — see [`gen_unique_client_id_base`]; every other atomic stays at
    /// its zeroed default).
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
        self.status().next_client_id.store_release(gen_unique_client_id_base() as u64);
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
    ///
    /// **Accepted SIGBUS window (M5 final review #2a):** this recreates the file
    /// IN PLACE (same inode) — `truncate: true` drops it to length 0, then
    /// `set_len(CNC_PAGE_LEN)` grows it back. A stale process from a previous
    /// node generation that still holds a live mmap of this same inode can, in
    /// the truncate→`set_len` gap, touch a page that is momentarily beyond
    /// end-of-file and take a SIGBUS. This is ACCEPTED: it is behaviorally
    /// equivalent to the SIGKILL that the crashtest (`node_sigkill_recovery`)
    /// already proves safe — a stale attachment dying is the intended outcome of
    /// a node restart (v2.0 contract). Recreating in place (rather than
    /// unlink+create a new inode) is LOAD-BEARING: it is what lets an attached
    /// client's own mmap observe the fresh `instance_id` and self-classify
    /// [`crate::cnc::CncPage::try_instance_id`]-driven `InstanceRestart`, instead
    /// of reading a stale detached page forever.
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

    /// `SnapshotSlots` cast at `CNC_OFF_SERVICE_SNAPSHOT_POS` (M6 Task 3).
    pub fn snapshots(&self) -> &SnapshotSlots {
        // SAFETY: as `counters()` — offset 1152, size 192, 1152+192=1344<=4096.
        unsafe { &*(self.region.ptr_at(CNC_OFF_SERVICE_SNAPSHOT_POS) as *const SnapshotSlots) }
    }

    /// The archive's first-retained log position (purge floor), mirrored by the
    /// consensus agent (M6 Task 9). Compare against `snapshots().node_snapshot_floor`
    /// for the "purge caught up to snapshot" health check.
    pub fn archive_first_base(&self) -> &PaddedAtomicU64 {
        // SAFETY: offset 1344, size 64, 1344+64=1408<=4096.
        unsafe { &*(self.region.ptr_at(CNC_OFF_ARCHIVE_FIRST_BASE) as *const PaddedAtomicU64) }
    }

    /// The `i`-th per-peer observability slot (M6 Task 9). Panics if `i` is out
    /// of range. A dormant slot reads all-zero.
    pub fn peer_slot(&self, i: usize) -> &PeerSlot {
        assert!(i < CNC_MAX_PEER_SLOTS, "peer slot index {i} out of range");
        let off = CNC_OFF_PEER_SLOTS + i * CNC_PEER_SLOT_STRIDE;
        // SAFETY: last slot ends at 1408 + 8*256 = 3456 <= 4096 (const-asserted
        // in `uc_protocol`); each is a `PeerSlot` (256 B, 4 padded atomics).
        unsafe { &*(self.region.ptr_at(off) as *const PeerSlot) }
    }

    /// M7: config version (adopted cluster-config).
    pub fn config_version(&self) -> u64 {
        // SAFETY: offset 3456, size 8 (first field of the 64-byte line).
        let ptr = unsafe { self.region.ptr_at(CNC_OFF_CONFIG_VERSION) as *const PaddedAtomicU64 };
        // SAFETY: cast is valid; the atomic is within bounds and properly aligned.
        unsafe { (*ptr).load_acquire() }
    }

    /// M7: store config version.
    pub fn store_config_version(&self, v: u64) {
        // SAFETY: offset 3456, size 8.
        let ptr = unsafe { self.region.ptr_at(CNC_OFF_CONFIG_VERSION) as *const PaddedAtomicU64 };
        unsafe { (*ptr).store_release(v) }
    }

    /// M7: config pending (1 = uncommitted, 0 = stable).
    pub fn config_pending(&self) -> u64 {
        // SAFETY: offset 3520, size 8.
        let ptr = unsafe { self.region.ptr_at(CNC_OFF_CONFIG_PENDING) as *const PaddedAtomicU64 };
        unsafe { (*ptr).load_acquire() }
    }

    /// M7: store config pending.
    pub fn store_config_pending(&self, pending: bool) {
        // SAFETY: offset 3520, size 8.
        let ptr = unsafe { self.region.ptr_at(CNC_OFF_CONFIG_PENDING) as *const PaddedAtomicU64 };
        unsafe { (*ptr).store_release(pending as u64) }
    }

    /// M7: read admin request if seq > last_seen_seq (seqlock semantics).
    /// Returns `None` if the seq hasn't advanced since last call with that seq.
    pub fn read_admin_req(&self, last_seen_seq: u64) -> Option<AdminReq> {
        let off = CNC_OFF_ADMIN_REQ;
        // SAFETY: off is CNC_OFF_ADMIN_REQ, a 64-byte-aligned offset within
        // the page; seq is the seqlock commit word (writer: write_admin_req,
        // store_release'd last), so it must be acquire-loaded — a plain byte
        // read here would let the compiler/CPU reorder the field reads below
        // ahead of it, observing a torn write.
        let ptr_seq = unsafe { self.region.ptr_at(off) as *const PaddedAtomicU64 };
        let seq = unsafe { (*ptr_seq).load_acquire() };
        if seq <= last_seen_seq {
            return None;
        }
        // The acquire above orders these plain field reads (seqlock discipline).
        let page = self.page();
        let nonce = u64::from_le_bytes(page[off + 8..off + 16].try_into().unwrap());
        let op = u32::from_le_bytes(page[off + 16..off + 20].try_into().unwrap());
        let id = u32::from_le_bytes(page[off + 20..off + 24].try_into().unwrap());
        let ip = u32::from_le_bytes(page[off + 24..off + 28].try_into().unwrap());
        let port = u32::from_le_bytes(page[off + 28..off + 32].try_into().unwrap()) as u16;
        Some(AdminReq { seq, nonce, op, id, ip, port })
    }

    /// M7: write admin request (fields then seq with release for seqlock semantics).
    pub fn write_admin_req(&self, req: &AdminReq) {
        let off = CNC_OFF_ADMIN_REQ;
        let ptr_seq = unsafe { self.region.ptr_at(off) as *const PaddedAtomicU64 };
        // Write fields first (not seq)
        // SAFETY: off is CNC_OFF_ADMIN_REQ; we write a 64-byte line at that offset.
        let page_mut = unsafe { std::slice::from_raw_parts_mut(self.region.ptr_at(off), 64) };
        page_mut[8..16].copy_from_slice(&req.nonce.to_le_bytes());
        page_mut[16..20].copy_from_slice(&req.op.to_le_bytes());
        page_mut[20..24].copy_from_slice(&req.id.to_le_bytes());
        page_mut[24..28].copy_from_slice(&req.ip.to_le_bytes());
        page_mut[28..32].copy_from_slice(&u32::from(req.port).to_le_bytes());
        // Write seq last with release
        // SAFETY: ptr_seq is valid (cast from self.region.ptr_at); it's a PaddedAtomicU64.
        unsafe { (*ptr_seq).store_release(req.seq) };
    }

    /// M7: read admin response if seq matches (seqlock semantics).
    pub fn read_admin_resp(&self, expect_seq: u64) -> Option<AdminResp> {
        // SAFETY: offset 3648, seq at +0, status at +8, reason at +12, version at +16.
        let off = CNC_OFF_ADMIN_RESP;
        // SAFETY: off is CNC_OFF_ADMIN_RESP, a 64-byte-aligned offset; seq is
        // the seqlock commit word (writer: write_admin_resp, store_release'd
        // last), so it must be acquire-loaded — see read_admin_req for why a
        // plain byte read here would be unsound.
        let ptr_seq = unsafe { self.region.ptr_at(off) as *const PaddedAtomicU64 };
        let seq = unsafe { (*ptr_seq).load_acquire() };
        if seq != expect_seq {
            return None;
        }
        // The acquire above orders these plain field reads (seqlock discipline).
        let page = self.page();
        let status = u32::from_le_bytes(page[off + 8..off + 12].try_into().unwrap());
        let reason = u32::from_le_bytes(page[off + 12..off + 16].try_into().unwrap());
        let version = u64::from_le_bytes(page[off + 16..off + 24].try_into().unwrap());
        Some(AdminResp { seq, status, reason, version })
    }

    /// M7: write admin response (fields then seq with release).
    pub fn write_admin_resp(&self, resp: &AdminResp) {
        let off = CNC_OFF_ADMIN_RESP;
        let ptr_seq = unsafe { self.region.ptr_at(off) as *const PaddedAtomicU64 };
        // Write fields first
        // SAFETY: off is CNC_OFF_ADMIN_RESP; we write a 64-byte line at that offset.
        let page_mut = unsafe { std::slice::from_raw_parts_mut(self.region.ptr_at(off), 64) };
        page_mut[8..12].copy_from_slice(&resp.status.to_le_bytes());
        page_mut[12..16].copy_from_slice(&resp.reason.to_le_bytes());
        page_mut[16..24].copy_from_slice(&resp.version.to_le_bytes());
        // Write seq last with release
        // SAFETY: ptr_seq is valid (cast from self.region.ptr_at); it's a PaddedAtomicU64.
        unsafe { (*ptr_seq).store_release(resp.seq) };
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

    /// Non-panicking `instance_id` read straight off the header bytes — a cheap
    /// two-`u64` hot-path probe for liveness / node-restart detection against a
    /// page another process may be recreating IN PLACE (M5 final review #2b/#2c).
    ///
    /// Returns `None` when the magic doesn't match — i.e. the header is being
    /// rewritten (a node restart truncates the file to zero, `set_len`s it back,
    /// then rewrites the header in place; a concurrent reader can catch that torn
    /// window as a zeroed / partial page) or the page is otherwise not a valid v2
    /// cnc page. Unlike [`Self::meta`] it NEVER panics and does NOT check the
    /// crc32 (this is a per-cycle probe, not an attach-time validation): callers
    /// treat `None` as "instance changing / unavailable" and re-probe next cycle
    /// rather than trusting a torn value. A `Some(id)` may still be a mid-write
    /// value during that window; the callers that fail-stop on a change require
    /// two CONSECUTIVE mismatching cycles so a single torn read cannot false-trip.
    pub fn try_instance_id(&self) -> Option<u128> {
        let page = self.page();
        if &page[cnc::CNC_OFF_MAGIC..cnc::CNC_OFF_MAGIC + 8] != cnc::CNC_MAGIC {
            return None;
        }
        let lo = u64::from_le_bytes(
            page[cnc::CNC_OFF_INSTANCE_LO..cnc::CNC_OFF_INSTANCE_LO + 8].try_into().ok()?,
        );
        let hi = u64::from_le_bytes(
            page[cnc::CNC_OFF_INSTANCE_HI..cnc::CNC_OFF_INSTANCE_HI + 8].try_into().ok()?,
        );
        Some(((hi as u128) << 64) | (lo as u128))
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
        assert_eq!(size_of::<SnapshotSlots>(), 192);
        assert_eq!(CNC_OFF_NODE_SNAPSHOT_FLOOR - CNC_OFF_SERVICE_SNAPSHOT_POS, 64);
        let page = CncPage::heap(&test_meta());
        let base = page.page_base_for_tests();
        assert_eq!(page.counters() as *const _ as usize - base, CNC_OFF_APPEND);
        assert_eq!(page.service() as *const _ as usize - base, CNC_OFF_SERVICE_APPLIED);
        assert_eq!(page.status() as *const _ as usize - base, CNC_OFF_TERM);
        assert_eq!(page.snapshots() as *const _ as usize - base, CNC_OFF_SERVICE_SNAPSHOT_POS);
        // M6 Task 9 observability band: archive_first_base + the 8 peer slots.
        assert_eq!(size_of::<PeerSlot>(), CNC_PEER_SLOT_STRIDE);
        assert_eq!(
            page.archive_first_base() as *const _ as usize - base,
            CNC_OFF_ARCHIVE_FIRST_BASE
        );
        for i in 0..CNC_MAX_PEER_SLOTS {
            assert_eq!(
                page.peer_slot(i) as *const _ as usize - base,
                CNC_OFF_PEER_SLOTS + i * CNC_PEER_SLOT_STRIDE,
                "peer slot {i} offset drift"
            );
        }
        // Sub-field offsets within a slot pin the packing the decoder relies on.
        let s0 = page.peer_slot(0) as *const _ as usize;
        assert_eq!(&page.peer_slot(0).id_and_role as *const _ as usize - s0, CNC_PEER_OFF_ID_AND_ROLE);
        assert_eq!(
            &page.peer_slot(0).reported_durable as *const _ as usize - s0,
            CNC_PEER_OFF_REPORTED_DURABLE
        );
        assert_eq!(
            &page.peer_slot(0).advertised_limit as *const _ as usize - s0,
            CNC_PEER_OFF_ADVERTISED_LIMIT
        );
        assert_eq!(
            &page.peer_slot(0).naks_plus_replay as *const _ as usize - s0,
            CNC_PEER_OFF_NAKS_PLUS_REPLAY
        );
        // M7: config band offsets.
        assert_eq!(CNC_OFF_CONFIG_VERSION, 3456);
        assert_eq!(CNC_OFF_CONFIG_PENDING, 3520);
        assert_eq!(CNC_OFF_ADMIN_REQ, 3584);
        assert_eq!(CNC_OFF_ADMIN_RESP, 3648);
    }

    #[test]
    fn peer_slots_pack_decode_and_are_independent() {
        use uc_protocol::v2::cnc::CNC_PEER_ROLE_LEARNER;
        let page = CncPage::heap(&test_meta());
        // Dormant by default.
        assert_eq!(page.peer_slot(3).id_and_role.load_acquire(), 0);
        // Pack round-trips.
        page.peer_slot(3).id_and_role.store_release(pack_id_and_role(7, CNC_PEER_ROLE_LEARNER));
        let raw = page.peer_slot(3).id_and_role.load_acquire();
        assert_eq!(raw >> 8, 7);
        assert_eq!((raw & 0xff) as u8, CNC_PEER_ROLE_LEARNER);
        page.peer_slot(3).naks_plus_replay.store_release(pack_naks_plus_replay(11, 42));
        let np = page.peer_slot(3).naks_plus_replay.load_acquire();
        assert_eq!((np >> 32) as u32, 11);
        assert_eq!(np as u32, 42);
        // Other slots untouched.
        assert_eq!(page.peer_slot(2).id_and_role.load_acquire(), 0);
        assert_eq!(page.peer_slot(4).id_and_role.load_acquire(), 0);
    }

    #[test]
    fn snapshot_slots_init_zero_and_are_independently_writable() {
        let page = CncPage::heap(&test_meta());
        assert_eq!(page.snapshots().service_snapshot_pos.load_acquire(), 0, "0 = no snapshot yet");
        assert_eq!(page.snapshots().node_snapshot_floor.load_acquire(), 0);
        page.snapshots().service_snapshot_pos.store_release(4096);
        assert_eq!(page.snapshots().service_snapshot_pos.load_acquire(), 4096);
        assert_eq!(page.snapshots().node_snapshot_floor.load_acquire(), 0, "independent slots");
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
        // Per-generation random base (T14): nonzero, top bit clear. Same value
        // through the shared mmap (not a fresh draw on open).
        let base = re.status().next_client_id.load_acquire();
        assert_ne!(base, 0, "0 is the reserved no-id sentinel");
        assert!(base < (1 << 31), "top bit must be clear (generation headroom)");
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
        // Base is a per-generation random value (T14), not a fixed 1; allocation
        // is still a monotone fetch_add from wherever the base landed.
        let base = page.status().next_client_id.load_acquire();
        assert_ne!(base, 0, "0 is the reserved no-id sentinel");
        assert!(base < (1 << 31), "top bit clear");
        let first = page.status().next_client_id.fetch_add(1);
        assert_eq!(first, base, "fetch_add returns the current base");
        assert_eq!(page.status().next_client_id.load_acquire(), base + 1);
    }

    #[test]
    fn client_id_base_is_generation_unique_nonzero_and_top_bit_clear() {
        // Two independently-created page generations must get different, nonzero
        // bases with the top bit clear (T14 MAJOR: generation-unique client ids
        // so an old generation's re-published (client_id, local_seq) can't
        // collide with a live client's). Statistically certain over 2^31.
        let a = CncPage::heap(&test_meta()).status().next_client_id.load_acquire();
        let b = CncPage::heap(&test_meta()).status().next_client_id.load_acquire();
        for base in [a, b] {
            assert_ne!(base, 0, "0 is the reserved no-id sentinel");
            assert_ne!(base, 1, "must NOT be the old fixed base of 1");
            assert!(base < (1 << 31), "top bit must be clear");
        }
        assert_ne!(a, b, "two generations must get different random bases");
    }

    #[test]
    fn service_epoch_bumps_with_fetch_add() {
        let page = CncPage::heap(&test_meta());
        assert_eq!(page.service().service_epoch.load_acquire(), 0);
        let prev = page.service().service_epoch.fetch_add(1);
        assert_eq!(prev, 0);
        assert_eq!(page.service().service_epoch.load_acquire(), 1);
    }

    #[test]
    fn admin_req_roundtrip_and_seq_discipline() {
        let page = CncPage::heap(&test_meta());
        // port=19100 exercises a normal value; a second req with port > 255
        // (65535) catches a width bug that a truncated/2-byte port write
        // would hide for small port numbers.
        let req = AdminReq { seq: 1, nonce: 0xDEAD_BEEF_CAFE_F00D, op: 3, id: 42, ip: 0x0A00_0001, port: 19100 };
        page.write_admin_req(&req);
        let out = page.read_admin_req(0).expect("seq 1 > last_seen 0");
        assert_eq!(out, req);

        let req2 = AdminReq { seq: 2, nonce: 1, op: 5, id: 7, ip: 0x7F00_0001, port: 65535 };
        page.write_admin_req(&req2);
        let out2 = page.read_admin_req(1).expect("seq 2 > last_seen 1");
        assert_eq!(out2, req2, "high port (>255) must round-trip through the u32-width field");

        // seq <= last_seen_seq must observe no new request.
        assert!(page.read_admin_req(2).is_none(), "seq == last_seen must yield None");
        assert!(page.read_admin_req(3).is_none(), "seq < last_seen must yield None");
    }

    #[test]
    fn admin_resp_roundtrip_and_seq_match() {
        let page = CncPage::heap(&test_meta());
        let resp = AdminResp { seq: 7, status: 1, reason: 0, version: 99 };
        page.write_admin_resp(&resp);
        let out = page.read_admin_resp(7).expect("seq matches");
        assert_eq!(out, resp);

        assert!(page.read_admin_resp(8).is_none(), "seq mismatch (too high) must yield None");
        assert!(page.read_admin_resp(6).is_none(), "seq mismatch (too low) must yield None");
    }

    #[test]
    fn config_version_and_pending_roundtrip() {
        let page = CncPage::heap(&test_meta());
        assert_eq!(page.config_version(), 0);
        assert_eq!(page.config_pending(), 0);

        page.store_config_version(42);
        assert_eq!(page.config_version(), 42);

        page.store_config_pending(true);
        assert_eq!(page.config_pending(), 1);
        page.store_config_pending(false);
        assert_eq!(page.config_pending(), 0);
    }
}
