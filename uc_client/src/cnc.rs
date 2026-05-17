//! Attach to `<instance_dir>/cnc.dat` + handshake.
//!
//! Opened read-write so the `next_client_id` atomic counter (written by
//! clients) is accessible. All header fields are treated as read-only after
//! `validate_cnc` returns; only the `next_client_id` sub-region is mutated.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::MmapMut;
use uc_protocol::ProtocolVersion;
use uc_protocol::cnc::{
    CncHeader, NodeStatus, ServiceStatus, next_client_id_ptr, sub, validate_cnc,
};

use crate::ClientError;

pub struct CncAttach {
    pub _mmap: MmapMut,
    pub base: *const u8,
    pub instance_id: u128,
    pub app_id: String,
    pub protocol_version: u32,
    pub client_id: u32,
}

// SAFETY: cnc.dat is shared read-only; all writes happen via atomics or
// during the node's exclusive init. Read access from any thread is safe.
unsafe impl Send for CncAttach {}
unsafe impl Sync for CncAttach {}

impl CncAttach {
    pub fn attach(instance_dir: &Path, expected_app_id: &str) -> Result<Self, ClientError> {
        let cnc_path = instance_dir.join("cnc.dat");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&cnc_path)?;
        // SAFETY: read-write mmap; we only write atomically to the
        // `next_client_id` sub-region. All header fields are treated as
        // read-only after validate_cnc returns.
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let header = validate_cnc(&mmap)
            .map_err(|e| ClientError::Decode(format!("cnc validate: {e}")))?;
        let actual_app_id = header.app_id_str().to_owned();
        if actual_app_id != expected_app_id {
            return Err(ClientError::AppIdMismatch {
                expected: expected_app_id.to_owned(),
                actual: actual_app_id,
            });
        }
        let local_version = ProtocolVersion::new(0, 1, 0).0;
        if header.protocol_version != local_version {
            return Err(ClientError::ProtocolMismatch {
                local: local_version,
                node: header.protocol_version,
            });
        }
        let instance_id = header.instance_id();
        let protocol_version = header.protocol_version;

        // SAFETY: cnc validated above.
        let counter = unsafe { &*(next_client_id_ptr(mmap.as_ptr()) as *const AtomicU64) };
        let raw = counter.fetch_add(1, Ordering::Relaxed);
        let client_id = raw as u32;

        let base = mmap.as_ptr();
        Ok(CncAttach {
            _mmap: mmap,
            base,
            instance_id,
            app_id: actual_app_id,
            protocol_version,
            client_id,
        })
    }

    pub fn node_status(&self) -> *const NodeStatus {
        // SAFETY: header validated; sub-buffer offsets are within the mmap.
        unsafe {
            let header = &*self.base.cast::<CncHeader>();
            self.base.add(header.sub_buffer_offsets[sub::NODE_STATUS] as usize)
                as *const NodeStatus
        }
    }

    pub fn service_status(&self) -> *const ServiceStatus {
        // SAFETY: same as node_status.
        unsafe {
            let header = &*self.base.cast::<CncHeader>();
            self.base
                .add(header.sub_buffer_offsets[sub::SERVICE_STATUS] as usize)
                as *const ServiceStatus
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::cnc::{cnc_file_size, init_cnc};

    fn fresh_cnc(dir: &std::path::Path, app_id: &str, node_id: u64, instance_id: u128) {
        let cnc_path = dir.join("cnc.dat");
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&cnc_path)
            .unwrap();
        f.set_len(cnc_file_size() as u64).unwrap();
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&f).unwrap() };
        init_cnc(&mut mmap[..], app_id, node_id, instance_id).unwrap();
    }

    #[test]
    fn attach_succeeds_and_allocates_distinct_ids() {
        let tmp = tempfile::tempdir().unwrap();
        fresh_cnc(tmp.path(), "kv", 1, 0xdead_beef);
        let a = CncAttach::attach(tmp.path(), "kv").expect("attach a");
        let b = CncAttach::attach(tmp.path(), "kv").expect("attach b");
        assert_ne!(a.client_id, b.client_id);
        assert_eq!(a.instance_id, 0xdead_beef);
    }

    #[test]
    fn attach_rejects_wrong_app_id() {
        let tmp = tempfile::tempdir().unwrap();
        fresh_cnc(tmp.path(), "kv", 1, 0);
        let r = CncAttach::attach(tmp.path(), "other");
        assert!(matches!(r, Err(ClientError::AppIdMismatch { .. })));
    }
}
