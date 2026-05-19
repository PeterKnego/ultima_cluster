//! Service-side attach: open the node's `cnc.dat`, validate the
//! `(app_id, protocol_version)` pair (Hard Rule 11), and open the four
//! SPSC ring files the node provisioned under `<instance_dir>/service/`.
//!
//! Layout:
//!
//! ```text
//! <instance_dir>/cnc.dat
//! <instance_dir>/service/apply.ring         # node→service  (we consume)
//! <instance_dir>/service/apply_resp.ring    # service→node  (we produce)
//! <instance_dir>/service/query.ring         # node→service  (we consume)
//! <instance_dir>/service/query_resp.ring    # service→node  (we produce)
//! <instance_dir>/service/output.ring        # node→service  (we consume)
//! <instance_dir>/service/output_resp.ring   # service→node  (we produce)
//! ```
//!
//! The `instance_id` check (the third leg of Hard Rule 11) is the service's
//! responsibility once it has stored the value from a previous attach — for
//! the *initial* attach there is nothing to compare against, so we just
//! record what the cnc says and let the heartbeat watcher pick up an
//! instance-id change as a hard restart later. The `_cnc_mmap` field keeps
//! the cnc mapping alive so `NodeStatus` / `ServiceStatus` reads stay valid.

use std::path::Path;

use memmap2::MmapMut;
use uc_protocol::ProtocolVersion;
use uc_protocol::cnc::validate_cnc;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer, SpscRing};
use uc_protocol::version;

use super::service::ServiceError;

/// Everything the service needs to drive its loops after a successful
/// handshake. The four ring halves are the ones the service actually
/// reads/writes; the unused halves of each ring (e.g. the producer half of
/// `apply.ring`) live on the node side. The `Arc<Inner>` inside each SPSC
/// half keeps the ring mmap alive for as long as we hold it.
pub struct AttachedRings {
    /// Keeps the cnc.dat mapping alive so `NodeStatus` / `ServiceStatus`
    /// reads through the header's sub-buffer offsets stay valid for the
    /// service's lifetime.
    pub cnc_mmap: MmapMut,
    pub instance_id: u128,
    pub node_id: u64,
    pub apply_consumer: SpscConsumer,
    pub apply_resp_producer: SpscProducer,
    pub query_consumer: SpscConsumer,
    pub query_resp_producer: SpscProducer,
    pub output_consumer: SpscConsumer,
    pub output_resp_producer: SpscProducer,
}

/// Open the node's `cnc.dat`, validate `(app_id, protocol_version)`, and
/// attach to the four service-side SPSC ring files.
pub fn attach(instance_dir: &Path, expected_app_id: &str) -> Result<AttachedRings, ServiceError> {
    let cnc_path = instance_dir.join("cnc.dat");
    let cnc_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&cnc_path)?;
    // SAFETY: standard mmap of a file we just opened R/W. The mapping is
    // exclusive in the process sense; cross-process synchronization with the
    // node side is via the atomics in `NodeStatus` / `ServiceStatus` (read
    // through the header's sub_buffer_offsets, not raw pointer arithmetic).
    let cnc_mmap = unsafe { MmapMut::map_mut(&cnc_file)? };
    let header = validate_cnc(&cnc_mmap[..])
        .map_err(|e| ServiceError::Ipc(format!("validate cnc {}: {e}", cnc_path.display())))?;

    let actual_app_id = header.app_id_str();
    if actual_app_id != expected_app_id {
        return Err(ServiceError::AppIdMismatch {
            expected: expected_app_id.to_string(),
            actual: actual_app_id.to_string(),
        });
    }

    let local = version::CURRENT;
    let peer = ProtocolVersion(header.protocol_version);
    if !local.compatible_with(peer) {
        return Err(ServiceError::ProtocolVersionMismatch {
            local: local.0,
            node: peer.0,
        });
    }

    let instance_id = header.instance_id();
    let node_id = header.node_id;

    let service_dir = instance_dir.join("service");
    let apply_ring = SpscRing::open(&service_dir.join("apply.ring"))?;
    let apply_resp_ring = SpscRing::open(&service_dir.join("apply_resp.ring"))?;
    let query_ring = SpscRing::open(&service_dir.join("query.ring"))?;
    let query_resp_ring = SpscRing::open(&service_dir.join("query_resp.ring"))?;
    let output_ring = SpscRing::open(&service_dir.join("output.ring"))?;
    let output_resp_ring = SpscRing::open(&service_dir.join("output_resp.ring"))?;

    // For each ring we only need one half; the other half's `Arc<Inner>` is
    // dropped immediately and the kept half keeps the underlying mmap alive.
    let (_, apply_consumer) = apply_ring.into_split();
    let (apply_resp_producer, _) = apply_resp_ring.into_split();
    let (_, query_consumer) = query_ring.into_split();
    let (query_resp_producer, _) = query_resp_ring.into_split();
    let (_, output_consumer) = output_ring.into_split();
    let (output_resp_producer, _) = output_resp_ring.into_split();

    Ok(AttachedRings {
        cnc_mmap,
        instance_id,
        node_id,
        apply_consumer,
        apply_resp_producer,
        query_consumer,
        query_resp_producer,
        output_consumer,
        output_resp_producer,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::cnc::{cnc_file_size, init_cnc};
    use uc_protocol::ring::spsc::SpscRing;

    /// Build a complete cnc.dat + four service rings under a fresh tempdir,
    /// matching what the node-side `ipc::instance` will create in M3 Task 12.
    fn provision_instance(dir: &Path, app_id: &str, node_id: u64, instance_id: u128) {
        // cnc.dat
        let cnc_path = dir.join("cnc.dat");
        let cnc_size = cnc_file_size();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&cnc_path)
            .unwrap();
        f.set_len(cnc_size as u64).unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&f).unwrap() };
        init_cnc(&mut mmap[..], app_id, node_id, instance_id).unwrap();
        mmap.flush().unwrap();
        drop(mmap);

        // service/ rings
        let service_dir = dir.join("service");
        std::fs::create_dir_all(&service_dir).unwrap();
        const CAP: u64 = 4096;
        const MAX: u32 = 1024;
        for name in [
            "apply.ring",
            "apply_resp.ring",
            "query.ring",
            "query_resp.ring",
            "output.ring",
            "output_resp.ring",
        ] {
            SpscRing::create(&service_dir.join(name), CAP, MAX).unwrap();
        }
    }

    #[test]
    fn attach_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        provision_instance(tmp.path(), "kv-cluster", 7, 0xdead_beef_cafe_babe);

        let rings = attach(tmp.path(), "kv-cluster").expect("attach");
        assert_eq!(rings.node_id, 7);
        assert_eq!(rings.instance_id, 0xdead_beef_cafe_babe);
    }

    #[test]
    fn attach_rejects_app_id_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        provision_instance(tmp.path(), "kv-cluster", 1, 1);

        let r = attach(tmp.path(), "wrong-app");
        assert!(matches!(
            r,
            Err(ServiceError::AppIdMismatch { expected, actual })
                if expected == "wrong-app" && actual == "kv-cluster"
        ));
    }

    #[test]
    fn attach_rejects_missing_cnc() {
        let tmp = tempfile::tempdir().unwrap();
        // No cnc.dat in the directory.
        let r = attach(tmp.path(), "any");
        assert!(matches!(r, Err(ServiceError::Io(_))));
    }

    #[test]
    fn attach_rejects_missing_ring_file() {
        let tmp = tempfile::tempdir().unwrap();
        // cnc only — no service/ subdir or rings.
        let cnc_path = tmp.path().join("cnc.dat");
        let cnc_size = cnc_file_size();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&cnc_path)
            .unwrap();
        f.set_len(cnc_size as u64).unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&f).unwrap() };
        init_cnc(&mut mmap[..], "app", 1, 1).unwrap();
        drop(mmap);

        let r = attach(tmp.path(), "app");
        // Missing ring file surfaces as RingError::Io → ServiceError::Ring.
        assert!(matches!(r, Err(ServiceError::Ring(_))));
    }
}
