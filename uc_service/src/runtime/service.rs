//! [`ServiceBuilder`] + [`ServiceConfig`] — the public entry point for
//! standing up the service-side runtime.
//!
//! `ServiceBuilder::run()` attaches to the node's cnc.dat (Hard Rule 11
//! validated), spawns three loops (sync apply on a `std::thread`, async
//! query on a tokio task, async liveness ticker on a tokio task), and
//! returns a [`Service`] handle. The handle owns the cnc mmap so the
//! liveness task's raw pointer stays valid. Shutdown is explicit via
//! [`Service::shutdown`] — dropping without shutdown leaks the loops
//! (the std::thread keeps running, the tokio tasks too).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use memmap2::MmapMut;
use parking_lot::RwLock;
use uc_protocol::cnc::{CncHeader, ServiceStatus, service_state, sub};
use uc_protocol::ring::RingError;

use super::apply_loop::{ApplyLoopHandle, spawn_apply_loop};
use super::handshake::set_service_state;
use super::liveness::{LivenessHandle, spawn_liveness};
use super::query_loop::{QueryLoopHandle, spawn_query_loop};
use crate::state_machine::StateMachine;

/// Service-side configuration. Mirrors the IPC contract: every field maps
/// to a `cnc.dat` or ring file the runtime will read/write at attach time.
pub struct ServiceConfig {
    /// Directory containing `cnc.dat` and the per-ring files. Owned by
    /// `uc_node`; the service only attaches.
    pub instance_dir: PathBuf,

    /// Expected `app_id`. Mismatch with the `app_id` recorded in `cnc.dat`
    /// is a hard error at attach time (Hard Rule 11).
    pub app_id: String,

    /// Service-side data directory. The user's `StateMachine` decides what
    /// to put here (e.g., `ultima_db` checkpoint files when using the
    /// default `StoreStateMachine` adapter).
    pub data_dir: PathBuf,

    /// If the node's heartbeat counter doesn't advance within this window,
    /// the service considers the node dead and exits its loops with
    /// `ServiceError::NodeStalled`. Symmetric to the node-side watcher.
    pub liveness_timeout: Duration,

    /// Capacity (slot-region bytes) for the per-service apply / apply_resp
    /// rings. Must be a power of two ≥ `RECORD_ALIGN`. Currently a
    /// reference value only — the rings are created by the node side
    /// (Task 13); the service attaches to whatever the node provisioned.
    pub apply_ring_capacity_bytes: u64,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            instance_dir: PathBuf::from("/tmp/ultima-default"),
            app_id: String::new(),
            data_dir: PathBuf::from("./service-data"),
            liveness_timeout: Duration::from_secs(5),
            apply_ring_capacity_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Fluent builder. Currently parks the configuration + user state machine;
/// `run()` is unimplemented until Task 11.
pub struct ServiceBuilder<S: StateMachine> {
    pub(crate) config: ServiceConfig,
    pub(crate) state_machine: S,
}

impl<S: StateMachine> ServiceBuilder<S> {
    pub fn new(config: ServiceConfig, state_machine: S) -> Self {
        Self {
            config,
            state_machine,
        }
    }

    /// Attach a leader-only `OutputHandler`. Currently a no-op — wired in
    /// M5 along with the durable `output_progress.state` marker.
    pub fn output_handler<O>(self, _handler: O) -> Self
    where
        O: crate::output_handler::OutputHandler<S>,
    {
        self
    }

    /// Attach to the node's cnc.dat + rings, spawn the apply / query /
    /// liveness loops, and return a [`Service`] handle. The handle owns
    /// the cnc mmap; the caller must keep it alive until they call
    /// [`Service::shutdown`].
    ///
    /// The full service-side handshake (publish `ServiceReady` on the cnc
    /// `control_to_node` ring) needs an MPSC sub-mmap-region attach API
    /// that doesn't exist yet — see the M3 follow-ups. For in-process
    /// tests, setting `ServiceStatus::state = Ready` is enough for the
    /// node side to proceed.
    pub async fn run(self) -> Result<Service, ServiceError> {
        let attached = super::attach::attach(&self.config.instance_dir, &self.config.app_id)?;

        let service_status_ptr = service_status_ptr(&attached.cnc_mmap);

        // Wrap the user SM so apply (sync thread) and query (tokio task)
        // can share it. parking_lot::RwLock allows query to take a shared
        // read lock concurrently; apply takes an exclusive write lock.
        // Neither is ever held across an await.
        let sm_shared = Arc::new(RwLock::new(self.state_machine));

        let apply = spawn_apply_loop(
            Arc::clone(&sm_shared),
            attached.apply_consumer,
            attached.apply_resp_producer,
        );
        let query = spawn_query_loop(
            Arc::clone(&sm_shared),
            attached.query_consumer,
            attached.query_resp_producer,
        );
        // SAFETY: `service_status_ptr` was derived from `attached.cnc_mmap`
        // which is moved into the `Service` handle below and stays alive
        // until `Service::shutdown` joins this task.
        let liveness = unsafe { spawn_liveness(service_status_ptr) };

        // Mark ourselves ready. The node side picks this up via Acquire on
        // ServiceStatus.state. The full `ServiceReady` frame publish
        // lands when the cnc-sub-region MPSC attach API exists.
        // SAFETY: cnc mmap owned by `Service` for the loop lifetime.
        let status = unsafe { &*service_status_ptr };
        set_service_state(status, service_state::READY);

        Ok(Service {
            _cnc_mmap: attached.cnc_mmap,
            instance_id: attached.instance_id,
            node_id: attached.node_id,
            apply,
            query,
            liveness,
        })
    }
}

/// Compute the `*const ServiceStatus` from the cnc mmap header's
/// sub-buffer offset table.
fn service_status_ptr(cnc_mmap: &MmapMut) -> *const ServiceStatus {
    let base = cnc_mmap.as_ptr();
    // SAFETY: cnc_mmap was just validated by `validate_cnc`; the header
    // layout is fixed by `#[repr(C)]` and the sub_buffer_offsets entry was
    // populated by `init_cnc` to point at the ServiceStatus block.
    let header = unsafe { &*(base as *const CncHeader) };
    let offset = header.sub_buffer_offsets[sub::SERVICE_STATUS] as usize;
    // SAFETY: offset is within the mmap (cnc_file_size > offset + STATUS_BLOCK_LEN
    // by construction in init_cnc) and the bytes at that offset are a
    // zero-initialized `ServiceStatus`.
    unsafe { base.add(offset) as *const ServiceStatus }
}

/// Running-service handle. Owns the cnc mmap and the three loop handles.
/// Call [`Service::shutdown`] before drop; drop without shutdown leaks
/// the loops (the std::thread and tokio tasks keep running).
pub struct Service {
    _cnc_mmap: MmapMut,
    pub instance_id: u128,
    pub node_id: u64,
    apply: ApplyLoopHandle,
    query: QueryLoopHandle,
    liveness: LivenessHandle,
}

impl Service {
    /// Set all stop flags and join every loop. Returns once all three are
    /// done. Cnc mmap is unmapped on the drop that follows.
    pub async fn shutdown(self) -> Result<(), ServiceError> {
        self.apply.stop.store(true, Ordering::Relaxed);
        self.query.stop.store(true, Ordering::Relaxed);
        self.liveness.stop.store(true, Ordering::Relaxed);

        // Join the std::thread on a blocking task so we don't pin a tokio
        // worker.
        let apply_join = self.apply.join;
        tokio::task::spawn_blocking(move || {
            let _ = apply_join.join();
        })
        .await
        .map_err(|e| ServiceError::Ipc(format!("apply join: {e}")))?;

        let _ = self.query.join.await;
        let _ = self.liveness.join.await;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("ipc: {0}")]
    Ipc(String),
    #[error("snapshot: {0}")]
    Snapshot(#[from] crate::SnapshotError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("ring: {0}")]
    Ring(#[from] RingError),
    #[error("node stalled: no heartbeat advance within liveness_timeout")]
    NodeStalled,
    #[error("app_id mismatch: expected `{expected}`, cnc has `{actual}`")]
    AppIdMismatch { expected: String, actual: String },
    #[error("protocol version mismatch: local {local}, node {node}")]
    ProtocolVersionMismatch { local: u32, node: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoopOutput;
    use std::io::{Read, Write};
    use std::path::Path;

    use uc_protocol::cnc::{cnc_file_size, init_cnc};
    use uc_protocol::frames::apply::{
        MSG_TYPE_APPLY, MSG_TYPE_APPLY_RESP, decode_extra_apply, encode_extra_apply,
        encode_flags_apply,
    };
    use uc_protocol::frames::query::{
        MSG_TYPE_QUERY, MSG_TYPE_QUERY_RESP, QueryKind, decode_extra_query, encode_extra_query,
        encode_flags_query,
    };
    use uc_protocol::ring::spsc::SpscRing;

    /// Minimal `StateMachine` for compile-only tests — no `ultima_db`
    /// involvement so this stays valid under `--no-default-features`.
    struct NoopSm;

    impl StateMachine for NoopSm {
        type Command = ();
        type Response = ();
        type Query = ();
        type QueryResponse = ();

        fn apply(&mut self, _log_index: u64, _cmd: ()) {}
        fn query(&self, _q: ()) {}
        fn last_applied(&self) -> Option<u64> {
            None
        }
        fn build_snapshot(&self, _dst: &mut dyn Write) -> Result<u64, crate::SnapshotError> {
            Ok(0)
        }
        fn install_snapshot(&mut self, _src: &mut dyn Read) -> Result<u64, crate::SnapshotError> {
            Ok(0)
        }
    }

    /// Counter SM whose `apply` increments and returns the new value, and
    /// whose `query` reads the current value. Drives the end-to-end loop
    /// test below.
    struct CounterSm {
        value: u64,
        last_applied: Option<u64>,
    }

    impl StateMachine for CounterSm {
        type Command = u64;
        type Response = u64;
        type Query = ();
        type QueryResponse = u64;

        fn apply(&mut self, log_index: u64, delta: u64) -> u64 {
            self.value = self.value.wrapping_add(delta);
            self.last_applied = Some(log_index);
            self.value
        }
        fn query(&self, _q: ()) -> u64 {
            self.value
        }
        fn last_applied(&self) -> Option<u64> {
            self.last_applied
        }
        fn build_snapshot(&self, _dst: &mut dyn Write) -> Result<u64, crate::SnapshotError> {
            Ok(self.last_applied.unwrap_or(0))
        }
        fn install_snapshot(&mut self, _src: &mut dyn Read) -> Result<u64, crate::SnapshotError> {
            Ok(self.last_applied.unwrap_or(0))
        }
    }

    /// Mirror of the test helper in `attach.rs` — creates a complete cnc
    /// + four SPSC rings under the given dir.
    fn provision_instance(dir: &Path, app_id: &str) {
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
        init_cnc(&mut mmap[..], app_id, 1, 1).unwrap();
        drop(mmap);

        let service_dir = dir.join("service");
        std::fs::create_dir_all(&service_dir).unwrap();
        for name in [
            "apply.ring",
            "apply_resp.ring",
            "query.ring",
            "query_resp.ring",
        ] {
            SpscRing::create(&service_dir.join(name), 8192, 1024).unwrap();
        }
    }

    #[test]
    fn default_config_has_sane_fields() {
        let c = ServiceConfig::default();
        assert_eq!(c.liveness_timeout, Duration::from_secs(5));
        assert!(c.apply_ring_capacity_bytes.is_power_of_two());
        assert!(!c.instance_dir.as_os_str().is_empty());
    }

    #[test]
    fn builder_accepts_output_handler() {
        // Pure compile-test: ensure the generic `output_handler` bound
        // resolves and is chainable. `NoopOutput` is the canonical
        // do-nothing handler.
        let _builder =
            ServiceBuilder::new(ServiceConfig::default(), NoopSm).output_handler(NoopOutput);
    }

    /// End-to-end exercise of the three runtime loops. We play the role of
    /// `uc_node` by attaching to the *other* halves of the four service
    /// rings: producing onto `apply.ring` / `query.ring` and consuming
    /// from `apply_resp.ring` / `query_resp.ring`.
    #[tokio::test]
    async fn apply_query_round_trip_through_service() {
        let tmp = tempfile::tempdir().unwrap();
        provision_instance(tmp.path(), "test-app");

        // Node side: open the same ring files and grab the halves the
        // service won't take.
        let service_dir = tmp.path().join("service");
        let apply_ring = SpscRing::open(&service_dir.join("apply.ring")).unwrap();
        let apply_resp_ring = SpscRing::open(&service_dir.join("apply_resp.ring")).unwrap();
        let query_ring = SpscRing::open(&service_dir.join("query.ring")).unwrap();
        let query_resp_ring = SpscRing::open(&service_dir.join("query_resp.ring")).unwrap();
        let (mut apply_producer, _) = apply_ring.into_split();
        let (_, mut apply_resp_consumer) = apply_resp_ring.into_split();
        let (mut query_producer, _) = query_ring.into_split();
        let (_, mut query_resp_consumer) = query_resp_ring.into_split();

        let cfg = ServiceConfig {
            instance_dir: tmp.path().to_path_buf(),
            app_id: "test-app".to_string(),
            ..ServiceConfig::default()
        };
        let sm = CounterSm {
            value: 0,
            last_applied: None,
        };
        let service = ServiceBuilder::new(cfg, sm).run().await.expect("run");

        // ── apply round-trip: send delta=5 at log_index=1 ────────────────
        let cmd_bytes = bincode::serde::encode_to_vec(5u64, bincode::config::standard()).unwrap();
        apply_producer
            .try_write(
                MSG_TYPE_APPLY,
                encode_flags_apply(0),
                encode_extra_apply(1),
                &cmd_bytes,
            )
            .expect("apply write");

        let resp_rec = poll_blocking_read(&mut apply_resp_consumer).await;
        assert_eq!(resp_rec.0.msg_type, MSG_TYPE_APPLY_RESP);
        assert_eq!(decode_extra_apply(resp_rec.0.header_extra), 1);
        let (resp_value, _): (u64, _) =
            bincode::serde::decode_from_slice(&resp_rec.1, bincode::config::standard()).unwrap();
        assert_eq!(resp_value, 5);

        // Apply again at log_index=2 to confirm the loop keeps draining.
        let cmd_bytes = bincode::serde::encode_to_vec(3u64, bincode::config::standard()).unwrap();
        apply_producer
            .try_write(
                MSG_TYPE_APPLY,
                encode_flags_apply(0),
                encode_extra_apply(2),
                &cmd_bytes,
            )
            .expect("apply write");
        let resp_rec = poll_blocking_read(&mut apply_resp_consumer).await;
        let (resp_value, _): (u64, _) =
            bincode::serde::decode_from_slice(&resp_rec.1, bincode::config::standard()).unwrap();
        assert_eq!(resp_value, 8);

        // ── query round-trip: ask for current counter ────────────────────
        let q_bytes = bincode::serde::encode_to_vec((), bincode::config::standard()).unwrap();
        query_producer
            .try_write(
                MSG_TYPE_QUERY,
                encode_flags_query(0),
                encode_extra_query(42, QueryKind::Snapshot),
                &q_bytes,
            )
            .expect("query write");

        let resp_rec = poll_blocking_read(&mut query_resp_consumer).await;
        assert_eq!(resp_rec.0.msg_type, MSG_TYPE_QUERY_RESP);
        let (req_id, kind) = decode_extra_query(resp_rec.0.header_extra).unwrap();
        assert_eq!(req_id, 42);
        assert_eq!(kind, QueryKind::Snapshot);
        let (qresp, _): (u64, _) =
            bincode::serde::decode_from_slice(&resp_rec.1, bincode::config::standard()).unwrap();
        assert_eq!(qresp, 8);

        service.shutdown().await.expect("shutdown");
    }

    /// Poll a consumer until a record arrives. Tokio-flavored busy-poll.
    async fn poll_blocking_read(
        consumer: &mut uc_protocol::ring::spsc::SpscConsumer,
    ) -> (uc_protocol::ring::common::RecordHeader, Vec<u8>) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut buf = Vec::new();
        while std::time::Instant::now() < deadline {
            match consumer.try_read(&mut buf) {
                Ok(Some(rec)) => return (rec, buf),
                Ok(None) => tokio::time::sleep(Duration::from_millis(2)).await,
                Err(e) => panic!("consumer error: {e}"),
            }
        }
        panic!("timed out waiting for record");
    }
}
