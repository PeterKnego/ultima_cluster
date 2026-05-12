//! [`ServiceBuilder`] + [`ServiceConfig`] — the public entry point for
//! standing up the service-side runtime.
//!
//! Only the configuration surface lands in M3 Task 9. The real `run()` body
//! arrives in Task 11 once the attach + apply/query/heartbeat loops are in.

use std::path::PathBuf;
use std::time::Duration;

use uc_protocol::ring::RingError;

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

    /// Run the service until the node exits or a fatal error is observed.
    /// Implementation lands in Task 11.
    pub async fn run(self) -> Result<Service, ServiceError> {
        let _ = self.config;
        let _ = self.state_machine;
        unimplemented!("ServiceBuilder::run lands in M3 Task 11")
    }
}

/// Opaque handle to a running service. The handle's destructor signals the
/// runtime to shut down its loops. Currently a placeholder; real shutdown
/// plumbing lands in Task 11.
pub struct Service {
    _opaque: (),
}

impl Service {
    /// Construct a placeholder handle. Hidden behind `#[doc(hidden)]` until
    /// Task 11 turns this into a real lifecycle owner.
    #[doc(hidden)]
    pub fn __placeholder() -> Self {
        Self { _opaque: () }
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
}
