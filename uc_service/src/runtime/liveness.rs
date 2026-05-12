//! Service-side heartbeat producer + node-liveness watcher.
//!
//! Ticks `ServiceStatus::heartbeat_seq` + `heartbeat_at_ns` on a fixed
//! cadence (default 100 ms) via `uc_protocol::liveness::tick_service`.
//! In parallel, watches `NodeStatus::heartbeat_seq` via
//! `HeartbeatWatcher` and exits the runtime with
//! `ServiceError::NodeStalled` if the counter doesn't advance within
//! `ServiceConfig::liveness_timeout`.
//!
//! Filled in by M3 Task 11.
