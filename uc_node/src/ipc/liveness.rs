//! Node-side heartbeat producer + service-liveness watcher.
//!
//! Ticks `NodeStatus::{heartbeat_seq, heartbeat_at_ns}` every 100 ms and
//! watches `ServiceStatus::heartbeat_seq` for stalls (triggers leader
//! transfer if we're the leader, otherwise voluntary down-graceful).
//!
//! Filled in by M3 Task 14.
