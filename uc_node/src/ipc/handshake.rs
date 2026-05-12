//! Node-side handshake watcher: reads `ServiceReady` from the cnc
//! `control_to_node` MPSC ring, transitions `NodeStatus::role` /
//! publishes `RoleChanged` on `control_to_service` when raft state
//! changes.
//!
//! Filled in by M3 Task 14.
