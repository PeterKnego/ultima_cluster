//! Service-side runtime — attaches to a `uc_node`'s `cnc.dat` + service rings.
//!
//! The service process runs the user's `StateMachine` (deterministic apply +
//! query) and `OutputHandler` (async, leader-only). It does not own the raft
//! log, does not talk QUIC, and does not hold the instance directory lock —
//! `uc_node` does all of that. The runtime here only:
//!   1. attaches to the existing cnc.dat + apply/query rings (Task 10);
//!   2. runs a sync apply loop on its own thread (Task 11);
//!   3. runs an async query loop + heartbeat producer on tokio tasks
//!      (Tasks 11 + 14).
//!
//! The IPC layout is the canonical `uc_protocol` wire format. The runtime
//! enforces the `(app_id, instance_id, protocol_version)` trio at attach
//! time (Hard Rule 11).

pub mod apply_loop;
pub mod attach;
pub mod handshake;
pub mod liveness;
pub mod output_loop;
pub mod query_loop;
pub mod service;

pub use service::{Service, ServiceBuilder, ServiceConfig, ServiceError};
