//! Service-side handshake: write `ServiceReady { last_applied }` to the
//! `control_to_node` ring and transition `ServiceStatus::state` from
//! `Handshaking` to `Ready`.
//!
//! Filled in by M3 Task 10.
