//! Service-side SDK for ultima_cluster.
//!
//! M1 shipped the trait surface only. M3 adds the `ultima_db` adapter
//! (default-on Cargo feature `ultima_db`); the shmem runtime / ServiceBuilder
//! arrive later in M3 Phase 2.

pub mod error;
pub mod output_handler;
pub mod runtime;
pub mod state_machine;

#[cfg(feature = "ultima_db")]
pub mod ultima_db;

pub use error::{OutputError, SnapshotError};
pub use output_handler::{NoopOutput, OutputHandler};
pub use runtime::{Service, ServiceBuilder, ServiceConfig, ServiceError};
pub use state_machine::StateMachine;
