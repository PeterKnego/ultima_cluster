//! Service-side SDK for ultima_cluster.
//!
//! M1 ships the trait surface only. ServiceBuilder + shmem runtime + ultima_db
//! adapter arrive in M3.

pub mod error;
pub mod output_handler;
pub mod state_machine;

pub use error::{OutputError, SnapshotError};
pub use output_handler::{NoopOutput, OutputHandler};
pub use state_machine::StateMachine;
