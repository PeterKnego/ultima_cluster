//! Local-shmem client SDK for ultima_cluster (M4).
//!
//! Connects to a `uc_node` over the `<instance_dir>/cnc.dat` +
//! `clients/*` shmem rings. The public entry point is [`Client`].

pub mod error;
pub use error::ClientError;

mod client;
mod cnc;
mod rings;
mod session;
mod watchers;

pub use client::Client;
