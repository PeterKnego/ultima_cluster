//! Local-shmem client SDK for ultima_cluster.
//!
//! M1 ships the error type only. The Client struct + submit/query API arrive in M4.

pub mod error;
pub use error::ClientError;
