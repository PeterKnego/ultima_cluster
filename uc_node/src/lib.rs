//! ultima_cluster engine. M1 = embedded single-node, M2 = multi-node QUIC,
//! M3 adds the shmem IPC layer (Phase 2: `ipc` module).

pub mod config;
pub mod error;
pub mod ipc;
pub mod network;
pub mod raft;
pub mod runtime;
#[cfg(feature = "test-support")]
pub mod test_support;

pub use config::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeConfig, NodeId, PeerSeed, RaftTuning,
    ServiceRingConfig, TlsConfig,
};
pub use ultima_journal::Durability;
pub use error::ClusterError;
pub use ipc::{Instance, IpcError};
pub use runtime::builder::NodeBuilder;
pub use runtime::node::NodeHandle;
