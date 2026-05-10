//! ultima_cluster engine. M1 = embedded single-node.

pub mod config;
pub mod error;
pub mod network;
pub mod raft;
pub mod runtime;

pub use config::{BootstrapConfig, NodeConfig, NodeId, PeerSeed, RaftTuning, TlsConfig};
pub use error::ClusterError;
pub use runtime::builder::NodeBuilder;
pub use runtime::node::NodeHandle;
