//! ultima_cluster engine. M1 = embedded single-node.

pub mod config;
pub mod error;
// pub mod raft;       // enabled in Task 6
// pub mod runtime;    // enabled in Task 12

pub use config::{BootstrapConfig, NodeConfig, NodeId, PeerSeed, RaftTuning};
pub use error::ClusterError;
// pub use runtime::builder::NodeBuilder;     // enabled in Task 12
// pub use runtime::node::NodeHandle;         // enabled in Task 12
