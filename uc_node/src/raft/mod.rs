//! openraft TypeConfig and supporting types for ultima_cluster.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

pub mod log_storage; // Task 7
pub mod state_machine; // Task 11

pub type NodeId = u64;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeAddr {
    pub raft_addr: SocketAddr,
}

// openraft's `Node` supertrait requires `Default`. `SocketAddr` itself has no `Default`,
// so we provide one that resolves to `0.0.0.0:0` — a placeholder used only when openraft
// constructs a stand-in Node value (e.g. for an unknown peer in metrics). Real node entries
// always carry a populated address from `BootstrapConfig` / membership changes.
impl Default for NodeAddr {
    fn default() -> Self {
        Self {
            raft_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
        }
    }
}

/// User-Command bytes, refcounted for zero-copy flow through the apply pipeline.
pub type AppCommand = Bytes;

/// User-Response bytes, also refcounted.
pub type AppResponse = Bytes;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = AppCommand,
        R = AppResponse,
        NodeId = NodeId,
        Node = NodeAddr,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,    // M1: in-memory; M5 swaps to snapshot.region reader
        AsyncRuntime = openraft::TokioRuntime,
);
