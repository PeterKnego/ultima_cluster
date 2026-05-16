//! openraft TypeConfig and supporting types for ultima_cluster.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

pub mod log_storage; // Task 7
pub mod state_machine; // M1/M2: embedded apply
pub mod state_machine_shmem; // M3: apply via shmem ring

pub type NodeId = u64;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeAddr {
    pub raft_addr: SocketAddr,
    /// Reserved for M2: client-facing address.
    /// Always None in M1 (no QUIC, no remote clients yet).
    #[serde(default)]
    pub client_addr: Option<SocketAddr>,
}

// openraft's `Node` supertrait requires `Default`. `SocketAddr` itself has no `Default`,
// so we provide one that resolves to `0.0.0.0:0` — a placeholder used only when openraft
// constructs a stand-in Node value (e.g. for an unknown peer in metrics). Real node entries
// always carry a populated address from `BootstrapConfig` / membership changes.
impl Default for NodeAddr {
    fn default() -> Self {
        Self {
            raft_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
            client_addr: None,
        }
    }
}

/// User-Command bytes, refcounted for zero-copy flow through the apply pipeline.
///
/// Newtype over `bytes::Bytes` so that openraft 0.10's `AppData: Display` bound is
/// satisfied.  `Deref<Target = Bytes>` / `From<Bytes>` / `Into<Bytes>` keep it
/// transparent in the apply pipeline.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct AppCommand(pub Bytes);

impl std::fmt::Display for AppCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{} bytes>", self.0.len())
    }
}

impl std::ops::Deref for AppCommand {
    type Target = Bytes;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Bytes> for AppCommand {
    fn from(b: Bytes) -> Self {
        Self(b)
    }
}

impl From<AppCommand> for Bytes {
    fn from(c: AppCommand) -> Self {
        c.0
    }
}

/// User-Response bytes, also refcounted.
pub type AppResponse = Bytes;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = AppCommand,
        R = AppResponse,
        NodeId = NodeId,
        Node = NodeAddr,
        Term = u64,
        LeaderId = openraft::impls::leader_id_adv::LeaderId<Self::Term, Self::NodeId>,
        Vote = openraft::impls::Vote<Self::LeaderId>,
        Entry = openraft::Entry<<Self::LeaderId as openraft::vote::RaftLeaderId>::Committed, Self::D, Self::NodeId, Self::Node>,
        SnapshotData = std::io::Cursor<Vec<u8>>, // M1: in-memory; M5 swaps to snapshot.region reader
        AsyncRuntime = openraft::impls::TokioRuntime,
);

// ---------------------------------------------------------------------------
// Shared concrete type aliases for openraft 0.10 with our TypeConfig.
//
// TypeConfig uses leader_id_adv, where CommittedLeaderId == LeaderId.
// Defined here once; imported via `use super::*` or named imports in
// log_storage.rs, state_machine.rs, state_machine_shmem.rs.
// ---------------------------------------------------------------------------

pub(crate) type LeaderId = openraft::impls::leader_id_adv::LeaderId<u64, u64>;
pub(crate) type RaftLogId = openraft::LogId<LeaderId>;
pub(crate) type RaftVote = openraft::Vote<LeaderId>;
pub(crate) type RaftStoredMembership = openraft::StoredMembership<LeaderId, NodeId, NodeAddr>;
pub(crate) type RaftSnapshotMeta = openraft::storage::SnapshotMeta<LeaderId, NodeId, NodeAddr>;
pub(crate) type RaftSnapshot =
    openraft::storage::Snapshot<LeaderId, NodeId, NodeAddr, std::io::Cursor<Vec<u8>>>;
