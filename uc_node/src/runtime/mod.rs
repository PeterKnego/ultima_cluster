//! Runtime glue for ultima_cluster: NodeBuilder<S> + NodeHandle<S>.
//!
//! M1 wires JournalLogStorage + AdaptedStateMachine<S> + a no-op network
//! placeholder into an `openraft::Raft<TypeConfig>`, applies bootstrap, and
//! returns a NodeHandle exposing `submit`, `current_leader`, `node_id`,
//! `shutdown`. M2 swaps the no-op network for the real QUIC transport.

pub mod builder;
pub mod node;
pub mod output_dispatcher;
pub mod output_replay;
pub(crate) mod reconstruct;
pub mod recovery;
