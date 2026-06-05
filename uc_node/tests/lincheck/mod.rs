//! Test-only linearizability harness: pure model/history/checker + a 3-node
//! shmem fault cluster. See
//! docs/superpowers/specs/2026-06-05-linearizability-harness-design.md.
pub mod checker;
pub mod cluster;
pub mod history;
pub mod model;
pub mod register_sm;
