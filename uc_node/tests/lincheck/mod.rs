//! Test-only linearizability harness: pure model/history/checker + a 3-node
//! shmem fault cluster. See
//! docs/superpowers/specs/2026-06-05-linearizability-harness-design.md.
pub mod model;
pub mod history;
pub mod checker;
pub mod register_sm;
