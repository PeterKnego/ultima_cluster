//! Test-only linearizability harness: pure model/history/checker + a 3-node
//! shmem fault cluster. See docs/tasks/task12_linearizability_harness.md.
pub mod checker;
pub mod cluster;
pub mod history;
pub mod model;
pub mod register_sm;
