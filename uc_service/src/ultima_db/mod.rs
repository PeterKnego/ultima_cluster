//! Canonical adapter from [`crate::StateMachine`] to [`ultima_db::Store`].
//!
//! Most ultima_cluster deployments will use `ultima_db` as the user state
//! store, so this adapter is the default path: gated on the `ultima_db`
//! Cargo feature (default-on). Users with non-`ultima_db` state implement
//! [`crate::StateMachine`] directly and disable the feature.
//!
//! See the module-level docs on [`store_state_machine`] for the apply /
//! query / snapshot contract.

pub mod builder;
pub mod store_state_machine;

pub use builder::{BuildError, StoreStateMachineBuilder};
pub use store_state_machine::StoreStateMachine;
