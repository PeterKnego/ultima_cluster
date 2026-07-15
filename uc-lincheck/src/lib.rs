//! Reusable WGL linearizability harness: the CAS-register sequential model, an
//! operation-history recorder, the linearizability checker, and the in-memory
//! `RegisterSm` (a `uc2_service::StateMachine`). Used by the in-process lincheck
//! capstone (`uc2_node` tests) and the multi-process hard-crash test
//! (`examples/uc2-crashtest`).
pub mod checker;
pub mod edn;
pub mod history;
pub mod list_append;
pub mod model;
pub mod register;
