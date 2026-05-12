//! Sync apply loop. Runs on a dedicated `std::thread` (NOT a tokio task)
//! because `StateMachine::apply` is sync and CPU-bound; we don't want to
//! block a tokio worker. Drains `service/apply.ring`, invokes
//! `state_machine.apply(log_index, cmd)`, publishes `ApplyRespFrame` into
//! `service/apply_resp.ring`.
//!
//! Filled in by M3 Task 11.
