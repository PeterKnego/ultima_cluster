//! Async query loop. Drains `service/query.ring`, calls
//! `state_machine.query(q)` (sync, but typically fast), publishes
//! `QueryRespFrame` into `service/query_resp.ring`. Runs as a tokio task.
//!
//! Filled in by M3 Task 11.
