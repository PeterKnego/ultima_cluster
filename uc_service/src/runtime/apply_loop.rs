//! Sync apply loop.
//!
//! Runs on a dedicated `std::thread` (NOT a tokio task) because
//! [`crate::StateMachine::apply`] is sync and may be CPU-bound; we don't
//! want to block a tokio worker. Drains `service/apply.ring`, invokes
//! `state_machine.apply(log_index, cmd)` while holding the SM write lock, then
//! publishes `ApplyRespFrame` into `service/apply_resp.ring`.
//!
//! Using `tokio::sync::RwLock` (instead of `Mutex`) allows output_loop to
//! hold a `Send` read guard across the on_committed await while apply owns
//! the exclusive write lock. Apply acquires via `blocking_write()` since it
//! runs on a plain `std::thread` outside any tokio runtime.
//!
//! Shutdown: caller sets [`ApplyLoopHandle::stop`] and joins.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use bincode::config::standard as bincode_standard;
use tokio::sync::RwLock;
use uc_protocol::frames::apply::{
    MSG_TYPE_APPLY, MSG_TYPE_APPLY_RESP, decode_extra_apply, decode_flags_apply,
    encode_extra_apply, encode_flags_apply,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};
use uc_protocol::ring::SPIN_TRIES;

use crate::StateMachine;

const ERROR_BACKOFF: Duration = Duration::from_millis(10);

/// Parse the `UC_APPLY_SPIN_BUDGET` value into a spin budget for the apply
/// consumer. Pure (testable): `None`/unparseable -> default `SPIN_TRIES`;
/// `busy`/`max` (case-insensitive) -> `u32::MAX` (pure busy-spin); `<N>` -> N.
fn parse_spin_budget(v: Option<&str>) -> u32 {
    match v {
        Some(s) if s.trim().eq_ignore_ascii_case("busy") || s.trim().eq_ignore_ascii_case("max") => {
            u32::MAX
        }
        Some(s) => s.trim().parse::<u32>().unwrap_or(SPIN_TRIES),
        None => SPIN_TRIES,
    }
}

fn apply_spin_budget() -> u32 {
    parse_spin_budget(std::env::var("UC_APPLY_SPIN_BUDGET").ok().as_deref())
}

pub struct ApplyLoopHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn_apply_loop<S>(
    sm: Arc<RwLock<S>>,
    mut consumer: SpscConsumer,
    mut resp_producer: SpscProducer,
) -> ApplyLoopHandle
where
    S: StateMachine,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_thread = Arc::clone(&stop);
    let budget = apply_spin_budget();
    consumer.set_spin_budget(budget);
    if budget == u32::MAX {
        tracing::info!("apply consumer: busy-spin mode (UC_APPLY_SPIN_BUDGET=busy)");
    } else if budget != SPIN_TRIES {
        tracing::info!(spin_budget = budget, "apply consumer: custom spin budget");
    }
    let join = std::thread::Builder::new()
        .name("uc-service-apply".into())
        .spawn(move || {
            apply_thread_body::<S>(sm, &mut consumer, &mut resp_producer, stop_for_thread)
        })
        .expect("spawn apply thread");
    ApplyLoopHandle { join, stop }
}

fn apply_thread_body<S>(
    sm: Arc<RwLock<S>>,
    consumer: &mut SpscConsumer,
    resp_producer: &mut SpscProducer,
    stop: Arc<AtomicBool>,
) where
    S: StateMachine,
{
    let mut payload_buf: Vec<u8> = Vec::with_capacity(4096);
    while !stop.load(Ordering::Relaxed) {
        match consumer.read_or_park(&mut payload_buf) {
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_APPLY => {
                if let Err(e) = decode_flags_apply(rec.flags) {
                    tracing::warn!("dropping apply frame with bad flags: {e}");
                    continue;
                }
                let log_index = decode_extra_apply(rec.header_extra);
                uc_protocol::probes::stamp_log(
                    log_index,
                    uc_protocol::probes::Checkpoint::ApplyStart,
                );
                let (cmd, _) = match bincode::serde::decode_from_slice::<S::Command, _>(
                    &payload_buf,
                    bincode_standard(),
                ) {
                    Ok(t) => t,
                    Err(e) => {
                        // Producer is the node within the same trust
                        // boundary; a decode failure means the wire format
                        // diverged. Surface loudly — the service can't
                        // safely continue applying.
                        tracing::error!(error = %e, log_index, "apply decode failed");
                        panic!("apply decode: {e}");
                    }
                };
                let resp = {
                    let mut guard = sm.blocking_write();
                    guard.apply(log_index, cmd)
                };
                uc_protocol::probes::stamp_log(
                    log_index,
                    uc_protocol::probes::Checkpoint::ApplyDone,
                );
                let resp_bytes = bincode::serde::encode_to_vec(&resp, bincode_standard())
                    .expect("apply response encode");
                publish_response(resp_producer, log_index, &resp_bytes, &stop);
            }
            Ok(Some(rec)) => {
                tracing::warn!(msg_type = rec.msg_type, "apply ring: unexpected frame");
            }
            Ok(None) => {} // parked up to PARK_CEIL; loop re-checks stop flag
            Err(e) => {
                tracing::warn!(error = %e, "apply ring read error");
                std::thread::sleep(ERROR_BACKOFF);
            }
        }
    }
}

fn publish_response(
    producer: &mut SpscProducer,
    log_index: u64,
    resp_bytes: &[u8],
    stop: &AtomicBool,
) {
    loop {
        match producer.try_write(
            MSG_TYPE_APPLY_RESP,
            encode_flags_apply(0),
            encode_extra_apply(log_index),
            resp_bytes,
        ) {
            Ok(()) => return,
            Err(RingError::Full) => {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::yield_now();
            }
            Err(e) => panic!("apply_resp write at log_index={log_index}: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spin_budget_parsing() {
        assert_eq!(parse_spin_budget(None), SPIN_TRIES);
        assert_eq!(parse_spin_budget(Some("busy")), u32::MAX);
        assert_eq!(parse_spin_budget(Some("BUSY")), u32::MAX);
        assert_eq!(parse_spin_budget(Some("max")), u32::MAX);
        assert_eq!(parse_spin_budget(Some(" 128 ")), 128);
        assert_eq!(parse_spin_budget(Some("garbage")), SPIN_TRIES);
        assert_eq!(parse_spin_budget(Some("0")), 0);
    }
}
