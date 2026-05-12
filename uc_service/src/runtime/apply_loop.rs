//! Sync apply loop.
//!
//! Runs on a dedicated `std::thread` (NOT a tokio task) because
//! [`crate::StateMachine::apply`] is sync and may be CPU-bound; we don't
//! want to block a tokio worker. Drains `service/apply.ring`, invokes
//! `state_machine.apply(log_index, cmd)` while holding the SM mutex, then
//! publishes `ApplyRespFrame` into `service/apply_resp.ring`.
//!
//! Shutdown: caller sets [`ApplyLoopHandle::stop`] and joins.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use bincode::config::standard as bincode_standard;
use parking_lot::Mutex;
use uc_protocol::frames::apply::{
    MSG_TYPE_APPLY, MSG_TYPE_APPLY_RESP, decode_extra_apply, encode_extra_apply,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};

use crate::StateMachine;

/// Backoff when the ring is empty. Tight enough that latency stays in the
/// hundreds of microseconds; loose enough that an idle service doesn't spin
/// a core.
const IDLE_BACKOFF: Duration = Duration::from_micros(100);
const ERROR_BACKOFF: Duration = Duration::from_millis(10);

pub struct ApplyLoopHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn_apply_loop<S>(
    sm: Arc<Mutex<S>>,
    mut consumer: SpscConsumer,
    mut resp_producer: SpscProducer,
) -> ApplyLoopHandle
where
    S: StateMachine,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_thread = Arc::clone(&stop);
    let join = std::thread::Builder::new()
        .name("uc-service-apply".into())
        .spawn(move || {
            apply_thread_body::<S>(sm, &mut consumer, &mut resp_producer, stop_for_thread)
        })
        .expect("spawn apply thread");
    ApplyLoopHandle { join, stop }
}

fn apply_thread_body<S>(
    sm: Arc<Mutex<S>>,
    consumer: &mut SpscConsumer,
    resp_producer: &mut SpscProducer,
    stop: Arc<AtomicBool>,
) where
    S: StateMachine,
{
    let mut payload_buf: Vec<u8> = Vec::with_capacity(4096);
    while !stop.load(Ordering::Relaxed) {
        match consumer.try_read(&mut payload_buf) {
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_APPLY => {
                let log_index = decode_extra_apply(rec.header_extra);
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
                    let mut guard = sm.lock();
                    guard.apply(log_index, cmd)
                };
                let resp_bytes = bincode::serde::encode_to_vec(&resp, bincode_standard())
                    .expect("apply response encode");
                publish_response(resp_producer, log_index, &resp_bytes, &stop);
            }
            Ok(Some(rec)) => {
                tracing::warn!(msg_type = rec.msg_type, "apply ring: unexpected frame");
            }
            Ok(None) => std::thread::sleep(IDLE_BACKOFF),
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
            0,
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
