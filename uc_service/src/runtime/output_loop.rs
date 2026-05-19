//! Service-side `output_loop` — consumes `OutputFrame` from
//! `service/output.ring`, decodes the user command, invokes the wired
//! `OutputHandler::on_committed` with a read borrow of the state, and
//! publishes `OutputResp` carrying the user's `Result<(), OutputError>`
//! on `service/output_resp.ring`.
//!
//! Lives in its own tokio task so user `on_committed` futures can await
//! freely while `apply_loop` continues to acquire the write lock when
//! the read lock is released. See the `OutputHandler` rustdoc for the
//! state-borrow contract that affects apply latency.

#![allow(dead_code)] // wired into ServiceBuilder in M5 Task 2.4

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bincode::config::standard as bincode_standard;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use uc_protocol::frames::output::{
    MSG_TYPE_OUTPUT, MSG_TYPE_OUTPUT_RESP, OutputError, decode_extra_output, decode_flags_output,
    encode_extra_output, encode_flags_output,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};

use crate::StateMachine;
use crate::output_handler::OutputHandler;

/// Backoff when the ring is empty.
const IDLE_BACKOFF: Duration = Duration::from_micros(100);
/// Backoff when the response ring is full.
const FULL_BACKOFF: Duration = Duration::from_micros(100);
/// Backoff after a read error.
const ERROR_BACKOFF: Duration = Duration::from_millis(10);

pub struct OutputLoopHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

/// Spawn the service-side output loop.
///
/// `consumer` is the service-side reader half of `service/output.ring`.
/// `producer` is the service-side writer half of `service/output_resp.ring`.
/// `state` is the same `Arc<RwLock<S>>` shared with `apply_loop`.
pub fn spawn_output_loop<S, O>(
    state: Arc<RwLock<S>>,
    handler: Arc<O>,
    consumer: SpscConsumer,
    producer: SpscProducer,
) -> OutputLoopHandle
where
    S: StateMachine,
    O: OutputHandler<S> + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    let join = tokio::spawn(output_task_body::<S, O>(
        state,
        handler,
        consumer,
        producer,
        stop_for_task,
    ));

    OutputLoopHandle { join, stop }
}

async fn output_task_body<S, O>(
    state: Arc<RwLock<S>>,
    handler: Arc<O>,
    mut consumer: SpscConsumer,
    mut producer: SpscProducer,
    stop: Arc<AtomicBool>,
) where
    S: StateMachine,
    O: OutputHandler<S> + 'static,
{
    let mut payload_buf: Vec<u8> = Vec::with_capacity(4096);
    while !stop.load(Ordering::Relaxed) {
        match consumer.try_read(&mut payload_buf) {
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_OUTPUT => {
                let log_index = decode_extra_output(rec.header_extra);

                // Validate flags (service_id must be 0 in v1).
                if let Err(e) = decode_flags_output(rec.flags) {
                    tracing::warn!(?e, log_index, "dropping output frame with bad flags");
                    publish_resp(
                        &mut producer,
                        log_index,
                        Err(OutputError::Permanent(format!("{e}"))),
                        &stop,
                    )
                    .await;
                    continue;
                }

                // Decode the user command from the payload.
                let cmd = match bincode::serde::decode_from_slice::<S::Command, _>(
                    &payload_buf,
                    bincode_standard(),
                ) {
                    Ok((cmd, _)) => cmd,
                    Err(e) => {
                        tracing::error!(?e, log_index, "output cmd decode failed");
                        publish_resp(
                            &mut producer,
                            log_index,
                            Err(OutputError::Permanent(format!("decode: {e}"))),
                            &stop,
                        )
                        .await;
                        continue;
                    }
                };

                // Invoke the user's on_committed under the read lock.
                //
                // `tokio::sync::RwLockReadGuard` is Send (unlike parking_lot's),
                // so it is safe to hold across the .await on on_committed.
                // While the guard is held, apply_loop's blocking_write() will
                // block, so on_committed should be short — extract what it needs
                // from state and drop the guard before any slow I/O. See
                // OutputHandler rustdoc for the recommended pattern.
                let result = {
                    let guard = state.read().await;
                    handler.on_committed(log_index, &cmd, &*guard).await
                };

                publish_resp(&mut producer, log_index, result, &stop).await;
            }
            Ok(Some(rec)) => {
                tracing::warn!(msg_type = rec.msg_type, "output ring: unexpected frame");
            }
            Ok(None) => tokio::time::sleep(IDLE_BACKOFF).await,
            Err(e) => {
                tracing::warn!(error = %e, "output ring read error");
                tokio::time::sleep(ERROR_BACKOFF).await;
            }
        }
    }
}

async fn publish_resp(
    producer: &mut SpscProducer,
    log_index: u64,
    result: Result<(), OutputError>,
    stop: &AtomicBool,
) {
    let payload = match bincode::serde::encode_to_vec(&result, bincode_standard()) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(?e, log_index, "encode OutputResp payload");
            return;
        }
    };
    loop {
        match producer.try_write(
            MSG_TYPE_OUTPUT_RESP,
            encode_flags_output(0),
            encode_extra_output(log_index),
            &payload,
        ) {
            Ok(()) => return,
            Err(RingError::Full) => {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                tokio::time::sleep(FULL_BACKOFF).await;
            }
            Err(e) => {
                tracing::error!(?e, log_index, "output_resp.ring write");
                return;
            }
        }
    }
}
