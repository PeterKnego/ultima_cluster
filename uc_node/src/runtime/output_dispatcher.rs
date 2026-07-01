//! Node-side output dispatcher.
//!
//! Reads `(log_index, cmd_bytes)` tuples off an in-process
//! `tokio::sync::mpsc::Receiver` (fed by apply_dispatcher in steady-state
//! and by output_replay during leader-transition gap-fill). Publishes
//! `OutputFrame` on `service/output.ring` with a 1 s grace before
//! skipping; awaits `OutputResp` on `service/output_resp.ring`. On Ok,
//! advances `output_progress.state` durably. On Retryable, exponential
//! backoff (10ms → 1s cap) and retries while still leader. On Permanent,
//! warn-logs and advances the marker anyway.
//!
//! Skip path: if `output.ring` stays Full for >1 s, the frame is dropped
//! and `output_progress` is NOT advanced. The next leader transition's
//! replay sweep will catch the gap.

#![allow(dead_code)] // wired into NodeBuilder in M5 Task 3.4

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex as PlMutex;
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use uc_protocol::frames::output::{
    MSG_TYPE_OUTPUT, MSG_TYPE_OUTPUT_RESP, OutputError, decode_extra_output, decode_flags_output,
    encode_extra_output, encode_flags_output,
};
use uc_protocol::ring::RingError;
use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer};

use ultima_journal::StableValue;

pub type LeaderStateRx = watch::Receiver<bool>;

pub struct OutputDispatcherHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

const PUBLISH_GRACE: Duration = Duration::from_secs(1);
const PUBLISH_RETRY_BACKOFF: Duration = Duration::from_micros(100);
const RESPONSE_POLL_IDLE: Duration = Duration::from_micros(10);
const RETRY_BACKOFF_INITIAL: Duration = Duration::from_millis(10);
const RETRY_BACKOFF_CAP: Duration = Duration::from_secs(1);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Max entries drained from `output_chan` before one coalesced `output_progress`
/// fsync. Advancing the durable marker is a fsync (`StableValue::store().wait()`);
/// doing it per entry caps the dispatcher far below the commit rate (15k/s ⇒ 15k
/// fsync/s, impossible), so `output_chan` backs up and `output_progress` stalls
/// near 0 — which makes leadership-transition output-replay scan the whole log.
/// We instead process a drained batch and fsync ONCE to the batch's highest
/// completed index. Under load the drain is large (naturally coalescing the
/// fsync); at low load batches are size 1 (a fsync per entry, but the rate is
/// low so it is free). At-least-once output is preserved: a crash mid-batch
/// replays from the last durably-marked index and `on_committed` is idempotent
/// by contract. Advancing to the last completed index in the batch is
/// behavior-equivalent to the old per-entry advance (indices arrive in ascending
/// log order, so the last completed IS the max).
const OUTPUT_BATCH_MAX: usize = 1024;

pub fn spawn_output_dispatcher(
    mut rx: Receiver<(u64, Bytes)>,
    output_producer: SpscProducer,
    mut output_resp_consumer: SpscConsumer,
    output_progress: Arc<StableValue<u64>>,
    leader_rx: LeaderStateRx,
) -> OutputDispatcherHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    let producer = Arc::new(PlMutex::new(output_producer));

    let join = tokio::spawn(async move {
        while !stop_for_task.load(Ordering::Relaxed) {
            // Block for the first item, then drain everything else immediately
            // available (up to OUTPUT_BATCH_MAX) so the durable output_progress
            // fsync coalesces to ONE store per batch instead of one per entry.
            let Some(first) = rx.recv().await else {
                break;
            };
            let mut batch: Vec<(u64, Bytes)> = Vec::with_capacity(16);
            batch.push(first);
            while batch.len() < OUTPUT_BATCH_MAX {
                match rx.try_recv() {
                    Ok(item) => batch.push(item),
                    Err(_) => break,
                }
            }

            // Process each entry (publish + await + decide) exactly as before,
            // but DEFER the durable marker: track the highest completed index and
            // fsync once after the batch. `highest_completed` ends at the last
            // Ok/Permanent entry, which — because rx delivers in ascending log
            // order — is the max, matching the old per-entry advance semantics
            // (including jumping past a mid-batch skip's gap).
            let mut highest_completed: Option<u64> = None;
            'batch: for (log_index, cmd_bytes) in batch {
                if stop_for_task.load(Ordering::Relaxed) {
                    break 'batch;
                }
                if !*leader_rx.borrow() {
                    tracing::debug!(log_index, "output_dispatcher: not leader; dropping");
                    continue;
                }

                let mut backoff = RETRY_BACKOFF_INITIAL;
                'entry: loop {
                    if stop_for_task.load(Ordering::Relaxed) || !*leader_rx.borrow() {
                        break 'batch;
                    }

                    // 1) Publish OutputFrame with 1s grace.
                    match publish_output_frame(&producer, log_index, &cmd_bytes).await {
                        PublishOutcome::Published => {}
                        PublishOutcome::Skipped => {
                            tracing::warn!(
                                log_index,
                                "output.ring full > 1s; skipping; replay will catch this"
                            );
                            break 'entry;
                        }
                        PublishOutcome::FatalError => break 'entry,
                    }

                    // 2) Await response. Honors `stop` so shutdown isn't gated
                    // on the 30s timeout when no service-side consumer is wired.
                    let resp = match await_output_resp(
                        &mut output_resp_consumer,
                        log_index,
                        &stop_for_task,
                    )
                    .await
                    {
                        Some(r) => r,
                        None => {
                            if stop_for_task.load(Ordering::Relaxed) {
                                break 'batch;
                            }
                            tracing::warn!(
                                log_index,
                                "output_resp timeout; skipping; replay will catch"
                            );
                            break 'entry;
                        }
                    };

                    // 3) Decide.
                    match resp {
                        Ok(()) => {
                            highest_completed = Some(log_index);
                            break 'entry;
                        }
                        Err(OutputError::Permanent(ref msg)) => {
                            tracing::warn!(
                                log_index,
                                msg,
                                "OutputError::Permanent — advancing marker anyway"
                            );
                            highest_completed = Some(log_index);
                            break 'entry;
                        }
                        Err(OutputError::Retryable(ref msg)) => {
                            tracing::info!(log_index, msg, ?backoff, "Retryable — backoff");
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(RETRY_BACKOFF_CAP);
                        }
                    }
                }
            }

            // ONE coalesced durable fsync for the whole batch.
            if let Some(idx) = highest_completed {
                advance_output_progress(&output_progress, idx);
            }
        }
    });

    OutputDispatcherHandle { join, stop }
}

fn advance_output_progress(output_progress: &StableValue<u64>, log_index: u64) {
    match output_progress.store(&log_index) {
        Ok(notifier) => {
            if let Err(e) = notifier.wait() {
                tracing::error!(?e, log_index, "output_progress wait failed");
            }
        }
        Err(e) => {
            tracing::error!(?e, log_index, "output_progress store failed");
        }
    }
}

enum PublishOutcome {
    Published,
    Skipped,
    FatalError,
}

async fn publish_output_frame(
    producer: &Arc<PlMutex<SpscProducer>>,
    log_index: u64,
    cmd_bytes: &[u8],
) -> PublishOutcome {
    let deadline = std::time::Instant::now() + PUBLISH_GRACE;
    loop {
        let result = {
            let mut g = producer.lock();
            g.try_write(
                MSG_TYPE_OUTPUT,
                encode_flags_output(0),
                encode_extra_output(log_index),
                cmd_bytes,
            )
        };
        match result {
            Ok(()) => return PublishOutcome::Published,
            Err(RingError::Full) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(PUBLISH_RETRY_BACKOFF).await;
            }
            Err(RingError::Full) => return PublishOutcome::Skipped,
            Err(e) => {
                tracing::error!(?e, log_index, "output.ring write");
                return PublishOutcome::FatalError;
            }
        }
    }
}

async fn await_output_resp(
    consumer: &mut SpscConsumer,
    expected_log_index: u64,
    stop: &AtomicBool,
) -> Option<Result<(), OutputError>> {
    let deadline = std::time::Instant::now() + RESPONSE_TIMEOUT;
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    while std::time::Instant::now() < deadline {
        if stop.load(Ordering::Relaxed) {
            return None;
        }
        match consumer.try_read(&mut buf) {
            Ok(Some(rec)) if rec.msg_type == MSG_TYPE_OUTPUT_RESP => {
                if let Err(e) = decode_flags_output(rec.flags) {
                    tracing::warn!(?e, log_index = expected_log_index, "OutputResp bad flags");
                    return Some(Err(OutputError::Permanent(format!("{e}"))));
                }
                let got_idx = decode_extra_output(rec.header_extra);
                if got_idx != expected_log_index {
                    tracing::warn!(
                        got_idx,
                        expected = expected_log_index,
                        "OutputResp log_index mismatch — dropping"
                    );
                    continue;
                }
                let (result, _) = match bincode::serde::decode_from_slice::<
                    Result<(), OutputError>,
                    _,
                >(&buf, bincode::config::standard())
                {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!(?e, "OutputResp decode");
                        return Some(Err(OutputError::Permanent(format!("decode: {e}"))));
                    }
                };
                return Some(result);
            }
            Ok(Some(rec)) => {
                tracing::warn!(
                    msg_type = rec.msg_type,
                    "unexpected frame on output_resp.ring"
                );
            }
            Ok(None) => tokio::time::sleep(RESPONSE_POLL_IDLE).await,
            Err(e) => {
                tracing::error!(?e, "output_resp.ring read");
                tokio::time::sleep(RESPONSE_POLL_IDLE).await;
            }
        }
    }
    None
}
