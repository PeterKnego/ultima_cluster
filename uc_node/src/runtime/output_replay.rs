//! Output replay — on becoming leader, scan the journal between
//! `output_progress.state` and `last_applied`, inject each entry's
//! `(log_index, cmd_bytes)` into the apply→output mpsc channel for the
//! output_dispatcher to process. Exits when caught up or when leadership
//! is lost.
//!
//! Blank and Membership entries are skipped (no user command), but the
//! marker still advances past them so they don't get re-scanned forever.

#![allow(dead_code)] // wired into builder below

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use openraft::EntryPayload;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use ultima_journal::{Journal, StableValue};

use crate::raft::TypeConfig;

pub struct OutputReplayHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

pub struct OutputReplayWatcherHandle {
    pub join: JoinHandle<()>,
    pub stop: Arc<AtomicBool>,
}

/// Spawn the leader-transition watcher.
///
/// Each time `leader_rx` transitions `false → true`, captures `last_applied`
/// at that instant and spawns a one-shot `output_replay` task that scans
/// `(output_progress, last_applied]` and feeds gap entries into the
/// apply→output mpsc channel.
pub fn spawn_output_replay_watcher<F>(
    journal: Arc<Journal>,
    output_progress: Arc<StableValue<u64>>,
    output_chan_tx: Sender<(u64, Bytes)>,
    leader_rx: watch::Receiver<bool>,
    last_applied_provider: F,
) -> OutputReplayWatcherHandle
where
    F: Fn() -> u64 + Send + Sync + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    let join = tokio::spawn(async move {
        let mut rx = leader_rx;
        let mut was_leader = *rx.borrow();

        // On startup, if we're already leader, fire an immediate replay.
        if was_leader {
            spawn_one_shot_replay(
                journal.clone(),
                output_progress.clone(),
                output_chan_tx.clone(),
                rx.clone(),
                last_applied_provider(),
            );
        }

        loop {
            if stop_for_task.load(Ordering::Relaxed) {
                break;
            }
            if rx.changed().await.is_err() {
                break; // sender dropped → exit
            }
            let is_leader = *rx.borrow();
            if is_leader && !was_leader {
                let last_applied = last_applied_provider();
                tracing::info!(last_applied, "leader transition — spawning output_replay");
                spawn_one_shot_replay(
                    journal.clone(),
                    output_progress.clone(),
                    output_chan_tx.clone(),
                    rx.clone(),
                    last_applied,
                );
            }
            was_leader = is_leader;
        }
    });

    OutputReplayWatcherHandle { join, stop }
}

fn spawn_one_shot_replay(
    journal: Arc<Journal>,
    output_progress: Arc<StableValue<u64>>,
    output_chan_tx: Sender<(u64, Bytes)>,
    leader_rx: watch::Receiver<bool>,
    last_applied_at_transition: u64,
) -> OutputReplayHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_task = Arc::clone(&stop);

    let join = tokio::spawn(async move {
        let last_completed = match output_progress.load() {
            Ok(Some(v)) => v,
            Ok(None) => 0,
            Err(e) => {
                tracing::error!(?e, "replay: output_progress load failed");
                return;
            }
        };

        if last_completed >= last_applied_at_transition {
            tracing::debug!(
                last_completed,
                last_applied = last_applied_at_transition,
                "replay: caught up; no gap"
            );
            return;
        }

        tracing::info!(
            last_completed,
            last_applied = last_applied_at_transition,
            count = last_applied_at_transition - last_completed,
            "replay: scanning gap"
        );

        let range = (last_completed + 1)..=last_applied_at_transition;
        let iter = match journal.iter_range(range) {
            Ok(it) => it,
            Err(e) => {
                tracing::error!(?e, "replay: journal iter_range failed");
                return;
            }
        };

        for record in iter {
            if stop_for_task.load(Ordering::Relaxed) || !*leader_rx.borrow() {
                tracing::debug!("replay: aborted (stopped or no longer leader)");
                return;
            }
            let (seq, _meta, payload) = match record {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(?e, "replay: journal record read failed");
                    continue;
                }
            };

            let log_index = seq;

            // Decode the openraft Entry and extract the command bytes for
            // Normal entries. Blank/Membership entries advance the marker
            // without dispatch.
            let cmd_bytes_opt = decode_entry_app_data(&payload);

            match cmd_bytes_opt {
                Some(cmd_bytes) => {
                    // Use send (blocking) so replay backpressures naturally
                    // against the dispatcher rather than dropping. If the
                    // channel is closed, the dispatcher is gone — abort.
                    if output_chan_tx.send((log_index, cmd_bytes)).await.is_err() {
                        tracing::warn!(log_index, "replay: output_chan closed; aborting");
                        return;
                    }
                }
                None => {
                    // Blank or Membership: advance output_progress past it.
                    advance_progress(&output_progress, log_index);
                }
            }
        }

        tracing::info!("replay: complete");
    });

    OutputReplayHandle { join, stop }
}

fn decode_entry_app_data(payload: &[u8]) -> Option<Bytes> {
    let (entry, _): (<TypeConfig as openraft::RaftTypeConfig>::Entry, _) =
        bincode::serde::decode_from_slice(payload, bincode::config::standard()).ok()?;
    match entry.payload {
        EntryPayload::Normal(app_data) => Some(app_data.0), // AppCommand newtype: .0 is Bytes
        EntryPayload::Blank | EntryPayload::Membership(_) => None,
    }
}

fn advance_progress(output_progress: &StableValue<u64>, log_index: u64) {
    match output_progress.store(&log_index) {
        Ok(notifier) => {
            if let Err(e) = notifier.wait() {
                tracing::error!(?e, log_index, "replay: output_progress wait failed");
            }
        }
        Err(e) => {
            tracing::error!(?e, log_index, "replay: output_progress store failed");
        }
    }
}
