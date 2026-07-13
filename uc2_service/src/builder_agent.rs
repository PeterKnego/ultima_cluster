// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The snapshot builder duty cycle (M6 Task 3). A single polling thread,
//! spawned only by [`crate::ServiceBuilder::start_with_snapshots`], that pulls
//! one `(position, streaming job)` handoff at a time off a 1-slot channel from
//! the apply thread, streams it to [`SnapshotStore::publish`] — off the SM
//! lock entirely — and, on success, publishes the position onto the cnc
//! marker.
//!
//! **Type-erased on purpose.** This module carries NO `S: StateMachine` /
//! `S: SnapshotStateMachine` generic parameter at all. The apply side
//! (`crate::apply::SnapshotTrigger`) already knows the concrete `S` when it
//! calls `sm.freeze()`, and wraps the resulting `stream_snapshot` call into a
//! boxed [`BuildJob`] closure BEFORE handing it across the channel — so by the
//! time this thread sees a job, the concrete snapshot-handle type has already
//! been erased. That keeps the builder thread's own machinery (and
//! [`ServiceBuilder::start`](crate::ServiceBuilder::start)'s callers, which
//! never touch this module) entirely free of the `SnapshotStateMachine` bound.
//!
//! **The "one in-flight build max" rule** is enforced by `busy`, a flag SHARED
//! with the apply thread's [`crate::apply::SnapshotTrigger`]: the apply thread
//! checks it BEFORE even calling `freeze()` (so a busy builder means no new
//! freeze calls, not just a full channel), and this thread holds it set for the
//! full stream+publish duration — not merely while the job sits in the
//! channel, which by itself would under-count a build already in progress.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;

use uc2_log::cnc::CncPage;

use crate::config::SnapshotError;
use crate::snapshots::SnapshotStore;

/// A type-erased streaming job: call it with the destination writer to stream
/// the (already frozen, off-lock-safe) pinned state.
pub(crate) type BuildJob = Box<dyn FnOnce(&mut dyn Write) -> Result<(), SnapshotError> + Send>;

/// Everything the builder thread owns.
pub(crate) struct BuilderState {
    pub(crate) rx: mpsc::Receiver<(u64, BuildJob)>,
    pub(crate) store: SnapshotStore,
    pub(crate) cnc: Arc<CncPage>,
    /// Shared with `SnapshotTrigger` on the apply side; see the module doc.
    pub(crate) busy: Arc<AtomicBool>,
}

/// One builder duty cycle. Returns `true` iff it drained a job (drives the
/// idle strategy) — at most one job per cycle, matching the "one in-flight
/// build max" contract (there is never more than one queued anyway, since the
/// apply side won't send another until `busy` clears).
pub(crate) fn builder_cycle(st: &mut BuilderState) -> bool {
    match st.rx.try_recv() {
        Ok((pos, job)) => {
            match st.store.publish(pos, job) {
                Ok(_path) => {
                    // The ONLY write site for this marker: after the atomic
                    // rename inside `publish` has already completed, so a torn
                    // build is never observed here (module doc / snapshots.rs
                    // doc).
                    st.cnc.snapshots().service_snapshot_pos.store_release(pos);
                }
                Err(e) => {
                    // Logged + dropped: the marker is not advanced, so the
                    // next policy-interval trip (from the SAME
                    // `last_snapshot_pos` basis — see `SnapshotTrigger`)
                    // retries with a fresh attempt.
                    eprintln!(
                        "uc2_service: snapshot build at position {pos} failed: {e} \
                         (dropped; the next policy interval retries)"
                    );
                }
            }
            st.busy.store(false, Ordering::Release);
            true
        }
        Err(mpsc::TryRecvError::Empty) => false,
        // The apply thread (and its `SnapshotTrigger`) is gone — nothing more
        // will ever arrive. Not an error: this is the ordinary shape of
        // teardown when the apply agent is stopped before the builder agent.
        Err(mpsc::TryRecvError::Disconnected) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc2_log::cnc::{CncMeta, CncPage};

    fn page() -> Arc<CncPage> {
        CncPage::heap(&CncMeta {
            node_id: 1,
            instance_id: 1,
            app_id: "builder-test".into(),
            buffer_bytes: 1 << 20,
            max_payload: 256,
        })
    }

    fn state(dir: &std::path::Path) -> (mpsc::SyncSender<(u64, BuildJob)>, BuilderState, Arc<AtomicBool>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let busy = Arc::new(AtomicBool::new(false));
        let st = BuilderState {
            rx,
            store: SnapshotStore::open(dir).unwrap(),
            cnc: page(),
            busy: Arc::clone(&busy),
        };
        (tx, st, busy)
    }

    #[test]
    fn empty_cycle_makes_no_progress() {
        let dir = tempfile::tempdir().unwrap();
        let (_tx, mut st, _busy) = state(dir.path());
        assert!(!builder_cycle(&mut st));
    }

    #[test]
    fn a_successful_job_publishes_the_file_and_the_cnc_marker_then_clears_busy() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, mut st, busy) = state(dir.path());
        let cnc = Arc::clone(&st.cnc);
        busy.store(true, Ordering::Release);

        let job: BuildJob = Box::new(|w| {
            w.write_all(b"snapshot-bytes")?;
            Ok(())
        });
        tx.try_send((4096, job)).unwrap();

        assert!(builder_cycle(&mut st), "one job drained");
        assert_eq!(cnc.snapshots().service_snapshot_pos.load_acquire(), 4096);
        assert!(!busy.load(Ordering::Acquire), "busy cleared after completion");

        let store = SnapshotStore::open(dir.path()).unwrap();
        let (pos, path) = store.newest(u64::MAX).unwrap().unwrap();
        assert_eq!(pos, 4096);
        assert_eq!(std::fs::read(path).unwrap(), b"snapshot-bytes");
    }

    /// A failing job must NOT advance the cnc marker, and must still clear
    /// `busy` (so the next interval can try again — the builder must never
    /// wedge on a single failed build).
    #[test]
    fn a_failing_job_does_not_advance_the_marker_but_clears_busy() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, mut st, busy) = state(dir.path());
        let cnc = Arc::clone(&st.cnc);
        busy.store(true, Ordering::Release);

        let job: BuildJob = Box::new(|_w| Err(SnapshotError::Codec("boom".into())));
        tx.try_send((4096, job)).unwrap();

        assert!(builder_cycle(&mut st));
        assert_eq!(cnc.snapshots().service_snapshot_pos.load_acquire(), 0, "marker not advanced");
        assert!(!busy.load(Ordering::Acquire), "busy cleared even on failure");
    }

    #[test]
    fn a_disconnected_channel_is_treated_as_no_progress_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, mut st, _busy) = state(dir.path());
        drop(tx);
        assert!(!builder_cycle(&mut st));
    }
}
